use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::sink::{Sink, SinkExt};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::task;

use crate::diag::SessionStats;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_codec::{NntpCodec, NntpResponse};
use crate::server::Notification;
use crate::spool::{ArtLoc, ArtPart, Spool};
use crate::util::Buffer;

// Sent from the nntp server incoming feeds to the masterfeed.
pub enum FeedItem {
    Article(FeedArticle),
    Notification(Notification),
}

// Article to be queued and a list of peers.
pub struct FeedArticle {
    // Message-Id.
    msgid:    String,
    // Location in the article spool.
    location: ArtLoc,
    // Peers to feed it to.
    peers:  Vec<String>,
}

// Masterfeed.
//
// Receives articles from the incoming feed, and then fans them
// out over all PeerFeeds that want the article.
//
pub struct MasterFeed {
    receiver:   mpsc::Receiver<FeedItem>,
    newsfeeds:  Arc<NewsFeeds>,
    peerfeeds:  HashMap<String, mpsc::Sender<PeerFeedItem>>,
}

impl MasterFeed {
    pub fn new(newsfeeds: Arc<NewsFeeds>, receiver: mpsc::Receiver<FeedItem>, spool: Spool) -> MasterFeed {

        // Create all the PeerFeeds.
        let mut peerfeeds = HashMap::new();
        for peer in &newsfeeds.peers {
            let peer_feed = PeerFeed::new(&peer.label, peer, newsfeeds.clone(), spool.clone());
            let tx_chan = peer_feed.tx_chan.clone();
            tokio::spawn(async move {
                peer_feed.run().await
            });
            peerfeeds.insert(peer.label.clone(), tx_chan);
        }

        MasterFeed {
            receiver,
            newsfeeds,
            peerfeeds,
        }
    }

    // Reconfigure - check the current set of peerfeeds that we feed to to the
    // total list in the newsfeed set. Stop any of them that are not configured anymore.
    fn reconfigure(&mut self) {
        debug!("MasterFeed::reconfigure called");
    }

    /// The actual task. This reads from the receiver channel and fans out
    /// the messages over all peerfeeds.
    ///
    pub async fn run(mut self) {
        while let Some(item) = self.receiver.recv().await {
            match item {
                FeedItem::Article(art) => {
                    // Forwards only to the peerfeeds in the list.
                    for peername in &art.peers {
                        if let Some(peerfeed) = self.peerfeeds.get_mut(peername) {
                            let peer_art = PeerArticle {
                                msgid:  art.msgid.clone(),
                                location: art.location.clone(),
                            };
                            let _ = peerfeed.send(PeerFeedItem::Article(peer_art)).await;
                        }
                    }
                },
                // XXX FIXME we need to look at notifications ourself too:
                // - ExitGraceful, need to poll to see if all peerfeeds are gone
                // - ExitNow, ditto
                FeedItem::Notification(msg) => {
                    match msg {
                        Notification::Reconfigure => self.reconfigure(),
                        _ => {},
                    }
                    // Forward to all peerfeeds.
                    for peerfeed in self.peerfeeds.values_mut() {
                        let _ = peerfeed.send(PeerFeedItem::Notification(msg.clone())).await;
                    }
                }
            }
        }
    }
}

// Sent from the masterfeed to the peerfeeds.
enum PeerFeedItem {
    Article(PeerArticle),
    Notification(Notification),
}

// Article in the peerfeed queue.
#[derive(Clone)]
struct PeerArticle {
    // Message-Id.
    msgid:    String,
    // Location in the article spool.
    location: ArtLoc,
}


// Newsfeed to an NNTP peer.
//
// A peerfeed contains an in memory queue, an on-disk backlog queue,
// and zero or more active NNTP connections to remote peers.
//
struct PeerFeed {
    // Name.
    label:          String,
    // Comms channel.
    tx_chan:        mpsc::Sender<PeerFeedItem>,
    rx_chan:        mpsc::Receiver<PeerFeedItem>,
    // Reference to newsfeed config.
    newsfeeds:      Arc<NewsFeeds>,
    // Peerfeed article queue.
    shared:          PeerFeedShared,
    // Active connections.
    connections:    Vec<mpsc::Sender<Notification>>,
    // For round-robining, the last idle connection that was awakened.
    last_idle: u32,
    // Backlog file(s) reader.
    //resend:         DiskReadQueue,
    // Backlog file(s) writer.
    //backlog:        DiskWriteQueue,
    // Spool instance.
    spool:  Spool,
    // From newsfeeds::NewsPeer.
    pub outhost:            String,
    pub bindaddress:        String,
    pub port:               u16,
    pub maxparallel:        u32,
    pub maxstream:          u32,
    pub nobatch:            bool,
    pub maxqueue:           u32,
    pub headfeed:           bool,
    pub genlines:           bool,
    pub preservebytes:      bool,
}

impl PeerFeed {

    /// Create a new PeerFeed.
    fn new(label: &str, nfpeer: &NewsPeer, newsfeeds: Arc<NewsFeeds>, spool: Spool) -> PeerFeed {
        let (tx_chan, rx_chan) = mpsc::channel::<PeerFeedItem>(16);
        PeerFeed {
            label:  label.to_string(),
            rx_chan,
            tx_chan: tx_chan.clone(),
            newsfeeds,
            shared:      PeerFeedShared::new(tx_chan),
            connections:    Vec::new(),
            last_idle:  0,
            spool,
            outhost:    nfpeer.outhost.clone(),
            bindaddress: nfpeer.bindaddress.clone(),
            port:       nfpeer.port,
            maxparallel:    nfpeer.maxparallel,
            nobatch:    nfpeer.nobatch,
            maxqueue:   nfpeer.maxqueue,
            maxstream:   nfpeer.maxstream,
            headfeed:   nfpeer.headfeed,
            genlines:   nfpeer.genlines,
            preservebytes:  nfpeer.preservebytes,
        }
    }

    /// Run the PeerFeed.
    async fn run(mut self) {
        while let Some(item) = self.rx_chan.recv().await {
            match item {
                PeerFeedItem::Notification(msg) => {
                    // XXX TODO handle notification
                },
                PeerFeedItem::Article(art) => {
                    // queue article
                    let mut shared = self.shared.lock();
                    if shared.queue.len() > 1000 {
                        // XXX TODO: full. send half of the queue to the backlog.
                        continue;
                    }
                    shared.queue.push_back(art);

                    // if we have no connections, or less than maxconn, create a connection here.
                    if shared.num_conns < self.maxparallel {
                        shared.num_conns += 1;
                        let label = self.label.clone();
                        let outhost = self.outhost.clone();
                        let peerfeed = self.shared.clone();
                        let spool = self.spool.clone();

                        // XXX TODO
                        task::spawn(async move {
                            let conn = Connection::new(&label, &outhost, peerfeed, spool);
                        });

                        // unless a connection is still being set up - we only create one connection
                        // at a time, so that we don't thunderherd the peer.
                        // TODO: create connection.
                    }

                    // now notify one of the connections. TODO: use u128.leading_zeros()
                    if shared.idle_conns != 0 {
                        let mut conn_id = self.last_idle;
                        let max = self.connections.len() as u32;
                        for _ in 0 .. max {
                            conn_id = (conn_id + 1) % max;
                            if (shared.idle_conns & (1 << conn_id)) != 0 {
                                // XXX TODO queue depth one
                                if let Ok(_) = self.connections[conn_id as usize].try_send(Notification::None) {
                                    self.last_idle = conn_id;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// Data shared between PeerFeed and Connections.
#[derive(Clone)]
struct PeerFeedShared {
    inner:  Arc<Mutex<PeerFeedSharedInner>>,
}

impl PeerFeedShared {
}

// Peerfeed article queue and Connection-idle indicator.
struct PeerFeedSharedInner {
    queue:  VecDeque<PeerArticle>,
    num_conns:  u32,
    idle_conns: u128,
    tx_chan:        mpsc::Sender<PeerFeedItem>,
}

impl PeerFeedShared {
    fn new(tx_chan: mpsc::Sender<PeerFeedItem>) -> PeerFeedShared {
        let inner = PeerFeedSharedInner {
            queue:  VecDeque::new(),
            idle_conns: 0,
            num_conns: 0,
            tx_chan,
        };
        PeerFeedShared {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    // Add a connection. Returns an id and a rx channel.
    fn add_connection(&self) -> (u32, mpsc::Receiver<Notification>) {
        let (_tx_chan, rx_chan) = mpsc::channel::<Notification>(16);
        (0, rx_chan)
    }

    fn get_article(&self, conn_id: u32, empty: bool) -> Option<PeerArticle> {
        let mut inner = self.lock();
        if let Some(art) = inner.queue.pop_front() {
            return Some(art);
        }
        if empty {
            // set connection to idle. Also send a notification to
            // the PeerFeed to request it to check the backlog.
            inner.idle_conns |= 1 << conn_id;
            let _ = inner.tx_chan.try_send(PeerFeedItem::Notification(Notification::None));
        }
        None
    }

    fn lock(&self) -> lock_api::MutexGuard<parking_lot::RawMutex, PeerFeedSharedInner> {
        self.inner.lock()
    }
}


// =========================================================================== //


//
// A connection.
//
// This is the actual TCP connection to a remote peer. It sends a CHECK message
// for every article to the peer. If the peers wants the article, we send
// a TAKETHIS. If the peer does not want the article, fine, drop it.
//
// If the peer defers the article, we put it in a local in-memory queue and
// try again after 5 seconds, for a max. of 3 times. If the deferred-queue
// gets larger than 5000 items, we drop the oldest one.
//
// Any unreckognized status code in the reply will cause us to close
// the connection, and put any outstanding request back in the PeerFeed queue.
//
struct Connection {
    id: u32,
    // reader / writer.
    reader:       NntpCodec<Box<dyn AsyncRead + Send + Unpin>>,
    writer:       NntpCodec<Box<dyn AsyncWrite + Send + Unpin>>,
    // Shared peerfeed.
    peerfeed:     PeerFeedShared,
    // Max number of outstanding requests.
    streaming:    usize,
    // Items waiting to be sent.
    send_queue:   VecDeque<ConnItem>,
    // Sent items, waiting for a reply.
    recv_queue:   VecDeque<ConnItem>,
    // Stats
    stats:        SessionStats,
    // Set after we have sent QUIT
    sender_done:  bool,
    // Notification channel receive side.
    notification: mpsc::Receiver<Notification>,
    // Spool.
    spool:        Spool,
}

impl Connection {
    // Create a new connection.
    async fn new(
        label: &str,
        outhost: &str,
        peerfeed: PeerFeedShared,
        spool: Spool,
    ) -> io::Result<Connection> {

        // First, connect.
        let codec = Connection::connect(outhost).await.map_err(|e| {
            ioerr!(e.kind(), "{}: {}", label, e)
        })?;

        // Success. Register a new connection.
        let (id, notification) = peerfeed.add_connection();

        // Build and return a new Connection struct.
        let (reader, writer) = codec.split();
        Ok(Connection {
            id,
            reader,
            writer,
            peerfeed,
            send_queue: VecDeque::new(),
            recv_queue: VecDeque::new(),
            stats: SessionStats::default(),
            sender_done: false,
            streaming: 20,
            notification,
            spool,
        })
    }

    // Spawn Self as a separate task.
    async fn run(mut self) {
        let _ = tokio::spawn(async move {
            // TODO: log initial start, final stats.
            let _ = self.feed().await;
        });
    }

    // Connect to remote peer.
    async fn connect(outhost: &str) -> io::Result<NntpCodec> {

        // A lookup of the hostname might return multiple addresses.
        // We're not sure of the order that tokio returns addresses
        // in, so sort IPv6 before IPv4.
        let addrs = match tokio::net::lookup_host(outhost).await {
            Ok(addr_iter) => {
                let mut addrs: Vec<std::net::SocketAddr> = addr_iter.collect();
                let mut addr2 = addrs.clone();
                let v6 = addr2.drain(..).filter(|a| a.is_ipv6());
                let v4 = addrs.drain(..).filter(|a| a.is_ipv4());
                let mut addrs = Vec::new();
                addrs.extend(v6);
                addrs.extend(v4);
                addrs
            },
            Err(e) => return Err(e),
        };

        // Try to connect to the peer.
        let mut last_err = None;
        for addr in &addrs {
            let result = async move {

                // Connect.
                trace!("Trying to connect to {:?}", addr);
                let socket = TcpStream::connect(addr).await.map_err(|e| {
                    trace!("connect {:?}: {}", addr, e);
                    ioerr!(e.kind(), "{}: {}", addr, e)
                })?;

                // Create codec from socket.
                let mut codec = NntpCodec::builder(socket)
                    .read_timeout(30)
                    .write_timeout(60)
                    .build();

                // Read initial response code.
                let resp = codec.read_response().await.map_err(|e| {
                    trace!("{:?} read_response: {}", addr, e);
                    ioerr!(e.kind(), "{}: {}", addr, e)
                })?;
                trace!("<< {}", resp.short());
                if resp.code != 200 {
                    Err(ioerr!(InvalidData, "{}: initial response {}, expected 200", addr, resp.code))?;
                }

                // Send MODE STREAM.
                trace!(">> MODE STREAM");
                let resp = codec.command("MODE STREAM").await.map_err(|e| {
                    ioerr!(e.kind(), "{}: {}", addr, e)
                })?;
                trace!("<< {}", resp.short());
                if resp.code != 203 {
                    Err(ioerr!(InvalidData, "{}: MODE STREAM response {}, expected 203", addr, resp.code))?;
                }

                Ok(codec)
            }.await;

            // On success, return. Otherwise, save the error.
            match result {
                Ok(codec) => return Ok(codec),
                Err(e) => last_err = Some(e),
            }
        }

        // Return the last error seen.
        Err(last_err.unwrap())
    }

    // Feeder loop.
    async fn feed(&mut self) -> io::Result<()> {
        let mut xmit_busy = false;

        loop {
            // see if we need to pull an article from the peerfeed queue.
            let queue_len = self.recv_queue.len() + self.send_queue.len();
            if queue_len < self.streaming {
                if let Some(art) = self.peerfeed.get_article(self.id, queue_len == 0) {
                    self.send_queue.push_back(ConnItem::Check(art));
                }
            }

            // If there is an item in the send queue, and we're not still busy
            // sending the previous item, pop it from the queue and start
            // sending it to the remote peer.
            if !xmit_busy {
                if let Some(item) = self.send_queue.pop_front() {
                    self.recv_queue.push_back(item.clone());
                    self.transmit_item(item).await?;
                    xmit_busy = true;
                }
            }

            tokio::select! {
                res = self.writer.flush(), if xmit_busy => {
                    // Done sending either CHECK or TAKETHIS.
                    if let Err(e) = res {
                        // TODO: update stats, log, return
                        panic!("transmit: {}", e);
                    }
                    xmit_busy = false;
                }
                res = self.reader.next() => {
                    // What did we receive?
                    match res.unwrap().and_then(NntpResponse::try_from) {
                        Err(e) => {
                        // TODO: update stats, log, return
                            panic!("transmit: {}", e);
                        },
                        Ok(resp) => {
                            // Got a reply. Find matching command.
                            match self.recv_queue.pop_front() {
                                None => panic!("recv queue out of sync"),
                                Some(ConnItem::Check(art)) => {
                                    // TODO check status code, update stats.
                                    self.send_queue.push_back(ConnItem::Takethis(art));
                                },
                                Some(ConnItem::Takethis(art)) => {
                                    // TODO check status code, send article, update stats.
                                },
                                Some(ConnItem::Quit) => {
                                    // TODO check status code, update stats, return.
                                },
                            }
                        },
                    }
                }
                res = self.notification.next() => {
                    // TODO: handle notification.
                }
            }
        }
    }

    // Put the ConnItem in the Sink.
    async fn transmit_item(&mut self, item: ConnItem) -> io::Result<()> {
        match item {
            ConnItem::Check(art) => {
                let line = format!("CHECK {}\r\n", art.msgid);
                Pin::new(&mut self.writer).start_send(line.into())
            },
            ConnItem::Takethis(art) => {
                let tmpbuf = Buffer::new();
                let buffer = match self.spool.read(art.location, ArtPart::Article, tmpbuf).await {
                    Ok(buf) => buf,
                    Err(e) => {
                        // check the error and see if it's fatal. If not,
                        // update stats and return Ok. TODO
                        return Ok(());
                    },
                };
                let line = format!("TAKETHIS {}\r\n", art.msgid);
                Pin::new(&mut self.writer).start_send(line.into())?;
                Pin::new(&mut self.writer).start_send(buffer)
            },
            ConnItem::Quit => Pin::new(&mut self.writer).start_send("QUIT\r\n".into()),
        }
    }
}

// Item in the Connection queue.
#[derive(Clone)]
enum ConnItem {
    Check(PeerArticle),
    Takethis(PeerArticle),
    Quit,
}

impl fmt::Debug for ConnItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            &ConnItem::Check(ref art) => write!(f, "\"CHECK {}\"", art.msgid),
            &ConnItem::Takethis(ref art) => write!(f, "\"TAKETHIS {}\"", art.msgid),
            &ConnItem::Quit => write!(f, "\"QUIT\""),
        }
    }
}
