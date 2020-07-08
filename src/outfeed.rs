use std::collections::{HashMap, HashSet, VecDeque};
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
use tokio::sync::{broadcast, mpsc};
use tokio::task;

use crate::bus::{self, Notification};
use crate::config;
use crate::diag::SessionStats;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_codec::{NntpCodec, NntpResponse};
use crate::spool::{ArtLoc, ArtPart, Spool};
use crate::util::Buffer;

/// Sent to the MasterFeed.
pub struct FeedArticle {
    // Message-Id.
    msgid:    String,
    // Location in the article spool.
    location: ArtLoc,
    // Peers to feed it to.
    peers:    Vec<String>,
}

// Sent from masterfeed -> peerfeed -> connection.
#[derive(Clone)]
enum PeerFeedItem {
    Article(PeerArticle),
    ConnExit,
    Reconfigure,
    ExitGraceful,
    ExitNow,
}

// Article in the peerfeed queue.
#[derive(Clone)]
struct PeerArticle {
    // Message-Id.
    msgid:    String,
    // Location in the article spool.
    location: ArtLoc,
}

// Masterfeed.
//
// Receives articles from the incoming feed, and then fans them
// out over all PeerFeeds that want the article.
//
pub struct MasterFeed {
    art_chan:  mpsc::Receiver<FeedArticle>,
    bus:       bus::Receiver,
    newsfeeds: Arc<NewsFeeds>,
    peerfeeds: HashMap<String, mpsc::Sender<PeerFeedItem>>,
    spool:     Spool,
}

impl MasterFeed {
    /// Create a new masterfeed.
    pub fn new(art_chan: mpsc::Receiver<FeedArticle>, bus: bus::Receiver, spool: Spool) -> MasterFeed {
        let mut masterfeed = MasterFeed {
            art_chan,
            bus,
            newsfeeds: config::get_newsfeeds(),
            peerfeeds: HashMap::new(),
            spool,
        };
        masterfeed.reconfigure();
        masterfeed
    }

    // Add/remove peers.
    fn reconfigure(&mut self) {
        log::debug!("MasterFeed::reconfigure called");
        self.newsfeeds = config::get_newsfeeds();

        // Find out what peers from self.peerfeeds are not in the new
        // newsfeed, and remove them.
        let mut removed: HashSet<_> = self.peerfeeds.keys().cloned().collect();
        for peer in &self.newsfeeds.peers {
            removed.remove(&peer.label);
        }
        for peer in removed.iter() {
            self.peerfeeds.remove(peer);
        }

        // Now add new peers.
        for peer in &self.newsfeeds.peers {
            let peer_feed = PeerFeed::new(peer, &self.newsfeeds, &self.spool);
            let tx_chan = peer_feed.tx_chan.clone();
            tokio::spawn(async move { peer_feed.run().await });
            self.peerfeeds.insert(peer.label.clone(), tx_chan);
        }
    }

    /// The actual task. This reads from the receiver channel and fans out
    /// the messages over all peerfeeds.
    ///
    pub async fn run(mut self) {
        log::debug!("MasterFeed::run: starting");
        let mut recv_arts = true;

        loop {
            tokio::select! {
                article = self.art_chan.next(), if recv_arts => {
                    let art = match article {
                        Some(article) => article,
                        None => {
                            // Hitting end-of-stream on the art_chan means that
                            // all incoming connections are gone. Translate that
                            // into an `ExitGraceful` for the PeerFeeds.
                            log::debug!("MasterFeed: hit end-of-stream on article channel");
                            let _ = self.broadcast(PeerFeedItem::ExitGraceful).await;
                            recv_arts = false;
                            continue;
                        }
                    };
                    // Forwards only to the peerfeeds in the list.
                    self.fanout(art).await;
                }
                notification = self.bus.recv() => {
                    match notification {
                        Some(Notification::ExitNow) | None => {
                            // Time's up!
                            log::debug!("MasterFeed: broadcasting ExitNow to peerfeeds");
                            let _ = self.broadcast(PeerFeedItem::ExitNow).await;
                            return;
                        }
                        Some(Notification::Reconfigure) => {
                            // The "newsfeeds" file might have been updated.
                            log::debug!("MasterFeed: reconfigure event");
                            self.reconfigure();
                            self.broadcast(PeerFeedItem::Reconfigure).await;
                        }
                        _ => {},
                    }
                }
            }
        }
    }

    // Send article to all peerfeeds that want it.
    async fn fanout(&mut self, art: FeedArticle) {
        for peername in &art.peers {
            if let Some(peerfeed) = self.peerfeeds.get_mut(peername) {
                let peer_art = PeerArticle {
                    msgid:    art.msgid.clone(),
                    location: art.location.clone(),
                };
                if let Err(e) = peerfeed.send(PeerFeedItem::Article(peer_art)).await {
                    log::warn!(
                        "MasterFeed::run: internal error: send to PeerFeed({}): {}",
                        peername,
                        e
                    );
                }
            }
        }
    }

    async fn broadcast(&mut self, item: PeerFeedItem) {
        // Forward to all peerfeeds.
        for (name, peer) in self.peerfeeds.iter_mut() {
            if let Err(e) = peer.send(item.clone()).await {
                log::warn!(
                    "MasterFeed::run: internal error: send to PeerFeed({}): {}",
                    name,
                    e
                );
            }
        }
    }
}

// A shorter version of newsfeeds::NewsPeer.
struct Peer {
    label:         String,
    outhost:       String,
    bindaddress:   String,
    port:          u16,
    maxparallel:   u32,
    maxstream:     u32,
    nobatch:       bool,
    maxqueue:      u32,
    headfeed:      bool,
    genlines:      bool,
    preservebytes: bool,
}

impl Peer {
    fn new(nfpeer: &NewsPeer) -> Peer {
        Peer {
            label:         nfpeer.label.clone(),
            outhost:       nfpeer.outhost.clone(),
            bindaddress:   nfpeer.bindaddress.clone(),
            port:          nfpeer.port,
            maxparallel:   nfpeer.maxparallel,
            nobatch:       nfpeer.nobatch,
            maxqueue:      nfpeer.maxqueue,
            maxstream:     nfpeer.maxstream,
            headfeed:      nfpeer.headfeed,
            genlines:      nfpeer.genlines,
            preservebytes: nfpeer.preservebytes,
        }
    }
}

// Newsfeed to an NNTP peer.
//
// A peerfeed contains an in memory queue, an on-disk backlog queue,
// and zero or more active NNTP connections to remote peers.
//
struct PeerFeed {
    // Name.
    label: String,

    // Reference to newsfeed config.
    newsfeeds: Arc<NewsFeeds>,

    // The relevant parts of newsfeed::NewsPeer.
    newspeer: Arc<Peer>,

    // The peerfeed listens on this channel for messages from:
    // - MasterFeed: articles, notifications.
    // - Connection: notifications.
    rx_chan: mpsc::Receiver<PeerFeedItem>,

    // We keep tx_chan around in order to clone it for newly
    // instantiated Connections so they can send notifications to us.
    tx_chan: mpsc::Sender<PeerFeedItem>,

    // MPSC channel that is the article queue for the Connections.
    rx_queue:       async_channel::Receiver<PeerArticle>,
    tx_queue:       async_channel::Sender<PeerArticle>,
    queue_capacity: usize,

    // broadcast channel to all Connections.
    broadcast: broadcast::Sender<PeerFeedItem>,

    // active connections.
    num_conns: u32,

    // Spool instance.
    spool: Spool,
}

impl PeerFeed {
    /// Create a new PeerFeed.
    fn new(nfpeer: &NewsPeer, newsfeeds: &Arc<NewsFeeds>, spool: &Spool) -> PeerFeed {
        let (tx_chan, rx_chan) = mpsc::channel::<PeerFeedItem>(16);
        let (broadcast, _) = broadcast::channel(64);
        let queue_capacity = 10000;
        let (tx_queue, rx_queue) = async_channel::bounded(queue_capacity);
        PeerFeed {
            label: nfpeer.label.clone(),
            newsfeeds: newsfeeds.clone(),
            rx_chan,
            tx_chan,
            rx_queue,
            tx_queue,
            queue_capacity,
            broadcast,
            num_conns: 0,
            spool: spool.clone(),
            newspeer: Arc::new(Peer::new(nfpeer)),
        }
    }

    /// Run the PeerFeed.
    async fn run(mut self) {
        let mut closed = false;

        while let Some(item) = self.rx_chan.recv().await {
            match item {
                PeerFeedItem::Article(art) => {
                    // if we have no connections, or less than maxconn, create a connection here.
                    if self.num_conns < self.newspeer.maxparallel {
                        self.add_connection().await;
                    }

                    //let mut art = art;
                    loop {
                        match self.tx_queue.try_send(art) {
                            Ok(()) => break,
                            Err(async_channel::TrySendError::Closed(_err)) => {
                                // should never happen.
                                if !closed {
                                    log::error!(
                                        "PeerFeed::run: {}: async_channel closed ?!",
                                        self.newspeer.label
                                    );
                                    closed = true;
                                }
                                // XXX TODO: full. send half of the queue to the backlog.
                                //art = err;
                                //continue;
                                break;
                            },
                            Err(async_channel::TrySendError::Full(_err)) => {
                                // XXX TODO: full. send half of the queue to the backlog.
                                //art = err;
                                //continue;
                                break;
                            },
                        }
                    }
                },
                PeerFeedItem::ExitGraceful => {
                    // XXX TODO handle exit
                },
                PeerFeedItem::ExitNow => {
                    // XXX TODO handle exit
                },
                PeerFeedItem::Reconfigure => {
                    // XXX TODO handle reconfigure
                },
                PeerFeedItem::ConnExit => {
                    self.num_conns -= 1;
                },
            }
        }
    }

    async fn add_connection(&mut self) {
        let id = 1;
        let newspeer = self.newspeer.clone();
        let mut tx_chan = self.tx_chan.clone();
        let rx_queue = self.rx_queue.clone();
        let spool = self.spool.clone();
        let broadcast = self.broadcast.clone();

        self.num_conns += 1;

        // We spawn a new Connection task.
        task::spawn(async move {
            match Connection::new(id, newspeer, tx_chan.clone(), rx_queue, broadcast, spool).await {
                Ok(conn) => {
                    // On succes, we start talking nntp.
                    drop(tx_chan);
                    conn.run().await;
                },
                Err(_) => {
                    // notify PeerFeed that we failed.
                    let _ = tx_chan.send(PeerFeedItem::ConnExit);
                },
            }
        });
    }
}

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
    // Unique identifier.
    id:          u32,
    // Peer info.
    newspeer:    Arc<Peer>,
    // reader / writer.
    reader:      NntpCodec<Box<dyn AsyncRead + Send + Unpin>>,
    writer:      NntpCodec<Box<dyn AsyncWrite + Send + Unpin>>,
    // Local items waiting to be sent (check -> takethis transition)
    send_queue:  VecDeque<ConnItem>,
    // Sent items, waiting for a reply.
    recv_queue:  VecDeque<ConnItem>,
    // Stats
    stats:       SessionStats,
    // Set after we have sent QUIT
    sender_done: bool,
    // Spool.
    spool:       Spool,
    // channel to send information to the PeerFeed.
    tx_chan:     mpsc::Sender<PeerFeedItem>,
    // article queue.
    rx_queue:    async_channel::Receiver<PeerArticle>,
    // broadcast channel to receive notifications from the PeerFeed.
    broadcast:   broadcast::Receiver<PeerFeedItem>,
}

impl Connection {
    // Create a new connection.
    async fn new(
        id: u32,
        newspeer: Arc<Peer>,
        tx_chan: mpsc::Sender<PeerFeedItem>,
        rx_queue: async_channel::Receiver<PeerArticle>,
        broadcast: broadcast::Sender<PeerFeedItem>,
        spool: Spool,
    ) -> io::Result<Connection>
    {
        // First, connect.
        let codec = Connection::connect(&newspeer.outhost)
            .await
            .map_err(|e| ioerr!(e.kind(), "{}: {}", newspeer.label, e))?;

        // Build and return a new Connection struct.
        let (reader, writer) = codec.split();
        Ok(Connection {
            id,
            newspeer,
            reader,
            writer,
            send_queue: VecDeque::new(),
            recv_queue: VecDeque::new(),
            stats: SessionStats::default(),
            sender_done: false,
            spool,
            tx_chan,
            rx_queue,
            broadcast: broadcast.subscribe(),
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
        // in, so sort IPv6 before IPv4 but otherwise keep the order
        // intact.
        let addrs = match tokio::net::lookup_host(outhost).await {
            Ok(addr_iter) => {
                let addrs: Vec<std::net::SocketAddr> = addr_iter.collect();
                let v6 = addrs.iter().filter(|a| a.is_ipv6()).cloned();
                let v4 = addrs.iter().filter(|a| a.is_ipv4()).cloned();
                let mut addrs2 = Vec::new();
                addrs2.extend(v6);
                addrs2.extend(v4);
                addrs2
            },
            Err(e) => return Err(e),
        };

        // Try to connect to the peer.
        let mut last_err = None;
        for addr in &addrs {
            let result = async move {
                // Connect.
                log::trace!("Trying to connect to {:?}", addr);
                let socket = TcpStream::connect(addr).await.map_err(|e| {
                    log::trace!("connect {:?}: {}", addr, e);
                    ioerr!(e.kind(), "{}: {}", addr, e)
                })?;

                // Create codec from socket.
                let mut codec = NntpCodec::builder(socket)
                    .read_timeout(30)
                    .write_timeout(60)
                    .build();

                // Read initial response code.
                let resp = codec.read_response().await.map_err(|e| {
                    log::trace!("{:?} read_response: {}", addr, e);
                    ioerr!(e.kind(), "{}: {}", addr, e)
                })?;
                log::trace!("<< {}", resp.short());
                if resp.code != 200 {
                    Err(ioerr!(
                        InvalidData,
                        "{}: initial response {}, expected 200",
                        addr,
                        resp.code
                    ))?;
                }

                // Send MODE STREAM.
                log::trace!(">> MODE STREAM");
                let resp = codec
                    .command("MODE STREAM")
                    .await
                    .map_err(|e| ioerr!(e.kind(), "{}: {}", addr, e))?;
                log::trace!("<< {}", resp.short());
                if resp.code != 203 {
                    Err(ioerr!(
                        InvalidData,
                        "{}: MODE STREAM response {}, expected 203",
                        addr,
                        resp.code
                    ))?;
                }

                Ok(codec)
            }
            .await;

            // On success, return. Otherwise, save the error.
            match result {
                Ok(codec) => return Ok(codec),
                Err(e) => last_err = Some(e),
            }
        }

        // Return the last error seen.
        Err(last_err.unwrap())
    }

    async fn feed(&mut self) {
        if let Err(e) = self.feed2().await {
            log::error!("Connection::feed: exit: {}", e);
        }
        // XXX TODO include remaining articles that weren't sent.
        let _ = self.tx_chan.send(PeerFeedItem::ConnExit).await;
    }

    // Feeder loop.
    async fn feed2(&mut self) -> io::Result<()> {
        let mut xmit_busy = false;
        let mut bcast_listen = true;

        loop {
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

            let queue_len = self.recv_queue.len() + self.send_queue.len();
            let need_item = !xmit_busy && queue_len < self.newspeer.maxstream as usize;

            // XXX TODO
            //  if need_item && self.rx_queue.len() == 0 {
            //      if !self.shared.no_backlog {
            //          self.tx_chan.send(PeerFeedItem::NeedMoreInput);
            //      }
            //  }
            tokio::select! {

                // If we need to, get an item from the global queue for this feed.
                res = self.rx_queue.recv(), if need_item => {
                    match res {
                        Ok(art) => self.send_queue.push_back(ConnItem::Check(art)),
                        Err(_) => {
                            // channel closed. shutdown in progress. send quit.
                            // XXX TODO: update stats, log, return
                            return Ok(());
                        },
                    }
                }

                // check for notifications from the broadcast channel.
                res = self.broadcast.recv(), if bcast_listen => {
                    match res {
                        Ok(PeerFeedItem::ExitGraceful) => {
                            // XXX TODO graceful exit.
                            break;
                        },
                        Ok(PeerFeedItem::ExitNow) => {
                            // XXX TODO ungraceful exit.
                            break;
                        },
                        Ok(PeerFeedItem::Reconfigure) => {
                            // XXX TODO graceful exit.
                            break;
                        },
                        Err(broadcast::RecvError::Lagged(_)) => {
                            // what else can we do ?
                            break;
                        },
                        Err(broadcast::RecvError::Closed) => {
                            // what else can we do?
                            bcast_listen = false;
                        },
                        _ => {},
                    }
                }

                // If we're writing, keep driving it.
                res = self.writer.flush(), if xmit_busy => {
                    // Done sending either CHECK or TAKETHIS.
                    if let Err(e) = res {
                        // XXX TODO: update stats, log, return
                        panic!("transmit: {}", e);
                    }
                    xmit_busy = false;
                }

                // process a reply from the remote server.
                res = self.reader.next() => {
                    // What did we receive?
                    match res.unwrap().and_then(NntpResponse::try_from) {
                        Err(e) => {
                            // XXX TODO: update stats, log, return
                            panic!("transmit: {}", e);
                        },
                        Ok(resp) => {
                            // Got a valid reply. Find matching command.
                            match self.recv_queue.pop_front() {
                                None => panic!("recv queue out of sync"),
                                Some(ConnItem::Check(art)) => {
                                    // TODO check resp status code, update stats.
                                    self.send_queue.push_back(ConnItem::Takethis(art));
                                },
                                Some(ConnItem::Takethis(art)) => {
                                    // TODO check resp status code, send article, update stats.
                                },
                                Some(ConnItem::Quit) => {
                                    // TODO check resp status code, update stats, return.
                                },
                            }
                        },
                    }
                }
            }
        }
        Ok(())
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
