//! Outgoing feeds.
//!
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::sink::{Sink, SinkExt};
use smartstring::alias::String as SmartString;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio::task;

use crate::bus::{self, Notification};
use crate::config;
use crate::diag::TxSessionStats;
use crate::hostcache;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_codec::{NntpCodec, NntpResponse};
use crate::spool::{ArtLoc, ArtPart, Spool};
use crate::util::Buffer;

// Size of the command channel for the peerfeed.
// The Masterfeed sends articles and notification messages down.
// Connections send notification messages up.
const PEERFEED_COMMAND_CHANNEL_SIZE: usize = 512;

// Size of the broadcast channel from the PeerFeed to the Connections.
// Not too small, as writes to the channel never ever block so
// no backpressure even if there are bursts (which there won't be).
const CONNECTION_BCAST_CHANNEL_SIZE: usize = 64;

// Size of the article queue in a PeerFeed from which the Connections read.
const PEERFEED_QUEUE_SIZE: usize = 5000;

/// Sent to the MasterFeed.
pub struct FeedArticle {
    /// Message-Id.
    pub msgid:    String,
    /// Location in the article spool.
    pub location: ArtLoc,
    /// Article size.
    pub size:     usize,
    /// Peers to feed it to.
    pub peers:    Vec<SmartString>,
}

// Sent from masterfeed -> peerfeed -> connection.
#[derive(Clone)]
enum PeerFeedItem {
    Article(PeerArticle),
    ConnExit(Vec<PeerArticle>),
    ReconfigurePeer(Peer),
    Reconfigure,
    ExitGraceful,
    ExitNow,
    Ping,
}

// Article queued for an outgoing connection.
#[derive(Clone)]
struct PeerArticle {
    // Message-Id.
    msgid:    String,
    // Location in the article spool.
    location: ArtLoc,
    // Size
    size:     usize,
}

/// Fans out articles to the peers.
///
/// Receives articles from the incoming feed, and then fans them
/// out over all PeerFeeds that want the article.
///
pub struct MasterFeed {
    art_chan:  mpsc::Receiver<FeedArticle>,
    bus:       bus::Receiver,
    newsfeeds: Arc<NewsFeeds>,
    peerfeeds: HashMap<SmartString, mpsc::Sender<PeerFeedItem>>,
    orphans:   Vec<mpsc::Sender<PeerFeedItem>>,
    spool:     Spool,
}

impl MasterFeed {
    /// Create a new masterfeed.
    pub async fn new(art_chan: mpsc::Receiver<FeedArticle>, bus: bus::Receiver, spool: Spool) -> MasterFeed {
        log::debug!("new MasterFeed created");
        let mut masterfeed = MasterFeed {
            art_chan,
            bus,
            newsfeeds: config::get_newsfeeds(),
            peerfeeds: HashMap::new(),
            orphans: Vec::new(),
            spool,
        };
        masterfeed.reconfigure(false).await;
        masterfeed
    }

    // Add/remove peers.
    async fn reconfigure(&mut self, send_reconfigure: bool) {
        log::debug!("MasterFeed::reconfigure called");
        self.newsfeeds = config::get_newsfeeds();

        // Find out what peers from self.peerfeeds are not in the new
        // newsfeed, and remove them.
        let mut removed: HashSet<_> = self.peerfeeds.keys().map(|s| s.as_str().to_owned()).collect();
        for peer in &self.newsfeeds.peers {
            removed.remove(&peer.label);
        }
        for peer in removed.iter() {
            if let Some(mut orphan) = self.peerfeeds.remove(peer.as_str()) {
                // notify NewsPeer to shut down.
                match orphan.send(PeerFeedItem::ExitGraceful).await {
                    Ok(_) => self.orphans.push(orphan),
                    Err(e) => {
                        // already gone. cannot happen, but expect the
                        // unexpected and log it anyway.
                        log::error!(
                            "MasterFeed::reconfigure: removed {}, but it was already vanished: {}",
                            peer,
                            e
                        );
                    },
                }
            }
        }

        // Before adding new peers, send all existing peers a Reconfigure.
        if send_reconfigure {
            for peer in &self.newsfeeds.peers {
                if let Some(chan) = self.peerfeeds.get_mut(peer.label.as_str()) {
                    if let Err(e) = chan.send(PeerFeedItem::ReconfigurePeer(Peer::new(peer))).await {
                        log::error!(
                            "MasterFeed::reconfigure: internal error: send to PeerFeed({}): {}",
                            peer.label,
                            e
                        );
                    }
                }
            }
        }

        // Now add new peers.
        for peer in &self.newsfeeds.peers {
            let peer_feed = PeerFeed::new(Peer::new(peer), &self.spool);
            let tx_chan = peer_feed.get_tx_chan();
            tokio::spawn(async move { peer_feed.run().await });
            self.peerfeeds.insert(peer.label.clone().into(), tx_chan);
        }
    }

    /// The actual task. This reads from the receiver channel and fans out
    /// the messages over all peerfeeds.
    ///
    pub async fn run(&mut self) {
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
                            self.broadcast(PeerFeedItem::ExitGraceful).await;
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
                            self.broadcast(PeerFeedItem::ExitNow).await;
                            return;
                        }
                        Some(Notification::Reconfigure) => {
                            // The "newsfeeds" file might have been updated.
                            log::debug!("MasterFeed: reconfigure event");
                            self.reconfigure(true).await;
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
                    size:     art.size,
                };
                if let Err(e) = peerfeed.send(PeerFeedItem::Article(peer_art)).await {
                    log::error!(
                        "MasterFeed::fanout: internal error: send to PeerFeed({}): {}",
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
                log::error!(
                    "MasterFeed::broadcast: internal error: send to PeerFeed({}): {}",
                    name,
                    e
                );
            }
        }

        // And the orphans. If it's not an ExitNow, just send a Ping.
        let item = match item {
            x @ PeerFeedItem::ExitNow => x,
            _ => PeerFeedItem::Ping,
        };
        let mut orphans = Vec::new();
        for mut peer in self.orphans.drain(..) {
            // Only retain if it hasn't gone away yet.
            if let Ok(_) = peer.send(item.clone()).await {
                orphans.push(peer);
            }
        }
        self.orphans = orphans;
    }
}

// A shorter version of newsfeeds::NewsPeer.
#[derive(Clone, PartialEq, Eq)]
struct Peer {
    label:         String,
    outhost:       String,
    bindaddress:   Option<IpAddr>,
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
    rx_queue: async_channel::Receiver<PeerArticle>,
    tx_queue: async_channel::Sender<PeerArticle>,

    // broadcast channel to all Connections.
    broadcast: broadcast::Sender<PeerFeedItem>,

    // A capacity-of-one channel that is used to signal that
    // the article queue is empty. We can then fill it with
    // data from the backlog (if we have a backlog).
    tx_empty: mpsc::Sender<()>,
    rx_empty: mpsc::Receiver<()>,

    num_conns: u32,

    // Spool instance.
    spool: Spool,
}

impl PeerFeed {
    /// Create a new PeerFeed.
    fn new(peer: Peer, spool: &Spool) -> PeerFeed {
        let (tx_chan, rx_chan) = mpsc::channel::<PeerFeedItem>(PEERFEED_COMMAND_CHANNEL_SIZE);
        let (broadcast, _) = broadcast::channel(CONNECTION_BCAST_CHANNEL_SIZE);
        let (tx_queue, rx_queue) = async_channel::bounded(PEERFEED_QUEUE_SIZE);
        let (tx_empty, rx_empty) = mpsc::channel::<()>(1);

        PeerFeed {
            label: peer.label.clone(),
            rx_chan,
            tx_chan,
            rx_queue,
            tx_queue,
            broadcast,
            tx_empty,
            rx_empty,
            num_conns: 0,
            spool: spool.clone(),
            newspeer: Arc::new(peer),
        }
    }

    // Get a clone of tx_chan. Masterfeed calls this.
    fn get_tx_chan(&self) -> mpsc::Sender<PeerFeedItem> {
        self.tx_chan.clone()
    }

    /// Run the PeerFeed.
    async fn run(mut self) {
        let mut closed = false;
        let mut exiting = false;

        // NOTE: since we have a clone of tx_chan ourselves, this will loop forever.
        // Or at least until we receive ExitGraceful or ExitNow.

        while let Some(item) = self.rx_chan.recv().await {
            if exiting && self.num_conns == 0 {
                break;
            }

            match item {
                PeerFeedItem::Article(art) => {
                    // if we have no connections, or less than maxconn, create a connection here.
                    if self.num_conns < self.newspeer.maxparallel {
                        self.add_connection().await;
                    }

                    loop {
                        match self.tx_queue.try_send(art) {
                            Ok(()) => break,
                            Err(async_channel::TrySendError::Closed(_art)) => {
                                // should never happen.
                                if !closed {
                                    log::error!("PeerFeed::run: {}: async_channel closed ?!", self.label);
                                    closed = true;
                                }
                                // XXX TODO: full. send article to the backlog.
                                //self.send_art_to_backlog(art);
                                break;
                            },
                            Err(async_channel::TrySendError::Full(_art)) => {
                                // XXX TODO: full. send article and half of the queue to the backlog.
                                //self.send_art_to_backlog(art)
                                //self.send_queue_to_backlog(false)
                                //continue;
                                break;
                            },
                        }
                    }
                },
                PeerFeedItem::ExitGraceful => {
                    exiting = true;
                    let _ = self.broadcast.send(PeerFeedItem::ExitGraceful);
                },
                PeerFeedItem::ExitNow => {
                    exiting = true;
                    let _ = self.broadcast.send(PeerFeedItem::ExitNow);
                },
                PeerFeedItem::Reconfigure => {},
                PeerFeedItem::ReconfigurePeer(peer) => {
                    if &peer != self.newspeer.as_ref() {
                        self.newspeer = Arc::new(peer);
                        let _ = self.broadcast.send(PeerFeedItem::Reconfigure);
                    }
                },
                PeerFeedItem::ConnExit(arts) => {
                    // XXX TODO handle returned arts.
                    //self.requeue(arts);
                    self.num_conns -= 1;
                },
                PeerFeedItem::Ping => {},
            }
        }

        // save queue to disk.
        // XXX TODO
        //self.send_queue_to_backlog(true);
    }

    async fn add_connection(&mut self) {
        let id = 1;
        let newspeer = self.newspeer.clone();
        let mut tx_chan = self.tx_chan.clone();
        let rx_queue = self.rx_queue.clone();
        let spool = self.spool.clone();
        let broadcast = self.broadcast.clone();
        let tx_empty = self.tx_empty.clone();

        self.num_conns += 1;

        // We spawn a new Connection task.
        task::spawn(async move {
            match Connection::new(
                id,
                newspeer,
                tx_chan.clone(),
                rx_queue,
                broadcast,
                tx_empty,
                spool,
            )
            .await
            {
                Ok(conn) => {
                    // On succes, we start talking nntp.
                    drop(tx_chan);
                    conn.run().await;
                },
                Err(_) => {
                    // notify PeerFeed that we failed.
                    let _ = tx_chan.send(PeerFeedItem::ConnExit(Vec::new()));
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
    id:         u32,
    // IP address we're connected to.
    ipaddr:     IpAddr,
    // Peer info.
    newspeer:   Arc<Peer>,
    // reader / writer.
    reader:     NntpCodec<Box<dyn AsyncRead + Send + Unpin>>,
    writer:     NntpCodec<Box<dyn AsyncWrite + Send + Unpin>>,
    // Local items waiting to be sent (check -> takethis transition)
    send_queue: VecDeque<ConnItem>,
    // Sent items, waiting for a reply.
    recv_queue: VecDeque<ConnItem>,
    // Dropped items to be pushed onto the backlog.
    dropped:    Vec<ConnItem>,
    // Stats
    stats:      TxSessionStats,
    // Spool.
    spool:      Spool,
    // channel to send information to the PeerFeed.
    tx_chan:    mpsc::Sender<PeerFeedItem>,
    // article queue.
    rx_queue:   async_channel::Receiver<PeerArticle>,
    // broadcast channel to receive notifications from the PeerFeed.
    broadcast:  broadcast::Receiver<PeerFeedItem>,
    // used to wake up the fill-the-queue-from-the-backlog task.
    tx_empty:   mpsc::Sender<()>,
}

impl Connection {
    // Create a new connection.
    async fn new(
        id: u32,
        newspeer: Arc<Peer>,
        tx_chan: mpsc::Sender<PeerFeedItem>,
        rx_queue: async_channel::Receiver<PeerArticle>,
        broadcast: broadcast::Sender<PeerFeedItem>,
        tx_empty: mpsc::Sender<()>,
        spool: Spool,
    ) -> io::Result<Connection>
    {
        let mut broadcast_rx = broadcast.subscribe();

        log::info!(
            "outfeed: {}:{}: connecting to {}",
            newspeer.label,
            id,
            newspeer.outhost
        );

        // Start connecting, but also listen to broadcasts while connecting.
        loop {
            tokio::select! {
                conn = Connection::connect(newspeer.as_ref()) => {
                    let (codec, ipaddr, connect_msg) = conn.map_err(|e| {
                        ioerr!(e.kind(), "{}:{}: {}", newspeer.label, id, e)
                    })?;

                    // Build and return a new Connection struct.
                    let (reader, writer) = codec.split();
                    let mut conn = Connection {
                        id,
                        ipaddr,
                        newspeer,
                        reader,
                        writer,
                        send_queue: VecDeque::new(),
                        recv_queue: VecDeque::new(),
                        dropped: Vec::new(),
                        stats: TxSessionStats::default(),
                        spool,
                        tx_chan,
                        rx_queue,
                        broadcast: broadcast_rx,
                        tx_empty,
                    };

                    // Initialize stats logger and log connect message.
                    conn.stats.on_connect(&conn.newspeer.label, id, &conn.newspeer.outhost, conn.ipaddr,  &connect_msg);
                    return Ok(conn);
                }
                item = broadcast_rx.recv() => {
                    // if any of these events happen, cancel the connect.
                    match item {
                        Ok(PeerFeedItem::Reconfigure) |
                        Ok(PeerFeedItem::ExitGraceful) |
                        Ok(PeerFeedItem::ExitNow) |
                        Err(_) => {
                            return Err(ioerr!(ConnectionAborted, "{}:{}: connection cancelled", newspeer.label, id));
                        },
                        _ => {},
                    }
                }
            }
        }
    }

    // Spawn Self as a separate task.
    async fn run(mut self) {
        let _ = tokio::spawn(async move {
            // call feeder loop.
            if let Err(e) = self.feed().await {
                log::error!("outfeed: {}:{}: fatal: {}", self.newspeer.label, self.id, e);
            }

            // log stats.
            self.stats.stats_final();

            // return remaining articles that weren't sent.
            let mut arts = Vec::new();
            for item in self
                .send_queue
                .iter()
                .chain(self.recv_queue.iter())
                .chain(self.dropped.iter())
            {
                match item {
                    &ConnItem::Check(ref art) | &ConnItem::Takethis(ref art) => {
                        arts.push(art.clone());
                    },
                    _ => {},
                }
            }
            let _ = self.tx_chan.send(PeerFeedItem::ConnExit(arts)).await;
        });
    }

    // Connect to remote peer.
    async fn connect(peer: &Peer) -> io::Result<(NntpCodec, IpAddr, String)> {
        // A lookup of the hostname might return multiple addresses.
        // We're not sure of the order that addresses are returned in,
        // so sort IPv6 before IPv4 but otherwise keep the order
        // intact.
        let addrs = match hostcache::RESOLVER.lookup_ip(peer.outhost.as_str()).await {
            Ok(lookupip) => {
                let addrs: Vec<SocketAddr> = lookupip.iter().map(|a| SocketAddr::new(a, peer.port)).collect();
                let v6 = addrs.iter().filter(|a| a.is_ipv6()).cloned();
                let v4 = addrs.iter().filter(|a| a.is_ipv4()).cloned();
                let mut addrs2 = Vec::new();
                addrs2.extend(v6);
                addrs2.extend(v4);
                addrs2
            },
            Err(e) => return Err(ioerr!(Other, e)),
        };

        // Try to connect to the peer.
        let mut last_err = None;
        for addr in &addrs {
            let result = async move {
                // Create socket.
                let is_ipv6 = peer
                    .bindaddress
                    .map(|ref a| a.is_ipv6())
                    .unwrap_or(addr.is_ipv6());
                let domain = if is_ipv6 {
                    socket2::Domain::ipv6()
                } else {
                    socket2::Domain::ipv4()
                };
                let socket = socket2::Socket::new(domain, socket2::Type::stream(), None).map_err(|e| {
                    log::trace!("Connection::connect: Socket::new({:?}): {}", domain, e);
                    e
                })?;

                // Set IPV6_V6ONLY if this is going to be an IPv6 connection.
                if is_ipv6 {
                    socket.set_only_v6(true).map_err(|_| {
                        log::trace!("Connection::connect: Socket.set_only_v6() failed");
                        ioerr!(AddrNotAvailable, "socket.set_only_v6() failed")
                    })?;
                }

                // Bind local address.
                if let Some(ref bindaddr) = peer.bindaddress {
                    let sa = SocketAddr::new(bindaddr.to_owned(), 0);
                    socket.bind(&sa.clone().into()).map_err(|e| {
                        log::trace!("Connection::connect: Socket::bind({:?}): {}", sa, e);
                        ioerr!(e.kind(), "bind {}: {}", sa, e)
                    })?;
                }

                // Now this sucks, having to run it on a threadpool.
                log::trace!("Trying to connect to {:?}", addr);
                let addr2: socket2::SockAddr = addr.to_owned().into();
                let res = task::spawn_blocking(move || {
                    // 10 second timeout for a connect is more than enough.
                    socket.connect_timeout(&addr2, Duration::new(10, 0))?;
                    Ok(socket)
                })
                .await
                .unwrap_or_else(|e| Err(ioerr!(Other, "spawn_blocking: {}", e)));
                let socket = res.map_err(|e| {
                    log::trace!("Connection::connect({}): {}", addr, e);
                    ioerr!(e.kind(), "{}: {}", addr, e)
                })?;

                // Now turn it into a tokio::net::TcpStream.
                let socket = TcpStream::from_std(socket.into()).unwrap();

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
                let connect_msg = resp.short().to_string();

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

                Ok((codec, connect_msg))
            }
            .await;

            // On success, return. Otherwise, save the error.
            match result {
                Ok((codec, connect_msg)) => return Ok((codec, addr.ip(), connect_msg)),
                Err(e) => last_err = Some(e),
            }
        }

        // Return the last error seen.
        Err(last_err.unwrap())
    }

    // Feeder loop.
    async fn feed(&mut self) -> io::Result<()> {
        let mut xmit_busy = false;
        let mut maxstream = self.newspeer.maxstream as usize;

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
            let mut need_item = !xmit_busy && queue_len < maxstream;

            if need_item {
                // Try to get one item from the queue. If it's empty,
                // signal the PeerFeed that we hit an empty queue.
                match self.rx_queue.try_recv() {
                    Ok(art) => {
                        self.send_queue.push_back(ConnItem::Check(art));
                        need_item = queue_len + 1 < maxstream;
                    },
                    Err(async_channel::TryRecvError::Empty) => {
                        let _ = self.tx_empty.try_send(());
                    },
                    _ => {},
                }
            }

            tokio::select! {

                // If we need to, get an item from the global queue for this feed.
                res = self.rx_queue.recv(), if need_item => {
                    match res {
                        Ok(art) => self.send_queue.push_back(ConnItem::Check(art)),
                        Err(_) => {
                            // channel closed. shutdown in progress.
                            // drop anything in the send_queue and send quit.
                            self.dropped.extend(self.send_queue.drain(..));
                            self.send_queue.push_back(ConnItem::Quit);
                            // this will make sure that need_item == false.
                            maxstream = 0;
                        },
                    }
                }

                // check for notifications from the broadcast channel.
                res = self.broadcast.recv() => {
                    match res {
                        Ok(PeerFeedItem::ExitNow) => {
                            return Err(ioerr!(Interrupted, "forced exit"));
                        }
                        Ok(PeerFeedItem::ExitGraceful) => {
                            // exit gracefully.
                            self.dropped.extend(self.send_queue.drain(..));
                            self.send_queue.push_back(ConnItem::Quit);
                            maxstream = 0;
                        },
                        Ok(PeerFeedItem::Reconfigure) => {
                            // config changed. drain slowly and quit.
                            log::info!("outfeed: {}:{}: reconfigure", self.newspeer.label, self.id);
                            self.send_queue.push_back(ConnItem::Quit);
                            maxstream = 0;
                        },
                        Err(broadcast::RecvError::Lagged(num)) => {
                            // what else can we do ?
                            return Err(ioerr!(TimedOut, "missed too many messages ({}) on bus", num));
                        },
                        Err(broadcast::RecvError::Closed) => {
                            // what else can we do?
                            return Err(ioerr!(TimedOut, "bus unexpectedly closed"));
                        },
                        _ => {},
                    }
                }

                // If we're writing, keep driving it.
                res = self.writer.flush(), if xmit_busy => {
                    // Done sending either CHECK or TAKETHIS.
                    if let Err(e) = res {
                        return Err(ioerr!(e.kind(), "writing to socket: {}", e));
                    }
                    xmit_busy = false;
                }

                // process a reply from the remote server.
                res = self.reader.next() => {
                    // Remap None (end of stream) to EOF.
                    let res = res.unwrap_or_else(|| Err(ioerr!(UnexpectedEof, "Connection closed")));

                    // What did we receive?
                    match res.and_then(NntpResponse::try_from) {
                        Err(e) => {
                            if e.kind() == io::ErrorKind::UnexpectedEof {
                                if self.recv_queue.len() == 0 {
                                    log::info!("outfeed: {}:{}: connection closed by remote", self.newspeer.label, self.id);
                                    return Ok(());
                                }
                                return Err(ioerr!(e.kind(), "connection closed unexpectedly"));
                            }
                            return Err(ioerr!(e.kind(), "reading from socket: {}", e));
                        },
                        Ok(resp) => {
                            // Got a valid reply. Find matching command.
                            let mut unexpected = false;
                            match self.recv_queue.pop_front() {
                                None => {
                                    if resp.code == 400 {
                                        log::info!("outfeed: {}:{}: connection closed by remote ({})", self.newspeer.label, self.id, resp.short());
                                        return Ok(());
                                    }
                                    return Err(ioerr!(InvalidData, "unsollicited response: {}", resp.short()));
                                },
                                Some(ConnItem::Check(art)) => {
                                    match resp.code {
                                        238 => {
                                            self.send_queue.push_back(ConnItem::Takethis(art));
                                        },
                                        431 => {
                                            // XXX TODO req-queue article.
                                            //self.stats.art_deferred(None);
                                            self.stats.art_deferred_fail(None); // XXX
                                        },
                                        438 => {
                                            self.stats.art_refused();
                                        },
                                        _ => unexpected = true,
                                    }
                                },
                                Some(ConnItem::Takethis(art)) => {
                                    match resp.code {
                                        239 => {
                                            self.stats.art_accepted(art.size);
                                            self.send_queue.push_back(ConnItem::Takethis(art));
                                        },
                                        431 => {
                                            // this is an invalid TAKETHIS reply!
                                            // XXX TODO req-queue article.
                                            //self.stats.art_deferred(Some(art.size));
                                            self.stats.art_deferred_fail(Some(art.size)); // XXX
                                        },
                                        439 => {
                                            self.stats.art_rejected(art.size);
                                        },
                                        _ => unexpected = true,
                                    }
                                },
                                Some(ConnItem::Quit) => {
                                    return Ok(());
                                },
                            }
                            if unexpected {
                                return Err(ioerr!(InvalidData, "unexpected response: {}", resp.short()));
                            }
                        },
                    }
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
                        if e.kind() == io::ErrorKind::NotFound {
                            self.stats.art_notfound();
                            return Ok(());
                        }
                        return Err(e);
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
