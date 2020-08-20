//! Outgoing feeds.
//!
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::sink::{Sink, SinkExt};
use smartstring::alias::String as SmartString;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tokio::time::delay_for;

use crate::article::{HeaderName, HeadersParser};
use crate::bus::{self, Notification};
use crate::config;
use crate::diag::TxSessionStats;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_client;
use crate::nntp_codec::{NntpCodec, NntpResponse};
use crate::queue::{Queue, QItems};
use crate::server;
use crate::spool::{ArtLoc, ArtPart, Spool, SpoolArt};
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
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub struct PeerArticle {
    // Message-Id.
    pub msgid:    String,
    // Location in the article spool.
    pub location: ArtLoc,
    // Size
    pub size:     usize,
    // Did it come from the backlog
    pub from_backlog: bool,

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
    pub async fn new(bus: bus::Receiver, spool: Spool) -> (MasterFeed, mpsc::Sender<FeedArticle>) {
        log::debug!("new MasterFeed created");
        let (art_chan_tx, art_chan) = mpsc::channel(1000);
        let mut masterfeed = MasterFeed {
            art_chan,
            bus,
            newsfeeds: config::get_newsfeeds(),
            peerfeeds: HashMap::new(),
            orphans: Vec::new(),
            spool,
        };
        masterfeed.reconfigure(false).await;
        (masterfeed, art_chan_tx)
    }

    // Add/remove peers.
    async fn reconfigure(&mut self, send_reconfigure: bool) {
        log::debug!("MasterFeed::reconfigure called");
        self.newsfeeds = config::get_newsfeeds();

        // Find out what peers from self.peerfeeds are not in the new
        // newsfeed, and remove them.
        let mut removed: HashSet<_> = self.peerfeeds.keys().map(|s| s.as_str().to_owned()).collect();
        for peer in &self.newsfeeds.peers {
            removed.remove(peer.label.as_str());
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
            if !self.peerfeeds.contains_key(&peer.label) && peer.outhost != "" {
                let peer_feed = PeerFeed::new(Peer::new(peer), &self.spool);
                peer_feed.queue.init().await;
                let tx_chan = peer_feed.get_tx_chan();
                tokio::spawn(async move { peer_feed.run().await });
                self.peerfeeds.insert(peer.label.clone().into(), tx_chan);
            }
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
                        Some(Notification::ExitGraceful) | None => {
                            log::debug!("MasterFeed: broadcasting ExitGraceful to peerfeeds");
                            self.broadcast(PeerFeedItem::ExitGraceful).await;
                        }
                        Some(Notification::ExitNow) => {
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
                    from_backlog: false,
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
                match item {
                    PeerFeedItem::ExitNow => {},
                    _ => {
                        log::error!(
                            "MasterFeed::broadcast: internal error: send to PeerFeed({}): {}",
                            name,
                            e
                        );
                    },
                }
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
#[derive(Clone, Debug, PartialEq, Eq)]
struct Peer {
    label:         SmartString,
    outhost:       String,
    bindaddress:   Option<IpAddr>,
    port:          u16,
    maxparallel:   u32,
    maxstream:     u32,
    nobatch:       bool,
    maxqueue:      u32,
    headfeed:      bool,
    preservebytes: bool,
    queue_only:    bool,
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
            headfeed:      nfpeer.send_headfeed,
            preservebytes: nfpeer.preservebytes,
            queue_only:    nfpeer.queue_only,
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
    label: SmartString,

    // Unique id for every connection.
    next_id: u64,

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

    // Number of active outgoing connections.
    num_conns: u32,

    // Spool instance.
    spool: Spool,

    // Outgoing queue for this peer.
    queue: Queue,
}

impl Drop for PeerFeed {
    fn drop(&mut self) {
        // decrement session counter.
        server::dec_sessions();
    }
}

impl PeerFeed {
    /// Create a new PeerFeed.
    fn new(peer: Peer, spool: &Spool) -> PeerFeed {
        let (tx_chan, rx_chan) = mpsc::channel::<PeerFeedItem>(PEERFEED_COMMAND_CHANNEL_SIZE);
        let (broadcast, _) = broadcast::channel(CONNECTION_BCAST_CHANNEL_SIZE);
        let (tx_queue, rx_queue) = async_channel::bounded(PEERFEED_QUEUE_SIZE);
        let config = config::get_config();
        let queue_dir = &config.paths.queue;

        server::inc_sessions();

        PeerFeed {
            label: peer.label.clone(),
            next_id: 1,
            rx_chan,
            tx_chan,
            rx_queue,
            tx_queue,
            broadcast,
            num_conns: 0,
            spool: spool.clone(),
            queue: Queue::new(&peer.label, queue_dir, peer.maxqueue),
            newspeer: Arc::new(peer),
        }
    }

    // Get a clone of tx_chan. Masterfeed calls this.
    fn get_tx_chan(&self) -> mpsc::Sender<PeerFeedItem> {
        self.tx_chan.clone()
    }

    /// Run the PeerFeed.
    async fn run(mut self) {
        log::trace!("PeerFeed::run: {}: starting", self.label);
        let mut exiting = false;
        let mut queue_only = self.newspeer.queue_only;

        // Tick every 5 seconds.
        let mut interval = tokio::time::interval(Duration::from_millis(5000));

        loop {
            if exiting && self.num_conns == 0 {
                break;
            }

            let item;
            tokio::select! {
                _ = interval.next() => {
                    // see if we need to add connections to process the backlog.
                    let queue_len = self.queue.len().await;
                    if queue_len > 0 {
                        if queue_only {
                            if let Err(_e) = self.send_queue_to_backlog(true).await {
                                // backlog fails. now what.
                            }
                        } else {
                            if self.num_conns < self.newspeer.maxparallel / 2 {
                                self.add_connection().await;
                            }
                            // wake up sleeping connections.
                            let _ = self.broadcast.send(PeerFeedItem::Ping);
                        }
                    }
                    continue;
                }
                res = self.rx_chan.recv() => {
                    // we got an item from the masterfeed.
                    match res {
                        Some(an_item) => item = Some(an_item),
                        None => {
                            // unreachable!(), but let's be careful.
                            break;
                        },
                    }
                }
            }
            let item = item.unwrap();
            log::trace!("PeerFeed::run: {}: recv {:?}", self.label, item);

            match item {
                PeerFeedItem::Article(art) => {
                    // if we have no connections, or less than maxconn, create a connection here.
                    if self.num_conns < self.newspeer.maxparallel && !queue_only {
                        let qlen = self.tx_queue.len();
                        // TODO: use average queue length.
                        if self.num_conns == 0 || qlen > 1000 || qlen > PEERFEED_QUEUE_SIZE / 2 {
                            self.add_connection().await;
                        }
                    }

                    match self.tx_queue.try_send(art) {
                        Ok(()) => {},
                        Err(async_channel::TrySendError::Closed(_)) => {
                            // This code is never reached. It happens if all
                            // senders or all receivers are dropped, but we hold one
                            // to one of both so that can't happen.
                            unreachable!();
                        },
                        Err(async_channel::TrySendError::Full(art)) => {
                            // Queue is full. Send half to the backlog.
                            match self.send_queue_to_backlog(false).await {
                                Ok(_) => {
                                    // We're sure there's room now.
                                    let _ = self.tx_queue.try_send(art);
                                },
                                Err(_e) => {
                                    // article dropped, and backlog fails. now what.
                                },
                            }
                        },
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
                        if !queue_only && self.newspeer.queue_only {
                            let _ = self.broadcast.send(PeerFeedItem::ExitGraceful);
                        } else {
                            let _ = self.broadcast.send(PeerFeedItem::Reconfigure);
                        }
                        queue_only = self.newspeer.queue_only;
                    }
                },
                PeerFeedItem::ConnExit(arts) => {
                    let _ = self.requeue(arts).await;
                    self.num_conns -= 1;
                },
                PeerFeedItem::Ping => {},
            }
        }

        log::trace!("PeerFeed::run: {}: exit", self.label);

        // save queue to disk.
        let _ = self.send_queue_to_backlog(true).await;
    }

    // We got an error writing to the queue file.
    //
    // That means we have to pause the server. Then we check what
    // the error was (possibly "disk full") and wait for the
    // problem to resolve itself.
    //
    async fn queue_error(&self, e: &io::Error) {
        // XXX TODO stop all feeds.
        log::error!("PeerFeed: {}: error writing outgoing queue: {}", self.label, e);
    }

    // A connection was closed. If there were still articles in its
    // outgoing queue, re-queue them.
    async fn requeue(&mut self, mut arts: Vec<PeerArticle>) -> io::Result<()> {
        while let Some(art) = arts.pop() {
            if let Err(_) = self.tx_queue.try_send(art) {
                break;
            }
        }
        if arts.len() > 0 {
            let res = async {
                self.send_queue_to_backlog(false).await?;
                self.queue.write_arts(&self.spool, &arts).await?;
                Ok(())
            }
            .await;
            if let Err(e) = res {
                self.queue_error(&e).await;
                return Err(e);
            }
        }
        Ok(())
    }

    // Write half or the entire current queue to the backlog.
    async fn send_queue_to_backlog(&mut self, entire_queue: bool) -> io::Result<()> {
        let capacity = self.tx_queue.capacity().unwrap_or(0);
        let target_len = if entire_queue { 0 } else { capacity / 2 };
        let mut arts = Vec::new();
        while self.rx_queue.len() > target_len {
            match self.rx_queue.try_recv() {
                Ok(art) => arts.push(art),
                Err(_) => break,
            }
        }

        if let Err(e) = self.queue.write_arts(&self.spool, &arts).await {
            self.queue_error(&e).await;
            return Err(e);
        }
        Ok(())
    }

    async fn add_connection(&mut self) {
        let newspeer = self.newspeer.clone();
        let mut tx_chan = self.tx_chan.clone();
        let rx_queue = self.rx_queue.clone();
        let spool = self.spool.clone();
        let broadcast = self.broadcast.clone();
        let id = self.next_id;
        let queue = self.queue.clone();

        self.next_id += 1;
        self.num_conns += 1;

        // We spawn a new Connection task.
        task::spawn(async move {
            match Connection::new(
                id,
                newspeer,
                tx_chan.clone(),
                rx_queue,
                broadcast,
                spool,
                queue,
            )
            .await
            {
                Ok(conn) => {
                    // On succes, we start talking nntp.
                    drop(tx_chan);
                    conn.run().await;
                },
                Err(e) => {
                    log::warn!("connection {}", e);
                    // notify PeerFeed that we failed.
                    let _ = tx_chan.send(PeerFeedItem::ConnExit(Vec::new())).await;
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
    id:         u64,
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
    // do we need to rewrite the headers?
    rewrite:    bool,
    // Backlog.
    queue:      Queue,
    qitems:     Option<QItems>,
}

impl Connection {
    // Create a new connection.
    //
    // If we fail, we delay bit then try again. This function
    // only returns when we succeed or when a Reconfigure/Exit
    // notification was received.
    async fn new(
        id: u64,
        newspeer: Arc<Peer>,
        tx_chan: mpsc::Sender<PeerFeedItem>,
        rx_queue: async_channel::Receiver<PeerArticle>,
        broadcast: broadcast::Sender<PeerFeedItem>,
        spool: Spool,
        queue: Queue,
    ) -> io::Result<Connection>
    {
        let mut broadcast_rx = broadcast.subscribe();
        let mut do_delay = false;

        let mut fail_delay = 1000f64;
        delay_jitter(&mut fail_delay);

        loop {
            log::info!(
                "outfeed: {}:{}: connecting to {}",
                newspeer.label,
                id,
                newspeer.outhost
            );

            let conn_fut = Connection::connect(newspeer.clone(), id);
            tokio::pin!(conn_fut);

            if do_delay {
                log::debug!("Connection::new: delay {} ms", fail_delay as u64);
            }
            let delay_fut = delay_for(Duration::from_millis(fail_delay as u64));
            tokio::pin!(delay_fut);

            // Start connecting, but also listen to broadcasts while connecting.
            loop {
                tokio::select! {
                    _ = &mut delay_fut, if do_delay => {
                        do_delay = false;
                        delay_increase(&mut fail_delay, 120_000);
                        delay_jitter(&mut fail_delay);
                        continue;
                    }
                    conn = &mut conn_fut, if !do_delay => {
                        let (codec, ipaddr, connect_msg) = match conn {
                            Ok(c) => c,
                            Err(e) => {
                                // break out of the inner loop and retry.
                                log::warn!("{}:{}: {}", newspeer.label, id, e);
                                do_delay = true;
                                break;
                            },
                        };
                        let rewrite = newspeer.headfeed && !newspeer.preservebytes;

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
                            rewrite,
                            queue,
                            qitems: None,
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
    }

    // Spawn Self as a separate task.
    async fn run(mut self) {
        let _ = tokio::spawn(async move {
            // call feeder loop.
            if let Err(e) = self.feed().await {
                log::error!("outfeed: {}:{}: fatal: {}", self.newspeer.label, self.id, e);
                // We got an error, delay a bit so the main loop doesn't
                // reconnect right away. We should have a better strategy.
                delay_for(Duration::new(1, 0)).await;
            }

            // If we were processing backlog messages, put them back
            // onto the backlog queue.
            if let Some(qitems) = self.qitems {
                self.queue.return_items(qitems).await;
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
                        if !art.from_backlog {
                            arts.push(art.clone());
                        }
                    },
                    _ => {},
                }
            }
            let _ = self.tx_chan.send(PeerFeedItem::ConnExit(arts)).await;
        });
    }

    // Connect to remote peer.
    async fn connect(
        newspeer: Arc<Peer>,
        id: u64,
    ) -> io::Result<(NntpCodec, IpAddr, String)>
    {
        let (cmd, code) = if newspeer.headfeed {
            ("MODE HEADFEED", 250)
        } else {
            ("MODE STREAM", 203)
        };
        let res = nntp_client::nntp_connect(
            &newspeer.outhost,
            newspeer.port,
            cmd,
            code,
            newspeer.bindaddress.clone(),
        )
        .await;
        match res {
            Ok(c) => Ok(c),
            Err(e) => {
                log::warn!("{}:{}: {}", newspeer.label, id, e);
                Err(e)
            },
        }
    }

    // Feeder loop.
    async fn feed(&mut self) -> io::Result<()> {
        let mut xmit_busy = false;
        let mut maxstream = self.newspeer.maxstream as usize;
        if maxstream == 0 {
            maxstream = 1;
        }
        let part = if self.newspeer.headfeed {
            ArtPart::Head
        } else {
            ArtPart::Article
        };
        let mut processing_backlog = false;

        loop {
            // If there is an item in the send queue, and we're not still busy
            // sending the previous item, pop it from the queue and start
            // sending it to the remote peer.
            if !xmit_busy {
                if let Some(mut item) = self.send_queue.pop_front() {
                    log::trace!(
                        "Connection::feed: {}:{}: sending {:?}",
                        self.newspeer.label,
                        self.id,
                        item,
                    );
                    let size = self.transmit_item(item.clone(), part).await?;

                    // for TAKETHIS, update sent item size.
                    match item {
                        ConnItem::Takethis(ref mut art) => {
                            if size == 0 {
                                // size == 0 means article not found. non-fatal error.
                                continue;
                            }
                            // update size.
                            art.size = size;
                        },
                        _ => {},
                    }
                    // and queue item for the receiving side.
                    self.recv_queue.push_back(item);
                    xmit_busy = true;
                }
            }

            // Do we want to queue a new article?
            let queue_len = self.recv_queue.len() + self.send_queue.len();
            let need_item = !xmit_busy && queue_len < maxstream;

            if processing_backlog && queue_len == 0 {
                if self.qitems.as_ref().map(|q| q.len()).unwrap_or(0) == 0 {
                    log::trace!("Connection::feed: {}:{}: backlog run done", self.newspeer.label, self.id);
                    if let Some(qitems) = self.qitems.take() {
                        self.queue.ack_items(qitems).await;
                    }
                    processing_backlog = false;
                }
            }

            if need_item && processing_backlog {
                // Get an items from the backlog.
                if let Some(art) = self.qitems.as_mut().unwrap().next_art(&self.spool) {
                    log::trace!(
                        "Connection::feed: {}:{}: push onto send queue: CHECK {} (backlog)",
                        self.newspeer.label,
                        self.id,
                        art.msgid,
                    );
                    self.send_queue.push_back(ConnItem::Check(art));
                    continue;
                }
            }

            if need_item && !processing_backlog {
                // Try to get one item from the main queue.
                match self.rx_queue.try_recv() {
                    Ok(art) => {
                        log::trace!(
                            "Connection::feed: {}:{}: push onto send queue: CHECK {}",
                            self.newspeer.label,
                            self.id,
                            art.msgid,
                        );
                        self.send_queue.push_back(ConnItem::Check(art));
                        continue;
                    },
                    Err(async_channel::TryRecvError::Empty) => {
                        log::trace!(
                            "Connection::feed: {}:{}: empty, trying backlog",
                            self.newspeer.label,
                            self.id,
                        );
                        if let Some(qitems) = self.queue.read_items(200).await {
                            log::trace!(
                                "Connection::feed: {}:{}: processing backlog count={}",
                                self.newspeer.label,
                                self.id,
                                qitems.len(),
                            );
                            self.qitems = Some(qitems);
                            processing_backlog = true;
                            continue;
                        }
                    },
                    _ => {},
                }
            }

            tokio::select! {

                // If we need to, get an item from the global queue for this feed.
                res = self.rx_queue.recv(), if need_item && !processing_backlog => {
                    match res {
                        Ok(art) => {
                            log::trace!(
                                "Connection::feed: {}:{}: pushing CHECK {} onto send queue",
                                self.newspeer.label,
                                self.id,
                                art.msgid,
                            );
                            self.send_queue.push_back(ConnItem::Check(art))
                        },
                        Err(_) => {
                            log::trace!(
                                "Connection::feed: {}:{}: queue closed, sending QUIT",
                                self.newspeer.label,
                                self.id,
                            );
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
                    log::trace!(
                        "Connection::feed: {}:{}: received broadcast {:?}",
                        self.newspeer.label,
                        self.id,
                        res
                    );
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

                // process a response from the remote server.
                res = self.reader.next() => {

                    // Remap None (end of stream) to EOF.
                    let res = res.unwrap_or_else(|| Err(ioerr!(UnexpectedEof, "Connection closed")));

                    // What did we receive?
                    match res.and_then(NntpResponse::try_from) {
                        Err(e) => {
                            if e.kind() == io::ErrorKind::UnexpectedEof {
                                if self.recv_queue.len() == 0 {
                                    log::info!(
                                        "{}:{}: connection closed by remote",
                                        self.newspeer.label,
                                        self.id
                                    );
                                    return Ok(());
                                }
                                return Err(ioerr!(e.kind(), "connection closed unexpectedly"));
                            }
                            return Err(ioerr!(e.kind(), "reading from socket: {}", e));
                        },
                        Ok(resp) => {
                            if self.handle_response(resp).await? {
                                return Ok(());
                            }
                        },
                    }
                }
            }
        }
    }

    // The remote server sent a response. Process it.
    async fn handle_response(&mut self, resp: NntpResponse) -> io::Result<bool> {
        // It's a response to the item at the front of the queue.
        let item = match self.recv_queue.pop_front() {
            None => {
                if resp.code == 400 {
                    log::info!(
                        "{}:{}: connection closed by remote ({})",
                        self.newspeer.label,
                        self.id,
                        resp.short()
                    );
                    return Ok(true);
                }
                return Err(ioerr!(InvalidData, "unsollicited response: {}", resp.short()));
            },
            Some(item) => item,
        };

        // Now we have the request item and the response.
        // Decide what the next step is.
        let mut unexpected = false;
        match item {
            ConnItem::Check(art) => {
                log::trace!(
                    "Connection::handle_response {}:{}: CHECK response: {}",
                    self.newspeer.label,
                    self.id,
                    resp.short(),
                );
                match resp.code {
                    238 => {
                        // remote wants it. queue a takethis command.
                        self.send_queue.push_back(ConnItem::Takethis(art));
                    },
                    431 => {
                        // remote deferred it (aka "try again a bit later")
                        // XXX TODO req-queue article.
                        //self.stats.art_deferred(None);
                        self.stats.art_deferred_fail(None); // XXX
                    },
                    438 => {
                        // remote doesn't want it.
                        self.stats.art_refused();
                    },
                    _ => unexpected = true,
                }
            },
            ConnItem::Takethis(art) => {
                log::trace!(
                    "Connection::handle_response {}:{}: TAKETHIS response: {}",
                    self.newspeer.label,
                    self.id,
                    resp.short(),
                );
                match resp.code {
                    239 => {
                        // Nice, remote accepted it.
                        self.stats.art_accepted(art.size);
                    },
                    431 => {
                        // this is an invalid TAKETHIS reply!
                        // XXX TODO req-queue article.
                        //self.stats.art_deferred(Some(art.size));
                        self.stats.art_deferred_fail(Some(art.size)); // XXX
                    },
                    439 => {
                        // Remote already got it.
                        self.stats.art_rejected(art.size);
                    },
                    _ => unexpected = true,
                }
            },
            ConnItem::Quit => {
                // Response to the QUIT we sent.
                return Ok(true);
            },
        }

        if unexpected {
            return Err(ioerr!(InvalidData, "unexpected response: {}", resp.short()));
        }
        Ok(false)
    }

    // Put the ConnItem in the Sink.
    async fn transmit_item(&mut self, item: ConnItem, part: ArtPart) -> io::Result<usize> {
        match item {
            ConnItem::Check(art) => {
                log::trace!("Connection::transmit_item: CHECK {}", art.msgid);
                let line = format!("CHECK {}\r\n", art.msgid);
                Pin::new(&mut self.writer).start_send(line.into())?;
                Ok(0)
            },
            ConnItem::Takethis(art) => {
                log::trace!("Connection::transmit_item: TAKETHIS {}", art.msgid);
                let tmpbuf = Buffer::new();
                let mut sp_art = match self.spool.read_art(art.location.clone(), part, tmpbuf).await {
                    Ok(sp_art) => sp_art,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            self.stats.art_notfound();
                            return Ok(0);
                        }
                        if e.kind() == io::ErrorKind::InvalidData {
                            // Corrupted article. Should not happen.
                            // Log an error and continue.
                            log::error!("Connection::transmit_item: {:?}: {}", art, e);
                            self.stats.art_notfound();
                            return Ok(0);
                        }
                        return Err(e);
                    },
                };
                if part == ArtPart::Head {
                    sp_art.data.push_str("\r\n.\r\n");
                }
                let line = format!("TAKETHIS {}\r\n", art.msgid);
                Pin::new(&mut self.writer).start_send(line.into())?;
                if self.rewrite {
                    let (head, body) = self.rewrite_headers(sp_art);
                    let len = head.len() + body.len() - 3;
                    Pin::new(&mut self.writer).start_send(head)?;
                    Pin::new(&mut self.writer).start_send(body)?;
                    Ok(len)
                } else {
                    let len = sp_art.data.len() - 3;
                    Pin::new(&mut self.writer).start_send(sp_art.data)?;
                    Ok(len)
                }
            },
            ConnItem::Quit => {
                Pin::new(&mut self.writer).start_send("QUIT\r\n".into())?;
                Ok(0)
            },
        }
    }

    fn rewrite_headers(&self, sp_art: SpoolArt) -> (Buffer, Buffer) {
        let mut parser = HeadersParser::new();
        match parser.parse(&sp_art.data, false, true) {
            Some(Ok(_)) => {},
            _ => {
                // Never happens.
                return (sp_art.data, Buffer::new());
            },
        }
        let SpoolArt { body_size, data, .. } = sp_art;
        let (mut headers, body) = parser.into_headers(data);

        if self.newspeer.headfeed && !self.newspeer.preservebytes {
            if let Some(size) = body_size {
                let b = size.to_string().into_bytes();
                headers.update(HeaderName::Bytes, &b);
            }
        }
        let mut hb = Buffer::new();
        headers.header_bytes(&mut hb);
        (hb, body)
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

// add in 10% jitter.
fn delay_jitter(fail_delay: &mut f64) {
    let ms = *fail_delay;
    *fail_delay += (ms / 10f64) * rand::random::<f64>();
    *fail_delay -= ms / 20f64;
}

// exponential backoff.
fn delay_increase(fail_delay: &mut f64, max: u64) {
    *fail_delay *= 2f64;
    if *fail_delay > max as f64 {
        *fail_delay = max as f64;
    }
}
