//! A PeerFeed is a Queue and a bunch of Connections.
//!
//! There is one PeerFeed instance per NewsPeer. The PeerFeed instantiates
//! a on-disk queue, and a number of Connections to the remote peer.
//!
//! Articles are received from the MasterFeed, and sent to the Connections.
//! If there are no connections, or all of them are busy, the articles
//! (that is, article-handles, message-id and ArtLoc) are queued.
//!
use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::future::FutureExt;
use smartstring::alias::String as SmartString;
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tokio::time::Instant;
use tokio_stream::StreamExt;

use crate::config;
use crate::server;
use crate::spool::Spool;

use super::delay_queue::DelayQueue;
use super::mpmc;
use super::queue::Queue;
use super::{Connection, Peer, PeerArticle, PeerFeedItem};

// Size of the command channel for the peerfeed.
// The Masterfeed sends articles and notification messages down.
// Connections send notification messages up.
const PEERFEED_COMMAND_CHANNEL_SIZE: usize = 512;

// Size of the broadcast channel from the PeerFeed to the Connections.
// Not too small, as writes to the channel never ever block so
// no backpressure even if there are bursts (which there won't be).
const CONNECTION_BCAST_CHANNEL_SIZE: usize = 64;

// Max size of the article queue in a PeerFeed from which the Connections read.
const PEERFEED_QUEUE_SIZE: usize = 2000;

// If no articles have been taken from the main article queue in a PeerFeed for this
// many seconds, start writing the incoming feed from the masterfeed to the backlog on disk.
const MOVE_TO_BACKLOG_AFTER_SECS: u64 = 10;

// How often to flush the in-memory queue of a PeerFeed to disk, if it
// is in queue-only mode, or if no articles have been taken from it by
// a Connection after START_SPOOLING_AFTER_SECS.
const MOVE_TO_BACKLOG_EVERY_SECS: u64 = 5;

// Maximum size of the delay queue.
const DELAY_QUEUE_MAX_SIZE: usize = 5000;

// A peerfeed contains an in memory queue, an on-disk backlog queue,
// and zero or more active NNTP connections to remote peers.
pub(super) struct PeerFeed {
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
    rx_queue: mpmc::Receiver<PeerArticle>,
    tx_queue: mpmc::Sender<PeerArticle>,

    // Delay queue for if we delay articles.
    delay_queue: DelayQueue<PeerFeedItem>,

    // broadcast channel to all Connections.
    broadcast: broadcast::Sender<PeerFeedItem>,

    // Number of active outgoing connections.
    num_conns: u32,

    // Spool instance.
    spool: Spool,

    // Outgoing queue for this peer.
    pub(super) backlog_queue: Queue,
}

impl Drop for PeerFeed {
    fn drop(&mut self) {
        // decrement session counter.
        server::dec_sessions();
    }
}

impl PeerFeed {
    /// Create a new PeerFeed.
    pub(super) fn new(peer: Peer, spool: &Spool) -> PeerFeed {
        let (tx_chan, rx_chan) = mpsc::channel::<PeerFeedItem>(PEERFEED_COMMAND_CHANNEL_SIZE);
        let (broadcast, _) = broadcast::channel(CONNECTION_BCAST_CHANNEL_SIZE);
        let (tx_queue, rx_queue) = mpmc::bounded(PEERFEED_QUEUE_SIZE);
        let delay_queue = DelayQueue::with_capacity(DELAY_QUEUE_MAX_SIZE);
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
            delay_queue,
            broadcast,
            num_conns: 0,
            spool: spool.clone(),
            backlog_queue: Queue::new(&peer.label, queue_dir, peer.maxqueue),
            newspeer: Arc::new(peer),
        }
    }

    // Get a clone of tx_chan. Masterfeed calls this.
    pub(super) fn get_tx_chan(&self) -> mpsc::Sender<PeerFeedItem> {
        self.tx_chan.clone()
    }

    /// Run the PeerFeed.
    pub(super) async fn run(mut self) {
        log::trace!("PeerFeed::run: {}: starting", self.label);
        let mut exiting = false;
        let mut masterfeed_eof = false;
        let mut queue_only = self.newspeer.queue_only;
        let mut prev_tx_queue_len = 0;
        let mut main_queue_unread_secs = 0u64;
        let mut last = Instant::now();

        // Tick every 5 seconds.
        let mut interval = tokio::time::interval(Duration::new(MOVE_TO_BACKLOG_EVERY_SECS, 0));

        // if there's a backlog, start sending articles now.
        if self.backlog_queue.len().await > 0 {
            self.add_connection().await;
            let _ = self.broadcast.send(PeerFeedItem::Ping);
        }

        loop {
            if exiting && masterfeed_eof && self.num_conns == 0 {
                break;
            }

            let item = futures::select_biased! {
                res = self.rx_chan.recv().fuse() => {
                    // we got an item from the masterfeed.
                    match res {
                        Some(art @ PeerFeedItem::Article(_)) if self.newspeer.delay_feed.as_secs() > 0 && !queue_only => {
                            //
                            // Got an article, and `delay` is set. Insert the article into the DelayQueue.
                            // If that queue is full, send part of it to the backlog, or just drop it
                            // if dont-queue is set.
                            //
                            if let Err(art) = self.delay_queue.insert(art, self.newspeer.delay_feed) {
                                // overflow.
                                if let Err(_e) = self.send_delay_queue_to_backlog().await {
                                    // FIXME: backlog fails. now what.
                                }
                                // there should be room now.
                                let _ = self.delay_queue.insert(art, self.newspeer.delay_feed);
                            }
                            continue;
                        },
                        item @ Some(_) => {
                            // Got an article and we don't have to delay it, so use this value.
                            item
                        },
                        None => {
                            // unreachable!(), but let's be careful.
                            break;
                        }
                    }
                }
                res = self.delay_queue.next().fuse() => {
                    // Delayed article. A DelayQueue always returns Some(item).
                    res
                }
                _ = interval.tick().fuse() => {

                    // queue_only mode, so every tick (5 secs) we flush the
                    // main queue to the backlog on disk.
                    if queue_only {
                        if let Err(_e) = self.send_queue_to_backlog(true).await {
                            // FIXME: backlog fails. now what.
                        }
                        continue;
                    }

                    // see if the main queue has been unread for too long,
                    // meaning no active connections takeing articles from the queue.
                    main_queue_unread_secs += MOVE_TO_BACKLOG_EVERY_SECS;
                    if main_queue_unread_secs >= MOVE_TO_BACKLOG_AFTER_SECS {
                        if let Err(_e) = self.send_queue_to_backlog(true).await {
                            // FIXME: backlog fails. now what.
                        }
                        prev_tx_queue_len = 0;
                    }

                    // see if we need to add connections to process the backlog.
                    let backlog_len = self.backlog_queue.len().await;
                    if backlog_len > 0 {
                        // TODO: scale number of connections in a smarter way.
                        if self.num_conns < self.newspeer.maxparallel / 2 {
                            self.add_connection().await;
                        }
                    }

                    if self.num_conns > 0 {
                        // wake up sleeping connections.
                        let _ = self.broadcast.send(PeerFeedItem::Ping);
                    }

                    continue;
                }
            };
            let item = item.unwrap();
            log::trace!("PeerFeed::run: {}: recv {:?}", self.label, item);

            match item {
                PeerFeedItem::Article(art) => {
                    // if we have no connections, or less than maxconn, create a connection here.
                    let qlen = self.tx_queue.len();
                    if self.num_conns < self.newspeer.maxparallel && !queue_only {
                        // TODO/FIXME: better algorithm.
                        // - average queue length?
                        // - average delay (how long has oldest article been sitting in queue) ?
                        // (20201218: lowered from qlen > 1000 to qlen > 2)
                        if self.num_conns < 2 || qlen > 2 || qlen > PEERFEED_QUEUE_SIZE / 2 {
                            // Add max. 1 connection per second.
                            let now = Instant::now();
                            if self.num_conns == 0 || now.duration_since(last) > Duration::from_millis(1000) {
                                self.add_connection().await;
                                last = now;
                            }
                        }
                    }

                    match self.tx_queue.try_send(art).map_err(|e| e.into()) {
                        Ok(()) => {
                            // compare the previous queue length to the queue length
                            // _before_ we pushed an article onto it.
                            if prev_tx_queue_len != qlen {
                                // queue len has changed.
                                main_queue_unread_secs = 0;
                                prev_tx_queue_len = qlen;
                            }
                            // increase length by one (because we pushed an article).
                            prev_tx_queue_len += 1;
                        },
                        Err(mpmc::TrySendError::Closed(_)) => {
                            // This code is never reached. It happens if all
                            // senders or all receivers are dropped, but we hold one
                            // to one of both so that can't happen.
                            unreachable!();
                        },
                        Err(mpmc::TrySendError::Full(art)) => {
                            // Queue is full. Send half to the backlog.
                            match self.send_queue_to_backlog(false).await {
                                Ok(_) => {
                                    // We're sure there's room now.
                                    let _ = self.tx_queue.try_send(art);
                                },
                                Err(_e) => {
                                    // FIXME: article dropped, and backlog fails. now what.
                                },
                            }
                        },
                    }
                },
                PeerFeedItem::ExitFeed => {
                    // The masterfeed has closed down. only now can we really exit, because a few
                    // articles might still have arrived in the interim, and we need to write
                    // those to the backlog queue as well.
                    if !exiting {
                        let _ = self.broadcast.send(PeerFeedItem::ExitGraceful);
                        queue_only = true;
                        exiting = true;
                    }
                    masterfeed_eof = true;
                },
                PeerFeedItem::ExitGraceful => {
                    // this only asks the connections to close down, we wait
                    // until all of them are gone.
                    if !exiting {
                        let _ = self.broadcast.send(PeerFeedItem::ExitGraceful);
                        queue_only = true;
                        exiting = true;
                    }
                },
                PeerFeedItem::ExitNow => {
                    // last call. we dont exit yet, if a connection is hanging,
                    // let's wait for it as long as possible. that's what the
                    // timeout in the main loop is _for_, right.
                    queue_only = true;
                    exiting = true;
                    masterfeed_eof = true;
                    let _ = self.broadcast.send(PeerFeedItem::ExitNow);
                    // do write the queue as-is, though.
                    let _ = self.send_queue_to_backlog(true).await;
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
                    if exiting && self.num_conns == 0 {
                        if let Err(_e) = self.send_queue_to_backlog(true).await {
                            // FIXME: backlog fails. now what.
                        }
                    }
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
        if arts.len() > 0 && !self.newspeer.no_backlog {
            let res = async {
                self.send_queue_to_backlog(false).await?;
                self.backlog_queue.write_arts(&self.spool, &arts).await?;
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
        // or, if 'no_backlog' is set, just trim the in-memory queue and return.
        if self.newspeer.no_backlog {
            if self.rx_queue.len() >= 200 {
                let target_len = self.rx_queue.len() / 2;
                while self.rx_queue.len() > target_len {
                    if self.rx_queue.try_recv().is_err() {
                        break;
                    }
                }
            }
            return Ok(());
        }

        let target_len = if entire_queue { 0 } else { PEERFEED_QUEUE_SIZE / 2 };
        let mut arts = Vec::new();
        while self.rx_queue.len() > target_len {
            match self.rx_queue.try_recv() {
                Ok(art) => arts.push(art),
                Err(_) => break,
            }
        }

        if entire_queue {
            // backlog queue (if any) as well.
            let mut drain = self.delay_queue.drain();
            while let Some(item) = drain.next() {
                match item {
                    PeerFeedItem::Article(art) => arts.push(art),
                    _ => {},
                }
            }
        }

        if let Err(e) = self.backlog_queue.write_arts(&self.spool, &arts).await {
            self.queue_error(&e).await;
            return Err(e);
        }
        Ok(())
    }

    // Write some of the current delay queue to the backlog.
    async fn send_delay_queue_to_backlog(&mut self) -> io::Result<()> {
        // or, if 'no_backlog' is set, don't bother.
        if self.newspeer.no_backlog {
            return Ok(());
        }

        let target_len = DELAY_QUEUE_MAX_SIZE / 4;
        let mut arts = Vec::new();
        let mut drain = self.delay_queue.drain();
        while drain.len() > target_len {
            match drain.next() {
                Some(PeerFeedItem::Article(art)) => arts.push(art),
                Some(_) => {},
                None => break,
            }
        }

        if let Err(e) = self.backlog_queue.write_arts(&self.spool, &arts).await {
            self.queue_error(&e).await;
            return Err(e);
        }
        Ok(())
    }

    async fn add_connection(&mut self) {
        let newspeer = self.newspeer.clone();
        let tx_chan = self.tx_chan.clone();
        let rx_queue = self.rx_queue.clone();
        let spool = self.spool.clone();
        let broadcast = self.broadcast.clone();
        let id = self.next_id;
        let queue = self.backlog_queue.clone();

        self.next_id += 1;
        self.num_conns += 1;

        // We spawn a new Connection task.
        task::spawn(async move {
            match Connection::new(id, newspeer, tx_chan.clone(), rx_queue, broadcast, spool, queue).await {
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
