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

use smartstring::alias::String as SmartString;
use tokio::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio::task;

use crate::config;
use crate::server;
use crate::spool::Spool;

use super:: queue::Queue;
use super::{Peer, PeerArticle, PeerFeedItem, Connection};

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
    rx_queue: async_channel::Receiver<PeerArticle>,
    tx_queue: async_channel::Sender<PeerArticle>,

    // broadcast channel to all Connections.
    broadcast: broadcast::Sender<PeerFeedItem>,

    // Number of active outgoing connections.
    num_conns: u32,

    // Spool instance.
    spool: Spool,

    // Outgoing queue for this peer.
    pub(super) queue: Queue,
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
    pub(super) fn get_tx_chan(&self) -> mpsc::Sender<PeerFeedItem> {
        self.tx_chan.clone()
    }

    /// Run the PeerFeed.
    pub(super) async fn run(mut self) {
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
                                // FIXME: backlog fails. now what.
                            }
                        } else {
                            // TODO: scale number of connections in a smarter way.
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
                                    // FIXME: article dropped, and backlog fails. now what.
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

