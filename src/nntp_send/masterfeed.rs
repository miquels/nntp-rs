//! Fans out articles to the peers.
//!
//! Receives articles from the incoming feed, and then fans them
//! out over all PeerFeeds that want the article.
//!
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::FutureExt;
use smartstring::alias::String as SmartString;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

use crate::bus::{self, Notification};
use crate::config;
use crate::newsfeeds::NewsFeeds;
use crate::spool::Spool;

use super::{FeedArticle, Peer, PeerArticle, PeerFeed, PeerFeedItem};

/// The MasterFeed.
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
                peer_feed.backlog_queue.init().await;
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
            futures::select_biased! {
                article = conditional_fut!(recv_arts, self.art_chan.next()) => {
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
                notification = self.bus.recv().fuse() => {
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
                    msgid:        art.msgid.clone(),
                    location:     art.location.clone(),
                    size:         art.size,
                    from_backlog: false,
                    deferred:     0,
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
