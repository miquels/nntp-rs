//! OUTFEED
//!
//! This module takes care of feeding articles to remote peers.
//!

use std::fmt;
use std::sync::atomic::AtomicUsize;

use crate::config;
use crate::spool::{ArtLoc, Spool};
use crate::history::HistEnt;
use crate::newsfeeds:NewsFeeds;
use crate::nntp_codec::NntpCodec;

use crossbeam_queue::ArrayQueue;
use tokio::prelude::*;
use tokio::io::{BufReader, AsyncBufReadExt};

trait BoolExt {
    fn if_true<T>(&self, v: T) -> Option<T>;
}

impl BoolExt for bool {
    fn if_true<T>(&self, v: T) -> Option<T> {
        if *self {
            Some(v)
        } else {
            None
        }
    }
}

// Queued article.
struct OutArticle {
    // Message-Id.
    msgid:      String,
    // Location in the article spool.
    location:   ArtLoc,
}

// Received from the NNTP server, article to be sent to a bunch of peers.
struct OutArticles {
    // Queued article.
    article:    OutArticle,
    // Peers that want it.
    peers:      Vec<String>,
}

// Items in the queue to be sent out on a Connection.
enum Item {
    Quit,
    Check(OutArticle),
    Takethis(OutArticle),
}

impl fmt::Display for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            &Quit => write!("QUIT"),
            &Check(ref art) => write("CHECK {}", art.msgid),
            &Takethis(ref art) => write("TAKETHIS {}", art.msgid),
        }
    }
}

// Master fanout task. Receives articles from the incoming feed, and
// sends them through to peerfeeds.
struct MasterFeed {
    receiver:   channel::Receiver<FeedItem>,
    newsfeeds:  Arc<NewsFeeds>,
    peerfeeds:  HashMap<String, channel::Sender<MasterFeedItem>>,
}

// Sent from the nntp server incoming feeds to the masterfeed.
pub enum FeedItem {
    Article(OutArticles),
    Notification(Notification),
}

// Sent from the masterfeed to the individual peerfeeds.
enum PeerFeedItem {
    Article(OutArticle),
    Notification(Notification),
}

impl MasterFeed {
    pub fn new(newsfeeds: Arc<NewsFeeds>, receiver: channel::Receiver<OutArticles>) -> MasterFeed {
        MasterFeed {
            receiver,
            newsfeeds,
            peerfeeds: HashMap::new(),
        }
    }

    // Reconfigure - check the current set of peerfeeds that we feed to to the
    // total list in the newsfeed set. Stop any of them that are not configured anymore.
    fn reconfigure(&mut self) {
    }

    ///
    /// The actual task. This reads from the receiver channel and fans out
    /// the messages over all peerfeeds.
    ///
    pub async fn run(&mut self) {
        while let Some(item) = self.receiver.recv() {
            match item {
                FeedItem::Article(art) => {
                    // Forwards only to the peerfeeds in the list.
                    for peername in &art.peers {
                        if let Some(peerfeed) = self.peerfeeds.get_mut(peername) {
                            let _ = peerfeed.send(PeerFeedItem::Article(art.clone())).await;
                        }
                    }
                },
                // XXX FIXME we need to look at notifications ourself too:
                // - notification that "newsfeeds" has been reloaded -> reconfigure
                // - ExitGraceful, need to poll to see if all peerfeeds are gone
                // - ExitNow, ditto
                FeedItem::Notification(n) => {
                    // Forward to all peerfeeds.
                    for peerfeed in self.peerfeeds.values_mut() {
                        let _ = peerfeed.send(PeerFeedItem::Notification(n));
                    }
                }
            }
        }
    }

    // Forward an article to a peerfeed. If it hasn't been instantiated yet, do it now.
    async fn forward_to_peerfeed(&mut self, peername: &str, art: OutArticle) {

        // Shortcut if it does exist.
        if let Some(peerfeed) = self.peerfeeds.get_mut(peername) {
            let _ = peerfeed.send(PeerFeedItem::Article(art)).await;
            return;
        }

        // Find the matching newsfeed.
        // If we can't find it it's probably because of some config change that has
        // not been synchronized to all parts of the server yet, so ignore for now.
        let idx = match self.newsfeeds.peer_map.get(peername) {
            None => return,
            Some(idx) => *idx,
        };
        let nfpeer = &self.newsfeeds.peers[idx];

        // Create peerfeed now, and launch it.
        let peerfeed = PeerFeed::new(peername, nfpeer, self.newsfeeds.clone());
        let mut tx = peerfeed.chan_tx.clone();
        tokio::spawn(peerfeed.run());

        // send this article, then store channel in the peerfeed map.
        let _ = tx.send(PeerFeedItem::Article(art)).await;
        self.peerfeeds.insert(peername.clone(), tx);
    }
}

// A peerfeed.
struct PeerFeed {
    // Name.
    label:          String,
    // Comms channel.
    tx_chan:        channel::Receiver<PeerFeedItem>,
    rx_chan:        channel::Sender<PeerFeedItem>,
    // Link to newsfeed config.
    newsfeeds:      Arc<NewsFeeds>,
    // Peerfeed article queue.
    queue:          ArrayQueue<Item>,
    // The <peerfeed>.R file
    resend:         DiskReadQueue,
    // The <peerfeed>.B.* file
    backlog:        DiskWriteQueue,
    // The <peerfeed>.D.* file
    deferred:       DiskWriteQueue,
    // Broadcast channel to all connections.
    notifier:       watch::Sender<Notification>,
    // Administrivia.
    cur_conns:      AtomicUsize,
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
    fn new(label: &str, nfpeer: &NewsPeer, newsfeeds: Arc<NewsFeeds>) -> PeerFeed {
        let (tx, rx) = channel::new::<PeerFeedItem>>();
        PeerFeed {
            label:  label.clone(),
            tx_chan:    tx,
            rx_chan:    rx,
            newsfeeds,
            queue:      ArrayQueue::new(),
            resend:     DiskReadQueue::new(label),
            backlog:    DiskWrite::new(label),
            deferred:   DiskWrite::new(label),
			notifier:	XXX,
            cur_conns:  AtomicUsize::new(0),
            outhost:    nfpeer.outhost.clone(),
            bindaddress: nfpeer.bindaddress.clone(),
            port:       nfpeer.port,
            maxparallel:    nfpeer.maxparallel,
            nobatch:    nfpeer.nobatch,
            maxqueue:   nfpeer.maxqueue,
            headfeed:   nfpeer.headfeed,
            genlines:   nfpeer.genlines,
            preservebytes:  nfpeer.perservebytes,
        }
    }

    /// Run the PeerFeed.
    async fn run(self) {
        while let Some(item) = self.rx_chan.recv() {
            match item {
                PeerFeedItem::Notification(n) => {
                    // handle notification
                },
                PeerFeedItem::Article(art) => {
                    // queue article
                    if queue.is_full() {
                        // full. send half of the queue to the backlog.
                        continue;
                    }
                    queue.push(art).expect("NewsPeer::run: queue overflow");
                    // if we have no connections, or less than maxconn, create a connection here.
                    // unless a connection is still being set up - we only create one connection
                    // at a time, so that we don't thunderherd the peer.

                    // now notify one of the connections.
                    // XXX how? we can do a broadcast on the notifier, or we
                    // can store a bunch of Wakers and just wake one of them,
                    // in round-robin fashion.
                }
            }
        }
    }
}


// A connection.
struct Connection {
    // Shared peerfeed.
    peerfeed:       Arc<PeerFeed>,
    // CHECK or TAKETHIS commands waiting to be sent.
    send_queue:     VecDeque<OutArticle>,
    // CHECK or TAKETHIS commands waiting for a reply.
    recv_queue:     VecDeque<OutArticle>,
    // current article being processed by the receiver.
    recv_current:   Option<OutArticle>,
    // Reuseable article buffer.
    art_buffer:     BytesMut,
    // NntpCodec.
    codec:          NntpCodec,
    // Notification receiver.
    watcher:        watch::Receiver<Notification>,
    // Stats
    stats:          SessionStats,
    // Set after we have sent QUIT
    sender_done:    bool,
}

// A future that first polls the recv task, then the send task.
impl Future for Connection {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>> {

        let mut is_err = false;

        // First check the notification channel.
        let fut = self.watcher.recv();
        pin_utils::pin_mut!(fut);
        match fut.poll(cx) {
            Poll::Ready(item) => {
                match item {
                    Some(Notificaton::ExitNow) => {
                        self.cleanup();
                        return Poll::Ready(());
                    },
                    Some(Notificaton::ExitGraceful) => {
                        if !self.sender_done {
                            self.send_queue.push_front(Item::Quit);
                            let send = &self.send;
                            pin_utils::pin_mut!(send);
                            match send.poll(cx) {
                                Poll::Ready(Ok(())) => self.sender_done = true,
                                Poll::Ready(Err(e)) => is_err = true,
                                Poll::Pending => {},
                            }
                        }
                    },
                    _ => {},
                }
            },
            Poll::Pending => {},
        }

        // Receive replies. This one is called first, since it
        // might queue one or more TAKETHIS commands.
        let recv = &self.recv;
        pin_utils::pin_mut!(recv);
        match recv.poll(cx) {
            Poll::Ready(Ok(())) => {
                // We got the 205 bye response. done.
                self.cleanup();
                return Poll::Ready(());
            },
            Poll::Ready(Err(e)) => is_err = true,
            Poll::Pending => {},
        }

        // Send commands.
        if !self.sender_done {
            let send = &self.send;
            pin_utils::pin_mut!(send);
            match send.poll(cx) {
                Poll::Ready(Ok(())) => self.sender_done = true,
                Poll::Ready(Err(e)) => is_err = true,
                Poll::Pending => {},
            }
        }

        if is_err {
            self.cleanup();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Connection {
    //
    // get the next item from the queue.
    //
    async fn get_item(&mut self) -> Option<Item> {
        // if the receive queue is too long, wait a bit.
        if self.recv_queue.len() > 100 {
            return None;
        }

        // first the local send queue.
        if let Some(item) = self.send_queue.pop_front() {
            return Some(item);
        }

        // then the common peerfeed sendqueue.
        self.peerfeed.get_item().await
    }

    //
    // take items off the queue, send them to the peer.
    //
    async fn send(&mut self) -> io::Result<()> {
        loop {
            let item = match self.get_item().await {
                Some(item) => item,
                None => {
                    // Wait for the next wakeup, any wakeup.
                    PollOnce::new().await;
                    continue;
                }
            }
            match item {
                Item::Quit {
                    // Send a QUIT command and exit.
                    self.recv_queue.push_back(Item::Quit);
                    self.socket.write_all("QUIT\r\n").await?;
                    return;
                },
                Item::Check(item) => {
                    // Send a CHECK command.
                    let cmd = format!("CHECK {}\r\n", item.msgid);
                    self.recv_queue.push_back(Item::Check(item));
                    self.socket.write_all(&cmd).await?;
                },
                Item::Takethis(item) {
                    // Get the article from spool.
                    let part = self.headfeed.if_true(ArtPart::Head).unwrap_or(ArtPart::Article);
                    let buf = self.art_buffer.take().unwrap_or(bytes::BytesMut::new());
                    let mut buf = match self.spool.read(item.location, part, buf).await {
                        Ok(buf) => buf,
                        Err(io::Error::NotFound) => {
                            // Allright, the article does not exist in the spool anymore.
                            // That's fine, we just skip it.
                            // XXX statistics
                            continue;
                        },
                        Err(e) => {
                            // Whoops we got a serious error reading the article.
                            /// XXX FIXME: stats, increase delay, backlog article
                            error!("Connection::send: {}: {}", item.msgid, e);
                            delay_for(Duration::from_secs(1)).await;
                            continue;
                        },
                    };

                    // push the item on the receive queue.
                    self.recv_queue.push_back(Item::Takethis(item));

                    // send TAKETHIS command and body.
                    let cmd = format!("TAKETHIS {}\r\n", item.msgid);
                    self.codec.write(cmd).await?;
                    self.codec.write_buf(&mut buf).await?;

                    // re-use buffer.
                    buf.truncate(0);
                    self.art_buffer = Some(buf);
                },
            }
        }
    }

    // Receive results from the peer, handle them.
    async fn recv(&mut self) -> io::Result<()> {
        loop {
            // Read buffer.
            let buf = match self.codec.read_line() {
                Ok(ReadLine::Line(buf)) => buf,
                Ok(ReadLine::Notify(_) => continue,
                Ok(ReadLine::Eof) => return Err(ioerr!(UnexpectedEOF, "UnexpectedEOF")),
                Err(e) => return Err(e),
            };
            
            // Get the sent item that this should be a reply to.
            // Don't pop it off the queue yet; if we error it should be left on
            // the queue so that we put it on the backlog queue in the error handler.
            let item = match self.recv_queue.front() {
                Some(item) => item,
                None => return Err(ioerr!(InvalidData, "out of sync due to spurious response: {}",
                                          NntpResponse::diag_response(&buf))),
            };

            // decode response.
            let resp = NntpResponse::parse(&buf)
                .map_err(|e| ioerr!(e.kind(), "{}: {}", item, e))?;

            // handle it.
            match item {
                Item::Quit => {
                    if code != 205 {
                        return Err(ioerr!(InvalidData, "QUIT: unexpected response code: ", resp.short));
                    }
                    return Ok(());
                },
                Item::Check(art) => {
                    match code {
                        238 => {
                            let art = msgids_match(true, art, resp)?;
                            // Wants it. push onto send queue as TAKETHIS. XXX FIXME stats
                            let item = self.recv_queue.pop_front().unwrap();
                            self.send_queue.push_back(item);
                        },
                        431 => {
                            let art = msgids_match(true, art, resp)?;
                            // deferred. XXX FIXME stats
                            if let Some(Item::Check(art)) = self.recv_queue.pop_front() {
                                self.peerconn.defer(art);
                            }
                        },
                        438 => {
                            let art = msgids_match(true, art, resp)?;
                            // don't want it. do nothing. XXX FIXME stats.
                        },
                        _ => {
                            let item = Item::Check(art);
                            let err = ioerr!(Other, "{}: unexpected response: {}", item, resp.short);
                            self.recv_queue.push_front(item);
                            return Err(err);
                        },
                    }
                }
                Item::Takethis(art) => {
                    match code {
                        239 => {
                            let art = msgids_match(false, art, resp)?;
                            // Transferred OK. XXX Stats
                        },
                        439 => {
                            let art = msgids_match(false, art, resp)?;
                            // Rejected. XXX Stats
                        },
                        431 => {
                            let art = msgids_match(false, art, resp)?;
                            // Deferred. THIS IS AN INVALID CODE FOR THIS COMMAND.
                            info!("Error on connection {} from {} {}:\
                                   unexpected response code 431 after TAKETHIS",
                                  self.stats.fdno, self.stats.hostname, self.stats.ipaddr);
                            self.peerconn.defer(art);
                        },
                        _ => {
                            let item = Item::Takethis(art);
                            let err = ioerr!(Other, "{}: unexpected response: {}", item, resp.short);
                            self.recv_queue.push_front(item);
                            return Err(err);
                        },
                    }
                },
            }
        }
    }
}

// helper function.
fn msgids_match(check: bool, art: OutArticle, resp: &NntpResponse) -> io::Result<OutArticle> {
    if art.msgid.as_str() != resp.args[0] {
        let item = if check {
            Item::Check(art)
        } else {
            Item::Takethis(art)
        };
        let err = ioerr!(InvalidData, "{}: message-id mismatch: {}", item, resp.short);
        self.recv_queue.push_front(item);
        Err(err)
    } else {
        Ok(art)
    }
}

/// A Future that waits for the next wakeup.
struct PollOnce(bool);

impl PollOnce {
    /// Create new PollOnce.
    fn new() -> PollOnce {
        PollOnce(false)
    }
}

impl Future for PollOnce {
    type Output = ();
    fn poll(&mut self, _cx: &mut Context) -> Poll<Self::Output>> {
        if !self.0 {
            self.0 = true;
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

