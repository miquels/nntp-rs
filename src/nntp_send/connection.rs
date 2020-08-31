//! An active connection to a peer. Plucks articles from the in-memory
//! queue in the PeerFeed, and if there are no articles left, articles
//! from the disk-queue.
//!
//! If a Connection gets closed while it is still processing articles,
//! all articles that have not been sent entirely will be put back
//! on the PeerFeed queue.
//!
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::sink::{Sink, SinkExt};
use tokio::prelude::*;
use tokio::stream::Stream;
use tokio::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{delay_for, Instant};

use crate::article::{HeaderName, HeadersParser};
use crate::diag::TxSessionStats;
use crate::nntp_client;
use crate::nntp_codec::{NntpCodec, NntpResponse};
use crate::spool::{ArtPart, Spool, SpoolArt};
use crate::util::Buffer;

use super::{Peer, PeerArticle, PeerFeedItem, QItems, Queue};

// How long to wait to re-offer a deferred article, initially.
const DEFER_DELAY_INITIAL: u64 = 10u64;

// How long to wait in case the article was deferred again.
const DEFER_DELAY_NEXT: u64 = 5u64;

// How many times to try to re-offer a deferred article.
// After this many, we give up and drop the article.
const DEFER_RETRIES: u32 = 3;

// How many articles too keep in the deferred-retry buffer.
const DEFER_MAX_QUEUE: usize = 1000;

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
pub(super) struct Connection {
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
    // deferred article queue.
    deferred:   DeferredQueue,
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
    pub(super) async fn new(
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
                "Feed {}:{}: connecting to {}",
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
                            deferred: DeferredQueue::new(),
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
    pub(super) async fn run(mut self) {
        let _ = tokio::spawn(async move {
            // call feeder loop.
            if let Err(e) = self.feed().await {
                log::error!("Feed {}:{}: fatal: {}", self.newspeer.label, self.id, e);
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
                .chain(self.deferred.iter())
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
    async fn connect(newspeer: Arc<Peer>, id: u64) -> io::Result<(NntpCodec, IpAddr, String)> {
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

                    if !self.transmit_item(&mut item, part).await? {
                        // wanted to do TAKETHIS, but article not found, so start from the top.
                        continue;
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
                    log::trace!(
                        "Connection::feed: {}:{}: backlog run done",
                        self.newspeer.label,
                        self.id
                    );
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

                // Check the deferred queue.
                defer_art = self.deferred.next() => {
                    let art = defer_art.unwrap();
                    log::trace!(
                        "Connection::feed: {}:{}: re-pushing CHECK {} onto send queue",
                        self.newspeer.label,
                        self.id,
                        art.msgid(),
                    );
                    // For now ignore dropped articles when re-queuing.
                    let _ = self.send_queue.push_back(art);
                }
            }
        }
    }

    // The remote server sent a response. Process it.
    async fn handle_response(&mut self, resp: NntpResponse) -> io::Result<bool> {
        // Remote side can close the connection at any time, if they are
        // nice they send a 400 code so we know it's on purpose.
        if resp.code == 400 {
            log::info!(
                "{}:{}: connection closed by remote ({})",
                self.newspeer.label,
                self.id,
                resp.short()
            );
            return Ok(true);
        }

        // Must be a response to the item at the front of the queue.
        let item = match self.recv_queue.pop_front() {
            None => return Err(ioerr!(InvalidData, "unsollicited response: {}", resp.short())),
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
                        match self.deferred.push(ConnItem::Check(art)) {
                            Ok(_) => self.stats.art_deferred(None),
                            Err(_) => self.stats.art_deferred_fail(None),
                        }
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
                        // NOTE: this is an invalid TAKETHIS reply!
                        // So, we ignore this reply, and we do not re-queue the article.
                        self.stats.art_deferred_fail(Some(art.size));
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
    //
    // Returns 'false' if the article was not found when about to send TAKETHIS.
    async fn transmit_item(&mut self, item: &mut ConnItem, part: ArtPart) -> io::Result<bool> {
        match item {
            ConnItem::Check(ref art) => {
                log::trace!("Connection::transmit_item: CHECK {}", art.msgid);
                let line = format!("CHECK {}\r\n", art.msgid);
                Pin::new(&mut self.writer).start_send(line.into())?;
                Ok(true)
            },
            ConnItem::Takethis(ref mut art) => {
                log::trace!("Connection::transmit_item: TAKETHIS {}", art.msgid);
                let tmpbuf = Buffer::new();
                let mut sp_art = match self.spool.read_art(art.location.clone(), part, tmpbuf).await {
                    Ok(sp_art) => sp_art,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            self.stats.art_notfound();
                            return Ok(false);
                        }
                        if e.kind() == io::ErrorKind::InvalidData {
                            // Corrupted article. Should not happen.
                            // Log an error and continue.
                            log::error!("Connection::transmit_item: {:?}: {}", art, e);
                            self.stats.art_notfound();
                            return Ok(false);
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
                    art.size = head.len() + body.len() - 3;
                    Pin::new(&mut self.writer).start_send(head)?;
                    Pin::new(&mut self.writer).start_send(body)?;
                    Ok(true)
                } else {
                    art.size = sp_art.data.len() - 3;
                    Pin::new(&mut self.writer).start_send(sp_art.data)?;
                    Ok(true)
                }
            },
            ConnItem::Quit => {
                Pin::new(&mut self.writer).start_send("QUIT\r\n".into())?;
                Ok(true)
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

struct DeferredArticle {
    art:  ConnItem,
    when: Instant,
}

// Our own version of DeferredQueue.
// Needed because the tokio version has no way to drain the
// queue in one go, which we need when the connection is dropped.
#[derive(Default)]
struct DeferredQueue {
    queue: VecDeque<DeferredArticle>,
    tick:  Option<tokio::time::Delay>,
}

impl DeferredQueue {
    fn new() -> DeferredQueue {
        DeferredQueue::default()
    }

    // Push an article onto the queue.
    //
    // If the queue exceeds `maxlen` items, remove the oldest
    // item and return it as an error. The caller can then
    // decide what to do with it.
    fn push(&mut self, mut art: ConnItem) -> Result<(), ConnItem> {
        art.deferred_inc();
        if art.deferred() > DEFER_RETRIES {
            return Err(art);
        }

        let delay = if art.deferred() == 1 {
            DEFER_DELAY_INITIAL
        } else {
            DEFER_DELAY_NEXT
        };
        let when = Instant::now() + Duration::new(delay, 0);
        if self.queue.is_empty() {
            if let Some(ref mut tick) = self.tick {
                tick.reset(when);
            } else {
                self.tick = Some(tokio::time::delay_until(when));
            }
        }
        self.queue.push_back(DeferredArticle { art, when });
        if self.queue.len() > DEFER_MAX_QUEUE {
            Err(self.queue.pop_front().unwrap().art)
        } else {
            Ok(())
        }
    }

    // Turn this queue into an iterator of PeerArticles, in order to drain it.
    fn iter(&self) -> impl Iterator<Item = &ConnItem> {
        self.queue.iter().map(|q| &q.art)
    }
}

impl Stream for DeferredQueue {
    type Item = ConnItem;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();

        // nothing to do?
        if this.queue.is_empty() {
            return Poll::Pending;
        }

        // See if an item at the front of the queue has expired.
        // We allow a window of one second, to batch up items that
        // expire at around the same time.
        let now = Instant::now() - Duration::new(1, 0);
        let when = this.queue[0].when;
        if now > when {
            let art = this.queue.pop_front().unwrap().art;
            return Poll::Ready(Some(art));
        }

        // set the next tick.
        if let Some(ref mut tick) = this.tick {
            tick.reset(when);
        } else {
            this.tick = Some(tokio::time::delay_until(when));
        }
        Poll::Pending
    }
}

// Item in the Connection queue.
#[derive(Clone)]
enum ConnItem {
    Check(PeerArticle),
    Takethis(PeerArticle),
    Quit,
}

impl ConnItem {
    fn msgid(&self) -> &str {
        match self {
            ConnItem::Check(ref art) => art.msgid.as_str(),
            ConnItem::Takethis(ref art) => art.msgid.as_str(),
            ConnItem::Quit => "",
        }
    }
    fn deferred(&self) -> u32 {
        match self {
            ConnItem::Check(ref art) => art.deferred,
            ConnItem::Takethis(ref art) => art.deferred,
            ConnItem::Quit => 0,
        }
    }
    fn deferred_inc(&mut self) {
        match self {
            ConnItem::Check(ref mut art) => art.deferred += 1,
            ConnItem::Takethis(ref mut art) => art.deferred += 1,
            ConnItem::Quit => {},
        }
    }
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
