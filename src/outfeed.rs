use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::sink::{Sink, SinkExt};
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

use crate::util::Buffer;
use crate::diag::SessionStats;
use crate::nntp_codec::{NntpCodec, NntpInput};
use crate::server::Notification;
use crate::spool::{ArtLoc, ArtPart, Spool};

// Aricle to be queued.
#[derive(Clone)]
struct OutArticle {
    // Message-Id.
    msgid:      String,
    // Location in the article spool.
    location:   ArtLoc,
}

// Items in the queue to be sent out on a Connection.
#[derive(Clone)]
enum Item {
    Check(OutArticle),
    Takethis(OutArticle),
    Quit,
}

impl fmt::Display for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            &Item::Check(ref art) => write!(f, "CHECK {}", art.msgid),
            &Item::Takethis(ref art) => write!(f, "TAKETHIS {}", art.msgid),
            &Item::Quit => write!(f, "QUIT"),
        }
    }
}

// A peerfeed.
struct PeerFeed {
    // Name.
    label:          String,
}

impl PeerFeed {
    fn get_item(&self, empty: bool) -> Option<Item> {
        // TODO
        None
    }
}

// A connection.
struct Connection<R, W> {
    // reader / writer.
    reader:         NntpCodec<R>,
    writer:         NntpCodec<W>,
    // Shared peerfeed.
    peerfeed:       Arc<PeerFeed>,
    // Max number of outstanding requests.
    streaming:      usize,
    // Items waiting to be sent.
    send_queue:     VecDeque<Item>,
    // Sent items, waiting for a reply.
    recv_queue:     VecDeque<Item>,
    // Stats
    stats:          SessionStats,
    // Set after we have sent QUIT
    sender_done:    bool,
    // Notification channel receive side.
    notification:   mpsc::Receiver<Notification>,
    // Spool.
    spool:          Spool,
}

impl<R, W> Connection<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    async fn run<T>(peerfeed: Arc<PeerFeed>, codec: NntpCodec<T>, recv: mpsc::Receiver<Notification>, spool: Spool)
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (reader, writer) = codec.split();
        let mut conn = Connection {
            reader,
            writer,
            peerfeed,
            send_queue: VecDeque::new(),
            recv_queue: VecDeque::new(),
            stats: SessionStats::default(),
            sender_done: false,
            streaming: 20,
            notification: recv,
            spool,
        };
        let _ = tokio::spawn(async move {
            // TODO: log initial start, final stats.
            conn.feed().await
        });
    }

    // Feeder loop.
    async fn feed(&mut self) -> io::Result<()> {
        let mut xmit_busy = false;

        loop {

            // see if we need to pull an article from the peerfeed queue.
            let queue_len = self.recv_queue.len() + self.send_queue.len();
            if queue_len < self.streaming {
                if let Some(item) = self.peerfeed.get_item(queue_len == 0) {
                    self.send_queue.push_back(item);
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
                    match res.unwrap() {
                        Err(e) => {
                        // TODO: update stats, log, return
                            panic!("transmit: {}", e);
                        },
                        Ok(NntpInput::Eof) => {
                            // TODO handle EOF
                        },
                        Ok(NntpInput::Line(buf)) => {
                            // Got a reply. Find matching command.
                            match self.recv_queue.pop_front() {
                                None => panic!("recv queue out of sync"),
                                Some(Item::Check(art)) => {
                                    // TODO check status code, update stats.
                                    self.send_queue.push_back(Item::Takethis(art));
                                },
                                Some(Item::Takethis(art)) => {
                                    // TODO check status code, update stats.
                                },
                                Some(Item::Quit) => {
                                    // TODO check status code, update stats, return.
                                },
                            }
                        },
                        Ok(_) => {
                            // TODO: log error, return.
                            panic!("unexpected state");
                        },
                    }
                }
                res = self.notification.next() => {
                    // TODO: handle notification.
                }
            }
        }
    }

    async fn transmit_item(&mut self, item: Item) -> io::Result<()> {
        match item {
            Item::Check(art) => {
                let line = format!("CHECK {}\r\n", art.msgid);
                Pin::new(&mut self.writer).start_send(line.into())
            },
            Item::Takethis(art) => {
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
            Item::Quit => {
                Pin::new(&mut self.writer).start_send("QUIT\r\n".into())
            },
        }
    }
}

