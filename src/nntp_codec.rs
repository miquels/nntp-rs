use std::io;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::article::Article;
use crate::arttype::ArtTypeScanner;
use crate::server::Notification;
use crate::util::HashFeed;

use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream};
use memchr::memchr;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::watch;
use tokio::timer::{self, Delay};

const INITIAL_TIMEOUT: u64 = 60;
const READ_TIMEOUT: u64 = 630;
const WRITE_TIMEOUT: u64 = 120;

// Reading state (multiline)
#[derive(Debug, PartialEq, Eq)]
enum State {
    Data,
    Cr1Seen,
    Lf1Seen,
    DotSeen,
    Cr2Seen,
    Lf2Seen,
    Eof,
}

/// Reading mode.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CodecMode {
    /// Initial mode
    Connect,
    /// in "read line" mode.
    ReadLine,
    /// in "read multiline block" mode
    ReadBlock,
    /// in "read article" mode
    ReadArticle,
    /// at next read, return quit.
    Quit,
}

/// Stream object.
pub enum NntpInput {
    Connect,
    Eof,
    Line(BytesMut),
    Block(BytesMut),
    Article(Article),
    Notification(Notification),
}

/// NntpCodec implements both Stream to receive and Sink to send either
/// lines or multi-line blocks.
///
/// Like a tokio::io::codec, but it has to be a seperate implementation
/// because we need to switch between reading lines and multi-line blocks,
/// and we might want to do more advanced buffering later on.
pub struct NntpCodec {
    socket:          TcpStream,
    watcher:         watch::Receiver<Notification>,
    rd:              BytesMut,
    rd_pos:          usize,
    rd_overflow:     bool,
    rd_state:        State,
    rd_line_start:   usize,
    rd_reserve_size: usize,
    wr:              Bytes,
    arttype_scanner: ArtTypeScanner,
    wr_timeout:      Delay,
    rd_timeout:      Delay,
    rd_mode:         CodecMode,
    msgid:           Option<String>,
    notification:    Option<Notification>,
}

impl NntpCodec {
    /// Returns a new NntpCodec.
    pub fn new(socket: TcpStream, watcher: watch::Receiver<Notification>) -> NntpCodec {
        let _ = socket.set_nodelay(true);
        NntpCodec {
            socket:          socket,
            watcher:         watcher,
            rd:              BytesMut::new(),
            wr:              Bytes::new(),
            rd_pos:          0,
            rd_overflow:     false,
            rd_reserve_size: 0,
            rd_state:        State::Lf1Seen,
            rd_line_start:   0,
            arttype_scanner: ArtTypeScanner::new(),
            rd_timeout:      timer::delay_for(Duration::new(INITIAL_TIMEOUT, 0)),
            wr_timeout:      timer::delay_for(Duration::new(WRITE_TIMEOUT, 0)),
            rd_mode:         CodecMode::Connect,
            msgid:           None,
            notification:    None,
        }
    }

    // fill the read buffer as much as possible.
    fn fill_read_buf(&mut self, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        loop {
            // in a overflow situation, truncate the buffer. 32768 should be enough
            // to still have the headers available. Maybe we should scan to find the
            // header/body separator though.
            if self.rd_overflow && self.rd.len() > 32768 {
                self.rd.truncate(32768);
            }

            // Ensure the read buffer has capacity.
            // We grow the reserve_size during the session, and never shrink it.
            if self.rd_reserve_size < 131072 {
                let buflen = self.rd.len();
                let size = if buflen <= 1024 {
                    1024
                } else if buflen <= 16384 {
                    16384
                } else {
                    131072
                };
                self.rd_reserve_size = size;
            }
            let size = self.rd_reserve_size;
            self.rd.reserve(size);

            // Read data into the buffer if it's available.
            let socket = &mut self.socket;
            pin_utils::pin_mut!(socket);
            match socket.poll_read_buf(cx, &mut self.rd) {
                Poll::Ready(Ok(n)) if n == 0 => return Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
                _ => {},
            };
        }
    }

    fn read_line(&mut self) -> Poll<Option<Result<NntpInput, io::Error>>> {
        // resume where we left off.
        let bufpos = self.rd_pos;
        let buflen = self.rd.len();
        let nl_pos = {
            let buf = &self.rd[..];
            match memchr(b'\n', &buf[bufpos..buflen]) {
                Some(z) => bufpos + z,
                None => {
                    self.rd_pos = buflen;
                    return Poll::Pending;
                },
            }
        };
        let buf = self.rd.split_to(nl_pos + 1);
        self.rd_pos = 0;
        Poll::Ready(Some(Ok(NntpInput::Line(buf))))
    }

    fn read_block(&mut self, do_scan: bool) -> Poll<Option<Result<NntpInput, io::Error>>> {
        // resume where we left off.
        let mut bufpos = self.rd_pos;
        {
            // Until we have NLL this needs to be in a block because of the borrowing of 'rd'
            let buf = &self.rd[..];
            let buflen = buf.len();

            // State machine.
            while bufpos < buflen {
                trace!("bufpos {}, buflen {}, state {:?}", bufpos, buflen, self.rd_state);
                match self.rd_state {
                    State::Data => {
                        match memchr(b'\r', &buf[bufpos..buflen]) {
                            Some(z) => {
                                self.rd_state = State::Cr1Seen;
                                bufpos += z + 1;
                            },
                            None => {
                                bufpos = buflen;
                            },
                        }
                    },
                    State::Cr1Seen => {
                        self.rd_state = match buf[bufpos] {
                            b'\n' => {
                                // have a full line. scan it.
                                if do_scan {
                                    self.arttype_scanner
                                        .scan_line(&buf[self.rd_line_start..bufpos + 1]);
                                    self.rd_line_start = bufpos + 1;
                                }
                                State::Lf1Seen
                            },
                            b'\r' => State::Cr1Seen,
                            _ => State::Data,
                        };
                        bufpos += 1;
                    },
                    State::Lf1Seen => {
                        self.rd_state = match buf[bufpos] {
                            b'.' => State::DotSeen,
                            b'\r' => State::Cr1Seen,
                            _ => State::Data,
                        };
                        bufpos += 1;
                    },
                    State::DotSeen => {
                        self.rd_state = match buf[bufpos] {
                            b'\r' => State::Cr2Seen,
                            _ => State::Data,
                        };
                        bufpos += 1;
                    },
                    State::Cr2Seen => {
                        self.rd_state = match buf[bufpos] {
                            b'\r' => State::Cr1Seen,
                            b'\n' => State::Lf2Seen,
                            _ => State::Data,
                        };
                        bufpos += 1;
                        if self.rd_state == State::Lf2Seen {
                            break;
                        }
                    },
                    State::Lf2Seen => unreachable!(),
                    State::Eof => unreachable!(),
                }
            }
        }
        self.rd_pos = bufpos;

        trace!("final bufpos {}, state {:?}", bufpos, self.rd_state);

        // are we done?
        if self.rd_state == State::Lf2Seen {
            let buf = self.rd.split_to(self.rd_pos);
            self.rd_pos = 0;
            self.rd_line_start = 0;
            self.rd_state = State::Lf1Seen;
            if self.rd_overflow {
                // recoverable error.
                self.rd_overflow = false;
                return Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::InvalidData, "Overflow"))));
            }
            return Poll::Ready(Some(Ok(NntpInput::Block(buf))));
        }

        // continue
        Poll::Pending
    }

    // read_article is a small wrapper around read_block that returns
    // an Article struct with the BytesMut and some article metadata.
    fn read_article(&mut self) -> Poll<Option<Result<NntpInput, io::Error>>> {
        match self.read_block(true) {
            Poll::Ready(Some(Ok(NntpInput::Block(buf)))) => {
                let msgid = self.get_msgid();
                let article = Article {
                    hash:     HashFeed::hash_str(&msgid),
                    msgid:    msgid,
                    len:      buf.len(),
                    data:     buf,
                    arttype:  self.arttype_scanner.art_type(),
                    lines:    self.arttype_scanner.lines(),
                    pathhost: None,
                };
                self.arttype_scanner.reset();
                Poll::Ready(Some(Ok(NntpInput::Article(article))))
            },
            data => data,
        }
    }

    fn nntp_sink_poll_ready(&mut self, cx: &mut Context, flush: bool) -> Poll<Result<(), io::Error>> {

        if !self.wr.is_empty() {
            trace!("writing; remaining={}", self.wr.len());

            let socket = &mut self.socket;
            pin_utils::pin_mut!(socket);
            match socket.poll_write(cx, &self.wr) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write buffer to socket",
                    )));
                },
                Poll::Ready(Ok(n)) => {
                    let _ = self.wr.split_to(n);
                },
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {},
            }
        }

        if self.wr.is_empty() {
            if flush {
                let socket = &mut self.socket;
                pin_utils::pin_mut!(socket);
                match socket.poll_flush(cx) {
                    Poll::Ready(Ok(_)) => {
                        self.wr_timeout.reset(calc_delay(WRITE_TIMEOUT));
                        return Poll::Ready(Ok(()));
                    },
                    Poll::Ready(Err(e)) => {
                        self.wr_timeout.reset(calc_delay(WRITE_TIMEOUT));
                        return Poll::Ready(Err(e));
                    },
                    Poll::Pending => {},
                }
            } else {
                return Poll::Ready(Ok(()));
            }
        }

        // check the notification channel.
        let fut = self.watcher.recv();
        pin_utils::pin_mut!(fut);
        match fut.poll(cx) {
            Poll::Ready(item) => {
                match item {
                    Some(Notification::ExitNow) => {
                        return Poll::Ready(Err(io::ErrorKind::NotFound.into()));
                    },
                    Some(Notification::None) => {},
                    other => self.notification = other,
                }
            },
            Poll::Pending => {},
        }

        // check the timer.
        let timeout = &mut self.wr_timeout;
        pin_utils::pin_mut!(timeout);
        match timeout.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                let err = Err(io::Error::new(io::ErrorKind::TimedOut, "TimedOut"));
                Poll::Ready(err)
            },
        }
    }

    fn nntp_sink_start_send(&mut self, item: Bytes) -> Result<(), io::Error> {
        self.wr = item;
        Ok(())
    }

    fn nntp_sink_poll_close(&mut self, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        match self.nntp_sink_poll_ready(cx, true) {
            Poll::Ready(Ok(())) => {},
            other => return other,
        }
        self.socket.shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }

    pub fn set_mode(&mut self, mode: CodecMode) {
        self.rd_mode = mode.clone();
    }

    pub fn get_mode(&self) -> CodecMode {
        self.rd_mode.clone()
    }

    pub fn set_msgid(&mut self, msgid: impl Into<String>) {
        self.msgid = Some(msgid.into());
    }

    pub fn get_msgid(&mut self) -> String {
        self.msgid.take().unwrap_or("".to_string())
    }

    pub fn quit(&mut self) {
        self.rd_mode = CodecMode::Quit;
    }
}

/// This is the reading part, we return a stream of NntpInputs. Those can be:
///
/// - NntpInput::Connect:               returned once at the start
/// - NntpInput::Eof:                   end-of-file seen.
/// - NntpInput::Line(BytesMut):        single command line
/// - NntpInput::Block(BytesMut):       multiline block
///
impl Stream for NntpCodec {
    type Item = Result<NntpInput, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {

        // check the notification channel.
        let n = {
            let fut = self.watcher.recv();
            pin_utils::pin_mut!(fut);
            match fut.poll(cx) {
                Poll::Ready(item) => {
                    match item {
                        Some(Notification::ExitNow) => {
                            return Poll::Ready(Some(Err(io::ErrorKind::NotFound.into())));
                        },
                        Some(Notification::None) => None,
                        other => other,
                    }
                },
                Poll::Pending => None,
            }
        };
        if n.is_some() {
            self.notification = n;
        }

        let rd_mode = self.get_mode();
        match rd_mode {
            CodecMode::Connect => {
                self.set_mode(CodecMode::ReadLine);
                return Poll::Ready(Some(Ok(NntpInput::Connect)));
            },
            CodecMode::Quit => return Poll::Ready(None),
            _ => {},
        }

        // read as much data as we can.
        let sock_closed = match self.fill_read_buf(cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            Poll::Pending => false,
        };

        // Now if we have no input yet process the notifications.
        if self.rd.len() == 0 {
            if let Some(notification) = self.notification.take() {
                return Poll::Ready(Some(Ok(NntpInput::Notification(notification))));
            }
        }

        // Then process the data.
        if self.rd.len() > 0 {
            let res = match rd_mode {
                CodecMode::ReadLine => self.read_line(),
                CodecMode::ReadBlock => self.read_block(false),
                CodecMode::ReadArticle => self.read_article(),
                _ => unreachable!(),
            };
            match res {
                Poll::Pending => {},
                res => {
                    self.rd_timeout.reset(calc_delay(READ_TIMEOUT));
                    return res;
                },
            }
        }

        // see if the other side closed the socket.
        if sock_closed {
            if self.rd_state != State::Eof {
                // we were still processing data .. this was unexpected!
                if self.rd.len() > 0 {
                    // We were still reading a line, or a block, and hit EOF
                    // before the end. That's unexpected.
                    self.rd_state = State::Eof;
                    let err = Err(io::Error::new(io::ErrorKind::UnexpectedEof, "UnexpectedEof"));
                    return Poll::Ready(Some(err));
                }

                // return an end-of-file indication once, the next poll will
                // return end-of-stream.
                self.rd_state = State::Eof;
                return Poll::Ready(Some(Ok(NntpInput::Eof)));
            }

            // end stream.
            return Poll::Ready(None);
        }

        // check the timer.
        let timeout = &mut self.rd_timeout;
        pin_utils::pin_mut!(timeout);
        match timeout.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                let err = Err(io::Error::new(io::ErrorKind::TimedOut, "TimedOut"));
                self.quit();
                Poll::Ready(Some(err))
            },
        }
    }
}

/// The Sink is what writes the buffered data to the socket. We handle
/// a "Bytes" struct as one item. We do not buffer (yet), only one
/// item can be in-flight at a time.
impl Sink<Bytes> for NntpCodec {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        trace!("sink: poll_ready");
        self.nntp_sink_poll_ready(cx, false)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), io::Error> {
        trace!("sink: start_send");
        self.nntp_sink_start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        trace!("sink: poll_flush");
        self.nntp_sink_poll_ready(cx, true)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        trace!("sink: poll_close");
        self.nntp_sink_poll_close(cx)
    }
}

// helper
fn calc_delay(secs: u64) -> Instant {
    Instant::now().checked_add(Duration::new(secs, 0)).unwrap()
}
