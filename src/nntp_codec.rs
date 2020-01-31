use std::future::Future;
use std::io;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::article::Article;
use crate::arttype::ArtTypeScanner;
use crate::server::Notification;
use crate::util::{Buffer, HashFeed};

use bytes::Buf;
use futures::future::poll_fn;
use memchr::memchr;
use smallvec::SmallVec;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::watch;
use tokio::time::{self, Delay, Instant};

pub const INITIAL_TIMEOUT: u64 = 60;
pub const READ_TIMEOUT: u64 = 630;
pub const WRITE_TIMEOUT: u64 = 120;

// Reading state (multiline)
#[derive(Debug, PartialEq, Eq)]
enum State {
    Data,
    Cr1Seen,
    Lf1Seen,
    DotSeen,
    Cr2Seen,
    Lf2Seen,
}

/// Reading mode.
#[derive(Debug, PartialEq, Eq, Clone)]
enum CodecMode {
    /// in "read line" mode.
    ReadLine,
    /// in "read multiline block" mode
    ReadBlock,
    /// in "read article" mode
    ReadArticle,
}

/// Possible return values from read_line()
pub enum NntpLine {
    Eof,
    Line(Buffer),
    Notification(Notification),
}

// Internal return values from poll_read().
enum NntpInput {
    Eof,
    Line(Buffer),
    Block(Buffer),
    Article(Article),
    Notification(Notification),
}

/// NntpCodec precursor.
pub struct NntpCodecBuilder {
    socket:   TcpStream,
    watcher:  Option<watch::Receiver<Notification>>,
    rd_tmout: Option<Duration>,
    wr_tmout: Option<Duration>,
}

impl NntpCodecBuilder {
    /// New builder for a NntpCodec.
    pub fn new(socket: TcpStream) -> NntpCodecBuilder {
        NntpCodecBuilder {
            socket,
            watcher: None,
            rd_tmout: None,
            wr_tmout: None,
        }
    }

    /// Set read timeout
    pub fn read_timeout(mut self, secs: u64) -> Self {
        self.rd_tmout = Some(Duration::from_secs(secs));
        self
    }

    /// Set write timeout
    pub fn write_timeout(mut self, secs: u64) -> Self {
        self.wr_tmout = Some(Duration::from_secs(secs));
        self
    }

    /// Set the watcher we watch for receipt of Notifications.
    pub fn watcher(mut self, w: watch::Receiver<Notification>) -> Self {
        self.watcher = Some(w);
        self
    }

    /// Build the final NntpCodec.
    pub fn build(self) -> NntpCodec {
        let _ = self.socket.set_nodelay(true);
        NntpCodec {
            socket:          self.socket,
            watcher:         self.watcher,
            rd:              Buffer::new(),
            rd_pos:          0,
            rd_overflow:     false,
            rd_reserve_size: 0,
            rd_state:        State::Lf1Seen,
            rd_line_start:   0,
            rd_mode:         CodecMode::ReadLine,
            rd_tmout:        self.rd_tmout,
            rd_timer:        None,
            wr:              Buffer::new(),
            wr_tmout:        self.wr_tmout,
            wr_timer:        None,
            arttype_scanner: ArtTypeScanner::new(),
            notification:    None,
            msgid:           None,
        }
    }
}

/// NntpCodec can read lines/blocks/articles from  a TcpStream, and
/// write buffers to a TcpStream.
pub struct NntpCodec {
    socket:          TcpStream,
    watcher:         Option<watch::Receiver<Notification>>,
    rd:              Buffer,
    rd_pos:          usize,
    rd_overflow:     bool,
    rd_state:        State,
    rd_line_start:   usize,
    rd_reserve_size: usize,
    rd_tmout:        Option<Duration>,
    rd_timer:        Option<Delay>,
    rd_mode:         CodecMode,
    wr:              Buffer,
    wr_tmout:        Option<Duration>,
    wr_timer:        Option<Delay>,
    arttype_scanner: ArtTypeScanner,
    notification:    Option<Notification>,
    msgid:           Option<String>,
}

impl NntpCodec {
    /// Returns a new NntpCodec. For more control, use builder() or NntpCodecBuilder::new().
    pub fn new(socket: TcpStream) -> NntpCodec {
        NntpCodecBuilder::new(socket).build()
    }

    /// New builder for a NntpCodec.
    pub fn builder(socket: TcpStream) -> NntpCodecBuilder {
        NntpCodecBuilder::new(socket)
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

    fn poll_read_line(&mut self) -> Poll<io::Result<NntpInput>> {
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
        Poll::Ready(Ok(NntpInput::Line(buf)))
    }

    fn poll_read_block(&mut self, do_scan: bool) -> Poll<io::Result<NntpInput>> {
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
                return Poll::Ready(Err(ioerr!(InvalidData, "Overflow")));
            }
            return Poll::Ready(Ok(NntpInput::Block(buf)));
        }

        // continue
        Poll::Pending
    }

    // read_article is a small wrapper around read_block that returns
    // an Article struct with the Buffer and some article metadata.
    fn poll_read_article(&mut self) -> Poll<io::Result<NntpInput>> {
        match self.poll_read_block(true) {
            Poll::Ready(Ok(NntpInput::Block(buf))) => {
                let msgid = self.msgid.take().unwrap_or("".to_string());
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
                Poll::Ready(Ok(NntpInput::Article(article)))
            },
            other => other,
        }
    }

    fn poll_read(&mut self, cx: &mut Context) -> Poll<io::Result<NntpInput>> {
        // first, check the notification channel.
        if let Some(watcher) = self.watcher.as_mut() {
            let n = {
                let fut = watcher.recv();
                pin_utils::pin_mut!(fut);
                match fut.poll(cx) {
                    Poll::Ready(item) => {
                        match item {
                            Some(Notification::ExitNow) => {
                                return Poll::Ready(Err(io::ErrorKind::NotFound.into()));
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
        }

        // read as much data as we can.
        let sock_closed = match self.fill_read_buf(cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => false,
        };

        // Now if we have no input yet process the notifications.
        if self.rd.len() == 0 && self.rd_mode == CodecMode::ReadLine {
            if let Some(notification) = self.notification.take() {
                return Poll::Ready(Ok(NntpInput::Notification(notification)));
            }
        }

        // Then process the data.
        if self.rd.len() > 0 {
            let res = match self.rd_mode {
                CodecMode::ReadLine => self.poll_read_line(),
                CodecMode::ReadBlock => self.poll_read_block(false),
                CodecMode::ReadArticle => self.poll_read_article(),
            };
            match res {
                Poll::Pending => {
                    // we read some data, so reset the timer.
                    self.reset_rd_timer();
                },
                res => {
                    // we got a full line/block/article.
                    return res;
                },
            }
        }

        // see if the other side closed the socket.
        if sock_closed {
            if self.rd.len() > 0 {
                // We were still reading a line, or a block, and hit EOF
                // before the end. That's unexpected.
                return Poll::Ready(Err(ioerr!(UnexpectedEof, "UnexpectedEof")));
            }
            // EOF.
            return Poll::Ready(Ok(NntpInput::Eof));
        }

        // check the timer.
        if let Some(timeout) = self.rd_timer.as_mut() {
            pin_utils::pin_mut!(timeout);
            return match timeout.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => Poll::Ready(Err(ioerr!(TimedOut, "TimedOut"))),
            };
        }
        Poll::Pending
    }

    fn poll_write(&mut self, cx: &mut Context, buf: &mut impl Buf) -> Poll<io::Result<()>> {
        if buf.remaining() > 0 {
            //
            // See if we can write more data to the socket.
            //
            trace!("writing; remaining={}", self.wr.len());
            let socket = &mut self.socket;
            pin_utils::pin_mut!(socket);
            match socket.poll_write(cx, buf.bytes()) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write buffer to socket",
                    )));
                },
                Poll::Ready(Ok(n)) => {
                    buf.advance(n);
                },
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {},
            }

            //
            // if there is more to write, reset the timer.
            //
            if buf.remaining() > 0 {
                self.reset_wr_timer();
            }
        }

        //
        // if the buffer is empty, flush the underlying socket.
        //
        if buf.remaining() == 0 {
            let socket = &mut self.socket;
            pin_utils::pin_mut!(socket);
            match socket.poll_flush(cx) {
                Poll::Ready(Ok(_)) => {
                    return Poll::Ready(Ok(()));
                },
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                },
                Poll::Pending => {},
            }
        }

        //
        // now check the notification channel.
        //
        if let Some(watcher) = self.watcher.as_mut() {
            let fut = watcher.recv();
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
        }

        //
        // finally, check the timer.
        //
        if let Some(timeout) = self.wr_timer.as_mut() {
            pin_utils::pin_mut!(timeout);
            return match timeout.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => {
                    let err = Err(io::Error::new(io::ErrorKind::TimedOut, "TimedOut"));
                    Poll::Ready(err)
                },
            };
        }
        Poll::Pending
    }

    // reset the read timer.
    fn reset_rd_timer(&mut self) {
        if let Some(ref tmout) = self.rd_tmout {
            if let Some(ref mut timer) = self.rd_timer {
                timer.reset(calc_delay(tmout));
            } else {
                self.rd_timer = Some(time::delay_for(tmout.clone()));
            }
        }
    }

    // reset the write timer.
    fn reset_wr_timer(&mut self) {
        if let Some(ref tmout) = self.wr_tmout {
            if let Some(ref mut timer) = self.wr_timer {
                timer.reset(calc_delay(tmout));
            } else {
                self.wr_timer = Some(time::delay_for(tmout.clone()));
            }
        }
    }

    /// Read a line.
    pub async fn read_line(&mut self) -> io::Result<NntpLine> {
        self.rd_mode = CodecMode::ReadLine;
        self.reset_rd_timer();
        match poll_fn(|cx: &mut Context| self.poll_read(cx)).await {
            Ok(NntpInput::Eof) => Ok(NntpLine::Eof),
            Ok(NntpInput::Notification(n)) => Ok(NntpLine::Notification(n)),
            Ok(NntpInput::Line(buf)) => Ok(NntpLine::Line(buf)),
            Ok(_) => Err(ioerr!(Other, "read_line: unexpected NntpInput state")),
            Err(e) => Err(e),
        }
    }

    /// Read a line with timeout.
    pub async fn read_line_with_timeout(&mut self, d: Duration) -> io::Result<NntpLine> {
        self.rd_mode = CodecMode::ReadLine;
        //
        // This is not very efficient, but we really only call this method once
        // in a server session, at the initial connect, because we use a lower
        // timeout for the first command only.
        //
        let old_tmout = std::mem::replace(&mut self.rd_tmout, Some(d));
        self.reset_rd_timer();
        let res = match poll_fn(|cx: &mut Context| self.poll_read(cx)).await {
            Ok(NntpInput::Eof) => Ok(NntpLine::Eof),
            Ok(NntpInput::Notification(n)) => Ok(NntpLine::Notification(n)),
            Ok(NntpInput::Line(buf)) => Ok(NntpLine::Line(buf)),
            Ok(_) => Err(ioerr!(Other, "read_line: unexpected NntpInput state")),
            Err(e) => Err(e),
        };
        self.rd_tmout = old_tmout;
        self.rd_timer = None;
        res
    }

    /// Read a block.
    pub async fn read_block(&mut self) -> io::Result<Buffer> {
        self.rd_mode = CodecMode::ReadBlock;
        self.reset_rd_timer();
        match poll_fn(|cx: &mut Context| self.poll_read(cx)).await {
            Ok(NntpInput::Block(buf)) => Ok(buf),
            Ok(_) => Err(ioerr!(Other, "read_block: unexpected NntpInput state")),
            Err(e) => Err(e),
        }
    }

    /// Read an article.
    pub async fn read_article(&mut self, msgid: impl Into<String>) -> io::Result<Article> {
        self.rd_mode = CodecMode::ReadArticle;
        self.msgid = Some(msgid.into());
        self.reset_rd_timer();
        match poll_fn(|cx: &mut Context| self.poll_read(cx)).await {
            Ok(NntpInput::Article(art)) => Ok(art),
            Ok(_) => Err(ioerr!(Other, "read_block: unexpected NntpInput state")),
            Err(e) => Err(e),
        }
    }

    /// Write a buffer that can be turned into a `Buffer` struct.
    pub async fn write(&mut self, buf: impl Into<Buffer>) -> io::Result<()> {
        self.reset_wr_timer();
        let mut buf = buf.into();
        poll_fn(move |cx: &mut Context| self.poll_write(cx, &mut buf)).await
    }

    /// Write a buffer that impl's the `Buf` trait.
    pub async fn write_buf(&mut self, buf: impl Buf) -> io::Result<()> {
        self.reset_wr_timer();
        let mut buf = buf;
        poll_fn(move |cx: &mut Context| self.poll_write(cx, &mut buf)).await
    }
}

// helper
fn calc_delay(d: &Duration) -> Instant {
    Instant::now().checked_add(d.clone()).unwrap()
}

/// NNTP response parsing
pub struct NntpResponse<'a> {
    /// reply code: 100..599
    pub code:  u32,
    /// arguments.
    pub args:  SmallVec<[&'a str; 5]>,
    /// short (< 100 chars) version of the response string for diagnostics.
    pub short: &'a str,
}

impl<'a> NntpResponse<'a> {
    /// Parse NNTP response.
    pub fn parse(r: &'a [u8]) -> io::Result<NntpResponse<'a>> {
        let (resp, short) = NntpResponse::utf8_response(r)?;

        // get code.
        let mut args = resp.split_ascii_whitespace();
        let num = args.next().ok_or_else(|| ioerr!(InvalidData, "empty response"))?;
        let code = match num.parse::<u32>() {
            Ok(code) if code >= 100 && code <= 599 => code,
            _ => return Err(ioerr!(InvalidData, "invalid response: {}", short)),
        };

        // get rest of the args, up to a maximum.
        let mut v = SmallVec::<[&'a str; 5]>::new();
        for w in args {
            if v.len() == v.inline_size() {
                break;
            }
            v.push(w);
        }
        let nargs = v.len();

        // now some checks.
        let ok = match code {
            // CHECK/TAKETHIS: 1 argument: message-id
            238 | 431 | 438 | 239 | 439 => nargs >= 1,
            // 1 argument: capability-label.
            401 => nargs >= 1,
            // ARTICLE,HEAD,BODY,LAST/NEXT/STAT: 2 arguments: n message-id
            220 | 221 | 222 | 223 => nargs >= 2,
            // GROUP/LISTGROUP => 4 arguments: number low high group
            211 => nargs >= 4,
            // DATE: 1 argument: yyyymmddhhmmss
            111 => nargs >= 1,
            _ => true,
        };

        if !ok {
            return Err(ioerr!(
                InvalidData,
                "invalid response: missing arguments: {}",
                short
            ));
        }

        Ok(NntpResponse { code, args: v, short })
    }

    /// version of response suitable for diagnostics / logging.
    pub fn diag_response(r: &'a [u8]) -> &'a str {
        NntpResponse::utf8_response(r)
            .map(|(_, diag)| diag)
            .unwrap_or("[invalid-utf8]")
    }

    // decode to utf-8, return the utf-8 string and a limited length version for diagnostics.
    fn utf8_response(r: &'a [u8]) -> io::Result<(&'a str, &'a str)> {
        // strip trailing \r\n
        let mut n = r.len();
        while n > 0 && (r[n - 1] == b'\r' || r[n - 1] == b'\n') {
            n -= 1;
        }
        let r = &r[..n];

        // decode utf8
        let resp = match std::str::from_utf8(r) {
            Ok(resp) => resp,
            Err(_) => return Err(ioerr!(InvalidData, "[invalid-utf8]")),
        };

        // length-limited string for logging purposes.
        let mut lm = if resp.len() > 100 {
            let mut n = 100;
            while n < resp.len() && !resp.is_char_boundary(n) {
                n += 1;
            }
            &resp[..n]
        } else {
            resp
        };
        if lm == "" {
            lm = "[empty]";
        }

        Ok((resp, lm))
    }
}
