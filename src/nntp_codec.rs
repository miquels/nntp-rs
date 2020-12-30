use std::any::Any;
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Debug};
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::article::Article;
use crate::arttype::ArtTypeScanner;
use crate::bus::Notification;
use crate::util::{Buffer, HashFeed};

use bytes::Buf;
use futures::future::poll_fn;
use futures::sink::Sink;
use memchr::memchr;
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::pin;
use tokio_stream::Stream;
use tokio::time::{self, Sleep, Instant};

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
}

// Return values from poll_read().
pub enum NntpInput {
    Eof,
    Line(Buffer),
    Block(Buffer),
    Article(Article),
}

impl Debug for NntpInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            &NntpInput::Eof => "NntpInput::Eof",
            &NntpInput::Line(_) => "NntpInput::Line",
            &NntpInput::Block(_) => "NntpInput::Block",
            &NntpInput::Article(_) => "NntpInput::Article",
        };
        write!(f, "{}", name)
    }
}

/// NntpCodec precursor.
pub struct NntpCodecBuilder<S = TcpStream> {
    socket:   S,
    rd_tmout: Option<Duration>,
    wr_tmout: Option<Duration>,
}

impl<S> NntpCodecBuilder<S> {
    /// New builder for a NntpCodec.
    pub fn new(socket: S) -> NntpCodecBuilder<S> {
        NntpCodecBuilder {
            socket,
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

    /// Build the final NntpCodec.
    pub fn build(self) -> NntpCodec<S>
    where S: Any {
        NntpCodec {
            socket:          self.socket,
            rd:              Buffer::new(),
            rd_pos:          0,
            rd_eof:          false,
            rd_overflow:     false,
            rd_reserve_size: 0,
            rd_state:        State::Lf1Seen,
            rd_line_start:   0,
            rd_mode:         CodecMode::ReadLine,
            rd_tmout:        self.rd_tmout,
            rd_timer:        None,
            wr_bufs:         Vec::new(),
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
pub struct NntpCodec<S = TcpStream> {
    socket:          S,
    rd:              Buffer,
    rd_pos:          usize,
    rd_eof:          bool,
    rd_overflow:     bool,
    rd_state:        State,
    rd_line_start:   usize,
    rd_reserve_size: usize,
    rd_tmout:        Option<Duration>,
    rd_timer:        Option<Pin<Box<Sleep>>>,
    rd_mode:         CodecMode,
    wr_bufs:         Vec<Buffer>,
    wr_tmout:        Option<Duration>,
    wr_timer:        Option<Pin<Box<Sleep>>>,
    arttype_scanner: ArtTypeScanner,
    notification:    Option<Notification>,
    msgid:           Option<String>,
}

impl<S> NntpCodec<S>
where S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    /// Returns a new NntpCodec. For more control, use builder() or NntpCodecBuilder::new().
    pub fn new(socket: S) -> NntpCodec<S> {
        NntpCodecBuilder::new(socket).build()
    }

    /// New builder for a NntpCodec.
    pub fn builder(socket: S) -> NntpCodecBuilder<S> {
        NntpCodecBuilder::new(socket)
    }

    /// Split the codec into a read and a write part.
    pub fn split(
        self,
    ) -> (
        NntpCodec<Box<dyn AsyncRead + Send + Unpin>>,
        NntpCodec<Box<dyn AsyncWrite + Send + Unpin>>,
    ) {
        let (rsock, wsock) = tokio::io::split(self.socket);

        let r = NntpCodec {
            socket:          Box::new(rsock) as Box<dyn AsyncRead + Send + Unpin>,
            rd:              self.rd,
            rd_pos:          self.rd_pos,
            rd_eof:          self.rd_eof,
            rd_overflow:     self.rd_overflow,
            rd_state:        self.rd_state,
            rd_line_start:   self.rd_line_start,
            rd_reserve_size: self.rd_reserve_size,
            rd_tmout:        self.rd_tmout,
            rd_timer:        self.rd_timer,
            rd_mode:         self.rd_mode,
            arttype_scanner: self.arttype_scanner,
            notification:    self.notification,
            msgid:           self.msgid,
            wr_bufs:         Vec::new(),
            wr_tmout:        None,
            wr_timer:        None,
        };

        let w = NntpCodec {
            socket:          Box::new(wsock) as Box<dyn AsyncWrite + Send + Unpin>,
            rd:              Buffer::new(),
            rd_pos:          0,
            rd_eof:          false,
            rd_overflow:     false,
            rd_state:        State::Data,
            rd_line_start:   0,
            rd_reserve_size: 0,
            rd_tmout:        None,
            rd_timer:        None,
            rd_mode:         CodecMode::ReadLine,
            arttype_scanner: ArtTypeScanner::new(),
            notification:    None,
            msgid:           None,
            wr_bufs:         self.wr_bufs,
            wr_tmout:        self.wr_tmout,
            wr_timer:        self.wr_timer,
        };

        (r, w)
    }

    /// Send a command and wait for the response.
    pub async fn command(&mut self, cmd: impl Into<String>) -> io::Result<NntpResponse> {
        let mut cmd = cmd.into();
        let is_listgroup = {
            let b = cmd.as_bytes();
            if b.len() >= 9 && (b[0] == b'l' || b[0] == b'L') {
                cmd.to_lowercase().starts_with("listgroup")
            } else {
                false
            }
        };
        cmd.push_str("\r\n");
        self.write(cmd).await?;
        let mut response = self.read_line().await.and_then(|l| NntpResponse::try_from(l))?;
        if response.is_multiline(is_listgroup) {
            response.body = self.read_block().await?.into_bytes();
            self.rd_mode = CodecMode::ReadLine;
        }
        Ok(response)
    }
}

impl<S> NntpCodec<S>
where
    S: AsyncRead,
    S: Unpin,
{
    // fill the read buffer as much as possible.
    fn fill_read_buf(&mut self, cx: &mut Context) -> Poll<Result<usize, io::Error>> {
        if self.rd_eof {
            return Poll::Ready(Ok(0));
        }
        let mut bytes_read = 0usize;
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
            let socket = Pin::new(&mut self.socket);
            match self.rd.poll_read(socket, cx) {
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        self.rd_eof = true;
                        return Poll::Ready(Ok(bytes_read));
                    }
                    bytes_read += n;
                },
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending if bytes_read > 0 => return Poll::Ready(Ok(bytes_read)),
                Poll::Pending => return Poll::Pending,
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
                // log::trace!("bufpos {}, buflen {}, state {:?}", bufpos, buflen, self.rd_state);
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

        // log::trace!("final bufpos {}, state {:?}", bufpos, self.rd_state);

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
        // read as much data as we can.
        let sock_closed = match self.fill_read_buf(cx) {
            Poll::Ready(Ok(0)) => true,
            Poll::Ready(Ok(_)) => false,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => false,
        };

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
            return match timeout.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => Poll::Ready(Err(ioerr!(TimedOut, "TimedOut"))),
            };
        }
        Poll::Pending
    }

    // reset the read timer.
    fn reset_rd_timer(&mut self) {
        if let Some(ref tmout) = self.rd_tmout {
            if let Some(ref mut timer) = self.rd_timer {
                timer.as_mut().reset(calc_delay(tmout));
            } else {
                self.rd_timer = Some(Box::pin(time::sleep(tmout.clone())));
            }
        }
    }

    /// Read a line.
    pub async fn read_line(&mut self) -> io::Result<NntpLine> {
        self.rd_mode = CodecMode::ReadLine;
        self.reset_rd_timer();
        match poll_fn(|cx: &mut Context| self.poll_read(cx)).await {
            Ok(NntpInput::Eof) => Ok(NntpLine::Eof),
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
            Ok(NntpInput::Line(buf)) => Ok(NntpLine::Line(buf)),
            Ok(_) => Err(ioerr!(Other, "read_line: unexpected NntpInput state")),
            Err(e) => Err(e),
        };
        self.rd_tmout = old_tmout;
        self.rd_timer = None;
        res
    }

    /// Read a response.
    pub async fn read_response(&mut self) -> io::Result<NntpResponse> {
        self.read_line().await?.try_into()
    }

    /// Read a response with timeout.
    pub async fn read_response_with_timeout(&mut self, d: Duration) -> io::Result<NntpResponse> {
        self.read_line_with_timeout(d).await?.try_into()
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

    pub fn input_empty(&self) -> bool {
        self.rd.len() == 0
    }
}

impl<S> NntpCodec<S>
where
    S: AsyncWrite,
    S: Unpin,
{
    fn poll_write(&mut self, cx: &mut Context, buf: &mut impl Buf) -> Poll<io::Result<()>> {
        let mut wrote_something = false;

        while buf.remaining() > 0 {
            //
            // See if we can write more data to the socket.
            //
            //log::trace!("writing; remaining={}", buf.remaining());
            let socket = &mut self.socket;
            pin!(socket);
            match socket.poll_write(cx, buf.chunk()) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write buffer to socket",
                    )));
                },
                Poll::Ready(Ok(n)) => {
                    wrote_something = true;
                    buf.advance(n);
                },
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => break,
            }
        }

        //
        // reset the timer if we wrote any data.
        //
        if wrote_something && buf.remaining() > 0 {
            self.reset_wr_timer();
        }

        //
        // if the buffer is empty, flush the underlying socket.
        //
        if buf.remaining() == 0 {
            let socket = &mut self.socket;
            pin!(socket);
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
        // finally, check the timer.
        //
        if let Some(timeout) = self.wr_timer.as_mut() {
            pin!(timeout);
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

    // reset the write timer.
    fn reset_wr_timer(&mut self) {
        if let Some(ref tmout) = self.wr_tmout {
            if let Some(ref mut timer) = self.wr_timer {
                timer.as_mut().reset(calc_delay(tmout));
            } else {
                self.wr_timer = Some(Box::pin(time::sleep(tmout.clone())));
            }
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.reset_wr_timer();
        if self.wr_bufs.len() > 0 {
            let mut this = Pin::new(self);
            poll_fn(move |cx: &mut Context| this.as_mut().poll_flush(cx)).await?;
        }
        Ok(())
    }

    /// Write a buffer that can be turned into a `Buffer` struct.
    pub async fn write(&mut self, buf: impl Into<Buffer>) -> io::Result<()> {
        let buf = buf.into();
        self.wr_bufs.push(buf);
        self.flush().await?;
        let mut this = Pin::new(self);
        poll_fn(move |cx: &mut Context| this.as_mut().poll_flush(cx)).await
    }

    /// Write a buffer that impl's the `Buf` trait.
    pub async fn write_buf(&mut self, buf: impl Buf) -> io::Result<()> {
        self.flush().await?;
        let mut buf = buf;
        poll_fn(move |cx: &mut Context| self.poll_write(cx, &mut buf)).await
    }

    /// Is there still data pending, waiting to be written?
    pub fn write_is_pending(&self) -> bool {
        for buf in &self.wr_bufs {
            if buf.remaining() > 0 {
                return true;
            }
        }
        false
    }
}

impl<S> Stream for NntpCodec<S>
where
    S: AsyncRead,
    S: Unpin,
{
    type Item = io::Result<NntpInput>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        this.reset_rd_timer();
        match this.poll_read(cx) {
            Poll::Ready(x) => Poll::Ready(Some(x)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Sink<Buffer> for NntpCodec<S>
where S: AsyncWrite + Unpin
{
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut();
        if this.wr_bufs.len() == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut bufs = std::mem::replace(&mut this.wr_bufs, Vec::new());
        let res = loop {
            // TODO FIXME: use poll_write_vectored (in tokio 0.3).
            let res = this.poll_write(cx, &mut bufs[0]);
            if bufs[0].remaining() == 0 {
                bufs.remove(0);
            }
            if bufs.len() == 0 {
                return Poll::Ready(Ok(()));
            }
            match res {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(Err(e)) => break Poll::Ready(Err(e)),
                _ => {},
            }
        };
        this.wr_bufs = bufs;
        res
    }

    fn start_send(mut self: Pin<&mut Self>, item: Buffer) -> Result<(), Self::Error> {
        if item.len() < 256 && self.wr_bufs.len() > 0 {
            // This is a bit of a hack. we want to put consecutive
            // CHECKS in one packet. Maybe better to mark the buffer with
            // a type like 'check', 'takethis', 'header', 'body', article
            // so that we can merge TAKETHIS + headers + article, etc.
            let last = self.wr_bufs.len() - 1;
            let b = &self.wr_bufs[last];
            if b.starts_with(b"CHECK <") && b.len() < 1200 {
                self.as_mut().wr_bufs[last].extend_from_slice(&item);
                return Ok(());
            }
        }
        self.as_mut().wr_bufs.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.poll_ready(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        // NOTE: we could call self.socket.shutdown() here, but then
        // we'd need to store the future it returns as state.
        self.poll_flush(cx)
    }
}

// helper
fn calc_delay(d: &Duration) -> Instant {
    Instant::now().checked_add(d.clone()).unwrap()
}

struct NntpArgs<'a> {
    data: &'a [u8],
    pos:  usize,
}

fn split_whitespace<'a>(data: &'a str) -> NntpArgs<'a> {
    NntpArgs {
        data: data.as_bytes(),
        pos:  0,
    }
}

impl<'a> NntpArgs<'a> {
    fn is_white(&self, pos: usize) -> bool {
        let c = self.data[pos];
        c == b' ' || c == b'\t' || c == b'\r' || c == b'\n'
    }
}

impl<'a> Iterator for NntpArgs<'a> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut start = self.pos;
        while start < self.data.len() && self.is_white(start) {
            start += 1;
        }
        if start == self.data.len() {
            return None;
        }
        let mut end = start;
        while end < self.data.len() && !self.is_white(end) {
            end += 1;
        }
        self.pos = end;
        Some(Range::<usize> { start, end })
    }
}

/// NNTP response parsing
pub struct NntpResponse {
    /// The entire response line.
    pub line:  String,
    /// reply code: 100..599
    pub code:  u32,
    /// body of a multi-line response.
    pub body:  Vec<u8>,
    // arguments.
    args:      SmallVec<[Range<usize>; 5]>,
    // short (< 100 chars) version of the response string for diagnostics.
    short_len: usize,
}

impl NntpResponse {
    /// Parse NNTP response.
    pub fn parse(rawline: impl Into<Buffer>) -> io::Result<NntpResponse> {
        // decode utf8, strip CRLF.
        let rawline = rawline.into().into_bytes();
        let mut line = String::from_utf8(rawline).map_err(|_| ioerr!(InvalidData, "[invalid-utf8]"))?;
        if line.ends_with("\r\n") {
            line.truncate(line.len() - 2);
        }

        // length-limited string for logging purposes.
        let short_len = if line.len() > 100 {
            let mut n = 100;
            while n < line.len() && !line.is_char_boundary(n) {
                n += 1;
            }
            n
        } else {
            line.len()
        };
        let short = &line[..short_len];

        // get code.
        let mut args = split_whitespace(&line);
        let idx = args.next().ok_or_else(|| ioerr!(InvalidData, "empty response"))?;
        let code = match line[idx].parse::<u32>() {
            Ok(code) if code >= 100 && code <= 599 => code,
            _ => return Err(ioerr!(InvalidData, "invalid response: {}", short)),
        };

        // get rest of the args, up to a maximum.
        let mut v = SmallVec::<[Range<usize>; 5]>::new();
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

        Ok(NntpResponse {
            code,
            line,
            args: v,
            short_len,
            body: Vec::new(),
        })
    }

    /// length-limited response for logging and diagnostics
    pub fn short(&self) -> &str {
        &self.line[..self.short_len]
    }

    /// Get nth argument.
    pub fn arg(&self, idx: usize) -> &str {
        if idx >= self.args.len() {
            return "";
        }
        &self.line[self.args[idx].clone()]
    }

    /// Get number of args.
    pub fn args_len(&self) -> usize {
        self.args.len()
    }

    /// is this a multi-line response?
    pub fn is_multiline(&self, is_listgroup: bool) -> bool {
        match self.code {
            100 | 101 | 215 | 220 | 221 | 222 | 224 | 225 | 230 | 231 => true,
            211 if is_listgroup => true,
            _ => false,
        }
    }
}

impl TryFrom<NntpLine> for NntpResponse {
    type Error = io::Error;

    fn try_from(value: NntpLine) -> io::Result<NntpResponse> {
        match value {
            NntpLine::Eof => Err(ioerr!(UnexpectedEof, "Connection closed")),
            NntpLine::Line(buffer) => NntpResponse::parse(buffer),
        }
    }
}

impl TryFrom<NntpInput> for NntpResponse {
    type Error = io::Error;

    fn try_from(value: NntpInput) -> io::Result<NntpResponse> {
        match value {
            NntpInput::Eof => Err(ioerr!(UnexpectedEof, "Connection closed")),
            NntpInput::Line(buffer) => NntpResponse::parse(buffer),
            _ => Err(ioerr!(InvalidData, "unexpected NntpInput state")),
        }
    }
}
