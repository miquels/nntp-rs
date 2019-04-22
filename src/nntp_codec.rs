use std::io;
use std::net::Shutdown;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::article::Article;
use crate::arttype::ArtTypeScanner;

use bytes::{Bytes, BytesMut};
use futures::{Async, AsyncSink,  Poll, Stream, Sink, StartSend};
use memchr::memchr;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

// Reading state (multiline)
#[derive(Debug,PartialEq,Eq)]
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
#[derive(Debug,PartialEq,Eq)]
pub enum CodecMode {
    /// Initial mode
    Connect     = 1,
    /// in "read line" mode.
    ReadLine    = 2,
    /// in "read multiline block" mode
    ReadBlock   = 3,
    /// in "read article" mode
    ReadArticle = 4,
    /// at next read, return quit.
    Quit        = 5,
    /// report write error.
    WriteError  = 6,
}

impl From<usize> for CodecMode {
    fn from(value: usize) -> Self {
        match value {
            1 => CodecMode::Connect,
            2 => CodecMode::ReadLine,
            3 => CodecMode::ReadBlock,
            4 => CodecMode::ReadArticle,
            5 => CodecMode::Quit,
            6 => CodecMode::WriteError,
            _ => unimplemented!(),
        }
    }
}


/// Stream object.
pub enum NntpInput {
    Connect,
    Eof,
    WriteError(io::Error),
    Line(BytesMut),
    Block(BytesMut),
    Article(Article),
}

/// NntpCodec implements both Stream to receive and Sink to send either
/// lines or multi-line blocks.
///
/// Like a tokio::io::codec, but it has to be a seperate implementation
/// because we need to switch between reading lines and multi-line blocks,
/// and we might want to do more advanced buffering later on.
pub struct NntpCodec {
    socket:	            TcpStream,
    rd:		            BytesMut,
    rd_pos:             usize,
    rd_overflow:        bool,
    rd_state:           State,
    rd_line_start:      usize,
    wr:		            Bytes,
    control:            NntpCodecControl,
    arttype_scanner:    ArtTypeScanner,
}

/// Changes the behaviour of the codec.
#[derive(Clone)]
pub struct NntpCodecControl {
    rd_mode:        Arc<AtomicUsize>,
    error:          Arc<Mutex<Option<io::Error>>>,
    msgid:          Arc<Mutex<Option<String>>>,
}

impl NntpCodec {
    /// Returns a new NntpCodec.
    pub fn new(socket: TcpStream) -> NntpCodec {
        NntpCodec {
			socket:	            socket,
			rd:		            BytesMut::new(),
			wr:		            Bytes::new(),
            rd_pos:             0,
            rd_overflow:        false,
            rd_state:           State::Lf1Seen,
            rd_line_start:      0,
            control:            NntpCodecControl::new(),
            arttype_scanner:    ArtTypeScanner::new(),
        }
    }

    pub fn control(&self) -> NntpCodecControl {
        self.control.clone()
    }

    // fill the read buffer as much as possible.
    fn fill_read_buf(&mut self) -> Result<Async<()>, io::Error> {
        loop {
            let mut buflen = self.rd.len();
            if self.rd_overflow && buflen > 32768 {
                self.rd.truncate(32768);
                buflen = 32768;
            }

            // Ensure the read buffer has capacity.
            // FIXME: this could be smarter. But perhaps it's just fine.
            let size = if buflen <= 1024 { 1024 } else if buflen <= 8192 { 8192 } else { 65536 };
            self.rd.reserve(size);

            // Read data into the buffer if it's available.
            let n = try_ready!(self.socket.read_buf(&mut self.rd));

            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }

    fn read_line(&mut self) -> Result<Async<Option<NntpInput>>, io::Error> {
        // resume where we left off.
        let bufpos = self.rd_pos;
        let buflen = self.rd.len();
        let nl_pos = {
            let buf = &self.rd[..];
            match memchr(b'\n', &buf[bufpos..buflen]) {
                Some(z) => bufpos + z,
                None => {
                    self.rd_pos = buflen;
                    return Ok(Async::NotReady);
                },
            }
        };
        let buf = self.rd.split_to(nl_pos + 1);
        self.rd_pos = 0;
        Ok(Async::Ready(Some(NntpInput::Line(buf))))
    }

    fn read_block(&mut self, do_scan: bool) -> Result<Async<Option<NntpInput>>, io::Error> {

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
                            }
                        }
                    },
                    State::Cr1Seen => {
                        self.rd_state = match buf[bufpos] {
                            b'\n' => {
                                // have a full line. scan it.
                                if do_scan {
                                    self.arttype_scanner.scan_line(&buf[self.rd_line_start..bufpos+1]);
                                    self.rd_line_start = bufpos+1;
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
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Overflow"));
            }
            return Ok(Async::Ready(Some(NntpInput::Block(buf))));
        }

        // continue
        Ok(Async::NotReady)
    }

    // read_article is a small wrapper around read_block that returns
    // an Article struct with the BytesMut and some article metadata.
    fn read_article(&mut self) -> Result<Async<Option<NntpInput>>, io::Error> {
        match self.read_block(true) {
            Ok(Async::Ready(Some(NntpInput::Block(buf)))) => {
                let article = Article{
                    msgid:      self.control.get_msgid(),
                    len:        buf.len(),
                    data:       buf,
                    arttype:    self.arttype_scanner.art_type(),
                    lines:      self.arttype_scanner.lines(),
                };
                self.arttype_scanner.reset();
                Ok(Async::Ready(Some(NntpInput::Article(article))))
            },
            buf => buf,
        }
    }

	fn nntp_start_send(&mut self, item: Bytes) -> StartSend<Bytes, io::Error> {

        // If we're still sending out the previous item...
        if !self.wr.is_empty() {

            // flush it ...
            self.nntp_sink_poll_complete()?;

            // not done yet, reject this item.
            if !self.wr.is_empty() {
                return Ok(AsyncSink::NotReady(item));
            }
        } else {
            let _ = self.socket.set_nodelay(false);
        }

        self.wr = item;

        Ok(AsyncSink::Ready)
    }

	fn nntp_sink_poll_complete(&mut self) -> Poll<(), io::Error> {
        trace!("flushing buffer");

        while !self.wr.is_empty() {
            trace!("writing; remaining={}", self.wr.len());

            let n = try_ready!(self.socket.poll_write(&self.wr));

            if n == 0 {
                let _ = self.socket.set_nodelay(true);
                return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to
                                          write buffer to socket").into());
            }

            let _ = self.wr.split_to(n);
        }

        // Try flushing the underlying IO
        try_ready!(self.socket.poll_flush());

        // Flushed, and done, so immediately send packet(s).
        // XXX FIXME for Linux use TCP_CORK
        let _ = self.socket.set_nodelay(true);

        trace!("buffer flushed");
        Ok(Async::Ready(()))
	}

	fn nntp_sink_close(&mut self) -> Poll<(), io::Error> {
        try_ready!(self.nntp_sink_poll_complete());
        self.socket.shutdown(Shutdown::Write)?;
        Ok(Async::Ready(()))
	}
}

impl NntpCodecControl {

    pub fn new() -> NntpCodecControl {
        NntpCodecControl {
            rd_mode:    Arc::new(AtomicUsize::new(CodecMode::Connect as usize)),
            error:      Arc::new(Mutex::new(None)),
            msgid:      Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_mode(&self, mode: CodecMode) {
        self.rd_mode.store(mode as usize, Ordering::SeqCst);
    }

    pub fn get_mode(&self) -> CodecMode {
        CodecMode::from(self.rd_mode.load(Ordering::SeqCst))
    }

    pub fn set_msgid(&self, msgid: &str) {
        *self.msgid.lock() = Some(msgid.to_string())
    }

    pub fn get_msgid(&self) -> String {
        self.msgid.lock().take().unwrap_or("".to_string())
    }

    pub fn quit(&self) {
        self.rd_mode.store(CodecMode::Quit as usize, Ordering::SeqCst);
    }

    pub fn write_error(&self, e: io::Error) {
        *self.error.lock() = Some(e);
        self.rd_mode.store(CodecMode::WriteError as usize, Ordering::SeqCst);
    }
}

/// This is the reading part, we return a stream of NntpInputs. Those can be:
///
/// - NntpInput::Connect:               returned once at the start
/// - NntpInput::Eof:                   end-of-file seen.
/// - NntpInput::WriteError(io::Error): the output writing routines reported an error.
/// - NntpInput::Line(BytesMut):        single command line
/// - NntpInput::Block(BytesMut):       multiline block
///
impl Stream for NntpCodec {
    type Item = NntpInput;
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {

        let rd_mode = self.control.get_mode();
        match rd_mode {
            CodecMode::Connect => {
                self.control.set_mode(CodecMode::ReadLine);
                return Ok(Async::Ready(Some(NntpInput::Connect)));
            },
            CodecMode::WriteError => {
                let e = match self.control.error.lock().take() {
                    Some(e) => e,
                    None => {
                        error!("nntp_codec::poll: CodecMode::WriteError but no error available");
                        io::Error::new(io::ErrorKind::Other, "write error")
                    },
                };
                self.control.quit();
                return Ok(Async::Ready(Some(NntpInput::WriteError(e))));
            },
            CodecMode::Quit => return Ok(Async::Ready(None)),
            _ => {},
        }

        // read as much data as we can.
        let sock_closed = self.fill_read_buf()?.is_ready();

        // other side closed.
        if sock_closed {
            if self.rd_state != State::Eof {
                // we were still processing data .. this was unexpected!
                if self.rd.len() > 0 {
                    self.rd_state = State::Eof;
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "UnexpectedEof"));
                }

                // return an end-of-file indication once, the next poll will
                // return end-of-stream.
                self.rd_state = State::Eof;
                return Ok(Async::Ready(Some(NntpInput::Eof)));
            }
            // end stream.
            return Ok(Async::Ready(None));
        }

        // Then process the data.
        match rd_mode {
            CodecMode::ReadLine => self.read_line(),
            CodecMode::ReadBlock => self.read_block(false),
            CodecMode::ReadArticle => self.read_article(),
            _ => unreachable!(),
        }
    }
}

/// The Sink is what writes the buffered data to the socket. We handle
/// a "Bytes" struct as one item. We do not buffer (yet), only one
/// item can be in-flight at a time.
impl Sink for NntpCodec {
    type SinkItem = Bytes;
    type SinkError = io::Error;

	fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.nntp_sink_poll_complete()
    }

	fn start_send(&mut self, item: Bytes) -> StartSend<Bytes, io::Error> {
        self.nntp_start_send(item)
    }

	fn close(&mut self) -> Poll<(), io::Error> {
        self.nntp_sink_close()
    }
}


