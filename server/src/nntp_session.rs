use std;
use std::io;
use std::mem;

use bytes::{BufMut, Bytes, BytesMut};
use futures_cpupool::CpuPool;
use futures::{Future,future};

use commands::{Cmd, CmdParser};
use nntp_codec::{CodecMode, NntpCodecControl};

enum NntpState {
    Cmd,
    Post,
    Ihave,
    TakeThis,
}

pub struct NntpSession {
    state:          NntpState,
    cpu_pool:       CpuPool,
    codec_control:  NntpCodecControl,
    parser:         CmdParser,
}

type NntpError = io::Error;
type NntpFuture<T> = Box<Future<Item=T, Error=NntpError> + Send>;

impl NntpSession {
    pub fn new(pool: CpuPool, control: NntpCodecControl) -> NntpSession {
        NntpSession {
            state:          NntpState::Cmd,
            cpu_pool:       pool,
            codec_control:  control,
            parser:         CmdParser::new(),
        }
    }

    /// Initial connect. Here we decide if we want to accept this
    /// connection, or refuse it.
    pub fn on_connect(&self, ) -> NntpFuture<Bytes> {
        Box::new(future::ok(Bytes::from(&b"200 Welcome\r\n"[..])))
    }

    /// Called when we got an error writing to the socket.
    /// Log an error, clean up, and exit.
    pub fn on_output_error(&self, _err: io::Error) {
    }

    /// Called when we got an error reading from the socket.
    /// Log an error, clean up, and exit.
    pub fn on_input_error(&self, _err: io::Error) {
    }

    /// Called when a line or block has been received.
    pub fn on_input(&mut self, input: BytesMut) -> NntpFuture<Bytes> {
        let state = mem::replace(&mut self.state, NntpState::Cmd);
        self.codec_control.set_rd_mode(CodecMode::ReadLine);
        match state {
            NntpState::Cmd => self.cmd(input),
            NntpState::Post => self.post_body(input),
            NntpState::Ihave => self.ihave_body(input),
            NntpState::TakeThis => self.takethis_body(input),
        }
    }

    /// Process NNTP command
    fn cmd(&mut self, input: BytesMut) -> NntpFuture<Bytes> {

        let line = match std::str::from_utf8(&input[..]) {
            Ok(l) => {
                if !l.ends_with("\r\n") {
                    // special case: initial connect.
                    if l == "CONNECT" {
                        return self.on_connect();
                    }
                    // another special case: end-of-file.
                    if l == "" {
                        self.codec_control.quit();
                        return Box::new(future::ok(Bytes::from(&b""[..])));
                    }
                    // allow QUIT even with improper line-ending.
                    let l = l.trim_right();
                    if !l.eq_ignore_ascii_case("quit") && l != "" {
                        // otherwise complain
                        return Box::new(future::ok(Bytes::from(&b"500 Lines must end with CRLF\r\n"[..])));
                    }
                    l
                } else {
                    &l[0..l.len()-2]
                }
            },
            Err(_) => return Box::new(future::ok(Bytes::from(&b"500 Invalid UTF-8\r\n"[..]))),
        };

        // ignore empty lines. most servers behave like this.
        if line.len() == 0 {
            return Box::new(future::ok(Bytes::new()));
        }

        let (cmd, args) = match self.parser.parse(line) {
            Err(e) => {
                let mut b = BytesMut::with_capacity(e.len() + 2);
                b.put(e);
                b.put("\r\n");
                return Box::new(future::ok(b.freeze()));
            },
            Ok(v) => v,
        };

        match cmd {
            Cmd::Help => {
                return Box::new(future::ok(self.parser.help()));
            },
            Cmd::Capabilities => {
                return Box::new(future::ok(self.parser.capabilities()));
            },
            Cmd::Quit => {
                self.codec_control.quit();
                return Box::new(future::ok(Bytes::from(&b"205 Bye\r\n"[..])));
            },
            Cmd::Post => {
                self.state = NntpState::Post;
                self.codec_control.set_rd_mode(CodecMode::ReadBlock);
                return Box::new(future::ok(Bytes::from(&b"340 Submit article; end with CRLF DOT CRLF\r\n"[..])));
            },
            Cmd::Ihave => {
                self.state = NntpState::Ihave;
                self.codec_control.set_rd_mode(CodecMode::ReadBlock);
                return Box::new(future::ok(Bytes::from(&b"335 Send article; end with CRLF DOT CRLF\r\n"[..])));
            },
            Cmd::Takethis => {
                self.state = NntpState::TakeThis;
                self.codec_control.set_rd_mode(CodecMode::ReadBlock);
                return Box::new(future::ok(Bytes::new()));
            },
            _ => {},
        }

        Box::new(future::ok(Bytes::from(&b"500 What?\r\n"[..])))
    }

    /// POST body has been received.
    fn post_body(&self, _input: BytesMut) -> NntpFuture<Bytes> {
         Box::new(future::ok(Bytes::from(&b"441 Posting failed\r\n"[..])))
    }

    /// IHAVE body has been received.
    fn ihave_body(&self, _input: BytesMut) -> NntpFuture<Bytes> {
         Box::new(future::ok(Bytes::from(&b"435 Transfer rejected\r\n"[..])))
    }

    /// TAKETHIS body has been received.
    fn takethis_body(&self, _input: BytesMut) -> NntpFuture<Bytes> {
         Box::new(future::ok(Bytes::from(&b"600 Takethis rejected\r\n"[..])))
    }
}

