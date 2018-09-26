use std;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Future,future};
use time;

use commands::{Capb, Cmd, CmdParser};
use config::{self,Config};
use newsfeeds::{NewsFeeds,NewsPeer};
use nntp_codec::{CodecMode, NntpCodecControl};
use nntp_rs_history::HistStatus;
use nntp_rs_spool::ArtPart;
use server::Server;

enum NntpState {
    Cmd,
    Post,
    Ihave,
    TakeThis,
}

pub struct NntpSession {
    state:          NntpState,
    codec_control:  NntpCodecControl,
    parser:         CmdParser,
    server:         Server,
    remote:         SocketAddr,
    newsfeeds:      Arc<NewsFeeds>,
    config:         Arc<Config>,
    peer_idx:       usize,
    active:         bool,
}

type NntpError = io::Error;
type NntpFuture<T> = Box<Future<Item=T, Error=NntpError> + Send>;

impl NntpSession {
    pub fn new(peer: SocketAddr, control: NntpCodecControl, server: Server) -> NntpSession {
        let newsfeeds = config::get_newsfeeds();
        let config = config::get_config();
        NntpSession {
            state:          NntpState::Cmd,
            codec_control:  control,
            parser:         CmdParser::new(),
            server:         server,
            remote:         peer,
            newsfeeds:      newsfeeds,
            config:         config,
            peer_idx:       0,
            active:         false,
        }
    }

    fn thispeer(&self) -> &NewsPeer {
        &self.newsfeeds.peers[self.peer_idx]
    }

    /// Initial connect. Here we decide if we want to accept this
    /// connection, or refuse it.
    pub fn on_connect(&mut self, ) -> NntpFuture<Bytes> {
        let remote = self.remote.ip();
        let (idx, peer) = match self.newsfeeds.find_peer(&remote) {
            None => {
                self.codec_control.quit();
                info!("connrefused reason=unknownpeer addr={}", remote);
                let msg = format!("502 permission denied to {}\r\n", remote); 
                return Box::new(future::ok(Bytes::from(msg.as_bytes())))
            },
            Some(x) => x,
        };
        self.peer_idx = idx;

        let count = self.server.add_connection(&peer.label);
        self.active = true;
        if count > peer.maxconnect as usize {
            self.codec_control.quit();
            info!("connrefused reason=maxconnect peer={} conncount={} addr={}",
                  peer.label, count - 1 , remote);
            let msg = format!("502 too many connections from {} (max {})\r\n", peer.label, count - 1);
            return Box::new(future::ok(Bytes::from(msg.as_bytes())))
        }

        info!("connstart peer={} addr={} ", peer.label, remote);
        let code = if peer.readonly {
            201
        } else {
            self.parser.add_cap(Capb::Ihave);
            self.parser.add_cap(Capb::Streaming);
            200
        };
        if peer.headfeed {
            self.parser.add_cap(Capb::ModeHeadfeed);
        }
        let msg = format!("{} {} hello {}\r\n", code, self.config.server.hostname, peer.label);
        Box::new(future::ok(Bytes::from(msg.as_bytes())))
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
            Cmd::Article | Cmd::Body | Cmd::Head | Cmd::Stat => {
                if args.len() == 0 {
                    return Box::new(future::ok(Bytes::from(&b"412 Not in a newsgroup\r\n"[..])));
                }
                let (code, part) = match cmd {
                    Cmd::Article => (220, ArtPart::Article),
                    Cmd::Head => (221, ArtPart::Head),
                    Cmd::Body => (222, ArtPart::Body),
                    Cmd::Stat => (223, ArtPart::Stat),
                    _ => unreachable!(),
                };
                let mut buf = BytesMut::from(format!("{} 0 {}\r\n", code, args[0]));
                return self.read_article(part, args[0], buf);
            },
            Cmd::Capabilities => {
                if args.len() > 0 && !self.parser.is_keyword(args[0]) {
                    return Box::new(future::ok(Bytes::from(&b"501 invalid keyword\r\n"[..])));
                }
                return Box::new(future::ok(self.parser.capabilities()));
            },
            Cmd::Date => {
                let tm = time::now_utc();
                let fmt = tm.strftime("%Y%m%d%H%M%S\r\n").unwrap();
                return Box::new(future::ok(Bytes::from(format!("111 {}", fmt))));
            },
            Cmd::Group => {
                return Box::new(future::ok(Bytes::from(&b"503 Not implemented\r\n"[..])));
            },
            Cmd::Help => {
                return Box::new(future::ok(self.parser.help()));
            },
            Cmd::Ihave => {
                // XXX Testing reading blocks
                self.state = NntpState::Ihave;
                self.codec_control.set_rd_mode(CodecMode::ReadBlock);
                return Box::new(future::ok(Bytes::from(&b"335 Send article; end with CRLF DOT CRLF\r\n"[..])));
            },
            Cmd::Last => {
                return Box::new(future::ok(Bytes::from(&b"412 Not in a newsgroup\r\n"[..])));
            },
            Cmd::List_Newsgroups => {
                return Box::new(future::ok(Bytes::from(&b"503 Not maintaining a newsgroups file\r\n"[..])));
            },
            Cmd::ListGroup => {
                if args.len() == 0 {
                    return Box::new(future::ok(Bytes::from(&b"412 Not in a newsgroup\r\n"[..])));
                } else {
                    return Box::new(future::ok(Bytes::from(&b"503 Not implemented\r\n"[..])));
                }
            },
            Cmd::NewGroups => {
                return Box::new(future::ok(Bytes::from(&b"503 Not maintaining an active file\r\n"[..])));
            },
            Cmd::Next => {
                return Box::new(future::ok(Bytes::from(&b"412 Not in a newsgroup\r\n"[..])));
            },
            Cmd::Post => {
                self.state = NntpState::Post;
                self.codec_control.set_rd_mode(CodecMode::ReadBlock);
                return Box::new(future::ok(Bytes::from(&b"340 Submit article; end with CRLF DOT CRLF\r\n"[..])));
            },
            Cmd::Quit => {
                self.codec_control.quit();
                return Box::new(future::ok(Bytes::from(&b"205 Bye\r\n"[..])));
            },
            Cmd::Takethis => {
                // XXX Testing reading blocks
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

    fn read_article<'a>(&self, part: ArtPart, msgid: &'a str, buf: BytesMut) -> NntpFuture<Bytes> {

        let spool = self.server.spool.clone();
        let f = self.server.history.lookup(msgid)
            .map_err(|_e| {
                "430 Not found\r\n".to_string()
            })
            .and_then(move |result| {
                match result {
                    None => future::err("430 Not found\r\n".to_string()),
                    Some(he) => {
                        match (he.status, he.location) {
                            (HistStatus::Present, Some(loc)) => {
                                future::ok(loc)
                            },
                            _ => future::err(format!("430 {:?}\r\n", he.status))
                        }
                    },
                }
            })
            .and_then(move |loc| {
                spool.read(loc, part, buf)
                    .map(move |mut buf| {
                        if part == ArtPart::Head {
                            buf.extend_from_slice(b".\r\n");
                        }
                        buf.freeze()
                    }).map_err(|_e| {
                        "430 Not found\r\n".to_string()
                    })
            })
            .or_else(|e| future::ok(Bytes::from(e)));

       Box::new(f)
    }
}

impl Drop for NntpSession {
    fn drop(&mut self) {
        if self.active {
            let name = &self.thispeer().label;
            self.server.remove_connection(name);
        }
    }
}

