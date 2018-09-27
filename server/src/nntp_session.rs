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
use headers::{HeaderName,HeadersParser};
use newsfeeds::{NewsFeeds,NewsPeer};
use nntp_codec::{CodecMode, NntpCodecControl,NntpInput};
use nntp_rs_history::{HistEnt,HistStatus};
use nntp_rs_spool::ArtPart;
use server::Server;
use nntp_rs_util as util;

#[derive(Debug,PartialEq,Eq)]
pub enum NntpState {
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
    msgid:          String,
    peer_idx:       usize,
    active:         bool,
}

pub struct NntpResult {
    pub data:       Bytes,
}

impl NntpResult {
    fn text(s: &str) -> NntpResult {
        let mut b = Bytes::with_capacity(s.len() + 2);
        b.extend_from_slice(s.as_bytes());
        b.extend_from_slice(&b"\r\n"[..]);
        NntpResult{
            data:       b,
        }
    }

    fn text_fut(s: &str) -> NntpFuture {
        Box::new(future::ok(NntpResult::text(s)))
    }

    fn bytes(b: Bytes) -> NntpResult {
        NntpResult{
            data:       b,
        }
    }

    fn empty() -> NntpResult {
        NntpResult::bytes(Bytes::new())
    }
}

type NntpError = io::Error;
type NntpFuture = Box<Future<Item=NntpResult, Error=NntpError> + Send>;

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
            msgid:          String::new(),
            peer_idx:       0,
            active:         false,
        }
    }

    pub fn set_state(&mut self, state: NntpState) {
        self.state = state;
    }

    fn thispeer(&self) -> &NewsPeer {
        &self.newsfeeds.peers[self.peer_idx]
    }

    /// Initial connect. Here we decide if we want to accept this
    /// connection, or refuse it.
    pub fn on_connect(&mut self, ) -> NntpFuture {
        let remote = self.remote.ip();
        let (idx, peer) = match self.newsfeeds.find_peer(&remote) {
            None => {
                self.codec_control.quit();
                info!("connrefused reason=unknownpeer addr={}", remote);
                let msg = format!("502 permission denied to {}", remote); 
                return NntpResult::text_fut(&msg);
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
            let msg = format!("502 too many connections from {} (max {})", peer.label, count - 1);
            return NntpResult::text_fut(&msg)
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
        let msg = format!("{} {} hello {}", code, self.config.server.hostname, peer.label);
        Box::new(future::ok(NntpResult::text(&msg)))
    }

    /// Called when we got an error writing to the socket.
    /// Log an error, clean up, and exit.
    pub fn on_write_error(&self, _err: io::Error) -> NntpFuture {
        println!("nntp_session.on_write_error");
        Box::new(future::ok(NntpResult::empty()))
    }

    /// Called when we got an error reading from the socket.
    /// Log an error, clean up, and exit.
    pub fn on_read_error(&self, _err: io::Error) -> NntpFuture {
        println!("nntp_session.on_read_error");
        Box::new(future::ok(NntpResult::empty()))
    }

    /// Call on end-of-file.
    pub fn on_eof(&self) -> NntpFuture {
        println!("nntp_session.on_eof");
        Box::new(future::ok(NntpResult::empty()))
    }

    /// Called when a line or block has been received.
    pub fn on_input(&mut self, input: NntpInput) -> NntpFuture {
        let input = match input {
            NntpInput::Line(d) => {
                // Bit of a hack, but doing this correctly is way too much
                // trouble to support IHAVE.
                if self.state == NntpState::Ihave {
                    self.state = NntpState::Cmd;
                }
                if self.state != NntpState::Cmd {
                    error!("got NntpInput::Line while in state {:?}", self.state);
                    self.codec_control.set_mode(CodecMode::Quit);
                    return NntpResult::text_fut("400 internal state out of sync");
                }
                d
            },
            NntpInput::Block(d) => {
                if self.state == NntpState::Cmd {
                    error!("got NntpInput::Block while in state {:?}", self.state);
                    self.codec_control.set_mode(CodecMode::Quit);
                    return NntpResult::text_fut("400 internal state out of sync");
                }
                d
            },
            _ => unreachable!(),
        };
        let state = mem::replace(&mut self.state, NntpState::Cmd);
        self.codec_control.set_mode(CodecMode::ReadLine);
        match state {
            NntpState::Cmd => self.cmd(input),
            NntpState::Post => self.post_body(input),
            NntpState::Ihave => self.ihave_body(input),
            NntpState::TakeThis => self.takethis_body(input),
        }
    }

    /// Process NNTP command
    fn cmd(&mut self, input: BytesMut) -> NntpFuture {

        let line = match std::str::from_utf8(&input[..]) {
            Ok(l) => {
                if !l.ends_with("\r\n") {
                    // allow QUIT even with improper line-ending.
                    let l = l.trim_right();
                    if !l.eq_ignore_ascii_case("quit") && l != "" {
                        // otherwise complain
                        return Box::new(future::ok(NntpResult::text("500 Lines must end with CRLF")));
                    }
                    l
                } else {
                    &l[0..l.len()-2]
                }
            },
            Err(_) => return Box::new(future::ok(NntpResult::text("500 Invalid UTF-8"))),
        };

        // ignore empty lines. most servers behave like this.
        if line.len() == 0 {
            return Box::new(future::ok(NntpResult::empty()));
        }

        let (cmd, args) = match self.parser.parse(line) {
            Err(e) => {
                let mut b = BytesMut::with_capacity(e.len() + 2);
                b.put(e);
                b.put("\r\n");
                return Box::new(future::ok(NntpResult::bytes(b.freeze())));
            },
            Ok(v) => v,
        };

        match cmd {
            Cmd::Article | Cmd::Body | Cmd::Head | Cmd::Stat => {
                if args.len() == 0 {
                    return Box::new(future::ok(NntpResult::text("412 Not in a newsgroup")));
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
                    return Box::new(future::ok(NntpResult::text("501 invalid keyword")));
                }
                return Box::new(future::ok(NntpResult::bytes(self.parser.capabilities())));
            },
            Cmd::Check => {
                let msgid = args[0].to_string();
                let fut = self.server.history.check(args[0])
                    .map(move |he| {
                        match he {
                            None => format!("238 {}", msgid),
                            Some(he) => match he.status {
                                HistStatus::Present|
                                HistStatus::Expired|
                                HistStatus::Rejected => format!("438 {}", msgid),
                                HistStatus::Tentative|
                                HistStatus::Writing => format!("431 {}", msgid),
                                HistStatus::NotFound => format!("238 {}", msgid),
                            }
                        }
                    })
                    .map(|s| NntpResult::text(&s));
                return Box::new(fut);
            },
            Cmd::Date => {
                let tm = time::now_utc();
                let fmt = tm.strftime("%Y%m%d%H%M%S").unwrap();
                return Box::new(future::ok(NntpResult::text(&format!("111 {}", fmt))));
            },
            Cmd::Group => {
                return Box::new(future::ok(NntpResult::text("503 Not implemented")));
            },
            Cmd::Help => {
                return Box::new(future::ok(NntpResult::bytes(self.parser.help())));
            },
            Cmd::Ihave => {
                let ok = "335 Send article; end with CRLF DOT CRLF";
                let codec_control = self.codec_control.clone();
                self.state = NntpState::Ihave;
                self.msgid = args[0].to_string();
                let fut = self.server.history.check(args[0])
                    .map(move |he| {
                        let r = match he {
                            None => ok,
                            Some(he) => match he.status {
                                HistStatus::Present|
                                HistStatus::Expired|
                                HistStatus::Rejected => "435 Duplicate",
                                HistStatus::Tentative|
                                HistStatus::Writing => "436 Retry later",
                                HistStatus::NotFound => ok,
                            }
                        };
                        if r == ok {
                            codec_control.set_mode(CodecMode::ReadBlock);
                        }
                        NntpResult::text(r)
                    });
                return Box::new(fut);
            },
            Cmd::Last => {
                return Box::new(future::ok(NntpResult::text("412 Not in a newsgroup")));
            },
            Cmd::List_Newsgroups => {
                return Box::new(future::ok(NntpResult::text("503 Not maintaining a newsgroups file")));
            },
            Cmd::ListGroup => {
                if args.len() == 0 {
                    return Box::new(future::ok(NntpResult::text("412 Not in a newsgroup")));
                } else {
                    return Box::new(future::ok(NntpResult::text("503 Not implemented")));
                }
            },
            Cmd::NewGroups => {
                return Box::new(future::ok(NntpResult::text("503 Not maintaining an active file")));
            },
            Cmd::Next => {
                return Box::new(future::ok(NntpResult::text("412 Not in a newsgroup")));
            },
            Cmd::Post => {
                self.codec_control.set_mode(CodecMode::ReadBlock);
                self.state = NntpState::Post;
                let ok = "340 Submit article; end with CRLF DOT CRLF";
                self.msgid = args[0].to_string();
                return Box::new(future::ok(NntpResult::text(ok)));
            },
            Cmd::Quit => {
                self.codec_control.quit();
                return Box::new(future::ok(NntpResult::text("205 Bye")));
            },
            Cmd::Takethis => {
                self.codec_control.set_mode(CodecMode::ReadBlock);
                self.state = NntpState::TakeThis;
                self.msgid = args[0].to_string();
                return Box::new(future::ok(NntpResult::empty()));
            },
            _ => {},
        }

        Box::new(future::ok(NntpResult::text("500 What?")))
    }

    /// POST body has been received.
    fn post_body(&self, _input: BytesMut) -> NntpFuture {
         Box::new(future::ok(NntpResult::text("441 Posting failed")))
    }

    /// IHAVE body has been received.
    fn ihave_body(&self, _input: BytesMut) -> NntpFuture {
         Box::new(future::ok(NntpResult::text("435 Transfer rejected")))
    }

    /// TAKETHIS body has been received.
    fn takethis_body(&self, input: BytesMut) -> NntpFuture {
        let mut parser = HeadersParser::new();
        let len = match parser.parse(&input, false, true) {
            None => {
                error!("failure parsing header, None returned");
                0
            },
            Some(Ok(n)) => n,
            Some(Err(e)) => {
                error!("parsing headers: {}", e);
                0
            },
        };
        if len == 0 {
            return NntpResult::text_fut(&format!("439 {}", self.msgid));
        }

        let (headers, body) = parser.into_headers(input);
        println!("Newsgroups header: {:#?}", headers.newsgroups());

        let msgid = match headers.message_id() {
            None => {
                debug!("bad message-id header");
                return NntpResult::text_fut(&format!("439 {}", self.msgid));
            },
            Some(m) => m,
        };
        if msgid != &self.msgid {
            debug!("nntp message-id doesn't match article message-id");
            return NntpResult::text_fut(&format!("439 {}", self.msgid));
        }

        let date = headers.get_str(HeaderName::Date).unwrap();
        let dp = util::DateParser::new();
        if dp.parse(&date).is_none() {
            error!("could not parse date header {}", date);
            return NntpResult::text_fut(&format!("439 {}", self.msgid));
        }

        // start phase 1, unless another peer got in before us.
        if self.server.history.store_begin(&self.msgid) == false {
            return NntpResult::text_fut(&format!("439 {}", self.msgid));
        }

        let recv_time = util::unixtime();
        let msgid = Arc::new(self.msgid.clone());
        let msgid2 = msgid.clone();
        let msgid3 = msgid.clone();
        let history = self.server.history.clone();
        let history2 = self.server.history.clone();

        // store the article.
        let mut buffer = BytesMut::new();
        headers.header_bytes(&mut buffer);
        let fut = self.server.spool.write(buffer, body)
            .and_then(move |artloc| {
                let he = HistEnt{
                    status:     HistStatus::Present,
                    time:       recv_time,
                    head_only:  false,
                    location:   Some(artloc),
                };
                history.store_commit(&msgid, he)
            }).and_then(move |_| {
                NntpResult::text_fut(&format!("239 {}", &msgid2))
            }).then(move |result| {
                match result {
                    Err(e) => {
                        error!("takethis {}: write: {}", &msgid3, e);
                        history2.store_rollback(&msgid3);
                        NntpResult::text_fut(&format!("439 {}", &msgid3))
                    },
                    Ok(_) => NntpResult::text_fut(&format!("239 {}", &msgid3)),
                }
            });
        Box::new(fut)
    }

    fn read_article<'a>(&self, part: ArtPart, msgid: &'a str, buf: BytesMut) -> NntpFuture {

        let spool = self.server.spool.clone();
        let f = self.server.history.lookup(msgid)
            .map_err(|_e| {
                "430 Not found".to_string()
            })
            .and_then(move |result| {
                match result {
                    None => future::err("430 Not found".to_string()),
                    Some(he) => {
                        match (he.status, he.location) {
                            (HistStatus::Present, Some(loc)) => {
                                future::ok(loc)
                            },
                            _ => future::err(format!("430 {:?}", he.status))
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
                        NntpResult::bytes(buf.freeze())
                    }).map_err(|_e| {
                        "430 Not found".to_string()
                    })
            })
            .or_else(|e| future::ok(NntpResult::text(&e)));

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

