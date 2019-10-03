use std;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use time;

use crate::article::{Article,Headers,HeaderName,HeadersParser};
use crate::commands::{Capb, Cmd, CmdParser};
use crate::config::{self,Config};
use crate::errors::*;
use crate::logger;
use crate::newsfeeds::{NewsFeeds,NewsPeer};
use crate::nntp_codec::{CodecMode, NntpCodecControl,NntpInput};
use crate::history::{HistEnt,HistError,HistStatus};
use crate::spool::{SPOOL_DONTSTORE,SPOOL_REJECTARTS,ArtPart};
use crate::util::{self,MatchList,MatchResult};
use crate::server::Server;

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

    fn bytes(b: Bytes) -> NntpResult {
        NntpResult{
            data:       b,
        }
    }

    fn empty() -> NntpResult {
        NntpResult::bytes(Bytes::new())
    }
}

// status for TAKETHIS/IHAVE/POST
enum ArtAccept {
    // accept and store.
    Accept,
    // reject and store rejection in history file.
    Reject,
    // try again later
    Defer,
}

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

    pub fn set_state(&mut self, state: NntpState) {
        self.state = state;
    }

    fn thispeer(&self) -> &NewsPeer {
        &self.newsfeeds.peers[self.peer_idx]
    }

    /// Initial connect. Here we decide if we want to accept this
    /// connection, or refuse it.
    pub async fn on_connect(&mut self, ) -> io::Result<NntpResult> {
        let remote = self.remote.ip();
        let (idx, peer) = match self.newsfeeds.find_peer(&remote) {
            None => {
                self.codec_control.quit();
                info!("connrefused reason=unknownpeer addr={}", remote);
                let msg = format!("502 permission denied to {}", remote); 
                return Ok(NntpResult::text(&msg));
            },
            Some(x) => x,
        };
        self.peer_idx = idx;

        let count = self.server.add_connection(&peer.label);
        self.active = true;
        if count > peer.maxconnect as usize && peer.maxconnect > 0 {
            self.codec_control.quit();
            info!("connrefused reason=maxconnect peer={} conncount={} addr={}",
                  peer.label, count - 1 , remote);
            let msg = format!("502 too many connections from {} (max {})", peer.label, count - 1);
            return Ok(NntpResult::text(&msg))
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
        Ok(NntpResult::text(&msg))
    }

    /// Called when we got an error writing to the socket.
    /// Log an error, clean up, and exit.
    pub async fn on_write_error(&self, _err: io::Error) -> io::Result<NntpResult> {
        println!("nntp_session.on_write_error");
        Ok(NntpResult::empty())
    }

    /// Called when we got an error reading from the socket.
    /// Log an error, clean up, and exit.
    pub async fn on_read_error(&self, _err: io::Error) -> io::Result<NntpResult> {
        println!("nntp_session.on_read_error");
        Ok(NntpResult::empty())
    }

    /// Call on end-of-file.
    pub async fn on_eof(&self) -> io::Result<NntpResult> {
        println!("nntp_session.on_eof");
        Ok(NntpResult::empty())
    }

    /// Called when a line or block has been received.
    pub async fn on_input(&mut self, input: NntpInput) -> io::Result<NntpResult> {
        match input {
            NntpInput::Line(line) => {

                // Bit of a hack, but doing this correctly is too much
                // trouble for now.
                if self.state == NntpState::Ihave ||
                    self.state == NntpState::Post {
                    self.state = NntpState::Cmd;
                }

                let state = mem::replace(&mut self.state, NntpState::Cmd);
                match state {
                    NntpState::Cmd => self.cmd(line).await,
                    _ => {
                        error!("got NntpInput::Line while in state {:?}", state);
                        self.codec_control.set_mode(CodecMode::Quit);
                        return Ok(NntpResult::text("400 internal state out of sync"));
                    }
                }
            },
            NntpInput::Article(art) => {
                self.codec_control.set_mode(CodecMode::ReadLine);
                let state = mem::replace(&mut self.state, NntpState::Cmd);
                match state {
                    NntpState::Post => self.post_body(art).await,
                    NntpState::Ihave => self.ihave_body(art).await,
                    NntpState::TakeThis => self.takethis_body(art).await,
                    NntpState::Cmd => {
                        error!("got NntpInput::Article while in state {:?}", self.state);
                        self.codec_control.set_mode(CodecMode::Quit);
                        return Ok(NntpResult::text("400 internal state out of sync"));
                    },
                }
            },
            NntpInput::Block(_buf) => {
                error!("got NntpInput::Block while in state {:?}", self.state);
                self.codec_control.set_mode(CodecMode::Quit);
                return Ok(NntpResult::text("400 internal state out of sync"))
            },
            _ => unreachable!(),
        }
    }

    /// Process NNTP command
    async fn cmd(&mut self, input: BytesMut) -> io::Result<NntpResult> {

        let line = match std::str::from_utf8(&input[..]) {
            Ok(l) => {
                if !l.ends_with("\r\n") {
                    // allow QUIT even with improper line-ending.
                    let l = l.trim_end();
                    if !l.eq_ignore_ascii_case("quit") && l != "" {
                        // otherwise complain
                        return Ok(NntpResult::text("500 Lines must end with CRLF"));
                    }
                    l
                } else {
                    &l[0..l.len()-2]
                }
            },
            Err(_) => return Ok(NntpResult::text("500 Invalid UTF-8")),
        };

        // ignore empty lines. most servers behave like this.
        if line.len() == 0 {
            return Ok(NntpResult::empty());
        }

        let (cmd, args) = match self.parser.parse(line) {
            Err(e) => {
                let mut b = BytesMut::with_capacity(e.len() + 2);
                b.put(e);
                b.put("\r\n");
                return Ok(NntpResult::bytes(b.freeze()));
            },
            Ok(v) => v,
        };

        match cmd {
            Cmd::Article | Cmd::Body | Cmd::Head | Cmd::Stat => {
                if args.len() == 0 {
                    return Ok(NntpResult::text("412 Not in a newsgroup"));
                }
                let (code, part) = match cmd {
                    Cmd::Article => (220u32, ArtPart::Article),
                    Cmd::Head => (221u32, ArtPart::Head),
                    Cmd::Body => (222u32, ArtPart::Body),
                    Cmd::Stat => (223u32, ArtPart::Stat),
                    _ => unreachable!(),
                };
                let buf = BytesMut::from(format!("{} 0 {}\r\n", code, args[0]));
                return self.read_article(part, args[0], buf).await;
            },
            Cmd::Capabilities => {
                if args.len() > 0 && !self.parser.is_keyword(args[0]) {
                    return Ok(NntpResult::text("501 invalid keyword"));
                }
                return Ok(NntpResult::bytes(self.parser.capabilities()));
            },
            Cmd::Check => {
                let msgid = args[0].to_string();
                let he = self.server.history.check(args[0]).await?;
                let s = match he {
                    None => format!("238 {}", msgid),
                    Some(status) => match status {
                        HistStatus::NotFound => format!("238 {}", msgid),
                        HistStatus::Tentative => format!("431 {}", msgid),
                        _ => format!("438 {}", msgid),
                    }
                };
                return Ok(NntpResult::text(&s));
            },
            Cmd::Date => {
                let tm = time::now_utc();
                let fmt = tm.strftime("%Y%m%d%H%M%S").unwrap();
                return Ok(NntpResult::text(&format!("111 {}", fmt)));
            },
            Cmd::Group => {
                return Ok(NntpResult::text("503 Not implemented"));
            },
            Cmd::Help => {
                return Ok(NntpResult::bytes(self.parser.help()));
            },
            Cmd::Ihave => {
                let ok = "335 Send article; end with CRLF DOT CRLF";
                let codec_control = self.codec_control.clone();
                let msgid = args[0].to_string();
                self.state = NntpState::Ihave;
                let he = self.server.history.check(args[0]).await?;
                let r = match he {
                    Some(HistStatus::NotFound)|None => ok,
                    Some(HistStatus::Tentative) => "436 Retry later",
                    _ => "435 Duplicate",
                };
                if r == ok {
                    codec_control.set_msgid(&msgid);
                    codec_control.set_mode(CodecMode::ReadArticle);
                }
                return Ok(NntpResult::text(r));
            },
            Cmd::Last => {
                return Ok(NntpResult::text("412 Not in a newsgroup"));
            },
            Cmd::List_Newsgroups => {
                return Ok(NntpResult::text("503 Not maintaining a newsgroups file"));
            },
            Cmd::ListGroup => {
                if args.len() == 0 {
                    return Ok(NntpResult::text("412 Not in a newsgroup"));
                } else {
                    return Ok(NntpResult::text("503 Not implemented"));
                }
            },
            Cmd::Mode_Stream => {
                return Ok(NntpResult::text("203 Streaming permitted"));
            }
            Cmd::NewGroups => {
                return Ok(NntpResult::text("503 Not maintaining an active file"));
            },
            Cmd::Next => {
                return Ok(NntpResult::text("412 Not in a newsgroup"));
            },
            Cmd::Post => {
                return Ok(NntpResult::text("500 Not implemented"));
            },
            Cmd::Quit => {
                self.codec_control.quit();
                return Ok(NntpResult::text("205 Bye"));
            },
            Cmd::Takethis => {
                self.codec_control.set_mode(CodecMode::ReadArticle);
                self.codec_control.set_msgid(args[0]);
                self.state = NntpState::TakeThis;
                return Ok(NntpResult::empty());
            },
            _ => {},
        }

        Ok(NntpResult::text("500 What?"))
    }

    /// POST body has been received.
    async fn post_body(&self, _input: Article) -> io::Result<NntpResult> {
         Ok(NntpResult::text("441 Posting failed"))
    }

    /// IHAVE body has been received.
    async fn ihave_body(&self, art: Article) -> io::Result<NntpResult> {
        let msgid = art.msgid.clone();
        let status = self.received_article(art, true).await?;
        let code = match status {
            ArtAccept::Accept => 235,
            ArtAccept::Defer => 436,
            ArtAccept::Reject => 437,
        };
        Ok(NntpResult::text(&format!("{} {}", code, msgid)))
    }

    /// TAKETHIS body has been received.
    async fn takethis_body(&self, art: Article) -> io::Result<NntpResult> {
        let msgid = art.msgid.clone();
        let status = self.received_article(art, false).await?;
        let code = match status {
            ArtAccept::Accept => 239,
            ArtAccept::Defer|
            ArtAccept::Reject => 439,
        };
        Ok(NntpResult::text(&format!("{} {}", code, msgid)))
    }

    async fn reject_art(&self, art: &Article, recv_time: u64, e: ArtError) -> io::Result<ArtAccept> {
        let he = HistEnt{
            status:     HistStatus::Rejected,
            time:       recv_time,
            head_only:  false,
            location:   None,
        };
        let label = &self.thispeer().label; //.clone();
        let res = self.server.history.store_begin(&art.msgid).await;
        match res {
            Ok(_) => {
                // succeeded adding reject entry.
                let _ = self.server.history.store_commit(&art.msgid, he).await;
                logger::incoming_reject(label, &art, e);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::Status(_)) => {
                // message-id seen before, log "duplicate"
                logger::incoming_reject(label, &art, ArtError::PostDuplicate);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::IoError(e)) => Err(e),
        }
    }

    // Received article: see if we want it, find out if it needs to be sent to other peers.
    async fn received_article(&self, art: Article, can_defer: bool) -> io::Result<ArtAccept> {
        let recv_time = util::unixtime();

        // parse article.
        let mut art = art;
        let (headers, body) = match self.process_headers(&mut art) {
            Err(e) => {
                return match e {
                    // article was mangled on the way. do not store the message-id
                    // in the history file, another peer may send a correct version.
                    ArtError::TooSmall|
                    ArtError::HdrOnlyNoBytes|
                    ArtError::NoHdrEnd|
                    ArtError::MsgIdMismatch|
                    ArtError::NoPath => {
                        if can_defer {
                            logger::incoming_defer(&self.thispeer().label, &art, e);
                            Ok(ArtAccept::Defer)
                        } else {
                            logger::incoming_reject(&self.thispeer().label, &art, e);
                            Ok(ArtAccept::Reject)
                        }
                    },

                    // all other errors. add a reject entry to the history file.
                    e => return self.reject_art(&art, recv_time, e).await,
                };
            },
            Ok(art) => art,
        };

        // See which spool we want the article to be stored in.
        let spool_no = {
            let newsgroups = headers.newsgroups().unwrap();
            match self.server.spool.get_spool(&art, &newsgroups) {
                None => return self.reject_art(&art, recv_time, ArtError::NoSpool).await,
                Some(SPOOL_DONTSTORE) => return self.reject_art(&art, recv_time, ArtError::DontStore).await,
                Some(SPOOL_REJECTARTS) => return self.reject_art(&art, recv_time, ArtError::RejSpool).await,
                Some(sp) => sp,
            }
        };

        // OK now we can go ahead and store the parsed article.
        let label = self.thispeer().label.clone();
        let history = self.server.history.clone();
        let spool = self.server.spool.clone();
        let msgid = art.msgid.clone();

        let res = self.server.history.store_begin(&art.msgid).await;
        match res {
            Err(HistError::Status(_)) => {
                // message-id seen before, log "duplicate"
                logger::incoming_reject(&label, &art, ArtError::PostDuplicate);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::IoError(e)) => {
                // I/O error, return error straight away.`
                Err(e)
            },
            Ok(_) => {
                // success, continue.
                let msgid2 = msgid.clone();
                let history2 = history.clone();

                // store the article.
                let mut buffer = BytesMut::new();
                headers.header_bytes(&mut buffer);
                let artloc = spool.write(spool_no, buffer, body).await?;
                let he = HistEnt{
                    status:     HistStatus::Present,
                    time:       recv_time,
                    head_only:  false,
                    location:   Some(artloc),
                };
                match history.store_commit(&msgid, he).await {
                    Err(e) => {
                        // XXX FIXME do not map I/O errors to reject; the
                        // caller of this future must do that. It should
                        // probably return a 400 error and close the connection.
                        error!("takethis {}: write: {}", &msgid2, e);
                        history2.store_rollback(&msgid2);
                        Ok(ArtAccept::Reject)
                    },
                    Ok(_) => Ok(ArtAccept::Accept),
                }
            },
        }
    }

    // parse the received article headers, then see if we want it.
    // Note: modifies/updates the Path: header.
    fn process_headers(&self, art: &mut Article) -> ArtResult<(Headers,BytesMut)> {

        let mut parser = HeadersParser::new();
        let buffer = mem::replace(&mut art.data, BytesMut::new());
        match parser.parse(&buffer, false, true) {
            None => {
                error!("failure parsing header, None returned");
                panic!("takethis_body: this code should never be reached")
            },
            Some(Err(e)) => return Err(e),
            Some(Ok(_)) => {},
        }
        let (mut headers, body) = parser.into_headers(buffer);

        // some more header checks.
        {
            let msgid_ok = match headers.message_id() {
                None => false,
                Some(msgid) => msgid == &art.msgid,
            };
            if !msgid_ok {
                return Err(ArtError::MsgIdMismatch);
            }

            // Servers like Diablo simply ignore the Date: header
            // if it cannot be parsed. So do the same.
            let date = headers.get_str(HeaderName::Date).unwrap();
            let dp = util::DateParser::new();
            if let Some(tm) = dp.parse(&date) {
                // FIXME make this configurable (and reasonable)
                // 315529200 is 1 Jan 1980, for now.
                if tm < 315529200 {
                    return Err(ArtError::TooOld);
                }
            }
        }

        let new_path;
        let mut mm : Option<String> = None;
        let thispeer = self.thispeer();

        {
            let newsgroups = headers.newsgroups().ok_or(ArtError::NoNewsgroups)?;
            let distribution = headers.distribution();

            // see if any of the groups in the NewsGroups: header
            // matched a "filter" statement in this peer's config.
            if thispeer.filter.matchlist(&newsgroups) == MatchResult::Match {
                return Err(ArtError::GroupFilter);
            }

            // see if the article matches the IFILTER label.
            let mut grouplist = MatchList::new(&newsgroups, &self.newsfeeds.groupdefs);
            if let Some(ref ifilter) = self.newsfeeds.infilter {
                if ifilter.wants(art, &[], &mut grouplist, distribution.as_ref()) {
                    return Err(ArtError::IncomingFilter);
                }
            }

            let mut pathelems = headers.path().ok_or(ArtError::NoPath)?; // .clone();

            // Now check which of our peers wants a copy.
            let peers = &self.newsfeeds.peers;
            let mut v = Vec::with_capacity(peers.len());
            for idx in 0 .. peers.len() {
                let peer = &peers[idx];
                if peer.wants(art, &pathelems, &mut grouplist, distribution.as_ref()) {
                    v.push(idx as u32);
                }
            }
            logger::incoming_accept(&self.thispeer().label, &art, peers, &v);

            // should match one of the pathaliases.
            if !thispeer.nomismatch {
                let is_match = thispeer.pathalias.iter().find(|s| s == &pathelems[0]).is_some();
                if !is_match {
                    info!("{} {} Path element fails to match aliases: {} in {}",
                        thispeer.label, self.remote.ip(), pathelems[0], art.msgid);
                    mm.get_or_insert(format!("{}.MISMATCH", self.remote.ip()));
                    pathelems.insert(0, mm.as_ref().unwrap());
                }
            }

            // insert our own name.
            pathelems.insert(0, &self.config.server.hostname);
            new_path = pathelems.join("!");
        }

        // update.
        headers.update(HeaderName::Path, new_path.as_bytes());


        Ok((headers, body))
    }

    async fn read_article(&self, part: ArtPart, msgid: &str, buf: BytesMut) -> io::Result<NntpResult> {

        let spool = self.server.spool.clone();
        let result = match self.server.history.lookup(msgid).await {
            Err(_) => return Ok(NntpResult::text("430 Not found")),
            Ok(None) => return Ok(NntpResult::text("430 Not found")),
            Ok(v) => v,
        };
        let loc = match result {
            None => return Ok(NntpResult::text("430 Not found")),
            Some(he) => {
                match (he.status, he.location) {
                    (HistStatus::Present, Some(loc)) => loc,
                    _ => return Ok(NntpResult::text(&format!("430 {:?}", he.status))),
                }
            },
        };
        match spool.read(loc, part, buf).await {
            Ok(mut buf) => {
                if part == ArtPart::Head {
                    buf.extend_from_slice(b".\r\n");
                }
                Ok(NntpResult::bytes(buf.freeze()))
            },
            Err(_) => Ok(NntpResult::text("430 Not found")),
        }
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

