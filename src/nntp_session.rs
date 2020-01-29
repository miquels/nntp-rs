use std;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::{atomic::Ordering, Arc};

use bytes::BytesMut;

use crate::article::{Article, HeaderName, Headers, HeadersParser};
use crate::util::Buffer;
use crate::commands::{self, Capb, Cmd, CmdParser};
use crate::config::{self, Config};
use crate::diag::{SessionStats, Stats};
use crate::errors::*;
use crate::history::{HistEnt, HistError, HistStatus};
use crate::logger;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_codec::{NntpCodec, NntpLine};
use crate::server::{Notification, Server};
use crate::spool::{ArtPart, SPOOL_DONTSTORE, SPOOL_REJECTARTS};
use crate::util::{self, HashFeed, MatchList, MatchResult, UnixTime};

pub struct NntpSession {
    pub codec:       NntpCodec,
    parser:          CmdParser,
    server:          Server,
    remote:          SocketAddr,
    newsfeeds:       Arc<NewsFeeds>,
    config:          Arc<Config>,
    incoming_logger: logger::Incoming,
    peer_idx:        usize,
    active:          bool,
    stats:           SessionStats,
    quit:            bool,
}

pub struct NntpResult {
    pub data: Buffer,
}

impl NntpResult {
    pub fn text(s: impl AsRef<str>) -> NntpResult {
        let mut data = Buffer::from(s.as_ref());
        data.put_str("\r\n");
        NntpResult { data }
    }

    pub fn buffer(b: impl Into<Buffer>) -> NntpResult {
        NntpResult { data: b.into() }
    }

    pub fn empty() -> NntpResult {
        NntpResult { data: Buffer::new() }
    }
}

impl From<NntpResult> for Buffer {
    fn from(n: NntpResult) -> Buffer {
        n.data
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
    pub fn new(peer: SocketAddr, codec: NntpCodec, server: Server, stats: SessionStats) -> NntpSession {
        let newsfeeds = config::get_newsfeeds();
        let config = config::get_config();
        let incoming_logger = logger::get_incoming_logger();

        // decremented in Drop.
        server.tot_sessions.fetch_add(1, Ordering::SeqCst);
        server.thr_sessions.fetch_add(1, Ordering::SeqCst);

        NntpSession {
            codec:           codec,
            parser:          CmdParser::new(),
            server:          server,
            remote:          peer,
            newsfeeds:       newsfeeds,
            config:          config,
            incoming_logger: incoming_logger,
            peer_idx:        0,
            active:          false,
            stats:           stats,
            quit:            false,
        }
    }

    fn thispeer(&self) -> &NewsPeer {
        &self.newsfeeds.peers[self.peer_idx]
    }

    pub async fn run(mut self) {
        //
        // See if we want to accept this connection.
        //
        let msg = match self.on_connect().await {
            Ok(msg) => msg,
            Err(msg) => {
                let _ = self.codec.write_buf(msg.data).await;
                return;
            }
        };
        if let Err(e) = self.codec.write_buf(msg.data).await {
            self.on_write_error(e);
            return;
        }

        //
        // Command loop.
        //
        while !self.quit {
            let response = match self.codec.read_line().await {
                Ok(NntpLine::Eof) => break,
                Ok(NntpLine::Notification(Notification::ExitGraceful)) => {
                    self.quit = true;
                    NntpResult::text("400 Server shutting down")
                },
                Ok(NntpLine::Notification(_)) => continue,
                Ok(NntpLine::Line(buf)) => match self.cmd(buf).await {
                    Ok(res) => res,
                    Err(e) => {
                        let res = NntpResult::text(format!("400 {}", e));
                        self.on_generic_error(e);
                        self.quit = true;
                        res
                    }
                },
                Err(e) => {
                    self.quit = true;
                    match self.on_read_error(e) {
                        Some(msg) => msg,
                        None => break,
                    }
                },
            };
            if let Err(e) = self.codec.write_buf(response.data).await {
                self.on_write_error(e);
                break;
            }
        }

        // save final stats.
        self.stats.on_disconnect();
    }

    // Initial connect. Here we decide if we want to accept this
    // connection, or refuse it.
    async fn on_connect(&mut self) -> Result<NntpResult, NntpResult> {
        let remote = self.remote.ip();
        let (idx, peer) = match self.newsfeeds.find_peer(&remote) {
            None => {
                info!("Connection {} from {} (no permission)", self.stats.fdno, remote);
                let msg = format!("502 permission denied to {}", remote);
                return Err(NntpResult::text(msg));
            },
            Some(x) => x,
        };
        self.peer_idx = idx;

        let count = self.server.add_connection(&peer.label);
        self.active = true;
        if count > peer.maxconnect as usize && peer.maxconnect > 0 {
            info!(
                "Connect Limit exceeded (from dnewsfeeds) for {} ({}) ({} > {})",
                peer.label, remote, count, peer.maxconnect
            );
            let msg = format!("502 too many connections from {} (max {})", peer.label, count - 1);
            return Err(NntpResult::text(msg));
        }

        self.stats
            .on_connect(remote.to_string(), peer.label.clone())
            .await;

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
        Ok(NntpResult::text(msg))
    }

    /// Called when we got an error writing to the socket.
    fn on_write_error(&self, err: io::Error) {
        let stats = &self.stats;
        if err.kind() == io::ErrorKind::NotFound {
            info!(
                "Forcibly closed connection {} from {} {}",
                stats.fdno, stats.hostname, stats.ipaddr
            );
        } else {
            info!(
                "Write error on connection {} from {} {}: {}",
                stats.fdno, stats.hostname, stats.ipaddr, err
            );
        }
    }

    /// Called when we got an error reading from the socket.
    /// Log an error, and perhaps create a reply if appropriate.
    fn on_read_error(&mut self, err: io::Error) -> Option<NntpResult> {
        let stats = &self.stats;
        if err.kind() == io::ErrorKind::NotFound {
            info!(
                "Forcibly closed connection {} from {} {}",
                stats.fdno, stats.hostname, stats.ipaddr
            );
        } else {
            info!(
                "Read error on connection {} from {} {}: {}",
                stats.fdno, stats.hostname, stats.ipaddr, err
            );
        }
        match err.kind() {
            io::ErrorKind::TimedOut => Some(NntpResult::text("400 Timeout - closing connection")),
            io::ErrorKind::NotFound => Some(NntpResult::text("400 Server shutting down")),
            _ => None,
        }
    }

    /// Called when we got an error handling the command. Could be anything-
    /// failure writing an article, history db corrupt, etc.
    pub fn on_generic_error(&mut self, err: io::Error) {
        let stats = &self.stats;
        info!(
            "Error on connection {} from {} {}: {}",
            stats.fdno, stats.hostname, stats.ipaddr, err
        );
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
                    &l[0..l.len() - 2]
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
                let mut b = Buffer::from(e);
                b.put_str("\r\n");
                return Ok(NntpResult::buffer(b))
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
                let buf = Buffer::from(format!("{} 0 {}\r\n", code, args[0]));
                return self.read_article(part, args[0], buf).await;
            },
            Cmd::Capabilities => {
                if args.len() > 0 && !self.parser.is_keyword(args[0]) {
                    return Ok(NntpResult::text("501 invalid keyword"));
                }
                return Ok(NntpResult::buffer(self.parser.capabilities()));
            },
            Cmd::Check => {
                self.stats.inc(Stats::Check);
                self.stats.inc(Stats::Offered);
                if !commands::is_msgid(&args[0]) {
                    self.stats.inc(Stats::RefBadMsgId);
                    return Ok(NntpResult::text(&format!("438 {} Bad Message-ID", args[0])));
                }
                let msgid = args[0];
                let he = self.server.history.check(args[0]).await?;
                let s = match he {
                    None => format!("238 {}", msgid),
                    Some(status) => {
                        match status {
                            HistStatus::NotFound => format!("238 {}", msgid),
                            HistStatus::Tentative => {
                                self.stats.inc(Stats::RefPreCommit);
                                format!("431 {}", msgid)
                            },
                            _ => {
                                self.stats.inc(Stats::RefHistory);
                                format!("438 {}", msgid)
                            },
                        }
                    },
                };
                return Ok(NntpResult::text(&s));
            },
            Cmd::Date => {
                let dt = UnixTime::now().datetime_utc();
                let fmt = format!(
                    "{:04}{:02}{:02}{:02}{:02}{:02}",
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour(),
                    dt.minute(),
                    dt.second()
                );
                return Ok(NntpResult::text(&format!("111 {}", fmt)));
            },
            Cmd::Group => {
                return Ok(NntpResult::text("503 Not implemented"));
            },
            Cmd::Help => {
                return Ok(NntpResult::buffer(self.parser.help()));
            },
            Cmd::Ihave => {
                self.stats.inc(Stats::Ihave);
                self.stats.inc(Stats::Offered);
                if !commands::is_msgid(&args[0]) {
                    self.stats.inc(Stats::RefBadMsgId);
                    // Return a 435 instead of 501, so that the remote peer does not
                    // close the connection. We might be stricter with message-ids and
                    // we do not want the remote feed to hang on one message.
                    return Ok(NntpResult::text(&format!("435 {} Bad Message-ID", args[0])));
                }

                match self.server.history.check(args[0]).await? {
                    Some(HistStatus::NotFound) | None => {},
                    Some(HistStatus::Tentative) => {
                        self.stats.inc(Stats::RefPreCommit);
                        return Ok(NntpResult::text("436 Retry later"));
                    },
                    _ => {
                        self.stats.inc(Stats::RefPreCommit);
                        return Ok(NntpResult::text("435 Duplicate"));
                    },
                }

                self.codec.write_buf(Buffer::from("335 Send article; end with CRLF DOT CRLF")).await?;

                let mut art = self.codec.read_article(args[0]).await?;
                let status = self.received_article(&mut art, true).await?;
                let code = match status {
                    ArtAccept::Accept => 235,
                    ArtAccept::Defer => 436,
                    ArtAccept::Reject => 437,
                };
                return Ok(NntpResult::text(&format!("{} {}", code, art.msgid)));
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
            },
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
                self.quit = true;
                return Ok(NntpResult::text("205 Bye"));
            },
            Cmd::Takethis => {
                self.stats.inc(Stats::Takethis);
                self.stats.inc(Stats::Offered);
                let mut art = self.codec.read_article(args[0]).await?;
                let status = self.received_article(&mut art, false).await?;
                let code = match status {
                    ArtAccept::Accept => 239,
                    ArtAccept::Defer | ArtAccept::Reject => 439,
                };
                return Ok(NntpResult::text(&format!("{} {}", code, art.msgid)));
            },
            _ => {},
        }

        Ok(NntpResult::text("500 What?"))
    }

    async fn reject_art(&mut self, art: &Article, recv_time: UnixTime, e: ArtError) -> io::Result<ArtAccept> {
        let he = HistEnt {
            status:    HistStatus::Rejected,
            time:      recv_time,
            head_only: false,
            location:  None,
        };
        let res = self.server.history.store_begin(&art.msgid).await;
        match res {
            Ok(_) => {
                // succeeded adding reject entry.
                let _ = self.server.history.store_commit(&art.msgid, he).await;
                self.stats.art_error(&art, &e);
                let label = &self.thispeer().label;
                self.incoming_logger.reject(label, &art, e);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::Status(_)) => {
                // message-id seen before, log "duplicate"
                let e = ArtError::PostDuplicate;
                self.stats.art_error(&art, &e);
                let label = &self.thispeer().label;
                self.incoming_logger.reject(label, &art, e);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::IoError(e)) => {
                self.stats.art_error(&art, &ArtError::IOError);
                Err(e)
            },
        }
    }

    async fn dontstore_art(&mut self, art: &Article, recv_time: UnixTime) -> io::Result<ArtAccept> {
        let he = HistEnt {
            status:    HistStatus::Expired,
            time:      recv_time,
            head_only: false,
            location:  None,
        };
        let res = self.server.history.store_begin(&art.msgid).await;
        match res {
            Ok(_) => {
                // succeeded adding reject entry.
                let _ = self.server.history.store_commit(&art.msgid, he).await;
                self.stats.art_accepted(&art);
                let label = &self.thispeer().label;
                self.incoming_logger.accept(label, art, &[], &[]);
                Ok(ArtAccept::Accept)
            },
            Err(HistError::Status(_)) => {
                // message-id seen before, log "duplicate"
                let e = ArtError::PostDuplicate;
                self.stats.art_error(&art, &e);
                let label = &self.thispeer().label;
                self.incoming_logger.reject(label, &art, e);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::IoError(e)) => Err(e),
        }
    }

    // Received article: see if we want it, find out if it needs to be sent to other peers.
    async fn received_article(&mut self, mut art: &mut Article, can_defer: bool) -> io::Result<ArtAccept> {
        let recv_time = UnixTime::now();

        // parse article.
        let (headers, body, wantpeers) = match self.process_headers(&mut art) {
            Err(e) => {
                return match e {
                    // article was mangled on the way. do not store the message-id
                    // in the history file, another peer may send a correct version.
                    ArtError::TooSmall |
                    ArtError::HdrOnlyNoBytes |
                    ArtError::NoHdrEnd |
                    ArtError::MsgIdMismatch |
                    ArtError::NoPath => {
                        if can_defer {
                            self.incoming_logger.defer(&self.thispeer().label, art, e);
                            Ok(ArtAccept::Defer)
                        } else {
                            self.incoming_logger.reject(&self.thispeer().label, art, e);
                            Ok(ArtAccept::Reject)
                        }
                    },

                    // all other errors. add a reject entry to the history file.
                    e => return self.reject_art(art, recv_time, e).await,
                };
            },
            Ok(art) => art,
        };

        // See which spool we want the article to be stored in.
        let spool_no = {
            let newsgroups = headers.newsgroups().unwrap();
            match self.server.spool.get_spool(art, &newsgroups) {
                None => return self.reject_art(art, recv_time, ArtError::NoSpool).await,
                Some(SPOOL_DONTSTORE) => return self.dontstore_art(art, recv_time).await,
                Some(SPOOL_REJECTARTS) => return self.reject_art(art, recv_time, ArtError::RejSpool).await,
                Some(sp) => sp,
            }
        };

        // OK now we can go ahead and store the parsed article.
        let label = &self.thispeer().label;
        let history = &self.server.history;
        let spool = &self.server.spool;
        let msgid = &art.msgid;

        let res = history.store_begin(msgid).await;
        match res {
            Err(HistError::Status(_)) => {
                // message-id seen before, log "duplicate"
                self.incoming_logger.reject(label, art, ArtError::PostDuplicate);
                Ok(ArtAccept::Reject)
            },
            Err(HistError::IoError(e)) => {
                // I/O error, no incoming.log - we reply with status 400.
                error!("received_article {}: history lookup: {}", msgid, e);
                self.stats.art_error(&art, &ArtError::IOError);
                Err(e)
            },
            Ok(_) => {
                // store the article.
                let mut buffer = BytesMut::new();
                headers.header_bytes(&mut buffer);
                let artloc = match spool.write(spool_no, buffer, body).await {
                    Ok(loc) => loc,
                    Err(e) => {
                        error!("received_article {}: spool write: {}", msgid, e);
                        self.stats.art_error(&art, &ArtError::IOError);
                        history.store_rollback(msgid);
                        return Err(e);
                    },
                };
                let he = HistEnt {
                    status:    HistStatus::Present,
                    time:      recv_time,
                    head_only: false,
                    location:  Some(artloc),
                };
                match history.store_commit(msgid, he).await {
                    Err(e) => {
                        error!("received_article {}: history write: {}", msgid, e);
                        self.stats.art_error(&art, &ArtError::IOError);
                        history.store_rollback(msgid);
                        Err(e)
                    },
                    Ok(_) => {
                        let peers = &self.newsfeeds.peers;
                        self.incoming_logger.accept(label, art, peers, &wantpeers);
                        self.stats.art_accepted(&art);
                        Ok(ArtAccept::Accept)
                    },
                }
            },
        }
    }

    // parse the received article headers, then see if we want it.
    // Note: modifies/updates the Path: header.
    fn process_headers(&self, art: &mut Article) -> ArtResult<(Headers, BytesMut, Vec<u32>)> {
        // Check that the article has a minimum size. Sometimes a peer is offering
        // you empty articles because they no longer exist on their spool.
        if art.data.len() < 80 {
            return Err(ArtError::TooSmall);
        }

        // Make sure the message-id we got from TAKETHIS is valid.
        // We cannot check this in advance; TAKETHIS must read the whole article first.
        if !commands::is_msgid(&art.msgid) {
            return Err(ArtError::BadMsgId);
        }

        let mut parser = HeadersParser::new();
        let buffer = mem::replace(&mut art.data, BytesMut::new());
        match parser.parse(&buffer, false, true) {
            None => {
                error!("failure parsing header, None returned");
                panic!("process_headers: this code should never be reached")
            },
            Some(Err(e)) => return Err(e),
            Some(Ok(_)) => {},
        }
        let (mut headers, body) = parser.into_headers(buffer);

        let mut pathelems = headers.path().ok_or(ArtError::NoPath)?;
        art.pathhost = Some(pathelems[0].to_string());

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
            if let Some(tm) = util::parse_date(&date) {
                // FIXME make this configurable (and reasonable)
                // 315529200 is 1 Jan 1980, for now.
                if tm.as_secs() < 315529200 {
                    return Err(ArtError::TooOld);
                }
            }
        }

        let new_path;
        let mut mm: Option<String> = None;
        let thispeer = self.thispeer();

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
            if ifilter.wants(
                art,
                &HashFeed::default(),
                &[],
                &mut grouplist,
                distribution.as_ref(),
            ) {
                return Err(ArtError::IncomingFilter);
            }
        }

        // Now check which of our peers wants a copy.
        let peers = &self.newsfeeds.peers;
        let mut v = Vec::with_capacity(peers.len());
        for idx in 0..peers.len() {
            let peer = &peers[idx];
            if peer.wants(
                art,
                &peer.hashfeed,
                &pathelems,
                &mut grouplist,
                distribution.as_ref(),
            ) {
                v.push(idx as u32);
            }
        }

        // should match one of the pathaliases.
        if !thispeer.nomismatch {
            let is_match = thispeer.pathalias.iter().find(|s| s == &pathelems[0]).is_some();
            if !is_match {
                info!(
                    "{} {} Path element fails to match aliases: {} in {}",
                    thispeer.label,
                    self.remote.ip(),
                    pathelems[0],
                    art.msgid
                );
                mm.get_or_insert(format!("{}.MISMATCH", self.remote.ip()));
                pathelems.insert(0, mm.as_ref().unwrap());
            }
        }

        // insert our own name.
        pathelems.insert(0, &self.config.server.hostname);
        new_path = pathelems.join("!");

        // update.
        headers.update(HeaderName::Path, new_path.as_bytes());

        Ok((headers, body, v))
    }

    async fn read_article(&self, part: ArtPart, msgid: &str, buf: Buffer) -> io::Result<NntpResult> {
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
        match self.server.spool.read(loc, part, buf).await {
            Ok(mut buf) => {
                if part == ArtPart::Head {
                    buf.extend_from_slice(b".\r\n");
                }
                Ok(NntpResult::buffer(buf))
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
        self.server.tot_sessions.fetch_sub(1, Ordering::SeqCst);
        self.server.thr_sessions.fetch_sub(1, Ordering::SeqCst);
    }
}
