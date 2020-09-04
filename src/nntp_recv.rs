//! NNTP receiver.
//!
//! This implements support for IHAVE, MODE STREAM, MODE HEADFEED, CHECK/TAKETHIS.
//!
use std;
use std::io;
use std::mem;
use std::net::{IpAddr, SocketAddr};

use crate::article::{Article, HeaderName, Headers, HeadersParser};
use crate::commands;
use crate::diag::Stats;
use crate::errors::*;
use crate::history::{HistEnt, HistError, HistStatus};
use crate::nntp_send::FeedArticle;
use crate::nntp_server::{ArtAccept, NntpResult, NntpServer};
use crate::spool::{SPOOL_DONTSTORE, SPOOL_REJECTARTS};
use crate::util::Buffer;
use crate::util::{self, HashFeed, MatchList, MatchResult, UnixTime};

impl NntpServer {
    pub(crate) async fn cmd_check(&mut self, args: Vec<&str>) -> io::Result<NntpResult> {
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
        Ok(NntpResult::text(&s))
    }

    pub(crate) async fn cmd_ihave(&mut self, args: Vec<&str>) -> io::Result<NntpResult> {
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

        self.codec
            .write_buf(Buffer::from("335 Send article; end with CRLF DOT CRLF"))
            .await?;

        let mut art = self.codec.read_article(args[0]).await?;
        let status = self.received_article(&mut art, true).await?;
        let code = match status {
            ArtAccept::Accept => 235,
            ArtAccept::Defer => 436,
            ArtAccept::Reject => 437,
        };
        Ok(NntpResult::text(&format!("{} {}", code, art.msgid)))
    }

    pub(crate) async fn cmd_mode_stream(&mut self) -> io::Result<NntpResult> {
        Ok(NntpResult::text("203 Streaming permitted"))
    }

    pub(crate) async fn cmd_mode_headfeed(&mut self) -> io::Result<NntpResult> {
        self.headfeed = true;
        Ok(NntpResult::text("250 Mode command OK"))
    }

    pub(crate) async fn cmd_takethis(&mut self, args: Vec<&str>) -> io::Result<NntpResult> {
        self.stats.inc(Stats::Takethis);
        self.stats.inc(Stats::Offered);
        let mut art = self.codec.read_article(args[0]).await?;
        let status = self.received_article(&mut art, false).await?;
        let code = match status {
            ArtAccept::Accept => 239,
            ArtAccept::Defer | ArtAccept::Reject => 439,
        };
        Ok(NntpResult::text(&format!("{} {}", code, art.msgid)))
    }

    pub(crate) async fn cmd_xclient(&mut self, args: Vec<&str>) -> io::Result<NntpResult> {
        let remote_ip = match args[0].parse::<IpAddr>() {
            Ok(remote) => remote,
            Err(_) => return Ok(NntpResult::text("501 invalid ip-address")),
        };
        self.remote = SocketAddr::new(remote_ip, 119);
        let reply = match self.on_connect().await {
            Ok(msg) => msg,
            Err(msg) => {
                self.quit = true;
                msg
            }
        };
        Ok(reply)
    }

    pub(crate) async fn reject_art(
        &mut self,
        art: &Article,
        recv_time: UnixTime,
        e: ArtError,
    ) -> io::Result<ArtAccept>
    {
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

    pub(crate) async fn dontstore_art(
        &mut self,
        art: &Article,
        recv_time: UnixTime,
    ) -> io::Result<ArtAccept>
    {
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
    pub(crate) async fn received_article(
        &mut self,
        mut art: &mut Article,
        can_defer: bool,
    ) -> io::Result<ArtAccept>
    {
        let recv_time = UnixTime::now();

        // parse article.
        let (headers, body, wantpeers) = match self.process_headers(&mut art) {
            Err(e) => {
                self.stats.art_error(&art, &e);
                return match e {
                    // article was mangled on the way. do not store the message-id
                    // in the history file, another peer may send a correct version.
                    ArtError::TooSmall |
                    ArtError::HdrOnlyNoBytes |
                    ArtError::HdrOnlyWithBody |
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
                return Ok(ArtAccept::Reject);
            },
            Err(HistError::IoError(e)) => {
                // I/O error, no incoming.log - we reply with status 400.
                log::error!("received_article {}: history lookup: {}", msgid, e);
                self.stats.art_error(art, &ArtError::IOError);
                return Err(e);
            },
            Ok(_) => {},
        }

        // store the article.
        let mut buffer = Buffer::new();
        headers.header_bytes(&mut buffer);
        let artloc = match spool.write(spool_no, buffer, body).await {
            Ok(loc) => loc,
            Err(e) => {
                log::error!("received_article {}: spool write: {}", msgid, e);
                self.stats.art_error(art, &ArtError::IOError);
                history.store_rollback(msgid);
                return Err(e);
            },
        };
        let he = HistEnt {
            status:    HistStatus::Present,
            time:      recv_time,
            head_only: self.headfeed,
            location:  Some(artloc.clone()),
        };

        if let Err(e) = history.store_commit(msgid, he).await {
            log::error!("received_article {}: history write: {}", msgid, e);
            self.stats.art_error(art, &ArtError::IOError);
            history.store_rollback(msgid);
            return Err(e);
        }

        let peers = &self.newsfeeds.peers;

        // log and stats.
        self.incoming_logger.accept(label, art, peers, &wantpeers);
        self.stats.art_accepted(art);

        // outgoing feed.
        let outpeers = wantpeers
            .iter()
            .map(|i| peers[*i as usize].label.clone())
            .collect();
        let feed_art = FeedArticle {
            msgid:    art.msgid.clone(),
            location: artloc,
            size:     art.len,
            peers:    outpeers,
        };
        let _ = self.outfeed.send(feed_art).await;

        Ok(ArtAccept::Accept)
    }

    // parse the received article headers, then see if we want it.
    // Note: modifies/updates the Path: header.
    fn process_headers(&self, art: &mut Article) -> ArtResult<(Headers, Buffer, Vec<u32>)> {
        // Check that the article has a minimum size. Sometimes a peer is offering
        // you empty articles because they no longer exist on their spool.
        if art.data.len() < 80 {
            return Err(ArtError::TooSmall);
        }

        // Check the maximum article size.
        if let Some(maxsize) = self.config.server.maxartsize {
            if art.data.len() as u64 > maxsize {
                return Err(ArtError::TooBig);
            }
        }

        // Make sure the message-id we got from TAKETHIS is valid.
        // We cannot check this in advance; TAKETHIS must read the whole article first.
        if !commands::is_msgid(&art.msgid) {
            return Err(ArtError::BadMsgId);
        }

        let mut parser = HeadersParser::new();
        let buffer = mem::replace(&mut art.data, Buffer::new());
        match parser.parse(&buffer, false, true) {
            None => {
                log::error!("failure parsing header, None returned");
                panic!("process_headers: this code should never be reached")
            },
            Some(Err(e)) => return Err(e),
            Some(Ok(_)) => {},
        }
        let (mut headers, mut body) = parser.into_headers(buffer);

        // Header-only feed cannot have a body.
        if self.headfeed {
            // "\r\n.\r\n" == 5 bytes.
            if body.len() > 5 {
                return Err(ArtError::HdrOnlyWithBody);
            }
            // truncate to just "\r\n".
            body.truncate(2);
        }

        let mut pathelems = headers.path().ok_or(ArtError::NoPath)?;
        art.pathhost = Some(pathelems[0].to_string());

        // some more header checks.
        {
            if self.headfeed {
                let bytes = headers
                    .get_str(HeaderName::Bytes)
                    .ok_or(ArtError::HdrOnlyNoBytes)?;
                bytes.parse::<u64>().map_err(|_| ArtError::HdrOnlyNoBytes)?;
            }

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
                false,
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
                self.headfeed,
            ) {
                v.push(idx as u32);
            }
        }

        // should match one of the pathaliases.
        if !thispeer.nomismatch && !thispeer.pathalias.contains(art.pathhost.as_ref().unwrap()) {
            use std::sync::atomic::AtomicU64;
            static LAST_MSG: AtomicU64 = AtomicU64::new(0);
            let last_msg: UnixTime =  (&LAST_MSG).into();
            if last_msg.seconds_elapsed() >= 10 {
                // Ratelimited message, max 1 per 10 secs.
                UnixTime::now().to_atomic(&LAST_MSG);
                log::warn!(
                    "{} {} Path element fails to match aliases: {} in {}",
                    thispeer.label,
                    self.remote.ip(),
                    pathelems[0],
                    art.msgid
                );
            }
            mm.get_or_insert(format!("{}.MISMATCH", self.remote.ip()));
            pathelems.insert(0, mm.as_ref().unwrap());
        }

        // insert our own name.
        let commonpath = self.config.server.commonpath.as_str();
        if commonpath != "" && !pathelems.contains(&commonpath) {
            pathelems.insert(0, commonpath);
        }
        pathelems.insert(0, &self.config.server.pathhost);
        new_path = pathelems.join("!");

        // update.
        headers.update(HeaderName::Path, new_path.as_bytes());

        Ok((headers, body, v))
    }
}
