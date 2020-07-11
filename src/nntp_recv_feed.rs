// Feeder.
//
// This implements support for IHAVE, TAKETHIS, ARTICLE/HEAD/BODY.
//
use std;
use std::io;
use std::mem;

use crate::article::{Article, HeaderName, Headers, HeadersParser};
use crate::commands;
use crate::errors::*;
use crate::history::{HistEnt, HistError, HistStatus};
use crate::nntp_recv::{ArtAccept, NntpReceiver, NntpResult};
use crate::nntp_send::FeedArticle;
use crate::spool::{ArtPart, SPOOL_DONTSTORE, SPOOL_REJECTARTS};
use crate::util::Buffer;
use crate::util::{self, HashFeed, MatchList, MatchResult, UnixTime};

impl NntpReceiver {
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
            head_only: false,
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
        let _ = self.outfeed.send(feed_art);

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
                // XXX TODO ratelimit message.
                log::warn!(
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

    pub(crate) async fn read_article(
        &self,
        part: ArtPart,
        msgid: &str,
        buf: Buffer,
    ) -> io::Result<NntpResult>
    {
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
