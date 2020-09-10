//! NNTP server.
//!
//! This handles the incoming NNTP protocol: read commands, send reply.
//!
//! - The basic and common commands are handled in this module.
//! - The feeder commands (IHAVE, CHECK, TAKETHIS etc) are handled in `nntp_recv`.
//! - The reader commands (having to do with newsgroups and posting) are in `nntp_reader`.
//!
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::bus::Notification;
use crate::commands::{Capb, Cmd, CmdParser};
use crate::config::{self, Config};
use crate::metrics::RxSessionStats;
use crate::history::HistStatus;
use crate::logger;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_codec::{NntpCodec, NntpLine};
use crate::nntp_send::FeedArticle;
use crate::server::{self, Server};
use crate::spool::ArtPart;
use crate::util::{Buffer, UnixTime};

pub struct NntpServer {
    pub codec:                  NntpCodec,
    pub(crate) parser:          CmdParser,
    pub(crate) server:          Server,
    pub(crate) remote:          SocketAddr,
    pub(crate) newsfeeds:       Arc<NewsFeeds>,
    pub(crate) config:          Arc<Config>,
    pub(crate) outfeed:         mpsc::Sender<FeedArticle>,
    pub(crate) incoming_logger: logger::Incoming,
    pub(crate) peer_idx:        usize,
    pub(crate) headfeed:        bool,
    pub(crate) active:          bool,
    pub(crate) stats:           RxSessionStats,
    pub(crate) quit:            bool,
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
pub(crate) enum ArtAccept {
    // accept and store.
    Accept,
    // reject and store rejection in history file.
    Reject,
    // try again later
    Defer,
}

impl NntpServer {
    pub fn new(peer: SocketAddr, codec: NntpCodec, server: Server, stats: RxSessionStats) -> NntpServer {
        let newsfeeds = config::get_newsfeeds();
        let config = config::get_config();
        let incoming_logger = logger::get_incoming_logger();
        let outfeed = server.outfeed.clone();

        // decremented in Drop.
        server::inc_sessions();

        NntpServer {
            codec,
            server,
            newsfeeds,
            config,
            incoming_logger,
            outfeed,
            parser: CmdParser::new(),
            remote: peer,
            peer_idx: 0,
            active: false,
            stats: stats,
            headfeed: false,
            quit: false,
        }
    }

    pub(crate) fn thispeer(&self) -> &NewsPeer {
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
            },
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
                Ok(NntpLine::Line(buf)) => {
                    match self.cmd(buf).await {
                        Ok(res) => res,
                        Err(e) => {
                            let res = NntpResult::text(format!("400 {}", e));
                            self.on_generic_error(e);
                            self.quit = true;
                            res
                        },
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
        if !self.thispeer().xclient {
            self.stats.on_disconnect();
        }
    }

    // Initial connect. Here we decide if we want to accept this
    // connection, or refuse it.
    pub(crate) async fn on_connect(&mut self) -> Result<NntpResult, NntpResult> {
        let remote = self.remote.ip();
        let (idx, peer) = match self.newsfeeds.find_peer(&remote) {
            None => {
                log::info!("Connection {} from {} (no permission)", self.stats.fdno(), remote);
                let msg = format!("502 permission denied to {}", remote);
                return Err(NntpResult::text(msg));
            },
            Some(x) => x,
        };
        self.peer_idx = idx;

        if peer.xclient {
            self.parser.remove_cap(Capb::Basic);
            self.parser.add_cap(Capb::XClient);
            let msg = format!("200 {} XCLIENT mode", self.config.server.hostname);
            return Ok(NntpResult::text(msg));
        }

        let count = self.server.add_connection(&peer.label);
        self.active = true;
        if count > peer.maxconnect as usize && peer.maxconnect > 0 {
            log::info!(
                "Connect Limit exceeded (from dnewsfeeds) for {} ({}) ({} > {})",
                peer.label,
                remote,
                count,
                peer.maxconnect
            );
            let msg = format!("502 too many connections from {} (max {})", peer.label, count - 1);
            return Err(NntpResult::text(msg));
        }

        self.stats
            .on_connect(peer.label.to_string(), remote)
            .await;

        let code = if peer.readonly {
            201
        } else {
            self.parser.add_cap(Capb::Ihave);
            self.parser.add_cap(Capb::Streaming);
            200
        };
        if peer.accept_headfeed {
            self.parser.add_cap(Capb::ModeHeadfeed);
        }
        let msg = format!("{} {} hello {}", code, self.config.server.hostname, peer.label);
        Ok(NntpResult::text(msg))
    }

    /// Called when we got an error writing to the socket.
    fn on_write_error(&self, err: io::Error) {
        let stats = &self.stats;
        if err.kind() == io::ErrorKind::NotFound {
            log::info!(
                "Forcibly closed connection {} from {} {}",
                stats.fdno(),
                stats.hostname(),
                stats.ipaddr()
            );
        } else {
            log::info!(
                "Write error on connection {} from {} {}: {}",
                stats.fdno(),
                stats.hostname(),
                stats.ipaddr(),
                err
            );
        }
    }

    /// Called when we got an error reading from the socket.
    /// Log an error, and perhaps create a reply if appropriate.
    fn on_read_error(&mut self, err: io::Error) -> Option<NntpResult> {
        let stats = &self.stats;
        if err.kind() == io::ErrorKind::NotFound {
            log::info!(
                "Forcibly closed connection {} from {} {}",
                stats.fdno(),
                stats.hostname(),
                stats.ipaddr()
            );
        } else {
            log::info!(
                "Read error on connection {} from {} {}: {}",
                stats.fdno(),
                stats.hostname(),
                stats.ipaddr(),
                err
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
        log::info!(
            "Error on connection {} from {} {}: {}",
            stats.fdno(),
            stats.hostname(),
            stats.ipaddr(),
            err
        );
    }

    /// Process NNTP command
    async fn cmd(&mut self, input: Buffer) -> io::Result<NntpResult> {
        let line = match input.as_utf8_str() {
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
                return Ok(NntpResult::buffer(b));
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
                return self.cmd_check(args).await;
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
                return self.cmd_ihave(args).await;
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
                return self.cmd_mode_stream().await;
            },
            Cmd::Mode_Headfeed => {
                return self.cmd_mode_headfeed().await;
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
                return self.cmd_takethis(args).await;
            },
            Cmd::XClient => {
                return self.cmd_xclient(args).await;
            },
            _ => {},
        }

        Ok(NntpResult::text("500 What?"))
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
                if he.head_only && part != ArtPart::Head {
                    return Ok(NntpResult::text("430 Not found"));
                }
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

impl Drop for NntpServer {
    fn drop(&mut self) {
        if self.active {
            let name = &self.thispeer().label;
            self.server.remove_connection(name);
        }
        server::dec_sessions();
    }
}
