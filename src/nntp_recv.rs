use std;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::bus::Notification;
use crate::commands::{self, Capb, Cmd, CmdParser};
use crate::config::{self, Config};
use crate::diag::{SessionStats, Stats};
use crate::history::HistStatus;
use crate::logger;
use crate::newsfeeds::{NewsFeeds, NewsPeer};
use crate::nntp_codec::{NntpCodec, NntpLine};
use crate::nntp_send::FeedArticle;
use crate::server::{self, Server};
use crate::spool::ArtPart;
use crate::util::{Buffer, UnixTime};

pub struct NntpReceiver {
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
    pub(crate) stats:           SessionStats,
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

impl NntpReceiver {
    pub fn new(peer: SocketAddr, codec: NntpCodec, server: Server, stats: SessionStats) -> NntpReceiver {
        let newsfeeds = config::get_newsfeeds();
        let config = config::get_config();
        let incoming_logger = logger::get_incoming_logger();
        let outfeed = server.outfeed.clone();

        // decremented in Drop.
        server::inc_sessions();

        NntpReceiver {
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
        self.stats.on_disconnect();
    }

    // Initial connect. Here we decide if we want to accept this
    // connection, or refuse it.
    async fn on_connect(&mut self) -> Result<NntpResult, NntpResult> {
        let remote = self.remote.ip();
        let (idx, peer) = match self.newsfeeds.find_peer(&remote) {
            None => {
                log::info!("Connection {} from {} (no permission)", self.stats.fdno, remote);
                let msg = format!("502 permission denied to {}", remote);
                return Err(NntpResult::text(msg));
            },
            Some(x) => x,
        };
        self.peer_idx = idx;

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
            .on_connect(remote.to_string(), peer.label.to_string())
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
                stats.fdno,
                stats.hostname,
                stats.ipaddr
            );
        } else {
            log::info!(
                "Write error on connection {} from {} {}: {}",
                stats.fdno,
                stats.hostname,
                stats.ipaddr,
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
                stats.fdno,
                stats.hostname,
                stats.ipaddr
            );
        } else {
            log::info!(
                "Read error on connection {} from {} {}: {}",
                stats.fdno,
                stats.hostname,
                stats.ipaddr,
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
            stats.fdno,
            stats.hostname,
            stats.ipaddr,
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
                return self.read_article(part, args[0], buf).await
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
            Cmd::Mode_Headfeed => {
                self.headfeed = true;
                return Ok(NntpResult::text("250 Mode command OK"));
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
}

impl Drop for NntpReceiver {
    fn drop(&mut self) {
        if self.active {
            let name = &self.thispeer().label;
            self.server.remove_connection(name);
        }
        server::dec_sessions();
    }
}
