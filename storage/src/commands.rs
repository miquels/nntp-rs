//! Basic NNTP command parsing.

use std::io;
use std::io::Write;

/// Capabilities.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Capb {
    Always          = 0x00001,
    Authinfo        = 0x00002,
    Hdr             = 0x00004,
    Ihave           = 0x00008,
    NewNews         = 0x00010,
    Over            = 0x00020,
    Post            = 0x00040,
    Reader          = 0x00080,
    Sasl            = 0x00100,
    StartTls        = 0x00200,
    Streaming       = 0x00400,
    Version         = 0x00800,
    XPat            = 0x01000,
    ListActive      = 0x02000,
    ListActiveTimes = 0x04000,
    ListDistribPats = 0x08000,
    ModeHeadfeed    = 0x10000,
    ModeReader      = 0x20000,
    ModeStream      = 0x40000,
}

/// NNTP Commands
// NOTE keep alphabetical, so that "CmdNo" and "commands" are in the same order.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
#[allow(non_camel_case_types)]
pub enum CmdNo {
    Article = 0,
    Authinfo,
    Body,
    Capabilities,
    Check,
    Date,
    Group,
    Hdr,
    Head,
    Help,
    Ihave,
    Last,
    List_Active,
    List_ActiveTimes,
    List_DistribPats,
    List_Headers,
    List_Newsgroups,
    List_OverviewFmt,
    ListGroup,
    Mode_Headfeed,
    Mode_Reader,
    Mode_Stream,
    NewGroups,
    NewNews,
    Next,
    Over,
    Post,
    Quit,
    Stat,
    Takethis,
    XHdr,
    XOver,
    XPat,
    #[doc(hidden)]
    MaxCmd,
}

#[derive(Debug)]
struct CmdDef {
    cmd:    CmdNo,
    min:    usize,
    max:    usize,
    cap:    Capb,
    h:      &'static str,
}

// NOTE keep "CmdNo" and "commands" equal and in the same order.
static COMMANDS: [CmdDef; CmdNo::MaxCmd as usize] = [
    CmdDef{ cmd: CmdNo::Article,            min: 0, max: 1, cap: Capb::Always,          h: "ARTICLE [%M]" },
    CmdDef{ cmd: CmdNo::Authinfo,           min: 2, max: 2, cap: Capb::Authinfo,        h: "AUTHINFO USER name|PASS password" },
    CmdDef{ cmd: CmdNo::Body,               min: 0, max: 1, cap: Capb::Always,          h: "BODY [%M]" },
    CmdDef{ cmd: CmdNo::Capabilities,       min: 0, max: 1, cap: Capb::Always,          h: "CAPABILITIES [keyword]" },
    CmdDef{ cmd: CmdNo::Check,              min: 1, max: 1, cap: Capb::Streaming,       h: "CHECK %m" },
    CmdDef{ cmd: CmdNo::Date,               min: 0, max: 0, cap: Capb::Always,          h: "DATE" },
    CmdDef{ cmd: CmdNo::Group,              min: 1, max: 1, cap: Capb::Reader,          h: "GROUP newsgroup" },
    CmdDef{ cmd: CmdNo::Hdr ,               min: 3, max: 3, cap: Capb::Hdr,             h: "HDR [%R]" },
    CmdDef{ cmd: CmdNo::Head,               min: 0, max: 1, cap: Capb::Always,          h: "HEAD [%M]" },
    CmdDef{ cmd: CmdNo::Help,               min: 0, max: 0, cap: Capb::Always,          h: "HELP" },
    CmdDef{ cmd: CmdNo::Ihave,              min: 1, max: 1, cap: Capb::Ihave,           h: "IHAVE %m" },
    CmdDef{ cmd: CmdNo::Last,               min: 0, max: 0, cap: Capb::Reader,          h: "LAST" },
    CmdDef{ cmd: CmdNo::List_Active,        min: 0, max: 1, cap: Capb::ListActive,      h: "LIST ACTIVE [%w]" },
    CmdDef{ cmd: CmdNo::List_ActiveTimes,   min: 0, max: 1, cap: Capb::ListActiveTimes, h: "LIST ACTIVE.TIMES [%w]" },
    CmdDef{ cmd: CmdNo::List_DistribPats,   min: 0, max: 0, cap: Capb::ListDistribPats, h: "LIST DISTRIB.PATS" },
    CmdDef{ cmd: CmdNo::List_Headers,       min: 0, max: 0, cap: Capb::Hdr,             h: "LIST HEADERS [%M]" },
    CmdDef{ cmd: CmdNo::List_Newsgroups,    min: 0, max: 1, cap: Capb::Reader,          h: "LIST NEWSGROUPS [%w]" },
    CmdDef{ cmd: CmdNo::List_OverviewFmt,   min: 0, max: 0, cap: Capb::Over,            h: "LIST OVERVIEW.FMT" },
    CmdDef{ cmd: CmdNo::ListGroup,          min: 1, max: 2, cap: Capb::Reader,          h: "LISTGROUP [newsgroup [%r]]" },
    CmdDef{ cmd: CmdNo::Mode_Headfeed,      min: 0, max: 0, cap: Capb::ModeHeadfeed,    h: "MODE HEADFEED" },
    CmdDef{ cmd: CmdNo::Mode_Reader,        min: 0, max: 0, cap: Capb::ModeReader,      h: "MODE READER" },
    CmdDef{ cmd: CmdNo::Mode_Stream,        min: 0, max: 0, cap: Capb::Streaming,       h: "MODE STREAM" },
    CmdDef{ cmd: CmdNo::NewGroups,          min: 1, max: 4, cap: Capb::Reader,
                h: "NEWGROUPS [yy]YYMMDD HHMMSS [GMT]" },
    CmdDef{ cmd: CmdNo::NewNews,            min: 1, max: 5, cap: Capb::NewNews,
                h: "NEWNEWS %w [yy]yymmdd <hhmmss> [GMT]" },
    CmdDef{ cmd: CmdNo::Next,               min: 0, max: 0, cap: Capb::Reader,          h: "NEXT", },
    CmdDef{ cmd: CmdNo::Over,               min: 0, max: 1, cap: Capb::Over,            h: "OVER [%r]", },
    CmdDef{ cmd: CmdNo::Post,               min: 0, max: 0, cap: Capb::Post,            h: "POST" },
    CmdDef{ cmd: CmdNo::Quit,               min: 0, max: 0, cap: Capb::Always,          h: "QUIT" },
    CmdDef{ cmd: CmdNo::Stat,               min: 0, max: 1, cap: Capb::Always,          h: "STAT [%M]" },
    CmdDef{ cmd: CmdNo::Takethis,           min: 1, max: 1, cap: Capb::Streaming,       h: "TAKETHIS %m" },
    CmdDef{ cmd: CmdNo::XHdr ,              min: 3, max: 3, cap: Capb::Hdr,             h: "XHDR [%R]" },
    CmdDef{ cmd: CmdNo::XOver,              min: 0, max: 1, cap: Capb::Over,            h: "XOVER [%r]", },
    CmdDef{ cmd: CmdNo::XPat,               min: 3, max: 0, cap: Capb::XPat,            h: "XPAT header %R pattern [pattern..]" },
];

// very cheap ASCII lowercasing.
fn lowercase<'a>(s: &str, buf: &'a mut [u8]) -> &'a [u8] {
    let b = s.as_bytes();
    let mut idx = 0;
    for i in 0..b.len() {
        if i < buf.len() {
            let mut c = b[idx];
            if c >= b'A' && c <= b'Z' {
                c += 32;
            }
            buf[idx] = c;
            idx += 1;
        }
    }
    &buf[..idx]
}

pub struct Cmd {
    caps:       usize,
}

impl Cmd {
    /// Return a fresh Cmd.
    pub fn new() -> Cmd {
        Cmd{
            caps:   Capb::Always as usize,
        }
    }

    /// Add a capability.
    pub fn add_cap(&mut self, caps: Capb) {
        self.caps |= caps as usize;
    }

    /// Remove a capability.
    pub fn remove_cap(&mut self, caps: Capb) {
        self.caps &= !(caps as usize);
    }

    /// Basic command parsing and checking.
    pub fn parse<'a>(&self, data: &'a mut String) -> Result<(CmdNo, Vec<&'a str>), &'static str> {

        let mut args : Vec<_> = data.split_whitespace().collect();
        if args.len() == 0 {
            return Err("501 Syntax error");
        }
        let mut buf = [0u8; 32];

        // turn `command subcommand' into a single command.
        match lowercase(args[0], &mut buf[..]) {
            b"mode" if args.len() == 1 => {
                return Err("501 Syntax error");
            },
            b"mode" => {
                match lowercase(args[1], &mut buf[..]) {
                    b"headfeed" => args[0] = "mode_headfeed",
                    b"reader"   => args[0] = "mode_reader",
                    b"stream"   => args[0] = "mode_stream",
                    _ => return Err("501 Unknown mode option"),
                }
                args.remove(1);
            },
            b"list" if args.len() == 1 => {
                args[0] = "list_active";
            },
            b"list" => {
                match lowercase(args[1], &mut buf[..]) {
                    b"active"           => args[0] = "list_active",
                    b"active.times"     => args[0] = "list_active.times",
                    b"distrib.pats"     => args[0] = "list_distrib.pats",
                    b"headers"          => args[0] = "list_headers",
                    b"newsgroups"       => args[0] = "list_newsgroups",
                    b"overview.fmt"     => args[0] = "list_overview.fmt",
                    _ => return Err("501 Unknown list option"),
                }
                args.remove(1);
            },
            _ => {},
        }

        // special handling for authinfo user|pass <arg>, do not split <arg>
        match lowercase(args[0], &mut buf[..]) {
            b"authinfo" if args.len() == 1 => {
                return Err("501 Syntax error");
            },
            b"authinfo" => {
                match lowercase(args[1], &mut buf[..]) {
                    s @ b"user" | s @ b"pass" => {
                        args.truncate(0);
                        args.extend(data.splitn(3, |c| c == ' ' || c == '\t'));
                        args[0] = "authinfo";
                        args[1] = if s == b"user" { "user" } else { "pass" };
                    },
                    _ => return Err("501 Unknown authinfo option"),
                }
                args.remove(1);
            },
            _ => {},
        }

        // match command
        let cmd = match lowercase(args[0], &mut buf[..]) {
            b"article"              => &COMMANDS[CmdNo::Article as usize],
            b"authinfo"             => &COMMANDS[CmdNo::Authinfo as usize],
            b"body"                 => &COMMANDS[CmdNo::Body as usize],
            b"capabilities"         => &COMMANDS[CmdNo::Capabilities as usize],
            b"check"                => &COMMANDS[CmdNo::Check as usize],
            b"date"                 => &COMMANDS[CmdNo::Date as usize],
            b"group"                => &COMMANDS[CmdNo::Group as usize],
            b"hdr"                  => &COMMANDS[CmdNo::Hdr as usize],
            b"head"                 => &COMMANDS[CmdNo::Head as usize],
            b"help"                 => &COMMANDS[CmdNo::Help as usize],
            b"ihave"                => &COMMANDS[CmdNo::Ihave as usize],
            b"last"                 => &COMMANDS[CmdNo::Last as usize],
            b"list_active"          => &COMMANDS[CmdNo::List_Active as usize],
            b"list_active.times"    => &COMMANDS[CmdNo::List_ActiveTimes as usize],
            b"list_distrib.pats"    => &COMMANDS[CmdNo::List_DistribPats as usize],
            b"list_heqders"         => &COMMANDS[CmdNo::List_Headers as usize],
            b"list_newsgroups"      => &COMMANDS[CmdNo::List_Newsgroups as usize],
            b"list_overview.fmt"    => &COMMANDS[CmdNo::List_OverviewFmt as usize],
            b"listgroup"            => &COMMANDS[CmdNo::ListGroup as usize],
            b"mode_headfeed"        => &COMMANDS[CmdNo::Mode_Headfeed as usize],
            b"mode_reader"          => &COMMANDS[CmdNo::Mode_Reader as usize],
            b"mode_stream"          => &COMMANDS[CmdNo::Mode_Stream as usize],
            b"newgroups"            => &COMMANDS[CmdNo::NewGroups as usize],
            b"newnews"              => &COMMANDS[CmdNo::NewNews as usize],
            b"next"                 => &COMMANDS[CmdNo::Next as usize],
            b"over"                 => &COMMANDS[CmdNo::Over as usize],
            b"post"                 => &COMMANDS[CmdNo::Post as usize],
            b"quit"                 => &COMMANDS[CmdNo::Quit as usize],
            b"stat"                 => &COMMANDS[CmdNo::Stat as usize],
            b"takethis"             => &COMMANDS[CmdNo::Takethis as usize],
            b"xhdr"                 => &COMMANDS[CmdNo::Hdr as usize],
            b"xover"                => &COMMANDS[CmdNo::Over as usize],
            b"xpat"                 => &COMMANDS[CmdNo::XPat as usize],
            _                       => return Err("500 Unknown command"),
        };

        // Do we allow this command?
        if (self.caps & (cmd.cap as usize)) == 0 {
            if args[0].contains("_") {
                return Err("503 Not supported");
            } else {
                return Err("500 Unknown command");
            }
        }

        // check number of args
        let c = args.len() - 1;
        if c < cmd.min {
            return Err("501 Missing argument(s)");
        }
        if c > cmd.max && cmd.max > 0 {
            return Err("501 Too many arguments");
        }

        Ok((cmd.cmd, args))
    }

    pub fn help<W: Write>(&self, mut out: W) -> io::Result<()> {
        write!(out, "100 Legal commands\r\n")?;
        for cmd in COMMANDS.iter() {
            if (self.caps & (cmd.cap as usize)) == 0 {
                continue;
            }
            let mut s = cmd.h.replace("%w", "wildmat");
            s = s.replace("%m", "message-ID");
            s = s.replace("%r", "range");
            if (self.caps & Capb::Reader as usize) > 0 {
                s = s.replace("%R", "message-ID|range");
                s = s.replace("%M", "message-ID|number>");
            } else {
                s = s.replace("[%R]", "message-ID");
                s = s.replace("[%M]", "message-ID");
                s = s.replace("%R", "message-ID");
                s = s.replace("%M", "message-ID");
            }
            write!(out, "  {}\r\n", s)?;
        }
        write!(out, ".\r\n")
    }

    pub fn capabilities<W: Write>(&self, mut out: W) -> io::Result<()> {
        write!(out, "101 Capability list:\r\n")?;
        write!(out, "VERSION: 2\r\n")?;
        write!(out, "IMPLEMENTATION: Duivel 0.1\r\n")?;
        if (self.caps & Capb::Authinfo as usize) > 0 {
            write!(out, "AUTHINFO\r\n")?;
        }
        if (self.caps & Capb::Hdr as usize) > 0 {
            write!(out, "HDR\r\n")?;
        }
        if (self.caps & Capb::Ihave as usize) > 0 {
            write!(out, "IHAVE\r\n")?;
        }

        let mut v = Vec::new();
        if (self.caps & Capb::ListActive as usize) > 0 {
            v.push("ACTIVE");
        }
        if (self.caps & Capb::ListActiveTimes as usize) > 0 {
            v.push("ACTIVE.TIMES");
        }
        if (self.caps & Capb::ListDistribPats as usize) > 0 {
            v.push("DISTRIB.PATS");
        }
        if (self.caps & Capb::Hdr as usize) > 0 {
            v.push("HEADERS");
        }
        if (self.caps & Capb::Reader as usize) > 0 {
            v.push("NEWSGROUPS");
        }
        if v.len() > 0 {
            write!(out, "LIST {}\r\n", v.join(" "))?;
        }

        if (self.caps & Capb::Reader as usize) > 0 {
            write!(out, "MODE-READER\r\n")?;
        }
        if (self.caps & Capb::NewNews as usize) > 0 {
            write!(out, "NEWSNEWS\r\n")?;
        }
        if (self.caps & Capb::Over as usize) > 0 {
            write!(out, "OVER\r\n")?;
        }
        if (self.caps & Capb::Post as usize) > 0 {
            write!(out, "POST\r\n")?;
        }
        if (self.caps & Capb::Reader as usize) > 0 {
            write!(out, "READER\r\n")?;
        }
        if (self.caps & Capb::Sasl as usize) > 0 {
            write!(out, "SASL\r\n")?;
        }
        if (self.caps & Capb::StartTls as usize) > 0 {
            write!(out, "STARTTLS\r\n")?;
        }
        if (self.caps & Capb::Streaming as usize) > 0 {
            write!(out, "STREAMING\r\n")?;
        }
        write!(out, ".\r\n")
    }
}

/// Is this a valid message-id?
pub fn is_msgid(m: &str) -> bool {
    m.starts_with("<")
}

/// Parse article number.
pub fn parse_artno(m: &str) -> Option<u64> {
    m.parse::<u64>().ok()
}

/// Article range or message id.
#[derive(Debug)]
pub enum ArtRange<'a> {
    MessageId(&'a str),
    ArtNo(u64),
    Begin(u64),
    Range((u64, u64)),
}

/// Parse article range.
pub fn parse_artrange(m: &str) -> Option<ArtRange> {
    let r : Vec<_> = m.split('-').collect();
    let b = r[0].parse::<u64>().ok()?;
    match r.len() {
        1 => Some(ArtRange::ArtNo(b)),
        2 if r[1].len() == 0 => Some(ArtRange::Begin(b)),
        2 if r[1].len() != 0 => Some(ArtRange::Range((b, r[1].parse::<u64>().ok()?))),
        _ => None,
    }
}

/// Parse message-id or article range.
pub fn parse_artrange_msgid(m: &str) -> Option<ArtRange> {
    if is_msgid(m) {
        Some(ArtRange::MessageId(m))
    } else {
        parse_artrange(m)
    }
}

