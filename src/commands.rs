//! NNTP command parsing.

use std::collections::{HashMap, HashSet};
use std::str;

use bytes::{BufMut, Bytes, BytesMut};
use once_cell::sync::Lazy;
use regex::Regex;

/// Capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capb {
    Never           = 0x00000,
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
    ListActive      = 0x01000,
    ListActiveTimes = 0x02000,
    ListDistribPats = 0x04000,
    ModeHeadfeed    = 0x08000,
    ModeReader      = 0x10000,
    ModeStream      = 0x20000,
}

#[derive(Debug)]
struct CmdDef {
    cmd:        Cmd,
    name:       &'static str,
    min:        usize,
    max:        usize,
    has_subcmd: bool,
    cap:        Capb,
    h:          &'static str,
}

// This macro generates the Cmd enum, and a few functions to initialize
// the cmdname -> CmdDef lookup hashmaps.
macro_rules! cmd {
    {$(($name:expr, $cmd:ident, $min:expr, $max:expr, $cap:expr, $help:expr)),*} => {
        #[derive(Debug,Clone,Copy,PartialEq,Eq)]
		#[allow(non_camel_case_types)]
        #[repr(usize)]
        pub enum Cmd { $($cmd),* }

        // build a command -> CmdDef hashmap
        fn build_cmd_map() -> HashMap<String, CmdDef> {
            let mut hm = HashMap::new();
            let mut has_sub = HashSet::new();
            $(
                // if this is a subcommand, remember the first word.
                let mut words = $name.split_whitespace();
                let w0 = words.next();
                if words.next().is_some() {
                    has_sub.insert(w0.unwrap());
                }

                let def = CmdDef{
                    cmd: Cmd::$cmd,
                    has_subcmd: has_sub.contains($name),
                    name: $name,
                    min: $min,
                    max:$max,
                    cap: $cap,
                    h: $help
                };
                hm.insert($name.to_string(), def);
            )*
            hm
        }
    }
}

// And here it is actually generated.
cmd! {
    ( "article",            Article,            0, 1, Capb::Always,          "[%M]" ),
    ( "authinfo user",      Authinfo_User,      2, 2, Capb::Authinfo,        "name" ),
    ( "authinfo pass",      Authinfo_Pass,      2, 2, Capb::Authinfo,        "password" ),
    ( "authinfo",           Authinfo,           1, 0, Capb::Never   ,        "" ),
    ( "body",               Body,               0, 1, Capb::Always,          "[%M]" ),
    ( "capabilities",       Capabilities,       0, 1, Capb::Always,          "[keyword]" ),
    ( "check",              Check,              1, 1, Capb::Streaming,       "%m" ),
    ( "date",               Date,               0, 0, Capb::Always,          "" ),
    ( "group",              Group,              1, 1, Capb::Reader,          "newsgroup" ),
    ( "hdr",                Hdr ,               3, 3, Capb::Hdr,             "[%R]" ),
    ( "head",               Head,               0, 1, Capb::Always,          "[%M]" ),
    ( "help",               Help,               0, 0, Capb::Always,          "" ),
    ( "ihave",              Ihave,              1, 1, Capb::Ihave,           "%m" ),
    ( "last",               Last,               0, 0, Capb::Reader,          "" ),
    ( "list active",        List_Active,        0, 1, Capb::ListActive,      "[%w]" ),
    ( "list active.times",  List_ActiveTimes,   0, 1, Capb::ListActiveTimes, "[%w]" ),
    ( "list distrib.pats",  List_DistribPats,   0, 0, Capb::ListDistribPats, "" ),
    ( "list headers",       List_Headers,       0, 0, Capb::Hdr,             "[%M]" ),
    ( "list newsgroups",    List_Newsgroups,    0, 1, Capb::Reader,          "[%w]" ),
    ( "list overview.fmt",  List_OverviewFmt,   0, 0, Capb::Over,            "" ),
    ( "list",               List,               0, 0, Capb::ListActive,      "" ),
    ( "listgroup",          ListGroup,          1, 2, Capb::Reader,          "[newsgroup [%r]]" ),
    ( "mode headfeed",      Mode_Headfeed,      0, 0, Capb::ModeHeadfeed,    "" ),
    ( "mode reader",        Mode_Reader,        0, 0, Capb::ModeReader,      "" ),
    ( "mode stream",        Mode_Stream,        0, 0, Capb::Streaming,       "" ),
    ( "mode",               Mode,               1, 0, Capb::Never,           "" ),
    ( "newgroups",          NewGroups,          1, 4, Capb::Reader,          "[yy]yymmdd hhmmss [GMT]" ),
    ( "newnews",            NewNews,            1, 5, Capb::NewNews,         " %w [yy]yymmdd <hhmmss> [GMT]" ),
    ( "next",               Next,               0, 0, Capb::Reader,          "" ),
    ( "over",               Over,               0, 1, Capb::Over,            "[%r]" ),
    ( "post",               Post,               0, 0, Capb::Post,            "" ),
    ( "quit",               Quit,               0, 0, Capb::Always,          "" ),
    ( "stat",               Stat,               0, 1, Capb::Always,          "[%M]" ),
    ( "takethis",           Takethis,           1, 1, Capb::Streaming,       "%m" ),
    ( "xhdr",               XHdr ,              3, 3, Capb::Hdr,             "[%R]" ),
    ( "xover",              XOver,              0, 1, Capb::Over,            "[%r]" ),
    ( "xpat",               XPat,               3, 0, Capb::Hdr,             "header %R pattern [pattern..]" )
}

// initialize globals.
static CMD_MAP: Lazy<HashMap<String, CmdDef>> = Lazy::new(|| build_cmd_map());

// very cheap ASCII lowercasing.
fn lowercase<'a>(s: &str, buf: &'a mut [u8]) -> &'a str {
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
    unsafe { str::from_utf8_unchecked(&buf[..idx]) }
}

fn put_str(bytes_mut: &mut BytesMut, s: impl AsRef<str>) {
    bytes_mut.put(s.as_ref().as_bytes())
}

/// NNTP Command parser.
pub struct CmdParser {
    caps:       usize,
    cmd_map:    &'static HashMap<String, CmdDef>,
    keyword_re: Regex,
}

impl CmdParser {
    /// Return a fresh Cmd.
    pub fn new() -> CmdParser {
        CmdParser {
            caps:       Capb::Always as usize,
            cmd_map:    &*CMD_MAP,
            keyword_re: Regex::new("^[A-Za-z][A-Za-z0-9.-]{2,}$").unwrap(),
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

    /// is this a valid keyword.
    pub fn is_keyword(&self, kw: &str) -> bool {
        self.keyword_re.is_match(kw)
    }

    /// Command parsing and checking.
    pub fn parse<'a>(&self, data: &'a str) -> Result<(Cmd, Vec<&'a str>), &'static str> {
        let mut args: Vec<_> = data.split_whitespace().collect();
        if args.len() == 0 {
            return Err("501 Syntax error");
        }

        // lowercase, then match command.
        let mut buf = [0u8; 32];
        let arg0 = lowercase(args[0], &mut buf[..]);
        let mut cmd = match self.cmd_map.get(arg0) {
            Some(cmd) => cmd,
            None => return Err("500 Unknown command"),
        };
        args.remove(0);

        if cmd.has_subcmd {
            if args.len() < cmd.min {
                if (self.caps & (cmd.cap as usize)) == 0 {
                    return Err("500 Unknown command");
                } else {
                    return Err("501 Missing argument");
                }
            }
            if args.len() > 0 {
                // match (command, keyword)
                let mut kw = format!("{} {}", arg0, args[0]);
                kw.as_mut_str().make_ascii_lowercase();
                cmd = match self.cmd_map.get(&kw) {
                    Some(cmd) => cmd,
                    None => return Err("501 Unknown subcommand"),
                };
                args.remove(0);
            }
        }

        // AUTHINFO PASS is special.
        if cmd.cmd == Cmd::Authinfo_Pass {
            args.truncate(0);
            args.extend(data.splitn(3, |c| c == ' ' || c == '\t').skip(2));
        }

        // Do we allow this command?
        if (self.caps & (cmd.cap as usize)) == 0 {
            return Err("500 Unknown command");
        }

        // check number of args
        let c = args.len();
        if c < cmd.min {
            return Err("501 Missing argument(s)");
        }
        if c > cmd.max {
            return Err("501 Too many arguments");
        }

        Ok((cmd.cmd, args))
    }

    pub fn help(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(4096);
        put_str(&mut out, "100 Legal commands\r\n");

        let mut cmds: Vec<&CmdDef> = self.cmd_map.values().collect();
        cmds.sort_unstable_by(|a, b| a.name.cmp(b.name));

        for cmd in cmds {
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
            put_str(&mut out, format!("  {} {}\r\n", cmd.name, s));
        }
        put_str(&mut out, ".\r\n");
        out.freeze()
    }

    pub fn capabilities(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(1024);
        put_str(&mut out, "101 Capability list:\r\n");
        put_str(&mut out, "VERSION: 2\r\n");
        put_str(&mut out, "IMPLEMENTATION: NNTP-RS 0.1\r\n");
        if (self.caps & Capb::Authinfo as usize) > 0 {
            put_str(&mut out, "AUTHINFO\r\n");
        }
        if (self.caps & Capb::Hdr as usize) > 0 {
            put_str(&mut out, "HDR\r\n");
        }
        if (self.caps & Capb::Ihave as usize) > 0 {
            put_str(&mut out, "IHAVE\r\n");
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
            put_str(&mut out, format!("LIST {}\r\n", v.join(" ")));
        }

        if (self.caps & Capb::Reader as usize) > 0 {
            put_str(&mut out, "MODE-READER\r\n");
        }
        if (self.caps & Capb::NewNews as usize) > 0 {
            put_str(&mut out, "NEWNEWS\r\n");
        }
        if (self.caps & Capb::Over as usize) > 0 {
            put_str(&mut out, "OVER\r\n");
        }
        if (self.caps & Capb::Post as usize) > 0 {
            put_str(&mut out, "POST\r\n");
        }
        if (self.caps & Capb::Reader as usize) > 0 {
            put_str(&mut out, "READER\r\n");
        }
        if (self.caps & Capb::Sasl as usize) > 0 {
            put_str(&mut out, "SASL\r\n");
        }
        if (self.caps & Capb::StartTls as usize) > 0 {
            put_str(&mut out, "STARTTLS\r\n");
        }
        if (self.caps & Capb::Streaming as usize) > 0 {
            put_str(&mut out, "STREAMING\r\n");
        }
        put_str(&mut out, ".\r\n");
        out.freeze()
    }
}

/// Is this a valid message-id?
pub fn is_msgid(m: &str) -> bool {
    if !m.starts_with("<") {
        return false;
    }
    match m.find('>') {
        Some(x) if x > 1 && x == m.len() - 1 => true,
        _ => false,
    }
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
    let r: Vec<_> = m.split('-').collect();
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
