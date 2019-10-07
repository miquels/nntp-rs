//! Read diablo configuration files.
//!
//! Configuration file formats supported are:
//!
//! - dnewsfeeds, diablo.hosts -> read into a `NewsFeeds` struct.
//! - dspool.ctl -> read into a `SpoolCfg` struct.
//!
use std::collections::HashMap;
use std::default::Default;
use std::fmt::{Debug,Display};
use std::fs::{self,File};
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::str::FromStr;
use std::time::Duration;

use crate::arttype::ArtType;
use crate::newsfeeds::*;
use crate::util::{self, HashFeed, WildMatList};
use crate::spool::{GroupMap,MetaSpool,SpoolCfg,SpoolDef};

macro_rules! invalid_data {
    ($($expr:expr),*) => (
        io::Error::new(io::ErrorKind::InvalidData, format!($($expr),*))
    )
}

// read_dnewsfeeds state.
enum DNState {
    Init,
    Label,
    GroupDef,
}

#[derive(Default)]
struct GroupDef {
    label:      String,
    groups:     WildMatList,
}

/// Read a NewsFeeds from a "dnewsfeeds" file.
pub fn read_dnewsfeeds(name: &str) -> io::Result<NewsFeeds> {

    let file = File::open(name)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", name, e)))?;
    let file = BufReader::new(file);
    let mut line_no = 0;

    let mut feeds = NewsFeeds::new();
    let mut nf = NewsPeer::new();
    let mut global = NewsPeer::new();
    let mut gdef = GroupDef::default();
    let mut state = DNState::Init;
    let mut info = String::new();

    let mut groupdef_map : HashMap<String, ()> = HashMap::new();

    for line in file.lines() {
        line_no += 1;
        info = format!("{}[{}]", name, line_no);
        let line = line.map_err(|e| invalid_data!("{}: {}", info, e))?;
        let words : Vec<_> = line
            .split_whitespace()
            .take_while(|w| !w.starts_with("#"))
            .collect();
	        if words.len() == 0 {
            continue;
        }
        match state {
            DNState::Init => {
                match words[0] {
                    "label" => {
                        if words.len() != 2 {
                            Err(invalid_data!("{}: {}: expected one argument", info, words[0]))?;
                        }
                        if feeds.peer_map.contains_key(words[0]) {
                            Err(invalid_data!("{}: {}: duplicate label", info, words[1]))?;
                        }
                        match words[1] {
                            "GLOBAL" => {
                                // slight concession - in the diablo implementation it
                                // doesn't matter if this is first or not, since defaults
                                // are merged in after the fact. however our implementation
                                // just uses GLOBAL as the default, so it must come first.
                                if feeds.peers.len() > 0 {
                                    Err(invalid_data!("{}: GLOBAL: must be first label", info))?;
                                }
                            },
                            "IFILTER" => {
                                // IFILTER does not inherit GLOBAL
                                nf = NewsPeer::new();
                            },
                            "ISPAM"|"ESPAM" => {
                                // ISPAM/ESPAM don't inherit GLOBAL either,
                                // besides, we ignore them for now.
                                nf = NewsPeer::new();
                                warn!("{}: ignoring {} label", info, words[1]);
                            },
                            _ => {},
                        }
                        state = DNState::Label;
                        nf.label = words[1].to_string();
                    },
                    "groupdef" => {
                        if words.len() != 2 {
                            Err(invalid_data!("{}: {}: expected one argument", info, words[0]))?;
                        }
                        if groupdef_map.contains_key(words[0]) {
                            Err(invalid_data!("{}: {}: duplicate groupdef", info, words[1]))?;
                        }
                        state = DNState::GroupDef;
                        gdef.label = words[1].to_string();
                    },
                    _ => Err(invalid_data!("{}: unknown keyword {} (expect label/groupdef)",
                                    info, words[0]))?,
                }
            },
            DNState::Label => {
                if words[0] == "end" {
                    state = DNState::Init;
                    match nf.label.as_str() {
                        "GLOBAL" => global = nf,
                        "IFILTER" => feeds.infilter = Some(nf),
                        "ISPAM"|"ESPAM" => {},
                        _ => {
                            feeds.peer_map.insert(nf.label.clone(), feeds.peers.len());
                            feeds.peers.push(nf);
                        }
                    }
                    nf = global.clone();
                } else {
                    set_newspeer_item(&mut nf, &words).map_err(|e| invalid_data!("{}: {}", info, e))?;
                }
            },
            DNState::GroupDef => {
                if words[0] == "end" {
                    state = DNState::Init;
                    let label = gdef.label.clone();
                    gdef.groups.set_name(&label);
                    groupdef_map.insert(label, ());
                    feeds.groupdefs.push(gdef.groups);
                    gdef = GroupDef::default();
                } else {
                    set_groupdef_item(&mut gdef, &words).map_err(|e| invalid_data!("{}: {}", info, e))?;
                }
            },
        }
    }

    match state {
        DNState::Init => {},
        DNState::Label => Err(invalid_data!("{}: unexpected EOF in label {}", info, nf.label))?,
        DNState::GroupDef => Err(invalid_data!("{}: unexpected EOF in groupdef {}", info, nf.label))?,
    }

    feeds.resolve_references();

    Ok(feeds)
}

/// Reads a "diablo.hosts" file. To be called after the "dnewsfeeds" file
/// has been read. If the file is not found, that's OK - it's optional.
pub fn read_diablo_hosts(nf: &mut NewsFeeds, name: &str) -> io::Result<()> {

    let file = match File::open(name) {
        Ok(f) => f,
        Err(e) => {
            // The file is optional, so if it's not found that is not OK.
            // We're paranoid and do check for a dangling symlink, if
            // that is the case, do report an error.
            if e.kind() == io::ErrorKind::NotFound {
                if !fs::symlink_metadata(name).is_ok() {
                    return Ok(());
                }
            }
            return Err(io::Error::new(e.kind(), format!("{}: {}", name, e)));
        },
    };
    let file = BufReader::new(file);
    let mut line_no = 0;

    let mut info;

    for line in file.lines() {
        line_no += 1;
        info = format!("{}[{}]", name, line_no);
        let line = line.map_err(|e| invalid_data!("{}: {}", info, e))?;
        let words : Vec<_> = line
            .split_whitespace()
            .take_while(|w| !w.starts_with("#"))
            .collect();
	        if words.len() == 0 {
            continue;
        }
        if words.len() == 1 {
            Err(invalid_data!("{}: {}: missing label", info, words[0]))?;
        }
        if words.len() > 2 {
            Err(invalid_data!("{}: {}: data after label", info, words[0]))?;
        }
        match nf.peer_map.get(words[1]) {
            None => warn!("{}: label {} not found in dnewsfeeds", info, words[1]),
            Some(idx) => {
                nf.peers[*idx].inhost.push(words[0].to_string());
            },
        }
    }
    Ok(())
}

// read_dspool_ctl state.
enum DCState {
    Init,
    Spool,
    MetaSpool,
}

/// Read SpoolCfg from a "dspool.ctl" file.
pub fn read_dspool_ctl(name: &str, spoolpath: &str, spool_cfg: &mut SpoolCfg) -> io::Result<()> {

    let file = File::open(name)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", name, e)))?;
    let file = BufReader::new(file);
    let mut line_no = 0;

    let mut spool = SpoolDef::default();
    let mut metaspool = MetaSpool::default();

    let mut state = DCState::Init;
    let mut info = String::new();

    for line in file.lines() {

        line_no += 1;
        info = format!("{}[{}]", name, line_no);

        let line = line.map_err(|e| invalid_data!("{}: {}", info, e))?;
        let words : Vec<_> = line
            .split_whitespace()
            .take_while(|w| !w.starts_with("#"))
            .collect();
	    if words.len() == 0 {
            continue;
        }

        match state {
            DCState::Init => {
                match words[0] {
                    "spool" => {
                        if words.len() != 2 {
                            Err(invalid_data!("{}: {}: expected one argument", info, words[0]))?;
                        }
                        let n = match parse_num::<u8>(&words) {
                            Ok(n) => n,
                            Err(e) => return Err(invalid_data!("{}: {}", info, e)),
                        };
                        spool.spool_no = n;
                        spool.backend = "diablo".to_string();
                        let num = n.to_string();
                        if spool_cfg.spool.contains_key(&num) {
                            Err(invalid_data!("{}: {}: duplicate spool", info, words[1]))?;
                        }
                        state = DCState::Spool;
                    },
                    "metaspool" => {
                        if words.len() != 2 {
                            Err(invalid_data!("{}: {}: expected one argument", info, words[0]))?;
                        }
                        metaspool.name = words[1].to_string();
                        state = DCState::MetaSpool;
                    },
                    "expire" => {
                        if words.len() != 3 {
                            Err(invalid_data!("{}: {}: expected two arguments", info, words[0]))?;
                        }
                        spool_cfg.groupmap.push(GroupMap{
                            groups:     words[1].to_string(),
                            spoolgroup: words[2].to_string(),
                        });
                    },
                    _ => Err(invalid_data!("{}: unknown keyword {} (expect spool/metaspool/expire)",
                                    info, words[0]))?,
                }
            },
            DCState::Spool => {
                if words[0] == "end" {
                    if spool.path.is_empty() {
                        spool.path = format!("P.{:02}", spool.spool_no);
                    }
                    spool_cfg.spool.insert(spool.spool_no.to_string(), spool);
                    spool = SpoolDef::default();
                    state = DCState::Init;
                } else {
                    set_spooldef_item(&mut spool, spoolpath, &words).map_err(|e| invalid_data!("{}: {}", info, e))?;
                }
            },
            DCState::MetaSpool => {
                match words[0] {
                    "end" => {
                        spool_cfg.spoolgroup.push(metaspool);
                        metaspool = MetaSpool::default();
                        state = DCState::Init;
                    },
                    "addgroup" => {
                        if words.len() != 2 {
                            Err(invalid_data!("{}: {}: expected one argument", info, words[0]))?;
                        }
                        spool_cfg.groupmap.push(GroupMap{
                            groups:     words[1].to_string(),
                            spoolgroup: metaspool.name.clone(),
                        });
                    },
                    _ => {
                        set_metaspool_item(&mut metaspool, &words)
                            .map_err(|e| invalid_data!("{}: {}", info, e))?;
                    },
                }
            },
        }
    }

    match state {
        DCState::Init => {},
        DCState::Spool => Err(invalid_data!("{}: unexpected EOF in spool {}", info, spool.spool_no))?,
        DCState::MetaSpool => Err(invalid_data!("{}: unexpected EOF in metaspool {}", info, metaspool.name))?,
    }

    Ok(())
}

// Set one item of a NewsPeer.
fn set_newspeer_item(peer: &mut NewsPeer, words: &[&str]) -> io::Result<()> {
    match words[0] {
        "pathalias" => peer.pathalias.push(parse_string(words)?),
        "alias" => peer.pathalias.push(parse_string(words)?),

        "host" => {
            let host = parse_string(words)?;
            peer.pathalias.push(host.clone());
            peer.inhost.push(host.clone());
            peer.outhost = host;
        },

        "inhost" => peer.inhost.push(parse_string(words)?),
        "maxconnect" => peer.maxconnect = parse_num::<u32>(words)?,
        "readonly" => peer.readonly = parse_bool(words)?,

        "filter" => peer.filter.push(parse_group(words)?),
        "nofilter" => peer.filter.push(&format!("!{}", parse_group(words)?)),
        "nomismatch" => peer.nomismatch = parse_bool(words)?,
        "precomreject" => peer.precomreject = parse_bool(words)?,

        "maxcross" => peer.maxcross = parse_num::<u32>(words)?,
        "maxpath" => peer.maxpath = parse_num::<u32>(words)?,
        "maxsize" => peer.maxsize = parse_size(words)?,
        "minsize" => peer.maxsize = parse_size(words)?,
        "mincross" => peer.mincross = parse_num::<u32>(words)?,
        "minpath" => peer.minpath = parse_num::<u32>(words)?,
        "arttypes" => parse_arttype(&mut peer.arttypes, words)?,
        "hashfeed" => peer.hashfeed = parse_hashfeed(words)?,
        "requiregroup" => peer.requiregroups.push(parse_group(words)?),

        "distributions" => parse_list(&mut peer.distributions, words, ",")?,
        "adddist" => peer.distributions.push(parse_string(words)?),
        "deldist" => peer.distributions.push("!".to_string() + &parse_string(words)?),

        //"groups" => parse_num_list(&mut peer.groups.patterns, words, ",")?,
        "addgroup" => peer.groups.push(parse_group(words)?),
        "delgroup" => peer.groups.push("!".to_string() + &parse_group(words)?),
        "delgroupany" => peer.groups.push("@".to_string() + &parse_group(words)?),
        "groupref" => peer.groups.push("=".to_string() + &parse_group(words)?),

        "outhost" => peer.outhost = parse_string(words)?,
        "hostname" => peer.outhost = parse_string(words)?,
        "bindaddress" => peer.bindaddress = parse_string(words)?,
        "port" => peer.port = parse_num::<u16>(words)?,
        "maxparallel" => peer.maxparallel = parse_num::<u32>(words)?,
        "maxstream" => peer.maxstream = parse_num::<u32>(words)?,
        "nobatch" => peer.nobatch = parse_bool(words)?,
        "maxqueue" => peer.maxqueue = parse_num::<u32>(words)?,
        "maxqueuefile" => peer.maxqueue = parse_num::<u32>(words)?,
        "headfeed" => peer.headfeed = parse_bool(words)?,
        "genlines" => peer.genlines = parse_bool(words)?,
        "preservebytes" => peer.preservebytes = parse_bool(words)?,

        // we do not support this, fatal.
        "onlyspam"|
        "offerfilter"|
        "noofferfilter" => Err(invalid_data!("{}: unsupported keyword", words[0]))?,

        // we do not support this, irrelevant, ignore.
        "transmitbuf"|
        "receivebuf"|
        "realtime"|
        "priority"|
        "incomingpriority"|
        "articlestat"|
        "notify"|
        "rtflush"|
        "nortflush"|
        "feederrxbuf" => {},

        // we do not support this, warn and ignore.
        "compress"|
        "throttle_delay"|
        "throttle_lines"|
        "allow_readonly"|
        "check"|
        "stream"|
        "nospam"|
        "whereis"|
        "addspam"|
        "delspam"|
        "spamalias"|
        "startdelay"|
        "logarts"|
        "hours"|
        "queueskip"|
        "delayfeed"|
        "delayinfeed"|
        "setqos"|
        "settos" => warn!("{}: unsupported keyword, ignoring", words[0]),

        // actually don't know.
        _ => Err(invalid_data!("{}: unrecognized keyword", words[0]))?,
    }
    Ok(())
}

// Set one item of a Groupdef.
fn set_groupdef_item(gdef: &mut GroupDef, words: &[&str]) -> io::Result<()> {
    match words[0] {
        // "groups" => parse_list(&mut gdef.groups, words, ",")?,
        "addgroup" => gdef.groups.push(parse_string(words)?),
        "delgroup" => gdef.groups.push("!".to_string() + &parse_string(words)?),
        "delgroupany" => gdef.groups.push("@".to_string() + &parse_string(words)?),
        "groupref" => gdef.groups.push("=".to_string() + &parse_string(words)?),
        _ => Err(invalid_data!("{}: unrecognized keyword", words[0]))?,
    }
    Ok(())
}

// Set one item of a SpoolDef
fn set_spooldef_item(spool: &mut SpoolDef, spoolpath: &str, words: &[&str]) -> io::Result<()> {
    match words[0] {
        "backend" => spool.backend = parse_string(words)?,
        "path" => spool.path = parse_spoolpath(spoolpath, words)?,
        "minfree" => spool.minfree = parse_size(words)?,
        "maxsize" => spool.maxsize = parse_size(words)?,
        "keeptime" => spool.keeptime = parse_duration(words)?,
        "weight" => spool.weight = parse_num::<u32>(words)?,

        // we do not support this, irrelevant, ignore.
        "expiremethod" => {},

        // we do not support this, warn and ignore.
        "minfreefiles"|
        "compresslvl" => warn!("{}: unsupported keyword, ignoring", words[0]),

        // we do not support this, error and return.
        "spooldirs" => Err(invalid_data!("{}: unsupported keyword", words[0]))?,

        // actually don't know.
        _ => Err(invalid_data!("{}: unrecognized keyword", words[0]))?,
    }
    Ok(())
}

// Set one item of a MetaSpool
fn set_metaspool_item(ms: &mut MetaSpool, words: &[&str]) -> io::Result<()> {
    match words[0] {
        "arttypes" => parse_arttype(&mut ms.v_arttypes, words)?,
        "dontstore" => ms.dontstore = parse_bool(words)?,
        "rejectarts" => ms.rejectarts = parse_bool(words)?,

        "hashfeed" => ms.hashfeed = parse_hashfeed(words)?,
        "maxsize" => ms.maxsize = parse_size(words)?,
        "maxcross" => ms.maxcross = parse_num::<u32>(words)?,
        "reallocint" => ms.reallocint = parse_duration(words)?,

        "spool" => parse_num_list(&mut ms.spool, words, ",")?,

        "allocstrat" => {
            let s = parse_string(words)?;
            match s.as_str() {
                "space" => Err(invalid_data!("{} space: not supported (only weighted)", words[0]))?,
                "sequential"|
                "single" => warn!("{} {}: ignoring, always using \"weighted\"", words[0], s),
                "weighted" => {},
                _ => Err(invalid_data!("{} {}: unknown allocstrat", words[0], s))?,
            }
        },

        // we do not support this, fatal.
        "label" => Err(invalid_data!("{}: unsupported keyword", words[0]))?,

        // actually don't know.
        _ => Err(invalid_data!("{}: unrecognized keyword", words[0]))?,
    }
    Ok(())
}

//
// Below are a bunch of parsing helpers for the set_STRUCT_item functions.
//

// parse a hashfeed.
fn parse_hashfeed(words: &[&str]) -> io::Result<HashFeed> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    match HashFeed::new(words[1]) {
        Ok(hf) => Ok(hf),
        Err(e) => {
            return Err(invalid_data!("{} {}: {}", words[0], words[1], e));
        }
    }
}

// parse a single word.
fn parse_string(words: &[&str]) -> io::Result<String> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    Ok(words[1].to_string())
}

// parse a single word as a newsgroup
fn parse_group(words: &[&str]) -> io::Result<String> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    if words[1].starts_with('!') || words[1].starts_with('=') || words[1].starts_with('@') || words[1].contains(',') {
        return Err(invalid_data!("{}: invalid group name {}", words[0], words[1]));
    }

    Ok(words[1].to_string())
}

// parse a single word,expand.
fn parse_spoolpath(spoolpath: &str, words: &[&str]) -> io::Result<String> {
    let mut p = parse_string(words)?;
    if !p.starts_with("/") {
        p = format!("{}/{}", spoolpath, p);
    }
    Ok(p)
}

// parse a single number.
fn parse_num<T>(words: &[&str]) -> io::Result<T>
    where T: FromStr,
          T: Default,
          T: Debug,
          <T as FromStr>::Err: Debug + Display {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    words[1].parse::<T>().map_err(|e| invalid_data!("{} {}: {}", words[0], words[1], e))
}

// parse a size (K/KB/KiB/M/MB/MiB etc)
fn parse_size(words: &[&str]) -> io::Result<u64> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    util::parse_size(words[1])
}

// parse a Duration
fn parse_duration(words: &[&str]) -> io::Result<Duration> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    util::parse_duration(words[1])
}

// parse a bool.
fn parse_bool(words: &[&str]) -> io::Result<bool> {
    if words.len() == 1 {
        return Ok(true);
    }
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected max 1 argument", words[0]));
    }
    let b = words[1].to_lowercase();
    match b.as_str() {
        "y"|"yes"|"t"|"true"|"on"|"1" => Ok(true),
        "n"|"no"|"f"|"false"|"off"|"0" => Ok(false),
        _ => Err(invalid_data!("{}: not a boolean value {}", words[0], words[1])),
    }
}

// parse a list of words, seperated by a character from "sep".
fn parse_list(list: &mut Vec<String>, words: &[&str], sep: &str) -> io::Result<()> {
    if words.len() < 2 {
        return Err(invalid_data!("{}: expected at least 1 argument", words[0]));
    }
    for w in words.into_iter().skip(1) {
        let _ : Vec<_> = w
            .split(|s: char| sep.contains(s))
            .filter(|s| !s.is_empty())
            .map(|s| { let s = s.trim(); list.push(s.to_string()); s } )
            .collect();
    }
    Ok(())
}

// parse a list of article types.
fn parse_arttype(arttypes: &mut Vec<ArtType>, words: &[&str]) -> io::Result<()> {
    let mut list = Vec::new();
    parse_list(&mut list, words, " \t:,")?;
    for w in &list {
        let a = w.parse().map_err(|_| invalid_data!("{}: unknown article type", w))?;
        arttypes.push(a);
    }
    Ok(())
}

// parse a list of numbers, seperated by a character from "sep".
fn parse_num_list<T>(list: &mut Vec<T>, words: &[&str], sep: &str) -> io::Result<()>
    where T: FromStr,
          T: Default,
          T: Debug,
          <T as FromStr>::Err: Debug + Display {
    let mut wl = Vec::new();
    parse_list(&mut wl, words, sep)?;
    for w in wl.into_iter() {
        let n = w.parse::<T>().map_err(|e| invalid_data!("{} {}: {}", words[0], w, e))?;
        list.push(n);
    }
    Ok(())
}

