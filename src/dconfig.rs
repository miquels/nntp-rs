//! Read diablo configuration files.
//!
//! Configuration file formats supported are:
//!
//! - dnewsfeeds, diablo.hosts -> read into a `NewsFeeds` struct.
//! - dspool.ctl -> read into a `SpoolCfg` struct.
//!
use std::collections::HashMap;
use std::default::Default;
use std::fmt::{Debug, Display};
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::str::FromStr;

use smartstring::alias::String as SmartString;

use crate::arttype::ArtType;
use crate::newsfeeds::*;
use crate::spool::SpoolCfg;
use crate::util::{self, HashFeed, WildMatList};

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
    label:  String,
    groups: WildMatList,
}

/// Read a NewsFeeds from a "dnewsfeeds" file.
pub fn read_dnewsfeeds(name: &str) -> io::Result<NewsFeeds> {
    let file = File::open(name).map_err(|e| io::Error::new(e.kind(), format!("{}: {}", name, e)))?;
    let file = BufReader::new(file);
    let mut line_no = 0;

    let mut feeds = NewsFeeds::new();
    let mut nf = NewsPeer::new();
    let mut global = NewsPeer::new();
    let mut gdef = GroupDef::default();
    let mut state = DNState::Init;
    let mut info = String::new();

    let mut groupdef_map: HashMap<String, ()> = HashMap::new();

    for line in file.lines() {
        line_no += 1;
        info = format!("{}[{}]", name, line_no);
        let line = line.map_err(|e| invalid_data!("{}: {}", info, e))?;
        let words: Vec<_> = line
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
                            "ISPAM" | "ESPAM" => {
                                // ISPAM/ESPAM don't inherit GLOBAL either,
                                // besides, we ignore them for now.
                                nf = NewsPeer::new();
                                log::warn!("{}: ignoring {} label", info, words[1]);
                            },
                            _ => {},
                        }
                        state = DNState::Label;
                        nf.label = SmartString::from(words[1]);
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
                    _ => {
                        Err(invalid_data!(
                            "{}: unknown keyword {} (expect label/groupdef)",
                            info,
                            words[0]
                        ))?
                    },
                }
            },
            DNState::Label => {
                if words[0] == "end" {
                    state = DNState::Init;
                    match nf.label.as_str() {
                        "GLOBAL" => global = nf,
                        "IFILTER" => feeds.infilter = Some(nf),
                        "ISPAM" | "ESPAM" => {},
                        _ => {
                            feeds.peer_map.insert(nf.label.clone(), feeds.peers.len());
                            feeds.peers.push(nf);
                        },
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
        let words: Vec<_> = line
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
            None => log::warn!("{}: label {} not found in dnewsfeeds", info, words[1]),
            Some(idx) => {
                nf.peers[*idx].inhost.push(words[0].to_string());
            },
        }
    }
    Ok(())
}

/// Read SpoolCfg from a "dspool.ctl" file.
pub fn read_dspool_ctl(name: &str, spool_cfg: &mut SpoolCfg) -> io::Result<()> {
    let cfg: SpoolCfg = curlyconf::Builder::new()
        .mode(curlyconf::Mode::Diablo)
        .from_file(name)?;
    spool_cfg.spool.extend(cfg.spool.into_iter());
    spool_cfg.spoolgroup.extend(cfg.spoolgroup.into_iter());
    spool_cfg.groupmap.0.extend(cfg.groupmap.0.into_iter());
    Ok(())
}

// Set one item of a NewsPeer.
#[rustfmt::skip]
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
        "deldist" => peer.distributions.push("!".to_string() + parse_string(words)?.as_str()),

        //"groups" => parse_num_list(&mut peer.groups.patterns, words, ",")?,
        "addgroup" => peer.groups.push(parse_group(words)?),
        "delgroup" => peer.groups.push("!".to_string() + parse_group(words)?.as_str()),
        "delgroupany" => peer.groups.push("@".to_string() + parse_group(words)?.as_str()),
        "groupref" => peer.groups.push("=".to_string() + parse_group(words)?.as_str()),

        "outhost" => peer.outhost = parse_string(words)?,
        "hostname" => peer.outhost = parse_string(words)?,
        "bindaddress" => peer.bindaddress = Some(parse_ipaddr(words)?),
        "port" => peer.port = parse_num::<u16>(words)?,
        "maxparallel" => peer.maxparallel = parse_num::<u32>(words)?,
        "maxstream" => peer.maxstream = parse_num::<u32>(words)?,
        "nobatch" => peer.nobatch = parse_bool(words)?,
        "maxqueue" => peer.maxqueue = parse_num::<u32>(words)?,
        "maxqueuefile" => peer.maxqueue = parse_num::<u32>(words)?,
        "headfeed" => peer.send_headfeed = parse_bool(words)?,
        "send-headfeed" => peer.send_headfeed = parse_bool(words)?,
        "accept-headfeed" => peer.accept_headfeed = parse_bool(words)?,
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
        "genlines"|
        "setqos"|
        "settos" => log::warn!("{}: unsupported keyword, ignoring", words[0]),

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
        "delgroup" => gdef.groups.push("!".to_string() + parse_string(words)?.as_str()),
        "delgroupany" => gdef.groups.push("@".to_string() + parse_string(words)?.as_str()),
        "groupref" => gdef.groups.push("=".to_string() + parse_string(words)?.as_str()),
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
        },
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
    if words[1].starts_with('!') ||
        words[1].starts_with('=') ||
        words[1].starts_with('@') ||
        words[1].contains(',')
    {
        return Err(invalid_data!("{}: invalid group name {}", words[0], words[1]));
    }

    Ok(words[1].to_string())
}

// parse a single number.
fn parse_num<T>(words: &[&str]) -> io::Result<T>
where
    T: FromStr,
    T: Default,
    T: Debug,
    <T as FromStr>::Err: Debug + Display,
{
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    words[1]
        .parse::<T>()
        .map_err(|e| invalid_data!("{} {}: {}", words[0], words[1], e))
}

// parse a size (K/KB/KiB/M/MB/MiB etc)
fn parse_size(words: &[&str]) -> io::Result<u64> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    util::parse_size(words[1])
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
        "y" | "yes" | "t" | "true" | "on" | "1" => Ok(true),
        "n" | "no" | "f" | "false" | "off" | "0" => Ok(false),
        _ => Err(invalid_data!("{}: not a boolean value {}", words[0], words[1])),
    }
}

// parse an IP address.
fn parse_ipaddr(words: &[&str]) -> io::Result<std::net::IpAddr> {
    if words.len() != 2 {
        return Err(invalid_data!("{}: expected 1 argument", words[0]));
    }
    std::net::IpAddr::from_str(words[1]).map_err(|e| invalid_data!("{}: {} {}", words[0], e, words[1]))
}

// parse a list of words, seperated by a character from "sep".
fn parse_list(list: &mut Vec<String>, words: &[&str], sep: &str) -> io::Result<()> {
    if words.len() < 2 {
        return Err(invalid_data!("{}: expected at least 1 argument", words[0]));
    }
    for w in words.into_iter().skip(1) {
        let _: Vec<_> = w
            .split(|s: char| sep.contains(s))
            .filter(|s| !s.is_empty())
            .map(|s| {
                let s = s.trim();
                list.push(s.to_string());
                s
            })
            .collect();
    }
    Ok(())
}

// parse a list of article types.
fn parse_arttype(arttypes: &mut Vec<ArtType>, words: &[&str]) -> io::Result<()> {
    let mut list = Vec::new();
    parse_list(&mut list, words, " \t:,")?;
    for w in &list {
        let a = w
            .parse()
            .map_err(|_| invalid_data!("{}: unknown article type", w))?;
        arttypes.push(a);
    }
    Ok(())
}

