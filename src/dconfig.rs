//! Read diablo configuration files.
//!
//! Configuration file formats supported are:
//!
//! - dnewsfeeds, diablo.hosts -> read into a `NewsFeeds` struct.
//! - dspool.ctl -> read into a `SpoolCfg` struct.
//!
//! The code here also check for compatibility. Some settings are
//! ignored, some are ignored but do generate a warning, and some
//! settings are not implemented and generate an error.
//!
use std::collections::HashSet;
use std::default::Default;
use std::fmt::{Debug, Display};
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::str::FromStr;

use serde::{de::Deserializer, de::MapAccess, de::Visitor};
use smartstring::alias::String as SmartString;

use crate::newsfeeds::*;
use crate::spool::{SpoolCfg, GroupMap, GroupMapEntry};
use crate::util::WildMatList;

// A type where a "dnewsfeeds" file can deserialize into.
#[derive(Default, Debug, Deserialize)]
struct DNewsFeeds {
    #[serde(rename = "label")]
    pub label:      Label,
    pub groupdef:   Vec<GroupDef>,
}

// Convert the DnewsFeeds we just read into a NewsFeeds.
impl From<DNewsFeeds> for NewsFeeds {
    fn from(mut dnf: DNewsFeeds) -> NewsFeeds {
        let mut nf = NewsFeeds::new();
        nf.infilter = dnf.ifilter;
        nf.peers = dnf.label.peers;
        for idx in 0 .. nf.peers.len() {
            nf.peer_map.insert(nf.peers[idx].label.as_str().into(), idx);

            let peer = &mut nf.peers[idx];
            peer.index = idx;
            peer.accept_headfeed = true;
            if peer.host != "" {
                if peer.outhost == "" {
                    peer.outhost = peer.host.clone();
                }
                if !peer.pathalias.contains(&peer.host) {
                    let h = peer.host.clone();
                    peer.pathalias.push(h);
                }
                if !peer.inhost.contains(&peer.host) {
                    let h = peer.host.clone();
                    peer.inhost.push(h);
                }
            }
        }
        for gd in dnf.groups.into_iter() {
            let mut groups = gd.groups;
            groups.name = gd.label;
            nf.groupdefs.push(groups);
        }
        nf
    }
}

// "groupdef" item in a dnewsfeeds file.
#[derive(Default, Debug, Deserialize)]
struct GroupDef {
    #[serde(rename = "__label__")]
    label:  String,
    groups: WildMatList,
}

// A bunch of labels.
//
// We use our own map-like deserializer so that we can handle
// labels like IFILTER/GLOBAL/ISPAM/ESPAM differently.
#[derive(Default)]
struct Label {
    global:     Option<NewsPeer>,
    ifilter:    Option<NewsPeer>,
    ispam:      Option<NewsPeer>,
    espam:      Option<NewsPeer>,
    peers:      Vec<NewsPeer>,
}

impl<'de> Deserialize<'de> for Label {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LabelVisitor;

        impl<'de> Visitor<'de> for LabelVisitor {
            type Value = Label;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a dnewsfeeds entry")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut this = Label::default();

                // We assume that we do two passes over the dnewsfeeds file:
                // 1. to read the GLOBAL label and discard the rest
                // 2. to read the other labels while using GLOBAL as a default.
                let first = get_default_newspeer().label == "";
                while let Some(label) = map.next_key::<String>()? {
                    let mut peer = map.next_value()?;
                    if first {
                        if label == "GLOBAL" {
                            set_default_newspeer(peer);
                        }
                        continue;
                    }
                    match label.as_str() {
                        "GLOBAL" => {},
                        "IFILTER" => this.ifilter = Some(peer),
                        "ISPAM" => this.ispam = Some(peer),
                        "ESPAM" => this.espam = Some(peer),
                        _ => this.peers.push(peer),
                    }
                }
                if first {
                    // Make sure that the default newspeer is initialized, even
                    // if there was no GLOBAL entry.
                    let mut global = get_default_newspeer();
                    global.label = "GLOBAL".to_string();
                    set_default_newspeer(global);
                }
                Ok(this)
            }
        }

        deserializer.deserialize_map(LabelVisitor)
    }
}

/// Read a NewsFeeds from a "dnewsfeeds" file.
pub fn read_dnewsfeeds(name: &str) -> io::Result<NewsFeeds> {

    // First, do a compat check.
    check_dnewsfeeds(name)?;

    // Now build the config reader configuration.
    let cfg_buider = curlyconf::Builder::new()
        .mode(curlyconf::Mode::Diablo)

        .alias::<NewsPeer>("nofilter", "filter")
        .alias::<NewsPeer>("addgroup", "groups")
        .alias::<NewsPeer>("delgroup", "groups")
        .alias::<NewsPeer>("delgroupany", "groups")
        .alias::<NewsPeer>("groupref", "groups")
        .alias::<NewsPeer>("alias", "pathalias")
        .alias::<NewsPeer>("adddist", "distributions")
        .alias::<NewsPeer>("deldist", "distributions")
        .alias::<NewsPeer>("hostname", "outhost")
        .alias::<NewsPeer>("headfeed", "send-headfeed")

        .alias::<GroupDef>("addgroup", "groups")
        .alias::<GroupDef>("delgroup", "groups")
        .alias::<GroupDef>("delgroupany", "groups")
        .alias::<GroupDef>("groupref", "groups")

        .ignore::<NewsPeer>("transmitbuf")
        .ignore::<NewsPeer>("receivebuf")
        .ignore::<NewsPeer>("realtime")
        .ignore::<NewsPeer>("priority")
        .ignore::<NewsPeer>("incomingpriority")
        .ignore::<NewsPeer>("articlestat")
        .ignore::<NewsPeer>("notify")
        .ignore::<NewsPeer>("rtflush")
        .ignore::<NewsPeer>("nortflush")
        .ignore::<NewsPeer>("feederrxbuf")
        .ignore::<NewsPeer>("compress")
        .ignore::<NewsPeer>("throttle_delay")
        .ignore::<NewsPeer>("throttle_lines")
        .ignore::<NewsPeer>("allow_readonly")
        .ignore::<NewsPeer>("check")
        .ignore::<NewsPeer>("stream")
        .ignore::<NewsPeer>("nospam")
        .ignore::<NewsPeer>("whereis")
        .ignore::<NewsPeer>("addspam")
        .ignore::<NewsPeer>("delspam")
        .ignore::<NewsPeer>("spamalias")
        .ignore::<NewsPeer>("startdelay")
        .ignore::<NewsPeer>("logarts")
        .ignore::<NewsPeer>("hours")
        .ignore::<NewsPeer>("queueskip")
        .ignore::<NewsPeer>("delayfeed")
        .ignore::<NewsPeer>("delayinfeed")
        .ignore::<NewsPeer>("genlines")
        .ignore::<NewsPeer>("setqos")
        .ignore::<NewsPeer>("settos");

    // Parse the config twice. The first time we only parse
    // the GLOBAL entry, the second time the GLOBAL entry is
    // used for NewsPeer defaults.
    set_default_newspeer(NewsPeer::default());
    let _ = cfg_builder.clone().from_file(name)?;
    let mut dnf: DNewsFeeds = cfg_builder.from_file(name)?;

    // And build a 'NewsFeeds' struct.
    let nf = dnf.into();
    nf.resolve_references();

    Ok(())
}

/// Read SpoolCfg from a "dspool.ctl" file.
pub fn read_dspool_ctl(name: &str, spool_cfg: &mut SpoolCfg) -> io::Result<()> {
    // First, do a compat check.
    check_spool_ctl(name)?;

    // Then actually read the config.
    let cfg: SpoolCfg = curlyconf::Builder::new()
        .mode(curlyconf::Mode::Diablo)

        .alias::<SpoolCfg>("expire", "groupmap")
        .alias::<SpoolDef>("metaspool", "spoolgroup")
        .alias::<MetaSpool>("addgroup", "groups")
        .alias::<MetaSpool>("label", "peer")

        .ignore::<SpoolDef>("expiremethod")
        .ignore::<SpoolDef>("minfreefiles")
        .ignore::<SpoolDef>("compresslvl")

        .from_file(name)?;

    spool_cfg.spool.extend(cfg.spool.into_iter());
    spool_cfg.spoolgroup.extend(cfg.spoolgroup.into_iter());
    spool_cfg.groupmap.0.extend(cfg.groupmap.0.into_iter());

    Ok(())
}

// A custom map Deserialize implementation for "GroupMap",
// so that the "expire" line works in dspool.ctl.
impl<'de> Deserialize<'de> for GroupMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct GroupMapVisitor;

        impl<'de> Visitor<'de> for GroupMapVisitor {
            type Value = GroupMap;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("wildmat and spoolgroup")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut v = Vec::new();
                while let Some(groups) = map.next_key::<String>()? {
                     let spoolgroup = map.next_value()?;
                    v.push(GroupMapEntry{groups, spoolgroup});
                }
                Ok(GroupMap(v))
            }
        }

        deserializer.deserialize_map(GroupMapVisitor)
    }
}

// deserializer for `distributions: Vec<String>` so that 'addist' and 'deldist' work.
pub(crate) fn deserialize_distributions<D>(deserializer: D) -> Result<Self, D::Error>
where
    D: Deserializer<'de>,
{
    struct DistVisitor {
        parser: curlyconf::Parser,
    }

    impl<'de> Visitor<'de> for DistVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("distributions")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut dists = Vec::new();

            while let Some(mut value) = seq.next_element::<String>()? {
                match self.parser.value_name().as_str() {
                    // addist / deldist
                    "deldist" => value.insert_str(0, "!"),
                    _ => {}
                }
                dists.push(value);
            }

            Ok(dists)
        }
    }

    let parser = deserializer.parser();
    deserializer.deserialize_seq(DistVisitor { parser })
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
        let line = line.map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
        let words: Vec<_> = line
            .split_whitespace()
            .take_while(|w| !w.starts_with("#"))
            .collect();
        if words.len() == 0 {
            continue;
        }
        if words.len() == 1 {
            Err(ioerr!(InvalidData, "{}: {}: missing label", info, words[0]))?;
        }
        if words.len() > 2 {
            Err(ioerr!(InvalidTata, "{}: {}: data after label", info, words[0]))?;
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

// check_dnewsfeeds state.
enum DNState {
    Init,
    Label,
    GroupDef,
}

/// Check a "dnewsfeeds" file.
///
/// We do this to check if there are settings that we don't support
/// anymore. Some of those settings we just ignore, others we ignore
/// with a warning, and for some settings we generate an error.
fn check_dnewsfeeds(name: &str) -> io::Result<NewsFeeds> {
    let file = File::open(name).map_err(|e| io::Error::new(e.kind(), format!("{}: {}", name, e)))?;
    let file = BufReader::new(file);

    let mut line_no = 0;
    let mut info = String::new();
    let mut state= DNState::Init;

    let mut groupdefs: HashSet<String> = HashSet::new();
    let mut labels: HashSet<String> = HashSet::new();

    for line in file.lines() {
        line_no += 1;
        info = format!("{}[{}]", name, line_no);
        let line = line.map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
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
                            Err(ioerr!(InvalidData, "{}: {}: expected one argument", info, words[0]))?;
                        }
                        if labels.contains(words[0]) {
                            Err(ioerr!(InvalidData, "{}: {}: duplicate label", info, words[1]))?;
                        }
                        match words[1] {
                            "ISPAM" | "ESPAM" => {
                                log::warn!("{}: ignoring {} label", info, words[1]);
                            },
                            _ => {},
                        }
                        state = DNState::Label;
                        label = words[1].to_string();
                    },
                    "groupdef" => {
                        if words.len() != 2 {
                            Err(ioerr!(InvalidData, "{}: {}: expected one argument", info, words[0]))?;
                        }
                        if groupdefs.contains(words[0]) {
                            Err(ioerr!(InvalidData, "{}: {}: duplicate groupdef", info, words[1]))?;
                        }
                        state = DNState::GroupDef;
                        label = words[1].to_string();
                    },
                    _ => {
                        Err(ioerr!(
                            InvalidData,
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
                    peers.insert(label.clone());
                } else {
                    check_newspeer_item(&words).map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
                }
            },
            DNState::GroupDef => {
                if words[0] == "end" {
                    state = DNState::Init;
                    groupdefs.insert(label.clone());
                } else {
                    check_groupdef_item(&words).map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
                }
            },
        }
    }

    match state {
        DNState::Init => {},
        DNState::Label => Err(ioerr!(InvalidData, "{}: unexpected EOF in label {}", info, label))?,
        DNState::GroupDef => Err(ioerr!(InvalidData, "{}: unexpected EOF in groupdef {}", info, label))?,
    }

    Ok(feeds)
}

// Check newspeer items.
#[rustfmt::skip]
fn check_newspeer_item(words: &[&str]) -> io::Result<()> {
    match words[0] {
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
        "settos" => log::warn!("{}: unsupported setting, ignoring", words[0]),

        // we do not support this, fatal.
        "onlyspam"|
        "offerfilter"|
        "noofferfilter" => Err(ioerr!(InvalidData, "{}: unsupported setting", words[0]))?,

        _ => {},
    }
    Ok(())
}

// Check one item of a Groupdef.
fn check_groupdef_item(words: &[&str]) -> io::Result<()> {
    match words[0] {
        "addgroup"|
        "delgroup"|
        "delgroupany"|
        "groupref" => {},
        _ => Err(ioerr!(InvalidData, "{}: unrecognized keyword", words[0]))?,
    }
    Ok(())
}

// read_dspool_ctl state.
enum DCState {
    Init,
    Spool,
    MetaSpool,
}

/// Check the dspool.ctl file for compat
fn check_dspool_ctl(name: &str) -> io::Result<()> {
    let file = File::open(name).map_err(|e| io::Error::new(e.kind(), format!("{}: {}", name, e)))?;
    let file = BufReader::new(file);
    let mut line_no = 0;

    let mut state = DCState::Init;
    let mut info = String::new();
    let mut spool_no = 0;
    let mut metaspool = String::new();

    for line in file.lines() {
        line_no += 1;
        info = format!("{}[{}]", name, line_no);

        let line = line.map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
        let words: Vec<_> = line
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
                            Err(ioerr!(InvalidData,"{}: {}: expected one argument", info, words[0]))?;
                        }
                        spool_no = match parse_num::<u8>(&words) {
                            Ok(n) => n,
                            Err(e) => return Err(ioerr!(InvalidData,"{}: {}", info, e)),
                        };
                        state = DCState::Spool;
                    },
                    "metaspool" => {
                        if words.len() != 2 {
                            Err(ioerr!(InvalidData,"{}: {}: expected one argument", info, words[0]))?;
                        }
                        metaspool = words[1].to_string();
                        state = DCState::MetaSpool;
                    },
                    "expire" => {
                        if words.len() != 3 {
                            Err(ioerr!(InvalidData,"{}: {}: expected two arguments", info, words[0]))?;
                        }
                    },
                    _ => {
                        Err(ioerr!(
                            InvalidData,
                            "{}: unknown keyword {} (expect spool/metaspool/expire)",
                            info,
                            words[0]
                        ))?
                    },
                }
            },
            DCState::Spool => {
                if words[0] == "end" {
                    state = DCState::Init;
                } else {
                    check_spooldef_item(&words).map_err(|e| ioerr!(InvalidData,"{}: {}", info, e))?;
                }
            },
            DCState::MetaSpool => {
                if words[0] == "end" {
                    state = DCState::Init;
                } else {
                    check_metaspool_item(&words).map_err(|e| ioerr!(InvalidData,"{}: {}", info, e))?;
                }
            },
        }
    }

    match state {
        DCState::Init => {},
        DCState::Spool => {
            Err(ioerr!(
                InvalidData,
                "{}: unexpected EOF in spool {}",
                info,
                spool_no
            ))?
        },
        DCState::MetaSpool => {
            Err(ioerr!(
                InvalidData,
                "{}: unexpected EOF in metaspool {}",
                info,
                metaspool,
            ))?
        },
    }

    Ok(())
}

// Check one item of a SpoolDef
fn check_spooldef_item(words: &[&str]) -> io::Result<()> {
    match words[0] {
        // we do not support this, warn and ignore.
        "minfreefiles" | "compresslvl" => log::warn!("{}: unsupported keyword, ignoring", words[0]),

        // we do not support this, error and return.
        "spooldirs" => Err(ioerr!(InvalidData,"{}: unsupported keyword", words[0]))?,

        _ => {},
    }
    Ok(())
}

// Check one item of a MetaSpool
fn check_metaspool_item(words: &[&str]) -> io::Result<()> {
    match words[0] {
        "allocstrat" => {
            if words.len() < 2 {
                Err(ioerr!(InvalidData,"{}: missing argument", words[0]))?;
            }
            match words[1] {
                "space" => Err(ioerr!(InvalidData,"{} space: not supported (only weighted)", words[0]))?,
                "sequential" | "single" => {
                    log::warn!("{} {}: ignoring, always using \"weighted\"", words[0], words[1])
                },
                "weighted" => {},
                _ => Err(ioerr!(InvalidData,"{} {}: unknown allocstrat", words[0], words[1]))?,
            }
        },

        // we do not support this, fatal.
        "label" => Err(ioerr!(InvalidData, "{}: unsupported keyword", words[0]))?,

        // actually don't know.
        _ => {},
    }
    Ok(())
}

