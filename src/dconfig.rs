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
use std::fmt::{self, Debug};
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, BufReader};

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{de, de::Deserializer, de::MapAccess, de::Visitor, Deserialize};

use crate::newsfeeds::*;
use crate::spool::{GroupMap, GroupMapEntry, MetaSpool, SpoolCfg, SpoolDef};
use crate::util::WildMatList;

// A type where a "dnewsfeeds" file can deserialize into.
#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct DNewsFeeds {
    #[serde(rename = "label")]
    pub labels:   Labels,
    pub groupdef: Vec<GroupDef>,
    pub global:   Option<NewsPeer>,
}

// Append all the DNewsfeeds data to the main NewsFeeds.
fn dnewsfeeds_to_newsfeeds(mut dnf: DNewsFeeds, nf: &mut NewsFeeds) {
    nf.infilter = dnf.labels.ifilter;
    for mut peer in dnf.labels.peers.drain(..) {
        peer.accept_headfeed = true;
        nf.peers.push(peer);
    }
    for gd in dnf.groupdef.into_iter() {
        let mut groups = gd.groups;
        groups.name = gd.label;
        nf.groupdefs.push(groups);
    }
    if let Some(global) = dnf.global.take() {
        nf.templates.push(global);
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
#[derive(Default, Debug)]
struct Labels {
    global:  Option<NewsPeer>,
    ifilter: Option<NewsPeer>,
    ispam:   Option<NewsPeer>,
    espam:   Option<NewsPeer>,
    peers:   Vec<NewsPeer>,
}

impl<'de> Deserialize<'de> for Labels {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct LabelsVisitor;

        impl<'de> Visitor<'de> for LabelsVisitor {
            type Value = Labels;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a dnewsfeeds entry")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where A: MapAccess<'de> {
                static NUMERIC_TRAILER: Lazy<Regex> = Lazy::new(|| {
                    let re = r"_\d{2}$";
                    Regex::new(re).expect("could not compile NUMERIC_TRAILER regexp")
                });
                let mut this = Labels::default();

                while let Some(label) = map.next_key::<String>()? {
                    let peer = map.next_value()?;
                    match label.as_str() {
                        "GLOBAL" => {
                            if this.global.is_some() {
                                return Err(de::Error::custom("GLOBAL already set"));
                            }
                            this.global = Some(peer);
                        },
                        // filters
                        "IFILTER" => this.ifilter = Some(peer),
                        "ISPAM" => this.ispam = Some(peer),
                        "ESPAM" => this.espam = Some(peer),
                        // peer.
                        label => {
                            // XS4ALL legacy -- if it ends in _\d{2}, skip it.
                            if !NUMERIC_TRAILER.is_match(label) {
                                this.peers.push(peer);
                            }
                        },
                    }
                }
                Ok(this)
            }
        }

        deserializer.deserialize_map(LabelsVisitor)
    }
}

/// Read a NewsFeeds from a "dnewsfeeds" file.
pub fn read_dnewsfeeds(name: &str, nf: &mut NewsFeeds) -> io::Result<()> {
    // First, do a compat check.
    check_dnewsfeeds(name)?;

    // Now build the config reader configuration.
    let dnf: DNewsFeeds = curlyconf::Builder::new()
        .mode(curlyconf::Mode::Diablo)
        .alias::<NewsPeer>("host", "hostname")
        .alias::<NewsPeer>("alias", "path-identity")
        .alias::<NewsPeer>("inhost", "accept-from")
        .alias::<NewsPeer>("maxconnect", "max-connections-in")
        .alias::<NewsPeer>("filter", "filter-groups")
        .alias::<NewsPeer>("nofilter", "filter-groups")
        .alias::<NewsPeer>("nomismatch", "ignore-path-mismatch")
        .alias::<NewsPeer>("precomreject", "dont-defer")
        .alias::<NewsPeer>("maxcross", "max-crosspost")
        .alias::<NewsPeer>("mincross", "min-crosspost")
        .alias::<NewsPeer>("maxpath", "max-path-length")
        .alias::<NewsPeer>("minpath", "min-path-length")
        .alias::<NewsPeer>("maxsize", "max-article-size")
        .alias::<NewsPeer>("minsize", "min-article-size")
        .alias::<NewsPeer>("arttypes", "article-types")
        .alias::<NewsPeer>("addgroup", "groups")
        .alias::<NewsPeer>("delgroup", "groups")
        .alias::<NewsPeer>("delgroupany", "groups")
        .alias::<NewsPeer>("groupref", "groups")
        .alias::<NewsPeer>("requiregroup", "groups-required")
        .alias::<NewsPeer>("adddist", "distributions")
        .alias::<NewsPeer>("deldist", "distributions")
        .alias::<NewsPeer>("hostname", "send-to")
        .alias::<NewsPeer>("bindaddress", "bind-address")
        .alias::<NewsPeer>("transmitbuf", "sendbuf-size")
        .alias::<NewsPeer>("maxparallel", "max-connections-out")
        .alias::<NewsPeer>("maxstream", "max-inflight")
        .alias::<NewsPeer>("delayfeed", "delay-feed")
        .alias::<NewsPeer>("nobatch", "no-backlog")
        .alias::<NewsPeer>("maxqueue", "max-backlog-queue")
        .alias::<NewsPeer>("headfeed", "send-headfeed")
        .alias::<NewsPeer>("preservebytes", "preserve-bytes")
        .alias::<GroupDef>("addgroup", "groups")
        .alias::<GroupDef>("delgroup", "groups")
        .alias::<GroupDef>("delgroupany", "groups")
        .alias::<GroupDef>("groupref", "groups")
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
        .ignore::<NewsPeer>("delayinfeed")
        .ignore::<NewsPeer>("genlines")
        .ignore::<NewsPeer>("setqos")
        .ignore::<NewsPeer>("settos")
        .from_file(name)?;

    // And add it to the 'newsfeeds' struct.
    dnewsfeeds_to_newsfeeds(dnf, nf);

    Ok(())
}

/// Read SpoolCfg from a "dspool.ctl" file.
pub fn read_dspool_ctl(name: &str, spool_cfg: &mut SpoolCfg) -> io::Result<()> {
    // First, do a compat check.
    check_dspool_ctl(name)?;

    // Then actually read the config.
    let mut cfg: SpoolCfg = curlyconf::Builder::new()
        .mode(curlyconf::Mode::Diablo)
        .alias::<SpoolCfg>("expire", "groupmap")
        .alias::<SpoolCfg>("metaspool", "spoolgroup")
        .alias::<MetaSpool>("addgroup", "groups")
        .alias::<MetaSpool>("label", "peer")
        .ignore::<SpoolDef>("expiremethod")
        .ignore::<SpoolDef>("minfreefiles")
        .ignore::<SpoolDef>("compresslvl")
        .from_file(name)?;

    for (_, sp) in cfg.spool.iter_mut() {
        if sp.backend == "" {
            sp.backend = "diablo".into();
        }
    }

    spool_cfg.spool.extend(cfg.spool.into_iter());
    spool_cfg.spoolgroup.extend(cfg.spoolgroup.into_iter());
    spool_cfg.groupmap.0.extend(cfg.groupmap.0.into_iter());

    Ok(())
}

// A custom map Deserialize implementation for "GroupMap",
// so that the "expire" line works in dspool.ctl.
impl<'de> Deserialize<'de> for GroupMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct GroupMapVisitor;

        impl<'de> Visitor<'de> for GroupMapVisitor {
            type Value = GroupMap;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("wildmat and spoolgroup")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where A: MapAccess<'de> {
                let mut v = Vec::new();
                while let Some(groups) = map.next_key::<String>()? {
                    let spoolgroup = map.next_value()?;
                    v.push(GroupMapEntry { groups, spoolgroup });
                }
                Ok(GroupMap(v))
            }
        }

        deserializer.deserialize_map(GroupMapVisitor)
    }
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
            Err(ioerr!(InvalidData, "{}: {}: data after label", info, words[0]))?;
        }
        match get_peer_idx(nf, words[1]) {
            None => log::warn!("{}: label {} not found in dnewsfeeds", info, words[1]),
            Some(idx) => {
                nf.peers[idx].accept_from.push(words[0].to_string());
            },
        }
    }
    Ok(())
}

fn get_peer_idx(nf: &NewsFeeds, name: &str) -> Option<usize> {
    for idx in 0..nf.peers.len() {
        if name == nf.peers[idx].label.as_str() {
            return Some(idx);
        }
    }
    None
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
fn check_dnewsfeeds(name: &str) -> io::Result<()> {
    let file = File::open(name).map_err(|e| io::Error::new(e.kind(), format!("{}: {}", name, e)))?;
    let file = BufReader::new(file);

    let mut line_no = 0;
    let mut info = String::new();
    let mut state = DNState::Init;

    let mut groupdefs: HashSet<String> = HashSet::new();
    let mut peers: HashSet<String> = HashSet::new();
    let mut label = String::new();

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
                            Err(ioerr!(
                                InvalidData,
                                "{}: {}: expected one argument",
                                info,
                                words[0]
                            ))?;
                        }
                        if peers.contains(words[0]) {
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
                            Err(ioerr!(
                                InvalidData,
                                "{}: {}: expected one argument",
                                info,
                                words[0]
                            ))?;
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
        DNState::GroupDef => {
            Err(ioerr!(
                InvalidData,
                "{}: unexpected EOF in groupdef {}",
                info,
                label
            ))?
        },
    }

    Ok(())
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
        "addgroup" | "delgroup" | "delgroupany" | "groupref" => {},
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
                            Err(ioerr!(
                                InvalidData,
                                "{}: {}: expected one argument",
                                info,
                                words[0]
                            ))?;
                        }
                        spool_no = match words[1].parse() {
                            Ok(n) => n,
                            Err(e) => return Err(ioerr!(InvalidData, "{}: {}", info, e)),
                        };
                        state = DCState::Spool;
                    },
                    "metaspool" => {
                        if words.len() != 2 {
                            Err(ioerr!(
                                InvalidData,
                                "{}: {}: expected one argument",
                                info,
                                words[0]
                            ))?;
                        }
                        metaspool = words[1].to_string();
                        state = DCState::MetaSpool;
                    },
                    "expire" => {
                        if words.len() != 3 {
                            Err(ioerr!(
                                InvalidData,
                                "{}: {}: expected two arguments",
                                info,
                                words[0]
                            ))?;
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
                    check_spooldef_item(&words).map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
                }
            },
            DCState::MetaSpool => {
                if words[0] == "end" {
                    state = DCState::Init;
                } else {
                    check_metaspool_item(&words).map_err(|e| ioerr!(InvalidData, "{}: {}", info, e))?;
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
        "spooldirs" => Err(ioerr!(InvalidData, "{}: unsupported keyword", words[0]))?,

        _ => {},
    }
    Ok(())
}

// Check one item of a MetaSpool
fn check_metaspool_item(words: &[&str]) -> io::Result<()> {
    match words[0] {
        "allocstrat" => {
            if words.len() < 2 {
                Err(ioerr!(InvalidData, "{}: missing argument", words[0]))?;
            }
            match words[1] {
                "space" => {
                    Err(ioerr!(
                        InvalidData,
                        "{} space: not supported (only weighted)",
                        words[0]
                    ))?
                },
                "sequential" | "single" => {
                    log::warn!("{} {}: ignoring, always using \"weighted\"", words[0], words[1])
                },
                "weighted" => {},
                _ => {
                    Err(ioerr!(
                        InvalidData,
                        "{} {}: unknown allocstrat",
                        words[0],
                        words[1]
                    ))?
                },
            }
        },

        // we do not support this, fatal.
        "label" => Err(ioerr!(InvalidData, "{}: unsupported keyword", words[0]))?,

        // actually don't know.
        _ => {},
    }
    Ok(())
}
