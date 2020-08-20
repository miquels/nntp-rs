///
/// Configuration file reader and checker.
///
use std::collections::HashMap;
use std::io;
use std::net::{AddrParseError, SocketAddr};
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use core_affinity::{get_core_ids, CoreId};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::{Captures, Regex};
use serde::Deserialize;
use users::switch::{set_effective_gid, set_effective_uid};
use users::{get_effective_gid, get_effective_uid, get_group_by_name, get_user_by_name};

use crate::dconfig::*;
use crate::newsfeeds::NewsFeeds;
use crate::spool::{SpoolCfg, SpoolDef, MetaSpool};
use crate::util;
use crate::util::BlockingType;

static CONFIG: Lazy<RwLock<Option<Arc<Config>>>> = Lazy::new(|| RwLock::new(None));
static NEWSFEEDS: Lazy<RwLock<Option<Arc<NewsFeeds>>>> = Lazy::new(|| RwLock::new(None));

/// Curlyconf configuration.
#[derive(Deserialize, Debug)]
#[rustfmt::skip]
pub struct Config {
    pub server:     Server,
    pub history:    HistFile,
    pub paths:      Paths,
    pub config:     CfgFiles,
    #[serde(default, rename = "log")]
    pub logging:    Logging,
    // Map of spools. Index is a number 0..99.
    #[serde(rename = "spool", default)]
    spooldef:       Option<HashMap<u8, SpoolDef>>,
    // List of spool groups (metaspool in diablo).
    #[serde(default)]
    spoolgroup:     Option<Vec<MetaSpool>>,
    #[serde(skip)]
    pub timestamp:  u64,
    #[serde(skip)]
    newsfeeds:      Option<NewsFeeds>,
    /// Flattened SpoolCfg (spool / spoolgroup).
    #[serde(skip)]
    pub spool:      SpoolCfg,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Runtime {
    Threaded(Threaded),
    MultiSingle(MultiSingle),
}

impl Default for Runtime {
    fn default() -> Runtime {
        Runtime::Threaded(Threaded::default())
    }
}

/// Server config table in Toml config file.
#[derive(Deserialize, Debug, Default)]
#[rustfmt::skip]
pub struct Server {
    #[serde(default = "util::hostname")]
    pub hostname:       String,
    pub listen:         Option<Vec<String>>,
    pub runtime:        Runtime,
    pub user:           Option<String>,
    pub group:          Option<String>,
    pub uid:            Option<users::uid_t>,
    pub gid:            Option<users::gid_t>,
    #[serde(default)]
    pub log_panics:     bool,
    #[serde(default,deserialize_with = "util::option_deserialize_size")]
    pub maxartsize:     Option<u64>,
}

/// Paths.
#[derive(Deserialize, Debug, Default)]
#[rustfmt::skip]
pub struct Paths {
    pub config:         String,
    pub spool:          String,
    pub log:            String,
    pub db:             String,
    pub run:            String,
    pub queue:          String,
}

/// Config files.
#[derive(Deserialize, Debug, Default)]
#[rustfmt::skip]
pub struct CfgFiles {
    #[serde(rename = "newsfeeds")]
    pub dnewsfeeds:     String,
    #[serde(rename = "spool")]
    pub dspool_ctl:     Option<String>,
    pub diablo_hosts:   Option<String>,
}

/// Logging.
#[derive(Deserialize, Debug, Default)]
#[rustfmt::skip]
pub struct Logging {
    pub general:        Option<String>,
    pub incoming:       Option<String>,
}

/// Histfile config table in Toml config file.
#[derive(Default,Deserialize, Debug)]
#[rustfmt::skip]
pub struct HistFile {
    pub file:       String,
    pub backend:    String,
    pub threads:    Option<usize>,
    #[serde(default,deserialize_with = "util::deserialize_duration")]
    pub remember:   Duration,
}

/// Multiple single-threaded executors.
#[derive(Default,Deserialize)]
#[rustfmt::skip]
pub struct MultiSingle {
    pub threads:            Option<usize>,
    pub cores:              Option<String>,
    pub threads_per_core:   Option<usize>,
    #[serde(skip)]
    pub core_ids:           Option<Vec<CoreId>>,
}

/// The (default) threaded executor.
#[derive(Default,Deserialize,Debug)]
#[rustfmt::skip]
pub struct Threaded {
    #[serde(rename = "blocking_io")]
    pub blocking_type:      BlockingType,
}

impl std::fmt::Debug for MultiSingle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiSingle {{ threads: {:?}, cores: {:?}, threads_per_core: {:?} }}",
            self.threads, self.cores, self.threads_per_core
        )
    }
}

/// Read the configuration.
pub fn read_config(name: &str, load_newsfeeds: bool) -> io::Result<Config> {
    let mut cfg: Config = curlyconf::from_file(name)?;

    // Because #[serde(flatten)] does not work in structs, do it manually.
    cfg.spool.spool = cfg.spooldef.take().unwrap_or(Default::default());
    cfg.spool.spoolgroup = cfg.spoolgroup.take().unwrap_or(Default::default());

    match cfg.server.maxartsize {
        None => cfg.server.maxartsize = Some(10_000_000),
        Some(0) => cfg.server.maxartsize = None,
        _ => {},
    }

    // If user or group was set
    resolve_user_group(&mut cfg)?;

    // Check the [multisingle] config
    if let Runtime::MultiSingle(ref mut multisingle) = cfg.server.runtime {
        check_multisingle(multisingle)
            .map_err(|e| ioerr!(InvalidData, format!("multisingle: {}", e)))?;
    }

    if load_newsfeeds {
        let mut feeds = read_dnewsfeeds(&expand_path(&cfg.paths, &cfg.config.dnewsfeeds))?;
        if let Some(ref dhosts) = expand_path_opt(&cfg.paths, &cfg.config.diablo_hosts) {
            read_diablo_hosts(&mut feeds, dhosts)?;
        }
        cfg.newsfeeds = Some(feeds);
    }

    if let Some(ref dspoolctl) = expand_path_opt(&cfg.paths, &cfg.config.dspool_ctl) {
        read_dspool_ctl(dspoolctl, &mut cfg.spool)?;
    }

    return Ok(cfg);
}

pub fn set_config(mut cfg: Config) -> Arc<Config> {
    if let Some(mut feeds) = cfg.newsfeeds.take() {
        // replace the NEWSFEEDS config.
        feeds.init_hostcache();
        *NEWSFEEDS.write() = Some(Arc::new(feeds));
    }

    // replace the CONFIG config.
    *CONFIG.write() = Some(Arc::new(cfg));

    get_config()
}

// lookup user and group.
fn resolve_user_group(cfg: &mut Config) -> io::Result<()> {
    let user = cfg.server.user.as_ref();
    let group = cfg.server.group.as_ref();

    // lookup username and group.
    let (uid, ugid) = match user {
        Some(u) => {
            let user = get_user_by_name(u).ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                format!("user {}: not found", u),
            ))?;
            (Some(user.uid()), Some(user.primary_group_id()))
        },
        None => (None, None),
    };
    // lookup group if specified separately.
    let gid = match group {
        Some(g) => {
            let group = get_group_by_name(g).ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                format!("group {}: not found", g),
            ))?;
            Some(group.gid())
        },
        None => ugid,
    };
    cfg.server.uid = uid;
    cfg.server.gid = gid;
    Ok(())
}

// do setuid/setgid.
pub fn switch_uids(cfg: &Config) -> io::Result<()> {
    let uid = cfg.server.uid;
    let gid = cfg.server.gid;

    // if user and group not set, return.
    if uid.is_none() && gid.is_none() {
        return Ok(());
    }
    let euid = get_effective_uid();
    let egid = get_effective_gid();
    // if user and group are unchanged, return.
    if Some(euid) == uid && Some(egid) == gid {
        return Ok(());
    }
    // switch to root.
    if let Err(e) = set_effective_uid(0) {
        return Err(io::Error::new(
            e.kind(),
            "change user/group: insufficient priviliges",
        ));
    }
    // this will panic on fail, but that's what we want.
    set_effective_gid(gid.unwrap_or(egid)).unwrap();
    set_effective_uid(uid.unwrap_or(euid)).unwrap();
    Ok(())
}

///
/// expand ${path} in string.
pub fn expand_path(paths: &Paths, path: &str) -> String {
    let re = Regex::new(r"(\$\{[a-z]+\})").unwrap();
    re.replace_all(path, |caps: &Captures| {
        match &caps[1] {
            "${config}" => paths.config.clone(),
            "${spool}" => paths.spool.clone(),
            "${log}" => paths.log.clone(),
            "${db}" => paths.db.clone(),
            "${run}" => paths.run.clone(),
            "${queue}" => paths.queue.clone(),
            "${date}" => {
                let now = util::UnixTime::now().datetime_local();
                format!("{:04}{:02}{:02}", now.year(), now.month(), now.day())
            },
            p => p.to_string(),
        }
    })
    .to_string()
}

pub fn expand_path_opt(paths: &Paths, path: &Option<String>) -> Option<String> {
    match path {
        &Some(ref cf) => Some(expand_path(paths, cf)),
        None => None,
    }
}

/// Get a reference-counted reference to the current config.
pub fn get_config() -> Arc<Config> {
    Arc::clone(CONFIG.read().as_ref().unwrap())
}

/// Get a reference-counted reference to the current newsfeeds.
pub fn get_newsfeeds() -> Arc<NewsFeeds> {
    Arc::clone(NEWSFEEDS.read().as_ref().unwrap())
}

fn err_invalid(e: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn err_invalid2(s: &str, e: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{}: {}", s, e.into()))
}

// parse a string of *inclusive* ranges into a Vec<Range<usize>>.
fn parse_ranges(s: &str) -> io::Result<Vec<Range<usize>>> {
    let mut res = Vec::new();

    // split string at comma.
    for r in s.split(',').map(|s| s.trim()) {
        // then split at '-'
        let mut x = r.splitn(2, '-');

        // parse the first number.
        let num = x.next().unwrap();
        let b = num.parse::<usize>().map_err(|_| err_invalid("parse error"))?;

        // if there is a second number, parse it as well.
        let e = match x.next() {
            None => b,
            Some(num) => num.parse::<usize>().map_err(|_| err_invalid("parse error"))?,
        };

        // another sanity check.
        if e < b {
            return Err(err_invalid("invalid range"));
        }

        res.push(Range {
            start: b,
            end:   e + 1,
        });
    }
    Ok(res)
}

fn parse_cores(s: &str) -> io::Result<Vec<CoreId>> {
    // first put all cores into a vector of Option<CoreId>.
    let mut cores = match get_core_ids() {
        Some(c) => c.into_iter().map(|c| Some(c)).collect::<Vec<_>>(),
        None => return Err(err_invalid("cannot get core ids from kernel")),
    };
    let ranges = if s.eq_ignore_ascii_case("all") {
        vec![Range {
            start: 0usize,
            end:   cores.len() - 1,
        }]
    } else {
        parse_ranges(s).map_err(|e| err_invalid2(s, e.to_string()))?
    };
    let mut res = Vec::new();

    for range in ranges.into_iter() {
        if range.end > cores.len() {
            return Err(err_invalid2(
                s,
                format!("id out of range [0..{})", cores.len() - 1),
            ));
        }
        for i in range {
            // move from the cores vec into the result vec.
            let mut core = None;
            std::mem::swap(&mut cores[i], &mut core);
            match core {
                Some(c) => res.push(c),
                None => return Err(err_invalid2(s, "overlapping ranges")),
            }
        }
    }
    Ok(res)
}

fn check_multisingle(multisingle: &mut MultiSingle) -> io::Result<()> {
    // "threads_per_core" might be set, but then "cores" must be set as well.
    let tpc = match multisingle.threads_per_core {
        Some(tpc) => {
            if multisingle.cores.is_none() {
                return Err(err_invalid("threads_per_core: \"cores\" must be set first"));
            }
            tpc
        },
        None => 1,
    };
    // parse "cores" if set.
    if let Some(cores) = multisingle.cores.as_ref() {
        let mut res = Vec::new();
        for c in &parse_cores(cores.as_str()).map_err(|e| err_invalid2("cores", e.to_string()))? {
            for _ in 0..tpc {
                res.push(c.clone());
            }
        }
        multisingle.core_ids = Some(res);
    }
    // if "threads" is set, the numbers must match up.
    if let Some(threads) = multisingle.threads {
        if let Some(core_ids) = multisingle.core_ids.as_ref() {
            if core_ids.len() != threads {
                if tpc == 1 {
                    return Err(err_invalid("threads: must match cores"));
                } else {
                    return Err(err_invalid("threads: must match cores * threads_per_core"));
                }
            }
        }
    }
    Ok(())
}

pub fn parse_listener(s: impl Into<String>) -> Result<SocketAddr, AddrParseError> {
    SocketAddr::from_str(&s.into())
}

pub fn parse_listeners(listeners: Option<&Vec<String>>) -> io::Result<Vec<SocketAddr>> {
    let dfl = vec![":119".to_string()];
    let mut res = Vec::new();
    for l in listeners.unwrap_or(&dfl).iter().map(|s| s.as_str()) {
        if l.starts_with(":") {
            let p = (&l[1..])
                .parse::<u16>()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}: {}", l, e)))?;
            res.push(parse_listener(format!("0.0.0.0:{}", p)).unwrap());
            res.push(parse_listener(format!("[::]:{}", p)).unwrap());
        } else if l.starts_with("*:") {
            let a = parse_listener(format!("0.0.0.0:{}", &l[2..]))
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}: {}", l, e)))?;
            res.push(a);
        } else {
            let a = parse_listener(l)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}: {}", l, e)))?;
            res.push(a);
        }
    }
    Ok(res)
}
