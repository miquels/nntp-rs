///
/// Configuration file reader and checker.
///
use std::io;
use std::net::{AddrParseError, SocketAddr};
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use core_affinity::{get_core_ids, CoreId};
use curlyconf::Watcher;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::{Captures, Regex};
use serde::Deserialize;
use users::switch::{set_effective_gid, set_effective_uid};
use users::{get_effective_gid, get_effective_uid, get_group_by_name, get_user_by_name};

use crate::dconfig::*;
use crate::history::MLockMode;
use crate::newsfeeds::NewsFeeds;
use crate::spool::SpoolCfg;
use crate::util;
use crate::util::BlockingType;

static CONFIG: Lazy<RwLock<Option<Arc<Config>>>> = Lazy::new(|| RwLock::new(None));
static NEWSFEEDS: Lazy<RwLock<Option<Arc<NewsFeeds>>>> = Lazy::new(|| RwLock::new(None));

/// Curlyconf configuration.
#[derive(Clone, Deserialize, Debug)]
#[rustfmt::skip]
pub struct Config {
    pub server:     Server,
    pub history:    HistFile,
    pub paths:      Paths,
    #[serde(default)]
    pub compat:     Compat,
    #[serde(default, rename = "log")]
    pub logging:    Logging,
    #[serde(default)]
    pub active:     Option<Active>,
    #[serde(default)]
    pub spool:      SpoolCfg,
    #[serde(default)]
    pub newsfeeds:  Arc<NewsFeeds>,
    #[serde(rename = "input-filter", default)]
    pub infilter:   InFilter,
    #[serde(skip)]
    filename:       String,
    #[serde(skip)]
    watcher:        Watcher,
}

#[derive(Clone, Deserialize, Debug)]
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
#[derive(Clone, Deserialize, Debug, Default)]
#[rustfmt::skip]
pub struct Server {
    #[serde(default = "util::hostname")]
    pub hostname:       String,
    #[serde(rename = "path-identity", default)]
    pub path_identity:  Vec<String>,
    #[serde(default)]
    pub commonpath:     Vec<String>,
    pub listen:         Option<Vec<String>>,
    #[serde(default)]
    pub runtime:        Runtime,
    pub user:           Option<String>,
    pub group:          Option<String>,
    #[serde(rename = "aux-groups")]
    pub aux_groups:     Option<Vec<String>>,
    #[serde(default)]
    pub pidfile:        Option<String>,
    #[serde(default)]
    pub log_panics:     bool,
    #[serde(default,rename = "max-article-size", deserialize_with = "util::option_deserialize_size")]
    pub maxartsize:     Option<u64>,
    #[serde(skip)]
    pub uid:            Option<users::uid_t>,
    #[serde(skip)]
    pub gid:            Option<users::gid_t>,
    #[serde(skip)]
    pub aux_gids:       Option<Vec<u32>>,
}

/// Paths.
#[derive(Clone, Deserialize, Debug, Default)]
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
#[derive(Clone, Deserialize, Debug, Default)]
#[serde(default)]
#[rustfmt::skip]
pub struct Compat {
    pub dnewsfeeds:     Option<String>,
    pub dspool_ctl:     Option<String>,
    pub diablo_hosts:   Option<String>,
}

/// Logging.
#[derive(Clone, Deserialize, Debug, Default)]
#[rustfmt::skip]
pub struct Logging {
    pub general:        Option<String>,
    pub incoming:       Option<String>,
    pub metrics:        Option<String>,
    pub prometheus:     Option<String>,
}

/// Histfile config section.
#[derive(Clone, Deserialize, Debug)]
#[rustfmt::skip]
pub struct HistFile {
    pub file:       String,
    pub backend:    String,
    #[serde(default)]
    pub threads:    Option<usize>,
    #[serde(default)]
    pub mlock:      MLockMode,
    #[serde(default,deserialize_with = "util::deserialize_duration")]
    pub remember:   Duration,
}

/// Input filters.
#[derive(Clone, Default, Deserialize, Debug)]
#[rustfmt::skip]
pub struct InFilter {
    #[serde(rename = "reject")]
    pub reject_:     Option<crate::newsfeeds::Filter>,
    #[serde(skip)]
    pub reject:     Option<crate::newsfeeds::NewsPeer>,
}

/// Active config section.
#[derive(Clone, Deserialize, Debug)]
#[rustfmt::skip]
pub struct Active {
    pub backend:    ActiveBackend,
    #[serde(default)]
    pub xref:       Option<XrefConfig>,
    #[serde(default)]
    pub sync:       Option<ActiveSync>,
}

/// Backend selection in the active section.
#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
#[rustfmt::skip]
pub enum ActiveBackend {
    Diablo(DActive),
}

/// Settings for the diablo active file backend.
#[derive(Clone, Default,Deserialize, Debug)]
#[rustfmt::skip]
pub struct DActive {
    pub file:       String,
}

/// Article numbering configuration.
#[derive(Clone, Deserialize, Debug)]
#[rustfmt::skip]
pub struct XrefConfig {
    mode:       XrefMode,
    #[serde(default)]
    hostname:   String,
}

/// Article numbering mode (primary/replica).
#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
#[rustfmt::skip]
pub enum XrefMode {
    Primary,
    Replica,
}

/// Active file synchronisation with a remote server.
#[derive(Clone, Default,Deserialize, Debug)]
#[rustfmt::skip]
pub struct ActiveSync {
    pub server:         String,
    pub groups:         String,
    #[serde(default)]
    pub remove:         bool,
    #[serde(default)]
    pub descriptions:   bool,
}

/// Multiple single-threaded executors.
#[derive(Clone, Default,Deserialize)]
#[rustfmt::skip]
pub struct MultiSingle {
    pub threads:            Option<usize>,
    pub cores:              Option<String>,
    pub threads_per_core:   Option<usize>,
    #[serde(skip)]
    pub core_ids:           Option<Vec<CoreId>>,
}

/// The (default) threaded executor.
#[derive(Clone, Default,Deserialize,Debug)]
#[rustfmt::skip]
#[serde(default)]
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
    let watcher = Watcher::new();
    let mut cfg: Config = curlyconf::Builder::new().watcher(&watcher).from_file(name)?;
    cfg.watcher = watcher;
    cfg.filename = name.to_string();

    match cfg.server.maxartsize {
        None => cfg.server.maxartsize = Some(10_000_000),
        Some(0) => cfg.server.maxartsize = None,
        _ => {},
    }

    // in hostname-related setting, replace ${hostname} with the
    // system hostname. really only useful for situations like
    // `path_identity sharedname!${hostname};`.
    let hostname = util::hostname();
    cfg.server.hostname = cfg.server.hostname.replace("${hostname}", &hostname);

    if let Some(xref) = cfg.active.as_mut().and_then(|a| a.xref.as_mut()) {
        if xref.hostname != "" {
            xref.hostname = xref.hostname.replace("${hostname}", &hostname);
        } else {
            xref.hostname = cfg.server.hostname.clone();
        }
    }

    for path_identity in cfg.server.path_identity.iter_mut() {
        *path_identity = path_identity.replace("${hostname}", &hostname);
    }
    if cfg.server.path_identity.is_empty() {
        cfg.server.path_identity.push(cfg.server.hostname.clone());
    }

    for commonpath in cfg.server.commonpath.iter_mut() {
        *commonpath = commonpath.replace("${hostname}", &hostname);
    }

    // Fix up pathhost and commonpath.
    pathhosts_fixup(&mut cfg.server.path_identity);
    pathhosts_fixup(&mut cfg.server.commonpath);

    if let Some(ref reject) = cfg.infilter.reject_ {
        cfg.infilter.reject = Some(reject.to_newspeer());
    }

    // metrics must be set if prometheus is set.
    if cfg.logging.prometheus.is_some() && cfg.logging.metrics.is_none() {
        return Err(ioerr!(
            InvalidData,
            "log: metrics cannot be empty if prometheus is set"
        ));
    }

    // If user or group was set
    resolve_user_group(&mut cfg)?;

    // Check the [multisingle] config
    if let Runtime::MultiSingle(ref mut multisingle) = cfg.server.runtime {
        check_multisingle(multisingle).map_err(|e| ioerr!(InvalidData, format!("multisingle: {}", e)))?;
    }

    if load_newsfeeds {
        if let Some(dnewsfeeds) = cfg.compat.dnewsfeeds.as_ref() {
            let newsfeeds = Arc::get_mut(&mut cfg.newsfeeds).unwrap();
            let dnewsfeeds = expand_path(&cfg.paths, dnewsfeeds);
            read_dnewsfeeds(&dnewsfeeds, newsfeeds)?;
            cfg.watcher.add_file(&dnewsfeeds)?;
            if let Some(ref dhosts) = expand_path_opt(&cfg.paths, &cfg.compat.diablo_hosts) {
                read_diablo_hosts(newsfeeds, dhosts)?;
                cfg.watcher.add_file(dhosts)?;
            }
        }
    }

    if let Some(ref dspoolctl) = expand_path_opt(&cfg.paths, &cfg.compat.dspool_ctl) {
        read_dspool_ctl(dspoolctl, &mut cfg.spool)?;
    }

    return Ok(cfg);
}

pub fn reread_config() -> io::Result<bool> {
    // Changed?
    let config = get_config();
    if !config.watcher.changed() {
        return Ok(false);
    }
    // Yes, so read again.
    let new_config = read_config(&config.filename, true)?;

    // Replace only the parts that are reconfigurable.
    // For now only the newsfeeds config.
    let mut config = (&*config).clone();
    config.watcher = new_config.watcher.clone();
    config.newsfeeds = new_config.newsfeeds;
    config.compat = new_config.compat;

    set_config(config)?;
    Ok(true)
}

pub fn set_config(mut cfg: Config) -> io::Result<Arc<Config>> {
    // initialize the new newsfeeds config.
    let newsfeeds = Arc::get_mut(&mut cfg.newsfeeds).unwrap();
    let mut nf = std::mem::replace(newsfeeds, NewsFeeds::default());
    nf.merge_templates()?;
    nf.set_hostname_default();
    nf.init_hostcache();
    nf.resolve_references();
    nf.check_self(&cfg);
    let newsfeeds = Arc::get_mut(&mut cfg.newsfeeds).unwrap();
    std::mem::swap(&mut nf, newsfeeds);

    // Set the global NEWSFEEDS.
    *NEWSFEEDS.write() = Some(Arc::clone(&cfg.newsfeeds));

    // replace the CONFIG config.
    *CONFIG.write() = Some(Arc::new(cfg));

    Ok(get_config())
}

// Any elements separated by '!' get split into sub-elements, and reversed.
// This way a setting like "pathhost clustername!hostname" works as expected.
fn pathhosts_fixup(path: &mut Vec<String>) {
    let mut newpath = Vec::new();
    for elem in path.iter() {
        let mut elems = elem.split("!").map(|e| e.to_string()).collect::<Vec<_>>();
        elems.reverse();
        newpath.extend(elems);
    }
    *path = newpath;
}

// lookup user and group.
fn resolve_user_group(cfg: &mut Config) -> io::Result<()> {
    let user = cfg.server.user.as_ref();
    let group = cfg.server.group.as_ref();
    let groups = cfg.server.aux_groups.as_ref();

    // lookup username and group.
    let (uid, ugid, ugids) = match user {
        Some(u) => {
            let user = get_user_by_name(u).ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                format!("user {}: not found", u),
            ))?;
            let gids = user.groups().map(|v| v.into_iter().map(|g| g.gid()).collect());
            (Some(user.uid()), Some(user.primary_group_id()), gids)
        },
        None => (None, None, None),
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
    // lookup auxilary groups if specified separately.
    let gids = match groups {
        Some(groups) => {
            let mut gids = Vec::new();
            gids.extend(&gid);
            gids.extend(&ugid);
            for g in groups {
                let group = get_group_by_name(g).ok_or(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("group {}: not found", g),
                ))?;
                gids.push(group.gid());
            }
            gids.dedup();
            Some(gids)
        },
        None => ugids,
    };
    cfg.server.uid = uid;
    cfg.server.gid = gid;
    cfg.server.aux_gids = gids;
    Ok(())
}

// do setuid/setgid.
pub fn switch_uids(cfg: &Config) -> io::Result<()> {
    let uid = cfg.server.uid;
    let gid = cfg.server.gid;
    let gids = cfg.server.aux_gids.as_ref().map(|g| g.as_slice()).unwrap_or(&[]);

    // if user and group not set, return.
    if uid.is_none() && gid.is_none() && gids.is_empty() {
        return Ok(());
    }
    let euid = get_effective_uid();
    let egid = get_effective_gid();
    // if user and group are unchanged and "aux-groups" was not set return.
    if Some(euid) == uid && Some(egid) == gid && cfg.server.aux_groups.is_none() {
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
    util::setgroups(gids).unwrap();
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
            end:   cores.len(),
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
