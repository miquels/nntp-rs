use std::sync::Arc;
use std::fs::File;
use std::io::prelude::*;
use std::io;

use chrono::{self,Datelike};
use parking_lot::RwLock;
use regex::{Captures,Regex};
use users::switch::{set_effective_gid, set_effective_uid};
use users::{get_effective_gid, get_effective_uid, get_group_by_name, get_user_by_name};

use crate::dconfig::*;
use crate::newsfeeds::NewsFeeds;
use crate::spool::SpoolCfg;
use crate::util;

use toml;

lazy_static! {
    static ref CONFIG: RwLock<Option<Arc<Config>>> = RwLock::new(None);
    static ref NEWSFEEDS: RwLock<Option<Arc<NewsFeeds>>> = RwLock::new(None);
}

/// Toml config.
#[derive(Deserialize, Debug)]
pub struct Config {
    pub server:     Server,
    #[serde(default,flatten)]
    pub spool:      SpoolCfg,
    pub history:    HistFile,
    pub paths:      Paths,
    pub config:     CfgFiles,
    #[serde(default)]
    pub logging:    Logging,
    #[serde(skip)]
    pub timestamp:  u64,
    #[serde(skip)]
    newsfeeds:      Option<NewsFeeds>,
}

/// Server config table in Toml config file.
#[derive(Deserialize, Debug, Default)]
pub struct Server {
    #[serde(default)]
    pub hostname:       String,
    pub listen:         Option<String>,
    pub threads:        Option<usize>,
    #[serde(default)]
    pub executor:       Option<String>,
    #[serde(default)]
    pub user:           Option<String>,
    #[serde(default)]
    pub group:          Option<String>,
    #[serde(skip)]
    pub uid:            Option<users::uid_t>,
    #[serde(skip)]
    pub gid:            Option<users::gid_t>,
    #[serde(default)]
    pub log_panics:     bool,
}

/// Paths.
#[derive(Deserialize, Debug, Default)]
pub struct Paths {
    pub config:         String,
    pub spool:          String,
    pub log:            String,
    pub db:             String,
    pub run:            String,
}

/// Config files.
#[derive(Deserialize, Debug, Default)]
pub struct CfgFiles {
    pub dnewsfeeds:     String,
    pub dspool_ctl:     Option<String>,
    pub diablo_hosts:   Option<String>,
}

/// Logging.
#[derive(Deserialize, Debug, Default)]
pub struct Logging {
    pub general:        Option<String>,
    pub incoming:       Option<String>,
}

/// Histfile config table in Toml config file.
#[derive(Default,Deserialize, Debug)]
pub struct HistFile {
    pub file:       String,
    pub backend:    String,
    pub threads:    Option<usize>,
}

/// Read the configuration.
pub fn read_config(name: &str) -> io::Result<Config> {
    let mut f = File::open(name)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;

    let mut cfg: Config = match toml::from_str(&buffer) {
        Ok(v) => v,
        Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData,
                                            format!("{}: {}", name, e))),
    };

    if cfg.server.hostname == "" {
        cfg.server.hostname = match util::hostname() {
            Some(h) => h,
            None => "unconfigured".to_string(),
        }
    }

    // If user or group was set
    resolve_user_group(&mut cfg)?;

    let mut feeds = read_dnewsfeeds(&expand_path(&cfg.paths, &cfg.config.dnewsfeeds))?;
    if let Some(ref dhosts) = expand_path_opt(&cfg.paths, &cfg.config.diablo_hosts) {
        read_diablo_hosts(&mut feeds, dhosts)?;
    }
    if let Some(ref dspoolctl) = expand_path_opt(&cfg.paths, &cfg.config.dspool_ctl) {
        read_dspool_ctl(dspoolctl, &cfg.paths.spool.clone(), &mut cfg.spool)?;
    }
    cfg.newsfeeds = Some(feeds);

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
            let user = get_user_by_name(u).ok_or(
                io::Error::new(io::ErrorKind::NotFound, format!("user {}: not found", u))
            )?;
            (Some(user.uid()), Some(user.primary_group_id()))
        },
        None => (None, None),
    };
    // lookup group if specified separately.
    let gid = match group {
        Some(g) => {
            let group = get_group_by_name(g).ok_or(
                io::Error::new(io::ErrorKind::NotFound, format!("group {}: not found", g))
            )?;
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
        return Err(io::Error::new(e.kind(), "change user/group: insufficient priviliges"));
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
            "${spool}"  => paths.spool.clone(),
            "${log}"    => paths.log.clone(),
            "${db}"     => paths.db.clone(),
            "${run}"    => paths.run.clone(),
            "${date}"   => {
                let now = chrono::offset::Local::now();
                format!("{:04}{:02}{:02}", now.year(), now.month(), now.day())
            },
            p           => p.to_string(),
        }
    }).to_string()
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

