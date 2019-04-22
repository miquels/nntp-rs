use std::sync::Arc;
use std::fs::File;
use std::io::prelude::*;
use std::io;

use chrono::{self,Datelike};
use parking_lot::RwLock;
use regex::{Captures,Regex};

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
    #[serde(skip)]
    pub timestamp:  u64,
}

/// Server config table in Toml config file.
#[derive(Deserialize, Debug, Default)]
pub struct Server {
    #[serde(default)]
    pub hostname:       String,
    pub listen:         Option<String>,
    pub threads:        Option<usize>,
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
pub struct Log {
    pub general:        String,
    pub incoming:       String,
}

/// Histfile config table in Toml config file.
#[derive(Default,Deserialize, Debug)]
pub struct HistFile {
    pub file:       String,
    pub backend:    String,
    pub threads:    Option<usize>,
}

/// Read the configuration.
pub fn read_config(name: &str) -> io::Result<()> {
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

    let mut feeds = read_dnewsfeeds(&expand_path(&cfg.paths, &cfg.config.dnewsfeeds))?;
    if let Some(ref dhosts) = expand_path_opt(&cfg.paths, &cfg.config.diablo_hosts) {
        read_diablo_hosts(&mut feeds, dhosts)?;
    }
    if let Some(ref dspoolctl) = expand_path_opt(&cfg.paths, &cfg.config.dspool_ctl) {
        read_dspool_ctl(dspoolctl, &cfg.paths.spool.clone(), &mut cfg.spool)?;
    }
    feeds.init_hostcache();

    // replace the NEWSFEEDS config.
    *NEWSFEEDS.write() = Some(Arc::new(feeds));

    // replace the CONFIG config.
    *CONFIG.write() = Some(Arc::new(cfg));

    Ok(())
}

/// expand ${path} in string.
pub fn expand_path(paths: &Paths, path: &str) -> String {
    let re = Regex::new(r"(\$\{[a-z]+\})").unwrap();
    let mut dt = None;
    re.replace_all(path, |caps: &Captures| {
        match &caps[1] {
            "${config}" => paths.config.as_str(),
            "${spool}"  => paths.spool.as_str(),
            "${log}"    => paths.log.as_str(),
            "${db}"     => paths.db.as_str(),
            "${run}"    => paths.run.as_str(),
            "${date}"   => {
                let now = chrono::offset::Local::now();
                dt.get_or_insert(format!("{:04}{:02}{:02}", now.year(), now.month(), now.day()))
            },
            p           => p,
        }.to_string()
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

