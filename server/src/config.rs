use std::sync::Arc;
use std::fs::File;
use std::io::prelude::*;
use std::io;

use parking_lot::RwLock;

use dconfig::*;
use newsfeeds::NewsFeeds;
use nntp_rs_spool::SpoolCfg;
use nntp_rs_util as util;

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
    pub dspool_ctl:     Option<String>,
    pub diablo_hosts:   Option<String>,
    pub dnewsfeeds:     String,
}

/// Histfile config table in Toml config file.
#[derive(Default,Deserialize, Debug)]
pub struct HistFile {
    pub path:       String,
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

    let mut feeds = read_dnewsfeeds(&cfg.paths.dnewsfeeds)?;
    if let Some(ref dhosts) = cfg.paths.diablo_hosts {
        read_diablo_hosts(&mut feeds, dhosts)?;
    }
    if let Some(ref dspoolctl) = cfg.paths.dspool_ctl {
        read_dspool_ctl(dspoolctl, &mut cfg.spool)?;
    }
    feeds.init_hostcache();

    // replace the NEWSFEEDS config.
    *NEWSFEEDS.write() = Some(Arc::new(feeds));

    // replace the CONFIG config.
    *CONFIG.write() = Some(Arc::new(cfg));

    Ok(())
}

/// Get a reference-counted reference to the current config.
pub fn get_config() -> Arc<Config> {
    Arc::clone(CONFIG.read().as_ref().unwrap())
}

/// Get a reference-counted reference to the current newsfeeds.
pub fn get_newsfeeds() -> Arc<NewsFeeds> {
    Arc::clone(NEWSFEEDS.read().as_ref().unwrap())
}

