
use std::fs::File;
use std::io::prelude::*;
use std::io;

use newsfeeds::NewsFeeds;
use nntp_rs_spool::SpoolCfg;
use dconfig::*;

use toml;

/// Toml config.
#[derive(Deserialize, Debug)]
pub struct TomlConfig {
    #[serde(default)]
    pub server:     Server,
    #[serde(default,flatten)]
    pub spool:      SpoolCfg,
    #[serde(default)]
    pub history:    HistFile,
}

/// Config from toml, dnewsfeeds, dspool.ctl files.
#[derive(Debug)]
pub struct Config {
    pub server:     Server,
    pub spool:      SpoolCfg,
    pub history:    HistFile,
    pub newsfeeds:  NewsFeeds,
}

/// Server config table in Toml config file.
#[derive(Deserialize, Debug, Default)]
pub struct Server {
    pub listen:         Option<String>,
    pub threads:        Option<usize>,
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
pub fn read_config(name: &str) -> io::Result<Config> {
    let mut f = File::open(name)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;
    
    let mut cfg : TomlConfig = match toml::from_str(&buffer) {
        Ok(v) => v,
        Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
    };

    let mut feeds = read_dnewsfeeds(&cfg.server.dnewsfeeds)?;
    if let Some(ref dhosts) = cfg.server.diablo_hosts {
        read_diablo_hosts(&mut feeds, dhosts)?;
    }
    if let Some(ref dspoolctl) = cfg.server.dspool_ctl {
        read_dspool_ctl(dspoolctl, &mut cfg.spool)?;
    }
    feeds.init_hostcache();

    Ok(Config{
        server:     cfg.server,
        spool:      cfg.spool,
        history:    cfg.history,
        newsfeeds:  feeds,
    })
}

