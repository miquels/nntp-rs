
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Debug;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::str::FromStr;
use std::time::Duration;

use nntp_rs_spool::SpoolCfg;
use nntp_rs_spool::MetaSpoolCfg;
use nntp_rs_util as util;

use toml;

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default)]
    pub server:     Server,
    #[serde(default)]
    pub spool:      HashMap<String, SpoolCfg>,
    #[serde(default)]
    pub metaspool:  Vec<MetaSpoolCfg>,
    #[serde(default)]
    pub expire:     Vec<SpoolExpire>,
    pub history:    HistFile,
}

#[derive(Deserialize, Debug, Default)]
pub struct Server {
    pub listen:     Option<String>,
    pub threads:    Option<usize>,
    pub dspoolctl:  Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct HistFile {
    pub path:       String,
    pub backend:    String,
    pub threads:    Option<usize>,
}

#[derive(Deserialize, Debug)]
pub struct SpoolExpire {
    pub groups:     String,
    pub metaspool:  String,
}

pub fn read_config(name: &str) -> io::Result<Config> {
    let mut f = File::open(name)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;
    
    let cfg :Config = match toml::from_str(&buffer) {
        Ok(v) => v,
        Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
    };
    Ok(cfg)
}

