
use std::io::prelude::*;
use std::io;
use std::fs::File;
use std::collections::HashMap;

use nntp_rs_spool::SpoolCfg as Spool;
use nntp_rs_spool::MetaSpoolCfg as MetaSpool;

use toml;

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default)]
    pub server:     Server,
    pub spool:      HashMap<String, Spool>,
    pub metaspool:  Vec<MetaSpool>,
    pub history:    HistFile,
}

#[derive(Deserialize, Debug, Default)]
pub struct Server {
    pub listen:     Option<String>,
    pub threads:    Option<usize>,
}

#[derive(Deserialize, Debug)]
pub struct HistFile {
    pub path:       String,
    pub backend:    String,
    pub threads:    Option<usize>,
}

pub fn read_config(name: &str) -> io::Result<Config> {
    let mut f = File::open(name)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;
    
    match toml::from_str(&buffer) {
        Ok(v) => Ok(v),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
    }
}
