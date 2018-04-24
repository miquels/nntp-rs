
use std::io::prelude::*;
use std::io;
use std::fs::File;
use std::collections::HashMap;

use storage::SpoolCfg as Spool;

use toml;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub spool:      HashMap<String, Spool>,
    pub history:    HistFile,
}

#[derive(Deserialize, Debug)]
pub struct HistFile {
    pub path:       String,
    pub backend:    String,
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
