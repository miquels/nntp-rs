//! Spool (article storage) functionality. Supports multiple spool types.
//!
//! Types currently supported:
//!   - diabo

#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate byteorder;
extern crate nntp_rs_util as util;
extern crate serde;
extern crate time;

use std::io;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

mod diablo;

/// Which part of the article to process: body/head/all
#[derive(Debug,Clone,Copy,PartialEq)]
pub enum ArtPart {
    Body,
    Head,
    Article,
}

pub(crate) trait SpoolBackend: Send + Sync {
    fn get_type(&self) -> Backend;
    fn open(&self, art_loc: &ArtLoc, part: ArtPart) -> io::Result<Box<io::Read>>;
    fn write(&self, art: &[u8], hdr_len: usize, head_only: bool) -> io::Result<ArtLoc>;
}

#[derive(Clone)]
pub struct ArtLoc {
    pub storage_type:   Backend,
    pub spool:          u8,
    pub token:          Vec<u8>,
}

impl std::fmt::Debug for ArtLoc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ArtLoc {{ storage_type: {:?}, spool: {}, token: [", self.storage_type, self.spool)?;
        for b in &self.token {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "] }}")
    }
}


#[derive(Debug,PartialEq,Clone,Copy)]
#[repr(u8)]
pub enum Backend {
    Diablo = 0,
    Reject = 253,
    DontStore = 254,
    Unknown = 255,
}

impl Backend {
    /// convert u8 to enum.
    pub fn from_u8(t: u8) -> Backend {
        match t {
            0 => Backend::Diablo,
            _ => Backend::Unknown,
        }
    }
}

/// Metaspool is a group of spools.
#[derive(Clone,Deserialize,Default,Debug)]
pub struct MetaSpool {
    pub spool:          Vec<u8>,
    #[serde(default)]
    pub groups:         Vec<String>,
    #[serde(default,deserialize_with = "util::deserialize_size")]
    pub maxsize:        u64,
    #[serde(default,deserialize_with = "util::deserialize_duration")]
    pub reallocint:     Duration,
    #[serde(default,deserialize_with = "util::deserialize_bool")]
    pub dontstore:      bool,
    #[serde(default,deserialize_with = "util::deserialize_bool")]
    pub rejectart:      bool,
    #[serde(skip)]
    last_spool:         u8,
}
pub type MetaSpoolCfg = MetaSpool;

/// Configuration for one spool instance.
#[derive(Deserialize,Default,Debug,Clone)]
pub struct SpoolCfg {
    pub backend:    String,
    pub path:       String,
    #[serde(deserialize_with = "util::option_deserialize_size")]
    pub minfree:    Option<u64>,
}

/// Article storage (spool) functionality.
pub struct Spool {
    spool:      HashMap<u8, Box<SpoolBackend>>,
    metaspool:  Mutex<Vec<MetaSpool>>,
}

impl Spool {
    /// initialize all storage backends.
    pub fn new(spoolcfg: &HashMap<String, SpoolCfg>, metaspool: &Vec<MetaSpool>) -> io::Result<Spool> {

        // parse spool definitions.
        let mut m = HashMap::new();
        for (num, cfg) in spoolcfg {
            let n = match num.parse::<u8>() {
                Ok(n) if n < 100 => n,
                _ => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("[spool.{}]: invalid spool number", num)));
                },
            };

            let be = match cfg.backend.as_ref() {
                "diablo" => {
                    diablo::DSpool::new(&cfg, n)
                },
                e => {
                    Err(io::Error::new(io::ErrorKind::InvalidData,
                                   format!("[spool.{}]: unknown backend {}", num, e)))
                },
            }?;
            m.insert(n as u8, be);
        }
        Ok(Spool{
            spool:      m,
            metaspool:  Mutex::new(metaspool.to_vec()),
        })
    }

    /// Open one article. Based on ArtLoc, it finds the
    /// right spool, and returns a "Read" handle.
    pub fn open(&self, art_loc: &ArtLoc, part: ArtPart) -> io::Result<Box<io::Read>> {
        let be = match self.spool.get(&art_loc.spool as &u8) {
            None => {
                return Err(io::Error::new(io::ErrorKind::NotFound,
                                   format!("spool {} not found", art_loc.spool)));
            },
            Some(be) => be,
        };
        be.open(art_loc, part)
    }

    /// save one article.
    pub fn write(&mut self, art: &[u8], hdr_len: usize, head_only: bool) -> io::Result<ArtLoc> {
        unimplemented!()
    }/*
        // XXX should return an error here, really.
        if self.metaspool.len() == 0 {
            return Ok(ArtLoc{
                storage_type:   Backend::Reject,
                spool:          0,
                token:          Vec::new(),
            });
        }

        // XXX for now just take the first metaspool
        let mut m = 
    */
}
