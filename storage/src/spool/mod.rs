//! Spool (article storage) functionality. Supports multiple spool types.
//!
//! Types currently supported:
//!   - diabo

use std;
use std::io;
use std::collections::HashMap;

mod diablo;

#[derive(Deserialize, Debug, Clone)]
pub struct SpoolCfg {
    spool_no:       u8,
    pub backend:    String,
    pub path:       String,
    pub minfree:    Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct MetaSpoolCfg {
    pub spool:          Vec<String>,
    pub groups:         String,
    pub maxsize:        String,
    pub reallocint:     String,
    pub dontstore:      String,
    pub rejectarts:     String,
}

#[derive(Debug,PartialEq,Clone,Copy)]
#[repr(u8)]
pub enum Backend {
    Diablo = 0,
    Unknown = 255,
}

#[derive(Debug,Clone,Copy,PartialEq)]
pub enum ArtPart {
    Body,
    Head,
    Article,
}

#[derive(Clone)]
pub struct ArtLoc {
    pub storage_type:   Backend,
    pub spool:          u8,
    pub token:          Vec<u8>,
}

pub(crate) trait SpoolBackend: Send + Sync {
    fn get_type(&self) -> Backend;
    fn open(&self, art_loc: &ArtLoc, part: ArtPart) -> io::Result<Box<io::Read>>;
    fn write(&self, art: &[u8], hdr_len: usize, head_only: bool) -> io::Result<ArtLoc>;
}

/// Article storage (spool) functionality.
pub struct Spool {
    spool:      HashMap<u8, Box<SpoolBackend>>,
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

impl Backend {
    /// convert u8 to enum.
    pub fn from_u8(t: u8) -> Backend {
        match t {
            0 => Backend::Diablo,
            _ => Backend::Unknown,
        }
    }
}

impl Spool {
    /// initialize all storage backends.
    pub fn new(spoolcfg: &HashMap<String, SpoolCfg>) -> io::Result<Spool> {

        let mut m = HashMap::new();

        for (num, cfg) in spoolcfg {
            let n = num.parse::<i8>().unwrap_or(-1);
            if n < 0 || n > 99 {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("[spool.{}]: invalid spool number", num)));
            }
            let mut cfg2 = cfg.to_owned();
            cfg2.spool_no = n as u8;

            let be = match cfg.backend.as_ref() {
                "diablo" => {
                    diablo::DSpool::new(&cfg2)
                },
                e => {
                    Err(io::Error::new(io::ErrorKind::InvalidData,
                                   format!("[spool.{}]: unknown backend {}", num, e)))
                },
            }?;
            m.insert(n as u8, be);
        }
        Ok(Spool{
            spool:  m,
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
}

