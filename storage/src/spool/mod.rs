//! Spool (article storage) functionality. Supports multiple spool types.
//!
//! Types currently supported:
//!   - diabo

use std;
use std::io;
use std::collections::HashMap;

mod diablo;

#[derive(Deserialize, Debug)]
pub struct SpoolCfg {
    pub backend:    String,
    pub path:       String,
    pub minfree:    Option<String>,
}

#[derive(Debug)]
#[repr(u8)]
pub enum Backend {
    Diablo = 0,
    Unknown = 255,
}

pub struct Token {
    pub storage_type:   Backend,
    pub spool:          u8,
    pub token:          Vec<u8>,
}

pub(crate) trait SpoolBackend {
    fn get_type(&self) -> Backend;
    fn open(&self, token: &Token, head_only: bool) -> io::Result<Box<io::Read>>;
}

/// Article storage (spool) functionality.
pub struct Spool {
    spool:      HashMap<u8, Box<SpoolBackend>>,
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Token {{ storage_type: {:?}, spool: {}, token: [", self.storage_type, self.spool)?;
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
            let be = match cfg.backend.as_ref() {
                "diablo" => {
                    diablo::DSpool::new(cfg)
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

    pub fn open(&self, token: &Token, head_only: bool) -> io::Result<Box<io::Read>> {
        let be = match self.spool.get(&token.spool as &u8) {
            None => {
                return Err(io::Error::new(io::ErrorKind::NotFound,
                                   format!("spool {} not found", token.spool)));
            },
            Some(be) => be,
        };
        be.open(token, head_only)
    }
}

