//! History database functionality. Supports multiple history file types.
//!
//! Types currently supported:
//!   - diablo
//!

mod diablo;

use std::io;
use std::path::Path;

use spool;

pub(crate) trait HistBackend {
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt>;
}

/// History database functionality.
pub struct History {
    inner:  Box<HistBackend>,
}

/// One history entry.
#[derive(Debug)]
pub struct HistEnt {
    pub status:     HistStatus,
    pub time:       u64,
    pub head_only:  bool,
    pub token:      Option<spool::Token>,
}

/// Status of entry.
#[derive(Debug, PartialEq)]
pub enum HistStatus {
    NotFound,
    Found,
    Expired,
    Rejected,
    Tentative,
}

impl History {
    /// Open history file.
    pub fn open<T: AsRef<Path>>(tp: &str, path: T) -> io::Result<History> {
        match tp {
            "diablo" => {
                let h = diablo::DHistory::open(path.as_ref())?;
                Ok(History{
                    inner:  Box::new(h),
                })
            },
            s => {
                Err(io::Error::new(io::ErrorKind::InvalidData, s.to_string()))
            }
        }
    }

    /// Find an entry in the history file.
    pub fn lookup<T: AsRef<[u8]>>(&self, msgid: T) -> io::Result<HistEnt> {
        self.inner.lookup(msgid.as_ref())
    }
}

