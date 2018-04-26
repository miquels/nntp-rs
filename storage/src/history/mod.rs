//! History database functionality. Supports multiple history file types.
//!
//! Types currently supported:
//!   - diablo
//!

mod cache;
mod diablo;

use std::io;
use std::sync::RwLock;
use std::path::Path;

use history::cache::HCache;
use spool;

pub(crate) trait HistBackend: Send + Sync {
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt>;
    fn store(&mut self, msgid: &[u8], he: &HistEnt) -> io::Result<()>;
}

/// History database functionality.
pub struct History {
    inner:  RwLock<InnerHistory>,
}

struct InnerHistory {
    backend:    Box<HistBackend>,
    cache:      HCache,
}

/// One history entry.
#[derive(Debug, Clone)]
pub struct HistEnt {
    pub status:     HistStatus,
    pub time:       u64,
    pub head_only:  bool,
    pub location:   Option<spool::ArtLoc>,
}

/// Status of entry.
#[derive(Debug, Clone, PartialEq)]
pub enum HistStatus {
    Present,
    Tentative,
    NotFound,
    Expired,
    Rejected,
}

impl History {
    /// Open history file.
    pub fn open<T: AsRef<Path>>(tp: &str, path: T) -> io::Result<History> {
        match tp {
            "diablo" => {
                let h = diablo::DHistory::open(path.as_ref())?;
                Ok(History{
                    inner:  RwLock::new(InnerHistory{
                        backend:    Box::new(h),
                        cache:      HCache::new(32000, 300),
                    })
                })
            },
            s => {
                Err(io::Error::new(io::ErrorKind::InvalidData, s.to_string()))
            }
        }
    }

    /// Find an entry in the history file.
    pub fn lookup<T: AsRef<[u8]>>(&self, msgid: T) -> io::Result<HistEnt> {
        let i = self.inner.read().unwrap();
        let inner = &*i;
        let id = msgid.as_ref();
        let he = inner.cache.lookup(id);
        if he.status != HistStatus::NotFound {
            return Ok(he)
        }
        inner.backend.lookup(msgid.as_ref())
    }

    pub fn store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        let mut i = self.inner.write().unwrap();
        let inner = &mut *i;
        inner.cache.store(msgid, he);
        inner.backend.store(msgid, he)
    }
}

