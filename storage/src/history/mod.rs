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
    inner:      RwLock<InnerHistory>,
    pc_cache:   RwLock<HCache>,
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
        let h = match tp {
            "diablo" => {
                diablo::DHistory::open(path.as_ref())
            },
            s => {
                Err(io::Error::new(io::ErrorKind::InvalidData, s.to_string()))
            },
        }?;
        Ok(History{
            inner:  RwLock::new(InnerHistory{
                backend:    Box::new(h),
                cache:      HCache::new(64000, 300),
            }),
            pc_cache:   RwLock::new(HCache::new(8000, 30)),
        })
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

    /// Like lookup, but also uses the precommit-cache.
    /// If an entry is found in the precommit-cache and it
    /// is not too old, return it. If no entry is found at
    /// all, put an entry into the precommit-cache.
    pub fn check<T: AsRef<[u8]>>(&self, msgid: T) -> io::Result<HistEnt> {

        let id = msgid.as_ref();
        {
            // check precommit-cache.
            let p = self.pc_cache.read().unwrap();
            let pc_cache = &*p;

            let he = pc_cache.lookup(id);
            if he.status == HistStatus::Tentative {
                return Ok(he);
            }
        }

        let i = self.inner.read().unwrap();
        let inner = &*i;

        // check history cache.
        let he = inner.cache.lookup(id);
        if he.status != HistStatus::NotFound {
            return Ok(he)
        }

        // check history database.
        let he = inner.backend.lookup(id)?;
        if he.status == HistStatus::NotFound {

            // not found, so insert a tentative entry into
            // the precommit-cache.
            let mut p = self.pc_cache.write().unwrap();
            let pc_cache = &mut *p;
            pc_cache.store(id, &HistEnt{
                status:     HistStatus::Tentative,
                time:       0,
                head_only:  false,
                location:   None,
            });
        }

        Ok(he)
    }

    pub fn store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        let mut i = self.inner.write().unwrap();
        let inner = &mut *i;
        inner.cache.store(msgid, he);
        inner.backend.store(msgid, he)
    }
}

