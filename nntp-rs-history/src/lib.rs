//! History database functionality. Supports multiple history file types.
//!
//! Types currently supported:
//!   - diablo
//!

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
extern crate byteorder;
extern crate time;
extern crate linked_hash_map;
extern crate nntp_rs_spool;

mod cache;
mod diablo;

use std::io;
use std::sync::{Mutex,RwLock};
use std::path::Path;
use std::collections::HashSet;

use nntp_rs_spool as spool;
use cache::HCache;

pub(crate) trait HistBackend: Send + Sync {
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt>;
    fn store(&mut self, msgid: &[u8], he: &HistEnt) -> io::Result<()>;
}

/// History database functionality.
pub struct History {
    inner:          RwLock<InnerHistory>,
    upgrade_lock:   Mutex<bool>,
}

struct CachedHistory {
    backend:    Box<HistBackend>,
    cache:      HCache,
}

struct InnerHistory {
    db:         CachedHistory,
    pc_cache:   HCache,
    writing:    HashSet<Vec<u8>>,
}

/// One history entry.
#[derive(Debug, Clone)]
pub struct HistEnt {
    pub status:     HistStatus,
    pub time:       u64,
    pub head_only:  bool,
    pub location:   Option<spool::ArtLoc>,
}

/// Status of a history entry.
#[derive(Debug, Clone, PartialEq)]
pub enum HistStatus {
    /// article is present
    Present,
    /// article not present yet but being received right now.
    Tentative,
    /// article not present.
    NotFound,
    /// article was present but has expired.
    Expired,
    /// article was offered but was rejected.
    Rejected,
}

impl CachedHistory {
    fn lookup(&self, msgid: &[u8], now: u64) -> io::Result<HistEnt> {
        let he = self.cache.lookup(msgid, now);
        if he.status != HistStatus::NotFound {
            return Ok(he)
        }
        self.backend.lookup(msgid)
    }

    fn store(&mut self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        self.backend.store(msgid, he)?;
        self.cache.store(msgid, he);
        Ok(())
    }
}

impl History {
    /// Open history database.
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
            inner: RwLock::new(InnerHistory{
                db:         CachedHistory{
                                backend:    Box::new(h),
                                cache:      HCache::new(64000, 300),
                },
                pc_cache:   HCache::new(8000, 30),
                writing:    HashSet::new(),
            }),
            upgrade_lock:  Mutex::new(true),
        })
    }

    /// Find an entry in the history database.
    pub fn lookup<T: AsRef<[u8]>>(&self, msgid: T) -> io::Result<HistEnt> {
        let inner = &*self.inner.read().unwrap();
        let now = time::now_utc().to_timespec().sec as u64;
        inner.db.lookup(msgid.as_ref(), now)
    }

    /// Check if we want to receive an article (by message-id).
    ///
    /// Returns the status of the entry in the database.
    /// If not present, mark the message-id in the precommit
    /// database as being received imminently.
    pub fn check<T: AsRef<[u8]>>(&self, msgid: T) -> io::Result<HistStatus> {

        let mut upgrade_lock = None;
        let msgid = msgid.as_ref();
        let now;

        {
            let inner = &* self.inner.read().unwrap();

            // check if this article is being written to spool right now.
            if inner.writing.contains(msgid) {
                return Ok(HistStatus::Tentative);
            }

            now = time::now_utc().to_timespec().sec as u64;

            // check precommit-cache.
            let he = inner.pc_cache.lookup(msgid, now);
            if he.status == HistStatus::Tentative {
                return Ok(HistStatus::Tentative);
            }

            // check history db
            let he = inner.db.lookup(msgid, now)?;
            if he.status != HistStatus::NotFound {
                return Ok(he.status);
            }

            // not found, so insert a tentative entry into
            // the precommit-cache. Need to upgrade the read lock
            // to a read-write lock first. That means jumping
            // through a few hoops until we have NLL.
            let m = self.upgrade_lock.lock().unwrap();
            upgrade_lock.get_or_insert(m);
        }

        let inner = &mut *self.inner.write().unwrap();
        let he = HistEnt{
            status:     HistStatus::Tentative,
            time:       now,
            head_only:  false,
            location:   None,
        };
        inner.pc_cache.store(msgid, &he);

        Ok(HistStatus::NotFound)
    }

    /// Try to reserve an entry in the history database.
    ///
    /// - check if this article is not already in the process of
    ///   being written to the spool.
    /// - then check if message-id already exists in history file
    /// - finally mark article as "being written now".
    ///
    /// return true if caller can go ahead.
    ///
    /// caller can go ahead and save the article to a spool, then
    /// call store_commit(), or store_rollback() in case of failure.
    ///
    pub fn store_reserve(&self, msgid: &[u8]) -> io::Result<(bool)> {

        let mut upgrade_lock = None;

        {
            let inner = &* self.inner.read().unwrap();

            // check if article is already being written to spool
            if inner.writing.contains(msgid) {
                return Ok(false);
            }

            // check history database.
            let now = time::now_utc().to_timespec().sec as u64;
            let he = inner.db.lookup(msgid, now)?;
            if he.status != HistStatus::NotFound {
                return Ok(false);
            }

            // not yet in the history db, so mark article as
            // in the process of being written to the spool.
            let m = self.upgrade_lock.lock().unwrap();
            upgrade_lock.get_or_insert(m);
        }

        let inner = &mut *self.inner.write().unwrap();
        inner.writing.insert(msgid.to_vec());

        Ok(true)
    }

    /// Called to roll back a "reserve" call.
    pub fn store_unreserve(&self, msgid: &[u8]) {
        let inner = &mut *self.inner.write().unwrap();
        inner.writing.remove(msgid);
        inner.pc_cache.remove(msgid);
    }

    /// Commit an entry to the history database.
    pub fn store_final(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        let inner = &mut *self.inner.write().unwrap();
        inner.writing.remove(msgid);
        inner.pc_cache.remove(msgid);
        inner.db.store(msgid, he)
    }
}

