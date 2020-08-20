//! History database functionality. Supports multiple history file types.
//!
//! Types currently supported:
//!   - diablo
//!   - memdb
//!

use std::pin::Pin;
use std::sync::Arc;

mod cache;
pub mod diablo;
pub mod memdb;

use std::future::Future;
use std::io;
use std::path::Path;
use std::time::Duration;

use self::cache::HCache;
use crate::spool;
use crate::util::BlockingType;
use crate::util::UnixTime;

const PRECOMMIT_MAX_AGE: u32 = 10;
const PRESTORE_MAX_AGE: u32 = 60;

/// Functionality a backend history database needs to make available.
pub trait HistBackend: Send + Sync {
    /// Look an entry up in the history database.
    fn lookup<'a>(
        &'a self,
        msgid: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<HistEnt>> + Send + 'a>>;
    /// Store an entry (an existing entry will be updated).
    fn store<'a>(
        &'a self,
        msgid: &'a [u8],
        he: &'a HistEnt,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
    /// Do an expire run on the history file.
    fn expire<'a>(
        &'a self,
        spool: &'a spool::Spool,
        remember: u64,
        no_rename: bool,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
    /// Inspect the history file (debugging).
    fn inspect<'a>(
        &'a self,
        spool: &'a spool::Spool,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

/// History database functionality.
#[derive(Clone)]
pub struct History {
    inner: Arc<HistoryInner>,
}

struct HistoryInner {
    cache:   HCache,
    backend: Box<dyn HistBackend>,
}

/// One history entry.
#[derive(Debug, Clone)]
pub struct HistEnt {
    /// Present, Expired, etc.
    pub status:    HistStatus,
    /// Unixtime when article was received.
    pub time:      UnixTime,
    /// Whether this article was received via MODE HEADFEED.
    pub head_only: bool,
    /// Location in the article spool.
    pub location:  Option<spool::ArtLoc>,
}

impl HistEnt {
    pub fn to_json(&self, spool: &spool::Spool) -> serde_json::Value {
        let mut obj = serde_json::json!({
            "status": self.status.name(),
            "time": &self.time.to_string(),
            "head_only": &self.head_only,
        });
        if let Some(ref loc) = self.location {
            let obj_mut = obj.as_object_mut().unwrap();
            for (k, v) in loc.to_json(spool).as_object().unwrap().iter() {
                obj_mut.insert(k.to_owned(), v.to_owned());
            }
        }
        obj
    }
}

/// Status of a history entry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HistStatus {
    /// article is present
    Present,
    /// article not present yet but being received right now.
    Tentative,
    /// article has been received, in flight to storage.
    Writing,
    /// article not present.
    NotFound,
    /// article was present but has expired.
    Expired,
    /// article was offered but was rejected.
    Rejected,
}

impl HistStatus {
    /// Short name of this state as &str
    pub fn name(&self) -> &'static str {
        match self {
            HistStatus::Present => "present",
            HistStatus::Tentative => "tentative",
            HistStatus::Writing => "writing",
            HistStatus::NotFound => "notfound",
            HistStatus::Expired => "expired",
            HistStatus::Rejected => "rejected",
        }
    }
}

/// Returned as error by some of the methods.
#[derive(Debug)]
pub enum HistError {
    IoError(io::Error),
    Status(HistStatus),
}

impl History {
    /// Open history database.
    pub fn open(
        tp: &str,
        path: impl AsRef<Path>,
        rw: bool,
        threads: Option<usize>,
        bt: BlockingType,
    ) -> io::Result<History>
    {
        let h: Box<dyn HistBackend> = match tp {
            "diablo" => Box::new(diablo::DHistory::open(path.as_ref(), rw, threads, bt)?),
            "memdb" => Box::new(memdb::MemDb::new()),
            s => Err(io::Error::new(io::ErrorKind::InvalidData, s.to_string()))?,
        };

        Ok(History {
            inner: Arc::new(HistoryInner {
                cache:   HCache::new(),
                backend: h,
            }),
        })
    }

    pub async fn expire(
        &self,
        spool: &spool::Spool,
        remember: Duration,
        no_rename: bool,
        force: bool,
    ) -> io::Result<()>
    {
        let remember = if remember.as_secs() == 0 {
            86400
        } else {
            remember.as_secs()
        };
        self.inner.backend.expire(spool, remember, no_rename, force).await
    }

    pub async fn inspect(&self, spool: &spool::Spool) -> io::Result<()> {
        self.inner.backend.inspect(spool).await
    }

    // Do a lookup in the history cache. Just a wrapper around partition.lookup()
    // that rewrites tentative cache entries that are too old to NotFound,
    // and that returns a HistStatus instead of HistEnt.
    fn cache_lookup(&self, partition: &mut cache::HCachePartition, msgid: &str) -> Option<HistStatus> {
        if let Some((mut h, age)) = partition.lookup() {
            match h.status {
                HistStatus::Writing => {
                    if age > PRESTORE_MAX_AGE {
                        // This should never happen, but if it does, log it,
                        // and invalidate the entry.
                        log::error!(
                            "cache_lookup: entry for {} in state HistStatus::Writing too old: {}s",
                            msgid,
                            age
                        );
                        h.status = HistStatus::NotFound;
                    }
                    Some(h.status)
                },
                HistStatus::Tentative => {
                    if age > PRECOMMIT_MAX_AGE {
                        // Not valid as tentative entry anymore, but we can
                        // interpret it as a negative cache entry.
                        h.status = HistStatus::NotFound;
                    }
                    Some(h.status)
                },
                _ => Some(h.status),
            }
        } else {
            None
        }
    }

    /// Find an entry in the history database. This lookup ignores the
    /// write-cache, it goes straight to the backend.
    pub async fn lookup(&self, msgid: &str) -> Result<Option<HistEnt>, io::Error> {
        match self.inner.backend.lookup(msgid.as_bytes()).await {
            Ok(he) => {
                if he.status == HistStatus::NotFound {
                    Ok(None)
                } else {
                    Ok(Some(he))
                }
            },
            Err(e) => {
                // we simply log an error and return not found.
                log::warn!("backend_lookup: {}", e);
                Ok(None)
            },
        }
    }

    /// The CHECK command.
    ///
    /// Lookup a message-id through the cache and the history db. If not found
    /// or invalid, mark the message-id in the cache as Tentative.
    ///
    /// Returns:
    /// - None: message-id not found
    /// - HistStatus::Tentative - message-id already tentative in cache
    /// - HistEnt with any other status - message-id already present
    ///
    pub async fn check(&self, msgid: &str) -> Result<Option<HistStatus>, io::Error> {
        self.do_check(msgid, HistStatus::Tentative).await
    }

    /// Article received. Call this before writing to history / spool.
    ///
    /// Much like the check method, but succeeds when the cache entry
    /// is Tentative, and sets the cache entry to status Writing.
    ///
    /// Returns Ok(()) if we succesfully set the cache entry to state Writing,
    /// Err(HistError) otherwise.
    pub async fn store_begin(&self, msgid: &str) -> Result<(), HistError> {
        match self.do_check(msgid, HistStatus::Writing).await {
            Err(e) => Err(HistError::IoError(e)),
            Ok(h) => {
                match h {
                    None | Some(HistStatus::NotFound) => Ok(()),
                    Some(s) => Err(HistError::Status(s)),
                }
            },
        }
    }

    // Function that does the actual work for check / store_begin.
    async fn do_check(&self, msgid: &str, what: HistStatus) -> Result<Option<HistStatus>, io::Error> {
        // First check the cache. HistStatus::NotFound means it WAS found in
        // the cache as negative entry, so we do not need to go check
        // the actual history db!
        loop {
            let mut partition = self.inner.cache.lock_partition(msgid);
            let f = match self.cache_lookup(&mut partition, msgid) {
                Some(HistStatus::NotFound) => {
                    partition.store_tentative(what);
                    None
                },
                Some(HistStatus::Tentative) if what == HistStatus::Writing => {
                    partition.store_update(what);
                    None
                },
                Some(HistStatus::Writing) => Some(HistStatus::Tentative),
                hs @ Some(_) => hs,
                None => break,
            };
            return Ok(f);
        }

        // No cache entry. Check the actual history database.
        let res = match self.lookup(msgid).await? {
            Some(he) => Some(he.status),
            None => {
                // Not present. Try to put a tentative entry in the cache.
                let mut partition = self.inner.cache.lock_partition(msgid);
                match self.cache_lookup(&mut partition, msgid) {
                    None | Some(HistStatus::NotFound) => {
                        partition.store_tentative(what);
                        None
                    },
                    Some(HistStatus::Tentative) if what == HistStatus::Writing => {
                        partition.store_tentative(what);
                        None
                    },
                    Some(HistStatus::Writing) => Some(HistStatus::Tentative),
                    hs @ Some(_) => hs,
                }
            },
        };
        Ok(res)
    }

    /// Done writing to the spool. Update the cache-entry and write-through
    /// to the actual history database backend.
    pub async fn store_commit(&self, msgid: &str, he: HistEnt) -> Result<(), io::Error> {
        {
            let mut partition = self.inner.cache.lock_partition(msgid);
            if partition.store_commit(he.clone()) == false {
                log::warn!("cache store_commit {} failed (fallen out of cache)", msgid);
            }
        }
        self.inner.backend.store(msgid.as_bytes(), &he).await
    }

    /// Something went wrong writing to the spool. Cancel the reservation
    /// in the cache.
    pub fn store_rollback(&self, msgid: &str) {
        let mut partition = self.inner.cache.lock_partition(msgid);
        partition.store_rollback()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    // run tests using
    //
    //      RUST_LOG=nntp_rs_history=debug cargo test -- --nocapture
    //
    //  to enable debug logging.
    use super::*;
    use env_logger;
    use std::sync::Once;

    static START: Once = Once::new();
    pub(crate) fn logger_init() {
        START.call_once(|| env_logger::init());
    }

    #[test]
    fn test_init() {
        logger_init();
    }

    async fn test_simple_async() -> Result<Option<HistEnt>, io::Error> {
        let h = History::open("memdb", "[memdb]", true, None, BlockingType::Blocking).unwrap();
        let he = HistEnt {
            status:    HistStatus::Tentative,
            time:      crate::util::UnixTime::now(),
            head_only: false,
            location:  None,
        };
        let msgid = "<12345@abcde>";
        let res = h.store_begin(msgid).await;
        if let Err(e) = res {
            panic!("store_begin: {:?}", e);
        }
        let res = h.store_commit(msgid, he.clone()).await;
        if let Err(e) = res {
            panic!("store_commit: {:?}", e);
        }
        let res = h.lookup(msgid).await;
        match res {
            Err(e) => panic!("lookup msgid: {:?}", e),
            Ok(Some(ref e)) => assert!(e.time == he.time),
            Ok(None) => panic!("lookup msgid: None result"),
        }
        log::debug!("final result: {:?}", res);
        res
    }

    #[test]
    fn test_simple() {
        use futures::executor::block_on;
        logger_init();
        block_on(test_simple_async()).expect("future returned error");
    }
}
