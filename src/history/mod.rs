//! History database functionality. Supports multiple history file types.
//!
//! Types currently supported:
//!   - diablo
//!   - memdb
//!

use std::sync::Arc;

use futures_cpupool::{self,CpuPool};
use futures::{Future,future};

mod cache;
pub mod diablo;
pub mod memdb;

use std::io;
use std::path::Path;

use spool;
use self::cache::HCache;

const PRECOMMIT_MAX_AGE: u32 = 10;
const PRESTORE_MAX_AGE: u32 = 60;

type HistStatusFuture = Future<Item=Option<HistStatus>, Error=io::Error> + Send;

/// Functionality a backend history database needs to make available.
pub trait HistBackend: Send + Sync {
    /// Look an entry up in the history database.
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt>;
    /// Store an entry (an existing entry will be updated).
    fn store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()>;
}

/// History database functionality.
#[derive(Clone)]
pub struct History {
    inner:      Arc<HistoryInner>,
}

struct HistoryInner {
    cache:      HCache,
    backend:    Box<HistBackend>,
    cpu_pool:   CpuPool,
}

/// One history entry.
#[derive(Debug, Clone)]
pub struct HistEnt {
    /// Present, Expired, etc.
    pub status:     HistStatus,
    /// Unixtime when article was received.
    pub time:       u64,
    /// Whether this article was received via MODE HEADFEED.
    pub head_only:  bool,
    /// Location in the article spool.
    pub location:   Option<spool::ArtLoc>,
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

/// Returned as error by some of the methods.
#[derive(Debug)]
pub enum HistError {
    IoError(io::Error),
    Status(HistStatus),
}

impl History {

    /// Open history database.
    pub fn open(tp: &str, path: impl AsRef<Path>, threads: Option<usize>) -> io::Result<History> {
        let h : Box<HistBackend> = match tp {
            "diablo" => {
                Box::new(diablo::DHistory::open(path.as_ref())?)
            },
            "memdb" => {
                Box::new(memdb::MemDb::new())
            },
            s => {
                Err(io::Error::new(io::ErrorKind::InvalidData, s.to_string()))?
            },
        };

        let mut builder = futures_cpupool::Builder::new();
        builder.name_prefix("history-");
        builder.pool_size(threads.unwrap_or(32));

        Ok(History{
            inner:  Arc::new(HistoryInner{
                cache:  HCache::new(),
                backend:    h,
                cpu_pool:   builder.create(),
            })
        })
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
                        error!("cache_lookup: entry for {} in state HistStatus::Writing too old: {}s",
                               msgid, age);
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
    pub fn lookup(&self, msgid: &str) -> impl Future<Item=Option<HistEnt>, Error=io::Error> + Send {
        let msgid = msgid.to_string().into_bytes();
        let inner = self.inner.clone();
        self.inner.cpu_pool.spawn_fn(move || {
            use std::thread;
            trace!("history worker on thread {:?}", thread::current().id());
            match inner.backend.lookup(&msgid) {
                Ok(he) => {
                    if he.status == HistStatus::NotFound {
                        future::ok(None)
                    } else {
                        future::ok(Some(he))
                    }
                },
                Err(e) => {
                    // we simply log an error and return not found.
                    warn!("backend_lookup: {}", e);
                    future::ok(None)
                },
            }
        })
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
    pub fn check(&self, msgid: &str) -> Box<HistStatusFuture> {
        self.do_check(msgid, HistStatus::Tentative)
    }

    /// Article received. Call this before writing to history / spool.
    ///
    /// Much like the check method, but succeeds when the cache entry
    /// is Tentative, and sets the cache entry to status Writing.
    ///
    /// Returns future::ok if we succesfully set the cache entry to state Writing,
    /// future::err(HistError) otherwise.
    pub fn store_begin(&self, msgid: &str) -> Box<Future<Item=(), Error=HistError> + Send> {
        let fut = self.do_check(msgid, HistStatus::Writing)
            .then(|res| {
                match res {
                    Err(e) => future::err(HistError::IoError(e)),
                    Ok(h) => match h {
                        None|
                        Some(HistStatus::NotFound) => future::ok(()),
                        Some(s) => future::err(HistError::Status(s)),
                    }
                }
            });
        Box::new(fut)
    }

    // Function that does the actual work for check / store_begin.
    fn do_check(&self, msgid: &str, what: HistStatus) -> Box<HistStatusFuture> {

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
                    partition.store_tentative(what);
                    None
                },
                Some(HistStatus::Writing) => Some(HistStatus::Tentative),
                hs @ Some(_) => hs,
                None => break,
            };
            return Box::new(future::ok(f));
        }

        // No cache entry. Check the actual history database.
        let this = self.clone();
        let msgid2 = msgid.to_string();
        let f = self.lookup(msgid)
            .map(move |he| {
                match he {
                    Some(he) => Some(he.status),
                    None => {
                        // Not present. Try to put a tentative entry in the cache.
                        let mut partition = this.inner.cache.lock_partition(&msgid2);
                        match this.cache_lookup(&mut partition, &msgid2) {
                            None|
                            Some(HistStatus::NotFound) => {
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
                }
            });
        Box::new(f)
    }

    /// Done writing to the spool. Update the cache-entry and write-through
    /// to the actual history database backend.
    pub fn store_commit(&self, msgid: &str, he: HistEnt) -> impl Future<Item=(), Error=io::Error> {
        {
            let mut partition = self.inner.cache.lock_partition(msgid);
            if partition.store_commit(he.clone()) == false {
                warn!("cache store_commit {} failed (fallen out of cache)", msgid);
            }
        }
        let inner = self.inner.clone();
        let msgid = msgid.to_string().into_bytes();
        let f = self.inner.cpu_pool.spawn_fn(move || {
            match inner.backend.store(&msgid, &he) {
                Ok(()) => future::ok(()),
                Err(e) => future::err(e),
            }
        });
        f
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
    use std::sync::Once;
    use std::time::SystemTime;
    use env_logger;
    use super::*;

    static START: Once = Once::new();
    pub(crate) fn logger_init() {
        START.call_once(|| env_logger::init() );
    }

    #[test]
    fn test_init() {
        logger_init();
    }

    #[test]
    fn test_simple() {
        logger_init();
        debug!("history test");
        let mut h = History::open("memdb", "[memdb]").unwrap();
        let he = HistEnt{
            status:     HistStatus::Tentative,
            time:       SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            head_only:  false,
            location:   None,
        };
        let msgid = "<12345@abcde>";
        h.store_begin(msgid);
        let fut = h.store_commit(msgid, he.clone())
            .then(|res| {
                match res {
                    Err(e) => panic!("store_commit: {:?}", e),
                    Ok(v) => assert!(v == true),
                }
                res
            })
            .then(|res| {
                if let Err(e) = res {
                    panic!("store msgid: {:?}", e);
                }
                Box::new(h.lookup(msgid))
            })
            .then(|res| {
                match res {
                    Err(e) => panic!("lookup msgid: {:?}", e),
                    Ok(Some(ref e)) => assert!(e.time == he.time),
                    Ok(None) => panic!("lookup msgid: None result"),
                }
                debug!("final result: {:?}", res);
                res
            });
        fut.wait().expect("future returned error");
	}
}
