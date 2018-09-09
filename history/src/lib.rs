//! History database functionality. Supports multiple history file types.
//!
//! Types currently supported:
//!   - diablo
//!

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
extern crate byteorder;
extern crate futures;
extern crate futures_cpupool;
extern crate nntp_rs_spool;
extern crate parking_lot;
extern crate time;

#[cfg(test)]
extern crate env_logger;

use std::sync::Arc;

use futures_cpupool::CpuPool;
use futures::{Future,future};

mod cache;
mod diablo;
mod memdb;

use std::io;
use std::path::Path;

use nntp_rs_spool as spool;
use cache::HCache;

const PRECOMMIT_MAX_AGE: u32 = 10;

type HistFuture = Future<Item=Option<HistEnt>, Error=io::Error> + Send;

pub(crate) trait HistBackend: Send + Sync {
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt>;
    fn store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()>;
}

/// History database functionality.
#[derive(Clone)]
pub struct History {
    inner:      Arc<HistoryInner>,
}

pub struct HistoryInner {
    cache:      HCache,
    backend:    Box<HistBackend>,
    cpu_pool:   CpuPool,
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
    /// article has been received, in flight to storage.
    Writing,
    /// article not present.
    NotFound,
    /// article was present but has expired.
    Expired,
    /// article was offered but was rejected.
    Rejected,
}

impl History {

    /// Open history database.
    pub fn open(tp: &str, path: impl AsRef<Path>) -> io::Result<History> {
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
        builder.pool_size(32);

        Ok(History{
            inner:  Arc::new(HistoryInner{
                cache:  HCache::new(),
                backend:    h,
                cpu_pool:   builder.create(),
            })
        })
    }

    // Find an entry in the history database.
    fn cache_lookup(&self, msgid: &str, check: bool, phase2: bool) -> Option<HistEnt> {
        let mut partition = self.inner.cache.lock_partition(msgid);
        if let Some((mut h, age)) = partition.lookup() {
            match h.status {
                HistStatus::Writing => {
                    if check {
                        // In the CHECK case, handle this as Tentative.
                        h.status = HistStatus::Tentative;
                    } else {
                        // Otherwise as "not found"
                        h.status = HistStatus::NotFound;
                    }
                    Some(h)
                },
                HistStatus::Tentative => {
                    if age > PRECOMMIT_MAX_AGE {
                        // Not valid as tentative entry anymore, but we can
                        // interpret it as a negative cache entry.
                        h.status = HistStatus::NotFound;
                        if check {
                            partition.store_tentative();
                        }
                    }
                    Some(h)
                },
                _ => Some(h),
            }
        } else {
            if phase2 {
                partition.store_tentative();
            }
            None
        }
    }

    // Do not really need to Box the returned future, since we only
    // ever return one type, so return impl Future. Unfortunately we cannot
    // use `impl HistFuture' as return value. So spell it out.
    pub(crate) fn backend_lookup(&self, msgid: &str) -> impl Future<Item=Option<HistEnt>, Error=io::Error> + Send {
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

    /// Find an entry in the history database.
    pub fn lookup(&self, msgid: &str) -> Box<HistFuture> {

        // First check the cache.
        if let Some(he) = self.cache_lookup(msgid, false, false) {
            let f = if he.status == HistStatus::NotFound {
                None
            } else {
                Some(he)
            };
            return Box::new(future::ok(f));
        }

        // Not in the cache. We have to do a lookup.
        Box::new(self.backend_lookup(msgid))
    }

    /// This is like `lookup', but it can return HistStatus::Tentative as well.
    /// It will also put a Tentative entry in the history cache if we did not
    /// have an entry for this message-id yet.
    pub fn check(&self, msgid: &str) -> Box<HistFuture> {

        // First check the cache.
        if let Some(he) = self.cache_lookup(msgid, true, false) {
            let f = if he.status == HistStatus::NotFound {
                None
            } else {
                Some(he)
            };
            return Box::new(future::ok(f));
        }

        // Do a lookup, and after the lookup check the cache again.
        let this = self.clone();
        let msgid2 = msgid.to_string();
        let f = self.backend_lookup(msgid)
            .map(move |he| {
                match he {
                    Some(he) => Some(he),
                    None => {
                        match this.cache_lookup(&msgid2, true, true) {
                            Some(he) => {
                                if he.status == HistStatus::NotFound {
                                    None
                                } else {
                                    Some(he)
                                }
                            },
                            None => None,
                        }
                    }
                }
            });
        Box::new(f)
    }

    /// We have received the article. Before we write it to the spool,
    /// mark it in the cache with status "Writing".
    pub fn store_begin(&mut self, msgid: &str) -> bool {
        let mut partition = self.inner.cache.lock_partition(msgid);
        if let Some((h, _age)) = partition.lookup() {
            match h.status {
                HistStatus::Present |
                HistStatus::Writing |
                HistStatus::Expired |
                HistStatus::Rejected => return false,
                HistStatus::Tentative |
                HistStatus::NotFound => {},
            }
        }
        partition.store_begin();
        true
    }

    /// Done writing to the spool. Update the cache-entry and write-through
    /// to the backend storage.
    pub fn store_commit(&self, msgid: &str, he: HistEnt) -> Box<Future<Item=bool, Error=io::Error>> {
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
                Ok(()) => future::ok(true),
                Err(e) => future::err(e),
            }
        });
        Box::new(f)
    }

    /// Something went wrong writing to the spool. Cancel the reservation
    /// in the cache.
    pub fn store_rollback(&mut self, msgid: &str) {
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
                Box::new(h.backend_lookup(msgid))
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
