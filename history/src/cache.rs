use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::SystemTime;

use parking_lot::{Mutex,MutexGuard};

use {HistEnt,HistStatus};

// Constants that define how the cache behaves.
const NUM_PARTITIONS: u32 = 32;
const CACHE_BUCKETS: u32 = 128;
const CACHE_LISTLEN: u32 = 16;
const CACHE_MAX_AGE: u32 = 300;

/// FIFO size and time limited cache.
/// The cache is partitioned for parallel access.
pub struct HCache {
    partitions:         Vec<Mutex<FifoMap>>,
    num_partitions:     u32,
}

// helper.
fn hash_str(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    let h = hasher.finish();
    trace!("hash({}) -> {}", s, h);
    h
}

// another helper.
fn unixtime() -> u32 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32
}

impl HCache {

    /// Return a new HCache. Using hardcoded params for now (defined at the top of this file):
    ///
    /// - 32 partitions
    /// - 128 buckets per partition
    /// - max 16 entries per bucket.
    ///
    /// This totals 64K entries, good for 30 seconds of cache with
    /// current usenet feed peak rates (2000/sec, sep 2018).
    ///
    /// TODO: We need metrics.
    pub fn new() -> HCache {

        let mut partitions = Vec::new();
        for _ in 0..NUM_PARTITIONS {
            let map = FifoMap::new(CACHE_BUCKETS, CACHE_LISTLEN, CACHE_MAX_AGE);
            partitions.push(Mutex::new(map));
        }

        HCache{
            partitions:         partitions,
            num_partitions:     NUM_PARTITIONS,
        }
    }

    /// Get and lock partition of the cache. With the returned HCachePartition
    /// you can then get/insert HistEnt items in the history and precommit cache.
    pub fn lock_partition(&self, msgid: &str) -> HCachePartition {
        let h = hash_str(msgid);
        let m = h % (self.num_partitions as u64);
        HCachePartition{
            inner:              self.partitions[m as usize].lock(),
            hash:               h,
            when:               unixtime(),
        }
    }
}

/// Returned by HCache.lock_partition(). Serves as a handle and a mutex guard.
/// As long as this struct is not dropped, the cache partition remains locked.
pub struct HCachePartition<'a> {
    inner:              MutexGuard<'a, FifoMap>,
    hash:               u64,
    when:               u32,
}

impl<'a> HCachePartition<'a> {

    /// find an entry in the history cache.
    pub fn lookup(&self) -> Option<(HistEnt, u32)> {
        let inner = &*self.inner;
        match inner.get(self.hash, self.when) {
            Some(dhe) => Some((dhe.histent, self.when - dhe.when)),
            None => None,
        }
    }

    /// add a tentative entry to the history cache.
    pub fn store_tentative(&mut self) {
        let he = HistEnt{
            status:     HistStatus::Tentative,
            time:       self.when as u64,
            head_only:  false,
            location:   None,
        };
        let inner = &mut *self.inner;
        inner.insert(HCacheEnt{
            histent: he,
            hash: self.hash,
            when: self.when,
        });
    }

    /// like tentative, but when writing the article.
    pub fn store_begin(&mut self) {
        let he = HistEnt{
            status:     HistStatus::Writing,
            time:       self.when as u64,
            head_only:  false,
            location:   None,
        };
        let inner = &mut *self.inner;
        inner.insert(HCacheEnt{
            histent: he,
            hash: self.hash,
            when: self.when,
        });
    }

    /// make entry permanent.
    pub fn store_commit(&mut self, he: HistEnt) -> bool {
        let inner = &mut *self.inner;
        inner.update(HCacheEnt{
            histent: he,
            hash: self.hash,
            when: self.when,
        })
    }

    /// remove entry.
    pub fn store_rollback(&mut self) {
        let inner = &mut *self.inner;
        inner.remove(self.hash, self.when);
    }
}

// Internal cache entry.
#[derive(Clone)]
struct HCacheEnt {
    histent:    HistEnt,
    hash:       u64,
    when:       u32,
}

// Simple FIFO hashmap. Might replace with LinkedHashMap.
struct FifoMap {
    buckets:        Vec<VecDeque<HCacheEnt>>,
    num_buckets:        u32,
    max_listlen:        u32,
    max_age:            u32,
}

// This is the inner map. Really simple, it has N buckets, each bucket has
// a VecDeque as a list, and we push new entries on the front and pop old
// entries from the back.
impl FifoMap {
    fn new(num_buckets: u32, max_listlen: u32, max_age: u32) -> FifoMap {
        let mut buckets = Vec::with_capacity(num_buckets as usize);
        for _ in 0..num_buckets {
            buckets.push(VecDeque::with_capacity(max_listlen as usize));
        }
        FifoMap{ buckets, num_buckets, max_listlen, max_age }
    }

    // Get a mutable entry.
    fn get_mut(&mut self, hash: u64, now: u32) -> Option<&mut HCacheEnt> {
        let bi = ((((hash >> 16) & 0xffffffff) as u32) % self.num_buckets) as usize;
        for b in self.buckets[bi].iter_mut() {
            if b.when + self.max_age < now + 1 {
                break;
            }
            if b.hash == hash {
                return Some(b);
            }
        }
        None
    }

    // Get an entry.
    fn get(&self, hash: u64, now: u32) -> Option<HCacheEnt> {
        let bi = ((((hash >> 16) & 0xffffffff) as u32) % self.num_buckets) as usize;
        for b in self.buckets[bi].iter() {
            if b.when + self.max_age < now + 1 {
                break;
            }
            if b.hash == hash {
                return Some(b.clone());
            }
        }
        None
    }

    // Insert an entry.
    fn insert(&mut self, ent: HCacheEnt) {
        let bi = ((((ent.hash >> 16) & 0xffffffff) as u32) % self.num_buckets) as usize;
        trace!("FifoMap.insert hash={} now={} bi={}", ent.hash, ent.when, bi);
        self.buckets[bi].push_front(ent);
        if self.buckets[bi].len() > self.max_listlen as usize {
            self.buckets[bi].pop_back();
        }
    }

    // Update an entry.
    fn update(&mut self, ent: HCacheEnt) -> bool {
        let mut do_insert = false;
        let r = match self.get_mut(ent.hash, ent.when) {
            Some(oent) => {
                if oent.when >= ent.when - 1 {
                    // if it's recent enough just update existing entry
                    oent.histent = ent.histent.clone();
                } else {
                    // otherwise insert a new one at the head.
                    // Must do that outside of this block because of the
                    // borrow checker - will probably be fixed when we have NLL.
                    do_insert = true;
                }
                true
            },
            None => false,
        };
        if do_insert {
            self.insert(ent);
        }
        r
    }

    // Remove an entry.
    fn remove(&mut self, hash: u64, now: u32) {
        match self.get_mut(hash, now) {
            Some(ent) => {
                ent.histent.status = HistStatus::NotFound;
            },
            None => {},
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use HistStatus;

    #[test]
    fn test_simple() {
        ::tests::logger_init();
        debug!("test_simple()");
        let cache = HCache::new();
        let histent = HistEnt{
            status:     HistStatus::Present,
            time:       unixtime() as u64,
            head_only:  false,
            location:   None,
        };
        let msgid = "<test@123>";
        {
            let mut p = cache.lock_partition(msgid.clone());
            let h = histent.clone();
            p.store_begin();
            p.store_commit(h);
        }
        let p = cache.lock_partition(msgid);
        let (he, _age) = p.lookup().unwrap();
        assert!(he.time == histent.time);
    }

    #[test]
    fn test_full() {
        ::tests::logger_init();
        debug!("test_full()");
        let cache = HCache::new();
        let histent = HistEnt{
            status:     HistStatus::Present,
            time:       unixtime() as u64,
            head_only:  false,
            location:   None,
        };

        // fill up the cache to the limit.
        for i in 0 .. NUM_PARTITIONS*CACHE_BUCKETS*CACHE_LISTLEN {
            let msgid = format!("<{}@bla>", i);
            let mut h = histent.clone();
            let mut p = cache.lock_partition(&msgid);
            p.store_begin();
            p.store_commit(h);
        }
        // first entry should still be there
        let first_msgid = format!("<{}@bla>", 0);
        {
            let p = cache.lock_partition(&first_msgid);
            let (he, _age) = p.lookup().unwrap();
            assert!(he.time == histent.time);
        }
        // add a bunch more.
        for i in 0 .. NUM_PARTITIONS*CACHE_BUCKETS*CACHE_LISTLEN {
            let msgid = format!("<{}@bla2>", i);
            let mut h = histent.clone();
            let mut p = cache.lock_partition(&msgid);
            p.store_begin();
            p.store_commit(h);
        }
        // first entry should be gone.
        {
            let p = cache.lock_partition(&first_msgid);
            assert!(p.lookup().is_none());
        }
    }
}

