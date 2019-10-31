use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use parking_lot::{Mutex, MutexGuard};

use crate::util::UnixTime;
use super::{HistEnt, HistStatus};

// Constants that define how the cache behaves.
pub const NUM_PARTITIONS: u32 = 32;
pub const CACHE_BUCKETS: u32 = 16384;
pub const CACHE_MAX_AGE: u32 = 300;

/// LRU size and time limited cache.
/// The cache is partitioned for parallel access.
pub struct HCache {
    partitions:     Vec<Mutex<LruMap>>,
    num_partitions: u32,
}

// helper.
#[inline]
fn hash_str(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[inline]
fn unixtime() -> u32 {
    crate::util::UnixTime::coarse().as_secs() as u32
}

impl HCache {
    /// Return a new HCache. Using hardcoded params for now (defined at the top of this file):
    ///
    /// - 32 partitions
    /// - 4096 entries per partition
    /// - max age 5 minutes.
    ///
    /// This totals 128K entries, good for 60 seconds of cache with
    /// current usenet feed peak rates (2000/sec, sep 2018).
    pub fn new() -> HCache {
        let mut partitions = Vec::new();
        for _ in 0..NUM_PARTITIONS {
            let map = LruMap::new(CACHE_BUCKETS, CACHE_MAX_AGE);
            partitions.push(Mutex::new(map));
        }

        HCache {
            partitions:     partitions,
            num_partitions: NUM_PARTITIONS,
        }
    }

    /// Get and lock partition of the cache. With the returned HCachePartition
    /// you can then get/insert HistEnt items in the history and precommit cache.
    pub fn lock_partition(&self, msgid: &str) -> HCachePartition {
        let h = hash_str(msgid);
        let m = h % (self.num_partitions as u64);
        HCachePartition {
            inner: self.partitions[m as usize].lock(),
            hash:  h,
            when:  unixtime(),
        }
    }
}

/// Returned by HCache.lock_partition(). Serves as a handle and a mutex guard.
/// As long as this struct is not dropped, the cache partition remains locked.
pub struct HCachePartition<'a> {
    inner: MutexGuard<'a, LruMap>,
    hash:  u64,
    when:  u32,
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
    pub fn store_tentative(&mut self, what: HistStatus) {
        let he = HistEnt {
            status:    what,
            time:      UnixTime::from_secs(self.when as u64),
            head_only: false,
            location:  None,
        };
        let inner = &mut *self.inner;
        inner.insert(HCacheEnt {
            histent: he,
            hash:    self.hash,
            when:    self.when,
        });
    }

    /// update entry (to go from tentative to writing).
    pub fn store_update(&mut self, what: HistStatus) {
        let he = HistEnt {
            status:    what,
            time:      UnixTime::from_secs(self.when  as u64),
            head_only: false,
            location:  None,
        };
        let inner = &mut *self.inner;
        inner.update(HCacheEnt {
            histent: he,
            hash:    self.hash,
            when:    self.when,
        });
    }

    /// make entry permanent.
    pub fn store_commit(&mut self, he: HistEnt) -> bool {
        let inner = &mut *self.inner;
        inner.update(HCacheEnt {
            histent: he,
            hash:    self.hash,
            when:    self.when,
        })
    }

    /// remove entry.
    pub fn store_rollback(&mut self) {
        let inner = &mut *self.inner;
        inner.remove(self.hash)
    }
}

// Internal cache entry.
#[derive(Clone)]
struct HCacheEnt {
    histent: HistEnt,
    hash:    u64,
    when:    u32,
}

// We create our own Hasher that always hashes an u64
// into the exact same u64 (hey, it's a perfect hash!).
use std::convert::TryInto;
use std::hash::BuildHasher;

struct MyBuildHasher;
impl BuildHasher for MyBuildHasher {
    type Hasher = MyHasher;
    fn build_hasher(&self) -> Self::Hasher {
        MyHasher(0)
    }
}

#[derive(PartialEq, Eq)]
struct MyHasher(u64);
impl Hasher for MyHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0 = u64::from_ne_bytes(bytes.try_into().unwrap());
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

use linked_hash_map::LinkedHashMap;

// Simple FIFO hashmap.
struct LruMap {
    map:         LinkedHashMap<u64, HCacheEnt, MyBuildHasher>,
    num_buckets: u32,
    max_age:     u32,
}

// This is the inner map. Really simple, it has N buckets, each bucket has
// a VecDeque as a list, and we push new entries on the front and pop old
// entries from the back.
impl LruMap {
    fn new(num_buckets: u32, max_age: u32) -> LruMap {
        LruMap {
            map:         LinkedHashMap::with_capacity_and_hasher(num_buckets as usize, MyBuildHasher {}),
            num_buckets: num_buckets,
            max_age:     max_age,
        }
    }

    // Get an entry.
    fn get(&self, hash: u64, now: u32) -> Option<HCacheEnt> {
        match self.map.get(&hash) {
            Some(e) if e.when + self.max_age > now => Some(e.clone()),
            _ => None,
        }
    }

    // Insert an entry.
    fn insert(&mut self, ent: HCacheEnt) {
        if self.map.len() >= self.num_buckets as usize {
            if let Some((_, oldest)) = self.map.pop_front() {
                // ent.when == "now" for all intents and purposes.
                if oldest.when + self.max_age > ent.when && oldest.histent.status == HistStatus::Writing {
                    // This is a very unlikely race condition, the entry expiring
                    // from the LRU cache while we're writing it to disk, but
                    // if it happens, just move it to the front again.
                    self.map.insert(oldest.hash, oldest);
                }
            }
        }
        self.map.insert(ent.hash, ent);
    }

    // Update an entry.
    fn update(&mut self, ent: HCacheEnt) -> bool {
        if self.map.get_refresh(&ent.hash).is_none() {
            return false;
        }
        if let Some(e) = self.map.get_mut(&ent.hash) {
            // ent.when == "now" for all intents and purposes.
            if e.when + self.max_age > ent.when {
                *e = ent;
                return true;
            }
        }
        false
    }

    // Remove an entry.
    fn remove(&mut self, hash: u64) {
        if let Some(ent) = self.map.get_mut(&hash) {
            ent.histent.status = HistStatus::NotFound;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::HistStatus;
    use crate::util::UnixTime;

    #[test]
    fn test_simple() {
        debug!("test_simple()");
        let cache = HCache::new();
        let histent = HistEnt {
            status:    HistStatus::Present,
            time:      UnixTime::coarse(),
            head_only: false,
            location:  None,
        };
        let msgid = "<test@123>";
        {
            let mut p = cache.lock_partition(msgid.clone());
            let h = histent.clone();
            p.store_tentative(h.status);
            p.store_commit(h);
        }
        let p = cache.lock_partition(msgid);
        let (he, _age) = p.lookup().unwrap();
        assert!(he.time == histent.time);
    }

    #[test]
    fn test_full() {
        debug!("test_full()");
        let cache = HCache::new();
        let histent = HistEnt {
            status:    HistStatus::Present,
            time:      UnixTime::coarse(),
            head_only: false,
            location:  None,
        };

        // fill up the cache to the limit.
        for i in 0..NUM_PARTITIONS * CACHE_BUCKETS {
            let msgid = format!("<{}@bla>", i);
            let h = histent.clone();
            let mut p = cache.lock_partition(&msgid);
            p.store_tentative(h.status);
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
        for i in 0..NUM_PARTITIONS * CACHE_BUCKETS {
            let msgid = format!("<{}@bla2>", i);
            let h = histent.clone();
            let mut p = cache.lock_partition(&msgid);
            p.store_tentative(h.status);
            p.store_commit(h);
        }
        // first entry should be gone.
        {
            let p = cache.lock_partition(&first_msgid);
            assert!(p.lookup().is_none());
        }
    }
}
