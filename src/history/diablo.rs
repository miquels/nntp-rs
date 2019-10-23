use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::io;
use std::io::{Read, Seek, Write};
use std::mem;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use fs2::FileExt as _;
use parking_lot::Mutex;

use crate::blocking::{try_read_at, BlockingPool, BlockingType};
use crate::history::{HistBackend, HistEnt, HistStatus};
use crate::spool;
use crate::util::byteorder::*;
use crate::util::DHash;
use crate::util::{self, MmapAtomicU32};

/// Diablo compatible history file.
#[derive(Debug)]
pub struct DHistory {
    inner:         ArcSwap<DHistoryInner>,
    blocking_pool: BlockingPool,
    blocking_type: Option<BlockingType>,
    num_threads:   Option<usize>,
}

// Most of the (synchronous) work is done by methods of DHistoryInner.
// The async methods of DHistory call them using a threadpool.
#[derive(Debug)]
struct DHistoryInner {
    path:         PathBuf,
    rfile:        fs::File,
    wfile:        Mutex<WFile>,
    hash_buckets: MmapAtomicU32,
    hash_size:    u32,
    hash_mask:    u32,
    data_offset:  u64,
}

// Internal file writer status.
#[derive(Debug)]
struct WFile {
    wpos:        u64,
    file:        fs::File,
    invalidated: bool,
}

// Header of the history file.
#[derive(Debug)]
#[repr(C)]
struct DHistHead {
    magic:        u32,
    hash_size:    u32,
    version:      u16,
    histent_size: u16,
    head_size:    u16,
    resv:         u16,
}
const DHISTHEAD_SIZE: usize = 16;
const DHISTHEAD_MAGIC: u32 = 0xA1B2C3D4;
const DHISTHEAD_VERSION2: u16 = 2;
#[allow(dead_code)]
const DHISTHEAD_DEADMAGIC: u32 = 0xDEADF5E6;

impl DHistory {
    /// open existing history database
    /// Note: blocking.
    pub fn open(path: &Path, threads: Option<usize>, bt: Option<BlockingType>) -> io::Result<DHistory> {
        let inner = DHistoryInner::open(path)?;
        Ok(DHistory {
            inner:         ArcSwap::from(Arc::new(inner)),
            blocking_type: bt.clone(),
            num_threads:   threads.clone(),
            blocking_pool: BlockingPool::new(bt, threads.unwrap_or(16)),
        })
    }

    /// Create a new history database. Fails if it is already in use (aka locked).
    /// If the file already exists, but we managed to lock it, just truncate it and go ahead.
    pub fn create(path: &Path, num_buckets: u32) -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // try to lock it, then truncate.
        if let Err(_) = file.try_lock_exclusive() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "in use"));
        }
        file.set_len(0)?;

        // write header
        let head = DHistHead {
            magic:        DHISTHEAD_MAGIC,
            hash_size:    num_buckets,
            version:      DHISTHEAD_VERSION2,
            histent_size: DHISTENT_SIZE as u16,
            head_size:    DHISTHEAD_SIZE as u16,
            resv:         0,
        };
        let buf: [u8; DHISTHEAD_SIZE] = unsafe { mem::transmute(head) };
        file.write(&buf)?;

        // write empty hash table, plus first empty histent.
        let buf = [0u8; 65536];
        let mut todo = (num_buckets * 4 + DHISTENT_SIZE as u32) as usize;
        while todo > 0 {
            let w = if todo > buf.len() { buf.len() } else { todo };
            todo -= file.write(&buf[0..w])?;
        }

        Ok(())
    }

    // Expire.
    async fn do_expire(&self, spool: spool::Spool, remember: u64, no_rename: bool) -> io::Result<()> {
        let inner_arcswap = self.inner.clone();
        let inner = self.inner.load().clone();

        self.blocking_pool
            .spawn_fn(move || inner.do_expire(inner_arcswap, spool, remember, no_rename))
            .await
    }

    // History lookup.
    async fn do_lookup(&self, msgid: &[u8]) -> io::Result<HistEnt> {
        let inner = self.inner.load();

        // Find first element of the chain.
        let hv = DHash::hash(msgid);
        let bucket = ((hv.h1 ^ hv.h2) & inner.hash_mask) as u64;
        let idx = inner.hash_buckets.load(bucket as usize);

        // First, we try walking the chain using non-blocking reads. If one
        // of the reads fails with EWOULDBLOCK we pass the whole thing off to
        // the threadpool.
        let res = match inner.walk_chain(idx, hv.clone(), msgid, try_read_dhistent_at) {
            Ok(Err(idx)) => {
                // failed at 'idx' with EWOULDBLOCK, continue with threadpool.
                let inner2 = inner.clone();
                let msgid = msgid.to_vec();
                self.blocking_pool
                    .spawn_fn(move || inner2.walk_chain(idx, hv, msgid, read_dhistent_at))
                    .await
            },
            other => other,
        };

        // Unpack the result.
        let dhe = match res {
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(HistEnt {
                        time:      0,
                        status:    HistStatus::NotFound,
                        head_only: false,
                        location:  None,
                    });
                } else {
                    return Err(e);
                }
            },
            Ok(Ok(he)) => he,
            _ => unreachable!(),
        };

        Ok(HistEnt {
            time:      dhe.when(),
            status:    dhe.status(),
            head_only: dhe.head_only(),
            location:  dhe.to_location(),
        })
    }

    async fn do_store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        // Call try_store in a loop. Normally it will succeed right  away, but it
        // will return "NotConnected" if the history file got changed beneath us
        // and then we try again.
        for _ in 0..100 {
            match self.try_store(msgid, he).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if e.kind() != io::ErrorKind::NotConnected {
                        return Err(e);
                    }
                },
            }
        }
        // should never happen
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "history.store: failed to reload history file",
        ))
    }

    // Try to store an entry for this msgid.
    // It can fail on I/O, or transiently fail because the history file has been replaced.
    async fn try_store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        let inner = self.inner.load().clone();

        let hv = DHash::hash(msgid);
        let dhe = DHistEnt::new(he, hv, 0);

        self.blocking_pool
            .spawn_fn(move || {
                // lock and validate.
                let mut wfile = inner.wfile.lock();
                if wfile.invalidated {
                    Err(io::ErrorKind::NotConnected)?;
                }

                // write it
                inner.write_dhistent(&mut wfile, dhe)
            })
            .await
    }
}

impl HistBackend for DHistory {
    /// lookup an article in the DHistory database
    fn lookup<'a>(
        &'a self,
        msgid: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<HistEnt>> + Send + 'a>>
    {
        Box::pin(self.do_lookup(msgid))
    }

    /// store an article in the DHistory database
    fn store<'a>(
        &'a self,
        msgid: &'a [u8],
        he: &'a HistEnt,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>
    {
        Box::pin(self.do_store(msgid, he))
    }

    /// expire the DHistory database.
    fn expire<'a>(
        &'a self,
        spool: &'a spool::Spool,
        remember: u64,
        no_rename: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>
    {
        Box::pin(self.do_expire(spool.clone(), remember, no_rename))
    }
}

impl DHistoryInner {
    /// open existing history database
    fn open(path: &Path) -> io::Result<DHistoryInner> {
        // open file, lock it, get the meta info, and clone a write handle.
        let f = fs::OpenOptions::new().read(true).write(true).open(path)?;
        if let Err(_) = f.try_lock_exclusive() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "in use"));
        }
        let meta = f.metadata()?;
        let w = f.try_clone()?;

        // read DHistHead and validate it.
        let dhh = read_dhisthead_at(&f, 0)?;
        debug!("{:?} - dhisthead: {:?}", path, &dhh);
        if dhh.magic != DHISTHEAD_MAGIC ||
            dhh.version != DHISTHEAD_VERSION2 ||
            dhh.histent_size as usize != DHISTENT_SIZE ||
            dhh.head_size as usize != DHISTHEAD_SIZE
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{:?}: not a dhistory file", path),
            ));
        }

        // consistency check.
        let data_offset = DHISTHEAD_SIZE as u64 + (dhh.hash_size * 4) as u64;
        if meta.len() < data_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{:?}: too small", path),
            ));
        }
        if (meta.len() - data_offset) % DHISTENT_SIZE as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{:?}: data misaligned", path),
            ));
        }

        // mmap the hash table index.
        let buckets = MmapAtomicU32::new(&f, DHISTHEAD_SIZE as u64, dhh.hash_size as usize)
            .map_err(|e| io::Error::new(e.kind(), format!("{:?}: mmap failed: {}", path, e)))?;

        Ok(DHistoryInner {
            path:         path.to_owned(),
            rfile:        f,
            wfile:        Mutex::new(WFile {
                wpos:        meta.len(),
                file:        w,
                invalidated: false,
            }),
            hash_buckets: buckets,
            hash_size:    dhh.hash_size,
            hash_mask:    dhh.hash_size - 1,
            data_offset:  data_offset,
        })
    }

    // Walk the chain of entries for this hashtable index.
    fn walk_chain<F>(
        &self,
        mut idx: u32,
        hv: DHash,
        msgid: impl AsRef<[u8]>,
        mut read_dhistent_at: F,
    ) -> io::Result<Result<DHistEnt, u32>>
    where
        F: FnMut(&fs::File, u64, Option<&[u8]>) -> io::Result<DHistEnt>,
    {
        let mut counter = 0;
        let mut found = false;
        let mut dhe: DHistEnt = Default::default();

        while idx != 0 {
            let pos = self.data_offset + (idx as u64) * (DHISTENT_SIZE as u64);
            dhe = match read_dhistent_at(&self.rfile, pos, Some(msgid.as_ref())) {
                Ok(dhe) => dhe,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(Err(idx));
                    } else {
                        return Err(e);
                    }
                },
            };
            if dhe.hv == hv {
                found = true;
                break;
            }
            idx = dhe.next;
            counter += 1;
            if counter > 5000 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "database loop"));
            }
        }
        if !found {
            Err(io::ErrorKind::NotFound)?;
        }
        Ok(Ok(dhe))
    }

    // add a DHistEnt to the history database.
    fn write_dhistent(&self, wfile: &mut WFile, mut dhe: DHistEnt) -> io::Result<()> {
        // calculate bucket.
        let bucket = ((dhe.hv.h1 ^ dhe.hv.h2) & self.hash_mask) as usize;
        dhe.next = self.hash_buckets.load(bucket as usize);

        // Calculate hashtable index and write pos.
        // Align it at a multiple of DHISTENT_SIZE after data_offset.
        // Kind of paranoid, really.
        let mut pos = wfile.wpos;
        pos -= self.data_offset;
        let idx = (pos + DHISTENT_SIZE as u64 - 1) / DHISTENT_SIZE as u64;
        let pos = idx * DHISTENT_SIZE as u64 + self.data_offset;

        // append to file.
        let buf: [u8; DHISTENT_SIZE] = unsafe { mem::transmute(dhe) };
        wfile.file.write_at(&buf, pos)?;
        wfile.wpos = pos + DHISTENT_SIZE as u64;

        // update hash index
        self.hash_buckets.store(bucket, idx as u32);

        Ok(())
    }

    // this is for bulk updates. used by do_expire().
    fn write_dhistent_append<W>(&self, file: &mut W, mut dhe: DHistEnt, pos: u64) -> io::Result<u64>
    where W: io::Write {
        // calculate bucket.
        let bucket = ((dhe.hv.h1 ^ dhe.hv.h2) & self.hash_mask) as usize;
        dhe.next = self.hash_buckets.load(bucket as usize);

        // calculate index.
        let idx = (pos + DHISTENT_SIZE as u64 - 1) / DHISTENT_SIZE as u64;

        // append to file.
        let buf: [u8; DHISTENT_SIZE] = unsafe { mem::transmute(dhe) };
        file.write(&buf)?;

        // update hash index
        self.hash_buckets.store(bucket, idx as u32);

        Ok(pos + DHISTENT_SIZE as u64)
    }

    // continue reading from a DHistory file, and writing to a new DHistory file.
    fn expire_incremental(
        &self,
        new: &DHistoryInner,
        rpos: u64,
        spool_oldest: &HashMap<u8, u64>,
        remember: u64,
        first: bool,
    ) -> io::Result<u64>
    {
        // seek to the end, clone filehandle, and buffer it.
        let mut w = new.wfile.lock();
        let mut wpos = w.file.seek(io::SeekFrom::End(0))?;
        let mut wfile = io::BufWriter::with_capacity(8000 * DHISTENT_SIZE, w.file.try_clone()?);

        // clone filehandle of current file and seek to the first entry.
        let mut rfile = io::BufReader::with_capacity(8000 * DHISTENT_SIZE, self.rfile.try_clone()?);
        let mut rpos = rpos;
        rfile.seek(io::SeekFrom::Start(rpos))?;
        let rsize = self.rfile.metadata().map(|m| m.len()).unwrap();

        let now = util::unixtime();
        let mut last_tm = now;
        let mut last_pct = 0;

        let mut kept = 0u64;
        let mut marked = 0u64;
        let mut removed = 0u64;

        // rebuild history file.
        loop {
            let mut did_mark = false;

            // read entry.
            let mut dhe = match read_dhistent(&mut rfile) {
                Ok(None) => break,
                Ok(Some(dhe)) => dhe,
                Err(e) => {
                    let _ = fs::remove_file(&new.path);
                    return Err(e);
                },
            };
            rpos += DHISTENT_SIZE as u64;

            // validate entry.
            let when = dhe.when();
            let mut expired = true;
            if dhe.status() == HistStatus::Present {
                // Not expired yet, see if it should be.
                if let Some(spool) = dhe.spool() {
                    // valid spool. get minimum age.
                    if let Some(&oldest) = spool_oldest.get(&spool) {
                        if when >= oldest {
                            // still valid.
                            expired = false;
                        }
                    }
                }
                if expired {
                    did_mark = true;
                    dhe.set_status(HistStatus::Expired);
                }
            }

            // only write the entry if it is not expired or if it still is within "remember".
            if !expired || when >= now - remember {
                kept += 1;
                if did_mark {
                    marked += 1;
                }
                wpos = new.write_dhistent_append(&mut wfile, dhe, wpos)?;
            } else {
                removed += 1;
            }

            // status update while we're running.

            if (kept + removed) % 10000 == 0 && last_tm + 10 < util::unixtime() {
                last_pct = (100 * rpos) / rsize;
                last_tm = util::unixtime();
                info!("expire {:?}: {}%", self.path, last_pct);
            }
        }

        // flush data to disk.
        let wfile = wfile.into_inner()?;
        wfile.sync_all()?;

        if last_pct > 0 && last_pct < 100 {
                info!("expire {:?}: 100%", self.path);
        }

        if first || removed > 0 || marked > 0 {
            let i = if first { "" } else { " (incremental)" };
            info!("expire {:?}{}: removed {} entries, kept {} entries, marked {} entries as expired",
                self.path, i, removed, kept, marked);
        }

        Ok(rpos)
    }

    // See if we need to expire. The first entry in the history file is the
    // oldest. If it has a timestamp _before_ any of the spool_oldest,
    // we need to expire the history file.
    fn need_expire(&self, spool_oldest: &HashMap<u8, u64>) -> bool {
        // get first entry (actually, second. First is a zero-entry).
        let pos = self.data_offset + DHISTENT_SIZE as u64;
        if let Ok(dhe) = read_dhistent_at(&self.rfile, pos, None) {
            for tm in spool_oldest.values() {
                if dhe.when() < *tm {
                    return true;
                }
            }
        }
        false
    }

    // Expire.
    fn do_expire(
        &self,
        arcswap: ArcSwap<DHistoryInner>,
        spool: spool::Spool,
        remember: u64,
        no_rename: bool,
    ) -> io::Result<()>
    {
        // get age of oldest article for each spool.
        let spool_oldest = spool.get_oldest();
        if !self.need_expire(&spool_oldest) {
            info!("expire {:?}: no expire needed", self.path);
            return Ok(());
        }

        info!("expire {:?}: start", self.path);

        // open dhistory.new.
        let mut new_path = self.path.clone();
        new_path.set_extension("new");
        DHistory::create(&new_path, self.hash_size)?;
        let mut new_inner = DHistoryInner::open(&new_path)?;

        let mut rpos = self.data_offset + DHISTENT_SIZE as u64;

        // Call expire_incremental a couple of times. The first time will take the longest
        // since we need to sync the entire new history file to disk. After that it will
        // get faster. Try until there are less than 1000 entries left, for a maximum of 5 times.
        for i in 0..5 {
            rpos = self.expire_incremental(&new_inner, rpos, &spool_oldest, remember, i == 0)?;
            let meta = self.rfile.metadata()?;
            if rpos + 1000 * (DHISTENT_SIZE as u64) > meta.len() {
                break;
            }
        }

        // The code below until the end of the closure is a critical section,
        // it blocks all other threads that try to write to the history file.
        // Reading from the history file goes uninterrupted.

        // Get a write lock on the current file.
        let mut wfile = self.wfile.lock();

        // process entries that were added in the mean time.
        self.expire_incremental(&new_inner, rpos, &spool_oldest, remember, false)?;

        if no_rename {
            info!("expire {:?}: done, new file is {:?}", self.path, new_path);
            return Ok(())
        }

        // first link "dhistory" to "dhistory.old"
        let mut old_path = self.path.clone();
        old_path.set_extension("old");
        let _ = fs::remove_file(&old_path);
        fs::hard_link(&self.path, &old_path)?;

        // then rename "dhistory.new" to "dhistory", so that we always have a valid "dhistory" file.
        fs::rename(&new_path, &self.path).map_err(|e| {
            let _ = fs::remove_file(&old_path);
            e
        })?;

        // and swap the reference.
        new_inner.path = self.path.clone();
        arcswap.swap(Arc::new(new_inner));
        wfile.invalidated = true;

        info!("expire {:?}: done", self.path);

        Ok(())
    }
}

// NOTE: "exp" is encoded as:
// - lower 8 bits: spool + 100
// - bits 9-12: storage type (0x0 == diablo)
// - bits 12-15:
//      BIT         NON-DIABLO  DIABLO
//      0x1000      1           0
//      0x2000      rejected    -
//      0x4000      expired     iter == 0xffff ? rejected : expired
//      0x8000      head-only   head-only
//
// If this is a diablo-spool entry, the fields are exactly what they say.
// For other spool types, iter, boffset, bsize are just 10 bytes that
// can be used for any encoding whatsoever.
//
#[derive(Debug, Default)]
#[repr(C)]
#[rustfmt::skip]
struct DHistEnt {
    next:       u32,        // link to next entry
    gmt:        u32,        // unixtime / 60
    hv:         DHash,      // hash
    iter:       u16,        // filename (B.04x)
    exp:        u16,        // see above
    boffset:    u32,        // article offset
    bsize:      u32,        // article size
}
const DHISTENT_SIZE: usize = 28;

impl DHistEnt {
    // Encode a new DHistEnt.
    fn new(he: &HistEnt, hv: DHash, next: u32) -> DHistEnt {
        let mut dhe: DHistEnt = Default::default();
        dhe.next = next;
        dhe.hv = hv;
        dhe.gmt = (he.time / 60) as u32;

        let mut storage_type = spool::Backend::Diablo;
        if let Some(ref loc) = he.location {
            storage_type = loc.storage_type;
            dhe.iter = u16_from_le_bytes(&loc.token[0..2]);
            dhe.boffset = u32_from_le_bytes(&loc.token[2..6]);
            dhe.bsize = u32_from_le_bytes(&loc.token[6..10]);
            dhe.exp = if loc.spool < 100 {
                loc.spool as u16 + 100
            } else {
                0xff
            };
        }

        if he.head_only {
            dhe.exp |= 0x8000;
        }

        if storage_type == spool::Backend::Diablo {
            if let Some(ref loc) = he.location {
                dhe.gmt = u32_from_le_bytes(&loc.token[10..14]);
            }
            match he.status {
                HistStatus::Expired => {
                    dhe.exp |= 0x4000;
                },
                HistStatus::Rejected => {
                    dhe.exp |= 0x4000;
                    dhe.iter = 0xffff;
                },
                _ => {},
            }
        } else {
            dhe.exp |= 0x1000 | (((storage_type as u8) & 0x0f) as u16) << 8;
            match he.status {
                HistStatus::Expired => {
                    dhe.exp |= 0x4000;
                },
                HistStatus::Rejected => {
                    dhe.exp = 0x2000;
                },
                _ => {},
            }
        }
        dhe
    }

    // convert a DHistEnt to a spool::ArtLoc
    fn to_location(&self) -> Option<spool::ArtLoc> {
        // validate.
        if self.status() != HistStatus::Present {
            return None;
        }
        let spool = self.spool()?;

        let mut t = [0u8; 14];
        u16_write_le_bytes(&mut t[0..2], self.iter);
        u32_write_le_bytes(&mut t[2..6], self.boffset);
        u32_write_le_bytes(&mut t[6..10], self.bsize);

        let s = if (self.exp & 0x1000) != 0 {
            // not a diablo-spool history entry.
            (&t)[0..10].to_vec()
        } else {
            // This is a diablo-spool history entry, so include
            // gmt in the returned StorageToken
            u32_write_le_bytes(&mut t[10..14], self.gmt);
            (&t)[0..14].to_vec()
        };
        let btype = spool::Backend::from_u8(((self.exp & 0x0f00) >> 8) as u8);
        Some(spool::ArtLoc {
            storage_type: btype,
            spool:        spool,
            token:        s,
        })
    }

    // return status of this DHistEnt as history::HistStatus.
    fn status(&self) -> HistStatus {
        if self.gmt == 0 {
            return HistStatus::NotFound;
        }
        if (self.exp & 0x1000) != 0 {
            // diablo type entry.
            if (self.exp & 0x4000) != 0 {
                if self.iter == 0xffff {
                    HistStatus::Rejected
                } else {
                    HistStatus::Expired
                }
            } else {
                let spool = self.exp & 0xff;
                if spool >= 100 && spool < 200 {
                    HistStatus::Present
                } else {
                    // Not sure if "rejected" is right.. this looks
                    // like a corrupt entry. But what can you do?
                    HistStatus::Rejected
                }
            }
        } else {
            // non-diablo type entry.
            if (self.exp & 0x4000) != 0 {
                HistStatus::Expired
            } else if (self.exp & 0x2000) != 0 {
                HistStatus::Rejected
            } else {
                let spool = self.exp & 0xff;
                if spool >= 100 && spool < 200 {
                    HistStatus::Present
                } else {
                    // See remark above.
                    HistStatus::Rejected
                }
            }
        }
    }

    // Set status.
    fn set_status(&mut self, status: HistStatus) {
        if (self.exp & 0x1000) == 0 {
            // diablo type entry.
            match status {
                HistStatus::Present => {
                    if self.spool().is_some() {
                        self.exp &= !0x4000u16;
                    }
                },
                HistStatus::Rejected => {
                    self.exp |= 0x4000;
                    self.iter = 0xffff;
                },
                HistStatus::Expired => {
                    self.exp |= 0x4000;
                },
                _ => {},
            }
        } else {
            // non-diablo type entry.
            match status {
                HistStatus::Present => {
                    if self.spool().is_some() {
                        self.exp &= !0x6000u16;
                    }
                },
                HistStatus::Rejected => {
                    self.exp |= 0x2000;
                    self.iter = 0xffff;
                },
                HistStatus::Expired => {
                    self.exp |= 0x4000;
                },
                _ => {},
            }
        }
    }

    // Get the "when" of this entry.
    fn when(&self) -> u64 {
        self.gmt as u64 * 60
    }

    // is this a head-only entry.
    fn head_only(&self) -> bool {
        (self.exp & 0x8000) > 0
    }

    // Get the spool number.
    fn spool(&self) -> Option<u8> {
        let spool = (self.exp & 0xff) as u8;
        if self.status() == HistStatus::Present && spool >= 100 && spool < 200 {
            Some(spool - 100)
        } else {
            None
        }
    }
}

// helper
fn read_dhisthead_at(file: &fs::File, pos: u64) -> io::Result<DHistHead> {
    let mut buf = [0u8; DHISTHEAD_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTHEAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("read_dhisthead_at({}): short read ({})", pos, n),
        ));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
fn read_dhistent<F>(file: &mut F) -> io::Result<Option<DHistEnt>>
where F: Read {
    // The file might be buffered. In that case, there is no guarantee
    // that we will read exactly DHISTENT_SIZE bytes in one read. So we
    // implement the equivalent of read_exact()..
    let mut buf = [0u8; DHISTENT_SIZE];
    let mut done = 0;
    while done < DHISTENT_SIZE {
        let n = file.read(&mut buf[done..])?;
        if n == 0 {
            break;
        }
        done += n;
    }
    if done == 0 {
        return Ok(None);
    }
    if done != DHISTENT_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("read_dhistent: short read: {} bytes", done),
        ));
    }
    Ok(Some(unsafe { mem::transmute(buf) }))
}

// helper
fn read_dhistent_at(file: &fs::File, pos: u64, msgid: Option<&[u8]>) -> io::Result<DHistEnt> {
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTENT_SIZE {
        let msg = if let Some(msgid) = msgid {
            format!(
                "read_dhistent_at({}) for {:?}: short read ({})",
                pos,
                std::str::from_utf8(msgid),
                n
            )
        } else {
            format!("read_dhistent_at({}): short read ({})", pos, n)
        };
        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
#[cfg(target_os = "linux")]
fn try_read_dhistent_at(file: &fs::File, pos: u64, msgid: Option<&[u8]>) -> io::Result<DHistEnt> {
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = try_read_at(file, &mut buf, pos)?;
    if n != DHISTENT_SIZE {
        let msg = if let Some(msgid) = msgid {
            format!(
                "read_dhistent_at({}) for {:?}: short read ({})",
                pos,
                std::str::from_utf8(msgid),
                n
            )
        } else {
            format!("read_dhistent_at({}): short read ({})", pos, n)
        };
        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
    }
    Ok(unsafe { mem::transmute(buf) })
}
