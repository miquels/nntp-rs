use std;
use std::io;
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::mem;
use std::io::{Seek,Read,Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use fs2::FileExt as _;
use parking_lot::Mutex;

use crate::blocking::{BlockingPool, BlockingType, try_read_at};
use crate::history::{HistBackend,HistEnt,HistStatus};
use crate::spool;
use crate::util::byteorder::*;
use crate::util::{self, MmapAtomicU32};

/// Diablo compatible history file.
#[derive(Debug)]
pub struct DHistory {
    inner:          ArcSwap<DHistoryInner>,
    crc_xor_table:  &'static Vec<DHash>,
    blocking_pool:  BlockingPool,
    blocking_type:  Option<BlockingType>,
    num_threads:    Option<usize>,
}

#[derive(Debug)]
pub struct DHistoryInner {
    path:           PathBuf,
    rfile:          fs::File,
    wfile:          Mutex<fs::File>,
    hash_buckets:   MmapAtomicU32,
    hash_size:      u32,
    hash_mask:      u32,
    data_offset:    u64,
    invalidated:    bool,
}

#[derive(Debug)]
#[repr(C)]
struct DHistHead {
    magic:          u32,
    hash_size:      u32,
    version:        u16,
    histent_size:   u16,
    head_size:      u16,
    resv:           u16,
}
const DHISTHEAD_SIZE : usize = 16;
const DHISTHEAD_MAGIC : u32 = 0xA1B2C3D4;
const DHISTHEAD_VERSION2 : u16 = 2;
#[allow(dead_code)]
const DHISTHEAD_DEADMAGIC : u32 = 0xDEADF5E6;

#[derive(Debug, Default, PartialEq, Clone)]
#[repr(C)]
struct DHash {
    h1:     u32,
    h2:     u32,
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
struct DHistEnt {
    next:       u32,        // link to next entry
    gmt:        u32,        // unixtime / 60
    hv:         DHash,      // hash
    iter:       u16,        // filename (B.04x)
    exp:        u16,        // see above
    boffset:    u32,        // article offset
    bsize:      u32,        // article size
}
const DHISTENT_SIZE : usize = 28;

lazy_static! {
        static ref CRC_XOR_TABLE: Vec<DHash> = crc_init();
}

impl DHistory {

    /// open existing history database
    pub fn open(path: &Path, threads: Option<usize>, bt: Option<BlockingType>) -> io::Result<DHistory> {

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
           dhh.head_size as usize != DHISTHEAD_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      format!("{:?}: not a dhistory file", path)));
        }

        // consistency check.
        let data_offset = DHISTHEAD_SIZE as u64 + (dhh.hash_size * 4) as u64;
        if meta.len() < data_offset {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      format!("{:?}: too small", path)));
        }
        if (meta.len() - data_offset) % DHISTENT_SIZE as u64 != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      format!("{:?}: data misaligned", path)));
        }

        // mmap the hash table index.
        let buckets = MmapAtomicU32::new(&f, DHISTHEAD_SIZE as u64, dhh.hash_size as usize)
            .map_err(|e| io::Error::new(e.kind(), format!("{:?}: mmap failed: {}", path, e)))?;

        let inner = DHistoryInner{
            path:           path.to_owned(),
            rfile:          f,
            wfile:          Mutex::new(w),
            hash_buckets:   buckets,
            hash_size:      dhh.hash_size,
            hash_mask:      dhh.hash_size - 1,
            data_offset:    data_offset,
            invalidated:    false,
        };
        Ok(DHistory{
            inner:          ArcSwap::from(Arc::new(inner)),
            crc_xor_table:  &*CRC_XOR_TABLE,
            blocking_type:  bt.clone(),
            num_threads:    threads.clone(),
            blocking_pool:  BlockingPool::new(bt, threads.unwrap_or(16)),
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
        let head = DHistHead{
            magic:          DHISTHEAD_MAGIC,
            hash_size:      num_buckets,
            version:        DHISTHEAD_VERSION2,
            histent_size:   DHISTENT_SIZE as u16,
            head_size:      DHISTHEAD_SIZE as u16,
            resv:           0,
        };
        let buf : [u8; DHISTHEAD_SIZE] = unsafe { mem::transmute(head) };
        file.write(&buf)?;

        // write empty hash table, plus first empty histent.
        let buf = [0u8; 65536];
        let todo = (num_buckets * 4 + DHISTENT_SIZE as u32) as usize;
        let mut done = 0;
        while todo < done {
            let w = if todo > buf.len() { buf.len() } else { todo };
            done += file.write(&buf[0..w])?;
        }

        Ok(())
    }

    // Expire.
    fn do_expire(&self, spool: spool::Spool, remember: u64) -> io::Result<()> {

        // open dhistory.new.
        let cur_inner = self.inner.load();
        let mut new_path = cur_inner.path.clone();
        new_path.set_extension("new");
        DHistory::create(&new_path, cur_inner.hash_size)?;
        let d = DHistory::open(&new_path, self.num_threads.clone(), self.blocking_type.clone())?;
        let new_inner = d.inner.load();

        // seek to the end, clone filehandle, and buffer it.
        let mut w = new_inner.wfile.lock();
        let mut wpos = w.seek(io::SeekFrom::End(0))?;
        let mut wfile = io::BufWriter::new(w.try_clone()?);

        // clone filehandle of current file and seek to the first entry.
        let mut rfile = io::BufReader::new(cur_inner.rfile.try_clone()?);
        rfile.seek(io::SeekFrom::Start(cur_inner.data_offset + DHISTENT_SIZE as u64))?;

        // get age of oldest article for each spool.
        let spool_oldest = spool.get_oldest();
        let now = util::unixtime();

        // rebuild history file.
        loop {
            // read entry.
            let mut dhe = match read_dhistent(&mut rfile) {
                Ok(None) => break,
                Ok(Some(dhe)) => dhe,
                Err(e) => {
                    let _ = fs::remove_file(&new_path);
                    return Err(e);
                },
            };

            let when = dhe.gmt as u64 * 60;
            let mut expired = true;

            if (dhe.gmt & 0x6000) == 0 {
                // not marked as expired, see if entry should be expired.
                let spool = (dhe.exp & 0xff) as u8;
                if spool >= 100 && spool < 200 && dhe.iter != 0xffff {
                    // valid spool. get minimum age.
                    if let Some(&o) = spool_oldest.get(&(spool - 100)) {
                        // spool exists, and has a minimum age.
                        if when < o {
                            // too old, mark as expired.
                            dhe.exp |= 0x4000;
                        } else {
                            // still valid.
                            expired = false;
                        }
                    }
                }
            }

            // only write the entry if it is not expired or if it still is within "remember".
            if !expired || when >= now - remember {
                wpos = new_inner.write_dhistent_append(&mut wfile, dhe, wpos)?;
            }
        }

        // Done. Now sync data, get a write lock on the current file, process the
        // entries that were written in the mean time, then move the new
        // file into place, and make it the current history file.
        // XXX TODO FIXME
        Ok(())
    }

    // History lookup.
    async fn do_lookup(&self, msgid: &[u8]) -> io::Result<HistEnt> {

        let inner = self.inner.load();

        // Find first element of the chain.
        let hv = crc_hash(self.crc_xor_table, msgid);
        let bucket = ((hv.h1 ^hv.h2) & inner.hash_mask) as u64;
        let idx = inner.hash_buckets.load(bucket as usize);

        // First, we try walking the chain using non-blocking reads. If one
        // of the reads fails with EWOULDBLOCK we pass the whole thing off to
        // the threadpool.
        let res = match inner.walk_chain(idx, hv.clone(), msgid, try_read_dhistent_at) {
            Ok(Err(idx)) => {
                // failed at 'idx' with EWOULDBLOCK, continue with threadpool.
                let inner2 = inner.clone();
                let msgid = msgid.to_vec();
                self.blocking_pool.spawn_fn(move || {
                    inner2.walk_chain(idx, hv, msgid, read_dhistent_at)
                }).await
            },
            other => other,
        };

        // Unpack the result.
        let dhe = match res {
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(HistEnt{
                        time:       0,
                        status:     HistStatus::NotFound,
                        head_only:  false,
                        location:   None,
                    });
                } else {
                    return Err(e);
                }
            },
            Ok(Ok(he)) => he,
            _ => unreachable!(),
        };

        // see if entry is valid. if it is not, we just return Rejected
        // instead of logging an error. debatable.
        let spool = (dhe.exp & 0xff) as u8;
        let status = dhe.status();
        if status == HistStatus::Present && (idx == 0 || spool < 100 || spool > 199) {
            return Ok(HistEnt{
                time:       0,
                status:     HistStatus::Rejected,
                head_only:  (dhe.exp & 0x8000) > 0,
                location:   None,
            });
        }

        let location = match status {
            HistStatus::Present => Some(dhe.to_location()),
            _ => None,
        };
        Ok(HistEnt{
            time:       (dhe.gmt as u64) * 60,
            status:     status,
            head_only:  (dhe.exp & 0x8000) > 0,
            location:   location,
        })
    }

    async fn do_store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        // Call try_store in a loop. Normally it will succeed right  away, but it
        // will return "NotConnected" if the history file got changed beneath us
        // and then we try again.
        for _ in 0 .. 1000 {
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
        Err(io::Error::new(io::ErrorKind::InvalidData, "history.store: failed to reload history file"))
    }

    // Try to store an entry for this msgid.
    // It can fail on I/O, or transiently fail because the history file has been replaced.
    async fn try_store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {

        let inner_ref = self.inner.load();
        let inner = inner_ref.clone();

        let hv = crc_hash(self.crc_xor_table, msgid);
        let dhe = DHistEnt::new(he, hv, 0);

        self.blocking_pool.spawn_fn(move || {
            // lock and validate.
            let mut wfile = inner.wfile.lock();
            if inner.invalidated {
                Err(io::ErrorKind::NotConnected)?;
            }

            // write it
            inner.write_dhistent(&mut *wfile, dhe)

        }).await
    }
}

impl HistBackend for DHistory {

    /// lookup an article in the DHistory database
    fn lookup<'a>(&'a self, msgid: &'a[u8]) -> Pin<Box<dyn Future<Output = io::Result<HistEnt>> + Send + 'a>> {
        Box::pin(self.do_lookup(msgid))
    }

    /// store an article in the DHistry database
    fn store<'a>(&'a self, msgid: &'a [u8], he: &'a HistEnt) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(self.do_store(msgid, he))
    }
}

impl DHistoryInner {

    fn walk_chain<F>(&self, mut idx: u32, hv: DHash, msgid: impl AsRef<[u8]>, mut read_dhistent_at: F) -> io::Result<Result<DHistEnt, u32>>
        where F: FnMut(&fs::File, u64, &[u8]) -> io::Result<DHistEnt>
    {
        let mut counter = 0;
        let mut found = false;
        let mut dhe : DHistEnt = Default::default();

        while idx != 0 {
            let pos = self.data_offset + (idx as u64) * (DHISTENT_SIZE as u64);
            dhe = match read_dhistent_at(&self.rfile, pos, msgid.as_ref()) {
                Ok(dhe) => dhe,
                Err(e) => if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(Err(idx));
                } else {
                    return Err(e);
                }
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

    fn write_dhistent(&self, file: &mut fs::File, mut dhe: DHistEnt) -> io::Result<()> {
        let bucket = ((dhe.hv.h1 ^ dhe.hv.h2) & self.hash_mask) as usize;
        dhe.next = self.hash_buckets.load(bucket as usize);

        // get the current file size.
        let mut pos = file.seek(io::SeekFrom::End(0))?;
        // sanity check.
        if pos < self.data_offset {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "corrupt dhistory file"));
        }

        // Calculate hashtable index and write pos.
        // Align it at a multiple of DHISTENT_SIZE after data_offset.
        // Kind of paranoid, really.
        pos -= self.data_offset;
        let idx = (pos + DHISTENT_SIZE as u64 - 1) / DHISTENT_SIZE as u64;
        let pos = idx * DHISTENT_SIZE as u64 + self.data_offset;

        // write at the end of the file.
        let buf : [u8; DHISTENT_SIZE] = unsafe { mem::transmute(dhe) };
        file.write_at(&buf, pos)?;

        // update hash index
        self.hash_buckets.store(bucket, idx as u32);

        Ok(())
    }

    fn write_dhistent_append<W>(&self, file: &mut W, mut dhe: DHistEnt, pos: u64) -> io::Result<u64>
        where
            W: io::Write,
    {
        // calculate bucket.
        let bucket = ((dhe.hv.h1 ^ dhe.hv.h2) & self.hash_mask) as usize;
        dhe.next = self.hash_buckets.load(bucket as usize);

        // calculate index.
        let idx = (pos + DHISTENT_SIZE as u64 - 1) / DHISTENT_SIZE as u64;

        // append to file.
        let buf : [u8; DHISTENT_SIZE] = unsafe { mem::transmute(dhe) };
        file.write(&buf)?;

        // update hash index
        self.hash_buckets.store(bucket, idx as u32);

        Ok(pos + DHISTENT_SIZE as u64)
    }
}

// helper
fn read_dhisthead_at(file: &fs::File, pos: u64) -> io::Result<DHistHead> {
    let mut buf = [0u8; DHISTHEAD_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTHEAD_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("read_dhisthead_at({}): short read ({})", pos, n)))
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
fn read_dhistent<F>(file: &mut F) -> io::Result<Option<DHistEnt>>
    where
        F: Read,
{
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = file.read(&mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    if n != DHISTENT_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("read_dhistent: short read: {} bytes", n)));
    }
    Ok(Some(unsafe { mem::transmute(buf) }))
}

// helper
fn read_dhistent_at(file: &fs::File, pos: u64, msgid: &[u8]) -> io::Result<DHistEnt> {
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTENT_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("read_dhistent_at({}) for {:?}: short read ({})", pos, std::str::from_utf8(msgid), n)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
#[cfg(target_os = "linux")]
fn try_read_dhistent_at(file: &fs::File, pos: u64, msgid: &[u8]) -> io::Result<DHistEnt> {
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = try_read_at(file, &mut buf, pos)?;
    if n != DHISTENT_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("read_dhistent_at({}) for {:?}: short read ({})",
                                            pos, std::str::from_utf8(msgid), n)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

impl DHistEnt {

    // Encode a new DHistEnt.
    fn new(he: &HistEnt, hv: DHash, next: u32) -> DHistEnt {

        let mut dhe : DHistEnt = Default::default();
        dhe.next = next;
        dhe.hv = hv;
        dhe.gmt = (he.time / 60) as u32;

        let mut storage_type = spool::Backend::Diablo;
        if let Some(ref loc) = he.location {
            storage_type = loc.storage_type;
            dhe.iter = u16_from_le_bytes(&loc.token[0..2]);
            dhe.boffset = u32_from_le_bytes(&loc.token[2..6]);
            dhe.bsize = u32_from_le_bytes(&loc.token[6..10]);
            dhe.exp = if loc.spool < 100 { loc.spool as u16 + 100 } else { 0xff };
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
    fn to_location(&self) -> spool::ArtLoc {
        let mut t = [0u8; 14];
        u16_write_le_bytes(&mut t[0..2], self.iter);
        u32_write_le_bytes(&mut t[2..6], self.boffset);
        u32_write_le_bytes(&mut t[6..10], self.bsize);

        let s  = if (self.exp & 0x1000) != 0 {
            // not a diablo-spool history entry.
            (&t)[0..10].to_vec()
        } else {
            // This is a diablo-spool history entry, so include
            // gmt in the returned StorageToken
            u32_write_le_bytes(&mut t[10..14], self.gmt);
            (&t)[0..14].to_vec()
        };
        let btype = spool::Backend::from_u8(((self.exp & 0x0f00) >> 8) as u8);
        let sp = (self.exp & 0xff) as u8;
        let spool = if sp > 99 && sp < 200 { sp - 100 } else { 0xff };
        spool::ArtLoc{
            storage_type:   btype,
            spool:          spool,
            token:          s,
        }
    }

    // return status of this DHistEnt as history::HistStatus.
    fn status(&self) -> HistStatus {
        if self.gmt == 0 {
            return HistStatus::NotFound;
        }
        // not a diablo spool?
        if (self.exp & 0x1000) != 0 {
            if (self.exp & 0x4000) != 0 {
                return HistStatus::Expired;
            }
            if (self.exp & 0x2000) != 0 {
                return HistStatus::Rejected;
            }
            return HistStatus::Present;
        }
        // is a diablo spool?
        if (self.exp & 0x4000) != 0 {
            if self.iter == 0xffff {
                return HistStatus::Rejected;
            } else {
                return HistStatus::Expired;
            }
        }
        HistStatus::Present
    }

}

const CRC_POLY1 : u32 = 0x00600340;
const CRC_POLY2 : u32 = 0x00F0D50B;
const CRC_HINIT1 : u32 = 0xFAC432B1;
const CRC_HINIT2 : u32 = 0x0CD5E44A;

fn crc_init() -> Vec<DHash> {
    let mut table = Vec::with_capacity(256);
    for i in 0..256 {
        let mut v = i as u32;
        let mut hv = DHash{h1: 0, h2: 0};

        for _ in 0..8 {
            if (v & 0x80) != 0 {
                hv.h1 ^= CRC_POLY1;
                hv.h2 ^= CRC_POLY2;
            }
            hv.h2 = hv.h2 << 1;
            if (hv.h1 & 0x80000000) != 0 {
                hv.h2 |= 1;
            }
            hv.h1 <<= 1;
            v <<= 1;
        }
        table.push(hv);
    }
    table
}

fn crc_hash(crc_xor_table: &Vec<DHash>, msgid: &[u8]) -> DHash {
	let mut hv = DHash{ h1: CRC_HINIT1, h2: CRC_HINIT2 };
    for b in msgid {
        let i = ((hv.h1 >> 24) & 0xff) as usize;
        hv.h1 = (hv.h1 << 8) ^ (hv.h2 >> 24) ^ crc_xor_table[i].h1;
        hv.h2 = (hv.h2 << 8) ^ (*b as u32) ^ crc_xor_table[i].h2;
    }
    // Note from the author of the diablo implementation lib/hash.c :
    // Fold the generated CRC.  Note, this is buggy but it is too late
    // for me to change it now.  I should have XOR'd the 1 in, not OR'd
    // it when folding the bits.
    if (hv.h1 & 0x80000000) != 0 {
        hv.h1 = (hv.h1 & 0x7FFFFFFF) | 1;
    }
    if (hv.h2 & 0x80000000) != 0 {
        hv.h2 = (hv.h2 & 0x7FFFFFFF) | 1;
    }
    hv.h1 |= 0x80000000;
    hv
}

