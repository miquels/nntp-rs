
use std::io;
use std::fs;
use std::mem;
use std::io::{Seek,Write};
use std::path::{Path, PathBuf};
use std::fmt::Debug;
use std::os::unix::fs::FileExt;

use spool;
use history;
use history::{HistEnt,HistStatus};
use {u16_to_b2,u32_to_b4,b2_to_u16,b4_to_u32};

#[derive(Debug)]
pub(crate) struct DHistory {
    path:           PathBuf,
    file:           fs::File,
    hash_size:      u32,
    hash_mask:      u32,
    data_offset:    u64,
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
const DHISTHEAD_DEADMAGIC : u32 = 0xDEADF5E6;
const DHISTHEAD_VERSION2 : u16 = 2;

#[derive(Debug, Default, PartialEq)]
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

    // open existing history database
    pub fn open(path: &Path) -> io::Result<DHistory> {

        // open file and read first 16 bytes into a DHistHead.
        let f = fs::File::open(path)?;
        let meta = f.metadata()?;
        let dhh = read_dhisthead_at(path, &f, 0)?;
        debug!("{:?} - dhisthead: {:?}", path, &dhh);

        // see if header indicates this is a diablo history file.
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

        Ok(DHistory{
            path:           path.to_owned(),
            file:           f,
            hash_size:      dhh.hash_size,
            hash_mask:      dhh.hash_size - 1,
            data_offset:    data_offset,
        })
    }

    // create a new history database, only if it doesn't exist yet.
    pub fn create(path: &Path, num_buckets: u32) -> io::Result<()> {

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

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
        let buf = [0u8; 1024*1024];
        let todo = (num_buckets * 4 + DHISTENT_SIZE as u32) as usize;
        let mut done = 0;
        while todo < done {
            let w = if todo > buf.len() { buf.len() } else { todo };
            let wbuf = &buf[0..w];
            done += file.write(wbuf)?;
        }

        Ok(())
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

fn crc_hash(msgid: &[u8]) -> DHash {
	let mut hv = DHash{ h1: CRC_HINIT1, h2: CRC_HINIT2 };
    for b in msgid {
        let i = ((hv.h1 >> 24) & 0xff) as usize;
        hv.h1 = (hv.h1 << 8) ^ (hv.h2 >> 24) ^ CRC_XOR_TABLE[i].h1;
        hv.h2 = (hv.h2 << 8) ^ (*b as u32) ^ CRC_XOR_TABLE[i].h2;
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

// helper
fn read_dhisthead_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<DHistHead> {
    let mut buf = [0u8; DHISTHEAD_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTHEAD_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
fn read_u32_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    let n = file.read_at(&mut buf, pos)?;
    if n != 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
fn write_u32_at<N: Debug>(_path: N, file: &fs::File, pos: u64, val: u32) -> io::Result<(usize)> {
    let buf : [u8; 4] = unsafe { mem::transmute(val) };
    file.write_at(&buf, pos)
}

// helper
fn read_dhistent_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<DHistEnt> {
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTENT_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// helper
fn write_dhistent_at<N: Debug>(_path: N, file: &fs::File, pos: u64, dhe: DHistEnt) -> io::Result<(usize)> {
    let buf : [u8; DHISTENT_SIZE] = unsafe { mem::transmute(dhe) };
    file.write_at(&buf, pos)
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
            dhe.iter = b2_to_u16(&loc.token[0..2]);
            dhe.boffset = b4_to_u32(&loc.token[2..6]);
            dhe.bsize = b4_to_u32(&loc.token[6..10]);
            dhe.exp = if loc.spool < 100 { loc.spool as u16 + 100 } else { 0xff };
        }

        if he.head_only {
            dhe.exp |= 0x8000;
        }

        if storage_type == spool::Backend::Diablo {
            if let Some(ref loc) = he.location {
                dhe.gmt = b4_to_u32(&loc.token[10..14]);
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
        u16_to_b2(&mut t, 0, self.iter);
        u32_to_b4(&mut t, 2, self.boffset);
        u32_to_b4(&mut t, 6, self.bsize);

        let s  = if (self.exp & 0x1000) != 0 {
            // not a diablo-spool history entry.
            (&t)[0..10].to_vec()
        } else {
            // This is a diablo-spool history entry, so include
            // gmt in the returned StorageToken
            u32_to_b4(&mut t, 10, self.gmt);
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

impl history::HistBackend for DHistory {

    // lookup an article in the DHistry database
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt> {

        let hv = crc_hash(msgid);
        let bucket = ((hv.h1 ^hv.h2) & self.hash_mask) as u64;
        let pos = DHISTHEAD_SIZE as u64 + bucket * 4;

        let mut dhe : DHistEnt = Default::default();
        let mut counter = 0;
        let mut found = false;

        let mut idx = read_u32_at(&self.path, &self.file, pos)?;

        while idx != 0 {
            let pos = self.data_offset + (idx as u64) * (DHISTENT_SIZE as u64);
            dhe = read_dhistent_at(&self.path, &self.file, pos)?;
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
            return Ok(HistEnt{
                time:       0,
                status:     HistStatus::NotFound,
                head_only:  false,
                location:   None,
            });
        }

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

    // store an article in the DHistry database
    fn store(&mut self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {

        let hv = crc_hash(msgid);
        let bucket = ((hv.h1 ^ hv.h2) & self.hash_mask) as u64;
        let bpos = DHISTHEAD_SIZE as u64 + bucket * 4;

        let next = read_u32_at(&self.path, &self.file, bpos)?;
        let dhe = DHistEnt::new(he, hv, next);

        // file must be bigger than histhead + hashtable.
        let mut pos = self.file.seek(io::SeekFrom::End(0))?;
        if pos < self.data_offset {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      format!("{:?}: corrupt dhistory file", &self.path)));
        }
        // calculate hashtable index and write pos.
        pos -= self.data_offset;
        let idx = (pos + DHISTENT_SIZE as u64 - 1) / DHISTENT_SIZE as u64;
        let pos = idx * DHISTENT_SIZE as u64 + self.data_offset;

        write_dhistent_at(&self.path, &self.file, pos, dhe)?;
        write_u32_at(&self.path, &self.file, bpos, idx as u32)?;

        Ok(())
    }
}
