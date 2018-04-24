
use std::io;
use std::fs;
use std::mem;
use std::path::{Path, PathBuf};

use std::fmt::Debug;
use std::os::unix::fs::FileExt;

use spool;
use history;
use history::{HistEnt,HistStatus};

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

fn read_dhisthead_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<DHistHead> {
    let mut buf = [0u8; DHISTHEAD_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTHEAD_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

fn read_u32_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    let n = file.read_at(&mut buf, pos)?;
    if n != 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

fn read_dhistent_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<DHistEnt> {
    let mut buf = [0u8; DHISTENT_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DHISTENT_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

impl DHistEnt {
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
            return HistStatus::Found;
        }
        // is a diablo spool?
        if (self.exp & 0x4000) != 0 {
            if self.iter == 0xffff {
                return HistStatus::Rejected;
            } else {
                return HistStatus::Expired;
            }
        }
        HistStatus::Found
    }

    fn to_token(&self) -> spool::Token {
        let mut t = [0u8; 14];
        t[0] = (self.iter >> 8) as u8;
        t[1] = (self.iter & 0xff) as u8;
        t[2] = ((self.boffset >> 24) & 0xff) as u8;
        t[3] = ((self.boffset >> 16) & 0xff) as u8;
        t[4] = ((self.boffset >> 8) & 0xff) as u8;
        t[5] = (self.boffset & 0xff) as u8;
        t[6] = ((self.bsize >> 24) & 0xff) as u8;
        t[7] = ((self.bsize >> 16) & 0xff) as u8;
        t[8] = ((self.bsize >> 8) & 0xff) as u8;
        t[9] = (self.bsize & 0xff) as u8;

        let s  = if (self.exp & 0x1000) != 0 {
            // not a diablo-spool history entry.
            (&t)[0..10].to_vec()
        } else {
            // This is a diablo-spool history entry, so include
            // gmt in the returned StorageToken
            t[10] = ((self.gmt >> 24) & 0xff) as u8;
            t[11] = ((self.gmt >> 16) & 0xff) as u8;
            t[12] = ((self.gmt >> 8) & 0xff) as u8;
            t[13] = (self.gmt & 0xff) as u8;
            (&t)[0..14].to_vec()
        };
        let btype = spool::Backend::from_u8(((self.exp & 0x0f00) >> 8) as u8);
        let sp = (self.exp & 0xff) as u8;
        let spool = if sp > 99 && sp < 200 { sp - 100 } else { 0xff };
        spool::Token{
            storage_type:   btype,
            spool:          spool,
            token:          s,
        }
    }
}

impl history::HistBackend for DHistory {
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt> {

        let hv = crc_hash(msgid);
        let bucket = ((hv.h1 ^hv.h2) & self.hash_mask) as u64;
        let pos = DHISTHEAD_SIZE as u64 + bucket * 4;
        let mut idx = read_u32_at(&self.path, &self.file, pos)?;

        let mut dhe : DHistEnt = Default::default();
        let mut counter = 0;
        let mut found = false;

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
                token:      None,
            });
        }

        // see if entry is valid. if it is not, we just return NotFound
        // instead of logging an error. debatable.
        let spool = (dhe.exp & 0xff) as u8;
        let status = dhe.status();
        if status == HistStatus::Found && (idx == 0 || spool < 100 || spool > 199) {
            return Ok(HistEnt{
                time:       0,
                status:     HistStatus::NotFound,
                head_only:  false,
                token:      None,
            });
        }

        let token = match status {
            HistStatus::Found => Some(dhe.to_token()),
            _ => None,
        };
        Ok(HistEnt{
            time:       (dhe.gmt as u64) * 60,
            status:     status,
            head_only:  (dhe.exp & 0x8000) > 0,
            token:      token,
        })
    }
}
