
use std::io;
use std::io::{BufRead,BufReader,Read,Seek};
use std::fs;
use std::mem;
use std::path::PathBuf;

use std::fmt::Debug;
use std::os::unix::fs::FileExt;

use spool;

pub(crate) struct DSpool {
    path:       PathBuf,
    //minfree:    u64,
}

#[derive(Debug)]
struct DArtLocation {
    dir:    u32,
    file:   u16,
    pos:    u64,
    size:   u64,
}

//
// NOTE: a header-only article is always stored including the CRLF header/body
// seperator. In that case, arthdr_len + size(CRLF) == art_len.
//
// a header-only article always ends in CRLF CRLF (without ending DOT CRLF)
//
// a complete article ends in CRLF DOT CRLF
//
#[derive(Debug)]
#[repr(C)]
struct DArtHead {
    magic1:     u8,     // 0xff
    magic2:     u8,     // 0x99
    version:    u8,     // 1
    head_len:   u8,     // size of this struct (24)
    store_type: u8,     // text:1 gzip:2 wireformat:4
    _unused1:   u8,
    _unused2:   u8,
    _unused3:   u8,
    arthdr_len: u32,    // art header size (without art/body CRLF separator)
    art_len:    u32,    // article header + body size
    store_len:  u32,    // size of article on disk (incl DArtHead + trailing \0)
    hdr_end:    u8,     // Nul (should be 0. on actual diablo spools - garbage).
    _unused4:   u8,
    _unused5:   u8,
    _unused6:   u8,
}
const DARTHEAD_SIZE : usize = 24;

fn to_location(tok: &spool::Token) -> DArtLocation {
    let t = &tok.token;
    let gmt = (t[10] as u32) << 24 | (t[11] as u32) << 16 | (t[12] as u32) << 8 | t[13] as u32;
    DArtLocation{
        file:   (t[0] as u16) << 8 | t[1] as u16,
        pos:    (t[2] as u64) << 24 | (t[3] as u64) << 16 | (t[4] as u64) << 8 | t[5] as u64,
        size:   (t[6] as u64) << 24 | (t[7] as u64) << 16 | (t[8] as u64) << 8 | t[9] as u64,
        dir:    gmt - (gmt % 10),
    }
}

fn read_darthead_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<DArtHead> {
    let mut buf = [0u8; DARTHEAD_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DARTHEAD_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("{:?}: short read", path)));
    }
    Ok(unsafe { mem::transmute(buf) })
}

// A reader-wrapper that translates CR to CRLF.
struct CrlfXlat<T> {
    inner:  BufReader<T>,
}

impl<T: Read> CrlfXlat<T> {
    fn new(file: T) -> CrlfXlat<T> {
        CrlfXlat{
            inner:  BufReader::new(file),
        }
    }
}

impl<T: Read> Read for CrlfXlat<T> {
    fn read(&mut self, outbuf: &mut [u8]) -> io::Result<usize> {
        let mut out_idx = 0;
        let mut in_idx = 0;
        {
            let inbuf = self.inner.fill_buf()?;
            while in_idx < inbuf.len() && out_idx < outbuf.len() {
                if inbuf[in_idx] == b'\n' {
                    outbuf[out_idx] = b'\r';
                    out_idx += 1;
                    if out_idx >= outbuf.len() {
                        break;
                    }
                }
                outbuf[out_idx] = inbuf[in_idx];
                out_idx += 1;
                in_idx += 1;
            }
        }
        self.inner.consume(in_idx);
        Ok(out_idx)
    }
}

impl DSpool {
    pub fn new(cfg: &spool::SpoolCfg) -> io::Result<Box<spool::SpoolBackend>> {
        Ok(Box::new(DSpool{
            path:       PathBuf::from(cfg.path.clone()),
            //minfree:    0,
        }))
    }
}

impl spool::SpoolBackend for DSpool {

    fn get_type(&self) -> spool::Backend {
        spool::Backend::Diablo
    }

    fn open(&self, token: &spool::Token, head_only: bool) -> io::Result<Box<io::Read>> {
        let t = to_location(token);
        debug!("art location: {:?}", t);
        let flnm = format!("D.{:08x}/B.{:04x}", t.dir, t.file);
        let mut path = self.path.clone();
        path.push(flnm);
        let mut file = fs::File::open(&path)?;
        let dh = read_darthead_at(&path, &file, t.pos)?;
        debug!("art header: {:?}", dh);
        file.seek(io::SeekFrom::Start(t.pos + DARTHEAD_SIZE as u64))?;
        let sz = if head_only {
            let mut s = dh.arthdr_len + 1;
            if (dh.store_type & 4) > 0 {
                s += 1
            }
            if s > dh.art_len {
                s = dh.art_len;
            }
            s
        } else {
            dh.art_len
        };

        // if this is not wireformat, translate to crlf on-the-fly.
        let rdr = file.take(sz as u64);
        if (dh.store_type & 1) > 0 {
            Ok(Box::new(CrlfXlat::new(rdr)))
        } else {
            Ok(Box::new(rdr))
        }
    }
}

