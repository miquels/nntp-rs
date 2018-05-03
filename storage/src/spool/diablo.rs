
use std::io;
use std::io::{BufRead,BufReader,Read,Write,Seek};
use std::fs;
use std::mem;
use std::path::PathBuf;
use std::sync::Mutex;

use std::fmt::Debug;
use std::os::unix::fs::FileExt;

use time;
use spool;
use {u16_to_b2,u32_to_b4,b2_to_u16,b4_to_u32};

pub(crate) struct DSpool {
    path:       PathBuf,
    spool_no:   u8,
    reallocint: u64,
    inner:      Mutex<DSpoolFile>,
    //minfree:    u64,
}

// A spoolfile.
struct DSpoolFile {
    time:       u64,
    file:       u16,
    dir:        u32,
    size:       u32,
    fh:         Option<fs::File>,
}

#[derive(Debug)]
struct DArtLocation {
    dir:    u32,
    file:   u16,
    pos:    u32,
    size:   u32,
}

//
// NOTE: a header-only article is always stored including the CRLF header/body
// seperator. In that case, arthdr_len + size(CRLF) == art_len.
//
// a header-only article always ends in CRLF CRLF (without ending DOT CRLF)
//
// a complete article ends in CRLF DOT CRLF
//
#[derive(Debug, Default)]
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

fn to_location(loc: &spool::ArtLoc) -> DArtLocation {
    let t = &loc.token;
    let mins = (t[10] as u32) << 24 | (t[11] as u32) << 16 | (t[12] as u32) << 8 | t[13] as u32;
    DArtLocation{
        file:   b2_to_u16(&t[0..2]),
        pos:    b4_to_u32(&t[2..6]),
        size:   b4_to_u32(&t[6..10]),
        dir:    mins - (mins % 10),
    }
}

fn from_location(loc: DArtLocation) -> Vec<u8> {
    let mut t = [0u8; 14];
    u16_to_b2(&mut t, 0, loc.file);
    u32_to_b4(&mut t, 2, loc.pos);
    u32_to_b4(&mut t, 6, loc.size);
    u32_to_b4(&mut t, 10, loc.dir);
    t.to_vec()
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
    lfseen: bool,
    eof:    bool,
}

impl<T: Read> CrlfXlat<T> {
    fn new(file: T) -> CrlfXlat<T> {
        CrlfXlat{
            inner:  BufReader::new(file),
            lfseen: true,
            eof:    false,
        }
    }
}

impl<T: Read> Read for CrlfXlat<T> {
    fn read(&mut self, outbuf: &mut [u8]) -> io::Result<usize> {

        if self.eof {
            return Ok(0);
        }

        let mut out_idx = 0;
        let mut in_idx = 0;
        {
            let inbuf = self.inner.fill_buf()?;
            if inbuf.len() == 0 {
                outbuf[0] = b'.';
                outbuf[1] = b'\r';
                outbuf[2] = b'\n';
                self.eof = true;
                return Ok(3);
            }

            while in_idx < inbuf.len() && out_idx < outbuf.len() {

                // dotstuffing.
                if self.lfseen && inbuf[in_idx] == b'.' {
                    // need to insert a dot. see if there's still space.
                    if out_idx > outbuf.len() - 2 {
                        break;
                    }
                    outbuf[out_idx] = b'.';
                    out_idx += 1;
                }
                self.lfseen = false;

                // LF -> CRLF
                if inbuf[in_idx] == b'\n' {
                    // need to insert a \r. see if there's still space.
                    if out_idx > outbuf.len() - 2 {
                        break;
                    }
                    outbuf[out_idx] = b'\r';
                    out_idx += 1;
                    self.lfseen = true;
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
    pub fn new(cfg: &spool::SpoolCfg, spool_no: u8) -> io::Result<Box<spool::SpoolBackend>> {
        Ok(Box::new(DSpool{
            path:       PathBuf::from(cfg.path.clone()),
            spool_no:   spool_no,
            reallocint: 600,
            inner:      Mutex::new(DSpoolFile{
                            time:   0,
                            file:   0,
                            dir:    0,
                            size:   0,
                            fh:     None,
                        }),
        }))
    }
}

impl spool::SpoolBackend for DSpool {

    fn get_type(&self) -> spool::Backend {
        spool::Backend::Diablo
    }

    fn open(&self, art_loc: &spool::ArtLoc, part: spool::ArtPart) -> io::Result<Box<io::Read>> {

        let t = to_location(art_loc);
        debug!("art location: {:?}", t);
        let flnm = format!("D.{:08x}/B.{:04x}", t.dir, t.file);
        let mut path = self.path.clone();
        path.push(flnm);
        let mut file = fs::File::open(&path)?;

        let dh = read_darthead_at(&path, &file, t.pos as u64)?;
        debug!("art header: {:?}", dh);

        let start = t.pos + DARTHEAD_SIZE as u32;
        let (pos, sz) = match part {
            spool::ArtPart::Article => {
                (start, dh.art_len)
            },
            spool::ArtPart::Head => {
                let mut s = dh.arthdr_len;
                if s > dh.art_len {
                    s = dh.art_len;
                }
                (start, s)
            },
            spool::ArtPart::Body => {
                let mut s = dh.arthdr_len + 1;
                if (dh.store_type & 4) > 0 {
                    s += 1;
                }
                if s > dh.art_len {
                    s = dh.art_len;
                }
                (start + s as u32, dh.art_len - s)
            },
        };

        file.seek(io::SeekFrom::Start(pos as u64))?;
        let rdr = file.take(sz as u64);

        // if this is not wireformat, translate to crlf on-the-fly.
        if (dh.store_type & 1) > 0 {
            Ok(Box::new(CrlfXlat::new(rdr)))
        } else {
            Ok(Box::new(rdr))
        }
    }

    fn write(&self, art: &[u8], hdr_len: usize, head_only: bool) -> io::Result<spool::ArtLoc> {

        // if file is open, see how long we've had it opened. If it's more
        // than reallocint (default 10 mins) close it and open a new file.
        // Same if file is > 1GB.
        let now = time::now_utc().to_timespec().sec as u64;
        let inner = &mut * self.inner.lock().unwrap();
        if inner.fh.is_some() {
            if now - inner.time > self.reallocint || inner.size > 1000000000 {
                inner.fh.take();
                inner.size = 0;
            }
        }

        // see if we need to create a new file.
        if inner.fh.is_none() {

            // create directory
            inner.time = now;
            inner.dir = (now / 60) as u32;
            inner.dir -= inner.dir % 10;
            let mut path = self.path.clone();
            path.push(format!("D.{:08x}", inner.dir));
            if let Err(e) = fs::create_dir(&path) {
                if e.kind() != io::ErrorKind::AlreadyExists {
                    return Err(e);
                }
            }

            // create file
            inner.size = 0;
            inner.file = (now & 0x7fff) as u16;

            for _ in 0..1000 {
                let mut name = path.clone();
                name.push(format!("B.{:04x}", inner.file));
                match fs::OpenOptions::new().write(true).create_new(true).open(name) {
                    Ok(fh) => {
                        inner.fh = Some(fh);
                        break;
                    },
                    Err(e) => {
                        if e.kind() != io::ErrorKind::AlreadyExists {
                            return Err(e);
                        }
                    }
                }
                inner.file = ((inner.file as u32 + 1) & 0x7fff) as u16;
            }

            // success?
            if inner.fh.is_none() {
                return Err(io::Error::new(io::ErrorKind::Other, "cannot create spool file"));
            }
        }

        let pos = inner.size;
        let store_len = (DARTHEAD_SIZE + art.len() + 1) as u32;
        let mut fh = inner.fh.take().unwrap();

        // write header.
        let mut ah = DArtHead::default();
        ah.magic1 = 0xff;
        ah.magic1 = 0x99;
        ah.version = 1;
        ah.head_len = DARTHEAD_SIZE as u8;
        ah.store_type = 4;
        ah.arthdr_len = hdr_len as u32;
        ah.art_len = art.len() as u32;
        ah.store_len = store_len;
        let buf : [u8; DARTHEAD_SIZE] = unsafe { mem::transmute(ah) };
        if let Err(e) = fh.write_all(&buf) {
            return Err(e);
        }
        inner.size += DARTHEAD_SIZE as u32;

        // and article itself.
        if let Err(e) = fh.write_all(art) {
            return Err(e);
        }
        inner.size += art.len() as u32;

        // add \0 at the end
        if let Err(e) = fh.write(b"\0") {
            return Err(e);
        }
        inner.size += 1;
        inner.fh.get_or_insert(fh);

        // build storage token
        let t = from_location(DArtLocation{
            dir:    inner.dir,
            file:   inner.file,
            pos:    pos as u32,
            size:   store_len,
        });

        // return article location
        Ok(spool::ArtLoc{
            storage_type:   spool::Backend::Diablo,
            spool:          self.spool_no,
            token:          t,
        })
    }
}

