
use std::io;
use std::io::{BufRead,BufReader,Read,Write,Seek,SeekFrom};
use std::fs;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc,Mutex};

use std::fmt::Debug;
use std::os::unix::fs::FileExt;

use byteorder::{ByteOrder,LE};
use bytes::{BufMut,BytesMut};
use libc;

use time;

use {ArtLoc,ArtPart,Backend,SpoolBackend,SpoolCfg};

/// A diablo spool instance.
///
/// Can be used for reading and writing articles from/to this spool.
///
/// It might contain one open file, the file that is currently being
/// used to write articles to for the owner of this struct.
pub struct DSpool {
    path:       PathBuf,
    spool_no:   u8,
    cfg:        Arc<SpoolCfg>,
    writer:     Arc<Mutex<DSpoolFile>>,
}

// Clone implementation for DSpool.
impl Clone for DSpool {
    fn clone(&self) -> DSpool {
        DSpool::new_dspool(self.cfg.clone())
    }
}

// A spoolfile.
#[derive(Default)]
struct DSpoolFile {
    file:       u16,
    dir:        u32,
    size:       u32,
    fh:         Option<fs::File>,
}

// article location, this struct is serialized/deserialized
// in the entry for this article in the history file.
#[derive(Debug)]
struct DArtLocation {
    dir:    u32,
    file:   u16,
    pos:    u32,
    size:   u32,
}

// Article header. This struct is stored on disk followed by the article.
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

fn to_location(loc: &ArtLoc) -> DArtLocation {
    let t = &loc.token;
    let mins = LE::read_u32(&t[10..14]);
    DArtLocation{
        file:   LE::read_u16(&t[0..2]),
        pos:    LE::read_u32(&t[2..6]),
        size:   LE::read_u32(&t[6..10]),
        dir:    mins - (mins % 10),
    }
}

fn from_location(loc: DArtLocation) -> Vec<u8> {
    let mut t = [0u8; 14];
    LE::write_u16(&mut t[0..2], loc.file);
    LE::write_u32(&mut t[2..6], loc.pos);
    LE::write_u32(&mut t[6..10], loc.size);
    LE::write_u32(&mut t[10..14], loc.dir);
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

    fn new_dspool(cfg: Arc<SpoolCfg>) -> DSpool {
        DSpool{
            path:       PathBuf::from(cfg.path.clone()),
            spool_no:   cfg.spool_no,
            cfg:        cfg,
            writer:     Arc::new(Mutex::new(DSpoolFile::default())),
        }
    }

    pub fn new(cfg: SpoolCfg) -> io::Result<Box<SpoolBackend>> {
        Ok(Box::new(DSpool::new_dspool(Arc::new(cfg))))
    }

    // locate file that holds the article, open it, read the DArtHead struct,
    // and return the info.
    fn open(&self, art_loc: &ArtLoc, part: &ArtPart) -> io::Result<(DArtHead, DArtLocation, fs::File)> {

        let loc = to_location(art_loc);
        debug!("art location: {:?}", loc);
        let flnm = format!("D.{:08x}/B.{:04x}", loc.dir, loc.file);
        let mut path = self.path.clone();
        path.push(flnm);
        let file = fs::File::open(&path)?;

        // tell kernel to read the headers, or the entire file.
        let size = match part {
            ArtPart::Stat => 0,
            ArtPart::Head => 16384,
            ArtPart::Article | ArtPart::Body => loc.size,
        };
        if size > 0 {
            unsafe {
                use std::os::unix::io::AsRawFd;
                libc::posix_fadvise(file.as_raw_fd(), loc.pos as libc::off_t,
                                size as libc::off_t, libc::POSIX_FADV_WILLNEED);
            }
        }

        let dh = read_darthead_at(&path, &file, loc.pos as u64)?;

        debug!("art header: {:?}", dh);

        // lots of sanity checks !
        if dh.magic1 != 0xff || dh.magic2 != 0x99 ||
            dh.version != 1 || dh.head_len != 24 {
            warn!("read({:?}): bad magic in header", dh);
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic in header"));
        }
        if dh.store_type != 1 && dh.store_type != 4 {
            warn!("read({:?}): unsupported store type", dh);
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported store type"));
        }
        if dh.arthdr_len > dh.art_len {
            warn!("read({:?}): arthdr_len > art_len", dh);
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid art_len or arthdr_len"));
        }
        if dh.art_len + DARTHEAD_SIZE as u32 > dh.store_len {
            warn!("read({:?}): art_len + DARTHEAD_SIZE > store_len", dh);
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid art_len or store_len"));
        }
        // store_len includes \0 after the article, the histfile loc entry doesn't.
        if dh.store_len - 1 > loc.size {
            warn!("read({:?}): article on disk larger than in history entry {:?}", dh, loc);
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid store_len"));
        }
        Ok((dh, loc, file))
    }
}

/// SpoolBackend::clone() calls box_clone() on the underlying struct.
impl Clone for Box<SpoolBackend>
{
    fn clone(&self) -> Box<SpoolBackend> {
        self.box_clone()
    }
}

impl SpoolBackend for DSpool {

    fn get_type(&self) -> Backend {
        Backend::Diablo
    }

    fn box_clone(&self) -> Box<SpoolBackend> {
        Box::new(self.clone())
    }

    fn read(&self, art_loc: &ArtLoc, part: ArtPart, mut buf: &mut BytesMut) -> io::Result<()> {

        let (head, loc, mut file) = self.open(art_loc, &part)?;

        let (start, len) = match part {
            ArtPart::Stat => return Ok(()),
            ArtPart::Head => {
                (loc.pos + DARTHEAD_SIZE as u32, head.arthdr_len)
            },
            ArtPart::Article => {
                (loc.pos + DARTHEAD_SIZE as u32, head.art_len)
            },
            ArtPart::Body => {
                let body_off = if head.store_type == 1 {
                    head.arthdr_len + 1
                } else {
                    head.arthdr_len + 2
                };
                (loc.pos + DARTHEAD_SIZE as u32 + body_off, head.art_len - body_off)
            }
        };
        file.seek(SeekFrom::Start(start as u64))?;
        let reader = file.try_clone()?.take(len as u64);

        if head.store_type == 1 {
            buf.reserve((len + len / 50) as usize);
            let reader = CrlfXlat::new(reader);
            read_to_bufmut(reader, &mut buf)?;
        } else {
            buf.reserve(len as usize);
            read_to_bufmut(reader, &mut buf)?;
        }
        Ok(())
    }

    fn write(&self, art: &[u8], hdr_len: usize) -> io::Result<ArtLoc> {

        // if the file is >1GB, close and allocate a new file.
        let writer = &mut *self.writer.lock().unwrap();
        if writer.fh.is_some() {
            if writer.size > 1000000000 {
                writer.fh.take();
                writer.size = 0;
            }
        }

        // see if we need to create a new file.
        if writer.fh.is_none() {

            // create directory
            let now = time::now_utc().to_timespec().sec as u64;
            writer.dir = (now / 60) as u32;
            writer.dir -= writer.dir % 10;
            let mut path = self.path.clone();
            path.push(format!("D.{:08x}", writer.dir));
            if let Err(e) = fs::create_dir(&path) {
                if e.kind() != io::ErrorKind::AlreadyExists {
                    return Err(e);
                }
            }

            // create file
            writer.size = 0;
            writer.file = (now & 0x7fff) as u16;

            for _ in 0..1000 {
                let mut name = path.clone();
                name.push(format!("B.{:04x}", writer.file));
                match fs::OpenOptions::new().write(true).create_new(true).open(name) {
                    Ok(fh) => {
                        writer.fh = Some(fh);
                        break;
                    },
                    Err(e) => {
                        if e.kind() != io::ErrorKind::AlreadyExists {
                            return Err(e);
                        }
                    }
                }
                writer.file = ((writer.file as u32 + 1) & 0x7fff) as u16;
            }

            // success?
            if writer.fh.is_none() {
                return Err(io::Error::new(io::ErrorKind::Other, "cannot create spool file"));
            }
        }

        let pos = writer.size;
        let store_len = (DARTHEAD_SIZE + art.len() + 1) as u32;
        let mut fh = writer.fh.take().unwrap();

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
        fh.write_all(&buf)?;
        writer.size += DARTHEAD_SIZE as u32;

        // and article itself.
        fh.write_all(art)?;
        writer.size += art.len() as u32;

        // add \0 at the end
        fh.write(b"\0")?;
        writer.size += 1;

        writer.fh.get_or_insert(fh);

        // build storage token
        let t = from_location(DArtLocation{
            dir:    writer.dir,
            file:   writer.file,
            pos:    pos as u32,
            size:   store_len - 1,
        });

        // return article location
        Ok(ArtLoc{
            storage_type:   Backend::Diablo,
            spool:          self.spool_no,
            token:          t,
        })
    }

    fn write_realloc(&self) -> io::Result<()> {
        let writer = &mut *self.writer.lock().unwrap();
        if writer.fh.is_some() {
            writer.fh.take();
            writer.size = 0;
        }
        Ok(())
    }
}

// helper function.
fn read_to_bufmut(mut reader: impl Read, buf: &mut BytesMut) -> io::Result<()> {
    loop {
        if buf.remaining_mut() == 0 {
            buf.reserve(4096);
        }
        let sz = unsafe {
            let sz = reader.read(buf.bytes_mut())?;
            buf.advance_mut(sz);
            sz
        };
        if sz == 0 {
            break;
        }
    }
    Ok(())
}
