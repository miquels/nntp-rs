
use std::fs;
use std::io;
use std::io::Error as IoError;
use std::io::{BufRead,BufReader,Read,Write,Seek,SeekFrom};
use std::mem;
use std::path::{Path,PathBuf};

use std::fmt::Debug;
use std::os::unix::fs::{FileExt, MetadataExt};

use bytes::{BufMut,BytesMut};
use libc;
use parking_lot::Mutex;

use crate::util::unixtime;
use crate::util::byteorder::*;
use super::{ArtLoc,ArtPart,Backend,MetaSpool,SpoolBackend,SpoolDef};

const MAX_SPOOLFILE_SIZE : u64 = 1_000_000_000;
const DFL_FILE_REALLOCINT : u32 = 600;
//const DFL_DIR_REALLOCINT : u32 = 3600;

/// A diablo spool instance.
///
/// Can be used for reading and writing articles from/to this spool.
pub struct DSpool {
    path:               PathBuf,
    spool_no:           u8,
    file_reallocint:    u32,
    dir_reallocint:     u32,
    minfree:            u64,
    maxsize:            u64,
    keeptime:           u64,
    writer:             Mutex<Writer>,
}

// The file we have open for writing.
#[derive(Default)]
struct Writer {
    fh:         Option<fs::File>,
    name:       String,
    file:       u16,
    dir:        u32,
    tm:         u64,
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

// article location, this struct is serialized/deserialized
// in the entry for this article in the history file.
#[derive(Debug)]
struct DArtLocation {
    dir:    u32,
    file:   u16,
    pos:    u32,
    size:   u32,
}

fn to_location(loc: &ArtLoc) -> DArtLocation {
    let t = &loc.token;
    DArtLocation{
        file:   u16_from_le_bytes(&t[0..2]),
        pos:    u32_from_le_bytes(&t[2..6]),
        size:   u32_from_le_bytes(&t[6..10]),
        dir:    u32_from_le_bytes(&t[10..14]),
    }
}

fn from_location(loc: DArtLocation) -> Vec<u8> {
    let mut t = [0u8; 14];
    u16_write_le_bytes(&mut t[0..2], loc.file);
    u32_write_le_bytes(&mut t[2..6], loc.pos);
    u32_write_le_bytes(&mut t[6..10], loc.size);
    u32_write_le_bytes(&mut t[10..14], loc.dir);
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

struct ReadAhead{}
trait ReadAheadTrait {
    // Optimization: when reading the header of the article, and we
    // know we're going to read more after that, ask the kernel
    // to do read-ahead.
    fn article(_part: &ArtPart, _loc: &DArtLocation, _file: &fs::File) {
    }
}
impl ReadAheadTrait for ReadAhead {
    #[cfg(all(target_family = "unix", not(target_os = "macos")))]
    fn article(part: &ArtPart, loc: &DArtLocation, file: &fs::File) {
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
    }
}

/// This is the main backend implementation.
impl DSpool {

    /// Create a new diablo-type spool backend.
    pub fn new(cfg: &SpoolDef, ms: &MetaSpool) -> io::Result<Box<dyn SpoolBackend>> {
        let file_reallocint = if ms.reallocint.as_secs() > 0 {
            ms.reallocint.as_secs() as u32
        } else {
            DFL_FILE_REALLOCINT
        };
        let file_reallocint = (file_reallocint / 60) * 60;
        let dir_reallocint = file_reallocint * 6;

        let sv = StatVfs::stat(&cfg.path)
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", cfg.path, e)))?;

        // minfree must be at least 10MB, if not force it.
        let minfree = {
            if cfg.minfree < 10_000_000 {
                warn!("spool {}: setting minfree to 10MiB", cfg.spool_no);
                10_000_000
            } else {
                cfg.minfree
            }
        };

        // if maxsize is not set, take the size of the filesystem.
        // check that it is bigger than minfree.
        let maxsize = {
            let m = if cfg.maxsize > 0 && cfg.maxsize < sv.b_total {
                cfg.maxsize
            } else {
                sv.b_total
            };
            if minfree > m {
                return Err(io::Error::new(io::ErrorKind::Other,
                              format!("spool {}: minfree > maxsize ({} > {})",
                              cfg.spool_no, minfree, m)));
            }
            m
        };

        let ds = DSpool{
            path:               PathBuf::from(cfg.path.clone()),
            spool_no:           cfg.spool_no,
            file_reallocint:    file_reallocint,
            dir_reallocint:     dir_reallocint,
            keeptime:           cfg.keeptime.as_secs(),
            minfree:            minfree,
            maxsize:            maxsize,
            writer:             Mutex::new(Writer::default()),
        };
        Ok(Box::new(ds))
    }

    // locate file that holds the article, open it, read the DArtHead struct,
    // and return the info.
    // XXX TODO: cache a few open filehandles.
    fn open(&self, art_loc: &ArtLoc, part: &ArtPart) -> io::Result<(DArtHead, DArtLocation, fs::File)> {

        let loc = to_location(art_loc);
        debug!("art location: {:?}", loc);
        let flnm = format!("D.{:08x}/B.{:04x}", loc.dir, loc.file);
        let mut path = self.path.clone();
        path.push(flnm);
        let file = fs::File::open(&path)?;

        ReadAhead::article(&part, &loc, &file);
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

    // Read an article. 
    // SpoolBackend::read() forwards to this method.
    fn do_read(&self, art_loc: &ArtLoc, part: ArtPart, mut buf: &mut BytesMut) -> io::Result<()> {

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

    // Finds the most recently modified spoolfile in the directory.
    fn find_most_recent(&self, path: impl AsRef<Path>) -> io::Result<Option<u16>> {
        let d = fs::read_dir(path)?;
        let d = d
            .filter_map(|r| r.ok())
            .filter_map(|e| e.metadata().ok().map(|m| (m.len(), e.file_name(), m.modified())))
            .filter_map(|(l, n, t)| n.into_string().ok().map(|n| (l, n, t.unwrap())))
            .filter(|(_, n, _)| n.starts_with("B.") && n.len() == 6)
            .filter_map(|(l, n, t)| u16::from_str_radix(&n[2..], 16).ok().map(|g| (l, n, g, t)));
        let mut files : Vec<_> = d.collect();
        files.sort_unstable_by(|(_, _, _, a), (_, _, _, b)| a.cmp(b));
        let res = match files.into_iter().last() {
            None => None,
            Some((len, _name, iter, _tm)) => {
                if len < MAX_SPOOLFILE_SIZE {
                    Some(iter)
                } else {
                    None
                }
            },
        };
        Ok(res)
    }

    // Write an article. 
    // SpoolBackend::write() forwards to this method.
    //
    // Yes, by locking here we basically make all writes single-threaded.
    // This should not be a problem since writes are buffered heavily in
    // the kernel and a kernel-thread is doing the writing.
    //
    // If it does turn out to be a bottleneck we'll have to figure out a
    // way to introduce parallelism again.
    fn do_write(&self, headers: &[u8], body: &[u8]) -> io::Result<ArtLoc> {

        // lock the writer so we have unique access.
        let mut writer = self.writer.lock();

        // check if we had this file open for more than file_reallocint secs,
        // or if we need to move to a new directory.
        let now = unixtime();
        if writer.fh.is_some() {
            let cur_dirslot = ((now / 60) as u32) / (self.dir_reallocint / 60);
            let wri_dirslot = writer.dir / (self.dir_reallocint / 60);
            if writer.tm + (self.file_reallocint as u64) < now || cur_dirslot != wri_dirslot {
                writer.fh.take();
            }
        }

        // need to start writing to a new file?
        let mut re_open = false;
        if writer.fh.is_none() {

            // Create directory if needed.
            let mut path = self.path.clone();
            let mut dir = (now / 60) as u32;
            dir -= dir % (self.dir_reallocint / 60);
            path.push(format!("D.{:08x}", dir));
            if dir != writer.dir {
                if let Err(e) = fs::create_dir(&path) {
                    if e.kind() != io::ErrorKind::AlreadyExists {
                        return Err(io::Error::new(e.kind(), format!("create {:?}: {}", path, e)));
                    }
                    if writer.file == 0 {
                        // Existing dir. If we just started, try to re-use the
                        // last spool file used.
                        let file = self.find_most_recent(&path)
                            .map_err(|e| IoError::new(e.kind(), format!("readdir {:?}: {}", path, e)))?;
                        writer.file = match file {
                            Some(f) => { re_open = true; f },
                            None => 1,
                        };
                    }
                } else {
                    // created new directory. start at B.0001.
                    writer.file = 1;
                }
                writer.dir = dir;
            }

            let start = writer.file;
            for _ in start..4095 {

                // get spoolfile name.
                let mut name = path.clone();
                name.push(format!("B.{:04x}", writer.file));

                // see if the file already exists.
                match fs::metadata(&name) {
                    // maybe re-open an existing file, continue where
                    // we left off (can happen after a reload/restart).
                    Ok(_) => {
                        if !re_open {
                            writer.file = (writer.file + 1) & 0x7fff;
                            if writer.file == 0 {
                                writer.file = 1;
                            }
                            continue
                        }
                    },
                    Err(e) => match e.kind() {
                        // notfound is OK, all other errors are fatal.
                        io::ErrorKind::NotFound => {},
                        _ => return Err(IoError::new(e.kind(), format!("{:?}: {}", name, e))),
                    },
                };

                // actually open/create the spoolfile.
                let fh =
                    fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&name)
                    .map_err(|e| IoError::new(e.kind(), format!("{:?}: {}", name, e)))?;
                writer.tm = now;
                writer.fh = Some(fh);
                writer.name = name.to_str().unwrap_or("impossible-non-utf8-filename").to_string();
                break;
            }

            // success?
            if writer.fh.is_none() {
                return Err(io::Error::new(io::ErrorKind::Other,
                                          format!("{:?}: cannot create spool file", path)));
            }
        }

        let hdr_len = headers.len();
        let art_len = headers.len() + body.len();

        let mut fh = writer.fh.take().unwrap();
        // XXX should we store the filelength instead of fstat()'ing every time?
        let meta = fh.metadata()?;
        let pos = meta.len();
        let store_len = (DARTHEAD_SIZE + art_len + 1) as u32;

        // build header.
        let mut ah = DArtHead::default();
        ah.magic1 = 0xff;
        ah.magic2 = 0x99;
        ah.version = 1;
        ah.head_len = DARTHEAD_SIZE as u8;
        ah.store_type = 4;
        ah.arthdr_len = hdr_len as u32;
        ah.art_len = art_len as u32;
        ah.store_len = store_len;
        let buf : [u8; DARTHEAD_SIZE] = unsafe { mem::transmute(ah) };

        // write header, article, trailing \0.
        fh.write_all(&buf)
            .and_then(|_| fh.write_all(headers))
            .and_then(|_| fh.write_all(body))
            .and_then(|_| fh.write(b"\0"))
            .map_err(|e| IoError::new(e.kind(),format!("writing to {}: {}", writer.name, e)))?;

        // store filehandle, unless we went over 1GB size.
        if pos + (store_len as u64) < MAX_SPOOLFILE_SIZE {
            writer.fh.get_or_insert(fh);
        }

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

    // Get a list of D.xxxxxxxx directories, and sort them. Then find the oldest
    // non-empty directory, and return the xxxxxxxx part as a timestamp.
    fn do_get_oldest(&self) -> io::Result<Option<u64>> {
        let d = match fs::read_dir(&self.path) {
            Ok(d) => d,
            Err(e) => {
                error!("get_oldest({:?}): {}", self.path, e);
                return Err(e);
            },
        };
        let d = d
            .filter_map(|r| r.ok())
            .filter_map(|r| r.file_name().into_string().ok())
            .filter(|f| f.starts_with("D.") && f.len() == 10);
        let mut files : Vec<_> = d.collect();
        files.sort();
        for file in &files {
            let when = match u64::from_str_radix(&file[2..], 16) {
                Ok(w) => w * 60,
                Err(_) => continue,
            };
            let mut path = self.path.clone();
            path.push(file);
            if let Ok(meta) = fs::metadata(&path) {
                // skip if directory is emtpy.
                if meta.nlink() != 2 {
                    return Ok(Some(when));
                }
            }
        }
        warn!("get_oldest({:?}): no spooldirs - skipping history expire", self.path);
        Ok(None)
    }
}

impl SpoolBackend for DSpool {

    fn get_type(&self) -> Backend {
        Backend::Diablo
    }

    fn read(&self, art_loc: &ArtLoc, part: ArtPart, buf: &mut BytesMut) -> io::Result<()> {
        self.do_read(art_loc, part, buf)
    }

    fn write(&self, headers: &[u8], body: &[u8]) -> io::Result<ArtLoc> {
        self.do_write(headers, body)
    }

    fn get_maxsize(&self) -> u64 {
        self.maxsize
    }

    fn get_oldest(&self) -> io::Result<Option<u64>> {
        self.do_get_oldest()
    }
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

// Mini statvfs implementation.
use std::ffi;

#[allow(dead_code)]
struct StatVfs {
    // bytes total
    b_total:    u64,
    // bytes available
    b_avail:    u64,
    // bytes in use.
    b_used:     u64,
}

impl StatVfs {
    fn stat(path: impl AsRef<Path>) -> io::Result<StatVfs> {
        let pathstr = match path.as_ref().to_str() {
            None => return Err(io::Error::new(io::ErrorKind::Other,
                                              format!("{:?}: invalid path", path.as_ref()))),
            Some(s) => s,
        };
        let cpath = ffi::CString::new(pathstr.as_bytes()).unwrap();
        let mut sv: libc::statvfs = unsafe { mem::zeroed() };
        let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut sv) };
        if rc != 0 {
            Err(io::Error::last_os_error())
        } else {
            let bs = sv.f_frsize;
            Ok(StatVfs{
                b_total:    bs * (sv.f_blocks - (sv.f_bfree - sv.f_bavail)) as u64,
                b_avail:    bs * sv.f_bavail as u64,
                b_used:     bs * (sv.f_blocks - sv.f_bfree) as u64,
            })
        }
    }
}

