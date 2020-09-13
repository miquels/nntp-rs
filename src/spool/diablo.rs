//! Diablo spool implementation.
//!
//! Reimplementation of the article spool from Matt Dillon's Diablo.
//!
//! Each spool contains 'D.xxxxxxxx' directories. where 'xxxxxxxx' is a timestamp
//! in hex: minutes since the unix epoch. Each directory then contains data files,
//! which are named 'B.xxxx' where 'xxxx' is a hex number 0..4095 (12 bits). Each
//! data file contains multiple articles.
//!
//! A storage token for diablo spool is built from:
//!
//! - spool number (u8, 0 .. 99)
//! - D. directory  (u32)
//! - B. file (u16, 12 bits)
//! - file offset (32 bits)
//! - article length (32 bits)
//!
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::io::Error as IoError;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use regex::Regex;
use typic::{self, stability::StableABI, transmute::StableTransmuteInto};

use super::{ArtLoc, ArtPart, Backend, MetaSpool, SpoolArt, SpoolBackend, SpoolDef};
use crate::util::byteorder::*;
use crate::util::{self, format, Buffer, UnixTime};

const MAX_SPOOLFILE_SIZE: u64 = 1_000_000_000;
const DFL_DIR_REALLOCINT: u32 = 600;
const EXPIRE_CHECK: u32 = 300;

/// A diablo spool instance.
///
/// Can be used for reading and writing articles from/to this spool.
#[rustfmt::skip]
#[derive(Clone)]
pub struct DSpool {
    path:               PathBuf,
    rel_path:           String,
    spool_no:           u8,
    file_reallocint:    u32,
    dir_reallocint:     u32,
    minfree:            u64,
    maxsize:            u64,
    weight:             u32,
    keeptime:           u64,
    shared:             Arc<DSpoolShared>,
}

struct DSpoolShared {
    expire_lock: Mutex<bool>,
    writer:      Mutex<Writer>,
    oldest:      AtomicU64,
    last_expire: AtomicU64,
}

// The file we have open for writing.
#[derive(Default)]
#[rustfmt::skip]
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
#[typic::repr(C)]
#[derive(Debug, Default, StableABI)]
#[rustfmt::skip]
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
const DARTHEAD_SIZE: usize = 24;

impl DArtHead {
    pub fn from_bytes(src: [u8; DARTHEAD_SIZE]) -> DArtHead {
        DArtHead {
            magic1:     src[0],
            magic2:     src[1],
            version:    src[2],
            head_len:   src[3],
            store_type: src[4],
            _unused1:   src[5],
            _unused2:   src[6],
            _unused3:   src[7],
            arthdr_len: u32::from_ne_bytes(src[8..12].try_into().unwrap()),
            art_len:    u32::from_ne_bytes(src[12..16].try_into().unwrap()),
            store_len:  u32::from_ne_bytes(src[16..20].try_into().unwrap()),
            hdr_end:    src[20],
            _unused4:   src[21],
            _unused5:   src[22],
            _unused6:   src[23],
        }
    }

    pub fn to_bytes(self) -> [u8; DARTHEAD_SIZE] {
        self.transmute_into()
    }
}

// article location, this struct is serialized/deserialized
// in the entry for this article in the history file.
#[derive(Debug)]
#[rustfmt::skip]
struct DArtLocation {
    dir:    u32,
    file:   u16,
    pos:    u32,
    size:   u32,
}

fn to_location(loc: &ArtLoc) -> DArtLocation {
    let t = &loc.token;
    // Diablo always rounds down the value used to generate
    // the directory name to the nearest multiple of 10.
    let gmt = u32_from_le_bytes(&t[10..14]);
    DArtLocation {
        file: u16_from_le_bytes(&t[0..2]),
        pos:  u32_from_le_bytes(&t[2..6]),
        size: u32_from_le_bytes(&t[6..10]),
        dir:  (gmt / 10) * 10,
    }
}

fn from_location(loc: DArtLocation) -> ([u8; 16], u8) {
    let mut t = [0u8; 16];
    u16_write_le_bytes(&mut t[0..2], loc.file);
    u32_write_le_bytes(&mut t[2..6], loc.pos);
    u32_write_le_bytes(&mut t[6..10], loc.size);
    u32_write_le_bytes(&mut t[10..14], loc.dir);
    (t, 14)
}

fn read_darthead_at<N: Debug>(path: N, file: &fs::File, pos: u64) -> io::Result<DArtHead> {
    let mut buf = [0u8; DARTHEAD_SIZE];
    let n = file.read_at(&mut buf, pos)?;
    if n != DARTHEAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{:?}: short read", path),
        ));
    }
    Ok(DArtHead::from_bytes(buf))
}

fn article_readahead(part: &ArtPart, loc: &DArtLocation, file: &fs::File) {
    let size = match part {
        ArtPart::Stat => 0,
        ArtPart::Head => 16384,
        ArtPart::Article | ArtPart::Body => loc.size,
    };
    if size > 0 {
        util::read_ahead(file, loc.pos.into(), size.into());
    }
}

// Internal helper struct for expire.
struct ExpFile {
    path:     PathBuf,
    size:     u64,
    modified: SystemTime,
    created:  Option<SystemTime>,
}

/// This is the main backend implementation.
impl DSpool {
    /// Create a new diablo-type spool backend.
    pub fn new(cfg: &SpoolDef, ms: &MetaSpool) -> io::Result<Box<dyn SpoolBackend>> {
        let dir_reallocint = if ms.reallocint.as_secs() > 0 {
            // cannot go lower than 600 secs (10 mins).
            std::cmp::max(600, ms.reallocint.as_secs() as u32)
        } else {
            DFL_DIR_REALLOCINT
        };

        // round down dir_reallocint to the nearest multiple of 600 (i.e. 10 minutes).
        let dir_reallocint = (dir_reallocint / 600) * 600;

        // Set file_reallocint to dir_reallocint / 100, so that we get 100
        // files per directory. However, it can not go lower than 1 per minute.
        let file_reallocint = std::cmp::max(dir_reallocint / 100, 60);

        // minfree must be at least 10MB, if not force it.
        const TEN_MIB: u64 = 10 * 1024 * 1024;
        let minfree = {
            if cfg.minfree < TEN_MIB {
                log::warn!("spool {}: setting minfree to 10MiB", cfg.spool_no);
                TEN_MIB
            } else {
                cfg.minfree
            }
        };

        // Calculate the "weight" factor. It is the maximum size of this spool in GB.
        let sv = fs2::statvfs(&cfg.path)
            .map_err(|e| ioerr!(e.kind(), "spool {}: {}: {}", cfg.spool_no, cfg.path, e))?;
        let reserved = sv.free_space() - sv.available_space();
        let fs_size = sv.total_space() - reserved;
        let mut weight = if cfg.maxsize != 0 && cfg.maxsize < fs_size {
            cfg.maxsize / 1_000_000_000
        } else {
            fs_size / 1_000_000_000
        } as u32;
        if weight == 0 {
            weight = 1;
        }

        // Return DSpool.
        let ds = DSpool {
            path: PathBuf::from(&cfg.path),
            rel_path: cfg.rel_path.clone(),
            spool_no: cfg.spool_no,
            file_reallocint: file_reallocint,
            dir_reallocint: dir_reallocint,
            keeptime: cfg.keeptime.as_secs(),
            minfree: minfree,
            maxsize: cfg.maxsize,
            weight,
            shared: Arc::new(DSpoolShared {
                oldest:      AtomicU64::new(0),
                last_expire: AtomicU64::new(0),
                expire_lock: Mutex::new(true),
                writer:      Mutex::new(Writer::default()),
            }),
        };

        // Find the oldest article.
        ds.do_expire(true, false)?;
        if let Ok(Some(t)) = ds.get_oldest() {
            log::info!(
                "spool {} ({}): age of oldest article: {}",
                ds.spool_no,
                ds.rel_path,
                util::format::duration(&t.elapsed())
            );
        } else {
            log::info!("spool {} ({}): no articles", ds.spool_no, ds.rel_path);
        }

        Ok(Box::new(ds))
    }

    // locate file that holds the article, open it, read the DArtHead struct,
    // and return the info.
    // XXX TODO: cache a few open filehandles.
    fn open(&self, art_loc: &ArtLoc, part: &ArtPart) -> io::Result<(DArtHead, DArtLocation, fs::File)> {
        let loc = to_location(art_loc);
        //log::debug!("art location: {:?}", loc);
        let flnm = format!("D.{:08x}/B.{:04x}", loc.dir, loc.file);
        let mut path = self.path.clone();
        path.push(flnm);
        let file = fs::File::open(&path)?;

        article_readahead(&part, &loc, &file);
        let dh = read_darthead_at(&path, &file, loc.pos as u64)?;

        //log::debug!("art header: {:?}", dh);

        // lots of sanity checks !
        if dh.magic1 != 0xff || dh.magic2 != 0x99 || dh.version != 1 || dh.head_len != 24 {
            log::warn!("read({:?}): bad magic in header", dh);
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic in header"));
        }
        if dh.store_type != 1 && dh.store_type != 4 {
            log::warn!("read({:?}): unsupported store type", dh);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported store type",
            ));
        }
        let linesep_sz = 1 + (dh.store_type != 1) as u32;
        if dh.arthdr_len + linesep_sz > dh.art_len {
            log::warn!("read({:?}): arthdr_len + line-seperator > art_len", dh);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid art_len or arthdr_len",
            ));
        }
        if dh.art_len + DARTHEAD_SIZE as u32 > dh.store_len {
            log::warn!("read({:?}): art_len + DARTHEAD_SIZE > store_len", dh);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid art_len or store_len",
            ));
        }
        // store_len includes \0 after the article, the histfile loc entry doesn't.
        if dh.store_len - 1 > loc.size {
            log::warn!(
                "read({:?}): article on disk larger than in history entry {:?}",
                dh,
                loc
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid store_len"));
        }
        Ok((dh, loc, file))
    }

    // Read an article.
    // SpoolBackend::read() forwards to this method.
    fn do_read(&self, art_loc: &ArtLoc, part: ArtPart, mut buffer: Buffer) -> io::Result<SpoolArt> {
        let (head, loc, mut file) = self.open(art_loc, &part)?;

        // Body offset.
        let linesep_sz = 1 + (head.store_type != 1) as u32; // LF or CRLF.
        let body_off = head.arthdr_len + linesep_sz;

        // If there is a body, it's at least DOT LF or DOT CRLF
        let mut body_size = None;
        let len = head.art_len - body_off;
        if len >= 1 + linesep_sz {
            // remove final .\r\n from the size.
            body_size = Some(len - 1 - linesep_sz);
        }

        let (start, len) = match part {
            ArtPart::Stat => {
                return Ok(SpoolArt {
                    data: buffer,
                    header_size: head.arthdr_len,
                    body_size,
                });
            },
            ArtPart::Head => (loc.pos + DARTHEAD_SIZE as u32, head.arthdr_len),
            ArtPart::Article => (loc.pos + DARTHEAD_SIZE as u32, head.art_len),
            ArtPart::Body => (loc.pos + DARTHEAD_SIZE as u32 + body_off, head.art_len - body_off),
        };
        file.seek(SeekFrom::Start(start as u64))?;

        if head.store_type == 1 {
            let reader = file.try_clone()?.take(len as u64);
            let reader = CrlfXlat::new(reader);
            buffer.reserve((len + len / 50) as usize);
            buffer.read_all(reader)?;
        } else {
            buffer.read_exact(file, len as usize)?;
        }

        match part {
            ArtPart::Article | ArtPart::Body => {
                if body_off == head.art_len {
                    // This is a header-only article. Those are
                    // stored without the final DOT CRLF. Add it.
                    buffer.push_str(".\r\n");
                }
                // Final check, what we have MUST end in \r\n.\r\n,
                // a lot of things depend on it.
                if !buffer.ends_with(b"\r\n.\r\n") {
                    return Err(ioerr!(InvalidData, "article corrupt on disk: {:?}", art_loc));
                }
            },
            _ => {},
        }

        Ok(SpoolArt {
            data: buffer,
            header_size: head.arthdr_len,
            body_size,
        })
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
        let mut files: Vec<_> = d.collect();
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
    fn do_write(&self, mut headers: Buffer, mut body: Buffer) -> io::Result<ArtLoc> {
        // lock the writer so we have unique access.
        let mut writer = self.shared.writer.lock();

        // see if we need to kick off an expire thread.
        // only if self.keeptime is set. otherwise we kick
        let unix_now = UnixTime::now();
        if self.keeptime > 0 {
            // if self.keeptime is set, check if we need to do an expire run.
            // otherwise, we check just before opening a new spool file.
            self.auto_expire(unix_now, false);
        }

        // Calculate what directory we should be writing to. It's the current
        // time, rounded down to the nearest multiple of dir_reallocint
        // (which itself is always a multiple of 600), then divided by 60.
        let now = unix_now.as_secs();
        let cur_dir = ((now / self.dir_reallocint as u64) as u32 * self.dir_reallocint) / 60;

        // check if we had this file open for more than file_reallocint secs,
        // or if we need to move to a new directory.
        if writer.fh.is_some() {
            if writer.tm + (self.file_reallocint as u64) < now || writer.dir != cur_dir {
                writer.fh.take();
            }
        }

        // need to start writing to a new file?
        let mut re_open = false;
        if writer.fh.is_none() {
            // Create directory if needed.
            let mut path = self.path.clone();
            path.push(format!("D.{:08x}", cur_dir));
            if writer.dir != cur_dir {
                if let Err(e) = fs::create_dir(&path) {
                    if e.kind() != io::ErrorKind::AlreadyExists {
                        return Err(io::Error::new(e.kind(), format!("create {:?}: {}", path, e)));
                    }
                    if writer.file == 0 {
                        // Existing dir. If we just started, try to re-use the
                        // last spool file used.
                        let file = self
                            .find_most_recent(&path)
                            .map_err(|e| IoError::new(e.kind(), format!("readdir {:?}: {}", path, e)))?;
                        writer.file = match file {
                            Some(f) => {
                                re_open = true;
                                f
                            },
                            None => 1,
                        };
                    }
                } else {
                    // created new directory. start at B.0001.
                    writer.file = 1;
                }
                writer.dir = cur_dir;
            }

            for _ in 0..32768 {
                // get spoolfile name.
                let mut name = path.clone();
                name.push(format!("B.{:04x}", writer.file));

                // see if the file already exists.
                match fs::metadata(&name) {
                    // maybe re-open an existing file, continue where
                    // we left off (can happen after a reload/restart).
                    Ok(_) => {
                        if !re_open {
                            // Limit to 2^15, it seems that the original diablo code also
                            // does that in some places (but not everywhere...). also
                            // make sure never to generate 0xffff or 0x0000.
                            writer.file = writer.file.wrapping_add(1) & 0x7fff;
                            if writer.file == 0 {
                                writer.file = 1;
                            }
                            continue;
                        }
                    },
                    Err(e) => {
                        match e.kind() {
                            // notfound is OK, all other errors are fatal.
                            io::ErrorKind::NotFound => {},
                            _ => return Err(IoError::new(e.kind(), format!("{:?}: {}", name, e))),
                        }
                    },
                };

                // actually open/create the spoolfile.
                let fh = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&name)
                    .map_err(|e| IoError::new(e.kind(), format!("{:?}: {}", name, e)))?;
                writer.tm = now;
                writer.fh = Some(fh);
                writer.name = name
                    .to_str()
                    .unwrap_or("impossible-non-utf8-filename")
                    .to_string();
                break;
            }

            // success?
            if writer.fh.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("{:?}: cannot create spool file", path),
                ));
            }

            // check if we need to run expire.
            self.auto_expire(unix_now, true);
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
        let buf: [u8; DARTHEAD_SIZE] = ah.to_bytes();

        // write header, article, trailing \0.
        fh.write_all(&buf)
            .and_then(|_| headers.write_all(&mut fh))
            .and_then(|_| body.write_all(&mut fh))
            .and_then(|_| fh.write(b"\0"))
            .map_err(|e| IoError::new(e.kind(), format!("writing to {}: {}", writer.name, e)))?;

        // store filehandle, unless we went over 1GB size.
        if pos + (store_len as u64) < MAX_SPOOLFILE_SIZE {
            writer.fh.get_or_insert(fh);
        }

        // build storage token
        let (token, toklen) = from_location(DArtLocation {
            dir:  writer.dir,
            file: writer.file,
            pos:  pos as u32,
            size: store_len - 1,
        });

        // return article location
        Ok(ArtLoc {
            storage_type: Backend::Diablo,
            spool: self.spool_no,
            token,
            toklen,
        })
    }

    fn auto_expire(&self, now: UnixTime, force: bool) {
        let last_expire: UnixTime = (&self.shared.last_expire).into();
        if now - last_expire >= Duration::new(EXPIRE_CHECK as u64, 0) || force {
            let this = self.clone();
            std::thread::spawn(move || {
                let _guard = match this.shared.expire_lock.try_lock() {
                    Some(guard) => guard,
                    None => {
                        log::debug!("auto_expire: already running");
                        return;
                    },
                };
                now.to_atomic(&this.shared.last_expire);
                if let Err(e) = this.do_expire(false, false) {
                    log::error!("auto_expire: {}", e);
                }
            });
        }
    }

    // Remove oldest data until
    //
    // - if 'keeptime' is set: we have removed all articles older than 'keeptime', and
    // - if 'minfree' is set: 'minfree' bytes are available on the filesystem, and
    // - if 'maxsize' is set: filesystem usage is < 'maxsize'
    //
    // We also update the `self.shared.oldest` member variable to the timestamp
    // of the oldest article after the expire.
    //
    // If `stat_only` is true, no files are actually removed, only `self.shared.oldest` gets updated.
    // If `dry_run` is true, go through the motions, but don't actually remove articles.
    //
    fn do_expire(&self, stat_only: bool, dry_run: bool) -> io::Result<u64> {
        let mut to_delete = 0;
        let mut do_expire = false;

        if self.minfree > 0 || self.maxsize > 0 {
            // get filesystem stats.
            let sv = fs2::statvfs(&self.path)
                .map_err(|e| ioerr!(e.kind(), "spool {}: {:?}: {}", self.spool_no, self.path, e))?;

            // if there's not enough free space, find out how much we need to free.
            if self.minfree > 0 && sv.available_space() < self.minfree {
                // to calculate how much we have to delete, make minfree 10% bigger.
                let minfree = self.minfree + self.minfree / 10;
                to_delete = minfree - sv.available_space();
                do_expire = true;
            }

            // if we have used too much space, find out how much we need to free.
            let used = sv.total_space() - sv.free_space();
            if self.maxsize > 0 && used > self.maxsize {
                // to calculate how much we have to delete, make maxsize 5% smaller.
                // never more than 50GB smaller though.
                let mut maxsize = self.maxsize - self.maxsize / 20;
                if maxsize + 50_000_000_000 < self.maxsize {
                    maxsize = self.maxsize - 50_000_000_000;
                }
                to_delete = std::cmp::max(to_delete, used - maxsize);
                do_expire = true;
            }
        }

        let mut keeptime = self.keeptime;
        if self.keeptime > 0 {
            if let Ok(Some(oldest)) = self.get_oldest() {
                if oldest + Duration::new(self.keeptime, 0) < UnixTime::now() {
                    do_expire = true;
                }
            } else {
                do_expire = true;
            }
            // to calculate how much we have to delete, scale down
            // keeptime by 10%, but not more than one hour.
            keeptime -= keeptime / 10;
            if keeptime + 3600 < self.keeptime {
                keeptime = self.keeptime - 3600;
            }
        }

        if !do_expire && !stat_only && !dry_run {
            return Ok(0);
        }

        let mut deleted = 0;

        let mut dirs: Vec<_> = fs::read_dir(&self.path)
            .map_err(|e| ioerr!(e.kind(), "spool {}: {:?}: {}", self.spool_no, self.path, e))?
            .filter_map(|r| r.ok())
            .filter_map(|r| r.file_name().into_string().ok())
            .filter(|f| f.starts_with("D.") && f.len() == 10)
            .collect();
        dirs.sort();

        for dirname in &dirs {
            // filename contains a timestamp in hex, in minutes.
            let dir_timestamp = match u64::from_str_radix(&dirname[2..], 16) {
                Ok(w) => UnixTime::from_secs(w * 60),
                Err(_) => continue,
            };

            // read directory. return an error if we fail - we can't ignore
            // it and continue, since we then might start to delete
            // articles that are too new.
            let mut dirpath = self.path.clone();
            dirpath.push(dirname);
            let files = fs::read_dir(&dirpath)
                .map_err(|e| ioerr!(e.kind(), "spool {}: {:?}: {}", self.spool_no, dirpath, e))?;

            // read all files. filter out the ones that start with "B." and
            // get their `modified` time. Then sort by `modified`.
            let mut files: Vec<_> = files
                .filter_map(|f| {
                    let file = f.ok()?;
                    if !file.file_name().to_str()?.starts_with("B.") {
                        return None;
                    }
                    let meta = file.metadata().ok()?;
                    let modified = meta.modified().ok()?;
                    Some(ExpFile {
                        path: file.path(),
                        size: meta.blocks() * 512u64,
                        modified,
                        created: meta.created().ok(),
                    })
                })
                .collect();
            files.sort_unstable_by(|a, b| a.modified.cmp(&b.modified));

            // Maybe initialize 'self.shared.oldest' timestamp.
            if !files.is_empty() && self.shared.oldest.load(Ordering::Acquire) == 0 {
                dir_timestamp.to_atomic(&self.shared.oldest);
            }

            // now walk over the files in the directory.
            let mut files: VecDeque<_> = files.into();
            let now = SystemTime::now();
            let mut removed = 0;
            while files.len() > 0 && !stat_only {
                let &ExpFile {
                    ref path,
                    size,
                    modified,
                    ..
                } = &files[0];
                let age = now.duration_since(modified).map(|d| d.as_secs()).unwrap_or(0);

                if (to_delete == 0 || deleted >= to_delete) && (keeptime == 0 || age <= keeptime) {
                    break;
                }

                // delete until we have enough space.
                if !dry_run {
                    fs::remove_file(&path).map_err(|e| {
                        ioerr!(
                            e.kind(),
                            "spool {}: expire failed: {:?}: {}",
                            self.spool_no,
                            path,
                            e
                        )
                    })?;
                }
                removed += size;
                deleted += size;
                files.pop_front();
            }

            if removed > 0 {
                log::info!(
                    "expire: spool {}: {}: removed {}",
                    self.spool_no,
                    dirname,
                    format::size(removed)
                );
            }

            // if we removed all the files, remove the directory as well,
            // and then continue with the next directory.
            if files.is_empty() {
                if !stat_only && !dry_run {
                    fs::remove_dir(&dirpath)
                        .map_err(|e| ioerr!(e.kind(), "spool: expire failed: {:?}: {}", dirpath, e))?;
                }
                continue;
            }

            self.update_oldest(dir_timestamp, &files);
            break;
        }

        Ok(deleted)
    }

    // Check the validity of all 'created' timestamps. They must all be OK,
    // and fall between dir_timestamp and dir_timestamp + dir_reallocint.
    // Then find the oldest timestamp.
    fn update_oldest(&self, dir_timestamp: UnixTime, files: &VecDeque<ExpFile>) {
        dir_timestamp.to_atomic(&self.shared.oldest);
        let max = dir_timestamp + Duration::new(self.dir_reallocint as u64, 0);
        let mut oldest = None;
        for file in files {
            let tm = match file.created {
                Some(tm) => UnixTime::from(tm),
                None => return,
            };
            if tm < dir_timestamp || tm > max {
                return;
            }
            if let Some(oldest) = oldest.as_mut() {
                if tm < *oldest {
                    *oldest = tm;
                }
            } else {
                oldest = Some(tm);
            }
        }
        if let Some(oldest) = oldest {
            oldest.to_atomic(&self.shared.oldest);
        }
    }

    // Generate a line in dqueue spool file format.
    fn do_token_to_text(&self, art_loc: &ArtLoc, msgid: &str) -> String {
        let dart_loc = to_location(art_loc);
        format!(
            "{}{}D.{:08x}/B.{:04x} {} {},{}",
            self.rel_path,
            if self.rel_path.len() > 0 { "/" } else { "" },
            dart_loc.dir,
            dart_loc.file,
            msgid,
            dart_loc.pos,
            dart_loc.size,
        )
    }

    // Parse a line in dqueue spool file format.
    fn do_text_to_token(&self, text: &str) -> Option<(ArtLoc, String)> {
        static PARSE_TOKEN: Lazy<Regex> = Lazy::new(|| {
            let re = r"^(.*/|)D.([0-9a-f]{8})/B.([0-9a-f]{4}) (<[^>]*>) (\d+),(\d+)$";
            Regex::new(re).expect("could not compile PARSE_TOKEN regexp")
        });

        let s = PARSE_TOKEN.captures(text)?;
        let dir = u32::from_str_radix(&s[2], 16).ok()?;
        let file = u16::from_str_radix(&s[3], 16).ok()?;
        let msgid = s[4].to_string();
        let pos: u32 = s[5].parse().ok()?;
        let size: u32 = s[6].parse().ok()?;

        // Build storage token and ArtLoc.
        let (token, toklen) = from_location(DArtLocation { dir, file, pos, size });
        let art_loc = ArtLoc {
            storage_type: Backend::Diablo,
            spool: self.spool_no,
            token,
            toklen,
        };
        Some((art_loc, msgid))
    }

    fn do_token_to_json(&self, art_loc: &ArtLoc) -> serde_json::Value {
        let dart_loc = to_location(art_loc);
        let sep = if self.rel_path.len() > 0 { "/" } else { "" };
        serde_json::json!({
            "file":   format!("{}{}D.{:08x}/B.{:04x}", self.rel_path, sep, dart_loc.dir, dart_loc.file),
            "offset": dart_loc.pos,
            "length": dart_loc.size,
        })
    }
}

impl SpoolBackend for DSpool {
    fn get_type(&self) -> Backend {
        Backend::Diablo
    }

    fn read(&self, art_loc: &ArtLoc, part: ArtPart, buffer: Buffer) -> io::Result<SpoolArt> {
        self.do_read(art_loc, part, buffer)
    }

    fn write(&self, headers: Buffer, body: Buffer) -> io::Result<ArtLoc> {
        self.do_write(headers, body)
    }

    fn expire(&self, dry_run: bool) -> io::Result<u64> {
        self.do_expire(false, dry_run)
    }

    fn get_weight(&self) -> u32 {
        self.weight
    }

    fn get_oldest(&self) -> io::Result<Option<UnixTime>> {
        let t = UnixTime::from(&self.shared.oldest);
        if t.is_zero() {
            Ok(None)
        } else {
            Ok(Some(t))
        }
    }

    fn token_to_text(&self, art_loc: &ArtLoc, msgid: &str) -> String {
        self.do_token_to_text(art_loc, msgid).to_string()
    }

    fn text_to_token(&self, text: &str) -> Option<(ArtLoc, String)> {
        self.do_text_to_token(text)
    }

    fn token_to_json(&self, art_loc: &ArtLoc) -> serde_json::Value {
        self.do_token_to_json(art_loc)
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
        CrlfXlat {
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

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut]) -> io::Result<usize> {
        // log::debug!("XXX read_vectored starts, #bufs: {}", bufs.len());
        let mut done = 0;
        for idx in 0..bufs.len() {
            let l = bufs[idx].len();
            if l != 0 {
                let n = self.read(&mut bufs[idx][..])?;
                done += n;
                if n < l {
                    break;
                }
            }
        }
        // log::debug!("XXX read_vectored done, read {}", done);
        Ok(done)
    }
}
