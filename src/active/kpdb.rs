//! Read / write a database in diablo's `dkp` format.
//!
//! Format of a kpdb database:
//!
//! Starts with a 'head' line:
//!
//! ```text
//! $Vvv.vv aaaaaaaa mmmmmmmm cccccccc\n
//! ```
//!
//! - Vvv.vv:   00.00  (version 0)
//! - aaaaaaaa: append seq, hex, bumped whenver an append is made
//! - mmmmmmmm: last modify timestamp to detect manual editing
//! - cccccccc: appends since last sort
//!
//! Then each record has this format:
//!
//! ```text
//! +ssssssss.mmmm:record_key key=value [key=value..]\n
//! ```
//!
//! - ssssssss: sort offset field
//! - mmmm:     modification counter
//!
//! The file can also contain deleted records (garbage), those are lines
//! that start with a '-'. These are ignored.
//!
//! Diablo uses the "sort offset field" to order the database by record_key.
//! It does a "re-sort" when "cccccccc" >= 128, rewriting the "ssssssss" and
//! "mmmm" fields of all records, and resetting "aaaaaaaa".
//!
//! We simply read the entire file on startup and keep an in-memory
//! BTreeMap that maps each record_key to an offset and length,
//! so we do not need that information.
//!
//! It's not too hard to keep interoperability with diablo:
//!
//! - always set "cccccccc" to 10000000, this causes diablo to "re-sort"
//!   the database when it starts up, fixing the "sssssssss" fields it needs.
//! - always set "ssssssss" to 00000023 (offset of the first line)
//! - always set "mmmmm" to 0000 (diablo appears to not use it either).
//!
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::io::{self, ErrorKind, Seek, Write};

use fs2::FileExt as _;
use memmap::MmapMut;

const MAX_FILE_SIZE: usize = 4_000_000_000;
const MAX_RECORD_SIZE: usize = 8192;

const OFF_SEQ: usize = 8;
const OFF_MOD: usize = 17;
const OFF_SSEQ: usize = 26;
const OFF_REC: usize = 35;

type Range = std::ops::Range<usize>;

//////////////////////////////////////////////////////////////
// KpDb implementation
//////////////////////////////////////////////////////////////

/// An open database handle.
pub struct KpDb {
    // for each group, location and current value of artno_xref.
    records:     BTreeMap<String, RecordLoc>,
    // Number of deleted ('-') records.
    deleted:     u32,
    // The mmap'd file (impl's DerefMut &[u8]).
    data:        MmapMut,
    // Size of the mmap'ed file.
    datasz:      usize,
    // File descriptor of the file.
    file:        fs::File,
    // Data to be appended.
    ndata:       Vec<u8>,
    // Filename of db file
    path:        String,
}

impl KpDb {
    /// Open the database file, then read and check it.
    ///
    /// If 'create' is true, the file is created if it did not exist,
    /// but if it _did_ exist the open will fail.
    pub fn open(path: impl AsRef<str>, create: bool) -> io::Result<KpDb> {

        let path = path.as_ref();
        let file = if create {
            // Create and initialize.
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|e| io::Error::new(e.kind(), format!("kpdb: create {}: {}", path, e)))?;
            write!(file, "$V00.00 00000000 {:08x} 10000000\n", unixtime_now())
                .map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;
            file
        } else {
            // Open existing file.
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| io::Error::new(e.kind(), format!("kpdb: open {}: {}", path, e)))?
        };

        // Get an exclusive lock (flock(2)).
        file.try_lock_exclusive()
            .map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;

        // SAFETY: Unrelated processes should not modify the file.
        // We have a fcntl lock on the file so other instances of nntp-rs will
        // not ever modify the file concurrently.
        let data = unsafe { MmapMut::map_mut(&file) }?;

        if data.len() > MAX_FILE_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("kpdb: {}: file too big (>={})", path, MAX_FILE_SIZE),
            ));
        }

        // Decode the head.
        if data.len() < OFF_REC || data[OFF_SEQ-1] != b' ' || data[OFF_MOD-1] != b' ' || data[OFF_SSEQ-1] != b' ' || data[OFF_REC-1] != b'\n' {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("kpdb: {}: head: cannot parse", path),
            ));
        }
        if &data[0..7] != &b"$V00.00"[..] {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("kpdb: {}: head: unsupported version", path),
            ));
        }
        let _append_seq = u32_from_hex(&data[OFF_SEQ..OFF_SEQ+8]).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("kpdb: {}: head: field 2 damaged", path),
            )
        })?;
        let _modified = u32_from_hex(&data[OFF_MOD..OFF_MOD+8]).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("kpdb: {}: head: field 3 damaged", path),
            )
        })?;
        let _append_sseq = u32_from_hex(&data[OFF_SSEQ..OFF_SSEQ+8]).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("kpdb: {}: head: field 4 damaged", path),
            )
        })?;

        // Lock the file into memory, so that we never block on pagefaults.
        match region::lock(data.as_ptr(), data.len()) {
            Ok(guard) => {
                // Don't need the guard, unmap will unlock.
                std::mem::forget(guard);
            },
            Err(e) => log::warn!("kpdb: {}: cannot mlock: {}", path, e),
        }

        // Now walk over the individual records.
        let mut pos = OFF_REC;
        let mut lineno = 1;
        let datasz = data.len();

        let mut records = BTreeMap::new();
        let mut deleted = 0;

        loop {
            // Find the newline at the end of the line.
            let mut idx = pos;
            loop {
                if idx == datasz || data[idx] == b'\n' {
                    break;
                }
                idx += 1;
            }
            if idx == datasz {
                break;
            }
            lineno += 1;

            // We have a line.
            let line = &data[pos..idx];
            if line.len() >= MAX_RECORD_SIZE {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!("dactive.kp: line {}: too long", lineno),
                ));
            }
            let lpos = pos;
            pos = idx + 1;

            if line.len() > 0 && line[0] == b'-' {
                deleted += 1;
                continue;
            }

            // Now parse it, and turn it into a RecordLoc struct.
            // That struct has the offset and length of the line.
            let (name, record) = RecordLoc::new(line, lpos as u32, lineno)?;
            records.insert(name, record);
        }

        Ok(KpDb {
            records,
            deleted,
            data,
            datasz,
            file,
            ndata: Vec::new(),
            path: path.to_string(),
        })
    }

    /// Write in-memory updates to disk.
    ///
    /// We only have in-memory updates if we could not update-in-place,
    /// so usually (when simply updating counters) this does nothing.
    #[inline]
    pub fn flush(&mut self) -> io::Result<()> {
        if self.ndata.len() == 0 {
            return Ok(());
        }
        self.do_flush()
    }

    // Write a new file, then make it active.
    fn do_rewrite(&mut self) -> io::Result<()> {

        // Open new file as 'file.new'.
        let path = self.path.clone() + ".new";
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("kpdb: create {}: {}", path, e)))?;
        file.try_lock_exclusive()
            .map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;

        // Write header.
        let mut file = io::BufWriter::with_capacity(65536, file);
        write!(file, "$V00.00 00000000 {:08x} 10000000\n", unixtime_now())
            .map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;

        let mut records = BTreeMap::new();
        let mut offset = OFF_REC as u32;
        let mut lineno = 2;

        // Write records.
        for (group, rec) in &self.records {
            let r_off = rec.offset as usize;
            let r_len = rec.len as usize;
            file.write_all(&self.data[r_off .. r_off + r_len])
                .map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;
            file.write_all(&b"\n"[..])
                .map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;
            let nrec = RecordLoc {
                lineno,
                offset,
                len: rec.len,
            };
            records.insert(group.to_string(), nrec);

            lineno += 1;
            offset += rec.len as u32 + 1;
        }

        // Flush data, then mmap.
        let file = file.into_inner()
            .map_err(|e| io::Error::new(e.error().kind(), format!("kpdb: {}: {}", path, e)))?;
        file.sync_all().map_err(|e| io::Error::new(e.kind(), format!("kpdb: {}: {}", path, e)))?;
        let data = unsafe { MmapMut::map_mut(&file) }?;

        // Lock the file into memory, so that we never block on pagefaults.
        match region::lock(data.as_ptr(), data.len()) {
            Ok(guard) => {
                // Don't need the guard, unmap will unlock.
                std::mem::forget(guard);
            },
            Err(e) => log::warn!("kpdb: {}: cannot mlock: {}", self.path, e),
        }

        // Move new file over old file.
        fs::rename(&path, &self.path)
            .map_err(|e| io::Error::new(e.kind(), format!("kpdb: rename({} {}): {}", path, self.path, e)))?;

        // Use new data.
        self.records = records;
        self.deleted = 0;
        self.datasz = data.len();
        self.data = data;
        self.file = file;
        self.ndata = Vec::new();

        Ok(())
    }

    fn do_flush(&mut self) -> io::Result<()> {
        // Will never happen but check anyway.
        if self.datasz + self.ndata.len() > MAX_FILE_SIZE {
            panic!("kpdb: FATAL: {}: grown too big (>{})", self.path, MAX_FILE_SIZE);
        }

        // If we have collected too many updates / deletions, rewrite.
        if self.deleted > self.records.len() as u32 / 10 || self.deleted >= 1024 {
            return self.do_rewrite();
        }

        let ndata = std::mem::replace(&mut self.ndata, Vec::new());

        // First append 'ndata' to the database file. If that fails, try to
        // recover and get to a stable state, and return an IO error.
        let oldlen = self.file.seek(io::SeekFrom::End(0))?;
        if let Err(e) = self.file.write_all(&ndata) {
            match self.file.metadata() {
                Err(_) => {
                    // If we can't even fstat() anymore, the world is broken.
                    panic!(
                        "kpdb: FATAL: {}: partial written database file, can't recover: {}",
                        self.path, e
                    );
                },
                Ok(meta) => {
                    // See if we can recover.
                    if meta.len() != oldlen {
                        if let Err(_) = self.file.set_len(oldlen) {
                            // If ftruncate fails, there's no way to recover.
                            panic!(
                                "kpdb: FATAL: {}: partial written database file, can't recover: {}",
                                self.path, e
                            );
                        }
                    }
                    self.ndata = ndata;
                    return Err(io::Error::new(e.kind(), format!("kpdb: {}: {}", self.path, e)));
                },
            }
        }

        self.set(OFF_MOD, unixtime_now());
        self.set(OFF_SSEQ, 0x10000000);

        // Now do a new mmap. Too bad we cannot use mremap, this is expensive.
        // If this fails, we are in an unrecoverable state.
        let data = match unsafe { MmapMut::map_mut(&self.file) } {
            Ok(data) => data,
            Err(e) => panic!("kpdb: FATAL: mmap {}: {}", self.path, e),
        };

        // Lock the file into memory, so that we never block on pagefaults.
        match region::lock(data.as_ptr(), data.len()) {
            Ok(guard) => {
                // Don't need the guard, unmap will unlock.
                std::mem::forget(guard);
            },
            Err(e) => log::warn!("kpdb: {}: cannot mlock: {}", self.path, e),
        }

        self.datasz = data.len();
        self.data = data;

        Ok(())
    }

    /// Get an `Record` from the database. The `Record` holds a multiple
    /// key/value pairs.
    pub fn get(&self, key: &str) -> Option<Record<'_>> {
        self.records.get(key).map(move |e| Record::from_loc(e, self))
    }

    /// Get an `RecordMut` from the database. Like `Record`, but mutable.
    pub fn get_mut(&mut self, key: &str) -> Option<RecordMut<'_>> {
        if let Some(loc) = self.records.get(key) {
            Some(RecordMut::from_loc(loc.clone(), self))
        } else {
            None
        }
    }

    /// Remove a record from the database.
    pub fn remove<'a>(&'a mut self, key: &str) -> io::Result<()> {
        self.deleted += 1;
        if let Some(mut r) = self.get_mut(key) {
            r.line_mut()[0] = b'-';
            return Ok(());
        }
        self.deleted -= 1;
        Err(io::Error::new(ErrorKind::NotFound, "key not found"))
    }

    /// Insert a new Record in the database.
    ///
    /// Fails if the record already exists.
    pub fn insert<'a>(&'a mut self, key: &str, kvpairs: &HashMap<&'static str, String>) -> io::Result<()> {
        if self.records.contains_key(key) {
            return Err(io::Error::new(ErrorKind::AlreadyExists, "duplicate key"));
        }

        let mut s = format!("+00000023.0000:{}", key);
        for (k, v) in kvpairs {
            s.push_str(&format!(" {}={}", k, v));
        }

        let now = unixtime_now();
        s.push_str(&format!(" CTS={:08x} LMTS={:08x}\n", now, now));

        let start = self.datasz + self.ndata.len();
        self.ndata.extend_from_slice(s.as_bytes());
        let end = self.datasz + self.ndata.len();

        let record_loc = RecordLoc {
            lineno: 0,
            offset: start as u32,
            len:    (end - start) as u16,
        };
        self.records.insert(key.to_string(), record_loc);

        Ok(())
    }

    /// Iterate over all `Record`s in the database.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a str, Record<'a>)> {
        self.records
            .iter()
            .map(move |(k, v)| (k.as_str(), Record::from_loc(v, self)))
    }

    /// Get the keys of all records.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a str> {
        self.records.keys().map(|k| k.as_str())
    }

    fn set(&mut self, offset: usize, val: u32) {
        let d = format!("{:08x}", val);
        self.data[offset..offset+8].copy_from_slice(d.as_bytes());
    }
}

// An RecordLoc is a reference to an Record in the database,
// it's what we store in the in-memory btree to refer to
// the actual data by offset + len. `line` is for context
// information in errors etc.
#[derive(Clone)]
struct RecordLoc {
    lineno: u32,
    offset: u32,
    len:    u16,
}

impl RecordLoc {
    // Parse a line, check its validity, then return an `RecordLoc` for it.
    fn new(line: &[u8], offset: u32, lineno: u32) -> io::Result<(String, RecordLoc)> {
        // Some sanity checks.
        if line.len() < 18 || line[0] != b'+' || line[14] != b':' {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("dactive.kp: line {}: damaged", lineno),
            ));
        }

        // Must be UTF-8 (and is, because it should be ASCII).
        std::str::from_utf8(line).map_err(|_| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("dactive.kp: line {}: not ASCII/UTF-8", lineno),
            )
        })?;

        let record = Record { line, lineno };

        let name = record
            .get_name()
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("dactive.kp: line {}: no newsgroup name", lineno),
                )
            })?
            .to_string();

        // Now check that all fields that we need are present.
        //
        // NOTE: this does not belong in a generic kpdb implementation,
        // but as long as we _only_ use this for dactive.kp it's
        // a good consistency check.
        for key in vec!["NB", "NE", "NX", "S", "CTS", "LMTS"].into_iter() {
            record.get_str(key).ok_or_else(|| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("dactive.kp: line {}: {} missing", lineno, key),
                )
            })?;
        }

        Ok((
            name,
            RecordLoc {
                lineno,
                offset,
                len: line.len() as u16,
            },
        ))
    }
}

/// A handle for an record in the database.
#[derive(Clone)]
pub struct Record<'a> {
    lineno: u32,
    line:   &'a [u8],
}

impl<'a> Record<'a> {
    // internal build an record from an RecordLoc.
    fn from_loc<'b>(e: &RecordLoc, kpdb: &'b KpDb) -> Record<'b> {
        let mut data: &[u8] = &kpdb.data;
        let mut start = e.offset as usize;
        if start >= kpdb.datasz {
            // If it starts beyond the data in the file, it's a buffered
            // record that does not exist in the database file yet.
            //
            // That's an optimization, for when we add a lot of new records
            // in succession, so that we do not have to mremap() all the time.
            //
            // Adjust the offset and refer to that internal buffered data.
            start -= kpdb.datasz;
            data = &kpdb.ndata;
        }
        let end = start + e.len as usize;
        Record {
            lineno: e.lineno,
            line:   &data[start..end],
        }
    }

    /// Get a direct reference to the raw data of this record (name key1=val1 ...)
    pub fn get_raw(&'a self) -> &'a [u8] {
        if self.line.len() < 15 {
            &b""[..]
        } else {
            &self.line[15..]
        }
    }

    /// Get the name of this record. Can only fail if the database is corrupt.
    /// (Perhaps we should use a Result instead of an Option).
    pub fn get_name(&'a self) -> Option<&'a str> {
        get_name(self.line)
    }

    /// Lookup a value, return it as a string.
    pub fn get_str(&'a self, key: &str) -> Option<&'a str> {
        let range = get_range(key, self.line)?;
        std::str::from_utf8(&self.line[range]).ok()
    }

    /// Lookup a value, then if it is found percent-decode it.
    pub fn get_decoded(&'a self, key: &str) -> Option<Vec<u8>> {
        let range = get_range(key, self.line)?;
        Some(pct_decode(&self.line[range]))
    }

    /// Lookup a value, then if it is found interpret it as a number.
    pub fn get_u64(&'a self, key: &str) -> Option<u64> {
        let s = self.get_str(key)?;
        u64::from_str_radix(s, 10).ok()
    }
}

/// A handle for a mutable record in the database.
pub struct RecordMut<'a> {
    kpdb:       &'a mut KpDb,
    record_loc: RecordLoc,
    changed:    bool,
    modified:   Option<HashMap<&'static [u8], String>>,
}

impl<'a> RecordMut<'a> {
    // See Record::from_loc().
    fn from_loc<'b>(record_loc: RecordLoc, kpdb: &'b mut KpDb) -> RecordMut<'b> {
        RecordMut {
            kpdb,
            record_loc,
            changed: false,
            modified: None,
        }
    }

    // Since we keep an &mut KpDb in self, we cannot also keep a &line[u8]
    // there - no self-referencing structs in rust. So we get it via record_loc.
    fn line(&self) -> &[u8] {
        let mut data = &self.kpdb.data[..];
        let mut start = self.record_loc.offset as usize;
        if start >= self.kpdb.datasz {
            start -= self.kpdb.datasz;
            data = &self.kpdb.ndata[..];
        }
        let end = start + self.record_loc.len as usize;
        &data[start..end]
    }

    // Since we keep an &mut KpDb in self, we cannot also keep a &mut line[u8]
    // there - no self-referencing structs in rust. So we get it via record_loc.
    fn line_mut(&mut self) -> &mut [u8] {
        let mut data = &mut self.kpdb.data[..];
        let mut start = self.record_loc.offset as usize;
        if start >= self.kpdb.datasz {
            start -= self.kpdb.datasz;
            data = &mut self.kpdb.ndata[..];
        }
        let end = start + self.record_loc.len as usize;
        &mut data[start..end]
    }

    /// Set or replace a string value.
    pub fn set_str(&mut self, key: &'static str, val: &str) {
        self.changed = true;

        // If we can modify the value in-place, do so.
        let line = self.line_mut();
        if let Some(range) = get_range(key, line) {
            if range.end - range.start == val.len() {
                line[range].copy_from_slice(val.as_bytes());
                return;
            }
        }

        // Otherwise stash the new value, we will deal with it later (on Drop).
        self.modified
            .get_or_insert_with(|| HashMap::new())
            .insert(key.as_bytes(), val.to_string());
    }

    pub fn get_str(&'a self, key: &str) -> Option<&'a str> {
        if let Some(ref hm) = self.modified {
            if let Some(val) = hm.get(key.as_bytes()) {
                return Some(val);
            }
        }
        let line = self.line();
        let range = get_range(key, line)?;
        std::str::from_utf8(&line[range]).ok()
    }

    pub fn get_u64(&'a self, key: &str) -> Option<u64> {
        let s = self.get_str(key)?;
        u64::from_str_radix(s, 10).ok()
    }
}

/// On drop, see if the self.modified hashmap is non-empty. If it is
/// not, we could not modify-in-place, so write a new record for this line.
impl<'a> Drop for RecordMut<'a> {
    fn drop(&mut self) {
        if self.changed {
            self.set_str("LMTS", &format!("{:08x}", unixtime_now()));
        }
        if let Some(mut hm) = self.modified.take() {
            // invalidate old record.
            self.line_mut()[0] = b'-';
            self.kpdb.deleted += 1;

            // Now remember location in ndata so we can update self.records.
            let start = self.kpdb.ndata.len() + self.kpdb.datasz;
            let mut name = Vec::new();

            // Make a copy of the line, validate, start building new record.
            let mut line = self.line().to_vec();
            line[0] = b'+';
            self.kpdb.ndata.extend_from_slice(&line[..15]);

            // Walk over all fields.
            let fields = line[15..].split(|b| b.is_ascii_whitespace());
            let mut first = true;
            for field in fields {
                if field.is_empty() {
                    continue;
                }

                // Split key/value on '='. Add key to new record.
                let mut kv = field.splitn(2, |&b| b == b'=');
                let key = kv.next().unwrap();
                if !first {
                    self.kpdb.ndata.push(b' ');
                }
                self.kpdb.ndata.extend_from_slice(key);

                if first {
                    // This was the name of the record, not a key/value pair.
                    first = false;
                    name = key.to_vec();
                    //println!("XXX Drop {:?}: updating: {:?}", std::str::from_utf8(key), hm);
                    continue;
                }

                // If this key was also present in 'modified', use it.
                //println!("XXX check key {:?}", std::str::from_utf8(key));
                if let Some(newval) = hm.remove(key) {
                    //println!("XXX UPD: = {}", newval);
                    self.kpdb.ndata.push(b'=');
                    self.kpdb.ndata.extend_from_slice(newval.as_bytes());
                } else if let Some(val) = kv.next() {
                    // use the original value.
                    self.kpdb.ndata.push(b'=');
                    self.kpdb.ndata.extend_from_slice(val);
                }
            }

            // And add new key/value pairs.
            for (key, newval) in hm.into_iter() {
                self.kpdb.ndata.push(b' ');
                self.kpdb.ndata.extend_from_slice(key);
                self.kpdb.ndata.push(b'=');
                self.kpdb.ndata.extend_from_slice(newval.as_bytes());
            }

            // finalize.
            self.kpdb.ndata.push(b'\n');
            let end = self.kpdb.ndata.len() + self.kpdb.datasz;
            //println!("XXX ndata is now: {:?}", std::str::from_utf8(&self.kpdb.ndata));
            let name = String::from_utf8(name).unwrap();
            self.kpdb.records.insert(
                name,
                RecordLoc {
                    lineno: 0,
                    offset: start as u32,
                    len:    (end - start) as u16,
                },
            );
        }
    }
}

// Helper.
// Scan the line to find 'key=value', then return the Range of value.
fn get_range(key: &str, line: &[u8]) -> Option<Range> {
    if line.len() < 15 {
        return None;
    }
    let mut idx = 15;

    let key = key.as_bytes();

    loop {
        // skip leading space.
        while idx < line.len() {
            if !line[idx].is_ascii_whitespace() {
                break;
            }
            idx += 1;
        }
        let start = idx;
        let mut eq = 0;

        // find end.
        while idx < line.len() {
            if line[idx] == b'=' {
                eq = idx;
            }
            if line[idx].is_ascii_whitespace() {
                break;
            }
            idx += 1;
        }

        if idx == start {
            return None;
        }
        if eq == 0 {
            continue;
        }

        if &line[start..eq] == key {
            if std::str::from_utf8(&line[eq + 1..idx]).is_ok() {
                return Some(Range {
                    start: eq + 1,
                    end:   idx,
                });
            }
        }
    }
}

// Helper.
// Get the name of this record.
fn get_name(line: &[u8]) -> Option<&str> {
    if line.len() < 15 {
        return None;
    }
    let mut idx = 15;

    // skip leading space.
    while idx < line.len() {
        if !line[idx].is_ascii_whitespace() {
            break;
        }
        idx += 1;
    }
    let start = idx;

    // find end.
    while idx < line.len() {
        if line[idx] == b'=' {
            return None;
        }
        if line[idx].is_ascii_whitespace() {
            break;
        }
        idx += 1;
    }

    if idx == start {
        return None;
    }

    std::str::from_utf8(&line[start..idx]).ok()
}

// Percent-decode a string. Returns a Vec<u8> since the
// result might not be valid utf-8.
fn pct_decode(d: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(d.len());
    let mut i = 0;
    while i < d.len() {
        if d[i] == b'%' && d[i..].len() >= 3 {
            if let Ok(hex) = std::str::from_utf8(&d[i + 1..i + 3]) {
                if let Ok(c) = u8::from_str_radix(hex, 16) {
                    v.push(c);
                    i += 3;
                    continue;
                }
            }
        }
        v.push(d[i]);
        i += 1;
    }
    v
}

/// Percent-encode a string.
pub fn percent_encode(s: impl AsRef<[u8]>) -> String {
    let mut p = String::new();
    for b in s.as_ref().iter().map(|&b| b) {
        if (b >= b'0' && b <= b'9') || (b >= b'A' && b <= b'Z') || (b >= b'a' && b <= b'z') {
            p.push(b as char);
        } else {
            p.push_str(&format!("%{:02x}", b));
        }
    }
    p
}

// helper.
fn u32_from_hex(data: &[u8]) -> Option<u32> {
    let hex = std::str::from_utf8(data).ok()?;
    u32::from_str_radix(hex, 16).ok()
}

// helper
fn unixtime_now() -> u32 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;

    #[test]
    fn test_kpdb() {
        let _ = fs::remove_file("test.kp");
        assert!(KpDb::open("test.kp", false).is_err());

        let mut db = KpDb::open("test.kp", true).expect("test.kp");

        let mut kv = HashMap::new();
        kv.insert("NB", "0000000001".into());
        kv.insert("NE", "0000000000".into());
        kv.insert("NX", "0000000000".into());
        kv.insert("S", "y".into());

        db.insert("test.1", &kv).expect("insert");

        kv.insert("GD", "testgroup2".into());
        db.insert("test.2", &kv).expect("insert");

        db.flush().expect("remap");

        {
            let mut t1 = db.get_mut("test.1").expect("test.1");
            assert!(t1.get_str("S").unwrap() == "y");

            t1.set_str("S", "n");
            assert!(t1.get_str("S").unwrap() == "n");

            t1.set_str("GD", "testgroup1");
            drop(t1);

            db.flush().expect("remap");
        }

        let t1 = db.get("test.1").expect("test.1");
        assert!(t1.get_str("GD").unwrap() == "testgroup1");
    }
}
