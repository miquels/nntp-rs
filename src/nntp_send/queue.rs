//! Outgoing queue management.
//!
use std::collections::{HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use once_cell::sync::Lazy;
use regex::Regex;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task;

use crate::nntp_send::PeerArticle;
use crate::spool::Spool;
use crate::util;

const QUEUE_ROTATE_SECS: u64 = 300;

/// A set of items that we got from the queue by calling read_items().
#[derive(Default, Clone, Debug)]
pub(super) struct QItems {
    pub(super) id:    u64,
    pub(super) items: String,
    pub(super) pos:   usize,
    pub(super) done:  bool,
}

impl QItems {
    pub(super) fn len(&self) -> usize {
        self.items[self.pos..].split('\n').filter(|s| *s != "").count()
    }

    pub(super) fn next_str(&mut self) -> Option<&str> {
        loop {
            match self.items[self.pos..].find('\n') {
                Some(len) => {
                    let s = &self.items[self.pos..self.pos + len];
                    self.pos += len + 1;
                    if s == "" {
                        continue;
                    }
                    return Some(s);
                },
                None => {
                    if self.pos != self.items.len() {
                        let s = &self.items[self.pos..];
                        self.pos = self.items.len();
                        return Some(s);
                    } else {
                        return None;
                    }
                },
            }
        }
    }

    pub(super) fn next_art(&mut self, spool: &Spool) -> Option<PeerArticle> {
        while let Some(s) = self.next_str() {
            if let Some((location, msgid)) = spool.text_to_token(s) {
                return Some(PeerArticle {
                    msgid,
                    location,
                    size: 0,
                    from_backlog: true,
                    deferred: 0,
                });
            }
        }
        None
    }
}

impl Drop for QItems {
    fn drop(&mut self) {
        // Make sure the id is always returned!
        if !self.done {
            log::error!("QItems.drop(): id {} was NOT returned to the queue", self.id);
        }
    }
}

#[derive(Default)]
struct QWriter {
    label:       String,
    dir:         String,
    file:        Option<fs::File>,
    next_seq:    u64,
    last_rotate: u64,
    is_empty:    bool,
}

impl QWriter {
    // Rotate the queue file.
    //
    // If "not_next" is true, rotate to the current S.* file,
    // not the next one, and don't update anything. This is used
    // if all S.* queue files are gone but we still want to
    // rotate the current queue file to process it right now.
    async fn rotate(&mut self, low_seq: u64, not_next: bool) -> io::Result<Option<QFile>> {
        // Skip if we're empty.
        if self.is_empty {
            if !not_next {
                self.last_rotate = util::unixtime();
            }
            return Ok(None);
        }

        let mut cur_path = PathBuf::from(&self.dir);
        cur_path.push(&self.label);

        let seq = self.next_seq - (not_next as u64);
        let mut rot_path = PathBuf::from(&self.dir);
        let name = format!("{}.S{:05}", self.label, seq);
        rot_path.push(&name);

        match fs::rename(&cur_path, &rot_path).await {
            Ok(()) => {},
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    self.file.take();
                    self.is_empty = true;
                    return Ok(None);
                }
                log::error!(
                    "QWriter::rotate: {}: rename({:?}, {:?}): {}",
                    self.label,
                    cur_path,
                    rot_path,
                    e,
                );
                return Err(e);
            },
        }

        self.file.take();
        self.is_empty = true;

        if !not_next {
            self.next_seq += 1;
            self.last_rotate = util::unixtime();
            self.write_seqno(low_seq).await.unwrap_or_else(|e| {
                log::warn!("QWriter::rotate: {}: write_seqno: {}", self.label, e);
            });
        }

        let time = fs::metadata(&rot_path)
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|t| t.as_secs())
            .unwrap_or(util::unixtime());
        Ok(Some(QFile {
            time,
            seq,
            name,
            erred: false,
        }))
    }

    // Read the high sequence number and timestamp from the .<LABEL>.seq file.
    async fn read_seqno(&mut self, next_seq: u64) {
        let mut path = PathBuf::from(&self.dir);
        path.push(&format!(".{}.seq", &self.label));

        // Set some defaults in case there is no .seq file.
        if self.last_rotate == 0 {
            self.last_rotate = util::unixtime();
        }
        self.next_seq = std::cmp::max(2, next_seq);

        match self.do_read_seqno(&path).await {
            Ok((seq, time)) => {
                self.next_seq = std::cmp::max(seq, next_seq);
                self.last_rotate = time;
            },
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    log::warn!("QWriter::read_seqno: {}: {} (deleting)", self.label, e);
                    let _ = fs::remove_file(&path).await;
                }
            },
        }
        //log::debug!("QWriter::read_seqno: {}: next_seq {}", self.label, self.next_seq);
    }

    async fn do_read_seqno(&self, path: &Path) -> io::Result<(u64, u64)> {
        let data = fs::read(&path).await?;
        let data = String::from_utf8(data).map_err(|_| ioerr!(InvalidData, "{:?}: utf8 error", path))?;
        let words: Vec<_> = data.split_whitespace().collect();
        if words.len() != 3 {
            return Err(ioerr!(InvalidData, "{:?}: corrupt .seq file", path));
        }
        let seq_high = words[1]
            .parse::<u64>()
            .map_err(|_| ioerr!(InvalidData, "{:?}: corrupt .seq file", path))?;
        let time = u64::from_str_radix(words[2], 16)
            .map_err(|_| ioerr!(InvalidData, "{:?}: corrupt .seq file", path))?;
        Ok((seq_high, time))
    }

    async fn write_seqno(&self, mut low_seqno: u64) -> io::Result<()> {
        if low_seqno == 0 {
            low_seqno = self.next_seq - 1;
        }
        let mut path = PathBuf::from(&self.dir);
        path.push(&format!(".{}.seq", &self.label));
        let data = format!("{} {} {:x}\n", low_seqno, self.next_seq, util::unixtime());
        fs::write(&path, &data).await
    }

    async fn open_qfile(&mut self, create: bool) -> io::Result<()> {
        let mut path = PathBuf::from(&self.dir);
        path.push(&self.label);
        self.is_empty = true;
        let file = match open_append(&path, create).await {
            Ok(file) => file,
            Err(e) => {
                if !create && e.kind() == io::ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(e);
            },
        };
        match file.metadata().await {
            Ok(m) if m.len() > 0 => self.is_empty = false,
            _ => {},
        }
        self.file = Some(file);
        Ok(())
    }
}

#[derive(Default)]
struct QReader {
    label:       String,
    dir:         String,
    qfiles:      Vec<QFile>,
    last_id:     u64,
    maxqueue:    u32,
    cur_file:    Option<tokio::io::BufReader<fs::File>>,
    cur_name:    Option<String>,
    cur_eof:     bool,
    cur_offset:  u64,
    cur_size:    u64,
    // id, offset
    outstanding: HashSet<u64>,
    buffered:    VecDeque<QItems>,
}

impl QReader {
    fn reset_cur(&mut self) {
        self.cur_file.take();
        self.cur_name.take();
        self.cur_eof = false;
        self.cur_offset = 0;
        self.cur_size = 0;
        self.outstanding.clear();
        self.buffered.clear();
    }

    // Close the current qfile and remove it.
    async fn close_qfile(&mut self) {
        //log::debug!(
        //    "close_qfile: self.cur_eof {}, outstanding: {}",
        //    self.cur_eof,
        //    self.outstanding.len()
        //);

        if let Some(name) = self.cur_name.take() {
            // Remove file from filesystem.
            let mut path = PathBuf::from(&self.dir);
            path.push(&name);
            //log::trace!("removing {:?}", path);
            if let Err(e) = fs::remove_file(&path).await {
                log::error!("QReader::check_cur_eof: remove({:?}): {}", path, e);
            }

            // And remove file from in-memory list.
            for i in 0..self.qfiles.len() {
                if self.qfiles[i].name == name {
                    //log::trace!("close_qfile: remove from in-mem list: {}", name);
                    self.qfiles.remove(i);
                    break;
                }
            }
        }

        // zero out all "current queue file" data.
        self.reset_cur();
    }

    async fn open_qfile(&mut self) -> bool {
        if self.cur_file.is_some() {
            if !self.cur_eof {
                return true;
            }
            if self.outstanding.len() > 0 {
                return false;
            }
            self.close_qfile().await;
        }

        // Before opening the next file, expire the old queue files.
        self.expire_qfiles().await;

        // Loop over all files in the queue.
        for i in 0..self.qfiles.len() {
            let qfile = &self.qfiles[i];
            if qfile.erred {
                continue;
            }

            let mut path = PathBuf::from(&self.dir);
            path.push(&qfile.name);

            // open.
            let file = match fs::File::open(&path).await {
                Ok(file) => file,
                Err(e) => {
                    if e.kind() == io::ErrorKind::NotFound {
                        continue;
                    }
                    // Can't open queuefile. Skip it for now. We'll try
                    // again at the next directory scan, probably.
                    log::error!("Queue::open_qfile: open {:?}: {}", path, e);
                    self.qfiles[i].erred = true;
                    continue;
                },
            };
            // stat.
            let cur_size = match file.metadata().await {
                Ok(meta) => {
                    let len = meta.len();
                    if len == 0 {
                        continue;
                    }
                    len
                },
                Err(e) => {
                    log::error!("Queue::open_qfile: {:?}.metadata(): {}", path, e);
                    self.qfiles[i].erred = true;
                    continue;
                },
            };

            // We have opened the next queue file.
            let cur_name = qfile.name.to_string();
            self.cur_name = Some(cur_name);
            self.cur_file = Some(tokio::io::BufReader::new(file));
            self.cur_size = cur_size;
            break;
        }

        self.cur_file.is_some()
    }

    // scan the queue directory.
    async fn scan_qfiles(&mut self) -> io::Result<()> {
        let mut rd = fs::read_dir(&self.dir).await.map_err(|e| {
            let e = ioerr!(e.kind(), "QReader::scan_qfiles: {}: {}", self.dir, e);
            log::error!("{}", e);
            e
        })?;

        let mut qfiles = Vec::new();
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Some(qfile) = QFile::from_dirent(&self.label, &entry).await {
                //log::debug!("qfile: {:?}", qfile);
                qfiles.push(qfile);
            }
        }
        qfiles.sort_unstable_by(|a, b| {
            if a.seq > 0 && b.seq > 0 {
                return a.seq.partial_cmp(&b.seq).unwrap();
            }
            let ord = a.time.partial_cmp(&b.time).unwrap();
            if ord == std::cmp::Ordering::Equal {
                a.name.partial_cmp(&b.name).unwrap()
            } else {
                ord
            }
        });
        //log::debug!("qfiles: {:?}", qfiles);
        self.qfiles = qfiles;
        Ok(())
    }

    // Find queue files that are too old and remove them.
    async fn expire_qfiles(&mut self) {
        if self.maxqueue == 0 {
            return;
        }

        let mut remove = Vec::new();
        {
            let now = util::unixtime();
            let min_age = now - QUEUE_ROTATE_SECS * (self.maxqueue as u64);

            for i in 0..self.qfiles.len() {
                if self.qfiles[i].time >= min_age {
                    if i > 0 {
                        remove = self.qfiles.split_off(i);
                        std::mem::swap(&mut self.qfiles, &mut remove);
                    }
                    break;
                }
            }
        }

        // remove the old files.
        if remove.len() > 0 {
            let dir = self.dir.clone();
            let _ = task::spawn_blocking(move || {
                while let Some(qfile) = remove.pop() {
                    let mut path = PathBuf::from(&dir);
                    path.push(&qfile.name);
                    //log::debug!("QReader::expire_qfiles: removing {:?}: too old", &qfile.name);
                    let _ = std::fs::remove_file(&path);
                }
            })
            .await;
        }
    }

    // get the lowest sequence number.
    pub(super) fn low_seq(&self) -> u64 {
        let mut seq = 0;
        for qfile in &self.qfiles {
            if seq == 0 || qfile.seq < seq {
                seq = qfile.seq;
            }
        }
        seq
    }
}

struct InnerQueue {
    qreader: Mutex<QReader>,
    qwriter: Mutex<QWriter>,
}

#[derive(Clone)]
pub(super) struct Queue {
    inner: Arc<InnerQueue>,
}

impl Queue {
    /// Create a new queue for this peer.
    pub(super) fn new(label: &str, queue_dir: &str, maxqueue: u32) -> Queue {
        let mut path = PathBuf::from(queue_dir);
        path.push(&Path::new(label));

        let qreader = QReader {
            label: label.to_string(),
            dir: queue_dir.to_string(),
            maxqueue,
            ..QReader::default()
        };
        let qwriter = QWriter {
            label: label.to_string(),
            dir: queue_dir.to_string(),
            ..QWriter::default()
        };

        let inner = InnerQueue {
            qwriter: Mutex::new(qwriter),
            qreader: Mutex::new(qreader),
        };
        Queue {
            inner: Arc::new(inner),
        }
    }

    pub(super) async fn len(&self) -> usize {
        let qreader = self.inner.qreader.lock().await;
        let qwriter = self.inner.qwriter.lock().await;
        qreader.qfiles.len() + (!qwriter.is_empty as usize)
    }

    pub(super) async fn init(&self) -> usize {
        // scan qeueu files.
        let mut qreader = self.inner.qreader.lock().await;
        let _ = qreader.scan_qfiles().await;
        qreader.expire_qfiles().await;

        // find lowest and highest seqno
        // note that the lower limit for high_seq is indeed 2.
        // that's so that high_seq - 1 > 0.
        let mut high_seq = 1;
        let mut low_seq = 0;
        for qfile in &qreader.qfiles {
            if qfile.seq > high_seq {
                high_seq = qfile.seq;
            }
            if low_seq == 0 || qfile.seq < low_seq {
                low_seq = qfile.seq;
            }
        }
        high_seq += 1;

        // combine with data from .label.seq
        let mut qwriter = self.inner.qwriter.lock().await;
        qwriter.read_seqno(high_seq).await;

        // rotate queue file if needed.
        let now = util::unixtime();
        if qwriter.last_rotate < now - QUEUE_ROTATE_SECS {
            if let Ok(Some(qfile)) = qwriter.rotate(low_seq, false).await {
                qreader.qfiles.push(qfile);
            }
        }

        // Open the main backlog queue file if it exists. Sets qwriter.is_empty
        // if the file does not exist or is empty.
        let _ = qwriter.open_qfile(false).await;
        qwriter.file.take();

        // Return the number of queue files.
        qreader.qfiles.len() + (!qwriter.is_empty as usize)
    }

    // get a block of items from the queue, LIFO mode.
    pub(super) async fn read_items(&self, num_entries: usize) -> Option<QItems> {
        let mut qreader = self.inner.qreader.lock().await;

        // If we still have a QItems queued up, return it.
        if let Some(items) = qreader.buffered.pop_front() {
            return Some(items);
        }

        // Check if the file is open and has content, or if not, see
        // if we can open the next queue file.
        if !qreader.open_qfile().await {
            // Now if we have no S.* queue files, but the currently being
            // written queue file has content, rotate it.
            let mut try_again = false;
            if qreader.qfiles.len() == 0 {
                let mut qwriter = self.inner.qwriter.lock().await;
                if !qwriter.is_empty {
                    if let Ok(Some(qfile)) = qwriter.rotate(0, true).await {
                        qreader.qfiles.push(qfile);
                        try_again = true;
                    }
                }
            }

            if !try_again || !qreader.open_qfile().await {
                // alas.
                return None;
            }
        }

        // take ownership of the file in the Option<File> thingy
        // to work around lifetime issues.
        let mut cur_file = qreader.cur_file.take().unwrap();

        // Now read up to `num_entries`.
        let mut items = String::new();
        for _ in 0..num_entries {
            match cur_file.read_line(&mut items).await {
                Ok(0) => {
                    if items.len() == 0 {
                        // We read nothing and hit EOF. Should not happen.
                        // Recovery strategy: close current file, return None.
                        log::warn!(
                            "QReader::read_items: {}: unexpectedly hit EOF. Bug.",
                            qreader
                                .cur_name
                                .as_ref()
                                .map(|x| x.as_str())
                                .unwrap_or("[unknown]"),
                        );
                        qreader.cur_eof = true;
                        qreader.cur_file.replace(cur_file);
                        return None;
                    }
                    break;
                },
                Ok(n) => qreader.cur_offset += n as u64,
                Err(e) => {
                    // at this point, with the file opened, if we hit
                    // an error, just log it and handle it as EOF.
                    if items.len() == 0 {
                        log::error!(
                            "Queue::read_items: read from {:?}: {}",
                            qreader.cur_name.as_ref().unwrap(),
                            e
                        );
                        qreader.cur_file.replace(cur_file);
                        qreader.cur_eof = true;
                        return None;
                    }
                    break;
                },
            }
        }

        // Get file back into the Option.
        qreader.cur_file.replace(cur_file);

        // Dit we hit EOF? If so, signal it, so that at the _next_
        // call we can try to switch to the next queue file.
        if qreader.cur_offset >= qreader.cur_size {
            qreader.cur_eof = true;
        }

        // Bookkeeping.
        qreader.last_id += 1;
        let id = qreader.last_id;
        qreader.outstanding.insert(id);

        //log::debug!("read_items: return {:?}", items);
        Some(QItems {
            items,
            id,
            pos: 0,
            done: false,
        })
    }

    /// Acknowledge that the items were processed and that we're done.
    pub(super) async fn ack_items(&self, items: QItems) {
        self.ack(items, true).await
    }

    /// Return the items, we did not process them (or not all of them).
    /// They will be re-queued for the next caller of read_items().
    pub(super) async fn return_items(&self, items: QItems) {
        self.ack(items, false).await
    }

    // A block of backlog messages was processed. Handle it.
    async fn ack(&self, mut items: QItems, done: bool) {
        let mut qreader = self.inner.qreader.lock().await;

        //log::debug!("ack id {} done {}", id, done);

        // See if we still care about this id.
        items.pos = 0;
        items.done = true;
        if qreader.outstanding.remove(&items.id) {
            if !done {
                items.done = false;
                qreader.buffered.push_back(items);
                return;
            }
        }

        // If this was the last one, close the current queue file.
        if qreader.cur_eof && qreader.buffered.len() == 0 && qreader.outstanding.len() == 0 {
            qreader.close_qfile().await;
        }
    }

    pub(super) async fn write_arts(&self, spool: &Spool, arts: &[PeerArticle]) -> io::Result<()> {
        if arts.len() == 0 {
            return Ok(());
        }
        let iter = arts.iter().filter_map(|art| {
            let res = spool.token_to_text(&art.location, &art.msgid);
            if res.is_none() {
                log::warn!(
                    "Queue::write_arts: token_to_text({}) failed",
                    art.location.to_json(spool)
                );
            }
            res
        });
        self.write_items(iter).await
    }

    pub(super) async fn write_items<I, S>(&self, items: I) -> io::Result<()>
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        let mut qwriter = self.inner.qwriter.lock().await;

        // See if it's time to rotate the queue file.
        let now = util::unixtime();
        if qwriter.last_rotate + QUEUE_ROTATE_SECS < now {
            let mut qreader = self.inner.qreader.lock().await;
            if let Ok(Some(qfile)) = qwriter.rotate(qreader.low_seq(), false).await {
                qreader.qfiles.push(qfile);
                qreader.expire_qfiles().await;
            }
        }

        let mut data = String::new();
        for item in items {
            data.push_str(item.as_ref());
            data.push('\n');
        }

        if qwriter.file.is_none() {
            // open the queue file, return error on fail.
            qwriter.open_qfile(true).await?;
        }
        let file = qwriter.file.as_mut().unwrap();
        file.write_all(data.as_bytes()).await?;
        qwriter.is_empty = false;

        Ok(())
    }
}

#[derive(Debug)]
struct QFile {
    time:  u64,
    seq:   u64,
    name:  String,
    erred: bool,
}

impl QFile {
    async fn from_dirent(label: &str, d: &fs::DirEntry) -> Option<QFile> {
        static PARSE_QFNAME: Lazy<Regex> = Lazy::new(|| {
            let re = r"^(_\d+)?(?:.S(\d+)|)$";
            Regex::new(re).expect("could not compile PARSE_QFNAME regexp")
        });

        // name must start with 'label'.
        let name = d.file_name();
        let name = name.to_str().unwrap();
        if !name.starts_with(label) {
            return None;
        }
        let suffix = &name[label.len()..];

        // parse the suffix after the label.
        let caps = PARSE_QFNAME.captures(suffix)?;
        let seq = if caps.get(1).is_some() {
            // It's a label_01 or label_01.S00001 style file. Ignore sequence.
            0
        } else {
            // Save sequence number.
            caps.get(2).and_then(|s| s.as_str().parse::<u64>().ok())?
        };

        // get last modified time.
        let meta = d.metadata().await.ok()?;
        let time = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_secs();
        let name = name.to_string();

        Some(QFile {
            time,
            seq,
            name,
            erred: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn write_file(name: &str, content: &str) {
        let name = String::from("/tmp/") + name;
        let mut file = fs::File::create(&name).expect("failed to create file");
        file.write_all(content.as_bytes())
            .expect("failed to write content");
    }

    //fn read_file(name: &str) -> String {
    //    let name = String::from("/tmp/") + name;
    //    String::from_utf8(fs::read(&name).expect("failed to read file")).unwrap()
    //}

    #[tokio::test]
    async fn test1() {
        write_file("label.S00001", "Hello\nWorld\n");
        write_file("label.S00002", "Hallo\nWereld\n");

        let q = Queue::new("label", "/tmp", 5);
        q.init().await;

        // 1. read "hello", but do not ack
        let mut items1 = q.read_items(1).await.unwrap();
        assert!(items1.next_str() == Some("Hello"));
        assert!(items1.next_str() == None);

        // 2. read "world".
        let mut items2 = q.read_items(10).await.unwrap();
        assert!(items2.next_str() == Some("World"));
        assert!(items2.next_str() == None);
        q.ack_items(items2).await;

        // 3. since the first was not acked, can't read further.
        let items3 = q.read_items(10).await;
        assert!(items3.is_none());

        // 4. now ack.
        q.ack_items(items1).await;
        let mut items = q.read_items(10).await.unwrap();
        assert!(items.next_str() == Some("Hallo"));
        assert!(items.next_str() == Some("Wereld"));
        assert!(items.next_str() == None);

        // Drop the ack, should repeat.
        q.return_items(items).await;

        let mut items = q.read_items(10).await.unwrap();
        assert!(items.next_str() == Some("Hallo"));
        assert!(items.next_str() == Some("Wereld"));
        assert!(items.next_str() == None);
        q.ack_items(items).await;
    }
}

async fn open_append(path: impl AsRef<Path>, create: bool) -> io::Result<fs::File> {
    fs::OpenOptions::new()
        .create(create)
        .append(true)
        .open(&path.as_ref())
        .await
        .map_err(|e| {
            ioerr!(e.kind(), "{:?}: {}", path.as_ref(), e);
            e
        })
}
