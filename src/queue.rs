//! Outgoing queue management.
//!
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::fs::OpenOptions as StdOpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use once_cell::sync::Lazy;
use parking_lot::Mutex as StdMutex;
use regex::Regex;
use tokio::fs;
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;
use tokio::task;

use crate::nntp_send::PeerArticle;
use crate::spool::Spool;
use crate::util;

#[derive(Default, Clone, Debug)]
pub struct QItems {
    pub(crate) id:     u64,
    pub(crate) items:  String,
    pub(crate) done:   bool,
}

impl QItems {
    /// An iterator that returns `PeerArticle` items.
    pub fn iter_arts<'a, 'b>(&'a self, spool: &'b Spool) -> QItemsArtIter<'a, 'b> {
        QItemsArtIter {
            iter: self.items.split('\n'),
            spool: spool,
        }
    }

    /// An iterator that returns `&str` items.
    pub fn iter_items<'a>(&'a self) -> QItemsStrIter<'a> {
        QItemsStrIter{ iter: self.items.split('\n') }
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

/// Iterator `Item = PeerArticle`
pub struct QItemsArtIter<'a, 'b> {
    iter:   std::str::Split<'a, char>,
    spool:  &'b Spool,
}

impl<'a, 'b> Iterator for QItemsArtIter<'a, 'b> {
    type Item = PeerArticle;

    fn next(&mut self) -> Option<PeerArticle> {
        while let Some(s) = self.iter.next() {
            if s == "" {
                continue;
            }
            if let Some((location, msgid)) = self.spool.text_to_token(s) {
                return Some(PeerArticle { msgid, location, size: 0 });
            }
        }
        None
    }
}

/// Iterator `Item = &str`
pub struct QItemsStrIter<'a> {
    iter:   std::str::Split<'a, char>,
}

impl<'a> Iterator for QItemsStrIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        while let Some(s) = self.iter.next() {
            log::debug!("NEXT: [{}]", s);
            if s == "" {
                continue;
            }
            return Some(s);
        }
        None
    }
}

#[derive(Default)]
struct QWriter {
    file: Option<StdFile>,
}

#[derive(Default)]
struct QReader {
    last_id:    u64,
    qfiles: Vec<QFile>,
    dir:        String,
    maxqueue:  u32,
    cur_file: Option<tokio::io::BufReader<fs::File>>,
    cur_name: Option<String>,
    cur_eof:    bool,
    cur_offset: u64,
    cur_size:   u64,
    // id, offset
    outstanding: HashMap<u64, u64>,
}

impl QReader {
    fn reset_cur(&mut self) {
        self.cur_file.take();
        self.cur_name.take();
        self.cur_eof = false;
        self.cur_offset = 0;
        self.cur_size = 0;
        self.outstanding.clear();
    }

    // Close the current qfile and remove it.
    async fn close_qfile(&mut self) {

        log::debug!("close_qfile: self.cur_eof {}, outstanding: {}", self.cur_eof, self.outstanding.len());

        // This _should_ always succeed, since cur_eof is set.
        if let Some(name) = self.cur_name.take() {

            // Remove file from filesystem.
            let mut path = PathBuf::from(&self.dir);
            path.push(&name);
            log::trace!("removing {:?}", path);
            if let Err(e) = fs::remove_file(&path).await {
                log::error!("QReader::check_cur_eof: remove({:?}): {}", path, e);
            }

            // And remove file from in-memory list.
            for i in 0 .. self.qfiles.len() {
                if self.qfiles[i].name == name {
                    log::trace!("close_qfile: remove from in-mem list: {}", name);
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
            log::debug!("open_qfile: is some, dude");
            if !self.cur_eof {
                log::debug!("... but not EOF");
                return true;
            }
            if self.outstanding.len() > 0 {
                log::debug!("... but outstanding");
                return false;
            }
            log::debug!("... closink!");
            self.close_qfile().await;
        }

        // Before opening a new file, expire the old queue files.
        self.expire_qfiles().await;

        log::debug!("XXX qfiles: {:?}", self.qfiles);

        // Loop over all files in the queue.
        for i in 0 .. self.qfiles.len() {

            let qfile = &self.qfiles[i];
            if qfile.erred {
                continue;
            }

            let mut path = PathBuf::from(&self.dir);
            path.push(&qfile.name);

            // open.
            log::debug!("Opening queue file: {:?}", path);

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
    async fn scan_qfiles(&mut self, label: &str) -> io::Result<()> {
        let mut rd = fs::read_dir(&self.dir).await.map_err(|e| {
            ioerr!(e.kind(), "QReader::scan_qfiles: {}: {}", self.dir, e);
            e
        })?;

        let mut qfiles = Vec::new();
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Some(qfile) = QFile::from_dirent(label, &entry).await {
                qfiles.push(qfile);
            }
        }
        qfiles.sort_unstable_by(|a, b| {
            let ord = a.time.partial_cmp(&b.time).unwrap();
            if ord == std::cmp::Ordering::Equal {
                a.name.partial_cmp(&b.name).unwrap()
            } else {
                ord
            }
        });
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
            let min_age = now - 300 * (self.maxqueue as u64);

            for i in 0 .. self.qfiles.len() {
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
                    log::debug!("QReader::expire_qfiles: removing {:?}: too old", &qfile.name);
                    let _ = std::fs::remove_file(&path);
                }
            }).await;
        }
    }
}

struct InnerQueue {
    label:   String,
    path:    String,
    qreader: Mutex<QReader>,
    qwriter: StdMutex<QWriter>,
}

pub struct Queue {
    inner: Arc<InnerQueue>,
}

impl Queue {
    /// Create a new queue for this peer.
    pub fn new(label: &str, queue_dir: &str, maxqueue: u32) -> Queue {

        let mut path = PathBuf::from(queue_dir);
        path.push(&Path::new(label));

        let qreader = QReader {
            dir: queue_dir.to_string(),
            maxqueue,
            ..QReader::default()
        };

        let inner = InnerQueue {
            label: label.to_string(),
            path:  path.to_str().unwrap().to_string(),
            qwriter: StdMutex::new(QWriter::default()),
            qreader: Mutex::new(qreader),
        };
        Queue { inner: Arc::new(inner) }
    }

    pub async fn init(&self) {
        // XXX TODO if there is a current queue file rotate it
        let mut qreader = self.inner.qreader.lock().await;
        let _ = qreader.scan_qfiles(&self.inner.label).await;
        qreader.expire_qfiles().await;
    }

    // get a block of items from the queue, LIFO mode.
    pub async fn read_items(&self, num_entries: usize) -> Option<QItems> {

        let mut qreader = self.inner.qreader.lock().await;
        let mut items = String::new();
        let mut offset = 0;

        // allow one retry.
        for _ in 0u32..=1 {

            // Check if the file is open and has content, or if not, see
            // if we can open the next queue file.
            if !qreader.open_qfile().await {
                // alas.
                return None;
            }

            offset = qreader.cur_offset;
            let mut do_retry = false;

            // take ownership of the file in the Option<File> thingy
            // to work around lifetime issues.
            let mut cur_file = qreader.cur_file.take().unwrap();

            // Now read up to `num_entries`.
            for _ in 0..num_entries {
                match cur_file.read_line(&mut items).await {
                    Ok(0) => {
                        if items.len() == 0 {
                            // We read nothing and hit EOF, so try the next file.
                            qreader.cur_eof = true;
                            do_retry = true;
                            log::debug!("Hit EOF, retry");
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
                            qreader.cur_eof = true;
                            do_retry = true;
                        }
                        break;
                    }
                }
            }

            // Get file back into the Option.
            qreader.cur_file.replace(cur_file);

            if !do_retry {
                break;
            }
        }

        // If we still did not manage to read anything, return None.
        if items.len() == 0 {
            qreader.close_qfile().await;
            return None;
        }

        // Bookkeeping.
        qreader.last_id += 1;
        let id = qreader.last_id;
        qreader.outstanding.insert(id, offset);

        log::debug!("read_items: return {:?}", items);

        Some(QItems { items, id, done: false })
    }


    /// Acknowledge that the items were processed and that we're done.
    pub async fn ack_items(&self, mut items: QItems) {
        items.done = true;
        self.ack(items.id, true).await
    }

    /// Return the items, we did not process them (or not all of them).
    pub async fn return_items(&self, mut items: QItems) {
        items.done = true;
        self.ack(items.id, false).await
    }

    // A block of backlog messages was processed. Handle it.
    async fn ack(&self, id: u64, done: bool) {
        let mut qreader = self.inner.qreader.lock().await;

        log::debug!("ack id {} done {}", id, done);

        // See if we still care about this id.
        if let Some(offset) = qreader.outstanding.remove(&id) {

            log::debug!("{} removed from outstanding [{}] outstanding.len = {}", id, offset, qreader.outstanding.len());
            // Yes, current file. See if we need to rewind.
            if !done && offset < qreader.cur_offset {

                // This block was not processed. Easiest way to handle this
                // is to rewind the file to the start of the block. Some other
                // data might get re-queued but that's OK.
                //
                // tokio::io::BufRead has no seek(), so we need to do this.
                if let Some(curfile) = qreader.cur_file.take() {
                    let mut f = curfile.into_inner();
                    if let Ok(_) = f.seek(std::io::SeekFrom::Start(offset)).await {
                        qreader.cur_eof = false;
                        qreader.cur_offset = offset;
                    }
                    qreader.cur_file.replace(tokio::io::BufReader::new(f));
                }
            }
        }

        // If this was the last one, close the current queue file.
        if qreader.cur_eof && qreader.outstanding.len() == 0 {
            qreader.close_qfile().await;
        }
    }

    pub async fn write_arts(&self, spool: &Spool, arts: &[PeerArticle]) -> io::Result<()> {
        if arts.len() == 0 {
            return Ok(());
        }
        let iter = arts
        .iter()
        .filter_map(|art| {
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

    // XXX IMPORTANT TODO when do we rotate this backlog?
    // Some ideas:
    // - if the oldest S.backlog file is more than 5 mins old
    // - or if there are no backlog files, and the current file is more than 1 minute old.
    //
    pub async fn write_items<I, S>(&self, items: I) -> io::Result<()>
    where
        I: Iterator<Item=S>,
        S: AsRef<str>,
    {
        let mut data = String::new();
        for item in items {
            data.push_str(item.as_ref());
            data.push('\n');
        }

        // XXX TODO use a blocking pool ?
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let mut qwriter = inner.qwriter.lock();
            // TODO: every so often re-open. maybe check if nlink == 0.
            if qwriter.file.is_none() {
                let writer = StdOpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&inner.path)
                    .map_err(|e| {
                        ioerr!(e.kind(), "{}: {}", inner.path, e);
                        e
                    })?;
                qwriter.file = Some(writer);
            }
            qwriter.file.as_mut().unwrap().write_all(data.as_bytes())
        })
        .await
        .unwrap_or_else(|e| Err(ioerr!(Other, "spawn_blocking: {}", e)))
    }
}

#[derive(Debug)]
struct QFile {
    time:   u64,
    seq:    u64,
    name:   String,
    erred:  bool,
}

impl QFile {
    async fn from_dirent(label: &str, d: &fs::DirEntry) -> Option<QFile> {
        static PARSE_QFNAME: Lazy<Regex> = Lazy::new(|| {
            let re = r"^(_\d+|)(?:.S(\d+)|)$";
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
            // we don't really need the sequence number, but wel'll use it
            // to write the .<label>.seq file for diablo compatibility.
            caps.get(2).and_then(|s| s.as_str().parse::<u64>().ok())?
        };

        // get last modified time.
        let meta = d.metadata().await.ok()?;
        let time = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_secs();
        let name = name.to_string();

        Some(QFile { time, seq, name, erred: false })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io;

    fn write_file(name: &str, content: &str) {
        let name = String::from("/tmp/") + name;
        let mut file = fs::File::create(&name).expect("failed to create file");
        file.write_all(content.as_bytes()).expect("failed to write content");
    }

    fn read_file(name: &str) -> String {
        let name = String::from("/tmp/") + name;
        String::from_utf8(fs::read(&name).expect("failed to read file")).unwrap()
    }

    #[tokio::test]
    async fn test1() {

        write_file("label.S00001", "Hello\nWorld\n");
        write_file("label.S00002", "Hallo\nWereld\n");

        let q = Queue::new("label", "/tmp", 5);
        q.init().await;

        // 1. read "hello", but do not ack
        let items1 = q.read_items(1).await.unwrap();
        log::debug!("XXX items is {:?}", items1);
        let mut iter1 = items1.iter_items();
        assert!(iter1.next() == Some("Hello"));
        assert!(iter1.next() == None);

        // 2. read "world".
        let items2 = q.read_items(10).await.unwrap();
        let mut iter2 = items2.iter_items();
        assert!(iter2.next() == Some("World"));
        assert!(iter2.next() == None);
        q.ack_items(items2).await;

        // 3. since the first was not acked, can't read further.
        let items3 = q.read_items(10).await;
        assert!(items3.is_none());

        // 4. now ack.
        q.ack_items(items1).await;
        let items = q.read_items(10).await.unwrap();
        let mut iter = items.iter_items();
        assert!(iter.next() == Some("Hallo"));
        assert!(iter.next() == Some("Wereld"));
        assert!(iter.next() == None);

        // Drop the ack, should repeat.
        q.return_items(items).await;

        let items = q.read_items(10).await.unwrap();
        let mut iter = items.iter_items();
        assert!(iter.next() == Some("Hallo"));
        assert!(iter.next() == Some("Wereld"));
        assert!(iter.next() == None);
        q.ack_items(items).await;
    }
}

