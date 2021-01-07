//! Read / write the active file in diablo's dactive.kp format.
//!
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::io::{self, ErrorKind};
use std::sync::Mutex;

use super::kpdb::{KpDb, Record};
use crate::util::wildmat;

// contains the exclusively locked active file.
pub struct ActiveFile(Mutex<ActiveInner>);

impl ActiveFile {
    pub fn open(path: impl AsRef<str>) -> io::Result<ActiveFile> {
        let kpdb = KpDb::open(path, false)?;
        let inner = ActiveInner { kpdb };
        Ok(ActiveFile(Mutex::new(inner)))
    }

    /// Return the low / high / xref counters for a group.
    pub fn group_counters(&self, name: &str) -> Option<GroupCounters> {
        let inner = self.0.lock().unwrap();
        let entry = inner.kpdb.get(name)?;
        Some(GroupCounters::read(&entry))
    }

    /// Output for the 'LIST active' command.
    pub fn list_active(&self, groups: Option<impl AsRef<str>>) -> Option<Vec<u8>> {
        self.list_data(groups.as_ref().map(|s| s.as_ref()), true)
    }

    /// Output for the 'LIST newsgroups' command.
    pub fn list_newsgroups(&self, groups: Option<impl AsRef<str>>) -> Option<Vec<u8>> {
        self.list_data(groups.as_ref().map(|s| s.as_ref()), false)
    }

    fn list_data(&self, groups: Option<&str>, active: bool) -> Option<Vec<u8>> {
        let inner = self.0.lock().unwrap();
        let mut buf = Vec::new();

        let groups = groups.unwrap_or("*");
        let is_wildpat = groups.chars().any(|c| "*?\\[".contains(c));

        if is_wildpat {
            for (group, entry) in inner.kpdb.iter() {
                if crate::util::wildmat(&group, groups) {
                    inner.list_data(&entry, &mut buf, active);
                }
            }
        } else {
            let entry = inner.kpdb.get(groups)?;
            inner.list_data(&entry, &mut buf, active);
        }
        if !buf.is_empty() {
            Some(buf)
        } else {
            None
        }
    }

    pub fn sync_active(
        &self,
        active: &[u8],
        groups: Option<impl AsRef<str>>,
        remove: bool,
    ) -> io::Result<()> {
        let mut inner = self.0.lock().unwrap();
        inner.sync_active(active, groups.as_ref().map(|s| s.as_ref()), remove)
    }

    pub fn sync_newsgroups(&self, newsgroups: &[u8], groups: Option<impl AsRef<str>>) -> io::Result<()> {
        let mut inner = self.0.lock().unwrap();
        inner.sync_newsgroups(newsgroups, groups.as_ref().map(|s| s.as_ref()))
    }

    /// Increment the NX field by one, and return the new value.
    ///
    /// Note that we don't check the "S" (status) field. Even if the group has
    /// the "Removed" status, we still increment and return the "NX" field.
    /// This is so that accidental removals don't ruin the article numbering.
    pub fn inc_xref(&mut self, group: &str) -> io::Result<u64> {
        let mut inner = self.0.lock().unwrap();
        let mut rec = inner
            .kpdb
            .get_mut(group)
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "NotFound"))?;

        let mut nx = rec
            .get_u64("NX")
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "NotFound"))?;

        nx += 1;
        rec.set_str("NX", &format!("{:10}", nx));
        drop(rec);

        inner.kpdb.flush()?;

        Ok(nx)
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut inner = self.0.lock().unwrap();
        inner.kpdb.flush()
    }
}

impl Drop for ActiveFile {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

struct ActiveInner {
    kpdb: KpDb,
}

impl ActiveInner {
    fn list_data(&self, entry: &Record, buf: &mut Vec<u8>, active: bool) {
        if active {
            self.list_active(entry, buf)
        } else {
            self.list_newsgroups(entry, buf)
        }
    }

    fn list_active(&self, entry: &Record, buf: &mut Vec<u8>) {
        // It's twice as fast to iterate once over all fields,
        // than to call entry.get_str() three times.
        let mut fields = entry.get_raw().split(|b| b.is_ascii_whitespace());

        let name = match fields.next() {
            Some(name) => name,
            None => return,
        };

        let mut high = &b"0000000000"[..];
        let mut low = &b"0000000000"[..];
        let mut status = &b"x"[..];

        for field in fields {
            if field.starts_with(&b"NE="[..]) {
                high = &field[3..];
            }
            if field.starts_with(&b"NB="[..]) {
                low = &field[3..];
            }
            if field.starts_with(&b"S="[..]) {
                status = &field[2..];
            }
        }

        // don't list removed groups.
        if status == &b"r"[..] {
            return;
        }

        // because we have &[u8] fields instead of &str we cannot use write!(...).
        if name.starts_with(&b"."[..]) {
            buf.push(b'.');
        }
        buf.extend_from_slice(name);
        buf.push(b' ');
        buf.extend_from_slice(high);
        buf.push(b' ');
        buf.extend_from_slice(low);
        buf.push(b' ');
        buf.extend_from_slice(status);
        buf.extend_from_slice(&b"\r\n"[..]);
    }

    fn list_newsgroups(&self, entry: &Record, buf: &mut Vec<u8>) {
        let name = match entry.get_name() {
            Some(name) => name,
            None => return,
        };

        // don't list removed groups.
        match entry.get_str("S") {
            Some("r") => return,
            Some(_) => {},
            None => return,
        }

        if let Some(desc) = entry.get_decoded("GD") {
            if !desc.is_empty() && desc != &b"?"[..] {
                if name.starts_with(".") {
                    buf.push(b'.');
                }
                buf.extend_from_slice(name.as_bytes());
                buf.push(b' ');
                buf.extend_from_slice(&desc);
                buf.extend_from_slice(&b"\r\n"[..]);
            }
        }
    }

    fn sync_active(&mut self, active: &[u8], groups: Option<&str>, remove: bool) -> io::Result<()> {
        let groups = groups.unwrap_or("*");

        // Store a set of all the groups we have now.
        let mut remaining = HashSet::new();
        for group in self.kpdb.keys() {
            if wildmat(&group, groups) {
                remaining.insert(group.to_string());
            }
        }

        for line in active.split(|&b| b == b'\n') {
            // If it's not UTF-8 just ignore it - what can we do?
            if let Ok(line) = std::str::from_utf8(line) {
                // Check that it has at least four fields.
                let fields: Vec<_> = line.split_ascii_whitespace().collect();
                if fields.len() < 4 {
                    continue;
                }

                // Must match the group selector.
                if !wildmat(fields[0], groups) {
                    continue;
                }

                // Remove from the list (i.o.w. mark as done).
                remaining.remove(fields[0]);

                // Look the group up in the current dactive.kp
                if let Some(mut rec) = self.kpdb.get_mut(fields[0]) {
                    // Exists. Check if the status flag changed.
                    let status = rec.get_str("S").unwrap_or("");
                    if status != fields[3] {
                        // Changed.
                        rec.set_str("S", fields[3]);
                    }
                    continue;
                }

                // We did not have this group yet, add it.
                let mut hm = HashMap::new();
                hm.insert("NB", "00000001".to_string());
                hm.insert("NE", "00000000".to_string());
                hm.insert("NX", "00000000".to_string());
                hm.insert("S", fields[3].to_string());
                let _ = self.kpdb.insert(fields[0], &hm);
            }
        }

        // Delete the groups that were not present.
        if remove {
            for group in remaining.drain() {
                if let Some(mut rec) = self.kpdb.get_mut(&group) {
                    rec.set_str("S", "r");
                }
            }
        }

        Ok(())
    }

    fn sync_newsgroups(&mut self, newsgroups: &[u8], groups: Option<&str>) -> io::Result<()> {
        let groups = groups.unwrap_or("*");

        for line in newsgroups.split(|&b| b == b'\n') {
            // Split. Must have 2 fields, and first one must be utf-8.
            let mut iter = line.splitn(2, |&c| c.is_ascii_whitespace());
            let name = match iter.next().map(|n| std::str::from_utf8(n).ok()).flatten() {
                Some(name) => name,
                None => continue,
            };
            let desc = match iter.next() {
                Some(desc) => trim_bytes(desc),
                None => continue,
            };

            // Ignore groups that just have '?' as description.
            if desc == &b"?"[..] {
                continue;
            }

            // Must match the group selector.
            if !wildmat(name, groups) {
                continue;
            }

            // Look the group up in the current dactive.kp
            // If we don't have it, ignore it.
            let mut rec = match self.kpdb.get_mut(name) {
                Some(rec) => rec,
                None => continue,
            };

            // Exists. Update it, if it hasn't changed it's a no-op.
            let enc_desc = super::kpdb::percent_encode(desc);
            rec.set_str("GD", &enc_desc);
        }

        Ok(())
    }
}

// active-file flag for a group.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStatus {
    PostingOk = b'y',
    NoPosting = b'n',
    Moderated = b'm',
    Junk      = b'j',
    Closed    = b'x',
    Removed   = b'r',
}

impl fmt::Display for GroupStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8 as char)
    }
}

impl std::default::Default for GroupStatus {
    fn default() -> Self {
        GroupStatus::Closed
    }
}

impl std::str::FromStr for GroupStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "y" => Ok(GroupStatus::PostingOk),
            "n" => Ok(GroupStatus::NoPosting),
            "m" => Ok(GroupStatus::Moderated),
            "j" => Ok(GroupStatus::Junk),
            "x" => Ok(GroupStatus::Closed),
            "r" => Ok(GroupStatus::Removed),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct GroupCounters {
    pub low:  u64,
    pub high: u64,
    pub xref: u64,
}

impl GroupCounters {
    fn read(entry: &Record) -> GroupCounters {
        let low = entry.get_u64("NB").unwrap_or(1);
        let high = entry.get_u64("NE").unwrap_or(0);
        let xref = entry.get_u64("NX").unwrap_or(1);
        GroupCounters { low, high, xref }
    }
}

// helper.
fn trim_bytes(mut b: &[u8]) -> &[u8] {
    while b.len() > 0 && b[0].is_ascii_whitespace() {
        b = &b[1..];
    }
    while b.len() > 0 && b[b.len() - 1].is_ascii_whitespace() {
        b = &b[..b.len() - 1];
    }
    b
}
