//! Buffer implementation like Bytes / BytesMut.
//!
//! It is way simpler and contains way, way less unsafe code.
//
// The unsafe code is needed for efficiency reasons. You do not
// want to zero-initialize buffers every time you use them.
//
// I have experimented with a non-contiguous buffer approach, with
// a pool of continously re-used blocks that are pre-initialized.
// That does away with a lot of unsafe code, since the blocks only have to
// be initialized once. However it turned out that too much code
// in the server still assumes it can Deref the Buffer as a flat &[u8].
//
// The old code is at
// https://github.com/miquels/nntp-rs/blob/8a70816767e62c62d2462671f76a8e0efa4552eb/src/util/buffer.rs
//
use std::default::Default;
use std::fmt;
use std::io::{self, Read, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice;

use bytes::{Buf, BufMut};

/// A buffer structure, like Bytes/BytesMut.
///
/// It is not much more than a wrapper around Vec.
pub struct Buffer {
    start_offset: usize,
    rd_pos:       usize,
    data:         Vec<u8>,
}

impl Buffer {
    /// Create new Buffer.
    pub fn new() -> Buffer {
        Buffer {
            start_offset: 0,
            rd_pos:       0,
            data:         Vec::new(),
        }
    }

    /// Clear this buffer.
    pub fn clear(&mut self) {
        self.start_offset = 0;
        self.rd_pos = 0;
        self.data.truncate(0);
    }

    /// Truncate this buffer.
    pub fn truncate(&mut self, size: usize) {
        if size == 0 {
            self.clear();
            return;
        }
        if size > self.len() {
            panic!("Buffer::truncate(size): size > self.len()");
        }
        if self.rd_pos > size {
            self.rd_pos = size;
        }
        self.data.truncate(size + self.start_offset);
    }

    /// Split this Buffer in two parts.
    ///
    /// The first part remains in this buffer. The second part is
    /// returned as a new Buffer.
    pub fn split_off(&mut self, at: usize) -> Buffer {
        if at > self.len() {
            panic!("Buffer:split_off(size): size > self.len()");
        }
        if self.rd_pos > at {
            self.rd_pos = at;
        }
        let mut bnew = Buffer::new();

        // If "header" < 32K and "body" >= 32K, use a start_offset
        // for "body" and copy "header".
        if self.start_offset == 0 && at < 32000 && self.len() - at >= 32000 {
            mem::swap(&mut self.data, &mut bnew.data);
            bnew.start_offset = at;
            self.extend_from_slice(&bnew[0..at]);
            return bnew;
        }

        bnew.data = self.data.split_off(at + self.start_offset);

        bnew
    }

    /// Split this Buffer in two parts.
    ///
    /// The second part remains in this buffer. The first part is
    /// returned to the caller.
    pub fn split_to(&mut self, size: usize) -> Buffer {
        // move self.data to a new Buffer.
        let mut nbuf = Buffer::new();
        let start_offset = self.start_offset;
        mem::swap(&mut self.data, &mut nbuf.data);

        // now copy the end of the data back to self.data.
        self.extend_from_slice(&nbuf.data[self.start_offset + size..]);
        self.start_offset = 0;

        // and truncate the new Buffer to the right length.
        nbuf.start_offset = start_offset;
        nbuf.data.truncate(start_offset + size);

        nbuf
    }

    /// total length of all data in this Buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() - self.start_offset
    }

    /// Write all data in this `Buffer` to a file.
    pub fn write_all(&mut self, mut file: impl Write) -> io::Result<()> {
        file.write_all(self.bytes())?;
        self.rd_pos = self.len();
        Ok(())
    }

    /// Read an exact number of bytes.
    ///
    /// NOTE: this function lets `reader` read into potentially
    /// uninitialized memory. So the reader that is passed in to this
    /// function must:
    ///
    /// - never read from the buffer past to it
    /// - return the correct number of bytes read (and thus, initialized)
    ///
    pub fn read_exact(&mut self, mut reader: impl Read, len: usize) -> io::Result<()> {
        self.data.reserve(len);
        let prev_len = self.data.len();
        unsafe { self.data.set_len(prev_len + len) };
        match reader.read_exact(&mut self.data[prev_len..]) {
            Ok(_) => Ok(()),
            Err(e) => {
                unsafe { self.data.set_len(prev_len) };
                Err(e)
            },
        }
    }

    /// Read until end-of-file.
    ///
    /// See the remarks on `read_exact` for unsafety.
    ///
    pub fn read_all(&mut self, mut reader: impl Read) -> io::Result<()> {
        let mut end_data = self.data.len();
        loop {
            self.data.reserve(4096);
            unsafe { self.data.set_len(self.data.capacity()) };
            match reader.read(&mut self.data[end_data..]) {
                Ok(n) => {
                    if n == 0 {
                        unsafe { self.data.set_len(end_data) };
                        break;
                    }
                    end_data += n;
                },
                Err(e) => {
                    unsafe { self.data.set_len(end_data) };
                    return Err(e);
                },
            }
        }
        Ok(())
    }

    /// Add data to this buffer.
    #[inline]
    pub fn extend_from_slice(&mut self, extend: &[u8]) {
        self.data.extend_from_slice(extend);
    }

    /// Make sure at least `size` bytes are available.
    #[inline]
    pub fn reserve(&mut self, size: usize) {
        self.data.reserve(size);
    }

    /// Add a string to the buffer.
    #[inline]
    pub fn put_str(&mut self, s: impl AsRef<str>) {
        self.extend_from_slice(s.as_ref().as_bytes());
    }

    /// Return a reference to this Buffer as an UTF-8 string.
    #[inline]
    pub fn as_utf8_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.bytes())
    }
}

impl BufMut for Buffer {
    unsafe fn advance_mut(&mut self, cnt: usize) {
        if self.data.len() + cnt > self.data.capacity() {
            panic!("Buffer::advance_mut(cnt): would advance past end of Buffer");
        }
        self.data.set_len(self.data.len() + cnt);
    }

    fn bytes_mut(&mut self) -> &mut [mem::MaybeUninit<u8>] {
        let len = self.data.len();
        let mut_len = self.data.capacity() - len;
        unsafe {
            self.data.set_len(self.data.capacity());
            let mut_data = &mut self.data[len..];
            let ptr = mut_data.as_mut_ptr() as *mut mem::MaybeUninit<u8>;
            let r = &mut slice::from_raw_parts_mut(ptr, mut_len)[..];
            self.data.set_len(len);
            r
        }
    }

    fn remaining_mut(&self) -> usize {
        self.data.capacity() - self.data.len()
    }
}

impl Buf for Buffer {
    fn advance(&mut self, cnt: usize) {
        // advance buffer read pointer.
        self.rd_pos += cnt;
        if self.rd_pos > self.len() {
            // "It is recommended for implementations of advance to
            // panic if cnt > self.remaining()"
            panic!("read position advanced beyond end of buffer");
        }
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        if self.rd_pos >= self.len() {
            return &[][..];
        }
        &self.data[self.start_offset + self.rd_pos..]
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.len() - self.rd_pos
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.start_offset + self.rd_pos..]
    }
}

impl From<&[u8]> for Buffer {
    fn from(src: &[u8]) -> Self {
        let mut buffer = Buffer::new();
        buffer.extend_from_slice(src);
        buffer
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(src: Vec<u8>) -> Self {
        Buffer::from(&src[..])
    }
}

impl From<&str> for Buffer {
    fn from(src: &str) -> Self {
        Buffer::from(src.as_bytes())
    }
}

impl From<String> for Buffer {
    fn from(src: String) -> Self {
        Buffer::from(src.as_str().as_bytes())
    }
}

impl From<bytes::Bytes> for Buffer {
    fn from(src: bytes::Bytes) -> Self {
        Buffer::from(&src[..])
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Buffer::new()
    }
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let cap = self.data.capacity();
        let len = self.len();
        f.debug_struct("Buffer")
            .field("start_offset", &self.start_offset)
            .field("rd_pos", &self.rd_pos)
            .field("len", &len)
            .field("capacity", &cap)
            .field("data", &"[data]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer() {
        let mut b = Buffer::new();
        b.reserve(4096);
        b.start_offset = 23;
        b.data.resize(b.start_offset, 0);
        for i in 0..50000 {
            b.put_str("xyzzyxyzzy");
        }
        assert!(b.len() == 500000);
        assert!(&b[1000..1010] == &b"xyzzyxyzzy"[..]);
    }

    #[test]
    fn test_split() {
        let mut b = Buffer::new();
        for i in 0..5000 {
            b.put_str("xyzzyxyzzy");
        }
        assert!(b.len() == 50000);
        let mut n = b.split_off(5000);
        assert!(b.len() == 5000);
        assert!(n.len() == 45000);
        assert!(&b[1000..1010] == &b"xyzzyxyzzy"[..]);
        assert!(&n[1000..1010] == &b"xyzzyxyzzy"[..]);

        n.start_offset += 10;
        n.put_str("xyzzyxyzzy");

        let x = n.split_to(20000);
        assert!(n.len() == 25000);
        assert!(x.len() == 20000);
        assert!(&n[1000..1010] == &b"xyzzyxyzzy"[..]);
        assert!(&x[1000..1010] == &b"xyzzyxyzzy"[..]);
    }
}
