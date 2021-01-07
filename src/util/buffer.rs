//! Buffer implementation like Bytes / BytesMut.
//!
//! It is simpler and contains less unsafe code.
use std::default::Default;
use std::fmt;
use std::io::{self, Read, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::marker::Unpin;
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

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

    /// Create new Buffer.
    pub fn with_capacity(cap: usize) -> Buffer {
        Buffer {
            start_offset: 0,
            rd_pos:       0,
            data:         Vec::with_capacity(Self::round_size_up(cap)),
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

    pub fn bytes(&self) -> &[u8] {
        if self.rd_pos >= self.len() {
            return &[][..];
        }
        &self.data[self.start_offset + self.rd_pos..]
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

        // If "header" < 32K and "body" >= 32K, use a start_offset
        // for "body" and copy "header".
        if self.start_offset == 0 && at < 32000 && self.len() - at >= 32000 {
            let mut bnew = Buffer::with_capacity(at);
            mem::swap(self, &mut bnew);
            self.extend_from_slice(&bnew[0..at]);
            bnew.start_offset = at;
            return bnew;
        }

        let mut bnew = Buffer::new();
        let bytes = self.bytes();
        bnew.extend_from_slice(&bytes[at..]);
        self.truncate(at);

        bnew
    }

    /// Add data to this buffer.
    #[inline]
    pub fn extend_from_slice(&mut self, extend: &[u8]) {
        self.reserve(extend.len());
        self.data.extend_from_slice(extend);
    }

    #[inline]
    fn round_size_up(size: usize) -> usize {
        if size < 128 {
            128
        } else if size < 4096 {
            4096
        } else if size < 65536 {
            65536
        } else {
            size.next_power_of_two()
        }
    }

    /// Make sure at least `size` bytes are available.
    #[inline]
    pub fn reserve(&mut self, size: usize) {
        let end = self.len() + size;
        if end < self.data.capacity() {
            return;
        }
        self.data.reserve(Self::round_size_up(end) - self.len());
    }

    /// total length of all data in this Buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() - self.start_offset
    }

    /// Split this Buffer in two parts.
    ///
    /// The second part remains in this buffer. The first part is
    /// returned to the caller.
    pub fn split_to(&mut self, size: usize) -> Buffer {
        let mut other = self.split_off(size);
        mem::swap(self, &mut other);
        other
    }

    /// Write all data in this `Buffer` to a file.
    pub fn write_all(&mut self, mut file: impl Write) -> io::Result<()> {
        while self.rd_pos < self.len() {
            let bytes = self.bytes();
            let size = bytes.len();
            file.write_all(bytes)?;
            self.rd_pos += size;
        }
        Ok(())
    }

    /// Add text data to this buffer.
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.extend_from_slice(s.as_bytes());
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

    /// Convert this buffer into a Vec<u8>.
    pub fn into_bytes(self) -> Vec<u8> {
        if self.start_offset > 0 {
            let mut v = Vec::with_capacity(Self::round_size_up(self.len()));
            v.extend_from_slice(self.bytes());
            v
        } else {
            self.data
        }
    }

    //
    // ===== Begin unsafe code =====
    //

    /// Read an exact number of bytes.
    pub fn read_exact(&mut self, reader: &mut std::fs::File, len: usize) -> io::Result<()> {
        self.reserve(len);

        // Safety: it is safe for a std::fs::File to read into uninitialized memory.
        unsafe {
            reader.read_exact(self.spare_capacity_mut())?;
            self.advance_mut(len);
        }
        Ok(())
    }

    unsafe fn spare_capacity_mut<T>(&mut self) -> &mut [T] {
        let len = self.data.len();
        let spare = self.data.capacity() - len;
        let ptr = self.data.as_mut_ptr().add(len) as *mut T;
        &mut slice::from_raw_parts_mut(ptr, spare)[..]
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        if self.data.len() + cnt > self.data.capacity() {
            panic!("Buffer::advance_mut(cnt): would advance past end of Buffer");
        }
        // Safety: unsafe fn calling unsafe fn.
        self.data.set_len(self.data.len() + cnt);
    }

    pub fn poll_read<R>(&mut self, reader: Pin<&mut R>, cx: &mut Context<'_>) -> Poll<io::Result<usize>>
    where
        R: AsyncRead + Unpin + ?Sized,
    {
        // Safety: ReadBuf::uninit takes a MaybeUninit.
        let mut buf = ReadBuf::uninit(unsafe { self.spare_capacity_mut() });
        futures::ready!(reader.poll_read(cx, &mut buf))?;
        // Safety: buf.filled is guaranteed to be initialized.
        let len = buf.filled().len();
        unsafe { self.advance_mut(len); }
        Poll::Ready(Ok(len))
    }

    //
    // ===== End unsafe code =====
    //
}

impl bytes::Buf for Buffer {
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
    fn chunk(&self) -> &[u8] {
        self.bytes()
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.len() - self.rd_pos
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.start_offset + self.rd_pos..]
    }
}

impl fmt::Write for Buffer {
    fn write_str(&mut self, s: &str) -> Result<(), fmt::Error> {
        self.push_str(s);
        Ok(())
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
        Buffer {
            start_offset: 0,
            rd_pos:       0,
            data:         src,
        }
    }
}

impl From<&str> for Buffer {
    fn from(src: &str) -> Self {
        Buffer::from(src.as_bytes())
    }
}

impl From<String> for Buffer {
    fn from(src: String) -> Self {
        Buffer::from(src.into_bytes())
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
        for _ in 0..50000 {
            b.put_str("xyzzyxyzzy");
        }
        assert!(b.len() == 500000);
        assert!(&b[1000..1010] == &b"xyzzyxyzzy"[..]);
    }

    #[test]
    fn test_split() {
        let mut b = Buffer::new();
        for _ in 0..5000 {
            b.put_str("xyzzyxyzzyz");
        }
        assert!(b.len() == 55000);
        let mut n = b.split_off(4918);
        assert!(b.len() == 4918);
        assert!(n.len() == 50082);
        println!("1. {}", std::str::from_utf8(&b[1100..1110]).unwrap());
        println!("2. {}", std::str::from_utf8(&n[1100..1110]).unwrap());
        assert!(&b[1100..1110] == &b"xyzzyxyzzy"[..]);
        assert!(&n[1100..1110] == &b"yzzyxyzzyz"[..]);

        n.start_offset += 13;

        let x = n.split_to(20000);
        println!("3. n.len() {}", n.len());
        println!("4. x.len() {}", x.len());
        println!("5. {}", std::str::from_utf8(&n[1000..1010]).unwrap());
        println!("6. {}", std::str::from_utf8(&x[1000..1010]).unwrap());
        assert!(n.len() == 30069);
        assert!(x.len() == 20000);
        assert!(&n[1000..1010] == &b"yxyzzyzxyz"[..]);
        assert!(&x[1000..1010] == &b"zzyxyzzyzx"[..]);
    }
}
