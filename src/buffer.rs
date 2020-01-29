use std::collections::{HashMap, VecDeque};
use std::io::{self, IoSlice, IoSliceMut as StdIoSliceMut, Read, Write};
use std::mem::MaybeUninit;
use std::slice;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::buf::IoSliceMut;
use bytes::{Buf, BufMut, BytesMut};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

static BUFPOOL: Lazy<BufPool> = Lazy::new(|| BufPool::new());

struct BufPool {
    inner:  Arc<Mutex<BufPoolInner>>,
}

struct PoolEntry {
    data:   Vec<u8>,
    when:   Instant,
}

struct BufPoolInner {
    pool: HashMap<usize, VecDeque<PoolEntry>>,
    now: Instant,
}

impl BufPool {
    fn new() -> BufPool {
        let inner = BufPoolInner {
            pool: HashMap::new(),
            now: Instant::now(),
        };
        BufPool::cleaner();
        BufPool {
            inner:  Arc::new(Mutex::new(inner)),
        }
    }

    fn cleaner() {
        thread::spawn(|| {
            thread::sleep(Duration::from_millis(1000));
            loop {
                // Every second, run this loop.
                thread::sleep(Duration::from_millis(1000));
                {
                    // Get current time. Cache it as well.
                    let now = Instant::now();
                    let mut inner = BUFPOOL.inner.lock();
                    inner.now = now;

                    // Walk over the HashMap containing the VecDeque's.
                    for m in inner.pool.values_mut() {

                        // Walk over the VecDequeue's.
                        while !m.is_empty() {

                            // Any entries older than 4-5 seconds get dropped.
                            let age = now.duration_since(m.back().unwrap().when);
                            if age > Duration::from_millis(4000) {
                                let v = m.pop_back().unwrap();
                                trace!("BUFPOOL.cleaner({}): dropped", v.data.len());
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn get(&self, size: usize) -> Vec<u8> {
        let mut inner = self.inner.lock();
        let pool = &mut inner.pool;

        if let Some(queue) = pool.get_mut(&size) {
            if let Some(v) = queue.pop_front() {
                trace!("BUFPOOL.get({}): from pool", size);
                return v.data;
            }
        }
        drop(inner);

        trace!("BUFPOOL.get({}): allocate", size);
        let mut v = Vec::with_capacity(size);
        v.resize(size, 0);
        v
    }

    fn put(&self, v: Vec<u8>) {
        let len = v.len();
        if len != v.capacity() ||
            (len != 128 && len != 256 && len != 512 && len % 1024 != 0) {
                trace!("BUFPOOL.put({}): dropped", v.len());
                return;
        }

        let mut inner = self.inner.lock();
        let now = inner.now;
        let pool = &mut inner.pool;

        if !pool.contains_key(&len) {
            pool.insert(len, VecDeque::new());
        }
        trace!("BUFPOOL.put({}): saved", v.len());
        let entry = PoolEntry {
            data: v,
            when: now,
        };
        pool.get_mut(&len).unwrap().push_front(entry);
    }

    fn put_vec(&self, vec: &mut Vec<Vec<u8>>) {
        if vec.len() == 0 {
            return;
        }
        let mut inner = self.inner.lock();
        let now = inner.now;
        let pool = &mut inner.pool;

        for v in vec.drain(..) {
            let len = v.len();
            if len != v.capacity() ||
                (len != 128 && len != 256 && len != 512 && len % 1024 != 0) {
                    trace!("BUFPOOL.put({}): dropped", v.len());
                    continue;
            }

            if !pool.contains_key(&len) {
                pool.insert(len, VecDeque::new());
            }
            trace!("BUFPOOL.put({}): saved", v.len());
            let entry = PoolEntry {
                data: v,
                when: now,
            };
            pool.get_mut(&len).unwrap().push_front(entry);
        }
    }
}

/// A `Buf` and `BufMut` with non-contiguous backing storage.
///
/// Backing storage is allocated in fixed-size blocks. This prevents reallocating
/// when the buffer grows.
///
/// We keep a pool of unused blocks around, so that we don't have to initialize
/// every allocation. If the whole read-into-uninitialized memory situation is
/// solved we might want to revisit this, as the system allocator is probably
/// a very good pool allocator itself.
///
pub struct Buffer {
    block_sz:   usize,
    rd_offset:  usize,
    wr_offset:  usize,
    capacity:   usize,
    data:       Vec<Vec<u8>>,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        BUFPOOL.put_vec(&mut self.data);
    }
}

impl Buffer {
    /// Create new buffer.
    pub fn new() -> Buffer {
        Buffer::with_block_size(0)
    }

    /// Create new Buffer, but start off with a minimal block size.
    pub fn with_block_size(block_size: usize) -> Buffer {
        Buffer {
            block_sz:   block_size,
            rd_offset:  0,
            wr_offset:  0,
            capacity:   0,
            data:       Vec::new(),
        }
    }

    /// clear buffers.
    pub fn clear(&mut self) {
        self.rd_offset = 0;
        self.wr_offset = 0;
        self.capacity = 0;
        BUFPOOL.put_vec(&mut self.data);
    }

    /// total length of all data in this Buffer.
    pub fn len(&self) -> usize {
        self.wr_offset
    }

    /// Get a Vec of IoSlices to use with `read_vectored()`.
    pub fn get_ioslices_mut(&mut self, len: usize) -> Vec<StdIoSliceMut<'_>> {

        // make sure we have enough capacity.
        let mut len = len;
        if len == 0 {
            if self.capacity < self.wr_offset + 4096 {
                self.add_capacity();
            }
            len = self.capacity - self.wr_offset;
        } else {
            while self.capacity - self.wr_offset < len {
                self.add_capacity();
            }
        }

        // get index and offset for start and end.
        let (mut wr_idx, mut wr_off) = self.get_pos(self.wr_offset);
        let (end_idx, end_off) = self.get_pos(self.wr_offset + len);

        // No more than 1024 slices at a time.
        let num = std::cmp::min(self.data.len() - wr_idx, 1024);
        let max = wr_idx + num;
        let mut slices = Vec::with_capacity(num);

        // mutable reference to all entries that we're splitting up later.
        let mut data_ref = &mut self.data[wr_idx..];

        while wr_idx < max && wr_idx <= end_idx {

            // get a mutable reference to the next entry in the Vec.
            let (head, tail) = data_ref.split_first_mut().unwrap();
            data_ref = tail;

            // the last entry might have a shorter length.
            let data = if wr_idx == end_idx {
                &mut head[wr_off..end_off]
            } else {
                &mut head[wr_off..]
            };

            //println!("XXX get_ioslices_mut idx {} off {} data.len() {}", wr_idx, wr_off, data.len());
            if data.len() > 0 {
                slices.push(StdIoSliceMut::new(data));
            }
            wr_idx += 1;
            wr_off = 0;
        }
        slices
    }

    /// Get a Vec of IoSlices to use with `write_vectored()`.
    pub fn get_ioslices(&self) -> Vec<IoSlice> {

        //println!("XXX get_ioslices");
        let (mut idx, mut off) = self.get_pos(self.rd_offset);

        // No more than 1024 slices at a time.
        let num = std::cmp::min(self.data.len() - idx, 1024);
        let max = idx + num;
        let mut slices = Vec::with_capacity(num);

        while idx < max {
            let data = &self.data[idx][off..];
            slices.push(IoSlice::new(data));
            idx += 1;
            off = 0;
        }
        slices
    }

    /// Write all data in this `Buffer` to a file.
    pub fn write_all(&mut self, mut file: impl Write) -> io::Result<()> {
        while self.remaining() > 0 {
            let mut slices = self.get_ioslices();
            let done = file.write_vectored(&mut slices)?;
            if done == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            self.advance(done);
        }
        self.clear();
        Ok(())
    }

    /// Read an exact number of bytes.
    ///
    /// The memory needed is allocated up front. More memory might be allocated
    /// than what is needed, since we allocate in blocks.
    pub fn read_exact(&mut self, mut reader: impl Read, len: usize) -> io::Result<()> {
        let mut done = 0;
        loop {
            let mut slices = self.get_ioslices_mut(len - done);
            let sz = reader.read_vectored(&mut slices)?;
            if sz == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.advance_mut_safe(sz);
            done += sz;
            if done == len {
                break;
            }
        }
        Ok(())
    }

    /// Read until end-of-file.
    pub fn read_all(&mut self, mut reader: impl Read) -> io::Result<()> {
        loop {
            let mut slices = self.get_ioslices_mut(0);
            let sz = reader.read_vectored(&mut slices)?;
            if sz == 0 {
                break;
            }
            self.advance_mut_safe(sz);
        }
        Ok(())
    }

    /// Add data to this buffer.
    pub fn extend_from_slice(&mut self, extend: &[u8]) {

        //println!("XXX extend_from_slice {}", extend.len());
        let mut extend = extend;
        while extend.len() > 0 {
            if self.capacity - self.wr_offset < extend.len() {
                self.add_capacity();
            }
            let (idx, off) = self.get_pos(self.wr_offset);
            let data = &mut self.data[idx][off..];
            let amount = std::cmp::min(data.len(), extend.len());
            (&mut data[..amount]).copy_from_slice(&extend[..amount]);
            self.wr_offset += amount;
            extend = &extend[amount..];
        }
    }

    /// Make sure at least `size` bytes are available for use with `get_ioslices_mut()`.
    pub fn reserve(&mut self, size: usize) {
        if self.data.len() == 0 {
            if size > 8192 {
                self.block_sz = 128 * 1024;
            } else if size > 1024 {
                self.block_sz = 8192;
            }
        }
        if self.data.len() == 1 {
            if self.block_sz < 8192 && self.wr_offset + size > 8192 {
                self.block_sz = 8192;
            }
        }
        while self.capacity - self.wr_offset < size {
            self.add_capacity();
        }
    }

    /// Copy this `Buffer` to a `BytesMut`.
    pub fn to_bytes_mut(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        for idx in 0 .. self.data.len() {
            buf.extend_from_slice(&self.data[idx][..]);
        }
        buf
    }

    /// Like `advance_mut()`, but no need to use `unsafe`.
    pub fn advance_mut_safe(&mut self, cnt: usize) {
        // If we have less remaining bytes than the amount we are being
        // asked to advance the write cursor, that must be a bug.
        if cnt > self.capacity - self.wr_offset {
            panic!("advance_mut: write cursor would be advanced beyond buffer");
        }
        self.wr_offset += cnt;
    }

    /// Add a string to the buffer.
    pub fn put_str(&mut self, s: impl AsRef<str>) {
        self.extend_from_slice(s.as_ref().as_bytes());
    }

    // Add capacity.
    //
    // Initial block_size is self.block_sz.
    //
    // Anytime we do an add_capacity() after that, block_size is made larger
    // until it is the maximum size.
    //
    // As long as the previous block_size <= 8192, we copy the data from
    // the old block to the new block and drop the old block.
    //
    // With larger block sizes, we actually add blocks.
    //
    fn add_capacity(&mut self) {

        let block_sz = if self.data.len() == 0 {
            self.block_sz
        } else if self.block_sz < 1024 {
            1024
        } else if self.block_sz < 8 * 1024 {
            8192
        } else {
            128 * 1024
        };

        let mut data = BUFPOOL.get(block_sz);

        if self.data.len() == 1 && self.block_sz <= 8192 {
            // extend the first block.
            data[..self.wr_offset].copy_from_slice(&self.data[0][..self.wr_offset]);
            let old_data = std::mem::replace(&mut self.data[0], data);
            BUFPOOL.put(old_data);
            self.capacity = block_sz;
        } else {
            // add a new block.
            self.data.push(data);
            self.block_sz = block_sz;
            self.capacity += block_sz;
        }
    }

    // get index of data block, and offset.
    fn get_pos(&self, offset: usize) -> (usize, usize) {
        let mut cnt = 0;
        let mut idx = 0;
        while cnt + self.data[idx].len() < offset {
            cnt += self.data[idx].len();
            idx += 1;
        }
        (idx, offset - cnt)
    }
}

// Can't put this in the impl, because the compiler then complains:
//
// 91 |     fn bytes_vectored_mut<'a>(&'a mut self, dst: &mut [IoSliceMut<'a>]) -> usize {
//    |                           -- lifetime `'a` defined here ...
// 99 |                 let data = self.do_bytes_mut(wr_offset);
//    |                            ^^^^------------------------
//    |                            |
//    |                            mutable borrow starts here in previous iteration of loop
//    |                            argument requires that `*self` is borrowed for `'a`
//
// So to be DRY, define it as a macro.
//
macro_rules! do_bytes_mut {
    ($this:expr, $wr_offset:expr) => ({
        if $wr_offset == $this.capacity {
            $this.add_capacity();
        }
        let (idx, off) = $this.get_pos($wr_offset);

        // This is safe, as the memory is actually initialized.
        let block_sz = $this.data[idx].len();
        let ptr = $this.data[idx].as_mut_ptr() as *mut MaybeUninit<u8>;
        unsafe {
            &mut slice::from_raw_parts_mut(ptr, block_sz)[off..]
        }
    })
}

impl BufMut for Buffer {

    fn bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        do_bytes_mut!(self, self.wr_offset)
    }

    fn bytes_vectored_mut<'a>(&'a mut self, dst: &mut [IoSliceMut<'a>]) -> usize {

        let mut wr_offset = self.wr_offset;
        let mut dst_idx = 0;
        let cap = self.capacity;

        while dst_idx < dst.len() && wr_offset < cap {
            let data = do_bytes_mut!(self, wr_offset);
            wr_offset += data.len();
            dst[dst_idx] = IoSliceMut::from(data);
            dst_idx += 1;
        }

        dst_idx
    }

    // The API says this must be "unsafe", but it's actually safe as we
    // don't have any uninitialized memory.
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.advance_mut_safe(cnt)
    }

    // basically infinite data.
    fn remaining_mut(&self) -> usize {
        std::usize::MAX - self.wr_offset
    }
}

impl Buf for Buffer {
    fn advance(&mut self, cnt: usize) {

        // advance buffer read pointer.
        self.rd_offset += cnt;
        if self.rd_offset > self.wr_offset {
            // "It is recommended for implementations of advance to
            // panic if cnt > self.remaining()"
            panic!("read position advanced beyond end of buffer");
        }

        // drop buffers we do not need anymore. XXX TODO
        // let (old_idx, _, _) = self.get_pos(rd_offset - cnt);
        // let (cur_idx, _, _) = self.get_pos(rd_offset);
        // if cur_idx > old_idx {
        //     for idx in old_idx .. cur_idx - 1 {
        //         let v = std::mem::replace(&mut self.data[idx], Vec::new());
        //         BUFPOOL.put(v);
        //     }
        // }
    }

    fn bytes(&self) -> &[u8] {
        if self.rd_offset >= self.wr_offset {
            return &[];
        }

        let (rd_idx, rd_off) = self.get_pos(self.rd_offset);
        let (wr_idx, wr_off) = self.get_pos(self.wr_offset);

        if rd_idx == wr_idx {
            &self.data[rd_idx][rd_off..wr_off]
        } else {
            &self.data[rd_idx][rd_off..]
        }
    }

    fn bytes_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {

        let mut rd_offset = self.rd_offset;
        let mut dst_idx = 0;

        while dst_idx < dst.len() && rd_offset < self.wr_offset {
            let (idx, off) = self.get_pos(rd_offset);
            let data = &self.data[idx][off..];
            dst[dst_idx] = IoSlice::new(data);
            rd_offset += data.len();
            dst_idx += 1;
        }

        dst_idx
    }

    fn remaining(&self) -> usize {
        self.wr_offset - self.rd_offset
    }
}

impl From<&[u8]> for Buffer {
    fn from(src: &[u8]) -> Self {
        let mut buffer = Buffer::with_block_size(1024);
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

