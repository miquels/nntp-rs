use std::collections::{HashMap, VecDeque};
use std::io::{self, IoSlice, IoSliceMut as StdIoSliceMut, Write};
use std::mem::MaybeUninit;
use std::slice;
use std::sync::Arc;

use bytes::buf::IoSliceMut;
use bytes::{Buf, BufMut, BytesMut};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

static BUFPOOL: Lazy<BufPool> = Lazy::new(|| BufPool::new());

struct BufPool {
    inner:  Arc<Mutex<BufPoolInner>>,
}

struct BufPoolInner {
    pool: HashMap<usize, VecDeque<Vec<u8>>>,
}

impl BufPool {
    fn new() -> BufPool {
        let inner = BufPoolInner {
            pool: HashMap::new()
        };
        BufPool {
            inner:  Arc::new(Mutex::new(inner)),
        }
    }

    fn get(&self, size: usize) -> Vec<u8> {
        let mut inner = self.inner.lock();
        let pool = &mut inner.pool;

        if let Some(queue) = pool.get_mut(&size) {
            if let Some(v) = queue.pop_front() {
                trace!("BUFPOOL.get({}): from pool", size);
                return v;
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
        let pool = &mut inner.pool;

        if !pool.contains_key(&len) {
            pool.insert(len, VecDeque::new());
        }
        trace!("BUFPOOL.put({}): saved", v.len());
        pool.get_mut(&len).unwrap().push_front(v);
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
        for d in self.data.drain(..) {
            if d.len() > 0 {
                BUFPOOL.put(d);
            }
        }
    }
}

impl Buffer {
    // Default blocksize.
    pub const DEFAULT_BLOCKSIZE: usize = 128; // 128*1024;

    /// Create new buffer, default block size.
    pub fn new() -> Buffer {
        Buffer::with_size(Self::DEFAULT_BLOCKSIZE)
    }

    /// Create new Buffer, define block size.
    pub fn with_size(block_size: usize) -> Buffer {
        let block_size = if block_size == 0 { Self::DEFAULT_BLOCKSIZE } else { block_size };
        Buffer {
            block_sz:   block_size,
            rd_offset:  0,
            wr_offset:  0,
            capacity:   0,
            data:       Vec::new(),
        }
    }

    /// Create new Buffer with a pre-allocated capacity.
    pub fn with_capacity(block_size: usize, capacity: usize) -> Buffer {
        let mut buf = Buffer::with_size(block_size);
        while buf.capacity < capacity {
            buf.add_block();
        }
        buf
    }

    /// clear buffers.
    pub fn clear(&mut self) {
        self.rd_offset = 0;
        self.wr_offset = 0;
        self.capacity = 0;
        for d in self.data.drain(..) {
            BUFPOOL.put(d);
        }
    }

    /// Get a Vec of IoSlices to use with `read_vectored()`.
    pub fn get_ioslices_mut(&mut self) -> Vec<StdIoSliceMut<'_>> {
        let mut slices = Vec::with_capacity(self.data.len());
        if self.wr_offset == self.capacity {
            self.add_block();
        }
        let mut idx = self.wr_offset / self.block_sz;
        let mut off = self.wr_offset - (idx * self.block_sz);
        let len = self.data.len();
        let mut data_ref = &mut self.data[idx..];
        while idx < len {
            let (head, tail) = data_ref.split_first_mut().unwrap();
            data_ref = tail;
            let data = &mut head[off..];
            //println!("XXX get_ioslices_mut idx {} off {} data.len() {}", idx, off, data.len());
            slices.push(StdIoSliceMut::new(data));
            idx += 1;
            off = 0;
        }
        slices
    }

    /// Get a Vec of IoSlices to use with `write_vectored()`.
    pub fn get_ioslices(&self) -> Vec<IoSlice> {

        //println!("XXX get_ioslices");
        let mut idx = self.rd_offset / self.block_sz;
        let mut off = self.rd_offset - (idx * self.block_sz);

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
        //println!("XXX write_all");
        while self.remaining() > 0 {
            let mut slices = self.get_ioslices();
            let done = file.write_vectored(&mut slices)?;
            self.advance(done);
        }
        self.clear();
        Ok(())
    }

    /// Add data to this buffer.
    pub fn extend_from_slice(&mut self, extend: &[u8]) {

        //println!("XXX extend_from_slice {}", extend.len());
        let mut extend = extend;
        while extend.len() > 0 {
            if self.capacity - self.wr_offset < extend.len() {
                self.add_block();
            }
            let idx = self.wr_offset / self.block_sz;
            let off = self.wr_offset - (idx * self.block_sz);
            let data = &mut self.data[idx][off..];
            let amount = std::cmp::min(data.len(), extend.len());
            (&mut data[..amount]).copy_from_slice(&extend[..amount]);
            self.wr_offset += amount;
            extend = &extend[amount..];
        }
    }

    /// Max sure at least `size` bytes are available for use with `get_ioslices_mut()`.
    pub fn reserve(&mut self, size: usize) {
        while self.capacity - self.wr_offset < size {
            self.add_block();
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

    // add one block of capacity.
    fn add_block(&mut self) {
        let block_sz = self.block_sz;
        let data = BUFPOOL.get(block_sz);
        self.data.push(data);
        self.capacity += block_sz;
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
        let block_sz = $this.block_sz;
        if $wr_offset == $this.capacity {
            $this.add_block();
        }
        let idx = $wr_offset / block_sz;
        let off = $wr_offset - (idx * block_sz);

        // This is safe, as the memory is actually initialized.
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
        // If we have less remaining bytes than the amount we are being
        // asked to advance the write cursor, that must be a bug.
        if cnt > self.capacity - self.wr_offset {
            panic!("write cursor would be advanced beyond buffer");
        }
        self.wr_offset += cnt;
    }

    // basically infinite data.
    fn remaining_mut(&self) -> usize {
        std::usize::MAX - self.wr_offset
    }
}

impl Buf for Buffer {
    fn advance(&mut self, cnt: usize) {
        let block_sz = self.block_sz;
        let old_idx = self.rd_offset / block_sz;

        // do the actual advancing.
        self.rd_offset += cnt;
        if self.rd_offset > self.wr_offset {
            // "It is recommended for implementations of advance to
            // panic if cnt > self.remaining()"
            println!("XXX advance({}): rd_offset {}, wr_offset {}", cnt, self.rd_offset, self.wr_offset);
            panic!("read position advanced beyond end of buffer");
        }

        // drop buffers we do not need anymore.
        let cur_idx = self.rd_offset / block_sz;
        if cur_idx > old_idx {
            for idx in old_idx .. cur_idx - 1 {
                let v = std::mem::replace(&mut self.data[idx], Vec::new());
                BUFPOOL.put(v);
            }
        }
    }

    fn bytes(&self) -> &[u8] {
        if self.rd_offset >= self.wr_offset {
            return &[];
        }

        let block_sz = self.block_sz;
        let rd_idx = self.rd_offset / block_sz;
        let rd_off = self.rd_offset - (rd_idx * block_sz);
        let wr_idx = self.wr_offset / block_sz;

        if rd_idx == wr_idx {
            let wr_off = self.wr_offset - (wr_idx * block_sz);
            &self.data[rd_idx][rd_off..wr_off]
        } else {
            &self.data[rd_idx][rd_off..]
        }
    }

    fn bytes_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {

        let mut rd_offset = self.rd_offset;
        let mut dst_idx = 0;
        let block_sz = self.block_sz;

        while dst_idx < dst.len() && rd_offset < self.wr_offset {
            let idx = rd_offset / block_sz;
            let off = rd_offset - (idx * block_sz);
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

