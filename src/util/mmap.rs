use std::fmt::{self, Debug};
use std::fs;
use std::io;
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap::{MmapMut, MmapOptions};

pub struct MmapAtomicU32 {
    map:  MmapMut,
    data: *const AtomicU32,
}

unsafe impl Sync for MmapAtomicU32 {}
unsafe impl Send for MmapAtomicU32 {}

impl Debug for MmapAtomicU32 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MmapAtomicU32 {{ map: {:?} }}", self.map)
    }
}

impl MmapAtomicU32 {
    pub fn new(file: &fs::File, offset: u64, num_elems: usize) -> io::Result<MmapAtomicU32> {
        let mut opts = MmapOptions::new();
        let mmap: MmapMut = unsafe { opts.offset(offset).len(num_elems * 4).map_mut(file)? };
        let mut m = MmapAtomicU32 {
            map:  mmap,
            data: ptr::null_mut(),
        };
        m.data = m.map.as_ptr() as *const AtomicU32;
        Ok(m)
    }

    pub fn load(&self, index: usize) -> u32 {
        let elem = unsafe { self.data.offset(index as isize).as_ref().unwrap() };
        elem.load(Ordering::SeqCst)
    }

    pub fn store(&self, index: usize, val: u32) {
        let elem = unsafe { self.data.offset(index as isize).as_ref().unwrap() };
        elem.store(val, Ordering::SeqCst);
    }
}
