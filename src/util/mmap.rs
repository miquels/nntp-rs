use std::fmt::{self, Debug};
use std::fs;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap::{Mmap, MmapMut, MmapOptions};

#[derive(Debug)]
pub enum MmapMode {
    Ro(Mmap),
    Rw(MmapMut),
}

pub struct MmapAtomicU32 {
    map:  MmapMode,
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
    pub fn new(file: &fs::File, rw: bool, offset: u64, num_elems: usize) -> io::Result<MmapAtomicU32> {
        let mut opts = MmapOptions::new();
        Ok(if rw {
            let mmap: MmapMut = unsafe { opts.offset(offset).len(num_elems * 4).map_mut(file)? };
            let data = mmap.as_ptr() as *const AtomicU32;
            MmapAtomicU32 {
                map: MmapMode::Rw(mmap),
                data,
            }
        } else {
            let mmap: Mmap = unsafe { opts.offset(offset).len(num_elems * 4).map(file)? };
            let data = mmap.as_ptr() as *const AtomicU32;
            MmapAtomicU32 {
                map: MmapMode::Ro(mmap),
                data,
            }
        })
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
