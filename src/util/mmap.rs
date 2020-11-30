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

// The data in MmapAtomicU32 is atomic, so is Send + Sync.
unsafe impl Sync for MmapAtomicU32 {}
unsafe impl Send for MmapAtomicU32 {}

impl Debug for MmapAtomicU32 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MmapAtomicU32 {{ map: {:?} }}", self.map)
    }
}

impl MmapAtomicU32 {
    pub fn new(
        file: &fs::File,
        rw: bool,
        lock: bool,
        offset: u64,
        num_elems: usize,
    ) -> io::Result<MmapAtomicU32>
    {
        let mut opts = MmapOptions::new();
        let len = num_elems * 4;
        Ok(if rw {
            // safe, as long as nothing "outside" changes the mmap'ed file.
            let mmap: MmapMut = unsafe { opts.offset(offset).len(len).map_mut(file)? };
            let data = mmap.as_ptr() as *const AtomicU32;
            if lock {
                lock_index(mmap.as_ptr(), len);
            }
            MmapAtomicU32 {
                map: MmapMode::Rw(mmap),
                data,
            }
        } else {
            // safe, as long as nothing "outside" changes the mmap'ed file.
            let mmap: Mmap = unsafe { opts.offset(offset).len(len).map(file)? };
            let data = mmap.as_ptr() as *const AtomicU32;
            if lock {
                lock_index(mmap.as_ptr(), len);
            }
            MmapAtomicU32 {
                map: MmapMode::Ro(mmap),
                data,
            }
        })
    }

    pub fn load(&self, index: usize) -> u32 {
        // safe, as long as nothing "outside" changes the mmap'ed file.
        let elem = unsafe { self.data.offset(index as isize).as_ref().unwrap() };
        elem.load(Ordering::SeqCst)
    }

    pub fn store(&self, index: usize, val: u32) {
        // safe: a store does not import any state.
        let elem = unsafe { self.data.offset(index as isize).as_ref().unwrap() };
        elem.store(val, Ordering::SeqCst);
    }
}

fn lock_index(address: *const u8, size: usize) {
    match region::lock(address, size) {
        Ok(guard) => {
            // Don't need the guard, unmap will unlock.
            std::mem::forget(guard);
        },
        Err(e) => log::warn!("history file index: cannot mlock: {}", e),
    }
}
