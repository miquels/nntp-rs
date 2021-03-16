use std::fs;
use std::io;
use std::os::unix::io::AsRawFd;
use std::ptr;

use libc::c_void;
use once_cell::sync::Lazy;

static PAGE_SIZE: Lazy<usize> = Lazy::new(|| unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize });

/// A handle to a file of which the pages are mlocked() in memory.
pub struct MemLock {
    ptr:    *const u8,
    len:    usize,
}

impl MemLock {

    /// Return new, _unlocked_ MemLock struct.
    pub fn new(file: fs::File) -> io::Result<MemLock> {
        let meta = file.metadata()?;
        let len = meta.len() as libc::size_t;

        let ptr = unsafe {
            libc::mmap(ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, file.as_raw_fd(), 0)
        } as *const u8;
        if ptr == ptr::null() {
            return Err(io::Error::last_os_error());
        }
        Ok(MemLock{ ptr, len })
    }

    /// Check if we can actually mlock the file's pages.
    ///
    /// Linux-only, uses `mlock2`.
    pub fn mlock_test(&self) -> io::Result<()> {
        const MLOCK_ONFAULT: libc::c_int = 0x01;

        unsafe {
            let res = libc::syscall(libc::SYS_mlock2, self.ptr, self.len, MLOCK_ONFAULT);
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res as i32));
            }
            libc::munlock(self.ptr as *const c_void, self.len);
        }
        Ok(())
    }

    /// Actually mlock the file's pages.
    pub fn mlock(&self) -> io::Result<()> {
        let res = unsafe {
            libc::mlock(self.ptr as *const c_void, self.len)
        };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Resize the locked area.
    pub fn resize(&mut self, new_len: usize) -> io::Result<()> {
        unsafe {

            // remap range.
            let ptr = self.ptr as *mut c_void;
            let ptr = libc::mremap(ptr, self.len, new_len, libc::MREMAP_MAYMOVE) as *const u8;
            if ptr == ptr::null() {
                return Err(io::Error::last_os_error());
            }

            // mremap keep the range that was locked, locked.
            // so we only need to mlock the extra pages if new_len > len.
            if new_len > self.len {
                let start_addr = round_pagesz(self.ptr as usize) as *const c_void;
                let len = new_len - round_pagesz(self.len);
                // XXX we ignore re-mlock() errors, for now.
                libc::mlock(start_addr, len);
            }

            self.ptr = ptr;
            self.len = new_len;
        }
        Ok(())
    }
}

impl Drop for MemLock {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut c_void, self.len);
        }
    }
}

fn round_pagesz(sz: usize) -> usize {
    use std::ops::Deref;
    let page_size = *(PAGE_SIZE.deref());
    (sz / page_size) * page_size
}

