use std::fs;
use std::io;
use std::os::unix::io::AsRawFd;
use std::ptr;

use libc::c_void;

/// A handle to a file of which the pages are mlocked() in memory.
pub struct MemLock {
    ptr:    *const u8,
    len:    usize,
}

impl MemLock {

    /// Return new, _unlocked_ MemLock struct.
    pub fn new(file: &fs::File) -> io::Result<MemLock> {
        let meta = file.metadata()?;
        if meta.len() > (usize::MAX as u64) {
            return Err(ioerr!(Other, "file too big"));
        }
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
            self.ptr = ptr;
            self.len = new_len;

            // mremap keep the range that was locked, locked.
            // so we only need to mlock the extra pages if new_len > len.
            if new_len > self.len {
                // ignore result.
                libc::mlock(self.ptr as *const c_void, self.len);
            }
        }
        Ok(())
    }

    /// Size of the locked area.
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for MemLock {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut c_void, self.len);
        }
    }
}

