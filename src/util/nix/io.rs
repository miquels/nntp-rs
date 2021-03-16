use std::convert::TryInto;
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;

use crate::util::UnixTime;

#[cfg(all(target_family = "unix", not(target_os = "macos")))]
#[inline]
pub fn read_ahead(file: &fs::File, pos: u64, size: u64) {
    // this is safe, just a systemcall wrapper, no pointers.
    unsafe {
        use std::os::unix::io::AsRawFd;
        libc::posix_fadvise(
            file.as_raw_fd(),
            pos as libc::off_t,
            size as libc::off_t,
            libc::POSIX_FADV_WILLNEED,
        );
    }
}

#[cfg(not(all(target_family = "unix", not(target_os = "macos"))))]
#[inline]
pub fn read_ahead(_file: &fs::File, _pos: u64, _size: u64) {}

// This is a wrapper for the linux systemcall `preadv2`. We use it to
// be able to specify the `RWF_NOWAIT` flag. It means 'return whatever
// is present in the pagecache right now, and do not block'. Useful
// if you're pretty sure the data is in the pagecache so you don't
// have to delegate the read() to a threadpool.
#[cfg(target_os = "linux")]
mod try_read_at {
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    extern "C" {
        pub fn preadv2(
            fd: libc::c_int,
            iov: *const libc::iovec,
            iovcnt: libc::c_int,
            offset: libc::off_t,
            flags: libc::c_int,
        ) -> libc::ssize_t;
    }
    const RWF_NOWAIT: libc::c_int = 0x00000008;

    pub fn try_read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len:  buf.len() as libc::size_t,
        };
        let fd = file.as_raw_fd();
        let iovptr = &iov as *const libc::iovec;
        // this is safe: no uninitialized memory is being passed in, no pointers are manipulated.
        let res = unsafe { preadv2(fd, iovptr, 1, offset as libc::off_t, RWF_NOWAIT) };
        if res < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(res as usize)
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod try_read_at {
    use std::fs::File;
    use std::io;

    pub fn try_read_at(_file: &File, _buf: &mut [u8], _offset: u64) -> io::Result<usize> {
        Err(io::ErrorKind::InvalidInput)?
    }
}

pub use try_read_at::*;

pub fn touch(path: impl Into<PathBuf>, time: UnixTime) -> io::Result<()> {
    let path = CString::new(path.into().into_os_string().into_vec()).unwrap();
    let time = time
        .as_secs()
        .try_into()
        .map_err(|_| ioerr!(InvalidData, "invalid time"))?;
    let buf = libc::utimbuf {
        actime:  time,
        modtime: time,
    };
    unsafe {
        // safe: all data has been initialized.
        if libc::utime(path.as_ptr() as *const libc::c_char, &buf as *const libc::utimbuf) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

