use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;

use libc::{self, gethostname};

use crate::util::UnixTime;

/// Rust interface to the libc gethostname function.
///
/// If unqualified, returns "hostname.local".
/// If unset, returns "unconfigured.local".
pub fn hostname() -> String {
    let len = 255;
    let mut buf = Vec::<u8>::new();
    buf.resize(len + 1, 0);
    let ptr = buf.as_mut_ptr() as *mut libc::c_char;

    unsafe {
        // this is safe, `ptr` points to buf which has been initialized.
        if gethostname(ptr, len as libc::size_t) != 0 {
            return String::from("unconfigured.local");
        }
        // this is also safe. since `buf` was initialized with zeroes and
        // is actually one byte longer than what we told `gethostname`,
        // it's guaranteed to be zero-terminated.
        let mut h = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        if !h.contains(".") {
            h += ".local";
        }
        h
    }
}

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

pub fn getpid() -> u32 {
    // safe: systemcall wrapper, no pointers.
    unsafe { libc::getpid() as u32 }
}

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

pub fn setgroups(gids: &[u32]) -> io::Result<()> {
    let mut g = Vec::<libc::gid_t>::new();
    g.extend(gids.iter().map(|&g| g as libc::gid_t));
    if unsafe { libc::setgroups(g.len(), g.as_ptr() as _) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
mod congestion_control {

    use serde::Deserialize;
    use std::io;
    use std::os::unix::io::AsRawFd;

    #[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
    pub enum CongestionControl {
        #[serde(rename = "cubic")]
        Cubic,
        #[serde(rename = "reno")]
        Reno,
        #[serde(rename = "bbr")]
        Bbr,
    }

    pub fn set_congestion_control(strm: &tokio::net::TcpStream, algo: CongestionControl) -> io::Result<()> {
        let algo_s = match algo {
            CongestionControl::Cubic => "cubic\0",
            CongestionControl::Reno => "reno\0",
            CongestionControl::Bbr => "bbr\0",
        };
        let r = unsafe {
            // safe: all data has been initialized.
            libc::setsockopt(
                strm.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_CONGESTION,
                algo_s.as_ptr() as *const libc::c_void,
                (algo_s.len() - 1) as libc::socklen_t,
            )
        };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod congestion_control {
    use serde::Deserialize;

    #[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
    pub enum CongestionControl {}

    pub fn set_congestion_control(_fd: impl AsRawFd, _algo: CongestionControl) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

pub use congestion_control::*;

#[cfg(target_os = "linux")]
mod pacing_rate {

    use std::io;
    use std::os::unix::io::AsRawFd;

    pub fn set_max_pacing_rate(strm: &tokio::net::TcpStream, bytes_per_sec: u32) -> io::Result<()> {
        let r = unsafe {
            // safe: all data has been initialized.
            libc::setsockopt(
                strm.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_MAX_PACING_RATE,
                (&bytes_per_sec) as *const u32 as *const libc::c_void,
                std::mem::size_of_val(&bytes_per_sec) as libc::socklen_t,
            )
        };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod pacing_rate {

    pub fn set_max_pacing_rate(_fd: impl AsRawFd, _bytes_per_sec: u32) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

pub use pacing_rate::*;
