use std::io;

pub fn set_nodelay(strm: &tokio::net::TcpStream) -> io::Result<()> {
    use std::os::unix::io::{AsRawFd, FromRawFd};

    let fd = strm.as_raw_fd();
    let std_strm = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    let res = std_strm.set_nodelay(true);
    std::mem::forget(std_strm);
    res
}

pub fn set_only_v6(socket: &tokio::net::TcpSocket) -> io::Result<()> {
    use std::os::unix::io::{AsRawFd, FromRawFd};

    let fd = socket.as_raw_fd();
    let socket2 = unsafe { socket2::Socket::from_raw_fd(fd) };
    let res = socket2.set_only_v6(true);
    std::mem::forget(socket2);
    res
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
