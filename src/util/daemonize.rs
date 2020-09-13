use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

fn pipe() -> io::Result<(fs::File, fs::File)> {
    let mut fds: [libc::c_int; 2] = [0; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((unsafe { fs::File::from_raw_fd(fds[0]) }, unsafe {
        fs::File::from_raw_fd(fds[1])
    }))
}

enum Fork {
    Parent(u32),
    Child,
}

fn fork() -> io::Result<Fork> {
    match unsafe { libc::fork() } {
        n if n > 0 => Ok(Fork::Parent(n as u32)),
        0 => Ok(Fork::Child),
        _ => Err(io::Error::last_os_error()),
    }
}

fn setsid() -> io::Result<()> {
    if unsafe { libc::setsid() } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn dup2(oldfd: RawFd, newfd: RawFd) -> io::Result<()> {
    if unsafe { libc::dup2(oldfd, newfd) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn daemonize(pidfile: Option<&String>, std_close: bool) -> io::Result<()> {
    let dev_null = if std_close {
        Some(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/null")
                .map_err(|e| ioerr!(e.kind(), "/dev/null: {}", e))?,
        )
    } else {
        None
    };

    let (mut wait_read, mut wait_write) = pipe().map_err(|e| ioerr!(e.kind(), "pipe: {}", e))?;

    match fork() {
        Ok(Fork::Child) => {
            drop(wait_write);
            let mut s = String::new();
            if let Err(_) = wait_read.read_to_string(&mut s) {
                std::process::exit(1);
            }
            match s.as_str() {
                "Ok" => {},
                _ => std::process::exit(1),
            }
            drop(wait_read);
            let _ = setsid();
            if let Some(dev_null) = dev_null {
                let fd = dev_null.as_raw_fd();
                let _ = dup2(fd, 0);
                let _ = dup2(fd, 1);
                let _ = dup2(fd, 2);
                if fd < 2 {
                    std::mem::forget(dev_null);
                }
            }
            Ok(())
        },
        Ok(Fork::Parent(pid)) => {
            drop(wait_read);
            if let Some(pidfile) = pidfile {
                let mut file =
                    fs::File::create(&pidfile).map_err(|e| ioerr!(e.kind(), "{}: {}", pidfile, e))?;
                writeln!(file, "{}", pid).map_err(|e| ioerr!(e.kind(), "{}: {}", pidfile, e))?;
            }
            let _ = write!(wait_write, "Ok");
            drop(wait_write);
            std::process::exit(0);
        },
        Err(e) => Err(ioerr!(e.kind(), "fork: {}", e)),
    }
}
