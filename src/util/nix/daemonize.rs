use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

// There are quite a few systemcall wrappers here. Most are abviously safe,
// but fork() can be very dangerous. Note that all the wrapper functions
// are private and cannot be used outside this file.

fn pipe() -> io::Result<(fs::File, fs::File)> {
    let mut fds: [libc::c_int; 2] = [0; 2];
    // this is safe, `fds` has been initialized.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // this is safe, `fds` is valid.
    Ok((unsafe { fs::File::from_raw_fd(fds[0]) }, unsafe {
        fs::File::from_raw_fd(fds[1])
    }))
}

enum Fork {
    Parent(u32),
    Child,
}

fn fork() -> io::Result<Fork> {
    // This can indeed be unsafe, the caller has to be very careful.
    // This function should also be marked unsafe because of that,
    // but we do not export it outside of this file / module.
    match unsafe { libc::fork() } {
        n if n > 0 => Ok(Fork::Parent(n as u32)),
        0 => Ok(Fork::Child),
        _ => Err(io::Error::last_os_error()),
    }
}

fn setsid() -> io::Result<()> {
    // safe: no rust-related side-effects.
    if unsafe { libc::setsid() } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn dup2(oldfd: RawFd, newfd: RawFd) -> io::Result<()> {
    // safe: no rust-related side-effects.
    if unsafe { libc::dup2(oldfd, newfd) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Daemonize: fork child, exit parent.
pub fn daemonize(pidfile: Option<&String>, std_close: bool) -> io::Result<()> {
    // if std_close is set filedescriptors 0, 1, 2 are set to /dev/null
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

    // create a pipe to be able to communicate with the child.
    let (mut wait_read, mut wait_write) = pipe().map_err(|e| ioerr!(e.kind(), "pipe: {}", e))?;

    match fork() {
        Ok(Fork::Child) => {
            // wait for the parent process to tell us it's OK to continue.
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

            // set a new sesison-id.
            let _ = setsid();

            // connect filedescriptors 0, 1, 2 to /dev/null if requested.
            if let Some(dev_null) = dev_null {
                let fd = dev_null.as_raw_fd();
                let _ = dup2(fd, 0);
                let _ = dup2(fd, 1);
                let _ = dup2(fd, 2);
                if fd < 2 {
                    std::mem::forget(dev_null);
                }
            }

            // we're ready!
            Ok(())
        },
        Ok(Fork::Parent(pid)) => {
            // Parent. Should exit ASAP.
            drop(wait_read);

            // See if we want to write a pidfile.
            if let Some(pidfile) = pidfile {
                // if this fails, we return early with an error. that means the
                // pipe will be closed without any data being written to it,
                // and that in turn will cause the child to exit as well.
                let mut file =
                    fs::File::create(&pidfile).map_err(|e| ioerr!(e.kind(), "{}: {}", pidfile, e))?;
                writeln!(file, "{}", pid).map_err(|e| ioerr!(e.kind(), "{}: {}", pidfile, e))?;
            }

            // Tell the child to go ahead, then exit.
            let _ = write!(wait_write, "Ok");
            drop(wait_write);
            std::process::exit(0);
        },
        Err(e) => Err(ioerr!(e.kind(), "fork: {}", e)),
    }
}
