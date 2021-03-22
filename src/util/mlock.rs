use std::env;
use std::fs;
use std::io;
use std::mem;
use std::os::unix::fs::MetadataExt;
use std::process::{Command, Stdio, exit};
use std::thread;
use std::time::Duration;

use crate::util::MemLock;

/// Mlock the pages of a file in the background.
///
/// This spawns and re-execs the current executable to do the actual
/// locking. The newly spawned executable must check to see if it was
/// called with arguments `mlock <file>`. If the environment variable
/// `NNTP_RS_MLOCK` is set to the same `<file>`, `mlock_run` must be called.
pub fn mlock_all(path: &str) -> io::Result<()> {

    // See if we can actually lock.
    let file = fs::File::open(path).map_err(|e| ioerr!(e.kind(), "{}: {}", path, e))?;
    let mem_lock = MemLock::new(&file).map_err(|e| ioerr!(e.kind(), "{}: {}", path, e))?;
    mem_lock.mlock_test().map_err(|e| ioerr!(e.kind(), "{}: {}", path, e))?;
    drop(mem_lock);

    // re-spawn current exe.
    let exe = env::current_exe().map_err(|e| ioerr!(e.kind(), "re-exec self: {}", e))?;
    let mut child = Command::new(&exe)
        .args(&[ "mlock", path ])
        .env("NNTP_RS_MLOCK", path)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| ioerr!(e.kind(), "{:?}: {}", exe, e))?;

    // stdin is the writing-side of a pipe that is the child's stdin.
    // forget it, so that it remains open until we exit.
    mem::forget(child.stdin.take());

    Ok(())
}

/// Actually open the file, and `mlock()` its pages.
///
/// The file is continually re`-stat`ted. If it was replaced, the
/// new file will be opened and `mlock`ed. If the size changed,
/// the locked memory region is adjusted.
///
/// A background thread reads from stdin, as soon as stdin is
/// readable (or EOF) the _entire process exits_.
pub fn mlock_run(path: &str) {

    thread::spawn(move || {
        // Read from stdin. When stdin is closed, the read succeeds and we exit.
        use std::io::Read;
        let mut buffer = [0; 16];
        let mut stdin = io::stdin();
        let _= stdin.read(&mut buffer);
        eprintln!("mlock process: exit");
        exit(0);
    });

    let mut mem_lock: Option<MemLock> = None;
    let mut file: Option<fs::File> = None;
    let mut first = true;

    loop {
        // sleep.
        if first {
            // first sleep is short enough to exit without pulling the
            // file into memory if the parent process fails initialization
            // for some reason.
            thread::sleep(Duration::from_millis(500));
            first = false;
        } else {
            thread::sleep(Duration::from_millis(5000));
        }

        // check if the history file exists.
        let path_meta = match fs::metadata(path) {
            Ok(meta) => meta,
            Err(_) => {
                // failed. close open file, and retry.
                mem_lock.take();
                file.take();
                continue;
            },
        };

        // if we have an open file..
        if let Some(ref file_ref) = file {

            // stat() the file.
            let file_meta = match file_ref.metadata() {
                Ok(meta) => meta,
                Err(e) => {
                    // failed. close open file, and retry.
                    eprintln!("nntp-rs: mlock: fstat {}: {}", path, e);
                    mem_lock.take();
                    file.take();
                    continue;
                },
            };

            if file_meta.ino() == path_meta.ino() {

                // file was not renamed. was it resize ?
                let mem_lock_ref = mem_lock.as_mut().unwrap();
                if file_meta.len() == mem_lock_ref.len() as u64 {

                    // same size, so nothing to do.
                    continue;
                }
                if file_meta.len() > usize::MAX as u64 {
                    eprintln!("nntp-rs: mlock: resize ({}, {}): file too big", path, file_meta.len());
                    mem_lock.take();
                    file.take();
                    continue;
                }

                // it was resized. resize the locked memory area too.
                if let Err(e) = mem_lock_ref.resize(file_meta.len() as usize) {

                    // failed. close open file.
                    eprintln!("nntp-rs: mlock: resize {}: {}", path, e);
                    mem_lock.take();
                    file.take();
                }

                // loop.
                continue;
            }

            // file was changed.
            mem_lock.take();
            file.take();
        }

        // re-open file.
        let new_file = match fs::File::open(path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("nntp-rs: mlock: open {}: {}", path, e);
                continue;
            }
        };

        // mlock new file.
        let new_mlock = match MemLock::new(&new_file) {
            Ok(mlock) => mlock,
            Err(e) => {
                eprintln!("nntp-rs: mlock: {}: {}", path, e);
                continue;
            }
        };
        if let Err(e) = new_mlock.mlock() {
            eprintln!("nntp-rs: mlock: {}: {}", path, e);
            continue;
        }

        file.replace(new_file);
        mem_lock.replace(new_mlock);
    }
}

