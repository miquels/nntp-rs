use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future;
use tokio_executor::threadpool;
use tokio_sync::semaphore::{Permit, Semaphore};

#[derive(Clone, Debug)]
pub(crate) struct BlockingPool {
    inner: Arc<InnerBlockingPool>,
}

#[derive(Debug)]
pub(crate) struct InnerBlockingPool {
    use_ownpool: AtomicUsize,
    sem:         Semaphore,
}

#[derive(Clone, Debug)]
pub enum BlockingType {
    OwnPool      = 1,
    SeparatePool = 2,
    Check        = 42,
}

#[allow(non_upper_case_globals)]
const OwnPool: usize = BlockingType::OwnPool as usize;
#[allow(non_upper_case_globals)]
const SeparatePool: usize = BlockingType::SeparatePool as usize;
#[allow(non_upper_case_globals)]
const Check: usize = BlockingType::Check as usize;

#[allow(non_upper_case_globals)]
impl BlockingPool {
    pub fn new(btype: Option<BlockingType>, max_threads: usize) -> BlockingPool {
        // max nr of blocking threads.
        let max_t = if max_threads == 0 { 128 } else { max_threads };

        // method of handling blocking calls.
        let bt = match btype {
            Some(BlockingType::OwnPool) => BlockingType::Check,
            Some(BlockingType::SeparatePool) => BlockingType::SeparatePool,
            Some(BlockingType::Check) | None => BlockingType::Check,
        };

        println!(
            "XXX building new pool blocking_type: {:?}, max_threads: {}",
            bt, max_threads
        );
        BlockingPool {
            inner: Arc::new(InnerBlockingPool {
                use_ownpool: AtomicUsize::new(bt as usize),
                sem:         Semaphore::new(max_t),
            }),
        }
    }

    pub async fn spawn_fn<F, T>(&self, func: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let use_ownpool = match self.inner.use_ownpool.load(Ordering::Relaxed) {
            OwnPool => true,
            SeparatePool => false,
            Check | _ => {
                match threadpool::blocking(|| 42) {
                    std::task::Poll::Ready(Err(_)) => {
                        // cannot use tokio_executor::threadpool::blocking (which does NOT
                        // run the closure on a threadpool), so use tokio_executor::blocking::run
                        // which DOES run it on a threadpool.
                        self.inner.use_ownpool.store(SeparatePool, Ordering::SeqCst);
                        false
                    },
                    _ => {
                        self.inner.use_ownpool.store(OwnPool, Ordering::SeqCst);
                        true
                    },
                }
            },
        };

        let mut permit = Permit::new();
        if future::poll_fn(|cx| permit.poll_acquire(cx, &self.inner.sem))
            .await
            .is_err()
        {
            panic!("BlockingPool::spawn_fn: poll_acquire() returned error (cannot happen)");
        }

        let res = if !use_ownpool {
            tokio_executor::blocking::run(move || func()).await
        } else {
            let mut func = Some(func);
            let r = future::poll_fn(move |_| threadpool::blocking(|| (func.take().unwrap())())).await;
            match r {
                Ok(x) => x,
                Err(_) => {
                    permit.release(&self.inner.sem);
                    panic!("the thread pool has shut down");
                },
            }
        };

        permit.release(&self.inner.sem);
        res
    }
}

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
    use std::io;

    pub fn try_read_at(_file: &File, _buf: &mut [u8], _offset: u64) -> Result<usize> {
        Err(io::ErrorKind::InvalidInput)?
    }
}

pub use try_read_at::*;
