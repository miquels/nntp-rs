use std::sync::Arc;

use tokio::task;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub(crate) struct BlockingPool {
    inner: Arc<InnerBlockingPool>,
}

#[derive(Debug)]
pub(crate) struct InnerBlockingPool {
    blocking_type: BlockingType,
    sem:         Semaphore,
}

#[derive(Clone, Debug)]
pub enum BlockingType {
    InPlace,
    ThreadPool,
    Blocking,
}

#[allow(non_upper_case_globals)]
impl BlockingPool {
    pub fn new(btype: Option<BlockingType>, max_threads: usize) -> BlockingPool {

        // max nr of blocking threads.
        let max_t = if max_threads == 0 { 128 } else { max_threads };
        // method of handling blocking calls.
        let bt = btype.unwrap_or(BlockingType::ThreadPool);

        BlockingPool {
            inner: Arc::new(InnerBlockingPool {
                blocking_type: bt,
                sem:           Semaphore::new(max_t),
            }),
        }
    }

    pub async fn spawn_fn<F, T>(&self, func: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // limit the number of outstanding requests.
        let _guard = self.inner.sem.acquire().await;

        match self.inner.blocking_type {
            BlockingType::ThreadPool => task::spawn_blocking(func).await.unwrap(),
            BlockingType::InPlace => task::block_in_place(func),
            BlockingType::Blocking => func(),
        }
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
