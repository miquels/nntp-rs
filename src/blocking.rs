use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future;
use tokio_executor::threadpool;
use tokio_sync::semaphore::{Semaphore, Permit};

#[derive(Clone)]
pub(crate) struct BlockingPool {
    inner:  Arc<InnerBlockingPool>,
}

pub(crate) struct InnerBlockingPool {
    use_pool:   AtomicUsize,
    sem:        Semaphore,
}

impl BlockingPool {
    pub fn new(max_threads: usize) -> BlockingPool {
        let t = if max_threads == 0 { 128 } else { max_threads };
        BlockingPool {
            inner:  Arc::new(
                InnerBlockingPool {
                    use_pool:   AtomicUsize::new(42),
                    sem:        Semaphore::new(t),
                }
            )
        }
    }

    pub async fn spawn_fn<F, T>(&self, func: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let use_pool = match self.inner.use_pool.load(Ordering::Relaxed)  {
            0 => false,
            1 => true,
            _ => {
                match threadpool::blocking(|| { 42 }) {
                    std::task::Poll::Ready(Err(_)) => {
                        // cannot use tokio_executor::threadpool::blocking (which does NOT
                        // run the closure on a threadpool), so use tokio_executor::blocking::run
                        // which DOES run it on a threadpool.
                        self.inner.use_pool.store(1, Ordering::SeqCst);
                        true
                    },
                    _ => {
                        self.inner.use_pool.store(0, Ordering::SeqCst);
                        false
                    },
                }
            }
        };

        let mut permit = Permit::new();
        if future::poll_fn(|cx| permit.poll_acquire(cx, &self.inner.sem)).await.is_err() {
            panic!("BlockingPool::spawn_fn: poll_acquire() returned error (cannot happen)");
        }

        let res = if use_pool {
            tokio_executor::blocking::run(move || func()).await
        } else {
            let mut func = Some(func);
            let r = future::poll_fn(move |_| threadpool::blocking(|| (func.take().unwrap())())).await;
            match r {
                Ok(x) => x,
                Err(_) => panic!("the thread pool has shut down"),
            }
        };

        permit.release(&self.inner.sem);
        res
    }
}
