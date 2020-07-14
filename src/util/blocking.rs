//! A `blocking pool` is a limited set of outstanding `blocking` futures.
//! 
//! Is useful if you have several types of blocking calls, and you want
//! to group and limit them. For example, the default tokio threadpool can
//! have up to ~500 outstanding requests (threads), and we want to
//! partition that into 3 pools (for spool lookup, history lookup,
//! queue file management) of max. 200, 200 and 100 requests.
//!
//! Each pool can have it's own blocking strategy:
//!
//! - ThreadPool: using `task::spawn_blocking`.
//! - InPlace: using `task::block_in_place`.
//! - Blocking: just call the blocking function directly and actually block.
//!
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task;

#[derive(Clone, Debug)]
pub(crate) struct BlockingPool {
    inner: Arc<InnerBlockingPool>,
}

#[derive(Debug)]
pub(crate) struct InnerBlockingPool {
    blocking_type: BlockingType,
    sem:           Semaphore,
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
