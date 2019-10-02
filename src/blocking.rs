use std::future::Future;
use std::io;

use futures::future;
use futures::future::FutureExt;
use tokio_executor::threadpool;

#[derive(Clone)]
pub(crate) struct BlockingPool;

impl BlockingPool {
    pub fn new(_num_threads: usize) -> BlockingPool {
        BlockingPool{}
    }

    pub fn spawn_fn<F, T>(&self, func: F) -> impl Future<Output = io::Result<T>>
    where
        F: FnOnce() -> io::Result<T>
    {
        let mut func = Some(func);
        future::poll_fn(move |_| threadpool::blocking(|| (func.take().unwrap())()))
            .then(|res| {
                match res {
                    Ok(x) => future::ready(x),
                    Err(_) => panic!("the thread pool has shut down"),
                }
            })
    }
}
/*
fn blocking_io<F, T>(f: F) -> Poll<io::Result<T>>
where
    F: FnOnce() -> io::Result<T>,
{
    match tokio_threadpool::blocking(f) {
        Ready(Ok(v)) => Ready(v),
        Ready(Err(_)) => Ready(Err(blocking_err())),
        Pending => Pending,
    }
}

async fn asyncify<F, T>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T>,
{
    use future::poll_fn;

    let mut f = Some(f);
    poll_fn(move |_| blocking_io(|| f.take().unwrap()())).await
}

fn blocking_err() -> io::Error {
    io::Error::new(
        Other,
        "`blocking` annotated I/O must be called \
         from the context of the Tokio runtime.",
    )
}
*/
