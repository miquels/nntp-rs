pub enum TrySendError<T> {
    Full(T),
    Closed(T),
}

pub enum TryRecvError {
    Empty,
    Closed,
}

#[cfg(feature = "async-channel")]
#[allow(dead_code)]
mod mpmc_internal {
    use super::{TryRecvError, TrySendError};

    pub type Receiver<T> = async_channel::Receiver<T>;
    pub type Sender<T> = async_channel::Sender<T>;
    pub type RecvError = async_channel::RecvError;

    impl<T> From<async_channel::TrySendError<T>> for TrySendError<T> {
        fn from(t: async_channel::TrySendError<T>) -> TrySendError<T> {
            match t {
                async_channel::TrySendError::Full(t) => TrySendError::Full(t),
                async_channel::TrySendError::Closed(t) => TrySendError::Closed(t),
            }
        }
    }

    impl From<async_channel::TryRecvError> for TryRecvError {
        fn from(t: async_channel::TryRecvError) -> TryRecvError {
            match t {
                async_channel::TryRecvError::Empty => TryRecvError::Empty,
                async_channel::TryRecvError::Closed => TryRecvError::Closed,
            }
        }
    }

    pub fn bounded<T: Unpin>(size: usize) -> (Sender<T>, Receiver<T>) {
        async_channel::bounded::<T>(size)
    }
}

#[cfg(feature = "crossfire")]
#[allow(dead_code)]
mod mpmc_internal {
    use super::{TryRecvError, TrySendError};

    pub type Receiver<T> = crossfire::mpmc::RxFuture<T, crossfire::mpmc::SharedFutureBoth>;
    pub type Sender<T> = crossfire::mpmc::TxFuture<T, crossfire::mpmc::SharedFutureBoth>;
    pub type RecvError = crossfire::mpsc::RecvError;

    impl<T> From<crossfire::mpmc::TrySendError<T>> for TrySendError<T> {
        fn from(t: crossfire::mpmc::TrySendError<T>) -> TrySendError<T> {
            match t {
                crossfire::mpmc::TrySendError::Full(t) => TrySendError::Full(t),
                crossfire::mpmc::TrySendError::Disconnected(t) => TrySendError::Closed(t),
            }
        }
    }

    impl From<crossfire::mpmc::TryRecvError> for TryRecvError {
        fn from(t: crossfire::mpmc::TryRecvError) -> TryRecvError {
            match t {
                crossfire::mpmc::TryRecvError::Empty => TryRecvError::Empty,
                crossfire::mpmc::TryRecvError::Disconnected => TryRecvError::Closed,
            }
        }
    }

    pub fn bounded<T: Unpin>(size: usize) -> (Sender<T>, Receiver<T>) {
        crossfire::mpmc::bounded_future_both::<T>(size)
    }
}

pub use self::mpmc_internal::*;
