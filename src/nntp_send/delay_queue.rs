//! Implementation of a DelayQueue.
//!
use std::cmp;
use std::collections::{BinaryHeap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::time::{sleep_until, Sleep};
use tokio::time::{Duration, Instant};
use tokio_stream::Stream;

struct DelayItem<T> {
    when:   Instant,
    item:   T,
}

// Reverse ordering.
impl<T> cmp::Ord for DelayItem<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        other.when.cmp(&self.when)
    }
}

impl<T> cmp::PartialOrd for DelayItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> cmp::PartialEq for DelayItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
    }
}
impl<T> cmp::Eq for DelayItem<T> {}

/// Our own version of DelayQueue.
///
/// The main difference is that this version can be drained.
pub struct DelayQueue<T> {
    queue:            BinaryHeap<DelayItem<T>>,
    ready_items:      VecDeque<T>,
    cap:              usize,
    pub(crate) batch: Duration,
    timer:            Option<Pin<Box<Sleep>>>,
}

impl<T: Unpin> DelayQueue<T> {
    /// Create a new bounded queue.
    pub fn with_capacity(cap: usize) -> DelayQueue<T> {
        DelayQueue {
            queue: BinaryHeap::new(),
            ready_items: VecDeque::new(),
            cap,
            batch: Duration::from_millis(500),
            timer: None,
        }
    }

    /// Insert an article in the queue.
    ///
    /// If the queue is full, the item is  returned as an error.
    pub fn insert(&mut self, item: T, delay: Duration) -> Result<(), T> {
        if self.queue.len() == self.cap {
            return Err(item);
        }

        let when = Instant::now() + delay;
        self.queue.push(DelayItem{ when, item });

        if self.queue.peek().map(|first| when < first.when).unwrap_or(true) {
            // empty queue, or "when" is earlier than "first.when". set timer.
            self.arm_timer(when);
        }
        Ok(())
    }

    /// Queue length.
    pub fn len(&self) -> usize {
        self.ready_items.len()+ self.queue.len()
    }

    /// Drain the queue.
    pub fn drain(&mut self) -> Drain<'_, T> {
        Drain {
            dq: self,
        }
    }

    // arm or re-arm the timer.
    fn arm_timer(&mut self, when: Instant) {
        if let Some(ref mut timer) = self.timer {
            timer.as_mut().reset(when);
        } else {
            self.timer = Some(Box::pin(sleep_until(when)));
        }
    }
}

impl<T> Stream for DelayQueue<T>
where T: Unpin
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // return ready_items first.
        if let Some(item) = this.ready_items.pop_front() {
            return Poll::Ready(Some(item));
        }

        if this.queue.len() == 0 {
            return Poll::Pending;
        }

        // We batch up the next 0.5 seconds worth of items.
        let now = Instant::now();
        let max = now + this.batch;

        while let Some(qitem) = this.queue.peek() {
            if qitem.when < max {
                let qitem = this.queue.pop().unwrap();
                this.ready_items.push_back(qitem.item);
            } else {
                break;
            }
        }

        // Do we have an item ready to go?
        if let Some(item) = this.ready_items.pop_front() {
            return Poll::Ready(Some(item));
        }

        // re-arm timer.
        let some_when = this.queue.peek().map(|item| item.when);
        if let Some(when) = some_when {
            this.arm_timer(when);
            return Poll::Pending;
        }


        Poll::Pending
    }
}

/// Iterator returned by DelayQueue::into_iter()
pub struct Drain<'a, T> {
    dq: &'a mut DelayQueue<T>,
}

impl<'a, T: Unpin> Drain<'a, T> {
    pub fn len(&self) -> usize {
        self.dq.len()
    }
}

impl<'a, T: Unpin> Iterator for Drain<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if let Some(item) = self.dq.ready_items.pop_front() {
            Some(item)
        } else {
            self.dq.queue.pop().map(|d| d.item)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DelayQueue;
    use tokio::time::{Duration, Instant};
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test() {
        let mut dq = DelayQueue::with_capacity(100);
        let start = Instant::now();
        dq.batch = Duration::from_millis(5);
        dq.insert(4u32, Duration::from_millis(80)).unwrap();
        dq.insert(5u32, Duration::from_millis(100)).unwrap();
        dq.insert(1u32, Duration::from_millis(20)).unwrap();
        dq.insert(3u32, Duration::from_millis(60)).unwrap();
        dq.insert(2u32, Duration::from_millis(40)).unwrap();
        assert!(dq.next().await.unwrap() == 1);
        assert!(start.elapsed() >= Duration::from_millis(20));
        assert!(dq.next().await.unwrap() == 2);
        assert!(start.elapsed() >= Duration::from_millis(40));
        assert!(dq.next().await.unwrap() == 3);
        assert!(start.elapsed() >= Duration::from_millis(60));
        assert!(dq.next().await.unwrap() == 4);
        assert!(start.elapsed() >= Duration::from_millis(80));
        assert!(dq.next().await.unwrap() == 5);
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
}

