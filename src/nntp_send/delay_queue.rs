//! Implementation of a DelayQueue.
//!
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::time::{sleep, Sleep};
use tokio::time::{Duration, Instant};
use tokio_stream::Stream;

// max 10 seconds, resolution 500ms.
const NUM_SLOTS: usize = 20;
const TICKS_PER_SEC: u64 = 2;

/// Iterator returned by DelayQueue::drain()
pub struct Drain<'a, T> {
    queue: &'a mut DelayQueue<T>,
    pos:   usize,
}

impl<'a, T: Unpin> Drain<'a, T> {
    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

impl<'a, T: Unpin> Iterator for Drain<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if let Some(item) = self.queue.ready_items.pop_front() {
            self.queue.size -= 1;
            return Some(item);
        }
        while self.pos < NUM_SLOTS {
            let slot = (self.queue.head + self.pos) % NUM_SLOTS;
            if let Some(item) = self.queue.slots[slot].pop_front() {
                self.queue.size -= 1;
                return Some(item);
            }
            self.pos += 1;
        }
        None
    }
}

/// Our own version of DelayQueue.
///
/// The main difference is that this version can be drained.
/// The other difference is that this version is very coarse,
/// and if it's not polled often it get completely inaccurate.
///
/// But that's OK, we just want "delay this a bit" in general
/// and we won't fuss over accuracy.
pub struct DelayQueue<T> {
    slots:       [VecDeque<T>; NUM_SLOTS],
    ready_items: VecDeque<T>,
    head:        usize,
    size:        usize,
    cap:         usize,
    timer:       Option<Pin<Box<Sleep>>>,
}

impl<T: Unpin> DelayQueue<T> {
    /// Create a new bounded queue.
    pub fn with_capacity(cap: usize) -> DelayQueue<T> {
        DelayQueue {
            slots: array_init::array_init(|_| VecDeque::<T>::new()),
            ready_items: VecDeque::<T>::new(),
            head: 0,
            size: 0,
            cap,
            timer: None,
        }
    }

    /// Insert an article in the queue.
    ///
    /// If the queue is full, the item is  returned as an error.
    pub fn insert(&mut self, item: T, delay: Duration) -> Result<(), T> {
        if self.size == self.cap {
            return Err(item);
        }
        self.size += 1;

        // calculate slot.
        let slot = std::cmp::max(delay.as_secs(), 1) as usize * TICKS_PER_SEC as usize;
        let slot = (self.head + std::cmp::min(slot, NUM_SLOTS - 1)) % NUM_SLOTS;
        self.slots[slot].push_back(item);
        if self.timer.is_none() {
            self.arm_timer();
        }
        Ok(())
    }

    /// Queue length.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Drain the queue.
    pub fn drain(&mut self) -> Drain<'_, T> {
        Drain {
            queue: self,
            pos:   0,
        }
    }

    // arm or re-arm the timer.
    fn arm_timer(&mut self) {
        let d = Duration::from_millis(1000 / TICKS_PER_SEC + 1);
        if let Some(timer) = self.timer.as_mut() {
            // second time as_mut() is to get at the Future.
            timer.as_mut().reset(Instant::now() + d);
        } else {
            self.timer = Some(Box::pin(sleep(d)));
        }
    }
}

impl<T> Stream for DelayQueue<T>
where T: Unpin
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        let mut tick = false;

        // return ready_items first.
        if let Some(item) = this.ready_items.pop_front() {
            this.size -= 1;
            return Poll::Ready(Some(item));
        }

        // poll timer.
        if let Some(timer) = this.timer.take() {
            tokio::pin!(timer);
            //futures::ready!(timer.as_mut().poll(cx));
            //futures::ready!(Pin::new(timer).poll(cx));
            futures::ready!(timer.poll(cx));
            tick = true;
        }

        // see if there's anything to do.
        if this.size == 0 {
            this.timer.take();
            return Poll::Pending;
        }

        // get next item, if any.
        let head = this.head;
        let item = if this.slots[head].len() < 2 {
            // current slot has 0 or 1 items.
            this.slots[head].pop_front()
        } else {
            // add current slot items to the back of the ready_items queue
            // and return the front item.
            while let Some(item) = this.slots[head].pop_front() {
                this.ready_items.push_back(item);
            }
            this.ready_items.pop_front()
        };
        if item.is_some() {
            this.size -= 1;
        }

        // the slot at head is now empty, so if the timer fired, advance head.
        if tick {
            this.head = (this.head + 1) % NUM_SLOTS;
            // if there are more items queued, rearm the timer. otherwise drop it.
            if this.size - this.ready_items.len() > 0 {
                this.arm_timer();
                // The timer needs to be polled at least once. If we're going to
                // return Poll::Pending, that will not happen, so do it now.
                if item.is_none() {
                    let _ = Pin::new(this.timer.as_mut().unwrap()).poll(cx);
                }
            } else {
                this.timer.take();
            }
        }

        if item.is_some() {
            Poll::Ready(item)
        } else {
            Poll::Pending
        }
    }
}
