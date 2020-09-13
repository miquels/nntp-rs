//! Implementation of a DelayQueue.
//!
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::stream::Stream;
use tokio::time::delay_for;

// max 10 seconds, resolution 500ms.
const NUM_SLOTS: usize = 20;
const TICKS_PER_SEC: u64 = 2;

/// Iterator returned by DelayQueue::drain()
pub struct Drain<'a, T> {
    queue:  &'a mut DelayQueue<T>,
    pos:    usize,
}

impl<'a, T: Unpin> Iterator for Drain<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if let Some(item) = self.queue.overflow.pop_front() {
            return Some(item);
        }
        while self.pos < NUM_SLOTS {
            let slot = (self.queue.head + self.pos) % NUM_SLOTS;
            if let Some(item) = self.queue.slots[slot].pop_front() {
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
    slots:    [VecDeque<T>; NUM_SLOTS],
    overflow: VecDeque<T>,
    head:     usize,
    size:     usize,
    cap:      usize,
    timer:     Option<tokio::time::Delay>,
}

impl<T: Unpin> DelayQueue<T> {
    /// Create a new bounded queue.
    pub fn with_capacity(cap: usize) -> DelayQueue<T> {
        DelayQueue {
            slots:  array_init::array_init(|_| VecDeque::<T>::new()),
            overflow: VecDeque::<T>::new(),
            head: 0,
            size: 0,
            cap,
            timer: None,
        }
    }

    /// Push an article onto the queue.
    ///
    /// If the queue is full, the item is  returned as an error.
    pub fn push(&mut self, item: T, delay: Duration) -> Result<(), T> {
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
        Drain { queue: self, pos: 0 }
    }

    // arm the timer. non-optimized version.
    fn arm_timer(&mut self) {
        if self.timer.is_none() {
            let d = Duration::from_millis(1000 / TICKS_PER_SEC + 1);
            self.timer = Some(delay_for(d));
        }
    }
}

impl<T> Stream for DelayQueue<T> where T: Unpin {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        let mut tick = false;

        // poll timer.
        if let Some(timer) = this.timer.as_mut() {
            futures::ready!(Pin::new(timer).poll(cx));
            tick = true;
        }

        // see if there's anything to do.
        if this.size == 0 {
            this.timer.take();
            return Poll::Pending;
        }

        // get next item, if any.
        let head = this.head;
        let item = if this.overflow.is_empty() && this.slots[this.head].len() < 2 {
            // no overflow, and current slot has 0 or 1 items.
            this.slots[head].pop_front()
        } else {
            // add current slot items to the back of the overflow queue
            // and return the front item.
            while let Some(item) = this.slots[head].pop_front() {
                this.overflow.push_back(item);
            }
            this.overflow.pop_front()
        };

        // if we got an item, adjust remaining size.
        if item.is_some() {
            this.size -= 1;
        }

        // the slot at head is now empty, so if the timer fired, advance head.
        if tick {
            this.head = (this.head + 1) % NUM_SLOTS;
            // if there are more items queued, rearm the timer. otherwise drop it.
            if this.size - this.overflow.len() > 0 {
                this.arm_timer();
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

