//! A simple message bus.
//!
//! The messages are simple enums, they cannot contain values.
//! It's pretty efficient though. We use an mpsc::watcher broadcast
//! of a state structure with generation counters. Then every
//! receiver checks if their internal counter < the new counter
//! and if so, generates that value.
//!
//! This prevents blocking and lost updates.
use std::default::Default;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::signal::unix::{signal, SignalKind};
use tokio::stream::{Stream, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::task;

#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub enum Notification {
    ExitGraceful,
    ExitNow,
    Reconfigure,
    Expire,
    HostCacheUpdate,
}

impl From<u32> for Notification {
    fn from(i: u32) -> Notification {
        match i {
            0 => Notification::ExitGraceful,
            1 => Notification::ExitNow,
            2 => Notification::Reconfigure,
            3 => Notification::Expire,
            4 => Notification::HostCacheUpdate,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Default)]
struct State(Vec<u64>);

/// Send messages on the bus.
#[derive(Clone)]
pub struct Sender {
    tx: mpsc::UnboundedSender<Notification>,
}

impl Sender {
    /// Send a message on the bus to all listeners.
    pub fn send(&mut self, n: Notification) -> Result<(), ()> {
        self.tx.send(n).map_err(|_| ())
    }
}

/// Receive messages on the bus.
pub struct Receiver {
    state:  State,
    nstate: State,
    subs:   u64,
    rx:     watch::Receiver<State>,
}

impl Receiver {
    fn next_item(&mut self) -> Option<Notification> {
        if self.state.0.len() < self.nstate.0.len() {
            self.state.0.resize(self.nstate.0.len(), 0);
        }
        for i in 0..self.state.0.len() {
            if self.state.0[i] < self.nstate.0[i] {
                self.state.0[i] = self.nstate.0[i];
                if self.subs == 0 || (self.subs & (1 << i)) > 0 {
                    return Some((i as u32).into());
                }
            }
        }
        None
    }

    /// Receive a message from the bus.
    pub async fn recv(&mut self) -> Option<Notification> {
        loop {
            let next = self.next_item();
            if next.is_some() {
                return next;
            }
            self.nstate = match self.rx.recv().await {
                Some(s) => s,
                None => return None,
            };
        }
    }

    /// Subscribe to a message.
    ///
    /// If subscribed to no messages at all, you get all of them.
    pub fn subscribe(&mut self, n: Notification) {
        self.subs |= 1u64 << (n as u32);
    }

    /// Subscribe to all message.
    ///
    /// This is the same as subscribing to no messages.
    pub fn subscribe_all(&mut self) {
        self.subs = 0;
    }

    /// Unsubscribe from a message.
    pub fn unsubscribe(&mut self, n: Notification) {
        self.subs &= !(1u64 << (n as u32));
    }
}

impl Stream for Receiver {
    type Item = Notification;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(item) = self.as_mut().next_item() {
            return Poll::Ready(Some(item));
        }
        match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(state)) => {
                self.nstate = state;
                if let Some(item) = self.next_item() {
                    return Poll::Ready(Some(item));
                }
            },
            _ => {},
        }
        Poll::Pending
    }
}

impl Clone for Receiver {
    fn clone(&self) -> Receiver {
        let state = self.rx.borrow().clone();
        Receiver {
            nstate: state.clone(),
            state,
            rx: self.rx.clone(),
            subs: self.subs,
        }
    }
}

/// Set up a simple bus.
///
/// SIGINT/SIGTERM are forwarded on the bus as notifications.
pub fn new() -> (Sender, Receiver) {
    // tokio::watch::channel is SPMC. Front it with a MPSC channel
    // so that we have, in effect, an MPMC channel.
    let (notifier_master, watcher) = watch::channel(State::default());
    let (notifier, mut notifier_receiver) = mpsc::unbounded_channel::<Notification>();

    let sender = Sender { tx: notifier.clone() };
    let receiver = Receiver {
        state:  State::default(),
        nstate: State::default(),
        subs:   0,
        rx:     watcher,
    };

    // forward a message from the MPSC channel to all the watchers.
    task::spawn(async move {
        let mut state = State::default();
        while let Some(notification) = notifier_receiver.next().await {
            let i = notification as u32 as usize;
            if state.0.len() < i + 1 {
                state.0.resize(i + 1, 0);
            }
            state.0[i] += 1;
            if let Err(e) = notifier_master.broadcast(state.clone()) {
                log::error!("bus::broadcast_task: exit");
                break;
            }
        }
    });

    // Forward control-c
    let tx = notifier.clone();
    task::spawn(async move {
        let mut sig_int = signal(SignalKind::interrupt()).unwrap();
        while let Some(_) = sig_int.next().await {
            log::info!("received SIGINT");
            let _ = tx.send(Notification::ExitGraceful);
        }
    });

    // Forward SIGTERM
    let tx = notifier.clone();
    task::spawn(async move {
        let mut sig_term = signal(SignalKind::terminate()).unwrap();
        while let Some(_) = sig_term.next().await {
            log::info!("received SIGTERM");
            let _ = tx.send(Notification::ExitGraceful);
        }
    });

    // Forward SIGHUP
    let tx = notifier.clone();
    task::spawn(async move {
        let mut sig_hup = signal(SignalKind::hangup()).unwrap();
        while let Some(_) = sig_hup.next().await {
            log::info!("received SIGHUP");
            let _ = tx.send(Notification::Reconfigure);
        }
    });

    // Forward SIGUSR1
    let tx = notifier.clone();
    task::spawn(async move {
        let mut sig_usr1 = signal(SignalKind::user_defined1()).unwrap();
        while let Some(_) = sig_usr1.next().await {
            log::info!("received SIGUSR1");
            let _ = tx.send(Notification::Expire);
        }
    });

    (sender, receiver)
}
