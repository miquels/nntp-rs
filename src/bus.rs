//! A simple message bus.
//!
//! Used to be more complex, is now just a wrapper around tokio::broadcast.
//!
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task;

use crate::config;

#[derive(Debug, Clone, Copy)]
pub enum Notification {
    ExitGraceful,
    ExitNow,
    Reconfigure,
    Expire,
    HostCacheUpdate,
}

/// Send messages on the bus.
#[derive(Clone)]
pub struct Sender {
    tx: broadcast::Sender<Notification>,
}

impl Sender {
    fn new(tx: broadcast::Sender<Notification>) -> Sender {
        Sender { tx }
    }

    /// Send a message on the bus to all listeners.
    pub fn send(&mut self, n: Notification) -> Result<(), ()> {
        self.tx.send(n).map(|_| ()).map_err(|_| ())
    }
}

/// Receive messages on the bus.
pub struct Receiver {
    tx:     broadcast::Sender<Notification>,
    rx:     broadcast::Receiver<Notification>,
}

impl Receiver {
    fn new(tx: broadcast::Sender<Notification>, rx: broadcast::Receiver<Notification>) -> Receiver {
        Receiver{ tx, rx }
    }

    /// Receive a message from the bus.
    pub async fn recv(&mut self) -> Option<Notification> {
        loop {
            match self.rx.recv().await {
                Ok(item) => return Some(item),
                Err(RecvError::Closed) => return None,
                Err(RecvError::Lagged(n)) => {
                    log::warn!("Receiver::recv: lagged {}", n);
                },
            }
        }
    }
}

impl Clone for Receiver {
    fn clone(&self) -> Self {
        Receiver {
            tx: self.tx.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

/// Set up a simple bus.
///
/// SIGINT/SIGTERM are forwarded on the bus as notifications.
pub fn new() -> (Sender, Receiver) {
    let (tx1, rx1) = broadcast::channel(32);

    // Forward control-c
    let tx = tx1.clone();
    task::spawn(async move {
        let mut sig_int = signal(SignalKind::interrupt()).unwrap();
        while let Some(_) = sig_int.recv().await {
            log::info!("received SIGINT");
            let _ = tx.send(Notification::ExitGraceful);
        }
    });

    // Forward SIGTERM
    let tx = tx1.clone();
    task::spawn(async move {
        let mut sig_term = signal(SignalKind::terminate()).unwrap();
        while let Some(_) = sig_term.recv().await {
            log::info!("received SIGTERM");
            let _ = tx.send(Notification::ExitGraceful);
        }
    });

    // Forward SIGHUP
    let tx = tx1.clone();
    task::spawn(async move {
        let mut sig_hup = signal(SignalKind::hangup()).unwrap();
        while let Some(_) = sig_hup.recv().await {
            log::info!("received SIGHUP");
            match config::reread_config() {
                Ok(false) => log::info!("configuration unchanged"),
                Ok(true) => {
                    log::info!("loaded new configuration from disk");
                    let _ = tx.send(Notification::Reconfigure);
                },
                Err(e) => log::error!("{}", e),
            }
        }
    });

    // Forward SIGUSR1
    let tx = tx1.clone();
    task::spawn(async move {
        let mut sig_usr1 = signal(SignalKind::user_defined1()).unwrap();
        while let Some(_) = sig_usr1.recv().await {
            log::info!("received SIGUSR1");
            let _ = tx.send(Notification::Expire);
        }
    });

    (Sender::new(tx1.clone()), Receiver::new(tx1, rx1))
}
