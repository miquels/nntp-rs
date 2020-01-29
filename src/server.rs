use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::TcpListener;
use std::os::unix::io::AsRawFd;
use std::process::exit;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc};
use std::thread;
use std::time::Duration;

use num_cpus;
use parking_lot::Mutex;
use tokio;
use tokio::stream::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio::signal::unix::{signal, SignalKind};

use crate::util::bind_socket;
use crate::config;
use crate::diag::SessionStats;
use crate::history::History;
use crate::logger;
use crate::nntp_codec::{self, NntpCodec};
use crate::nntp_session::NntpSession;
use crate::spool::Spool;

#[derive(Clone)]
pub struct Server {
    pub history:      History,
    pub spool:        Spool,
    pub conns:        Arc<Mutex<HashMap<String, usize>>>,
    pub tot_sessions: Arc<AtomicU64>,
    pub thr_sessions: Arc<AtomicU64>,
}

impl Server {
    /// Create a new Server.
    pub fn new(history: History, spool: Spool) -> Server {
        Server {
            history,
            spool,
            conns: Arc::new(Mutex::new(HashMap::new())),
            tot_sessions: Arc::new(AtomicU64::new(0)),
            thr_sessions: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Run the server on a bunch of current_thread executors.
    pub fn run_multisingle(self, listeners: Vec<TcpListener>) -> io::Result<()> {
        trace!("main server running on thread {:?}", thread::current().id());
        let config = config::get_config();

        // See how many threads we want to start and on what cores.
        let mut threads = Vec::new();
        let (num_threads, mut core_ids) = match config.multisingle.core_ids {
            Some(ref c) => (c.len(), c.to_vec()),
            None => (num_cpus::get(), Vec::new()),
        };

        // copy addresses of the listening sockets, then push the first
        // listener-set on to the listener_sets vector.
        let addrs = listeners
            .iter()
            .map(|l| l.local_addr().unwrap())
            .collect::<Vec<_>>();
        let mut listener_sets = Vec::new();
        listener_sets.push(listeners);

        // create one listener-set per thread, in a seperate setuid-root scope.
        {
            // try to switch to root. if we fail, that's ok. either the socket
            // will bind, or not, and if not, we error on that.
            let egid = users::get_effective_gid();
            let _guard = match users::switch::switch_user_group(0, egid) {
                Ok(g) => Some(g),
                Err(_) => None,
            };

            // create a listener-set per thread.
            for _ in 1..num_threads {
                let mut v = Vec::new();
                for addr in &addrs {
                    let l = bind_socket(&addr)
                        .map_err(|e| {
                            eprintln!("nntp-rs: server: fatal: {}", e);
                            exit(1);
                        })
                        .unwrap();
                    v.push(l);
                }
                listener_sets.push(v);
            }
        }

        let (notifier, watcher, task) = self.notification_setup();
        threads.push(self.notification_thread(notifier.clone(), watcher.clone(), task));

        for listener_set in listener_sets.into_iter() {
            let mut server = self.clone();
            server.thr_sessions = Arc::new(AtomicU64::new(0));

            let core_id = if core_ids.len() > 0 {
                Some(core_ids.remove(0))
            } else {
                None
            };

            let mut watcher = watcher.clone();

            let tid = thread::spawn(move || {
                if let Some(id) = core_id {
                    // tie this thread to a specific core.
                    core_affinity::set_for_current(id);
                }

                // tokio runtime for this thread alone.
                let mut runtime = tokio::runtime::Builder::new()
                    .basic_scheduler()
                    .enable_all()
                    .build()
                    .unwrap();
                trace!("runtime::basic_scheduler on {:?}", thread::current().id());

                runtime.block_on(async move {

                    for listener in listener_set.into_iter() {
                        let listener = tokio::net::TcpListener::from_std(listener)
                            .expect("cannot convert from net2 listener to tokio listener");
                        tokio::spawn(server.clone().run(listener, watcher.clone()));
                    }

                    // wait for a shutdown notification
                    while let Some(notification) = watcher.next().await {
                        match notification {
                            Notification::ExitGraceful | Notification::ExitNow => break,
                            _ => {},
                        }
                    }

                    // now busy-wait until all connections are closed.
                    while server.thr_sessions.load(Ordering::SeqCst) > 0 {
                        let _ = tokio::time::delay_for(Duration::from_millis(100)).await;
                    }
                })
            });

            threads.push(tid);
        }

        // and wait for the threads to come home
        for t in threads.into_iter() {
            let _ = t.join();
        }
        Ok(())
    }

    // run the server on the default threaded executor.
    pub fn run_threaded(&mut self, listeners: Vec<TcpListener>) -> io::Result<()> {
        let mut runtime = tokio::runtime::Runtime::new()?;

        runtime.block_on(async move {
            let (notifier, watcher, task) = self.notification_setup();
            tokio::spawn(task);

            for listener in listeners.into_iter() {
                let listener = tokio::net::TcpListener::from_std(listener)
                    .expect("cannot convert from net2 listener to tokio listener");
                tokio::spawn(self.clone().run(listener, watcher.clone()));
            }
            self.wait(notifier, watcher).await;
        });

        Ok(())
    }

    // Set up a notification channel, and forward SIGINT/SIGTERM as notifications.
    fn notification_setup(
        &self,
    ) -> (
        mpsc::Sender<Notification>,
        watch::Receiver<Notification>,
        impl Future<Output = ()>,
    ) {
        // tokio::watch::channel is SPMC, so front it with a MPSC channel
        // so that we have, in effect, a MPMC channel.
        let (notifier_master, watcher) = watch::channel(Notification::None);
        let (notifier, mut notifier_receiver) = mpsc::channel::<Notification>(16);

        let notifier2 = notifier.clone();
        let watcher2 = watcher.clone();
        let server = self.clone();

        let task = async move {
            tokio::spawn(async move {
                while let Some(notification) = notifier_receiver.next().await {
                    if let Err(_) = notifier_master.broadcast(notification) {
                        break;
                    }
                }
            });

            // Forward control-c
            let mut tx1 = notifier.clone();
            tokio::spawn(async move {
                let mut sig_int = signal(SignalKind::interrupt()).unwrap();
                while let Some(_) = sig_int.next().await {
                    info!("received SIGINT");
                    let _ = tx1.send(Notification::ExitGraceful).await;
                }
            });

            // Forward SIGTERM
            let mut tx2 = notifier.clone();
            tokio::spawn(async move {
                let mut sig_term = signal(SignalKind::terminate()).unwrap();
                while let Some(_) = sig_term.next().await {
                    info!("received SIGTERM");
                    let _ = tx2.send(Notification::ExitGraceful).await;
                }
            });

            // Catch SIGUSR1
            let server = server.clone();
            tokio::spawn(async move {
                let mut sig_term = signal(SignalKind::user_defined1()).unwrap();
                while let Some(_) = sig_term.next().await {
                    info!("received USR1");
                    let config = config::get_config();
                    if let Err(e) = server
                        .history
                        .expire(&server.spool, config.history.remember.clone(), false, true)
                        .await
                    {
                        error!("expire: {}", e);
                    }
                }
            });
        };

        (notifier2, watcher2, task)
    }

    // The "multisingle" runtime needs to run the notification task and server.wait()
    // on a separate runtime.
    fn notification_thread<F>(
        &self,
        notifier: mpsc::Sender<Notification>,
        watcher: watch::Receiver<Notification>,
        task: F,
    ) -> thread::JoinHandle<()>
    where
        F: Future<Output = ()> + 'static + Send,
    {
        let server = self.clone();
        thread::spawn(move || {
            let mut runtime = tokio::runtime::Builder::new()
                .basic_scheduler()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                tokio::spawn(task);
                server.wait(notifier, watcher).await;
            });
        })
    }

    // wait for all sessions to finish.
    async fn wait(
        &self,
        mut notifier: mpsc::Sender<Notification>,
        mut watcher: watch::Receiver<Notification>,
    )
    {
        // wait for a shutdown notification
        let mut waited = 0u32;
        while let Some(notification) = watcher.next().await {
            match notification {
                Notification::ExitGraceful => {
                    info!("received Notification::ExitGraceful");
                    break;
                },
                Notification::ExitNow => {
                    waited = 51;
                    break;
                },
                _ => {},
            }
        }

        // now busy-wait until all connections are closed.
        while self.tot_sessions.load(Ordering::SeqCst) > 0 {
            if waited == 50 {
                info!("sending Notification::ExitNow to all remaining sessions");
                let _ = notifier.send(Notification::ExitNow).await;
            }
            waited += 1;
            let _ = tokio::time::delay_for(Duration::from_millis(100)).await;
            if waited == 100 || waited % 600 == 0 {
                warn!("still waiting!");
            }
        }

        let incoming_logger = logger::get_incoming_logger();
        incoming_logger.quit();

        info!("exiting.");
        logger::logger_flush();
    }

    // This is run for every TCP listener socket.
    async fn run(self, mut listener: tokio::net::TcpListener, watcher: watch::Receiver<Notification>) {
        use futures::future::Either;
        use futures::stream;

        // We have two streams. One, a stream of incoming connections.
        // Two, a stream of notifications. Combine them.
        let incoming = listener.incoming().map(|s| Either::Left(s));
        let watcher2 = watcher.clone().map(|w| Either::Right(w));
        let mut items = stream::select(incoming, watcher2);

        // Now iterate over the combined stream.
        while let Some(item) = items.next().await {
            let socket = match item {
                Either::Left(Ok(s)) => s,
                Either::Left(Err(_)) => {
                    tokio::time::delay_for(Duration::from_millis(50)).await;
                    continue;
                },
                Either::Right(Notification::ExitGraceful) => break,
                Either::Right(Notification::ExitNow) => break,
                _ => continue,
            };

            // set up codec for reader and writer.
            let peer = socket.peer_addr().unwrap_or("0.0.0.0:0".parse().unwrap());
            let fdno = socket.as_raw_fd() as u32;
            let codec = NntpCodec::builder(socket)
                .watcher(watcher.clone())
                .read_timeout(nntp_codec::READ_TIMEOUT)
                .write_timeout(nntp_codec::WRITE_TIMEOUT)
                .build();

            let stats = SessionStats {
                hostname: peer.to_string(),
                ipaddr: peer.to_string(),
                label: "unknown".to_string(),
                fdno: fdno,
                ..SessionStats::default()
            };

            // build and run an nntp session.
            let session = NntpSession::new(peer, codec, self.clone(), stats);
            tokio::spawn(session.run());
        }
    }

    /// Increment the connection counter for this peer, and return the new value.
    pub fn add_connection(&self, peername: &str) -> usize {
        let mut conns = self.conns.lock();
        let c = match conns.get_mut(peername) {
            Some(val) => {
                *val += 1;
                *val
            },
            None => 1,
        };
        if c == 1 {
            conns.insert(peername.to_string(), c);
        }
        c
    }

    /// Decrement the connection counter for this peer.
    pub fn remove_connection(&self, peername: &str) {
        let mut conns = self.conns.lock();
        let c = match conns.get_mut(peername) {
            Some(val) => {
                *val -= 1;
                *val
            },
            None => 0,
        };
        if c == 0 {
            conns.remove(peername);
        }
    }
}

#[derive(Debug, Clone)]
pub enum Notification {
    ExitGraceful,
    ExitNow,
    None,
}
