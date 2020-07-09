use std::collections::HashMap;
use std::io;
use std::net::TcpListener;
use std::os::unix::io::AsRawFd;
use std::process::exit;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::runtime::{self, Runtime};
use tokio::stream::StreamExt;
use tokio::task;

use crate::bus::{self, Notification};
use crate::config;
use crate::diag::SessionStats;
use crate::history::History;
use crate::hostcache::HostCache;
use crate::logger;
use crate::nntp_codec::{self, NntpCodec};
use crate::nntp_recv::NntpReceiver;
use crate::spool::Spool;
use crate::util::TcpListenerSets;

#[derive(Clone)]
pub struct Server {
    pub history:      History,
    pub spool:        Spool,
    pub conns:        Arc<Mutex<HashMap<String, usize>>>,
    pub tot_sessions: Arc<AtomicU64>,
}

impl Server {
    /// Create a new Server.
    pub fn new(history: History, spool: Spool) -> Server {
        Server {
            history,
            spool,
            conns: Arc::new(Mutex::new(HashMap::new())),
            tot_sessions: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn start(history: History, spool: Spool, listener_sets: TcpListenerSets) -> io::Result<()> {
        let server = Server {
            history,
            spool,
            conns: Arc::new(Mutex::new(HashMap::new())),
            tot_sessions: Arc::new(AtomicU64::new(0)),
        };
        let config = config::get_config();

        Runtime::new().unwrap().block_on(async move {
            let (bus_sender, bus_recv) = bus::new();

            // Start the trust-resolver task and the hostcache task.
            HostCache::resolver_task(bus_recv.clone())
                .await
                .map_err(|e| ioerr!(Other, "initializing trust dns resolver: {}", e))?;

            match config.server.runtime.as_str() {
                "threaded" => {
                    let server = server.clone();
                    let mut listener_sets = listener_sets;
                    let listeners = listener_sets.pop().unwrap();
                    task::spawn(server.run_threaded(listeners, bus_recv.clone()));
                },
                "multisingle" => {
                    let server = server.clone();
                    server.run_multisingle(listener_sets, bus_recv.clone());
                },
                _ => unreachable!(),
            }

            // SIGUSR1 -> expire.
            server.listen_expire(bus_recv.clone());

            server.wait(bus_sender, bus_recv).await;
            Ok(())
        })
    }

    /// Run the server on a bunch of current_thread executors.
    fn run_multisingle(self, mut listener_sets: TcpListenerSets, bus_recv: bus::Receiver) {
        let config = config::get_config();

        let mut core_ids = config.multisingle.core_ids.clone().unwrap_or(Vec::new());

        while let Some(listeners) = listener_sets.pop() {
            let server = self.clone();
            let bus_recv = bus_recv.clone();
            let core_id = core_ids.pop();

            thread::spawn(move || {
                if let Some(id) = core_id {
                    // tie this thread to a specific core.
                    core_affinity::set_for_current(id);
                }

                // tokio runtime for this thread alone.
                let mut basic_runtime = runtime::Builder::new()
                    .basic_scheduler()
                    .enable_all()
                    .build()
                    .unwrap();
                log::trace!("runtime::basic_scheduler on {:?}", thread::current().id());

                basic_runtime.block_on(async move {
                    let mut tasks = Vec::new();
                    for listener in listeners.into_iter() {
                        let server = server.clone();
                        let bus_recv = bus_recv.clone();
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap_or_else(|_| {
                            // Never happens.
                            eprintln!("cannot convert from net2 listener to tokio listener");
                            exit(1);
                        });
                        let task = task::spawn(async move {
                            server.run(listener, bus_recv).await;
                        });
                        tasks.push(task);
                    }
                    for task in tasks.into_iter() {
                        let _ = task.await;
                    }
                });
            });
        }
    }

    // run the server on the default threaded executor.
    async fn run_threaded(self, listeners: Vec<TcpListener>, bus_recv: bus::Receiver) {
        for listener in listeners.into_iter() {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap_or_else(|_| {
                // Never happens.
                eprintln!("cannot convert from net2 listener to tokio listener");
                exit(1);
            });
            task::spawn(self.clone().run(listener, bus_recv.clone()));
        }
    }

    // Listen for an `Expire` notification (SIGUSR1) then run expire.
    fn listen_expire(&self, mut bus_recv: bus::Receiver) {
        let server = self.clone();
        task::spawn(async move {
            while let Some(notification) = bus_recv.recv().await {
                match notification {
                    Notification::ExitGraceful | Notification::ExitNow => break,
                    Notification::Expire => {
                        let config = config::get_config();
                        if let Err(e) = server
                            .history
                            .expire(&server.spool, config.history.remember.clone(), false, true)
                            .await
                        {
                            log::error!("expire: {}", e);
                        }
                    },
                    _ => {},
                }
            }
        });
    }

    // wait for all sessions to finish.
    async fn wait(&self, mut bus_sender: bus::Sender, mut bus_recv: bus::Receiver) {
        // wait for a shutdown notification
        let mut waited = 0u32;
        while let Some(notification) = bus_recv.recv().await {
            match notification {
                Notification::ExitGraceful => {
                    log::info!("received Notification::ExitGraceful");
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
                log::info!("sending Notification::ExitNow to all remaining sessions");
                let _ = bus_sender.send(Notification::ExitNow);
            }
            waited += 1;
            let _ = tokio::time::delay_for(Duration::from_millis(100)).await;
            if waited == 100 || waited % 600 == 0 {
                log::warn!("still waiting!");
            }
        }

        let incoming_logger = logger::get_incoming_logger();
        incoming_logger.quit();

        log::info!("exiting.");
        logger::logger_flush();
    }

    // This is run for every TCP listener socket.
    async fn run(self, mut listener: tokio::net::TcpListener, bus_recv: bus::Receiver) {
        use futures::future::Either;
        use futures::stream;

        // We have two streams. One, a stream of incoming connections.
        // Two, a stream of notifications. Combine them.
        let incoming = listener.incoming().map(|s| Either::Left(s));
        let bus_recv2 = bus_recv.clone().map(|w| Either::Right(w));
        let mut items = stream::select(incoming, bus_recv2);

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
                .bus_recv(bus_recv.clone())
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
            let session = NntpReceiver::new(peer, codec, self.clone(), stats);
            task::spawn(session.run());
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
