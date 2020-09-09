use std::collections::{HashMap, VecDeque};
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
use tokio::sync::mpsc;
use tokio::task;

use crate::bus::{self, Notification};
use crate::config::{self, MultiSingle};
use crate::metrics::RxSessionStats;
use crate::dns::HostCache;
use crate::history::History;
use crate::logger;
use crate::nntp_codec::{self, NntpCodec};
use crate::nntp_send::{FeedArticle, MasterFeed};
use crate::nntp_server::NntpServer;
use crate::spool::Spool;
use crate::util::TcpListenerSets;

static TOT_SESSIONS: AtomicU64 = AtomicU64::new(0);

pub fn inc_sessions() {
    TOT_SESSIONS.fetch_add(1, Ordering::SeqCst);
}

pub fn dec_sessions() {
    TOT_SESSIONS.fetch_sub(1, Ordering::SeqCst);
}

#[derive(Clone)]
pub struct Server {
    pub history: History,
    pub spool:   Spool,
    pub conns:   Arc<Mutex<HashMap<String, usize>>>,
    pub outfeed: mpsc::Sender<FeedArticle>,
}

impl Server {
    pub fn start(history: History, spool: Spool, listener_sets: TcpListenerSets) -> io::Result<()> {
        Runtime::new().unwrap().block_on(async move {
            // Create pub/sub bus.
            let (bus_sender, bus_recv) = bus::new();

            // Start the trust-resolver task and the hostcache task.
            HostCache::start(bus_recv.clone())
                .await
                .map_err(|e| ioerr!(Other, "initializing trust dns resolver: {}", e))?;

            // Start the outgoing feed.
            let (mut master, master_chan) = MasterFeed::new(bus_recv.clone(), spool.clone()).await;
            task::spawn(async move {
                master.run().await;
            });

            // Create server struct.
            let mut server = Server {
                history,
                spool,
                conns: Arc::new(Mutex::new(HashMap::new())),
                outfeed: master_chan,
            };
            let config = config::get_config();

            // Start the nntp sender.
            match config.server.runtime {
                config::Runtime::Threaded(_) => {
                    let server = server.clone();
                    let mut listener_sets = listener_sets;
                    let listeners = listener_sets.pop_front().unwrap();
                    task::spawn(server.run_threaded(listeners, bus_recv.clone()));
                },
                config::Runtime::MultiSingle(ref mcfg) => {
                    let server = server.clone();
                    server.run_multisingle(mcfg, listener_sets, bus_recv.clone());
                },
            }

            // SIGUSR1 -> expire.
            server.listen_expire(bus_recv.clone());

            // Do not hold on to the masterfeed channel.
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            server.outfeed = tx;
            drop(rx);

            server.wait(bus_sender, bus_recv).await;
            Ok(())
        })
    }

    /// Run the server on a bunch of current_thread executors.
    fn run_multisingle(
        self,
        mcfg: &MultiSingle,
        mut listener_sets: TcpListenerSets,
        bus_recv: bus::Receiver,
    )
    {
        let core_ids = mcfg
            .core_ids
            .as_ref()
            .map(|c| c.iter().cloned().collect::<VecDeque<_>>());
        let mut core_ids = core_ids.unwrap_or(VecDeque::new());

        let mut n = 0;

        while let Some(listeners) = listener_sets.pop_front() {
            let server = self.clone();
            let bus_recv = bus_recv.clone();
            let core_id = core_ids.pop_front();

            thread::spawn(move || {
                if let Some(id) = core_id {
                    // tie this thread to a specific core.
                    log::info!(
                        "runtime: binding tokio basic runtime #{} to core id {:?}",
                        n,
                        core_id
                    );
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

            n += 1;
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
        while TOT_SESSIONS.load(Ordering::SeqCst) > 0 {
            if waited == 50 {
                // after 5 seconds.
                log::info!("sending Notification::ExitNow to all remaining sessions");
                let _ = bus_sender.send(Notification::ExitNow);
            }
            waited += 1;
            let _ = tokio::time::delay_for(Duration::from_millis(100)).await;
            if waited % 100 == 0 {
                // every 10 seconds.
                log::warn!("still waiting!");
            }
            if waited == 600 {
                // after one minute.
                log::error!(
                    "shutting down server: {} sessions stuck, exiting anyway",
                    TOT_SESSIONS.load(Ordering::SeqCst)
                );
                std::process::exit(1);
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
            let stats = RxSessionStats::new(peer, fdno);

            // build and run an nntp session.
            let session = NntpServer::new(peer, codec, self.clone(), stats);
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
