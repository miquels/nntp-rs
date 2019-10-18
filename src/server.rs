use std::collections::HashMap;
use std::io;
use std::net::TcpListener;
use std::os::unix::io::AsRawFd;
use std::process::exit;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use num_cpus;
use parking_lot::Mutex;
use tokio;
use tokio::prelude::*;
use tokio::runtime::current_thread;

use crate::bind_socket;
use crate::config;
use crate::diag::SessionStats;
use crate::nntp_codec::{NntpCodec,NntpInput};
use crate::history::History;
use crate::spool::Spool;
use crate::nntp_session::NntpSession;

#[derive(Clone)]
pub struct Server {
    pub history:    History,
    pub spool:      Spool,
    pub conns:      Arc<Mutex<HashMap<String, usize>>>
}

impl Server {

    /// Create a new Server.
    pub fn new(history: History, spool: Spool) -> Server {
        Server{
            history,
            spool,
            conns: Arc::new(Mutex::new(HashMap::new())),
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
        let addrs = listeners.iter().map(|l| l.local_addr().unwrap()).collect::<Vec<_>>();
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
                    let l = bind_socket(&addr).map_err(|e| {
                            eprintln!("nntp-rs: server: fatal: {}", e);
                            exit(1);
                        }).unwrap();
                    v.push(l);
                }
                listener_sets.push(v);
            }
        }

        for listener_set in listener_sets.into_iter() {

            let server = self.clone();
            let core_id = if core_ids.len() > 0 {
                Some(core_ids.remove(0))
            } else {
                None
            };

            let tid = thread::spawn(move || {

                // tokio runtime for this thread alone.
                let mut runtime = current_thread::Runtime::new().unwrap();

                trace!("current_thread::runtime on {:?}", thread::current().id());
                let reactor_handle = tokio_net::driver::Handle::default();

                if let Some(id) = core_id {
                    core_affinity::set_for_current(id);
                }

                for listener in listener_set.into_iter() {
                    let listener = tokio::net::TcpListener::from_std(listener, &reactor_handle)
                        .expect("cannot convert from net2 listener to tokio listener");
                    runtime.spawn(server.clone().run(listener));
                }

                let _ = runtime.run();
            });

            threads.push(tid);
        }

        // and wait for the threads to come home
        for t in threads.into_iter() {
            let _ = t.join();
        }
        Ok(())
    }

    // run the server on the default threadpool executor.
    pub fn run_threadpool(&mut self, listeners: Vec<TcpListener>) -> io::Result<()> {

        let reactor_handle = tokio_net::driver::Handle::default();
        let runtime = tokio::runtime::Runtime::new()?;

        for listener in listeners.into_iter() {
            let listener = tokio::net::TcpListener::from_std(listener, &reactor_handle)
                .expect("cannot convert from net2 listener to tokio listener");
            runtime.spawn(self.clone().run(listener));
        }

        runtime.shutdown_on_idle();

        Ok(())
    }

    async fn run(self, listener: tokio::net::TcpListener)
    {
        // Pull out a stream of sockets for incoming connections
        let mut incoming = listener.incoming();
        while let Some(socket) = incoming.next().await {

            let socket = match socket {
                Ok(s) => s,
                Err(_) => {
                    tokio::timer::delay_for(Duration::from_millis(50));
                    continue;
                },
            };

            // set up codec for reader and writer.
            let peer = socket.peer_addr().unwrap_or("0.0.0.0:0".parse().unwrap());
            let fdno = socket.as_raw_fd() as u32;
            let codec = NntpCodec::new(socket);
            let control = codec.control();
            let (mut writer, mut reader) = codec.split();

            let stats = SessionStats{
                hostname:   peer.to_string(),
                ipaddr:     peer.to_string(),
                label:      "unknown".to_string(),
                fdno:       fdno,
                ..SessionStats::default()
            };

            // build an nntp session.
            let mut session = NntpSession::new(peer, control.clone(), self.clone(), stats);

            let task = async move {
                while let Some(result) = reader.next().await {
                    let response = match result {
                        Err(e) => session.on_read_error(e).await,
                        Ok(input) => match input {
                            NntpInput::Connect => session.on_connect().await,
                            NntpInput::Eof => session.on_eof().await,
                            buf @ NntpInput::Line(_)|
                            buf @ NntpInput::Block(_)|
                            buf @ NntpInput::Article(_) => {
                                match session.on_input(buf).await {
                                    Ok(res) => res,
                                    Err(e) => session.on_generic_error(e).await,
                                }
                            }
                        }
                    };
                    if let Err(e) = writer.send(response.data).await {
                        session.on_write_error(e).await;
                        break;
                    }
                }
            };

            tokio::spawn(task);
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

