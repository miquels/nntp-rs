use std::collections::HashMap;
use std::io;
use std::net::TcpListener;

use std::process::exit;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::{Future,Stream};
use num_cpus;
use parking_lot::Mutex;
use tk_listen::ListenExt;
use tokio::prelude::*;
use tokio;
use tokio::runtime::current_thread;

use crate::bind_socket;
use crate::config;
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

    /// Run the server.
    pub fn run(self, listener: TcpListener) -> io::Result<()> {

        trace!("main server running on thread {:?}", thread::current().id());
        let config = config::get_config();

        // Now start a bunch of threads to serve the requests.
        let num_threads = config.server.threads.unwrap_or(num_cpus::get());
        let mut threads = Vec::new();

        let addr = listener.local_addr().unwrap();
        let mut first = Some(listener);

        for _ in 0..num_threads {

            // The first listener is passed in, after that we need to
            // create extra listeners here.
            let listener = if first.is_some() {
                first.take().unwrap()
            } else {
                bind_socket(&addr).map_err(|e| {
                    eprintln!("nntp-rs: server: fatal: {}", e);
                    exit(1);
                }).unwrap()
            };

            let server = self.clone();

            let tid = thread::spawn(move || {

                // tokio runtime for this thread alone.
                let mut runtime = current_thread::Runtime::new().unwrap();
                let handle = runtime.handle();

                trace!("current_thread::runtime on {:?}", thread::current().id());
                let reactor_handle = tokio::reactor::Handle::default();

                let listener = tokio::net::TcpListener::from_std(listener, &reactor_handle)
                    .expect("cannot convert from net2 listener to tokio listener");

                // Pull out a stream of sockets for incoming connections
                let nntp_server = listener.incoming()
                    .sleep_on_error(Duration::from_millis(100))
                    .map(move |socket| {

                        // set up codec for reader and writer.
                        let peer = socket.peer_addr().unwrap_or("0.0.0.0:0".parse().unwrap());
                        let codec = NntpCodec::new(socket);
                        let control = codec.control();
                        let (writer, reader) = codec.split();

                        // build an nntp session.
                        let mut session = NntpSession::new(peer, control.clone(), server.clone());

                        let responses = reader.then(move |result| {
                            match result {
                                Err(e) => session.on_read_error(e),
                                Ok(input) => match input {
                                    NntpInput::Connect => session.on_connect(),
                                    NntpInput::WriteError(e) => session.on_write_error(e),
                                    NntpInput::Eof => session.on_eof(),
                                    buf @ NntpInput::Line(_)|
                                    buf @ NntpInput::Block(_)|
                                    buf @ NntpInput::Article(_) => session.on_input(buf),
                                }
                            }
                        })
                        .map(move |r| r.data);

                        let session = writer
                            .send_all(responses)
                            .map_err(move |e| control.write_error(e))
                            .map(|_| ());

                        /*
                        let session = AssertUnwindSafe(session);
                        handle.spawn(session.catch_unwind().then(|result| {
                            match result {
                                Ok(f) => f,
                                Err(_) => {
                                    error!("thread panicked - recovering");
                                    Ok(())
                                },
                            }
                        })).map_err(|_| ())
                        */
                        handle.spawn(session).map_err(|e| error!("run: error {}", e))
                    })
                    .listen(65524);

                // And spawn it on the thread-local runtime.
                let _ = runtime.block_on(nntp_server);
            });

            threads.push(tid);
        }

        // and wait for the threads to come home
        for t in threads.into_iter() {
            let _ = t.join();
        }
        Ok(())
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

