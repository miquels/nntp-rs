use std::net::TcpListener;
use std::panic::AssertUnwindSafe;
use std::thread;
use std::time::Duration;
use std::io;
use std::process::exit;
use std::sync::Arc;

use bytes::BytesMut;
use futures::{Future,Stream};
use num_cpus;
use tk_listen::ListenExt;
use tokio::prelude::*;
use tokio;
use tokio::runtime::current_thread;

use bind_socket;
use config::Config;
use nntp_codec::NntpCodec;
use nntp_rs_history::History;
use nntp_rs_spool::Spool;
use nntp_session::NntpSession;

pub struct Server {
    pub history:    History,
    pub spool:      Spool,
    pub config:     Config,
}

impl Server {

    /// Create a new Server.
    pub fn new(config: Config, history: History, spool: Spool) -> Server {
        Server{ history, spool, config }
    }

    /// Run the server.
    pub fn run(self, listener: TcpListener) -> io::Result<()> {

        trace!("main server running on thread {:?}", thread::current().id());

        // Now start a bunch of threads to serve the requests.
        let num_threads = self.config.server.threads.unwrap_or(num_cpus::get());
        let mut threads = Vec::new();

        let addr = listener.local_addr().unwrap();
        let mut first = Some(listener);

        let server = Arc::new(self);

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

            let server = server.clone();

            let tid = thread::spawn(move || {

                // tokio runtime for this thread alone.
                let mut runtime = current_thread::Runtime::new().unwrap();

                trace!("current_thread::runtime on {:?}", thread::current().id());
                let handle = tokio::reactor::Handle::current();

                let listener = tokio::net::TcpListener::from_std(listener, &handle)
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
                        let mut session = NntpSession::new(peer, control, server.clone());

                        // fake an "initial command".
                        let s = Box::new(stream::iter_ok::<_, io::Error>(vec![BytesMut::from(&b"CONNECT"[..])]));

                        let responses = s.chain(reader).and_then(move |inbuf| {
                            trace!("connection running on thread {:?}", thread::current().id());
                            session.on_input(inbuf)
                        })
                        .map_err(|e| {
                            warn!("got error from stream: {:?}", e);
                            e
                        });
                        let session = writer.send_all(responses).map_err(|_| ()).map(|_| ());

                        // catch panics and recover.
                        let session = AssertUnwindSafe(session);
                        tokio::spawn(session.catch_unwind().then(|result| {
                            match result {
                                Ok(f) => f,
                                Err(_) => {
                                    error!("thread panicked - recovering");
                                    Ok(())
                                },
                            }
                        }))
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
}

