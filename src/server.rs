#[macro_use] extern crate futures;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
extern crate bytes;
extern crate env_logger;
extern crate futures_cpupool;
extern crate memchr;
extern crate net2;
extern crate tk_listen;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io_pool;

mod commands;
mod nntp_codec;
mod nntp_session;

use std::net::SocketAddr;
use std::panic::{self, AssertUnwindSafe};
use std::thread;
use std::time::Duration;
use std::io;

use bytes::BytesMut;
use futures::{Future,future,Stream};
use net2::unix::UnixTcpBuilderExt;
use tk_listen::ListenExt;
use tokio::prelude::*;
use tokio::runtime::current_thread;

use nntp_codec::NntpCodec;
use nntp_session::NntpSession;

fn server() {

    env_logger::init();

    // This hook mimics the standard logging hook, it adds some extra
    // thread-id info, and logs to error!().
    panic::set_hook(Box::new(|info| {
        let mut msg = "".to_string();
        let mut loc = "".to_string();
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            msg = "'".to_string() + s + "', ";
        }
        if let Some(s) = info.payload().downcast_ref::<String>() {
            msg = "'".to_string() + &s + "', ";
        }
        if let Some(l) = info.location() {
            loc = format!("{}", l);
        }
        let t = thread::current();
        let name = match t.name() {
            Some(n) => format!("{} ({:?})", n, t.id()),
            None => format!("{:?}", t.id()),
        };
        if msg == "" && loc == "" {
            error!("thread '{}' panicked", name);
        } else {
            error!("thread '{}' panicked at {}{}", name, msg, loc);
        }
    }));

    // pool to run blocking io on.
    let mut builder = futures_cpupool::Builder::new();
    builder.name_prefix("filesystem-io-");
    builder.pool_size(4);
    let pool = builder.create();

    trace!("main server running on thread {:?}", thread::current().id());

    // Now start a bunch of threads to serve the requests.
    let mut threads = Vec::new();

    for _ in 0..4 {

        // Bind a listener - multiple times to the same port.
        let addr : SocketAddr = "0.0.0.0:12345".parse().unwrap();
        let builder = net2::TcpBuilder::new_v4().expect("could not get IPv4 socket");
        let builder = builder.reuse_port(true).expect("could not enable REUSE_PORT on socket");
        let builder = builder.bind(&addr).expect("unable to bind TCP socket");
        let listener = builder.listen(128).expect("unable to listen() on TCP socket");

        let pool = pool.clone();

        let tid = thread::spawn(move || {

            // tokio runtime for this thread alone.
            let mut runtime = current_thread::Runtime::new().unwrap();

            trace!("current_thread::runtime on {:?}", thread::current().id());
            let handle = tokio::reactor::Handle::current();

            let listener = tokio::net::TcpListener::from_std(listener, &handle)
                .expect("cannot convert from net2 listener to tokio listener");

            // Pull out a stream of sockets for incoming connections
            let server = listener.incoming()
                .map_err(|e| {
                    error!("accept error = {:?}", e);
                    // don't use .sleep_on_error since tokio_io_pool::Runtime
                    // is dumb and does not support timers or other tokio goodies.
                    thread::sleep(Duration::from_millis(100));
                    e
                })
                .map(move |socket| {

                    // set up codec for reader and writer.
                    let codec = NntpCodec::new(socket);
                    let control = codec.control();
                    let (writer, reader) = codec.split();

                    // build an nntp session.
                    let mut session = NntpSession::new(pool.clone(), control);

                    // fake an "initial command".
                    let s = Box::new(stream::iter_ok::<_, io::Error>(vec![BytesMut::from(&b"CONNECT"[..])]));

                    let responses = s.chain(reader).and_then(move |inbuf| {
                        trace!("connection running on thread {:?}", thread::current().id());
                        session.on_input(inbuf)
                        /*
                        // spawn on thread pool
                        pool.spawn_fn(move || {
                            let line = std::str::from_utf8(&inbuf[..]).unwrap();
                            trace!("worker on thread {:?}", thread::current().id());
                            debug!("got {}", line);
                            let mut b = Bytes::new();
                            b.extend_from_slice(line.as_bytes());
                            future::ok(b)
                        })
                        */
                    })
                    .map_err(|e| {
                        trace!("got error from stream: {:?}", e);
                        e
                    });
                    let session = writer.send_all(responses).map_err(|_| ()).map(|_| ());
                    let session = future::lazy(|| session);

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
                .listen(128);

            // And spawn it on the thread-local runtime.
            let _ = runtime.block_on(server);
        });

        threads.push(tid);
    }

    // and wait for the threads to come home
    for t in threads.into_iter() {
        let _ = t.join();
    }

}

