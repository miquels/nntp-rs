#[macro_use] extern crate clap;
#[macro_use] extern crate futures;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate byteorder;
extern crate bytes;
extern crate chrono;
extern crate dns_lookup;
extern crate env_logger;
extern crate futures_cpupool;
extern crate ipnet;
extern crate libc;
extern crate memchr;
extern crate net2;
extern crate num_cpus;
extern crate parking_lot;
extern crate regex;
extern crate serde;
extern crate time;
extern crate tk_listen;
extern crate tokio;
extern crate toml;

pub mod article;
pub mod arttype;
pub mod commands;
pub mod config;
pub mod dconfig;
pub mod errors;
pub mod history;
pub mod hostcache;
pub mod logger;
pub mod newsfeeds;
pub mod nntp_codec;
pub mod nntp_session;
pub mod server;
pub mod spool;
pub mod util;

use std::io;
use std::net::{SocketAddr,TcpListener};
use std::panic;
use std::process::exit;
use std::thread;

use net2::unix::UnixTcpBuilderExt;
use history::History;
use spool::Spool;

fn main() -> io::Result<()> {
    env_logger::init();

    let matches = clap_app!(nntp_rs =>
        (version: "0.1")
        (@arg CONFIG: -c --config +takes_value "config file (config.toml)")
        (@arg LISTEN: -l --listen +takes_value "listen address/port ([::]:1119)")
    ).get_matches();

    let cfg_file = matches.value_of("CONFIG").unwrap_or("config.toml");
    if let Err(e) = config::read_config(cfg_file) {
        eprintln!("{}", e);
        exit(1);
    }
    let config = config::get_config();

    // open history file. this will remain open as long as we run,
    // configuration file changes do not influence that.
    let hist = History::open(&config.history.backend, config.history.path.clone(), config.history.threads).map_err(|e| {
         eprintln!("nntp-rs: history {}: {}", config.history.path, e);
         exit(1);
    }).unwrap();
    let spool = Spool::new(&config.spool).map_err(|e| {
         eprintln!("nntp-rs: initializing spool: {}", e);
         exit(1);
    }).unwrap();

    // start listening on a socket.
    let listen = matches.value_of("LISTEN").unwrap_or("[::]:1119");
    let addr = listen.parse().map_err(|e| {
        eprintln!("nntp-rs: listen address {}: {}", listen, e);
        exit(1);
    }).unwrap();
    let listener = bind_socket(&addr).map_err(|e| {
        eprintln!("nntp-rs: {}", e);
        exit(1);
    }).unwrap();
    println!("Listening on port {}", addr.port());

    // install custom panic logger.
    //handle_panic();

    // and start server.
    let server = server::Server::new(hist, spool);
    server.run(listener)
}

/// Create a socket, set SO_REUSEPORT on it, bind it to an address,
/// and start listening for connections.
pub fn bind_socket(addr: &SocketAddr) -> io::Result<TcpListener> {

    let builder = net2::TcpBuilder::new_v6().map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("creating IPv6 socket: {}", e))
    })?;
    let builder = builder.reuse_port(true).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("setting SO_REUSEPORT on socket: {}", e))
    })?;
    let builder = builder.bind(addr).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("binding socket to {}: {}", addr, e))
    })?;
    let listener = builder.listen(128).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("listening on socket: {}", e))
    })?;
    Ok(listener)
}

fn handle_panic() {
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
}