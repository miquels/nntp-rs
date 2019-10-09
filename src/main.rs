#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
#[macro_use] extern crate clap;
#[macro_use] extern crate serde_derive;

pub mod article;
pub mod arttype;
pub mod blocking;
pub mod commands;
pub mod config;
pub mod dconfig;
pub mod diag;
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

    let matches = clap_app!(nntp_rs =>
        (version: "0.1")
        (@arg CONFIG: -c --config +takes_value "config file (config.toml)")
        (@arg LISTEN: -l --listen +takes_value "listen address/port ([::]:1119)")
        (@arg DEBUG: --debug "maximum log verbosity: debug (info)")
        (@arg TRACE: --trace "maximum log verbosity: trace (info)")
    ).get_matches();

    if matches.is_present("TRACE") {
        log::set_max_level(log::LevelFilter::Trace);
    } else if matches.is_present("DEBUG") {
        log::set_max_level(log::LevelFilter::Debug);
    } else {
        log::set_max_level(log::LevelFilter::Info);
    }

    let cfg_file = matches.value_of("CONFIG").unwrap_or("config.toml");
    if let Err(e) = config::read_config(cfg_file) {
        eprintln!("{}", e);
        exit(1);
    }
    let config = config::get_config();

    // open history file. this will remain open as long as we run,
    // configuration file changes do not influence that.
    let hpath = config::expand_path(&config.paths, &config.history.file);
    let hist = History::open(&config.history.backend, hpath.clone(), config.history.threads).map_err(|e| {
         eprintln!("nntp-rs: history {}: {}", hpath, e);
         exit(1);
    }).unwrap();
    let spool = Spool::new(&config.spool).map_err(|e| {
         eprintln!("nntp-rs: initializing spool: {}", e);
         exit(1);
    }).unwrap();

    // start listening on a socket.
    let cfg_listen = config.server.listen.as_ref().map(|s| s.as_str());
    let listen = matches.value_of("LISTEN").unwrap_or(cfg_listen.unwrap_or("[::]:119"));
    let addr = listen.parse().map_err(|e| {
        eprintln!("nntp-rs: listen address {}: {}", listen, e);
        exit(1);
    }).unwrap();
    let listener = bind_socket(&addr).map_err(|e| {
        eprintln!("nntp-rs: {}", e);
        exit(1);
    }).unwrap();
    info!("Listening on port {}", addr.port());

    // install custom panic logger.
    //handle_panic();

    // and start server.
    let mut server = server::Server::new(hist, spool);
    match config.server.executor.as_ref().map(|s| s.as_str()) {
        None|Some("threadpool") => server.run_threadpool(listener),
        Some("current_thread") => server.run_current_thread(listener),
        Some(e) => {
            eprintln!("nntp-rs: unknown executor {}", e);
            exit(1);
        }
    }
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
