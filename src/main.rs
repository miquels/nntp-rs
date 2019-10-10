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

use logger::LogTarget;
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

    // first read the config file.
    let cfg_file = matches.value_of("CONFIG").unwrap_or("config.toml");
    let config = match config::read_config(cfg_file) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{}", e);
            exit(1);
        },
    };

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

    // switch uids after binding the socket.
    if let Err(e) = config::switch_uids(&config) {
        eprintln!("nntp-rs: {}", e);
        exit(1);
    }

    // Open the general log.
    let g_log = config.logging.general.as_ref();
    let g_log = g_log.map(|s| s.as_str()).unwrap_or("stderr");
    match LogTarget::new_with(g_log, &config) {
        Ok(t) => logger::logger_init(t),
        Err(e) => {
            eprintln!("nntp-rs: logging.general: {}: {}", g_log, e);
            exit(1);
        },
    }

    // save the config permanently.
    let config = config::set_config(config);

    // Open the incoming log
    let i_log = config.logging.incoming.as_ref();
    let i_log = i_log.map(|s| s.as_str()).unwrap_or("null");
    match logger::LogTarget::new_with(i_log, &config) {
        Ok(t) => logger::set_incoming_logger(t),
        Err(e) => {
            eprintln!("nntp-rs: logging.incoming: {}: {}", i_log, e);
            exit(1);
        },
    }

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

    info!("Listening on port {}", addr.port());

    // install custom panic logger.
    if config.server.log_panics {
        handle_panic();
    }

    // and start server.
    let mut server = server::Server::new(hist, spool);
    match config.server.runtime.as_ref().map(|s| s.as_str()) {
        None|Some("threadpool") => server.run_threadpool(listener),
        Some("multisingle") => server.run_multisingle(listener),
        Some(e) => {
            eprintln!("nntp-rs: unknown runtime {}", e);
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
