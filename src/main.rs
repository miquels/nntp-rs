#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

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
use std::net::{SocketAddr, TcpListener};
use std::panic;
use std::process::exit;
use std::thread;

use logger::LogTarget;
use net2::unix::UnixTcpBuilderExt;
use structopt::StructOpt;

use blocking::BlockingType;
use history::History;
use spool::Spool;

// use jemalloc instead of system malloc.
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(StructOpt, Debug)]
pub struct MainOpts {
    #[structopt(short, long, default_value = "config.toml")]
    /// Config file (config.toml)
    pub config: String,
    #[structopt(short, long)]
    /// Maximum log verbosity: debug (info)
    pub debug: bool,
    #[structopt(short, long)]
    /// Maximum log verbosity: debug (trace)
    pub trace: bool,
    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(StructOpt, Debug)]
pub enum Command {
    /// Run the server
    Run(RunOpts),
    /// History file stuff
    History(HistoryCmd),
}

#[derive(StructOpt, Debug)]
pub struct RunOpts {
    #[structopt(short, long)]
    /// listen address/port ([::]:1119)
    pub listen: Option<Vec<String>>,
}

#[derive(StructOpt, Debug)]
pub struct HistoryCmd {
    #[structopt(subcommand)]
    sub: HistorySubCmd,
}

#[derive(StructOpt, Debug)]
pub enum HistorySubCmd {
    Expire(ExpireOpts),
    Inspect(ExpireOpts),
}

#[derive(StructOpt, Debug)]
pub struct ExpireOpts {
    /// file to run expire on
    pub file: String,
}

fn main() -> io::Result<()> {
    let opts = MainOpts::from_args();

    if opts.trace {
        log::set_max_level(log::LevelFilter::Trace);
    } else if opts.debug {
        log::set_max_level(log::LevelFilter::Debug);
    } else {
        log::set_max_level(log::LevelFilter::Info);
    }

    // first read the config file.
    let config = match config::read_config(&opts.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{}", e);
            exit(1);
        },
    };

    // save the config permanently.
    let config = config::set_config(config);

    let run_opts = match opts.cmd {
        Command::Run(opts) => opts,
        Command::History(cmd) => {
            history(&*config, cmd.sub);
            logger::logger_flush();
            return Ok(());
        },
    };

    let mut listenaddrs = &config.server.listen;
    let cmd_listenaddrs;
    if let Some(ref l) = run_opts.listen {
        cmd_listenaddrs = Some(config::StringOrVec::Vec(l.to_vec()));
        listenaddrs = &cmd_listenaddrs;
    }
    let addrs = config::parse_listeners(listenaddrs)
        .map_err(|e| {
            eprintln!("nntp-rs: {}", e);
            exit(1);
        })
        .unwrap();
    let mut listeners = Vec::new();
    for addr in &addrs {
        let listener = bind_socket(&addr)
            .map_err(|e| {
                eprintln!("nntp-rs: listen socket: bind to {}: {}", addr, e);
                exit(1);
            })
            .unwrap();
        listeners.push(listener);
    }

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

    let bt = config.threadpool.blocking_type.clone();

    // open history file. this will remain open as long as we run,
    // configuration file changes do not influence that.
    let hpath = config::expand_path(&config.paths, &config.history.file);
    let hist = History::open(
        &config.history.backend,
        hpath.clone(),
        true,
        config.history.threads,
        bt.clone(),
    )
    .map_err(|e| {
        eprintln!("nntp-rs: history {}: {}", hpath, e);
        exit(1);
    })
    .unwrap();

    // open spool. ditto.
    let spool = Spool::new(&config.spool, None, bt)
        .map_err(|e| {
            eprintln!("nntp-rs: initializing spool: {}", e);
            exit(1);
        })
        .unwrap();

    info!("Listening on {:?}", addrs);

    // install custom panic logger.
    if config.server.log_panics {
        handle_panic();
    }

    // and start server.
    let mut server = server::Server::new(hist, spool);
    match config.server.runtime.as_ref().map(|s| s.as_str()) {
        None | Some("threadpool") => server.run_threadpool(listeners),
        Some("multisingle") => server.run_multisingle(listeners),
        Some(e) => {
            eprintln!("nntp-rs: unknown runtime {}", e);
            exit(1);
        },
    }
}

/*
fn history_help() {
    let _ = HistoryOpts::clap().bin_name("history").print_help();
    println!("");
    exit(1);
}
*/

fn history(config: &config::Config, cmd: HistorySubCmd) {
    // set up the logger.
    let target = LogTarget::new_with("stderr", &config).unwrap();
    logger::logger_init(target);

    match cmd {
        HistorySubCmd::Expire(opts) => history_expire(config, opts),
        HistorySubCmd::Inspect(opts) => history_inspect(config, opts),
    }
}

fn history_expire(config: &config::Config, opts: ExpireOpts) {
    // open history file.
    let hpath = config::expand_path(&config.paths, &opts.file);
    let hist = History::open(
        &config.history.backend,
        hpath.clone(),
        false,
        None,
        Some(BlockingType::Blocking),
    )
    .map_err(|e| {
        eprintln!("nntp-rs: history {}: {}", hpath, e);
        exit(1);
    })
    .unwrap();

    // open spool.
    let spool = Spool::new(&config.spool, None, Some(BlockingType::Blocking))
        .map_err(|e| {
            eprintln!("nntp-rs: initializing spool: {}", e);
            exit(1);
        })
        .unwrap();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        if let Err(e) = hist.expire(&spool, config.history.remember.clone(), true, true).await {
            eprintln!("{}", e);
        }
    })
}

fn history_inspect(config: &config::Config, opts: ExpireOpts) {
    // open history file.
    let hpath = config::expand_path(&config.paths, &opts.file);
    let hist = History::open(
        &config.history.backend,
        hpath.clone(),
        false,
        None,
        Some(BlockingType::Blocking),
    )
    .map_err(|e| {
        eprintln!("nntp-rs: history {}: {}", hpath, e);
        exit(1);
    })
    .unwrap();

    // open spool.
    let spool = Spool::new(&config.spool, None, Some(BlockingType::Blocking))
        .map_err(|e| {
            eprintln!("nntp-rs: initializing spool: {}", e);
            exit(1);
        })
        .unwrap();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        if let Err(e) = hist.inspect(&spool).await {
            eprintln!("{}", e);
        }
    })
}


/// Create a socket, set SO_REUSEPORT on it, bind it to an address,
/// and start listening for connections.
pub fn bind_socket(addr: &SocketAddr) -> io::Result<TcpListener> {
    let builder = if addr.is_ipv6() {
        // create IPv6 socket and make it v6-only.
        let b = net2::TcpBuilder::new_v6()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("creating IPv6 socket: {}", e)))?;
        if let Err(e) = b.only_v6(true) {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("setting socket to only_v6: {}", e),
            ))
        } else {
            Ok(b)
        }
    } else {
        net2::TcpBuilder::new_v4()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("creating IPv4 socket: {}", e)))
    }?;
    // reuse_addr to make sure we can restart quickly.
    let builder = builder.reuse_address(true).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("setting SO_REUSEPORT on socket: {}", e),
        )
    })?;
    // reuse_port to be able to have multiple sockets listening on the same port.
    let builder = builder.reuse_port(true).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("setting SO_REUSEPORT on socket: {}", e),
        )
    })?;
    let builder = builder
        .bind(addr)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("binding socket to {}: {}", addr, e)))?;
    let listener = builder
        .listen(128)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("listening on socket: {}", e)))?;
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
