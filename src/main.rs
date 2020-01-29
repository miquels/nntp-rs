#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

pub mod article;
pub mod arttype;
pub mod blocking;
pub mod buffer;
pub mod commands;
pub mod config;
pub mod dconfig;
pub mod diag;
#[macro_use]
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
#[structopt(setting = clap::AppSettings::VersionlessSubcommands)]
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
    /// Prettify (json) output
    #[structopt(long)]
    pub pretty: bool,
    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(StructOpt, Debug)]
#[structopt(rename_all = "kebab-case")]
pub enum Command {
    #[structopt(display_order = 1)]
    /// Run the server
    Serve(ServeOpts),
    #[structopt(display_order = 2)]
    /// History file lookup
    HistLookup(HistLookupOpts),
    #[structopt(display_order = 3)]
    /// History file inspection / debugging
    HistInspect(HistInspectOpts),
    #[structopt(display_order = 4)]
    /// History file expire (offline)
    HistExpire(HistExpireOpts),
    #[structopt(display_order = 5)]
    /// Read article from spool.
    SpoolRead(SpoolReadOpts),
}

#[derive(StructOpt, Debug)]
pub struct ServeOpts {
    #[structopt(short, long)]
    /// listen address/port ([::]:1119)
    pub listen: Option<Vec<String>>,
}

#[derive(StructOpt, Debug)]
pub struct HistInspectOpts {
    #[structopt(short, long)]
    /// history file.
    pub file: Option<String>,
}

#[derive(StructOpt, Debug)]
pub struct HistExpireOpts {
    #[structopt(short, long)]
    /// history file.
    pub file: Option<String>,
}

#[derive(StructOpt, Debug)]
pub struct HistLookupOpts {
    #[structopt(short, long)]
    /// history file.
    pub file: Option<String>,
    /// Message-Id.
    pub msgid: String,
}

#[derive(StructOpt, Debug)]
pub struct SpoolReadOpts {
    #[structopt(short, long)]
    /// Default is headers only, add this to read the body.
    pub body: bool,
    #[structopt(short, long)]
    /// Dump article in raw wire-format.
    pub raw: bool,
    #[structopt(short, long)]
    /// history file.
    pub file: Option<String>,
    /// Message-Id.
    pub msgid: String,
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
    let load_newsfeeds = match opts.cmd {
        Command::Serve(_) => true,
        _ => false,
    };
    let config = match config::read_config(&opts.config, load_newsfeeds) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{}", e);
            exit(1);
        },
    };

    // save the config permanently.
    let config = config::set_config(config);

    let run_opts = match opts.cmd {
        Command::Serve(opts) => opts,
        other => run_subcommand(other, &*config, opts.pretty),
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

    let bt = config.threaded.blocking_type.clone();

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
        None | Some("threaded") => server.run_threaded(listeners),
        Some("multisingle") => server.run_multisingle(listeners),
        Some(e) => {
            eprintln!("nntp-rs: unknown runtime {}", e);
            exit(1);
        },
    }
}

fn run_subcommand(cmd: Command, config: &config::Config, pretty: bool) -> ! {
    // set up the logger.
    let target = LogTarget::new_with("stderr", &config).unwrap();
    logger::logger_init(target);

    // run subcommand.
    let res = match cmd {
        Command::Serve(_) => unreachable!(),
        Command::HistLookup(opts) => history_lookup(&*config, opts, pretty),
        Command::HistExpire(opts) => history_expire(&*config, opts),
        Command::HistInspect(opts) => history_inspect(&*config, opts),
        Command::SpoolRead(opts) => spool_read(&*config, opts),
    };

    // flush and exit.
    logger::logger_flush();
    match res {
        Ok(_) => exit(0),
        Err(_) => exit(1),
    }
}

fn history_common(
    config: &config::Config,
    file: Option<&String>,
) -> io::Result<(history::History, spool::Spool)>
{
    // open history file.
    let hpath = config::expand_path(&config.paths, file.unwrap_or(&config.history.file));
    let hist = History::open(
        &config.history.backend,
        hpath.clone(),
        false,
        None,
        Some(BlockingType::Blocking),
    )
    .map_err(|e| {
        eprintln!("nntp-rs: history {}: {}", hpath, e);
        e
    })?;

    // open spool.
    let spool = Spool::new(&config.spool, None, Some(BlockingType::Blocking)).map_err(|e| {
        eprintln!("nntp-rs: initializing spool: {}", e);
        e
    })?;

    Ok((hist, spool))
}

fn history_inspect(config: &config::Config, opts: HistInspectOpts) -> io::Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        hist.inspect(&spool).await.map_err(|e| {
            eprintln!("{}", e);
            e
        })
    })
}

fn history_expire(config: &config::Config, opts: HistExpireOpts) -> io::Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        hist.expire(&spool, config.history.remember.clone(), true, true)
            .await
            .map_err(|e| {
                eprintln!("{}", e);
                e
            })
    })
}

fn history_lookup(config: &config::Config, opts: HistLookupOpts, pretty: bool) -> io::Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let he = hist.lookup(&opts.msgid).await.map_err(|e| {
            eprintln!("{}", e);
            e
        })?;
        let json = he
            .map(|h| h.to_json(&spool))
            .unwrap_or(serde_json::json!({"status":"notfound"}));
        println!(
            "{}",
            if pretty {
                serde_json::to_string_pretty(&json).unwrap()
            } else {
                json.to_string()
            }
        );
        Ok(())
    })
}

// Read an article from the spool.
fn spool_read(config: &config::Config, opts: SpoolReadOpts) -> io::Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        // For now, lookup goes through the history file. We might add
        // lookup by "storage token" or hash.
        let he = match hist.lookup(&opts.msgid).await {
            Ok(Some(he)) => he,
            Ok(None) => {
                eprintln!("{} not found", opts.msgid);
                return Err(io::ErrorKind::NotFound.into());
            },
            Err(e) => {
                eprintln!("{}", e);
                return Err(e);
            },
        };
        let loc = match he.location {
            Some(loc) => loc,
            None => {
                eprintln!("{} {}", opts.msgid, he.status.name());
                return Err(io::ErrorKind::NotFound.into());
            },
        };

        // just headers or whole article?
        let part = match opts.body {
            true => spool::ArtPart::Article,
            false => spool::ArtPart::Head,
        };

        // find it
        let buffer = crate::buffer::Buffer::new();
        let buf = spool.read(loc, part, buffer).await.map_err(|e| {
            eprintln!("spool_read {}: {}", opts.msgid, e);
            e
        })?;
        let mut buf = buf.to_bytes_mut();

        // Output article
        use std::io::Write;
        if opts.raw {
            // wire-format
            io::stdout().write_all(&buf[..])?;
        } else {
            // translate from on-the-wire format to normal format.
            for line in buf.split_mut(|&b| b == b'\n') {
                if line.ends_with(b"\r") {
                    line[line.len() - 1] = b'\n';
                }
                if line == b".\n" {
                    break;
                }
                let start = if line.starts_with(b".") { 1 } else { 0 };
                io::stdout().write_all(&line[start..])?;
            }
        }

        Ok(())
    })
}


/// Create a socket, set SO_REUSEPORT on it, bind it to an address,
/// and start listening for connections.
pub fn bind_socket(addr: &SocketAddr) -> io::Result<TcpListener> {
    let builder = if addr.is_ipv6() {
        // create IPv6 socket and make it v6-only.
        let b = net2::TcpBuilder::new_v6()
            .map_err(|e| ioerr!(Other, "creating IPv6 socket: {}", e))?;
        b.only_v6(true).map_err(|e| ioerr!(e.kind(), "setting socket to only_v6: {}", e))?;
        b
    } else {
        net2::TcpBuilder::new_v4()
            .map_err(|e| ioerr!(e.kind(), "creating IPv4 socket: {}", e))?
    };
    // reuse_addr to make sure we can restart quickly.
    let builder = builder.reuse_address(true)
        .map_err(|e| ioerr!(e.kind(), "setting SO_REUSEADDR on socket: {}", e))?;
    // reuse_port to be able to have multiple sockets listening on the same port.
    let builder = builder.reuse_port(true)
        .map_err(|e| ioerr!(e.kind(), "setting SO_REUSEPORT on socket: {}", e))?;
    let builder = builder
        .bind(addr)
        .map_err(|e| ioerr!(e.kind(), "binding socket to {}: {}", addr, e))?;
    let listener = builder
        .listen(128)
        .map_err(|e| ioerr!(e.kind(), "listening on socket: {}", e))?;
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
