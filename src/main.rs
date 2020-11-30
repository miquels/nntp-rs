use std::fmt::Write as _;
use std::io;
use std::panic;
use std::process::exit;
use std::thread;

use structopt::StructOpt;

use nntp_rs::config;
use nntp_rs::dns;
use nntp_rs::history::{self, History, MLockMode};
use nntp_rs::ioerr;
use nntp_rs::logger::{self, LogTarget};
use nntp_rs::nntp_client;
use nntp_rs::server;
use nntp_rs::spool::{self, Spool};
use nntp_rs::util::{self, format, BlockingType, TcpListenerSets};

// use jemalloc instead of system malloc.
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(StructOpt, Debug)]
#[structopt(setting = clap::AppSettings::VersionlessSubcommands)]
pub struct MainOpts {
    #[structopt(short, long, default_value = "config.cfg")]
    /// Config file (config.cfg)
    pub config: String,
    #[structopt(short, long)]
    /// Maximum log verbosity: debug (info)
    pub debug:  bool,
    #[structopt(short, long)]
    /// Maximum log verbosity: debug (trace)
    pub trace:  bool,
    /// Prettify (json) output
    #[structopt(long)]
    pub pretty: bool,
    #[structopt(subcommand)]
    pub cmd:    Command,
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
    #[structopt(display_order = 4)]
    /// Initialize new history file.
    HistInit(HistInitOpts),
    #[structopt(display_order = 6)]
    /// Read article from spool.
    SpoolRead(SpoolReadOpts),
    #[structopt(display_order = 7)]
    /// Expire a spool.
    SpoolExpire(SpoolExpireOpts),
    #[structopt(display_order = 8)]
    /// Generate a test article and send it out.
    TestArticle(TestArticleOpts),
    #[structopt(display_order = 9)]
    /// Dump the main configuration.
    DumpConfig,
    #[structopt(display_order = 10)]
    /// Dump the newsfeeds configuration.
    DumpNewsfeeds,
}

#[derive(StructOpt, Debug)]
pub struct ServeOpts {
    #[structopt(short = "-D", long)]
    /// Run in the background.
    pub daemonize: bool,
    #[structopt(short, long)]
    /// listen address/port ([::]:1119)
    pub listen:    Option<Vec<String>>,
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

// XXX for now this is diablo specific, fix that later.
#[derive(StructOpt, Debug)]
pub struct HistInitOpts {
    #[structopt(short, long)]
    /// history file.
    pub file:        Option<String>,
    #[structopt(short = "N", long = "num-buckets")]
    /// number of hash buckets (default 256K)
    pub num_buckets: Option<u32>,
}

#[derive(StructOpt, Debug)]
pub struct HistLookupOpts {
    #[structopt(short, long)]
    /// history file.
    pub file:  Option<String>,
    /// Message-Id.
    pub msgid: String,
}

#[derive(StructOpt, Debug)]
pub struct SpoolReadOpts {
    #[structopt(short, long)]
    /// Default is headers only, add this to read the body.
    pub body:  bool,
    #[structopt(short, long)]
    /// Dump article in raw wire-format.
    pub raw:   bool,
    #[structopt(short, long)]
    /// history file.
    pub file:  Option<String>,
    /// Message-Id.
    pub msgid: String,
}

#[derive(StructOpt, Debug)]
pub struct SpoolExpireOpts {
    #[structopt(short, long)]
    /// Spool number to expire.
    pub spool: u8,
    #[structopt(short, long)]
    /// Default is dry_run, use this to actually expire.
    pub doit:  bool,
}

#[derive(StructOpt, Debug)]
pub struct TestArticleOpts {
    #[structopt(short, long)]
    /// Newsgroup to post to (must be *.test)
    pub group:    String,
    #[structopt(short, long)]
    /// Subject (default: test <MSGID>)
    pub subject:  Option<String>,
    #[structopt(short, long)]
    /// Number of articles to send (default: 1)
    pub num_arts: Option<u32>,
    #[structopt(long)]
    /// Size of articles to send.
    pub size:     Option<u32>,
    #[structopt(short, long)]
    /// Delay between articles (ms)
    pub delay:    Option<f64>,
    #[structopt(short, long)]
    /// Port to use (default: 119)
    pub port:     Option<u16>,
    #[structopt(short, long)]
    /// Use MODE HEADFEED
    pub headfeed: bool,
    /// Server to connect to
    pub hostname: String,
}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let opts = MainOpts::from_args();

    if opts.trace {
        log::set_max_level(log::LevelFilter::Trace);
    } else if opts.debug {
        log::set_max_level(log::LevelFilter::Debug);
    } else {
        log::set_max_level(log::LevelFilter::Info);
    }

    // early check for commands that do not need to read the config file.
    match opts.cmd {
        cmd @ Command::TestArticle(..) => run_subcommand(cmd, None, opts.pretty),
        _ => {},
    };

    // first read the config file.
    let load_newsfeeds = match opts.cmd {
        Command::Serve(_) => true,
        Command::DumpNewsfeeds => true,
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
        other => run_subcommand(other, Some(&*config), opts.pretty),
    };

    let listenaddrs = run_opts.listen.as_ref().or_else(|| config.server.listen.as_ref());
    let addrs = config::parse_listeners(listenaddrs).unwrap_or_else(|e| {
        eprintln!("nntp-rs: {}", e);
        exit(1);
    });
    let num_sets = match config.server.runtime {
        config::Runtime::MultiSingle(ref multisingle) => {
            multisingle
                .core_ids
                .as_ref()
                .map(|c| c.len())
                .unwrap_or(num_cpus::get())
        },
        config::Runtime::Threaded(_) => 1,
    };
    let listener_sets = TcpListenerSets::new(&addrs, num_sets).unwrap_or_else(|e| {
        eprintln!("nntp-rs: {}", e);
        exit(1);
    });

    // switch uids after binding the socket.
    if let Err(e) = config::switch_uids(&config) {
        eprintln!("nntp-rs: {}", e);
        exit(1);
    }

    // Open the general log.
    let g_log = config.logging.general.as_ref();
    let g_log = g_log.map(|s| s.as_str()).unwrap_or("stderr");
    match LogTarget::new_with(g_log, &config) {
        Ok(t) => logger::logger_init(t, Some("nntp_rs_server".to_string())),
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

    let bt = match config.server.runtime {
        config::Runtime::Threaded(ref threaded) => threaded.blocking_type.clone(),
        config::Runtime::MultiSingle(_) => BlockingType::default(),
    };

    // open history file. this will remain open as long as we run,
    // configuration file changes do not influence that.
    let hpath = config::expand_path(&config.paths, &config.history.file);
    let hist = History::open(
        &config.history.backend,
        hpath.clone(),
        true,
        config.history.mlock,
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

    if run_opts.daemonize {
        if let Err(e) = util::daemonize(config.server.pidfile.as_ref(), true) {
            eprintln!("nntp-rs: daemonizing: {}", e);
            exit(1);
        }
    }

    log::info!("Listening on {:?}", addrs);

    // install custom panic logger.
    if config.server.log_panics {
        handle_panic();
    }

    // and start server.
    if let Err(e) = server::Server::start(hist, spool, listener_sets) {
        log::error!("{}", e);
        exit(1);
    }
    Ok(())
}

fn run_subcommand(cmd: Command, config: Option<&config::Config>, pretty: bool) -> ! {
    // set up the logger.
    let target = LogTarget::new_stdout();
    logger::logger_init(target, Some("nntp_rs_server".to_string()));

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let res = runtime.block_on(async move {
        // run subcommand.
        let config = config.map(|c| &*c);
        match cmd {
            Command::Serve(_) => unreachable!(),
            Command::HistLookup(opts) => history_lookup(config.unwrap(), opts, pretty).await,
            Command::HistExpire(opts) => history_expire(config.unwrap(), opts).await,
            Command::HistInit(opts) => diablo_history_init(config.unwrap(), opts).await,
            Command::HistInspect(opts) => history_inspect(config.unwrap(), opts).await,
            Command::SpoolRead(opts) => spool_read(config.unwrap(), opts).await,
            Command::SpoolExpire(opts) => spool_expire(config.unwrap(), opts).await,
            Command::TestArticle(opts) => test_article(opts).await,
            Command::DumpConfig => dump_config(config.unwrap()),
            Command::DumpNewsfeeds => dump_newsfeeds(),
        }
    });

    // flush and exit.
    logger::logger_flush();
    match res {
        Ok(_) => exit(0),
        Err(e) => {
            eprintln!("{}", e);
            exit(1);
        },
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
        MLockMode::None,
        None,
        BlockingType::Blocking,
    )
    .map_err(|e| ioerr!(e.kind(), "nntp-rs: history {}: {}", hpath, e))?;

    // open spool.
    let spool = Spool::new(&config.spool, None, BlockingType::Blocking)
        .map_err(|e| ioerr!(e.kind(), "nntp-rs: initializing spool: {}", e))?;

    Ok((hist, spool))
}

async fn history_inspect(config: &config::Config, opts: HistInspectOpts) -> Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    hist.inspect(&spool).await?;
    Ok(())
}

async fn history_expire(config: &config::Config, opts: HistExpireOpts) -> Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    let res = hist
        .expire(&spool, config.history.remember.clone(), true, true)
        .await?;
    Ok(res)
}

// For now, diablo specific. fix later
async fn diablo_history_init(config: &config::Config, opts: HistInitOpts) -> Result<()> {
    let num_buckets = opts.num_buckets.unwrap_or(256 * 1024 * 1024);
    let hpath = config::expand_path(&config.paths, &config.history.file);
    let hfile = opts.file.as_ref().unwrap_or(&hpath);
    nntp_rs::history::diablo::DHistory::create(std::path::Path::new(hfile), num_buckets)?;
    Ok(())
}

async fn history_lookup(config: &config::Config, opts: HistLookupOpts, pretty: bool) -> Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;
    let he = hist.lookup(&opts.msgid).await?;
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
}

// Read an article from the spool.
async fn spool_read(config: &config::Config, opts: SpoolReadOpts) -> Result<()> {
    let (hist, spool) = history_common(config, opts.file.as_ref())?;

    // For now, lookup goes through the history file. We might add
    // lookup by "storage token" or hash.
    let he = match hist.lookup(&opts.msgid).await {
        Ok(Some(he)) => Ok(he),
        Ok(None) => Err(ioerr!(NotFound, "{} not found", opts.msgid)),
        Err(e) => Err(e),
    }?;
    let status = he.status.name();
    let loc = he
        .location
        .ok_or_else(|| ioerr!(NotFound, "{} {}", opts.msgid, status))?;

    // just headers or whole article?
    let part = match opts.body {
        true => spool::ArtPart::Article,
        false => spool::ArtPart::Head,
    };

    // find it
    let buffer = util::Buffer::new();
    let mut data = spool
        .read(loc, part, buffer)
        .await
        .map_err(|e| ioerr!(e.kind(), "spool_read {}: {}", opts.msgid, e))?;

    // Output article
    use std::io::Write;
    if opts.raw {
        // wire-format
        io::stdout().write_all(&data)?;
    } else {
        // translate from on-the-wire format to normal format.
        for line in data.split_mut(|&b| b == b'\n') {
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
}

async fn spool_expire(config: &config::Config, opts: SpoolExpireOpts) -> Result<()> {
    // open spool.
    let spool = Spool::new(&config.spool, None, BlockingType::Blocking)
        .map_err(|e| ioerr!(e.kind(), "nntp-rs: initializing spool: {}", e))?;
    // call expire.
    let bytes = spool.expire(opts.spool, !opts.doit).await?;
    println!("{} article data total expired", format::size(bytes));
    if let Some(oldest) = spool.get_oldest().get(&opts.spool) {
        println!("oldest article: {}", oldest);
    }
    Ok(())
}

async fn test_article(opts: TestArticleOpts) -> Result<()> {
    if !opts.group.ends_with(".test") {
        Err(ioerr!(InvalidData, "test-article: newsgroup must end in '.test'"))?;
    }

    dns::init_resolver().await?;

    let port = opts.port.unwrap_or(119);
    let (cmd, code) = if opts.headfeed {
        ("MODE HEADFEED", 250)
    } else {
        ("MODE STREAM", 203)
    };
    let (mut codec, _, welcome) =
        nntp_client::nntp_connect(&opts.hostname, port, cmd, code, None, None, None, None).await?;
    println!("<< {}", welcome);

    let num_arts = opts.num_arts.unwrap_or(1);

    for _ in 0..num_arts {
        let msgid = nntp_client::message_id(None);
        let hostname = msgid.split("@").nth(1).unwrap();

        let mut buf = String::new();
        let cmd = format!("TAKETHIS <{}>", msgid);

        write!(buf, "{}\r\n", cmd)?;
        write!(buf, "Path: test!not-for-mail\r\n")?;
        write!(buf, "Newsgroups: {}\r\n", opts.group)?;
        write!(buf, "Distribution: local\r\n")?;
        write!(buf, "Message-Id: <{}>\r\n", msgid)?;
        if opts.headfeed {
            write!(buf, "Bytes: 17\r\n")?;
        }
        write!(buf, "Date: {}\r\n", util::UnixTime::now().to_rfc2822())?;
        write!(buf, "From: test@{}\r\n", hostname)?;
        write!(
            buf,
            "Subject: {}\r\n",
            opts.subject.as_ref().unwrap_or(&format!("test {}", msgid))
        )?;
        if !opts.headfeed {
            if opts.size.is_none() {
                write!(buf, "\r\ntest, ignore.")?;
            }
            if let Some(size) = opts.size {
                let size = size as usize;
                let mut done = 0;
                let data = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                let mut d = 0;
                let mut line = Vec::new();
                while done < size {
                    line.truncate(0);
                    line.extend_from_slice(&b"\r\n"[..]);
                    line.extend(format!("{:06} ", done).into_bytes().into_iter());
                    line.resize(70 + (rand::random::<u8>() / 12) as usize, data[d]);
                    d += 1;
                    if d >= data.len() {
                        d = 0;
                    }
                    buf.push_str(std::str::from_utf8(&line).unwrap());
                    done += line.len();
                }
            }
        }
        write!(buf, "\r\n.")?;

        println!(">> {}", cmd);
        let resp = codec.command(buf).await?;
        println!("<< {}", resp.short());

        if let Some(delay) = opts.delay {
            tokio::time::delay_for(std::time::Duration::from_micros((delay * 1000f64) as u64)).await;
        }
    }
    Ok(())
}

fn handle_panic() {
    // This hook mimics the standard logging hook, it adds some extra
    // thread-id info, and logs to log::error!().
    panic::set_hook(Box::new(|info| {
        let mut msg = "".to_string();
        let mut loc = "".to_string();
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            msg = "'".to_string() + *s + "', ";
        }
        if let Some(s) = info.payload().downcast_ref::<String>() {
            msg = "'".to_string() + s.as_str() + "', ";
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
            log::error!("thread '{}' panicked", name);
        } else {
            log::error!("thread '{}' panicked at {}{}", name, msg, loc);
        }
    }));
}

fn dump_config(config: &config::Config) -> Result<()> {
    println!("{:#?}", config);
    Ok(())
}

fn dump_newsfeeds() -> Result<()> {
    println!("{:#?}", config::get_newsfeeds());
    Ok(())
}
