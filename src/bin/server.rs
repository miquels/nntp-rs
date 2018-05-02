#[macro_use] extern crate clap;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate time;
extern crate nntp;
extern crate storage;

use std::net::{TcpListener, TcpStream};
use std::thread;
use std::io;
use std::io::{Read,Write};
use std::sync::Arc;

use storage::{History,HistStatus,Spool,ArtPart};
use storage::nntpproto::NntpReader;
use storage::{Cmd,Capb,CmdNo};
use nntp::config;

#[derive(Clone)]
struct Store {
	history:	Arc<History>,
	spool:		Arc<Spool>,
}

fn main() {
    env_logger::init().unwrap();

    let matches = clap_app!(server =>
        (version: "0.1")
        (@arg CONFIG: -c --config +takes_value "config file (config.toml)")
        (@arg LISTEN: -l --listen +takes_value "listen address/port ([::]:1119)")
    ).get_matches();

    let cfg_file = matches.value_of("CONFIG").unwrap_or("config.toml");
    let config = config::read_config(cfg_file).map_err(|e| {
        println!("{}", e);
        return;
    }).unwrap();

    let dh = History::open(&config.history.backend, config.history.path).map_err(|e| {
         println!("{}", e);
         return;
    }).unwrap();
    let st = Spool::new(&config.spool, &config.metaspool).map_err(|e| {
         println!("{}", e);
         return;
    }).unwrap();
	let store = Store{
		history:	Arc::new(dh),
        spool:      Arc::new(st),
    };

    let listen = matches.value_of("LISTEN").unwrap_or("[::]:1119");
    let listener = TcpListener::bind(listen).expect("Unable to bind to socket");

    let addr = listener.local_addr().expect("unable to get the local port?");
    println!("Listening on port {}", addr.port());

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let st = store.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, st) {
                        println!("{}", e);
                    }
                });
            }
            Err(e) => panic!(e),
        }
    }
}

fn serve_article<W: Write>(mut out: W, store: &Store, argv: &[&str], part: ArtPart) -> io::Result<()> {
    if argv.len() < 2 || !argv[1].starts_with("<") {
        write!(out, "412 No newsgroup selected\r\n")?;
        return Ok(());
    }
    let he = store.history.lookup(argv[1])?;
    if he.status != HistStatus::Present ||
       (he.head_only && part != ArtPart::Head) {
        write!(out, "430 No such article\r\n")?;
        return Ok(());
    }
    let mut art = match store.spool.open(&he.location.unwrap(), part) {
        Ok(art) => art,
        Err(_) => {
            write!(out, "430 No such article\r\n")?;
            return Ok(());
        },
    };

    if argv[0] == "stat" {
        write!(out, "223 0 {}\r\n", argv[1])?;
        return Ok(());
    }

    let code = match part {
        ArtPart::Article => 220,
        ArtPart::Head => 221,
        ArtPart::Body => 222,
    };
    write!(out, "{} 0 {}\r\n", code, argv[1])?;
    let sz = if part == ArtPart::Head { 8192 } else { 32768 };
    let mut s = Vec::with_capacity(sz);
    art.read_to_end(&mut s).unwrap();
    out.write(&s)?;
    if part == ArtPart::Head {
        out.write(b".\r\n")?;
    }
    Ok(())
}

fn takethis<R: Read, W: Write>(rdr: &mut NntpReader<R>, mut out: W, store: &Store, argv: &[&str]) -> io::Result<()> {
    let mut bufs = vec![Vec::with_capacity(8192)];
    let res = rdr.read_data(&mut bufs, 1500000)?;
    write!(out, "439 {}\r\n", argv[1])
}

fn help<W: Write>(cmd: &Cmd, out: W) -> io::Result<()> {
    cmd.help(out)
}

fn capabilities<W: Write>(cmd: &Cmd, out: W) -> io::Result<()> {
    cmd.capabilities(out)
}

fn handle_client(mut out: TcpStream, store: Store) -> io::Result<()> {
	let mut rdr = NntpReader::new(out.try_clone()?);
    write!(out, "200 Ready\r\n")?;

    let mut line = String::new();

    let mut cmd = Cmd::new();
    cmd.add_cap(Capb::Ihave);
    cmd.add_cap(Capb::Streaming);

    loop {
        line.clear();
        let len = rdr.read_cmd_string(&mut line)?;
        if len == 0 {
            break;
        }
        let (cmd_no, argv) = match cmd.parse(&mut line) {
            Err(m) => {
                write!(out, "{}\r\n", m)?;
                continue;
            },
            Ok((c, a)) => (c, a),
        };

        match cmd_no {
            CmdNo::Quit => {
                write!(out, "205 Bye!\r\n")?;
                break;
            },
            CmdNo::Help => {
                help(&cmd, &out)?;
            },
            CmdNo::Capabilities => {
                capabilities(&cmd, &out)?;
            },
            CmdNo::Date => {
                let tm = time::now_utc();
                write!(out, "111 {}\r\n", time::strftime("%Y%m%d%H%M%S", &tm).unwrap())?;
            },
            CmdNo::Head => {
                serve_article(&out, &store, &argv, ArtPart::Head)?;
            },
            CmdNo::Body => {
                serve_article(&out, &store, &argv, ArtPart::Body)?;
            },
            CmdNo::Article | CmdNo::Stat => {
                serve_article(&out, &store, &argv, ArtPart::Article)?;
            },
            CmdNo::Mode_Stream => {
                write!(out, "203 Streaming permitted\r\n")?;
            },
            CmdNo::Check => {
                let code = match store.history.check(argv[1])? {
                    HistStatus::Tentative => 431,
                    HistStatus::NotFound => 238,
                    _ => 438,
                };
                write!(out, "{} {}\r\n", code, argv[1])?;
            },
            CmdNo::Takethis => {
                takethis(&mut rdr, &out, &store, &argv)?;
            },
            _ => {
                write!(out, "500 what?\r\n")?;
            },
        }
    }
    Ok(())
}
