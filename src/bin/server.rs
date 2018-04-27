#[macro_use] extern crate clap;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate time;
extern crate nntp;
extern crate storage;

use std::net::{TcpListener, TcpStream};
use std::thread;
use std::io;
use std::io::{BufRead,BufReader,Read,Write};
use std::sync::Arc;

use storage::{History,HistStatus,Spool,ArtPart};
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
    let st = Spool::new(&config.spool).map_err(|e| {
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
                    handle_client(stream, st).map_err(|e| {
                        println!("{}", e);
                    }).unwrap();
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

fn help<W: Write>(mut out: W) -> io::Result<()> {
    write!(out, "100 Hallo! Ik begrijp de volgende commando's:\r\n")?;
    write!(out, "  help\r\n")?;
    write!(out, "  date\r\n")?;
    write!(out, "  head <msgid>\r\n")?;
    write!(out, "  body <msgid>\r\n")?;
    write!(out, "  article <msgid>\r\n")?;
    write!(out, "  check <msgid>\r\n")?;
    write!(out, ".\r\n")
}

fn handle_client(mut out: TcpStream, store: Store) -> io::Result<()> {
	let mut rdr = BufReader::new(out.try_clone()?);
    write!(out, "200 Ready\r\n")?;

    let mut line = String::new();

    loop {
        line.clear();
        let len = rdr.read_line(&mut line)?;
        if len == 0 {
            break;
        }
        let mut cmd = String::new();
        let mut argv : Vec<_> = line.split_whitespace().collect();
        if argv.len() == 0 {
            write!(out, "500 what?\r\n")?;
            continue;
        }
        cmd.push_str(&argv[0].to_lowercase());
        argv[0] = cmd.as_ref();
        match argv[0] {
            "quit" => {
                write!(out, "205 bye\r\n")?;
                break;
            },
            "help" => {
                help(&out)?;
            },
            "date" => {
                let tm = time::now_utc();
                write!(out, "111 {}\r\n", time::strftime("%Y%m%d%H%M%S", &tm).unwrap())?;
            },
            "head" => {
                serve_article(&out, &store, &argv, ArtPart::Head)?;
            },
            "body" => {
                serve_article(&out, &store, &argv, ArtPart::Body)?;
            },
            "article" | "stat" => {
                serve_article(&out, &store, &argv, ArtPart::Article)?;
            },
            "check" => {
                let code = match store.history.check(argv[1])? {
                    HistStatus::Tentative => 431,
                    HistStatus::NotFound => 238,
                    _ => 438,
                };
                write!(out, "{} {}\r\n", code, argv[1])?;
            },
            _ => {
                write!(out, "500 what?\r\n")?;
            },
        }
        drop(argv);
    }
    Ok(())
}
