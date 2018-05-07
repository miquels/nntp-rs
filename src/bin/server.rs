#[macro_use] extern crate clap;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate time;
extern crate nntp;
extern crate backends;

use std::net::TcpListener;
use std::thread;
use std::io;
use std::io::{Read,Write};
use std::sync::Arc;

use backends::{History,HistStatus,Spool,ArtPart};
use nntp::nntpproto::{NntpStream,DotReader,DataReader};
use nntp::commands::{Cmd,Capb,CmdNo};
use nntp::config;

#[derive(Clone)]
struct Store {
	history:	Arc<History>,
	spool:		Arc<Spool>,
}

struct NntpSession<T> {
    store:      Store,
    strm:       NntpStream<T>,
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
                let mut sess = NntpSession::new(stream, store.clone());
                thread::spawn(move || {
                    if let Err(e) = sess.handle_client() {
                        println!("{}", e);
                    }
                });
            }
            Err(e) => panic!(e),
        }
    }
}

impl<T: Read + Write> NntpSession<T> {

    pub fn new(strm: T, store: Store) -> NntpSession<T> {
        NntpSession{
            strm:   NntpStream::new(strm),
            store:  store,
        }
    }

    fn serve_article(&mut self, argv: &[&str], part: ArtPart) -> io::Result<()> {
        if argv.len() < 2 || !argv[1].starts_with("<") {
            write!(self.strm, "412 No newsgroup selected\r\n")?;
            return Ok(());
        }
        let he = self.store.history.lookup(argv[1])?;
        if he.status != HistStatus::Present ||
           (he.head_only && part != ArtPart::Head) {
            write!(self.strm, "430 No such article\r\n")?;
            return Ok(());
        }
        let mut art = match self.store.spool.open(&he.location.unwrap(), part) {
            Ok(art) => art,
            Err(_) => {
                write!(self.strm, "430 No such article\r\n")?;
                return Ok(());
            },
        };

        if argv[0] == "stat" {
            write!(self.strm, "223 0 {}\r\n", argv[1])?;
            return Ok(());
        }

        let code = match part {
            ArtPart::Article => 220,
            ArtPart::Head => 221,
            ArtPart::Body => 222,
        };
        write!(self.strm, "{} 0 {}\r\n", code, argv[1])?;
        let sz = if part == ArtPart::Head { 8192 } else { 32768 };
        let mut s = Vec::with_capacity(sz);
        art.read_to_end(&mut s).unwrap();
        self.strm.write(&s)?;
        if part == ArtPart::Head {
            self.strm.write(b".\r\n")?;
        }
        Ok(())
    }

    fn takethis(&mut self, argv: &[&str]) -> io::Result<()> {
        let mut bufs = Vec::new();
        let r = {
            let mut rdr = DataReader::new(DotReader::new(&mut self.strm), 2000000, 8192, 65536);
            rdr.read_all(&mut bufs)?
        };
        write!(self.strm, "439 {} {}\r\n", argv[1], r)
    }

    fn help(&mut self, cmd: &Cmd) -> io::Result<()> {
        cmd.help(&mut self.strm)
    }

    fn capabilities(&mut self, cmd: &Cmd) -> io::Result<()> {
        cmd.capabilities(&mut self.strm)
    }

    fn handle_client(&mut self) -> io::Result<()> {

        write!(self.strm, "200 Ready\r\n")?;

        let mut line = String::new();

        let mut cmd = Cmd::new();
        cmd.add_cap(Capb::Ihave);
        cmd.add_cap(Capb::Streaming);

        loop {
            line.clear();
            self.strm.read_line_string(&mut line)?;
            if !line.ends_with("\r\n") {
                //use std::ascii::AsciiExt;
                line.make_ascii_lowercase();
                if &line != "quit\n" {
                    write!(self.strm, "500 lines must be terminated with CRLF\r\n")?;
                    continue;
                }
                line.truncate(4);
            } else {
                let l = line.len();
                line.truncate(l - 2);
            }

            if line.is_empty() {
                continue;
            }

            let (cmd_no, argv) = match cmd.parse(&mut line) {
                Err(m) => {
                    write!(self.strm, "{}\r\n", m)?;
                    continue;
                },
                Ok((c, a)) => (c, a),
            };

            match cmd_no {
                CmdNo::Quit => {
                    write!(self.strm, "205 Bye!\r\n")?;
                    break;
                },
                CmdNo::Help => {
                    self.help(&cmd)?;
                },
                CmdNo::Capabilities => {
                    self.capabilities(&cmd)?;
                },
                CmdNo::Date => {
                    let tm = time::now_utc();
                    write!(self.strm, "111 {}\r\n", time::strftime("%Y%m%d%H%M%S", &tm).unwrap())?;
                },
                CmdNo::Head => {
                    self.serve_article(&argv, ArtPart::Head)?;
                },
                CmdNo::Body => {
                    self.serve_article(&argv, ArtPart::Body)?;
                },
                CmdNo::Article | CmdNo::Stat => {
                    self.serve_article(&argv, ArtPart::Article)?;
                },
                CmdNo::Mode_Stream => {
                    write!(self.strm, "203 Streaming permitted\r\n")?;
                },
                CmdNo::Check => {
                    let code = match self.store.history.check(argv[1])? {
                        HistStatus::Tentative => 431,
                        HistStatus::NotFound => 238,
                        _ => 438,
                    };
                    write!(self.strm, "{} {}\r\n", code, argv[1])?;
                },
                CmdNo::Takethis => {
                    self.takethis(&argv)?;
                },
                _ => {
                    write!(self.strm, "500 what?\r\n")?;
                },
            }
        }
        Ok(())
    }
}
