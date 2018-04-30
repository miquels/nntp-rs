#[macro_use] extern crate clap;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate nntp;
extern crate storage;

use storage::{History,HistStatus,Spool,ArtPart};
use nntp::config;

fn main() {
    env_logger::init().unwrap();

    let matches = clap_app!(readart =>
        (version: "0.1")
        (@arg CONFIG: -c --config +takes_value "config file (config.toml)")
        (@arg HONLY: -h --headonly "headers only")
        (@arg MSGID: +required "message-id")
    ).get_matches();

    let cfg_file = matches.value_of("CONFIG").unwrap_or("config.toml");
    let msgid = matches.value_of("MSGID").unwrap();
    let part = match matches.is_present("HONLY") {
        true => ArtPart::Head,
        false => ArtPart::Article,
    };

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

    let dhe = dh.lookup(msgid).map_err(|e| {
         println!("{}", e);
         return;
    }).unwrap();

    debug!("histent: {:?}", dhe);
    if dhe.status != HistStatus::Present {
        println!("{:?}", dhe.status);
        return;
    }

    let mut art = st.open(&dhe.location.unwrap(), part).map_err(|e| {
        println!("{}", e);
        return;
    }).unwrap();
    let sz = if part == ArtPart::Head { 8192 } else { 32768 };
    let mut s = Vec::with_capacity(sz);
    art.read_to_end(&mut s).unwrap();
    use std::io::Write;
    std::io::stdout().write(&s).ok();
}

