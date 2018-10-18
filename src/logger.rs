
use log::{self, LevelFilter, Log, Metadata, Record};
use std::fs;
use std::io::{self,Write};

use parking_lot::Mutex;

use article::Article;
use errors::*;
use newsfeeds::NewsPeer;

pub fn incoming_reject(label: &str, art: &Article, error: ArtError) {
    info!("{} - {} {} {} {:?}", label, art.msgid, art.len, art.arttype, error);
}

pub fn incoming_defer(label: &str, art: &Article, error: ArtError) {
    info!("{} d {} {} {} {:?}", label, art.msgid, art.len, art.arttype, error);
}

pub fn incoming_accept(label: &str, art: &Article, peers: &[NewsPeer], wantpeers: &[u32]) {
    // allocate string with peers in one go.
    let len = wantpeers.iter().fold(0, |t, i| t + peers[*i as usize].label.len() + 1);
    let mut s = String::with_capacity(len);
    // push peers onto string, separated by space.
    for idx in 0..wantpeers.len() {
        s.push_str(&peers[wantpeers[idx as usize] as usize].label);
        if idx as usize + 1 < wantpeers.len() {
            s.push(' ');
        }
    }
    // and log.
    info!("{} + {} {} {} {}", label, art.msgid, art.len, art.arttype, s);
}



lazy_static! {
    static ref LOGGER: NntpLogger = NntpLogger::new();
}

struct NntpLogger {
    inner: Mutex<LoggerInner>,
}

impl NntpLogger {
    fn new() -> NntpLogger {
        let inner = LoggerInner{
            file:   Box::new(io::stderr()),
            name:   "stderr".to_string(),
        };
        NntpLogger{
            inner:  Mutex::new(inner),
        }
    }

    fn update<T: Write + Send + 'static>(&self, file: T, name: String) {
        *self.inner.lock() = LoggerInner {
            file:   Box::new(file),
            name:   name,
        };
    }
}

impl Log for NntpLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        self.inner.lock().log(record)
    }

    fn flush(&self) {
    }
}

struct LoggerInner {
	file:		Box<Write + Send>,
	name:		String,
}

impl LoggerInner {
    fn log(&mut self, record: &Record) {
        let mut target = record.target();
        let prefix = "nntp_rs_server::";
        if target.starts_with(prefix) {
            target = &target[prefix.len()..];
        }
        let _ = write!(self.file, "{}: {} {}\n", record.level(), target, record.args());
    }
}

pub fn nntp_logger_init() {
    log::set_max_level(LevelFilter::Debug);
    let _ = log::set_logger(&*LOGGER);
}

