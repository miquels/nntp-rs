use std::io::{self,Write};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::thread;
use std::time::Duration;

use chrono::{Datelike, Timelike, offset::Local, offset::TimeZone};
use crossbeam_channel as channel;
use log::{self, LevelFilter, Log, Metadata, Record};
use parking_lot::Mutex;

use crate::article::Article;
use crate::config;
use crate::errors::*;
use crate::newsfeeds::NewsPeer;
use crate::util;

pub fn incoming_reject(logger: &Logger, label: &str, art: &Article, error: ArtError) {
    let l = format!("{} - {} {} {} {:?}", label, art.msgid, art.len, art.arttype, error);
    logger.log_line(l);
}

pub fn incoming_defer(logger: &Logger, label: &str, art: &Article, error: ArtError) {
    let l = format!("{} d {} {} {} {:?}", label, art.msgid, art.len, art.arttype, error);
    logger.log_line(l);
}

pub fn incoming_accept(logger: &Logger, label: &str, art: &Article, peers: &[NewsPeer], wantpeers: &[u32]) {
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
    let l = format!("{} + {} {} {} {}", label, art.msgid, art.len, art.arttype, s);
    logger.log_line(l);
}

lazy_static! {
    static ref LOGGER_: Mutex<Option<Logger>> = Mutex::new(None);
    static ref LOGGER: Logger = {
        LOGGER_.lock().take().unwrap()
    };
    static ref INCOMING_: Mutex<Option<Logger>> = Mutex::new(None);
    static ref INCOMING: Logger = {
        INCOMING_.lock().take().unwrap()
    };

}

pub struct FileData {
    file:       io::BufWriter<fs::File>,
    name:       String,
    curname:    String,
    ino:        u64,
    when:       u64,
}

pub enum LogDest {
    Stderr,
    File(String),
    Syslog,
    Null,
    #[doc(hidden)]
    FileData(FileData),
}

enum Message {
    Record((log::Level, String, String)),
    Line(String),
    Reconfig(LogDest),
    Quit,
}

#[derive(Clone)]
pub struct Logger {
    tx:     channel::Sender<Message>,
}

impl Logger {
    pub fn new(d: LogDest) -> io::Result<Logger> {
        let mut dest = LogDest::open(d)?;
        let (tx, rx) = channel::unbounded();

        thread::spawn(move || {
            let ticker = channel::tick(Duration::from_millis(1000));
            loop {
                channel::select! {
                    recv(ticker) -> _ => {
                        let _ = dest.check();
                    },
                    recv(rx) -> msg => {
                        match msg {
                            Ok(Message::Record(r)) => dest.log_record(r),
                            Ok(Message::Line(s)) => dest.log_line(log::Level::Info, s),
                            Ok(Message::Reconfig(d)) => dest.reopen(d),
                            Ok(Message::Quit) |
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(Logger{ tx })
    }

    pub fn log_record(&self, record: &Record) {

        // strip the program-name prefix from "target". If it is then
        // empty, replace it with "main". If the "target" prefix is NOT
        // program-name, only log Warn and Error messages.
        let level = record.level();
        let mut target = record.target();
        let prefix = "nntp_rs_server::";
        if target.starts_with(prefix) {
            target = &target[prefix.len()..];
        } else if target == "nntp_rs_server" {
            target = "main";
        } else {
            match level {
                log::Level::Error|
                log::Level::Warn => {},
                _ => return,
            }
        }

        let line = record.args().to_string();
        let _ = self.tx.send(Message::Record((level, target.to_string(), line)));
    }

    pub fn log_line(&self, s: String) {
        let _ = self.tx.send(Message::Line(s));
    }

    pub fn reconfig(&self, d: LogDest) {
        let _ = self.tx.send(Message::Reconfig(d));
    }

    pub fn quit(&self) {
        let _ = self.tx.send(Message::Quit);
    }
}

impl Log for Logger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        self.log_record(record);
    }

    fn flush(&self) {
    }
}

impl LogDest {
    fn log_record(&mut self, r: (log::Level, String, String)) {
        match self {
            LogDest::Syslog => {
                // Do not add [target] for info level messages.
                let line = match r.0 {
                    log::Level::Info => r.2,
                    _ => format!("[{}] {}", r.1, r.2),
                };
                self.log_line(r.0, line);
            },
            LogDest::FileData(_) => {
                self.log_line(r.0, format!("[{}] [{}] {}\n", r.0, r.1, r.2));
            },
            LogDest::Stderr => {
                self.log_line(r.0, format!("[{}] [{}] {}", r.0, r.1, r.2));
            },
            LogDest::File(_) => {
                unreachable!();
            },
            LogDest::Null => {},
        }
    }

    fn log_line(&mut self, level: log::Level, line: String) {

        let _ = self.check();

        match self {
            LogDest::Syslog => {
                let formatter = syslog::Formatter3164 {
                    facility:   syslog::Facility::LOG_NEWS,
                    hostname:   None,
                    process:    "nntp-rs-server".to_string(),
                    pid:        0,
                };
                match syslog::unix(formatter) {
                    Err(_) => {},
                    Ok(mut writer) => {
                        match level {
                            log::Level::Error => writer.err(line).ok(),
                            log::Level::Warn => writer.warning(line).ok(),
                            log::Level::Info => writer.info(line).ok(),
                            log::Level::Debug => writer.debug(line).ok(),
                            log::Level::Trace => None,
                        };
                    }
                }
            },
            LogDest::FileData(FileData{ref mut file, when, ..}) => {
                let ns = ((*when % 1000) * 1_000_000) as u32;
                let now = Local.timestamp((*when/1000) as i64, ns);
                let t = format!("{:04}-{:02}-{:02} {:02}:{:02}.{:03}",
                                now.year(), now.month(), now.day(),
                                now.minute(), now.second(),
                                now.timestamp_subsec_millis());
                let _ = write!(file, "{} {}\n", t, line);
            },
            LogDest::Stderr => {
                let _ = eprintln!("{}", line);
            },
            LogDest::File(_) => {
                unreachable!();
            },
            LogDest::Null => {},
        }
    }

    fn reopen(&mut self, dest: LogDest) {
        if let Ok(d) = LogDest::open(dest) {
            *self = d;
        }
    }

    fn open(dest: LogDest) -> io::Result<LogDest> {
        let d = match dest {
            LogDest::Stderr => LogDest::Stderr,
            LogDest::Syslog => LogDest::Syslog,
            LogDest::FileData(_) => unreachable!(),
            LogDest::Null => LogDest::Null,
            LogDest::File(name) => {
                let config = config::get_config();
                let curname = config::expand_path(&config.paths, &name);
                let file = fs::OpenOptions::new().write(true).create(true).append(true).open(&curname)
                    .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", curname, e)))?;
                let ino = file.metadata()?.ino();
                let file = io::BufWriter::new(file);
                let when = util::unixtime_ms();
                LogDest::FileData(FileData{ file, name, curname, ino, when })
            },
        };
        Ok(d)
    }

    // check if we need to reopen the logfile.
    fn check(&mut self) -> io::Result<()> {
        // only if we're the FileData variant.
        let mut fd = match self {
            &mut LogDest::FileData(ref mut fd) => fd,
            _ => return Ok(()),
        };
        // max once a second.
        let now = util::unixtime_ms();
        let when = fd.when;
        fd.when = now;
        if now / 1000 <= when / 1000 {
            return Ok(());
        }

        // flush buffer.
        let _ = fd.file.flush();

        // First check if the filename has a date in it,
        // and that date changed.
        let mut do_reopen = false;
        if fd.name.contains("${date}") {
            let config = config::get_config();
            let curname = config::expand_path(&config.paths, &fd.name);
            if curname != fd.curname {
                do_reopen = true;
            }
        }

        // Might not have changed, but see if file was renamed/moved.
        if !do_reopen {
            do_reopen = match fs::metadata(&fd.curname) {
                Ok(m) => m.ino() != fd.ino,
                Err(e) if e.kind() == io::ErrorKind::NotFound => true,
                Err(e) => return Err(e),
            };
        }

        if do_reopen {
            let name = fd.name.clone();
            *self = LogDest::open(LogDest::File(name))?;
        }
        Ok(())
    }

    pub fn from_str(d: &str) -> LogDest {
        match d {
            "" | "null" | "/dev/null" => LogDest::Null,
            "stderr" => LogDest::Stderr,
            "syslog" => LogDest::Syslog,
            x => LogDest::File(x.to_string()),
        }
    }
}

pub fn logger_init(dest: LogDest) -> io::Result<()> {
    log::set_max_level(LevelFilter::Debug);
    (*LOGGER_.lock()) = Some(Logger::new(dest)?);
    let _ = log::set_logger(&*LOGGER);
    Ok(())
}

