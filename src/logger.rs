use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use chrono::{offset::Local, offset::TimeZone, Datelike, Timelike};
use crossbeam_channel as channel;
use log::{self, Log, Metadata, Record};
use parking_lot::{Mutex, RwLock};

use crate::article::Article;
use crate::config::{self, Config};
use crate::errors::*;
use crate::newsfeeds::NewsPeer;
use crate::util;

lazy_static! {
    static ref LOGGER_: Mutex<Option<Logger>> = Mutex::new(None);
    static ref LOGGER: Logger = { LOGGER_.lock().take().unwrap() };
    static ref INCOMING_LOG: RwLock<Option<Incoming>> = RwLock::new(None);
}

/// Logger for the "incoming.log" logfile.
#[derive(Clone)]
pub struct Incoming {
    logger: Logger,
}

impl Incoming {
    pub fn reject(&self, label: &str, art: &Article, error: ArtError) {
        let pathhost = art.pathhost.as_ref().map(|s| s.as_str()).unwrap_or(label);
        let l = format!(
            "{} - {} {} {} {:?}",
            pathhost, art.msgid, art.len, art.arttype, error
        );
        self.logger.log_line(l);
    }

    pub fn defer(&self, label: &str, art: &Article, error: ArtError) {
        let pathhost = art.pathhost.as_ref().map(|s| s.as_str()).unwrap_or(label);
        let l = format!(
            "{} d {} {} {} {:?}",
            pathhost, art.msgid, art.len, art.arttype, error
        );
        self.logger.log_line(l);
    }

    pub fn accept(&self, label: &str, art: &Article, peers: &[NewsPeer], wantpeers: &[u32]) {
        // allocate string with peers in one go.
        let len = wantpeers
            .iter()
            .fold(0, |t, i| t + peers[*i as usize].label.len() + 1);
        let mut s = String::with_capacity(len);

        // push peers onto string, separated by space.
        for idx in 0..wantpeers.len() {
            s.push_str(&peers[wantpeers[idx as usize] as usize].label);
            if idx as usize + 1 < wantpeers.len() {
                s.push(' ');
            }
        }

        // special case.
        if peers.len() == 0 && wantpeers.len() == 0 {
            s.push_str("DontStore");
        }

        // and log.
        let pathhost = art.pathhost.as_ref().map(|s| s.as_str()).unwrap_or(label);
        let l = format!("{} + {} {} {} {}", pathhost, art.msgid, art.len, art.arttype, s);
        self.logger.log_line(l);
    }

    pub fn quit(&self) {
        self.logger.quit()
    }
}

/// Target destination of the log. First create a LogTarget, then
/// use it to construct a new Logger, or pass it to Logger.reconfig().
pub struct LogTarget {
    dest: LogDest,
}

// Open logfile.
struct FileData {
    file:    io::BufWriter<fs::File>,
    name:    String,
    curname: String,
    ino:     u64,
    when:    u64,
}

// Type of log.
enum LogDest {
    Stderr,
    File(String),
    Syslog,
    Null,
    #[doc(hidden)]
    FileData(FileData),
}

// Message sent over the channel to the logging thread.
enum Message {
    Record((log::Level, String, String)),
    Line(String),
    Reconfig(LogDest),
    Flush(channel::Sender<()>),
    Quit,
}

/// Contains a channel over which messages can be sent to the logger thread.
#[derive(Clone)]
pub struct Logger {
    tx:              channel::Sender<Message>,
    tid:             Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    pkg_name_prefix: String,
    pkg_name:        String,
}

impl Logger {
    /// Create a new Logger.
    pub fn new(target: LogTarget) -> Logger {
        Logger::new2(target.dest, false)
    }

    // This one does the actual work.
    fn new2(mut dest: LogDest, is_log: bool) -> Logger {
        let (tx, rx) = channel::unbounded();

        let tid = thread::spawn(move || {
            let ticker = channel::tick(Duration::from_millis(1000));
            loop {
                channel::select! {
                    recv(ticker) -> _ => {
                        // every second, check.
                        if let Err(e) = dest.check() {
                            if !is_log {
                                error!("{}", e);
                            }
                        }
                    },
                    recv(rx) -> msg => {
                        // got a message over the channel.
                        match msg {
                            Ok(Message::Record(r)) => dest.log_record(is_log, r),
                            Ok(Message::Line(s)) => dest.log_line(is_log, log::Level::Info, s),
                            Ok(Message::Reconfig(d)) => dest = d,
                            Ok(Message::Flush(tx)) => {
                                dest.log_flush();
                                let _ = tx.send(());
                            },
                            Ok(Message::Quit) |
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Logger {
            tx,
            tid: Arc::new(Mutex::new(Some(tid))),
            pkg_name_prefix: concat!(env!("CARGO_PKG_NAME"), "::").replace('-', "_"),
            pkg_name: env!("CARGO_PKG_NAME").replace('-', "_"),
        }
    }

    /// For use with the 'log' crate.
    fn log_record(&self, record: &Record) {
        // strip the program-name prefix from "target". If it is then
        // empty, replace it with "main". If the "target" prefix is NOT
        // program-name, only log Warn and Error messages.
        let level = record.level();
        let mut target = record.target();
        if target.starts_with(&self.pkg_name_prefix) {
            target = &target[self.pkg_name_prefix.len()..];
        } else if target == self.pkg_name {
            target = "main";
        } else {
            match level {
                log::Level::Error | log::Level::Warn => {},
                _ => return,
            }
        }

        let line = record.args().to_string();
        let _ = self.tx.send(Message::Record((level, target.to_string(), line)));
    }

    pub fn log_line(&self, s: String) {
        let _ = self.tx.send(Message::Line(s));
    }

    pub fn reconfig(&self, t: LogTarget) {
        let _ = self.tx.send(Message::Reconfig(t.dest));
    }

    pub fn quit(&self) {
        let _ = self.tx.send(Message::Quit);
        let mut tid = self.tid.lock();
        if let Some(tid) = tid.take() {
            let _ = tid.join();
        }
    }

    pub fn log_flush(&self) {
        let (tx, rx) = channel::unbounded();
        let _ = self.tx.send(Message::Flush(tx));
        let _ = rx.recv();
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            self.log_record(record);
        }
    }

    fn flush(&self) {
        self.log_flush();
    }
}

impl LogDest {
    // Log a "log" crate record to the destination.
    fn log_record(&mut self, is_log: bool, r: (log::Level, String, String)) {
        match self {
            LogDest::Syslog => {
                // Do not add [target] for info level messages.
                let line = match r.0 {
                    log::Level::Info => r.2,
                    _ => format!("[{}] {}", r.1, r.2),
                };
                self.log_line(is_log, r.0, line);
            },
            LogDest::FileData(_) => {
                self.log_line(is_log, r.0, format!("[{}] [{}] {}\n", r.0, r.1, r.2));
            },
            LogDest::Stderr => {
                self.log_line(is_log, r.0, format!("[{}] [{}] {}", r.0, r.1, r.2));
            },
            LogDest::File(_) => {
                unreachable!();
            },
            LogDest::Null => {},
        }
    }

    // Log a simple line to the destination.
    fn log_line(&mut self, is_log: bool, level: log::Level, line: String) {
        if let Err(e) = self.check() {
            if !is_log {
                error!("{}", e);
            }
        }

        match self {
            LogDest::Syslog => {
                let formatter = syslog::Formatter3164 {
                    facility: syslog::Facility::LOG_NEWS,
                    hostname: None,
                    process:  "nntp-rs-server".to_string(),
                    pid:      unsafe { libc::getpid() },
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
                    },
                }
            },
            LogDest::FileData(FileData {
                ref mut file, when, ..
            }) => {
                let ns = ((*when % 1000) * 1_000_000) as u32;
                let now = Local.timestamp((*when / 1000) as i64, ns);
                let t = format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                    now.year(),
                    now.month(),
                    now.day(),
                    now.hour(),
                    now.minute(),
                    now.second(),
                    now.timestamp_subsec_millis()
                );
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

    fn log_flush(&mut self) {
        match self {
            LogDest::FileData(FileData { ref mut file, .. }) => {
                let _ = file.flush();
            },
            _ => {},
        }
    }

    // Open a log destination.
    fn open(dest: LogDest, config: &Config) -> io::Result<LogDest> {
        let d = match dest {
            LogDest::Stderr => LogDest::Stderr,
            LogDest::Syslog => LogDest::Syslog,
            LogDest::FileData(_) => unreachable!(),
            LogDest::Null => LogDest::Null,
            LogDest::File(name) => {
                let curname = config::expand_path(&config.paths, &name);
                let file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(&curname)
                    .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", curname, e)))?;
                let ino = file.metadata()?.ino();
                let file = io::BufWriter::new(file);
                let when = util::unixtime_ms();
                LogDest::FileData(FileData {
                    file,
                    name,
                    curname,
                    ino,
                    when,
                })
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

        // First check if the filename has a date in it, and that date changed.
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
                Err(e) => return Err(io::Error::new(e.kind(), format!("{}: {}", fd.curname, e))),
            };
        }

        if do_reopen {
            let name = fd.name.clone();
            let config = config::get_config();
            *self = LogDest::open(LogDest::File(name), &config)?;
        }
        Ok(())
    }
}

impl LogTarget {
    /// Create a new LogTarget with a Config.
    pub fn new_with(d: &str, cfg: &Config) -> io::Result<LogTarget> {
        let dest = match d {
            "" | "null" | "/dev/null" => LogDest::Null,
            "stderr" => LogDest::Stderr,
            "syslog" => LogDest::Syslog,
            name => LogDest::open(LogDest::File(name.to_string()), cfg)?,
        };
        Ok(LogTarget { dest })
    }

    /// Create a new LogTarget.
    pub fn new(d: &str) -> io::Result<LogTarget> {
        LogTarget::new_with(d, &config::get_config())
    }
}

/// initialize global logger.
pub fn logger_init(target: LogTarget) {
    (*LOGGER_.lock()) = Some(Logger::new2(target.dest, true));
    let _ = log::set_logger(&*LOGGER);
}

/// reconfigure global logger.
pub fn logger_reconfig(target: LogTarget) {
    let l = &*LOGGER;
    l.reconfig(target);
}

pub fn logger_flush() {
    let l = &*LOGGER;
    l.flush();
}

/// Get a clone of the incoming.log logger.
pub fn get_incoming_logger() -> Incoming {
    match INCOMING_LOG.read().as_ref() {
        Some(l) => l.clone(),
        None => {
            Incoming {
                logger: Logger::new2(LogDest::Null, false),
            }
        },
    }
}

/// Set the incoming.log logger.
pub fn set_incoming_logger(target: LogTarget) {
    let mut lock = INCOMING_LOG.write();
    if let Some(l) = lock.as_ref() {
        l.logger.reconfig(target);
    } else {
        *lock = Some(Incoming {
            logger: Logger::new(target),
        });
    }
}
