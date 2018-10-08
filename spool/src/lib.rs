//! Spool (article storage) functionality. Supports multiple spool types.
//!
//! Types currently supported:
//!   - diabo

#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate byteorder;
extern crate bytes;
extern crate futures_cpupool;
extern crate futures;
extern crate libc;
extern crate nntp_rs_util as util;
extern crate parking_lot;
extern crate serde;
extern crate time;

use std::io;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_cpupool::CpuPool;
use futures::{Future,future};

mod diablo;

/// Which part of the article to process: body/head/all
#[derive(Debug,Clone,Copy,PartialEq)]
pub enum ArtPart {
    Article,
    Body,
    Head,
    Stat,
}

/// Trait implemented by all spool backends.
pub trait SpoolBackend: Send + Sync {
    /// Get the type. E.g. Backend::Diablo.
    fn get_type(&self) -> Backend;

    /// Read one article from the spool.
    fn read(&self, art_loc: &ArtLoc, part: ArtPart, buf: &mut BytesMut) -> io::Result<()>;

    /// Write an article to the spool.
    fn write(&self, headers: &[u8], body: &[u8]) -> io::Result<ArtLoc>;

    /// Get the maximum size of this spool.
    fn get_maxsize(&self) -> u64;
}

/// Article location.
/// A lookup in the history database for a message-id returns this struct,
/// if succesfull
#[derive(Clone)]
pub struct ArtLoc {
    /// Storage type, e.g. Backend::Diablo.
    pub storage_type:   Backend,
    /// On what spool the article lives (0..99).
    pub spool:          u8,
    /// The backend-specific, otherwise opaque, storage-token.
    pub token:          Vec<u8>,
}

impl std::fmt::Debug for ArtLoc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ArtLoc {{ storage_type: {:?}, spool: {}, token: [", self.storage_type, self.spool)?;
        for b in &self.token {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "] }}")
    }
}


/// Backend storage, e.g. Backend::Diablo, or Backend::Cyclic.
#[derive(Debug,PartialEq,Clone,Copy)]
#[repr(u8)]
pub enum Backend {
    /// Diablo spool
    Diablo = 0,
    /// Cyclic storage
    Cyclic = 1,
    /// Unknown storage (when converting from values >=2)
    Unknown = 15,
}

impl Backend {
    /// convert u8 to enum.
    pub fn from_u8(t: u8) -> Backend {
        match t {
            0 => Backend::Diablo,
            1 => Backend::Cyclic,
            _ => Backend::Unknown,
        }
    }
}

/// Complete spool configuration.
#[derive(Clone,Deserialize,Default,Debug)]
pub struct SpoolCfg {
    /// Map of spools. Index is a number 0..99.
    #[serde(default)]
    pub spool:      HashMap<String, SpoolDef>,
    /// List of spool groups (metaspool in diablo).
    #[serde(default)]
    pub spoolgroup: Vec<MetaSpool>,
    #[serde(skip)]
    #[doc(hidden)]
    // used internally, and by dspool.ctl.
    pub groupmap:   Vec<GroupMap>,
}

// used internally, and by dspool.ctl.
#[doc(hidden)]
#[derive(Clone,Default,Debug)]
pub struct GroupMap {
    pub groups:     String,
    pub spoolgroup: String,
}

#[derive(Clone,Default,Debug)]
struct InnerGroupMap {
    groups:         util::WildMatList,
    spoolgroup:     usize,
}

/// Metaspool is a group of spools.
#[derive(Clone,Deserialize,Default,Debug)]
pub struct MetaSpool {
    /// name of this metaspool.
    pub name:           String,
    /// Article types: control, cancel, binary, base64, yenc etc.
    #[serde(default)]
    pub arttypes:       String,
    /// Accept articles that match this metaspool but discard them.
    #[serde(default,deserialize_with = "util::deserialize_bool")]
    pub dontstore:      bool,
    /// Match articles on Newsgroups: line.
    #[serde(default)]
    pub groups:         Vec<String>,
    /// Match articles on the hash of their message-id.
    #[serde(default)]
    pub hashfeed:       String,
    /// Match based on where we received this article from (label from newsfeeds file).
    #[serde(default)]
    pub label:          Vec<String>,
    /// Match articles if they are smaller than N
    #[serde(default,deserialize_with = "util::deserialize_size")]
    pub maxsize:        u64,
    /// Match articles if they are crossposted in less than N groups
    #[serde(default)]
    pub maxcross:       u32,
    /// diablo: spool reallocation interval (default 10m).
    #[serde(default,deserialize_with = "util::deserialize_duration")]
    pub reallocint:     Duration,
    /// Spools in this metaspool
    pub spool:          Vec<u8>,
    /// Reject articles that match this metaspool.
    #[serde(default,deserialize_with = "util::deserialize_bool")]
    pub rejectarts:     bool,
}

/// Configuration for one spool instance.
#[derive(Deserialize,Default,Debug,Clone)]
pub struct SpoolDef {
    /// Backend to use: diablo, cyclic.
    pub backend:    String,
    /// Path to directory (for diablo) or file/blockdev (for cyclic)
    pub path:       String,
    /// Weight of this spool.
    #[serde(default)]
    pub weight:     u32,
    /// diablo: minimum free diskspace (K/KB/KiB/M/MB/MiB/G/GB/GiB)
    #[serde(default, deserialize_with = "util::deserialize_size")]
    pub minfree:    u64,
    /// diablo: maximum diskspace in use for this spool object (K/KB/KiB/M/MB/MiB/G/GB/GiB)
    #[serde(default, deserialize_with = "util::deserialize_size")]
    pub maxsize:    u64,
    /// diablo: amount of time to keep articles (seconds, or suffix with s/m/h/d).
    #[serde(default, deserialize_with = "util::deserialize_duration")]
    pub keeptime:   Duration,

    #[serde(skip)]
    pub spool_no:   u8,
}

/// Article storage (spool) functionality.
#[derive(Clone)]
pub struct Spool {
    cpu_pool:   CpuPool,
    inner:      Arc<SpoolInner>,
}

// Inner stuff.
struct SpoolInner {
    spool:      HashMap<u8, Box<SpoolBackend>>,
    metaspool:  Vec<MetaSpool>,
    groupmap:   Vec<InnerGroupMap>,
}

impl Spool {
    /// initialize all storage backends.
    pub fn new(spoolcfg: &SpoolCfg) -> io::Result<Spool> {

        // parse spool definitions.
        let mut spools = HashMap::new();
        for (num, cfg) in &spoolcfg.spool {
            let n = match num.parse::<u8>() {
                Ok(n) if n < 100 => n,
                _ => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("[spool.{}]: invalid spool number", num)));
                },
            };

            // make a copy of the config with spool_no filled in.
            let mut cfg_c = cfg.clone();
            cfg_c.spool_no = n;

            // find the metaspool.
            let ms = match spoolcfg.spoolgroup.iter().find(|m| m.spool.contains(&n)) {
                Some(ms) => ms,
                None => return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("[spool.{}]: not part of any metaspools", n))),
            };

            let be = match cfg.backend.as_ref() {
                "diablo" => {
                    diablo::DSpool::new(&cfg_c, &ms)
                },
                e => {
                    Err(io::Error::new(io::ErrorKind::InvalidData,
                                   format!("[spool.{}]: unknown backend {}", num, e)))
                },
            }?;
            spools.insert(n as u8, be);
        }

        // check metaspools, all spools must exist.
        let mut ms = HashMap::new();
        let mut gm = spoolcfg.groupmap.clone();
        for i in 0..spoolcfg.spoolgroup.len() {
            let m = &spoolcfg.spoolgroup[i];
            for n in &m.spool {
                if !spools.contains_key(n) {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                   format!("metaspool {}: spool.{} not defined", m.name, *n)));
                }
            }
            for g in &m.groups {
                gm.push(GroupMap{ groups: g.clone(), spoolgroup: m.name.clone() });
            }
            ms.insert(m.name.clone(), i);
        }

        // now build the inner spoolgroup list.
        let mut groupmap = Vec::new();
        for m in &gm {
            // find the index of this spoolgroup
            let i = match ms.get(&m.spoolgroup) {
                None => return Err(io::Error::new(io::ErrorKind::InvalidData,
                                    format!("dspool.ctl: metaspool {} not defined", m.spoolgroup))),
                Some(i) => *i,
            };
            groupmap.push(InnerGroupMap{
                groups:     util::WildMatList::new(&m.spoolgroup, &m.groups),
                spoolgroup: i,
            });
        }

        let mut builder = futures_cpupool::Builder::new();
        builder.name_prefix("spool-");
        builder.pool_size(64);

        Ok(Spool{
            inner: Arc::new(SpoolInner{
                spool:      spools,
                metaspool:  spoolcfg.spoolgroup.clone(),
                groupmap:   groupmap,
            }),
            cpu_pool:   builder.create(),
        })
    }

    pub fn read(&self, art_loc: ArtLoc, part: ArtPart, mut buf: BytesMut) -> impl Future<Item=BytesMut, Error=io::Error> + Send {
        let inner = self.inner.clone();
        self.cpu_pool.spawn_fn(move || {
            use std::thread;
            trace!("spool reader on thread {:?}", thread::current().id());
            let be = match inner.spool.get(&art_loc.spool as &u8) {
                None => {
                    return future::err(io::Error::new(io::ErrorKind::NotFound,
                                   format!("spool {} not found", art_loc.spool)));
                },
                Some(be) => be,
            };
            match be.read(&art_loc, part, &mut buf) {
                Ok(()) => future::ok(buf),
                Err(e) => future::err(e),
            }
        })
    }

    /// Write one article to the spool.
    ///
    /// NOTE: we stay as close to wireformat as possible. So
    /// - head_only: body is empty.
    /// - !headonly: body includes hdr/body seperator, ends in .\r\n
    pub fn write(&self, headers: BytesMut, body: BytesMut) -> impl Future<Item=ArtLoc, Error=io::Error> + Send {

        //if self.inner.metaspool.len() == 0 {
        //    return Err(io::Error::new(io::ErrorKind::InvalidData, "no metaspools defined"));
        //}

        // Here we should select the correct metaspool for the article.

        let inner = self.inner.clone();
        self.cpu_pool.spawn_fn(move || {
            use std::thread;
            trace!("spool writer on thread {:?}", thread::current().id());
            let spool = inner.spool.get(&0).unwrap();
            spool.write(&headers[..], &body[..])
        })
    }
}

