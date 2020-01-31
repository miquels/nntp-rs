//! Spool (article storage) functionality. Supports multiple spool types.
//!
//! Types currently supported:
//!   - diabo

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io;
use std::sync::Arc;
use std::time::Duration;

mod diablo;

use crate::article::Article;
use crate::arttype::ArtType;
use crate::blocking::{BlockingPool, BlockingType};
use crate::config;
use crate::util::Buffer;
use crate::util::{self, HashFeed, MatchResult, UnixTime};

// Faux spoolno's returned by get_spool.
pub const SPOOL_REJECTARTS: u8 = 253;
pub const SPOOL_DONTSTORE: u8 = 254;

/// Which part of the article to process: body/head/all
#[derive(Debug, Clone, Copy, PartialEq)]
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
    fn read(&self, art_loc: &ArtLoc, part: ArtPart, buffer: Buffer) -> io::Result<Buffer>;

    /// Write an article to the spool.
    fn write(&self, headers: Buffer, body: Buffer) -> io::Result<ArtLoc>;

    /// Get the maximum size of this spool.
    fn get_maxsize(&self) -> u64;

    /// Get the timestamp of the oldest article.
    fn get_oldest(&self) -> io::Result<Option<UnixTime>>;

    /// Return JSON version of token.
    fn token_to_json(&self, art_loc: &ArtLoc) -> serde_json::Value;
}

/// Backend storage, e.g. Backend::Diablo, or Backend::Cyclic.
#[derive(Debug,PartialEq,Clone,Copy)]
#[repr(u8)]
#[rustfmt::skip]
pub enum Backend {
    /// Diablo spool
    Diablo = 0,
    /// Cyclic storage
    Cyclic = 1,
    /// Unknown storage (when converting from values >=2)
    Unknown = 15,
    NoSpool = 16,
    DontStore = 17,
    RejectArts = 18,
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

    pub fn name(&self) -> &'static str {
        match self {
            Backend::Diablo => "diablo",
            Backend::Cyclic => "cyclic",
            // the ones below are virtual.
            Backend::Unknown => "unknown",
            Backend::NoSpool => "nospool",
            Backend::DontStore => "dontstore",
            Backend::RejectArts => "rejectarts",
        }
    }
}

/// Article location.
/// A lookup in the history database for a message-id returns this struct,
/// if succesfull
#[derive(Clone)]
#[rustfmt::skip]
pub struct ArtLoc {
    /// Storage type, e.g. Backend::Diablo.
    pub storage_type:   Backend,
    /// On what spool the article lives (0..99).
    pub spool:          u8,
    /// The backend-specific, otherwise opaque, storage-token.
    pub token:          [u8; 16],
    pub toklen:         u8,
}

impl Debug for ArtLoc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let token = &self.token[..self.toklen as usize]
            .iter()
            .fold("0x".to_string(), |acc, x| acc + &format!("{:02x}", x));
        f.debug_struct("ArtLoc")
            .field("storage_type", &self.storage_type.name())
            .field("spool", &self.spool)
            .field("token", &token)
            .finish()
    }
}

impl ArtLoc {
    pub fn to_json(&self, spool: &Spool) -> serde_json::Value {
        let location = spool
            .inner
            .spool
            .get(&self.spool)
            .map(|s| s.backend.token_to_json(self));
        serde_json::json!({
            "type":   self.storage_type.name(),
            "spool":  self.spool,
            "location": location
        })
    }
}

/// Complete spool configuration.
#[derive(Clone,Deserialize,Default,Debug)]
#[rustfmt::skip]
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
#[rustfmt::skip]
pub struct GroupMap {
    pub groups:     String,
    pub spoolgroup: String,
}

#[derive(Clone,Default,Debug)]
#[rustfmt::skip]
struct InnerGroupMap {
    groups:         util::WildMatList,
    spoolgroup:     usize,
}

/// Metaspool is a group of spools.
#[derive(Clone,Deserialize,Default,Debug)]
#[rustfmt::skip]
pub struct MetaSpool {
    /// name of this metaspool.
    pub name:           String,
    /// Article types: control, cancel, binary, base64, yenc etc.
    #[serde(default)]
    pub arttypes:       Vec<String>,
    /// Accept articles that match this metaspool but discard them.
    #[serde(default,deserialize_with = "util::deserialize_bool")]
    pub dontstore:      bool,
    /// Match articles on Newsgroups: line.
    #[serde(default)]
    pub groups:         Vec<String>,
    /// Match articles on the hash of their message-id.
    #[serde(default)]
    pub hashfeed:       HashFeed,
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

    #[doc(hidden)]
    #[serde(skip)]
    pub totweight:      u32,

    #[doc(hidden)]
    #[serde(skip)]
    pub v_arttypes:     Vec<ArtType>,
}

/// Configuration for one spool instance.
#[derive(Deserialize,Default,Debug,Clone)]
#[rustfmt::skip]
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
#[rustfmt::skip]
pub struct Spool {
    pool:       BlockingPool,
    inner:      Arc<SpoolInner>,
}

// Inner stuff.
#[rustfmt::skip]
struct BackendDef {
    backend:    Box<dyn SpoolBackend>,
    weight:     u32,
}

struct SpoolInner {
    spool:     HashMap<u8, BackendDef>,
    metaspool: Vec<MetaSpool>,
    groupmap:  Vec<InnerGroupMap>,
}

impl Spool {
    /// initialize all storage backends.
    pub fn new(spoolcfg: &SpoolCfg, threads: Option<usize>, bt: Option<BlockingType>) -> io::Result<Spool> {
        let mainconfig = config::get_config();

        // very basic check
        if spoolcfg.spoolgroup.len() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no metaspools defined",
            ));
        }

        // parse spool definitions.
        let mut spools = HashMap::new();
        for (num, cfg) in &spoolcfg.spool {
            let n = match num.parse::<u8>() {
                Ok(n) if n < 100 => n,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("[spool.{}]: invalid spool number", num),
                    ));
                },
            };

            // make a copy of the config with spool_no filled in.
            let mut cfg_c = cfg.clone();
            cfg_c.spool_no = n;

            // expand Path.
            cfg_c.path = config::expand_path(&mainconfig.paths, &cfg.path);

            // find the metaspool.
            let ms = match spoolcfg.spoolgroup.iter().find(|m| m.spool.contains(&n)) {
                Some(ms) => ms,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("[spool.{}]: not part of any metaspools", n),
                    ))
                },
            };

            let be = match cfg.backend.as_ref() {
                "diablo" => diablo::DSpool::new(&cfg_c, &ms),
                e => {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("[spool.{}]: unknown backend {}", num, e),
                    ))
                },
            }?;
            let weight = if cfg.weight != 0 {
                cfg.weight
            } else {
                let w = (be.get_maxsize() / 1_000_000_000) as u32;
                if w == 0 {
                    1
                } else {
                    w
                }
            };
            debug!("XXX insert {} into spools, weight {}", n, weight);
            spools.insert(
                n as u8,
                BackendDef {
                    backend: be,
                    weight:  weight,
                },
            );
        }

        // check metaspools, all spools must exist.
        let mut ms = HashMap::new();
        let mut gm = spoolcfg.groupmap.clone();
        let mut spoolgroup = spoolcfg.spoolgroup.clone();
        for i in 0..spoolgroup.len() {
            let mut totweight = 0;
            {
                let m = &spoolgroup[i];
                for n in &m.spool {
                    match spools.get(n) {
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("metaspool {}: spool.{} not defined", m.name, *n),
                            ))
                        },
                        Some(sp) => totweight += sp.weight,
                    }
                }
                for g in &m.groups {
                    gm.push(GroupMap {
                        groups:     g.clone(),
                        spoolgroup: m.name.clone(),
                    });
                }
                ms.insert(m.name.clone(), i);
            }
            spoolgroup[i].totweight = totweight;

            // parse arttypes.
            if spoolgroup[i].arttypes.len() > 0 {
                let m = &mut spoolgroup[i];
                let mut v = Vec::new();
                for w in &m.arttypes {
                    let a = w.parse().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("metaspool {}: arttype {} unknown", m.name, w),
                        )
                    })?;
                    v.push(a);
                }
                m.v_arttypes.append(&mut v);
            }
        }

        // now build the inner spoolgroup list.
        let mut groupmap = Vec::new();
        for m in &gm {
            // find the index of this spoolgroup
            let i = match ms.get(&m.spoolgroup) {
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("dspool.ctl: metaspool {} not defined", m.spoolgroup),
                    ))
                },
                Some(i) => *i,
            };
            groupmap.push(InnerGroupMap {
                groups:     util::WildMatList::new(&m.spoolgroup, &m.groups),
                spoolgroup: i,
            });
        }

        Ok(Spool {
            inner: Arc::new(SpoolInner {
                spool:     spools,
                metaspool: spoolgroup,
                groupmap:  groupmap,
            }),
            pool:  BlockingPool::new(bt, threads.unwrap_or(64)),
        })
    }

    pub fn get_oldest(&self) -> HashMap<u8, UnixTime> {
        let mut res = HashMap::new();
        for (s, b) in &self.inner.spool {
            if let Ok(Some(oldest)) = b.backend.get_oldest() {
                res.insert(*s, oldest);
            }
        }
        res
    }

    pub async fn read(&self, art_loc: ArtLoc, part: ArtPart, buffer: Buffer) -> io::Result<Buffer> {
        let inner = self.inner.clone();
        self.pool
            .spawn_fn(move || {
                use std::thread;
                trace!("spool reader on thread {:?}", thread::current().id());
                let be = match inner.spool.get(&art_loc.spool as &u8) {
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("spool {} not found", art_loc.spool),
                        ));
                    },
                    Some(be) => &be.backend,
                };
                be.read(&art_loc, part, buffer)
            })
            .await
    }

    /// Find out which spool we want to put this article in.
    pub fn get_spool(&self, art: &Article, newsgroups: &[&str]) -> Option<u8> {
        // calculate a hash to use with the weights thing below.
        let hash = util::DHash::hash_str(&art.msgid).as_u64();
        debug!("get_spool: {} hash {}", art.msgid, hash);

        for g in &self.inner.groupmap {
            // if newsgroups matches, try this metaspool.
            if g.groups.matchlist(newsgroups) != MatchResult::Match {
                continue;
            }
            let ms = &self.inner.metaspool[g.spoolgroup];

            // XXX FIXME: match hashfeed.

            if !art.arttype.matches(&ms.v_arttypes) {
                continue;
            }
            if ms.maxsize > 0 && (art.len as u64) > ms.maxsize {
                continue;
            }
            if ms.maxcross > 0 && newsgroups.len() > ms.maxcross as usize {
                continue;
            }

            // special type of spool, blackhole.
            if ms.dontstore {
                return Some(SPOOL_DONTSTORE);
            }
            // special type of spool, reject article.
            if ms.rejectarts {
                return Some(SPOOL_REJECTARTS);
            }
            // if no spools otherwise, no match.
            if ms.spool.len() == 0 {
                debug!("XXX no spools");
                continue;
            }

            // shortcut for simple case.
            if ms.spool.len() == 1 {
                debug!("XXX 1 spools");
                return Some(ms.spool[0]);
            }

            // use weights as a hashfeed of sorts.
            // IMPROVEMENT: use real hashfeed somehow.
            let totweight = ms.totweight;
            let x = ((hash % totweight as u64) & 0xffffffff) as u32;
            let mut a = 0;
            for spoolno in &ms.spool {
                debug!("XXX check spoolno {}", spoolno);
                let sp = self.inner.spool.get(spoolno).unwrap();
                debug!("get_spool: check weight {} <= {} < {}", a, x, a + sp.weight);
                if x >= a && x < a + sp.weight {
                    return Some(*spoolno);
                }
                a += sp.weight;
            }
            // NOTREACHED
        }
        return None;
    }

    /// Write one article to the spool.
    ///
    /// NOTE: we stay as close to wireformat as possible. So
    /// - head_only: body is empty.
    /// - !headonly: body includes hdr/body seperator, ends in .\r\n
    pub async fn write(&self, spoolno: u8, headers: Buffer, body: Buffer) -> Result<ArtLoc, io::Error> {
        let inner = self.inner.clone();
        self.pool
            .spawn_fn(move || {
                use std::thread;
                trace!("spool writer on thread {:?}", thread::current().id());
                let spool = &inner.spool.get(&spoolno).unwrap().backend;
                spool.write(headers, body)
            })
            .await
    }
}
