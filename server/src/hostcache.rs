//! HostCache, a DNS lookup cache.
//!
use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::net::IpAddr;
use std::sync::{Arc,mpsc};
use std::time::Duration;
use std::thread;

use dns_lookup::{self,AddrInfoHints,SockType};
use nntp_rs_util as util;
use parking_lot::Mutex;

use newsfeeds::NewsFeeds;

const DNS_REFRESH_SECS : u64 = 3600;
const DNS_MAX_TEMPERROR_SECS : u64 = 86400;

#[derive(Clone,Default,Debug)]
struct HostEntry {
    label:      String,
    hostname:   String,
    addrs:      Vec<IpAddr>,
    lastupdate:  u64,
    valid:      bool,
}

// Host cache.
#[derive(Clone)]
pub struct HostCache {
    inner:      Arc<Mutex<HostCacheInner>>,
}

#[derive(Debug)]
struct HostCacheInner {
    generation: u64,
    entries:    Vec<HostEntry>,
    entry_map:  HashMap<String, usize>,
    tx:         mpsc::Sender<Message>,
}

enum Message {
    Update,
    Quit,
}

impl HostCache {
    /// Initialize a new HostCache instance.
    pub fn new() -> HostCache {
        let (tx, rx) = mpsc::channel();
        let inner = HostCacheInner{
            tx:         tx,
            generation: 1,
            entries:    Vec::new(),
            entry_map:  HashMap::new(),
        };
        let hc = HostCache {
            inner:  Arc::new(Mutex::new(inner))
        };
        let hc2 = hc.clone();
        thread::spawn(move || {
            hc2.resolver_thread(rx);
        });
        hc
    }

    /// add entries to the cache with data from a (new) NewsFeeds struct.
    pub fn update(&self, feeds: &NewsFeeds) {

        let mut inner = self.inner.lock();
        let first = inner.entries.is_empty();

        // first mark all entries as invalid.
        for h in inner.entries.iter_mut() {
            h.valid = false;
        }

        // walk over all peers.
        for p in &feeds.peers {

            // and all hostnames
            for h in &p.inhost {

                // Update or add entry.
                let idx = inner.entry_map.get(h).map(|x| *x);
                match idx {
                    Some(idx) => inner.entries[idx].valid = true,
                    None => {
                        inner.entries.push(HostEntry{
                            label:      p.label.clone(),
                            hostname:   h.clone(),
                            addrs:      Vec::new(),
                            lastupdate: 0,
                            valid:      true,
                        });
                    }
                }
            }
        }

        // Build new entry list.
        let entries : Vec<HostEntry> = inner.entries.iter().cloned().filter(|e| e.valid).collect();
        inner.entry_map.clear();
        for (idx, e) in entries.iter().enumerate() {
            inner.entry_map.insert(e.hostname.clone(), idx);
        }
        inner.entries = entries;

        // Update the entries.
        if first {
            drop(inner);
            self.resolve();
        } else {
            inner.tx.send(Message::Update).unwrap();
        }
    }

    /// Find a host in the cache. Returns label.
    pub fn lookup(&self, ip: &IpAddr) -> Option<String> {
        let inner = self.inner.lock();
        for e in &inner.entries {
            for a in &e.addrs {
                if ip == a {
                    return Some(e.label.clone());
                }
            }
        }
        None
    }

    // This is the resolver. It runs in a seperate thread,
    fn resolver_thread(&self, rx: mpsc::Receiver<Message>) {

        loop {
            // wait for a message, or a timeout (every minute).
            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok(Message::Quit) => return,
                Ok(Message::Update) => {},
                Err(mpsc::RecvTimeoutError::Timeout) => {},
                Err(e) => {
                    error!("resolver: got error on channel: {} - FATAL", e);
                    return;
                },
            }

            self.resolve();
        }
    }

    // Walk over all hostentries that we have, and see if any of them
    // need refreshing. Ignores transient errors.
    fn resolve(&self) {

        let hints = AddrInfoHints{
            socktype:   SockType::Stream.into(),
            .. AddrInfoHints::default()
        };

        let mut generation = 0;
        let mut len = 0;
        let mut idx = 0;

        // check all entries.
        loop {

            // Critical section.
            let (host, o_addrs, lastupdate) = {

                let mut inner = self.inner.lock();

                // retry if changed.
                if generation != inner.generation {
                    idx = 0;
                    len = inner.entries.len();
                    generation = inner.generation;
                }

                // see if any entries need updating.
                let now = util::unixtime();
                while idx < len {
                    if inner.entries[idx].lastupdate < now - DNS_REFRESH_SECS {
                        break;
                    }
                    idx += 1;
                }

                // if not, break.
                if idx == len {
                    break;
                }

                let entry = &inner.entries[idx];
                (entry.hostname.clone(), entry.addrs.clone(), entry.lastupdate)
            };

            // We are not locked here anymore. Lookup "hostname".
            let (mut addrs, mut lastupdate) = match dns_lookup::getaddrinfo(Some(&host), None, Some(hints)) {
                Ok(a) => {
                    let addrs = a.filter(|a| a.is_ok())
                        .map(|a| a.unwrap().sockaddr.ip())
                        .collect::<Vec<_>>();
                    if addrs.len() == 0 {
                        // should not happen. log and handle as transient error.
                        warn!("resolver: lookup {}: OK, but 0 results?!", host);
                        (o_addrs, lastupdate)
                    } else {
                        (addrs, util::unixtime())
                    }
                },
                Err(e) => {
                    // XXX FIXME as soon as dns-lookup exposes LookupErrorKind.
                    // https://github.com/keeperofdakeys/dns-lookup/issues/10
                    /*
                    match e.kind() {
                        // NXDOMAIN or NODATA - normal retry time.
                        LookupErrorKind::NoName|
                        LookupErrorKind::NoData => {
                            warn!("resolver: lookup {}: host not found", host);
                            (Vec::new(), util::unixtime())
                        },
                        // Transient error, retry soon.
                        _ => {
                    */
                            warn!("resolver: lookup {}: {:?}", host, e);
                            (o_addrs, 0)
                    /*
                        },
                    }
                    */
                }
            };

            let now = util::unixtime();
            if lastupdate < now - DNS_MAX_TEMPERROR_SECS {
                addrs.truncate(0);
                lastupdate = 0;
            }

            // Critical section.
            {
                let mut inner = self.inner.lock();

                // only update if data didn't change under us.
                if generation == inner.generation {
                    inner.entries[idx].lastupdate = lastupdate;
                    inner.entries[idx].addrs = addrs;
                }
            }

            idx += 1;
        }
    }
}

/// On drop, send a quit message to the resolver thread.
impl Drop for HostCache {
    fn drop(&mut self) {
        let inner = self.inner.lock();
        inner.tx.send(Message::Quit).ok();
    }
}

impl fmt::Debug for HostCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let inner = self.inner.lock();
        inner.fmt(f)
    }
}
