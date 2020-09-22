//! The main function of this module is the HostCache.
//!
//! Why do we need this? Because it is the habit on NNTP servers to
//! configure access based on hostname. All hostnames in the `newsfeeds`
//! file are forward-resolved and the result (A and AAAA) is cached.
//! Then if a peer connects, we try to find the peers' IP address in the
//! cache. This way we're not dependent on PTR lookups.
//!
use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::stream::StreamExt;
use tokio::task;
use tokio::time::delay_for;
use trust_dns_resolver::{
    config::LookupIpStrategy,
    error::{ResolveError, ResolveErrorKind},
    TokioAsyncResolver,
};

use crate::bus::{self, Notification};
use crate::newsfeeds::NewsFeeds;

const DNS_REFRESH_SECS: Duration = Duration::from_secs(3600);
const DNS_MAX_TEMPERROR_SECS: Duration = Duration::from_secs(86400);

static HOST_CACHE: Lazy<HostCache> = Lazy::new(|| HostCache::new());
static RESOLVER_DONE: AtomicBool = AtomicBool::new(false);
static RESOLVER_OPT: Lazy<Mutex<Option<TokioAsyncResolver>>> = Lazy::new(|| Mutex::new(None));
pub static RESOLVER: Lazy<TokioAsyncResolver> = Lazy::new(|| RESOLVER_OPT.lock().take().unwrap());

// Initialize trust-dns resolver.
pub async fn init_resolver() -> Result<(), ResolveError> {
    if RESOLVER_DONE.load(Ordering::SeqCst) {
        return Ok(());
    }
    RESOLVER_DONE.store(true, Ordering::SeqCst);
    log::trace!("initializing trust-dns-resolver.");
    let (config, mut opts) = trust_dns_resolver::system_conf::read_system_conf()?;
    opts.timeout = Duration::new(1, 0);
    opts.attempts = 5;
    opts.rotate = true;
    opts.edns0 = true;
    opts.use_hosts_file = true;
    opts.ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
    let resolver = TokioAsyncResolver::tokio(config, opts).await?;
    RESOLVER_OPT.lock().replace(resolver);
    Ok(())
}

#[derive(Clone, Default, Debug)]
struct HostEntry {
    label:      String,
    hostname:   String,
    addrs:      Vec<IpAddr>,
    lastupdate: Option<Instant>,
}

// Host cache.
#[derive(Clone)]
pub struct HostCache {
    inner: Arc<Mutex<HostCacheInner>>,
}

#[derive(Debug)]
struct HostCacheInner {
    generation: u64,
    updating:   bool,
    entries:    Vec<HostEntry>,
}

impl HostCache {
    // Initialize a new HostCache instance.
    fn new() -> HostCache {
        let inner = HostCacheInner {
            generation: 0,
            updating:   false,
            entries:    Vec::new(),
        };
        let hc = HostCache {
            inner: Arc::new(Mutex::new(inner)),
        };
        hc
    }

    /// Get an instance of the host cache. This is a reference,
    pub fn get() -> HostCache {
        HOST_CACHE.clone()
    }

    /// new NewsFeed struct, add/delete entries.
    pub fn update(&self, feeds: &NewsFeeds) {
        let mut inner = self.inner.lock();

        // First empty the list and put all entries in a temp HashMap.
        let mut hm = HashMap::new();
        for entry in inner.entries.drain(..) {
            hm.insert(entry.hostname.clone(), Some(entry));
        }

        // Now walk over all configured hostnames, if it's in the
        // temp hashmap put it back on the list, otherwise add empty entry.
        let iter = feeds
            .peers
            .iter()
            .map(|p| p.accept_from.iter().map(move |h| (h, &p.label)))
            .flatten();
        for (host, label) in iter {
            match hm.remove(host) {
                Some(Some(entry)) => inner.entries.push(entry),
                Some(None) => {},
                None => {
                    inner.entries.push(HostEntry {
                        label:      label.to_string(),
                        hostname:   host.clone(),
                        addrs:      Vec::new(),
                        lastupdate: None,
                    });
                    hm.insert(host.clone(), None);
                },
            }
        }
        inner.generation += 1;
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

    // Spawn the resolver task.
    pub async fn start(mut bus_recv: bus::Receiver) -> Result<(), ResolveError> {
        log::debug!("HostCache::start: initializing");
        init_resolver().await?;

        let this = Self::get().clone();
        task::spawn(async move {
            this.resolve(true).await;
        });

        let this = Self::get().clone();
        task::spawn(async move {
            loop {
                tokio::select! {
                    _ = delay_for(Duration::from_secs(60)) => {
                        this.resolve(false).await;
                    }
                    item = bus_recv.recv() => {
                        match item {
                            Some(Notification::ExitGraceful) => break,
                            Some(Notification::ExitNow) => break,
                            Some(Notification::Reconfigure) => this.resolve(true).await,
                            Some(_) => {},
                            None => break,
                        }
                    }
                }
            }
            log::debug!("resolver_task: shutting down");
        });
        Ok(())
    }

    // Walk over all hostentries that we have, and see if any of them
    // need refreshing. Ignores transient errors.
    async fn resolve(&self, force: bool) {
        let (mut generation, mut entries) = {
            let mut inner = self.inner.lock();
            if inner.updating {
                return;
            }
            // See if any entries need to be refreshed.
            let now = Instant::now();
            let mut refresh = false;
            for e in &inner.entries {
                if needs_update(e, &now) {
                    refresh = true;
                    break;
                }
            }
            if !refresh && !force {
                // Nope.
                return;
            }
            inner.updating = true;
            (inner.generation, inner.entries.clone())
        };

        let resolver = &*RESOLVER;
        let mut updated = HashMap::new();

        loop {
            // We have a clone of the `entries` Vec. Check for each entry
            // if an update is needed. Store the update in the `updated` map.
            let now = Instant::now();
            let mut tasks = futures::stream::FuturesUnordered::new();
            let mut delay_ms = 0;
            for entry in &entries {
                if updated.contains_key(&entry.hostname) || !needs_update(entry, &now) {
                    continue;
                }
                let delay = Duration::from_millis(delay_ms);
                delay_ms += 2;

                // Run the host lookups in parallel because why not.
                let task = async move {
                    // Space a few ms between lookups.
                    tokio::time::delay_for(delay).await;

                    // Lookup "hostname".
                    log::debug!("Refreshing host cache for {}", entry.hostname);
                    let start = Instant::now();
                    let res = resolver.lookup_ip(entry.hostname.as_str()).await;
                    let elapsed = start.elapsed();
                    let elapsed_ms = elapsed.as_millis();
                    if elapsed_ms >= 1500 {
                        let elapsed = (elapsed_ms / 100) as f64 / 10f64;
                        log::warn!("resolver: lookup {}: took {} seconds", entry.hostname, elapsed);
                    }
                    let mut entry = entry.clone();

                    match res {
                        Ok(a) => {
                            let addrs: Vec<_> = a.iter().collect();
                            if addrs.len() == 0 {
                                // should not happen. log and handle as transient error.
                                log::warn!("resolver: lookup {}: OK, but 0 results?!", entry.hostname);
                            } else {
                                entry.addrs = addrs;
                                entry.lastupdate = Some(start);
                            }
                        },
                        Err(e) => {
                            match e.kind() {
                                // NXDOMAIN or NODATA - normal retry time.
                                ResolveErrorKind::NoRecordsFound { .. } => {
                                    log::warn!("resolver: lookup {}: host not found", entry.hostname);
                                    entry.addrs.truncate(0);
                                    entry.lastupdate = Some(start);
                                },
                                // Transient error, retry soon.
                                _ => {
                                    log::warn!("resolver: lookup {}: {}", entry.hostname, e);
                                    if elapsed >= DNS_MAX_TEMPERROR_SECS {
                                        entry.addrs.truncate(0);
                                    }
                                    entry.lastupdate = Some(start);
                                },
                            }
                        },
                    }
                    entry
                };
                tasks.push(task);
            }

            // Store the updated version in the hashmap.
            while let Some(entry) = tasks.next().await {
                updated.insert(entry.hostname.clone(), entry);
            }
            drop(tasks);

            {
                // All updated entries are now present in `updated`.
                // Patch the actual entries.
                let mut inner = self.inner.lock();
                for entry in inner.entries.iter_mut() {
                    if let Some(e) = updated.get(&entry.hostname) {
                        entry.addrs = e.addrs.clone();
                        entry.lastupdate = e.lastupdate.clone();
                    }
                }

                // Now if the generation did not change, we're done.
                if generation == inner.generation {
                    inner.updating = false;
                    break;
                }

                // Loop once more.
                generation = inner.generation;
                entries = inner.entries.clone();
            }
        }
    }
}

fn needs_update(entry: &HostEntry, now: &Instant) -> bool {
    match entry.lastupdate {
        Some(t) => now.saturating_duration_since(t) >= DNS_REFRESH_SECS,
        None => true,
    }
}

/*
/// On drop, send a quit message to the resolver thread.
impl Drop for HostCache {
    fn drop(&mut self) {
        let inner = self.inner.lock();
        inner.tx.send(Message::Quit).ok();
    }
}
*/

impl fmt::Debug for HostCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let inner = self.inner.lock();
        inner.fmt(f)
    }
}
