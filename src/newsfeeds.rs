//! The newfeeds{ .. } section contains a list of all NNTP peers.
//!
//! It is used for a couple of things:
//!
//! - When a peer connects. The peer is looked up in the newsfeed list
//!   by IP address. If it's not listed, access is denied.
//!
//! - When the peer sends an article. If it is allowed to send articles,
//!   the article is checked against "filter-groups" to see
//!   if we want it. If not, it is rejected. Otherwise it is sent to
//!   the `spool` code to be stored somewhere.
//!
//! - After an article is received by any peer, the list of peers is
//!   checked to see which ones of them want to receive that article.
//!   For those the article will be queued to be sent to the peer.
//!
use std::collections::HashMap;
use std::default::Default;
use std::io;
use std::mem;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use smartstring::alias::String as SmartString;

use crate::article::Article;
use crate::arttype::ArtType;
use crate::config;
use crate::dns::HostCache;
use crate::util::{self, CongestionControl, HashFeed, MatchList, MatchResult, UnixTime, WildMatList};

use ipnet::IpNet;

/// A complete newsfeeds file.
#[rustfmt::skip]
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct NewsFeeds {
    /// Full templates to be referenced as defaults in peers.
    #[serde(rename = "template")]
    pub templates:          Vec<NewsPeer>,
    /// Group definition the can referenced in a "groups" entry in a peer.
    #[serde(rename = "groupdef")]
    pub groupdefs:          Vec<WildMatList>,
    /// Peer definitions.
    #[serde(rename = "peer")]
    pub peers:              Vec<NewsPeer>,
    /// Input filter, applied on all incoming feeds.
    #[serde(rename = "input-filter")]
    pub infilter:           Option<NewsPeer>,

    // The below are all non-configfile items.

    // timestamp of file when we loaded this data
    #[serde(skip)]
    pub(crate) timestamp:   UnixTime,
    // HostCache for all hostname entries.
    #[serde(skip)]
    hcache:                 HostCache,
}

impl Default for NewsFeeds {
    fn default() -> NewsFeeds {
        NewsFeeds {
            templates: Vec::new(),
            groupdefs: Vec::new(),
            peers:     Vec::new(),
            timestamp: UnixTime::now(),
            infilter:  None,
            hcache:    HostCache::get(),
        }
    }
}

impl NewsFeeds {
    /// Initialize the host cache.
    pub fn init_hostcache(&mut self) {
        for e in self.peers.iter_mut() {
            let mut hosts = Vec::new();
            for h in &e.accept_from {
                if let Ok(ipaddr) = h.parse::<IpAddr>() {
                    e.innet.push(ipaddr.into());
                } else {
                    match IpNet::from_str(h) {
                        Ok(net) => e.innet.push(net),
                        Err(_) => hosts.push(h.clone()),
                    }
                }
            }
            e.accept_from = hosts;
        }
        self.hcache.update(&self);
    }

    /// Check all peers to see if one of them is actually ourself.
    pub fn check_self(&mut self, cfg: &config::Config) {
        for e in self.peers.iter_mut() {
            for pathident in &cfg.server.path_identity {
                if e.path_identity.matches(pathident) == MatchResult::Match {
                    e.is_self = true;
                    break;
                }
            }
        }
    }

    /// Setup %XCLIENT / xclient.
    pub fn setup_xclient(&mut self) {
        for e in self.peers.iter_mut() {
            if e.label.as_str() == "%XCLIENT" {
                e.xclient = true;
            } else if e.xclient {
                e.label = "%XCLIENT".into()
            }
        }
    }

    /// Update the hostcache, after changes to self.
    pub fn update_hostcache(&self) {
        self.hcache.update(&self);
    }

    /// resolve references in WildMatLists.
    pub fn resolve_references(&mut self) {
        let mut empty = WildMatList::default();
        for idx in 0..self.peers.len() {
            let mut this = mem::replace(&mut self.peers[idx].groups, empty);
            this.resolve(&self.groupdefs);
            empty = mem::replace(&mut self.peers[idx].groups, this);
        }
        for idx in 0..self.groupdefs.len() {
            let mut this = mem::replace(&mut self.groupdefs[idx], empty);
            this.resolve(&self.groupdefs);
            empty = mem::replace(&mut self.groupdefs[idx], this);
        }
    }

    /// Merge templates into newsfeeds.
    pub fn merge_templates(&mut self) -> io::Result<()> {
        for idx in 0..self.peers.len() {
            for tmpl_idx in (0 .. self.peers[idx].templates.len()).rev() {
                let tmpl_name = &self.peers[idx].templates[tmpl_idx];
                let template = match self.templates.iter().find(|t| &t.label == tmpl_name) {
                    Some(t) => t,
                    None => return Err(ioerr!(NotFound, "peer {}: template {}: no such template",
                                              self.peers[idx].label, tmpl_name)),
                };
                self.peers[idx].merge_template(template);
            }
        }
        Ok(())
    }

    /// If the "hostname" field was set, use it to set defaults
    /// for outhost, path_identity and accept_from.
    pub fn set_hostname_default(&mut self) {
        for peer in self.peers.iter_mut() {
            peer.hostname = peer.hostname.replace("${label}", peer.label.as_str());
            if peer.hostname != "" {
                if peer.outhost == "" {
                    peer.outhost = peer.hostname.clone();
                }
                if peer.path_identity.matches(&peer.hostname) != MatchResult::Match {
                    let h = peer.hostname.clone();
                    peer.path_identity.push(h);
                }
                if !peer.accept_from.contains(&peer.hostname) {
                    let h = peer.hostname.clone();
                    peer.accept_from.push(h);
                }
            }
        }
    }

    /// Look up a peer by IP address.
    ///
    /// Returns a tuple consisting of the index into the peers vector
    /// and a reference to the NewsPeer instance.
    pub fn find_peer(&self, ipaddr: &IpAddr) -> Option<(usize, &NewsPeer)> {
        // hostcache lookup.
        let hcache_name = self.hcache.lookup(ipaddr);
        let hcache_name = hcache_name.as_ref().map(|name| name.as_str());

        // walk over the peers one by one (instead of using a hashmap or some other
        // optimization) since we want to match the entries in the order they
        // were put in the config file.
        for idx in 0..self.peers.len() {
            let peer = &self.peers[idx];
            if let Some(name) = hcache_name {
                if name == peer.label {
                    return Some((idx, peer));
                }
            }
            for net in &peer.innet {
                if net.contains(ipaddr) {
                    return Some((idx, peer));
                }
            }
        }
        None
    }
}

/// Definition of a newspeer.
#[rustfmt::skip]
#[derive(Default,Debug,Clone,Deserialize)]
#[serde(default = "NewsPeer::new")]
pub struct NewsPeer {
    /// Name of this feed.
    #[serde(rename = "__label__")]
    pub label:              SmartString,

    /// Default template(s) to use.
    #[serde(rename = "template")]
    pub templates:          Vec<String>,

    // if set, sets accept_from, outhost, and path-identity in dnewsfeeds.
    pub(crate) hostname:     String,

    /// used both to filter incoming and outgoing articles.
    #[serde(rename = "path-identity")]
    pub path_identity:      WildMatList,

    /// used on connects from remote host
    #[serde(rename = "accept-from")]
    pub accept_from:        Vec<String>,
    pub innet:              Vec<IpNet>,
    #[serde(rename = "max-connections-in")]
    pub maxconnect:         u32,
    #[serde(rename = "accept-headfeed")]
    pub accept_headfeed:    bool,
    pub readonly:           bool,
    pub xclient:            bool,

    /// used when processing incoming articles
    #[serde(rename = "filter-groups")]
    pub filter_groups:      WildMatList,
    #[serde(rename = "ignore-path-mismatch")]
    pub ignore_path_mismatch:         bool,
    #[serde(rename = "dont-defer")]
    pub dont_defer:         bool,

    /// used to select outgoing articles.
    pub groups:             WildMatList,
    #[serde(rename = "groups-required")]
    pub requiregroups:      WildMatList,
    pub distributions:      WildMatList,
    pub hashfeed:           HashFeed,

    /// used with the outgoing feed.
    #[serde(rename = "send-to")]
    pub outhost:            String,
    #[serde(rename = "bind-address")]
    pub bindaddress:        Option<IpAddr>,
    pub port:               u16,
    #[serde(rename = "sendbuf-size", deserialize_with = "util::option_deserialize_size")]
    pub sendbuf_size:       Option<u64>,
    #[serde(rename = "congestion-control")]
    pub congestion_control: Option<CongestionControl>,
    #[serde(rename = "max-pacing-rate")]
    pub max_pacing_rate:    Option<u32>,

    #[serde(rename = "min-crosspost")]
    pub mincross:           u32,
    #[serde(rename = "max-crosspost")]
    pub maxcross:           u32,
    #[serde(rename = "max-path-length")]
    pub maxpath:            u32,
    #[serde(rename = "min-path-length")]
    pub minpath:            u32,
    #[serde(rename = "max-article-size",deserialize_with = "util::deserialize_size")]
    pub maxsize:            u64,
    #[serde(rename = "min-article-size",deserialize_with = "util::deserialize_size")]
    pub minsize:            u64,
    #[serde(rename = "article-types")]
    pub arttypes:           Vec<ArtType>,
    #[serde(rename = "max-connections-out")]
    pub maxparallel:        u32,
    #[serde(rename = "max-inflight")]
    pub maxstream:          u32,
    #[serde(rename = "delay-feed", deserialize_with = "util::deserialize_duration")]
    pub delay_feed:         Duration,
    #[serde(rename = "no-backlog")]
    pub no_backlog:         bool,
    #[serde(rename = "drop-deferred")]
    pub drop_deferred:      bool,
    #[serde(rename = "max-backlog-queue")]
    pub maxqueue:           u32,
    #[serde(rename = "send-headfeed")]
    pub send_headfeed:      bool,
    #[serde(rename = "preserve-bytes")]
    pub preservebytes:      bool,
    #[serde(rename = "queue-only")]
    pub queue_only:         bool,

    /// metadata for non-feed related things.
    pub meta:               HashMap<String, Vec<String>>,

    #[serde(skip)]
    pub is_self:            bool,
}

impl NewsPeer {
    /// Get a fresh NewsPeer with defaults set.
    pub fn new() -> NewsPeer {
        NewsPeer {
            port: 119,
            maxparallel: 2,
            maxstream: 100,
            ..Default::default()
        }
    }

    /// Merge in a template.
    pub fn merge_template(&mut self, tmpl: &NewsPeer) {
        // hostname. only if it contains a variable.
        if self.hostname == "" && tmpl.hostname.contains("${") {
            self.hostname = tmpl.hostname.clone();
        }

        // max-connections-in.
        if self.maxconnect == 0 {
            self.maxconnect = tmpl.maxconnect;
        }

        // accept-headfeed.
        if tmpl.accept_headfeed {
            self.accept_headfeed = tmpl.accept_headfeed;
        }

        // readonly.
        if tmpl.readonly {
            self.readonly = true;
        }

        // filter-groups.
        self.filter_groups.prepend(&tmpl.filter_groups);

        // groups.
        self.groups.prepend(&tmpl.groups);

        // groups-required.
        self.requiregroups.prepend(&tmpl.requiregroups);

        // distributions.
        self.distributions.prepend(&tmpl.distributions);

        // meta.
        for (k, v) in &tmpl.meta {
            if !self.meta.contains_key(k) {
                self.meta.insert(k.to_owned(), v.to_owned());
            }
        }

        // crosspost.
        if self.mincross == 0 {
            self.mincross = tmpl.mincross;
        }
        if self.maxcross == 0 {
            self.maxcross = tmpl.maxcross;
        }

        // path-length.
        if self.minpath == 0 {
            self.minpath = tmpl.minpath;
        }
        if self.maxpath == 0 {
            self.maxpath = tmpl.maxpath;
        }

        // size.
        if self.minsize == 0 {
            self.minsize = tmpl.minsize;
        }
        if self.maxsize == 0 {
            self.maxsize = tmpl.maxsize;
        }

        // article-types.
        if tmpl.arttypes.len() > 0 {
            let mut v = tmpl.arttypes.clone();
            v.extend(self.arttypes.drain(..));
            self.arttypes = v;
        }

        // max-connections-out.
        if self.maxparallel == 0 {
            self.maxparallel = tmpl.maxparallel;
        }

        // max-streaming-queue-size.
        if self.maxstream == 0 {
            self.maxstream = tmpl.maxstream;
        }

        // delay-feed.
        if self.delay_feed.as_secs() == 0 {
            self.delay_feed = tmpl.delay_feed.clone();
        }

        // no-backlog.
        if tmpl.no_backlog {
            self.no_backlog = tmpl.no_backlog;
        }

        // drop-deferred.
        if tmpl.drop_deferred {
            self.drop_deferred = tmpl.drop_deferred;
        }

        // max-backlog-queue.
        if self.maxqueue == 0 {
            self.maxqueue = tmpl.maxqueue;
        }

        // send-headfeed.
        if tmpl.send_headfeed {
            self.send_headfeed = tmpl.send_headfeed;
        }

        // preserve-bytes.
        if tmpl.preservebytes {
            self.preservebytes = tmpl.preservebytes;
        }

        // queue-only.
        if tmpl.queue_only {
            self.queue_only = tmpl.queue_only;
        }
    }

    ///
    /// Check if this peer wants to have this article.
    pub fn wants(
        &self,
        art: &Article,
        hashfeed: &HashFeed,
        path: &[&str],
        newsgroups: &mut MatchList,
        dist: Option<&Vec<&str>>,
        headonly: bool,
    ) -> bool {
        // must be an actual outgoing feed.
        if self.outhost.is_empty() && &self.label != "IFILTER" {
            return false;
        }

        // If one of the path-identities contains our path-identity, skip.
        if self.is_self {
            return false;
        }

        // don't send headers-only articles to normal peers.
        if headonly && !self.send_headfeed {
            return false;
        }

        // check hashfeed.
        if !hashfeed.matches(art.hash) {
            return false;
        }

        // check article type.
        if !art.arttype.matches(&self.arttypes) {
            return false;
        }

        // check path.
        if self.path_identity.matchlist(path) == MatchResult::Match {
            return false;
        }

        // check distribution header
        if let Some(dist) = dist {
            if !self.distributions.is_empty() && self.distributions.matchlist(dist) != MatchResult::Match {
                return false;
            }
        }

        // several min/max matchers.
        if (self.mincross > 0 && newsgroups.len() < (self.mincross as usize)) ||
            (self.maxcross > 0 && newsgroups.len() > (self.maxcross as usize)) ||
            (self.minpath > 0 && path.len() < (self.minpath as usize)) ||
            (self.maxpath > 0 && path.len() > (self.maxpath) as usize) ||
            (self.minsize > 0 && (art.len as u64) < self.minsize) ||
            (self.maxsize > 0 && (art.len as u64) > self.maxsize)
        {
            return false;
        }

        // newsgroup matching.
        if self.groups.matchlistx(newsgroups) != MatchResult::Match {
            return false;
        }

        // requiregroups matching.
        if !self.requiregroups.is_empty() && self.requiregroups.matchlistx(newsgroups) != MatchResult::Match {
            return false;
        }

        true
    }
}

/// Definition of a filter, which is a stripped-down NewsPeer.
#[rustfmt::skip]
#[derive(Default,Debug,Clone,Deserialize)]
#[serde(default)]
pub struct Filter {
    /// Paths to reject.
    #[serde(rename = "path-identity")]
    pub path_identity:      WildMatList,

    /// Groups to reject.
    pub groups:             WildMatList,

    /// Distributions to reject.
    pub distributions:      WildMatList,

    #[serde(rename = "min-crosspost")]
    pub mincross:           u32,
    #[serde(rename = "max-crosspost")]
    pub maxcross:           u32,
    #[serde(rename = "max-path-length")]
    pub maxpath:            u32,
    #[serde(rename = "min-path-length")]
    pub minpath:            u32,
    #[serde(rename = "max-article-size",deserialize_with = "util::deserialize_size")]
    pub maxsize:            u64,
    #[serde(rename = "min-article-size",deserialize_with = "util::deserialize_size")]
    pub minsize:            u64,
    #[serde(rename = "article-types")]
    pub arttypes:           Vec<ArtType>,
}

impl Filter {
    pub fn to_newspeer(&self) -> NewsPeer {
        let mut nf = NewsPeer::new();
        nf.path_identity = self.path_identity.clone();
        nf.groups = self.groups.clone();
        nf.distributions = self.distributions.clone();
        nf.mincross = self.mincross;
        nf.maxcross = self.maxcross;
        nf.maxpath = self.maxpath;
        nf.minpath = self.minpath;
        nf.maxsize = self.maxsize;
        nf.minsize = self.minsize;
        nf.arttypes = self.arttypes.clone();
        nf
    }
}

