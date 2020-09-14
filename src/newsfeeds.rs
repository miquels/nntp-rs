//! NewsFeeds contains a list of all NNTP peers.
//!
//! It is used for a couple of things:
//!
//! - When a peer connects. The peer is looked up in the newsfeed list
//!   by IP address. If it's not listed, access is denied.
//!
//! - When the peer sends an article. If it is allowed to send articles,
//!   the article is checked against "pathalias" and "filter" to see
//!   if we want it. If not, it is rejected. Otherwise it is sent to
//!   the `spool` code to be stored somewhere.
//!
//! - After an article is received by any peer, the list of peers is
//!   checked to see which ones of them want to receive that article.
//!   For those the article will be queued to be sent to the peer.
//!
use std::collections::HashMap;
use std::default::Default;
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
use crate::util::{self, HashFeed, MatchList, MatchResult, UnixTime, WildMatList};

use ipnet::IpNet;

/// A complete newsfeeds file.
#[derive(Debug)]
#[rustfmt::skip]
pub struct NewsFeeds {
    /// The IFILTER label
    pub infilter:       Option<NewsPeer>,
    /// All peers we have.
    //#[serde(rename = "peer")]
    pub peers:          Vec<NewsPeer>,
    pub peer_map:       HashMap<SmartString, usize>,
    /// And the groupdefs that can be referenced by the peers.
    pub groupdefs:      Vec<WildMatList>,
    /// timestamp of file when we loaded this data
    pub timestamp:      UnixTime,
    hcache:             HostCache,
}

impl NewsFeeds {
    /// Intialize a new "newsfeeds".
    pub fn new() -> NewsFeeds {
        NewsFeeds {
            infilter:  None,
            peers:     Vec::new(),
            peer_map:  HashMap::new(),
            groupdefs: Vec::new(),
            hcache:    HostCache::get(),
            timestamp: UnixTime::now(),
        }
    }

    /// Initialize the host cache.
    pub fn init_hostcache(&mut self) {
        for e in self.peers.iter_mut() {
            let mut hosts = Vec::new();
            for h in &e.inhost {
                match IpNet::from_str(h) {
                    Ok(net) => e.innet.push(net),
                    Err(_) => hosts.push(h.clone()),
                }
            }
            e.inhost = hosts;
        }
        self.hcache.update(&self);
    }

    /// Check all peers to see if one of them is actually ourself.
    pub fn check_self(&mut self, cfg: &config::Config) {
        for e in self.peers.iter_mut() {
            for pathhost in &cfg.server.pathhost {
                if e.pathalias.matches(pathhost) == MatchResult::Match {
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

    /// Look up a peer by IP address.
    ///
    /// Returns a tuple consisting of the index into the peers vector
    /// and a reference to the NewsPeer instance.
    pub fn find_peer(&self, ipaddr: &IpAddr) -> Option<(usize, &NewsPeer)> {
        if let Some(name) = self.hcache.lookup(ipaddr) {
            return self
                .peer_map
                .get(name.as_str())
                .map(|idx| (*idx, &self.peers[*idx]));
        }
        for i in 0..self.peers.len() {
            let e = &self.peers[i];
            for n in &e.innet {
                if n.contains(ipaddr) {
                    return Some((i, e));
                }
            }
        }
        None
    }
}

/// Definition of a newspeer.
#[rustfmt::skip]
#[derive(Default,Debug,Clone,Deserialize)]
#[serde(default = "get_default_newspeer")]
pub struct NewsPeer {
    /// Name of this feed.
    #[serde(rename = "__label__")]
    pub label:              SmartString,

    /// used both to filter incoming and outgoing articles.
    pub pathalias:          WildMatList,

    // if set, sets inhost, outhost, and pathalias in dnewsfeeds.
    pub(crate) host:        String,

    /// used on connects from remote host
    pub inhost:             Vec<String>,
    pub innet:              Vec<IpNet>,
    pub maxconnect:         u32,
    pub readonly:           bool,
    pub xclient:            bool,

    /// used when processing incoming articles
    pub filter:             WildMatList,
    pub nomismatch:         bool,
    #[serde(rename = "dont-defer")]
    pub dont_defer:         bool,

    /// used to select outgoing articles.
    pub maxcross:           u32,
    pub maxpath:            u32,
    #[serde(default,deserialize_with = "util::deserialize_size")]
    pub maxsize:            u64,
    #[serde(default,deserialize_with = "util::deserialize_size")]
    pub minsize:            u64,
    pub mincross:           u32,
    pub minpath:            u32,
    pub arttypes:           Vec<ArtType>,
    pub groups:             WildMatList,
    pub requiregroups:      WildMatList,
    // "deserialize_with" is for dnewsfeeds compatibility (adddist / deldist).
    pub distributions:      WildMatList,
    pub hashfeed:           HashFeed,

    /// used with the outgoing feed.
    pub outhost:            String,
    pub bindaddress:        Option<IpAddr>,
    pub port:               u16,
    pub maxparallel:        u32,
    pub maxstream:          u32,
    #[serde(rename = "delay-feed", deserialize_with = "util::deserialize_duration")]
    pub delay_feed:         Duration,
    #[serde(rename = "no-backlog")]
    pub no_backlog:         bool,
    #[serde(rename = "drop-deferred")]
    pub drop_deferred:      bool,
    pub maxqueue:           u32,
    #[serde(rename = "send-headfeed")]
    pub send_headfeed:      bool,
    #[serde(rename = "accept-headfeed")]
    pub accept_headfeed:    bool,
    pub preservebytes:      bool,
    #[serde(rename = "queue-only")]
    pub queue_only:         bool,

    /// metadata for non-feed related things.
    pub meta:               HashMap<String, Vec<String>>,

    /// non-config items.
    #[serde(skip)]
    pub index:              usize,
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

    /// Check if this peer wants to have this article.
    pub fn wants(
        &self,
        art: &Article,
        hashfeed: &HashFeed,
        path: &[&str],
        newsgroups: &mut MatchList,
        dist: Option<&Vec<&str>>,
        headonly: bool,
    ) -> bool
    {
        // must be an actual outgoing feed.
        if self.outhost.is_empty() && &self.label != "IFILTER" {
            return false;
        }

        // If one of the pathaliases contains our pathalias, skip.
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
        if self.pathalias.matchlist(path) == MatchResult::Match {
            return false;
        }

        // check distribution header
        if let Some(dist) = dist {
            if self.distributions.matchlist(dist) != MatchResult::Match {
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
        if self.requiregroups.patterns.len() > 0 &&
            self.requiregroups.matchlistx(newsgroups) != MatchResult::Match
        {
            return false;
        }

        true
    }
}

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use std::sync::Arc;

static GLOBAL: Lazy<ArcSwap<NewsPeer>> = Lazy::new(|| ArcSwap::from(Arc::new(NewsPeer::new())));

pub(crate) fn get_default_newspeer() -> NewsPeer {
    GLOBAL.load().as_ref().clone()
}

pub(crate) fn set_default_newspeer(peer: NewsPeer) {
    GLOBAL.store(Arc::new(peer));
}
