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
use std::default::Default;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

use hostcache::HostCache;
use nntp_rs_util as util;

use ipnet::IpNet;

/// A complete newsfeeds file.
#[derive(Debug)]
pub struct NewsFeeds {
    /// The IFILTER label
    pub infilter:       Option<NewsPeer>,
    /// All peers we have.
    pub peers:          Vec<NewsPeer>,
    pub peer_map:       HashMap<String, usize>,
    /// And the groupdefs that can be referenced by the peers.
    pub groupdefs:      Vec<GroupDef>,
    pub groupdef_map:   HashMap<String, usize>,

    pub timestamp:      u64,
    hcache:             HostCache,
}

impl NewsFeeds {

    /// Intialize a new "newsfeeds".
    pub fn new() -> NewsFeeds {
        NewsFeeds{
            infilter:       None,
            peers:          Vec::new(),
            peer_map:       HashMap::new(),
            groupdefs:      Vec::new(),
            groupdef_map:   HashMap::new(),
            hcache:         HostCache::get(),
            timestamp:      util::unixtime_ms(),
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

    /// Update the hostcache, after changes to self.
    pub fn update_hostcache(&self) {
        self.hcache.update(&self);
    }

    /// Look up a peer by IP address.
    ///
    /// Returns a tuple consisting of the index into the peers vector
    /// and a reference to the NewsPeer instance.
    pub fn find_peer(&self, ipaddr: &IpAddr) -> Option<(usize, &NewsPeer)> {
        if let Some(name) =  self.hcache.lookup(ipaddr) {
            return self.peer_map.get(&name).map(|idx| (*idx, &self.peers[*idx]));
        }
        for i in 0 .. self.peers.len() {
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
#[derive(Default,Debug,Clone)]
pub struct NewsPeer {
    /// Name of this feed.
    pub label:              String,

    /// used both to filter incoming and outgoing articles.
    pub pathalias:          Vec<String>,

    /// used on connects from remote host
    pub inhost:             Vec<String>,
    pub innet:              Vec<IpNet>,
    pub maxconnect:         u32,
    pub readonly:           bool,

    /// used when processing incoming articles
    pub filter:             Vec<String>,
    pub nomismatch:         bool,
    pub precomreject:       bool,

    /// used to select outgoing articles.
    pub maxcross:           u32,
    pub maxpath:            u32,
    pub maxsize:            u64,
    pub minsize:            u64,
    pub mincross:           u32,
    pub minpath:            u32,
    pub arttypes:           Vec<String>,
    pub groups:             Vec<String>,
    pub requiregroups:      Vec<String>,
    pub distributions:      Vec<String>,
    pub hashfeed:           String,

    /// used with the outgoing feed.
    pub outhost:            String,
    pub bindaddress:        String,
    pub port:               u16,
    pub maxparallel:        u32,
    pub maxstream:          u32,
    pub nobatch:            bool,
    pub maxqueue:           u32,
    pub headfeed:           bool,
    pub genlines:           bool,
    pub preservebytes:      bool,

    pub index:              usize,
}
pub type GroupDef = NewsPeer;

impl NewsPeer {
    /// Get a fresh NewsPeer with defaults set.
    pub fn new() -> NewsPeer {
        NewsPeer{
            port:           119,
            maxparallel:    2,
            ..Default::default()
        }
    }
}

