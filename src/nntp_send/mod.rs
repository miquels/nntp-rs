//! nntp_send - send articles to peers.
//!
use smartstring::alias::String as SmartString;
use std::net::IpAddr;

use crate::newsfeeds::NewsPeer;
use crate::spool::ArtLoc;

mod connection;
mod masterfeed;
mod peerfeed;
mod queue;

use connection::Connection;
use peerfeed::PeerFeed;
use queue::{QItems, Queue};

pub use masterfeed::MasterFeed;

/// Articles sent to the MasterFeed channel.
pub struct FeedArticle {
    /// Message-Id.
    pub msgid:    String,
    /// Location in the article spool.
    pub location: ArtLoc,
    /// Article size.
    pub size:     usize,
    /// Peers to feed it to.
    pub peers:    Vec<SmartString>,
}

// Sent from masterfeed -> peerfeed -> connection.
#[derive(Clone, Debug)]
enum PeerFeedItem {
    Article(PeerArticle),
    ConnExit(Vec<PeerArticle>),
    ReconfigurePeer(Peer),
    Reconfigure,
    ExitGraceful,
    ExitNow,
    Ping,
}

// Article queued for an outgoing connection.
#[derive(Clone, Debug)]
struct PeerArticle {
    // Message-Id.
    pub msgid:        String,
    // Location in the article spool.
    pub location:     ArtLoc,
    // Size
    pub size:         usize,
    // Did it come from the backlog
    pub from_backlog: bool,
    // How many times was this articles deferred.
    pub deferred:     u32,
}

// A shorter version of newsfeeds::NewsPeer.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Peer {
    label:         SmartString,
    outhost:       String,
    bindaddress:   Option<IpAddr>,
    port:          u16,
    maxparallel:   u32,
    maxstream:     u32,
    nobatch:       bool,
    maxqueue:      u32,
    headfeed:      bool,
    preservebytes: bool,
    queue_only:    bool,
}

impl Peer {
    fn new(nfpeer: &NewsPeer) -> Peer {
        Peer {
            label:         nfpeer.label.clone(),
            outhost:       nfpeer.outhost.clone(),
            bindaddress:   nfpeer.bindaddress.clone(),
            port:          nfpeer.port,
            maxparallel:   nfpeer.maxparallel,
            nobatch:       nfpeer.nobatch,
            maxqueue:      nfpeer.maxqueue,
            maxstream:     nfpeer.maxstream,
            headfeed:      nfpeer.send_headfeed,
            preservebytes: nfpeer.preservebytes,
            queue_only:    nfpeer.queue_only,
        }
    }
}
