//! nntp_send - send articles to peers.
//!
use smartstring::alias::String as SmartString;
use std::net::IpAddr;
use std::time::Duration;

use crate::newsfeeds::NewsPeer;
use crate::spool::ArtLoc;

macro_rules! conditional_fut {
    ($cond:expr, $fut:expr) => {{
        use futures::future::{pending, Either, FutureExt};
        {
            if $cond {
                Either::Left($fut.fuse())
            } else {
                Either::Right(pending())
            }
        }
    }};
}

mod connection;
mod delay_queue;
mod masterfeed;
mod mpmc;
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
    ExitFeed,
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
    sendbuf_size:  Option<usize>,
    maxparallel:   u32,
    maxstream:     u32,
    max_qbytes:    u64,
    delay_feed:    Duration,
    no_backlog:    bool,
    drop_deferred: bool,
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
            sendbuf_size: nfpeer.sendbuf_size.map(|sz| sz as usize),
            maxparallel:   nfpeer.maxparallel,
            delay_feed:    nfpeer.delay_feed,
            no_backlog:    nfpeer.no_backlog,
            drop_deferred: nfpeer.drop_deferred,
            maxqueue:      nfpeer.maxqueue,
            maxstream:     nfpeer.maxstream,
            max_qbytes:    nfpeer.max_qbytes,
            headfeed:      nfpeer.send_headfeed,
            preservebytes: nfpeer.preservebytes,
            queue_only:    nfpeer.queue_only,
        }
    }
}
