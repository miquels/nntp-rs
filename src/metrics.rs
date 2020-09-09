//! Diagnostics, statistics and telemetry.

use std::collections::HashMap;
use std::default::Default;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::article::Article;
use crate::dns;
use crate::errors::ArtError;

// After how many articles sent a stats update should be logged.
const MARK_AFTER_OFFERED_N: u64 = 1000;

// Global per-peer stats.
static PEER_RECV_STATS: Lazy<Mutex<HashMap<String, Arc<PeerRecvStats>>>> = Lazy::new(Default::default);
static PEER_SENT_STATS: Lazy<Mutex<HashMap<String, Arc<PeerSentStats>>>> = Lazy::new(Default::default);

//
// A macro to generate the `Stats` enum and the `ConnRecvStats` and `PeerRecvStats` structs.
// Also generates helper functions that map the Stats enum to the struct fields.
//
macro_rules! recv_stats {
    (@IF_COMMON X, $block:tt) => {};
    (@IF_COMMON $common_field:ident, $block:tt) => { $block };

    ($([ $enum_field:ident, $common_field:ident, $struct_field:ident ],)*) => {
        /// All the metrics that we maintain, per incoming feed.
        #[derive(Debug)]
        pub enum Stats {
            $(
                $enum_field,
            )*
        }
        /// Metrics per connection, non persistent.
        #[derive(Default, Debug)]
        pub struct ConnRecvStats {
            $(
                $struct_field: u64,
            )*
        }
        /// Metrics per peer (all connections), persistent.
        #[derive(Default, Debug)]
        pub struct PeerRecvStats {
            $(
                $struct_field: AtomicU64,
            )*
        }
        /// Add a value to a metric.
        fn stats_add(this: &mut RxSessionStats, metric: Stats, count: u64) {
            match metric {
                $(
                    Stats::$enum_field => {
                        this.conn_stats.$struct_field = this.conn_stats.$struct_field.wrapping_add(count);
                        this.peer_stats.$struct_field.fetch_add(count, Ordering::AcqRel);
                        recv_stats!(@IF_COMMON $common_field, {
                            this.conn_stats.$common_field = this.conn_stats.$common_field.wrapping_add(count);
                            this.peer_stats.$common_field.fetch_add(count, Ordering::AcqRel);
                        });
                    },
                )*
            }
        }
    }
}

recv_stats! {
    [   Offered,            X,          offered                     ], // offered (ihave/check)
    [   Accepted,           X,          accepted                    ], // accepted
    [   AcceptedBytes,      X,          accepted_bytes              ],
    [   Received,           X,          received                    ], // received
    [   ReceivedBytes,      X,          received_bytes              ],
    [   Refused,            X,          refused                     ], // refused
    [   RefHistory,         refused,    refused_history             ], // ref, in history
    [   RefPreCommit,       refused,    refused_precommit           ], // ref, offered by another host
    [   RefPostCommit,      refused,    refused_postcommit          ], // ref, in history cache
    [   RefBadMsgId,        refused,    refused_bad_msgid           ], // ref, bad msgid
    [   RefIfiltHash,       refused,    refused_ifilter_hash        ], // ref, by IFILTER hash
    [   Ihave,              X,          ihave                       ], // ihave
    [   Check,              X,          check                       ], // check
    [   Takethis,           X,          takethis                    ], // takethis
    [   Control,            X,          control                     ], // control message
    [   Rejected,           X,          rejected                    ], // rejected
    [   RejectedBytes,      X,          rejected_bytes              ],
    [   RejFailsafe,        rejected,   rejected_failsafe           ], // rej, failsafe
    [   RejMissHdrs,        rejected,   rejected_missing_headers    ], // rej, missing headers
    [   RejTooOld,          rejected,   rejected_too_old            ], // rej, too old
    [   RejGrpFilter,       rejected,   rejected_group_filter       ], // rej, incoming grp filter
    [   RejIntSpamFilter,   rejected,   rejected_ispam              ], // rej, internal spam filter
    [   RejExtSpamFilter,   rejected,   rejected_espam              ], // rej, external spam filter
    [   RejIncFilter,       rejected,   rejected_ifilter            ], // rej, incoming filter
    [   RejNoSpool,         rejected,   rejected_no_spool           ], // rej, no spool object
    [   RejIOError,         rejected,   rejected_io_error           ], // rej, io error
    [   RejNotInActv,       rejected,   rejected_not_in_active      ], // rej, not in active file
    [   RejPathTab,         rejected,   rejected_tab_in_path        ], // rej, TAB in Path: header
    [   RejNgTab,           rejected,   rejected_tab_in_newsgroups  ], // rej, TAB in Newsgroups: hdr
    [   RejPosDup,          rejected,   rejected_post_dup           ], // rej, dup detected after receive
    [   RejHdrError,        rejected,   rejected_bad_header         ], // rej, dup or missing headers
    [   RejTooSmall,        rejected,   rejected_art_too_small      ], // rej, article too small
    [   RejArtIncompl,      rejected,   rejected_art_incomplete     ], // rej, article incomplete
    [   RejArtNul,          rejected,   rejected_art_has_nul        ], // rej, article has a nul
    [   RejNoBytes,         rejected,   rejected_no_bytes_header    ], // rej, header only, no Bytes:
    [   RejProtoErr,        rejected,   rejected_protocol_error     ], // rej, protocol error
    [   RejMsgIdMis,        rejected,   rejected_msgid_mismatch     ], // rej, msgid mismatch
    [   RejErr,             rejected,   rejected_error              ], // rej, unknown error
    [   RejTooBig,          rejected,   rejected_art_too_big        ], // rej, too big
    [   RejBigHeader,       rejected,   rejected_hdr_too_big        ], // header too big
    [   RejNoHdrEnd,        rejected,   rejected_no_hdr_end         ], // header too big
    [   RejBareCR,          rejected,   rejected_art_has_bare_cr    ], // article has a CR without LF
}

pub struct RxSessionStats {
    // identification.
    hostname:   String,
    ipaddr:     String,
    label:      String,
    fdno:       u32,
    // stats
    instant:    Instant,
    pub conn_stats: ConnRecvStats,
    pub peer_stats: Arc<PeerRecvStats>,
}

impl Default for RxSessionStats {
    fn default() -> RxSessionStats {
        RxSessionStats {
            hostname: String::new(),
            ipaddr:   String::new(),
            label:    String::new(),
            fdno:     0,
            instant:  Instant::now(),
            conn_stats: ConnRecvStats::default(),
            peer_stats: Arc::new(PeerRecvStats::default()),
        }
    }
}

impl RxSessionStats {
    pub fn new(ipaddr: std::net::SocketAddr, fdno: u32) -> RxSessionStats {
        RxSessionStats {
            hostname: ipaddr.to_string(),
            ipaddr: ipaddr.to_string(),
            label: "unknown".to_string(),
            fdno: fdno,
            ..RxSessionStats::default()
        }
    }

    pub fn hostname(&self) -> &str {
        self.hostname.as_str()
    }
    pub fn ipaddr(&self) -> &str {
        self.ipaddr.as_str()
    }
    pub fn label(&self) -> &str {
        self.label.as_str()
    }
    pub fn fdno(&self) -> u32 {
        self.fdno
    }

    pub fn add(&mut self, field: Stats, count: u64) {
        stats_add(self, field, count);
        if self.conn_stats.received >= 1024 {
            self.log_stats();
        }
    }

    pub fn inc(&mut self, field: Stats) {
        stats_add(self, field, 1);
        if self.conn_stats.received >= 1024 {
            self.log_stats();
        }
    }

    pub async fn on_connect(&mut self, ipaddr_str: String, label: String) {

        let ipaddr: std::net::IpAddr = ipaddr_str.parse().unwrap();
        let host = match dns::RESOLVER.reverse_lookup(ipaddr).await {
            Ok(m) => {
                let mut h = m.iter().next().map(|name| name.to_utf8());
                if let Some(ref mut h) = h {
                    // reverse lookup might return hostname terminated with a '.'.
                    if h.ends_with(".") {
                        h.pop();
                    }
                }
                h
            },
            Err(_) => None,
        };
        let peer_stats = {
            let mut stats = PEER_RECV_STATS.lock();
            stats.entry(label.clone()).or_insert_with(|| Arc::new(PeerRecvStats::default())).clone()
        };
        self.hostname = host.unwrap_or(ipaddr_str);
        self.label = label;
        self.peer_stats = peer_stats;
        log::info!(
            "Connection {} from {} {} [{}]",
            self.fdno,
            self.hostname,
            self.ipaddr,
            self.label
        );
    }

    pub fn on_disconnect(&mut self) {
        self.log_stats();
        let elapsed = self.instant.elapsed().as_secs();
        log::info!(
            "Disconnect {} from {} {} ({} elapsed)",
            self.fdno,
            self.hostname,
            self.ipaddr,
            elapsed
        );
    }

    pub fn log_stats(&mut self) {
        self.log_mainstats();
        if self.conn_stats.rejected > 0 {
            self.log_rejstats();
        }

        // reset connection stats.
        self.instant = Instant::now();
        self.conn_stats = ConnRecvStats::default();
    }

    pub fn log_mainstats(&self) {
        // This calculation comes straight from diablo, not sure
        // why it is done this way.
        let mut nuse = self.conn_stats.check + self.conn_stats.ihave;
        if nuse < self.conn_stats.received {
            nuse = self.conn_stats.received;
        }

        let elapsed = self.instant.elapsed().as_millis() as u64;
        let rate = format_rate(elapsed, nuse);

        log::info!("Stats {} secs={:.1} ihave={} chk={} takethis={} rec={} acc={} ref={} precom={} postcom={} his={} badmsgid={} ifilthash={} rej={} ctl={} spam={} err={} recbytes={} accbytes={} rejbytes={} ({})",
            self.fdno,
            (elapsed as f64) / 1000f64,
            self.conn_stats.ihave,
            self.conn_stats.check,
            self.conn_stats.takethis,
            self.conn_stats.received,
            self.conn_stats.accepted,
            self.conn_stats.refused,
            self.conn_stats.refused_precommit,
            self.conn_stats.refused_postcommit,
            self.conn_stats.refused_history,
            self.conn_stats.refused_bad_msgid,
            self.conn_stats.refused_ifilter_hash,
            self.conn_stats.rejected,
            self.conn_stats.control,
            self.conn_stats.rejected_ispam + self.conn_stats.rejected_espam,
            self.conn_stats.rejected_error,
            self.conn_stats.received_bytes,
            self.conn_stats.accepted_bytes,
            self.conn_stats.rejected_bytes,
            rate,
        );
    }

    pub fn log_rejstats(&self) {
        log::info!("Stats {} rejstats rej={} failsafe={} misshdrs={} tooold={} grpfilt={} intspamfilt={} extspamfilt={} incfilter={} nospool={} ioerr={} notinactv={} pathtab={} ngtab={} posdup={} hdrerr={} toosmall={} incompl={} nul={} nobytes={} proto={} msgidmis={} nohdrend={} bighdr={} barecr={} err={} toobig={}",
            self.fdno,
            self.conn_stats.rejected,
            self.conn_stats.rejected_failsafe,
            self.conn_stats.rejected_missing_headers,
            self.conn_stats.rejected_too_old,
            self.conn_stats.rejected_group_filter,
            self.conn_stats.rejected_ispam,
            self.conn_stats.rejected_espam,
            self.conn_stats.rejected_ifilter,
            self.conn_stats.rejected_no_spool,
            self.conn_stats.rejected_io_error,
            self.conn_stats.rejected_not_in_active,
            self.conn_stats.rejected_tab_in_path,
            self.conn_stats.rejected_tab_in_newsgroups,
            self.conn_stats.rejected_post_dup,
            self.conn_stats.rejected_bad_header,
            self.conn_stats.rejected_art_too_small,
            self.conn_stats.rejected_art_incomplete,
            self.conn_stats.rejected_art_has_nul,
            self.conn_stats.rejected_no_bytes_header,
            self.conn_stats.rejected_protocol_error,
            self.conn_stats.rejected_msgid_mismatch,
            self.conn_stats.rejected_no_hdr_end,
            self.conn_stats.rejected_hdr_too_big,
            self.conn_stats.rejected_art_has_bare_cr,
            self.conn_stats.rejected_error,
            self.conn_stats.rejected_art_too_big,
        );
    }

    pub fn art_error(&mut self, art: &Article, e: &ArtError) {
        log::trace!("art_error: {}: {:?}", art.msgid, e);

        #[rustfmt::skip]
        let rej = match e {
            &ArtError::PostDuplicate      => Stats::RejPosDup,
            &ArtError::ArtIncomplete      => Stats::RejArtIncompl,
            &ArtError::TooSmall           => Stats::RejTooSmall,
            &ArtError::TooBig             => Stats::RejTooBig,
            &ArtError::NotInActive        => Stats::RejNotInActv,
            &ArtError::TooOld             => Stats::RejTooOld,
            &ArtError::HdrOnlyNoBytes     => Stats::RejNoBytes,
            &ArtError::HdrOnlyWithBody    => Stats::RejTooBig,
            &ArtError::GroupFilter        => Stats::RejGrpFilter,
            &ArtError::IncomingFilter     => Stats::RejIncFilter,
            &ArtError::InternalSpamFilter => Stats::RejIntSpamFilter,
            &ArtError::ExternalSpamFilter => Stats::RejExtSpamFilter,
            &ArtError::RejSpool           => Stats::RejNoSpool,
            &ArtError::NoSpool            => Stats::RejNoSpool,
            &ArtError::FileWriteError     => Stats::RejIOError,
            &ArtError::IOError            => Stats::RejIOError,
            &ArtError::NoHdrEnd           => Stats::RejNoHdrEnd,
            &ArtError::HeaderTooBig       => Stats::RejBigHeader,
            &ArtError::BadHdrName         => Stats::RejHdrError,
            &ArtError::DupHdr             => Stats::RejHdrError,
            &ArtError::EmptyHdr           => Stats::RejHdrError,
            &ArtError::BadUtf8Hdr         => Stats::RejHdrError,
            &ArtError::BadMsgId           => Stats::RejErr,
            &ArtError::MsgIdMismatch      => Stats::RejMsgIdMis,
            &ArtError::MissingHeader      => Stats::RejMissHdrs,
            &ArtError::NoNewsgroups       => Stats::RejMissHdrs,
            &ArtError::NoPath             => Stats::RejMissHdrs,
            &ArtError::PathTab            => Stats::RejPathTab,
            &ArtError::NewsgroupsTab      => Stats::RejNgTab,
            &ArtError::ArticleNul         => Stats::RejArtNul,
            &ArtError::BareCR             => Stats::RejBareCR,
        };
        self.inc(rej);
        self.add(Stats::RejectedBytes, art.len as u64);
        self.inc(Stats::Received);
        self.add(Stats::ReceivedBytes, art.len as u64);
    }

    pub fn art_accepted(&mut self, art: &Article) {
        self.inc(Stats::Accepted);
        self.add(Stats::AcceptedBytes, art.len as u64);
        self.inc(Stats::Received);
        self.add(Stats::ReceivedBytes, art.len as u64);
    }
}

#[derive(Debug, Default)]
struct ConnSentStats {
    offered: u64,
    accepted: u64,
    accepted_bytes: u64,
    rejected: u64,
    rejected_bytes: u64,
    refused: u64,
    deferred: u64,
    deferred_bytes: u64,
    deferred_fail: u64,
    not_found: u64,
}

#[derive(Debug, Default)]
pub struct PeerSentStats {
    offered: AtomicU64,
    accepted: AtomicU64,
    accepted_bytes: AtomicU64,
    rejected: AtomicU64,
    rejected_bytes: AtomicU64,
    refused: AtomicU64,
    deferred: AtomicU64,
    deferred_bytes: AtomicU64,
    deferred_fail: AtomicU64,
    not_found: AtomicU64,
}

macro_rules! stats_add {
    ($self:expr, $count:expr, $field:ident) => {
        $self.delta_stats.$field = $self.conn_stats.$field.wrapping_add($count);
        $self.peer_stats.$field.fetch_add($count, Ordering::AcqRel);
    }
}

#[rustfmt::skip]
pub struct TxSessionStats {
    // identification.
    label:       String,
    id:          u64,
    start:       Instant,
    mark:        Instant,
    // stats
    conn_stats:  ConnSentStats,
    delta_stats: ConnSentStats,
    peer_stats:  Arc<PeerSentStats>,
}

impl Default for TxSessionStats {
    fn default() -> TxSessionStats {
        TxSessionStats {
            label: String::new(),
            id:    0,
            start: Instant::now(),
            mark:  Instant::now(),
            conn_stats: ConnSentStats::default(),
            delta_stats: ConnSentStats::default(),
            peer_stats: Arc::new(PeerSentStats::default()),
        }
    }
}

impl TxSessionStats {
    pub fn on_connect(&mut self, label: &str, id: u64, outhost: &str, ipaddr: IpAddr, line: &str) {
        let peer_stats = {
            let mut stats = PEER_SENT_STATS.lock();
            stats.entry(label.to_string()).or_insert_with(|| Arc::new(PeerSentStats::default())).clone()
        };
        self.label = label.to_string();
        self.id = id;
        self.start = Instant::now();
        self.mark = Instant::now();
        self.peer_stats = peer_stats;
        log::info!("Feed {}:{} connect: {} ({}/{})", label, id, line, outhost, ipaddr);
    }

    fn stats_update(&mut self) {
        log::info!(
            "Feed {}:{} mark {}",
            self.label,
            self.id,
            self.log_stats(self.mark, &self.delta_stats)
        );
        if self.delta_stats.deferred > 0 || self.delta_stats.deferred_fail > 0 {
            log::info!(
                "Feed {}:{} mark {}",
                self.label,
                self.id,
                self.log_defer(self.mark, &self.delta_stats)
            );
        }
        self.update_total();
    }

    pub fn stats_final(&mut self) {
        self.update_total();
        log::info!(
            "Feed {}:{} final {}",
            self.label,
            self.id,
            self.log_stats(self.start, &self.conn_stats)
        );
        if self.conn_stats.deferred > 0 || self.conn_stats.deferred_fail > 0 {
            log::info!(
                "Feed {}:{} final {}",
                self.label,
                self.id,
                self.log_defer(self.start, &self.conn_stats)
            );
        }
    }

    fn update_total(&mut self) {
        self.mark = Instant::now();
        self.conn_stats.offered += self.delta_stats.offered;
        self.conn_stats.accepted += self.delta_stats.accepted;
        self.conn_stats.accepted_bytes += self.delta_stats.accepted_bytes;
        self.conn_stats.rejected += self.delta_stats.rejected;
        self.conn_stats.rejected_bytes += self.delta_stats.rejected_bytes;
        self.conn_stats.refused += self.delta_stats.refused;
        self.conn_stats.deferred += self.delta_stats.deferred;
        self.conn_stats.deferred_bytes += self.delta_stats.deferred_bytes;
        self.conn_stats.deferred_fail += self.delta_stats.deferred_fail;
        self.conn_stats.not_found += self.delta_stats.not_found;
        self.delta_stats = ConnSentStats::default();
    }

    fn log_stats(&self, start: Instant, stats: &ConnSentStats) -> String {
        let elapsed_ms = std::cmp::max(1, Instant::now().saturating_duration_since(start).as_millis());
        let rate = format_rate(elapsed_ms as u64, stats.offered);
        format!(
            "secs={} acc={} dup={} rej={} tot={} bytes={} ({}) avpend={:.1}",
            format_secs(elapsed_ms as u64),
            stats.accepted,
            stats.refused,
            stats.rejected,
            stats.offered,
            stats.accepted_bytes,
            rate,
            1,
        )
    }

    fn log_defer(&self, start: Instant, stats: &ConnSentStats) -> String {
        let elapsed_ms = std::cmp::max(1, Instant::now().saturating_duration_since(start).as_millis());
        format!(
            "secs={} defer={} deferfail={}",
            format_secs(elapsed_ms as u64),
            stats.deferred,
            stats.deferred_fail,
        )
    }

    // About to send TAKETHIS but article not present in spool anymore.
    pub fn art_notfound(&mut self) {
        stats_add!(self, 1, offered);
        stats_add!(self, 1, not_found);
    }

    // CHECK 431 (and incorrectly, TAKETHIS 431)
    pub fn art_deferred(&mut self, size: Option<usize>) {
        stats_add!(self, 1, offered);
        stats_add!(self, 1, deferred);
        if let Some(size) = size {
            stats_add!(self, size as u64, deferred_bytes);
        }
        self.check_mark();
    }

    // CHECK 431 (and incorrectly, TAKETHIS 431) where we failed to re-queue.
    pub fn art_deferred_fail(&mut self, size: Option<usize>) {
        stats_add!(self, 1, offered);
        stats_add!(self, 1, deferred_fail);
        if let Some(size) = size {
            stats_add!(self, size as u64, deferred_bytes);
        }
        self.check_mark();
    }

    // CHECK 438
    pub fn art_refused(&mut self) {
        stats_add!(self, 1, offered);
        stats_add!(self, 1, refused);
    }

    // TAKETHIS 239
    pub fn art_accepted(&mut self, size: usize) {
        stats_add!(self, 1, offered);
        stats_add!(self, 1, accepted);
        stats_add!(self, size as u64, accepted_bytes);
        self.check_mark();
    }

    // TAKETHIS 439
    pub fn art_rejected(&mut self, size: usize) {
        stats_add!(self, 1, offered);
        stats_add!(self, 1, rejected);
        stats_add!(self, size as u64, rejected_bytes);
        self.check_mark();
    }

    #[inline]
    fn check_mark(&mut self) {
        if self.delta_stats.offered % MARK_AFTER_OFFERED_N == 0 {
            self.stats_update();
        }
    }
}

fn format_rate(elapsed_ms: u64, count: u64) -> String {
    let dt = std::cmp::max(100, elapsed_ms) as f64 / 1000f64;
    let mut rate = count as f64 / dt;
    let mut unit = "sec";

    if rate < 0.1 && rate >= 0.0001 {
        rate *= 60.0;
        unit = "min";
    }
    if rate < 0.01 {
        format!("{:.4}/{}", rate, unit)
    } else if rate < 0.1 {
        format!("{:.2}/{}", rate, unit)
    } else if rate < 10.0 {
        format!("{:.1}/{}", rate, unit)
    } else {
        format!("{}/{}", rate.round() as u64, unit)
    }
}

fn format_secs(ms: u64) -> String {
    if ms < 1000 {
        format!("{:.2}", ms as f64 / 1000.0)
    } else if ms < 10_000 {
        format!("{:.1}", ms as f64 / 1000.0)
    } else {
        format!("{}", ms / 1000)
    }
}

