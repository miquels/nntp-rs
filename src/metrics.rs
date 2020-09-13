//! Diagnostics, statistics and telemetry.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::time::delay_for;

use crate::article::Article;
use crate::bus::{self, Notification};
use crate::config;
use crate::dns;
use crate::errors::ArtError;
use crate::util::{self, UnixTime};

// After how many articles sent a stats update should be logged.
const MARK_AFTER_OFFERED_N: u64 = 1000;

// Global per-peer stats.
#[derive(Default, Debug, Serialize, Deserialize)]
struct PeerStats {
    recv: HashMap<String, Arc<PeerRecvStats>>,
    sent: HashMap<String, Arc<PeerSentStats>>,
}
static PEER_STATS: Lazy<RwLock<PeerStats>> = Lazy::new(Default::default);

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
        #[derive(Default, Debug, Serialize, Deserialize)]
        #[serde(default)]
        pub struct PeerRecvStats {
            $(
                $struct_field: AtomicU64,
            )*
            connections:    AtomicU64,
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
    hostname:       String,
    ipaddr:         IpAddr,
    label:          String,
    fdno:           u32,
    connected:      bool,
    // stats
    instant:        Instant,
    pub conn_stats: ConnRecvStats,
    pub peer_stats: Arc<PeerRecvStats>,
}

impl Drop for RxSessionStats {
    fn drop(&mut self) {
        if self.connected {
            self.peer_stats.connections.fetch_sub(1, Ordering::SeqCst);
        }
    }
}


impl RxSessionStats {
    pub fn new(ipaddr: IpAddr, fdno: u32) -> RxSessionStats {
        RxSessionStats {
            hostname: ipaddr.to_string(),
            ipaddr,
            label: "unknown".to_string(),
            fdno: fdno,
            connected: false,
            instant: Instant::now(),
            conn_stats: ConnRecvStats::default(),
            peer_stats: Arc::new(PeerRecvStats::default()),
        }
    }

    pub fn hostname(&self) -> &str {
        self.hostname.as_str()
    }
    pub fn ipaddr(&self) -> IpAddr {
        self.ipaddr
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

    pub async fn on_connect(&mut self, label: String, ipaddr: IpAddr) {
        self.ipaddr = ipaddr;
        let host = match dns::RESOLVER.reverse_lookup(self.ipaddr).await {
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
            let mut stats = PEER_STATS.write();
            stats
                .recv
                .entry(label.clone())
                .or_insert_with(|| Arc::new(PeerRecvStats::default()))
                .clone()
        };
        self.hostname = host.unwrap_or(self.ipaddr.to_string());
        self.label = label;
        self.peer_stats = peer_stats;
        if !self.connected {
            // on_connect might be called twice in the XCLIENT case.
            self.connected = true;
            self.peer_stats.connections.fetch_add(1, Ordering::SeqCst);
        }
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
    offered:        u64,
    accepted:       u64,
    accepted_bytes: u64,
    rejected:       u64,
    rejected_bytes: u64,
    refused:        u64,
    deferred:       u64,
    deferred_bytes: u64,
    deferred_fail:  u64,
    not_found:      u64,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PeerSentStats {
    offered:        AtomicU64,
    accepted:       AtomicU64,
    accepted_bytes: AtomicU64,
    rejected:       AtomicU64,
    rejected_bytes: AtomicU64,
    refused:        AtomicU64,
    deferred:       AtomicU64,
    deferred_bytes: AtomicU64,
    deferred_fail:  AtomicU64,
    not_found:      AtomicU64,
    connections:    AtomicU64,
}

macro_rules! stats_add {
    ($self:expr, $count:expr, $field:ident) => {
        $self.delta_stats.$field = $self.conn_stats.$field.wrapping_add($count);
        $self.peer_stats.$field.fetch_add($count, Ordering::AcqRel);
    };
}

#[rustfmt::skip]
pub struct TxSessionStats {
    // identification.
    label:       String,
    id:          u64,
    start:       Instant,
    mark:        Instant,
    connected:  bool,
    // stats
    conn_stats:  ConnSentStats,
    delta_stats: ConnSentStats,
    peer_stats:  Arc<PeerSentStats>,
}

impl Default for TxSessionStats {
    fn default() -> TxSessionStats {
        TxSessionStats {
            label:       String::new(),
            id:          0,
            start:       Instant::now(),
            mark:        Instant::now(),
            connected:   false,
            conn_stats:  ConnSentStats::default(),
            delta_stats: ConnSentStats::default(),
            peer_stats:  Arc::new(PeerSentStats::default()),
        }
    }
}

impl Drop for TxSessionStats {
    fn drop(&mut self) {
        if self.connected {
            self.peer_stats.connections.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl TxSessionStats {
    pub fn on_connect(&mut self, label: &str, id: u64, outhost: &str, ipaddr: IpAddr, line: &str) {
        let peer_stats = {
            let mut stats = PEER_STATS.write();
            stats
                .sent
                .entry(label.to_string())
                .or_insert_with(|| Arc::new(PeerSentStats::default()))
                .clone()
        };
        self.label = label.to_string();
        self.id = id;
        self.start = Instant::now();
        self.mark = Instant::now();
        self.connected = true;
        self.peer_stats = peer_stats;
        self.peer_stats.connections.fetch_add(1, Ordering::SeqCst);
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

pub async fn load() -> io::Result<()> {
    // load file data.
    let config = config::get_config();
    let filename = match config.logging.metrics.as_ref() {
        Some(filename) => filename,
        None => return Ok(()),
    };
    let mut path = PathBuf::from(&config.paths.db);
    path.push(filename);
    let json = match tokio::fs::read_to_string(&path).await {
        Ok(json) => json,
        Err(e) => {
            if e.kind() == io::ErrorKind::NotFound {
                log::info!("metrics: no existing metrics file found, starting empty");
                return Ok(());
            }
            return Err(ioerr!(e.kind(), "metrics: {:?}: {}", path, e));
        },
    };

    // deserialize json.
    let data: PeerStats = serde_json::from_str(&json).map_err(|e| {
        ioerr!(
            InvalidData,
            "metrics: {:?}: could not deserialize JSON data: {}",
            path,
            e
        )
    })?;

    // save into global.
    *PEER_STATS.write() = data;

    Ok(())
}

/// Save metrics to disk.
///
/// Returns the hash of the json text. If a hash value is passed in,
/// and the generated json text has the same hash, the file is
/// considered not to have changed and consequently, not written.
pub async fn save(hash: Option<u64>) -> io::Result<u64> {
    let config = config::get_config();
    let mut newhash = 0;
    let mut changed = false;

    if let Some(filename) = config.logging.metrics.as_ref() {
        let path = config::expand_path(&config.paths, &filename);
        let text = serde_json::to_string(&*PEER_STATS.read()).unwrap();
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        newhash = hasher.finish();
        if hash != Some(newhash) {
            changed = true;
            write_file(&path, text).await?;
        }
    }

    if let Some(filename) = config.logging.prometheus.as_ref() {
        let path = config::expand_path(&config.paths, &filename);
        if !changed {
            // Just try to update the timestamp.
            if util::touch(&path, UnixTime::now()).is_ok() {
                return Ok(newhash);
            }
        }
        let text = prometheus(&*PEER_STATS.read());
        write_file(&path, text).await?;
    }

    Ok(newhash)
}

// Write temporary file and rename into place.
async fn write_file(path: impl AsRef<Path>, data: String) -> io::Result<()> {
    let path = path.as_ref();
    let tmp = path.with_extension("tmp");
    if let Err(e) = tokio::fs::write(&tmp, data).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(ioerr!(
            e.kind(),
            "metrics: creating/writing temp file {:?}: {}",
            tmp,
            e
        ));
    }
    if let Err(e) = tokio::fs::rename(&tmp, &path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(ioerr!(e.kind(), "metrics: replacing {:?}: {}", path, e));
    }
    Ok(())
}

/// Start a task to write the metrics to disk periodically (once every minute).
pub async fn metrics_task(mut bus_recv: bus::Receiver) {
    tokio::spawn(async move {
        let mut error = false;
        let mut hash = None;
        loop {
            tokio::select! {
                _ = delay_for(Duration::from_secs(60)) => {
                    match save(hash).await {
                        Ok(val) => {
                            if error {
                                log::info!("metrics: save: ok");
                                error = false;
                            }
                            hash = Some(val);
                        },
                        Err(e) => {
                            if !error {
                                log::error!("{}", e);
                                error = true;
                            }
                        },
                    }
                }
                item = bus_recv.recv() => {
                    match item {
                        Some(Notification::ExitGraceful) => break,
                        Some(Notification::ExitNow) => break,
                        Some(_) => {},
                        None => break,
                    }
                }
            }
        }
    });
}

fn prometheus(stats: &PeerStats) -> String {
    // First, walk over all peers and get the meta prometheus data.
    let nf = config::get_newsfeeds();
    let mut hm = HashMap::new();
    for peer in &nf.peers {
        if let Some(meta) = peer.meta.get("prometheus-label") {
            let mut labels = PromLabels::default();
            for label in meta {
                let a = label.splitn(2, '=').collect::<Vec<_>>();
                if a.len() == 2 {
                    labels.push(a[0], a[1]);
                }
            }
            hm.insert(peer.label.to_string(), labels);
        }
    }
    let mut text = String::with_capacity(4000);
    prom_metrics_recv(stats, &hm, &mut text);
    prom_metrics_sent(stats, &hm, &mut text);
    text
}

// This macro defines the functions "prom_metrics_recv" and "prom_metric_sent".
// Those functions generate the prometheus protocol data in text form.
macro_rules! prom_metrics {
    ($func:ident, $field:ident, $($metric:ident => [ $mfield:ident, $type:expr, $help: expr ],)*) => {
        fn $func(peer_stats: &PeerStats, labelmap: &HashMap<String, PromLabels>, output: &mut String) {
            let mut metrics = HashMap::new();

            // Iterate over the 'recv' or the 'sent' HashMap by peer name.
            for (peer, stats) in peer_stats.$field.iter() {

                // Generate labels for this peer.
                let mut labels = if let Some(pl) = labelmap.get(peer) {
                    pl.deep_clone()
                } else {
                    PromLabels::default()
                };
                labels.push("newspeer", peer);

                // Then, for each metric, add the stats for this peer.
                $(
                    let metric = metrics.entry(stringify!($metric)).or_insert_with(|| {
                        PromMetric::new(stringify!($metric), $help, $type)
                    });
                    let value = stats.$mfield.load(Ordering::Acquire);
                    metric.add_value(labels.clone(), value, None);
                )*
            }

            // Finally put all the metrics in a string.
            for metric in metrics.values() {
                metric.to_string(output);
            }
        }
    }
}

// This generates the prom_metrics_recv() function.
prom_metrics! {
    prom_metrics_recv, recv,
    nntp_input_connection_count => [ connections, "gauge", "Number of incoming connections" ],
    nntp_input_offered_count => [ offered, "counter", "Offered articles in (CHECK)" ],
    nntp_input_refused_count => [ refused, "counter", "Refused articles in (CHECK)" ],
    nntp_input_accepted_count => [ accepted, "counter", "Accepted articles in (CHECK)" ],
    nntp_input_received_count => [ received, "counter", "Received articles in (TAKETHIS)" ],
    nntp_input_rejected_count => [ rejected, "counter", "Rejected articles in (TAKETHIS)" ],
    nntp_input_accepted_bytes => [ accepted_bytes, "counter", "Accepted bytes in (TAKETHIS)" ],
    nntp_input_rejected_bytes => [ rejected_bytes, "counter", "Rejected bytes in (TAKETHIS)" ],
    nntp_input_received_bytes => [ received_bytes, "counter", "Received bytes in" ],
}

// This generates the prom_metrics_sent() function.
prom_metrics! {
    prom_metrics_sent, sent,
    nntp_output_connection_count => [ connections, "gauge", "Number of outgoing connections" ],
    // nntp_output_queueage_secs => [ max_queue_age, "counter", "Age of oldest queued article" ],
    nntp_output_offered_count => [ offered, "counter", "Offered articles out (CHECK)" ],
    nntp_output_accepted_count => [ accepted, "counter", "Accepted articles out (TAKETHIS)" ],
    nntp_output_refused_count => [ refused, "counter", "Refused articles out (CHECK)" ],
    nntp_output_rejected_count => [ rejected, "counter", "Rejected articles out (TAKETHIS)" ],
    nntp_output_deferfail_count => [ deferred_fail, "counter", "Defer fails (CHECK)" ],
    nntp_output_rejected_bytes => [ rejected_bytes, "counter", "Rejected bytes out (TAKETHIS)" ],
    nntp_output_accepted_bytes => [ accepted_bytes, "counter", "Accepted bytes out (TAKETHIS)" ],
}

#[derive(Default, Clone)]
struct PromLabels {
    labels: Rc<Vec<(String, String)>>,
}

impl PromLabels {
    fn push(&mut self, name: impl Into<String>, value: impl Into<String>) {
        Rc::get_mut(&mut self.labels)
            .unwrap()
            .push((name.into(), value.into()));
    }

    fn deep_clone(&self) -> PromLabels {
        PromLabels {
            labels: Rc::new((&*self.labels).clone()),
        }
    }

    fn to_string(&self) -> String {
        let mut s = String::new();
        for l in self.labels.iter() {
            if s.len() > 0 {
                s.push(',');
            }
            s.push_str(&format!("{}=\"{}\"", l.0, l.1));
        }
        s
    }
}

struct PromMetric {
    name:   &'static str,
    help:   &'static str,
    ptype:  &'static str,
    values: Vec<(u64, PromLabels, Option<UnixTime>)>,
}

impl PromMetric {
    fn new(name: &'static str, help: &'static str, ptype: &'static str) -> PromMetric {
        PromMetric {
            name,
            help,
            ptype,
            values: Vec::new(),
        }
    }

    fn add_value(&mut self, labels: PromLabels, mut value: u64, timestamp: Option<UnixTime>) {
        // we limit counters to 2**53 and then let them wrap around.
        // this way, we keep the precision of an integer.
        // (see also https://github.com/dirac-institute/zads-terraform/issues/32)
        if self.ptype == "counter" {
            value &= 0x1fffffffffffff;
        }

        self.values.push((value, labels, timestamp));
    }

    fn to_string(&self, s: &mut String) {
        if self.values.is_empty() {
            return;
        }
        s.push_str(&format!("# HELP {} {}\n", self.name, self.help));
        s.push_str(&format!("# TYPE {} {}\n", self.name, self.ptype));
        for value in &self.values {
            s.push_str(&self.name);
            let labs = value.1.to_string();
            if labs.len() > 0 {
                s.push_str(&format!("{{{}}}", labs));
            }
            s.push(' ');
            s.push_str(&value.0.to_string());
            if let Some(tm) = value.2 {
                s.push(' ');
                s.push_str(&tm.as_millis().to_string());
            }
            s.push('\n');
        }
    }
}
