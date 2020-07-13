//! Diagnostics, statistics and telemetry.

use std::default::Default;
use std::net::IpAddr;
use std::time::Instant;

use crate::article::Article;
use crate::dns;
use crate::errors::ArtError;

#[repr(usize)]
#[rustfmt::skip]
pub enum Stats {
    Offered,            // offered (ihave/check)
    Accepted,           // accepted
    AcceptedBytes,
    Received,           // received
    ReceivedBytes,
    Refused,            // refused
    RefHistory,         // ref, in history
    RefPreCommit,       // ref, offered by another host
    RefPostCommit,      // ref, in history cache
    RefBadMsgId,        // ref, bad msgid
    RefIfiltHash,       // ref, by IFILTER hash
    Ihave,              // ihave
    Check,              // check
    Takethis,           // takethis
    Control,            // control message
    Rejected,           // rejected
    RejectedBytes,
    RejFailsafe,        // rej, failsafe
    RejMissHdrs,        // rej, missing headers
    RejTooOld,          // rej, too old
    RejGrpFilter,       // rej, incoming grp filter
    RejIntSpamFilter,   // rej, internal spam filter
    RejExtSpamFilter,   // rej, external spam filter
    RejIncFilter,       // rej, incoming filter
    RejNoSpool,         // rej, no spool object
    RejIOError,         // rej, io error
    RejNotInActv,       // rej, not in active file
    RejPathTab,         // rej, TAB in Path: header
    RejNgTab,           // rej, TAB in Newsgroups: hdr
    RejPosDup,          // rej, dup detected after receive
    RejHdrError,        // rej, dup or missing headers
    RejTooSmall,        // rej, article too small
    RejArtIncompl,      // rej, article incomplete
    RejArtNul,          // rej, article has a nul
    RejNoBytes,         // rej, header only, no Bytes:
    RejProtoErr,        // rej, protocol error
    RejMsgIdMis,        // rej, msgid mismatch
    RejErr,             // rej, unknown error
    RejTooBig,          // rej, too big
    RejBigHeader,       // header too big
    RejNoHdrEnd,        // header too big
    RejBareCR,          // article has a CR without LF
    NumSlots,
}

#[rustfmt::skip]
pub struct SessionStats {
    // identification.
    pub hostname:   String,
    pub ipaddr:     String,
    pub label:      String,
    pub fdno:       u32,
    pub instant:     Instant,
    // stats
    pub stats:      [u64; Stats::NumSlots as usize],
}

impl Default for SessionStats {
    fn default() -> SessionStats {
        SessionStats {
            hostname: String::new(),
            ipaddr:   String::new(),
            label:    String::new(),
            fdno:     0,
            instant:  Instant::now(),
            stats:    [0u64; Stats::NumSlots as usize],
        }
    }
}

impl SessionStats {
    pub fn add(&mut self, field: Stats, count: u64) {
        let n = field as usize;
        if n >= Stats::RefHistory as usize && n <= Stats::RefIfiltHash as usize {
            self.stats[Stats::Refused as usize] += count;
        }
        if n >= Stats::RejFailsafe as usize {
            self.stats[Stats::Rejected as usize] += count;
        }
        self.stats[n] += count;
    }

    pub fn inc(&mut self, field: Stats) {
        self.add(field, 1);
    }

    pub async fn on_connect(&mut self, ipaddr_str: String, label: String) {
        let ipaddr: std::net::IpAddr = ipaddr_str.parse().unwrap();
        let host = match dns::RESOLVER.reverse_lookup(ipaddr).await {
            Ok(m) => m.iter().next().map(|name| name.to_utf8()),
            Err(_) => None,
        };
        self.hostname = host.unwrap_or(ipaddr_str);
        self.label = label;
        log::info!(
            "Connection {} from {} {} [{}]",
            self.fdno,
            self.hostname,
            self.ipaddr,
            self.label
        );
    }

    pub fn on_disconnect(&self) {
        let elapsed = self.instant.elapsed().as_secs();
        log::info!(
            "Disconnect {} from {} {} ({} elapsed)",
            self.fdno,
            self.hostname,
            self.ipaddr,
            elapsed
        );
        self.log_stats();
        if self.stats[Stats::Rejected as usize] > 0 {
            self.log_rejstats();
        }
    }

    pub fn log_stats(&self) {
        // This calculation comes straight from diablo, not sure
        // why it is done this way.
        let mut nuse = self.stats[Stats::Check as usize] + self.stats[Stats::Ihave as usize];
        if nuse < self.stats[Stats::Received as usize] {
            nuse = self.stats[Stats::Received as usize];
        }

        let elapsed = self.instant.elapsed().as_secs();
        let dt = std::cmp::max(1, elapsed);
        let mut rate = nuse as f64 / (dt as f64);
        if rate >= 10.0 {
            rate = rate.round();
        }

        log::info!("{} secs={} ihave={} chk={} takethis={} rec={} acc={} ref={} precom={} postcom={} his={} badmsgid={} ifilthash={} rej={} ctl={} spam={} err={} recbytes={} accbytes={} rejbytes={} ({}/sec)",
            self.hostname,
            self.instant.elapsed().as_secs(),
            self.stats[Stats::Ihave as usize],
            self.stats[Stats::Check as usize],
            self.stats[Stats::Takethis as usize],
            self.stats[Stats::Received as usize],
            self.stats[Stats::Accepted as usize],
            self.stats[Stats::Refused as usize],
            self.stats[Stats::RefPreCommit as usize],
            self.stats[Stats::RefPostCommit as usize],
            self.stats[Stats::RefHistory as usize],
            self.stats[Stats::RefBadMsgId as usize],
            self.stats[Stats::RefIfiltHash as usize],
            self.stats[Stats::Rejected as usize],
            self.stats[Stats::Control as usize],
            self.stats[Stats::RejIntSpamFilter as usize] + self.stats[Stats::RejExtSpamFilter as usize],
            self.stats[Stats::RejErr as usize],
            self.stats[Stats::ReceivedBytes as usize],
            self.stats[Stats::AcceptedBytes as usize],
            self.stats[Stats::RejectedBytes as usize],
            rate,
        );
    }

    pub fn log_rejstats(&self) {
        log::info!("{} rejstats rej={} failsafe={} misshdrs={} tooold={} grpfilt={} intspamfilt={} extspamfilt={} incfilter={} nospool={} ioerr={} notinactv={} pathtab={} ngtab={} posdup={} hdrerr={} toosmall={} incompl={} nul={} nobytes={} proto={} msgidmis={} nohdrend={} bighdr={} barecr={} err={} toobig={}",
            self.hostname,
            self.stats[Stats::Rejected as usize],
            self.stats[Stats::RejFailsafe as usize],
            self.stats[Stats::RejMissHdrs as usize],
            self.stats[Stats::RejTooOld as usize],
            self.stats[Stats::RejGrpFilter as usize],
            self.stats[Stats::RejIntSpamFilter as usize],
            self.stats[Stats::RejExtSpamFilter as usize],
            self.stats[Stats::RejIncFilter as usize],
            self.stats[Stats::RejNoSpool as usize],
            self.stats[Stats::RejIOError as usize],
            self.stats[Stats::RejNotInActv as usize],
            self.stats[Stats::RejPathTab as usize],
            self.stats[Stats::RejNgTab as usize],
            self.stats[Stats::RejPosDup as usize],
            self.stats[Stats::RejHdrError as usize],
            self.stats[Stats::RejTooSmall as usize],
            self.stats[Stats::RejArtIncompl as usize],
            self.stats[Stats::RejArtNul as usize],
            self.stats[Stats::RejNoBytes as usize],
            self.stats[Stats::RejProtoErr as usize],
            self.stats[Stats::RejMsgIdMis as usize],
            self.stats[Stats::RejNoHdrEnd as usize],
            self.stats[Stats::RejBigHeader as usize],
            self.stats[Stats::RejBareCR as usize],
            self.stats[Stats::RejErr as usize],
            self.stats[Stats::RejTooBig as usize],
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

#[repr(usize)]
#[rustfmt::skip]
pub enum TxStats {
    Offered,            // offered (ihave/check)
    Accepted,           // accepted
    AcceptedBytes,
    Rejected,
    RejectedBytes,
    Refused,
    Deferred,
    DeferredBytes,
    DeferredFail,
    NotFound,
    NumSlots,
}

#[rustfmt::skip]
pub struct TxSessionStats {
    // identification.
    pub label:      String,
    pub id:         u64,
    pub start:      Instant,
    pub mark:       Instant,
    // stats
    pub stats:      [u64; TxStats::NumSlots as usize],
    pub total:      [u64; TxStats::NumSlots as usize],
}

impl Default for TxSessionStats {
    fn default() -> TxSessionStats {
        TxSessionStats {
            label: String::new(),
            id:    0,
            start: Instant::now(),
            mark:  Instant::now(),
            stats: [0u64; TxStats::NumSlots as usize],
            total: [0u64; TxStats::NumSlots as usize],
        }
    }
}

impl TxSessionStats {
    pub fn add(&mut self, field: TxStats, count: u64) {
        let n = field as usize;
        self.stats[n] += count;
    }

    pub fn inc(&mut self, field: TxStats) {
        self.add(field, 1);
    }

    pub fn on_connect(&mut self, label: &str, id: u64, outhost: &str, ipaddr: IpAddr, line: &str) {
        self.label = label.to_string();
        self.id = id;
        self.start = Instant::now();
        self.mark = Instant::now();
        log::info!("{}:{} connect: {} ({}/{})", label, id, line, outhost, ipaddr);
    }

    pub fn stats_update(&mut self) {
        log::info!(
            "{}:{} mark {}",
            self.label,
            self.id,
            self.log_stats(self.mark, &self.stats)
        );
        if self.stats[TxStats::Deferred as usize] > 0 || self.stats[TxStats::DeferredFail as usize] > 0 {
            log::info!(
                "{}:{} mark {}",
                self.label,
                self.id,
                self.log_stats(self.mark, &self.stats)
            );
        }
        self.update_total();
    }

    pub fn stats_final(&mut self) {
        self.update_total();
        log::info!(
            "{}:{} final {}",
            self.label,
            self.id,
            self.log_stats(self.start, &self.total)
        );
        if self.total[TxStats::Deferred as usize] > 0 || self.total[TxStats::DeferredFail as usize] > 0 {
            log::info!(
                "{}:{} final {}",
                self.label,
                self.id,
                self.log_defer(self.start, &self.total)
            );
        }
    }

    fn update_total(&mut self) {
        self.mark = Instant::now();
        for i in 0..TxStats::NumSlots as usize {
            self.total[i] += self.stats[i];
            self.stats[i] = 0;
        }
    }

    pub fn log_stats(&self, start: Instant, stats: &[u64]) -> String {
        let secs = self.mark.saturating_duration_since(start).as_secs();
        format!(
            "secs={} acc={} dup={} rej={} tot={} bytes={} ({}/min) avpend={:.1}",
            secs,
            stats[TxStats::Accepted as usize],
            stats[TxStats::Refused as usize],
            stats[TxStats::Rejected as usize],
            stats[TxStats::Offered as usize],
            stats[TxStats::AcceptedBytes as usize],
            if secs > 0 {
                stats[TxStats::Offered as usize] * 60 / secs
            } else {
                0
            },
            1,
        )
    }

    pub fn log_defer(&self, start: Instant, stats: &[u64]) -> String {
        let secs = self.mark.saturating_duration_since(start).as_secs();
        format!(
            "secs={} defer={} deferfail={}",
            secs,
            stats[TxStats::Deferred as usize],
            stats[TxStats::DeferredFail as usize],
        )
    }

    // About to send TAKETHIS but article not present in spool anymore.
    pub fn art_notfound(&mut self) {
        self.inc(TxStats::Offered);
        self.inc(TxStats::NotFound);
    }

    // CHECK 431 (and incorrectly, TAKETHIS 431)
    pub fn art_deferred(&mut self, size: Option<usize>) {
        self.inc(TxStats::Offered);
        self.inc(TxStats::Deferred);
        if let Some(size) = size {
            self.add(TxStats::DeferredBytes, size as u64);
        }
    }

    // CHECK 431 (and incorrectly, TAKETHIS 431) where we failed to re-queue.
    pub fn art_deferred_fail(&mut self, size: Option<usize>) {
        self.inc(TxStats::Offered);
        self.inc(TxStats::DeferredFail);
        if let Some(size) = size {
            self.add(TxStats::DeferredBytes, size as u64);
        }
    }

    // CHECK 438
    pub fn art_refused(&mut self) {
        self.inc(TxStats::Offered);
        self.inc(TxStats::Refused);
    }

    // TAKETHIS 239
    pub fn art_accepted(&mut self, size: usize) {
        self.inc(TxStats::Offered);
        self.inc(TxStats::Accepted);
        self.add(TxStats::AcceptedBytes, size as u64);
    }

    // TAKETHIS 439
    pub fn art_rejected(&mut self, size: usize) {
        self.inc(TxStats::Offered);
        self.inc(TxStats::Rejected);
        self.add(TxStats::RejectedBytes, size as u64);
    }
}
