//! Diagnostics, statistics and telemetry.

use std::default::Default;
use std::time::Instant;

#[repr(usize)]
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
            hostname:   String::new(),
            ipaddr:     String::new(),
            label:      String::new(),
            fdno:       0,
            instant:    Instant::now(),
            stats:      [0u64; Stats::NumSlots as usize],
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

    pub fn on_connect(&mut self, hostname: String, label: String) {
        self.hostname = hostname;
        self.label = label;
        info!("Connection {} from {} {} [{}]", self.fdno, self.hostname, self.ipaddr, self.label);
    }

    pub fn on_disconnect(&self) {
        let elapsed = self.instant.elapsed().as_secs();
        info!("Disconnect {} from {} {} ({} elapsed)", self.fdno, self.hostname, self.ipaddr, elapsed);
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

    	info!("{} secs={} ihave={} chk={} takethis={} rec={} acc={} ref={} precom={} postcom={} his={} badmsgid={} ifilthash={} rej={} ctl={} spam={} err={} recbytes={} accbytes={} rejbytes={} ({}/sec)",
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
			nuse as u64 / dt,
        );
    }

    pub fn log_rejstats(&self) {
        info!("{} rejstats rej={} failsafe={} misshdrs={} tooold={} grpfilt={} intspamfilt={} extspamfilt={} incfilter={} nospool={} ioerr={} notinactv={} pathtab={} ngtab={} posdup={} hdrerr={} toosmall={} incompl={} nul={} nobytes={} proto={} msgidmis={} nohdrend={} bighdr={} barecr={} err={}",
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
            self.stats[Stats::RejErr as usize]
        );
    }
}

