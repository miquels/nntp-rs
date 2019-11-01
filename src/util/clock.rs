//! Interface to the clock_gettime systemcall.
//! Useful for network code where you need to get the current time often.
//!
//! Benchmark results:
//!
//! SystemTime::now()               28 ns
//! Instant::now()                  15 ns
//! libc::gettimeofday()            15 ns
//! libc::clock_gettime             15 ns
//! libc::clock_gettime (coarse)     5 ns
//!
use libc::{self, clockid_t};

/// Returned by clock::gettime()
pub struct Timespec {
    pub sec:  i64,
    pub nsec: i32,
}

/// Clock ID for clock::gettime, see also the manpage for the
/// clock_gettime() systemcall.
#[cfg(target_os = "linux")]
pub enum ClockId {
    /// realtime since the Unix epoch. can jump back or forward.
    Realtime        = libc::CLOCK_REALTIME as isize,
    /// like REALTIME, but with a granularity of about 1ms. 3x faster though.
    RealtimeCoarse  = libc::CLOCK_REALTIME_COARSE as isize,
    /// time since some internal epoch. monotonic, no jumps.
    Monotonic       = libc::CLOCK_MONOTONIC as isize,
    /// like MONOTONIC, but with a granularity of about 1ms. 3x faster though.
    MonotonicCoarse = libc::CLOCK_MONOTONIC_COARSE as isize,
    #[doc(hidden)]
    _Placeholder    = -1,
}

#[cfg(not(target_os = "linux"))]
#[derive(PartialEq, Eq)]
pub enum ClockId {
    /// realtime since the Unix epoch. can jump back or forward.
    Realtime     = libc::CLOCK_REALTIME as isize,
    /// time since some internal epoch. monotonic, no jumps.
    Monotonic    = libc::CLOCK_MONOTONIC as isize,
    #[doc(hidden)]
    _Placeholder = -1,
}
#[cfg(not(target_os = "linux"))]
#[allow(non_upper_case_globals)]
impl ClockId {
    /// on this non-linux platform RealtimeCoarse is the same as Realtime
    pub const RealtimeCoarse: ClockId = ClockId::Realtime;
    /// on this non-linux platform MonotonicCoarse is the same as Monotonic
    pub const MonotonicCoarse: ClockId = ClockId::Monotonic;
}

/// Read the internal clock of the system.
#[inline]
pub fn gettime(clk_id: ClockId) -> Timespec {
    let mut ts = libc::timespec {
        tv_sec:  0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(clk_id as clockid_t, &mut ts);
    }
    Timespec {
        sec:  ts.tv_sec as i64,
        nsec: ts.tv_nsec as i32,
    }
}

/// Return unix time in seconds.
#[inline(always)]
pub fn unixtime() -> u64 {
    let mut ts = libc::timespec {
        tv_sec:  0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(ClockId::RealtimeCoarse as clockid_t, &mut ts);
    }
    ts.tv_sec as u64
}

/// Return unix time in milliseconds.
#[inline(always)]
pub fn unixtime_ms() -> u64 {
    let mut ts = libc::timespec {
        tv_sec:  0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(ClockId::RealtimeCoarse as clockid_t, &mut ts);
    }
    1000 * ts.tv_sec as u64 + (ts.tv_nsec / 1000000) as u64
}

/// Return monotonic time in seconds.
#[inline(always)]
pub fn monotime() -> u64 {
    let mut ts = libc::timespec {
        tv_sec:  0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(ClockId::MonotonicCoarse as clockid_t, &mut ts);
    }
    ts.tv_sec as u64
}
/// Return monotonic time in milliseconds.
#[inline(always)]
pub fn monotime_ms() -> u64 {
    let mut ts = libc::timespec {
        tv_sec:  0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(ClockId::MonotonicCoarse as clockid_t, &mut ts);
    }
    1000 * ts.tv_sec as u64 + (ts.tv_nsec / 1000000) as u64
}
