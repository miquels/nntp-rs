//! Interface to the clock_gettime systemcall.
//! Useful for network code where you need to get the current time often.
//!
//! Benchmark results:
//!
//! time::now_utc().to_timespec()   95 ns
//! libc::gettimeofday()            15 ns
//! libc::clock_gettime             15 ns
//! libc::clock_gettime (coarse)     5 ns
//!
use libc::{self,clockid_t};

pub use time::Timespec;

/// Clock ID for clock::gettime, see also the manpage for the
/// clock_gettime() systemcall.
pub enum ClockId {
    /// realtime since the Unix epoch. can jump back or forward.
    RealTime = libc::CLOCK_REALTIME as isize,
    /// like REALTIME, but with a granularity of about 1ms. 3x faster though.
    RealtimeCourse = libc::CLOCK_REALTIME_COARSE as isize,
    /// time since some internal epoch. monotonic, no jumps.
    Monotonic = libc::CLOCK_MONOTONIC as isize,
    /// like MONOTONIC, but with a granularity of about 1ms. 3x faster though.
    MonotonicCoarse = libc::CLOCK_MONOTONIC_COARSE as isize,
    #[doc(hidden)]
    _Placeholder = -1,
}

/// Read the internal clock of the system.
#[inline]
pub fn gettime(clk_id: ClockId) -> Timespec {
    let mut ts = libc::timespec{ tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(clk_id as clockid_t, &mut ts); }
    Timespec{
        sec:    ts.tv_sec as i64,
        nsec:   ts.tv_nsec as i32,
    }
}

/// Convenience function for gettime(ClockId::RealtimeCoarse), returns ms.
/// Called js_now() because it's basically javascript's Date.now()
#[inline(always)]
pub fn js_now() -> u64 {
	let mut ts = libc::timespec{ tv_sec: 0, tv_nsec: 0 };
	unsafe { libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts); }
	ts.tv_sec as u64 + (ts.tv_nsec / 1000000) as u64
}

/// Convenience function for gettime(ClockId::MonotonicCoarse), returns ms.
#[inline(always)]
pub fn mt_now() -> u64 {
	let mut ts = libc::timespec{ tv_sec: 0, tv_nsec: 0 };
	unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts); }
	ts.tv_sec as u64 + (ts.tv_nsec / 1000000) as u64
}


#[cfg(test)]
mod tests {
    use test::{Bencher, black_box};
    use time;
    use libc;

    #[bench]
    fn bench_time(b: &mut Bencher) {
        b.iter(|| {
             time::now_utc().to_timespec().sec as u64;
        });
    }

    #[bench]
    fn bench_clock_real(b: &mut Bencher) {
        b.iter(|| {
            let mut ts = libc::timespec{ tv_sec: 0, tv_nsec: 0 };
            unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts); }
        });
    }

    #[bench]
    fn bench_clock_mono(b: &mut Bencher) {
        b.iter(|| {
            let mut ts = libc::timespec{ tv_sec: 0, tv_nsec: 0 };
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts); }
        });
    }

    #[bench]
    fn bench_clock_mono_course(b: &mut Bencher) {
        b.iter(|| {
            let mut ts = libc::timespec{ tv_sec: 0, tv_nsec: 0 };
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts); }
        });
    }

    #[bench]
    fn bench_clock_gettimeofday(b: &mut Bencher) {
        use std;
        let nullptr : *mut libc::c_void = std::ptr::null_mut();
        b.iter(|| {
            let mut ts = libc::timeval{ tv_sec: 0, tv_usec: 0 };
            unsafe { libc::gettimeofday(&mut ts, nullptr); }
        });
    }
}

