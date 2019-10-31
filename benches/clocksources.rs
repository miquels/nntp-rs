use libc;
use std::time::{Instant, SystemTime};

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_timesources(c: &mut Criterion) {
    let mut group = c.benchmark_group("Time sources");
    group.bench_function("SystemTime::now()", |b| {
        b.iter(|| {
            SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs();
        })
    });
    group.bench_function("Instant::now()", |b| {
        b.iter(|| {
            Instant::now();
        })
    });
    group.bench_function("libc::gettimeofday", |b| {
        b.iter(|| {
            let mut ts = libc::timeval {
                tv_sec:  0,
                tv_usec: 0,
            };
            let nullptr: *mut libc::timezone = std::ptr::null_mut();
            unsafe {
                libc::gettimeofday(&mut ts, nullptr);
            }
        })
    });
    group.bench_function("libc::CLOCK_REALTIME", |b| {
        b.iter(|| {
            let mut ts = libc::timespec {
                tv_sec:  0,
                tv_nsec: 0,
            };
            unsafe {
                libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts);
            }
        })
    });
    group.bench_function("libc::CLOCK_REALTIME_COARSE", |b| {
        b.iter(|| {
            let mut ts = libc::timespec {
                tv_sec:  0,
                tv_nsec: 0,
            };
            unsafe {
                libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts);
            }
        })
    });
    group.bench_function("libc::CLOCK_MONOTONIC", |b| {
        b.iter(|| {
            let mut ts = libc::timespec {
                tv_sec:  0,
                tv_nsec: 0,
            };
            unsafe {
                libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
            }
        })
    });
    group.bench_function("libc::CLOCK_MONOTONIC_COARSE", |b| {
        b.iter(|| {
            let mut ts = libc::timespec {
                tv_sec:  0,
                tv_nsec: 0,
            };
            unsafe {
                libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts);
            }
        })
    });
    group.finish();
}

criterion_group!(benches, bench_timesources);
criterion_main!(benches);
