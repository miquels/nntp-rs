// Benches for several nntp_rs functions and methods
//
// Just some checks to see if things are speedy enough.

use criterion::{criterion_group, criterion_main, Criterion};

use nntp_rs::util::Buffer;
use nntp_rs::article::{Headers, HeadersParser};

fn bench_nntp_rs(c: &mut Criterion) {
    let mut group = c.benchmark_group("nntp_rs functions");

    // This function peaks out at 20GB/sec on my office workstation. Good enough.
    group.bench_function("Headers", |b| {
        b.iter(|| {
            parse_headers();
        })
    });

    group.finish();
}

fn parse_headers() -> (Headers, Buffer) {
    let art = Buffer::from(include_str!("article.txt"));
    let mut parser = HeadersParser::new();
    parser.parse(&art, false, true).expect("failed to parse headers").unwrap();
    let (headers, body) = parser.into_headers(art);
    (headers, body)
}

criterion_group!(benches, bench_nntp_rs);
criterion_main!(benches);
