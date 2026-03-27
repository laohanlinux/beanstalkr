//! Protocol command parsing microbenchmarks (`Command::parse`).
//!
//! Run: `cargo bench --bench cmd_parse`
//!
//! For real TCP + subprocess server workloads, see `benches/server_e2e.rs`.

use std::sync::atomic::Ordering;

use beanstalkr::architecture::cmd::Command;
use beanstalkr::architecture::stats::GLOBAL_STATS;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_parse_use(c: &mut Criterion) {
    let line = "use my-tube_name";
    c.bench_function("cmd_parse/use", |b| {
        b.iter(|| {
            let mut cmd = Command::default();
            let _ = black_box(cmd.parse(black_box(line)));
        });
    });
}

fn bench_parse_delete(c: &mut Criterion) {
    let line = "delete 98765";
    c.bench_function("cmd_parse/delete", |b| {
        b.iter(|| {
            let mut cmd = Command::default();
            let _ = black_box(cmd.parse(black_box(line)));
        });
    });
}

fn bench_parse_put_two_rounds(c: &mut Criterion) {
    GLOBAL_STATS
        .max_job_size
        .store(65535, Ordering::SeqCst);
    let line1 = "put 0 0 120 5";
    let line2 = "hello";
    c.bench_function("cmd_parse/put_two_rounds", |b| {
        b.iter(|| {
            let mut cmd = Command::default();
            let _ = black_box(cmd.parse(black_box(line1)));
            let _ = black_box(cmd.parse(black_box(line2)));
        });
    });
}

criterion_group!(
    benches,
    bench_parse_use,
    bench_parse_delete,
    bench_parse_put_two_rounds
);
criterion_main!(benches);
