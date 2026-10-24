//! What a delegate's reads of its own outstanding stream cost against the
//! number of its transactions not yet back through the log (the xcheckfs
//! two-node runs: a delegate held 142,000). An `#[ignore]`d measurement
//! (release mode, no FUSE), per call, p50:
//!
//! - `stream_batch`: the next stream or backup batch near the stream's
//!   end (`delegate_txs_from`, 256 rows, as `delegation_stream_rows`);
//! - `tx_pending`: whether one transaction is still here
//!   (`delegate_tx_pending`, what a parked acknowledgement asks);
//! - `origins_tail`: the origins of the stream's last 256 journal rows,
//!   and `origins_head`: of the journal's first 10,000 (`journal_origins`,
//!   a ship envelope; `segment_batch`);
//! - `delegate_execute`: the delegate's own op, building the stream.
//!
//! Env: `DS_SIZES` (outstanding transaction counts, comma-separated,
//! default `1000,10000,100000`), `DS_CALLS` (calls timed per read,
//! default 40).
//!
//!   cargo test --release -p constellation-meta --test delegate_stream_perf \
//!       -- --ignored --nocapture --test-threads=1
//!
//! Every result line is `RESULT name value unit`.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MetaStore, MutateOp, Rid, TouchSet};
use constellation_types::clock::Now;
use std::time::{Duration, Instant};

fn p50(v: &mut [Duration]) -> f64 {
    v.sort();
    v[v.len() / 2].as_secs_f64() * 1e6
}

fn timed(calls: u64, mut f: impl FnMut(u64)) -> f64 {
    let mut times = Vec::new();
    for i in 0..calls {
        let t = Instant::now();
        f(i);
        times.push(t.elapsed());
    }
    p50(&mut times)
}

#[test]
#[ignore]
fn delegate_stream_reads_against_the_backlog() {
    let sizes: Vec<u64> = std::env::var("DS_SIZES")
        .unwrap_or_else(|_| "1000,10000,100000".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    let calls: u64 = std::env::var("DS_CALLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    for n in sizes {
        let meta = Meta::open_in_memory().unwrap();
        let d = (2 << 40) | 1;
        let mkdir = LogRecord::Mkdir {
            parent: ROOT_INO,
            name: "d".into(),
            ino: d,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        };
        meta.apply_segment(1, 1, &[mkdir], &TouchSet::default())
            .unwrap();
        let gen = 7;
        let t = Instant::now();
        for i in 1..=n {
            let op = MutateOp::Create {
                parent: d,
                name: format!("f{i}"),
                ino: (2 << 40) | (10 + i),
                mode: 0o644,
                uid: 0,
                gid: 0,
            };
            meta.delegate_execute(
                &op,
                Some(Rid {
                    node: 3,
                    incarnation: 1,
                    seq: i,
                }),
                gen,
                Default::default(),
                &Default::default(),
                Now::same(0),
            )
            .unwrap();
        }
        println!(
            "RESULT delegate_execute_n{n} {:.2} us/op",
            t.elapsed().as_secs_f64() * 1e6 / n as f64
        );
        let journal: Vec<u64> = meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        let us = timed(calls, |i| {
            let txs = meta.delegate_txs_from(gen, n - 100 - i % 50, 256).unwrap();
            assert!(!txs.is_empty());
        });
        println!("RESULT stream_batch_n{n} {us:.1} us");
        let us = timed(calls, |i| {
            assert!(meta.delegate_tx_pending(gen, n - 50 - i % 50).unwrap());
        });
        println!("RESULT tx_pending_n{n} {us:.1} us");
        let tail = &journal[journal.len().saturating_sub(256)..];
        let us = timed(calls, |_| {
            assert_eq!(meta.journal_origins(tail).unwrap().len(), tail.len());
        });
        println!("RESULT origins_tail_n{n} {us:.1} us");
        let head = &journal[..journal.len().min(10_000)];
        let us = timed(calls.min(10), |_| {
            assert_eq!(meta.journal_origins(head).unwrap().len(), head.len());
        });
        println!("RESULT origins_head_n{n} {us:.1} us");
    }
}
