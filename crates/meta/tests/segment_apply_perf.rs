//! What a tailed segment costs a delegate whose own stream is far ahead of
//! the log (the xcheckfs two-node runs: a delegate holding 142,000
//! unappended `Local` rows spent 0.5 to 0.65 s on every tailed segment, fell
//! tens of thousands of segments behind, and a forwarded op waiting for the
//! log waited 20 minutes). An `#[ignore]`d measurement (release mode, no
//! FUSE): per-segment apply time against the number of the delegate's
//! outstanding rows, for a segment elsewhere in the tree and for one that
//! brings some of the delegate's own rows back; and the delegate's own
//! op before and after a segment looked at its rows.
//!
//! Env: `SA_DIR` (a scratch path, removed first), `SA_SIZES` (outstanding
//! row counts, comma-separated, default `1000,10000,40000`), `SA_SEGS`
//! (segments timed per size, default 40).
//!
//!   SA_DIR=/path/db cargo test --release -p constellation-meta \
//!       --test segment_apply_perf -- --ignored --nocapture --test-threads=1
//!
//! Every result line is `RESULT name value unit`.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MutateOp, Rid, TouchSet};
use constellation_types::clock::Now;
use std::path::Path;
use std::time::{Duration, Instant};

fn rid(seq: u64) -> Rid {
    Rid {
        node: 3,
        incarnation: 1,
        seq,
    }
}

fn p50(v: &mut [Duration]) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn create(parent: u64, name: String, ino: u64) -> LogRecord {
    LogRecord::Create {
        parent,
        name,
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: 5,
    }
}

#[test]
#[ignore]
fn segment_apply_against_the_delegates_backlog() {
    let dir = std::env::var("SA_DIR").expect("SA_DIR");
    let sizes: Vec<u64> = std::env::var("SA_SIZES")
        .unwrap_or_else(|_| "1000,10000,40000".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    let segs: u64 = std::env::var("SA_SEGS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    for n in sizes {
        let db = Path::new(&dir).join(format!("n{n}"));
        let _ = std::fs::remove_dir_all(&db);
        let meta = Meta::open(&db).unwrap();
        let (d, e) = ((2 << 40) | 1, (4 << 40) | 1);
        let mut seq = 1;
        let mkdirs: Vec<LogRecord> = [("d", d), ("e", e)]
            .into_iter()
            .map(|(name, ino)| LogRecord::Mkdir {
                parent: ROOT_INO,
                name: name.into(),
                ino,
                mode: 0o755,
                uid: 0,
                gid: 0,
                time_ns: 1,
            })
            .collect();
        meta.apply_segment(seq, 1, &mkdirs, &TouchSet::default())
            .unwrap();
        // The delegate of `d` (generation 7) executes n creates in it.
        let gen = 7;
        let mut own: Vec<(u64, Vec<LogRecord>)> = Vec::new();
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
            let (records, idx) = meta
                .delegate_execute(
                    &op,
                    Some(rid(i)),
                    gen,
                    Default::default(),
                    &Default::default(),
                    Now::same(0),
                )
                .unwrap();
            own.push((idx, records));
        }
        println!(
            "RESULT delegate_execute_n{n} {:.2} us/op",
            t.elapsed().as_secs_f64() * 1e6 / n as f64
        );
        assert_eq!(meta.speculation_counts().unwrap().local, n);
        // Segments elsewhere in the tree: 10 creates in `e` each.
        let mut ino = 4 << 40 | 100;
        let mut times = Vec::new();
        for _ in 0..segs {
            seq += 1;
            let records: Vec<LogRecord> = (0..10)
                .map(|_| {
                    ino += 1;
                    create(e, format!("x{ino}"), ino)
                })
                .collect();
            let t = Instant::now();
            let applied = meta
                .apply_segment(seq, 1, &records, &TouchSet::default())
                .unwrap();
            times.push(t.elapsed());
            assert!(!applied.inserted_before_local);
        }
        println!(
            "RESULT segment_elsewhere_n{n} {:.1} us",
            p50(&mut times).as_secs_f64() * 1e6
        );
        // Segments that bring the delegate's own rows back (5 a segment,
        // oldest first, as the root appends them), with 5 creates in `e`.
        let mut times = Vec::new();
        let mut jseq = 0;
        let mut own = own.into_iter();
        for _ in 0..segs {
            seq += 1;
            let mut records = Vec::new();
            let mut rows = Vec::new();
            let mut origins = Vec::new();
            for _ in 0..5 {
                ino += 1;
                jseq += 1;
                records.push(create(e, format!("x{ino}"), ino));
                rows.push(jseq);
                origins.push((0, 0));
            }
            for _ in 0..5 {
                let (idx, recs) = own.next().unwrap();
                for r in recs {
                    if !matches!(r, LogRecord::Atime { .. }) {
                        jseq += 1;
                        rows.push(jseq);
                        origins.push((gen, idx));
                    }
                    records.push(r);
                }
            }
            let t = Instant::now();
            meta.apply_segment_rows(
                seq,
                1,
                jseq,
                &rows,
                &origins,
                &records,
                &TouchSet::default(),
            )
            .unwrap();
            times.push(t.elapsed());
        }
        println!(
            "RESULT segment_own_rows_n{n} {:.1} us",
            p50(&mut times).as_secs_f64() * 1e6
        );
        assert_eq!(meta.speculation_counts().unwrap().local, n - 5 * segs);
        // More of the delegate's ops, once a segment has looked at its rows.
        let more = (n / 4).max(1);
        let t = Instant::now();
        for i in n + 1..=n + more {
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
                Some(rid(i)),
                gen,
                Default::default(),
                &Default::default(),
                Now::same(0),
            )
            .unwrap();
        }
        println!(
            "RESULT delegate_execute_after_n{n} {:.2} us/op",
            t.elapsed().as_secs_f64() * 1e6 / more as f64
        );
        drop(meta);
        let _ = std::fs::remove_dir_all(&db);
    }
}
