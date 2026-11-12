//! What a steady-state backup's append costs over a tenure (ADR-59 part A,
//! review 3 S1): the holder appends a transaction, the segment that ships
//! it lands, and the backup trims it, again and again. Every trim leaves
//! tombstones below the live tail; an append that probed the keyspace from
//! the epoch's start walked them all. An `#[ignore]`d measurement (release
//! mode, no FUSE): per-append time in windows over the run.
//!
//! Env: `BA_DIR` (a scratch path, removed first), `BA_N` (appends, default
//! 40000), `BA_WINDOW` (appends per reported window, default 10000).
//!
//!   BA_DIR=/path/db cargo test --release -p constellation-meta \
//!       --test backup_append_perf -- --ignored --nocapture --test-threads=1
//!
//! Every result line is `RESULT name value unit`.

use constellation_meta::{BackupTx, LogRecord, Meta};
use std::time::{Duration, Instant};

fn env(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

#[test]
#[ignore]
fn backup_append_after_trims() {
    let dir = std::path::PathBuf::from(
        std::env::var("BA_DIR").unwrap_or_else(|_| "/tmp/backup-append-perf".into()),
    );
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let n = env("BA_N", 40_000);
    let window = env("BA_WINDOW", 10_000).max(1);
    let meta = Meta::open(dir.join("m.db")).unwrap();
    let mut appends = Duration::ZERO;
    let mut trims = Duration::ZERO;
    let started = Instant::now();
    for i in 0..n {
        let first = 2 * i + 1;
        let ns = i as i64;
        let tx = BackupTx {
            first,
            last: first + 1,
            records: vec![
                LogRecord::Atime {
                    ino: 1 << 40 | (i + 2),
                    atime_ns: ns,
                    time_ns: ns,
                },
                LogRecord::Atime {
                    ino: 1 << 40 | (i + 2),
                    atime_ns: ns + 1,
                    time_ns: ns + 1,
                },
            ],
            origin: (0, 0),
        };
        let t = Instant::now();
        meta.backup_append(1, std::slice::from_ref(&tx)).unwrap();
        appends += t.elapsed();
        let t = Instant::now();
        meta.backup_trim(1, first + 1, &[]).unwrap();
        trims += t.elapsed();
        if (i + 1) % window == 0 {
            println!(
                "RESULT append_us_window_{} {:.2} us",
                (i + 1) / window,
                appends.as_secs_f64() * 1e6 / window as f64
            );
            println!(
                "RESULT trim_us_window_{} {:.2} us",
                (i + 1) / window,
                trims.as_secs_f64() * 1e6 / window as f64
            );
            appends = Duration::ZERO;
            trims = Duration::ZERO;
        }
    }
    println!("RESULT total_s {:.3} s", started.elapsed().as_secs_f64());
    meta.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
