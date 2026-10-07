//! Commits must not freeze behind a major compaction (`lsm-version-lock`).
//!
//! Under `stress-ng-fs-nodes` the meta store logged `slow commit: 15433 ms
//! for 16 items (memtables=14707ms)` right after lsm-tree's `Starting major
//! compaction` (`Meta::vacuum_churn` compacts a churn keyspace to its last
//! level). A commit's memtable insert first takes its tree's version lock
//! for reading (`Tree::append_entry`); see
//! `vendor/lsm-tree/CONSTELLATION-PATCH.md` for which section of a
//! compaction held it and how it no longer does.
//!
//! The test loads a keyspace with many tables, then runs its major
//! compaction while a writer thread commits small transactions back to
//! back, into that keyspace and into another one, and records each
//! transaction's latency (`write_tx` + inserts + `commit`). A disk-pressure
//! thread writes and fsyncs a scratch file next to the database meanwhile,
//! so every fsync of the compaction is as slow as on the busy host the
//! stall was seen on.
//!
//! Knobs (all optional): `LSM_STALL_KEYS` (rows loaded, default 400 000),
//! `LSM_STALL_PRESSURE=0` (no disk pressure), `LSM_STALL_MAX_MS` (the
//! asserted worst commit, default 1 000).
//!
//! The bound is coarse: it catches a freeze of the reported size, not the
//! 50–260 ms the unfixed store showed here on an idle NVMe (the fixed one:
//! well under a millisecond), which a loaded test host's scheduling could
//! reach too. The deterministic regression test is the vendored lsm-tree's
//! `inserts_never_wait_for_a_version_being_persisted`, which slows every
//! version persist by 400 ms. Numbers on a slow disk: run this with
//! `--release`, `LSM_STALL_KEYS=4000000` and `TMPDIR` on that disk.

use fjall::{KeyspaceCreateOptions, SingleWriterTxDatabase};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Cheap incompressible bytes, so flushes and compactions write real data.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let x = self.next().to_le_bytes();
            chunk.copy_from_slice(&x[..chunk.len()]);
        }
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

/// Writes and fsyncs 4 MiB at a time into a scratch file in `dir` until
/// `stop`: every other fsync on the filesystem queues behind it.
fn disk_pressure(dir: std::path::PathBuf, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let path = dir.join("pressure");
        let mut buf = vec![0u8; 4 << 20];
        XorShift(7).fill(&mut buf);
        let mut file = std::fs::File::create(&path).unwrap();
        let mut written = 0u64;
        while !stop.load(Ordering::Relaxed) {
            file.write_all(&buf).unwrap();
            file.sync_data().unwrap();
            written += buf.len() as u64;
            if written >= 256 << 20 {
                file.set_len(0).unwrap();
                file = std::fs::File::create(&path).unwrap();
                written = 0;
            }
        }
        let _ = std::fs::remove_file(&path);
    })
}

#[test]
fn a_major_compaction_never_freezes_commits() {
    let keys: u64 = env_or("LSM_STALL_KEYS", 400_000);
    let pressure = env_or("LSM_STALL_PRESSURE", 1u8) != 0;
    let max_ms: u64 = env_or("LSM_STALL_MAX_MS", 1_000);

    let dir = tempfile::Builder::new()
        .prefix("lsm-stall-")
        .tempdir()
        .unwrap();
    let db = SingleWriterTxDatabase::builder(dir.path().join("db"))
        .worker_threads(4)
        .open()
        .unwrap();
    // Small memtables: the big keyspace ends up in many tables, so its
    // major compaction has many inputs and many outputs, as a churn
    // keyspace's does after hours of service.
    let big = db
        .keyspace("big", || {
            KeyspaceCreateOptions::default().max_memtable_size(4 << 20)
        })
        .unwrap();
    let small = db
        .keyspace("small", KeyspaceCreateOptions::default)
        .unwrap();

    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let mut value = vec![0u8; 100];
    let load = Instant::now();
    for batch in 0..keys.div_ceil(1_000) {
        let mut tx = db.write_tx();
        for i in 0..1_000 {
            rng.fill(&mut value);
            tx.insert(&big, (batch * 1_000 + i).to_be_bytes(), value.clone());
        }
        tx.commit().unwrap();
    }
    big.inner().rotate_memtable().unwrap();
    while big.inner().sealed_memtable_count() > 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    eprintln!(
        "loaded {keys} rows in {:?}: {} tables",
        load.elapsed(),
        big.inner().table_count()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let pressure = pressure.then(|| disk_pressure(dir.path().to_path_buf(), stop.clone()));
    let writer = {
        let (db, big, small, stop) = (db.clone(), big.clone(), small.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut rng = XorShift(42);
            let mut latencies = Vec::new();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                let mut tx = db.write_tx();
                tx.insert(&small, i.to_be_bytes(), b"row".to_vec());
                tx.remove(&small, i.saturating_sub(100).to_be_bytes());
                tx.insert(&big, (rng.next() % keys).to_be_bytes(), b"churn".to_vec());
                tx.commit().unwrap();
                latencies.push(started.elapsed());
                i += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
            latencies
        })
    };

    std::thread::sleep(Duration::from_millis(200));
    let compaction = Instant::now();
    big.inner().major_compact().unwrap();
    let compaction = compaction.elapsed();
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    let mut latencies = writer.join().unwrap();
    if let Some(p) = pressure {
        p.join().unwrap();
    }

    latencies.sort();
    let max = *latencies.last().unwrap();
    eprintln!(
        "major compaction {compaction:?} ({} tables after); {} commits meanwhile: \
         p50 {:?} p99 {:?} p99.9 {:?} max {max:?}",
        big.inner().table_count(),
        latencies.len(),
        percentile(&latencies, 0.5),
        percentile(&latencies, 0.99),
        percentile(&latencies, 0.999),
    );
    assert!(
        max < Duration::from_millis(max_ms),
        "a commit took {max:?} during a major compaction (limit {max_ms} ms)"
    );
}
