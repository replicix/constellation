//! A checkpoint snapshot must not stall concurrent metadata operations.
//!
//! `snapshot()` copies the whole database, which costs roughly 2ms per MiB
//! and therefore grows without bound as the namespace grows. It used to run
//! while holding the store's single connection, so every FUSE operation
//! queued behind it: measured at a 23ms `getattr` against a 30k-file DB,
//! ~7000x that DB's median. A file-backed store now copies through its own
//! read-only connection, which WAL makes both consistent and concurrent
//! with writers.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{MetaStore, SqliteMeta};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Files needed to make the copy slow enough that a stall would be obvious.
const FILES: usize = 30_000;

fn populate(meta: &SqliteMeta, files: usize) -> Vec<u64> {
    let mut inos = Vec::with_capacity(files);
    for i in 0..files {
        let attr = meta
            .create(ROOT_INO, &format!("f{i}"), 0o644, 1000, 1000)
            .unwrap();
        // Manifests dominate row width, so they set the snapshot's size.
        meta.set_manifest(attr.ino, &[7u8; 64], 4096).unwrap();
        inos.push(attr.ino);
    }
    inos
}

#[test]
fn snapshot_does_not_stall_concurrent_readers() {
    let dir = tempfile::TempDir::new().unwrap();
    let meta = Arc::new(SqliteMeta::open(dir.path().join("meta.db")).unwrap());
    let inos = populate(&meta, FILES);

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let meta = Arc::clone(&meta);
        let stop = Arc::clone(&stop);
        let inos = inos.clone();
        std::thread::spawn(move || {
            let mut samples = Vec::new();
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                meta.getattr(inos[i % inos.len()]).unwrap();
                samples.push(t.elapsed());
                i += 1;
            }
            samples
        })
    };

    // Let the reader settle, snapshot underneath it, then let it run on.
    std::thread::sleep(Duration::from_millis(200));
    let started = Instant::now();
    let snap = meta.snapshot().unwrap();
    let snapshot_time = started.elapsed();
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);

    let mut samples = reader.join().unwrap();
    assert!(
        samples.len() > 1000,
        "reader took too few samples to be meaningful: {}",
        samples.len()
    );
    samples.sort();
    let median = samples[samples.len() / 2];
    let worst = *samples.last().unwrap();

    // The bound is deliberately loose — this guards against serialization on
    // the store's connection, not against scheduler noise. Holding the lock
    // across the copy put the worst read in the tens of milliseconds and at
    // thousands of times the median; either check catches a regression.
    assert!(
        worst < snapshot_time / 2,
        "a read blocked for {worst:?}, about as long as the {snapshot_time:?} snapshot — \
         snapshot() is holding the store connection"
    );
    assert!(
        worst < median * 500,
        "worst read {worst:?} is {}x the median {median:?}; snapshot() is serializing reads",
        worst.as_nanos() / median.as_nanos().max(1)
    );
    assert!(!snap.is_empty(), "snapshot payload must not be empty");
}

#[test]
fn snapshot_does_not_stall_concurrent_writers() {
    let dir = tempfile::TempDir::new().unwrap();
    let meta = Arc::new(SqliteMeta::open(dir.path().join("meta.db")).unwrap());
    let inos = populate(&meta, FILES);

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let meta = Arc::clone(&meta);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut samples = Vec::new();
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                meta.setattr(inos[i % inos.len()], Some(0o600), None, None, None, None, None)
                    .unwrap();
                samples.push(t.elapsed());
                i += 1;
            }
            samples
        })
    };

    std::thread::sleep(Duration::from_millis(200));
    let started = Instant::now();
    meta.snapshot().unwrap();
    let snapshot_time = started.elapsed();
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);

    let mut samples = writer.join().unwrap();
    samples.sort();
    let worst = *samples.last().unwrap();
    assert!(
        worst < snapshot_time / 2,
        "a write blocked for {worst:?} against a {snapshot_time:?} snapshot — \
         the copy is not concurrent with writers"
    );
}

/// The payload must carry the deletes that strip journal rows and node
/// identity. Dropping the second `VACUUM` removed the rewrite that used to
/// flush them, so this pins the replacement.
#[test]
fn snapshot_payload_is_stripped_and_restorable() {
    let dir = tempfile::TempDir::new().unwrap();
    let meta = SqliteMeta::open(dir.path().join("meta.db")).unwrap();
    meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
    let file = meta.create(ROOT_INO, "keep.txt", 0o644, 7, 9).unwrap();
    assert!(meta.journal_len().unwrap() > 0, "expected journaled records");

    let snap = meta.snapshot().unwrap();
    let restored_path = dir.path().join("restored.db");
    std::fs::write(&restored_path, &snap).unwrap();
    let restored = SqliteMeta::open(&restored_path).unwrap();

    assert_eq!(
        restored.journal_len().unwrap(),
        0,
        "a restored replica must not re-ship our records"
    );
    assert!(restored.lookup(ROOT_INO, "d").unwrap().is_some());
    let kept = restored.lookup(ROOT_INO, "keep.txt").unwrap().unwrap();
    assert_eq!(kept.ino, file.ino);
    assert_eq!((kept.uid, kept.gid), (7, 9));
}
