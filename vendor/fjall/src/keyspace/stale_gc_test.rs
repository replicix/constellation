// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 12): a memtable
// rotation's version GC does not hold `keyspaces.read()` while it frees.
#![expect(clippy::expect_used, reason = "tests: a failed step is a failed test")]

use super::stale_gc_hook;
use crate::{Database, KeyspaceCreateOptions};
use std::time::{Duration, Instant};
use test_log::test;

const GC_DELAY: Duration = Duration::from_millis(300);

/// A rotation that is slow to free a keyspace's stale versions must not
/// hold up a commit: a journal rotation waiting for `keyspaces.write()`
/// holds the journal writer, which every commit takes. Before change 12 the
/// commit waited as long as the GC loop, the whole `GC_DELAY`.
#[test]
fn a_slow_version_gc_does_not_stall_commits_behind_a_journal_rotation() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().to_path_buf();
    let db = Database::builder(&folder).worker_threads(2).open()?;
    let ks = db.keyspace("gc", KeyspaceCreateOptions::default)?;
    let other = db.keyspace("other", KeyspaceCreateOptions::default)?;

    // Garbage: a snapshot held across flushes, then released.
    ks.insert(b"a", b"1")?;
    let snapshot = db.snapshot();
    ks.rotate_memtable_and_wait()?;
    ks.insert(b"b", b"2")?;
    ks.rotate_memtable_and_wait()?;
    ks.insert(b"c", b"3")?;
    ks.rotate_memtable_and_wait()?;
    drop(snapshot);
    ks.insert(b"d", b"4")?;

    stale_gc_hook::set(&folder, GC_DELAY);
    let rotator = std::thread::spawn(move || ks.rotate_memtable_and_wait());
    // Wait until the rotation is inside the hook.
    let started = Instant::now();
    while stale_gc_hook::fired() == 0 && started.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(stale_gc_hook::fired() > 0, "the version GC had no garbage");

    let journal = {
        let db = db.clone();
        std::thread::spawn(move || db.rotate_journal_and_evict())
    };
    // The journal rotation holds the journal writer and waits for the
    // keyspaces write lock (before the fix: for the GC loop's read guard).
    std::thread::sleep(Duration::from_millis(50));

    let began = Instant::now();
    other.insert(b"k", b"v")?;
    let took = began.elapsed();

    rotator.join().expect("rotator panicked")?;
    journal.join().expect("journal rotation panicked")?;
    stale_gc_hook::clear();
    drop(db);
    assert!(
        took < Duration::from_millis(100),
        "a commit waited {took:?} behind the version GC"
    );
    Ok(())
}
