//! `Meta::close`: a clean exit leaves no journal to replay (F1 of the
//! journal eviction fix), and a crash still replays what it leaves.
//!
//! fjall deletes a sealed journal only once every keyspace has flushed past
//! it. `journal` and `journal_tx` grow ~100 B per operation and never reach
//! their memtable size, so without a flush at the close the journals pile up
//! to `MAX_JOURNALING_SIZE`, and the next open replays them item by item
//! (300 000 creates: 493 MB of journal, a 13 s open).
//!
//! The always-on tests run 20 000 creates (4 s in a debug build): too few to
//! rotate a journal, but the reopen replays the journal that is there all
//! the same (debug: 100 000 creates, a 24 s reopen without the flush, 17 ms
//! with it), which is what they check. `CLOSE_TEST_CREATES` changes the
//! count; the `#[ignore]`d release tests run the full 300 000, which leaves
//! several sealed journals.

use super::Meta;
use crate::MetaStore;
use constellation_fs_core::types::ROOT_INO;
use std::path::Path;
use std::time::{Duration, Instant};

fn creates(default: u64) -> u64 {
    std::env::var("CLOSE_TEST_CREATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `.jnl` files under `dir`: the journals the next open replays.
fn journal_files(dir: &Path) -> usize {
    let mut n = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            n += journal_files(&path);
        } else if path.extension().is_some_and(|e| e == "jnl") {
            n += 1;
        }
    }
    n
}

fn fill(meta: &Meta, n: u64) {
    for i in 0..n {
        meta.create(ROOT_INO, &format!("f{i:07}"), 0o644, 0, 0)
            .unwrap();
    }
}

fn assert_all_there(meta: &Meta, n: u64) {
    assert_eq!(meta.readdir(ROOT_INO).unwrap().len() as u64, n);
    for i in [0, n / 2, n - 1] {
        assert!(meta
            .lookup(ROOT_INO, &format!("f{i:07}"))
            .unwrap()
            .is_some());
    }
}

/// `n` creates, the close `how` says, a reopen: (journal files after the
/// close, the reopen's time).
fn close_and_reopen(n: u64, how: impl FnOnce(Meta)) -> (usize, Duration) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let meta = Meta::open(&db).unwrap();
    let t = Instant::now();
    fill(&meta, n);
    eprintln!("{n} creates: {:?}", t.elapsed());
    let t = Instant::now();
    how(meta);
    eprintln!("close: {:?}", t.elapsed());
    let files = journal_files(&db);
    let t = Instant::now();
    let meta = Meta::open(&db).unwrap();
    let reopen = t.elapsed();
    eprintln!("{files} journal file(s), reopen {reopen:?}");
    assert_all_there(&meta, n);
    (files, reopen)
}

fn run_clean(n: u64) {
    let (files, reopen) = close_and_reopen(n, |meta| {
        meta.close().unwrap();
        assert!(meta.is_closed());
    });
    assert!(files <= 2, "{files} journal files after a clean close");
    assert!(
        reopen < Duration::from_secs(1),
        "the reopen after a clean close took {reopen:?}"
    );
}

/// Fail-before: without the flush, the creates leave sealed journals and the
/// reopen replays them.
#[test]
fn a_clean_close_leaves_no_journal_to_replay() {
    run_clean(creates(20_000));
}

/// The full-size check of the report (300 000 creates: 493 MB of journal and
/// a 13 s reopen before the fix). `cargo test --release -p
/// constellation-meta close_tests -- --ignored --nocapture`.
#[test]
#[ignore]
fn a_clean_close_leaves_no_journal_to_replay_300k() {
    run_clean(300_000);
}

/// A plain drop is a clean close too (a test, a failed start, a panic's
/// unwind).
#[test]
fn a_drop_closes() {
    let n = creates(20_000);
    let (files, reopen) = close_and_reopen(n, drop);
    assert!(files <= 2, "{files} journal files after a drop");
    assert!(reopen < Duration::from_secs(1), "reopen {reopen:?}");
}

/// The crash path: nothing is flushed at the end, the open replays the
/// journals, and no create is lost. (What a crash leaves is what a commit
/// has put in the journal: `PersistMode::Buffer` reaches the OS, which is
/// all a process crash needs.)
#[test]
fn a_crash_replays_the_journals_and_loses_nothing() {
    let n = creates(20_000);
    let (files, _) = close_and_reopen(n, Meta::drop_without_close_for_tests);
    assert!(
        files >= 1,
        "no journal left: the creates are in the memtables only"
    );
}

/// The same with a sealed journal: 300 000 creates rotate the journal, the
/// stragglers (`journal`, `journal_tx`) keep it, and a crash replays it and
/// the active one, in order. (Release, 128 MiB limit: 2 files and a 3.7 s
/// open; before the fix, a clean drop after the same creates: a 6.9 s open.)
#[test]
#[ignore]
fn a_crash_replays_the_sealed_journals_and_loses_nothing_300k() {
    let (files, reopen) = close_and_reopen(300_000, Meta::drop_without_close_for_tests);
    assert!(
        files >= 2,
        "{files} journal file(s): the sealed journal was evicted"
    );
    eprintln!("crash reopen of {files} journals: {reopen:?}");
}

/// `close` can be called again, after more writes, and by a store in use.
#[test]
fn close_is_repeatable_and_the_store_stays_usable() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let meta = Meta::open(&db).unwrap();
    fill(&meta, 1_000);
    meta.close().unwrap();
    meta.create(ROOT_INO, "after", 0o644, 0, 0).unwrap();
    meta.close().unwrap();
    meta.create(ROOT_INO, "later", 0o644, 0, 0).unwrap();
    drop(meta);
    let meta = Meta::open(&db).unwrap();
    assert_eq!(meta.readdir(ROOT_INO).unwrap().len(), 1_002);
}

/// The journal limit is the one set (fjall's default is 512 MiB).
#[test]
fn the_journal_limit_is_128_mib() {
    assert_eq!(super::MAX_JOURNALING_SIZE, 128 * 1024 * 1024);
}

/// The `.jnl` file names under `dir`, sorted.
fn journal_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".jnl"))
        .collect();
    names.sort();
    names
}

/// An open that refuses the store (here: another on-disk format) leaves it
/// as it found it: the half-built `Meta`'s drop neither flushes nor rotates
/// the journal. Fail-before: the drop ran `close`, whose rotation replaced
/// the journal file.
#[test]
fn a_refused_open_leaves_the_journals_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let meta = Meta::open(&db).unwrap();
    fill(&meta, 100);
    meta.kv_set(super::KV_FORMAT, "999").unwrap();
    meta.drop_without_close_for_tests();
    let before = journal_names(&db);
    assert!(!before.is_empty());
    let refused = Meta::open(&db).err().expect("another format is refused");
    assert!(refused.to_string().contains("format 999"), "{refused}");
    assert_eq!(
        journal_names(&db),
        before,
        "the refused open rotated the journal"
    );
}

/// Writes that race the close lose nothing: the journal a write lands in is
/// never evicted before that write's keyspace has flushed past it. The
/// writer runs through the whole close, and the store is then dropped the
/// way a crash leaves it (nothing flushed after the close), so the reopen
/// has only the close's tables and the journals it kept.
#[test]
fn writes_racing_a_close_survive_a_crash_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let meta = Meta::open(&db).unwrap();
    fill(&meta, 2_000);
    let stop = std::sync::atomic::AtomicBool::new(false);
    let written = std::thread::scope(|s| {
        let writer = s.spawn(|| {
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) || i < 200 {
                meta.create(ROOT_INO, &format!("r{i:07}"), 0o644, 0, 0)
                    .unwrap();
                i += 1;
            }
            i
        });
        for _ in 0..3 {
            meta.close().unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap()
    });
    meta.drop_without_close_for_tests();
    let meta = Meta::open(&db).unwrap();
    assert_eq!(
        meta.readdir(ROOT_INO).unwrap().len() as u64,
        2_000 + written
    );
    for i in [0, written / 2, written - 1] {
        assert!(meta
            .lookup(ROOT_INO, &format!("r{i:07}"))
            .unwrap()
            .is_some());
    }
}

/// Every folder under `dir` read-only (`ro`) or writable again.
fn set_tree_read_only(dir: &Path, ro: bool) {
    use std::os::unix::fs::PermissionsExt;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if !ro {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            set_tree_read_only(&path, ro);
            if ro {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o555)).unwrap();
            }
        }
    }
}

/// A flush that fails poisons the store and ends its worker, so nothing
/// ever flushes what it left sealed: `close` gives up at once instead of
/// holding the exit (and `Drop`) for `CLOSE_FLUSH_TIMEOUT`, and a reopen
/// replays the journals it kept. Fail-before: `close` waited the whole
/// 120 s. (The tables cannot be written: their folders are read-only,
/// which does not stop root, so the test checks that first.)
#[test]
fn close_gives_up_at_once_when_a_flush_fails() {
    let dir = tempfile::tempdir().unwrap();
    let probe = dir.path().join("probe");
    std::fs::create_dir(&probe).unwrap();
    set_tree_read_only(dir.path(), true);
    let writable = std::fs::write(probe.join("x"), b"x").is_ok();
    set_tree_read_only(dir.path(), false);
    if writable {
        eprintln!("skipped: a read-only folder is writable here (root)");
        return;
    }
    let db = dir.path().join("m.db");
    let meta = Meta::open(&db).unwrap();
    fill(&meta, 1_000);
    let keyspaces = db.join("keyspaces");
    set_tree_read_only(&keyspaces, true);
    let t = Instant::now();
    let closed = meta.close();
    let took = t.elapsed();
    set_tree_read_only(&keyspaces, false);
    assert!(closed.is_err(), "the failed flush is reported");
    assert!(
        took < Duration::from_secs(30),
        "close waited {took:?} for a flush that failed"
    );
    drop(meta);
    let meta = Meta::open(&db).unwrap();
    assert_all_there(&meta, 1_000);
}
