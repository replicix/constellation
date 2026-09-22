//! Plan 29 M3c / plan 28 §P7 measurement: is `Meta::recursive_size`'s DFS
//! over `0x02` dentries (no maintained per-directory counter yet) fast
//! enough to keep as-is, or does per-directory recursive size need
//! maintained counters instead?
//!
//! `#[ignore]`d and release-mode only — debug-build timings are not
//! representative of the decision this measures. Run with:
//!
//!   cargo test --release -p constellation-meta --test recursive_size_perf \
//!       -- --ignored --nocapture --test-threads=1
//!
//! Decision rule (plan 29 M3c): more than 100 ms for 100k entries warm
//! means the DFS is bad and maintained counters are needed; otherwise
//! keep the DFS and take only cheap wins.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::{Meta, MetaStore};
use std::time::{Duration, Instant};

fn time_it<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t = Instant::now();
    let r = f();
    (r, t.elapsed())
}

/// `n` files directly under `dir`, named so a `readdir` sorts them
/// predictably (irrelevant here, but matches other fixtures' style).
fn build_flat(meta: &Meta, dir: Ino, n: u64) {
    for i in 0..n {
        meta.create(dir, &format!("f{i}"), 0o644, 0, 0).unwrap();
    }
}

/// `fanout^3` leaf directories under a fresh `top` directory, each
/// holding `files_per_leaf` files — depth 4 counting the file level, a
/// realistic multi-tenant shape (many directories, not one giant flat
/// one) rather than a synthetic worst case. At `fanout=10,
/// files_per_leaf=1000` this is 1000 leaf directories and 1,000,000
/// files. Returns `(top, one_b_level_dir)`: `top` is the whole tree's
/// root for the full-tree measurement, `one_b_level_dir` is a
/// mid-level directory (10 leaf dirs, `10 * files_per_leaf` files)
/// for the subtree measurement.
fn build_tree(meta: &Meta, root: Ino, fanout: usize, files_per_leaf: u64) -> (Ino, Ino) {
    let top = meta.mkdir(root, "top", 0o755, 0, 0).unwrap().ino;
    let mut mid = None;
    for a in 0..fanout {
        let da = meta.mkdir(top, &format!("a{a}"), 0o755, 0, 0).unwrap().ino;
        for b in 0..fanout {
            let db = meta.mkdir(da, &format!("b{b}"), 0o755, 0, 0).unwrap().ino;
            if mid.is_none() {
                mid = Some(db);
            }
            for c in 0..fanout {
                let dc = meta.mkdir(db, &format!("c{c}"), 0o755, 0, 0).unwrap().ino;
                build_flat(meta, dc, files_per_leaf);
            }
        }
    }
    (top, mid.expect("fanout > 0"))
}

/// A 100,000-file flat directory: `recursive_size` on the root, warm
/// (repeated in-process) and cold (after closing and reopening the same
/// on-disk store, so fjall's own block cache starts empty).
#[test]
#[ignore]
fn flat_100k_directory() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("m.db");
    let meta = Meta::open(&db_path).unwrap();
    let d = meta.mkdir(ROOT_INO, "flat", 0o755, 0, 0).unwrap().ino;
    let (_, setup) = time_it(|| build_flat(&meta, d, 100_000));
    eprintln!("flat-100k: setup (100,000 creates) took {setup:?}");

    let (first, warm1) = time_it(|| meta.recursive_size(d).unwrap());
    let (second, warm2) = time_it(|| meta.recursive_size(d).unwrap());
    assert_eq!(first, second);
    assert_eq!(first.1, 100_000, "must count every file exactly once");
    eprintln!("flat-100k: warm recursive_size first={warm1:?} second={warm2:?} result={first:?}");

    drop(meta);
    let meta = Meta::open(&db_path).unwrap();
    let (cold_result, cold) = time_it(|| meta.recursive_size(d).unwrap());
    assert_eq!(cold_result, first, "reopened store must answer identically");
    eprintln!("flat-100k: cold (reopened) recursive_size took {cold:?}");

    assert!(
        warm2 < Duration::from_millis(100),
        "plan 29 M3c decision rule: {warm2:?} for 100k entries warm exceeds the 100ms bad-DFS threshold"
    );
}

/// A million-file, depth-4, 1000-leaf-directory tree: `recursive_size`
/// on the whole tree's root and on one mid-level subtree (10 leaf dirs,
/// 10,000 files), warm and cold.
#[test]
#[ignore]
fn million_file_tree() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("m.db");
    let meta = Meta::open(&db_path).unwrap();
    let (top, mid) = {
        let (pair, setup) = time_it(|| build_tree(&meta, ROOT_INO, 10, 1_000));
        eprintln!("million-file-tree: setup (1,000,000 creates across 1,111 dirs) took {setup:?}");
        pair
    };

    let (root_first, root_warm1) = time_it(|| meta.recursive_size(top).unwrap());
    let (root_second, root_warm2) = time_it(|| meta.recursive_size(top).unwrap());
    assert_eq!(root_first, root_second);
    assert_eq!(root_first.1, 1_000_000);
    eprintln!(
        "million-file-tree: root warm recursive_size first={root_warm1:?} second={root_warm2:?} result={root_first:?}"
    );

    let (mid_first, mid_warm1) = time_it(|| meta.recursive_size(mid).unwrap());
    let (mid_second, mid_warm2) = time_it(|| meta.recursive_size(mid).unwrap());
    assert_eq!(mid_first, mid_second);
    assert_eq!(
        mid_first.1, 10_000,
        "one b-level dir holds 10 leaf dirs of 1000 files each"
    );
    eprintln!(
        "million-file-tree: mid-level (10k files) warm recursive_size first={mid_warm1:?} second={mid_warm2:?} result={mid_first:?}"
    );

    drop(meta);
    let meta = Meta::open(&db_path).unwrap();
    let (root_cold_result, root_cold) = time_it(|| meta.recursive_size(top).unwrap());
    assert_eq!(root_cold_result, root_first);
    eprintln!("million-file-tree: root cold (reopened) recursive_size took {root_cold:?}");

    let (mid_cold_result, mid_cold) = time_it(|| meta.recursive_size(mid).unwrap());
    assert_eq!(mid_cold_result, mid_first);
    eprintln!("million-file-tree: mid-level cold (reopened) recursive_size took {mid_cold:?}");

    assert!(
        mid_warm2 < Duration::from_millis(100),
        "plan 29 M3c decision rule: {mid_warm2:?} for a 10k-entry subtree warm exceeds the \
         100ms-per-100k-entries bad-DFS threshold (10k entries budget is 10ms)"
    );
}
