//! `create` throughput of the meta store, and what a clean close and the
//! next open cost (the journal eviction fix: `Meta::close`, the 128 MiB
//! journal limit).
//!
//! `#[ignore]`d and release-mode only. Run with:
//!
//!   cargo test --release -p constellation-meta --test create_throughput \
//!       -- --ignored --nocapture --test-threads=1
//!
//! `CREATE_BENCH_N` sets the creates (default 300 000). The line it prints
//! (`RESULT ...`) is what the ABBA comparison reads. The store is closed by
//! dropping it, which is a clean close before and after the fix, so the
//! same file measures both.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MetaStore};
use std::time::Instant;

#[test]
#[ignore]
fn create_throughput_close_and_reopen() {
    let n: u64 = std::env::var("CREATE_BENCH_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300_000);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let meta = Meta::open(&db).unwrap();
    let t = Instant::now();
    for i in 0..n {
        meta.create(ROOT_INO, &format!("f{i:07}"), 0o644, 0, 0)
            .unwrap();
    }
    let create = t.elapsed();
    let t = Instant::now();
    drop(meta);
    let close = t.elapsed();
    let t = Instant::now();
    let meta = Meta::open(&db).unwrap();
    let reopen = t.elapsed();
    assert!(meta.lookup(ROOT_INO, "f0000000").unwrap().is_some());
    println!(
        "RESULT n={n} create_s={:.3} creates_per_s={:.0} close_s={:.3} reopen_s={:.3}",
        create.as_secs_f64(),
        n as f64 / create.as_secs_f64(),
        close.as_secs_f64(),
        reopen.as_secs_f64()
    );
}
