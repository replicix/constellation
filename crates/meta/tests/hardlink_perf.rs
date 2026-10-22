//! Plan 45 H0, meta level: what an inode's n names cost. An `#[ignore]`d
//! measurement (release mode, no FUSE), run before and after the plan's
//! change and compared A/B/B/A (the plan's perf gates, §7); the same
//! scenario is the plan's `link_cost_is_flat_in_the_link_count` in
//! `hardlink.rs` as a pass/fail test on key counts.
//!
//! Env: `HL_DIR` (a scratch path, removed first), `HL_N` (names of the
//! one hard-linked file, default 2,000), `HL_SINGLES` (single-name files
//! and repetitions, default 2,000).
//!
//!   HL_DIR=/path/db cargo test --release -p constellation-meta \
//!       --test hardlink_perf -- --ignored --nocapture --test-threads=1
//!
//! Every result line is `RESULT name value unit` so a script can pair the
//! runs.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::{Meta, MetaStore};
use std::path::Path;
use std::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Operations in the window a size's p50 is taken over (the ones ending at
/// the checkpoint): wide enough that a loaded host's spikes do not move it.
const WINDOW: usize = 100;

fn p50(v: &mut [Duration]) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn result(name: &str, d: Duration) {
    println!("RESULT {name} {:.2} us", d.as_secs_f64() * 1e6);
}

#[test]
#[ignore]
fn hard_link_scaling() {
    let db = std::env::var("HL_DIR").expect("HL_DIR");
    let db = Path::new(&db);
    let n = env_u64("HL_N", 2000);
    let _ = std::fs::remove_dir_all(db);
    let meta = Meta::open(db).unwrap();
    let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;

    // n = 1: a fresh single-name file each time, one link (1 -> 2) and
    // one unlink (2 -> 1), the transitions included.
    let singles: Vec<Ino> = (0..200)
        .map(|i| {
            let ino = meta.create(d, &format!("s{i}"), 0o644, 0, 0).unwrap().ino;
            meta.set_manifest(ino, b"m", 10).unwrap();
            ino
        })
        .collect();
    let (mut links, mut unlinks) = (Vec::new(), Vec::new());
    for (i, &ino) in singles.iter().enumerate() {
        let t = Instant::now();
        meta.link(ino, d, &format!("sl{i}")).unwrap();
        links.push(t.elapsed());
        let t = Instant::now();
        meta.unlink(d, &format!("sl{i}")).unwrap();
        unlinks.push(t.elapsed());
    }
    result("link_p50_n1", p50(&mut links));
    result("unlink_p50_n1", p50(&mut unlinks));
    let mut v = Vec::new();
    for (i, &ino) in singles.iter().enumerate() {
        let t = Instant::now();
        meta.setattr(
            ino,
            Some(0o600 + (i as u32 % 7)),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        v.push(t.elapsed());
    }
    result("chmod_p50_n1", p50(&mut v));
    let mut v = Vec::new();
    for (i, &ino) in singles.iter().enumerate() {
        let t = Instant::now();
        meta.set_manifest(ino, &[i as u8; 16], 4096 + i as u64)
            .unwrap();
        v.push(t.elapsed());
    }
    result("write_p50_n1", p50(&mut v));

    // One file, n names. The window is the operations ending at the
    // checkpoint.
    let f = meta.create(d, "f", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest(f, b"manifest-bytes", 4096).unwrap();
    let checkpoints = [WINDOW as u64, 250, 500, 1000, n];
    let mut window: Vec<Duration> = Vec::new();
    let start = Instant::now();
    for i in 1..=n {
        let t = Instant::now();
        meta.link(f, d, &format!("l{i}")).unwrap();
        window.push(t.elapsed());
        if window.len() > WINDOW {
            window.remove(0);
        }
        if checkpoints.contains(&i) {
            result(&format!("link_p50_at_{i}"), p50(&mut window.clone()));
        }
    }
    println!(
        "RESULT link_burst_total {:.3} s ({n} links)",
        start.elapsed().as_secs_f64()
    );

    let mut v = Vec::new();
    for k in 0..101u32 {
        let t = Instant::now();
        meta.setattr(f, Some(0o600 + k), None, None, None, None, None)
            .unwrap();
        v.push(t.elapsed());
    }
    result(&format!("chmod_p50_at_{n}"), p50(&mut v));
    let mut v = Vec::new();
    for k in 0..101u64 {
        let t = Instant::now();
        meta.set_manifest(f, &[k as u8; 16], 4096 + k).unwrap();
        v.push(t.elapsed());
    }
    result(&format!("write_p50_at_{n}"), p50(&mut v));

    // A warm listing right after the burst: plain readdir, then a lookup
    // per entry (what plus entries do).
    let mut best = (Duration::MAX, Duration::MAX);
    let mut entries = 0;
    for _ in 0..3 {
        let t = Instant::now();
        let list = meta.readdir(d).unwrap();
        let readdir = t.elapsed();
        let t = Instant::now();
        for e in &list {
            meta.lookup(d, &e.name).unwrap().unwrap();
        }
        let lookups = t.elapsed();
        entries = list.len();
        best = (best.0.min(readdir), best.1.min(lookups));
    }
    println!(
        "RESULT listing_warm {:.3} ms readdir, {:.3} us per lookup ({entries} entries)",
        best.0.as_secs_f64() * 1e3,
        best.1.as_secs_f64() * 1e6 / entries as f64
    );
    let t = Instant::now();
    let rs = meta.recursive_size(d).unwrap();
    println!(
        "RESULT recursive_size {:.3} ms {rs:?}",
        t.elapsed().as_secs_f64() * 1e3
    );

    // unlink, newest first, the same window.
    let mut window: Vec<Duration> = Vec::new();
    let start = Instant::now();
    for i in (1..=n).rev() {
        let t = Instant::now();
        meta.unlink(d, &format!("l{i}")).unwrap();
        window.push(t.elapsed());
        if window.len() > WINDOW {
            window.remove(0);
        }
        if checkpoints.contains(&i) {
            result(&format!("unlink_p50_at_{i}"), p50(&mut window.clone()));
        }
    }
    println!(
        "RESULT unlink_burst_total {:.3} s ({n} unlinks)",
        start.elapsed().as_secs_f64()
    );
    drop(meta);
}

/// The single-name path (the regression gate): create, setattr,
/// set_manifest, unlink of `HL_SINGLES` files in one directory, median of
/// five repetitions per operation.
#[test]
#[ignore]
fn single_name_path() {
    let db = std::env::var("HL_DIR").expect("HL_DIR");
    let db = Path::new(&db);
    let m = env_u64("HL_SINGLES", 2000);
    let _ = std::fs::remove_dir_all(db);
    let meta = Meta::open(db).unwrap();
    let s = meta.mkdir(ROOT_INO, "s", 0o755, 0, 0).unwrap().ino;
    let (mut create, mut setattr, mut manifest, mut unlink) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for rep in 0..5 {
        let mut inos: Vec<Ino> = Vec::new();
        let t = Instant::now();
        for i in 0..m {
            inos.push(
                meta.create(s, &format!("f{rep}-{i}"), 0o644, 0, 0)
                    .unwrap()
                    .ino,
            );
        }
        create.push(t.elapsed() / m as u32);
        let t = Instant::now();
        for &ino in &inos {
            meta.setattr(ino, Some(0o600), None, None, None, None, None)
                .unwrap();
        }
        setattr.push(t.elapsed() / m as u32);
        let t = Instant::now();
        for &ino in &inos {
            meta.set_manifest(ino, b"m", 10).unwrap();
        }
        manifest.push(t.elapsed() / m as u32);
        let t = Instant::now();
        for i in 0..m {
            meta.unlink(s, &format!("f{rep}-{i}")).unwrap();
        }
        unlink.push(t.elapsed() / m as u32);
    }
    result("single_create", p50(&mut create));
    result("single_setattr", p50(&mut setattr));
    result("single_set_manifest", p50(&mut manifest));
    result("single_unlink", p50(&mut unlink));
    drop(meta);
}
