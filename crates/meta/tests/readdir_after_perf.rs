//! Plan 40 P1, meta level: what a directory page costs, `readdir` against
//! `readdir_after`, on whatever file system `RA_DIR` is on. An `#[ignore]`d
//! measurement (release mode, no FUSE), compared A/B/B/A with the P1 code
//! against the code before it (the plan's perf gates, spec §4).
//!
//! The workloads that name `readdir_after` sit between `P1-ONLY` markers (and
//! on the line that calls them); the "before" build is this file with those
//! removed (`sed '/^\/\/ P1-ONLY begin/,/^\/\/ P1-ONLY end/d; /\/\/ P1-ONLY$/d'`), so both
//! builds run the same `full` and `skip_paged` code against one database.
//!
//! Env: `RA_DIR` (the database path), `RA_MODE` (`populate`, which removes
//! `RA_DIR` first, or `run`), `RA_N` (names per directory, default 50 000),
//! `RA_REPS` (default 7).
//!
//!   RA_DIR=/mnt/fs-xfs/x/db RA_MODE=populate cargo test --release \
//!       -p constellation-meta --test readdir_after_perf \
//!       -- --ignored --nocapture --test-threads=1
//!
//! Directory `c` holds `RA_N` single-name files (the dentry holds the
//! attribute copy); `r` and `r2` hold `RA_N` files of two names each (both
//! dentries are `Remote`, plan 45). Every result line is
//! `RESULT <dir> <workload> <value> <unit>`.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MetaStore};
use std::path::Path;
use std::time::Instant;

/// The page the engine asks for: 680 entries of ~24-byte names fill a 128 KiB
/// `READDIR` reply.
const PAGE: usize = 680;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn name(i: u64) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789._-";
    let mut x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xd1b5_4a32_d192_ed03;
    let mut next = || {
        x ^= x >> 30;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 27;
        x
    };
    let len = 12 + (next() % 29) as usize;
    let mut s: String = (0..len)
        .map(|_| ALPHABET[(next() % ALPHABET.len() as u64) as usize] as char)
        .collect();
    // The index keeps names distinct.
    s.push_str(&format!(".{i}"));
    s
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[test]
#[ignore]
fn directory_pages() {
    let db = std::env::var("RA_DIR").expect("RA_DIR");
    let db = Path::new(&db);
    let n = env_u64("RA_N", 50_000);
    let reps = env_u64("RA_REPS", 7) as usize;
    if std::env::var("RA_MODE").as_deref() == Ok("populate") {
        let _ = std::fs::remove_dir_all(db);
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        let meta = Meta::open(db).unwrap();
        let t = Instant::now();
        let c = meta.mkdir(ROOT_INO, "c", 0o755, 0, 0).unwrap().ino;
        let r = meta.mkdir(ROOT_INO, "r", 0o755, 0, 0).unwrap().ino;
        let r2 = meta.mkdir(ROOT_INO, "r2", 0o755, 0, 0).unwrap().ino;
        for i in 0..n {
            meta.create(c, &name(i), 0o644, 0, 0).unwrap();
            let ino = meta.create(r, &name(i), 0o644, 0, 0).unwrap().ino;
            meta.link(ino, r2, &name(i)).unwrap();
        }
        println!("RESULT populate - {:.3} s", t.elapsed().as_secs_f64());
        return;
    }
    let meta = Meta::open(db).unwrap();
    for dir in ["c", "r"] {
        let ino = meta.lookup(ROOT_INO, dir).unwrap().unwrap().ino;
        let all = meta.readdir(ino).unwrap();
        assert_eq!(all.len() as u64, n, "{dir}: populated with RA_N names");
        let (mut full, mut skip) = (Vec::new(), Vec::new());
        for rep in 0..reps + 1 {
            // `readdir`, the whole directory.
            let t = Instant::now();
            let v = meta.readdir(ino).unwrap();
            let d = t.elapsed().as_secs_f64() * 1e3;
            assert_eq!(v.len() as u64, n);
            // The page loop as the engine runs it before P1: every page
            // reads the whole directory (`readdir` keeps no order, so it
            // is sorted too) and skips to its place.
            let t = Instant::now();
            let mut listed = 0usize;
            loop {
                let v = meta.readdir(ino).unwrap();
                let mut names: Vec<&str> = v.iter().map(|e| e.name.as_str()).collect();
                names.sort_unstable();
                let from = listed.min(names.len());
                let page = &names[from..(from + PAGE).min(names.len())];
                if page.is_empty() {
                    break;
                }
                listed += page.len();
            }
            let s = t.elapsed().as_secs_f64() * 1e3;
            assert_eq!(listed as u64, n);
            if rep > 0 {
                full.push(d);
                skip.push(s);
            }
        }
        println!("RESULT {dir} full {:.3} ms", median(&mut full));
        println!("RESULT {dir} skip_paged {:.3} ms", median(&mut skip));
        let samples: Vec<Vec<u8>> = all
            .iter()
            .step_by((all.len() / 200).max(1))
            .map(|e| e.name.clone().into_bytes())
            .collect();
        p1_pages(&meta, dir, ino, &samples, reps, n); // P1-ONLY
    }
}

// P1-ONLY begin
/// A whole listing by `readdir_after` pages, and one page resumed from
/// each of ~200 names (a cold resume: the cookie is not in any cache).
fn p1_pages(
    meta: &Meta,
    dir: &str,
    ino: constellation_fs_core::Ino,
    samples: &[Vec<u8>],
    reps: usize,
    n: u64,
) {
    let (mut paged, mut resume) = (Vec::new(), Vec::new());
    for rep in 0..reps + 1 {
        let t = Instant::now();
        let mut listed = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = meta.readdir_after(ino, after.as_deref(), PAGE).unwrap();
            let Some(last) = page.last() else { break };
            after = Some(last.name.clone());
            listed += page.len();
        }
        let p = t.elapsed().as_secs_f64() * 1e3;
        assert_eq!(listed as u64, n);
        let t = Instant::now();
        for s in samples {
            let page = meta.readdir_after(ino, Some(s), PAGE).unwrap();
            assert!(page.len() <= PAGE);
        }
        let r = t.elapsed().as_secs_f64() * 1e6 / samples.len() as f64;
        if rep > 0 {
            paged.push(p);
            resume.push(r);
        }
    }
    println!("RESULT {dir} paged {:.3} ms", median(&mut paged));
    println!("RESULT {dir} resume_page {:.3} us", median(&mut resume));
}
// P1-ONLY end
