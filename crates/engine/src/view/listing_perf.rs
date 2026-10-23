//! Plan 40 P3, engine level: what directory listing costs through
//! `Vfs::readdir` on a `View` whose replica lives on whatever file system
//! `LP_DIR` is on. An `#[ignore]`d measurement (release mode, no FUSE),
//! compared A/B/B/A with the P3 code against the code before it: the file
//! uses only the `Vfs` surface that exists on both, so one copy builds
//! against either tree.
//!
//! Env: `LP_DIR` (the database path), `LP_MODE` (`populate`, which removes
//! `LP_DIR` first, or `run`), `LP_SIZES` (default `5000,50000,1000000`),
//! `LP_REPS` (default 5), `LP_SAMPLE_PAGES` (default 12: at the largest
//! size, the pages timed one by one where a full listing would take
//! minutes), `LP_FULL_MAX` (the largest directory listed to the end,
//! default 50000; `LP_FULL_MAX=1000000` on the code that can).
//!
//!   LP_DIR=/mnt/fs-xfs/x/db LP_MODE=populate cargo test --release \
//!       -p constellation-engine --lib -- view::listing_perf --ignored \
//!       --nocapture --test-threads=1
//!
//! Every result line is `RESULT <size> <workload> <value> <unit>`:
//! `pages_680` lists the directory in the 680-name pages a 128 KiB
//! `READDIR` holds, each resumed from the last cookie of the one before
//! (the kernel's pattern); `one_call` in a single call; `page_us` is the
//! median time of one such page (the first `LP_SAMPLE_PAGES` where the
//! listing is not run to the end), `first_page_us` the first, and
//! `cold_resume_us` one page resumed from a cookie at 50-95 % of the
//! directory by a view that never listed it (a cold scan on the code that
//! resolves cookies by name; one more page on the code that does not).

use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{CollectDir, Fh, OpCtx, OpKind, Vfs};
use std::time::Instant;

const PAGE: usize = 680;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A name of 12..=40 random bytes and the index (distinct).
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
    s.push_str(&format!(".{i}"));
    s
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn sizes() -> Vec<u64> {
    std::env::var("LP_SIZES")
        .unwrap_or_else(|_| "5000,50000,1000000".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect()
}

fn dir_name(n: u64) -> String {
    format!("d{n}")
}

/// One `readdir` page into a `CollectDir`: its entries' last cookie and
/// their number.
fn page(view: &View, caller: &Caller, ino: Ino, cookie: u64, limit: usize) -> (u64, usize) {
    let (sink, wait) = CollectDir::pair(limit);
    view.readdir(
        &OpCtx::new(OpKind::Readdir, caller),
        ino,
        Fh(0),
        cookie,
        false,
        sink,
    );
    let entries = wait.wait().unwrap();
    (entries.last().map_or(cookie, |e| e.next), entries.len())
}

/// The cookie of every entry of `ino`, in listing order (`.` and `..`
/// first), from one call.
fn all_cookies(view: &View, caller: &Caller, ino: Ino) -> Vec<u64> {
    let (sink, wait) = CollectDir::pair(usize::MAX);
    view.readdir(
        &OpCtx::new(OpKind::Readdir, caller),
        ino,
        Fh(0),
        0,
        false,
        sink,
    );
    wait.wait().unwrap().into_iter().map(|e| e.next).collect()
}

#[test]
#[ignore]
fn directory_listing() {
    let db = std::env::var("LP_DIR").expect("LP_DIR");
    let db = std::path::Path::new(&db);
    let reps = env_u64("LP_REPS", 5) as usize;
    if std::env::var("LP_MODE").as_deref() == Ok("populate") {
        let _ = std::fs::remove_dir_all(db);
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        let meta = Meta::open(db).unwrap();
        for n in sizes() {
            let t = Instant::now();
            let dir = meta.mkdir(ROOT_INO, &dir_name(n), 0o755, 0, 0).unwrap().ino;
            for i in 0..n {
                meta.create(dir, &name(i), 0o644, 0, 0).unwrap();
            }
            println!("RESULT {n} populate {:.3} s", t.elapsed().as_secs_f64());
        }
        return;
    }
    let meta = Arc::new(Meta::open(db).unwrap());
    let (view, _tmp) = super::quota_tests::test_fs(meta.clone());
    let caller = Caller::new(1000, 1000, None);
    let sample_pages = env_u64("LP_SAMPLE_PAGES", 12) as usize;
    let full_max = env_u64("LP_FULL_MAX", 50_000);
    for n in sizes() {
        let ino = meta.lookup(ROOT_INO, &dir_name(n)).unwrap().unwrap().ino;
        let full = n <= full_max;
        let (mut one, mut paged, mut per_page, mut first) = (vec![], vec![], vec![], vec![]);
        let mut cold = vec![];
        let cookies = all_cookies(&view, &caller, ino);
        assert_eq!(cookies.len() as u64, n + 2);
        for rep in 0..reps + 1 {
            // A page resumed by a view that has never listed the directory.
            let at = (n as f64 * (0.5 + 0.45 * rep as f64 / (reps + 1) as f64)) as usize;
            let (fresh, _keep) = super::quota_tests::test_fs(meta.clone());
            let t = Instant::now();
            let (_, got) = page(&fresh, &caller, ino, cookies[at + 1], PAGE);
            let d = t.elapsed().as_secs_f64() * 1e6;
            assert_eq!(got as u64, PAGE.min(n as usize - at) as u64);
            if rep > 0 {
                cold.push(d);
            }
            // The whole directory in one call.
            if full {
                let t = Instant::now();
                let (_, listed) = page(&view, &caller, ino, 0, usize::MAX);
                let d = t.elapsed().as_secs_f64() * 1e3;
                assert_eq!(listed as u64, n + 2);
                if rep > 0 {
                    one.push(d);
                }
            }
            // In 680-name pages, each from the last cookie of the one
            // before: to the end, or the first `sample_pages` of them.
            let t_all = Instant::now();
            let mut cookie = 0;
            let mut listed = 0usize;
            let mut times = Vec::new();
            for p in 0.. {
                if !full && p == sample_pages {
                    break;
                }
                let t = Instant::now();
                let (next, got) = page(&view, &caller, ino, cookie, PAGE);
                times.push(t.elapsed().as_secs_f64() * 1e6);
                if got == 0 {
                    break;
                }
                cookie = next;
                listed += got;
            }
            let all = t_all.elapsed().as_secs_f64() * 1e3;
            if full {
                assert_eq!(listed as u64, n + 2);
            }
            if rep > 0 {
                if full {
                    paged.push(all);
                }
                first.push(times[0]);
                per_page.push(median(&mut times.clone()));
            }
        }
        if full {
            println!("RESULT {n} one_call {:.3} ms", median(&mut one));
            println!("RESULT {n} pages_680 {:.3} ms", median(&mut paged));
        }
        println!("RESULT {n} first_page_us {:.1} us", median(&mut first));
        println!("RESULT {n} page_us {:.1} us", median(&mut per_page));
        println!("RESULT {n} cold_resume_us {:.1} us", median(&mut cold));
    }
}
