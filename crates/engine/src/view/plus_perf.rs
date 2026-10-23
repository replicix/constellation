//! Plan 40 P7, engine level: what a plus listing costs through
//! `Vfs::readdir` on a `View` whose replica lives on whatever file system
//! `PP_DIR` is on. An `#[ignore]`d measurement (release mode, no FUSE),
//! compared A/B/B/A with the code of P7 against the code before it: the
//! file uses only the `Vfs` surface that exists on both (before P7 a
//! `plus` page carries no entries, so its numbers are the plain ones and
//! its entry count is 0).
//!
//! Env: `PP_DIR` (the database path), `PP_MODE` (`populate`, which removes
//! `PP_DIR` first, or `run`), `PP_SIZES` (default `2000,20000`),
//! `PP_REPS` (default 7).
//!
//!   PP_DIR=/mnt/fs-xfs/x/db PP_MODE=populate cargo test --release \
//!       -p constellation-engine --lib -- view::plus_perf --ignored \
//!       --nocapture --test-threads=1
//!
//! Every result line is `RESULT <size> <workload> <value> <unit>`, per
//! directory of `size` names: `single` names (each its inode's only name)
//! or `linked` ones (each inode has two):
//!
//! - `plain_ms`: the directory in 680-name plain pages;
//! - `plain186_ms`: in 186-name plain pages, the plus pages' size, so that
//!   `plus_ms - plain186_ms` is what the entries cost (the listing scans
//!   the directory once a page until the O(page) listing of plan 40 P3,
//!   which the page count multiplies);
//! - `lookup_ms`: plain pages, then a `lookup` of every name (what
//!   `ls -l` makes of a plain listing);
//! - `plus_ms`: the directory in 186-name plus pages (what a 32 KiB
//!   `READDIRPLUS` buffer holds);
//! - `plus_entries`: the entries those pages carried;
//! - `plus_inode_reads`: the `0x01` records they read for them
//!   (`Meta::inode_reads`).

use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{Blocking, CollectDir, Fh, Name, OpCtx, OpKind, Vfs};
use std::time::Instant;

const PLAIN_PAGE: usize = 680;
const PLUS_PAGE: usize = 186;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn name(i: u64) -> String {
    format!(
        "entry-{i:08}-{:x}",
        i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 40
    )
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn sizes() -> Vec<u64> {
    std::env::var("PP_SIZES")
        .unwrap_or_else(|_| "2000,20000".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect()
}

/// The whole directory in pages of `limit`, plain or plus: the names
/// listed and the entries carried.
fn list(view: &View, caller: &Caller, ino: Ino, limit: usize, plus: bool) -> (Vec<String>, usize) {
    let (mut names, mut entries, mut cookie) = (Vec::new(), 0, 0);
    loop {
        let (sink, wait) = CollectDir::pair(limit);
        view.readdir(
            &OpCtx::new(OpKind::Readdir, caller),
            ino,
            Fh(0),
            cookie,
            plus,
            sink,
        );
        let page = wait.wait().unwrap();
        let Some(last) = page.last() else { break };
        cookie = last.next;
        entries += page.iter().filter(|e| e.entry.is_some()).count();
        names.extend(
            page.into_iter()
                .map(|e| String::from_utf8(e.name.into_bytes()).unwrap()),
        );
    }
    (names, entries)
}

#[test]
#[ignore]
fn plus_listing() {
    let db = std::env::var("PP_DIR").expect("PP_DIR");
    let db = std::path::Path::new(&db);
    let reps = env_u64("PP_REPS", 7) as usize;
    let caller = Caller::new(1000, 1000, None);
    if std::env::var("PP_MODE").as_deref() == Ok("populate") {
        let _ = std::fs::remove_dir_all(db);
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        let meta = Arc::new(Meta::open(db).unwrap());
        let (view, _tmp) = super::quota_tests::test_fs(meta.clone());
        for n in sizes() {
            let t = Instant::now();
            let single = meta
                .mkdir(ROOT_INO, &format!("single{n}"), 0o755, 0, 0)
                .unwrap()
                .ino;
            let linked = meta
                .mkdir(ROOT_INO, &format!("linked{n}"), 0o755, 0, 0)
                .unwrap()
                .ino;
            for i in 0..n {
                meta.create(single, &name(i), 0o644, 0, 0).unwrap();
            }
            for i in 0..n / 2 {
                let f = meta.create(linked, &name(2 * i), 0o644, 0, 0).unwrap().ino;
                Blocking::run(|r| {
                    view.link(
                        &OpCtx::new(OpKind::Link, &caller),
                        f,
                        linked,
                        Name::new(&name(2 * i + 1)),
                        r,
                    )
                })
                .unwrap();
            }
            println!("RESULT {n} populate {:.3} s", t.elapsed().as_secs_f64());
        }
        return;
    }
    let meta = Arc::new(Meta::open(db).unwrap());
    let (view, _tmp) = super::quota_tests::test_fs(meta.clone());
    for n in sizes() {
        for kind in ["single", "linked"] {
            let ino = meta
                .lookup(ROOT_INO, &format!("{kind}{n}"))
                .unwrap()
                .unwrap()
                .ino;
            let (mut plain, mut plain186, mut lookup, mut plus) = (vec![], vec![], vec![], vec![]);
            let (mut entries, mut reads) = (0, 0);
            for rep in 0..reps + 1 {
                let t = Instant::now();
                let (names, _) = list(&view, &caller, ino, PLAIN_PAGE, false);
                let d = t.elapsed().as_secs_f64() * 1e3;
                assert_eq!(names.len() as u64, n + 2);
                if rep > 0 {
                    plain.push(d);
                }

                let t = Instant::now();
                let (names, _) = list(&view, &caller, ino, PLUS_PAGE, false);
                let d = t.elapsed().as_secs_f64() * 1e3;
                assert_eq!(names.len() as u64, n + 2);
                if rep > 0 {
                    plain186.push(d);
                }

                let t = Instant::now();
                let (names, _) = list(&view, &caller, ino, PLAIN_PAGE, false);
                for name in names.iter().filter(|n| *n != "." && *n != "..") {
                    Blocking::run(|r| {
                        view.lookup(
                            &OpCtx::new(OpKind::Lookup, &caller),
                            ino,
                            Name::new(name),
                            r,
                        )
                    })
                    .unwrap();
                }
                let d = t.elapsed().as_secs_f64() * 1e3;
                if rep > 0 {
                    lookup.push(d);
                }

                let before = meta.inode_reads();
                let t = Instant::now();
                let (names, got) = list(&view, &caller, ino, PLUS_PAGE, true);
                let d = t.elapsed().as_secs_f64() * 1e3;
                assert_eq!(names.len() as u64, n + 2);
                (entries, reads) = (got, meta.inode_reads() - before);
                if rep > 0 {
                    plus.push(d);
                }
            }
            println!("RESULT {n} {kind}_plain_ms {:.3} ms", median(&mut plain));
            println!(
                "RESULT {n} {kind}_plain186_ms {:.3} ms",
                median(&mut plain186)
            );
            println!("RESULT {n} {kind}_lookup_ms {:.3} ms", median(&mut lookup));
            println!("RESULT {n} {kind}_plus_ms {:.3} ms", median(&mut plus));
            println!("RESULT {n} {kind}_plus_entries {entries} entries");
            println!("RESULT {n} {kind}_plus_inode_reads {reads} reads");
        }
    }
}
