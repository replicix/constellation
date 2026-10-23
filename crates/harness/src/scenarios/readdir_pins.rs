//! Plan 40 §7 / P9: listing exactness under deletion and pressure, through
//! a real mount. A name-ordered listing resumes by a keyed cookie, and an
//! open directory pins the pages its streams can still ask for (plan 40
//! §4.9), so `fts`-style deletion, `telldir`/`seekdir` and thousands of
//! open streams stay exact. Every scenario reads the node-wide `readdir`
//! block of `node.status` (`lost_cookies`, `cold_scans`, `pinned_*`,
//! `pin_overflows`).
//!
//! Sizes are the plan's where a harness run can afford them and are
//! environment knobs otherwise (the knob and the default are named at each
//! scenario); the scaled-down shape keeps what the plan's numbers were
//! for: the doomed directory needs more than one `fts` round (100,000
//! entries), the filler directories outgrow the hint cache's cap, the pin
//! caps are lowered until the open streams overflow them.

use super::readdirplus::{
    entry_name, env_usize, fill, num, raise_nofile, readdir_status, syncfs, DirStream, Fixture,
    Loop, RawDir,
};
use anyhow::{bail, ensure, Context, Result};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const HINT_BYTES: usize = 1 << 20;

/// A filler directory's name for entry `i`: another namespace than the
/// doomed directory's, so a mix-up shows.
fn filler_name(i: usize) -> String {
    format!("filler-{i:07}-{:08x}", (i as u32).wrapping_mul(40_503))
}

/// `rm -rf` of a huge directory (gnulib `fts` reads up to 100,000 entries
/// per round and deletes between rounds on the same stream) while the hint
/// cache is under pressure, and `find -ls` reads it from another process.
///
/// Sizes: `READDIR_HUGE_ENTRIES` (default 1,000,000, the plan's),
/// `READDIR_HUGE_FILLERS` (20) directories of `READDIR_HUGE_FILLER_ENTRIES`
/// (20,000; the plan says 200k) names listed in a loop; the hint cache is
/// `CONSTELLATION_READDIR_HINT_BYTES=1MiB`, which the fillers' pages
/// overflow many times.
pub fn huge_delete(_seed: u64) -> Result<()> {
    const NAME: &str = "readdir-huge-delete";
    let big = env_usize("READDIR_HUGE_ENTRIES", 1_000_000);
    let fillers = env_usize("READDIR_HUGE_FILLERS", 20);
    let filler_entries = env_usize("READDIR_HUGE_FILLER_ENTRIES", 20_000);
    let hint = HINT_BYTES.to_string();
    let mut fx = Fixture::new(
        NAME,
        &["a"],
        &[("CONSTELLATION_READDIR_HINT_BYTES", &hint)],
        Some("back"),
        &[],
    )?;
    let r = (|| -> Result<()> {
        let c = &fx.clients[0];
        let doomed = c.mnt.join("doomed");
        std::fs::create_dir(&doomed)?;
        let t = Instant::now();
        fill(&doomed, 0..big, 16, entry_name)?;
        let mut filler_dirs = Vec::new();
        for f in 0..fillers {
            let d = c.mnt.join(format!("filler-{f:02}"));
            std::fs::create_dir(&d)?;
            fill(&d, 0..filler_entries, 8, filler_name)?;
            filler_dirs.push(d);
        }
        eprintln!(
            "    {NAME}: created {big} + {fillers} x {filler_entries} entries in {:?}",
            t.elapsed()
        );
        let before = readdir_status(c)?;
        // The pressure: a second process lists the fillers the whole time,
        // so unpinned hints are evicted continuously.
        let lister = {
            let dirs = filler_dirs.clone();
            Loop::start(move || {
                for d in &dirs {
                    let mut s = DirStream::open(d)?;
                    let n = s.rest()?.len();
                    ensure!(n == filler_entries, "{} lists {n}", d.display());
                }
                Ok(())
            })
        };
        // What the status shows while the deleters run.
        let max_pinned = Arc::new(AtomicU64::new(0));
        let max_hint = Arc::new(AtomicU64::new(0));
        let sampler = {
            let (mp, mh) = (max_pinned.clone(), max_hint.clone());
            let state = c.state_dir().to_path_buf();
            Loop::start(move || {
                let st = crate::client::control_call_at(
                    &state,
                    "node.status",
                    serde_json::json!({}),
                    Duration::from_secs(10),
                )?;
                mp.fetch_max(num(&st["readdir"], "pinned_dirs"), Ordering::Relaxed);
                mh.fetch_max(num(&st["readdir"], "hint_bytes"), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(250));
                Ok(())
            })
        };
        // `find -ls` of the doomed directory from a third process.
        let finder = {
            let d = doomed.clone();
            std::thread::spawn(move || {
                Command::new("find")
                    .arg(&d)
                    .arg("-ls")
                    .stdout(Stdio::null())
                    .output()
            })
        };
        let t = Instant::now();
        let rm = Command::new("rm").arg("-rf").arg(&doomed).output()?;
        let rm_took = t.elapsed();
        ensure!(
            rm.status.success(),
            "rm -rf of the doomed directory failed ({}): {}",
            rm.status,
            String::from_utf8_lossy(&rm.stderr)
        );
        let found = finder
            .join()
            .map_err(|_| anyhow::anyhow!("the find thread panicked"))??;
        for line in String::from_utf8_lossy(&found.stderr).lines() {
            ensure!(
                line.contains("No such file or directory"),
                "find -ls: {line}"
            );
        }
        ensure!(!doomed.exists(), "the doomed directory is still there");
        let listed = lister.finish().context("the fillers' lister")?;
        sampler.finish()?;
        let after = readdir_status(c)?;
        eprintln!(
            "    {NAME}: rm -rf of {big} entries took {rm_took:?}; the lister completed {listed} rounds of \
             the fillers; pinned_dirs max {}, hint_bytes max {} (cap {HINT_BYTES}); lost_cookies {} -> {}, \
             cold_scans {} -> {}, hint hits {} -> {}",
            max_pinned.load(Ordering::Relaxed),
            max_hint.load(Ordering::Relaxed),
            num(&before, "lost_cookies"),
            num(&after, "lost_cookies"),
            num(&before, "cold_scans"),
            num(&after, "cold_scans"),
            num(&before, "hint_hits"),
            num(&after, "hint_hits"),
        );
        ensure!(
            num(&after, "lost_cookies") == num(&before, "lost_cookies"),
            "rm -rf lost cookies: {after}"
        );
        ensure!(
            max_pinned.load(Ordering::Relaxed) > 0,
            "no directory was pinned while rm -rf ran"
        );
        ensure!(
            num(&after, "cold_scans") == num(&before, "cold_scans"),
            "a listing paid a cold scan under the pressure: {after}"
        );
        super::eventually("no stream or pin is left", Duration::from_secs(30), || {
            let st = readdir_status(c)?;
            ensure!(
                num(&st, "pinned_dirs") == 0 && num(&st, "open_streams") == 0,
                "pinned_dirs {} open_streams {}",
                num(&st, "pinned_dirs"),
                num(&st, "open_streams")
            );
            Ok(())
        })
    })();
    fx.unmount_all();
    r
}

/// Reads up to `k` names from `d`.
fn names(d: &mut DirStream, k: usize) -> std::io::Result<Vec<String>> {
    Ok(d.take(k)?.into_iter().map(|(n, _)| n).collect())
}

/// The first `k` names of `expected` (the survivors in listing order)
/// after position `idx` of the original listing.
fn expect_after(survivors: &[usize], all: &[(String, i64)], idx: usize, k: usize) -> Vec<String> {
    let from = survivors.partition_point(|&i| i <= idx);
    survivors[from..]
        .iter()
        .take(k)
        .map(|&i| all[i].0.clone())
        .collect()
}

/// A directory opened by a program that `telldir`s every 1,000th entry,
/// deletes 90 % of the names (the saved ones included) with the stream
/// still open, and `seekdir`s to each saved position in random order: every
/// resume continues exactly after its name, no error. Second phase: the
/// same with a daemon restart (`kill -9` and remount, no handover) between
/// the deletion and the `seekdir`s, on a fresh `opendir`: the saved names
/// that survive resume (a cold scan), the deleted ones answer `ENOENT`, and
/// none is skipped silently.
///
/// `READDIR_SEEKDIR_ENTRIES` (default 200,000, the plan's).
pub fn seekdir_after_delete(seed: u64) -> Result<()> {
    const NAME: &str = "readdir-seekdir-after-delete";
    const EVERY: usize = 1000;
    const K: usize = 64;
    let entries = env_usize("READDIR_SEEKDIR_ENTRIES", 200_000);
    let mut fx = Fixture::new(NAME, &["a"], &[], Some("back"), &[])?;
    let r = (|| -> Result<()> {
        let mut rng = StdRng::seed_from_u64(seed);
        let dir = fx.clients[0].mnt.join("sk");
        std::fs::create_dir(&dir)?;
        let t = Instant::now();
        fill(&dir, 0..entries, 16, entry_name)?;
        eprintln!("    {NAME}: created {entries} entries in {:?}", t.elapsed());

        // Phase 1: the stream stays open across the deletion.
        let mut d = DirStream::open(&dir)?;
        let all = d.rest()?;
        ensure!(all.len() == entries, "listed {} of {entries}", all.len());
        let saved: Vec<usize> = (EVERY - 1..entries).step_by(EVERY).collect();
        let saved_set: BTreeSet<usize> = saved.iter().copied().collect();
        let mut survivors: Vec<usize> = (0..entries)
            .filter(|i| !saved_set.contains(i) && rng.random_range(0..10) == 0)
            .collect();
        survivors.sort_unstable();
        let keep: BTreeSet<usize> = survivors.iter().copied().collect();
        let doomed: Vec<String> = (0..entries)
            .filter(|i| !keep.contains(i))
            .map(|i| all[i].0.clone())
            .collect();
        delete_all(&dir, doomed)?;
        eprintln!(
            "    {NAME}: deleted {} names, {} survive; seeking to {} saved positions",
            entries - survivors.len(),
            survivors.len(),
            saved.len()
        );
        let mut order = saved.clone();
        order.shuffle(&mut rng);
        let lost0 = num(&readdir_status(&fx.clients[0])?, "lost_cookies");
        for idx in &order {
            d.seek(all[*idx].1);
            let got = names(&mut d, K)
                .with_context(|| format!("resuming after the deleted {}", all[*idx].0))?;
            let want = expect_after(&survivors, &all, *idx, K);
            ensure!(
                got == want,
                "seekdir to the deleted {} (entry {idx}): got {:?}, want {:?}",
                all[*idx].0,
                &got[..got.len().min(3)],
                &want[..want.len().min(3)]
            );
        }
        let st = readdir_status(&fx.clients[0])?;
        ensure!(
            num(&st, "lost_cookies") == lost0,
            "a pinned resume lost its cookie: {st}"
        );
        eprintln!(
            "    {NAME}: phase 1: {} resumes exact; pinned_bytes {}, pin_overflows {}",
            order.len(),
            num(&st, "pinned_bytes"),
            num(&st, "pin_overflows")
        );
        drop(d);

        // Phase 2: a restart between the deletion and the seeks. Positions
        // are saved from a listing of the survivors, half of the saved
        // names (and half of the rest) are deleted, the daemon is killed.
        let mut d = DirStream::open(&dir)?;
        let live = d.rest()?;
        drop(d);
        let saved2: Vec<usize> = (199..live.len()).step_by(200).collect();
        let mut gone: BTreeSet<usize> = BTreeSet::new();
        for (n, idx) in saved2.iter().enumerate() {
            if n % 2 == 0 {
                gone.insert(*idx);
            }
        }
        for i in 0..live.len() {
            if !saved2.contains(&i) && rng.random_range(0..2) == 0 {
                gone.insert(i);
            }
        }
        delete_all(&dir, gone.iter().map(|i| live[*i].0.clone()).collect())?;
        syncfs(&fx.clients[0].mnt)?;
        fx.clients[0].kill9()?;
        fx.clients[0].mount()?;
        let dir = fx.clients[0].mnt.join("sk");
        let mut d = DirStream::open(&dir)?;
        let now = d.rest()?;
        drop(d);
        let now_idx: std::collections::HashMap<&str, usize> = now
            .iter()
            .enumerate()
            .map(|(i, (n, _))| (n.as_str(), i))
            .collect();
        let cold0 = num(&readdir_status(&fx.clients[0])?, "cold_scans");
        let lost0 = num(&readdir_status(&fx.clients[0])?, "lost_cookies");
        let mut d = RawDir::open(&dir)?;
        let mut order = saved2.clone();
        order.shuffle(&mut rng);
        let (mut resumed, mut enoent) = (0u64, 0u64);
        for idx in &order {
            let (name, pos) = &live[*idx];
            d.seek(*pos)?;
            match (now_idx.get(name.as_str()), d.take(K)) {
                (Some(at), Ok(got)) => {
                    let got: Vec<String> = got.into_iter().map(|(n, _)| n).collect();
                    let want: Vec<String> = now[at + 1..]
                        .iter()
                        .take(K)
                        .map(|(n, _)| n.clone())
                        .collect();
                    ensure!(
                        got == want,
                        "a cold resume after the surviving {name} lists {:?}, want {:?}",
                        &got[..got.len().min(3)],
                        &want[..want.len().min(3)]
                    );
                    resumed += 1;
                }
                (None, Err(e)) if e.raw_os_error() == Some(libc::ENOENT) => enoent += 1,
                (None, Ok(got)) => bail!(
                    "a resume after the deleted {name} answered {} entries instead of ENOENT",
                    got.len()
                ),
                (_, Err(e)) => bail!("a resume after {name} failed: {e}"),
            }
        }
        let st = readdir_status(&fx.clients[0])?;
        eprintln!(
            "    {NAME}: phase 2 after kill -9 and remount: {resumed} resumes exact (cold scans {} -> {}), \
             {enoent} answered ENOENT (lost_cookies {} -> {})",
            cold0,
            num(&st, "cold_scans"),
            lost0,
            num(&st, "lost_cookies")
        );
        ensure!(
            resumed > 0 && enoent > 0,
            "the phase exercised only one case"
        );
        ensure!(
            num(&st, "cold_scans") >= cold0 + resumed,
            "the survivors' resumes were not cold scans: {st}"
        );
        ensure!(
            num(&st, "lost_cookies") >= lost0 + enoent,
            "the ENOENT answers were not counted as lost cookies: {st}"
        );
        Ok(())
    })();
    fx.unmount_all();
    r
}

/// Unlinks `names` in `dir` from eight threads.
fn delete_all(dir: &Path, names: Vec<String>) -> Result<()> {
    let names = Arc::new(names);
    let next = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let (names, next, dir) = (names.clone(), next.clone(), dir.to_path_buf());
        handles.push(std::thread::spawn(move || -> Result<()> {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                if i >= names.len() {
                    return Ok(());
                }
                std::fs::remove_file(dir.join(&names[i]))
                    .with_context(|| format!("unlinking {}", names[i]))?;
            }
        }));
    }
    for h in handles {
        h.join()
            .map_err(|_| anyhow::anyhow!("a deleter panicked"))??;
    }
    Ok(())
}

/// Thousands of directory streams open at once, each paused mid-listing,
/// with the pin caps lowered below what they need: the daemon's memory
/// stays within the caps plus the hint cap (and each stream's last page,
/// which no cap may give up), `pin_overflows` counts, and every stream's
/// continuation completes exactly.
///
/// `READDIR_MANY_STREAMS` (default 5,000, the plan's) streams over
/// `READDIR_MANY_DIRS` (100; the plan has one directory each, of
/// 1,000-20,000 entries) directories of `READDIR_MANY_MIN`..`READDIR_MANY_MAX`
/// (1,000..6,000) entries: the daemon's per-stream and per-directory state
/// is what is measured, and 5,000 distinct directories of that size would
/// be tens of millions of files.
pub fn many_open_dirs(seed: u64) -> Result<()> {
    const NAME: &str = "readdir-many-open-dirs";
    const PER_DIR: usize = 32 << 10;
    const PER_NODE: usize = 256 << 10;
    let streams = env_usize("READDIR_MANY_STREAMS", 5_000);
    let dirs = env_usize("READDIR_MANY_DIRS", 100);
    let (min, max) = (
        env_usize("READDIR_MANY_MIN", 1_000),
        env_usize("READDIR_MANY_MAX", 6_000),
    );
    raise_nofile(streams as u64 + 512)?;
    let (per_dir, per_node, hint) = (
        PER_DIR.to_string(),
        PER_NODE.to_string(),
        HINT_BYTES.to_string(),
    );
    let mut fx = Fixture::new(
        NAME,
        &["a"],
        &[
            ("CONSTELLATION_READDIR_PIN_BYTES_PER_DIR", &per_dir),
            ("CONSTELLATION_READDIR_PIN_BYTES", &per_node),
            ("CONSTELLATION_READDIR_HINT_BYTES", &hint),
        ],
        Some("back"),
        &[],
    )?;
    let r = (|| -> Result<()> {
        let c = &fx.clients[0];
        let mut rng = StdRng::seed_from_u64(seed);
        let t = Instant::now();
        let mut dir_paths = Vec::new();
        let mut sizes = Vec::new();
        for d in 0..dirs {
            let p = c.mnt.join(format!("md-{d:03}"));
            std::fs::create_dir(&p)?;
            let n = rng.random_range(min..=max);
            fill(&p, 0..n, 16, entry_name)?;
            dir_paths.push(p);
            sizes.push(n);
        }
        eprintln!(
            "    {NAME}: created {dirs} directories ({} entries) in {:?}",
            sizes.iter().sum::<usize>(),
            t.elapsed()
        );
        // The listing order of each directory, once.
        let mut reference: Vec<Vec<String>> = Vec::new();
        for (p, n) in dir_paths.iter().zip(&sizes) {
            let mut s = DirStream::open(p)?;
            let all: Vec<String> = s.rest()?.into_iter().map(|(n, _)| n).collect();
            ensure!(
                all.len() == *n,
                "{} lists {} of {n}",
                p.display(),
                all.len()
            );
            reference.push(all);
        }
        let rss0 = c.rss_bytes()?;
        // Open the streams, each read a different way into its directory.
        let t = Instant::now();
        let mut open: Vec<(usize, DirStream, usize)> = Vec::new();
        for s in 0..streams {
            let d = s % dirs;
            let mut stream = DirStream::open(&dir_paths[d])?;
            let k = rng.random_range(1..=sizes[d] / 3);
            let got = names(&mut stream, k)?;
            ensure!(
                got == reference[d][..k],
                "stream {s} of directory {d} listed a different head"
            );
            open.push((d, stream, k));
        }
        let rss1 = c.rss_bytes()?;
        let st = readdir_status(c)?;
        let exempt = num(&st, "exempt_bytes");
        let allowed = (PER_NODE + HINT_BYTES) as u64 + exempt + (48 << 20) + streams as u64 * 4096;
        eprintln!(
            "    {NAME}: {streams} streams open in {:?}: open_streams {}, pinned_dirs {}, pinned_bytes {} \
             (cap {PER_NODE}), exempt_bytes {exempt}, hint_bytes {}, pin_overflows {}; RSS {} -> {} \
             (+{} MiB; allowed {} MiB)",
            t.elapsed(),
            num(&st, "open_streams"),
            num(&st, "pinned_dirs"),
            num(&st, "pinned_bytes"),
            num(&st, "hint_bytes"),
            num(&st, "pin_overflows"),
            rss0 >> 20,
            rss1 >> 20,
            rss1.saturating_sub(rss0) >> 20,
            allowed >> 20
        );
        ensure!(
            num(&st, "open_streams") >= streams as u64,
            "only {} streams open",
            num(&st, "open_streams")
        );
        ensure!(
            num(&st, "pinned_bytes") <= PER_NODE as u64,
            "pinned_bytes {} above the node cap {PER_NODE}",
            num(&st, "pinned_bytes")
        );
        ensure!(num(&st, "pin_overflows") > 0, "the caps never overflowed");
        ensure!(
            rss1.saturating_sub(rss0) <= allowed,
            "RSS grew by {} MiB, above the caps plus the hint cap ({} MiB)",
            rss1.saturating_sub(rss0) >> 20,
            allowed >> 20
        );
        // Every stream continues sequentially, exactly.
        let lost0 = num(&st, "lost_cookies");
        let reference = Arc::new(reference);
        let chunk = open.len().div_ceil(8);
        let mut handles = Vec::new();
        while !open.is_empty() {
            let part: Vec<_> = open.drain(..chunk.min(open.len())).collect();
            let reference = reference.clone();
            handles.push(std::thread::spawn(move || -> Result<usize> {
                let mut done = 0;
                for (d, mut stream, k) in part {
                    let rest: Vec<String> = stream.rest()?.into_iter().map(|(n, _)| n).collect();
                    ensure!(
                        rest == reference[d][k..],
                        "a stream of directory {d} resumed after {k} names and listed {} (want {})",
                        rest.len(),
                        reference[d].len() - k
                    );
                    done += 1;
                }
                Ok(done)
            }));
        }
        let mut done = 0;
        for h in handles {
            done += h
                .join()
                .map_err(|_| anyhow::anyhow!("a continuation thread panicked"))??;
        }
        let st = readdir_status(c)?;
        eprintln!(
            "    {NAME}: {done} continuations exact; lost_cookies {lost0} -> {}, cold_scans {}",
            num(&st, "lost_cookies"),
            num(&st, "cold_scans")
        );
        ensure!(done == streams, "{done} of {streams} streams completed");
        ensure!(
            num(&st, "lost_cookies") == lost0,
            "a sequential continuation lost its cookie: {st}"
        );
        super::eventually("the pins are released", Duration::from_secs(30), || {
            let st = readdir_status(c)?;
            ensure!(
                num(&st, "open_streams") == 0 && num(&st, "pinned_bytes") == 0,
                "open_streams {} pinned_bytes {}",
                num(&st, "open_streams"),
                num(&st, "pinned_bytes")
            );
            Ok(())
        })
    })();
    fx.unmount_all();
    r
}
