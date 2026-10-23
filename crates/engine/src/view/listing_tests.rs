//! Plan 40 R3a: directory pages are name-ordered range scans resumed from
//! keyed cookies (`listing`, `cookie`). In-process `readdir` calls against
//! a real [`View`] over an in-memory replica, populated through `Meta`
//! directly (as a replayed foreign change would be), with the replica's
//! `dentries_read` counter as the cost measure.

use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{CollectDir, Fh, OpCtx, OpKind, Vfs, VfsResult};
use cookie::{CookieBits, CookieKey, COLLIDING_COOKIE};
use listing::BATCH;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

struct Fx {
    view: View,
    meta: Arc<Meta>,
    listing: Arc<Listing>,
    caller: Caller,
    _dir: tempfile::TempDir,
}

impl Fx {
    fn new() -> Self {
        Self::with(Listing::for_test())
    }

    fn with(listing: Arc<Listing>) -> Self {
        Self::on(Arc::new(Meta::open_in_memory().unwrap()), listing)
    }

    fn on(meta: Arc<Meta>, listing: Arc<Listing>) -> Self {
        let (mut view, dir) = super::quota_tests::test_fs(meta.clone());
        view.listing = listing.clone();
        Fx {
            view,
            meta,
            listing,
            caller: Caller::new(1000, 1000, None),
            _dir: dir,
        }
    }

    /// Another view of the same node: the same replica, the same listing
    /// state, no handle shared with the first.
    fn second_view(&self) -> (View, tempfile::TempDir) {
        let (mut view, dir) = super::quota_tests::test_fs(self.meta.clone());
        view.listing = self.listing.clone();
        (view, dir)
    }

    /// A directory `name` under the root holding files `names`.
    fn dir(&self, name: &str, names: &[String]) -> Ino {
        let dir = self.meta.mkdir(ROOT_INO, name, 0o755, 0, 0).unwrap().ino;
        for n in names {
            self.meta.create(dir, n, 0o644, 0, 0).unwrap();
        }
        dir
    }

    fn page(&self, ino: Ino, cookie: u64, limit: usize) -> VfsResult<Vec<(String, u64)>> {
        page_of(&self.view, &self.caller, ino, cookie, limit, false)
    }

    /// The children (not `.`/`..`) and their cookies, page by page from
    /// the start until an empty page; each page resumed from the last
    /// cookie of the one before.
    fn walk(&self, ino: Ino, limit: usize) -> Vec<Vec<(String, u64)>> {
        walk_of(&self.view, &self.caller, ino, limit)
    }
}

fn page_of(
    view: &View,
    caller: &Caller,
    ino: Ino,
    cookie: u64,
    limit: usize,
    plus: bool,
) -> VfsResult<Vec<(String, u64)>> {
    let (sink, wait) = CollectDir::pair(limit);
    view.readdir(
        &OpCtx::new(OpKind::Readdir, caller),
        ino,
        Fh(0),
        cookie,
        plus,
        sink,
    );
    wait.wait().map(|entries| {
        entries
            .into_iter()
            .map(|e| (String::from_utf8(e.name.into_bytes()).unwrap(), e.next))
            .collect()
    })
}

fn walk_of(view: &View, caller: &Caller, ino: Ino, limit: usize) -> Vec<Vec<(String, u64)>> {
    let mut pages = Vec::new();
    let mut cookie = 0;
    loop {
        let page = page_of(view, caller, ino, cookie, limit, false).unwrap();
        let Some(last) = page.last() else {
            return pages;
        };
        cookie = last.1;
        pages.push(
            page.into_iter()
                .filter(|(n, _)| n != "." && n != "..")
                .collect(),
        );
    }
}

/// Names that look like nothing in particular and are all distinct.
fn names(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "{:x}-{i}",
                (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 20
            )
        })
        .collect()
}

fn sorted(names: &[String]) -> Vec<String> {
    let mut v = names.to_vec();
    v.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    v
}

fn flat(pages: &[Vec<(String, u64)>]) -> Vec<String> {
    pages.iter().flatten().map(|(n, _)| n.clone()).collect()
}

/// Plan 40 R3a, the cost: a full listing of 100k names reads each dentry a
/// constant number of times. The page loop used to read and sort the whole
/// directory per page (~n²/680 ≈ 1.5×10⁷ dentries here).
#[test]
fn a_full_listing_reads_each_dentry_o1_times() {
    let fx = Fx::new();
    let n = 100_000;
    let all = names(n);
    let dir = fx.dir("d", &all);
    let before = fx.meta.dentries_read();
    let pages = fx.walk(dir, 680);
    let read = fx.meta.dentries_read() - before;
    assert_eq!(flat(&pages), sorted(&all), "name order, every name once");
    let bound = 2 * n as u64 + pages.len() as u64 * BATCH as u64;
    assert!(
        read <= bound,
        "{read} dentries read for {n} names in {} pages (bound {bound})",
        pages.len()
    );
    let stats = fx.listing.stats();
    assert_eq!(stats.cold_scans, 0, "{stats:?}");
    assert_eq!(stats.lost_cookies, 0, "{stats:?}");
    println!(
        "{n} names, {} pages of 680: {read} dentries read ({:.2} per name), bound {bound}",
        pages.len(),
        read as f64 / n as f64
    );
}

/// K5: the kernel may resume from any cookie of the reply it got last.
/// Each resumes exactly after that entry, from the hint.
#[test]
fn resume_from_every_cookie_of_the_last_page() {
    let fx = Fx::new();
    let all = sorted(&names(2000));
    let dir = fx.dir("d", &all);
    let pages = fx.walk(dir, 50);
    let before = fx.listing.stats();
    let mut resumes = 0;
    // Every page the cache holds, every cookie of it, in order.
    for page in &pages[pages.len() - 4..] {
        for (i, (_, cookie)) in page.iter().enumerate() {
            let got = fx.page(dir, *cookie, 50).unwrap();
            let at = all.iter().position(|n| *n == page[i].0).unwrap();
            let expected: Vec<&String> = all[at + 1..].iter().take(50).collect();
            let got_names: Vec<&String> = got.iter().map(|(n, _)| n).collect();
            assert_eq!(got_names, expected, "after {}", page[i].0);
            resumes += 1;
        }
    }
    let after = fx.listing.stats();
    assert_eq!(after.cold_scans, before.cold_scans, "{after:?}");
    assert_eq!(after.hint_hits, before.hint_hits + resumes, "{after:?}");
}

#[test]
fn resume_three_pages_back_hits_the_cache() {
    let fx = Fx::new();
    let all = sorted(&names(1000));
    let dir = fx.dir("d", &all);
    let pages = fx.walk(dir, 50);
    // Four pages are remembered: the cookie of the first entry of the
    // fourth page from the end is three pages back.
    let back = &pages[pages.len() - 4];
    let (name, cookie) = &back[10];
    let before = fx.listing.stats();
    let got = fx.page(dir, *cookie, 50).unwrap();
    let at = all.iter().position(|n| n == name).unwrap();
    assert_eq!(flat(&[got]), all[at + 1..at + 51].to_vec());
    let after = fx.listing.stats();
    assert_eq!(after.hint_hits, before.hint_hits + 1);
    assert_eq!(after.cold_scans, before.cold_scans);
}

/// The cache is node-wide: a second view, with no handle in common, finds
/// the first one's pages. With the cache gone, one cold scan, then hits.
#[test]
fn a_cookie_resumes_through_a_fresh_view_of_the_node() {
    let fx = Fx::new();
    let all = sorted(&names(1500));
    let dir = fx.dir("d", &all);
    let first = fx.page(dir, 0, 100).unwrap();
    let (_, cookie) = first[60].clone();
    let (other, _keep) = fx.second_view();

    let before = fx.listing.stats();
    let hit = page_of(&other, &fx.caller, dir, cookie, 100, false).unwrap();
    let at = all.iter().position(|n| *n == first[60].0).unwrap();
    assert_eq!(flat(&[hit]), all[at + 1..at + 101].to_vec());
    let after = fx.listing.stats();
    assert_eq!(
        (after.hint_hits, after.cold_scans),
        (before.hint_hits + 1, 0)
    );

    fx.listing.clear();
    let cold = page_of(&other, &fx.caller, dir, cookie, 100, false).unwrap();
    assert_eq!(
        flat(std::slice::from_ref(&cold)),
        all[at + 1..at + 101].to_vec()
    );
    let after_cold = fx.listing.stats();
    assert_eq!(after_cold.cold_scans, 1, "{after_cold:?}");
    assert!(after_cold.cold_scan_entries >= (at + 1) as u64);
    // The cold page was recorded: the next one is a hit.
    let next = page_of(&other, &fx.caller, dir, cold.last().unwrap().1, 100, false).unwrap();
    assert_eq!(flat(&[next]), all[at + 101..at + 201].to_vec());
    let after_next = fx.listing.stats();
    assert_eq!(after_next.cold_scans, 1);
    assert_eq!(after_next.hint_hits, after_cold.hint_hits + 1);
}

/// A cookie nothing places (no hint, and no name hashes to it) is an
/// error, never a position to guess: a silent skip would lose entries.
#[test]
fn a_cold_cookie_whose_name_was_removed_is_not_found_never_a_skip() {
    let fx = Fx::new();
    let all = sorted(&names(300));
    let dir = fx.dir("d", &all);
    let first = fx.page(dir, 0, 52).unwrap();
    let (gone, cookie) = first[40].clone();

    // With the hint, the removal does not matter: exact.
    fx.meta.unlink(dir, &gone).unwrap();
    let hit = fx.page(dir, cookie, 20).unwrap();
    let at = all.iter().position(|n| *n == gone).unwrap();
    assert_eq!(flat(&[hit]), all[at + 1..at + 21].to_vec());
    assert_eq!(fx.listing.stats().lost_cookies, 0);

    // Without it, the name is nowhere: NotFound, not the next entry.
    fx.listing.clear();
    let lost = fx.page(dir, cookie, 20);
    assert_eq!(lost.unwrap_err().code(), Code::NotFound);
    let stats = fx.listing.stats();
    assert_eq!((stats.cold_scans, stats.lost_cookies), (1, 1), "{stats:?}");
    // The cold scan read the directory once, in batches.
    assert_eq!(stats.cold_scan_entries, 299);
}

/// Two names of one cookie (a hook: at 62 bits it is a 2^-62 event): a
/// cold resume lists the second again or once, never zero times, and a page
/// never holds both.
#[test]
fn two_names_with_one_cookie_are_never_skipped_by_a_cold_resume() {
    let key = CookieKey::colliding(&[b"m1", b"m2"]);
    let fx = Fx::with(Listing::new(key, 1 << 20));
    let all: Vec<String> = ["a", "b", "m1", "m2", "x", "y", "z"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let dir = fx.dir("d", &all);

    // Walk with the cache: each name once, and no page holds both.
    let pages = fx.walk(dir, 100);
    assert_eq!(flat(&pages), all);
    for page in &pages {
        let cookies: BTreeSet<u64> = page.iter().map(|(_, c)| *c).collect();
        assert_eq!(cookies.len(), page.len(), "a page holds one cookie once");
    }
    assert_eq!(pages.len(), 2, "the page ended before m2: {pages:?}");
    assert_eq!(pages[0].last().unwrap().0, "m1");
    assert_eq!(fx.listing.stats().cookie_collisions, 1);

    // Resume from the shared cookie cold: m2 is not skipped (m1 may not
    // be repeated, the first of the name order is the anchor).
    fx.listing.clear();
    let rest = fx.page(dir, COLLIDING_COOKIE, 100).unwrap();
    let names: Vec<String> = flat(&[rest]);
    assert!(names.contains(&"m2".to_string()), "{names:?}");
    assert_eq!(&names[names.len() - 3..], ["x", "y", "z"]);
}

/// 31-bit cookies collide often enough to see, and a full listing still
/// lists every name once.
#[test]
fn a_page_never_holds_two_equal_cookies() {
    let key = CookieKey::colliding(&[b"c1", b"c2", b"c3", b"c4"]);
    let fx = Fx::with(Listing::new(key, 1 << 20));
    let all: Vec<String> = ["b", "c1", "c2", "c3", "c4", "d"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let dir = fx.dir("d", &all);
    for limit in [2, 3, 4, 100] {
        fx.listing.clear();
        let pages = fx.walk(dir, limit);
        assert_eq!(flat(&pages), all, "limit {limit}");
        for page in &pages {
            let cookies: BTreeSet<u64> = page.iter().map(|(_, c)| *c).collect();
            assert_eq!(cookies.len(), page.len(), "limit {limit}: {page:?}");
        }
    }
}

#[test]
fn cookies_are_in_range_and_dots_keep_theirs() {
    let fx = Fx::new();
    let all = names(3000);
    let dir = fx.dir("d", &all);
    let first = fx.page(dir, 0, 3).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|(n, c)| (n.as_str(), *c))
            .collect::<Vec<_>>()[..2],
        [(".", 1), ("..", 2)]
    );
    for page in fx.walk(dir, 100) {
        for (_, c) in page {
            assert!((3..=(1u64 << 62) + 2).contains(&c), "{c}");
        }
    }
}

#[test]
fn thirty_one_bit_cookies_stay_below_2_31() {
    let key = CookieKey::fixed(3, 4).with_bits(CookieBits::Narrow);
    let fx = Fx::with(Listing::new(key, 1 << 20));
    let all = names(5000);
    let dir = fx.dir("d", &all);
    let pages = fx.walk(dir, 100);
    assert_eq!(flat(&pages).len(), 5000);
    for (_, c) in pages.iter().flatten() {
        assert!((3..(1u64 << 31) - 1).contains(c), "{c}");
    }
}

/// Entries created and removed between pages: what was there throughout is
/// listed exactly once, including when the entry the next cookie names is
/// the one removed (the page's own hint places it).
#[test]
fn a_listing_is_exact_across_inserts_and_removals_between_pages() {
    let fx = Fx::new();
    let stable = names(3000);
    let dir = fx.dir("d", &stable);
    let mut seen: Vec<String> = Vec::new();
    let mut cookie = 0;
    let mut round = 0usize;
    let mut removed: BTreeSet<String> = BTreeSet::new();
    loop {
        let page = fx.page(dir, cookie, 100).unwrap();
        let Some(last) = page.last().cloned() else {
            break;
        };
        seen.extend(
            page.iter()
                .filter(|(n, _)| n != "." && n != "..")
                .map(|(n, _)| n.clone()),
        );
        cookie = last.1;
        round += 1;
        // The entry the cookie names goes (when it is one), others behind
        // and ahead of the cursor come and go.
        if last.0 != "." && last.0 != ".." {
            fx.meta.unlink(dir, &last.0).unwrap();
            removed.insert(last.0.clone());
        }
        for i in 0..5 {
            fx.meta
                .create(dir, &format!("new-{round}-{i}"), 0o644, 0, 0)
                .unwrap();
        }
        // Remove the one stable name just after the cursor: it is simply
        // not listed (removed before it was reached).
        if let Some(ahead) = sorted(&stable)
            .into_iter()
            .find(|n| n.as_bytes() > last.0.as_bytes() && !removed.contains(n))
        {
            if round.is_multiple_of(3) {
                fx.meta.unlink(dir, &ahead).unwrap();
                removed.insert(ahead);
            }
        }
    }
    let unique: BTreeSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), seen.len(), "no name twice");
    for n in &stable {
        if !removed.contains(n) {
            assert!(
                unique.contains(n),
                "{n} was there throughout and is missing"
            );
        }
    }
    assert_eq!(fx.listing.stats().cold_scans, 0);
    assert_eq!(fx.listing.stats().lost_cookies, 0);
}

/// The same under real concurrency: a thread creates and removes names of
/// its own in the directory while another lists it.
#[test]
fn a_listing_is_exact_while_another_thread_changes_the_directory() {
    let fx = Fx::new();
    let stable = names(4000);
    let dir = fx.dir("d", &stable);
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        let churn = s.spawn(|| {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // Names across the whole order, so some are ahead of the
                // cursor and some behind.
                let name = format!(
                    "{:x}-churn-{i}",
                    i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 20
                );
                fx.meta.create(dir, &name, 0o644, 0, 0).unwrap();
                if i >= 3 {
                    let old = format!(
                        "{:x}-churn-{}",
                        (i - 3).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 20,
                        i - 3
                    );
                    fx.meta.unlink(dir, &old).unwrap();
                }
                i += 1;
            }
        });
        for _ in 0..3 {
            let pages = fx.walk(dir, 70);
            let listed = flat(&pages);
            let unique: BTreeSet<&String> = listed.iter().collect();
            assert_eq!(unique.len(), listed.len(), "no name twice");
            for n in &stable {
                assert!(
                    unique.contains(n),
                    "{n} was there throughout and is missing"
                );
            }
        }
        stop.store(true, Ordering::Relaxed);
        churn.join().unwrap();
    });
    assert_eq!(fx.listing.stats().lost_cookies, 0);
}

/// Scratch directories page the same way (`scratch_readdir_after`).
#[test]
fn a_scratch_directory_pages_like_any_other() {
    let fx = Fx::new();
    let dir = fx.meta.mkdir(ROOT_INO, "tmp", 0o755, 0, 0).unwrap().ino;
    fx.meta
        .set_xattr(
            dir,
            constellation_meta::SCRATCH_XATTR,
            b"1",
            constellation_meta::SetXattrMode::Set,
        )
        .unwrap();
    let all = names(900);
    for (i, n) in all.iter().enumerate() {
        fx.meta
            .scratch_create(dir, n, (1 << 40) + i as u64, 0o644, 0, 0)
            .unwrap();
    }
    let before = fx.meta.dentries_read();
    let pages = fx.walk(dir, 100);
    assert_eq!(flat(&pages), sorted(&all));
    assert!(fx.meta.dentries_read() - before <= 2 * 900 + pages.len() as u64 * BATCH as u64);
}

/// A removed directory answers `NotFound` (or `Stale` once the replica has
/// let go of it) to a first page and to a resume alike, and a
/// non-directory `NotDir`; the end of a listing is an empty page, not an
/// error.
#[test]
fn a_removed_or_non_directory_inode_fails_before_the_first_entry() {
    let fx = Fx::new();
    let dir = fx.dir("d", &names(10));
    let file = fx.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap().ino;
    assert_eq!(fx.page(file, 0, 10).unwrap_err().code(), Code::NotDir);
    let first = fx.page(dir, 0, 5).unwrap();
    let cookie = first.last().unwrap().1;
    let rest = fx.page(dir, cookie, 100).unwrap();
    assert_eq!(rest.len(), 7);
    // The end of the listing: an empty page.
    assert!(fx
        .page(dir, rest.last().unwrap().1, 100)
        .unwrap()
        .is_empty());
    for n in fx.meta.readdir(dir).unwrap() {
        fx.meta.unlink(dir, &n.name).unwrap();
    }
    fx.meta.rmdir(ROOT_INO, "d").unwrap();
    for from in [0, cookie, rest.last().unwrap().1] {
        let code = fx.page(dir, from, 10).unwrap_err().code();
        assert!(
            matches!(code, Code::NotFound | Code::Stale),
            "{from}: {code:?}"
        );
    }
    // ... also once no hint knows the cookie (a cold scan starts at the
    // directory's first page).
    fx.listing.clear();
    let code = fx.page(dir, cookie, 10).unwrap_err().code();
    assert!(matches!(code, Code::NotFound | Code::Stale), "{code:?}");
}

/// The cap holds: pages of many large directories evict each other
/// whole, and the accounted bytes are what the shards hold.
#[test]
fn hint_bytes_never_exceed_the_cap() {
    let cap = 1 << 20;
    let listing = Listing::new(CookieKey::fixed(1, 2), cap);
    for d in 0..50u64 {
        // Pages as the page loop records them: 680 cookies of 20k names.
        for p in 0..30usize {
            let cookies: Vec<u64> = (0..680)
                .map(|i| {
                    listing
                        .key()
                        .cookie(format!("{d}-{}", p * 680 + i).as_bytes())
                })
                .collect();
            listing.record_for_test(1000 + d, format!("{d}-a{p}").as_bytes(), &cookies);
            assert!(
                listing.stats().hint_bytes <= cap as u64,
                "{} > {cap}",
                listing.stats().hint_bytes
            );
        }
    }
    let stats = listing.stats();
    assert_eq!(stats.hint_bytes, listing.accounted_bytes_for_test());
    assert!(
        stats.hint_bytes > cap as u64 / 2,
        "the cache is used: {stats:?}"
    );
}

/// ... and through the real page loop.
#[test]
fn the_page_loop_keeps_the_cache_under_a_small_cap() {
    let cap = 256 << 10;
    let fx = Fx::with(Listing::new(CookieKey::fixed(1, 2), cap));
    for d in 0..6 {
        let dir = fx.dir(&format!("d{d}"), &names(5000));
        let pages = fx.walk(dir, 680);
        assert_eq!(flat(&pages).len(), 5000);
        assert!(fx.listing.stats().hint_bytes <= cap as u64);
    }
    // Evicted directories resume cold, exactly.
    let dir = fx.meta.lookup(ROOT_INO, "d0").unwrap().unwrap().ino;
    let before = fx.listing.stats().cold_scans;
    let all = sorted(&names(5000));
    let got = fx
        .page(dir, fx.listing.key().cookie(all[2000].as_bytes()), 10)
        .unwrap();
    assert_eq!(flat(&[got]), all[2001..2011].to_vec());
    assert_eq!(fx.listing.stats().cold_scans, before + 1);
}

/// A non-UTF-8 name round-trips through the page loop and resumes by its
/// exact key.
#[test]
fn a_name_that_is_not_utf8_resumes_exactly() {
    let fx = Fx::new();
    let dir = fx.meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let wires: Vec<Vec<u8>> = vec![
        b"a".to_vec(),
        b"b\xff".to_vec(),
        b"b\xfe".to_vec(),
        b"c".to_vec(),
    ];
    for w in &wires {
        let stored = constellation_vfs::name::stored_form(w);
        fx.meta.create(dir, &stored, 0o644, 0, 0).unwrap();
    }
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut cookie = 0;
    loop {
        let (sink, wait) = CollectDir::pair(3);
        fx.view.readdir(
            &OpCtx::new(OpKind::Readdir, &fx.caller),
            dir,
            Fh(0),
            cookie,
            false,
            sink,
        );
        let entries = wait.wait().unwrap();
        let Some(last) = entries.last() else { break };
        cookie = last.next;
        got.extend(
            entries
                .iter()
                .map(|e| e.name.as_bytes().to_vec())
                .filter(|n| n != b"." && n != b".."),
        );
    }
    got.sort();
    let mut expected = wires;
    expected.sort();
    assert_eq!(got, expected);
}

/// Q1: what the cookie hash costs, and what a cold resume costs at 1M
/// (recorded in PROGRESS; run with `--ignored --nocapture`, release).
#[test]
#[ignore]
fn cookie_hash_and_cold_scan_cost() {
    use std::time::Instant;
    let key = CookieKey::fixed(11, 13);
    let sample = names(200_000);
    let bytes: u64 = sample.iter().map(|n| n.len() as u64).sum();
    let blake_key = blake3::derive_key("bench", b"seed");
    for _ in 0..2 {
        let t = Instant::now();
        let mut x = 0u64;
        for n in &sample {
            x ^= key.cookie(n.as_bytes());
        }
        let sip = t.elapsed();
        let t = Instant::now();
        for n in &sample {
            x ^= u64::from_le_bytes(
                blake3::keyed_hash(&blake_key, n.as_bytes()).as_bytes()[..8]
                    .try_into()
                    .unwrap(),
            );
        }
        let blake = t.elapsed();
        println!(
            "RESULT cookie-hash siphash13 {:.1} ns/name blake3-keyed {:.1} ns/name (avg name {} bytes) {x}",
            sip.as_nanos() as f64 / sample.len() as f64,
            blake.as_nanos() as f64 / sample.len() as f64,
            bytes / sample.len() as u64
        );
    }

    let n: usize = std::env::var("COLD_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let fx = Fx::new();
    let t = Instant::now();
    let all = names(n);
    let dir = fx.dir("d", &all);
    println!("RESULT populate {n} {:.1} s", t.elapsed().as_secs_f64());
    let order = sorted(&all);
    for frac in [10usize, 2] {
        let name = &order[n - n / frac];
        let cookie = fx.listing.key().cookie(name.as_bytes());
        fx.listing.clear();
        let before = fx.listing.stats();
        let t = Instant::now();
        let page = fx.page(dir, cookie, 680).unwrap();
        let cold = t.elapsed();
        let stats = fx.listing.stats();
        let t = Instant::now();
        let warm = fx.page(dir, page.last().unwrap().1, 680).unwrap();
        let hit = t.elapsed();
        println!(
            "RESULT cold-resume at {}/{n}: {:.1} ms ({} entries scanned), next page (hint) {:.2} ms, {} + {} entries",
            n - n / frac,
            cold.as_secs_f64() * 1e3,
            stats.cold_scan_entries - before.cold_scan_entries,
            hit.as_secs_f64() * 1e3,
            page.len(),
            warm.len()
        );
    }
    // Page cost at size: sequential pages of 680 from the start.
    let t = Instant::now();
    let pages = fx.walk(dir, 680);
    let total = t.elapsed();
    println!(
        "RESULT full listing {n}: {:.2} s, {:.1} us/page over {} pages",
        total.as_secs_f64(),
        total.as_micros() as f64 / pages.len() as f64,
        pages.len()
    );
}

/// Where a page's time goes: the replica's range read alone, in batches of
/// different sizes, the same with the cookie hash, and the whole page loop
/// (`--ignored --nocapture`, release).
#[test]
#[ignore]
fn batch_cost() {
    use std::time::Instant;
    // `BATCH_COST_DB`: a database `view::listing_perf` populated (its 50000
    // directory), instead of an in-memory one.
    let (fx, n, dir) = match std::env::var("BATCH_COST_DB") {
        Ok(db) => {
            let meta = Arc::new(Meta::open(std::path::Path::new(&db)).unwrap());
            let dir = meta.lookup(ROOT_INO, "d50000").unwrap().unwrap().ino;
            (Fx::on(meta, Listing::for_test()), 50_000, dir)
        }
        Err(_) => {
            let fx = Fx::new();
            let dir = fx.dir("d", &names(50_000));
            (fx, 50_000, dir)
        }
    };
    let key = fx.listing.key().clone();
    let med = |f: &mut dyn FnMut() -> f64| {
        let mut v: Vec<f64> = (0..7).map(|_| f()).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[3]
    };
    for batch in [64usize, 256, 680, 2048, 1 << 20] {
        let mut read_only = || {
            let t = Instant::now();
            let mut after: Option<Vec<u8>> = None;
            let mut got = 0;
            loop {
                let b = fx.meta.readdir_after(dir, after.as_deref(), batch).unwrap();
                got += b.len();
                if b.len() < batch {
                    break;
                }
                after = b.into_iter().next_back().map(|e| e.name);
            }
            assert_eq!(got, n);
            t.elapsed().as_secs_f64() * 1e3
        };
        let mut with_hash = || {
            let t = Instant::now();
            let mut after: Option<Vec<u8>> = None;
            let mut x = 0u64;
            loop {
                let b = fx.meta.readdir_after(dir, after.as_deref(), batch).unwrap();
                for e in &b {
                    x ^= key.cookie(&e.name);
                }
                if b.len() < batch {
                    break;
                }
                after = b.into_iter().next_back().map(|e| e.name);
            }
            std::hint::black_box(x);
            t.elapsed().as_secs_f64() * 1e3
        };
        println!(
            "RESULT batch {batch}: read {:.2} ms, read+hash {:.2} ms",
            med(&mut read_only),
            med(&mut with_hash)
        );
    }
    let old = med(&mut || {
        let t = Instant::now();
        let v = fx.meta.readdir(dir).unwrap();
        assert_eq!(v.len(), n);
        t.elapsed().as_secs_f64() * 1e3
    });
    let page = med(&mut || {
        fx.listing.clear();
        let t = Instant::now();
        let _ = fx.page(dir, 0, usize::MAX).unwrap();
        t.elapsed().as_secs_f64() * 1e3
    });
    println!("RESULT Meta::readdir {old:.2} ms, one-call page loop {page:.2} ms");
}
