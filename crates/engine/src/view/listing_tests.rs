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

/// K5 with a create behind the cursor: the kernel resumes from a cookie
/// inside the reply it got last (getdents' buffer took only part of it),
/// after a name was created before the entries it consumed. That page, and
/// a page resumed inside it in turn, list no name that was there throughout
/// twice and skip none, whether the entry the cookie names is still there
/// or was removed meanwhile. A page resumed by skipping the consumed
/// cookies from its anchor used to list the new name first, so the next
/// resume inside it, skipping only that page's cookies, listed again the
/// names the page before had already given.
#[test]
fn a_resume_inside_a_page_after_a_create_behind_it_repeats_nothing() {
    for remove_resume_entry in [false, true] {
        let fx = Fx::new();
        let stable: Vec<String> = (1..=9).map(|i| format!("b{i}")).collect();
        let dir = fx.dir("d", &stable);
        // [., .., b1, b2, b3, b4]: the kernel consumes up to b2.
        let p1 = fx.page(dir, 0, 6).unwrap();
        assert_eq!(p1[3].0, "b2");
        let mut consumed: Vec<String> = vec!["b1".into(), "b2".into()];
        // Created behind the cursor (and, in the second round, the entry
        // the cookie names removed).
        fx.meta.create(dir, "a0", 0o644, 0, 0).unwrap();
        if remove_resume_entry {
            fx.meta.unlink(dir, "b2").unwrap();
        }
        let p2 = fx.page(dir, p1[3].1, 3).unwrap();
        // The kernel consumes two entries of this reply.
        consumed.extend(p2[..2].iter().map(|(n, _)| n.clone()));
        let mut cookie = p2[1].1;
        loop {
            let page = fx.page(dir, cookie, 3).unwrap();
            let Some(last) = page.last().cloned() else {
                break;
            };
            consumed.extend(page.into_iter().map(|(n, _)| n));
            cookie = last.1;
        }
        // `a0` came after the listing began: it may or may not be listed.
        let listed: Vec<&String> = consumed.iter().filter(|n| *n != "a0").collect();
        assert_eq!(
            listed,
            stable.iter().collect::<Vec<_>>(),
            "removed b2: {remove_resume_entry}; listed {consumed:?}"
        );
        let stats = fx.listing.stats();
        assert_eq!((stats.cold_scans, stats.lost_cookies), (0, 0), "{stats:?}");
    }
}

/// The same at random: the kernel takes a random part of each reply and
/// resumes from the last entry it took, while names are created across
/// the whole order and removed (the one the next cookie names among them),
/// between every two requests. Every name is listed at most once, and each
/// that was there throughout exactly once, from the hints alone.
#[test]
fn random_partial_replies_under_churn_list_each_name_once() {
    let fx = Fx::new();
    let stable = names(600);
    let dir = fx.dir("d", &stable);
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let mut next = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    let mut listed: Vec<String> = Vec::new();
    let mut live_new: Vec<String> = Vec::new();
    let mut created = 0u64;
    let mut cookie = 0;
    loop {
        let page = fx.page(dir, cookie, 3 + next(40) as usize).unwrap();
        if page.is_empty() {
            break;
        }
        // The kernel takes a part of the reply, at least one entry.
        let took = 1 + next(page.len() as u64) as usize;
        for (n, _) in &page[..took] {
            if n != "." && n != ".." {
                listed.push(n.clone());
            }
        }
        let (at, c) = page[took - 1].clone();
        cookie = c;
        // Names come and go, behind and ahead of the cursor; sometimes
        // the one the cookie names goes (only once it was listed, so
        // that every name of `stable` is there until it is).
        for _ in 0..next(4) {
            let name = format!("{:x}-new-{created}", next(1 << 40));
            created += 1;
            fx.meta.create(dir, &name, 0o644, 0, 0).unwrap();
            live_new.push(name);
        }
        if at != "." && at != ".." && next(3) == 0 {
            fx.meta.unlink(dir, &at).unwrap();
            live_new.retain(|n| *n != at);
        }
        if !live_new.is_empty() && next(2) == 0 {
            let gone = live_new.swap_remove(next(live_new.len() as u64) as usize);
            fx.meta.unlink(dir, &gone).unwrap();
        }
    }
    let unique: BTreeSet<&String> = listed.iter().collect();
    assert_eq!(unique.len(), listed.len(), "a name listed twice");
    for n in &stable {
        assert!(
            unique.contains(n),
            "{n} was there until listed, and is missing"
        );
    }
    let stats = fx.listing.stats();
    assert_eq!((stats.cold_scans, stats.lost_cookies), (0, 0), "{stats:?}");
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

// ---------------------------------------------------------------------------
// Plan 40 R3b: an open directory's listing hints are pinned.
// ---------------------------------------------------------------------------

use constellation_vfs::Blocking;
use listing::{PinCaps, DIR_FH_BIT};

fn opendir_of(view: &View, caller: &Caller, ino: Ino) -> Fh {
    Blocking::run(|r| view.opendir(&OpCtx::new(OpKind::Opendir, caller), ino, r)).unwrap()
}

fn releasedir_of(view: &View, caller: &Caller, ino: Ino, fh: Fh) {
    Blocking::run(|r| view.releasedir(&OpCtx::new(OpKind::Releasedir, caller), ino, fh, r)).unwrap()
}

fn page_fh(
    view: &View,
    caller: &Caller,
    ino: Ino,
    fh: Fh,
    cookie: u64,
    limit: usize,
) -> VfsResult<Vec<(String, u64)>> {
    let (sink, wait) = CollectDir::pair(limit);
    view.readdir(
        &OpCtx::new(OpKind::Readdir, caller),
        ino,
        fh,
        cookie,
        false,
        sink,
    );
    wait.wait().map(|entries| {
        entries
            .into_iter()
            .map(|e| (String::from_utf8(e.name.into_bytes()).unwrap(), e.next))
            .collect()
    })
}

fn children(page: Vec<(String, u64)>) -> Vec<(String, u64)> {
    page.into_iter()
        .filter(|(n, _)| n != "." && n != "..")
        .collect()
}

/// A hint cache small enough that other listings evict a directory's
/// pages.
fn pressed() -> Fx {
    Fx::with(Listing::new(CookieKey::fixed(1, 2), 128 << 10))
}

/// What `fts` (`rm -rf`, `find -delete`) does to a directory: read a round
/// of entries, delete them all, the one the next cookie names included,
/// and continue on the same open stream, while a second view lists other
/// directories so that the hint cache evicts. Returns every name listed
/// (or the first error) and the counters.
///
/// Scaled from the plan (100k names in rounds of 10k, 50 directories of
/// 20k names, a 1 MiB cap): 20k names in rounds of ~2k, 50 directories of
/// 1.5k, a 128 KiB cap. The cap is what makes the pressure; pages are 680
/// names.
fn fts_walk(with_stream: bool) -> (Result<Vec<String>, Code>, ListingStats) {
    let fx = pressed();
    let all = names(20_000);
    let dir = fx.dir("d", &all);
    let others: Vec<Ino> = (0..50)
        .map(|i| fx.dir(&format!("p{i}"), &names(1500)))
        .collect();
    let (second, _keep) = fx.second_view();
    let fh = if with_stream {
        opendir_of(&fx.view, &fx.caller, dir)
    } else {
        Fh(0)
    };
    let mut listed: Vec<String> = Vec::new();
    let mut cookie = 0;
    let out = loop {
        let mut round: Vec<(String, u64)> = Vec::new();
        let mut last = cookie;
        let mut failed = None;
        while round.len() < 2000 {
            match page_fh(&fx.view, &fx.caller, dir, fh, last, 680) {
                Ok(page) => {
                    let Some(l) = page.last() else { break };
                    last = l.1;
                    round.extend(children(page));
                }
                Err(e) => {
                    failed = Some(e.code());
                    break;
                }
            }
        }
        if let Some(code) = failed {
            break Err(code);
        }
        if round.is_empty() {
            break Ok(listed);
        }
        for (n, _) in &round {
            fx.meta.unlink(dir, n).unwrap();
        }
        listed.extend(round.into_iter().map(|(n, _)| n));
        for p in &others {
            walk_of(&second, &fx.caller, *p, 680);
        }
        cookie = last;
    };
    (out, fx.listing.stats())
}

/// Against R3a alone (no stream): the cookie of a deleted name, evicted
/// by other listings, is lost (`ENOENT`). This is what the pins are for.
#[test]
fn without_a_stream_the_same_deletion_loses_its_cookie() {
    let (out, stats) = fts_walk(false);
    assert_eq!(out.unwrap_err(), Code::NotFound, "{stats:?}");
    assert!(stats.lost_cookies > 0, "{stats:?}");
}

#[test]
fn fts_deletion_under_hint_pressure_lists_every_name_once() {
    let (out, stats) = fts_walk(true);
    let mut got = out.unwrap_or_else(|c| panic!("{c:?}: {stats:?}"));
    assert_eq!(got.len(), 20_000, "{stats:?}");
    got.sort();
    got.dedup();
    assert_eq!(got.len(), 20_000, "every name once");
    assert_eq!(stats.lost_cookies, 0, "{stats:?}");
}

/// A small xorshift, for a shuffle that is the same every run.
fn shuffle<T>(v: &mut [T], mut x: u64) {
    for i in (1..v.len()).rev() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.swap(i, (x % (i as u64 + 1)) as usize);
    }
}

/// `telldir` every 1,000th entry, delete 90 % of the names including the
/// saved ones, `seekdir` to each in random order: each resumes exactly
/// after its name, with the stream open and the cache pressed.
#[test]
fn seekdir_after_mass_deletion_with_the_stream_open() {
    let fx = pressed();
    let all = sorted(&names(10_000));
    let dir = fx.dir("d", &all);
    let others: Vec<Ino> = (0..30)
        .map(|i| fx.dir(&format!("p{i}"), &names(1500)))
        .collect();
    let (second, _keep) = fx.second_view();
    let fh = opendir_of(&fx.view, &fx.caller, dir);
    let mut listed: Vec<(String, u64)> = Vec::new();
    let mut cookie = 0;
    loop {
        let page = page_fh(&fx.view, &fx.caller, dir, fh, cookie, 680).unwrap();
        let Some(l) = page.last() else { break };
        cookie = l.1;
        listed.extend(children(page));
    }
    assert_eq!(listed.len(), 10_000);
    let saved: Vec<(String, u64)> = listed.iter().step_by(1000).cloned().collect();
    let saved_names: BTreeSet<&str> = saved.iter().map(|(n, _)| n.as_str()).collect();
    let mut gone = BTreeSet::new();
    for (i, (n, _)) in listed.iter().enumerate() {
        if saved_names.contains(n.as_str()) || i % 10 != 3 {
            fx.meta.unlink(dir, n).unwrap();
            gone.insert(n.clone());
        }
    }
    assert!(gone.len() >= 9000);
    for p in &others {
        walk_of(&second, &fx.caller, *p, 680);
    }
    let survivors: Vec<&String> = all.iter().filter(|n| !gone.contains(*n)).collect();
    let before = fx.listing.stats();
    let mut order = saved.clone();
    shuffle(&mut order, 0x9e37_79b9);
    for (name, cookie) in &order {
        let got = children(page_fh(&fx.view, &fx.caller, dir, fh, *cookie, 100).unwrap());
        let expected: Vec<&String> = survivors
            .iter()
            .copied()
            .filter(|n| n.as_bytes() > name.as_bytes())
            .take(100)
            .collect();
        let got_names: Vec<&String> = got.iter().map(|(n, _)| n).collect();
        assert_eq!(got_names, expected, "seekdir to the removed {name}");
    }
    let after = fx.listing.stats();
    assert_eq!(after.cold_scans, before.cold_scans, "{after:?}");
    assert_eq!(after.lost_cookies, 0, "{after:?}");
    // Without the stream the same seekdirs lose their cookies.
    releasedir_of(&fx.view, &fx.caller, dir, fh);
    for p in &others {
        walk_of(&second, &fx.caller, *p, 680);
    }
    let lost = order
        .iter()
        .filter(|(_, c)| page_fh(&fx.view, &fx.caller, dir, Fh(0), *c, 100).is_err())
        .count();
    assert!(lost > 0, "the pins were what kept them");
}

/// A `rewinddir` moves the window: pages before it may be evicted, after
/// it not.
#[test]
fn a_rewind_moves_the_pin_window() {
    let fx = pressed();
    let all = sorted(&names(4000));
    let dir = fx.dir("d", &all);
    let others: Vec<Ino> = (0..30)
        .map(|i| fx.dir(&format!("p{i}"), &names(1500)))
        .collect();
    let (second, _keep) = fx.second_view();
    let fh = opendir_of(&fx.view, &fx.caller, dir);
    let mut cookie = 0;
    let mut first_page_end = None;
    let mut pages = 0;
    loop {
        let page = page_fh(&fx.view, &fx.caller, dir, fh, cookie, 100).unwrap();
        let Some(l) = page.last() else { break };
        cookie = l.1;
        pages += 1;
        if pages == 3 {
            first_page_end = Some(children(page)[99].clone());
        }
    }
    assert_eq!(pages, 41, "4000 names and the dots, 100 to a page");
    let (kept, ..) = fx.listing.window_for_test(dir);
    assert_eq!(kept, pages, "every page since the open is pinned");
    // Rewind: the first page served after it starts the window.
    let page = children(page_fh(&fx.view, &fx.caller, dir, fh, 0, 100).unwrap());
    let (kept, ..) = fx.listing.window_for_test(dir);
    assert_eq!(kept, 1, "the pages before the rewind left the window");
    let (late_name, late_cookie) = page[page.len() - 1].clone();
    // Both names are deleted and the cache is pressed.
    let (early_name, early_cookie) = first_page_end.unwrap();
    fx.meta.unlink(dir, &early_name).unwrap();
    fx.meta.unlink(dir, &late_name).unwrap();
    for p in &others {
        walk_of(&second, &fx.caller, *p, 680);
    }
    // After the rewind: pinned, exact.
    let got = children(page_fh(&fx.view, &fx.caller, dir, fh, late_cookie, 100).unwrap());
    let expected: Vec<&String> = all
        .iter()
        .filter(|n| n.as_bytes() > late_name.as_bytes())
        .take(100)
        .collect();
    assert_eq!(got.iter().map(|(n, _)| n).collect::<Vec<_>>(), expected);
    // Before it: the window no longer holds it, so a cache that evicted
    // it answers `ENOENT` (never a guess).
    match page_fh(&fx.view, &fx.caller, dir, fh, early_cookie, 100) {
        Err(e) => assert_eq!(e.code(), Code::NotFound),
        Ok(got) => {
            let expected: Vec<&String> = all
                .iter()
                .filter(|n| n.as_bytes() > early_name.as_bytes() && **n != late_name)
                .take(100)
                .collect();
            assert_eq!(
                children(got).iter().map(|(n, _)| n).collect::<Vec<_>>(),
                expected
            );
        }
    }
}

/// The view's side of a handover: export and import of the handle table
/// between two views, one per node (their listings are separate), the next
/// resume entry deleted in between.
#[test]
fn a_listing_resumes_exactly_across_a_handle_table_handover() {
    let old = Fx::new();
    let all = sorted(&names(3000));
    let dir = old.dir("d", &all);
    let fh = opendir_of(&old.view, &old.caller, dir);
    let mut cookie = 0;
    let mut mid = None;
    let mut last = String::new();
    for p in 0..3 {
        let page = children(page_fh(&old.view, &old.caller, dir, fh, cookie, 100).unwrap());
        if p == 1 {
            mid = Some(page[40].clone());
        }
        cookie = page.last().unwrap().1;
        last = page.last().unwrap().0.clone();
    }
    let (mid_name, mid_cookie) = mid.unwrap();
    // The names the resumes point at are deleted.
    old.meta.unlink(dir, &last).unwrap();
    old.meta.unlink(dir, &mid_name).unwrap();
    // A stream of another directory that was never listed pins nothing.
    let empty = old.dir("e", &[]);
    let idle = opendir_of(&old.view, &old.caller, empty);
    let snapshot = old.view.export_handles();
    let json = serde_json::to_string(&snapshot).unwrap();
    let snapshot: HandleTableSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(snapshot.listing.streams.len(), 2);
    // The next process: a view of the same replica with a listing of its
    // own and nothing cached.
    let new = Fx::on(old.meta.clone(), Listing::for_test());
    new.view.import_handles(&snapshot);
    let alive: Vec<&String> = all
        .iter()
        .filter(|n| **n != last && **n != mid_name)
        .collect();
    let after = |name: &str| -> Vec<&String> {
        alive
            .iter()
            .copied()
            .filter(|n| n.as_bytes() > name.as_bytes())
            .take(100)
            .collect()
    };
    let got = children(page_fh(&new.view, &new.caller, dir, fh, cookie, 100).unwrap());
    assert_eq!(got.iter().map(|(n, _)| n).collect::<Vec<_>>(), after(&last));
    let got = children(page_fh(&new.view, &new.caller, dir, fh, mid_cookie, 100).unwrap());
    assert_eq!(
        got.iter().map(|(n, _)| n).collect::<Vec<_>>(),
        after(&mid_name)
    );
    let stats = new.listing.stats();
    assert_eq!(stats.cold_scans, 0, "{stats:?}");
    assert_eq!(stats.lost_cookies, 0, "{stats:?}");
    assert_eq!(stats.open_streams, 2, "{stats:?}");
    assert!(stats.pinned_bytes > 0, "{stats:?}");
    // The new numbering does not reuse an id the kernel holds.
    let fresh = opendir_of(&new.view, &new.caller, dir);
    assert!(fresh != fh && fresh != idle, "{fresh:?}");
    // Released there, it is all let go of.
    for (ino, fh) in [(dir, fh), (empty, idle), (dir, fresh)] {
        releasedir_of(&new.view, &new.caller, ino, fh);
    }
    let stats = new.listing.stats();
    assert_eq!(
        (stats.open_streams, stats.pinned_bytes, stats.exempt_bytes),
        (0, 0, 0)
    );
    new.listing.accounted_for_test();
    assert!(
        stats.hint_bytes <= 4 * 1000,
        "only ordinary hints remain: {stats:?}"
    );
    // The old view going away lets go of its own streams.
    assert_eq!(old.listing.stats().open_streams, 2);
    let Fx { view, listing, .. } = old;
    drop(view);
    assert_eq!(listing.stats().open_streams, 0);
    assert_eq!(listing.stats().pinned_bytes, 0);
}

/// ... and within one process: the new view imports into the listing the
/// old one still pins in, then the old one goes away.
#[test]
fn an_in_process_handover_keeps_the_pins_of_the_stream() {
    let fx = pressed();
    let all = sorted(&names(2000));
    let dir = fx.dir("d", &all);
    let fh = opendir_of(&fx.view, &fx.caller, dir);
    let mut cookie = 0;
    let mut name = String::new();
    for _ in 0..3 {
        let page = page_fh(&fx.view, &fx.caller, dir, fh, cookie, 100).unwrap();
        cookie = page.last().unwrap().1;
        name = page.last().unwrap().0.clone();
    }
    let snapshot = fx.view.export_handles();
    let (next, _keep) = fx.second_view();
    next.import_handles(&snapshot);
    let pinned = fx.listing.stats().pinned_bytes;
    assert_eq!(fx.listing.stats().open_streams, 2, "both views' streams");
    // The old view goes away; the new one's stream holds the same pages.
    let Fx {
        view,
        listing,
        meta,
        caller,
        ..
    } = fx;
    drop(view);
    let stats = listing.stats();
    assert_eq!(stats.open_streams, 1, "{stats:?}");
    assert_eq!(stats.pinned_bytes, pinned, "{stats:?}");
    listing.accounted_for_test();
    meta.unlink(dir, &name).unwrap();
    let got = children(page_fh(&next, &caller, dir, fh, cookie, 5).unwrap());
    let at = all.iter().position(|n| *n == name).unwrap();
    assert_eq!(
        got.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        all[at + 1..at + 6]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(listing.stats().cold_scans, 0);
    releasedir_of(&next, &caller, dir, fh);
    assert_eq!(listing.stats().pinned_bytes, 0);
}

/// The pin bounds hold with many streams over many directories, each
/// stream's last page stays resolvable, and releasing frees everything.
#[test]
fn pin_caps_hold_and_each_streams_last_page_stays_pinned() {
    let caps = PinCaps {
        per_dir: 24 << 10,
        per_node: 1 << 20,
    };
    let listing = Listing::with_caps(CookieKey::fixed(1, 2), 256 << 10, caps);
    let (dirs, per_dir, pages_each) = (1000u64, 10u64, 30u64);
    let owner = 7;
    let key = |dir: u64, s: u64| (owner, DIR_FH_BIT | (dir * per_dir + s + 1));
    for d in 0..dirs {
        for s in 0..per_dir {
            listing.open_stream(5000 + d, key(d, s));
        }
    }
    assert_eq!(listing.stats().open_streams, dirs * per_dir);
    assert_eq!(listing.stats().pinned_dirs, dirs);
    let cookie =
        |d: u64, s: u64, p: u64, i: u64| 3 + (((d * per_dir + s) * pages_each + p) << 8) + i;
    let name = |d: u64, s: u64, p: u64| format!("n-{d}-{s}-{p}");
    for p in 0..pages_each {
        for d in 0..dirs {
            for s in 0..per_dir {
                let cookies: Vec<u64> = (0..100).map(|i| cookie(d, s, p, i)).collect();
                listing.record_via_for_test(
                    5000 + d,
                    Some(b"anchor"),
                    name(d, s, p).as_bytes(),
                    &cookies,
                    Some(key(d, s)),
                );
                let stats = listing.stats();
                assert!(
                    stats.pinned_bytes <= caps.per_node as u64,
                    "node cap: {stats:?}"
                );
            }
            let (_, window, _) = listing.window_for_test(5000 + d);
            assert!(window <= caps.per_dir, "dir cap: {window}");
        }
    }
    let stats = listing.stats();
    assert!(stats.pin_overflows > 0, "{stats:?}");
    assert!(
        stats.exempt_bytes > 0,
        "last pages outside the caps: {stats:?}"
    );
    listing.accounted_for_test();
    // Every stream continues exactly from the last page it was served.
    for d in 0..dirs {
        for s in 0..per_dir {
            let last = cookie(d, s, pages_each - 1, 99);
            let resume = listing.resolve(5000 + d, last, None);
            assert!(
                resume.is_after_for_test(name(d, s, pages_each - 1).as_bytes()),
                "dir {d} stream {s}"
            );
        }
    }
    for d in 0..dirs {
        for s in 0..per_dir {
            listing.close_stream(5000 + d, key(d, s));
        }
    }
    let stats = listing.stats();
    assert_eq!(
        (
            stats.open_streams,
            stats.pinned_dirs,
            stats.pinned_bytes,
            stats.exempt_bytes
        ),
        (0, 0, 0, 0),
        "{stats:?}"
    );
    let (_, _, held) = listing.accounted_for_test();
    assert_eq!(held, 0);
    assert!(stats.hint_bytes <= 256 << 10, "{stats:?}");
}

/// `RELEASEDIR` runs inline on the ring thread and never blocks (plan 40
/// §P5: the shard lock of the directory's hints, nothing else): while
/// another thread is in the middle of a cold scan of a big directory and
/// the hint cache is full, every release completes and frees its stream.
///
/// The scan is parked inside its reads (`Listing::cold_scan_hook`), so a
/// release that waited for it could not complete until the park ends: the
/// releases must all be done while the scan is still parked. (This measured
/// a 10 ms wall bound before, which CPU load alone broke: the plan's own
/// latency target, `releasedir` p99 < 1 ms, is a bench's, not this test's.)
#[test]
fn releasedir_never_waits_for_a_cold_scan_or_a_full_cache() {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;
    // The plan says a 1M-name directory, sized for a scan to outlast a
    // timed release; a scan parked in its reads needs only more than one
    // batch.
    let fx = Fx::with(Listing::new(CookieKey::fixed(1, 2), 64 << 10));
    let big = fx.dir("big", &names(4 * BATCH));
    let small: Vec<Ino> = (0..200)
        .map(|i| fx.dir(&format!("s{i}"), &names(50)))
        .collect();
    // Open streams with a page served on each: what a release frees.
    let streams: Vec<(Ino, Fh)> = small
        .iter()
        .map(|d| {
            let fh = opendir_of(&fx.view, &fx.caller, *d);
            let page = page_fh(&fx.view, &fx.caller, *d, fh, 0, 100).unwrap();
            assert!(page.len() > 2);
            (*d, fh)
        })
        .collect();
    // Fill the hint cache to its cap.
    for i in 0..30 {
        let d = fx.dir(&format!("f{i}"), &names(3000));
        walk_of(&fx.view, &fx.caller, d, 680);
    }
    let before = fx.listing.stats();
    assert_eq!(before.open_streams, small.len() as u64);
    // (parked, let go): the scan parks in its first batch until let go.
    // The park is bounded (a release that blocks fails the test below
    // instead of hanging it).
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let hook_gate = gate.clone();
    *fx.listing.cold_scan_hook.lock().unwrap() = Some(Arc::new(move || {
        let (state, cv) = &*hook_gate;
        let mut st = state.lock().unwrap();
        if st.1 {
            return;
        }
        st.0 = true;
        cv.notify_all();
        let (mut st, _) = cv
            .wait_timeout_while(st, Duration::from_secs(60), |st| !st.1)
            .unwrap();
        st.0 = false;
    }));
    std::thread::scope(|scope| {
        let scan = scope.spawn(|| {
            // A cookie no name has: one full cold scan, then `ENOENT`.
            let err = page_fh(&fx.view, &fx.caller, big, Fh(0), 12345, 10).unwrap_err();
            assert_eq!(err.code(), Code::NotFound);
        });
        {
            let (state, cv) = &*gate;
            let st = state.lock().unwrap();
            let (st, _) = cv
                .wait_timeout_while(st, Duration::from_secs(60), |st| !st.0)
                .unwrap();
            assert!(st.0, "the cold scan never began");
        }
        for (d, fh) in &streams {
            releasedir_of(&fx.view, &fx.caller, *d, *fh);
        }
        let parked = gate.0.lock().unwrap().0;
        let after = fx.listing.stats();
        {
            let (state, cv) = &*gate;
            state.lock().unwrap().1 = true;
            cv.notify_all();
        }
        scan.join().unwrap();
        assert!(
            parked,
            "a release waited for the cold scan: the releases ended after its park"
        );
        assert_eq!(after.open_streams, 0, "every release freed: {after:?}");
        assert_eq!(after.pinned_bytes, 0, "{after:?}");
        assert_eq!(
            (after.cold_scans, after.cold_scan_entries),
            (before.cold_scans + 1, before.cold_scan_entries),
            "the releases scanned nothing (only the parked scan began): {after:?}"
        );
    });
    *fx.listing.cold_scan_hook.lock().unwrap() = None;
}

/// D8: a stream id is never a file handle, so a directory `fsync` on it is
/// the barrier and owes and consumes nothing of a file's.
#[test]
fn a_stream_id_never_equals_a_file_handle() {
    use constellation_vfs::{Durability, OpenFlags, OpenOwner};
    let fx = Fx::new();
    let dir = fx.dir("d", &[]);
    let file = fx.meta.create(dir, "f", 0o644, 0, 0).unwrap().ino;
    let mut files = BTreeSet::new();
    let mut streams = BTreeSet::new();
    for _ in 0..50 {
        let opened = Blocking::run(|r| {
            fx.view.open(
                &OpCtx::new(OpKind::Open, &fx.caller),
                file,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        files.insert(opened.fh.0);
        streams.insert(opendir_of(&fx.view, &fx.caller, dir).0);
    }
    assert_eq!((files.len(), streams.len()), (50, 50));
    assert!(streams.iter().all(|s| s & DIR_FH_BIT != 0));
    assert!(files.iter().all(|f| f & DIR_FH_BIT == 0));
    assert!(files.is_disjoint(&streams));
    // The barrier on a stream id succeeds ...
    let stream = Fh(*streams.iter().next().unwrap());
    Blocking::run(|r| {
        fx.view.fsync(
            &OpCtx::new(OpKind::Fsync, &fx.caller),
            dir,
            stream,
            Durability::Configured,
            r,
        )
    })
    .unwrap();
    // ... and an error event owed to a file handle is not owed to a
    // directory stream, nor consumed by it.
    let file_fh = Fh(*files.iter().next().unwrap());
    assert!(!fx.view.handles.take_error(stream, file, u64::MAX));
    assert!(!fx.view.handles.take_error(stream, dir, u64::MAX));
    assert!(fx.view.handles.take_error(file_fh, file, u64::MAX));
}

/// Releasing an unknown, stale or non-stream handle is nothing.
#[test]
fn releasing_what_is_no_stream_is_nothing() {
    let fx = Fx::new();
    let dir = fx.dir("d", &names(10));
    let fh = opendir_of(&fx.view, &fx.caller, dir);
    releasedir_of(&fx.view, &fx.caller, dir, Fh(0));
    releasedir_of(&fx.view, &fx.caller, dir, Fh(DIR_FH_BIT | 999));
    releasedir_of(&fx.view, &fx.caller, dir, Fh(5));
    assert_eq!(fx.listing.stats().open_streams, 1);
    releasedir_of(&fx.view, &fx.caller, dir, fh);
    releasedir_of(&fx.view, &fx.caller, dir, fh);
    assert_eq!(fx.listing.stats().open_streams, 0);
    // A readdir through a released stream is a plain one.
    assert_eq!(
        children(page_fh(&fx.view, &fx.caller, dir, fh, 0, 100).unwrap()).len(),
        10
    );
    assert_eq!(fx.listing.stats().pinned_bytes, 0);
    fx.listing.accounted_for_test();
}

/// A stream continues from the page last served through it, even when a
/// newer page of another stream holds a different name with the same
/// cookie (a collision across pages: in-page cookies are unique, cookies
/// of a directory are not). Resolving by the newest page alone resumed
/// after the other name, past entries the stream had not read.
#[test]
fn a_stream_resumes_from_its_own_last_page_past_a_colliding_newer_one() {
    let listing = Listing::with_caps(CookieKey::fixed(1, 2), 256 << 10, PinCaps::default());
    let dir = 9000;
    let (s, t) = ((7, DIR_FH_BIT | 1), (7, DIR_FH_BIT | 2));
    listing.open_stream(dir, s);
    listing.open_stream(dir, t);
    // `s` was served `a`..`m`, the last one's cookie 12.
    listing.record_via_for_test(dir, None, b"m", &[10, 11, 12], Some(s));
    // Then `t`, further on: `x`, whose cookie is 12 too, mid-page.
    listing.record_via_for_test(dir, Some(b"p"), b"z", &[20, 12, 21], Some(t));
    assert!(
        listing.resolve(dir, 12, Some(s)).is_after_for_test(b"m"),
        "`s` continues after `m`, not after `t`'s `x`"
    );
    // Through no stream (or `t`), the newest page still answers.
    assert!(!listing.resolve(dir, 12, None).is_after_for_test(b"m"));
    assert!(!listing.resolve(dir, 12, Some(t)).is_after_for_test(b"m"));
    // A cookie that is not in the stream's last page: the other pages.
    assert!(listing.resolve(dir, 21, Some(s)).is_after_for_test(b"z"));
    listing.close_stream(dir, s);
    listing.close_stream(dir, t);
    listing.accounted_for_test();
}

/// A listing's pages are hints again once its last stream is released:
/// the window is freed, not kept.
#[test]
fn a_released_pin_frees_its_hints() {
    let fx = Fx::new();
    let dir = fx.dir("d", &names(3000));
    let a = opendir_of(&fx.view, &fx.caller, dir);
    let b = opendir_of(&fx.view, &fx.caller, dir);
    let mut cookie = 0;
    loop {
        let page = page_fh(&fx.view, &fx.caller, dir, a, cookie, 100).unwrap();
        let Some(l) = page.last() else { break };
        cookie = l.1;
    }
    let (n, bytes, _) = fx.listing.window_for_test(dir);
    assert_eq!(n, 31, "3000 names and the dots, 100 to a page");
    assert_eq!(fx.listing.stats().pinned_bytes, bytes as u64);
    // The older stream (b, opened before the listing) holds the window.
    releasedir_of(&fx.view, &fx.caller, dir, a);
    assert_eq!(fx.listing.window_for_test(dir).0, 31, "b still pins them");
    releasedir_of(&fx.view, &fx.caller, dir, b);
    let stats = fx.listing.stats();
    assert_eq!((stats.pinned_bytes, stats.pinned_dirs), (0, 0), "{stats:?}");
    let (ring, pinned, held) = fx.listing.accounted_for_test();
    assert_eq!((pinned, held), (0, 0));
    assert_eq!(ring, stats.hint_bytes);
    assert!(
        ring > 0 && ring <= 4 * 1000,
        "the last pages stay as hints: {ring}"
    );
}

/// The handoff carries a page's cookies as base64 text.
#[test]
fn a_pinned_pages_cookies_round_trip_through_the_handoffs_json() {
    for n in [0usize, 1, 2, 3, 4, 5, 7, 680] {
        let cookies: Vec<u8> = (0..n * 8).map(|i| (i * 37 + 11) as u8).collect();
        let page = listing::PageSnapshot {
            seq: 9,
            anchor: Some(b"a".to_vec()),
            last: b"z".to_vec(),
            cookies: cookies.clone(),
        };
        let json = serde_json::to_string(&page).unwrap();
        let back: listing::PageSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, page, "{n} cookies");
        assert!(
            n < 100 || json.len() < cookies.len() * 3 / 2,
            "compact: {}",
            json.len()
        );
    }
    let bad = r#"{"seq":1,"anchor":null,"last":[],"cookies":"a!"}"#;
    assert!(serde_json::from_str::<listing::PageSnapshot>(bad).is_err());
}
