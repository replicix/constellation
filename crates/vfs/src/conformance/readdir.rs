//! `readdir`: `.` and `..`, cookies that resume where they left off, the
//! `plus` flag, and a listing that neither repeats nor loses an entry that
//! was there throughout while the directory changes under it.

use super::{fail, joined, must, refused, refused_any, Client, Env, TestResult};
use crate::types::{FileKind, Ino};
use crate::DirEntry;
use constellation_types::Code;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn lists_dot_dotdot_and_children(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    let a = must("put a", c.put(d, "a", b"1"));
    let sub = must("mkdir sub", c.mkdir(d, "sub"));
    let s = must("symlink", c.symlink(d, "s", "a"));
    let all = must("readdir", c.readdir_all(d, 100));
    let by_name: BTreeMap<String, (u64, FileKind)> = all
        .iter()
        .map(|e| {
            (
                String::from_utf8_lossy(e.name.as_bytes()).into_owned(),
                (e.ino, e.kind),
            )
        })
        .collect();
    assert_eq!(all.len(), by_name.len(), "no name twice: {all:?}");
    assert_eq!(by_name["."], (d, FileKind::Dir));
    assert_eq!(by_name[".."].1, FileKind::Dir);
    assert_eq!(by_name["a"], (a.attr.ino, FileKind::File));
    assert_eq!(by_name["sub"], (sub.attr.ino, FileKind::Dir));
    assert_eq!(by_name["s"], (s.attr.ino, FileKind::Symlink));
    assert_eq!(by_name.len(), 5);
    // The root's `..` is the root.
    let top = must("readdir root", c.readdir_all(root, 100));
    let dotdot = top.iter().find(|e| e.name.as_bytes() == b"..").expect("..");
    assert_eq!(dotdot.ino, root, "`..` of the view's root is the root");
    // An empty directory has just the two.
    let e = must("mkdir e", c.mkdir(root, "e")).attr.ino;
    let names: Vec<_> = must("readdir e", c.readdir_all(e, 100))
        .into_iter()
        .map(|e| String::from_utf8_lossy(e.name.as_bytes()).into_owned())
        .collect();
    assert_eq!(names, [".", ".."]);
    Ok(())
}

pub(super) fn cookies_resume_where_they_left_off(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    let names: Vec<String> = (0..37).map(|i| format!("file-{i:02}")).collect();
    for n in &names {
        must("put", c.put(d, n, b""));
    }
    let whole = must("readdir whole", c.readdir_all(d, 1000));
    assert_eq!(whole.len(), names.len() + 2);
    for page in [1usize, 2, 3, 7, 36, 38] {
        let paged = must("readdir paged", c.readdir_all(d, page));
        let got: Vec<_> = paged.iter().map(|e| (e.ino, e.name.clone())).collect();
        let want: Vec<_> = whole.iter().map(|e| (e.ino, e.name.clone())).collect();
        assert_eq!(
            got, want,
            "paging by {page} lists the same entries in the same order"
        );
    }
    // Each entry's cookie resumes right after it, from anywhere.
    for (i, e) in whole.iter().enumerate() {
        let rest = must("resume", c.readdir_page(d, e.next, 1000, false));
        assert_eq!(rest.len(), whole.len() - i - 1, "resuming after entry {i}");
        if let Some(first) = rest.first() {
            assert_eq!(first.name, whole[i + 1].name);
        }
    }
    // Cookie 0 restarts; a listing can be read twice.
    assert_eq!(must("again", c.readdir_all(d, 5)).len(), whole.len());
    Ok(())
}

pub(super) fn plus_lists_the_same_entries(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    for n in ["x", "y", "z"] {
        must("put", c.put(d, n, n.as_bytes()));
    }
    let plain = must("readdir", c.readdir_page(d, 0, 100, false));
    let plus = must("readdirplus", c.readdir_page(d, 0, 100, true));
    let key = |v: &[crate::DirEntry]| -> Vec<(u64, Vec<u8>, u64)> {
        v.iter()
            .map(|e| (e.ino, e.name.as_bytes().to_vec(), e.next))
            .collect()
    };
    assert_eq!(key(&plain), key(&plus));
    Ok(())
}

pub(super) fn removal_between_pages_never_repeats_or_loses(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    let names: Vec<String> = (0..40).map(|i| format!("n{i:02}")).collect();
    for n in &names {
        must("put", c.put(d, n, b""));
    }
    let mut seen: Vec<String> = Vec::new();
    let mut cookie = 0;
    let mut removed: HashSet<String> = HashSet::new();
    let mut cookies: HashSet<u64> = HashSet::new();
    let mut step = 0;
    loop {
        let page = must("readdir", c.readdir_page(d, cookie, 6, false));
        let Some(last) = page.last() else { break };
        assert!(
            cookies.insert(last.next),
            "readdir cookie {} came back: the listing would never end",
            last.next
        );
        cookie = last.next;
        for e in &page {
            seen.push(String::from_utf8_lossy(e.name.as_bytes()).into_owned());
        }
        // Between pages: remove one entry already returned and one that is
        // not yet (the second may or may not still be listed).
        if step < 3 {
            let done: Vec<&String> = seen.iter().filter(|n| n.starts_with('n')).collect();
            let gone = done[step].clone();
            must("unlink returned", c.unlink(d, &gone));
            removed.insert(gone);
            let ahead = names[30 + step].clone();
            must("unlink ahead", c.unlink(d, &ahead));
            removed.insert(ahead);
        }
        step += 1;
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for n in &seen {
        *counts.entry(n.as_str()).or_default() += 1;
    }
    assert!(
        counts.values().all(|c| *c == 1),
        "an entry was returned twice: {counts:?}"
    );
    for n in &names {
        if !removed.contains(n) {
            assert!(
                counts.contains_key(n.as_str()),
                "{n} was there throughout and was never listed"
            );
        }
    }
    // Entries removed after they were listed are simply not in the final
    // directory; the survivors are exactly the unremoved ones.
    let left = must("names", c.names(d));
    let mut want: Vec<String> = names
        .iter()
        .filter(|n| !removed.contains(*n))
        .cloned()
        .collect();
    want.sort();
    assert_eq!(left, want);
    Ok(())
}

pub(super) fn stable_under_concurrent_create(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    let before: Vec<String> = (0..30).map(|i| format!("old-{i:02}")).collect();
    for n in &before {
        must("put", c.put(d, n, b""));
    }
    let creator_done = AtomicBool::new(false);
    let total = 150;
    let mut listings = 0;
    std::thread::scope(|s| {
        let creator = s.spawn(|| {
            let c = fx.client();
            for i in 0..total {
                must("put new", c.put(d, &format!("new-{i:03}"), b""));
            }
            creator_done.store(true, Ordering::Release);
        });
        // Full listings, in small pages, while the directory grows (a
        // creator that panicked is finished, not done: stop listing).
        while listings < 4 || !(creator_done.load(Ordering::Acquire) || creator.is_finished()) {
            let mut seen: Vec<String> = Vec::new();
            let mut cookie = 0;
            let mut cookies: HashSet<u64> = HashSet::new();
            loop {
                let page = must("readdir", c.readdir_page(d, cookie, 5, false));
                let Some(last) = page.last() else { break };
                assert!(
                    cookies.insert(last.next),
                    "readdir cookie {} came back: the listing would never end",
                    last.next
                );
                cookie = last.next;
                seen.extend(
                    page.iter()
                        .map(|e| String::from_utf8_lossy(e.name.as_bytes()).into_owned()),
                );
            }
            let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
            for n in &seen {
                *counts.entry(n.as_str()).or_default() += 1;
            }
            assert!(
                counts.values().all(|c| *c == 1),
                "an entry was listed twice while another thread created: {:?}",
                counts.iter().filter(|(_, c)| **c > 1).collect::<Vec<_>>()
            );
            for n in &before {
                assert!(
                    counts.contains_key(n.as_str()),
                    "{n} existed throughout and was not listed"
                );
            }
            listings += 1;
        }
        joined(creator.join());
    });
    assert!(listings >= 4);
    let final_names = must("names", c.names(d));
    assert_eq!(
        final_names.len(),
        before.len() + total,
        "every entry made during the listings is there"
    );
    assert!(before.iter().all(|n| final_names.contains(n)));
    Ok(())
}

pub(super) fn readdir_of_a_file_is_notdir(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x"));
    refused(
        "readdir of a file",
        c.readdir_page(f.attr.ino, 0, 10, false),
        Code::NotDir,
    );
    Ok(())
}

/// The `(ino, name, cookie)` of each entry: what a listing is, whichever
/// way it was paged.
fn key(entries: &[DirEntry]) -> Vec<(Ino, String, u64)> {
    entries
        .iter()
        .map(|e| {
            (
                e.ino,
                String::from_utf8_lossy(e.name.as_bytes()).into_owned(),
                e.next,
            )
        })
        .collect()
}

/// The rest of the listing of `d` after `cookie`, in pages of `page`
/// entries; page number `n` (from 0) is a plus page when `plus(n)`.
fn paged(
    c: &Client,
    d: Ino,
    mut cookie: u64,
    page: usize,
    plus: impl Fn(usize) -> bool,
) -> crate::VfsResult<Vec<DirEntry>> {
    let mut all = Vec::new();
    // A listing of a directory of a few dozen names ends within this many
    // pages, however it is paged; more is a cookie that does not advance.
    for n in 0..1000 {
        let got = c.readdir_page(d, cookie, page, plus(n))?;
        let Some(last) = got.last() else {
            return Ok(all);
        };
        assert!(last.next != cookie, "cookie {cookie} did not advance");
        cookie = last.next;
        all.extend(got);
    }
    panic!("a listing of {d} did not end in 1000 pages");
}

/// As [`paged`], plain and plus pages alternating, the first one being
/// plus or not as `first_plus` says.
fn alternating(
    c: &Client,
    d: Ino,
    cookie: u64,
    page: usize,
    first_plus: bool,
) -> crate::VfsResult<Vec<DirEntry>> {
    paged(c, d, cookie, page, |n| (n % 2 == 0) == first_plus)
}

pub(super) fn plus_dot_and_dotdot_are_name_only(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    must("put a", c.put(d, "a", b"1"));
    must("mkdir sub", c.mkdir(d, "sub"));
    must("symlink", c.symlink(d, "s", "a"));
    if env.caps().hard_links {
        let a = must("lookup a", c.lookup(d, "a")).attr.ino;
        must("link", c.link(a, d, "a2"));
    }
    for dir in [d, root] {
        for page in [1usize, 2, 100] {
            for plus in [false, true] {
                let all = must("readdir", paged(&c, dir, 0, page, |_| plus));
                let dots: Vec<_> = all
                    .iter()
                    .filter(|e| matches!(e.name.as_bytes(), b"." | b".."))
                    .collect();
                if dots.len() != 2 {
                    fail!("page {page} plus {plus}: expected . and .., got {dots:?}");
                }
                for e in &all {
                    let dot = matches!(e.name.as_bytes(), b"." | b"..");
                    match &e.entry {
                        Some(_) if dot => fail!(
                            "page {page} plus {plus}: {:?} carries an entry, `.` and `..` are \
                             always name-only",
                            e.name
                        ),
                        Some(_) if !plus => fail!(
                            "page {page}: {:?} carries an entry in a plain listing",
                            e.name
                        ),
                        // Whatever a view hands out has to be about the
                        // listed name. (What it must equal is the lookup's,
                        // which needs a target that answers plus entries:
                        // plan 40 P7.)
                        Some(entry) if entry.attr.ino != e.ino || entry.attr.kind != e.kind => {
                            fail!(
                                "page {page}: the entry of {:?} is for inode {} ({:?}), the \
                                 listing says {} ({:?})",
                                e.name,
                                entry.attr.ino,
                                entry.attr.kind,
                                e.ino,
                                e.kind
                            )
                        }
                        Some(_) => {}
                        None => {}
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn plus_and_plain_cookies_interleave(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    for i in 0..37 {
        must("put", c.put(d, &format!("file-{i:02}"), b""));
    }
    let whole = must("readdir whole", c.readdir_all(d, 1000));
    assert_eq!(whole.len(), 39);
    // From the start and from every entry's cookie, pages of 7 that
    // alternate plain and plus give the plain listing's rest, whichever
    // kind comes first.
    let starts = std::iter::once((0u64, 0usize))
        .chain(whole.iter().enumerate().map(|(i, e)| (e.next, i + 1)));
    for (cookie, from) in starts {
        for first_plus in [false, true] {
            let got = must("alternating", alternating(&c, d, cookie, 7, first_plus));
            if key(&got) != key(&whole[from..]) {
                fail!(
                    "alternating pages of 7 from cookie {cookie} (first plus: {first_plus}) \
                     list {:?}, one plain listing has {:?}",
                    key(&got),
                    key(&whole[from..])
                );
            }
        }
    }
    Ok(())
}

pub(super) fn plus_in_a_removed_directory_is_not_found(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    // Removed before any listing.
    let e = must("mkdir e", c.mkdir(root, "e")).attr.ino;
    must("rmdir e", c.rmdir(root, "e"));
    // Removed between pages: the client holds the directory (it was
    // handed it) and the cookie of a page it listed.
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    let names: Vec<String> = (0..10).map(|i| format!("n{i}")).collect();
    for n in &names {
        must("put", c.put(d, n, b""));
    }
    let first = must("first page", c.readdir_page(d, 0, 7, true));
    let cookie = first.last().expect("a page").next;
    for n in &names {
        must("unlink", c.unlink(d, n));
    }
    must("rmdir d", c.rmdir(root, "d"));
    for (what, dir, from) in [("before a listing", e, 0), ("between pages", d, cookie)] {
        for plus in [true, false] {
            refused(
                &format!("readdir (plus: {plus}) of a directory removed {what}"),
                c.readdir_page(dir, from, 7, plus),
                Code::NotFound,
            );
        }
    }
    // Once the platform has let go of it nothing answers for it.
    c.forget(d);
    refused_any(
        "readdirplus of a forgotten removed directory",
        c.readdir_page(d, cookie, 7, true),
        &[Code::NotFound, Code::Stale],
    );
    Ok(())
}

pub(super) fn a_cookie_resumes_from_a_fresh_client(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let first = fx.client();
    let d = must("mkdir d", first.mkdir(first.root(), "d")).attr.ino;
    for i in 0..23 {
        must("put", first.put(d, &format!("name-{i:02}"), b""));
    }
    // The first client lists in pages of 5, plain and plus alternating.
    let listed = must("first client", alternating(&first, d, 0, 5, false));
    assert_eq!(listed.len(), 25);
    // A client that never saw any of it resumes from each cookie, with no
    // stream open, and gets the rest of the listing.
    let second = fx.client();
    for plus in [false, true] {
        for (i, e) in listed.iter().enumerate() {
            let rest = must("second client", paged(&second, d, e.next, 4, |_| plus));
            if key(&rest) != key(&listed[i + 1..]) {
                fail!(
                    "a fresh client resumed (plus: {plus}) after {:?} and got {:?}, the first \
                     client listed {:?}",
                    e.name,
                    key(&rest),
                    key(&listed[i + 1..])
                );
            }
        }
    }
    // The name a cookie follows is removed before the second client
    // resumes from it. A view that placed the cookie by the name may say
    // `NotFound`, as nothing holds its place (no stream is open); it may
    // not skip: whatever it lists is exactly the names after it.
    for (i, e) in listed.iter().enumerate().skip(2) {
        let name = String::from_utf8_lossy(e.name.as_bytes()).into_owned();
        must("unlink", first.unlink(d, &name));
        for plus in [false, true] {
            match second.readdir_page(d, e.next, 1000, plus) {
                Ok(rest) => {
                    if key(&rest) != key(&listed[i + 1..]) {
                        fail!(
                            "resuming (plus: {plus}) after the removed {name:?} lists {:?}, the \
                             names after it are {:?}",
                            key(&rest),
                            key(&listed[i + 1..])
                        );
                    }
                }
                Err(err) if err.code() == Code::NotFound => {}
                Err(err) => fail!(
                    "resuming (plus: {plus}) after the removed {name:?}: {:?}",
                    err.code()
                ),
            }
        }
    }
    Ok(())
}

/// The naive `readdir`s the plus cases guard against: each is the
/// reference filesystem's listing with one naive habit, scripted into a
/// `MockVfs`. A case that passed on one of them would test nothing.
#[cfg(test)]
mod naive {
    use super::super::{run_all, ConformanceTarget, Fixture, Outcome, TargetOpts};
    use crate::caps::FrontendCaps;
    use crate::ctx::{Caller, OpCtx, OpKind};
    use crate::error::VfsError;
    use crate::mock::{Args, MockVfs, Script};
    use crate::responder::{Blocking, CollectDir};
    use crate::types::{Fh, Ino};
    use crate::{DirEntry, Name, Vfs, VfsResult};
    use constellation_types::Code;
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    enum Habit {
        /// `.` and `..` get entries in a plus listing, as any name does.
        DotsWithEntries,
        /// A plus page skips the entry right after its cookie (a plus
        /// cursor that is a position, not the cookie).
        PlusSkipsOne,
        /// A plus page of a directory that is gone is an empty listing.
        GoneIsEmpty,
        /// The cookie is a position in the listing as it is now.
        PositionCookies,
    }

    struct Naive(Habit);

    fn listing(inner: &MockVfs, ino: Ino, cookie: u64, plus: bool) -> VfsResult<Vec<DirEntry>> {
        let caller = Caller::with_groups(1000, 1000, &[]);
        let (sink, wait) = CollectDir::pair(usize::MAX);
        inner.readdir(
            &OpCtx::new(OpKind::Readdir, &caller),
            ino,
            Fh(0),
            cookie,
            plus,
            sink,
        );
        wait.wait()
    }

    fn entry_of(inner: &MockVfs, ino: Ino, name: &str) -> VfsResult<crate::Entry> {
        let caller = Caller::with_groups(1000, 1000, &[]);
        Blocking::run(|r| {
            inner.lookup(
                &OpCtx::new(OpKind::Lookup, &caller),
                ino,
                Name::new(name),
                r,
            )
        })
    }

    impl ConformanceTarget for Naive {
        type V = MockVfs;

        fn fresh(&self, caps: &FrontendCaps, _opts: TargetOpts) -> Fixture<MockVfs> {
            let mock = MockVfs::reference(caps.clone());
            let inner = mock.view_of("/", false).expect("a second view");
            let habit = self.0;
            mock.always_readdir(Script::With(Arc::new(move |call| {
                let Args::Readdir {
                    ino, cookie, plus, ..
                } = call.args
                else {
                    return Err(VfsError::new(Code::Invalid));
                };
                match habit {
                    Habit::DotsWithEntries => {
                        let mut all = listing(&inner, ino, cookie, plus)?;
                        if plus {
                            for e in &mut all {
                                if matches!(e.name.as_bytes(), b"." | b"..") {
                                    let name = String::from_utf8_lossy(e.name.as_bytes());
                                    e.entry = Some(entry_of(&inner, ino, &name)?);
                                }
                            }
                        }
                        Ok(all)
                    }
                    Habit::PlusSkipsOne => {
                        let mut all = listing(&inner, ino, cookie, plus)?;
                        if plus && cookie != 0 && !all.is_empty() {
                            all.remove(0);
                        }
                        Ok(all)
                    }
                    Habit::GoneIsEmpty => match listing(&inner, ino, cookie, plus) {
                        Err(e) if plus && e.code() == Code::NotFound => Ok(Vec::new()),
                        other => other,
                    },
                    Habit::PositionCookies => {
                        let mut all = listing(&inner, ino, 0, plus)?;
                        let from = (cookie as usize).min(all.len());
                        all.drain(..from);
                        for (i, e) in all.iter_mut().enumerate() {
                            e.next = (from + i + 1) as u64;
                        }
                        Ok(all)
                    }
                }
            })));
            Fixture::new(Arc::new(mock), caps.clone())
        }
    }

    fn fails(habit: Habit, case: &str) {
        let report = run_all(&Naive(habit), Some(case));
        assert_eq!(report.tests.len(), 1, "{case}");
        let outcome = report.outcome(case).expect("ran");
        assert!(
            matches!(outcome, Outcome::Failed(_)),
            "{case} passed on a naive implementation: {outcome:?}"
        );
    }

    #[test]
    fn name_only_dots_fail_on_a_plus_listing_that_gives_them_entries() {
        fails(
            Habit::DotsWithEntries,
            "readdir::plus_dot_and_dotdot_are_name_only",
        );
    }

    #[test]
    fn interleaving_fails_on_a_plus_cursor_that_is_not_the_cookie() {
        fails(
            Habit::PlusSkipsOne,
            "readdir::plus_and_plain_cookies_interleave",
        );
    }

    #[test]
    fn a_removed_directory_fails_on_a_plus_that_lists_it_empty() {
        fails(
            Habit::GoneIsEmpty,
            "readdir::plus_in_a_removed_directory_is_not_found",
        );
    }

    #[test]
    fn a_fresh_client_fails_on_cookies_that_are_positions() {
        fails(
            Habit::PositionCookies,
            "readdir::a_cookie_resumes_from_a_fresh_client",
        );
    }

    #[test]
    fn the_naive_listings_are_otherwise_the_reference() {
        // Guards the guard: a habit that broke the listing in general
        // would make the failures above vacuous.
        for habit in [Habit::DotsWithEntries, Habit::GoneIsEmpty] {
            let report = run_all(&Naive(habit), Some("readdir::cookies_resume"));
            assert_eq!(
                report.outcome("readdir::cookies_resume_where_they_left_off"),
                Some(&Outcome::Passed)
            );
        }
    }
}
