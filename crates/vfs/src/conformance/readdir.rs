//! `readdir`: `.` and `..`, cookies that resume where they left off, the
//! `plus` flag, and a listing that neither repeats nor loses an entry that
//! was there throughout while the directory changes under it.

use super::{joined, must, refused, Env, TestResult};
use crate::types::FileKind;
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
