//! Plus entries (plan 40 §4.5): what a `readdir` with `plus` hands out
//! with a name is what `lookup(dir, name)` answers at that moment, waited
//! for as `getattr` of the child waits — or the name alone, when the view
//! cannot vouch for it. The page loop (`listing::list_page`) asks, per batch of
//! up to [`BATCH`](super::listing::BATCH) names it reads (one replica
//! read, and more than a plus page holds: 32 KiB over ~176 bytes a record,
//! so a page is one batch), for one session wait ([`PlusPage::wait`]) and
//! then, per name it adds, for its entry ([`PlusPage::entry`]):
//!
//! 0. **Held names.** A name whose entry the platform already holds,
//!    leading to the listed inode, is added name-only
//!    (`DirSink::add_held`) and nothing below runs for it, not even the
//!    wait: an entry would repeat the platform's. The FUSE
//!    adapter answers [`DirSink::holds`](constellation_vfs::DirSink::holds)
//!    from the lookups its kernel was answered, within their TTL; `git
//!    status` lists each directory right after it looked up its names.
//! 1. **Zero TTL** (`--cto strict` on a node that is not a lone
//!    sequencer): every entry is name-only and nothing below runs. An
//!    attribute with timeout 0 is pure overhead for the kernel, which
//!    falls back to plain pages by itself.
//! 2. **One session wait per batch**, with the keys a `lookup` of each
//!    name waits on (`Dentry(dir, name)`, then `Ino(child)`), before any
//!    write shard is taken (plan 30 §M6: never block holding one). The
//!    listing's own `Dir(dir)` wait (cookie 0 only) is weaker than the
//!    `Ino(child)` wait `getattr` makes: a queued replay or an unshipped
//!    durable-gated row that touched only the child does not touch
//!    `Dir(dir)`. And a later page waits on nothing else: a queued replay
//!    of this node's unlink or rename of a name touches the name, not the
//!    inode it led to.
//! 3. **The attributes under the child's write shard**, from the dentry's
//!    copy ([`MetaStore::lookup`]: the copy and the `atime` point read; a
//!    hard-linked name holds none, so its attributes cost one `0x01`
//!    read, plan 45), overlaid with the child's pending write session.
//!    A name that is gone, now names another inode or cannot be read is
//!    name-only: the kernel `LOOKUP`s it later, and the page never fails
//!    after an entry was added. This is as fresh as `current_attr`: the
//!    copy is written in the transaction that writes the inode row, and
//!    a flush commits and retires a session under the same shard lock,
//!    so the read sees the session or its committed row, never neither.
//!    It decodes no attributes for names outside the page: an entry is
//!    made only when the page is about to add it.
//! 4. **The entry** is [`View::entry_out`]: the view's TTL, generation 0,
//!    `note_reached` for a confined view. The view's root and `ROOT_INO`
//!    are name-only (K4: node id 1 is `-EIO`).
//!
//! Synthetic directories ([`PlusPage::synthetic_entry`]): each child's
//! entry is what a lookup answers there, built from the node the listing
//! already holds, not by a `lookup_synthetic` per name (which clones a
//! mirror's frozen directory per call and would make a page O(n) in
//! clones). A node of a snapshot that is gone is name-only.
//!
//! The fault point of plan 42 K1 (`fault::getattr_delay`) delays the page
//! once, by the longest delay of the entries it made, before it is
//! answered: a `getattr` per entry, held, as `ls -l` makes.

use super::*;
use constellation_vfs::Entry;
use std::collections::HashSet;
use std::time::Duration;

/// What the tests of this module run the module's rules against: each is
/// one naive implementation of a step (a test that passes on one tests
/// nothing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(super) enum Naive {
    /// Step 3 without the pending write session.
    NoOverlay,
    /// Step 3 with the dentry read before the shard (and so before
    /// whatever lands between).
    ReadBeforeShard,
    /// Step 2 without the children's keys (the listing's `Dir` wait only).
    NoChildWait,
    /// Step 2 with the children's inodes but not their names.
    NoDentryWait,
    /// Step 1 ignored.
    KeepZeroTtl,
    /// Step 3 without the check that the name still leads to the listed
    /// inode.
    AnyInode,
    /// Step 4 without `note_reached`.
    NoReach,
    /// A scratch directory's names looked up in the shared namespace.
    NoScratch,
}

/// A test's hook between a plus page's listing and its attribute reads
/// ([`BETWEEN`]).
#[cfg(test)]
type BetweenHook = Box<dyn FnMut(&str)>;

#[cfg(test)]
thread_local! {
    /// The naive implementation this thread's pages run (`None`: the real one).
    pub(super) static NAIVE: std::cell::Cell<Option<Naive>> = const { std::cell::Cell::new(None) };
    /// Called with the name, on the listing thread, after a name's batch
    /// was waited for and before its attributes are read (like
    /// `copy::AFTER_STEP`): what tests do between the listing and the
    /// attribute read.
    pub(super) static BETWEEN: std::cell::RefCell<Option<BetweenHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn naive(which: Naive) -> bool {
    NAIVE.with(|n| n.get()) == Some(which)
}

#[cfg(not(test))]
#[inline(always)]
fn naive(_: Naive) -> bool {
    false
}

#[cfg(test)]
fn between(name: &str) {
    BETWEEN.with(|h| {
        if let Some(hook) = h.borrow_mut().as_mut() {
            hook(name);
        }
    });
}

#[cfg(not(test))]
#[inline(always)]
fn between(_: &str) {}

/// The plus entries of one page of directory `dir`.
pub(super) struct PlusPage<'a> {
    view: &'a View,
    dir: Ino,
    /// `dir` is a node-private scratch directory: its names are looked up
    /// in the scratch namespace.
    scratch: bool,
    /// Every entry is name-only (step 1).
    name_only: bool,
    /// The longest `getattr` delay the entries made so far.
    delay: Option<Duration>,
    /// The ids of the snapshots that exist, read once for the page.
    snapshots: Option<HashSet<String>>,
}

impl View {
    /// The plus entries of a page of directory `dir`.
    pub(super) fn plus_page(&self, dir: Ino, scratch: bool) -> PlusPage<'_> {
        PlusPage {
            view: self,
            dir,
            scratch: scratch && !naive(Naive::NoScratch),
            name_only: self.ttl().is_zero() && !naive(Naive::KeepZeroTtl),
            delay: None,
            snapshots: None,
        }
    }
}

impl PlusPage<'_> {
    /// Step 2: wait, once, for what a `lookup` of each of `children` (a
    /// name and the inode the listing says it leads to) would wait for:
    /// `Dentry(dir, name)`, then `Ino(child)` as `getattr` waits. The
    /// dentry's key is not implied by the listing's `Dir(dir)` wait, which
    /// only a first page makes: a later page's names are read with no
    /// wait at all, and a queued replay of this node's own unlink or
    /// rename touches the name and the directory, never the inode the
    /// name led to. Takes no lock.
    pub(super) fn wait<'n>(&self, children: impl Iterator<Item = (Ino, &'n str)>) {
        if self.name_only || naive(Naive::NoChildWait) {
            return;
        }
        let dentries = !naive(Naive::NoDentryWait);
        let mut keys: Vec<ReadKey> = Vec::new();
        for (child, name) in children {
            if dentries {
                keys.push(ReadKey::Dentry(self.dir, name.to_string()));
            }
            keys.push(ReadKey::Ino(child));
        }
        if !keys.is_empty() {
            self.view.session_wait(&keys);
        }
    }

    /// Steps 3 and 4: the entry of `name`, which the listing says leads
    /// to `child`, or `None` for a name-only entry. `name` is the stored
    /// form (UTF-8).
    pub(super) fn entry(&mut self, child: Ino, name: &[u8]) -> Option<Entry> {
        let view = self.view;
        if self.name_only
            || child == view.view_root
            || child == constellation_fs_core::types::ROOT_INO
        {
            return None;
        }
        let name = std::str::from_utf8(name).ok()?;
        let dir = self.dir;
        let scratch = self.scratch;
        let read_dentry = || -> Result<Option<constellation_fs_core::FileAttr>, MetaError> {
            if scratch {
                if let Some(attr) = view.meta.scratch_lookup(dir, name)? {
                    return Ok(Some(attr));
                }
            }
            // As `lookup`: a name the scratch namespace does not hold is
            // the shared namespace's.
            view.meta.lookup(dir, name)
        };
        let early = naive(Naive::ReadBeforeShard).then(read_dentry);
        between(name);
        let shard = view.writes.lock(child);
        constellation_vfs::watch::stage("meta read");
        let found = match early {
            Some(early) => early,
            None => read_dentry(),
        };
        let mut attr = match found {
            Ok(Some(attr)) if attr.ino == child || naive(Naive::AnyInode) => attr,
            // Gone, another inode's now, or unreadable: the kernel looks
            // the name up itself.
            _ => return None,
        };
        if !naive(Naive::NoOverlay) {
            if let Some(pending) = view.writes.pending(&shard, child) {
                pending.overlay(&mut attr);
            }
        }
        drop(shard);
        if let Some(delay) = crate::fault::getattr_delay(
            attr.ino,
            attr.kind == constellation_fs_core::InodeKind::Dir,
        ) {
            self.delay = Some(self.delay.map_or(delay, |d| d.max(delay)));
        }
        Some(self.entered(&attr))
    }

    /// The entry of a child of a synthetic directory (`node`, numbered
    /// `ino`), or `None` for a node of a snapshot that is gone.
    pub(super) fn synthetic_entry(&mut self, ino: Ino, node: &SyntheticNode) -> Option<Entry> {
        if self.name_only {
            return None;
        }
        if let SyntheticNode::Frozen { snapshot_id, .. } = node {
            let view = self.view;
            let known = self.snapshots.get_or_insert_with(|| {
                view.meta
                    .snapshots(None)
                    .map(|rows| rows.into_iter().map(|row| row.id).collect())
                    .unwrap_or_default()
            });
            if !known.contains(snapshot_id) {
                return None;
            }
        }
        Some(self.view.entry_out(&self.view.synthetic_attr(ino, node)))
    }

    fn entered(&self, attr: &constellation_fs_core::FileAttr) -> Entry {
        if naive(Naive::NoReach) {
            return Entry {
                attr: self.view.attr_out(attr),
                generation: 0,
            };
        }
        self.view.entry_out(attr)
    }

    /// The page is complete: the fault point's delay, once.
    pub(super) fn finish(self) {
        if let Some(delay) = self.delay {
            std::thread::sleep(delay);
        }
    }
}

/// What the tests of plus pages share.
#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use std::sync::mpsc;

    /// This thread's plus pages run `which` (a naive step) until dropped.
    pub(in crate::view) struct NaiveGuard;

    impl NaiveGuard {
        pub(in crate::view) fn set(which: Naive) -> Self {
            NAIVE.with(|n| n.set(Some(which)));
            NaiveGuard
        }
    }

    impl Drop for NaiveGuard {
        fn drop(&mut self) {
            NAIVE.with(|n| n.set(None));
        }
    }

    /// Runs `check` on the real implementation, which has to pass, and on
    /// `naive`, which has to fail: a check that passes on a naive
    /// implementation of the step it guards tests nothing.
    pub(in crate::view) fn guarded(
        naive: Naive,
        check: impl Fn(Option<Naive>) -> Result<(), String>,
    ) {
        if let Err(e) = check(None) {
            panic!("the real plus page fails its check: {e}");
        }
        let _naive = NaiveGuard::set(naive);
        if check(Some(naive)).is_ok() {
            panic!("the check passes on the naive plus page ({naive:?}): it guards nothing");
        }
    }

    /// One plus page of directory `dir` from `cookie`, at most `limit`
    /// entries.
    pub(in crate::view) fn plus_page(
        view: &View,
        dir: Ino,
        cookie: u64,
        limit: usize,
    ) -> constellation_vfs::VfsResult<Vec<constellation_vfs::DirEntry>> {
        let caller = constellation_vfs::Caller::root();
        let (sink, wait) = constellation_vfs::CollectDir::pair(limit);
        constellation_vfs::Vfs::readdir(
            view,
            &constellation_vfs::OpCtx::new(constellation_vfs::OpKind::Readdirplus, &caller),
            dir,
            constellation_vfs::Fh(0),
            cookie,
            true,
            sink,
        );
        wait.wait()
    }

    /// [`BETWEEN`] is set on this thread until dropped.
    pub(in crate::view) struct BetweenGuard;

    impl BetweenGuard {
        pub(in crate::view) fn set(hook: impl FnMut(&str) + 'static) -> Self {
            BETWEEN.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
            BetweenGuard
        }

        /// The hook hands each name to a thread of `scope`, which runs
        /// `act` on it while the page waits: what another client does
        /// between a page's listing and its attribute reads. (The page's
        /// thread holds no lock then.)
        pub(in crate::view) fn acting<'scope, 'env>(
            scope: &'scope std::thread::Scope<'scope, 'env>,
            act: impl Fn(&str) + Send + 'scope,
        ) -> Self {
            let (go_tx, go_rx) = mpsc::channel::<String>();
            let (done_tx, done_rx) = mpsc::channel::<()>();
            scope.spawn(move || {
                for name in go_rx {
                    act(&name);
                    if done_tx.send(()).is_err() {
                        break;
                    }
                }
            });
            Self::set(move |name| {
                go_tx.send(name.to_string()).unwrap();
                done_rx.recv().unwrap();
            })
        }
    }

    impl Drop for BetweenGuard {
        fn drop(&mut self) {
            BETWEEN.with(|h| *h.borrow_mut() = None);
        }
    }
}
