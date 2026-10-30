//! Directory pages (plan 40 §4.9): name-ordered range scans resumed from
//! keyed cookies, and the hints that place them, pinned while the
//! directory is open.
//!
//! A page of `readdir` is a range scan of the directory's dentry keys
//! (`0x02 | dir | name`) from a name, in batches of [`BATCH`], emitted until
//! the sink is full: O(page), for plain `READDIR` as for `READDIRPLUS`, and
//! for scratch directories. No listing is sorted, kept or snapshotted.
//!
//! The cookie of an entry is a keyed hash of its name ([`CookieKey`]), the
//! same in every stream, view and node. To resume at cookie `c` the view
//! must find the *name* `c` stands for:
//!
//! - **Hint**: every served page is remembered ([`Listing::record`]) as
//!   `{anchor, last, cookies}`: the name before its first entry, its last
//!   name, and the cookies of its entries in order. The last [`PAGES`]
//!   pages of a directory are kept, least recently used directory first
//!   out, under a byte cap. A cookie found in a remembered page resumes
//!   after that page's last name when it is the page's last cookie (the
//!   sequential case: the kernel continues from where the reply ended),
//!   and otherwise by scanning from the page's anchor to the cookie's own
//!   entry ([`Seek`]): the resume is after that name, as if it were known.
//!   That is exact even if the anchor has since been removed (a key seek
//!   past a name does not need the name to exist), and if the entry the
//!   cookie names was removed too: the place is then where the first of
//!   the page's later entries, or the first name past the page, begins. It
//!   covers the kernel's re-asks from any cookie of a reply, `seekdir` back
//!   within the last pages, and another handle (`nfsd`).
//!
//!   A resume is always after a place in name order, so a page served
//!   holds every entry from its anchor to its last name (nothing in
//!   between is left out), which is what lets a later resume inside it
//!   find its place. A page that instead listed the entries of its range
//!   that a resume had not yet consumed, a name created behind the cursor
//!   among them, would make the next resume inside it list again what the
//!   page before had given (plan 40 P3 review).
//! - **Cold**: a cookie no page holds costs one O(n) scan of the
//!   directory's names, hashing them, until one hashes to `c`: the page
//!   resumes after it. A collision (two names of one cookie) resumes after
//!   the first in name order: entries between them may be listed twice,
//!   never skipped. A cookie whose name is gone answers `NotFound`.
//!
//! - **Pinned**: while a directory is open (`Vfs::opendir` answered, not yet
//!   `releasedir`), its hints are not left to the cache. Every page served
//!   for it gets the directory's next sequence number; every open stream
//!   records the sequence number it started at (its `opendir` or last
//!   rewind: a page requested at cookie 0 through it) and the number of
//!   the page last served through it. The **pin window** is every page
//!   from the smallest start among the open streams on: a superset of
//!   every cookie a `telldir` on any of them could have returned and of
//!   every cookie the kernel can resume one from. Its pages are kept
//!   outside the hint cap, so a listing that deletes what it has read
//!   (`rm -rf`, `find -delete`), a `seekdir` back to a deleted entry and a
//!   handover mid-listing resume exactly. Cookies stay stream-independent
//!   (a lookup is by `(directory, cookie)`; the stream only decides what
//!   may not be dropped).
//!
//!   The window is bounded: `CONSTELLATION_READDIR_PIN_BYTES_PER_DIR`
//!   (128 MiB) per directory and `CONSTELLATION_READDIR_PIN_BYTES`
//!   (512 MiB) per node, per node the directory with the most pinned bytes
//!   giving up first. Past a bound the oldest pages of the window become
//!   ordinary hints, **except each open stream's last page**, which is
//!   kept whatever the bounds say (so sequential continuation stays exact
//!   at any size; only a `seekdir` back into an overflowed region can meet
//!   a lost cookie). `release_stream` frees what only that stream
//!   pinned, each page once (the cost is that of the pages it frees; the
//!   window start is recomputed in O(log streams)); a view that goes away
//!   releases its streams; nothing that is released or unpinned stays
//!   counted.
//!
//! A resume of a directory that is gone answers like the first page of one
//! (an empty tail is checked against the directory's record, once, at the
//! end of a listing).
//!
//! A page never holds two equal cookies: when the next entry's cookie
//! already occurs in the page, the page ends before it.
//!
//! The cache is node-wide (every view of the node shares one [`Listing`])
//! and keyed by the replica's directory inode, never by file handle. No
//! lock is held across a replica read: a scan reads first and records
//! after. Nothing here writes: no new key, no put on any mutation.

use super::*;
use constellation_meta::RawDirEntry;
use constellation_vfs::{DirSink, Fh};
use cookie::{CookieBits, CookieKey};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Entries read per replica round trip of a page or a cold scan.
pub(crate) const BATCH: usize = 256;

/// Pages remembered per directory.
const PAGES: usize = 4;

/// Shards of the hint cache (by directory).
const SHARDS: usize = 16;

/// Env `CONSTELLATION_READDIR_HINT_BYTES`: the hint cache's byte cap.
pub const HINT_BYTES_ENV: &str = "CONSTELLATION_READDIR_HINT_BYTES";

/// The cap's default: 16 MiB per node.
pub const DEFAULT_HINT_BYTES: usize = 16 << 20;

/// The cap `CONSTELLATION_READDIR_HINT_BYTES` names, or the default.
pub fn hint_bytes_from(var: Option<&str>) -> usize {
    match var.map(str::trim) {
        None | Some("") => DEFAULT_HINT_BYTES,
        Some(v) => v.parse::<usize>().unwrap_or_else(|_| {
            tracing::warn!(
                env = HINT_BYTES_ENV,
                value = v,
                "ignoring an unparseable listing hint cache size (expected a byte count)"
            );
            DEFAULT_HINT_BYTES
        }),
    }
}

/// A set of cookies (already uniformly distributed: no second hash).
pub(crate) type Cookies = std::collections::HashSet<u64, BuildHasherDefault<Identity>>;

#[derive(Default)]
pub(crate) struct Identity(u64);

impl Hasher for Identity {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0 << 8) | u64::from(*b);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

/// One served page: enough to place any of its cookies. Immutable once
/// recorded (pages are shared by `Arc`: a lookup that searches the old
/// pages of a big pin window does so outside the shard lock).
pub(crate) struct Page {
    /// The directory's page sequence number at the time it was served
    /// (set by [`Listing::record`]); pages of one directory are numbered
    /// 0, 1, 2, … in the order they were served, through any stream.
    seq: u64,
    /// The name before the page's first entry (`None`: the directory's
    /// start). A scan resumes strictly after it.
    anchor: Option<Box<[u8]>>,
    /// The name of the page's last entry.
    last: Box<[u8]>,
    /// The cookies of its entries, in order; all distinct.
    cookies: Box<[u64]>,
}

type PageRef = Arc<Page>;

impl Page {
    fn bytes(&self) -> usize {
        8 * self.cookies.len() + self.anchor.as_ref().map_or(0, |a| a.len()) + self.last.len() + 64
    }

    /// Where `cookie` is among the page's entries.
    fn locate(&self, cookie: u64) -> Option<usize> {
        // The sequential case first: the page's own last cookie.
        if self.cookies.last() == Some(&cookie) {
            return Some(self.cookies.len() - 1);
        }
        self.cookies.iter().position(|c| *c == cookie)
    }
}

/// An open directory stream: its owner (a view) and its id.
pub(crate) type StreamKey = (u64, u64);

/// What a directory knows of one open stream.
#[derive(Clone, Copy)]
struct Open {
    /// The first page sequence number the stream may still be resumed
    /// from: the directory's next one at its `opendir` or last rewind.
    start: u64,
    /// The sequence number of the page last served through the stream.
    last: Option<u64>,
}

/// The newest pages of a directory's pin window a lookup searches under
/// the lock; older ones are searched outside it.
const RECENT: usize = 4;

#[derive(Default)]
struct DirHints {
    /// Ordinary hints (the last [`PAGES`] pages when no stream pins the
    /// directory), oldest first. Counted against the hint cap.
    ring: VecDeque<PageRef>,
    /// The pin window: while a stream is open, every page served at or
    /// after the smallest `start` among the open streams, oldest first.
    /// Never evicted by the hint cap; bounded by the pin caps.
    pinned: VecDeque<PageRef>,
    /// The last page of a stream that left the window (a pin cap
    /// overflow): kept for that stream whatever the caps say.
    held: BTreeMap<u64, PageRef>,
    /// The sequence number the next page served gets.
    seq_next: u64,
    /// The streams open on the directory.
    open: BTreeMap<StreamKey, Open>,
    /// `open`'s starts in order: the window begins at the first.
    by_start: BTreeSet<(u64, StreamKey)>,
    /// How many open streams have a page as their last one.
    lasts: BTreeMap<u64, u32>,
    ring_bytes: usize,
    pinned_bytes: usize,
    held_bytes: usize,
    /// The overflow of this directory's pin cap was logged.
    warned: bool,
    /// The shard tick of the last use (the LRU key).
    tick: u64,
    /// Pages dropped under the shard lock, freed after it.
    garbage: Vec<PageRef>,
}

impl DirHints {
    fn is_empty(&self) -> bool {
        self.open.is_empty()
            && self.ring.is_empty()
            && self.pinned.is_empty()
            && self.held.is_empty()
    }

    fn acct(&self) -> Acct {
        Acct {
            ring: self.ring_bytes,
            pinned: self.pinned_bytes,
            held: self.held_bytes,
            open: self.open.len(),
        }
    }

    /// An ordinary hint, in sequence order; the oldest past [`PAGES`] go.
    fn ring_insert(&mut self, page: PageRef) {
        let at = self.ring.partition_point(|p| p.seq < page.seq);
        if self.ring.get(at).is_some_and(|p| p.seq == page.seq) {
            self.garbage.push(page);
            return;
        }
        self.ring_bytes += page.bytes();
        self.ring.insert(at, page);
        while self.ring.len() > PAGES {
            if let Some(p) = self.ring.pop_front() {
                self.ring_bytes -= p.bytes();
                self.garbage.push(p);
            }
        }
    }

    /// The pinned or held page numbered `seq` (a stream's last page is
    /// always one or the other).
    fn page_at(&self, seq: u64) -> Option<&PageRef> {
        let at = self.pinned.partition_point(|p| p.seq < seq);
        match self.pinned.get(at) {
            Some(p) if p.seq == seq => Some(p),
            _ => self.held.get(&seq),
        }
    }

    fn pinned_push(&mut self, page: PageRef) {
        self.pinned_bytes += page.bytes();
        self.pinned.push_back(page);
    }

    fn pinned_pop(&mut self) -> Option<PageRef> {
        let p = self.pinned.pop_front()?;
        self.pinned_bytes -= p.bytes();
        Some(p)
    }

    fn hold(&mut self, page: PageRef) {
        self.held_bytes += page.bytes();
        if let Some(old) = self.held.insert(page.seq, page) {
            self.held_bytes -= old.bytes();
        }
    }

    fn unhold(&mut self, seq: u64) -> Option<PageRef> {
        let p = self.held.remove(&seq)?;
        self.held_bytes -= p.bytes();
        Some(p)
    }

    /// A page that left the pin window: held while a stream has it as its
    /// last page, an ordinary hint otherwise.
    fn leave_window(&mut self, page: PageRef) {
        if self.lasts.contains_key(&page.seq) {
            self.hold(page);
        } else {
            self.ring_insert(page);
        }
    }

    /// Register a stream.
    fn open_stream(&mut self, key: StreamKey, start: u64, last: Option<u64>) {
        if self.open.contains_key(&key) {
            return;
        }
        self.open.insert(key, Open { start, last });
        self.by_start.insert((start, key));
        if let Some(seq) = last {
            *self.lasts.entry(seq).or_default() += 1;
        }
    }

    /// `seq` is no longer the last page of one stream.
    fn drop_last(&mut self, seq: u64) {
        match self.lasts.get_mut(&seq) {
            Some(n) if *n > 1 => *n -= 1,
            Some(_) => {
                self.lasts.remove(&seq);
                if let Some(p) = self.unhold(seq) {
                    self.ring_insert(p);
                }
            }
            None => {}
        }
    }

    /// Stream `key` was served page `seq`.
    fn set_last(&mut self, key: StreamKey, seq: u64) {
        let Some(open) = self.open.get_mut(&key) else {
            return;
        };
        let old = open.last.replace(seq);
        *self.lasts.entry(seq).or_default() += 1;
        if let Some(old) = old {
            self.drop_last(old);
        }
    }

    /// The window begins where the oldest open stream does: pages before
    /// it leave. Each page leaves once; the cost of a release is that of
    /// the pages it frees.
    fn advance_window(&mut self) {
        let start = self.by_start.first().map_or(u64::MAX, |(s, _)| *s);
        let mut left = Vec::new();
        while self.pinned.front().is_some_and(|p| p.seq < start) {
            if let Some(p) = self.pinned_pop() {
                if self.lasts.contains_key(&p.seq) {
                    self.hold(p);
                } else {
                    left.push(p);
                }
            }
        }
        // Only the newest few are worth keeping as hints.
        let skip = left.len().saturating_sub(PAGES);
        self.garbage.extend(left.drain(..skip));
        for p in left {
            self.ring_insert(p);
        }
    }

    /// Stream `key` was closed.
    fn close_stream(&mut self, key: StreamKey) {
        let Some(open) = self.open.remove(&key) else {
            return;
        };
        self.by_start.remove(&(open.start, key));
        if let Some(seq) = open.last {
            self.drop_last(seq);
        }
        if self.open.is_empty() {
            self.warned = false;
        }
        self.advance_window();
    }

    /// Unpin the oldest page of the window (a pin cap overflow): it
    /// becomes an ordinary hint, except a stream's last page, which is
    /// held. Returns the bytes taken out of the window.
    fn unpin_oldest(&mut self) -> Option<usize> {
        let p = self.pinned_pop()?;
        let bytes = p.bytes();
        self.leave_window(p);
        Some(bytes)
    }

    /// Unpin the oldest pages until the window is within `cap`; whether
    /// any went (and whether this directory has not been warned about).
    fn enforce(&mut self, cap: usize) -> Overflow {
        let mut out = Overflow::default();
        while self.pinned_bytes > cap {
            if self.unpin_oldest().is_none() {
                break;
            }
            out.unpinned = true;
        }
        self.overflowed(out)
    }

    fn overflowed(&mut self, mut out: Overflow) -> Overflow {
        if out.unpinned && !self.warned {
            self.warned = true;
            out.first = true;
        }
        out
    }
}

/// The result of a pin cap enforcement.
#[derive(Default, Clone, Copy)]
struct Overflow {
    unpinned: bool,
    /// The first for this directory since it was last without streams.
    first: bool,
}

/// A directory's byte counts, to adjust the shard's and the node's.
#[derive(Clone, Copy)]
struct Acct {
    ring: usize,
    pinned: usize,
    held: usize,
    open: usize,
}

#[derive(Default)]
struct Shard {
    dirs: HashMap<Ino, DirHints>,
    /// Directories by last use, least recent first.
    lru: BTreeMap<u64, Ino>,
    tick: u64,
    /// Ordinary hints' bytes (what the hint cap bounds).
    ring_bytes: usize,
}

impl Shard {
    fn touch(&mut self, dir: Ino) {
        self.tick += 1;
        if let Some(d) = self.dirs.get_mut(&dir) {
            self.lru.remove(&d.tick);
            d.tick = self.tick;
            self.lru.insert(d.tick, dir);
        }
    }
}

/// Where a cookie resumes.
pub(crate) enum Resume {
    /// 0, 1 or 2: from the first entry.
    Start,
    /// A remembered page holds it: scan strictly after `after`, and, with
    /// a `seek`, pass the entries before the cookie's place first.
    Hint {
        after: Option<Vec<u8>>,
        seek: Option<Seek>,
    },
    /// No page holds it.
    Cold,
}

#[cfg(test)]
impl Resume {
    /// Resumes strictly after `name`, with no seek (the sequential case).
    pub(crate) fn is_after_for_test(&self, name: &[u8]) -> bool {
        matches!(self, Resume::Hint { after: Some(a), seek: None } if a.as_slice() == name)
    }
}

/// A resume inside a remembered page: the place of `cookie` among the
/// entries after the page's anchor.
///
/// The page held every entry from its anchor to `last` when it was served,
/// in name order, the cookie's own among them; the entries of `ahead` came
/// after it. Scanning from the anchor, an entry is before the place (the
/// kernel consumed it, or it was created since behind the cursor) while it
/// is not past `last` and not one of `ahead`, and the cookie's own entry is
/// the last of those. So the place is after the cookie's entry, or, if it
/// was removed, before the first entry of `ahead` or past `last` that is
/// still there. An entry that was there throughout is never passed: it is
/// either at or before the cookie's (consumed), or in `ahead`, or after
/// `last`. A name created since with the cookie's own value (a collision)
/// can only end the seek early, which lists names twice, never skips one.
pub(crate) struct Seek {
    cookie: u64,
    ahead: Cookies,
    last: Vec<u8>,
}

impl Seek {
    fn of(page: &Page, i: usize) -> Resume {
        if i + 1 == page.cookies.len() {
            // The page's end: resume after its last name.
            Resume::Hint {
                after: Some(page.last.to_vec()),
                seek: None,
            }
        } else {
            Resume::Hint {
                after: page.anchor.as_ref().map(|a| a.to_vec()),
                seek: Some(Seek {
                    cookie: page.cookies[i],
                    ahead: page.cookies[i + 1..].iter().copied().collect(),
                    last: page.last.to_vec(),
                }),
            }
        }
    }
}

/// The counters of the listing path (exported by plan 40 P8).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListingStats {
    /// Pages resumed from a remembered page.
    pub hint_hits: u64,
    /// Pages resumed by scanning the directory from its start.
    pub cold_scans: u64,
    /// Entries those scans read.
    pub cold_scan_entries: u64,
    /// Cold cookies whose name was gone (`ENOENT`).
    pub lost_cookies: u64,
    /// Equal cookies of different names seen (a page ended before one, or
    /// a cold resume listed a second name of its cookie).
    pub cookie_collisions: u64,
    /// Bytes the ordinary hints hold (what the hint cap bounds).
    pub hint_bytes: u64,
    /// Directory streams open.
    pub open_streams: u64,
    /// Directories with at least one stream open.
    pub pinned_dirs: u64,
    /// Bytes of pinned pages (what the pin caps bound).
    pub pinned_bytes: u64,
    /// Bytes of streams' last pages kept past the pin caps.
    pub exempt_bytes: u64,
    /// Times a pin cap unpinned pages.
    pub pin_overflows: u64,
}

/// Env `CONSTELLATION_READDIR_PIN_BYTES_PER_DIR`.
pub const PIN_BYTES_PER_DIR_ENV: &str = "CONSTELLATION_READDIR_PIN_BYTES_PER_DIR";
/// Env `CONSTELLATION_READDIR_PIN_BYTES`.
pub const PIN_BYTES_ENV: &str = "CONSTELLATION_READDIR_PIN_BYTES";
/// 128 MiB (about 16M cookies) per directory.
pub const DEFAULT_PIN_BYTES_PER_DIR: usize = 128 << 20;
/// 512 MiB per node.
pub const DEFAULT_PIN_BYTES: usize = 512 << 20;

/// The bounds of the pin windows (plan 40 §4.9). Each open stream's last
/// page is outside both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinCaps {
    pub per_dir: usize,
    pub per_node: usize,
}

impl Default for PinCaps {
    fn default() -> Self {
        Self {
            per_dir: DEFAULT_PIN_BYTES_PER_DIR,
            per_node: DEFAULT_PIN_BYTES,
        }
    }
}

impl PinCaps {
    /// The caps the two env variables name (an unparseable value warns
    /// and keeps the default, as the hint cache size does).
    pub fn from_vars(per_dir: Option<&str>, per_node: Option<&str>) -> Self {
        let one = |var: Option<&str>, env: &str, default: usize| match var.map(str::trim) {
            None | Some("") => default,
            Some(v) => v.parse::<usize>().unwrap_or_else(|_| {
                tracing::warn!(
                    env,
                    value = v,
                    "ignoring an unparseable listing pin cap (expected a byte count)"
                );
                default
            }),
        };
        Self {
            per_dir: one(per_dir, PIN_BYTES_PER_DIR_ENV, DEFAULT_PIN_BYTES_PER_DIR),
            per_node: one(per_node, PIN_BYTES_ENV, DEFAULT_PIN_BYTES),
        }
    }

    pub fn from_env() -> Self {
        Self::from_vars(
            std::env::var(PIN_BYTES_PER_DIR_ENV).ok().as_deref(),
            std::env::var(PIN_BYTES_ENV).ok().as_deref(),
        )
    }
}

/// The node's listing state: the cookie key, the hint cache and the pin
/// windows of its open directory streams.
pub struct Listing {
    key: CookieKey,
    shards: Vec<Mutex<Shard>>,
    shard_cap: usize,
    caps: PinCaps,
    hint_hits: AtomicU64,
    cold_scans: AtomicU64,
    cold_scan_entries: AtomicU64,
    lost_cookies: AtomicU64,
    cookie_collisions: AtomicU64,
    hint_bytes: AtomicU64,
    open_streams: AtomicU64,
    pinned_dirs: AtomicU64,
    pinned_bytes: AtomicU64,
    exempt_bytes: AtomicU64,
    pin_overflows: AtomicU64,
    /// Tests only: run after each batch a cold scan reads (`list_page`).
    #[cfg(test)]
    pub(crate) cold_scan_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Listing {
    /// A listing with `key`, remembering at most `hint_bytes` bytes of
    /// ordinary hints (split evenly over the shards), under the default
    /// pin caps.
    pub fn new(key: CookieKey, hint_bytes: usize) -> Arc<Self> {
        Self::with_caps(key, hint_bytes, PinCaps::default())
    }

    pub fn with_caps(key: CookieKey, hint_bytes: usize, caps: PinCaps) -> Arc<Self> {
        Arc::new(Self {
            key,
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            shard_cap: hint_bytes / SHARDS,
            caps,
            hint_hits: AtomicU64::new(0),
            cold_scans: AtomicU64::new(0),
            cold_scan_entries: AtomicU64::new(0),
            lost_cookies: AtomicU64::new(0),
            cookie_collisions: AtomicU64::new(0),
            hint_bytes: AtomicU64::new(0),
            open_streams: AtomicU64::new(0),
            pinned_dirs: AtomicU64::new(0),
            pinned_bytes: AtomicU64::new(0),
            exempt_bytes: AtomicU64::new(0),
            pin_overflows: AtomicU64::new(0),
            #[cfg(test)]
            cold_scan_hook: Mutex::new(None),
        })
    }

    /// A listing under a fixed key (unit tests of views).
    #[cfg(test)]
    pub fn for_test() -> Arc<Self> {
        Self::new(CookieKey::fixed(0x5eed, 0xcafe), DEFAULT_HINT_BYTES)
    }

    pub fn key(&self) -> &CookieKey {
        &self.key
    }

    pub fn cookie_bits(&self) -> CookieBits {
        self.key.bits()
    }

    pub fn pin_caps(&self) -> PinCaps {
        self.caps
    }

    pub fn stats(&self) -> ListingStats {
        ListingStats {
            hint_hits: self.hint_hits.load(Ordering::Relaxed),
            cold_scans: self.cold_scans.load(Ordering::Relaxed),
            cold_scan_entries: self.cold_scan_entries.load(Ordering::Relaxed),
            lost_cookies: self.lost_cookies.load(Ordering::Relaxed),
            cookie_collisions: self.cookie_collisions.load(Ordering::Relaxed),
            hint_bytes: self.hint_bytes.load(Ordering::Relaxed),
            open_streams: self.open_streams.load(Ordering::Relaxed),
            pinned_dirs: self.pinned_dirs.load(Ordering::Relaxed),
            pinned_bytes: self.pinned_bytes.load(Ordering::Relaxed),
            exempt_bytes: self.exempt_bytes.load(Ordering::Relaxed),
            pin_overflows: self.pin_overflows.load(Ordering::Relaxed),
        }
    }

    /// Forget every remembered page and every stream (tests).
    #[cfg(test)]
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            for d in shard.dirs.values() {
                self.apply(
                    d.acct(),
                    Acct {
                        ring: 0,
                        pinned: 0,
                        held: 0,
                        open: 0,
                    },
                );
            }
            *shard = Shard::default();
        }
    }

    /// Record a page of `dir` the way the page loop does (tests).
    #[cfg(test)]
    pub(crate) fn record_for_test(&self, dir: Ino, anchor: &[u8], cookies: &[u64]) {
        self.record(
            dir,
            Page {
                seq: 0,
                anchor: Some(anchor.into()),
                last: b"last".as_slice().into(),
                cookies: cookies.into(),
            },
            None,
        );
    }

    /// Record a page the way the page loop does, served through `via`
    /// (tests of the pin bounds, without a replica).
    #[cfg(test)]
    pub(crate) fn record_via_for_test(
        &self,
        dir: Ino,
        anchor: Option<&[u8]>,
        last: &[u8],
        cookies: &[u64],
        via: Option<StreamKey>,
    ) {
        self.record(
            dir,
            Page {
                seq: 0,
                anchor: anchor.map(Into::into),
                last: last.into(),
                cookies: cookies.into(),
            },
            via,
        );
    }

    /// The bytes the shards hold, summed (tests: the counters must agree):
    /// ordinary hints, pinned pages, held pages.
    #[cfg(test)]
    pub(crate) fn accounted_bytes_for_test(&self) -> u64 {
        self.accounted_for_test().0
    }

    #[cfg(test)]
    pub(crate) fn accounted_for_test(&self) -> (u64, u64, u64) {
        let (mut ring, mut pinned, mut held) = (0u64, 0u64, 0u64);
        for s in &self.shards {
            let s = s.lock().unwrap();
            let mut shard_ring = 0;
            for d in s.dirs.values() {
                let r: usize = d.ring.iter().map(|p| p.bytes()).sum();
                let p: usize = d.pinned.iter().map(|p| p.bytes()).sum();
                let h: usize = d.held.values().map(|p| p.bytes()).sum();
                assert_eq!(r, d.ring_bytes, "a directory's ring bytes are its pages'");
                assert_eq!(
                    p, d.pinned_bytes,
                    "a directory's pinned bytes are its pages'"
                );
                assert_eq!(h, d.held_bytes, "a directory's held bytes are its pages'");
                assert!(d.ring.len() <= PAGES);
                assert!(
                    d.pinned
                        .iter()
                        .zip(d.pinned.iter().skip(1))
                        .all(|(a, b)| a.seq < b.seq),
                    "the pin window is in sequence order"
                );
                if d.open.is_empty() {
                    assert!(
                        d.pinned.is_empty() && d.held.is_empty(),
                        "a directory with no stream open pins nothing"
                    );
                }
                shard_ring += r;
                ring += r as u64;
                pinned += p as u64;
                held += h as u64;
            }
            assert_eq!(shard_ring, s.ring_bytes, "a shard's bytes are its pages'");
        }
        (ring, pinned, held)
    }

    /// The pin window of `dir`: its pinned page count, the bytes of its
    /// window, and the held pages' bytes (tests).
    #[cfg(test)]
    pub(crate) fn window_for_test(&self, dir: Ino) -> (usize, usize, usize) {
        let s = self.shard(dir).lock().unwrap();
        s.dirs.get(&dir).map_or((0, 0, 0), |d| {
            (d.pinned.len(), d.pinned_bytes, d.held_bytes)
        })
    }

    fn shard(&self, dir: Ino) -> &Mutex<Shard> {
        // Inodes are dense counters: the low bits spread them.
        &self.shards[(dir as usize) % SHARDS]
    }

    /// Adjust the node's counters for a directory whose counts went from
    /// `before` to `after`, and the shard's ordinary-hint bytes.
    fn apply(&self, before: Acct, after: Acct) {
        fn delta(a: &AtomicU64, before: usize, after: usize) {
            if after >= before {
                a.fetch_add((after - before) as u64, Ordering::Relaxed);
            } else {
                a.fetch_sub((before - after) as u64, Ordering::Relaxed);
            }
        }
        delta(&self.hint_bytes, before.ring, after.ring);
        delta(&self.pinned_bytes, before.pinned, after.pinned);
        delta(&self.exempt_bytes, before.held, after.held);
        delta(&self.open_streams, before.open, after.open);
        match (before.open > 0, after.open > 0) {
            (false, true) => {
                self.pinned_dirs.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.pinned_dirs.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Run `f` on `dir`'s hints under its shard's lock (created if
    /// `create`; `None` if there are none), keeping the counters, the
    /// hint cap and the LRU. Pages `f` and the cap drop are freed after
    /// the lock is released. **`f` reads no replica**: no lock is held
    /// across one.
    fn with_dir<T>(&self, dir: Ino, create: bool, f: impl FnOnce(&mut DirHints) -> T) -> Option<T> {
        let (out, garbage) = {
            let mut shard = self.shard(dir).lock().unwrap();
            if create {
                shard.dirs.entry(dir).or_default();
            }
            let d = shard.dirs.get_mut(&dir)?;
            let before = d.acct();
            let out = f(d);
            let after = d.acct();
            let mut garbage = std::mem::take(&mut d.garbage);
            let (empty, tick) = (d.is_empty(), d.tick);
            shard.ring_bytes = shard.ring_bytes + after.ring - before.ring;
            if empty {
                shard.dirs.remove(&dir);
                shard.lru.remove(&tick);
            } else {
                shard.touch(dir);
            }
            self.apply(before, after);
            // Over the hint cap: the least recently used directories lose
            // their ordinary hints (a directory with a stream open keeps
            // its pin window).
            while shard.ring_bytes > self.shard_cap {
                let Some((_, victim)) = shard.lru.pop_first() else {
                    break;
                };
                let Some(v) = shard.dirs.get_mut(&victim) else {
                    continue;
                };
                let was = v.acct();
                garbage.extend(v.ring.drain(..));
                v.ring_bytes = 0;
                let now = v.acct();
                let gone = v.is_empty();
                shard.ring_bytes -= was.ring;
                if gone {
                    shard.dirs.remove(&victim);
                }
                self.apply(was, now);
            }
            (out, garbage)
        };
        drop(garbage);
        Some(out)
    }

    /// Where `cookie` resumes in `dir`, asked through the open stream
    /// `via` (if it is one).
    pub(crate) fn resolve(&self, dir: Ino, cookie: u64, via: Option<StreamKey>) -> Resume {
        if cookie <= 2 {
            return Resume::Start;
        }
        // The page last served through the stream first: it is where the
        // stream continues from (K5), whatever was served since to other
        // streams, and a newer page of another stream may hold a different
        // name with the same cookie (two names share one in a directory of
        // ~2^31 names at 62 bits, and of ~2^16 at 31: the newest page would
        // then place the resume after the wrong name, which can skip).
        // Otherwise the newest page that holds it knows the most. The
        // newest pinned pages and the ordinary hints are looked at under
        // the lock; the rest of a big pin window (a `seekdir` far back)
        // outside it, on pages held by `Arc`.
        let mut older: Vec<PageRef> = Vec::new();
        let mut hit: Option<(PageRef, usize)>;
        {
            let shard = self.shard(dir).lock().unwrap();
            let Some(d) = shard.dirs.get(&dir) else {
                return Resume::Cold;
            };
            hit = via
                .and_then(|key| d.open.get(&key)?.last)
                .and_then(|seq| d.page_at(seq))
                .and_then(|p| Some((p.clone(), p.locate(cookie)?)));
            if hit.is_none() {
                hit = d
                    .pinned
                    .iter()
                    .rev()
                    .take(RECENT)
                    .find_map(|p| Some((p.clone(), p.locate(cookie)?)));
            }
            if hit.is_none() {
                let ring = d
                    .ring
                    .iter()
                    .rev()
                    .find_map(|p| Some((p.clone(), p.locate(cookie)?)));
                let held = d
                    .held
                    .values()
                    .rev()
                    .find_map(|p| Some((p.clone(), p.locate(cookie)?)));
                hit = match (ring, held) {
                    (Some(a), Some(b)) => Some(if a.0.seq >= b.0.seq { a } else { b }),
                    (a, b) => a.or(b),
                };
                if d.pinned.len() > RECENT {
                    older = d
                        .pinned
                        .iter()
                        .take(d.pinned.len() - RECENT)
                        .cloned()
                        .collect();
                }
            }
        }
        if let Some(found) = older
            .iter()
            .rev()
            .find_map(|p| Some((p.clone(), p.locate(cookie)?)))
        {
            hit = Some(found);
        }
        match hit {
            Some((page, i)) => {
                self.with_dir(dir, false, |_| ());
                self.hint_hits.fetch_add(1, Ordering::Relaxed);
                Seek::of(&page, i)
            }
            None => Resume::Cold,
        }
    }

    /// Remember a served page of `dir`; when it was served through the
    /// open stream `via`, it is that stream's last page. Under the shard
    /// lock only; the caller read the replica before.
    fn record(&self, dir: Ino, mut page: Page, via: Option<StreamKey>) {
        let mut over = Overflow::default();
        self.with_dir(dir, true, |d| {
            page.seq = d.seq_next;
            d.seq_next += 1;
            let page = Arc::new(page);
            if d.open.is_empty() {
                d.ring_insert(page);
                return;
            }
            let seq = page.seq;
            d.pinned_push(page);
            if let Some(key) = via {
                d.set_last(key, seq);
            }
            over = d.enforce(self.caps.per_dir);
        });
        self.overflowed(dir, over);
        self.enforce_node_cap();
    }

    fn overflowed(&self, dir: Ino, over: Overflow) {
        if over.unpinned {
            self.pin_overflows.fetch_add(1, Ordering::Relaxed);
        }
        if over.first {
            tracing::warn!(
                dir,
                "a directory's listing pin cap was exceeded: the oldest pinned pages are now \
                 ordinary hints (a seekdir back into them can meet a lost cookie)"
            );
        }
    }

    /// The node's pin window bytes are over the cap: the directory with
    /// the most pinned bytes gives up its oldest pages, to a little under
    /// the cap (so that the next page does not scan again).
    fn enforce_node_cap(&self) {
        let cap = self.caps.per_node as u64;
        let pinned = |l: &Self| l.pinned_bytes.load(Ordering::Relaxed);
        while pinned(self) > cap {
            let mut biggest: Option<(Ino, usize)> = None;
            for shard in &self.shards {
                let shard = shard.lock().unwrap();
                for (ino, d) in &shard.dirs {
                    if d.pinned_bytes > biggest.map_or(0, |(_, b)| b) {
                        biggest = Some((*ino, d.pinned_bytes));
                    }
                }
            }
            let Some((dir, _)) = biggest else {
                break;
            };
            let target = cap - cap / 16;
            let mut over = Overflow::default();
            let freed = self.with_dir(dir, false, |d| {
                let mut freed = 0;
                // Per directory the others' bytes are not known here:
                // free what this one holds above the target, or all.
                let excess = (pinned(self).saturating_sub(target)) as usize;
                while freed < excess {
                    let Some(n) = d.unpin_oldest() else { break };
                    freed += n;
                    over.unpinned = true;
                }
                over = d.overflowed(over);
                freed
            });
            self.overflowed(dir, over);
            if freed.unwrap_or(0) == 0 {
                break;
            }
        }
    }

    /// A stream `key` was opened on `dir`: pages served from now on are
    /// pinned while it is open.
    pub(crate) fn open_stream(&self, dir: Ino, key: StreamKey) {
        self.with_dir(dir, true, |d| {
            let start = d.seq_next;
            d.open_stream(key, start, None);
        });
    }

    /// The stream `key` of `dir` was closed: what only it pinned is let
    /// go of (each page once).
    pub(crate) fn close_stream(&self, dir: Ino, key: StreamKey) {
        self.with_dir(dir, false, |d| d.close_stream(key));
    }

    /// The stream `key` of `dir` is rewound (`rewinddir`, `seekdir(0)`):
    /// its window starts at the next page served.
    pub(crate) fn rewind(&self, dir: Ino, key: StreamKey) {
        self.with_dir(dir, false, |d| {
            let next = d.seq_next;
            let Some(open) = d.open.get_mut(&key) else {
                return;
            };
            if next > open.start {
                d.by_start.remove(&(open.start, key));
                open.start = next;
                d.by_start.insert((next, key));
                d.advance_window();
            }
        });
    }
}

/// Directory stream ids (plan 40 §4.9) are a number space of their own,
/// disjoint from the file handles of plan 39 (which count up from 1, far
/// below this bit) and from inode numbers (`SYNTHETIC_INO_BIT` is bit 63):
/// `fsyncdir`, which carries one, finds no write session and no discard
/// error event behind it, as it did with `Fh(0)`.
pub(crate) const DIR_FH_BIT: u64 = 1 << 62;

/// Owners of streams: one per view, so that two views' stream ids (each
/// counts from 1) never meet in a directory's hints.
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

/// The directory streams a view handed out.
pub(super) struct DirStreams {
    owner: u64,
    table: Mutex<StreamTable>,
}

#[derive(Default)]
struct StreamTable {
    /// The next stream's number (ids are `DIR_FH_BIT | n`, n from 1).
    next: u64,
    /// Open streams on directories the listing pins (not synthetic ones).
    live: HashMap<u64, Ino>,
}

impl DirStreams {
    pub(super) fn new() -> Self {
        Self {
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            table: Mutex::new(StreamTable::default()),
        }
    }
}

/// A view's open directory streams and what they pin, as a handover
/// carries them (plan 40 §4.9): the streams, each pinned directory's page
/// sequence and its pinned pages. Unpinned hints do not cross.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingSnapshot {
    /// The next stream number.
    pub next: u64,
    pub streams: Vec<StreamSnapshot>,
    pub dirs: Vec<DirSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshot {
    pub id: u64,
    pub dir: Ino,
    pub start: u64,
    pub last: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirSnapshot {
    pub dir: Ino,
    pub seq_next: u64,
    /// The pinned and held pages, oldest first.
    pub pages: Vec<PageSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageSnapshot {
    pub seq: u64,
    pub anchor: Option<Vec<u8>>,
    pub last: Vec<u8>,
    /// The page's cookies, little-endian, eight bytes each; base64 in the
    /// handoff's JSON (an array of numbers would be four times larger).
    #[serde(with = "b64")]
    pub cookies: Vec<u8>,
}

/// Standard base64, no padding: serde's string form of a byte run.
mod b64 {
    use serde::{Deserialize, Deserializer, Serializer};

    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    const REVERSE: [u8; 256] = {
        let mut table = [255u8; 256];
        let mut i = 0;
        while i < 64 {
            table[ALPHABET[i] as usize] = i as u8;
            i += 1;
        }
        table
    };

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let n = chunk
                .iter()
                .enumerate()
                .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
            for i in 0..=chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            }
        }
        s.serialize_str(&out)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        use serde::de::Error;
        let text = String::deserialize(d)?;
        let mut out = Vec::with_capacity(text.len() / 4 * 3 + 2);
        for chunk in text.as_bytes().chunks(4) {
            if chunk.len() == 1 {
                return Err(D::Error::custom("a truncated base64 run"));
            }
            let mut n = 0u32;
            for (i, c) in chunk.iter().enumerate() {
                let v = REVERSE[*c as usize];
                if v == 255 {
                    return Err(D::Error::custom("not base64"));
                }
                n |= u32::from(v) << (18 - 6 * i);
            }
            for i in 0..chunk.len() - 1 {
                out.push((n >> (16 - 8 * i)) as u8);
            }
        }
        Ok(out)
    }
}

impl PageSnapshot {
    fn of(page: &Page) -> Self {
        Self {
            seq: page.seq,
            anchor: page.anchor.as_ref().map(|a| a.to_vec()),
            last: page.last.to_vec(),
            cookies: page.cookies.iter().flat_map(|c| c.to_le_bytes()).collect(),
        }
    }

    fn into_page(self) -> Page {
        Page {
            seq: self.seq,
            anchor: self.anchor.map(Vec::into_boxed_slice),
            last: self.last.into_boxed_slice(),
            cookies: self
                .cookies
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| u64::from_le_bytes(*b))
                .collect(),
        }
    }
}

/// A stream a handover carried: its key, start and last page.
type StreamState = (StreamKey, u64, Option<u64>);

/// What a handover carried for one directory: its page sequence, the
/// streams on it and its pinned pages.
type DirImport = (u64, Vec<StreamState>, Vec<PageRef>);

impl DirHints {
    /// Adopt streams and pages a handover carried: pages already here
    /// (the same directory's, from another view's export, or this
    /// process's own) are not duplicated.
    fn import(&mut self, seq_next: u64, streams: Vec<StreamState>, pages: Vec<PageRef>) {
        self.seq_next = self.seq_next.max(seq_next);
        for (key, start, last) in streams {
            self.open_stream(key, start, last);
        }
        let mut all: BTreeMap<u64, PageRef> = BTreeMap::new();
        for p in std::mem::take(&mut self.pinned)
            .into_iter()
            .chain(std::mem::take(&mut self.held).into_values())
            .chain(pages)
        {
            all.entry(p.seq).or_insert(p);
        }
        self.pinned_bytes = 0;
        self.held_bytes = 0;
        let start = self.by_start.first().map(|(s, _)| *s);
        for (seq, p) in all {
            if start.is_some_and(|s| seq >= s) {
                self.pinned_push(p);
            } else if self.lasts.contains_key(&seq) {
                self.hold(p);
            } else {
                self.ring_insert(p);
            }
        }
    }
}

impl Listing {
    /// What the streams `live` (ids and directories) of the view `owner`
    /// pin, for a handover.
    fn export_streams(&self, owner: u64, live: &[(u64, Ino)], next: u64) -> ListingSnapshot {
        let mut out = ListingSnapshot {
            next,
            ..Default::default()
        };
        let mut dirs: BTreeMap<Ino, (u64, Vec<PageRef>)> = BTreeMap::new();
        for (id, dir) in live {
            let shard = self.shard(*dir).lock().unwrap();
            let Some(d) = shard.dirs.get(dir) else {
                continue;
            };
            let Some(open) = d.open.get(&(owner, *id)) else {
                continue;
            };
            out.streams.push(StreamSnapshot {
                id: *id,
                dir: *dir,
                start: open.start,
                last: open.last,
            });
            dirs.entry(*dir).or_insert_with(|| {
                let mut pages: Vec<PageRef> = d
                    .pinned
                    .iter()
                    .cloned()
                    .chain(d.held.values().cloned())
                    .collect();
                pages.sort_by_key(|p| p.seq);
                (d.seq_next, pages)
            });
        }
        out.streams.sort_by_key(|s| s.id);
        out.dirs = dirs
            .into_iter()
            .map(|(dir, (seq_next, pages))| DirSnapshot {
                dir,
                seq_next,
                pages: pages.iter().map(|p| PageSnapshot::of(p)).collect(),
            })
            .collect();
        out
    }

    /// Adopt what a handover carried for the view `owner`.
    fn import_streams(&self, owner: u64, snap: &ListingSnapshot) {
        let mut dirs: BTreeMap<Ino, DirImport> = BTreeMap::new();
        for d in &snap.dirs {
            let e = dirs.entry(d.dir).or_default();
            e.0 = e.0.max(d.seq_next);
            e.2.extend(d.pages.iter().cloned().map(|p| Arc::new(p.into_page())));
        }
        for s in &snap.streams {
            dirs.entry(s.dir)
                .or_default()
                .1
                .push(((owner, s.id), s.start, s.last));
        }
        for (dir, (seq_next, streams, pages)) in dirs {
            let mut over = Overflow::default();
            self.with_dir(dir, true, |d| {
                d.import(seq_next, streams, pages);
                over = d.enforce(self.caps.per_dir);
            });
            self.overflowed(dir, over);
        }
        self.enforce_node_cap();
    }
}

impl View {
    /// `OPENDIR`: a stream id for the directory. No I/O, no wait: it only
    /// registers the stream, so that the pages served on it are pinned
    /// until [`Self::releasedir_stream`].
    pub(super) fn opendir_stream(&self, ino: Ino) -> Fh {
        let dir = self.real_ino(ino);
        let mut t = self.dir_streams.table.lock().unwrap();
        t.next += 1;
        let id = DIR_FH_BIT | t.next;
        if View::is_synthetic(dir) {
            // Listed positionally from an immutable listing: nothing to pin.
            return Fh(id);
        }
        t.live.insert(id, dir);
        drop(t);
        self.listing.open_stream(dir, (self.dir_streams.owner, id));
        Fh(id)
    }

    /// `RELEASEDIR`: the stream is closed. Never blocks: the shard lock
    /// of the directory's hints, nothing else.
    pub(super) fn releasedir_stream(&self, fh: Fh) {
        if fh.0 & DIR_FH_BIT == 0 {
            return;
        }
        let dir = self.dir_streams.table.lock().unwrap().live.remove(&fh.0);
        if let Some(dir) = dir {
            self.listing
                .close_stream(dir, (self.dir_streams.owner, fh.0));
        }
    }

    /// The stream a `readdir` came through, if `fh` is one.
    pub(super) fn stream_key(&self, fh: Fh) -> Option<StreamKey> {
        (fh.0 & DIR_FH_BIT != 0).then_some((self.dir_streams.owner, fh.0))
    }

    /// This view's streams and what they pin (a handover).
    pub(super) fn export_listing(&self) -> ListingSnapshot {
        let t = self.dir_streams.table.lock().unwrap();
        let live: Vec<(u64, Ino)> = t.live.iter().map(|(id, dir)| (*id, *dir)).collect();
        let next = t.next;
        drop(t);
        self.listing
            .export_streams(self.dir_streams.owner, &live, next)
    }

    /// Adopt a handover's streams: the kernel's open directories keep
    /// their ids and what they pinned.
    pub(super) fn import_listing(&self, snap: &ListingSnapshot) {
        {
            let mut t = self.dir_streams.table.lock().unwrap();
            t.next = t.next.max(snap.next);
            for s in &snap.streams {
                t.live.insert(s.id, s.dir);
            }
        }
        self.listing.import_streams(self.dir_streams.owner, snap);
    }
}

impl View {
    /// Every stream of this view is closed: its frontend is gone (no
    /// `RELEASEDIR` will come for them, `Engine::close_view`) or the view
    /// itself is. What they pinned is let go of. A later `RELEASEDIR` or
    /// page on one of their ids finds no stream (nothing to pin, nothing
    /// to free).
    pub(crate) fn release_dir_streams(&self) {
        let live = std::mem::take(
            &mut self
                .dir_streams
                .table
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .live,
        );
        for (id, dir) in live {
            self.listing.close_stream(dir, (self.dir_streams.owner, id));
        }
    }
}

impl Drop for View {
    /// A view that goes away lets go of what its streams pinned.
    fn drop(&mut self) {
        self.release_dir_streams();
    }
}

/// Why a page failed (before it added anything).
pub(super) enum PageError {
    /// The replica failed; the code answers for `ino`.
    Meta(Code),
    /// A cold cookie whose name is gone (not an `ino_err`: the directory
    /// is there, the position is not).
    LostCookie,
}

/// A stored name as the wire carries it.
fn wire_name(stored: &[u8]) -> Cow<'_, [u8]> {
    match std::str::from_utf8(stored) {
        Ok(s) => constellation_vfs::name::wire_bytes(s),
        Err(_) => Cow::Borrowed(stored),
    }
}

impl View {
    /// One page of directory `ino` (not synthetic) from `cookie` into `r`;
    /// under `plus`, each entry is what a lookup of its name answers (plan
    /// 40 §4.5, [`super::plus`]): one session wait per batch read, for the
    /// names of the batch the page may still add, then the entry of each
    /// name as it is added.
    ///
    /// Errors only before the first `add`: the first batch is read before
    /// `.` or `..` are added, and a replica failure after that ends the
    /// page short (the kernel asks again from the last cookie and meets
    /// the failure on an empty page).
    pub(super) fn list_page<R: DirSink>(
        &self,
        ino: Ino,
        visible_ino: Ino,
        fh: Fh,
        cookie: u64,
        plus: bool,
        r: &mut R,
    ) -> Result<(), PageError> {
        let listing = &*self.listing;
        // A rewind of an open stream moves its pin window to the pages
        // served from now on.
        let via = self.stream_key(fh);
        if let (0, Some(key)) = (cookie, via) {
            listing.rewind(ino, key);
        }
        let key = listing.key();
        let scratch = self.meta.is_scratch_dir(ino).unwrap_or(false)
            || self.meta.scratch_getattr(ino).ok().flatten().is_some();
        let read = |after: Option<&[u8]>| -> Result<Vec<RawDirEntry>, MetaError> {
            if scratch {
                self.meta.scratch_readdir_after(ino, after, BATCH)
            } else {
                self.meta.readdir_after(ino, after, BATCH)
            }
        };
        let meta_err = |e: MetaError| PageError::Meta(e.code());

        // Where the page starts.
        let mut cold = false;
        let (after, mut seek) = match listing.resolve(ino, cookie, via) {
            Resume::Start => (None, None),
            Resume::Hint { after, seek } => (after, seek),
            Resume::Cold => {
                listing.cold_scans.fetch_add(1, Ordering::Relaxed);
                let mut scanned = 0u64;
                let mut from: Option<Vec<u8>> = None;
                let found = 'scan: loop {
                    let batch = read(from.as_deref()).map_err(meta_err)?;
                    #[cfg(test)]
                    {
                        let hook = listing.cold_scan_hook.lock().unwrap().clone();
                        if let Some(hook) = hook {
                            hook();
                        }
                    }
                    for e in &batch {
                        scanned += 1;
                        if key.cookie(&e.name) == cookie {
                            break 'scan Some(e.name.clone());
                        }
                    }
                    if batch.len() < BATCH {
                        break None;
                    }
                    from = batch.into_iter().next_back().map(|e| e.name);
                };
                listing
                    .cold_scan_entries
                    .fetch_add(scanned, Ordering::Relaxed);
                let Some(name) = found else {
                    listing.lost_cookies.fetch_add(1, Ordering::Relaxed);
                    return Err(PageError::LostCookie);
                };
                cold = true;
                (Some(name), None)
            }
        };

        // The first batch, before anything is added: a removed or
        // non-directory `ino` fails here, not after `.`.
        let mut batch = read(after.as_deref()).map_err(meta_err)?;
        // A resume reads no directory record, so an empty tail is either
        // the end of the listing or a directory that is gone (an empty one,
        // as only an empty directory is removed): ask the replica, which
        // fails with `NotFound`/`NotDir` for the first page of one.
        if batch.is_empty() && after.is_some() && !scratch {
            self.meta.readdir_after(ino, None, 1).map_err(meta_err)?;
        }
        if cookie == 0 && r.add(visible_ino, 1, FileKind::Dir, b".") {
            return Ok(());
        }
        if cookie <= 1 && r.add(visible_ino, 2, FileKind::Dir, b"..") {
            return Ok(());
        }

        let mut page = plus.then(|| self.plus_page(ino, scratch));
        let mut anchor = after;
        let mut emitted: Vec<u64> = Vec::with_capacity(BATCH * 3);
        let mut in_page = Cookies::with_capacity_and_hasher(BATCH * 3, Default::default());
        let mut last: Option<Vec<u8>> = None;
        // Per name of the batch: whether the platform holds its entry.
        let mut held: Vec<bool> = Vec::new();
        loop {
            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }
            // The last entry of this batch that went into the page, and
            // whether the page is over.
            let mut added = None;
            let mut over = false;
            held.clear();
            if let Some(page) = &page {
                // The names the platform holds already get no entry
                // (`plus` step 0), so they need no wait either.
                held.extend(batch.iter().map(|e| r.holds(e.ino, &wire_name(&e.name))));
                // What a lookup of each other name of the batch waits for
                // (`Dentry(dir, name)` and `Ino(child)`). Stored names are
                // UTF-8 (escaped, `constellation_vfs::name`); one that is
                // not gets no entry, so it needs no wait.
                page.wait(
                    batch
                        .iter()
                        .zip(&held)
                        .filter(|(_, held)| !**held)
                        .filter_map(|(e, _)| Some((e.ino, std::str::from_utf8(&e.name).ok()?))),
                );
            }
            for (i, e) in batch.iter().enumerate() {
                let c = key.cookie(&e.name);
                if let Some(s) = &seek {
                    // Before the place (`Seek`): passed, and the page
                    // starts after it. Only before the first entry added.
                    if e.name.as_slice() <= s.last.as_slice() && !s.ahead.contains(&c) {
                        if c == s.cookie {
                            seek = None;
                        }
                        anchor = Some(e.name.clone());
                        continue;
                    }
                    seek = None;
                }
                if !in_page.insert(c) {
                    // Two names of one cookie in one page: it ends here,
                    // and the next page starts at this one.
                    listing.cookie_collisions.fetch_add(1, Ordering::Relaxed);
                    over = true;
                    break;
                }
                if cold && c == cookie {
                    listing.cookie_collisions.fetch_add(1, Ordering::Relaxed);
                }
                let kind = kind_out(e.kind);
                let name = wire_name(&e.name);
                let full = match page.as_mut() {
                    // An entry the platform already holds is not made again.
                    Some(_) if held[i] => r.add_held(e.ino, c, kind, &name),
                    Some(page) => {
                        let entry = page.entry(e.ino, &e.name);
                        r.add_entry(e.ino, c, kind, &name, entry.as_ref())
                    }
                    None => r.add(e.ino, c, kind, &name),
                };
                if full {
                    over = true;
                    break;
                }
                emitted.push(c);
                added = Some(i);
            }
            if let Some(i) = added {
                last = Some(batch[i].name.clone());
            }
            if over || batch_len < BATCH {
                break;
            }
            // A failure now is a short page, not an error: entries are
            // added.
            match read(Some(&batch[batch_len - 1].name)) {
                Ok(next) => batch = next,
                Err(_) => break,
            }
        }
        if let Some(page) = page {
            page.finish();
        }
        if let Some(last) = last {
            listing.record(
                ino,
                Page {
                    seq: 0,
                    anchor: anchor.map(Vec::into_boxed_slice),
                    last: last.into_boxed_slice(),
                    cookies: emitted.into_boxed_slice(),
                },
                via,
            );
        }
        Ok(())
    }
}
