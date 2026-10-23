//! Directory pages (plan 40 §4.9): name-ordered range scans resumed from
//! keyed cookies.
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
//!   and otherwise by scanning from the page's anchor and skipping the
//!   entries whose cookies the page held up to it. That is exact even if
//!   the entry the cookie names, or the anchor, has since been removed (a
//!   key seek past a name does not need the name to exist), and covers the
//!   kernel's re-asks from any cookie of a reply, `seekdir` back within the
//!   last pages, and another handle (`nfsd`).
//! - **Cold**: a cookie no page holds costs one O(n) scan of the
//!   directory's names, hashing them, until one hashes to `c`: the page
//!   resumes after it. A collision (two names of one cookie) resumes after
//!   the first in name order: entries between them may be listed twice,
//!   never skipped. A cookie whose name is gone answers `NotFound`.
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
use constellation_vfs::DirSink;
use cookie::{CookieBits, CookieKey};
use std::borrow::Cow;
use std::collections::BTreeMap;
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

/// One served page: enough to place any of its cookies.
struct Page {
    /// The name before the page's first entry (`None`: the directory's
    /// start). A scan resumes strictly after it.
    anchor: Option<Box<[u8]>>,
    /// The name of the page's last entry.
    last: Box<[u8]>,
    /// The cookies of its entries, in order; all distinct.
    cookies: Box<[u64]>,
}

impl Page {
    fn bytes(&self) -> usize {
        8 * self.cookies.len() + self.anchor.as_ref().map_or(0, |a| a.len()) + self.last.len() + 64
    }
}

#[derive(Default)]
struct DirHints {
    /// Oldest first.
    pages: VecDeque<Page>,
    /// The shard tick of the last use (the LRU key).
    tick: u64,
}

#[derive(Default)]
struct Shard {
    dirs: HashMap<Ino, DirHints>,
    /// Directories by last use, least recent first.
    lru: BTreeMap<u64, Ino>,
    tick: u64,
    bytes: usize,
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
    /// A remembered page holds it: scan strictly after `after`, skipping
    /// the entries whose cookies are in `skip` (each at most once).
    Hint {
        after: Option<Vec<u8>>,
        skip: Cookies,
    },
    /// No page holds it.
    Cold,
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
    /// Bytes the remembered pages hold.
    pub hint_bytes: u64,
}

/// The node's listing state: the cookie key and the hint cache.
pub struct Listing {
    key: CookieKey,
    shards: Vec<Mutex<Shard>>,
    shard_cap: usize,
    hint_hits: AtomicU64,
    cold_scans: AtomicU64,
    cold_scan_entries: AtomicU64,
    lost_cookies: AtomicU64,
    cookie_collisions: AtomicU64,
    hint_bytes: AtomicU64,
}

impl Listing {
    /// A listing with `key`, remembering at most `hint_bytes` bytes of
    /// pages (split evenly over the shards).
    pub fn new(key: CookieKey, hint_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            key,
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            shard_cap: hint_bytes / SHARDS,
            hint_hits: AtomicU64::new(0),
            cold_scans: AtomicU64::new(0),
            cold_scan_entries: AtomicU64::new(0),
            lost_cookies: AtomicU64::new(0),
            cookie_collisions: AtomicU64::new(0),
            hint_bytes: AtomicU64::new(0),
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

    pub fn stats(&self) -> ListingStats {
        ListingStats {
            hint_hits: self.hint_hits.load(Ordering::Relaxed),
            cold_scans: self.cold_scans.load(Ordering::Relaxed),
            cold_scan_entries: self.cold_scan_entries.load(Ordering::Relaxed),
            lost_cookies: self.lost_cookies.load(Ordering::Relaxed),
            cookie_collisions: self.cookie_collisions.load(Ordering::Relaxed),
            hint_bytes: self.hint_bytes.load(Ordering::Relaxed),
        }
    }

    /// Forget every remembered page.
    #[cfg(test)]
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            self.hint_bytes
                .fetch_sub(shard.bytes as u64, Ordering::Relaxed);
            *shard = Shard::default();
        }
    }

    /// Record a page of `dir` the way the page loop does (tests).
    #[cfg(test)]
    pub(crate) fn record_for_test(&self, dir: Ino, anchor: &[u8], cookies: &[u64]) {
        self.record(
            dir,
            Page {
                anchor: Some(anchor.into()),
                last: b"last".as_slice().into(),
                cookies: cookies.into(),
            },
        );
    }

    /// The bytes the shards hold, summed (tests: the counter must agree).
    #[cfg(test)]
    pub(crate) fn accounted_bytes_for_test(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| {
                let s = s.lock().unwrap();
                let real: usize = s
                    .dirs
                    .values()
                    .flat_map(|d| &d.pages)
                    .map(Page::bytes)
                    .sum();
                assert_eq!(real, s.bytes, "a shard's bytes are its pages'");
                real as u64
            })
            .sum()
    }

    fn shard(&self, dir: Ino) -> &Mutex<Shard> {
        // Inodes are dense counters: the low bits spread them.
        &self.shards[(dir as usize) % SHARDS]
    }

    /// Where `cookie` resumes in `dir`.
    pub(crate) fn resolve(&self, dir: Ino, cookie: u64) -> Resume {
        if cookie <= 2 {
            return Resume::Start;
        }
        let mut shard = self.shard(dir).lock().unwrap();
        let Some(hints) = shard.dirs.get(&dir) else {
            return Resume::Cold;
        };
        // The newest page that holds it knows the most.
        let found = hints.pages.iter().rev().find_map(|page| {
            // The sequential case first: the page's own last cookie.
            let i = if page.cookies.last() == Some(&cookie) {
                page.cookies.len() - 1
            } else {
                page.cookies.iter().position(|c| *c == cookie)?
            };
            Some(if i + 1 == page.cookies.len() {
                // The page's end: resume after its last name.
                Resume::Hint {
                    after: Some(page.last.to_vec()),
                    skip: Cookies::default(),
                }
            } else {
                Resume::Hint {
                    after: page.anchor.as_ref().map(|a| a.to_vec()),
                    skip: page.cookies[..=i].iter().copied().collect(),
                }
            })
        });
        match found {
            Some(hit) => {
                shard.touch(dir);
                drop(shard);
                self.hint_hits.fetch_add(1, Ordering::Relaxed);
                hit
            }
            None => Resume::Cold,
        }
    }

    /// Remember a served page of `dir`. Under the shard lock only; the
    /// caller read the replica before.
    fn record(&self, dir: Ino, page: Page) {
        let added = page.bytes();
        let mut shard = self.shard(dir).lock().unwrap();
        let before = shard.bytes;
        let hints = shard.dirs.entry(dir).or_default();
        hints.pages.push_back(page);
        let mut dropped = 0;
        while hints.pages.len() > PAGES {
            dropped += hints.pages.pop_front().map_or(0, |p| p.bytes());
        }
        shard.bytes = shard.bytes + added - dropped;
        shard.touch(dir);
        // Over the cap: the least recently used directories go whole.
        while shard.bytes > self.shard_cap {
            let Some((_, victim)) = shard.lru.pop_first() else {
                break;
            };
            if let Some(gone) = shard.dirs.remove(&victim) {
                let freed: usize = gone.pages.iter().map(Page::bytes).sum();
                shard.bytes -= freed;
            }
        }
        let after = shard.bytes;
        drop(shard);
        if after >= before {
            self.hint_bytes
                .fetch_add((after - before) as u64, Ordering::Relaxed);
        } else {
            self.hint_bytes
                .fetch_sub((before - after) as u64, Ordering::Relaxed);
        }
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
        cookie: u64,
        plus: bool,
        r: &mut R,
    ) -> Result<(), PageError> {
        let listing = &*self.listing;
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
        let (after, mut skip) = match listing.resolve(ino, cookie) {
            Resume::Start => (None, Cookies::default()),
            Resume::Hint { after, skip } => (after, skip),
            Resume::Cold => {
                listing.cold_scans.fetch_add(1, Ordering::Relaxed);
                let mut scanned = 0u64;
                let mut from: Option<Vec<u8>> = None;
                let found = 'scan: loop {
                    let batch = read(from.as_deref()).map_err(meta_err)?;
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
                (Some(name), Cookies::default())
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
        loop {
            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }
            // The last entry of this batch that went into the page, and
            // whether the page is over.
            let mut added = None;
            let mut over = false;
            if let Some(page) = &page {
                // What a lookup of each name the page may add waits for
                // (`Dentry(dir, name)` and `Ino(child)`): a name already
                // consumed (`skip`) is not added. Stored names are UTF-8
                // (escaped, `constellation_vfs::name`); one that is not
                // gets no entry, so it needs no wait.
                page.wait(batch.iter().filter_map(|e| {
                    if !skip.is_empty() && skip.contains(&key.cookie(&e.name)) {
                        return None;
                    }
                    Some((e.ino, std::str::from_utf8(&e.name).ok()?))
                }));
            }
            for (i, e) in batch.iter().enumerate() {
                let c = key.cookie(&e.name);
                if !skip.is_empty() && skip.remove(&c) {
                    if emitted.is_empty() {
                        anchor = Some(e.name.clone());
                    }
                    continue;
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
                    anchor: anchor.map(Vec::into_boxed_slice),
                    last: last.into_boxed_slice(),
                    cookies: emitted.into_boxed_slice(),
                },
            );
        }
        Ok(())
    }
}
