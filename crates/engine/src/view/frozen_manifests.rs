//! A view's cache of frozen (snapshot) file manifests (plan 38 Z3c).
//!
//! A snapshot file's manifest is read from the snapshot's retained tree
//! (`SnapshotManager::load_manifest`), not from the replica's tables, and
//! it is immutable: a [`FrozenObject`] names one inode of one retained
//! tree root, whose manifest can never change. So the cache needs no
//! invalidation at all — only a bound.
//!
//! Without it, every `read` of a frozen file loaded its manifest again
//! (`read_frozen`), and the passthrough check at `open` (plan 38 Z3c's
//! `frozen_passthrough_backing`) would load it once more before the first
//! read: two `block_on` loads on a FUSE worker per small-file open+read.
//! With it, the open loads it once and every read after that — and every
//! later open of the same file — finds it here.
//!
//! The bound is a count of entries (`CONSTELLATION_SNAPSHOT_MANIFEST_CACHE`,
//! default [`DEFAULT_ENTRIES`], `0` turns the cache off), evicting the least
//! recently used. A count is enough because what an entry can hold is
//! itself bounded: a manifest keeps at most `INLINE_CHUNKS_MAX` chunk
//! hashes inline and spills a longer list to a blob, which is not cached
//! here (`chunk_list` fetches it), so an entry is a few hundred bytes and
//! the default costs a few MiB at most.

use crate::snapshot::FrozenObject;
use constellation_fs_core::manifest::Manifest;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// Entries kept when `CONSTELLATION_SNAPSHOT_MANIFEST_CACHE` is unset: the
/// small-file working set the read-cpu gate's `smallfiles` lane reads
/// (4096 files), twice over.
pub(super) const DEFAULT_ENTRIES: usize = 8192;

/// `CONSTELLATION_SNAPSHOT_MANIFEST_CACHE`, as an entry count; an unset
/// or unparsable value is the default (logged when unparsable).
pub(super) fn entries_from_env() -> usize {
    parse_entries(
        std::env::var("CONSTELLATION_SNAPSHOT_MANIFEST_CACHE")
            .ok()
            .as_deref(),
    )
}

fn parse_entries(raw: Option<&str>) -> usize {
    match raw.map(str::trim) {
        None | Some("") => DEFAULT_ENTRIES,
        Some(s) => s.parse().unwrap_or_else(|_| {
            tracing::warn!(
                value = s,
                default = DEFAULT_ENTRIES,
                "CONSTELLATION_SNAPSHOT_MANIFEST_CACHE is not an entry count; using the default"
            );
            DEFAULT_ENTRIES
        }),
    }
}

/// See the module doc.
pub(super) struct FrozenManifests {
    cap: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Each entry with the tick of its last use.
    map: HashMap<FrozenObject, (Arc<Manifest>, u64)>,
    /// Last-use tick → entry: the first key is the least recently used.
    recency: BTreeMap<u64, FrozenObject>,
    tick: u64,
    hits: u64,
    misses: u64,
}

impl Inner {
    fn touch(&mut self, key: FrozenObject, old: Option<u64>) -> u64 {
        if let Some(old) = old {
            self.recency.remove(&old);
        }
        self.tick += 1;
        self.recency.insert(self.tick, key);
        self.tick
    }
}

impl FrozenManifests {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            cap,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// `key`'s manifest if it is cached (made the most recently used),
    /// counting the hit or the miss.
    pub(super) fn get(&self, key: &FrozenObject) -> Option<Arc<Manifest>> {
        let mut inner = self.inner.lock().unwrap();
        let Some((manifest, tick)) = inner.map.get(key).map(|(m, t)| (Arc::clone(m), *t)) else {
            inner.misses += 1;
            return None;
        };
        inner.hits += 1;
        let tick = inner.touch(*key, Some(tick));
        if let Some(slot) = inner.map.get_mut(key) {
            slot.1 = tick;
        }
        Some(manifest)
    }

    /// Keep `manifest` as `key`'s, evicting the least recently used entry
    /// past the bound. Two loads of one key racing both insert the same
    /// (immutable) manifest; the second simply replaces the first.
    pub(super) fn insert(&self, key: FrozenObject, manifest: Arc<Manifest>) {
        if self.cap == 0 {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        let old = inner.map.get(&key).map(|(_, t)| *t);
        let tick = inner.touch(key, old);
        inner.map.insert(key, (manifest, tick));
        while inner.map.len() > self.cap {
            let Some((_, oldest)) = inner.recency.pop_first() else {
                break;
            };
            inner.map.remove(&oldest);
        }
    }

    /// `(entries, hits, misses)`.
    #[cfg(test)]
    pub(super) fn stats(&self) -> (usize, u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.map.len(), inner.hits, inner.misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_mtree::NodeHash;

    fn key(ino: u64) -> FrozenObject {
        FrozenObject {
            root: NodeHash([7; 32]),
            ino,
        }
    }

    fn manifest(len: u64) -> Arc<Manifest> {
        let mut m = Manifest::empty(1 << 20);
        m.file_len = len;
        Arc::new(m)
    }

    #[test]
    fn a_miss_then_a_hit() {
        let c = FrozenManifests::new(4);
        assert!(c.get(&key(1)).is_none());
        c.insert(key(1), manifest(10));
        assert_eq!(c.get(&key(1)).unwrap().file_len, 10);
        assert_eq!(c.stats(), (1, 1, 1));
        // Another root's inode of the same number is another file.
        let other = FrozenObject {
            root: NodeHash([8; 32]),
            ino: 1,
        };
        assert!(c.get(&other).is_none());
    }

    #[test]
    fn the_bound_evicts_the_least_recently_used() {
        let c = FrozenManifests::new(2);
        c.insert(key(1), manifest(1));
        c.insert(key(2), manifest(2));
        // 1 is used, so 2 is now the oldest.
        assert!(c.get(&key(1)).is_some());
        c.insert(key(3), manifest(3));
        assert_eq!(c.stats().0, 2);
        assert!(c.get(&key(2)).is_none(), "the least recently used went");
        assert!(c.get(&key(1)).is_some());
        assert!(c.get(&key(3)).is_some());
        // Re-inserting a present key does not grow it.
        c.insert(key(3), manifest(3));
        assert_eq!(c.stats().0, 2);
        assert!(c.get(&key(1)).is_some());
    }

    #[test]
    fn a_zero_bound_keeps_nothing() {
        let c = FrozenManifests::new(0);
        c.insert(key(1), manifest(1));
        assert!(c.get(&key(1)).is_none());
        assert_eq!(c.stats(), (0, 0, 1));
    }

    #[test]
    fn the_bound_parses_from_the_environment_value() {
        assert_eq!(parse_entries(None), DEFAULT_ENTRIES);
        assert_eq!(parse_entries(Some("")), DEFAULT_ENTRIES);
        assert_eq!(parse_entries(Some(" 16 ")), 16);
        assert_eq!(parse_entries(Some("0")), 0);
        assert_eq!(parse_entries(Some("lots")), DEFAULT_ENTRIES);
    }
}
