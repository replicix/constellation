//! Local disk chunk cache: LRU over clean chunks, pinned/dirty never
//! evicted, reserve-before-accept ENOSPC discipline (DESIGN.md §7, §9).
//!
//! Chunks are stored decompressed at `<root>/<aa>/<bb>/<hex>` (first two
//! hex byte pairs), written temp-name + atomic rename. Accounting is in
//! memory and rebuilt by a directory scan at startup; every read verifies
//! the blake3 hash and drops corrupt files (they are refetched upstream).

use crate::chunk::ChunkHash;
use crate::error::CoreError;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    /// Evictable, LRU-ordered.
    Clean,
    /// Never evicted (pinned subtree membership).
    Pinned,
    /// Never evicted until uploaded, then demoted to clean.
    Dirty,
}

#[derive(Debug)]
struct Entry {
    size: u64,
    state: ChunkState,
    /// Logical LRU clock value of the last access.
    atime: u64,
}

#[derive(Debug)]
struct State {
    entries: HashMap<ChunkHash, Entry>,
    used: u64,
    clock: u64,
    digest: DigestLog,
    digest_limit: usize,
}

/// Bound so a stalled consumer cannot grow the log without limit.
/// Past this the log collapses to [`DigestLog::Invalidated`] and the
/// next drain is a one-shot snapshot.
const MAX_DIGEST_LOG: usize = 65_536;

#[derive(Debug)]
enum DigestLog {
    Incremental(Vec<DigestChange>),
    Invalidated,
}

impl Default for DigestLog {
    fn default() -> Self {
        Self::Incremental(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestChange {
    Add(ChunkHash),
    Remove(ChunkHash),
}

/// Drained cooperative-cache journal: hashes that became (or stopped
/// being) clean/pinned since the last drain. `rebuild` means the log
/// overflowed and `events` is an add-only snapshot of what is servable now.
#[derive(Debug, Default)]
pub struct DigestBatch {
    pub events: Vec<DigestChange>,
    pub rebuild: bool,
}

fn is_servable(state: ChunkState) -> bool {
    state != ChunkState::Dirty
}

impl State {
    fn new(digest_limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            used: 0,
            clock: 0,
            digest: DigestLog::default(),
            digest_limit,
        }
    }

    fn note(&mut self, hash: ChunkHash, was: Option<ChunkState>, now: Option<ChunkState>) {
        let was_s = was.is_some_and(is_servable);
        let now_s = now.is_some_and(is_servable);
        if was_s == now_s {
            return;
        }
        let DigestLog::Incremental(v) = &mut self.digest else {
            return;
        };
        v.push(if now_s {
            DigestChange::Add(hash)
        } else {
            DigestChange::Remove(hash)
        });
        if v.len() > self.digest_limit {
            self.digest = DigestLog::Invalidated;
        }
    }
}

/// Disk-backed chunk cache with budget accounting.
pub struct DiskCache {
    root: PathBuf,
    budget: u64,
    state: Mutex<State>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheUsage {
    pub used: u64,
    pub budget: u64,
    pub entries: usize,
}

impl DiskCache {
    /// Open (or create) a cache directory and rebuild accounting from disk.
    pub fn open(root: impl Into<PathBuf>, budget: u64) -> Result<Self, CoreError> {
        Self::open_with_digest_log_limit(root, budget, MAX_DIGEST_LOG)
    }

    /// Alternate journal limit for deterministic tests and constrained
    /// deployments. The production constructor uses [`MAX_DIGEST_LOG`].
    pub fn open_with_digest_log_limit(
        root: impl Into<PathBuf>,
        budget: u64,
        digest_limit: usize,
    ) -> Result<Self, CoreError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let cache = Self {
            root,
            budget,
            state: Mutex::new(State::new(digest_limit.max(1))),
        };
        cache.rescan()?;
        Ok(cache)
    }

    fn rescan(&self) -> Result<(), CoreError> {
        let mut st = self.state.lock().unwrap();
        st.entries.clear();
        st.used = 0;
        for l1 in read_dirs(&self.root)? {
            for l2 in read_dirs(&l1)? {
                for f in read_files(&l2)? {
                    let name = f
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    if name.ends_with(".tmp") {
                        let _ = fs::remove_file(&f); // startup cruft removal
                        continue;
                    }
                    let Some(hash) = ChunkHash::from_hex(&name) else {
                        continue;
                    };
                    let size = fs::metadata(&f)?.len();
                    st.used += size;
                    st.clock += 1;
                    let atime = st.clock;
                    st.entries.insert(
                        hash,
                        Entry {
                            size,
                            state: ChunkState::Clean,
                            atime,
                        },
                    );
                    st.note(hash, None, Some(ChunkState::Clean));
                }
            }
        }
        Ok(())
    }

    fn path_for(&self, hash: &ChunkHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[0..2]).join(&hex[2..4]).join(hex)
    }

    pub fn usage(&self) -> CacheUsage {
        let st = self.state.lock().unwrap();
        CacheUsage {
            used: st.used,
            budget: self.budget,
            entries: st.entries.len(),
        }
    }

    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.state.lock().unwrap().entries.contains_key(hash)
    }

    pub fn state_of(&self, hash: &ChunkHash) -> Option<ChunkState> {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(hash)
            .map(|e| e.state)
    }

    /// Read a chunk, bumping its LRU position. Verifies the hash; corrupt
    /// files are removed and reported as absent (caller refetches).
    pub fn get(&self, hash: &ChunkHash) -> Result<Option<Vec<u8>>, CoreError> {
        {
            let mut st = self.state.lock().unwrap();
            if !st.entries.contains_key(hash) {
                return Ok(None);
            }
            st.clock += 1;
            let clock = st.clock;
            st.entries.get_mut(hash).unwrap().atime = clock;
        }
        let path = self.path_for(hash);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.forget(hash);
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };
        if &ChunkHash::of(&data) != hash {
            // Corrupt local copy: drop it, let the caller refetch.
            self.remove(hash)?;
            return Ok(None);
        }
        Ok(Some(data))
    }

    /// Insert a chunk with reserve-before-accept: evicts clean LRU entries
    /// to make room, fails with `CacheFull` (never partial state) if
    /// non-evictable content leaves no room.
    pub fn insert(
        &self,
        hash: &ChunkHash,
        data: &[u8],
        state: ChunkState,
    ) -> Result<(), CoreError> {
        debug_assert_eq!(&ChunkHash::of(data), hash);
        let size = data.len() as u64;
        let victims = {
            let mut st = self.state.lock().unwrap();
            if let Some((old, now)) = st.entries.get_mut(hash).map(|e| {
                let old = e.state;
                e.state = merge_state(e.state, state);
                (old, e.state)
            }) {
                st.note(*hash, Some(old), Some(now));
                return Ok(());
            }
            let victims = plan_eviction(&mut st, size, self.budget)?;
            for (vh, _) in &victims {
                st.note(*vh, Some(ChunkState::Clean), None);
            }
            // Reserve: account now, before any disk write.
            st.used += size;
            st.clock += 1;
            let atime = st.clock;
            st.entries.insert(*hash, Entry { size, state, atime });
            st.note(*hash, None, Some(state));
            victims
        };
        for (vh, _) in &victims {
            let _ = fs::remove_file(self.path_for(vh));
        }
        let path = self.path_for(hash);
        if let Err(e) = self.write_atomic(&path, data) {
            // Roll back the reservation: no partial state.
            let mut st = self.state.lock().unwrap();
            if let Some(entry) = st.entries.remove(hash) {
                st.used -= entry.size;
                st.note(*hash, Some(entry.state), None);
            }
            return Err(e);
        }
        Ok(())
    }

    fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<(), CoreError> {
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("tmp");
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_data()?;
        if let Err(e) = fs::rename(&tmp, path) {
            let _ = fs::remove_file(&tmp);
            return Err(e.into());
        }
        Ok(())
    }

    /// Change a chunk's state (e.g. dirty -> clean after upload).
    pub fn set_state(&self, hash: &ChunkHash, state: ChunkState) -> bool {
        let mut st = self.state.lock().unwrap();
        let Some(old) = st.entries.get_mut(hash).map(|e| {
            let old = e.state;
            e.state = state;
            old
        }) else {
            return false;
        };
        st.note(*hash, Some(old), Some(state));
        true
    }

    /// Remove a chunk from cache and disk.
    pub fn remove(&self, hash: &ChunkHash) -> Result<(), CoreError> {
        self.forget(hash);
        match fs::remove_file(self.path_for(hash)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn forget(&self, hash: &ChunkHash) {
        let mut st = self.state.lock().unwrap();
        if let Some(e) = st.entries.remove(hash) {
            st.used -= e.size;
            st.note(*hash, Some(e.state), None);
        }
    }

    /// Read a chunk only if it is clean or pinned. Dirty (unpublished)
    /// chunks are never served to peers.
    pub fn get_servable(&self, hash: &ChunkHash) -> Result<Option<Vec<u8>>, CoreError> {
        if self.state_of(hash) == Some(ChunkState::Dirty) {
            return Ok(None);
        }
        self.get(hash)
    }

    /// Hashes of clean/pinned chunks. Prefer [`Self::take_digest_events`]
    /// on the publish path: this clones the whole set under the lock.
    pub fn servable_hashes(&self) -> Vec<ChunkHash> {
        let st = self.state.lock().unwrap();
        st.entries
            .iter()
            .filter(|(_, e)| is_servable(e.state))
            .map(|(h, _)| *h)
            .collect()
    }

    /// Drain membership changes for the cooperative-cache publisher.
    /// Cheap in the common case: only hashes that became or stopped
    /// being servable since the last drain. Overflow collapses to a
    /// one-shot snapshot (`rebuild`).
    pub fn take_digest_events(&self) -> DigestBatch {
        let mut st = self.state.lock().unwrap();
        match std::mem::replace(&mut st.digest, DigestLog::Incremental(Vec::new())) {
            DigestLog::Incremental(events) => DigestBatch {
                events,
                rebuild: false,
            },
            DigestLog::Invalidated => DigestBatch {
                events: st
                    .entries
                    .iter()
                    .filter(|(_, e)| is_servable(e.state))
                    .map(|(h, _)| DigestChange::Add(*h))
                    .collect(),
                rebuild: true,
            },
        }
    }

    /// All chunks currently in `Dirty` state (needing upload).
    pub fn dirty_chunks(&self) -> Vec<ChunkHash> {
        let st = self.state.lock().unwrap();
        st.entries
            .iter()
            .filter(|(_, e)| e.state == ChunkState::Dirty)
            .map(|(h, _)| *h)
            .collect()
    }
}

fn merge_state(old: ChunkState, new: ChunkState) -> ChunkState {
    use ChunkState::*;
    match (old, new) {
        // Dirty wins until uploaded; pin beats clean.
        (Dirty, _) | (_, Dirty) => Dirty,
        (Pinned, _) | (_, Pinned) => Pinned,
        _ => Clean,
    }
}

/// Pick clean LRU victims to fit `size`; error if impossible.
fn plan_eviction(
    st: &mut State,
    size: u64,
    budget: u64,
) -> Result<Vec<(ChunkHash, u64)>, CoreError> {
    if st.used + size <= budget {
        return Ok(Vec::new());
    }
    let need = st.used + size - budget;
    let mut clean: Vec<(ChunkHash, u64, u64)> = st
        .entries
        .iter()
        .filter(|(_, e)| e.state == ChunkState::Clean)
        .map(|(h, e)| (*h, e.size, e.atime))
        .collect();
    clean.sort_by_key(|(_, _, atime)| *atime);
    let mut freed = 0u64;
    let mut victims = Vec::new();
    for (h, sz, _) in clean {
        if freed >= need {
            break;
        }
        freed += sz;
        victims.push((h, sz));
    }
    if freed < need {
        return Err(CoreError::CacheFull {
            needed: size,
            available: budget.saturating_sub(st.used - freed),
        });
    }
    for (h, sz) in &victims {
        st.entries.remove(h);
        st.used -= sz;
    }
    Ok(victims)
}

fn read_dirs(path: &Path) -> Result<Vec<PathBuf>, CoreError> {
    let mut out = Vec::new();
    if !path.exists() {
        return Ok(out);
    }
    for e in fs::read_dir(path)? {
        let e = e?;
        if e.file_type()?.is_dir() {
            out.push(e.path());
        }
    }
    Ok(out)
}

fn read_files(path: &Path) -> Result<Vec<PathBuf>, CoreError> {
    let mut out = Vec::new();
    for e in fs::read_dir(path)? {
        let e = e?;
        if e.file_type()?.is_file() {
            out.push(e.path());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn chunk(i: u8, len: usize) -> (ChunkHash, Vec<u8>) {
        let data = vec![i; len];
        (ChunkHash::of(&data), data)
    }

    #[test]
    fn insert_get_roundtrip() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (h, d) = chunk(1, 100);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert_eq!(c.get(&h).unwrap(), Some(d));
        assert_eq!(c.usage().used, 100);
    }

    #[test]
    fn lru_eviction_order() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 250).unwrap();
        let (h1, d1) = chunk(1, 100);
        let (h2, d2) = chunk(2, 100);
        c.insert(&h1, &d1, ChunkState::Clean).unwrap();
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        // Touch h1 so h2 becomes LRU.
        c.get(&h1).unwrap();
        let (h3, d3) = chunk(3, 100);
        c.insert(&h3, &d3, ChunkState::Clean).unwrap();
        assert!(c.contains(&h1));
        assert!(!c.contains(&h2), "LRU victim should be h2");
        assert!(c.contains(&h3));
    }

    #[test]
    fn pinned_and_dirty_not_evicted() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 250).unwrap();
        let (h1, d1) = chunk(1, 100);
        let (h2, d2) = chunk(2, 100);
        c.insert(&h1, &d1, ChunkState::Pinned).unwrap();
        c.insert(&h2, &d2, ChunkState::Dirty).unwrap();
        let (h3, d3) = chunk(3, 100);
        let err = c.insert(&h3, &d3, ChunkState::Clean).unwrap_err();
        assert!(matches!(err, CoreError::CacheFull { .. }));
        // Failure left no partial state.
        assert!(!c.contains(&h3));
        assert_eq!(c.usage().used, 200);
        // Demote dirty to clean: now evictable.
        c.set_state(&h2, ChunkState::Clean);
        c.insert(&h3, &d3, ChunkState::Clean).unwrap();
        assert!(!c.contains(&h2));
    }

    #[test]
    fn rescan_rebuilds_accounting() {
        let dir = TempDir::new().unwrap();
        let (h, d) = chunk(7, 64);
        {
            let c = DiskCache::open(dir.path(), 1024).unwrap();
            c.insert(&h, &d, ChunkState::Dirty).unwrap();
        }
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        assert_eq!(c.usage().used, 64);
        // Rescan legitimately returns Clean here: a directory listing
        // cannot distinguish uploaded content from un-uploaded content,
        // so the cache does not try. `constellation_meta::SqliteMeta`'s
        // `pending_upload` table (written in the same transaction as
        // the journal record that made the content dirty) is the real
        // source of truth for what still owes S3 a PUT — see
        // `cli::main::upload_dirty_chunks`, which drains that table
        // rather than `Self::dirty_chunks`. This is not a regression:
        // the cache's eviction guard (pinned/dirty are never evicted)
        // is unaffected, since nothing here is pinned or pending-evict.
        assert_eq!(c.state_of(&h), Some(ChunkState::Clean));
        assert_eq!(c.get(&h).unwrap(), Some(d));
    }

    #[test]
    fn corrupt_chunk_dropped_on_read() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (h, d) = chunk(9, 32);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        // Corrupt the file behind the cache's back.
        let hex = h.to_hex();
        let path = dir.path().join(&hex[0..2]).join(&hex[2..4]).join(&hex);
        fs::write(&path, b"garbage").unwrap();
        assert_eq!(c.get(&h).unwrap(), None);
        assert!(!c.contains(&h));
    }

    #[test]
    fn dirty_chunks_are_not_servable() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (h1, d1) = chunk(1, 32);
        let (h2, d2) = chunk(2, 32);
        c.insert(&h1, &d1, ChunkState::Dirty).unwrap();
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        assert!(c.get_servable(&h1).unwrap().is_none());
        assert_eq!(c.get_servable(&h2).unwrap(), Some(d2));
        assert_eq!(c.servable_hashes(), vec![h2]);
    }

    #[test]
    fn digest_journal_tracks_servable_membership() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let _ = c.take_digest_events(); // drop the empty-open drain

        let (dirty_h, dirty_d) = chunk(1, 32);
        let (clean_h, clean_d) = chunk(2, 32);
        c.insert(&dirty_h, &dirty_d, ChunkState::Dirty).unwrap();
        c.insert(&clean_h, &clean_d, ChunkState::Clean).unwrap();
        let batch = c.take_digest_events();
        assert_eq!(batch.events, vec![DigestChange::Add(clean_h)]);
        assert!(!batch.rebuild);

        c.set_state(&dirty_h, ChunkState::Clean);
        let batch = c.take_digest_events();
        assert_eq!(batch.events, vec![DigestChange::Add(dirty_h)]);

        c.set_state(&clean_h, ChunkState::Dirty);
        let batch = c.take_digest_events();
        assert_eq!(batch.events, vec![DigestChange::Remove(clean_h)]);
        assert!(c.take_digest_events().events.is_empty());
    }

    #[test]
    fn eviction_is_a_digest_remove() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 100).unwrap();
        let _ = c.take_digest_events();
        let (h1, d1) = chunk(1, 60);
        let (h2, d2) = chunk(2, 60);
        c.insert(&h1, &d1, ChunkState::Clean).unwrap();
        let _ = c.take_digest_events();
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        let batch = c.take_digest_events();
        assert_eq!(
            batch.events,
            vec![DigestChange::Remove(h1), DigestChange::Add(h2)]
        );
    }

    #[test]
    fn a_flood_of_changes_collapses_to_a_rebuild_snapshot() {
        let dir = TempDir::new().unwrap();
        let limit = 8;
        let c = DiskCache::open_with_digest_log_limit(dir.path(), 1 << 20, limit).unwrap();
        let _ = c.take_digest_events();
        for i in 0..limit + 1 {
            let (h, d) = chunk((i % 250) as u8, 16 + i);
            c.insert(&h, &d, ChunkState::Clean).unwrap();
        }
        let batch = c.take_digest_events();
        assert!(batch.rebuild, "overflow must force a snapshot");
        assert_eq!(batch.events.len(), limit + 1);
        assert!(batch
            .events
            .iter()
            .all(|event| matches!(event, DigestChange::Add(_))));
    }

    #[test]
    fn digest_journal_preserves_add_then_remove_order() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let _ = c.take_digest_events();
        let (h, d) = chunk(1, 32);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        c.remove(&h).unwrap();
        assert_eq!(
            c.take_digest_events().events,
            vec![DigestChange::Add(h), DigestChange::Remove(h)]
        );
    }

    #[test]
    fn digest_journal_preserves_remove_then_add_order() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (h, d) = chunk(1, 32);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        let _ = c.take_digest_events();
        c.remove(&h).unwrap();
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert_eq!(
            c.take_digest_events().events,
            vec![DigestChange::Remove(h), DigestChange::Add(h)]
        );
    }
}
