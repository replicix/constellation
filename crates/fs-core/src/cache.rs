//! Local disk chunk cache: LRU over clean chunks, pinned/dirty never
//! evicted, reserve-before-accept ENOSPC discipline (DESIGN.md §7, §9).
//!
//! A clean chunk is also un-evictable while somebody holds its *file*
//! open ([`DiskCache::pin_open`], plan 38 §3(c)): an in-memory
//! open-refcount, not a [`ChunkState`], since it is a function of the
//! descriptors that exist right now and nothing a restart should carry.
//! It is the eviction paths it binds — explicit removal
//! ([`DiskCache::remove`]) ignores it exactly as it ignores
//! `Pinned`/`Dirty`.
//!
//! Chunks are stored decompressed at `<root>/<aa>/<bb>/<hex>` (first two
//! hex byte pairs), written temp-name + atomic rename. Accounting is in
//! memory and rebuilt by a directory scan at startup; a read that cannot
//! trust the file's bytes verifies the blake3 hash and drops corrupt
//! files (they are refetched upstream).
//!
//! What "cannot trust" means is [`CacheVerify`] (`--cache-verify`, plan
//! 38 §2.3). Every chunk is hashed exactly once before it is ever
//! admitted — while it streams in from the store or a peer — so under
//! the default [`CacheVerify::Admit`] a disk read re-hashes only entries
//! this process has not hashed itself, i.e. the files a restart's
//! directory scan found. [`CacheVerify::Always`] re-hashes every disk
//! read instead, which is what this cache did before the knob existed.
//!
//! Optionally ([`DiskCache::with_memory_cache`]) verified contents are
//! also kept in memory ([`crate::memcache`]): [`DiskCache::get_shared`]
//! serves a resident chunk without touching the disk or re-hashing, and
//! loads a missing one once however many readers ask for it at the same
//! time. A fetch that still holds the bytes it just committed admits
//! them itself ([`DiskCache::admit_verified`]) instead of making the
//! first read load them back. Memory entries are a subset of the disk
//! entries at all times:
//! every path that drops a disk entry drops its memory copy under the
//! same state-lock hold, and a load is admitted only if its disk entry is
//! still there.

use crate::chunk::ChunkHash;
use crate::error::CoreError;
use crate::memcache::{MemCache, MemCacheStats};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use zeroize::Zeroize;

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
    /// This process hashed these bytes: either it fetched them (the
    /// store/peer fetch hashes in flight and [`DiskCache::commit_spill`]
    /// publishes them) or a [`DiskCache::get_disk`] verified them here.
    /// Clear for a file the startup scan found on disk, which nobody in
    /// this process has checked. Under [`CacheVerify::Admit`] a verified
    /// entry's disk reads skip the blake3 pass (plan 38 §2.3); free in
    /// space, since the `bool` lands in the padding next to the one-byte
    /// `ChunkState`.
    verified: bool,
}

/// When a disk read re-verifies the chunk file it just read
/// (`--cache-verify`, `CONSTELLATION_CACHE_VERIFY`; plan 38 §2.3).
///
/// A chunk is always hashed **once**, while it streams in from the store
/// or a peer, before it is ever admitted; this only decides whether the
/// *local disk copy* is hashed again on every read afterwards.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheVerify {
    /// Verify once per file this process has not hashed yet — the
    /// in-flight hash covers a fetched chunk, and the first read of a
    /// file a restart found on disk covers that one. Later reads of a
    /// verified entry skip the hash.
    #[default]
    Admit,
    /// Hash every disk read, as before this knob existed. The `verified`
    /// bit is ignored; local corruption after admission is caught on the
    /// next read rather than on the next restart. Plan 38 also makes
    /// this mode disable zero-copy and passthrough, which by
    /// construction let the kernel serve the chunk file without the
    /// daemon seeing the bytes (Z3/Z4 wire that up).
    Always,
}

impl CacheVerify {
    pub fn as_str(self) -> &'static str {
        match self {
            CacheVerify::Admit => "admit",
            CacheVerify::Always => "always",
        }
    }

    /// Exactly the two names the CLI help, `docs/reference/configuration.md`
    /// and plan 38 §2.3 give, case- and whitespace-insensitive like
    /// `--cto`/`--locks`: an alias nobody documents is a value that works
    /// on one host and fails on the next.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "admit" => Some(CacheVerify::Admit),
            "always" => Some(CacheVerify::Always),
            _ => None,
        }
    }
}

impl std::fmt::Display for CacheVerify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct State {
    entries: HashMap<ChunkHash, Entry>,
    /// Chunks whose file someone currently holds open, and how many
    /// holders ([`DiskCache::pin_open`]). Never persisted: a crash drops
    /// every open file descriptor with the process. Kept beside
    /// `entries` rather than as a third [`ChunkState`] because the two
    /// lifetimes are different things — a `Pinned` entry is an operator
    /// decision that survives restarts, this is "the kernel has a
    /// backing fd on it right now" (plan 38 §3(c)).
    open_pins: HashMap<ChunkHash, u32>,
    used: u64,
    clock: u64,
    digest: DigestLog,
    digest_limit: usize,
}

/// Bound so a stalled consumer cannot grow the log without limit.
/// Past this the log collapses to [`DigestLog::Invalidated`] and the
/// next drain is a one-shot snapshot.
const MAX_DIGEST_LOG: usize = 65_536;
static SPILL_NONCE: AtomicU64 = AtomicU64::new(0);

/// Cache-owned temporary file used to receive a chunk incrementally.
///
/// The file lives under the cache root, so committing it can use an atomic
/// rename. Dropping an uncommitted spill removes it.
pub struct SpillFile {
    file: Option<fs::File>,
    path: PathBuf,
}

impl Write for SpillFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.as_mut().unwrap().flush()
    }
}

impl Read for SpillFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.as_mut().unwrap().read(buf)
    }
}

impl Seek for SpillFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.as_mut().unwrap().seek(pos)
    }
}

impl SpillFile {
    pub fn as_file(&self) -> &fs::File {
        self.file.as_ref().unwrap()
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

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
            open_pins: HashMap::new(),
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

/// One in-progress load of a chunk into memory, which concurrent readers
/// of the same chunk wait for instead of loading it again.
#[derive(Default)]
struct Flight {
    outcome: Mutex<Option<FlightOutcome>>,
    done: Condvar,
}

#[derive(Clone)]
enum FlightOutcome {
    /// The verified bytes, or `None`: absent (or corrupt, and dropped).
    Loaded(Option<Bytes>),
    /// The load failed (an I/O error, a panic): load it yourself.
    Failed,
}

impl Flight {
    fn wait(&self) -> FlightOutcome {
        let mut outcome = self.outcome.lock().unwrap();
        loop {
            if let Some(outcome) = &*outcome {
                return outcome.clone();
            }
            outcome = self.done.wait(outcome).unwrap();
        }
    }
}

/// The loading reader's registration: ends the flight (as failed, if
/// the load did not report an outcome — an error or a panic) when dropped.
struct FlightLead<'a> {
    flights: &'a Mutex<HashMap<ChunkHash, Arc<Flight>>>,
    hash: ChunkHash,
    flight: Arc<Flight>,
    outcome: Option<FlightOutcome>,
}

impl Drop for FlightLead<'_> {
    fn drop(&mut self) {
        self.flights.lock().unwrap().remove(&self.hash);
        *self.flight.outcome.lock().unwrap() =
            Some(self.outcome.take().unwrap_or(FlightOutcome::Failed));
        self.flight.done.notify_all();
    }
}

/// Disk-backed chunk cache with budget accounting.
pub struct DiskCache {
    root: PathBuf,
    budget: u64,
    addressing_key: Option<Box<[u8; 32]>>,
    state: Mutex<State>,
    /// Verified contents in memory (see the module doc); `None`: off.
    memory: Option<MemCache>,
    flights: Mutex<HashMap<ChunkHash, Arc<Flight>>>,
    verify: CacheVerify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheUsage {
    pub used: u64,
    pub budget: u64,
    pub entries: usize,
    pub pinned: u64,
}

/// What [`DiskCache::resident`] knows about a cached chunk, for a caller
/// deciding whether it may hand the chunk *file* to someone else to read
/// instead of serving the bytes itself (plan 38 §3(c)'s passthrough
/// eligibility rule, which the engine owns: this is only the facts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resident {
    /// The chunk file's length on disk, as the accounting has it.
    pub len: u64,
    pub state: ChunkState,
    /// This process hashed these bytes (the `verified` bit, plan 38
    /// §2.3). A caller that lets someone else read the file without the
    /// daemon seeing the bytes must refuse an unverified entry: nobody
    /// here has checked the copy a restart's directory scan found.
    pub verified: bool,
}

/// A chunk file held open by someone outside the cache: while this guard
/// lives, eviction never chooses `hash` — exactly as it never chooses a
/// `Pinned` or `Dirty` entry (plan 38 §3(c)'s "pin while open").
///
/// Dropping it releases the pin, so the chunk becomes evictable again the
/// instant the last holder goes away; nothing about it is persisted,
/// because a crash drops every open descriptor anyway. It holds an
/// [`Arc`] of its cache, so a pin outliving the rest of the engine's
/// references cannot leave a dangling count.
pub struct OpenPin {
    cache: Arc<DiskCache>,
    hash: ChunkHash,
}

impl OpenPin {
    pub fn hash(&self) -> ChunkHash {
        self.hash
    }
}

impl std::fmt::Debug for OpenPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenPin")
            .field("hash", &self.hash.to_hex())
            .finish()
    }
}

impl Drop for OpenPin {
    fn drop(&mut self) {
        let mut st = self.cache.state.lock().unwrap_or_else(|e| e.into_inner());
        if let std::collections::hash_map::Entry::Occupied(mut slot) = st.open_pins.entry(self.hash)
        {
            if *slot.get() <= 1 {
                slot.remove();
            } else {
                *slot.get_mut() -= 1;
            }
        }
    }
}

/// Result of [`DiskCache::prune_to`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneReport {
    pub freed_bytes: u64,
    pub freed_chunks: u64,
    pub used_bytes: u64,
    pub pinned_bytes: u64,
    pub dirty_bytes: u64,
    pub entries: u64,
}

/// The cache holds decrypted file contents: make its root private to the
/// owner (0o700). Best-effort — a filesystem without Unix permissions must not
/// make the cache unusable. This crate has no logger; the only report is
/// on stderr.
fn restrict_to_owner(root: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(error) = fs::set_permissions(root, fs::Permissions::from_mode(0o700)) {
            eprintln!(
                "warning: could not restrict cache directory {} to mode 0700: {error}",
                root.display()
            );
        }
    }
    #[cfg(not(unix))]
    let _ = root;
}

impl DiskCache {
    /// Open (or create) a cache directory and rebuild accounting from disk.
    pub fn open(root: impl Into<PathBuf>, budget: u64) -> Result<Self, CoreError> {
        Self::open_inner(root, budget, MAX_DIGEST_LOG, None)
    }

    /// Open an E2E cache whose decompressed entries use keyed identities.
    /// The key is needed locally because cache corruption must be detected
    /// before bytes are trusted or offered to a peer.
    pub fn open_keyed(
        root: impl Into<PathBuf>,
        budget: u64,
        addressing_key: [u8; 32],
    ) -> Result<Self, CoreError> {
        let key = Box::new(addressing_key);
        if region::lock(key.as_ptr(), key.len()).is_err() {
            eprintln!(
                "warning: could not mlock E2E cache verifier key; continuing in ordinary memory"
            );
        }
        Self::open_inner(root, budget, MAX_DIGEST_LOG, Some(key))
    }

    /// Alternate journal limit for deterministic tests and constrained
    /// deployments. The production constructor uses [`MAX_DIGEST_LOG`].
    pub fn open_with_digest_log_limit(
        root: impl Into<PathBuf>,
        budget: u64,
        digest_limit: usize,
    ) -> Result<Self, CoreError> {
        Self::open_inner(root, budget, digest_limit, None)
    }

    fn open_inner(
        root: impl Into<PathBuf>,
        budget: u64,
        digest_limit: usize,
        addressing_key: Option<Box<[u8; 32]>>,
    ) -> Result<Self, CoreError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        restrict_to_owner(&root);
        let spill_dir = root.join(".spill");
        let _ = fs::remove_dir_all(&spill_dir);
        fs::create_dir_all(&spill_dir)?;
        let cache = Self {
            root,
            budget,
            addressing_key,
            state: Mutex::new(State::new(digest_limit.max(1))),
            memory: None,
            flights: Mutex::new(HashMap::new()),
            verify: CacheVerify::default(),
        };
        cache.rescan()?;
        Ok(cache)
    }

    /// Keep up to `budget` bytes of verified chunk contents in memory
    /// ([`crate::memcache`]); `0` leaves the memory tier off (every read
    /// loads and verifies the disk copy, as without it).
    pub fn with_memory_cache(mut self, budget: u64) -> Self {
        self.memory = (budget > 0).then(|| MemCache::new(budget));
        self
    }

    /// When a disk read re-hashes the file it read ([`CacheVerify`]).
    /// The default is [`CacheVerify::Admit`].
    pub fn with_verify(mut self, verify: CacheVerify) -> Self {
        self.verify = verify;
        self
    }

    /// The mode [`Self::with_verify`] set (`node.status`, diagnostics).
    pub fn verify_mode(&self) -> CacheVerify {
        self.verify
    }

    /// Whether `hash`'s disk entry has been hashed by this process
    /// (tests, diagnostics). `None`: no such entry.
    ///
    /// Public only because `engine`'s tests assert on it across the crate
    /// boundary; nothing in the running system reads the bit directly.
    #[doc(hidden)]
    pub fn is_verified(&self, hash: &ChunkHash) -> Option<bool> {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(hash)
            .map(|e| e.verified)
    }

    /// The memory tier's counters, if it is on.
    pub fn memory_stats(&self) -> Option<MemCacheStats> {
        self.memory.as_ref().map(MemCache::stats)
    }

    /// Whether `hash` is resident in the memory tier (tests, diagnostics).
    pub fn memory_contains(&self, hash: &ChunkHash) -> bool {
        self.memory.as_ref().is_some_and(|m| m.contains(hash))
    }

    /// Drop `hash`'s memory copy (its disk entry is going). Called under
    /// the state lock; the bytes go to `dropped`, freed after it.
    fn drop_memory(&self, hash: &ChunkHash, dropped: &mut Vec<Bytes>) {
        if let Some(bytes) = self.memory.as_ref().and_then(|m| m.remove(hash)) {
            dropped.push(bytes);
        }
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
                            // Found on disk: nobody in this process has
                            // hashed it, so the first read still does.
                            verified: false,
                        },
                    );
                    st.note(hash, None, Some(ChunkState::Clean));
                }
            }
        }
        Ok(())
    }

    /// `<root>/ab/cd/abcd…`, built in one allocation (a lookup's path is
    /// on every cached read).
    fn path_for(&self, hash: &ChunkHash) -> PathBuf {
        let hex = hash.hex_ascii();
        let hex = std::str::from_utf8(&hex).expect("ascii");
        let mut path = PathBuf::with_capacity(self.root.as_os_str().len() + 72);
        path.push(&self.root);
        path.push(&hex[0..2]);
        path.push(&hex[2..4]);
        path.push(hex);
        path
    }

    fn hash(&self, data: &[u8]) -> ChunkHash {
        self.addressing_key
            .as_ref()
            .map_or_else(|| ChunkHash::of(data), |key| ChunkHash::keyed(key, data))
    }

    pub fn usage(&self) -> CacheUsage {
        let st = self.state.lock().unwrap();
        CacheUsage {
            used: st.used,
            budget: self.budget,
            entries: st.entries.len(),
            pinned: st
                .entries
                .values()
                .filter(|entry| entry.state == ChunkState::Pinned)
                .map(|entry| entry.size)
                .sum(),
        }
    }

    /// Stable operator-facing snapshot of cache contents. Data bytes are
    /// deliberately not read: listing a TiB cache must remain metadata-only.
    pub fn entries(&self) -> Vec<(ChunkHash, u64, ChunkState)> {
        let st = self.state.lock().unwrap();
        let mut entries: Vec<_> = st
            .entries
            .iter()
            .map(|(hash, entry)| (*hash, entry.size, entry.state))
            .collect();
        entries.sort_by_key(|(hash, _, _)| hash.0);
        entries
    }

    pub fn dirty_bytes(&self) -> u64 {
        self.state
            .lock()
            .unwrap()
            .entries
            .values()
            .filter(|entry| entry.state == ChunkState::Dirty)
            .map(|entry| entry.size)
            .sum()
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

    /// [`Resident`] facts about `hash`'s disk entry; `None`: not resident.
    pub fn resident(&self, hash: &ChunkHash) -> Option<Resident> {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(hash)
            .map(|e| Resident {
                len: e.size,
                state: e.state,
                verified: e.verified,
            })
    }

    /// Where `hash`'s chunk file is, so a caller that was granted an
    /// [`OpenPin`] can open it. Take the pin *first*: a pinned entry is
    /// never evicted, so the path cannot be unlinked between the two
    /// (the other order races with a prune).
    /// A new, already unlinked regular file in the cache directory: the
    /// filesystem every chunk file lives on, for a frontend's one-time
    /// check that its kernel accepts a file from there as a passthrough
    /// backing file (plan 38 Z3b). It never has a name a scan could see
    /// (a crash between the create and the unlink leaves a `.tmp` at the
    /// root, which no scan reads either).
    pub fn probe_file(&self) -> std::io::Result<fs::File> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self
            .root
            .join(format!("passthrough-probe-{}-{n}.tmp", std::process::id()));
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let _ = fs::remove_file(&path);
        Ok(file)
    }

    pub fn chunk_path(&self, hash: &ChunkHash) -> PathBuf {
        self.path_for(hash)
    }

    /// Hold `hash`'s chunk file against eviction for as long as the
    /// returned [`OpenPin`] lives (plan 38 §3(c)).
    ///
    /// `CoreError::NotCached` if the chunk is not resident — there is
    /// nothing to hand out, and the caller falls back to serving the
    /// bytes itself. The pin counts holders, so concurrent opens of the
    /// same chunk each take their own and the last one to drop releases
    /// it. Bumps the LRU position, because a passthrough open *is* a read
    /// of the whole chunk that the cache will never see again: without
    /// this the chunk would look untouched since before it was opened and
    /// be the first victim the moment the last handle closed.
    pub fn pin_open(self: &Arc<Self>, hash: &ChunkHash) -> Result<OpenPin, CoreError> {
        let mut st = self.state.lock().unwrap();
        if !st.entries.contains_key(hash) {
            return Err(CoreError::NotCached(hash.to_hex()));
        }
        st.clock += 1;
        let clock = st.clock;
        st.entries.get_mut(hash).unwrap().atime = clock;
        *st.open_pins.entry(*hash).or_insert(0) += 1;
        drop(st);
        Ok(OpenPin {
            cache: Arc::clone(self),
            hash: *hash,
        })
    }

    /// [`OpenPin`]s over every chunk (`node.status`'s `cache.open_pins`).
    pub fn open_pin_total(&self) -> u64 {
        self.state
            .lock()
            .unwrap()
            .open_pins
            .values()
            .map(|n| u64::from(*n))
            .sum()
    }

    /// How many [`OpenPin`]s `hash` currently has (tests, diagnostics).
    pub fn open_pin_count(&self, hash: &ChunkHash) -> u32 {
        self.state
            .lock()
            .unwrap()
            .open_pins
            .get(hash)
            .copied()
            .unwrap_or(0)
    }

    /// Read a chunk, bumping its LRU position. Verifies the hash; corrupt
    /// files are removed and reported as absent (caller refetches).
    ///
    /// A copy of the memory tier's verified bytes when resident; a miss
    /// is not admitted (this is the path of uploads, peer serving and
    /// pins, which would only crowd reads out of memory: reads use
    /// [`Self::get_shared`]).
    pub fn get(&self, hash: &ChunkHash) -> Result<Option<Vec<u8>>, CoreError> {
        if let Some(bytes) = self.memory.as_ref().and_then(|m| m.get(hash)) {
            return Ok(Some(bytes.to_vec()));
        }
        self.get_disk(hash)
    }

    /// Read a chunk for a file read: shared, verified bytes. A resident
    /// copy is served from memory (no disk read, no hash); otherwise the
    /// disk copy is loaded and verified once — concurrent readers of the
    /// same chunk wait for that one load — and admitted to memory. Absent
    /// or corrupt (dropped) chunks are `None`, exactly like [`Self::get`].
    pub fn get_shared(&self, hash: &ChunkHash) -> Result<Option<Bytes>, CoreError> {
        let Some(memory) = &self.memory else {
            return Ok(self.get_disk(hash)?.map(Bytes::from));
        };
        if let Some(bytes) = memory.get(hash) {
            return Ok(Some(bytes));
        }
        if !self.contains(hash) {
            // Cold: nothing to load or wait for.
            return Ok(None);
        }
        let flight = {
            let mut flights = self.flights.lock().unwrap();
            match flights.get(hash) {
                Some(flight) => Err(flight.clone()),
                None => {
                    let flight = Arc::new(Flight::default());
                    flights.insert(*hash, flight.clone());
                    Ok(flight)
                }
            }
        };
        let flight = match flight {
            Ok(flight) => flight,
            Err(leader) => {
                memory.note_coalesced();
                return match leader.wait() {
                    FlightOutcome::Loaded(bytes) => Ok(bytes),
                    FlightOutcome::Failed => self.load_shared(memory, hash),
                };
            }
        };
        let mut lead = FlightLead {
            flights: &self.flights,
            hash: *hash,
            flight,
            outcome: None,
        };
        // A load that finished between the miss above and this flight's
        // registration has admitted the chunk already.
        if let Some(bytes) = memory.get(hash) {
            lead.outcome = Some(FlightOutcome::Loaded(Some(bytes.clone())));
            return Ok(Some(bytes));
        }
        let loaded = self.load_shared(memory, hash)?;
        lead.outcome = Some(FlightOutcome::Loaded(loaded.clone()));
        Ok(loaded)
    }

    /// Load and verify the disk copy, and admit it to memory if its disk
    /// entry is still there (checked under the state lock, which every
    /// removal holds while it drops the memory copy: an entry removed
    /// meanwhile is not resurrected in memory).
    fn load_shared(&self, memory: &MemCache, hash: &ChunkHash) -> Result<Option<Bytes>, CoreError> {
        let Some(data) = self.get_disk(hash)? else {
            return Ok(None);
        };
        memory.note_miss();
        let bytes = Bytes::from(data);
        let evicted = {
            let st = self.state.lock().unwrap();
            if st.entries.contains_key(hash) {
                memory.insert(*hash, bytes.clone())
            } else {
                Vec::new()
            }
        };
        drop(evicted);
        Ok(Some(bytes))
    }

    /// Admit bytes this process has already verified — the `Bytes` a
    /// store or peer fetch holds after [`Self::commit_spill`], hashed in
    /// flight on their way into the spill file — to the memory tier.
    ///
    /// The point is the I/O it does *not* do: without this the first
    /// read of a just-fetched chunk goes through [`Self::load_shared`],
    /// which reads the whole file back and hashes it a second time (plan
    /// 38 §2.3, change 1). Only admitted if the disk entry is still
    /// there, checked under the state lock exactly as `load_shared`
    /// does, so a chunk removed between the fetch and here is not
    /// resurrected in memory.
    ///
    /// A no-op with the memory tier off, and (like every admission)
    /// for a chunk too large for the budget or already resident. Debug
    /// builds assert the bytes really are `hash`'s.
    pub fn admit_verified(&self, hash: &ChunkHash, bytes: Bytes) {
        debug_assert_eq!(&self.hash(&bytes), hash);
        let Some(memory) = &self.memory else {
            return;
        };
        let evicted = {
            let st = self.state.lock().unwrap();
            if st.entries.contains_key(hash) {
                memory.insert(*hash, bytes)
            } else {
                Vec::new()
            }
        };
        drop(evicted);
    }

    /// Read a chunk whose bytes are about to leave this node for shared
    /// storage, hashing it even where [`CacheVerify::Admit`] would trust
    /// the entry.
    ///
    /// `admit`'s trade (plan 38 §2.3) is that local corruption after
    /// admission reaches a *reader*, who finds the hash wrong and
    /// refetches: what is in the bucket is still right. Publishing those
    /// bytes is a different class of damage — `chunk/<H>` would hold
    /// content that is not `H`, every node's in-flight check would refuse
    /// it from then on, and `ChunkPutMode::Create`'s "already exists is a
    /// dedup hit" means a node holding the correct bytes never overwrites
    /// it. So the uploader pays one blake3 pass — next to a zstd encode
    /// and an S3 PUT, which it is not measurable against — and a corrupt
    /// local copy is dropped here instead, putting the row back on the
    /// "not in the cache" path it took before this knob existed.
    ///
    /// A resident memory copy is used but hashed too: under `admit` it may
    /// itself have come from a disk read that skipped the hash
    /// ([`Self::load_shared`]), so trusting it would leave the same hole.
    pub fn get_verified(&self, hash: &ChunkHash) -> Result<Option<Vec<u8>>, CoreError> {
        if let Some(bytes) = self.memory.as_ref().and_then(|m| m.get(hash)) {
            if &self.hash(&bytes) == hash {
                return Ok(Some(bytes.to_vec()));
            }
            // Rotted in memory (or admitted from a disk copy that had
            // already rotted): drop it under the state lock, as every
            // other memory removal does, and fall through to the disk
            // copy — which the forced read below hashes as well, so a
            // corrupt file is dropped and reported absent.
            let dropped = {
                let _st = self.state.lock().unwrap();
                self.memory.as_ref().and_then(|m| m.remove(hash))
            };
            drop(dropped);
        }
        self.read_disk(hash, true)
    }

    /// [`Self::get`] from the disk copy only.
    ///
    /// The blake3 pass is skipped for an entry this process has already
    /// hashed, unless [`CacheVerify::Always`] is in force (plan 38 §2.3,
    /// change 2); an unverified one — a file the startup scan found — is
    /// hashed here and marked, so a restart costs one hash per file and
    /// not one per read.
    fn get_disk(&self, hash: &ChunkHash) -> Result<Option<Vec<u8>>, CoreError> {
        self.read_disk(hash, false)
    }

    /// [`Self::get_disk`], with `force_verify` for the callers that must
    /// not trust the `verified` bit ([`Self::get_verified`]).
    fn read_disk(
        &self,
        hash: &ChunkHash,
        force_verify: bool,
    ) -> Result<Option<Vec<u8>>, CoreError> {
        let verified = {
            let mut st = self.state.lock().unwrap();
            if !st.entries.contains_key(hash) {
                return Ok(None);
            }
            st.clock += 1;
            let clock = st.clock;
            let entry = st.entries.get_mut(hash).unwrap();
            entry.atime = clock;
            entry.verified
        };
        let path = self.path_for(hash);
        let data = loop {
            match fs::read(&path) {
                Ok(d) => break d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // An entry's file appears and disappears only under the
                    // state lock (see `commit_spill`, `unlink_absent`), so
                    // re-check there: the entry this read saw may have been
                    // evicted and re-inserted meanwhile, and forgetting the
                    // new entry would orphan its file — a pending chunk the
                    // uploader then reports as lost.
                    let st = self.state.lock().unwrap();
                    if !st.entries.contains_key(hash) {
                        return Ok(None);
                    }
                    if path.exists() {
                        continue;
                    }
                    drop(st);
                    // Present in the accounting but gone from disk:
                    // something outside the cache removed it.
                    self.forget(hash);
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            }
        };
        if force_verify || self.verify == CacheVerify::Always || !verified {
            if &self.hash(&data) != hash {
                // Corrupt local copy: drop it, let the caller refetch.
                self.remove(hash)?;
                return Ok(None);
            }
            // Hashed here: later reads under `Admit` need not. Only if
            // the entry is still the one this read saw — an eviction and
            // re-insert in between has already marked its own.
            if let Some(entry) = self.state.lock().unwrap().entries.get_mut(hash) {
                entry.verified = true;
            }
        }
        Ok(Some(data))
    }

    /// Begin receiving a chunk into cache-owned storage.
    pub fn begin_spill(&self) -> Result<SpillFile, CoreError> {
        let nonce = SPILL_NONCE.fetch_add(1, Ordering::Relaxed);
        let path = self
            .root
            .join(".spill")
            .join(format!("{}-{nonce}", std::process::id()));
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(SpillFile {
            file: Some(file),
            path,
        })
    }

    /// Commit a fully written and externally verified spill into the cache.
    ///
    /// Accounting and eviction match [`Self::insert`]. If another fetch won
    /// the same-hash race, its entry is retained and this spill is discarded.
    pub fn commit_spill(
        &self,
        hash: &ChunkHash,
        mut spill: SpillFile,
        state: ChunkState,
    ) -> Result<(), CoreError> {
        let file = spill.file.as_mut().unwrap();
        file.flush()?;
        file.sync_data()?;
        let size = file.metadata()?.len();
        let path = self.path_for(hash);
        fs::create_dir_all(path.parent().unwrap())?;
        drop(spill.file.take());

        let mut dropped = Vec::new();
        let victims = {
            let mut st = self.state.lock().unwrap();
            if let Some((old, now)) = st.entries.get_mut(hash).map(|entry| {
                let old = entry.state;
                entry.state = merge_state(entry.state, state);
                (old, entry.state)
            }) {
                st.note(*hash, Some(old), Some(now));
                return Ok(());
            }
            let victims = plan_eviction(&mut st, size, self.budget, self.memory.as_ref())?;
            for (victim, _) in &victims {
                st.note(*victim, Some(ChunkState::Clean), None);
                self.drop_memory(victim, &mut dropped);
            }
            st.used += size;
            st.clock += 1;
            let atime = st.clock;
            st.entries.insert(
                *hash,
                Entry {
                    size,
                    state,
                    atime,
                    // The caller hashed these bytes (the fetch does it in
                    // flight, `insert` against the data it was handed).
                    verified: true,
                },
            );
            st.note(*hash, None, Some(state));
            if let Err(error) = fs::rename(&spill.path, &path) {
                if let Some(entry) = st.entries.remove(hash) {
                    st.used -= entry.size;
                    st.note(*hash, Some(entry.state), None);
                }
                return Err(error.into());
            }
            victims
        };

        self.unlink_absent(&victims);
        Ok(())
    }

    /// Insert a chunk: evicts clean LRU entries to make room, fails with
    /// `CacheFull` (never partial state) if non-evictable content leaves
    /// no room.
    ///
    /// The bytes go to a private spill file first and are published by
    /// [`Self::commit_spill`], which creates the entry and renames the file
    /// into place under one lock hold. An entry is therefore never visible
    /// before its file: a concurrent insert of the same hash (two files
    /// with the same content, which the write path dedups) returns only
    /// once the bytes are on disk, and a concurrent `get` can never see
    /// the entry without its file and forget it. Reserving the entry first
    /// and writing afterwards allowed exactly that, and a forgotten dirty
    /// entry is a pending upload whose chunk is "missing from the local
    /// cache" although its file is right there.
    pub fn insert(
        &self,
        hash: &ChunkHash,
        data: &[u8],
        state: ChunkState,
    ) -> Result<(), CoreError> {
        debug_assert_eq!(&self.hash(data), hash);
        {
            let mut st = self.state.lock().unwrap();
            if let Some((old, now)) = st.entries.get_mut(hash).map(|e| {
                let old = e.state;
                e.state = merge_state(e.state, state);
                (old, e.state)
            }) {
                st.note(*hash, Some(old), Some(now));
                return Ok(());
            }
        }
        let mut spill = self.begin_spill()?;
        spill.write_all(data)?;
        self.commit_spill(hash, spill, state)
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

    /// Remove a chunk from cache and disk (and memory).
    ///
    /// Unconditional: `Pinned`, `Dirty` and open-pinned
    /// ([`Self::pin_open`]) entries go too. This is the caller saying
    /// "this chunk must not be here" (a corrupt copy, the conformance
    /// kit's `evict` hook), not eviction choosing a victim — the
    /// non-evictable states are only about the latter. A holder's open
    /// descriptor keeps reading the unlinked file, as any open file does.
    pub fn remove(&self, hash: &ChunkHash) -> Result<(), CoreError> {
        let mut dropped = Vec::new();
        let mut st = self.state.lock().unwrap();
        if let Some(e) = st.entries.remove(hash) {
            st.used -= e.size;
            st.note(*hash, Some(e.state), None);
        }
        self.drop_memory(hash, &mut dropped);
        match fs::remove_file(self.path_for(hash)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete evicted victims' files, each only if its hash has not been
    /// re-inserted since the eviction dropped its entry. Files appear only
    /// by a rename under the state lock, so checking and unlinking under
    /// it cannot delete a re-inserted chunk's fresh file (an unconditional
    /// unlink after the lock was released could, leaving a dirty entry
    /// without its bytes). One short lock hold per victim keeps a large
    /// prune from stalling readers.
    fn unlink_absent(&self, victims: &[(ChunkHash, u64)]) {
        for (victim, _) in victims {
            let st = self.state.lock().unwrap();
            if !st.entries.contains_key(victim) {
                let _ = fs::remove_file(self.path_for(victim));
            }
        }
    }

    /// Drop clean LRU chunks until `used <= target_used`, or until nothing
    /// clean remains. Pinned and dirty entries are never removed.
    ///
    /// Best-effort: if non-evictable content already exceeds `target_used`,
    /// every clean chunk is still freed and the report reflects what is
    /// left. Digest remove events are recorded for cooperative-cache
    /// republish.
    pub fn prune_to(&self, target_used: u64) -> Result<PruneReport, CoreError> {
        let mut dropped = Vec::new();
        let (victims, report) = {
            let mut st = self.state.lock().unwrap();
            let before = st.used;
            let need = st.used.saturating_sub(target_used);
            let clean = clean_by_recency(&st, self.memory.as_ref());
            let mut freed = 0u64;
            let mut victims = Vec::new();
            for (h, sz, _) in clean {
                if freed >= need {
                    break;
                }
                freed += sz;
                victims.push((h, sz));
            }
            for (h, sz) in &victims {
                st.entries.remove(h);
                st.used -= sz;
                st.note(*h, Some(ChunkState::Clean), None);
                self.drop_memory(h, &mut dropped);
            }
            let pinned: u64 = st
                .entries
                .values()
                .filter(|e| e.state == ChunkState::Pinned)
                .map(|e| e.size)
                .sum();
            let dirty: u64 = st
                .entries
                .values()
                .filter(|e| e.state == ChunkState::Dirty)
                .map(|e| e.size)
                .sum();
            let report = PruneReport {
                freed_bytes: before - st.used,
                freed_chunks: victims.len() as u64,
                used_bytes: st.used,
                pinned_bytes: pinned,
                dirty_bytes: dirty,
                entries: st.entries.len() as u64,
            };
            (victims, report)
        };
        self.unlink_absent(&victims);
        Ok(report)
    }

    fn forget(&self, hash: &ChunkHash) {
        let mut dropped = Vec::new();
        let mut st = self.state.lock().unwrap();
        if let Some(e) = st.entries.remove(hash) {
            st.used -= e.size;
            st.note(*hash, Some(e.state), None);
        }
        self.drop_memory(hash, &mut dropped);
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

impl Drop for DiskCache {
    fn drop(&mut self) {
        if let Some(key) = &mut self.addressing_key {
            let _ = unsafe { region::unlock(key.as_ptr(), key.len()) };
            key.zeroize();
        }
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

/// Clean entries, least recently used first. A chunk resident in the
/// memory tier counts as more recent than any that is not: its reads are
/// served from memory and so never bump its disk `atime`, and it is by
/// construction among the most recently read (the memory budget is a
/// small fraction of the disk's).
///
/// A chunk whose file someone holds open ([`DiskCache::pin_open`]) is not
/// a candidate at all, exactly as a `Pinned` or `Dirty` one is not: this
/// is the one filter every eviction path shares ([`DiskCache::prune_to`]
/// and [`plan_eviction`] both select through it), so "pin while open"
/// (plan 38 §3(c)) is enforced in one place and the cache's `used`
/// accounting keeps covering every byte that is still on disk.
fn clean_by_recency(st: &State, memory: Option<&MemCache>) -> Vec<(ChunkHash, u64, u64)> {
    let mut clean: Vec<(ChunkHash, u64, u64, bool)> = st
        .entries
        .iter()
        .filter(|(h, e)| e.state == ChunkState::Clean && !st.open_pins.contains_key(*h))
        .map(|(h, e)| (*h, e.size, e.atime, memory.is_some_and(|m| m.contains(h))))
        .collect();
    clean.sort_by_key(|(_, _, atime, resident)| (*resident, *atime));
    clean
        .into_iter()
        .map(|(h, size, atime, _)| (h, size, atime))
        .collect()
}

/// Pick clean LRU victims to fit `size`; error if impossible.
fn plan_eviction(
    st: &mut State,
    size: u64,
    budget: u64,
    memory: Option<&MemCache>,
) -> Result<Vec<(ChunkHash, u64)>, CoreError> {
    if st.used + size <= budget {
        return Ok(Vec::new());
    }
    let need = st.used + size - budget;
    let clean = clean_by_recency(st, memory);
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
    use std::sync::Arc;
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
    #[cfg(unix)]
    fn a_probe_file_is_a_nameless_regular_file_in_the_cache_dir() {
        use std::os::unix::fs::MetadataExt;
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let before: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
        let f = c.probe_file().unwrap();
        assert!(f.metadata().unwrap().is_file());
        assert_eq!(f.metadata().unwrap().nlink(), 0, "unlinked");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), before.len());
        assert_eq!(c.usage().used, 0);
    }

    #[test]
    fn a_chunk_lives_at_its_two_level_hex_path() {
        // The on-disk layout survives restarts (and upgrades): unchanged.
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (h, d) = chunk(9, 10);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        let hex = h.to_hex();
        let expected = dir.path().join(&hex[0..2]).join(&hex[2..4]).join(&hex);
        assert_eq!(c.path_for(&h), expected);
        assert_eq!(std::fs::read(expected).unwrap(), d);
    }

    #[cfg(unix)]
    #[test]
    fn cache_root_is_private_to_the_owner() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        let dir = TempDir::new().unwrap();

        // Created by the open.
        let fresh = dir.path().join("fresh");
        let _c = DiskCache::open(&fresh, 1024).unwrap();
        assert_eq!(mode(&fresh), 0o700);
    }

    #[test]
    fn spill_commit_matches_insert_accounting() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (hash, data) = chunk(4, 100);
        let mut spill = c.begin_spill().unwrap();
        spill.write_all(&data).unwrap();
        c.commit_spill(&hash, spill, ChunkState::Clean).unwrap();
        assert_eq!(c.get(&hash).unwrap(), Some(data));
        assert_eq!(c.usage().used, 100);
    }

    #[test]
    fn dropped_spill_cleans_up() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        {
            let mut spill = c.begin_spill().unwrap();
            spill.write_all(b"incomplete").unwrap();
        }
        assert_eq!(fs::read_dir(dir.path().join(".spill")).unwrap().count(), 0);
        assert_eq!(c.usage().used, 0);
    }

    #[test]
    fn spill_commit_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1024).unwrap();
        let (hash, data) = chunk(5, 100);
        c.insert(&hash, &data, ChunkState::Clean).unwrap();
        let mut spill = c.begin_spill().unwrap();
        spill.write_all(&data).unwrap();
        c.commit_spill(&hash, spill, ChunkState::Pinned).unwrap();
        assert_eq!(c.usage().used, 100);
        assert_eq!(c.state_of(&hash), Some(ChunkState::Pinned));
        assert_eq!(fs::read_dir(dir.path().join(".spill")).unwrap().count(), 0);
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
    fn prune_drops_clean_lru_and_spares_pinned_dirty() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 10_000).unwrap();
        let (h1, d1) = chunk(1, 100);
        let (h2, d2) = chunk(2, 100);
        let (h3, d3) = chunk(3, 100);
        c.insert(&h1, &d1, ChunkState::Clean).unwrap();
        c.insert(&h2, &d2, ChunkState::Pinned).unwrap();
        c.insert(&h3, &d3, ChunkState::Dirty).unwrap();
        c.get(&h1).unwrap(); // touch so h1 is MRU among clean (only clean)

        let report = c.prune_to(0).unwrap();
        assert_eq!(report.freed_chunks, 1);
        assert_eq!(report.freed_bytes, 100);
        assert!(!c.contains(&h1));
        assert!(c.contains(&h2));
        assert!(c.contains(&h3));
        assert_eq!(report.pinned_bytes, 100);
        assert_eq!(report.dirty_bytes, 100);
        assert_eq!(report.used_bytes, 200);

        let again = c.prune_to(0).unwrap();
        assert_eq!(again.freed_chunks, 0);
        assert_eq!(again.used_bytes, 200);
    }

    #[test]
    fn prune_to_target_keeps_newest_clean() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 10_000).unwrap();
        let (h1, d1) = chunk(1, 100);
        let (h2, d2) = chunk(2, 100);
        let (h3, d3) = chunk(3, 100);
        c.insert(&h1, &d1, ChunkState::Clean).unwrap();
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        c.insert(&h3, &d3, ChunkState::Clean).unwrap();
        // Target just under 200 so one clean chunk (LRU = h1) must go.
        let report = c.prune_to(200).unwrap();
        assert_eq!(report.freed_chunks, 1);
        assert!(!c.contains(&h1));
        assert!(c.contains(&h2));
        assert!(c.contains(&h3));
        assert_eq!(report.used_bytes, 200);
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
        // so the cache does not try. `constellation_meta::Meta`'s
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
        // `always`: this chunk was inserted by this process, so under
        // the default `admit` its file is trusted and not re-hashed
        // (plan 38 §2.3 — `a_startup_scanned_entry_is_hashed_on_its_first_read_only`
        // and `always_rehashes_every_disk_read` cover that split). What
        // is asserted here is the drop-on-corruption itself.
        let c = DiskCache::open(dir.path(), 1024)
            .unwrap()
            .with_verify(CacheVerify::Always);
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

    /// The storm-hang regression (inbox-create-storm-p2p-off): two files
    /// with the same content insert the same hash concurrently. The
    /// second insert used to return as soon as the first had *reserved*
    /// the entry, before its file existed, so the second writer's read
    /// (or the uploader's) found no file and forgot the entry — leaving a
    /// pending upload whose chunk was "missing from the local cache"
    /// while the file sat on disk. After an insert returns, the chunk must
    /// be readable and must stay in the accounting.
    #[test]
    fn concurrent_same_hash_insert_is_readable_on_return() {
        let dir = TempDir::new().unwrap();
        let c = std::sync::Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let threads = 8;
        let rounds = 400u32;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let c = c.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    for round in 0..rounds {
                        let data = format!("worker 0 file {round} xxxxxx").into_bytes();
                        let hash = ChunkHash::of(&data);
                        barrier.wait();
                        c.insert(&hash, &data, ChunkState::Dirty).unwrap();
                        assert_eq!(
                            c.get(&hash).unwrap().as_deref(),
                            Some(&data[..]),
                            "round {round}: a just-inserted chunk must be readable"
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for round in 0..rounds {
            let data = format!("worker 0 file {round} xxxxxx").into_bytes();
            let hash = ChunkHash::of(&data);
            assert_eq!(c.state_of(&hash), Some(ChunkState::Dirty), "round {round}");
        }
        assert_eq!(c.dirty_chunks().len(), rounds as usize);
    }

    // ---- the memory tier (`with_memory_cache`, `get_shared`) ----

    fn file_of(dir: &TempDir, h: &ChunkHash) -> PathBuf {
        let hex = h.to_hex();
        dir.path().join(&hex[0..2]).join(&hex[2..4]).join(&hex)
    }

    #[test]
    fn a_resident_chunk_is_served_without_rereading_the_disk() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20);
        let (h, d) = chunk(1, 4096);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert!(!c.memory_contains(&h), "a write is not admitted");
        let first = c.get_shared(&h).unwrap().unwrap();
        assert_eq!(first, d);
        assert!(c.memory_contains(&h));
        // Were the disk copy re-read (and re-verified), this would now be
        // a corrupt chunk: dropped, and the read a miss.
        fs::write(file_of(&dir, &h), b"garbage").unwrap();
        let second = c.get_shared(&h).unwrap().unwrap();
        assert_eq!(second.as_ptr(), first.as_ptr(), "the same shared copy");
        assert_eq!(
            c.get(&h).unwrap(),
            Some(d),
            "`get` copies out of memory too"
        );
        let stats = c.memory_stats().unwrap();
        assert_eq!((stats.misses, stats.hits, stats.entries), (1, 2, 1));
    }

    #[test]
    fn a_corrupt_disk_copy_is_neither_served_nor_cached() {
        let dir = TempDir::new().unwrap();
        // `always`, so the check runs on a chunk this process inserted;
        // the second half below is the same assertion in the shape the
        // default `admit` mode still hashes — a file a restart found.
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20)
            .with_verify(CacheVerify::Always);
        let (h, d) = chunk(2, 4096);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        let mut bad = d.clone();
        bad[100] ^= 1; // same length, one bit off
        fs::write(file_of(&dir, &h), &bad).unwrap();
        assert_eq!(c.get_shared(&h).unwrap(), None);
        assert!(!c.memory_contains(&h), "unverified bytes were cached");
        assert!(!c.contains(&h), "the corrupt disk entry is dropped");
        assert_eq!(c.memory_stats().unwrap().used_bytes, 0);
        // Refetched and inserted again: served, and cached, normally.
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert_eq!(c.get_shared(&h).unwrap().as_deref(), Some(&d[..]));
        assert!(c.memory_contains(&h));
        drop(c);

        // Under `admit`: the startup scan's entries are unverified, so a
        // corrupt file found on disk is still caught on its first read.
        fs::write(file_of(&dir, &h), &bad).unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20);
        assert_eq!(c.get_shared(&h).unwrap(), None);
        assert!(!c.memory_contains(&h), "unverified bytes were cached");
        assert!(!c.contains(&h), "the corrupt disk entry is dropped");
    }

    #[test]
    fn a_keyed_e2e_cache_verifies_before_admitting() {
        let dir = TempDir::new().unwrap();
        let key = [7u8; 32];
        // `always`: the point here is the keyed hasher, so the check has
        // to run on chunks this process inserted (plan 38 §2.3).
        let c = DiskCache::open_keyed(dir.path(), 1 << 20, key)
            .unwrap()
            .with_memory_cache(1 << 20)
            .with_verify(CacheVerify::Always);
        let d = vec![3u8; 4096];
        let h = ChunkHash::keyed(&key, &d);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert_eq!(c.get_shared(&h).unwrap().as_deref(), Some(&d[..]));
        assert!(c.memory_contains(&h));
        // A second keyed chunk whose file is swapped for bytes that match
        // only the *plain* hash: the keyed check refuses it.
        let d2 = vec![4u8; 4096];
        let h2 = ChunkHash::keyed(&key, &d2);
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        fs::write(file_of(&dir, &h2), &d).unwrap();
        assert_eq!(c.get_shared(&h2).unwrap(), None);
        assert!(!c.memory_contains(&h2));
    }

    #[test]
    fn dropping_the_disk_entry_drops_the_memory_copy() {
        let dir = TempDir::new().unwrap();
        // Disk room for three 100-byte chunks.
        let c = DiskCache::open(dir.path(), 300)
            .unwrap()
            .with_memory_cache(1 << 20);
        let load = |i: u8| {
            let (h, d) = chunk(i, 100);
            c.insert(&h, &d, ChunkState::Clean).unwrap();
            c.get_shared(&h).unwrap().unwrap();
            assert!(c.memory_contains(&h));
            h
        };
        // remove
        let h = load(1);
        c.remove(&h).unwrap();
        assert!(!c.memory_contains(&h));
        // prune
        let h = load(2);
        c.prune_to(0).unwrap();
        assert!(!c.memory_contains(&h));
        // removed behind the cache's back, then noticed by a disk read
        let h = load(3);
        fs::remove_file(file_of(&dir, &h)).unwrap();
        drop(c.memory.as_ref().unwrap().remove(&h)); // force the disk path
        assert_eq!(c.get_shared(&h).unwrap(), None);
        assert!(!c.contains(&h) && !c.memory_contains(&h));
        // disk eviction: fill the disk budget with resident chunks, then
        // one more; the victim leaves memory with its disk entry
        let hs: Vec<_> = (10..13).map(load).collect();
        let (h4, d4) = chunk(20, 100);
        c.insert(&h4, &d4, ChunkState::Clean).unwrap();
        for h in &hs {
            assert_eq!(c.contains(h), c.memory_contains(h), "{h:?}");
        }
        assert_eq!(hs.iter().filter(|h| c.contains(h)).count(), 2);
        assert_eq!(c.memory_stats().unwrap().entries, 2);
    }

    #[test]
    fn disk_eviction_spares_memory_resident_chunks() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 250)
            .unwrap()
            .with_memory_cache(1 << 20);
        let (h1, d1) = chunk(1, 100);
        let (h2, d2) = chunk(2, 100);
        c.insert(&h1, &d1, ChunkState::Clean).unwrap();
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        c.get_shared(&h1).unwrap(); // resident in memory
        c.get(&h2).unwrap(); // a disk read: h2's atime is now the newest
        for _ in 0..3 {
            c.get_shared(&h1).unwrap(); // memory hits: no disk atime bump
        }
        let (h3, d3) = chunk(3, 100);
        c.insert(&h3, &d3, ChunkState::Clean).unwrap();
        assert!(
            c.contains(&h1),
            "the hot, memory-resident chunk was evicted"
        );
        assert!(!c.contains(&h2));
    }

    #[test]
    fn a_plain_get_miss_is_not_admitted() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20);
        let (h, d) = chunk(1, 4096);
        c.insert(&h, &d, ChunkState::Dirty).unwrap();
        assert_eq!(c.get(&h).unwrap(), Some(d.clone()));
        assert!(!c.memory_contains(&h));
        // A read (here: a writer reading its own sealed chunk) admits.
        assert_eq!(c.get_shared(&h).unwrap().as_deref(), Some(&d[..]));
        assert!(c.memory_contains(&h));
    }

    #[test]
    fn with_the_memory_tier_off_reads_are_unchanged() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(0);
        let (h, d) = chunk(1, 4096);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert_eq!(c.get_shared(&h).unwrap().as_deref(), Some(&d[..]));
        assert!(c.memory_stats().is_none());
        assert!(!c.memory_contains(&h));
        let (absent, _) = chunk(2, 10);
        assert_eq!(c.get_shared(&absent).unwrap(), None);
    }

    /// Concurrent first reads of one chunk load and verify it once.
    #[test]
    fn concurrent_first_reads_load_once() {
        let dir = TempDir::new().unwrap();
        let c = Arc::new(
            DiskCache::open(dir.path(), 64 << 20)
                .unwrap()
                .with_memory_cache(64 << 20),
        );
        let threads = 16;
        for round in 0..20u8 {
            let (h, d) = chunk(round, 4 << 20);
            c.insert(&h, &d, ChunkState::Clean).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(threads));
            let before = c.memory_stats().unwrap();
            let readers: Vec<_> = (0..threads)
                .map(|_| {
                    let (c, barrier) = (c.clone(), barrier.clone());
                    std::thread::spawn(move || {
                        barrier.wait();
                        c.get_shared(&h).unwrap().unwrap()
                    })
                })
                .collect();
            for r in readers {
                assert_eq!(r.join().unwrap().len(), d.len());
            }
            let after = c.memory_stats().unwrap();
            assert_eq!(after.misses - before.misses, 1, "round {round}: one load");
            assert_eq!(
                (after.hits - before.hits) + (after.coalesced - before.coalesced),
                threads as u64 - 1,
                "round {round}: every other reader hit or waited"
            );
        }
    }

    /// Memory entries are always a subset of the disk entries, under
    /// concurrent reads, removals, prunes, evicting inserts and direct
    /// admissions of fetched bytes (plan 38 §2.3's new writer into the
    /// memory tier: it must respect the invariant `load_shared` does).
    #[test]
    fn memory_never_outlives_the_disk_entry() {
        let dir = TempDir::new().unwrap();
        let c = Arc::new(
            DiskCache::open(dir.path(), 40 * 256)
                .unwrap()
                .with_memory_cache(16 * 256),
        );
        let data = |i: u32| {
            let mut d = format!("chunk {i} ").into_bytes();
            d.resize(256, b'z');
            d
        };
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let workers: Vec<_> = (0..6u32)
            .map(|t| {
                let (c, stop) = (c.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut i = t;
                    while !stop.load(Ordering::Relaxed) {
                        let d = data(i % 97);
                        let h = ChunkHash::of(&d);
                        match i % 7 {
                            0 => c.remove(&h).unwrap(),
                            1 if t == 0 => {
                                c.prune_to(20 * 256).unwrap();
                            }
                            1..=3 => {
                                let _ = c.insert(&h, &d, ChunkState::Clean);
                            }
                            4 => {
                                // The fetch path's shape: commit, then
                                // hand the bytes it still holds to the
                                // memory tier.
                                let _ = c.insert(&h, &d, ChunkState::Clean);
                                c.admit_verified(&h, Bytes::from(d.clone()));
                            }
                            5 => {
                                // A passthrough open's shape (plan 38
                                // §3(c)): pin, read through the pin,
                                // release. The pin only ever makes a
                                // chunk *less* evictable, so the memory
                                // subset invariant must still hold with
                                // pins coming and going under the prunes.
                                if let Ok(pin) = c.pin_open(&h) {
                                    if let Some(got) = c.get_shared(&h).unwrap() {
                                        assert_eq!(got, d);
                                    }
                                    drop(pin);
                                }
                            }
                            _ => {
                                if let Some(got) = c.get_shared(&h).unwrap() {
                                    assert_eq!(got, d);
                                }
                            }
                        }
                        i = i.wrapping_add(13);
                    }
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(500));
        stop.store(true, Ordering::Relaxed);
        for w in workers {
            w.join().unwrap();
        }
        let stats = c.memory_stats().unwrap();
        assert!(stats.hits > 0 && stats.misses > 0, "{stats:?}");
        assert!(stats.used_bytes <= stats.budget_bytes);
        let mut resident = 0;
        for i in 0..97 {
            let h = ChunkHash::of(&data(i));
            if c.memory_contains(&h) {
                resident += 1;
                assert!(c.contains(&h), "chunk {i} is in memory but not on disk");
            }
        }
        assert_eq!(resident, stats.entries);
    }

    // ---- pin while open (`pin_open`; plan 38 §3(c)) ----

    /// A chunk whose file someone holds open is not an eviction
    /// candidate, by either path that picks victims — and is one again
    /// the instant the guard drops.
    #[test]
    fn an_open_pinned_chunk_is_not_evicted_until_its_guard_drops() {
        let dir = TempDir::new().unwrap();
        let c = Arc::new(DiskCache::open(dir.path(), 250).unwrap());
        let (h1, d1) = chunk(1, 100);
        let (h2, d2) = chunk(2, 100);
        c.insert(&h1, &d1, ChunkState::Clean).unwrap();
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        let pin = c.pin_open(&h1).unwrap();

        // `prune_to`: everything clean and unpinned goes, the pinned one
        // stays, and `used` still accounts for its bytes (which are still
        // on the disk — the point of pinning instead of unlinking under
        // the holder).
        let report = c.prune_to(0).unwrap();
        assert_eq!((report.freed_chunks, report.freed_bytes), (1, 100));
        assert!(c.contains(&h1) && !c.contains(&h2));
        assert_eq!(c.usage().used, 100);
        assert!(file_of(&dir, &h1).exists());

        // `plan_eviction`: an insert that needs the room cannot take it,
        // and fails whole rather than evicting the pinned chunk — exactly
        // what a `Pinned`/`Dirty` entry does (`pinned_and_dirty_not_evicted`).
        let (h3, d3) = chunk(3, 200);
        let err = c.insert(&h3, &d3, ChunkState::Clean).unwrap_err();
        assert!(matches!(err, CoreError::CacheFull { .. }));
        assert!(c.contains(&h1) && !c.contains(&h3));

        // The last holder closes: evictable again, by both paths.
        drop(pin);
        assert_eq!(c.open_pin_count(&h1), 0);
        c.insert(&h3, &d3, ChunkState::Clean).unwrap();
        assert!(!c.contains(&h1), "h1 should have made room for h3");
        assert!(!file_of(&dir, &h1).exists());
    }

    /// The count is the number of concurrent holders, and reaches zero
    /// only when the last one goes.
    #[test]
    fn open_pins_count_their_holders() {
        let dir = TempDir::new().unwrap();
        let c = Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap());
        let (h, d) = chunk(1, 4096);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        assert_eq!(c.open_pin_count(&h), 0);
        let a = c.pin_open(&h).unwrap();
        let b = c.pin_open(&h).unwrap();
        assert_eq!(c.open_pin_count(&h), 2);
        assert_eq!(a.hash(), h);
        drop(a);
        assert_eq!(c.open_pin_count(&h), 1);
        // Still held: a prune leaves it alone.
        c.prune_to(0).unwrap();
        assert!(c.contains(&h));
        drop(b);
        assert_eq!(c.open_pin_count(&h), 0);
        c.prune_to(0).unwrap();
        assert!(!c.contains(&h));
    }

    /// Nothing to hand out: the caller (plan 38 §3(c)'s `View::open`)
    /// falls back to serving the reads itself.
    #[test]
    fn pinning_a_chunk_that_is_not_cached_fails() {
        let dir = TempDir::new().unwrap();
        let c = Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap());
        let (h, _) = chunk(7, 4096);
        assert!(matches!(
            c.pin_open(&h),
            Err(CoreError::NotCached(hex)) if hex == h.to_hex()
        ));
        assert_eq!(c.open_pin_count(&h), 0);
        // And a pin taken, then dropped, leaves no entry behind in the
        // table either (a leaked count would make the chunk immortal).
        let (h2, d2) = chunk(8, 4096);
        c.insert(&h2, &d2, ChunkState::Clean).unwrap();
        drop(c.pin_open(&h2).unwrap());
        assert_eq!(c.open_pin_count(&h2), 0);
    }

    /// A pin names a chunk the caller is about to open by path, so
    /// `chunk_path` must be where the bytes are, and the entry's
    /// [`Resident`] facts must describe that file.
    #[test]
    fn a_pinned_chunks_path_and_resident_facts_describe_its_file() {
        let dir = TempDir::new().unwrap();
        let c = Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap());
        let (h, d) = chunk(4, 4096);
        assert_eq!(c.resident(&h), None);
        c.insert(&h, &d, ChunkState::Dirty).unwrap();
        let _pin = c.pin_open(&h).unwrap();
        assert_eq!(
            c.resident(&h),
            Some(Resident {
                len: 4096,
                state: ChunkState::Dirty,
                // `insert` hashed what it was handed (plan 38 §2.3).
                verified: true,
            })
        );
        assert_eq!(c.chunk_path(&h), file_of(&dir, &h));
        assert_eq!(fs::read(c.chunk_path(&h)).unwrap(), d);
    }

    // ---- verify-once (`CacheVerify`, `admit_verified`; plan 38 §2.3) ----

    /// Bytes a fetch already holds go to the memory tier without the
    /// whole-file read (and second hash) `load_shared` would do.
    #[test]
    fn fetched_bytes_are_admitted_without_reading_the_file_back() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20);
        let (h, d) = chunk(1, 4096);
        let mut spill = c.begin_spill().unwrap();
        spill.write_all(&d).unwrap();
        c.commit_spill(&h, spill, ChunkState::Clean).unwrap();
        c.admit_verified(&h, Bytes::from(d.clone()));
        assert!(c.memory_contains(&h));
        // Nothing was loaded from disk: no miss was counted, and the
        // read below is a hit on the admitted copy — which a file
        // clobbered afterwards proves, since a disk read would have
        // dropped this chunk as corrupt.
        assert_eq!(c.memory_stats().unwrap().misses, 0);
        fs::write(file_of(&dir, &h), b"garbage").unwrap();
        assert_eq!(c.get_shared(&h).unwrap().as_deref(), Some(&d[..]));
        let stats = c.memory_stats().unwrap();
        assert_eq!((stats.misses, stats.hits, stats.admissions), (0, 1, 1));
    }

    /// A chunk whose disk entry went while the fetch was in flight is not
    /// resurrected in memory (`load_shared`'s invariant, same check).
    #[test]
    fn admitting_a_removed_chunk_is_a_no_op() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20);
        let (h, d) = chunk(2, 4096);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        c.remove(&h).unwrap();
        c.admit_verified(&h, Bytes::from(d));
        assert!(!c.memory_contains(&h));
        assert_eq!(c.memory_stats().unwrap().entries, 0);
    }

    /// `commit_spill` marks what this process hashed; the startup scan
    /// does not. Under `admit` that is exactly the difference between a
    /// disk read that hashes and one that does not.
    #[test]
    fn a_startup_scanned_entry_is_hashed_on_its_first_read_only() {
        let dir = TempDir::new().unwrap();
        let (good, gd) = chunk(3, 4096);
        let (bad, bd) = chunk(4, 4096);
        {
            let c = DiskCache::open(dir.path(), 1 << 20).unwrap();
            c.insert(&good, &gd, ChunkState::Clean).unwrap();
            c.insert(&bad, &bd, ChunkState::Clean).unwrap();
            assert_eq!(c.is_verified(&good), Some(true));
        }
        // Corrupt one of the two files behind the cache's back, then
        // restart: the scan trusts neither.
        let mut rot = bd.clone();
        rot[7] ^= 0x80;
        fs::write(file_of(&dir, &bad), &rot).unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(0);
        assert_eq!(c.is_verified(&good), Some(false));
        assert_eq!(c.is_verified(&bad), Some(false));
        // The corrupt one is hashed, refused and dropped.
        assert_eq!(c.get(&bad).unwrap(), None);
        assert!(!c.contains(&bad));
        // The good one is hashed once, served, and marked.
        assert_eq!(c.get(&good).unwrap(), Some(gd.clone()));
        assert_eq!(c.is_verified(&good), Some(true));
        // Marked means not hashed again: rot it now and the next read
        // hands the bytes over as they are (the trust model of §2.3 —
        // `always` below is the mode that keeps checking).
        let mut rot = gd.clone();
        rot[9] ^= 0x80;
        fs::write(file_of(&dir, &good), &rot).unwrap();
        assert_eq!(c.get(&good).unwrap(), Some(rot));
        assert!(c.contains(&good));
    }

    #[test]
    fn always_rehashes_every_disk_read() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(0)
            .with_verify(CacheVerify::Always);
        assert_eq!(c.verify_mode(), CacheVerify::Always);
        let (h, d) = chunk(5, 4096);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        // Verified by this process, and re-hashed all the same: the
        // clobbered file is caught and dropped.
        assert_eq!(c.is_verified(&h), Some(true));
        assert_eq!(c.get(&h).unwrap(), Some(d.clone()));
        let mut rot = d.clone();
        rot[11] ^= 0x80;
        fs::write(file_of(&dir, &h), &rot).unwrap();
        assert_eq!(c.get(&h).unwrap(), None);
        assert!(!c.contains(&h));
        // And the same cache under `admit` (the default) would not have.
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(0);
        assert_eq!(c.verify_mode(), CacheVerify::Admit);
        c.insert(&h, &d, ChunkState::Clean).unwrap();
        fs::write(file_of(&dir, &h), &rot).unwrap();
        assert_eq!(c.get(&h).unwrap(), Some(rot));
    }

    /// `get_verified` is the read for bytes that are about to be
    /// published: it hashes under `admit` too, where the entry's
    /// `verified` bit would otherwise skip the pass — both for the disk
    /// copy and for a memory-resident one (which, under `admit`, may have
    /// been admitted from a disk read that itself skipped the hash).
    #[test]
    fn get_verified_hashes_even_what_admit_trusts() {
        let dir = TempDir::new().unwrap();
        let c = DiskCache::open(dir.path(), 1 << 20)
            .unwrap()
            .with_memory_cache(1 << 20);
        assert_eq!(c.verify_mode(), CacheVerify::Admit);
        let (h, d) = chunk(7, 4096);
        c.insert(&h, &d, ChunkState::Dirty).unwrap();
        assert_eq!(c.is_verified(&h), Some(true));
        assert_eq!(c.get_verified(&h).unwrap(), Some(d.clone()));

        // Rot the file. A plain `get` trusts it (§2.3's trade for
        // readers); `get_verified` drops it and reports it absent, which
        // is the "not in the cache" path the uploader took before `admit`.
        let mut rot = d.clone();
        rot[17] ^= 0x80;
        fs::write(file_of(&dir, &h), &rot).unwrap();
        assert_eq!(c.get_verified(&h).unwrap(), None);
        assert!(!c.contains(&h));

        // Same, with the bytes resident in the memory tier: a read admits
        // them, the file rots underneath, and the memory copy still holds
        // the truth — so this one is served, not dropped.
        c.insert(&h, &d, ChunkState::Dirty).unwrap();
        assert_eq!(c.get_shared(&h).unwrap(), Some(Bytes::from(d.clone())));
        assert!(c.memory_contains(&h));
        fs::write(file_of(&dir, &h), &rot).unwrap();
        assert_eq!(c.get_verified(&h).unwrap(), Some(d.clone()));
        assert!(c.contains(&h));

        // And a memory copy that does not match is dropped, falling
        // through to the (also rotted) disk copy: absent.
        let (h2, d2) = chunk(8, 4096);
        c.insert(&h2, &d2, ChunkState::Dirty).unwrap();
        let mut rot2 = d2.clone();
        rot2[3] ^= 0x80;
        fs::write(file_of(&dir, &h2), &rot2).unwrap();
        // Admit the rotted bytes to memory the way `admit` allows: the
        // entry is `verified`, so this disk read skips the hash.
        assert_eq!(c.get_shared(&h2).unwrap(), Some(Bytes::from(rot2)));
        assert!(c.memory_contains(&h2));
        assert_eq!(c.get_verified(&h2).unwrap(), None);
        assert!(!c.contains(&h2));
        assert!(!c.memory_contains(&h2));
    }

    #[test]
    fn verify_modes_parse() {
        assert_eq!(CacheVerify::parse("admit"), Some(CacheVerify::Admit));
        assert_eq!(CacheVerify::parse(" Always "), Some(CacheVerify::Always));
        assert_eq!(CacheVerify::parse("sometimes"), None);
        // Only the two documented names, no convenience aliases.
        assert_eq!(CacheVerify::parse("once"), None);
        assert_eq!(CacheVerify::parse("every"), None);
        assert_eq!(CacheVerify::default().as_str(), "admit");
        assert_eq!(CacheVerify::Always.to_string(), "always");
    }

    /// Eviction drops the victim's entry under the lock but used to unlink
    /// its file after releasing it; a dirty re-insert of the same hash in
    /// between had its fresh file deleted. Churn a tiny cache with clean
    /// fillers while other threads re-insert recently evicted hashes as
    /// dirty: every dirty chunk must stay readable.
    #[test]
    fn eviction_never_unlinks_a_reinserted_chunk() {
        let dir = TempDir::new().unwrap();
        // Room for ~16 chunks of 64 bytes.
        let c = std::sync::Arc::new(DiskCache::open(dir.path(), 16 * 64).unwrap());
        let shared = |i: u32| {
            let mut d = format!("shared {i} ").into_bytes();
            d.resize(64, b'x');
            d
        };
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let churners: Vec<_> = (0..4u32)
            .map(|t| {
                let c = c.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut i = 0u32;
                    while !stop.load(Ordering::Relaxed) {
                        // Clean copies of the shared set plus fillers keep
                        // the LRU turning over the very hashes the dirty
                        // writers insert.
                        let d = shared(i % 64);
                        let _ = c.insert(&ChunkHash::of(&d), &d, ChunkState::Clean);
                        let mut f = format!("filler {t} {i} ").into_bytes();
                        f.resize(64, b'y');
                        let _ = c.insert(&ChunkHash::of(&f), &f, ChunkState::Clean);
                        i = i.wrapping_add(1);
                    }
                })
            })
            .collect();
        let writers: Vec<_> = (0..2u32)
            .map(|w| {
                let c = c.clone();
                std::thread::spawn(move || {
                    for n in 0..300u32 {
                        let d = shared((n * 2 + w) % 64);
                        let h = ChunkHash::of(&d);
                        if c.insert(&h, &d, ChunkState::Dirty).is_err() {
                            continue; // full of dirty chunks: fine
                        }
                        assert_eq!(
                            c.get(&h).unwrap().as_deref(),
                            Some(&d[..]),
                            "a dirty chunk lost its file"
                        );
                        // "Uploaded": make it evictable again.
                        c.set_state(&h, ChunkState::Clean);
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        for t in churners {
            t.join().unwrap();
        }
        // Accounting matches the disk.
        for (hash, size, _) in c.entries() {
            let on_disk = fs::metadata(c.path_for(&hash)).map(|m| m.len());
            assert_eq!(
                on_disk.ok(),
                Some(size),
                "{hash:?} accounted but not on disk"
            );
        }
    }
}
