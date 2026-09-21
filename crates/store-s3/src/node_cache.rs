//! [`NodeCache`]: `mtree::NodeStore` over packs, the disk cache, and
//! peers (plan 28 §P5, §P8, §P11).
//!
//! This is the seam S2 drew the `NodeStore` trait for. Above it,
//! `mtree` is pure, synchronous and knows nothing about S3; below it,
//! everything is asynchronous, retrying and I/O-bound. A read resolves
//! down the same four-step ladder the data plane already uses for
//! chunks — **memory → disk cache → peer → S3** — and the disk tier is
//! `fs-core::cache` *verbatim*: the same LRU, the same
//! reserve-before-accept ENOSPC discipline, the same blake3
//! verify-and-drop on read. One cache, one eviction policy, one
//! verification path, one P2P transfer protocol for metadata and data
//! alike (§P5). A second, metadata-only cache with its own eviction
//! policy would be the wrong kind of new: two budgets to size, two
//! things to get wrong under memory pressure, and no shared benefit.
//!
//! ## Trust: `parse`, not `new`
//!
//! Every node that arrives from the disk cache, a peer or S3 goes
//! through [`NodeRef::parse`] — the O(entries) check — and never
//! through the O(1) constructor. These are bytes off a network, and the
//! key-*ordering* check is the one thing that cannot be done lazily: a
//! node whose keys are out of order decodes perfectly and merely makes
//! every binary search over it return the wrong answer. The hash check
//! comes first and catches most of it, but the hash only proves the
//! bytes are the ones that were named, not that whoever named them
//! built a well-formed node. Nodes this process just encoded (the
//! memory tier) are exempt: they came out of `mtree::encode` in this
//! address space.
//!
//! ## Missing concurrently
//!
//! §14.9 measured the miss path — a ranged read plus a zstd decode —
//! scaling near-linearly to 8 threads, so it must not be serialized.
//! No lock in this module is ever held across I/O: the maps are read,
//! copied out of, and released before the fetch starts.
//! `misses_are_concurrent` is a test that deadlocks rather than merely
//! slows down if that ever stops being true.
//!
//! The one deliberate cost is that two threads missing on the *same*
//! node both fetch it. Single-flight would need a per-hash wait map,
//! which is a lock on the hot path to save a duplicate ranged GET of an
//! 8 KiB node; the duplicate is cheap, converges (nodes are immutable),
//! and the disk cache dedupes the second insert.
//!
//! ## The memory tier holds the interior
//!
//! §14.1 measured the entire interior of a 35.8M-key filesystem at
//! 19.75 MiB, and §14.5 confirmed the resident cost of this design is
//! "~20 MiB of interior plus whatever leaf cache §14.2 says you must
//! buy". So the memory tier admits interior nodes only, under a byte
//! budget, and leaves live in the disk cache where the LRU already is.
//! Past the budget admission simply stops — degrading to the disk
//! cache, which is correct and merely slower — rather than growing a
//! second LRU next to the one `fs-core::cache` already implements.
//!
//! ## Writing: put, then seal, then commit
//!
//! [`NodeStore::put`] does not touch S3. It records the node as
//! *pending* and puts it in the disk cache as `Dirty`, which is exactly
//! the state `fs-core::cache` already means by that word: present,
//! not yet durable upstream, never evicted. [`NodeCache::seal_packs`]
//! is what makes a batch durable — it packs the pending nodes in key
//! order, PUTs them, registers their locations and demotes them to
//! `Clean`. Only then may the caller CAS-create the commit that names
//! them, which is the §P8/§P2 ordering invariant and the whole
//! crash-safety argument: see `commits.rs`.

use crate::error::StoreError;
use crate::packs::{build_packs, PackHash, PackIndex, PackNode, PackStore};
use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::ChunkHash;
use constellation_mtree::{Hasher, MtreeError, NodeHash, NodeRef, NodeStore};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Default byte budget for the resident interior tier. Three times
/// §14.1's 19.75 MiB census-scale interior, so the common case is
/// entirely resident with room for the retained-history nodes a
/// tailing reader briefly holds.
pub const DEFAULT_NODE_MEMORY_BYTES: u64 = 64 << 20;

/// Env `CONSTELLATION_NODE_MEMORY_BYTES`: byte budget for interior
/// nodes held in RAM. `0` disables the tier entirely (every read goes
/// to the disk cache), which is a legitimate configuration for a
/// memory-starved node and is what the partial-replica tests use.
pub fn node_memory_bytes() -> u64 {
    std::env::var("CONSTELLATION_NODE_MEMORY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_NODE_MEMORY_BYTES)
}

/// Where a node lives on the bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeLocation {
    pub pack: PackHash,
    pub level: u8,
    pub offset: u32,
    pub compressed_len: u32,
}

/// A peer that may already hold a metadata node (§P11).
///
/// Metadata nodes are content-addressed blobs like any chunk, so the
/// cooperative cache serves them with no new protocol — but the
/// cooperative cache lives in `cli`, above this crate, so the ladder
/// takes it as a hook rather than a dependency. Bytes a peer returns
/// are hash-checked and parsed here exactly like bytes from S3: a
/// lying peer is a failed read.
pub trait PeerNodeSource: Send + Sync {
    fn fetch(&self, hash: &NodeHash) -> Option<Vec<u8>>;
}

#[derive(Default)]
struct Counters {
    hits_memory: AtomicU64,
    hits_disk: AtomicU64,
    hits_peer: AtomicU64,
    pack_reads: AtomicU64,
    leaf_packs_touched: Mutex<HashSet<PackHash>>,
}

/// Read counters, for tests and for the metrics a node exports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeCacheStats {
    pub hits_memory: u64,
    pub hits_disk: u64,
    pub hits_peer: u64,
    /// Ranged GETs into packs — the §14.2 "pack reads per lookup"
    /// column.
    pub pack_reads: u64,
    /// Distinct packs a *leaf* read has touched. §14.2's
    /// "distinct packs per `ls -la`", which is the measurement the key
    /// encoding and the pack ordering exist to make equal 1.
    pub distinct_leaf_packs: usize,
}

struct MemoryTier {
    nodes: HashMap<NodeHash, Arc<[u8]>>,
    used: u64,
    budget: u64,
}

impl MemoryTier {
    fn admit(&mut self, hash: NodeHash, bytes: &Arc<[u8]>) {
        let size = bytes.len() as u64;
        if self.used + size > self.budget || self.nodes.contains_key(&hash) {
            return;
        }
        self.used += size;
        self.nodes.insert(hash, bytes.clone());
    }
}

/// `mtree::NodeStore` over `packs/*`, the local disk cache and peers.
pub struct NodeCache {
    packs: PackStore,
    cache: Arc<DiskCache>,
    hasher: Hasher,
    handle: tokio::runtime::Handle,
    peer: Option<Arc<dyn PeerNodeSource>>,
    memory: RwLock<MemoryTier>,
    locations: RwLock<HashMap<NodeHash, NodeLocation>>,
    /// Packs whose index has been attached, so a catalog refresh only
    /// fetches the indices it has not seen.
    attached: RwLock<HashSet<PackHash>>,
    /// Bumped by every catalog refresh; `refresh` is held across one. A miss
    /// notes the generation before it looks, and refreshes only if
    /// nobody else has since — so a burst of concurrent misses costs
    /// one LIST, not one each.
    refresh_generation: AtomicU64,
    refresh: Mutex<()>,
    pending: Mutex<Vec<PackNode>>,
    /// Packs a GC round has condemned (plan 28 S7b). `put` never treats a
    /// node located in one as already durable: the pack may be deleted
    /// before the commit that would name the node lands.
    condemned: RwLock<HashSet<PackHash>>,
    /// Packs `put` deduplicated against since [`NodeCache::start_dedup_log`]
    /// — what a publisher re-checks just before its commit CAS.
    deduped: Mutex<HashSet<PackHash>>,
    counters: Counters,
}

impl NodeCache {
    /// `cache` must hash the way `hasher` does — a plain [`DiskCache`]
    /// for [`Hasher::Plain`], a keyed one (`DiskCache::open_keyed`)
    /// under the same addressing key for [`Hasher::Keyed`]. The cache
    /// verifies every read against its own hash function, so a mismatch
    /// would not corrupt anything; it would make every disk hit a miss.
    /// This is not checkable from here (the cache does not expose its
    /// key, deliberately), so it is a documented precondition.
    ///
    /// `handle` is how the synchronous [`NodeStore`] side reaches the
    /// asynchronous object store. It is taken explicitly rather than
    /// grabbed from the ambient context so that a caller cannot
    /// accidentally bind a `NodeCache` to a current-thread runtime and
    /// deadlock on the first miss.
    pub fn new(
        packs: PackStore,
        cache: Arc<DiskCache>,
        hasher: Hasher,
        handle: tokio::runtime::Handle,
    ) -> NodeCache {
        NodeCache {
            packs,
            cache,
            hasher,
            handle,
            peer: None,
            memory: RwLock::new(MemoryTier {
                nodes: HashMap::new(),
                used: 0,
                budget: node_memory_bytes(),
            }),
            locations: RwLock::new(HashMap::new()),
            attached: RwLock::new(HashSet::new()),
            refresh_generation: AtomicU64::new(0),
            refresh: Mutex::new(()),
            pending: Mutex::new(Vec::new()),
            condemned: RwLock::new(HashSet::new()),
            deduped: Mutex::new(HashSet::new()),
            counters: Counters::default(),
        }
    }

    pub fn with_peer(mut self, peer: Arc<dyn PeerNodeSource>) -> NodeCache {
        self.peer = Some(peer);
        self
    }

    /// Override the resident-interior budget (tests, and a node that
    /// sizes it from measured RSS rather than from the env var).
    pub fn with_memory_budget(self, budget: u64) -> NodeCache {
        self.memory.write().expect("memory tier").budget = budget;
        self
    }

    pub fn packs(&self) -> &PackStore {
        &self.packs
    }

    pub fn stats(&self) -> NodeCacheStats {
        NodeCacheStats {
            hits_memory: self.counters.hits_memory.load(Ordering::Relaxed),
            hits_disk: self.counters.hits_disk.load(Ordering::Relaxed),
            hits_peer: self.counters.hits_peer.load(Ordering::Relaxed),
            pack_reads: self.counters.pack_reads.load(Ordering::Relaxed),
            distinct_leaf_packs: self
                .counters
                .leaf_packs_touched
                .lock()
                .expect("leaf packs")
                .len(),
        }
    }

    pub fn reset_stats(&self) {
        self.counters.hits_memory.store(0, Ordering::Relaxed);
        self.counters.hits_disk.store(0, Ordering::Relaxed);
        self.counters.hits_peer.store(0, Ordering::Relaxed);
        self.counters.pack_reads.store(0, Ordering::Relaxed);
        self.counters
            .leaf_packs_touched
            .lock()
            .expect("leaf packs")
            .clear();
    }

    /// Nodes written but not yet packed. Non-zero means a commit naming
    /// them would violate the ordering invariant.
    pub fn pending_nodes(&self) -> usize {
        self.pending.lock().expect("pending nodes").len()
    }

    // ------------------------------------------------------- locations

    /// Teach the cache where a pack's nodes live.
    pub fn attach_index(&self, pack: PackHash, index: &PackIndex) {
        self.attached.write().expect("attached packs").insert(pack);
        let mut locations = self.locations.write().expect("locations");
        for entry in &index.entries {
            locations.insert(
                entry.hash,
                NodeLocation {
                    pack,
                    level: entry.level,
                    offset: entry.offset,
                    compressed_len: entry.compressed_len,
                },
            );
        }
    }

    /// Fetch and attach the indices of `packs`, concurrently.
    ///
    /// This is how a reader that did not write the tree learns where
    /// anything is: a commit names its packs, S6's bootstrap walks the
    /// commit chain, and each index is a few KiB against a 1–16 MiB
    /// body. Idempotent.
    pub async fn load_pack_indices(&self, packs: &[PackHash]) -> Result<(), StoreError> {
        use futures::StreamExt;
        // `hash` is copied out rather than borrowed: an async block that
        // captures the iterator item by reference makes this closure —
        // and so this future — non-general over the item's lifetime,
        // which costs the future its `Send` bound at any caller that
        // spawns it.
        let mut fetched = futures::stream::iter(packs.iter().copied().map(|hash| {
            let store = self.packs.clone();
            async move { (hash, store.get_index(&hash).await) }
        }))
        .buffer_unordered(8);
        while let Some((hash, index)) = fetched.next().await {
            self.attach_index(hash, &index?);
        }
        Ok(())
    }

    /// LIST `packs/` and attach every index this cache has not seen.
    ///
    /// The complete answer to "where does this node live": a commit
    /// names only the packs it wrote, a tree mostly lives in its
    /// ancestors' packs, and after compaction some nodes live in packs
    /// no commit names at all. Readers call this once up front (S6's
    /// bootstrap, a restarted publisher), and [`NodeStore::get`] calls
    /// it on a miss — which is how a replica learns about packs another
    /// writer, or the compactor, produced after its last look. A pack
    /// that compaction replaced keeps its stale entries until the
    /// replacement's index overwrites them, which this does.
    ///
    /// Returns how many new indices were attached.
    pub async fn refresh_catalog(&self) -> Result<usize, StoreError> {
        use futures::TryStreamExt;
        let listed = self
            .packs
            .inner()
            .list(Some(&crate::layout::packs_prefix()))
            .try_collect::<Vec<_>>()
            .await?;
        let fresh: Vec<PackHash> = {
            let attached = self.attached.read().expect("attached packs");
            listed
                .iter()
                .filter_map(|meta| meta.location.filename()?.strip_suffix(".idx"))
                .filter_map(PackHash::from_hex)
                .filter(|hash| !attached.contains(hash))
                .collect()
        };
        self.load_pack_indices(&fresh).await?;
        Ok(fresh.len())
    }

    /// The miss path's refresh: at most one catalog load per burst of
    /// concurrent misses: whoever takes the lock first refreshes, and
    /// everyone who noted the same generation before missing just
    /// retries.
    fn refresh_after_miss(&self, seen: u64) -> bool {
        let _single_flight = self.refresh.lock().expect("catalog refresh");
        if self.refresh_generation.load(Ordering::Acquire) != seen {
            return true;
        }
        let refreshed = self.block_on(self.refresh_catalog()).is_ok();
        self.refresh_generation.fetch_add(1, Ordering::Release);
        refreshed
    }

    // ------------------------------------------------ GC handshake (S7b)

    /// Install the current condemned-pack list.
    pub fn set_condemned(&self, packs: HashSet<PackHash>) {
        *self.condemned.write().expect("condemned packs") = packs;
    }

    /// Begin recording which packs `put` deduplicates against.
    pub fn start_dedup_log(&self) {
        self.deduped.lock().expect("dedup log").clear();
    }

    /// Packs `put` has deduplicated against since the log started.
    pub fn deduped_packs(&self) -> Vec<PackHash> {
        self.deduped
            .lock()
            .expect("dedup log")
            .iter()
            .copied()
            .collect()
    }

    /// Drop every location in `packs`: they are condemned or gone, so a
    /// later `put` of one of their nodes must upload it again rather
    /// than trust a copy that will not (or does not) exist.
    pub fn forget_packs(&self, packs: &HashSet<PackHash>) {
        self.locations
            .write()
            .expect("locations")
            .retain(|_, location| !packs.contains(&location.pack));
        self.attached
            .write()
            .expect("attached packs")
            .retain(|pack| !packs.contains(pack));
    }

    /// Whether every pack deduplicated against since the log started is
    /// still safe to name: present on the bucket and not condemned. Any
    /// that is not is forgotten, so the caller's retry re-uploads.
    pub async fn dedup_is_sound(&self, condemned: &HashSet<PackHash>) -> Result<bool, StoreError> {
        let mut bad = HashSet::new();
        for pack in self.deduped_packs() {
            if condemned.contains(&pack) || !self.packs.contains(&pack).await? {
                bad.insert(pack);
            }
        }
        if bad.is_empty() {
            return Ok(true);
        }
        self.forget_packs(&bad);
        Ok(false)
    }

    pub fn location_of(&self, hash: &NodeHash) -> Option<NodeLocation> {
        self.locations.read().expect("locations").get(hash).copied()
    }

    // ----------------------------------------------------------- write

    /// Pack every pending node and make the packs durable.
    ///
    /// Returns the packs written, which is what a commit's `packs`
    /// field names. On success every node this cache has been handed is
    /// durable on the bucket, which is the precondition
    /// [`crate::commits::CommitChain::publish`] refuses to run without.
    ///
    /// On failure or cancellation the batch stays on the pending list, so a retry
    /// re-packs and re-PUTs. Packs that did land are content-addressed
    /// duplicates of what the retry produces, so the retry converges;
    /// any that the retry does not reproduce are orphans, which is
    /// exactly the state the crash-ordering argument permits and which
    /// S7's reachability sweep reclaims.
    pub async fn seal_packs(&self) -> Result<Vec<PackHash>, StoreError> {
        // Copied, not taken: a caller's future may be dropped at any
        // await below (the daemon cancels sync rounds), and a batch
        // taken up front would then vanish from the pending list while
        // never having been made durable. Nodes leave the list only once
        // their pack is on the bucket.
        let batch = self.pending.lock().expect("pending nodes").clone();
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let built = build_packs(batch.clone(), self.packs.target_bytes())?;
        for pack in &built {
            self.packs.put_pack(pack).await?;
        }
        // Durable: publish the locations and let the LRU have the bytes
        // back. Neither step can fail, so there is no window where a
        // node is packed but unreachable.
        let mut sealed = HashSet::new();
        for pack in &built {
            self.attach_index(pack.hash, &pack.index);
            for entry in pack.nodes() {
                self.cache
                    .set_state(&chunk_hash(&entry.hash), ChunkState::Clean);
                sealed.insert(entry.hash);
            }
        }
        self.pending
            .lock()
            .expect("pending nodes")
            .retain(|node| !sealed.contains(&node.hash));
        Ok(built.iter().map(|pack| pack.hash).collect())
    }

    // ------------------------------------------------------------ read

    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        // Called from a runtime worker (a FUSE request bridged through
        // `SyncHandle`, or a `spawn_blocking` build) as often as from a
        // plain thread, and `Handle::block_on` panics in the first case.
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| self.handle.block_on(fut)),
            Err(_) => self.handle.block_on(fut),
        }
    }

    /// Hash-check, structurally validate, and cache bytes that arrived
    /// from somewhere untrusted.
    fn accept(&self, hash: &NodeHash, bytes: Vec<u8>) -> Result<Arc<[u8]>, MtreeError> {
        if self.hasher.hash(&bytes) != *hash {
            return Err(MtreeError::Malformed(
                "node bytes do not hash to the node they were fetched as",
            ));
        }
        let node = NodeRef::parse(&bytes)?;
        let level = node.level();
        let bytes: Arc<[u8]> = bytes.into();
        let _ = self
            .cache
            .insert(&chunk_hash(hash), &bytes, ChunkState::Clean);
        if level > 0 {
            self.memory
                .write()
                .expect("memory tier")
                .admit(*hash, &bytes);
        }
        Ok(bytes)
    }
}

/// A node hash and a chunk hash are the same 32 bytes of blake3 over
/// the same plaintext, so `fs-core::cache` stores metadata nodes with
/// no adaptation at all — including its verify-and-drop read path,
/// which is why this is a reinterpretation and not a conversion with a
/// caveat. The types stay distinct above this line because the two
/// namespaces are swept by different rules (§P10).
fn chunk_hash(hash: &NodeHash) -> ChunkHash {
    ChunkHash(hash.0)
}

impl NodeStore for NodeCache {
    fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        if let Some(bytes) = self.memory.read().expect("memory tier").nodes.get(hash) {
            self.counters.hits_memory.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes.clone());
        }
        // The cache verifies blake3 itself and reports a corrupt file as
        // absent, so a hit here is already known to be the right bytes —
        // but not yet known to be a well-formed node, which is what
        // `accept` re-establishes. Cheap next to the read.
        if let Ok(Some(bytes)) = self.cache.get(&chunk_hash(hash)) {
            self.counters.hits_disk.fetch_add(1, Ordering::Relaxed);
            return self.accept(hash, bytes);
        }
        if let Some(peer) = &self.peer {
            if let Some(bytes) = peer.fetch(hash) {
                self.counters.hits_peer.fetch_add(1, Ordering::Relaxed);
                return self.accept(hash, bytes);
            }
        }
        // S3. The location lookup releases its lock before the GET, so
        // concurrent misses stay concurrent (§14.9).
        let seen = self.refresh_generation.load(Ordering::Acquire);
        if let Ok(bytes) = self.fetch_located(hash) {
            return Ok(bytes);
        }
        // Either no index this cache holds names the node, or the pack
        // it named is gone (compacted). Both are what a catalog refresh
        // repairs; one attempt, then the miss is real.
        if self.refresh_after_miss(seen) {
            return self.fetch_located(hash);
        }
        Err(MtreeError::MissingNode(*hash))
    }

    fn put(&self, hash: NodeHash, level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
        self.put_node(hash, level, bytes)
    }
}

impl NodeCache {
    /// The S3 rung of the ladder, from whatever location is known now.
    fn fetch_located(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        let Some(location) = self.location_of(hash) else {
            return Err(MtreeError::MissingNode(*hash));
        };
        self.counters.pack_reads.fetch_add(1, Ordering::Relaxed);
        if location.level == 0 {
            self.counters
                .leaf_packs_touched
                .lock()
                .expect("leaf packs")
                .insert(location.pack);
        }
        let bytes = self
            .block_on(self.packs.get_node_bytes(
                &location.pack,
                location.offset,
                location.compressed_len,
            ))
            .map_err(|_| MtreeError::MissingNode(*hash))?;
        self.accept(hash, bytes)
    }

    fn put_node(&self, hash: NodeHash, level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
        if let Some(location) = self.location_of(&hash) {
            // Already durable; nodes are immutable — unless a GC round
            // is about to delete the pack that holds it.
            if !self
                .condemned
                .read()
                .expect("condemned packs")
                .contains(&location.pack)
            {
                self.deduped
                    .lock()
                    .expect("dedup log")
                    .insert(location.pack);
                return Ok(());
            }
        }
        let node = PackNode::from_bytes(hash, bytes)?;
        debug_assert_eq!(
            node.level, level,
            "caller's level disagrees with the header"
        );
        let shared: Arc<[u8]> = node.bytes.clone().into();
        // `Dirty` in `fs-core::cache` means exactly what an unpacked
        // node is: present locally, not yet durable upstream, never
        // evicted. `seal_packs` demotes it to `Clean`.
        let _ = self
            .cache
            .insert(&chunk_hash(&hash), &shared, ChunkState::Dirty);
        if node.level > 0 {
            self.memory
                .write()
                .expect("memory tier")
                .admit(hash, &shared);
        }
        let mut pending = self.pending.lock().expect("pending nodes");
        if !pending.iter().any(|existing| existing.hash == hash) {
            pending.push(node);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_mtree::{Config, Tree};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::{Condvar, MutexGuard};
    use std::time::Duration;
    use tempfile::TempDir;

    struct Fixture {
        _dir: TempDir,
        cache: Arc<NodeCache>,
    }

    fn fixture(store: Arc<dyn ObjectStore>, pack_target: usize) -> Fixture {
        let dir = TempDir::new().unwrap();
        let disk = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let packs = PackStore::new(store).with_target_bytes(pack_target);
        let cache = NodeCache::new(
            packs,
            disk,
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        );
        Fixture {
            _dir: dir,
            cache: Arc::new(cache),
        }
    }

    /// A key set shaped like §P6's `0x02` dentry range: `dirs`
    /// directories of `per_dir` names each, so one directory is one
    /// contiguous key range.
    ///
    /// Values are deterministic but incompressible, standing in for the
    /// denormalized attr copy §P6 puts in a dentry. Compressible filler
    /// would let a whole test filesystem zstd down into a single pack
    /// and quietly make the locality assertions vacuous.
    fn dentries(dirs: u64, per_dir: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::with_capacity((dirs * per_dir) as usize);
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for dir in 0..dirs {
            for i in 0..per_dir {
                let mut key = vec![0x02u8];
                key.extend_from_slice(&dir.to_be_bytes());
                key.extend_from_slice(format!("file-{i:08}").as_bytes());
                let mut value = Vec::with_capacity(48);
                while value.len() < 48 {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    value.extend_from_slice(&state.to_le_bytes());
                }
                out.push((key, value));
            }
        }
        out
    }

    fn dir_prefix(dir: u64) -> Vec<u8> {
        let mut prefix = vec![0x02u8];
        prefix.extend_from_slice(&dir.to_be_bytes());
        prefix
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_tree_round_trips_through_packs_and_a_cold_cache() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = fixture(store.clone(), 1 << 20);
        let pairs = dentries(4, 200);
        let tree = Tree::new(writer.cache.clone());
        let root = tree.build(pairs.clone()).unwrap();
        assert!(writer.cache.pending_nodes() > 0);
        let packs = writer.cache.seal_packs().await.unwrap();
        assert!(!packs.is_empty());
        assert_eq!(writer.cache.pending_nodes(), 0);

        // A reader with nothing local: fresh disk cache, empty memory
        // tier, and only the commit's pack list to go on.
        let reader = fixture(store, 1 << 20);
        reader.cache.load_pack_indices(&packs).await.unwrap();
        let cold = Tree::new(reader.cache.clone());
        for (key, value) in &pairs {
            assert_eq!(cold.get(&root, key).unwrap().as_ref(), Some(value));
        }
        assert!(reader.cache.stats().pack_reads > 0);
    }

    /// §14.2's measured property, and the reason [`build_packs`] sorts
    /// by key: a cold directory scan touches one distinct pack.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_directory_is_one_pack() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // 256 KiB packs over 64 x 500 dentries: ~7 packs of leaves, and
        // a directory's ~20 KiB of leaves is a small fraction of one.
        let writer = fixture(store.clone(), 256 << 10);
        let dirs = 64u64;
        let per_dir = 500u64;
        let pairs = dentries(dirs, per_dir);
        let tree = Tree::new(writer.cache.clone());
        let root = tree.build(pairs.clone()).unwrap();
        let packs = writer.cache.seal_packs().await.unwrap();
        assert!(
            packs.len() >= 4,
            "the fixture must produce several packs or the property is vacuous: {}",
            packs.len()
        );

        let reader = fixture(store, 256 << 10);
        reader.cache.load_pack_indices(&packs).await.unwrap();
        let cold = Tree::new(reader.cache.clone());

        let mut one_pack = 0u64;
        let mut worst = 0usize;
        for dir in 0..dirs {
            let prefix = dir_prefix(dir);
            reader.cache.reset_stats();
            let scanned = cold
                .range(&root, &prefix, &prefix, per_dir as usize + 1)
                .unwrap();
            assert_eq!(scanned.len(), per_dir as usize);
            let touched = reader.cache.stats().distinct_leaf_packs;
            worst = worst.max(touched);
            if touched <= 1 {
                one_pack += 1;
            }
        }
        // Measured on this fixture: 60 of 64 directories are exactly
        // one pack and none is worse than two, which is §14.2's
        // "distinct packs per `ls -la` = 1" reproduced against the real
        // pack reader. The four stragglers are directories that happen
        // to straddle a pack seal, which is a function of where the
        // 256 KiB boundary falls and not of the ordering.
        assert!(
            one_pack * 10 >= dirs * 9,
            "only {one_pack}/{dirs} directories were one distinct pack"
        );
        assert!(
            worst <= 2,
            "a directory straddling {worst} packs means the key sort is not being applied"
        );

        // The control: the same number of dentries drawn across the
        // whole keyspace instead of from one directory, against an
        // equally cold reader. If this also came out at ~1 pack the
        // assertions above would be measuring nothing.
        let scattered = fixture(reader.cache.packs().inner(), 256 << 10);
        scattered.cache.load_pack_indices(&packs).await.unwrap();
        let scattered_tree = Tree::new(scattered.cache.clone());
        for dir in 0..dirs {
            let prefix = dir_prefix(dir);
            let _ = scattered_tree.range(&root, &prefix, &prefix, 8).unwrap();
        }
        assert!(
            scattered.cache.stats().distinct_leaf_packs >= 4,
            "a scattered read of the same {} dentries must touch many packs, not {}",
            dirs * 8,
            scattered.cache.stats().distinct_leaf_packs
        );
    }

    /// The store blocks every ranged GET until `width` of them are in
    /// flight at once. A `NodeCache` that serialized its miss path
    /// would never reach `width` and this test would fail on the
    /// timeout rather than pass slowly.
    #[derive(Debug)]
    struct RendezvousStore {
        inner: InMemory,
        width: usize,
        /// `(in flight, peak in flight)`.
        state: Mutex<(usize, usize)>,
        arrived: Condvar,
        /// Off during setup, so the single-threaded warm-up does not sit
        /// out the timeout waiting for peers that do not exist yet.
        armed: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for RendezvousStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RendezvousStore")
        }
    }

    impl RendezvousStore {
        fn new(width: usize) -> RendezvousStore {
            RendezvousStore {
                inner: InMemory::new(),
                width,
                state: Mutex::new((0, 0)),
                arrived: Condvar::new(),
                armed: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        fn peak(&self) -> usize {
            self.state.lock().unwrap().1
        }

        fn rendezvous(&self) {
            let mut state: MutexGuard<'_, (usize, usize)> = self.state.lock().unwrap();
            state.0 += 1;
            state.1 = state.1.max(state.0);
            if state.0 >= self.width {
                self.arrived.notify_all();
            } else {
                let (guard, _) = self
                    .arrived
                    .wait_timeout_while(state, Duration::from_secs(10), |s| s.0 < self.width)
                    .unwrap();
                state = guard;
            }
            state.0 -= 1;
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RendezvousStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.range.is_some() && self.armed.load(Ordering::SeqCst) {
                self.rendezvous();
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn misses_are_concurrent() {
        let width = 4usize;
        let rendezvous = Arc::new(RendezvousStore::new(width));
        let store = rendezvous.clone() as Arc<dyn ObjectStore>;
        let writer = fixture(store.clone(), 1 << 20);
        let pairs = dentries(width as u64, 400);
        let tree = Tree::new(writer.cache.clone());
        let root = tree.build(pairs.clone()).unwrap();
        let packs = writer.cache.seal_packs().await.unwrap();

        let reader = fixture(store, 1 << 20);
        reader.cache.load_pack_indices(&packs).await.unwrap();
        // Warm the interior so the parallel readers miss on leaves and
        // not on a shared root.
        let warm = Tree::new(reader.cache.clone());
        warm.get(&root, &pairs[0].0).unwrap();
        rendezvous.arm();

        let mut threads = Vec::new();
        for i in 0..width {
            let cache = reader.cache.clone();
            let key = pairs[i * 400 + 399].0.clone();
            threads.push(std::thread::spawn(move || {
                let tree = Tree::new(cache);
                tree.get(&root, &key).unwrap()
            }));
        }
        for thread in threads {
            assert!(thread.join().unwrap().is_some());
        }
        assert_eq!(
            rendezvous.peak(),
            width,
            "the miss path serialized: {width} concurrent readers never overlapped"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_that_does_not_hash_to_its_name_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let f = fixture(store, 1 << 20);
        let tree = Tree::new(f.cache.clone());
        let root = tree.build(dentries(1, 10)).unwrap();
        f.cache.seal_packs().await.unwrap();

        // Point a fabricated hash at the root's location: the bytes are
        // well-formed but they are not the bytes that were asked for.
        let location = f.cache.location_of(&root).unwrap();
        let liar = NodeHash([0xab; 32]);
        f.cache.locations.write().unwrap().insert(liar, location);
        f.cache.memory.write().unwrap().nodes.clear();
        assert!(matches!(
            f.cache.get(&liar),
            Err(MtreeError::Malformed(_)) | Err(MtreeError::MissingNode(_))
        ));
    }

    /// Bytes off a network get the O(entries) check, not the O(1)
    /// constructor: a node whose keys are out of order decodes fine and
    /// would silently break every binary search over it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn out_of_order_entries_from_a_peer_are_refused() {
        use constellation_mtree::{node, Entry};

        struct LyingPeer(Vec<u8>);
        impl PeerNodeSource for LyingPeer {
            fn fetch(&self, _hash: &NodeHash) -> Option<Vec<u8>> {
                Some(self.0.clone())
            }
        }

        let bytes = node::encode(
            0,
            &[Entry::leaf(b"b".to_vec(), b"1"), Entry::leaf(b"a", b"2")],
        );
        let hash = Hasher::Plain.hash(&bytes);
        let dir = TempDir::new().unwrap();
        let disk = Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap());
        let packs = PackStore::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>);
        let cache = NodeCache::new(
            packs,
            disk,
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        )
        .with_peer(Arc::new(LyingPeer(bytes)));
        // The hash matches — this is genuinely the node that was named —
        // so only the ordering pass can reject it.
        assert!(matches!(cache.get(&hash), Err(MtreeError::Malformed(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_ladder_prefers_memory_then_disk_then_peer() {
        struct CountingPeer(AtomicU64);
        impl PeerNodeSource for CountingPeer {
            fn fetch(&self, _hash: &NodeHash) -> Option<Vec<u8>> {
                self.0.fetch_add(1, Ordering::Relaxed);
                None
            }
        }

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let disk = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let peer = Arc::new(CountingPeer(AtomicU64::new(0)));
        let cache = Arc::new(
            NodeCache::new(
                PackStore::new(store).with_target_bytes(1 << 20),
                disk,
                Hasher::Plain,
                tokio::runtime::Handle::current(),
            )
            .with_peer(peer.clone()),
        );
        let tree = Tree::with_config(cache.clone(), Config::default()).unwrap();
        let pairs = dentries(1, 400);
        let root = tree.build(pairs.clone()).unwrap();
        cache.seal_packs().await.unwrap();

        cache.reset_stats();
        // The root is interior, so it is resident.
        tree.get(&root, &pairs[0].0).unwrap();
        let stats = cache.stats();
        assert!(stats.hits_memory > 0, "{stats:?}");
        // Leaves come off the disk cache, which the write path filled,
        // so neither the peer nor S3 is consulted.
        assert!(stats.hits_disk > 0, "{stats:?}");
        assert_eq!(stats.pack_reads, 0, "{stats:?}");
        assert_eq!(peer.0.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_with_no_known_location_is_missing_not_a_hang() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let f = fixture(store, 1 << 20);
        assert!(matches!(
            f.cache.get(&NodeHash([5u8; 32])),
            Err(MtreeError::MissingNode(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sealing_twice_is_a_no_op() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let f = fixture(store, 1 << 20);
        let tree = Tree::new(f.cache.clone());
        tree.build(dentries(1, 50)).unwrap();
        assert!(!f.cache.seal_packs().await.unwrap().is_empty());
        assert!(f.cache.seal_packs().await.unwrap().is_empty());
    }
}
