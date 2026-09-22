//! The reachability mark: which metadata nodes, and therefore which
//! packs, are live (plan 28 §P10, step S7a).
//!
//! This is the half of metadata GC that decides *what* is garbage. The
//! other half — deleting and rewriting packs — is `compact.rs`, and the
//! policy above both (retention, scheduling, the rate budget, the
//! condemned-list handshake) is S7b's, in `cli`. Nothing in this module
//! deletes anything.
//!
//! ## Why this replaces a refcount table
//!
//! Chunk GC (`crate::gc`, `cli::gc`) used to work from SQLite's
//! continuously maintained `deref` index: every dereference was written
//! down as it happened, and GC read the table. That was a second source
//! of truth about liveness, maintained by a different code path than
//! the one that creates the references, and keeping the two agreeing is
//! where plan 26's finding 6 lived. The tree needs none of it. A commit
//! *is* the complete state, so liveness is a graph walk from the root
//! set — the newest commit, the retained window, every `snaps/*`, every
//! clone, every unexpired `holds/*` (§P10). The bucket is authoritative
//! and no bookkeeping can drift from it.
//!
//! **Plan 29 M0c retired the `deref` table and the `superseded-checkpoint`
//! rule**, now that this module and S7b's `cli::gc` wiring are the sole
//! source of chunk-GC candidates (the orphan LIST pass) and metadata GC
//! (this mark + `compact.rs`).
//!
//! ## The one property that makes GC affordable
//!
//! Consecutive commits share almost all of their nodes, so marking a
//! retained window of N commits must cost O(their differences) and not
//! O(N × state). The walk gets that for free from content addressing:
//! a node already in `seen` is not descended into, and an unchanged
//! subtree has the same hash in every commit that contains it, so it is
//! entered exactly once across the whole root set. This is the same
//! structural fact §14.6 measured for `diff` (a one-key diff of a
//! 35.8M-key tree costs 20 node reads), applied to the walk.
//!
//! [`Mark::node_visits`] exists so that a test can *assert* it rather
//! than assume it — see `marking_a_chain_costs_the_differences`, which
//! fails if a future change ever makes the walk re-enter shared
//! subtrees.
//!
//! ## Parallel, because §14.9 says it pays
//!
//! §14.9 measured the mark at 8.8 s single-threaded and 2.1 s at 8
//! threads (4.16×) against a dirty census-scale tip, so [`mark`] runs a
//! work-stealing frontier over a private rayon pool. The frontier is
//! level-synchronous: every node in a frontier sits at the same level
//! of the tree, so there is no straggler to wait on, and the only
//! serial step is inserting the next level's child hashes into the
//! `seen` set — hashing, no I/O. Deduplication has to be serial
//! somewhere, because "have I already visited this hash" is the
//! termination condition, and doing it once per level over an
//! already-gathered vector is cheaper than a shared concurrent set
//! consulted per edge.
//!
//! Nodes are read through [`NodeRef::new`], not `parse`: whatever
//! [`NodeStore`] the caller supplies is responsible for its own trust
//! boundary, and `NodeCache` already hash-checks and structurally
//! validates every byte that arrives from a pack, a peer or the disk
//! cache. Re-validating here would double the cost of the walk to
//! re-check bytes this process just verified. The consequence, stated
//! plainly: **a mark is only as sound as the store's verification.**
//! Hand it an unverified store and a corrupt interior node can hide a
//! live subtree, which is the one corruption that loses data rather
//! than leaking space. That is why `NodeCache` parses.
//!
//! ## What `PackCatalog` is for
//!
//! A mark produces node hashes; a sweep needs pack names. The mapping
//! lives in the `packs/*.idx` siblings and nowhere else — deliberately,
//! because a pack outlives the commit that wrote it (§P8), so there is
//! no commit whose `packs` field enumerates everything a reader might
//! resolve. [`PackCatalog`] is therefore one LIST of `packs/` plus a
//! concurrent fetch of every index, and it is both how the walk learns
//! where nodes are and how the sweep learns which packs a live node
//! keeps alive.

use crate::commits::CommitChain;
use crate::error::StoreError;
use crate::layout;
use crate::node_cache::NodeCache;
use crate::packs::{PackHash, PackIndex, PackStore};
use constellation_mtree::keys::{RANGE_INODE, RANGE_XATTR};
use constellation_mtree::record::{InodeRecord, Payload};
use constellation_mtree::{BlobHash, NodeHash, NodeRef, NodeStore};
use futures::TryStreamExt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// One pack as the catalog sees it: its index, and the size of its body
/// on the bucket.
#[derive(Clone, Debug)]
pub struct CatalogPack {
    pub index: PackIndex,
    /// Sealed body size from the LIST, so a sweep can report bytes
    /// reclaimed without a second HEAD per pack.
    pub body_bytes: u64,
}

/// Every pack on the bucket and every node it holds.
#[derive(Clone, Debug, Default)]
pub struct PackCatalog {
    packs: BTreeMap<PackHash, CatalogPack>,
    owner: HashMap<NodeHash, PackHash>,
    incomplete: Vec<PackHash>,
}

impl PackCatalog {
    /// LIST `packs/` and fetch every index.
    ///
    /// A body whose `.idx` sibling is missing is **not** entered in the
    /// catalog and **not** reported as dead. It is the exact state a
    /// crash between [`PackStore::put_pack`]'s two PUTs leaves, and
    /// also the state a writer is in for a few milliseconds during a
    /// perfectly healthy seal. It holds no resolvable node, so it costs
    /// only space — and deleting it needs the same age horizon and
    /// condemned-list handshake today's chunk orphan pass uses, which
    /// is policy and therefore S7b's. [`PackCatalog::incomplete`]
    /// reports them so that policy has something to work from.
    pub async fn load(packs: &PackStore) -> Result<PackCatalog, StoreError> {
        use futures::StreamExt;

        let listed = packs
            .inner()
            .list(Some(&layout::packs_prefix()))
            .try_collect::<Vec<_>>()
            .await?;
        let mut bodies: BTreeMap<PackHash, u64> = BTreeMap::new();
        let mut indexed: HashSet<PackHash> = HashSet::new();
        for meta in &listed {
            let Some(name) = meta.location.filename() else {
                continue;
            };
            match name.strip_suffix(".idx") {
                Some(stem) => {
                    if let Some(hash) = PackHash::from_hex(stem) {
                        indexed.insert(hash);
                    }
                }
                None => {
                    if let Some(hash) = PackHash::from_hex(name) {
                        bodies.insert(hash, meta.size);
                    }
                }
            }
        }

        let complete: Vec<(PackHash, u64)> = bodies
            .iter()
            .filter(|(hash, _)| indexed.contains(hash))
            .map(|(hash, size)| (*hash, *size))
            .collect();
        let incomplete: Vec<PackHash> = bodies
            .keys()
            .filter(|hash| !indexed.contains(hash))
            .copied()
            .collect();

        let mut fetched = futures::stream::iter(complete.into_iter().map(|(hash, size)| {
            let store = packs.clone();
            async move { (hash, size, store.get_index(&hash).await) }
        }))
        .buffer_unordered(16);

        let mut catalog = PackCatalog {
            incomplete,
            ..PackCatalog::default()
        };
        while let Some((hash, body_bytes, index)) = fetched.next().await {
            catalog.insert(hash, index?, body_bytes);
        }
        Ok(catalog)
    }

    pub fn insert(&mut self, hash: PackHash, index: PackIndex, body_bytes: u64) {
        for entry in &index.entries {
            // A node may sit in more than one pack: a crash mid-rewrite
            // leaves the original and its replacement both holding it,
            // which the ordering invariant permits on purpose. Either
            // copy resolves, so the first one wins and the other stays
            // discoverable through `packs_holding`.
            self.owner.entry(entry.hash).or_insert(hash);
        }
        self.packs.insert(hash, CatalogPack { index, body_bytes });
    }

    pub fn len(&self) -> usize {
        self.packs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }

    /// Ascending by pack hash — the order a restartable sweep cursor
    /// walks, since it is stable across runs and independent of LIST
    /// ordering.
    pub fn iter(&self) -> impl Iterator<Item = (&PackHash, &CatalogPack)> {
        self.packs.iter()
    }

    pub fn get(&self, pack: &PackHash) -> Option<&CatalogPack> {
        self.packs.get(pack)
    }

    /// Pack bodies with no `.idx` sibling. Space, not garbage a sweep
    /// may delete on its own authority — see [`PackCatalog::load`].
    pub fn incomplete(&self) -> &[PackHash] {
        &self.incomplete
    }

    pub fn total_body_bytes(&self) -> u64 {
        self.packs.values().map(|pack| pack.body_bytes).sum()
    }

    /// Which packs hold `node`. More than one after an interrupted
    /// compaction.
    pub fn packs_holding(&self, node: &NodeHash) -> Vec<PackHash> {
        self.packs
            .iter()
            .filter(|(_, pack)| pack.index.entries.iter().any(|e| e.hash == *node))
            .map(|(hash, _)| *hash)
            .collect()
    }

    /// Teach a [`NodeCache`] every location the catalog knows, which is
    /// what lets a marker resolve a node whose pack was written by a
    /// commit that retention has already deleted.
    pub fn attach_to(&self, cache: &NodeCache) {
        for (hash, pack) in &self.packs {
            cache.attach_index(*hash, &pack.index);
        }
    }

    /// The packs a live node set keeps alive.
    pub fn live_packs(&self, live: &HashSet<NodeHash>) -> HashSet<PackHash> {
        live.iter()
            .filter_map(|node| self.owner.get(node).copied())
            .collect()
    }
}

/// The result of a walk.
#[derive(Clone, Debug, Default)]
pub struct Mark {
    /// Every node hash reachable from the root set.
    pub nodes: HashSet<NodeHash>,
    /// Nodes fetched and decoded. Equal to `nodes.len()` for a walk
    /// that terminates on shared subtrees as it must, which is what
    /// makes this the number a test asserts on rather than a metric.
    pub node_visits: u64,
    /// Tree depth reached, for a report. `0` for an empty root set.
    pub levels: usize,
    /// Every blob hash reachable through a `0x01`/`0x03` leaf value's
    /// `Payload::Spilled` (plan 29 M3a `blobs/` GC). The module doc
    /// above named this gap when S7a landed structural marking only;
    /// this is that walk extended to also decode the leaf values it was
    /// already fetching, rather than a second pass over the tree.
    pub blob_hashes: HashSet<BlobHash>,
}

/// Pull any `Payload::Spilled` hash out of a `0x01`/`0x03` leaf entry.
/// Every other range's leaf value never holds a blob reference (§P6: a
/// `0x02` dentry's attrs are copied inline, never spilled; `0x04` and
/// `0x30` carry no `Payload` at all), so this is a no-op for them.
///
/// A decode failure here is bucket corruption, not a caller error — the
/// node itself was already hash-verified by the `NodeStore`, so a value
/// that does not parse as its range's format is exactly the "wrong
/// answer" a verified store must never hand back silently.
fn collect_spilled_blobs(
    key: &[u8],
    value: &[u8],
    out: &mut Vec<BlobHash>,
) -> Result<(), StoreError> {
    match key.first().copied() {
        Some(RANGE_INODE) => {
            let record = InodeRecord::decode(value)
                .map_err(|e| StoreError::CorruptObject(format!("0x01 leaf value: {e}")))?;
            if let Some(Payload::Spilled(hash)) = record.manifest {
                out.push(hash);
            }
            if let Some(Payload::Spilled(hash)) = record.symlink_target {
                out.push(hash);
            }
        }
        Some(RANGE_XATTR) => {
            let payload = Payload::decode(value)
                .map_err(|e| StoreError::CorruptObject(format!("0x03 leaf value: {e}")))?;
            if let Payload::Spilled(hash) = payload {
                out.push(hash);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Every node reachable from `roots`, walked over `threads` workers
/// (`0` = one per core).
///
/// Termination is by hash: a subtree shared between two roots is
/// entered once. See the module docs for why that is the whole
/// affordability argument, and [`Mark::node_visits`] for how it is
/// tested.
pub fn mark<S>(store: &S, roots: &[NodeHash], threads: usize) -> Result<Mark, StoreError>
where
    S: NodeStore + Sync,
{
    use rayon::prelude::*;

    let pool = crate::parallel::thread_pool(threads)?;
    let mut seen: HashSet<NodeHash> = HashSet::new();
    let mut frontier: Vec<NodeHash> = Vec::new();
    for root in roots {
        if seen.insert(*root) {
            frontier.push(*root);
        }
    }
    let visits = AtomicU64::new(0);
    let mut levels = 0usize;
    let mut blob_hashes: HashSet<BlobHash> = HashSet::new();

    while !frontier.is_empty() {
        levels += 1;
        let results: Vec<(Vec<NodeHash>, Vec<BlobHash>)> = pool.install(|| {
            frontier
                .par_iter()
                .map(
                    |hash| -> Result<(Vec<NodeHash>, Vec<BlobHash>), StoreError> {
                        let buf = store.get(hash)?;
                        visits.fetch_add(1, Ordering::Relaxed);
                        let node = NodeRef::new(&buf)?;
                        if node.is_leaf() {
                            let mut blobs = Vec::new();
                            for i in 0..node.count() {
                                collect_spilled_blobs(
                                    node.key(i)?,
                                    node.leaf_value(i)?,
                                    &mut blobs,
                                )?;
                            }
                            return Ok((Vec::new(), blobs));
                        }
                        let mut children = Vec::with_capacity(node.count());
                        for i in 0..node.count() {
                            children.push(node.child(i)?.0);
                        }
                        Ok((children, Vec::new()))
                    },
                )
                .collect::<Result<Vec<_>, StoreError>>()
        })?;
        frontier = Vec::new();
        for (children, blobs) in results {
            for child in children {
                if seen.insert(child) {
                    frontier.push(child);
                }
            }
            blob_hashes.extend(blobs);
        }
    }

    Ok(Mark {
        node_visits: visits.load(Ordering::Relaxed),
        nodes: seen,
        levels,
        blob_hashes,
    })
}

/// Everything a sweep needs: the live nodes, the live packs, and the
/// catalog the classification is computed against.
pub struct LiveSet {
    pub mark: Mark,
    pub live_packs: HashSet<PackHash>,
    pub catalog: PackCatalog,
    /// Root commits that were named but do not exist. Retention having
    /// deleted a commit out from under a caller's root list is normal;
    /// the caller decides whether it is an error, because "the newest
    /// commit vanished" and "a retained commit aged out mid-run" are
    /// the same observation with very different meanings.
    pub missing_roots: Vec<u64>,
}

/// Mark from a root set given as commit sequence numbers (§P10's
/// "newest commit, retained window, snaps, clones, holds" — S7b
/// resolves those to sequence numbers; this resolves sequence numbers
/// to nodes).
///
/// Loads the pack catalog first and attaches it to `cache`, because a
/// commit's own `packs` field names only the packs *that* commit
/// created and the walk will immediately need its ancestors'.
///
/// The walk itself runs on [`tokio::task::spawn_blocking`]: it is
/// seconds of CPU (§14.9: 8.8 s single-threaded against a dirty
/// census-scale tip) and must not sit on a runtime worker.
pub async fn live_set(
    chain: &CommitChain,
    cache: &Arc<NodeCache>,
    roots: &[u64],
    threads: usize,
) -> Result<LiveSet, StoreError> {
    let catalog = PackCatalog::load(cache.packs()).await?;
    catalog.attach_to(cache);

    let mut node_roots: Vec<NodeHash> = Vec::new();
    let mut missing_roots: Vec<u64> = Vec::new();
    for seq in roots {
        match chain.get(*seq).await? {
            Some(commit) => {
                for shard in commit.roots.keys() {
                    let root = commit.root(shard).ok_or_else(|| {
                        StoreError::CorruptObject(format!(
                            "commit {seq} shard {shard} has an unreadable root hash"
                        ))
                    })?;
                    node_roots.push(root);
                }
            }
            None => missing_roots.push(*seq),
        }
    }

    let walker = cache.clone();
    let marked = tokio::task::spawn_blocking(move || mark(&walker, &node_roots, threads))
        .await
        .map_err(|e| StoreError::Parallel(e.to_string()))??;
    let live_packs = catalog.live_packs(&marked.nodes);
    Ok(LiveSet {
        mark: marked,
        live_packs,
        catalog,
        missing_roots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commits::{CommitPayload, SHARD0};
    use crate::packs::PackStore;
    use constellation_fs_core::cache::DiskCache;
    use constellation_mtree::{Hasher, MemoryNodeStore, Tree};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use tempfile::TempDir;

    /// Values are pseudorandom, not constant: compressible filler lets
    /// a whole test filesystem zstd into a single pack, which would
    /// make every "several packs" assertion below vacuous.
    fn pairs(range: std::ops::Range<u64>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        range
            .map(|i| {
                let mut key = vec![0x02u8];
                key.extend_from_slice(&i.to_be_bytes());
                let mut value = Vec::with_capacity(48);
                while value.len() < 48 {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    value.extend_from_slice(&state.to_le_bytes());
                }
                (key, value)
            })
            .collect()
    }

    /// §P10's affordability claim, as an assertion rather than a
    /// comment: marking a chain of commits costs the *differences*
    /// between them, not the state times the chain length.
    ///
    /// The proof is exact, not statistical. Every node the chain adds
    /// is a distinct content address, and `MemoryNodeStore` counts
    /// distinct writes, so "nodes reachable from all 17 roots" is
    /// knowable independently of the walk: it is the first tree's node
    /// count plus every node the 16 later commits introduced. If the
    /// walk re-entered shared subtrees, `node_visits` would exceed
    /// `nodes.len()` — and if it re-walked each root from scratch it
    /// would be ~17× the first mark.
    #[test]
    fn marking_a_chain_costs_the_differences() {
        let store = MemoryNodeStore::new();
        let tree = Tree::new(&store);
        let base = tree.build(pairs(0..60_000)).unwrap();

        let first = mark(&&store, &[base], 1).unwrap();
        let base_nodes = first.nodes.len() as u64;
        assert_eq!(
            first.node_visits, base_nodes,
            "a single-root walk must visit each node once"
        );
        assert!(base_nodes > 150, "{base_nodes} nodes is too small a tree");

        // Sixteen commits, four keys changed each: a retained window
        // of the shape §P10b bounds.
        let chain_len = 16u64;
        let mut roots = vec![base];
        let mut head = base;
        let mut introduced = 0u64;
        for commit in 0..chain_len {
            store.reset_counters();
            let mut edits: Vec<_> = (0..4u64)
                .map(|i| {
                    let mut key = vec![0x02u8];
                    key.extend_from_slice(&((commit * 3_571 + i * 7_919) % 60_000).to_be_bytes());
                    (key, Some(vec![0xee, commit as u8, i as u8]))
                })
                .collect();
            edits.sort();
            head = tree.apply(&head, &edits).unwrap();
            introduced += store.distinct_writes();
            roots.push(head);
        }
        assert!(introduced > 0);

        let all = mark(&&store, &roots, 1).unwrap();
        println!(
            "1 root: {base_nodes} nodes, {} visits; {} roots: {} nodes, {} visits \
             ({introduced} nodes introduced by the chain)",
            first.node_visits,
            roots.len(),
            all.nodes.len(),
            all.node_visits,
        );
        assert_eq!(
            all.nodes.len() as u64,
            base_nodes + introduced,
            "the reachable set must be the base tree plus exactly the nodes the chain added"
        );
        assert_eq!(
            all.node_visits,
            all.nodes.len() as u64,
            "a shared subtree was entered more than once"
        );
        // The whole point, stated as the inequality that fails if the
        // walk ever becomes O(N x state): 17 roots cost far less than
        // twice one root, let alone 17 times.
        assert!(
            all.node_visits < base_nodes * 2,
            "{} visits for {} roots against {base_nodes} for one — \
             the walk is not terminating on shared subtrees",
            all.node_visits,
            roots.len()
        );
        // And the differences really are small, so the assertion above
        // is not passing because the deltas happen to be huge.
        assert!(
            introduced * 4 < base_nodes,
            "{introduced} nodes introduced against a {base_nodes}-node tree \
             makes this test vacuous"
        );
    }

    #[test]
    fn a_wider_pool_marks_the_same_set() {
        let store = MemoryNodeStore::new();
        let tree = Tree::new(&store);
        let root = tree.build(pairs(0..30_000)).unwrap();
        let one = mark(&&store, &[root], 1).unwrap();
        let many = mark(&&store, &[root], 8).unwrap();
        assert_eq!(one.nodes, many.nodes);
        assert_eq!(one.node_visits, many.node_visits);
        assert_eq!(one.levels, many.levels);
        assert!(one.levels >= 2, "a 30k-key tree must have an interior");
    }

    #[test]
    fn an_empty_root_set_marks_nothing() {
        let store = MemoryNodeStore::new();
        let marked = mark(&&store, &[], 4).unwrap();
        assert!(marked.nodes.is_empty());
        assert_eq!(marked.node_visits, 0);
        assert_eq!(marked.levels, 0);
    }

    #[test]
    fn a_root_that_is_not_in_the_store_is_an_error_not_a_partial_answer() {
        let store = MemoryNodeStore::new();
        let err = mark(&&store, &[NodeHash([3u8; 32])], 2).unwrap_err();
        assert!(matches!(err, StoreError::Node(_)), "{err}");
    }

    struct Fixture {
        _dir: TempDir,
        cache: Arc<NodeCache>,
        chain: CommitChain,
    }

    fn fixture(store: Arc<dyn ObjectStore>, target: usize) -> Fixture {
        let dir = TempDir::new().unwrap();
        let disk = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let cache = Arc::new(NodeCache::new(
            PackStore::new(store.clone()).with_target_bytes(target),
            disk,
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ));
        Fixture {
            _dir: dir,
            cache,
            chain: CommitChain::new(store),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_catalog_finds_every_pack_and_its_nodes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let f = fixture(store.clone(), 64 << 10);
        let tree = Tree::new(f.cache.clone());
        let root = tree.build(pairs(0..4_000)).unwrap();
        let written = f.cache.seal_packs().await.unwrap();
        assert!(written.len() > 2, "{} packs", written.len());

        let catalog = PackCatalog::load(f.cache.packs()).await.unwrap();
        assert_eq!(catalog.len(), written.len());
        assert!(catalog.incomplete().is_empty());
        assert!(catalog.total_body_bytes() > 0);

        // A reader with nothing local resolves the whole tree from the
        // catalog alone — no commit consulted, which is §P8's point.
        let reader = fixture(store, 64 << 10);
        catalog.attach_to(&reader.cache);
        let marked = mark(&reader.cache.clone(), &[root], 4).unwrap();
        assert_eq!(catalog.live_packs(&marked.nodes).len(), written.len());
    }

    /// A pack body whose index PUT never landed must not be catalogued
    /// and must not be called dead: it is the healthy mid-seal state as
    /// well as the crash state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_body_without_an_index_is_reported_not_catalogued() {
        use object_store::{ObjectStoreExt, PutPayload};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let f = fixture(store.clone(), 1 << 20);
        let tree = Tree::new(f.cache.clone());
        tree.build(pairs(0..500)).unwrap();
        f.cache.seal_packs().await.unwrap();

        let orphan = PackHash([0x5a; 32]);
        store
            .put(
                &layout::pack(&orphan.to_hex()),
                PutPayload::from(b"half-written".to_vec()),
            )
            .await
            .unwrap();
        let catalog = PackCatalog::load(f.cache.packs()).await.unwrap();
        assert!(catalog.get(&orphan).is_none());
        assert_eq!(catalog.incomplete(), &[orphan]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_set_marks_from_commit_sequence_numbers() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let f = fixture(store.clone(), 64 << 10);
        let tree = Tree::new(f.cache.clone());

        let mut head = tree.build(pairs(0..3_000)).unwrap();
        let mut packs = f.cache.seal_packs().await.unwrap();
        let first = f
            .chain
            .publish(
                &f.cache,
                0,
                CommitPayload::single_root(head, packs),
                |p, _| Ok(p),
                2,
            )
            .await
            .unwrap();

        head = tree
            .apply(&head, &[(vec![0x02u8, 9, 9], Some(b"new".to_vec()))])
            .unwrap();
        packs = f.cache.seal_packs().await.unwrap();
        let second = f
            .chain
            .publish(
                &f.cache,
                first.seq,
                CommitPayload::single_root(head, packs),
                |p, _| Ok(p),
                2,
            )
            .await
            .unwrap();

        // Both commits retained: the union is live.
        let both = live_set(&f.chain, &f.cache, &[first.seq, second.seq], 4)
            .await
            .unwrap();
        assert!(both.missing_roots.is_empty());
        assert!(both.mark.nodes.contains(&head));
        assert!(both.mark.nodes.contains(&first.root(SHARD0).unwrap()));
        assert!(!both.live_packs.is_empty());

        // Only the tip retained: the first commit's superseded nodes
        // fall out of the live set, which is the garbage §P10b is about.
        let tip = live_set(&f.chain, &f.cache, &[second.seq], 4)
            .await
            .unwrap();
        assert!(tip.mark.nodes.len() < both.mark.nodes.len());
        assert!(!tip.mark.nodes.contains(&first.root(SHARD0).unwrap()));

        // A sequence number retention has already removed is reported,
        // not fatal.
        let gone = live_set(&f.chain, &f.cache, &[second.seq, 9_999], 2)
            .await
            .unwrap();
        assert_eq!(gone.missing_roots, vec![9_999]);
        assert_eq!(gone.mark.nodes, tip.mark.nodes);
    }
}
