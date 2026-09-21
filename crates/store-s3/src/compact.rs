//! The sweep: delete dead packs, rewrite partially dead ones (plan 28
//! §P10, §P10b, step S7a).
//!
//! `mark.rs` decides what is live. This module turns that into bytes
//! removed from the bucket, and nothing here decides *when* or *how
//! fast* — that is S7b's, in `cli`, where the retention policy, the
//! `gc.horizon`, the condemned-list handshake and the rate budget live.
//! The seam is [`CompactionPacer`].
//!
//! ## Why compaction is the normal path and not the exception
//!
//! §P10b hoped that pack locality would make whole-pack death the
//! common case: a pack written by one commit superseded in its entirety
//! by later commits to the same key range, reclaimed by a `delete` with
//! no rewrite. §14.5 measured that hope and it is false for the
//! workload it ran — **0.7% of packs died whole**, and the compactor
//! rewrote **14.79 GiB of live nodes, 117% of the 12.64 GiB the commits
//! themselves wrote.** The steady-state footprint plateaus *because of*
//! the compactor, not in spite of needing one; switched off, it grew
//! without bound.
//!
//! Two consequences shape this module:
//!
//! - the rewrite path is the hot path, so it is the one that had to be
//!   made concurrent ([`crate::packs::build_packs_concurrent`]);
//! - the cheap path still has to exist and still has to be free.
//!   [`Compactor::delete_dead`] moves zero bytes — two DELETEs per
//!   pack, body and `.idx` — because 0.7% of a census-scale bucket is
//!   still thousands of packs, and because the untested optimistic case
//!   §14.5 names (a real writer returning to the same directories,
//!   rather than a fresh random directory per commit) is precisely the
//!   one where that fraction rises.
//!
//! ## The ordering invariant, restated for a rewrite
//!
//! S4 established it for the write path: every pack a commit names is
//! durable before the commit exists. The rewrite's version is the
//! mirror image, and it is not optional:
//!
//! > **A replacement pack must be durable before the original it
//! > replaces is deleted.**
//!
//! The two crash outcomes that follow are asymmetric on purpose. A
//! crash after the PUTs and before the DELETEs leaves a live node with
//! *two* copies — garbage, which the next round's mark classifies and
//! the next sweep reclaims, and which no reader can even observe
//! because both copies hash to the same node. A crash in the other
//! order would leave a live node with *no* copy, which is data loss and
//! unrecoverable. So the code does all the PUTs, awaits all of them,
//! and only then deletes; `a_failed_replacement_put_leaves_every_live_node_readable`
//! and `a_failed_delete_leaves_every_live_node_readable` inject a
//! failure on each side of that line and assert the surviving property
//! from a cold cache.
//!
//! One small guard falls out of content addressing and is worth naming:
//! a replacement is never allowed to delete *itself*. If a rewrite
//! happens to reproduce a pack that is also in the batch being
//! replaced, `put_pack`'s `Create` returns `AlreadyExists` (success,
//! since the bytes are identical) and the delete list skips it.
//!
//! ## Deletes are best-effort, and that is deliberate
//!
//! A DELETE that fails is not a correctness problem — the pack is
//! unreachable garbage that the next round re-classifies — whereas
//! aborting the batch on one 503 would throw away the record of every
//! pack that *was* reclaimed. So failures are collected into
//! [`Reclaim::delete_failures`] and the batch continues. A failed PUT
//! is the opposite and aborts immediately: nothing may be deleted
//! against a replacement that is not there.
//!
//! ## Restartability
//!
//! Nothing depends on a sweep completing promptly (§S7), so both
//! entry points take a batch size and a resume point and hand back the
//! cursor to continue from. Packs are processed in ascending hash
//! order, which is stable across runs and independent of LIST ordering,
//! so a cursor is one 32-byte value and a resumed run repeats no work.
//! Re-running a batch that already completed is harmless: the DELETEs
//! are idempotent and the PUTs are content-addressed.

use crate::error::StoreError;
use crate::layout;
use crate::mark::PackCatalog;
use crate::packs::{build_packs_in, PackEntry, PackHash, PackNode, PackStore};
use constellation_mtree::{Hasher, NodeHash, NodeRef};
use object_store::ObjectStoreExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// How much of a pack the live set still needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackFate {
    /// Every node is live. Untouched, at zero cost — the outcome the
    /// key ordering is trying to produce.
    FullyLive,
    /// Some live, some not. Rewritten: the survivors are re-emitted in
    /// key order and the original is deleted afterwards.
    PartiallyDead,
    /// No live node. Deleted with its `.idx` sibling, no bytes moved.
    /// §14.5 measured this at 0.7% of packs.
    FullyDead,
}

/// What a sweep concluded about one pack.
#[derive(Clone, Debug)]
pub struct PackVerdict {
    pub pack: PackHash,
    pub fate: PackFate,
    /// Sealed body size on the bucket.
    pub body_bytes: u64,
    pub live_nodes: usize,
    pub dead_nodes: usize,
    /// Compressed frame bytes the rewrite has to carry over.
    pub live_frame_bytes: u64,
    /// Compressed frame bytes the rewrite drops.
    pub dead_frame_bytes: u64,
    /// The entries a rewrite must preserve, in the pack's own order —
    /// which is key order, because that is how it was filled.
    ///
    /// Only populated for [`PackFate::PartiallyDead`]: a fully live
    /// pack is never read and a fully dead one has no survivors, so
    /// carrying their entries would make a sweep's memory O(all nodes)
    /// instead of O(nodes needing a rewrite).
    survivors: Vec<PackEntry>,
}

/// Every pack classified against one mark.
///
/// Ordered by pack hash, which is what makes the cursor in
/// [`Reclaim::next`] meaningful.
#[derive(Clone, Debug, Default)]
pub struct Sweep {
    verdicts: Vec<PackVerdict>,
}

impl Sweep {
    /// Classify every pack in `catalog` against `live`.
    ///
    /// Pure: no I/O, and the `(level, first key)` the rewrite needs for
    /// ordering comes straight off the `.idx` entries, so a pack is
    /// classified and its rewrite is *planned* without decoding a
    /// single node body. That is the reason S4 put level and first key
    /// in the index rather than leaving them implicit in the node.
    pub fn classify(catalog: &PackCatalog, live: &HashSet<NodeHash>) -> Sweep {
        let mut verdicts = Vec::with_capacity(catalog.len());
        for (hash, pack) in catalog.iter() {
            let mut survivors = Vec::new();
            let mut live_frame_bytes = 0u64;
            let mut dead_frame_bytes = 0u64;
            let mut dead_nodes = 0usize;
            for entry in &pack.index.entries {
                if live.contains(&entry.hash) {
                    live_frame_bytes += entry.compressed_len as u64;
                    survivors.push(entry.clone());
                } else {
                    dead_frame_bytes += entry.compressed_len as u64;
                    dead_nodes += 1;
                }
            }
            let live_nodes = survivors.len();
            let fate = match (live_nodes, dead_nodes) {
                (0, _) => PackFate::FullyDead,
                (_, 0) => PackFate::FullyLive,
                _ => PackFate::PartiallyDead,
            };
            if fate != PackFate::PartiallyDead {
                survivors = Vec::new();
            }
            verdicts.push(PackVerdict {
                pack: *hash,
                fate,
                body_bytes: pack.body_bytes,
                live_nodes,
                dead_nodes,
                live_frame_bytes,
                dead_frame_bytes,
                survivors,
            });
        }
        Sweep { verdicts }
    }

    pub fn verdicts(&self) -> &[PackVerdict] {
        &self.verdicts
    }

    pub fn verdict(&self, pack: &PackHash) -> Option<&PackVerdict> {
        self.verdicts.iter().find(|v| v.pack == *pack)
    }

    fn with_fate(&self, fate: PackFate) -> impl Iterator<Item = &PackVerdict> {
        self.verdicts.iter().filter(move |v| v.fate == fate)
    }

    pub fn fully_live(&self) -> Vec<PackHash> {
        self.with_fate(PackFate::FullyLive)
            .map(|v| v.pack)
            .collect()
    }

    pub fn fully_dead(&self) -> Vec<PackHash> {
        self.with_fate(PackFate::FullyDead)
            .map(|v| v.pack)
            .collect()
    }

    pub fn partially_dead(&self) -> Vec<PackHash> {
        self.with_fate(PackFate::PartiallyDead)
            .map(|v| v.pack)
            .collect()
    }

    /// §14.5's headline number: the fraction of packs that die whole
    /// and cost nothing to reclaim. Measured at 0.007 there, against
    /// §P10b's original assumption that it would be the common case.
    pub fn whole_pack_death_fraction(&self) -> f64 {
        if self.verdicts.is_empty() {
            return 0.0;
        }
        self.with_fate(PackFate::FullyDead).count() as f64 / self.verdicts.len() as f64
    }

    /// Compressed bytes a full compaction would carry over — the
    /// quantity §14.5 measured at 117% of the commit write rate, and
    /// the one S7b's rate budget has to be sized against.
    pub fn rewrite_bytes(&self) -> u64 {
        self.with_fate(PackFate::PartiallyDead)
            .map(|v| v.live_frame_bytes)
            .sum()
    }

    /// Bytes a full sweep would free: whole dead bodies plus the dead
    /// frames inside partially dead packs.
    pub fn reclaimable_bytes(&self) -> u64 {
        self.verdicts
            .iter()
            .map(|v| match v.fate {
                PackFate::FullyDead => v.body_bytes,
                PackFate::PartiallyDead => v.dead_frame_bytes,
                PackFate::FullyLive => 0,
            })
            .sum()
    }
}

/// Where S7b's rate budget plugs in.
///
/// The compactor asks, before each batch, how long to wait before
/// moving that many bytes, and sleeps for exactly that long. This crate
/// implements no policy at all: `CONSTELLATION_COMPACT_BYTES_PER_S` is
/// S7b's knob, and so is every question about what to do when the node
/// is also serving FUSE traffic, whether the budget is per-node or
/// per-bucket, and how it interacts with the lease. The mechanism is
/// here because only the compactor knows the byte counts, and because a
/// policy that could only throttle *between* whole sweeps would be
/// useless at §14.5's 14.79 GiB per run.
pub trait CompactionPacer: Send + Sync {
    fn pace(&self, bytes: u64) -> Duration;
}

/// No throttling. The default, because a library that slept by default
/// would be making S7b's decision for it.
pub struct Unpaced;

impl CompactionPacer for Unpaced {
    fn pace(&self, _bytes: u64) -> Duration {
        Duration::ZERO
    }
}

/// What one batch did.
#[derive(Clone, Debug, Default)]
pub struct Reclaim {
    pub deleted: Vec<PackHash>,
    pub written: Vec<PackHash>,
    pub nodes_moved: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    /// Packs whose DELETE failed. Not an error: the pack is unreachable
    /// garbage either way and the next round re-classifies it. Reported
    /// so an operator can see a bucket that is refusing deletes.
    pub delete_failures: Vec<(PackHash, String)>,
    /// Cursor for the next batch, or `None` when the plan is finished.
    pub next: Option<PackHash>,
}

/// Rewrites and deletes packs against one [`Sweep`].
///
/// Holds its own rayon pool for the lifetime of the compactor, because
/// a sweep is many batches and a pool per batch would be a thread spawn
/// storm.
///
/// A [`Sweep`] is a snapshot of one mark, and the catalog it came from
/// is stale the moment a batch completes. A caller that wants to
/// re-derive fates must re-mark; batching within one sweep is safe
/// because the fates of the packs a batch has not reached cannot be
/// changed by the batches before it.
pub struct Compactor {
    packs: PackStore,
    hasher: Hasher,
    pool: Arc<rayon::ThreadPool>,
    pacer: Arc<dyn CompactionPacer>,
    request_concurrency: usize,
}

/// Concurrent GETs/PUTs/DELETEs against the bucket. Not the same knob
/// as the thread width: these are latency-bound requests, not CPU.
const DEFAULT_REQUEST_CONCURRENCY: usize = 16;

impl Compactor {
    /// `hasher` must be the one the filesystem addresses nodes with —
    /// the same precondition `NodeCache` documents. A rewrite
    /// re-verifies every node it moves against the hash the index
    /// claimed, so a mismatched hasher turns the whole compaction into
    /// a hash error rather than corrupting anything.
    pub fn new(packs: PackStore, hasher: Hasher, threads: usize) -> Result<Compactor, StoreError> {
        Ok(Compactor {
            packs,
            hasher,
            pool: Arc::new(crate::parallel::thread_pool(threads)?),
            pacer: Arc::new(Unpaced),
            request_concurrency: DEFAULT_REQUEST_CONCURRENCY,
        })
    }

    pub fn with_pacer(mut self, pacer: Arc<dyn CompactionPacer>) -> Compactor {
        self.pacer = pacer;
        self
    }

    pub fn with_request_concurrency(mut self, concurrency: usize) -> Compactor {
        self.request_concurrency = concurrency.max(1);
        self
    }

    pub fn threads(&self) -> usize {
        self.pool.current_num_threads()
    }

    /// Delete up to `max_packs` fully dead packs, body and `.idx`.
    ///
    /// The zero-rewrite path. Nothing is read and nothing is written,
    /// so there is no ordering invariant to honour: a fully dead pack
    /// holds no live node by construction.
    pub async fn delete_dead(
        &self,
        sweep: &Sweep,
        max_packs: usize,
        resume_after: Option<PackHash>,
    ) -> Result<Reclaim, StoreError> {
        let batch = batch_of(sweep, PackFate::FullyDead, max_packs, resume_after);
        let mut outcome = Reclaim {
            next: cursor_after(sweep, PackFate::FullyDead, &batch),
            ..Reclaim::default()
        };
        if batch.is_empty() {
            return Ok(outcome);
        }
        let bytes: u64 = batch.iter().map(|v| v.body_bytes).sum();
        self.pace(bytes).await;
        self.delete_packs(batch.iter().map(|v| v.pack), &mut outcome)
            .await;
        Ok(outcome)
    }

    /// Rewrite up to `max_packs` partially dead packs.
    ///
    /// `live` is only used to re-check the survivors the sweep already
    /// selected, so that a [`Sweep`] cannot be paired with a different
    /// mark than the one that produced it without the mismatch being
    /// visible.
    pub async fn compact(
        &self,
        sweep: &Sweep,
        live: &HashSet<NodeHash>,
        max_packs: usize,
        resume_after: Option<PackHash>,
    ) -> Result<Reclaim, StoreError> {
        use futures::StreamExt;

        let batch = batch_of(sweep, PackFate::PartiallyDead, max_packs, resume_after);
        let mut outcome = Reclaim {
            next: cursor_after(sweep, PackFate::PartiallyDead, &batch),
            ..Reclaim::default()
        };
        if batch.is_empty() {
            return Ok(outcome);
        }
        self.pace(batch.iter().map(|v| v.body_bytes).sum()).await;

        // `Bytes`, not `Vec<u8>`: a batch reads whole pack bodies and
        // then only ever slices them, so the one thing it must not do
        // is copy them. §14.9's phase breakdown for this path put the
        // body read at 219 ms against a 352 ms rewrite and — unlike the
        // rewrite — it did not shrink with threads at all, because a
        // `memcpy` is a `memcpy` on any number of cores. This is
        // therefore [`PackStore::get_body`] without its `to_vec`, which
        // is also why it goes through the raw store rather than through
        // that helper.
        let mut bodies: HashMap<PackHash, bytes::Bytes> = HashMap::with_capacity(batch.len());
        let store = self.packs.inner();
        let mut reads = futures::stream::iter(batch.iter().map(|verdict| {
            let store = store.clone();
            let hash = verdict.pack;
            async move {
                let key = layout::pack(&hash.to_hex());
                let body = async { store.get(&key).await?.bytes().await };
                (hash, body.await)
            }
        }))
        .buffer_unordered(self.request_concurrency);
        while let Some((hash, body)) = reads.next().await {
            let body = body?;
            outcome.bytes_read += body.len() as u64;
            bodies.insert(hash, body);
        }

        let survivors: Vec<(PackHash, Vec<PackEntry>)> = batch
            .iter()
            .map(|verdict| (verdict.pack, verdict.survivors.clone()))
            .collect();
        let nodes = self.extract(&bodies, &survivors, live)?;
        outcome.nodes_moved = nodes.len() as u64;
        let built = build_packs_in(&self.pool, nodes, self.packs.target_bytes())?;

        // Durability first, and *all* of it: one failed PUT aborts the
        // batch with nothing deleted, because a partial replacement set
        // cannot safely retire any original.
        let mut writes = futures::stream::iter(built.iter().map(|pack| {
            let packs = self.packs.clone();
            async move { packs.put_pack(pack).await }
        }))
        .buffer_unordered(self.request_concurrency);
        while let Some(written) = writes.next().await {
            written?;
        }
        drop(writes);
        outcome.written = built.iter().map(|pack| pack.hash).collect();
        outcome.bytes_written = built.iter().map(|pack| pack.body.len() as u64).sum();

        let replacements: HashSet<PackHash> = outcome.written.iter().copied().collect();
        let retirable: Vec<PackHash> = batch
            .iter()
            .map(|verdict| verdict.pack)
            .filter(|pack| !replacements.contains(pack))
            .collect();
        self.delete_packs(retirable.into_iter(), &mut outcome).await;
        Ok(outcome)
    }

    async fn pace(&self, bytes: u64) {
        let wait = self.pacer.pace(bytes);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    /// Read the live nodes out of the batch's bodies, verify them, and
    /// hand back pack-ready nodes.
    ///
    /// Runs on the pool: the work is one zstd decode and one blake3 per
    /// node, and §14.9 measured that shape scaling near-linearly.
    ///
    /// Every node is hash-checked and then [`NodeRef::parse`]d, exactly
    /// as `NodeCache` does for bytes off a network — the index is
    /// untrusted (§P8) and a rewrite is the one place where believing
    /// it would *persist* the lie into a new pack. The level and first
    /// key the sweep planned the ordering from are cross-checked
    /// against the node itself for the same reason.
    fn extract(
        &self,
        bodies: &HashMap<PackHash, bytes::Bytes>,
        survivors: &[(PackHash, Vec<PackEntry>)],
        live: &HashSet<NodeHash>,
    ) -> Result<Vec<PackNode>, StoreError> {
        use rayon::prelude::*;

        let flat: Vec<(&PackHash, &PackEntry)> = survivors
            .iter()
            .flat_map(|(pack, entries)| entries.iter().map(move |entry| (pack, entry)))
            .collect();
        self.pool.install(|| {
            flat.par_iter()
                .map(|(pack, entry)| {
                    if !live.contains(&entry.hash) {
                        return Err(StoreError::Conflict(format!(
                            "pack {pack} entry {} is not in the live set this sweep was \
                             classified against",
                            entry.hash
                        )));
                    }
                    let body = bodies.get(pack).ok_or_else(|| {
                        StoreError::CorruptObject(format!("pack {pack} body was not read"))
                    })?;
                    let start = entry.offset as usize;
                    let end = start + entry.compressed_len as usize;
                    let frame = body.get(start..end).ok_or_else(|| {
                        StoreError::CorruptObject(format!(
                            "pack {pack} index puts node {} outside the body",
                            entry.hash
                        ))
                    })?;
                    let bytes = zstd::decode_all(frame)
                        .map_err(|e| StoreError::Compression(e.to_string()))?;
                    if self.hasher.hash(&bytes) != entry.hash {
                        return Err(StoreError::HashMismatch {
                            key: layout::pack(&pack.to_hex()).to_string(),
                        });
                    }
                    NodeRef::parse(&bytes)?;
                    let node = PackNode::from_bytes(entry.hash, bytes)?;
                    if node.level != entry.level || node.first_key != entry.first_key {
                        return Err(StoreError::CorruptObject(format!(
                            "pack {pack} index describes node {} as level {} but it is level {}",
                            entry.hash, entry.level, node.level
                        )));
                    }
                    Ok(node)
                })
                .collect::<Result<Vec<_>, StoreError>>()
        })
    }

    /// Both objects of each pack. A `NotFound` is success: an earlier
    /// round, or an earlier attempt at this one, already did it.
    async fn delete_packs<I>(&self, packs: I, outcome: &mut Reclaim)
    where
        I: Iterator<Item = PackHash>,
    {
        use futures::StreamExt;

        let store = self.packs.inner();
        let mut deletes = futures::stream::iter(packs.map(|pack| {
            let store = store.clone();
            let hex = pack.to_hex();
            async move {
                for key in [layout::pack(&hex), layout::pack_index(&hex)] {
                    match store.delete(&key).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(e) => return (pack, Err(e)),
                    }
                }
                (pack, Ok(()))
            }
        }))
        .buffer_unordered(self.request_concurrency);
        while let Some((pack, result)) = deletes.next().await {
            match result {
                Ok(()) => outcome.deleted.push(pack),
                Err(e) => {
                    tracing::warn!(
                        pack = %pack,
                        error = %e,
                        "could not delete a reclaimable pack; the next sweep will retry"
                    );
                    outcome.delete_failures.push((pack, e.to_string()));
                }
            }
        }
        outcome.deleted.sort_unstable();
        outcome.delete_failures.sort_by_key(|(pack, _)| *pack);
    }
}

fn batch_of(
    sweep: &Sweep,
    fate: PackFate,
    max_packs: usize,
    resume_after: Option<PackHash>,
) -> Vec<&PackVerdict> {
    sweep
        .verdicts
        .iter()
        .filter(|v| v.fate == fate)
        .filter(|v| resume_after.is_none_or(|after| v.pack > after))
        .take(max_packs.max(1))
        .collect()
}

/// The cursor a caller should pass next: the last pack this batch
/// covered, or `None` once the plan holds nothing beyond it.
fn cursor_after(sweep: &Sweep, fate: PackFate, batch: &[&PackVerdict]) -> Option<PackHash> {
    let last = batch.last()?.pack;
    sweep
        .verdicts
        .iter()
        .any(|v| v.fate == fate && v.pack > last)
        .then_some(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mark::mark;
    use crate::node_cache::NodeCache;
    use constellation_fs_core::cache::DiskCache;
    use constellation_mtree::Tree;
    use futures::stream::BoxStream;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path as OPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::TempDir;

    /// An `InMemory` that can be told to refuse writes to `packs/*`, or
    /// to refuse deletes, from a chosen point onwards. The two arms are
    /// the two sides of the ordering invariant.
    #[derive(Debug)]
    struct FaultStore {
        inner: InMemory,
        /// PUTs to `packs/*` start failing once this many have landed.
        fail_pack_put_after: AtomicUsize,
        fail_deletes: AtomicBool,
    }

    impl FaultStore {
        fn new() -> Arc<FaultStore> {
            Arc::new(FaultStore {
                inner: InMemory::new(),
                fail_pack_put_after: AtomicUsize::new(usize::MAX),
                fail_deletes: AtomicBool::new(false),
            })
        }
    }

    impl std::fmt::Display for FaultStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FaultStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FaultStore {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if location.as_ref().starts_with("packs/")
                && self
                    .fail_pack_put_after
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        Some(n.saturating_sub(1))
                    })
                    == Ok(0)
            {
                return Err(object_store::Error::Generic {
                    store: "FaultStore",
                    source: "crashed before the replacement pack was durable".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OPath>>,
        ) -> BoxStream<'static, object_store::Result<OPath>> {
            if self.fail_deletes.load(Ordering::SeqCst) {
                use futures::StreamExt;
                return locations
                    .map(|_| {
                        Err(object_store::Error::Generic {
                            store: "FaultStore",
                            source: "crashed before the original pack was deleted".into(),
                        })
                    })
                    .boxed();
            }
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn cache_for(store: Arc<dyn ObjectStore>, dir: &TempDir, target: usize) -> Arc<NodeCache> {
        let disk = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        Arc::new(NodeCache::new(
            PackStore::new(store).with_target_bytes(target),
            disk,
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ))
    }

    /// Values are pseudorandom rather than constant: compressible
    /// filler would let a whole test filesystem zstd into one pack and
    /// make every classification assertion vacuous.
    fn pairs(range: std::ops::Range<u64>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        range
            .map(|i| {
                let mut key = vec![0x01u8];
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

    /// Build a tree, age it with edits, and hand back the live tip plus
    /// the garbage the edits left behind.
    struct Dirty {
        _dir: TempDir,
        cache: Arc<NodeCache>,
        tip: NodeHash,
    }

    async fn dirty_tip(store: Arc<dyn ObjectStore>, keys: u64, generations: u64) -> Dirty {
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir, 64 << 10);
        let tree = Tree::new(cache.clone());
        let mut tip = tree.build(pairs(0..keys)).unwrap();
        cache.seal_packs().await.unwrap();
        for generation in 0..generations {
            let edits: Vec<_> = (0..keys / 8)
                .map(|i| {
                    let mut key = vec![0x01u8];
                    key.extend_from_slice(&((i * 7 + generation) % keys).to_be_bytes());
                    (key, Some(vec![generation as u8; 48]))
                })
                .collect();
            tip = tree.apply(&tip, &edits).unwrap();
            cache.seal_packs().await.unwrap();
        }
        Dirty {
            _dir: dir,
            cache,
            tip,
        }
    }

    /// Everything reachable from `root` resolves, from a cache that
    /// holds nothing and knows only what the bucket says. The property
    /// both crash tests assert.
    async fn assert_every_live_node_is_readable(store: &Arc<dyn ObjectStore>, root: &NodeHash) {
        let dir = TempDir::new().unwrap();
        let cold = cache_for(store.clone(), &dir, 64 << 10);
        let catalog = PackCatalog::load(cold.packs()).await.unwrap();
        catalog.attach_to(&cold);
        let marked = mark(&cold, &[*root], 4)
            .unwrap_or_else(|e| panic!("a live node became unreadable: {e}"));
        assert!(!marked.nodes.is_empty());
        // And every one of them is genuinely resolvable, not merely
        // named: the walk above only reads interior nodes' children.
        let tree = Tree::new(cold.clone());
        assert!(!tree.census(root).unwrap().leaf_entries.is_empty());
    }

    async fn live_sweep(d: &Dirty) -> (PackCatalog, Sweep, HashSet<NodeHash>) {
        let catalog = PackCatalog::load(d.cache.packs()).await.unwrap();
        catalog.attach_to(&d.cache);
        let live = mark(&d.cache, &[d.tip], 4).unwrap().nodes;
        let sweep = Sweep::classify(&catalog, &live);
        (catalog, sweep, live)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn classification_splits_packs_three_ways() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store, 8_000, 6).await;
        let (catalog, sweep, live) = live_sweep(&d).await;

        assert_eq!(sweep.verdicts().len(), catalog.len());
        assert!(catalog.len() > 10, "{} packs", catalog.len());
        let (dead, partial, alive) = (
            sweep.fully_dead().len(),
            sweep.partially_dead().len(),
            sweep.fully_live().len(),
        );
        assert_eq!(dead + partial + alive, catalog.len());
        // §14.5's finding, reproduced in miniature: an aged tree leaves
        // mostly *partially* dead packs, which is why the compactor is
        // the normal path and the delete-whole path is the exception.
        assert!(partial > 0, "no pack was partially dead: {sweep:?}");
        assert!(
            sweep.rewrite_bytes() > 0,
            "a partially dead pack must have bytes to carry over"
        );
        assert!(sweep.reclaimable_bytes() > 0);

        // The classification agrees with the live set node by node.
        for verdict in sweep.verdicts() {
            let index = &catalog.get(&verdict.pack).unwrap().index;
            let counted = index
                .entries
                .iter()
                .filter(|e| live.contains(&e.hash))
                .count();
            assert_eq!(counted, verdict.live_nodes);
            assert_eq!(index.entries.len() - counted, verdict.dead_nodes);
            match verdict.fate {
                PackFate::FullyLive => assert_eq!(verdict.dead_nodes, 0),
                PackFate::FullyDead => assert_eq!(verdict.live_nodes, 0),
                PackFate::PartiallyDead => {
                    assert!(verdict.live_nodes > 0 && verdict.dead_nodes > 0)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fully_dead_pack_is_deleted_with_its_index_and_no_rewrite() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store.clone(), 6_000, 8).await;
        let (_, sweep, _) = live_sweep(&d).await;
        let dead = sweep.fully_dead();
        if dead.is_empty() {
            // Nothing to assert about deletion, but the *reason* is
            // §14.5's finding and not a broken fixture, so make that
            // visible rather than passing silently.
            assert!(!sweep.partially_dead().is_empty());
            return;
        }

        let compactor = Compactor::new(d.cache.packs().clone(), Hasher::Plain, 2).unwrap();
        let outcome = compactor
            .delete_dead(&sweep, usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(outcome.deleted.len(), dead.len());
        assert!(outcome.delete_failures.is_empty());
        assert_eq!(outcome.bytes_written, 0, "the dead path moves no bytes");
        assert!(outcome.written.is_empty());
        assert_eq!(outcome.next, None);

        for pack in &dead {
            assert!(!d.cache.packs().contains(pack).await.unwrap());
            let hex = pack.to_hex();
            for key in [layout::pack(&hex), layout::pack_index(&hex)] {
                assert!(
                    store.get(&key).await.is_err(),
                    "{key} survived its pack's deletion"
                );
            }
        }
        assert_every_live_node_is_readable(&store, &d.tip).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compaction_rewrites_survivors_in_key_order_and_retires_the_originals() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store.clone(), 8_000, 6).await;
        let (_, sweep, live) = live_sweep(&d).await;
        let partial = sweep.partially_dead();
        assert!(partial.len() > 2, "{} partial packs", partial.len());

        let compactor = Compactor::new(d.cache.packs().clone(), Hasher::Plain, 4).unwrap();
        let outcome = compactor
            .compact(&sweep, &live, usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(outcome.next, None);
        assert_eq!(outcome.deleted.len(), partial.len());
        assert!(outcome.delete_failures.is_empty());
        assert!(!outcome.written.is_empty());
        assert_eq!(
            outcome.nodes_moved,
            partial
                .iter()
                .map(|p| sweep.verdict(p).unwrap().live_nodes as u64)
                .sum::<u64>()
        );
        // The rewrite is smaller than what it replaced: that is the
        // point of the exercise.
        assert!(
            outcome.bytes_written < outcome.bytes_read,
            "{} written against {} read",
            outcome.bytes_written,
            outcome.bytes_read
        );

        // Every original is gone, every survivor is still resolvable,
        // and the replacements are in key order.
        for pack in &partial {
            assert!(!d.cache.packs().contains(pack).await.unwrap());
        }
        assert_every_live_node_is_readable(&store, &d.tip).await;

        let after = PackCatalog::load(d.cache.packs()).await.unwrap();
        for pack in &outcome.written {
            let entries = &after.get(pack).unwrap().index.entries;
            let mut previous: Option<(u8, &Vec<u8>)> = None;
            for entry in entries {
                if let Some(prev) = previous {
                    assert!(
                        prev <= (entry.level, &entry.first_key),
                        "a replacement pack is not in (level, first key) order"
                    );
                }
                previous = Some((entry.level, &entry.first_key));
            }
        }
    }

    /// The crash on the safe side of the line: replacements are durable
    /// and the DELETE never happens. A live node then has two copies —
    /// harmless garbage the next round reclaims — and *every* live node
    /// must still resolve.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_delete_leaves_every_live_node_readable() {
        let faulty = FaultStore::new();
        let store = faulty.clone() as Arc<dyn ObjectStore>;
        let d = dirty_tip(store.clone(), 8_000, 6).await;
        let (_, sweep, live) = live_sweep(&d).await;
        let partial = sweep.partially_dead();
        assert!(!partial.is_empty());

        faulty.fail_deletes.store(true, Ordering::SeqCst);
        let outcome = compactor_for(&d)
            .compact(&sweep, &live, usize::MAX, None)
            .await
            .unwrap();
        assert!(outcome.deleted.is_empty());
        assert_eq!(
            outcome.delete_failures.len(),
            partial.len(),
            "every delete must be reported, not swallowed"
        );
        assert!(!outcome.written.is_empty(), "the replacements did land");

        // Both copies exist, which the invariant permits, and the live
        // set is intact.
        for pack in &partial {
            assert!(d.cache.packs().contains(pack).await.unwrap());
        }
        for pack in &outcome.written {
            assert!(d.cache.packs().contains(pack).await.unwrap());
        }
        assert_every_live_node_is_readable(&store, &d.tip).await;

        // And the duplication really is transient: with deletes working
        // again, the same batch completes.
        faulty.fail_deletes.store(false, Ordering::SeqCst);
        let retry = compactor_for(&d)
            .compact(&sweep, &live, usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(retry.deleted.len(), partial.len());
        assert!(retry.delete_failures.is_empty());
        assert_every_live_node_is_readable(&store, &d.tip).await;
    }

    /// The crash on the dangerous side: a replacement PUT fails. The
    /// batch must abort with *nothing* deleted, because a live node
    /// whose only copy was in a retired original is unrecoverable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_replacement_put_leaves_every_live_node_readable() {
        let faulty = FaultStore::new();
        let store = faulty.clone() as Arc<dyn ObjectStore>;
        let d = dirty_tip(store.clone(), 8_000, 6).await;
        let (before, sweep, live) = live_sweep(&d).await;
        let partial = sweep.partially_dead();
        assert!(partial.len() > 2);

        // The first replacement body lands; the next PUT does not.
        faulty.fail_pack_put_after.store(1, Ordering::SeqCst);
        let err = compactor_for(&d)
            .compact(&sweep, &live, usize::MAX, None)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::ObjectStore(_)), "{err}");

        // Nothing was retired, so every original still holds its
        // survivors and the live set resolves.
        for pack in &partial {
            assert!(
                d.cache.packs().contains(pack).await.unwrap(),
                "an original was deleted against a replacement that never landed"
            );
        }
        assert_every_live_node_is_readable(&store, &d.tip).await;

        // Whatever half-written replacement exists is an orphan body
        // with no index — catalogued as incomplete, never as dead.
        faulty
            .fail_pack_put_after
            .store(usize::MAX, Ordering::SeqCst);
        let after = PackCatalog::load(d.cache.packs()).await.unwrap();
        assert!(after.len() >= before.len());

        // The retry converges and the invariant still holds.
        let outcome = compactor_for(&d)
            .compact(&sweep, &live, usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(outcome.deleted.len(), partial.len());
        assert_every_live_node_is_readable(&store, &d.tip).await;
    }

    fn compactor_for(d: &Dirty) -> Compactor {
        Compactor::new(d.cache.packs().clone(), Hasher::Plain, 4).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_batched_sweep_resumes_from_its_cursor_and_repeats_no_work() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store.clone(), 8_000, 6).await;
        let (_, sweep, live) = live_sweep(&d).await;
        let partial = sweep.partially_dead();
        assert!(partial.len() >= 3, "{} partial packs", partial.len());

        let compactor = compactor_for(&d);
        let mut cursor = None;
        let mut rounds = 0usize;
        let mut retired: Vec<PackHash> = Vec::new();
        loop {
            let outcome = compactor.compact(&sweep, &live, 1, cursor).await.unwrap();
            retired.extend(outcome.deleted.iter().copied());
            rounds += 1;
            match outcome.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
            assert!(rounds < 100, "the cursor is not advancing");
        }
        assert!(
            rounds >= partial.len(),
            "a batch size of one pack took only {rounds} rounds for {} packs",
            partial.len()
        );
        retired.sort_unstable();
        let mut expected = partial.clone();
        expected.sort_unstable();
        assert_eq!(
            retired, expected,
            "a batched sweep must cover the plan once"
        );
        assert_every_live_node_is_readable(&store, &d.tip).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sweep_from_a_different_mark_is_refused_not_applied() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store.clone(), 4_000, 4).await;
        let (_, sweep, _) = live_sweep(&d).await;
        assert!(!sweep.partially_dead().is_empty());
        // An empty live set is the most wrong mark there is; pairing it
        // with this sweep must be an error rather than a rewrite that
        // drops every survivor.
        let err = compactor_for(&d)
            .compact(&sweep, &HashSet::new(), usize::MAX, None)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Conflict(_)), "{err}");
        for pack in sweep.partially_dead() {
            assert!(d.cache.packs().contains(&pack).await.unwrap());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lying_index_is_a_read_error_not_a_rewritten_lie() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store.clone(), 4_000, 4).await;
        let (catalog, sweep, live) = live_sweep(&d).await;
        let victim = sweep.partially_dead()[0];

        // Repoint a live entry at another node's frame: the bytes are a
        // well-formed node, they are just not the node the index names.
        let mut index = catalog.get(&victim).unwrap().index.clone();
        let other = index
            .entries
            .iter()
            .find(|e| e.offset != index.entries[0].offset)
            .cloned()
            .unwrap();
        let target = index
            .entries
            .iter_mut()
            .find(|e| live.contains(&e.hash) && e.offset != other.offset)
            .unwrap();
        target.offset = other.offset;
        target.compressed_len = other.compressed_len;
        store
            .put(
                &layout::pack_index(&victim.to_hex()),
                PutPayload::from(index.encode()),
            )
            .await
            .unwrap();

        let relisted = PackCatalog::load(d.cache.packs()).await.unwrap();
        let resweep = Sweep::classify(&relisted, &live);
        let err = compactor_for(&d)
            .compact(&resweep, &live, usize::MAX, None)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::HashMismatch { .. }), "{err}");
        assert!(d.cache.packs().contains(&victim).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_empty_plan_is_a_no_op() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store, &dir, 1 << 20);
        let sweep = Sweep::default();
        let compactor = Compactor::new(cache.packs().clone(), Hasher::Plain, 2).unwrap();
        for outcome in [
            compactor
                .compact(&sweep, &HashSet::new(), 8, None)
                .await
                .unwrap(),
            compactor.delete_dead(&sweep, 8, None).await.unwrap(),
        ] {
            assert!(outcome.deleted.is_empty());
            assert!(outcome.written.is_empty());
            assert_eq!(outcome.next, None);
        }
        assert_eq!(Sweep::default().whole_pack_death_fraction(), 0.0);
    }

    /// The pacer is a mechanism, not a policy: the compactor asks and
    /// sleeps, and S7b decides. Assert that it is consulted with the
    /// batch's byte count and that the delay is honoured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pacer_is_asked_before_bytes_move() {
        #[derive(Default)]
        struct Recording {
            asked: std::sync::Mutex<Vec<u64>>,
        }
        impl CompactionPacer for Recording {
            fn pace(&self, bytes: u64) -> Duration {
                self.asked.lock().unwrap().push(bytes);
                Duration::from_millis(20)
            }
        }

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = dirty_tip(store, 4_000, 4).await;
        let (_, sweep, live) = live_sweep(&d).await;
        let pacer = Arc::new(Recording::default());
        let compactor = Compactor::new(d.cache.packs().clone(), Hasher::Plain, 2)
            .unwrap()
            .with_pacer(pacer.clone());

        let started = std::time::Instant::now();
        let outcome = compactor.compact(&sweep, &live, 1, None).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));
        let asked = pacer.asked.lock().unwrap().clone();
        assert_eq!(asked.len(), 1);
        assert_eq!(
            asked[0],
            sweep
                .verdict(&sweep.partially_dead()[0])
                .unwrap()
                .body_bytes
        );
        assert!(!outcome.written.is_empty());
    }

    /// One synthetic metadata node of about `bytes`, filled with
    /// pseudorandom entries so zstd cannot make the measurement below
    /// a compression-ratio benchmark instead of a throughput one.
    fn synthetic_node(seed: &mut u64, ordinal: u64, bytes: usize) -> PackNode {
        let per_entry = bytes / 64;
        let mut entries = Vec::with_capacity(64);
        for entry in 0..64u32 {
            let mut key = ordinal.to_be_bytes().to_vec();
            key.extend_from_slice(&entry.to_be_bytes());
            let mut value = Vec::with_capacity(per_entry);
            while value.len() < per_entry {
                *seed ^= *seed << 13;
                *seed ^= *seed >> 7;
                *seed ^= *seed << 17;
                value.extend_from_slice(&seed.to_le_bytes());
            }
            entries.push(constellation_mtree::Entry::leaf(key, value));
        }
        let encoded = constellation_mtree::node::encode(0, &entries);
        PackNode::from_bytes(Hasher::Plain.hash(&encoded), encoded).unwrap()
    }

    /// §14.9's compaction ceiling — "compaction tops out around 1.7×
    /// because the pack writer is serial" — re-measured against the
    /// concurrent writer this step added.
    ///
    /// Ignored by default: it is a measurement, not an assertion, and
    /// it moves a quarter of a GiB. Run with
    /// `cargo test --release -p constellation-store-s3 --lib
    /// compaction_thread_scaling -- --ignored --nocapture`.
    ///
    /// Two tables, because §14.9 blamed one specific component. The
    /// first isolates the pack writer, which is the thing that was
    /// serial; the second is end-to-end [`Compactor::compact`] against
    /// `InMemory` — GET the bodies, decompress, hash-verify, parse,
    /// re-emit in key order, PUT, DELETE — which is what §14.9's
    /// "compact MiB/s" column actually measured. MiB/s is live node
    /// bytes carried over per second, the same quantity §14.5 reports
    /// as 14.79 GiB per run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "measurement; prints the §14.9 comparison table"]
    async fn compaction_thread_scaling() {
        use crate::packs::{build_packs, build_packs_concurrent};

        const NODE_BYTES: usize = 96 << 10;
        const TOTAL_BYTES: usize = 256 << 20;
        let target = 4 << 20;
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let nodes: Vec<PackNode> = (0..(TOTAL_BYTES / NODE_BYTES) as u64)
            .map(|i| synthetic_node(&mut seed, i, NODE_BYTES))
            .collect();
        let all_bytes: u64 = nodes.iter().map(|n| n.bytes.len() as u64).sum();

        let serial_input = nodes.clone();
        let started = std::time::Instant::now();
        let originals = build_packs(serial_input, target).unwrap();
        let baseline = started.elapsed();
        let writer_base = all_bytes as f64 / baseline.as_secs_f64() / (1 << 20) as f64;
        println!(
            "\n{:.1} MiB of nodes in {} packs, {} hardware threads available\n",
            all_bytes as f64 / (1 << 20) as f64,
            originals.len(),
            crate::parallel::effective_threads(0),
        );
        println!("### writer only (`build_packs_concurrent`)\n");
        println!("| threads | wall | MiB/s | scaling |");
        println!("|---:|---:|---:|---:|");
        println!("| serial `build_packs` | {baseline:?} | {writer_base:.0} | 1.00x |");
        for threads in [1usize, 4, 8, 16] {
            let input = nodes.clone();
            let started = std::time::Instant::now();
            let built = build_packs_concurrent(input, target, threads).unwrap();
            let elapsed = started.elapsed();
            assert_eq!(built.len(), originals.len());
            let rate = all_bytes as f64 / elapsed.as_secs_f64() / (1 << 20) as f64;
            println!(
                "| {threads} | {elapsed:?} | {rate:.0} | {:.2}x |",
                rate / writer_base
            );
        }

        // Every second node dies, so every pack is partially dead and
        // the rewrite has to carry roughly half the bytes — §14.5's
        // shape, where 0.7% of packs died whole.
        let live: HashSet<NodeHash> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, node)| node.hash)
            .collect();
        let live_bytes: u64 = nodes
            .iter()
            .filter(|n| live.contains(&n.hash))
            .map(|n| n.bytes.len() as u64)
            .sum();

        println!("\n### end to end (`Compactor::compact`, `InMemory` bucket)\n");
        println!("| threads | wall | MiB/s | scaling |");
        println!("|---:|---:|---:|---:|");
        let mut end_to_end_base = 0f64;
        for threads in [1usize, 4, 8, 16] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let packs = PackStore::new(store).with_target_bytes(target);
            for pack in &originals {
                packs.put_pack(pack).await.unwrap();
            }
            let catalog = PackCatalog::load(&packs).await.unwrap();
            let sweep = Sweep::classify(&catalog, &live);
            assert_eq!(sweep.partially_dead().len(), originals.len());
            let compactor = Compactor::new(packs, Hasher::Plain, threads).unwrap();

            let started = std::time::Instant::now();
            let outcome = compactor
                .compact(&sweep, &live, usize::MAX, None)
                .await
                .unwrap();
            let elapsed = started.elapsed();
            assert_eq!(outcome.deleted.len(), originals.len());
            let rate = live_bytes as f64 / elapsed.as_secs_f64() / (1 << 20) as f64;
            if threads == 1 {
                end_to_end_base = rate;
            }
            println!(
                "| {threads} | {elapsed:?} | {rate:.0} | {:.2}x |",
                rate / end_to_end_base
            );
        }
    }

    /// Why the rewrite stops where it does, phase by phase.
    ///
    /// A companion to `compaction_thread_scaling`, and the reason this
    /// step can report a *precise* ceiling instead of a hopeful one.
    /// [`build_packs_in`] has exactly two serial steps — the
    /// `(level, first key)` sort and the pass that decides pack
    /// boundaries — and this re-runs all four phases with a timer on
    /// each so their share is a number rather than an assumption. If a
    /// future change reintroduces a serial fraction that matters, this
    /// is where it shows up.
    ///
    /// Run with `cargo test --release -p constellation-store-s3 --lib
    /// where_the_rewrite_time_goes -- --ignored --nocapture`.
    #[test]
    #[ignore = "measurement; prints the per-phase breakdown"]
    fn where_the_rewrite_time_goes() {
        use rayon::prelude::*;

        const NODE_BYTES: usize = 96 << 10;
        const TOTAL_BYTES: usize = 256 << 20;
        let target = 4usize << 20;
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let nodes: Vec<PackNode> = (0..(TOTAL_BYTES / NODE_BYTES) as u64)
            .map(|i| synthetic_node(&mut seed, i, NODE_BYTES))
            .collect();
        let total: u64 = nodes.iter().map(|node| node.bytes.len() as u64).sum();
        let mib = |bytes: u64, d: Duration| bytes as f64 / d.as_secs_f64() / (1 << 20) as f64;

        println!("\n| threads | sort | compress | cut | seal |");
        println!("|---:|---:|---:|---:|---:|");
        for threads in [1usize, 8] {
            let pool = crate::parallel::thread_pool(threads).unwrap();
            let mut input = nodes.clone();

            let at = std::time::Instant::now();
            input.sort_by(|a, b| {
                a.level
                    .cmp(&b.level)
                    .then_with(|| a.first_key.cmp(&b.first_key))
                    .then_with(|| a.hash.0.cmp(&b.hash.0))
            });
            input.dedup_by(|a, b| a.hash == b.hash);
            let sort = at.elapsed();

            let at = std::time::Instant::now();
            let frames: Vec<Vec<u8>> = pool.install(|| {
                input
                    .par_iter()
                    .map(|node| zstd::encode_all(&node.bytes[..], 3).unwrap())
                    .collect()
            });
            let compress = at.elapsed();

            let at = std::time::Instant::now();
            let mut cuts: Vec<std::ops::Range<usize>> = Vec::new();
            let mut start = 0usize;
            let mut body_len = 12usize;
            for (i, frame) in frames.iter().enumerate() {
                body_len += frame.len();
                if body_len >= target {
                    cuts.push(start..i + 1);
                    start = i + 1;
                    body_len = 12;
                }
            }
            if start < input.len() {
                cuts.push(start..input.len());
            }
            let cut = at.elapsed();

            let at = std::time::Instant::now();
            let sealed: Vec<PackHash> = pool.install(|| {
                cuts.par_iter()
                    .map(|range| {
                        let frames = &frames[range.clone()];
                        let mut body =
                            Vec::with_capacity(12 + frames.iter().map(Vec::len).sum::<usize>());
                        body.extend_from_slice(&crate::packs::PACK_MAGIC);
                        body.extend_from_slice(&[crate::packs::PACK_FORMAT_VERSION, 0, 0, 0]);
                        body.extend_from_slice(&(frames.len() as u32).to_le_bytes());
                        for frame in frames {
                            body.extend_from_slice(frame);
                        }
                        PackHash::of(&body)
                    })
                    .collect()
            });
            assert_eq!(sealed.len(), cuts.len());
            let seal = at.elapsed();

            println!(
                "| {threads} | {sort:?} | {compress:?} ({:.0} MiB/s) | {cut:?} | {seal:?} ({:.0} MiB/s) |",
                mib(total, compress),
                mib(frames.iter().map(|f| f.len() as u64).sum(), seal),
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_pack_still_fully_live_is_never_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir, 64 << 10);
        let tree = Tree::new(cache.clone());
        let root = tree.build(pairs(0..4_000)).unwrap();
        cache.seal_packs().await.unwrap();

        let catalog = PackCatalog::load(cache.packs()).await.unwrap();
        catalog.attach_to(&cache);
        let live = mark(&cache, &[root], 4).unwrap().nodes;
        let sweep = Sweep::classify(&catalog, &live);
        assert_eq!(sweep.fully_live().len(), catalog.len(), "{sweep:?}");
        assert_eq!(sweep.rewrite_bytes(), 0);
        assert_eq!(sweep.reclaimable_bytes(), 0);
        assert_eq!(sweep.whole_pack_death_fraction(), 0.0);

        let compactor = Compactor::new(cache.packs().clone(), Hasher::Plain, 2).unwrap();
        let outcome = compactor
            .compact(&sweep, &live, usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(outcome.bytes_read, 0, "a fresh tree needs no compaction");
        assert!(outcome.deleted.is_empty());

        let keys: Vec<String> = store
            .list(Some(&layout::packs_prefix()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(keys.len(), catalog.len() * 2);
    }
}
