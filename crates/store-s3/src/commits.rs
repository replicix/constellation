//! The commit chain: `commits/<seq:016x>`, CAS-created and immutable
//! (plan 28 §P2).
//!
//! One object per state of the whole filesystem, created with
//! `If-None-Match: *` and never touched again. It is
//! [`crate::log::LogStore::put_segment`]'s CAS with a different payload,
//! which is the point: plan 26's GET-next tailer, its idle backoff and
//! its LIST-only-when-catching-up rule all carry over unchanged, so
//! [`CommitChain::get_run`] and [`CommitChain::discover_head`] are the
//! same shape as `log.rs`'s and deliberately not a parallel design.
//!
//! Three properties follow from "a commit is a complete state" and are
//! worth naming, because they are what this replaces:
//!
//! - **There is no checkpoint.** Nothing has to be reconstructed by
//!   replaying a log onto a base image, so the three-write publish this
//!   supersedes — body PUT, `LATEST` PUT, `VECTOR.json` PUT, with a
//!   window after each — collapses into one atomic operation.
//! - **Deleting old commits costs time-travel depth and nothing else.**
//!   A log segment below the checkpoint is load-bearing; a commit never
//!   is (§P10b).
//! - **A reader's view is a version, not a prefix**: snapshot isolation
//!   at a named root hash.
//!
//! ## The ordering invariant
//!
//! **Every node and pack a commit names must be durable before the
//! commit object is CAS-created.** [`CommitChain::publish`] enforces it
//! by construction: it refuses to run while the node cache still holds
//! unpacked nodes, and it verifies that every pack the payload names is
//! present on the bucket before the CAS. The two crash outcomes are
//! therefore:
//!
//! - **before the CAS** — orphan packs. Garbage, reclaimed by S7's
//!   reachability sweep, and invisible to every reader because nothing
//!   names them.
//! - **after the CAS** — a commit all of whose nodes provably exist.
//!
//! There is no third state, and that single fact is the entire
//! crash-safety argument for the design. `no_commit_ever_names_a_missing_node`
//! is the test; it injects a failure between the pack PUT and the CAS
//! and asserts both halves.
//!
//! ## Contention
//!
//! Two writers racing for `seq + 1` both CAS; one gets a 412 and must
//! retry at `seq + 2`. What the loser does in between is a *policy*
//! question this module deliberately does not answer: §P3's structural
//! rebase (diff the winner's roots against the parent, splice a
//! disjoint write-set, re-execute an overlapping one) needs the key
//! codec and the operation semantics, neither of which live here. So
//! [`CommitChain::publish`] takes the rebase as a callback, retries the
//! CAS around it, and guarantees only that the loser's payload is
//! carried into the next attempt rather than dropped. S5 supplies the
//! real rebase; until then the identity rebase is correct for a single
//! writer and honest about what it is not.
//!
//! ## §P13
//!
//! A commit object is JSON; on an E2E filesystem ([`CommitChain::with_sealing`])
//! it is AEAD-sealed under the tree's commit key with the object path as
//! associated data — the same `encrypt_object`/`decrypt_object` pair
//! `log.rs` applies to segments. [`Commit::v`] versions the payload, not
//! the sealing. Keyed node addressing needs nothing here: `roots` and
//! `packs` are hex hashes whose derivation this module never inspects.

use crate::error::StoreError;
use crate::layout;
use crate::node_cache::NodeCache;
use crate::packs::{PackHash, PackStore};
use constellation_mtree::NodeHash;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Commit payload encoding version. A reader that does not recognize it
/// must refuse the object rather than guess: a commit is the one object
/// in the bucket whose misinterpretation is unbounded.
pub const COMMIT_VERSION: u32 = 1;

/// Default GET-next probe width. The same shape as plan 26's tailer:
/// against AWS a GET-404 is no slower than an empty LIST (177 vs 175 ms
/// from Europe) and costs ~1/12.5 of a LIST request, so the poll probes
/// and only catch-up lists.
pub const DEFAULT_PROBE_WINDOW: usize = 8;

/// Env `CONSTELLATION_COMMIT_PROBE_WINDOW`: how many commit slots ahead
/// of the known head are probed in one round before the tailer decides
/// it is far enough behind to be worth a LIST.
pub fn probe_window() -> usize {
    std::env::var("CONSTELLATION_COMMIT_PROBE_WINDOW")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_PROBE_WINDOW)
}

/// §P7's subtree aggregate, as carried by a commit: the filesystem's
/// totals at this state, so `statfs` and quota admission are answered
/// from the commit object alone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitAgg {
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub files: u64,
    #[serde(default)]
    pub keys: u64,
    #[serde(default)]
    pub max_mtime: i64,
}

impl From<constellation_mtree::Agg> for CommitAgg {
    fn from(agg: constellation_mtree::Agg) -> CommitAgg {
        CommitAgg {
            bytes: agg.bytes,
            files: agg.files,
            keys: agg.keys,
            max_mtime: agg.max_mtime,
        }
    }
}

/// Why this commit exists: `{kind: "batch", ops: 9812}` for an ordinary
/// publish, or §P9's compact macro (`{kind: "chmod_r", root: …,
/// mode: …}`) whose parameters land in `params`.
///
/// `params` is a flattened free map rather than an enum because the
/// macro registry is versioned and closed *above* this layer (§P9); a
/// reader that does not know a macro must still be able to read the
/// commit's roots, which are the authority — the macro is a bandwidth
/// optimization, never the source of truth.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Intent {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub ops: u64,
    #[serde(default, flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, serde_json::Value>,
}

impl Intent {
    pub fn batch(ops: u64) -> Intent {
        Intent {
            kind: "batch".to_string(),
            ops,
            params: BTreeMap::new(),
        }
    }
}

/// One complete state of the filesystem (§P2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    #[serde(default = "default_version")]
    pub v: u32,
    pub seq: u64,
    /// `seq - 1` for every commit but the first, whose parent is 0.
    /// Recorded rather than inferred so a hole punched by retention is
    /// visible instead of silently bridged.
    pub parent: u64,
    /// Shard id → root node hash (§P4). One entry until sharding
    /// exists; the map is the format so that adding shards is not a
    /// format change.
    pub roots: BTreeMap<String, String>,
    /// Packs this commit created. Not the packs it *reads* — those are
    /// its ancestors' and are found through the packs' own indices.
    pub packs: Vec<String>,
    pub author: u64,
    /// Lease epoch of the author, so a deposed holder's late commit is
    /// recognizable exactly as a late log segment is (§P3: leases are
    /// demoted to policy, and this is the policy guard).
    pub epoch: u64,
    #[serde(default)]
    pub agg: CommitAgg,
    #[serde(default)]
    pub intent: Intent,
    #[serde(default)]
    pub unix_ms: i64,
    /// Log position this commit's tree reflects: partition id → the
    /// highest segment sequence applied to the replica it was built
    /// from (plan 28 §11, S5/S6).
    ///
    /// Two jobs. A bootstrap restores the tree and resumes tailing each
    /// partition from here, exactly as it used to resume from a
    /// checkpoint's `VECTOR.json`. And it is the publisher's guard
    /// against regressing the tree: a replica may only build on a head
    /// whose vector its own dominates component-wise, because a replica
    /// *behind* the head would otherwise overwrite newer values with
    /// the older ones it still holds. Along the chain the vectors
    /// therefore only grow.
    ///
    /// Like the checkpoint it replaces, the tree may additionally hold
    /// its author's not-yet-shipped journal suffix; the author holds
    /// the partition lease, so the log appends that suffix after this
    /// position and replay absorbs it.
    #[serde(default)]
    pub applied: BTreeMap<String, u64>,
}

/// `mine` has applied at least as much of every partition's log as
/// `theirs`. A partition `theirs` names and `mine` does not counts as
/// position 0 — a replica that has never heard of a partition is behind
/// on it by definition.
pub fn vector_covers(mine: &BTreeMap<String, u64>, theirs: &BTreeMap<String, u64>) -> bool {
    theirs
        .iter()
        .all(|(part, seq)| mine.get(part).copied().unwrap_or(0) >= *seq)
}

fn default_version() -> u32 {
    COMMIT_VERSION
}

impl Commit {
    pub fn root(&self, shard: &str) -> Option<NodeHash> {
        NodeHash::from_hex(self.roots.get(shard)?)
    }

    pub fn pack_hashes(&self) -> Result<Vec<PackHash>, StoreError> {
        self.packs
            .iter()
            .map(|hex| {
                PackHash::from_hex(hex)
                    .ok_or_else(|| StoreError::CorruptObject(format!("bad pack hash {hex}")))
            })
            .collect()
    }
}

/// The genesis shard id. One shard until §P4's keyspace sharding lands,
/// and named rather than implied so that the format does not change
/// when it does.
pub const SHARD0: &str = "0";

/// Everything a commit needs except its place in the chain, which the
/// chain assigns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CommitPayload {
    pub roots: BTreeMap<String, NodeHash>,
    pub packs: Vec<PackHash>,
    pub author: u64,
    pub epoch: u64,
    pub agg: CommitAgg,
    pub intent: Intent,
    pub applied: BTreeMap<String, u64>,
}

impl CommitPayload {
    /// A single-shard payload, the shape everything before §P4 writes.
    pub fn single_root(root: NodeHash, packs: Vec<PackHash>) -> CommitPayload {
        CommitPayload {
            roots: BTreeMap::from([(SHARD0.to_string(), root)]),
            packs,
            ..CommitPayload::default()
        }
    }

    pub fn with_author(mut self, author: u64, epoch: u64) -> CommitPayload {
        self.author = author;
        self.epoch = epoch;
        self
    }

    pub fn with_intent(mut self, intent: Intent) -> CommitPayload {
        self.intent = intent;
        self
    }

    pub fn with_agg(mut self, agg: CommitAgg) -> CommitPayload {
        self.agg = agg;
        self
    }

    pub fn with_applied(mut self, applied: BTreeMap<String, u64>) -> CommitPayload {
        self.applied = applied;
        self
    }

    fn at(&self, seq: u64, unix_ms: i64) -> Commit {
        Commit {
            v: COMMIT_VERSION,
            seq,
            parent: seq.saturating_sub(1),
            roots: self
                .roots
                .iter()
                .map(|(shard, hash)| (shard.clone(), hash.to_hex()))
                .collect(),
            packs: self.packs.iter().map(|hash| hash.to_hex()).collect(),
            author: self.author,
            epoch: self.epoch,
            agg: self.agg,
            intent: self.intent.clone(),
            unix_ms,
            applied: self.applied.clone(),
        }
    }
}

/// Read and write `commits/*` against one bucket prefix.
pub struct CommitChain {
    store: Arc<dyn ObjectStore>,
    packs: PackStore,
    probe_window: usize,
    seal: Option<Arc<crate::e2e::TreeSealing>>,
}

impl CommitChain {
    pub fn new(store: Arc<dyn ObjectStore>) -> CommitChain {
        CommitChain {
            packs: PackStore::new(store.clone()),
            store,
            probe_window: probe_window(),
            seal: None,
        }
    }

    /// Seal commit objects under `sealing` (an E2E filesystem).
    pub fn with_sealing(mut self, sealing: Option<crate::e2e::TreeSealing>) -> CommitChain {
        self.seal = sealing.map(Arc::new);
        self
    }

    pub fn with_probe_window(mut self, window: usize) -> CommitChain {
        self.probe_window = window.max(1);
        self
    }

    pub fn probe_width(&self) -> usize {
        self.probe_window
    }

    /// CAS-create one commit. [`StoreError::CasConflict`] means another
    /// writer took this sequence number.
    ///
    /// Public because a caller that has already established durability
    /// its own way (S6's `fsck`, a repair tool) may want the primitive
    /// without [`CommitChain::publish`]'s checks. Ordinary writers
    /// should not: the checks are the invariant.
    pub async fn create(&self, commit: &Commit) -> Result<(), StoreError> {
        let key = layout::commit(commit.seq);
        let json = serde_json::to_vec(commit)?;
        let body = match self.seal.as_deref() {
            Some(seal) => {
                crate::e2e::encrypt_object(&seal.commits, key.as_ref().as_bytes(), &json)?
            }
            None => json,
        };
        match self
            .store
            .put_opts(
                &key,
                PutPayload::from(body),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(StoreError::CasConflict),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get(&self, seq: u64) -> Result<Option<Commit>, StoreError> {
        let key = layout::commit(seq);
        match self.store.get(&key).await {
            Ok(res) => {
                let stored = res.bytes().await?;
                let body = match self.seal.as_deref() {
                    Some(seal) => {
                        crate::e2e::decrypt_object(&seal.commits, key.as_ref().as_bytes(), &stored)?
                    }
                    None => stored.to_vec(),
                };
                let commit: Commit = serde_json::from_slice(&body)?;
                if commit.v != COMMIT_VERSION {
                    return Err(StoreError::CorruptObject(format!(
                        "commit {seq} is version {}, this build reads {COMMIT_VERSION}",
                        commit.v
                    )));
                }
                Ok(Some(commit))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Commits `from, from+1, …` fetched concurrently, truncated at the
    /// first gap. Verbatim in shape from
    /// [`crate::log::LogStore::get_run`], for the reasons that module
    /// gives: a 404 is as cheap as an empty LIST and a twelfth of the
    /// request cost.
    pub async fn get_run(&self, from: u64, k: usize) -> Result<Vec<Commit>, StoreError> {
        use futures::StreamExt;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut fetched =
            futures::stream::iter((0..k as u64).map(|i| self.get(from.saturating_add(i))))
                .buffered(k);
        let mut run = Vec::new();
        while let Some(commit) = fetched.next().await {
            match commit? {
                Some(commit) => run.push(commit),
                None => break,
            }
        }
        Ok(run)
    }

    /// Sequence numbers present, ascending, at or above `from`. The
    /// catch-up fallback, and S7's input.
    pub async fn list_from(&self, from: u64) -> Result<Vec<u64>, StoreError> {
        let prefix = layout::commits_prefix();
        let mut seqs: Vec<u64> = self
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|meta| u64::from_str_radix(meta.location.filename()?, 16).ok())
            .filter(|seq| *seq >= from)
            .collect();
        seqs.sort_unstable();
        Ok(seqs)
    }

    /// Highest sequence number that exists, given that `known` is
    /// already known to.
    ///
    /// The probe answers it in one concurrent round of GETs in the
    /// steady state, where the head is a commit or two ahead. Two cases
    /// hand over to LIST, and both are "catch-up", not "poll":
    ///
    /// - the probe window came back **full**, so the head is at least
    ///   `known + k` and probing again would be a second guess;
    /// - the probe window came back **empty**, which usually means
    ///   `known` is already the head — but can also mean a *gap*: the
    ///   retention sweep deleted `known + 1` while later commits exist,
    ///   and a probe cannot see past a hole it does not know the width
    ///   of. One LIST settles it, and it is the only thing that can.
    pub async fn discover_head(&self, known: u64) -> Result<Option<u64>, StoreError> {
        let k = self.probe_window;
        let run = self.get_run(known.saturating_add(1), k).await?;
        if !run.is_empty() && run.len() < k {
            return Ok(run.last().map(|commit| commit.seq));
        }
        let listed = self.list_from(known.max(1)).await?;
        match listed.last().copied() {
            Some(seq) => Ok(Some(seq.max(known))),
            None if known > 0 => Ok(Some(known)),
            None => Ok(None),
        }
    }

    /// Publish a payload as `parent + 1`, honouring the ordering
    /// invariant and retrying around CAS contention.
    ///
    /// `parent` is the sequence number whose roots the payload was
    /// computed against, not "wherever the head happens to be now" —
    /// that is §P3's rule, and it is what makes a lost update
    /// impossible: if anyone committed in between, this CAS fails and
    /// `rebase` is told exactly what it missed.
    ///
    /// The sequence is fixed and is the design:
    ///
    /// 1. every node the payload names is already packed — the caller
    ///    proves this by having called
    ///    [`NodeCache::seal_packs`], and `cache` is asked to confirm it
    ///    still holds nothing unpacked;
    /// 2. every pack the payload names is present on the bucket;
    /// 3. *then* the commit is CAS-created.
    ///
    /// On a 412 the winner is re-read and handed to `rebase` together
    /// with the payload, so the loser's work is carried forward rather
    /// than dropped. `rebase` returning `Err` abandons the publish and
    /// surfaces the error; that is how a caller declines to merge.
    ///
    /// If `rebase` produces new packs it must have made them durable
    /// itself — step 2 re-runs on every attempt, so a payload naming a
    /// pack that is not on the bucket is refused rather than committed.
    pub async fn publish<F>(
        &self,
        cache: &NodeCache,
        parent: u64,
        payload: CommitPayload,
        mut rebase: F,
        max_attempts: usize,
    ) -> Result<Commit, StoreError>
    where
        F: FnMut(CommitPayload, &Commit) -> Result<CommitPayload, StoreError>,
    {
        if cache.pending_nodes() != 0 {
            return Err(StoreError::Conflict(
                "refusing to commit: the node cache still holds unpacked nodes".into(),
            ));
        }
        let mut payload = payload;
        let mut head = parent;
        for _ in 0..max_attempts.max(1) {
            self.assert_packs_durable(&payload).await?;
            let commit = payload.at(head + 1, now_unix_ms());
            match self.create(&commit).await {
                Ok(()) => return Ok(commit),
                Err(StoreError::CasConflict) => {}
                Err(e) => return Err(e),
            }
            let winner = self.get(head + 1).await?.ok_or_else(|| {
                // The slot refused our create and then read back empty.
                // Nothing may delete an unretired commit, so this is
                // bucket corruption, not a race we can retry through.
                StoreError::CorruptObject(format!(
                    "commit {} refused a create but does not exist",
                    head + 1
                ))
            })?;
            head = self.discover_head(winner.seq).await?.unwrap_or(winner.seq);
            payload = rebase(payload, &winner)?;
        }
        Err(StoreError::CasConflict)
    }

    /// Every pack the payload names is on the bucket, body and index.
    async fn assert_packs_durable(&self, payload: &CommitPayload) -> Result<(), StoreError> {
        for pack in &payload.packs {
            if !self.packs.contains(pack).await? {
                return Err(StoreError::Conflict(format!(
                    "refusing to commit: pack {pack} is not durable"
                )));
            }
        }
        Ok(())
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_cache::NodeCache;
    use crate::packs::PackStore;
    use constellation_fs_core::cache::DiskCache;
    use constellation_mtree::{Hasher, Tree};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as OPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStoreExt, PutMultipartOptions, PutResult,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::TempDir;

    fn cache_for(store: Arc<dyn ObjectStore>, dir: &TempDir) -> Arc<NodeCache> {
        let disk = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        Arc::new(NodeCache::new(
            PackStore::new(store).with_target_bytes(1 << 20),
            disk,
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ))
    }

    fn pairs(n: u64, salt: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let mut key = vec![0x01u8];
                key.extend_from_slice(&i.to_be_bytes());
                (key, vec![salt; 32])
            })
            .collect()
    }

    /// Build a tree, seal its packs, and hand back a payload naming it.
    async fn durable_payload(cache: &Arc<NodeCache>, n: u64, salt: u8) -> CommitPayload {
        let tree = Tree::new(cache.clone());
        let root = tree.build(pairs(n, salt)).unwrap();
        let packs = cache.seal_packs().await.unwrap();
        CommitPayload::single_root(root, packs)
            .with_author(7, 19)
            .with_intent(Intent::batch(n))
            .with_agg(tree.aggregate(&root).unwrap().into())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_round_trips_with_every_p2_field() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir);
        let chain = CommitChain::new(store);
        let payload = durable_payload(&cache, 300, 1)
            .await
            .with_applied(BTreeMap::from([("p0".into(), 41), ("p3".into(), 2)]));
        let root = payload.roots[SHARD0];

        let commit = chain
            .publish(&cache, 0, payload, |p, _| Ok(p), 4)
            .await
            .unwrap();
        assert_eq!((commit.seq, commit.parent), (1, 0));
        let read = chain.get(1).await.unwrap().unwrap();
        assert_eq!(read, commit);
        assert_eq!(read.root(SHARD0), Some(root));
        assert_eq!((read.author, read.epoch), (7, 19));
        assert_eq!(read.intent, Intent::batch(300));
        assert_eq!(read.agg.keys, 300);
        assert_eq!(read.applied.get("p0"), Some(&41));
        assert!(read.unix_ms > 0);
        assert!(!read.packs.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sequence_number_can_never_be_overwritten() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chain = CommitChain::new(store);
        let commit = CommitPayload::default().at(1, 1);
        chain.create(&commit).await.unwrap();
        let mut evil = commit.clone();
        evil.author = 99;
        assert!(matches!(
            chain.create(&evil).await,
            Err(StoreError::CasConflict)
        ));
        assert_eq!(chain.get(1).await.unwrap().unwrap(), commit);
    }

    /// Two writers race for `seq + 1`. One wins; the loser sees 412,
    /// rebases (here: keeps its payload verbatim, which is what a
    /// disjoint write-set does in §P3) and lands at `seq + 2` with its
    /// own roots and its own packs intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_loser_of_a_cas_race_retries_without_losing_its_payload() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let cache_a = cache_for(store.clone(), &dir_a);
        let cache_b = cache_for(store.clone(), &dir_b);
        let chain = CommitChain::new(store);

        let a = durable_payload(&cache_a, 300, 1).await;
        let b = durable_payload(&cache_b, 300, 2).await;
        assert_ne!(a.roots[SHARD0], b.roots[SHARD0]);

        // A wins the slot outright.
        let won = chain
            .publish(&cache_a, 0, a.clone(), |p, _| Ok(p), 4)
            .await
            .unwrap();
        assert_eq!(won.seq, 1);

        // B was built against the same empty parent and now collides.
        let mut saw_winner = None;
        let landed = chain
            .publish(
                &cache_b,
                0,
                b.clone(),
                |payload, winner| {
                    saw_winner = Some(winner.seq);
                    Ok(payload)
                },
                4,
            )
            .await
            .unwrap();
        assert_eq!(saw_winner, Some(1), "the loser must see who beat it");
        assert_eq!(landed.seq, 2);
        assert_eq!(landed.parent, 1);
        assert_eq!(landed.root(SHARD0), Some(b.roots[SHARD0]));
        assert_eq!(
            landed.packs,
            b.packs.iter().map(|p| p.to_hex()).collect::<Vec<_>>(),
            "the loser's packs must survive the retry"
        );
        // And the winner is untouched.
        assert_eq!(chain.get(1).await.unwrap().unwrap(), won);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rebase_that_declines_abandons_the_publish() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir);
        let chain = CommitChain::new(store);
        chain
            .create(&CommitPayload::default().at(1, 1))
            .await
            .unwrap();
        let payload = durable_payload(&cache, 50, 3).await;
        let err = chain
            .publish(
                &cache,
                0,
                payload,
                |_, _| Err(StoreError::Conflict("EEXIST".into())),
                4,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Conflict(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_discovery_probes_then_falls_back_to_list() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chain = CommitChain::new(store).with_probe_window(4);
        assert_eq!(chain.discover_head(0).await.unwrap(), None);

        // Inside the probe window: one concurrent round of GETs finds
        // the head with no LIST.
        for seq in 1..=3u64 {
            chain
                .create(&CommitPayload::default().at(seq, 1))
                .await
                .unwrap();
        }
        assert_eq!(chain.discover_head(0).await.unwrap(), Some(3));

        // Beyond it: the window saturates and only a LIST can say how
        // far behind we really are.
        for seq in 4..=20u64 {
            chain
                .create(&CommitPayload::default().at(seq, 1))
                .await
                .unwrap();
        }
        assert_eq!(chain.discover_head(0).await.unwrap(), Some(20));
        assert_eq!(chain.discover_head(19).await.unwrap(), Some(20));
        assert_eq!(chain.discover_head(20).await.unwrap(), Some(20));
    }

    /// A hole punched by retention (§P10b deletes old commits freely)
    /// is invisible to a probe, which stops at the first 404. The LIST
    /// fallback is the only thing that can see past it, and it must.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_gap_falls_back_to_list() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chain = CommitChain::new(store.clone()).with_probe_window(4);
        for seq in [1u64, 2, 9, 10] {
            chain
                .create(&CommitPayload::default().at(seq, 1))
                .await
                .unwrap();
        }
        // 3..8 are absent, so probing from 2 sees nothing at all.
        assert!(chain.get_run(3, 4).await.unwrap().is_empty());
        assert_eq!(chain.discover_head(2).await.unwrap(), Some(10));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_run_stops_at_the_first_gap() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chain = CommitChain::new(store);
        for seq in [1u64, 2, 3, 5] {
            chain
                .create(&CommitPayload::default().at(seq, 1))
                .await
                .unwrap();
        }
        let run = chain.get_run(1, 8).await.unwrap();
        assert_eq!(run.iter().map(|c| c.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert!(chain.get_run(4, 8).await.unwrap().is_empty());
        assert!(chain.get_run(1, 0).await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payload_naming_an_absent_pack_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir);
        let chain = CommitChain::new(store);
        let mut payload = durable_payload(&cache, 50, 4).await;
        payload.packs.push(PackHash([0xcd; 32]));
        let err = chain
            .publish(&cache, 0, payload, |p, _| Ok(p), 2)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Conflict(_)), "{err}");
        assert_eq!(chain.get(1).await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unpacked_nodes_block_the_commit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir);
        let chain = CommitChain::new(store);
        let tree = Tree::new(cache.clone());
        let root = tree.build(pairs(300, 5)).unwrap();
        // Deliberately skipping `seal_packs`.
        let payload = CommitPayload::single_root(root, Vec::new());
        let err = chain
            .publish(&cache, 0, payload, |p, _| Ok(p), 2)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Conflict(_)), "{err}");
        assert_eq!(chain.get(1).await.unwrap(), None);
    }

    /// A store that starts refusing writes to `commits/*` once armed:
    /// the crash that lands between the pack PUT and the commit CAS,
    /// which is the exact window the ordering invariant is about.
    #[derive(Debug)]
    struct CommitFailingStore {
        inner: InMemory,
        armed: AtomicBool,
    }

    impl std::fmt::Display for CommitFailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CommitFailingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CommitFailingStore {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.armed.load(Ordering::SeqCst) && location.as_ref().starts_with("commits/") {
                return Err(object_store::Error::Generic {
                    store: "CommitFailingStore",
                    source: "crashed before the commit CAS (test injection)".into(),
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

    /// Every commit on the bucket names packs that exist, and every
    /// root it names resolves through them from a cold cache.
    async fn assert_no_commit_names_a_missing_node(
        store: &Arc<dyn ObjectStore>,
        chain: &CommitChain,
    ) {
        for seq in chain.list_from(0).await.unwrap() {
            let commit = chain.get(seq).await.unwrap().unwrap();
            let packs = commit.pack_hashes().unwrap();
            let dir = TempDir::new().unwrap();
            let cold = cache_for(store.clone(), &dir);
            cold.load_pack_indices(&packs).await.unwrap();
            let tree = Tree::new(cold.clone());
            for shard in commit.roots.keys() {
                let root = commit.root(shard).unwrap();
                let census = tree.census(&root).unwrap_or_else(|e| {
                    panic!("commit {seq} names a root that does not resolve: {e}")
                });
                assert!(!census.leaf_entries.is_empty());
            }
        }
    }

    /// The whole crash-safety argument, as a test. A failure injected
    /// between the pack PUT and the commit CAS must leave *only* orphan
    /// packs — never a commit naming a node that is not there — and a
    /// retry must converge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_commit_ever_names_a_missing_node() {
        let failing = Arc::new(CommitFailingStore {
            inner: InMemory::new(),
            armed: AtomicBool::new(true),
        });
        let store = failing.clone() as Arc<dyn ObjectStore>;
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir);
        let chain = CommitChain::new(store.clone());

        let payload = durable_payload(&cache, 2_000, 6).await;
        assert!(!payload.packs.is_empty());

        // The crash: packs are durable, the commit is not.
        assert!(chain
            .publish(&cache, 0, payload.clone(), |p, _| Ok(p), 2)
            .await
            .is_err());
        let listed: Vec<_> = store
            .list(Some(&layout::packs_prefix()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!listed.is_empty(), "the orphan packs must be on the bucket");
        assert_eq!(
            chain.list_from(0).await.unwrap(),
            Vec::<u64>::new(),
            "and no commit may exist"
        );
        assert_no_commit_names_a_missing_node(&store, &chain).await;

        // The retry converges: same content-addressed packs, one commit.
        failing.armed.store(false, Ordering::SeqCst);
        let commit = chain
            .publish(&cache, 0, payload.clone(), |p, _| Ok(p), 2)
            .await
            .unwrap();
        assert_eq!(commit.seq, 1);
        assert_eq!(
            commit.packs,
            payload.packs.iter().map(|p| p.to_hex()).collect::<Vec<_>>()
        );
        assert_no_commit_names_a_missing_node(&store, &chain).await;
    }

    /// The other half of the window: the crash lands *during* the pack
    /// PUTs, so some packs are orphaned and the payload was never
    /// completed. Nothing may commit, and re-sealing must converge on
    /// the same packs rather than duplicating them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crash_during_the_pack_puts_leaves_only_orphans() {
        #[derive(Debug)]
        struct PackFailingStore {
            inner: InMemory,
            fail_after: std::sync::atomic::AtomicUsize,
        }

        impl std::fmt::Display for PackFailingStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "PackFailingStore")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for PackFailingStore {
            async fn put_opts(
                &self,
                location: &OPath,
                payload: PutPayload,
                opts: PutOptions,
            ) -> object_store::Result<PutResult> {
                if location.as_ref().starts_with("packs/")
                    && self.fail_after.fetch_sub(1, Ordering::SeqCst) == 0
                {
                    return Err(object_store::Error::Generic {
                        store: "PackFailingStore",
                        source: "crashed mid pack PUT (test injection)".into(),
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

        let failing = Arc::new(PackFailingStore {
            inner: InMemory::new(),
            // Body of the first pack lands, its index does not.
            fail_after: std::sync::atomic::AtomicUsize::new(1),
        });
        let store = failing.clone() as Arc<dyn ObjectStore>;
        let dir = TempDir::new().unwrap();
        let cache = cache_for(store.clone(), &dir);
        let chain = CommitChain::new(store.clone());

        let tree = Tree::new(cache.clone());
        let root = tree.build(pairs(2_000, 7)).unwrap();
        assert!(cache.seal_packs().await.is_err());
        assert_ne!(
            cache.pending_nodes(),
            0,
            "a failed seal must put the batch back so a retry re-packs it"
        );
        assert_eq!(chain.list_from(0).await.unwrap(), Vec::<u64>::new());

        // Recover: the same nodes re-pack to the same content-addressed
        // packs, so the half-written one is completed rather than
        // duplicated.
        failing.fail_after.store(usize::MAX, Ordering::SeqCst);
        let packs = cache.seal_packs().await.unwrap();
        let commit = chain
            .publish(
                &cache,
                0,
                CommitPayload::single_root(root, packs),
                |p, _| Ok(p),
                2,
            )
            .await
            .unwrap();
        assert_eq!(commit.seq, 1);
        assert_no_commit_names_a_missing_node(&store, &chain).await;

        // No duplicate pack objects: one body and one index per pack.
        let keys: Vec<String> = store
            .list(Some(&layout::packs_prefix()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        let bodies = keys.iter().filter(|k| !k.ends_with(".idx")).count();
        assert_eq!(bodies * 2, keys.len(), "{keys:?}");
        assert_eq!(bodies, commit.packs.len());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_from_the_future_is_refused_not_guessed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chain = CommitChain::new(store.clone());
        let mut commit = CommitPayload::default().at(1, 1);
        commit.v = COMMIT_VERSION + 1;
        store
            .put(
                &layout::commit(1),
                PutPayload::from(serde_json::to_vec(&commit).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(
            chain.get(1).await,
            Err(StoreError::CorruptObject(_))
        ));
    }

    #[test]
    fn an_intent_keeps_its_macro_parameters() {
        let intent = Intent {
            kind: "chmod_r".into(),
            ops: 1,
            params: BTreeMap::from([
                ("root".into(), serde_json::json!(42)),
                ("mode".into(), serde_json::json!(0o644)),
            ]),
        };
        let json = serde_json::to_string(&intent).unwrap();
        assert!(json.contains("\"root\":42"), "{json}");
        assert_eq!(
            serde_json::from_str::<Intent>(&json).unwrap(),
            intent,
            "a macro's parameters must survive a round trip"
        );
    }

    #[test]
    fn vector_cover_is_componentwise_and_treats_unknown_parts_as_zero() {
        let v = |pairs: &[(&str, u64)]| -> BTreeMap<String, u64> {
            pairs.iter().map(|(p, s)| (p.to_string(), *s)).collect()
        };
        assert!(vector_covers(&v(&[("p0", 5)]), &v(&[("p0", 5)])));
        assert!(vector_covers(&v(&[("p0", 6), ("p1", 1)]), &v(&[("p0", 5)])));
        assert!(!vector_covers(
            &v(&[("p0", 6)]),
            &v(&[("p0", 5), ("p1", 1)])
        ));
        assert!(!vector_covers(
            &v(&[("p0", 4), ("p1", 9)]),
            &v(&[("p0", 5)])
        ));
        assert!(vector_covers(&v(&[]), &v(&[("p1", 0)])));
    }
}
