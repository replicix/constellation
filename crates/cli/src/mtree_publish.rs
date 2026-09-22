//! Plan 28 §11 / plan 29 M2: turn the live fjall replica's dirty key set
//! into an `mtree` delta and publish it as a commit.
//!
//! ## The incremental path is native, not derived
//!
//! Plan 29 M1 made `ns`'s key/value encoding *exactly* plan 28 §P6's —
//! the same encoding the published tree uses, modulo one difference:
//! `0x01`/`0x03` values may spill to a *local* blob (`Meta`'s own
//! `blobs` keyspace, plain-hashed) instead of the *published* blob store
//! (bucket-addressed, keyed on E2E). Plan 29 M2 finishes the thought:
//! since M0a/M0b/M0c and M1 already collapsed "which keys changed" into
//! a property the engine itself can track, the publisher no longer
//! re-derives it from the op log (`Touched`, deleted here) or by diffing
//! against a rebuilt tree. Instead:
//!
//! 1. every write to `ns` marks its key in a sibling `dirty` keyspace,
//!    in the *same* transaction (`constellation_meta::store::ns::Dirty`,
//!    centralised in the `ns` write helpers — see plan
//!    `docs/plans/v1/wip/29-fjall-metadata-engine.md`'s M2 section);
//! 2. a publish reads one fjall snapshot, takes `Meta::dirty_snapshot`
//!    as its whole read set, and for each dirty key reads `ns`'s current
//!    value at that key (absent → a delete edit);
//! 3. `0x01`/`0x03` values are "expanded" (resolved against `Meta`'s
//!    local blobs) and re-placed with `constellation_mtree::record::{plan_inode,
//!    place_value}` against the *published* hasher (`BlobStore::hash`,
//!    keyed on E2E), collecting bodies to upload before the commit that
//!    names them (§S4's ordering invariant). Every other key — `0x02`
//!    dentries, `0x04` reverse dentries, `0x30` subsystem records —
//!    copies verbatim: none of them wrap a `Payload`, so there is
//!    nothing to convert;
//! 4. on a successful commit (direct or spliced), `Meta::clear_dirty_upto`
//!    retires exactly the `(key, counter)` pairs this publish observed —
//!    a key re-dirtied by a write this publish never saw keeps a higher
//!    counter and survives the clear, staying dirty for the next round.
//!
//! One consequence worth stating: a restart never needs a rebuild. The
//! dirty set is durable fjall state, not an in-memory `Touched` that a
//! crash erases, so [`TreePublisher::restore`] is just "read back the
//! root/seq/vector this replica last published or bootstrapped onto";
//! whatever is still dirty (survived a crash, or accumulated while no
//! publisher was attached) is picked up by the very next `publish` call
//! exactly as it would be mid-session. `Builder::rebuild` and the old
//! restart-vector-equality check are gone; the equivalent full walk
//! that remains, [`rebuild_root`], exists only for `fsck` and the
//! genesis/bootstrap cases that have no prior tree to diff against.
//!
//! ## Never building on a head this replica is behind
//!
//! Unchanged from S5: a publish adopts the chain head and then writes
//! *this replica's* values for the keys it touched, which is only
//! correct if the replica has applied at least as much of the log as
//! the head reflects. Every commit records the applied vector it was
//! planned at ([`Commit::applied`], read in the same fjall snapshot as
//! the plan), and a publish — or a splice onto a lost race's winner —
//! proceeds only when this replica's vector covers the other one.
//!
//! ## The §P3 splice, restated for key deltas
//!
//! A lost CAS diffs the winner's root against the parent this batch was
//! planned against. The old read set (dirty keys plus every prefix the
//! builder scanned to find them) is gone because there is nothing left
//! to scan: the dirty key set *is* the batch's read set now, since every
//! key this batch could possibly depend on is a key it also (re)writes
//! — reading a dentry's old value, a directory's link count, a scanned
//! `0x03`/`0x04` range for "is anything else there" are all now folded
//! into the write path that already dirties the keys those decisions
//! touch (§P6's "every write locates its own dependents"). So a disjoint
//! splice is safe iff the winner's diff touches none of this batch's
//! edited keys *and* the vector guard holds — [`Plan::conflicts_with`]
//! and [`TreePublisher::splice`] implement exactly that.
//!
//! ## Not stalling FUSE
//!
//! Unchanged from S5: every read goes through a lock-free `fjall`
//! snapshot (`Meta::read_consistent`), and the tree build runs on a
//! blocking thread rather than a runtime worker.

use anyhow::{Context, Result};
use constellation_meta::Meta;
use constellation_mtree::{keys, record, ChangeKind, NodeHash, NodeStore, Tree, VALUE_SPILL};
use constellation_store_s3::log::PARTITION;
use constellation_store_s3::{
    vector_covers, BlobStore, Commit, CommitChain, CommitPayload, Intent, NodeCache, StoreError,
    SHARD0,
};
use std::collections::BTreeMap;
use std::sync::Arc;

/// CAS attempts before a publish gives up and leaves its work pending.
/// Small on purpose: in (B) the writer still holds the partition lease,
/// so repeated contention means something is wrong that another round
/// of retries will not fix.
const PUBLISH_ATTEMPTS: usize = 4;

/// Publish attempts [`TreePublisher::publish_now`] makes before it
/// reports a deferral.
const PUBLISH_NOW_ATTEMPTS: usize = 3;

/// Where the published tree's identity is remembered across restarts.
const KV_ROOT: &str = "mtree/root";
const KV_SEQ: &str = "mtree/commit_seq";
const KV_VECTOR: &str = "mtree/applied_vector";

/// The commit this replica last published or was loaded from, if any.
pub fn remembered_seq(meta: &Meta) -> Result<Option<u64>> {
    Ok(meta.kv_get(KV_SEQ)?.and_then(|seq| seq.parse().ok()))
}

/// Record a commit this replica's state was loaded from (S6's/M2's
/// bootstrap), so the node's publisher picks it up as its parent instead
/// of ever needing a rebuild. Unlike S5, there is no vector-equality
/// re-check on restore: a bootstrap that ingested this commit left
/// `dirty` empty for everything it loaded (`Meta::clear_all_dirty`), so
/// whatever is dirty by the time a publisher attaches is genuinely new
/// local or tailed work, not a sign the tree fell behind.
pub fn remember_loaded_commit(meta: &Meta, commit: &Commit) -> Result<()> {
    let Some(root) = commit.root(SHARD0) else {
        return Ok(());
    };
    meta.kv_set(KV_ROOT, &root.to_hex())?;
    meta.kv_set(KV_SEQ, &commit.seq.to_string())?;
    meta.kv_set(KV_VECTOR, &encode_vector(commit.applied))?;
    Ok(())
}

/// A batch turned into tree edits: dirty keys, converted to their
/// published form, keyed so the batch is deduplicated and sorted by
/// construction (`Tree::apply` demands strictly ascending keys).
#[derive(Debug, Default)]
pub struct Plan {
    edits: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Bodies that exceeded `VALUE_SPILL` and must be durable before the
    /// commit that names them.
    blobs: Vec<Vec<u8>>,
    /// Dirty keys this plan covers, for the commit's intent.
    entities: usize,
}

impl Plan {
    fn upsert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.edits.insert(key, Some(value));
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.edits.insert(key, None);
    }

    fn edits(&self) -> Vec<constellation_mtree::Edit> {
        self.edits
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.edits.len()
    }

    /// True when a winner's change set touches any key this batch edits.
    /// The dirty set *is* the read set now (see the module docs), so
    /// this is the whole of §P3's disjointness check.
    fn conflicts_with(&self, changed: &[(Vec<u8>, ChangeKind)]) -> bool {
        changed.iter().any(|(key, _)| self.edits.contains_key(key))
    }
}

/// Builds and publishes the §P6 tree for one replica.
///
/// Owned by `shipper::Shipper` and driven by its publish cadence (every
/// `PUBLISH_EVERY` shipped segments, on idle, and at shutdown).
pub struct TreePublisher {
    meta: Arc<Meta>,
    cache: Arc<NodeCache>,
    blobs: BlobStore,
    chain: CommitChain,
    config: constellation_mtree::Config,
    node_id: u64,
    /// The root this replica last published, and the commit that names
    /// it. `None` until the first publish.
    state: Option<Published>,
    /// Whether the node cache has loaded the pack catalog. False until
    /// the first publish of every process (see `new`).
    hydrated: bool,
    /// When a head discovery last proved the chain empty (a LIST found no
    /// commit). Until the commit retention age has passed since then, no
    /// commit can have been created *and* pruned, so an empty probe at
    /// seq 1 is proof enough that the chain is still empty.
    empty_chain_seen: Option<std::time::Instant>,
    handle: tokio::runtime::Handle,
}

#[derive(Clone, Debug)]
struct Published {
    root: NodeHash,
    seq: u64,
    /// The commit's [`Commit::applied`] position: what this tree reflects.
    applied: Vector,
}

/// Highest applied log segment. One metadata stream since plan 29 M0a
/// removed namespace partitions.
type Vector = u64;

/// How one publish attempt ended.
enum Outcome {
    Committed(Box<Commit>),
    /// The tree already reflects the batch (an empty dirty set, or a
    /// plan whose net effect reproduces the published root exactly);
    /// nothing to commit.
    Unchanged,
    /// Not now: behind the head, or a declined splice. `dirty` is left
    /// untouched, so the next round picks the same keys back up.
    Deferred,
}

/// What the blocking half of a publish decided.
enum Planned {
    /// A plan, the root it produces, the vector it was read at, and the
    /// `dirty_snapshot` this plan's read set came from — handed back to
    /// `clear_dirty_upto` on success.
    Ready(Plan, NodeHash, Vector, Vec<(Vec<u8>, u64)>),
    /// Nothing was dirty.
    Empty,
    /// This replica has applied less of the log than the head it would
    /// build on, so every value it holds for a key it touched might be
    /// older than the head's. Publishing would regress the tree; the
    /// tailer closes the gap and a later round publishes.
    Behind { mine: Vector, head: Vector },
}

impl TreePublisher {
    pub fn new(
        meta: Arc<Meta>,
        cache: Arc<NodeCache>,
        blobs: BlobStore,
        chain: CommitChain,
        config: constellation_mtree::Config,
        node_id: u64,
        handle: tokio::runtime::Handle,
    ) -> TreePublisher {
        TreePublisher {
            meta,
            cache,
            blobs,
            chain,
            config,
            node_id,
            state: None,
            // Always hydrate before the first publish, restored root or
            // not: the first publish applies edits onto whatever root
            // `restore` found, and only a cache that knows which packs
            // hold its nodes can resolve them.
            hydrated: false,
            empty_chain_seen: None,
            handle,
        }
    }

    #[cfg(test)]
    pub fn published(&self) -> Option<(NodeHash, u64)> {
        self.state.as_ref().map(|s| (s.root, s.seq))
    }

    /// Pick the published root back up after a restart.
    ///
    /// No longer validates the tree against the replica's applied-seq
    /// vector: a node's dirty set persists in `fjall` across restarts,
    /// so there is nothing a restart could have silently lost that
    /// would make a rebuild necessary (see the module docs). A missing
    /// or corrupt kv entry just means no publish or bootstrap has ever
    /// landed on this replica, which `publish_batch` already handles
    /// (`state: None`, first publish builds on the empty tree).
    pub fn restore(&mut self) -> Result<()> {
        let (Some(root_hex), Some(seq)) = (self.meta.kv_get(KV_ROOT)?, self.meta.kv_get(KV_SEQ)?)
        else {
            return Ok(());
        };
        let (Some(root), Ok(seq)) = (NodeHash::from_hex(&root_hex), seq.parse::<u64>()) else {
            return Ok(());
        };
        let applied = self
            .meta
            .kv_get(KV_VECTOR)?
            .and_then(|s| parse_vector(&s))
            .unwrap_or(0);
        self.state = Some(Published { root, seq, applied });
        self.hydrated = false;
        Ok(())
    }

    /// Teach a cold node cache where the published root's nodes live.
    ///
    /// A restart restores the root hash from the replica's kv store,
    /// but the cache that has to resolve it has never seen a pack. The
    /// nodes are spread over packs written by every ancestor commit —
    /// some of them retired by retention, and after compaction some in
    /// packs no commit names — so the pack catalog is the only complete
    /// map. Once per process, and only when a root was restored rather
    /// than written.
    async fn hydrate(&mut self) -> Result<()> {
        if self.hydrated {
            return Ok(());
        }
        crate::mtree_read::attach_catalog(&self.cache).await?;
        self.hydrated = true;
        Ok(())
    }

    /// Do a first publish's one-time discovery (pack catalog LIST, chain
    /// head) now rather than on the first publish, which the idle-publish
    /// cadence otherwise defers into a quiet period. Best effort: a
    /// failure here just leaves the work to the first publish.
    pub async fn warm_up(&mut self) -> Result<()> {
        self.hydrate().await?;
        self.adopt_head().await?;
        Ok(())
    }

    fn tree(&self) -> Result<Tree<Arc<NodeCache>>, StoreError> {
        Ok(Tree::with_config(self.cache.clone(), self.config)?)
    }

    /// Publish the current dirty set, if there is one.
    ///
    /// Returns the commit that landed, or `None` when there was nothing
    /// to do or when the publish deferred to let the tailer catch up.
    ///
    /// **Cancellation- and crash-safe**: nothing about the batch lives
    /// only in this process's memory. `dirty` is only ever *read* while
    /// a publish is in flight, and cleared — durably, in `fjall` — only
    /// once a commit has actually landed. Dropping this future at any
    /// await point (the daemon's sync loop does exactly that whenever an
    /// explicit request arrives) leaves `dirty` exactly as it was, so
    /// the next publish (in this process or, after a crash, the next
    /// one) picks the same keys back up.
    pub async fn publish(&mut self, epoch: u64) -> Result<Option<Commit>> {
        if !self.meta.has_dirty() {
            return Ok(None);
        }
        let started = std::time::Instant::now();
        match self.publish_batch(epoch).await {
            Ok(Outcome::Unchanged) => Ok(None),
            Ok(Outcome::Deferred) => Ok(None),
            Err(e) => Err(e),
            Ok(Outcome::Committed(commit)) => {
                let commit = *commit;
                tracing::info!(
                    seq = commit.seq,
                    root = %commit.roots.get(SHARD0).map(String::as_str).unwrap_or(""),
                    keys = commit.intent.ops,
                    packs = commit.packs.len(),
                    ms = started.elapsed().as_millis(),
                    "published metadata commit"
                );
                Ok(Some(commit))
            }
        }
    }

    /// Publish whatever is dirty and return the commit that now
    /// reflects this replica: the new one, or — when nothing was dirty
    /// — the last one, which already does. A deferred publish is an
    /// error, because a caller asking for "now" (a snapshot) must not be
    /// handed an older state.
    pub async fn publish_now(&mut self, epoch: u64) -> Result<(u64, NodeHash)> {
        // A deferral that the next attempt resolves by itself — most
        // often a pack GC condemned after this batch deduplicated against
        // it, which the retry re-uploads — is retried here rather than
        // surfaced, because the caller is waiting.
        for _ in 0..PUBLISH_NOW_ATTEMPTS {
            if let Some(commit) = self.publish(epoch).await? {
                let root = commit
                    .root(SHARD0)
                    .with_context(|| format!("commit {} names no shard 0 root", commit.seq))?;
                return Ok((commit.seq, root));
            }
            if !self.meta.has_dirty() {
                break;
            }
        }
        if self.meta.has_dirty() {
            anyhow::bail!(
                "the metadata publish was deferred (this replica is behind the chain head, \
                 or a concurrent commit overlaps its batch); retry shortly"
            );
        }
        let state = self
            .state
            .as_ref()
            .context("nothing has been published on this mount yet")?;
        Ok((state.seq, state.root))
    }

    async fn publish_batch(&mut self, epoch: u64) -> Result<Outcome> {
        // Adopt whatever the chain's head is before planning. A commit
        // from another writer is the normal reason our parent moved,
        // and its records have already reached this replica through the
        // log — so re-planning against the winner's root is the cheap,
        // correct thing to do, and it leaves the rebase callback for
        // the genuinely concurrent case.
        self.hydrate().await?;
        let parent = self.adopt_head().await?;
        // GC handshake (S7b): never deduplicate a node against a pack a
        // GC round has condemned, and remember which packs this batch
        // did deduplicate against, to re-check just before the CAS.
        let backend = self.cache.packs().inner();
        self.cache
            .set_condemned(constellation_store_s3::read_condemned_packs(&backend).await?);
        self.cache.start_dedup_log();
        let base = self.state.as_ref().map(|s| s.root);
        let head_applied = self.state.as_ref().map(|s| s.applied);

        let meta = Arc::clone(&self.meta);
        let cache = Arc::clone(&self.cache);
        let config = self.config;
        let blobs = self.blobs.clone();
        // The tree build reads fjall and decompresses nodes. Tens of
        // milliseconds on a large batch, so it does not belong on a
        // runtime worker any more than `VACUUM INTO` did.
        //
        // The vector, the dirty snapshot and every read the plan makes
        // share one fjall snapshot (`read_consistent`), so the commit's
        // `applied` claim, the read set and the values planned are
        // provably the same point-in-time view.
        let planned = tokio::task::spawn_blocking(move || -> Result<Planned> {
            let tree = Tree::with_config(cache, config).map_err(StoreError::from)?;
            meta.read_consistent(|snap| -> Result<Planned> {
                let vector = meta.applied_seq_at(snap)?;
                if let Some(head) = head_applied.filter(|head| !vector_covers(vector, *head)) {
                    return Ok(Planned::Behind { mine: vector, head });
                }
                let dirty = meta.dirty_snapshot(snap)?;
                if dirty.is_empty() {
                    return Ok(Planned::Empty);
                }
                let plan = plan_from_dirty(&meta, snap, &blobs, &dirty)?;
                let base_for_apply = match base {
                    Some(root) => root,
                    None => tree.empty().map_err(StoreError::from)?,
                };
                let root = tree
                    .apply(&base_for_apply, &plan.edits())
                    .map_err(StoreError::from)?;
                Ok(Planned::Ready(plan, root, vector, dirty))
            })
        })
        .await
        .context("metadata tree build task")??;
        let (plan, root, vector, observed_dirty) = match planned {
            Planned::Ready(plan, root, vector, dirty) => (plan, root, vector, dirty),
            Planned::Empty => return Ok(Outcome::Unchanged),
            Planned::Behind { mine, head } => {
                tracing::debug!(
                    mine = encode_vector(mine),
                    head = encode_vector(head),
                    "metadata publish deferred: this replica is behind the chain head"
                );
                return Ok(Outcome::Deferred);
            }
        };

        if Some(root) == base {
            // The published root already reflects the replica at
            // `vector` — an empty net effect (every dirty value already
            // matches what is published). Nothing to commit, but the
            // dirty set this plan covered is genuinely reconciled, so
            // it still clears.
            if let Some(state) = self.state.as_mut() {
                state.applied = vector;
            }
            self.remember()?;
            self.meta.clear_dirty_upto(&observed_dirty)?;
            return Ok(Outcome::Unchanged);
        }

        // Ordering invariant (§P2/S4), extended to blobs: every byte
        // the tree names is durable before the commit exists. Blobs
        // first because a pack may name a node whose value names a
        // blob, and packs are sealed below.
        let blob_hashes: Vec<_> = plan
            .blobs
            .iter()
            .map(|body| self.blobs.hash(body))
            .collect();
        self.blobs.put_all(plan.blobs.clone()).await?;
        let packs = self.cache.seal_packs().await?;

        // The other half of the GC handshake: a pack this batch trusted
        // as holding an unchanged node must still exist and must not
        // have been condemned since planning. Otherwise its locations
        // are forgotten and the batch is re-planned next round, which
        // uploads those nodes afresh. GC waits a lease TTL between
        // condemning and deleting, which covers the gap from here to
        // the CAS.
        let condemned = constellation_store_s3::read_condemned_packs(&backend).await?;
        if !self.cache.dedup_is_sound(&condemned).await? {
            tracing::info!(
                "metadata publish deferred: a pack it deduplicated against is condemned by GC"
            );
            return Ok(Outcome::Deferred);
        }

        // Plan 29 M3a's blob twin: `BlobStore::put` above treats an
        // already-present object as a dedup hit and returns success
        // without re-checking liveness, so a blob this batch is about
        // to name in the commit could be deleted by GC's two-mark
        // horizon in the gap between that PUT and this commit's CAS.
        // Re-reading the condemned-blob list right here, rather than
        // trusting the one read (if any) at planning time, is what
        // closes it — mirroring the pack check immediately above: a
        // commit that still names a condemned blob is deferred rather
        // than let through, and the retry either lands after GC's
        // grace wait cleared the condemnation or re-uploads into a
        // fresh, uncondemned object (content-addressed, so "fresh" is
        // simply the same PUT succeeding for real instead of hitting
        // `AlreadyExists`).
        let condemned_blobs = constellation_store_s3::read_condemned_blobs(&backend).await?;
        if blob_hashes
            .iter()
            .any(|hash| condemned_blobs.contains(hash))
        {
            tracing::info!("metadata publish deferred: a blob it references is condemned by GC");
            return Ok(Outcome::Deferred);
        }

        let tree = self.tree()?;
        let agg = tree.aggregate(&root).map_err(StoreError::from)?;
        let payload = CommitPayload::single_root(root, packs)
            .with_author(self.node_id, epoch)
            .with_intent(Intent::batch(plan.len() as u64))
            .with_agg(agg.into())
            .with_applied(vector);

        // §P3's rebase. `spliced` carries the root the *current*
        // payload was computed against, because a second lost race must
        // diff from the first winner rather than from our own parent.
        // Every capture is owned: a closure borrowing locals here makes
        // the enclosing future's `Send` bound rank-restricted, and the
        // daemon spawns it.
        let spliced = std::cell::Cell::new(base);
        let declined: Arc<std::sync::Mutex<Option<String>>> = Arc::default();
        let noted = Arc::clone(&declined);
        let cache = Arc::clone(&self.cache);
        let handle = self.handle.clone();
        let mine = vector;
        let commit = self
            .chain
            .publish(
                &self.cache,
                parent,
                payload,
                move |payload: CommitPayload, winner: &Commit| {
                    let from = spliced.get();
                    match Self::splice(&tree, &cache, &handle, &plan, mine, from, winner, payload) {
                        Ok((next, root)) => {
                            spliced.set(Some(root));
                            Ok(next)
                        }
                        Err(e) => {
                            *noted.lock().expect("declined reason") = Some(e.to_string());
                            Err(e)
                        }
                    }
                },
                PUBLISH_ATTEMPTS,
            )
            .await;
        let declined = declined.lock().expect("declined reason").clone();
        match commit {
            Ok(commit) => {
                self.state = Some(Published {
                    // The splice may have moved the root off the one we
                    // computed; the commit is the authority on which
                    // root actually landed.
                    root: commit.root(SHARD0).unwrap_or(root),
                    seq: commit.seq,
                    applied: vector,
                });
                self.remember()?;
                self.meta.clear_dirty_upto(&observed_dirty)?;
                Ok(Outcome::Committed(Box::new(commit)))
            }
            // A refused splice is not a failure of this publish so much
            // as a statement that the batch has to be re-executed
            // against state this replica has not applied yet. `dirty`
            // is left untouched — the tailer supplies what re-execution
            // needs, and the next round re-plans the same keys from the
            // winner's root, which is re-execution in §P3's sense,
            // deferred by one round.
            Err(StoreError::Conflict(_)) if declined.is_some() => {
                tracing::info!(
                    reason = declined.unwrap_or_default(),
                    "metadata publish deferred: the winning commit overlaps this batch"
                );
                Ok(Outcome::Deferred)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// The §P3 splice: re-apply this batch's write-set onto the
    /// winner's root when — and only when — the winner changed nothing
    /// the batch depended on.
    ///
    /// Not a retry. A retry would re-CAS this replica's root, which was
    /// computed from a parent the winner has since replaced, and would
    /// therefore delete every key the winner added and restore every
    /// key it removed. The difference is invisible until two writers
    /// with disjoint key sets publish concurrently, which is what
    /// `two_publishers_with_disjoint_keys_both_survive` constructs.
    #[allow(clippy::too_many_arguments)]
    fn splice(
        tree: &Tree<Arc<NodeCache>>,
        cache: &Arc<NodeCache>,
        handle: &tokio::runtime::Handle,
        plan: &Plan,
        mine: Vector,
        from: Option<NodeHash>,
        winner: &Commit,
        payload: CommitPayload,
    ) -> Result<(CommitPayload, NodeHash), StoreError> {
        // The same guard `publish_batch` applies to the head it adopts,
        // applied to the head it lost to: a winner that has seen log
        // this replica has not may hold newer values for keys in this
        // batch, and a splice would overwrite them. `conflicts_with`
        // cannot see that case, because the stale value is ours and
        // never appears in the winner's diff.
        if !vector_covers(mine, winner.applied) {
            return Err(StoreError::Conflict(format!(
                "commit {} reflects log this replica has not applied ({} against {})",
                winner.seq,
                encode_vector(winner.applied),
                encode_vector(mine)
            )));
        }
        let winner_root = winner.root(SHARD0).ok_or_else(|| {
            StoreError::CorruptObject(format!("commit {} names no shard 0 root", winner.seq))
        })?;
        // The winner's nodes live in the winner's packs, which this
        // replica has never read. Without their indices every descent
        // into its tree is a missing node.
        let packs = winner.pack_hashes()?;
        blocking(handle, || cache.load_pack_indices(&packs))?;

        let changed = match from {
            Some(from) => tree.diff(&from, &winner_root)?,
            // No parent to diff against: this replica's first publish
            // lost to someone else's, so every key the winner holds is
            // news to it and there is nothing safe to splice.
            None => {
                return Err(StoreError::Conflict(
                    "no parent root to diff the winner against".into(),
                ))
            }
        };
        if plan.conflicts_with(&changed) {
            return Err(StoreError::Conflict(format!(
                "commit {} changed {} keys, overlapping this batch's edits",
                winner.seq,
                changed.len()
            )));
        }
        let root = tree.apply(&winner_root, &plan.edits())?;
        let packs = blocking(handle, || cache.seal_packs())?;
        Ok((
            CommitPayload {
                roots: std::collections::BTreeMap::from([(SHARD0.to_string(), root)]),
                packs,
                ..payload
            },
            root,
        ))
    }

    /// Move our idea of the chain head forward, so the CAS aims at the
    /// slot after the newest commit rather than at one that is taken.
    async fn adopt_head(&mut self) -> Result<u64> {
        let known = self.state.as_ref().map(|s| s.seq).unwrap_or(0);
        let retention = std::time::Duration::from_secs(
            std::env::var("CONSTELLATION_COMMIT_RETENTION_S")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(crate::mtree_gc::DEFAULT_COMMIT_RETENTION_S),
        );
        if known == 0
            && self
                .empty_chain_seen
                .is_some_and(|seen| seen.elapsed() < retention / 2)
            && self.chain.get(1).await?.is_none()
        {
            return Ok(0);
        }
        let Some(head) = self.chain.discover_head(known).await? else {
            if known == 0 {
                self.empty_chain_seen = Some(std::time::Instant::now());
            }
            return Ok(0);
        };
        if head <= known {
            return Ok(known);
        }
        let Some(commit) = self.chain.get(head).await? else {
            return Ok(known);
        };
        let Some(root) = commit.root(SHARD0) else {
            return Ok(known);
        };
        self.cache.load_pack_indices(&commit.pack_hashes()?).await?;
        tracing::info!(
            from = known,
            to = head,
            author = commit.author,
            "adopting a newer metadata commit as this publish's parent"
        );
        self.state = Some(Published {
            root,
            seq: head,
            applied: commit.applied,
        });
        Ok(head)
    }

    /// Persist the published root with the vector it was *planned* at,
    /// not the replica's vector now: a segment applied since the plan
    /// makes the two differ, and a later restart's guard (§P3's
    /// disjointness check, not a rebuild — see the module docs) must
    /// see the vector the tree actually reflects.
    fn remember(&self) -> Result<()> {
        let Some(state) = self.state.as_ref() else {
            return Ok(());
        };
        self.meta.kv_set(KV_ROOT, &state.root.to_hex())?;
        self.meta.kv_set(KV_SEQ, &state.seq.to_string())?;
        self.meta.kv_set(KV_VECTOR, &encode_vector(state.applied))?;
        Ok(())
    }
}

/// Run one async step from inside the synchronous rebase callback.
///
/// The callback is handed to `CommitChain::publish` as a plain closure
/// — resolving a lost CAS needs the §P6 codec, which `store-s3` cannot
/// see, so the hook has to be synchronous — but loading a pack index
/// and sealing packs are I/O. `block_in_place` is the same bridge
/// `NodeCache` already uses for exactly this reason.
fn blocking<F, T>(handle: &tokio::runtime::Handle, f: impl FnOnce() -> F) -> Result<T, StoreError>
where
    F: std::future::Future<Output = Result<T, StoreError>>,
{
    let fut = f();
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => handle.block_on(fut),
    }
}

fn encode_vector(applied: Vector) -> String {
    format!("{PARTITION}:{applied}")
}

fn parse_vector(s: &str) -> Option<Vector> {
    s.rsplit(':').next()?.parse().ok()
}

// ------------------------------------------------------- key conversion

/// The three cases a `ns` key's current value falls into when it moves
/// from local to published form: `0x01` and `0x03` wrap a `Payload` that
/// may point at a *local* blob and must be re-placed against the
/// *published* hasher; everything else copies verbatim (`value` is
/// already known present).
fn republish_present(
    meta: &Meta,
    snap: &fjall::Snapshot,
    blobs: &BlobStore,
    key: &[u8],
    value: &[u8],
) -> Result<(Vec<u8>, Vec<Vec<u8>>)> {
    match keys::Key::parse(key)? {
        keys::Key::Inode { ino } => {
            let row = meta.tree_inode_at(snap, ino)?.with_context(|| {
                format!("inode {ino}: present in ns but tree_inode_at found nothing")
            })?;
            let xattrs: Vec<(Vec<u8>, Vec<u8>)> = row
                .xattrs
                .iter()
                .map(|(name, value)| (name.as_bytes().to_vec(), value.clone()))
                .collect();
            let planned = record::plan_inode(
                attrs_of(&row.attr),
                row.manifest.clone(),
                row.target.as_ref().map(|t| t.as_bytes().to_vec()),
                &xattrs,
                |bytes| blobs.hash(bytes),
            );
            Ok((planned.record.encode(), planned.blobs))
        }
        keys::Key::Xattr { .. } => {
            let payload = record::Payload::decode(value)?;
            let bytes = meta.resolve_local_payload_at(snap, &payload)?;
            let (payload, blob) = record::place_value(bytes, |b| blobs.hash(b));
            debug_assert!(
                payload.encoded_len() <= VALUE_SPILL + 1,
                "a spilled payload must be a hash"
            );
            Ok((payload.encode(), blob.into_iter().collect()))
        }
        keys::Key::Dentry { .. } | keys::Key::RDentry { .. } | keys::Key::Subsystem { .. } => {
            Ok((value.to_vec(), Vec::new()))
        }
    }
}

/// Turn a `dirty_snapshot` into a [`Plan`]: for each key, read `ns`'s
/// current raw value under the same snapshot (absent → delete edit),
/// converting `0x01`/`0x03` payloads to their published form.
fn plan_from_dirty(
    meta: &Meta,
    snap: &fjall::Snapshot,
    blobs: &BlobStore,
    dirty: &[(Vec<u8>, u64)],
) -> Result<Plan> {
    let mut plan = Plan::default();
    for (key, _counter) in dirty {
        match meta.ns_get_at(snap, key)? {
            None => plan.remove(key.clone()),
            Some(value) => {
                let (published, new_blobs) = republish_present(meta, snap, blobs, key, &value)?;
                plan.blobs.extend(new_blobs);
                plan.upsert(key.clone(), published);
            }
        }
    }
    plan.entities = dirty.len();
    Ok(plan)
}

/// Build the tree the replica's current state calls for, from nothing,
/// into `tree`'s store — `fsck`'s half of "recompute the root hash and
/// compare", and the genesis/bootstrap escape hatch for a replica with
/// no prior published root to diff against. Nothing is uploaded:
/// spilled values are hashed, never stored.
///
/// A full walk rather than the incremental path: unlike a publish, this
/// has no dirty set to work from (independent verification is the
/// point), so it visits every key in `ns` once. `Tree::build`'s own
/// memory cost is bounded (one pending run per level); the input vector
/// this assembles is not, which is an acceptable trade for a maintenance
/// operation that already reads and hashes the whole tree elsewhere
/// (`TreeReader::verify`).
pub fn rebuild_root<S: NodeStore>(
    meta: &Meta,
    tree: &Tree<S>,
    blobs: &BlobStore,
) -> Result<NodeHash> {
    meta.read_consistent(|snap| -> Result<NodeHash> {
        let dump = meta.ns_dump_at(snap)?;
        let mut pairs = Vec::with_capacity(dump.len());
        for (key, value) in &dump {
            let (published, _blobs) = republish_present(meta, snap, blobs, key, value)?;
            pairs.push((key.clone(), published));
        }
        Ok(tree.build(pairs).map_err(StoreError::from)?)
    })
}

/// `fs-core`'s attrs as §P6 stores them: the same fields minus atime,
/// which the tree does not have a place for (§P6, `record::Attrs`).
///
/// The kind mapping is a cast because S3 made `record::Kind`'s
/// discriminants match `InodeKind::as_u8` on purpose; `from_u8` is
/// still called rather than transmuted so a future divergence is an
/// error instead of a reinterpretation.
fn attrs_of(attr: &constellation_fs_core::FileAttr) -> record::Attrs {
    record::Attrs {
        kind: record::Kind::from_u8(attr.kind.as_u8()).unwrap_or(record::Kind::File),
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        size: attr.size,
        mtime_ns: attr.mtime_ns,
        ctime_ns: attr.ctime_ns,
        rdev: attr.rdev,
    }
}

/// The daemon spawns the sync loop that drives a publish, so the
/// publish future has to be `Send` — and "not `Send`" shows up as a
/// rank-restricted `FnOnce` bound thousands of lines away in
/// `node_runtime`, which is an unpleasant way to learn it. Stated here
/// instead, where the cause would be.
#[allow(dead_code)]
fn assert_publish_future_is_send(publisher: &mut TreePublisher) {
    fn is_send<T: Send>(_: T) {}
    is_send(publisher.publish(0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::cache::DiskCache;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_meta::{LogRecord, MetaStore, SetXattrMode};
    use constellation_mtree::{Hasher, Payload};
    use constellation_store_s3::PackStore;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::TempDir;

    /// A replica, a bucket, and however many publishers a test needs.
    struct Fixture {
        store: Arc<dyn ObjectStore>,
        meta: Arc<Meta>,
        dirs: Vec<TempDir>,
    }

    impl Fixture {
        fn new() -> Fixture {
            Fixture {
                store: Arc::new(InMemory::new()),
                meta: Arc::new(Meta::open_in_memory().unwrap()),
                dirs: Vec::new(),
            }
        }

        fn cache(&mut self, store: Arc<dyn ObjectStore>) -> Arc<NodeCache> {
            let dir = TempDir::new().unwrap();
            let disk = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
            self.dirs.push(dir);
            Arc::new(NodeCache::new(
                PackStore::new(store).with_target_bytes(1 << 20),
                disk,
                Hasher::Plain,
                tokio::runtime::Handle::current(),
            ))
        }

        fn publisher_over(
            &mut self,
            meta: Arc<Meta>,
            store: Arc<dyn ObjectStore>,
            node_id: u64,
        ) -> TreePublisher {
            let cache = self.cache(store.clone());
            TreePublisher::new(
                meta,
                cache,
                BlobStore::new(store.clone(), Hasher::Plain),
                CommitChain::new(store),
                record::config(),
                node_id,
                tokio::runtime::Handle::current(),
            )
        }

        fn publisher_on(&mut self, store: Arc<dyn ObjectStore>, node_id: u64) -> TreePublisher {
            let meta = Arc::clone(&self.meta);
            self.publisher_over(meta, store, node_id)
        }

        fn publisher(&mut self, node_id: u64) -> TreePublisher {
            let store = Arc::clone(&self.store);
            self.publisher_on(store, node_id)
        }

        /// Fork a second, independently-replicated node for the §P3
        /// race tests: a fresh `Meta` under its own ino prefix, replayed
        /// to `self.meta`'s current state (so both agree on ino
        /// numbering), with its own publisher already `restore`d onto
        /// whatever `from` has last committed — exactly what a real
        /// second machine's bootstrap would leave it with. From here on
        /// the two replicas are genuinely separate: each has its own
        /// `dirty` keyspace, so mutating one never touches the other's.
        fn peer(
            &mut self,
            node_id: u64,
            store: Arc<dyn ObjectStore>,
            from: &TreePublisher,
        ) -> Peer {
            let meta = Arc::new(Meta::open_in_memory().unwrap());
            meta.set_node_prefix(node_id).unwrap();
            let records: Vec<LogRecord> = self
                .meta
                .peek_journal_after(0)
                .unwrap()
                .into_iter()
                .map(|(_, r)| r)
                .collect();
            meta.apply_records(&records).unwrap();
            meta.set_applied_seq(self.meta.applied_seq().unwrap())
                .unwrap();
            if let Some((root, seq)) = from.published() {
                meta.kv_set(KV_ROOT, &root.to_hex()).unwrap();
                meta.kv_set(KV_SEQ, &seq.to_string()).unwrap();
                meta.kv_set(KV_VECTOR, &encode_vector(meta.applied_seq().unwrap()))
                    .unwrap();
                // This replica's content is exactly what `from`'s commit
                // already published (the records just replayed are the
                // same ones that produced it), so — like a real
                // bootstrap — it starts with nothing to publish. Without
                // this, the replay above would leave every replayed key
                // dirty, and this batch's plan would spuriously include
                // them (as no-op edits) alongside whatever this peer
                // genuinely changes, tripping `conflicts_with` on a key
                // the winner touched that this peer only "touches" by
                // republishing an unchanged value.
                meta.clear_all_dirty().unwrap();
            }
            let mut publisher = self.publisher_over(Arc::clone(&meta), store, node_id);
            publisher.restore().unwrap();
            Peer { meta, publisher }
        }

        /// A tree over a cold cache holding every pack every commit has
        /// written — what a reader in S6 will do, and the only honest
        /// way to ask what a published root actually contains.
        async fn reader(&mut self) -> (Tree<Arc<NodeCache>>, Vec<Commit>) {
            let store = Arc::clone(&self.store);
            let cache = self.cache(Arc::clone(&self.store));
            let chain = CommitChain::new(store);
            let mut commits = Vec::new();
            for seq in chain.list_from(0).await.unwrap() {
                let commit = chain.get(seq).await.unwrap().unwrap();
                cache
                    .load_pack_indices(&commit.pack_hashes().unwrap())
                    .await
                    .unwrap();
                commits.push(commit);
            }
            (Tree::with_config(cache, record::config()).unwrap(), commits)
        }
    }

    /// A second node in a §P3 race test (see `Fixture::peer`).
    struct Peer {
        meta: Arc<Meta>,
        publisher: TreePublisher,
    }

    fn dirty_keys(meta: &Meta) -> BTreeSet<Vec<u8>> {
        meta.read_consistent(|snap| meta.dirty_snapshot(snap))
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect()
    }

    async fn publish(publisher: &mut TreePublisher) -> Option<Commit> {
        publisher.publish(7).await.unwrap()
    }

    fn seed(meta: &Meta, files: u64) -> constellation_fs_core::Ino {
        let dir = meta.mkdir(ROOT_INO, "d", 0o755, 1000, 1000).unwrap().ino;
        for i in 0..files {
            meta.create(dir, &format!("f{i}"), 0o644, 1000, 1000)
                .unwrap();
        }
        dir
    }

    /// The claim the whole step rests on: a tree grown one publish at a
    /// time is byte-identical to [`rebuild_root`]'s independent full
    /// walk of the same replica, at every step — an equality of root
    /// hashes rather than a structural comparison, because the tree is
    /// canonical. This single assertion covers dentry attr copies, the
    /// reverse index, xattr placement and delete handling at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_incremental_history_matches_a_full_rebuild() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 40);
        let mut publisher = fx.publisher(1);
        let first = publish(&mut publisher).await.unwrap();
        assert_eq!(first.seq, 1);

        let meta = Arc::clone(&fx.meta);
        let other = meta.mkdir(ROOT_INO, "other", 0o755, 0, 0).unwrap().ino;
        let steps: Vec<Box<dyn Fn()>> = vec![
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    meta.create(dir, "new", 0o600, 1, 1).unwrap();
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || meta.unlink(dir, "f3").unwrap()
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || meta.rename(dir, "f7", other, "moved").unwrap()
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    let ino = meta.child_ino(dir, "f1").unwrap().unwrap();
                    meta.link(ino, other, "hard").unwrap();
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    let ino = meta.child_ino(dir, "f2").unwrap().unwrap();
                    meta.setattr(ino, Some(0o600), None, None, Some(4096), None, Some(99))
                        .unwrap();
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    meta.symlink(other, "sym", "../d/f0", 0, 0).unwrap();
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    let ino = meta.child_ino(dir, "f4").unwrap().unwrap();
                    meta.set_xattr(ino, "user.a", b"one", SetXattrMode::Set)
                        .unwrap();
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    // Enough labels to push the set past XATTR_INLINE,
                    // so the record sheds them into 0x03 keys.
                    let ino = meta.child_ino(dir, "f5").unwrap().unwrap();
                    for i in 0..40 {
                        meta.set_xattr(ino, &format!("user.k{i}"), b"vvvv", SetXattrMode::Set)
                            .unwrap();
                    }
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    // And back below the budget: the 0x03 keys must be
                    // deleted, not merely stop being written.
                    let ino = meta.child_ino(dir, "f5").unwrap().unwrap();
                    for i in 0..40 {
                        meta.remove_xattr(ino, &format!("user.k{i}")).unwrap();
                    }
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    let ino = meta.child_ino(dir, "f6").unwrap().unwrap();
                    meta.set_manifest(ino, &vec![0xab; 4096], 1 << 20).unwrap();
                }
            }),
            Box::new({
                let meta = Arc::clone(&meta);
                move || {
                    // A directory that comes and goes: `rmdir` has to
                    // remove the 0x01, the 0x02 and the 0x04 together,
                    // and leave the parent's mtime copy consistent.
                    meta.mkdir(ROOT_INO, "scratch", 0o700, 0, 0).unwrap();
                    meta.rmdir(ROOT_INO, "scratch").unwrap();
                }
            }),
        ];
        let mut last = first;
        for (i, step) in steps.iter().enumerate() {
            step();
            last = publish(&mut publisher)
                .await
                .unwrap_or_else(|| panic!("step {i} published nothing"));

            let blobs = BlobStore::new(Arc::new(InMemory::new()), Hasher::Plain);
            let tmp = TempDir::new().unwrap();
            let cache = Arc::new(NodeCache::new(
                PackStore::new(Arc::new(InMemory::new())),
                Arc::new(DiskCache::open(tmp.path(), 1 << 20).unwrap()),
                Hasher::Plain,
                tokio::runtime::Handle::current(),
            ));
            let tree = Tree::with_config(cache, record::config()).unwrap();
            let rebuilt = rebuild_root(&fx.meta, &tree, &blobs).unwrap();
            assert_eq!(
                last.root(SHARD0),
                Some(rebuilt),
                "step {i}: incremental and rebuilt roots disagree"
            );
        }
        assert!(last.agg.keys > 0);
    }

    /// The efficiency claim, stated as a measurement rather than as
    /// prose: a publish after one `create` edits a handful of keys, not
    /// the namespace, and writes a pack that is a small fraction of the
    /// first one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_publish_costs_the_keys_that_changed() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 3_000);
        let mut publisher = fx.publisher(1);
        let full = publish(&mut publisher).await.unwrap();
        // 3,000 files + the directory + root: an 0x01, an 0x02 and an
        // 0x04 for each named inode.
        assert!(full.intent.ops > 9_000, "{}", full.intent.ops);

        fx.meta.create(dir, "just-one", 0o644, 1, 1).unwrap();
        let one = publish(&mut publisher).await.unwrap();
        // The new file's 0x01/0x02/0x04, and its parent's 0x01 plus the
        // 0x02/0x04 that copy the parent's now-bumped mtime.
        assert!(
            one.intent.ops <= 8,
            "a one-file publish edited {} keys",
            one.intent.ops
        );

        // And the same claim on the write side, which is the one that
        // costs money: a commit's packs hold exactly the nodes it
        // wrote, so counting their index entries counts rewritten
        // nodes. An incremental publish rewrites one root path;
        // a full build rewrites the tree.
        let nodes_written = |commit: &Commit| {
            let packs = PackStore::new(Arc::clone(&fx.store));
            let hashes = commit.pack_hashes().unwrap();
            async move {
                let mut total = 0usize;
                for hash in hashes {
                    total += packs.get_index(&hash).await.unwrap().entries.len();
                }
                total
            }
        };
        let full_nodes = nodes_written(&full).await;
        let one_node_count = nodes_written(&one).await;
        assert!(full_nodes > 60, "{full_nodes}");
        // The absolute bound is the interesting one: a root path is
        // O(depth), so this number must not grow with the namespace.
        assert!(
            one_node_count <= 8 && one_node_count * 8 <= full_nodes,
            "a one-file publish rewrote {one_node_count} nodes against a full build's {full_nodes}"
        );
    }

    /// §P6 excludes atime from the tree, so the read-time atime merge
    /// must move nothing at all: it never touches `ns`, so it never
    /// touches `dirty` either. Without this, a read-only `find` over
    /// the namespace would publish a commit per batch — the write shape
    /// §14.4 measured, arriving through the read path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn atime_never_reaches_the_tree() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 4);
        let mut publisher = fx.publisher(1);
        let first = publish(&mut publisher).await.unwrap();

        let ino = fx.meta.child_ino(dir, "f0").unwrap().unwrap();
        for i in 0..100 {
            fx.meta.apply_atime(&[(ino, 1_000 + i, 1_000 + i)]).unwrap();
        }
        assert!(!fx.meta.has_dirty());
        assert!(publisher.publish(7).await.unwrap().is_none());
        assert_eq!(
            publisher.published(),
            Some((first.root(SHARD0).unwrap(), 1))
        );
    }

    /// A restart never needs a rebuild (plan 29 M2): the dirty set is
    /// durable `fjall` state, so a fresh `TreePublisher` over the same
    /// replica just picks its parent back up and, when a key changes
    /// after that, edits exactly that key — not the whole namespace.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restart_never_needs_a_rebuild() {
        let mut fx = Fixture::new();
        seed(&fx.meta, 20);
        let mut publisher = fx.publisher(1);
        let first = publish(&mut publisher).await.unwrap();

        // The "restart": a brand-new `TreePublisher` over the same
        // meta/bucket, as a fresh process would construct.
        let mut warm = fx.publisher(1);
        warm.restore().unwrap();
        assert_eq!(warm.published(), Some((first.root(SHARD0).unwrap(), 1)));
        assert!(!fx.meta.has_dirty(), "the first publish cleared its keys");
        assert!(
            warm.publish(7).await.unwrap().is_none(),
            "nothing dirty means nothing to publish"
        );

        let ino = fx
            .meta
            .child_ino(fx.meta.resolve_path("d").unwrap().unwrap(), "f0")
            .unwrap()
            .unwrap();
        fx.meta
            .setattr(ino, Some(0o600), None, None, None, None, None)
            .unwrap();
        let second = warm.publish(7).await.unwrap().unwrap();
        assert_eq!(second.seq, 2);
        // Just this file's 0x01 plus its one dentry copy.
        assert!(
            second.intent.ops <= 2,
            "a restart's next publish edited {} keys, not just the changed one",
            second.intent.ops
        );
    }

    /// An object store that lets a test put a competing commit into
    /// the slot *between* a publisher's head discovery and its CAS.
    #[derive(Debug)]
    struct RaceStore {
        inner: Arc<dyn ObjectStore>,
        staged: std::sync::Mutex<Option<(object_store::path::Path, Vec<u8>)>>,
    }

    impl std::fmt::Display for RaceStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RaceStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RaceStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location.as_ref().starts_with("commits/") {
                let staged = self.staged.lock().expect("staged commit").take();
                if let Some((path, body)) = staged {
                    self.inner
                        .put(&path, object_store::PutPayload::from(body))
                        .await?;
                }
            }
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

    /// Lift `commit`'s object off the bucket and stage it so the next
    /// write to `commits/` through `racer` puts it right back — opening
    /// the race window a real concurrent publish would create.
    async fn stage_for_race(store: &Arc<dyn ObjectStore>, racer: &RaceStore, commit: &Commit) {
        let path = constellation_store_s3::layout::commit(commit.seq);
        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();
        *racer.staged.lock().expect("staged commit") = Some((path, body.to_vec()));
    }

    /// §P3's splice, and the test a blind retry fails.
    ///
    /// Two independently-replicated nodes build on the same parent and
    /// touch disjoint keys. One wins the slot; the loser must re-apply
    /// its own edits **onto the winner's root** and land at the next
    /// sequence with both batches present. Simply retrying its own
    /// payload would also produce a commit, and would also look green —
    /// while silently deleting every key the winner added, because its
    /// root was computed from a parent that no longer exists. That is
    /// the failure this asserts against: `a` must still be there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_publishers_with_disjoint_keys_both_survive() {
        let mut fx = Fixture::new();
        let da = fx.meta.mkdir(ROOT_INO, "da", 0o755, 0, 0).unwrap().ino;
        let db = fx.meta.mkdir(ROOT_INO, "db", 0o755, 0, 0).unwrap().ino;
        let mut winner = fx.publisher(1);
        publish(&mut winner).await.unwrap();

        let racer = Arc::new(RaceStore {
            inner: Arc::clone(&fx.store),
            staged: std::sync::Mutex::new(None),
        });
        let mut loser = fx.peer(2, Arc::clone(&racer) as Arc<dyn ObjectStore>, &winner);
        assert!(
            loser.publisher.published().is_some(),
            "the loser must share a parent"
        );

        fx.meta.create(da, "a", 0o644, 0, 0).unwrap();
        let won = publish(&mut winner).await.unwrap();
        stage_for_race(&fx.store, &racer, &won).await;

        loser.meta.create(db, "b", 0o644, 0, 0).unwrap();
        let landed = loser.publisher.publish(7).await.unwrap();

        let landed = landed.expect("a disjoint batch must splice, not defer");
        assert_eq!((landed.seq, landed.parent), (won.seq + 1, won.seq));
        assert_ne!(landed.root(SHARD0), won.root(SHARD0));

        let (tree, _) = fx.reader().await;
        let root = landed.root(SHARD0).unwrap();
        assert!(
            tree.get(&root, &keys::dentry(da, b"a")).unwrap().is_some(),
            "the winner's key was dropped: this is a blind retry, not a splice"
        );
        assert!(
            tree.get(&root, &keys::dentry(db, b"b")).unwrap().is_some(),
            "the loser's own key is missing"
        );
        // The winner's commit is immutable and still names its own root.
        let winner_root = won.root(SHARD0).unwrap();
        assert!(tree
            .get(&winner_root, &keys::dentry(db, b"b"))
            .unwrap()
            .is_none());
    }

    /// The other half of §P3: when the difference *does* intersect the
    /// batch, there is no splice to make. Both writers changed the same
    /// directory, so the loser's dentry copies and its parent record
    /// were computed against attrs the winner has replaced. It must
    /// decline rather than overwrite, keep its work, and re-execute on
    /// a later round once the log has delivered what it missed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_overlapping_batch_declines_instead_of_clobbering() {
        let mut fx = Fixture::new();
        let shared = fx.meta.mkdir(ROOT_INO, "shared", 0o755, 0, 0).unwrap().ino;
        let mut winner = fx.publisher(1);
        publish(&mut winner).await.unwrap();

        let racer = Arc::new(RaceStore {
            inner: Arc::clone(&fx.store),
            staged: std::sync::Mutex::new(None),
        });
        let mut loser = fx.peer(2, Arc::clone(&racer) as Arc<dyn ObjectStore>, &winner);

        fx.meta.create(shared, "a", 0o644, 0, 0).unwrap();
        let won = publish(&mut winner).await.unwrap();
        stage_for_race(&fx.store, &racer, &won).await;

        loser.meta.create(shared, "b", 0o644, 0, 0).unwrap();
        let landed = loser.publisher.publish(7).await.unwrap();

        assert!(landed.is_none(), "an overlapping batch must not commit");
        assert!(
            loser.meta.has_dirty(),
            "a declined batch must survive to be re-executed"
        );
        let chain = CommitChain::new(Arc::clone(&fx.store));
        assert_eq!(chain.list_from(0).await.unwrap().last(), Some(&won.seq));
    }

    /// The S5/M2 regression the `applied` vector exists to stop: a
    /// replica that is behind the chain head must not publish on top of
    /// it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_behind_the_head_defers_instead_of_regressing() {
        let mut fx = Fixture::new();
        seed(&fx.meta, 5);
        fx.meta.set_applied_seq(10).unwrap();
        let mut ahead = fx.publisher(1);
        let head = publish(&mut ahead).await.unwrap();
        assert_eq!(head.applied, 10);

        // A second node replicated to the same structure, but which has
        // tailed less of the shared log than the head it would build on
        // and has some local work of its own to publish.
        let mut behind = fx.peer(2, Arc::clone(&fx.store), &ahead);
        behind.meta.set_applied_seq(3).unwrap();
        behind
            .meta
            .create(ROOT_INO, "local-work", 0o644, 0, 0)
            .unwrap();
        assert!(
            behind.publisher.publish(7).await.unwrap().is_none(),
            "a replica behind the head must defer"
        );
        assert!(behind.meta.has_dirty(), "and keep its work");
        let chain = CommitChain::new(Arc::clone(&fx.store));
        assert_eq!(chain.discover_head(0).await.unwrap(), Some(head.seq));

        // Once the tailer has caught up, the same publisher lands (with
        // a change of its own).
        behind.meta.set_applied_seq(10).unwrap();
        behind
            .meta
            .create(ROOT_INO, "after-catch-up", 0o644, 0, 0)
            .unwrap();
        let landed = behind
            .publisher
            .publish(7)
            .await
            .unwrap()
            .expect("caught up");
        assert_eq!((landed.seq, landed.applied), (2, 10));
    }

    /// The same guard on the lost-CAS path: a winner whose vector this
    /// replica does not cover is declined rather than spliced onto,
    /// even when its key set is disjoint from the batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_winner_ahead_of_the_loser_is_not_spliced_onto() {
        let mut fx = Fixture::new();
        let da = fx.meta.mkdir(ROOT_INO, "da", 0o755, 0, 0).unwrap().ino;
        let db = fx.meta.mkdir(ROOT_INO, "db", 0o755, 0, 0).unwrap().ino;
        let mut winner = fx.publisher(1);
        publish(&mut winner).await.unwrap();

        let racer = Arc::new(RaceStore {
            inner: Arc::clone(&fx.store),
            staged: std::sync::Mutex::new(None),
        });
        let mut loser = fx.peer(2, Arc::clone(&racer) as Arc<dyn ObjectStore>, &winner);

        fx.meta.create(da, "a", 0o644, 0, 0).unwrap();
        fx.meta.set_applied_seq(10).unwrap();
        let won = publish(&mut winner).await.unwrap();
        assert_eq!(won.applied, 10);
        stage_for_race(&fx.store, &racer, &won).await;

        loser.meta.create(db, "b", 0o644, 0, 0).unwrap();
        loser.meta.set_applied_seq(5).unwrap();
        let landed = loser.publisher.publish(7).await.unwrap();
        assert!(landed.is_none(), "an ahead winner must not be spliced onto");
        assert!(loser.meta.has_dirty());
    }

    /// An object store whose writes stall, so a publish can be caught
    /// in flight. `resume` lets a test release a stalled call instead of
    /// only ever cancelling it: `notify_one` stores its permit even if
    /// called before anything is waiting, so there is no race between
    /// "the stalled call started waiting" and "the test decided to let
    /// it through".
    #[derive(Debug)]
    struct SlowStore {
        inner: Arc<dyn ObjectStore>,
        stall: AtomicBool,
        resume: tokio::sync::Notify,
    }

    impl std::fmt::Display for SlowStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SlowStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for SlowStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if self.stall.load(Ordering::Relaxed) {
                self.resume.notified().await;
            }
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

    /// The daemon's sync loop drops a running round whenever an explicit
    /// request arrives, so a publish must survive being cancelled at any
    /// await point: its dirty keys stay dirty, untouched by a publish
    /// that never reached `clear_dirty_upto`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancelled_publish_keeps_its_batch() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 3);
        let slow = Arc::new(SlowStore {
            inner: Arc::clone(&fx.store),
            stall: AtomicBool::new(false),
            resume: tokio::sync::Notify::new(),
        });
        let mut publisher = fx.publisher_on(Arc::clone(&slow) as Arc<dyn ObjectStore>, 1);
        publish(&mut publisher).await.unwrap();

        let late = fx.meta.create(dir, "late", 0o644, 0, 0).unwrap().ino;
        slow.stall.store(true, Ordering::Relaxed);
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_millis(200), publisher.publish(7)).await;
        assert!(cancelled.is_err(), "the stalled publish must be cut off");
        assert!(
            fx.meta.has_dirty(),
            "the cancelled batch's dirty keys were dropped"
        );

        slow.stall.store(false, Ordering::Relaxed);
        let landed = publisher
            .publish(7)
            .await
            .unwrap()
            .expect("the batch lands");
        let (tree, _) = fx.reader().await;
        assert!(tree
            .get(&landed.root(SHARD0).unwrap(), &keys::inode(late))
            .unwrap()
            .is_some());
        assert!(!fx.meta.has_dirty());
    }

    /// The `clear_dirty_upto` counter check, exercised through a real
    /// publish: a key re-dirtied while a publish is stalled in flight
    /// must survive that publish's clear, because the publish's plan
    /// (and the value it published) never saw the re-dirtying write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_key_re_dirtied_mid_publish_survives_the_clear() {
        let mut fx = Fixture::new();
        let f = fx.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let slow = Arc::new(SlowStore {
            inner: Arc::clone(&fx.store),
            stall: AtomicBool::new(false),
            resume: tokio::sync::Notify::new(),
        });
        let mut publisher = fx.publisher_on(Arc::clone(&slow) as Arc<dyn ObjectStore>, 1);
        publish(&mut publisher).await.unwrap();

        fx.meta
            .setattr(f.ino, Some(0o600), None, None, None, None, None)
            .unwrap();
        slow.stall.store(true, Ordering::Relaxed);

        let publisher = Arc::new(tokio::sync::Mutex::new(publisher));
        let task = {
            let publisher = Arc::clone(&publisher);
            tokio::spawn(async move { publisher.lock().await.publish(7).await })
        };
        // Give the task time to pass the (fast, local) snapshot/plan
        // step and reach the stalled S3 call.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let key = keys::inode(f.ino);
        fx.meta
            .setattr(f.ino, Some(0o644), None, None, None, None, None)
            .unwrap();

        // Release the stalled call rather than only cancelling it, so
        // this publish actually lands: `stall` first, so no *later*
        // put (the commit CAS after the pack upload this unblocks)
        // stalls again, then the notify.
        slow.stall.store(false, Ordering::Relaxed);
        slow.resume.notify_one();
        let landed = task.await.unwrap().unwrap();
        assert!(landed.is_some(), "the stalled publish must eventually land");

        assert!(
            dirty_keys(&fx.meta).contains(&key),
            "a key re-dirtied mid-publish must survive clear_dirty_upto"
        );
    }

    /// A value over `VALUE_SPILL` leaves the node and becomes a blob
    /// reference, and the blob is on the bucket before the commit that
    /// names it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_oversized_value_spills_to_a_blob_that_is_durable_first() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 2);
        let ino = fx.meta.child_ino(dir, "f0").unwrap().unwrap();
        let value = vec![0x5a; 40_000];
        fx.meta
            .set_xattr(ino, "user.big", &value, SetXattrMode::Set)
            .unwrap();
        // A PATH_MAX symlink target spills too, through a different
        // field of the same record.
        let target = "x".repeat(3_000);
        fx.meta.symlink(dir, "long", &target, 0, 0).unwrap();

        let mut publisher = fx.publisher(1);
        let commit = publish(&mut publisher).await.unwrap();

        let blobs = BlobStore::new(Arc::clone(&fx.store), Hasher::Plain);
        let (tree, _) = fx.reader().await;
        let root = commit.root(SHARD0).unwrap();

        let spilled = tree
            .get(&root, &keys::xattr(ino, b"user.big"))
            .unwrap()
            .expect("an oversized xattr must have its own 0x03 key");
        match Payload::decode(&spilled).unwrap() {
            Payload::Spilled(hash) => assert_eq!(blobs.get(&hash).await.unwrap(), value),
            other => panic!("expected a blob reference, got {other:?}"),
        }

        let link = fx.meta.child_ino(dir, "long").unwrap().unwrap();
        let encoded = tree.get(&root, &keys::inode(link)).unwrap().unwrap();
        match record::InodeRecord::decode(&encoded)
            .unwrap()
            .symlink_target
        {
            Some(Payload::Spilled(hash)) => {
                assert_eq!(blobs.get(&hash).await.unwrap(), target.as_bytes())
            }
            other => panic!("expected a spilled target, got {other:?}"),
        }
    }

    /// Plan 28 §13's measurement: `getattr` latency on a file-backed
    /// replica while a full-rebuild publish runs, against the same loop
    /// with nothing publishing. Ignored: it is a measurement, run by
    /// hand with `cargo test --release -p constellation getattr_latency
    /// -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn getattr_latency_during_a_publish() {
        let dir = TempDir::new().unwrap();
        let mut fx = Fixture::new();
        fx.meta = Arc::new(Meta::open(dir.path().join("m.db")).unwrap());
        let files: u64 = std::env::var("GETATTR_BENCH_FILES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);
        let d = fx.meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
        let mut inos = Vec::new();
        for i in 0..files {
            let ino = fx
                .meta
                .create(d, &format!("f{i}"), 0o644, 0, 0)
                .unwrap()
                .ino;
            fx.meta.set_manifest(ino, &[7u8; 64], 4096).unwrap();
            inos.push(ino);
        }

        let sample = |meta: Arc<Meta>, inos: Vec<u64>, stop: Arc<std::sync::atomic::AtomicBool>| {
            std::thread::spawn(move || {
                let mut samples = Vec::new();
                let mut i = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let t = std::time::Instant::now();
                    meta.getattr(inos[(i * 7919) % inos.len()]).unwrap();
                    samples.push(t.elapsed());
                    i += 1;
                }
                samples.sort();
                samples
            })
        };
        let pct = |s: &[std::time::Duration], p: f64| s[((s.len() as f64 - 1.0) * p) as usize];

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let idle = sample(Arc::clone(&fx.meta), inos.clone(), Arc::clone(&stop));
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let idle = idle.join().unwrap();

        let mut publisher = fx.publisher(1);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let busy = sample(Arc::clone(&fx.meta), inos.clone(), Arc::clone(&stop));
        let started = std::time::Instant::now();
        let commit = publisher.publish(1).await.unwrap().unwrap();
        let publish_ms = started.elapsed().as_millis();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let busy = busy.join().unwrap();

        eprintln!(
            "getattr over {files} files: idle p50 {:?} p99 {:?} max {:?} ({} samples); \
             during a {publish_ms} ms full publish ({} keys) p50 {:?} p99 {:?} max {:?} ({} samples)",
            pct(&idle, 0.5),
            pct(&idle, 0.99),
            idle.last().unwrap(),
            idle.len(),
            commit.intent.ops,
            pct(&busy, 0.5),
            pct(&busy, 0.99),
            busy.last().unwrap(),
            busy.len(),
        );

        // And the other direction: a fresh replica loaded from that
        // commit (bootstrap, minus log tailing), from a cold cache.
        let scratch = TempDir::new().unwrap();
        let reader =
            crate::mtree_read::ChainReader::for_store(Arc::clone(&fx.store), None, scratch.path())
                .unwrap();
        let fresh = Arc::new(Meta::open(dir.path().join("fresh.db")).unwrap());
        let started = std::time::Instant::now();
        let loaded = crate::mtree_read::bootstrap_from_commit(&reader, Arc::clone(&fresh))
            .await
            .unwrap()
            .unwrap();
        eprintln!(
            "bootstrap from commit {}: {} inodes, {} dentries in {} ms (in-memory store, cold node cache)",
            loaded.commit.seq,
            loaded.inodes,
            loaded.dentries,
            started.elapsed().as_millis()
        );
        assert_eq!(
            fresh.dump_replicated().unwrap().len(),
            fx.meta.dump_replicated().unwrap().len()
        );
    }
}
