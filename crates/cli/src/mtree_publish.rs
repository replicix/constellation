//! Plan 28 §11 (S5): turn the live SQLite replica into an `mtree` over
//! the §P6 encoding and publish it as a commit.
//!
//! This is the step that makes option (B) real. SQLite remains the
//! query engine and the op log remains the transport (§11b) — nothing
//! here reads the tree back, and no FUSE path consults it. What changes
//! is what *durability* means: instead of `VACUUM INTO`-ing the whole
//! database every N segments and PUTting the result, a publish writes
//! only the tree nodes the batch actually touched and CAS-creates one
//! commit naming the new root.
//!
//! ## The incremental path is the whole point
//!
//! A checkpoint costs O(database). A publish must cost O(keys changed),
//! or the plan's central claim is false. The changed key set comes from
//! the place that already knows it — the op log — and nowhere else:
//!
//! 1. every record that reaches this replica passes through
//!    `shipper::Shipper::apply_decoded_segment` (foreign segments) or
//!    `ship_part` (our own), and both hand it to [`Touched::note`];
//! 2. [`Touched`] keeps *entity references* (inode numbers, and
//!    `(parent, name)` dentry coordinates), not values. A record says
//!    which entities moved; what they moved *to* is whatever the
//!    replica now holds, which is both cheaper to read and immune to a
//!    record this code does not know how to interpret;
//! 3. at publish time [`TreePublisher::plan`] turns that reference set
//!    into `mtree` edits with a bounded number of indexed SQLite reads
//!    and a bounded number of tree point/range reads per entity, and
//!    hands them to `Tree::apply`, whose cost is O(edits + the nodes
//!    they touch).
//!
//! A full rebuild happens on the first publish and after a restart that
//! cannot prove the tree is level with the replica (see
//! [`TreePublisher::restore`]); it is the same per-inode code path fed
//! every inode a page at a time, so the two cannot drift apart. That
//! sharing is deliberate and is what
//! `an_incremental_history_matches_a_full_rebuild` checks: because the
//! tree is canonical, "incremental and bulk agree" is an equality of
//! root hashes rather than a structural comparison.
//!
//! ## Never building on a head this replica is behind
//!
//! A publish adopts the chain head and then writes *this replica's*
//! values for the keys it touched. That is only correct if the replica
//! has applied at least as much of every partition's log as the head
//! reflects; a replica that is behind would overwrite newer values with
//! the older ones it still holds, and no diff can show it, because the
//! stale value is ours. So every commit records the applied vector it
//! was planned at ([`Commit::applied`], read in the same SQLite snapshot
//! as the plan), and a publish — or a splice onto a lost race's winner —
//! proceeds only when this replica's vector covers the other one.
//! Otherwise it defers and the tailer closes the gap.
//!
//! With that, vectors only grow along the chain, and each commit's tree
//! is its author's replica at its vector: a key the author did not touch
//! since its last publish already holds, in the head, a value at least
//! as new as the author's.
//!
//! ## What is in the tree
//!
//! `0x01`–`0x04`: inodes, dentries with S1's attr copy, spilled xattrs,
//! and the reverse dentry index. An inode with `nlink == 0` — an
//! unlinked file some process still holds open — is **not** in the
//! tree: it is node-local transient state that no other replica can
//! reach, and including it would make the §P7 aggregate disagree with
//! the reachable filesystem it is supposed to describe. `0x30` holds the
//! snapshot rows, the replicated quota and the partition map — what a
//! bootstrap needs beyond the namespace (codec in `mtree_read`).
//!
//! atime appears nowhere. `LogRecord::Atime` is noted as touching
//! nothing at all, which `atime_never_reaches_the_tree` pins — not as a
//! micro-optimization but because a tree that moved on read would turn
//! every `find` into a publish.
//!
//! ## Not stalling FUSE
//!
//! Every read this module makes goes through `SqliteMeta`'s per-thread
//! read-only WAL connection, so a publish never takes the write mutex
//! that FUSE writers contend on, and the tree build itself runs on a
//! blocking thread rather than a runtime worker. Replacing a
//! `VACUUM INTO` stall with a tree-build stall would be a regression
//! dressed as a win, so this is measured rather than asserted; the
//! numbers are in `PROGRESS.md`.

use anyhow::{Context, Result};
use constellation_fs_core::{FileAttr, Ino};
use constellation_meta::{LogRecord, SqliteMeta, TreeInode};
use constellation_mtree::{
    keys, record, Attrs, DentryRecord, Edit, Kind, NodeHash, NodeStore, Tree, VALUE_SPILL,
};
use constellation_store_s3::{
    vector_covers, BlobStore, Commit, CommitChain, CommitPayload, Intent, NodeCache, StoreError,
    SHARD0,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Inodes per page of a full rebuild. Bounds the rebuild's memory at
/// one page of records rather than the whole namespace; the tree is
/// canonical, so the page size cannot affect the root hash it produces.
const REBUILD_PAGE: usize = 4096;

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
pub fn remembered_seq(meta: &SqliteMeta) -> Result<Option<u64>> {
    Ok(meta.kv_get(KV_SEQ)?.and_then(|seq| seq.parse().ok()))
}

/// Record a commit this replica's state was loaded from (S6's
/// bootstrap), so the node's publisher can pick it up as its parent
/// instead of rebuilding. `restore` still trusts it only if the replica
/// has applied exactly `commit.applied` — a bootstrap that tailed past
/// the commit leaves the vectors unequal and the first publish rebuilds.
pub fn remember_loaded_commit(meta: &SqliteMeta, commit: &Commit) -> Result<()> {
    let Some(root) = commit.root(SHARD0) else {
        return Ok(());
    };
    meta.kv_set(KV_ROOT, &root.to_hex())?;
    meta.kv_set(KV_SEQ, &commit.seq.to_string())?;
    meta.kv_set(KV_VECTOR, &encode_vector(&commit.applied))?;
    Ok(())
}

/// The entities a batch of log records touched.
///
/// References, not values, and a set rather than a list: publishing is
/// idempotent per key, so a thousand `setattr`s on one inode cost one
/// entry here and one edit later. This is the whole of the incremental
/// path's input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Touched {
    inodes: BTreeSet<Ino>,
    dentries: BTreeSet<(Ino, String)>,
    /// A snapshot, quota or partition record moved, so the `0x30`
    /// subsystem range is re-planned. It holds a handful of records per
    /// filesystem, so re-deriving all of it is cheaper than tracking
    /// which one.
    subsystems: bool,
    /// Set when the changed set is unknown and only a rebuild can
    /// restore agreement with the replica.
    full: bool,
}

impl Touched {
    pub fn is_empty(&self) -> bool {
        !self.full && !self.subsystems && self.inodes.is_empty() && self.dentries.is_empty()
    }

    pub fn needs_rebuild(&self) -> bool {
        self.full
    }

    fn inode(&mut self, ino: Ino) {
        self.inodes.insert(ino);
    }

    fn dentry(&mut self, parent: Ino, name: &str) {
        self.dentries.insert((parent, name.to_string()));
    }

    fn link(&mut self, parent: Ino, name: &str, ino: Ino) {
        self.inode(ino);
        self.inode(parent);
        self.dentry(parent, name);
    }

    /// Fold one record's entity references in.
    ///
    /// Deliberately over-approximate where a record does not name
    /// everything it moves: `Unlink` does not carry the target's ino,
    /// and `Rename` does not carry the moved or clobbered one. Those
    /// are recovered at plan time from the dentry's *old* value in the
    /// tree and its new value in the replica, which is the only place
    /// both are known. Over-approximating costs a few wasted point
    /// reads; under-approximating would silently strand a key.
    pub fn note(&mut self, record: &LogRecord) {
        match record {
            LogRecord::Mkdir {
                parent, name, ino, ..
            }
            | LogRecord::Create {
                parent, name, ino, ..
            }
            | LogRecord::Symlink {
                parent, name, ino, ..
            }
            | LogRecord::Mknod {
                parent, name, ino, ..
            }
            | LogRecord::Link {
                parent, name, ino, ..
            } => self.link(*parent, name, *ino),
            LogRecord::Unlink { parent, name, .. } | LogRecord::Rmdir { parent, name, .. } => {
                self.inode(*parent);
                self.dentry(*parent, name);
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                self.inode(*parent);
                self.inode(*new_parent);
                self.dentry(*parent, name);
                self.dentry(*new_parent, new_name);
            }
            LogRecord::RenameXpartSrc {
                from_parent,
                name,
                ino,
                ..
            } => self.link(*from_parent, name, *ino),
            LogRecord::RenameXpartDst {
                to_parent,
                new_name,
                ino,
                ..
            } => self.link(*to_parent, new_name, *ino),
            LogRecord::Setattr { ino, .. }
            | LogRecord::WriteManifest { ino, .. }
            | LogRecord::SetXattr { ino, .. }
            | LogRecord::RemoveXattr { ino, .. } => self.inode(*ino),
            LogRecord::Clone { nodes, .. } => {
                for node in nodes {
                    self.link(node.parent, &node.name, node.ino);
                }
            }
            // A cross-partition rename that never completed leaves the
            // source where it was; the replica's own abort path is what
            // restores it, and the next record it journals is what this
            // sees. Nothing to do here.
            LogRecord::RenameXpartAbort { .. } => {}
            // The `0x30` records a bootstrap needs: snapshot rows, the
            // replicated quota, and the partition map (which moves no
            // inode, name or attribute, but tells a bootstrapped
            // replica which logs to tail).
            LogRecord::SnapCreate { .. }
            | LogRecord::SnapDelete { .. }
            | LogRecord::SetQuota { .. }
            | LogRecord::PartSplit { .. }
            | LogRecord::PartMerge { .. } => self.subsystems = true,
            // The one record that must never move the tree. §P6
            // excludes atime from the encoding entirely; a tree that
            // changed on read would make `find` a publish storm.
            LogRecord::Atime { .. } => {}
        }
    }

    pub fn note_all<'a>(&mut self, records: impl IntoIterator<Item = &'a LogRecord>) {
        for record in records {
            self.note(record);
        }
    }

    /// Entities, for logging and for the batch size a commit records.
    pub fn len(&self) -> usize {
        self.inodes.len() + self.dentries.len() + usize::from(self.subsystems)
    }
}

/// What a batch depended on, at key granularity (§P3).
///
/// The keys whose presence, absence or content the plan read, plus the
/// prefixes it scanned. A scanned prefix is a *negative* observation —
/// "these are all the names pointing at this inode" — so a winner that
/// added a key inside one invalidates the plan just as surely as one
/// that changed a key it read by name. Node granularity would be wrong
/// in both directions: it would report conflicts for unrelated keys
/// that happen to share a leaf, and it would have no way to express the
/// absence the scan relied on.
#[derive(Clone, Debug, Default)]
pub struct ReadSet {
    keys: BTreeSet<Vec<u8>>,
    prefixes: BTreeSet<Vec<u8>>,
}

impl ReadSet {
    fn key(&mut self, key: Vec<u8>) {
        self.keys.insert(key);
    }

    fn prefix(&mut self, prefix: Vec<u8>) {
        self.prefixes.insert(prefix);
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key) || self.prefixes.iter().any(|p| key.starts_with(p))
    }
}

/// A batch turned into tree edits.
#[derive(Debug, Default)]
pub struct Plan {
    /// Keyed so the batch is deduplicated and sorted by construction;
    /// `Tree::apply` demands strictly ascending keys.
    edits: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Bodies that exceeded `VALUE_SPILL` and must be durable before
    /// the commit that names them.
    blobs: Vec<Vec<u8>>,
    reads: ReadSet,
    /// Entities the plan covered, for the commit's intent.
    entities: usize,
}

impl Plan {
    fn upsert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.edits.insert(key, Some(value));
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.edits.insert(key, None);
    }

    fn edits(&self) -> Vec<Edit> {
        self.edits
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.edits.len()
    }

    /// True when a winner's change set touches anything this batch read
    /// or wrote. The write-set counts as read: two publishers that both
    /// set the same key have no disjoint splice, whether or not either
    /// looked at the old value.
    fn conflicts_with(&self, changed: &[(Vec<u8>, constellation_mtree::ChangeKind)]) -> bool {
        changed
            .iter()
            .any(|(key, _)| self.reads.contains(key) || self.edits.contains_key(key))
    }
}

/// Builds and publishes the §P6 tree for one replica.
///
/// Owned by `shipper::Shipper` and driven from the same place the
/// checkpoint cadence is, so a publish happens exactly where a
/// checkpoint used to.
pub struct TreePublisher {
    meta: Arc<SqliteMeta>,
    cache: Arc<NodeCache>,
    blobs: BlobStore,
    chain: CommitChain,
    config: constellation_mtree::Config,
    node_id: u64,
    /// The root this replica last published, and the commit that names
    /// it. `None` until the first publish.
    state: Option<Published>,
    pending: Touched,
    /// Whether the node cache has loaded the pack catalog. False until
    /// the first publish of every process (see `new`).
    hydrated: bool,
    handle: tokio::runtime::Handle,
}

#[derive(Clone, Debug)]
struct Published {
    root: NodeHash,
    seq: u64,
    /// The commit's [`Commit::applied`] vector: what this tree reflects.
    applied: Vector,
}

/// Partition id → highest applied log segment.
type Vector = BTreeMap<String, u64>;

/// How one publish attempt ended.
enum Outcome {
    Committed(Box<Commit>),
    /// The tree already reflects the batch; nothing to commit.
    Unchanged,
    /// Not now: behind the head, a parked rename, or a declined splice.
    /// The pending set is kept for the next round.
    Deferred,
}

/// What the blocking half of a publish decided.
enum Planned {
    /// A plan, the root it produces, and the vector it was read at.
    Ready(Plan, NodeHash, Vector),
    /// This replica has applied less of some partition's log than the
    /// head it would build on, so every value it holds for a key it
    /// touched might be older than the head's. Publishing would regress
    /// the tree; the tailer closes the gap and a later round publishes.
    Behind { mine: Vector, head: Vector },
    /// A cross-partition rename half is parked waiting for its partner.
    /// That state lives only in `xpart_pending`, which no commit
    /// carries, so a commit now could fall between the two halves and
    /// strand the second one on every replica bootstrapped from it.
    Parked(u64),
}

impl TreePublisher {
    pub fn new(
        meta: Arc<SqliteMeta>,
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
            pending: Touched {
                full: true,
                ..Touched::default()
            },
            // Always hydrate before the first publish, restored root or
            // not: a rebuild re-`put`s every node of the namespace, and
            // only a cache that knows which ones are already on the
            // bucket skips the upload for the unchanged ones.
            hydrated: false,
            handle,
        }
    }

    pub fn note(&mut self, records: &[LogRecord]) {
        self.pending.note_all(records);
    }

    #[cfg(test)]
    pub fn pending(&self) -> &Touched {
        &self.pending
    }

    #[cfg(test)]
    pub fn published(&self) -> Option<(NodeHash, u64)> {
        self.state.as_ref().map(|s| (s.root, s.seq))
    }

    /// Pick the published root back up after a restart.
    ///
    /// The tree is only reusable if it is level with the replica, and
    /// the cheapest honest proof of that is the applied-sequence vector
    /// recorded alongside it: the replica has applied exactly the log
    /// the tree was built from. Anything else — a crash between an
    /// `apply` and a publish, a tail that ran without a publisher — and
    /// the in-memory touched set is gone with no way to reconstruct it,
    /// so the next publish rebuilds.
    ///
    /// Re-deriving the delta by re-reading the log segments between the
    /// two vectors would avoid the rebuild at the cost of S3 GETs for
    /// data the replica already has; that is left for a later step and
    /// noted in `PROGRESS.md`.
    pub fn restore(&mut self) -> Result<()> {
        let (Some(root_hex), Some(seq)) = (self.meta.kv_get(KV_ROOT)?, self.meta.kv_get(KV_SEQ)?)
        else {
            return Ok(());
        };
        let (Some(root), Ok(seq)) = (NodeHash::from_hex(&root_hex), seq.parse::<u64>()) else {
            return Ok(());
        };
        let recorded = self.meta.kv_get(KV_VECTOR)?.unwrap_or_default();
        let applied = self.meta.applied_vector()?;
        if recorded != encode_vector(&applied) {
            tracing::info!(
                "metadata tree is behind the replica; the next publish rebuilds it in full"
            );
            return Ok(());
        }
        self.state = Some(Published { root, seq, applied });
        self.pending = Touched::default();
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

    fn tree(&self) -> Result<Tree<Arc<NodeCache>>, StoreError> {
        Ok(Tree::with_config(self.cache.clone(), self.config)?)
    }

    /// Publish the pending set, if there is one.
    ///
    /// Returns the commit that landed, or `None` when there was nothing
    /// to do or when the publish deferred to let the tailer catch up.
    /// The pending set is cleared only on success, so a failure costs a
    /// round rather than a key.
    ///
    /// **Cancellation-safe**, and it has to be: the daemon's sync loop
    /// drops a running round (and with it this future) whenever an
    /// explicit request arrives. The pending set is therefore only
    /// *read* while the publish is in flight and cleared once it has
    /// landed; `&mut self` guarantees nothing is noted in between. An
    /// earlier version took the set up front, and a cancelled round
    /// silently dropped its keys from the tree for good (found by
    /// `snapshot-churn`: a clone never reached any commit).
    pub async fn publish(&mut self, epoch: u64) -> Result<Option<Commit>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let started = std::time::Instant::now();
        let batch = self.pending.clone();
        match self.publish_batch(&batch, epoch).await {
            Ok(Outcome::Unchanged) => {
                // The tree already says what the replica says (e.g. a
                // cancelled round's commit landed and was adopted).
                self.pending = Touched::default();
                Ok(None)
            }
            Ok(Outcome::Deferred) => Ok(None),
            Err(e) => Err(e),
            Ok(Outcome::Committed(commit)) => {
                let commit = *commit;
                self.pending = Touched::default();
                tracing::info!(
                    seq = commit.seq,
                    root = %commit.roots.get(SHARD0).map(String::as_str).unwrap_or(""),
                    entities = batch.len(),
                    keys = commit.intent.ops,
                    packs = commit.packs.len(),
                    ms = started.elapsed().as_millis(),
                    "published metadata commit"
                );
                Ok(Some(commit))
            }
        }
    }

    /// Publish whatever is pending and return the commit that now
    /// reflects this replica: the new one, or — when nothing was pending
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
            if self.pending.is_empty() {
                break;
            }
        }
        if !self.pending.is_empty() {
            anyhow::bail!(
                "the metadata publish was deferred (this replica is behind the chain head, \
                 or a cross-partition rename is half applied); retry shortly"
            );
        }
        let state = self
            .state
            .as_ref()
            .context("nothing has been published on this mount yet")?;
        Ok((state.seq, state.root))
    }

    async fn publish_batch(&mut self, batch: &Touched, epoch: u64) -> Result<Outcome> {
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
        let head_applied = self.state.as_ref().map(|s| s.applied.clone());

        let meta = Arc::clone(&self.meta);
        let cache = Arc::clone(&self.cache);
        let config = self.config;
        let blobs = self.blobs.clone();
        let batch = batch.clone();
        // The tree build reads SQLite and decompresses nodes. Tens of
        // milliseconds on a large batch, so it does not belong on a
        // runtime worker any more than `VACUUM INTO` did.
        //
        // The vector and every read the plan makes share one SQLite
        // snapshot (`read_consistent`), so the commit's `applied` claim
        // is exactly what the tree was built from.
        let planned = tokio::task::spawn_blocking(move || -> Result<Planned> {
            let tree = Tree::with_config(cache, config).map_err(StoreError::from)?;
            let builder = Builder {
                meta: &meta,
                tree: &tree,
                blobs: &blobs,
            };
            meta.read_consistent(|| -> Result<Planned> {
                let vector = meta.applied_vector_reader()?;
                if let Some(head) = head_applied.filter(|head| !vector_covers(&vector, head)) {
                    return Ok(Planned::Behind { mine: vector, head });
                }
                let parked = meta.xpart_pending_count_reader()?;
                if parked > 0 {
                    return Ok(Planned::Parked(parked));
                }
                let (plan, root) = match base {
                    Some(base) if !batch.needs_rebuild() => {
                        let plan = builder.plan(Some(&base), &batch)?;
                        let edits = plan.edits();
                        let root = tree.apply(&base, &edits).map_err(StoreError::from)?;
                        (plan, root)
                    }
                    _ => builder.rebuild()?,
                };
                Ok(Planned::Ready(plan, root, vector))
            })
        })
        .await
        .context("metadata tree build task")??;
        let (plan, root, vector) = match planned {
            Planned::Ready(plan, root, vector) => (plan, root, vector),
            Planned::Behind { mine, head } => {
                tracing::debug!(
                    mine = encode_vector(&mine),
                    head = encode_vector(&head),
                    "metadata publish deferred: this replica is behind the chain head"
                );
                return Ok(Outcome::Deferred);
            }
            Planned::Parked(halves) => {
                tracing::debug!(
                    halves,
                    "metadata publish deferred: a cross-partition rename is half applied"
                );
                return Ok(Outcome::Deferred);
            }
        };

        if Some(root) == base {
            // The published root already reflects the replica at
            // `vector` — an empty plan, or a restart's rebuild that
            // reproduced the head exactly. Nothing to commit; say so
            // locally, so a restart need not rebuild again.
            if let Some(state) = self.state.as_mut() {
                state.applied = vector;
            }
            self.remember()?;
            return Ok(Outcome::Unchanged);
        }

        // Ordering invariant (§P2/S4), extended to blobs: every byte
        // the tree names is durable before the commit exists. Blobs
        // first because a pack may name a node whose value names a
        // blob, and packs are sealed below.
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

        let tree = self.tree()?;
        let agg = tree.aggregate(&root).map_err(StoreError::from)?;
        let payload = CommitPayload::single_root(root, packs)
            .with_author(self.node_id, epoch)
            .with_intent(Intent::batch(plan.len() as u64))
            .with_agg(agg.into())
            .with_applied(vector.clone());

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
        let mine = vector.clone();
        let commit = self
            .chain
            .publish(
                &self.cache,
                parent,
                payload,
                move |payload: CommitPayload, winner: &Commit| {
                    let from = spliced.get();
                    match Self::splice(&tree, &cache, &handle, &plan, &mine, from, winner, payload)
                    {
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
                Ok(Outcome::Committed(Box::new(commit)))
            }
            // A refused splice is not a failure of this publish so much
            // as a statement that the batch has to be re-executed
            // against state this replica has not applied yet. The
            // tailer is what supplies it, so the work goes back on the
            // pending set and the next round re-plans from the winner's
            // root — which is re-execution in §P3's sense, deferred by
            // one round so that it re-executes against a replica that
            // has actually seen the winner's records.
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
        mine: &Vector,
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
        if !vector_covers(mine, &winner.applied) {
            return Err(StoreError::Conflict(format!(
                "commit {} reflects log this replica has not applied ({} against {})",
                winner.seq,
                encode_vector(&winner.applied),
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
                "commit {} changed {} keys, overlapping this batch's read set",
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
        let Some(head) = self.chain.discover_head(known).await? else {
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
    /// makes the two differ, and `restore` must then rebuild rather
    /// than trust a tree that never saw that segment.
    fn remember(&self) -> Result<()> {
        let Some(state) = self.state.as_ref() else {
            return Ok(());
        };
        self.meta.kv_set(KV_ROOT, &state.root.to_hex())?;
        self.meta.kv_set(KV_SEQ, &state.seq.to_string())?;
        self.meta
            .kv_set(KV_VECTOR, &encode_vector(&state.applied))?;
        Ok(())
    }
}

impl Touched {
    fn from_inodes(inodes: impl IntoIterator<Item = Ino>) -> Touched {
        Touched {
            inodes: inodes.into_iter().collect(),
            ..Touched::default()
        }
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

fn encode_vector(vector: &std::collections::BTreeMap<String, u64>) -> String {
    vector
        .iter()
        .map(|(part, seq)| format!("{part}:{seq}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Build the tree the replica's current state calls for, from nothing,
/// into `tree`'s store — `fsck`'s half of "recompute the root hash and
/// compare". The same code path as a publisher's rebuild, so an
/// agreement bug between them cannot hide here. Nothing is uploaded:
/// spilled values are hashed, never stored.
pub fn rebuild_root<S: NodeStore>(
    meta: &SqliteMeta,
    tree: &Tree<S>,
    blobs: &BlobStore,
) -> Result<NodeHash> {
    let builder = Builder { meta, tree, blobs };
    meta.read_consistent(|| Ok(builder.rebuild()?.1))
}

// ----------------------------------------------------------- the builder

/// One key/value pair as the tree hands it back.
type Entry = (Vec<u8>, Vec<u8>);

/// Turns replica state into §P6 edits.
///
/// Borrowed rather than owned so the same code serves the incremental
/// path and the rebuild; the only difference between them is whether
/// there is an old tree to diff against.
struct Builder<'a, S> {
    meta: &'a SqliteMeta,
    tree: &'a Tree<S>,
    blobs: &'a BlobStore,
}

impl<S: NodeStore> Builder<'_, S> {
    /// Every inode, a page at a time, onto an empty tree.
    ///
    /// The first publish and `fsck`'s path. It is the incremental
    /// planner fed the whole namespace rather than a second
    /// implementation, which is what makes "incremental equals bulk" a
    /// property of one code path instead of a coincidence between two.
    /// The cost is that a rebuild pays `apply`'s rewrite per page
    /// rather than a single streaming build; rebuilds are rare by
    /// construction, and an agreement bug here would be silent and
    /// permanent.
    fn rebuild(&self) -> Result<(Plan, NodeHash)> {
        let mut root = self.tree.empty().map_err(StoreError::from)?;
        let mut total = Plan::default();
        let mut after: Ino = 0;
        loop {
            let page = self.meta.scan_inos(after, REBUILD_PAGE)?;
            let Some(last) = page.last().copied() else {
                break;
            };
            after = last;
            let plan = self.plan(None, &Touched::from_inodes(page))?;
            self.absorb_page(&mut root, &mut total, plan)?;
        }
        let subsystems = Touched {
            subsystems: true,
            ..Touched::default()
        };
        let plan = self.plan(None, &subsystems)?;
        self.absorb_page(&mut root, &mut total, plan)?;
        // The rebuild depends on nothing in any previous tree, so its
        // read set is empty: a concurrent winner can change any key at
        // all without invalidating "this is the replica".
        total.reads = ReadSet::default();
        Ok((total, root))
    }

    /// Fold one page of a rebuild into the running root. The edit count
    /// is kept for the commit's intent.
    fn absorb_page(&self, root: &mut NodeHash, total: &mut Plan, plan: Plan) -> Result<()> {
        *root = self
            .tree
            .apply(root, &plan.edits())
            .map_err(StoreError::from)?;
        total.blobs.extend(plan.blobs);
        total.entities += plan.entities;
        total.edits.extend(plan.edits);
        Ok(())
    }

    /// The incremental plan: what has to change in the tree so that it
    /// agrees with the replica about `batch`'s entities.
    fn plan(&self, base: Option<&NodeHash>, batch: &Touched) -> Result<Plan> {
        let mut plan = Plan::default();
        let mut inodes: BTreeSet<Ino> = batch.inodes.clone();
        // Dentries whose `0x02` value has to be rewritten, either
        // because the name moved or because the attrs it copies did.
        let mut names: BTreeSet<(Ino, String)> = batch.dentries.clone();

        // A record that removes or moves a name does not carry the ino
        // it pointed at. The tree's old value is where that is written
        // down, so read it before anything else and close the inode set
        // over what turns up.
        for (parent, name) in &batch.dentries {
            let key = keys::dentry(*parent, name.as_bytes());
            if let Some(old) = self.get(base, &key)? {
                inodes.insert(DentryRecord::decode(&old)?.ino);
            }
            plan.reads.key(key);
            if let Some(ino) = self.meta.child_ino_reader(*parent, name)? {
                inodes.insert(ino);
            }
        }

        let mut rows: BTreeMap<Ino, Option<TreeInode>> = BTreeMap::new();
        for ino in &inodes {
            let row = self.meta.tree_inode(*ino)?;
            let links = self.meta.links_of(*ino)?;
            // Every name pointing at this inode carries a copy of its
            // attrs (§P6's `0x02` value, S1's verdict), so an attribute
            // change fans out to each of them. Hard links are rare, so
            // this is one extra key in the common case.
            names.extend(links.iter().cloned());
            let present = row.as_ref().is_some_and(|row| row.attr.nlink > 0);
            self.plan_inode(base, &mut plan, *ino, row.as_ref(), &links, present)?;
            rows.insert(*ino, row);
        }

        for (parent, name) in &names {
            let key = keys::dentry(*parent, name.as_bytes());
            plan.reads.key(key.clone());
            let ino = self.meta.child_ino_reader(*parent, name)?;
            let attrs = match ino {
                Some(ino) => match rows.entry(ino) {
                    std::collections::btree_map::Entry::Occupied(row) => {
                        row.get().as_ref().map(|row| attrs_of(&row.attr))
                    }
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        let row = self.meta.tree_inode(ino)?;
                        let attrs = row.as_ref().map(|row| attrs_of(&row.attr));
                        slot.insert(row);
                        attrs
                    }
                },
                None => None,
            };
            match (ino, attrs) {
                (Some(ino), Some(attrs)) if attrs.nlink > 0 => {
                    plan.upsert(key, DentryRecord::new(ino, attrs).encode())
                }
                _ => plan.remove(key),
            }
        }

        if batch.subsystems {
            self.plan_subsystems(base, &mut plan)?;
        }
        plan.entities = inodes.len() + names.len() + usize::from(batch.subsystems);
        Ok(plan)
    }

    /// The `0x30` records (B) publishes, re-derived whole: they are a
    /// handful per filesystem.
    fn plan_subsystems(&self, base: Option<&NodeHash>, plan: &mut Plan) -> Result<()> {
        let wanted = crate::mtree_read::subsystem_state(self.meta)?;
        for subsystem in crate::mtree_read::PUBLISHED_SUBSYSTEMS {
            let range = keys::records_of(subsystem);
            plan.reads.prefix(range.prefix().to_vec());
            for (key, _) in self.scan(base, &range)? {
                if !wanted.contains_key(&key) {
                    plan.remove(key);
                }
            }
        }
        for (key, value) in wanted {
            plan.upsert(key, value);
        }
        Ok(())
    }

    /// One inode's `0x01`, `0x03` and `0x04` keys.
    fn plan_inode(
        &self,
        base: Option<&NodeHash>,
        plan: &mut Plan,
        ino: Ino,
        row: Option<&TreeInode>,
        links: &[(Ino, String)],
        present: bool,
    ) -> Result<()> {
        let inode_key = keys::inode(ino);
        plan.reads.key(inode_key.clone());

        // The reverse index (`0x04`) is a set of names, so the edit is
        // the symmetric difference between what the tree holds and what
        // the replica holds. The scan is a negative observation and
        // goes in the read set as a prefix: a winner that added a name
        // here invalidates the deletes computed from it.
        let name_range = keys::names_of(ino);
        plan.reads.prefix(name_range.prefix().to_vec());
        let stale: BTreeSet<Vec<u8>> = self
            .scan(base, &name_range)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        let wanted: BTreeSet<Vec<u8>> = if present {
            links
                .iter()
                .map(|(parent, name)| keys::rdentry(ino, *parent, name.as_bytes()))
                .collect()
        } else {
            BTreeSet::new()
        };
        for key in stale.difference(&wanted) {
            plan.remove(key.clone());
        }
        for key in wanted {
            plan.upsert(key, record::RDENTRY_VALUE.to_vec());
        }

        let xattr_range = keys::xattrs_of(ino);
        plan.reads.prefix(xattr_range.prefix().to_vec());
        let spilled: BTreeSet<Vec<u8>> = self
            .scan(base, &xattr_range)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        let Some(row) = row.filter(|_| present) else {
            plan.remove(inode_key);
            for key in spilled {
                plan.remove(key);
            }
            return Ok(());
        };

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
            |bytes| self.blobs.hash(bytes),
        );
        plan.blobs.extend(planned.blobs);
        plan.upsert(inode_key, planned.record.encode());

        let mut wanted: BTreeSet<Vec<u8>> = BTreeSet::new();
        if planned.xattrs == record::XattrPlacement::Spilled {
            for (name, value) in &xattrs {
                let key = keys::xattr(ino, name);
                let (payload, blob) =
                    record::place_value(value.clone(), |bytes| self.blobs.hash(bytes));
                debug_assert!(
                    payload.encoded_len() <= VALUE_SPILL + 1,
                    "a spilled payload must be a hash"
                );
                plan.blobs.extend(blob);
                plan.upsert(key.clone(), payload.encode());
                wanted.insert(key);
            }
        }
        for key in spilled.difference(&wanted) {
            plan.remove(key.clone());
        }
        Ok(())
    }

    fn get(&self, base: Option<&NodeHash>, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        match base {
            Some(root) => Ok(self.tree.get(root, key)?),
            None => Ok(None),
        }
    }

    fn scan(
        &self,
        base: Option<&NodeHash>,
        range: &keys::KeyRange,
    ) -> Result<Vec<Entry>, StoreError> {
        match base {
            Some(root) => Ok(self
                .tree
                .range(root, range.start(), range.prefix(), usize::MAX)?),
            None => Ok(Vec::new()),
        }
    }
}

/// `fs-core`'s attrs as §P6 stores them: the same fields minus atime,
/// which the tree does not have a place for (§P6, `record::Attrs`).
///
/// The kind mapping is a cast because S3 made `record::Kind`'s
/// discriminants match `InodeKind::as_u8` on purpose; `from_u8` is
/// still called rather than transmuted so a future divergence is an
/// error instead of a reinterpretation.
fn attrs_of(attr: &FileAttr) -> Attrs {
    Attrs {
        kind: Kind::from_u8(attr.kind.as_u8()).unwrap_or(Kind::File),
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
    use constellation_meta::{MetaStore, SetXattrMode};
    use constellation_mtree::{Hasher, Payload};
    use constellation_store_s3::PackStore;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};
    use tempfile::TempDir;

    /// A replica, a bucket, and however many publishers a test needs.
    /// Several publishers over *one* replica is the shape the rebase
    /// tests want: two writers whose batches are disjoint, which is
    /// what §P3 splices, rather than two unrelated filesystems.
    struct Fixture {
        store: Arc<dyn ObjectStore>,
        meta: Arc<SqliteMeta>,
        dirs: Vec<TempDir>,
    }

    impl Fixture {
        fn new() -> Fixture {
            Fixture {
                store: Arc::new(InMemory::new()),
                meta: Arc::new(SqliteMeta::open_in_memory().unwrap()),
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

        fn publisher_on(&mut self, store: Arc<dyn ObjectStore>, node_id: u64) -> TreePublisher {
            let cache = self.cache(store.clone());
            TreePublisher::new(
                Arc::clone(&self.meta),
                cache,
                BlobStore::new(store.clone(), Hasher::Plain),
                CommitChain::new(store),
                record::config(),
                node_id,
                tokio::runtime::Handle::current(),
            )
        }

        fn publisher(&mut self, node_id: u64) -> TreePublisher {
            let store = Arc::clone(&self.store);
            self.publisher_on(store, node_id)
        }

        /// Drain the journal exactly as shipping does — read the batch,
        /// then acknowledge it — so a test's second publish sees only
        /// the second batch's records. Handing the whole journal over
        /// every time would make every publish look incremental while
        /// actually re-deriving the namespace.
        fn records(&self) -> Vec<LogRecord> {
            let batch = self.meta.take_journal(usize::MAX).unwrap();
            if let Some((seq, _)) = batch.last() {
                self.meta.ack_journal(*seq).unwrap();
            }
            batch.into_iter().map(|(_, record)| record).collect()
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

    /// Note everything journaled so far and publish it.
    async fn publish(publisher: &mut TreePublisher, records: &[LogRecord]) -> Option<Commit> {
        publisher.note(records);
        publisher.publish(7).await.unwrap()
    }

    fn seed(meta: &SqliteMeta, files: u64) -> Ino {
        let dir = meta.mkdir(ROOT_INO, "d", 0o755, 1000, 1000).unwrap().ino;
        for i in 0..files {
            meta.create(dir, &format!("f{i}"), 0o644, 1000, 1000)
                .unwrap();
        }
        dir
    }

    /// The claim the whole step rests on: a tree grown one batch at a
    /// time is byte-identical to one built from the replica in bulk.
    ///
    /// It is an equality of root hashes rather than a structural
    /// comparison because the tree is canonical — which is also why
    /// this single assertion covers dentry attr copies, the reverse
    /// index, xattr placement and delete handling at once. Anything the
    /// incremental path forgets to write, or writes differently,
    /// changes the hash.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_incremental_history_matches_a_full_rebuild() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 40);
        let mut publisher = fx.publisher(1);
        let first = publish(&mut publisher, &fx.records()).await.unwrap();
        assert_eq!(first.seq, 1);

        // A mixed workload, published incrementally after each step, so
        // every intermediate root is checked too rather than only the
        // final one.
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
            let records = fx.records();
            last = publish(&mut publisher, &records)
                .await
                .unwrap_or_else(|| panic!("step {i} published nothing"));
            // A fresh publisher over a fresh bucket rebuilds from the
            // replica alone; the roots must agree at every step.
            let mut bulk = Fixture {
                store: Arc::new(InMemory::new()),
                meta: Arc::clone(&fx.meta),
                dirs: Vec::new(),
            };
            let mut rebuilder = bulk.publisher(2);
            let rebuilt = rebuilder.publish(7).await.unwrap().unwrap();
            assert_eq!(
                last.root(SHARD0),
                rebuilt.root(SHARD0),
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
        let full = publish(&mut publisher, &fx.records()).await.unwrap();
        // 3,000 files + the directory + root: an 0x01, an 0x02 and an
        // 0x04 for each named inode.
        assert!(full.intent.ops > 9_000, "{}", full.intent.ops);

        fx.meta.create(dir, "just-one", 0o644, 1, 1).unwrap();
        let records = fx.records();
        let one = publish(&mut publisher, &records).await.unwrap();
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

    /// §P6 excludes atime from the tree, so the record that carries it
    /// must move nothing at all. Without this, a read-only `find` over
    /// the namespace would publish a commit per batch — the write shape
    /// §14.4 measured, arriving through the read path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn atime_never_reaches_the_tree() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 4);
        let mut publisher = fx.publisher(1);
        let first = publish(&mut publisher, &fx.records()).await.unwrap();

        let ino = fx.meta.child_ino(dir, "f0").unwrap().unwrap();
        let bumps: Vec<LogRecord> = (0..100)
            .map(|i| LogRecord::Atime {
                ino,
                atime_ns: 1_000 + i,
                time_ns: 1_000 + i,
            })
            .collect();
        publisher.note(&bumps);
        assert!(publisher.pending().is_empty());
        assert!(publisher.publish(7).await.unwrap().is_none());
        assert_eq!(
            publisher.published(),
            Some((first.root(SHARD0).unwrap(), 1))
        );
    }

    /// A restart reuses the published root only when it can prove the
    /// tree is level with the replica, and rebuilds when it cannot.
    ///
    /// The rebuild has to be *equal*, not merely valid: if a crash
    /// could change the root hash for a replica that did not change,
    /// every node's idea of the filesystem would depend on its restart
    /// history and structural sharing between them would collapse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restart_rebuilds_unless_the_tree_is_level_with_the_replica() {
        let mut fx = Fixture::new();
        seed(&fx.meta, 20);
        let mut publisher = fx.publisher(1);
        let first = publish(&mut publisher, &fx.records()).await.unwrap();

        let mut warm = fx.publisher(1);
        warm.restore().unwrap();
        assert_eq!(warm.published(), Some((first.root(SHARD0).unwrap(), 1)));
        assert!(warm.pending().is_empty(), "a level tree owes no edits");

        // The crash: the replica applied log the publisher never saw,
        // so the in-memory changed set is gone and unreconstructable.
        fx.meta.kv_set(KV_VECTOR, "p0:9999").unwrap();
        let mut cold = fx.publisher(1);
        cold.restore().unwrap();
        assert!(
            cold.published().is_none(),
            "a stale tree must not be trusted"
        );

        // The rebuild must reproduce the published root exactly — which
        // also means it has nothing to commit.
        assert!(
            cold.publish(7).await.unwrap().is_none(),
            "an unchanged replica must rebuild to the same root and commit nothing"
        );
        assert_eq!(cold.published(), Some((first.root(SHARD0).unwrap(), 1)));
        assert!(cold.pending().is_empty());
    }

    /// An object store that lets a test put a competing commit into
    /// the slot *between* a publisher's head discovery and its CAS.
    ///
    /// The race the rebase hook exists for is a window a few
    /// microseconds wide, and a test that tried to hit it by running
    /// two publishers concurrently would be a flake generator. So the
    /// window is opened deliberately: the winner's commit is published
    /// normally, lifted off the bucket, and re-installed by this
    /// wrapper on the loser's first write to `commits/`. What the loser
    /// sees is exactly what it would see in a real race.
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

    /// Set a race up: a common base commit, then two publishers whose
    /// batches are whatever the caller does in `a` and `b`. Returns the
    /// loser's publish result.
    async fn race(
        fx: &mut Fixture,
        a: impl FnOnce(&SqliteMeta),
        b: impl FnOnce(&SqliteMeta),
    ) -> (Option<Commit>, Commit, TreePublisher) {
        let mut winner = fx.publisher(1);
        publish(&mut winner, &fx.records()).await.unwrap();

        // The loser picks the base commit up from the replica exactly
        // as a restarted daemon would, before the winner moves on.
        let racer = Arc::new(RaceStore {
            inner: Arc::clone(&fx.store),
            staged: std::sync::Mutex::new(None),
        });
        let mut loser = fx.publisher_on(Arc::clone(&racer) as Arc<dyn ObjectStore>, 2);
        loser.restore().unwrap();
        assert!(loser.published().is_some(), "the loser must share a parent");

        a(&fx.meta);
        let won = publish(&mut winner, &fx.records()).await.unwrap();

        // Lift the winner's commit off the bucket and hand it to the
        // wrapper, which puts it back the instant the loser CASes.
        let path = constellation_store_s3::layout::commit(won.seq);
        let body = fx.store.get(&path).await.unwrap().bytes().await.unwrap();
        fx.store.delete(&path).await.unwrap();
        *racer.staged.lock().expect("staged commit") = Some((path, body.to_vec()));

        b(&fx.meta);
        let records = fx.records();
        loser.note(&records);
        (loser.publish(7).await.unwrap(), won, loser)
    }

    /// §P3's splice, and the test a blind retry fails.
    ///
    /// Two publishers build on the same parent and touch disjoint keys.
    /// One wins the slot; the loser must re-apply its write-set **onto
    /// the winner's root** and land at the next sequence with both
    /// batches present. Simply retrying its own payload would also
    /// produce a commit, and would also look green — while silently
    /// deleting every key the winner added, because its root was
    /// computed from a parent that no longer exists. That is the
    /// failure this asserts against: `a` must still be there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_publishers_with_disjoint_keys_both_survive() {
        let mut fx = Fixture::new();
        let da = fx.meta.mkdir(ROOT_INO, "da", 0o755, 0, 0).unwrap().ino;
        let db = fx.meta.mkdir(ROOT_INO, "db", 0o755, 0, 0).unwrap().ino;
        let (landed, won, _) = race(
            &mut fx,
            |meta| {
                meta.create(da, "a", 0o644, 0, 0).unwrap();
            },
            |meta| {
                meta.create(db, "b", 0o644, 0, 0).unwrap();
            },
        )
        .await;

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
        let (landed, won, loser) = race(
            &mut fx,
            |meta| {
                meta.create(shared, "a", 0o644, 0, 0).unwrap();
            },
            |meta| {
                meta.create(shared, "b", 0o644, 0, 0).unwrap();
            },
        )
        .await;
        assert!(landed.is_none(), "an overlapping batch must not commit");
        assert!(
            !loser.pending().is_empty(),
            "a declined batch must survive to be re-executed"
        );
        let chain = CommitChain::new(Arc::clone(&fx.store));
        assert_eq!(chain.list_from(0).await.unwrap().last(), Some(&won.seq));
    }

    /// The S5 regression the `applied` vector exists to stop: a replica
    /// that is behind the chain head must not publish on top of it.
    ///
    /// Adopting the head and then writing this replica's values for its
    /// touched keys would overwrite anything newer the head holds with
    /// the older value still in this replica, and `conflicts_with`
    /// never sees it — the head was adopted *before* planning, so the
    /// stale value is ours and appears in no diff. The replica has to
    /// wait for its tailer instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_behind_the_head_defers_instead_of_regressing() {
        let mut fx = Fixture::new();
        seed(&fx.meta, 5);
        fx.meta.set_applied_seq_of("p0", 10).unwrap();
        let mut ahead = fx.publisher(1);
        let head = publish(&mut ahead, &fx.records()).await.unwrap();
        assert_eq!(head.applied.get("p0"), Some(&10));

        // Same bucket, a replica that has applied less of p0.
        fx.meta.set_applied_seq_of("p0", 3).unwrap();
        let mut behind = fx.publisher(2);
        assert!(
            behind.publish(7).await.unwrap().is_none(),
            "a replica behind the head must defer"
        );
        assert!(!behind.pending().is_empty(), "and keep its work");
        let chain = CommitChain::new(Arc::clone(&fx.store));
        assert_eq!(chain.discover_head(0).await.unwrap(), Some(head.seq));

        // Once the tailer has caught up, the same publisher lands (with a
        // change of its own: a replica level with the head and holding
        // nothing new has nothing to commit).
        fx.meta.set_applied_seq_of("p0", 10).unwrap();
        fx.meta
            .create(ROOT_INO, "after-catch-up", 0o644, 0, 0)
            .unwrap();
        behind.note(&fx.records());
        let landed = behind.publish(7).await.unwrap().expect("caught up");
        assert_eq!((landed.seq, landed.applied.get("p0")), (2, Some(&10)));
    }

    /// The same guard on the lost-CAS path: a winner whose vector this
    /// replica does not cover is declined rather than spliced onto,
    /// even when its key set is disjoint from the batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_winner_ahead_of_the_loser_is_not_spliced_onto() {
        let mut fx = Fixture::new();
        let da = fx.meta.mkdir(ROOT_INO, "da", 0o755, 0, 0).unwrap().ino;
        let db = fx.meta.mkdir(ROOT_INO, "db", 0o755, 0, 0).unwrap().ino;
        let (landed, won, loser) = race(
            &mut fx,
            |meta| {
                meta.create(da, "a", 0o644, 0, 0).unwrap();
                meta.set_applied_seq_of("p0", 10).unwrap();
            },
            |meta| {
                meta.create(db, "b", 0o644, 0, 0).unwrap();
                meta.set_applied_seq_of("p0", 5).unwrap();
            },
        )
        .await;
        assert_eq!(won.applied.get("p0"), Some(&10));
        assert!(landed.is_none(), "an ahead winner must not be spliced onto");
        assert!(!loser.pending().is_empty());
    }

    /// An object store whose writes stall, so a publish can be caught
    /// in flight.
    #[derive(Debug)]
    struct SlowStore {
        inner: Arc<dyn ObjectStore>,
        stall: std::sync::atomic::AtomicBool,
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
            if self.stall.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
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
    /// await point: its batch stays pending and the next publish carries
    /// it. (Found by `snapshot-churn`, where a clone never reached any
    /// commit because a cancelled round had already taken its keys.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancelled_publish_keeps_its_batch() {
        let mut fx = Fixture::new();
        let dir = seed(&fx.meta, 3);
        let slow = Arc::new(SlowStore {
            inner: Arc::clone(&fx.store),
            stall: std::sync::atomic::AtomicBool::new(false),
        });
        let mut publisher = fx.publisher_on(Arc::clone(&slow) as Arc<dyn ObjectStore>, 1);
        publish(&mut publisher, &fx.records()).await.unwrap();

        let late = fx.meta.create(dir, "late", 0o644, 0, 0).unwrap().ino;
        publisher.note(&fx.records());
        slow.stall.store(true, std::sync::atomic::Ordering::Relaxed);
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_millis(200), publisher.publish(7)).await;
        assert!(cancelled.is_err(), "the stalled publish must be cut off");
        assert!(
            !publisher.pending().is_empty(),
            "the cancelled batch was dropped"
        );

        slow.stall
            .store(false, std::sync::atomic::Ordering::Relaxed);
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
        assert!(publisher.pending().is_empty());
    }

    /// Plan 28 §13's measurement: `getattr` latency on a file-backed
    /// replica while a full-rebuild publish runs, against the same loop
    /// with nothing publishing. The publish reads through the per-thread
    /// WAL reader connections and builds on a blocking thread, so FUSE's
    /// point reads should not queue behind it the way they did behind
    /// `VACUUM INTO`. Ignored: it is a measurement, run by hand with
    /// `cargo test --release -p constellation getattr_latency -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn getattr_latency_during_a_publish() {
        let dir = TempDir::new().unwrap();
        let mut fx = Fixture::new();
        fx.meta = Arc::new(SqliteMeta::open(dir.path().join("m.db")).unwrap());
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
        fx.records();

        let sample =
            |meta: Arc<SqliteMeta>, inos: Vec<u64>, stop: Arc<std::sync::atomic::AtomicBool>| {
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
        // commit (S6's bootstrap, minus log tailing), from a cold cache.
        let scratch = TempDir::new().unwrap();
        let reader =
            crate::mtree_read::ChainReader::for_store(Arc::clone(&fx.store), None, scratch.path())
                .unwrap();
        let fresh = Arc::new(SqliteMeta::open(dir.path().join("fresh.db")).unwrap());
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
        let commit = publish(&mut publisher, &fx.records()).await.unwrap();

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
}
