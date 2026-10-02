//! Plan 32 Step 0.1: snapshot row operations execute at the root-lease
//! holder, so creating, deleting or holding a snapshot never moves the
//! write lease.
//!
//! Every snapshot row write — the `snaps/` object plus the replicated
//! row of a create, the row plus object of a delete, the `SnapHold` row
//! of a hold — is a journaled metadata mutation, and the journal only
//! reaches the cluster from the node holding the root lease. Before this
//! module each of them sent `SyncRequest::Acquire` first: a node taking
//! a snapshot pulled the lease off whichever node was writing, which
//! plan 32's scheduler (a snapshot every few minutes from whichever node
//! leads it) would have turned into a lease migration every few minutes.
//!
//! Instead every such write is an item of a [`SnapshotItem`] batch, and
//! a batch is routed the way `prune::unlink_now` routes an unlink:
//!
//! | this node | the lease object | the batch |
//! |---|---|---|
//! | holds a usable root lease | — | executes here |
//! | does not | names a live holder, and this node has a peer path | drains this node's own pending chunks, then is forwarded to the holder over the peer path (`Payload::SnapshotBatchRequest`) |
//! | does not | names a live holder, and P2P is off on this node | acquires the lease through the cooperative `wanted_by` handover (what every write does in that mode) and executes here |
//! | does not | names nobody, an expired or released lease, or this node | acquires the lease (an unheld lease is free) and executes here |
//!
//! A live holder is never preempted while there is a peer path to it,
//! and a forward that fails (holder unreachable, timeout, the holder no
//! longer holds) is the caller's error — never a reason to fall back to
//! acquiring. The scheduler simply tries again on its next tick. With
//! P2P off (an S3-only cluster) there is no forward to make: a non-holder
//! writes nothing without the lease in that mode, so a snapshot asks for
//! it exactly as it did before batches.
//!
//! **The holder executes a batch** ([`SnapshotBatcher::execute`]) in one
//! order: it drains what `SyncRequest::Barrier` drains for every
//! distinct create path; forces **one** publish if the batch creates
//! anything (one commit serves every policy due at the same tick); then
//! per item, in item order: a create is skipped when its
//! subtree is unchanged since its `skip_if_unchanged_since` root
//! ([`subtree_unchanged`]: no key of the tree diff maps into it),
//! otherwise it CAS-creates the `snaps/` object and journals the row; a
//! delete removes the row, then the object; a hold writes its row. A
//! drain or publish failure fails the whole batch before any item ran.
//!
//! **Exactly once.** A batch's rid is allocated once
//! ([`crate::forward::ForwardState::next_system_rid`]) and kept across
//! every retry. The executing node keeps recent batch results by rid
//! (bounded, like the core's `recent` outcomes) and answers a duplicate
//! from them without running it again; batches execute one at a time,
//! so a duplicate arriving while its original runs waits for it and
//! then reads its result. Across a holder change the new holder has no
//! such memory, and the `snaps/` name CAS is the backstop: a retried
//! create finds its own object and answers `AlreadyExists`, which the
//! scheduler counts as success (a manual `snapshot create` reports it
//! as today's "already exists" error). A retried delete finds no row
//! (`NotFound`), and a retried hold is idempotent. A result holding a
//! lease-loss refusal ([`LEASE_LOST`]) is not kept: that item did not
//! run, and a retry under the same rid must run it, not replay the
//! refusal.

use crate::snapshot::{self, PutRecord, SnapshotManager, SnapshotOptions, SnapshotRoot};
use crate::snapwalk::{self, Membership};
use anyhow::{bail, Context, Result};
use constellation_fs_core::Ino;
use constellation_meta::{Meta, Rid, SnapshotRow};
use constellation_mtree::keys::Key;
use constellation_mtree::{NodeHash, NodeStore, Tree};
use futures::future::BoxFuture;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use constellation_net::{
    SnapshotBatchOutcome, SnapshotItem, SnapshotItemResult as ItemResult, SnapshotRowWire,
};

/// The prefix of every item refusal that means "this node lost the root
/// lease before writing this item": transient, so a batch with one is
/// not kept by rid (see the module doc).
pub const LEASE_LOST: &str = "this node lost the root write lease";

fn lease_lost() -> ItemResult {
    ItemResult::Refused {
        reason: format!("{LEASE_LOST}; retry"),
    }
}

fn is_lease_lost(result: &ItemResult) -> bool {
    matches!(result, ItemResult::Refused { reason } if reason.starts_with(LEASE_LOST))
}

/// How many batch results an executing node remembers by rid. A batch's
/// retries come within seconds; this only has to outlast them.
const RECENT_BATCHES: usize = 1024;

/// How long the executor waits for its lease view to open for new
/// mutations (a takeover gate right after an acquisition, a handoff
/// pause) before it gives up on the batch.
const ADMIT_WAIT: Duration = Duration::from_secs(5);

/// How many sync rounds a snapshot's drain may take on a busy holder
/// (see [`SnapshotBatcher::drain`]).
const DRAIN_ATTEMPTS: u32 = 40;

/// Env: how long a forwarded snapshot batch may take at the holder
/// before the requester gives up (default 30 s). A batch drains, ships
/// and publishes there, so it needs far longer than a forwarded
/// mutation's `CONSTELLATION_FORWARD_TIMEOUT_MS`. The requester's error
/// is safe to retry: the rid makes a retry exactly-once.
pub fn forward_timeout() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_SNAPSHOT_FORWARD_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30_000),
    )
}

/// Env: how many keys of `Tree::diff(prev, new)` the skip-empty check
/// ([`subtree_unchanged`]) examines before it gives up and calls the
/// subtree changed (default 100 000). A diff that long means the tree
/// was busy since the previous snapshot; taking one more snapshot is
/// cheaper than reading the rest of it to be sure. Read once per batch.
pub fn empty_check_keys() -> usize {
    std::env::var("CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000)
}

/// [`subtree_unchanged`]'s answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Emptiness {
    /// Nothing under the directory changed: the previous snapshot *is*
    /// this state, and the create is skipped.
    Unchanged,
    /// Something did, or the check could not tell.
    Changed,
}

/// Plan 32 Step 3.4's empty check: whether directory `ino` is unchanged
/// between `prev` (the previous snapshot's root) and `root`, the commit
/// the batch just published.
///
/// Unchanged when `root == prev.root`, or when no key of
/// `Tree::diff(prev.root, root)` maps into the subtree of `ino` in
/// either root:
///
/// | key | counts when |
/// |---|---|
/// | `0x01 ino` (inode record) | `ino` is in the subtree, in the old or the new root |
/// | `0x02 parent \| name` (dentry) | `parent` is in the subtree (or is the directory), in either root |
/// | `0x03 ino \| xattr` | `ino` is in the subtree, in either root |
/// | `0x04 ino \| parent \| name` (reverse dentry) | `parent` is in the subtree, in either root |
/// | `0x30` subsystem rows | never: they belong to no directory |
///
/// "In either root" is what catches a rename across the boundary: the
/// name leaving is under the subtree in the old root, the name arriving
/// in the new one. Membership is snapwalk's ([`snapwalk::in_subtree`]),
/// with its per-`(root, dir)` answers in `membership`.
///
/// **atime.** A read's atime bump (plan 20) never reaches the tree:
/// §P6 keeps atime out of the inode record entirely
/// (`constellation_mtree::record::InodeRecord` has no atime field, and a
/// test pins the record's width), so an atime-only change is not a diff
/// key at all and needs no filtering here. An explicit `utimensat` is a
/// different thing: it moves ctime, which is in the record, and counts.
///
/// At most `budget` diff keys are examined; one more means `Changed`.
/// `prev.ino != ino` (the policy root was replaced by a new directory
/// at the same path) is `Changed`. So is every error — an unreadable
/// root, a malformed key, a membership walk snapwalk cannot vouch for —
/// logged at `debug`: a wrong `Unchanged` loses a snapshot, a wrong
/// `Changed` only takes one more.
pub fn subtree_unchanged<S: NodeStore>(
    tree: &Tree<S>,
    prev: &SnapshotRoot,
    root: NodeHash,
    ino: Ino,
    budget: usize,
    membership: &mut Membership,
) -> Emptiness {
    if prev.ino != ino {
        tracing::debug!(
            prev = prev.ino,
            ino,
            "skip-empty: the directory was replaced since the previous snapshot"
        );
        return Emptiness::Changed;
    }
    if prev.root == root {
        return Emptiness::Unchanged;
    }
    match diff_touches(tree, &prev.root, &root, ino, budget, membership) {
        Ok(Some(examined)) => {
            tracing::debug!(
                ino,
                examined,
                "skip-empty: nothing under the directory changed"
            );
            Emptiness::Unchanged
        }
        Ok(None) => Emptiness::Changed,
        Err(error) => {
            tracing::debug!(
                ino,
                prev = prev.seq,
                error = %format!("{error:#}"),
                "skip-empty: the check failed; treating the directory as changed"
            );
            Emptiness::Changed
        }
    }
}

/// `Some(keys examined)` when no key of the diff maps into the subtree
/// of `dir`; `None` as soon as one does or the budget runs out.
fn diff_touches<S: NodeStore>(
    tree: &Tree<S>,
    old: &NodeHash,
    new: &NodeHash,
    dir: Ino,
    budget: usize,
    membership: &mut Membership,
) -> Result<Option<usize>> {
    let mut examined = 0usize;
    let mut failed = None;
    let member = |of: Ino, membership: &mut Membership| -> Result<bool> {
        Ok(snapwalk::in_subtree(tree, old, of, dir, membership)?
            || snapwalk::in_subtree(tree, new, of, dir, membership)?)
    };
    let finished = tree.diff_each(old, new, |key, _| {
        examined += 1;
        if examined > budget {
            tracing::debug!(
                dir,
                budget,
                "skip-empty: the diff exceeds CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS; \
                 treating the directory as changed"
            );
            return false;
        }
        let touches = Key::parse(key)
            .map_err(anyhow::Error::from)
            .and_then(|key| match key {
                Key::Inode { ino } | Key::Xattr { ino, .. } => member(ino, membership),
                Key::Dentry { parent_ino, .. } | Key::RDentry { parent_ino, .. } => {
                    member(parent_ino, membership)
                }
                Key::Subsystem { .. } => Ok(false),
            });
        match touches {
            Ok(touches) => !touches,
            Err(error) => {
                failed = Some(error);
                false
            }
        }
    })?;
    if let Some(error) = failed {
        return Err(error.context("mapping a diff key into the subtree"));
    }
    Ok(finished.then_some(examined))
}

pub fn row_to_wire(row: &SnapshotRow) -> SnapshotRowWire {
    SnapshotRowWire {
        id: row.id.clone(),
        path: row.path.clone(),
        name: row.name.clone(),
        root_hash: row.root_hash.clone(),
        created_unix_ms: row.created_unix_ms,
        origin: row.origin,
        policy_ino: row.policy_ino,
        held: row.held,
        creator: row.creator,
        held_by: row.held_by.clone(),
        refer_bytes: row.refer_bytes,
    }
}

pub fn row_from_wire(row: SnapshotRowWire) -> SnapshotRow {
    SnapshotRow {
        id: row.id,
        path: row.path,
        name: row.name,
        root_hash: row.root_hash,
        created_unix_ms: row.created_unix_ms,
        origin: row.origin,
        policy_ino: row.policy_ino,
        held: row.held,
        creator: row.creator,
        held_by: row.held_by,
        refer_bytes: row.refer_bytes,
    }
}

fn rid_to_wire(rid: Rid) -> (u64, u32, u64) {
    (rid.node, rid.incarnation, rid.seq)
}

pub fn rid_from_wire(rid: (u64, u32, u64)) -> Rid {
    Rid {
        node: rid.0,
        incarnation: rid.1,
        seq: rid.2,
    }
}

/// A row write admitted under this node's write authority: in the
/// daemon, the lease view's [`crate::lease::AdmitGuard`], so a release's
/// final flush cannot miss the row. Hold it across the metadata write
/// and no longer.
pub struct Admission<'a>(#[allow(dead_code)] Option<crate::lease::AdmitGuard<'a>>);

impl Admission<'_> {
    /// An admission with no lease view behind it (tests, whose hosts
    /// decide authority themselves).
    pub fn unguarded() -> Admission<'static> {
        Admission(None)
    }
}

/// What a batch needs from the node it runs on. The daemon's is
/// [`EngineBatchHost`]; the multi-node tests wire standalone drivers.
pub trait BatchHost: Send + Sync {
    /// This node holds a usable root lease.
    fn holds(&self) -> bool;
    /// Admit one row write (`None`: the lease view is closed to new
    /// mutations — not held, releasing, gated, paused).
    fn admit(&self) -> Option<Admission<'_>>;
    /// The live holder the lease object names, if any: `None` for no
    /// lease, an expired or released one.
    fn live_holder(&self) -> BoxFuture<'_, Result<Option<u64>>>;
    /// Take the lease if it is free (`SyncRequest::Acquire`).
    fn acquire(&self) -> BoxFuture<'_, Result<bool>>;
    /// This node has a peer path at all (P2P is on). `false`: no batch
    /// can be forwarded, and a non-holder acquires as before batches.
    fn peer_path(&self) -> bool;
    /// Upload this node's own pending chunks, all of them
    /// (`SyncRequest::DrainInode { ino: 0 }`), before a create is
    /// forwarded: the holder's barrier cannot ship the journal rows this
    /// node forwarded to it while their chunks are only here.
    fn drain_own(&self) -> BoxFuture<'_, Result<()>>;
    /// What `SyncRequest::Barrier` drains for directory `ino`.
    fn drain(&self, ino: Ino) -> BoxFuture<'_, Result<()>>;
    /// Send the batch to `to`, the believed holder, and return its
    /// answer. An error is a transport failure: the holder may or may
    /// not have executed it.
    fn forward(
        &self,
        to: u64,
        rid: Rid,
        items: Vec<SnapshotItem>,
    ) -> BoxFuture<'_, Result<SnapshotBatchOutcome>>;
    /// Best effort, after a forwarded batch: bring this replica up to
    /// the holder's log, so a `snapshot ls` here right after shows it.
    fn catch_up(&self) -> BoxFuture<'_, ()>;
    /// Rows were journaled: ship them soon.
    fn nudge(&self);
}

/// One batch's skip-empty settings: the diff budget, and membership
/// answers shared by its items (cached per `(root, dir)`, so items of
/// different directories or previous roots do not collide).
struct EmptyCheck {
    budget: usize,
    membership: Arc<Mutex<Membership>>,
}

/// A create validated against the executing replica: its normalized
/// path, directory inode and row options — or why it was refused.
type Prepared = std::result::Result<(String, Ino, SnapshotOptions), String>;

/// Where a batch ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// This node held the root lease.
    Local,
    /// Forwarded to this live holder.
    Forwarded(u64),
    /// Nobody held the lease: this node took it and ran the batch.
    Acquired,
}

/// A batch's results, in item order, and where it ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReport {
    pub results: Vec<ItemResult>,
    pub route: Route,
}

#[derive(Default)]
struct Recent {
    order: VecDeque<Rid>,
    results: HashMap<Rid, Vec<ItemResult>>,
}

impl Recent {
    fn get(&self, rid: &Rid) -> Option<Vec<ItemResult>> {
        self.results.get(rid).cloned()
    }

    fn insert(&mut self, rid: Rid, results: Vec<ItemResult>) {
        if self.results.insert(rid, results).is_none() {
            self.order.push_back(rid);
        }
        while self.order.len() > RECENT_BATCHES {
            if let Some(old) = self.order.pop_front() {
                self.results.remove(&old);
            }
        }
    }
}

/// The router and the holder-side executor of snapshot batches (see the
/// module doc). One per engine.
pub struct SnapshotBatcher {
    node_id: u64,
    meta: Arc<Meta>,
    snapshots: Arc<SnapshotManager>,
    host: Arc<dyn BatchHost>,
    forward: Arc<crate::forward::ForwardState>,
    /// A read-only member journals nothing and may not ask another node
    /// to journal on its behalf either.
    read_only: bool,
    /// Batches execute one at a time on a node: one publish each, and a
    /// duplicate waits for its original instead of racing it.
    exec: tokio::sync::Mutex<()>,
    recent: Mutex<Recent>,
}

impl SnapshotBatcher {
    pub fn new(
        node_id: u64,
        meta: Arc<Meta>,
        snapshots: Arc<SnapshotManager>,
        host: Arc<dyn BatchHost>,
        forward: Arc<crate::forward::ForwardState>,
        read_only: bool,
    ) -> SnapshotBatcher {
        SnapshotBatcher {
            node_id,
            meta,
            snapshots,
            host,
            forward,
            read_only,
            exec: tokio::sync::Mutex::new(()),
            recent: Mutex::new(Recent::default()),
        }
    }

    /// A fresh rid for one batch. Keep it across that batch's retries.
    pub fn next_rid(&self) -> Rid {
        self.forward.next_system_rid(self.node_id)
    }

    /// Run `items` as batch `rid`, wherever it has to run, and return the
    /// per-item results. The engine API the scheduler calls from a
    /// background task (no control round trip). Allocate `rid` once with
    /// [`Self::next_rid`] and pass the same one to every retry of the
    /// batch: that is what makes a retry exactly-once (see the module
    /// doc). [`Self::run`] is the same, and also says where it ran.
    pub async fn submit(&self, rid: Rid, items: Vec<SnapshotItem>) -> Result<Vec<ItemResult>> {
        Ok(self.run(rid, items).await?.results)
    }

    /// Route and run a batch under `rid` (see the module doc's table).
    pub async fn run(&self, rid: Rid, items: Vec<SnapshotItem>) -> Result<BatchReport> {
        if self.read_only {
            bail!("this node is a read-only member: it cannot take, delete or hold snapshots");
        }
        if self.host.holds() {
            match self.execute(rid, &items).await {
                SnapshotBatchOutcome::Done(results) => {
                    return Ok(BatchReport {
                        results,
                        route: Route::Local,
                    })
                }
                // Lost between the check and the execution: route anew.
                SnapshotBatchOutcome::NotHolder => {}
                SnapshotBatchOutcome::Failed(error) => bail!(error),
            }
        }
        let holder = self
            .host
            .live_holder()
            .await
            .context("reading the root lease to find its holder")?;
        match holder {
            Some(holder) if holder != self.node_id && self.host.peer_path() => {
                // What the requester's own `Barrier` used to upload
                // before it took the lease: chunks written here, whose
                // forwarded rows the holder's drain must ship.
                if items
                    .iter()
                    .any(|item| matches!(item, SnapshotItem::Create { .. }))
                {
                    self.host
                        .drain_own()
                        .await
                        .context("uploading this node's pending writes before the snapshot")?;
                }
                tracing::debug!(rid = ?rid, holder, items = items.len(), "snapshot batch: forwarding to the holder");
                let outcome = self
                    .host
                    .forward(holder, rid, items)
                    .await
                    .with_context(|| {
                        format!(
                            "forwarding the snapshot operation to node {holder}, \
                             which holds the root write lease"
                        )
                    })?;
                match outcome {
                    SnapshotBatchOutcome::Done(results) => {
                        self.host.catch_up().await;
                        Ok(BatchReport {
                            results,
                            route: Route::Forwarded(holder),
                        })
                    }
                    SnapshotBatchOutcome::NotHolder => bail!(
                        "node {holder} no longer holds the root write lease; retry the snapshot operation"
                    ),
                    SnapshotBatchOutcome::Failed(error) => {
                        bail!("node {holder}, the root-lease holder: {error}")
                    }
                }
            }
            // Nobody holds it (or the object still names this node,
            // whose view is closed): an unheld lease is free to take. Or
            // a live holder, but P2P is off here: no forward exists, so
            // ask for the lease through the cooperative handover, as
            // every write in that mode does (and as snapshots did before
            // batches).
            _ => {
                if !self.host.acquire().await? {
                    bail!("subtree write lease is held by another node");
                }
                match self.execute(rid, &items).await {
                    SnapshotBatchOutcome::Done(results) => Ok(BatchReport {
                        results,
                        route: Route::Acquired,
                    }),
                    SnapshotBatchOutcome::NotHolder => {
                        bail!("lost the root write lease right after taking it; retry")
                    }
                    SnapshotBatchOutcome::Failed(error) => bail!(error),
                }
            }
        }
    }

    /// Execute a batch as the root-lease holder: locally routed, or a
    /// peer's forward. A rid this node already executed is answered from
    /// its results.
    pub async fn execute(&self, rid: Rid, items: &[SnapshotItem]) -> SnapshotBatchOutcome {
        let _one_at_a_time = self.exec.lock().await;
        if let Some(results) = self.recent.lock().unwrap().get(&rid) {
            tracing::debug!(rid = ?rid, "snapshot batch: a duplicate, answered from its first execution");
            return SnapshotBatchOutcome::Done(results);
        }
        if !self.host.holds() {
            return SnapshotBatchOutcome::NotHolder;
        }
        match self.execute_now(items).await {
            Ok(results) => {
                // A lease-loss refusal is transient: the retry under
                // this rid must run, not read it back.
                if !results.iter().any(is_lease_lost) {
                    self.recent.lock().unwrap().insert(rid, results.clone());
                }
                SnapshotBatchOutcome::Done(results)
            }
            Err(error) => SnapshotBatchOutcome::Failed(format!("{error:#}")),
        }
    }

    async fn execute_now(&self, items: &[SnapshotItem]) -> Result<Vec<ItemResult>> {
        // Validate the creates against this (the holder's) replica.
        let mut prepared: Vec<Option<Prepared>> = Vec::with_capacity(items.len());
        let mut drains = BTreeSet::new();
        for item in items {
            prepared.push(match item {
                SnapshotItem::Create {
                    path,
                    name,
                    origin,
                    policy_ino,
                    held,
                    held_by,
                    ..
                } => {
                    let options = SnapshotOptions {
                        origin: *origin,
                        policy_ino: *policy_ino,
                        held: *held,
                        held_by: held_by.clone(),
                    };
                    Some(match self.snapshots.prepare_create(path, name, &options) {
                        Ok((path, ino)) => {
                            drains.insert(ino);
                            Ok((path, ino, options))
                        }
                        Err(error) => Err(format!("{error:#}")),
                    })
                }
                _ => None,
            });
        }
        // 1. What `Barrier` drains, once per distinct create path.
        for &ino in &drains {
            self.drain(ino).await?;
        }
        // 2. One publish for every create in the batch.
        let commit = if drains.is_empty() {
            None
        } else {
            Some(self.snapshots.publish_commit().await?)
        };
        self.wait_admissible().await?;
        // 3. The items, in order.
        let empty = EmptyCheck {
            budget: empty_check_keys(),
            membership: Arc::new(Mutex::new(Membership::default())),
        };
        let mut results = Vec::with_capacity(items.len());
        let mut wrote = false;
        for (item, prepared) in items.iter().zip(prepared) {
            let result = match (item, prepared) {
                (_, Some(Err(reason))) => ItemResult::Refused { reason },
                (
                    SnapshotItem::Create {
                        name,
                        creator,
                        skip_if_unchanged_since,
                        ..
                    },
                    Some(Ok((path, ino, options))),
                ) => {
                    let commit = commit.expect("a valid create publishes");
                    self.create(
                        &path,
                        name,
                        &options,
                        *creator,
                        ino,
                        commit,
                        skip_if_unchanged_since.as_deref(),
                        &empty,
                        &mut wrote,
                    )
                    .await
                }
                (SnapshotItem::Delete { id, force }, _) => {
                    self.delete(id, *force, &mut wrote).await
                }
                (
                    SnapshotItem::Hold {
                        id,
                        held,
                        by,
                        force,
                    },
                    _,
                ) => self.hold(id, *held, by.as_deref(), *force, &mut wrote),
                (SnapshotItem::Create { .. }, None) => unreachable!("every create is prepared"),
            };
            results.push(result);
        }
        if wrote {
            self.host.nudge();
        }
        Ok(results)
    }

    /// [`subtree_unchanged`] against this node's tree, off the runtime
    /// (tree reads block on the node cache). An error reaching the tree
    /// at all is `Changed`, like every other error of the check.
    async fn emptiness(
        &self,
        prev: SnapshotRoot,
        root: NodeHash,
        ino: Ino,
        empty: &EmptyCheck,
    ) -> Emptiness {
        if prev.ino == ino && prev.root == root {
            return Emptiness::Unchanged;
        }
        let access = match self.snapshots.tree() {
            Ok(access) => access.clone(),
            Err(error) => {
                tracing::debug!(ino, error = %format!("{error:#}"), "skip-empty: no tree to diff; treating the directory as changed");
                return Emptiness::Changed;
            }
        };
        let (budget, membership) = (empty.budget, empty.membership.clone());
        let checked = tokio::task::spawn_blocking(move || -> Result<Emptiness> {
            let tree = access.tree()?;
            let mut membership = membership.lock().expect("membership lock");
            Ok(subtree_unchanged(
                &tree,
                &prev,
                root,
                ino,
                budget,
                &mut membership,
            ))
        })
        .await;
        match checked {
            Ok(Ok(emptiness)) => emptiness,
            Ok(Err(error)) => {
                tracing::debug!(ino, error = %format!("{error:#}"), "skip-empty: opening the tree failed; treating the directory as changed");
                Emptiness::Changed
            }
            Err(error) => {
                tracing::debug!(ino, error = %error, "skip-empty: the check's task failed; treating the directory as changed");
                Emptiness::Changed
            }
        }
    }

    /// `Barrier` for `ino`. A round answers "not shipped" when the
    /// journal is not empty at its end — on a holder whose own clients
    /// keep writing, rows that arrived during the round. What a snapshot
    /// needs shipped is what was journaled before it asked, so a busy
    /// holder retries (bounded) while it still holds.
    async fn drain(&self, ino: Ino) -> Result<()> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.host.drain(ino).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < DRAIN_ATTEMPTS && self.host.holds() => {
                    tracing::debug!(ino, attempt, error = %format!("{error:#}"), "snapshot barrier: retrying");
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => {
                    return Err(error.context(format!("snapshot barrier for directory {ino}")))
                }
            }
        }
    }

    /// Wait (bounded) for the lease view to admit new mutations: right
    /// after an acquisition its takeover gate may still be pending.
    async fn wait_admissible(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + ADMIT_WAIT;
        loop {
            if self.host.admit().is_some() {
                return Ok(());
            }
            if !self.host.holds() {
                bail!("this node no longer holds the root write lease");
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "the root write lease is not open for new mutations \
                     (a release or a takeover gate is in progress); retry"
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create(
        &self,
        path: &str,
        name: &str,
        options: &SnapshotOptions,
        creator: u64,
        ino: Ino,
        commit: (u64, NodeHash),
        skip_if_unchanged_since: Option<&str>,
        empty: &EmptyCheck,
        wrote: &mut bool,
    ) -> ItemResult {
        if let Some(prev) = skip_if_unchanged_since {
            match SnapshotRoot::parse(prev) {
                Ok(prev) => {
                    if self.emptiness(prev, commit.1, ino, empty).await == Emptiness::Unchanged {
                        return ItemResult::Skipped;
                    }
                }
                Err(error) => {
                    return ItemResult::Refused {
                        reason: format!("skip_if_unchanged_since: {error:#}"),
                    }
                }
            }
        }
        // Refuse before writing the object if the lease is already gone.
        if self.host.admit().is_none() {
            return lease_lost();
        }
        let row = match self
            .snapshots
            .put_record(path, name, options, creator, ino, commit)
            .await
        {
            Ok(PutRecord::Put(row)) => row,
            Ok(PutRecord::AlreadyExists(id)) => return ItemResult::AlreadyExists { id },
            Err(error) => {
                return ItemResult::Refused {
                    reason: format!("{error:#}"),
                }
            }
        };
        let Some(_admitted) = self.host.admit() else {
            // The object is written and the row is not: an orphan, the
            // failure direction plan 32 §0.3's reconciliation repairs. A
            // retry under the same name finds the object (`AlreadyExists`).
            return ItemResult::Refused {
                reason: format!(
                    "{LEASE_LOST} after writing snaps/ object {}; \
                     its row was not recorded",
                    row.id
                ),
            };
        };
        match self.meta.record_snapshot(&row) {
            Ok(()) => {
                *wrote = true;
                ItemResult::Created {
                    id: row.id.clone(),
                    seq: commit.0,
                    root_hash: row.root_hash.clone(),
                    row: row_to_wire(&row),
                }
            }
            Err(error) => ItemResult::Refused {
                reason: format!("{error:#}"),
            },
        }
    }

    async fn delete(&self, id: &str, force: bool, wrote: &mut bool) -> ItemResult {
        let row = match self.meta.snapshot_by_id(id) {
            Ok(Some(row)) => row,
            Ok(None) => return ItemResult::NotFound,
            Err(error) => {
                return ItemResult::Refused {
                    reason: format!("{error:#}"),
                }
            }
        };
        // A held snapshot is never deleted out from under its owner
        // (plan 32 Step 5, plan 37's CSI driver).
        if row.held && !force {
            return ItemResult::Refused {
                reason: snapshot::held_refusal(&row),
            };
        }
        {
            let Some(_admitted) = self.host.admit() else {
                return lease_lost();
            };
            match self.meta.delete_snapshot_by_id(id) {
                Ok(true) => *wrote = true,
                Ok(false) => return ItemResult::NotFound,
                Err(error) => {
                    return ItemResult::Refused {
                        reason: format!("{error:#}"),
                    }
                }
            }
        }
        // Row first, then the object: a failure here leaves an orphan
        // object (GC's reconciliation deletes it once it is older than
        // `gc.horizon`, plan 32 §0.3), never a dangling row. The
        // caller hears of it, as it did before batches.
        if let Err(error) = self.snapshots.delete_record(&row.path, &row.name).await {
            tracing::warn!(
                id,
                error = %format!("{error:#}"),
                "snapshot row deleted, but its snaps/ object was not; it is an orphan now"
            );
            return ItemResult::DeletedObjectRemains {
                reason: format!("{error:#}"),
            };
        }
        ItemResult::Deleted
    }

    fn hold(
        &self,
        id: &str,
        held: bool,
        by: Option<&str>,
        force: bool,
        wrote: &mut bool,
    ) -> ItemResult {
        let by = by.filter(|by| !by.is_empty());
        if let Some(by) = by {
            if let Err(error) = snapshot::validate_owner(by) {
                return ItemResult::Refused {
                    reason: format!("{error:#}"),
                };
            }
        }
        let Some(_admitted) = self.host.admit() else {
            return lease_lost();
        };
        // The owner rule is `set_snapshot_hold`'s, inside its write
        // transaction (see `SnapshotManager::hold`).
        match self.meta.set_snapshot_hold(id, held, by, force) {
            Ok(Some(row)) => {
                *wrote = true;
                ItemResult::HoldSet {
                    row: row_to_wire(&row),
                }
            }
            Ok(None) => ItemResult::NotFound,
            Err(error) => ItemResult::Refused {
                reason: format!("{error:#}"),
            },
        }
    }
}

/// The daemon's [`BatchHost`]: the lease view, the sync task, the lease
/// object, and the peer path.
pub(crate) struct EngineBatchHost {
    pub(crate) node_id: u64,
    pub(crate) lease: Arc<crate::lease::LeaseView>,
    pub(crate) sync_tx: tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) lease_mode: constellation_store_s3::LeaseMode,
    pub(crate) peers: constellation_net::Peers,
    pub(crate) next_req: std::sync::atomic::AtomicU64,
}

impl EngineBatchHost {
    async fn ask<T>(
        &self,
        what: &str,
        request: impl FnOnce(tokio::sync::oneshot::Sender<T>) -> crate::sync::SyncRequest,
    ) -> Result<T> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(request(reply))
            .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
        receive.await.map_err(|_| anyhow::anyhow!("{what} stopped"))
    }
}

impl BatchHost for EngineBatchHost {
    fn holds(&self) -> bool {
        self.lease.usable()
    }

    fn admit(&self) -> Option<Admission<'_>> {
        self.lease.admit().map(|guard| Admission(Some(guard)))
    }

    fn live_holder(&self) -> BoxFuture<'_, Result<Option<u64>>> {
        Box::pin(async move {
            let leases = constellation_store_s3::LeaseStore::new(
                self.store.clone(),
                constellation_store_s3::log::PARTITION,
                self.lease_mode,
            );
            let now = crate::prune::now_unix_ms() as i64;
            Ok(leases
                .get()
                .await?
                .map(|(lease, _)| lease)
                .filter(|lease| !lease.is_claimable(now))
                .map(|lease| lease.holder))
        })
    }

    fn acquire(&self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let progress = self
                .ask("lease acquisition", |reply| {
                    crate::sync::SyncRequest::Acquire { reply }
                })
                .await?
                .map_err(|error| anyhow::anyhow!(error))?;
            Ok(progress.acquired)
        })
    }

    fn peer_path(&self) -> bool {
        self.peers.is_enabled()
    }

    fn drain_own(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.ask("pending-chunk drain", |reply| {
                crate::sync::SyncRequest::DrainInode { ino: 0, reply }
            })
            .await?
            .map_err(|error| anyhow::anyhow!(error))
        })
    }

    fn drain(&self, ino: Ino) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.ask("snapshot barrier", |reply| {
                crate::sync::SyncRequest::Barrier { ino, reply }
            })
            .await?
            .map_err(|error| anyhow::anyhow!(error))
        })
    }

    fn forward(
        &self,
        to: u64,
        rid: Rid,
        items: Vec<SnapshotItem>,
    ) -> BoxFuture<'_, Result<SnapshotBatchOutcome>> {
        Box::pin(async move {
            if crate::fault::p2p_denied(to) {
                bail!("the link to node {to} is cut (fault injection)");
            }
            let req_id = self
                .next_req
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let payload = constellation_net::Payload::SnapshotBatchRequest {
                requester: self.node_id,
                req_id,
                rid: rid_to_wire(rid),
                items,
            };
            match self
                .peers
                .request_to_node_timeout(to, &payload, forward_timeout())
                .await?
            {
                constellation_net::Payload::SnapshotBatchReply {
                    req_id: answered,
                    outcome,
                } if answered == req_id => Ok(outcome),
                other => bail!("node {to} answered a snapshot batch with {other:?}"),
            }
        })
    }

    fn catch_up(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let tail = self.ask("tail to head", |reply| {
                crate::sync::SyncRequest::TailToHead { reply }
            });
            let _ = tokio::time::timeout(Duration::from_secs(5), tail).await;
        })
    }

    fn nudge(&self) {
        let _ = self.sync_tx.send(crate::sync::SyncRequest::Nudge);
    }
}

#[cfg(test)]
mod tests {
    //! In-process multi-node tests: each node is the real authority core
    //! over a real `Meta` (`Standalone`, IO inline) on one shared
    //! in-memory bucket, with a real tree publisher; the "peer path" is a
    //! direct call into the holder's [`SnapshotBatcher::execute`].
    use super::*;
    use crate::authority_driver::Standalone;
    use crate::lease::LeaseView;
    use constellation_fs_core::cache::DiskCache;
    use constellation_meta::MetaStore;
    use constellation_mtree::{record, Hasher};
    use constellation_store_s3::{
        BlobStore, ChunkStore, CommitChain, LeaseMode, LeaseStore, NodeCache, PackStore,
    };
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Weak;

    type Driver = Arc<tokio::sync::Mutex<Standalone>>;

    /// Who can reach whom: node id → its executor.
    #[derive(Default)]
    struct Net {
        nodes: Mutex<HashMap<u64, Weak<SnapshotBatcher>>>,
    }

    struct TestHost {
        driver: Driver,
        view: Arc<LeaseView>,
        leases: LeaseStore,
        net: Arc<Net>,
        unreachable: Mutex<HashSet<u64>>,
        /// Forwards whose reply is lost after the holder executed them.
        lose_replies: AtomicU64,
        forwards: AtomicU64,
        acquires: AtomicU64,
        /// P2P is on (`BatchHost::peer_path`).
        p2p: AtomicBool,
        /// `drain_own` fails.
        drain_own_fails: AtomicBool,
        /// How many more admissions succeed (negative: unlimited).
        admits_left: std::sync::atomic::AtomicI64,
        /// `drain_own` / `forward` / `acquire`, in call order.
        calls: Mutex<Vec<&'static str>>,
    }

    impl TestHost {
        fn refresh(&self) {
            if let Ok(driver) = self.driver.try_lock() {
                driver.mirror(&self.view);
            }
        }
    }

    impl BatchHost for TestHost {
        fn holds(&self) -> bool {
            self.refresh();
            self.view.usable()
        }

        fn admit(&self) -> Option<Admission<'_>> {
            self.refresh();
            if self
                .admits_left
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    (n != 0).then(|| if n > 0 { n - 1 } else { n })
                })
                .is_err()
            {
                return None;
            }
            self.view.admit().map(|guard| Admission(Some(guard)))
        }

        fn peer_path(&self) -> bool {
            self.p2p.load(Ordering::Relaxed)
        }

        fn drain_own(&self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.calls.lock().unwrap().push("drain_own");
                if self.drain_own_fails.load(Ordering::Relaxed) {
                    bail!("S3 is unreachable");
                }
                Ok(())
            })
        }

        fn live_holder(&self) -> BoxFuture<'_, Result<Option<u64>>> {
            Box::pin(async move {
                let now = crate::prune::now_unix_ms() as i64;
                Ok(self
                    .leases
                    .get()
                    .await?
                    .map(|(lease, _)| lease)
                    .filter(|lease| !lease.is_claimable(now))
                    .map(|lease| lease.holder))
            })
        }

        fn acquire(&self) -> BoxFuture<'_, Result<bool>> {
            Box::pin(async move {
                self.acquires.fetch_add(1, Ordering::Relaxed);
                self.calls.lock().unwrap().push("acquire");
                let mut driver = self.driver.lock().await;
                let acquired = driver.acquire().await?;
                driver.mirror(&self.view);
                Ok(acquired)
            })
        }

        fn drain(&self, _ino: Ino) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                let mut driver = self.driver.lock().await;
                driver.sync().await?;
                driver.mirror(&self.view);
                Ok(())
            })
        }

        fn forward(
            &self,
            to: u64,
            rid: Rid,
            items: Vec<SnapshotItem>,
        ) -> BoxFuture<'_, Result<SnapshotBatchOutcome>> {
            Box::pin(async move {
                self.forwards.fetch_add(1, Ordering::Relaxed);
                self.calls.lock().unwrap().push("forward");
                if self.unreachable.lock().unwrap().contains(&to) {
                    bail!("node {to} is unreachable");
                }
                let holder = self
                    .net
                    .nodes
                    .lock()
                    .unwrap()
                    .get(&to)
                    .and_then(Weak::upgrade)
                    .with_context(|| format!("no address for node {to}"))?;
                let outcome = holder.execute(rid, &items).await;
                if self
                    .lose_replies
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                    .is_ok()
                {
                    bail!("the reply from node {to} was lost");
                }
                Ok(outcome)
            })
        }

        fn catch_up(&self) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                let _ = self.driver.lock().await.tail_to_head().await;
            })
        }

        fn nudge(&self) {}
    }

    struct Node {
        id: u64,
        meta: Arc<Meta>,
        driver: Driver,
        snapshots: Arc<SnapshotManager>,
        batcher: Arc<SnapshotBatcher>,
        host: Arc<TestHost>,
        _cache: tempfile::TempDir,
    }

    fn node(store: &Arc<InMemory>, net: &Arc<Net>, id: u64) -> Node {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let backend = store.clone() as Arc<dyn ObjectStore>;
        let dir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(NodeCache::new(
            PackStore::new(backend.clone()),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ));
        let mut publisher = crate::mtree_publish::TreePublisher::new(
            meta.clone(),
            cache.clone(),
            BlobStore::new(backend.clone(), Hasher::Plain),
            CommitChain::new(backend.clone()),
            record::config(),
            id,
            tokio::runtime::Handle::current(),
        );
        publisher.restore().unwrap();
        let driver: Driver = Arc::new(tokio::sync::Mutex::new(Standalone::with_publisher(
            meta.clone(),
            backend.clone(),
            id,
            LeaseMode::Cas,
            Some(publisher),
        )));
        let hook_driver = driver.clone();
        let hook: crate::snapshot::PublishHook = Arc::new(move || {
            let driver = hook_driver.clone();
            Box::pin(async move {
                // As the daemon's hook (its budget is 10 s too): a write
                // landing between the round and the publish is retried,
                // not failed.
                let mut tries = 0;
                loop {
                    match driver.lock().await.publish_commit().await {
                        Err(error)
                            if (format!("{error:#}")
                                .contains(crate::mtree_publish::SPECULATION_OUTSTANDING)
                                || format!("{error:#}")
                                    .contains(crate::mtree_publish::PUBLISH_DEFERRED))
                                && tries < 5000 =>
                        {
                            tries += 1;
                        }
                        other => return other,
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
        });
        let snapshots = Arc::new(
            SnapshotManager::new(
                meta.clone(),
                Arc::new(ChunkStore::new(backend.clone())),
                constellation_fs_core::DEFAULT_CHUNK_SIZE,
                id,
            )
            .with_tree(crate::snapshot::TreeAccess {
                nodes: cache,
                config: record::config(),
                blobs: BlobStore::new(backend.clone(), Hasher::Plain),
            })
            .with_publisher(hook),
        );
        let host = Arc::new(TestHost {
            driver: driver.clone(),
            view: Arc::new(LeaseView::default()),
            leases: LeaseStore::new(
                backend,
                constellation_store_s3::log::PARTITION,
                LeaseMode::Cas,
            ),
            net: net.clone(),
            unreachable: Mutex::new(HashSet::new()),
            lose_replies: AtomicU64::new(0),
            forwards: AtomicU64::new(0),
            acquires: AtomicU64::new(0),
            p2p: AtomicBool::new(true),
            drain_own_fails: AtomicBool::new(false),
            admits_left: std::sync::atomic::AtomicI64::new(-1),
            calls: Mutex::new(Vec::new()),
        });
        let batcher = Arc::new(SnapshotBatcher::new(
            id,
            meta.clone(),
            snapshots.clone(),
            host.clone(),
            crate::forward::ForwardState::new(1),
            false,
        ));
        net.nodes
            .lock()
            .unwrap()
            .insert(id, Arc::downgrade(&batcher));
        Node {
            id,
            meta,
            driver,
            snapshots,
            batcher,
            host,
            _cache: dir,
        }
    }

    impl Node {
        async fn acquire(&self) {
            let mut driver = self.driver.lock().await;
            assert!(driver.acquire().await.unwrap(), "node {} acquires", self.id);
            driver.mirror(&self.host.view);
        }

        async fn sync(&self) {
            let mut driver = self.driver.lock().await;
            driver.sync().await.unwrap();
            driver.mirror(&self.host.view);
        }

        async fn tail(&self) {
            self.driver.lock().await.tail_to_head().await.unwrap();
        }

        fn rows(&self) -> Vec<SnapshotRow> {
            self.meta.snapshots(None).unwrap()
        }
    }

    /// `(holder, epoch)` of the root lease as the bucket has it.
    async fn lease_of(store: &Arc<InMemory>) -> (u64, u64) {
        let (lease, _) = LeaseStore::new(
            store.clone() as Arc<dyn ObjectStore>,
            constellation_store_s3::log::PARTITION,
            LeaseMode::Cas,
        )
        .get()
        .await
        .unwrap()
        .expect("a lease object");
        (lease.holder, lease.epoch)
    }

    async fn snaps_objects(store: &Arc<InMemory>) -> usize {
        use futures::TryStreamExt;
        let prefix = object_store::path::Path::from("snaps");
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    }

    fn create(path: &str, name: &str, creator: u64) -> SnapshotItem {
        create_full(path, name, creator, None, None)
    }

    fn create_full(
        path: &str,
        name: &str,
        creator: u64,
        held_by: Option<&str>,
        skip_if_unchanged_since: Option<String>,
    ) -> SnapshotItem {
        SnapshotItem::Create {
            path: path.into(),
            name: name.into(),
            origin: 1,
            policy_ino: 0,
            creator,
            held: held_by.is_some(),
            held_by: held_by.map(str::to_string),
            skip_if_unchanged_since,
        }
    }

    fn created(result: &ItemResult) -> SnapshotRow {
        match result {
            ItemResult::Created { row, id, .. } => {
                assert_eq!(&row.id, id);
                row_from_wire(row.clone())
            }
            other => panic!("expected Created, got {other:?}"),
        }
    }

    /// The writer's sequence on `/data`: entry `w{k}` then the counter
    /// file's `user.counter = k`, for k = 1, 2, …
    fn write_step(meta: &Meta, data: Ino, counter: Ino, k: u64) {
        meta.create(data, &format!("w{k:06}"), 0o644, 0, 0).unwrap();
        meta.set_xattr(
            counter,
            "user.counter",
            k.to_string().as_bytes(),
            constellation_meta::SetXattrMode::Set,
        )
        .unwrap();
    }

    /// The writer's sequence as a snapshot froze it: `m` entries, which
    /// must be exactly `w1..=wm`, and the counter, which is `m` or (the
    /// snapshot fell between the two writes of step `m`) `m - 1`.
    async fn frozen_prefix(snapshots: &SnapshotManager, row: &SnapshotRow) -> u64 {
        let root = SnapshotRoot::parse(&row.root_hash).unwrap();
        let dir = snapshots.list_frozen(&root.object()).await.unwrap();
        let mut names: Vec<&str> = dir
            .entries
            .iter()
            .map(|e| e.name.as_str())
            .filter(|n| n.starts_with('w'))
            .collect();
        names.sort();
        let m = names.len() as u64;
        let want: Vec<String> = (1..=m).map(|k| format!("w{k:06}")).collect();
        assert_eq!(names, want, "snapshot {} is not a prefix", row.name);
        let counter = dir
            .entries
            .iter()
            .find(|e| e.name == "counter")
            .expect("the counter file");
        let value: u64 = counter
            .xattrs
            .iter()
            .find(|(k, _)| k == "user.counter")
            .map(|(_, v)| std::str::from_utf8(v).unwrap().parse().unwrap())
            .unwrap_or(0);
        assert!(
            value == m || value + 1 == m,
            "snapshot {}: counter {value} with {m} entries",
            row.name
        );
        m
    }

    /// Two nodes: B holds the root lease with `/data` and its counter.
    async fn holder_b() -> (Arc<InMemory>, Node, Node, Ino, Ino) {
        let store = Arc::new(InMemory::new());
        let net = Arc::new(Net::default());
        let a = node(&store, &net, 1);
        let b = node(&store, &net, 2);
        b.acquire().await;
        let data = b.meta.mkdir(1, "data", 0o755, 0, 0).unwrap();
        let counter = b.meta.create(data.ino, "counter", 0o644, 0, 0).unwrap();
        b.sync().await;
        a.tail().await;
        // Leak the net with the nodes: the weak references stay valid.
        std::mem::forget(net);
        (store, a, b, data.ino, counter.ino)
    }

    /// The plan's test: node B holds the lease and writes continuously;
    /// node A creates 10 snapshots. The lease never moves, every snapshot
    /// is a prefix of B's writes, and the prefixes only grow.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshots_from_a_non_holder_never_move_the_lease() {
        let (store, a, b, data, counter) = holder_b().await;
        let before = lease_of(&store).await;
        assert_eq!(before.0, 2);

        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(AtomicU64::new(0));
        let writer = {
            let (meta, driver, view, stop, written) = (
                b.meta.clone(),
                b.driver.clone(),
                b.host.view.clone(),
                stop.clone(),
                written.clone(),
            );
            tokio::spawn(async move {
                let mut k = 0;
                while !stop.load(Ordering::Relaxed) {
                    k += 1;
                    // Each step is journaled and shipped under B's
                    // driver, so it never lands between the round and
                    // the publish of a snapshot B is taking. A write that
                    // does land there makes `publish_now` refuse (unshipped
                    // work), and a holder writing faster than one
                    // round + publish starves its snapshots: pre-existing,
                    // recorded in PROGRESS ("Plan 32 M0a") for the
                    // scheduler.
                    {
                        let mut driver = driver.lock().await;
                        write_step(&meta, data, counter, k);
                        let _ = driver.sync().await;
                        driver.mirror(&view);
                    }
                    written.store(k, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        let mut prefixes = Vec::new();
        for i in 0..10 {
            while written.load(Ordering::Relaxed) < (i + 1) * 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let report = a
                .batcher
                .run(
                    a.batcher.next_rid(),
                    vec![create("/data", &format!("s{i}"), 1)],
                )
                .await
                .unwrap();
            assert_eq!(report.route, Route::Forwarded(2));
            let row = created(&report.results[0]);
            assert_eq!(row.creator, 1, "the requester is the creator");
            assert_eq!(row.origin, 1);
            prefixes.push(frozen_prefix(&b.snapshots, &row).await);
            assert_eq!(
                lease_of(&store).await,
                before,
                "snapshot {i} moved the lease"
            );
        }
        stop.store(true, Ordering::Relaxed);
        writer.await.unwrap();
        let total = written.load(Ordering::Relaxed);
        eprintln!(
            "lease (holder, epoch) before {before:?} after {:?}; prefixes {prefixes:?} of {total} writes",
            lease_of(&store).await
        );
        assert!(
            prefixes.windows(2).all(|w| w[0] <= w[1]),
            "prefixes went backwards: {prefixes:?}"
        );
        assert!(prefixes[0] >= 3 && *prefixes.last().unwrap() <= total);
        assert_eq!(lease_of(&store).await, before);
        assert_eq!(
            a.host.acquires.load(Ordering::Relaxed),
            0,
            "A never acquired"
        );
        assert_eq!(a.host.forwards.load(Ordering::Relaxed), 10);
        // A heard of all ten through the log, as B recorded them.
        b.sync().await;
        a.tail().await;
        assert_eq!(a.rows(), b.rows());
        assert_eq!(a.rows().len(), 10);
    }

    /// Deletes and holds from a non-holder execute at the holder too: the
    /// lease never moves, the owner rule and the held-refusal hold there,
    /// and both replicas converge on the same rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deletes_and_holds_from_a_non_holder_run_at_the_holder() {
        let (store, a, b, _, _) = holder_b().await;
        let before = lease_of(&store).await;
        let results = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![create("/data", "keep", 1), create("/data", "drop", 1)],
            )
            .await
            .unwrap();
        let keep = created(&results[0]);
        let drop = created(&results[1]);
        assert_eq!(
            SnapshotRoot::parse(&keep.root_hash).unwrap().seq,
            SnapshotRoot::parse(&drop.root_hash).unwrap().seq,
            "one publish serves the whole batch"
        );
        let results = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![
                    SnapshotItem::Hold {
                        id: keep.id.clone(),
                        held: true,
                        by: Some("user:ops".into()),
                        force: false,
                    },
                    // Held in this same batch, before the delete: refused.
                    SnapshotItem::Delete {
                        id: keep.id.clone(),
                        force: false,
                    },
                    SnapshotItem::Delete {
                        id: drop.id.clone(),
                        force: false,
                    },
                    SnapshotItem::Delete {
                        id: "no-such-id".into(),
                        force: false,
                    },
                    // Another owner cannot release it.
                    SnapshotItem::Hold {
                        id: keep.id.clone(),
                        held: false,
                        by: Some("user:other".into()),
                        force: false,
                    },
                ],
            )
            .await
            .unwrap();
        match &results[0] {
            ItemResult::HoldSet { row } => {
                assert!(row.held);
                assert_eq!(row.held_by.as_deref(), Some("user:ops"));
            }
            other => panic!("{other:?}"),
        }
        assert!(
            matches!(&results[1], ItemResult::Refused { reason } if reason.contains("is held by user:ops")),
            "{:?}",
            results[1]
        );
        assert_eq!(results[2], ItemResult::Deleted);
        assert_eq!(results[3], ItemResult::NotFound);
        assert!(
            matches!(&results[4], ItemResult::Refused { reason } if reason.contains("user:ops")),
            "{:?}",
            results[4]
        );
        assert_eq!(snaps_objects(&store).await, 1, "drop's object is gone");
        // The owner releases it, and then it deletes.
        let results = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![
                    SnapshotItem::Hold {
                        id: keep.id.clone(),
                        held: false,
                        by: Some("user:ops".into()),
                        force: false,
                    },
                    SnapshotItem::Delete {
                        id: keep.id.clone(),
                        force: false,
                    },
                ],
            )
            .await
            .unwrap();
        assert!(matches!(&results[0], ItemResult::HoldSet { row } if !row.held));
        assert_eq!(results[1], ItemResult::Deleted);
        assert_eq!(snaps_objects(&store).await, 0);
        // One more, held, so there is something to converge on.
        let results = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![create_full("/data", "pinned", 1, Some("csi:uid"), None)],
            )
            .await
            .unwrap();
        let pinned = created(&results[0]);
        assert!(pinned.held);
        assert_eq!(lease_of(&store).await, before, "the lease moved");
        assert_eq!(a.host.acquires.load(Ordering::Relaxed), 0);
        b.sync().await;
        a.tail().await;
        assert_eq!(a.rows(), b.rows());
        assert_eq!(b.rows(), vec![pinned]);
    }

    /// A duplicate batch (same rid) is answered from the holder's memory
    /// and creates nothing; a retry that crosses a holder change finds
    /// its own `snaps/` object and answers `AlreadyExists`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_retried_batch_is_exactly_once() {
        let (store, a, b, _, _) = holder_b().await;
        let rid = a.batcher.next_rid();
        let items = vec![create("/data", "once", 1)];
        let first = a.batcher.run(rid, items.clone()).await.unwrap();
        let row = created(&first.results[0]);
        let again = a.batcher.run(rid, items.clone()).await.unwrap();
        assert_eq!(
            again, first,
            "a duplicate is answered from the first execution"
        );
        assert_eq!(snaps_objects(&store).await, 1);
        assert_eq!(b.rows(), vec![row.clone()]);

        // The holder executes a new batch but its reply is lost; then it
        // releases, and the requester's retry (same rid) runs elsewhere.
        let rid = a.batcher.next_rid();
        let items = vec![create("/data", "lost-reply", 1)];
        a.host.lose_replies.store(1, Ordering::Relaxed);
        let error = a.batcher.run(rid, items.clone()).await.unwrap_err();
        assert!(format!("{error:#}").contains("lost"), "{error:#}");
        assert_eq!(snaps_objects(&store).await, 2, "it did execute at B");
        b.driver.lock().await.flush_release().await.unwrap();
        let retry = a.batcher.run(rid, items).await.unwrap();
        assert_eq!(retry.route, Route::Acquired, "nobody held it any more");
        assert!(
            matches!(&retry.results[0], ItemResult::AlreadyExists { id }
                if *id == constellation_store_s3::snapshot_id("/data", "lost-reply")),
            "{:?}",
            retry.results
        );
        assert_eq!(snaps_objects(&store).await, 2, "nothing new was created");
        // The row B recorded is what A has, once: no second row.
        let names: Vec<String> = a.rows().into_iter().map(|r| r.name).collect();
        assert_eq!(
            names.iter().filter(|n| *n == "lost-reply").count(),
            1,
            "{names:?}"
        );
    }

    /// No holder anywhere: the requester takes the free lease and runs the
    /// batch itself (today's behaviour).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_no_holder_the_requester_acquires_and_creates() {
        let store = Arc::new(InMemory::new());
        let net = Arc::new(Net::default());
        let a = node(&store, &net, 1);
        a.meta.mkdir(1, "data", 0o755, 0, 0).unwrap();
        let report = a
            .batcher
            .run(a.batcher.next_rid(), vec![create("/data", "solo", 1)])
            .await
            .unwrap();
        assert_eq!(report.route, Route::Acquired);
        created(&report.results[0]);
        assert_eq!(lease_of(&store).await.0, 1);
        assert_eq!(a.host.forwards.load(Ordering::Relaxed), 0);
        // Holding now, the next one runs locally.
        let report = a
            .batcher
            .run(a.batcher.next_rid(), vec![create("/data", "local", 1)])
            .await
            .unwrap();
        assert_eq!(report.route, Route::Local);
        assert_eq!(a.host.acquires.load(Ordering::Relaxed), 1);
        assert_eq!(snaps_objects(&store).await, 2);
    }

    /// A live holder that cannot be reached is an error for the caller —
    /// never a reason to take the lease from it — and nothing is created.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unreachable_holder_is_an_error_not_a_takeover() {
        let (store, a, b, _, _) = holder_b().await;
        let before = lease_of(&store).await;
        a.host.unreachable.lock().unwrap().insert(2);
        let error = a
            .batcher
            .submit(a.batcher.next_rid(), vec![create("/data", "nope", 1)])
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("unreachable"), "{error:#}");
        assert_eq!(lease_of(&store).await, before);
        assert_eq!(a.host.acquires.load(Ordering::Relaxed), 0);
        assert_eq!(snaps_objects(&store).await, 0);
        assert!(b.rows().is_empty() && a.rows().is_empty());
    }

    /// `skip_if_unchanged_since` naming the root the batch's publish
    /// returns skips that item (no object, no row); the batch's other
    /// creates still run, against the same commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unchanged_root_is_skipped() {
        let (store, a, b, data, _) = holder_b().await;
        // Nothing is dirty after this, so the batch's own publish returns
        // this same commit.
        let (seq, root) = b.snapshots.publish_commit().await.unwrap();
        let prev = SnapshotRoot {
            seq,
            root,
            ino: data,
        }
        .encode();
        // A root that differs: "changed", so it is taken.
        let other = SnapshotRoot {
            seq: seq.saturating_sub(1),
            root: NodeHash([9; 32]),
            ino: data,
        }
        .encode();
        let results = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![
                    create_full("/data", "unchanged", 1, None, Some(prev)),
                    create_full("/data", "changed", 1, None, Some(other)),
                    create_full("/data", "malformed", 1, None, Some("not a root".into())),
                ],
            )
            .await
            .unwrap();
        assert_eq!(results[0], ItemResult::Skipped);
        let row = created(&results[1]);
        assert_eq!(SnapshotRoot::parse(&row.root_hash).unwrap().root, root);
        assert!(
            matches!(&results[2], ItemResult::Refused { .. }),
            "{:?}",
            results[2]
        );
        assert_eq!(snaps_objects(&store).await, 1);
        let names: Vec<String> = b.rows().into_iter().map(|r| r.name).collect();
        assert_eq!(names, vec!["changed".to_string()]);
    }

    /// The executor refuses a batch outright when this node does not
    /// hold the root lease, before draining or publishing anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_non_holder_does_not_execute() {
        let (store, a, _b, _, _) = holder_b().await;
        let outcome = a
            .batcher
            .execute(a.batcher.next_rid(), &[create("/data", "x", 1)])
            .await;
        assert_eq!(outcome, SnapshotBatchOutcome::NotHolder);
        assert_eq!(snaps_objects(&store).await, 0);
    }

    /// P2P off (an S3-only cluster, `Peers::disabled()`): there is no
    /// forward to make, so a non-holder asks for the lease the way every
    /// write in that mode does, as snapshots did before batches — the
    /// first attempt registers in `wanted_by` and fails ("held by another
    /// node"), the holder hands over at its next round, and a retry runs
    /// the batch here. Without a peer path a forward could only ever fail
    /// ("no address for node N"), for as long as B held.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_p2p_off_a_non_holder_takes_the_lease_as_before() {
        let (store, a, b, _, _) = holder_b().await;
        a.host.p2p.store(false, Ordering::Relaxed);
        let items = vec![create("/data", "s3-only", 1)];
        let rid = a.batcher.next_rid();
        let error = a.batcher.run(rid, items.clone()).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("held by another node"),
            "{error:#}"
        );
        // (The registration is best effort and in flight when the
        // acquisition answers; let A's driver finish it.)
        a.tail().await;
        let (lease, _) = LeaseStore::new(
            store.clone() as Arc<dyn ObjectStore>,
            constellation_store_s3::log::PARTITION,
            LeaseMode::Cas,
        )
        .get()
        .await
        .unwrap()
        .unwrap();
        assert_eq!(lease.holder, 2, "B was not preempted");
        assert_eq!(lease.wanted_by, vec![1], "A asked for a handover");
        assert_eq!(snaps_objects(&store).await, 0);
        // The handover B makes when it sees `wanted_by` (its dwell and
        // grace are real time; the `lease-handover` scenario runs them).
        // B's first release loses its CAS to A's `wanted_by` edit (a
        // stale tag) and re-reads the lease; the next one releases.
        let mut tries = 0;
        while let Err(error) = b.driver.lock().await.flush_release().await {
            tries += 1;
            assert!(tries < 5, "B never released: {error:#}");
        }
        let report = a.batcher.run(rid, items).await.unwrap();
        assert_eq!(report.route, Route::Acquired);
        created(&report.results[0]);
        assert_eq!(a.host.forwards.load(Ordering::Relaxed), 0, "no forward");
        assert_eq!(*a.host.calls.lock().unwrap(), vec!["acquire", "acquire"]);
        assert_eq!(lease_of(&store).await.0, 1, "A took the lease");
        assert_eq!(snaps_objects(&store).await, 1);
    }

    /// Before a create is forwarded, the requester uploads its own
    /// pending chunks (what its `Barrier` did before batches); a drain
    /// that fails is the caller's error and nothing is forwarded. A batch
    /// without a create has nothing to freeze and does not drain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_forwarded_create_drains_the_requesters_own_writes_first() {
        let (store, a, b, _, _) = holder_b().await;
        let results = a
            .batcher
            .submit(a.batcher.next_rid(), vec![create("/data", "mine", 1)])
            .await
            .unwrap();
        let row = created(&results[0]);
        assert_eq!(*a.host.calls.lock().unwrap(), vec!["drain_own", "forward"]);

        a.host.calls.lock().unwrap().clear();
        a.host.drain_own_fails.store(true, Ordering::Relaxed);
        let error = a
            .batcher
            .submit(a.batcher.next_rid(), vec![create("/data", "blocked", 1)])
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("pending writes"), "{error:#}");
        assert_eq!(*a.host.calls.lock().unwrap(), vec!["drain_own"]);
        assert_eq!(snaps_objects(&store).await, 1, "nothing forwarded");

        a.host.calls.lock().unwrap().clear();
        let results = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![SnapshotItem::Delete {
                    id: row.id.clone(),
                    force: false,
                }],
            )
            .await
            .unwrap();
        assert_eq!(results, vec![ItemResult::Deleted]);
        assert_eq!(*a.host.calls.lock().unwrap(), vec!["forward"]);
        assert!(b.rows().is_empty());
    }

    /// An item refused because the executor lost the lease did not run:
    /// its batch is not kept by rid, so the retry under the same rid
    /// executes it instead of replaying the refusal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lease_loss_refusal_is_not_replayed_to_a_retry() {
        let (_store, _a, b, _, _) = holder_b().await;
        let results = b
            .batcher
            .submit(b.batcher.next_rid(), vec![create("/data", "h", 2)])
            .await
            .unwrap();
        let row = created(&results[0]);
        let rid = b.batcher.next_rid();
        let items = vec![SnapshotItem::Hold {
            id: row.id.clone(),
            held: true,
            by: Some("user:ops".into()),
            force: false,
        }];
        // The batch's admission check passes; the hold's own does not, as
        // if the lease went between the two.
        b.host.admits_left.store(1, Ordering::Relaxed);
        let first = b.batcher.run(rid, items.clone()).await.unwrap();
        assert_eq!(first.route, Route::Local);
        assert!(
            matches!(&first.results[0], ItemResult::Refused { reason } if reason.starts_with(LEASE_LOST)),
            "{:?}",
            first.results
        );
        assert!(!b.meta.snapshot_by_id(&row.id).unwrap().unwrap().held);
        b.host.admits_left.store(-1, Ordering::Relaxed);
        let retry = b.batcher.run(rid, items).await.unwrap();
        assert!(
            matches!(&retry.results[0], ItemResult::HoldSet { row } if row.held),
            "{:?}",
            retry.results
        );
        assert!(b.meta.snapshot_by_id(&row.id).unwrap().unwrap().held);
    }

    // ------------------------------------------------- skip-empty (§3.4)

    /// A single holder with `/a/b` (the policy root, holding file `f`)
    /// and its sibling `/a/c` (holding file `g`), and a first snapshot of
    /// `/a/b` taken through the batch: `prev`, the root every later
    /// create names as `skip_if_unchanged_since`.
    struct Skip {
        store: Arc<InMemory>,
        node: Node,
        a: Ino,
        b: Ino,
        c: Ino,
        f: Ino,
        g: Ino,
        prev: String,
        taken: u64,
    }

    impl Skip {
        async fn new() -> Skip {
            let store = Arc::new(InMemory::new());
            let net = Arc::new(Net::default());
            let node = node(&store, &net, 1);
            std::mem::forget(net);
            node.acquire().await;
            let a = node.meta.mkdir(1, "a", 0o755, 0, 0).unwrap().ino;
            let b = node.meta.mkdir(a, "b", 0o755, 0, 0).unwrap().ino;
            let c = node.meta.mkdir(a, "c", 0o755, 0, 0).unwrap().ino;
            let f = node.meta.create(b, "f", 0o644, 0, 0).unwrap().ino;
            let g = node.meta.create(c, "g", 0o644, 0, 0).unwrap().ino;
            let mut skip = Skip {
                store,
                node,
                a,
                b,
                c,
                f,
                g,
                prev: String::new(),
                taken: 0,
            };
            skip.rebase().await;
            skip
        }

        /// One create of `/a/b` naming `prev`, through the batch.
        async fn snap(&mut self, prev: Option<String>) -> ItemResult {
            self.taken += 1;
            let mut results = self
                .node
                .batcher
                .submit(
                    self.node.batcher.next_rid(),
                    vec![create_full(
                        "/a/b",
                        &format!("s{}", self.taken),
                        1,
                        None,
                        prev,
                    )],
                )
                .await
                .unwrap();
            assert_eq!(results.len(), 1);
            results.remove(0)
        }

        /// The next create, against `prev`.
        async fn again(&mut self) -> ItemResult {
            let prev = self.prev.clone();
            self.snap(Some(prev)).await
        }

        /// Take an unconditional snapshot and make it `prev`.
        async fn rebase(&mut self) {
            let row = created(&self.snap(None).await);
            self.prev = row.root_hash;
        }

        /// The next create must be taken, and becomes `prev`.
        async fn expect_created(&mut self, what: &str) {
            let rows = self.node.rows().len();
            let objects = snaps_objects(&self.store).await;
            let result = self.again().await;
            let ItemResult::Created { row, .. } = &result else {
                panic!("{what}: expected Created, got {result:?}");
            };
            self.prev = row.root_hash.clone();
            assert_eq!(self.node.rows().len(), rows + 1, "{what}: one more row");
            assert_eq!(snaps_objects(&self.store).await, objects + 1, "{what}");
        }

        /// The next create must be skipped, writing nothing.
        async fn expect_skipped(&mut self, what: &str) {
            let rows = self.node.rows().len();
            let objects = snaps_objects(&self.store).await;
            assert_eq!(self.again().await, ItemResult::Skipped, "{what}");
            assert_eq!(self.node.rows().len(), rows, "{what}: no row");
            assert_eq!(
                snaps_objects(&self.store).await,
                objects,
                "{what}: no object"
            );
        }

        /// `len` bytes of fresh content into `ino`.
        fn write(&self, ino: Ino, len: u64, tag: &str) {
            use constellation_fs_core::manifest::Manifest;
            use constellation_fs_core::ChunkHash;
            let chunks = [(0, ChunkHash::of(tag.as_bytes()))].into_iter().collect();
            let (manifest, _) = Manifest::from_sparse_chunks(
                constellation_fs_core::DEFAULT_CHUNK_SIZE,
                len,
                chunks,
                64,
                ChunkHash::of,
            );
            self.node
                .meta
                .set_manifest(ino, &manifest.encode(), len)
                .unwrap();
        }

        /// Publish now and return the commit's root (what the next
        /// batch's publish would return).
        async fn publish(&self) -> NodeHash {
            self.node.sync().await;
            self.node.snapshots.publish_commit().await.unwrap().1
        }

        /// The tree root moved since `prev` (so a skip that follows is
        /// the diff's verdict, not the equal-roots shortcut), and the
        /// diff check alone says unchanged.
        async fn expect_moved(&self) {
            let root = self.publish().await;
            assert_ne!(root, SnapshotRoot::parse(&self.prev).unwrap().root);
            assert_eq!(self.check(root, usize::MAX).await, Emptiness::Unchanged);
        }

        /// [`subtree_unchanged`] of `/a/b` from `prev` to `root`.
        async fn check(&self, root: NodeHash, budget: usize) -> Emptiness {
            let prev = SnapshotRoot::parse(&self.prev).unwrap();
            let access = self.node.snapshots.tree().unwrap().clone();
            let b = self.b;
            tokio::task::spawn_blocking(move || {
                let tree = access.tree().unwrap();
                subtree_unchanged(&tree, &prev, root, b, budget, &mut Membership::default())
            })
            .await
            .unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_idle_subtree_is_skipped() {
        let mut fx = Skip::new().await;
        fx.expect_skipped("idle").await;
        fx.expect_skipped("still idle").await;
        // The previous snapshot stays the reference: a later change is
        // still measured against it.
        fx.write(fx.f, 1, "x");
        fx.expect_created("a write after skips").await;
        fx.expect_skipped("idle again").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_one_byte_write_under_the_root_is_taken() {
        let mut fx = Skip::new().await;
        fx.write(fx.f, 1, "one byte");
        fx.expect_created("one-byte write").await;
        // Deeper: a file in a new subdirectory of the root.
        let deep = fx.node.meta.mkdir(fx.b, "deep", 0o755, 0, 0).unwrap().ino;
        fx.expect_created("mkdir under the root").await;
        let h = fx.node.meta.create(deep, "h", 0o644, 0, 0).unwrap().ino;
        fx.expect_created("create two levels down").await;
        fx.write(h, 1, "deep byte");
        fx.expect_created("a write two levels down").await;
    }

    /// A read with atime on bumps atime through plan 20's path (applied
    /// to the overlay, queued for shipping, never journaled): it never
    /// reaches the tree, so it is no diff key and the root is skipped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_atime_only_change_is_skipped() {
        let mut fx = Skip::new().await;
        let before = fx.node.meta.getattr(fx.f).unwrap().unwrap().atime_ns;
        let now = crate::prune::now_unix_ms() as i64 * 1_000_000 + 1;
        let bump = [(fx.f, now, now)];
        let (applied, _) = fx.node.meta.apply_atime(&bump).unwrap();
        fx.node.meta.queue_atime(&bump).unwrap();
        assert_eq!(applied, 1);
        let after = fx.node.meta.getattr(fx.f).unwrap().unwrap().atime_ns;
        assert!(after > before, "the atime moved ({before} → {after})");
        fx.expect_skipped("atime only").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_change_outside_the_subtree_only_is_skipped() {
        let mut fx = Skip::new().await;
        // Above the root: a new file at `/`, which also moves `/`'s
        // mtime; and a new entry in `/a`, the root's own parent.
        let x = fx.node.meta.create(1, "x", 0o644, 0, 0).unwrap().ino;
        fx.write(x, 4096, "outside");
        fx.node.meta.create(fx.a, "sibling", 0o644, 0, 0).unwrap();
        fx.node
            .meta
            .set_xattr(fx.a, "user.k", b"v", constellation_meta::SetXattrMode::Set)
            .unwrap();
        fx.expect_moved().await;
        fx.expect_skipped("outside only").await;
    }

    /// The nested case: the policy is on `/a/b`, the change is in `/a/c`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_change_in_a_sibling_subtree_is_skipped() {
        let mut fx = Skip::new().await;
        fx.write(fx.g, 1, "sibling");
        let deeper = fx.node.meta.mkdir(fx.c, "d", 0o755, 0, 0).unwrap().ino;
        fx.node.meta.create(deeper, "e", 0o644, 0, 0).unwrap();
        fx.expect_moved().await;
        fx.expect_skipped("change in /a/c").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rename_into_or_out_of_the_subtree_is_taken() {
        let mut fx = Skip::new().await;
        fx.node.meta.rename(fx.c, "g", fx.b, "g").unwrap();
        fx.expect_created("rename into").await;
        fx.node.meta.rename(fx.b, "f", fx.c, "f").unwrap();
        fx.expect_created("rename out").await;
        // A whole directory moving in, then out.
        let d = fx.node.meta.mkdir(fx.c, "d", 0o755, 0, 0).unwrap().ino;
        fx.node.meta.create(d, "inner", 0o644, 0, 0).unwrap();
        fx.expect_skipped("built outside").await;
        fx.node.meta.rename(fx.c, "d", fx.b, "d").unwrap();
        fx.expect_created("directory moved in").await;
        fx.node.meta.rename(fx.b, "d", fx.c, "d").unwrap();
        fx.expect_created("directory moved out").await;
        // A hard link from outside landing inside.
        fx.node.meta.link(fx.f, fx.b, "f-again").unwrap();
        fx.expect_created("hard link into").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_xattr_or_chmod_under_the_root_is_taken() {
        let mut fx = Skip::new().await;
        fx.node
            .meta
            .set_xattr(
                fx.f,
                "user.tag",
                b"1",
                constellation_meta::SetXattrMode::Set,
            )
            .unwrap();
        fx.expect_created("xattr").await;
        fx.node
            .meta
            .setattr(fx.f, Some(0o600), None, None, None, None, None)
            .unwrap();
        fx.expect_created("chmod of a file").await;
        // The root directory itself is in its subtree.
        fx.node
            .meta
            .setattr(fx.b, Some(0o700), None, None, None, None, None)
            .unwrap();
        fx.expect_created("chmod of the root").await;
    }

    /// Past the budget the check stops reading and says "changed", even
    /// when every key it would have read lies outside the subtree.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_diff_past_the_budget_counts_as_changed() {
        let fx = Skip::new().await;
        for k in 0..20 {
            fx.node
                .meta
                .create(fx.c, &format!("n{k}"), 0o644, 0, 0)
                .unwrap();
        }
        let root = fx.publish().await;
        assert_eq!(fx.check(root, 100_000).await, Emptiness::Unchanged);
        assert_eq!(fx.check(root, 3).await, Emptiness::Changed);
        assert_eq!(fx.check(root, 0).await, Emptiness::Changed);
        // A root equal to `prev` needs no diff, whatever the budget.
        let prev = SnapshotRoot::parse(&fx.prev).unwrap().root;
        assert_eq!(fx.check(prev, 0).await, Emptiness::Unchanged);
        assert_eq!(empty_check_keys(), 100_000);
    }

    /// `/a/b` removed and made again: same path, a new directory ino, so
    /// the previous snapshot is of a directory that no longer exists.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replaced_root_directory_is_taken() {
        let mut fx = Skip::new().await;
        fx.node.meta.unlink(fx.b, "f").unwrap();
        fx.node.meta.rmdir(fx.a, "b").unwrap();
        let b = fx.node.meta.mkdir(fx.a, "b", 0o755, 0, 0).unwrap().ino;
        assert_ne!(b, fx.b);
        fx.expect_created("replaced root").await;
        let row = SnapshotRoot::parse(&fx.prev).unwrap();
        assert_eq!(row.ino, b);
        fx.expect_skipped("the new root, idle").await;
    }
}
