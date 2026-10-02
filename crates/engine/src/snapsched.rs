//! Automatic snapshot schedules (plan 32), the engine side.
//!
//! The policy language and the retention rule are pure and live in
//! `constellation_meta::snapsched`. A policy is bound to a directory by
//! the `user.constellation.snapshots` xattr alone (Step 3.1): the View's
//! setxattr gate validates it on the way in, the control methods in
//! `control::snapsched` read and write it, and nothing else records which
//! directories are policy roots.
//!
//! This module holds [`SnapSchedStats`], the node's counters (Step 9), and
//! [`Scheduler`], the task that **creates** the snapshots a policy
//! describes (Steps 3.2–3.3). It never deletes one: expiry is M4.
//!
//! # The tick
//!
//! Every daemon runs a scheduler task ticking every
//! `CONSTELLATION_SNAPSCHED_TICK_MS`; one of them leads. A tick, in order:
//!
//! 1. **Inert without policies.** The policy roots come from the local
//!    replica (`Meta::snapshot_policy_roots`, the `xattr_by_name` index).
//!    With none, the tick does no S3 work at all — it does not even look
//!    at the lease — and a node that was leading releases its lease once.
//!    The feature costs nothing until an operator sets a policy (plan 32
//!    Goal).
//! 2. **Refusal gates**, the pruner's: the node departed, its continuation
//!    epoch refuses writes (frozen, or offline with no lease to write
//!    under), it is a read-only member (`refused_state`), or its replica
//!    trails the log tail by more than `CONSTELLATION_SNAPSCHED_MAX_LAG_S`
//!    (`refused_lag`). A refused tick creates nothing and does not renew:
//!    a leader that stays refused lets its lease lapse, and a healthy node
//!    takes over.
//! 3. **Sticky leadership.** The `_snapsched` [`SingletonLease`] — its own,
//!    not `_prune`'s, so a long prune walk never delays a snapshot — is
//!    kept across ticks: a leader renews it at the start of every tick and
//!    again immediately before its batch, and a [`Fenced`] renewal (someone
//!    else took it while this node was paused or partitioned) ends the
//!    leadership and the tick, before any batch. A non-leader tries to
//!    acquire; finding it held, it returns.
//! 4. **Due roots** (Step 3.3), from the local replica and the leader's
//!    clock: a root whose xattr parses (an unparseable one counts
//!    `unparseable_roots` and is skipped — fail closed), is not `paused`,
//!    whose directory still exists, and which has fewer than
//!    `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT` live auto snapshots (else
//!    `capped_roots`), is due when no auto snapshot of it, held or not, was
//!    created inside the current finest bucket (`retention::due`).
//!    **Catch-up, not backfill:** at most one snapshot per root per tick,
//!    so after downtime the root gets one snapshot now, named for the
//!    current bucket, and the missed buckets stay empty.
//! 5. **One [`SnapshotItem`] batch** for every due root, through the
//!    holder-side batch (`crate::snapshot_batch`): creation runs where the
//!    root lease is, and never moves it. Each item names the bucket
//!    (`auto-<UTC bucket start>`, [`retention::auto_name`]), the root's
//!    current path, `origin = auto`, `policy_ino`, this node as creator, and
//!    — with `skip-empty=yes` — the newest auto snapshot's root as
//!    `skip_if_unchanged_since`. `Created` counts `created`; `Skipped`
//!    counts `skipped_empty`; `AlreadyExists` is success (the name is the
//!    bucket, so a new leader retrying a bucket its predecessor took hits
//!    the `snaps/` create-if-absent: no bucket ever gets two snapshots);
//!    a refused item counts `create_failed`. A batch that fails as a whole
//!    creates nothing and the next tick tries again.
//! 6. **Audit**: one `snapsched/journal/` object per tick that created a
//!    snapshot or failed one (`constellation_store_s3::snapsched`); a
//!    tick whose every answer was "unchanged" or "already exists" changed
//!    nothing and writes none, so an idle skip-empty root costs no object
//!    per bucket.
//!
//! A bucket this leader settled without a new row it can see is
//! remembered (in memory, per root) so the next ticks of the same bucket
//! do not ask the holder to drain and publish again for nothing; the next
//! bucket asks afresh. `AlreadyExists` settles the bucket for good (the
//! snapshot exists; its row arrives with the next sync). `Skipped` settles
//! it only while the leader's replica stays where it was when the tick
//! began (its applied seq and journal position): once anything newer
//! reaches the replica, the root may have changed, and the next tick asks
//! again — so a write late in a bucket still gets that bucket's snapshot,
//! and in an idle cluster an idle root costs the holder one check per
//! bucket, not one per tick. The comparison is two local reads; the
//! position is cluster-wide, so while anything is written an idle root is
//! re-checked every tick.
//!
//! # Names and clocks
//!
//! The name is the bucket by the *leader's* clock; `created_unix_ms` is
//! the holder's, truthfully. A holder that executes late (or runs
//! behind) may record a snapshot in the previous bucket; the leader then
//! still sees the current bucket uncovered, asks again under the same
//! name, gets `AlreadyExists`, and settles it. Cosmetic, and bounded to
//! one bucket.
//!
//! "The root's current path" is what the leader's replica resolves the
//! policy root's inode to now (`Meta::ancestry`). A renamed root keeps its
//! stream: identity is `policy_ino`, and listing is rename-safe (§0.5);
//! only the new snapshots carry the new path. The holder resolves that
//! path again and refuses the item unless it still names `policy_ino`
//! (`mv /a /a.old; mkdir /a` not yet seen here must not put the new `/a`
//! into the old one's stream); the next tick asks with the path this
//! replica has caught up to. A root whose directory is gone is skipped
//! (its snapshots are orphaned and kept, Step 4.4).

use crate::singleton::{Fenced, HeldElsewhere, SingletonLease};
use crate::snapshot_batch::{ItemResult, SnapshotBatcher, SnapshotItem};
use constellation_control::proto::types::{
    SnapSchedReport, SnapSchedRootState, SnapSchedRunResult, SnapSchedRunRoot,
};
use constellation_fs_core::Ino;
use constellation_meta::snapsched::{retention, SnapFacts, SnapPolicy};
use constellation_meta::{Meta, SnapshotRow};
use constellation_store_s3::snapsched::{
    SnapSchedJournalEntry, SnapSchedJournalRoot, SnapSchedJournalSkip, SnapSchedJournalSnap,
};
use constellation_store_s3::{LeaseMode, StoreError};
use object_store::ObjectStore;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Scheduler and binding counters, modeled on `crate::prune::PruneStats`:
/// shared by the View's setxattr gate, the scheduler task (M3) and the
/// control plane, which copies them into `node.status`'s `snapsched`.
#[derive(Debug, Default)]
pub struct SnapSchedStats {
    /// Scheduler ticks run on this node.
    pub ticks: AtomicU64,
    /// Whether this node holds the `_snapsched` singleton lease.
    pub leader: AtomicBool,
    /// Policy roots seen by the last tick, and how many of them were
    /// paused, unparseable (skipped fail-closed), or capped
    /// (`CONSTELLATION_SNAPSCHED_MAX_PER_ROOT`).
    pub roots: AtomicU64,
    pub paused_roots: AtomicU64,
    pub unparseable_roots: AtomicU64,
    pub capped_roots: AtomicU64,
    /// Auto snapshots whose policy root no longer carries a parseable
    /// policy, as of the last tick: kept, never expired (Step 4.2).
    pub orphaned_snapshots: AtomicU64,
    pub created: AtomicU64,
    pub skipped_empty: AtomicU64,
    pub create_failed: AtomicU64,
    pub expired: AtomicU64,
    /// Victims dropped because the row changed under the expiry pass
    /// (held, deleted, re-owned) between evaluation and delete.
    pub skipped_reverify: AtomicU64,
    /// Victims kept by the grace window after a policy change (Step 4.3).
    pub skipped_grace: AtomicU64,
    pub budget_expired: AtomicU64,
    pub budget_stale: AtomicU64,
    /// Ticks refused for replica lag, and for node state (departed,
    /// frozen epoch, offline read-only, read-only member).
    pub refused_lag: AtomicU64,
    pub refused_state: AtomicU64,
    pub last_create_unix_ms: AtomicU64,
    /// The scheduler's last failure, as text.
    pub last_error: Mutex<Option<String>>,
    /// The last policy the setxattr gate refused: `(expression, byte
    /// offset, message)`, kept so an operator who got a bare `EINVAL`
    /// from `setfattr` can see why (plan 22's posture).
    pub last_parse_error: Mutex<Option<(String, usize, String)>>,
}

impl SnapSchedStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_parse_error(&self, expr: &str, offset: usize, msg: &str) {
        if let Ok(mut slot) = self.last_parse_error.lock() {
            *slot = Some((expr.to_string(), offset, msg.to_string()));
        }
    }

    pub fn record_error(&self, msg: impl Into<String>) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(msg.into());
        }
    }

    /// The counters as `node.status` reports them.
    pub fn status(&self) -> constellation_control::proto::types::SnapSchedStatus {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        constellation_control::proto::types::SnapSchedStatus {
            ticks: load(&self.ticks),
            leader: self.leader.load(Ordering::Relaxed),
            roots: load(&self.roots),
            paused_roots: load(&self.paused_roots),
            unparseable_roots: load(&self.unparseable_roots),
            capped_roots: load(&self.capped_roots),
            orphaned_snapshots: load(&self.orphaned_snapshots),
            created: load(&self.created),
            skipped_empty: load(&self.skipped_empty),
            create_failed: load(&self.create_failed),
            expired: load(&self.expired),
            skipped_reverify: load(&self.skipped_reverify),
            skipped_grace: load(&self.skipped_grace),
            budget_expired: load(&self.budget_expired),
            budget_stale: load(&self.budget_stale),
            refused_lag: load(&self.refused_lag),
            refused_state: load(&self.refused_state),
            last_create_unix_ms: load(&self.last_create_unix_ms),
            last_error: self.last_error.lock().ok().and_then(|g| g.clone()),
            last_parse_error: self.last_parse_error.lock().ok().and_then(|g| g.clone()),
        }
    }
}

fn inc(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

// --- configuration (CONSTELLATION_SNAPSCHED_*; plan 32 Step 10) -----------

/// The singleton lease's partition: `leases/_snapsched.json`.
pub const LEASE_NAME: &str = "_snapsched";

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// The scheduler's knobs, read once when the engine starts.
#[derive(Debug, Clone)]
pub struct SchedConfig {
    /// `CONSTELLATION_SNAPSCHED` (default on; `0`, `false` or `off` =
    /// never lead). A
    /// node with it off still serves batches as the root-lease holder and
    /// still reports `snapshot.sched.status`; it just never takes the
    /// `_snapsched` lease, so it never creates on its own.
    pub enabled: bool,
    /// `CONSTELLATION_SNAPSCHED_TICK_MS` (default 10 000): how often every
    /// node ticks. The finest tier a policy may have is 10 s, so the
    /// default places a snapshot within one tick of its bucket's start.
    pub tick: Duration,
    /// `CONSTELLATION_SNAPSCHED_MAX_LAG_S` (default 300): refuse to act
    /// from a replica that trails the log tail by more than this — a stale
    /// replica does not know the snapshots another leader just took.
    pub max_lag: Duration,
    /// `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT` (default 5000): a root with
    /// this many live auto snapshots (held ones included) gets no more,
    /// counts `capped_roots`, and shows an error. A guard against
    /// unbounded growth; `last=` and `*` tiers should never reach it.
    pub max_per_root: usize,
    /// The `_snapsched` lease's TTL: `CONSTELLATION_LEASE_TTL_MS`, as
    /// every lease (default 60 s), and never less than three ticks, so a
    /// leader renewing every tick cannot lapse between two of them.
    pub lease_ttl_ms: u64,
}

impl SchedConfig {
    pub fn from_env() -> SchedConfig {
        let tick = Duration::from_millis(env_u64("CONSTELLATION_SNAPSCHED_TICK_MS", 10_000).max(1));
        SchedConfig {
            enabled: !std::env::var("CONSTELLATION_SNAPSCHED").is_ok_and(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "off"
                )
            }),
            tick,
            max_lag: Duration::from_secs(env_u64("CONSTELLATION_SNAPSCHED_MAX_LAG_S", 300)),
            max_per_root: env_u64("CONSTELLATION_SNAPSCHED_MAX_PER_ROOT", 5000) as usize,
            lease_ttl_ms: constellation_store_s3::lease::lease_ttl_ms()
                .max(3 * tick.as_millis() as u64),
        }
    }
}

/// The scheduler's notion of "now" (Unix ms). The daemon's is the wall
/// clock; tests move it to cross buckets without waiting for them.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

pub fn wall_clock() -> Clock {
    Arc::new(constellation_store_s3::lease::now_unix_ms)
}

/// Everything the scheduler needs from the node.
pub struct SchedDeps {
    pub node_id: u64,
    /// The bucket: the `_snapsched` lease and the audit journal.
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<Meta>,
    pub batches: Arc<SnapshotBatcher>,
    pub lease_mode: LeaseMode,
    pub read_only_member: bool,
    pub departed: Arc<AtomicBool>,
    /// The continuation epoch's "writes refused" (frozen, or offline with
    /// no lease to write under).
    pub epoch_frozen: Option<Arc<AtomicBool>>,
    /// Unix ms of the sync task's last pass: the replica's freshness.
    pub last_sync_ms: Arc<AtomicU64>,
    pub stats: Arc<SnapSchedStats>,
    pub config: SchedConfig,
    pub clock: Clock,
}

// --- what the replica says ------------------------------------------------

/// One policy root as the replica has it now, and what a tick would do
/// with it.
#[derive(Debug, Clone)]
struct RootPlan {
    ino: Ino,
    path: Option<String>,
    expr: String,
    policy: Option<SnapPolicy>,
    paused: bool,
    auto_snapshots: usize,
    last_created: Option<i64>,
    capped: bool,
    /// Why the root is not armed (unparseable, gone, capped).
    error: Option<String>,
    /// Asked a snapshot of this tick.
    due: bool,
    next_due: Option<i64>,
    /// The current bucket's name.
    name: Option<String>,
    /// The newest auto snapshot's root, when the policy skips empty ones.
    skip_if_unchanged_since: Option<String>,
}

/// The replica's policy roots and the number of orphaned auto snapshots
/// (whose `policy_ino` carries no parseable policy), at `now_ms`. Local
/// reads only.
///
/// Cost: with any policy root, every node scans every snapshot row on
/// every tick (and on every `snapshot.sched.status`) — non-leaders too,
/// for their gauges. That is a local read, O(all snapshots), not
/// measured to matter at the per-root cap; a per-`policy_ino` index would
/// make it O(the roots' own rows) if it ever does. With no policy root it
/// is one index lookup and nothing else.
fn plan_roots(
    meta: &Meta,
    now_ms: i64,
    max_per_root: usize,
) -> anyhow::Result<(Vec<RootPlan>, u64)> {
    let roots = meta.snapshot_policy_roots()?;
    if roots.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let mut autos: HashMap<Ino, Vec<SnapshotRow>> = HashMap::new();
    for row in meta.snapshots(None)? {
        if retention::Origin::from_u8(row.origin).is_auto() {
            autos.entry(row.policy_ino).or_default().push(row);
        }
    }
    let mut plans = Vec::with_capacity(roots.len());
    for (ino, expr) in roots {
        let mut rows = autos.remove(&ino).unwrap_or_default();
        rows.sort_by(|a, b| (a.created_unix_ms, &a.id).cmp(&(b.created_unix_ms, &b.id)));
        let path = meta.ancestry(ino)?.map(|chain| {
            let names: Vec<&str> = chain.iter().skip(1).map(|(_, n)| n.as_str()).collect();
            format!("/{}", names.join("/"))
        });
        let mut plan = RootPlan {
            ino,
            path,
            expr: expr.clone(),
            policy: None,
            paused: false,
            auto_snapshots: rows.len(),
            last_created: rows.last().map(|r| r.created_unix_ms),
            capped: false,
            error: None,
            due: false,
            next_due: None,
            name: None,
            skip_if_unchanged_since: None,
        };
        let policy = match SnapPolicy::parse(&expr) {
            Ok(policy) => policy,
            Err(e) => {
                // Unparseable: its stream is orphaned (Step 4.2).
                autos.insert(ino, rows);
                plan.error = Some(format!(
                    "unparseable policy at byte {}: {}",
                    e.offset, e.msg
                ));
                plans.push(plan);
                continue;
            }
        };
        plan.paused = policy.paused;
        plan.name = retention::auto_name(&policy, now_ms);
        let facts: Vec<SnapFacts> = rows.iter().map(SnapFacts::from_row).collect();
        if plan.path.is_none() {
            plan.error = Some("the policy's directory is gone".to_string());
        } else if rows.len() >= max_per_root {
            plan.capped = true;
            plan.error = Some(format!(
                "{} live auto snapshots, at the cap (CONSTELLATION_SNAPSCHED_MAX_PER_ROOT = \
                 {max_per_root}): creating none",
                rows.len()
            ));
        } else if !policy.paused {
            plan.due = retention::due(&policy, ino, &facts, now_ms);
            plan.next_due =
                retention::current_bucket(&policy, now_ms)
                    .map(|(from, to)| if plan.due { from } else { to });
            if plan.due && policy.skip_empty {
                plan.skip_if_unchanged_since = rows.last().map(|r| r.root_hash.clone());
            }
        }
        plan.policy = Some(policy);
        plans.push(plan);
    }
    // What is left in `autos` belongs to no parseable root.
    let orphaned = autos.values().map(|rows| rows.len() as u64).sum();
    Ok((plans, orphaned))
}

// --- the scheduler --------------------------------------------------------

/// How a tick was asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Run {
    /// The ticker.
    Periodic,
    /// `snapshot.sched.run`.
    Manual { dry_run: bool },
}

#[derive(Default)]
struct SchedState {
    /// The `_snapsched` lease while this node leads.
    lease: Option<SingletonLease>,
    /// Per root, the bucket this leader already settled without a row it
    /// can see yet (see the module doc). Only roots of the last plan.
    settled: HashMap<Ino, Settled>,
}

/// A bucket the leader need not ask about again.
struct Settled {
    bucket: String,
    /// `None`: for good (created, or it already exists). `Some(at)`:
    /// skipped as unchanged, for as long as the leader's replica stays at
    /// `at` ([`replica_position`]).
    while_at: Option<(u64, u64)>,
}

impl Settled {
    fn covers(&self, plan: &RootPlan, position: Option<(u64, u64)>) -> bool {
        plan.name.as_ref() == Some(&self.bucket)
            && self
                .while_at
                .is_none_or(|at| position.is_some_and(|now| now == at))
    }
}

/// Where the leader's replica is: the log seq it has applied and the
/// next seq of its own journal. Either moves when the tree may have
/// changed under it — a follower applies a newer segment, a holder
/// journals a write. `None` when the replica cannot say (then nothing
/// skipped is assumed unchanged). Local reads only.
fn replica_position(meta: &Meta) -> Option<(u64, u64)> {
    Some((meta.applied_seq().ok()?, meta.journal_next_seq().ok()?))
}

/// The node's snapshot scheduler (see the module doc). One per engine;
/// its ticker is [`Scheduler::spawn`], `snapshot.sched.run` is
/// [`Scheduler::tick`] with [`Run::Manual`].
pub struct Scheduler {
    deps: SchedDeps,
    /// Held for a whole tick: the ticker and a manual run never interleave.
    state: tokio::sync::Mutex<SchedState>,
    /// The last creation failure per root, for `snapshot.sched.status`.
    errors: Mutex<BTreeMap<Ino, String>>,
}

impl Scheduler {
    pub fn new(deps: SchedDeps) -> Arc<Scheduler> {
        Arc::new(Scheduler {
            deps,
            state: tokio::sync::Mutex::new(SchedState::default()),
            errors: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn config(&self) -> &SchedConfig {
        &self.deps.config
    }

    /// The ticker: a tick every `tick`, after the node's background gate
    /// opens, until `stop`; then it resigns. A node with
    /// `CONSTELLATION_SNAPSCHED=0` ticks nothing at all.
    pub fn spawn(
        self: &Arc<Self>,
        rt: &tokio::runtime::Handle,
        stop: Arc<AtomicBool>,
        background: Option<Arc<crate::lifecycle::BackgroundGate>>,
    ) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        rt.spawn(async move {
            let mut timer = tokio::time::interval(this.deps.config.tick);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            timer.tick().await;
            loop {
                timer.tick().await;
                if let Some(gate) = &background {
                    gate.wait_active().await;
                }
                if stop.load(Ordering::Relaxed) {
                    this.resign().await;
                    break;
                }
                if !this.deps.config.enabled {
                    continue;
                }
                let report = this.tick(Run::Periodic).await;
                if let Some(error) = &report.error {
                    tracing::warn!(%error, "snapshot scheduler: the batch failed; retrying next tick");
                }
            }
        })
    }

    /// Give the lease back if this node leads (best effort; the TTL
    /// reclaims it otherwise). Skipped while a tick runs: that tick's
    /// own renewals are then the only writes, and the TTL ends them.
    pub async fn resign(&self) {
        if let Ok(mut state) = self.state.try_lock() {
            if let Some(lease) = state.lease.take() {
                lease.release().await;
            }
            self.deps.stats.leader.store(false, Ordering::Relaxed);
        }
    }

    fn refusal(&self) -> Option<(bool, String)> {
        let d = &self.deps;
        if d.departed.load(Ordering::Relaxed) {
            return Some((false, "node has departed the cluster".into()));
        }
        if d.epoch_frozen
            .as_ref()
            .is_some_and(|f| f.load(Ordering::Relaxed))
        {
            return Some((
                false,
                "the node's continuation epoch refuses writes (frozen, or offline)".into(),
            ));
        }
        if d.read_only_member {
            return Some((false, "node is a read-only member".into()));
        }
        let lag = Duration::from_millis(
            crate::prune::now_unix_ms().saturating_sub(d.last_sync_ms.load(Ordering::Relaxed)),
        );
        if lag > d.config.max_lag {
            return Some((
                true,
                format!(
                    "replica trails the log tail by {}s (limit {}s, \
                     CONSTELLATION_SNAPSCHED_MAX_LAG_S)",
                    lag.as_secs(),
                    d.config.max_lag.as_secs()
                ),
            ));
        }
        None
    }

    /// Renew the lease this node holds, or try to take it. `Ok(true)`:
    /// this node leads now. `Ok(false)`: another node does (or this one
    /// was just fenced). `Err`: the store failed; nothing is known.
    async fn lead(&self, state: &mut SchedState) -> anyhow::Result<bool> {
        let stats = &self.deps.stats;
        if let Some(lease) = state.lease.as_mut() {
            match lease.renew().await {
                Ok(()) => {
                    stats.leader.store(true, Ordering::Relaxed);
                    return Ok(true);
                }
                Err(error) if error.downcast_ref::<Fenced>().is_some() => {
                    tracing::info!("snapshot scheduler: fenced; another node leads now");
                    state.lease = None;
                    state.settled.clear();
                    stats.leader.store(false, Ordering::Relaxed);
                    return Ok(false);
                }
                // Still ours as far as anyone knows: keep it and retry
                // the renewal next tick (the TTL outlasts three ticks).
                Err(error) => return Err(error),
            }
        }
        match SingletonLease::acquire_with_ttl(
            self.deps.store.clone(),
            LEASE_NAME,
            self.deps.lease_mode,
            self.deps.config.lease_ttl_ms,
        )
        .await
        {
            Ok(lease) => {
                tracing::info!(epoch = lease.epoch(), "snapshot scheduler: leading");
                state.lease = Some(lease);
                state.settled.clear();
                stats.leader.store(true, Ordering::Relaxed);
                Ok(true)
            }
            // Held, or another node won the create/swap race for it
            // (the CAS conflict): someone else's turn, not a failure.
            Err(error)
                if error.downcast_ref::<HeldElsewhere>().is_some()
                    || matches!(
                        error.downcast_ref::<StoreError>(),
                        Some(StoreError::CasConflict)
                    ) =>
            {
                stats.leader.store(false, Ordering::Relaxed);
                Ok(false)
            }
            Err(error) => {
                stats.leader.store(false, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    fn set_gauges(&self, plans: &[RootPlan], orphaned: u64) {
        let stats = &self.deps.stats;
        let count = |f: &dyn Fn(&RootPlan) -> bool| plans.iter().filter(|p| f(p)).count() as u64;
        stats.roots.store(plans.len() as u64, Ordering::Relaxed);
        stats
            .paused_roots
            .store(count(&|p| p.paused), Ordering::Relaxed);
        stats
            .unparseable_roots
            .store(count(&|p| p.policy.is_none()), Ordering::Relaxed);
        stats
            .capped_roots
            .store(count(&|p| p.capped), Ordering::Relaxed);
        stats.orphaned_snapshots.store(orphaned, Ordering::Relaxed);
    }

    fn note_error(&self, ino: Option<Ino>, error: String) {
        if let Some(ino) = ino {
            self.errors.lock().unwrap().insert(ino, error.clone());
        }
        self.deps.stats.record_error(error);
    }

    /// One tick (see the module doc). Never fails: what went wrong is in
    /// the result (`refused`, `error`, per-root `error`), the counters and
    /// `last_error`.
    pub async fn tick(&self, run: Run) -> SnapSchedRunResult {
        let dry_run = matches!(run, Run::Manual { dry_run: true });
        let mut result = SnapSchedRunResult {
            dry_run,
            ..Default::default()
        };
        let mut state = self.state.lock().await;
        let stats = &self.deps.stats;
        inc(&stats.ticks, 1);
        if !self.deps.config.enabled {
            result.refused =
                Some("the scheduler is disabled on this node (CONSTELLATION_SNAPSCHED=0)".into());
            return result;
        }
        let now = (self.deps.clock)();
        // Read before the plan, so a change that lands while this tick
        // runs moves the replica past it and is asked about next tick.
        let position = replica_position(&self.deps.meta);
        let (plans, orphaned) =
            match plan_roots(&self.deps.meta, now, self.deps.config.max_per_root) {
                Ok(planned) => planned,
                Err(error) => {
                    let error = format!("reading the policy roots: {error:#}");
                    self.note_error(None, error.clone());
                    result.error = Some(error);
                    return result;
                }
            };
        self.set_gauges(&plans, orphaned);
        state
            .settled
            .retain(|ino, _| plans.iter().any(|p| p.ino == *ino));
        // 1. No policy anywhere: no S3 work at all.
        if plans.is_empty() {
            if let Some(lease) = state.lease.take() {
                tracing::info!("snapshot scheduler: no policy roots left; resigning");
                lease.release().await;
            }
            state.settled.clear();
            stats.leader.store(false, Ordering::Relaxed);
            result.leader = false;
            return result;
        }
        // 2. Refusal gates.
        if let Some((lag, why)) = self.refusal() {
            inc(
                if lag {
                    &stats.refused_lag
                } else {
                    &stats.refused_state
                },
                1,
            );
            tracing::debug!(reason = %why, "snapshot scheduler: tick refused");
            // The lease is kept, but no longer renewed: this node does
            // not lead now, and another takes over once it lapses. A
            // later healthy tick renews it if nobody has.
            stats.leader.store(false, Ordering::Relaxed);
            result.leader = false;
            result.refused = Some(why);
            return result;
        }
        let due: Vec<&RootPlan> = plans
            .iter()
            .filter(|p| p.due)
            .filter(|p| {
                !state
                    .settled
                    .get(&p.ino)
                    .is_some_and(|s| s.covers(p, position))
            })
            .collect();
        if dry_run {
            result.leader = state.lease.is_some();
            result.roots = due
                .iter()
                .map(|p| run_root(p, "would_create", None, None))
                .collect();
            return result;
        }
        // 3. Leadership, renewed at the start of the tick.
        match self.lead(&mut state).await {
            Ok(true) => result.leader = true,
            Ok(false) => {
                result.refused = Some(format!(
                    "another node leads the snapshot scheduler (the {LEASE_NAME} lease is held)"
                ));
                return result;
            }
            Err(error) => {
                let error = format!("the {LEASE_NAME} lease: {error:#}");
                self.note_error(None, error.clone());
                result.leader = state.lease.is_some();
                result.error = Some(error);
                return result;
            }
        }
        // A capped root is skipped, and says so (its ino in the message).
        for p in plans.iter().filter(|p| p.capped) {
            if let Some(error) = &p.error {
                self.note_error(Some(p.ino), format!("policy root {}: {error}", p.ino));
            }
        }
        // 4. Nothing due: done.
        if due.is_empty() {
            return result;
        }
        // 5. Renew immediately before the batch: a pause between the two
        // (a long plan, a stalled runtime) must not let two leaders
        // submit for the same bucket.
        if let Some(lease) = state.lease.as_mut() {
            if let Err(error) = lease.renew().await {
                if error.downcast_ref::<Fenced>().is_some() {
                    state.lease = None;
                    state.settled.clear();
                    stats.leader.store(false, Ordering::Relaxed);
                    result.leader = false;
                    result.refused = Some("fenced: another node took the scheduler lease".into());
                } else {
                    let error = format!("renewing the {LEASE_NAME} lease: {error:#}");
                    self.note_error(None, error.clone());
                    result.error = Some(error);
                }
                return result;
            }
        }
        let items: Vec<SnapshotItem> = due
            .iter()
            .map(|p| SnapshotItem::Create {
                path: p.path.clone().expect("a due root has a path"),
                name: p.name.clone().expect("a due root has a bucket name"),
                origin: retention::Origin::Auto.to_u8(),
                policy_ino: p.ino,
                creator: self.deps.node_id,
                held: false,
                held_by: None,
                skip_if_unchanged_since: p.skip_if_unchanged_since.clone(),
            })
            .collect();
        let batches = &self.deps.batches;
        let results = match batches.submit(batches.next_rid(), items).await {
            Ok(results) => results,
            Err(error) => {
                // Nothing was created; the next tick asks again (the
                // names make a retry of a half-done batch harmless).
                let error = format!("snapshot batch: {error:#}");
                inc(&stats.create_failed, due.len() as u64);
                for p in &due {
                    self.errors.lock().unwrap().insert(p.ino, error.clone());
                }
                stats.record_error(error.clone());
                result.error = Some(error);
                result.roots = due
                    .iter()
                    .map(|p| run_root(p, "failed", None, result.error.clone()))
                    .collect();
                return result;
            }
        };
        let mut audit = Vec::new();
        for (p, item) in due.iter().zip(results) {
            let name = p.name.clone().unwrap_or_default();
            let mut journal = SnapSchedJournalRoot {
                root_ino: p.ino,
                path: p.path.clone().unwrap_or_default(),
                policy: p.policy.as_ref().map(|q| q.to_string()).unwrap_or_default(),
                created: Vec::new(),
                skipped: Vec::new(),
                failed: Vec::new(),
                deleted: Vec::new(),
            };
            let outcome = match item {
                ItemResult::Created { id, row, .. } => {
                    inc(&stats.created, 1);
                    stats
                        .last_create_unix_ms
                        .fetch_max(row.created_unix_ms.max(0) as u64, Ordering::Relaxed);
                    state.settled.insert(
                        p.ino,
                        Settled {
                            bucket: name.clone(),
                            while_at: None,
                        },
                    );
                    self.errors.lock().unwrap().remove(&p.ino);
                    journal.created.push(SnapSchedJournalSnap {
                        id: id.clone(),
                        name: name.clone(),
                        created_unix_ms: row.created_unix_ms,
                    });
                    run_root(p, "created", Some(id), None)
                }
                ItemResult::Skipped => {
                    inc(&stats.skipped_empty, 1);
                    if let Some(at) = position {
                        state.settled.insert(
                            p.ino,
                            Settled {
                                bucket: name.clone(),
                                while_at: Some(at),
                            },
                        );
                    }
                    self.errors.lock().unwrap().remove(&p.ino);
                    journal.skipped.push(SnapSchedJournalSkip {
                        name: name.clone(),
                        reason: "unchanged since the newest snapshot".into(),
                    });
                    run_root(p, "skipped_empty", None, None)
                }
                ItemResult::AlreadyExists { id } => {
                    state.settled.insert(
                        p.ino,
                        Settled {
                            bucket: name.clone(),
                            while_at: None,
                        },
                    );
                    self.errors.lock().unwrap().remove(&p.ino);
                    journal.skipped.push(SnapSchedJournalSkip {
                        name: name.clone(),
                        reason: "already exists".into(),
                    });
                    run_root(p, "already_exists", Some(id), None)
                }
                other => {
                    let reason = match other {
                        ItemResult::Refused { reason } => reason,
                        other => format!("unexpected answer to a create: {other:?}"),
                    };
                    inc(&stats.create_failed, 1);
                    let error = format!("{}@{name}: {reason}", journal.path);
                    self.note_error(Some(p.ino), error.clone());
                    journal.failed.push(SnapSchedJournalSkip {
                        name: name.clone(),
                        reason,
                    });
                    run_root(p, "failed", None, Some(error))
                }
            };
            result.roots.push(outcome);
            audit.push(journal);
        }
        // 6. Audit, when this tick changed something or failed to.
        if !audit
            .iter()
            .any(|r| !r.created.is_empty() || !r.failed.is_empty())
        {
            return result;
        }
        let entry = SnapSchedJournalEntry {
            ts: now,
            node: self.deps.node_id,
            roots: audit,
        };
        if let Err(error) =
            constellation_store_s3::snapsched::append_journal(&self.deps.store, &entry).await
        {
            tracing::warn!(error = %error, "snapshot scheduler: writing the audit object failed");
        }
        result
    }

    /// `snapshot.sched.status`: the counters, the knobs, and every policy
    /// root as this node's replica has it now. Local reads only.
    pub fn report(&self) -> anyhow::Result<SnapSchedReport> {
        let now = (self.deps.clock)();
        let (plans, _) = plan_roots(&self.deps.meta, now, self.deps.config.max_per_root)?;
        let errors = self.errors.lock().unwrap().clone();
        Ok(SnapSchedReport {
            node_id: self.deps.node_id,
            enabled: self.deps.config.enabled,
            tick_ms: self.deps.config.tick.as_millis() as u64,
            max_per_root: self.deps.config.max_per_root as u64,
            stats: self.deps.stats.status(),
            roots: plans
                .into_iter()
                .map(|p| SnapSchedRootState {
                    ino: p.ino,
                    error: p.error.or_else(|| errors.get(&p.ino).cloned()),
                    path: p.path,
                    expr: p.expr,
                    canonical: p.policy.as_ref().map(|q| q.to_string()),
                    paused: p.paused,
                    due: p.due,
                    next_due_unix_ms: p.next_due,
                    bucket_name: p.name,
                    auto_snapshots: p.auto_snapshots as u64,
                    last_created_unix_ms: p.last_created,
                    capped: p.capped,
                })
                .collect(),
        })
    }
}

fn run_root(
    p: &RootPlan,
    outcome: &str,
    id: Option<String>,
    error: Option<String>,
) -> SnapSchedRunRoot {
    SnapSchedRunRoot {
        ino: p.ino,
        path: p.path.clone().unwrap_or_default(),
        name: p.name.clone().unwrap_or_default(),
        outcome: outcome.to_string(),
        id,
        error,
    }
}

#[cfg(test)]
mod tests {
    //! In-process multi-node tests over the snapshot batch's fixture
    //! (`snapshot_batch::tests`): real authority cores over real
    //! replicas on one in-memory bucket, a real tree publisher, and the
    //! peer path as a direct call into the holder's executor.
    //!
    //! The scheduler's clock is a test clock moved one bucket at a time;
    //! the holder still stamps `created_unix_ms` from the wall clock. So
    //! every bucket the test clock enters looks uncovered to a leader
    //! that does not remember settling it — exactly the "new leader
    //! retries the bucket its predecessor took" case the name CAS is for.
    use super::*;
    use crate::snapshot_batch::tests::{
        holder_b, lease_of, node, snaps_objects, write_step, Net, Node,
    };
    use constellation_meta::snapsched::SNAPSHOT_POLICY_XATTR;
    use constellation_meta::{MetaStore, SetXattrMode};
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStoreExt;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicI64, AtomicUsize};

    const SEC: i64 = 1000;

    fn config() -> SchedConfig {
        SchedConfig {
            enabled: true,
            tick: Duration::from_millis(50),
            max_lag: Duration::from_secs(300),
            max_per_root: 5000,
            lease_ttl_ms: 1000,
        }
    }

    /// A test clock 10 s past the start of a 10 s bucket in the future:
    /// nothing the holder stamps with the wall clock falls in its buckets.
    fn test_clock() -> Arc<AtomicI64> {
        let now = constellation_store_s3::lease::now_unix_ms();
        Arc::new(AtomicI64::new((now / (10 * SEC) + 6) * (10 * SEC) + SEC))
    }

    fn scheduler(
        n: &Node,
        store: Arc<dyn ObjectStore>,
        now: &Arc<AtomicI64>,
        config: SchedConfig,
    ) -> Arc<Scheduler> {
        let now = now.clone();
        Scheduler::new(SchedDeps {
            node_id: n.id,
            store,
            meta: n.meta.clone(),
            batches: n.batcher.clone(),
            lease_mode: LeaseMode::Cas,
            read_only_member: false,
            departed: Arc::new(AtomicBool::new(false)),
            epoch_frozen: Some(Arc::new(AtomicBool::new(false))),
            last_sync_ms: Arc::new(AtomicU64::new(crate::prune::now_unix_ms())),
            stats: SnapSchedStats::new(),
            config,
            clock: Arc::new(move || now.load(Ordering::Relaxed)),
        })
    }

    fn advance(now: &AtomicI64, by: i64) {
        now.fetch_add(by, Ordering::Relaxed);
    }

    /// Bind `policy` to `dir` at the holder `h` and let `others` see it.
    async fn bind(h: &Node, others: &[&Node], dir: Ino, policy: &str) {
        h.meta
            .set_xattr(
                dir,
                SNAPSHOT_POLICY_XATTR,
                policy.as_bytes(),
                SetXattrMode::Set,
            )
            .unwrap();
        h.sync().await;
        for o in others {
            o.tail().await;
        }
    }

    fn autos(n: &Node) -> Vec<SnapshotRow> {
        n.rows().into_iter().filter(|r| r.origin == 1).collect()
    }

    fn names(rows: &[SnapshotRow]) -> Vec<String> {
        let mut names: Vec<String> = rows.iter().map(|r| r.name.clone()).collect();
        names.sort();
        names
    }

    fn outcomes(r: &SnapSchedRunResult) -> Vec<&str> {
        r.roots.iter().map(|x| x.outcome.as_str()).collect()
    }

    async fn objects(store: &Arc<InMemory>, prefix: &str) -> usize {
        store
            .list(Some(&ObjPath::from(prefix)))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    }

    /// Whether `leases/_snapsched.json` exists.
    async fn lease_object(store: &Arc<InMemory>) -> bool {
        constellation_store_s3::LeaseStore::new(
            store.clone() as Arc<dyn ObjectStore>,
            LEASE_NAME,
            LeaseMode::Cas,
        )
        .get()
        .await
        .unwrap()
        .is_some()
    }

    /// A single node holding the root lease, with `/data` and a policy.
    async fn solo(policy: &str) -> (Arc<InMemory>, Node, Ino) {
        let store = Arc::new(InMemory::new());
        let net = Arc::new(Net::default());
        let a = node(&store, &net, 1);
        std::mem::forget(net);
        a.acquire().await;
        let data = a.meta.mkdir(1, "data", 0o755, 0, 0).unwrap().ino;
        a.meta.create(data, "f", 0o644, 0, 0).unwrap();
        a.sync().await;
        bind(&a, &[], data, policy).await;
        (store, a, data)
    }

    /// Plan 32 Step 11: the scheduler on A while B holds the root lease and
    /// writes. A snapshot appears every bucket — one per bucket, however
    /// often A ticks — every one is created at B through the batch, the
    /// root lease's holder and epoch never change, and both replicas list
    /// the same set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_scheduler_on_a_snapshots_bs_writes_without_moving_the_lease() {
        let (store, a, b, data, counter) = holder_b().await;
        bind(&b, &[&a], data, "10s:1m 1m:4m").await;
        let before = lease_of(&store).await;
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
                    {
                        let mut driver = driver.lock().await;
                        write_step(&meta, data, counter, k);
                        let _ = driver.sync().await;
                        driver.mirror(&view);
                    }
                    written.store(k, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
        };
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        let mut expected = BTreeSet::new();
        for i in 0..6 {
            // Something to snapshot (skip-empty is on).
            let seen = written.load(Ordering::Relaxed);
            while written.load(Ordering::Relaxed) < seen + 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let report = sched.tick(Run::Periodic).await;
            assert!(report.leader, "{report:?}");
            assert_eq!(outcomes(&report), ["created"], "bucket {i}: {report:?}");
            assert_eq!(report.roots[0].ino, data);
            expected.insert(report.roots[0].name.clone());
            // Ticking again inside the same bucket asks for nothing.
            let again = sched.tick(Run::Periodic).await;
            assert!(again.roots.is_empty(), "bucket {i} twice: {again:?}");
            assert_eq!(lease_of(&store).await, before, "bucket {i} moved the lease");
            advance(&now, 10 * SEC);
        }
        stop.store(true, Ordering::Relaxed);
        writer.await.unwrap();
        assert_eq!(lease_of(&store).await, before);
        assert_eq!(
            a.host.acquires.load(Ordering::Relaxed),
            0,
            "A never acquired"
        );
        assert_eq!(a.host.forwards.load(Ordering::Relaxed), 6);
        b.sync().await;
        a.tail().await;
        assert_eq!(a.rows(), b.rows(), "both replicas list the same set");
        let rows = autos(&a);
        assert_eq!(names(&rows), expected.into_iter().collect::<Vec<_>>());
        for row in &rows {
            assert_eq!((row.origin, row.policy_ino, row.creator), (1, data, 1));
            assert_eq!(row.path, "/data");
            assert!(!row.held);
        }
        let stats = sched.deps.stats.status();
        assert_eq!((stats.created, stats.ticks, stats.roots), (6, 12, 1));
        assert!(stats.leader && stats.last_create_unix_ms > 0);
        assert_eq!(
            objects(&store, "snapsched/journal").await,
            6,
            "one audit per tick that created"
        );
    }

    /// Plan 32 Step 11: two schedulers race for `_snapsched` and exactly one
    /// leads. The leader stops dead (its task dropped, the lease not
    /// released); the other takes over within one lease TTL and one tick,
    /// retries the bucket its predecessor already took — `AlreadyExists`,
    /// success — and no bucket ever has two snapshots.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_leader_and_a_takeover_never_doubles_a_bucket() {
        let (store, a, b, data, _) = holder_b().await;
        bind(&b, &[&a], data, "10s:1m; skip-empty=no").await;
        let now = test_clock();
        // A TTL long against a tick, so a slow first batch on a loaded
        // host does not hand the lease over before the test looks.
        let cfg = SchedConfig {
            lease_ttl_ms: 3000,
            ..config()
        };
        let (ttl, tick) = (Duration::from_millis(cfg.lease_ttl_ms), cfg.tick);
        let sa = scheduler(&a, store.clone(), &now, cfg.clone());
        let sb = scheduler(&b, store.clone(), &now, cfg);
        let stop = Arc::new(AtomicBool::new(false));
        let rt = tokio::runtime::Handle::current();
        let ta = sa.spawn(&rt, stop.clone(), None);
        let tb = sb.spawn(&rt, stop.clone(), None);
        let created = |s: &Scheduler| s.deps.stats.created.load(Ordering::Relaxed);
        let leads = |s: &Scheduler| s.deps.stats.leader.load(Ordering::Relaxed);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while created(&sa) + created(&sb) == 0 || leads(&sa) == leads(&sb) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "no single leader created"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Both keep ticking inside the same bucket: still one leader, still
        // one snapshot.
        tokio::time::sleep(tick * 10).await;
        assert!(leads(&sa) != leads(&sb), "exactly one leads");
        assert_eq!(
            created(&sa) + created(&sb),
            1,
            "one snapshot for the bucket"
        );
        let (leader, follower, task) = if leads(&sa) {
            (sa.clone(), sb.clone(), ta)
        } else {
            (sb.clone(), sa.clone(), tb)
        };
        let before = (created(&leader), created(&follower));
        // The leader dies: no release, its lease runs out.
        task.abort();
        let _ = task.await;
        let died = tokio::time::Instant::now();
        while !leads(&follower) {
            assert!(
                died.elapsed() < ttl + tick + Duration::from_secs(10),
                "no takeover"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let took = died.elapsed();
        eprintln!("takeover after {took:?} (lease TTL {ttl:?}, tick {tick:?})");
        assert!(
            took <= ttl + tick + Duration::from_secs(1),
            "takeover took {took:?}"
        );
        // Its first ticks retry the bucket the dead leader took: the name
        // CAS answers `AlreadyExists`, and nothing new is created.
        tokio::time::sleep(tick * 6).await;
        assert_eq!(created(&follower), before.1, "the bucket was taken twice");
        // The next bucket is the new leader's.
        advance(&now, 10 * SEC);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while created(&follower) == before.1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the new leader created nothing"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(tick * 6).await;
        stop.store(true, Ordering::Relaxed);
        b.sync().await;
        a.tail().await;
        let rows = autos(&a);
        let unique: BTreeSet<String> = rows.iter().map(|r| r.name.clone()).collect();
        assert_eq!(rows.len(), 2, "{:?}", names(&rows));
        assert_eq!(
            unique.len(),
            2,
            "a bucket has two snapshots: {:?}",
            names(&rows)
        );
        assert_eq!(snaps_objects(&store).await, 2);
        assert_eq!(a.rows(), b.rows());
        assert_eq!(
            created(&leader),
            before.0,
            "the dead leader created nothing more"
        );
        assert_eq!(created(&follower), before.1 + 1);
    }

    /// Catch-up, not backfill: after five buckets without a tick, one tick
    /// takes exactly one snapshot, named for the current bucket.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn downtime_gets_one_catch_up_snapshot_not_a_burst() {
        let (store, a, data) = solo("10s:1m 1m:10m; skip-empty=no").await;
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        let first = sched.tick(Run::Periodic).await;
        assert_eq!(outcomes(&first), ["created"]);
        advance(&now, 50 * SEC);
        let expected = retention::auto_name(
            &SnapPolicy::parse("10s:1m").unwrap(),
            now.load(Ordering::Relaxed),
        )
        .unwrap();
        let caught_up = sched.tick(Run::Periodic).await;
        assert_eq!(outcomes(&caught_up), ["created"]);
        assert_eq!(caught_up.roots[0].name, expected);
        assert!(sched.tick(Run::Periodic).await.roots.is_empty());
        let rows = autos(&a);
        assert_eq!(rows.len(), 2, "{:?}", names(&rows));
        assert!(rows.iter().all(|r| r.policy_ino == data));
    }

    /// Paused and unparseable roots create nothing (an unparseable xattr
    /// injected past the setxattr gate is inert, fail closed), and a node
    /// with `CONSTELLATION_SNAPSCHED=0` never leads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn paused_unparseable_and_disabled_create_nothing() {
        let (store, a, data) = solo("10s:1m; paused").await;
        let other = a.meta.mkdir(1, "other", 0o755, 0, 0).unwrap().ino;
        bind(&a, &[], other, "every now and then").await;
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        for _ in 0..3 {
            let report = sched.tick(Run::Periodic).await;
            assert!(report.roots.is_empty(), "{report:?}");
            advance(&now, 10 * SEC);
        }
        let stats = sched.deps.stats.status();
        assert_eq!(
            (
                stats.roots,
                stats.paused_roots,
                stats.unparseable_roots,
                stats.created
            ),
            (2, 1, 1, 0)
        );
        let report = sched.report().unwrap();
        let by_ino: HashMap<u64, &SnapSchedRootState> =
            report.roots.iter().map(|r| (r.ino, r)).collect();
        assert!(by_ino[&data].paused && !by_ino[&data].due);
        assert_eq!(by_ino[&data].next_due_unix_ms, None);
        assert!(by_ino[&other].canonical.is_none());
        assert!(by_ino[&other]
            .error
            .as_deref()
            .unwrap()
            .contains("unparseable"));
        assert!(autos(&a).is_empty());
        assert_eq!(snaps_objects(&store).await, 0);

        // Resumed, a disabled node still never leads or creates.
        bind(&a, &[], data, "10s:1m").await;
        let disabled = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                enabled: false,
                ..config()
            },
        );
        sched.resign().await;
        assert!(
            !lease_object(&store).await || {
                // The released lease object stays; it names nobody live.
                let leases = constellation_store_s3::LeaseStore::new(
                    store.clone() as Arc<dyn ObjectStore>,
                    LEASE_NAME,
                    LeaseMode::Cas,
                );
                leases.get().await.unwrap().unwrap().0.released
            }
        );
        let report = disabled.tick(Run::Periodic).await;
        assert!(report.refused.unwrap().contains("disabled"));
        assert!(!report.leader && !disabled.deps.stats.leader.load(Ordering::Relaxed));
        let report = disabled.tick(Run::Manual { dry_run: false }).await;
        assert!(report.refused.is_some() && report.roots.is_empty());
        assert!(autos(&a).is_empty());
    }

    /// The refusal gates: a lagging replica, a departed node, a frozen
    /// epoch and a read-only member create nothing, take no lease, and
    /// count `refused_lag` / `refused_state`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn refused_ticks_create_nothing_and_count() {
        let (store, a, _) = solo("10s:1m; skip-empty=no").await;
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        sched.deps.last_sync_ms.store(0, Ordering::Relaxed);
        let report = sched.tick(Run::Periodic).await;
        assert!(report.refused.unwrap().contains("trails the log"));
        sched
            .deps
            .last_sync_ms
            .store(crate::prune::now_unix_ms(), Ordering::Relaxed);
        sched.deps.departed.store(true, Ordering::Relaxed);
        assert!(sched.tick(Run::Periodic).await.refused.is_some());
        sched.deps.departed.store(false, Ordering::Relaxed);
        let frozen = sched.deps.epoch_frozen.clone().unwrap();
        frozen.store(true, Ordering::Relaxed);
        assert!(sched
            .tick(Run::Manual { dry_run: false })
            .await
            .refused
            .is_some());
        frozen.store(false, Ordering::Relaxed);
        let read_only = Scheduler::new(SchedDeps {
            read_only_member: true,
            ..deps_of(&a, store.clone(), &now)
        });
        assert!(read_only
            .tick(Run::Periodic)
            .await
            .refused
            .unwrap()
            .contains("read-only"));
        let stats = sched.deps.stats.status();
        assert_eq!((stats.refused_lag, stats.refused_state), (1, 2));
        assert_eq!(read_only.deps.stats.status().refused_state, 1);
        assert!(autos(&a).is_empty());
        assert!(!lease_object(&store).await, "a refused tick takes no lease");
        // The gates down again, the same scheduler creates.
        assert_eq!(outcomes(&sched.tick(Run::Periodic).await), ["created"]);
        assert!(sched.deps.stats.leader.load(Ordering::Relaxed));
        // A leader refused stops renewing, so it does not report leading;
        // healthy again, it renews the lease nobody took.
        sched.deps.departed.store(true, Ordering::Relaxed);
        let refused = sched.tick(Run::Periodic).await;
        assert!(refused.refused.is_some() && !refused.leader);
        assert!(!sched.deps.stats.leader.load(Ordering::Relaxed));
        sched.deps.departed.store(false, Ordering::Relaxed);
        assert!(sched.tick(Run::Periodic).await.leader);
        assert!(sched.deps.stats.leader.load(Ordering::Relaxed));
    }

    fn deps_of(n: &Node, store: Arc<dyn ObjectStore>, now: &Arc<AtomicI64>) -> SchedDeps {
        let now = now.clone();
        SchedDeps {
            node_id: n.id,
            store,
            meta: n.meta.clone(),
            batches: n.batcher.clone(),
            lease_mode: LeaseMode::Cas,
            read_only_member: false,
            departed: Arc::new(AtomicBool::new(false)),
            epoch_frozen: None,
            last_sync_ms: Arc::new(AtomicU64::new(crate::prune::now_unix_ms())),
            stats: SnapSchedStats::new(),
            config: config(),
            clock: Arc::new(move || now.load(Ordering::Relaxed)),
        }
    }

    /// The cap: a root with `MAX_PER_ROOT` live auto snapshots gets no
    /// more, counts `capped_roots`, and says so in `last_error` and in its
    /// status row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_root_at_the_cap_creates_nothing() {
        let (store, a, data) = solo("10s:1h; skip-empty=no").await;
        let now = test_clock();
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                max_per_root: 2,
                ..config()
            },
        );
        for _ in 0..2 {
            assert_eq!(outcomes(&sched.tick(Run::Periodic).await), ["created"]);
            advance(&now, 10 * SEC);
        }
        let report = sched.tick(Run::Periodic).await;
        assert!(report.roots.is_empty(), "{report:?}");
        let stats = sched.deps.stats.status();
        assert_eq!((stats.capped_roots, stats.created), (1, 2));
        let error = stats.last_error.expect("a capped root sets last_error");
        assert!(
            error.contains("MAX_PER_ROOT") && error.contains(&data.to_string()),
            "{error}"
        );
        let row = &sched.report().unwrap().roots[0];
        assert_eq!(row.ino, data);
        assert!(row.capped && !row.due);
        assert!(row.error.as_deref().unwrap().contains("MAX_PER_ROOT"));
        assert_eq!(autos(&a).len(), 2);
    }

    /// Skip-empty through the scheduler: an idle tree gets no new
    /// snapshot, and an idle bucket is asked about once, not every tick;
    /// a one-byte change later in the *same* bucket gets that bucket's
    /// snapshot, exactly one. Ticks that only skipped write no audit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_idle_root_is_skipped_and_a_change_is_taken() {
        let (store, a, data) = solo("10s:1h").await;
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        assert_eq!(outcomes(&sched.tick(Run::Periodic).await), ["created"]);
        // The daemon's sync task ships the new row right away.
        a.sync().await;
        advance(&now, 10 * SEC);
        assert_eq!(
            outcomes(&sched.tick(Run::Periodic).await),
            ["skipped_empty"]
        );
        for _ in 0..3 {
            let again = sched.tick(Run::Periodic).await;
            assert!(again.roots.is_empty(), "an idle bucket re-asked: {again:?}");
        }
        // Written later in the same bucket: the replica moved past the
        // check, so the bucket is asked again and taken.
        a.meta
            .set_xattr(data, "user.k", b"1", SetXattrMode::Set)
            .unwrap();
        a.sync().await;
        let taken = sched.tick(Run::Periodic).await;
        assert_eq!(outcomes(&taken), ["created"], "{taken:?}");
        a.sync().await;
        assert!(sched.tick(Run::Periodic).await.roots.is_empty());
        let stats = sched.deps.stats.status();
        assert_eq!((stats.created, stats.skipped_empty), (2, 1));
        assert_eq!(autos(&a).len(), 2);
        // The audit names what each tick that changed something did.
        let mut entries = Vec::new();
        let keys: Vec<ObjPath> = store
            .list(Some(&constellation_store_s3::snapsched::journal_prefix()))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        for key in keys {
            let body = store.get(&key).await.unwrap().bytes().await.unwrap();
            entries.push(serde_json::from_slice::<SnapSchedJournalEntry>(&body).unwrap());
        }
        entries.sort_by_key(|e| e.ts);
        let shape: Vec<(usize, usize)> = entries
            .iter()
            .map(|e| (e.roots[0].created.len(), e.roots[0].skipped.len()))
            .collect();
        assert_eq!(shape, [(1, 0), (1, 0)]);
        assert!(entries.iter().all(|e| e.roots[0].root_ino == data
            && e.roots[0].policy == "10s:1h"
            && e.roots[0].deleted.is_empty()));
    }

    /// Plan 32's identity is `policy_ino`. `mv /data /data.old; mkdir
    /// /data` at the holder, while the leader's replica still has the
    /// policy root at `/data`: the leader asks for `/data`, the holder
    /// resolves it to the new directory and refuses the item, and nothing
    /// foreign enters the root's stream. Once the leader's replica sees the
    /// rename, the next tick takes the root's bucket at `/data.old`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replaced_root_is_refused_until_the_leader_sees_the_rename() {
        let (store, a, b, data, _) = holder_b().await;
        bind(&b, &[&a], data, "10s:1m; skip-empty=no").await;
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        b.meta.rename_noreplace(1, "data", 1, "data.old").unwrap();
        let fresh = b.meta.mkdir(1, "data", 0o755, 0, 0).unwrap().ino;
        assert_ne!(fresh, data);
        b.sync().await;
        let refused = sched.tick(Run::Periodic).await;
        assert_eq!(outcomes(&refused), ["failed"], "{refused:?}");
        assert_eq!(refused.roots[0].path, "/data", "A's replica lags");
        assert!(
            refused.roots[0]
                .error
                .as_deref()
                .unwrap()
                .contains("not the policy root"),
            "{refused:?}"
        );
        assert!(autos(&b).is_empty());
        assert_eq!(snaps_objects(&store).await, 0, "no object either");
        assert_eq!(sched.deps.stats.status().create_failed, 1);
        // The leader's replica catches up: same bucket, the real root.
        a.tail().await;
        let taken = sched.tick(Run::Periodic).await;
        assert_eq!(outcomes(&taken), ["created"], "{taken:?}");
        assert_eq!(taken.roots[0].path, "/data.old");
        assert_eq!(taken.roots[0].name, refused.roots[0].name);
        b.sync().await;
        a.tail().await;
        let rows = autos(&a);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].policy_ino, rows[0].path.as_str()),
            (data, "/data.old")
        );
        assert_eq!(a.rows(), b.rows());
        assert!(sched.report().unwrap().roots.iter().all(|r| r.ino != fresh));
    }

    /// `retention::due` answering "covered": rows the holder stamped in
    /// the bucket the leader's clock is in. A new leader — taking over a
    /// dead one, with no memory of what it settled — sees the replicated
    /// row and submits nothing at all: no forward, no item. On the holder
    /// itself as well.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_covered_bucket_submits_no_batch_after_a_takeover() {
        let (store, a, b, data, _) = holder_b().await;
        bind(&b, &[&a], data, "1h:1d; skip-empty=no").await;
        // The wall clock, the one the holder stamps rows with.
        let now = Arc::new(AtomicI64::new(constellation_store_s3::lease::now_unix_ms()));
        let first = scheduler(&a, store.clone(), &now, config());
        assert_eq!(outcomes(&first.tick(Run::Periodic).await), ["created"]);
        b.sync().await;
        a.tail().await;
        let row = autos(&a).pop().unwrap();
        // Inside the bucket the holder stamped, whatever the leader named.
        now.store(row.created_unix_ms + 1, Ordering::Relaxed);
        let forwards = a.host.forwards.load(Ordering::Relaxed);
        // The leader dies holding the lease (no release); a fresh one on
        // the same replica takes over once it lapses.
        drop(first);
        let second = scheduler(&a, store.clone(), &now, config());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let report = loop {
            let report = second.tick(Run::Periodic).await;
            assert!(report.roots.is_empty(), "{report:?}");
            assert!(report.error.is_none(), "{report:?}");
            if report.leader {
                break report;
            }
            assert!(tokio::time::Instant::now() < deadline, "no takeover");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(report.refused.is_none(), "{report:?}");
        assert_eq!(
            a.host.forwards.load(Ordering::Relaxed),
            forwards,
            "the new leader submitted a batch for a covered bucket"
        );
        let status = second.report().unwrap();
        assert!(!status.roots[0].due);
        assert!(status.roots[0].next_due_unix_ms.unwrap() > row.created_unix_ms);
        // The holder as the new leader: the same answer.
        second.resign().await;
        let on_b = scheduler(&b, store.clone(), &now, config());
        let report = on_b.tick(Run::Periodic).await;
        assert!(report.leader && report.roots.is_empty(), "{report:?}");
        assert_eq!(autos(&b).len(), 1);
        assert_eq!(snaps_objects(&store).await, 1);
    }

    /// A store that, the first time a `_snapsched` lease is created, lets
    /// another node's create land first: the acquire loses the race.
    #[derive(Debug)]
    struct Racing {
        inner: Arc<InMemory>,
        raced: AtomicBool,
    }

    impl std::fmt::Display for Racing {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Racing({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Racing {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if *location == constellation_store_s3::layout::lease(LEASE_NAME)
                && matches!(opts.mode, object_store::PutMode::Create)
                && !self.raced.swap(true, Ordering::SeqCst)
            {
                let body: Vec<u8> = payload.iter().flat_map(|c| c.iter().copied()).collect();
                let mut theirs: constellation_store_s3::lease::Lease =
                    serde_json::from_slice(&body).unwrap();
                theirs.holder ^= 1;
                self.inner
                    .put_opts(
                        location,
                        serde_json::to_vec(&theirs).unwrap().into(),
                        object_store::PutMode::Create.into(),
                    )
                    .await?;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Losing the race to create `_snapsched` is another node's turn, not
    /// a store failure: no `error`, no `last_error`, nothing created.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn losing_the_lease_race_is_not_an_error() {
        let (store, a, _) = solo("10s:1m; skip-empty=no").await;
        let racing = Arc::new(Racing {
            inner: store.clone(),
            raced: AtomicBool::new(false),
        });
        let now = test_clock();
        let sched = scheduler(&a, racing.clone(), &now, config());
        let report = sched.tick(Run::Periodic).await;
        assert!(racing.raced.load(Ordering::SeqCst), "the race ran");
        assert!(report.error.is_none(), "{report:?}");
        assert!(report.refused.unwrap().contains("another node leads"));
        assert!(!report.leader && report.roots.is_empty());
        let stats = sched.deps.stats.status();
        assert!(stats.last_error.is_none(), "{:?}", stats.last_error);
        assert!(!stats.leader);
        assert!(autos(&a).is_empty());
    }

    /// A dry run reports what would be created and changes nothing: no
    /// lease, no snapshot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dry_run_takes_nothing() {
        let (store, a, data) = solo("10s:1m").await;
        let now = test_clock();
        let sched = scheduler(&a, store.clone(), &now, config());
        let report = sched.tick(Run::Manual { dry_run: true }).await;
        assert_eq!(outcomes(&report), ["would_create"]);
        assert_eq!(
            (report.roots[0].ino, report.roots[0].path.as_str()),
            (data, "/data")
        );
        assert!(!report.leader && !lease_object(&store).await);
        assert!(autos(&a).is_empty());
        let status = sched.report().unwrap();
        assert!(status.roots[0].due);
        assert_eq!(
            status.roots[0].bucket_name.as_deref(),
            Some(report.roots[0].name.as_str())
        );
    }

    /// A store that counts every request.
    #[derive(Debug)]
    struct Counting {
        inner: Arc<InMemory>,
        requests: AtomicUsize,
    }

    impl std::fmt::Display for Counting {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Counting({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The feature is inert by default (plan 32 Goal): with no policy root
    /// a tick makes no request at all — no lease object, nothing. A leader
    /// whose last policy is removed gives its lease back once and is quiet
    /// from then on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_no_policy_roots_a_tick_makes_no_request() {
        let store = Arc::new(InMemory::new());
        let net = Arc::new(Net::default());
        let a = node(&store, &net, 1);
        std::mem::forget(net);
        a.acquire().await;
        let data = a.meta.mkdir(1, "data", 0o755, 0, 0).unwrap().ino;
        a.sync().await;
        let counting = Arc::new(Counting {
            inner: store.clone(),
            requests: AtomicUsize::new(0),
        });
        let now = test_clock();
        let sched = scheduler(&a, counting.clone(), &now, config());
        for _ in 0..5 {
            let report = sched.tick(Run::Periodic).await;
            assert!(report.roots.is_empty() && report.refused.is_none() && !report.leader);
            advance(&now, 10 * SEC);
        }
        assert_eq!(counting.requests.load(Ordering::SeqCst), 0, "no request");
        assert!(!lease_object(&store).await, "no lease object");
        assert_eq!(sched.deps.stats.status().ticks, 5);

        // A policy arrives: the tick leads and creates.
        bind(&a, &[], data, "10s:1m; skip-empty=no").await;
        assert_eq!(outcomes(&sched.tick(Run::Periodic).await), ["created"]);
        assert!(counting.requests.load(Ordering::SeqCst) > 0);
        // It goes away again: one release, then silence.
        a.meta.remove_xattr(data, SNAPSHOT_POLICY_XATTR).unwrap();
        a.sync().await;
        counting.requests.store(0, Ordering::SeqCst);
        sched.tick(Run::Periodic).await;
        assert_eq!(counting.requests.load(Ordering::SeqCst), 1, "the release");
        assert!(!sched.deps.stats.leader.load(Ordering::Relaxed));
        counting.requests.store(0, Ordering::SeqCst);
        for _ in 0..3 {
            advance(&now, 10 * SEC);
            sched.tick(Run::Periodic).await;
        }
        assert_eq!(counting.requests.load(Ordering::SeqCst), 0);
        // The snapshot it took stays: removing a policy deletes nothing.
        assert_eq!(autos(&a).len(), 1);
    }
}
