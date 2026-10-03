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
//! describes (Steps 3.2–3.3) and then **expires** the ones it no longer
//! keeps (Step 4; the algorithm, its grace state and its never-delete
//! rules are `crate::snapexpire`'s, the only code that deletes snapshots
//! automatically).
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
//! 6. **Expiry** (Step 4, `crate::snapexpire`), when the creation step
//!    did not fail as a whole and this node still leads: for every armed
//!    root (parseable, not paused, its directory present) whose last run
//!    is at least `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` old, record the
//!    grace state (`snapsched/state.json`, CAS), then delete what
//!    retention and the grace windows expire — renewing the lease and
//!    re-reading every victim before each delete batch.
//! 7. **Audit**: one `snapsched/journal/` object per tick that created,
//!    failed or deleted a snapshot (`constellation_store_s3::snapsched`); a
//!    tick whose every answer was "unchanged" or "already exists" changed
//!    nothing and writes none, so an idle skip-empty root costs no object
//!    per bucket.
//! 8. **An unreachable holder.** Snapshot batches have no S3 path (they
//!    run at the root-lease holder, reached over P2P). A leader whose
//!    batches cannot reach the holder for
//!    `CONSTELLATION_SNAPSCHED_RESIGN_AFTER` ticks in a row resigns and
//!    stays out for one lease TTL, so a node that can reach it — the
//!    holder itself always can — leads instead ([`Scheduler`]'s
//!    `track_holder`).
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
use crate::snapexpire;
use crate::snapshot_batch::{HolderUnreachable, ItemResult, SnapshotBatcher, SnapshotItem};
use constellation_control::proto::types::{
    SnapSchedReport, SnapSchedRootState, SnapSchedRunResult, SnapSchedRunRoot,
};
use constellation_fs_core::Ino;
use constellation_meta::snapsched::{retention, SnapFacts, SnapPolicy};
use constellation_meta::{Meta, SnapshotRow};
use constellation_store_s3::snapsched::{
    self as snapsched_store, SnapSchedJournalEntry, SnapSchedJournalRoot, SnapSchedJournalSkip,
    SnapSchedJournalSnap, SnapSchedState,
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
    /// `CONSTELLATION_SNAPSCHED_GRACE_S` (default 86 400): after a root's
    /// policy changes (or a root is first seen), expiry deletes only what
    /// the old and the new policy both expire, for this long (Step 4.3).
    pub grace: Duration,
    /// `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` (default 60): the least
    /// time between two expiry runs of one root.
    pub expire_every: Duration,
    /// `CONSTELLATION_SNAPSCHED_MAX_DELETES` (default 500): the most
    /// snapshots one root's expiry run deletes; the rest wait for the
    /// next run.
    pub max_deletes: usize,
    /// `CONSTELLATION_SNAPSCHED_EXPIRE_BATCH` (default 32, at most
    /// `MAX_SNAPSHOT_DELETES_PER_BATCH`): victims per delete batch, each
    /// batch renewed and re-read immediately before it is sent
    /// (`crate::snapexpire`). `1` is plan §4.1's per-victim loop.
    pub expire_batch: usize,
    /// `CONSTELLATION_SNAPSCHED_RESIGN_AFTER` (default 3; 0 = never): a
    /// leader whose batches cannot reach the root-lease holder for this
    /// many ticks in a row resigns, so a node that can takes over.
    pub resign_after: u32,
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
            grace: Duration::from_secs(env_u64("CONSTELLATION_SNAPSCHED_GRACE_S", 86_400)),
            expire_every: Duration::from_secs(env_u64(
                "CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S",
                60,
            )),
            max_deletes: env_u64("CONSTELLATION_SNAPSCHED_MAX_DELETES", 500) as usize,
            expire_batch: (env_u64("CONSTELLATION_SNAPSCHED_EXPIRE_BATCH", 32) as usize)
                .clamp(1, constellation_net::MAX_SNAPSHOT_DELETES_PER_BATCH),
            resign_after: env_u64("CONSTELLATION_SNAPSCHED_RESIGN_AFTER", 3) as u32,
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
    /// Per root, when (scheduler clock) its last expiry run began.
    last_expiry: HashMap<Ino, i64>,
    /// Ticks in a row whose batch could not reach the root-lease holder.
    unreachable_ticks: u32,
    /// After resigning for that: no acquire before this instant.
    backoff_until: Option<tokio::time::Instant>,
}

/// Whether a tick's batches reached the root-lease holder.
#[derive(Debug, Default, Clone, Copy)]
struct Reach {
    reached: bool,
    unreachable: bool,
}

/// How long a read grace state serves displays before it is read again.
const GRACE_CACHE: Duration = Duration::from_secs(30);
/// How long a display waits for that read when nothing was read yet (an
/// old copy is served at once while a background read refreshes it).
const GRACE_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// An expiry run found no scheduler lease after `lead()` had taken it:
/// unreachable, but the deletion path never stops without saying why.
const NO_LEASE: &str = "internal: an expiry run without the scheduler lease; nothing expires";

type GraceCache = Arc<Mutex<Option<(tokio::time::Instant, SnapSchedState)>>>;

fn put_grace_cache(cache: &GraceCache, grace: &SnapSchedState) {
    *cache.lock().unwrap() = Some((tokio::time::Instant::now(), grace.clone()));
}

/// A root an expiry run may delete from: its policy parses, it is not
/// paused, and its directory is still there (a deleted root's snapshots
/// are orphaned and kept, Step 4.4).
fn expirable(p: &RootPlan) -> bool {
    p.policy.is_some() && !p.paused && p.path.is_some()
}

/// The audit record of one root in this tick, before anything happened.
fn journal_root(p: &RootPlan) -> SnapSchedJournalRoot {
    SnapSchedJournalRoot {
        root_ino: p.ino,
        path: p.path.clone().unwrap_or_default(),
        policy: p.policy.as_ref().map(|q| q.to_string()).unwrap_or_default(),
        created: Vec::new(),
        skipped: Vec::new(),
        failed: Vec::new(),
        deleted: Vec::new(),
    }
}

/// A run result row for one snapshot an expiry run handled.
fn expiry_root(
    p: &RootPlan,
    row: &SnapshotRow,
    outcome: &str,
    error: Option<String>,
) -> SnapSchedRunRoot {
    SnapSchedRunRoot {
        ino: p.ino,
        path: p.path.clone().unwrap_or_default(),
        name: row.name.clone(),
        outcome: outcome.to_string(),
        id: Some(row.id.clone()),
        error,
    }
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
    /// The grace state as last read, and when (for displays).
    grace_cache: GraceCache,
    /// A background refresh of `grace_cache` is under way.
    grace_refreshing: Arc<AtomicBool>,
    /// Test seam: called before each delete batch.
    #[cfg(test)]
    before_delete: Mutex<Option<snapexpire::BeforeBatch>>,
}

impl Scheduler {
    pub fn new(deps: SchedDeps) -> Arc<Scheduler> {
        Arc::new(Scheduler {
            deps,
            state: tokio::sync::Mutex::new(SchedState::default()),
            errors: Mutex::new(BTreeMap::new()),
            grace_cache: Arc::new(Mutex::new(None)),
            grace_refreshing: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            before_delete: Mutex::new(None),
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

    /// A leader's tick that failed as a whole (the store unreachable for
    /// its lease or its batch): every due root's snapshot was not taken,
    /// so each counts `create_failed` and carries the error; the next tick
    /// asks again.
    fn fail_due(&self, due: &[&RootPlan], error: String, result: &mut SnapSchedRunResult) {
        inc(&self.deps.stats.create_failed, due.len() as u64);
        for p in due {
            self.errors.lock().unwrap().insert(p.ino, error.clone());
        }
        self.deps.stats.record_error(error.clone());
        result.roots = due
            .iter()
            .map(|p| run_root(p, "failed", None, Some(error.clone())))
            .collect();
        result.error = Some(error);
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
        state
            .last_expiry
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
            // What an expiry run would delete now, by this replica and the
            // grace state as last read, projected (`grace_view`).
            match self.grace_view().await {
                Some(grace) => {
                    let grace_ms = self.grace_ms();
                    for p in plans.iter().filter(|p| expirable(p)) {
                        let policy = p.policy.as_ref().expect("an expirable root parses");
                        let Ok(rows) = snapexpire::root_rows(&self.deps.meta, p.ino) else {
                            continue;
                        };
                        let facts: Vec<SnapFacts> = rows.iter().map(SnapFacts::from_row).collect();
                        let g = snapexpire::graced(
                            policy,
                            p.ino,
                            &facts,
                            grace.roots.get(&p.ino),
                            now,
                            grace_ms,
                        );
                        for (row, v) in rows.iter().zip(&g.verdicts) {
                            if !v.keep {
                                result.roots.push(expiry_root(p, row, "would_expire", None));
                            }
                        }
                    }
                }
                None => {
                    result.error = Some(
                        "the grace state (snapsched/state.json) could not be read: \
                         what would expire is unknown"
                            .into(),
                    );
                }
            }
            return result;
        }
        // A leader that resigned for an unreachable root-lease holder
        // stays out for a lease TTL, so a node that can reach it takes
        // over (see `track_holder`).
        if state.lease.is_none()
            && state
                .backoff_until
                .is_some_and(|until| tokio::time::Instant::now() < until)
        {
            result.refused = Some(
                "this node resigned the snapshot scheduler: it could not reach the \
                 root-lease holder; another node may lead meanwhile"
                    .into(),
            );
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
                result.leader = state.lease.is_some();
                if result.leader {
                    // The leader could not reach the store: its due
                    // snapshots are not taken, and count as failed (an
                    // outage's signature). A node that does not lead
                    // does not know it would have, and counts nothing.
                    self.fail_due(&due, error, &mut result);
                } else {
                    self.note_error(None, error.clone());
                    result.error = Some(error);
                }
                return result;
            }
        }
        // A capped root is skipped, and says so (its ino in the message).
        for p in plans.iter().filter(|p| p.capped) {
            if let Some(error) = &p.error {
                self.note_error(Some(p.ino), format!("policy root {}: {error}", p.ino));
            }
        }
        let mut audit = BTreeMap::new();
        let mut reach = Reach::default();
        // 4–5. Creation.
        if self
            .create(
                &mut state,
                &due,
                position,
                &mut result,
                &mut audit,
                &mut reach,
            )
            .await
        {
            // 6. Expiry, after creation, while still leading.
            self.expire(&mut state, &plans, now, &mut result, &mut audit, &mut reach)
                .await;
        }
        // 7. Audit, when this tick changed something or failed to.
        self.write_audit(now, audit).await;
        self.track_holder(&mut state, reach, &mut result).await;
        result
    }

    /// Steps 4–5 of the tick: one batch creating every due root's
    /// snapshot. `false` when the tick must stop here (fenced, or the
    /// store or the batch failed as a whole).
    async fn create(
        &self,
        state: &mut SchedState,
        due: &[&RootPlan],
        position: Option<(u64, u64)>,
        result: &mut SnapSchedRunResult,
        audit: &mut BTreeMap<Ino, SnapSchedJournalRoot>,
        reach: &mut Reach,
    ) -> bool {
        let stats = &self.deps.stats;
        // 4. Nothing due: done.
        if due.is_empty() {
            return true;
        }
        // 5. Renew immediately before the batch: a pause between the two
        // (a long plan, a stalled runtime) must not let two leaders
        // submit for the same bucket.
        if let Some(lease) = state.lease.as_mut() {
            if let Err(error) = lease.renew().await {
                if error.downcast_ref::<Fenced>().is_some() {
                    self.fenced(state, result);
                } else {
                    let error = format!("renewing the {LEASE_NAME} lease: {error:#}");
                    self.fail_due(due, error, result);
                }
                return false;
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
            Ok(results) => {
                reach.reached = true;
                results
            }
            Err(error) => {
                reach.unreachable |= error.downcast_ref::<HolderUnreachable>().is_some();
                // Nothing was created; the next tick asks again (the
                // names make a retry of a half-done batch harmless).
                self.fail_due(due, format!("snapshot batch: {error:#}"), result);
                return false;
            }
        };
        for (p, item) in due.iter().zip(results) {
            let name = p.name.clone().unwrap_or_default();
            let journal = audit.entry(p.ino).or_insert_with(|| journal_root(p));
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
                        reason: None,
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
        }
        true
    }

    /// The renewal was refused: another node took the lease. Leadership
    /// ends here, before anything else is sent.
    fn fenced(&self, state: &mut SchedState, result: &mut SnapSchedRunResult) {
        tracing::info!("snapshot scheduler: fenced; another node leads now");
        state.lease = None;
        state.settled.clear();
        self.deps.stats.leader.store(false, Ordering::Relaxed);
        result.leader = false;
        result.refused = Some("fenced: another node took the scheduler lease".into());
    }

    /// Step 6 of the tick: plan 32 Step 4's expiry run for every root due
    /// one (`crate::snapexpire` has the algorithm and its reasons).
    async fn expire(
        &self,
        state: &mut SchedState,
        plans: &[RootPlan],
        now: i64,
        result: &mut SnapSchedRunResult,
        audit: &mut BTreeMap<Ino, SnapSchedJournalRoot>,
        reach: &mut Reach,
    ) {
        let config = &self.deps.config;
        let stats = &self.deps.stats;
        let every = config.expire_every.as_millis() as i64;
        let runs: Vec<&RootPlan> = plans
            .iter()
            .filter(|p| expirable(p))
            .filter(|p| {
                state
                    .last_expiry
                    .get(&p.ino)
                    .is_none_or(|last| now.saturating_sub(*last) >= every || now < *last)
            })
            .collect();
        if runs.is_empty() {
            return;
        }
        // 1. The grace state, recorded before anything is deleted.
        let store = &self.deps.store;
        let (mut grace, tag) = match snapsched_store::load_state(store).await {
            Ok(read) => read,
            Err(error) => {
                let error = format!(
                    "reading the grace state (snapsched/state.json): {error}; \
                     nothing expires until it is readable"
                );
                self.note_error(None, error.clone());
                result.error = Some(error);
                return;
            }
        };
        let grace_ms = self.grace_ms();
        let seen: Vec<(Ino, String)> = plans
            .iter()
            .filter_map(|p| {
                p.policy
                    .as_ref()
                    .map(|q| (p.ino, snapexpire::canonical_unpaused(q)))
            })
            .collect();
        if snapexpire::observe(&mut grace, &seen, now, grace_ms) {
            grace.updated_unix_ms = now;
            grace.node = self.deps.node_id;
            // Only the leader writes it: renew first.
            let Some(lease) = state.lease.as_mut() else {
                result.error = Some(NO_LEASE.into());
                return;
            };
            if let Err(error) = lease.renew().await {
                if error.downcast_ref::<Fenced>().is_some() {
                    self.fenced(state, result);
                } else {
                    let error = format!("renewing the {LEASE_NAME} lease: {error:#}");
                    self.note_error(None, error.clone());
                    result.error = Some(error);
                }
                return;
            }
            let saved =
                snapsched_store::save_state(store, self.deps.lease_mode, &grace, &tag).await;
            if let Err(error) = saved {
                let error = match error {
                    StoreError::CasConflict => "recording the grace state: another writer \
                         changed snapsched/state.json; nothing expires in this run (retried \
                         next tick)"
                        .to_string(),
                    error => format!(
                        "recording the grace state (snapsched/state.json): {error}; \
                         nothing expires in this run"
                    ),
                };
                self.note_error(None, error.clone());
                result.error = Some(error);
                return;
            }
        }
        self.cache_grace(&grace);
        // 2–4. Each root's run.
        for p in runs {
            state.last_expiry.insert(p.ino, now);
            let policy = p.policy.as_ref().expect("an expirable root parses");
            // The rows as they are now: after this tick's creation.
            let rows = match snapexpire::root_rows(&self.deps.meta, p.ino) {
                Ok(rows) => rows,
                Err(error) => {
                    self.note_error(Some(p.ino), format!("reading the snapshot rows: {error:#}"));
                    continue;
                }
            };
            let facts: Vec<SnapFacts> = rows.iter().map(SnapFacts::from_row).collect();
            let g = snapexpire::graced(
                policy,
                p.ino,
                &facts,
                grace.roots.get(&p.ino),
                now,
                grace_ms,
            );
            inc(&stats.skipped_grace, g.kept_by_grace() as u64);
            let mut victims: Vec<SnapshotRow> = rows
                .into_iter()
                .zip(&g.verdicts)
                .filter(|(_, v)| !v.keep)
                .map(|(row, _)| row)
                .collect();
            victims.truncate(config.max_deletes);
            if victims.is_empty() {
                continue;
            }
            let Some(lease) = state.lease.as_mut() else {
                result.error = Some(NO_LEASE.into());
                return;
            };
            let run = snapexpire::delete_victims(
                &self.deps.batches,
                &self.deps.meta,
                Some(lease),
                p.ino,
                &victims,
                config.expire_batch,
                self.before_delete(),
            )
            .await;
            inc(&stats.expired, run.deleted.len() as u64);
            inc(&stats.skipped_reverify, run.skipped.len() as u64);
            reach.reached |= run.reached_holder;
            reach.unreachable |= run.holder_unreachable;
            if !run.deleted.is_empty() {
                let journal = audit.entry(p.ino).or_insert_with(|| journal_root(p));
                for row in &run.deleted {
                    journal
                        .deleted
                        .push(snapexpire::journal_snap(row, snapexpire::REASON_NO_TIER));
                    result.roots.push(expiry_root(p, row, "expired", None));
                }
            }
            for (row, why) in &run.skipped {
                result
                    .roots
                    .push(expiry_root(p, row, "skipped_reverify", Some(why.clone())));
            }
            if run.fenced {
                self.fenced(state, result);
                return;
            }
            if let Some(error) = run.error {
                let error = format!("policy root {} expiry: {error}", p.ino);
                self.note_error(Some(p.ino), error.clone());
                result.error = Some(error);
                return;
            }
        }
    }

    /// Write the tick's audit object when it created, failed or deleted
    /// anything. A tick of only skips / already-exists writes none.
    async fn write_audit(&self, now: i64, audit: BTreeMap<Ino, SnapSchedJournalRoot>) {
        let roots: Vec<SnapSchedJournalRoot> = audit.into_values().collect();
        if !roots
            .iter()
            .any(|r| !r.created.is_empty() || !r.failed.is_empty() || !r.deleted.is_empty())
        {
            return;
        }
        let entry = SnapSchedJournalEntry {
            ts: now,
            node: self.deps.node_id,
            roots,
        };
        if let Err(error) = snapsched_store::append_journal(&self.deps.store, &entry).await {
            tracing::warn!(error = %error, "snapshot scheduler: writing the audit object failed");
        }
    }

    /// The carried M3c gap (snapshot batches have no S3 path): a leader
    /// whose batches cannot reach the root-lease holder for
    /// `CONSTELLATION_SNAPSCHED_RESIGN_AFTER` ticks in a row gives the
    /// `_snapsched` lease back and stays out for one lease TTL, so a node
    /// that can reach the holder (the holder itself included) takes over.
    /// A tick whose batch reached the holder resets the count; a tick
    /// that sent nothing leaves it. If no node can reach the holder the
    /// lease rotates through them, one TTL each — nothing is created or
    /// deleted either way, so the rotation is harmless.
    async fn track_holder(
        &self,
        state: &mut SchedState,
        reach: Reach,
        result: &mut SnapSchedRunResult,
    ) {
        if reach.reached && !reach.unreachable {
            state.unreachable_ticks = 0;
            return;
        }
        if !reach.unreachable {
            return;
        }
        state.unreachable_ticks += 1;
        let limit = self.deps.config.resign_after;
        if limit == 0 || state.unreachable_ticks < limit {
            return;
        }
        state.unreachable_ticks = 0;
        if let Some(lease) = state.lease.take() {
            let error = format!(
                "resigned the snapshot scheduler: its batches could not reach the root-lease \
                 holder for {limit} ticks in a row (CONSTELLATION_SNAPSCHED_RESIGN_AFTER); \
                 another node may lead"
            );
            tracing::warn!("snapshot scheduler: {error}");
            lease.release().await;
            state.settled.clear();
            state.backoff_until = Some(
                tokio::time::Instant::now() + Duration::from_millis(self.deps.config.lease_ttl_ms),
            );
            self.deps.stats.leader.store(false, Ordering::Relaxed);
            self.note_error(None, error);
            result.leader = false;
        }
    }

    fn grace_ms(&self) -> i64 {
        i64::try_from(self.deps.config.grace.as_millis()).unwrap_or(i64::MAX)
    }

    fn cache_grace(&self, grace: &SnapSchedState) {
        put_grace_cache(&self.grace_cache, grace);
    }

    /// The grace state for display (`snapshot.list`, `policy show`, the
    /// `policy set` note, a dry run), *projected*: the bucket's copy with
    /// what the next expiry run will record first ([`snapexpire::observe`]
    /// of the policy roots this replica has now) applied, not written. So
    /// a newly seen or just changed root shows the grace its run will
    /// give it. The bucket's copy is the one the last expiry run read; when
    /// that is older than [`GRACE_CACHE`] it is still served and a
    /// background read refreshes it; only with no copy at all does the
    /// call read (bounded by [`GRACE_READ_TIMEOUT`]). `None` when nothing
    /// is known.
    pub async fn grace_view(&self) -> Option<SnapSchedState> {
        let cached = self.grace_cache.lock().unwrap().clone();
        let mut state = match cached {
            Some((at, state)) => {
                if at.elapsed() >= GRACE_CACHE {
                    self.refresh_grace_in_background();
                }
                state
            }
            None => match tokio::time::timeout(
                GRACE_READ_TIMEOUT,
                snapsched_store::load_state(&self.deps.store),
            )
            .await
            {
                Ok(Ok((state, _))) => {
                    self.cache_grace(&state);
                    state
                }
                _ => return None,
            },
        };
        if let Ok(roots) = self.deps.meta.snapshot_policy_roots() {
            let seen: Vec<(Ino, String)> = roots
                .iter()
                .filter_map(|(ino, expr)| {
                    let policy = SnapPolicy::parse(expr).ok()?;
                    Some((*ino, snapexpire::canonical_unpaused(&policy)))
                })
                .collect();
            snapexpire::observe(&mut state, &seen, (self.deps.clock)(), self.grace_ms());
        }
        Some(state)
    }

    fn refresh_grace_in_background(&self) {
        if self.grace_refreshing.swap(true, Ordering::SeqCst) {
            return;
        }
        let (store, cache) = (self.deps.store.clone(), self.grace_cache.clone());
        let refreshing = self.grace_refreshing.clone();
        tokio::spawn(async move {
            if let Ok(Ok((state, _))) =
                tokio::time::timeout(GRACE_CACHE, snapsched_store::load_state(&store)).await
            {
                put_grace_cache(&cache, &state);
            }
            refreshing.store(false, Ordering::SeqCst);
        });
    }

    /// The grace every display applies ([`Self::grace_view`] at this
    /// node's clock and `GRACE_S`): `snapshot.list`, `policy show` and
    /// the `policy set` note agree because they all use it.
    pub async fn display_grace(&self) -> snapexpire::GraceView {
        snapexpire::GraceView {
            state: self.grace_view().await,
            now: (self.deps.clock)(),
            grace_ms: self.grace_ms(),
        }
    }

    /// `KEPT BY` / `EXPIRES` for `snapshot.list` ([`snapexpire::listing`]).
    pub async fn listing(&self) -> anyhow::Result<HashMap<String, snapexpire::Listed>> {
        let grace = self.display_grace().await;
        snapexpire::listing(&self.deps.meta, &grace)
    }

    /// `snapshot.policy.remove {expire}`: delete `victims` (the root's
    /// unheld auto snapshots, oldest first) through the same re-read and
    /// holder-side batch as an expiry run, after the policy is gone. No
    /// scheduler lease: an operator's confirmed delete, like
    /// `snapshot.delete`. Counts `expired` / `skipped_reverify` and writes
    /// an audit object.
    pub async fn expire_removed(
        &self,
        ino: Ino,
        path: &str,
        policy: &str,
        victims: &[SnapshotRow],
    ) -> snapexpire::DeleteRun {
        let run = snapexpire::delete_victims(
            &self.deps.batches,
            &self.deps.meta,
            None,
            ino,
            victims,
            self.deps.config.expire_batch,
            self.before_delete(),
        )
        .await;
        let stats = &self.deps.stats;
        inc(&stats.expired, run.deleted.len() as u64);
        inc(&stats.skipped_reverify, run.skipped.len() as u64);
        if !run.deleted.is_empty() {
            let entry = SnapSchedJournalEntry {
                ts: (self.deps.clock)(),
                node: self.deps.node_id,
                roots: vec![SnapSchedJournalRoot {
                    root_ino: ino,
                    path: path.to_string(),
                    policy: policy.to_string(),
                    created: Vec::new(),
                    skipped: Vec::new(),
                    failed: Vec::new(),
                    deleted: run
                        .deleted
                        .iter()
                        .map(|row| snapexpire::journal_snap(row, snapexpire::REASON_REMOVED))
                        .collect(),
                }],
            };
            if let Err(error) = snapsched_store::append_journal(&self.deps.store, &entry).await {
                tracing::warn!(error = %error, "policy remove --expire: writing the audit object failed");
            }
        }
        run
    }

    #[cfg(test)]
    fn before_delete(&self) -> Option<snapexpire::BeforeBatch> {
        self.before_delete.lock().unwrap().clone()
    }

    #[cfg(not(test))]
    fn before_delete(&self) -> Option<snapexpire::BeforeBatch> {
        None
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
            grace: Duration::from_secs(86_400),
            expire_every: Duration::from_secs(60),
            max_deletes: 500,
            expire_batch: 32,
            resign_after: 3,
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

    /// A store that counts every request, and while `down` fails every
    /// get and put (an S3 outage, as far as a lease is concerned).
    #[derive(Debug)]
    struct Counting {
        inner: Arc<InMemory>,
        requests: AtomicUsize,
        down: AtomicBool,
    }

    impl Counting {
        fn over(inner: &Arc<InMemory>) -> Arc<Counting> {
            Arc::new(Counting {
                inner: inner.clone(),
                requests: AtomicUsize::new(0),
                down: AtomicBool::new(false),
            })
        }

        fn outage(&self) -> object_store::Result<()> {
            if self.down.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "Counting",
                    source: "connection reset by peer".into(),
                });
            }
            Ok(())
        }
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
            self.outage()?;
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
            self.outage()?;
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

    /// Plan 32 Step 9's outage signature: a leader that cannot reach the
    /// store (its lease renewal fails before any batch) counts each due
    /// root's snapshot as `create_failed`, creates nothing, keeps its
    /// lease, and takes the bucket once the store is back. A node that
    /// does not lead counts nothing: it does not know it would have.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unreachable_store_counts_create_failed_at_the_leader_only() {
        let (store, a, _) = solo("10s:1m; skip-empty=no").await;
        let counting = Counting::over(&store);
        let now = test_clock();
        let sched = scheduler(&a, counting.clone(), &now, config());
        assert_eq!(outcomes(&sched.tick(Run::Periodic).await), ["created"]);
        advance(&now, 10 * SEC);
        counting.down.store(true, Ordering::SeqCst);
        for _ in 0..3 {
            let report = sched.tick(Run::Periodic).await;
            assert_eq!(outcomes(&report), ["failed"]);
            assert!(report.leader, "still the leader as far as anyone knows");
            assert!(report.error.unwrap().contains("_snapsched"));
        }
        let stats = sched.deps.stats.status();
        assert_eq!((stats.created, stats.create_failed), (1, 3));
        assert!(stats.last_error.unwrap().contains("connection reset"));
        // A second scheduler that never led: the same failure, no count.
        let other = scheduler(&a, counting.clone(), &now, config());
        let report = other.tick(Run::Periodic).await;
        assert!(report.error.is_some() && report.roots.is_empty() && !report.leader);
        assert_eq!(other.deps.stats.status().create_failed, 0);
        assert_eq!(autos(&a).len(), 1);
        // The store is back: one snapshot for the bucket, no more.
        counting.down.store(false, Ordering::SeqCst);
        assert_eq!(outcomes(&sched.tick(Run::Periodic).await), ["created"]);
        assert!(sched.tick(Run::Periodic).await.roots.is_empty());
        assert_eq!(autos(&a).len(), 2);
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
        let counting = Counting::over(&store);
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

    // --- expiry (plan 32 Step 4, M4a) ----------------------------------

    /// Expiry knobs for the tests: no grace window, a run every tick.
    fn expiry_config() -> SchedConfig {
        SchedConfig {
            grace: Duration::ZERO,
            expire_every: Duration::ZERO,
            ..config()
        }
    }

    /// The holder stamps `created_unix_ms` from the test clock, so the
    /// stream spreads over the buckets the test clock walks.
    fn stamp(n: &Node, now: &Arc<AtomicI64>) {
        let now = now.clone();
        *n.snapshots.created_clock.lock().unwrap() =
            Some(Arc::new(move || now.load(Ordering::Relaxed)));
    }

    /// The ids `policy` keeps of `rows` (retention, the oracle).
    fn kept_ids(policy: &str, ino: Ino, rows: &[SnapshotRow]) -> BTreeSet<String> {
        let policy = SnapPolicy::parse(policy).unwrap();
        let facts: Vec<SnapFacts> = rows.iter().map(SnapFacts::from_row).collect();
        rows.iter()
            .zip(retention::evaluate(&policy, ino, &facts))
            .filter(|(_, v)| v.keep)
            .map(|(r, _)| r.id.clone())
            .collect()
    }

    /// Add the auto rows `n` lists now that `created` lacks.
    fn record_new(created: &mut Vec<SnapshotRow>, n: &Node) {
        for row in autos(n) {
            if !created.iter().any(|c| c.id == row.id) {
                created.push(row);
            }
        }
    }

    fn ids(rows: &[SnapshotRow]) -> BTreeSet<String> {
        rows.iter().map(|r| r.id.clone()).collect()
    }

    async fn journal(store: &Arc<InMemory>) -> Vec<SnapSchedJournalEntry> {
        let keys: Vec<ObjPath> = store
            .list(Some(&snapsched_store::journal_prefix()))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        let mut entries = Vec::new();
        for key in keys {
            let body = store.get(&key).await.unwrap().bytes().await.unwrap();
            entries.push(serde_json::from_slice::<SnapSchedJournalEntry>(&body).unwrap());
        }
        entries.sort_by_key(|e| e.ts);
        entries
    }

    fn hold_item(id: &str, by: Option<&str>) -> SnapshotItem {
        SnapshotItem::Hold {
            id: id.to_string(),
            held: true,
            by: by.map(str::to_string),
            force: false,
        }
    }

    async fn hold(n: &Node, id: &str, by: Option<&str>) {
        let results = n
            .batcher
            .submit(n.batcher.next_rid(), vec![hold_item(id, by)])
            .await
            .unwrap();
        assert!(
            matches!(&results[0], ItemResult::HoldSet { row } if row.held),
            "{results:?}"
        );
    }

    /// `count` more snapshots of `/data`, one per 10 s bucket, taken by a
    /// scheduler that records the grace state but deletes nothing
    /// (`max_deletes: 0`); it resigns, so the test's own scheduler leads
    /// next. Returns every auto row, oldest first.
    /// The builder records the first sighting with `grace`, the window
    /// the test's own scheduler is configured with: a recorded window
    /// stays open for its recorded length whatever a later reader's
    /// `grace` (Step 4.3).
    async fn history(
        a: &Node,
        holder: &Node,
        store: &Arc<InMemory>,
        now: &Arc<AtomicI64>,
        count: usize,
        grace: Duration,
    ) -> Vec<SnapshotRow> {
        let builder = scheduler(
            a,
            store.clone(),
            now,
            SchedConfig {
                max_deletes: 0,
                grace,
                ..expiry_config()
            },
        );
        let before = autos(a).len();
        for _ in 0..count {
            assert_eq!(outcomes(&builder.tick(Run::Periodic).await), ["created"]);
            advance(now, 10 * SEC);
        }
        builder.resign().await;
        holder.sync().await;
        if holder.id != a.id {
            a.tail().await;
        }
        let rows = autos(a);
        assert_eq!(rows.len(), before + count);
        rows
    }

    /// Plan 32 Step 4 / Step 11: in steady state the surviving set is
    /// `retention::evaluate` over **every** snapshot ever created — after
    /// each tick, not just at the end. A manual snapshot of the same
    /// directory survives throughout; every deletion is audited with its
    /// reason, and `expired` counts exactly the deletions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn steady_state_survivors_are_evaluate_over_every_creation() {
        const POLICY: &str = "10s:1m 1m:3m; last=2; skip-empty=no";
        let (store, a, data) = solo(POLICY).await;
        let now = test_clock();
        stamp(&a, &now);
        let manual = a
            .batcher
            .submit(
                a.batcher.next_rid(),
                vec![SnapshotItem::Create {
                    path: "/data".into(),
                    name: "by-hand".into(),
                    origin: 0,
                    policy_ino: 0,
                    creator: 1,
                    held: false,
                    held_by: None,
                    skip_if_unchanged_since: None,
                }],
            )
            .await
            .unwrap();
        assert!(matches!(&manual[0], ItemResult::Created { .. }));
        let sched = scheduler(&a, store.clone(), &now, expiry_config());
        let mut created: Vec<SnapshotRow> = Vec::new();
        for tick in 0..40 {
            let report = sched.tick(Run::Periodic).await;
            assert!(report.error.is_none(), "tick {tick}: {report:?}");
            record_new(&mut created, &a);
            let survivors = autos(&a);
            assert_eq!(
                ids(&survivors),
                kept_ids(POLICY, data, &created),
                "tick {tick}: the survivors are not evaluate(all creations)"
            );
            advance(&now, 10 * SEC);
        }
        assert_eq!(created.len(), 40);
        let survivors = autos(&a);
        let stats = sched.deps.stats.status();
        assert_eq!(stats.expired as usize, created.len() - survivors.len());
        assert!(stats.expired > 20, "{stats:?}");
        assert_eq!(stats.skipped_reverify, 0);
        assert!(
            a.rows()
                .iter()
                .any(|r| r.name == "by-hand" && r.origin == 0),
            "the manual snapshot survives"
        );
        let deleted: Vec<SnapSchedJournalSnap> = journal(&store)
            .await
            .into_iter()
            .flat_map(|e| e.roots.into_iter().flat_map(|r| r.deleted))
            .collect();
        assert_eq!(deleted.len() as u64, stats.expired);
        assert!(deleted
            .iter()
            .all(|d| d.reason.as_deref() == Some(snapexpire::REASON_NO_TIER)));
        let gone: BTreeSet<String> = deleted.iter().map(|d| d.id.clone()).collect();
        assert_eq!(gone, &ids(&created) - &ids(&survivors));
        assert_eq!(snaps_objects(&store).await, survivors.len() + 1);
    }

    /// Step 4.1: a hold that lands between evaluation and the delete wins,
    /// both ways it can land. Victim 0 is held at the holder B behind the
    /// leader A's back (A's replica still has it unheld, so the re-read
    /// passes and B's own check refuses it); victim 1 is held through A,
    /// whose replica then sees it, so the re-read drops it. Both count
    /// `skipped_reverify`, survive on both replicas, and the rest go.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_hold_between_evaluation_and_delete_wins() {
        const POLICY: &str = "10s:1m; skip-empty=no";
        let (store, a, b, data, _) = holder_b().await;
        bind(&b, &[&a], data, POLICY).await;
        let now = test_clock();
        stamp(&b, &now);
        let rows = history(&a, &b, &store, &now, 10, expiry_config().grace).await;
        b.sync().await;
        a.tail().await;
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                expire_batch: 1,
                ..expiry_config()
            },
        );
        let (a_batcher, b_batcher) = (a.batcher.clone(), b.batcher.clone());
        let (a_driver, b_driver, b_host) = (a.driver.clone(), b.driver.clone(), b.host.clone());
        let hook: snapexpire::BeforeBatch = Arc::new(move |index, ids: Vec<String>| {
            let (a_batcher, b_batcher) = (a_batcher.clone(), b_batcher.clone());
            let (a_driver, b_driver, b_host) = (a_driver.clone(), b_driver.clone(), b_host.clone());
            Box::pin(async move {
                let via = match index {
                    0 => (&b_batcher, "csi:content-uid"),
                    1 => (&a_batcher, "user:ops"),
                    _ => return,
                };
                let results = via
                    .0
                    .submit(via.0.next_rid(), vec![hold_item(&ids[0], Some(via.1))])
                    .await
                    .unwrap();
                assert!(matches!(&results[0], ItemResult::HoldSet { .. }));
                if index == 1 {
                    // B ships it (the daemon's nudge) and A tails it.
                    let mut driver = b_driver.lock().await;
                    driver.sync().await.unwrap();
                    driver.mirror(&b_host.view);
                    drop(driver);
                    a_driver.lock().await.tail_to_head().await.unwrap();
                }
            })
        });
        *sched.before_delete.lock().unwrap() = Some(hook);
        let report = sched.tick(Run::Manual { dry_run: false }).await;
        assert!(report.error.is_none(), "{report:?}");
        let stats = sched.deps.stats.status();
        // 10s:1m keeps 6 buckets, so the 4 oldest of 10 were victims.
        assert_eq!(
            (stats.skipped_reverify, stats.expired),
            (2, 2),
            "{report:?}"
        );
        let skipped: Vec<(&str, &str)> = report
            .roots
            .iter()
            .filter(|r| r.outcome == "skipped_reverify")
            .map(|r| (r.name.as_str(), r.error.as_deref().unwrap()))
            .collect();
        assert_eq!(
            skipped,
            [
                (rows[0].name.as_str(), "held at the holder"),
                (rows[1].name.as_str(), "held")
            ]
        );
        b.sync().await;
        a.tail().await;
        for n in [&a, &b] {
            let left = autos(n);
            // Victims 2 and 3 went; everything else (and the tick's own
            // new snapshot) is there.
            let gone = &ids(&rows) - &ids(&left);
            assert_eq!(gone, ids(&rows[2..4]), "node {}", n.id);
            assert_eq!(left.len(), rows.len() - 2 + 1, "node {}", n.id);
            assert!(left[0].held && left[1].held);
        }
    }

    /// Step 4.3: a root seen for the first time expires nothing for the
    /// grace window (`skipped_grace` counts the survivors); a policy
    /// shortened through the xattr deletes nothing the old policy keeps
    /// inside the window, and after it exactly what the new one says.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn grace_after_a_first_sighting_and_after_a_shortened_policy() {
        const SHORT: &str = "10s:1m; skip-empty=no";
        const LONG: &str = "10s:3m; skip-empty=no";
        const WINDOW: i64 = 600 * SEC;
        let (store, a, data) = solo(SHORT).await;
        let now = test_clock();
        stamp(&a, &now);
        // First sighting at t0 (the history's first tick records it); 20
        // buckets of which SHORT keeps 6.
        let t0 = now.load(Ordering::Relaxed);
        let mut created = history(
            &a,
            &a,
            &store,
            &now,
            20,
            Duration::from_millis(WINDOW as u64),
        )
        .await;
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                grace: Duration::from_millis(WINDOW as u64),
                ..expiry_config()
            },
        );
        for _ in 0..5 {
            let report = sched.tick(Run::Periodic).await;
            assert!(report.error.is_none(), "{report:?}");
            advance(&now, 10 * SEC);
        }
        record_new(&mut created, &a);
        let stats = sched.deps.stats.status();
        assert_eq!(stats.expired, 0, "nothing inside the first-sighting window");
        assert!(stats.skipped_grace >= 14, "{stats:?}");
        assert_eq!(autos(&a).len(), 25);
        // The window closes: exactly what SHORT says.
        now.store(t0 + WINDOW, Ordering::Relaxed);
        sched.tick(Run::Periodic).await;
        record_new(&mut created, &a);
        assert_eq!(ids(&autos(&a)), kept_ids(SHORT, data, &created));

        // Lengthened to LONG: nothing more goes (LONG keeps a superset).
        bind(&a, &[], data, LONG).await;
        let mut created = autos(&a);
        for _ in 0..24 {
            advance(&now, 10 * SEC);
            sched.tick(Run::Periodic).await;
            record_new(&mut created, &a);
            assert_eq!(ids(&autos(&a)), kept_ids(LONG, data, &created));
        }
        // Shortened to SHORT at T through the xattr: inside the window only
        // what LONG also expires goes.
        bind(&a, &[], data, SHORT).await;
        let t = now.load(Ordering::Relaxed);
        let grace_before = sched.deps.stats.status().skipped_grace;
        for _ in 0..10 {
            sched.tick(Run::Periodic).await;
            record_new(&mut created, &a);
            assert_eq!(
                ids(&autos(&a)),
                kept_ids(LONG, data, &created),
                "inside the window only what both expire goes"
            );
            assert!(
                autos(&a).len() > kept_ids(SHORT, data, &created).len(),
                "the window held some back"
            );
            advance(&now, 10 * SEC);
        }
        assert!(sched.deps.stats.status().skipped_grace > grace_before);
        // After it: exactly SHORT.
        now.store(t + WINDOW, Ordering::Relaxed);
        sched.tick(Run::Periodic).await;
        record_new(&mut created, &a);
        assert_eq!(ids(&autos(&a)), kept_ids(SHORT, data, &created));
        // The grace record: SHORT since T, every window closed (a closed
        // prior stays recorded a while; it decides nothing).
        let (state, _) = snapsched_store::load_state(&(store.clone() as Arc<dyn ObjectStore>))
            .await
            .unwrap();
        let entry = &state.roots[&data];
        assert_eq!(
            entry.canonical,
            SnapPolicy::parse(SHORT).unwrap().to_string()
        );
        assert_eq!(entry.since_unix_ms, t);
        let at = now.load(Ordering::Relaxed);
        assert!(
            entry.prior.iter().all(|p| p.until_unix_ms <= at),
            "{entry:?}"
        );
    }

    /// Step 4.3: a window's length is recorded with it. A first sighting
    /// recorded under a day-long grace stays open for a leader configured
    /// with ten minutes: it deletes nothing and keeps the prior in
    /// `state.json`, so the window is still there for the next leader.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_shorter_grace_leader_honours_a_longer_recorded_window() {
        const POLICY: &str = "10s:1m; skip-empty=no";
        let (store, a, _data) = solo(POLICY).await;
        let now = test_clock();
        stamp(&a, &now);
        let day = Duration::from_secs(86_400);
        let rows = history(&a, &a, &store, &now, 10, day).await;
        let short = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                grace: Duration::from_secs(600),
                ..expiry_config()
            },
        );
        advance(&now, 3600 * SEC);
        let report = short.tick(Run::Periodic).await;
        assert!(report.error.is_none(), "{report:?}");
        let stats = short.deps.stats.status();
        assert_eq!(stats.expired, 0, "{report:?}");
        assert!(stats.skipped_grace > 0, "{stats:?}");
        let mut created = rows.clone();
        record_new(&mut created, &a);
        assert_eq!(autos(&a), created, "nothing deleted");
        let (state, _) = snapsched_store::load_state(&(store.clone() as Arc<dyn ObjectStore>))
            .await
            .unwrap();
        let prior = &state.roots.values().next().unwrap().prior;
        assert!(
            prior.iter().any(
                |p| p.canonical.is_none() && p.until_unix_ms == p.replaced_unix_ms + 86_400_000
            ),
            "{prior:?}"
        );
    }

    /// Step 4.2: a paused root deletes nothing, nor does a refused tick;
    /// resuming is not a policy change, so expiry resumes at once although
    /// a grace window is configured. Removing a policy orphans its
    /// snapshots, and none is ever deleted afterwards, however long the
    /// scheduler keeps ticking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn paused_refused_and_removed_roots_delete_nothing() {
        const POLICY: &str = "10s:1m; skip-empty=no";
        const PAUSED: &str = "10s:1m; skip-empty=no; paused";
        let (store, a, data) = solo(POLICY).await;
        let now = test_clock();
        stamp(&a, &now);
        // The first sighting (and its window) is long past by the end.
        let rows = history(&a, &a, &store, &now, 10, Duration::from_secs(30)).await;
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                grace: Duration::from_secs(30),
                ..expiry_config()
            },
        );
        // Paused: nothing created, nothing deleted.
        bind(&a, &[], data, PAUSED).await;
        for _ in 0..3 {
            let report = sched.tick(Run::Periodic).await;
            assert!(report.roots.is_empty(), "paused: {report:?}");
            advance(&now, 10 * SEC);
        }
        assert_eq!(autos(&a), rows);
        // Refused (a departed node): nothing either.
        bind(&a, &[], data, POLICY).await;
        sched.deps.departed.store(true, Ordering::Relaxed);
        assert!(sched.tick(Run::Periodic).await.refused.is_some());
        assert_eq!(autos(&a), rows);
        sched.deps.departed.store(false, Ordering::Relaxed);
        // Resumed: not a change, no new window — the victims go now.
        let report = sched.tick(Run::Periodic).await;
        assert!(report.error.is_none(), "{report:?}");
        assert!(sched.deps.stats.status().expired > 0, "{report:?}");
        let mut created = rows.clone();
        record_new(&mut created, &a);
        assert_eq!(ids(&autos(&a)), kept_ids(POLICY, data, &created));
        assert_eq!(sched.deps.stats.status().skipped_grace, 0);
        // Removed: its snapshots are orphaned and stay, tick after tick.
        // (Another root keeps the scheduler busy: with no policy root at
        // all a tick does nothing, not even count orphans.)
        let other = a.meta.mkdir(1, "other", 0o755, 0, 0).unwrap().ino;
        bind(&a, &[], other, "1h:1d; skip-empty=no").await;
        let left = autos(&a);
        a.meta.remove_xattr(data, SNAPSHOT_POLICY_XATTR).unwrap();
        a.sync().await;
        let expired = sched.deps.stats.status().expired;
        for _ in 0..12 {
            advance(&now, 10 * SEC);
            sched.tick(Run::Periodic).await;
        }
        let left_now: Vec<SnapshotRow> = autos(&a)
            .into_iter()
            .filter(|r| r.policy_ino == data)
            .collect();
        assert_eq!(left_now, left);
        let stats = sched.deps.stats.status();
        assert_eq!(stats.expired, expired);
        assert_eq!(stats.orphaned_snapshots, left.len() as u64);
    }

    /// Step 3.1 / 4.2: an unparseable policy written past the setxattr gate
    /// (straight into the replica) is inert on both nodes: whichever leads
    /// creates nothing and deletes nothing — though the last parseable
    /// policy had victims — and the stream counts as orphaned everywhere.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unparseable_policy_is_inert_on_both_nodes() {
        let (store, a, b, data, _) = holder_b().await;
        bind(&b, &[&a], data, "10s:1m; skip-empty=no").await;
        let now = test_clock();
        stamp(&b, &now);
        let rows = history(&a, &b, &store, &now, 9, expiry_config().grace).await;
        b.meta
            .set_xattr(
                data,
                SNAPSHOT_POLICY_XATTR,
                b"10s:1m; every now and then",
                SetXattrMode::Set,
            )
            .unwrap();
        b.sync().await;
        a.tail().await;
        let sa = scheduler(&a, store.clone(), &now, expiry_config());
        let sb = scheduler(&b, store.clone(), &now, expiry_config());
        for _ in 0..6 {
            for s in [&sa, &sb] {
                let report = s.tick(Run::Periodic).await;
                assert!(report.roots.is_empty(), "{report:?}");
            }
            advance(&now, 10 * SEC);
        }
        b.sync().await;
        a.tail().await;
        for (n, s) in [(&a, &sa), (&b, &sb)] {
            assert_eq!(ids(&autos(n)), ids(&rows), "node {}", n.id);
            let stats = s.deps.stats.status();
            assert_eq!(
                (
                    stats.unparseable_roots,
                    stats.orphaned_snapshots,
                    stats.expired
                ),
                (1, 9, 0)
            );
        }
    }

    /// A renewal refused mid-run (another node took `_snapsched` between two
    /// delete batches) stops the deletions on the spot: the first batch's
    /// victim is gone, nothing after it is sent, and the node no longer
    /// leads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fenced_renewal_stops_the_deletions_at_once() {
        let (store, a, _) = solo("10s:1m; skip-empty=no").await;
        let now = test_clock();
        stamp(&a, &now);
        let rows = history(&a, &a, &store, &now, 9, expiry_config().grace).await;
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                expire_batch: 1,
                ..expiry_config()
            },
        );
        let thief = store.clone() as Arc<dyn ObjectStore>;
        let hook: snapexpire::BeforeBatch = Arc::new(move |index, _| {
            let thief = thief.clone();
            Box::pin(async move {
                if index == 1 {
                    let leases =
                        constellation_store_s3::LeaseStore::new(thief, LEASE_NAME, LeaseMode::Cas);
                    let (lease, tag) = leases.get().await.unwrap().unwrap();
                    let other = constellation_store_s3::Lease::granted(
                        LEASE_NAME,
                        lease.holder ^ 1,
                        lease.epoch + 1,
                        60_000,
                    );
                    leases.try_swap(&other, &tag).await.unwrap();
                }
            })
        });
        *sched.before_delete.lock().unwrap() = Some(hook);
        let report = sched.tick(Run::Periodic).await;
        assert!(report.refused.unwrap().contains("fenced"));
        assert!(!report.leader && !sched.deps.stats.leader.load(Ordering::Relaxed));
        let stats = sched.deps.stats.status();
        assert_eq!((stats.expired, stats.skipped_reverify), (1, 0));
        let left = autos(&a);
        // Ten rows (the tick created one) less the one deleted.
        assert_eq!(left.len(), rows.len());
        assert!(!ids(&left).contains(&rows[0].id), "victim 0 went");
        assert!(ids(&left).contains(&rows[1].id), "victim 1 was never sent");
        // Not the leader any more: the next tick takes nothing.
        let next = sched.tick(Run::Periodic).await;
        assert!(next.refused.is_some() && next.roots.is_empty(), "{next:?}");
        assert_eq!(autos(&a).len(), rows.len());
    }

    /// A store where another writer replaces `snapsched/state.json` right
    /// before each of this node's writes while `steal` is set: the CAS on
    /// the ETag this node read is lost.
    #[derive(Debug)]
    struct StateThief {
        inner: Arc<InMemory>,
        steal: AtomicBool,
    }

    impl std::fmt::Display for StateThief {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "StateThief({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for StateThief {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if *location == snapsched_store::state_key() && self.steal.load(Ordering::SeqCst) {
                let theirs = SnapSchedState {
                    node: 99,
                    ..Default::default()
                };
                self.inner
                    .put(location, serde_json::to_vec(&theirs).unwrap().into())
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

    /// Step 4.3: a lost CAS on `snapsched/state.json` aborts the run's
    /// deletions — the change it had to record (here a new policy) is not
    /// in the bucket, so nothing is deleted on it — and the next tick
    /// retries and deletes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lost_state_cas_aborts_the_deletions() {
        let (store, a, data) = solo("10s:3m; skip-empty=no").await;
        let now = test_clock();
        stamp(&a, &now);
        let rows = history(&a, &a, &store, &now, 9, expiry_config().grace).await;
        bind(&a, &[], data, "10s:1m; skip-empty=no").await;
        let thief = Arc::new(StateThief {
            inner: store.clone(),
            steal: AtomicBool::new(true),
        });
        let sched = scheduler(&a, thief.clone(), &now, expiry_config());
        let report = sched.tick(Run::Manual { dry_run: false }).await;
        assert!(
            report
                .error
                .as_deref()
                .unwrap_or("")
                .contains("another writer"),
            "{report:?}"
        );
        assert_eq!(sched.deps.stats.status().expired, 0);
        assert_eq!(autos(&a).len(), rows.len() + 1, "nothing deleted");
        // The other writer is done: the retry records and deletes.
        thief.steal.store(false, Ordering::SeqCst);
        let report = sched.tick(Run::Manual { dry_run: false }).await;
        assert!(report.error.is_none(), "{report:?}");
        assert_eq!(sched.deps.stats.status().expired, 4);
        assert_eq!(autos(&a).len(), 6);
    }

    /// Plan 32 / plan 37: held snapshots — `csi:`, `user:` and plain alike
    /// — are never expired, even as the oldest snapshots, the ones the
    /// policy would expire first, and they do not count against the
    /// candidates: what goes is exactly what `evaluate` over the unheld
    /// ones expires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn held_snapshots_of_every_owner_are_never_expired() {
        const POLICY: &str = "10s:1m; skip-empty=no";
        let (store, a, data) = solo(POLICY).await;
        let now = test_clock();
        stamp(&a, &now);
        let rows = history(&a, &a, &store, &now, 10, expiry_config().grace).await;
        hold(&a, &rows[0].id, Some("csi:content-uid")).await;
        hold(&a, &rows[1].id, Some("user:ops")).await;
        hold(&a, &rows[2].id, None).await;
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                expire_every: Duration::from_secs(3600),
                ..expiry_config()
            },
        );
        let report = sched.tick(Run::Periodic).await;
        assert!(report.error.is_none(), "{report:?}");
        let left = autos(&a);
        let mut unheld: Vec<SnapshotRow> = rows[3..].to_vec();
        record_new(&mut unheld, &a);
        let mut want: BTreeSet<String> = ids(&rows[..3]);
        want.extend(kept_ids(POLICY, data, &unheld));
        assert_eq!(ids(&left), want);
        // 10 + 1 new; 3 held; 6 of the 8 unheld kept.
        assert_eq!(sched.deps.stats.status().expired, 2);
        // Many runs later the held ones are still there.
        let sched = scheduler(&a, store.clone(), &now, expiry_config());
        for _ in 0..12 {
            advance(&now, 10 * SEC);
            sched.tick(Run::Periodic).await;
        }
        let left = autos(&a);
        for row in &rows[..3] {
            assert!(left.iter().any(|r| r.id == row.id && r.held));
        }
        assert_eq!(left.len(), 3 + 6);
    }

    /// The carried M3c gap, option (a): a leader that cannot reach the
    /// root-lease holder over P2P resigns after `resign_after` failed
    /// ticks and stays out for a lease TTL; the holder (which can always
    /// reach itself) takes over and creates. A shorter run of failures
    /// that ends in a reachable batch does not resign: no flapping on a
    /// blip.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leader_that_cannot_reach_the_holder_resigns_and_the_holder_leads() {
        let (store, a, b, data, _) = holder_b().await;
        bind(&b, &[&a], data, "10s:1m; skip-empty=no").await;
        let now = test_clock();
        let cfg = SchedConfig {
            lease_ttl_ms: 3000,
            ..expiry_config()
        };
        let sa = scheduler(&a, store.clone(), &now, cfg.clone());
        let sb = scheduler(&b, store.clone(), &now, cfg);
        assert_eq!(outcomes(&sa.tick(Run::Periodic).await), ["created"]);
        // A blip: two failed ticks, then the holder is reachable again.
        a.host.unreachable.lock().unwrap().insert(2);
        for _ in 0..2 {
            advance(&now, 10 * SEC);
            assert_eq!(outcomes(&sa.tick(Run::Periodic).await), ["failed"]);
        }
        a.host.unreachable.lock().unwrap().clear();
        advance(&now, 10 * SEC);
        assert_eq!(outcomes(&sa.tick(Run::Periodic).await), ["created"]);
        // A partition: three failed ticks in a row, and A gives up.
        a.host.unreachable.lock().unwrap().insert(2);
        for k in 0..3 {
            advance(&now, 10 * SEC);
            let report = sa.tick(Run::Periodic).await;
            assert_eq!(outcomes(&report), ["failed"]);
            assert_eq!(report.leader, k < 2, "tick {k}: {report:?}");
        }
        assert!(!sa.deps.stats.leader.load(Ordering::Relaxed));
        assert!(sa
            .deps
            .stats
            .status()
            .last_error
            .unwrap()
            .contains("resigned"));
        // A stays out; B takes the released lease at once and creates.
        let report = sa.tick(Run::Periodic).await;
        assert!(report.refused.unwrap().contains("resigned"));
        let report = sb.tick(Run::Periodic).await;
        assert!(report.leader, "{report:?}");
        assert_eq!(report.roots[0].outcome, "created", "{report:?}");
        assert_eq!(
            b.host.forwards.load(Ordering::Relaxed),
            0,
            "B ran it locally"
        );
        // A, back after its backoff, finds B leading.
        tokio::time::sleep(Duration::from_millis(3100)).await;
        sb.tick(Run::Periodic).await;
        let report = sa.tick(Run::Periodic).await;
        assert!(report.refused.unwrap().contains("another node leads"));
    }

    /// `snapshot.list`'s `KEPT BY` / `EXPIRES` source: armed roots only,
    /// unheld auto rows only, `grace` while a window holds a victim back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_listing_shows_why_each_snapshot_is_kept() {
        let (store, a, data) = solo("10s:1m; skip-empty=no").await;
        let now = test_clock();
        stamp(&a, &now);
        let rows = history(&a, &a, &store, &now, 9, Duration::from_secs(3600)).await;
        let sched = scheduler(
            &a,
            store.clone(),
            &now,
            SchedConfig {
                grace: Duration::from_secs(3600),
                ..expiry_config()
            },
        );
        hold(&a, &rows[0].id, Some("csi:x")).await;
        let listing = sched.listing().await.unwrap();
        assert!(!listing.contains_key(&rows[0].id), "held: not filled");
        // The first sighting (by `history`) is an hour's window here.
        let cell = &listing[&rows[1].id];
        assert_eq!(cell.kept_by, ["grace"]);
        assert!(cell.expires_unix_ms.unwrap() > now.load(Ordering::Relaxed));
        assert_eq!(listing[&rows[8].id].kept_by, ["10s", "last"]);
        assert_eq!(listing[&rows[5].id].kept_by, ["10s"]);
        // A scheduler with no window of its own still honours the
        // recorded one ...
        let plain = scheduler(&a, store.clone(), &now, expiry_config());
        let listing = plain.listing().await.unwrap();
        assert_eq!(listing[&rows[1].id].kept_by, ["grace"]);
        // ... and once it has closed, the victims are due now.
        advance(&now, 3600 * SEC);
        let listing = plain.listing().await.unwrap();
        assert!(listing[&rows[1].id].kept_by.is_empty());
        assert_eq!(listing[&rows[1].id].expires_unix_ms, None);
        // Paused: not armed, nothing filled.
        bind(&a, &[], data, "10s:1m; skip-empty=no; paused").await;
        assert!(sched.listing().await.unwrap().is_empty());
    }
}
