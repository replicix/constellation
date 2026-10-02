//! Plan 32 §6.2–6.3: the accounting index on a node.
//!
//! [`SnapAcct`] is the data structure; this is the engine service that
//! owns one per filesystem, in `<state dir>/snapacct/`, and keeps it equal
//! to the replica's snapshot rows and the live tree. It is advisory: GC
//! and snapshot deletion never consult it, nothing here is published, and
//! every answer says which commit it is "as of".
//!
//! ## When it works (`CONSTELLATION_SNAPACCT=auto|on|off`)
//!
//! - `off`: nothing is opened, every query answers [`SnapAnswer::Off`].
//! - `on`: the index is built at start and maintained for good.
//! - `auto` (the default): nothing at all happens until something asks
//!   for a size (a query sets *demand*). The first query creates the
//!   index and the task builds it; afterwards the node keeps maintaining
//!   it while it has a snapshot-policy root (any inode carrying
//!   `user.constellation.snapshots`, one probe of the replica's by-name
//!   xattr index) or its web UI is enabled — the daemon's HTTP listener
//!   bound, which the host reports through
//!   [`SnapAcctService::set_web_ui`]. Without either, the task catches
//!   up for each query and goes dormant again; the index stays on disk,
//!   so the next query costs only what changed since. A node that never
//!   receives a size query does no work — not even a timer — beyond
//!   waking up on a snapshot-change hint and going back to sleep.
//!
//! Demand is cleared after any pass that finishes with nothing pending,
//! even one that started before the query arrived: the query has its
//! answer by then (it waits for that pass, [`SnapAcctConfig::answer_wait`]).
//! A snapshot change after that pass is then applied only when the next
//! query (or a maintaining reason) wakes the task; until it is, that
//! query's gate sees the rows moved and answers [`SnapAnswer::Building`]
//! if the pass it starts does not finish within the wait.
//!
//! ## Building is reconciling
//!
//! There is no separate build path. Every pass compares the replica's
//! rows, grouped into chains by `SnapshotRoot.ino` and ordered by `(seq,
//! created_unix_ms)` (GC's order), with each chain the index holds, and
//! performs the operations that bring them together: delete a snapshot
//! the rows no longer have (the head's predecessor stepped back to,
//! [`SnapAcct::apply_deleted`]), append the next missing one
//! ([`ChainWalk::first`] for a chain's first snapshot, then
//! [`ChainWalk::step`]s), or — for a row that sorts before the chain's
//! head, a late-applied create — rewind and re-append the suffix
//! ([`SnapAcct::rederive_suffix`]). Deletions go first, across every
//! chain: a snapshot id names `path@name`, so a re-created `/x@s1` over
//! another directory can only be appended once the old chain has let go
//! of it. An empty index is the case where every operation is an append.
//! Each operation is one index transaction, so **the index is its own
//! resume cursor**: a restarted node continues from the snapshots already
//! applied, and a lost notification costs nothing because the next pass
//! diffs the whole row set again (O(rows), local reads). Passes are
//! budgeted (`CONSTELLATION_SNAPACCT_BUDGET_MS`, checked between
//! operations; one first walk of a large snapshot is not split).
//!
//! An operation that fails clears its chain, which is then re-applied
//! from its rows; a second failure in the same pass *stalls* the chain:
//! it is skipped for the rest of the pass (`stalled_chains` in the
//! stats, logged once) and retried on the next one, and the other chains
//! go on.
//!
//! **Never a partial number**: a query answers [`SnapAnswer::Building`]
//! (with the share of rows applied) unless the last pass matched every
//! chain to the rows and the rows have not changed since — not only
//! during the first build (`aux/built` in `snapacct_meta`), but also
//! while a chain is cleared and being re-applied, while a pass is cut
//! short by its budget, and while a chain is stalled. A missing chain
//! would not only drop its own snapshots from the totals: chunks it
//! shares would look unique to the others.
//!
//! Triggers: [`Meta::set_snapshot_change_hook`] (a row created or
//! deleted, locally or by replay) wakes the task, which debounces for
//! 100 ms; the refresh tick runs a pass as well.
//!
//! ## Live flags
//!
//! A chunk counts toward `USED` and `reclaim` only if the live tree does
//! not reference it. The live tree is never indexed: only the flag of an
//! indexed chunk is kept, and the live total comes from the replica's
//! usage counter. A chunk is live when a live inode's manifest names it
//! ([`Meta::chunk_ref_any`]) or it is a member of a spilled chunk list the
//! live tree references; `chunk_ref` rows name the list and not its
//! members, so the service records the live spilled lists and their
//! members in the index's `snapacct_lspill` keyspace.
//!
//! Every `CONSTELLATION_SNAPACCT_REFRESH_S` (60) the task diffs the
//! commit the flags are accounted against with a newer one (`Tree::diff`,
//! cost tracks the change), and for every chunk named by a changed
//! manifest — old or new, members of a list whose liveness changed
//! included — that is in the index, recomputes its flag. The commit is
//! recorded in `aux/live_root`, and it is the index's `accounted_seq`:
//! the `as_of_seq` of every answer.
//!
//! Flags are recomputed from the **replica**, so the refresh only moves
//! to a commit this replica has applied: one whose `applied` log
//! position the replica's covers (the guard `follow_head` uses, the other
//! way round). A follower behind the newest commit takes the newest one
//! it covers (looking back a few commits), or keeps its flags where they
//! are and tries again next tick (`refreshes_deferred`). Otherwise a
//! change in the diff the replica had not applied yet would be judged on
//! the old state and then never looked at again — the next diff starts
//! past it. A replica *ahead* of the commit is harmless: everything it
//! has beyond the commit is in a later diff and is looked at again.
//!
//! A refresh that cannot diff (the old root collected), a crash
//! interrupted (`aux/refresh_pending`), or that starts a new index
//! recomputes every flag from the replica instead: O(index) local reads
//! plus one GET per live spilled list not recorded yet (every file over
//! 32 MiB on a new index). Those GETs are budgeted like operations and
//! resume where they stopped (the recorded lists are the cursor); the
//! flags themselves change once, when every list is in.
//!
//! ## Physical estimates
//!
//! Chunk sizes are logical. [`SpaceBreakdown::compression_ratio`] is an
//! **estimate** from the newest GC round's census of `chunks/`
//! (`gc/summary.json`): the mean stored size of a chunk object against
//! the mean logical size of an indexed chunk, which assumes the chunks
//! snapshots hold compress like the bucket's average.

use super::{
    first_deltas, Amount, NewSnapshot, Opened, SnapAcct, SnapAcctError, SnapAcctParams, SnapNumbers,
};
use crate::snapshot::{SnapshotRoot, TreeAccess};
use crate::snapwalk::{ChainWalk, Delta};
use anyhow::{bail, Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_meta::Meta;
use constellation_mtree::keys::Key;
use constellation_mtree::record::InodeRecord;
use constellation_mtree::NodeHash;
use constellation_store_s3::{vector_covers, ChunkCensus, ChunkStore, CommitChain, SHARD0};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};

/// The index directory under the node's state directory.
pub const DIR: &str = "snapacct";

/// How long a cached GC census is reused before it is read again.
const CENSUS_TTL: Duration = Duration::from_secs(600);

/// How long the task waits after a snapshot-change hint before reading
/// the rows: a batch of creates or an expiry run is one pass.
const DEBOUNCE: Duration = Duration::from_millis(100);

const AUX_BUILT: &str = "built";
const AUX_LIVE_ROOT: &str = "live_root";
const AUX_REFRESH_PENDING: &str = "refresh_pending";
const AUX_AS_OF_MS: &str = "as_of_ms";

/// How many commits back from the newest a live refresh looks for one
/// this replica has applied (module docs).
const COVER_SEARCH: u64 = 8;

/// `CONSTELLATION_SNAPACCT` (module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapAcctMode {
    Auto,
    On,
    Off,
}

impl SnapAcctMode {
    pub fn parse(value: &str) -> Option<SnapAcctMode> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" | "" => Some(SnapAcctMode::Auto),
            "on" | "1" | "true" => Some(SnapAcctMode::On),
            "off" | "0" | "false" => Some(SnapAcctMode::Off),
            _ => None,
        }
    }

    /// `CONSTELLATION_SNAPACCT`; an unknown value reads as `auto`, with a
    /// warning.
    pub fn from_env() -> SnapAcctMode {
        match std::env::var("CONSTELLATION_SNAPACCT") {
            Err(_) => SnapAcctMode::Auto,
            Ok(value) => SnapAcctMode::parse(&value).unwrap_or_else(|| {
                tracing::warn!(%value, "CONSTELLATION_SNAPACCT is not auto, on or off; using auto");
                SnapAcctMode::Auto
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SnapAcctMode::Auto => "auto",
            SnapAcctMode::On => "on",
            SnapAcctMode::Off => "off",
        }
    }
}

/// The service's knobs.
#[derive(Clone, Debug)]
pub struct SnapAcctConfig {
    pub mode: SnapAcctMode,
    /// `CONSTELLATION_SNAPACCT_REFRESH_S` (60): the live-tree refresh and
    /// periodic reconcile cadence.
    pub refresh: Duration,
    /// `CONSTELLATION_SNAPACCT_BUDGET_MS` (500): a pass's time budget,
    /// checked between operations. While building, the task rests as long
    /// as it worked, so a build takes at most half a core.
    pub budget: Duration,
    /// An operation budget per pass on top of the time (tests).
    pub max_ops_per_pass: Option<usize>,
    /// How long a query waits for a pending pass before answering from
    /// the index as it stands (or `Building`).
    pub answer_wait: Duration,
    pub params: SnapAcctParams,
}

impl SnapAcctConfig {
    pub fn from_env() -> SnapAcctConfig {
        let number = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(default)
        };
        SnapAcctConfig {
            mode: SnapAcctMode::from_env(),
            refresh: Duration::from_secs(number("CONSTELLATION_SNAPACCT_REFRESH_S", 60).max(1)),
            budget: Duration::from_millis(number("CONSTELLATION_SNAPACCT_BUDGET_MS", 500).max(1)),
            max_ops_per_pass: None,
            answer_wait: Duration::from_secs(2),
            params: SnapAcctParams {
                gc_horizon_ms: number(
                    "CONSTELLATION_GC_HORIZON_S",
                    crate::gc::DEFAULT_GC_HORIZON_S,
                )
                .saturating_mul(1000),
                gc_interval_ms: number(
                    "CONSTELLATION_GC_INTERVAL_S",
                    crate::gc::DEFAULT_GC_INTERVAL_S,
                )
                .saturating_mul(1000),
                ..SnapAcctParams::default()
            },
        }
    }
}

/// What the service reads.
pub struct SnapAcctDeps {
    pub meta: Arc<Meta>,
    pub chunks: Arc<ChunkStore>,
    pub tree: TreeAccess,
    /// The bucket's commit chain (sealed like the node's): where the live
    /// refresh finds the newest root.
    pub commits: CommitChain,
    /// The index directory (`<state dir>/snapacct`).
    pub dir: PathBuf,
    pub fs_uuid: String,
}

/// Plan 32 Step 9's `SnapAcctStats`, plus a few counters the tests and
/// `status` use.
#[derive(Debug, Default)]
pub struct SnapAcctStats {
    /// Queries answer `Building` (module docs: never a partial number).
    pub building: AtomicBool,
    pub build_progress_pct: AtomicU64,
    pub indexed_chunks: AtomicU64,
    pub index_bytes: AtomicU64,
    pub as_of_seq: AtomicU64,
    pub refresh_ms_last: AtomicU64,
    pub verify_mismatches: AtomicU64,
    /// Chains the last pass skipped after an operation failed twice.
    pub stalled_chains: AtomicU64,
    /// Live refreshes that kept the flags at an older commit because the
    /// replica had not applied the newer ones.
    pub refreshes_deferred: AtomicU64,
    /// Passes run (each reads the rows once).
    pub passes: AtomicU64,
    /// Index operations applied: snapshots appended, deleted, re-derived
    /// suffixes, chains cleared.
    pub operations: AtomicU64,
    /// [`ChainWalk::first`] calls (full walks of a snapshot).
    pub first_walks: AtomicU64,
    /// [`ChainWalk::step`] calls.
    pub steps: AtomicU64,
    /// Live refreshes that found a newer commit.
    pub refreshes: AtomicU64,
    /// Refreshes that recomputed every flag.
    pub full_refreshes: AtomicU64,
    pub errors: AtomicU64,
    pub last_error: Mutex<Option<String>>,
}

impl SnapAcctStats {
    fn error(&self, error: &anyhow::Error) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().expect("stats lock") = Some(format!("{error:#}"));
    }
}

/// An answer, or why there is none yet.
#[derive(Clone, Debug, PartialEq)]
pub enum SnapAnswer<T> {
    Ready(T),
    /// The index does not match the snapshot rows yet (module docs):
    /// never a partial number.
    Building {
        pct: u8,
    },
    /// `CONSTELLATION_SNAPACCT=off`.
    Off,
}

impl<T> SnapAnswer<T> {
    pub fn ready(self) -> Option<T> {
        match self {
            SnapAnswer::Ready(value) => Some(value),
            _ => None,
        }
    }
}

/// One snapshot's numbers (plan 32 §6.1), logical bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Numbers {
    pub id: String,
    pub used: u64,
    pub written: u64,
    pub refer: u64,
    pub lsize: u64,
    pub as_of_seq: u64,
    pub as_of_ms: u64,
}

/// `reclaim(D)`: what deleting exactly the set returns once GC has run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimEstimate {
    pub bytes: u64,
    pub chunks: u64,
    pub as_of_seq: u64,
    pub as_of_ms: u64,
}

/// `snapshot space`'s breakdown. With a path, the snapshot buckets cover
/// the chains of directories at or under it (as the live tree places
/// them now), `live_logical` is the subtree's apparent size, and
/// `awaiting_gc` stays filesystem-wide (freed chunks belong to no chain).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpaceBreakdown {
    /// Apparent bytes of the live tree (the replica's usage counter).
    pub live_logical: u64,
    /// `usedbysnapshots`: `reclaim` of every snapshot in scope.
    pub snapshots_total: Amount,
    /// Σ `USED`: chunks exactly one snapshot holds.
    pub unique: Amount,
    /// Not live, held by two or more snapshots (and only by snapshots in
    /// scope).
    pub shared_snapshots_only: Amount,
    /// In a snapshot and in the live tree: costs nothing extra.
    pub shared_with_live: Amount,
    /// Freed by snapshot deletion, presumed not yet collected.
    pub awaiting_gc: Amount,
    /// **Estimate**: logical over stored bytes, from the newest GC
    /// round's census of `chunks/` (module docs). `None` before any GC
    /// round has written one, or with nothing indexed. Physical figures
    /// are logical ones divided by it and must be shown as `≈`.
    pub compression_ratio: Option<f64>,
    pub as_of_seq: u64,
    pub as_of_ms: u64,
}

/// What [`SnapAcctService::verify`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// Numbers that differ between the index and the brute force.
    pub mismatches: u64,
    /// One line per mismatch (the first 200).
    pub details: Vec<String>,
    pub snapshots: u64,
    /// Distinct chunks the snapshots reference.
    pub chunks: u64,
    pub as_of_seq: u64,
}

/// The commit the live flags are as of.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LiveRoot {
    seq: u64,
    root: String,
}

/// One snapshot row, as a chain member.
#[derive(Clone, Debug)]
pub(crate) struct Desired {
    pub id: String,
    pub root: SnapshotRoot,
    /// The row's `root_hash`, as the index records it.
    pub root_str: String,
}

/// The replica's rows grouped by chain directory, each chain in GC's
/// order: `(seq, created_unix_ms)`, then the root and the id so the order
/// is total. A row whose root does not parse is skipped (GC skips it
/// too).
pub(crate) fn desired_chains(meta: &Meta) -> Result<BTreeMap<Ino, Vec<Desired>>> {
    let mut chains: BTreeMap<Ino, Vec<(u64, i64, NodeHash, Desired)>> = BTreeMap::new();
    for row in meta.snapshots(None)? {
        match SnapshotRoot::parse(&row.root_hash) {
            Ok(root) => chains.entry(root.ino).or_default().push((
                root.seq,
                row.created_unix_ms,
                root.root,
                Desired {
                    id: row.id,
                    root,
                    root_str: row.root_hash,
                },
            )),
            Err(error) => {
                tracing::warn!(id = %row.id, %error, "snapshot accounting: a row with an unreadable root")
            }
        }
    }
    Ok(chains
        .into_iter()
        .map(|(ino, mut chain)| {
            chain.sort_by(|a, b| (a.0, a.1, a.2, &a.3.id).cmp(&(b.0, b.1, b.2, &b.3.id)));
            (ino, chain.into_iter().map(|(_, _, _, d)| d).collect())
        })
        .collect())
}

/// The next operation that brings one chain of the index closer to the
/// rows.
enum Op {
    Done,
    /// Delete `ord`; for the head with a predecessor, the step back.
    Delete {
        ord: u32,
        id: String,
        head_step: Option<(SnapshotRoot, SnapshotRoot)>,
    },
    /// The chain's order contradicts the rows: start it over.
    Clear,
    /// Append `want[next]` after the head (`prev` = its root).
    Append {
        next: Desired,
        prev: Option<SnapshotRoot>,
    },
    /// Rewind to `after` and re-append `suffix`.
    Rederive {
        after: Option<(u32, SnapshotRoot)>,
        head: SnapshotRoot,
        suffix: Vec<Desired>,
    },
}

fn plan(want: &[Desired], have: &[SnapNumbers]) -> Result<Op> {
    let matches = |d: &Desired, h: &SnapNumbers| d.id == h.id && d.root_str == h.root;
    // Deletions first, newest first.
    if let Some(stale) = have
        .iter()
        .rposition(|h| !want.iter().any(|d| matches(d, h)))
    {
        let head_step = match stale.checked_sub(1) {
            Some(prev) if stale == have.len() - 1 => Some((
                SnapshotRoot::parse(&have[prev].root)?,
                SnapshotRoot::parse(&have[stale].root)?,
            )),
            _ => None,
        };
        return Ok(Op::Delete {
            ord: have[stale].ord,
            id: have[stale].id.clone(),
            head_step,
        });
    }
    // Every indexed snapshot is wanted: where does each sit in the rows?
    let positions: Vec<usize> = have
        .iter()
        .map(|h| {
            want.iter()
                .position(|d| matches(d, h))
                .expect("checked above")
        })
        .collect();
    if positions.windows(2).any(|w| w[0] >= w[1]) {
        return Ok(Op::Clear);
    }
    let present: HashSet<usize> = positions.iter().copied().collect();
    let Some(next) = (0..want.len()).find(|i| !present.contains(i)) else {
        return Ok(Op::Done);
    };
    if positions.last().is_none_or(|&last| last < next) {
        return Ok(Op::Append {
            next: want[next].clone(),
            prev: have
                .last()
                .map(|h| SnapshotRoot::parse(&h.root))
                .transpose()?,
        });
    }
    // A row sorts before the head: everything from it on is re-applied.
    let after = match next.checked_sub(1) {
        Some(prev) => {
            let at = positions
                .iter()
                .position(|&p| p == prev)
                .expect("every row before the first missing one is indexed");
            Some((have[at].ord, want[prev].root))
        }
        None => None,
    };
    Ok(Op::Rederive {
        after,
        head: SnapshotRoot::parse(&have.last().expect("a later row is indexed").root)?,
        suffix: want[next..].to_vec(),
    })
}

/// A pass's allowance. The first operation of a pass is always allowed,
/// so a pass that spent its time on something else still moves.
struct Budget {
    deadline: Option<Instant>,
    ops_left: Option<usize>,
    spent: bool,
}

impl Budget {
    fn unlimited() -> Budget {
        Budget {
            deadline: None,
            ops_left: None,
            spent: false,
        }
    }

    fn exhausted(&self) -> bool {
        self.spent && (self.out_of_time() || self.ops_left == Some(0))
    }

    /// The time budget alone: what spilled-list GETs are held to (the
    /// operation budget counts index operations).
    fn out_of_time(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
    }

    fn spend(&mut self) {
        self.spent = true;
        if let Some(ops) = &mut self.ops_left {
            *ops = ops.saturating_sub(1);
        }
    }
}

/// The index of one filesystem on this node, and its task.
pub struct SnapAcctService {
    cfg: SnapAcctConfig,
    deps: SnapAcctDeps,
    /// Shared with the blocking task that opens (or wipes) the index.
    index: Arc<Mutex<Option<Arc<SnapAcct>>>>,
    /// Serializes passes (and `verify`, which must not interleave).
    work: tokio::sync::Mutex<()>,
    wake: Arc<Notify>,
    /// Bumped after every pass that finished with nothing pending.
    caught_up: watch::Sender<u64>,
    /// `auto`: a query arrived since the last caught-up pass.
    demand: AtomicBool,
    web_ui: AtomicBool,
    stats: Arc<SnapAcctStats>,
    last_refresh: Arc<Mutex<Option<Instant>>>,
    /// `Meta::snapshot_gen` when the last complete reconcile started.
    reconciled_gen: AtomicU64,
    /// The last pass matched every chain to the rows.
    complete: AtomicBool,
    /// Stalled chains already warned about (cleared when they recover).
    warned: Mutex<HashSet<Ino>>,
    census: Mutex<Option<(Instant, Option<ChunkCensus>)>>,
    wipe_pending: Arc<AtomicBool>,
}

/// How far [`SnapAcctService::reconcile`] got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reconciled {
    /// Every chain matches the rows.
    Complete,
    /// The budget ran out; the next pass continues.
    OutOfBudget,
    /// Every chain was visited, and some could not be applied.
    Stalled,
}

/// How far one chain got in a pass.
enum Advance {
    Done,
    OutOfBudget,
    Stalled,
}

impl SnapAcctService {
    /// The service, idle: [`SnapAcctService::run`] is its task. Registers
    /// the snapshot-change hint on `deps.meta` (replacing any earlier
    /// one).
    pub fn new(cfg: SnapAcctConfig, deps: SnapAcctDeps) -> Arc<SnapAcctService> {
        let wake = Arc::new(Notify::new());
        if cfg.mode != SnapAcctMode::Off {
            let wake = wake.clone();
            deps.meta
                .set_snapshot_change_hook(Some(Arc::new(move || wake.notify_one())));
        }
        let stats = SnapAcctStats {
            // `on` builds from the start: say so before the first pass.
            building: AtomicBool::new(cfg.mode == SnapAcctMode::On),
            ..SnapAcctStats::default()
        };
        Arc::new(SnapAcctService {
            cfg,
            deps,
            index: Arc::new(Mutex::new(None)),
            work: tokio::sync::Mutex::new(()),
            wake,
            caught_up: watch::channel(0).0,
            demand: AtomicBool::new(false),
            web_ui: AtomicBool::new(false),
            stats: Arc::new(stats),
            last_refresh: Arc::new(Mutex::new(None)),
            reconciled_gen: AtomicU64::new(u64::MAX),
            complete: AtomicBool::new(false),
            warned: Mutex::new(HashSet::new()),
            census: Mutex::new(None),
            wipe_pending: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn mode(&self) -> SnapAcctMode {
        self.cfg.mode
    }

    pub fn stats(&self) -> &Arc<SnapAcctStats> {
        &self.stats
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.deps.dir
    }

    /// The host's web UI is (or is no longer) serving: under `auto`, a
    /// reason to keep the index maintained once it exists.
    pub fn set_web_ui(&self, enabled: bool) {
        self.web_ui.store(enabled, Ordering::Relaxed);
        self.wake.notify_one();
    }

    /// Whether the task should work now (module docs).
    pub fn maintaining(&self) -> bool {
        match self.cfg.mode {
            SnapAcctMode::Off => false,
            SnapAcctMode::On => true,
            SnapAcctMode::Auto => {
                self.demand.load(Ordering::Relaxed)
                    || (self.index_exists()
                        && (self.web_ui.load(Ordering::Relaxed) || self.policy_root()))
            }
        }
    }

    fn index_exists(&self) -> bool {
        self.deps.dir.join(super::MARKER).exists()
    }

    fn policy_root(&self) -> bool {
        self.deps
            .meta
            .any_xattr_named(constellation_meta::SNAPSHOT_POLICY_XATTR)
            .unwrap_or(false)
    }

    /// The index, opened (and created) on first use. A wipe asked for by
    /// [`Self::wipe`] happens here, once nothing else holds the index;
    /// `None` while something still does. Opening and wiping (fjall
    /// recovery, directory removal) run off the async runtime.
    async fn open(&self) -> Result<Option<Arc<SnapAcct>>> {
        if !self.wipe_pending.load(Ordering::Acquire) {
            if let Some(ix) = self.index.lock().expect("index lock").as_ref() {
                return Ok(Some(ix.clone()));
            }
        }
        let slot = self.index.clone();
        let wipe = self.wipe_pending.clone();
        let last_refresh = self.last_refresh.clone();
        let dir = self.deps.dir.clone();
        let fs_uuid = self.deps.fs_uuid.clone();
        let params = self.cfg.params;
        blocking(move || open_index(&slot, &wipe, &last_refresh, &dir, &fs_uuid, params)).await
    }

    /// Ask for the index to be deleted and rebuilt: the recovery for an
    /// index that contradicts itself. It is dropped and removed by the
    /// next [`Self::open`] that finds nobody else holding it (never under
    /// a pass that still has it open).
    pub(crate) fn wipe(&self) {
        self.wipe_pending.store(true, Ordering::Release);
    }

    // -------------------------------------------------------------- task

    /// The background task: until `stop`, run passes while
    /// [`Self::maintaining`], otherwise sleep until woken.
    pub async fn run(
        self: Arc<Self>,
        stop: Arc<AtomicBool>,
        gate: Option<Arc<crate::lifecycle::BackgroundGate>>,
    ) {
        if self.cfg.mode == SnapAcctMode::Off {
            return;
        }
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            if let Some(gate) = &gate {
                gate.wait_active().await;
            }
            if !self.maintaining() {
                // Dormant. With an index on disk a policy root may
                // appear, so look again on the refresh cadence; without
                // one only a query (or `set_web_ui`) can start anything.
                if self.cfg.mode == SnapAcctMode::Auto && self.index_exists() {
                    tokio::select! {
                        _ = self.wake.notified() => {}
                        _ = tokio::time::sleep(self.cfg.refresh) => {}
                    }
                } else {
                    self.wake.notified().await;
                }
                continue;
            }
            let budget = Budget {
                deadline: Some(Instant::now() + self.cfg.budget),
                ops_left: self.cfg.max_ops_per_pass,
                spent: false,
            };
            match self.pass(budget, false).await {
                Ok(true) => tokio::time::sleep(self.cfg.budget).await,
                Ok(false) => {
                    self.demand.store(false, Ordering::Relaxed);
                    if !self.maintaining() {
                        continue;
                    }
                    let due = self.refresh_due_in();
                    tokio::select! {
                        _ = self.wake.notified() => tokio::time::sleep(DEBOUNCE).await,
                        _ = tokio::time::sleep(due) => {}
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), "snapshot accounting pass failed");
                    self.stats.error(&error);
                    tokio::select! {
                        _ = self.wake.notified() => {}
                        _ = tokio::time::sleep(self.cfg.refresh) => {}
                    }
                }
            }
        }
    }

    fn refresh_due_in(&self) -> Duration {
        match *self.last_refresh.lock().expect("refresh lock") {
            Some(at) => self.cfg.refresh.saturating_sub(at.elapsed()),
            None => Duration::ZERO,
        }
    }

    /// Run passes without a budget until the index is current, the live
    /// refresh included (forced). For tests and `verify`.
    pub async fn catch_up(&self) -> Result<()> {
        if self.cfg.mode == SnapAcctMode::Off {
            bail!("snapshot accounting is off (CONSTELLATION_SNAPACCT=off)");
        }
        while self.pass(Budget::unlimited(), true).await? {}
        Ok(())
    }

    /// One budgeted pass (tests drive the build step by step with it).
    /// `Ok(true)`: work remains.
    pub async fn step(&self, max_ops: usize) -> Result<bool> {
        self.pass(
            Budget {
                deadline: None,
                ops_left: Some(max_ops),
                spent: false,
            },
            false,
        )
        .await
    }

    /// One pass held to a time budget alone (tests).
    #[cfg(test)]
    pub(crate) async fn step_for(&self, time: Duration) -> Result<bool> {
        self.pass(
            Budget {
                deadline: Some(Instant::now() + time),
                ops_left: None,
                spent: false,
            },
            false,
        )
        .await
    }

    async fn pass(&self, budget: Budget, force_refresh: bool) -> Result<bool> {
        let _work = self.work.lock().await;
        self.pass_locked(budget, force_refresh).await
    }

    async fn pass_locked(&self, mut budget: Budget, force_refresh: bool) -> Result<bool> {
        self.stats.passes.fetch_add(1, Ordering::Relaxed);
        let Some(ix) = self.open().await? else {
            // A wipe waits for a query to let go of the index.
            return Ok(true);
        };
        let generation = self.deps.meta.snapshot_gen();
        let built = ix.aux(AUX_BUILT)?.is_some();
        // The live flags' starting point comes first: snapshots applied
        // below take their chunks' flags from it.
        if ix.aux(AUX_LIVE_ROOT)?.is_none() || ix.aux(AUX_REFRESH_PENDING)?.is_some() {
            if !self.refresh_full(&ix, &mut budget).await? {
                self.complete.store(false, Ordering::Relaxed);
                self.stats.building.store(true, Ordering::Relaxed);
                return Ok(true);
            }
            *self.last_refresh.lock().expect("refresh lock") = Some(Instant::now());
        }
        let walk = ChainWalk::new(self.deps.tree.clone(), self.deps.chunks.clone());
        let outcome = self.reconcile(&ix, &walk, &mut budget).await?;
        let complete = outcome == Reconciled::Complete;
        self.complete.store(complete, Ordering::Relaxed);
        self.stats.building.store(!complete, Ordering::Relaxed);
        if outcome == Reconciled::OutOfBudget {
            return Ok(true);
        }
        if complete {
            if !built {
                ix.put_aux(AUX_BUILT, b"1")?;
            }
            self.reconciled_gen.store(generation, Ordering::Relaxed);
        }
        // A stalled pass still keeps the flags current: the rest of the
        // index is right, and answers resume once the chain applies.
        let due = force_refresh || self.refresh_due_in().is_zero();
        if due && !self.refresh_live(&ix, &mut budget).await? {
            return Ok(true);
        }
        let expired = {
            let ix = ix.clone();
            blocking(move || Ok(ix.expire_tombstones()?)).await?
        };
        if expired > 0 {
            tracing::debug!(
                expired,
                "snapshot accounting: tombstones presumed collected"
            );
        }
        ix.put_aux(AUX_AS_OF_MS, &now_ms().to_be_bytes())?;
        self.publish_stats(&ix)?;
        self.caught_up.send_modify(|n| *n += 1);
        Ok(false)
    }

    fn publish_stats(&self, ix: &SnapAcct) -> Result<()> {
        let fs = ix.fs_breakdown()?;
        self.stats.indexed_chunks.store(
            fs.unique.chunks + fs.shared_only.chunks + fs.shared_with_live.chunks,
            Ordering::Relaxed,
        );
        self.stats
            .index_bytes
            .store(ix.footprint_bytes()?, Ordering::Relaxed);
        self.stats
            .as_of_seq
            .store(ix.accounted_seq()?, Ordering::Relaxed);
        Ok(())
    }

    // --------------------------------------------------------- reconcile

    /// Bring every chain closer to the rows until the budget runs out:
    /// first every deletion, then everything else (module docs).
    async fn reconcile(
        &self,
        ix: &Arc<SnapAcct>,
        walk: &ChainWalk,
        budget: &mut Budget,
    ) -> Result<Reconciled> {
        let desired = {
            let meta = self.deps.meta.clone();
            blocking(move || desired_chains(&meta)).await?
        };
        let mut dirs: BTreeSet<Ino> = desired.keys().copied().collect();
        dirs.extend(ix.chains()?.into_iter().map(|(_, ino)| ino));
        let empty = Vec::new();
        let mut stalled = BTreeSet::new();
        let mut out_of_budget = false;
        'phases: for deletions_only in [true, false] {
            for &dir in &dirs {
                if stalled.contains(&dir) {
                    continue;
                }
                let want = desired.get(&dir).unwrap_or(&empty);
                match self
                    .advance(ix, walk, dir, want, budget, deletions_only)
                    .await?
                {
                    Advance::Done => {}
                    Advance::OutOfBudget => {
                        out_of_budget = true;
                        break 'phases;
                    }
                    Advance::Stalled => {
                        stalled.insert(dir);
                    }
                }
            }
        }
        if !out_of_budget {
            self.stats
                .stalled_chains
                .store(stalled.len() as u64, Ordering::Relaxed);
            let mut warned = self.warned.lock().expect("warned lock");
            for dir in warned.iter().filter(|d| !stalled.contains(d)) {
                tracing::info!(dir, "snapshot accounting: a stalled chain applies again");
            }
            warned.retain(|dir| stalled.contains(dir));
        }
        self.progress(ix, &desired)?;
        Ok(if out_of_budget {
            Reconciled::OutOfBudget
        } else if stalled.is_empty() {
            Reconciled::Complete
        } else {
            Reconciled::Stalled
        })
    }

    /// Apply operations to the chain of `dir` until it matches `want` (or,
    /// with `deletions_only`, until nothing in it is to be deleted).
    async fn advance(
        &self,
        ix: &Arc<SnapAcct>,
        walk: &ChainWalk,
        dir: Ino,
        want: &[Desired],
        budget: &mut Budget,
        deletions_only: bool,
    ) -> Result<Advance> {
        let mut failures = 0;
        loop {
            let chain = match ix.chain_of(dir)? {
                Some(chain) => chain,
                None if want.is_empty() || deletions_only => return Ok(Advance::Done),
                None => ix.register_chain(dir)?,
            };
            let have = ix.chain_snapshots(chain)?;
            let op = plan(want, &have)?;
            match op {
                Op::Done => return Ok(Advance::Done),
                Op::Delete { .. } => {}
                _ if deletions_only => return Ok(Advance::Done),
                _ => {}
            }
            if budget.exhausted() {
                return Ok(Advance::OutOfBudget);
            }
            budget.spend();
            let Err(error) = self.execute(ix, walk, chain, op).await else {
                self.stats.operations.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            failures += 1;
            self.stats.error(&error);
            let known = self.warned.lock().expect("warned lock").contains(&dir);
            if failures > 1 {
                // Twice in one pass is not a glitch: skip the chain until
                // the next pass, and let the others go on.
                if self.warned.lock().expect("warned lock").insert(dir) {
                    tracing::warn!(
                        dir,
                        chain,
                        error = %format!("{error:#}"),
                        "snapshot accounting: a chain cannot be applied; skipping it, \
                         and answering `building` until it can be"
                    );
                } else {
                    tracing::debug!(dir, chain, error = %format!("{error:#}"), "snapshot accounting: a chain is still stalled");
                }
                return Ok(Advance::Stalled);
            }
            // Derived state: a chain the index cannot move forward is
            // cleared and re-applied from its rows.
            if known {
                tracing::debug!(dir, chain, error = %format!("{error:#}"), "snapshot accounting: rebuilding a stalled chain");
            } else {
                tracing::warn!(
                    dir,
                    chain,
                    error = %format!("{error:#}"),
                    "snapshot accounting: rebuilding a chain"
                );
            }
            let cleared = {
                let ix = ix.clone();
                blocking(move || Ok(ix.clear_chain(chain, None)?)).await
            };
            if let Err(clear) = cleared {
                self.wipe();
                return Err(clear.context("clearing a chain failed; the index will be rebuilt"));
            }
        }
    }

    /// The share of rows the index holds, into the stats.
    fn progress(&self, ix: &SnapAcct, desired: &BTreeMap<Ino, Vec<Desired>>) -> Result<()> {
        let total: usize = desired.values().map(Vec::len).sum();
        let mut done = 0usize;
        for (dir, want) in desired {
            if let Some(chain) = ix.chain_of(*dir)? {
                let have = ix.chain_snapshots(chain)?;
                done += want
                    .iter()
                    .filter(|d| have.iter().any(|h| h.id == d.id && h.root == d.root_str))
                    .count();
            }
        }
        let pct = (done * 100).checked_div(total).unwrap_or(100);
        self.stats
            .build_progress_pct
            .store(pct as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn first(&self, walk: &ChainWalk, root: SnapshotRoot) -> Result<(u64, Vec<Delta>)> {
        self.stats.first_walks.fetch_add(1, Ordering::Relaxed);
        let occurrences = walk.first(root).await?;
        Ok((occurrences.lsize(), first_deltas(&occurrences).collect()))
    }

    async fn step_between(
        &self,
        walk: &ChainWalk,
        prev: SnapshotRoot,
        next: SnapshotRoot,
    ) -> Result<(i64, Vec<Delta>)> {
        self.stats.steps.fetch_add(1, Ordering::Relaxed);
        let deltas = walk.step(prev, next).await?;
        Ok((deltas.lsize_delta, deltas.iter().collect()))
    }

    async fn execute(
        &self,
        ix: &Arc<SnapAcct>,
        walk: &ChainWalk,
        chain: u32,
        op: Op,
    ) -> Result<()> {
        let mut live = self.oracle(ix);
        match op {
            Op::Done => Ok(()),
            Op::Clear => {
                let ix = ix.clone();
                blocking(move || Ok(ix.clear_chain(chain, None)?)).await
            }
            Op::Delete { ord, id, head_step } => {
                let step = match head_step {
                    Some((prev, head)) => Some(
                        self.step_between(walk, prev, head)
                            .await
                            .with_context(|| format!("stepping back from deleted head {id}"))?
                            .1,
                    ),
                    None => None,
                };
                let ix = ix.clone();
                blocking(move || Ok(ix.apply_deleted(chain, ord, step, None)?)).await
            }
            Op::Append { next, prev } => {
                let ix = ix.clone();
                match prev {
                    None => {
                        let (lsize, occurrences) = self.first(walk, next.root).await?;
                        blocking(move || {
                            ix.apply_first(
                                chain,
                                &next.id,
                                &next.root_str,
                                lsize,
                                occurrences,
                                &mut live,
                                None,
                            )?;
                            Ok(())
                        })
                        .await
                    }
                    Some(prev) => {
                        let (lsize_delta, deltas) =
                            self.step_between(walk, prev, next.root).await?;
                        blocking(move || {
                            ix.apply_created(
                                chain,
                                &next.id,
                                &next.root_str,
                                lsize_delta,
                                deltas,
                                &mut live,
                                None,
                            )?;
                            Ok(())
                        })
                        .await
                    }
                }
            }
            Op::Rederive {
                after,
                head,
                suffix,
            } => {
                let rewind = match after {
                    Some((_, root)) if root != head => self.step_between(walk, root, head).await?.1,
                    _ => Vec::new(),
                };
                let mut snapshots = Vec::with_capacity(suffix.len());
                let mut pred = after.map(|(_, root)| root);
                for d in suffix {
                    let (lsize_delta, deltas) = match pred {
                        None => {
                            let (lsize, deltas) = self.first(walk, d.root).await?;
                            (lsize as i64, deltas)
                        }
                        Some(prev) => self.step_between(walk, prev, d.root).await?,
                    };
                    pred = Some(d.root);
                    snapshots.push(NewSnapshot {
                        id: d.id,
                        root: d.root_str,
                        lsize_delta,
                        deltas,
                    });
                }
                let ix = ix.clone();
                blocking(move || {
                    ix.rederive_suffix(
                        chain,
                        after.map(|(ord, _)| ord),
                        rewind,
                        snapshots,
                        &mut live,
                        None,
                    )?;
                    Ok(())
                })
                .await
            }
        }
    }

    /// Whether the live tree references `hash`, as the index needs it for
    /// a chunk entering it. An error reads as live: a chunk wrongly live
    /// is shown as shared rather than reclaimable, the safe mistake.
    fn oracle(&self, ix: &Arc<SnapAcct>) -> impl FnMut(&ChunkHash) -> bool + Send + 'static {
        let meta = self.deps.meta.clone();
        let ix = ix.clone();
        move |hash| is_live(&meta, &ix, hash)
    }

    // ------------------------------------------------------ live refresh

    /// The newest commit in `(floor, newest]` whose `applied` position
    /// this replica's (`mine`) covers, looking back at most
    /// [`COVER_SEARCH`] commits: the newest state whose every change the
    /// replica has (module docs).
    async fn covered_commit(
        &self,
        newest: u64,
        floor: u64,
        mine: u64,
    ) -> Result<Option<(u64, NodeHash)>> {
        let lowest = newest.saturating_sub(COVER_SEARCH - 1).max(floor + 1);
        for seq in (lowest..=newest).rev() {
            let Some(commit) = self.deps.commits.get(seq).await? else {
                continue;
            };
            if vector_covers(mine, commit.applied) {
                let root = commit
                    .root(SHARD0)
                    .with_context(|| format!("commit {seq} names no shard 0 root"))?;
                return Ok(Some((seq, root)));
            }
        }
        Ok(None)
    }

    /// Diff the accounted commit with the newest one this replica has
    /// applied and refresh the flags of every indexed chunk a changed
    /// manifest names (module docs). `Ok(false)`: it fell back to a full
    /// recompute that the budget cut short (resumed by the next pass).
    async fn refresh_live(&self, ix: &Arc<SnapAcct>, budget: &mut Budget) -> Result<bool> {
        let started = Instant::now();
        let state: Option<LiveRoot> = ix
            .aux(AUX_LIVE_ROOT)?
            .map(|v| postcard::from_bytes(&v))
            .transpose()
            .context("the accounting index's live root")?;
        let known = state.as_ref().map_or(0, |s| s.seq);
        // Read before the flags are: the replica only moves forward.
        let mine = self.deps.meta.applied_seq()?;
        // Nothing newer: the probe alone (no commit GET).
        let head = match self.deps.commits.discover_head(known).await? {
            Some(seq) if seq > known => {
                let covered = self.covered_commit(seq, known, mine).await?;
                if covered.is_none() {
                    self.stats
                        .refreshes_deferred
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        head = seq,
                        applied = mine,
                        "snapshot accounting: the replica has not applied the newer commits; \
                         keeping the live flags at {known}"
                    );
                }
                covered
            }
            _ => None,
        };
        let Some((seq, root)) = head else {
            *self.last_refresh.lock().expect("refresh lock") = Some(Instant::now());
            return Ok(true);
        };
        let old = state
            .as_ref()
            .filter(|s| s.seq != 0)
            .and_then(|s| NodeHash::from_hex(&s.root));
        ix.put_aux(AUX_REFRESH_PENDING, &seq.to_be_bytes())?;
        let diffed = match old {
            Some(old) => self.refresh_by_diff(ix, old, root).await,
            None => Err(anyhow::anyhow!("no accounted commit to diff from")),
        };
        if let Err(error) = diffed {
            tracing::debug!(error = %format!("{error:#}"), "snapshot accounting: recomputing every live flag");
            // `refresh_pending` is set: an interrupted recompute resumes
            // as the next pass's first step.
            let done = self.refresh_full(ix, budget).await?;
            if done {
                *self.last_refresh.lock().expect("refresh lock") = Some(Instant::now());
            }
            return Ok(done);
        }
        self.set_live_root(ix, seq, root)?;
        self.stats.refreshes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .refresh_ms_last
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        *self.last_refresh.lock().expect("refresh lock") = Some(Instant::now());
        Ok(true)
    }

    fn set_live_root(&self, ix: &SnapAcct, seq: u64, root: NodeHash) -> Result<()> {
        ix.put_aux(
            AUX_LIVE_ROOT,
            &postcard::to_allocvec(&LiveRoot {
                seq,
                root: root.to_hex(),
            })?,
        )?;
        if seq > ix.accounted_seq()? {
            ix.set_accounted_seq(seq)?;
        }
        ix.remove_aux(AUX_REFRESH_PENDING)?;
        Ok(())
    }

    async fn refresh_by_diff(
        &self,
        ix: &Arc<SnapAcct>,
        old: NodeHash,
        new: NodeHash,
    ) -> Result<()> {
        let tree = self.deps.tree.clone();
        let (direct, spills) = tokio::task::spawn_blocking(move || -> Result<_> {
            let handle = tokio::runtime::Handle::current();
            let resolver = crate::mtree_read::Resolver {
                blobs: &tree.blobs,
                handle: &handle,
            };
            let t = tree.tree()?;
            let mut direct = HashSet::new();
            let mut spills = HashSet::new();
            for (key, _) in t.diff(&old, &new)? {
                if !matches!(Key::parse(&key)?, Key::Inode { .. }) {
                    continue;
                }
                for root in [&old, &new] {
                    let Some(value) = t.get(root, &key)? else {
                        continue;
                    };
                    let record = InodeRecord::decode(&value)?;
                    let Some(payload) = &record.manifest else {
                        continue;
                    };
                    let manifest = Manifest::decode(&resolver.payload(payload)?)?;
                    match manifest.chunks {
                        ChunkInfo::Inline(chunks) => direct.extend(chunks.into_values()),
                        ChunkInfo::Spilled(spill) => {
                            direct.insert(spill);
                            spills.insert(spill);
                        }
                    }
                }
            }
            Ok((direct, spills))
        })
        .await
        .context("live refresh diff task")??;
        let mut candidates = direct;
        for spill in spills {
            let live = self.deps.meta.chunk_ref_any(&spill)?;
            let known = ix.live_spill_known(&spill)?;
            if live == known {
                continue;
            }
            // The list's members change liveness with it.
            let members = match self.spill_members(&spill).await {
                Ok(members) => members,
                Err(error) if known => {
                    tracing::debug!(error = %format!("{error:#}"), "snapshot accounting: a dropped spilled list is gone; using the recorded members");
                    ix.live_spill_members(&spill)?
                }
                Err(error) => return Err(error),
            };
            candidates.extend(members.iter().copied());
            if live {
                ix.record_live_spill(&spill, members)?;
            } else {
                ix.forget_live_spill(&spill, members)?;
            }
        }
        let ix = ix.clone();
        let meta = self.deps.meta.clone();
        blocking(move || {
            let mut updates = Vec::new();
            for hash in candidates {
                if let Some(entry) = ix.chunk_entry(&hash)? {
                    let live = is_live(&meta, &ix, &hash);
                    if live != entry.live {
                        updates.push((hash, live));
                    }
                }
            }
            ix.set_live_many(updates, None)?;
            Ok(())
        })
        .await
    }

    /// Record every live spilled list from the replica and recompute
    /// every indexed chunk's flag: the starting point of a new index, and
    /// the fallback of a refresh that cannot diff. `Ok(false)`: the
    /// budget ran out while fetching spilled lists (`refresh_pending`
    /// stays set, and the next pass resumes with the lists not recorded
    /// yet).
    async fn refresh_full(&self, ix: &Arc<SnapAcct>, budget: &mut Budget) -> Result<bool> {
        let started = Instant::now();
        ix.put_aux(AUX_REFRESH_PENDING, &[])?;
        // Read before the flags are: the commit the flags are labelled
        // with must be one the replica had applied by then, so that every
        // change after it is in a later diff (module docs).
        let mine = self.deps.meta.applied_seq()?;
        if !self.recompute_flags(ix, budget).await? {
            return Ok(false);
        }
        let head = match self.deps.commits.discover_head(0).await? {
            Some(seq) => self.covered_commit(seq, 0, mine).await?,
            None => None,
        };
        match head {
            Some((seq, root)) => self.set_live_root(ix, seq, root)?,
            None => {
                // No commit yet, or none this replica has applied: no
                // commit to diff from, so the next refresh recomputes in
                // full again.
                ix.put_aux(
                    AUX_LIVE_ROOT,
                    &postcard::to_allocvec(&LiveRoot {
                        seq: 0,
                        root: String::new(),
                    })?,
                )?;
                ix.remove_aux(AUX_REFRESH_PENDING)?;
            }
        }
        self.stats
            .refresh_ms_last
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(true)
    }

    /// The flags from the replica (module docs). The spilled lists
    /// already recorded are the resume cursor: only lists the live tree
    /// references and the index lacks are fetched, one GET each, until
    /// the time budget runs out (`Ok(false)`, after at least one); the
    /// flags change once every list is in.
    async fn recompute_flags(&self, ix: &Arc<SnapAcct>, budget: &mut Budget) -> Result<bool> {
        let (wanted, known) = {
            let meta = self.deps.meta.clone();
            let ix = ix.clone();
            blocking(move || {
                let mut wanted = HashSet::new();
                for bytes in meta.live_manifests()? {
                    if let ChunkInfo::Spilled(spill) = Manifest::decode(&bytes)?.chunks {
                        wanted.insert(spill);
                    }
                }
                ix.retain_live_spills(&wanted)?;
                let known = ix.live_spills()?;
                Ok((wanted, known))
            })
            .await?
        };
        // At least one list per call, so a resumed recompute always moves.
        for (i, spill) in wanted.difference(&known).enumerate() {
            if i > 0 && budget.out_of_time() {
                return Ok(false);
            }
            let members = self.spill_members(spill).await?;
            ix.record_live_spill(spill, members)?;
        }
        self.stats.full_refreshes.fetch_add(1, Ordering::Relaxed);
        let ix = ix.clone();
        let meta = self.deps.meta.clone();
        blocking(move || {
            let mut updates = Vec::new();
            ix.for_each_chunk(|hash, entry| {
                let live = is_live(&meta, &ix, &hash);
                if live != entry.live {
                    updates.push((hash, live));
                }
                Ok(())
            })?;
            ix.set_live_many(updates, None)?;
            Ok(true)
        })
        .await
    }

    async fn spill_members(&self, spill: &ChunkHash) -> Result<Vec<ChunkHash>> {
        let bytes = self
            .deps
            .chunks
            .get_chunk(spill)
            .await
            .with_context(|| format!("spilled chunk list {}", spill.to_hex()))?;
        Ok(decode_chunk_list(&bytes)?.into_values().collect())
    }

    // ----------------------------------------------------------- queries

    /// Gate a query: `Off`, `Building`, or the index. Records demand and
    /// waits (up to `answer_wait`) for a pass when something is pending.
    async fn gate(&self) -> Result<Result<Arc<SnapAcct>, SnapAnswer<()>>> {
        if self.cfg.mode == SnapAcctMode::Off {
            return Ok(Err(SnapAnswer::Off));
        }
        self.demand.store(true, Ordering::Relaxed);
        let mut caught_up = self.caught_up.subscribe();
        caught_up.mark_unchanged();
        self.wake.notify_one();
        let Some(ix) = self.open().await? else {
            // A wipe waits for queries to let go: the rebuild is next.
            return Ok(Err(SnapAnswer::Building { pct: 0 }));
        };
        if !self.current(&ix)? && !self.cfg.answer_wait.is_zero() {
            let _ = tokio::time::timeout(self.cfg.answer_wait, caught_up.changed()).await;
        }
        if !self.current(&ix)? {
            return Ok(Err(self.building()));
        }
        Ok(Ok(ix))
    }

    /// Whether the index matches the rows (module docs): built, the last
    /// pass complete, and no snapshot row changed since it started.
    fn current(&self, ix: &SnapAcct) -> Result<bool> {
        Ok(ix.aux(AUX_BUILT)?.is_some()
            && self.complete.load(Ordering::Relaxed)
            && self.deps.meta.snapshot_gen() == self.reconciled_gen.load(Ordering::Relaxed))
    }

    fn building(&self) -> SnapAnswer<()> {
        SnapAnswer::Building {
            pct: self
                .stats
                .build_progress_pct
                .load(Ordering::Relaxed)
                .min(99) as u8,
        }
    }

    fn as_of(&self, ix: &SnapAcct) -> Result<(u64, u64)> {
        let ms = ix
            .aux(AUX_AS_OF_MS)?
            .and_then(|v| v.try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0);
        Ok((ix.accounted_seq()?, ms))
    }

    /// Where snapshot `id` sits in the index; `Err(Building)` when the
    /// replica has the row but the index has not applied it yet.
    fn locate(&self, ix: &SnapAcct, id: &str) -> Result<Result<(u32, u32), SnapAnswer<()>>> {
        if let Some(at) = ix.locate(id)? {
            return Ok(Ok(at));
        }
        if self.deps.meta.snapshot_by_id(id)?.is_some() {
            self.wake.notify_one();
            return Ok(Err(self.building()));
        }
        bail!("no snapshot with id {id}")
    }

    /// One snapshot's `USED`/`WRITTEN`/`REFER`/`LSIZE`.
    pub async fn snap_numbers(&self, id: &str) -> Result<SnapAnswer<Numbers>> {
        let ix = match self.gate().await? {
            Ok(ix) => ix,
            Err(other) => return Ok(cast(other)),
        };
        let (chain, ord) = match self.locate(&ix, id)? {
            Ok(at) => at,
            Err(other) => return Ok(cast(other)),
        };
        let numbers = ix.snap_numbers(chain, ord)?.ok_or_else(|| {
            SnapAcctError::Corrupt(format!("snapshot {id} located but not recorded"))
        })?;
        let (as_of_seq, as_of_ms) = self.as_of(&ix)?;
        Ok(SnapAnswer::Ready(Numbers {
            id: numbers.id,
            used: numbers.used,
            written: numbers.written,
            refer: numbers.refer,
            lsize: numbers.lsize,
            as_of_seq,
            as_of_ms,
        }))
    }

    /// Every listed snapshot's numbers under one gate: what a snapshot
    /// listing shows (`snapshot.list`). Ids without a row any more are
    /// left out; a row the index has not applied yet makes the whole
    /// answer `Building`, so a listing never mixes figures from before
    /// and after a change.
    ///
    /// With `demand`, this is a size request like the others (it records
    /// demand, starts a build under `auto`, waits for a pending pass).
    /// Without, it only *peeks*: the numbers when the index is open and
    /// current right now, `Building` otherwise, and no work is caused —
    /// a plain listing must not start a build.
    pub async fn snap_numbers_many(
        &self,
        ids: &[String],
        demand: bool,
    ) -> Result<SnapAnswer<HashMap<String, Numbers>>> {
        let ix = if demand {
            match self.gate().await? {
                Ok(ix) => ix,
                Err(other) => return Ok(cast(other)),
            }
        } else {
            if self.cfg.mode == SnapAcctMode::Off {
                return Ok(SnapAnswer::Off);
            }
            let open = if self.wipe_pending.load(Ordering::Acquire) {
                None
            } else {
                self.index.lock().expect("index lock").clone()
            };
            match open {
                Some(ix) if self.current(&ix)? => ix,
                _ => return Ok(cast(self.building())),
            }
        };
        let (as_of_seq, as_of_ms) = self.as_of(&ix)?;
        let mut out = HashMap::with_capacity(ids.len());
        for id in ids {
            let Some((chain, ord)) = ix.locate(id)? else {
                if self.deps.meta.snapshot_by_id(id)?.is_some() {
                    if demand {
                        self.wake.notify_one();
                    }
                    return Ok(cast(self.building()));
                }
                continue;
            };
            let numbers = ix.snap_numbers(chain, ord)?.ok_or_else(|| {
                SnapAcctError::Corrupt(format!("snapshot {id} located but not recorded"))
            })?;
            out.insert(
                id.clone(),
                Numbers {
                    id: numbers.id,
                    used: numbers.used,
                    written: numbers.written,
                    refer: numbers.refer,
                    lsize: numbers.lsize,
                    as_of_seq,
                    as_of_ms,
                },
            );
        }
        Ok(SnapAnswer::Ready(out))
    }

    /// The GC horizon the "awaiting GC" bucket assumes
    /// (`CONSTELLATION_GC_HORIZON_S`): freed chunks are presumed collected
    /// one horizon plus one GC interval after they lost their last
    /// snapshot.
    pub fn gc_horizon_ms(&self) -> u64 {
        self.cfg.params.gc_horizon_ms
    }

    /// `reclaim(D)` for the snapshots with these ids.
    pub async fn reclaim(&self, ids: &[String]) -> Result<SnapAnswer<ReclaimEstimate>> {
        let ix = match self.gate().await? {
            Ok(ix) => ix,
            Err(other) => return Ok(cast(other)),
        };
        let mut at = Vec::with_capacity(ids.len());
        for id in ids {
            match self.locate(&ix, id)? {
                Ok(place) => at.push(place),
                Err(other) => return Ok(cast(other)),
            }
        }
        let amount = if at.is_empty() {
            Amount::default()
        } else {
            ix.reclaim(&at)?
        };
        let (as_of_seq, as_of_ms) = self.as_of(&ix)?;
        Ok(SnapAnswer::Ready(ReclaimEstimate {
            bytes: amount.bytes,
            chunks: amount.chunks,
            as_of_seq,
            as_of_ms,
        }))
    }

    /// `snapshot space` (see [`SpaceBreakdown`]).
    pub async fn space(&self, path: Option<&str>) -> Result<SnapAnswer<SpaceBreakdown>> {
        let ix = match self.gate().await? {
            Ok(ix) => ix,
            Err(other) => return Ok(cast(other)),
        };
        let fs = ix.fs_breakdown()?;
        let (as_of_seq, as_of_ms) = self.as_of(&ix)?;
        let compression_ratio = self.compression_ratio(&fs).await;
        let breakdown = match path {
            None => SpaceBreakdown {
                live_logical: self.deps.meta.usage_bytes_files().0,
                snapshots_total: fs.snapshots_total,
                unique: fs.unique,
                shared_snapshots_only: fs.shared_only,
                shared_with_live: fs.shared_with_live,
                awaiting_gc: fs.awaiting_gc,
                compression_ratio,
                as_of_seq,
                as_of_ms,
            },
            Some(path) => {
                let meta = self.deps.meta.clone();
                let path = path.to_string();
                let ix = ix.clone();
                let (live_logical, total, unique, with_live) = blocking(move || {
                    let Some(top) = meta.resolve_path(&path)? else {
                        bail!("no such path {path}");
                    };
                    let live_logical = meta.subtree_file_bytes(top)?;
                    let mut members = Vec::new();
                    let mut unique = Amount::default();
                    let mut in_scope = std::collections::HashMap::new();
                    for (chain, dir) in ix.chains()? {
                        let under = dir == top
                            || meta
                                .ancestry(dir)?
                                .is_some_and(|chain| chain.iter().any(|(ino, _)| *ino == top));
                        if !under {
                            continue;
                        }
                        in_scope.insert(chain, ix.chain_numbers(chain)?.and_then(|c| c.head));
                        for snap in ix.chain_snapshots(chain)? {
                            members.push((chain, snap.ord));
                            unique.bytes += snap.used;
                        }
                    }
                    // `USED` chunk counts are not kept per snapshot: count
                    // the unique chunks from the entries the scope holds.
                    let mut with_live = Amount::default();
                    let mut unique_chunks = 0;
                    if !in_scope.is_empty() {
                        ix.for_each_chunk(|_, entry| {
                            let in_chains =
                                entry.runs.iter().any(|r| in_scope.contains_key(&r.chain));
                            if !in_chains {
                                return Ok(());
                            }
                            if entry.live {
                                with_live.bytes += entry.size;
                                with_live.chunks += 1;
                            } else if let [run] = entry.runs.as_slice() {
                                let head = in_scope.get(&run.chain).copied().flatten();
                                if run.first == run.last
                                    || (run.is_open() && head == Some(run.first))
                                {
                                    unique_chunks += 1;
                                }
                            }
                            Ok(())
                        })?;
                    }
                    unique.chunks = unique_chunks;
                    let total = if members.is_empty() {
                        Amount::default()
                    } else {
                        ix.reclaim(&members)?
                    };
                    Ok((live_logical, total, unique, with_live))
                })
                .await?;
                SpaceBreakdown {
                    live_logical,
                    snapshots_total: total,
                    unique,
                    shared_snapshots_only: Amount {
                        bytes: total.bytes.saturating_sub(unique.bytes),
                        chunks: total.chunks.saturating_sub(unique.chunks),
                    },
                    shared_with_live: with_live,
                    awaiting_gc: fs.awaiting_gc,
                    compression_ratio,
                    as_of_seq,
                    as_of_ms,
                }
            }
        };
        Ok(SnapAnswer::Ready(breakdown))
    }

    /// Logical over stored bytes per chunk (module docs), from a census
    /// read at most every [`CENSUS_TTL`].
    async fn compression_ratio(&self, fs: &super::FsBreakdown) -> Option<f64> {
        let cached = *self.census.lock().expect("census lock");
        let census = match cached {
            Some((at, census)) if at.elapsed() < CENSUS_TTL => census,
            _ => {
                let census = constellation_store_s3::read_chunk_census(self.deps.chunks.inner())
                    .await
                    .unwrap_or_else(|error| {
                        tracing::debug!(%error, "snapshot accounting: no GC census");
                        None
                    });
                *self.census.lock().expect("census lock") = Some((Instant::now(), census));
                census
            }
        }?;
        let indexed = Amount {
            bytes: fs.unique.bytes + fs.shared_only.bytes + fs.shared_with_live.bytes,
            chunks: fs.unique.chunks + fs.shared_only.chunks + fs.shared_with_live.chunks,
        };
        if census.chunk_objects == 0 || census.physical_bytes == 0 || indexed.chunks == 0 {
            return None;
        }
        let stored = census.physical_bytes as f64 / census.chunk_objects as f64;
        let logical = indexed.bytes as f64 / indexed.chunks as f64;
        Some(logical / stored)
    }

    /// The brute-force oracle: bring the index current, walk every
    /// snapshot in full, compute every number from the exact sets, and
    /// diff (module `verify`). Counts into `verify_mismatches`. It reads
    /// the live tree from the replica, so a write not yet published can
    /// show as a live-flag mismatch until the next refresh: run it on a
    /// quiet tree.
    pub async fn verify(&self) -> Result<VerifyReport> {
        if self.cfg.mode == SnapAcctMode::Off {
            bail!("snapshot accounting is off (CONSTELLATION_SNAPACCT=off)");
        }
        self.demand.store(true, Ordering::Relaxed);
        let _work = self.work.lock().await;
        while self.pass_locked(Budget::unlimited(), true).await? {}
        let ix = self
            .open()
            .await?
            .context("the accounting index is due a rebuild but still in use")?;
        let brute =
            super::verify::BruteForce::compute(&self.deps.meta, &self.deps.tree, &self.deps.chunks)
                .await?;
        let mut report = brute.diff(&ix)?;
        report.as_of_seq = ix.accounted_seq()?;
        self.stats
            .verify_mismatches
            .store(report.mismatches, Ordering::Relaxed);
        Ok(report)
    }
}

/// [`SnapAcctService::open`]'s blocking part.
fn open_index(
    slot: &Mutex<Option<Arc<SnapAcct>>>,
    wipe: &AtomicBool,
    last_refresh: &Mutex<Option<Instant>>,
    dir: &std::path::Path,
    fs_uuid: &str,
    params: SnapAcctParams,
) -> Result<Option<Arc<SnapAcct>>> {
    let mut slot = slot.lock().expect("index lock");
    if wipe.load(Ordering::Acquire) {
        if let Some(ix) = slot.take() {
            if Arc::strong_count(&ix) > 1 {
                *slot = Some(ix);
                return Ok(None);
            }
        }
        let db = dir.join("db");
        if db.exists() {
            std::fs::remove_dir_all(&db)?;
        }
        *last_refresh.lock().expect("refresh lock") = None;
        wipe.store(false, Ordering::Release);
    }
    if let Some(ix) = slot.as_ref() {
        return Ok(Some(ix.clone()));
    }
    let (ix, opened) = SnapAcct::open(dir, fs_uuid, params)
        .with_context(|| format!("opening the accounting index in {}", dir.display()))?;
    match opened {
        Opened::Reused => tracing::debug!("snapshot accounting: resuming the index on disk"),
        Opened::Fresh => tracing::info!("snapshot accounting: building the index"),
        Opened::Rebuilt => {
            tracing::info!("snapshot accounting: the index on disk was stale; rebuilding")
        }
    }
    let ix = Arc::new(ix);
    *slot = Some(ix.clone());
    Ok(Some(ix))
}

fn cast<T>(answer: SnapAnswer<()>) -> SnapAnswer<T> {
    match answer {
        SnapAnswer::Ready(()) => unreachable!("a gate never answers Ready"),
        SnapAnswer::Building { pct } => SnapAnswer::Building { pct },
        SnapAnswer::Off => SnapAnswer::Off,
    }
}

fn is_live(meta: &Meta, ix: &SnapAcct, hash: &ChunkHash) -> bool {
    match meta.chunk_ref_any(hash) {
        Ok(true) => return true,
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%error, "snapshot accounting: liveness unknown; counting as live");
            return true;
        }
    }
    ix.in_live_spill(hash).unwrap_or_else(|error| {
        tracing::warn!(%error, "snapshot accounting: liveness unknown; counting as live");
        true
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Run index work (fjall transactions, tree reads) off the runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .context("snapshot accounting task")?
}
