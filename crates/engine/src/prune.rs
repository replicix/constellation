//! Retention pruning (plan 22): a singleton background job that evaluates
//! the prune policies stored in `user.constellation.prune` xattrs and
//! removes stale entries by publishing ordinary `Unlink` mutations.
//!
//! The *decision* is made once, on one node, from the replicated SQLite
//! replica, so every node converges on the same namespace — divergent
//! local deletion is the one outcome this must make impossible. The pure
//! policy language and per-entry evaluation live in the meta crate
//! (`constellation_meta::prune`); this module is the I/O half: the
//! `_prune` singleton lease, the namespace walk, victim selection, the
//! re-verify-then-unlink execution with partition fan-out, and the
//! observability counters.

use crate::singleton::SingletonLease;
use anyhow::Result;
use constellation_fs_core::{Ino, InodeKind};
use constellation_meta::prune::{Candidate, EntryFacts, Of, Policy, Verdict, Watermark};
use constellation_meta::{Meta, MetaStore, MutateOp, MutateOutcome};
use constellation_store_s3::LeaseMode;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Per-directory `keep` candidates: `(mtime_ns, ino, name, size, keep_n)`.
type KeepEntry = (i64, Ino, String, u64, u32);

// --- configuration (CONSTELLATION_PRUNE_*; documented in features/prune.md) ---

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Master switch for the background pruner. On-demand `prune run` ignores
/// it; only the periodic ticker consults it.
pub fn enabled() -> bool {
    !matches!(
        std::env::var("CONSTELLATION_PRUNE").as_deref(),
        Ok("0") | Ok("false")
    )
}

pub fn interval() -> Duration {
    Duration::from_secs(env_u64("CONSTELLATION_PRUNE_INTERVAL_S", 3600).max(1))
}

/// Quiet period after a marked directory's ctime changes: a policy is not
/// acted on until it has been installed at least this long, so a freshly
/// marked tree whose atimes were never bumped is not swept immediately.
pub fn grace() -> Duration {
    Duration::from_secs(env_u64("CONSTELLATION_PRUNE_GRACE_S", 86_400))
}

/// Replica-staleness refusal threshold: the pruner refuses to run when its
/// replica trails the log tail by more than this, because pruning from a
/// stale replica is how you delete a file someone else just recreated.
pub fn max_lag() -> Duration {
    Duration::from_secs(env_u64("CONSTELLATION_PRUNE_MAX_LAG_S", 300))
}

/// Per-run wall-clock budget for the namespace walk before the cursor is
/// saved and the rest deferred to the next run.
pub fn scan_budget() -> Duration {
    Duration::from_millis(env_u64("CONSTELLATION_PRUNE_SCAN_BUDGET_MS", 5000))
}

// --- observability ---

/// Prune counters, surfaced on `StatusReport` and in `/metrics`. A
/// climbing `unparseable_roots`/`inert_roots`/`refused_lag` is the
/// operator's signal that retention is silently not happening.
#[derive(Default, Debug)]
pub struct PruneStats {
    pub runs: AtomicU64,
    pub roots: AtomicU64,
    pub armed_roots: AtomicU64,
    pub unparseable_roots: AtomicU64,
    pub inert_roots: AtomicU64,
    pub entries_examined: AtomicU64,
    pub selected: AtomicU64,
    pub deleted: AtomicU64,
    pub bytes_deleted: AtomicU64,
    pub bytes_freed: AtomicU64,
    pub skipped_reverify: AtomicU64,
    pub skipped_forward_err: AtomicU64,
    pub skipped_hardlink: AtomicU64,
    pub skipped_repartition: AtomicU64,
    pub leases_acquired: AtomicU64,
    pub refused_lag: AtomicU64,
    pub last_run_unix_ms: AtomicU64,
    /// Last setxattr rejection (expression, byte offset, message), cached
    /// so an operator who hit a bare `EINVAL` from `setfattr` can see why.
    pub last_parse_error: Mutex<Option<(String, usize, String)>>,
}

impl PruneStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_parse_error(&self, expr: &str, offset: usize, msg: &str) {
        if let Ok(mut slot) = self.last_parse_error.lock() {
            *slot = Some((expr.to_string(), offset, msg.to_string()));
        }
    }
}

fn inc(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

/// The per-run summary returned to `prune run`.
#[derive(Debug, Clone, Default)]
pub struct PruneReport {
    pub dry_run: bool,
    pub roots: Vec<RootReport>,
    pub refused: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RootReport {
    pub path: String,
    pub policy: String,
    pub armed: bool,
    pub examined: u64,
    pub selected: u64,
    pub deleted: u64,
    pub bytes_deleted: u64,
    /// A note when the root was not acted on (inert, unparseable, grace).
    pub note: Option<String>,
}

// --- the engine ---

/// Everything the pruner needs from the node runtime. Cloned cheaply
/// (all `Arc`/handles) and passed to [`run`].
pub struct PruneDeps {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<Meta>,
    pub sync_tx: tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
    /// The core's lease view (the fast path's admission gate).
    pub lease: Arc<crate::lease::LeaseView>,
    pub forward: Arc<crate::forward::ForwardState>,
    pub node_id: u64,
    pub lease_mode: LeaseMode,
    pub read_only_member: bool,
    pub departed: Arc<std::sync::atomic::AtomicBool>,
    pub epoch_frozen: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub stats: Arc<PruneStats>,
    /// Freshness of the local replica: seconds since the last successful
    /// log-tail catch-up (0 when the node is the sole writer / just
    /// tailed). Compared against [`max_lag`].
    pub replica_lag: Duration,
}

/// Run one prune pass. `dry_run_override` forces dry-run regardless of
/// per-policy arming (used by `prune run --dry-run`). `only` restricts to
/// the marked roots at those inodes (used by `prune run <path>`).
pub async fn run(
    deps: &PruneDeps,
    only: Option<Vec<Ino>>,
    dry_run_override: bool,
) -> Result<PruneReport> {
    // Refusal gates (Step 4.2): the most important guards. Each is cheap
    // and each prevents deleting against state we cannot trust.
    if deps.departed.load(Ordering::Relaxed) {
        return Ok(refused(&deps.stats, "node has departed the cluster"));
    }
    if deps
        .epoch_frozen
        .as_ref()
        .is_some_and(|f| f.load(Ordering::Relaxed))
    {
        return Ok(refused(&deps.stats, "node is epoch-frozen"));
    }
    if deps.read_only_member {
        return Ok(refused(&deps.stats, "node is a read-only member"));
    }
    if deps.replica_lag > max_lag() {
        inc(&deps.stats.refused_lag, 1);
        return Ok(refused(
            &deps.stats,
            &format!(
                "replica trails the log tail by {}s (limit {}s)",
                deps.replica_lag.as_secs(),
                max_lag().as_secs()
            ),
        ));
    }

    let lease = SingletonLease::acquire(deps.store.clone(), "_prune", deps.lease_mode).await?;
    let result = run_held(deps, only, dry_run_override).await;
    lease.release().await;
    result
}

fn refused(stats: &PruneStats, why: &str) -> PruneReport {
    tracing::debug!(reason = why, "prune run refused");
    let _ = stats;
    PruneReport {
        dry_run: true,
        roots: Vec::new(),
        refused: Some(why.to_string()),
    }
}

async fn run_held(
    deps: &PruneDeps,
    only: Option<Vec<Ino>>,
    dry_run_override: bool,
) -> Result<PruneReport> {
    inc(&deps.stats.runs, 1);
    let now_ns = now_ns();
    let grace_ns = grace().as_nanos() as i64;
    let deadline = std::time::Instant::now() + scan_budget();
    let mut report = PruneReport {
        dry_run: dry_run_override,
        ..Default::default()
    };

    // A global LRU candidate pool shared by every armed `of=fs` root, and
    // per-root pools for `of=subtree` roots. Keyed by (parent, name) so a
    // victim can be re-verified and unlinked later.
    let mut fs_lru: LruPool = LruPool::default();

    let roots = deps.meta.prune_roots().unwrap_or_default();
    for (root_ino, expr) in roots {
        if let Some(only) = &only {
            if !only.contains(&root_ino) {
                continue;
            }
        }
        inc(&deps.stats.roots, 1);
        let path = deps.meta.path_of(root_ino).unwrap_or_else(|_| "?".into());

        let policy = match Policy::parse(&expr) {
            Ok(p) => p,
            Err(e) => {
                // Fail-closed: an unreadable policy deletes nothing.
                inc(&deps.stats.unparseable_roots, 1);
                report.roots.push(RootReport {
                    path,
                    policy: expr,
                    armed: false,
                    examined: 0,
                    selected: 0,
                    deleted: 0,
                    bytes_deleted: 0,
                    note: Some(format!("unparseable: {}", e.msg)),
                });
                continue;
            }
        };
        if policy.off {
            continue;
        }
        if policy.armed {
            inc(&deps.stats.armed_roots, 1);
        }
        // A percentage-watermark `of=fs` lru with no quota is inert: it
        // can never trigger, so report it and move on.
        if let Some((Watermark::Percent(_), _low, Of::Fs)) = policy.lru_watermarks() {
            if deps.meta.quota().ok().flatten().unwrap_or(0) == 0 {
                inc(&deps.stats.inert_roots, 1);
                report.roots.push(root_report_note(
                    &path,
                    &policy,
                    "inert: lru percentage needs a quota",
                ));
                continue;
            }
        }

        // Grace: the marked directory's own ctime is "policy installed
        // at"; SetXattr bumps it. Any unrelated change only delays us.
        if let Ok(Some(attr)) = deps.meta.getattr(root_ino) {
            if now_ns.saturating_sub(attr.ctime_ns) < grace_ns {
                report
                    .roots
                    .push(root_report_note(&path, &policy, "within grace period"));
                continue;
            }
        }

        let mut rr = RootReport {
            path: path.clone(),
            policy: policy.to_string(),
            armed: policy.armed,
            examined: 0,
            selected: 0,
            deleted: 0,
            bytes_deleted: 0,
            note: None,
        };

        // Whole-FS usage decides whether an `of=fs` lru even needs to run.
        let fs_over = lru_fs_triggered(deps, &policy);
        let mut subtree_pool: LruPool = LruPool::default();
        let mut subtree_bytes: u64 = 0;
        let mut keep_groups: HashMap<Ino, Vec<KeepEntry>> = HashMap::new();

        // Walk the subtree depth-first, collecting deterministic victims
        // now and lru/keep candidates for later ranking. An unarmed root
        // must never feed the shared `of=fs` deletion pool (its `of=fs`
        // candidates would otherwise be deleted on a real run), so route
        // them to a discarded pool; the armed pool is executed below.
        let mut victims: Vec<VictimRef> = Vec::new();
        let mut discard_lru = LruPool::default();
        // Contribute to the shared `of=fs` pool only when this root is
        // armed *and* its `high` watermark is currently crossed — that
        // hysteresis is what keeps `lru` from trimming to `low` the
        // moment usage merely exceeds `low`.
        let fs_pool = if policy.armed && fs_over {
            &mut fs_lru
        } else {
            &mut discard_lru
        };
        walk(
            &deps.meta,
            &deps.stats,
            root_ino,
            root_ino,
            &policy,
            now_ns,
            &mut rr,
            &mut victims,
            fs_pool,
            &mut subtree_pool,
            &mut subtree_bytes,
            &mut keep_groups,
            deadline,
        );

        // keep(n): per directory, keep the newest n, select the rest.
        for (_dir, mut entries) in keep_groups {
            if entries.is_empty() {
                continue;
            }
            let keep_n = entries[0].4 as usize;
            entries.sort_by_key(|e| std::cmp::Reverse(e.0)); // newest mtime first
            for (_mtime, ino, name, size, _n) in entries.into_iter().skip(keep_n) {
                victims.push(VictimRef {
                    parent: parent_of(deps, ino).unwrap_or(root_ino),
                    ino,
                    name,
                    size,
                });
            }
        }

        // of=subtree lru: rank this root's own pool against its budget.
        if let Some((high, low, Of::Subtree)) = policy.lru_watermarks() {
            select_lru_subtree(&mut subtree_pool, subtree_bytes, high, low, &mut victims);
        }

        rr.selected += victims.len() as u64;
        inc(&deps.stats.selected, victims.len() as u64);

        let dry = dry_run_override || !policy.armed;
        for v in victims {
            if dry {
                rr.deleted += 0;
                rr.bytes_deleted += 0;
                continue;
            }
            match execute_unlink(deps, &policy, now_ns, &v).await {
                UnlinkResult::Done { freed } => {
                    rr.deleted += 1;
                    rr.bytes_deleted += v.size;
                    inc(&deps.stats.deleted, 1);
                    inc(&deps.stats.bytes_deleted, v.size);
                    if freed {
                        inc(&deps.stats.bytes_freed, v.size);
                    }
                }
                UnlinkResult::SkippedReverify => inc(&deps.stats.skipped_reverify, 1),
                UnlinkResult::SkippedForward => inc(&deps.stats.skipped_forward_err, 1),
                UnlinkResult::SkippedRepartition => inc(&deps.stats.skipped_repartition, 1),
            }
        }
        report.roots.push(rr);
    }

    // of=fs lru: one shared pool, ranked against the lowest low watermark
    // in play, after every root's candidates are gathered.
    if !fs_lru.entries.is_empty() {
        let mut fs_victims: Vec<VictimRef> = Vec::new();
        let (used, _files) = deps.meta.usage();
        let quota = deps.meta.quota().ok().flatten();
        select_lru_fs(used, quota, &mut fs_lru, &mut fs_victims);
        for v in fs_victims {
            // fs-lru victims always come from armed roots (unarmed roots
            // never contribute); honour a global dry-run override.
            if dry_run_override {
                continue;
            }
            match execute_unlink_no_policy(deps, now_ns, &v).await {
                UnlinkResult::Done { freed } => {
                    inc(&deps.stats.deleted, 1);
                    inc(&deps.stats.bytes_deleted, v.size);
                    if freed {
                        inc(&deps.stats.bytes_freed, v.size);
                    }
                }
                UnlinkResult::SkippedReverify => inc(&deps.stats.skipped_reverify, 1),
                UnlinkResult::SkippedForward => inc(&deps.stats.skipped_forward_err, 1),
                UnlinkResult::SkippedRepartition => inc(&deps.stats.skipped_repartition, 1),
            }
        }
    }

    deps.stats
        .last_run_unix_ms
        .store(now_unix_ms(), Ordering::Relaxed);
    write_audit(deps, &report).await;
    Ok(report)
}

fn root_report_note(path: &str, policy: &Policy, note: &str) -> RootReport {
    RootReport {
        path: path.to_string(),
        policy: policy.to_string(),
        armed: policy.armed,
        examined: 0,
        selected: 0,
        deleted: 0,
        bytes_deleted: 0,
        note: Some(note.to_string()),
    }
}

/// A selected entry, addressed by its parent+name so execution can
/// re-resolve the partition and re-verify before unlinking.
#[derive(Debug, Clone)]
struct VictimRef {
    parent: Ino,
    ino: Ino,
    name: String,
    size: u64,
}

#[derive(Default)]
struct LruPool {
    /// (atime_ns, VictimRef, size) — ranked coldest-first at selection.
    entries: Vec<(i64, VictimRef)>,
    /// The lowest `low` watermark seen across contributing `of=fs` roots.
    low: Option<Watermark>,
}

#[allow(clippy::too_many_arguments)]
fn walk(
    meta: &Meta,
    stats: &PruneStats,
    root_ino: Ino,
    dir: Ino,
    policy: &Policy,
    now_ns: i64,
    rr: &mut RootReport,
    victims: &mut Vec<VictimRef>,
    fs_lru: &mut LruPool,
    subtree_pool: &mut LruPool,
    subtree_bytes: &mut u64,
    keep_groups: &mut HashMap<Ino, Vec<KeepEntry>>,
    deadline: std::time::Instant,
) {
    if std::time::Instant::now() >= deadline {
        return;
    }
    let entries = match meta.readdir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries {
        let facts = match facts_of(meta, &entry.name, entry.ino) {
            Some(f) => f,
            None => continue,
        };
        if entry.kind == InodeKind::Dir {
            // Skip scratch roots and any nested directory carrying its own
            // policy (that subtree gets its own pass).
            if meta.is_scratch_dir(entry.ino).unwrap_or(false) {
                continue;
            }
            if entry.ino != root_ino
                && meta
                    .get_xattr(entry.ino, constellation_meta::prune::PRUNE_XATTR)
                    .ok()
                    .flatten()
                    .is_some()
            {
                continue;
            }
            walk(
                meta,
                stats,
                root_ino,
                entry.ino,
                policy,
                now_ns,
                rr,
                victims,
                fs_lru,
                subtree_pool,
                subtree_bytes,
                keep_groups,
                deadline,
            );
            continue;
        }

        rr.examined += 1;
        inc(&stats.entries_examined, 1);
        *subtree_bytes = subtree_bytes.saturating_add(facts.size);

        // Deterministic rules first (union of age/unused).
        if policy.deterministic_verdict(&facts, now_ns) == Verdict::Prune {
            victims.push(VictimRef {
                parent: dir,
                ino: entry.ino,
                name: entry.name.clone(),
                size: facts.size,
            });
            continue;
        }
        // keep candidates grouped per directory.
        if let Some((n, mtime)) = policy.keep_candidate(&facts, now_ns) {
            keep_groups.entry(dir).or_default().push((
                mtime,
                entry.ino,
                entry.name.clone(),
                facts.size,
                n,
            ));
        }
        // lru candidates. A hardlinked file can never be an lru victim
        // (unlinking one name frees no bytes), so count what lru skips.
        if policy.has_lru() && facts.nlink > 1 {
            inc(&stats.skipped_hardlink, 1);
        }
        if let Some(cand) = policy.lru_candidate(&facts, now_ns) {
            match policy.lru_watermarks() {
                Some((_h, low, Of::Fs)) => {
                    push_candidate(fs_lru, cand, dir, entry.ino, &entry.name);
                    fs_lru.low = Some(lower_watermark(fs_lru.low, low));
                }
                Some((_h, _low, Of::Subtree)) => {
                    push_candidate(subtree_pool, cand, dir, entry.ino, &entry.name);
                }
                None => {}
            }
        }
    }
}

fn push_candidate(pool: &mut LruPool, cand: Candidate, parent: Ino, ino: Ino, name: &str) {
    pool.entries.push((
        cand.atime_ns,
        VictimRef {
            parent,
            ino,
            name: name.to_string(),
            size: cand.size,
        },
    ));
}

fn lower_watermark(a: Option<Watermark>, b: Watermark) -> Watermark {
    match a {
        None => b,
        Some(prev) => {
            // Compare only within the same unit; percentages and sizes are
            // not comparable at parse time, so keep the first seen.
            match (prev, b) {
                (Watermark::Percent(p), Watermark::Percent(q)) => Watermark::Percent(p.min(q)),
                (Watermark::Size(p), Watermark::Size(q)) => Watermark::Size(p.min(q)),
                _ => prev,
            }
        }
    }
}

/// Is an `of=fs` lru even triggered? Whole-FS usage vs the quota (for a
/// percentage `high`) or the raw byte figure (for a size). O(1).
fn lru_fs_triggered(deps: &PruneDeps, policy: &Policy) -> bool {
    let Some((high, _low, Of::Fs)) = policy.lru_watermarks() else {
        return false;
    };
    let (used, _files) = deps.meta.usage();
    match high {
        Watermark::Size(bytes) => used >= bytes,
        Watermark::Percent(p) => match deps.meta.quota().ok().flatten() {
            Some(quota) if quota > 0 => used * 100 >= quota * p as u64,
            // No quota: a percentage watermark is inert.
            _ => false,
        },
    }
}

fn select_lru_fs(used: u64, quota: Option<u64>, pool: &mut LruPool, out: &mut Vec<VictimRef>) {
    let target_low: u64 = match pool.low {
        Some(Watermark::Size(b)) => b,
        Some(Watermark::Percent(p)) => match quota {
            Some(q) if q > 0 => q / 100 * p as u64,
            _ => return, // inert
        },
        None => return,
    };
    if used <= target_low {
        return;
    }
    pool.entries.sort_by_key(|e| e.0); // coldest atime first
    let mut projected = used;
    for (_atime, v) in std::mem::take(&mut pool.entries) {
        if projected <= target_low {
            break;
        }
        projected = projected.saturating_sub(v.size);
        out.push(v);
    }
}

fn select_lru_subtree(
    pool: &mut LruPool,
    subtree_bytes: u64,
    high: Watermark,
    low: Watermark,
    out: &mut Vec<VictimRef>,
) {
    let high_b = match high {
        Watermark::Size(b) => b,
        Watermark::Percent(_) => return, // invalid for subtree; validated away
    };
    if subtree_bytes < high_b {
        return;
    }
    let low_b = match low {
        Watermark::Size(b) => b,
        Watermark::Percent(p) => high_b / 100 * p as u64,
    };
    pool.entries.sort_by_key(|e| e.0);
    let mut projected = subtree_bytes;
    for (_atime, v) in std::mem::take(&mut pool.entries) {
        if projected <= low_b {
            break;
        }
        projected = projected.saturating_sub(v.size);
        out.push(v);
    }
}

enum UnlinkResult {
    Done { freed: bool },
    SkippedReverify,
    SkippedForward,
    SkippedRepartition,
}

/// Unlink one victim after re-verifying it still matches the policy.
async fn execute_unlink(
    deps: &PruneDeps,
    policy: &Policy,
    now_ns: i64,
    v: &VictimRef,
) -> UnlinkResult {
    // Re-verify against fresh state: a read or write between selection and
    // now must spare the entry (deterministic + lru + keep all recheck
    // via the policy where possible; lru/keep coldness is best-effort).
    let facts = match facts_of(&deps.meta, &v.name, v.ino) {
        Some(f) => f,
        None => return UnlinkResult::SkippedReverify,
    };
    // The (parent,name) must still resolve to the same inode (guards
    // against a rename/cross-partition move mid-run).
    match deps.meta.lookup(v.parent, &v.name) {
        Ok(Some(attr)) if attr.ino == v.ino => {}
        _ => return UnlinkResult::SkippedRepartition,
    }
    let still = policy.deterministic_verdict(&facts, now_ns) == Verdict::Prune
        || policy.lru_candidate(&facts, now_ns).is_some()
        || policy.keep_candidate(&facts, now_ns).is_some();
    if !still {
        return UnlinkResult::SkippedReverify;
    }
    unlink_now(deps, v, facts.nlink).await
}

/// fs-lru victims are ranked by the shared pool; there is no single policy
/// to recheck (several roots contribute), so re-verify is limited to the
/// name/inode identity and hardlink accounting.
async fn execute_unlink_no_policy(deps: &PruneDeps, _now_ns: i64, v: &VictimRef) -> UnlinkResult {
    let facts = match facts_of(&deps.meta, &v.name, v.ino) {
        Some(f) => f,
        None => return UnlinkResult::SkippedReverify,
    };
    match deps.meta.lookup(v.parent, &v.name) {
        Ok(Some(attr)) if attr.ino == v.ino => {}
        _ => return UnlinkResult::SkippedRepartition,
    }
    unlink_now(deps, v, facts.nlink).await
}

async fn unlink_now(deps: &PruneDeps, v: &VictimRef, nlink: u32) -> UnlinkResult {
    let op = MutateOp::Unlink {
        parent: v.parent,
        name: v.name.clone(),
    };
    // freed iff this was the last link (bytes actually reclaimed).
    let freed = nlink <= 1;
    // Plan 30 §M2: allocated once, kept across the forward attempt below
    // and the lease-acquisition fallback further down, exactly like
    // `mutate_op_rebasable` — this unlink has the same at-least-once
    // shape bug A did if a forward times out after the holder already
    // executed it and this node then falls back to a local retry.
    let rid = deps.forward.next_system_rid(deps.node_id);

    // Held locally with a usable, non-lost shipping lease → execute
    // directly (no `touch()`, so a pure prune never pins the lease).
    // Plan 30 §M3b: admitted through the lease view like every other
    // local mutation, so a release's final flush cannot miss it.
    // Plan 30 §M12: and only for a name the root owns; one a delegation
    // owns goes through the core below (`Meta::root_fast_path`).
    if let Some(_admitted) = deps.lease.admit() {
        if let Some(_owned) = deps.meta.root_fast_path(&op) {
            match constellation_meta::execute_mutate(&deps.meta, &op, Some(rid)) {
                Ok(_) => {
                    let _ = deps.sync_tx.send(crate::sync::SyncRequest::Nudge);
                    return UnlinkResult::Done { freed };
                }
                Err(_) => return UnlinkResult::SkippedForward,
            }
        }
    }

    // Non-holder: the core forwards once to the known holder, else takes
    // the lease if it is free — never preempting a live holder
    // (`Policy::System`) — and, having held, resolves the rid against
    // `completed` before executing (plan 30 §M2's in-doubt rule).
    let (reply, rx) = tokio::sync::oneshot::channel();
    if deps
        .sync_tx
        .send(crate::sync::SyncRequest::Submit {
            op,
            rid,
            policy: constellation_authority::Policy::System,
            in_doubt: false,
            tag: constellation_meta::locks::LockTag::NONE,
            reply,
        })
        .is_err()
    {
        return UnlinkResult::SkippedForward;
    }
    match rx.await {
        Ok(constellation_authority::ClientReply::Outcome(MutateOutcome::Accepted { .. })) => {
            UnlinkResult::Done { freed }
        }
        // Refused, in doubt, foreign holder or acquisition failure:
        // skip, next run retries.
        _ => UnlinkResult::SkippedForward,
    }
}

// --- small helpers ---

fn facts_of(meta: &Meta, name: &str, ino: Ino) -> Option<EntryFacts> {
    let attr = meta.getattr(ino).ok().flatten()?;
    Some(EntryFacts {
        name: name.to_string(),
        kind: attr.kind,
        size: attr.size,
        uid: attr.uid,
        gid: attr.gid,
        atime_ns: attr.atime_ns,
        mtime_ns: attr.mtime_ns,
        ctime_ns: attr.ctime_ns,
        nlink: attr.nlink,
    })
}

fn parent_of(deps: &PruneDeps, ino: Ino) -> Option<Ino> {
    deps.meta.parent_of(ino).ok().flatten()
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One audit record per run under `prune/journal/`, mirroring the GC
/// journal. A dry run writes the same shape with zero deletes.
async fn write_audit(deps: &PruneDeps, report: &PruneReport) {
    if report.roots.is_empty() && report.refused.is_none() {
        return;
    }
    let ts = now_unix_ms();
    let nonce = std::process::id() as u64 ^ ts.rotate_left(21);
    let key = ObjPath::from(format!("prune/journal/{ts}-{nonce:x}.json"));
    let body = serde_json::json!({
        "ts_unix_ms": ts,
        "dry_run": report.dry_run,
        "refused": report.refused,
        "node": deps.node_id,
        "roots": report.roots.iter().map(|r| serde_json::json!({
            "path": r.path,
            "policy": r.policy,
            "armed": r.armed,
            "examined": r.examined,
            "selected": r.selected,
            "deleted": r.deleted,
            "bytes_deleted": r.bytes_deleted,
            "note": r.note,
        })).collect::<Vec<_>>(),
    });
    if let Ok(bytes) = serde_json::to_vec(&body) {
        let _ = deps
            .store
            .put_opts(
                &key,
                PutPayload::from(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_meta::prune::{Of, Policy, Watermark};
    use constellation_meta::{MetaStore, SetXattrMode};

    const S: i64 = 1_000_000_000;

    /// Drive the namespace walk over an in-memory replica and return the
    /// deterministic + keep victims as `(parent, name)`.
    fn select_victims(meta: &Meta, root: Ino, expr: &str, now_ns: i64) -> Vec<(Ino, String)> {
        let policy = Policy::parse(expr).unwrap();
        let stats = PruneStats::default();
        let mut rr = RootReport {
            path: "/".into(),
            policy: policy.to_string(),
            armed: policy.armed,
            examined: 0,
            selected: 0,
            deleted: 0,
            bytes_deleted: 0,
            note: None,
        };
        let mut victims = Vec::new();
        let mut fs_lru = LruPool::default();
        let mut subtree_pool = LruPool::default();
        let mut subtree_bytes = 0u64;
        let mut keep_groups: HashMap<Ino, Vec<KeepEntry>> = HashMap::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        walk(
            meta,
            &stats,
            root,
            root,
            &policy,
            now_ns,
            &mut rr,
            &mut victims,
            &mut fs_lru,
            &mut subtree_pool,
            &mut subtree_bytes,
            &mut keep_groups,
            deadline,
        );
        // Resolve keep groups the same way run_held does.
        for (_dir, mut entries) in keep_groups {
            if entries.is_empty() {
                continue;
            }
            let keep_n = entries[0].4 as usize;
            entries.sort_by_key(|e| std::cmp::Reverse(e.0));
            for (_m, ino, name, size, _n) in entries.into_iter().skip(keep_n) {
                victims.push(VictimRef {
                    parent: meta.parent_of(ino).unwrap().unwrap(),
                    ino,
                    name,
                    size,
                });
            }
        }
        victims.into_iter().map(|v| (v.parent, v.name)).collect()
    }

    /// Backdate an inode's mtime/ctime/atime so age/unused rules fire.
    fn backdate(meta: &Meta, ino: Ino, secs_ago: i64, now_ns: i64) {
        let ts = now_ns - secs_ago * S;
        meta.setattr(ino, None, None, None, None, Some(ts), Some(ts))
            .unwrap();
    }

    #[test]
    fn walk_selects_old_files_and_skips_nested_and_scratch() {
        let meta = Meta::open_in_memory().unwrap();
        let now = 1_000_000 * S;
        let root = meta.mkdir(1, "data", 0o755, 0, 0).unwrap().ino;
        let old = meta.create(root, "old.log", 0o644, 0, 0).unwrap().ino;
        let fresh = meta.create(root, "fresh.log", 0o644, 0, 0).unwrap().ino;
        backdate(&meta, old, 100 * 86_400, now); // 100 days old
        backdate(&meta, fresh, 86_400, now); // 1 day old

        // A nested subtree with its own policy is skipped (own pass).
        let nested = meta.mkdir(root, "nested", 0o755, 0, 0).unwrap().ino;
        meta.set_xattr(
            nested,
            constellation_meta::prune::PRUNE_XATTR,
            b"age(1d)",
            SetXattrMode::Set,
        )
        .unwrap();
        let nested_old = meta.create(nested, "buried.log", 0o644, 0, 0).unwrap().ino;
        backdate(&meta, nested_old, 100 * 86_400, now);

        // A scratch root is skipped entirely.
        let scratch = meta.mkdir(root, "tmp", 0o755, 0, 0).unwrap().ino;
        meta.set_xattr(
            scratch,
            constellation_meta::SCRATCH_XATTR,
            b"1",
            SetXattrMode::Set,
        )
        .unwrap();

        let victims = select_victims(&meta, root, "age(30d)", now);
        assert_eq!(victims, vec![(root, "old.log".to_string())]);
    }

    #[test]
    fn walk_keep_newest_n_per_directory() {
        let meta = Meta::open_in_memory().unwrap();
        let now = 1_000_000 * S;
        let root = meta.mkdir(1, "snaps", 0o755, 0, 0).unwrap().ino;
        // Five files, mtimes 10..50 days old; keep(2) drops the 3 oldest.
        let mut inos = Vec::new();
        for i in 1..=5 {
            let ino = meta
                .create(root, &format!("s{i}"), 0o644, 0, 0)
                .unwrap()
                .ino;
            backdate(&meta, ino, i as i64 * 10 * 86_400, now);
            inos.push((i, ino));
        }
        let mut victims = select_victims(&meta, root, "keep(2)", now);
        victims.sort();
        // Newest two are s1, s2 (10d, 20d); s3,s4,s5 are pruned.
        let mut expect = vec![
            (root, "s3".to_string()),
            (root, "s4".to_string()),
            (root, "s5".to_string()),
        ];
        expect.sort();
        assert_eq!(victims, expect);
    }

    #[test]
    fn lru_fs_percentage_evicts_to_low_of_quota() {
        // quota 1000, low 70% -> target 700. used 900 -> evict 200+.
        let mut pool = LruPool {
            low: Some(Watermark::Percent(70)),
            ..Default::default()
        };
        for (atime, size) in [(3, 150u64), (1, 150), (2, 150)] {
            pool.entries.push((
                atime * S,
                VictimRef {
                    parent: 1,
                    ino: atime as u64,
                    name: format!("f{atime}"),
                    size,
                },
            ));
        }
        let mut out = Vec::new();
        select_lru_fs(900, Some(1000), &mut pool, &mut out);
        // Coldest first: f1 (900-150=750), f2 (750-150=600<=700 stop).
        let names: Vec<_> = out.iter().map(|v| v.name.clone()).collect();
        assert_eq!(names, vec!["f1".to_string(), "f2".to_string()]);
    }

    #[test]
    fn lru_fs_percentage_is_inert_without_quota() {
        let mut pool = LruPool {
            low: Some(Watermark::Percent(70)),
            entries: vec![(
                0,
                VictimRef {
                    parent: 1,
                    ino: 1,
                    name: "f".into(),
                    size: 100,
                },
            )],
        };
        let mut out = Vec::new();
        select_lru_fs(10_000, None, &mut pool, &mut out);
        assert!(out.is_empty(), "no quota -> percentage lru is inert");
    }

    #[test]
    fn lru_subtree_evicts_coldest_to_low() {
        // Subtree is 1000 bytes over a 900 high; low 500. Coldest first.
        let mut pool = LruPool::default();
        for (atime, size) in [(10, 300u64), (5, 300), (1, 300), (20, 100)] {
            pool.entries.push((
                atime * S,
                VictimRef {
                    parent: 1,
                    ino: atime as u64,
                    name: format!("f{atime}"),
                    size,
                },
            ));
        }
        let mut out = Vec::new();
        select_lru_subtree(
            &mut pool,
            1000,
            Watermark::Size(900),
            Watermark::Size(500),
            &mut out,
        );
        // Evict coldest (atime 1, 5, ...) until <= 500: 1000-300-300=400.
        let names: Vec<_> = out.iter().map(|v| v.name.clone()).collect();
        assert_eq!(names, vec!["f1".to_string(), "f5".to_string()]);
        let _ = Of::Subtree;
    }
}
