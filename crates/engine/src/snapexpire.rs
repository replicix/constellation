//! Plan 32 Step 4: **expiry**, the only code that deletes snapshots
//! automatically. The scheduler (`crate::snapsched`) calls it once per
//! tick after creation; `snapshot.policy.remove {expire}` calls its
//! deletion half. Everything here that decides *which* snapshots go is
//! pure: the replica's rows, the policy text, and the grace state in the
//! bucket. The clock decides only *when* (a run's spacing, a grace
//! window's end) and can only delay a deletion, never cause one.
//!
//! # One run, per policy root
//!
//! At most once per `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` per root, on
//! the `_snapsched` leader, with no refusal gate up, after the tick's
//! creation:
//!
//! 1. **The grace state.** `snapsched/state.json` is read, every
//!    parseable policy root's canonical form (without `paused`) is
//!    recorded in it ([`observe`]), and, if that changed anything, it is
//!    written back with a CAS on the ETag read — after renewing the
//!    lease, since only the leader writes it. A read or write that fails
//!    (a conflict included) ends the run before any deletion: nothing is
//!    ever deleted on grace state that is not recorded in the bucket.
//! 2. **Victims** ([`graced`]): `retention::evaluate` over the root's auto
//!    rows as the leader's replica has them now, narrowed by every grace
//!    window still open on the root (`retention::grace_intersection` with
//!    each policy it replaced, `retention::grace_first_seen` while the
//!    root is newly seen). Survivors the grace windows alone keep count
//!    `skipped_grace`. Oldest first, at most
//!    `CONSTELLATION_SNAPSCHED_MAX_DELETES` per run.
//! 3. **Deletion** ([`delete_victims`]), in batches of
//!    `CONSTELLATION_SNAPSCHED_EXPIRE_BATCH` victims: renew the lease
//!    ([`Fenced`] ends the run on the spot, nothing more is sent), re-read
//!    each victim's row from the replica — it must still exist, still be
//!    `auto`, still belong to this `policy_ino` and still not be held, or
//!    it is dropped as `skipped_reverify` — then send the survivors as one
//!    holder-side batch of `SnapshotItem::Delete { force: false }`. The
//!    holder checks the hold again in its own replica (the one every hold
//!    is written to), so a hold that lands after the re-read still wins:
//!    its `Refused` counts `skipped_reverify` too.
//! 4. **Audit**: each deletion joins the tick's `snapsched/journal/`
//!    object, `{id, name, created_unix_ms, reason: "no tier keeps it"}`.
//!
//! **Batching.** Plan §4.1 renews and re-reads per victim. A batch of
//! *k* victims is renewed and re-read as a unit, immediately before it is
//! sent, so the window between a victim's re-read and its delete is one
//! batch's round trip whatever *k* is, and the authoritative hold check
//! runs at the holder per item anyway. The default *k* = 32 turns a
//! 500-victim catch-up into 16 renewals and 16 forwards instead of 500 of
//! each; `1` is the plan's literal per-victim loop.
//!
//! # The space budget (Step 8)
//!
//! A policy with `budget=<size>` is the one rule that deletes snapshots
//! the tier windows keep. After a root's tier deletions in the same run
//! (`Scheduler::budget_choice` in `crate::snapsched`):
//!
//! 1. **Not during grace.** While any grace window is open on the root
//!    (a first sighting, or a policy change — a `budget=` change is one)
//!    the budget deletes nothing and the root carries a note. Of plan
//!    §4.3's two options this is the safer: intersecting with the old
//!    policy's budget decision could only delete a subset of what the new
//!    one asks, and skipping deletes none of it, so lowering a budget by
//!    mistake costs nothing for `CONSTELLATION_SNAPSCHED_GRACE_S`. Nor
//!    while the tier rule alone fills the run's `MAX_DELETES`.
//! 2. **Only from a fresh index.** The bytes are the space-accounting
//!    index's `reclaim` (logical, deduplicated bytes; physical ≈ logical ×
//!    `snapshot.space`'s `physical_ratio`, stored bytes per logical
//!    byte). The index must
//!    be current — every row applied, no build — and as of no longer than
//!    `2 × CONSTELLATION_SNAPACCT_REFRESH_S` ago; otherwise the step is
//!    skipped and counts `budget_stale` (accounting off, or absent, too).
//! 3. **Victims**: the rows read again, the tier rule's verdicts, and
//!    `retention::budget_order` over them: kept candidates outside the
//!    newest `last`, those the coarsest tier does not keep first, oldest
//!    first, then the coarsest tier's. The measured set is the candidates
//!    the tier rule keeps, `U = reclaim(kept)`. When `U` exceeds the
//!    budget the victims are the shortest prefix `P` of the order with
//!    `U − reclaim(P)` — what would remain: a chunk `P` shares with a
//!    snapshot left behind still counts — within it (the whole order,
//!    capped by the run's room, when no prefix is). `reclaim(P)` grows
//!    with `P`, so a binary search finds it in `O(log n)` scans, all
//!    under one index lock (`SnapAcctService::budget_plan`).
//! 4. **Deletion** is exactly the tier victims' path above — renew,
//!    re-read, holder-side batch, a hold landing in between wins — with
//!    audit reason `budget` ([`REASON_BUDGET`]), counting `expired` and
//!    `budget_expired`.
//! 5. A budget that cannot be met — everything the order offers is gone
//!    and the rest is the `last` floor or held — is a note on the root
//!    (`snapshot.sched.status`'s `budget_note`), not an error.
//!
//! # What is never deleted automatically (Step 4.2)
//!
//! Manual snapshots and other policies' snapshots are not candidates
//! (`SnapFacts::is_candidate`, and the re-read checks `origin` and
//! `policy_ino` again); held ones are never candidates whatever the owner
//! (plain, `user:`, `csi:`); a root whose policy does not parse, or
//! that has none, has no run, so its snapshots are orphaned and kept; a
//! `paused` root has no run; a refused tick has no run.
//!
//! # Grace (Step 4.3)
//!
//! A root whose canonical policy changed less than
//! `CONSTELLATION_SNAPSCHED_GRACE_S` ago expires only what the new policy
//! **and** every policy it replaced inside that window expire; a root seen
//! for the first time expires nothing for the window. "Changed" is
//! measured on the canonical form *without* `paused`: pausing and resuming
//! is not a policy change. A change is dated when the leader first sees
//! it, which is never earlier than when it was made, so the window can
//! only start late, never early.
//!
//! Each recorded window carries its end (`until_unix_ms`, the recording
//! leader's `GRACE_S` added), and a reader keeps it open while `now <
//! max(until, replaced + its own GRACE_S)`: a leader configured with a
//! shorter grace never closes a window recorded under a longer one. The
//! grace is wall-clock time, so a leader's clock that jumps forwards
//! closes windows early by the jump (one running backwards only keeps
//! them open longer); a closed prior stays in `state.json` for
//! [`PRIOR_KEEP_SLACK_MS`] more, so a leader whose clock runs ahead does
//! not erase a window from the bucket that the next leader still honours.
//!
//! `skipped_grace` counts, per run, the survivors only grace kept: during
//! a window the same snapshots count again every run (every
//! `EXPIRE_EVERY_S`), like `skipped_empty` counts per tick.
//!
//! # A batch that outlives the lease
//!
//! The lease is renewed before every batch, not during one: a holder
//! slow enough to spend a lease TTL on one batch lets another node lead
//! while it still deletes. That is harmless: victims are a pure function
//! of the rows, the policy text and the recorded grace state, so an
//! overlapping leader computes the same set (minus what is already gone,
//! which re-verification and `NotFound` absorb).

use crate::singleton::{Fenced, SingletonLease};
use crate::snapshot_batch::{HolderUnreachable, ItemResult, SnapshotBatcher, SnapshotItem};
use constellation_fs_core::Ino;
use constellation_meta::snapsched::{retention, Reason, SnapFacts, SnapPolicy, Verdict};
use constellation_meta::{Meta, SnapshotRow};
use constellation_store_s3::snapsched::{
    SnapSchedJournalSnap, SnapSchedPrior, SnapSchedRootGrace, SnapSchedState,
};
use futures::future::BoxFuture;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// The audit reason of a scheduled expiry.
pub const REASON_NO_TIER: &str = "no tier keeps it";
/// The audit reason of a deletion the policy's `budget=` asked for
/// (plan 32 Step 8): the tier rule keeps it, the space budget does not.
pub const REASON_BUDGET: &str = "budget";
/// The audit reason of `snapshot policy rm --expire`.
pub const REASON_REMOVED: &str = "policy removed with --expire";

/// The most prior policies one root's record keeps. Past it, the oldest
/// ones collapse into one "unknown" prior, which keeps everything until
/// its window closes: a root whose policy is flipped back and forth
/// cannot grow the state object without bound, and can only lose
/// deletions by it, never gain one.
const MAX_PRIORS: usize = 16;

/// How long a prior stays recorded in `state.json` after its window
/// closed by the observing leader's clock. Deciding uses the window
/// itself ([`open`]); only forgetting waits this much longer, so a leader
/// whose clock runs ahead (by less than this) may stop gracing a little
/// early by its own clock, but never erases a window from the bucket
/// that a leader with a correct clock still needs.
const PRIOR_KEEP_SLACK_MS: i64 = 24 * 3_600_000;

/// `policy`'s canonical form without `paused`: what grace compares.
pub fn canonical_unpaused(policy: &SnapPolicy) -> String {
    let mut policy = policy.clone();
    policy.paused = false;
    policy.to_string()
}

/// Record what the leader sees at `now` in `state`: every parseable
/// policy root `seen` as `(ino, canonical_unpaused)`. A root seen for the
/// first time gets an "unknown" prior; a changed one pushes the policy it
/// replaced; each new prior records `until_unix_ms = now + grace_ms`.
/// Priors whose window has closed ([`open`], with the local `grace_ms`)
/// for more than [`PRIOR_KEEP_SLACK_MS`] are dropped, and so are roots
/// no longer seen (a policy removed and set again later is a first
/// sighting again). Returns whether `state` changed.
pub fn observe(
    state: &mut SnapSchedState,
    seen: &[(Ino, String)],
    now: i64,
    grace_ms: i64,
) -> bool {
    let before = state.roots.clone();
    let current: BTreeSet<Ino> = seen.iter().map(|(ino, _)| *ino).collect();
    state.roots.retain(|ino, _| current.contains(ino));
    for (ino, canonical) in seen {
        match state.roots.get_mut(ino) {
            None => {
                state.roots.insert(
                    *ino,
                    SnapSchedRootGrace {
                        canonical: canonical.clone(),
                        since_unix_ms: now,
                        prior: vec![SnapSchedPrior {
                            canonical: None,
                            replaced_unix_ms: now,
                            until_unix_ms: now.saturating_add(grace_ms),
                        }],
                    },
                );
            }
            Some(entry) if entry.canonical != *canonical => {
                let old = std::mem::replace(&mut entry.canonical, canonical.clone());
                entry.prior.push(SnapSchedPrior {
                    canonical: Some(old),
                    replaced_unix_ms: now,
                    until_unix_ms: now.saturating_add(grace_ms),
                });
                entry.since_unix_ms = now;
            }
            Some(_) => {}
        }
    }
    for entry in state.roots.values_mut() {
        entry
            .prior
            .retain(|p| now < closes(p, grace_ms).saturating_add(PRIOR_KEEP_SLACK_MS));
        if entry.prior.len() > MAX_PRIORS {
            let excess = entry.prior.len() - MAX_PRIORS + 1;
            let collapsed = &entry.prior[..excess];
            let newest = collapsed.iter().map(|p| p.replaced_unix_ms).max();
            let latest = collapsed.iter().map(|p| closes(p, grace_ms)).max();
            let unknown = SnapSchedPrior {
                canonical: None,
                replaced_unix_ms: newest.unwrap_or(now),
                until_unix_ms: latest.unwrap_or(now),
            };
            entry.prior.drain(..excess);
            entry.prior.insert(0, unknown);
        }
    }
    state.roots != before
}

/// When `prior`'s grace window closes for a reader whose own grace is
/// `grace_ms`: the later of what the recording leader stored and what
/// this reader's length gives, so neither a shorter local grace nor a
/// longer one can close a window early.
fn closes(prior: &SnapSchedPrior, grace_ms: i64) -> i64 {
    prior
        .until_unix_ms
        .max(prior.replaced_unix_ms.saturating_add(grace_ms))
}

/// Whether `prior`'s grace window is still open at `now`. A clock that
/// went backwards past the change keeps it open (only delays); a clock
/// that jumps forwards closes it early by that much (the grace is wall
/// clock, plan 32 Step 4.3; see the module doc).
fn open(prior: &SnapSchedPrior, now: i64, grace_ms: i64) -> bool {
    now < closes(prior, grace_ms)
}

/// The grace a display applies (`snapshot.list`, `policy show`, the
/// `policy set` note): the projected grace state
/// (`Scheduler::grace_view`; `None` when it could not be read), the
/// instant, and this node's window length.
#[derive(Debug, Clone)]
pub struct GraceView {
    pub state: Option<SnapSchedState>,
    pub now: i64,
    pub grace_ms: i64,
}

impl GraceView {
    /// `policy`'s verdicts over `facts` as the display shows them:
    /// [`graced`] by the known state, or the policy's own verdicts when
    /// the state is unknown (a display that then may claim a deletion a
    /// grace window still holds back, never the reverse).
    pub fn verdicts(&self, policy: &SnapPolicy, ino: Ino, facts: &[SnapFacts]) -> Graced {
        match &self.state {
            Some(state) => graced(
                policy,
                ino,
                facts,
                state.roots.get(&ino),
                self.now,
                self.grace_ms,
            ),
            None => {
                let plain = retention::evaluate(policy, ino, facts);
                Graced {
                    verdicts: plain.clone(),
                    plain,
                    grace_until: None,
                }
            }
        }
    }
}

/// One root's verdicts with grace applied.
#[derive(Debug, Clone)]
pub struct Graced {
    /// The verdicts that decide: what is deleted is `!keep` here.
    pub verdicts: Vec<Verdict>,
    /// The policy's own verdicts, without grace (the delta and
    /// `skipped_grace` compare the two).
    pub plain: Vec<Verdict>,
    /// When the last open grace window closes; `None` when none is open.
    pub grace_until: Option<i64>,
}

impl Graced {
    /// How many snapshots only a grace window keeps.
    pub fn kept_by_grace(&self) -> usize {
        self.plain
            .iter()
            .zip(&self.verdicts)
            .filter(|(plain, graced)| !plain.keep && graced.keep)
            .count()
    }
}

/// `policy`'s verdicts over `facts` (one root's rows, any order), narrowed
/// by `entry`'s open grace windows at `now`. `entry` is `None` for a root
/// the grace state has not recorded: a first sighting at `now`, which
/// keeps everything. A prior policy that no longer parses keeps
/// everything too (fail closed).
pub fn graced(
    policy: &SnapPolicy,
    ino: Ino,
    facts: &[SnapFacts],
    entry: Option<&SnapSchedRootGrace>,
    now: i64,
    grace_ms: i64,
) -> Graced {
    let plain = retention::evaluate(policy, ino, facts);
    let mut verdicts = plain.clone();
    let mut grace_until = None;
    let first_sighting = [SnapSchedPrior {
        canonical: None,
        replaced_unix_ms: now,
        until_unix_ms: now.saturating_add(grace_ms),
    }];
    let priors: &[SnapSchedPrior] = match entry {
        Some(entry) => &entry.prior,
        None => &first_sighting,
    };
    for prior in priors.iter().filter(|p| open(p, now, grace_ms)) {
        let until = closes(prior, grace_ms);
        grace_until = Some(grace_until.map_or(until, |u: i64| u.max(until)));
        verdicts = match prior.canonical.as_deref().map(SnapPolicy::parse) {
            Some(Ok(old)) => {
                retention::grace_intersection(&retention::evaluate(&old, ino, facts), &verdicts)
            }
            _ => retention::grace_first_seen(&verdicts),
        };
    }
    Graced {
        verdicts,
        plain,
        grace_until,
    }
}

/// The root's auto rows as the replica has them now, oldest first: the
/// only rows its policy can govern (the rest are never candidates).
pub fn root_rows(meta: &Meta, ino: Ino) -> anyhow::Result<Vec<SnapshotRow>> {
    let mut rows: Vec<SnapshotRow> = meta
        .snapshots(None)?
        .into_iter()
        .filter(|r| retention::Origin::from_u8(r.origin).is_auto() && r.policy_ino == ino)
        .collect();
    rows.sort_by(|a, b| (a.created_unix_ms, &a.id).cmp(&(b.created_unix_ms, &b.id)));
    Ok(rows)
}

/// Re-read a victim right before its delete: `Ok` with the row while it
/// is still an unheld auto snapshot of `ino`, else why it is skipped.
fn reverify(meta: &Meta, ino: Ino, id: &str) -> Result<SnapshotRow, String> {
    match meta.snapshot_by_id(id) {
        Ok(Some(row)) if !retention::Origin::from_u8(row.origin).is_auto() => {
            Err("no longer an auto snapshot".into())
        }
        Ok(Some(row)) if row.policy_ino != ino => {
            Err(format!("now belongs to policy root {}", row.policy_ino))
        }
        Ok(Some(row)) if row.held => Err("held".into()),
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err("already gone".into()),
        Err(error) => Err(format!("re-reading the row: {error}")),
    }
}

/// A test seam called before each delete batch with its index and the
/// victims' ids — after evaluation, before the renewal, the re-read and
/// the batch.
pub type BeforeBatch = Arc<dyn Fn(usize, Vec<String>) -> BoxFuture<'static, ()> + Send + Sync>;

/// What [`delete_victims`] did.
#[derive(Debug, Default)]
pub struct DeleteRun {
    /// Deleted (the row is gone; a `snaps/` object left behind is GC's
    /// orphan reconciliation's), in order.
    pub deleted: Vec<SnapshotRow>,
    /// Dropped by the re-read or refused by the holder because the row
    /// changed (held, gone, re-owned), with why.
    pub skipped: Vec<(SnapshotRow, String)>,
    /// The lease renewal before a batch was refused: another node leads.
    pub fenced: bool,
    /// Why the run stopped early (a store error renewing, a batch that
    /// failed as a whole, a refusal other than a hold).
    pub error: Option<String>,
    /// Whether a batch was sent and answered by the holder.
    pub reached_holder: bool,
    /// Whether a batch failed because the holder could not be reached.
    pub holder_unreachable: bool,
}

/// Delete `victims` (oldest first) through the holder-side batch, `batch`
/// at a time: before each batch renew `lease` (when given: the
/// scheduler's) and re-read every victim in it; see the module doc. Stops
/// at the first fenced renewal, store error or failed batch.
pub async fn delete_victims(
    batches: &SnapshotBatcher,
    meta: &Meta,
    mut lease: Option<&mut SingletonLease>,
    ino: Ino,
    victims: &[SnapshotRow],
    batch: usize,
    before_batch: Option<BeforeBatch>,
) -> DeleteRun {
    let mut run = DeleteRun::default();
    for (index, chunk) in victims.chunks(batch.max(1)).enumerate() {
        if let Some(hook) = &before_batch {
            hook(index, chunk.iter().map(|r| r.id.clone()).collect()).await;
        }
        if let Some(lease) = lease.as_deref_mut() {
            if let Err(error) = lease.renew().await {
                if error.downcast_ref::<Fenced>().is_some() {
                    run.fenced = true;
                } else {
                    run.error = Some(format!("renewing the scheduler lease: {error:#}"));
                }
                return run;
            }
        }
        let mut sent = Vec::with_capacity(chunk.len());
        for victim in chunk {
            match reverify(meta, ino, &victim.id) {
                Ok(row) => sent.push(row),
                Err(why) => run.skipped.push((victim.clone(), why)),
            }
        }
        if sent.is_empty() {
            continue;
        }
        let items = sent
            .iter()
            .map(|row| SnapshotItem::Delete {
                id: row.id.clone(),
                force: false,
            })
            .collect();
        let results = match batches.submit(batches.next_rid(), items).await {
            Ok(results) => results,
            Err(error) => {
                run.holder_unreachable = error.downcast_ref::<HolderUnreachable>().is_some();
                run.error = Some(format!("snapshot delete batch: {error:#}"));
                return run;
            }
        };
        run.reached_holder = true;
        if results.len() != sent.len() {
            run.error = Some(format!(
                "the holder answered {} results for {} deletes",
                results.len(),
                sent.len()
            ));
            return run;
        }
        for (row, result) in sent.into_iter().zip(results) {
            match result {
                ItemResult::Deleted | ItemResult::DeletedObjectRemains { .. } => {
                    run.deleted.push(row)
                }
                ItemResult::NotFound => run.skipped.push((row, "already gone".into())),
                ItemResult::Held { .. } => run.skipped.push((row, "held at the holder".into())),
                ItemResult::Refused { reason } => {
                    run.error = Some(format!("deleting {}@{}: {reason}", row.path, row.name));
                    return run;
                }
                other => {
                    run.error = Some(format!(
                        "deleting {}@{}: unexpected answer {other:?}",
                        row.path, row.name
                    ));
                    return run;
                }
            }
        }
    }
    run
}

/// A deleted row as the audit journal records it.
pub fn journal_snap(row: &SnapshotRow, reason: &str) -> SnapSchedJournalSnap {
    SnapSchedJournalSnap {
        id: row.id.clone(),
        name: row.name.clone(),
        created_unix_ms: row.created_unix_ms,
        reason: Some(reason.to_string()),
    }
}

/// What `snapshot.list` shows for one auto snapshot of an armed root:
/// `KEPT BY` and `EXPIRES` (plan 32 Step 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// The keeping tiers (`5m`, `1h`, …), then `last`; `["grace"]` when
    /// only a grace window keeps it; empty when the next run deletes it.
    pub kept_by: Vec<String>,
    /// The forecast (`Verdict::expires_at`; while only grace keeps it,
    /// the earlier of the window's end and the old policy's own
    /// forecast). `None`: kept forever by a `*` tier, or due now.
    pub expires_unix_ms: Option<i64>,
}

fn listed(verdict: &Verdict, grace_until: Option<i64>) -> Listed {
    if !verdict.keep {
        return Listed {
            kept_by: Vec::new(),
            expires_unix_ms: None,
        };
    }
    if verdict.reasons.first() == Some(&Reason::Grace) {
        let until = grace_until.unwrap_or(i64::MAX);
        return Listed {
            kept_by: vec!["grace".into()],
            expires_unix_ms: Some(verdict.expires_at.map_or(until, |at| at.min(until))),
        };
    }
    let mut kept_by: Vec<String> = verdict.tiers().map(|i| i.to_string()).collect();
    if verdict.reasons.iter().any(|r| matches!(r, Reason::Last(_))) {
        kept_by.push("last".into());
    }
    Listed {
        kept_by,
        expires_unix_ms: verdict.expires_at,
    }
}

/// `KEPT BY`/`EXPIRES` for every unheld auto snapshot of an *armed* root
/// (a parseable, unpaused policy on a directory that still exists), by
/// id, as the next expiry run would see it at `grace.now`
/// ([`GraceView::verdicts`]). Local reads only.
pub fn listing(meta: &Meta, grace: &GraceView) -> anyhow::Result<HashMap<String, Listed>> {
    let mut out = HashMap::new();
    let roots = meta.snapshot_policy_roots()?;
    if roots.is_empty() {
        return Ok(out);
    }
    for (ino, expr) in roots {
        let Ok(policy) = SnapPolicy::parse(&expr) else {
            continue;
        };
        if policy.paused || meta.ancestry(ino)?.is_none() {
            continue;
        }
        let rows = root_rows(meta, ino)?;
        let facts: Vec<SnapFacts> = rows.iter().map(SnapFacts::from_row).collect();
        let g = grace.verdicts(&policy, ino, &facts);
        for (row, verdict) in rows.iter().zip(&g.verdicts) {
            if !row.held {
                out.insert(row.id.clone(), listed(verdict, g.grace_until));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 3_600_000;

    fn facts(times: &[i64]) -> Vec<SnapFacts> {
        times
            .iter()
            .enumerate()
            .map(|(i, &t)| SnapFacts {
                id: format!("s{i}"),
                created_unix_ms: t,
                origin: retention::Origin::Auto,
                policy_ino: 7,
                held: false,
                held_by: None,
            })
            .collect()
    }

    fn keeps(g: &Graced) -> Vec<bool> {
        g.verdicts.iter().map(|v| v.keep).collect()
    }

    #[test]
    fn a_first_sighting_and_a_change_open_windows_and_they_close() {
        let mut state = SnapSchedState::default();
        let grace = 24 * H;
        assert!(observe(&mut state, &[(7, "1h:1d".into())], 0, grace));
        assert!(!observe(&mut state, &[(7, "1h:1d".into())], H, grace));
        let entry = &state.roots[&7];
        assert_eq!(entry.prior.len(), 1);
        assert!(entry.prior[0].canonical.is_none());
        // A change pushes the old policy.
        assert!(observe(&mut state, &[(7, "1h:2h".into())], 2 * H, grace));
        let entry = &state.roots[&7];
        assert_eq!(
            (
                entry.canonical.as_str(),
                entry.since_unix_ms,
                entry.prior.len()
            ),
            ("1h:2h", 2 * H, 2)
        );
        assert_eq!(entry.prior[1].until_unix_ms, 26 * H);
        // The first-sighting window closes at 24h, the change's at 26h;
        // each stays recorded for the slack after that.
        let slack = PRIOR_KEEP_SLACK_MS;
        let f = facts(&[0]);
        let new = SnapPolicy::parse("1h:2h").unwrap();
        let at = |now| graced(&new, 7, &f, state.roots.get(&7), now, grace).grace_until;
        assert_eq!(at(24 * H), Some(26 * H));
        assert_eq!(at(26 * H), None);
        assert!(!observe(&mut state, &[(7, "1h:2h".into())], 26 * H, grace));
        assert!(observe(
            &mut state,
            &[(7, "1h:2h".into())],
            24 * H + slack,
            grace
        ));
        assert_eq!(state.roots[&7].prior.len(), 1);
        assert!(observe(
            &mut state,
            &[(7, "1h:2h".into())],
            26 * H + slack,
            grace
        ));
        assert!(state.roots[&7].prior.is_empty());
        // A root no longer seen is forgotten.
        assert!(observe(&mut state, &[], 27 * H + slack, grace));
        assert!(state.roots.is_empty());
    }

    #[test]
    fn grace_keeps_what_any_open_prior_keeps() {
        // Hourly snapshots over 30 hours; the anchor is the newest.
        let times: Vec<i64> = (0..30).map(|h| h * H).collect();
        let f = facts(&times);
        let new = SnapPolicy::parse("1h:2h").unwrap();
        let grace = 24 * H;
        let now = 30 * H;
        // No record: a first sighting keeps everything.
        let g = graced(&new, 7, &f, None, now, grace);
        assert!(keeps(&g).iter().all(|k| *k));
        assert_eq!(g.kept_by_grace(), 28);
        assert_eq!(g.grace_until, Some(now + grace));
        // Changed from 1h:1d at `now`: only what both expire goes.
        let entry = SnapSchedRootGrace {
            canonical: "1h:2h".into(),
            since_unix_ms: now,
            prior: vec![SnapSchedPrior {
                canonical: Some("1h:1d".into()),
                replaced_unix_ms: now,
                until_unix_ms: now + grace,
            }],
        };
        let g = graced(&new, 7, &f, Some(&entry), now, grace);
        let kept = keeps(&g).iter().filter(|k| **k).count();
        assert_eq!(kept, 24, "1h:1d keeps the newest 24 hourly buckets");
        assert_eq!(g.kept_by_grace(), 22);
        assert!(g.verdicts[29].keep && g.verdicts[28].keep);
        assert_eq!(g.verdicts[10].reasons.first(), Some(&Reason::Grace));
        // After the window: exactly the new policy.
        let g = graced(&new, 7, &f, Some(&entry), now + grace, grace);
        assert_eq!(
            keeps(&g),
            g.plain.iter().map(|v| v.keep).collect::<Vec<_>>()
        );
        assert_eq!(g.grace_until, None);
        // A prior that no longer parses keeps everything while open.
        let broken = SnapSchedRootGrace {
            prior: vec![SnapSchedPrior {
                canonical: Some("not a policy".into()),
                replaced_unix_ms: now,
                until_unix_ms: now + grace,
            }],
            ..entry
        };
        assert!(keeps(&graced(&new, 7, &f, Some(&broken), now, grace))
            .iter()
            .all(|k| *k));
    }

    #[test]
    fn pausing_is_not_a_change_and_priors_are_bounded() {
        let paused = SnapPolicy::parse("1h:1d; paused").unwrap();
        let running = SnapPolicy::parse("1h:1d").unwrap();
        assert_eq!(canonical_unpaused(&paused), canonical_unpaused(&running));
        let mut state = SnapSchedState::default();
        let grace = 24 * H;
        for k in 0..40 {
            observe(&mut state, &[(7, format!("1h:{}h", k + 2))], k, grace);
        }
        let prior = &state.roots[&7].prior;
        assert_eq!(prior.len(), MAX_PRIORS);
        assert!(
            prior[0].canonical.is_none(),
            "the oldest collapse to unknown"
        );
    }

    /// Hourly snapshots over 30 hours under `1h:2h`, changed from
    /// `1h:1d` at 30h: what each leader's view deletes.
    fn shortened_at_30h() -> (Vec<SnapFacts>, SnapPolicy, SnapSchedState) {
        let times: Vec<i64> = (0..30).map(|h| h * H).collect();
        let mut state = SnapSchedState::default();
        observe(&mut state, &[(7, "1h:1d".into())], 0, 24 * H);
        // The first sighting's window (closed at 24h) is still recorded.
        observe(&mut state, &[(7, "1h:2h".into())], 30 * H, 24 * H);
        let policy = SnapPolicy::parse("1h:2h").unwrap();
        (facts(&times), policy, state)
    }

    #[test]
    fn a_shorter_local_grace_does_not_close_a_longer_recorded_window() {
        // Recorded by a leader with a 24h grace; read by one with 10 min.
        let (f, policy, mut state) = shortened_at_30h();
        let short = 600_000;
        let now = 30 * H + 2 * short;
        let g = graced(&policy, 7, &f, state.roots.get(&7), now, short);
        assert_eq!(g.grace_until, Some(54 * H), "the recorded window holds");
        assert_eq!(g.kept_by_grace(), 22, "only what 1h:1d also expires goes");
        // Its `observe` keeps the prior in the bucket's state, too.
        observe(&mut state, &[(7, "1h:2h".into())], now, short);
        let prior = &state.roots[&7].prior;
        assert!(prior
            .iter()
            .any(|p| p.canonical.as_deref() == Some("1h:1d") && p.until_unix_ms == 54 * H));
        // A longer local grace extends the recorded one (only delays).
        let g = graced(&policy, 7, &f, state.roots.get(&7), 54 * H, 48 * H);
        assert_eq!(g.grace_until, Some(78 * H));
        // After the recorded window: exactly the new policy.
        let g = graced(&policy, 7, &f, state.roots.get(&7), 54 * H, short);
        assert_eq!(g.grace_until, None);
        assert_eq!(g.kept_by_grace(), 0);
    }

    #[test]
    fn a_leader_with_its_clock_ahead_does_not_drop_a_prior_early() {
        // The change was recorded at 30h (window until 54h) by a correct
        // clock. A leader 1h ahead reads 54h at real time 53h: it stops
        // gracing by its own clock (inherent in a wall-clock grace) but
        // must not erase the prior, so a correct leader at real 53h still
        // honours the window.
        let (f, policy, mut state) = shortened_at_30h();
        let ahead = H;
        observe(&mut state, &[(7, "1h:2h".into())], 53 * H + ahead, 24 * H);
        assert!(state.roots[&7]
            .prior
            .iter()
            .any(|p| p.canonical.as_deref() == Some("1h:1d")));
        let g = graced(&policy, 7, &f, state.roots.get(&7), 53 * H, 24 * H);
        assert_eq!(g.grace_until, Some(54 * H));
        assert_eq!(g.kept_by_grace(), 22);
    }

    #[test]
    fn listed_cells() {
        let times: Vec<i64> = (0..3).map(|h| h * H).collect();
        let f = facts(&times);
        let policy = SnapPolicy::parse("1h:2h; last=1").unwrap();
        let v = retention::evaluate(&policy, 7, &f);
        assert_eq!(listed(&v[0], None).kept_by, Vec::<String>::new());
        assert_eq!(listed(&v[2], None).kept_by, ["1h", "last"]);
        let g = graced(&policy, 7, &f, None, 3 * H, H);
        let cell = listed(&g.verdicts[0], g.grace_until);
        assert_eq!(cell.kept_by, ["grace"]);
        assert_eq!(cell.expires_unix_ms, Some(4 * H));
    }
}
