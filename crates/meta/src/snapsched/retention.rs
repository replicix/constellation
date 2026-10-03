//! Retention: which snapshots a policy keeps (plan 32, Step 2).
//!
//! This is the only code in the tree that decides which automatic
//! snapshots die. The scheduler's expiry pass (Step 4), `policy check`
//! and `policy set`'s delta (Step 5) and the web UI's retention
//! timeline (Step 7.4, through `snapshot.policy.simulate`) all call
//! [`evaluate`] or [`simulate`]. **Nothing else may restate the rule** —
//! not the CLI, and above all not JavaScript: one implementation, one
//! behaviour, or two surfaces disagree about what is about to be
//! deleted.
//!
//! It is a *pure function*: no I/O, no clock, no environment. `now_ms`
//! is a parameter where it is needed at all ([`simulate`] and [`due`],
//! both of which only decide *when to create*). [`evaluate`] does not
//! take a time, because "which snapshots survive" must not depend on
//! one — an outage would otherwise age history away (L3, and plan 32's
//! "Expiry uses no clock").
//!
//! # The rule, stated once
//!
//! The policy's **candidates** are its `origin=auto` snapshots of
//! `policy_ino` that are **not held**. A held snapshot is invisible to
//! the policy whatever its `held_by` says — plain, `user:…` or a CSI
//! driver's `csi:…` (plan 37) — so it is never expired *and* never
//! counted against the ones that are (PBS `protected`, L6). Then:
//!
//! 1. the **anchor** is the newest candidate;
//! 2. for each tier `I:K`, a candidate is its bucket's
//!    **representative** if it is the *oldest* candidate in that
//!    `I`-bucket (aligned by [`super::calendar`]). The representative is
//!    final the moment it exists and never flips (L5);
//! 3. a candidate is **kept** if it is among the newest `last`
//!    candidates, **or** if for some tier it is the representative and
//!    `bucket_start > bucket_start_I(anchor) − K`. The comparison is
//!    strict, so `5m:1d` keeps exactly 288 buckets including the
//!    anchor's; `K = *` always keeps;
//! 4. every other candidate **expires**.
//!
//! [`Verdict::reasons`] lists *every* tier that keeps a snapshot (L2's
//! union semantics, L7's "explain every decision"), finest first, with
//! [`Reason::Last`] after them.
//!
//! # What is deliberately not here
//!
//! * **`paused`.** It is a gate on *acting*, not part of the rule: the
//!   plan's scheduler only walks "armed (non-`paused`, parseable)"
//!   roots (Step 3.2), and [`due`] refuses to create for a paused
//!   policy. [`evaluate`] still answers "what would this policy keep",
//!   which is exactly what the editor's simulator must show while an
//!   operator edits a paused root's expression. A caller that expires
//!   snapshots **must** check `policy.paused` itself.
//! * **The clock, and therefore grace.** [`grace_intersection`] and
//!   [`grace_first_seen`] implement Step 4.3's rule over two verdict
//!   lists; *whether* a root is inside its grace window is the
//!   scheduler's `state.json` question (M4).
//! * **Deletion.** Nothing here deletes anything or looks at a replica.
//!
//! # `expires_at`
//!
//! A display forecast (L7's `EXPIRES` column), never an input to the
//! rule: the time at which, *if snapshots keep arriving on the policy's
//! schedule*, the last reason that keeps this snapshot falls away. Per
//! tier that is the first `I`-bucket whose window has moved past this
//! snapshot's bucket; for [`Reason::Last`] it is the arrival that pushes
//! it out of the newest `last`. The maximum over the keeping reasons is
//! the forecast, and `None` means "not forecastable": a `*` tier keeps
//! it forever, or it has already expired (`keep` tells which).

use smallvec::SmallVec;

use super::calendar::{add_keep, bucket_start, next_bucket_start, policy_time_zone, subtract_keep};
use super::policy::{Interval, SnapPolicy, Tier};
use crate::SnapshotRow;
use jiff::tz::TimeZone;

/// How a snapshot came to exist. The replica stores this as a `u8`
/// (plan 32 §0.4); a word here so no caller has to remember that 1
/// means "a policy's own".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Origin {
    /// Someone asked for it. Never deleted automatically.
    #[default]
    Manual,
    /// A policy's own. The only kind retention can expire.
    Auto,
    /// A value a later step introduced, read by an older binary.
    /// Treated as "not ours", i.e. never expired.
    Unknown(u8),
}

impl Origin {
    pub fn from_u8(v: u8) -> Origin {
        match v {
            0 => Origin::Manual,
            1 => Origin::Auto,
            other => Origin::Unknown(other),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Origin::Manual => 0,
            Origin::Auto => 1,
            Origin::Unknown(v) => v,
        }
    }

    pub fn is_auto(self) -> bool {
        self == Origin::Auto
    }
}

/// Everything retention knows about one snapshot. A projection of the
/// replica's [`SnapshotRow`]: no root hash, no path, no size — the rule
/// uses none of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapFacts {
    /// The snapshot id, used only for display and as the tie-break
    /// between two snapshots created in the same millisecond.
    pub id: String,
    pub created_unix_ms: i64,
    pub origin: Origin,
    /// The directory inode of the policy that owns it; 0 when none
    /// does.
    pub policy_ino: u64,
    pub held: bool,
    /// The hold's owner, carried through to [`Reason::Held`] unchanged
    /// for display. Never an input to the rule.
    pub held_by: Option<String>,
}

impl SnapFacts {
    /// The replica's row, as retention sees it.
    pub fn from_row(row: &SnapshotRow) -> SnapFacts {
        SnapFacts {
            id: row.id.clone(),
            created_unix_ms: row.created_unix_ms,
            origin: Origin::from_u8(row.origin),
            policy_ino: row.policy_ino,
            held: row.held,
            held_by: row.held_by.clone(),
        }
    }

    /// Whether this policy's rule may expire this snapshot.
    pub fn is_candidate(&self, policy_ino: u64) -> bool {
        self.origin.is_auto() && self.policy_ino == policy_ino && !self.held
    }
}

/// Why a snapshot is kept. A kept verdict lists all of them; an expired
/// one lists none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    /// It is this tier's bucket representative, inside the tier's
    /// window.
    Tier(Interval),
    /// It is among the newest `n` candidates (`last=n`, floor 1: the
    /// newest snapshot is never auto-deleted, L3).
    Last(u32),
    /// A hold, with its owner for display. Holds are outside the
    /// candidate set entirely.
    Held(Option<String>),
    /// Step 4.3's grace window after a policy change: the *old* policy
    /// still keeps it, so nothing is deleted yet.
    Grace,
    /// Not this policy's to delete: a manual snapshot, or one belonging
    /// to another `policy_ino` (or an orphan whose policy is gone).
    NotCandidate,
}

/// One snapshot's fate under one policy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Verdict {
    pub keep: bool,
    /// Every reason, finest tier first, [`Reason::Last`] after the
    /// tiers. Empty exactly when `keep` is false.
    pub reasons: SmallVec<[Reason; 4]>,
    /// The display forecast; see the module docs.
    pub expires_at: Option<i64>,
}

impl Verdict {
    fn kept(reasons: SmallVec<[Reason; 4]>, expires_at: Option<i64>) -> Verdict {
        Verdict {
            keep: true,
            reasons,
            expires_at,
        }
    }

    fn expired() -> Verdict {
        Verdict {
            keep: false,
            reasons: SmallVec::new(),
            expires_at: None,
        }
    }

    fn one(reason: Reason) -> Verdict {
        let mut reasons = SmallVec::new();
        reasons.push(reason);
        Verdict::kept(reasons, None)
    }

    /// The tier intervals that keep this snapshot, for `KEPT BY`.
    pub fn tiers(&self) -> impl Iterator<Item = Interval> + '_ {
        self.reasons.iter().filter_map(|r| match r {
            Reason::Tier(i) => Some(*i),
            _ => None,
        })
    }
}

// --- the rule --------------------------------------------------------

/// Evaluate `policy` over `snaps`, returning one [`Verdict`] per input
/// **in the same order**.
///
/// Non-candidates (manual, held, or another `policy_ino`'s) always come
/// back `keep: true`, with [`Reason::Held`] carrying `held_by` when a
/// hold is what protects them and [`Reason::NotCandidate`] otherwise.
/// See the module docs for the rule itself.
///
/// If `policy.tz` is not in the bundled tzdb — which
/// [`SnapPolicy::parse`] cannot produce, since it validates and
/// canonicalizes the name — the policy cannot be evaluated at all, and
/// every snapshot comes back [`Reason::NotCandidate`]: the plan's
/// posture for an unparseable policy is that its snapshots are
/// orphaned and kept (Step 4.2), never that they are deleted by a
/// guessed timezone.
pub fn evaluate(policy: &SnapPolicy, policy_ino: u64, snaps: &[SnapFacts]) -> Vec<Verdict> {
    let Some(tz) = policy_time_zone(policy) else {
        return snaps
            .iter()
            .map(|_| Verdict::one(Reason::NotCandidate))
            .collect();
    };
    let rule = Rule { policy, tz };
    let mut out: Vec<Verdict> = snaps
        .iter()
        .map(|s| {
            if s.held {
                Verdict::one(Reason::Held(s.held_by.clone()))
            } else {
                Verdict::one(Reason::NotCandidate)
            }
        })
        .collect();
    let cands = rule.candidates(policy_ino, snaps);
    for (pos, verdict) in rule.decide(&cands).into_iter().enumerate() {
        out[cands[pos].snap] = verdict;
    }
    out
}

/// Step 4.3's grace rule: inside the grace window after a policy
/// change, expire only what **both** the old and the new policy expire.
///
/// `old` and `new` are [`evaluate`] over the *same* snapshot slice with
/// the two policies, so they line up index for index. A snapshot the
/// new policy expires but the old one keeps comes back kept, with
/// [`Reason::Grace`] in front of the old policy's own reasons and the
/// old policy's forecast — that is why it is still here, and what the
/// UI should say. Shortening `1d:1y` to `1d:7d` therefore deletes
/// nothing for a day (the TrueNAS footgun, L6).
///
/// Mismatched lengths are resolved conservatively: an index the old
/// list does not cover is kept.
pub fn grace_intersection(old: &[Verdict], new: &[Verdict]) -> Vec<Verdict> {
    new.iter()
        .enumerate()
        .map(|(i, newv)| {
            if newv.keep {
                return newv.clone();
            }
            match old.get(i) {
                Some(oldv) if !oldv.keep => newv.clone(),
                Some(oldv) => {
                    let mut reasons: SmallVec<[Reason; 4]> = SmallVec::new();
                    reasons.push(Reason::Grace);
                    reasons.extend(oldv.reasons.iter().cloned());
                    Verdict::kept(reasons, oldv.expires_at)
                }
                // No old verdict for this index: keep it.
                None => Verdict::one(Reason::Grace),
            }
        })
        .collect()
}

/// Step 4.3's first-sighting rule: a root the scheduler has never seen
/// has no "old" policy, so the intersection is "expire nothing". That
/// makes adopting a directory which already holds orphaned auto
/// snapshots safe.
pub fn grace_first_seen(new: &[Verdict]) -> Vec<Verdict> {
    new.iter()
        .map(|v| {
            if v.keep {
                v.clone()
            } else {
                Verdict::one(Reason::Grace)
            }
        })
        .collect()
}

/// Step 8's victim order for `budget=`: which kept candidates a space
/// budget gives up, first to last. The caller deletes a prefix of it, as
/// short as gets the policy's snapshot-only bytes under the budget; the
/// bytes are the accounting index's business, the order is this rule's.
///
/// `verdicts` are the ones the tier rule (and any grace) decided over the
/// same `snaps`, index for index ([`evaluate`]). The order holds only
/// candidates those verdicts **keep** — what they expire, the tier step
/// deletes anyway — and never:
///
/// * a non-candidate: manual, held (whatever the owner), another
///   `policy_ino`'s;
/// * one of the newest `last` candidates (the floor, `last ≥ 1`: the
///   newest snapshot is never deleted automatically, L3);
/// * one kept by [`Reason::Grace`] or [`Reason::Last`] in `verdicts`, or
///   by an index the verdicts do not cover (fail closed: the scheduler
///   does not run the budget during a grace window at all, so a graced
///   verdict here is a caller's mistake that must not delete).
///
/// The rest come in two runs, each oldest first: the candidates the
/// **coarsest** tier does not keep, then the ones it does. "Kept by the
/// coarsest tier" is the rule's own test — the oldest candidate of its
/// coarsest bucket, that bucket inside the coarsest window — so the long
/// history the policy promises (the monthlies of `… 1mo:1y`) is shed
/// last, and the finer, shorter-lived tiers' extra points first (snapper's
/// `SPACE_LIMIT`, VSS `MaxSize`). Ties order as [`evaluate`] does (time,
/// then id), so the order is a function of the inputs alone.
pub fn budget_order(
    policy: &SnapPolicy,
    policy_ino: u64,
    snaps: &[SnapFacts],
    verdicts: &[Verdict],
) -> Vec<String> {
    let Some(tz) = policy_time_zone(policy) else {
        return Vec::new();
    };
    let rule = Rule { policy, tz };
    let cands = rule.candidates(policy_ino, snaps);
    let n = cands.len();
    let coarsest = policy.coarsest();
    let mut by_coarsest = vec![false; n];
    rule.for_each_kept_representative(&cands, |i, tier| {
        if Some(tier.every) == coarsest {
            by_coarsest[i] = true;
        }
    });
    let floor = n.saturating_sub(rule.last());
    let eligible = |pos: usize| {
        if pos >= floor {
            return false;
        }
        match verdicts.get(cands[pos].snap) {
            Some(v) => v.keep && v.reasons.iter().all(|r| matches!(r, Reason::Tier(_))),
            None => false,
        }
    };
    let id = |pos: usize| snaps[cands[pos].snap].id.clone();
    let mut order: Vec<String> = (0..n)
        .filter(|&pos| eligible(pos) && !by_coarsest[pos])
        .map(id)
        .collect();
    order.extend(
        (0..n)
            .filter(|&pos| eligible(pos) && by_coarsest[pos])
            .map(id),
    );
    order
}

/// Step 3.3's creation rule: is this root due for a snapshot at
/// `now_ms`?
///
/// Due means no auto snapshot of this root — **candidate or held**,
/// since a held snapshot still covers its bucket — was created inside
/// the current finest bucket. A `paused` policy is never due (the
/// maintenance switch stops creation as well as expiry), and neither is
/// a tierless one, which only a hand-built [`SnapPolicy`] can be.
///
/// This is catch-up, not backfill: after downtime the root is due
/// *once*, and the snapshot taken now becomes the first of the current
/// bucket. Missed buckets are never filled in.
pub fn due(policy: &SnapPolicy, policy_ino: u64, snaps: &[SnapFacts], now_ms: i64) -> bool {
    if policy.paused {
        return false;
    }
    let (Some(finest), Some(tz)) = (policy.finest(), policy_time_zone(policy)) else {
        return false;
    };
    let rule = Rule { policy, tz };
    let (from, to) = rule.bucket_bounds(finest, now_ms);
    !snaps.iter().any(|s| {
        s.origin.is_auto()
            && s.policy_ino == policy_ino
            && s.created_unix_ms >= from
            && s.created_unix_ms < to
    })
}

/// Step 3.3's name for the snapshot a policy takes at `now_ms`: `auto-`
/// and the UTC start of the current finest bucket in basic ISO-8601
/// (`auto-20260928T1405Z`; `auto-20260928T140510Z` for a sub-minute
/// tier). The same name [`simulate`] gives its synthetic snapshots.
///
/// The name is the bucket, never the creation instant: two leaders (one
/// retrying after the other's failover) asking for the same bucket ask
/// for the same name, and the `snaps/` create-if-absent lets only one of
/// them create it. `None` exactly when [`due`] could never be true: no
/// tier, or a timezone outside the bundled tzdb.
pub fn auto_name(policy: &SnapPolicy, now_ms: i64) -> Option<String> {
    let finest = policy.finest()?;
    let tz = policy_time_zone(policy)?;
    let rule = Rule { policy, tz };
    Some(synthetic_name(rule.bucket(finest, now_ms), finest))
}

/// The current finest bucket at `now_ms`, `[start, end)` in Unix ms:
/// the window [`due`] looks for a snapshot in. `None` as for
/// [`auto_name`].
pub fn current_bucket(policy: &SnapPolicy, now_ms: i64) -> Option<(i64, i64)> {
    let finest = policy.finest()?;
    let tz = policy_time_zone(policy)?;
    Some(Rule { policy, tz }.bucket_bounds(finest, now_ms))
}

// --- simulation ------------------------------------------------------

/// The number of synthetic snapshots [`simulate`] will create before it
/// gives up and reports [`Timeline::truncated`]. A 30-day horizon on a
/// 5-minute cadence is 8640, which fits; the same horizon on a
/// (test-only) `10s` cadence does not, and a UI asking for it gets a
/// truncated answer rather than a hung node.
pub const MAX_SYNTHETIC: usize = 10_000;

/// The expiry work [`simulate`] will do before it gives up and reports
/// [`Timeline::truncated`], counted in live candidates evaluated, summed
/// over every expiry pass.
///
/// Capping creations alone does not bound the time: each pass looks at
/// every live candidate, so a policy that keeps most of what it creates
/// (`1m:1y`, `1m:*`), or a directory with many existing candidates,
/// costs the *square* of its creations. 2²⁷ evaluations is about a
/// second of a release build; the densest policy the plan's examples
/// table settles (`5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y`, ~114 000
/// creations over ~400 live) needs under half of it.
pub const MAX_WORK: u64 = 1 << 27;

/// How much a simulation may do; past any of these it stops and reports
/// [`Timeline::truncated`]. [`simulate`] runs under
/// [`SimLimits::DEFAULT`]; `policy check` chooses its own (see
/// `snapsched::check`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimLimits {
    /// Synthetic snapshots created.
    pub max_synthetic: usize,
    /// Live candidates: the simulation stops before a creation once this
    /// many of the policy's own snapshots are alive.
    pub max_live: usize,
    /// Live candidates evaluated, summed over every expiry pass; see
    /// [`MAX_WORK`].
    pub max_work: u64,
}

impl SimLimits {
    /// [`simulate`]'s limits: the UI's timeline returns every snapshot,
    /// so creations are capped at [`MAX_SYNTHETIC`]; the work at
    /// [`MAX_WORK`] (existing rows can make each pass large).
    pub const DEFAULT: SimLimits = SimLimits {
        max_synthetic: MAX_SYNTHETIC,
        max_live: usize::MAX,
        max_work: MAX_WORK,
    };
}

/// What [`simulate`] returns: every snapshot's fate plus the
/// snapshot-count-over-time series the UI plots (Step 7.4).
///
/// Serialized field names are **stable** — the control protocol returns
/// this value verbatim:
///
/// | field | meaning |
/// |---|---|
/// | `policy` | the canonical policy expression that was simulated |
/// | `policy_ino` | the root the simulation was run for |
/// | `paused` | the policy carries `paused`; the scheduler would do nothing until it is resumed, while the simulation still shows what the expression means |
/// | `now_unix_ms` | the simulation's starting instant (the caller's clock) |
/// | `horizon_unix_ms` | the last instant simulated, `now + horizon` |
/// | `cadence` | the finest tier's interval, canonical form (`"5m"`), or `null` for a tierless policy |
/// | `snapshots` | one entry per snapshot: the existing ones in the order they were given, then the synthetic ones oldest first |
/// | `counts` | the step series: one point at `now`, then one per creation |
/// | `created` | synthetic snapshots the simulation created |
/// | `expired` | snapshots the simulation expired, existing and synthetic |
/// | `final_count` | snapshots alive at the horizon |
/// | `steady_state_bound` | [`SnapPolicy::steady_state_bound`], the plan's upper bound, for comparison |
/// | `truncated` | the simulation stopped before the horizon: it needed more than [`MAX_SYNTHETIC`] creations or [`MAX_WORK`] evaluations ([`SimLimits`]); `counts`' last point is where it stopped |
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Timeline {
    pub policy: String,
    pub policy_ino: u64,
    pub paused: bool,
    pub now_unix_ms: i64,
    pub horizon_unix_ms: i64,
    pub cadence: Option<String>,
    pub snapshots: Vec<TimelineSnap>,
    pub counts: Vec<CountPoint>,
    pub created: u32,
    pub expired: u32,
    pub final_count: u32,
    pub steady_state_bound: Option<u64>,
    pub truncated: bool,
    /// Live candidates evaluated over every expiry pass, the quantity
    /// [`SimLimits::max_work`] bounds. Not part of the protocol.
    #[serde(skip)]
    pub work: u64,
}

/// One snapshot in a [`Timeline`]. Field names are stable; see
/// [`Timeline`].
///
/// | field | meaning |
/// |---|---|
/// | `id` | the snapshot id; for a synthetic one, the name Step 3.3 would give it (`auto-20260928T1405Z`) |
/// | `created_unix_ms` | when it exists from |
/// | `synthetic` | the simulation invented it |
/// | `candidate` | this policy's rule may expire it |
/// | `held` / `held_by` | the hold and its owner, for the pin glyph |
/// | `keep` | its verdict at the end of the simulation (or at the moment it was expired) |
/// | `reasons` | why it is kept, finest tier first |
/// | `expires_unix_ms` | the forecast, `null` when kept forever or already expired |
/// | `expired_unix_ms` | when the simulation actually deleted it, `null` if it survived to the horizon |
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TimelineSnap {
    pub id: String,
    pub created_unix_ms: i64,
    pub synthetic: bool,
    pub candidate: bool,
    pub held: bool,
    pub held_by: Option<String>,
    pub keep: bool,
    pub reasons: Vec<Reason>,
    #[serde(rename = "expires_unix_ms")]
    pub expires_at: Option<i64>,
    #[serde(rename = "expired_unix_ms")]
    pub expired_at: Option<i64>,
}

/// One point of the count-over-time step chart. `total` counts every
/// live snapshot (held and manual included), `candidates` only the ones
/// the policy owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CountPoint {
    pub at_unix_ms: i64,
    pub total: u32,
    pub candidates: u32,
}

/// Run `policy` forward from `now_ms` for `horizon_ms`, creating the
/// snapshots its schedule would create and expiring what its rule would
/// expire after each one.
///
/// `horizon_ms` is a **duration** forward from `now_ms`, as the plan's
/// `snap_policy_simulate {horizon_ms}` is. Synthetic snapshots land on
/// the finest tier's schedule — the first instant of each bucket, or
/// `now_ms` for the current bucket if it is not already covered, which
/// is exactly Step 3.3's catch-up. Buckets already covered by an
/// existing snapshot get none.
///
/// The simulation starts with one expiry pass at `now_ms`, because that
/// is what the scheduler's next tick does; `counts[0]` is the result.
/// Existing non-candidates (manual, held, another root's) never expire
/// and are carried through to the horizon.
pub fn simulate(
    policy: &SnapPolicy,
    policy_ino: u64,
    existing: &[SnapFacts],
    now_ms: i64,
    horizon_ms: i64,
) -> Timeline {
    simulate_capped(
        policy,
        policy_ino,
        existing,
        now_ms,
        horizon_ms,
        SimLimits::DEFAULT,
    )
}

/// [`simulate`] under caller-chosen [`SimLimits`] in place of
/// [`SimLimits::DEFAULT`]. `policy check` wants one settled count, not a
/// timeline to ship, and a settled yearly policy on a 5-minute cadence
/// needs ten times the UI's creations (see `snapsched::check`).
pub fn simulate_capped(
    policy: &SnapPolicy,
    policy_ino: u64,
    existing: &[SnapFacts],
    now_ms: i64,
    horizon_ms: i64,
    limits: SimLimits,
) -> Timeline {
    let horizon_end = now_ms.saturating_add(horizon_ms.max(0));
    let mut timeline = Timeline {
        policy: policy.to_string(),
        policy_ino,
        paused: policy.paused,
        now_unix_ms: now_ms,
        horizon_unix_ms: horizon_end,
        cadence: policy.finest().map(|i| i.to_string()),
        snapshots: Vec::with_capacity(existing.len()),
        counts: Vec::new(),
        created: 0,
        expired: 0,
        final_count: 0,
        steady_state_bound: policy.steady_state_bound(),
        truncated: false,
        work: 0,
    };

    let Some(tz) = policy_time_zone(policy) else {
        // Unevaluable policy: nothing is created, nothing expires.
        timeline.snapshots = existing
            .iter()
            .map(|s| snap_entry(s, false, false, &Verdict::one(Reason::NotCandidate), None))
            .collect();
        timeline.final_count = timeline.snapshots.len() as u32;
        timeline.counts.push(CountPoint {
            at_unix_ms: now_ms,
            total: timeline.final_count,
            candidates: 0,
        });
        return timeline;
    };
    let rule = Rule { policy, tz };

    // Every snapshot the simulation will ever know about, existing
    // first. `facts` and `timeline.snapshots` stay index-aligned.
    let mut facts: Vec<SnapFacts> = existing.to_vec();
    let mut synthetic = vec![false; facts.len()];
    // The live candidate set, oldest first; non-candidates are live
    // forever and only counted.
    let mut live = rule.candidates(policy_ino, &facts);
    let live_others = facts.len() - live.len();
    let mut expired_at: Vec<Option<i64>> = vec![None; facts.len()];

    expire_pass(
        &rule,
        &mut live,
        &mut expired_at,
        now_ms,
        &mut timeline,
        live_others,
    );

    if let Some(finest) = policy.finest() {
        let (mut from, mut to) = rule.bucket_bounds(finest, now_ms);
        let mut covered: Vec<i64> = facts
            .iter()
            .filter(|s| s.origin.is_auto() && s.policy_ino == policy_ino)
            .map(|s| s.created_unix_ms)
            .collect();
        covered.sort_unstable();
        while from <= horizon_end {
            let at = from.max(now_ms);
            if at > horizon_end {
                break;
            }
            let first = covered.partition_point(|c| *c < from);
            let already = covered.get(first).is_some_and(|c| *c < to);
            if !already {
                if timeline.created as usize >= limits.max_synthetic
                    || live.len() >= limits.max_live
                    || timeline.work >= limits.max_work
                {
                    timeline.truncated = true;
                    break;
                }
                let facts_index = facts.len();
                let fact = SnapFacts {
                    id: synthetic_name(at, finest),
                    created_unix_ms: at,
                    origin: Origin::Auto,
                    policy_ino,
                    held: false,
                    held_by: None,
                };
                facts.push(fact);
                synthetic.push(true);
                expired_at.push(None);
                // `live` must stay oldest-first: an *existing* snapshot
                // dated in the future is newer than the snapshots the
                // simulation is about to create.
                let cand = rule.cand(facts_index, at);
                let pos = live.partition_point(|c| c.created <= at);
                live.insert(pos, cand);
                timeline.created += 1;
                expire_pass(
                    &rule,
                    &mut live,
                    &mut expired_at,
                    at,
                    &mut timeline,
                    live_others,
                );
            }
            let next = rule.next_bucket(finest, from);
            if next <= from {
                break;
            }
            from = next;
            to = rule.next_bucket(finest, from);
        }
    }

    // Final verdicts: the full rule (with reasons and forecasts) over
    // whatever is still alive, and the expiry time for the rest.
    let mut final_verdicts: Vec<Option<Verdict>> = vec![None; facts.len()];
    for (pos, verdict) in rule.decide(&live).into_iter().enumerate() {
        final_verdicts[live[pos].snap] = Some(verdict);
    }
    for (i, fact) in facts.iter().enumerate() {
        let candidate = fact.is_candidate(policy_ino);
        let verdict = final_verdicts[i].take().unwrap_or_else(|| {
            if candidate {
                Verdict::expired()
            } else if fact.held {
                Verdict::one(Reason::Held(fact.held_by.clone()))
            } else {
                Verdict::one(Reason::NotCandidate)
            }
        });
        timeline.snapshots.push(snap_entry(
            fact,
            synthetic[i],
            candidate,
            &verdict,
            expired_at[i],
        ));
    }
    timeline.final_count = (live.len() + live_others) as u32;
    timeline
}

/// One expiry pass: drop every live candidate the rule no longer
/// keeps, remember when each died, and record the count point.
fn expire_pass(
    rule: &Rule<'_>,
    live: &mut Vec<Cand>,
    expired_at: &mut [Option<i64>],
    at: i64,
    timeline: &mut Timeline,
    live_others: usize,
) {
    timeline.work = timeline.work.saturating_add(live.len() as u64);
    let keep = rule.keep_mask(live);
    let mut kept: Vec<Cand> = Vec::with_capacity(live.len());
    for (pos, cand) in live.drain(..).enumerate() {
        if keep[pos] {
            kept.push(cand);
        } else {
            expired_at[cand.snap] = Some(at);
            timeline.expired += 1;
        }
    }
    *live = kept;
    timeline.counts.push(CountPoint {
        at_unix_ms: at,
        total: (live.len() + live_others) as u32,
        candidates: live.len() as u32,
    });
}

fn snap_entry(
    fact: &SnapFacts,
    synthetic: bool,
    candidate: bool,
    verdict: &Verdict,
    expired_at: Option<i64>,
) -> TimelineSnap {
    TimelineSnap {
        id: fact.id.clone(),
        created_unix_ms: fact.created_unix_ms,
        synthetic,
        candidate,
        held: fact.held,
        held_by: fact.held_by.clone(),
        keep: verdict.keep && expired_at.is_none(),
        reasons: verdict.reasons.to_vec(),
        expires_at: verdict.expires_at,
        expired_at,
    }
}

/// Step 3.3's name for the snapshot of a bucket: UTC basic ISO-8601, so
/// DST never produces two identical labels, with seconds only for the
/// test-only sub-minute cadences.
fn synthetic_name(at_unix_ms: i64, finest: Interval) -> String {
    let Ok(ts) = jiff::Timestamp::from_millisecond(at_unix_ms) else {
        return format!("auto-{at_unix_ms}");
    };
    let fmt = if matches!(finest, Interval::Seconds(_)) {
        "auto-%Y%m%dT%H%M%SZ"
    } else {
        "auto-%Y%m%dT%H%MZ"
    };
    ts.strftime(fmt).to_string()
}

// --- internals -------------------------------------------------------

/// One candidate, with its bucket start in every tier precomputed. The
/// simulation re-decides after every creation, and the calendar is by
/// far the expensive part of a decision, so it is computed once per
/// (snapshot, tier) and never again.
struct Cand {
    /// Index into the caller's snapshot slice.
    snap: usize,
    created: i64,
    buckets: SmallVec<[i64; 6]>,
}

struct Rule<'p> {
    policy: &'p SnapPolicy,
    tz: TimeZone,
}

impl Rule<'_> {
    fn bucket(&self, interval: Interval, t: i64) -> i64 {
        bucket_start(
            interval,
            t,
            &self.tz,
            self.policy.day_start,
            self.policy.week_start,
        )
    }

    fn next_bucket(&self, interval: Interval, t: i64) -> i64 {
        #[cfg(test)]
        tests::NEXT_BUCKET_CALLS.with(|c| c.set(c.get() + 1));
        next_bucket_start(
            interval,
            t,
            &self.tz,
            self.policy.day_start,
            self.policy.week_start,
        )
    }

    fn bucket_bounds(&self, interval: Interval, t: i64) -> (i64, i64) {
        let from = self.bucket(interval, t);
        (from, self.next_bucket(interval, from))
    }

    fn cand(&self, snap: usize, created: i64) -> Cand {
        Cand {
            snap,
            created,
            buckets: self
                .policy
                .tiers
                .iter()
                .map(|t| self.bucket(t.every, created))
                .collect(),
        }
    }

    /// This policy's candidates out of `snaps`, oldest first. Ties on
    /// `created_unix_ms` break on the id, so "the oldest in the bucket"
    /// is deterministic for two snapshots of the same millisecond.
    fn candidates(&self, policy_ino: u64, snaps: &[SnapFacts]) -> Vec<Cand> {
        let mut found: Vec<(usize, &SnapFacts)> = snaps
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_candidate(policy_ino))
            .collect();
        found.sort_by(|a, b| {
            a.1.created_unix_ms
                .cmp(&b.1.created_unix_ms)
                .then_with(|| a.1.id.cmp(&b.1.id))
                .then_with(|| a.0.cmp(&b.0))
        });
        found
            .into_iter()
            .map(|(i, s)| self.cand(i, s.created_unix_ms))
            .collect()
    }

    /// Visit every `(candidate, tier)` pair where the candidate is that
    /// tier's bucket representative **and** the bucket is inside the
    /// tier's window — i.e. every reason of the form "kept by tier I".
    /// Tiers are visited finest first, which is the order
    /// [`Verdict::reasons`] promises.
    ///
    /// The candidates are sorted and [`bucket_start`] is monotone, so
    /// each bucket is one run of equal values and its representative is
    /// the run's first element. That is the whole of the rule's
    /// bucketing: no map, no allocation.
    fn for_each_kept_representative(&self, cands: &[Cand], mut visit: impl FnMut(usize, Tier)) {
        let n = cands.len();
        if n == 0 {
            return;
        }
        for (j, tier) in self.policy.tiers.iter().enumerate() {
            let edge = self.edge(*tier, cands[n - 1].buckets[j]);
            let mut i = 0;
            while i < n {
                let b = cands[i].buckets[j];
                if edge.is_none_or(|e| b > e) {
                    visit(i, *tier);
                }
                i += 1;
                while i < n && cands[i].buckets[j] == b {
                    i += 1;
                }
            }
        }
    }

    /// The window edge of tier `j`: `bucket_start_I(anchor) − K`, or
    /// `None` for `K = *` (no edge; everything is inside).
    fn edge(&self, tier: Tier, anchor_bucket: i64) -> Option<i64> {
        subtract_keep(anchor_bucket, tier.keep, tier.every, &self.tz)
    }

    /// Keep flags only — no reasons, no forecasts, no allocation per
    /// candidate. This is what the simulation runs after every
    /// creation.
    fn keep_mask(&self, cands: &[Cand]) -> Vec<bool> {
        let n = cands.len();
        let mut keep = vec![false; n];
        self.for_each_kept_representative(cands, |i, _| keep[i] = true);
        for k in keep.iter_mut().skip(n.saturating_sub(self.last())) {
            *k = true;
        }
        keep
    }

    fn last(&self) -> usize {
        self.policy.last.max(1) as usize
    }

    /// The full verdicts: every keeping reason, finest tier first, and
    /// the `expires_at` forecast.
    fn decide(&self, cands: &[Cand]) -> Vec<Verdict> {
        let n = cands.len();
        let mut out: Vec<Verdict> = (0..n).map(|_| Verdict::expired()).collect();
        if n == 0 {
            return out;
        }
        let anchor_created = cands[n - 1].created;
        self.for_each_kept_representative(cands, |i, tier| {
            out[i].reasons.push(Reason::Tier(tier.every))
        });
        let first_last = n.saturating_sub(self.last());
        for verdict in out.iter_mut().skip(first_last) {
            verdict.reasons.push(Reason::Last(self.policy.last));
        }
        // Walked once for the whole floor, not once per snapshot in it:
        // a `last=` in the thousands over thousands of candidates would
        // otherwise cost their product in calendar steps (32-M1b review).
        let floor = self.last_schedule(anchor_created);
        for (i, verdict) in out.iter_mut().enumerate() {
            verdict.keep = !verdict.reasons.is_empty();
            if verdict.keep {
                verdict.expires_at = self.forecast(cands, i, n, first_last, &floor);
            }
        }
        out
    }

    /// `expires_at`: the latest of the keeping reasons' own forecasts,
    /// or `None` if any of them keeps it forever.
    fn forecast(
        &self,
        cands: &[Cand],
        i: usize,
        n: usize,
        first_last: usize,
        floor: &LastSchedule,
    ) -> Option<i64> {
        let mut latest: Option<i64> = None;
        for (j, tier) in self.policy.tiers.iter().enumerate() {
            let b = cands[i].buckets[j];
            // Only a tier that actually keeps it has a say.
            let edge = self.edge(*tier, cands[n - 1].buckets[j]);
            let kept = edge.is_none_or(|e| b > e);
            let is_rep = i == 0 || cands[i - 1].buckets[j] != b;
            if !kept || !is_rep {
                continue;
            }
            latest = Some(
                latest
                    .unwrap_or(i64::MIN)
                    .max(self.tier_forecast(*tier, b)?),
            );
        }
        if i >= first_last {
            let needed = self.last() - (n - 1 - i);
            latest = Some(latest.unwrap_or(i64::MIN).max(floor.forecast(needed)?));
        }
        latest
    }

    /// When tier `I:K` stops keeping the representative of bucket `b`:
    /// the first `I`-bucket whose window edge has reached `b`.
    fn tier_forecast(&self, tier: Tier, b: i64) -> Option<i64> {
        let target = add_keep(b, tier.keep, tier.every, &self.tz)?;
        let mut at = self.bucket(tier.every, target);
        if at < target {
            at = self.next_bucket(tier.every, at);
        }
        // Calendar months are not exactly invertible ("one month after
        // 31 January" is 28 February, whose own "one month before" is
        // 28 January), so walk the last few buckets rather than trust
        // the arithmetic. The forecast is for display; a bounded walk
        // is enough.
        for _ in 0..64 {
            match self.edge(tier, at) {
                Some(e) if b > e => {
                    let next = self.next_bucket(tier.every, at);
                    if next <= at {
                        break;
                    }
                    at = next;
                }
                _ => break,
            }
        }
        Some(at)
    }

    /// The finest-bucket starts after the anchor's, for the `last=n`
    /// forecast: `starts[k]` is the start of the `k`-th finest bucket
    /// after the anchor's (`starts[0]` is the anchor's own). Walked once
    /// per decision, at most `min(last, LAST_WALK_CAP)` steps, so the
    /// floor's forecasts cost O(n + last) together rather than
    /// O(n × last).
    fn last_schedule(&self, anchor_created: i64) -> LastSchedule {
        let Some(finest) = self.policy.finest() else {
            return LastSchedule {
                starts: Vec::new(),
                step_ms: 0,
            };
        };
        let steps = self.last().min(LAST_WALK_CAP);
        let mut starts = Vec::with_capacity(steps + 1);
        let mut at = self.bucket(finest, anchor_created);
        starts.push(at);
        for _ in 0..steps {
            let next = self.next_bucket(finest, at);
            if next <= at {
                break;
            }
            at = next;
            starts.push(at);
        }
        LastSchedule {
            starts,
            step_ms: (finest.nominal_duration() as i64).saturating_mul(1_000),
        }
    }
}

/// How far [`Rule::last_schedule`] walks the calendar. Past it the
/// forecast extrapolates on the nominal cadence: a `last=` in the
/// millions is a display problem, not a retention one.
const LAST_WALK_CAP: usize = 4096;

/// The precomputed `last=n` schedule; see [`Rule::last_schedule`].
struct LastSchedule {
    /// Empty for a tierless policy, which has no schedule.
    starts: Vec<i64>,
    step_ms: i64,
}

impl LastSchedule {
    /// When `last=n` stops keeping a snapshot with `needed` arrivals to
    /// go: the start of the `needed`-th finest bucket after the
    /// anchor's, since on schedule each bucket brings exactly one.
    fn forecast(&self, needed: usize) -> Option<i64> {
        let last = *self.starts.last()?;
        match self.starts.get(needed) {
            Some(at) => Some(*at),
            None => {
                let extra = (needed - (self.starts.len() - 1)) as i64;
                Some(last.saturating_add(extra.saturating_mul(self.step_ms)))
            }
        }
    }
}

// --- serde -----------------------------------------------------------

/// [`Reason`] serializes as a uniform four-key map, so the shape is the
/// same for every variant in both of the control protocol's codecs (no
/// internal tagging, no skipped fields — postcard is not
/// self-describing):
///
/// ```json
/// {"kind": "tier", "every": "1h", "last": null, "held_by": null}
/// {"kind": "last", "every": null, "last": 3,    "held_by": null}
/// {"kind": "held", "every": null, "last": null, "held_by": "csi:abc"}
/// {"kind": "grace", …}  {"kind": "not_candidate", …}
/// ```
impl serde::Serialize for Reason {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = ser.serialize_struct("Reason", 4)?;
        let (kind, every, last, held_by) = match self {
            Reason::Tier(i) => ("tier", Some(i.to_string()), None, None),
            Reason::Last(n) => ("last", None, Some(*n), None),
            Reason::Held(by) => ("held", None, None, by.clone()),
            Reason::Grace => ("grace", None, None, None),
            Reason::NotCandidate => ("not_candidate", None, None, None),
        };
        s.serialize_field("kind", kind)?;
        s.serialize_field("every", &every)?;
        s.serialize_field("last", &last)?;
        s.serialize_field("held_by", &held_by)?;
        s.end()
    }
}

/// `keep`, `reasons`, and `expires_unix_ms` — the `EXPIRES` column's
/// forecast, under the name the rest of the protocol uses for an
/// absolute time.
impl serde::Serialize for Verdict {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = ser.serialize_struct("Verdict", 3)?;
        s.serialize_field("keep", &self.keep)?;
        s.serialize_field("reasons", &self.reasons.as_slice())?;
        s.serialize_field("expires_unix_ms", &self.expires_at)?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapsched::policy::Keep;
    use proptest::prelude::*;
    use std::cell::Cell;

    thread_local! {
        /// Calendar steps taken by [`Rule::next_bucket`] on this
        /// thread, for the cost regression tests.
        pub(super) static NEXT_BUCKET_CALLS: Cell<u64> = const { Cell::new(0) };
    }

    fn next_bucket_calls() -> u64 {
        NEXT_BUCKET_CALLS.with(Cell::get)
    }

    fn policy(src: &str) -> SnapPolicy {
        SnapPolicy::parse(src).unwrap_or_else(|e| panic!("parse {src:?}: {e}"))
    }

    const INO: u64 = 42;

    /// An `origin=auto` snapshot of [`INO`], created `at`.
    fn auto(at: i64) -> SnapFacts {
        SnapFacts {
            id: format!("s{at}"),
            created_unix_ms: at,
            origin: Origin::Auto,
            policy_ino: INO,
            held: false,
            held_by: None,
        }
    }

    fn manual(at: i64) -> SnapFacts {
        SnapFacts {
            origin: Origin::Manual,
            policy_ino: 0,
            ..auto(at)
        }
    }

    fn held(at: i64, by: Option<&str>) -> SnapFacts {
        SnapFacts {
            held: true,
            held_by: by.map(str::to_string),
            ..auto(at)
        }
    }

    /// 2026-06-15T00:00:00Z, a Monday, so weekly buckets start on it.
    const T0: i64 = 1_781_481_600_000;

    const MIN: i64 = 60_000;
    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 86_400_000;

    fn kept(verdicts: &[Verdict]) -> Vec<bool> {
        verdicts.iter().map(|v| v.keep).collect()
    }

    // --- hand cases --------------------------------------------------

    /// `5m:1d` keeps exactly 288 buckets, the anchor's included, and the
    /// 289th-newest bucket is gone. The plan states the figure.
    #[test]
    fn five_minute_day_keeps_exactly_288_buckets() {
        let p = policy("5m:1d");
        // One snapshot per 5m bucket for two days.
        let snaps: Vec<SnapFacts> = (0..576).map(|i| auto(T0 + i * 5 * MIN)).collect();
        let v = evaluate(&p, INO, &snaps);
        let survivors = v.iter().filter(|v| v.keep).count();
        assert_eq!(survivors, 288);
        // They are the newest 288, contiguous, ending at the anchor.
        assert!(v[576 - 288..].iter().all(|v| v.keep));
        assert!(v[..576 - 288].iter().all(|v| !v.keep));
    }

    /// Union semantics (L2): one snapshot can be the representative of
    /// every tier at once, and `reasons` says so, finest first.
    #[test]
    fn reasons_list_every_keeping_tier_finest_first() {
        let p = policy("5m:1d 1h:7d 1d:30d");
        let mut snaps = vec![auto(T0)]; // midnight: 5m, 1h and 1d rep
        snaps.push(auto(T0 + 5 * MIN)); // its own 5m bucket only
        snaps.push(auto(T0 + HOUR)); // a 5m and an 1h rep
        let v = evaluate(&p, INO, &snaps);
        assert_eq!(
            v[0].reasons.as_slice(),
            [
                Reason::Tier(Interval::Minutes(5)),
                Reason::Tier(Interval::Hours(1)),
                Reason::Tier(Interval::Day),
            ]
        );
        assert_eq!(
            v[1].reasons.as_slice(),
            [Reason::Tier(Interval::Minutes(5))]
        );
        assert_eq!(
            v[2].reasons.as_slice(),
            [
                Reason::Tier(Interval::Minutes(5)),
                Reason::Tier(Interval::Hours(1)),
                Reason::Last(1),
            ]
        );
        assert!(v.iter().all(|v| v.keep));
    }

    /// `last=n` is a floor under the rule: the newest `n` candidates
    /// survive whatever the tiers say.
    #[test]
    fn last_is_a_floor_under_the_tiers() {
        // Four snapshots in the same 5m bucket, a tier that keeps one
        // day. Only the oldest is a representative.
        let snaps: Vec<SnapFacts> = (0..4).map(|i| auto(T0 + i * 1_000)).collect();
        let v = evaluate(&policy("5m:1d"), INO, &snaps);
        assert_eq!(kept(&v), [true, false, false, true]);
        assert_eq!(v[3].reasons.as_slice(), [Reason::Last(1)]);
        let v = evaluate(&policy("5m:1d; last=3"), INO, &snaps);
        assert_eq!(kept(&v), [true, true, true, true]);
        assert_eq!(v[1].reasons.as_slice(), [Reason::Last(3)]);
        // A `last` larger than the history keeps everything.
        let v = evaluate(&policy("5m:1d; last=99"), INO, &snaps);
        assert!(v.iter().all(|v| v.keep));
    }

    /// A `*` tier keeps its representatives forever, and says so by
    /// having no forecast.
    #[test]
    fn star_tiers_keep_forever_and_have_no_forecast() {
        let p = policy("1d:14d 1mo:*; day-start=02:00");
        // One snapshot a day for 400 days, from 2025-01-01.
        let base = 1_735_689_600_000; // 2025-01-01T00:00:00Z
        let snaps: Vec<SnapFacts> = (0..400).map(|i| auto(base + i * DAY + 3 * HOUR)).collect();
        let v = evaluate(&p, INO, &snaps);
        let monthly: Vec<usize> = v
            .iter()
            .enumerate()
            .filter(|(_, v)| v.tiers().any(|t| t == Interval::Month))
            .map(|(i, _)| i)
            .collect();
        // 400 days spans 14 month buckets, all kept, none forecastable.
        assert_eq!(monthly.len(), 14);
        for i in &monthly {
            assert!(v[*i].keep);
            assert_eq!(v[*i].expires_at, None, "snapshot {i} has a forecast");
        }
        // The daily tier keeps 14 buckets; a day that is neither is gone.
        assert!(!v[200].keep, "{:?}", v[200]);
        assert!(v[399].keep);
    }

    /// Held, manual and other roots' snapshots are untouched, are never
    /// counted, and never shift which candidate is a representative.
    #[test]
    fn non_candidates_are_kept_and_never_shift_a_representative() {
        let p = policy("1h:7d");
        // Two candidates in the same hour; the older is the rep. A
        // held, a manual and another root's snapshot are *older* still
        // and in the same bucket — none of them may become the rep.
        let snaps = vec![
            held(T0, None),
            held(T0 + 1, Some("user:ops")),
            held(T0 + 2, Some("csi:pvc-1234")),
            manual(T0 + 3),
            SnapFacts {
                policy_ino: 7,
                ..auto(T0 + 4)
            },
            auto(T0 + 5),
            auto(T0 + 6),
        ];
        let v = evaluate(&p, INO, &snaps);
        assert!(v.iter().all(|v| v.keep), "{v:?}");
        assert_eq!(v[0].reasons.as_slice(), [Reason::Held(None)]);
        assert_eq!(
            v[1].reasons.as_slice(),
            [Reason::Held(Some("user:ops".into()))]
        );
        assert_eq!(
            v[2].reasons.as_slice(),
            [Reason::Held(Some("csi:pvc-1234".into()))]
        );
        assert_eq!(v[3].reasons.as_slice(), [Reason::NotCandidate]);
        assert_eq!(v[4].reasons.as_slice(), [Reason::NotCandidate]);
        // The two real candidates: the older is the hourly rep, the
        // newer survives on `last`.
        assert_eq!(v[5].reasons.as_slice(), [Reason::Tier(Interval::Hours(1))]);
        assert_eq!(v[6].reasons.as_slice(), [Reason::Last(1)]);
        // And the non-candidates are not counted by `last` either: with
        // the candidates removed from an otherwise identical history,
        // the verdicts of the rest do not move.
        let only = vec![auto(T0 + 5), auto(T0 + 6)];
        let w = evaluate(&p, INO, &only);
        assert_eq!(v[5].reasons, w[0].reasons);
        assert_eq!(v[6].reasons, w[1].reasons);
    }

    /// A held candidate is outside the set whatever its owner, so it
    /// cannot be the anchor either — the windows do not move when one
    /// is pinned.
    #[test]
    fn a_hold_does_not_move_the_anchor() {
        let p = policy("1d:7d");
        let mut snaps: Vec<SnapFacts> = (0..10).map(|i| auto(T0 + i * DAY)).collect();
        let without = evaluate(&p, INO, &snaps);
        // Pin a snapshot 100 days in the future with a CSI hold.
        snaps.push(held(T0 + 100 * DAY, Some("csi:x")));
        let with = evaluate(&p, INO, &snaps);
        assert_eq!(kept(&without), kept(&with)[..10]);
        assert!(with[10].keep);
    }

    /// An unparseable timezone cannot delete anything.
    #[test]
    fn a_policy_with_an_unknown_timezone_keeps_everything() {
        let mut p = policy("5m:1d");
        p.tz = "Nowhere/Nothing".into();
        let snaps: Vec<SnapFacts> = (0..600).map(|i| auto(T0 + i * 5 * MIN)).collect();
        let v = evaluate(&p, INO, &snaps);
        assert!(v
            .iter()
            .all(|v| v.keep && v.reasons.as_slice() == [Reason::NotCandidate]));
        let t = simulate(&p, INO, &snaps, T0, 7 * DAY);
        assert_eq!(t.created, 0);
        assert_eq!(t.expired, 0);
        assert_eq!(t.final_count, 600);
    }

    #[test]
    fn grace_keeps_what_only_the_old_policy_keeps() {
        let snaps: Vec<SnapFacts> = (0..40).map(|i| auto(T0 + i * DAY)).collect();
        let old = evaluate(&policy("1d:30d"), INO, &snaps);
        let new = evaluate(&policy("1d:7d"), INO, &snaps);
        assert!(new.iter().filter(|v| !v.keep).count() > 0);
        let graced = grace_intersection(&old, &new);
        // Everything the old policy kept is still here, and the reason
        // says why.
        for i in 0..40 {
            assert_eq!(graced[i].keep, old[i].keep || new[i].keep, "{i}");
            if old[i].keep && !new[i].keep {
                assert_eq!(graced[i].reasons[0], Reason::Grace);
                assert!(graced[i].reasons.len() > 1, "{:?}", graced[i]);
                assert_eq!(graced[i].expires_at, old[i].expires_at);
            }
        }
        // Shortening to a window neither policy covers still expires:
        // the intersection is not "keep everything".
        let newer = evaluate(&policy("1d:2d"), INO, &snaps);
        let graced = grace_intersection(&new, &newer);
        assert_eq!(kept(&graced), kept(&new));
        // First sighting expires nothing at all.
        let first = grace_first_seen(&newer);
        assert!(first.iter().all(|v| v.keep));
        assert!(first
            .iter()
            .zip(&newer)
            .all(|(f, n)| n.keep || f.reasons.as_slice() == [Reason::Grace]));
    }

    #[test]
    fn due_covers_the_current_finest_bucket() {
        let p = policy("1h:7d");
        assert!(due(&p, INO, &[], T0), "an empty root is due");
        let inside = vec![auto(T0 + 59 * MIN)];
        assert!(!due(&p, INO, &inside, T0 + 30 * MIN));
        assert!(due(&p, INO, &inside, T0 + HOUR));
        // A held snapshot covers its bucket, though it is no candidate.
        let pinned = vec![held(T0 + 10 * MIN, Some("csi:x"))];
        assert!(!due(&p, INO, &pinned, T0));
        // Another root's does not.
        let elsewhere = vec![SnapFacts {
            policy_ino: 7,
            ..auto(T0 + 10 * MIN)
        }];
        assert!(due(&p, INO, &elsewhere, T0));
        // A manual snapshot of this root does not cover the bucket
        // either: the policy's own stream is what is scheduled.
        assert!(due(&p, INO, &[manual(T0 + 10 * MIN)], T0));
        // Paused roots are never due.
        assert!(!due(&policy("1h:7d; paused"), INO, &[], T0));
        // Catch-up, not backfill: after a long outage the root is due
        // once, and one snapshot taken now covers the bucket.
        let stale = vec![auto(T0 - 400 * DAY)];
        assert!(due(&p, INO, &stale, T0));
        let caught_up = vec![auto(T0 - 400 * DAY), auto(T0 + 17 * MIN)];
        assert!(!due(&p, INO, &caught_up, T0 + 30 * MIN));
    }

    /// Step 3.3's names: the UTC start of the finest bucket, minutes or
    /// (sub-minute tiers) seconds, the same for every instant inside the
    /// bucket; and the bucket is the window `due` looks in.
    #[test]
    fn the_auto_name_is_the_finest_bucket_in_utc() {
        // 2026-09-28T14:07:31.250Z
        let t = 1_790_604_451_250;
        let p = policy("5m:1d 1h:7d");
        assert_eq!(auto_name(&p, t).as_deref(), Some("auto-20260928T1405Z"));
        assert_eq!(auto_name(&p, t + 2 * MIN), auto_name(&p, t));
        assert_eq!(
            auto_name(&p, t + 3 * MIN).as_deref(),
            Some("auto-20260928T1410Z")
        );
        let s = policy("10s:1m 1m:4m");
        assert_eq!(auto_name(&s, t).as_deref(), Some("auto-20260928T140730Z"));
        // An hourly tier in Budapest (+02:00 in September) still names
        // the bucket in UTC.
        let b = policy("1h:1d; tz=Europe/Budapest");
        assert_eq!(auto_name(&b, t).as_deref(), Some("auto-20260928T1400Z"));
        let (from, to) = current_bucket(&p, t).unwrap();
        assert_eq!(
            (from, to),
            (t - 2 * MIN - 31_250, t - 2 * MIN - 31_250 + 5 * MIN)
        );
        assert!(due(&p, INO, &[auto(from - 1)], t));
        assert!(!due(&p, INO, &[auto(from)], t));
    }

    /// `day-start=02:00` in Budapest, on the day 02:00 does not exist:
    /// the daily bucket starts when the clocks have jumped, and the
    /// snapshot taken then is its representative.
    #[test]
    fn day_start_in_a_dst_gap_still_has_one_daily_representative() {
        let p = policy("1h:2d 1d:30d; tz=Europe/Budapest; day-start=02:00");
        // 2026-03-29, the spring-forward day. Local 00:30 and 01:30 are
        // before the (nonexistent) 02:00 boundary, so they are still
        // the 28th's bucket; 03:30 and 04:30 are the 29th's, whose
        // bucket starts where the clocks landed.
        let base = 1_774_738_800_000; // 2026-03-29T00:00:00+01:00
        let snaps = vec![
            auto(base + 30 * MIN),            // 00:30
            auto(base + 90 * MIN),            // 01:30
            auto(base + 2 * HOUR + 30 * MIN), // 03:30 (02:30 does not exist)
            auto(base + 3 * HOUR + 30 * MIN), // 04:30
        ];
        let v = evaluate(&p, INO, &snaps);
        let daily: Vec<bool> = v
            .iter()
            .map(|v| v.tiers().any(|t| t == Interval::Day))
            .collect();
        // One daily representative per day bucket, the first of each.
        assert_eq!(daily, [true, false, true, false]);
        assert!(v.iter().all(|v| v.keep));
        // The 29th's day bucket starts at the instant local 02:00
        // resumed, i.e. local 03:00 = 01:00Z.
        let tz = super::super::calendar::time_zone("Europe/Budapest").unwrap();
        assert_eq!(
            super::super::calendar::bucket_start(
                Interval::Day,
                base + 2 * HOUR + 30 * MIN,
                &tz,
                (2, 0),
                p.week_start
            ),
            base + 2 * HOUR
        );
    }

    // --- the forecast ------------------------------------------------

    #[test]
    fn expires_at_is_the_last_window_that_keeps_it() {
        let p = policy("1h:4h");
        // Hourly snapshots; the newest is the anchor.
        let snaps: Vec<SnapFacts> = (0..4).map(|i| auto(T0 + i * HOUR)).collect();
        let v = evaluate(&p, INO, &snaps);
        assert!(v.iter().all(|x| x.keep));
        // The oldest sits in the bucket T0. It stops being kept once
        // the anchor reaches T0 + 4h, which on schedule is then.
        assert_eq!(v[0].expires_at, Some(T0 + 4 * HOUR));
        assert_eq!(v[1].expires_at, Some(T0 + 5 * HOUR));
        // The newest is also kept by `last=1`, which outlasts the tier
        // only until the next snapshot arrives.
        assert_eq!(v[3].expires_at, Some(T0 + 7 * HOUR));
        // An expired snapshot has no forecast; `keep` says which case
        // `None` is.
        let snaps: Vec<SnapFacts> = (0..8).map(|i| auto(T0 + i * HOUR)).collect();
        let v = evaluate(&p, INO, &snaps);
        assert!(!v[0].keep && v[0].expires_at.is_none());
    }

    #[test]
    fn expires_at_forecasts_the_last_floor_too() {
        // Kept only by `last=3`: three more arrivals push it out, one
        // per 5m bucket after the anchor's.
        let snaps: Vec<SnapFacts> = (0..4).map(|i| auto(T0 + i * 1_000)).collect();
        let v = evaluate(&policy("5m:1d; last=3"), INO, &snaps);
        assert_eq!(v[1].reasons.as_slice(), [Reason::Last(3)]);
        // It is the third-newest of four, so one more arrival — one
        // more 5m bucket — pushes it out of `last=3`.
        assert_eq!(v[1].expires_at, Some(T0 + 5 * MIN));
        // The newest needs three arrivals.
        assert_eq!(v[3].expires_at, Some(T0 + 3 * 5 * MIN));
        // The oldest is the 5m representative, and that window is the
        // one that outlasts the floor.
        assert_eq!(
            v[0].reasons.as_slice(),
            [Reason::Tier(Interval::Minutes(5))]
        );
        assert_eq!(v[0].expires_at, Some(T0 + DAY));
    }

    /// The `last=` forecast walks the calendar once per decision, not
    /// once per snapshot inside the floor. A huge floor over the
    /// `MAX_PER_ROOT`-sized history took 14.5 s per `evaluate` in a
    /// release build when every snapshot walked up to 4096 buckets on
    /// its own (32-M1b review); the step count is the regression check.
    #[test]
    fn last_forecast_costs_one_walk_not_one_per_snapshot() {
        let p = policy("1h:7d; tz=Europe/Budapest; last=1000000");
        let n: i64 = 5_000;
        let snaps: Vec<SnapFacts> = (0..n).map(|i| auto(T0 + i * HOUR)).collect();
        let before = next_bucket_calls();
        let v = evaluate(&p, INO, &snaps);
        let steps = next_bucket_calls() - before;
        assert!(v.iter().all(|v| v.keep));
        assert!(
            steps <= LAST_WALK_CAP as u64 + 1,
            "{steps} calendar steps for one evaluate"
        );
        // Still a forecast, and still ordered: the oldest goes first.
        let first = v[0].expires_at.unwrap();
        let newest = v[n as usize - 1].expires_at.unwrap();
        assert!(first < newest);
        // The newest needs a million arrivals, one an hour: past the
        // walked part it extrapolates on the nominal cadence.
        let anchor_bucket = T0 + (n - 1) * HOUR;
        assert_eq!(newest, anchor_bucket + 1_000_000 * HOUR);

        // A floor the walk covers still lands on real bucket starts.
        let p = policy("1h:1h; tz=Europe/Budapest; last=500");
        let before = next_bucket_calls();
        let v = evaluate(&p, INO, &snaps);
        let steps = next_bucket_calls() - before;
        assert!(steps <= 501, "{steps}");
        assert_eq!(
            v[n as usize - 1].expires_at,
            Some(anchor_bucket + 500 * HOUR)
        );
        assert_eq!(v[n as usize - 500].expires_at, Some(anchor_bucket + HOUR));
    }

    /// A month keep is calendar arithmetic, and "one month after 31
    /// January" is 28 February, whose "one month before" is not the
    /// 31st. The forecast walks past that rather than firing early.
    #[test]
    fn expires_at_handles_month_end_clamping() {
        let p = policy("1d:1mo");
        let jan31 = 1_769_817_600_000; // 2026-01-31T00:00:00Z
        let snaps = vec![auto(jan31), auto(jan31 + DAY)];
        let v = evaluate(&p, INO, &snaps);
        let forecast = v[0].expires_at.unwrap();
        // At the forecast instant the window really has moved past it.
        let later = vec![auto(jan31), auto(jan31 + DAY), auto(forecast)];
        assert!(!evaluate(&p, INO, &later)[0].keep);
        // One bucket earlier it had not.
        let earlier = vec![auto(jan31), auto(jan31 + DAY), auto(forecast - DAY)];
        assert!(evaluate(&p, INO, &earlier)[0].keep);
    }

    // --- simulation --------------------------------------------------

    /// 2026-03-20T00:00:00Z: nine days before Budapest springs forward
    /// (2026-03-29), so a simulation from here crosses the 23-hour day.
    const MAR20: i64 = T0 - 87 * DAY;

    #[test]
    fn simulate_from_an_empty_history_settles_under_the_bound() {
        // The plan's examples table, asserted at *every* point of the
        // count series, not only at the horizon, and from two starts:
        // mid-June (no transition in range) and 20 March, which crosses
        // Budapest's spring-forward. Horizons are long enough for the
        // finer tiers to reach their steady state; the coarser ones fill
        // as far as a debug-build test budget allows.
        for start in [T0, MAR20] {
            for (expr, horizon, bound) in [
                ("1h:1d 1d:7d", 30 * DAY, 31u64),
                ("1h:1d 1d:7d; tz=Europe/Budapest", 30 * DAY, 31),
                ("15m:1d 1h:2d 1d:30d 1mo:1y", 32 * DAY, 186),
                (
                    "5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y; tz=Europe/Budapest",
                    12 * DAY,
                    510,
                ),
                ("1d:7d; last=3; skip-empty=no", 30 * DAY, 7),
                ("1d:7d; tz=Europe/Budapest", 30 * DAY, 7),
                ("1w:4w; tz=Europe/Budapest", 100 * DAY, 4),
            ] {
                let p = policy(expr);
                assert_eq!(p.steady_state_bound(), Some(bound), "{expr}");
                let t = simulate(&p, INO, &[], start, horizon);
                assert!(!t.truncated, "{expr}: truncated");
                assert_eq!(t.steady_state_bound, Some(bound));
                let peak = t.counts.iter().map(|c| c.total).max().unwrap();
                assert!(
                    u64::from(peak) <= bound,
                    "{expr} from {start}: peaked at {peak} snapshots > bound {bound}"
                );
                // Every snapshot is accounted for, and the series ends
                // where the count does.
                assert_eq!(t.snapshots.len(), t.created as usize);
                assert_eq!(
                    t.expired + t.final_count,
                    t.created,
                    "{expr}: {t:?} does not balance"
                );
                assert_eq!(t.counts.last().unwrap().total, t.final_count);
                // The simulated figure is the one `policy check` prints
                // next to the bound, and union semantics make it smaller.
                assert!(t.final_count > 0, "{expr}");
            }
        }

        // The rows whose bound a long enough horizon reaches exactly,
        // DST in range or not: a daily tier holds K calendar days.
        for (expr, horizon, steady) in [
            ("1d:7d; tz=Europe/Budapest", 30 * DAY, 7u32),
            ("1w:4w; tz=Europe/Budapest", 100 * DAY, 4),
            ("1h:1d 1d:7d; tz=Europe/Budapest", 30 * DAY, 30),
        ] {
            let t = simulate(&policy(expr), INO, &[], MAR20, horizon);
            assert_eq!(t.final_count, steady, "{expr}");
        }

        // The one unbounded row: "14 + months elapsed". Over 100 days
        // from 2026-06-15 that is the 14 daily buckets plus the 1st of
        // July, August and September — and the June monthly, which is
        // the first snapshot the simulation takes.
        let p = policy("1d:14d 1mo:*; day-start=02:00");
        assert_eq!(p.steady_state_bound(), None);
        let t = simulate(&p, INO, &[], T0, 100 * DAY);
        assert!(!t.truncated);
        assert_eq!(t.steady_state_bound, None);
        assert_eq!(t.created, 101);
        assert_eq!(t.final_count, 14 + 4);
    }

    /// The exact steady state of the plan's first example, by hand:
    /// `1h:1d` keeps 24 hourly representatives (the last 24 hours) and
    /// `1d:7d` keeps 7 daily ones, of which the newest two fall inside
    /// those 24 hours. 24 + 7 − 2 = 29, or 30 on the hour where the
    /// day boundary is itself the newest hourly bucket.
    #[test]
    fn simulate_reaches_the_hand_computed_steady_state() {
        let p = policy("1h:1d 1d:7d");
        let t = simulate(&p, INO, &[], T0, 21 * DAY);
        assert!(!t.truncated);
        // Stable: the count a full day before the horizon is the same.
        let end = t.counts.last().unwrap();
        let day_before = t
            .counts
            .iter()
            .rev()
            .find(|c| c.at_unix_ms <= end.at_unix_ms - DAY)
            .unwrap();
        assert_eq!(end.total, day_before.total);
        assert!(
            (29..=30).contains(&end.total),
            "steady state is {}",
            end.total
        );
    }

    #[test]
    fn simulate_carries_existing_snapshots_and_expires_the_stale_ones() {
        let p = policy("1h:6h");
        // Ten hourly snapshots already exist; six windows' worth
        // survive the very first pass, plus a manual and a held one.
        let mut snaps: Vec<SnapFacts> = (0..10).map(|i| auto(T0 + i * HOUR)).collect();
        snaps.push(manual(T0));
        snaps.push(held(T0 + 1, Some("csi:x")));
        let t = simulate(&p, INO, &snaps, T0 + 10 * HOUR, 3 * HOUR);
        // The first pass is at `now`, before any creation.
        assert_eq!(t.counts[0].at_unix_ms, T0 + 10 * HOUR);
        assert_eq!(t.counts[0].candidates, 6);
        assert_eq!(t.counts[0].total, 8); // + the manual and the held
                                          // The current bucket (nothing in it yet) plus three more.
        assert_eq!(t.created, 4);
        // The manual and held ones are never expired and never counted
        // as candidates.
        let manual_entry = t
            .snapshots
            .iter()
            .find(|s| !s.candidate && !s.held)
            .unwrap();
        assert!(manual_entry.keep && manual_entry.expired_at.is_none());
        let held_entry = t.snapshots.iter().find(|s| s.held).unwrap();
        assert_eq!(held_entry.held_by.as_deref(), Some("csi:x"));
        assert!(held_entry.keep && held_entry.expired_at.is_none());
        // Synthetic names are the bucket, in UTC basic ISO-8601.
        let synth: Vec<&str> = t
            .snapshots
            .iter()
            .filter(|s| s.synthetic)
            .map(|s| s.id.as_str())
            .collect();
        assert_eq!(
            synth,
            [
                "auto-20260615T1000Z",
                "auto-20260615T1100Z",
                "auto-20260615T1200Z",
                "auto-20260615T1300Z"
            ]
        );
        // Expired snapshots carry the instant they died.
        let dead: Vec<i64> = t.snapshots.iter().filter_map(|s| s.expired_at).collect();
        assert_eq!(dead.len() as u32, t.expired);
        assert!(dead.iter().all(|d| *d >= T0 + 10 * HOUR));
    }

    /// A bucket an existing snapshot already covers gets no synthetic
    /// one, and the current bucket's creation is catch-up (at `now`),
    /// not at the bucket's start.
    #[test]
    fn simulate_skips_covered_buckets_and_catches_up_at_now() {
        let p = policy("1h:7d");
        let now = T0 + 30 * MIN;
        let t = simulate(&p, INO, &[auto(T0 + 10 * MIN)], now, 2 * HOUR);
        let made: Vec<i64> = t
            .snapshots
            .iter()
            .filter(|s| s.synthetic)
            .map(|s| s.created_unix_ms)
            .collect();
        assert_eq!(made, [T0 + HOUR, T0 + 2 * HOUR]);
        // With nothing in the current bucket, the first creation is now.
        let t = simulate(&p, INO, &[], now, 2 * HOUR);
        let made: Vec<i64> = t
            .snapshots
            .iter()
            .filter(|s| s.synthetic)
            .map(|s| s.created_unix_ms)
            .collect();
        assert_eq!(made, [now, T0 + HOUR, T0 + 2 * HOUR]);
    }

    /// An existing snapshot dated in the future (a clock that was
    /// ahead, or a caller simulating from a past `now`) is newer than
    /// everything the simulation creates, and must stay the anchor
    /// rather than corrupt the ordering.
    #[test]
    fn simulate_handles_an_existing_snapshot_from_the_future() {
        let p = policy("1h:3h");
        let snaps = vec![auto(T0), auto(T0 + 100 * HOUR)];
        let t = simulate(&p, INO, &snaps, T0 + HOUR, 3 * HOUR);
        assert_eq!(t.created, 4);
        // The future snapshot is the anchor, so the window is its own
        // bucket and the two before it: everything the simulation
        // creates around `now` is outside it, and the `last=1` floor is
        // held by the future snapshot itself.
        let future = t
            .snapshots
            .iter()
            .find(|s| s.created_unix_ms == T0 + 100 * HOUR)
            .unwrap();
        assert!(future.keep && future.expired_at.is_none());
        assert_eq!(future.reasons.last(), Some(&Reason::Last(1)));
        assert_eq!(t.final_count, 1);
        assert_eq!(t.expired, 5);
    }

    #[test]
    fn simulate_truncates_rather_than_running_forever() {
        // A 10s cadence over a year is far more than MAX_SYNTHETIC.
        let p = policy("10s:10min 1h:1d");
        let t = simulate(&p, INO, &[], T0, 365 * DAY);
        assert!(t.truncated);
        assert_eq!(t.created as usize, MAX_SYNTHETIC);
    }

    /// Creations do not bound the time when each pass is large: a
    /// directory full of candidates the policy keeps makes every pass
    /// look at all of them. The work cap stops it, the live cap too.
    #[test]
    fn simulate_truncates_on_work_and_on_live_candidates() {
        let p = policy("1m:*");
        let history: Vec<SnapFacts> = (0..2000).map(|i| auto(T0 + i * MIN)).collect();
        let now = T0 + 2000 * MIN;
        let limits = SimLimits {
            max_work: 100_000,
            ..SimLimits::DEFAULT
        };
        let t = simulate_capped(&p, INO, &history, now, DAY, limits);
        assert!(t.truncated);
        // One pass may overshoot the cap by its own size, never more.
        assert!(t.work >= 100_000 && t.work < 100_000 + 2100, "{}", t.work);
        assert!(t.created < 60, "{}", t.created);
        assert!(t.counts.last().unwrap().at_unix_ms < now + DAY);

        let limits = SimLimits {
            max_live: 2010,
            ..SimLimits::DEFAULT
        };
        let t = simulate_capped(&p, INO, &history, now, DAY, limits);
        assert!(t.truncated);
        assert_eq!(t.created, 10);
        assert_eq!(t.final_count, 2010);
        // The default work cap does not fire on a plain simulation.
        let t = simulate(&p, INO, &history, now, HOUR);
        assert!(!t.truncated);
        assert!(t.work < MAX_WORK);
    }

    /// The serialized shape is part of the control protocol's surface,
    /// so the field names are pinned here.
    #[test]
    fn timeline_serializes_with_stable_field_names() {
        let p = policy("1h:2h; tz=Europe/Budapest");
        let t = simulate(&p, INO, &[held(T0, Some("csi:x"))], T0, 2 * HOUR);
        let json = serde_json::to_value(&t).unwrap();
        let obj = json.as_object().unwrap();
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "cadence",
                "counts",
                "created",
                "expired",
                "final_count",
                "horizon_unix_ms",
                "now_unix_ms",
                "paused",
                "policy",
                "policy_ino",
                "snapshots",
                "steady_state_bound",
                "truncated",
            ]
        );
        assert_eq!(obj["policy"], "1h:2h; tz=Europe/Budapest");
        assert_eq!(obj["cadence"], "1h");
        assert_eq!(obj["now_unix_ms"], T0);
        assert_eq!(obj["horizon_unix_ms"], T0 + 2 * HOUR);

        let snap = &json["snapshots"][0];
        let mut keys: Vec<&str> = snap
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "candidate",
                "created_unix_ms",
                "expired_unix_ms",
                "expires_unix_ms",
                "held",
                "held_by",
                "id",
                "keep",
                "reasons",
                "synthetic",
            ]
        );
        assert_eq!(snap["held_by"], "csi:x");
        assert_eq!(snap["reasons"][0]["kind"], "held");
        assert_eq!(snap["reasons"][0]["held_by"], "csi:x");

        let point = &json["counts"][0];
        let mut keys: Vec<&str> = point
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, ["at_unix_ms", "candidates", "total"]);

        // The reason variants, in full.
        let tier = serde_json::to_value(Reason::Tier(Interval::Minutes(5))).unwrap();
        assert_eq!(tier["kind"], "tier");
        assert_eq!(tier["every"], "5m");
        assert!(tier["last"].is_null() && tier["held_by"].is_null());
        let last = serde_json::to_value(Reason::Last(3)).unwrap();
        assert_eq!(
            (last["kind"].as_str(), last["last"].as_u64()),
            (Some("last"), Some(3))
        );
        assert_eq!(
            serde_json::to_value(Reason::Grace).unwrap()["kind"],
            "grace"
        );
        assert_eq!(
            serde_json::to_value(Reason::NotCandidate).unwrap()["kind"],
            "not_candidate"
        );
        // A `Verdict` on its own uses the protocol's time field name.
        let v = serde_json::to_value(evaluate(&p, INO, &[auto(T0)]).remove(0)).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["expires_unix_ms", "keep", "reasons"]);
    }

    #[test]
    fn origin_round_trips_the_rows_byte() {
        for b in [0u8, 1, 2, 255] {
            assert_eq!(Origin::from_u8(b).to_u8(), b);
        }
        assert!(Origin::from_u8(1).is_auto());
        assert!(!Origin::from_u8(2).is_auto());
        // A row from a future step is "not ours", so it is never
        // expired.
        let row = SnapshotRow {
            origin: 9,
            policy_ino: INO,
            ..SnapshotRow::new("id", "/p", "n", "h", T0)
        };
        let facts = SnapFacts::from_row(&row);
        assert_eq!(facts.origin, Origin::Unknown(9));
        assert!(!facts.is_candidate(INO));
        let row = SnapshotRow {
            origin: 1,
            policy_ino: INO,
            held: true,
            held_by: Some("csi:x".into()),
            ..SnapshotRow::new("id", "/p", "n", "h", T0)
        };
        let facts = SnapFacts::from_row(&row);
        assert!(!facts.is_candidate(INO));
        assert_eq!(facts.held_by.as_deref(), Some("csi:x"));
    }

    // --- the naive implementation, for the differential test ---------
    //
    // Deliberately independent of `super::calendar`: it materializes
    // every bucket boundary of every tier by walking the calendar
    // forward with `jiff` and then marks representatives by brute
    // force, bucket by bucket. Slow, and obviously correct.
    mod naive {
        use jiff::civil::{DateTime, Weekday};
        use jiff::tz::{AmbiguousOffset, TimeZone, TimeZoneDatabase};
        use jiff::{Span, Timestamp};

        use crate::snapsched::policy::{Interval, Keep, SnapPolicy, WeekStart};
        use crate::snapsched::retention::SnapFacts;

        fn zone(name: &str) -> TimeZone {
            if name == "UTC" {
                return TimeZone::UTC;
            }
            TimeZoneDatabase::bundled().get(name).unwrap()
        }

        fn local(tz: &TimeZone, ms: i64) -> DateTime {
            let ts = Timestamp::from_millisecond(ms).unwrap();
            tz.to_offset(ts).to_datetime(ts)
        }

        /// Every instant the local label `dt` denotes: two in a fold
        /// hour (hour buckets only — a calendar date happens once), the
        /// resumption instant for a gap.
        fn instants(tz: &TimeZone, dt: DateTime, split_folds: bool) -> Vec<i64> {
            let ms = |o: jiff::tz::Offset| o.to_timestamp(dt).unwrap().as_millisecond();
            match tz.to_ambiguous_timestamp(dt).offset() {
                AmbiguousOffset::Unambiguous { offset } => vec![ms(offset)],
                AmbiguousOffset::Gap { before, .. } => vec![ms(before)],
                AmbiguousOffset::Fold { before, after } => {
                    if split_folds {
                        vec![ms(before), ms(after)]
                    } else {
                        vec![ms(before)]
                    }
                }
            }
        }

        /// All bucket boundaries of `interval` in `[from, to]`, plus one
        /// either side, found by walking the calendar one bucket at a
        /// time from well before `from`.
        pub fn boundaries(policy: &SnapPolicy, interval: Interval, from: i64, to: i64) -> Vec<i64> {
            let tz = zone(&policy.tz);
            let (dsh, dsm) = (policy.day_start.0 as i8, policy.day_start.1 as i8);
            let width = match interval {
                Interval::Seconds(n) => Some(i64::from(n) * 1_000),
                Interval::Minutes(n) => Some(i64::from(n) * 60_000),
                _ => None,
            };
            if let Some(w) = width {
                let mut t = from - from.rem_euclid(w) - w;
                let mut out = Vec::new();
                while t <= to + w {
                    out.push(t);
                    t += w;
                }
                return out;
            }
            // Start from a label comfortably before `from` and step.
            let start_local = local(&tz, from);
            let mut label = match interval {
                Interval::Hours(n) => {
                    let n = n as i8;
                    let d = start_local.date().yesterday().unwrap();
                    let h = (start_local.hour() / n) * n;
                    d.at(h, 0, 0, 0)
                }
                Interval::Day => start_local
                    .date()
                    .checked_sub(Span::new().days(2))
                    .unwrap()
                    .at(dsh, dsm, 0, 0),
                Interval::Week => {
                    let mut d = start_local.date().checked_sub(Span::new().days(9)).unwrap();
                    let want = match policy.week_start {
                        WeekStart::Mon => Weekday::Monday,
                        WeekStart::Sun => Weekday::Sunday,
                    };
                    while d.weekday() != want {
                        d = d.yesterday().unwrap();
                    }
                    d.at(dsh, dsm, 0, 0)
                }
                Interval::Month => {
                    let d = start_local.date().first_of_month();
                    let d = d.checked_sub(Span::new().months(1)).unwrap();
                    d.at(dsh, dsm, 0, 0)
                }
                Interval::Year => {
                    let d = start_local.date().first_of_year();
                    let d = d.checked_sub(Span::new().years(1)).unwrap();
                    d.at(dsh, dsm, 0, 0)
                }
                Interval::Seconds(_) | Interval::Minutes(_) => unreachable!(),
            };
            let step = match interval {
                Interval::Hours(n) => Span::new().hours(i64::from(n)),
                Interval::Day => Span::new().days(1),
                Interval::Week => Span::new().days(7),
                Interval::Month => Span::new().months(1),
                Interval::Year => Span::new().years(1),
                Interval::Seconds(_) | Interval::Minutes(_) => unreachable!(),
            };
            let split = matches!(interval, Interval::Hours(_));
            let mut out: Vec<i64> = Vec::new();
            loop {
                let found = instants(&tz, label, split);
                let past = found.iter().all(|ms| *ms > to);
                out.extend(found);
                if past {
                    break;
                }
                label = label.checked_add(step).unwrap();
            }
            out.sort_unstable();
            out.dedup();
            out
        }

        fn minus_keep(policy: &SnapPolicy, t: i64, keep: Keep, every: Interval) -> Option<i64> {
            // Daily-or-coarser tiers count days on the calendar; jiff's
            // own `Zoned` arithmetic does exactly that for a `days` span.
            let daily = !matches!(
                every,
                Interval::Seconds(_) | Interval::Minutes(_) | Interval::Hours(_)
            );
            let span = match keep {
                Keep::Forever => return None,
                Keep::Minutes(n) => return Some(t - i64::from(n) * 60_000),
                Keep::Hours(n) => return Some(t - i64::from(n) * 3_600_000),
                Keep::Days(n) if !daily => return Some(t - i64::from(n) * 86_400_000),
                Keep::Weeks(n) if !daily => return Some(t - i64::from(n) * 7 * 86_400_000),
                Keep::Days(n) => Span::new().days(i64::from(n)),
                Keep::Weeks(n) => Span::new().weeks(i64::from(n)),
                Keep::Months(n) => Span::new().months(i64::from(n)),
                Keep::Years(n) => Span::new().years(i64::from(n)),
            };
            let tz = zone(&policy.tz);
            let zoned = Timestamp::from_millisecond(t).unwrap().to_zoned(tz);
            Some(
                zoned
                    .checked_sub(span)
                    .unwrap()
                    .timestamp()
                    .as_millisecond(),
            )
        }

        /// `(keep, the tiers that keep it)` for every snapshot, in input
        /// order.
        pub fn evaluate(
            policy: &SnapPolicy,
            policy_ino: u64,
            snaps: &[SnapFacts],
        ) -> Vec<(bool, Vec<Interval>)> {
            let mut out: Vec<(bool, Vec<Interval>)> =
                snaps.iter().map(|_| (false, Vec::new())).collect();
            let mut cands: Vec<(usize, &SnapFacts)> = snaps
                .iter()
                .enumerate()
                .filter(|(_, s)| s.origin.is_auto() && s.policy_ino == policy_ino && !s.held)
                .collect();
            for (i, s) in snaps.iter().enumerate() {
                if !(s.origin.is_auto() && s.policy_ino == policy_ino && !s.held) {
                    out[i].0 = true;
                }
            }
            if cands.is_empty() {
                return out;
            }
            cands.sort_by(|a, b| {
                a.1.created_unix_ms
                    .cmp(&b.1.created_unix_ms)
                    .then_with(|| a.1.id.cmp(&b.1.id))
                    .then_with(|| a.0.cmp(&b.0))
            });
            let oldest = cands[0].1.created_unix_ms;
            let anchor = cands[cands.len() - 1].1.created_unix_ms;
            for tier in &policy.tiers {
                let bounds = boundaries(policy, tier.every, oldest, anchor);
                // The anchor's bucket, found by scanning.
                let anchor_bucket = *bounds.iter().filter(|b| **b <= anchor).max().unwrap();
                let edge = minus_keep(policy, anchor_bucket, tier.keep, tier.every);
                // Brute force: walk every materialized bucket in order,
                // taking the first candidate that falls inside it. The
                // candidates are sorted, so the walk is a merge.
                let mut next = 0;
                for w in bounds.windows(2) {
                    let (lo, hi) = (w[0], w[1]);
                    while next < cands.len() && cands[next].1.created_unix_ms < lo {
                        next += 1;
                    }
                    let Some((idx, s)) = cands.get(next) else {
                        break;
                    };
                    if s.created_unix_ms >= hi {
                        continue;
                    }
                    if edge.is_none_or(|e| lo > e) {
                        out[*idx].0 = true;
                        out[*idx].1.push(tier.every);
                    }
                }
            }
            let last = policy.last.max(1) as usize;
            for (idx, _) in cands.iter().rev().take(last) {
                out[*idx].0 = true;
            }
            out
        }
    }

    // --- generators --------------------------------------------------

    /// Tier clauses with pairwise distinct intervals and a keep that
    /// covers one bucket, so any subsequence parses.
    const TIER_CLAUSES: &[&str] = &[
        "10s:10min",
        "30s:5min",
        "1m:1h",
        "5m:1d",
        "15m:6h",
        "30m:12h",
        "1h:7d",
        "6h:3d",
        "12h:2d",
        "1d:30d",
        "1w:12w",
        "1mo:1y",
        "1y:*",
    ];

    /// Zones with something awkward about them: DST both ways, a half
    /// hour offset, a quarter hour offset, a southern-hemisphere
    /// schedule, and a 30-minute DST shift at 02:00.
    const ZONES: &[&str] = &[
        "UTC",
        "Europe/Budapest",
        "Asia/Kolkata",
        "Pacific/Chatham",
        "America/Santiago",
        "Australia/Lord_Howe",
    ];

    /// A random policy in one named timezone. `day-start=12:00` is the
    /// only shifted boundary used, because it is a multiple of every
    /// hourly tier in [`TIER_CLAUSES`] and so always parses; the gap
    /// and fold cases of `day-start=02:00` have hand tests.
    fn arb_policy_in(tz: &'static str) -> impl Strategy<Value = SnapPolicy> {
        (
            prop::sample::subsequence(TIER_CLAUSES.to_vec(), 1..=4),
            prop::sample::select(vec!["", "; day-start=12:00"]),
            prop::sample::select(vec!["", "; week-start=sun"]),
            1u32..=4,
        )
            .prop_map(move |(tiers, day_start, week_start, last)| {
                let expr = format!(
                    "{}; tz={tz}{day_start}{week_start}; last={last}",
                    tiers.join(" ")
                );
                SnapPolicy::parse(&expr).unwrap_or_else(|e| panic!("{expr}: {e}"))
            })
    }

    fn arb_policy() -> impl Strategy<Value = SnapPolicy> {
        prop::sample::select(ZONES).prop_flat_map(arb_policy_in)
    }

    /// Gaps that make histories interesting: several snapshots inside
    /// one bucket, and jumps over whole tiers' worth of them.
    const GAPS: &[i64] = &[
        0,
        1,
        1_000,
        11_000,
        61_000,
        5 * MIN,
        17 * MIN,
        HOUR,
        7 * HOUR,
        DAY,
        3 * DAY,
        9 * DAY,
        40 * DAY,
        400 * DAY,
    ];

    /// A history: creation times built from random gaps, with a few
    /// held / manual / foreign-root snapshots mixed in.
    fn arb_history(max: usize) -> impl Strategy<Value = Vec<SnapFacts>> {
        (
            prop::collection::vec(
                (
                    prop::sample::select(GAPS),
                    0u8..16, // the kind
                ),
                1..=max,
            ),
            // A base instant inside a DST transition week, or not.
            prop::sample::select(BASES.to_vec()),
        )
            .prop_map(|(steps, base)| {
                let mut at = base;
                let mut out = Vec::with_capacity(steps.len());
                for (i, (gap, kind)) in steps.into_iter().enumerate() {
                    at = at.saturating_add(gap);
                    out.push(fact(format!("s{i:04}"), at, kind));
                }
                out
            })
    }

    /// Where a history starts, **paired with the zone whose calendar it
    /// is awkward for**: two hours before each of the 2026 DST
    /// transitions of the zones in [`ZONES`], plus an ordinary Monday,
    /// a month start and a leap day. A cadence-relative history
    /// starting two hours before a transition puts snapshots on both
    /// sides of the fold or gap, which is where bucket alignment is
    /// hard — pairing the two is what makes that likely rather than a
    /// one-in-sixty coincidence.
    const ZONE_BASES: &[(&str, i64)] = &[
        ("UTC", T0),
        ("UTC", 1_709_164_800_000),                        // 2024-02-29
        ("UTC", 1_740_787_200_000),                        // 2025-03-01
        ("Europe/Budapest", 1_774_746_000_000 - 2 * HOUR), // spring forward
        ("Europe/Budapest", 1_792_890_000_000 - 2 * HOUR), // fall back
        ("Europe/Budapest", T0),
        ("Asia/Kolkata", T0),
        ("Asia/Kolkata", 1_740_787_200_000),
        ("Pacific/Chatham", 1_775_311_200_000 - 2 * HOUR), // fall back
        ("Pacific/Chatham", 1_790_431_200_000 - 2 * HOUR), // spring forward
        ("America/Santiago", 1_775_358_000_000 - 2 * HOUR), // fall back
        ("America/Santiago", 1_788_667_200_000 - 2 * HOUR), // spring forward
        ("Australia/Lord_Howe", 1_775_314_800_000 - 2 * HOUR), // fall back
        ("Australia/Lord_Howe", 1_791_041_400_000 - 2 * HOUR), // spring forward
    ];

    /// The base instants [`arb_history`] starts from, zone-agnostic.
    const BASES: &[i64] = &[
        T0,
        1_740_787_200_000,
        1_709_164_800_000,
        1_774_746_000_000 - 2 * HOUR,
        1_792_890_000_000 - 2 * HOUR,
    ];

    /// A policy together with a history whose gaps are **multiples of
    /// its own cadence**, so the snapshots land on, just before and
    /// just after bucket boundaries instead of at arbitrary points.
    ///
    /// This is the generator the differential test uses: it probes the
    /// boundaries that matter, and it bounds the work the naive
    /// implementation does (it materializes every bucket in the span,
    /// so an unbounded span would be an unbounded test).
    fn arb_policy_and_history(max: usize) -> impl Strategy<Value = (SnapPolicy, Vec<SnapFacts>)> {
        prop::sample::select(ZONE_BASES.to_vec())
            .prop_flat_map(|(tz, base)| (arb_policy_in(tz), Just(base)))
            .prop_flat_map(move |(p, base)| {
                // One gap set per tier, so a history probes the boundaries
                // of the coarse tiers as well as the fine ones.
                let widths: Vec<i64> = p
                    .tiers
                    .iter()
                    .map(|t| t.every.nominal_duration() as i64 * 1_000)
                    .collect();
                let mut gaps: Vec<i64> = vec![0, 1, 1_000];
                for w in &widths {
                    gaps.extend([w / 3 + 1, *w, w + 1, 2 * w, 7 * w, 61 * w]);
                }
                // The naive implementation materializes every bucket of
                // every tier across the whole span, so the span has to be
                // bounded in units of the *finest* tier or the test is
                // unbounded. 20 000 buckets of it, and never more than two
                // centuries.
                let span_cap = widths[0].saturating_mul(20_000).min(200 * 365 * DAY);
                (
                    Just(p),
                    Just(base),
                    prop::collection::vec((prop::sample::select(gaps), 0u8..16), 1..=max),
                )
                    .prop_map(move |(p, base, steps)| {
                        let ceiling = base.saturating_add(span_cap);
                        let mut at = base;
                        let mut out = Vec::with_capacity(steps.len());
                        for (i, (gap, kind)) in steps.into_iter().enumerate() {
                            at = at.saturating_add(gap).min(ceiling);
                            out.push(fact(format!("s{i:04}"), at, kind));
                        }
                        (p, out)
                    })
            })
    }

    /// One snapshot, of a kind selected by `kind`: mostly plain
    /// candidates, with a manual, a held and another root's mixed in.
    fn fact(id: String, at: i64, kind: u8) -> SnapFacts {
        match kind {
            13 => SnapFacts {
                id,
                created_unix_ms: at,
                origin: Origin::Manual,
                policy_ino: 0,
                held: false,
                held_by: None,
            },
            14 => SnapFacts {
                id,
                created_unix_ms: at,
                origin: Origin::Auto,
                policy_ino: INO,
                held: true,
                held_by: Some("csi:x".into()),
            },
            15 => SnapFacts {
                id,
                created_unix_ms: at,
                origin: Origin::Auto,
                policy_ino: 999,
                held: false,
                held_by: None,
            },
            _ => SnapFacts {
                id,
                created_unix_ms: at,
                origin: Origin::Auto,
                policy_ino: INO,
                held: false,
                held_by: None,
            },
        }
    }

    fn survivors(snaps: &[SnapFacts], verdicts: &[Verdict]) -> Vec<SnapFacts> {
        snaps
            .iter()
            .zip(verdicts)
            .filter(|(_, v)| v.keep)
            .map(|(s, _)| s.clone())
            .collect()
    }

    // --- the four properties -----------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig { cases: 192, max_shrink_iters: 2_000, ..ProptestConfig::default() })]

        /// Idempotent: evaluating over the survivors expires nothing.
        #[test]
        fn prop_idempotent(policy in arb_policy(), snaps in arb_history(60)) {
            let first = evaluate(&policy, INO, &snaps);
            let kept = survivors(&snaps, &first);
            let second = evaluate(&policy, INO, &kept);
            prop_assert!(second.iter().all(|v| v.keep), "second pass expired something");
            // And the reasons are unchanged, not merely the flags.
            let first_reasons: Vec<_> = snaps
                .iter()
                .zip(&first)
                .filter(|(_, v)| v.keep)
                .map(|(_, v)| v.reasons.clone())
                .collect();
            let second_reasons: Vec<_> = second.iter().map(|v| v.reasons.clone()).collect();
            prop_assert_eq!(first_reasons, second_reasons);
        }

        /// Monotone, part one: adding newer snapshots never revives an
        /// expired one.
        #[test]
        fn prop_adding_newer_never_revives(
            policy in arb_policy(),
            snaps in arb_history(40),
            extra in prop::collection::vec(prop::sample::select(GAPS), 1..6),
        ) {
            let before = evaluate(&policy, INO, &snaps);
            let mut grown = snaps.clone();
            let mut at = snaps.iter().map(|s| s.created_unix_ms).max().unwrap();
            for (i, gap) in extra.iter().enumerate() {
                at = at.saturating_add(*gap);
                grown.push(SnapFacts { id: format!("n{i}"), ..auto(at) });
            }
            let after = evaluate(&policy, INO, &grown);
            for (i, v) in before.iter().enumerate() {
                if !v.keep {
                    prop_assert!(!after[i].keep, "snapshot {i} came back to life");
                }
            }
        }

        /// Monotone, part two: deleting a candidate that is no tier's
        /// representative and is outside the `last` floor changes
        /// nothing at all for the others.
        #[test]
        fn prop_deleting_a_non_representative_changes_nothing(
            policy in arb_policy(),
            snaps in arb_history(50),
        ) {
            let before = evaluate(&policy, INO, &snaps);
            let n_cands = snaps.iter().filter(|s| s.is_candidate(INO)).count();
            // Representative-ness is visible in the verdicts of a
            // policy whose every tier keeps forever, which is the same
            // bucket structure with no window.
            let mut all_forever = policy.clone();
            for tier in &mut all_forever.tiers {
                tier.keep = Keep::Forever;
            }
            all_forever.last = 1;
            let reps = evaluate(&all_forever, INO, &snaps);
            let mut sorted: Vec<(i64, &str, usize)> = snaps
                .iter()
                .enumerate()
                .filter(|(_, s)| s.is_candidate(INO))
                .map(|(i, s)| (s.created_unix_ms, s.id.as_str(), i))
                .collect();
            sorted.sort();
            let last = policy.last.max(1) as usize;
            let floor: Vec<usize> = sorted
                .iter()
                .rev()
                .take(last)
                .map(|(_, _, i)| *i)
                .collect();
            for victim in 0..snaps.len() {
                if !snaps[victim].is_candidate(INO)
                    || !reps[victim].reasons.is_empty()
                    || floor.contains(&victim)
                {
                    continue;
                }
                let kept: Vec<SnapFacts> = snaps
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != victim)
                    .map(|(_, s)| s.clone())
                    .collect();
                let after = evaluate(&policy, INO, &kept);
                let survivors = before
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != victim);
                for ((i, was), now) in survivors.zip(&after) {
                    prop_assert_eq!(
                        (was.keep, &was.reasons),
                        (now.keep, &now.reasons),
                        "deleting non-representative {} changed {} (of {} candidates)",
                        victim, i, n_cands
                    );
                }
            }
        }

        /// Outage-safe: truncating the history at T (the scheduler was
        /// down after T) never expires anything older than T that the
        /// full history keeps.
        #[test]
        fn prop_outage_safe(
            policy in arb_policy(),
            snaps in arb_history(50),
            cut in 0usize..50,
        ) {
            let full = evaluate(&policy, INO, &snaps);
            let mut times: Vec<i64> = snaps.iter().map(|s| s.created_unix_ms).collect();
            times.sort_unstable();
            times.dedup();
            let t = times[cut % times.len()];
            let truncated: Vec<SnapFacts> = snaps
                .iter()
                .filter(|s| s.created_unix_ms <= t)
                .cloned()
                .collect();
            let short = evaluate(&policy, INO, &truncated);
            let mut j = 0;
            for (i, s) in snaps.iter().enumerate() {
                if s.created_unix_ms > t {
                    continue;
                }
                if full[i].keep {
                    prop_assert!(
                        short[j].keep,
                        "the outage history expired snapshot {i}, which the full one keeps"
                    );
                }
                j += 1;
            }
        }

        /// Nesting: with `day-start=00:00`, the daily representative of
        /// a day is also that day's first hourly and first 5-minute
        /// representative.
        #[test]
        fn prop_daily_representative_is_the_days_first(
            tz in prop::sample::select(ZONES),
            snaps in arb_history(60),
        ) {
            // Every tier keeps forever, so "kept by tier I" is exactly
            // "is a representative of tier I".
            let policy = SnapPolicy::parse(&format!("5m:* 1h:* 1d:*; tz={tz}")).unwrap();
            let v = evaluate(&policy, INO, &snaps);
            for (i, verdict) in v.iter().enumerate() {
                if !snaps[i].is_candidate(INO) {
                    continue;
                }
                if verdict.tiers().any(|t| t == Interval::Day) {
                    prop_assert!(
                        verdict.tiers().any(|t| t == Interval::Hours(1)),
                        "snapshot {i} is a daily but not an hourly representative"
                    );
                    prop_assert!(
                        verdict.tiers().any(|t| t == Interval::Minutes(5)),
                        "snapshot {i} is a daily but not a 5m representative"
                    );
                }
            }
        }

        /// The differential test: the same verdicts as a naive
        /// implementation that materializes every bucket.
        #[test]
        fn prop_matches_the_naive_implementation(
            (policy, snaps) in arb_policy_and_history(40),
        ) {
            let ours = evaluate(&policy, INO, &snaps);
            let theirs = naive::evaluate(&policy, INO, &snaps);
            for (i, (mine, (keep, tiers))) in ours.iter().zip(&theirs).enumerate() {
                prop_assert_eq!(
                    mine.keep, *keep,
                    "snapshot {} ({}): {:?} vs naive keep={} tiers={:?} [{}]",
                    i, snaps[i].created_unix_ms, mine, keep, tiers, policy
                );
                let my_tiers: Vec<Interval> = mine.tiers().collect();
                if snaps[i].is_candidate(INO) {
                    prop_assert_eq!(
                        &my_tiers, tiers,
                        "snapshot {} ({}) kept by different tiers [{}]",
                        i, snaps[i].created_unix_ms, policy
                    );
                }
            }
        }
    }

    /// The heavier sweep of the same properties. Deliberately
    /// `#[ignore]`d: it runs for tens of seconds in a debug build.
    #[test]
    #[ignore = "slow: the full differential sweep, run it explicitly"]
    fn differential_sweep_many_cases() {
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 20_000,
            ..ProptestConfig::default()
        });
        runner
            .run(&arb_policy_and_history(150), |(policy, snaps)| {
                let ours = evaluate(&policy, INO, &snaps);
                let theirs = naive::evaluate(&policy, INO, &snaps);
                for (i, (mine, (keep, tiers))) in ours.iter().zip(&theirs).enumerate() {
                    prop_assert_eq!(mine.keep, *keep, "snapshot {} [{}]", i, policy);
                    if snaps[i].is_candidate(INO) {
                        let my_tiers: Vec<Interval> = mine.tiers().collect();
                        prop_assert_eq!(&my_tiers, tiers, "snapshot {} [{}]", i, policy);
                    }
                }
                // Idempotence, over the same histories.
                let kept = survivors(&snaps, &ours);
                prop_assert!(evaluate(&policy, INO, &kept).iter().all(|v| v.keep));
                Ok(())
            })
            .unwrap();
    }

    // --- Step 8: the budget's victim order -----------------------------

    /// Hourly for 30 hours under `1h:1d 1d:7d; last=2`: the tier rule
    /// expires hours 1–5 (outside the hourly window, not a daily). The
    /// budget order is the hourlies the daily tier does not keep, oldest
    /// first, without the `last` floor (hours 28, 29), then the dailies
    /// (hours 0 and 24); a held, a manual and another root's snapshot in
    /// the middle are never in it.
    #[test]
    fn budget_order_sheds_the_fine_tiers_first_then_the_coarsest() {
        let p = policy("1h:1d 1d:7d; last=2");
        let mut snaps: Vec<SnapFacts> = (0..30).map(|h| auto(T0 + h * HOUR)).collect();
        snaps.push(held(T0 + 10 * HOUR + MIN, Some("csi:x")));
        snaps.push(manual(T0 + 11 * HOUR + MIN));
        snaps.push(SnapFacts {
            id: "foreign".into(),
            policy_ino: 7,
            ..auto(T0 + 12 * HOUR + MIN)
        });
        let verdicts = evaluate(&p, INO, &snaps);
        let order = budget_order(&p, INO, &snaps, &verdicts);
        let hours = |hs: &[i64]| -> Vec<String> {
            hs.iter().map(|h| format!("s{}", T0 + h * HOUR)).collect()
        };
        let mut want = hours(&(6..24).chain(25..28).collect::<Vec<_>>());
        want.extend(hours(&[0, 24]));
        assert_eq!(order, want);
        // Input order does not matter.
        let mut rev = snaps.clone();
        rev.reverse();
        let rev_verdicts = evaluate(&p, INO, &rev);
        assert_eq!(budget_order(&p, INO, &rev, &rev_verdicts), want);
    }

    /// The floor holds however small the history: one candidate, or only
    /// `last` of them, gives an empty order; graced verdicts give nothing
    /// they keep for grace; an uncovered verdict index is never a victim.
    #[test]
    fn budget_order_never_goes_below_last_nor_past_grace() {
        let p = policy("1h:1d; last=3");
        let snaps: Vec<SnapFacts> = (0..3).map(|h| auto(T0 + h * HOUR)).collect();
        assert!(budget_order(&p, INO, &snaps, &evaluate(&p, INO, &snaps)).is_empty());
        let one = vec![auto(T0)];
        assert!(budget_order(&p, INO, &one, &evaluate(&p, INO, &one)).is_empty());
        let snaps: Vec<SnapFacts> = (0..6).map(|h| auto(T0 + h * HOUR)).collect();
        let plain = evaluate(&p, INO, &snaps);
        assert_eq!(
            budget_order(&p, INO, &snaps, &plain),
            vec![
                format!("s{T0}"),
                format!("s{}", T0 + HOUR),
                format!("s{}", T0 + 2 * HOUR)
            ]
        );
        // Every verdict graced: nothing.
        let graced: Vec<Verdict> = plain
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.reasons.insert(0, Reason::Grace);
                v
            })
            .collect();
        assert!(budget_order(&p, INO, &snaps, &graced).is_empty());
        // Verdicts that do not cover the slice: the uncovered are kept.
        assert_eq!(
            budget_order(&p, INO, &snaps, &plain[..1]),
            vec![format!("s{T0}")]
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 192, max_shrink_iters: 2_000, ..ProptestConfig::default() })]

        /// Step 8's guarantees, over random policies and histories: the
        /// order holds only candidates the verdicts keep for a tier, never
        /// a held, manual or other root's snapshot, never one of the
        /// newest `last`, each at most once; the non-coarsest come before
        /// the coarsest, each run oldest first; and it is a function of
        /// the set, not of the slice's order.
        #[test]
        fn prop_budget_order_is_safe_and_deterministic(
            (policy, snaps) in arb_policy_and_history(60),
            seed in any::<u64>(),
        ) {
            let verdicts = evaluate(&policy, INO, &snaps);
            let order = budget_order(&policy, INO, &snaps, &verdicts);
            let by_id: std::collections::HashMap<&str, usize> =
                snaps.iter().enumerate().map(|(i, s)| (s.id.as_str(), i)).collect();
            let mut cands: Vec<&SnapFacts> = snaps.iter().filter(|s| s.is_candidate(INO)).collect();
            cands.sort_by(|a, b| (a.created_unix_ms, &a.id).cmp(&(b.created_unix_ms, &b.id)));
            let last = policy.last.max(1) as usize;
            let floor: std::collections::HashSet<&str> =
                cands.iter().rev().take(last).map(|s| s.id.as_str()).collect();
            let coarsest = policy.coarsest().unwrap();
            let mut seen = std::collections::HashSet::new();
            let mut in_coarsest = false;
            let mut prev: Option<(i64, &str)> = None;
            for id in &order {
                prop_assert!(seen.insert(id.clone()), "{} twice", id);
                let i = by_id[id.as_str()];
                let s = &snaps[i];
                prop_assert!(s.is_candidate(INO), "{} is not a candidate", id);
                prop_assert!(!s.held && s.origin.is_auto() && s.policy_ino == INO);
                prop_assert!(!floor.contains(id.as_str()), "{} is in the last floor", id);
                prop_assert!(verdicts[i].keep, "{} is expired by the tiers already", id);
                let coarse = verdicts[i].tiers().any(|t| t == coarsest);
                if coarse && !in_coarsest {
                    in_coarsest = true;
                    prev = None;
                }
                prop_assert_eq!(coarse, in_coarsest, "{} out of its run", id);
                let key = (s.created_unix_ms, s.id.as_str());
                if let Some(p) = prev {
                    prop_assert!(p < key, "{} not oldest first", id);
                }
                prev = Some(key);
            }
            // Every kept, tier-only, non-floor candidate is in it.
            let expected = cands
                .iter()
                .filter(|s| !floor.contains(s.id.as_str()))
                .filter(|s| {
                    let v = &verdicts[by_id[s.id.as_str()]];
                    v.keep && v.reasons.iter().all(|r| matches!(r, Reason::Tier(_)))
                })
                .count();
            prop_assert_eq!(order.len(), expected);
            // Shuffled input, same order.
            let mut shuffled = snaps.clone();
            let mut x = seed | 1;
            for i in (1..shuffled.len()).rev() {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                shuffled.swap(i, (x % (i as u64 + 1)) as usize);
            }
            let again = budget_order(&policy, INO, &shuffled, &evaluate(&policy, INO, &shuffled));
            prop_assert_eq!(again, order);
        }
    }
}
