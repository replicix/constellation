//! The `snapshot.policy.*` methods (plan 32): automatic snapshot
//! schedules, as the control protocol exposes them.
//!
//! - `check` (Step 5's `policy check`) and `simulate` (Step 2's
//!   simulation, the web UI's retention timeline) read only (M1).
//! - `list` and `show` read the binding (M2): the policy roots are the
//!   directories carrying [`SNAPSHOT_POLICY_XATTR`], found through the
//!   replica's `xattr_by_name` index (`Meta::snapshot_policy_roots`) —
//!   there is no registry. `list` adds the *orphaned* streams: auto
//!   snapshots whose `policy_ino` carries no parseable policy, which are
//!   never expired automatically (Step 4.2).
//! - `set`, `remove` and `pause` write the binding (M2). Each writes the
//!   xattr exactly as a `setxattr` would: through the service's own
//!   whole-filesystem View ([`ControlVfs`]), so the View's setxattr gate
//!   validates it and the mutation is forwarded to the holder like any
//!   other. Never a direct `Meta` write: on a non-holder that would fork
//!   the replica.
//!
//! - `sched.status` and `sched.run` expose the scheduler (M3,
//!   `crate::snapsched`): its counters and per-root state, and one tick on
//!   demand.
//!
//! Nothing here creates a snapshot, and only `remove {expire}` deletes
//! any — through the scheduler's own re-read and holder-side batch
//! (`Scheduler::expire_removed`); the scheduler (through `sched.run` or
//! its ticker) creates and expires. `set` therefore only *previews* what a
//! policy would expire (and, in `grace_note`, when: Step 4.3's window),
//! and guards it server-side: an expiring change is written only when the
//! caller confirms the exact count it was shown (`confirm_expiring`, Step
//! 7.3), so a stale client cannot confirm a different delta. `remove
//! {expire}` has the same guard over the count of the root's unheld auto
//! snapshots.
//!
//! Every number and verdict comes from `constellation_meta::snapsched`:
//! the parse and the bound from [`SnapPolicy`], the verdicts from
//! [`retention::evaluate`], the timeline from [`retention::simulate`],
//! the warnings and the settled count from [`check`]. This module only
//! finds the rows a directory's policy would govern and changes types.
//!
//! ## Which rows are a directory's
//!
//! A policy root's snapshots are the rows recorded at its path **and**
//! the rows whose `policy_ino` is its inode: after a rename the auto
//! snapshots keep their creation-time path but still belong to the
//! root (plan 32 §0.5), and they are exactly the ones a policy could
//! expire. Everything else at the path — manual snapshots, held ones of
//! any owner (`csi:…` included), another root's auto snapshots — is
//! listed with its verdict too, and retention keeps all of them.
//!
//! The daemon's clock enters only as `now`: the instant the simulation
//! starts and its synthetic ticks are placed from. Verdicts over real
//! rows do not depend on it (retention anchors on the newest candidate,
//! never on the wall clock).
//!
//! ## What is stored
//!
//! A plain `setxattr` stores its bytes verbatim once the gate accepts
//! them (plan 22's posture: what you set is what you read back). `set`
//! and `pause` store the *canonical* form, which they compute anyway and
//! which is what every reader displays; a later `pause` therefore also
//! canonicalizes a verbatim expression.

use super::{unary, ControlVfs, EngineControl};
use crate::snapexpire;
use constellation_control::methods::{
    SnapshotPolicyCheck, SnapshotPolicyList, SnapshotPolicyPause, SnapshotPolicyRemove,
    SnapshotPolicySet, SnapshotPolicyShow, SnapshotPolicySimulate, SnapshotSchedRun,
    SnapshotSchedStatus,
};
use constellation_control::proto::types::{
    PolicyErrorInfo, SnapPolicyAgainst, SnapPolicyCheckParams, SnapPolicyCheckResult,
    SnapPolicyDelta, SnapPolicyListing, SnapPolicyPauseParams, SnapPolicyRemoveParams,
    SnapPolicyRemoved, SnapPolicyRoot, SnapPolicySetParams, SnapPolicyShown,
    SnapPolicySimulateParams, SnapPolicySkipped, SnapReason, SnapTimeline, SnapVerdict,
};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_control::Router;
use constellation_fs_core::InodeKind;
use constellation_meta::snapsched::{
    check, retention, SnapFacts, SnapPolicy, SNAPSHOT_POLICY_XATTR,
};
use constellation_meta::{Meta, MetaStore, SnapshotRow};
use constellation_store_s3::snapsched::SnapSchedState;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) fn register(r: &mut Router, svc: &Arc<EngineControl>) {
    unary::<SnapshotPolicyCheck>(r, svc, |s, _, p| {
        policy_check(&s.meta, &p, constellation_store_s3::lease::now_unix_ms())
    });
    unary::<SnapshotPolicySimulate>(r, svc, |s, _, p| {
        policy_simulate(&s.meta, &p, constellation_store_s3::lease::now_unix_ms())
    });
    unary::<SnapshotPolicyList>(r, svc, |s, _, _| policy_list(&s.meta));
    unary::<SnapshotPolicyShow>(r, svc, |s, _, p| {
        policy_show(&s.meta, &p.path, &display_grace(s))
    });
    unary::<SnapshotPolicySet>(r, svc, |s, c, p| {
        let writer = Writer::new(s.browser(&c.principal)?);
        policy_set(&s.meta, &writer, &p, &display_grace(s))
    });
    unary::<SnapshotPolicyRemove>(r, svc, |s, c, p| {
        let writer = Writer::new(s.browser(&c.principal)?);
        policy_remove(&s.meta, &writer, &p, |root, policy, victims| {
            s.rt.block_on(
                s.engine
                    .snapsched()
                    .expire_removed(root.ino, &root.path, policy, victims),
            )
        })
    });
    unary::<SnapshotPolicyPause>(r, svc, |s, c, p| {
        let writer = Writer::new(s.browser(&c.principal)?);
        policy_pause(&s.meta, &writer, &p)
    });
    // The scheduler (M3): its state, and one tick on demand. A tick
    // never fails as a call: refusals and batch errors are in the result.
    unary::<SnapshotSchedStatus>(r, svc, |s, _, _| {
        let mut report = s
            .engine
            .snapsched()
            .report()
            .map_err(|e| ControlError::failed(format!("{e:#}")))?;
        s.fill_roots_used(&mut report.roots)?;
        Ok(report)
    });
    unary::<SnapshotSchedRun>(r, svc, |s, _, p| {
        let run = crate::snapsched::Run::Manual { dry_run: p.dry_run };
        Ok(s.rt.block_on(s.engine.snapsched().tick(run)))
    });
}

/// The scheduler's grace as every display applies it
/// (`Scheduler::display_grace`).
fn display_grace(s: &EngineControl) -> snapexpire::GraceView {
    let sched = s.engine.snapsched().clone();
    tokio::task::block_in_place(|| s.rt.block_on(async move { sched.display_grace().await }))
}

/// `set`'s grace note (plan 32 Step 5): when what the new policy would
/// expire is actually deleted. A change of retention (or a root's first
/// policy) opens Step 4.3's grace window when the scheduler sees it, so
/// only what every policy in an open window expires as well goes before
/// the windows close; an unchanged retention (a resume, a `skip-empty`
/// flip) opens none, but a window already open (a first sighting, an
/// earlier change) still holds. With the grace state known this is the
/// scheduler's own computation: the projected state
/// ([`snapexpire::GraceView`]) with this policy observed now, then
/// [`snapexpire::graced`]; unknown, only the stored policy is compared.
/// Counted over the root's snapshots now.
fn grace_note(
    policy: &SnapPolicy,
    previous: Option<&str>,
    root: &Root,
    new: &[SnapVerdict],
    grace: &snapexpire::GraceView,
) -> String {
    let n = new.iter().filter(|v| !v.keep).count();
    if policy.paused {
        return "paused: nothing expires while it is paused".to_string();
    }
    if n == 0 {
        return "nothing expires now".to_string();
    }
    let old = previous.and_then(|p| SnapPolicy::parse(p).ok());
    let unchanged = old.as_ref().is_some_and(|old| {
        snapexpire::canonical_unpaused(old) == snapexpire::canonical_unpaused(policy)
    });
    let snapshots = |k: usize| {
        if k == 1 {
            "1 snapshot".to_string()
        } else {
            format!("{k} snapshots")
        }
    };
    let facts: Vec<SnapFacts> = root.rows.iter().map(SnapFacts::from_row).collect();
    // How many go at the next run, and when the last open window closes.
    let (now_n, until) = match &grace.state {
        Some(state) => {
            let mut mine = SnapSchedState::default();
            if let Some(entry) = state.roots.get(&root.ino) {
                mine.roots.insert(root.ino, entry.clone());
            }
            let seen = [(root.ino, snapexpire::canonical_unpaused(policy))];
            snapexpire::observe(&mut mine, &seen, grace.now, grace.grace_ms);
            let g = snapexpire::graced(
                policy,
                root.ino,
                &facts,
                mine.roots.get(&root.ino),
                grace.now,
                grace.grace_ms,
            );
            (g.verdicts.iter().filter(|v| !v.keep).count(), g.grace_until)
        }
        None if unchanged || grace.grace_ms == 0 => (n, None),
        None => {
            // What the old policy expires too goes without waiting.
            let both = old.as_ref().map_or(0, |old| {
                retention::evaluate(old, root.ino, &facts)
                    .iter()
                    .zip(new)
                    .filter(|(o, n)| !o.keep && !n.keep)
                    .count()
            });
            (both, Some(grace.now.saturating_add(grace.grace_ms)))
        }
    };
    let lead = if unchanged {
        "the retention is unchanged: "
    } else if grace.grace_ms == 0 {
        "no grace window (CONSTELLATION_SNAPSCHED_GRACE_S=0): "
    } else {
        ""
    };
    let Some(until) = until else {
        return format!(
            "{lead}{} expire at the scheduler's next expiry run",
            snapshots(n)
        );
    };
    let window = human_remaining(until.saturating_sub(grace.now));
    if now_n == 0 {
        format!(
            "{lead}nothing is deleted for {window}; after that {} expire",
            snapshots(n)
        )
    } else {
        format!(
            "{lead}{} expire at the next expiry run (no open grace window keeps them); \
             the other {} not before {window} from now",
            snapshots(now_n),
            n - now_n
        )
    }
}

/// A remaining time in ms for a person, rounded up to the minute past
/// the first one: `24h`, `23h59m`, `5m`, `45s`.
fn human_remaining(ms: i64) -> String {
    let secs = u64::try_from(ms).unwrap_or(0).div_ceil(1000);
    if secs < 60 {
        return format!("{secs}s");
    }
    let mins = secs.div_ceil(60);
    match (mins / 60, mins % 60) {
        (0, m) => format!("{m}m"),
        (h, 0) => format!("{h}h"),
        (h, m) => format!("{h}h{m}m"),
    }
}

/// Writes a directory's policy xattr the way a `setxattr` would: through
/// the View, as the calling principal, addressed to the inode that was
/// evaluated ([`ControlVfs::snapshot_policy`]: a rename in between is a
/// `conflict`). A refusal by the View's gate carries the gate's own
/// reason, returned by the gate itself — never read back from the
/// node-wide `last_parse_error` slot, which any concurrent `setfattr`
/// may overwrite.
struct Writer {
    vfs: ControlVfs,
}

impl Writer {
    fn new(vfs: ControlVfs) -> Writer {
        Writer { vfs }
    }

    fn set(&self, root: &Root, value: &str) -> Result<(), ControlError> {
        self.vfs.snapshot_policy(&root.path, root.ino, Some(value))
    }

    fn remove(&self, root: &Root) -> Result<(), ControlError> {
        self.vfs.snapshot_policy(&root.path, root.ino, None)
    }
}

/// A directory as a policy root: its normalized path, inode and rows,
/// oldest first.
struct Root {
    path: String,
    ino: u64,
    rows: Vec<SnapshotRow>,
}

fn root_of(meta: &Meta, path: &str) -> Result<Root, ControlError> {
    let path = super::normalize_control_path(path);
    let internal = |e: constellation_meta::MetaError| ControlError::failed(format!("{path}: {e}"));
    let ino = meta
        .resolve_path(&path)
        .map_err(internal)?
        .ok_or_else(|| ControlError::not_found(format!("{path}: not found")))?;
    let attr = meta
        .getattr(ino)
        .map_err(internal)?
        .ok_or_else(|| ControlError::not_found(format!("{path}: not found")))?;
    if attr.kind.as_u8() != InodeKind::Dir.as_u8() {
        return Err(ControlError::invalid(format!(
            "{path}: not a directory; a snapshot policy belongs to a directory"
        )));
    }
    let mut rows: Vec<SnapshotRow> = meta
        .snapshots(None)
        .map_err(internal)?
        .into_iter()
        .filter(|row| row.path == path || row.policy_ino == ino)
        .collect();
    rows.sort_by(|a, b| (a.created_unix_ms, &a.id).cmp(&(b.created_unix_ms, &b.id)));
    Ok(Root { path, ino, rows })
}

/// A value serialized by `constellation_meta` read back as its control
/// mirror. The meta types document their serialized names as stable and
/// the control types copy them field for field, so this cannot fail for
/// a well-formed value; the tests prove the round trip.
fn mirror<T: serde::de::DeserializeOwned>(value: &impl serde::Serialize) -> T {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .expect("the control mirror of a snapsched type")
}

fn origin_name(origin: u8) -> String {
    match origin {
        0 => "manual".to_string(),
        1 => "auto".to_string(),
        other => format!("unknown({other})"),
    }
}

/// `policy` evaluated over `root`'s real rows, as if it were the
/// directory's policy: every verdict, oldest first.
fn against_of(policy: &SnapPolicy, root: &Root) -> SnapPolicyAgainst {
    let facts: Vec<SnapFacts> = root.rows.iter().map(SnapFacts::from_row).collect();
    against_from(root, &retention::evaluate(policy, root.ino, &facts))
}

/// [`against_of`] with the scheduler's grace applied
/// ([`snapexpire::GraceView::verdicts`], the same computation as
/// `snapshot.list`'s `KEPT BY`/`EXPIRES`): what the next expiry run
/// deletes is `keep: false`, and a snapshot only a grace window keeps
/// has the reason `grace`.
fn against_graced(
    policy: &SnapPolicy,
    root: &Root,
    grace: &snapexpire::GraceView,
) -> SnapPolicyAgainst {
    let facts: Vec<SnapFacts> = root.rows.iter().map(SnapFacts::from_row).collect();
    against_from(root, &grace.verdicts(policy, root.ino, &facts).verdicts)
}

fn against_from(root: &Root, verdicts: &[retention::Verdict]) -> SnapPolicyAgainst {
    let verdicts: Vec<SnapVerdict> = root
        .rows
        .iter()
        .zip(verdicts)
        .map(|(row, v)| SnapVerdict {
            id: row.id.clone(),
            path: row.path.clone(),
            name: row.name.clone(),
            created_unix_ms: row.created_unix_ms,
            origin: origin_name(row.origin),
            policy_ino: row.policy_ino,
            held: row.held,
            held_by: row.held_by.clone().filter(|by| !by.is_empty()),
            keep: v.keep,
            reasons: v.reasons.iter().map(mirror::<SnapReason>).collect(),
            expires_unix_ms: v.expires_at,
        })
        .collect();
    SnapPolicyAgainst {
        path: root.path.clone(),
        policy_ino: root.ino,
        snapshots: verdicts.len() as u32,
        would_expire: verdicts.iter().filter(|v| !v.keep).count() as u32,
        verdicts,
    }
}

/// `snapshot.policy.check` without `against`, needing no daemon: the
/// CLI's local `policy check` calls this directly, so the daemonless and
/// the daemon answers are the same code.
pub fn check_expression(
    expr: &str,
    simulate_ms: Option<u64>,
    now_ms: i64,
) -> SnapPolicyCheckResult {
    check_over(expr, simulate_ms, now_ms, None)
}

fn check_over(
    expr: &str,
    simulate_ms: Option<u64>,
    now_ms: i64,
    root: Option<&Root>,
) -> SnapPolicyCheckResult {
    let policy = match SnapPolicy::parse(expr) {
        Ok(policy) => policy,
        Err(e) => {
            return SnapPolicyCheckResult {
                ok: false,
                error: Some(PolicyErrorInfo {
                    offset: e.offset as u64,
                    message: e.msg,
                }),
                ..Default::default()
            }
        }
    };
    let (ino, facts): (u64, Vec<SnapFacts>) = match root {
        Some(root) => (
            root.ino,
            root.rows.iter().map(SnapFacts::from_row).collect(),
        ),
        None => (0, Vec::new()),
    };
    let horizon = simulate_ms.map(|ms| i64::try_from(ms).unwrap_or(i64::MAX));
    let report = check::check(&policy, ino, &facts, now_ms, horizon);
    let against = root.map(|root| against_of(&policy, root));
    SnapPolicyCheckResult {
        ok: true,
        canonical: Some(report.canonical),
        error: None,
        warnings: report.warnings,
        steady_state_bound: report.steady_state_bound,
        simulated_count: Some(report.simulated_count),
        simulate_horizon_ms: Some(report.horizon_ms.max(0) as u64),
        simulate_truncated: report.truncated,
        simulate_reached_ms: Some(report.reached_ms.max(0) as u64),
        against,
    }
}

pub(crate) fn policy_check(
    meta: &Meta,
    p: &SnapPolicyCheckParams,
    now_ms: i64,
) -> Result<SnapPolicyCheckResult, ControlError> {
    let root = p
        .against
        .as_deref()
        .map(|path| root_of(meta, path))
        .transpose()?;
    Ok(check_over(&p.expr, p.simulate_ms, now_ms, root.as_ref()))
}

pub(crate) fn policy_simulate(
    meta: &Meta,
    p: &SnapPolicySimulateParams,
    now_ms: i64,
) -> Result<SnapTimeline, ControlError> {
    let policy = SnapPolicy::parse(&p.expr)
        .map_err(|e| ControlError::invalid(format!("invalid policy: {e}")))?;
    let root = p
        .path
        .as_deref()
        .map(|path| root_of(meta, path))
        .transpose()?;
    let (ino, facts): (u64, Vec<SnapFacts>) = match &root {
        Some(root) => (
            root.ino,
            root.rows.iter().map(SnapFacts::from_row).collect(),
        ),
        None => (0, Vec::new()),
    };
    let horizon = i64::try_from(p.horizon_ms).unwrap_or(i64::MAX);
    let timeline = retention::simulate(&policy, ino, &facts, now_ms, horizon);
    Ok(mirror(&timeline))
}

fn internal(e: constellation_meta::MetaError) -> ControlError {
    ControlError::failed(e.to_string())
}

/// The policy xattr on `ino`, lossily as text; `None` when it has none.
fn stored_policy(meta: &Meta, ino: u64) -> Result<Option<String>, ControlError> {
    Ok(meta
        .get_xattr(ino, SNAPSHOT_POLICY_XATTR)
        .map_err(internal)?
        .map(|v| String::from_utf8_lossy(&v).into_owned()))
}

/// Where `ino` is now; `None` once it is no longer linked anywhere.
fn current_path(meta: &Meta, ino: u64) -> Option<String> {
    if ino == constellation_fs_core::types::ROOT_INO {
        return Some("/".into());
    }
    match meta.parents_of(ino) {
        Ok(parents) if !parents.is_empty() => meta.path_of(ino).ok(),
        _ => None,
    }
}

/// The [`SnapPolicyRoot`] of `ino` carrying `expr` (or nothing), given
/// every snapshot row.
fn root_status(meta: &Meta, ino: u64, expr: Option<&str>, rows: &[SnapshotRow]) -> SnapPolicyRoot {
    let auto_snapshots = rows
        .iter()
        .filter(|r| is_auto(r) && r.policy_ino == ino)
        .count() as u32;
    let parsed = expr.map(SnapPolicy::parse);
    let (canonical, paused, error) = match &parsed {
        Some(Ok(policy)) => (Some(policy.to_string()), policy.paused, None),
        Some(Err(e)) => (
            None,
            false,
            Some(PolicyErrorInfo {
                offset: e.offset as u64,
                message: e.msg.clone(),
            }),
        ),
        None => (None, false, None),
    };
    SnapPolicyRoot {
        ino,
        path: current_path(meta, ino),
        expr: expr.unwrap_or_default().to_string(),
        canonical,
        paused,
        error,
        auto_snapshots,
        orphaned: auto_snapshots > 0 && !matches!(parsed, Some(Ok(_))),
    }
}

/// `snapshot.policy.list`: the policy roots, then the orphaned streams
/// (auto snapshots whose `policy_ino` carries no policy at all), each
/// ascending by inode. An unparseable root is listed once, as a root,
/// with `orphaned` set when it has auto snapshots.
pub(crate) fn policy_list(meta: &Meta) -> Result<SnapPolicyListing, ControlError> {
    let marked = meta.snapshot_policy_roots().map_err(internal)?;
    let rows = meta.snapshots(None).map_err(internal)?;
    let mut roots: Vec<SnapPolicyRoot> = marked
        .iter()
        .map(|(ino, expr)| root_status(meta, *ino, Some(expr), &rows))
        .collect();
    let marked: BTreeSet<u64> = marked.iter().map(|(ino, _)| *ino).collect();
    let orphans: BTreeSet<u64> = rows
        .iter()
        .filter(|r| is_auto(r) && r.policy_ino != 0 && !marked.contains(&r.policy_ino))
        .map(|r| r.policy_ino)
        .collect();
    roots.extend(
        orphans
            .into_iter()
            .map(|ino| root_status(meta, ino, None, &rows)),
    );
    Ok(SnapPolicyListing { roots })
}

fn is_auto(row: &SnapshotRow) -> bool {
    retention::Origin::from_u8(row.origin).is_auto()
}

/// `snapshot.policy.show`. The verdicts are grace-aware (`grace`, the
/// scheduler's [`snapexpire::GraceView`]), so `show` and `snapshot ls`'s
/// `KEPT BY`/`EXPIRES` agree on what the next expiry run deletes.
pub(crate) fn policy_show(
    meta: &Meta,
    path: &str,
    grace: &snapexpire::GraceView,
) -> Result<SnapPolicyShown, ControlError> {
    let root = root_of(meta, path)?;
    let expr = stored_policy(meta, root.ino)?;
    let all = meta.snapshots(None).map_err(internal)?;
    let status = root_status(meta, root.ino, expr.as_deref(), &all);
    if expr.is_none() && status.auto_snapshots == 0 {
        return Err(ControlError::not_found(format!(
            "{}: no snapshot policy",
            root.path
        )));
    }
    let verdicts = match expr.as_deref().map(SnapPolicy::parse) {
        Some(Ok(policy)) => Some(against_graced(&policy, &root, grace)),
        _ => None,
    };
    Ok(SnapPolicyShown {
        root: status,
        verdicts,
    })
}

fn invalid_policy(e: constellation_meta::policy_lex::PolicyError) -> ControlError {
    ControlError::invalid(format!("invalid policy {e}")).with_details(serde_json::json!({
        "offset": e.offset,
        "message": e.msg,
    }))
}

/// `snapshot.policy.set`.
///
/// The guard covers a paused policy too: it expires nothing while
/// paused, but resuming it is a `pause` call, which never asks again.
/// `confirm_expiring` matters only when the policy would expire
/// something; confirming a count for a change that expires nothing
/// writes (nothing is at stake).
fn policy_set(
    meta: &Meta,
    writer: &Writer,
    p: &SnapPolicySetParams,
    grace: &snapexpire::GraceView,
) -> Result<SnapPolicyDelta, ControlError> {
    let policy = SnapPolicy::parse(&p.expr).map_err(invalid_policy)?;
    let root = root_of(meta, &p.path)?;
    let previous = stored_policy(meta, root.ino)?;
    let against = against_of(&policy, &root);
    let mut delta = SnapPolicyDelta {
        path: root.path.clone(),
        ino: root.ino,
        canonical: policy.to_string(),
        previous,
        creates_every: if policy.paused {
            None
        } else {
            policy.finest().map(|every| every.to_string())
        },
        would_expire: against.would_expire,
        would_expire_ids: against
            .verdicts
            .iter()
            .filter(|v| !v.keep)
            .map(|v| v.id.clone())
            .collect(),
        grace_note: String::new(),
        warnings: check::warnings(&policy),
        written: false,
    };
    delta.grace_note = grace_note(
        &policy,
        delta.previous.as_deref(),
        &root,
        &against.verdicts,
        grace,
    );
    if p.dry_run {
        return Ok(delta);
    }
    if delta.would_expire > 0 && p.confirm_expiring != Some(delta.would_expire) {
        let asked = match p.confirm_expiring {
            None => "no confirmation was given".to_string(),
            Some(n) => format!("the confirmation was for {n}"),
        };
        return Err(ControlError::new(
            ErrorKind::Conflict,
            format!(
                "{}: this policy would expire {} snapshot(s) and {asked}; \
                 review the delta and confirm exactly {}",
                root.path, delta.would_expire, delta.would_expire
            ),
        )
        .with_details(serde_json::to_value(&delta).unwrap_or_default())
        .with_remediation(format!("pass confirm_expiring = {}", delta.would_expire)));
    }
    writer.set(&root, &delta.canonical)?;
    delta.written = true;
    Ok(delta)
}

/// `snapshot.policy.remove`. Without `expire` the root's auto snapshots
/// stay, orphaned: removing a policy freezes its snapshots and never
/// sweeps them. With `expire` (Step 4.2's explicit way) its unheld auto
/// snapshots — counted now, confirmed by `confirm_expiring` like `set`'s
/// guard — are deleted after the policy is gone (so the scheduler takes
/// no new one meanwhile), through `expire`: the scheduler's re-read and
/// holder-side batch (`Scheduler::expire_removed`). A snapshot held or
/// deleted between the count and its delete is kept / skipped, not an
/// error.
fn policy_remove(
    meta: &Meta,
    writer: &Writer,
    p: &SnapPolicyRemoveParams,
    expire: impl FnOnce(&Root, &str, &[SnapshotRow]) -> crate::snapexpire::DeleteRun,
) -> Result<SnapPolicyRemoved, ControlError> {
    let root = root_of(meta, &p.path)?;
    let Some(expr) = stored_policy(meta, root.ino)? else {
        return Err(ControlError::not_found(format!(
            "{}: no snapshot policy",
            root.path
        )));
    };
    let victims: Vec<SnapshotRow> = if p.expire {
        root.rows
            .iter()
            .filter(|r| is_auto(r) && r.policy_ino == root.ino && !r.held)
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    let all = meta.snapshots(None).map_err(internal)?;
    let mut removed = SnapPolicyRemoved {
        root: root_status(meta, root.ino, Some(&expr), &all),
        would_expire: victims.len() as u32,
        would_expire_ids: victims.iter().map(|r| r.id.clone()).collect(),
        ..Default::default()
    };
    if p.dry_run {
        return Ok(removed);
    }
    if p.expire && removed.would_expire > 0 && p.confirm_expiring != Some(removed.would_expire) {
        let asked = match p.confirm_expiring {
            None => "no confirmation was given".to_string(),
            Some(n) => format!("the confirmation was for {n}"),
        };
        return Err(ControlError::new(
            ErrorKind::Conflict,
            format!(
                "{}: removing the policy with expire deletes {} snapshot(s) and {asked}; \
                 review and confirm exactly {}",
                root.path, removed.would_expire, removed.would_expire
            ),
        )
        .with_details(serde_json::to_value(&removed).unwrap_or_default())
        .with_remediation(format!("pass confirm_expiring = {}", removed.would_expire)));
    }
    writer.remove(&root)?;
    removed.written = true;
    if !victims.is_empty() {
        let policy = SnapPolicy::parse(&expr)
            .map(|q| q.to_string())
            .unwrap_or(expr.clone());
        let run = expire(&root, &policy, &victims);
        removed.expired = run.deleted.iter().map(|r| r.id.clone()).collect();
        removed.skipped = run
            .skipped
            .iter()
            .map(|(r, why)| SnapPolicySkipped {
                id: r.id.clone(),
                reason: why.clone(),
            })
            .collect();
        removed.error = run
            .error
            .or_else(|| run.fenced.then(|| "the deletion was fenced".to_string()));
    }
    let rows = meta.snapshots(None).map_err(internal)?;
    removed.root = root_status(meta, root.ino, None, &rows);
    Ok(removed)
}

/// `snapshot.policy.pause`: the stored policy, re-parsed, with `paused`
/// set or cleared, written back in canonical form.
fn policy_pause(
    meta: &Meta,
    writer: &Writer,
    p: &SnapPolicyPauseParams,
) -> Result<SnapPolicyRoot, ControlError> {
    let root = root_of(meta, &p.path)?;
    let expr = stored_policy(meta, root.ino)?
        .ok_or_else(|| ControlError::not_found(format!("{}: no snapshot policy", root.path)))?;
    let mut policy = SnapPolicy::parse(&expr).map_err(|e| {
        ControlError::invalid(format!(
            "{}: the stored policy does not parse ({e}); set a valid one first",
            root.path
        ))
    })?;
    policy.paused = p.paused;
    let canonical = policy.to_string();
    writer.set(&root, &canonical)?;
    let rows = meta.snapshots(None).map_err(internal)?;
    Ok(root_status(meta, root.ino, Some(&canonical), &rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_control::proto::ErrorKind;

    /// 2026-10-01T00:00:00Z.
    const NOW: i64 = 1_790_812_800_000;
    const HOUR: i64 = 3_600_000;

    fn row(id: &str, path: &str, created: i64) -> SnapshotRow {
        SnapshotRow::new(id, path, id, "mtree:0:00:1", created)
    }

    fn auto(id: &str, path: &str, created: i64, policy_ino: u64) -> SnapshotRow {
        SnapshotRow {
            origin: 1,
            policy_ino,
            ..row(id, path, created)
        }
    }

    fn mkdir(meta: &Meta, name: &str) -> u64 {
        let attr = meta
            .mkdir(constellation_fs_core::types::ROOT_INO, name, 0o755, 0, 0)
            .unwrap();
        attr.ino
    }

    #[test]
    fn a_valid_expression_reports_canonical_form_bound_and_count() {
        let r = check_expression("1d:7d 1h:1d", None, NOW);
        assert!(r.ok);
        assert_eq!(r.canonical.as_deref(), Some("1h:1d 1d:7d"));
        assert_eq!(r.steady_state_bound, Some(31));
        let n = r.simulated_count.unwrap();
        assert!((24..31).contains(&n), "{n}");
        assert_eq!(r.simulate_horizon_ms, Some(8 * 24 * HOUR as u64));
        assert!(!r.simulate_truncated);
        assert!(r.simulate_reached_ms.unwrap() > 7 * 24 * HOUR as u64);
        assert!(r.warnings.is_empty() && r.error.is_none() && r.against.is_none());
    }

    /// A keep-everything policy stops early (bounded work, 32-M1c review)
    /// and reports how far it got, short of the horizon.
    #[test]
    fn a_truncated_check_reports_where_it_stopped() {
        let r = check_expression("1m:*", None, NOW);
        assert!(r.ok && r.simulate_truncated, "{r:?}");
        let reached = r.simulate_reached_ms.unwrap();
        assert!(reached < r.simulate_horizon_ms.unwrap(), "{r:?}");
        assert_eq!(
            r.simulated_count,
            Some(check::CHECK_MAX_LIVE as u64),
            "{r:?}"
        );
    }

    #[test]
    fn an_invalid_expression_is_a_result_with_the_offset() {
        let r = check_expression("1h:1d 7m:1d", None, NOW);
        assert!(!r.ok);
        let e = r.error.unwrap();
        assert_eq!(e.offset, 6, "{}", e.message);
        assert!(e.message.contains("7m"), "{}", e.message);
        assert!(r.canonical.is_none() && r.simulated_count.is_none());
    }

    #[test]
    fn warnings_come_through() {
        let r = check_expression("10s:1h 1m:2d", Some(60_000), NOW);
        assert_eq!(r.warnings.len(), 2, "{:?}", r.warnings);
    }

    /// Only the root's own, unheld auto snapshots may be expired: manual,
    /// held (`csi:` and plain) and another root's auto snapshots at the
    /// same path are all kept.
    #[test]
    fn against_expires_only_the_roots_unheld_auto_snapshots() {
        let meta = Meta::open_in_memory().unwrap();
        let proj = mkdir(&meta, "proj");
        let other = proj + 1000;
        // Hourly auto snapshots for two days; `1h:1d` keeps the last day.
        for h in 0..48 {
            meta.record_snapshot(&auto(
                &format!("a{h:02}"),
                "/proj",
                NOW - (48 - h) * HOUR,
                proj,
            ))
            .unwrap();
        }
        let old = NOW - 47 * HOUR;
        meta.record_snapshot(&row("manual", "/proj", old)).unwrap();
        meta.record_snapshot(&SnapshotRow {
            held: true,
            held_by: Some("csi:content-uid".into()),
            ..auto("csi-held", "/proj", old + 1, proj)
        })
        .unwrap();
        meta.record_snapshot(&SnapshotRow {
            held: true,
            ..auto("plain-held", "/proj", old + 2, proj)
        })
        .unwrap();
        meta.record_snapshot(&auto("foreign", "/proj", old + 3, other))
            .unwrap();
        // Recorded elsewhere (renamed since) but owned by this root.
        meta.record_snapshot(&auto("renamed", "/old-name", old + 4, proj))
            .unwrap();
        // Unrelated.
        meta.record_snapshot(&row("elsewhere", "/x", old)).unwrap();

        let r = policy_check(
            &meta,
            &SnapPolicyCheckParams {
                expr: "1h:1d".into(),
                against: Some("proj/".into()),
                simulate_ms: Some(HOUR as u64),
            },
            NOW,
        )
        .unwrap();
        assert!(r.ok);
        let a = r.against.unwrap();
        assert_eq!(a.path, "/proj");
        assert_eq!(a.policy_ino, proj);
        assert_eq!(a.snapshots, 48 + 5, "48 hourlies + 5 others");
        let by_id = |id: &str| a.verdicts.iter().find(|v| v.id == id).unwrap();
        for keeper in ["manual", "csi-held", "plain-held", "foreign"] {
            assert!(by_id(keeper).keep, "{keeper}");
        }
        assert_eq!(by_id("csi-held").reasons[0].kind, "held");
        assert_eq!(
            by_id("csi-held").reasons[0].held_by.as_deref(),
            Some("csi:content-uid")
        );
        assert_eq!(
            by_id("csi-held").held_by.as_deref(),
            Some("csi:content-uid")
        );
        assert_eq!(by_id("manual").reasons[0].kind, "not_candidate");
        assert_eq!(by_id("manual").origin, "manual");
        assert_eq!(by_id("foreign").reasons[0].kind, "not_candidate");
        // Every expired one is a same-ino, unheld, auto snapshot.
        for v in a.verdicts.iter().filter(|v| !v.keep) {
            assert!(
                v.origin == "auto" && v.policy_ino == proj && !v.held,
                "{v:?}"
            );
            assert!(v.reasons.is_empty());
        }
        // `1h:1d` anchored on the newest hourly keeps 24 of the 48, plus
        // nothing of the older day; the renamed one is in the older day.
        assert_eq!(a.would_expire, 24 + 1, "{:?}", a.verdicts);
        assert!(!by_id("renamed").keep);
        assert!(by_id("a47").keep && !by_id("a00").keep);
        // Oldest first.
        assert!(a
            .verdicts
            .windows(2)
            .all(|w| w[0].created_unix_ms <= w[1].created_unix_ms));
    }

    #[test]
    fn against_refuses_missing_paths_and_files() {
        let meta = Meta::open_in_memory().unwrap();
        let params = |against: &str| SnapPolicyCheckParams {
            expr: "1h:1d".into(),
            against: Some(against.into()),
            simulate_ms: None,
        };
        let missing = policy_check(&meta, &params("/nope"), NOW).unwrap_err();
        assert_eq!(missing.kind, ErrorKind::NotFound);
        meta.create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let file = policy_check(&meta, &params("/f"), NOW).unwrap_err();
        assert_eq!(file.kind, ErrorKind::Invalid, "{file:?}");
        // An invalid expression is still a result, after the path check.
        let r = policy_check(
            &meta,
            &SnapPolicyCheckParams {
                expr: "1M:1y".into(),
                against: Some("/".into()),
                simulate_ms: None,
            },
            NOW,
        )
        .unwrap();
        assert!(!r.ok && r.error.is_some());
    }

    #[test]
    fn simulate_is_retentions_timeline_field_for_field() {
        let meta = Meta::open_in_memory().unwrap();
        let proj = mkdir(&meta, "proj");
        meta.record_snapshot(&row("manual", "/proj", NOW - HOUR))
            .unwrap();
        meta.record_snapshot(&SnapshotRow {
            held: true,
            held_by: Some("csi:x".into()),
            ..auto("held", "/proj", NOW - 2 * HOUR, proj)
        })
        .unwrap();
        let p = SnapPolicySimulateParams {
            path: Some("/proj".into()),
            expr: "1h:6h".into(),
            horizon_ms: 12 * HOUR as u64,
        };
        let got = policy_simulate(&meta, &p, NOW).unwrap();
        let facts: Vec<SnapFacts> = root_of(&meta, "/proj")
            .unwrap()
            .rows
            .iter()
            .map(SnapFacts::from_row)
            .collect();
        let want = retention::simulate(
            &SnapPolicy::parse("1h:6h").unwrap(),
            proj,
            &facts,
            NOW,
            12 * HOUR,
        );
        assert_eq!(
            serde_json::to_value(&got).unwrap(),
            serde_json::to_value(&want).unwrap()
        );
        assert_eq!(got.created, 13);
        assert_eq!(got.policy_ino, proj);
        assert!(got
            .snapshots
            .iter()
            .any(|s| s.held_by.as_deref() == Some("csi:x")));
        // No path: an empty history at policy_ino 0.
        let bare = policy_simulate(
            &meta,
            &SnapPolicySimulateParams {
                path: None,
                ..p.clone()
            },
            NOW,
        )
        .unwrap();
        assert_eq!((bare.policy_ino, bare.snapshots.len()), (0, 13));
        let bad = policy_simulate(
            &meta,
            &SnapPolicySimulateParams {
                expr: "nonsense".into(),
                ..p
            },
            NOW,
        )
        .unwrap_err();
        assert_eq!(bad.kind, ErrorKind::Invalid);
    }

    /// `list` over what replay may hold: an unparseable expression
    /// (replay never validates), a parseable one, and auto snapshots of
    /// a directory that is gone — its stream is orphaned, with no path.
    #[test]
    fn list_reports_roots_unparseable_ones_and_orphaned_streams() {
        use constellation_meta::SetXattrMode;
        let meta = Meta::open_in_memory().unwrap();
        let good = mkdir(&meta, "good");
        let bad = mkdir(&meta, "bad");
        let gone = mkdir(&meta, "gone");
        meta.set_xattr(
            good,
            SNAPSHOT_POLICY_XATTR,
            b"1d:7d 1h:1d",
            SetXattrMode::Set,
        )
        .unwrap();
        meta.set_xattr(
            bad,
            SNAPSHOT_POLICY_XATTR,
            b"1h:1d 7m:1d",
            SetXattrMode::Set,
        )
        .unwrap();
        meta.record_snapshot(&auto("b1", "/bad", NOW - HOUR, bad))
            .unwrap();
        meta.record_snapshot(&auto("g1", "/good", NOW - HOUR, good))
            .unwrap();
        meta.record_snapshot(&auto("x1", "/gone", NOW - 2 * HOUR, gone))
            .unwrap();
        meta.record_snapshot(&auto("x2", "/gone", NOW - HOUR, gone))
            .unwrap();
        meta.rmdir(constellation_fs_core::types::ROOT_INO, "gone")
            .unwrap();

        let roots = policy_list(&meta).unwrap().roots;
        let inos: Vec<u64> = roots.iter().map(|r| r.ino).collect();
        assert_eq!(inos, [good, bad, gone], "roots by ino, then orphans");
        let [g, b, x] = &roots[..] else {
            unreachable!()
        };
        assert_eq!(g.path.as_deref(), Some("/good"));
        assert_eq!(g.canonical.as_deref(), Some("1h:1d 1d:7d"));
        assert_eq!((g.auto_snapshots, g.orphaned, g.paused), (1, false, false));
        assert_eq!(b.expr, "1h:1d 7m:1d");
        assert_eq!(b.canonical, None);
        assert_eq!(b.error.as_ref().map(|e| e.offset), Some(6));
        assert!(b.orphaned, "an unparseable policy orphans its snapshots");
        assert_eq!(x.path, None, "the directory is gone");
        assert_eq!(
            (x.expr.as_str(), x.auto_snapshots, x.orphaned),
            ("", 2, true)
        );

        // `show` refuses a directory with neither a policy nor autos.
        mkdir(&meta, "plain");
        let unknown = snapexpire::GraceView {
            state: None,
            now: NOW,
            grace_ms: 24 * HOUR,
        };
        let e = policy_show(&meta, "/plain", &unknown).unwrap_err();
        assert_eq!(e.kind, ErrorKind::NotFound);
        let shown = policy_show(&meta, "/bad", &unknown).unwrap();
        assert!(shown.verdicts.is_none() && shown.root.orphaned);
    }

    /// `show`'s verdicts, `snapshot.list`'s `KEPT BY` and `set`'s note
    /// apply the same grace: inside a window they agree on what the next
    /// run deletes (nothing during a first sighting), after it on
    /// exactly what the policy expires.
    #[test]
    fn show_list_and_note_apply_the_same_grace() {
        use constellation_meta::SetXattrMode;
        let meta = Meta::open_in_memory().unwrap();
        let proj = mkdir(&meta, "proj");
        meta.set_xattr(proj, SNAPSHOT_POLICY_XATTR, b"1h:2h", SetXattrMode::Set)
            .unwrap();
        for h in 0..6 {
            let id = format!("a{h}");
            meta.record_snapshot(&auto(&id, "/proj", NOW - (6 - h) * HOUR, proj))
                .unwrap();
        }
        // First seen at NOW - 1h: its window is open until NOW + 23h.
        let mut state = SnapSchedState::default();
        let canonical = snapexpire::canonical_unpaused(&SnapPolicy::parse("1h:2h").unwrap());
        snapexpire::observe(&mut state, &[(proj, canonical)], NOW - HOUR, 24 * HOUR);
        let view = |now| snapexpire::GraceView {
            state: Some(state.clone()),
            now,
            grace_ms: 24 * HOUR,
        };
        let agree = |now| {
            let grace = view(now);
            let shown = policy_show(&meta, "/proj", &grace).unwrap();
            let listed = snapexpire::listing(&meta, &grace).unwrap();
            let verdicts = shown.verdicts.unwrap().verdicts;
            for v in &verdicts {
                assert_eq!(listed[&v.id].kept_by.is_empty(), !v.keep, "{now}: {v:?}");
            }
            verdicts
        };
        let inside = agree(NOW);
        assert!(inside.iter().all(|v| v.keep));
        assert_eq!(inside[0].reasons[0].kind, "grace");
        let after = agree(NOW + 23 * HOUR);
        assert_eq!(after.iter().filter(|v| !v.keep).count(), 4);
        // The note: the retention is unchanged, but the first sighting's
        // window still holds everything back.
        let root = root_of(&meta, "/proj").unwrap();
        let policy = SnapPolicy::parse("1h:2h").unwrap();
        let plain = against_of(&policy, &root);
        let note = grace_note(&policy, Some("1h:2h"), &root, &plain.verdicts, &view(NOW));
        assert_eq!(
            note,
            "the retention is unchanged: nothing is deleted for 23h; after that 4 snapshots \
             expire"
        );
        let note = grace_note(
            &policy,
            Some("1h:2h"),
            &root,
            &plain.verdicts,
            &view(NOW + 23 * HOUR),
        );
        assert_eq!(
            note,
            "the retention is unchanged: 4 snapshots expire at the scheduler's next expiry run"
        );
        assert_eq!(human_remaining(23 * HOUR - 1), "23h");
        assert_eq!(human_remaining(23 * HOUR - 60_001), "22h59m");
    }
}
