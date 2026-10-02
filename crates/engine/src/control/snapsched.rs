//! The `snapshot.policy.*` methods (plan 32): automatic snapshot
//! schedules, as the control protocol exposes them.
//!
//! M1 has only the read-only pair — `snapshot.policy.check` (Step 5's
//! `policy check`) and `snapshot.policy.simulate` (Step 2's simulation,
//! the web UI's retention timeline). M2 adds `set/show/list/remove/pause`
//! here, M3 the scheduler's status.
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

use super::{unary, EngineControl};
use constellation_control::methods::{SnapshotPolicyCheck, SnapshotPolicySimulate};
use constellation_control::proto::types::{
    PolicyErrorInfo, SnapPolicyAgainst, SnapPolicyCheckParams, SnapPolicyCheckResult,
    SnapPolicySimulateParams, SnapReason, SnapTimeline, SnapVerdict,
};
use constellation_control::proto::ControlError;
use constellation_control::Router;
use constellation_fs_core::InodeKind;
use constellation_meta::snapsched::{check, retention, SnapFacts, SnapPolicy};
use constellation_meta::{Meta, MetaStore, SnapshotRow};
use std::sync::Arc;

pub(super) fn register(r: &mut Router, svc: &Arc<EngineControl>) {
    unary::<SnapshotPolicyCheck>(r, svc, |s, _, p| {
        policy_check(&s.meta, &p, constellation_store_s3::lease::now_unix_ms())
    });
    unary::<SnapshotPolicySimulate>(r, svc, |s, _, p| {
        policy_simulate(&s.meta, &p, constellation_store_s3::lease::now_unix_ms())
    });
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
    let against = root.map(|root| {
        let verdicts = retention::evaluate(&policy, root.ino, &facts);
        let verdicts: Vec<SnapVerdict> = root
            .rows
            .iter()
            .zip(&verdicts)
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
    });
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
}
