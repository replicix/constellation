//! `constellation snapshot sched status | run [--dry-run]` (plan 32 Step 5,
//! M3): the CLI over `snapshot.sched.status` and `snapshot.sched.run`.
//!
//! Both are about *this node's* scheduler: the counters are node-local
//! (only the leader's `created` moves), and the roots are what this node's
//! replica sees now. Run `status` on every node to find the leader — the
//! one line saying `leader` — which is what the harness does before it
//! kills it. As in `policy_cli`, every figure is the daemon's and the
//! rendering is pure functions the tests drive directly.

use crate::control;
use crate::policy_cli::align;
use crate::snapshot_cli::utc_seconds;
use anyhow::{bail, Result};
use constellation_control::methods as cm;
use constellation_control::proto::types as api;
use std::path::Path;

/// A root's state: why it is not armed, else whether it is due now.
/// `unparseable` and `gone` win over `paused` (they are what to act on);
/// `capped` is armed but full.
pub fn state(root: &api::SnapSchedRootState) -> &'static str {
    if root.canonical.is_none() {
        "unparseable"
    } else if root.path.is_none() {
        "gone"
    } else if root.paused {
        "paused"
    } else if root.capped {
        "capped"
    } else if root.due {
        "due"
    } else {
        "armed"
    }
}

/// `NEXT`: `now (<bucket name>)` while due, the next bucket's start
/// otherwise, `-` when it never will be (paused, unparseable, gone).
fn next_cell(root: &api::SnapSchedRootState) -> String {
    match (root.due, root.next_due_unix_ms) {
        (true, _) => match &root.bucket_name {
            Some(name) => format!("now ({name})"),
            None => "now".into(),
        },
        (false, Some(ms)) => utc_seconds(ms),
        (false, None) => "-".into(),
    }
}

fn opt_time(ms: Option<i64>) -> String {
    ms.filter(|&ms| ms > 0)
        .map_or_else(|| "-".into(), utc_seconds)
}

/// `snapshot sched status`: the node line (leader or not), the counters,
/// then one row per policy root.
pub fn render_status(r: &api::SnapSchedReport) -> String {
    let s = &r.stats;
    let mut out = format!(
        "node {}: {} (scheduler {}, tick {} ms, cap {} auto snapshots per root)\n",
        r.node_id,
        if s.leader { "leader" } else { "not leading" },
        if r.enabled {
            "enabled"
        } else {
            "disabled here"
        },
        r.tick_ms,
        r.max_per_root,
    );
    out.push_str(&format!(
        "ticks {}  created {}  skipped-empty {}  create-failed {}  expired {}  \
         refused-lag {}  refused-state {}\n",
        s.ticks,
        s.created,
        s.skipped_empty,
        s.create_failed,
        s.expired,
        s.refused_lag,
        s.refused_state,
    ));
    out.push_str(&format!(
        "roots {} (paused {}, unparseable {}, capped {})  orphaned snapshots {}  \
         last created {}\n",
        s.roots,
        s.paused_roots,
        s.unparseable_roots,
        s.capped_roots,
        s.orphaned_snapshots,
        opt_time(i64::try_from(s.last_create_unix_ms).ok()),
    ));
    if let Some(e) = &s.last_error {
        out.push_str(&format!("last error: {e}\n"));
    }
    if let Some((expr, offset, msg)) = &s.last_parse_error {
        out.push_str(&format!(
            "last refused policy: {expr:?}: {msg} (byte {offset})\n"
        ));
    }
    out.push('\n');
    if r.roots.is_empty() {
        out.push_str("no snapshot policies\n");
        return out;
    }
    let rows: Vec<Vec<String>> = r
        .roots
        .iter()
        .map(|root| {
            vec![
                root.path.clone().unwrap_or_else(|| "(unlinked)".into()),
                root.canonical.clone().unwrap_or_else(|| root.expr.clone()),
                state(root).into(),
                next_cell(root),
                opt_time(root.last_created_unix_ms),
                root.error.clone().unwrap_or_else(|| "-".into()),
            ]
        })
        .collect();
    out.push_str(&align(
        &["PATH", "POLICY", "STATE", "NEXT", "LAST CREATED", "ERROR"],
        &rows,
    ));
    out.push_str("times UTC\n");
    out
}

/// `snapshot sched run`'s report, and whether it counts as a failure
/// (exit non-zero): refused, a whole-batch error, or a failed root. A dry
/// run fails only when refused (it creates nothing to fail).
pub fn render_run(r: &api::SnapSchedRunResult) -> (String, bool) {
    let mut out = String::new();
    let mut failed = false;
    if r.dry_run {
        out.push_str("dry run: no lease taken, nothing created\n");
    } else if r.leader {
        out.push_str("this node leads the scheduler\n");
    }
    if let Some(why) = &r.refused {
        out.push_str(&format!("refused: {why}\n"));
        failed = true;
    }
    if let Some(e) = &r.error {
        out.push_str(&format!(
            "error: {e} (nothing created; the next tick retries)\n"
        ));
        failed = true;
    }
    if r.roots.is_empty() {
        if r.refused.is_none() && r.error.is_none() {
            out.push_str("nothing due\n");
        }
        return (out, failed);
    }
    let rows: Vec<Vec<String>> = r
        .roots
        .iter()
        .map(|root| {
            failed |= root.outcome == "failed";
            let detail = match (&root.error, &root.id) {
                (Some(e), _) => e.clone(),
                (None, Some(id)) => id.clone(),
                (None, None) => "-".into(),
            };
            vec![
                root.path.clone(),
                root.name.clone(),
                root.outcome.clone(),
                detail,
            ]
        })
        .collect();
    out.push_str(&align(&["PATH", "NAME", "OUTCOME", "ID / ERROR"], &rows));
    (out, failed)
}

/// `snapshot sched status [--json]`.
pub async fn status(dir: &Path, json: bool) -> Result<()> {
    let report = control::call::<cm::SnapshotSchedStatus>(dir, Default::default()).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", render_status(&report));
    }
    Ok(())
}

/// `snapshot sched run [--dry-run] [--json]`. Exits non-zero when the
/// tick was refused, failed as a whole, or failed a root.
pub async fn run(dir: &Path, dry_run: bool, json: bool) -> Result<()> {
    let result =
        control::call::<cm::SnapshotSchedRun>(dir, api::SnapSchedRunParams { dry_run }).await?;
    let (text, failed) = render_run(&result);
    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        print!("{text}");
    }
    if failed {
        bail!("snapshot sched run: not every due snapshot was taken");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(path: Option<&str>, expr: &str) -> api::SnapSchedRootState {
        api::SnapSchedRootState {
            ino: 7,
            path: path.map(Into::into),
            expr: expr.into(),
            canonical: Some(expr.into()),
            ..Default::default()
        }
    }

    /// Every run of blanks as one space: the column widths are not the
    /// point.
    fn squeezed(text: &str) -> String {
        text.lines()
            .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn states_put_what_to_act_on_first() {
        let mut r = root(Some("/a"), "10s:1m");
        assert_eq!(state(&r), "armed");
        r.due = true;
        assert_eq!(state(&r), "due");
        r.capped = true;
        assert_eq!(state(&r), "capped");
        r.paused = true;
        assert_eq!(state(&r), "paused");
        r.path = None;
        assert_eq!(state(&r), "gone");
        r.canonical = None;
        assert_eq!(state(&r), "unparseable");
    }

    #[test]
    fn status_names_the_leader_counters_and_every_root() {
        let mut due = root(Some("/proj"), "10s:1m 1m:4m; last=2");
        due.due = true;
        due.bucket_name = Some("auto-20261002T120010Z".into());
        due.last_created_unix_ms = Some(1_790_000_000_000);
        let mut later = root(Some("/db"), "1h:1d");
        later.next_due_unix_ms = Some(1_790_000_000_000);
        let mut bad = root(None, "garbage");
        bad.canonical = None;
        bad.error = Some("unknown tier".into());
        let report = api::SnapSchedReport {
            node_id: 2,
            enabled: true,
            tick_ms: 1000,
            max_per_root: 5000,
            stats: api::SnapSchedStatus {
                leader: true,
                ticks: 9,
                created: 3,
                create_failed: 1,
                roots: 3,
                last_error: Some("s3 down".into()),
                ..Default::default()
            },
            roots: vec![due, later, bad],
        };
        let text = render_status(&report);
        assert!(
            text.starts_with("node 2: leader (scheduler enabled, tick 1000 ms"),
            "{text}"
        );
        assert!(
            text.contains("created 3  skipped-empty 0  create-failed 1"),
            "{text}"
        );
        assert!(text.contains("last error: s3 down"), "{text}");
        let lines: Vec<&str> = text.lines().collect();
        let header = lines.iter().position(|l| l.starts_with("PATH")).unwrap();
        assert_eq!(
            lines[header].split_whitespace().collect::<Vec<_>>(),
            ["PATH", "POLICY", "STATE", "NEXT", "LAST", "CREATED", "ERROR"]
        );
        assert!(lines[header + 1].starts_with("/proj"), "{text}");
        assert!(lines[header + 1].contains("due"), "{text}");
        assert!(
            lines[header + 1].contains("now (auto-20261002T120010Z)"),
            "{text}"
        );
        assert!(lines[header + 1].contains("2026-09-21 14:13:20"), "{text}");
        assert!(lines[header + 2].contains("armed"), "{text}");
        assert!(lines[header + 2].contains("2026-09-21 14:13:20"), "{text}");
        assert!(lines[header + 3].starts_with("(unlinked)"), "{text}");
        assert!(lines[header + 3].contains("unparseable"), "{text}");
        assert!(lines[header + 3].contains("unknown tier"), "{text}");

        let idle = api::SnapSchedReport {
            node_id: 1,
            enabled: false,
            ..Default::default()
        };
        let text = render_status(&idle);
        assert!(
            text.starts_with("node 1: not leading (scheduler disabled here"),
            "{text}"
        );
        assert!(text.ends_with("no snapshot policies\n"), "{text}");
    }

    #[test]
    fn run_fails_on_refusal_batch_error_or_a_failed_root() {
        let created = api::SnapSchedRunRoot {
            ino: 7,
            path: "/proj".into(),
            name: "auto-20261002T120010Z".into(),
            outcome: "created".into(),
            id: Some("abc".into()),
            error: None,
        };
        let ok = api::SnapSchedRunResult {
            leader: true,
            roots: vec![created.clone()],
            ..Default::default()
        };
        let (text, failed) = render_run(&ok);
        assert!(!failed, "{text}");
        assert!(text.contains("this node leads"), "{text}");
        assert!(
            squeezed(&text).contains("/proj auto-20261002T120010Z created abc"),
            "{text}"
        );

        let refused = api::SnapSchedRunResult {
            refused: Some("_snapsched is held by node 3".into()),
            ..Default::default()
        };
        let (text, failed) = render_run(&refused);
        assert!(failed);
        assert!(
            text.contains("refused: _snapsched is held by node 3"),
            "{text}"
        );
        assert!(!text.contains("nothing due"), "{text}");

        let mut bad = created.clone();
        bad.outcome = "failed".into();
        bad.error = Some("not the policy root".into());
        let (text, failed) = render_run(&api::SnapSchedRunResult {
            leader: true,
            roots: vec![bad],
            ..Default::default()
        });
        assert!(failed);
        assert!(
            squeezed(&text).contains("failed not the policy root"),
            "{text}"
        );

        let (text, failed) = render_run(&api::SnapSchedRunResult {
            leader: true,
            error: Some("timeout".into()),
            ..Default::default()
        });
        assert!(failed);
        assert!(text.contains("error: timeout"), "{text}");

        let mut would = created;
        would.outcome = "would_create".into();
        would.id = None;
        let (text, failed) = render_run(&api::SnapSchedRunResult {
            dry_run: true,
            roots: vec![would],
            ..Default::default()
        });
        assert!(!failed);
        assert!(text.starts_with("dry run: no lease taken"), "{text}");
        assert!(squeezed(&text).contains("would_create -"), "{text}");

        let (text, failed) = render_run(&api::SnapSchedRunResult {
            leader: true,
            ..Default::default()
        });
        assert!(!failed);
        assert!(text.ends_with("nothing due\n"), "{text}");
    }
}
