//! `constellation snapshot policy set|show|ls|rm|pause|resume` (plan 32
//! Step 5, M2): the CLI over the `snapshot.policy.*` control methods.
//!
//! Every figure printed here is the daemon's. The CLI never evaluates
//! retention: `set`'s delta (what the new policy creates and would
//! expire), `show`'s `KEPT BY` and every root's state come back from the
//! methods, and this module only lays them out. The rendering is pure
//! functions the tests drive directly; the command flows below them only
//! sequence calls and prompts.
//!
//! `set` is two calls on purpose. The first is a `dry_run`, whose delta is
//! shown (and, when it would expire anything, confirmed); the second
//! passes that delta's `would_expire` back as `confirm_expiring`. The
//! daemon writes only if its own count still matches, so a snapshot taken
//! or held between the two calls makes the write fail loudly instead of
//! confirming a delta nobody saw. The CLI does not retry that: the
//! operator reruns and reviews the new delta.

use crate::control;
use crate::snapshot_cli::{confirm, under, utc_minutes};
use anyhow::{bail, Result};
use constellation_control::methods as cm;
use constellation_control::proto::types as api;
use constellation_control::proto::{ControlError, ErrorKind};
use std::path::Path;

/// How many expiring snapshot ids `set` lists before summarizing.
const LISTED_IDS: usize = 10;

/// What the daemon said about `set`'s expression, rendered for a person:
/// a refusal of the expression itself carries `{offset, message}` in its
/// details, which become the caret under `expr`.
fn parse_error_of(e: &ControlError) -> Option<constellation_meta::policy_lex::PolicyError> {
    if e.kind != ErrorKind::Invalid {
        return None;
    }
    let details = e.details.as_ref()?;
    let offset = details.0.get("offset")?.as_u64()?;
    let message = details.0.get("message")?.as_str()?;
    Some(constellation_meta::policy_lex::PolicyError {
        offset: usize::try_from(offset).unwrap_or(usize::MAX),
        msg: message.to_string(),
    })
}

/// A daemon refusal as one line: its message, then what to do about it.
fn refusal(e: &ControlError) -> String {
    match &e.remediation {
        Some(fix) => format!("{} ({fix})", e.message),
        None => e.message.clone(),
    }
}

/// A root's state as `policy ls`/`show` print it. An expression that does
/// not parse is `unparseable` whether or not it has snapshots (the error
/// is what to act on); a stream with no expression at all is `orphaned`.
pub fn state(root: &api::SnapPolicyRoot) -> &'static str {
    if root.error.is_some() {
        "unparseable"
    } else if root.canonical.is_none() {
        "orphaned"
    } else if root.paused {
        "paused"
    } else {
        "armed"
    }
}

/// The policy cell: the canonical form, else the stored text, else `-`.
fn policy_text(root: &api::SnapPolicyRoot) -> &str {
    match root.canonical.as_deref() {
        Some(canonical) => canonical,
        None if !root.expr.is_empty() => &root.expr,
        None => "-",
    }
}

fn path_text(root: &api::SnapPolicyRoot) -> &str {
    root.path.as_deref().unwrap_or("(unlinked)")
}

/// Left-aligned columns two spaces apart, padded by `char`s (`—`, `⚑`),
/// trailing blanks trimmed.
fn align(header: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = (0..header.len())
        .map(|i| {
            rows.iter()
                .map(|r| r[i].chars().count())
                .chain([header[i].chars().count()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    let lines = std::iter::once(header.iter().map(|h| h.to_string()).collect::<Vec<_>>())
        .chain(rows.iter().cloned());
    for cells in lines {
        let mut line = String::new();
        for (i, cell) in cells.iter().enumerate() {
            line.push_str(&format!("{cell:<w$}  ", w = widths[i]));
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

/// `snapshot policy ls`'s table, or `no snapshot policies`. Rows keep the
/// daemon's order (policy roots, then orphaned streams, each by inode);
/// `under` keeps the roots at or below a directory (an unlinked stream
/// has no path, so only an unfiltered listing shows it).
pub fn render_roots(roots: &[api::SnapPolicyRoot], under_dir: Option<&str>) -> String {
    let rows: Vec<Vec<String>> = roots
        .iter()
        .filter(|r| match under_dir {
            None => true,
            Some(dir) => r.path.as_deref().is_some_and(|p| under(p, dir)),
        })
        .map(|r| {
            vec![
                path_text(r).to_string(),
                r.ino.to_string(),
                policy_text(r).to_string(),
                state(r).to_string(),
                r.auto_snapshots.to_string(),
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no snapshot policies\n".into();
    }
    align(&["PATH", "INO", "POLICY", "STATE", "AUTO SNAPSHOTS"], &rows)
}

/// `set`'s delta sentence (plan 32 Step 5): "creates every 5m; will
/// expire 312 snapshots; <grace note>".
pub fn delta_sentence(d: &api::SnapPolicyDelta) -> String {
    let creates = match &d.creates_every {
        Some(every) => format!("creates every {every}"),
        None => "creates nothing (paused)".to_string(),
    };
    let expires = match d.would_expire {
        0 => "will expire nothing".to_string(),
        1 => "will expire 1 snapshot".to_string(),
        n => format!("will expire {n} snapshots"),
    };
    format!("{creates}; {expires}; {}", d.grace_note)
}

/// `set`'s whole preview: the change, the delta sentence, the expiring
/// ids (the first [`LISTED_IDS`]), then the warnings.
pub fn render_delta(d: &api::SnapPolicyDelta) -> String {
    let mut out = format!("{} (ino {}): ", d.path, d.ino);
    match d.previous.as_deref() {
        Some(previous) if previous == d.canonical => {
            out.push_str(&format!("{} (unchanged)\n", d.canonical))
        }
        Some(previous) => out.push_str(&format!("{previous} -> {}\n", d.canonical)),
        None => out.push_str(&format!("(no policy) -> {}\n", d.canonical)),
    }
    out.push_str(&delta_sentence(d));
    out.push('\n');
    for id in d.would_expire_ids.iter().take(LISTED_IDS) {
        out.push_str(&format!("  would expire {id}\n"));
    }
    if d.would_expire_ids.len() > LISTED_IDS {
        out.push_str(&format!(
            "  ... and {} more\n",
            d.would_expire_ids.len() - LISTED_IDS
        ));
    }
    for w in &d.warnings {
        out.push_str(&format!("warning: {w}\n"));
    }
    out
}

/// A verdict's `KEPT BY` cell (plan 32 Step 5): the keeping tiers as
/// `5m·1h·1d`, `last=n`, a hold by owner namespace (`held: csi`), or
/// `expire` for one the policy would delete.
pub fn kept_by(v: &api::SnapVerdict) -> String {
    if !v.keep {
        return "expire".to_string();
    }
    let tiers: Vec<&str> = v
        .reasons
        .iter()
        .filter(|r| r.kind == "tier")
        .filter_map(|r| r.every.as_deref())
        .collect();
    let mut parts = Vec::new();
    if !tiers.is_empty() {
        parts.push(tiers.join("·"));
    }
    for r in &v.reasons {
        match r.kind.as_str() {
            "tier" => {}
            "last" => parts.push(format!("last={}", r.last.unwrap_or(1))),
            "held" => parts.push(
                match r.held_by.as_deref().and_then(|by| by.split(':').next()) {
                    Some(ns) if !ns.is_empty() => format!("held: {ns}"),
                    _ => "held".to_string(),
                },
            ),
            "grace" => parts.push("grace".to_string()),
            "not_candidate" if v.origin == "auto" => parts.push("another policy".to_string()),
            "not_candidate" => parts.push(v.origin.clone()),
            other => parts.push(other.to_string()),
        }
    }
    parts.join(", ")
}

/// `snapshot policy show`: the root, then its own auto snapshots with the
/// daemon's verdicts. `orphans` are the root's auto snapshots as
/// `snapshot.list` rows, used only when there are no verdicts (no
/// parseable policy: nothing keeps or expires them, they are orphaned).
pub fn render_shown(shown: &api::SnapPolicyShown, orphans: &[api::SnapshotStatus]) -> String {
    let root = &shown.root;
    let mut out = format!("{} (ino {})\n", path_text(root), root.ino);
    out.push_str(&format!("  policy:  {}\n", policy_text(root)));
    if let (Some(canonical), false) = (&root.canonical, root.expr.is_empty()) {
        if *canonical != root.expr {
            out.push_str(&format!("  stored:  {}\n", root.expr));
        }
    }
    out.push_str(&format!("  state:   {}\n", state(root)));
    if let Some(e) = &root.error {
        let e = constellation_meta::policy_lex::PolicyError {
            offset: usize::try_from(e.offset).unwrap_or(usize::MAX),
            msg: e.message.clone(),
        };
        for line in e.render(&root.expr).lines() {
            out.push_str(&format!("  {line}\n"));
        }
    }
    out.push_str(&format!("  auto snapshots: {}\n", root.auto_snapshots));
    let rows: Vec<Vec<String>> = match &shown.verdicts {
        Some(against) => against
            .verdicts
            .iter()
            .filter(|v| v.origin == "auto" && v.policy_ino == root.ino)
            .map(|v| {
                vec![
                    format!("{}@{}", v.path, v.name),
                    utc_minutes(v.created_unix_ms),
                    kept_by(v),
                ]
            })
            .collect(),
        None => orphans
            .iter()
            .map(|s| {
                vec![
                    format!("{}@{}", s.path, s.name),
                    utc_minutes(s.created_unix_ms),
                    if s.held { "held" } else { "orphaned" }.to_string(),
                ]
            })
            .collect(),
    };
    if let Some(against) = &shown.verdicts {
        out.push_str(&format!(
            "  would expire {} of {} snapshots{}\n",
            against.would_expire,
            against.snapshots,
            if root.paused { " (paused)" } else { "" }
        ));
    }
    if !rows.is_empty() {
        for line in align(&["NAME", "CREATED (UTC)", "KEPT BY"], &rows).lines() {
            out.push_str(&format!("  {line}\n"));
        }
    }
    out
}

/// `set`'s refusal of the write because the preview went stale: what the
/// policy would expire now (the delta in the conflict's details) against
/// what was previewed. Without a decodable delta, the daemon's own words.
fn conflict_message(preview: &api::SnapPolicyDelta, e: &ControlError) -> String {
    let now = e
        .details
        .clone()
        .and_then(|d| serde_json::from_value::<api::SnapPolicyDelta>(d.0).ok());
    match now {
        Some(now) if now.would_expire != preview.would_expire => format!(
            "{}: nothing written: the policy would now expire {} snapshot(s), not the \
             {} previewed (snapshots changed meanwhile); rerun to review the new delta",
            preview.path, now.would_expire, preview.would_expire
        ),
        _ => format!("nothing written: {}", refusal(e)),
    }
}

/// `/a//b/` → `/a/b`; empty → `/` (the server's `normalize_control_path`).
fn normalize_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    format!("/{}", parts.join("/"))
}

/// `snapshot policy set <fs:path> '<expr>' [--yes] [--dry-run]`.
pub async fn set(dir: &Path, path: String, expr: String, yes: bool, dry_run: bool) -> Result<()> {
    let preview = control::try_call::<cm::SnapshotPolicySet>(
        dir,
        api::SnapPolicySetParams {
            path: path.clone(),
            expr: expr.clone(),
            confirm_expiring: None,
            dry_run: true,
        },
        None,
    )
    .await?;
    let preview = match preview {
        Ok(delta) => delta,
        Err(e) => match parse_error_of(&e) {
            // The same two-line caret as `policy check`, and its exit code.
            Some(parse) => {
                eprintln!("{}", parse.render(&expr));
                std::process::exit(2);
            }
            None => bail!("{}", refusal(&e)),
        },
    };
    print!("{}", render_delta(&preview));
    if dry_run {
        println!("dry run: nothing written");
        return Ok(());
    }
    if preview.would_expire > 0 && !yes {
        let question = format!(
            "set this policy on {}? it would expire {} snapshot(s)",
            preview.path, preview.would_expire
        );
        if !confirm(&question)? {
            bail!("nothing written");
        }
    }
    let written = control::try_call::<cm::SnapshotPolicySet>(
        dir,
        api::SnapPolicySetParams {
            path,
            expr,
            confirm_expiring: Some(preview.would_expire),
            dry_run: false,
        },
        None,
    )
    .await?;
    match written {
        Ok(delta) => {
            println!("{}: snapshot policy set: {}", delta.path, delta.canonical);
            Ok(())
        }
        Err(e) if e.kind == ErrorKind::Conflict => bail!("{}", conflict_message(&preview, &e)),
        Err(e) => bail!("{}", refusal(&e)),
    }
}

/// `snapshot policy show <fs:path>`.
pub async fn show(dir: &Path, path: String) -> Result<()> {
    let shown =
        control::call::<cm::SnapshotPolicyShow>(dir, api::PathParams { path: path.clone() })
            .await?;
    let orphans = if shown.verdicts.is_none() && shown.root.auto_snapshots > 0 {
        let ino = shown.root.ino;
        control::call::<cm::SnapshotList>(
            dir,
            api::SnapshotListParams {
                path: None,
                sizes: false,
            },
        )
        .await?
        .snapshots
        .into_iter()
        .filter(|s| s.origin == "auto" && s.policy_ino == ino)
        .collect()
    } else {
        Vec::new()
    };
    print!("{}", render_shown(&shown, &orphans));
    Ok(())
}

/// `snapshot policy ls [<fs[:path]>]`.
pub async fn ls(dir: &Path, under_dir: Option<String>) -> Result<()> {
    let listing = control::call::<cm::SnapshotPolicyList>(dir, Default::default()).await?;
    let under_dir = under_dir.map(|p| normalize_path(&p)).filter(|p| p != "/");
    print!("{}", render_roots(&listing.roots, under_dir.as_deref()));
    Ok(())
}

/// `snapshot policy rm <fs:path> [--expire] [--yes]`.
pub async fn rm(dir: &Path, path: String, expire: bool, yes: bool) -> Result<()> {
    if expire {
        // The daemon refuses it too (`REMOVE_EXPIRE_REFUSED`); saying so
        // before the prompt spares a confirmation that cannot be acted on.
        bail!(
            "snapshot policy rm --expire: not available until expiry ships (plan 32 M4); \
             without --expire the policy is removed and its auto snapshots are kept, orphaned"
        );
    }
    let shown =
        control::call::<cm::SnapshotPolicyShow>(dir, api::PathParams { path: path.clone() })
            .await?;
    let root = &shown.root;
    if root.expr.is_empty() {
        bail!("{}: no snapshot policy", path_text(root));
    }
    if !yes {
        let question = format!(
            "remove the snapshot policy of {} ({})? its {} auto snapshot(s) are kept, orphaned",
            path_text(root),
            policy_text(root),
            root.auto_snapshots
        );
        if !confirm(&question)? {
            bail!("nothing removed");
        }
    }
    let now = control::call::<cm::SnapshotPolicyRemove>(
        dir,
        api::SnapPolicyRemoveParams {
            path,
            expire: false,
            confirm_expiring: None,
        },
    )
    .await?;
    println!(
        "{}: snapshot policy removed; {} auto snapshot(s) kept{}",
        path_text(&now),
        now.auto_snapshots,
        if now.orphaned { " (orphaned)" } else { "" }
    );
    Ok(())
}

/// `snapshot policy pause|resume <fs:path>`.
pub async fn pause(dir: &Path, path: String, paused: bool) -> Result<()> {
    let root =
        control::call::<cm::SnapshotPolicyPause>(dir, api::SnapPolicyPauseParams { path, paused })
            .await?;
    println!(
        "{}: {} ({})",
        path_text(&root),
        state(&root),
        policy_text(&root)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(ino: u64, path: Option<&str>, expr: &str) -> api::SnapPolicyRoot {
        api::SnapPolicyRoot {
            ino,
            path: path.map(Into::into),
            expr: expr.into(),
            canonical: (!expr.is_empty()).then(|| expr.to_string()),
            ..Default::default()
        }
    }

    fn delta(would_expire: u32) -> api::SnapPolicyDelta {
        api::SnapPolicyDelta {
            path: "/projects".into(),
            ino: 42,
            canonical: "5m:1d 1h:7d".into(),
            previous: Some("1h:30d".into()),
            creates_every: Some("5m".into()),
            would_expire,
            would_expire_ids: (0..would_expire).map(|i| format!("snap-{i}")).collect(),
            grace_note: "grace ends in 24h".into(),
            warnings: vec![],
            written: false,
        }
    }

    #[test]
    fn the_delta_sentence_is_step_5s() {
        assert_eq!(
            delta_sentence(&delta(312)),
            "creates every 5m; will expire 312 snapshots; grace ends in 24h"
        );
        assert_eq!(
            delta_sentence(&delta(1)),
            "creates every 5m; will expire 1 snapshot; grace ends in 24h"
        );
        let mut paused = delta(0);
        paused.creates_every = None;
        assert_eq!(
            delta_sentence(&paused),
            "creates nothing (paused); will expire nothing; grace ends in 24h"
        );
    }

    #[test]
    fn the_delta_shows_the_change_ids_and_warnings() {
        let mut d = delta(12);
        d.warnings = vec!["sub-minute tier".into()];
        let text = render_delta(&d);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "/projects (ino 42): 1h:30d -> 5m:1d 1h:7d");
        assert!(lines[1].starts_with("creates every 5m; will expire 12"));
        assert_eq!(lines[2], "  would expire snap-0");
        assert_eq!(lines[11], "  would expire snap-9");
        assert_eq!(lines[12], "  ... and 2 more");
        assert_eq!(lines[13], "warning: sub-minute tier");
        let mut fresh = delta(0);
        fresh.previous = None;
        assert!(
            render_delta(&fresh).starts_with("/projects (ino 42): (no policy) -> 5m:1d 1h:7d\n")
        );
        fresh.previous = Some(fresh.canonical.clone());
        assert!(render_delta(&fresh).contains(": 5m:1d 1h:7d (unchanged)\n"));
    }

    #[test]
    fn the_roots_table_has_every_state() {
        let armed = root(10, Some("/projects"), "5m:1d");
        let mut paused = root(11, Some("/projects/web"), "1h:7d; paused");
        paused.paused = true;
        let mut bad = root(12, Some("/scratchy"), "1h:1d 7m:1d");
        bad.canonical = None;
        bad.error = Some(api::PolicyErrorInfo {
            offset: 6,
            message: "not a multiple".into(),
        });
        bad.auto_snapshots = 2;
        bad.orphaned = true;
        let mut gone = root(13, None, "");
        gone.auto_snapshots = 7;
        gone.orphaned = true;
        let roots = vec![armed, paused, bad, gone];
        let table = render_roots(&roots, None);
        let lines: Vec<Vec<&str>> = table
            .lines()
            .map(|l| {
                l.split("  ")
                    .filter(|c| !c.is_empty())
                    .map(str::trim)
                    .collect()
            })
            .collect();
        assert_eq!(
            lines[0],
            ["PATH", "INO", "POLICY", "STATE", "AUTO SNAPSHOTS"]
        );
        assert_eq!(lines[1], ["/projects", "10", "5m:1d", "armed", "0"]);
        assert_eq!(
            lines[2],
            ["/projects/web", "11", "1h:7d; paused", "paused", "0"]
        );
        assert_eq!(
            lines[3],
            ["/scratchy", "12", "1h:1d 7m:1d", "unparseable", "2"]
        );
        assert_eq!(lines[4], ["(unlinked)", "13", "-", "orphaned", "7"]);
        // Filtering by directory is component-aware and drops unlinked
        // streams.
        let under_projects = render_roots(&roots, Some("/projects"));
        assert_eq!(under_projects.lines().count(), 3, "{under_projects}");
        assert_eq!(render_roots(&[], None), "no snapshot policies\n");
        assert_eq!(
            render_roots(&roots, Some("/proj")),
            "no snapshot policies\n"
        );
    }

    fn verdict(name: &str, origin: &str, policy_ino: u64, keep: bool) -> api::SnapVerdict {
        api::SnapVerdict {
            id: format!("id-{name}"),
            path: "/projects".into(),
            name: name.into(),
            created_unix_ms: 1_790_603_759_000,
            origin: origin.into(),
            policy_ino,
            keep,
            reasons: if keep {
                vec![
                    api::SnapReason {
                        kind: "tier".into(),
                        every: Some("5m".into()),
                        ..Default::default()
                    },
                    api::SnapReason {
                        kind: "tier".into(),
                        every: Some("1h".into()),
                        ..Default::default()
                    },
                ]
            } else {
                vec![]
            },
            ..Default::default()
        }
    }

    #[test]
    fn show_lists_the_roots_own_auto_snapshots_with_the_daemons_verdicts() {
        let shown = api::SnapPolicyShown {
            root: api::SnapPolicyRoot {
                auto_snapshots: 2,
                expr: "5m:1d   1h:7d".into(),
                canonical: Some("5m:1d 1h:7d".into()),
                ..root(42, Some("/projects"), "")
            },
            verdicts: Some(api::SnapPolicyAgainst {
                path: "/projects".into(),
                policy_ino: 42,
                snapshots: 4,
                would_expire: 1,
                verdicts: vec![
                    verdict("auto-a", "auto", 42, true),
                    verdict("auto-b", "auto", 42, false),
                    verdict("monday", "manual", 0, true),
                    verdict("auto-other", "auto", 7, true),
                ],
            }),
        };
        let text = render_shown(&shown, &[]);
        assert!(text.starts_with("/projects (ino 42)\n  policy:  5m:1d 1h:7d\n  stored:  5m:1d   1h:7d\n  state:   armed\n"), "{text}");
        assert!(text.contains("would expire 1 of 4 snapshots\n"), "{text}");
        let mut paused = shown.clone();
        paused.root.paused = true;
        let text = render_shown(&paused, &[]);
        assert!(
            text.contains("would expire 1 of 4 snapshots (paused)\n"),
            "{text}"
        );
        assert!(
            text.contains("/projects@auto-a") && text.contains("5m·1h"),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|l| l.contains("/projects@auto-b") && l.ends_with("expire")),
            "{text}"
        );
        assert!(
            !text.contains("monday") && !text.contains("auto-other"),
            "{text}"
        );
    }

    #[test]
    fn show_of_an_unparseable_root_carets_and_lists_orphans() {
        let mut r = root(42, Some("/projects"), "1h:1d 7m:1d");
        r.canonical = None;
        r.error = Some(api::PolicyErrorInfo {
            offset: 6,
            message: "boom".into(),
        });
        r.auto_snapshots = 1;
        r.orphaned = true;
        let orphan = api::SnapshotStatus {
            path: "/projects".into(),
            name: "auto-20260928T1400Z".into(),
            origin: "auto".into(),
            policy_ino: 42,
            created_unix_ms: 1_790_603_759_000,
            ..Default::default()
        };
        let text = render_shown(
            &api::SnapPolicyShown {
                root: r,
                verdicts: None,
            },
            &[orphan],
        );
        assert!(text.contains("  state:   unparseable\n"), "{text}");
        assert!(text.contains("  1h:1d 7m:1d\n        ^ boom"), "{text}");
        assert!(
            text.lines()
                .any(|l| l.contains("@auto-20260928T1400Z") && l.ends_with("orphaned")),
            "{text}"
        );
    }

    #[test]
    fn a_refused_expression_becomes_a_caret_and_others_do_not() {
        let e = ControlError::invalid("invalid policy").with_details(serde_json::json!({
            "offset": 6, "message": "not a multiple",
        }));
        let p = parse_error_of(&e).unwrap();
        assert_eq!((p.offset, p.msg.as_str()), (6, "not a multiple"));
        assert!(parse_error_of(&ControlError::invalid("/f: not a directory")).is_none());
        let conflict =
            ControlError::new(ErrorKind::Conflict, "x").with_details(serde_json::json!({
                "offset": 1, "message": "m",
            }));
        assert!(parse_error_of(&conflict).is_none());
    }

    #[test]
    fn a_stale_preview_is_reported_with_the_new_count() {
        let preview = delta(3);
        let stale = ControlError::new(ErrorKind::Conflict, "snapshots changed")
            .with_details(serde_json::to_value(delta(5)).unwrap());
        let text = conflict_message(&preview, &stale);
        assert!(
            text.contains("would now expire 5 snapshot(s), not the 3 previewed")
                && text.contains("rerun"),
            "{text}"
        );
        // The same count, no details, or details that are not a delta: the
        // daemon's message, still a failure.
        let same = ControlError::new(ErrorKind::Conflict, "snapshots changed")
            .with_details(serde_json::to_value(delta(3)).unwrap());
        for e in [
            same,
            ControlError::new(ErrorKind::Conflict, "snapshots changed"),
            ControlError::new(ErrorKind::Conflict, "snapshots changed")
                .with_details(serde_json::json!({"nope": 1})),
        ] {
            let text = conflict_message(&preview, &e);
            assert!(
                text.starts_with("nothing written: snapshots changed")
                    && !text.contains("would now expire"),
                "{text}"
            );
        }
    }

    #[test]
    fn paths_normalize_like_the_servers() {
        assert_eq!(normalize_path("/a//b/"), "/a/b");
        assert_eq!(normalize_path("a/b"), "/a/b");
        assert_eq!(normalize_path(""), "/");
        assert_eq!(normalize_path("//"), "/");
    }
}
