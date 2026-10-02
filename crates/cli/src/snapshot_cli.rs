//! `constellation snapshot ls|delete|hold|release` (plan 32 Step 5): the
//! snapshot table and the multi-selector commands.
//!
//! The table is rendered here from `snapshot.list`'s rows, by pure
//! functions the tests drive directly. Its columns are Step 5's; the ones
//! whose numbers come from later milestones print `-` until then: `USED`
//! and `WRITTEN` need the accounting index (M5), `EXPIRES` needs expiry
//! (M4), which is also when `KEPT BY` learns retention reasons. `REFER`
//! already has a number for most snapshots — the subtree's logical size
//! the replica measured when the snapshot was taken (plan 32 §0.4's
//! `refer_bytes`) — and prints it with a `≈`, because it is a creation-time
//! measurement and not yet the accounting index's figure. `-p` drops the
//! `≈` and every unit: exact integers, as ZFS's `-p`.
//!
//! Times print in UTC (`CREATED (UTC)`), computed here from the Unix
//! epoch: no timezone database yet. The policy chunks bring one, and with
//! it the option of the policy's own `tz`.
//!
//! `delete`, `hold` and `release` take any number of selectors
//! (`path@name`, `path@a%b`, `path@prefix*`, or a bare id), resolved by the
//! daemon (`snapshot.resolve`) so the CLI and every other client agree on
//! what a selector names.

use crate::control;
use anyhow::{bail, Result};
use constellation_control::methods as cm;
use constellation_control::proto::types as api;
use std::cmp::Ordering;
use std::path::Path;

/// One column of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    Name,
    Created,
    Origin,
    Used,
    Written,
    Refer,
    KeptBy,
    Expires,
    Id,
    Seq,
    Creator,
    Policy,
}

/// What `snapshot ls` shows without `-o`.
pub const DEFAULT_COLUMNS: &[Column] = &[
    Column::Name,
    Column::Created,
    Column::Origin,
    Column::Used,
    Column::Written,
    Column::Refer,
    Column::KeptBy,
    Column::Expires,
];

impl Column {
    const ALL: [Column; 12] = [
        Column::Name,
        Column::Created,
        Column::Origin,
        Column::Used,
        Column::Written,
        Column::Refer,
        Column::KeptBy,
        Column::Expires,
        Column::Id,
        Column::Seq,
        Column::Creator,
        Column::Policy,
    ];

    /// The `-o`/`-s` spelling.
    fn key(self) -> &'static str {
        match self {
            Column::Name => "name",
            Column::Created => "created",
            Column::Origin => "origin",
            Column::Used => "used",
            Column::Written => "written",
            Column::Refer => "refer",
            Column::KeptBy => "kept-by",
            Column::Expires => "expires",
            Column::Id => "id",
            Column::Seq => "seq",
            Column::Creator => "creator",
            Column::Policy => "policy",
        }
    }

    fn header(self) -> &'static str {
        match self {
            Column::Name => "NAME",
            Column::Created => "CREATED (UTC)",
            Column::Origin => "ORIGIN",
            Column::Used => "USED",
            Column::Written => "WRITTEN",
            Column::Refer => "REFER",
            Column::KeptBy => "KEPT BY",
            Column::Expires => "EXPIRES",
            Column::Id => "ID",
            Column::Seq => "SEQ",
            Column::Creator => "CREATOR",
            Column::Policy => "POLICY",
        }
    }

    pub fn parse(raw: &str) -> Result<Column> {
        let key = raw.trim().to_ascii_lowercase().replace('_', "-");
        let key = if key == "keptby" {
            "kept-by".into()
        } else {
            key
        };
        Column::ALL
            .into_iter()
            .find(|c| c.key() == key)
            .ok_or_else(|| {
                let known: Vec<&str> = Column::ALL.iter().map(|c| c.key()).collect();
                anyhow::anyhow!("unknown column {raw:?}; known: {}", known.join(", "))
            })
    }

    /// `-o name,created,id`.
    pub fn parse_list(raw: &str) -> Result<Vec<Column>> {
        let columns = raw
            .split(',')
            .filter(|c| !c.trim().is_empty())
            .map(Column::parse)
            .collect::<Result<Vec<_>>>()?;
        if columns.is_empty() {
            bail!("-o needs at least one column");
        }
        Ok(columns)
    }

    /// `-s`'s order for this column: numbers as numbers, absent values
    /// first.
    fn compare(self, a: &api::SnapshotStatus, b: &api::SnapshotStatus) -> Ordering {
        match self {
            Column::Created => a.created_unix_ms.cmp(&b.created_unix_ms),
            Column::Refer => a.refer_bytes.cmp(&b.refer_bytes),
            Column::Seq => a.seq.cmp(&b.seq),
            Column::Creator => a.creator.cmp(&b.creator),
            Column::Policy => a.policy_ino.cmp(&b.policy_ino),
            // Nothing to order by until M4/M5 fill them in.
            Column::Used | Column::Written | Column::Expires => Ordering::Equal,
            _ => self.cell(a, false).cmp(&self.cell(b, false)),
        }
    }

    fn cell(self, row: &api::SnapshotStatus, parsable: bool) -> String {
        match self {
            Column::Name => format!("{}@{}", row.path, row.name),
            Column::Created if parsable => row.created_unix_ms.to_string(),
            Column::Created => {
                let when = utc_minutes(row.created_unix_ms);
                // Plan 32 Step 5's flag on a held row.
                if row.held {
                    format!("{when} ⚑")
                } else {
                    when
                }
            }
            Column::Origin => row.origin.clone(),
            Column::Used | Column::Written => "-".into(),
            Column::Refer => match row.refer_bytes {
                Some(bytes) if parsable => bytes.to_string(),
                Some(bytes) => format!("≈{}", human_bytes(bytes)),
                None => "-".into(),
            },
            Column::KeptBy => kept_by(row),
            // Manual and held snapshots never expire; an automatic one's
            // expiry is M4's.
            Column::Expires if row.held || row.origin == "manual" => "never".into(),
            Column::Expires => "-".into(),
            Column::Id => row.id.clone(),
            Column::Seq => row.seq.map_or_else(|| "-".into(), |s| s.to_string()),
            Column::Creator if row.creator == 0 => "-".into(),
            Column::Creator => row.creator.to_string(),
            Column::Policy if row.policy_ino == 0 => "-".into(),
            Column::Policy => row.policy_ino.to_string(),
        }
    }
}

/// `KEPT BY`: the hold, by its owner's namespace (`held: csi` reads
/// differently from an operator's `held: user` and from a plain `held`),
/// `—` when nothing keeps the snapshot. Retention reasons join in M4.
fn kept_by(row: &api::SnapshotStatus) -> String {
    if !row.held {
        return "—".into();
    }
    match row.held_by.as_deref().filter(|by| !by.is_empty()) {
        None => "held".into(),
        Some(by) => format!("held: {}", by.split(':').next().unwrap_or(by)),
    }
}

/// `YYYY-MM-DD HH:MM` in UTC.
pub fn utc_minutes(unix_ms: i64) -> String {
    let secs = unix_ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60
    )
}

/// Days since 1970-01-01 → proleptic Gregorian `(year, month, day)`
/// (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// Binary units with one decimal below 10 (`4.1G`, `210M`, `512`).
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["K", "M", "G", "T", "P", "E"];
    if bytes < 1024 {
        return bytes.to_string();
    }
    let mut value = bytes as f64;
    let mut unit = "";
    for u in UNITS {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = u;
    }
    if value < 10.0 {
        format!("{value:.1}{unit}")
    } else {
        format!("{value:.0}{unit}")
    }
}

/// Whether a snapshot recorded under `path` is `dir` or below it,
/// component-aware (`/project-old` is not under `/project`).
pub fn under(path: &str, dir: &str) -> bool {
    let dir = dir.trim_end_matches('/');
    dir.is_empty()
        || path == dir
        || path
            .strip_prefix(dir)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Which snapshots `--auto`/`--manual` keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginFilter {
    All,
    Auto,
    Manual,
}

impl OriginFilter {
    pub fn keep(self, row: &api::SnapshotStatus) -> bool {
        match self {
            OriginFilter::All => true,
            OriginFilter::Auto => row.origin == "auto",
            OriginFilter::Manual => row.origin == "manual",
        }
    }
}

/// The table, or `no snapshots`. Rows are in chain order per path (path,
/// then commit seq, then creation time) unless `sort` names a column; a
/// sort is stable, so ties keep that order.
pub fn render_table(
    rows: &[api::SnapshotStatus],
    columns: &[Column],
    sort: Option<Column>,
    parsable: bool,
) -> String {
    if rows.is_empty() {
        return "no snapshots\n".into();
    }
    let mut rows: Vec<&api::SnapshotStatus> = rows.iter().collect();
    rows.sort_by(|a, b| {
        (a.path.as_str(), a.seq, a.created_unix_ms, a.name.as_str()).cmp(&(
            b.path.as_str(),
            b.seq,
            b.created_unix_ms,
            b.name.as_str(),
        ))
    });
    if let Some(column) = sort {
        rows.sort_by(|a, b| column.compare(a, b));
    }
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| columns.iter().map(|c| c.cell(row, parsable)).collect())
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            cells
                .iter()
                .map(|r| r[i].chars().count())
                .chain([c.header().len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    let mut line = |values: Vec<&str>| {
        let last = values.len() - 1;
        let mut text = String::new();
        for (i, value) in values.into_iter().enumerate() {
            if i == last {
                text.push_str(value);
            } else {
                // `{:<w}` pads by `char`s, which is what `⚑`/`—` need.
                text.push_str(&format!("{value:<w$}  ", w = widths[i]));
            }
        }
        out.push_str(text.trim_end());
        out.push('\n');
    };
    line(columns.iter().map(|c| c.header()).collect());
    for row in &cells {
        line(row.iter().map(String::as_str).collect());
    }
    out
}

/// The confirmation `snapshot delete` asks for before deleting more than
/// one snapshot. Anything but `y`/`yes` (EOF included) declines.
fn confirm(question: &str) -> Result<bool> {
    use std::io::Write;
    let mut err = std::io::stderr().lock();
    write!(err, "{question} [y/N] ")?;
    err.flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn label(row: &api::SnapshotStatus) -> String {
    format!("{}@{}", row.path, row.name)
}

/// `snapshot delete <sel>... [--dry-run] [--yes] [--force]`.
pub async fn delete(
    dir: &Path,
    selectors: Vec<String>,
    dry_run: bool,
    yes: bool,
    force: bool,
) -> Result<()> {
    if dry_run {
        let out = control::call::<cm::SnapshotDeleteMany>(
            dir,
            api::SnapshotDeleteManyParams {
                selectors,
                dry_run: true,
                force,
            },
        )
        .await?;
        for row in &out.resolved {
            match out.refused.iter().find(|r| r.id == row.id) {
                Some(refusal) => println!("would refuse: {}", refusal.reason),
                None => println!("would delete {} ({})", label(row), row.id),
            }
        }
        if let Some(reclaim) = &out.reclaim {
            println!(
                "would reclaim ≈ {} in {} chunks (after GC){}",
                human_bytes(reclaim.bytes),
                reclaim.chunks,
                if reclaim.building {
                    "; the accounting index is still building"
                } else {
                    ""
                }
            );
        }
        return Ok(());
    }
    let resolved =
        control::call::<cm::SnapshotResolve>(dir, api::SnapshotResolveParams { selectors })
            .await?
            .snapshots;
    if resolved.len() > 1 && !yes {
        for row in &resolved {
            eprintln!("  {}", label(row));
        }
        if !confirm(&format!("delete {} snapshots?", resolved.len()))? {
            bail!("nothing deleted");
        }
    }
    // Delete exactly what was confirmed: the ids, not the selectors again
    // (a glob re-resolved now could name a snapshot taken meanwhile).
    let out = control::call::<cm::SnapshotDeleteMany>(
        dir,
        api::SnapshotDeleteManyParams {
            selectors: resolved.iter().map(|r| r.id.clone()).collect(),
            dry_run: false,
            force,
        },
    )
    .await?;
    let name_of = |id: &str| {
        out.resolved
            .iter()
            .find(|r| r.id == id)
            .map_or_else(|| id.to_string(), label)
    };
    for id in &out.deleted {
        println!("deleted snapshot {}", name_of(id));
    }
    // Every refusal names its snapshot.
    for refusal in &out.refused {
        eprintln!("{}", refusal.reason);
    }
    match out.refused.len() {
        0 => Ok(()),
        n => bail!("{n} of {} snapshots not deleted", resolved.len()),
    }
}

/// `snapshot hold|release <sel>... [--by <owner>] [--force]`: resolve,
/// then one `snapshot.hold` per snapshot (its owner rule applies to each).
/// Every snapshot is tried; any failure fails the command after the rest.
pub async fn hold(
    dir: &Path,
    selectors: Vec<String>,
    held: bool,
    by: Option<String>,
    force: bool,
) -> Result<()> {
    let resolved =
        control::call::<cm::SnapshotResolve>(dir, api::SnapshotResolveParams { selectors })
            .await?
            .snapshots;
    let mut failed = 0;
    for row in &resolved {
        let result = control::call::<cm::SnapshotHold>(
            dir,
            api::SnapshotHoldParams {
                id: row.id.clone(),
                held,
                by: by.clone(),
                force,
            },
        )
        .await;
        match result {
            Ok(h) => println!("{}", h.detail),
            Err(error) => {
                failed += 1;
                eprintln!("snapshot {}: {error:#}", label(row));
            }
        }
    }
    match failed {
        0 => Ok(()),
        n => bail!(
            "{n} of {} snapshots not {}",
            resolved.len(),
            if held { "held" } else { "released" }
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(path: &str, name: &str, seq: u64, created_unix_ms: i64) -> api::SnapshotStatus {
        api::SnapshotStatus {
            id: format!("id-{name}"),
            path: path.into(),
            name: name.into(),
            root_hash: format!("mtree:{seq}:00:9"),
            created_unix_ms,
            origin: "manual".into(),
            seq: Some(seq),
            ..Default::default()
        }
    }

    #[test]
    fn utc_times_and_sizes() {
        assert_eq!(utc_minutes(0), "1970-01-01 00:00");
        // 2026-09-28 13:55:59 UTC.
        assert_eq!(utc_minutes(1_790_603_759_000), "2026-09-28 13:55");
        assert_eq!(utc_minutes(951_782_400_000), "2000-02-29 00:00");
        assert_eq!(utc_minutes(-60_000), "1969-12-31 23:59");
        assert_eq!(human_bytes(512), "512");
        assert_eq!(human_bytes(4096), "4.0K");
        assert_eq!(human_bytes(220_200_960), "210M");
        assert_eq!(human_bytes(4_402_341_478), "4.1G");
    }

    #[test]
    fn the_default_table_has_step_5s_columns_and_kept_by() {
        let mut held_csi = row("/projects", "pvc-a1", 3, 1_790_603_759_000);
        held_csi.held = true;
        held_csi.held_by = Some("csi:content-uid".into());
        held_csi.refer_bytes = Some(4_402_341_478);
        let mut held_user = row("/projects", "keep", 2, 1_790_603_700_000);
        held_user.held = true;
        held_user.held_by = Some("user:attila".into());
        let mut held_plain = row("/projects", "plain", 4, 1_790_603_800_000);
        held_plain.held = true;
        let mut auto = row("/projects", "auto-1", 1, 1_790_600_000_000);
        auto.origin = "auto".into();
        let rows = vec![held_csi, held_user, held_plain, auto];
        let table = render_table(&rows, DEFAULT_COLUMNS, None, false);
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(
            lines[0]
                .split("  ")
                .filter(|c| !c.is_empty())
                .map(str::trim)
                .collect::<Vec<_>>(),
            [
                "NAME",
                "CREATED (UTC)",
                "ORIGIN",
                "USED",
                "WRITTEN",
                "REFER",
                "KEPT BY",
                "EXPIRES"
            ]
        );
        // Chain order (seq), not the server's name order.
        assert!(lines[1].starts_with("/projects@auto-1 "), "{table}");
        assert!(
            lines[1].contains("—") && lines[1].trim_end().ends_with('-'),
            "{table}"
        );
        assert!(
            lines[2].contains("⚑") && lines[2].contains("held: user"),
            "{table}"
        );
        assert!(
            lines[3].contains("≈4.1G") && lines[3].contains("held: csi"),
            "{table}"
        );
        assert!(
            lines[4].contains("held ") && lines[4].ends_with("never"),
            "{table}"
        );
        // Columns line up although `⚑` and `—` are multi-byte.
        let origin_col = lines[0].find("ORIGIN").unwrap();
        for line in &lines[1..] {
            let chars: Vec<char> = line.chars().collect();
            assert!(
                matches!(chars[origin_col], 'm' | 'a'),
                "ORIGIN misaligned in {line:?}"
            );
        }
    }

    #[test]
    fn columns_sort_and_parsable() {
        let mut a = row("/v", "a", 2, 2000);
        a.refer_bytes = Some(10 << 20);
        let mut b = row("/v", "b", 1, 1000);
        b.refer_bytes = Some(1 << 20);
        b.creator = 7;
        let columns = Column::parse_list("name,refer,seq,id,creator,policy").unwrap();
        let table = render_table(&[a.clone(), b.clone()], &columns, Some(Column::Refer), true);
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(
            lines[0].split_whitespace().collect::<Vec<_>>(),
            ["NAME", "REFER", "SEQ", "ID", "CREATOR", "POLICY"]
        );
        assert_eq!(
            lines[1].split_whitespace().collect::<Vec<_>>(),
            ["/v@b", "1048576", "1", "id-b", "7", "-"]
        );
        assert_eq!(
            lines[2].split_whitespace().collect::<Vec<_>>(),
            ["/v@a", "10485760", "2", "id-a", "-", "-"]
        );
        let created = render_table(&[a, b], &[Column::Created], None, true);
        assert_eq!(created.lines().nth(1), Some("1000"));
        assert!(Column::parse("kept_by").is_ok() && Column::parse("KEPT-BY").is_ok());
        assert!(Column::parse_list("name,size").is_err());
        assert_eq!(
            render_table(&[], DEFAULT_COLUMNS, None, false),
            "no snapshots\n"
        );
        assert!(under("/a", "/") && under("/a/b", "/a") && under("/a", "/a/"));
        assert!(!under("/ab", "/a") && !under("/", "/a"));
    }
}
