//! `constellation snapshot ls|space|delete|hold|release` (plan 32 Step 5):
//! the snapshot table, the space breakdown and the multi-selector
//! commands.
//!
//! The table is rendered here from `snapshot.list`'s rows, by pure
//! functions the tests drive directly. `USED`, `WRITTEN`, `REFER` and the
//! extra `LSIZE` are the accounting index's (plan 32 §6.1, logical bytes,
//! deduplicated), which `snapshot ls` asks for (`sizes: true`) whenever it
//! shows one of them; a footer says which commit they are as of. While the
//! index is being built the cells read `building (37%)`, with accounting
//! off they read `-` — never `0`, which would be a real (and wrong) answer.
//! `REFER` then falls back to `≈` the subtree size the replica measured
//! when the snapshot was taken (plan 32 §0.4's `refer_bytes`), the one
//! size that needs no index. `EXPIRES` waits for expiry (M4), which is
//! also when `KEPT BY` learns retention reasons. `-p` drops every unit and
//! `≈`: exact integers, as ZFS's `-p`, and `-` for "no number".
//!
//! `snapshot space` prints Step 5's breakdown from `snapshot.space`; every
//! figure in it is the daemon's, the CLI only lays it out.
//!
//! An auto snapshot whose `policy_ino` carries no parseable policy reads
//! `auto (orphaned)` in `ORIGIN` (plan 32 Step 4.2: never expired
//! automatically). Which streams are orphaned is the daemon's answer
//! (`snapshot.policy.list`), not something the CLI works out.
//!
//! Times print in UTC (`CREATED (UTC)`), computed here from the Unix
//! epoch: no timezone database yet. The policy chunks bring one, and with
//! it the option of the policy's own `tz`.
//!
//! `delete`, `hold` and `release` take any number of selectors
//! (`path@name`, `path@a%b`, `path@prefix*`, or a bare id), resolved by the
//! daemon (`snapshot.resolve`) so the CLI and every other client agree on
//! what a selector names. A `delete --dry-run`, and the confirmation a
//! multi-snapshot delete asks for, show what the delete would give back
//! (`would reclaim ≈ X in N chunks (after GC)`).

use crate::control;
use anyhow::{bail, Result};
use constellation_control::methods as cm;
use constellation_control::proto::types as api;
use std::cmp::Ordering;
use std::collections::BTreeSet;
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
    Lsize,
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
    const ALL: [Column; 13] = [
        Column::Name,
        Column::Created,
        Column::Origin,
        Column::Used,
        Column::Written,
        Column::Refer,
        Column::KeptBy,
        Column::Expires,
        Column::Lsize,
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
            Column::Lsize => "lsize",
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
            Column::Lsize => "LSIZE",
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

    /// Whether the column is one of the accounting index's sizes (the
    /// ones `snapshot ls` asks for and the footer dates).
    pub fn is_size(self) -> bool {
        matches!(
            self,
            Column::Used | Column::Written | Column::Refer | Column::Lsize
        )
    }

    /// The accounting index's figure for this column, if the row has it.
    fn size(self, row: &api::SnapshotStatus) -> Option<u64> {
        if row.size_state != Some(api::SizeState::Ok) {
            return None;
        }
        match self {
            Column::Used => row.used,
            Column::Written => row.written,
            Column::Refer => row.refer,
            Column::Lsize => row.lsize,
            _ => None,
        }
    }

    /// `-s`'s order for this column: numbers as numbers, absent values
    /// first. `REFER` without the index's figure orders by the
    /// creation-time `refer_bytes` it then shows.
    fn compare(self, a: &api::SnapshotStatus, b: &api::SnapshotStatus, ctx: &Ctx) -> Ordering {
        match self {
            Column::Created => a.created_unix_ms.cmp(&b.created_unix_ms),
            Column::Refer => {
                let refer = |r: &api::SnapshotStatus| match self.size(r) {
                    Some(bytes) => (1, bytes),
                    None => (0, r.refer_bytes.unwrap_or(0)),
                };
                refer(a).cmp(&refer(b))
            }
            Column::Used | Column::Written | Column::Lsize => self.size(a).cmp(&self.size(b)),
            Column::Seq => a.seq.cmp(&b.seq),
            Column::Creator => a.creator.cmp(&b.creator),
            Column::Policy => a.policy_ino.cmp(&b.policy_ino),
            // Nothing to order by until M4 fills it in.
            Column::Expires => Ordering::Equal,
            _ => self.cell(a, ctx).cmp(&self.cell(b, ctx)),
        }
    }

    /// A size cell: the number, `building (37%)` (`-` with `-p`), or `-`
    /// (accounting off, or not asked for), where `REFER` shows `≈` the
    /// creation-time measurement instead.
    fn size_cell(self, row: &api::SnapshotStatus, parsable: bool) -> String {
        if let Some(bytes) = self.size(row) {
            return if parsable {
                bytes.to_string()
            } else {
                human_bytes(bytes)
            };
        }
        if row.size_state == Some(api::SizeState::Building) {
            return if parsable {
                "-".into()
            } else {
                format!("building ({}%)", row.building_pct.unwrap_or(0))
            };
        }
        match (self, row.refer_bytes) {
            (Column::Refer, Some(bytes)) if parsable => bytes.to_string(),
            (Column::Refer, Some(bytes)) => format!("≈{}", human_bytes(bytes)),
            _ => "-".into(),
        }
    }

    fn cell(self, row: &api::SnapshotStatus, ctx: &Ctx) -> String {
        let parsable = ctx.parsable;
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
            // Step 4.2: an auto snapshot no parseable policy owns is never
            // expired automatically, and says so.
            Column::Origin if is_orphaned(row, ctx.orphaned) => "auto (orphaned)".into(),
            Column::Origin => row.origin.clone(),
            Column::Used | Column::Written | Column::Refer | Column::Lsize => {
                self.size_cell(row, parsable)
            }
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

/// What a cell needs besides its row.
struct Ctx<'a> {
    parsable: bool,
    orphaned: &'a BTreeSet<u64>,
}

/// Whether `snapshot ls` needs the orphan set: `--orphaned` filters on it
/// and the `ORIGIN` column (shown or sorted on) labels with it.
pub fn wants_orphans(
    filter: OriginFilter,
    columns: &[Column],
    sort: Option<Column>,
    json: bool,
) -> bool {
    filter == OriginFilter::Orphaned
        || (!json && (columns.contains(&Column::Origin) || sort == Some(Column::Origin)))
}

/// The orphan set from the `snapshot.policy.list` answer. Only the
/// `--orphaned` filter depends on it: a failed fetch there is the error;
/// elsewhere it is a warning and the listing degrades to plain `auto`.
pub fn orphans_or_degrade(
    filter: OriginFilter,
    fetched: Result<api::SnapPolicyListing>,
) -> Result<(BTreeSet<u64>, Option<String>)> {
    match fetched {
        Ok(listing) => Ok((orphaned_streams(&listing), None)),
        Err(e) if filter == OriginFilter::Orphaned => Err(e),
        Err(e) => Ok((
            BTreeSet::new(),
            Some(format!(
                "warning: could not tell which auto snapshots are orphaned \
                 (snapshot.policy.list failed: {e:#}); showing plain `auto`"
            )),
        )),
    }
}

/// Whether `row` is an auto snapshot of one of the `orphaned` streams
/// (`policy_ino`s that `snapshot.policy.list` reports as orphaned).
pub fn is_orphaned(row: &api::SnapshotStatus, orphaned: &BTreeSet<u64>) -> bool {
    row.origin == "auto" && orphaned.contains(&row.policy_ino)
}

/// The `policy_ino`s `snapshot.policy.list` reports as orphaned: their
/// auto snapshots have no parseable policy.
pub fn orphaned_streams(listing: &api::SnapPolicyListing) -> BTreeSet<u64> {
    listing
        .roots
        .iter()
        .filter(|r| r.orphaned)
        .map(|r| r.ino)
        .collect()
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

/// `YYYY-MM-DD HH:MM:SS` in UTC (the scheduler's `s` tiers need the
/// seconds).
pub fn utc_seconds(unix_ms: i64) -> String {
    let secs = unix_ms.div_euclid(1000);
    format!("{}:{:02}", utc_minutes(unix_ms), secs.rem_euclid(60))
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

/// Which snapshots `--auto`/`--manual`/`--orphaned` keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginFilter {
    All,
    Auto,
    Manual,
    Orphaned,
}

impl OriginFilter {
    pub fn keep(self, row: &api::SnapshotStatus, orphaned: &BTreeSet<u64>) -> bool {
        match self {
            OriginFilter::All => true,
            OriginFilter::Auto => row.origin == "auto",
            OriginFilter::Manual => row.origin == "manual",
            OriginFilter::Orphaned => is_orphaned(row, orphaned),
        }
    }
}

/// The table, or `no snapshots`. Rows are in chain order per path (path,
/// then commit seq, then creation time) unless `sort` names a column; a
/// sort is stable, so ties keep that order. `orphaned` are the orphaned
/// streams' `policy_ino`s ([`orphaned_streams`]), whose auto snapshots
/// read `auto (orphaned)`.
pub fn render_table(
    rows: &[api::SnapshotStatus],
    columns: &[Column],
    sort: Option<Column>,
    parsable: bool,
    orphaned: &BTreeSet<u64>,
) -> String {
    let ctx = Ctx { parsable, orphaned };
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
        rows.sort_by(|a, b| column.compare(a, b, &ctx));
    }
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| columns.iter().map(|c| c.cell(row, &ctx)).collect())
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

/// The line under the table that dates its sizes (plan 32 Step 5):
/// `USED/WRITTEN/REFER as of commit 88213 (4s ago) · logical bytes,
/// pre-compression`, naming the size columns shown; or why there are no
/// sizes. `None` when no size column is shown or nothing was asked.
pub fn size_footer(
    rows: &[api::SnapshotStatus],
    columns: &[Column],
    now_ms: u64,
) -> Option<String> {
    let names: Vec<&str> = columns
        .iter()
        .filter(|c| c.is_size())
        .map(|c| c.header())
        .collect();
    if names.is_empty() || rows.is_empty() {
        return None;
    }
    let names = names.join("/");
    let state = |s: api::SizeState| rows.iter().filter(move |r| r.size_state == Some(s));
    if let Some(as_of_seq) = state(api::SizeState::Ok).filter_map(|r| r.as_of_seq).max() {
        let as_of_ms = state(api::SizeState::Ok)
            .filter_map(|r| r.as_of_ms)
            .max()
            .unwrap_or(0);
        return Some(format!(
            "{names} as of commit {as_of_seq} ({}) · logical bytes, pre-compression",
            ago(now_ms, as_of_ms)
        ));
    }
    if let Some(row) = state(api::SizeState::Building).next() {
        return Some(format!(
            "{names}: the accounting index is building ({}%); run again shortly",
            row.building_pct.unwrap_or(0)
        ));
    }
    if state(api::SizeState::Off).next().is_some() {
        return Some(format!(
            "{names}: snapshot accounting is off (CONSTELLATION_SNAPACCT=off)"
        ));
    }
    None
}

/// `4s ago`, `3m ago`, …; `time unknown` for a zero timestamp.
fn ago(now_ms: u64, then_ms: u64) -> String {
    if then_ms == 0 {
        return "time unknown".into();
    }
    let secs = now_ms.saturating_sub(then_ms) / 1000;
    let text = match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86_400 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86_400),
    };
    format!("{text} ago")
}

/// A configured duration in its largest exact unit (`7d`, `36h`, `90s`).
fn exact_duration(ms: u64) -> String {
    let secs = ms / 1000;
    for (unit, size) in [("d", 86_400), ("h", 3600), ("m", 60)] {
        if secs >= size && secs.is_multiple_of(size) {
            return format!("{}{unit}", secs / size);
        }
    }
    format!("{secs}s")
}

/// `would reclaim ≈ 41.3G in 812 chunks (after GC)`, or why there is no
/// estimate: still building, or none at all (accounting off, or the
/// estimate failed — the daemon logs why).
pub fn reclaim_line(reclaim: Option<&api::ReclaimEstimate>) -> String {
    let Some(reclaim) = reclaim else {
        return "would reclaim: no estimate (snapshot accounting is off or unavailable)".into();
    };
    if reclaim.building {
        return format!(
            "would reclaim: no estimate yet, the accounting index is building ({}%)",
            reclaim.building_pct
        );
    }
    format!(
        "would reclaim ≈ {} in {} chunks (after GC)",
        human_bytes(reclaim.bytes),
        reclaim.chunks
    )
}

/// `snapshot space`: plan 32 Step 5's breakdown, every figure the
/// daemon's.
pub fn render_space(b: &api::SpaceBreakdown, now_ms: u64) -> String {
    if b.building {
        return format!(
            "the accounting index is building ({}%); run again shortly\n",
            b.building_pct
        );
    }
    let mut out = String::new();
    if let Some(path) = &b.path {
        out.push_str(&format!(
            "space of {path}: snapshots of directories at or under it \
             (awaiting GC is filesystem-wide)\n"
        ));
    }
    let rows: [(&str, String, String); 6] = [
        (
            "live data (logical)",
            human_bytes(b.live_logical),
            String::new(),
        ),
        (
            "snapshots, total (usedbysnapshots)",
            human_bytes(b.snapshots_total.bytes),
            "← deleting every snapshot returns this".into(),
        ),
        (
            "  unique to one snapshot",
            human_bytes(b.unique.bytes),
            "← Σ USED; not the total (see note)".into(),
        ),
        (
            "  shared by ≥2 snapshots only",
            human_bytes(b.shared_snapshots_only.bytes),
            String::new(),
        ),
        (
            "shared between live and snapshots",
            human_bytes(b.shared_with_live.bytes),
            "(costs nothing extra)".into(),
        ),
        (
            "awaiting GC",
            human_bytes(b.awaiting_gc.bytes),
            format!(
                "(freed snapshots; horizon {})",
                exact_duration(b.gc_horizon_ms)
            ),
        ),
    ];
    let label_w = rows.iter().map(|r| r.0.chars().count()).max().unwrap_or(0);
    let value_w = rows.iter().map(|r| r.1.chars().count()).max().unwrap_or(0);
    for (label, value, note) in &rows {
        let line = format!("{label:<label_w$}  {value:<value_w$}  {note}");
        out.push_str(line.trim_end());
        out.push('\n');
    }
    match (b.physical_ratio, b.physical_estimate) {
        (Some(ratio), Some(physical)) => out.push_str(&format!(
            "estimated physical (×{ratio:.2} compression, from last GC round): \
             snapshots, total ≈ {}\n",
            human_bytes(physical)
        )),
        _ => out.push_str("estimated physical: - (no GC round has measured the bucket yet)\n"),
    }
    out.push_str(
        "note: per-snapshot USED values do not sum to the snapshots total: a chunk two or \
         more snapshots share is in no snapshot's USED (it becomes one snapshot's USED once \
         the others are deleted).\n",
    );
    if b.estimate_pending {
        out.push_str(
            "estimate: the live flags are pending a recheck at a settled moment; \
             \"shared with live\" and the reclaimable figures may be off until it runs.\n",
        );
    }
    out.push_str(&format!(
        "as of commit {} ({}) · logical bytes, pre-compression\n",
        b.as_of_seq,
        ago(now_ms, b.as_of_ms)
    ));
    out
}

/// `snapshot space [<fs:path>] [--verify] [--json]`. With `--verify` the
/// index is first checked against a full walk of every snapshot (which
/// also brings it current); any mismatch fails the command after printing
/// the breakdown and the differences.
pub async fn space(dir: &Path, path: Option<String>, verify: bool, json: bool) -> Result<()> {
    let verified = if verify {
        Some(
            control::call::<cm::SnapshotSpaceVerify>(dir, constellation_control::proto::Empty {})
                .await?,
        )
    } else {
        None
    };
    let breakdown =
        control::call::<cm::SnapshotSpace>(dir, api::SnapshotSpaceParams { path }).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "space": breakdown,
                "verify": verified,
            }))?
        );
    } else {
        print!("{}", render_space(&breakdown, now_ms()));
        if let Some(v) = &verified {
            print!("{}", render_verify(v));
        }
    }
    match verified {
        Some(v) if v.mismatches > 0 => bail!("{} accounting mismatches", v.mismatches),
        _ => Ok(()),
    }
}

/// `--verify`'s verdict: `0 mismatches` or the differences.
pub fn render_verify(v: &api::SpaceVerified) -> String {
    let mut out = format!(
        "verify: {} mismatches ({} snapshots, {} chunks walked, as of commit {})\n",
        v.mismatches, v.snapshots, v.chunks, v.as_of_seq
    );
    for detail in &v.details {
        out.push_str(&format!("  {detail}\n"));
    }
    if v.details.len() as u64 > 0 && (v.details.len() as u64) < v.mismatches {
        out.push_str(&format!(
            "  … and {} more\n",
            v.mismatches - v.details.len() as u64
        ));
    }
    out
}

/// Wall-clock Unix ms (for "4s ago").
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The confirmation `snapshot delete` asks for before deleting more than
/// one snapshot (and `snapshot policy set|rm` before an expiring or
/// orphaning change). Anything but `y`/`yes` (EOF included) declines.
pub fn confirm(question: &str) -> Result<bool> {
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
        println!("{}", reclaim_line(out.reclaim.as_ref()));
        return Ok(());
    }
    let resolved =
        control::call::<cm::SnapshotResolve>(dir, api::SnapshotResolveParams { selectors })
            .await?
            .snapshots;
    if resolved.len() > 1 && !yes {
        // The preview is a dry run over exactly these ids: what the delete
        // would refuse, and what it would give back. A single snapshot is
        // deleted without asking, so it costs no estimate.
        let preview = control::call::<cm::SnapshotDeleteMany>(
            dir,
            api::SnapshotDeleteManyParams {
                selectors: resolved.iter().map(|r| r.id.clone()).collect(),
                dry_run: true,
                force,
            },
        )
        .await?;
        for row in &resolved {
            match preview.refused.iter().find(|r| r.id == row.id) {
                Some(_) => eprintln!("  {} (would be refused)", label(row)),
                None => eprintln!("  {}", label(row)),
            }
        }
        eprintln!("{}", reclaim_line(preview.reclaim.as_ref()));
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

/// Whether `snapshot ls` asks the node for the accounting index's sizes:
/// JSON always carries them; a table only when a size column is shown or
/// is the sort key (`-o name,created -s used`).
pub fn wants_sizes(json: bool, columns: &[Column], sort: Option<Column>) -> bool {
    json || columns.iter().any(|c| c.is_size()) || sort.is_some_and(|c| c.is_size())
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
        let table = render_table(&rows, DEFAULT_COLUMNS, None, false, &BTreeSet::new());
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
        let table = render_table(
            &[a.clone(), b.clone()],
            &columns,
            Some(Column::Refer),
            true,
            &BTreeSet::new(),
        );
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
        let created = render_table(&[a, b], &[Column::Created], None, true, &BTreeSet::new());
        assert_eq!(created.lines().nth(1), Some("1000"));
        assert!(Column::parse("kept_by").is_ok() && Column::parse("KEPT-BY").is_ok());
        assert!(Column::parse_list("name,size").is_err());
        assert_eq!(
            render_table(&[], DEFAULT_COLUMNS, None, false, &BTreeSet::new()),
            "no snapshots\n"
        );
        assert!(under("/a", "/") && under("/a/b", "/a") && under("/a", "/a/"));
        assert!(!under("/ab", "/a") && !under("/", "/a"));
    }

    fn sized(
        mut r: api::SnapshotStatus,
        used: u64,
        written: u64,
        refer: u64,
    ) -> api::SnapshotStatus {
        r.used = Some(used);
        r.written = Some(written);
        r.refer = Some(refer);
        r.lsize = Some(refer + 7);
        r.as_of_seq = Some(88_213);
        r.as_of_ms = Some(1_790_603_755_000);
        r.size_state = Some(api::SizeState::Ok);
        r
    }

    fn cells(table: &str, line: usize) -> Vec<String> {
        table
            .lines()
            .nth(line)
            .unwrap()
            .split("  ")
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .map(String::from)
            .collect()
    }

    #[test]
    fn sizes_are_requested_for_a_size_sort_column_too() {
        let names = [Column::Name, Column::Created];
        assert!(!wants_sizes(false, &names, None));
        assert!(!wants_sizes(false, &names, Some(Column::Created)));
        assert!(wants_sizes(false, &names, Some(Column::Used)));
        assert!(wants_sizes(false, &[Column::Name, Column::Refer], None));
        assert!(wants_sizes(true, &names, None));
    }

    #[test]
    fn sizes_render_in_units_parsable_and_sort_by_used() {
        let mut big = sized(
            row("/p", "big", 1, 1000),
            4_402_341_478,
            6_442_450_944,
            86_114_094_285,
        );
        big.refer_bytes = Some(1);
        let small = sized(
            row("/p", "small", 2, 2000),
            3 << 20,
            9 << 20,
            87_295_506_841,
        );
        let zero = sized(row("/p", "zero", 3, 3000), 0, 0, 87_295_506_841);
        let rows = vec![big, small, zero];
        let table = render_table(&rows, DEFAULT_COLUMNS, None, false, &BTreeSet::new());
        // The index's REFER, not `≈refer_bytes`; a real 0 is printed as 0.
        assert_eq!(cells(&table, 1)[3..6], ["4.1G", "6.0G", "80G"], "{table}");
        assert_eq!(cells(&table, 2)[3..6], ["3.0M", "9.0M", "81G"], "{table}");
        assert_eq!(cells(&table, 3)[3..6], ["0", "0", "81G"], "{table}");
        assert!(!table.contains('≈'), "{table}");

        // `-s used` finds the one eating the space (ascending: last).
        let sorted = render_table(
            &rows,
            &[Column::Name, Column::Used],
            Some(Column::Used),
            false,
            &BTreeSet::new(),
        );
        let order: Vec<String> = (1..=3).map(|i| cells(&sorted, i)[0].clone()).collect();
        assert_eq!(order, ["/p@zero", "/p@small", "/p@big"]);

        // `-p`: exact bytes; `-o lsize` is an extra column.
        let columns = Column::parse_list("name,used,written,refer,lsize").unwrap();
        let exact = render_table(&rows[..1], &columns, None, true, &BTreeSet::new());
        assert_eq!(
            cells(&exact, 0),
            ["NAME", "USED", "WRITTEN", "REFER", "LSIZE"]
        );
        assert_eq!(
            cells(&exact, 1),
            [
                "/p@big",
                "4402341478",
                "6442450944",
                "86114094285",
                "86114094292"
            ]
        );
        assert!(!DEFAULT_COLUMNS.contains(&Column::Lsize));

        // The footer names the size columns shown and the commit.
        let now = 1_790_603_759_000;
        assert_eq!(
            size_footer(&rows, DEFAULT_COLUMNS, now).as_deref(),
            Some("USED/WRITTEN/REFER as of commit 88213 (4s ago) · logical bytes, pre-compression")
        );
        assert_eq!(
            size_footer(&rows, &columns, now + 3_600_000).as_deref(),
            Some(
                "USED/WRITTEN/REFER/LSIZE as of commit 88213 (1h ago) · logical bytes, \
                 pre-compression"
            )
        );
        assert_eq!(size_footer(&rows, &[Column::Name, Column::Id], now), None);
        assert_eq!(size_footer(&[], DEFAULT_COLUMNS, now), None);
    }

    #[test]
    fn building_and_off_cells_are_never_zero() {
        let mut building = row("/p", "b", 1, 1000);
        building.size_state = Some(api::SizeState::Building);
        building.building_pct = Some(37);
        building.refer_bytes = Some(5 << 30);
        let table = render_table(
            &[building.clone()],
            DEFAULT_COLUMNS,
            None,
            false,
            &BTreeSet::new(),
        );
        assert_eq!(
            table.matches("building (37%)").count(),
            3,
            "USED, WRITTEN and REFER: {table}"
        );
        assert!(!table.contains(" 0 ") && !table.contains('≈'), "{table}");
        let exact = render_table(
            &[building.clone()],
            &[Column::Used, Column::Refer],
            None,
            true,
            &BTreeSet::new(),
        );
        assert_eq!(cells(&exact, 1), ["-", "-"]);
        assert_eq!(
            size_footer(&[building], DEFAULT_COLUMNS, 0).as_deref(),
            Some("USED/WRITTEN/REFER: the accounting index is building (37%); run again shortly")
        );

        let mut off = row("/p", "o", 1, 1000);
        off.size_state = Some(api::SizeState::Off);
        off.refer_bytes = Some(5 << 30);
        let table = render_table(
            &[off.clone()],
            DEFAULT_COLUMNS,
            None,
            false,
            &BTreeSet::new(),
        );
        // USED/WRITTEN `-`; REFER falls back to the creation-time size.
        assert_eq!(cells(&table, 1)[3..6], ["-", "-", "≈5.0G"], "{table}");
        assert_eq!(
            size_footer(&[off.clone()], DEFAULT_COLUMNS, 0).as_deref(),
            Some("USED/WRITTEN/REFER: snapshot accounting is off (CONSTELLATION_SNAPACCT=off)")
        );
        off.refer_bytes = None;
        let table = render_table(&[off], DEFAULT_COLUMNS, None, false, &BTreeSet::new());
        assert_eq!(cells(&table, 1)[3..6], ["-", "-", "-"], "{table}");
    }

    #[test]
    fn reclaim_and_space_lines() {
        let est = api::ReclaimEstimate {
            bytes: 44_345_643_008,
            chunks: 812,
            as_of_seq: 9,
            ..Default::default()
        };
        assert_eq!(
            reclaim_line(Some(&est)),
            "would reclaim ≈ 41G in 812 chunks (after GC)"
        );
        let building = api::ReclaimEstimate {
            building: true,
            building_pct: 12,
            ..Default::default()
        };
        assert_eq!(
            reclaim_line(Some(&building)),
            "would reclaim: no estimate yet, the accounting index is building (12%)"
        );
        assert_eq!(
            reclaim_line(None),
            "would reclaim: no estimate (snapshot accounting is off or unavailable)"
        );

        let gib = 1u64 << 30;
        let amount = |bytes: u64| api::SpaceAmount {
            bytes,
            chunks: bytes >> 22,
        };
        let b = api::SpaceBreakdown {
            live_logical: 1_121_501_860_331,
            snapshots_total: amount(96 * gib + 400 * (1 << 20)),
            unique: amount(7 * gib + 100 * (1 << 20)),
            shared_snapshots_only: amount(89 * gib + 300 * (1 << 20)),
            shared_with_live: amount(500 * gib),
            awaiting_gc: amount(12 * gib),
            gc_horizon_ms: 7 * 86_400_000,
            physical_ratio: Some(0.61),
            physical_estimate: Some(63_150_000_000),
            as_of_seq: 88_213,
            as_of_ms: 1_000_000,
            ..Default::default()
        };
        let text = render_space(&b, 1_004_000);
        let lines: Vec<&str> = text.lines().collect();
        assert!(
            lines[0].starts_with("live data (logical)") && lines[0].ends_with("1.0T"),
            "{text}"
        );
        assert!(
            lines[1].contains("96G")
                && lines[1].ends_with("← deleting every snapshot returns this"),
            "{text}"
        );
        assert!(
            lines[2].contains("7.1G") && lines[2].contains("Σ USED; not the total"),
            "{text}"
        );
        assert!(lines[3].starts_with("  shared by ≥2 snapshots only") && lines[3].contains("89G"));
        assert!(lines[4].contains("500G") && lines[4].ends_with("(costs nothing extra)"));
        assert!(lines[5].ends_with("(freed snapshots; horizon 7d)") && lines[5].contains("12G"));
        assert_eq!(
            lines[6],
            "estimated physical (×0.61 compression, from last GC round): snapshots, total ≈ 59G"
        );
        assert!(
            lines[7].starts_with("note: per-snapshot USED values do not sum"),
            "{text}"
        );
        assert_eq!(
            lines[8],
            "as of commit 88213 (4s ago) · logical bytes, pre-compression"
        );
        // The values line up in one column (by chars: `≥`, `Σ` are
        // multi-byte).
        let col = lines[0].chars().count() - "1.0T".len();
        for line in &lines[1..6] {
            let chars: Vec<char> = line.chars().collect();
            assert!(
                chars[col].is_ascii_digit() && chars[col - 1] == ' ',
                "{line:?} (value column {col})"
            );
        }

        let pending = api::SpaceBreakdown {
            estimate_pending: true,
            ..Default::default()
        };
        let text = render_space(&pending, 0);
        assert!(
            text.contains("estimate: the live flags are pending"),
            "{text}"
        );
        assert!(!render_space(&b, 1_004_000).contains("estimate: the live flags"));
        let none = api::SpaceBreakdown {
            path: Some("/projects".into()),
            gc_horizon_ms: 36 * 3_600_000,
            ..Default::default()
        };
        let text = render_space(&none, 0);
        assert!(text.starts_with("space of /projects:"), "{text}");
        assert!(
            text.contains("horizon 36h") && text.contains("estimated physical: -"),
            "{text}"
        );
        assert!(text.contains("(time unknown)"), "{text}");
        let building = api::SpaceBreakdown {
            building: true,
            building_pct: 40,
            ..Default::default()
        };
        assert_eq!(
            render_space(&building, 0),
            "the accounting index is building (40%); run again shortly\n"
        );

        let ok = api::SpaceVerified {
            snapshots: 4,
            chunks: 9,
            as_of_seq: 3,
            ..Default::default()
        };
        assert_eq!(
            render_verify(&ok),
            "verify: 0 mismatches (4 snapshots, 9 chunks walked, as of commit 3)\n"
        );
        let bad = api::SpaceVerified {
            mismatches: 3,
            details: vec!["USED of /a@x: index 5, brute force 7".into()],
            ..ok
        };
        let text = render_verify(&bad);
        assert!(text.starts_with("verify: 3 mismatches"), "{text}");
        assert!(
            text.contains("  USED of /a@x: index 5, brute force 7\n  … and 2 more"),
            "{text}"
        );
        assert_eq!(exact_duration(90_000), "90s");
        assert_eq!(ago(3 * 86_400_000, 86_400_000), "2d ago");
    }

    #[test]
    fn orphaned_auto_snapshots_say_so_and_filter() {
        let mut owned = row("/p", "auto-1", 1, 1000);
        owned.origin = "auto".into();
        owned.policy_ino = 10;
        let mut orphan = row("/q", "auto-2", 2, 2000);
        orphan.origin = "auto".into();
        orphan.policy_ino = 11;
        // A manual snapshot of an orphaned stream's directory is manual.
        let mut manual = row("/q", "monday", 3, 3000);
        manual.policy_ino = 11;
        let listing = api::SnapPolicyListing {
            roots: vec![
                api::SnapPolicyRoot {
                    ino: 10,
                    canonical: Some("1h:1d".into()),
                    auto_snapshots: 1,
                    ..Default::default()
                },
                api::SnapPolicyRoot {
                    ino: 11,
                    auto_snapshots: 1,
                    orphaned: true,
                    ..Default::default()
                },
            ],
        };
        let orphaned = orphaned_streams(&listing);
        assert_eq!(orphaned, BTreeSet::from([11]));
        let rows = [owned, orphan, manual];
        let table = render_table(
            &rows,
            &[Column::Name, Column::Origin],
            None,
            false,
            &orphaned,
        );
        let lines: Vec<Vec<&str>> = table
            .lines()
            .map(|l| {
                l.split("  ")
                    .filter(|c| !c.is_empty())
                    .map(str::trim)
                    .collect()
            })
            .collect();
        assert_eq!(lines[1], ["/p@auto-1", "auto"]);
        assert_eq!(lines[2], ["/q@auto-2", "auto (orphaned)"]);
        assert_eq!(lines[3], ["/q@monday", "manual"]);
        let kept: Vec<&str> = rows
            .iter()
            .filter(|r| OriginFilter::Orphaned.keep(r, &orphaned))
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(kept, ["auto-2"]);
        assert!(OriginFilter::Auto.keep(&rows[1], &BTreeSet::new()));
    }

    #[test]
    fn orphan_fetch_failure_degrades_except_for_the_filter() {
        let fail = || Err(anyhow::anyhow!("control socket gone"));
        let (set, warn) = orphans_or_degrade(OriginFilter::All, fail()).unwrap();
        assert!(set.is_empty());
        assert!(warn.unwrap().contains("control socket gone"));
        assert!(orphans_or_degrade(OriginFilter::Orphaned, fail()).is_err());
        let (set, warn) =
            orphans_or_degrade(OriginFilter::All, Ok(api::SnapPolicyListing::default())).unwrap();
        assert!(set.is_empty() && warn.is_none());
        // Fetched only when something uses it.
        assert!(wants_orphans(
            OriginFilter::All,
            DEFAULT_COLUMNS,
            None,
            false
        ));
        assert!(!wants_orphans(
            OriginFilter::All,
            &[Column::Name],
            None,
            false
        ));
        assert!(!wants_orphans(
            OriginFilter::Auto,
            DEFAULT_COLUMNS,
            None,
            true
        ));
        assert!(wants_orphans(
            OriginFilter::Orphaned,
            &[Column::Name],
            None,
            true
        ));
        // And the degraded table says plain `auto`.
        let mut r = row("/q", "auto-2", 2, 2000);
        r.origin = "auto".into();
        r.policy_ino = 11;
        let table = render_table(&[r], &[Column::Origin], None, false, &set);
        assert!(!table.contains("orphaned"), "{table}");
    }
}
