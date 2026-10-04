//! Plan 32 M3 + M4: automatic snapshot creation and expiry, end to end
//! (Step 11's `snapsched` and `snapsched-s3-outage`, plus `snapsched-grace`
//! for Step 4.3).
//!
//! A policy sits on `/proj` (set by `setxattr`, the way an operator's
//! `setfattr` would), the scheduler ticks every second
//! (`CONSTELLATION_SNAPSCHED_TICK_MS=1000`), and a seeded writer renames a
//! fresh counter value into `/proj/counter` about once a second, so every
//! 10 s bucket has a change to snapshot (`skip-empty` is on by default).
//!
//! # The retention oracle
//!
//! Every scenario ends the same way: the writer stops, and once the newest
//! snapshot holds its last value (skip-empty then creates nothing more) the
//! surviving auto set must equal **`retention::evaluate`** — the real
//! function, linked from `constellation-meta`, not a copy — over the
//! *creation list*: every snapshot the run ever created, read back from the
//! scheduler's audit journal (`snapsched/journal/*.json` in the bucket, with
//! `constellation-store-s3`'s own types), with the snapshots the scenario
//! held marked held. Both lists are printed side by side ([`render`]).
//! "Survivors equal `evaluate` over every creation" is the fixed point the
//! in-process test `steady_state_survivors_are_evaluate_over_every_creation`
//! proves for one process; here it runs across processes, a leader kill and
//! an S3 outage.
//!
//! The journal is checked on its own too ([`check_deletions`]): each
//! expiry run's victims must be ones `evaluate` expires over the creations
//! journaled up to that run (monotonicity makes that the exact bound: a
//! later creation only expires more), each with the reason `no tier keeps
//! it`, none held, and none before the grace window `snapsched/state.json`
//! records for the root closed.
//!
//! The creation list is the journal's `created` entries, plus (by id) its
//! `deleted` entries and every auto snapshot the scenario saw listed. Those
//! extras are normally already journaled; the one legitimate exception is a
//! leader killed after its holder created a snapshot but before the tick
//! wrote its audit object (the new leader's retry of that bucket answers
//! `AlreadyExists`, journaled as skipped), or an S3 cut swallowing that
//! write. A scenario names the window where that is allowed; anywhere else an
//! unjournaled snapshot fails.
//!
//! # The scenarios
//!
//! **`snapsched`** (two nodes, `10s:1m 1m:4m; last=2`, grace 120 s, expiry
//! every 10 s, ~7.5 min): `b` creates the filesystem, holds the root lease and
//! runs the writer; `a` binds the policy and takes the scheduler's lead
//! with `snapshot sched run`, so snapshots are created through the
//! holder-side batch, forwarded from `a` to `b` over P2P. Early, inside the
//! grace window, it creates a manual snapshot of `/proj`, holds the first
//! auto snapshot (plain) and the second (`--by csi:test-uid`). 150 s
//! of schedule (root lease never moves, never two leaders), then `kill -9`
//! of the leader right after a snapshot lands, 100 s more, the killed
//! node mounted again, skip-empty (40 s idle → no creation, one write → one),
//! and the oracle. The gap and bucket-coverage checks run over the full
//! creation list (every bucket the writer covered), not over the survivors,
//! which expiry thins out.
//!
//! **`snapsched-s3-outage`** (one node, grace 20 s): once expiry is
//! deleting for real, S3 is cut for 90 s: `create_failed` rises, `expired`
//! does not move and no snapshot disappears; after the heal exactly one
//! catch-up snapshot appears (no backfill), expiry resumes, and the oracle
//! holds.
//!
//! **`snapsched-grace`** (one node, grace 30 s): `10s:5m` until about ten
//! snapshots exist, then `setfattr` shortens it to `10s:1m`. Nothing is
//! deleted until the window `state.json` records for the change closes,
//! although the new policy wants to (`skipped_grace` rises); then exactly
//! what `evaluate` with the new policy says. Inside the window it holds the
//! oldest snapshot, one the new policy expires: it survives, and the oracle
//! marks it held.

use super::m11::dump_logs_on_failure;
use super::m9::node_id;
use super::{eventually, raw_objects, set_xattr, setup, ts};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{anyhow, bail, ensure, Context, Result};
use constellation_meta::snapsched::{evaluate, Origin, Reason, SnapFacts, SnapPolicy, Verdict};
use constellation_store_s3::snapsched::{SnapSchedJournalEntry, SnapSchedState};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const POLICY: &str = "10s:1m 1m:4m; last=2";
const POLICY_XATTR: &str = "user.constellation.snapshots";
/// The policies' finest tier: one snapshot per bucket.
const BUCKET_MS: i64 = 10_000;
const TICK_MS: i64 = 1_000;
/// `CONSTELLATION_LEASE_TTL_MS` for `snapsched`: both the root lease and
/// the scheduler's `_snapsched` lease (which is at least three ticks).
/// Shortened from the 60 s default so a takeover fits the scenario.
const TTL_MS: i64 = 10_000;
/// Slack on top of the plan's "lease TTL plus one tick" for everything
/// between the last snapshot before the kill and the first one after it
/// that is not the scheduler's wait for the lease: the harness notices
/// the last snapshot through a replica (up to a sync round, ~1 s) and then
/// kills (`kill -9` + reap, < 1 s); the dead node renewed its lease up to
/// one tick before it died, so it lapses up to a TTL after the kill; the
/// new leader's tick then runs the holder's batch — one drain and one
/// publish to S3, 1–3 s on this shared host under load — and, when the
/// dead leader was also the root lease holder, first takes the root lease
/// (one more CAS round, and the writer's forwarded writes back off until
/// then). Measured takeover gaps are 10.5–10.9 s (TTL plus a bit), so 5 s
/// leaves about 5 s of slack; a scheduler that waited a second TTL overshoots
/// it. It does not by itself prove no bucket was dropped: that is
/// [`check_consecutive`]'s job, on the bucket names (a dropped 10 s bucket
/// shows as a ~20 s gap, which a bound this loose cannot tell from load).
/// `snap-drain-busy` made the holder's batch fast under a busy writer, but
/// the margin was never for a slow batch alone, so it stays.
const GAP_MARGIN_MS: i64 = 5_000;
/// The largest gap allowed between consecutive auto snapshots, anywhere in
/// the run (a steady-state gap is one bucket plus jitter, well below it).
const MAX_GAP_MS: i64 = TTL_MS + TICK_MS + GAP_MARGIN_MS;
/// `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` in `snapsched` and
/// `snapsched-s3-outage`: one expiry run per bucket (the default, 60 s,
/// would leave six creations between two runs and stretch the oracle's
/// wait for a fixed point to a minute).
const EXPIRE_EVERY_S: u64 = 10;
/// How long, after the stream stops growing, the survivors may take to
/// reach `evaluate`'s keep set: one expiry period, a few ticks for the
/// run to be scheduled and its batches to land, and a replica sync round
/// for the listing to show it.
const FIXED_POINT: Duration = Duration::from_secs(EXPIRE_EVERY_S + 20);
/// The audit reason of an expiry deletion (`snapexpire`).
const EXPIRY_REASON: &str = "no tier keeps it";
/// The owner of `snapsched`'s second hold.
const CSI_OWNER: &str = "csi:test-uid";

pub(super) fn tuned(c: Client, ttl_ms: Option<i64>, grace_s: u64, expire_every_s: u64) -> Client {
    let c = c
        .with_env("CONSTELLATION_SNAPSCHED_TICK_MS", &TICK_MS.to_string())
        .with_env("CONSTELLATION_SNAPSCHED_GRACE_S", &grace_s.to_string())
        .with_env(
            "CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S",
            &expire_every_s.to_string(),
        );
    match ttl_ms {
        Some(ttl) => c.with_env("CONSTELLATION_LEASE_TTL_MS", &ttl.to_string()),
        None => c,
    }
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn bucket_of(ms: i64) -> i64 {
    ms.div_euclid(BUCKET_MS) * BUCKET_MS
}

pub(super) fn parse_policy(text: &str) -> Result<SnapPolicy> {
    SnapPolicy::parse(text).map_err(|e| anyhow!("parsing {text:?}: {e:?}"))
}

/// Days from 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `auto-20261002T120010Z` → the bucket's start, Unix ms. `None` for any
/// other shape (the scheduler's names for `s` tiers carry seconds).
fn auto_bucket(name: &str) -> Option<i64> {
    let s = name.strip_prefix("auto-")?.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    if date.len() != 8
        || time.len() != 6
        || !(date.bytes().chain(time.bytes())).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let n = |s: &str| s.parse::<i64>().ok();
    let days = days_from_civil(n(&date[..4])?, n(&date[4..6])?, n(&date[6..])?);
    let secs = n(&time[..2])? * 3600 + n(&time[2..4])? * 60 + n(&time[4..])?;
    Some((days * 86_400 + secs) * 1000)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Snap {
    pub(super) name: String,
    pub(super) id: String,
    pub(super) created: i64,
    pub(super) bucket: i64,
}

impl Snap {
    fn new(name: &str, id: &str, created: i64) -> Result<Snap> {
        Ok(Snap {
            name: name.to_string(),
            id: id.to_string(),
            created,
            bucket: auto_bucket(name)
                .with_context(|| format!("{name:?} is not an auto-<UTC> name"))?,
        })
    }
}

pub(super) fn auto_snaps(rows: &[serde_json::Value]) -> Result<Vec<Snap>> {
    let mut out = Vec::new();
    for row in rows {
        if row["path"] != "/proj" || row["origin"] != "auto" {
            continue;
        }
        out.push(Snap::new(
            row["name"].as_str().unwrap_or_default(),
            row["id"].as_str().unwrap_or_default(),
            row["created_unix_ms"].as_i64().unwrap_or_default(),
        )?);
    }
    out.sort_by_key(|s| (s.created, s.bucket));
    Ok(out)
}

/// Every snapshot of `/proj` (auto, manual, held): `(name, id, held_by)`
/// in `created` order, `held_by` `Some("")` for a plain hold.
fn proj_rows(rows: &[serde_json::Value]) -> Vec<(String, String, Option<String>)> {
    let mut out: Vec<(i64, String, String, Option<String>)> = rows
        .iter()
        .filter(|row| row["path"] == "/proj")
        .map(|row| {
            (
                row["created_unix_ms"].as_i64().unwrap_or_default(),
                row["name"].as_str().unwrap_or_default().to_string(),
                row["id"].as_str().unwrap_or_default().to_string(),
                (row["held"] == true).then(|| row["held_by"].as_str().unwrap_or("").to_string()),
            )
        })
        .collect();
    out.sort();
    out.into_iter().map(|(_, n, i, h)| (n, i, h)).collect()
}

/// `/proj`'s auto snapshots through `snapshot ls / --json` (the CLI).
fn listed(c: &Client) -> Result<Vec<Snap>> {
    auto_snaps(&c.snapshot_rows()?)
}

/// The same, straight from `snapshot.list` without sizes: cheap enough to
/// poll (no accounting work).
pub(super) fn polled(c: &Client) -> Result<Vec<Snap>> {
    let listing = c.control_call(
        "snapshot.list",
        serde_json::json!({"path": null, "sizes": false}),
        Duration::from_secs(60),
    )?;
    auto_snaps(
        listing["snapshots"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default(),
    )
}

fn names(snaps: &[Snap]) -> BTreeSet<String> {
    snaps.iter().map(|s| s.name.clone()).collect()
}

/// `snapshot.sched.status` (what `snapshot sched status --json` prints),
/// straight over the control socket with a 60 s bound: a daemon frozen
/// with the host for a while (17 s seen here, both nodes at once) answers
/// late, and a status read is not what any scenario asserts on. The CLI's
/// own 10 s handshake timeout failed such a run.
pub(super) fn sched_status(c: &Client) -> Result<serde_json::Value> {
    c.control_call(
        "snapshot.sched.status",
        serde_json::json!({}),
        Duration::from_secs(60),
    )
    .with_context(|| format!("{}: snapshot.sched.status", c.name))
}

pub(super) fn is_leader(c: &Client) -> Result<bool> {
    Ok(sched_status(c)?["stats"]["leader"] == true)
}

pub(super) fn stat(status: &serde_json::Value, key: &str) -> u64 {
    status["stats"][key].as_u64().unwrap_or(0)
}

/// The root lease as `c` reports it: `(holder, epoch)`.
pub(super) fn root_lease(c: &Client) -> Result<(u64, u64)> {
    let lease = c.control_status()?["lease"].clone();
    Ok((
        lease["holder"].as_u64().unwrap_or(0),
        lease["epoch"].as_u64().unwrap_or(0),
    ))
}

/// The snapshot invariants every phase checks: names are `auto-<UTC>` of
/// 10 s buckets, no bucket twice, nobody stamped a snapshot before its
/// bucket began (the name is the leader's bucket, `created` the holder's
/// later clock — same host), and consecutive snapshots are at most
/// `max_gap` apart.
fn check_series(what: &str, snaps: &[Snap], max_gap: i64) -> Result<()> {
    let mut buckets = BTreeSet::new();
    for s in snaps {
        ensure!(
            s.bucket % BUCKET_MS == 0,
            "{what}: {} is not a {BUCKET_MS} ms bucket",
            s.name
        );
        ensure!(
            buckets.insert(s.bucket),
            "{what}: bucket {} has two snapshots: {snaps:?}",
            s.name
        );
        ensure!(
            s.created >= s.bucket,
            "{what}: {} created at {} before its bucket began",
            s.name,
            s.created
        );
    }
    for w in snaps.windows(2) {
        let gap = w[1].created - w[0].created;
        ensure!(
            gap <= max_gap,
            "{what}: {gap} ms between {} and {} (allowed {max_gap} ms = TTL {TTL_MS} + \
             tick {TICK_MS} + margin {GAP_MARGIN_MS})",
            w[0].name,
            w[1].name
        );
    }
    Ok(())
}

/// While the writer runs every bucket has a change, so every bucket from
/// the first snapshot's to the last one's must be covered — in plan 32
/// Step 3.3's sense (`retention::due`): a snapshot is named for it, or was
/// *created* inside it. The second is the plan's documented late-holder
/// case: a batch that lands after its bucket ended (the holder busy or
/// stalled) stamps a `created` in the next bucket, which then counts as
/// covered, so that bucket gets no snapshot of its own name. The one
/// exception is the pair that straddles the kill (`t_kill`): the takeover
/// may miss at most one bucket — callers pass `Some` only where that is
/// legitimate (the writer died with the leader). Two misses, or a miss
/// anywhere else, fail; so does a pair out of order or sharing a bucket.
fn check_consecutive(what: &str, snaps: &[Snap], t_kill: Option<i64>) -> Result<()> {
    let created_in: BTreeSet<i64> = snaps.iter().map(|s| bucket_of(s.created)).collect();
    for w in snaps.windows(2) {
        let d = w[1].bucket - w[0].bucket;
        ensure!(
            d >= BUCKET_MS,
            "{what}: {} then {} is {d} ms of buckets apart: a bucket was duplicated (or the \
             stream ran backwards)",
            w[0].name,
            w[1].name,
        );
        let straddles = t_kill.is_some_and(|k| w[0].created <= k && w[1].created > k);
        let allowed = usize::from(straddles);
        let missed: Vec<i64> = (1..d / BUCKET_MS)
            .map(|k| w[0].bucket + k * BUCKET_MS)
            .filter(|m| !created_in.contains(m))
            .collect();
        ensure!(
            missed.len() <= allowed,
            "{what}: {} then {} leaves {} bucket(s) uncovered {missed:?} (allowed {allowed}{}): a \
             bucket was dropped while the writer ran",
            w[0].name,
            w[1].name,
            missed.len(),
            if straddles { ", across the kill" } else { "" }
        );
    }
    Ok(())
}

fn gaps(snaps: &[Snap]) -> Vec<i64> {
    snaps
        .windows(2)
        .map(|w| w[1].created - w[0].created)
        .collect()
}

/// The counter value frozen in a snapshot, read through `c`'s mount.
fn frozen_counter(c: &Client, name: &str) -> Result<u64> {
    let path = c
        .mnt
        .join("proj/.constellation/snapshot")
        .join(name)
        .join("counter");
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("{}: reading {}", c.name, path.display()))?;
    text.trim()
        .parse()
        .with_context(|| format!("{}: {name}'s counter is {text:?}", c.name))
}

/// One counter bump: a fresh value renamed over `counter`, so a snapshot
/// never freezes a half-written file.
fn bump(dir: &Path, value: u64) -> std::io::Result<()> {
    let tmp = dir.join(".counter.tmp");
    std::fs::write(&tmp, value.to_string())?;
    std::fs::rename(&tmp, dir.join("counter"))
}

/// The seeded writer: bumps `<target>/counter` every 0.8–1.2 s (the
/// jitter from the scenario's seed). The target mount can be swapped
/// (after the node it wrote through was killed) or taken away (`pause`);
/// the lock is held across a bump, so after `pause` returns no bump is in
/// flight.
struct Writer {
    target: Arc<Mutex<Option<PathBuf>>>,
    /// The last value whose rename returned.
    value: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Writer {
    fn start(seed: u64, dir: PathBuf, first: u64) -> Writer {
        let target = Arc::new(Mutex::new(Some(dir)));
        let value = Arc::new(AtomicU64::new(first));
        let errors = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let (target, value, errors, stop) =
                (target.clone(), value.clone(), errors.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(seed);
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(rng.random_range(800..1200)));
                    let guard = target.lock().unwrap();
                    if let Some(dir) = guard.as_ref() {
                        let next = value.load(Ordering::Relaxed) + 1;
                        match bump(dir, next) {
                            Ok(()) => value.store(next, Ordering::Relaxed),
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        };
        Writer {
            target,
            value,
            errors,
            stop,
            handle: Some(handle),
        }
    }

    fn retarget(&self, dir: Option<PathBuf>) {
        *self.target.lock().unwrap() = dir;
    }

    fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Stop the thread; a bump wedged on a dead mount is left behind
    /// rather than hanging the scenario.
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let deadline = Instant::now() + Duration::from_secs(30);
            while !handle.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(100));
            }
            if handle.is_finished() {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Sleep `total`, failing early if `check` does (every `every`).
fn run_for(total: Duration, every: Duration, mut check: impl FnMut() -> Result<()>) -> Result<()> {
    let end = Instant::now() + total;
    while Instant::now() < end {
        std::thread::sleep((end - Instant::now()).min(every));
        check()?;
    }
    Ok(())
}

/// Stop writing through `writer` and wait until the newest snapshot on
/// `c` holds the last value: from then on skip-empty creates nothing, so
/// the stream is final. Returns the last value and when the writer stopped.
fn quiesce(writer: &Writer, c: &Client) -> Result<(u64, i64)> {
    writer.retarget(None);
    let t_stop = now_ms();
    let last = writer.value();
    eventually(
        "the last write is snapshotted",
        Duration::from_secs(40),
        || {
            let snaps = polled(c)?;
            let newest = snaps.last().context("no snapshot")?;
            let v = frozen_counter(c, &newest.name)?;
            ensure!(v == last, "newest {} froze {v}, want {last}", newest.name);
            Ok(())
        },
    )?;
    Ok((last, t_stop))
}

// --- the audit journal and the oracle --------------------------------

/// Everything the scheduler's audit journal says about `/proj`'s stream.
struct Audit {
    /// The policy root's inode (the journal's `root_ino`).
    ino: u64,
    /// The canonical policies the entries carry, in journal order.
    policies: Vec<String>,
    /// `(tick ts, snapshot)` per creation, in journal order.
    created: Vec<(i64, Snap)>,
    /// `(tick ts, snapshot, reason)` per deletion, in journal order.
    deleted: Vec<(i64, Snap, Option<String>)>,
    /// `(tick ts, name, reason)` per skipped creation.
    skipped: Vec<(i64, String, String)>,
    /// How many entries the run wrote.
    entries: usize,
}

pub(super) fn journal_entries(endpoint: &str, prefix: &str) -> Result<Vec<SnapSchedJournalEntry>> {
    let mut keys: Vec<String> = raw_objects(endpoint, &format!("{prefix}/snapsched/journal/"))?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    // `<ts as 16 hex digits>-<nonce>.json`: the key order is the tick order.
    keys.sort();
    let mut out = Vec::new();
    for key in keys {
        let body = crate::s3auth::get(&format!("{endpoint}/{BUCKET}/{key}"))
            .call()
            .with_context(|| format!("reading {key}"))?
            .into_string()?;
        out.push(serde_json::from_str(&body).with_context(|| format!("parsing {key}: {body}"))?);
    }
    Ok(out)
}

/// Every journal entry, one line each (tick time relative to the first,
/// node, created / skipped / failed / deleted): attached to a failed
/// series check, since the journal goes with the scenario's bucket.
fn journal_digest(endpoint: &str, prefix: &str) -> String {
    let entries = match journal_entries(endpoint, prefix) {
        Ok(entries) => entries,
        Err(e) => return format!("    (the journal could not be read: {e:#})\n"),
    };
    let t0 = entries.first().map_or(0, |e| e.ts);
    let mut out = String::from("    journal digest (tick +s, node: what):\n");
    for e in &entries {
        let mut parts = Vec::new();
        for r in &e.roots {
            parts.extend(
                r.created
                    .iter()
                    .map(|x| format!("created {} (+{} ms)", x.name, x.created_unix_ms - e.ts)),
            );
            parts.extend(
                r.skipped
                    .iter()
                    .map(|x| format!("skipped {}: {}", x.name, x.reason)),
            );
            parts.extend(
                r.failed
                    .iter()
                    .map(|x| format!("failed {}: {}", x.name, x.reason)),
            );
            if !r.deleted.is_empty() {
                parts.push(format!("deleted {}", r.deleted.len()));
            }
        }
        let _ = writeln!(
            out,
            "      +{:>8.1}s  {} (ts {}): {}",
            (e.ts - t0) as f64 / 1000.0,
            e.node,
            e.ts,
            parts.join("; ")
        );
    }
    out
}

fn read_audit(endpoint: &str, prefix: &str) -> Result<Audit> {
    let entries = journal_entries(endpoint, prefix)?;
    let mut audit = Audit {
        ino: 0,
        policies: Vec::new(),
        created: Vec::new(),
        deleted: Vec::new(),
        skipped: Vec::new(),
        entries: entries.len(),
    };
    let mut inos = BTreeSet::new();
    for entry in &entries {
        for root in &entry.roots {
            inos.insert(root.root_ino);
            ensure!(
                root.path == "/proj",
                "a journal entry names the root {:?}, not /proj",
                root.path
            );
            if audit.policies.last() != Some(&root.policy) {
                audit.policies.push(root.policy.clone());
            }
            for s in &root.created {
                ensure!(
                    s.reason.is_none(),
                    "creation {} carries a reason {:?}",
                    s.name,
                    s.reason
                );
                audit
                    .created
                    .push((entry.ts, Snap::new(&s.name, &s.id, s.created_unix_ms)?));
            }
            for s in &root.deleted {
                audit.deleted.push((
                    entry.ts,
                    Snap::new(&s.name, &s.id, s.created_unix_ms)?,
                    s.reason.clone(),
                ));
            }
            for s in &root.skipped {
                audit
                    .skipped
                    .push((entry.ts, s.name.clone(), s.reason.clone()));
            }
        }
    }
    ensure!(
        inos.len() == 1,
        "the journal names {} policy roots ({inos:?}), want exactly /proj's",
        inos.len()
    );
    audit.ino = *inos.first().unwrap();
    Ok(audit)
}

/// `snapsched/state.json`, the grace state.
pub(super) fn read_state(endpoint: &str, prefix: &str) -> Result<SnapSchedState> {
    let key = format!("{prefix}/snapsched/state.json");
    let body = crate::s3auth::get(&format!("{endpoint}/{BUCKET}/{key}"))
        .call()
        .with_context(|| format!("reading {key}"))?
        .into_string()?;
    serde_json::from_str(&body).with_context(|| format!("parsing {key}: {body}"))
}

/// The creation list: the journal's creations, plus (by id) its deletions
/// and every auto snapshot the scenario saw. Returns it in `created`
/// order with the unjournaled extras separately; the caller decides
/// whether those are legitimate (module doc).
fn creation_list(audit: &Audit, seen: &BTreeMap<String, Snap>) -> Result<(Vec<Snap>, Vec<Snap>)> {
    let mut by_id: BTreeMap<String, Snap> = BTreeMap::new();
    let mut names = BTreeSet::new();
    for (_, s) in &audit.created {
        ensure!(
            by_id.insert(s.id.clone(), s.clone()).is_none(),
            "the journal records the creation of {} ({}) twice",
            s.name,
            s.id
        );
        ensure!(
            names.insert(s.name.clone()),
            "two journaled creations share the name {}: a bucket got two snapshots",
            s.name
        );
    }
    let mut extra = BTreeMap::new();
    for s in audit.deleted.iter().map(|(_, s, _)| s).chain(seen.values()) {
        match by_id.get(&s.id) {
            Some(known) => ensure!(
                known.name == s.name && known.created == s.created,
                "{} ({}) is journaled as {known:?} but seen as {s:?}",
                s.name,
                s.id
            ),
            None => {
                extra.insert(s.id.clone(), s.clone());
            }
        }
    }
    let extra: Vec<Snap> = extra.into_values().collect();
    let mut all: Vec<Snap> = by_id.into_values().chain(extra.iter().cloned()).collect();
    all.sort_by_key(|s| (s.created, s.bucket));
    Ok((all, extra))
}

/// Retention's facts for the creation list: `origin = auto` of `ino`,
/// held as `holds` (id → owner, `""` for a plain hold) says.
pub(super) fn facts(ino: u64, snaps: &[Snap], holds: &BTreeMap<String, String>) -> Vec<SnapFacts> {
    snaps
        .iter()
        .map(|s| SnapFacts {
            id: s.id.clone(),
            created_unix_ms: s.created,
            origin: Origin::Auto,
            policy_ino: ino,
            held: holds.contains_key(&s.id),
            held_by: holds
                .get(&s.id)
                .and_then(|by| (!by.is_empty()).then(|| by.clone())),
        })
        .collect()
}

fn verdict_text(v: &Verdict) -> String {
    if !v.keep {
        return "expire".into();
    }
    let reasons: Vec<String> = v
        .reasons
        .iter()
        .map(|r| match r {
            Reason::Tier(i) => i.to_string(),
            Reason::Last(_) => "last".into(),
            Reason::Held(None) => "held".into(),
            Reason::Held(Some(by)) => format!("held: {by}"),
            Reason::Grace => "grace".into(),
            Reason::NotCandidate => "not a candidate".into(),
        })
        .collect();
    format!("keep ({})", reasons.join("·"))
}

/// The creation list with `evaluate`'s verdict and whether each snapshot
/// survives, one line each: the side-by-side view PROGRESS quotes.
fn render(
    what: &str,
    snaps: &[Snap],
    verdicts: &[Verdict],
    survivors: &BTreeSet<String>,
) -> String {
    let t0 = snaps.first().map_or(0, |s| s.created);
    let mut out = format!(
        "    {what}: {:<24} {:>9}  {:<26} survives\n",
        "creation (journal)", "created", "retention::evaluate"
    );
    for (s, v) in snaps.iter().zip(verdicts) {
        let survives = survivors.contains(&s.id);
        let mark = if survives == v.keep {
            ""
        } else {
            "  <-- MISMATCH"
        };
        let _ = writeln!(
            out,
            "    {what}: {:<24} {:>8.1}s  {:<26} {}{mark}",
            s.name,
            (s.created - t0) as f64 / 1000.0,
            verdict_text(v),
            if survives { "yes" } else { "no" },
        );
    }
    out
}

/// The oracle: the surviving auto set (ids) equals `evaluate`'s keep set
/// over the creation list. Returns the side-by-side table.
fn check_oracle(
    what: &str,
    policy: &SnapPolicy,
    ino: u64,
    creations: &[Snap],
    holds: &BTreeMap<String, String>,
    survivors: &[Snap],
) -> Result<String> {
    let verdicts = evaluate(policy, ino, &facts(ino, creations, holds));
    let alive: BTreeSet<String> = survivors.iter().map(|s| s.id.clone()).collect();
    let table = render(what, creations, &verdicts, &alive);
    let known: BTreeSet<&String> = creations.iter().map(|s| &s.id).collect();
    let strangers: Vec<&Snap> = survivors
        .iter()
        .filter(|s| !known.contains(&s.id))
        .collect();
    ensure!(
        strangers.is_empty(),
        "{what}: surviving snapshots missing from the creation list: {strangers:?}\n{table}"
    );
    let keep: BTreeSet<String> = creations
        .iter()
        .zip(&verdicts)
        .filter(|(_, v)| v.keep)
        .map(|(s, _)| s.id.clone())
        .collect();
    ensure!(
        keep == alive,
        "{what}: the survivors are not retention::evaluate's keep set ({} kept by evaluate, {} \
         survive)\n{table}",
        keep.len(),
        alive.len()
    );
    Ok(table)
}

/// Each expiry run's deletions against `evaluate` over the creations
/// journaled up to that run (and the unjournaled `extra` created before
/// it): every victim is one it expires, with the expiry reason, held by
/// nobody, and deleted no earlier than `not_before`. Returns the number of
/// deletions checked.
fn check_deletions(
    policy: &SnapPolicy,
    audit: &Audit,
    extra: &[Snap],
    holds: &BTreeMap<String, String>,
    not_before: i64,
) -> Result<usize> {
    let mut by_tick: BTreeMap<i64, Vec<&Snap>> = BTreeMap::new();
    for (tick, s, reason) in &audit.deleted {
        ensure!(
            reason.as_deref() == Some(EXPIRY_REASON),
            "{} was deleted with the reason {reason:?}, not {EXPIRY_REASON:?}",
            s.name
        );
        ensure!(
            !holds.contains_key(&s.id),
            "{} was deleted although it was held ({:?})",
            s.name,
            holds[&s.id]
        );
        ensure!(
            *tick >= not_before,
            "{} was deleted by the tick of {tick}, {} ms before the grace window closed",
            s.name,
            not_before - tick
        );
        by_tick.entry(*tick).or_default().push(s);
    }
    for (tick, victims) in &by_tick {
        let mut known: Vec<Snap> = audit
            .created
            .iter()
            .filter(|(t, _)| t <= tick)
            .map(|(_, s)| s.clone())
            .chain(extra.iter().filter(|s| s.created < *tick).cloned())
            .collect();
        known.sort_by_key(|s| s.created);
        let verdicts = evaluate(policy, audit.ino, &facts(audit.ino, &known, holds));
        for victim in victims {
            let pos = known
                .iter()
                .position(|s| s.id == victim.id)
                .with_context(|| {
                    format!(
                        "the tick of {tick} deleted {}, which no earlier tick created",
                        victim.name
                    )
                })?;
            ensure!(
                !verdicts[pos].keep,
                "the tick of {tick} deleted {}, but evaluate over the {} snapshots created by \
                 then keeps it ({})\n{}",
                victim.name,
                known.len(),
                verdict_text(&verdicts[pos]),
                render(
                    "at that tick",
                    &known,
                    &verdicts,
                    &BTreeSet::new() // survivors unknown here; the verdicts are the point
                )
            );
        }
    }
    Ok(audit.deleted.len())
}

/// The window, recorded in `state.json`, that a policy's first sighting
/// (`canonical: null` prior) or a change away from `from` opened on the
/// root: `(replaced_unix_ms, until_unix_ms)`.
pub(super) fn grace_window(
    state: &SnapSchedState,
    ino: u64,
    from: Option<&str>,
) -> Result<(i64, i64)> {
    let root = state
        .roots
        .get(&ino)
        .with_context(|| format!("state.json has no root {ino}: {state:?}"))?;
    let prior = root
        .prior
        .iter()
        .find(|p| p.canonical.as_deref() == from)
        .with_context(|| format!("state.json records no prior {from:?} for {ino}: {root:?}"))?;
    Ok((prior.replaced_unix_ms, prior.until_unix_ms))
}

// --- snapsched ---------------------------------------------------------

/// `CONSTELLATION_SNAPSCHED_GRACE_S` in `snapsched`. Small but non-zero,
/// and chosen so that every assertion stays exact:
/// - the manual snapshot and both holds land within the first ~30 s (the
///   scenario fails if they take half the window), well inside the
///   first-sighting window, so no hold can race a deletion: the oracle may
///   mark them held for every expiry run, and "no deleted snapshot was
///   held" needs no timing;
/// - the window outlasts the policy's first victim, so `skipped_grace`
///   must rise (the window is not vacuous). With the first two snapshots
///   held, the candidates start at the third (~20 s in); the first one that
///   is not its minute's `1m` representative leaves the `10s:1m` window
///   once six newer buckets exist, 90–100 s in depending on where the UTC
///   minute boundary falls. 120 s keeps it (and the next) back for two or
///   three expiry runs;
/// - it still leaves expiry running for real for most of the ~5 minutes the
///   writer runs, before the kill (the scenario asserts `expired > 0` by
///   then) and across it, through the `1m:4m` tier's first expirations
///   about five minutes in.
const SNAPSCHED_GRACE_S: u64 = 120;

pub fn snapsched(seed: u64) -> Result<()> {
    const NAME: &str = "snapsched";
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("snapsched-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    // Own node keys: a leader that is not the root lease holder reaches
    // it over P2P (snapshot batches have no S3 slow path).
    let node = |name: &str| -> Result<Client> {
        Ok(tuned(
            Client::new(root.path(), name, &env.endpoint, &backend)?.with_own_node_key(),
            Some(TTL_MS),
            SNAPSCHED_GRACE_S,
            EXPIRE_EVERY_S,
        ))
    };
    let mut a = node("a")?;
    let mut b = node("b")?;
    // b creates the filesystem and mounts first: it takes the root lease
    // at mount and keeps it, as the writer and the only node that writes.
    b.fs_create()?;
    b.mount()?;
    a.mount()?;
    let mut clients = [a, b];
    let result = snapsched_body(seed, &mut clients, &env.direct_endpoint, &prefix);
    dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        if c.is_mounted() {
            let _ = c.unmount();
        }
    }
    result
}

/// What the sampling loops collect: every auto snapshot seen listed (by
/// id), the leader per sample, and the highest `skipped_grace` a leader
/// reported.
#[derive(Default)]
struct Samples {
    seen: BTreeMap<String, Snap>,
    leaders: BTreeMap<u64, usize>,
    /// Samples that read two nodes leading one after the other, the
    /// older's lease lapsing between the two reads (legitimate; counted).
    overlaps: usize,
    skipped_grace: u64,
}

impl Samples {
    /// One sample over the mounted `clients`: never two leaders, and
    /// every listed auto snapshot recorded.
    ///
    /// The nodes are read one after the other, so two of them may both
    /// report `leader` legitimately: the first read just before its lease
    /// lapsed, the second just after it took the next epoch. A node
    /// reports `leader` only until its own lease deadline (a stalled
    /// leader past it does not), so two claims are two leaders when they
    /// name the same epoch, or when the older epoch's deadline is later
    /// than the moment the newer one was read leading: the newer took the
    /// lease while the older's was still live by the older's own last
    /// renewal. (One host, one clock; the taker could only claim the lease
    /// once the stored deadline, which is never earlier than the holder's
    /// own, had passed.)
    fn take(&mut self, clients: &[Client], ids: &[u64]) -> Result<()> {
        // (node, epoch, lease deadline, read at)
        let mut leading: Vec<(u64, u64, u64, u64)> = Vec::new();
        for (c, id) in clients.iter().zip(ids) {
            if !c.is_mounted() {
                continue;
            }
            let status = sched_status(c)?;
            if status["stats"]["leader"] == true {
                leading.push((
                    *id,
                    stat(&status, "lease_epoch"),
                    stat(&status, "lease_until_unix_ms"),
                    stat(&status, "now_unix_ms"),
                ));
                self.skipped_grace = self.skipped_grace.max(stat(&status, "skipped_grace"));
            }
            for s in polled(c)? {
                self.seen.insert(s.id.clone(), s);
            }
        }
        leading.sort_by_key(|&(_, epoch, _, _)| epoch);
        for pair in leading.windows(2) {
            let ((_, old_epoch, old_until, _), (_, new_epoch, _, new_read)) = (pair[0], pair[1]);
            ensure!(
                old_epoch != new_epoch && old_until <= new_read,
                "two scheduler leaders at once: (node, epoch, lease until, read at) {leading:?}"
            );
        }
        // The newest epoch is the leader of this sample.
        if let Some((id, _, _, _)) = leading.last() {
            *self.leaders.entry(*id).or_default() += 1;
            self.overlaps += usize::from(leading.len() > 1);
        }
        Ok(())
    }
}

fn snapsched_body(
    seed: u64,
    clients: &mut [Client; 2],
    endpoint: &str,
    prefix: &str,
) -> Result<()> {
    let policy = parse_policy(POLICY)?;
    let ids = [node_id(&clients[0])?, node_id(&clients[1])?];
    eventually("b holds the root lease", Duration::from_secs(30), || {
        let (holder, _) = root_lease(&clients[1])?;
        ensure!(holder == ids[1], "holder {holder}, want b ({})", ids[1]);
        Ok(())
    })?;
    let b_dir = clients[1].mnt.join("proj");
    std::fs::create_dir(&b_dir)?;
    bump(&b_dir, 0)?;
    let mut writer = Writer::start(seed, b_dir.clone(), 0);
    let a_dir = clients[0].mnt.join("proj");
    eventually("/proj on a", Duration::from_secs(20), || {
        ensure!(a_dir.join("counter").is_file(), "not yet");
        Ok(())
    })?;
    let lease_before = root_lease(&clients[1])?;
    eprintln!(
        "    snapsched: root lease before: holder {} epoch {}",
        lease_before.0, lease_before.1
    );

    // a binds the policy (as `setfattr` would; the write is forwarded to
    // b, the holder) and at once runs a tick, which takes the scheduler's
    // lease: the plan's case of a leader that does not hold the root
    // lease and creates through the holder. b's own ticks can still win
    // that race (it applies the policy first); the scenario then runs all
    // the same with the leader at the holder, and says so.
    let started = now_ms();
    set_xattr(&a_dir, POLICY_XATTR, POLICY.as_bytes()).context("setting the policy")?;
    let (_, out, _) = clients[0].snapshot_cli(&["sched", "run"])?;
    eprintln!(
        "    snapsched: `snapshot sched run` on a: {}",
        out.trim().replace('\n', " | ")
    );
    eventually("a scheduler leads", Duration::from_secs(30), || {
        ensure!(
            is_leader(&clients[0])? || is_leader(&clients[1])?,
            "nobody leads yet"
        );
        Ok(())
    })?;

    // Early, inside the grace window: a manual snapshot, a plain hold on
    // the first auto snapshot and a `csi:` hold on the second.
    let mut first_two = Vec::new();
    eventually("two auto snapshots", Duration::from_secs(40), || {
        first_two = polled(&clients[1])?;
        ensure!(first_two.len() >= 2, "{} so far", first_two.len());
        Ok(())
    })?;
    clients[0].snapshot_create("/proj@manual")?;
    let mut holds: BTreeMap<String, String> = BTreeMap::new();
    for (s, by) in first_two.iter().zip(["", CSI_OWNER]) {
        let mut args = vec!["hold", s.id.as_str()];
        if !by.is_empty() {
            args.extend(["--by", by]);
        }
        let (ok, stdout, stderr) = clients[0].snapshot_cli(&args)?;
        ensure!(ok, "holding {} ({args:?}): {stdout}{stderr}", s.name);
        holds.insert(s.id.clone(), by.to_string());
    }
    let t_holds = now_ms();
    eprintln!(
        "    snapsched: manual snapshot `manual`, held {} (plain) and {} (by {CSI_OWNER}), {} ms \
         after the policy was set",
        first_two[0].name,
        first_two[1].name,
        t_holds - started
    );
    ensure!(
        t_holds - started < SNAPSCHED_GRACE_S as i64 * 1000 / 2,
        "the holds took {} ms: too close to the end of the {SNAPSCHED_GRACE_S} s grace window \
         to rule out a race with the first deletion",
        t_holds - started
    );

    // 150 s of schedule: exactly one leader at a time; expiry
    // starts once the grace window closes.
    let mut samples = Samples::default();
    run_for(Duration::from_secs(150), Duration::from_secs(5), || {
        samples.take(clients, &ids)
    })?;
    eprintln!(
        "    snapsched: leader samples {:?} ({} handovers read mid-way); skipped_grace (max at a \
         leader) {}",
        samples.leaders, samples.overlaps, samples.skipped_grace
    );
    let lease_after = root_lease(&clients[1])?;
    eprintln!(
        "    snapsched: root lease after:  holder {} epoch {}",
        lease_after.0, lease_after.1
    );
    ensure!(
        lease_after == lease_before,
        "creating and expiring snapshots moved the root lease: (holder, epoch) {lease_before:?} \
         -> {lease_after:?}"
    );
    let expired_before_kill: u64 = clients
        .iter()
        .map(|c| sched_status(c).map(|s| stat(&s, "expired")))
        .sum::<Result<u64>>()?;
    ensure!(
        expired_before_kill > 0,
        "nothing expired in the first 150 s (grace {SNAPSCHED_GRACE_S} s)"
    );

    // Kill the leader right after a snapshot lands, so the gap measured
    // across the takeover is (almost) all takeover.
    // Exactly one leader (a read can fall mid-handover, or on a leader
    // stalled past its lease: read again).
    let mut leader = 0;
    eventually(
        "exactly one scheduler leader",
        Duration::from_secs(30),
        || {
            leader = match (is_leader(&clients[0])?, is_leader(&clients[1])?) {
                (true, false) => 0,
                (false, true) => 1,
                other => bail!("(a, b) leading = {other:?}"),
            };
            Ok(())
        },
    )?;
    let survivor = 1 - leader;
    let before_kill = names(&polled(&clients[survivor])?);
    eventually(
        "a fresh snapshot to kill after",
        Duration::from_secs(30),
        || {
            let now = names(&polled(&clients[survivor])?);
            ensure!(now.difference(&before_kill).next().is_some(), "none yet");
            Ok(())
        },
    )?;
    let t_kill = now_ms();
    clients[leader].kill9()?;
    if leader == 1 {
        eprintln!(
            "    snapsched: NOTE the killed leader is b, the root lease holder and the writer: a \
             must take over the root lease AND the scheduler (the rarer path)"
        );
        // b was also the writer (and the root lease holder): write
        // through a now, which takes the root lease once b's lapses.
        writer.retarget(Some(clients[0].mnt.join("proj")));
    }
    eprintln!(
        "    snapsched: RESULT killed leader = {} (a led before the kill: {}; node {}){}",
        clients[leader].name,
        leader == 0,
        ids[leader],
        if leader == 1 {
            ", also the root lease holder and the writer"
        } else {
            ""
        }
    );
    eventually(
        "the survivor leads",
        Duration::from_millis((TTL_MS + 5 * TICK_MS + 10_000) as u64),
        || {
            ensure!(is_leader(&clients[survivor])?, "not yet");
            Ok(())
        },
    )?;
    // The killed node comes back (it must catch up on the deletions its
    // replica missed), and 100 s more of schedule run.
    clients[leader].mount()?;
    run_for(Duration::from_secs(100), Duration::from_secs(5), || {
        samples.take(clients, &ids)
    })?;
    let s = &clients[survivor];

    // Skip-empty: stop writing; once the last value is frozen in a
    // snapshot, 40 s pass without a creation (expiry may still delete).
    let (last, t_stop) = quiesce(&writer, s)?;
    samples.take(clients, &ids)?;
    let idle_from = names(&polled(s)?);
    // Summed over both nodes: the killed node is mounted again and may
    // lead now (the survivor stalled past its lease, say).
    let skipped_empty = |clients: &[Client]| -> Result<u64> {
        clients
            .iter()
            .map(|c| sched_status(c).map(|s| stat(&s, "skipped_empty")))
            .sum()
    };
    let skipped_before = skipped_empty(clients)?;
    run_for(Duration::from_secs(40), Duration::from_secs(5), || {
        samples.take(clients, &ids)
    })?;
    let idle_to = names(&polled(s)?);
    let created_idle: Vec<&String> = idle_to.difference(&idle_from).collect();
    ensure!(
        created_idle.is_empty(),
        "an idle /proj got snapshots: {created_idle:?}"
    );
    // Non-vacuity: the leader did ask, and the holder answered unchanged.
    let skipped_after = skipped_empty(clients)?;
    ensure!(
        skipped_after > skipped_before,
        "skipped_empty did not move ({skipped_before} -> {skipped_after}): the idle buckets were \
         never asked about"
    );
    // One write: exactly one snapshot, of that value, in the bucket of
    // the write or the next (when the write lands at a bucket's end).
    let s_dir = s.mnt.join("proj");
    let t_write = now_ms();
    bump(&s_dir, last + 1)?;
    let mut new = Vec::new();
    eventually("the write is snapshotted", Duration::from_secs(30), || {
        new = polled(s)?
            .into_iter()
            .filter(|x| x.created > t_write)
            .collect();
        ensure!(!new.is_empty(), "not yet");
        Ok(())
    })?;
    std::thread::sleep(Duration::from_millis((3 * BUCKET_MS) as u64));
    samples.take(clients, &ids)?;
    let new: Vec<Snap> = samples
        .seen
        .values()
        .filter(|x| x.created > t_write)
        .cloned()
        .collect();
    ensure!(
        new.len() == 1,
        "one write gave {} snapshots: {new:?}",
        new.len()
    );
    let one = &new[0];
    ensure!(
        one.bucket == bucket_of(t_write) || one.bucket == bucket_of(t_write) + BUCKET_MS,
        "the write at {t_write} was snapshotted as {} (bucket {})",
        one.name,
        one.bucket
    );
    ensure!(
        frozen_counter(s, &one.name)? == last + 1,
        "{} froze the wrong value",
        one.name
    );
    eprintln!(
        "    snapsched: skip-empty: 40 s idle, skipped_empty {skipped_before} -> {skipped_after}; \
         one write -> {} ({} ms after it)",
        one.name,
        one.created - t_write
    );
    writer.stop();

    // ---- the oracle ----
    let audit = read_audit(endpoint, prefix)?;
    ensure!(
        audit.policies == [policy.to_string()],
        "the journal carries the policies {:?}, want only {:?}",
        audit.policies,
        policy.to_string()
    );
    let (creations, extra) = creation_list(&audit, &samples.seen)?;
    // Unjournaled creations: only the killed leader's last tick may lose
    // its audit object (its holder created the snapshot; the new leader's
    // retry got `AlreadyExists`).
    for x in &extra {
        ensure!(
            x.created <= t_kill && x.created > t_kill - 2 * BUCKET_MS,
            "{} ({}) exists but no journal entry records its creation, and it was not created \
             just before the kill (created {} ms before it)",
            x.name,
            x.id,
            t_kill - x.created
        );
    }
    ensure!(
        extra.len() <= 1,
        "{} unjournaled creations around the kill: {extra:?}",
        extra.len()
    );
    // No bucket twice, no gap above the bound, and while the writer ran
    // no bucket skipped (one allowed across the kill when the writer died
    // with the leader) — over every creation, kept or expired.
    let while_writing: Vec<Snap> = creations
        .iter()
        .filter(|x| x.created <= t_stop)
        .cloned()
        .collect();
    let series = check_series("every creation", &creations, i64::MAX)
        .and_then(|()| check_series("while writing", &while_writing, MAX_GAP_MS))
        .and_then(|()| {
            check_consecutive(
                "while writing",
                &while_writing,
                (leader == 1).then_some(t_kill),
            )
        });
    if let Err(e) = series {
        // The evidence goes with the bucket: keep it in the failure.
        let mut why = format!(
            "{e:#}\n    policy set at {started}, holds at {t_holds}, kill at {t_kill} (node {}),              writer stopped at {t_stop}\n",
            ids[leader]
        );
        for c in clients.iter() {
            let _ = writeln!(
                why,
                "    {} sched status: {}",
                c.name,
                sched_status(c).map_or_else(|e| format!("{e:#}"), |s| s["stats"].to_string())
            );
        }
        why.push_str(&journal_digest(endpoint, prefix));
        bail!("{why}");
    }
    ensure!(
        creations.first().is_some_and(|x| x.created >= started),
        "a snapshot predates the policy: {creations:?}"
    );
    let across: Vec<i64> = creations
        .windows(2)
        .filter(|w| w[0].created <= t_kill && w[1].created > t_kill)
        .map(|w| w[1].created - w[0].created)
        .collect();
    eprintln!(
        "    snapsched: {} creations ({} journal entries, {} unjournaled: {:?}), {} deletions \
         journaled, {} skipped; takeover gap {across:?} ms (allowed {MAX_GAP_MS}); gaps while \
         writing {:?}",
        creations.len(),
        audit.entries,
        extra.len(),
        extra.iter().map(|x| &x.name).collect::<Vec<_>>(),
        audit.deleted.len(),
        audit.skipped.len(),
        gaps(&while_writing)
    );

    // The grace window: nothing deleted before the first sighting's
    // window closed, which `skipped_grace` shows held real victims back.
    let state = read_state(endpoint, prefix)?;
    let (seen_at, until) = grace_window(&state, audit.ino, None)?;
    ensure!(
        until - seen_at == SNAPSCHED_GRACE_S as i64 * 1000,
        "the first sighting's window is {} ms, want {SNAPSCHED_GRACE_S} s",
        until - seen_at
    );
    ensure!(
        seen_at <= creations[0].created + EXPIRE_EVERY_S as i64 * 1000 + 5_000,
        "the root was first seen at {seen_at}, long after the first creation ({})",
        creations[0].created
    );
    ensure!(
        samples.skipped_grace > 0,
        "skipped_grace stayed 0: the {SNAPSCHED_GRACE_S} s window never held a victim back"
    );
    ensure!(
        t_holds < until,
        "the holds landed after the grace window closed"
    );
    let checked = check_deletions(&policy, &audit, &extra, &holds, until)?;
    ensure!(checked > 0, "nothing was deleted");
    eprintln!(
        "    snapsched: grace: first seen {seen_at}, window until +{} ms; first deletion at +{} \
         ms; {checked} deletions each expired by evaluate at their tick",
        until - seen_at,
        audit
            .deleted
            .first()
            .map_or(0, |(tick, _, _)| tick - seen_at)
    );

    // The survivors are evaluate's keep set — on both mounts, once the
    // last expiry run after the final write has landed.
    let mut table = String::new();
    eventually(
        "the survivors are retention::evaluate's keep set",
        FIXED_POINT,
        || {
            let audit = read_audit(endpoint, prefix)?;
            let (creations, _) = creation_list(&audit, &samples.seen)?;
            for c in clients.iter() {
                table = check_oracle(
                    &format!("oracle ({})", c.name),
                    &policy,
                    audit.ino,
                    &creations,
                    &holds,
                    &listed(c)?,
                )?;
            }
            Ok(())
        },
    )?;
    // It stays there: two more expiry periods delete nothing more.
    let journal_len = read_audit(endpoint, prefix)?.deleted.len();
    std::thread::sleep(Duration::from_secs(2 * EXPIRE_EVERY_S));
    let audit = read_audit(endpoint, prefix)?;
    ensure!(
        audit.deleted.len() == journal_len,
        "expiry kept deleting at the fixed point: {:?}",
        &audit.deleted[journal_len..]
    );
    let (creations, extra) = creation_list(&audit, &samples.seen)?;
    for c in clients.iter() {
        table = check_oracle(
            &format!("oracle ({})", c.name),
            &policy,
            audit.ino,
            &creations,
            &holds,
            &listed(c)?,
        )?;
    }
    check_deletions(&policy, &audit, &extra, &holds, until)?;
    eprint!("{table}");
    // The holds are not vacuous: without them, evaluate expires both
    // held snapshots, so the holds are what keeps them.
    let unheld = evaluate(
        &policy,
        audit.ino,
        &facts(audit.ino, &creations, &BTreeMap::new()),
    );
    for id in holds.keys() {
        let pos = creations
            .iter()
            .position(|x| &x.id == id)
            .with_context(|| format!("held snapshot {id} is not in the creation list"))?;
        ensure!(
            !unheld[pos].keep,
            "evaluate keeps the held {} even unheld ({}): the hold proves nothing",
            creations[pos].name,
            verdict_text(&unheld[pos])
        );
    }
    eprintln!(
        "    snapsched: unheld, evaluate expires both held snapshots ({}): the holds keep them",
        holds
            .keys()
            .filter_map(|id| creations.iter().find(|x| &x.id == id))
            .map(|x| x.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Both mounts list the same snapshots of /proj (auto, manual, held);
    // the manual and both held ones survive, still held as they were.
    let rows_b = proj_rows(&clients[1].snapshot_rows()?);
    let rows_a = proj_rows(&clients[0].snapshot_rows()?);
    ensure!(
        rows_a == rows_b,
        "the mounts list different snapshots:\na {rows_a:?}\nb {rows_b:?}"
    );
    ensure!(
        rows_b.iter().any(|(n, _, _)| n == "manual"),
        "the manual snapshot is gone: {rows_b:?}"
    );
    for (id, by) in &holds {
        ensure!(
            rows_b
                .iter()
                .any(|(_, i, h)| i == id && h.as_deref() == Some(by.as_str())),
            "held snapshot {id} (by {by:?}) is gone or no longer held so: {rows_b:?}"
        );
    }
    // Each survivor's frozen counter is consistent with its `created`
    // order (never backwards) and no newer than the writer — on both
    // mounts, the manual snapshot included.
    for c in clients.iter() {
        let mut prev = 0;
        for (name, _, _) in &rows_b {
            let v = frozen_counter(c, name)?;
            ensure!(
                v >= prev,
                "{}: {name} froze counter {v} after {prev}",
                c.name
            );
            prev = v;
        }
        ensure!(
            prev == last + 1,
            "{}: the newest snapshot froze {prev}, the last write was {}",
            c.name,
            last + 1
        );
    }
    eprintln!(
        "    snapsched: {} snapshots of /proj survive on both mounts ({} auto); writer errors {}",
        rows_b.len(),
        listed(&clients[1])?.len(),
        writer.errors.load(Ordering::Relaxed)
    );
    Ok(())
}

// --- snapsched-s3-outage -------------------------------------------------

/// `CONSTELLATION_SNAPSCHED_GRACE_S` in `snapsched-s3-outage`: short, so
/// expiry is deleting for real (the `10s:1m` tier's first victims appear
/// about 70 s in) before the cut, which makes "nothing expires during the
/// cut" a statement about a scheduler that was expiring.
const OUTAGE_GRACE_S: u64 = 20;

pub fn snapsched_s3_outage(seed: u64) -> Result<()> {
    const NAME: &str = "snapsched-s3-outage";
    let (env, root) = setup(NAME)?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("snapout-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    // The default lease TTL (60 s): the 90 s cut outlasts it, as a real
    // outage would.
    let mut c = tuned(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        None,
        OUTAGE_GRACE_S,
        EXPIRE_EVERY_S,
    );
    c.fs_create()?;
    c.mount()?;
    let result = outage_body(seed, &c, &proxy, &env.direct_endpoint, &prefix);
    let _ = proxy.heal();
    dump_logs_on_failure(NAME, std::slice::from_ref(&c), &result);
    let _ = c.unmount();
    result
}

fn outage_body(
    seed: u64,
    c: &Client,
    proxy: &crate::toxiproxy::Proxy<'_>,
    endpoint: &str,
    prefix: &str,
) -> Result<()> {
    const CUT: Duration = Duration::from_secs(90);
    let policy = parse_policy(POLICY)?;
    let dir = c.mnt.join("proj");
    std::fs::create_dir(&dir)?;
    bump(&dir, 0)?;
    let mut writer = Writer::start(seed, dir.clone(), 0);
    set_xattr(&dir, POLICY_XATTR, POLICY.as_bytes()).context("setting the policy")?;
    let mut seen: BTreeMap<String, Snap> = BTreeMap::new();
    let mut see = |snaps: &[Snap]| {
        for s in snaps {
            seen.insert(s.id.clone(), s.clone());
        }
    };

    // A healthy schedule first, expiring for real.
    eventually(
        "expiry deleting before the cut",
        Duration::from_secs(150),
        || {
            see(&polled(c)?);
            ensure!(
                stat(&sched_status(c)?, "expired") > 0,
                "nothing expired yet"
            );
            Ok(())
        },
    )?;
    let failed_before = stat(&sched_status(c)?, "create_failed");

    let t_cut = now_ms();
    proxy.cut()?;
    // The baseline is read once the cut is in place: an expiry run that
    // completed just before it is not "during the cut".
    let expired_before = stat(&sched_status(c)?, "expired");
    let pre = polled(c)?;
    see(&pre);
    let pre = names(&pre);
    // Nothing appears while S3 is gone. A batch already past its S3
    // writes when the cut landed may still show up: allow a snapshot
    // stamped within the first 2 s of the cut, nothing later. And nothing
    // expires: `expired` stays put, and every snapshot from before the
    // cut is still listed.
    let mut during: Vec<Snap> = Vec::new();
    let mut failed_during = failed_before;
    run_for(CUT, Duration::from_secs(5), || {
        let now = polled(c)?;
        let now_names = names(&now);
        let gone: Vec<&String> = pre.difference(&now_names).collect();
        ensure!(
            gone.is_empty(),
            "snapshots vanished during the cut: {gone:?}"
        );
        for s in now {
            if pre.contains(&s.name) || during.contains(&s) {
                continue;
            }
            ensure!(
                s.created < t_cut + 2_000,
                "{} (created {} ms into the cut) appeared during the cut",
                s.name,
                s.created - t_cut
            );
            during.push(s);
        }
        let status = sched_status(c)?;
        failed_during = stat(&status, "create_failed");
        let expired = stat(&status, "expired");
        ensure!(
            expired == expired_before,
            "expired moved during the cut ({expired_before} -> {expired})"
        );
        Ok(())
    })?;
    see(&during);
    let status = sched_status(c)?;
    ensure!(
        failed_during > failed_before,
        "create_failed did not rise during a {CUT:?} cut ({failed_before} -> {failed_during}): {status}"
    );
    ensure!(
        stat(&status, "expired") == expired_before,
        "expired moved during the cut: {status}"
    );
    eprintln!(
        "    snapsched-s3-outage: during the cut: create_failed {failed_before} -> {failed_during}, \
         expired {expired_before} -> {} (unchanged), {} snapshots before it all still listed, \
         last_error {}, in-flight snapshots {}",
        stat(&status, "expired"),
        pre.len(),
        status["stats"]["last_error"],
        during.len()
    );
    let t_heal = now_ms();
    proxy.heal()?;

    // One catch-up snapshot, then the schedule.
    let known: BTreeSet<String> = pre
        .iter()
        .cloned()
        .chain(during.iter().map(|s| s.name.clone()))
        .collect();
    let fresh = |c: &Client| -> Result<Vec<Snap>> {
        Ok(polled(c)?
            .into_iter()
            .filter(|s| !known.contains(&s.name) && s.created >= t_cut)
            .collect())
    };
    eventually(
        "a catch-up snapshot after the heal",
        Duration::from_secs(60),
        || {
            ensure!(!fresh(c)?.is_empty(), "none yet");
            Ok(())
        },
    )?;
    let t_first = now_ms();
    // Sample through the next 40 s: expiry may thin the new ones out
    // (the anchor moves past the cut), so collect every one seen.
    let mut after: BTreeMap<String, Snap> = BTreeMap::new();
    run_for(Duration::from_secs(40), Duration::from_secs(2), || {
        for s in fresh(c)? {
            after.insert(s.id.clone(), s);
        }
        Ok(())
    })?;
    let mut after: Vec<Snap> = after.into_values().collect();
    after.sort_by_key(|s| s.created);
    see(&after);
    let first = after.first().context("the catch-up vanished")?;
    eprintln!(
        "    snapsched-s3-outage: catch-up {} created {} ms after the heal (seen at +{} ms); then {:?}",
        first.name,
        first.created - t_heal,
        t_first - t_heal,
        after[1..].iter().map(|s| &s.name).collect::<Vec<_>>()
    );
    // Not a burst: no snapshot names a bucket the cut swallowed, i.e.
    // every one was taken in (or within a tick of) the bucket it names —
    // a backfilled bucket would be stamped up to 90 s after its start.
    for s in &after {
        ensure!(
            s.created - s.bucket < BUCKET_MS + TICK_MS + GAP_MARGIN_MS,
            "{} was created {} ms after its bucket began: a backfill, not a catch-up ({after:?})",
            s.name,
            s.created - s.bucket
        );
        ensure!(
            s.bucket + BUCKET_MS > t_heal - 2_000,
            "{} names a bucket that ended during the cut ({after:?})",
            s.name
        );
    }
    // Exactly one in the catch-up's own window: nothing else stamped
    // within a tick of it for the catch-up's bucket or an earlier one. The
    // next bucket's own snapshot may follow within a tick when the catch-up
    // lands in its bucket's last second (seen: created at :59.2, the next
    // one at :00.2); that is the schedule, not a burst. (This alone would
    // miss a backfill that makes one snapshot per tick; the real guard
    // against a backfill is the "names a bucket that ended during the cut"
    // check above.)
    let burst = after
        .iter()
        .filter(|s| s.created < first.created + TICK_MS && s.bucket <= first.bucket)
        .count();
    ensure!(burst == 1, "{burst} snapshots at the catch-up: {after:?}");
    // The schedule resumes: one per bucket for the 40 s after it. (Not
    // checked bucket by bucket: right after the heal the holder may execute
    // a snapshot late, into the next bucket — seen 12.3 s after its bucket
    // began — and that next bucket then counts as covered, by design.)
    ensure!(
        after.len() >= 4,
        "the schedule did not resume after the catch-up: {after:?}"
    );
    check_series(
        "after the outage",
        &after,
        BUCKET_MS + TICK_MS + GAP_MARGIN_MS,
    )?;
    // Expiry resumes too: the catch-up moved the anchor past the cut.
    let expired_after = stat(&sched_status(c)?, "expired");
    ensure!(
        expired_after > expired_before,
        "expiry did not resume after the heal (expired {expired_before} -> {expired_after})"
    );

    // The oracle, once the stream stops.
    let (_, _) = quiesce(&writer, c)?;
    see(&polled(c)?);
    writer.stop();
    let mut table = String::new();
    let mut audit = None;
    eventually(
        "the survivors are retention::evaluate's keep set",
        FIXED_POINT,
        || {
            let a = read_audit(endpoint, prefix)?;
            let (creations, extra) = creation_list(&a, &seen)?;
            // Only a snapshot in flight when the cut landed may have lost its
            // audit object (the journal write failed with S3).
            for x in &extra {
                ensure!(
                    during.iter().any(|d| d.id == x.id),
                    "{} exists but no journal entry records its creation",
                    x.name
                );
            }
            table = check_oracle(
                "oracle",
                &policy,
                a.ino,
                &creations,
                &BTreeMap::new(),
                &listed(c)?,
            )?;
            audit = Some((a, extra));
            Ok(())
        },
    )?;
    let (audit, extra) = audit.expect("set by the oracle check");
    let state = read_state(endpoint, prefix)?;
    let (_, until) = grace_window(&state, audit.ino, None)?;
    let checked = check_deletions(&policy, &audit, &extra, &BTreeMap::new(), until)?;
    let in_cut: Vec<&Snap> = audit
        .deleted
        .iter()
        .filter(|(tick, _, _)| *tick >= t_cut && *tick < t_heal)
        .map(|(_, s, _)| s)
        .collect();
    ensure!(
        in_cut.is_empty(),
        "the journal records deletions by ticks during the cut: {in_cut:?}"
    );
    eprint!("{table}");
    eprintln!(
        "    snapsched-s3-outage: {checked} deletions, each expired by evaluate at its tick, none \
         during the cut; expired {expired_before} before the cut, {expired_after} 40 s after the \
         heal"
    );
    Ok(())
}

// --- snapsched-grace -------------------------------------------------------

/// `snapsched-grace`'s policies: about ten 10 s snapshots under the long
/// one keep everything; the short one keeps six buckets (and `last=1`).
const GRACE_LONG: &str = "10s:5m";
const GRACE_SHORT: &str = "10s:1m";
/// `CONSTELLATION_SNAPSCHED_GRACE_S` in `snapsched-grace`: long enough to
/// watch (several expiry runs, every 5 s, find victims and keep them), short
/// enough for a short scenario. The first sighting's window (also 30 s)
/// closes long before the change, and `10s:5m` expires nothing in the
/// ~100 s before it, so the only window that matters is the change's.
const GRACE_GRACE_S: u64 = 30;
const GRACE_EXPIRE_EVERY_S: u64 = 5;

pub fn snapsched_grace(seed: u64) -> Result<()> {
    const NAME: &str = "snapsched-grace";
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("snapgrace-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = tuned(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        None,
        GRACE_GRACE_S,
        GRACE_EXPIRE_EVERY_S,
    );
    c.fs_create()?;
    c.mount()?;
    let result = grace_body(seed, &c, &env.direct_endpoint, &prefix);
    dump_logs_on_failure(NAME, std::slice::from_ref(&c), &result);
    let _ = c.unmount();
    result
}

fn grace_body(seed: u64, c: &Client, endpoint: &str, prefix: &str) -> Result<()> {
    let long = parse_policy(GRACE_LONG)?;
    let short = parse_policy(GRACE_SHORT)?;
    let dir = c.mnt.join("proj");
    std::fs::create_dir(&dir)?;
    bump(&dir, 0)?;
    let mut writer = Writer::start(seed, dir.clone(), 0);
    set_xattr(&dir, POLICY_XATTR, GRACE_LONG.as_bytes()).context("setting the policy")?;
    let mut seen: BTreeMap<String, Snap> = BTreeMap::new();
    eventually("ten snapshots", Duration::from_secs(150), || {
        let snaps = polled(c)?;
        ensure!(snaps.len() >= 10, "{} so far", snaps.len());
        Ok(())
    })?;
    let status = sched_status(c)?;
    ensure!(
        stat(&status, "expired") == 0,
        "{GRACE_LONG} expired something: {status}"
    );
    let skipped_grace_before = stat(&status, "skipped_grace");
    let before = polled(c)?;
    for s in &before {
        seen.insert(s.id.clone(), s.clone());
    }
    let oldest = before
        .iter()
        .min_by_key(|s| s.created)
        .cloned()
        .context("no snapshot")?;
    let before = names(&before);

    // Shorten it, as `setfattr` would. The next expiry run records the
    // change and its window in state.json.
    let t_change = now_ms();
    set_xattr(&dir, POLICY_XATTR, GRACE_SHORT.as_bytes()).context("shortening the policy")?;
    let mut window = (0, 0);
    eventually(
        "state.json records the change",
        Duration::from_secs(30),
        || {
            let state = read_state(endpoint, prefix)?;
            let ino = *state.roots.keys().next().context("no root yet")?;
            ensure!(
                state.roots[&ino].canonical == short.to_string(),
                "still {:?}",
                state.roots[&ino].canonical
            );
            window = grace_window(&state, ino, Some(&long.to_string()))?;
            Ok(())
        },
    )?;
    let (replaced, until) = window;
    ensure!(
        until - replaced == GRACE_GRACE_S as i64 * 1000,
        "the change's window is {} ms, want {GRACE_GRACE_S} s",
        until - replaced
    );
    ensure!(
        replaced >= t_change - 1_000
            && replaced <= t_change + 2 * GRACE_EXPIRE_EVERY_S as i64 * 1000,
        "the change was dated {replaced}, {} ms from the setxattr",
        replaced - t_change
    );

    // Inside the window, hold the oldest snapshot: one the new policy
    // expires (checked over what exists now), so only the hold can keep
    // it once the window closes.
    let mut holds: BTreeMap<String, String> = BTreeMap::new();
    {
        let mut now: Vec<Snap> = polled(c)?;
        now.sort_by_key(|s| s.created);
        let verdicts = evaluate(&short, 0, &facts(0, &now, &holds));
        let pos = now
            .iter()
            .position(|s| s.id == oldest.id)
            .context("the oldest snapshot is gone before the hold")?;
        ensure!(
            !verdicts[pos].keep,
            "{GRACE_SHORT} keeps {} ({}): holding it would prove nothing",
            oldest.name,
            verdict_text(&verdicts[pos])
        );
        let (ok, stdout, stderr) = c.snapshot_cli(&["hold", oldest.id.as_str()])?;
        ensure!(ok, "holding {}: {stdout}{stderr}", oldest.name);
        holds.insert(oldest.id.clone(), String::new());
        ensure!(
            now_ms() < until - 5_000,
            "the hold landed {} ms before the window closed: too close to rule out a race",
            until - now_ms()
        );
    }
    eprintln!(
        "    snapsched-grace: held {} inside the window ({} ms before it closes); {GRACE_SHORT} \
         expires it unheld",
        oldest.name,
        until - now_ms()
    );

    // Inside the window: nothing is deleted, though the new policy wants
    // to (skipped_grace rises).
    let expired_at_change = stat(&sched_status(c)?, "expired");
    while now_ms() < until - 1_000 {
        std::thread::sleep(Duration::from_secs(1));
        let now = polled(c)?;
        for s in &now {
            seen.insert(s.id.clone(), s.clone());
        }
        let now_names = names(&now);
        let gone: Vec<&String> = before.difference(&now_names).collect();
        ensure!(
            gone.is_empty(),
            "{} ms into the grace window, snapshots vanished: {gone:?}",
            now_ms() - replaced
        );
        let expired = stat(&sched_status(c)?, "expired");
        ensure!(
            expired == expired_at_change,
            "expired moved inside the grace window ({expired_at_change} -> {expired})"
        );
    }
    let status = sched_status(c)?;
    let skipped_grace = stat(&status, "skipped_grace");
    ensure!(
        skipped_grace > skipped_grace_before,
        "skipped_grace did not rise ({skipped_grace_before} -> {skipped_grace}): {GRACE_SHORT} \
         wanted nothing deleted, the window held nothing back"
    );
    eprintln!(
        "    snapsched-grace: {} snapshots under {GRACE_LONG}; changed to {GRACE_SHORT} at \
         {t_change}, window {replaced}..{until}; nothing deleted inside it, skipped_grace \
         {skipped_grace_before} -> {skipped_grace}",
        before.len()
    );

    // After it: deletions start within an expiry period (and a few
    // ticks), and the survivors become exactly evaluate's keep set under
    // the new policy.
    eventually(
        "expiry after the window",
        Duration::from_secs(GRACE_EXPIRE_EVERY_S + 15),
        || {
            for s in polled(c)? {
                seen.insert(s.id.clone(), s);
            }
            ensure!(
                stat(&sched_status(c)?, "expired") > expired_at_change,
                "nothing expired yet"
            );
            Ok(())
        },
    )?;
    let t_expired = now_ms();
    quiesce(&writer, c)?;
    for s in polled(c)? {
        seen.insert(s.id.clone(), s);
    }
    writer.stop();
    let mut table = String::new();
    let mut audit = None;
    eventually(
        "the survivors are retention::evaluate's keep set",
        Duration::from_secs(GRACE_EXPIRE_EVERY_S + 20),
        || {
            let a = read_audit(endpoint, prefix)?;
            let (creations, extra) = creation_list(&a, &seen)?;
            ensure!(extra.is_empty(), "unjournaled snapshots: {extra:?}");
            table = check_oracle("oracle", &short, a.ino, &creations, &holds, &listed(c)?)?;
            audit = Some(a);
            Ok(())
        },
    )?;
    let audit = audit.expect("set by the oracle check");
    ensure!(
        audit.policies == [long.to_string(), short.to_string()],
        "the journal carries the policies {:?}",
        audit.policies
    );
    let checked = check_deletions(&short, &audit, &[], &holds, until)?;
    let rows = proj_rows(&c.snapshot_rows()?);
    ensure!(
        rows.iter()
            .any(|(_, id, by)| *id == oldest.id && by.as_deref() == Some("")),
        "the held {} is gone or no longer held: {rows:?}",
        oldest.name
    );
    let first = audit.deleted.first().map_or(0, |(tick, _, _)| *tick);
    eprint!("{table}");
    eprintln!(
        "    snapsched-grace: {checked} deletions, the first by the tick of +{} ms after the \
         window closed (seen expired {} ms after it)",
        first - until,
        t_expired - until
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_names_parse_to_their_bucket() {
        // 2026-10-02T12:00:10Z
        assert_eq!(
            auto_bucket("auto-20261002T120010Z"),
            Some(1_790_942_410_000)
        );
        assert_eq!(auto_bucket("auto-19700101T000000Z"), Some(0));
        assert_eq!(
            auto_bucket("auto-20240229T235950Z"),
            Some(1_709_251_190_000)
        );
        assert_eq!(auto_bucket("auto-20261002T1200Z"), None);
        assert_eq!(auto_bucket("manual"), None);
    }

    fn snap(name: &str, created: i64) -> Snap {
        Snap {
            name: name.into(),
            id: name.into(),
            created,
            bucket: auto_bucket(name).unwrap(),
        }
    }

    #[test]
    fn the_series_check_catches_gaps_and_early_stamps() {
        let base = auto_bucket("auto-20261002T120000Z").unwrap();
        let ok = [
            snap("auto-20261002T120000Z", base + 300),
            snap("auto-20261002T120010Z", base + 10_200),
        ];
        check_series("t", &ok, MAX_GAP_MS).unwrap();
        let early = [snap("auto-20261002T120010Z", base + 9_000)];
        assert!(check_series("t", &early, MAX_GAP_MS).is_err());
        let gap = [
            snap("auto-20261002T120000Z", base + 300),
            snap("auto-20261002T120100Z", base + 60_000),
        ];
        assert!(check_series("t", &gap, MAX_GAP_MS).is_err());
    }

    #[test]
    fn the_consecutive_check_catches_a_dropped_bucket() {
        let base = auto_bucket("auto-20261002T120000Z").unwrap();
        let a = snap("auto-20261002T120000Z", base + 300);
        let b = snap("auto-20261002T120010Z", base + 10_300);
        let c = snap("auto-20261002T120020Z", base + 20_300);
        let d = snap("auto-20261002T120040Z", base + 40_300);
        check_consecutive("t", &[a.clone(), b.clone(), c.clone()], None).unwrap();
        // 12:00:30 is missing: fails in steady state ...
        assert!(
            check_consecutive("t", &[a.clone(), b.clone(), c.clone(), d.clone()], None).is_err()
        );
        // ... is the one allowed miss when the kill falls inside it ...
        check_consecutive("t", &[b.clone(), c.clone(), d.clone()], Some(base + 30_000)).unwrap();
        // ... but not when the kill falls elsewhere, or for two misses.
        assert!(
            check_consecutive("t", &[b.clone(), c.clone(), d.clone()], Some(base + 5_000)).is_err()
        );
        assert!(check_consecutive("t", &[a, d], Some(base + 20_000)).is_err());
    }

    /// Plan 32 Step 3.3: a batch that lands after its bucket ended covers
    /// the next bucket (by `created`), which then gets no snapshot of its
    /// own name — not a drop. A bucket covered by neither is.
    #[test]
    fn a_late_snapshot_covers_the_next_bucket_and_nothing_more() {
        let base = auto_bucket("auto-20261002T120000Z").unwrap();
        let a = snap("auto-20261002T120000Z", base + 300);
        // Asked for 12:00:10, landed at 12:00:20.7 (a 10 s batch).
        let late = snap("auto-20261002T120010Z", base + 20_700);
        let c = snap("auto-20261002T120030Z", base + 30_300);
        let d = snap("auto-20261002T120050Z", base + 50_300);
        check_consecutive("t", &[a.clone(), late.clone(), c.clone()], None).unwrap();
        // 12:00:40 is covered by nothing.
        assert!(check_consecutive("t", &[a.clone(), late.clone(), c.clone(), d], None).is_err());
        // On time, the same names leave 12:00:20 uncovered.
        let on_time = snap("auto-20261002T120010Z", base + 10_300);
        assert!(check_consecutive("t", &[a.clone(), on_time, c.clone()], None).is_err());
        // Two snapshots in one bucket's name still fail.
        let twice = snap("auto-20261002T120000Z", base + 900);
        assert!(check_consecutive("t", &[a, twice], None).is_err());
    }

    /// A stream of 10 s snapshots, one per bucket from 12:00:00, created
    /// 300 ms into each.
    fn stream(n: usize) -> Vec<Snap> {
        let base = auto_bucket("auto-20261002T120000Z").unwrap();
        (0..n)
            .map(|i| {
                let t = base + i as i64 * BUCKET_MS;
                let secs = t / 1000 % 86_400;
                let name = format!(
                    "auto-20261002T{:02}{:02}{:02}Z",
                    secs / 3600,
                    secs / 60 % 60,
                    secs % 60
                );
                snap(&name, t + 300)
            })
            .collect()
    }

    fn audit_of(created: &[Snap], deleted: &[(i64, &Snap)]) -> Audit {
        Audit {
            ino: 7,
            policies: vec![GRACE_SHORT.into()],
            created: created
                .iter()
                .map(|s| (s.created - 300, s.clone()))
                .collect(),
            deleted: deleted
                .iter()
                .map(|(t, s)| (*t, (*s).clone(), Some(EXPIRY_REASON.to_string())))
                .collect(),
            skipped: Vec::new(),
            entries: created.len(),
        }
    }

    #[test]
    fn the_oracle_is_evaluates_keep_set() {
        // `10s:1m` keeps the anchor's bucket and the five before it.
        let policy = parse_policy(GRACE_SHORT).unwrap();
        let all = stream(9);
        let survivors = all[3..].to_vec();
        check_oracle("t", &policy, 7, &all, &BTreeMap::new(), &survivors).unwrap();
        // One too many survivors, one too few, or a stranger: all fail.
        assert!(check_oracle("t", &policy, 7, &all, &BTreeMap::new(), &all[2..]).is_err());
        assert!(check_oracle("t", &policy, 7, &all, &BTreeMap::new(), &all[4..]).is_err());
        assert!(check_oracle("t", &policy, 7, &all[1..], &BTreeMap::new(), &all[..]).is_err());
        // A held snapshot is kept, and does not count against the others.
        let holds = BTreeMap::from([(all[0].id.clone(), CSI_OWNER.to_string())]);
        let mut with_held = vec![all[0].clone()];
        with_held.extend_from_slice(&all[3..]);
        let table = check_oracle("t", &policy, 7, &all, &holds, &with_held).unwrap();
        assert!(table.contains("held: csi:test-uid"), "{table}");
    }

    #[test]
    fn the_deletion_check_judges_each_run_by_its_own_past() {
        let policy = parse_policy(GRACE_SHORT).unwrap();
        let all = stream(9);
        // The tick that created the 7th snapshot may delete the 1st (seven
        // buckets: the oldest left the window), not the 2nd.
        let tick7 = all[6].created - 300;
        let ok = audit_of(&all, &[(tick7, &all[0])]);
        assert_eq!(
            check_deletions(&policy, &ok, &[], &BTreeMap::new(), 0).unwrap(),
            1
        );
        let early = audit_of(&all, &[(tick7, &all[1])]);
        assert!(check_deletions(&policy, &early, &[], &BTreeMap::new(), 0).is_err());
        // Before the grace window closed, or of a held one: fails.
        assert!(check_deletions(&policy, &ok, &[], &BTreeMap::new(), tick7 + 1).is_err());
        let holds = BTreeMap::from([(all[0].id.clone(), String::new())]);
        assert!(check_deletions(&policy, &ok, &[], &holds, 0).is_err());
    }
}
