//! Plan 32 M3: automatic snapshot creation, end to end (Step 11's
//! `snapsched` and `snapsched-s3-outage`, the creation half; M4 extends
//! both with retention assertions).
//!
//! A policy `10s:1m 1m:4m; last=2` sits on `/proj` (set by `setxattr`, the
//! way an operator's `setfattr` would), the scheduler ticks every second
//! (`CONSTELLATION_SNAPSCHED_TICK_MS=1000`), and a seeded writer renames a
//! fresh counter value into `/proj/counter` about once a second, so every
//! 10 s bucket has a change to snapshot (`skip-empty` is on by default).
//!
//! **`snapsched-create`** (two nodes; `b` creates the filesystem, holds
//! the root lease and runs the writer; `a` binds the policy and takes the
//! scheduler's lead with `snapshot sched run`, so snapshots are created
//! through the holder-side batch, forwarded from `a` to `b` over P2P)
//! runs three minutes and asserts what the plan's
//! Step 0.1 and 3.3 promise: both mounts list the same snapshots; every
//! name is `auto-<UTC>` of its own 10 s bucket; the counter frozen in each
//! snapshot never goes backwards in `created` order; and the root lease's
//! holder and epoch are the same before and after (creation runs at the
//! holder, it never takes the lease). Then it `kill -9`s the scheduler's
//! leader — whichever node `snapshot sched status` names — right after a
//! snapshot lands, and asserts that no bucket gets two snapshots and that
//! no gap between consecutive snapshots exceeds [`MAX_GAP_MS`]. Last,
//! skip-empty: with the writer stopped for 40 s no snapshot appears, and
//! one write gives exactly one more.
//!
//! **`snapsched-s3-outage`** (one node) cuts S3 for 90 s: the scheduler's
//! `create_failed` rises and no snapshot appears during the cut; after the
//! heal exactly one catch-up snapshot appears, named for the bucket it was
//! taken in (no backfill of the nine missed buckets), and the schedule
//! resumes at one per bucket.
//!
//! Neither scenario sees a snapshot deleted: the auto set only grows. M4
//! (expiry) changes that assertion: `last=2` and the `1m:4m` tier then
//! bound the set.

use super::m11::dump_logs_on_failure;
use super::m9::node_id;
use super::{eventually, set_xattr, setup, ts};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{bail, ensure, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const POLICY: &str = "10s:1m 1m:4m; last=2";
const POLICY_XATTR: &str = "user.constellation.snapshots";
/// The policy's finest tier: one snapshot per bucket.
const BUCKET_MS: i64 = 10_000;
const TICK_MS: i64 = 1_000;
/// `CONSTELLATION_LEASE_TTL_MS` for `snapsched-create`: both the root lease
/// and the scheduler's `_snapsched` lease (which is at least three ticks).
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
const GAP_MARGIN_MS: i64 = 5_000;
/// The largest gap allowed between consecutive auto snapshots, anywhere in
/// the run (a steady-state gap is one bucket plus jitter, well below it).
const MAX_GAP_MS: i64 = TTL_MS + TICK_MS + GAP_MARGIN_MS;

fn tuned(c: Client, ttl_ms: Option<i64>) -> Client {
    let c = c.with_env("CONSTELLATION_SNAPSCHED_TICK_MS", &TICK_MS.to_string());
    match ttl_ms {
        Some(ttl) => c.with_env("CONSTELLATION_LEASE_TTL_MS", &ttl.to_string()),
        None => c,
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn bucket_of(ms: i64) -> i64 {
    ms.div_euclid(BUCKET_MS) * BUCKET_MS
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
struct Snap {
    name: String,
    id: String,
    created: i64,
    bucket: i64,
}

fn auto_snaps(rows: &[serde_json::Value]) -> Result<Vec<Snap>> {
    let mut out = Vec::new();
    for row in rows {
        if row["path"] != "/proj" || row["origin"] != "auto" {
            continue;
        }
        let name = row["name"].as_str().unwrap_or_default().to_string();
        let bucket =
            auto_bucket(&name).with_context(|| format!("{name:?} is not an auto-<UTC> name"))?;
        out.push(Snap {
            id: row["id"].as_str().unwrap_or_default().to_string(),
            created: row["created_unix_ms"].as_i64().unwrap_or_default(),
            name,
            bucket,
        });
    }
    out.sort_by_key(|s| (s.created, s.bucket));
    Ok(out)
}

/// `/proj`'s auto snapshots through `snapshot ls / --json` (the CLI).
fn listed(c: &Client) -> Result<Vec<Snap>> {
    auto_snaps(&c.snapshot_rows()?)
}

/// The same, straight from `snapshot.list` without sizes: cheap enough to
/// poll (no accounting work).
fn polled(c: &Client) -> Result<Vec<Snap>> {
    let listing = c.control_call(
        "snapshot.list",
        serde_json::json!({"path": null, "sizes": false}),
        Duration::from_secs(20),
    )?;
    auto_snaps(
        listing["snapshots"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default(),
    )
}

/// `snapshot sched status --json` (the CLI).
fn sched_status(c: &Client) -> Result<serde_json::Value> {
    let (ok, stdout, stderr) = c.snapshot_cli(&["sched", "status", "--json"])?;
    ensure!(
        ok,
        "{}: snapshot sched status failed: {stdout}{stderr}",
        c.name
    );
    serde_json::from_str(&stdout).with_context(|| format!("parsing sched status: {stdout}"))
}

fn is_leader(c: &Client) -> Result<bool> {
    Ok(sched_status(c)?["stats"]["leader"] == true)
}

fn stat(status: &serde_json::Value, key: &str) -> u64 {
    status["stats"][key].as_u64().unwrap_or(0)
}

/// The root lease as `c` reports it: `(holder, epoch)`.
fn root_lease(c: &Client) -> Result<(u64, u64)> {
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

/// While the writer runs every bucket has a change, so consecutive
/// snapshots (in `created` order) must name consecutive buckets. The one
/// exception is the pair that straddles the kill (`t_kill`): the takeover may
/// miss at most one bucket (a gap of two) — callers pass `Some` only where
/// that is legitimate (the writer died with the leader). Two misses, or a miss
/// anywhere else, fail.
fn check_consecutive(what: &str, snaps: &[Snap], t_kill: Option<i64>) -> Result<()> {
    for w in snaps.windows(2) {
        let d = w[1].bucket - w[0].bucket;
        let straddles = t_kill.is_some_and(|k| w[0].created <= k && w[1].created > k);
        let allowed = if straddles { 2 * BUCKET_MS } else { BUCKET_MS };
        ensure!(
            d >= BUCKET_MS && d <= allowed,
            "{what}: {} then {} is {d} ms of buckets apart (allowed {allowed}{}): a bucket \
             was dropped (or duplicated) while the writer ran",
            w[0].name,
            w[1].name,
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

/// Sleep `total`, failing early if `check` does (every 5 s).
fn run_for(total: Duration, mut check: impl FnMut() -> Result<()>) -> Result<()> {
    let end = Instant::now() + total;
    while Instant::now() < end {
        std::thread::sleep((end - Instant::now()).min(Duration::from_secs(5)));
        check()?;
    }
    Ok(())
}

pub fn snapsched_create(seed: u64) -> Result<()> {
    const NAME: &str = "snapsched-create";
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/snapsched-{}", ts());
    // Own node keys: a leader that is not the root lease holder reaches
    // it over P2P (snapshot batches have no S3 slow path).
    let mut a = tuned(
        Client::new(root.path(), "a", &env.endpoint, &backend)?.with_own_node_key(),
        Some(TTL_MS),
    );
    let mut b = tuned(
        Client::new(root.path(), "b", &env.endpoint, &backend)?.with_own_node_key(),
        Some(TTL_MS),
    );
    // b creates the filesystem and mounts first: it takes the root lease
    // at mount and keeps it, as the writer and the only node that writes.
    b.fs_create()?;
    b.mount()?;
    a.mount()?;
    let mut clients = [a, b];
    let result = create_body(seed, &mut clients);
    dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        if c.is_mounted() {
            let _ = c.unmount();
        }
    }
    result
}

fn create_body(seed: u64, clients: &mut [Client; 2]) -> Result<()> {
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
        "    snapsched-create: root lease before: holder {} epoch {}",
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
        "    snapsched-create: `snapshot sched run` on a: {}",
        out.trim().replace('\n', " | ")
    );
    eventually("a scheduler leads", Duration::from_secs(30), || {
        ensure!(
            is_leader(&clients[0])? || is_leader(&clients[1])?,
            "nobody leads yet"
        );
        Ok(())
    })?;

    // Three minutes of schedule. Exactly one node leads throughout, and
    // the snapshot set only grows.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    run_for(Duration::from_secs(180), || {
        let mut leading = Vec::new();
        for (c, id) in clients.iter().zip(ids) {
            if is_leader(c)? {
                leading.push(id);
            }
        }
        ensure!(
            leading.len() <= 1,
            "two scheduler leaders at once: {leading:?}"
        );
        if let Some(id) = leading.first() {
            *leaders.entry(*id).or_default() += 1;
        }
        let now: BTreeSet<String> = polled(&clients[1])?.into_iter().map(|s| s.name).collect();
        // Nothing is deleted in M3; M4's expiry makes this `last=2` +
        // `1m:4m` bound the set instead.
        ensure!(
            seen.is_subset(&now),
            "a snapshot vanished: had {seen:?}, now {now:?}"
        );
        seen = now;
        Ok(())
    })?;
    eprintln!("    snapsched-create: leader samples {leaders:?}");

    let lease_after = root_lease(&clients[1])?;
    eprintln!(
        "    snapsched-create: root lease after:  holder {} epoch {}",
        lease_after.0, lease_after.1
    );
    ensure!(
        lease_after == lease_before,
        "creating snapshots moved the root lease: (holder, epoch) {lease_before:?} -> {lease_after:?}"
    );

    // Both mounts list the same snapshots (the rows replicate).
    let mut on_b = Vec::new();
    eventually(
        "both nodes list the same snapshots",
        Duration::from_secs(30),
        || {
            on_b = listed(&clients[1])?;
            let on_a = listed(&clients[0])?;
            let ids = |s: &[Snap]| {
                s.iter()
                    .map(|s| (s.name.clone(), s.id.clone()))
                    .collect::<BTreeSet<_>>()
            };
            ensure!(
                ids(&on_a) == ids(&on_b),
                "a lists {on_a:?}\nb lists {on_b:?}"
            );
            Ok(())
        },
    )?;
    ensure!(
        on_b.first().is_some_and(|s| s.created >= started),
        "a snapshot predates the policy: {on_b:?}"
    );
    // Three minutes of 10 s buckets, minus the tail the replicas have not
    // caught up with: at least 15 of 18.
    ensure!(
        on_b.len() >= 15,
        "only {} snapshots in 180 s: {on_b:?}",
        on_b.len()
    );
    check_series("schedule", &on_b, MAX_GAP_MS)?;
    check_consecutive("schedule", &on_b, None)?;
    eprintln!(
        "    snapsched-create: {} snapshots, gaps (ms) {:?}",
        on_b.len(),
        gaps(&on_b)
    );

    // The counter frozen in each snapshot never goes backwards in created
    // order, and is no newer than what the writer had written — read
    // through both mounts.
    for c in clients.iter() {
        let mut prev = 0;
        for s in &on_b {
            let v = frozen_counter(c, &s.name)?;
            ensure!(
                v >= prev,
                "{}: {} froze counter {v} after {prev}",
                c.name,
                s.name
            );
            prev = v;
        }
        ensure!(
            prev <= writer.value(),
            "{}: counter {prev} ahead of the writer's {}",
            c.name,
            writer.value()
        );
    }

    // Kill the leader right after a snapshot lands, so the gap measured
    // across the takeover is (almost) all takeover.
    let leader = match (is_leader(&clients[0])?, is_leader(&clients[1])?) {
        (true, false) => 0,
        (false, true) => 1,
        other => bail!("expected exactly one scheduler leader, got (a, b) = {other:?}"),
    };
    let survivor = 1 - leader;
    let before_kill = polled(&clients[survivor])?.len();
    eventually(
        "a fresh snapshot to kill after",
        Duration::from_secs(30),
        || {
            ensure!(polled(&clients[survivor])?.len() > before_kill, "none yet");
            Ok(())
        },
    )?;
    let t_kill = now_ms();
    clients[leader].kill9()?;
    if leader == 1 {
        eprintln!(
            "    snapsched-create: NOTE the killed leader is b, the root lease holder and \
             the writer: a must take over the root lease AND the scheduler (the rarer path)"
        );
        // b was also the writer (and the root lease holder): write
        // through a now, which takes the root lease once b's lapses.
        writer.retarget(Some(clients[0].mnt.join("proj")));
    }
    eprintln!(
        "    snapsched-create: RESULT killed leader = {} (a led before the kill: {}; node {}){}",
        clients[leader].name,
        leader == 0,
        ids[leader],
        if leader == 1 {
            ", also the root lease holder and the writer"
        } else {
            ""
        }
    );
    let s = &clients[survivor];
    eventually(
        "the survivor leads",
        Duration::from_millis((TTL_MS + 5 * TICK_MS + 10_000) as u64),
        || {
            ensure!(is_leader(s)?, "not yet");
            Ok(())
        },
    )?;
    // A minute of schedule under the new leader.
    std::thread::sleep(Duration::from_secs(60));
    let after = listed(s)?;
    let had: BTreeSet<_> = on_b.iter().map(|s| &s.name).collect();
    let has: BTreeSet<_> = after.iter().map(|s| &s.name).collect();
    ensure!(
        had.is_subset(&has),
        "a snapshot vanished across the takeover"
    );
    check_series("takeover", &after, MAX_GAP_MS)?;
    // The one allowed miss exists only when the writer died with the leader
    // (b): its writes stall until a holds the root lease, so a bucket can be
    // legitimately empty. With a killed, the writer never stops and the new
    // leader's first bucket (within the 16 s bound) must be the next one.
    check_consecutive("takeover", &after, (leader == 1).then_some(t_kill))?;
    let across: Vec<i64> = after
        .windows(2)
        .filter(|w| w[0].created <= t_kill && w[1].created > t_kill)
        .map(|w| w[1].created - w[0].created)
        .collect();
    let post = after.iter().filter(|x| x.created > t_kill).count();
    ensure!(
        post >= 4,
        "only {post} snapshots in the minute after the takeover: {after:?}"
    );
    eprintln!(
        "    snapsched-create: takeover gap {across:?} ms (allowed {MAX_GAP_MS}); \
         {post} snapshots since; writer errors {}",
        writer.errors.load(Ordering::Relaxed)
    );

    // Skip-empty: stop writing; once the last value is frozen in a
    // snapshot, 40 s pass without another.
    writer.retarget(None);
    let last = writer.value();
    let s_dir = s.mnt.join("proj");
    eventually(
        "the last write is snapshotted",
        Duration::from_secs(40),
        || {
            let snaps = polled(s)?;
            let newest = snaps.last().context("no snapshot")?;
            let v = frozen_counter(s, &newest.name)?;
            ensure!(v == last, "newest {} froze {v}, want {last}", newest.name);
            Ok(())
        },
    )?;
    let idle_from = polled(s)?;
    let skipped_before = stat(&sched_status(s)?, "skipped_empty");
    std::thread::sleep(Duration::from_secs(40));
    let idle_to = polled(s)?;
    ensure!(
        idle_to == idle_from,
        "an idle /proj got snapshots: {:?}",
        &idle_to[idle_from.len().min(idle_to.len())..]
    );
    // Non-vacuity: the leader did ask, and the holder answered unchanged.
    let skipped_after = stat(&sched_status(s)?, "skipped_empty");
    ensure!(
        skipped_after > skipped_before,
        "skipped_empty did not move ({skipped_before} -> {skipped_after}): the idle buckets were never asked about"
    );

    // One write: exactly one snapshot, of that value, in the bucket of
    // the write or the next (when the write lands at a bucket's end).
    let t_write = now_ms();
    bump(&s_dir, last + 1)?;
    eventually("the write is snapshotted", Duration::from_secs(30), || {
        ensure!(polled(s)?.len() > idle_to.len(), "not yet");
        Ok(())
    })?;
    std::thread::sleep(Duration::from_millis((3 * BUCKET_MS) as u64));
    let end = listed(s)?;
    ensure!(
        end.len() == idle_to.len() + 1,
        "one write gave {} snapshots: {:?}",
        end.len() - idle_to.len(),
        &end[idle_to.len()..]
    );
    let new = end.last().unwrap();
    ensure!(
        new.bucket == bucket_of(t_write) || new.bucket == bucket_of(t_write) + BUCKET_MS,
        "the write at {t_write} was snapshotted as {} (bucket {})",
        new.name,
        new.bucket
    );
    ensure!(
        frozen_counter(s, &new.name)? == last + 1,
        "{} froze the wrong value",
        new.name
    );
    eprintln!(
        "    snapsched-create: skip-empty: 40 s idle, skipped_empty {skipped_before} -> {skipped_after}; \
         one write -> {} ({} ms after it)",
        new.name,
        new.created - t_write
    );
    writer.stop();
    Ok(())
}

pub fn snapsched_s3_outage(seed: u64) -> Result<()> {
    const NAME: &str = "snapsched-s3-outage";
    let (env, root) = setup(NAME)?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/snapout-{}", ts());
    // The default lease TTL (60 s): the 90 s cut outlasts it, as a real
    // outage would.
    let mut c = tuned(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        None,
    );
    c.fs_create()?;
    c.mount()?;
    let result = outage_body(seed, &c, &proxy);
    let _ = proxy.heal();
    dump_logs_on_failure(NAME, std::slice::from_ref(&c), &result);
    let _ = c.unmount();
    result
}

fn outage_body(seed: u64, c: &Client, proxy: &crate::toxiproxy::Proxy<'_>) -> Result<()> {
    const CUT: Duration = Duration::from_secs(90);
    let dir = c.mnt.join("proj");
    std::fs::create_dir(&dir)?;
    bump(&dir, 0)?;
    let mut writer = Writer::start(seed, dir.clone(), 0);
    set_xattr(&dir, POLICY_XATTR, POLICY.as_bytes()).context("setting the policy")?;

    // A healthy schedule first.
    eventually(
        "three snapshots before the cut",
        Duration::from_secs(60),
        || {
            ensure!(polled(c)?.len() >= 3, "not yet");
            Ok(())
        },
    )?;
    let failed_before = stat(&sched_status(c)?, "create_failed");
    let pre: BTreeSet<String> = polled(c)?.into_iter().map(|s| s.name).collect();

    let t_cut = now_ms();
    proxy.cut()?;
    // Nothing appears while S3 is gone. A batch already past its S3
    // writes when the cut landed may still show up: allow a snapshot
    // stamped within the first 2 s of the cut, nothing later.
    let mut during: Vec<Snap> = Vec::new();
    let mut failed_during = failed_before;
    run_for(CUT, || {
        for s in polled(c)? {
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
        failed_during = stat(&sched_status(c)?, "create_failed");
        Ok(())
    })?;
    let status = sched_status(c)?;
    ensure!(
        failed_during > failed_before,
        "create_failed did not rise during a {CUT:?} cut ({failed_before} -> {failed_during}): {status}"
    );
    eprintln!(
        "    snapsched-s3-outage: during the cut: create_failed {failed_before} -> {failed_during}, \
         last_error {}, in-flight snapshots {}",
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
            .filter(|s| !known.contains(&s.name))
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
    std::thread::sleep(Duration::from_secs(40));
    let after = fresh(c)?;
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
    // within a tick of it. (This alone would miss a backfill that makes one
    // snapshot per tick; the real guard against a backfill is the "names a
    // bucket that ended during the cut" check above.)
    let burst = after
        .iter()
        .filter(|s| s.created < first.created + TICK_MS)
        .count();
    ensure!(burst == 1, "{burst} snapshots at the catch-up: {after:?}");
    // The schedule resumes: one per bucket for the 40 s after it.
    ensure!(
        after.len() >= 4,
        "the schedule did not resume after the catch-up: {after:?}"
    );
    check_series(
        "after the outage",
        &after,
        BUCKET_MS + TICK_MS + GAP_MARGIN_MS,
    )?;
    // Every snapshot from before is still there (nothing is deleted
    // until M4).
    let now: BTreeSet<String> = polled(c)?.into_iter().map(|s| s.name).collect();
    ensure!(pre.is_subset(&now), "a snapshot vanished across the outage");
    writer.stop();
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
}
