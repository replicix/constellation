//! The K5a fix round's two delegation findings, outside Kubernetes. Two
//! nodes: `a` holds the root lease, `b` is its backup and the delegate of
//! `/d1`, the kind topology of a controller engine pod and a node engine
//! pod.
//!
//! - `delegate-root-loss`: `a` is `kill -9`ed while `b` writes and
//!   `fsync`s under its delegation. `b` seals and takes the root over
//!   (the seal-based failover, `CONSTELLATION_BACKUP_TAKEOVER_MS` plus
//!   one CAS); no `fsync` may wait past that bound plus a margin, where on
//!   kind they hung for minutes.
//! - `delegate-handoff-renewal`: `b` runs `daemon --upgrade` (the same
//!   node identity and state directory, as a K5a handoff) under that
//!   writer; the resumed image's writes must not wait for the root to
//!   reclaim the delegation the old image held.
//! - `delegate-backup-handoff-failover`: `b` is handed off first; `a`,
//!   which drops `b` across the restart gap, lists it as its backup again
//!   within seconds (`b` does not seal `a`'s live epoch), and `a`'s
//!   `kill -9` then fails over by seal, as without the handoff.
//! - `delegate-store-stall`: `b`'s core is stuck behind a slow store
//!   write (the store-stall fault hook) for longer than its grant's
//!   renewal slack; the off-core renewal lane keeps the delegation, up to
//!   the hung bound.

use super::m11::{
    cluster, deleg_of, delegate, dump_logs_on_failure, holder_of, n, node_id, print_deleg,
    unmount_all, wait_installed,
};
use super::{eventually, lease_of};
use crate::client::Client;
use crate::timings::Timings;
use anyhow::{Context, Result};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The timings that matter here: a 60 s root lease, as kind runs it, and
/// a 5 s delegation grant, which is a stress setting (the default is the
/// lock grant's ttl, 20 s, since c612585: about 11 s of renewal slack, not
/// the 3.5 s this leaves). The M11 scenarios shorten both further.
const ENV: &[(&str, &str)] = &[
    ("CONSTELLATION_LEASE_TTL_MS", "60000"),
    ("CONSTELLATION_DELEGATION_TTL_MS", "5000"),
    ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "50"),
    // `daemon --upgrade` detaches a dev-fuse session only.
    ("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse"),
];

/// How long a write + `fsync` + close may take right after a handoff of
/// the delegate, beyond the upgrade itself: one round trip after the
/// re-adoption (the resumed image's first renewal, answered over a fresh
/// connection), plus host load. The old image's delegation is re-adopted
/// with the node identity, so no write waits for the root to reclaim it
/// (that wait was the grant's ttl and margin, 3.4-4.7 s on kind and up to
/// 7 s in the harness); and the old image closes its P2P connections
/// before the `exec`, so the root does not answer into the dead
/// incarnation's connection until its 2 s restart probe evicts it (the
/// first create waited 2.0-2.7 s for that, every resume). 1.5 s separates
/// the two: 0.9 s at most measured at a load average of 36, against 2.0 s
/// and more without the close.
const HANDOFF_BOUND: Duration = Duration::from_millis(1_500);

/// One write + `fsync` + close: when it started (since the writer's
/// start) and how long it took.
struct Op {
    at: Duration,
    took: Duration,
    /// The metadata part: the create and the close (both through the
    /// sequencer, the delegate here); the rest is the write and the
    /// `fsync`'s chunk upload.
    meta: Duration,
    err: Option<String>,
}

/// A writer creating, `fsync`ing and closing small files under `dir` of
/// `mnt` in a loop until stopped.
struct Writer {
    stop: Arc<AtomicBool>,
    started: Instant,
    /// The start of the op in flight (ms since `started`, `u64::MAX` when
    /// none), so a hung op is visible while it hangs.
    inflight: Arc<AtomicU64>,
    ops: Arc<Mutex<Vec<Op>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Writer {
    fn start(mnt: PathBuf, dir: &str) -> Writer {
        let stop = Arc::new(AtomicBool::new(false));
        let inflight = Arc::new(AtomicU64::new(u64::MAX));
        let ops = Arc::new(Mutex::new(Vec::new()));
        let started = Instant::now();
        let dir = mnt.join(dir);
        let thread = {
            let (stop, inflight, ops) = (stop.clone(), inflight.clone(), ops.clone());
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let at = started.elapsed();
                    inflight.store(at.as_millis() as u64, Ordering::SeqCst);
                    let t = Instant::now();
                    let mut meta = Duration::ZERO;
                    let r = (|| -> std::io::Result<()> {
                        let mut f = std::fs::File::create(dir.join(format!("w-{i}")))?;
                        meta += t.elapsed();
                        f.write_all(format!("payload {i}\n").repeat(64).as_bytes())?;
                        f.sync_all()?;
                        let c = Instant::now();
                        drop(f);
                        meta += c.elapsed();
                        Ok(())
                    })();
                    let took = t.elapsed();
                    inflight.store(u64::MAX, Ordering::SeqCst);
                    ops.lock().unwrap().push(Op {
                        at,
                        took,
                        meta,
                        err: r.err().map(|e| format!("w-{i}: {e}")),
                    });
                    i += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };
        Writer {
            stop,
            started,
            inflight,
            ops,
            thread: Some(thread),
        }
    }

    /// Now, on the writer's clock.
    fn now(&self) -> Duration {
        self.started.elapsed()
    }

    /// Wait until `count` ops completed after `since` (writer clock), or
    /// `deadline` passed; `Err` names the op still in flight.
    fn wait_ops_after(&self, since: Duration, count: usize, deadline: Duration) -> Result<()> {
        let t = Instant::now();
        loop {
            let after = self
                .ops
                .lock()
                .unwrap()
                .iter()
                .filter(|o| o.at >= since)
                .count();
            if after >= count {
                return Ok(());
            }
            if t.elapsed() > deadline {
                let inflight = self.inflight.load(Ordering::SeqCst);
                anyhow::bail!(
                    "only {after} of {count} writes completed within {deadline:?}; the op in \
                     flight started {} ago",
                    if inflight == u64::MAX {
                        "(none)".to_string()
                    } else {
                        format!(
                            "{:?}",
                            self.now().saturating_sub(Duration::from_millis(inflight))
                        )
                    }
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Stop and join (only once no op hangs).
    fn finish(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }

    /// The ops in flight at `since` or started after it, the errors, and
    /// the longest of those ops.
    fn after(&self, since: Duration) -> (Vec<Duration>, Vec<String>) {
        let ops = self.ops.lock().unwrap();
        let mut took = Vec::new();
        let mut errs = Vec::new();
        for o in ops.iter().filter(|o| o.at + o.took >= since) {
            took.push(o.took);
            if let Some(e) = &o.err {
                errs.push(e.clone());
            }
        }
        (took, errs)
    }

    /// The metadata parts of the ops [`Self::after`] returns.
    fn meta_after(&self, since: Duration) -> Vec<Duration> {
        let ops = self.ops.lock().unwrap();
        ops.iter()
            .filter(|o| o.at + o.took >= since)
            .map(|o| o.meta)
            .collect()
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // A failed scenario drops the writer before it unmounts: let the
        // op in flight finish (a busy mount cannot be unmounted and is
        // left stale), but never hang the scenario on an op that hangs —
        // that thread is detached and dies with the mount.
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !t.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            if t.is_finished() {
                let _ = t.join();
            }
        }
    }
}

fn dist(v: &[Duration]) -> String {
    if v.is_empty() {
        return "no ops".into();
    }
    let mut s = v.to_vec();
    s.sort();
    let q = |p: f64| s[((s.len() - 1) as f64 * p) as usize];
    format!(
        "{} ops, p50 {:?} p90 {:?} max {:?}",
        s.len(),
        q(0.5),
        q(0.9),
        s[s.len() - 1]
    )
}

/// Two nodes, `/d1` delegated to `b`; `b` the root's backup when
/// `backed`, else no backup at all (`CONSTELLATION_BACKUP_RTT_BUDGET_MS=0`)
/// and a 20 s root lease, so the TTL takeover fits a scenario.
fn delegated_pair(
    name: &str,
    backed: bool,
) -> Result<(crate::s3env::S3Env, tempfile::TempDir, Vec<Client>)> {
    delegated_pair_with(name, backed, &[])
}

/// [`delegated_pair`] with `extra` env on both nodes.
fn delegated_pair_with(
    name: &str,
    backed: bool,
    extra: &[(&str, &str)],
) -> Result<(crate::s3env::S3Env, tempfile::TempDir, Vec<Client>)> {
    let mut env_vars = ENV.to_vec();
    env_vars.extend_from_slice(extra);
    if !backed {
        env_vars.push(("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"));
        env_vars.push(("CONSTELLATION_LEASE_TTL_MS", "20000"));
    }
    let (env, root, clients, _) = cluster(name, &["a", "b"], &env_vars, 0)?;
    Ok((env, root, clients))
}

/// Delegate `/d1` to `b` (and, when `backed`, wait until `a` backs up to
/// `b`). Inside each scenario's body, so a failure keeps the logs.
fn delegate_d1(name: &str, clients: &[Client], backed: bool) -> Result<()> {
    let a = &clients[0];
    let b = &clients[1];
    let b_id = node_id(b)?;
    std::fs::create_dir(a.mnt.join("d1"))?;
    eventually("d1 visible on b", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("d1").is_dir());
        Ok(())
    })?;
    delegate(a, "/d1", b_id)?;
    let gen = wait_installed(b, "/d1", Duration::from_secs(20))?;
    if backed {
        let backups = super::m9::wait_for_backup(a, Duration::from_secs(30))?;
        anyhow::ensure!(
            backups == vec![b_id],
            "a backs up to {backups:?}, expected b ({b_id})"
        );
        eprintln!(
            "    {name}: a holds the root, b ({b_id}) is its backup and the delegate of /d1 \
             (gen {gen})"
        );
    } else {
        eprintln!(
            "    {name}: a holds the root (no backup), b ({b_id}) is the delegate of /d1 (gen {gen})"
        );
    }
    Ok(())
}

/// Finding 1: the root holder dies (`kill -9`) while its backup is its
/// delegate with `fsync`s in flight.
pub fn delegate_root_loss(_seed: u64) -> Result<()> {
    root_loss("delegate-root-loss", true, false, false)
}

/// The backup's own handoff: `b` runs `daemon --upgrade` first. The root
/// drops it across the restart gap; before, the resumed `b` then sealed
/// the root's live epoch on the silence, was never invited back, and a
/// later death of the root was a TTL failover (up to a minute) instead
/// of a seal (1.5 s). Now `b` never seals, is listed again (the time is
/// reported, see [`RELIST_WAIT`]), and the root's `kill -9` fails over
/// by seal.
pub fn delegate_backup_handoff_failover(_seed: u64) -> Result<()> {
    root_loss("delegate-backup-handoff-failover", true, false, true)
}

/// How long the root may take to list a handed-off backup again. Not a
/// tight bound: the root needs its link to the restarted peer connected
/// again, and that recovery is slow here (gossip rejoin 5-23 s, or not
/// within 30 s: an open P2P bug, see PROGRESS). The time is reported.
const RELIST_WAIT: Duration = Duration::from_secs(90);

/// Finding 1 as kind showed it: a force-deleted pod's address vanishes,
/// so the dead root answers nothing (no reset, no ICMP) — the root is
/// frozen (`SIGSTOP`) rather than killed, then killed once the writes
/// were measured.
pub fn delegate_root_blackhole(_seed: u64) -> Result<()> {
    root_loss("delegate-root-blackhole", true, true, false)
}

/// Finding 1's unbounded wait itself: the frozen root has no backup, so
/// only a TTL takeover can replace it — which nothing started while the
/// delegate's own writes waited, parked, for a renewal the dead root
/// would never answer.
pub fn delegate_root_loss_ttl(_seed: u64) -> Result<()> {
    root_loss("delegate-root-loss-ttl", false, true, false)
}

/// The bounds of [`root_loss`]: a listed backup's seal-based failover
/// (`CONSTELLATION_BACKUP_TAKEOVER_MS`, 1.5 s, then one CAS and the
/// successor's gate; about 1.5 s in the harness), or, when the delegate
/// is no backup (never was, or the root dropped it before it died), the
/// TTL takeover: the lease's expiry plus the 3 s a non-backup waits past
/// it, measured from the root's last renewal (at most one TTL before its
/// death), plus the inbox and escalation steps. On kind the wait was over
/// four minutes.
///
/// Since plan 41 L0 (`9d00417`) a root that answered a delegation renewal
/// marks its tenure granting first, so the seal's successor waits out the
/// predecessor's strict-read horizon before it acknowledges a mutation
/// (`note_marker_landed`, [`Timings::ack_floor_wait`]: 1.5 + 5 + 6 + 2 =
/// 14.5 s with [`ENV`]). The fast bound is the protocol's, derived from
/// the configured timings — the takeover window, plus that wait when the
/// successor counted one (`ack_floor_waits`) — plus [`FAST_SLACK`] of
/// host time, and it is judged against the host-lag probe
/// ([`crate::hostlag::latency_bound`]); the constant 8 s it replaces
/// predates L0 (finding F4).
const FAST_SLACK: Duration = Duration::from_millis(6_500);

/// The TTL takeover's protocol part: expiry plus the 3 s a non-backup
/// waits past it.
fn ttl_protocol(lease_ttl: Duration) -> Duration {
    lease_ttl + Duration::from_secs(3)
}

fn ttl_bound(lease_ttl: Duration) -> Duration {
    ttl_protocol(lease_ttl) + Duration::from_secs(12)
}

/// The timings `root_loss`'s daemons run with (see [`delegated_pair`]).
fn pair_timings(backed: bool) -> Timings {
    let mut env: Vec<(&str, &str)> = ENV.to_vec();
    if !backed {
        env.push(("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"));
        env.push(("CONSTELLATION_LEASE_TTL_MS", "20000"));
    }
    Timings::from_pairs(&env)
}

/// The waits `node`'s log reports for the successor's quarantine
/// (`wait_ms` of "acknowledging no mutation until its delegation horizon
/// has passed").
pub(super) fn floor_waits_logged(node: &Client) -> Vec<u64> {
    node.log_text()
        .lines()
        .filter(|l| l.contains("acknowledging no mutation until its delegation horizon"))
        .filter_map(|l| {
            crate::verify::field(&crate::verify::strip_ansi(l), "wait_ms")
                .and_then(|v| v.parse().ok())
        })
        .collect()
}

/// The backup's handoff before the root's loss: `b` runs `daemon
/// --upgrade`, and `a` must list it as its backup again within
/// [`RELIST_WAIT`] (the time is reported), with `b` never having sealed
/// `a`'s epoch.
fn handoff_backup(name: &str, clients: &[Client]) -> Result<()> {
    let (a, b) = (&clients[0], &clients[1]);
    let b_id = node_id(b)?;
    let a0 = super::m9::ack_of(a)?;
    let started = Instant::now();
    let took = super::handover::upgrade(b).context("upgrading b")?;
    let listed = |v: &serde_json::Value| {
        v["policy"] == "backup"
            && v["backups"]
                .as_array()
                .is_some_and(|l| l.iter().any(|x| x.as_u64() == Some(b_id)))
    };
    // b is listed again once a removal of it was followed by an
    // addition; or it never left, if a still lists it, with no removal,
    // 3 s after the resume (well past the acknowledgement timeout that
    // drops it).
    let mut relisted = None;
    let mut last = serde_json::Value::Null;
    // a's view of its link to b meanwhile (a backup candidate must be
    // connected, within the RTT budget, and up for 2 s): (connected
    // samples, samples, the RTTs seen).
    let (mut up, mut samples, mut rtts) = (0, 0, Vec::new());
    while started.elapsed() < RELIST_WAIT {
        let peer = a.control_status()?["p2p"]["peers"]
            .as_array()
            .and_then(|v| {
                v.iter()
                    .find(|p| p["node_id"].as_u64() == Some(b_id))
                    .cloned()
            })
            .unwrap_or_default();
        samples += 1;
        if peer["connected"] == true {
            up += 1;
        }
        if let Some(rtt) = peer["rtt_ms"].as_u64() {
            if rtts.last() != Some(&rtt) {
                rtts.push(rtt);
            }
        }
        let ack = super::m9::ack_of(a)?;
        let removed = n(&ack, "backups_removed") > n(&a0, "backups_removed");
        let added = n(&ack, "backups_added") > n(&a0, "backups_added");
        let settled = started.elapsed() >= took + Duration::from_secs(3);
        last = ack;
        if listed(&last) && (added || (!removed && settled)) {
            relisted = Some(started.elapsed());
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let epoch = lease_of(a)?["epoch"].as_u64().unwrap_or(0);
    let ack_b = super::m9::ack_of(b)?;
    eprintln!(
        "    {name}: b's upgrade took {took:?}; a lists b again {relisted:?} after it started (removed +{} added +{}); b sealed epoch {} (a's is {epoch}), seals {}; a's link to \
         b connected in {up} of {samples} samples, RTTs (ms) {rtts:?}",
        n(&last, "backups_removed").saturating_sub(n(&a0, "backups_removed")),
        n(&last, "backups_added").saturating_sub(n(&a0, "backups_added")),
        n(&ack_b, "sealed_epoch"),
        n(&ack_b, "seals"),
    );
    anyhow::ensure!(
        n(&ack_b, "sealed_epoch") < epoch && n(&ack_b, "seals") == 0,
        "the resumed b sealed a's live epoch {epoch}: {ack_b}"
    );
    let relisted = relisted.with_context(|| {
        format!(
            "a did not list b as its backup again within {RELIST_WAIT:?} of its upgrade: {last}"
        )
    })?;
    eprintln!("    {name}: a listed b again {relisted:?} after b's upgrade started");
    Ok(())
}

fn root_loss(name: &'static str, backed: bool, freeze: bool, handoff_first: bool) -> Result<()> {
    let (_env, _root, mut clients) = delegated_pair(name, backed)?;
    let result = (|| -> Result<()> {
        delegate_d1(name, &clients, backed)?;
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        let lease_ttl = Duration::from_secs(if backed { 60 } else { 20 });
        let w = Writer::start(clients[1].mnt.clone(), "d1");
        w.wait_ops_after(Duration::ZERO, 20, Duration::from_secs(30))?;
        let (before, _) = w.after(Duration::ZERO);
        eprintln!(
            "    {name}: b's writes under its delegation: {}",
            dist(&before)
        );
        if handoff_first {
            handoff_backup(name, &clients)?;
        }
        // `b` must be a's listed backup at the kill, or the takeover is
        // the TTL path and a regression of the seal path could hide
        // behind it.
        let b_id = node_id(&clients[1])?;
        if backed {
            eventually("a lists b as its backup", Duration::from_secs(60), || {
                let ack = super::m9::ack_of(&clients[0])?;
                anyhow::ensure!(
                    ack["backups"]
                        .as_array()
                        .is_some_and(|v| v.iter().any(|x| x.as_u64() == Some(b_id))),
                    "a's backups: {ack}"
                );
                Ok(())
            })?;
        }
        let listed = backed
            && super::m9::ack_of(&clients[0])?["backups"]
                .as_array()
                .is_some_and(|v| v.iter().any(|x| x.as_u64() == Some(b_id)));
        // The successor's quarantine waits so far (F4: since L0 one
        // follows every seal of a root that renewed delegations).
        let floor_waits0 = n(&super::m9::ack_of(&clients[1])?, "ack_floor_waits");
        let logged0 = floor_waits_logged(&clients[1]).len();
        let killed_at = w.now();
        let killed = Instant::now();
        if freeze {
            clients[0].pause()?;
        } else {
            clients[0].kill9()?;
        }
        // Writes keep going well past the delegation's grant (5 s, the
        // point where b must renew with a root that no longer exists);
        // writes must still complete after that, the takeover included.
        // A delegate the root no longer lists can only take it over by
        // TTL: the window is that bound.
        let past = killed_at
            + if listed {
                Duration::from_secs(20)
            } else {
                ttl_bound(lease_ttl)
            };
        // When b is first seen holding the root (writer clock).
        let mut held_at: Option<Duration> = None;
        let progressed = loop {
            if held_at.is_none()
                && lease_of(&clients[1])
                    .is_ok_and(|l| l["held"] == true && l["epoch"].as_u64().unwrap_or(0) > epoch)
            {
                held_at = Some(w.now());
            }
            if w.now() >= past {
                // Liveness, not latency: no write may wait for the dead
                // root. The hang bound is the arbiter of "never" (a
                // starved host makes the TTL bound alone too short).
                break w.wait_ops_after(
                    past,
                    10,
                    ttl_bound(lease_ttl).max(crate::verify::hang_bound()),
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let b = &clients[1];
        let took_over = holder_of(&clients[1..])
            .map(|_| lease_of(b).map(|l| l["epoch"].as_u64().unwrap_or(0) > epoch))
            .transpose()?
            .unwrap_or(false);
        let db = deleg_of(b)?;
        let ack = super::m9::ack_of(b)?;
        print_deleg(name, "b", &db);
        eprintln!(
            "    {name}: {:?} after the {} b {} the root (epoch {} -> {}); seals {} backup \
             takeovers {}; lapsed grants {}; fsync status {}",
            killed.elapsed(),
            if freeze { "freeze" } else { "kill" },
            if took_over { "holds" } else { "does NOT hold" },
            epoch,
            lease_of(b)?["epoch"],
            n(&ack, "seals"),
            n(&ack, "backup_takeovers"),
            n(&db, "lapsed"),
            b.control_status()?["fsync"]
        );
        progressed?;
        let (_, errs) = w.after(killed_at);
        // The failover's writes: those in flight at the kill or started
        // before b was seen holding the root, plus 2 s to settle. Later
        // ones run on the new root; a host stall then (a core step
        // blocked 8 s at load 200 in one run, writes of 5.8-30 s at load
        // 150) is no failover wait, and they are bound only by the TTL
        // takeover's bound: no write waits for the dead root.
        let settled = held_at.unwrap_or(past) + Duration::from_secs(2);
        // (took, when it began).
        let (mut failover, mut later) = (Vec::new(), Vec::new());
        for o in w.ops.lock().unwrap().iter() {
            if o.at + o.took < killed_at {
                continue;
            }
            if o.at <= settled {
                failover.push((o.took, w.started + o.at));
            } else {
                later.push((o.took, w.started + o.at));
            }
        }
        let total = w.ops.lock().unwrap().len() as u64;
        w.finish();
        let longest = failover
            .iter()
            .max()
            .copied()
            .unwrap_or((Duration::ZERO, killed));
        let longest_later = later
            .iter()
            .max()
            .copied()
            .unwrap_or((Duration::ZERO, killed));
        // A listed backup takes over by seal.
        let fast = n(&ack, "backup_takeovers") >= 1;
        let timings = pair_timings(backed);
        let quarantined = n(&ack, "ack_floor_waits") > floor_waits0;
        let waits: Vec<u64> = floor_waits_logged(b)
            .get(logged0..)
            .unwrap_or_default()
            .to_vec();
        // The protocol's part is what the successor logged it waited
        // (measured); the mirrored formula is only the cap it is checked
        // against below, a loud tripwire for drift.
        let logged = Duration::from_millis(waits.iter().copied().max().unwrap_or(0));
        let protocol = if fast {
            timings.backup_takeover + logged
        } else {
            ttl_protocol(lease_ttl)
        };
        let bound = if fast {
            protocol + FAST_SLACK
        } else {
            ttl_bound(lease_ttl)
        };
        let durations = |v: &[(Duration, Instant)]| v.iter().map(|x| x.0).collect::<Vec<_>>();
        eprintln!(
            "    {name}: b's writes across the root's loss: {} ({} takeover, protocol {protocol:?}, \
             bound {bound:?}; successor quarantine {} (logged waits {waits:?} ms, configured at \
             most {:?}); b held the root {:?} after the {}); afterwards: {}; {} inherited \
             generations, {} reclaimed",
            dist(&durations(&failover)),
            if fast { "seal-based" } else { "TTL" },
            if quarantined { "waited" } else { "none" },
            timings.ack_floor_wait(true),
            held_at.map(|t| t.saturating_sub(killed_at)),
            if freeze { "freeze" } else { "kill" },
            dist(&durations(&later)),
            n(&db, "inherited"),
            n(&db, "reclaimed")
        );
        anyhow::ensure!(
            errs.is_empty(),
            "writes failed across the failover: {errs:?}"
        );
        anyhow::ensure!(took_over, "b never took the root over");
        // A counted quarantine is logged (else the measured term above
        // would be zero: a log text drift), and never longer than the
        // configured one (it is exactly that one, less the reclaim horizon
        // when no write delegation was live).
        anyhow::ensure!(
            !quarantined || !waits.is_empty(),
            "the successor counted a quarantine (ack_floor_waits) but logged no wait_ms"
        );
        let cap = timings.ack_floor_wait(true).as_millis() as u64;
        anyhow::ensure!(
            waits.iter().all(|w| *w <= cap),
            "the successor's quarantine waited {waits:?} ms, past the configured {cap} ms"
        );
        if handoff_first {
            anyhow::ensure!(listed, "b was not a's listed backup at the kill");
        }
        if listed {
            anyhow::ensure!(
                fast,
                "b, a listed backup, did not take the root over by seal"
            );
        }
        // Every write is there after a's return.
        if freeze {
            clients[0].kill9()?;
        }
        clients[0].mount()?;
        let names: Vec<String> = (0..total).map(|i| format!("d1/w-{i}")).collect();
        let a = &clients[0];
        eventually("a sees b's writes", Duration::from_secs(60), || {
            for name in &names {
                anyhow::ensure!(a.mnt.join(name).exists(), "{name} missing on a");
            }
            Ok(())
        })?;
        super::ensure_no_conflicts(&[&clients[0], &clients[1]])?;
        // Latency last, once every correctness check held: judged against
        // the host-lag probe, inconclusive (not failed) on a starved host.
        crate::hostlag::latency_bound(
            &format!("{name}: a write + fsync across the root's loss"),
            longest.1,
            longest.0,
            protocol,
            bound,
        )?;
        crate::hostlag::latency_bound(
            &format!("{name}: a write + fsync on the new root"),
            longest_later.1,
            longest_later.0,
            ttl_protocol(lease_ttl),
            ttl_bound(lease_ttl),
        )?;
        Ok(())
    })();
    dump_logs_on_failure(name, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// `b`'s live, renewed generation of `/d1` in a status document of `b`.
fn held_generation(d: &serde_json::Value) -> Option<u64> {
    d["mine"].as_array()?.iter().find_map(|m| {
        (m[3] == false && m[2].as_i64().unwrap_or(0) > 0).then(|| m[1].as_u64().unwrap_or(0))
    })
}

/// What `b`'s daemon classified of its own losses (status `delegation`):
/// every grant it gave up (a lapse, a refusal after the grant ran out, or
/// the log ending the generation after it ran out) is counted in
/// `grants_lost`, and, apart, in `lapses_during_freeze` (with the length and
/// generation of the latest) if a freeze of its whole process covered the
/// time from the renewal's due time to the loss (the heartbeat thread and the
/// shared runtime's lag probe both missed it: nothing in the process could
/// renew), else in `lapses_root_silent` (with its generation) if a renewal it
/// had sent by the time the lane would have acted went unanswered by the
/// root. Only a freeze excuses: a loss is excused when every loss of the
/// window being judged was one, for that generation. A silent root is a
/// known gap, not an excuse (the root answers renewals from its core, and a
/// core blocked in a shared log sink answers nothing: plan 44), reported as
/// a failure with its own label so batches can count it; it must not hide a
/// lane or a transport failure. A loss neither classified is a failure.
struct Losses {
    reclaimed: u64,
    recalls: u64,
    lapsed: u64,
    lost: u64,
    frozen: u64,
    frozen_gen: u64,
    frozen_ms: u64,
    silent: u64,
    silent_gen: u64,
}

/// The label of a failure that is a root whose core was stuck, to grep and
/// count over a batch.
const KNOWN_GAP: &str = "KNOWN-GAP root core stuck";

impl Losses {
    /// `a`'s reclaims and recalls and `b`'s losses since `a0` / `b0`
    /// (earlier status documents of the same images).
    fn since(
        da: &serde_json::Value,
        a0: &serde_json::Value,
        db: &serde_json::Value,
        b0: &serde_json::Value,
    ) -> Losses {
        let d = |x: &serde_json::Value, x0: &serde_json::Value, k: &str| {
            n(x, k).saturating_sub(n(x0, k))
        };
        Losses {
            reclaimed: d(da, a0, "reclaimed"),
            recalls: d(da, a0, "recalls_sent"),
            lapsed: d(db, b0, "lapsed"),
            lost: d(db, b0, "grants_lost"),
            frozen: d(db, b0, "lapses_during_freeze"),
            frozen_gen: n(db, "last_freeze_gen"),
            frozen_ms: n(db, "last_freeze_ms"),
            silent: d(db, b0, "lapses_root_silent"),
            silent_gen: n(db, "last_silent_gen"),
        }
    }

    fn any(&self) -> bool {
        self.reclaimed + self.recalls + self.lost + self.lapsed > 0
    }

    /// The grants lost, as `b` or `a` saw them: `b` counts every loss it
    /// noticed, `a` every reclaim (a loss `b` has not noticed yet is not
    /// counted twice).
    fn grants_lost(&self) -> u64 {
        self.lost.max(self.lapsed).max(self.reclaimed)
    }

    /// The loss of generation `gen` (`None`: any) is explained by freezes
    /// the daemon classified: every grant lost was one, and the last of them
    /// was of that generation.
    fn excused(&self, gen: Option<u64>) -> Option<String> {
        let same = gen.is_none_or(|g| g == self.frozen_gen);
        (self.frozen > 0 && self.frozen >= self.grants_lost() && same).then(|| {
            format!(
                "loss permitted, classified by the daemon: {} grant(s) lost under a freeze of \
                 its whole process (the heartbeat thread and the runtime's lag probe did not run \
                 from the renewal's due time to the grant's loss; the last {} ms, generation {})",
                self.frozen, self.frozen_ms, self.frozen_gen
            )
        })
    }

    /// The failure for a loss nothing excused: the known gap, if every loss
    /// beyond the freezes was a silent root (of that generation, if given);
    /// else the plain one. `what` says where it was seen.
    fn failure(&self, what: &str, gen: Option<u64>, detail: &str) -> anyhow::Error {
        let silent_only = self.silent > 0
            && self.frozen + self.silent >= self.grants_lost()
            && gen.is_none_or(|g| g == self.silent_gen);
        if silent_only {
            anyhow::anyhow!(
                "{KNOWN_GAP} (it answers renewals from its core: a renewal sent in time was \
                 never answered; plan 44's class when its core was blocked in a shared log \
                 sink): {what}: a reclaimed +{}, recalled +{}, b lapsed {}, lost {} (under a \
                 freeze {}, under a silent root {}, generation {}){detail}",
                self.reclaimed,
                self.recalls,
                self.lapsed,
                self.lost,
                self.frozen,
                self.silent,
                self.silent_gen
            )
        } else {
            anyhow::anyhow!(
                "{what}: a reclaimed +{}, recalled +{}, b lapsed {}, lost {}; the daemon \
                 classified {} under a freeze of its whole process (the last of generation {}) \
                 and {} under a silent root, and the rest of them nothing explains (not a \
                 freeze, not a root that did not answer: a renewal not sent in time, or a \
                 transport failure), nor was its core stuck for the hung bound{detail}",
                self.reclaimed,
                self.recalls,
                self.lapsed,
                self.lost,
                self.frozen,
                self.frozen_gen,
                self.silent
            )
        }
    }
}

/// Whether the run injects its own freeze
/// (`CONSTELLATION_HARNESS_DELEG_FREEZE_MS`): a note about a classified
/// freeze is then what the run is for. Otherwise a loss excused as a freeze
/// (or by the hung bound) leaves a window the lane was not judged in: the run
/// is INCONCLUSIVE, not passed.
fn injected_freeze() -> bool {
    std::env::var("CONSTELLATION_HARNESS_DELEG_FREEZE_MS").is_ok()
}

/// The inconclusive note an excused loss leaves (`None`: the freeze was
/// injected, and the classified note is what the run is for).
fn excused_inconclusive(name: &str, note: &str, injected: bool) -> Option<String> {
    (!injected)
        .then(|| format!("{name}: a loss was excused, the lane not judged in that window: {note}"))
}

/// Print `note` of an excused loss: a NOTE if the freeze was injected,
/// else also an inconclusive verdict for the scenario.
fn excused_note(name: &str, note: &str) {
    eprintln!("    {name}: NOTE {note}");
    if let Some(n) = excused_inconclusive(name, note, injected_freeze()) {
        crate::hostlag::inconclusive(n);
    }
}

/// The hung bound of the authority driver (`CONSTELLATION_HOLDER_STALL_MS`,
/// 15 s): a core stuck in one step that long is hung, not slow, and its
/// delegation is not kept alive off the core any more.
fn holder_stall_bound() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_HOLDER_STALL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15_000),
    )
}

/// The longest step `c`'s authority core has taken since the mount, and
/// the longest of the last minute or two (status `ack`).
fn core_steps(c: &Client) -> Result<(Duration, Duration)> {
    let s = c.control_status()?;
    let ms = |k: &str| Duration::from_millis(s["ack"][k].as_u64().unwrap_or(0));
    Ok((ms("longest_step_ms"), ms("recent_longest_step_ms")))
}

/// `b`'s core stuck as long as the hung bound in one step within the last
/// minute or two: the lane stops there by design and the root reclaims as it
/// would a dead delegate's.
fn hung_core(b: &Client) -> Result<Option<String>> {
    let (longest, recent) = core_steps(b)?;
    Ok((recent >= holder_stall_bound()).then(|| {
        format!(
            "core stuck {recent:?} in one step (longest {longest:?}), the hung bound {:?}: the \
             lane stops there by design",
            holder_stall_bound()
        )
    }))
}

/// `b`'s live, renewed generation of `/d1`. A delegate whose core is only
/// slow keeps its grant (the off-core renewal lane renews while the core
/// is stuck, up to the hung bound), so a generation that is gone before a
/// round is a failure, unless it is the generation the previous round lost
/// to a classified freeze (`carried`: that round's note and generation; its
/// image, and its counters, ended with the handoff), or this image's own
/// counters classify the loss, or `b`'s core was stuck for the hung bound:
/// then the note is printed and `/d1` is delegated again, from `a`. A node
/// whose delegation was lost writes through the root, becomes its only
/// writer, and asks for the lease, which the root hands over whatever its
/// dwell (the lease follows the writes): if `b` holds it then, `a` cannot
/// delegate and the premise of the rest of the scenario is gone (`b` is the
/// root): `None`, and the caller ends the scenario with a note, inconclusive
/// from there on, not failed.
fn ensure_delegated(
    name: &str,
    a: &Client,
    b: &Client,
    carried: Option<&(u64, String)>,
) -> Result<Option<u64>> {
    let b_id = node_id(b)?;
    if let Some(gen) = held_generation(&deleg_of(b)?) {
        return Ok(Some(gen));
    }
    let own = Losses::since(
        &deleg_of(a)?,
        &serde_json::Value::Null,
        &deleg_of(b)?,
        &serde_json::Value::Null,
    );
    let note = match carried {
        Some((gen, note)) => Some(format!(
            "{note}; generation {gen}, lost in the previous round"
        )),
        None => own.excused(None).or(hung_core(b)?),
    };
    let Some(note) = note else {
        return Err(own.failure(
            &format!("{name}: b lost its delegation before the round"),
            None,
            &format!(": {}", deleg_of(b)?),
        ));
    };
    excused_note(
        name,
        &format!("b lost its delegation before the round: {note}; delegating again"),
    );
    if lease_of(a)?["held"] != true {
        // Not a pass: the lane was not judged for the rest of the run.
        crate::hostlag::inconclusive(format!(
            "{name}: the root lease moved to b (its writes went through the root after the \
             loss, and the lease follows the writer): no root to delegate from; the rest of \
             the run is skipped"
        ));
        return Ok(None);
    }
    eventually("the lost generation ended", Duration::from_secs(60), || {
        let d = deleg_of(a)?;
        let table = d["table"].as_array().cloned().unwrap_or_default();
        anyhow::ensure!(
            !table.iter().any(|e| e["path"] == "/d1"),
            "/d1 still delegated: {d}"
        );
        Ok(())
    })
    .or_else(|_| {
        held_generation(&deleg_of(b)?)
            .map(|_| ())
            .context("neither ended nor held")
    })?;
    if held_generation(&deleg_of(b)?).is_none() {
        delegate(a, "/d1", b_id)?;
    }
    let mut gen = 0;
    eventually("b renewed its delegation", Duration::from_secs(30), || {
        gen = held_generation(&deleg_of(b)?).context("not held and renewed yet")?;
        Ok(())
    })?;
    Ok(Some(gen))
}

/// Since `a_before` / `b_before` (earlier status documents of the same
/// images; `Null`: the image's start): no delegation lapsed at `b`, none
/// was reclaimed or recalled by `a`, unless the daemon classified every one
/// as lost under a freeze of its process ([`Losses::excused`], for `gen`) or
/// `b`'s core was stuck for the hung bound: then a labelled note, returned
/// (the next image of `b` after a handoff starts its counters over, and the
/// round's later checks need it), not a failure. Errors, data loss and the
/// latency bounds fail regardless.
fn ensure_no_loss(
    name: &str,
    a: &Client,
    b: &Client,
    (a_before, b_before): (&serde_json::Value, &serde_json::Value),
    gen: Option<u64>,
) -> Result<Option<String>> {
    let (da, db) = (deleg_of(a)?, deleg_of(b)?);
    let l = Losses::since(&da, a_before, &db, b_before);
    if !l.any() {
        return Ok(None);
    }
    let Some(note) = l.excused(gen).or(hung_core(b)?) else {
        return Err(l.failure(
            &format!("{name}: b's delegation was lost"),
            gen,
            &format!(": a {da} b {db}"),
        ));
    };
    excused_note(
        name,
        &format!(
            "a reclaimed +{}, b lapsed {}: {note}",
            l.reclaimed, l.lapsed
        ),
    );
    Ok(Some(note))
}

/// Finding 2: the delegate is handed off (`daemon --upgrade`, the same
/// node identity and state directory); the resumed image re-adopts its
/// generation, so its writes right after the resume wait for no renewal,
/// recall or reclaim. Asserted on the mechanism (the same generation on
/// both sides of every handoff, nothing reclaimed or recalled, no
/// `NotHolder` bounce at the root, the writes executed by b as the
/// delegate) and on the latency of the writes' metadata part (the create
/// and the close, which the old wait held; the `fsync`'s chunk upload
/// is S3's and is reported only): the longest after the resume within
/// [`HANDOFF_BOUND`], or twice the longest of the writes just
/// before the upgrade, or the upgrade's own duration, if the host is
/// slower than that (its load, not a wait on the delegation — that wait
/// was the grant's ttl and more, and the mechanism checks catch it). The
/// same bound holds for the longest whole write overlapping the upgrade,
/// less the upgrade's own duration, so a wait that lands on a write's
/// `fsync` (the first renewal's answer was 2.0-2.7 s late while the old
/// image left its P2P connections open) is caught as well. That whole-write
/// bound includes the `fsync`'s chunk upload: twice the control writes
/// absorbs steady S3 latency, but a single S3 hiccup could fail the run —
/// if it flakes, bound only the metadata part.
///
/// Nor may the delegation be lost between the handoffs: no lapse at `b`,
/// no reclaim at `a`, and `b` holds its generation at every round's start
/// ([`ensure_no_loss`], [`ensure_delegated`]). A core blocked behind a slow
/// store or log write used to lose it (the off-core renewal lane keeps
/// it, `delegate-store-stall`). Excused: a loss the daemon classified as
/// under a freeze of its whole process ([`Losses`]), and a core stuck for
/// the hung bound; `/d1` is then delegated again, from `a`, if it still holds
/// the lease (a node whose delegation was lost writes through the root and
/// the lease follows it, whatever the dwell: then the rest of the scenario
/// is skipped with a note, [`ensure_delegated`]).
///
/// `CONSTELLATION_HARNESS_DELEG_FREEZE_MS=<ms>` freezes `b` (`SIGSTOP`) that
/// long at the start of round 0, outside the measured windows (the control
/// and the upgrade), to exercise the classified-freeze path.
pub fn delegate_handoff_renewal(_seed: u64) -> Result<()> {
    const NAME: &str = "delegate-handoff-renewal";
    let inject_freeze = std::env::var("CONSTELLATION_HARNESS_DELEG_FREEZE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    let (_env, _root, mut clients) = delegated_pair(NAME, true)?;
    let result = (|| -> Result<()> {
        delegate_d1(NAME, &clients, true)?;
        let a = &clients[0];
        let b = &clients[1];
        let w = Writer::start(b.mnt.clone(), "d1");
        w.wait_ops_after(Duration::ZERO, 20, Duration::from_secs(30))?;
        let (before, _) = w.after(Duration::ZERO);
        eprintln!(
            "    {NAME}: b's writes under its delegation: {}",
            dist(&before)
        );
        let mut a_base = deleg_of(a)?;
        let mut carried: Option<(u64, String)> = None;
        for round in 0..3 {
            let Some(gen) = ensure_delegated(NAME, a, b, carried.take().as_ref())? else {
                w.finish();
                return Ok(());
            };
            a_base = deleg_of(a)?;
            let b_base = deleg_of(b)?;
            if let (0, Some(ms)) = (round, inject_freeze) {
                // Outside the control and the upgrade: the writer waits it
                // out and goes on through the root once the grant is gone.
                eprintln!("    {NAME}: freezing b for {ms} ms (injected)");
                b.pause()?;
                std::thread::sleep(Duration::from_millis(ms));
                b.resume()?;
                w.wait_ops_after(w.now(), 20, Duration::from_secs(60))?;
            }
            // The control: the writes of the last 3 s before the upgrade.
            let control_from = w.now();
            std::thread::sleep(Duration::from_secs(3));
            let (control, _) = w.after(control_from);
            let control_meta = w.meta_after(control_from);
            // Nothing lapsed or was reclaimed while the writer ran (a
            // loss 160 ms before the handoff is not the handoff's).
            let freeze = ensure_no_loss(NAME, a, b, (&a_base, &b_base), Some(gen))?;
            let da0 = deleg_of(a)?;
            let up_at = w.now();
            let took = super::handover::upgrade(b).context("upgrading b")?;
            let resumed_at = w.now();
            w.wait_ops_after(resumed_at, 20, Duration::from_secs(60))?;
            // The writes answered after the upgrade returned: the resumed
            // image's first ones.
            let (ops, errs) = w.after(resumed_at);
            let (across, _) = w.after(up_at);
            let longest_across = across.iter().max().copied().unwrap_or_default();
            let longest_across_at = w
                .ops
                .lock()
                .unwrap()
                .iter()
                .filter(|o| o.at + o.took >= up_at && o.took == longest_across)
                .map(|o| w.started + o.at)
                .next()
                .unwrap_or_else(Instant::now);
            let meta = w.meta_after(resumed_at);
            let longest = meta.iter().max().copied().unwrap_or_default();
            let longest_at = w
                .ops
                .lock()
                .unwrap()
                .iter()
                .filter(|o| o.at + o.took >= resumed_at && o.meta == longest)
                .map(|o| w.started + o.at)
                .next()
                .unwrap_or_else(Instant::now);
            let slowest_control = control_meta.iter().max().copied().unwrap_or_default();
            // (A host stall slows the writes before the upgrade too.)
            let bound = HANDOFF_BOUND.max(slowest_control * 2);
            // The same for a whole write overlapping the upgrade, less the
            // upgrade's own duration: a wait that lands on a write's
            // `fsync` rather than its create is a wait all the same.
            let slowest_control_write = control.iter().max().copied().unwrap_or_default();
            let across_bound = HANDOFF_BOUND.max(slowest_control_write * 2);
            let da = deleg_of(a)?;
            let db = deleg_of(b)?;
            let gen_after = db["mine"]
                .as_array()
                .and_then(|m| m.first())
                .and_then(|m| m[1].as_u64())
                .unwrap_or(0);
            let delta = |d: &serde_json::Value, d0: &serde_json::Value, k: &str| {
                n(d, k).saturating_sub(n(d0, k))
            };
            eprintln!(
                "    {NAME}: round {round}: upgrade took {took:?}; writes after the resume: {}, \
                 their create + close {} (bound {bound:?}); across the upgrade: {}; control \
                 before: {}, create + close {}; b's generation {gen} -> {gen_after}, executed \
                 here {} after the resume; at a: reclaimed +{} recalls +{} not-owner +{}",
                dist(&ops),
                dist(&meta),
                dist(&across),
                dist(&control),
                dist(&control_meta),
                n(&db, "executed") + n(&db, "fast_path_executed"),
                delta(&da, &da0, "reclaimed"),
                delta(&da, &da0, "recalls_sent"),
                delta(&da, &da0, "not_owner"),
            );
            anyhow::ensure!(errs.is_empty(), "writes failed after the upgrade: {errs:?}");
            // The delegation's mechanism: the same generation, nothing
            // reclaimed, recalled or bounced, the delegate executing. A loss
            // the daemon classified as under a freeze of its process (before
            // the upgrade, from the old image's counters carried here in
            // `freeze`, or after it, from the new image's) is a labelled
            // note, not a failure. Errors above and the latency bounds below
            // fail regardless.
            let mechanism = (|| -> Result<()> {
                anyhow::ensure!(
                    gen_after == gen,
                    "b's generation {gen} did not survive the handoff (now {gen_after}): {db}"
                );
                anyhow::ensure!(
                    delta(&da, &da0, "reclaimed") == 0 && delta(&da, &da0, "recalls_sent") == 0,
                    "the root reclaimed or recalled b's delegation across the handoff: {da}"
                );
                anyhow::ensure!(
                    delta(&da, &da0, "not_owner") == 0,
                    "b's writes bounced off the root after the handoff: {da}"
                );
                anyhow::ensure!(
                    n(&db, "executed") + n(&db, "fast_path_executed") > 0,
                    "the resumed image executed nothing as the delegate: {db}"
                );
                Ok(())
            })();
            if let Err(e) = mechanism {
                let post = Losses::since(&da, &da0, &db, &serde_json::Value::Null);
                let Some(note) = freeze.clone().or(post.excused(Some(gen))) else {
                    return Err(if post.silent > 0 {
                        post.failure(
                            &format!("{NAME}: round {round}: the delegation did not hold"),
                            Some(gen),
                            &format!(" ({e:#})"),
                        )
                    } else {
                        e
                    });
                };
                excused_note(
                    NAME,
                    &format!(
                        "round {round}: {note}; the mechanism check that failed ({}) is excused",
                        e.to_string().chars().take(80).collect::<String>()
                    ),
                );
                // Only the generation actually gone is carried to the next
                // round's check.
                if gen_after != gen {
                    carried = Some((gen, note));
                }
            }
            // No wait for a reclaim (the checks above); the time itself is
            // latency, judged against the host-lag probe.
            crate::hostlag::latency_bound(
                &format!("{NAME}: round {round}: a create + close after the resume"),
                longest_at,
                longest,
                Duration::ZERO,
                bound,
            )?;
            crate::hostlag::latency_bound(
                &format!(
                    "{NAME}: round {round}: a write across the upgrade, less the upgrade's {took:?}"
                ),
                longest_across_at,
                longest_across,
                took,
                took + across_bound,
            )?;
        }
        ensure_no_loss(NAME, a, b, (&a_base, &serde_json::Value::Null), None)?;
        w.finish();
        print_deleg(NAME, "a", &deleg_of(a)?);
        print_deleg(NAME, "b", &deleg_of(b)?);
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// `p` (0..1) of `v`, `ZERO` when empty.
fn quantile(v: &[Duration], p: f64) -> Duration {
    if v.is_empty() {
        return Duration::ZERO;
    }
    let mut s = v.to_vec();
    s.sort();
    s[((s.len() - 1) as f64 * p) as usize]
}

/// Arm the store-stall hook of the daemon `pid` (`CONSTELLATION_FAULT_STORE_STALL_FILE`):
/// the first ordinary metadata write it makes holds the store's writer
/// lock for `ms`. Written whole (a reader that sees a prefix stalls for
/// the wrong time).
fn arm_store_stall(file: &std::path::Path, ms: u64, pid: u32) -> Result<()> {
    let tmp = file.with_extension("tmp");
    std::fs::write(&tmp, format!("{ms} once pid={pid}\n"))?;
    std::fs::rename(&tmp, file)?;
    Ok(())
}

/// A delegate whose core is stuck behind a slow store write keeps its
/// delegation. One ordinary metadata write on `b` (the writer's create)
/// holds the store's writer lock for 5, 9 and 12 s
/// (`CONSTELLATION_FAULT_STORE_STALL_FILE`); the core's step meanwhile (as
/// `a`'s backup it persists the tail `a` appends of its own stream) waits
/// behind that write, so it sends no renewal for longer than the grant's
/// renewal slack (about 3.5 s at the 5 s ttl here): the 2026-10-17 shape
/// (a 6.7-11.9 s journal write on a loaded btrfs) made deterministic. The
/// off-core renewal lane (`DelegLane`) renews meanwhile, up to the hung
/// bound. Asserted: the same generation before and after, none reclaimed or
/// recalled at `a`, none lapsed at `b`, no write failed, the lane sent
/// (`renewals_off_core`; for the stalls longer than the slack) and the
/// writes after the stall are back to the control's create + close
/// latency (the median within twice the control's p90 plus 5 ms: a lost
/// delegation sends every write through the root, 4 to 12 times slower).
/// Then one 25 s stall, past the hung bound: the lane stops at 15 s, its
/// last grant runs out, the root reclaims (a hung delegate is a dead one,
/// as it always was), no write fails and `b`'s writes go on through the
/// root. `CONSTELLATION_DELEG_LANE=off` is the fail-before: the 9 and 12 s
/// stalls then lose the delegation.
pub fn delegate_store_stall(_seed: u64) -> Result<()> {
    const NAME: &str = "delegate-store-stall";
    // `CONSTELLATION_HARNESS_DELEG_STALLS_MS=9000,12000` runs those stalls
    // only (the A/B runs of the fail-before).
    let stalls: Vec<u64> = std::env::var("CONSTELLATION_HARNESS_DELEG_STALLS_MS")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty())
        .unwrap_or_else(|| vec![5_000, 9_000, 12_000, 25_000]);
    let stall_dir = tempfile::tempdir()?;
    let file = stall_dir.path().join("store-stall");
    let file_str = file.display().to_string();
    let (_env, _root, mut clients) = delegated_pair_with(
        NAME,
        true,
        &[("CONSTELLATION_FAULT_STORE_STALL_FILE", &file_str)],
    )?;
    let result = (|| -> Result<()> {
        delegate_d1(NAME, &clients, true)?;
        let a = &clients[0];
        let b = &clients[1];
        let pid = b.pid().context("b has no daemon")?;
        let w = Writer::start(b.mnt.clone(), "d1");
        w.wait_ops_after(Duration::ZERO, 20, Duration::from_secs(30))?;
        let delta = |d: &serde_json::Value, d0: &serde_json::Value, k: &str| {
            n(d, k).saturating_sub(n(d0, k))
        };
        // The slack the lane is for: the stalls past it would have lost
        // the grant (renewal due at half the ttl, honoured to ttl - 1 s).
        let slack = Duration::from_millis(3_500);
        let hung = holder_stall_bound();
        for ms in stalls {
            let past_bound = Duration::from_millis(ms) > hung;
            let Some(gen) = ensure_delegated(NAME, a, b, None)? else {
                break;
            };
            // The control: the writes of the last 3 s before the stall.
            let control_from = w.now();
            std::thread::sleep(Duration::from_secs(3));
            let control_meta = w.meta_after(control_from);
            let control_p90 = quantile(&control_meta, 0.9);
            let (da0, db0) = (deleg_of(a)?, deleg_of(b)?);
            let armed_at = w.now();
            arm_store_stall(&file, ms, pid)?;
            std::thread::sleep(Duration::from_millis(ms) + Duration::from_secs(3));
            // The writes after the stall: the longest wait of the writer
            // is the stall itself; then back to normal.
            let back_from = armed_at + Duration::from_millis(ms) + Duration::from_secs(1);
            w.wait_ops_after(back_from, 20, Duration::from_secs(60))?;
            let (ops, errs) = w.after(armed_at);
            let after_meta = w.meta_after(back_from);
            let after_p50 = quantile(&after_meta, 0.5);
            let (da, db) = (deleg_of(a)?, deleg_of(b)?);
            let gen_after = held_generation(&db);
            let (_, recent_step) = core_steps(b)?;
            let lane = delta(&db, &db0, "renewals_off_core");
            let reclaimed = delta(&da, &da0, "reclaimed");
            let recalls = delta(&da, &da0, "recalls_sent");
            eprintln!(
                "    {NAME}: a {ms} ms store stall{}: b's core stuck {recent_step:?} at most; \
                 the lane sent {lane} renewals; generation {gen} -> {gen_after:?}; at a: \
                 reclaimed +{reclaimed} recalls +{recalls}; at b: lapsed {}; writes across it: \
                 {}; create + close before: p50 {:?} p90 {control_p90:?}, after: p50 \
                 {after_p50:?}",
                if past_bound {
                    " (past the hung bound)"
                } else {
                    ""
                },
                n(&db, "lapsed"),
                dist(&ops),
                quantile(&control_meta, 0.5),
            );
            anyhow::ensure!(errs.is_empty(), "writes failed across the stall: {errs:?}");
            if past_bound {
                // A hung delegate is a dead one: the lane stops at the
                // bound, its last grant runs out, the root reclaims.
                anyhow::ensure!(
                    recent_step >= hung,
                    "a {ms} ms store stall never held b's core past the hung bound (longest \
                     step {recent_step:?}): the scenario did not exercise it"
                );
                anyhow::ensure!(
                    reclaimed + recalls > 0,
                    "the root kept the delegation of a delegate hung for {recent_step:?}, past \
                     the bound: the lane renewed without end: a {da} b {db}"
                );
                continue;
            }
            let l = Losses::since(&da, &da0, &db, &db0);
            let lost = gen_after != Some(gen) || l.any();
            if lost {
                // A loss the daemon classified as under a freeze of its
                // whole process, of this generation, in this iteration (the
                // counters are deltas over it): a note, and the iteration
                // is skipped (nothing to judge). Anything else fails.
                let Some(note) = l.excused(Some(gen)) else {
                    return Err(l.failure(
                        &format!(
                            "b's delegation did not survive a {ms} ms store stall (generation \
                             {gen} -> {gen_after:?})"
                        ),
                        Some(gen),
                        &format!(": a {da} b {db}"),
                    ));
                };
                // Nothing was judged in this iteration: a note, and the run
                // is not a pass.
                excused_note(NAME, &format!("a {ms} ms store stall: {note}"));
                continue;
            }
            if Duration::from_millis(ms) > slack {
                anyhow::ensure!(
                    recent_step >= Duration::from_millis(ms) * 4 / 5,
                    "a {ms} ms store stall never held b's core (longest step {recent_step:?}): \
                     the scenario did not exercise the lane"
                );
                anyhow::ensure!(
                    lane > 0,
                    "b's core was stuck {recent_step:?} and the off-core lane sent no renewal"
                );
            }
            anyhow::ensure!(
                after_p50 <= control_p90 * 2 + Duration::from_millis(5),
                "after a {ms} ms store stall b's create + close p50 is {after_p50:?}, against \
                 the control's p90 {control_p90:?}: the writes no longer run on the delegate"
            );
        }
        w.finish();
        print_deleg(NAME, "a", &deleg_of(a)?);
        print_deleg(NAME, "b", &deleg_of(b)?);
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn losses(
        reclaimed: u64,
        lapsed: u64,
        lost: u64,
        frozen: (u64, u64),
        silent: (u64, u64),
    ) -> Losses {
        let a = json!({"reclaimed": reclaimed, "recalls_sent": 0});
        let b = json!({
            "lapsed": lapsed,
            "grants_lost": lost,
            "lapses_during_freeze": frozen.0,
            "last_freeze_gen": frozen.1,
            "last_freeze_ms": 5_700,
            "lapses_root_silent": silent.0,
            "last_silent_gen": silent.1,
        });
        Losses::since(&a, &json!({}), &b, &json!({}))
    }

    /// Only a freeze the daemon classified, of that generation, covering every
    /// grant lost, excuses a loss.
    #[test]
    fn only_a_classified_freeze_of_the_generation_covering_every_loss_excuses_it() {
        assert!(losses(1, 1, 1, (1, 7), (0, 0)).excused(Some(7)).is_some());
        // Another generation's freeze.
        assert!(losses(1, 1, 1, (1, 6), (0, 0)).excused(Some(7)).is_none());
        // A loss with nothing classified.
        assert!(losses(1, 1, 1, (0, 0), (0, 0)).excused(Some(7)).is_none());
        // Two grants lost (a lapse and a refusal: `lost` counts both, `lapsed`
        // only the lapse), one under a freeze: like with like, not excused.
        assert!(losses(2, 1, 2, (1, 7), (0, 0)).excused(Some(7)).is_none());
        // Two reclaims at the root, one freeze at the delegate.
        assert!(losses(2, 1, 1, (1, 7), (0, 0)).excused(Some(7)).is_none());
        // A silent root is never an excuse.
        assert!(losses(1, 1, 1, (0, 0), (1, 7)).excused(Some(7)).is_none());
        assert!(losses(2, 2, 2, (1, 7), (1, 7)).excused(Some(7)).is_none());
    }

    /// A loss that was a silent root fails with its own greppable label; one
    /// nothing classified fails without it.
    #[test]
    fn a_silent_root_fails_with_the_known_gap_label_and_nothing_else_does() {
        let e = losses(0, 1, 1, (0, 0), (1, 7)).failure("w", Some(7), "");
        assert!(e.to_string().contains(KNOWN_GAP), "{e}");
        // A freeze and a silent root together cover the losses: the silent
        // one still fails, by the label.
        let e = losses(2, 2, 2, (1, 7), (1, 7)).failure("w", Some(7), "");
        assert!(e.to_string().contains(KNOWN_GAP), "{e}");
        // Nothing classified, or a loss beyond what was classified: no label.
        let e = losses(1, 1, 1, (0, 0), (0, 0)).failure("w", Some(7), "");
        assert!(!e.to_string().contains(KNOWN_GAP), "{e}");
        let e = losses(2, 2, 2, (0, 0), (1, 7)).failure("w", Some(7), "");
        assert!(!e.to_string().contains(KNOWN_GAP), "{e}");
        // A silent root of another generation.
        let e = losses(1, 1, 1, (0, 0), (1, 6)).failure("w", Some(7), "");
        assert!(!e.to_string().contains(KNOWN_GAP), "{e}");
    }

    /// An excused loss (a freeze, the hung bound) leaves the run INCONCLUSIVE,
    /// not passed (the scenario's notes become the outcome, `verdict` in
    /// `main.rs`); only a run that injects its own freeze takes the note as
    /// what it is for.
    #[test]
    fn an_excused_loss_leaves_the_run_inconclusive_unless_the_freeze_was_injected() {
        let n = excused_inconclusive("t", "a loss permitted", false).expect("no note");
        assert!(n.contains("not judged"), "{n}");
        assert!(excused_inconclusive("t", "a loss permitted", true).is_none());
    }
}
