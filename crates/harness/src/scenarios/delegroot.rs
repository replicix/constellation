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

use super::m11::{
    cluster, deleg_of, delegate, dump_logs_on_failure, holder_of, n, node_id, print_deleg,
    unmount_all, wait_installed,
};
use super::{eventually, lease_of};
use crate::client::Client;
use anyhow::{Context, Result};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The production timings that matter here (kind runs the defaults): a
/// 60 s root lease and a 5 s delegation grant, where the M11 scenarios
/// shorten both.
const ENV: &[(&str, &str)] = &[
    ("CONSTELLATION_LEASE_TTL_MS", "60000"),
    ("CONSTELLATION_DELEGATION_TTL_MS", "5000"),
    ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "50"),
    // `daemon --upgrade` detaches a dev-fuse session only.
    ("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse"),
];

/// How long a write + `fsync` + close may take right after a handoff of
/// the delegate. The old image's delegation is re-adopted with the node
/// identity, so no write waits for the root to reclaim it: that wait was
/// the grant's ttl (5 s) and its margin, 3.4-4.7 s on kind and up to 7 s
/// in the harness.
const HANDOFF_BOUND: Duration = Duration::from_secs(3);

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
    let mut env_vars = ENV.to_vec();
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
/// death), plus the inbox and escalation steps. Each plus slack for a
/// loaded host. On kind the wait was over four minutes.
const FAST_BOUND: Duration = Duration::from_secs(8);

fn ttl_bound(lease_ttl: Duration) -> Duration {
    lease_ttl + Duration::from_secs(3) + Duration::from_secs(12)
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
                break w.wait_ops_after(past, 10, ttl_bound(lease_ttl));
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
        let (mut failover, mut later) = (Vec::new(), Vec::new());
        for o in w.ops.lock().unwrap().iter() {
            if o.at + o.took < killed_at {
                continue;
            }
            if o.at <= settled {
                failover.push(o.took);
            } else {
                later.push(o.took);
            }
        }
        let total = w.ops.lock().unwrap().len() as u64;
        w.finish();
        let longest = failover.iter().max().copied().unwrap_or_default();
        // A listed backup takes over by seal.
        let fast = n(&ack, "backup_takeovers") >= 1;
        let bound = if fast {
            FAST_BOUND
        } else {
            ttl_bound(lease_ttl)
        };
        eprintln!(
            "    {name}: b's writes across the root's loss: {} ({} takeover, bound {bound:?}; b \
             held the root {:?} after the {}); afterwards: {}; {} inherited generations, {} \
             reclaimed",
            dist(&failover),
            if fast { "seal-based" } else { "TTL" },
            held_at.map(|t| t.saturating_sub(killed_at)),
            if freeze { "freeze" } else { "kill" },
            dist(&later),
            n(&db, "inherited"),
            n(&db, "reclaimed")
        );
        anyhow::ensure!(
            errs.is_empty(),
            "writes failed across the failover: {errs:?}"
        );
        anyhow::ensure!(took_over, "b never took the root over");
        anyhow::ensure!(
            longest <= bound,
            "a write + fsync took {longest:?} across the root's loss (bound {bound:?})"
        );
        let longest_later = later.iter().max().copied().unwrap_or_default();
        anyhow::ensure!(
            longest_later <= ttl_bound(lease_ttl),
            "a write + fsync on the new root took {longest_later:?} (bound {:?})",
            ttl_bound(lease_ttl)
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
        Ok(())
    })();
    dump_logs_on_failure(name, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// `b`'s live, renewed generation of `/d1`, delegating it again first
/// if the host lost it before a round (a core step blocked for seconds
/// on a host at load 200+ outlasts the grant: then the root reclaims it,
/// which is not what this scenario measures, and is logged).
fn ensure_delegated(name: &str, a: &Client, b: &Client) -> Result<u64> {
    let b_id = node_id(b)?;
    let held = |d: &serde_json::Value| -> Option<u64> {
        d["mine"].as_array()?.iter().find_map(|m| {
            (m[3] == false && m[2].as_i64().unwrap_or(0) > 0).then(|| m[1].as_u64().unwrap_or(0))
        })
    };
    if let Some(gen) = held(&deleg_of(b)?) {
        return Ok(gen);
    }
    eprintln!("    {name}: b lost its delegation before the round (host load); delegating again");
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
        held(&deleg_of(b)?)
            .map(|_| ())
            .context("neither ended nor held")
    })?;
    if held(&deleg_of(b)?).is_none() {
        delegate(a, "/d1", b_id)?;
    }
    let mut gen = 0;
    eventually("b renewed its delegation", Duration::from_secs(30), || {
        gen = held(&deleg_of(b)?).context("not held and renewed yet")?;
        Ok(())
    })?;
    Ok(gen)
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
/// was the grant's ttl and more, and the mechanism checks catch it).
pub fn delegate_handoff_renewal(_seed: u64) -> Result<()> {
    const NAME: &str = "delegate-handoff-renewal";
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
        for round in 0..3 {
            let gen = ensure_delegated(NAME, a, b)?;
            // The control: the writes of the last 3 s before the upgrade.
            let control_from = w.now();
            std::thread::sleep(Duration::from_secs(3));
            let (control, _) = w.after(control_from);
            let control_meta = w.meta_after(control_from);
            let da0 = deleg_of(a)?;
            let up_at = w.now();
            let took = super::handover::upgrade(b).context("upgrading b")?;
            let resumed_at = w.now();
            w.wait_ops_after(resumed_at, 20, Duration::from_secs(60))?;
            // The writes answered after the upgrade returned: the resumed
            // image's first ones.
            let (ops, errs) = w.after(resumed_at);
            let (across, _) = w.after(up_at);
            let meta = w.meta_after(resumed_at);
            let longest = meta.iter().max().copied().unwrap_or_default();
            let slowest_control = control_meta.iter().max().copied().unwrap_or_default();
            // (A host stall slows the writes before the upgrade too.)
            let bound = HANDOFF_BOUND.max(slowest_control * 2);
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
            anyhow::ensure!(
                longest <= bound,
                "a create + close after the resume took {longest:?} (bound {bound:?})"
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
