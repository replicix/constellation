//! Plan 30 §M9 scenarios: a backup peer within the RTT budget, seal-based
//! failover before the lease expires, `ack=s3` with fast takeover, the
//! delegation horizon across a fast failover, a backup that departs or is
//! partitioned, no peer in budget, and the single node — where M9 must be
//! a strict no-op.
//!
//! Every scenario prints the nodes' `status.ack` block and the
//! measurements the plan asks for: the acknowledgement latency on a LAN
//! under each policy, S3 requests per reconfiguration, and the
//! failover-time distribution.

use super::m8::{dist, read_timed, write_timed};
use super::{ensure_no_conflicts, eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::BUCKET;
use anyhow::{bail, Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn ack_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["ack"].clone())
}

fn n(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn node_id(c: &Client) -> Result<u64> {
    c.control_status()?["node_id"]
        .as_u64()
        .with_context(|| format!("{} reports no node id", c.name))
}

fn print_ack(scenario: &str, who: &str, s: &serde_json::Value) {
    eprintln!(
        "    {scenario}: {who} ack: policy {} (mount ack=s3 {}) backups {} candidate {} \
         config_version {} durable {} parked {} gated {} | backing {}@{} through {} sealed {} | \
         added {} removed {} reconfig-cas {} appends {} acks {} timeouts {} acks-waited {} \
         (ms total {}) aborted {} streamed {}/{} installed {} dropped {} persisted {} seals {} \
         takeovers {} tail-applied {} s3-fast-takeovers {} floor-waits {} stale-refusals {} \
         reads-durability-blocked {}",
        s["policy"],
        s["ack_s3"],
        s["backups"],
        s["candidate"],
        n(s, "config_version"),
        s["durable"],
        n(s, "parked_acks"),
        s["gated"],
        n(s, "backing_holder"),
        n(s, "backing_epoch"),
        n(s, "backing_acked"),
        n(s, "sealed_epoch"),
        n(s, "backups_added"),
        n(s, "backups_removed"),
        n(s, "reconfig_cas"),
        n(s, "backup_appends"),
        n(s, "backup_acks"),
        n(s, "backup_ack_timeouts"),
        n(s, "acks_waited"),
        n(s, "ack_wait_ms_total"),
        n(s, "acks_aborted"),
        n(s, "streamed_ahead"),
        n(s, "streamed_installed"),
        n(s, "streamed_installed"),
        n(s, "streamed_dropped"),
        n(s, "backup_persisted"),
        n(s, "seals"),
        n(s, "backup_takeovers"),
        n(s, "backup_tail_applied"),
        n(s, "s3_fast_takeovers"),
        n(s, "ack_floor_waits"),
        n(s, "stale_liveness_refusals"),
        n(s, "reads_durability_blocked"),
    );
}

/// The lease TTL every M9 scenario mounts with: long enough that a
/// takeover well inside it can only be seal-based (or `ack=s3`'s fast
/// path), never expiry.
const TTL_MS: u64 = 20_000;

/// `names` nodes on a fresh filesystem with `extra` env on every mount
/// (M9's knobs are per mount); node 0 holds the lease and `f` exists
/// everywhere. The first `counting` nodes reach S3 through a counting
/// proxy each (returned in order).
fn cluster(
    scenario: &str,
    names: &[&str],
    extra: &[(&str, &str)],
    counting: usize,
) -> Result<(
    crate::s3env::S3Env,
    tempfile::TempDir,
    Vec<Client>,
    Vec<CountingProxy>,
)> {
    let (env, root) = setup(scenario)?;
    let backend = format!("s3://{BUCKET}/{scenario}-{}", ts());
    let ttl = TTL_MS.to_string();
    let mut clients = Vec::new();
    let mut proxies = Vec::new();
    if counting > 0 {
        // The counting relay chains through toxiproxy's `s3` route, which
        // exists only once created (and lives as long as the environment).
        env.s3_proxy()?;
    }
    for _ in 0..counting.min(names.len()) {
        proxies.push(env.counting_proxy()?);
    }
    for (i, name) in names.iter().enumerate() {
        let endpoint = proxies
            .get(i)
            .map(|p| p.endpoint())
            .unwrap_or_else(|| env.direct_endpoint.clone());
        let mut c = Client::new(root.path(), name, &endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", &ttl)
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
            .with_env(
                "CONSTELLATION_FAULT_P2P_DENY_FILE",
                &c_deny_path(root.path(), name).display().to_string(),
            );
        for (k, v) in extra {
            c = c.with_env(k, v);
        }
        clients.push(c);
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount()?;
    }
    if clients.len() > 1 {
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
    }
    std::fs::write(clients[0].mnt.join("f"), b"initial")?;
    eventually("node 0 holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&clients[0])?;
        anyhow::ensure!(lease["held"] == true, "node 0 does not hold: {lease}");
        Ok(())
    })?;
    for c in &clients[1..] {
        eventually("f visible everywhere", Duration::from_secs(30), || {
            anyhow::ensure!(
                std::fs::read(c.mnt.join("f")).ok().as_deref() == Some(&b"initial"[..]),
                "f not visible on {}",
                c.name
            );
            Ok(())
        })?;
    }
    Ok((env, root, clients, proxies))
}

/// The P2P deny file of `name` (`fault::p2p_denied`): peers listed in it
/// are unreachable from that node. Absent until a scenario cuts a link.
fn c_deny_path(root: &std::path::Path, name: &str) -> std::path::PathBuf {
    root.join(name).join("deny")
}

fn unmount_all(clients: &mut [Client]) {
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
}

/// Wait until `c` lists at least one backup in its lease; returns them.
pub(super) fn wait_for_backup(c: &Client, deadline: Duration) -> Result<Vec<u64>> {
    let mut backups = Vec::new();
    eventually(&format!("{} lists a backup", c.name), deadline, || {
        let a = ack_of(c)?;
        backups = a["backups"]
            .as_array()
            .map(|b| b.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default();
        anyhow::ensure!(
            !backups.is_empty() && a["policy"] == "backup",
            "{} has no backup yet: {a}; p2p {}",
            c.name,
            c.control_status()?["p2p"]
        );
        Ok(())
    })?;
    Ok(backups)
}

/// The client whose node id is `id`.
fn by_id(clients: &[Client], id: u64) -> Result<&Client> {
    for c in clients {
        if node_id(c)? == id {
            return Ok(c);
        }
    }
    bail!("no client has node id {id}")
}

/// Wait until `c` holds the lease at an epoch above `after`; how long it
/// took.
fn wait_holds(c: &Client, after: u64, deadline: Duration) -> Result<Duration> {
    let t = Instant::now();
    loop {
        if let Ok(l) = lease_of(c) {
            if l["held"] == true && l["epoch"].as_u64().unwrap_or(0) > after {
                return Ok(t.elapsed());
            }
        }
        if t.elapsed() > deadline {
            bail!(
                "{} did not take the lease over within {deadline:?}: {}",
                c.name,
                lease_of(c).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Every `names` file reads as its own name on `c`.
fn all_visible(c: &Client, names: &[String], deadline: Duration) -> Result<()> {
    eventually(&format!("{} sees every file", c.name), deadline, || {
        for name in names {
            let got = std::fs::read(c.mnt.join(name))
                .with_context(|| format!("{} reading {name}", c.name))?;
            anyhow::ensure!(
                got == name.as_bytes(),
                "{} read {name} as {:?}",
                c.name,
                String::from_utf8_lossy(&got)
            );
        }
        Ok(())
    })
}

/// `count` files `<tag>-<i>` (content = name) written on `c`; the
/// open+write+close latencies.
fn write_files(c: &Client, tag: &str, count: usize) -> Result<(Vec<String>, Vec<Duration>)> {
    let mut names = Vec::new();
    let mut lat = Vec::new();
    for i in 0..count {
        let name = format!("{tag}-{i}");
        lat.push(write_timed(&c.mnt.join(&name), name.as_bytes())?);
        names.push(name);
    }
    Ok((names, lat))
}

const ROUNDS: usize = 3;
const FILES: usize = 30;

/// A backup peer takes an unexpired lease over by sealing when the
/// holder dies: three rounds of "write 30 files on the holder, kill it,
/// the backup holds within a few seconds (the TTL is 20 s), every
/// acknowledged file is on the new holder, the dead node remounts and
/// converges". Prints the failover-time distribution and the
/// acknowledgement latency under the `backup` policy.
///
/// Four nodes, so that every round's backup is a node that has never
/// been restarted: a node killed and remounted with the same identity
/// is not reachable over P2P by the others for a long while (its pings
/// fail and it never rejoins gossip on their side; see the M9 notes),
/// which is the P2P layer's business, not this scenario's — the
/// remounted nodes converge over S3 here.
pub fn backup_failover(_seed: u64) -> Result<()> {
    const NAME: &str = "backup-failover";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c", "d"], &[], 0)?;
    let result = (|| -> Result<()> {
        let mut holder = 0usize;
        let mut failovers = Vec::new();
        let mut ack_latency = Vec::new();
        let mut all = Vec::new();
        for round in 0..ROUNDS {
            let h = &clients[holder];
            let backups = wait_for_backup(h, Duration::from_secs(30))?;
            let backup_id = backups[0];
            let backup_idx = (0..clients.len())
                .find(|i| node_id(&clients[*i]).ok() == Some(backup_id))
                .context("backup is not one of ours")?;
            let epoch = lease_of(h)?["epoch"].as_u64().unwrap_or(0);
            eprintln!(
                "    {NAME}: round {round}: {} holds epoch {epoch}, backup {}",
                h.name, clients[backup_idx].name
            );
            let (names, lat) = write_files(h, &format!("r{round}"), FILES)?;
            ack_latency.extend(lat);
            all.extend(names.clone());
            print_ack(NAME, &h.name, &ack_of(h)?);
            // The holder dies with (most likely) unshipped rows: the
            // backup's tail is what carries them.
            clients[holder].kill9()?;
            let killed = Instant::now();
            let took = wait_holds(&clients[backup_idx], epoch, Duration::from_secs(15))?;
            let b = &clients[backup_idx];
            eprintln!(
                "    {NAME}: round {round}: {} holds epoch {} after {took:?}",
                b.name,
                lease_of(b)?["epoch"]
            );
            anyhow::ensure!(
                took < Duration::from_millis(TTL_MS / 2),
                "takeover took {took:?}: not seal-based (TTL {TTL_MS} ms)"
            );
            failovers.push(took);
            // The lease is held before the takeover gate re-applies the
            // tail (the gate runs in the rounds after the claim): give
            // the counters a moment.
            let a = {
                let t = Instant::now();
                loop {
                    let a = ack_of(b)?;
                    if (n(&a, "backup_takeovers") >= 1 && n(&a, "seals") >= 1)
                        || t.elapsed() > Duration::from_secs(10)
                    {
                        break a;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            };
            print_ack(NAME, &b.name, &a);
            anyhow::ensure!(
                n(&a, "backup_takeovers") >= 1,
                "{} did not take over as a backup: {a}",
                b.name
            );
            anyhow::ensure!(n(&a, "seals") >= 1, "{} did not seal: {a}", b.name);
            // Every acknowledged file is on the new holder, at once.
            all_visible(b, &names, Duration::from_secs(10))?;
            let _ = killed;
            // The dead node comes back and converges (its stranded
            // journal replays by rid: the rows the tail re-shipped are
            // already completed).
            clients[holder].mount()?;
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
            for c in &clients {
                all_visible(c, &all, Duration::from_secs(60))?;
            }
            eprintln!(
                "    {NAME}: round {round}: {} remounted and converged; failover {took:?}",
                clients[holder].name
            );
            holder = backup_idx;
        }
        eprintln!("    {NAME}: failover time {}", dist(failovers));
        eprintln!(
            "    {NAME}: holder write latency under the backup policy {}",
            dist(ack_latency)
        );
        let refs: Vec<&Client> = clients.iter().collect();
        ensure_no_conflicts(&refs)?;
        for c in &clients {
            print_ack(NAME, &c.name, &ack_of(c)?);
        }
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// A backup departs (clean unmount): the holder reconfigures it out and
/// a replacement in, acknowledging throughout. Prints the S3 requests
/// the reconfiguration cost (a counting proxy on the holder: lease PUTs
/// beyond the renewals) and the write latency during it.
pub fn backup_departs(_seed: u64) -> Result<()> {
    const NAME: &str = "backup-departs";
    let (_env, _root, mut clients, proxies) = cluster(NAME, &["a", "b", "c"], &[], 1)?;
    let counter = &proxies[0];
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let backups = wait_for_backup(a, Duration::from_secs(30))?;
        let first = backups[0];
        let departing = (1..clients.len())
            .find(|i| node_id(&clients[*i]).ok() == Some(first))
            .context("backup is not one of ours")?;
        let (_, warm) = write_files(a, "warm", 20)?;
        let before = ack_of(a)?;
        print_ack(NAME, "A", &before);
        counter.reset();
        eprintln!("    {NAME}: backup {} departs", clients[departing].name);
        clients[departing].unmount()?;
        let departed = Instant::now();
        // Writes on A keep completing while it reconfigures.
        let mut during = Vec::new();
        let mut names = Vec::new();
        let mut replaced = None;
        while departed.elapsed() < Duration::from_secs(20) {
            let name = format!("during-{}", names.len());
            during.push(write_timed(&clients[0].mnt.join(&name), name.as_bytes())?);
            names.push(name);
            let ack = ack_of(&clients[0])?;
            let now_backups: Vec<u64> = ack["backups"]
                .as_array()
                .map(|b| b.iter().filter_map(|v| v.as_u64()).collect())
                .unwrap_or_default();
            if !now_backups.is_empty() && !now_backups.contains(&first) {
                replaced = Some(departed.elapsed());
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let replaced = replaced.context("A never listed a replacement backup")?;
        let after = ack_of(&clients[0])?;
        let reqs = counter.requests();
        counter.ensure_sane()?;
        let lease_puts = reqs
            .iter()
            .filter(|r| r.method == "PUT" && r.path().contains("leases/"))
            .count();
        let tally = crate::reqlog::tally(&reqs);
        eprintln!(
            "    {NAME}: replacement listed {replaced:?} after the departure; S3 during it: \
             {} requests (lease PUTs {lease_puts}, GET {}, PUT {}, LIST {}); reconfiguration CAS \
             {} (removed {}, added {})",
            tally.total(),
            tally.get,
            tally.put,
            tally.list,
            n(&after, "reconfig_cas") - n(&before, "reconfig_cas"),
            n(&after, "backups_removed") - n(&before, "backups_removed"),
            n(&after, "backups_added") - n(&before, "backups_added"),
        );
        eprintln!(
            "    {NAME}: write latency before {} / during the reconfiguration {}",
            dist(warm),
            dist(during)
        );
        print_ack(NAME, "A", &after);
        anyhow::ensure!(
            n(&after, "backups_removed") > n(&before, "backups_removed"),
            "A did not remove the departed backup: {after}"
        );
        anyhow::ensure!(
            n(&after, "reconfig_cas") - n(&before, "reconfig_cas") <= 4,
            "a departure cost more than 4 reconfiguration CAS: {after}"
        );
        // The departed node comes back; everything written meanwhile
        // reaches it.
        clients[departing].mount()?;
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        all_visible(&clients[departing], &names, Duration::from_secs(60))?;
        ensure_no_conflicts(&refs)?;
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// No peer within the RTT budget (`CONSTELLATION_BACKUP_RTT_BUDGET_MS=0`):
/// today's behaviour — local acknowledgements, no backup, no seal, and a
/// dead holder is replaced only when its lease expires.
pub fn no_peer_in_budget(_seed: u64) -> Result<()> {
    const NAME: &str = "no-peer-in-budget";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let (names, lat) = write_files(&clients[0], "local", 50)?;
        std::thread::sleep(Duration::from_secs(4));
        let a = ack_of(&clients[0])?;
        print_ack(NAME, "A", &a);
        eprintln!(
            "    {NAME}: holder write latency with no peer in budget {}",
            dist(lat)
        );
        anyhow::ensure!(
            a["policy"] == "local",
            "A is not under the local policy: {a}"
        );
        anyhow::ensure!(
            a["backups"].as_array().is_some_and(|b| b.is_empty()) && a["candidate"].is_null(),
            "A has a backup or candidate with a 0 ms budget: {a}"
        );
        anyhow::ensure!(
            n(&a, "backups_added") == 0 && n(&a, "reconfig_cas") == 0,
            "{a}"
        );
        anyhow::ensure!(a["gated"] == false, "A's fast path is gated: {a}");
        anyhow::ensure!(n(&a, "acks_waited") == 0, "acknowledgements waited: {a}");
        all_visible(&clients[1], &names, Duration::from_secs(30))?;
        // A dies: B's write waits for the lease to expire (no seal, no
        // fast takeover) — at least TTL minus what has elapsed since A's
        // last renewal, i.e. seconds, not the backup takeover's 1.5 s.
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        clients[0].kill9()?;
        let t = Instant::now();
        std::fs::write(clients[1].mnt.join("after"), b"after")?;
        let took = t.elapsed();
        let b = ack_of(&clients[1])?;
        print_ack(NAME, "B", &b);
        eprintln!("    {NAME}: B's first write after A's death returned after {took:?}");
        anyhow::ensure!(
            lease_of(&clients[1])?["epoch"].as_u64().unwrap_or(0) > epoch,
            "B did not take over"
        );
        anyhow::ensure!(
            n(&b, "backup_takeovers") == 0
                && n(&b, "seals") == 0
                && n(&b, "s3_fast_takeovers") == 0,
            "B took over as a backup / fast: {b}"
        );
        anyhow::ensure!(
            took >= Duration::from_secs(3),
            "B took over {took:?} after A's death: before the lease could have expired"
        );
        clients[0].mount()?;
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        ensure_no_conflicts(&refs)?;
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// `--ack s3` (through `CONSTELLATION_ACK`): every acknowledgement waits
/// for the record to land in the log; a silent holder (frozen) is taken
/// over well inside its lease, and every acknowledged file is on the
/// successor; the thawed holder is deposed with no conflict.
pub fn ack_s3_failover(_seed: u64) -> Result<()> {
    const NAME: &str = "ack-s3-failover";
    let (_env, _root, mut clients, _) =
        cluster(NAME, &["a", "b", "c"], &[("CONSTELLATION_ACK", "s3")], 0)?;
    let result = (|| -> Result<()> {
        let (names, lat) = write_files(&clients[0], "s3", FILES)?;
        let a = ack_of(&clients[0])?;
        print_ack(NAME, "A", &a);
        eprintln!(
            "    {NAME}: holder write latency under ack=s3 {}",
            dist(lat.clone())
        );
        anyhow::ensure!(a["policy"] == "s3", "A is not under the s3 policy: {a}");
        anyhow::ensure!(
            a["gated"] == true,
            "A's fast path is open under ack=s3: {a}"
        );
        anyhow::ensure!(
            n(&a, "acks_waited") >= 1,
            "no acknowledgement waited for the log: {a}"
        );
        // Every acknowledged file is in the log: a fresh reader through
        // S3 alone sees it (C's replica follows the log).
        all_visible(&clients[2], &names, Duration::from_secs(30))?;
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        eprintln!("    {NAME}: freezing A (SIGSTOP)");
        clients[0].pause()?;
        let frozen = Instant::now();
        let t = Instant::now();
        std::fs::write(clients[1].mnt.join("after-freeze"), b"after-freeze")
            .context("B's write after A froze")?;
        let took = t.elapsed();
        let b_lease = lease_of(&clients[1])?;
        let b = ack_of(&clients[1])?;
        print_ack(NAME, "B", &b);
        eprintln!("    {NAME}: B's write returned {took:?} after A froze; B's lease {b_lease}");
        anyhow::ensure!(
            b_lease["held"] == true && b_lease["epoch"].as_u64().unwrap_or(0) > epoch,
            "B does not hold a newer epoch: {b_lease}"
        );
        anyhow::ensure!(
            frozen.elapsed() < Duration::from_millis(TTL_MS / 2),
            "the takeover took {:?}: not the fast path (TTL {TTL_MS} ms)",
            frozen.elapsed()
        );
        anyhow::ensure!(
            n(&b, "s3_fast_takeovers") >= 1,
            "B did not take over fast: {b}"
        );
        all_visible(&clients[1], &names, Duration::from_secs(10))?;
        clients[0].resume()?;
        eventually("A learns it was deposed", Duration::from_secs(30), || {
            let l = lease_of(&clients[0])?;
            anyhow::ensure!(l["held"] == false, "A still holds: {l}");
            Ok(())
        })?;
        let mut all = names.clone();
        all.push("after-freeze".to_string());
        for c in &clients {
            all_visible(c, &all, Duration::from_secs(60))?;
        }
        let refs: Vec<&Client> = clients.iter().collect();
        ensure_no_conflicts(&refs)?;
        print_ack(NAME, "A", &ack_of(&clients[0])?);
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// One node, default knobs: M9 is a no-op — local policy, no backup, no
/// candidate, the fast path open, no acknowledgement waited, no
/// reconfiguration CAS; prints the write latency and the S3 requests of
/// the workload (renewals only, beyond the segments).
pub fn single_node_unchanged(_seed: u64) -> Result<()> {
    const NAME: &str = "single-node-unchanged";
    let (_env, _root, mut clients, proxies) = cluster(NAME, &["solo"], &[], 1)?;
    let counter = &proxies[0];
    let result = (|| -> Result<()> {
        let c = &clients[0];
        let (_, warm) = write_files(c, "warm", 20)?;
        counter.reset();
        let (names, lat) = write_files(c, "solo", 200)?;
        let t = counter.tally();
        counter.ensure_sane()?;
        let lease_puts = counter
            .requests()
            .iter()
            .filter(|r| r.method == "PUT" && r.path().contains("leases/"))
            .count();
        let a = ack_of(c)?;
        print_ack(NAME, "solo", &a);
        eprintln!(
            "    {NAME}: 200 writes: latency {} (warm-up {}); S3 requests {} (PUT {} of which \
             lease PUTs {lease_puts}, GET {}, LIST {})",
            dist(lat),
            dist(warm),
            t.total(),
            t.put,
            t.get,
            t.list
        );
        anyhow::ensure!(a["policy"] == "local", "single node not under local: {a}");
        anyhow::ensure!(a["gated"] == false, "single node's fast path gated: {a}");
        anyhow::ensure!(
            a["backups"].as_array().is_some_and(|b| b.is_empty()) && a["candidate"].is_null(),
            "single node has a backup: {a}"
        );
        for key in [
            "acks_waited",
            "reconfig_cas",
            "backups_added",
            "backup_appends",
            "streamed_ahead",
            "seals",
            "parked_acks",
        ] {
            anyhow::ensure!(n(&a, key) == 0, "single node: {key} = {}: {a}", n(&a, key));
        }
        all_visible(c, &names, Duration::from_secs(5))?;
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// Plan 30 §M9's delegation horizon, strict: R holds a read delegation
/// on `f` from A; A dies; the backup takes over inside A's lease and
/// writes `f` — its acknowledgement waits until R's delegation has
/// certainly expired, so no read R starts after that acknowledgement
/// returns can show the old content. R reads `f` continuously the whole
/// time; every read that started after the write returned must see it,
/// and R never degrades a strict read.
pub fn backup_failover_with_delegation(_seed: u64) -> Result<()> {
    const NAME: &str = "backup-failover-with-delegation";
    const DELEG_TTL_MS: u64 = 3_000;
    let deleg = DELEG_TTL_MS.to_string();
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "x", "r"],
        &[
            ("CONSTELLATION_CTO", "strict"),
            ("CONSTELLATION_READ_DELEGATION_TTL_MS", &deleg),
            // A strict read outwaits the failover instead of degrading.
            ("CONSTELLATION_READ_INDEX_BUDGET_MS", "20000"),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let backups = wait_for_backup(a, Duration::from_secs(30))?;
        let backup = by_id(&clients, backups[0])?;
        let x_idx = (0..clients.len())
            .find(|i| clients[*i].name == backup.name)
            .expect("ours");
        let r_idx = (1..clients.len())
            .find(|i| *i != x_idx)
            .expect("three nodes");
        let x = &clients[x_idx];
        let r = &clients[r_idx];
        eprintln!("    {NAME}: A holds; backup {}; reader {}", x.name, r.name);
        // R takes a delegation on `f` (repeated opens: one ReadIndex,
        // then local).
        for _ in 0..5 {
            read_timed(&r.mnt.join("f")).0.context("R reading f")?;
        }
        let rc = r.control_status()?["cto"].clone();
        anyhow::ensure!(
            n(&rc, "delegations_held") >= 1 || n(&rc, "delegation_local") >= 1,
            "R holds no delegation on f: {rc}"
        );
        let epoch = lease_of(a)?["epoch"].as_u64().unwrap_or(0);
        // R reads `f` continuously; each sample is (start, content).
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(std::sync::Mutex::new(Vec::<(Instant, Vec<u8>)>::new()));
        let reader = {
            let stop = stop.clone();
            let samples = samples.clone();
            let path = r.mnt.join("f");
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let got = std::fs::read(&path).unwrap_or_default();
                    samples.lock().unwrap().push((started, got));
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };
        // A dies with R's delegation outstanding and unrecallable.
        std::thread::sleep(Duration::from_millis(300));
        read_timed(&r.mnt.join("f"))
            .0
            .context("R refreshing its delegation")?;
        let granted = Instant::now();
        clients[0].kill9()?;
        let x = &clients[x_idx];
        let r = &clients[r_idx];
        let took = wait_holds(x, epoch, Duration::from_secs(15))?;
        eprintln!("    {NAME}: {} took over after {took:?}", x.name);
        // The successor's first write of `f`: acknowledged only past the
        // delegation horizon.
        let t = Instant::now();
        std::fs::write(x.mnt.join("f"), b"after-failover").context("X writing f")?;
        let returned = Instant::now();
        let write_took = t.elapsed();
        let since_grant = returned.duration_since(granted);
        std::thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Relaxed);
        reader.join().ok();
        let xa = ack_of(x)?;
        print_ack(NAME, &x.name, &xa);
        let rc = r.control_status()?["cto"].clone();
        eprintln!(
            "    {NAME}: X's write of f returned after {write_took:?} ({since_grant:?} after R's \
             last grant; delegation TTL {DELEG_TTL_MS} ms); floor waits {}",
            n(&xa, "ack_floor_waits")
        );
        let samples = samples.lock().unwrap();
        let after: Vec<&(Instant, Vec<u8>)> =
            samples.iter().filter(|(s, _)| *s >= returned).collect();
        let stale: Vec<String> = after
            .iter()
            .filter(|(_, got)| got != b"after-failover")
            .map(|(s, got)| {
                format!(
                    "{:?} after the ack: {:?}",
                    s.duration_since(returned),
                    String::from_utf8_lossy(got)
                )
            })
            .collect();
        eprintln!(
            "    {NAME}: R sampled f {} times, {} after the ack, {} stale; R cto: degraded {} \
             read-index {} delegation-local {}",
            samples.len(),
            after.len(),
            stale.len(),
            n(&rc, "degraded"),
            n(&rc, "read_index"),
            n(&rc, "delegation_local")
        );
        for s in stale.iter().take(5) {
            eprintln!("    {NAME}:   stale: {s}");
        }
        anyhow::ensure!(
            stale.is_empty(),
            "R read stale content after X's acknowledgement"
        );
        anyhow::ensure!(
            !after.is_empty(),
            "no read sampled after the acknowledgement"
        );
        anyhow::ensure!(
            since_grant >= Duration::from_millis(DELEG_TTL_MS),
            "X acknowledged {since_grant:?} after R's grant: inside the delegation's TTL"
        );
        anyhow::ensure!(
            n(&rc, "degraded") == 0,
            "R degraded a strict read during the failover: {rc}"
        );
        clients[0].mount()?;
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        ensure_no_conflicts(&refs)?;
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// The holder and its backup are partitioned from each other (both keep
/// S3 and the third node): either the holder reconfigures the backup out
/// (and the third node in) or the sealed backup takes over — never both
/// holding, every acknowledged write kept, and the cluster converges once
/// the partition heals.
pub fn backup_partition(_seed: u64) -> Result<()> {
    const NAME: &str = "backup-partition";
    let (_env, root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let backups = wait_for_backup(a, Duration::from_secs(30))?;
        let x = by_id(&clients, backups[0])?;
        let y = clients[1..]
            .iter()
            .find(|c| c.name != x.name)
            .expect("three nodes");
        let a_id = node_id(a)?;
        let x_id = node_id(x)?;
        let (before_names, _) = write_files(a, "before", 10)?;
        let a_before = ack_of(a)?;
        let epoch = lease_of(a)?["epoch"].as_u64().unwrap_or(0);
        eprintln!(
            "    {NAME}: cutting P2P between A and its backup {}",
            x.name
        );
        std::fs::write(c_deny_path(root.path(), &a.name), format!("{x_id}\n"))?;
        std::fs::write(c_deny_path(root.path(), &x.name), format!("{a_id}\n"))?;
        let cut = Instant::now();
        // Y (reaching everyone) writes throughout the partition.
        let mut names = before_names.clone();
        let mut lat = Vec::new();
        while cut.elapsed() < Duration::from_secs(12) {
            let name = format!("part-{}", names.len());
            lat.push(write_timed(&y.mnt.join(&name), name.as_bytes())?);
            names.push(name);
            std::thread::sleep(Duration::from_millis(100));
        }
        let a_after = ack_of(a)?;
        let x_after = ack_of(x)?;
        let a_lease = lease_of(a)?;
        let x_lease = lease_of(x)?;
        print_ack(NAME, "A", &a_after);
        print_ack(NAME, &x.name, &x_after);
        let removed = n(&a_after, "backups_removed") > n(&a_before, "backups_removed");
        let took_over = x_lease["held"] == true && x_lease["epoch"].as_u64().unwrap_or(0) > epoch;
        eprintln!(
            "    {NAME}: after {:?}: A removed its backup: {removed}; {} took over: {took_over}; \
             A {a_lease}; {} {x_lease}; Y's write latency {}",
            cut.elapsed(),
            x.name,
            x.name,
            dist(lat)
        );
        anyhow::ensure!(
            removed || took_over,
            "neither a reconfiguration nor a takeover within the partition"
        );
        anyhow::ensure!(
            !(a_lease["held"] == true && x_lease["held"] == true),
            "two holders: A {a_lease}, {} {x_lease}",
            x.name
        );
        eprintln!("    {NAME}: healing");
        let _ = std::fs::remove_file(c_deny_path(root.path(), &a.name));
        let _ = std::fs::remove_file(c_deny_path(root.path(), &x.name));
        for c in &clients {
            all_visible(c, &names, Duration::from_secs(60))?;
        }
        let refs: Vec<&Client> = clients.iter().collect();
        ensure_no_conflicts(&refs)?;
        eventually("exactly one holder", Duration::from_secs(30), || {
            let holders = clients
                .iter()
                .filter(|c| lease_of(c).is_ok_and(|l| l["held"] == true))
                .count();
            anyhow::ensure!(holders == 1, "{holders} holders");
            Ok(())
        })?;
        for c in &clients {
            print_ack(NAME, &c.name, &ack_of(c)?);
        }
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}
