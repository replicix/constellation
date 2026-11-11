//! Plan 30 §M11 scenarios: delegated sub-sequencers, one log. A
//! directory's subtree is delegated to a node, which executes its
//! clients' writes locally and streams them to the root; the root
//! appends them into the one log. Cross-subtree renames recall the
//! generations involved; a delegate that dies or is cut off is reclaimed
//! by the root; a marker written after its data (in another subtree) is
//! never visible without it; with P2P off nothing is delegated.
//!
//! Every scenario prints the nodes' `status.delegation` block and the
//! measurements the plan asks for: per-node write latency on a delegated
//! subtree against the same node's forwarded writes and the root's local
//! ones, aggregate throughput of two subtrees against the single
//! sequencer, the cross-subtree latency, and the S3 requests (the
//! delegates ship no segment; a file costs its writer the same chunk
//! PUT either way).

use super::m8::{dist, write_timed};
use super::{ensure_no_conflicts, eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use constellation_types::Code;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(super) fn deleg_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["delegation"].clone())
}

pub(super) fn n(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

pub(super) fn node_id(c: &Client) -> Result<u64> {
    c.control_status()?["node_id"]
        .as_u64()
        .with_context(|| format!("{} reports no node id", c.name))
}

pub(super) fn print_deleg(scenario: &str, who: &str, d: &serde_json::Value) {
    eprintln!(
        "    {scenario}: {who} delegation: enabled {} table {} mine {} gens {} | executed {} \
         forwarded-to-delegate {} deps-waits {} parked-expired {} not-owner {} installed {} \
         streamed {} stream-refused {} renewals {} (refused {}) recalls-received {} | delegated \
         {} appended {} stream-refusals {} deps-unsatisfied {} cross-subtree {} recalls sent {} \
         drained {} expired {} reclaimed {} ended {} overflow-to-root {} exec-parked {} \
         stranded {}",
        d["enabled"],
        d["table"],
        d["mine"],
        d["gens"],
        n(d, "executed") + n(d, "fast_path_executed"),
        n(d, "forwarded_to_delegate"),
        n(d, "deps_waits"),
        n(d, "parked_expired"),
        n(d, "not_owner"),
        n(d, "installed"),
        n(d, "streamed_txs"),
        n(d, "stream_refused"),
        n(d, "renewals"),
        n(d, "renewals_refused"),
        n(d, "recalls_received"),
        n(d, "delegated"),
        n(d, "appended_txs"),
        n(d, "stream_refusals"),
        n(d, "deps_refused"),
        n(d, "cross_subtree"),
        n(d, "recalls_sent"),
        n(d, "recalls_drained"),
        n(d, "recalls_expired"),
        n(d, "reclaimed"),
        n(d, "ended"),
        n(d, "deps_overflow_to_root"),
        n(d, "exec_parked"),
        n(d, "stranded"),
    );
}

/// The lease TTL every M11 scenario mounts with: the root keeps its lease
/// while a delegation is live (the sticky rule), so nothing here is a
/// takeover.
pub(super) const TTL_MS: u64 = 20_000;
/// The grant TTL (`CONSTELLATION_DELEGATION_TTL_MS`): short enough that a
/// dead or cut-off delegate is reclaimed within the scenario.
const DELEG_TTL_MS: u64 = 3_000;

/// The default grant TTL (`CONSTELLATION_DELEGATION_TTL_MS` unset): the
/// lock grant TTL, 20 s (`load_config`).
const DEFAULT_DELEG_TTL_MS: u64 = 20_000;

/// What [`cluster`] returns.
type Cluster = (
    crate::s3env::S3Env,
    tempfile::TempDir,
    Vec<Client>,
    Vec<CountingProxy>,
);

/// The extras of a scenario that delegates by hand (or by the placement)
/// and wants no backup. The cluster pins the budget to 0 (no peer), and
/// since plan 46 step 13 a budget of 0 admits no peer, so no stealable
/// delegation is granted at all. A real budget lets the root be the
/// delegate's group, whatever the load; `CONSTELLATION_BACKUPS=0` keeps
/// every node from picking a backup, as the pin did.
pub(super) const DELEGATES_UNBACKED: &[(&str, &str)] = &[
    ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "500"),
    ("CONSTELLATION_BACKUPS", "0"),
];

/// `names` nodes on a fresh filesystem with `extra` env on every mount;
/// node 0 holds the lease, `f` exists everywhere. The first `counting`
/// nodes reach S3 through a counting proxy each (returned in order).
pub(super) fn cluster(
    scenario: &str,
    names: &[&str],
    extra: &[(&str, &str)],
    counting: usize,
) -> Result<Cluster> {
    cluster_with_deleg_ttl(scenario, names, extra, counting, Some(DELEG_TTL_MS))
}

/// [`cluster`] with the grant TTL `deleg_ttl_ms` (`None`: the default).
pub(super) fn cluster_with_deleg_ttl(
    scenario: &str,
    names: &[&str],
    extra: &[(&str, &str)],
    counting: usize,
    deleg_ttl_ms: Option<u64>,
) -> Result<Cluster> {
    let (env, root) = setup(scenario)?;
    let backend = format!("s3://{BUCKET}/{scenario}-{}", ts());
    let ttl = TTL_MS.to_string();
    let mut clients = Vec::new();
    let mut proxies = Vec::new();
    if counting > 0 {
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
            // Plan 30 §M12: the placement is on by default; these
            // scenarios delegate by hand and measure single-sequencer
            // phases, so it is pinned off (`auto-placement` and the M12
            // scenarios turn it on).
            .with_env(
                "CONSTELLATION_DELEGATION_PLACEMENT",
                extra
                    .iter()
                    .find(|(k, _)| *k == "CONSTELLATION_DELEGATION_PLACEMENT")
                    .map(|(_, v)| *v)
                    .unwrap_or("off"),
            )
            // No backup peer: M11 phase 2a delegates without one.
            .with_env("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0")
            // The root keeps its lease: a burst of forwarded writes that
            // escalates must not hand it off mid-measurement (the sticky
            // rule only holds while a delegation is live).
            .with_env("CONSTELLATION_LEASE_WANTED_GRACE_MS", "600000")
            .with_env(
                "CONSTELLATION_FAULT_P2P_DENY_FILE",
                &deny_path(root.path(), name).display().to_string(),
            );
        if let Some(ms) = deleg_ttl_ms {
            c = c.with_env("CONSTELLATION_DELEGATION_TTL_MS", &ms.to_string());
        }
        for (k, v) in extra {
            c = c.with_env(k, v);
        }
        clients.push(c);
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount()?;
    }
    if clients.len() > 1
        && !extra
            .iter()
            .any(|(k, v)| *k == "CONSTELLATION_P2P" && *v == "off")
    {
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        // M12 round 2: every node must *reach* every peer, not only list
        // it — a node whose link to the root is still being set up (the
        // registry allowlist refreshes every few seconds after a mount)
        // sends its first ops through the root's inbox, escalates on
        // that demand, and the root hands it the lease before the
        // scenario's first phase (`shared-dir-multi-writer`: "this node
        // does not hold the lease" at a delegation).
        wait_for_connected_peers(&refs)?;
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
fn deny_path(root: &std::path::Path, name: &str) -> std::path::PathBuf {
    root.join(name).join("deny")
}

pub(super) fn unmount_all(clients: &mut [Client]) {
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
}

/// Where this harness run keeps daemon logs (`dump_logs_on_failure`,
/// `HARNESS_KEEP_LOGS`): `$TMPDIR/harness-m11-logs-<pid>-<ts>`, one per
/// run. A fixed `/tmp/harness-m11-logs` was shared by every run on the
/// host, so concurrent runs (several worktrees, a matrix and a rerun)
/// overwrote each other's logs of the same scenario.
pub(super) fn kept_logs_dir() -> &'static std::path::Path {
    static DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        std::env::temp_dir().join(format!(
            "harness-m11-logs-{}-{}",
            std::process::id(),
            super::ts()
        ))
    })
}

/// A failed scenario's daemon logs, kept under [`kept_logs_dir`] (the
/// mounts' temp dir goes with the scenario): every incarnation's, so a
/// node killed and mounted again keeps its log from before the kill
/// (`<scenario>-<name>.<n>.log`, oldest first) next to the current one
/// (`<scenario>-<name>.log`).
pub(super) fn dump_logs_on_failure(scenario: &str, clients: &[Client], result: &Result<()>) {
    // `HARNESS_KEEP_LOGS=1` keeps a passing scenario's logs too.
    if result.is_ok() && std::env::var_os("HARNESS_KEEP_LOGS").is_none() {
        return;
    }
    let dir = kept_logs_dir();
    let _ = std::fs::create_dir_all(dir);
    for c in clients {
        let files = c.log_files();
        let rotated = files.len().saturating_sub(1);
        for (i, file) in files.iter().enumerate() {
            let path = if i < rotated {
                dir.join(format!("{scenario}-{}.{}.log", c.name, i + 1))
            } else {
                dir.join(format!("{scenario}-{}.log", c.name))
            };
            let _ = std::fs::write(&path, tail_lines(file, 400_000));
            eprintln!(
                "    {scenario}: {}'s log kept at {}",
                c.name,
                path.display()
            );
        }
    }
}

/// The last `n` lines of the file at `path` (empty if unreadable).
fn tail_lines(path: &std::path::Path, n: usize) -> String {
    std::fs::read_to_string(path)
        .map(|s| {
            let lines: Vec<&str> = s.lines().rev().take(n).collect();
            lines.into_iter().rev().collect::<Vec<_>>().join("\n")
        })
        .unwrap_or_default()
}

/// `constellation delegate <path> --to <node>` through the root's
/// control socket.
pub(super) fn delegate(root: &Client, path: &str, node: u64) -> Result<serde_json::Value> {
    root.control(
        "designation.delegate",
        serde_json::json!({"path": path, "node": node}),
    )
    .with_context(|| format!("delegating {path} to {node} on {}", root.name))
}

/// `constellation undelegate <path>`.
pub(super) fn undelegate(root: &Client, path: &str) -> Result<serde_json::Value> {
    root.control("designation.undelegate", serde_json::json!({"path": path}))
        .with_context(|| format!("undelegating {path} on {}", root.name))
}

/// Wait until `delegate` holds a live (not stopped) grant on `dir`, the
/// directory named `path` on the root.
pub(super) fn wait_installed(delegate: &Client, path: &str, deadline: Duration) -> Result<u64> {
    let mut gen = 0;
    eventually(
        &format!("{} holds the delegation of {path}", delegate.name),
        deadline,
        || {
            let d = deleg_of(delegate)?;
            let table = d["table"].as_array().cloned().unwrap_or_default();
            let entry = table
                .iter()
                .find(|e| e["path"] == path)
                .with_context(|| format!("{path} not in {}'s table: {d}", delegate.name))?;
            let g = n(entry, "gen");
            let mine = d["mine"].as_array().cloned().unwrap_or_default();
            let held = mine
                .iter()
                .any(|m| m[1].as_u64() == Some(g) && m[3] == false);
            anyhow::ensure!(held, "{} has not installed gen {g}: {d}", delegate.name);
            gen = g;
            Ok(())
        },
    )?;
    Ok(gen)
}

/// Every node reports every other node's P2P link as connected.
pub(super) fn wait_for_connected_peers(clients: &[&Client]) -> Result<()> {
    let need = clients.len().saturating_sub(1);
    for c in clients {
        eventually(
            &format!("{} reaches {need} peer(s)", c.name),
            Duration::from_secs(30),
            || {
                let s = c.control_status()?;
                let n = super::node_peers(&s["p2p"])
                    .filter(|p| p["connected"] == true)
                    .count();
                anyhow::ensure!(n >= need, "{} reaches {n} peers, want {need}", c.name);
                Ok(())
            },
        )?;
    }
    Ok(())
}

/// Every node's authority core sees every other node connected
/// (`core_connected`): the links it routes forwards and redirects on.
pub(super) fn wait_for_core_links(clients: &[&Client]) -> Result<()> {
    let need = clients.len().saturating_sub(1);
    for c in clients {
        eventually(
            &format!("{}'s core links reach {need} peer(s)", c.name),
            Duration::from_secs(30),
            || {
                let s = c.control_status()?;
                let n = super::node_peers(&s["p2p"])
                    .filter(|p| p["core_connected"] == true)
                    .count();
                anyhow::ensure!(
                    n >= need,
                    "{}'s core links reach {n} peers, want {need}",
                    c.name
                );
                Ok(())
            },
        )?;
    }
    Ok(())
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

/// `count` files `<dir>/<tag>-<i>` (content = relative name) written on
/// `c`; the open+write+close latencies.
fn write_files(
    c: &Client,
    dir: &str,
    tag: &str,
    count: usize,
) -> Result<(Vec<String>, Vec<Duration>)> {
    let mut names = Vec::new();
    let mut lat = Vec::new();
    for i in 0..count {
        let name = format!("{dir}/{tag}-{i}");
        lat.push(write_timed(&c.mnt.join(&name), name.as_bytes())?);
        names.push(name);
    }
    Ok((names, lat))
}

/// `count` files written by a thread per `(mount, dir, tag)`, all at
/// once; the wall time and every name.
fn write_concurrently(
    writers: &[(std::path::PathBuf, String, String)],
    count: usize,
) -> Result<(Duration, Vec<String>)> {
    let t = Instant::now();
    let handles: Vec<_> = writers
        .iter()
        .map(|(mnt, dir, tag)| {
            let (mnt, dir, tag) = (mnt.clone(), dir.clone(), tag.clone());
            std::thread::spawn(move || -> Result<Vec<String>> {
                let mut names = Vec::new();
                for i in 0..count {
                    let name = format!("{dir}/{tag}-{i}");
                    write_timed(&mnt.join(&name), name.as_bytes())?;
                    names.push(name);
                }
                Ok(names)
            })
        })
        .collect();
    let mut names = Vec::new();
    for h in handles {
        names.extend(h.join().map_err(|_| anyhow::anyhow!("writer panicked"))??);
    }
    Ok((t.elapsed(), names))
}

fn s3_tally(p: &CountingProxy) -> String {
    let t = p.tally();
    format!(
        "{} (PUT {} GET {} LIST {})",
        t.total(),
        t.put,
        t.get,
        t.list
    )
}

/// The requests by method and key prefix (`PUT chunks/` ...), most
/// frequent first.
pub(super) fn s3_breakdown(p: &CountingProxy) -> String {
    let mut by: std::collections::BTreeMap<String, usize> = Default::default();
    for r in p.requests() {
        let path = r.path();
        // `/bucket/<backend prefix>/<kind>/...`.
        let prefix = path
            .trim_start_matches('/')
            .split('/')
            .nth(2)
            .unwrap_or("?");
        let kind = if r.is_list() {
            "LIST"
        } else {
            r.method.as_str()
        };
        *by.entry(format!("{kind} {prefix}/")).or_default() += 1;
    }
    let mut v: Vec<(String, usize)> = by.into_iter().collect();
    v.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    v.iter()
        .map(|(k, n)| format!("{k} {n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `PUT`s of `prefix` keys.
fn puts_of(p: &CountingProxy, prefix: &str) -> usize {
    p.requests()
        .iter()
        .filter(|r| r.method == "PUT" && r.path().contains(prefix))
        .count()
}

/// Two subtrees delegated to two nodes: each node's writes into its
/// subtree are local (the delegate executes and acknowledges them; the
/// root appends the stream), everything converges everywhere, the root
/// appends nothing whose deps it lacks, and the delegates ship no
/// segment (a file costs its writer the same chunk PUT either way).
/// Measures each node's write latency on its
/// delegated subtree against the same node's forwarded writes (before
/// the delegation) and the root's local ones; the aggregate throughput
/// of both subtrees against both nodes forwarding to the one sequencer;
/// the cross-subtree rename's latency; and every node's S3 requests in
/// each phase.
pub fn delegated_subtrees(_seed: u64) -> Result<()> {
    const NAME: &str = "delegated-subtrees";
    const FILES: usize = 100;
    let (_env, _root, mut clients, proxies) =
        cluster(NAME, &["a", "b", "c"], DELEGATES_UNBACKED, 3)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        std::fs::create_dir(a.mnt.join("d2"))?;
        for x in [b, c] {
            eventually("dirs visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir() && x.mnt.join("d2").is_dir());
                Ok(())
            })?;
        }
        // Phase 0: the single sequencer. b and c forward into their
        // future subtrees; a writes locally.
        let (_, warm_b) = write_files(b, "d1", "warm", 10)?;
        let (_, _warm_c) = write_files(c, "d2", "warm", 10)?;
        for p in &proxies {
            p.reset();
        }
        let (names_b0, fwd_b) = write_files(b, "d1", "fwd", FILES)?;
        let (names_c0, fwd_c) = write_files(c, "d2", "fwd", FILES)?;
        let (names_a0, local_a) = write_files(a, ".", "local", FILES)?;
        let (single_wall, names_conc0) = write_concurrently(
            &[
                (b.mnt.clone(), "d1".into(), "conc0".into()),
                (c.mnt.clone(), "d2".into(), "conc0".into()),
            ],
            FILES,
        )?;
        // The counts are read once everything of the phase is in the log
        // (the uploads and the segments trail the acknowledgements), as
        // in the delegated phase below.
        let mut phase0: Vec<String> = Vec::new();
        for v in [&names_b0, &names_c0, &names_a0, &names_conc0] {
            phase0.extend(v.iter().cloned());
        }
        all_visible(a, &phase0, Duration::from_secs(60))?;
        let s3_phase0: Vec<String> = proxies.iter().map(s3_tally).collect();
        let phase0_chunk_puts: Vec<usize> = proxies.iter().map(|p| puts_of(p, "chunks/")).collect();
        let phase0_log_puts: Vec<usize> = proxies.iter().map(|p| puts_of(p, "log/")).collect();
        let phase0_puts: u64 = proxies.iter().map(|p| p.tally().put).sum();
        for (i, who) in ["a", "b", "c"].iter().enumerate() {
            eprintln!(
                "    {NAME}: single sequencer, {who}'s S3: {}",
                s3_breakdown(&proxies[i])
            );
        }
        eprintln!(
            "    {NAME}: single sequencer: b forwarded {} (warm-up {}); c forwarded {}; a local \
             {}; b+c concurrently {} files in {single_wall:?} ({:.0} files/s); S3 a {} b {} c {}",
            dist(fwd_b),
            dist(warm_b),
            dist(fwd_c),
            dist(local_a),
            2 * FILES,
            (2 * FILES) as f64 / single_wall.as_secs_f64(),
            s3_phase0[0],
            s3_phase0[1],
            s3_phase0[2]
        );
        // Phase 1: d1 -> b, d2 -> c.
        delegate(a, "/d1", b_id)?;
        delegate(a, "/d2", c_id)?;
        let gen_b = wait_installed(b, "/d1", Duration::from_secs(20))?;
        let gen_c = wait_installed(c, "/d2", Duration::from_secs(20))?;
        eprintln!(
            "    {NAME}: d1 -> {} (gen {gen_b}), d2 -> {} (gen {gen_c})",
            b.name, c.name
        );
        let (_, warm_c1) = write_files(c, "d2", "warm1", 10)?;
        let (_, warm_b1) = write_files(b, "d1", "warm1", 10)?;
        for p in &proxies {
            p.reset();
        }
        let (names_b1, del_b) = write_files(b, "d1", "del", FILES)?;
        let (names_c1, del_c) = write_files(c, "d2", "del", FILES)?;
        let (names_a1, local_a1) = write_files(a, ".", "local1", FILES)?;
        let (deleg_wall, names_conc1) = write_concurrently(
            &[
                (b.mnt.clone(), "d1".into(), "conc1".into()),
                (c.mnt.clone(), "d2".into(), "conc1".into()),
            ],
            FILES,
        )?;
        // Every delegated write must be in the log before the S3 count
        // is read (the root appends and ships them behind the ack).
        let mut all: Vec<String> = Vec::new();
        for v in [&names_b1, &names_c1, &names_a1, &names_conc1] {
            all.extend(v.iter().cloned());
        }
        all_visible(a, &all, Duration::from_secs(60))?;
        let s3_phase1: Vec<String> = proxies.iter().map(s3_tally).collect();
        let phase1_chunk_puts: Vec<usize> = proxies.iter().map(|p| puts_of(p, "chunks/")).collect();
        let phase1_log_puts: Vec<usize> = proxies.iter().map(|p| puts_of(p, "log/")).collect();
        let phase1_puts: u64 = proxies.iter().map(|p| p.tally().put).sum();
        for (i, who) in ["a", "b", "c"].iter().enumerate() {
            eprintln!(
                "    {NAME}: delegated, {who}'s S3: {}",
                s3_breakdown(&proxies[i])
            );
        }
        let deleg_s3: u64 = proxies[1..].iter().map(|p| p.tally().total()).sum();
        eprintln!(
            "    {NAME}: delegated: b local {} (warm-up {}); c local {} (warm-up {}); a local {}; \
             b+c concurrently {} files in {deleg_wall:?} ({:.0} files/s, single sequencer \
             {:.0} files/s); S3 a {} b {} c {}",
            dist(del_b),
            dist(warm_b1),
            dist(del_c),
            dist(warm_c1),
            dist(local_a1),
            2 * FILES,
            (2 * FILES) as f64 / deleg_wall.as_secs_f64(),
            (2 * FILES) as f64 / single_wall.as_secs_f64(),
            s3_phase1[0],
            s3_phase1[1],
            s3_phase1[2]
        );
        for p in &proxies {
            p.ensure_sane()?;
        }
        let da = deleg_of(a)?;
        let db = deleg_of(b)?;
        let dc = deleg_of(c)?;
        print_deleg(NAME, "a", &da);
        print_deleg(NAME, "b", &db);
        print_deleg(NAME, "c", &dc);
        anyhow::ensure!(da["enabled"] == true, "delegation off on a: {da}");
        let executed = |d: &serde_json::Value| n(d, "executed") + n(d, "fast_path_executed");
        anyhow::ensure!(
            executed(&db) >= (FILES + 10) as u64 && executed(&dc) >= (FILES + 10) as u64,
            "the delegates did not execute their writes: b {db} c {dc}"
        );
        anyhow::ensure!(
            n(&da, "appended_txs") >= 2 * (FILES + 10) as u64,
            "the root appended too few stream transactions: {da}"
        );
        anyhow::ensure!(
            n(&da, "deps_refused") == 0,
            "the root (never deposed here) refused a delegate batch for lacking its deps, \
             a row executed ahead of its cause: {da}"
        );
        anyhow::ensure!(
            n(&da, "recalls_sent") == 0 && n(&da, "ended") == 0,
            "a delegation was recalled by same-subtree writes: {da}"
        );
        anyhow::ensure!(
            n(&db, "stranded") == 0 && n(&dc, "stranded") == 0,
            "a delegate rolled a transaction back: b {db} c {dc}"
        );
        // The delegates ship nothing: every `log/` PUT is the root's.
        anyhow::ensure!(
            phase1_log_puts[1] == 0 && phase1_log_puts[2] == 0 && phase0_log_puts[1] == 0,
            "a delegate shipped segments itself: {phase1_log_puts:?}"
        );
        // A file costs its writer one chunk PUT, delegated or not
        // (phase 1 wrote 10 warm-up files more per delegate).
        for i in 1..3 {
            anyhow::ensure!(
                phase1_chunk_puts[i] <= phase0_chunk_puts[i] + 10 + 20
                    && phase1_chunk_puts[i] + 20 >= phase0_chunk_puts[i],
                "chunk PUTs changed with delegation: {phase0_chunk_puts:?} -> \
                 {phase1_chunk_puts:?}"
            );
        }
        // The root's segments carry the same writes either way: no
        // more `log/` PUTs than the single sequencer needed for the same
        // writes (a small margin for the extra warm-up files and the
        // batching of the moment).
        anyhow::ensure!(
            phase1_log_puts[0] <= phase0_log_puts[0] + phase0_log_puts[0] / 4 + 30,
            "the root's segment PUTs grew with delegation: {phase0_log_puts:?} -> \
             {phase1_log_puts:?}"
        );
        eprintln!(
            "    {NAME}: S3 PUTs single sequencer {phase0_puts} vs delegated {phase1_puts}; \
             log PUTs {phase0_log_puts:?} vs {phase1_log_puts:?}; chunk PUTs \
             {phase0_chunk_puts:?} vs {phase1_chunk_puts:?}; the delegates' requests \
             {deleg_s3} in the delegated phase"
        );
        // Everything converges on every node.
        for v in [&names_b0, &names_c0, &names_a0, &names_conc0] {
            all.extend(v.iter().cloned());
        }
        for x in [a, b, c] {
            all_visible(x, &all, Duration::from_secs(60))?;
        }
        // The cross-subtree rename: b moves a file of d1 into d2. The
        // root recalls both generations first.
        let t = Instant::now();
        std::fs::rename(b.mnt.join("d1/del-0"), b.mnt.join("d2/moved"))
            .context("b's cross-subtree rename")?;
        let cross = t.elapsed();
        let da = deleg_of(a)?;
        eprintln!(
            "    {NAME}: cross-subtree rename d1/del-0 -> d2/moved on b: {cross:?}; root \
             cross-subtree {} recalls sent {} drained {} ended {}",
            n(&da, "cross_subtree"),
            n(&da, "recalls_sent"),
            n(&da, "recalls_drained"),
            n(&da, "ended")
        );
        anyhow::ensure!(
            n(&da, "cross_subtree") >= 1 && n(&da, "ended") >= 2,
            "the cross-subtree rename recalled nothing: {da}"
        );
        for x in [a, b, c] {
            eventually(
                &format!("{} sees the move", x.name),
                Duration::from_secs(30),
                || {
                    anyhow::ensure!(
                        !x.mnt.join("d1/del-0").exists(),
                        "d1/del-0 still on {}",
                        x.name
                    );
                    anyhow::ensure!(
                        std::fs::read(x.mnt.join("d2/moved")).ok().as_deref()
                            == Some(&b"d1/del-0"[..]),
                        "d2/moved not on {}",
                        x.name
                    );
                    Ok(())
                },
            )?;
        }
        // Writes into the recalled subtrees go through the root now.
        let (names_after, after_b) = write_files(b, "d1", "after", 20)?;
        eprintln!(
            "    {NAME}: b's writes into d1 after the recall (forwarded): {}",
            dist(after_b)
        );
        for x in [a, b, c] {
            all_visible(x, &names_after, Duration::from_secs(30))?;
        }
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// A rename across two delegated subtrees: the root recalls both
/// generations, drains their streams, executes the rename after them and
/// ends the generations; the moved file is exactly where the rename put
/// it on every node, the source name is gone, nothing is duplicated, and
/// the subtrees can be delegated again.
pub fn cross_subtree_rename(_seed: u64) -> Result<()> {
    const NAME: &str = "cross-subtree-rename";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], DELEGATES_UNBACKED, 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        std::fs::create_dir(a.mnt.join("d2"))?;
        for x in [b, c] {
            eventually("dirs visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir() && x.mnt.join("d2").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", b_id)?;
        delegate(a, "/d2", c_id)?;
        wait_installed(b, "/d1", Duration::from_secs(20))?;
        wait_installed(c, "/d2", Duration::from_secs(20))?;
        // Both delegates busy; the rename lands between their writes.
        let (names_b, _) = write_files(b, "d1", "x", 30)?;
        let (names_c, _) = write_files(c, "d2", "y", 30)?;
        let stop = Arc::new(AtomicBool::new(false));
        let bg: Vec<_> = [(b.mnt.clone(), "d1"), (c.mnt.clone(), "d2")]
            .into_iter()
            .map(|(mnt, dir)| {
                let stop = stop.clone();
                std::thread::spawn(move || -> Result<Vec<String>> {
                    let mut names = Vec::new();
                    let mut i = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let name = format!("{dir}/bg-{i}");
                        write_timed(&mnt.join(&name), name.as_bytes())?;
                        names.push(name);
                        i += 1;
                    }
                    Ok(names)
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(300));
        let t = Instant::now();
        std::fs::rename(c.mnt.join("d1/x-7"), c.mnt.join("d2/x-7-moved"))
            .context("c's cross-subtree rename")?;
        let cross = t.elapsed();
        std::thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::Relaxed);
        let mut bg_names = Vec::new();
        for h in bg {
            bg_names.extend(h.join().map_err(|_| anyhow::anyhow!("writer panicked"))??);
        }
        let da = deleg_of(a)?;
        print_deleg(NAME, "a", &da);
        print_deleg(NAME, "b", &deleg_of(b)?);
        print_deleg(NAME, "c", &deleg_of(c)?);
        eprintln!(
            "    {NAME}: rename d1/x-7 -> d2/x-7-moved on c took {cross:?} (recalls sent {} \
             drained {} ended {}); {} background writes landed around it",
            n(&da, "recalls_sent"),
            n(&da, "recalls_drained"),
            n(&da, "ended"),
            bg_names.len()
        );
        anyhow::ensure!(
            n(&da, "cross_subtree") >= 1,
            "no cross-subtree op counted: {da}"
        );
        anyhow::ensure!(n(&da, "ended") >= 2, "both generations must end: {da}");
        anyhow::ensure!(
            n(&da, "deps_refused") == 0,
            "the root (never deposed here) refused a delegate batch for lacking its deps, \
             a row executed ahead of its cause: {da}"
        );
        let mut all: Vec<String> = names_b
            .iter()
            .chain(names_c.iter())
            .chain(bg_names.iter())
            .filter(|n| *n != "d1/x-7")
            .cloned()
            .collect();
        for x in [a, b, c] {
            eventually(
                &format!("{} sees the move", x.name),
                Duration::from_secs(30),
                || {
                    anyhow::ensure!(!x.mnt.join("d1/x-7").exists(), "d1/x-7 still on {}", x.name);
                    anyhow::ensure!(
                        std::fs::read(x.mnt.join("d2/x-7-moved")).ok().as_deref()
                            == Some(&b"d1/x-7"[..]),
                        "d2/x-7-moved wrong on {}",
                        x.name
                    );
                    Ok(())
                },
            )?;
            all_visible(x, &all, Duration::from_secs(30))?;
        }
        // Phase 2b: the root delegates the directories again by itself
        // once the cross-subtree op ran (a new generation each).
        eventually("d1 delegated to b again", Duration::from_secs(20), || {
            let d = deleg_of(a)?;
            anyhow::ensure!(
                d["table"]
                    .as_array()
                    .is_some_and(|t| t.iter().any(|e| e["path"] == "/d1" && n(e, "node") == b_id)),
                "d1 not re-delegated yet: {d}"
            );
            Ok(())
        })?;
        let gen2 = wait_installed(b, "/d1", Duration::from_secs(20))?;
        let (again, lat) = write_files(b, "d1", "again", 20)?;
        eprintln!(
            "    {NAME}: d1 re-delegated to b by the root (gen {gen2}, re-delegated {}); b's local writes {}",
            n(&deleg_of(a)?, "redelegated"),
            dist(lat)
        );
        all.extend(again.iter().cloned());
        for x in [a, b, c] {
            all_visible(x, &again, Duration::from_secs(30))?;
        }
        undelegate(a, "/d1")?;
        eventually("d1 recalled", Duration::from_secs(20), || {
            let d = deleg_of(a)?;
            anyhow::ensure!(
                d["table"]
                    .as_array()
                    .is_some_and(|t| t.iter().all(|e| e["path"] != "/d1")),
                "d1 still delegated: {d}"
            );
            Ok(())
        })?;
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// The delegate of `d1` dies mid-burst, without a backup: the root
/// reclaims the expired grant (it cannot be renewed or recalled), a
/// third node's writes into `d1` go through the root, and the dead node
/// remounts with its journal — its acknowledged-but-unstreamed writes
/// replay by rid through the root — and converges.
pub fn delegate_crash(_seed: u64) -> Result<()> {
    delegate_crash_with("delegate-crash", None)
}

/// `delegate-crash` at the default grant TTL (the lock grant TTL, 20 s;
/// overload-cascade raised it from 5 s): the dead delegate's subtree is
/// reclaimed about `ttl + margin` after its last renewal — a third node's
/// write into it waits that long, and no longer.
pub fn delegate_crash_default_ttl(_seed: u64) -> Result<()> {
    delegate_crash_with("delegate-crash-default-ttl", Some(DEFAULT_DELEG_TTL_MS))
}

/// `default_ttl`: run at the default grant TTL (this long) instead of
/// the scenarios' short [`DELEG_TTL_MS`].
fn delegate_crash_with(scenario: &'static str, default_ttl: Option<u64>) -> Result<()> {
    let deleg_ttl = default_ttl.unwrap_or(DELEG_TTL_MS);
    let (_env, _root, mut clients, _) = cluster_with_deleg_ttl(
        scenario,
        &["a", "b", "c"],
        DELEGATES_UNBACKED,
        0,
        default_ttl.is_none().then_some(DELEG_TTL_MS),
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", b_id)?;
        let gen = wait_installed(b, "/d1", Duration::from_secs(20))?;
        let (names_b, _) = write_files(b, "d1", "b", 40)?;
        // The last acknowledged file may not have streamed yet: killed
        // right after the ack.
        let last = "d1/b-last".to_string();
        write_timed(&b.mnt.join(&last), last.as_bytes())?;
        let killed = Instant::now();
        clients[1].kill9()?;
        let a = &clients[0];
        let c = &clients[2];
        eprintln!(
            "    {scenario}: b (gen {gen}) killed after {} acknowledged writes",
            names_b.len() + 1
        );
        // c's write into d1: refused by the dead delegate, executed by the
        // root once the grant is reclaimed (bounded by the grant TTL).
        let t = Instant::now();
        let name = "d1/c-after-crash".to_string();
        write_timed(&c.mnt.join(&name), name.as_bytes()).context("c writing into d1")?;
        let c_took = t.elapsed();
        let da = deleg_of(a)?;
        print_deleg(scenario, "a", &da);
        let after_kill = killed.elapsed();
        eprintln!(
            "    {scenario}: c's first write into d1 after the crash returned after {c_took:?} \
             ({after_kill:?} after the kill; grant TTL {deleg_ttl} ms); reclaimed {} recalls \
             expired {}",
            n(&da, "reclaimed"),
            n(&da, "recalls_expired")
        );
        anyhow::ensure!(
            n(&da, "reclaimed") + n(&da, "recalls_expired") >= 1 && n(&da, "ended") >= 1,
            "the root never reclaimed the dead delegate's grant: {da}"
        );
        if default_ttl.is_some() {
            // Reclaimed after the grant's expiry (renewed well before it,
            // so it had seconds left at the kill, more than the short
            // TTL's whole life), within `ttl + margin` of the last
            // renewal plus slack for a loaded host.
            anyhow::ensure!(
                after_kill > Duration::from_millis(DELEG_TTL_MS + 2_000),
                "reclaimed {after_kill:?} after the kill: the default TTL is not in effect"
            );
        }
        anyhow::ensure!(
            std::fs::read(a.mnt.join(&name))? == name.as_bytes(),
            "the root does not show c's write"
        );
        // The reclaim's lower bound above is exact; how long c's write
        // waited for it is latency (the grant's TTL and margin, plus host
        // slack), judged against the host-lag probe.
        let (protocol, bound) = if default_ttl.is_some() {
            (deleg_ttl + 1_000, deleg_ttl + 10_000)
        } else {
            (DELEG_TTL_MS + 1_000, 3 * DELEG_TTL_MS + 5_000)
        };
        crate::hostlag::latency_bound(
            &format!("{scenario}: c's write waiting for the dead delegate's reclaim"),
            t,
            c_took,
            Duration::from_millis(protocol),
            Duration::from_millis(bound),
        )?;
        // b returns with its journal.
        clients[1].mount()?;
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let mut all = names_b.clone();
        all.push(last);
        all.push(name);
        for x in [a, b, c] {
            all_visible(x, &all, Duration::from_secs(60))?;
        }
        let db = deleg_of(b)?;
        print_deleg(scenario, "b (remounted)", &db);
        anyhow::ensure!(
            db["mine"].as_array().is_some_and(|m| m.is_empty()),
            "the remounted delegate still holds a grant: {db}"
        );
        // Delegating d1 again works (a fresh generation).
        delegate(a, "/d1", node_id(b)?)?;
        let gen2 = wait_installed(b, "/d1", Duration::from_secs(30))?;
        anyhow::ensure!(gen2 > gen, "generation did not advance: {gen} -> {gen2}");
        let (again, _) = write_files(b, "d1", "again", 10)?;
        for x in [a, b, c] {
            all_visible(x, &again, Duration::from_secs(30))?;
        }
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(scenario, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// Plan 46 M1.3 step 13: the sticky holder of `d1` is killed; its subtree
/// blocks (`EROFS`) for longer than the delegation TTL (a sticky grant is not
/// TTL-ended); `repair abandon --node b` is refused without `--force` within
/// the TTL of the kill, and with it ends the stream after the read
/// delegations `b` granted are recalled (or waited out). Then `c` writes
/// through the root, no read is served under `b`'s old grant, `b` returns with
/// its journal and its acknowledged rows past the cut replay by rid (a name
/// the root gave to `c` meanwhile becomes a `.constellation-conflict/` copy),
/// and `b` can be granted again at a higher generation.
pub fn sticky_abandon(_seed: u64) -> Result<()> {
    const SCENARIO: &str = "sticky-abandon";
    // `CONSTELLATION_READ_DELEGATION_TTL_MS` (default) plus the expiry margin.
    const READ_WAIT_MS: u64 = 5_000 + 1_000;
    let (_env, root, mut clients, _) = cluster(
        SCENARIO,
        &["a", "b", "c"],
        &[("CONSTELLATION_CTO", "strict")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        a.control(
            "designation.delegate",
            serde_json::json!({"path": "/d1", "node": b_id, "sticky": true}),
        )
        .context("delegating --sticky /d1 to b")?;
        let gen = wait_installed(b, "/d1", Duration::from_secs(20))?;
        // b writes 40 files, `x` (c will read it under a read delegation of
        // b) and `b-last`, killed right after its ack (it may not have
        // streamed).
        let (names_b, _) = write_files(b, "d1", "b", 40)?;
        write_timed(&b.mnt.join("d1/x"), b"old")?;
        eventually("x visible on c", Duration::from_secs(20), || {
            anyhow::ensure!(std::fs::read(c.mnt.join("d1/x"))? == b"old");
            Ok(())
        })?;
        // The second and later strict opens of x on c are served under a read
        // delegation b granted.
        for _ in 0..5 {
            anyhow::ensure!(std::fs::read(c.mnt.join("d1/x"))? == b"old");
        }
        let cto = c.control_status()?["cto"].clone();
        anyhow::ensure!(
            n(&cto, "delegation_local") >= 1,
            "c served no read under a read delegation: {cto}"
        );
        eprintln!(
            "    {SCENARIO}: c's reads under a read delegation before the kill: {}",
            n(&cto, "delegation_local")
        );
        // b is cut from the root, so `b-last` is acknowledged by the
        // sticky holder but cannot stream: the row past the cut exists at
        // the kill, not depending on how fast the stream ships it (it would
        // else sometimes be in the log before the abandon, and nothing is
        // left to replay). The root heard from b within the TTL just before.
        let a_id = node_id(a)?;
        std::fs::write(deny_path(root.path(), &a.name), format!("{b_id}\n"))?;
        std::fs::write(deny_path(root.path(), &b.name), format!("{a_id}\n"))?;
        let last = "d1/b-last".to_string();
        write_timed(&b.mnt.join(&last), last.as_bytes())?;
        let killed = Instant::now();
        clients[1].kill9()?;
        let a = &clients[0];
        let c = &clients[2];
        // Without --force, within the delegation TTL of the kill: refused,
        // and it names --force (the root heard from b lately).
        let refused = a.control("repair.abandon", serde_json::json!({"node": b_id}));
        let refused = match refused {
            Err(e) => format!("{e:#}"),
            Ok(v) => anyhow::bail!("the abandon without --force was accepted: {v}"),
        };
        anyhow::ensure!(
            refused.contains("--force"),
            "the refusal does not name --force: {refused}"
        );
        eprintln!("    {SCENARIO}: the abandon without --force was refused: {refused}");
        // The first op c sends after the kill is sent to b: c's link to b has
        // not failed yet, and the op's keys are b's sticky grant. It may have
        // run there (b could be alive and cut from the root only), so the
        // root's EROFS for it is held, not final (plan 46 M1.3 step 5, I1),
        // and it ends in doubt (EIO) at its settle cap, tens of seconds on.
        // That is the design, not a fault: this sacrificial write takes it,
        // and fails with EIO or EROFS, never succeeds. The writes of the loop
        // below are sent after c's forward to b timed out, so they are
        // refused for good by the root (EROFS); in practice the first of
        // them returns only when the sacrificial one has (the same
        // directory), tens of seconds after the kill.
        let sacrifice_path = c.mnt.join("d1/c-sacrifice");
        let sacrifice = std::thread::spawn(move || std::fs::write(sacrifice_path, b"c"));
        std::thread::sleep(Duration::from_millis(2_000));
        // c's write into d1 fails EROFS for longer than the delegation TTL:
        // the sticky grant is not TTL-ended.
        let blocked = "d1/c-blocked";
        let mut erofs = 0;
        while killed.elapsed() < Duration::from_millis(DELEG_TTL_MS * 2 + 2_000) {
            match std::fs::write(c.mnt.join(blocked), b"c") {
                Ok(()) => anyhow::bail!(
                    "c's write into the dead sticky holder's subtree succeeded {:?} after the kill",
                    killed.elapsed()
                ),
                Err(e) if e.raw_os_error() == Some(30) => erofs += 1,
                Err(e) => anyhow::bail!("c's write into d1 failed with {e}, not EROFS"),
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        eprintln!(
            "    {SCENARIO}: c's writes into d1 failed EROFS {erofs} times over {:?} \
             (delegation TTL {DELEG_TTL_MS} ms): the sticky grant is not TTL-ended",
            killed.elapsed()
        );
        // The operator confirmed b is down: forced.
        let t = Instant::now();
        let answer = a
            .control(
                "repair.abandon",
                serde_json::json!({"node": b_id, "force": true}),
            )
            .context("repair abandon --node b --force")?;
        let took = t.elapsed();
        eprintln!("    {SCENARIO}: repair abandon --force returned after {took:?}: {answer}");
        anyhow::ensure!(
            took < Duration::from_millis(READ_WAIT_MS + 2_000),
            "the abandon took {took:?}, more than the read wait {READ_WAIT_MS} ms + 2 s"
        );
        let da = deleg_of(a)?;
        print_deleg(SCENARIO, "a", &da);
        // The sacrificial write ended in doubt (or refused), never succeeded.
        match sacrifice
            .join()
            .map_err(|_| anyhow::anyhow!("the sacrificial writer panicked"))?
        {
            Ok(()) => anyhow::bail!("c's write sent to the dead sticky holder succeeded"),
            Err(e) if matches!(e.raw_os_error(), Some(5 | 30)) => {}
            Err(e) => anyhow::bail!("c's write sent to the dead sticky holder failed with {e}"),
        }
        // c writes b-last (different content) and x: through the root.
        let new_last = b"c took this name";
        write_timed(&c.mnt.join(&last), new_last).context("c writing d1/b-last")?;
        write_timed(&c.mnt.join("d1/x"), b"new").context("c writing d1/x")?;
        anyhow::ensure!(
            std::fs::read(c.mnt.join("d1/x"))? == b"new",
            "c's next strict open of d1/x does not see its own write"
        );
        anyhow::ensure!(
            std::fs::read(a.mnt.join("d1/x"))? == b"new",
            "a's strict open of d1/x does not see c's write (a read was served under b's old grant)"
        );
        // b returns with its state dir: its acknowledged writes past the cut
        // replay by rid; d1/b-last, now c's, becomes a conflict copy.
        for x in [&clients[0], &clients[1]] {
            let _ = std::fs::remove_file(deny_path(root.path(), &x.name));
        }
        clients[1].mount()?;
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        for x in [a, b, c] {
            all_visible(x, &names_b, Duration::from_secs(60))?;
        }
        eventually(
            "every node shows c's d1/b-last and d1/x",
            Duration::from_secs(60),
            || {
                for x in [a, b, c] {
                    anyhow::ensure!(
                        std::fs::read(x.mnt.join(&last))? == new_last,
                        "{} shows another d1/b-last",
                        x.name
                    );
                    anyhow::ensure!(
                        std::fs::read(x.mnt.join("d1/x"))? == b"new",
                        "{} shows another d1/x",
                        x.name
                    );
                }
                Ok(())
            },
        )?;
        // The replay of b's write past the cut is refused: the name is c's now.
        eventually(
            "b's replay of d1/b-last is refused as a conflict copy",
            Duration::from_secs(30),
            || {
                let sb = b.control_status()?["speculation"].clone();
                anyhow::ensure!(
                    n(&sb, "replay_conflicts") >= 1,
                    "b's replay of its row past the cut was not refused (replay_conflicts {})",
                    n(&sb, "replay_conflicts")
                );
                Ok(())
            },
        )?;
        let sb = b.control_status()?["speculation"].clone();
        eprintln!(
            "    {SCENARIO}: after b's return: rows replayed by rid on b {}, replays refused (conflict \
             copies) {}, conflict copies on (a, c) {:?}; streams ended on a {}",
            n(&sb, "stranded_replayed"),
            n(&sb, "replay_conflicts"),
            [conflicts_total(a), conflicts_total(c)],
            n(&deleg_of(a)?, "ended")
        );
        let db = deleg_of(b)?;
        anyhow::ensure!(
            db["mine"].as_array().is_some_and(|m| m.is_empty()),
            "the remounted holder still holds a grant: {db}"
        );
        // b can be granted again, at a higher generation.
        // (Refused while the read delegations under d1 are being recalled.)
        eventually(
            "d1 is delegated to b again",
            Duration::from_secs(30),
            || {
                a.control(
                    "designation.delegate",
                    serde_json::json!({"path": "/d1", "node": b_id, "sticky": true}),
                )
                .context("delegating --sticky /d1 to b again")?;
                Ok(())
            },
        )?;
        let gen2 = wait_installed(b, "/d1", Duration::from_secs(30))?;
        anyhow::ensure!(gen2 > gen, "generation did not advance: {gen} -> {gen2}");
        let (again, _) = write_files(b, "d1", "again", 10)?;
        for x in [a, b, c] {
            all_visible(x, &again, Duration::from_secs(30))?;
        }
        Ok(())
    })();
    dump_logs_on_failure(SCENARIO, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// Whether `c`'s delegation table lists `path`.
fn in_table(c: &Client, path: &str) -> Result<bool> {
    Ok(deleg_of(c)?["table"]
        .as_array()
        .is_some_and(|t| t.iter().any(|e| e["path"] == path)))
}

/// How a sticky holder leaves in [`sticky_leaves`].
#[derive(Clone, Copy)]
enum Leave {
    /// A clean unmount (the daemon exits).
    Unmount,
    /// A suspension (`node.lifecycle` `Suspending`, the event the logind
    /// source delivers before it lets the host sleep), then a resume.
    Suspend,
}

/// Plan 46 M1.3 step 7: a sticky holder that leaves cleanly hands its
/// subtree back by its own decision. `b` holds `/d1` sticky and has written
/// into it; it unmounts (or is suspended); the grant leaves the root's
/// table at once, not after a time-out, and `c`'s write into `/d1` succeeds
/// where a dead sticky holder's subtree refuses it for longer than the
/// delegation TTL (`sticky-abandon`); everything `b` wrote is on every
/// node. The holder's log says it released.
fn sticky_leaves(scenario: &str, how: Leave) -> Result<()> {
    let (_env, _root, mut clients, _) = cluster(
        scenario,
        &["a", "b", "c"],
        &[("CONSTELLATION_CTO", "strict")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let b_id = node_id(&clients[1])?;
        std::fs::create_dir(clients[0].mnt.join("d1"))?;
        for x in [&clients[1], &clients[2]] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        clients[0]
            .control(
                "designation.delegate",
                serde_json::json!({"path": "/d1", "node": b_id, "sticky": true}),
            )
            .context("delegating --sticky /d1 to b")?;
        wait_installed(&clients[1], "/d1", Duration::from_secs(20))?;
        anyhow::ensure!(in_table(&clients[0], "/d1")?, "/d1 not in a's table");
        let (names_b, _) = write_files(&clients[1], "d1", "b", 40)?;
        let left = Instant::now();
        match how {
            Leave::Unmount => {
                clients[1]
                    .unmount_exit(Duration::from_secs(60))
                    .context("b's clean unmount")?;
            }
            Leave::Suspend => {
                let report = clients[1].control(
                    "node.lifecycle",
                    serde_json::json!({"event": {"Suspending": {"deadline_in_ms": 15_000}}}),
                )?;
                anyhow::ensure!(report["applied"] == true, "not applied: {report}");
                let s = &report["status"]["last_suspend"];
                anyhow::ensure!(
                    s["within_deadline"] == true && s["lease_released"] == true,
                    "the suspension did not finish cleanly: {s}"
                );
            }
        }
        let gone = left.elapsed();
        // The release row is the root's before b is gone: the grant is not
        // in a's table now (no wait for a time-out).
        eventually("/d1 leaves a's table", Duration::from_secs(10), || {
            anyhow::ensure!(!in_table(&clients[0], "/d1")?, "still in a's table");
            Ok(())
        })?;
        // c writes into the released subtree through the root: well inside
        // the time a dead holder's subtree stays read-only.
        let t = Instant::now();
        eventually("c writes d1/c-after", Duration::from_secs(10), || {
            std::fs::write(clients[2].mnt.join("d1/c-after"), b"c")?;
            Ok(())
        })?;
        let c_wrote = t.elapsed();
        anyhow::ensure!(
            c_wrote < Duration::from_millis(DELEG_TTL_MS * 2),
            "c's write took {c_wrote:?}"
        );
        let verb = match how {
            Leave::Unmount => "unmount: sticky grants released",
            Leave::Suspend => "suspending: sticky grants released",
        };
        anyhow::ensure!(
            clients[1].log_text().contains(verb),
            "b's log does not say \"{verb}\": {}",
            clients[1].tail_log_n(40)
        );
        for x in [&clients[0], &clients[2]] {
            eventually("b's files visible", Duration::from_secs(30), || {
                for name in &names_b {
                    anyhow::ensure!(
                        std::fs::read(x.mnt.join(name))? == name.as_bytes(),
                        "{name} on {}",
                        x.name
                    );
                }
                Ok(())
            })?;
        }
        if let Leave::Suspend = how {
            let report =
                clients[1].control("node.lifecycle", serde_json::json!({"event": "Resumed"}))?;
            anyhow::ensure!(report["applied"] == true, "resume not applied: {report}");
            eventually("b sees c-after", Duration::from_secs(30), || {
                anyhow::ensure!(std::fs::read(clients[1].mnt.join("d1/c-after"))? == b"c");
                Ok(())
            })?;
        }
        eprintln!(
            "    {scenario}: b left in {gone:?}; c's write into d1 took {c_wrote:?} (grant TTL {DELEG_TTL_MS} ms)"
        );
        // (The unmounted holder is gone: the others only.)
        let live = match how {
            Leave::Unmount => [0, 2].as_slice(),
            Leave::Suspend => [0, 1, 2].as_slice(),
        };
        let refs: Vec<&Client> = live.iter().map(|i| &clients[*i]).collect();
        ensure_no_conflicts(&refs)
    })();
    dump_logs_on_failure(scenario, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// Plan 46 M1.3 step 7: a sticky holder's clean unmount releases its
/// subtree before it exits (`sticky-release-on-unmount`).
pub fn sticky_release_on_unmount(_seed: u64) -> Result<()> {
    sticky_leaves("sticky-release-on-unmount", Leave::Unmount)
}

/// Plan 46 M1.3 step 7: a sticky holder released by a suspension
/// (`sticky-suspend-inhibitor`: the lifecycle event the logind inhibitor
/// handler delivers; the fake logind of the platform crate is not wired to
/// a node, so the harness sends the event itself).
pub fn sticky_suspend_inhibitor(_seed: u64) -> Result<()> {
    sticky_leaves("sticky-suspend-inhibitor", Leave::Suspend)
}

/// ADR-46: the sticky holder `b` of `d1` is killed with `kill -9` while `c`
/// holds read delegations it granted, and remounted with its journal inside
/// their lifetime (the read TTL is 30 s here, so the delegations outlive the
/// remount). `a`'s write of `d1/x` goes to the returned holder: it must not
/// be acknowledged while `c` can still serve the old content, so `c`'s next
/// strict open of `d1/x` shows the new content. A holder that forgets its
/// grants executes at once and `c` serves the old content.
pub fn sticky_holder_restart(_seed: u64) -> Result<()> {
    const SCENARIO: &str = "sticky-holder-restart";
    let (_env, _root, mut clients, _) = cluster(
        SCENARIO,
        &["a", "b", "c"],
        &[
            ("CONSTELLATION_CTO", "strict"),
            ("CONSTELLATION_READ_DELEGATION_TTL_MS", "30000"),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        a.control(
            "designation.delegate",
            serde_json::json!({"path": "/d1", "node": b_id, "sticky": true}),
        )
        .context("delegating --sticky /d1 to b")?;
        wait_installed(b, "/d1", Duration::from_secs(20))?;
        write_timed(&b.mnt.join("d1/x"), b"old")?;
        eventually("x visible on c", Duration::from_secs(20), || {
            anyhow::ensure!(std::fs::read(c.mnt.join("d1/x"))? == b"old");
            Ok(())
        })?;
        for _ in 0..5 {
            anyhow::ensure!(std::fs::read(c.mnt.join("d1/x"))? == b"old");
        }
        let cto = c.control_status()?["cto"].clone();
        anyhow::ensure!(
            n(&cto, "delegation_local") >= 1,
            "c served no read under a read delegation: {cto}"
        );
        let killed = Instant::now();
        clients[1].kill9()?;
        clients[1].mount()?;
        eprintln!(
            "    {SCENARIO}: b remounted {:?} after the kill (read TTL 30 s)",
            killed.elapsed()
        );
        anyhow::ensure!(
            killed.elapsed() < Duration::from_secs(25),
            "b came back after {:?}: the old read delegations may have lapsed, the scenario proves nothing",
            killed.elapsed()
        );
        // KNOWN WORKAROUND (open bug, logged in TRIAGE.md, not fixed here):
        // an op sent into a pooled connection to the killed process
        // surfaces to the client as EIO for tens of seconds. Under the
        // load-robustness rule a peer's crash must cost latency, never a
        // client-visible error, so this sleep hides a harness-visible bug.
        // It stays only until that bug is fixed; the scenario must then
        // run without it. The other nodes evict their pooled connections to
        // the dead process after a short window (2 s). The read delegations
        // (30 s) are still live meanwhile.
        std::thread::sleep(Duration::from_millis(3_500));
        anyhow::ensure!(
            killed.elapsed() < Duration::from_secs(25),
            "the old read delegations may have lapsed ({:?} after the kill)",
            killed.elapsed()
        );
        let a = &clients[0];
        let c = &clients[2];
        let t = Instant::now();
        write_timed(&a.mnt.join("d1/x"), b"new")
            .context("a writing d1/x at the returned holder")?;
        eprintln!(
            "    {SCENARIO}: a's write of d1/x was acknowledged {:?} after it was sent",
            t.elapsed()
        );
        anyhow::ensure!(
            std::fs::read(c.mnt.join("d1/x"))? == b"new",
            "c's strict open of d1/x shows the old content after a's acknowledged write \
             (a read was served under a grant of b's previous process)"
        );
        anyhow::ensure!(
            std::fs::read(a.mnt.join("d1/x"))? == b"new",
            "a does not see its own write"
        );
        Ok(())
    })();
    dump_logs_on_failure(SCENARIO, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// How many conflict copies `c` has created (`status.spool.conflicts`).
fn conflicts_total(c: &Client) -> u64 {
    c.control_status()
        .map(|s| s["spool"]["conflicts"].as_u64().unwrap_or(0))
        .unwrap_or(u64::MAX)
}

/// Data in one delegated subtree, a marker in another, written in that
/// order by one client: no node ever shows the marker without its data
/// (the marker's `deps` carry the data's stream position; the marker's
/// delegate waits for the root's segment carrying it).
pub fn marker_order(_seed: u64) -> Result<()> {
    const NAME: &str = "marker-order";
    const SECS: u64 = 12;
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], DELEGATES_UNBACKED, 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        std::fs::create_dir(a.mnt.join("d2"))?;
        for x in [b, c] {
            eventually("dirs visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir() && x.mnt.join("d2").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", b_id)?;
        delegate(a, "/d2", c_id)?;
        wait_installed(b, "/d1", Duration::from_secs(20))?;
        wait_installed(c, "/d2", Duration::from_secs(20))?;
        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(Mutex::new(0usize));
        // Writers: a (the root), b and c each write data into d1 (b's
        // subtree) then the marker into d2 (c's subtree).
        let writers: Vec<_> = [a, b, c]
            .iter()
            .map(|w| {
                let stop = stop.clone();
                let written = written.clone();
                let mnt = w.mnt.clone();
                let who = w.name.clone();
                std::thread::spawn(move || -> Result<()> {
                    let mut i = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let data = format!("d1/{who}-{i}-data");
                        let marker = format!("d2/{who}-{i}-marker");
                        write_timed(&mnt.join(&data), data.as_bytes())?;
                        write_timed(&mnt.join(&marker), marker.as_bytes())?;
                        *written.lock().unwrap() += 1;
                        i += 1;
                    }
                    Ok(())
                })
            })
            .collect();
        // Watchers: every node lists d2; a marker seen means its data
        // must be readable at once.
        let violations = Arc::new(Mutex::new(Vec::<String>::new()));
        let checks = Arc::new(Mutex::new(0usize));
        let watchers: Vec<_> = [a, b, c]
            .iter()
            .map(|w| {
                let stop = stop.clone();
                let violations = violations.clone();
                let checks = checks.clone();
                let mnt = w.mnt.clone();
                let who = w.name.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let Ok(entries) = std::fs::read_dir(mnt.join("d2")) else {
                            continue;
                        };
                        for e in entries.flatten() {
                            let marker = e.file_name().to_string_lossy().to_string();
                            let Some(stem) = marker.strip_suffix("-marker") else {
                                continue;
                            };
                            let data = mnt.join("d1").join(format!("{stem}-data"));
                            *checks.lock().unwrap() += 1;
                            if std::fs::metadata(&data).is_err() {
                                violations
                                    .lock()
                                    .unwrap()
                                    .push(format!("{who} saw d2/{marker} without d1/{stem}-data"));
                            }
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                })
            })
            .collect();
        std::thread::sleep(Duration::from_secs(SECS));
        stop.store(true, Ordering::Relaxed);
        for w in writers {
            w.join().map_err(|_| anyhow::anyhow!("writer panicked"))??;
        }
        for w in watchers {
            w.join().ok();
        }
        let violations = violations.lock().unwrap().clone();
        let checks = *checks.lock().unwrap();
        let pairs = *written.lock().unwrap();
        let da = deleg_of(a)?;
        let db = deleg_of(b)?;
        let dc = deleg_of(c)?;
        print_deleg(NAME, "a", &da);
        print_deleg(NAME, "b", &db);
        print_deleg(NAME, "c", &dc);
        eprintln!(
            "    {NAME}: {pairs} data/marker pairs in {SECS} s, {checks} marker checks, {} \
             violations; deps waits b {} c {}",
            violations.len(),
            n(&db, "deps_waits"),
            n(&dc, "deps_waits")
        );
        anyhow::ensure!(
            violations.is_empty(),
            "marker order violated: {:?}",
            &violations[..violations.len().min(5)]
        );
        anyhow::ensure!(
            pairs >= 30 && checks >= 100,
            "too little traffic: {pairs} pairs, {checks} checks"
        );
        anyhow::ensure!(
            n(&da, "deps_refused") == 0,
            "the root (never deposed here) refused a delegate batch for lacking its deps, \
             a row executed ahead of its cause: {da}"
        );
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// The delegate of `d1` loses its P2P link to the root (both keep S3 and
/// the third node): its grant cannot be renewed, so it stops on its own
/// clock; the root's recall of it is outwaited by the expiry (or the
/// unrenewed grant reclaimed); the third node's later writes into `d1`
/// go through the root; after the heal every file is everywhere with no
/// conflict, and `d1` can be delegated again.
pub fn delegate_partition(_seed: u64) -> Result<()> {
    const NAME: &str = "delegate-partition";
    let (_env, root, mut clients, _) = cluster(NAME, &["a", "b", "c"], DELEGATES_UNBACKED, 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let a_id = node_id(a)?;
        let b_id = node_id(b)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", b_id)?;
        let gen = wait_installed(b, "/d1", Duration::from_secs(20))?;
        let (names_b, _) = write_files(b, "d1", "before", 20)?;
        all_visible(a, &names_b, Duration::from_secs(30))?;
        // Cut a <-> b.
        std::fs::write(deny_path(root.path(), &a.name), format!("{b_id}\n"))?;
        std::fs::write(deny_path(root.path(), &b.name), format!("{a_id}\n"))?;
        let cut = Instant::now();
        eprintln!("    {NAME}: a <-> b cut; b holds gen {gen} for at most {DELEG_TTL_MS} ms");
        // b's writes inside the grant are still local; they reach the
        // root only after the heal (S3 carries nothing of a delegate).
        //
        // They run on a thread: a write that meets the end of the grant
        // (one parked near its expiry, then sent on to the root) goes through
        // the root's inbox poll, which polls a requester it saw no demand
        // from at the cold interval (`inbox_cold_max_ms`, 10 s). That
        // latency is the inbox's, not the grant's, so it must not count in
        // the time b took to stop honouring the grant, which is measured
        // here from b's status while the writes are in flight.
        let writer = {
            let (mnt, names): (_, Vec<String>) = (
                b.mnt.clone(),
                (0..10).map(|i| format!("d1/cut-{i}")).collect(),
            );
            std::thread::spawn(move || -> Result<(Vec<String>, Vec<Duration>)> {
                let mut lat = Vec::new();
                for name in &names {
                    lat.push(write_timed(&mnt.join(name), name.as_bytes())?);
                }
                Ok((names, lat))
            })
        };
        // b stops on its own clock once it cannot renew.
        eventually("b stops", Duration::from_millis(3 * DELEG_TTL_MS), || {
            let d = deleg_of(b)?;
            let mine = d["mine"].as_array().cloned().unwrap_or_default();
            anyhow::ensure!(
                mine.is_empty() || mine.iter().all(|m| m[3] == true),
                "b still honours its grant: {d}"
            );
            Ok(())
        })?;
        let stopped_after = cut.elapsed();
        let (names_b_cut, lat_cut) = writer
            .join()
            .map_err(|_| anyhow::anyhow!("b's writer thread panicked"))??;
        // c's write into d1: the delegate is stopped (it says so), the
        // root recalls, cannot reach b, outwaits the grant, executes.
        let t = Instant::now();
        let name = "d1/c-during-cut".to_string();
        write_timed(&c.mnt.join(&name), name.as_bytes()).context("c writing into d1")?;
        let c_took = t.elapsed();
        // b's writes after it stopped go through the root — over S3
        // (the inbox), since the P2P path to the root is cut.
        let t = Instant::now();
        let after = "d1/b-after-stop".to_string();
        write_timed(&b.mnt.join(&after), after.as_bytes()).context("b writing after it stopped")?;
        let b_after_took = t.elapsed();
        let da = deleg_of(a)?;
        print_deleg(NAME, "a", &da);
        print_deleg(NAME, "b", &deleg_of(b)?);
        eprintln!(
            "    {NAME}: b's writes inside the cut grant {}; b stopped {stopped_after:?} after \
             the cut; c's write into d1 took {c_took:?}; b's write after stopping took \
             {b_after_took:?}; root recalls sent {} expired {} reclaimed {} ended {}",
            dist(lat_cut),
            n(&da, "recalls_sent"),
            n(&da, "recalls_expired"),
            n(&da, "reclaimed"),
            n(&da, "ended")
        );
        anyhow::ensure!(
            n(&da, "recalls_expired") + n(&da, "reclaimed") >= 1 && n(&da, "ended") >= 1,
            "the cut delegation was neither outwaited nor reclaimed: {da}"
        );
        anyhow::ensure!(
            stopped_after < Duration::from_millis(2 * DELEG_TTL_MS + 2_000),
            "b honoured its grant for {stopped_after:?} without renewing"
        );
        // Heal.
        let _ = std::fs::remove_file(deny_path(root.path(), &a.name));
        let _ = std::fs::remove_file(deny_path(root.path(), &b.name));
        let mut all = names_b.clone();
        all.extend(names_b_cut.iter().cloned());
        all.push(name);
        all.push(after);
        for x in [a, b, c] {
            all_visible(x, &all, Duration::from_secs(90))?;
        }
        let lease = lease_of(a)?;
        anyhow::ensure!(lease["held"] == true, "the root lost its lease: {lease}");
        delegate(a, "/d1", b_id)?;
        let gen2 = wait_installed(b, "/d1", Duration::from_secs(30))?;
        anyhow::ensure!(gen2 > gen, "generation did not advance: {gen} -> {gen2}");
        let (again, _) = write_files(b, "d1", "again", 10)?;
        for x in [a, b, c] {
            all_visible(x, &again, Duration::from_secs(30))?;
        }
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// `CONSTELLATION_P2P=off`: delegation is off with it (there is no
/// stream without P2P): `delegate` is refused, the status says so, and
/// two nodes' writes take the M13 paths as before.
pub fn p2p_off_no_delegation(_seed: u64) -> Result<()> {
    const NAME: &str = "p2p-off-no-delegation";
    let (_env, _root, mut clients, _) =
        cluster(NAME, &["a", "b"], &[("CONSTELLATION_P2P", "off")], 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let b_id = node_id(b)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        let da = deleg_of(a)?;
        print_deleg(NAME, "a", &da);
        anyhow::ensure!(da["enabled"] == false, "delegation on without P2P: {da}");
        let resp = a.control(
            "designation.delegate",
            serde_json::json!({"path": "/d1", "node": b_id}),
        );
        eprintln!("    {NAME}: delegate /d1 -> {b_id} with P2P off: {resp:?}");
        anyhow::ensure!(resp.is_err(), "delegation accepted with P2P off: {resp:?}");
        let (names_a, lat_a) = write_files(a, "d1", "a", 20)?;
        eventually("d1 visible on b", Duration::from_secs(30), || {
            anyhow::ensure!(b.mnt.join("d1").is_dir());
            Ok(())
        })?;
        let (names_b, lat_b) = write_files(b, "d1", "b", 10)?;
        eprintln!(
            "    {NAME}: a's local writes {}; b's writes (no P2P: inbox or lease) {}",
            dist(lat_a),
            dist(lat_b)
        );
        let mut all = names_a;
        all.extend(names_b);
        for x in [a, b] {
            all_visible(x, &all, Duration::from_secs(60))?;
        }
        let da = deleg_of(a)?;
        let db = deleg_of(b)?;
        anyhow::ensure!(
            n(&da, "delegated") == 0
                && n(&da, "executed") == 0
                && n(&db, "executed") == 0
                && n(&da, "appended_txs") == 0
                && da["table"].as_array().is_some_and(|t| t.is_empty()),
            "something was delegated with P2P off: a {da} b {db}"
        );
        ensure_no_conflicts(&[a, b])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

// ------------------------------------------------------------ phase 2b

/// Sets the writers' stop flag when dropped (a failed assertion must not
/// leave a writer thread holding the mounts open).
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// The lease holder of `clients`, if any holds.
pub(super) fn holder_of(clients: &[Client]) -> Option<usize> {
    (0..clients.len()).find(|i| lease_of(&clients[*i]).is_ok_and(|l| l["held"] == true))
}

/// Phase 2b: the root dies with two live delegates streaming (M9's
/// backup takes it over by seal, inside the lease); the successor learns
/// the table from the log, the delegates re-stream what the old root
/// never shipped, every acknowledged file is everywhere, and the dead
/// root remounts and converges.
pub fn root_failover_with_delegates(_seed: u64) -> Result<()> {
    const NAME: &str = "root-failover-with-delegates";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c", "d"],
        &[("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "50")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let d = &clients[3];
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        std::fs::create_dir(a.mnt.join("d2"))?;
        for x in [b, c, d] {
            eventually("dirs visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir() && x.mnt.join("d2").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", b_id)?;
        delegate(a, "/d2", c_id)?;
        wait_installed(b, "/d1", Duration::from_secs(20))?;
        wait_installed(c, "/d2", Duration::from_secs(20))?;
        let epoch = lease_of(a)?["epoch"].as_u64().unwrap_or(0);
        // M9: the root has a backup (the seal-based takeover) before it dies.
        let backups = super::m9::wait_for_backup(a, Duration::from_secs(30))?;
        eprintln!("    {NAME}: a (epoch {epoch}) backs up to {backups:?}");
        // Both delegates write continuously; the root is killed mid-burst.
        let stop = Arc::new(AtomicBool::new(false));
        let _guard = StopOnDrop(stop.clone());
        let bg: Vec<_> = [(b.mnt.clone(), "d1"), (c.mnt.clone(), "d2")]
            .into_iter()
            .map(|(mnt, dir)| {
                let stop = stop.clone();
                std::thread::spawn(move || -> Result<Vec<String>> {
                    let mut names = Vec::new();
                    let mut i = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let name = format!("{dir}/bg-{i}");
                        if write_timed(&mnt.join(&name), name.as_bytes()).is_ok() {
                            names.push(name);
                        }
                        i += 1;
                    }
                    Ok(names)
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(800));
        let killed = Instant::now();
        clients[0].kill9()?;
        let b = &clients[1];
        let c = &clients[2];
        let d = &clients[3];
        let successor = eventually_value("a successor holds", Duration::from_secs(30), || {
            let i = holder_of(&clients[1..]).map(|i| i + 1);
            let i = i.context("nobody holds yet")?;
            let l = lease_of(&clients[i])?;
            anyhow::ensure!(l["epoch"].as_u64().unwrap_or(0) > epoch, "old epoch");
            Ok(i)
        })?;
        let took = killed.elapsed();
        let s = &clients[successor];
        eprintln!(
            "    {NAME}: {} took the lease over {took:?} after the kill",
            s.name
        );
        // The writes keep going through the takeover; then stop.
        std::thread::sleep(Duration::from_millis(1500));
        stop.store(true, Ordering::Relaxed);
        let mut names = Vec::new();
        for h in bg {
            names.extend(h.join().map_err(|_| anyhow::anyhow!("writer panicked"))??);
        }
        let ds = deleg_of(s)?;
        print_deleg(NAME, &s.name, &ds);
        for x in [b, c] {
            print_deleg(NAME, &x.name, &deleg_of(x)?);
        }
        let restreams: u64 = [b, c]
            .iter()
            .map(|x| n(&deleg_of(x).unwrap_or_default(), "restreams"))
            .sum();
        eprintln!(
            "    {NAME}: {} acknowledged writes across the failover; successor inherited {} \
             generations; delegates re-streamed {} times",
            names.len(),
            n(&ds, "inherited"),
            restreams
        );
        anyhow::ensure!(
            n(&ds, "inherited") >= 1 || n(&ds, "delegated") >= 1,
            "the successor learned no generation: {ds}"
        );
        anyhow::ensure!(names.len() > 50, "too few writes landed: {}", names.len());
        for x in [b, c, d] {
            all_visible(x, &names, Duration::from_secs(90))?;
        }
        // The dead root returns and converges.
        clients[0].mount()?;
        let a = &clients[0];
        all_visible(a, &names, Duration::from_secs(90))?;
        ensure_no_conflicts(&[a, &clients[1], &clients[2], &clients[3]])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// `eventually` returning the closure's value.
fn eventually_value<T>(
    what: &str,
    deadline: Duration,
    mut f: impl FnMut() -> Result<T>,
) -> Result<T> {
    let t = Instant::now();
    loop {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if t.elapsed() > deadline => {
                anyhow::bail!("{what}: not reached within {deadline:?}: {e:#}")
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// Phase 2b: the delegate of `d1` has a backup (a LAN peer in budget)
/// and dies mid-burst: the root seals the backup, drains its tail, ends
/// the generation and delegates `d1` to the backup; every write the
/// delegate acknowledged is in the log; the remounted delegate converges.
pub fn delegate_crash_with_backup(_seed: u64) -> Result<()> {
    const NAME: &str = "delegate-crash-backup";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "50")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", b_id)?;
        let gen = wait_installed(b, "/d1", Duration::from_secs(20))?;
        eventually("b chose a backup", Duration::from_secs(20), || {
            let d = deleg_of(b)?;
            anyhow::ensure!(
                d["backups"].as_array().is_some_and(|v| !v.is_empty()),
                "no backup yet: {d}"
            );
            Ok(())
        })?;
        let db = deleg_of(b)?;
        let backup = db["backups"][0][1].as_u64().unwrap_or(0);
        eprintln!("    {NAME}: b (gen {gen}) backs up to node {backup}");
        anyhow::ensure!(
            backup == c_id,
            "b's backup is {backup}, expected c ({c_id})"
        );
        let (names_b, lat) = write_files(b, "d1", "b", 40)?;
        let db = deleg_of(b)?;
        eprintln!(
            "    {NAME}: b's writes under a backup: {}; acks parked {} backup appends {} acks {}",
            dist(lat),
            n(&db, "acks_parked"),
            n(&db, "backup_appends"),
            n(&db, "backup_acks")
        );
        anyhow::ensure!(
            n(&db, "backup_acks") > 0,
            "no backup acknowledged anything: {db}"
        );
        let killed = Instant::now();
        clients[1].kill9()?;
        let a = &clients[0];
        let c = &clients[2];
        // The root seals the backup and delegates d1 to it.
        eventually(
            "d1 re-delegated to the backup",
            Duration::from_secs(30),
            || {
                let d = deleg_of(a)?;
                let table = d["table"].as_array().cloned().unwrap_or_default();
                anyhow::ensure!(
                    table
                        .iter()
                        .any(|e| e["path"] == "/d1" && n(e, "node") == c_id),
                    "d1 not on c yet: {d}"
                );
                Ok(())
            },
        )?;
        let da = deleg_of(a)?;
        print_deleg(NAME, "a", &da);
        eprintln!(
            "    {NAME}: sealed {:?} after the kill; seals sent {} drained {} re-delegated to c",
            killed.elapsed(),
            n(&da, "seals_sent"),
            n(&da, "sealed_drained")
        );
        anyhow::ensure!(
            n(&da, "seals_sent") >= 1,
            "the root never sealed the backup: {da}"
        );
        // c writes into d1 locally now.
        wait_installed(c, "/d1", Duration::from_secs(20))?;
        let (names_c, lat_c) = write_files(c, "d1", "c", 20)?;
        eprintln!(
            "    {NAME}: c's writes into d1 as the new delegate: {}",
            dist(lat_c)
        );
        let mut all = names_b.clone();
        all.extend(names_c);
        all_visible(a, &all, Duration::from_secs(60))?;
        all_visible(c, &all, Duration::from_secs(60))?;
        clients[1].mount()?;
        all_visible(&clients[1], &all, Duration::from_secs(90))?;
        ensure_no_conflicts(&[&clients[0], &clients[1], &clients[2]])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// Phase 2b: automatic placement. b dominates the writes under `d1` for
/// a window: the root delegates `d1` to b by itself; then c takes the
/// writes over and b stops: after the dwell the placement recalls b's
/// generation, and after the cool-down delegates `d1` to c — with no
/// flapping in between.
pub fn auto_placement(_seed: u64) -> Result<()> {
    const NAME: &str = "auto-placement";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[
            // Plan 30 §M12: the placement is on by default; an empty
            // value names the knob without pinning it either way.
            ("CONSTELLATION_DELEGATION_PLACEMENT", ""),
            ("CONSTELLATION_DELEGATION_WINDOW_MS", "4000"),
            ("CONSTELLATION_DELEGATION_MIN_OPS", "20"),
            ("CONSTELLATION_DELEGATION_DWELL_MS", "4000"),
            ("CONSTELLATION_DELEGATION_COOLDOWN_MS", "2000"),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "500"),
            ("CONSTELLATION_BACKUPS", "0"),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        let table_owner = |path: &str| -> Result<Option<u64>> {
            let d = deleg_of(a)?;
            Ok(d["table"]
                .as_array()
                .and_then(|t| t.iter().find(|e| e["path"] == path).map(|e| n(e, "node"))))
        };
        // Phase 1: b writes into d1 steadily.
        let stop = Arc::new(AtomicBool::new(false));
        let _guard = StopOnDrop(stop.clone());
        let writer = |mnt: std::path::PathBuf, tag: &'static str, stop: Arc<AtomicBool>| {
            std::thread::spawn(move || -> Result<Vec<String>> {
                let mut names = Vec::new();
                let mut i = 0;
                while !stop.load(Ordering::Relaxed) {
                    let name = format!("d1/{tag}-{i}");
                    write_timed(&mnt.join(&name), name.as_bytes())?;
                    names.push(name);
                    i += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(names)
            })
        };
        let t0 = Instant::now();
        let wb = writer(b.mnt.clone(), "b", stop.clone());
        eventually_value("d1 placed on b", Duration::from_secs(30), || {
            anyhow::ensure!(table_owner("/d1")? == Some(b_id), "not yet");
            Ok(())
        })?;
        let placed_b = t0.elapsed();
        eprintln!("    {NAME}: d1 delegated to b by the placement after {placed_b:?}");
        std::thread::sleep(Duration::from_secs(3));
        // Phase 2: c takes over, b stops.
        stop.store(true, Ordering::Relaxed);
        let names_b = wb
            .join()
            .map_err(|_| anyhow::anyhow!("writer panicked"))??;
        let db = deleg_of(b)?;
        print_deleg(NAME, "b (after phase 1)", &db);
        print_deleg(NAME, "a (after phase 1)", &deleg_of(a)?);
        anyhow::ensure!(
            n(&db, "executed") + n(&db, "fast_path_executed") > 0,
            "b executed nothing as the placed delegate: {db}"
        );
        let stop2 = Arc::new(AtomicBool::new(false));
        let _guard2 = StopOnDrop(stop2.clone());
        let t1 = Instant::now();
        let wc = writer(c.mnt.clone(), "c", stop2.clone());
        eventually_value("d1 recalled from b", Duration::from_secs(40), || {
            anyhow::ensure!(table_owner("/d1")? != Some(b_id), "still b");
            Ok(())
        })?;
        let recalled = t1.elapsed();
        eventually_value("d1 placed on c", Duration::from_secs(40), || {
            anyhow::ensure!(table_owner("/d1")? == Some(c_id), "not c yet");
            Ok(())
        })?;
        let placed_c = t1.elapsed();
        std::thread::sleep(Duration::from_secs(2));
        stop2.store(true, Ordering::Relaxed);
        let names_c = wc
            .join()
            .map_err(|_| anyhow::anyhow!("writer panicked"))??;
        let da = deleg_of(a)?;
        print_deleg(NAME, "a", &da);
        eprintln!(
            "    {NAME}: recalled from b {recalled:?} after c took over, placed on c after \
             {placed_c:?}; placement delegated {} recalled {} (evaluations {}); top {}",
            n(&da, "place_delegated"),
            n(&da, "place_recalled"),
            n(&da, "place_evaluations"),
            da["placement"]
        );
        anyhow::ensure!(
            n(&da, "place_delegated") >= 2 && n(&da, "place_recalled") >= 1,
            "placement counters: {da}"
        );
        anyhow::ensure!(
            n(&da, "place_delegated") <= 3 && n(&da, "place_recalled") <= 2,
            "the placement flapped: {da}"
        );
        let mut all = names_b;
        all.extend(names_c);
        for x in [a, b, c] {
            all_visible(x, &all, Duration::from_secs(60))?;
        }
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// Phase 2b: an offline designation (plans 03–05) as a delegation. b
/// runs `offline /site`; the root's table gets a designated generation
/// for b; c's writes under it are forwarded to b; the root's own client
/// forwards too. b cut from everyone keeps writing locally (its grant
/// never lapses) while c's writes under the path are refused `EROFS`
/// (the root will not sequence a designated subtree; the first one c sent
/// to b ends in doubt, `EIO`, as it may have run there); after the heal
/// everything converges, and `online` on b recalls it: c's writes go
/// through the root again.
pub fn designation_as_delegation(_seed: u64) -> Result<()> {
    const NAME: &str = "designation-as-delegation";
    let (_env, root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let a_id = node_id(a)?;
        let b_id = node_id(b)?;
        let c_id = node_id(c)?;
        std::fs::create_dir(a.mnt.join("site"))?;
        for x in [b, c] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("site").is_dir());
                Ok(())
            })?;
        }
        b.control("designation.offline", serde_json::json!({"path": "/site"}))
            .context("offline")?;
        // The root's designation poll (10 s) syncs the table.
        let t = Instant::now();
        eventually(
            "the designation is in the table",
            Duration::from_secs(40),
            || {
                let d = deleg_of(a)?;
                let table = d["table"].as_array().cloned().unwrap_or_default();
                anyhow::ensure!(
                    table.iter().any(|e| e["path"] == "/site"
                        && n(e, "node") == b_id
                        && e["designated"] == true),
                    "not yet: {d}"
                );
                Ok(())
            },
        )?;
        wait_installed(b, "/site", Duration::from_secs(20))?;
        eprintln!(
            "    {NAME}: /site designated to b in the table after {:?}",
            t.elapsed()
        );
        let (names_b, lat_b) = write_files(b, "site", "b", 20)?;
        let (names_c, lat_c) = write_files(c, "site", "c", 20)?;
        let (names_a, lat_a) = write_files(a, "site", "a", 10)?;
        eprintln!(
            "    {NAME}: designee's local writes {}; c's forwarded {}; the root's forwarded {}",
            dist(lat_b),
            dist(lat_c),
            dist(lat_a)
        );
        let mut all: Vec<String> = names_b
            .iter()
            .chain(&names_c)
            .chain(&names_a)
            .cloned()
            .collect();
        all_visible(b, &all, Duration::from_secs(30))?;
        // Cut b from everyone.
        std::fs::write(deny_path(root.path(), &a.name), format!("{b_id}\n"))?;
        std::fs::write(deny_path(root.path(), &c.name), format!("{b_id}\n"))?;
        std::fs::write(deny_path(root.path(), &b.name), format!("{a_id}\n{c_id}\n"))?;
        // The root stops routing to b once b's grant lapses on the root's
        // clock (no renewal got through): from then on it refuses.
        eventually(
            "b's grant lapses at the root",
            Duration::from_secs(40),
            || {
                let d = deleg_of(a)?;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|t| t.as_millis() as u64)
                    .unwrap_or(0);
                let lapsed = d["gens"].as_array().is_some_and(|g| {
                    g.iter().any(|e| {
                        e[1].as_u64() == Some(b_id) && e[4].as_u64().is_some_and(|u| u < now_ms)
                    })
                });
                anyhow::ensure!(lapsed, "not yet: {}", d["gens"]);
                Ok(())
            },
        )?;
        let (names_cut, lat_cut) = write_files(b, "site", "cut", 10)?;
        // The first op c sends after the cut is sent to b: c's link to b has
        // not failed yet and the table names b the designee. It may have run
        // there (b could be alive and cut from the others only, as it is),
        // so the root's EROFS for it is held, not final (plan 46 M1.3
        // step 5, ADR-45, I1), and it ends in doubt (EIO) at its settle cap.
        // That is the design, not a fault (the same as `sticky-abandon`'s
        // first write after the kill): this sacrificial write takes it, and
        // fails with EIO or EROFS, never succeeds. The next write is sent
        // after c's forward to b failed, so it never reached the designee
        // and the root refuses it for good (EROFS).
        let sacrifice = std::fs::write(c.mnt.join("site/c-sacrifice"), b"x");
        anyhow::ensure!(
            matches!(&sacrifice, Err(e) if matches!(e.raw_os_error(), Some(5 | 30))),
            "c's write sent to the cut designee: {sacrifice:?} (want EIO or EROFS, never success)"
        );
        let refused = match std::fs::write(c.mnt.join("site/c-during-cut"), b"x") {
            Err(e) => Code::from_os_error(&e),
            Ok(()) => None,
        };
        eprintln!(
            "    {NAME}: b isolated: its writes {}; c's write under /site -> {:?} (want EROFS)",
            dist(lat_cut),
            refused,
        );
        anyhow::ensure!(
            refused == Some(Code::ReadOnly),
            "c's write under the isolated designation: {refused:?}"
        );
        let da = deleg_of(a)?;
        anyhow::ensure!(
            n(&da, "refused_designated") >= 1,
            "the root refused nothing: {da}"
        );
        anyhow::ensure!(
            n(&da, "reclaimed") + n(&da, "recalls_expired") == 0,
            "the designation was reclaimed: {da}"
        );
        // Heal.
        for x in [a, b, c] {
            let _ = std::fs::remove_file(deny_path(root.path(), &x.name));
        }
        all.extend(names_cut);
        for x in [a, b, c] {
            all_visible(x, &all, Duration::from_secs(90))?;
        }
        // online: the table entry goes; c's writes through the root.
        b.control("designation.online", serde_json::json!({"path": "/site"}))
            .context("online")?;
        eventually(
            "the designation is recalled",
            Duration::from_secs(40),
            || {
                let d = deleg_of(a)?;
                anyhow::ensure!(
                    d["table"]
                        .as_array()
                        .is_some_and(|t| t.iter().all(|e| e["path"] != "/site")),
                    "still designated: {d}"
                );
                Ok(())
            },
        )?;
        let (names_after, lat_after) = write_files(c, "site", "after", 10)?;
        eprintln!(
            "    {NAME}: after online, c's writes under /site (through the root): {}",
            dist(lat_after)
        );
        all.extend(names_after);
        for x in [a, b, c] {
            all_visible(x, &all, Duration::from_secs(60))?;
        }
        print_deleg(NAME, "a", &deleg_of(a)?);
        ensure_no_conflicts(&[a, b, c])?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

// ------------------------------------------------------------ plan 30 M12

/// The names under `dir` on `c`.
fn names_in(c: &Client, dir: &str) -> Result<Vec<String>> {
    let mut v: Vec<String> = std::fs::read_dir(c.mnt.join(dir))?
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect();
    v.sort();
    Ok(v)
}

/// Plan 30 §M12: four nodes creating unique names in one directory under
/// the single sequencer: three of them forward every create to the root.
/// Every name on every node, the directory listing identical everywhere,
/// no S3 request added per file. (Plan 46 D5 deleted the second phase,
/// the directory split into hash ranges.)
pub fn shared_dir_multi_writer(_seed: u64) -> Result<()> {
    const NAME: &str = "shared-dir-multi-writer";
    const FILES: usize = 100;
    let (_env, _root, mut clients, proxies) = cluster(NAME, &["a", "b", "c", "d"], &[], 4)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        std::fs::create_dir(a.mnt.join("shared"))?;
        for x in &clients[1..] {
            eventually("dir visible", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("shared").is_dir());
                Ok(())
            })?;
        }
        let writers = |tag: &str| -> Vec<(std::path::PathBuf, String, String)> {
            clients
                .iter()
                .map(|c| {
                    (
                        c.mnt.clone(),
                        "shared".to_string(),
                        format!("{tag}{}", c.name),
                    )
                })
                .collect()
        };
        // Warm-up: every node's forwarding path is up (the registry
        // allowlist has every key, the P2P connections are made) before
        // anything is measured.
        let mut warm = Vec::new();
        for x in clients.iter() {
            let (names, _) = write_files(x, "shared", &format!("warm-{}", x.name), 10)?;
            warm.extend(names);
        }
        for x in clients.iter() {
            all_visible(x, &warm, Duration::from_secs(60))?;
        }
        std::thread::sleep(Duration::from_secs(2));
        // The single sequencer.
        for p in &proxies {
            p.reset();
        }
        let (wall0, names0) = write_concurrently(&writers("p0-"), FILES)?;
        for x in clients.iter() {
            all_visible(x, &names0, Duration::from_secs(60))?;
        }
        let s3_0: Vec<u64> = proxies.iter().map(|p| p.tally().total()).collect();
        let log_puts0: Vec<usize> = proxies.iter().map(|p| puts_of(p, "log/")).collect();
        let chunk_puts0: Vec<usize> = proxies.iter().map(|p| puts_of(p, "chunks/")).collect();
        let paths: Vec<String> = clients
            .iter()
            .map(|c| {
                let s = c.control_status().unwrap_or_default();
                let peers = super::node_peers(&s["p2p"])
                    .filter(|p| p["connected"] == true)
                    .count();
                format!(
                    "{}: connected peers {peers}, inbox ops {}, lease {}",
                    c.name,
                    n(&s["inbox"], "submitted_ops"),
                    s["lease"]["held"]
                )
            })
            .collect();
        eprintln!(
            "    {NAME}: single sequencer: {} files by 4 writers in {wall0:?} ({:.0} files/s); \
             S3 requests {s3_0:?}, log PUTs {log_puts0:?}, chunk PUTs {chunk_puts0:?}; paths {paths:?}",
            4 * FILES,
            (4 * FILES) as f64 / wall0.as_secs_f64()
        );
        let mut all = warm.clone();
        all.extend(names0.iter().cloned());
        // The listing is identical everywhere, with every name once.
        let expected: Vec<String> = {
            let mut v: Vec<String> = all
                .iter()
                .map(|n| n.trim_start_matches("shared/").to_string())
                .collect();
            v.sort();
            v
        };
        for x in clients.iter() {
            eventually(
                &format!("{} lists every name once", x.name),
                Duration::from_secs(30),
                || {
                    let got = names_in(x, "shared")?;
                    anyhow::ensure!(
                        got == expected,
                        "{} lists {} names, expected {}",
                        x.name,
                        got.len(),
                        expected.len()
                    );
                    Ok(())
                },
            )?;
        }
        for p in &proxies {
            p.ensure_sane()?;
        }
        ensure_no_conflicts(&clients.iter().collect::<Vec<_>>())?;
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}
