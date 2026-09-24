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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn deleg_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["delegation"].clone())
}

fn n(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn node_id(c: &Client) -> Result<u64> {
    c.control_status()?["node_id"]
        .as_u64()
        .with_context(|| format!("{} reports no node id", c.name))
}

fn print_deleg(scenario: &str, who: &str, d: &serde_json::Value) {
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
        n(d, "deps_unsatisfied_at_append"),
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
const TTL_MS: u64 = 20_000;
/// The grant TTL (`CONSTELLATION_DELEGATION_TTL_MS`): short enough that a
/// dead or cut-off delegate is reclaimed within the scenario.
const DELEG_TTL_MS: u64 = 3_000;

/// `names` nodes on a fresh filesystem with `extra` env on every mount;
/// node 0 holds the lease, `f` exists everywhere. The first `counting`
/// nodes reach S3 through a counting proxy each (returned in order).
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
    let deleg_ttl = DELEG_TTL_MS.to_string();
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
            .with_env("CONSTELLATION_DELEGATION_TTL_MS", &deleg_ttl)
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

fn unmount_all(clients: &mut [Client]) {
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
}

/// A failed scenario's daemon logs, kept under `/tmp/harness-m11-logs/`
/// (the mounts' temp dir goes with the scenario).
fn dump_logs_on_failure(scenario: &str, clients: &[Client], result: &Result<()>) {
    if result.is_ok() {
        return;
    }
    let dir = std::path::Path::new("/tmp/harness-m11-logs");
    let _ = std::fs::create_dir_all(dir);
    for c in clients {
        let path = dir.join(format!("{scenario}-{}.log", c.name));
        let _ = std::fs::write(&path, c.tail_log_n(400_000));
        eprintln!(
            "    {scenario}: {}'s log kept at {}",
            c.name,
            path.display()
        );
    }
}

/// `constellation delegate <path> --to <node>` through the root's
/// control socket.
fn delegate(root: &Client, path: &str, node: u64) -> Result<serde_json::Value> {
    let resp = root.control(&serde_json::json!({"cmd": "delegate", "path": path, "node": node}))?;
    anyhow::ensure!(
        resp["resp"] == "ok",
        "delegating {path} to {node} on {}: {resp}",
        root.name
    );
    Ok(resp)
}

/// `constellation undelegate <path>`.
fn undelegate(root: &Client, path: &str) -> Result<serde_json::Value> {
    let resp = root.control(&serde_json::json!({"cmd": "undelegate", "path": path}))?;
    anyhow::ensure!(
        resp["resp"] == "ok",
        "undelegating {path} on {}: {resp}",
        root.name
    );
    Ok(resp)
}

/// Wait until `delegate` holds a live (not stopped) grant on `dir`, the
/// directory named `path` on the root.
fn wait_installed(delegate: &Client, path: &str, deadline: Duration) -> Result<u64> {
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
fn s3_breakdown(p: &CountingProxy) -> String {
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
    let (_env, _root, mut clients, proxies) = cluster(NAME, &["a", "b", "c"], &[], 3)?;
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
            n(&da, "deps_unsatisfied_at_append") == 0,
            "the root appended a batch whose deps it lacked: {da}"
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
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
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
            n(&da, "deps_unsatisfied_at_append") == 0,
            "the root appended a batch whose deps it lacked: {da}"
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
        // The same directories can be delegated again (a new generation).
        delegate(a, "/d1", b_id)?;
        let gen2 = wait_installed(b, "/d1", Duration::from_secs(20))?;
        let (again, lat) = write_files(b, "d1", "again", 20)?;
        eprintln!(
            "    {NAME}: d1 delegated again to b (gen {gen2}); b's local writes {}",
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
    const NAME: &str = "delegate-crash";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
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
            "    {NAME}: b (gen {gen}) killed after {} acknowledged writes",
            names_b.len() + 1
        );
        // c's write into d1: refused by the dead delegate, executed by the
        // root once the grant is reclaimed (bounded by the grant TTL).
        let t = Instant::now();
        let name = "d1/c-after-crash".to_string();
        write_timed(&c.mnt.join(&name), name.as_bytes()).context("c writing into d1")?;
        let c_took = t.elapsed();
        let da = deleg_of(a)?;
        print_deleg(NAME, "a", &da);
        eprintln!(
            "    {NAME}: c's first write into d1 after the crash returned after {c_took:?} \
             ({:?} after the kill; grant TTL {DELEG_TTL_MS} ms); reclaimed {} recalls expired {}",
            killed.elapsed(),
            n(&da, "reclaimed"),
            n(&da, "recalls_expired")
        );
        anyhow::ensure!(
            n(&da, "reclaimed") + n(&da, "recalls_expired") >= 1 && n(&da, "ended") >= 1,
            "the root never reclaimed the dead delegate's grant: {da}"
        );
        anyhow::ensure!(
            c_took < Duration::from_millis(3 * DELEG_TTL_MS + 5_000),
            "c's write waited {c_took:?}"
        );
        anyhow::ensure!(
            std::fs::read(a.mnt.join(&name))? == name.as_bytes(),
            "the root does not show c's write"
        );
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
        print_deleg(NAME, "b (remounted)", &db);
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
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}

/// Data in one delegated subtree, a marker in another, written in that
/// order by one client: no node ever shows the marker without its data
/// (the marker's `deps` carry the data's stream position; the marker's
/// delegate waits for the root's segment carrying it).
pub fn marker_order(_seed: u64) -> Result<()> {
    const NAME: &str = "marker-order";
    const SECS: u64 = 12;
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
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
            n(&da, "deps_unsatisfied_at_append") == 0,
            "the root appended a batch whose deps it lacked: {da}"
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
    let (_env, root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
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
        let (names_b_cut, lat_cut) = write_files(b, "d1", "cut", 10)?;
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
        let resp =
            a.control(&serde_json::json!({"cmd": "delegate", "path": "/d1", "node": b_id}))?;
        eprintln!("    {NAME}: delegate /d1 -> {b_id} with P2P off: {resp}");
        anyhow::ensure!(
            resp["resp"] != "ok",
            "delegation accepted with P2P off: {resp}"
        );
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
