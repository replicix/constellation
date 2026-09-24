//! Plan 30 §M8 scenarios: `--cto strict` (close-to-open through ReadIndex
//! and read delegations) against `--cto bounded`, recalls, an unreachable
//! delegate, and the latency measurements the plan asks for.
//!
//! The harness process is the out-of-band channel: a writer's `close`
//! returns, and the very next thing the harness does is open the file on
//! the reader. Under strict mode the reader must see the new content every
//! time; under bounded mode it may not (and the scenario prints how often
//! and for how long).

use super::{eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{bail, Context, Result};
use std::path::Path;
use std::time::{Duration, Instant};

fn cto_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["cto"].clone())
}

fn n(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn print_cto(scenario: &str, who: &str, s: &serde_json::Value) {
    eprintln!(
        "    {scenario}: {who} cto: strict {} reads {} (holder {} delegation {} read-index {} \
         s3-tail {} degraded {}) read-index ms total {} histogram(log2 ms) {} renewals {} \
         installed {} raced {} held {} recalled {} | sequencer: served {} refused {} grants {} \
         live {} recalls sent {} acked {} expired {} ack-waits {} (ms total {}) held-replies {} \
         held-retries {} fuse-writes-recalled {} (ms total {})",
        s["strict"],
        n(s, "strict_reads"),
        n(s, "holder_local"),
        n(s, "delegation_local"),
        n(s, "read_index"),
        n(s, "s3_tail"),
        n(s, "degraded"),
        n(s, "read_index_ms_total"),
        s["read_index_ms"],
        n(s, "renewals"),
        n(s, "delegations_installed"),
        n(s, "delegations_raced"),
        n(s, "delegations_held"),
        n(s, "recalled"),
        n(s, "read_index_served"),
        n(s, "read_index_refused"),
        n(s, "grants"),
        n(s, "live_grants"),
        n(s, "recalls_sent"),
        n(s, "recalls_acked"),
        n(s, "recalls_expired"),
        n(s, "recall_waits"),
        n(s, "recall_wait_ms_total"),
        n(s, "held_replies"),
        n(s, "held_retries"),
        n(s, "fuse_writes_recalled"),
        n(s, "fuse_recall_wait_ms_total"),
    );
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn dist(mut v: Vec<Duration>) -> String {
    v.sort();
    format!(
        "n={} p50={:?} p90={:?} p99={:?} max={:?}",
        v.len(),
        pct(&v, 0.5),
        pct(&v, 0.9),
        pct(&v, 0.99),
        v.last().copied().unwrap_or_default()
    )
}

/// `n` nodes on a fresh filesystem, every one mounted with `--cto
/// <mode>` (through `CONSTELLATION_CTO`) plus `extra` env; node 0 holds
/// the lease and `f` exists everywhere.
fn cluster(
    scenario: &str,
    names: &[&str],
    mode: &str,
    extra: &[(&str, &str)],
) -> Result<(crate::s3env::S3Env, tempfile::TempDir, Vec<Client>)> {
    let (env, root) = setup(scenario)?;
    let backend = format!("s3://{BUCKET}/{scenario}-{}", ts());
    let mut clients = Vec::new();
    for name in names {
        let mut c = Client::new(root.path(), name, &env.direct_endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_CTO", mode);
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
        eventually("f visible everywhere", Duration::from_secs(20), || {
            anyhow::ensure!(
                std::fs::read(c.mnt.join("f")).ok().as_deref() == Some(&b"initial"[..]),
                "f not visible on {}",
                c.name
            );
            Ok(())
        })?;
    }
    Ok((env, root, clients))
}

fn unmount_all(clients: &mut [Client]) {
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
}

/// Write `content` to `path` (open, write, close) and time it.
fn write_timed(path: &Path, content: &[u8]) -> Result<Duration> {
    let t = Instant::now();
    std::fs::write(path, content).with_context(|| format!("writing {}", path.display()))?;
    Ok(t.elapsed())
}

/// Open and read `path`, and time it.
fn read_timed(path: &Path) -> (std::io::Result<Vec<u8>>, Duration) {
    let t = Instant::now();
    let r = std::fs::read(path);
    (r, t.elapsed())
}

/// What one close-to-open loop observed on the reader.
#[derive(Default)]
struct Loop {
    /// Reads of a close's content right after it returned elsewhere.
    reads: usize,
    stale: Vec<String>,
    /// For each stale read: how long until the reader saw the content.
    time_to_visible: Vec<Duration>,
    read_latency: Vec<Duration>,
    write_latency: Vec<Duration>,
}

/// The loop both `cto-*` scenarios run: writers close, the harness at
/// once opens on the reader. Overwrites of `f` by a forwarding writer and
/// by the holder, and brand-new files (the lookup must see the entry).
fn close_then_open(
    writer: &Client,
    holder: &Client,
    reader: &Client,
    iterations: usize,
    strict: bool,
) -> Result<Loop> {
    let mut out = Loop::default();
    for i in 0..iterations {
        let content = format!("v{i}-{}", "x".repeat(i % 7));
        let by_holder = i % 4 == 3;
        let src = if by_holder { holder } else { writer };
        out.write_latency
            .push(write_timed(&src.mnt.join("f"), content.as_bytes())?);
        let new_name = format!("n{i}");
        src_write(src, &new_name, &content)?;
        for (name, want) in [("f", &content), (new_name.as_str(), &content)] {
            let path = reader.mnt.join(name);
            let (got, lat) = read_timed(&path);
            out.reads += 1;
            out.read_latency.push(lat);
            let ok = got.as_ref().is_ok_and(|g| g == want.as_bytes());
            if !ok {
                let what = format!(
                    "iteration {i}: {} read {name} as {:?} right after {} closed {:?}",
                    reader.name,
                    got.as_ref()
                        .map(|g| String::from_utf8_lossy(g).into_owned())
                        .map_err(|e| e.to_string()),
                    src.name,
                    want
                );
                if strict {
                    bail!("close-to-open violated under --cto strict: {what}");
                }
                let started = Instant::now();
                eventually(
                    "the close becomes visible (bounded)",
                    Duration::from_secs(10),
                    || {
                        anyhow::ensure!(
                            std::fs::read(&path).ok().as_deref() == Some(want.as_bytes()),
                            "not yet"
                        );
                        Ok(())
                    },
                )?;
                out.time_to_visible.push(started.elapsed() + lat);
                out.stale.push(what);
            }
        }
    }
    Ok(out)
}

fn src_write(src: &Client, name: &str, content: &str) -> Result<()> {
    std::fs::write(src.mnt.join(name), content.as_bytes())
        .with_context(|| format!("{} writing {name}", src.name))
}

fn report_loop(scenario: &str, l: &Loop) {
    eprintln!(
        "    {scenario}: {} reads right after a close elsewhere, {} stale",
        l.reads,
        l.stale.len()
    );
    for s in l.stale.iter().take(5) {
        eprintln!("    {scenario}:   stale: {s}");
    }
    if !l.time_to_visible.is_empty() {
        eprintln!(
            "    {scenario}: stale reads became visible after {}",
            dist(l.time_to_visible.clone())
        );
    }
    eprintln!(
        "    {scenario}: reader open+read latency {}",
        dist(l.read_latency.clone())
    );
    eprintln!(
        "    {scenario}: writer open+write+close latency {}",
        dist(l.write_latency.clone())
    );
}

const ITERATIONS: usize = 40;

/// `--cto strict`: every read that starts after another node's close
/// returned sees it — overwrites through a forwarding writer, the
/// holder's own overwrites, and new names. Three nodes: A (sequencer), W
/// (writer), R (reader).
pub fn cto_strict(_seed: u64) -> Result<()> {
    const NAME: &str = "cto-strict";
    let (_env, _root, mut clients) = cluster(NAME, &["a", "w", "r"], "strict", &[])?;
    let result = (|| -> Result<()> {
        let (a, w, r) = (&clients[0], &clients[1], &clients[2]);
        // Degraded reads while the cluster forms are expected: a node's
        // P2P allowlist learns a newly registered peer at its next
        // registry refresh, and until then that peer's ReadIndex cannot
        // reach it (M6's rule: answer from the replica, counted). Judge
        // the loop only.
        let before = cto_of(r)?;
        let l = close_then_open(w, a, r, ITERATIONS, true)?;
        report_loop(NAME, &l);
        print_cto(NAME, "A", &cto_of(a)?);
        print_cto(NAME, "W", &cto_of(w)?);
        let rc = cto_of(r)?;
        print_cto(NAME, "R", &rc);
        anyhow::ensure!(rc["strict"] == true, "R is not strict: {rc}");
        anyhow::ensure!(
            n(&rc, "degraded") == n(&before, "degraded"),
            "R answered strict reads degraded during the loop (no sequencer answer): {rc}"
        );
        anyhow::ensure!(
            n(&rc, "read_index") + n(&rc, "delegation_local") > 0,
            "R never asked the sequencer nor read under a delegation: {rc}"
        );
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// The same loop under `--cto bounded` (the default), documenting the
/// staleness strict mode removes: a read right after another node's
/// close may miss it, and sees it once the log reaches the reader (M7's
/// bound). The scenario fails only if a close never becomes visible.
pub fn cto_bounded(_seed: u64) -> Result<()> {
    const NAME: &str = "cto-bounded";
    let (_env, _root, mut clients) = cluster(NAME, &["a", "w", "r"], "bounded", &[])?;
    let result = (|| -> Result<()> {
        let (a, w, r) = (&clients[0], &clients[1], &clients[2]);
        let l = close_then_open(w, a, r, ITERATIONS, false)?;
        report_loop(NAME, &l);
        let rc = cto_of(r)?;
        print_cto(NAME, "R", &rc);
        anyhow::ensure!(rc["strict"] == false, "R is strict: {rc}");
        anyhow::ensure!(
            n(&rc, "read_index") == 0,
            "bounded mode sent a ReadIndex: {rc}"
        );
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// Read delegations: R's repeated opens of `f` cost one ReadIndex, then
/// none (local under the delegation); a write by W recalls R's
/// delegation before W's close returns (R acks), and R's next open sees
/// the new content. Measures R's first open against its delegated opens,
/// and W's close latency with and without an outstanding delegation.
pub fn cto_delegation_recall(_seed: u64) -> Result<()> {
    const NAME: &str = "cto-delegation-recall";
    let (_env, _root, mut clients) = cluster(NAME, &["a", "w", "r"], "strict", &[])?;
    let result = (|| -> Result<()> {
        let (a, w, r) = (&clients[0], &clients[1], &clients[2]);
        // Baseline: W's close latency with nobody holding a delegation on
        // `g` (a file R never opens).
        std::fs::write(w.mnt.join("g"), b"g0")?;
        let mut no_deleg = Vec::new();
        for i in 0..20 {
            no_deleg.push(write_timed(&w.mnt.join("g"), format!("g{i}").as_bytes())?);
        }
        // R opens `f` repeatedly: one ReadIndex, then local.
        let before = cto_of(r)?;
        let mut first = None;
        let mut delegated = Vec::new();
        for i in 0..30 {
            let (got, lat) = read_timed(&r.mnt.join("f"));
            got.context("R reading f")?;
            if i == 0 {
                first = Some(lat);
            } else {
                delegated.push(lat);
            }
        }
        let after = cto_of(r)?;
        let ri = n(&after, "read_index") - n(&before, "read_index");
        let local = n(&after, "delegation_local") - n(&before, "delegation_local");
        eprintln!(
            "    {NAME}: R's first strict open of f {:?} ({ri} ReadIndex round trips for 30 opens, \
             {local} strict reads under a delegation); delegated opens {}",
            first.unwrap_or_default(),
            dist(delegated)
        );
        anyhow::ensure!(
            ri <= 4,
            "R's 30 opens of f took {ri} ReadIndex round trips (delegations not used): {after}"
        );
        anyhow::ensure!(local >= 20, "few reads under a delegation: {after}");
        // Now W writes `f` while R holds the delegation: A recalls it.
        let a_before = cto_of(a)?;
        let mut with_deleg = Vec::new();
        for i in 0..10 {
            let content = format!("recalled-{i}");
            with_deleg.push(write_timed(&w.mnt.join("f"), content.as_bytes())?);
            let (got, _) = read_timed(&r.mnt.join("f"));
            let got = got.context("R reading f after W's close")?;
            anyhow::ensure!(
                got == content.as_bytes(),
                "R read {:?} after W closed {content:?}",
                String::from_utf8_lossy(&got)
            );
            // R re-reads a few times: a new delegation, local again.
            for _ in 0..3 {
                read_timed(&r.mnt.join("f")).0?;
            }
        }
        let a_after = cto_of(a)?;
        let acked = n(&a_after, "recalls_acked") - n(&a_before, "recalls_acked");
        let sent = n(&a_after, "recalls_sent") - n(&a_before, "recalls_sent");
        eprintln!(
            "    {NAME}: W's close latency without a delegation outstanding {}",
            dist(no_deleg)
        );
        eprintln!(
            "    {NAME}: W's close latency with R's delegation outstanding {} \
             ({sent} recalls sent, {acked} acked, for 10 writes)",
            dist(with_deleg)
        );
        print_cto(NAME, "A", &a_after);
        print_cto(NAME, "R", &cto_of(r)?);
        anyhow::ensure!(
            acked >= 5,
            "A recalled R's delegation {acked} times for 10 writes"
        );
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// An unreachable delegate: R holds a delegation on `f` and is frozen
/// (SIGSTOP). W's close of `f` cannot be acknowledged until A has waited
/// out the grant — TTL plus the lease's drift margin by A's clock — and
/// then returns; A's own write of `f` waits the same way. R, thawed,
/// reads the new content: its delegation expired by its own clock
/// (earlier than A assumed, by the margin on each side).
pub fn cto_recall_unreachable(_seed: u64) -> Result<()> {
    const NAME: &str = "cto-recall-unreachable";
    const TTL_MS: u64 = 3_000;
    let ttl = TTL_MS.to_string();
    let (_env, _root, mut clients) = cluster(
        NAME,
        &["a", "w", "r"],
        "strict",
        &[("CONSTELLATION_READ_DELEGATION_TTL_MS", &ttl)],
    )?;
    let result = (|| -> Result<()> {
        let (a, w, r) = (&clients[0], &clients[1], &clients[2]);
        // The lease's margin (`expiry_margin_ms`, 1 s at the default TTL).
        let margin = Duration::from_millis(1_000);
        for (round, writer) in [(0, w), (1, a)] {
            let content = format!("while-frozen-{round}");
            // Let any delegation R still holds lapse, so its next open
            // asks for (and is granted) a fresh one we can time.
            std::thread::sleep(Duration::from_millis(TTL_MS + 200));
            let rc0 = cto_of(r)?;
            read_timed(&r.mnt.join("f")).0.context("R reading f")?;
            let granted = Instant::now();
            let rc = cto_of(r)?;
            anyhow::ensure!(
                n(&rc, "read_index") > n(&rc0, "read_index") && n(&rc, "delegations_held") >= 1,
                "R's open of f did not get a fresh delegation: {rc}"
            );
            let a_before = cto_of(a)?;
            r.pause()?;
            let wrote = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                write_timed(&writer.mnt.join("f"), content.as_bytes())
            }));
            let since_grant = granted.elapsed();
            r.resume()?;
            let took = match wrote {
                Ok(r) => r?,
                Err(_) => bail!("the write panicked"),
            };
            let a_after = cto_of(a)?;
            let expired = n(&a_after, "recalls_expired") - n(&a_before, "recalls_expired");
            eprintln!(
                "    {NAME}: {}'s close with R frozen took {took:?} (grant {since_grant:?} ago \
                 when it returned; TTL {TTL_MS} ms + margin {margin:?}); recalls outwaited {expired}",
                writer.name
            );
            anyhow::ensure!(expired >= 1, "A did not outwait R's grant: {a_after}");
            // It returned only after the grant expired by A's clock:
            // granted + TTL + margin, measured here from after R's open
            // returned (so a little less).
            anyhow::ensure!(
                since_grant >= Duration::from_millis(TTL_MS) + margin - Duration::from_millis(300),
                "{}'s close returned {since_grant:?} after R's grant: before TTL + margin",
                writer.name
            );
            anyhow::ensure!(
                took <= Duration::from_millis(TTL_MS) + margin + Duration::from_secs(3),
                "{}'s close took {took:?}: far past TTL + margin",
                writer.name
            );
            let (got, _) = read_timed(&r.mnt.join("f"));
            let got = got.context("R reading f after thawing")?;
            anyhow::ensure!(
                got == content.as_bytes(),
                "R read {:?} after thawing, expected {content:?}",
                String::from_utf8_lossy(&got)
            );
        }
        print_cto(NAME, "A", &cto_of(a)?);
        print_cto(NAME, "R", &cto_of(r)?);
        Ok(())
    })();
    let _ = clients[2].resume();
    unmount_all(&mut clients);
    result
}

/// Latency: a single node pays nothing for strict (it is the sequencer:
/// no ReadIndex, no wait), measured against bounded on the same workload;
/// and a LAN non-sequencer's first strict open (one round trip) against
/// its delegated opens (none), for files nobody else writes.
pub fn cto_latency(_seed: u64) -> Result<()> {
    const NAME: &str = "cto-latency";
    const FILES: usize = 50;
    const ROUNDS: usize = 5;
    let mut single = Vec::new();
    for mode in ["bounded", "strict"] {
        let (_env, _root, mut clients) =
            cluster(&format!("{NAME}-1-{mode}"), &["solo"], mode, &[])?;
        let result = (|| -> Result<(Vec<Duration>, serde_json::Value)> {
            let c = &clients[0];
            for i in 0..FILES {
                std::fs::write(c.mnt.join(format!("s{i}")), format!("s{i}"))?;
            }
            let mut lat = Vec::new();
            for _ in 0..ROUNDS {
                for i in 0..FILES {
                    let (got, l) = read_timed(&c.mnt.join(format!("s{i}")));
                    got?;
                    lat.push(l);
                }
            }
            Ok((lat, cto_of(c)?))
        })();
        unmount_all(&mut clients);
        let (lat, cto) = result?;
        eprintln!(
            "    {NAME}: single node, {mode}: open+read {}",
            dist(lat.clone())
        );
        print_cto(NAME, &format!("solo ({mode})"), &cto);
        if mode == "strict" {
            anyhow::ensure!(
                n(&cto, "read_index") == 0 && n(&cto, "degraded") == 0,
                "a single strict node asked a sequencer: {cto}"
            );
        }
        single.push((mode, lat));
    }
    // Two nodes on the LAN: R opens files A wrote once (nobody writes
    // them again): the first open of each file is one ReadIndex round
    // trip; afterwards the directory's and the file's delegations answer
    // locally.
    let (_env, _root, mut clients) = cluster(&format!("{NAME}-lan"), &["a", "r"], "strict", &[])?;
    let result = (|| -> Result<()> {
        let (a, r) = (&clients[0], &clients[1]);
        for i in 0..FILES {
            std::fs::write(a.mnt.join(format!("s{i}")), format!("s{i}"))?;
        }
        eventually("A's files visible on R", Duration::from_secs(20), || {
            anyhow::ensure!(r.mnt.join(format!("s{}", FILES - 1)).is_file(), "not yet");
            Ok(())
        })?;
        let before = cto_of(r)?;
        let mut first = Vec::new();
        for i in 0..FILES {
            let (got, l) = read_timed(&r.mnt.join(format!("s{i}")));
            got?;
            first.push(l);
        }
        let mid = cto_of(r)?;
        let mut again = Vec::new();
        for _ in 0..ROUNDS {
            for i in 0..FILES {
                let (got, l) = read_timed(&r.mnt.join(format!("s{i}")));
                got?;
                again.push(l);
            }
        }
        let after = cto_of(r)?;
        eprintln!(
            "    {NAME}: LAN non-sequencer, first strict open+read of each file {} \
             ({} ReadIndex round trips for {FILES} files)",
            dist(first),
            n(&mid, "read_index") - n(&before, "read_index")
        );
        eprintln!(
            "    {NAME}: LAN non-sequencer, repeat opens under delegations {} \
             ({} ReadIndex round trips for {} opens)",
            dist(again),
            n(&after, "read_index") - n(&mid, "read_index"),
            FILES * ROUNDS
        );
        print_cto(NAME, "R", &after);
        print_cto(NAME, "A", &cto_of(a)?);
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// A lone strict sequencer keeps a kernel cache (strict costs a single
/// node nothing); a second node that joins and writes must still be seen
/// by the first node's very next open: the first sign of the newcomer
/// turns the cache off and the newcomer's first acknowledgement waits for
/// the entries cached until then to expire.
pub fn cto_second_node_joins(_seed: u64) -> Result<()> {
    const NAME: &str = "cto-second-node-joins";
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mk = |who: &str| -> Result<Client> {
        Ok(
            Client::new(root.path(), who, &env.direct_endpoint, &backend)?
                .with_own_node_key()
                .with_env("CONSTELLATION_CTO", "strict"),
        )
    };
    let mut a = mk("a")?;
    let mut b = mk("b")?;
    a.fs_create()?;
    a.mount()?;
    let result = (|| -> Result<()> {
        std::fs::write(a.mnt.join("f"), b"alone")?;
        // Warm A's kernel cache on `f` (lookup + attributes).
        for _ in 0..20 {
            anyhow::ensure!(std::fs::read(a.mnt.join("f"))? == b"alone");
        }
        b.mount()?;
        // B writes as soon as it can (its first op forwards to A), and A
        // opens right after B's close returns.
        let took = write_timed(&b.mnt.join("f"), b"from-b")?;
        let got = std::fs::read(a.mnt.join("f"))?;
        eprintln!(
            "    {NAME}: B's first close took {took:?}; A then read {:?}",
            String::from_utf8_lossy(&got)
        );
        anyhow::ensure!(
            got == b"from-b",
            "A read {:?} right after B's close: its kernel cache outlived the latch",
            String::from_utf8_lossy(&got)
        );
        print_cto(NAME, "A", &cto_of(&a)?);
        Ok(())
    })();
    let _ = b.unmount();
    let _ = a.unmount();
    result
}
