//! Cooperative-cache membership under churn (plan 30 §M15).
//!
//! Three nodes with small caches (24 one-MiB chunks each) run seeded
//! rounds: every node writes a fresh 4-chunk file, then reads two recent
//! files written by others while S3 is 100 ms away (so peers are the
//! preferred source). Each round adds ~12 chunks per node, so from the
//! second round on every node is evicting while its peers look it up:
//! chunks are added and evicted on several nodes at once.
//!
//! A holder answers a chunk it lacks with `Absent`, unless it dropped it
//! within its recent-removal window (`RecentlyRemoved`: a propagation
//! race). `peer_false_positives` counts only `Absent`, i.e. fetches a
//! digest sent to a peer that never had the chunk or dropped it long ago.
//!
//! * `coop-exact-churn` asserts zero false-positive peer fetches in exact
//!   mode, real peer hits, forced eviction, and — after quiescence —
//!   that every node's mirrors hold exactly its peers' published sets.
//! * `coop-digest-compare` runs the same workload in `bloom` and `exact`
//!   mode and prints digest bytes/s, false positives and CPU per round
//!   for both. It asserts only the exact-mode invariants; the numbers are
//!   the measurement that decides whether the bloom code stays.

use super::{coop_client, coop_of, eventually, setup, ts, wait_for_peers};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::{Duration, Instant};

const NODES: usize = 3;
const ROUNDS: usize = 8;
const CHUNK: usize = 1 << 20;
const CHUNKS_PER_FILE: usize = 4;
const CACHE_CHUNKS: u64 = 24;
const READS_PER_ROUND: usize = 2;

#[derive(Debug, Default, Clone)]
pub(super) struct ChurnStats {
    mode: String,
    elapsed: Duration,
    peer_hits: u64,
    peer_misses: u64,
    false_positives: u64,
    stale_misses: u64,
    s3_fetches: u64,
    digest_bytes: u64,
    digest_messages: u64,
    digest_cpu_us: u64,
    reconcile_rounds: u64,
    reconcile_cpu_us: u64,
    reconcile_failures: u64,
    peer_set_bytes: u64,
}

impl ChurnStats {
    fn print(&self) {
        let secs = self.elapsed.as_secs_f64().max(0.001);
        let per_round = if self.reconcile_rounds == 0 {
            0.0
        } else {
            self.reconcile_cpu_us as f64 / self.reconcile_rounds as f64
        };
        eprintln!(
            "    coop-churn[{}]: peer_hits={} peer_misses={} false_positives={} stale_misses={} \
             s3_fetches={} digest_bytes={} ({:.0} B/s fleet) msgs={} digest_cpu_us={} \
             reconcile_rounds={} ({:.1} us/round, failures={}) peer_set_bytes={} in {:.1?}",
            self.mode,
            self.peer_hits,
            self.peer_misses,
            self.false_positives,
            self.stale_misses,
            self.s3_fetches,
            self.digest_bytes,
            self.digest_bytes as f64 / secs,
            self.digest_messages,
            self.digest_cpu_us,
            self.reconcile_rounds,
            per_round,
            self.reconcile_failures,
            self.peer_set_bytes,
            self.elapsed,
        );
    }
}

/// Deterministic, per-chunk-distinct content: no two chunks of the run
/// dedup, so every chunk is its own cache entry.
fn file_bytes(seed: u64, round: usize, node: usize) -> Vec<u8> {
    let mut out = vec![0u8; CHUNKS_PER_FILE * CHUNK];
    let mut x = seed ^ ((round as u64) << 32) ^ ((node as u64) << 48) ^ 0x5eed;
    for word in out.chunks_mut(8) {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        word.copy_from_slice(&z.to_le_bytes()[..word.len()]);
    }
    out
}

fn u(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn drained(c: &Client) -> Result<()> {
    let s = c.control_status()?;
    anyhow::ensure!(
        s["writeback"]["pending_uploads"].as_u64() == Some(0)
            && s["spool"]["journal_backlog"].as_u64() == Some(0),
        "{} still uploading/shipping",
        c.name
    );
    Ok(())
}

/// Run the churn workload in `mode` and return fleet-wide counters.
pub(super) fn run_churn(seed: u64, mode: &str, label: &str) -> Result<ChurnStats> {
    let (env, root) = setup(label)?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/{label}-{mode}-{}", ts());
    let mut nodes: Vec<Client> = (0..NODES)
        .map(|i| {
            coop_client(
                root.path(),
                &format!("churn-{mode}-{i}"),
                &env.endpoint,
                &backend,
            )
            .map(|c| {
                c.with_env("CONSTELLATION_COOP_DIGEST", mode)
                    .with_env("CONSTELLATION_STAGING_BUDGET", &(8 * CHUNK).to_string())
                    .with_cache_size(CACHE_CHUNKS * CHUNK as u64)
            })
        })
        .collect::<Result<_>>()?;
    nodes[0].fs_create()?;
    for n in nodes.iter_mut() {
        n.mount()?;
    }
    let result = churn_rounds(seed, mode, &nodes, &proxy);
    let _ = proxy.heal();
    for n in nodes.iter_mut() {
        let _ = n.unmount();
    }
    result
}

fn churn_rounds(
    seed: u64,
    mode: &str,
    nodes: &[Client],
    proxy: &crate::toxiproxy::Proxy<'_>,
) -> Result<ChurnStats> {
    let refs: Vec<&Client> = nodes.iter().collect();
    wait_for_peers(&refs)?;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut written: Vec<(String, usize, blake3::Hash)> = Vec::new();
    let started = Instant::now();
    for round in 0..ROUNDS {
        for (i, n) in nodes.iter().enumerate() {
            let name = format!("r{round}-n{i}");
            let data = file_bytes(seed, round, i);
            std::fs::write(n.mnt.join(&name), &data)?;
            written.push((name, i, blake3::hash(&data)));
        }
        for n in nodes {
            eventually(
                &format!("{} drains round {round}", n.name),
                Duration::from_secs(30),
                || drained(n),
            )?;
        }
        for n in nodes {
            eventually(
                &format!("round {round} visible on {}", n.name),
                Duration::from_secs(30),
                || {
                    for (name, _, _) in written.iter().rev().take(NODES) {
                        anyhow::ensure!(n.mnt.join(name).is_file(), "{name} not on {}", n.name);
                    }
                    Ok(())
                },
            )?;
        }
        // Peers must beat S3 so reads really consult the digests.
        proxy.latency(100, 0)?;
        for (i, n) in nodes.iter().enumerate() {
            // Recent files by other nodes: most are still cached on their
            // writer or an earlier reader, some were just evicted.
            let candidates: Vec<&(String, usize, blake3::Hash)> = written
                .iter()
                .rev()
                .take(NODES * 3)
                .filter(|(_, writer, _)| *writer != i)
                .collect();
            for _ in 0..READS_PER_ROUND {
                let (name, _, want) = candidates[rng.random_range(0..candidates.len())];
                let got = std::fs::read(n.mnt.join(name))?;
                anyhow::ensure!(
                    blake3::hash(&got) == *want,
                    "{} read a corrupt {name}",
                    n.name
                );
            }
        }
        proxy.heal()?;
    }
    let elapsed = started.elapsed();

    // Every node touched far more chunks than its cache holds, and the
    // cache stayed within its byte budget: eviction really happened
    // everywhere. Entry COUNT is not itself bounded by CACHE_CHUNKS: the
    // disk cache is shared with small mtree metadata nodes (both data
    // chunks and metadata nodes are content-addressed and cooperatively
    // served from the same store), so more than CACHE_CHUNKS entries can
    // coexist inside the same byte budget once metadata churns too.
    for n in nodes {
        let s = n.control_status()?;
        let used = u(&s["cache"], "used_bytes");
        let budget = u(&s["cache"], "budget_bytes");
        anyhow::ensure!(
            used <= budget,
            "{} cache used {used} bytes, budget {budget}",
            n.name
        );
    }
    let touched = (ROUNDS * CHUNKS_PER_FILE * (1 + READS_PER_ROUND)) as u64;
    anyhow::ensure!(
        touched > 2 * CACHE_CHUNKS,
        "workload too small to force eviction"
    );

    if mode == "exact" {
        // Quiescent: every mirror must equal its owner's published set.
        eventually(
            "exact mirrors converge to the peers' published sets",
            Duration::from_secs(20),
            || {
                let coops: Vec<serde_json::Value> =
                    nodes.iter().map(coop_of).collect::<Result<_>>()?;
                let total: u64 = coops.iter().map(|c| u(c, "local_set_entries")).sum();
                for (n, c) in nodes.iter().zip(&coops) {
                    let want = total - u(c, "local_set_entries");
                    let have = u(c, "peer_set_entries");
                    anyhow::ensure!(
                        have == want,
                        "{} mirrors {have} keys, peers publish {want}",
                        n.name
                    );
                }
                Ok(())
            },
        )?;
    }

    let mut stats = ChurnStats {
        mode: mode.to_string(),
        elapsed,
        ..ChurnStats::default()
    };
    for n in nodes {
        let c = coop_of(n)?;
        anyhow::ensure!(
            c["digest_mode"].as_str() == Some(mode),
            "{} runs digest mode {} not {mode}",
            n.name,
            c["digest_mode"]
        );
        stats.peer_hits += u(&c, "peer_hits");
        stats.peer_misses += u(&c, "peer_misses");
        stats.false_positives += u(&c, "peer_false_positives");
        stats.stale_misses += u(&c, "peer_stale_misses");
        stats.s3_fetches += u(&c, "s3_fetches");
        stats.digest_bytes += u(&c, "digest_bytes_sent");
        stats.digest_messages += u(&c, "digest_messages");
        stats.digest_cpu_us += u(&c, "digest_cpu_us");
        stats.reconcile_rounds += u(&c, "reconcile_rounds");
        stats.reconcile_cpu_us += u(&c, "reconcile_cpu_us");
        stats.reconcile_failures += u(&c, "reconcile_failures");
        stats.peer_set_bytes += u(&c, "peer_set_bytes");
    }
    Ok(stats)
}

fn check_exact(stats: &ChurnStats) -> Result<()> {
    anyhow::ensure!(
        stats.false_positives == 0,
        "exact mirrors produced {} false-positive peer fetches: {stats:?}",
        stats.false_positives
    );
    anyhow::ensure!(
        stats.peer_hits > 0,
        "no peer hits: the digests were never consulted: {stats:?}"
    );
    anyhow::ensure!(stats.digest_bytes > 0, "no digest traffic: {stats:?}");
    Ok(())
}

pub(super) fn coop_exact_churn(seed: u64) -> Result<()> {
    let stats = run_churn(seed, "exact", "coop-exact-churn")?;
    stats.print();
    check_exact(&stats)
}

pub(super) fn coop_digest_compare(seed: u64) -> Result<()> {
    let bloom = run_churn(seed, "bloom", "coop-digest-compare")?;
    let exact = run_churn(seed, "exact", "coop-digest-compare")?;
    bloom.print();
    exact.print();
    check_exact(&exact)?;
    anyhow::ensure!(
        bloom.peer_hits > 0,
        "bloom mode never hit a peer: {bloom:?}"
    );
    Ok(())
}
