//! Constellation test harness: orchestrates S3 (floci) + toxiproxy in
//! docker, runs constellation clients on the host, injects faults, and
//! verifies reality against a filesystem model (Jepsen-style oracle).
//!
//!   harness list
//!   harness run [scenario ...] [--seed N] [--shard i/n] [--results-json PATH [--lane NAME]]
//!
//! Requires: docker, fusermount3, a release `constellation` binary
//! (CONSTELLATION_BIN or target/release/constellation).

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use constellation_harness::bench;
use constellation_harness::corpus;
use constellation_harness::metabench;
use constellation_harness::results::{self, Outcome, RunResults, Shard};
use constellation_harness::scenarios::{self, SCENARIOS};
use constellation_harness::snapchurn;
use constellation_harness::suites;

#[derive(Parser)]
#[command(name = "harness", about = "Constellation fault-injection test harness")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List available scenarios.
    List,
    /// Run scenarios (all when none are named).
    Run {
        /// Scenario names (see `list`).
        names: Vec<String>,
        /// Workload seed; failures reproduce with the same seed.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Replay a snapshot-churn JSONL audit trail.
        #[arg(long)]
        replay: Option<std::path::PathBuf>,
        /// Replay without preserving recorded inter-operation timing.
        #[arg(long, requires = "replay")]
        replay_no_sleep: bool,
        /// Run only shard `i` of `n` (1-based, e.g. `2/4`) of the selected
        /// scenarios: the scenario at position `idx` (after name filtering)
        /// belongs to shard `idx % n + 1`.
        #[arg(long, value_name = "i/n")]
        shard: Option<String>,
        /// Write a machine-readable result file (see `results` module docs)
        /// after the run, also when scenarios failed.
        #[arg(long, value_name = "PATH")]
        results_json: Option<std::path::PathBuf>,
        /// Lane name recorded in the results file (`<os>-<frontend>`).
        #[arg(long, default_value = results::DEFAULT_LANE)]
        lane: String,
    },
    /// Census-scale import benchmark (many small files).
    Bench {
        /// Number of files to import.
        #[arg(long, default_value_t = 20_000)]
        files: u64,
        /// Size of each file in bytes.
        #[arg(long, default_value_t = 4096)]
        file_size: u64,
        /// Number of directories the files spread across.
        #[arg(long, default_value_t = 100)]
        fanout: u64,
        /// Fail when durable import exceeds this many seconds.
        #[arg(long)]
        budget_s: Option<u64>,
        /// Benchmark an E2E passphrase filesystem.
        #[arg(long)]
        e2e: bool,
        /// Deterministic seed for synthetic dataset generation.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Replay the bundled anonymized corpus manifest.
        #[arg(long)]
        corpus_shape: bool,
        /// Explicit corpus manifest (zstd JSONL of hashed paths + sizes).
        #[arg(long)]
        corpus_manifest: Option<std::path::PathBuf>,
        /// Replay at most this many files from the manifest.
        #[arg(long)]
        corpus_limit: Option<u64>,
        /// Cap each staged file's payload (directory shape is unchanged).
        #[arg(long)]
        max_file_bytes: Option<u64>,
        /// Add S3 latency via toxiproxy (applies both directions).
        #[arg(long)]
        s3_latency_ms: Option<u64>,
        /// Limit S3 link throughput via toxiproxy (megabits/s).
        #[arg(long)]
        s3_bandwidth_mbps: Option<u64>,
        /// Label embedded into the JSON report.
        #[arg(long)]
        label: Option<String>,
        /// Emit the measured rates as a JSON object on stdout.
        #[arg(long)]
        json: bool,
    },
    /// Plan 29 M4: single- and multi-node metadata-op throughput/latency
    /// matrix (creates lease-serialization vs forwarding vs S3 CAS
    /// measurements). Prints one JSON report per configuration.
    MetaBench {
        /// Emit each report as a JSON object on stdout (one per line),
        /// in addition to the human-readable summary on stderr.
        #[arg(long)]
        json: bool,
    },
    /// Test helper for `stale-daemon-lock`: hold `daemon.lock` and a
    /// `control.sock` listener in STATE_DIR that accepts connections and
    /// never answers, until killed (a stand-in for a daemon the kernel has
    /// killed whose last thread is stuck in the kernel).
    #[command(hide = true)]
    MuteDaemon { state_dir: std::path::PathBuf },
    /// Snapshot a local directory into an anonymized corpus manifest.
    CorpusSnapshot {
        /// Directory to walk (`.git` / `.hg` / `.svn` skipped).
        #[arg(long)]
        src: std::path::PathBuf,
        /// Output path (`.jsonl.zst`).
        #[arg(long)]
        out: std::path::PathBuf,
        /// Keyed-hash seed baked into path tokens.
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match Cli::parse().command {
        Command::List => {
            for s in SCENARIOS {
                println!("{:22} {}", s.name, s.desc);
            }
            println!();
            println!("known-bug reproductions (expected to FAIL until fixed):");
            for s in scenarios::KNOWN_BUG_REPROS {
                println!("{:22} {}", s.name, s.desc);
            }
            Ok(())
        }
        Command::Run {
            names,
            seed,
            replay,
            replay_no_sleep,
            shard,
            results_json,
            lane,
        } => run(RunOpts {
            names,
            seed,
            replay,
            replay_no_sleep,
            shard,
            results_json,
            lane,
        }),
        Command::Bench {
            files,
            file_size,
            fanout,
            budget_s,
            e2e,
            seed,
            corpus_shape,
            corpus_manifest,
            corpus_limit,
            max_file_bytes,
            s3_latency_ms,
            s3_bandwidth_mbps,
            label,
            json,
        } => {
            let cfg = bench::BenchConfig {
                files,
                file_size,
                fanout,
                budget_s,
                e2e,
                seed,
                corpus_shape,
                corpus_manifest,
                corpus_limit,
                max_file_bytes,
                s3_latency_ms,
                s3_bandwidth_mbps,
                label,
                json,
            };
            cfg.validate()?;
            let report = bench::run(&cfg)?;
            if cfg.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            Ok(())
        }
        Command::MetaBench { json } => {
            let (reports, raw) = metabench::run_matrix()?;
            if json {
                for r in &reports {
                    println!("{}", serde_json::to_string(r)?);
                }
                for r in &raw {
                    println!("{}", serde_json::to_string(r)?);
                }
            }
            eprintln!("\n=== metabench summary ===");
            for r in &reports {
                eprintln!(
                    "{:28} nodes={} p2p={:<5} layout={:<14} agg={:>8.0} ops/s p50={:>7.2}ms p99={:>7.2}ms fwd_ok={:>5} fwd_p50={:>5?} handoffs={:>3} errors={}",
                    r.label,
                    r.nodes,
                    r.p2p,
                    r.layout,
                    r.aggregate_ops_per_sec,
                    r.p50_ms,
                    r.p99_ms,
                    r.forwarded_ok_total,
                    r.forward_p50_ms,
                    r.handoffs,
                    r.errors,
                );
            }
            for r in &raw {
                eprintln!(
                    "raw S3 @ {}ms latency: PUT p50={:.2}ms CAS-create p50={:.2}ms GET p50={:.2}ms",
                    r.latency_ms, r.put_p50_ms, r.cas_create_p50_ms, r.get_p50_ms
                );
            }
            Ok(())
        }
        Command::CorpusSnapshot { src, out, seed } => corpus::snapshot_cmd(src, out, seed),
        Command::MuteDaemon { state_dir } => mute_daemon(&state_dir),
    }
}

struct RunOpts {
    names: Vec<String>,
    seed: u64,
    replay: Option<std::path::PathBuf>,
    replay_no_sleep: bool,
    shard: Option<String>,
    results_json: Option<std::path::PathBuf>,
    lane: String,
}

fn run(opts: RunOpts) -> Result<()> {
    let RunOpts {
        names,
        seed,
        replay,
        replay_no_sleep,
        shard,
        results_json,
        lane,
    } = opts;
    let shard = shard.as_deref().map(Shard::parse).transpose()?;
    if replay.is_some() && names.as_slice() != ["snapshot-churn"] {
        bail!("--replay is valid only with exactly one scenario: snapshot-churn");
    }
    snapchurn::set_replay(replay, replay_no_sleep)?;
    let selected: Vec<&scenarios::Scenario> = if names.is_empty() {
        SCENARIOS.iter().collect()
    } else {
        let mut v = Vec::new();
        for n in &names {
            match SCENARIOS
                .iter()
                .chain(scenarios::KNOWN_BUG_REPROS.iter())
                .find(|s| s.name == n)
            {
                Some(s) => v.push(s),
                None => bail!("unknown scenario {n:?} (try `harness list`)"),
            }
        }
        v
    };
    let selected = match shard {
        Some(sh) => {
            let total = selected.len();
            let part = sh.partition(selected);
            eprintln!("=== shard {sh}: {} of {total} scenario(s)", part.len());
            part
        }
        None => selected,
    };

    let mut report = RunResults::new(&lane, seed, shard);
    let mut failures = Vec::new();
    let mut skipped = Vec::new();
    for s in selected {
        if let Some(missing) = s.requires.iter().find(|b| !suites::have(b)) {
            eprintln!("=== {} SKIPPED ({missing} not installed)", s.name);
            report.push(
                s.name,
                Outcome::Skipped,
                0.0,
                Some(format!("{missing} not installed")),
            );
            skipped.push(s.name);
            continue;
        }
        let t0 = std::time::Instant::now();
        eprintln!("=== {} (seed {seed}) ===", s.name);
        match (s.run)(seed) {
            Ok(()) => {
                eprintln!("=== {} PASSED in {:.1?}", s.name, t0.elapsed());
                report.push(s.name, Outcome::Passed, t0.elapsed().as_secs_f64(), None);
            }
            Err(e) => {
                eprintln!("=== {} FAILED in {:.1?}: {e:#}", s.name, t0.elapsed());
                report.push(
                    s.name,
                    Outcome::Failed,
                    t0.elapsed().as_secs_f64(),
                    Some(format!("{e:#}")),
                );
                failures.push(s.name);
            }
        }
    }
    if let Some(path) = &results_json {
        report.write(path)?;
        eprintln!("results written to {}", path.display());
    }
    if !failures.is_empty() {
        bail!(
            "{} scenario(s) failed: {}",
            failures.len(),
            failures.join(", ")
        );
    }
    if skipped.is_empty() {
        eprintln!("ALL SCENARIOS PASSED");
    } else {
        eprintln!(
            "ALL RUN SCENARIOS PASSED ({} skipped: {})",
            skipped.len(),
            skipped.join(", ")
        );
    }
    Ok(())
}

/// See `Command::MuteDaemon`.
fn mute_daemon(state_dir: &std::path::Path) -> Result<()> {
    use std::os::fd::AsRawFd;
    std::fs::create_dir_all(state_dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(state_dir.join("daemon.lock"))?;
    // SAFETY: `lock` owns a valid open fd for the duration of the call.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        bail!(
            "daemon.lock is already held: {}",
            std::io::Error::last_os_error()
        );
    }
    let sock = state_dir.join("control.sock");
    let _ = std::fs::remove_file(&sock);
    let listener = std::os::unix::net::UnixListener::bind(&sock)?;
    std::fs::write(state_dir.join("daemon.pid"), std::process::id().to_string())?;
    eprintln!(
        "mute daemon {} holding {}",
        std::process::id(),
        state_dir.display()
    );
    // Keep every accepted connection open (a closed one would give the
    // client EOF, which is an answer of sorts) and never read or write.
    let mut held = Vec::new();
    loop {
        match listener.accept() {
            Ok((stream, _)) => held.push(stream),
            Err(e) => bail!("accept: {e}"),
        }
    }
}
