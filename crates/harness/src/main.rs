//! Constellation test harness: orchestrates S3 (floci) + toxiproxy in
//! docker, runs constellation clients on the host, injects faults, and
//! verifies reality against a filesystem model (Jepsen-style oracle).
//!
//!   harness list
//!   harness run [scenario ...] [--seed N]
//!
//! Requires: docker, fusermount3, a release `constellation` binary
//! (CONSTELLATION_BIN or target/release/constellation).

mod client;
mod docker;
mod model;
mod s3env;
mod scenarios;
mod toxiproxy;
mod workload;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use scenarios::SCENARIOS;

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
            Ok(())
        }
        Command::Run { names, seed } => run(names, seed),
    }
}

fn run(names: Vec<String>, seed: u64) -> Result<()> {
    let selected: Vec<&scenarios::Scenario> = if names.is_empty() {
        SCENARIOS.iter().collect()
    } else {
        let mut v = Vec::new();
        for n in &names {
            match SCENARIOS.iter().find(|s| s.name == n) {
                Some(s) => v.push(s),
                None => bail!("unknown scenario {n:?} (try `harness list`)"),
            }
        }
        v
    };

    let mut failures = Vec::new();
    for s in selected {
        let t0 = std::time::Instant::now();
        eprintln!("=== {} (seed {seed}) ===", s.name);
        match (s.run)(seed) {
            Ok(()) => eprintln!("=== {} PASSED in {:.1?}", s.name, t0.elapsed()),
            Err(e) => {
                eprintln!("=== {} FAILED in {:.1?}: {e:#}", s.name, t0.elapsed());
                failures.push(s.name);
            }
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} scenario(s) failed: {}",
            failures.len(),
            failures.join(", ")
        );
    }
    eprintln!("ALL SCENARIOS PASSED");
    Ok(())
}
