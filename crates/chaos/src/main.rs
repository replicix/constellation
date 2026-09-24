//! Chaos CLI: worker / run / check.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use constellation_chaos::check::check_file;
use constellation_chaos::cluster::{LocalCluster, TcpCluster};
use constellation_chaos::gen::{Profile, ScenarioSet};
use constellation_chaos::worker;
use constellation_chaos::Coordinator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "chaos",
    about = "Constellation multi-node FS consistency stress tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve ops against an already-mounted constellation filesystem.
    Worker {
        /// Address to listen on (e.g. 0.0.0.0:7400).
        #[arg(long)]
        listen: SocketAddr,
        /// Local constellation mount point.
        #[arg(long)]
        mount: PathBuf,
    },
    /// Coordinate a run against local mounts and/or remote workers.
    Run {
        /// Remote workers as host:port (repeat or comma-separated).
        #[arg(long, value_delimiter = ',')]
        workers: Vec<String>,
        /// Local mount paths (comma-separated) for --local / in-process cluster.
        #[arg(long, value_delimiter = ',')]
        mounts: Vec<PathBuf>,
        /// Profile name: ci | soak.
        #[arg(long, default_value = "ci")]
        profile: String,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Soak duration (e.g. 60s, 4h). Ignored for ci.
        #[arg(long)]
        duration: Option<String>,
        /// Scenario subset: namespace | data | cto | all.
        #[arg(long, default_value = "all")]
        scenario: String,
        /// Artifact directory.
        #[arg(long, default_value = "./chaos-store")]
        store: PathBuf,
    },
    /// Re-check a saved history.jsonl offline.
    Check {
        #[arg(long)]
        history: PathBuf,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Worker { listen, mount } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(worker::serve(listen, mount))
        }
        Command::Run {
            workers,
            mounts,
            profile,
            seed,
            duration,
            scenario,
            store,
        } => {
            let scenarios = ScenarioSet::parse(&scenario)?;
            std::fs::create_dir_all(&store)?;
            if !workers.is_empty() {
                let addrs = TcpCluster::parse_workers(&workers)?;
                let n = addrs.len();
                let mut cluster = TcpCluster::connect(&addrs)?;
                let mut prof = match profile.as_str() {
                    "ci" => Profile::ci(seed, n),
                    "soak" => {
                        let secs = parse_duration(duration.as_deref().unwrap_or("1h"))?;
                        Profile::soak(seed, n, secs)
                    }
                    other => bail!("unknown profile: {other} (ci|soak)"),
                };
                prof.scenarios = scenarios;
                Coordinator::run(&mut cluster, prof, &store)
            } else if !mounts.is_empty() {
                let n = mounts.len();
                let mut cluster = LocalCluster::new(mounts)?;
                let mut prof = match profile.as_str() {
                    "ci" => Profile::ci(seed, n),
                    "soak" => {
                        let secs = parse_duration(duration.as_deref().unwrap_or("60s"))?;
                        Profile::soak(seed, n, secs)
                    }
                    other => bail!("unknown profile: {other} (ci|soak)"),
                };
                prof.scenarios = scenarios;
                Coordinator::run(&mut cluster, prof, &store)
            } else {
                bail!("provide --workers host:port,... and/or --mounts /path1,/path2");
            }
        }
        Command::Check { history } => {
            check_file(&history).with_context(|| format!("check {}", history.display()))?;
            // Plan 30 §M6: the session checkers' coverage, so a run shows
            // how much they judged even while they only report.
            let loaded = constellation_chaos::History::load_jsonl(&history)?;
            let sessions = constellation_chaos::sessions::check_sessions(&loaded);
            println!(
                "sessions: {} observations judged, {} unexplained, {} violations ({})",
                sessions.judged,
                sessions.unexplained,
                sessions.violations.len(),
                if constellation_chaos::sessions::enforced() {
                    "enforced"
                } else {
                    "reported only"
                }
            );
            // Plan 30 §M8: close-to-open coverage (enforced for histories
            // recorded with `--cto strict` mounts).
            let cto = constellation_chaos::sessions::check_close_to_open(&loaded);
            println!(
                "close-to-open: {} observations judged, {} unexplained, {} violations ({})",
                cto.judged,
                cto.unexplained,
                cto.violations.len(),
                if constellation_chaos::sessions::close_to_open_enforced(&loaded) {
                    "enforced: cto=strict"
                } else {
                    "reported only: bounded"
                }
            );
            println!("ok: {}", history.display());
            Ok(())
        }
    }
}

fn parse_duration(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix('s') {
        return num.parse::<u64>().context("duration seconds");
    }
    if let Some(num) = s.strip_suffix('m') {
        return Ok(num.parse::<u64>().context("duration minutes")? * 60);
    }
    if let Some(num) = s.strip_suffix('h') {
        return Ok(num.parse::<u64>().context("duration hours")? * 3600);
    }
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(secs);
    }
    // Also accept humantime-ish "4h" already handled; try Duration parse via std
    let _ = Duration::from_secs(0);
    bail!("invalid duration {s:?} (use 60s, 30m, or 4h)")
}
