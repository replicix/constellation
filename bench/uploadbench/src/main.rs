//! Benchmark and controller shoot-out for adaptive chunk-upload
//! concurrency.
//!
//! `sim` runs against a deterministic synthetic network + S3 model, so
//! controllers can be compared repeatably and fault injection
//! (transient SlowDown storms) lands at an exact, known time. `live`
//! runs the same controllers against a real bucket.
//!
//! Prefer the `Makefile` targets, which wire up `BUCKET` from the
//! gitignored `local.mk` (see `local.mk.example`) so no bucket name or
//! account ID ever needs to appear in code:
//!
//! ```text
//! make uploadbench-sim    # deterministic comparison, no AWS needed
//! make uploadbench-live   # against the real bucket from local.mk
//! ```
//!
//! Equivalent by hand:
//!
//! ```text
//! cargo run -p uploadbench --release -- sim --controllers fixed,aimd,pid \
//!     --duration-secs 90 --fault-start-secs 30 --fault-duration-secs 15 --fault-error-rate 0.7 \
//!     --csv /tmp/uploadbench-sim.csv
//!
//! # needs AWS credentials in the environment, e.g.
//! # eval "$(aws configure export-credentials --format env)"
//! BUCKET=s3://your-bucket/your-prefix \
//!     cargo run -p uploadbench --release -- live --controllers aimd,pid \
//!     --duration-secs 60
//! ```

mod controller;
mod live;
mod report;
mod run;
mod sim;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

use controller::{AimdController, Controller, FixedController, PidController};

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Deterministic synthetic network + S3 model.
    Sim(SimArgs),
    /// Real S3 bucket.
    Live(LiveArgs),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
enum ControllerKind {
    Fixed,
    Aimd,
    Pid,
}

impl ControllerKind {
    fn build(self, opts: &ControllerOpts) -> Box<dyn Controller> {
        match self {
            ControllerKind::Fixed => Box::new(FixedController::new(opts.initial_concurrency)),
            ControllerKind::Aimd => Box::new(AimdController::new(
                opts.initial_concurrency,
                opts.min_concurrency,
                opts.max_concurrency,
            )),
            ControllerKind::Pid => Box::new(PidController::new(
                opts.initial_concurrency,
                opts.min_concurrency,
                opts.max_concurrency,
                opts.pid_target_ratio,
                opts.pid_kp,
                opts.pid_ki,
                opts.pid_kd,
                opts.pid_step_limit,
            )),
        }
    }
}

#[derive(clap::Args, Clone)]
struct ControllerOpts {
    /// Controllers to run and compare, in one process, back to back.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "fixed,aimd,pid"
    )]
    controllers: Vec<ControllerKind>,

    #[arg(long, default_value_t = 4)]
    initial_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    min_concurrency: usize,
    #[arg(long, default_value_t = 128)]
    max_concurrency: usize,

    /// Vegas-style tolerated latency inflation over baseline before the
    /// PID controller starts backing off.
    #[arg(long, default_value_t = 1.3)]
    pid_target_ratio: f64,
    #[arg(long, default_value_t = 6.0)]
    pid_kp: f64,
    #[arg(long, default_value_t = 0.8)]
    pid_ki: f64,
    #[arg(long, default_value_t = 0.15)]
    pid_kd: f64,
    #[arg(long, default_value_t = 12.0)]
    pid_step_limit: f64,
}

#[derive(clap::Args)]
struct CommonRunOpts {
    #[command(flatten)]
    controller_opts: ControllerOpts,

    /// How long each controller's run lasts.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// Size of each uploaded object.
    #[arg(long, default_value_t = 4 << 20)]
    object_size: u64,

    #[arg(long, default_value_t = 250)]
    sample_interval_ms: u64,

    /// Write the combined per-controller time series here for plotting.
    #[arg(long)]
    csv: Option<PathBuf>,
}

#[derive(Parser)]
struct SimArgs {
    #[command(flatten)]
    common: CommonRunOpts,

    #[arg(long, default_value_t = 150)]
    base_rtt_ms: u64,

    /// Aggregate simulated bandwidth, in bytes/sec, shared across all
    /// in-flight requests (default ~1 Gbps).
    #[arg(long, default_value_t = 125_000_000)]
    bandwidth_bytes_per_sec: u64,

    /// In-flight requests beyond which the simulated backend starts
    /// throttling and queueing.
    #[arg(long, default_value_t = 24)]
    capacity: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Seconds into the run at which a SlowDown storm starts. Omit to
    /// disable fault injection.
    #[arg(long)]
    fault_start_secs: Option<u64>,
    #[arg(long, default_value_t = 15)]
    fault_duration_secs: u64,
    #[arg(long, default_value_t = 0.7)]
    fault_error_rate: f64,
}

#[derive(Parser)]
struct LiveArgs {
    #[command(flatten)]
    common: CommonRunOpts,

    /// `s3://bucket/prefix`. Defaults to $BUCKET.
    #[arg(long, env = "BUCKET")]
    bucket: String,

    /// Retries `object_store` performs internally before surfacing a
    /// failure to the controller under test. Keep this low (0-1) so the
    /// controller — not object_store's own backoff — is what you are
    /// measuring.
    #[arg(long, default_value_t = 1)]
    max_retries: usize,

    /// Skip deleting uploaded objects afterwards (for inspection).
    #[arg(long)]
    keep_objects: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive(
                "uploadbench=info"
                    .parse()
                    .expect("static filter directive is valid"),
            ),
        )
        .init();

    match Cli::parse().command {
        Command::Sim(args) => run_sim(args).await,
        Command::Live(args) => run_live(args).await,
    }
}

async fn run_sim(args: SimArgs) -> Result<()> {
    let cfg = run::RunConfig {
        duration: Duration::from_secs(args.common.duration_secs),
        object_size: args.common.object_size,
        sample_interval: Duration::from_millis(args.common.sample_interval_ms),
    };
    let fault_start = args.fault_start_secs.map(Duration::from_secs);
    let fault_duration = Duration::from_secs(args.fault_duration_secs);

    let mut results = Vec::new();
    for kind in &args.common.controller_opts.controllers {
        let controller = kind.build(&args.common.controller_opts);
        tracing::info!(controller = ?kind, "starting sim run");
        let sim_cfg = sim::SimConfig {
            base_rtt: Duration::from_millis(args.base_rtt_ms),
            bandwidth_bytes_per_sec: args.bandwidth_bytes_per_sec,
            capacity: args.capacity,
            seed: args.seed,
            fault_start,
            fault_duration,
            fault_error_rate: args.fault_error_rate,
        };
        let target = run::Target::Sim(sim::SimTarget::new(sim_cfg));
        let result = run_boxed(controller_name(*kind), controller, target, &cfg).await;
        report_progress(&result);
        if let Some(fault_start) = fault_start {
            let fault_end = fault_start + fault_duration;
            match report::recovery_time_secs(
                &result,
                fault_start,
                fault_end,
                Duration::from_secs(10),
                0.8,
            ) {
                Some(secs) => tracing::info!(
                    controller = result.controller_name,
                    recovery_secs = secs,
                    "recovered to >=80% of pre-fault throughput"
                ),
                None => tracing::warn!(
                    controller = result.controller_name,
                    "did not recover to >=80% of pre-fault throughput within the run"
                ),
            }
        }
        results.push(result);
    }

    finish(&results, args.common.csv.as_deref())
}

async fn run_live(args: LiveArgs) -> Result<()> {
    let cfg = run::RunConfig {
        duration: Duration::from_secs(args.common.duration_secs),
        object_size: args.common.object_size,
        sample_interval: Duration::from_millis(args.common.sample_interval_ms),
    };

    let mut results = Vec::new();
    for kind in &args.common.controller_opts.controllers {
        let controller = kind.build(&args.common.controller_opts);
        let run_label = format!(
            "uploadbench-{}-{}",
            controller_name(*kind),
            chrono_like_timestamp()
        );
        tracing::info!(controller = ?kind, run_label, "starting live run");
        let s3 = live::S3Target::new(&args.bucket, &run_label, args.max_retries)
            .with_context(|| format!("opening {}", args.bucket))?;
        let s3 = std::sync::Arc::new(s3);
        let target = run::Target::Live(s3.clone());
        let result = run_boxed(controller_name(*kind), controller, target, &cfg).await;
        report_progress(&result);
        if args.keep_objects {
            tracing::info!("--keep-objects set: leaving uploaded objects in place");
        } else {
            match s3.cleanup().await {
                Ok(n) => tracing::info!(deleted = n, "cleaned up uploaded objects"),
                Err(error) => {
                    tracing::error!(%error, "cleanup failed; objects may remain under {run_label}")
                }
            }
        }
        results.push(result);
    }

    finish(&results, args.common.csv.as_deref())
}

async fn run_boxed(
    name: &'static str,
    controller: Box<dyn Controller>,
    target: run::Target,
    cfg: &run::RunConfig,
) -> run::RunResult {
    run::run(name, BoxedController(controller), target, cfg).await
}

/// [`Controller`] is object-safe but [`run::run`] is generic over `impl
/// Controller`; this thin wrapper lets a `Box<dyn Controller>` satisfy
/// that bound without duplicating the driver per concrete type.
struct BoxedController(Box<dyn Controller>);

impl Controller for BoxedController {
    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn current(&self) -> usize {
        self.0.current()
    }
    fn on_success(&mut self, now: std::time::Instant, bytes: u64, latency: Duration) -> usize {
        self.0.on_success(now, bytes, latency)
    }
    fn on_error(&mut self, now: std::time::Instant, latency: Duration, slowdown: bool) -> usize {
        self.0.on_error(now, latency, slowdown)
    }
}

fn controller_name(kind: ControllerKind) -> &'static str {
    match kind {
        ControllerKind::Fixed => "fixed",
        ControllerKind::Aimd => "aimd",
        ControllerKind::Pid => "pid",
    }
}

fn report_progress(result: &run::RunResult) {
    let s = report::summarize(result);
    tracing::info!(
        controller = s.controller_name,
        mean_mib_s = format!("{:.2}", s.mean_throughput_mib_s),
        peak_mib_s = format!("{:.2}", s.peak_throughput_mib_s),
        errors = s.total_errors,
        error_rate_pct = format!("{:.2}", s.error_rate * 100.0),
        p50_ms = format!("{:.1}", s.p50_latency_ms),
        p95_ms = format!("{:.1}", s.p95_latency_ms),
        mean_concurrency = format!("{:.1}", s.mean_concurrency),
        "run complete"
    );
}

fn finish(results: &[run::RunResult], csv: Option<&std::path::Path>) -> Result<()> {
    let summaries: Vec<_> = results.iter().map(report::summarize).collect();
    println!();
    report::print_summary_table(&summaries);
    if let Some(path) = csv {
        report::write_csv(path, results)?;
        println!("\nwrote time series to {}", path.display());
    }
    Ok(())
}

/// A dependency-free, sortable, filesystem-safe timestamp for run
/// labels — full RFC3339 precision is not needed, just uniqueness and
/// readability.
fn chrono_like_timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", now.as_millis())
}
