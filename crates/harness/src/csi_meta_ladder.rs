//! Plan 37 K0 Track B: the `Controller.CreateVolume` metadata-op
//! throughput ladder (§5/§15/§2.3 of `docs/plans/v1/done/37-kubernetes-csi.md`).
//!
//! `CreateVolume` for a pool-layout volume runs `browse.mkdir` + six
//! `browse.xattr{set}` + one `quota.set` through the control protocol
//! (`fs.create` happens once for the whole pool, not per volume). This
//! module drives that exact op sequence through a real `constellation`
//! daemon's control socket — never through a FUSE mount, since
//! `EngineControl::browser()` opens its own internal view lazily and
//! every `browse.*`/`quota.*` handler needs nothing else — at rising
//! concurrency (`CONCURRENCY_LADDER`) and rising cumulative subtree count
//! (`CHECKPOINTS`) on one *unsharded* pool filesystem, to find the point
//! where the shared metadata commit chain's latency knee sits. A second,
//! trivial `node.ping` ladder on the same grid proves the control socket
//! itself (framing, dispatch, the blocking-pool handoff) is not what's
//! limiting throughput.
//!
//! `harness csi-meta-ladder` runs the whole grid and prints one JSON
//! report per (op kind, concurrency, checkpoint) data point.

use crate::client::Client;
use crate::reqlog;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{Context, Result};
use constellation_control::methods::{BrowseMkdir, BrowseXattr, NodePing, QuotaSet};
use constellation_control::proto::types::{MkdirParams, SetQuotaParams, XattrOp, XattrParams};
use constellation_control::proto::Empty;
use constellation_control::Client as ControlClient;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Client concurrency levels the grid sweeps. 8 and 32 are in the
/// committed ladder because the interesting transition is between 16 and
/// 64: a run of the plain `harness csi-meta-ladder` has to reproduce every
/// row of the published table without the diagnostic env knob below.
pub const CONCURRENCY_LADDER: &[usize] = &[1, 4, 8, 16, 32, 64, 256];

/// Cumulative subtree-count checkpoints the grid measures at, per
/// concurrency level (each concurrency level grows one pool filesystem
/// from 0 subtrees up through every checkpoint in order).
pub const CHECKPOINTS: &[u64] = &[100, 1_000, 5_000, 10_000];

/// `quota.set`'s `max_bytes` value used for every `CreateVolume` call
/// (10 GiB, a plausible PVC size), set on the volume's own subtree — §5's
/// `quota.set{subtree, bytes}`, the shape the CSI controller sends since
/// plan 37 K2 (K0 and K2a measured the filesystem-wide `{max_bytes}` RPC,
/// the only one that existed then).
const VOLUME_CAPACITY_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Checkpoints scaled down under `CONSTELLATION_CSI_LADDER_QUICK=1`, for
/// a fast smoke run of this driver itself.
fn checkpoints() -> Vec<u64> {
    if std::env::var_os("CONSTELLATION_CSI_LADDER_QUICK").is_some_and(|v| v != "0") {
        vec![5, 20]
    } else {
        CHECKPOINTS.to_vec()
    }
}

/// Concurrency ladder scaled down under the same quick-mode knob.
fn concurrency_ladder() -> Vec<usize> {
    // Diagnostic-only filter (not a documented knob, matching
    // `metabench`'s `CONSTELLATION_METABENCH_ONLY` precedent): rerun a
    // subset of the concurrency ladder without paying for the whole grid,
    // e.g. to confirm a result at one concurrency level is stable.
    if let Ok(only) = std::env::var("CONSTELLATION_CSI_LADDER_CONCURRENCY") {
        return only
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
    }
    if std::env::var_os("CONSTELLATION_CSI_LADDER_QUICK").is_some_and(|v| v != "0") {
        vec![1, 4]
    } else {
        CONCURRENCY_LADDER.to_vec()
    }
}

fn ts() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p * (sorted.len() as f64 - 1.0)).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn percentiles(mut ms: Vec<f64>) -> (f64, f64, f64) {
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (pct(&ms, 0.50), pct(&ms, 0.99), pct(&ms, 0.999))
}

/// This process's `utime+stime` in clock ticks, from `/proc/<pid>/stat`.
/// Parsed after the last `)` so a command name containing spaces or
/// parens (unlikely for `constellation`, defensive anyway) can't shift
/// the field count.
pub(crate) fn cpu_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rfind(')')?;
    let fields: Vec<&str> = stat[after_comm + 1..].split_whitespace().collect();
    // Numbering from `proc(5)`: field 1 is pid, 2 is comm (stripped above
    // along with field 3, state), so `fields[0]` is field 4 (ppid).
    // utime is field 14 (index 10 here), stime is field 15 (index 11).
    let utime: u64 = fields.get(10)?.parse().ok()?;
    let stime: u64 = fields.get(11)?.parse().ok()?;
    Some(utime + stime)
}

/// `/proc/loadavg`'s 1-minute figure. Recorded per step because every
/// number in this grid is a function of how much of the host the daemon
/// actually got: the same ladder on the same machine moves its failure
/// rate by an order of magnitude between a quiet host and a loaded one
/// (this box runs other agents' builds), so a step without its load
/// figure is not comparable with another run's step.
fn loadavg_1min() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/loadavg").ok()?;
    s.split_whitespace().next()?.parse().ok()
}

/// [`cpu_ticks`] with the failure said out loud: a missing `/proc` read
/// is recorded as `null`, never as 0% CPU.
fn cpu_ticks_logged(pid: u32) -> Option<u64> {
    let t = cpu_ticks(pid);
    if t.is_none() {
        eprintln!(
            "csi-meta-ladder: /proc/{pid}/stat unreadable; this step's daemon_cpu_pct is null"
        );
    }
    t
}

pub(crate) fn clk_tck() -> f64 {
    // SAFETY: `sysconf(_SC_CLK_TCK)` takes no pointers and never fails in
    // a way that matters here (POSIX guarantees it on Linux).
    let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if tck > 0 {
        tck as f64
    } else {
        100.0
    }
}

/// One (concurrency, checkpoint) data point of the `CreateVolume` grid.
/// `Default` exists so a test can name the three or four fields it cares
/// about; the driver always fills every field.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CreateVolumeStepReport {
    pub concurrency: usize,
    pub cumulative_before: u64,
    pub volumes: u64,
    pub cumulative_after: u64,
    pub errors: u64,
    pub error_samples: Vec<String>,
    /// How the `errors` split across the sequence's op kinds (`mkdir`,
    /// `xattr`, `quota`), so "the cliff is all `quota.set`" is a recorded
    /// number rather than an impression from the error samples.
    pub failures_by_op: BTreeMap<String, u64>,
    pub wall_s: f64,
    pub volumes_per_sec: f64,
    pub rpcs_per_sec: f64,
    pub mkdir_p50_ms: f64,
    pub mkdir_p99_ms: f64,
    pub mkdir_p999_ms: f64,
    pub xattr_p50_ms: f64,
    pub xattr_p99_ms: f64,
    pub xattr_p999_ms: f64,
    pub quota_p50_ms: f64,
    pub quota_p99_ms: f64,
    pub quota_p999_ms: f64,
    /// The whole 8-op `CreateVolume` sequence, start to finish —
    /// **successful sequences only**, so at a step with a high `errors`
    /// count these are survivorship-biased and must be read next to
    /// `failed_*` below, not on their own.
    pub sequence_p50_ms: f64,
    pub sequence_p99_ms: f64,
    pub sequence_p999_ms: f64,
    /// The failing op's own latency, over the sequences that failed: how
    /// expensive a failure is to a caller (K2 needs this to decide whether
    /// retrying `quota.set` is cheap).
    pub failed_op_p50_ms: f64,
    pub failed_op_p99_ms: f64,
    pub failed_op_p999_ms: f64,
    /// Sequence start → the failure, over the sequences that failed (the
    /// `sequence_*` percentiles' counterpart for the discarded half).
    pub failed_sequence_p50_ms: f64,
    pub failed_sequence_p99_ms: f64,
    pub failed_sequence_p999_ms: f64,
    /// Daemon's own `(utime+stime)` over this step's wall-clock window, as
    /// a percentage of one core. `null` if `/proc/<pid>/stat` could not be
    /// read at both ends of the window (never silently 0.0).
    pub daemon_cpu_pct: Option<f64>,
    pub s3_requests_total: u64,
    /// `reqlog::breakdown`: class+bucket-area counts, busiest first.
    pub s3_breakdown: String,
    /// `SpoolStatus::journal_backlog` (DESIGN.md §12), before/after: the
    /// commit chain's own observable backlog, as a proxy for its length.
    /// `null` (not 0) if the `node.status` call itself failed.
    pub journal_backlog_before: Option<u64>,
    pub journal_backlog_after: Option<u64>,
    pub ship_rounds_completed_delta: Option<u64>,
    /// `LeaseStatus::held`/`lost` from the same `node.status` calls. The
    /// failure message this grid collects says "no lease", so whether the
    /// lease was *actually* held throughout is recorded, not assumed.
    pub lease_held_before: Option<bool>,
    pub lease_held_after: Option<bool>,
    pub lease_lost_after: Option<bool>,
    /// `/proc/loadavg`'s 1-minute figure at each end of the step, and the
    /// host's CPU count: the comparability data for a rerun (see
    /// [`loadavg_1min`]).
    pub loadavg_1min_before: Option<f64>,
    pub loadavg_1min_after: Option<f64>,
    pub host_cpus: usize,
}

/// One (concurrency) data point of the `node.ping` control-plane-overhead
/// ladder: run at every checkpoint's cumulative call count so its total
/// op count matches the `CreateVolume` grid's total RPC count at the same
/// concurrency, for an apples-to-apples comparison.
#[derive(Debug, Clone, Serialize)]
pub struct PingStepReport {
    pub concurrency: usize,
    pub pings: u64,
    pub errors: u64,
    pub wall_s: f64,
    pub ops_per_sec: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

/// Which op of the `CreateVolume` sequence a latency sample (or a
/// failure) belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Mkdir,
    Xattr,
    Quota,
}

impl OpKind {
    fn as_str(self) -> &'static str {
        match self {
            OpKind::Mkdir => "mkdir",
            OpKind::Xattr => "xattr",
            OpKind::Quota => "quota",
        }
    }
}

/// One failed `CreateVolume` sequence: which op failed, how long that op
/// took before failing, how far into the sequence it was, and the
/// daemon's message.
#[derive(Debug, Clone)]
struct Failure {
    op: OpKind,
    message: String,
}

#[derive(Debug, Default)]
struct SeqLatencies {
    mkdir_ms: Vec<f64>,
    xattr_ms: Vec<f64>,
    quota_ms: Vec<f64>,
    /// Successful sequences only.
    sequence_ms: Vec<f64>,
    /// The failing op's own latency, over failed sequences.
    failed_op_ms: Vec<f64>,
    /// Sequence start → the failure, over failed sequences.
    failed_sequence_ms: Vec<f64>,
    failures_by_op: BTreeMap<String, u64>,
}

impl SeqLatencies {
    fn merge(&mut self, other: SeqLatencies) {
        self.mkdir_ms.extend(other.mkdir_ms);
        self.xattr_ms.extend(other.xattr_ms);
        self.quota_ms.extend(other.quota_ms);
        self.sequence_ms.extend(other.sequence_ms);
        self.failed_op_ms.extend(other.failed_op_ms);
        self.failed_sequence_ms.extend(other.failed_sequence_ms);
        for (op, n) in other.failures_by_op {
            *self.failures_by_op.entry(op).or_insert(0) += n;
        }
    }

    /// Record the one op that ended a sequence early. Keeps the ops that
    /// had already succeeded in their own series (dropping them would
    /// silently shrink every per-op sample set by the error rate) and
    /// times the failing op itself, which is the number K2 needs to know
    /// whether a retry is cheap.
    fn record_failure(
        &mut self,
        op: OpKind,
        op_ms: f64,
        seq_ms: f64,
        e: constellation_control::ControlError,
    ) -> Failure {
        self.failed_op_ms.push(op_ms);
        self.failed_sequence_ms.push(seq_ms);
        *self
            .failures_by_op
            .entry(op.as_str().to_string())
            .or_insert(0) += 1;
        Failure {
            op,
            message: e.message,
        }
    }
}

/// One `CreateVolume` sequence against subtree index `idx`: `browse.mkdir`,
/// six `browse.xattr{set}` calls (the exact keys §5 of the plan names:
/// `pv`, `pvc`, `namespace`, `capacity`, `source`, `created`), then
/// `quota.set`.
///
/// Returns the latencies collected *and* the failure, if any, rather than
/// short-circuiting with `?`: a sequence that dies on its trailing
/// `quota.set` has already produced seven good samples, and the failing
/// op's own latency is itself a result (see [`SeqLatencies::record_failure`]).
async fn create_volume_once(control: &ControlClient, idx: u64) -> (SeqLatencies, Option<Failure>) {
    let name = format!("pv-{idx:08}");
    let path = format!("/volumes/{name}");
    let t_seq = Instant::now();
    let mut lat = SeqLatencies::default();
    let ms = |t: Instant| t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    match control
        .call::<BrowseMkdir>(MkdirParams {
            path: path.clone(),
            mode: Some(0o755),
            parents: false,
        })
        .await
    {
        Ok(_) => lat.mkdir_ms.push(ms(t)),
        Err(e) => {
            let f = lat.record_failure(OpKind::Mkdir, ms(t), ms(t_seq), e);
            return (lat, Some(f));
        }
    }

    let xattrs: [(&str, String); 6] = [
        // `user.*`: the only namespace a non-root caller may set
        // (`XattrPolicy::check_name`, plan 31 C4) — bare names (what §5's
        // shorthand `{pv,pvc,namespace,capacity,source,created}` literally
        // reads) are `NotSupported`; a real CSI driver would namespace
        // them the same way.
        ("user.pv", name.clone()),
        (
            "user.pvc",
            format!("pvc-{idx:08x}{:08x}", idx.wrapping_mul(2_654_435_761)),
        ),
        ("user.namespace", "default".to_string()),
        ("user.capacity", VOLUME_CAPACITY_BYTES.to_string()),
        (
            "user.source",
            "constellation.csi.replicix.com/pool".to_string(),
        ),
        ("user.created", ts().to_string()),
    ];
    for (attr_name, value) in xattrs {
        let t = Instant::now();
        match control
            .call::<BrowseXattr>(XattrParams {
                path: path.clone(),
                op: XattrOp::Set {
                    name: attr_name.to_string(),
                    value: value.into_bytes().into(),
                },
            })
            .await
        {
            Ok(_) => lat.xattr_ms.push(ms(t)),
            Err(e) => {
                let f = lat.record_failure(OpKind::Xattr, ms(t), ms(t_seq), e);
                return (lat, Some(f));
            }
        }
    }

    let t = Instant::now();
    match control
        .call::<QuotaSet>(SetQuotaParams {
            max_bytes: Some(VOLUME_CAPACITY_BYTES),
            subtree: Some(path.clone()),
        })
        .await
    {
        Ok(_) => lat.quota_ms.push(ms(t)),
        Err(e) => {
            let f = lat.record_failure(OpKind::Quota, ms(t), ms(t_seq), e);
            return (lat, Some(f));
        }
    }

    lat.sequence_ms.push(ms(t_seq));
    (lat, None)
}

struct BatchOutcome {
    lat: SeqLatencies,
    errors: u64,
    error_samples: Vec<String>,
}

/// Error samples kept per step, across all tasks.
const ERROR_SAMPLE_CAP: usize = 8;

/// Run `count` `CreateVolume` sequences (subtree indices
/// `start_idx..start_idx+count`) across `concurrency` concurrent tasks on
/// one shared, already-connected `control` handle — mirroring one CSI
/// controller process issuing many concurrent `CreateVolume` RPCs over its
/// one connection to an engine pod (confirmed pipelined server-side:
/// `server.rs`'s `serve_connection` spawns a task per inbound request
/// rather than awaiting handlers in sequence).
async fn run_createvolume_batch(
    control: &ControlClient,
    concurrency: usize,
    start_idx: u64,
    count: u64,
) -> BatchOutcome {
    let next = Arc::new(AtomicU64::new(start_idx));
    let end = start_idx + count;
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let control = control.clone();
        let next = next.clone();
        handles.push(tokio::spawn(async move {
            let mut local = SeqLatencies::default();
            let mut samples: Vec<String> = Vec::new();
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= end {
                    break;
                }
                let (l, failure) = create_volume_once(&control, idx).await;
                local.merge(l);
                if let Some(f) = failure {
                    if samples.len() < ERROR_SAMPLE_CAP {
                        samples.push(format!("idx {idx} {}: {}", f.op.as_str(), f.message));
                    }
                }
            }
            (local, samples)
        }));
    }
    // Both the latencies and the samples come back through the join
    // handles, so nothing depends on a clone count at the end of the step.
    let mut merged = SeqLatencies::default();
    let mut error_samples = Vec::new();
    for h in handles {
        if let Ok((local, samples)) = h.await {
            merged.merge(local);
            for s in samples {
                if error_samples.len() < ERROR_SAMPLE_CAP {
                    error_samples.push(s);
                }
            }
        }
    }
    let errors = merged.failures_by_op.values().sum();
    BatchOutcome {
        lat: merged,
        errors,
        error_samples,
    }
}

/// `node.ping`'s equivalent of [`run_createvolume_batch`]: `count` trivial
/// round trips across `concurrency` tasks.
async fn run_ping_batch(
    control: &ControlClient,
    concurrency: usize,
    count: u64,
) -> (Vec<f64>, u64) {
    let remaining = Arc::new(AtomicU64::new(count));
    let errors = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let control = control.clone();
        let remaining = remaining.clone();
        let errors = errors.clone();
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            loop {
                let prev = remaining.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    if n == 0 {
                        None
                    } else {
                        Some(n - 1)
                    }
                });
                if prev.is_err() {
                    break;
                }
                let t = Instant::now();
                match control.call::<NodePing>(Empty {}).await {
                    Ok(_) => lat.push(t.elapsed().as_secs_f64() * 1000.0),
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            lat
        }));
    }
    let mut merged = Vec::new();
    for h in handles {
        if let Ok(lat) = h.await {
            merged.extend(lat);
        }
    }
    (merged, errors.load(Ordering::Relaxed))
}

/// What one `node.status` call contributes to a step: the commit chain's
/// own backlog and round counter, plus the lease state the failure
/// message this grid collects ("no lease") talks about. Every field is
/// `Option` so a failed call is `null` in the published JSON rather than
/// an indistinguishable zero / "lease not held".
#[derive(Debug, Clone, Copy, Default)]
struct NodeSnapshot {
    journal_backlog: Option<u64>,
    ship_rounds_completed: Option<u64>,
    lease_held: Option<bool>,
    lease_lost: Option<bool>,
}

fn node_snapshot(c: &Client) -> NodeSnapshot {
    match c.control_status() {
        Ok(s) => NodeSnapshot {
            journal_backlog: s["spool"]["journal_backlog"].as_u64(),
            ship_rounds_completed: s["spool"]["ship_rounds_completed"].as_u64(),
            lease_held: s["lease"]["held"].as_bool(),
            lease_lost: s["lease"]["lost"].as_bool(),
        },
        Err(e) => {
            eprintln!(
                "csi-meta-ladder: node.status failed; this step's spool/lease fields are null: {e:#}"
            );
            NodeSnapshot::default()
        }
    }
}

async fn connect_control(c: &Client) -> Result<ControlClient> {
    let sock = constellation_control::transport::locate_socket(c.state_dir())
        .context("no daemon has recorded a control socket")?;
    ControlClient::connect_unix(&sock)
        .await
        .map_err(|e| anyhow::anyhow!("connecting to {}: {}", sock.display(), e.message))
}

/// Run the full `CreateVolume` checkpoint ladder at one concurrency level,
/// against one freshly created, freshly mounted pool filesystem that
/// grows from 0 subtrees through every entry of [`checkpoints`] in order
/// — the shape that lets later checkpoints show the effect of a larger
/// existing pool, not just more concurrent load.
fn run_createvolume_concurrency(
    env: &S3Env,
    root: &Path,
    concurrency: usize,
) -> Result<Vec<CreateVolumeStepReport>> {
    let counter = env.counting_proxy()?;
    let backend = format!("s3://{BUCKET}/csi-meta-ladder-{concurrency}-{}", ts());
    let cfg_root = root.join(format!("c{concurrency}"));
    std::fs::create_dir_all(&cfg_root)?;
    let mut c = Client::new(
        &cfg_root,
        &format!("cv{concurrency}"),
        &counter.endpoint(),
        &backend,
    )?;
    c.fs_create()
        .context("fs.create (once, for the whole pool)")?;
    c.mount().context("mounting the pool daemon")?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .context("building the control-protocol runtime")?;
    let control = rt.block_on(connect_control(&c))?;

    // The one-time parent directory `CreateVolume`'s own `browse.mkdir
    // {/volumes/<name>}` sits under; not part of any step's measurement.
    rt.block_on(control.call::<BrowseMkdir>(MkdirParams {
        path: "/volumes".to_string(),
        mode: Some(0o755),
        parents: true,
    }))
    .map_err(|e| anyhow::anyhow!("creating /volumes: {}", e.message))?;

    let mut reports = Vec::with_capacity(checkpoints().len());
    let mut cumulative = 0u64;
    for checkpoint in checkpoints() {
        let delta = checkpoint.saturating_sub(cumulative);
        if delta == 0 {
            continue;
        }
        let pid = c.pid().context("daemon not running")?;
        counter.reset();
        let cpu_before = cpu_ticks_logged(pid);
        let before = node_snapshot(&c);
        let load_before = loadavg_1min();
        let t0 = Instant::now();
        let outcome = rt.block_on(run_createvolume_batch(
            &control,
            concurrency,
            cumulative,
            delta,
        ));
        let wall_s = t0.elapsed().as_secs_f64().max(1e-9);
        let cpu_after = cpu_ticks_logged(pid);
        let load_after = loadavg_1min();
        let after = node_snapshot(&c);
        let tally = counter.tally();
        let breakdown = reqlog::breakdown(&counter.requests());

        let daemon_cpu_pct = match (cpu_before, cpu_after) {
            (Some(b), Some(a)) => Some((a.saturating_sub(b) as f64 / clk_tck() / wall_s) * 100.0),
            _ => None,
        };

        let (mkdir_p50, mkdir_p99, mkdir_p999) = percentiles(outcome.lat.mkdir_ms);
        let (xattr_p50, xattr_p99, xattr_p999) = percentiles(outcome.lat.xattr_ms);
        let (quota_p50, quota_p99, quota_p999) = percentiles(outcome.lat.quota_ms);
        let (seq_p50, seq_p99, seq_p999) = percentiles(outcome.lat.sequence_ms);
        let (fail_op_p50, fail_op_p99, fail_op_p999) = percentiles(outcome.lat.failed_op_ms);
        let (fail_seq_p50, fail_seq_p99, fail_seq_p999) =
            percentiles(outcome.lat.failed_sequence_ms);
        let rpcs = delta * 8;

        eprintln!(
            "csi-meta-ladder createvolume c={concurrency:<3} cumulative={cumulative:>6}->{checkpoint:<6} \
             in {wall_s:>6.1}s: {:>7.1} vol/s seq p50={seq_p50:>7.2}ms p99={seq_p99:>8.2}ms \
             cpu={:>5.1}% s3_reqs={} backlog {:?}->{:?} load {:?}->{:?} errors={} ({}) \
             failed-op p50={fail_op_p50:>7.2}ms p99={fail_op_p99:>8.2}ms",
            delta as f64 / wall_s,
            daemon_cpu_pct.unwrap_or(f64::NAN),
            tally.total(),
            before.journal_backlog,
            after.journal_backlog,
            load_before,
            load_after,
            outcome.errors,
            outcome
                .lat
                .failures_by_op
                .iter()
                .map(|(op, n)| format!("{op}={n}"))
                .collect::<Vec<_>>()
                .join(" "),
        );
        if !outcome.error_samples.is_empty() {
            for s in &outcome.error_samples {
                eprintln!("  csi-meta-ladder error: {s}");
            }
        }

        reports.push(CreateVolumeStepReport {
            concurrency,
            cumulative_before: cumulative,
            volumes: delta,
            cumulative_after: checkpoint,
            errors: outcome.errors,
            error_samples: outcome.error_samples,
            failures_by_op: outcome.lat.failures_by_op,
            wall_s,
            volumes_per_sec: delta as f64 / wall_s,
            rpcs_per_sec: rpcs as f64 / wall_s,
            mkdir_p50_ms: mkdir_p50,
            mkdir_p99_ms: mkdir_p99,
            mkdir_p999_ms: mkdir_p999,
            xattr_p50_ms: xattr_p50,
            xattr_p99_ms: xattr_p99,
            xattr_p999_ms: xattr_p999,
            quota_p50_ms: quota_p50,
            quota_p99_ms: quota_p99,
            quota_p999_ms: quota_p999,
            sequence_p50_ms: seq_p50,
            sequence_p99_ms: seq_p99,
            sequence_p999_ms: seq_p999,
            failed_op_p50_ms: fail_op_p50,
            failed_op_p99_ms: fail_op_p99,
            failed_op_p999_ms: fail_op_p999,
            failed_sequence_p50_ms: fail_seq_p50,
            failed_sequence_p99_ms: fail_seq_p99,
            failed_sequence_p999_ms: fail_seq_p999,
            daemon_cpu_pct,
            s3_requests_total: tally.total(),
            s3_breakdown: breakdown,
            journal_backlog_before: before.journal_backlog,
            journal_backlog_after: after.journal_backlog,
            ship_rounds_completed_delta: match (
                after.ship_rounds_completed,
                before.ship_rounds_completed,
            ) {
                (Some(a), Some(b)) => Some(a.saturating_sub(b)),
                _ => None,
            },
            lease_held_before: before.lease_held,
            lease_held_after: after.lease_held,
            lease_lost_after: after.lease_lost,
            loadavg_1min_before: load_before,
            loadavg_1min_after: load_after,
            host_cpus: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(0),
        });
        cumulative = checkpoint;
    }

    drop(control);
    rt.shutdown_timeout(Duration::from_secs(5));
    c.unmount().context("unmounting the pool daemon")?;
    Ok(reports)
}

/// `node.ping`'s ladder at one concurrency level: run at the same total
/// op count the `CreateVolume` grid issues at that concurrency (so the
/// two are directly comparable), against a freshly mounted, otherwise
/// empty filesystem (no subtree-count dimension — `node.ping` touches no
/// metadata at all, so there is nothing for it to scale against).
fn run_ping_concurrency(env: &S3Env, root: &Path, concurrency: usize) -> Result<PingStepReport> {
    let backend = format!("s3://{BUCKET}/csi-meta-ladder-ping-{concurrency}-{}", ts());
    let cfg_root = root.join(format!("ping-c{concurrency}"));
    std::fs::create_dir_all(&cfg_root)?;
    let mut c = Client::new(
        &cfg_root,
        &format!("ping{concurrency}"),
        &env.endpoint,
        &backend,
    )?;
    c.fs_create().context("fs.create")?;
    c.mount().context("mounting the ping daemon")?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .context("building the control-protocol runtime")?;
    let control = rt.block_on(connect_control(&c))?;

    let pings: u64 = checkpoints().last().copied().unwrap_or(10_000) * 8;
    let t0 = Instant::now();
    let (lat, errors) = rt.block_on(run_ping_batch(&control, concurrency, pings));
    let wall_s = t0.elapsed().as_secs_f64().max(1e-9);
    let (p50, p99, p999) = percentiles(lat);

    eprintln!(
        "csi-meta-ladder node.ping   c={concurrency:<3} {pings:>6} pings in {wall_s:>6.1}s: \
         {:>9.1} ops/s p50={p50:>6.3}ms p99={p99:>7.3}ms errors={errors}",
        pings as f64 / wall_s,
    );

    drop(control);
    rt.shutdown_timeout(Duration::from_secs(5));
    c.unmount().context("unmounting the ping daemon")?;

    Ok(PingStepReport {
        concurrency,
        pings,
        errors,
        wall_s,
        ops_per_sec: pings as f64 / wall_s,
        p50_ms: p50,
        p99_ms: p99,
        p999_ms: p999,
    })
}

#[derive(Debug, Serialize)]
pub struct CsiLadderResults {
    pub createvolume: Vec<CreateVolumeStepReport>,
    pub ping: Vec<PingStepReport>,
}

/// One concurrency level of the grid, aggregated over its checkpoints —
/// the row shape the plan's Track B table publishes, computed here so the
/// document and the tool cannot disagree about what "seq/s" means:
/// `sum(volumes) / sum(wall_s)` over the level's steps (attempted), and
/// the same with the failures removed from the numerator (successful).
#[derive(Debug, Clone, Serialize)]
pub struct ConcurrencySummary {
    pub concurrency: usize,
    pub attempted: u64,
    pub errors: u64,
    pub error_pct: f64,
    pub attempted_seq_per_sec: f64,
    pub successful_seq_per_sec: f64,
    /// At the last (largest) checkpoint of the level: successful-sequence
    /// percentiles, daemon CPU, and the host load the step ran under.
    pub last_seq_p50_ms: f64,
    pub last_seq_p99_ms: f64,
    /// The failing op's own percentiles, from the level's **last step
    /// that recorded a failure** (the last checkpoint often has none at
    /// the low levels, and a 0.0 there would read as "failures are
    /// instant" rather than "no failures to time").
    pub last_failed_op_p50_ms: f64,
    pub last_failed_op_p99_ms: f64,
    pub last_daemon_cpu_pct: Option<f64>,
    pub last_loadavg_1min: Option<f64>,
}

/// Aggregate [`CreateVolumeStepReport`]s into one row per concurrency
/// level, in ladder order.
pub fn summarize(steps: &[CreateVolumeStepReport]) -> Vec<ConcurrencySummary> {
    let mut levels: Vec<usize> = Vec::new();
    for s in steps {
        if !levels.contains(&s.concurrency) {
            levels.push(s.concurrency);
        }
    }
    levels
        .into_iter()
        .map(|concurrency| {
            let mut level: Vec<&CreateVolumeStepReport> = steps
                .iter()
                .filter(|s| s.concurrency == concurrency)
                .collect();
            level.sort_by_key(|s| s.cumulative_before);
            let attempted: u64 = level.iter().map(|s| s.volumes).sum();
            let errors: u64 = level.iter().map(|s| s.errors).sum();
            let wall: f64 = level.iter().map(|s| s.wall_s).sum::<f64>().max(1e-9);
            let last = level.last().expect("a level has at least one step");
            let last_failing = level.iter().rev().find(|s| s.errors > 0);
            ConcurrencySummary {
                concurrency,
                attempted,
                errors,
                error_pct: if attempted == 0 {
                    0.0
                } else {
                    errors as f64 / attempted as f64 * 100.0
                },
                attempted_seq_per_sec: attempted as f64 / wall,
                successful_seq_per_sec: attempted.saturating_sub(errors) as f64 / wall,
                last_seq_p50_ms: last.sequence_p50_ms,
                last_seq_p99_ms: last.sequence_p99_ms,
                last_failed_op_p50_ms: last_failing.map_or(0.0, |s| s.failed_op_p50_ms),
                last_failed_op_p99_ms: last_failing.map_or(0.0, |s| s.failed_op_p99_ms),
                last_daemon_cpu_pct: last.daemon_cpu_pct,
                last_loadavg_1min: last.loadavg_1min_after,
            }
        })
        .collect()
}

/// The end-of-run table (`metabench`'s precedent), on stderr.
pub fn print_summary(results: &CsiLadderResults) {
    eprintln!(
        "\n=== csi-meta-ladder summary (per concurrency level, aggregated over checkpoints) ==="
    );
    eprintln!(
        "{:>5} {:>9} {:>7} {:>8} {:>11} {:>11} {:>10} {:>10} {:>12} {:>12} {:>8} {:>7}",
        "conc",
        "attempted",
        "errors",
        "err%",
        "att_seq/s",
        "ok_seq/s",
        "seq_p50ms",
        "seq_p99ms",
        "failop_p50",
        "failop_p99",
        "cpu%",
        "load1",
    );
    for r in summarize(&results.createvolume) {
        eprintln!(
            "{:>5} {:>9} {:>7} {:>7.2}% {:>11.2} {:>11.2} {:>10.2} {:>10.2} {:>12.2} {:>12.2} {:>8.1} {:>7.2}",
            r.concurrency,
            r.attempted,
            r.errors,
            r.error_pct,
            r.attempted_seq_per_sec,
            r.successful_seq_per_sec,
            r.last_seq_p50_ms,
            r.last_seq_p99_ms,
            r.last_failed_op_p50_ms,
            r.last_failed_op_p99_ms,
            r.last_daemon_cpu_pct.unwrap_or(f64::NAN),
            r.last_loadavg_1min.unwrap_or(f64::NAN),
        );
    }
    eprintln!("--- node.ping (control-plane overhead, same grid) ---");
    for p in &results.ping {
        eprintln!(
            "{:>5} {:>9} pings {:>12.1} ops/s p50={:>7.3}ms p99={:>7.3}ms p999={:>8.3}ms errors={}",
            p.concurrency, p.pings, p.ops_per_sec, p.p50_ms, p.p99_ms, p.p999_ms, p.errors,
        );
    }
}

/// The whole K0 Track B grid: see the module doc.
pub fn run_all() -> Result<CsiLadderResults> {
    let env = S3Env::start()?;
    // Registers the toxiproxy "s3" route `env.endpoint` depends on (no
    // toxics applied — this grid injects no faults); every client, and
    // `env.counting_proxy()`'s relay chained in front of it, go through
    // `env.endpoint`, which is a dead port until this runs once.
    let _proxy = env.s3_proxy()?;
    let mut root = tempfile::Builder::new()
        .prefix("harness-csi-meta-ladder-")
        .tempdir()?;
    if std::env::var_os("CHAOS_KEEP_TMP").is_some_and(|v| v != "0") {
        root.disable_cleanup(true);
        eprintln!(
            "CHAOS_KEEP_TMP: artifacts kept at {}",
            root.path().display()
        );
    }

    let mut createvolume = Vec::new();
    for &concurrency in &concurrency_ladder() {
        createvolume.extend(run_createvolume_concurrency(
            &env,
            root.path(),
            concurrency,
        )?);
    }

    let mut ping = Vec::new();
    for &concurrency in &concurrency_ladder() {
        ping.push(run_ping_concurrency(&env, root.path(), concurrency)?);
    }

    Ok(CsiLadderResults { createvolume, ping })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_on_empty_is_zero() {
        assert_eq!(percentiles(Vec::new()), (0.0, 0.0, 0.0));
    }

    #[test]
    fn percentiles_sorts_first() {
        let (p50, p99, p999) = percentiles(vec![5.0, 1.0, 3.0, 2.0, 4.0]);
        assert_eq!(p50, 3.0);
        assert_eq!(p99, 5.0);
        assert_eq!(p999, 5.0);
    }

    #[test]
    fn seq_latencies_merge_concatenates() {
        let mut a = SeqLatencies {
            mkdir_ms: vec![1.0],
            xattr_ms: vec![2.0, 3.0],
            quota_ms: vec![4.0],
            sequence_ms: vec![5.0],
            ..Default::default()
        };
        let b = SeqLatencies {
            mkdir_ms: vec![10.0],
            xattr_ms: vec![20.0],
            quota_ms: vec![30.0],
            sequence_ms: vec![40.0],
            ..Default::default()
        };
        a.merge(b);
        assert_eq!(a.mkdir_ms, vec![1.0, 10.0]);
        assert_eq!(a.xattr_ms, vec![2.0, 3.0, 20.0]);
        assert_eq!(a.quota_ms, vec![4.0, 30.0]);
        assert_eq!(a.sequence_ms, vec![5.0, 40.0]);
    }

    fn err(message: &str) -> constellation_control::ControlError {
        constellation_control::ControlError::new(
            constellation_control::proto::error::ErrorKind::Failed,
            message,
        )
    }

    #[test]
    fn record_failure_keeps_the_ops_that_already_succeeded() {
        // The shape the cliff actually has: mkdir + six xattrs land, the
        // trailing quota.set fails. All seven good samples must survive,
        // and the failing op must itself be timed.
        let mut lat = SeqLatencies {
            mkdir_ms: vec![1.0],
            xattr_ms: vec![2.0; 6],
            ..Default::default()
        };
        let f = lat.record_failure(
            OpKind::Quota,
            12.5,
            25.0,
            err("journal not shipped: no lease"),
        );
        assert_eq!(f.op, OpKind::Quota);
        assert_eq!(f.message, "journal not shipped: no lease");
        assert_eq!(lat.mkdir_ms, vec![1.0]);
        assert_eq!(lat.xattr_ms.len(), 6);
        assert_eq!(lat.failed_op_ms, vec![12.5]);
        assert_eq!(lat.failed_sequence_ms, vec![25.0]);
        assert_eq!(lat.failures_by_op.get("quota"), Some(&1));
        // A failed sequence contributes no successful-sequence sample.
        assert!(lat.sequence_ms.is_empty());
    }

    #[test]
    fn merge_sums_failure_counts_per_op() {
        let mut a = SeqLatencies::default();
        a.record_failure(OpKind::Quota, 1.0, 2.0, err("x"));
        let mut b = SeqLatencies::default();
        b.record_failure(OpKind::Quota, 3.0, 4.0, err("x"));
        b.record_failure(OpKind::Mkdir, 5.0, 6.0, err("y"));
        a.merge(b);
        assert_eq!(a.failures_by_op.get("quota"), Some(&2));
        assert_eq!(a.failures_by_op.get("mkdir"), Some(&1));
        assert_eq!(a.failed_op_ms, vec![1.0, 3.0, 5.0]);
        assert_eq!(a.failed_sequence_ms, vec![2.0, 4.0, 6.0]);
    }

    #[test]
    fn summarize_aggregates_a_level_over_its_checkpoints() {
        // Two checkpoints at one concurrency level: the published "seq/s"
        // columns are sum(volumes)/sum(wall_s), not a mean of the steps'
        // own rates (which would over-report, the steps being unequal).
        let steps = vec![
            CreateVolumeStepReport {
                concurrency: 4,
                cumulative_before: 0,
                volumes: 100,
                errors: 0,
                wall_s: 1.0,
                ..Default::default()
            },
            CreateVolumeStepReport {
                concurrency: 4,
                cumulative_before: 100,
                volumes: 900,
                errors: 100,
                wall_s: 3.0,
                sequence_p50_ms: 7.5,
                sequence_p99_ms: 20.0,
                failed_op_p50_ms: 42.0,
                failed_op_p99_ms: 99.0,
                daemon_cpu_pct: Some(110.0),
                loadavg_1min_after: Some(42.0),
                ..Default::default()
            },
            CreateVolumeStepReport {
                concurrency: 16,
                volumes: 1000,
                errors: 0,
                wall_s: 2.0,
                ..Default::default()
            },
        ];
        let rows = summarize(&steps);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].concurrency, 4);
        assert_eq!(rows[0].attempted, 1000);
        assert_eq!(rows[0].errors, 100);
        assert!((rows[0].error_pct - 10.0).abs() < 1e-9);
        // 1000 / 4.0s attempted, 900 / 4.0s successful.
        assert!((rows[0].attempted_seq_per_sec - 250.0).abs() < 1e-9);
        assert!((rows[0].successful_seq_per_sec - 225.0).abs() < 1e-9);
        // "@ last checkpoint" columns come from the largest checkpoint.
        assert_eq!(rows[0].last_seq_p99_ms, 20.0);
        assert_eq!(rows[0].last_failed_op_p99_ms, 99.0);
        assert_eq!(rows[0].last_daemon_cpu_pct, Some(110.0));
        assert_eq!(rows[0].last_loadavg_1min, Some(42.0));
        // Ladder order is preserved, not sorted by level.
        assert_eq!(rows[1].concurrency, 16);
        assert_eq!(rows[1].successful_seq_per_sec, 500.0);
        // A level with no failure anywhere reports no failure latency.
        assert_eq!(rows[1].last_failed_op_p99_ms, 0.0);
    }

    #[test]
    fn summarize_takes_failure_latency_from_the_last_failing_step() {
        // The low levels' failures land in the middle of the ladder; the
        // last checkpoint has none, and must not be read as "a failure
        // costs 0 ms".
        let steps = vec![
            CreateVolumeStepReport {
                concurrency: 8,
                cumulative_before: 0,
                volumes: 100,
                errors: 3,
                wall_s: 1.0,
                failed_op_p50_ms: 11.0,
                failed_op_p99_ms: 12.0,
                ..Default::default()
            },
            CreateVolumeStepReport {
                concurrency: 8,
                cumulative_before: 100,
                volumes: 900,
                errors: 0,
                wall_s: 1.0,
                ..Default::default()
            },
        ];
        let rows = summarize(&steps);
        assert_eq!(rows[0].last_failed_op_p50_ms, 11.0);
        assert_eq!(rows[0].last_failed_op_p99_ms, 12.0);
    }

    #[test]
    fn loadavg_is_readable_on_this_host() {
        // The comparability field Must-fix 1 turns on: if /proc/loadavg
        // ever stops parsing, every future rerun silently loses the only
        // record of how busy the host was.
        let l = loadavg_1min().expect("/proc/loadavg 1-minute field");
        assert!(l >= 0.0, "load average cannot be negative: {l}");
    }

    #[test]
    fn checkpoints_and_concurrency_quick_mode_shrinks() {
        std::env::set_var("CONSTELLATION_CSI_LADDER_QUICK", "1");
        assert_eq!(checkpoints(), vec![5, 20]);
        assert_eq!(concurrency_ladder(), vec![1, 4]);
        std::env::remove_var("CONSTELLATION_CSI_LADDER_QUICK");
        assert_eq!(checkpoints(), CHECKPOINTS.to_vec());
        assert_eq!(concurrency_ladder(), CONCURRENCY_LADDER.to_vec());
    }
}
