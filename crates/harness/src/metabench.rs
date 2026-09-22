//! Plan 29 M4 measurement driver.
//!
//! M4 asks a single question: with M3's fixes landed (sticky leases,
//! forced handoff under contention, jittered retries, the S3-only create
//! storm fixed), is lease serialization the measured bottleneck for
//! metadata-op throughput, or is it something else (forwarding RTT, S3
//! CAS/PUT latency, or the local fjall engine)? This module answers that
//! with real multi-node runs against the same floci+toxiproxy harness
//! environment every other scenario uses, rather than guessing.
//!
//! `harness metabench` runs the whole matrix (single-node baseline, 2-
//! and 3-node with P2P on in a shared and in disjoint directories, and
//! 3-node with P2P off) and prints one JSON report per run plus a plain
//! text table. Results feed directly into
//! `docs/plans/v1/done/29-fjall-metadata-engine.md`'s "M4 — leaseless
//! optimistic commits: decision" section; rerun this to reproduce them.

use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{Context, Result};
use serde::Serialize;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    /// `O_CREAT` + close, zero bytes.
    Create,
    /// `O_CREAT` + write 4 KiB + close.
    Write4k,
}

impl Workload {
    fn label(self) -> &'static str {
        match self {
            Workload::Create => "create",
            Workload::Write4k => "write4k",
        }
    }

    fn run_one(self, path: &Path) -> std::io::Result<()> {
        match self {
            Workload::Create => {
                std::fs::File::create(path)?;
                Ok(())
            }
            Workload::Write4k => {
                let mut f = std::fs::File::create(path)?;
                f.write_all(&[0x42u8; 4096])?;
                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// Every node writes into the same one directory.
    Shared,
    /// Node `i` writes only into its own `node-<i>` directory.
    Disjoint,
    /// Single-node only: ops spread round-robin over `n` directories.
    ManyDirs(u32),
}

impl Layout {
    fn label(self) -> String {
        match self {
            Layout::Shared => "shared".to_string(),
            Layout::Disjoint => "disjoint".to_string(),
            Layout::ManyDirs(n) => format!("many-dirs-{n}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetaBenchConfig {
    pub label: String,
    pub nodes: usize,
    pub ops_per_node: u64,
    pub workload: Workload,
    pub layout: Layout,
    pub p2p: bool,
    /// Extra one-way S3 latency injected via toxiproxy (0 = local floci's
    /// native latency only, sub-millisecond on a dev host).
    pub s3_latency_ms: u64,
    /// `CONSTELLATION_LEASE_TTL_MS` override. The default (60s) makes a
    /// P2P-off, multi-writer contention run take tens of minutes per
    /// config (sticky-lease dwell + the notice-at-renewal worst case are
    /// both TTL-scaled) — short TTLs are how the existing
    /// `create-storm-s3-only` scenario keeps that path's wall-clock
    /// bounded too.
    pub lease_ttl_ms: Option<u64>,
    /// FUSE worker threads issuing ops concurrently *per node* (each
    /// doing `ops_per_node / threads_per_node` ops, one op in flight at a
    /// time on that thread). `1` (every pre-existing config) reproduces
    /// plan 29 M4's matrix exactly: at most one forward in flight per
    /// node, which cannot exercise the single-dispatch-task
    /// serialization M4 found — see plan 29 M5. A value `>1` is what
    /// actually stresses that path.
    pub threads_per_node: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct MetaBenchReport {
    pub label: String,
    pub nodes: usize,
    pub workload: String,
    pub layout: String,
    pub p2p: bool,
    pub s3_latency_ms: u64,
    pub lease_ttl_ms: Option<u64>,
    pub threads_per_node: usize,
    pub ops_per_node: u64,
    pub completed_ops: u64,
    pub errors: u64,
    pub wall_s: f64,
    pub aggregate_ops_per_sec: f64,
    pub per_node_ops_per_sec: Vec<f64>,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub per_node_p50_ms: Vec<f64>,
    pub per_node_p99_ms: Vec<f64>,
    /// Ops the lease holder executed on behalf of a non-holder over P2P
    /// (ADR-14), summed across nodes, during the measured window only.
    pub forwarded_ok_total: u64,
    pub forwarded_err_total: u64,
    /// Max of each node's own rolling forward-latency p50 (last-256
    /// window; see `ForwardState::p50_ms`), sampled at the end of the run.
    pub forward_p50_ms: Option<u64>,
    /// Lease epoch advanced by exactly 1 per genuine handoff to a new
    /// holder (never on same-holder renewal); this is `end - start` on
    /// node 0's view, i.e. handoffs observed cluster-wide.
    pub handoffs: u64,
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

fn eventually(deadline: Duration, mut f: impl FnMut() -> bool) -> Result<()> {
    let start = Instant::now();
    loop {
        if f() {
            return Ok(());
        }
        if start.elapsed() > deadline {
            anyhow::bail!("condition not met within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Run one configuration: mount `cfg.nodes` clients on one shared
/// backend, fire `cfg.ops_per_node` create/write ops per node
/// concurrently (`cfg.threads_per_node` worker threads per node, each
/// with one op in flight at a time), and report throughput, latency
/// percentiles, and the forwarding/handoff counters the daemon already
/// exposes over the control socket.
pub fn run_one(
    env: &S3Env,
    proxy: &crate::toxiproxy::Proxy<'_>,
    root: &Path,
    cfg: &MetaBenchConfig,
) -> Result<MetaBenchReport> {
    // `heal` clears any toxic left by a previous config in the same
    // matrix before (re)applying this one's shape.
    proxy.heal()?;
    if cfg.s3_latency_ms > 0 {
        proxy.latency(cfg.s3_latency_ms, cfg.s3_latency_ms / 8 + 1)?;
    }

    let backend = format!("s3://{BUCKET}/metabench-{}-{}", cfg.label, ts());
    // Each config gets its own state-dir subtree: `Client`'s node identity
    // is tied to `state/node.key` + the registry it last joined, and
    // configs run back-to-back in the same matrix would otherwise reuse
    // "n0"'s state dir against a brand-new backend and hit "node has no
    // registry record" on the second config.
    let cfg_root = root.join(&cfg.label);
    std::fs::create_dir_all(&cfg_root)?;
    let mut clients = Vec::with_capacity(cfg.nodes);
    for i in 0..cfg.nodes {
        let mut c = Client::new(&cfg_root, &format!("n{i}"), &env.endpoint, &backend)?;
        if cfg.nodes > 1 {
            c = c.with_own_node_key();
        }
        if !cfg.p2p {
            c = c.with_env("CONSTELLATION_P2P", "off");
        }
        if let Some(ttl) = cfg.lease_ttl_ms {
            c = c.with_env("CONSTELLATION_LEASE_TTL_MS", &ttl.to_string());
        }
        clients.push(c);
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount().with_context(|| format!("mounting {}", c.name))?;
    }

    let dirs: Vec<String> = match cfg.layout {
        Layout::Shared => vec!["work".to_string()],
        Layout::Disjoint => (0..cfg.nodes).map(|i| format!("node-{i}")).collect(),
        Layout::ManyDirs(n) => (0..n).map(|i| format!("d{i}")).collect(),
    };
    for d in &dirs {
        std::fs::create_dir_all(clients[0].mnt.join(d))?;
    }
    for c in &clients {
        let mnt = c.mnt.clone();
        let dirs = dirs.clone();
        eventually(Duration::from_secs(20), move || {
            dirs.iter().all(|d| mnt.join(d).is_dir())
        })
        .with_context(|| format!("directories visible on {}", c.name))?;
    }

    let mut base_fwd_ok = vec![0u64; cfg.nodes];
    let mut base_fwd_err = vec![0u64; cfg.nodes];
    // Max across nodes, not node 0 alone: a node idles once its own
    // quota is done and its lease view goes stale, so node 0 can miss
    // handoffs between the other two entirely. Whichever node is
    // currently contending has the freshest view.
    let mut start_epoch = 0u64;
    for (i, c) in clients.iter().enumerate() {
        if let Ok(s) = c.control_status() {
            base_fwd_ok[i] = s["forwarded_ok"].as_u64().unwrap_or(0);
            base_fwd_err[i] = s["forwarded_err"].as_u64().unwrap_or(0);
            start_epoch = start_epoch.max(s["lease"]["epoch"].as_u64().unwrap_or(0));
        }
    }

    let errors = Arc::new(AtomicU64::new(0));
    // First few error messages, for diagnosis — a rare op failure under
    // contention is expected (e.g. a lease-handoff-window race), but
    // which errno it is matters for telling "expected transient EIO/
    // EAGAIN" apart from a real bug.
    let error_samples: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let t0 = Instant::now();
    // `threads_per_node` FUSE worker threads per node, each with exactly
    // one op in flight on its own thread at a time (mirroring real
    // concurrent FUSE dispatch) — `>1` is what actually puts more than
    // one forward from this node in flight at once (plan 29 M5); `1`
    // reproduces plan 29 M4's original one-in-flight-per-node shape.
    let threads_per_node = cfg.threads_per_node.max(1);
    let mut handles: Vec<Vec<std::thread::JoinHandle<Vec<u128>>>> = Vec::with_capacity(cfg.nodes);
    for (i, c) in clients.iter().enumerate() {
        let shared_dir = match cfg.layout {
            Layout::Shared => Some(dirs[0].clone()),
            Layout::Disjoint => Some(dirs[i].clone()),
            Layout::ManyDirs(_) => None,
        };
        let mut node_handles = Vec::with_capacity(threads_per_node);
        for tid in 0..threads_per_node {
            // Give any remainder from an uneven split to thread 0, so
            // `sum(ops per thread) == ops_per_node` exactly.
            let ops = cfg.ops_per_node / threads_per_node as u64
                + if tid == 0 {
                    cfg.ops_per_node % threads_per_node as u64
                } else {
                    0
                };
            let mnt = c.mnt.clone();
            let shared_dir = shared_dir.clone();
            let many_dirs = dirs.clone();
            let workload = cfg.workload;
            let errors = Arc::clone(&errors);
            let error_samples = Arc::clone(&error_samples);
            // `Disjoint` with more than one thread per node: without a
            // per-thread subdirectory, every thread on a given node would
            // still write into that node's *one* directory, so their
            // conflict-key sets (the shared parent) would still overlap
            // and the ordering gate would (correctly) serialize them —
            // silently defeating the point of a "disjoint" concurrency
            // config. `Shared` deliberately keeps every thread on every
            // node in the *same* one directory (maximal, intentional
            // contention: the gate must still serialize that case).
            let per_thread_subdir = threads_per_node > 1 && matches!(cfg.layout, Layout::Disjoint);
            if per_thread_subdir {
                if let Some(d) = &shared_dir {
                    let _ = std::fs::create_dir_all(mnt.join(d).join(format!("t{tid}")));
                }
            }
            node_handles.push(std::thread::spawn(move || -> Vec<u128> {
                let mut lat = Vec::with_capacity(ops as usize);
                for n in 0..ops {
                    let d = match &shared_dir {
                        Some(d) => d,
                        None => &many_dirs[(n as usize) % many_dirs.len()],
                    };
                    let path = if per_thread_subdir {
                        mnt.join(d).join(format!("t{tid}")).join(format!("n{i}-{n}"))
                    } else {
                        mnt.join(d).join(format!("n{i}-t{tid}-{n}"))
                    };
                    let t = Instant::now();
                    match workload.run_one(&path) {
                        Ok(()) => lat.push(t.elapsed().as_micros()),
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            let mut samples = error_samples.lock().unwrap();
                            if samples.len() < 8 {
                                samples.push(format!(
                                    "node n{i} thread {tid} op {n}: {e} (kind={:?}, raw_os_error={:?})",
                                    e.kind(),
                                    e.raw_os_error()
                                ));
                            }
                        }
                    }
                }
                lat
            }));
        }
        handles.push(node_handles);
    }
    let mut per_node_lat = Vec::with_capacity(cfg.nodes);
    for node_handles in handles {
        let mut lat = Vec::new();
        for h in node_handles {
            lat.extend(
                h.join()
                    .map_err(|_| anyhow::anyhow!("metabench worker thread panicked"))?,
            );
        }
        per_node_lat.push(lat);
    }
    let wall_s = t0.elapsed().as_secs_f64();
    for sample in error_samples.lock().unwrap().iter() {
        eprintln!("metabench {}: op error: {sample}", cfg.label);
    }

    let mut fwd_ok_total = 0u64;
    let mut fwd_err_total = 0u64;
    let mut fwd_p50s = Vec::new();
    let mut end_epoch = start_epoch;
    for (i, c) in clients.iter().enumerate() {
        if let Ok(s) = c.control_status() {
            fwd_ok_total += s["forwarded_ok"]
                .as_u64()
                .unwrap_or(0)
                .saturating_sub(base_fwd_ok[i]);
            fwd_err_total += s["forwarded_err"]
                .as_u64()
                .unwrap_or(0)
                .saturating_sub(base_fwd_err[i]);
            if let Some(p50) = s["forward_p50_ms"].as_u64() {
                fwd_p50s.push(p50);
            }
            end_epoch = end_epoch.max(s["lease"]["epoch"].as_u64().unwrap_or(0));
        }
    }
    let handoffs = end_epoch.saturating_sub(start_epoch);

    for c in clients.iter_mut() {
        c.unmount()
            .with_context(|| format!("unmounting {}", c.name))?;
    }

    let mut all_lat_ms: Vec<f64> = per_node_lat
        .iter()
        .flatten()
        .map(|&us| us as f64 / 1000.0)
        .collect();
    all_lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let per_node_ops_per_sec: Vec<f64> = per_node_lat
        .iter()
        .map(|v| v.len() as f64 / wall_s.max(1e-9))
        .collect();
    let per_node_p50_ms: Vec<f64> = per_node_lat
        .iter()
        .map(|v| {
            let mut ms: Vec<f64> = v.iter().map(|&us| us as f64 / 1000.0).collect();
            ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            pct(&ms, 0.50)
        })
        .collect();
    let per_node_p99_ms: Vec<f64> = per_node_lat
        .iter()
        .map(|v| {
            let mut ms: Vec<f64> = v.iter().map(|&us| us as f64 / 1000.0).collect();
            ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            pct(&ms, 0.99)
        })
        .collect();
    let completed_ops: u64 = per_node_lat.iter().map(|v| v.len() as u64).sum();

    Ok(MetaBenchReport {
        label: cfg.label.clone(),
        nodes: cfg.nodes,
        workload: cfg.workload.label().to_string(),
        layout: cfg.layout.label(),
        p2p: cfg.p2p,
        s3_latency_ms: cfg.s3_latency_ms,
        lease_ttl_ms: cfg.lease_ttl_ms,
        threads_per_node: cfg.threads_per_node,
        ops_per_node: cfg.ops_per_node,
        completed_ops,
        errors: errors.load(Ordering::Relaxed),
        wall_s,
        aggregate_ops_per_sec: completed_ops as f64 / wall_s.max(1e-9),
        per_node_ops_per_sec,
        p50_ms: pct(&all_lat_ms, 0.50),
        p99_ms: pct(&all_lat_ms, 0.99),
        per_node_p50_ms,
        per_node_p99_ms,
        forwarded_ok_total: fwd_ok_total,
        forwarded_err_total: fwd_err_total,
        forward_p50_ms: fwd_p50s.into_iter().max(),
        handoffs,
    })
}

/// Raw S3 op latency against the harness's floci container, bypassing
/// constellation entirely: plain `PUT`, CAS-create (`If-None-Match: *`,
/// the same header `store-s3::log`/`commits` use), and `GET`. Gives the
/// floor a lease renewal/handoff CAS or a commit publish can possibly
/// beat, at whatever latency `latency_ms` (via toxiproxy) adds.
#[derive(Debug, Serialize)]
pub struct RawS3Timing {
    pub latency_ms: u64,
    pub put_p50_ms: f64,
    pub cas_create_p50_ms: f64,
    pub get_p50_ms: f64,
}

pub fn raw_s3_timing(
    env: &S3Env,
    proxy: &crate::toxiproxy::Proxy<'_>,
    iterations: u64,
    latency_ms: u64,
) -> Result<RawS3Timing> {
    proxy.heal()?;
    if latency_ms > 0 {
        proxy.latency(latency_ms, latency_ms / 8 + 1)?;
    }
    let base = format!("{}/{}/metabench-raw-{}", env.endpoint, BUCKET, ts());
    let mut put_ms = Vec::new();
    let mut cas_ms = Vec::new();
    let mut get_ms = Vec::new();
    for i in 0..iterations {
        let key = format!("{base}/obj-{i}");
        let t = Instant::now();
        ureq::put(&key).send_bytes(b"x")?;
        put_ms.push(t.elapsed().as_secs_f64() * 1000.0);

        let cas_key = format!("{base}/cas-{i}");
        let t = Instant::now();
        ureq::put(&cas_key)
            .set("If-None-Match", "*")
            .send_bytes(b"x")?;
        cas_ms.push(t.elapsed().as_secs_f64() * 1000.0);

        let t = Instant::now();
        let _ = ureq::get(&key).call()?;
        get_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let p50 = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        pct(v, 0.50)
    };
    Ok(RawS3Timing {
        latency_ms,
        put_p50_ms: p50(&mut put_ms),
        cas_create_p50_ms: p50(&mut cas_ms),
        get_p50_ms: p50(&mut get_ms),
    })
}

/// Ops-per-node scaled down when `CONSTELLATION_METABENCH_QUICK=1`, for a
/// fast smoke run of this driver itself.
fn ops(default: u64) -> u64 {
    if std::env::var_os("CONSTELLATION_METABENCH_QUICK").is_some_and(|v| v != "0") {
        (default / 10).max(20)
    } else {
        default
    }
}

/// The full M4 measurement matrix. See the module doc.
pub fn run_matrix() -> Result<(Vec<MetaBenchReport>, Vec<RawS3Timing>)> {
    let env = S3Env::start()?;
    let proxy = env.s3_proxy()?;
    let mut root = tempfile::Builder::new()
        .prefix("harness-metabench-")
        .tempdir()?;
    if std::env::var_os("CHAOS_KEEP_TMP").is_some_and(|v| v != "0") {
        root.disable_cleanup(true);
        eprintln!(
            "CHAOS_KEEP_TMP: artifacts kept at {}",
            root.path().display()
        );
    }

    // Every row is run at BOTH 0ms and 20ms injected S3 latency, and
    // both `create` (pure metadata, no data chunk) and `write4k`
    // (metadata + one 4 KiB data-chunk PUT, which pays its own S3 RTT
    // independent of, and on top of, any metadata forwarding cost) are
    // reported separately for every multi-node row — otherwise a
    // single-node/0ms baseline compared against a multi-node/20ms row
    // confounds "more nodes" with "slower S3", and `write4k`'s own
    // chunk upload confounds "forwarding cost" with "data-plane cost".
    let mut configs = Vec::new();
    for latency in [0u64, 20u64] {
        for workload in [Workload::Create, Workload::Write4k] {
            let wl = workload.label();
            configs.push(MetaBenchConfig {
                label: format!("1node-{wl}-lat{latency}"),
                nodes: 1,
                ops_per_node: ops(4000),
                threads_per_node: 1,
                workload,
                layout: Layout::Shared,
                p2p: true,
                s3_latency_ms: latency,
                lease_ttl_ms: None,
            });
            for layout in [Layout::Shared, Layout::Disjoint] {
                let ly = layout.label();
                configs.push(MetaBenchConfig {
                    label: format!("3node-p2pon-{ly}-{wl}-lat{latency}"),
                    nodes: 3,
                    ops_per_node: ops(1200),
                    threads_per_node: 1,
                    workload,
                    layout,
                    p2p: true,
                    s3_latency_ms: latency,
                    lease_ttl_ms: None,
                });
                // P2P off (S3-only lease path): a short TTL keeps
                // sticky-lease dwell/notice-at-renewal bounded (the
                // default 60s TTL would make this config alone take
                // tens of minutes); see `create-storm-s3-only`'s
                // scenario for the same precedent.
                configs.push(MetaBenchConfig {
                    label: format!("3node-p2poff-{ly}-{wl}-lat{latency}"),
                    nodes: 3,
                    ops_per_node: ops(300),
                    threads_per_node: 1,
                    workload,
                    layout,
                    p2p: false,
                    s3_latency_ms: latency,
                    lease_ttl_ms: Some(5_000),
                });
            }
        }
    }

    // Plan 29 M5: every row above uses exactly one FUSE worker thread per
    // node, so it has at most one forward in flight per node at a time —
    // the shape plan 29 M4's own matrix used, which cannot exercise the
    // single-dispatch-task serialization M4 found (that bottleneck only
    // bites when *this node's own* concurrent FUSE threads each have a
    // forward outstanding). These rows add several concurrent threads
    // per node on the `create` workload at 0ms latency, 3-node/P2P-on,
    // shared and disjoint — the exact condition the M5 fix targets — plus
    // a matching 1-node concurrent baseline for comparison.
    configs.push(MetaBenchConfig {
        label: "1node-create-concurrent4-lat0".to_string(),
        nodes: 1,
        ops_per_node: ops(4000),
        threads_per_node: 4,
        workload: Workload::Create,
        layout: Layout::Shared,
        p2p: true,
        s3_latency_ms: 0,
        lease_ttl_ms: None,
    });
    for layout in [Layout::Shared, Layout::Disjoint] {
        let ly = layout.label();
        configs.push(MetaBenchConfig {
            label: format!("3node-p2pon-{ly}-create-concurrent4-lat0"),
            nodes: 3,
            ops_per_node: ops(1200),
            threads_per_node: 4,
            workload: Workload::Create,
            layout,
            p2p: true,
            s3_latency_ms: 0,
            lease_ttl_ms: None,
        });
    }

    // Diagnostic-only filter (not a documented knob): run a subset of
    // the matrix by label substring, for reproducing a rare failure
    // without paying for the whole ~20-config matrix each attempt.
    if let Ok(only) = std::env::var("CONSTELLATION_METABENCH_ONLY") {
        configs.retain(|c| c.label.contains(&only));
    }
    let mut reports = Vec::with_capacity(configs.len());
    for cfg in &configs {
        eprintln!("=== metabench {} ===", cfg.label);
        let t0 = Instant::now();
        let report = run_one(&env, &proxy, root.path(), cfg)
            .with_context(|| format!("metabench config {}", cfg.label))?;
        eprintln!(
            "=== metabench {} done in {:.1?}: {:.0} ops/s agg, p50={:.2}ms p99={:.2}ms fwd_ok={} handoffs={}",
            cfg.label,
            t0.elapsed(),
            report.aggregate_ops_per_sec,
            report.p50_ms,
            report.p99_ms,
            report.forwarded_ok_total,
            report.handoffs,
        );
        reports.push(report);
    }

    // 4. Raw S3 op latency floor, at the same injected latency the
    // multi-node configs above used, plus a 0ms baseline for reference.
    let mut raw = Vec::new();
    for latency_ms in [0, 20] {
        raw.push(raw_s3_timing(&env, &proxy, 30, latency_ms)?);
    }

    Ok((reports, raw))
}
