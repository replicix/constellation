//! The controller's purge worker (plan 37 settled decision 19, §"Deletion
//! and purge", K6b): what makes a deleted pool volume actually go.
//!
//! `DeleteVolume` only renames `/volumes/<pv>` to `/.trash/<pv>-<unix ms>`
//! (one metadata op, the quota released first). This worker, running in
//! whichever controller replica holds the purge lease ([`Leader`]), walks
//! every pool's `/.trash` every `--purge-interval` and removes the entries
//! older than `--purge-grace` for real.
//!
//! **Where it runs: the controller-owned engine pods only.** Each pool has
//! one (`constellation-engine-<unit>-controller`, [`crate::engine_pods`]).
//! The controller cannot reach a node-owned pod at all (the exec policy
//! holds its relay to `-controller` pods).
//!
//! **Which pools: from the cluster, not from the pods that happen to be
//! up** ([`PurgeBackend::pools`]). A pass covers every pool that a
//! `PersistentVolume` of this driver names, every pool recorded as having
//! had a volume trashed (the `constellation-csi-pools` ConfigMap:
//! `DeleteVolume` records a pool before it trashes into it, and every pass
//! records the pools its PVs name, so a pool whose last PV is gone is still
//! known), and every controller-owned pod that is up; it brings up the
//! controller-owned pod of each that has none (a drained node, an
//! eviction, a node loss: the pod is bare and its state an `emptyDir`, so
//! nothing else would). §7: one controller-owned pod per pool with a
//! volume — or with trash. The pods come up concurrently, so a pool that
//! cannot waits out its ready timeout alone. A record is built from the PV
//! itself (its `volumeAttributes` and its provisioner deletion-secret
//! annotations), so a StorageClass deleted first loses nothing. A pool's
//! record goes when its pod is reaped — or when no PV names the pool and
//! its pod failed to come up in several passes in a row over an hour (its
//! bucket or credentials gone, a reinstall against other storage): then
//! it is dropped with a `warn`, and any trash it had stays in the bucket.
//! A `DeleteVolume` the provisioner repeats after its PV is gone still
//! brings nothing back (plan 37 K4): only the worker starts a pod for
//! trash.
//!
//! **How: an entry at a time, a file at a time, paced.** A trashed volume
//! is walked depth first (`browse.readdir`), each regular file's size read
//! (`browse.stat`) and the file unlinked (`browse.delete`), each directory
//! removed once empty. Every unlink and rmdir is one op against the pool's
//! budget of `--purge-ops-per-second` and every file's size is charged to
//! its `--purge-bytes-per-second`; up to `--purge-max-concurrent-deletes`
//! unlinks of one directory are in flight at once. Budgets are per pool and
//! every pool is purged by a task of its own, so one huge trashed volume
//! slows only its own pool's purge (and a pool still being purged when the
//! next tick comes is simply not started twice). A whole-tree
//! `browse.delete{recursive}` would be one call of unbounded length that no
//! budget could pace and no crash could resume.
//!
//! **Resumable and idempotent.** There is no purge state anywhere but the
//! pool's own `/.trash`: a pass that dies part-way (the controller
//! restarted, leadership moved, the relay broke, a call failed) leaves a
//! smaller but valid subtree, and the next pass re-lists and goes on; an
//! entry somebody else removed meanwhile (`NotFound`) is already done.
//! Only a directory too large for one `browse.readdir` frame (the protocol
//! has no paging; about 90 000 entries of one directory), which the server
//! answers with `EOVERFLOW`, is removed with one recursive `browse.delete`,
//! logged at `warn`, charged as one op. Any other failure to list leaves
//! the entry for the next pass.
//!
//! **GC interaction: no double accounting.** The worker deletes no S3
//! object and touches no quota: the volume's quota was released when
//! `DeleteVolume` trashed it, and the chunks its files referenced become
//! unreferenced once the entries are gone, for the engine's own bucket GC
//! (`gc.run`, the `_gc` singleton) to reclaim on its schedule — exactly as
//! for any other unlink. `csi-trash-purge-under-load` checks both halves:
//! GC's candidates grow by the purged volumes' chunks only once the purge
//! removed them, and the chunk census does not move with the purge itself.
//!
//! **After a pass**, through the same pod:
//! - **Registry sweep** (the 37-k6a review's registry churn): the
//!   controller-owned pod keeps its state on `emptyDir`, so every
//!   rescheduled incarnation joins the pool's registry as a fresh node and
//!   leaves the old record behind; a node-owned pod's node that vanished
//!   without a drain (a crash, a scale-down that skipped it) leaves its
//!   record too. Records of this pod's hostname but not its node id, and
//!   records of a node-owned engine whose Kubernetes node no longer exists
//!   (a node-owned pod's hostname is set to name its node,
//!   [`crate::engine_pods::node_engine_hostname`]: the default, the pod
//!   name cut to 63 bytes, loses it), are retired with an admin
//!   `node.leave{node_id}` — refused (and retried next pass) while the dead
//!   node's lease is still live.
//! - **Reap**: a pool whose `/.trash` and `/volumes` have both been empty
//!   for a while (over two passes, at least a minute: a `CreateVolume`
//!   finds a new pool empty too, between its calls into the pod) has no
//!   volume left; its pod leaves the registry and is deleted (§7), and its
//!   record goes. The next `CreateVolume` starts a new one.
//! - **Roll**: K5a's handoff rolls only node-owned pods. A controller-owned
//!   pod serves no view, so one whose engine settings drifted from the
//!   controller's (a chart upgrade) is replaced plainly — leave, delete,
//!   recreate from its own spec with the new settings — between passes.
//!
//! Neither runs while any controller replica's RPC (the worker's own calls
//! included) is using the pod: each
//! replica *holds* a pod it calls into (an annotation it writes with a
//! compare-and-swap before its first call, renewed while it calls), and a
//! reap or roll first *marks* the pod retiring by a compare-and-swap of its
//! own, which fails on — or makes a later hold wait for — the other
//! (`crate::engine_pods`, "Retiring a controller-owned pod").

use crate::control_client::ControlClient;
use crate::volume_id::{TRASH_DIR, VOLUMES_DIR};
use async_trait::async_trait;
use constellation_control::proto::types::{DeleteParams, LeaveParams};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_types::Code;
use futures::stream::{self, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The worker's settings (the chart's `purge.*`).
#[derive(Debug, Clone, PartialEq)]
pub struct PurgeConfig {
    /// Between passes; `None`: the worker is off.
    pub interval: Option<Duration>,
    /// How old a trash entry must be (by the timestamp in its name).
    pub grace: Duration,
    /// Unlinks of one directory in flight at once, per pool.
    pub concurrency: usize,
    /// Unlinks and rmdirs per second, per pool (`0`: unpaced).
    pub ops_per_s: u64,
    /// File bytes unlinked per second, per pool (`0`: unpaced).
    pub bytes_per_s: u64,
}

impl Default for PurgeConfig {
    /// Plan 37 §"Deletion and purge": every 5 minutes, a 1 minute grace,
    /// 4 concurrent deletes; 500 ops/s and 256 MiB/s per pool.
    fn default() -> Self {
        PurgeConfig {
            interval: Some(Duration::from_secs(300)),
            grace: Duration::from_secs(60),
            concurrency: 4,
            ops_per_s: 500,
            bytes_per_s: 256 << 20,
        }
    }
}

impl PurgeConfig {
    /// `CONSTELLATION_CSI_PURGE_INTERVAL` (a duration, `0` turns the worker
    /// off), `CONSTELLATION_CSI_PURGE_GRACE`,
    /// `CONSTELLATION_CSI_PURGE_MAX_CONCURRENT_DELETES`,
    /// `CONSTELLATION_CSI_PURGE_OPS_PER_SECOND`,
    /// `CONSTELLATION_CSI_PURGE_BYTES_PER_SECOND` (each unset: the
    /// default).
    pub fn from_env() -> Result<PurgeConfig, String> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<PurgeConfig, String> {
        let var = |k: &str| var(k).filter(|v| !v.trim().is_empty());
        let mut cfg = PurgeConfig::default();
        if let Some(v) = var("CONSTELLATION_CSI_PURGE_INTERVAL") {
            let d = crate::params::parse_duration(&v)
                .map_err(|e| format!("CONSTELLATION_CSI_PURGE_INTERVAL: {e}"))?;
            cfg.interval = (!d.is_zero()).then_some(d);
        }
        if let Some(v) = var("CONSTELLATION_CSI_PURGE_GRACE") {
            cfg.grace = crate::params::parse_duration(&v)
                .map_err(|e| format!("CONSTELLATION_CSI_PURGE_GRACE: {e}"))?;
        }
        let number = |k: &str| -> Result<Option<u64>, String> {
            var(k)
                .map(|v| crate::params::parse_quantity(&v).map_err(|e| format!("{k}: {e}")))
                .transpose()
        };
        if let Some(n) = number("CONSTELLATION_CSI_PURGE_MAX_CONCURRENT_DELETES")? {
            cfg.concurrency = (n as usize).max(1);
        }
        if let Some(n) = number("CONSTELLATION_CSI_PURGE_OPS_PER_SECOND")? {
            cfg.ops_per_s = n;
        }
        if let Some(n) = number("CONSTELLATION_CSI_PURGE_BYTES_PER_SECOND")? {
            cfg.bytes_per_s = n;
        }
        Ok(cfg)
    }
}

/// A per-pool pace, one clock per budget (a generic cell rate algorithm):
/// an op may start once both clocks have reached it — never before now —
/// and then moves the ops clock on by `1/ops_per_s` and the bytes clock by
/// its bytes over `bytes_per_s`. A clock that fell behind real time is
/// brought up to it first, so one budget idling while the other rules
/// banks no credit: no stretch of ops ever beats either budget.
pub struct Pace {
    ops_per_s: u64,
    bytes_per_s: u64,
    /// (ops clock, bytes clock): when the next op may start by each.
    next: Mutex<(tokio::time::Instant, tokio::time::Instant)>,
}

impl Pace {
    pub fn new(cfg: &PurgeConfig) -> Pace {
        let now = tokio::time::Instant::now();
        Pace {
            ops_per_s: cfg.ops_per_s,
            bytes_per_s: cfg.bytes_per_s,
            next: Mutex::new((now, now)),
        }
    }

    /// Charge one op of `bytes` and wait until the budget allows it.
    pub async fn charge(&self, bytes: u64) {
        let due = {
            let mut next = self.next.lock().unwrap();
            let now = tokio::time::Instant::now();
            let due = now.max(next.0).max(next.1);
            if self.ops_per_s > 0 {
                next.0 = due + Duration::from_secs_f64(1.0 / self.ops_per_s as f64);
            }
            if self.bytes_per_s > 0 {
                next.1 = due + Duration::from_secs_f64(bytes as f64 / self.bytes_per_s as f64);
            }
            due
        };
        tokio::time::sleep_until(due).await;
    }
}

/// What purging one trash entry did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EntryReport {
    /// Unlinks and rmdirs (a recursive fallback counts as one).
    pub ops: u64,
    pub files: u64,
    pub dirs: u64,
    pub bytes: u64,
    /// Directories too large to list, removed with one recursive delete.
    pub fallbacks: u64,
}

/// What one pass over one pool did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassReport {
    /// Entries removed whole.
    pub purged: Vec<(String, EntryReport)>,
    /// Entries younger than the grace window.
    pub waiting: usize,
    /// Entries a failure left for the next pass.
    pub failed: usize,
    /// `/.trash` and `/volumes` were both empty at the end.
    pub empty: bool,
}

/// `<pv>-<unix ms>`'s timestamp (`DeleteVolume`'s trash names).
pub fn trashed_at_ms(name: &str) -> Option<u64> {
    let (_, ts) = name.rsplit_once('-')?;
    ts.parse().ok()
}

/// Gone already: every `browse.*` answers a missing path `NotFound`.
pub fn not_found(e: &ControlError) -> bool {
    e.kind == ErrorKind::NotFound
}

/// The entries of `dir`, `None` when it does not exist.
async fn list(
    fs: &dyn ControlClient,
    dir: &str,
) -> Result<Option<Vec<(String, bool)>>, ControlError> {
    match fs.browse_readdir(dir).await {
        Ok(listing) => Ok(Some(
            listing
                .entries
                .into_iter()
                .map(|e| (e.path, e.kind == "dir" || e.kind == "directory"))
                .collect(),
        )),
        Err(e) if not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn delete(fs: &dyn ControlClient, path: &str, recursive: bool) -> Result<(), ControlError> {
    match fs
        .browse_delete(DeleteParams {
            path: path.to_string(),
            recursive,
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if not_found(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Remove `root` (a trash entry) and everything under it, paced by
/// `pace` (module docs). `stop`: leadership lost; the walk ends at the
/// next op, leaving the rest for whoever leads next.
pub async fn purge_entry(
    fs: &dyn ControlClient,
    root: &str,
    is_dir: bool,
    cfg: &PurgeConfig,
    pace: &Pace,
    stop: &AtomicBool,
) -> Result<EntryReport, ControlError> {
    let report = Mutex::new(EntryReport::default());
    let stopped = || {
        if stop.load(Ordering::SeqCst) {
            Err(ControlError::unavailable(
                "the purge lease moved to another replica",
            ))
        } else {
            Ok(())
        }
    };
    if !is_dir {
        let size = fs.browse_stat(root).await.map(|s| s.size).unwrap_or(0);
        pace.charge(size).await;
        delete(fs, root, false).await?;
        let mut r = report.into_inner().unwrap();
        r.ops = 1;
        r.files = 1;
        r.bytes = size;
        return Ok(r);
    }
    // Depth first, iteratively: (directory, its children are gone).
    let mut stack = vec![(root.to_string(), false)];
    while let Some((dir, emptied)) = stack.pop() {
        stopped()?;
        if emptied {
            pace.charge(0).await;
            delete(fs, &dir, false).await?;
            let mut r = report.lock().unwrap();
            r.ops += 1;
            r.dirs += 1;
            continue;
        }
        let entries = match list(fs, &dir).await {
            Ok(Some(entries)) => entries,
            Ok(None) => continue,
            Err(e) if e.code == Some(Code::Overflow) => {
                // A listing over one frame (no paging): the one case the
                // budget cannot pace. Anything else is retried next pass.
                tracing::warn!(dir, error = %e.message,
                    "a trashed directory is too large to list; removing it with one recursive delete");
                pace.charge(0).await;
                delete(fs, &dir, true).await?;
                let mut r = report.lock().unwrap();
                r.ops += 1;
                r.dirs += 1;
                r.fallbacks += 1;
                continue;
            }
            Err(e) => return Err(e),
        };
        stack.push((dir, true));
        let (dirs, files): (Vec<_>, Vec<_>) = entries.into_iter().partition(|(_, d)| *d);
        stack.extend(dirs.into_iter().map(|(p, _)| (p, false)));
        stream::iter(files.into_iter().map(Ok))
            .try_for_each_concurrent(cfg.concurrency.max(1), |(path, _)| {
                let report = &report;
                async move {
                    stopped()?;
                    let size = match fs.browse_stat(&path).await {
                        Ok(stat) => stat.size,
                        Err(e) if not_found(&e) => return Ok(()),
                        Err(e) => return Err(e),
                    };
                    pace.charge(size).await;
                    delete(fs, &path, false).await?;
                    let mut r = report.lock().unwrap();
                    r.ops += 1;
                    r.files += 1;
                    r.bytes += size;
                    Ok(())
                }
            })
            .await?;
    }
    Ok(report.into_inner().unwrap())
}

/// One pass over one pool's `/.trash` (module docs), oldest entry first.
pub async fn purge_pool(
    pool: &str,
    fs: &dyn ControlClient,
    cfg: &PurgeConfig,
    now_ms: u64,
    stop: &AtomicBool,
) -> Result<PassReport, ControlError> {
    let mut report = PassReport::default();
    let pace = Pace::new(cfg);
    let mut entries: Vec<(u64, String, bool)> = list(fs, TRASH_DIR)
        .await?
        .unwrap_or_default()
        .into_iter()
        .map(|(path, is_dir)| {
            let name = path.rsplit('/').next().unwrap_or_default();
            (trashed_at_ms(name), path, is_dir)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(at, path, is_dir)| (at.unwrap_or(u64::MAX), path, is_dir))
        .collect();
    entries.sort();
    let grace_ms = cfg.grace.as_millis() as u64;
    for (at, path, is_dir) in entries {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        // A name without a timestamp is not `DeleteVolume`'s; it is in the
        // trash all the same, and its own mtime says how long.
        let at = if at == u64::MAX {
            match fs.browse_stat(&path).await {
                Ok(stat) => (stat.mtime_ns.max(0) / 1_000_000) as u64,
                Err(_) => now_ms,
            }
        } else {
            at
        };
        if now_ms.saturating_sub(at) < grace_ms {
            report.waiting += 1;
            continue;
        }
        let started = Instant::now();
        match purge_entry(fs, &path, is_dir, cfg, &pace, stop).await {
            Ok(entry) => {
                let took = started.elapsed();
                let secs = took.as_secs_f64().max(1e-3);
                tracing::info!(
                    pool,
                    entry = %path,
                    ops = entry.ops,
                    files = entry.files,
                    dirs = entry.dirs,
                    bytes = entry.bytes,
                    fallbacks = entry.fallbacks,
                    took_ms = took.as_millis() as u64,
                    ops_per_s = format!("{:.1}", entry.ops as f64 / secs),
                    bytes_per_s = format!("{:.0}", entry.bytes as f64 / secs),
                    "purged trash entry"
                );
                report.purged.push((path, entry));
            }
            Err(e) => {
                tracing::warn!(pool, entry = %path, error = %e.message,
                    "purging a trash entry failed; the next pass resumes it");
                report.failed += 1;
            }
        }
    }
    if report.waiting == 0 && report.failed == 0 && !stop.load(Ordering::SeqCst) {
        let trash = list(fs, TRASH_DIR).await?.unwrap_or_default();
        let volumes = list(fs, VOLUMES_DIR).await?.unwrap_or_default();
        report.empty = trash.is_empty() && volumes.is_empty();
    }
    Ok(report)
}

/// Whether `host` has the exact shape of
/// [`crate::engine_pods::node_engine_hostname`] (`csi-node-[<head>-]<10
/// hex>`, at most 56 bytes): a human's mount merely named `csi-node-…` is
/// not judged by the sweep.
fn is_node_engine_hostname(host: &str) -> bool {
    use crate::engine_pods::NODE_ENGINE_HOST_PREFIX;
    let Some(rest) = host.strip_prefix(NODE_ENGINE_HOST_PREFIX) else {
        return false;
    };
    let (head, hash) = match rest.rsplit_once('-') {
        Some((head, hash)) => (head, hash),
        None => ("", rest),
    };
    host.len() <= 56
        && hash.len() == 10
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && head.len() <= 35
        && head
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !head.starts_with('-')
        && !head.ends_with('-')
}

/// The registry sweep through pool pod `pod` (module docs): the ids it
/// retired. `nodes`: the cluster's nodes now. A node-owned engine's record
/// carries its pod's hostname, [`crate::engine_pods::node_engine_hostname`]
/// of its node; a controller-owned one's is its pod name as kubelet makes
/// it a hostname ([`crate::engine_pods::pod_hostname`]).
pub async fn sweep_registry(
    fs: &dyn ControlClient,
    pod: &str,
    nodes: &HashSet<String>,
) -> Result<Vec<u64>, ControlError> {
    use crate::engine_pods::{node_engine_hostname, pod_hostname};
    let me = fs.node_id().await?;
    let peers = fs.peers_list().await?;
    let own = pod_hostname(pod);
    let live: HashSet<String> = nodes.iter().map(|n| node_engine_hostname(n)).collect();
    let mut retired = Vec::new();
    for peer in peers.peers {
        if peer.s3 || peer.node_id == 0 || peer.node_id == me {
            continue;
        }
        let Some(host) = peer.hostname.as_deref() else {
            continue;
        };
        // An earlier incarnation of this very pod (emptyDir state), or a
        // node-owned engine of a node that is gone. Anything else — a
        // human's mount (settled decision 18), a node that still exists —
        // is not this sweep's to judge.
        let dead = host == own || (is_node_engine_hostname(host) && !live.contains(host));
        if !dead {
            continue;
        }
        match fs
            .node_leave(LeaveParams {
                node_id: Some(peer.node_id),
                force: false,
            })
            .await
        {
            Ok(_) => {
                tracing::info!(
                    pod,
                    node_id = peer.node_id,
                    hostname = host,
                    "retired a dead engine's registry record"
                );
                retired.push(peer.node_id);
            }
            Err(e) => tracing::info!(pod, node_id = peer.node_id, hostname = host,
                error = %e.message, "a dead engine's registry record stays for now"),
        }
    }
    Ok(retired)
}

/// One pool the worker can purge now.
pub struct PurgePool {
    /// Its controller-owned engine pod.
    pub pod: String,
    /// Its unit (`<pool>[-shard-<k>]`).
    pub unit: String,
    pub client: Arc<dyn ControlClient>,
}

/// What the worker needs of the cluster ([`crate::engine_pods::EnginePodManager`]
/// in a cluster, a fake in the tests).
#[async_trait]
pub trait PurgeBackend: Send + Sync {
    /// Every pool with a volume or with trash, as the cluster knows them
    /// (module docs: its PVs, the pool records, the controller-owned pods
    /// that are up), each through its controller-owned engine pod —
    /// brought up first when it has none. A pool whose pod cannot be
    /// reached now is left out (logged) and tried again next pass.
    /// `skip`: pods whose pool's pass is still running (neither brought up
    /// nor attached).
    async fn pools(&self, skip: &HashSet<String>) -> Result<Vec<PurgePool>, ControlError>;
    /// The names of the cluster's nodes (the registry sweep's liveness
    /// evidence).
    async fn node_names(&self) -> Result<HashSet<String>, ControlError>;
    /// Pod `pod`'s pool has been empty for a while: unless an RPC of any
    /// controller replica holds the pod, or its pool turns out not to be
    /// empty after all (checked again once the pod is marked retiring),
    /// leave the registry, delete it and drop its pool's record. Whether
    /// it went.
    async fn reap(&self, pod: &str) -> Result<bool, ControlError>;
    /// Replace pod `pod` when its engine settings drifted (and no RPC of
    /// any replica holds it). Whether it did.
    async fn roll_if_drifted(&self, pod: &str) -> Result<bool, ControlError>;
}

/// The worker: a pass every interval while this replica leads.
pub struct PurgeWorker {
    backend: Arc<dyn PurgeBackend>,
    cfg: PurgeConfig,
    /// Pools whose pass is still running (a slow pool is not started twice).
    running: Arc<Mutex<HashSet<String>>>,
    /// Pool pod → since when its passes have found it empty.
    empty_since: Arc<Mutex<HashMap<String, Instant>>>,
    /// How long a pool must have been found empty, over at least two
    /// passes, before its pod is reaped: a `CreateVolume` finds the new
    /// pool empty too, between its first and its next call into the pod.
    reap_after: Duration,
}

impl PurgeWorker {
    pub fn new(backend: Arc<dyn PurgeBackend>, cfg: PurgeConfig) -> PurgeWorker {
        let reap_after = cfg
            .interval
            .unwrap_or_default()
            .max(Duration::from_secs(60));
        PurgeWorker {
            backend,
            cfg,
            running: Arc::default(),
            empty_since: Arc::default(),
            reap_after,
        }
    }

    /// Reap an empty pool's pod once it has been empty this long (and over
    /// two passes at least).
    pub fn reap_after(mut self, after: Duration) -> PurgeWorker {
        self.reap_after = after;
        self
    }

    /// One tick: start a pass for every reachable pool not already being
    /// purged. Returns the passes' handles (tests join them).
    pub async fn tick(
        &self,
        leading: Arc<Leadership>,
    ) -> Vec<tokio::task::JoinHandle<Option<PassReport>>> {
        let busy = self.running.lock().unwrap().clone();
        let pools = match self.backend.pools(&busy).await {
            Ok(pools) => pools,
            Err(e) => {
                tracing::warn!(error = %e.message, "listing the pools to purge");
                return Vec::new();
            }
        };
        let names = match self.backend.node_names().await {
            Ok(names) => Some(Arc::new(names)),
            Err(e) => {
                tracing::warn!(error = %e.message, "listing nodes; no registry sweep this pass");
                None
            }
        };
        let mut handles = Vec::new();
        for pool in pools {
            if !self.running.lock().unwrap().insert(pool.pod.clone()) {
                continue;
            }
            let backend = self.backend.clone();
            let cfg = self.cfg.clone();
            let running = self.running.clone();
            let leading = leading.clone();
            let names = names.clone();
            let empty_since = self.empty_since.clone();
            let reap_after = self.reap_after;
            handles.push(tokio::spawn(async move {
                let pod = pool.pod.clone();
                let stop = Arc::new(AtomicBool::new(false));
                // Leadership lost mid-pass ends the walk at its next op.
                let watcher = {
                    let (stop, leading) = (stop.clone(), leading.clone());
                    tokio::spawn(async move {
                        while leading.is_leading() {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        stop.store(true, Ordering::SeqCst);
                    })
                };
                let report = pass(
                    pool,
                    backend.as_ref(),
                    &cfg,
                    &stop,
                    names.as_deref(),
                    (&empty_since, reap_after),
                )
                .await;
                watcher.abort();
                running.lock().unwrap().remove(&pod);
                report
            }));
        }
        handles
    }

    /// Run until the process ends: a tick every interval while `leading`
    /// says this replica leads.
    pub fn spawn(self, leading: Arc<Leadership>) {
        let Some(interval) = self.cfg.interval else {
            return;
        };
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if leading.is_leading() {
                    drop(self.tick(leading.clone()).await);
                }
            }
        });
    }
}

/// One pool's pass, then its sweep and its reap or roll (module docs).
async fn pass(
    pool: PurgePool,
    backend: &dyn PurgeBackend,
    cfg: &PurgeConfig,
    stop: &AtomicBool,
    nodes: Option<&HashSet<String>>,
    (empty_since, reap_after): (&Mutex<HashMap<String, Instant>>, Duration),
) -> Option<PassReport> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let report = match purge_pool(&pool.pod, pool.client.as_ref(), cfg, now_ms, stop).await {
        Ok(report) => report,
        Err(e) => {
            tracing::warn!(pod = %pool.pod, error = %e.message, "purge pass failed");
            return None;
        }
    };
    if stop.load(Ordering::SeqCst) {
        return Some(report);
    }
    if let Some(nodes) = nodes {
        if let Err(e) = sweep_registry(pool.client.as_ref(), &pool.pod, nodes).await {
            tracing::info!(pod = %pool.pod, error = %e.message, "registry sweep skipped");
        }
    }
    // Every client into the pod must be gone before a reap or a roll,
    // which refuse a pod an RPC holds a client into.
    let PurgePool { pod, client, .. } = pool;
    drop(client);
    let due = {
        let mut empty = empty_since.lock().unwrap();
        if report.empty {
            match empty.get(&pod) {
                Some(since) => since.elapsed() >= reap_after,
                None => {
                    empty.insert(pod.clone(), Instant::now());
                    false
                }
            }
        } else {
            empty.remove(&pod);
            false
        }
    };
    if due {
        match backend.reap(&pod).await {
            Ok(true) => {
                empty_since.lock().unwrap().remove(&pod);
                tracing::info!(pod, "reaped the engine pod of an empty pool")
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(pod, error = %e.message, "reaping an empty pool's pod"),
        }
    } else if !report.empty {
        if let Err(e) = backend.roll_if_drifted(&pod).await {
            tracing::warn!(pod, error = %e.message, "replacing a drifted controller-owned pod");
        }
    }
    Some(report)
}

/// Whether this replica leads the purge now: until when the start of
/// its last successful lease write lets it ([`Leader`]). Read at the
/// moment of use, so a lease write that hangs ends the leadership on time
/// whatever the renewing task is doing.
#[derive(Debug, Default)]
pub struct Leadership {
    until: Mutex<Option<Instant>>,
}

impl Leadership {
    /// A leadership that never ends (tests, and a worker without rivals).
    pub fn always() -> Arc<Leadership> {
        Self::until(Instant::now() + Duration::from_secs(10 * 365 * 86_400))
    }

    /// A leadership that ends at `until`.
    pub fn until(until: Instant) -> Arc<Leadership> {
        Arc::new(Leadership {
            until: Mutex::new(Some(until)),
        })
    }

    pub fn is_leading(&self) -> bool {
        self.until
            .lock()
            .unwrap()
            .is_some_and(|until| Instant::now() < until)
    }

    /// Lead until `until` (`None`: stop now).
    pub fn set_until(&self, until: Option<Instant>) {
        *self.until.lock().unwrap() = until;
    }
}

/// The purge lease's timing: the chart's `controller.leaderElection.*`,
/// the same as the sidecars' (client-go's meaning).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderTiming {
    /// How long a rival waits, from when it first sees a record that then
    /// does not change, before it takes the lease.
    pub lease: Duration,
    /// How long this replica counts itself the leader after the *start* of
    /// its last successful renewal: less than `lease`, so it stops before
    /// any rival may take over.
    pub renew_deadline: Duration,
    /// Between attempts; also each API call's timeout.
    pub retry: Duration,
}

impl Default for LeaderTiming {
    /// The chart's defaults: 15 s, 10 s, 5 s.
    fn default() -> Self {
        LeaderTiming {
            lease: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry: Duration::from_secs(5),
        }
    }
}

impl LeaderTiming {
    /// `CONSTELLATION_CSI_LEADER_LEASE_DURATION`,
    /// `CONSTELLATION_CSI_LEADER_RENEW_DEADLINE`,
    /// `CONSTELLATION_CSI_LEADER_RETRY_PERIOD` (durations; unset: the
    /// default). Refused unless lease > renew deadline > retry period.
    pub fn from_env() -> Result<LeaderTiming, String> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<LeaderTiming, String> {
        let mut t = LeaderTiming::default();
        for (key, field) in [
            ("CONSTELLATION_CSI_LEADER_LEASE_DURATION", &mut t.lease),
            (
                "CONSTELLATION_CSI_LEADER_RENEW_DEADLINE",
                &mut t.renew_deadline,
            ),
            ("CONSTELLATION_CSI_LEADER_RETRY_PERIOD", &mut t.retry),
        ] {
            if let Some(v) = var(key).filter(|v| !v.trim().is_empty()) {
                *field = crate::params::parse_duration(&v).map_err(|e| format!("{key}: {e}"))?;
            }
        }
        if !(t.lease > t.renew_deadline && t.renew_deadline > t.retry && !t.retry.is_zero()) {
            return Err(format!(
                "leader election needs leaseDuration > renewDeadline > retryPeriod > 0, not \
                 {:?} / {:?} / {:?}",
                t.lease, t.renew_deadline, t.retry
            ));
        }
        Ok(t)
    }
}

/// Whether a replica named `me` may write the lease whose record says
/// `holder` (empty: nobody) for `duration`, a record it has seen
/// unchanged since `seen_since` (client-go's rule: expiry by this
/// replica's own clock since the record last changed, never by the
/// holder's timestamps, so no clock skew between replicas matters).
pub fn may_take(
    holder: Option<&str>,
    me: &str,
    duration: Duration,
    seen_since: Instant,
    now: Instant,
) -> bool {
    match holder.filter(|h| !h.is_empty()) {
        None => true,
        Some(h) if h == me => true,
        Some(_) => now.saturating_duration_since(seen_since) >= duration,
    }
}

/// The purge lease (`coordination.k8s.io/v1` `Lease` `constellation-csi-purge`
/// in the driver's namespace): exactly one controller replica purges at a
/// time. The sidecars' leader election is theirs; this binary holds its
/// own, with their timing ([`LeaderTiming`], client-go's rules): every API
/// call is bounded by the retry period, a replica leads until
/// `renew_deadline` after the start of its last successful write, and a
/// rival takes the lease only once it has seen the record unchanged for
/// the lease duration by its own clock.
pub struct Leader {
    leading: Arc<Leadership>,
}

impl Leader {
    pub const LEASE: &'static str = "constellation-csi-purge";

    /// Start contending for the lease as `identity`.
    pub fn spawn(
        client: kube::Client,
        namespace: &str,
        identity: String,
        timing: LeaderTiming,
    ) -> Leader {
        use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
        use kube::api::{Api, PostParams};
        let leading = Arc::new(Leadership::default());
        let flag = leading.clone();
        let api: Api<Lease> = Api::namespaced(client, namespace);
        tokio::spawn(async move {
            // The record last seen (its resourceVersion) and since when.
            let mut seen: Option<(String, Instant)> = None;
            // When the last successful write started.
            let mut renewed: Option<Instant> = None;
            loop {
                let attempt = Instant::now();
                let now = k8s_openapi::jiff::Timestamp::now();
                let spec = |transitions: i32, acquired: Option<MicroTime>| LeaseSpec {
                    holder_identity: Some(identity.clone()),
                    lease_duration_seconds: Some(timing.lease.as_secs().max(1) as i32),
                    acquire_time: acquired.or(Some(MicroTime(now))),
                    renew_time: Some(MicroTime(now)),
                    lease_transitions: Some(transitions),
                    ..Default::default()
                };
                let written =
                    match tokio::time::timeout(timing.retry, api.get_opt(Self::LEASE)).await {
                        Ok(Ok(None)) => {
                            let lease = Lease {
                                metadata: ObjectMeta {
                                    name: Some(Self::LEASE.into()),
                                    ..Default::default()
                                },
                                spec: Some(spec(0, None)),
                            };
                            matches!(
                                tokio::time::timeout(
                                    timing.retry,
                                    api.create(&PostParams::default(), &lease)
                                )
                                .await,
                                Ok(Ok(_))
                            )
                        }
                        Ok(Ok(Some(mut lease))) => {
                            let rv = lease.metadata.resource_version.clone().unwrap_or_default();
                            let since = match &seen {
                                Some((v, at)) if *v == rv => *at,
                                _ => {
                                    seen = Some((rv, attempt));
                                    attempt
                                }
                            };
                            let current = lease.spec.clone().unwrap_or_default();
                            let holder = current.holder_identity.as_deref();
                            let mine = holder == Some(identity.as_str());
                            let duration = current
                                .lease_duration_seconds
                                .filter(|s| *s > 0)
                                .map(|s| Duration::from_secs(s as u64))
                                .unwrap_or(timing.lease);
                            if may_take(holder, &identity, duration, since, Instant::now()) {
                                let transitions = current.lease_transitions.unwrap_or(0)
                                    + if mine { 0 } else { 1 };
                                let acquired = if mine { current.acquire_time } else { None };
                                lease.spec = Some(spec(transitions, acquired));
                                // The resourceVersion read above makes this a
                                // compare-and-swap: two replicas taking an
                                // expired lease at once, one wins.
                                matches!(
                                    tokio::time::timeout(
                                        timing.retry,
                                        api.replace(Self::LEASE, &PostParams::default(), &lease)
                                    )
                                    .await,
                                    Ok(Ok(_))
                                )
                            } else {
                                false
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::debug!(error = %e, "reading the purge lease");
                            false
                        }
                        Err(_) => {
                            tracing::debug!("reading the purge lease timed out");
                            false
                        }
                    };
                if written {
                    if renewed.is_none() {
                        tracing::info!(identity, "leading the purge worker");
                    }
                    renewed = Some(attempt);
                }
                let until = renewed
                    .map(|t| t + timing.renew_deadline)
                    .filter(|until| Instant::now() < *until);
                if until.is_none() && renewed.is_some() {
                    tracing::info!(identity, "no longer leading the purge worker");
                    renewed = None;
                }
                flag.set_until(until);
                tokio::time::sleep(timing.retry).await;
            }
        });
        Leader { leading }
    }

    /// Whether this replica leads now (shared with the worker).
    pub fn flag(&self) -> Arc<Leadership> {
        self.leading.clone()
    }
}

/// A fake pool: (pod, unit, its engine).
pub type FakePool = (String, String, Arc<dyn ControlClient>);

/// A [`PurgeBackend`] over fixed pools (tests).
#[derive(Default)]
pub struct FakePurgeBackend {
    pub pools: Mutex<Vec<FakePool>>,
    pub nodes: Mutex<HashSet<String>>,
    pub reaped: Mutex<Vec<String>>,
    pub rolled: Mutex<Vec<String>>,
    /// Pods whose settings drifted.
    pub drifted: Mutex<HashMap<String, bool>>,
}

#[async_trait]
impl PurgeBackend for FakePurgeBackend {
    async fn pools(&self, skip: &HashSet<String>) -> Result<Vec<PurgePool>, ControlError> {
        Ok(self
            .pools
            .lock()
            .unwrap()
            .iter()
            .filter(|(pod, _, _)| !self.reaped.lock().unwrap().contains(pod) && !skip.contains(pod))
            .map(|(pod, unit, client)| PurgePool {
                pod: pod.clone(),
                unit: unit.clone(),
                client: client.clone(),
            })
            .collect())
    }
    async fn node_names(&self) -> Result<HashSet<String>, ControlError> {
        Ok(self.nodes.lock().unwrap().clone())
    }
    async fn reap(&self, pod: &str) -> Result<bool, ControlError> {
        self.reaped.lock().unwrap().push(pod.to_string());
        Ok(true)
    }
    async fn roll_if_drifted(&self, pod: &str) -> Result<bool, ControlError> {
        if self.drifted.lock().unwrap().remove(pod).unwrap_or(false) {
            self.rolled.lock().unwrap().push(pod.to_string());
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_client::InMemoryControl;

    fn cfg() -> PurgeConfig {
        PurgeConfig {
            interval: Some(Duration::from_secs(1)),
            grace: Duration::from_secs(60),
            concurrency: 4,
            ops_per_s: 0,
            bytes_per_s: 0,
        }
    }

    /// A trashed volume of `dirs` directories × `files` files of `size`.
    fn trash(fs: &InMemoryControl, name: &str, dirs: usize, files: usize, size: u64) {
        fs.plant_dir(&format!("/.trash/{name}"));
        for d in 0..dirs {
            for f in 0..files {
                fs.plant_file(&format!("/.trash/{name}/d{d}/f{f}"), size);
            }
        }
    }

    #[test]
    fn trash_names_carry_their_time() {
        assert_eq!(trashed_at_ms("pvc-1-1790000000000"), Some(1790000000000));
        assert_eq!(trashed_at_ms("pvc-a-b-17"), Some(17));
        assert_eq!(trashed_at_ms("lost+found"), None);
        assert_eq!(trashed_at_ms("x"), None);
    }

    #[test]
    fn settings_come_from_the_environment() {
        let vars: HashMap<&str, &str> = HashMap::from([
            ("CONSTELLATION_CSI_PURGE_INTERVAL", "10s"),
            ("CONSTELLATION_CSI_PURGE_GRACE", "2m"),
            ("CONSTELLATION_CSI_PURGE_MAX_CONCURRENT_DELETES", "8"),
            ("CONSTELLATION_CSI_PURGE_OPS_PER_SECOND", "1000"),
            ("CONSTELLATION_CSI_PURGE_BYTES_PER_SECOND", "64Mi"),
        ]);
        let cfg = PurgeConfig::from_vars(|k| vars.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(cfg.interval, Some(Duration::from_secs(10)));
        assert_eq!(cfg.grace, Duration::from_secs(120));
        assert_eq!(cfg.concurrency, 8);
        assert_eq!(cfg.ops_per_s, 1000);
        assert_eq!(cfg.bytes_per_s, 64 << 20);
        let off = PurgeConfig::from_vars(|k| {
            (k == "CONSTELLATION_CSI_PURGE_INTERVAL").then(|| "0".into())
        })
        .unwrap();
        assert_eq!(off.interval, None);
        assert_eq!(
            PurgeConfig::from_vars(|_| None).unwrap(),
            PurgeConfig::default()
        );
        assert!(PurgeConfig::from_vars(
            |k| (k == "CONSTELLATION_CSI_PURGE_GRACE").then(|| "soon".into())
        )
        .is_err());
    }

    #[tokio::test]
    async fn old_entries_go_whole_young_ones_wait() {
        let fs = InMemoryControl::default();
        let now = 1_000_000_000u64;
        trash(&fs, &format!("old-{}", now - 120_000), 3, 5, 10);
        trash(&fs, &format!("young-{}", now - 1_000), 1, 1, 10);
        fs.plant_dir("/volumes/live");
        fs.plant_file("/volumes/live/keep", 7);
        let stop = AtomicBool::new(false);
        let report = purge_pool("p", &fs, &cfg(), now, &stop).await.unwrap();
        assert_eq!(report.purged.len(), 1);
        let (path, entry) = &report.purged[0];
        assert_eq!(path, &format!("/.trash/old-{}", now - 120_000));
        assert_eq!((entry.files, entry.dirs, entry.bytes), (15, 4, 150));
        assert_eq!(entry.ops, 19);
        assert_eq!(report.waiting, 1);
        assert!(!report.empty);
        assert!(!fs.exists(path));
        assert!(fs.exists(&format!("/.trash/young-{}", now - 1_000)));
        assert!(
            fs.exists("/volumes/live/keep"),
            "live volumes are never touched"
        );
        // Every file went by itself: no recursive delete.
        assert_eq!(fs.deletes(), 19);
    }

    #[tokio::test]
    async fn a_failed_pass_is_resumed_by_the_next() {
        let fs = InMemoryControl::default();
        let now = 5_000_000u64;
        let name = format!("pvc-x-{}", now - 3_600_000);
        trash(&fs, &name, 4, 10, 1);
        let stop = AtomicBool::new(false);
        let mut config = cfg();
        config.concurrency = 1;
        // The relay breaks after 12 deletes: the entry stays, smaller.
        fs.allow_deletes(12);
        let r = purge_pool("p", &fs, &config, now, &stop).await.unwrap();
        assert_eq!(r.failed, 1);
        assert!(r.purged.is_empty());
        assert!(!r.empty);
        assert_eq!(fs.deletes(), 12);
        assert!(
            fs.exists(&format!("/.trash/{name}")),
            "partly gone, not whole"
        );
        // The next pass (a new leader, say) re-lists and finishes it.
        fs.allow_deletes(u64::MAX);
        let r = purge_pool("p", &fs, &config, now, &stop).await.unwrap();
        assert_eq!(r.purged.len(), 1);
        // 40 files and 5 directories in all, 12 of them removed before.
        assert_eq!(r.purged[0].1.ops + 12, 45, "{:?}", r.purged[0].1);
        assert!(r.empty, "trash and volumes are both empty now");
        assert!(fs.children("/.trash").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn the_pace_holds_both_budgets() {
        let fs = InMemoryControl::default();
        let now = 10_000_000u64;
        // 40 files of 1 MiB in one directory: by ops alone (100/s) 0.41 s;
        // by bytes (8 MiB/s) about 4.9 s — the bytes budget rules.
        trash(&fs, &format!("big-{}", now - 600_000), 1, 40, 1 << 20);
        let mut config = cfg();
        config.ops_per_s = 100;
        config.bytes_per_s = 8 << 20;
        let stop = AtomicBool::new(false);
        let t0 = tokio::time::Instant::now();
        let r = purge_pool("p", &fs, &config, now, &stop).await.unwrap();
        let took = t0.elapsed();
        assert_eq!(r.purged.len(), 1);
        assert!(took >= Duration::from_millis(4800), "{took:?}");
        assert!(took <= Duration::from_millis(5300), "{took:?}");
        // Many small files: the ops budget rules.
        trash(&fs, &format!("many-{}", now - 600_000), 2, 100, 1);
        let t0 = tokio::time::Instant::now();
        let r = purge_pool("p", &fs, &config, now, &stop).await.unwrap();
        let took = t0.elapsed();
        assert_eq!(r.purged[0].1.ops, 203);
        assert!(took >= Duration::from_millis(2000), "{took:?}");
        assert!(took <= Duration::from_millis(2200), "{took:?}");
    }

    /// One budget idling while the other rules banks no credit for it: 20
    /// big files (bytes rule, 2.4 s) then 200 empty ones in the same pass
    /// still take their 2 s by the ops budget.
    #[tokio::test(start_paused = true)]
    async fn an_idle_budget_banks_no_credit() {
        let mut config = cfg();
        config.ops_per_s = 100;
        config.bytes_per_s = 8 << 20;
        let pace = Pace::new(&config);
        for _ in 0..20 {
            pace.charge(1 << 20).await;
        }
        let t0 = tokio::time::Instant::now();
        for _ in 0..200 {
            pace.charge(0).await;
        }
        let took = t0.elapsed();
        assert!(took >= Duration::from_millis(1990), "{took:?}");
        assert!(took <= Duration::from_millis(2200), "{took:?}");
    }

    #[tokio::test]
    async fn the_sweep_retires_dead_incarnations_and_vanished_nodes_only() {
        use crate::engine_pods::{node_engine_hostname, pod_hostname};
        let fs = InMemoryControl::default();
        // Over 63 bytes: its hostname is the name cut short.
        let pod = "constellation-engine-csi-node-drain-d6123c0dab-controller";
        fs.set_node_id(7);
        fs.plant_peer(3, &pod_hostname(pod)); // an earlier incarnation of this pod
        fs.plant_peer(7, &pod_hostname(pod)); // itself (never)
        fs.plant_peer(4, &node_engine_hostname("w1")); // node alive
        fs.plant_peer(5, &node_engine_hostname("gone")); // node gone
        fs.plant_peer(6, "laptop"); // a human's mount (settled decision 18)
                                    // A node pod's name cut to a hostname (what fooled an early sweep).
        fs.plant_peer(
            8,
            "constellation-engine-csi-node-drain-d6123c0dab-kind-37-k6b-work",
        );
        // A human's mount merely named like a node engine (no hash).
        fs.plant_peer(9, "csi-node-my-laptop");
        let nodes = HashSet::from(["w1".to_string(), "w2".to_string()]);
        let retired = sweep_registry(&fs, pod, &nodes).await.unwrap();
        assert_eq!(retired, vec![3, 5]);
        assert_eq!(fs.leaves(), vec![Some(3), Some(5)]);
        for node in [
            "w1",
            "a.very-long.node.name.example.internal.cluster.local",
            "--",
        ] {
            assert!(
                is_node_engine_hostname(&node_engine_hostname(node)),
                "{node}"
            );
        }
        assert!(!is_node_engine_hostname("csi-node-my-laptop"));
        assert!(!is_node_engine_hostname("csi-node-Box-0123456789"));
    }

    #[tokio::test]
    async fn a_tick_purges_every_pool_reaps_the_empty_and_rolls_the_drifted() {
        let backend = Arc::new(FakePurgeBackend::default());
        let a = Arc::new(InMemoryControl::default());
        let b = Arc::new(InMemoryControl::default());
        let old = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 3_600_000;
        trash(&a, &format!("v1-{old}"), 1, 3, 1);
        trash(&b, &format!("v2-{old}"), 1, 3, 1);
        b.plant_dir("/volumes/still-here");
        backend.pools.lock().unwrap().extend([
            (
                "pa".to_string(),
                "ua".to_string(),
                a.clone() as Arc<dyn ControlClient>,
            ),
            (
                "pb".to_string(),
                "ub".to_string(),
                b.clone() as Arc<dyn ControlClient>,
            ),
        ]);
        backend.drifted.lock().unwrap().insert("pb".into(), true);
        let worker = PurgeWorker::new(backend.clone(), cfg()).reap_after(Duration::from_millis(50));
        let leading = Leadership::always();
        for h in worker.tick(leading.clone()).await {
            h.await.unwrap().unwrap();
        }
        assert!(a.children("/.trash").is_empty());
        assert!(b.children("/.trash").is_empty());
        assert_eq!(*backend.rolled.lock().unwrap(), vec!["pb".to_string()]);
        // Empty at one pass is not enough to reap (a new pool is empty
        // too); empty for long enough over two passes is.
        assert!(backend.reaped.lock().unwrap().is_empty());
        tokio::time::sleep(Duration::from_millis(60)).await;
        for h in worker.tick(leading.clone()).await {
            h.await.unwrap().unwrap();
        }
        assert_eq!(*backend.reaped.lock().unwrap(), vec!["pa".to_string()]);
    }

    #[tokio::test]
    async fn a_lost_lease_stops_the_walk() {
        let fs = InMemoryControl::default();
        let now = 10_000_000u64;
        trash(&fs, &format!("v-{}", now - 600_000), 3, 3, 1);
        let stop = AtomicBool::new(true);
        let r = purge_pool("p", &fs, &cfg(), now, &stop).await.unwrap();
        assert!(r.purged.is_empty());
        assert!(!r.empty);
        assert_eq!(fs.deletes(), 0);
    }

    /// Only `EOVERFLOW` (a listing over one frame) earns the recursive
    /// fallback; any other listing failure leaves the entry for the next
    /// pass, untouched.
    #[tokio::test]
    async fn only_an_overflowing_listing_is_deleted_whole() {
        let now = 10_000_000u64;
        let entry = format!("/.trash/v-{}", now - 600_000);
        for (error, recursive) in [
            (ControlError::from(Code::Overflow), true),
            (
                ControlError::failed("meta store: transient read error"),
                false,
            ),
            (ControlError::from(Code::Io), false),
        ] {
            let fs = InMemoryControl::default();
            trash(&fs, &format!("v-{}", now - 600_000), 2, 5, 1);
            fs.fail_next_readdir(&format!("{entry}/d1"), error.clone());
            let stop = AtomicBool::new(false);
            let r = purge_pool("p", &fs, &cfg(), now, &stop).await.unwrap();
            if recursive {
                assert_eq!(r.purged.len(), 1, "{error:?}");
                assert_eq!(r.purged[0].1.fallbacks, 1);
                assert!(!fs.exists(&entry));
            } else {
                assert_eq!((r.purged.len(), r.failed), (0, 1), "{error:?}");
                assert!(
                    fs.exists(&format!("{entry}/d1/f0")),
                    "nothing under the unlistable directory went: {error:?}"
                );
                // The next pass, the listing works again: done, file by file.
                let r = purge_pool("p", &fs, &cfg(), now, &stop).await.unwrap();
                assert_eq!(r.purged.len(), 1);
                assert_eq!(r.purged[0].1.fallbacks, 0);
                assert!(!fs.exists(&entry));
            }
        }
    }

    /// client-go's rule: a rival's record is taken only once this replica
    /// has seen it unchanged for the lease duration, by its own clock.
    #[test]
    fn a_lease_is_taken_only_once_seen_unchanged_for_its_duration() {
        let t0 = Instant::now();
        let d = Duration::from_secs(15);
        assert!(may_take(None, "a", d, t0, t0));
        assert!(may_take(Some(""), "a", d, t0, t0));
        assert!(may_take(Some("a"), "a", d, t0, t0));
        assert!(!may_take(Some("b"), "a", d, t0, t0));
        assert!(!may_take(
            Some("b"),
            "a",
            d,
            t0,
            t0 + Duration::from_secs(14)
        ));
        assert!(may_take(Some("b"), "a", d, t0, t0 + d));
    }

    #[test]
    fn leadership_ends_on_time_without_its_renewer() {
        let l = Leadership::until(Instant::now() + Duration::from_millis(50));
        assert!(l.is_leading());
        std::thread::sleep(Duration::from_millis(60));
        assert!(!l.is_leading(), "a hung renewal must not keep it leading");
        assert!(!Leadership::default().is_leading());
        l.set_until(Some(Instant::now() + Duration::from_secs(5)));
        assert!(l.is_leading());
        l.set_until(None);
        assert!(!l.is_leading());
    }

    #[test]
    fn leader_timing_comes_from_the_chart() {
        let vars: HashMap<&str, &str> = HashMap::from([
            ("CONSTELLATION_CSI_LEADER_LEASE_DURATION", "30s"),
            ("CONSTELLATION_CSI_LEADER_RENEW_DEADLINE", "20s"),
            ("CONSTELLATION_CSI_LEADER_RETRY_PERIOD", "4s"),
        ]);
        let t = LeaderTiming::from_vars(|k| vars.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(
            (t.lease, t.renew_deadline, t.retry),
            (
                Duration::from_secs(30),
                Duration::from_secs(20),
                Duration::from_secs(4)
            )
        );
        assert_eq!(
            LeaderTiming::from_vars(|_| None).unwrap(),
            LeaderTiming::default()
        );
        assert!(LeaderTiming::from_vars(|k| {
            (k == "CONSTELLATION_CSI_LEADER_RENEW_DEADLINE").then(|| "20s".into())
        })
        .is_err());
    }
}
