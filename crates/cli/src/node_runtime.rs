//! `NodeRuntime`: the per-node (per state-dir) daemon state, shared by
//! every mounted view.
//!
//! This is a structural extraction of the old monolithic `mount()`
//! function (see `main.rs` history) into two layers:
//!
//!   - **Per-node** (`NodeRuntime::start`): backend/replica/cache open,
//!     node identity, lease keeper, P2P endpoint, the periodic GC task,
//!     the metadata shipper/sync task, and (today, single-view only)
//!     the control socket + web UI.
//!   - **Per-view** (`NodeRuntime::add_mount`): selector/clone
//!     resolution, the `FuseFs` instance, and the `fuser::Session` for
//!     one mountpoint, run on its own dedicated OS thread.
//!
//! Plan 21 step 0 keeps this an *inert* refactor: today only one view is
//! ever mounted (the CLI's `mount` command calls `add_mount` exactly
//! once and blocks until that view's session ends), and the
//! control-socket wire format is untouched. `DaemonStatus` still reports
//! a single `mountpoint`, so it — and starting the control API/web UI —
//! is built lazily, the first time a view is added, rather than inside
//! `start()` itself; multi-view `StatusReport`/`MountAdd`/`MountList`
//! wiring is Step 1's job, not this one's.
//!
//! Even though only one view exists today, `remove_mount` is written to
//! be correct for several: it unmounts exactly the requested view (via
//! its `SessionUnmounter`) and joins that view's thread, leaving
//! siblings untouched. Every view's session thread performs its own
//! per-view teardown (ephemeral clone removal) and, if it happens to be
//! the last view standing, triggers `NodeRuntime::shutdown` itself —
//! this is what makes an externally-triggered unmount (a bare
//! `fusermount -u`, a kernel-forced unmount, or a crash) behave the same
//! as an explicit `remove_mount` call.

use anyhow::{bail, Context, Result};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::Meta;
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{
    coop, designation, epoch, forward, fusefs, lease, log_buffer, pin, placement, reintegrate,
    shipper, snapshot, staging, writeback,
};

/// Detach a stale FUSE mount left behind by a previous daemon that exited
/// without unmounting (crash, `kill`, or an orphaned view). Such a
/// mountpoint answers `stat` with `ENOTCONN`; if we don't clear it first,
/// building a fresh FUSE session over it fails with the same "Transport
/// endpoint is not connected". Best-effort and quiet on the common case
/// (no stale mount): only acts when the path actually reports `ENOTCONN`.
fn clear_stale_mount(mountpoint: &std::path::Path) {
    match std::fs::metadata(mountpoint) {
        // A live FUSE mount or an ordinary directory stats fine — leave it.
        Ok(_) => return,
        Err(e) if e.raw_os_error() == Some(libc::ENOTCONN) => {}
        // Anything else (NotFound, permission, …) is not ours to fix here.
        Err(_) => return,
    }
    tracing::warn!(
        ?mountpoint,
        "detaching stale FUSE mount from a previous daemon before remounting"
    );
    // `fusermount3 -uz` (lazy) is the portable way to drop a dead FUSE
    // mount from userspace; fall back to `fusermount` for older systems.
    for bin in ["fusermount3", "fusermount"] {
        let status = std::process::Command::new(bin)
            .args(["-uz", &mountpoint.to_string_lossy()])
            .status();
        if let Ok(s) = status {
            if s.success() {
                return;
            }
        }
    }
    tracing::warn!(
        ?mountpoint,
        "could not detach stale mount automatically; \
         run `fusermount3 -uz <mountpoint>` if the remount fails"
    );
}

/// Env: `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`, milliseconds
/// (default 0 = no delay, and no `tokio::time::sleep` call at all).
///
/// Fault injection only (plan 30 M0) — no other code path reads this.
/// Delays a forwarded mutation's reply on the *holder* side
/// (`SyncRequest::Mutate` below), after the op has already executed and
/// the keepers lock has been released, so the delay races only the
/// requester's own `CONSTELLATION_FORWARD_TIMEOUT_MS` deadline and
/// blocks nothing else on this node. This reproduces bug A
/// (`docs/plans/v1/wip/30-write-path-resilience-and-scale-out.md`
/// §1.1): the requester's forward times out, `request_mutate_with` maps
/// that to `Busy`, `mutate_op_rebasable` falls back to acquiring the
/// lease, and it re-executes locally an op the holder already applied.
///
/// A `SIGSTOP`-based trigger cannot do this deterministically: the
/// holder's `HandOff` arm and a forwarded execution race for the same
/// keepers lock, so freezing the process can freeze the handoff instead
/// of the reply. Read once: this is on the per-forward hot path.
fn fault_forward_reply_delay_ms() -> u64 {
    static DELAY_MS: OnceLock<u64> = OnceLock::new();
    *DELAY_MS.get_or_init(|| {
        std::env::var("CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    })
}

/// Ceiling of the sync task's idle poll backoff, in milliseconds
/// (`CONSTELLATION_SYNC_IDLE_MAX_MS`). `CONSTELLATION_SYNC_INTERVAL_MS`
/// is the floor.
///
/// 10 s rather than the 30 s plan 26 first chose. The ceiling was set high
/// because the idle probe was 16 GETs wide and therefore expensive to
/// repeat; narrowing it to one (`shipper::TAIL_PROBE_IDLE`) makes a 10 s
/// ceiling cost 8,640 requests/node/partition/day against 46,080 for the
/// 30 s/16-wide pairing — 5.3× cheaper *and* three times fresher. With
/// P2P down this is the freshness bound, so it is worth spending the
/// saving on latency rather than banking it.
const SYNC_IDLE_MAX_MS: u64 = 10_000;

/// Everything needed to open/create a node's backend + local state,
/// independent of any particular mounted view.
#[allow(clippy::too_many_arguments)]
pub struct NodeConfig {
    pub s3: String,
    pub state_dir: Option<PathBuf>,
    pub cache_size: u64,
    pub fsync_s3: bool,
    /// Plan 30 §M8: `--cto strict`.
    pub cto_strict: bool,
    /// Plan 30 §M14: `--locks cluster` (`Some(true)`), `--locks local`
    /// (`Some(false)`), or neither (cluster when P2P runs).
    pub locks: Option<bool>,
    pub initial_write_mode: writeback::WriteMode,
    pub read_only_member: bool,
    pub web_ui: u16,
    pub log_buffer: log_buffer::LogBuffer,
    /// Resolved read-time atime mode (plan 20). `Off` by default.
    pub atime_mode: crate::atime::AtimeMode,
    /// E2E passphrase collected in the foreground before daemonizing, so
    /// the setsid'd daemon child never has to prompt on a terminal it no
    /// longer has. `None` falls back to the env var / an interactive
    /// prompt (fine in `--foreground`, or when driven by the env var).
    pub passphrase: Option<zeroize::Zeroizing<String>>,
}

/// Everything needed to mount one view (root, subtree, or snapshot
/// selector) of an already-running `NodeRuntime`.
pub struct ViewConfig {
    /// Raw inner-path / `@snapshot` selector argument, as given on the
    /// command line (`"/"` for the root).
    pub inner_path: String,
    pub mountpoint: PathBuf,
    pub allow_other: bool,
    pub fs_name: String,
    pub fuse_threads: usize,
    pub rw_snapshot: bool,
    pub clone_name: Option<String>,
    pub ephemeral: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MountId(u64);

impl MountId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// A snapshot of one mounted view, for listing (`MountList`, Step 7's
/// `fs list`).
pub struct MountInfo {
    pub id: MountId,
    pub subtree: String,
    pub mountpoint: PathBuf,
    pub since: Instant,
}

struct MountHandle {
    subtree: String,
    mountpoint: PathBuf,
    since: Instant,
    unmounter: Mutex<fuser::SessionUnmounter>,
    /// This view's own quota-cap cache, so a live `SetQuota` can
    /// invalidate every mounted view instead of just the one that
    /// happened to build the shared `DaemonStatus`.
    quota_cache: fusefs::QuotaCache,
}

pub struct NodeRuntime {
    node_id: u64,
    /// Plan 30 §M2: this mount's incarnation (bumped once, before
    /// serving, in `NodeRuntime::new`). Part of every rid this mount
    /// allocates.
    incarnation: u32,
    /// Plan 30 §M2: the next `seq` to allocate within this incarnation.
    /// Shared with every `SyncHandle` this runtime hands out (one per
    /// mounted view) so rid allocation is unique across all of them, not
    /// just within one. Volatile — restarts at 0 every mount; the
    /// incarnation bump is what keeps that safe.
    next_rid_seq: Arc<std::sync::atomic::AtomicU64>,
    fsmeta: FsMeta,
    backend_url: String,
    state_dir: PathBuf,
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    compression: CompressionSetting,
    snapshots: Arc<snapshot::SnapshotManager>,
    staging_dir: PathBuf,
    staging_budget: Arc<staging::StagingBudget>,
    lease_mode: constellation_store_s3::LeaseMode,
    /// The authority core's lease state, mirrored for the FUSE fast path
    /// (`authority_driver`).
    lease: Arc<lease::LeaseView>,
    /// Plan 30 §M11: the delegations this node holds, mirrored for the
    /// FUSE fast path's sibling check.
    delegates: Arc<lease::DelegateView>,
    /// The core's observable state, for `status` and the tickers.
    core_status: Arc<std::sync::Mutex<crate::authority_driver::CoreStatus>>,
    /// Rid seqs the FUSE fast path completed, drained by the driver.
    pending_acks: Arc<std::sync::Mutex<Vec<u64>>>,
    acquire_deadline: Duration,
    write_mode: Arc<writeback::WriteModeState>,
    sync_tx: tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    peers: constellation_net::Peers,
    epochs: Arc<epoch::EpochManager>,
    designations: Arc<designation::DesignationManager>,
    coop: Arc<coop::Coop>,
    upload: Arc<crate::UploadRuntime>,
    forward: Arc<forward::ForwardState>,
    placement: Arc<placement::Placement>,
    departed: Arc<AtomicBool>,
    /// Node-level read-time atime accumulator + counters (plan 20),
    /// shared by every view's FUSE fs and drained by the flush ticker.
    atime: Arc<crate::atime::AtimeAccumulator>,
    /// Node-level prune counters (plan 22), shared with the pruner task
    /// and the control plane.
    prune_stats: Arc<crate::prune::PruneStats>,
    /// Unix-ms heartbeat of the sync task's last loop pass; the pruner's
    /// replica-freshness gate (plan 22, Step 4.2) reads it.
    last_sync_ms: Arc<AtomicU64>,
    read_only_member: bool,
    fsync_s3: bool,
    cto_strict: bool,
    /// Plan 30 §M14: the effective `--locks` mode, and every view's flush
    /// for a recalled grant.
    locks_cluster: bool,
    lock_flushers: Arc<crate::locks::LockFlushers>,
    pins: Arc<pin::PinManager>,
    reintegration: Arc<reintegrate::ReintegrationState>,
    stop: Arc<AtomicBool>,
    web_ui: u16,
    log_buffer: log_buffer::LogBuffer,
    rt: tokio::runtime::Handle,
    started: Instant,

    /// Built lazily, the first time a view is added (see module docs).
    status: Mutex<Option<Arc<crate::DaemonStatus>>>,

    mounts: Mutex<HashMap<MountId, MountHandle>>,
    /// Session-thread handles, kept separate from `mounts` so a thread's
    /// own teardown (which removes its `mounts` entry) never races
    /// `remove_mount`'s attempt to join it.
    threads: Mutex<HashMap<MountId, std::thread::JoinHandle<()>>>,
    next_mount_id: AtomicU64,
    /// Plan 30 §M7: drops the kernel's cached view of what another node's
    /// writes changed (`kernel_inval`); `None` when
    /// `CONSTELLATION_KERNEL_INVALIDATE=0`.
    kernel_inval: Option<crate::kernel_inval::KernelInvalidator>,
    shutdown_started: AtomicBool,
    /// Why the node-wide shutdown could not ship everything, when it
    /// could not: the process must then exit non-zero (see
    /// [`NodeRuntime::shutdown`]).
    shutdown_error: Mutex<Option<String>>,
}

impl NodeRuntime {
    /// Plan 30 §M8: this node's mounts are `--cto strict`.
    pub fn cto_strict(&self) -> bool {
        self.cto_strict
    }

    /// Per-node setup: open the backend/replica/cache, claim or validate
    /// node identity, start the lease keeper, P2P endpoint, periodic GC,
    /// and the metadata shipper/sync task. No view is mounted yet.
    pub fn start(cfg: NodeConfig, rt: tokio::runtime::Handle) -> Result<Arc<Self>> {
        let NodeConfig {
            s3,
            state_dir,
            cache_size,
            fsync_s3,
            cto_strict,
            locks,
            initial_write_mode,
            read_only_member,
            web_ui,
            log_buffer,
            atime_mode,
            passphrase,
        } = cfg;

        let fault_forward_delay_ms = fault_forward_reply_delay_ms();
        if fault_forward_delay_ms > 0 {
            tracing::warn!(
                "fault injection: delaying forwarded-mutation replies by \
                 {fault_forward_delay_ms} ms (testing only)"
            );
        }

        let (backend, backend_info) = rt
            .block_on(crate::backend::open_backend_described(&s3))
            .context("opening backend")?;
        let fsmeta = rt
            .block_on(crate::backend::load_fs_explained(
                &backend,
                &backend_info,
                None,
            ))
            .context("loading filesystem")?;
        let e2e_keys = if fsmeta.e2e {
            // Prefer the passphrase collected in the foreground before the
            // fork; fall back to the env var / a prompt (works in
            // `--foreground`, where the terminal is still attached).
            let secret = match passphrase {
                Some(secret) => secret,
                None => crate::passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?,
            };
            Some(
                fsmeta
                    .unlock(&secret)
                    .context("unlocking E2E keyring (wrong passphrase?)")?,
            )
        } else {
            None
        };
        let store = Arc::new(match &e2e_keys {
            Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
            None => ChunkStore::new(backend.clone()),
        });
        let state_dir = state_dir.unwrap_or_else(|| crate::default_state_dir(&fsmeta));
        std::fs::create_dir_all(&state_dir)?;
        let log = match &e2e_keys {
            Some(keys) => constellation_store_s3::LogStore::new_e2e(backend, keys.clone()),
            None => constellation_store_s3::LogStore::new(backend),
        };
        let db_path = state_dir.join("meta.db");
        // Fresh node: rebuild the replica from the commit chain plus log
        // replay (or, with no commit yet, a genesis replay of the whole log).
        if !db_path.exists() {
            rt.block_on(shipper::bootstrap(&db_path, &log))
                .context("bootstrapping metadata replica")?;
        }
        let meta = Arc::new(Meta::open(&db_path)?);
        meta.scratch_purge_all()?;
        if matches!(meta.kv_get("left")?.as_deref(), Some("1")) {
            bail!(
                "this state directory has permanently left the cluster \
                 (kv left=1); mount with a fresh --state-dir to re-enroll \
                 under a new node id"
            );
        }
        // Node identity: claim a cluster-unique id on first mount of this
        // state dir; it scopes ino allocation and marks log segment origin.
        let first_mount = meta.kv_get("node_id")?.is_none();
        let node_id: u64 = match meta.kv_get("node_id")? {
            Some(v) => v.parse().context("corrupt node_id in state dir")?,
            None => {
                let id = rt
                    .block_on(constellation_store_s3::claim_node_id(store.inner().clone()))
                    .context("claiming node id")?;
                meta.kv_set("node_id", &id.to_string())?;
                id
            }
        };
        // A remount whose registry record was retired (or deleted) under us
        // must not silently reclaim that id.
        match rt.block_on(constellation_store_s3::get_node(
            store.inner().clone(),
            node_id,
        ))? {
            None => bail!(
                "node {node_id} has no registry record; an operator may have \
                 retired it. Use a fresh --state-dir to claim a new id"
            ),
            Some(info) if info.retired => bail!(
                "node {node_id} is retired in the registry; use a fresh \
                 --state-dir to re-enroll under a new id"
            ),
            Some(_) => {}
        }
        if first_mount {
            rt.block_on(constellation_store_s3::publish_ro(
                store.inner().clone(),
                node_id,
                read_only_member,
            ))
            .context("publishing read-only membership")?;
            meta.kv_set("read_only_member", if read_only_member { "1" } else { "0" })?;
        } else if read_only_member
            != matches!(meta.kv_get("read_only_member")?.as_deref(), Some("1"))
        {
            bail!("--read-only-member is fixed on first mount for this state directory");
        }
        meta.set_node_prefix(node_id)?;
        // Plan 30 §M2: bump this node's incarnation before serving any
        // mutation. This is what keeps a rid unique across a crash: the
        // volatile per-incarnation seq counter (`ForwardState`'s
        // `next_rid_seq`) restarts at 0 every mount, but the persisted
        // incarnation never repeats, so the pair never does either.
        let incarnation = meta.bump_incarnation()?;
        tracing::info!(node_id, incarnation, "node incarnation");
        // Plan 25: drop pending_upload rows that belong to another node's
        // ino prefix (a copied meta.db dropped into an existing state
        // dir). Same-prefix rows stay for crash recovery. On a fresh
        // bootstrap this is a no-op after `clear_pending_uploads`.
        let purged = meta
            .purge_foreign_pending_uploads(node_id)
            .context("purging foreign pending_upload rows")?;
        if purged > 0 {
            tracing::info!(
                purged,
                node_id,
                "dropped foreign pending_upload rows inherited from another node"
            );
        }
        tracing::info!(node_id, "node identity");

        // Mirror the creation-time cap from meta.json into node-local kv.
        // `read_quota` falls back to it only while no replicated `SetQuota`
        // exists, so this never journals, never needs a lease, and cannot
        // resurrect a cap an operator cleared live.
        match fsmeta.max_logical_bytes {
            Some(cap) => {
                meta.kv_set(
                    constellation_meta::store::QUOTA_CREATION_KV_KEY,
                    &cap.to_string(),
                )?;
                tracing::info!(cap, "filesystem quota from meta.json");
            }
            None => meta.kv_del(constellation_meta::store::QUOTA_CREATION_KV_KEY)?,
        }

        // Mount-time staging GC (plan 05a step 6): nothing under
        // `staging/` can be live at mount start. A crash mid-write leaves
        // no orphaned staging bytes because this always runs first.
        let staging_dir = state_dir.join("staging");
        let reclaimed = staging::gc(&staging_dir).context("clearing orphaned staging files")?;
        if reclaimed > 0 {
            tracing::info!(
                bytes = reclaimed,
                "reclaimed orphaned staging bytes (previous crash)"
            );
        }
        let staging_budget_bytes: u64 = std::env::var("CONSTELLATION_STAGING_BUDGET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(cache_size / 4);
        let staging_budget = staging::StagingBudget::new(staging_budget_bytes);

        let cache = Arc::new(match &e2e_keys {
            Some(keys) => {
                DiskCache::open_keyed(state_dir.join("cache"), cache_size, *keys.addressing_key())?
            }
            None => DiskCache::open(state_dir.join("cache"), cache_size)?,
        });
        let compression: CompressionSetting = fsmeta
            .compression
            .parse()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        // Plan 28's metadata tree: one node cache per node, shared by the
        // publisher and by snapshot reads.
        //
        // The hasher has to match the one the disk cache was opened
        // with. On an E2E filesystem node identity, the boundary function
        // and blob addressing are all keyed under the addressing key
        // (§P13), so a plain-hashing reader or writer would look for
        // nodes nothing names and build a tree of a different shape.
        let tree_hasher = match &e2e_keys {
            Some(keys) => constellation_mtree::Hasher::Keyed(*keys.addressing_key()),
            None => constellation_mtree::Hasher::Plain,
        };
        // …and on an E2E filesystem every tree object is also sealed
        // (§P13): pack frames and indices, spilled blobs, commits.
        let tree_sealing = constellation_store_s3::TreeSealing::for_keys(e2e_keys.as_ref());
        let tree_access = snapshot::TreeAccess {
            nodes: Arc::new(constellation_store_s3::NodeCache::new(
                constellation_store_s3::PackStore::new(store.inner().clone())
                    .with_sealing(tree_sealing.clone()),
                cache.clone(),
                tree_hasher,
                rt.clone(),
            )),
            config: constellation_mtree::record::config().with_hasher(tree_hasher),
            blobs: constellation_store_s3::BlobStore::new(store.inner().clone(), tree_hasher)
                .with_sealing(tree_sealing.clone()),
        };
        let snapshots_base =
            snapshot::SnapshotManager::new(meta.clone(), store.clone(), fsmeta.chunk_size, node_id)
                .with_tree(tree_access.clone());

        // Write authority (DESIGN.md §4/§5). Renew and takeover need
        // If-Match; a backend without it can only be driven safely by one
        // node at a time, so say so loudly and fall back to create-only
        // lease semantics instead of refusing to mount at all.
        let caps = rt
            .block_on(store.probe_conditional_writes())
            .context("probing backend conditional writes")?;
        if !caps.create_if_absent {
            bail!(
                "backend lacks create-if-absent (If-None-Match); unusable as a constellation backend"
            );
        }
        let lease_mode = if caps.etag_cas {
            constellation_store_s3::LeaseMode::Cas
        } else {
            tracing::warn!(
                "backend has no etag CAS (If-Match): lease renew/takeover cannot be \
                 enforced. Running in single-writer mode — mount this filesystem from \
                 ONE node only. Run `constellation doctor` and use an S3 backend with \
                 If-Match for multi-node operation."
            );
            constellation_store_s3::LeaseMode::SingleWriter
        };
        let write_mode = Arc::new(writeback::WriteModeState::new(initial_write_mode));
        // Plan 30 M5: the lease is the authority core's state; this is the
        // lock-free mirror the FUSE fast path and `status` read.
        let lease_view = Arc::new(lease::LeaseView::default());
        let delegate_view = Arc::new(lease::DelegateView::default());
        // A mutation waits at most ~2 TTLs for a foreign holder to release
        // or expire before failing with EIO.
        let acquire_deadline = Duration::from_millis(2 * lease::lease_ttl_ms());

        // Sync task channel: FUSE nudges it on close (publication point),
        // blocks on it for fsync in --fsync-mode s3, and asks it to take
        // the lease on the first mutation.
        let (sync_tx, sync_rx) = tokio::sync::mpsc::unbounded_channel::<fusefs::SyncRequest>();

        // A snapshot is a retained metadata root (plan 28), so taking one
        // forces a publish on the sync task, which owns the publisher. A
        // read-only member publishes nothing and so cannot take one.
        let snapshots = Arc::new(if read_only_member {
            snapshots_base
        } else {
            let tx = sync_tx.clone();
            snapshots_base.with_publisher(Arc::new(move || {
                let tx = tx.clone();
                Box::pin(async move {
                    // Plan 30 §M3a: a replica with forwarded ops the log
                    // has not confirmed yet cannot publish; that clears
                    // as soon as the holder ships them, so wait it out
                    // (bounded) rather than fail the snapshot.
                    let mut waited = 0u32;
                    loop {
                        let (reply, receive) = tokio::sync::oneshot::channel();
                        tx.send(fusefs::SyncRequest::Publish { reply })
                            .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
                        match receive
                            .await
                            .map_err(|_| anyhow::anyhow!("metadata publish stopped"))?
                        {
                            Ok(commit) => return Ok(commit),
                            Err(e)
                                if (e.contains(crate::mtree_publish::SPECULATION_OUTSTANDING)
                                    || e.contains("speculation outstanding"))
                                    && waited < 100 =>
                            {
                                waited += 1;
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                            Err(e) => return Err(anyhow::anyhow!(e)),
                        }
                    }
                })
            }))
        });

        // P2P fast path (DESIGN.md §8, M3.3). Every failure here is
        // non-fatal: without peers the daemon behaves exactly as phases 1-2,
        // reaching other nodes through S3 polling. Built before `fs` because
        // the offline-designation gate (phase 4a) needs it for delegation
        // requests.
        let peers = rt.block_on(crate::start_p2p(
            &fsmeta,
            e2e_keys.as_ref(),
            store.inner().clone(),
            node_id,
        ));
        let _gc_task = {
            let interval = std::env::var("CONSTELLATION_GC_INTERVAL_S")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(crate::gc::DEFAULT_GC_INTERVAL_S);
            let object_store = store.inner().clone();
            let chunks = store.clone();
            let meta = meta.clone();
            let gc_peers = peers.clone();
            let gc_sync_tx = sync_tx.clone();
            rt.spawn(async move {
                let mut timer =
                    tokio::time::interval(std::time::Duration::from_secs(interval.max(1)));
                timer.tick().await;
                loop {
                    timer.tick().await;
                    let tail = crate::gc::GcTail::Daemon(gc_sync_tx.clone());
                    if let Err(error) = crate::gc::run(
                        object_store.clone(),
                        chunks.clone(),
                        meta.clone(),
                        lease_mode,
                        false,
                        Some(&gc_peers),
                        &tail,
                    )
                    .await
                    {
                        tracing::warn!(%error, "periodic bucket GC pass failed");
                    }
                }
            })
        };
        // Plan 30 §M2: prune `completed` entries older than the
        // retention window on a cadence tied to that window itself
        // (a quarter of it, clamped to something reasonable) rather
        // than the much coarser bucket-GC interval above — the two
        // serve different purposes (bucket cleanup vs. bounding this
        // node-local table's size) and the default retention (900s) is
        // far shorter than the default GC interval (a day).
        let retention_s = std::env::var("CONSTELLATION_COMPLETION_RETENTION_S")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(crate::gc::DEFAULT_COMPLETION_RETENTION_S);
        let _completed_prune_task = {
            let meta = meta.clone();
            let prune_interval = (retention_s / 4).clamp(30, 3600);
            rt.spawn(async move {
                let mut timer =
                    tokio::time::interval(std::time::Duration::from_secs(prune_interval));
                timer.tick().await;
                loop {
                    timer.tick().await;
                    let now_ms = constellation_store_s3::lease::now_unix_ms();
                    match meta.prune_completed(now_ms, retention_s as i64 * 1000) {
                        Ok(0) => {}
                        Ok(pruned) => tracing::debug!(pruned, "pruned expired completed rids"),
                        Err(error) => tracing::warn!(%error, "completed-table prune failed"),
                    }
                    // Plan 30 §M2 coordinator review item 1: the
                    // per-(node,incarnation) entry cap in
                    // `Meta::remember_outcome` bounds `recent`'s *size*
                    // on every insert, but a low-traffic requester whose
                    // in-flight ops never get acked (a crash, a
                    // permanently departed peer) can sit under that cap
                    // indefinitely with genuinely stale entries. Same
                    // cadence and window as the `completed` prune above.
                    let pruned_recent =
                        meta.prune_recent_older_than(now_ms, retention_s as i64 * 1000);
                    if pruned_recent > 0 {
                        tracing::debug!(
                            pruned = pruned_recent,
                            "pruned stale recent-outcome entries"
                        );
                    }
                }
            })
        };
        let epochs = Arc::new(epoch::EpochManager::new(
            node_id,
            meta.clone(),
            peers.clone(),
        ));
        let lost_on_mount = matches!(meta.kv_get("lease_lost")?.as_deref(), Some("1"));
        let roster = rt
            .block_on(constellation_store_s3::write_eligible_roster(
                store.inner().clone(),
            ))
            .context("loading write-eligible roster")?;
        epochs.set_roster(roster.clone());
        let initial_roster = roster;

        let designations = Arc::new(designation::DesignationManager::new(
            constellation_store_s3::designation::DesignationStore::new(
                store.inner().clone(),
                if lease_mode == constellation_store_s3::LeaseMode::Cas {
                    constellation_store_s3::designation::DesignationMode::Cas
                } else {
                    constellation_store_s3::designation::DesignationMode::SingleWriter
                },
            ),
            meta.clone(),
            peers.clone(),
            node_id,
        ));
        rt.block_on(designations.refresh());

        let coop = crate::coop::Coop::new(
            cache.clone(),
            store.clone(),
            peers.clone(),
            node_id,
            fsmeta.chunk_size,
        );
        let upload = Arc::new(crate::UploadRuntime::new(
            caps.create_if_absent,
            Some(coop.clone()),
            crate::existence::Existence::with_meta(meta.clone()),
        ));
        upload.inherit(meta.pending_uploads()?.into_iter().map(|(hash, _)| hash));
        let forward = forward::ForwardState::new(incarnation);
        let placement = Arc::new(placement::Placement::new());
        let departed = Arc::new(AtomicBool::new(false));
        let atime_stats = crate::atime::AtimeStats::new();
        let atime = Arc::new(crate::atime::AtimeAccumulator::new(atime_mode, atime_stats));
        let prune_stats = crate::prune::PruneStats::new();
        let last_sync_ms = Arc::new(AtomicU64::new(crate::prune::now_unix_ms()));

        // Background metadata sync: tail foreign segments + ship the
        // journal, every interval or on demand (close/fsync nudges).
        let interval_ms: u64 = std::env::var("CONSTELLATION_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);
        // The interval is the *floor* of an exponential idle backoff; this
        // is its ceiling (see `next_poll_ms`).
        let idle_max_ms: u64 = std::env::var("CONSTELLATION_SYNC_IDLE_MAX_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(SYNC_IDLE_MAX_MS);
        // A continuation epoch opens only once S3 has been failing for a
        // whole round interval (plan 30 M5: rounds are no longer cancelled
        // by requests, so failing rounds complete more often than before;
        // the grace keeps a blip inside one interval from opening an epoch).
        epochs.set_propose_grace(Duration::from_millis(interval_ms));
        // Plan 30 M5: the authority core's tunables, from the same
        // `CONSTELLATION_*` knobs the extracted code read.
        let mut core_config = crate::authority_driver::load_config(
            node_id,
            incarnation,
            lease_mode,
            peers.is_enabled(),
            read_only_member,
            interval_ms,
            idle_max_ms,
            retention_s,
        );
        // Plan 30 §M8: a lone strict sequencer keeps a short kernel cache
        // TTL; the core drains it (see `cto::lone_kernel_ttl_ms`) before
        // acknowledging anything another node started.
        if cto_strict {
            core_config.kernel_cache_ttl_ms = crate::cto::lone_kernel_drain_ms();
        }
        // Plan 30 §M9: `ack=s3` is the filesystem's policy (M16: one
        // policy for every mount and tenure, fixed at `fs create`); a
        // strict mount marks its tenures as serving strict reads (a fast
        // successor then waits the delegation horizon).
        core_config.ack_s3 = crate::authority_driver::ack_s3_of(fsmeta.ack_policy.as_deref());
        if let Ok(env) = std::env::var("CONSTELLATION_ACK") {
            if !env.trim().is_empty()
                && crate::authority_driver::ack_s3_of(Some(&env)) != core_config.ack_s3
            {
                tracing::warn!(
                    CONSTELLATION_ACK = %env,
                    fs_policy = fsmeta.ack_policy.as_deref().unwrap_or("local"),
                    "CONSTELLATION_ACK only sets `fs create`'s default; this filesystem's \
                     acknowledgement policy applies"
                );
            }
        }
        core_config.strict_mounts = cto_strict;
        // Plan 30 §M14: cluster locks need a P2P path to the sequencer.
        let locks_cluster = crate::locks::cluster_effective(locks, peers.is_enabled())?;
        core_config.locks = locks_cluster;
        tracing::info!(
            locks = if locks_cluster { "cluster" } else { "local" },
            "file locks"
        );
        let lock_flushers = Arc::new(crate::locks::LockFlushers::default());
        // Plan 30 §M10: flexible-quorum continuation epochs.
        core_config.epoch_slack = fsmeta.epoch_slack();
        {
            let promise = constellation_store_s3::PromiseConfig::from_env(core_config.ttl_ms);
            if let Err(why) = promise.validate(core_config.ttl_ms) {
                if core_config.epoch_slack > 0 {
                    anyhow::bail!("epoch_slack {}: {why}", core_config.epoch_slack);
                }
                tracing::warn!(%why, "promise TTL invalid (unused while epoch_slack is 0)");
            }
            core_config.promise_ttl_ms = promise.ttl_ms;
        }
        epochs.set_slack(core_config.epoch_slack);
        if core_config.epoch_slack > 0 {
            tracing::info!(
                epoch_slack = core_config.epoch_slack,
                promise_ttl_ms = core_config.promise_ttl_ms,
                "flexible-quorum continuation epochs"
            );
        }
        if core_config.ack_s3 {
            tracing::info!("acknowledgement policy: s3 (every acknowledgement waits for the log)");
        }
        // Plan 28 §11: publish the §P6 tree on the publish cadence.
        // A read-only member publishes nothing: it ships no segments, so
        // it has no authority to commit one.
        let publisher = if read_only_member {
            None
        } else {
            let mut publisher = crate::mtree_publish::TreePublisher::new(
                meta.clone(),
                tree_access.nodes.clone(),
                tree_access.blobs.clone(),
                constellation_store_s3::CommitChain::new(store.inner().clone())
                    .with_sealing(tree_sealing.clone()),
                tree_access.config,
                node_id,
                rt.clone(),
            );
            publisher
                .restore()
                .context("restoring the published metadata tree")?;
            let publisher = Arc::new(tokio::sync::Mutex::new(publisher));
            {
                let publisher = publisher.clone();
                rt.spawn(async move {
                    if let Err(e) = publisher.lock().await.warm_up().await {
                        tracing::debug!(error = %e, "publisher warm-up failed; the first publish will retry it");
                    }
                });
            }
            Some(publisher)
        };
        let pins = Arc::new(pin::PinManager::new(
            meta.clone(),
            store.clone(),
            cache.clone(),
            Some(coop.clone()),
        ));
        let reintegration = Arc::new(reintegrate::ReintegrationState::default());
        // Only a persisted deposition is known to be a stranded branch.
        // Ordinary crash-recovery journals must retain their existing
        // ship-in-place path; treating every pending row as deposed would
        // unnecessarily rebuild a healthy replica on each remount.
        let reintegrate_on_mount = lost_on_mount && !epochs.is_open();
        let core_status = Arc::new(std::sync::Mutex::new(
            crate::authority_driver::CoreStatus::default(),
        ));
        let pending_acks: Arc<std::sync::Mutex<Vec<u64>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        // The core's IO driver: the sync task (plan 30 M5).
        {
            let driver = crate::authority_driver::Driver::new(
                crate::authority_driver::DriverDeps {
                    meta: meta.clone(),
                    store_inner: store.inner().clone(),
                    log: log.with_partition(constellation_store_s3::log::PARTITION),
                    lease_mode,
                    e2e: e2e_keys.clone(),
                    peers: peers.clone(),
                    view: lease_view.clone(),
                    delegates: delegate_view.clone(),
                    epochs: epochs.clone(),
                    designations: designations.clone(),
                    placement: placement.clone(),
                    pins: pins.clone(),
                    publisher,
                    cache: cache.clone(),
                    chunk_store: store.clone(),
                    compression,
                    upload: upload.clone(),
                    forward: forward.clone(),
                    reintegration: reintegration.clone(),
                    state_dir: state_dir.clone(),
                    last_sync_ms: last_sync_ms.clone(),
                    pending_acks: pending_acks.clone(),
                    status: core_status.clone(),
                    config: core_config,
                    lock_flush: {
                        let flushers = lock_flushers.clone();
                        Arc::new(move |ino| flushers.flush(ino))
                    },
                    fault_reply_delay_ms: fault_forward_delay_ms,
                },
                sync_tx.clone(),
                sync_rx,
            );
            // A panic in the core or the driver must not leave a mounted
            // filesystem whose every mutation now waits on a dead sync
            // task: the task's join error is turned into a loud abort
            // (the kill-9 model every scenario already covers), not a
            // silent wedge (M5 round 5).
            let driver_task = rt.spawn(driver.run());
            rt.spawn(async move {
                if let Err(error) = driver_task.await {
                    if error.is_panic() {
                        tracing::error!(%error, "the authority driver panicked; aborting");
                        std::process::abort();
                    }
                }
            });
        }
        let _ = sync_tx.send(fusefs::SyncRequest::Roster(initial_roster));
        if !read_only_member {
            rt.block_on(crate::adopt_root(&meta, &sync_tx, &forward, node_id))
                .context("adopting the root directory owner")?;
        }
        let bridge = Arc::new(crate::P2pBridge {
            node_id,
            nudge: sync_tx.clone(),
            epochs: epochs.clone(),
            store: store.inner().clone(),
            coop: coop.clone(),
            placement: placement.clone(),
        });
        if peers.is_enabled() {
            // Refresh-on-miss: an unknown key may be a peer that mounted
            // after us, which on a cold start is the normal case rather than
            // the exception.
            {
                let (p, store_inner, epochs) =
                    (peers.clone(), store.inner().clone(), epochs.clone());
                peers.set_refresher(Arc::new(move || {
                    let (p, store_inner, epochs) = (p.clone(), store_inner.clone(), epochs.clone());
                    Box::pin(async move {
                        let _ = crate::refresh_peers(&p, store_inner, Some(&epochs)).await;
                    })
                }));
            }
            // Accept inbound peer connections.
            {
                let (peers, bridge) = (peers.clone(), bridge.clone());
                rt.spawn(async move { peers.serve(bridge).await });
            }
            // Join the gossip topic and consume it. Bootstrapping from the
            // registry replaces a global discovery service; the node that
            // mounts first has nobody to bootstrap from, so poll briefly for
            // a peer instead of joining a topic alone.
            {
                let (peers, bridge, store_inner, epochs) = (
                    peers.clone(),
                    bridge.clone(),
                    store.inner().clone(),
                    epochs.clone(),
                );
                rt.spawn(async move {
                    for _ in 0..40 {
                        if !peers.snapshot().is_empty() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        crate::refresh_peers(&peers, store_inner.clone(), Some(&epochs)).await;
                    }
                    let bootstrap: Vec<constellation_net::EndpointId> =
                        peers.snapshot().iter().map(|p| p.addr.id).collect();
                    tracing::info!(bootstrap = bootstrap.len(), "joining the gossip topic");
                    match peers.join_topic(bootstrap).await {
                        Ok(rx) => constellation_net::run_gossip(peers.clone(), rx, bridge).await,
                        Err(e) => {
                            tracing::warn!(error = %e, "gossip unavailable; peers will poll S3")
                        }
                    }
                });
            }
            // Cooperative-cache digest publisher (DESIGN.md §7).
            {
                let coop = coop.clone();
                rt.spawn(async move { coop.publish_loop().await });
            }
            // Periodically re-read the registry so nodes that join later are
            // dialable and enrolled without a remount. Also detect our own
            // record vanishing or being retired (admin leave under us).
            {
                let (peers, store_inner, epochs, departed, node_id, meta, sync_tx) = (
                    peers.clone(),
                    store.inner().clone(),
                    epochs.clone(),
                    departed.clone(),
                    node_id,
                    meta.clone(),
                    sync_tx.clone(),
                );
                rt.spawn(async move {
                    let mut tick: u64 = 0;
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        let scan =
                            crate::refresh_peers(&peers, store_inner.clone(), Some(&epochs)).await;
                        let _ = sync_tx.send(fusefs::SyncRequest::Roster(epochs.roster()));
                        peers.probe_all().await;
                        tick += 1;
                        if tick.is_multiple_of(SLACK_REREAD_TICKS) {
                            reread_slack(&store_inner, &sync_tx).await;
                        }
                        // The scan just listed and (ETag-cached) read our
                        // record: live, so there is nothing to check. Only
                        // an absent or retired one is read directly before
                        // anything stops (EC2 finding R2-2: this GET was
                        // one per node every 5 s).
                        if scan.as_ref().is_some_and(|s| s.is_live(node_id)) {
                            continue;
                        }
                        match constellation_store_s3::get_node(store_inner.clone(), node_id).await {
                            Ok(None) => {
                                tracing::error!(
                                    node_id,
                                    "our registry record vanished; stopping writes \
                                     (operator admin-leave?). remount with a fresh state dir"
                                );
                                departed.store(true, Ordering::Relaxed);
                                let _ = meta.kv_set("left", "1");
                            }
                            Ok(Some(info)) if info.retired => {
                                tracing::error!(
                                    node_id,
                                    "our registry record is retired; stopping writes"
                                );
                                departed.store(true, Ordering::Relaxed);
                                let _ = meta.kv_set("left", "1");
                                let _ = sync_tx.send(fusefs::SyncRequest::Retired);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::debug!(error = %e, "own-record membership check failed")
                            }
                        }
                    }
                });
            }
        } else {
            // P2P off: still refresh the epoch roster and watch our own record.
            let (store_inner, epochs, departed, node_id, meta) = (
                store.inner().clone(),
                epochs.clone(),
                departed.clone(),
                node_id,
                meta.clone(),
            );
            let sync_tx_roster = sync_tx.clone();
            rt.spawn(async move {
                let mut tick: u64 = 0;
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    tick += 1;
                    if tick.is_multiple_of(SLACK_REREAD_TICKS) {
                        reread_slack(&store_inner, &sync_tx_roster).await;
                    }
                    if let Some(roster) =
                        crate::epoch::refresh_roster(&epochs, store_inner.clone()).await
                    {
                        // A requester this holder has not polled yet (a
                        // node that mounted after our last read): the core
                        // polls it at once (plan 30 M13 round 2).
                        let _ = sync_tx_roster.send(fusefs::SyncRequest::Roster(roster));
                    }
                    match constellation_store_s3::get_node(store_inner.clone(), node_id).await {
                        Ok(None) => {
                            tracing::error!(
                                node_id,
                                "our registry record vanished; stopping writes"
                            );
                            departed.store(true, Ordering::Relaxed);
                            let _ = meta.kv_set("left", "1");
                        }
                        Ok(Some(info)) if info.retired => {
                            tracing::error!(
                                node_id,
                                "our registry record is retired; stopping writes"
                            );
                            departed.store(true, Ordering::Relaxed);
                            let _ = meta.kv_set("left", "1");
                            let _ = sync_tx_roster.send(fusefs::SyncRequest::Retired);
                        }
                        _ => {}
                    }
                }
            });
        }
        // Designations are rare, operator-driven objects, but the daemon
        // must notice one appear/disappear without a remount (e.g. another
        // node ran `offline`). Poll less aggressively than the peer
        // registry since S3 LIST is not free and there is no gossip signal
        // for this yet.
        {
            let designations = designations.clone();
            let deleg_tx = sync_tx.clone();
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    designations.refresh().await;
                    // Plan 30 §M11 phase 2b: the root keeps the table in
                    // step with the designations (a no-op elsewhere).
                    let entries = designations.delegation_entries();
                    let _ = deleg_tx.send(fusefs::SyncRequest::SyncDesignations { entries });
                }
            });
        }
        if peers.is_enabled() {
            let (placement, peers, lease_view) =
                (placement.clone(), peers.clone(), lease_view.clone());
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    let status = lease_view.status();
                    if !(status.held && status.holder == node_id) || lease_view.is_lost() {
                        continue;
                    }
                    placement.gossip_rtts(&peers, node_id).await;
                    if let Some(best) = placement.recommend(node_id, &peers) {
                        let _ = peers
                            .request_to_node(
                                best,
                                &constellation_net::Payload::LeaseOffer {
                                    part: constellation_store_s3::log::PARTITION.to_string(),
                                    epoch: status.epoch,
                                },
                            )
                            .await;
                    }
                }
            });
        }
        let stop = Arc::new(AtomicBool::new(false));
        // Read-time atime flush ticker (plan 20). Off-mode accumulators
        // never queue anything, so this loop drains empty and is cheap;
        // it only does work when the operator opted in.
        if atime.mode() != crate::atime::AtimeMode::Off {
            let (atime, meta, lease_view, forward, sync_tx, stop) = (
                atime.clone(),
                meta.clone(),
                lease_view.clone(),
                forward.clone(),
                sync_tx.clone(),
                stop.clone(),
            );
            rt.spawn(async move {
                let period = crate::atime::flush_interval();
                loop {
                    tokio::time::sleep(period).await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    atime_flush_once(
                        &atime,
                        &meta,
                        &lease_view,
                        &sync_tx,
                        &forward,
                        node_id,
                        read_only_member,
                    )
                    .await;
                }
            });
        }
        // Retention pruner ticker (plan 22, Step 4). The default has no
        // marked roots, so a run walks nothing and is cheap; it only does
        // work once an operator sets a `user.constellation.prune` policy.
        {
            let (store_inner, meta, lease_view, forward, stop, departed, epoch_frozen) = (
                store.inner().clone(),
                meta.clone(),
                lease_view.clone(),
                forward.clone(),
                stop.clone(),
                departed.clone(),
                epochs.writes_refused.clone(),
            );
            let sync_tx = sync_tx.clone();
            let prune_stats = prune_stats.clone();
            let last_sync_ms = last_sync_ms.clone();
            rt.spawn(async move {
                let mut timer = tokio::time::interval(crate::prune::interval());
                timer.tick().await;
                loop {
                    timer.tick().await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if !crate::prune::enabled() {
                        continue;
                    }
                    let lag = std::time::Duration::from_millis(
                        crate::prune::now_unix_ms()
                            .saturating_sub(last_sync_ms.load(Ordering::Relaxed)),
                    );
                    let deps = crate::prune::PruneDeps {
                        store: store_inner.clone(),
                        meta: meta.clone(),
                        sync_tx: sync_tx.clone(),
                        lease: lease_view.clone(),
                        forward: forward.clone(),
                        node_id,
                        lease_mode,
                        read_only_member,
                        departed: departed.clone(),
                        epoch_frozen: Some(epoch_frozen.clone()),
                        stats: prune_stats.clone(),
                        replica_lag: lag,
                    };
                    if let Err(error) = crate::prune::run(&deps, None, false).await {
                        tracing::warn!(%error, "periodic prune pass failed");
                    }
                }
            });
        }

        if reintegrate_on_mount {
            let (reply, receive) = tokio::sync::oneshot::channel();
            sync_tx
                .send(fusefs::SyncRequest::Reintegrate(reply))
                .map_err(|_| anyhow::anyhow!("sync task stopped before automatic reintegration"))?;
            rt.block_on(receive)
                .context("automatic reintegration task stopped")?
                .map_err(anyhow::Error::msg)
                .context("automatic reintegration after mount")?;
        }

        let kernel_inval = crate::kernel_inval::enabled().then(|| {
            let k = crate::kernel_inval::KernelInvalidator::start();
            meta.set_foreign_apply_hook(k.hook());
            k
        });
        let node = Arc::new(NodeRuntime {
            kernel_inval,
            node_id,
            incarnation,
            next_rid_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fsmeta,
            backend_url: s3,
            state_dir,
            meta,
            store,
            cache,
            compression,
            snapshots,
            staging_dir,
            staging_budget,
            lease_mode,
            lease: lease_view,
            delegates: delegate_view,
            core_status,
            pending_acks,
            acquire_deadline,
            write_mode,
            sync_tx,
            peers,
            epochs,
            designations,
            coop,
            upload,
            forward,
            placement,
            departed,
            atime,
            prune_stats,
            last_sync_ms,
            read_only_member,
            fsync_s3,
            cto_strict,
            locks_cluster,
            lock_flushers,
            pins,
            reintegration,
            stop,
            web_ui,
            log_buffer,
            rt: rt.clone(),
            started: Instant::now(),
            status: Mutex::new(None),
            mounts: Mutex::new(HashMap::new()),
            threads: Mutex::new(HashMap::new()),
            next_mount_id: AtomicU64::new(1),
            shutdown_started: AtomicBool::new(false),
            shutdown_error: Mutex::new(None),
        });

        // Signals are node-level: unmount every currently-mounted view,
        // then run the one clean node shutdown. The actual unmount+join
        // work happens on a plain OS thread (not this async task) so a
        // slow drain never blocks a tokio worker; a second signal aborts
        // immediately regardless of how far that drain got.
        {
            let node = node.clone();
            rt.spawn(async move {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
                        tracing::warn!("failed to install SIGINT handler; Ctrl-C will not unmount");
                        return;
                    };
                    let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
                        tracing::warn!("failed to install SIGTERM handler");
                        return;
                    };
                    tokio::select! {
                        _ = sigint.recv() => tracing::info!("SIGINT received; unmounting FUSE"),
                        _ = sigterm.recv() => tracing::info!("SIGTERM received; unmounting FUSE"),
                    }
                    let drain = node.clone();
                    std::thread::spawn(move || {
                        for id in drain.mount_ids() {
                            if let Err(e) = drain.remove_mount(id) {
                                tracing::warn!(error = %e, "signal-triggered unmount failed");
                            }
                        }
                    });
                    // A second signal during the post-unmount drain aborts immediately
                    // so a hung ship/upload cannot trap the process forever.
                    tokio::select! {
                        _ = sigint.recv() => {}
                        _ = sigterm.recv() => {}
                    }
                    tracing::error!("second signal during shutdown; exiting immediately");
                    std::process::exit(130);
                }
                #[cfg(not(unix))]
                {
                    tracing::warn!("signal-driven FUSE unmount is only supported on Unix");
                }
            });
        }

        Ok(node)
    }

    /// Mount one view (root, subtree, or snapshot selector) and spawn its
    /// `fuser` session on a dedicated OS thread. Returns immediately;
    /// the thread runs until the view is unmounted (via `remove_mount`,
    /// an external `fusermount -u`, or process shutdown).
    pub fn add_mount(self: &Arc<Self>, view: ViewConfig) -> Result<MountId> {
        // Refuse to attach a view onto a daemon whose final shutdown has
        // already begun. Once the last view is removed the FUSE thread
        // runs `shutdown()` (drain + ship, then exit); a view added after
        // that point is never joined, so when the drain completes the
        // process exits and orphans the new kernel mount, leaving a dead
        // mountpoint (`Transport endpoint is not connected`). Rejecting
        // here lets the client fall through to `BecomeDaemon` cleanly.
        if self.shutdown_started.load(Ordering::SeqCst) {
            bail!("daemon is shutting down; retry once it has exited");
        }
        let ViewConfig {
            inner_path,
            mountpoint,
            allow_other,
            fs_name,
            fuse_threads,
            rw_snapshot,
            clone_name,
            ephemeral,
        } = view;

        let selector = inner_path
            .contains('@')
            .then(|| snapshot::split_selector(&inner_path))
            .transpose()?;
        if rw_snapshot && selector.is_none() {
            bail!("--rw is only valid when mounting <path>@<snapshot>");
        }
        let mut ephemeral_clone = None;
        let mounted_path = if let Some((source_path, snapshot_name)) = &selector {
            if rw_snapshot {
                let destination = if ephemeral {
                    format!(
                        "/.constellation-ephemeral-{}-{}",
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|duration| duration.as_millis())
                            .unwrap_or(0)
                    )
                } else {
                    let clone_name = clone_name
                        .as_deref()
                        .context("--rw snapshot mounts require --clone-name or --ephemeral")?;
                    if clone_name.starts_with('/') {
                        snapshot::normalize_path(clone_name)
                    } else {
                        let parent = source_path
                            .rsplit_once('/')
                            .map(|pair| pair.0)
                            .unwrap_or("");
                        snapshot::normalize_path(&format!("{parent}/{clone_name}"))
                    }
                };
                self.rt.block_on(self.snapshots.clone_to(
                    source_path,
                    snapshot_name,
                    &destination,
                ))?;
                if ephemeral {
                    ephemeral_clone = Some(destination.clone());
                }
                destination
            } else {
                source_path.clone()
            }
        } else {
            snapshot::normalize_path(&inner_path)
        };

        let departed = self.departed.clone();
        let mut fs = fusefs::ConstellationFs::new(
            fusefs::FsDependencies {
                meta: self.meta.clone(),
                store: self.store.clone(),
                cache: self.cache.clone(),
                rt: self.rt.clone(),
                sync: Some(fusefs::SyncHandle {
                    tx: self.sync_tx.clone(),
                    fsync_s3: self.fsync_s3,
                    cto_strict: self.cto_strict,
                    locks: self.locks_cluster.then(|| {
                        Arc::new(crate::locks::ClusterLocks {
                            meta: self.meta.clone(),
                            tx: self.sync_tx.clone(),
                            inval: self.kernel_inval.as_ref().map(|k| k.inodes()),
                        })
                    }),
                    lease: self.lease.clone(),
                    delegates: self.delegates.clone(),
                    acquire_deadline: self.acquire_deadline,
                    epoch_frozen: Some(self.epochs.writes_refused.clone()),
                    epoch_active: Some(self.epochs.active.clone()),
                    departed: Some(departed),
                    read_only_member: self.read_only_member,
                    write_mode: self.write_mode.clone(),
                    node_id: self.node_id,
                    incarnation: self.incarnation,
                    next_rid_seq: self.next_rid_seq.clone(),
                    acked: self.pending_acks.clone(),
                }),
                coop: Some(self.coop.clone()),
                staging_dir: self.staging_dir.clone(),
                staging_budget: self.staging_budget.clone(),
                snapshots: self.snapshots.clone(),
                atime: self.atime.clone(),
                prune_stats: self.prune_stats.clone(),
            },
            self.fsmeta.chunk_size,
            self.compression,
        );
        let prefetch_stats = fs.prefetch.stats();
        let quota_cache = fs.quota_cache_handle();
        if let Some((path, name)) = &selector {
            if rw_snapshot {
                fs.set_subtree_root(&mounted_path)?;
            } else {
                fs.set_snapshot_root(path, name)?;
            }
        } else {
            fs.set_subtree_root(&mounted_path)?;
        }

        // First view: build the status object + start the control API and
        // web UI (see module docs for why this waits for a mountpoint).
        {
            let mut status_guard = self.status.lock().unwrap();
            if status_guard.is_none() {
                let status = Arc::new(crate::DaemonStatus {
                    meta: self.meta.clone(),
                    cache: self.cache.clone(),
                    staging_budget: self.staging_budget.clone(),
                    core: self.core_status.clone(),
                    lease: self.lease.clone(),
                    fs_uuid: self.fsmeta.uuid.to_string(),
                    backend: self.backend_url.clone(),
                    node: self.clone(),
                    node_id: self.node_id,
                    started: self.started,
                    peers: self.peers.clone(),
                    pins: self.pins.clone(),
                    designations: self.designations.clone(),
                    epochs: self.epochs.clone(),
                    reintegration: self.reintegration.clone(),
                    sync_tx: self.sync_tx.clone(),
                    store: self.store.inner().clone(),
                    departed: self.departed.clone(),
                    rt: self.rt.clone(),
                    coop: self.coop.clone(),
                    prefetch_stats,
                    write_mode: self.write_mode.clone(),
                    upload: self.upload.clone(),
                    snapshots: self.snapshots.clone(),
                    log_buffer: self.log_buffer.clone(),
                    forward: self.forward.clone(),
                    placement: self.placement.clone(),
                    atime: self.atime.clone(),
                    prune_stats: self.prune_stats.clone(),
                    lease_mode: self.lease_mode,
                    read_only_member: self.read_only_member,
                    last_sync_ms: self.last_sync_ms.clone(),
                    state_dir: self.state_dir.clone(),
                    compression: self.compression,
                });
                *status_guard = Some(status.clone());
                drop(status_guard);
                let _guard = self.rt.enter();
                if let Err(e) = constellation_api::serve(&self.state_dir, status.clone()) {
                    tracing::warn!(error = %e, "control API unavailable");
                }
                if self.web_ui != 0 {
                    match self
                        .rt
                        .block_on(constellation_api::web::serve(self.web_ui, status))
                    {
                        Ok(address) => {
                            tracing::info!(%address, "web UI listening (localhost only)")
                        }
                        Err(error) => tracing::warn!(%error, "web UI unavailable"),
                    }
                }
            }
        }

        let options = vec![
            fuser::MountOption::FSName(fs_name),
            fuser::MountOption::DefaultPermissions,
        ];
        let acl = if allow_other {
            fuser::SessionACL::All
        } else {
            fuser::SessionACL::Owner
        };
        let mut options = options;
        if selector.is_some() && !rw_snapshot {
            options.push(fuser::MountOption::RO);
        }
        // Self-heal a stale mountpoint left by a previous daemon that
        // exited without unmounting (crash, kill, or an orphaned view
        // attached during shutdown). Such a mountpoint answers stat with
        // `ENOTCONN`; a fresh `Session::new` on it fails with the same
        // "Transport endpoint is not connected". Lazily detach it first so
        // the remount just works instead of surfacing os error 107.
        clear_stale_mount(&mountpoint);
        tracing::info!(?mountpoint, state_dir = ?self.state_dir, fs = %self.fsmeta.uuid, "mounting");
        let mut fuse_config = fuser::Config::default();
        fuse_config.mount_options = options;
        fuse_config.acl = acl;
        fuse_config.n_threads = Some(fuse_threads);
        fuse_config.clone_fd = cfg!(target_os = "linux") && fuse_config.n_threads != Some(1);
        // Build an explicit Session so `remove_mount`/signals can unmount
        // from inside this process (via SessionUnmounter). Plain
        // `fuser::mount` has no hook for that; without it, an external
        // kill leaves a dead mountpoint that needs `fusermount3 -u`.
        let view_root = fs.view_root();
        let frozen_view = selector.is_some() && !rw_snapshot;
        // Plan 30 §M14: shared, so a recalled lock grant's flush reaches
        // this view's write state (`locks::LockFlushers`).
        let fs = Arc::new(fs);
        let flusher: std::sync::Weak<dyn crate::locks::LockFlush> =
            Arc::downgrade(&(fs.clone() as Arc<dyn crate::locks::LockFlush>));
        let mut session = fuser::Session::new(fusefs::FuseFs(fs), &mountpoint, &fuse_config)
            .context("FUSE mount")?;
        let unmounter = session.unmount_callable();

        let id = MountId(self.next_mount_id.fetch_add(1, Ordering::Relaxed));
        self.lock_flushers.register(id.0, flusher);
        if let (Some(k), false) = (&self.kernel_inval, frozen_view) {
            k.register(id.0, session.notifier(), view_root);
        }
        let subtree = inner_path.clone();
        self.mounts.lock().unwrap().insert(
            id,
            MountHandle {
                subtree,
                mountpoint: mountpoint.clone(),
                since: Instant::now(),
                unmounter: Mutex::new(unmounter),
                quota_cache,
            },
        );

        let node = self.clone();
        let thread = std::thread::spawn(move || {
            if let Err(e) = session.run() {
                tracing::warn!(error = %e, "FUSE session ended with an error");
            }
            if let Some(k) = &node.kernel_inval {
                k.unregister(id.0);
            }
            node.lock_flushers.unregister(id.0);
            tracing::info!("FUSE detached");
            if let Some(path) = ephemeral_clone {
                if let Err(e) = crate::remove_live_subtree(&node.meta, &path) {
                    tracing::warn!(error = %e, path, "removing ephemeral clone failed");
                }
            }
            // This view is gone: drop its bookkeeping entry, and if it
            // was the last one, run the one node-wide clean shutdown.
            let now_empty = {
                let mut mounts = node.mounts.lock().unwrap();
                mounts.remove(&id);
                mounts.is_empty()
            };
            if now_empty {
                // `shutdown` logs its own failure (and records it for the
                // process's exit status).
                let _ = node.shutdown();
            }
        });
        self.threads.lock().unwrap().insert(id, thread);

        Ok(id)
    }

    /// Unmount exactly this view (via its `SessionUnmounter`) and join its
    /// session thread. Siblings are untouched. If this was the last view,
    /// the thread itself runs `shutdown()` before this call returns.
    pub fn remove_mount(&self, id: MountId) -> Result<()> {
        {
            let mounts = self.mounts.lock().unwrap();
            let handle = mounts
                .get(&id)
                .with_context(|| format!("no such mount: {id:?}"))?;
            let unmount_result = handle.unmounter.lock().unwrap().unmount();
            if let Err(e) = unmount_result {
                tracing::warn!(error = %e, "unmount request failed (already unmounted?)");
            }
        }
        if let Some(thread) = self.threads.lock().unwrap().remove(&id) {
            if thread.join().is_err() {
                tracing::warn!("FUSE session thread panicked");
            }
        }
        Ok(())
    }

    /// Block the calling thread until the given view's session ends,
    /// however it ends (an explicit `remove_mount`, an external
    /// `fusermount -u`, or the kernel force-unmounting it). Used by the
    /// CLI's single-view `mount` command to preserve today's "mount
    /// blocks until unmounted" behavior.
    pub fn join_mount(&self, id: MountId) -> Result<()> {
        let thread = self.threads.lock().unwrap().remove(&id);
        if let Some(thread) = thread {
            if thread.join().is_err() {
                tracing::warn!("FUSE session thread panicked");
            }
        }
        Ok(())
    }

    pub fn mounts(&self) -> Vec<MountInfo> {
        self.mounts
            .lock()
            .unwrap()
            .iter()
            .map(|(id, handle)| MountInfo {
                id: *id,
                subtree: handle.subtree.clone(),
                mountpoint: handle.mountpoint.clone(),
                since: handle.since,
            })
            .collect()
    }

    fn mount_ids(&self) -> Vec<MountId> {
        self.mounts.lock().unwrap().keys().copied().collect()
    }

    /// Invalidate every mounted view's cached quota cap after a live
    /// `SetQuota`. Quota is node-level (one `meta.db`, one cap), but each
    /// view's `ConstellationFs` keeps its own short-TTL read cache of it
    /// (`QUOTA_CACHE_TTL`) to avoid a `meta.quota()` round trip on every
    /// statfs/write; a single-view invalidation would leave any other
    /// mounted view serving the stale cap for up to that TTL.
    pub fn invalidate_quota_caches(&self) {
        for handle in self.mounts.lock().unwrap().values() {
            fusefs::ConstellationFs::invalidate_quota_cache(&handle.quota_cache);
        }
    }

    /// Clean shutdown: drain shipper, release leases, close meta.db, then
    /// remove `control.sock`/`daemon.pid` so a waiting `umount`/`export`
    /// (or a later `mount` probing for a live daemon) sees this process
    /// is really gone rather than timing out. Idempotent — called once,
    /// when the last mount is removed or on signal; later calls are a
    /// harmless no-op.
    ///
    /// A drain that fails (records held back behind a lost chunk, S3
    /// unreachable, a drain that stops making progress) never blocks the
    /// exit: the journal, the pending-upload rows and the held set all
    /// stay in `meta.db` and the chunks in the cache, and the next mount
    /// ships them. If anything is actually left unshipped, the error is
    /// logged, returned, and recorded for [`Self::shutdown_error`] so the
    /// foreground `mount` exits non-zero; a failure that left nothing
    /// behind (typically the lease release losing a race, which costs
    /// peers at most a TTL) is only a warning.
    pub fn shutdown(&self) -> Result<()> {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let result = match self.drain_for_shutdown() {
            Ok(()) => Ok(()),
            Err(error) => match self.unshipped_summary() {
                None => {
                    tracing::warn!(
                        error = %format!("{error:#}"),
                        "final flush failed, but nothing is left unshipped; exiting cleanly"
                    );
                    Ok(())
                }
                Some(left) => {
                    let message = format!(
                        "final flush failed: {error:#}; left on disk for the next mount \
                         of this state dir: {left}"
                    );
                    tracing::error!("{message}");
                    *self.shutdown_error.lock().unwrap() = Some(message.clone());
                    Err(anyhow::anyhow!(message))
                }
            },
        };
        // Best-effort, and unconditional even if the drain above failed:
        // a process that is exiting either way must not leave files
        // behind that make it look like a live daemon is still here.
        let _ = std::fs::remove_file(self.state_dir.join(constellation_api::SOCKET_NAME));
        let _ = std::fs::remove_file(self.state_dir.join("daemon.pid"));
        result
    }

    /// Why the node-wide shutdown left something unshipped, if it did.
    pub fn shutdown_error(&self) -> Option<String> {
        self.shutdown_error.lock().unwrap().clone()
    }

    /// What an exiting node leaves for its next mount, or `None` when
    /// the journal and the pending uploads are both empty.
    fn unshipped_summary(&self) -> Option<String> {
        let backlog = constellation_meta::MetaStore::journal_len(&*self.meta).unwrap_or(u64::MAX);
        let pending = self.meta.pending_upload_count().unwrap_or(u64::MAX);
        if backlog == 0 && pending == 0 {
            return None;
        }
        let held = self.meta.held_summary();
        let mut left =
            format!("journal backlog {backlog} record(s), {pending} pending chunk upload(s)");
        if held.transactions > 0 {
            left.push_str(&format!(
                ", {} transaction(s) held back behind unrecoverable chunks of inode(s) {:?} \
                 (`constellation status` lists them; `constellation repair drop-held <ino>` \
                 discards them into a conflict copy)",
                held.transactions,
                held.inodes.keys().collect::<Vec<_>>()
            ));
        }
        Some(left)
    }

    fn drain_for_shutdown(&self) -> Result<()> {
        // Clean unmount: ship the journal tail, publish a metadata commit,
        // then release the lease so a peer does not have to wait out the
        // TTL. Skip when we already flushed and retired via `leave` — the
        // registry record is a tombstone and a second ship is unnecessary.
        tracing::info!("draining uploads and shipping journal before exit");
        self.stop.store(true, Ordering::Relaxed);
        if matches!(self.meta.kv_get("left")?.as_deref(), Some("1")) {
            tracing::info!("node already left; skipping final drain");
            return Ok(());
        }
        let pending = self.meta.pending_upload_count().unwrap_or(0);
        let backlog = constellation_meta::MetaStore::journal_len(&*self.meta).unwrap_or(0);
        tracing::info!(
            pending_uploads = pending,
            journal_backlog = backlog,
            "clean unmount drain starting"
        );
        let flush = async {
            // Plan 05a step 2: an orderly unmount must not publish manifests
            // for chunks that never made it to S3. If a previous best-effort
            // eager upload (`try_upload_dirty`) failed and only logged, this
            // is the last chance to drain `pending_upload` before the
            // journal ships — an unmount that refuses to finish cleanly here
            // is strictly better than one that silently strands content.
            if let Err(e) = crate::upload_dirty_chunks(
                &self.cache,
                &self.meta,
                &self.store,
                self.compression,
                &self.upload,
                None,
                None,
            )
            .await
            {
                return Err(e).context(
                    "uploading dirty chunks before unmount; the journal was left un-shipped \
                     (run `constellation status --state-dir ...` after remounting to drain it)",
                );
            }
            // Plan 30 M5: the final flush + publish + release is the core's
            // `Control::Shutdown` — a release like any other (nothing new
            // executes locally from here on), after which the core stops.
            let (reply, receive) = tokio::sync::oneshot::channel();
            self.sync_tx
                .send(fusefs::SyncRequest::Shutdown { reply })
                .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
            receive
                .await
                .map_err(|_| anyhow::anyhow!("sync task stopped before the final flush"))?
                .map_err(anyhow::Error::msg)
        };
        // Bound the drain by progress, not by a wall clock: a large
        // write-back backlog may legitimately take a long time, but a
        // drain that stops shrinking the journal and the pending uploads
        // (S3 unreachable: every PUT spends its retry budget and fails,
        // round after round) must not keep the process alive forever.
        let stall = shutdown_stall_limit();
        let meta = self.meta.clone();
        let watchdog = async move {
            let progress = || {
                (
                    meta.pending_upload_count().unwrap_or(u64::MAX),
                    constellation_meta::MetaStore::journal_len(&*meta).unwrap_or(u64::MAX),
                )
            };
            let mut best = progress();
            let mut since = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let now = progress();
                if now.0 < best.0 || now.1 < best.1 {
                    best = (now.0.min(best.0), now.1.min(best.1));
                    since = Instant::now();
                } else if since.elapsed() >= stall {
                    return now;
                }
            }
        };
        let flush = self.rt.block_on(async {
            tokio::select! {
                result = flush => result,
                (pending, backlog) = watchdog => Err(anyhow::anyhow!(
                    "the drain made no progress for {stall:?} (pending uploads {pending}, \
                     journal backlog {backlog}; is S3 reachable?); giving up \
                     (CONSTELLATION_SHUTDOWN_STALL_S)"
                )),
            }
        });
        flush.context("final log flush")?;
        tracing::info!("clean unmount drain complete");
        Ok(())
    }
}

/// How long an unmount's drain may go without shrinking the journal or
/// the pending uploads before the process gives up and exits (non-zero,
/// everything left on disk for the next mount).
fn shutdown_stall_limit() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_SHUTDOWN_STALL_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(120),
    )
}

impl std::fmt::Debug for MountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One read-time atime flush (plan 20, Step 4). Drains the in-memory
/// accumulator, applies every bump to the *local* replica first (the
/// "always at least try" half, so local `stat` reflects local reads
/// regardless of what happens next), then chooses a publication path:
///
/// - local holder → queue into `atime_journal` for the core's ride-along
///   and standalone atime ships;
/// - non-holder (or a RO member with `CONSTELLATION_ATIME_RO_FORWARD`)
///   → one best-effort batched forward to the cached holder
///   (`Policy::BestEffort`), discarded on any failure — never retried
///   into a lease acquisition;
/// - RO member without the opt-in, or no known holder → local only.
///
/// Atime never acquires a lease and never wakes the shipper.
async fn atime_flush_once(
    atime: &crate::atime::AtimeAccumulator,
    meta: &Arc<Meta>,
    lease: &lease::LeaseView,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    forward: &forward::ForwardState,
    node_id: u64,
    read_only_member: bool,
) {
    use constellation_meta::MetaStore;
    let stats = &atime.stats;
    let drained = atime.drain();
    if drained.is_empty() {
        return;
    }
    // 1. Local apply through the shared helper (guard + clamp + max).
    match meta.apply_atime(&drained) {
        Ok((applied, clamped)) => {
            stats.applied.fetch_add(applied, Ordering::Relaxed);
            stats.skew_clamped.fetch_add(clamped, Ordering::Relaxed);
        }
        Err(e) => tracing::debug!(error = %e, "atime local apply failed"),
    }
    let ro_forward = crate::atime::ro_forward_enabled();
    if lease.usable() {
        // Holder: publish into atime_journal for the core's drain.
        match meta.queue_atime(&drained) {
            Ok(()) => stats.local_only.fetch_add(1, Ordering::Relaxed),
            Err(e) => {
                tracing::debug!(error = %e, "atime queue failed");
                0
            }
        };
    } else if read_only_member && !ro_forward {
        // RO member without the opt-in: applied locally, not published.
        stats.local_only.fetch_add(1, Ordering::Relaxed);
    } else {
        // Non-holder (or RO with forward enabled): one best-effort
        // forward to the cached holder. No holder known → kept local
        // (the core answers in doubt at once).
        let op = constellation_meta::MutateOp::AtimeBatch { entries: drained };
        let (reply, rx) = tokio::sync::oneshot::channel();
        if sync_tx
            .send(fusefs::SyncRequest::Submit {
                op,
                rid: forward.next_system_rid(node_id),
                policy: constellation_authority::Policy::BestEffort,
                in_doubt: false,
                reply,
            })
            .is_err()
        {
            stats.local_only.fetch_add(1, Ordering::Relaxed);
            return;
        }
        match rx.await {
            Ok(constellation_authority::ClientReply::Outcome(
                constellation_meta::MutateOutcome::Accepted { .. },
            )) => {
                stats.forward_ok.fetch_add(1, Ordering::Relaxed);
            }
            // Busy / NotHolder / Errno / timeout / no holder known:
            // discard, never retry into a lease acquisition.
            _ => {
                stats.forward_err.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Plan 30 §M10: re-read `meta.json`'s `epoch_slack` every this many
/// registry-poll ticks (5 s each): one GET a minute per node.
const SLACK_REREAD_TICKS: u64 = 12;

/// Plan 30 §M10: `fs set epoch-slack` changed `meta.json`: tell the core
/// (which re-advertises it in its heartbeat) and the epoch coordinator.
async fn reread_slack(
    store: &Arc<dyn object_store::ObjectStore>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
) {
    match ChunkStore::new(store.clone()).load_fs().await {
        Ok(meta) => {
            let _ = sync_tx.send(fusefs::SyncRequest::Slack(meta.epoch_slack()));
        }
        Err(e) => tracing::debug!(error = %e, "re-reading meta.json for epoch_slack failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start_node(
        rt: &tokio::runtime::Handle,
        backend: &str,
        state_dir: PathBuf,
    ) -> Arc<NodeRuntime> {
        NodeRuntime::start(
            NodeConfig {
                s3: backend.to_string(),
                state_dir: Some(state_dir),
                cache_size: 16 * 1024 * 1024,
                fsync_s3: false,
                cto_strict: false,
                locks: None,
                initial_write_mode: writeback::WriteMode::Through,
                read_only_member: false,
                web_ui: 0,
                log_buffer: log_buffer::LogBuffer::default(),
                atime_mode: crate::atime::AtimeMode::Off,
                passphrase: None,
            },
            rt.clone(),
        )
        .expect("NodeRuntime::start")
    }

    fn view(inner_path: &str, mountpoint: PathBuf) -> ViewConfig {
        ViewConfig {
            inner_path: inner_path.to_string(),
            mountpoint,
            allow_other: false,
            fs_name: "constellation-test".to_string(),
            fuse_threads: 1,
            rw_snapshot: false,
            clone_name: None,
            ephemeral: false,
        }
    }

    /// Polls `f` until it reports true or `deadline` elapses. FUSE attach
    /// and cross-node sync (the periodic sync task, ~500ms interval) are
    /// asynchronous; a fixed sleep would be either flaky (too short) or
    /// needlessly slow (too long) depending on host load.
    fn eventually(deadline: std::time::Duration, mut f: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        loop {
            if f() {
                return true;
            }
            if start.elapsed() > deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// The storm-hang follow-up: a final flush that cannot ship what the
    /// node holds must neither wedge nor pass for a clean exit. Here a
    /// pending upload whose chunk is not in the cache (as with a lost
    /// chunk) fails the drain: `shutdown` returns promptly, says what was
    /// left behind, records it for the process's exit status, and leaves
    /// the row for the next mount. A node with nothing to ship shuts down
    /// cleanly and records nothing.
    #[test]
    fn a_failed_final_flush_is_reported_and_left_for_the_next_mount() {
        unsafe {
            std::env::set_var("CONSTELLATION_P2P", "off");
        }
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        {
            let store = ChunkStore::new(
                rt.block_on(crate::backend::open_backend(&backend))
                    .expect("open backend"),
            );
            let meta = FsMeta::new(1024 * 1024, "raw");
            rt.block_on(store.create_fs(&meta)).expect("create_fs");
        }

        let clean = start_node(rt.handle(), &backend, root.path().join("state-clean"));
        clean.shutdown().expect("nothing to ship: a clean shutdown");
        assert_eq!(clean.shutdown_error(), None);

        let node = start_node(rt.handle(), &backend, root.path().join("state-lost"));
        let lost = constellation_fs_core::ChunkHash::of(b"never reached the cache");
        node.meta.add_pending_upload(&lost, 42).unwrap();
        let started = Instant::now();
        let error = node
            .shutdown()
            .expect_err("the drain cannot upload the lost chunk");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "a failed drain must not wedge the shutdown"
        );
        let message = error.to_string();
        assert!(
            message.contains("final flush failed") && message.contains("1 pending chunk upload"),
            "{message}"
        );
        assert_eq!(node.shutdown_error(), Some(message));
        assert_eq!(
            node.meta.pending_upload_count().unwrap(),
            1,
            "the pending row stays for the next mount"
        );
        // Idempotent: a second call neither drains again nor clears it.
        node.shutdown().unwrap();
        assert!(node.shutdown_error().is_some());
    }

    /// Regression for the daemon-sharing refactor (plan 21): two views of
    /// ONE `NodeRuntime` (root + a subtree) must both see the same live
    /// replica, and a peer node must converge with writes made through
    /// either view exactly as it would have with two independent
    /// single-view processes before this plan — the refactor changed how
    /// many *processes* serve a filesystem, not what gets replicated.
    /// This also covers the plan's view-agnostic-control-op case: pinning
    /// a path does not depend on which, or how many, views expose it.
    #[test]
    fn two_views_of_one_node_converge_with_a_peer_and_pin_is_view_agnostic() {
        // The gossip/bootstrap poll (up to ~10s) is pure overhead for an
        // in-process test with no real peer discovery to do.
        unsafe {
            std::env::set_var("CONSTELLATION_P2P", "off");
        }
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        // `fs create`, once, shared by both nodes (same backend prefix) —
        // mirrors `constellation fs create` before any `mount`.
        {
            let store = ChunkStore::new(
                rt.block_on(crate::backend::open_backend(&backend))
                    .expect("open backend"),
            );
            let meta = FsMeta::new(1024 * 1024, "raw");
            rt.block_on(store.create_fs(&meta)).expect("create_fs");
        }

        let a_root_mnt = root.path().join("a-root");
        let a_sub_mnt = root.path().join("a-sub");
        let b_mnt = root.path().join("b-root");
        std::fs::create_dir_all(&a_root_mnt).unwrap();
        std::fs::create_dir_all(&a_sub_mnt).unwrap();
        std::fs::create_dir_all(&b_mnt).unwrap();

        let node_a = start_node(rt.handle(), &backend, root.path().join("state-a"));
        let a_root_id = node_a
            .add_mount(view("/", a_root_mnt.clone()))
            .expect("mount a root");

        std::fs::create_dir(a_root_mnt.join("sub")).expect("mkdir sub via root view");
        let a_sub_id = node_a
            .add_mount(view("/sub", a_sub_mnt.clone()))
            .expect("mount a subtree");

        let node_b = start_node(rt.handle(), &backend, root.path().join("state-b"));
        let b_id = node_b
            .add_mount(view("/", b_mnt.clone()))
            .expect("mount b root");

        // Write through the root view, read back through the subtree view
        // of the SAME node: both must see one shared replica, not two.
        std::fs::write(a_root_mnt.join("sub/from-root.txt"), b"via-root").unwrap();
        assert!(
            eventually(std::time::Duration::from_secs(5), || {
                std::fs::read(a_sub_mnt.join("from-root.txt"))
                    .ok()
                    .as_deref()
                    == Some(b"via-root".as_slice())
            }),
            "subtree view did not see a write made through the root view of the same node"
        );

        // Write through the subtree view, read back through the root view.
        std::fs::write(a_sub_mnt.join("from-sub.txt"), b"via-sub").unwrap();
        assert!(
            eventually(std::time::Duration::from_secs(5), || {
                std::fs::read(a_root_mnt.join("sub/from-sub.txt"))
                    .ok()
                    .as_deref()
                    == Some(b"via-sub".as_slice())
            }),
            "root view did not see a write made through the subtree view of the same node"
        );

        // Both writes converge on the independent peer node — the same
        // cross-node correctness two single-view processes had before
        // this plan, now proven against a node hosting two views at once.
        assert!(
            eventually(std::time::Duration::from_secs(15), || {
                std::fs::read(b_mnt.join("sub/from-root.txt"))
                    .ok()
                    .as_deref()
                    == Some(b"via-root".as_slice())
                    && std::fs::read(b_mnt.join("sub/from-sub.txt"))
                        .ok()
                        .as_deref()
                        == Some(b"via-sub".as_slice())
            }),
            "peer node did not converge on writes made through either view of the multi-view node"
        );

        // View-agnostic control op: pinning "/sub" must not depend on
        // which — or how many — views currently expose it.
        let sock_a = root
            .path()
            .join("state-a")
            .join(constellation_api::SOCKET_NAME);
        let pin = rt
            .block_on(constellation_api::call(
                &sock_a,
                &constellation_api::Request::Pin {
                    path: "/sub".into(),
                },
            ))
            .unwrap();
        assert!(
            matches!(pin, constellation_api::Response::Ok { .. }),
            "pin failed: {pin:?}"
        );
        let list_pins = |rt: &tokio::runtime::Runtime| -> Vec<constellation_api::PinStatus> {
            match rt
                .block_on(constellation_api::call(
                    &sock_a,
                    &constellation_api::Request::ListPins,
                ))
                .unwrap()
            {
                constellation_api::Response::Pins { pins } => pins,
                other => panic!("unexpected response {other:?}"),
            }
        };
        let pins_before = list_pins(&rt);
        assert!(
            pins_before.iter().any(|p| p.path == "/sub"),
            "pin not listed: {pins_before:?}"
        );

        // Detach the subtree view; the pin (node-level, tracked against
        // the metadata replica, not against any one FUSE session) must
        // survive — proving it never depended on that view being mounted.
        node_a.remove_mount(a_sub_id).expect("unmount a subtree");
        let pins_after = list_pins(&rt);
        assert_eq!(
            pins_before.len(),
            pins_after.len(),
            "pin set changed after unmounting a view that never held any pins"
        );
        assert!(pins_after.iter().any(|p| p.path == "/sub"));

        node_a.remove_mount(a_root_id).expect("unmount a root");
        node_b.remove_mount(b_id).expect("unmount b root");
    }
}
