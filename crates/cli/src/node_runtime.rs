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
use constellation_meta::SqliteMeta;
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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

/// Everything needed to open/create a node's backend + local state,
/// independent of any particular mounted view.
#[allow(clippy::too_many_arguments)]
pub struct NodeConfig {
    pub s3: String,
    pub state_dir: Option<PathBuf>,
    pub cache_size: u64,
    pub fsync_s3: bool,
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
    fsmeta: FsMeta,
    backend_url: String,
    state_dir: PathBuf,
    meta: Arc<SqliteMeta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    compression: CompressionSetting,
    snapshots: Arc<snapshot::SnapshotManager>,
    staging_dir: PathBuf,
    staging_budget: Arc<staging::StagingBudget>,
    // Kept for future per-view partition keeper creation (Step 1+); the
    // sync task itself closes over a plain local copy captured at spawn
    // time, so this field has no reader yet in Step 0.
    #[allow(dead_code)]
    lease_mode: constellation_store_s3::LeaseMode,
    lease_views: Arc<std::sync::Mutex<HashMap<String, Arc<lease::LeaseView>>>>,
    acquire_deadline: Duration,
    write_mode: Arc<writeback::WriteModeState>,
    sync_tx: tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    peers: constellation_net::Peers,
    epochs: Arc<epoch::EpochManager>,
    designations: Arc<designation::DesignationManager>,
    coop: Arc<coop::Coop>,
    existence: Arc<crate::existence::Existence>,
    /// Set once, by whichever `add_mount` call is first to run (see its
    /// body): the LIST-seeded existence scan is spawned right before that
    /// view's `fuser::Session` is created, matching the original
    /// monolithic `mount()`'s exact call site (a short grace delay lets
    /// the mount finish attaching before LIST work begins). A second
    /// view must not spawn a second, redundant bucket-wide scan.
    existence_seeded: AtomicBool,
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
    ship: Arc<tokio::sync::Mutex<shipper::Shipper>>,
    spool: Arc<std::sync::Mutex<shipper::SpoolInfo>>,
    keepers: Arc<tokio::sync::Mutex<HashMap<String, lease::LeaseKeeper>>>,
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
    shutdown_started: AtomicBool,
}

impl NodeRuntime {
    /// Per-node setup: open the backend/replica/cache, claim or validate
    /// node identity, start the lease keeper, P2P endpoint, periodic GC,
    /// and the metadata shipper/sync task. No view is mounted yet.
    pub fn start(cfg: NodeConfig, rt: tokio::runtime::Handle) -> Result<Arc<Self>> {
        let NodeConfig {
            s3,
            state_dir,
            cache_size,
            fsync_s3,
            initial_write_mode,
            read_only_member,
            web_ui,
            log_buffer,
            atime_mode,
            passphrase,
        } = cfg;

        let backend = rt
            .block_on(crate::backend::open_backend(&s3))
            .context("opening backend")?;
        let fsmeta = rt
            .block_on(ChunkStore::new(backend.clone()).load_fs())
            .context("loading filesystem (fs create first?)")?;
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
        // Fresh node: rebuild the replica from checkpoint + log replay.
        if !db_path.exists() {
            rt.block_on(shipper::bootstrap(&db_path, &log))
                .context("bootstrapping metadata replica")?;
        }
        let meta = Arc::new(SqliteMeta::open(&db_path)?);
        meta.backfill_deref_once()?;
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
        tracing::info!(node_id, "node identity");

        // Mirror the creation-time cap from meta.json into node-local kv.
        // `read_quota` falls back to it only while no replicated `SetQuota`
        // exists, so this never journals, never needs a lease, and cannot
        // resurrect a cap an operator cleared live.
        match fsmeta.max_logical_bytes {
            Some(cap) => {
                meta.kv_set(
                    constellation_meta::sqlite::QUOTA_CREATION_KV_KEY,
                    &cap.to_string(),
                )?;
                tracing::info!(cap, "filesystem quota from meta.json");
            }
            None => meta.kv_del(constellation_meta::sqlite::QUOTA_CREATION_KV_KEY)?,
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
        let snapshots = Arc::new(snapshot::SnapshotManager::new(
            meta.clone(),
            store.clone(),
            compression,
            fsmeta.chunk_size,
            node_id,
        ));

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
        let mut keeper = lease::LeaseKeeper::new(
            constellation_store_s3::LeaseStore::new(
                store.inner().clone(),
                constellation_store_s3::log::PARTITION,
                lease_mode,
            ),
            node_id,
        );
        let lease_views = Arc::new(std::sync::Mutex::new({
            let mut m = HashMap::new();
            m.insert(
                constellation_store_s3::log::PARTITION.to_string(),
                keeper.view(),
            );
            m
        }));
        // A mutation waits at most ~2 TTLs for a foreign holder to release
        // or expire before failing with EIO.
        let acquire_deadline = Duration::from_millis(2 * keeper.ttl_ms());

        // Sync task channel: FUSE nudges it on close (publication point),
        // blocks on it for fsync in --fsync-mode s3, and asks it to take
        // the lease on the first mutation.
        let (sync_tx, mut sync_rx) = tokio::sync::mpsc::unbounded_channel::<fusefs::SyncRequest>();

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
            rt.spawn(async move {
                let mut timer =
                    tokio::time::interval(std::time::Duration::from_secs(interval.max(1)));
                timer.tick().await;
                loop {
                    timer.tick().await;
                    if let Err(error) = crate::gc::run(
                        object_store.clone(),
                        chunks.clone(),
                        meta.clone(),
                        lease_mode,
                        false,
                        false,
                        Some(&gc_peers),
                    )
                    .await
                    {
                        tracing::warn!(%error, "periodic bucket GC pass failed");
                    }
                }
            })
        };
        let epochs = Arc::new(epoch::EpochManager::new(
            node_id,
            meta.clone(),
            peers.clone(),
        ));
        keeper.share_takeover_gate(epochs.blocks_takeover.clone());
        let lost_on_mount = matches!(meta.kv_get("lease_lost")?.as_deref(), Some("1"));
        if lost_on_mount {
            keeper.force_lost();
        }
        let roster = rt
            .block_on(constellation_store_s3::write_eligible_roster(
                store.inner().clone(),
            ))
            .context("loading write-eligible roster")?;
        epochs.set_roster(roster);

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
        let existence = crate::existence::Existence::from_env();
        let upload = Arc::new(crate::UploadRuntime::new(
            caps.create_if_absent,
            Some(coop.clone()),
            existence.clone(),
        ));
        let forward = forward::ForwardState::new();
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
        let ship = shipper::Shipper::attach_with_mode(meta.clone(), log, node_id, lease_mode)?;
        let spool = ship.spool.clone();
        let mut ship = ship;
        if !read_only_member {
            rt.block_on(crate::adopt_root(&meta, &mut ship, &mut keeper))
                .context("adopting the root directory owner")?;
        }
        ship.set_peers(peers.clone());
        ship.set_designations(designations.clone());
        let ship = Arc::new(tokio::sync::Mutex::new(ship));
        let pins = Arc::new(pin::PinManager::new(
            meta.clone(),
            store.clone(),
            cache.clone(),
            Some(coop.clone()),
        ));
        let keepers = Arc::new(tokio::sync::Mutex::new({
            let mut m = HashMap::new();
            m.insert(constellation_store_s3::log::PARTITION.to_string(), keeper);
            m
        }));
        let reintegration = Arc::new(reintegrate::ReintegrationState::default());
        // Only a persisted deposition is known to be a stranded branch.
        // Ordinary crash-recovery journals must retain their existing
        // ship-in-place path; treating every pending row as deposed would
        // unnecessarily rebuild a healthy replica on each remount.
        let reintegrate_on_mount = lost_on_mount && !epochs.is_open();
        let bridge = Arc::new(crate::P2pBridge {
            node_id,
            nudge: sync_tx.clone(),
            designations: designations.clone(),
            meta: meta.clone(),
            epochs: epochs.clone(),
            coop: coop.clone(),
            forward: forward.clone(),
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
                    Box::pin(
                        async move { crate::refresh_peers(&p, store_inner, Some(&epochs)).await },
                    )
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
                let (peers, store_inner, epochs, departed, node_id, meta) = (
                    peers.clone(),
                    store.inner().clone(),
                    epochs.clone(),
                    departed.clone(),
                    node_id,
                    meta.clone(),
                );
                rt.spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        crate::refresh_peers(&peers, store_inner.clone(), Some(&epochs)).await;
                        peers.probe_all().await;
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
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    match constellation_store_s3::write_eligible_roster(store_inner.clone()).await {
                        Ok(roster) => epochs.set_roster(roster),
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "cannot determine the write-eligible roster; \
                                 continuation epochs stay unavailable"
                            );
                            epochs.set_roster(Vec::new());
                        }
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
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    designations.refresh().await;
                }
            });
        }
        if peers.is_enabled() {
            let (placement, peers, keepers) = (placement.clone(), peers.clone(), keepers.clone());
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    let held: Vec<(String, u64)> = {
                        let keepers = keepers.lock().await;
                        keepers
                            .iter()
                            .filter_map(|(part, keeper)| {
                                keeper.ship_epoch().map(|epoch| (part.clone(), epoch))
                            })
                            .collect()
                    };
                    if held.is_empty() {
                        continue;
                    }
                    placement.gossip_rtts(&peers, node_id).await;
                    for (part, epoch) in held {
                        if let Some(best) = placement.recommend(node_id, &peers) {
                            let _ = peers
                                .request_to_node(
                                    best,
                                    &constellation_net::Payload::LeaseOffer { part, epoch },
                                )
                                .await;
                        }
                    }
                }
            });
        }
        let stop = Arc::new(AtomicBool::new(false));
        // Read-time atime flush ticker (plan 20). Off-mode accumulators
        // never queue anything, so this loop drains empty and is cheap;
        // it only does work when the operator opted in.
        if atime.mode() != crate::atime::AtimeMode::Off {
            let (atime, meta, keepers, forward, peers, stop) = (
                atime.clone(),
                meta.clone(),
                keepers.clone(),
                forward.clone(),
                peers.clone(),
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
                        &keepers,
                        &forward,
                        &peers,
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
            let (store_inner, meta, keepers, forward, peers, stop, departed, epoch_frozen) = (
                store.inner().clone(),
                meta.clone(),
                keepers.clone(),
                forward.clone(),
                peers.clone(),
                stop.clone(),
                departed.clone(),
                epochs.frozen.clone(),
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
                        keepers: keepers.clone(),
                        forward: forward.clone(),
                        peers: peers.clone(),
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
        {
            let (
                ship,
                stop,
                spool,
                keepers,
                lease_views,
                store_inner,
                lease_mode,
                peers,
                pins,
                epochs,
                meta,
                cache,
                chunk_store,
                reintegration,
                state_dir_task,
                designations,
                upload,
                forward,
                placement,
            ) = (
                ship.clone(),
                stop.clone(),
                spool.clone(),
                keepers.clone(),
                lease_views.clone(),
                store.inner().clone(),
                lease_mode,
                peers.clone(),
                pins.clone(),
                epochs.clone(),
                meta.clone(),
                cache.clone(),
                store.clone(),
                reintegration.clone(),
                state_dir.clone(),
                designations.clone(),
                upload.clone(),
                forward.clone(),
                placement.clone(),
            );
            let last_sync_ms = last_sync_ms.clone();
            rt.spawn(async move {
                let mut pending: Option<fusefs::SyncRequest> = None;
                // The periodic poll is a *persistent* deadline, not a fresh
                // sleep per loop iteration. A fresh sleep inside `select!`
                // resets whenever any request arrives first, so a peer (or
                // a FUSE thread) sending requests more often than the sync
                // interval would starve the poll forever: this node would
                // keep answering forwards/acquires but never tail or ship
                // again — a livelock where every node waits for a record
                // its holder never publishes.
                let poll = tokio::time::sleep(std::time::Duration::from_millis(interval_ms));
                tokio::pin!(poll);
                'sync: loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    // Freshness heartbeat for the pruner's lag gate: the
                    // task tails/answers every pass, so this bounds how
                    // stale our replica can be while the task is alive.
                    last_sync_ms.store(crate::prune::now_unix_ms(), Ordering::Relaxed);
                    let request = if let Some(req) = pending.take() {
                        Some(req)
                    } else {
                        tokio::select! {
                            msg = sync_rx.recv() => match msg {
                                Some(req) => Some(req),
                                None => break,
                            },
                            _ = poll.as_mut() => None,
                        }
                    };
                    match request {
                        Some(fusefs::SyncRequest::Acquire { part, reply }) => {
                            let deposed = match meta.kv_get("lease_lost") {
                                Ok(value) => matches!(value.as_deref(), Some("1")),
                                Err(error) => {
                                    let _ = reply.send(Err(format!(
                                        "cannot read persisted deposition state: {error}"
                                    )));
                                    continue;
                                }
                            };
                            if deposed {
                                let _ = reply.send(Err(
                                    "this node was deposed; run reintegration before acquiring leases"
                                        .into(),
                                ));
                                continue;
                            }
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            if !keepers.contains_key(&part) {
                                let mut k = lease::LeaseKeeper::new(
                                    constellation_store_s3::LeaseStore::new(
                                        store_inner.clone(),
                                        &part,
                                        lease_mode,
                                    ),
                                    node_id,
                                );
                                k.share_takeover_gate(epochs.blocks_takeover.clone());
                                lease_views.lock().unwrap().insert(part.clone(), k.view());
                                keepers.insert(part.clone(), k);
                            }
                            let keeper = keepers.get_mut(&part).unwrap();
                            keeper.note_acquire_reason("fuse-acquire");
                            let mut r = if epochs.writes_ok() {
                                if keeper.holds_authority() {
                                    Ok(true)
                                } else if peers.request_lease(&part, None).await {
                                    keeper.adopt_epoch_hold(keeper.authority_epoch());
                                    Ok(true)
                                } else {
                                    Ok(false)
                                }
                            } else {
                                shipper::acquire_lease_for(&mut ship, keeper, &part).await
                            };
                            // Fast path (M3.3): a live holder can hand the
                            // lease over in ~1 RTT instead of making us wait
                            // out its idle window or TTL. Only worth asking
                            // when the plain CAS just failed, and the retry
                            // is still an ordinary CAS — S3 stays the commit
                            // point, so a lying peer only wastes one round.
                            if !epochs.is_open()
                                && matches!(r, Ok(false))
                                && peers.is_enabled()
                                && peers.request_lease(&part, None).await
                            {
                                keeper.note_acquire_reason("fuse-acquire-after-handoff");
                                r = shipper::acquire_lease_for(&mut ship, keeper, &part).await;
                            }
                            if let Err(e) = &r {
                                tracing::warn!(error = %e, part, "lease acquisition failed");
                            }
                            let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                        }
                        Some(fusefs::SyncRequest::HandOff { part, reply }) => {
                            // Fast-path handoff (M3.3): flush this partition
                            // so the requester sees every committed record,
                            // then release. Declining is always safe — the
                            // requester waits the lease out through S3.
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            let result = match keepers.get_mut(&part) {
                                Some(k) if k.ship_epoch().is_some() && !k.is_lost() => {
                                    let epoch = k.ship_epoch().unwrap();
                                    if epochs.writes_ok() {
                                        k.release_local();
                                        Some(fusefs::HandoffResult {
                                            epoch,
                                            etag: None,
                                            head_seq: ship.last_shipped_seq(&part),
                                        })
                                    } else if let Err(e) = crate::upload_dirty_chunks(
                                        &cache,
                                        &meta,
                                        &chunk_store,
                                        compression,
                                        &upload,
                                        None,
                                        Some(&part),
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            error = %e,
                                            part,
                                            "dirty chunk upload before handoff failed; keeping the lease"
                                        );
                                        None
                                    } else {
                                        match ship.sync_one(&part, k).await {
                                            Ok(()) => match k.release().await {
                                                Ok(etag) => Some(fusefs::HandoffResult {
                                                    epoch,
                                                    etag,
                                                    head_seq: ship.last_shipped_seq(&part),
                                                }),
                                                Err(e) => {
                                                    tracing::warn!(error = %e, part,
                                                        "lease release failed; keeping it");
                                                    None
                                                }
                                            },
                                            Err(e) => {
                                                tracing::warn!(error = %e, part,
                                                    "flush before handoff failed; keeping the lease");
                                                None
                                            }
                                        }
                                    }
                                }
                                _ => None,
                            };
                            let _ = reply.send(result);
                        }
                        Some(fusefs::SyncRequest::Mutate {
                            part,
                            requester,
                            op,
                            reply,
                        }) => {
                            // Only inspect lease state under the keeper lock. The
                            // metadata transaction must not serialize unrelated
                            // lease maintenance or network requests.
                            let (ship_epoch, is_lost) = {
                                let keepers = keepers.lock().await;
                                keepers
                                    .get(&part)
                                    .map(|keeper| (keeper.ship_epoch(), keeper.is_lost()))
                                    .unwrap_or((None, false))
                            };
                            let known_holder = if ship_epoch.is_some() {
                                node_id
                            } else if let Some(holder) = forward.cached_holder(&part) {
                                holder
                            } else {
                                constellation_store_s3::LeaseStore::new(
                                    store_inner.clone(),
                                    &part,
                                    lease_mode,
                                )
                                .get()
                                .await
                                .ok()
                                .flatten()
                                .map(|(lease, _)| lease.holder)
                                .unwrap_or(0)
                            };
                            let outcome = forward::holder_execute(
                                &meta,
                                ship_epoch,
                                is_lost,
                                known_holder,
                                &op,
                            );
                            if matches!(
                                outcome,
                                constellation_meta::MutateOutcome::Accepted { .. }
                            ) {
                                if let Some(view) = lease_views.lock().unwrap().get(&part) {
                                    view.touch();
                                }
                                placement.note_forwarded(requester);
                                pending = Some(fusefs::SyncRequest::Nudge);
                            }
                            let _ = reply.send(outcome);
                        }
                        Some(fusefs::SyncRequest::Forward { part, op, reply }) => {
                            let local_epoch = {
                                let keepers = keepers.lock().await;
                                keepers.get(&part).and_then(|keeper| keeper.ship_epoch())
                            };
                            let outcome = if let Some(epoch) = local_epoch {
                                match constellation_meta::execute_mutate(&meta, &op) {
                                    Ok(records) => {
                                        if let Some(view) = lease_views.lock().unwrap().get(&part) {
                                            view.touch();
                                        }
                                        placement.note_local(node_id);
                                        constellation_meta::MutateOutcome::Accepted { epoch, records }
                                    }
                                    Err(constellation_meta::MetaError::Conflict) => {
                                        // We are the holder, so our own replica is
                                        // authoritative; the requester rebases from it.
                                        constellation_meta::MutateOutcome::Conflict {
                                            manifest: None,
                                        }
                                    }
                                    Err(error) => {
                                        constellation_meta::MutateOutcome::Errno(
                                            forward::meta_errno(&error),
                                        )
                                    }
                                }
                            } else {
                                let mut holder = forward.cached_holder(&part);
                                if holder.is_none() {
                                    let store = constellation_store_s3::LeaseStore::new(
                                        store_inner.clone(),
                                        &part,
                                        lease_mode,
                                    );
                                    holder = store
                                        .get()
                                        .await
                                        .ok()
                                        .flatten()
                                        .map(|(lease, _)| lease.holder)
                                        .filter(|holder| *holder != 0);
                                    if let Some(holder) = holder {
                                        forward.note_holder(&part, holder);
                                    }
                                }
                                if let Some(mut holder) = holder {
                                    let mut outcome = forward::request_mutate(
                                        &peers,
                                        &forward,
                                        &part,
                                        node_id,
                                        holder,
                                        &op,
                                    )
                                    .await;
                                    if let constellation_meta::MutateOutcome::NotHolder {
                                        holder: next,
                                    } = outcome
                                    {
                                        if next != 0 && next != holder {
                                            holder = next;
                                            outcome = forward::request_mutate(
                                                &peers,
                                                &forward,
                                                &part,
                                                node_id,
                                                holder,
                                                &op,
                                            )
                                            .await;
                                        }
                                    }
                                    if let constellation_meta::MutateOutcome::Accepted {
                                        epoch,
                                        ref records,
                                    } = outcome
                                    {
                                        if let Err(error) =
                                            forward::apply_accepted(&meta, &part, epoch, records)
                                        {
                                            tracing::warn!(
                                                %error,
                                                part,
                                                "failed to apply accepted forwarded mutation"
                                            );
                                            let _ = reply.send(Err(error.to_string()));
                                            continue;
                                        }
                                    }
                                    outcome
                                } else {
                                    forward.clear_holder(&part);
                                    constellation_meta::MutateOutcome::Busy
                                }
                            };
                            if matches!(
                                outcome,
                                constellation_meta::MutateOutcome::Accepted { .. }
                            ) {
                                pending = Some(fusefs::SyncRequest::Nudge);
                            }
                            let _ = reply.send(Ok(outcome));
                        }
                        Some(fusefs::SyncRequest::ApplyPushed {
                            part,
                            seq,
                            epoch,
                            holder_node,
                            payload,
                        }) => {
                            let holder_node = (holder_node != 0)
                                .then_some(holder_node)
                                .or_else(|| shipper::segment_node(&payload))
                                .unwrap_or(0);
                            let applied = ship
                                .lock()
                                .await
                                .try_apply_pushed(&part, seq, epoch, &payload)
                                .unwrap_or(false);
                            if applied {
                                forward
                                    .pushed_applied
                                    .fetch_add(1, Ordering::Relaxed);
                                if holder_node != 0 {
                                    forward.note_holder(&part, holder_node);
                                }
                            } else {
                                pending = Some(fusefs::SyncRequest::Nudge);
                            }
                        }
                        Some(fusefs::SyncRequest::ClaimOffer { part, epoch }) => {
                            tracing::debug!(part, epoch, "claiming offered lease");
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            if !keepers.contains_key(&part) {
                                let mut keeper = lease::LeaseKeeper::new(
                                    constellation_store_s3::LeaseStore::new(
                                        store_inner.clone(),
                                        &part,
                                        lease_mode,
                                    ),
                                    node_id,
                                );
                                keeper.share_takeover_gate(epochs.blocks_takeover.clone());
                                lease_views
                                    .lock()
                                    .unwrap()
                                    .insert(part.clone(), keeper.view());
                                keepers.insert(part.clone(), keeper);
                            }
                            let _ = peers.request_lease(&part, forward.cached_holder(&part)).await;
                            if let Some(keeper) = keepers.get_mut(&part) {
                                keeper.note_acquire_reason("claim-offer");
                                if shipper::acquire_lease_for(&mut ship, keeper, &part)
                                    .await
                                    .unwrap_or(false)
                                {
                                    placement.mark_migrated();
                                }
                            }
                        }
                        Some(fusefs::SyncRequest::DrainInode { ino, reply }) => {
                            let result = crate::upload_dirty_chunks(
                                &cache,
                                &meta,
                                &chunk_store,
                                compression,
                                &upload,
                                (ino != 0).then_some(ino),
                                None,
                            )
                            .await;
                            let _ = reply.send(result.map_err(|error| format!("{error:#}")));
                        }
                        Some(fusefs::SyncRequest::Barrier { ino, reply }) => {
                            // `--fsync-mode s3` is an inode/partition
                            // barrier, not a whole-mount backlog drain.
                            let mut r = crate::upload_dirty_chunks(
                                &cache,
                                &meta,
                                &chunk_store,
                                compression,
                                &upload,
                                Some(ino),
                                None,
                            )
                            .await;
                            if r.is_ok() {
                                let part = meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
                                let mut ship = ship.lock().await;
                                let mut keepers = keepers.lock().await;
                                r = match keepers.get_mut(&part) {
                                    Some(keeper) => ship.sync_one(&part, keeper).await,
                                    None => Err(anyhow::anyhow!(
                                        "no lease keeper for fsync partition {part}"
                                    )),
                                };
                            }
                            if let Err(e) = &r {
                                tracing::warn!(error = %e, "metadata sync failed; will retry");
                                spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                            } else {
                                pins.refresh_all().await;
                            }
                            let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                        }
                        Some(fusefs::SyncRequest::Reintegrate(reply)) => {
                            if epochs.is_open() {
                                let _ = reply.send(Err(
                                    "cannot reintegrate while a continuation epoch is open".into(),
                                ));
                                continue;
                            }
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            let r = reintegrate::run(
                                &meta,
                                &mut ship,
                                &mut keepers,
                                node_id,
                                &state_dir_task,
                                &reintegration,
                            )
                            .await;
                            let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                        }
                        Some(fusefs::SyncRequest::Leave { force, reply }) => {
                            if let Err(e) = crate::leave::refuse_open_epoch(&epochs) {
                                let _ = reply.send(Err(e.to_string()));
                                continue;
                            }
                            // Upload dirty chunks before the journal flush so
                            // self-leave does not strand content that only
                            // exists in the local cache.
                            if let Err(e) = crate::upload_dirty_chunks(
                                &cache,
                                &meta,
                                &chunk_store,
                                compression,
                                &upload,
                                None,
                                None,
                            )
                            .await
                            {
                                let _ = reply.send(Err(format!(
                                    "cannot upload dirty chunks before leave: {e:#}"
                                )));
                                continue;
                            }
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            let r = crate::leave::self_leave(
                                store_inner.clone(),
                                &meta,
                                &mut ship,
                                &mut keepers,
                                &designations,
                                node_id,
                                force,
                            )
                            .await;
                            let _ = reply.send(r.map(|_| format!(
                                "left cluster as node {node_id}; registry record retired"
                            )).map_err(|e| e.to_string()));
                        }
                        Some(fusefs::SyncRequest::Nudge) | None => {
                            // A round is about to run; push the periodic
                            // poll out by one interval so it only fires when
                            // rounds have genuinely stopped happening.
                            poll.as_mut().reset(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(interval_ms),
                            );
                            // Keep polling one round while draining ordinary
                            // nudges. Dropping this future used to cancel the
                            // async upload side while already-started
                            // spawn_blocking encoders continued, so a close()
                            // storm could multiply CPU work and blocking threads.
                            let round = crate::run_managed_sync_round(
                                &ship,
                                &keepers,
                                &epochs,
                                &meta,
                                &cache,
                                &chunk_store,
                                compression,
                                &upload,
                            );
                            tokio::pin!(round);
                            loop {
                                tokio::select! {
                                    biased;
                                    r = &mut round => {
                                        if let Err(e) = r {
                                            tracing::warn!(
                                                error = %e,
                                                "metadata sync failed; will retry"
                                            );
                                            spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                                        } else {
                                            pins.refresh_all().await;
                                        }
                                        break;
                                    }
                                    msg = sync_rx.recv() => match msg {
                                        Some(fusefs::SyncRequest::Nudge) => {
                                            // Coalesced: the current round already
                                            // covers the work visible at its start.
                                        }
                                        Some(request) => {
                                            // Explicit operations retain their old
                                            // prompt-response behavior.
                                            pending = Some(request);
                                            break;
                                        }
                                        None => break 'sync,
                                    },
                                }
                            }
                        }
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

        let node = Arc::new(NodeRuntime {
            node_id,
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
            lease_views,
            acquire_deadline,
            write_mode,
            sync_tx,
            peers,
            epochs,
            designations,
            coop,
            existence,
            existence_seeded: AtomicBool::new(false),
            upload,
            forward,
            placement,
            departed,
            atime,
            prune_stats,
            last_sync_ms,
            read_only_member,
            fsync_s3,
            ship,
            spool,
            keepers,
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
                    leases: self.lease_views.clone(),
                    acquire_deadline: self.acquire_deadline,
                    designations: Some(self.designations.clone()),
                    epoch_frozen: Some(self.epochs.frozen.clone()),
                    epoch_active: Some(self.epochs.active.clone()),
                    departed: Some(departed),
                    read_only_member: self.read_only_member,
                    write_mode: self.write_mode.clone(),
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
                    spool: self.spool.clone(),
                    leases: self.lease_views.clone(),
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
                    keepers: self.keepers.clone(),
                    lease_mode: self.lease_mode,
                    read_only_member: self.read_only_member,
                    last_sync_ms: self.last_sync_ms.clone(),
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
        // Matches the original monolithic `mount()`'s exact call site:
        // right before the FUSE session is built, not any earlier. The
        // scan's own 100ms grace delay assumes the mount is about to
        // attach; spawning it during node-level startup (well before any
        // view exists) measurably shifted upload-path timing in testing
        // and is not an equivalent reordering. Only the first view seeds
        // it — the scan is bucket-wide and node-level, not per-view.
        if self
            .existence_seeded
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.existence.spawn_seed(self.store.clone(), &self.rt);
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
        let mut session =
            fuser::Session::new(fs, &mountpoint, &fuse_config).context("FUSE mount")?;
        let unmounter = session.unmount_callable();

        let id = MountId(self.next_mount_id.fetch_add(1, Ordering::Relaxed));
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
                if let Err(e) = node.shutdown() {
                    tracing::warn!(error = %e, "node shutdown after last mount removed failed");
                }
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
    pub fn shutdown(&self) -> Result<()> {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let result = self.drain_for_shutdown();
        // Best-effort, and unconditional even if the drain above failed:
        // a process that is exiting either way must not leave files
        // behind that make it look like a live daemon is still here.
        let _ = std::fs::remove_file(self.state_dir.join(constellation_api::SOCKET_NAME));
        let _ = std::fs::remove_file(self.state_dir.join("daemon.pid"));
        result
    }

    fn drain_for_shutdown(&self) -> Result<()> {
        // Clean unmount: ship the journal tail, checkpoint, then release the
        // lease so a peer does not have to wait out the TTL. Skip when we
        // already flushed and retired via `leave` — the registry record is
        // a tombstone and a second ship is unnecessary.
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
        let flush = self.rt.block_on(async {
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
                self.ship.lock().await.set_skip_ship(true);
                return Err(e).context(
                    "uploading dirty chunks before unmount; the journal was left un-shipped \
                     (run `constellation status --state-dir ...` after remounting to drain it)",
                );
            }
            let mut ship = self.ship.lock().await;
            let mut keepers = self.keepers.lock().await;
            let r = ship.shutdown_all(&mut keepers).await;
            for k in keepers.values_mut() {
                k.release().await?;
            }
            r
        });
        flush.context("final log flush")?;
        tracing::info!("clean unmount drain complete");
        Ok(())
    }
}

impl std::fmt::Debug for MountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One read-time atime flush (plan 20, Step 4). Drains the in-memory
/// accumulator, applies every bump to the *local* replica first (the
/// "always at least try" half, so local `stat` reflects local reads
/// regardless of what happens next), then per partition chooses a
/// publication path:
///
/// - local holder → queue into `atime_journal` for the shipper to drain;
/// - non-holder (or a RO member with `CONSTELLATION_ATIME_RO_FORWARD`)
///   → one best-effort batched forward to the cached holder, discarded
///   on any failure — never retried into a lease acquisition;
/// - RO member without the opt-in, or no known holder → local only.
///
/// Atime never acquires a lease and never wakes the shipper.
async fn atime_flush_once(
    atime: &crate::atime::AtimeAccumulator,
    meta: &Arc<SqliteMeta>,
    keepers: &Arc<tokio::sync::Mutex<HashMap<String, lease::LeaseKeeper>>>,
    forward: &Arc<forward::ForwardState>,
    peers: &constellation_net::Peers,
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
    // 2. Group by partition (resolve once per inode).
    let mut by_part: HashMap<String, Vec<(constellation_fs_core::Ino, i64, i64)>> = HashMap::new();
    for (ino, atime_ns, time_ns) in drained {
        match meta.partition_of(ino) {
            Ok(part) => by_part
                .entry(part)
                .or_default()
                .push((ino, atime_ns, time_ns)),
            Err(_) => continue,
        }
    }
    // 3. Snapshot the partitions this node currently holds a usable,
    //    non-lost shipping lease for.
    let held: std::collections::HashSet<String> = {
        let keepers = keepers.lock().await;
        keepers
            .iter()
            .filter(|(_, k)| !k.is_lost() && k.ship_epoch().is_some() && k.view().usable())
            .map(|(p, _)| p.clone())
            .collect()
    };
    let ro_forward = crate::atime::ro_forward_enabled();
    let timeout = crate::atime::forward_timeout();
    for (part, entries) in by_part {
        if held.contains(&part) {
            // Holder: publish into atime_journal for the shipper drain.
            match meta.queue_atime(&entries) {
                Ok(()) => stats.local_only.fetch_add(1, Ordering::Relaxed),
                Err(e) => {
                    tracing::debug!(error = %e, part, "atime queue failed");
                    0
                }
            };
        } else if read_only_member && !ro_forward {
            // RO member without the opt-in: applied locally, not published.
            stats.local_only.fetch_add(1, Ordering::Relaxed);
        } else {
            // Non-holder (or RO with forward enabled): one best-effort
            // forward to the cached holder. No holder known → keep local.
            let Some(holder) = forward.cached_holder(&part) else {
                stats.local_only.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let op = constellation_meta::MutateOp::AtimeBatch { entries };
            match forward::request_mutate_with(peers, forward, &part, node_id, holder, &op, timeout)
                .await
            {
                constellation_meta::MutateOutcome::Accepted { .. } => {
                    stats.forward_ok.fetch_add(1, Ordering::Relaxed);
                }
                // Busy / NotHolder / Errno / timeout: discard, never
                // retry into a lease acquisition.
                _ => {
                    stats.forward_err.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
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
