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

/// How long to wait before the next periodic sync round after
/// `idle_rounds` consecutive rounds found nothing to do: `interval`
/// doubled per idle round, clamped to `max`, never below `interval`.
///
/// An idle node's poll is what it costs the cluster to sit still: every
/// round probes each partition's stream in S3. That probe is now a single
/// GET rather than a LIST (see `shipper::tail_part_probed`), but ten idle
/// nodes at a fixed 500 ms interval are still ~1.7M requests a day for
/// nothing. Backing off to the 10 s ceiling cuts that by 20×, on top of
/// the request-class and probe-width changes.
///
/// The cost is freshness, and only in the degraded case: gossip
/// (`P2pBridge`'s `Nudge`) resets the backoff the moment a peer publishes,
/// so with P2P up this is invisible. With P2P down, a follower's worst
/// case staleness grows from 0.5 s to `max` after ~5 idle rounds
/// (0.5+1+2+4+8 s ≈ 15 s of quiet) and snaps back to 0.5 s on the next
/// segment it applies. DESIGN.md §12's posture ("eventual S3 polling
/// closes it") is unchanged; its bound is now `max` rather than the
/// interval.
///
/// A node that *holds* a lease is clamped tighter still — see
/// [`lease_poll_cap_ms`], because this deadline is also the only thing
/// driving lease renewal.
fn next_poll_ms(interval_ms: u64, idle_rounds: u32, max_ms: u64) -> u64 {
    // Shifting by >= 64 is UB-adjacent nonsense and the product overflows
    // long before that; either way the answer is "the ceiling".
    let backoff = if idle_rounds >= u64::BITS {
        u64::MAX
    } else {
        interval_ms.saturating_mul(1u64 << idle_rounds)
    };
    backoff.min(max_ms).max(interval_ms)
}

/// The longest this node may sleep before its next sync round while it
/// holds a partition lease: a quarter TTL, so the half-TTL renewal is
/// never late and a registered handoff request is seen within the window
/// plan 26 Step 7 promises. `None` when it holds nothing — then there is
/// no lease to maintain and the idle backoff runs to its ceiling.
///
/// A quarter rather than a half so a single slow round cannot push the
/// renewal past its deadline. The added traffic is small next to what a
/// holder already generates: it must do a renewal HEAD+PUT every TTL/2
/// regardless, and this adds at most two more probe rounds per TTL.
async fn lease_poll_cap_ms(
    keepers: &Arc<tokio::sync::Mutex<HashMap<String, lease::LeaseKeeper>>>,
) -> Option<u64> {
    keepers
        .lock()
        .await
        .values()
        .filter(|k| !k.is_lost() && k.ship_epoch().is_some())
        .map(|k| (k.ttl_ms() / 4).max(1))
        .min()
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
    meta: Arc<Meta>,
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

        let fault_forward_delay_ms = fault_forward_reply_delay_ms();
        if fault_forward_delay_ms > 0 {
            tracing::warn!(
                "fault injection: delaying forwarded-mutation replies by \
                 {fault_forward_delay_ms} ms (testing only)"
            );
        }

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

        // A snapshot is a retained metadata root (plan 28), so taking one
        // forces a publish on the sync task, which owns the shipper. A
        // read-only member publishes nothing and so cannot take one.
        let snapshots = Arc::new(if read_only_member {
            snapshots_base
        } else {
            let tx = sync_tx.clone();
            snapshots_base.with_publisher(Arc::new(move || {
                let tx = tx.clone();
                Box::pin(async move {
                    let (reply, receive) = tokio::sync::oneshot::channel();
                    tx.send(fusefs::SyncRequest::Publish { reply })
                        .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
                    receive
                        .await
                        .map_err(|_| anyhow::anyhow!("metadata publish stopped"))?
                        .map_err(|e| anyhow::anyhow!(e))
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
        let upload = Arc::new(crate::UploadRuntime::new(
            caps.create_if_absent,
            Some(coop.clone()),
            crate::existence::Existence::with_meta(meta.clone()),
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
        // The interval is the *floor* of an exponential idle backoff; this
        // is its ceiling (see `next_poll_ms`).
        let idle_max_ms: u64 = std::env::var("CONSTELLATION_SYNC_IDLE_MAX_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(SYNC_IDLE_MAX_MS);
        let ship = shipper::Shipper::attach_with_mode(meta.clone(), log, node_id, lease_mode)?;
        let spool = ship.spool.clone();
        let mut ship = ship;
        if !read_only_member {
            rt.block_on(async { crate::adopt_root(&meta, &mut ship, &mut keeper).await })
                .context("adopting the root directory owner")?;
        }
        ship.set_peers(peers.clone());
        ship.set_designations(designations.clone());
        // Plan 28 §11: publish the §P6 tree on the publish cadence.
        // A read-only member publishes nothing: it ships no segments, so
        // it has no authority to commit one.
        if !read_only_member {
            let publisher = crate::mtree_publish::TreePublisher::new(
                meta.clone(),
                tree_access.nodes.clone(),
                tree_access.blobs.clone(),
                constellation_store_s3::CommitChain::new(store.inner().clone())
                    .with_sealing(tree_sealing.clone()),
                tree_access.config,
                node_id,
                rt.clone(),
            );
            ship.enable_tree_publish(publisher)
                .context("restoring the published metadata tree")?;
        }
        if let Some(publisher) = ship.publisher_handle() {
            rt.spawn(async move {
                if let Err(e) = publisher.lock().await.warm_up().await {
                    tracing::debug!(error = %e, "publisher warm-up failed; the first publish will retry it");
                }
            });
        }
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
                sync_tx,
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
                sync_tx.clone(),
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
                // Consecutive poll-triggered rounds that found nothing to
                // do. Drives the backoff; any request at all resets it.
                let mut idle_rounds: u32 = 0;
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
                    // Any request — a FUSE nudge, a barrier, an acquire, a
                    // forward, or a gossip-driven `Nudge` from `P2pBridge`
                    // — means this node is not idle. Collapse the backoff
                    // before handling it, so the round it triggers is
                    // followed by a prompt one.
                    //
                    // The `idle_rounds > 0` guard is what keeps this from
                    // re-introducing the starvation the persistent deadline
                    // exists to prevent: the deadline is pushed out at most
                    // once per idle period, not once per request, so a peer
                    // sending requests faster than the interval still
                    // cannot hold the periodic round off forever.
                    if request.is_some() && idle_rounds > 0 {
                        tracing::debug!(
                            idle_rounds,
                            interval_ms,
                            "sync request ended the idle backoff"
                        );
                        idle_rounds = 0;
                        poll.as_mut().reset(
                            tokio::time::Instant::now()
                                + std::time::Duration::from_millis(interval_ms),
                        );
                    }
                    // Only the periodic poll's own rounds count towards the
                    // backoff; a round somebody asked for is by definition
                    // not this node sitting still.
                    let poll_triggered = request.is_none();
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
                            if keeper.is_paused_for_handoff() {
                                // The sync task is mid-way through forcing
                                // this partition's backlog to zero for an
                                // overdue handoff (plan 29 M3c). Answering
                                // `Plan::Held` here — which is what a
                                // same-node classify would otherwise see —
                                // would let this node's own writes keep
                                // flowing and defeat the pause. Decline;
                                // the FUSE thread's own backoff retries
                                // well within the pause's short budget.
                                tracing::debug!(part, "declining Acquire: paused for handoff");
                                let _ = reply.send(Ok(fusefs::AcquireProgress::busy(0, 0)));
                                continue;
                            }
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
                            // A fresh classify (one extra GET, only paid on
                            // the busy path) gives the caller a holder/epoch
                            // snapshot to detect progress by — see
                            // `fusefs::AcquireProgress`'s doc.
                            let reply_progress = match r {
                                Ok(true) => Ok(fusefs::AcquireProgress::acquired()),
                                Ok(false) => Ok(match keeper.classify().await {
                                    Ok(lease::Plan::Busy { holder, prev, .. }) => {
                                        fusefs::AcquireProgress::busy(holder, prev.epoch)
                                    }
                                    _ => fusefs::AcquireProgress::busy(0, 0),
                                }),
                                Err(e) => Err(format!("{e:#}")),
                            };
                            let _ = reply.send(reply_progress);
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
                            // Symmetric with the `Forward` arm above (plan
                            // 29 M5): a peer's incoming forwarded mutation
                            // needs no ordering gate here (each remote
                            // requester already serializes its own
                            // overlapping ops before sending; fjall's
                            // single-writer tx serializes concurrent
                            // `holder_execute` calls regardless of arrival
                            // order, which is all correctness requires on
                            // this side), but it still deserves to run off
                            // this loop: under real concurrent load from
                            // several peers, funnelling every request
                            // through one synchronous arm serializes their
                            // per-message scheduling/wake overhead even
                            // though each `holder_execute` itself is fast,
                            // and — on a cold cache — this arm's holder
                            // lookup can hit an S3 `GET`.
                            let ship_floor =
                                spool.lock().unwrap().head_seq.saturating_add(1);
                            let (keepers, forward, store_inner, meta, lease_views, placement, sync_tx) = (
                                keepers.clone(),
                                forward.clone(),
                                store_inner.clone(),
                                meta.clone(),
                                lease_views.clone(),
                                placement.clone(),
                                sync_tx.clone(),
                            );
                            tokio::spawn(async move {
                                // Check the lease and execute under one keeper-lock
                                // hold: handoff/release take the same lock around
                                // their final flush + release CAS, so an accepted
                                // op can never land after that flush under an
                                // epoch the requester is about to supersede.
                                // `holder_execute` is a local fjall write, so the
                                // hold is short; the not-holder lookup below (may
                                // GET the lease from S3) runs after it is dropped.
                                let executed = {
                                    let keepers = keepers.lock().await;
                                    let (ship_epoch, is_lost) = keepers
                                        .get(&part)
                                        .map(|keeper| (keeper.ship_epoch(), keeper.is_lost()))
                                        .unwrap_or((None, false));
                                    (ship_epoch.is_some() || is_lost).then(|| {
                                        forward::holder_execute(
                                            &meta, ship_epoch, is_lost, node_id, &op, ship_floor,
                                        )
                                    })
                                };
                                let outcome = if let Some(outcome) = executed {
                                    outcome
                                } else {
                                let known_holder = if let Some(holder) = forward.cached_holder(&part) {
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
                                forward::holder_execute(&meta, None, false, known_holder, &op, ship_floor)
                                };
                                if matches!(
                                    outcome,
                                    constellation_meta::MutateOutcome::Accepted { .. }
                                ) {
                                    if let Some(view) = lease_views.lock().unwrap().get(&part) {
                                        view.touch();
                                    }
                                    placement.note_forwarded(requester);
                                    let _ = sync_tx.send(fusefs::SyncRequest::Nudge);
                                }
                                // Fault injection only (plan 30 M0): the op
                                // above already executed and the keepers
                                // lock is already released, so this only
                                // delays the reply the requester is
                                // waiting on — see
                                // `fault_forward_reply_delay_ms`'s doc.
                                let fault_delay_ms = fault_forward_reply_delay_ms();
                                if fault_delay_ms > 0 {
                                    tokio::time::sleep(std::time::Duration::from_millis(
                                        fault_delay_ms,
                                    ))
                                    .await;
                                }
                                let _ = reply.send(outcome);
                            });
                        }
                        Some(fusefs::SyncRequest::Forward { part, op, reply }) => {
                            let local_epoch = {
                                let keepers = keepers.lock().await;
                                keepers.get(&part).and_then(|keeper| keeper.ship_epoch())
                            };
                            if let Some(epoch) = local_epoch {
                                // We hold the lease: this is a local fjall
                                // write, not a network round trip, so it
                                // stays inline (plan 29 M5 only targets the
                                // network-bound non-holder path below).
                                let outcome = match constellation_meta::execute_mutate(&meta, &op)
                                {
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
                                };
                                if matches!(
                                    outcome,
                                    constellation_meta::MutateOutcome::Accepted { .. }
                                ) {
                                    pending = Some(fusefs::SyncRequest::Nudge);
                                }
                                let _ = reply.send(Ok(outcome));
                                continue;
                            }
                            // Not the holder: forwarding this op means a
                            // network round trip (plus, on success, the
                            // local shadow+apply). Plan 29 M4 found that
                            // awaiting this inline serializes every FUSE
                            // thread's forward behind whichever one is
                            // currently in flight, well past the point
                            // where the round trip itself completes.
                            // Spawn it instead so this loop returns to
                            // `sync_rx.recv()` immediately; the requester-
                            // side ordering gate (plan 29 M5,
                            // `crate::keygate`) keeps ops that touch a
                            // common inode from landing (and being
                            // applied here) out of the order this node
                            // issued them in, and the semaphore bounds how
                            // many such round trips run at once.
                            let (peers, forward, meta, store_inner, sync_tx) = (
                                peers.clone(),
                                forward.clone(),
                                meta.clone(),
                                store_inner.clone(),
                                sync_tx.clone(),
                            );
                            tokio::spawn(async move {
                                // Gate first: a forward queued behind an
                                // overlapping one must not sit on an in-flight
                                // permit that a disjoint forward could use.
                                let keys = forward::conflict_keys(&op, &meta);
                                let _gate = forward.gate.acquire(keys).await;
                                let _permit = forward.inflight.clone().acquire_owned().await;
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
                                let outcome = if let Some(mut holder) = holder {
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
                                    if let constellation_meta::MutateOutcome::Exists {
                                        ref records,
                                        ship_floor,
                                    } = outcome
                                    {
                                        // The entry the holder refused us comes
                                        // back with the refusal: install it so
                                        // the caller's next lookup of that name
                                        // succeeds here too. Not shadowed —
                                        // these are the holder's own records,
                                        // and its segment re-applies them
                                        // idempotently
                                        // (`replay::insert_node`). Only below
                                        // the holder's ship floor, though
                                        // (`forward::safe_to_install_early`);
                                        // above it the bounded wait below takes
                                        // over, which only ever observes.
                                        if forward::safe_to_install_early(&meta, ship_floor) {
                                            if let Err(error) = meta.apply_records(records) {
                                                tracing::warn!(
                                                    %error,
                                                    part,
                                                    "failed to apply the entry behind an EEXIST refusal"
                                                );
                                            }
                                        }
                                    }
                                    if let constellation_meta::MutateOutcome::Accepted {
                                        epoch,
                                        ref records,
                                    } = outcome
                                    {
                                        // Installed ahead of the log for
                                        // read-your-writes: the caller's next op
                                        // on this entry resolves it here. Unlike
                                        // `Exists`, this is not skipped above
                                        // the ship floor: skipping would leave
                                        // the caller's own op invisible until
                                        // its segment lands, and its next write
                                        // would then run against a stale base
                                        // (`disjoint-write-4` fails with EIO).
                                        // The window where a *later* record for
                                        // the same entry reaches this replica
                                        // before this reply is applied needs that
                                        // record executed, made durable in S3 and
                                        // pushed back within the reply's own
                                        // 1–3 ms round trip; closing it
                                        // precisely needs the holder's journal
                                        // position in replies and segments
                                        // (tracked in PROGRESS.md, plan 29 M6).
                                        if let Err(error) =
                                            forward::apply_accepted(&meta, &part, epoch, records)
                                        {
                                            tracing::warn!(
                                                %error,
                                                part,
                                                "failed to apply accepted forwarded mutation"
                                            );
                                            let _ = reply.send(Err(error.to_string()));
                                            return;
                                        }
                                    }
                                    outcome
                                } else {
                                    forward.clear_holder(&part);
                                    constellation_meta::MutateOutcome::Busy
                                };
                                // The gate/permit guards are still held here
                                // (dropped at the end of this scope, after
                                // the apply above completed), so the next
                                // overlapping forward cannot even start its
                                // own request until this one's apply has
                                // landed.
                                // Read-your-refusals: the errno is about an
                                // entry the holder has and this replica may
                                // not (yet), so wait for the holder's segment
                                // before the caller acts on it. Still under
                                // the gate, so overlapping forwards stay
                                // ordered behind it.
                                if let Some((parent, name, exists)) =
                                    forward::causal_wait_target(&op, &outcome)
                                {
                                    let deadline = tokio::time::Instant::now() + forward::CAUSAL_WAIT;
                                    use constellation_meta::MetaStore as _;
                                    while meta.lookup(parent, &name).ok().flatten().is_some() != exists
                                        && tokio::time::Instant::now() < deadline
                                    {
                                        let _ = sync_tx.send(fusefs::SyncRequest::Nudge);
                                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                    }
                                }
                                if matches!(
                                    outcome,
                                    constellation_meta::MutateOutcome::Accepted { .. }
                                ) {
                                    // The sync loop's own state (`pending`)
                                    // is not reachable from this spawned
                                    // task; ask it for a prompt round the
                                    // same way any other caller does.
                                    let _ = sync_tx.send(fusefs::SyncRequest::Nudge);
                                }
                                let _ = reply.send(Ok(outcome));
                            });
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
                        Some(fusefs::SyncRequest::Publish { reply }) => {
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            let r = async {
                                ship.sync_all(&mut keepers).await?;
                                ship.publish_now().await
                            }
                            .await;
                            let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                        }
                        Some(fusefs::SyncRequest::TailToHead { reply }) => {
                            let mut ship = ship.lock().await;
                            let r = ship.tail_to_head().await;
                            let _ = reply.send(r.map(|_| ()).map_err(|e| format!("{e:#}")));
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
                                let part = "p0".to_string();
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
                            // poll out so it only fires when rounds have
                            // genuinely stopped happening. It is set again
                            // from the round's outcome below, which is what
                            // the idle backoff rides on; this one covers the
                            // path where a request cuts the round short.
                            poll.as_mut().reset(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(interval_ms),
                            );
                            // What "idle" means for the backoff: nothing was
                            // applied or shipped (both advance `head_seq`)
                            // and nothing is waiting to ship. A journal that
                            // stays non-empty means we are blocked on a
                            // foreign lease, not idle — backing off there
                            // would delay our own writes reaching S3.
                            let head_before = spool.lock().unwrap().head_seq;
                            let mut nudged = false;
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
                            let mut completed = false;
                            loop {
                                tokio::select! {
                                    biased;
                                    r = &mut round => {
                                        completed = true;
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
                                            // Somebody is still asking, so this is
                                            // not an idle round whatever it finds.
                                            nudged = true;
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
                            // A round interrupted by a request is neither
                            // idle nor productive: the request it yielded to
                            // resets the backoff on the next pass anyway.
                            if completed {
                                let head_after = spool.lock().unwrap().head_seq;
                                let backlog =
                                    constellation_meta::MetaStore::journal_len(&*meta).unwrap_or(0);
                                let productive = nudged
                                    || head_after != head_before
                                    || backlog > 0
                                    || !poll_triggered;
                                let was = idle_rounds;
                                if productive {
                                    idle_rounds = 0;
                                } else {
                                    idle_rounds = idle_rounds.saturating_add(1);
                                }
                                let next_ms = next_poll_ms(interval_ms, idle_rounds, idle_max_ms);
                                // The sync round is the *only* thing that
                                // renews a lease or notices a `wanted_by`
                                // handoff request: `run_sync_round` is the
                                // sole caller of `renew_if_due` and
                                // `idle_release_due`. Backing off past the
                                // renewal cadence therefore does not merely
                                // delay a read, it starves lease maintenance
                                // — with `idle_max_ms` above TTL/2 a
                                // holder sleeps through its own renewal and
                                // lets the lease expire, and Step 7's stated
                                // worst case ("the holder notices at its next
                                // renewal, <= TTL/2") silently becomes "after
                                // one backoff interval" instead.
                                let next_ms = match lease_poll_cap_ms(&keepers).await {
                                    Some(cap) => next_ms.min(cap),
                                    None => next_ms,
                                };
                                if productive && was > 0 {
                                    tracing::debug!(
                                        idle_rounds = was,
                                        next_ms,
                                        "sync round was productive; idle backoff reset"
                                    );
                                } else if !productive
                                    && next_ms >= idle_max_ms
                                    && next_poll_ms(interval_ms, was, idle_max_ms) < idle_max_ms
                                {
                                    tracing::debug!(
                                        idle_rounds,
                                        idle_max_ms,
                                        "sync idle backoff reached its ceiling; \
                                         with P2P down this is now the freshness bound"
                                    );
                                }
                                poll.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + std::time::Duration::from_millis(next_ms),
                                );
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
    meta: &Arc<Meta>,
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
    // 2. Group by partition. One stream (`p0`) since plan 29 M0a removed
    // namespace partitions; the grouping stays so the rest of this
    // function does not care how many streams there are.
    let mut by_part: HashMap<String, Vec<(constellation_fs_core::Ino, i64, i64)>> = HashMap::new();
    by_part.entry("p0".to_string()).or_default().extend(drained);
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

    /// The idle poll doubles from the configured interval and stops at the
    /// ceiling, which is what turns an idle node's steady-state S3 cost
    /// from ~2 requests/s into ~2 requests/minute (plan 26 step 4c).
    #[test]
    fn idle_poll_doubles_up_to_the_ceiling() {
        let (interval, max) = (500u64, 30_000u64);
        assert_eq!(next_poll_ms(interval, 0, max), 500);
        assert_eq!(next_poll_ms(interval, 1, max), 1_000);
        assert_eq!(next_poll_ms(interval, 2, max), 2_000);
        assert_eq!(next_poll_ms(interval, 5, max), 16_000);
        // ~6 idle rounds reach the ceiling, and no number of further
        // idle rounds goes past it.
        assert_eq!(next_poll_ms(interval, 6, max), 30_000);
        assert_eq!(next_poll_ms(interval, 7, max), 30_000);
        // At the shipped default (10 s) the ceiling arrives a round or
        // two sooner: 0.5+1+2+4+8 s of quiet.
        assert_eq!(next_poll_ms(interval, 4, SYNC_IDLE_MAX_MS), 8_000);
        assert_eq!(next_poll_ms(interval, 5, SYNC_IDLE_MAX_MS), 10_000);
        assert_eq!(next_poll_ms(interval, 99, SYNC_IDLE_MAX_MS), 10_000);
        assert_eq!(next_poll_ms(interval, 4_000, max), 30_000);
        // The shift must not overflow or wrap into a short poll: 500 << 60
        // wraps in `u64`, and `<< 64` is not a shift at all.
        assert_eq!(next_poll_ms(interval, 60, max), 30_000);
        assert_eq!(next_poll_ms(interval, 64, max), 30_000);
        assert_eq!(next_poll_ms(interval, u32::MAX, max), 30_000);
        // The interval is the floor even if the ceiling is set below it.
        assert_eq!(next_poll_ms(interval, 0, 100), 500);
        assert_eq!(next_poll_ms(interval, 9, 100), 500);
        // A ceiling equal to the interval disables the backoff entirely.
        assert_eq!(next_poll_ms(interval, 3, interval), 500);
    }

    /// A lease holder may not back off past its own renewal cadence.
    ///
    /// `run_sync_round` is the only caller of `renew_if_due` and
    /// `idle_release_due`, so the poll deadline *is* the lease-maintenance
    /// deadline. Left unclamped, a 30 s ceiling over a 5 s TTL lets the
    /// lease expire while the node still believes it holds it — which is
    /// how `node-leave` came to fail with EIO — and stretches plan 26
    /// Step 7's "the holder notices at its next renewal (<= TTL/2)" into
    /// "after one backoff interval".
    #[test]
    fn a_lease_holder_never_backs_off_past_its_renewal() {
        let (interval, max) = (500u64, 30_000u64);
        // The pathological case: ceiling far above the whole TTL.
        let cap = |ttl: u64| (ttl / 4).max(1);
        let at_ceiling = next_poll_ms(interval, 6, max);
        assert_eq!(at_ceiling, 30_000);

        // 5 s TTL renews at 2.5 s: the clamped poll must beat that.
        assert_eq!(at_ceiling.min(cap(5_000)), 1_250);
        assert!(at_ceiling.min(cap(5_000)) < 5_000 / 2);
        // 60 s TTL renews at 30 s: 15 s still leaves a whole round of slack.
        assert_eq!(at_ceiling.min(cap(60_000)), 15_000);
        assert!(at_ceiling.min(cap(60_000)) < 60_000 / 2);
        // The clamp only ever shortens the wait; a node holding nothing
        // (no cap) keeps the full ceiling.
        assert_eq!(at_ceiling, 30_000);
        // And it never turns into a busy loop on an absurdly short TTL.
        assert_eq!(cap(1), 1);
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
