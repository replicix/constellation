//! [`Engine`]: one node — one identity, one metadata replica, one chunk
//! cache — and the background tasks that keep it a member of its cluster
//! (plan 31 §4, §4.1). [`Engine::open_view`] opens a [`View`] of it for a
//! frontend; a host ([`crate::EngineHost`], a daemon, a CSI engine pod)
//! runs any number of views of one engine, and any number of engines.
//!
//! Plan 31 C4c moved this out of the `constellation` binary, where it was
//! `NodeRuntime::start` (plan 21's per-node half of the old monolithic
//! `mount()`): the backend, `meta.json` and its E2E unlock, the replica's
//! bootstrap and pruned-past rebuild, node identity and incarnation, the
//! staging and chunk caches, the metadata tree, the conditional-write
//! probe, P2P, the periodic GC and completed-table prune, epochs,
//! designations, coop, the authority driver (the sync task) and every
//! ticker (registry and roster, designations, placement, open-orphan
//! holds, atime, the pruner), root adoption, kernel-invalidation
//! delivery, and the clean shutdown's drain. The daemon host keeps what
//! is a host's: the control socket and web UI, the FUSE sessions and
//! their threads, `daemon.lock`/`daemon.pid`, signals.
//!
//! Environment knobs are read where they always were (`CONSTELLATION_*`,
//! documented at each read); an [`EngineConfig`] field overrides one only
//! where it says so.

use crate::view::{
    FsDependencies, HandleTableSnapshot, QuotaCache, SyncHandle, View, ViewHandoff, ViewSpec,
};
use crate::{
    coop, designation, epoch, forward, kernel_inval, lease, pin, placement, reintegrate, shipper,
    snapshot, staging, writeback,
};
use anyhow::{bail, Context, Result};
use constellation_fs_core::cache::{CacheVerify, DiskCache};
use constellation_meta::Meta;
use constellation_platform::{CredentialSource, HostServices};
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta};
use constellation_vfs::{FrontendCaps, FrontendEvents, OpWatch};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

/// Where an E2E filesystem's passphrase comes from.
pub enum PassphraseSource {
    /// Collected by the host already (a daemon asks in the foreground,
    /// before it forks away from the terminal).
    Given(Zeroizing<String>),
    /// Asked for only if the filesystem turns out to be encrypted (an
    /// environment variable, a terminal prompt, a CSI secret).
    Ask(Box<dyn FnOnce() -> Result<Zeroizing<String>> + Send>),
    /// None: an encrypted filesystem fails to start.
    Absent,
}

/// A host's hook into startup's phases (the daemon's `startup` watchdog
/// names the phase a stuck startup is in).
pub type PhaseHook = Arc<dyn Fn(&str) + Send + Sync>;

fn phase(hook: &Option<PhaseHook>, name: &str) {
    match hook {
        Some(hook) => hook(name),
        None => tracing::info!(phase = name, "engine startup phase"),
    }
}

/// Everything that opens one node of one filesystem (plan 31 §4): what
/// the daemon's `mount` flags and the registry say about it.
pub struct EngineConfig {
    /// `s3://bucket/prefix`, `file:///path`, or an absolute path.
    pub backend: String,
    /// The node's local state (replica, caches, staging); `None`: the
    /// host's state dir for the filesystem's UUID ([`default_state_dir`]).
    pub state_dir: Option<PathBuf>,
    /// Chunk-cache bytes (`--cache-size`), capped by the host's share.
    pub cache_size: u64,
    /// Staging bytes; `None`: `CONSTELLATION_STAGING_BUDGET`, else a
    /// quarter of the cache.
    pub staging_budget: Option<u64>,
    /// `--cache-verify admit|always` (plan 38 §2.3): whether a disk-cache
    /// read re-hashes the file it read. `None`:
    /// `CONSTELLATION_CACHE_VERIFY`, else `admit`.
    pub cache_verify: Option<CacheVerify>,
    /// `--fsync-mode s3`.
    pub fsync_s3: bool,
    /// Plan 39: `--fsync-timeout`, the opt-in soft bound on how long an
    /// `fsync` waits for an unreachable S3. `None` (no flag):
    /// `CONSTELLATION_FSYNC_TIMEOUT`, else wait forever; `Some(None)`: an
    /// explicit `hard` (`0`/`off`/`hard`), which the environment does not
    /// override; `Some(Some(t))`: soft, `t`.
    pub fsync_timeout: Option<Option<std::time::Duration>>,
    /// `--cto strict`.
    pub cto_strict: bool,
    /// `--locks cluster` (`Some(true)`) / `local` (`Some(false)`) /
    /// neither (cluster when P2P runs).
    pub locks: Option<bool>,
    pub initial_write_mode: writeback::WriteMode,
    /// `--read-only-member` (fixed on the state dir's first start).
    pub read_only_member: bool,
    pub atime_mode: crate::atime::AtimeMode,
    pub passphrase: PassphraseSource,
    /// The local E2E pin `meta.json` is held against; `None` skips it.
    pub pin_target: Option<crate::e2e_pin::PinTarget>,
    /// The engine's S3 credentials (plan 31 §9.8).
    pub credentials: CredentialSource,
    /// The version this node publishes in the registry.
    pub version: String,
    /// The tokio runtime the engine runs on; `None`: the one this call is
    /// made in the context of (`Handle::try_current`). An
    /// [`crate::EngineHost`] sets its own. Startup blocks on it, so
    /// [`Engine::start`] must not be called from one of its workers.
    pub runtime: Option<tokio::runtime::Handle>,
    pub on_phase: Option<PhaseHook>,
    /// The space-accounting service's knobs (plan 32 §6.3); `None`:
    /// `CONSTELLATION_SNAPACCT*` ([`crate::snapacct::SnapAcctConfig::from_env`]).
    pub snapacct: Option<crate::snapacct::SnapAcctConfig>,
}

impl EngineConfig {
    /// `backend` with the daemon's defaults: a 10 GiB cache, `fsync`
    /// local, `cto` bounded, write-through, atime off, the AWS chain.
    pub fn new(backend: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            state_dir: None,
            cache_size: 10 * 1024 * 1024 * 1024,
            staging_budget: None,
            cache_verify: None,
            fsync_s3: false,
            fsync_timeout: None,
            cto_strict: false,
            locks: None,
            initial_write_mode: writeback::WriteMode::Through,
            read_only_member: false,
            atime_mode: crate::atime::AtimeMode::Off,
            passphrase: PassphraseSource::Absent,
            pin_target: None,
            credentials: CredentialSource::AwsDefaultChain,
            version: env!("CARGO_PKG_VERSION").to_string(),
            runtime: None,
            on_phase: None,
            snapacct: None,
        }
    }
}

/// `<data dir>/<uuid>` (the host's `Dirs::state_dir`); without a `HOME`,
/// relative to the working directory, as ever.
pub fn default_state_dir(host: &HostServices, meta: &FsMeta) -> PathBuf {
    let uuid = meta.uuid.to_string();
    host.dirs
        .state_dir(&uuid)
        .unwrap_or_else(|_| PathBuf::from(".local/share/constellation").join(uuid))
}

/// A frontend's [`FrontendEvents`] that exists only once the frontend
/// does: a FUSE session's notifier is born with the mount, and the mount
/// needs the view first. Invalidations before [`Self::set`] are dropped
/// (nothing can be cached yet).
#[derive(Default)]
pub struct DeferredEvents(OnceLock<Arc<dyn FrontendEvents>>);

impl DeferredEvents {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Deliver to `events` from now on (the first call wins).
    pub fn set(&self, events: Arc<dyn FrontendEvents>) {
        let _ = self.0.set(events);
    }
}

impl FrontendEvents for DeferredEvents {
    fn invalidate(&self, batch: &[constellation_vfs::Invalidation]) {
        if let Some(events) = self.0.get() {
            events.invalidate(batch);
        }
    }
}

/// One open view, as [`Engine::views`] lists it.
#[derive(Debug, Clone)]
pub struct ViewInfo {
    pub id: u64,
    /// `ViewSpec::root` as given.
    pub root: String,
    pub labels: BTreeMap<String, String>,
    pub since: Instant,
}

struct OpenView {
    info: ViewInfo,
    /// The spec it resolved to (`Engine::export_view`).
    resolved: ViewSpec,
    quota_cache: QuotaCache,
    /// The view's cached subtree cap (`View::cached_subtree_quota`).
    subtree_quota_cache: QuotaCache,
    /// A `--rw --ephemeral` clone, removed when the view closes.
    ephemeral_clone: Option<String>,
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
/// (`docs/plans/v1/done/30-write-path-resilience-and-scale-out.md`
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

/// One node (see the module doc).
pub struct Engine {
    host: HostServices,
    profile: crate::EngineProfile,
    allotment: crate::ResourceBudget,
    node_id: u64,
    /// Plan 30 §M2: this node's incarnation (bumped once, before serving,
    /// in `Engine::start`). Part of every rid it allocates.
    incarnation: u32,
    /// Plan 30 §M2: the next `seq` to allocate within this incarnation,
    /// shared with every view's `SyncHandle`, so rid allocation is unique
    /// across views. Volatile — the incarnation bump keeps that safe.
    next_rid_seq: Arc<AtomicU64>,
    fsmeta: FsMeta,
    backend_url: String,
    state_dir: PathBuf,
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    compression: CompressionSetting,
    snapshots: Arc<snapshot::SnapshotManager>,
    /// Plan 32 Step 0.1: every snapshot row write, routed to the
    /// root-lease holder.
    snapshot_batches: Arc<crate::snapshot_batch::SnapshotBatcher>,
    /// Plan 32 Steps 3.2–3.3: the snapshot scheduler (its ticker runs on
    /// every node; one leads).
    snapsched: Arc<crate::snapsched::Scheduler>,
    staging_dir: PathBuf,
    staging_budget: Arc<staging::StagingBudget>,
    lease_mode: constellation_store_s3::LeaseMode,
    /// The authority core's lease state, mirrored for the views' fast path.
    lease: Arc<lease::LeaseView>,
    /// Plan 30 §M11: the delegations this node holds.
    delegates: Arc<lease::DelegateView>,
    /// The core's observable state, for `status` and the tickers.
    core_status: Arc<Mutex<crate::authority_driver::CoreStatus>>,
    /// Rid seqs the views' fast path completed, drained by the driver.
    pending_acks: Arc<Mutex<Vec<u64>>>,
    acquire_deadline: Duration,
    write_mode: Arc<writeback::WriteModeState>,
    sync_tx: tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
    peers: constellation_net::Peers,
    epochs: Arc<epoch::EpochManager>,
    designations: Arc<designation::DesignationManager>,
    coop: Arc<coop::Coop>,
    upload: Arc<crate::upload::UploadRuntime>,
    forward: Arc<forward::ForwardState>,
    placement: Arc<placement::Placement>,
    departed: Arc<AtomicBool>,
    /// Node-level read-time atime accumulator (plan 20), shared by every
    /// view and drained by the flush ticker.
    atime: Arc<crate::atime::AtimeAccumulator>,
    /// Node-level prune counters (plan 22).
    prune_stats: Arc<crate::prune::PruneStats>,
    /// Node-level snapshot-schedule counters (plan 32).
    snapsched_stats: Arc<crate::snapsched::SnapSchedStats>,
    /// Plan 32 §6.3: this filesystem's space-accounting index.
    snapacct: Arc<crate::snapacct::SnapAcctService>,
    /// Unix-ms heartbeat of the sync task's last loop pass.
    last_sync_ms: Arc<AtomicU64>,
    read_only_member: bool,
    fsync_s3: bool,
    /// Plan 39: every view's `fsync` policy and waits.
    fsync_waits: Arc<crate::fsync_wait::FsyncWaits>,
    cto_strict: bool,
    /// Plan 30 §M14: the effective `--locks` mode, and every view's flush
    /// for a recalled grant.
    locks_cluster: bool,
    lock_flushers: Arc<crate::locks::LockFlushers>,
    /// Open-orphan holds (DESIGN.md §3): every view's open-handle table,
    /// and the writer of this node's `holds/<node>.json`.
    hold_sources: Arc<crate::holds::HoldSources>,
    holds: Arc<crate::holds::Holds>,
    pins: Arc<pin::PinManager>,
    reintegration: Arc<reintegrate::ReintegrationState>,
    stop: Arc<AtomicBool>,
    rt: tokio::runtime::Handle,
    started: Instant,
    /// Plan 30 §M7: drops the frontends' cached view of what another
    /// node's writes changed; `None` when `CONSTELLATION_KERNEL_INVALIDATE=0`.
    kernel_inval: Option<kernel_inval::KernelInvalidator>,
    /// The request watchdog every view's ops register with: one per node,
    /// so `status` keeps counting across views coming and going.
    op_watch: OpWatch,
    views: Mutex<HashMap<u64, OpenView>>,
    next_view_id: AtomicU64,
    /// Where its S3 credentials come from (plan 31 §9.8): `fs.unlock`
    /// rotates a static source in place.
    credentials: Arc<CredentialSource>,
    shutdown_started: AtomicBool,
    /// Why the shutdown could not ship everything, when it could not.
    shutdown_error: Mutex<Option<String>>,
    /// Plan 31 C8: the host's lifecycle events, applied.
    lifecycle: Arc<crate::lifecycle::Lifecycle>,
}

impl Engine {
    /// Open and start one node (see the module doc): no view is open yet.
    pub fn start(
        cfg: EngineConfig,
        host: HostServices,
        profile: crate::EngineProfile,
    ) -> Result<Engine> {
        Self::start_with(cfg, host, profile, crate::ResourceBudget::unlimited())
    }

    /// [`Self::start`] within `allotment`, an [`crate::EngineHost`]'s
    /// share of its budget.
    pub(crate) fn start_with(
        cfg: EngineConfig,
        host: HostServices,
        profile: crate::EngineProfile,
        allotment: crate::ResourceBudget,
    ) -> Result<Engine> {
        let EngineConfig {
            backend: backend_url,
            state_dir,
            cache_size,
            staging_budget,
            cache_verify,
            fsync_s3,
            fsync_timeout,
            cto_strict,
            locks,
            initial_write_mode,
            read_only_member,
            atime_mode,
            passphrase,
            pin_target,
            credentials,
            version,
            runtime,
            on_phase,
            snapacct: snapacct_config,
        } = cfg;
        let snapacct_config =
            snapacct_config.unwrap_or_else(crate::snapacct::SnapAcctConfig::from_env);
        let rt = match runtime {
            Some(rt) => rt,
            None => tokio::runtime::Handle::try_current()
                .context("Engine::start needs a tokio runtime (EngineConfig::runtime)")?,
        };
        let credentials = Arc::new(credentials);
        // The host's share, or the profile's explicit override of it, and
        // never more than the engine asked for.
        let cache_size = cache_size.min(profile.cache_budget.unwrap_or(allotment.cache_bytes));
        let fault_forward_delay_ms = fault_forward_reply_delay_ms();
        if fault_forward_delay_ms > 0 {
            tracing::warn!(
                "fault injection: delaying forwarded-mutation replies by \
                 {fault_forward_delay_ms} ms (testing only)"
            );
        }

        phase(&on_phase, "opening the backend and meta.json");
        let (backend, backend_info) = rt
            .block_on(crate::backend::open_backend_described_with(
                &backend_url,
                Some(&credentials),
            ))
            .context("opening backend")?;
        let fsmeta = rt
            .block_on(crate::backend::load_fs_explained(
                &backend,
                &backend_info,
                None,
            ))
            .context("loading filesystem")?;
        // `meta.json` is unauthenticated: hold its E2E state against this
        // machine's pin before trusting `e2e` or the keyring block.
        let pin = pin_target
            .as_ref()
            .map(|target| crate::e2e_pin::check_in(&host, target, &fsmeta))
            .transpose()?;
        let e2e_keys = if fsmeta.e2e {
            // Prefer the passphrase collected in the foreground before the
            // fork; fall back to the env var / a prompt (works in
            // `--foreground`, where the terminal is still attached).
            let secret = match passphrase {
                PassphraseSource::Given(secret) => secret,
                PassphraseSource::Ask(ask) => ask()?,
                PassphraseSource::Absent => {
                    bail!("the filesystem is end-to-end encrypted and no passphrase was supplied")
                }
            };
            Some(
                fsmeta
                    .unlock(&secret)
                    .context("unlocking E2E keyring (wrong passphrase?)")?,
            )
        } else {
            None
        };
        if let Some(pin) = &pin {
            pin.confirm(e2e_keys.as_deref())?;
        }
        let store = Arc::new(match &e2e_keys {
            Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
            None => ChunkStore::new(backend.clone()),
        });
        let state_dir = state_dir.unwrap_or_else(|| default_state_dir(&host, &fsmeta));
        std::fs::create_dir_all(&state_dir)?;
        let log = match &e2e_keys {
            Some(keys) => constellation_store_s3::LogStore::new_e2e(backend, keys.clone()),
            None => constellation_store_s3::LogStore::new(backend),
        };
        let db_path = state_dir.join("meta.db");
        // Fresh node: rebuild the replica from the commit chain plus log
        // replay (or, with no commit yet, a genesis replay of the whole log).
        let existing_replica = db_path.exists();
        if !existing_replica {
            phase(&on_phase, "bootstrapping the metadata replica from S3");
            rt.block_on(shipper::bootstrap(&db_path, &log))
                .context("bootstrapping metadata replica")?;
        }
        phase(&on_phase, "opening meta.db");
        let meta = Arc::new(Meta::open(&db_path)?);
        meta.scratch_purge_all()?;
        if existing_replica {
            // A replica the log was pruned past while this node was
            // offline is rebuilt from the head commit before anything
            // tails or mounts (DESIGN.md §14 "Falling behind segment GC").
            phase(&on_phase, "checking the replica against the retained log");
            rt.block_on(shipper::rebuild_if_pruned(&meta, &log, &state_dir))
                .context("checking the replica against the retained log")?;
        }
        // Plan 31 C8: the node's background work pauses as the profile and
        // the host's lifecycle say (`crate::lifecycle`).
        let background = crate::lifecycle::BackgroundGate::new();
        spawn_vacuum(&meta, &background);
        if matches!(meta.kv_get("left")?.as_deref(), Some("1")) {
            bail!(
                "this state directory has permanently left the cluster \
                 (kv left=1); mount with a fresh --state-dir to re-enroll \
                 under a new node id"
            );
        }
        // Node identity: claim a cluster-unique id on first mount of this
        // state dir; it scopes ino allocation and marks log segment origin.
        phase(&on_phase, "resolving the node identity in the registry");
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
        // `EngineConfig::staging_budget`, else `CONSTELLATION_STAGING_BUDGET`
        // (bytes), else a quarter of the cache.
        let staging_budget_bytes: u64 = staging_budget
            .or_else(|| {
                std::env::var("CONSTELLATION_STAGING_BUDGET")
                    .ok()
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(cache_size / 4)
            .min(allotment.staging_bytes);
        let staging_budget = staging::StagingBudget::new(staging_budget_bytes);

        // Verified chunk contents in memory (`fs-core::memcache`), sized
        // from this engine's memory share (`EngineProfile::chunk_memcache`).
        let chunk_memcache = crate::profile::chunk_memcache_bytes(
            &profile,
            profile.memory_budget.unwrap_or(allotment.memory_bytes),
            cache_size,
        );
        tracing::info!(bytes = chunk_memcache, "chunk memory cache");
        // Plan 38 §2.3: `admit` trusts a chunk file this process hashed
        // (in flight on the fetch, or on the first read of a file the
        // startup scan found); `always` re-hashes every disk read.
        let cache_verify = crate::profile::cache_verify(cache_verify);
        tracing::info!(mode = cache_verify.as_str(), "disk cache verification");
        let cache = Arc::new(
            match &e2e_keys {
                Some(keys) => DiskCache::open_keyed(
                    state_dir.join("cache"),
                    cache_size,
                    *keys.addressing_key(),
                )?,
                None => DiskCache::open(state_dir.join("cache"), cache_size)?,
            }
            .with_memory_cache(chunk_memcache)
            .with_verify(cache_verify),
        );
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
                .with_tree(tree_access.clone())
                .with_host(&host);

        // Write authority (DESIGN.md §4/§5). Renew and takeover need
        // If-Match; a backend without it can only be driven safely by one
        // node at a time, so say so loudly and fall back to create-only
        // lease semantics instead of refusing to mount at all.
        phase(&on_phase, "probing the backend's conditional writes");
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
        let (sync_tx, sync_rx) = tokio::sync::mpsc::unbounded_channel::<crate::sync::SyncRequest>();

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
                        tx.send(crate::sync::SyncRequest::Publish { reply })
                            .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
                        match receive
                            .await
                            .map_err(|_| anyhow::anyhow!("metadata publish stopped"))?
                        {
                            Ok(commit) => return Ok(commit),
                            // Plan 32 Step 0.1: snapshots now run at the
                            // holder, which may be writing continuously; a
                            // publish that kept deferring behind its writes
                            // is retried the same way.
                            Err(e)
                                if (e.contains(crate::mtree_publish::SPECULATION_OUTSTANDING)
                                    || e.contains("speculation outstanding")
                                    || e.contains(crate::mtree_publish::PUBLISH_DEFERRED))
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
        phase(&on_phase, "starting P2P (endpoint bind, registry peers)");
        let peers = if profile.p2p_enabled() {
            rt.block_on(crate::p2p::start_p2p(
                &host,
                &fsmeta,
                e2e_keys.as_ref(),
                store.inner().clone(),
                node_id,
                &version,
            ))
        } else {
            tracing::info!("P2P off by the engine profile; using the S3 path only");
            constellation_net::Peers::disabled()
        };
        // Plan 31 C8: a dial-only endpoint refuses inbound connections
        // from before it serves any.
        peers.set_dial_only(profile.p2p == crate::P2pMode::DialOnly);
        phase(
            &on_phase,
            "configuring the node (roster, designations, lease)",
        );
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
            let background = background.clone();
            rt.spawn(async move {
                let mut timer =
                    tokio::time::interval(std::time::Duration::from_secs(interval.max(1)));
                timer.tick().await;
                loop {
                    timer.tick().await;
                    background.wait_active().await;
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
            let background = background.clone();
            rt.spawn(async move {
                let mut timer =
                    tokio::time::interval(std::time::Duration::from_secs(prune_interval));
                timer.tick().await;
                loop {
                    timer.tick().await;
                    background.wait_active().await;
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
        coop.set_epoch_members(epochs.members_open.clone());
        coop.set_background(background.clone());
        let upload = Arc::new(crate::upload::UploadRuntime::new(
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
        let snapsched_stats = crate::snapsched::SnapSchedStats::new();
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
        // Plan 31 C8: `LeaseMode::ForwardOnly` from the first op on.
        core_config.forward_only = profile.leases == crate::LeaseMode::ForwardOnly;
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
        pins.set_background(background.clone());
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
        // Open-orphan holds (DESIGN.md §3): the writer is built here so
        // the sync task can nudge it; its loop starts once `stop` exists.
        let hold_sources = Arc::new(crate::holds::HoldSources::default());
        let holds = crate::holds::Holds::new(
            store.inner().clone(),
            store.clone(),
            meta.clone(),
            node_id,
            hold_sources.clone(),
            crate::holds::HoldConfig::from_env(core_config.ttl_ms),
        );
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
                    holds: Some(holds.clone()),
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
        let _ = sync_tx.send(crate::sync::SyncRequest::Roster(initial_roster));
        if !read_only_member {
            phase(&on_phase, "adopting the root directory owner");
            rt.block_on(adopt_root(&host, &meta, &sync_tx, &forward, node_id))
                .context("adopting the root directory owner")?;
        }
        phase(&on_phase, "starting the node's background tasks");
        let snapshot_batches = Arc::new(crate::snapshot_batch::SnapshotBatcher::new(
            node_id,
            meta.clone(),
            snapshots.clone(),
            Arc::new(crate::snapshot_batch::EngineBatchHost {
                node_id,
                lease: lease_view.clone(),
                sync_tx: sync_tx.clone(),
                store: store.inner().clone(),
                lease_mode,
                peers: peers.clone(),
                next_req: std::sync::atomic::AtomicU64::new(1),
            }),
            forward.clone(),
            read_only_member,
        ));
        let bridge = Arc::new(crate::p2p::P2pBridge {
            snapshot_batches: snapshot_batches.clone(),
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
                        let _ = crate::p2p::refresh_peers(&p, store_inner, Some(&epochs)).await;
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
                        crate::p2p::refresh_peers(&peers, store_inner.clone(), Some(&epochs)).await;
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
                let version = version.clone();
                let background = background.clone();
                rt.spawn(async move {
                    let mut tick: u64 = 0;
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        background.wait_active().await;
                        let scan =
                            crate::p2p::refresh_peers(&peers, store_inner.clone(), Some(&epochs))
                                .await;
                        let _ = sync_tx.send(crate::sync::SyncRequest::Roster(epochs.roster()));
                        peers.probe_all().await;
                        tick += 1;
                        if tick.is_multiple_of(SLACK_REREAD_TICKS) {
                            reread_slack(&store_inner, &sync_tx).await;
                        }
                        if let Some(scan) = scan.as_ref() {
                            crate::p2p::republish_addr_if_changed(
                                &peers,
                                store_inner.clone(),
                                node_id,
                                &version,
                                scan,
                            )
                            .await;
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
                                let _ = sync_tx.send(crate::sync::SyncRequest::Retired);
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
            let background = background.clone();
            rt.spawn(async move {
                let mut tick: u64 = 0;
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    background.wait_active().await;
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
                        let _ = sync_tx_roster.send(crate::sync::SyncRequest::Roster(roster));
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
                            let _ = sync_tx_roster.send(crate::sync::SyncRequest::Retired);
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
            let background = background.clone();
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    background.wait_active().await;
                    designations.refresh().await;
                    // Plan 30 §M11 phase 2b: the root keeps the table in
                    // step with the designations (a no-op elsewhere).
                    let entries = designations.delegation_entries();
                    let _ = deleg_tx.send(crate::sync::SyncRequest::SyncDesignations { entries });
                }
            });
        }
        if peers.is_enabled() {
            let (placement, peers, lease_view) =
                (placement.clone(), peers.clone(), lease_view.clone());
            let background = background.clone();
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    background.wait_active().await;
                    let status = lease_view.status();
                    if !(status.held && status.holder == node_id) || lease_view.is_lost() {
                        continue;
                    }
                    placement.gossip_rtts(&peers, node_id).await;
                    if let Some(best) = placement.recommend(node_id, &peers) {
                        placement.note_offer(best);
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
        rt.spawn(holds.clone().run(stop.clone()));
        // Plan 32 §6.3: the space-accounting index. Under the default
        // `auto` its task sleeps until something asks for a size.
        let snapacct = crate::snapacct::SnapAcctService::new(
            snapacct_config,
            crate::snapacct::SnapAcctDeps {
                meta: meta.clone(),
                chunks: store.clone(),
                tree: tree_access.clone(),
                // A refresh with nothing new is then one GET that misses
                // and one of the known head (`discover_head`).
                commits: constellation_store_s3::CommitChain::new(store.inner().clone())
                    .with_sealing(tree_sealing.clone())
                    .with_probe_window(1),
                dir: state_dir.join(crate::snapacct::service::DIR),
                fs_uuid: fsmeta.uuid.to_string(),
            },
        );
        rt.spawn(snapacct.clone().run(stop.clone(), Some(background.clone())));
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
            let background = background.clone();
            rt.spawn(async move {
                let period = crate::atime::flush_interval();
                loop {
                    tokio::time::sleep(period).await;
                    background.wait_active().await;
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
        // Snapshot scheduler ticker (plan 32 Steps 3.2–3.3). Without a
        // `user.constellation.snapshots` policy anywhere a tick reads the
        // local replica's xattr index and stops: no S3 request at all.
        let snapsched = crate::snapsched::Scheduler::new(crate::snapsched::SchedDeps {
            node_id,
            store: store.inner().clone(),
            meta: meta.clone(),
            batches: snapshot_batches.clone(),
            lease_mode,
            read_only_member,
            departed: departed.clone(),
            epoch_frozen: Some(epochs.writes_refused.clone()),
            last_sync_ms: last_sync_ms.clone(),
            stats: snapsched_stats.clone(),
            config: crate::snapsched::SchedConfig::from_env(),
            clock: crate::snapsched::wall_clock(),
        });
        snapsched.spawn(&rt, stop.clone(), Some(background.clone()));
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
            let background = background.clone();
            rt.spawn(async move {
                let mut timer = tokio::time::interval(crate::prune::interval());
                timer.tick().await;
                loop {
                    timer.tick().await;
                    background.wait_active().await;
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
                .send(crate::sync::SyncRequest::Reintegrate(reply))
                .map_err(|_| anyhow::anyhow!("sync task stopped before automatic reintegration"))?;
            rt.block_on(receive)
                .context("automatic reintegration task stopped")?
                .map_err(anyhow::Error::msg)
                .context("automatic reintegration after mount")?;
        }

        let kernel_inval = kernel_inval::enabled().then(kernel_inval::KernelInvalidator::start);
        {
            // Every foreign apply feeds the kernel invalidations and the
            // cooperative cache's fresh-chunk hints (who wrote the chunks
            // a new manifest names, `coop::fresh`).
            let inval = kernel_inval.as_ref().map(|k| k.hook());
            let coop = coop.clone();
            meta.set_foreign_apply_hook(Box::new(move |records| {
                if let Some(inval) = &inval {
                    inval(records);
                }
                coop.note_foreign_records(records);
            }));
        }
        // Plan 31 C8: the host's lifecycle, applied from now on.
        let lifecycle = crate::lifecycle::Lifecycle::new(
            profile.clone(),
            crate::lifecycle::LifecycleDeps {
                sync_tx: sync_tx.clone(),
                peers: peers.clone(),
                upload: upload.clone(),
                write_mode: write_mode.clone(),
                meta: meta.clone(),
                lease: lease_view.clone(),
                core_status: core_status.clone(),
                background,
                rt: rt.clone(),
            },
        );
        if profile.leases == crate::LeaseMode::ForwardOnly
            || profile.p2p == crate::P2pMode::DialOnly
            || profile.uploads == crate::UploadMode::UnmeteredOnly
            || profile.background == crate::BackgroundMode::OnDemand
        {
            tracing::info!(
                p2p = profile.p2p.as_str(),
                leases = profile.leases.as_str(),
                uploads = profile.uploads.as_str(),
                background = profile.background.as_str(),
                "engine profile"
            );
        }
        lifecycle.spawn(&*host.lifecycle, stop.clone());
        Ok(Engine {
            lifecycle,
            credentials,
            host,
            profile,
            allotment,
            kernel_inval,
            op_watch: OpWatch::from_env("fuse-watch"),
            node_id,
            incarnation,
            next_rid_seq: Arc::new(AtomicU64::new(0)),
            fsmeta,
            backend_url,
            state_dir,
            meta,
            store,
            cache,
            compression,
            snapshots,
            snapshot_batches,
            snapsched,
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
            snapsched_stats,
            snapacct,
            last_sync_ms,
            read_only_member,
            fsync_s3,
            fsync_waits: Arc::new(crate::fsync_wait::FsyncWaits::new(
                fsync_timeout.unwrap_or_else(crate::fsync_wait::timeout_from_env),
            )),
            cto_strict,
            locks_cluster,
            lock_flushers,
            hold_sources,
            holds,
            pins,
            reintegration,
            stop,
            rt,
            started: Instant::now(),
            views: Mutex::new(HashMap::new()),
            next_view_id: AtomicU64::new(1),
            shutdown_started: AtomicBool::new(false),
            shutdown_error: Mutex::new(None),
        })
    }

    /// Open a view of this node for a frontend with `caps`, delivering
    /// its cache invalidations to `events` (plan 31 §4). Resolves the
    /// spec's root (a subtree, a frozen snapshot, or a writable clone of
    /// one, created here), applies its labels, QoS and `confine_links`,
    /// and registers the view with the node's lock flushes, open-orphan
    /// holds and — unless it is frozen — invalidation delivery. Close it
    /// with [`Self::close_view`].
    pub fn open_view(
        &self,
        spec: ViewSpec,
        caps: FrontendCaps,
        events: Arc<dyn FrontendEvents>,
    ) -> Result<Arc<View>> {
        self.open_view_with(spec, caps, events, None)
    }

    /// Reopen a view handed over from another process (plan 31 §6.11): a
    /// [`ViewHandoff`]'s spec (already resolved: no clone is made) with its
    /// handle table imported before the view serves anything. Everything
    /// else is [`Self::open_view`]'s.
    pub fn open_view_resumed(
        &self,
        spec: ViewSpec,
        handles: HandleTableSnapshot,
        caps: FrontendCaps,
        events: Arc<dyn FrontendEvents>,
    ) -> Result<Arc<View>> {
        if spec.rw_snapshot {
            bail!("a handed-over view spec is resolved: it names no snapshot to clone");
        }
        self.open_view_with(spec, caps, events, Some(handles))
    }

    /// What reopens `view` in the next process ([`Self::open_view_resumed`]):
    /// its resolved spec and its handle table. Take it once nothing
    /// reaches the view any more (its frontend detached).
    pub fn export_view(&self, view: &View) -> Result<ViewHandoff> {
        let spec = self
            .views
            .lock()
            .unwrap()
            .get(&view.id())
            .map(|v| v.resolved.clone())
            .with_context(|| format!("view {} is not open", view.id()))?;
        Ok(ViewHandoff {
            spec,
            handles: view.export_handles(),
        })
    }

    fn open_view_with(
        &self,
        spec: ViewSpec,
        caps: FrontendCaps,
        events: Arc<dyn FrontendEvents>,
        resumed: Option<HandleTableSnapshot>,
    ) -> Result<Arc<View>> {
        if self.shutdown_started.load(Ordering::SeqCst) {
            bail!("engine is shutting down; retry once it has exited");
        }
        let selector = spec
            .root
            .contains('@')
            .then(|| snapshot::split_selector(&spec.root))
            .transpose()?;
        if spec.rw_snapshot && selector.is_none() {
            bail!("--rw is only valid when mounting <path>@<snapshot>");
        }
        // A handed-over `ephemeral` spec names the temporary clone itself.
        let mut ephemeral_clone = (resumed.is_some() && spec.ephemeral && selector.is_none())
            .then(|| snapshot::normalize_path(&spec.root));
        let mounted_path = if let Some((source_path, snapshot_name)) = &selector {
            if spec.rw_snapshot {
                let destination = if spec.ephemeral {
                    format!(
                        "/.constellation-ephemeral-{}-{}",
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|duration| duration.as_millis())
                            .unwrap_or(0)
                    )
                } else {
                    let clone_name = spec
                        .clone_name
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
                if spec.ephemeral {
                    ephemeral_clone = Some(destination.clone());
                }
                destination
            } else {
                source_path.clone()
            }
        } else {
            snapshot::normalize_path(&spec.root)
        };
        let frozen = spec.is_frozen();
        let mut view = View::new(
            FsDependencies {
                inflight: self
                    .kernel_inval
                    .as_ref()
                    .map_or_else(kernel_inval::InFlight::disabled, |k| k.inflight()),
                meta: self.meta.clone(),
                store: self.store.clone(),
                cache: self.cache.clone(),
                rt: self.rt.clone(),
                sync: Some(SyncHandle {
                    tx: self.sync_tx.clone(),
                    fsync_s3: self.fsync_s3,
                    fsync: self.fsync_waits.clone(),
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
                    departed: Some(self.departed.clone()),
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
                snapsched_stats: self.snapsched_stats.clone(),
                holds: Some(self.holds.clone()),
                watch: self.op_watch.clone(),
                caps,
                host: self.host.clone(),
            },
            self.fsmeta.chunk_size,
            self.compression,
        );
        if let Some(handles) = &resumed {
            // Before the root: a snapshot root's synthetic key then finds
            // the number the kernel knows it by.
            view.import_handles(handles);
        }
        let rooted = match &selector {
            Some((path, name)) if !spec.rw_snapshot => view.set_snapshot_root(path, name),
            _ => view.set_subtree_root(&mounted_path),
        };
        if let Err(error) = rooted {
            self.remove_ephemeral(ephemeral_clone.as_deref());
            return Err(error);
        }
        view.apply_spec(&spec);
        // What this view is, resolved: reopening it (a handover) must not
        // clone again.
        let resolved = if spec.rw_snapshot || ephemeral_clone.is_some() {
            ViewSpec {
                root: mounted_path.clone(),
                rw_snapshot: false,
                clone_name: None,
                ephemeral: ephemeral_clone.is_some(),
                ..spec.clone()
            }
        } else {
            spec.clone()
        };
        let id = self.next_view_id.fetch_add(1, Ordering::Relaxed);
        view.set_id(id);
        let view = Arc::new(view);
        view.bind();
        tracing::info!(
            view = id,
            root = %spec.root,
            labels = ?spec.labels,
            confine_links = spec.confine_links,
            qos = ?spec.qos,
            "view opened"
        );
        // Plan 30 §M14: a recalled lock grant's flush reaches this view's
        // write state; DESIGN.md §3: its open handles hold orphans.
        self.lock_flushers.register(
            id,
            Arc::downgrade(&(view.clone() as Arc<dyn crate::locks::LockFlush>)),
        );
        self.hold_sources.register(
            id,
            Arc::downgrade(&(view.clone() as Arc<dyn crate::holds::OpenHandles>)),
        );
        if let (Some(k), false) = (&self.kernel_inval, frozen) {
            k.register(id, events, view.view_root());
        }
        self.lifecycle.register_view(id, Arc::downgrade(&view));
        self.views.lock().unwrap().insert(
            id,
            OpenView {
                resolved,
                info: ViewInfo {
                    id,
                    root: spec.root.clone(),
                    labels: spec.labels.clone(),
                    since: Instant::now(),
                },
                quota_cache: view.quota_cache_handle(),
                subtree_quota_cache: view.subtree_quota_cache_handle(),
                ephemeral_clone,
            },
        );
        Ok(view)
    }

    /// The frontend serving `view` is gone: unregister it, remove its
    /// ephemeral clone, and let the hold writer publish that its handles
    /// closed. Returns whether it was the last open view. Idempotent.
    pub fn close_view(&self, view: &View) -> bool {
        let id = view.id();
        // No `release` will arrive for whatever this frontend had open,
        // so the passthrough handles' pins go now rather than whenever
        // the last `Arc<View>` happens to die (plan 38 §3(c)).
        view.drop_all_passthrough();
        if let Some(k) = &self.kernel_inval {
            k.unregister(id);
        }
        self.lock_flushers.unregister(id);
        self.hold_sources.unregister(id);
        self.lifecycle.unregister_view(id);
        self.holds.nudge();
        let (entry, now_empty) = {
            let mut views = self.views.lock().unwrap();
            let entry = views.remove(&id);
            (entry, views.is_empty())
        };
        if let Some(entry) = entry {
            self.remove_ephemeral(entry.ephemeral_clone.as_deref());
        }
        now_empty
    }

    /// A view whose frontend was handed over to another process (plan 31
    /// §6.11): stop delivering to it and forget it, but leave its
    /// ephemeral clone (the next process serves it) and its open-orphan
    /// claims (the next process's hold writer takes them over; a
    /// withdrawal now would let another node reap an orphan an
    /// application still has open).
    pub fn close_view_for_handover(&self, view: &View) {
        let id = view.id();
        // Not `drop_all_passthrough`, unlike `close_view`: the kernel goes
        // on serving the handed-over passthrough handles from the backing
        // files this process registered, so their chunks stay pinned here
        // until the process `exec`s (the view lives that long), and the
        // resumed view re-pins them from the snapshot (plan 38 Z3b).
        if let Some(k) = &self.kernel_inval {
            k.unregister(id);
        }
        self.lock_flushers.unregister(id);
        self.lifecycle.unregister_view(id);
        self.views.lock().unwrap().remove(&id);
    }

    fn remove_ephemeral(&self, path: Option<&str>) {
        if let Some(path) = path {
            if let Err(e) = remove_live_subtree(&self.meta, path) {
                tracing::warn!(error = %e, path, "removing ephemeral clone failed");
            }
        }
    }

    /// The open views.
    pub fn views(&self) -> Vec<ViewInfo> {
        let mut views: Vec<ViewInfo> = self
            .views
            .lock()
            .unwrap()
            .values()
            .map(|v| v.info.clone())
            .collect();
        views.sort_by_key(|v| v.id);
        views
    }

    /// Invalidate every open view's cached quota cap after a live
    /// `SetQuota`. Quota is node-level, but each view keeps its own
    /// short-TTL read of it (`QUOTA_CACHE_TTL`), which a single-view
    /// invalidation would leave stale in the others.
    pub fn invalidate_quota_caches(&self) {
        for view in self.views.lock().unwrap().values() {
            View::invalidate_quota_cache(&view.quota_cache);
            View::invalidate_quota_cache(&view.subtree_quota_cache);
        }
    }

    /// Whether [`Self::shutdown`] has begun (new views are refused).
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_started.load(Ordering::SeqCst)
    }

    /// Clean shutdown: drain the uploads and the shipper, publish,
    /// release the lease, withdraw the open-orphan claim. Idempotent —
    /// called once, when the last view closes or on a signal; later calls
    /// are a harmless no-op (and [`Self::is_shutting_down`] refuses new
    /// views from the first). The host removes its own files (a daemon's
    /// `control.sock`/`daemon.pid`) after it.
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
        self.shutdown_inner(true)
    }

    /// [`Self::shutdown`] before a session handover (plan 31 §6.11): the
    /// same drain, publish and lease release — the next process restarts
    /// on this state dir exactly as a remount would — except that the
    /// open-orphan claim is left in place for the next process's hold
    /// writer (applications still hold those files open, across the
    /// handover). The replica is synced to disk last: the process may
    /// `exec` rather than exit, and nothing is dropped then.
    pub fn shutdown_for_handover(&self) -> Result<()> {
        let result = self.shutdown_inner(false);
        if let Err(error) = self.meta.sync() {
            tracing::warn!(%error, "syncing the replica before the handover failed");
        }
        result
    }

    fn shutdown_inner(&self, withdraw_holds: bool) -> Result<()> {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let result = match self.drain_for_shutdown(withdraw_holds) {
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

    fn drain_for_shutdown(&self, withdraw_holds: bool) -> Result<()> {
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
            if let Err(e) = crate::upload::upload_dirty_chunks(
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
                .send(crate::sync::SyncRequest::Shutdown { reply })
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
        // No view is left to hold an orphan open: withdraw the claim now
        // rather than let its TTL run out (unless a handover carries the
        // views on).
        if withdraw_holds {
            self.rt.block_on(self.holds.withdraw());
        }
        // Hand the scheduler's lease back rather than let a successor wait
        // out its TTL (a no-op, and no request, on a node not leading).
        self.rt.block_on(self.snapsched.resign());
        tracing::info!("clean unmount drain complete");
        Ok(())
    }

    pub fn host(&self) -> &HostServices {
        &self.host
    }
    pub fn profile(&self) -> &crate::EngineProfile {
        &self.profile
    }
    /// The budget this engine runs within (an [`crate::EngineHost`]'s
    /// share; unlimited for a standalone engine).
    pub fn allotment(&self) -> &crate::ResourceBudget {
        &self.allotment
    }
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn fsmeta(&self) -> &FsMeta {
        &self.fsmeta
    }
    pub fn backend_url(&self) -> &str {
        &self.backend_url
    }
    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }
    pub fn meta(&self) -> &Arc<Meta> {
        &self.meta
    }
    pub fn store(&self) -> &Arc<ChunkStore> {
        &self.store
    }
    pub fn cache(&self) -> &Arc<DiskCache> {
        &self.cache
    }
    pub fn compression(&self) -> CompressionSetting {
        self.compression
    }
    pub fn snapshots(&self) -> &Arc<snapshot::SnapshotManager> {
        &self.snapshots
    }
    /// Plan 32 Step 0.1: snapshot creates, deletes and holds, executed at
    /// the root-lease holder ([`crate::snapshot_batch`]). The scheduler
    /// calls [`crate::snapshot_batch::SnapshotBatcher::submit`] directly,
    /// with one rid per batch ([`crate::snapshot_batch::SnapshotBatcher::next_rid`])
    /// kept across that batch's retries.
    pub fn snapshot_batches(&self) -> &Arc<crate::snapshot_batch::SnapshotBatcher> {
        &self.snapshot_batches
    }
    pub fn staging_budget(&self) -> &Arc<staging::StagingBudget> {
        &self.staging_budget
    }
    pub fn lease_mode(&self) -> constellation_store_s3::LeaseMode {
        self.lease_mode
    }
    pub fn lease(&self) -> &Arc<lease::LeaseView> {
        &self.lease
    }
    pub fn core_status(&self) -> &Arc<Mutex<crate::authority_driver::CoreStatus>> {
        &self.core_status
    }
    pub fn write_mode(&self) -> &Arc<writeback::WriteModeState> {
        &self.write_mode
    }
    pub fn sync_tx(&self) -> &tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest> {
        &self.sync_tx
    }
    pub fn peers(&self) -> &constellation_net::Peers {
        &self.peers
    }
    pub fn epochs(&self) -> &Arc<epoch::EpochManager> {
        &self.epochs
    }
    pub fn designations(&self) -> &Arc<designation::DesignationManager> {
        &self.designations
    }
    pub fn coop(&self) -> &Arc<coop::Coop> {
        &self.coop
    }
    pub fn upload(&self) -> &Arc<crate::upload::UploadRuntime> {
        &self.upload
    }
    pub fn forward(&self) -> &Arc<forward::ForwardState> {
        &self.forward
    }
    pub fn placement(&self) -> &Arc<placement::Placement> {
        &self.placement
    }
    pub fn departed(&self) -> &Arc<AtomicBool> {
        &self.departed
    }
    pub fn atime(&self) -> &Arc<crate::atime::AtimeAccumulator> {
        &self.atime
    }
    pub fn prune_stats(&self) -> &Arc<crate::prune::PruneStats> {
        &self.prune_stats
    }
    /// Plan 32's snapshot scheduler: `snapshot.sched.status` and
    /// `snapshot.sched.run`.
    pub fn snapsched(&self) -> &Arc<crate::snapsched::Scheduler> {
        &self.snapsched
    }
    pub fn snapsched_stats(&self) -> &Arc<crate::snapsched::SnapSchedStats> {
        &self.snapsched_stats
    }
    /// Plan 32 §6.3: the space-accounting index (`32-m5c`'s surfaces
    /// query it; the host reports its web UI through it).
    pub fn snapacct(&self) -> &Arc<crate::snapacct::SnapAcctService> {
        &self.snapacct
    }
    pub fn last_sync_ms(&self) -> &Arc<AtomicU64> {
        &self.last_sync_ms
    }
    /// Plan 39: the node's `fsync` policy and waits (`node.status.fsync`).
    pub fn fsync_waits(&self) -> &Arc<crate::fsync_wait::FsyncWaits> {
        &self.fsync_waits
    }

    pub fn read_only_member(&self) -> bool {
        self.read_only_member
    }
    pub fn pins(&self) -> &Arc<pin::PinManager> {
        &self.pins
    }
    pub fn reintegration(&self) -> &Arc<reintegrate::ReintegrationState> {
        &self.reintegration
    }
    pub fn runtime(&self) -> &tokio::runtime::Handle {
        &self.rt
    }
    pub fn started(&self) -> Instant {
        self.started
    }
    /// Plan 30 §M8: `--cto strict`.
    pub fn cto_strict(&self) -> bool {
        self.cto_strict
    }
    /// Plan 30 §M14: whether views forward file locks cluster-wide.
    pub fn locks_cluster(&self) -> bool {
        self.locks_cluster
    }
    /// The request watchdog every view's ops register with.
    pub fn op_watch(&self) -> &OpWatch {
        &self.op_watch
    }

    /// The engine's credential source (plan 31 §9.8).
    pub fn credentials(&self) -> &Arc<CredentialSource> {
        &self.credentials
    }

    /// Plan 31 C8: the host's lifecycle, as this engine applies it.
    pub fn lifecycle(&self) -> &Arc<crate::lifecycle::Lifecycle> {
        &self.lifecycle
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
    sync_tx: &tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
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
            .send(crate::sync::SyncRequest::Submit {
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
    sync_tx: &tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
) {
    match ChunkStore::new(store.clone()).load_fs().await {
        Ok(meta) => {
            let _ = sync_tx.send(crate::sync::SyncRequest::Slack(meta.epoch_slack()));
        }
        Err(e) => tracing::debug!(error = %e, "re-reading meta.json for epoch_slack failed"),
    }
}

/// Keep the churn keyspaces' tombstones bounded (`Meta::vacuum_churn`):
/// checked every 10 s, on a thread of its own (a compaction blocks), for
/// as long as the replica is open.
fn spawn_vacuum(meta: &Arc<Meta>, background: &Arc<crate::lifecycle::BackgroundGate>) {
    let weak = Arc::downgrade(meta);
    let background = background.clone();
    let spawned = std::thread::Builder::new()
        .name("meta-vacuum".into())
        .spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(10));
            let Some(meta) = weak.upgrade() else {
                return;
            };
            if background.is_paused() {
                continue;
            }
            match meta.vacuum_churn() {
                Ok(done) if !done.is_empty() => {
                    tracing::debug!(keyspaces = ?done, "vacuumed churn keyspaces")
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "vacuuming churn keyspaces failed"),
            }
        });
    if let Err(error) = spawned {
        tracing::warn!(%error, "could not start the metadata vacuum thread");
    }
}

/// Give the root directory to the mounting user on a freshly created
/// filesystem. Skipped entirely unless the root is still 0:0, so this
/// costs nothing (and needs no lease) on every subsequent mount; when it
/// does apply, it takes the lease like any other mutation (through the
/// authority core).
///
/// EC2 finding R2-4: one attempt is not enough. A `Policy::System` op
/// gives up the moment its acquisition does not open the view, and a
/// fresh acquisition routinely waits a moment behind its takeover gate —
/// under `--cto strict`, when two nodes mount together, the kernel-cache
/// drain the first sight of the other node arms (plan 30 §M8) keeps the
/// gate shut for a second. Both nodes then answered "deferred" (the
/// holder's own attempt refused by its gate, the other's forward refused
/// by the holder's), nobody retried, and the root stayed genesis' `root:
/// root 0755` — unwritable by the mounting user and, without
/// `allow_other`, by root too. So: retry until the root has an owner
/// (this node's attempt or anyone else's, seen by tailing), for a short
/// while before the mount appears and then in the background.
async fn adopt_root(
    host: &HostServices,
    meta: &std::sync::Arc<Meta>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
    forward: &std::sync::Arc<crate::forward::ForwardState>,
    node_id: u64,
) -> Result<()> {
    let (euid, _) = host.process.effective_ids();
    if euid == 0 {
        return Ok(());
    }
    /// How long the mount waits for the root to have an owner before it
    /// appears anyway (and keeps adopting in the background).
    const BEFORE_MOUNT: std::time::Duration = std::time::Duration::from_secs(10);
    const RETRY: std::time::Duration = std::time::Duration::from_millis(250);
    let started = std::time::Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        if adopt_root_once(host, meta, sync_tx, forward, node_id).await? {
            if attempts > 1 {
                tracing::info!(attempts, "root directory owner adopted");
            }
            return Ok(());
        }
        if started.elapsed() >= BEFORE_MOUNT {
            break;
        }
        tokio::time::sleep(RETRY).await;
    }
    tracing::warn!(
        attempts,
        "root directory still has no owner; mounting anyway and adopting it in the background"
    );
    let (host, meta, sync_tx, forward) =
        (host.clone(), meta.clone(), sync_tx.clone(), forward.clone());
    tokio::spawn(async move {
        let mut wait = std::time::Duration::from_secs(1);
        loop {
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(std::time::Duration::from_secs(60));
            match adopt_root_once(&host, &meta, &sync_tx, &forward, node_id).await {
                Ok(true) => {
                    tracing::info!("root directory owner adopted");
                    return;
                }
                Ok(false) => {}
                // The sync task is gone: the mount is shutting down.
                Err(_) => return,
            }
        }
    });
    Ok(())
}

/// One adoption attempt: `Ok(true)` once the root has an owner (it
/// already had one, or this attempt gave it one), `Ok(false)` when the
/// attempt was deferred (the lease is held elsewhere or not open yet).
async fn adopt_root_once(
    host: &HostServices,
    meta: &std::sync::Arc<Meta>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
    forward: &crate::forward::ForwardState,
    node_id: u64,
) -> Result<bool> {
    let (euid, egid) = host.process.effective_ids();
    // Another node may already have done it; make sure we have its log.
    // A tail that fails (S3 unreachable) is not fatal: the replica we
    // have decides, and a later attempt tails again.
    let (reply, rx) = tokio::sync::oneshot::channel();
    sync_tx
        .send(crate::sync::SyncRequest::TailToHead { reply })
        .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
    match rx.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "root adoption: tail failed"),
        Err(_) => anyhow::bail!("sync task stopped"),
    }
    let root =
        constellation_meta::MetaStore::getattr(&**meta, constellation_fs_core::types::ROOT_INO)?;
    tracing::debug!(
        uid = root.as_ref().map(|a| a.uid),
        gid = root.as_ref().map(|a| a.gid),
        applied = meta.applied_seq().unwrap_or(0),
        "root adoption check"
    );
    if !matches!(root, Some(a) if a.uid == 0) {
        return Ok(true);
    }
    let op = constellation_meta::MutateOp::Setattr {
        ino: constellation_fs_core::types::ROOT_INO,
        mode: None,
        uid: Some(euid),
        gid: Some(egid),
        size: None,
        atime_ns: None,
        mtime_ns: None,
    };
    let (reply, rx) = tokio::sync::oneshot::channel();
    sync_tx
        .send(crate::sync::SyncRequest::Submit {
            op,
            rid: forward.next_system_rid(node_id),
            policy: constellation_authority::Policy::System,
            in_doubt: false,
            reply,
        })
        .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
    match rx.await {
        Ok(constellation_authority::ClientReply::Outcome(
            constellation_meta::MutateOutcome::Accepted { .. },
        )) => Ok(true),
        Err(_) => anyhow::bail!("sync task stopped"),
        // Another node holds authority (it adopts the root itself, or a
        // later attempt here reaches it), or this node's own fresh
        // acquisition has not opened its view yet (a takeover gate).
        other => {
            tracing::info!(reply = ?other, "root adoption deferred; retrying");
            Ok(false)
        }
    }
}

/// Remove the live subtree at `path` (an ephemeral clone).
fn remove_live_subtree(meta: &Meta, path: &str) -> Result<()> {
    let ino = meta
        .resolve_path(path)?
        .with_context(|| format!("clone path {path} disappeared"))?;
    fn clear(meta: &Meta, ino: u64) -> Result<()> {
        for entry in constellation_meta::MetaStore::readdir(meta, ino)? {
            if entry.kind == constellation_fs_core::InodeKind::Dir {
                clear(meta, entry.ino)?;
                constellation_meta::MetaStore::rmdir(meta, ino, &entry.name)?;
            } else {
                constellation_meta::MetaStore::unlink(meta, ino, &entry.name)?;
            }
        }
        Ok(())
    }
    clear(meta, ino)?;
    let parent = meta
        .parent_of(ino)?
        .context("ephemeral clone cannot be the filesystem root")?;
    let name = path
        .rsplit('/')
        .find(|part| !part.is_empty())
        .context("ephemeral clone has no basename")?;
    constellation_meta::MetaStore::rmdir(meta, parent, name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EngineHost, EngineProfile, FsId, P2pMode, ResourceBudget, ViewQos};
    use constellation_fs_core::types::ROOT_INO;
    use constellation_vfs::{Blocking, Caller, Name, OpCtx, OpKind, OpenFlags, OpenOwner, Vfs};

    fn create_fs(rt: &tokio::runtime::Runtime, backend: &str) {
        let store = ChunkStore::new(
            rt.block_on(crate::backend::open_backend(backend))
                .expect("open backend"),
        );
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .expect("create_fs");
    }

    fn config(rt: &tokio::runtime::Runtime, backend: &str, state: PathBuf) -> EngineConfig {
        EngineConfig {
            state_dir: Some(state),
            cache_size: 64 * 1024 * 1024,
            runtime: Some(rt.handle().clone()),
            ..EngineConfig::new(backend)
        }
    }

    fn offline() -> EngineProfile {
        EngineProfile {
            p2p: P2pMode::Off,
            ..EngineProfile::desktop()
        }
    }

    /// `Engine::start` on a local backend, two views (the root and a
    /// confined subtree) through `open_view`, ops through `Vfs`, then the
    /// views closed and the engine shut down cleanly.
    #[test]
    fn an_engine_starts_opens_views_and_shuts_down() {
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        create_fs(&rt, &backend);
        let engine = Engine::start(
            config(&rt, &backend, root.path().join("state")),
            HostServices::native(),
            offline(),
        )
        .expect("Engine::start");
        assert!(!engine.peers().is_enabled(), "the profile turned P2P off");
        assert_eq!(engine.allotment(), &ResourceBudget::unlimited());
        let caps = FrontendCaps::linux_fuse(engine.locks_cluster());
        let whole = engine
            .open_view(ViewSpec::new("/"), caps.clone(), DeferredEvents::new())
            .unwrap();
        let caller = Caller::root();
        let cx = |kind| OpCtx::new(kind, &caller);
        let sub = Blocking::run(|r| {
            whole.mkdir(&cx(OpKind::Mkdir), ROOT_INO, Name::new("sub"), 0o755, r)
        })
        .unwrap();
        let (file, _opened) = Blocking::run(|r| {
            whole.create(
                &cx(OpKind::Create),
                sub.attr.ino,
                Name::new("f"),
                0o644,
                OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        let mut spec = ViewSpec::new("/sub");
        spec.confine_links = true;
        spec.labels.insert("pv".into(), "pv-1".into());
        spec.qos = ViewQos {
            max_inflight_ops: Some(8),
            max_staging_bytes: None,
        };
        let confined = engine.open_view(spec, caps, DeferredEvents::new()).unwrap();
        assert_ne!(whole.id(), confined.id());
        assert_eq!(
            confined.labels().get("pv").map(String::as_str),
            Some("pv-1")
        );
        let seen =
            Blocking::run(|r| confined.lookup(&cx(OpKind::Lookup), ROOT_INO, Name::new("f"), r))
                .unwrap();
        assert_eq!(seen.attr.ino, file.attr.ino);
        // The root's parent is not reachable through the subtree view.
        let up =
            Blocking::run(|r| confined.lookup(&cx(OpKind::Lookup), ROOT_INO, Name::new(".."), r))
                .unwrap();
        assert_eq!(up.attr.ino, ROOT_INO);
        let listed: Vec<_> = engine.views().into_iter().map(|v| v.root).collect();
        assert_eq!(listed, ["/", "/sub"]);
        assert!(engine
            .open_view(
                ViewSpec::new("/missing"),
                FrontendCaps::linux_fuse(false),
                DeferredEvents::new()
            )
            .is_err());
        assert!(!engine.close_view(&confined), "the root view is still open");
        assert!(engine.close_view(&whole), "the last view closed");
        engine.shutdown().expect("a clean shutdown");
        assert!(engine.is_shutting_down());
        assert!(
            engine
                .open_view(
                    ViewSpec::new("/"),
                    FrontendCaps::linux_fuse(false),
                    DeferredEvents::new()
                )
                .is_err(),
            "no views once shutting down"
        );
        assert_eq!(engine.shutdown_error(), None);
    }

    /// An `EngineHost` starts its engines on its runtime within their
    /// share: the first one's cache is capped by the host's budget.
    #[test]
    fn an_engine_host_starts_engines_within_their_share() {
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        create_fs(&rt, &backend);
        let host = EngineHost::start(
            rt.handle().clone(),
            ResourceBudget {
                memory_bytes: 1 << 30,
                cache_bytes: 16 * 1024 * 1024,
                staging_bytes: 1024 * 1024,
            },
        );
        let mut cfg = config(&rt, &backend, root.path().join("state"));
        cfg.runtime = None;
        let id = FsId::new("myfs");
        let engine = host
            .add_engine(id.clone(), cfg, HostServices::native(), offline())
            .expect("add_engine");
        assert_eq!(
            engine.cache().usage().budget,
            16 * 1024 * 1024,
            "capped by the share"
        );
        assert_eq!(engine.staging_budget().budget(), 1024 * 1024);
        assert!(host.engine(&id).is_some());
        let again = config(&rt, &backend, root.path().join("state2"));
        assert!(
            host.add_engine(id.clone(), again, HostServices::native(), offline())
                .is_err(),
            "one engine per id"
        );
        // The next engine would get half.
        assert_eq!(host.allotment_for(&offline()).cache_bytes, 8 * 1024 * 1024);
        let removed = host.remove_engine(&id).unwrap();
        removed.shutdown().unwrap();
        assert!(host.engines().is_empty());
    }
}
