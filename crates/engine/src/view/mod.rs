//! `View`: one mounted view of a filesystem — the engine's side of the
//! [`constellation_vfs::Vfs`] contract (plan 31 §4, §6). `ConstellationFs`
//! before plan 31 C3; the FUSE adapter's `fusefs.rs` before C4, when the
//! adapter became `constellation-frontend-fuse` and every inline policy
//! step it took before a backend call (the root renumbering of a subtree
//! or snapshot view, the kernel-invalidation hold-back registration, name
//! and xattr policy, the synthetic `.constellation` tree, scratch
//! directories, `cto=strict`'s read waits, the pending-write size
//! overlay, lock fencing, `O_SYNC` publication, the cluster-lock
//! capability check, the virtual xattrs) moved beneath the trait, here.
//!
//! Phase 5b seals a sequential writer's crossed chunks using a
//! contiguous high-water mark. Sealing uploads immutable content early
//! but never publishes a partial manifest, so close-to-open semantics
//! are unchanged. Write-through pays chunk RTTs before close returns;
//! write-back returns after local journaling and shares the continuation
//! epoch's durable pending queue and ship-time barrier.
//!
//! Write-back is disk-bound for small-file imports and coalesces edits
//! made before drain, at the cost of delayed cross-node visibility,
//! non-evictable dirty cache pressure, and loss exposure if the writer
//! node is permanently destroyed before drain. P2P announcements remain
//! post-S3 and peer serving rejects Dirty chunks, so neither path can
//! expose a manifest whose content is absent from S3.
//!
//! The modules: [`ops`] (the `Vfs` impl: each op's checks in their
//! order, then the backend call), `write_gate` (the lease, the mutation
//! paths, durability acknowledgements, the read waits), `shards` (write
//! sessions and per-inode ordering), `io` (read/write/truncate/fallocate/
//! seek on a session), `flush` (chunk fetch, sealing, the publishing
//! flush), `create` (`create(2)` on a cluster), `lock_gate` (§M14's
//! fence), `synthetic` (the `.constellation` tree), `confine` (subtree
//! confinement), `admission` (per-view QoS), `spec` ([`ViewSpec`]).
//!
//! # Confinement (plan 31 §6.12)
//!
//! A view is rooted at a subtree (or a snapshot), and nothing above that
//! root is reachable through it — a guarantee of the view, beneath
//! `Vfs`, not a convention of its frontends:
//!
//! - **`..` at the view's root is the root.** `lookup(root, "..")` answers
//!   the root itself, as a real filesystem's root does; `..` of any other
//!   directory is its parent, which the root dominates. (A kernel frontend
//!   resolves `..` itself and never crosses a mount's root; this is for
//!   frontends that ask, NFS's `LOOKUPP` among them.)
//! - **No inode outside the subtree is reachable by handle.** Every inode
//!   a frontend names (a parent, a target, a file handle's inode) must be
//!   dominated by the view's root — one of its names lies beneath it —
//!   or the op answers `Code::Stale` (`ESTALE`: exactly what a handle to
//!   something the server no longer serves is). A whole-filesystem view
//!   dominates everything and checks nothing. A confined view answers
//!   from a per-view cache of the inodes it handed out (every entry of a
//!   lookup or a create) or already proved inside (`confine::Reach`), and
//!   walks the replica's parent chain only on a miss — so a stale,
//!   replayed or forged number costs a few local reads once, and the
//!   steady state costs one shard lock and a hash probe, no metadata
//!   read. An inode open through this view stays addressable (an
//!   unlinked-open file has no name left to walk). A snapshot view hands
//!   out synthetic inodes only, so any live inode number is refused.
//!   What the cache does not undo: an inode this view resolved and that
//!   is then renamed out of the subtree (by another view or node) stays
//!   addressable by handle, as an open descriptor across a bind mount's
//!   boundary does on Linux; lookup by name never reaches it again.
//! - **`.constellation` stays inside.** The synthetic tree under a
//!   directory lists the snapshots covering *that directory* (taken of
//!   it or of an ancestor, matched by inode or by path:
//!   `SnapshotManager::covering`) and mirrors each at the same relative
//!   path — so a view rooted at `/volumes/pv-1` sees pv-1's
//!   history only, never a sibling volume's, and never the filesystem
//!   root's content.
//!
//! # Link domains (`ViewSpec::confine_links`)
//!
//! Without `confine_links`, `link()` is POSIX (within the view, which the
//! checks above already bound). With it, every directory of the view
//! belongs to a *link domain*: its nearest ancestor, itself included,
//! that is the view's root or carries the root-only marker
//! [`confine::LINK_DOMAIN_XATTR`] (`trusted.constellation.link_domain`).
//! Then:
//!
//! - `link(ino, new_parent, name)` succeeds only if at least one existing
//!   name of `ino` is in `new_parent`'s domain; otherwise `EXDEV` (what
//!   `ln` and `cp -l` already handle). The check reads the replica, not
//!   the view's cache: an inode reached through a stale handle, or renamed
//!   out since, has no name inside and is refused.
//! - `rename()` of a non-directory that has other names (`nlink > 1`)
//!   into a different domain is `EXDEV` too (`mv` falls back to copy and
//!   unlink), since it would leave one inode named in two domains.
//!
//! So a view rooted at `/volumes/pv-1` refuses to link in anything whose
//! names all lie outside pv-1; and a maintenance view at `/` over a pool
//! whose `/volumes/<pv>` directories carry the marker refuses any link
//! from one volume into another while allowing links within one (and
//! anywhere outside the marked volumes, which form the root's domain).
//! Confinement governs the `link()`/`rename()` calls made through the
//! view; it does not undo links made before it was set, or through a view
//! without it. The check and the link are two steps: a rename elsewhere
//! racing them is not excluded (the replica's order decides).

mod admission;
mod confine;
mod create;
mod flush;
mod handoff;
mod io;
mod lock_gate;
pub mod ops;
mod passthrough;
mod shards;
mod spec;
mod synthetic;
mod write_gate;

pub use confine::LINK_DOMAIN_XATTR;
pub use handoff::{HandleTableSnapshot, ViewHandoff};
pub use passthrough::PassthroughStatus;
pub use spec::{metric_view_label, ViewQos, ViewSpec, METRIC_LABELS};

#[cfg(test)]
mod confine_tests;
#[cfg(test)]
mod durable_ack_tests;
#[cfg(test)]
mod memcache_tests;
#[cfg(test)]
mod passthrough_tests;
#[cfg(test)]
mod pending_row_tests;
#[cfg(test)]
mod qos_tests;
#[cfg(test)]
mod quota_tests;
#[cfg(test)]
mod staging_code_tests;
mod subtree_quota;
#[cfg(test)]
mod vfs_tests;

use passthrough::PassthroughHandle;
use shards::*;
pub(crate) use subtree_quota::is_internal_xattr;
pub use subtree_quota::SUBTREE_QUOTA_XATTR;
use subtree_quota::{statfs_blocks_capped, SubtreeUsage};
use synthetic::*;
use write_gate::*;

pub(crate) use synthetic::SyntheticNode;

use crate::staging::{GenCounter, Staging, StagingBudget};
use crate::sync::SyncRequest;
use anyhow::{Context, Result};
use bytes::Bytes;
use constellation_fs_core::cache::{CacheVerify, ChunkState, DiskCache, OpenPin};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest, SparseChunks};
use constellation_fs_core::{ChunkHash, FileAttr, Ino, InodeKind, INLINE_CHUNKS_MAX};
use constellation_meta::{Meta, MetaError, MetaStore, ReadKey};
use constellation_store_s3::{ChunkStore, CompressionSetting, DecodePriority};
use constellation_types::Code;
use constellation_vfs::{
    Attr, Caller, Durability, Entry, FallocateMode, FileKind, FrontendCaps, OpWatch, OpenFlags,
    PassthroughChunk, PolicyStack, ReadData, SeekWhence,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

/// The attribute and entry TTL a frontend may cache replies for (1 s;
/// none under `--cto strict`, [`View::ttl`]).
pub const TTL: Duration = Duration::from_secs(1);
const QUOTA_CACHE_TTL: Duration = Duration::from_secs(5);
const DEFAULT_STATFS_TTL_S: u64 = 5;

fn parse_statfs_ttl_secs(raw: Option<&str>) -> Duration {
    let secs = raw
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_STATFS_TTL_S);
    Duration::from_secs(secs)
}

fn statfs_ttl_from_env() -> Duration {
    parse_statfs_ttl_secs(std::env::var("CONSTELLATION_STATFS_TTL_S").ok().as_deref())
}

/// `statfs` block arithmetic, as `(total, free)`.
///
/// The two columns answer different questions and so read different
/// numbers. Used (which the kernel derives as `total - free`) is logical
/// bytes under the *mounted view*, so a subtree mount reports its own
/// subtree. Free is what a writer can actually still consume: headroom
/// under the cluster-wide cap, a whole-filesystem property no matter how
/// narrow the view is. Total is their sum, which keeps both truthful and
/// collapses to the cap for a whole-filesystem mount.
fn statfs_blocks(view_used: u64, fs_used: u64, cap: Option<u64>, block: u64) -> (u64, u64) {
    let used_blocks = view_used.div_ceil(block);
    // Round free down and used up: never promise a block that is not there.
    let free_blocks = match cap {
        Some(cap) => cap.saturating_sub(fs_used) / block,
        None => u64::MAX / block / 2,
    };
    (used_blocks.saturating_add(free_blocks), free_blocks)
}

/// Cached effective quota shared between the filesystem and the control
/// plane: `(fetched_at, cap)`, where `cap: None` is unlimited and the
/// outer `None` means "not populated" (a live `SetQuota` clears it).
pub type QuotaCache = Arc<Mutex<Option<(Instant, Option<u64>)>>>;

/// The I/O size a view reports: each file's `blksize`, and `statfs`'s
/// block.
pub(crate) const BLOCK_SIZE: u32 = 131072;

/// An inode kind as the contract names it.
pub(crate) fn kind_out(kind: InodeKind) -> FileKind {
    match kind {
        InodeKind::File => FileKind::File,
        InodeKind::Dir => FileKind::Dir,
        InodeKind::Symlink => FileKind::Symlink,
        InodeKind::Fifo => FileKind::Fifo,
        InodeKind::Socket => FileKind::Socket,
        InodeKind::BlockDev => FileKind::BlockDev,
        InodeKind::CharDev => FileKind::CharDev,
    }
}

/// A view's handle to the metadata sync task.
pub struct SyncHandle {
    pub tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    /// `--fsync-mode s3`: fsync() returns only once the journal is in S3.
    pub fsync_s3: bool,
    /// Plan 30 §M8: `--cto strict` (see `crate::cto`).
    pub cto_strict: bool,
    /// Plan 30 §M14: `--locks cluster` (see `crate::locks`); `None` is
    /// `--locks local` (the kernel keeps locks node-local).
    pub locks: Option<Arc<crate::locks::ClusterLocks>>,
    /// Lock-free lease view (the core's state, mirrored by the driver);
    /// the write gate reads it per mutating op.
    pub lease: Arc<crate::lease::LeaseView>,
    /// Plan 30 §M11: the delegations this node holds (the fast path's
    /// sibling check).
    pub delegates: Arc<crate::lease::DelegateView>,
    /// Bound on how long a mutation waits for a foreign holder.
    pub acquire_deadline: Duration,
    /// Continuation epoch (DESIGN.md §5.3): frozen ⇒ EROFS; active ⇒
    /// epoch is the authority root (writes without S3 CAS).
    pub epoch_frozen: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub epoch_active: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Set after a successful self-leave, or when our registry record is
    /// retired/vanished under us: mutations fail with EIO.
    pub departed: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// A read-only registry member never enters the write gate.
    pub read_only_member: bool,
    pub write_mode: Arc<crate::writeback::WriteModeState>,
    /// Plan 30 §M2: this mount's node id and incarnation, and the
    /// shared per-incarnation seq counter — everything
    /// `mutate_op_rebasable` needs to allocate this op's rid at the top,
    /// before any forward or lease-acquisition attempt.
    pub node_id: u64,
    pub incarnation: u32,
    pub next_rid_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Plan 30 §M2 GC: rid seqs whose op completed here (every path of
    /// `mutate_op_rebasable`, the fast path included), drained by the
    /// driver into the core's ack tracker so `acked_through` has no gaps
    /// for the holder to stall behind.
    pub acked: Arc<std::sync::Mutex<Vec<u64>>>,
}

/// Everything [`View::new`] needs besides the two filesystem
/// format knobs (`chunk_size`, `compression`). Grouped so a new
/// dependency cannot be silently swapped with a neighbour of the same type.
pub struct FsDependencies {
    pub meta: Arc<Meta>,
    pub store: Arc<ChunkStore>,
    pub cache: Arc<DiskCache>,
    pub rt: Handle,
    pub sync: Option<SyncHandle>,
    pub coop: Option<Arc<crate::coop::Coop>>,
    /// Root of `<state_dir>/staging`; write staging files live here
    /// (plan 07). Callers GC this directory before mounting.
    pub staging_dir: PathBuf,
    /// Shared bound on in-flight (unflushed) write bytes across every
    /// open dirty inode, decoupled from the chunk cache budget: a
    /// staged write is not sealed into the cache until flush.
    pub staging_budget: Arc<StagingBudget>,
    pub snapshots: Arc<crate::snapshot::SnapshotManager>,
    /// Node-level read-time atime accumulator (plan 20). Shared across
    /// all views and drained by the sync task's flush ticker. `Off` by
    /// default, in which case the read hook short-circuits.
    pub atime: Arc<crate::atime::AtimeAccumulator>,
    /// Node-level prune counters (plan 22), shared with the pruner task
    /// and the control plane; the setxattr gate records the last policy
    /// parse rejection here.
    pub prune_stats: Arc<crate::prune::PruneStats>,
    /// Node-level snapshot-schedule counters (plan 32), shared with the
    /// scheduler and the control plane; the setxattr gate records the
    /// last snapshot-policy parse rejection here.
    pub snapsched_stats: Arc<crate::snapsched::SnapSchedStats>,
    /// The kernel invalidation thread's registry of FUSE requests in
    /// flight (`kernel_inval`); `InFlight::disabled()` without one.
    pub inflight: crate::kernel_inval::InFlight,
    /// The node's open-orphan hold writer (`crate::holds`), whose view
    /// registry says whether *any* view has an inode open: `unlink`'s
    /// fast reap asks it before reaping. `None` in unit tests.
    pub holds: Option<Arc<crate::holds::Holds>>,
    /// The watchdog every op of this view registers with (the daemon
    /// shares one across its views; `status` reports it).
    pub watch: OpWatch,
    /// What the frontend serving this view can do; the view derives its
    /// name/xattr/identity policies from it.
    pub caps: FrontendCaps,
    /// The engine's host services (staging's hole punching).
    pub host: constellation_platform::HostServices,
}

/// One mounted view of the filesystem, behind [`constellation_vfs::Vfs`]
/// (see the module doc).
pub struct View {
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    rt: Handle,
    chunk_size: u32,
    writes: WriteShards,
    /// Per-inode ordering of write-session operations (see [`InodeOps`]).
    inode_ops: InodeOps,
    /// Open handle counts per inode, for orphan reaping on last close.
    opens: Mutex<HashMap<Ino, u32>>,
    /// What each passthrough open holds until its `release`
    /// ([`PassthroughHandle`]), per inode. Keyed by inode and not by
    /// handle because a view's handle *is* its inode (`open` answers
    /// `Fh(ino)`, §6.12), so several concurrent passthrough opens of one
    /// file are several entries in one vector.
    passthrough: Mutex<HashMap<Ino, Vec<PassthroughHandle>>>,
    /// Sequential readahead.
    pub(crate) prefetch: crate::prefetch::Prefetcher,
    /// Cross-file readahead for ordered directory walks.
    scan: crate::scan::ScanAhead,
    /// Cooperative cache (phase 5). `None` only in unit tests that
    /// construct a filesystem without a live store/P2P stack.
    coop: Option<std::sync::Arc<crate::coop::Coop>>,
    /// Publication path to the sync task (None in tests).
    sync: Option<SyncHandle>,
    staging_dir: PathBuf,
    staging_budget: Arc<StagingBudget>,
    staging_gen: GenCounter,
    snapshots: Arc<crate::snapshot::SnapshotManager>,
    synthetic: Mutex<SyntheticRegistry>,
    tree_cache: Mutex<(
        HashMap<crate::snapshot::FrozenObject, crate::snapshot::FrozenDir>,
        VecDeque<crate::snapshot::FrozenObject>,
    )>,
    view_root: Ino,
    /// Cached effective quota (`None` = unlimited), refreshed at most
    /// every [`QUOTA_CACHE_TTL`]. Shared with the control plane so a live
    /// `SetQuota` can invalidate without waiting for the TTL.
    quota_cache: QuotaCache,
    /// Cached cap on this view's own root directory (`subtree_quota`),
    /// refreshed like `quota_cache` and invalidated with it.
    subtree_quota_cache: QuotaCache,
    /// The bytes under this view's root as subtree-cap admission sees
    /// them: a walk plus a running delta of growth admitted since
    /// (`SubtreeUsage`).
    subtree_usage: Mutex<SubtreeUsage>,
    /// Cached `(bytes, files)` for a *scoped* mount's [`Self::statfs`],
    /// refreshed at most every [`Self::statfs_ttl`]. `None` means never
    /// populated. Unused by a whole-filesystem mount, which reads the
    /// maintained counter instead.
    usage_cache: Mutex<Option<(Instant, u64, u64)>>,
    /// From `CONSTELLATION_STATFS_TTL_S` (default 5s). Zero disables caching.
    statfs_ttl: Duration,
    /// Read-time atime accumulator (plan 20), shared node-wide.
    pub(crate) atime: Arc<crate::atime::AtimeAccumulator>,
    /// Prune counters (plan 22), shared node-wide.
    pub(crate) prune_stats: Arc<crate::prune::PruneStats>,
    /// Snapshot-schedule counters (plan 32), shared node-wide.
    pub(crate) snapsched_stats: Arc<crate::snapsched::SnapSchedStats>,
    /// Requests in flight, for the kernel invalidation thread: a
    /// notification for an inode with a request in flight would block
    /// in the kernel until that request is answered (`kernel_inval`).
    inflight: crate::kernel_inval::InFlight,
    holds: Option<Arc<crate::holds::Holds>>,
    /// Every op in flight, for the stalled-op watchdog.
    watch: OpWatch,
    /// The frontend's capabilities, and the policies derived from them.
    caps: FrontendCaps,
    policies: PolicyStack,
    host: constellation_platform::HostServices,
    /// Confinement's cache of inodes known inside the view (`confine`).
    reach: confine::Reach,
    /// `ViewSpec::confine_links`.
    confine_links: bool,
    /// `ViewSpec::qos`, enforced.
    admission: admission::Admission,
    /// `ViewSpec::labels`.
    labels: std::collections::BTreeMap<String, String>,
    /// The engine's number for this view (`Engine::open_view`; 0 for a
    /// view built outside an engine).
    id: u64,
    /// This view's own `Arc`, once an engine opened it ([`View::bind`]):
    /// what a deferred op (a cold read, `io`'s `defer_cold_read`) carries to
    /// the completion pool. Unset (a view built outside an engine, in unit
    /// tests), nothing defers.
    this: std::sync::OnceLock<std::sync::Weak<View>>,
}

fn staging_code(e: &crate::staging::StagingError) -> Code {
    match e {
        crate::staging::StagingError::Full { .. } => Code::NoSpace,
        // A length the staging *filesystem* refuses as too large is
        // `EFBIG`, not `EIO`: the state directory's own maximum file size
        // bounds a sparse `ftruncate` of a staging file (ext4 stops at
        // 16 TiB), and a `truncate(2)` past the maximum is exactly what
        // POSIX gives `EFBIG` for. Found by pjdfstest's `truncate/12.t`
        // and `ftruncate/12.t`, which truncate to ~909 TiB and accept
        // `EFBIG`, `EINVAL` or success: with the state directory on ext4
        // they saw `EIO` and failed (and on a filesystem with no such
        // limit, like the ZFS the compliance lane's own host uses, the
        // truncate simply succeeds, which is why the lane never showed
        // it). Only this one errno changes shape; every other staging IO
        // error keeps today's `Code::Io` rather than taking the whole
        // `from_io_error` mapping, which would restate the write path's
        // refusals wholesale.
        crate::staging::StagingError::Io(e) => match Code::from_io_error(e) {
            Code::FileTooBig => Code::FileTooBig,
            _ => Code::Io,
        },
    }
}

impl View {
    pub fn new(deps: FsDependencies, chunk_size: u32, _compression: CompressionSetting) -> Self {
        let prefetch = crate::prefetch::Prefetcher::new(
            deps.rt.clone(),
            deps.store.clone(),
            deps.cache.clone(),
            deps.coop.clone(),
        );
        let scan = crate::scan::ScanAhead::new(deps.meta.clone(), deps.cache.usage().budget);
        Self {
            meta: deps.meta,
            store: deps.store,
            cache: deps.cache,
            rt: deps.rt,
            chunk_size,
            writes: WriteShards::new(),
            inode_ops: InodeOps::new(),
            opens: Mutex::new(HashMap::new()),
            passthrough: Mutex::new(HashMap::new()),
            prefetch,
            scan,
            coop: deps.coop,
            sync: deps.sync,
            staging_dir: deps.staging_dir,
            staging_budget: deps.staging_budget,
            staging_gen: GenCounter::default(),
            snapshots: deps.snapshots,
            synthetic: Mutex::new(SyntheticRegistry {
                nodes: HashMap::new(),
                keys: HashMap::new(),
                next: SYNTHETIC_INO_BIT,
            }),
            tree_cache: Mutex::new((HashMap::new(), VecDeque::new())),
            view_root: constellation_fs_core::types::ROOT_INO,
            quota_cache: Arc::new(Mutex::new(None)),
            subtree_quota_cache: Arc::new(Mutex::new(None)),
            subtree_usage: Mutex::new(SubtreeUsage::default()),
            usage_cache: Mutex::new(None),
            statfs_ttl: statfs_ttl_from_env(),
            atime: deps.atime,
            prune_stats: deps.prune_stats,
            snapsched_stats: deps.snapsched_stats,
            inflight: deps.inflight,
            holds: deps.holds,
            watch: deps.watch,
            policies: PolicyStack::for_caps(&deps.caps),
            caps: deps.caps,
            host: deps.host,
            reach: confine::Reach::new(),
            confine_links: false,
            admission: admission::Admission::default(),
            labels: Default::default(),
            id: 0,
            this: std::sync::OnceLock::new(),
        }
    }

    /// Let this view's ops defer to the engine's completion pool
    /// (`crate::completion`): they need the view's `Arc` to outlive the
    /// frontend thread that called them. `Engine::open_view` calls it.
    pub(crate) fn bind(self: &Arc<Self>) {
        let _ = self.this.set(Arc::downgrade(self));
    }

    /// The engine's number for this view.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn set_id(&mut self, id: u64) {
        self.id = id;
        self.tag_watch();
    }

    /// Register this view's ops with the watchdog under its id and
    /// labels (`node.ops` attributes and filters by them).
    fn tag_watch(&mut self) {
        self.watch = self.watch.for_view(self.id, self.labels.clone());
    }

    /// Apply `spec`'s view options (labels, QoS, `confine_links`); the
    /// root is set by [`Self::set_subtree_root`]/[`Self::set_snapshot_root`].
    pub fn apply_spec(&mut self, spec: &ViewSpec) {
        self.confine_links = spec.confine_links;
        self.admission = admission::Admission::new(&spec.qos, &self.staging_budget);
        self.labels = spec.labels.clone();
        self.tag_watch();
    }

    /// `ViewSpec::labels`.
    pub fn labels(&self) -> &std::collections::BTreeMap<String, String> {
        &self.labels
    }

    /// The staging budget a new write session reserves against: the
    /// view's own share when it has a staging limit, else the node's.
    pub(crate) fn session_staging_budget(&self) -> Arc<StagingBudget> {
        self.admission
            .staging()
            .cloned()
            .unwrap_or_else(|| self.staging_budget.clone())
    }

    /// The name is gone (a local `unlink` just succeeded): reap the
    /// orphan now if no handle anywhere on this node keeps it — this
    /// view's own table *and* every other view's, through the hold
    /// writer's registry (a second view of the same node used to be
    /// invisible here, and its open handle lost the inode). Otherwise
    /// the orphan stays for the handles, and the hold writer is nudged so
    /// the claim reaches the bucket at once. Returns whether it reaped.
    pub(crate) fn reap_after_unlink(&self, ino: Ino) -> bool {
        let open_here = self.opens.lock().unwrap().get(&ino).copied().unwrap_or(0) > 0;
        let open_elsewhere = self
            .holds
            .as_ref()
            .is_some_and(|h| h.sources().is_open(ino));
        if open_here || open_elsewhere {
            if let Some(h) = &self.holds {
                h.nudge();
            }
            return false;
        }
        let _ = self.meta.reap_orphan(ino);
        true
    }

    /// Logical used space for the *mounted view*: `(bytes, file_count)`.
    ///
    /// A whole-filesystem mount reads the maintained counter, which is
    /// exact and O(1). A subtree or snapshot mount has to walk its own
    /// root, so that result is cached for [`Self::statfs_ttl`] (env
    /// `CONSTELLATION_STATFS_TTL_S`, default 5s; `0` disables the cache).
    pub(crate) fn view_usage(&self) -> (u64, u64) {
        let root = self.real_ino(constellation_fs_core::types::ROOT_INO);
        if root == constellation_fs_core::types::ROOT_INO {
            return self.meta.usage();
        }
        if !self.statfs_ttl.is_zero() {
            if let Some((fetched_at, bytes, files)) = *self.usage_cache.lock().unwrap() {
                if fetched_at.elapsed() < self.statfs_ttl {
                    return (bytes, files);
                }
            }
        }
        let usage = if Self::is_synthetic(root) {
            self.synthetic_recursive_size(root).unwrap_or((0, 0))
        } else {
            self.meta.recursive_size(root).unwrap_or((0, 0))
        };
        if !self.statfs_ttl.is_zero() {
            *self.usage_cache.lock().unwrap() = Some((Instant::now(), usage.0, usage.1));
        }
        usage
    }

    /// Shared handle so the control plane can invalidate after `SetQuota`.
    pub fn quota_cache_handle(&self) -> QuotaCache {
        self.quota_cache.clone()
    }

    /// Effective quota, cached briefly (quota changes are rare).
    pub(crate) fn cached_quota(&self) -> Option<u64> {
        {
            let guard = self.quota_cache.lock().unwrap();
            if let Some((fetched_at, cap)) = *guard {
                if fetched_at.elapsed() < QUOTA_CACHE_TTL {
                    return cap;
                }
            }
        }
        let cap = self.meta.quota().ok().flatten();
        *self.quota_cache.lock().unwrap() = Some((Instant::now(), cap));
        cap
    }

    /// Invalidate the quota cache after a live `SetQuota`.
    pub fn invalidate_quota_cache(cache: &Mutex<Option<(Instant, Option<u64>)>>) {
        *cache.lock().unwrap() = None;
    }

    /// Best-effort admission check against the cluster-wide logical byte
    /// cap. Growth is measured against `inode.size` -- the same value the
    /// usage counter already accounts for -- and not against the manifest,
    /// whose `file_len` still trails after a sparse `ftruncate` and would
    /// charge the same bytes twice. Not additive across writes on the same
    /// handle: `new_file_len` is the whole intended length.
    ///
    /// A view mounted at a directory with its own subtree cap
    /// (`subtree_quota`) is checked against that too, with the bytes under
    /// its root as the usage: a walk taken at most once per
    /// [`Self::statfs_ttl`] and only near the cap, plus the growth this
    /// view admitted since (`subtree_admit`).
    pub(crate) fn quota_check(&self, ino: Ino, new_file_len: u64) -> Result<(), Code> {
        let fs_cap = self.cached_quota();
        let subtree_cap = self.cached_subtree_quota();
        if fs_cap.is_none() && subtree_cap.is_none() {
            return Ok(());
        }
        // Only paid for when a cap is actually configured.
        let committed = self
            .meta
            .getattr(ino)
            .ok()
            .flatten()
            .map(|attr| attr.size)
            .unwrap_or(0);
        let pending_growth = new_file_len.saturating_sub(committed);
        if let Some(cap) = fs_cap {
            let (used, _) = self.meta.usage();
            if used.saturating_add(pending_growth) > cap {
                return Err(Code::NoSpace);
            }
        }
        if let Some(cap) = subtree_cap {
            self.subtree_admit(ino, committed, new_file_len, cap)?;
        }
        Ok(())
    }

    pub fn set_subtree_root(&mut self, path: &str) -> Result<()> {
        let path = crate::snapshot::normalize_path(path);
        let ino = self
            .meta
            .resolve_path(&path)?
            .with_context(|| format!("mount root {path} does not exist"))?;
        let attr = self.meta.getattr(ino)?.context("mount root disappeared")?;
        if attr.kind != InodeKind::Dir {
            anyhow::bail!("mount root {path} is not a directory");
        }
        self.view_root = ino;
        *self.usage_cache.lock().unwrap() = None;
        Self::invalidate_quota_cache(&self.subtree_quota_cache);
        *self.subtree_usage.lock().unwrap() = SubtreeUsage::default();
        Ok(())
    }

    pub fn set_snapshot_root(&mut self, path: &str, name: &str) -> Result<()> {
        let path = crate::snapshot::normalize_path(path);
        let row = self
            .meta
            .snapshots(Some(&path))?
            .into_iter()
            .find(|row| row.name == name)
            .with_context(|| format!("snapshot {path}@{name} does not exist"))?;
        let node = SyntheticNode::Frozen {
            snapshot_id: row.id,
            kind: InodeKind::Dir,
            mode: 0o555,
            uid: 0,
            gid: 0,
            size: 0,
            mtime_ns: row.created_unix_ms * 1_000_000,
            target: None,
            object: Some(crate::snapshot::SnapshotRoot::parse(&row.root_hash)?.object()),
            xattrs: Vec::new(),
        };
        self.view_root = self.intern_synthetic(format!("mount:{path}@{name}"), node);
        *self.usage_cache.lock().unwrap() = None;
        Ok(())
    }

    /// The replica inode this view shows as its root (plan 30 §M7's
    /// kernel invalidation renumbers it).
    pub fn view_root(&self) -> Ino {
        self.view_root
    }

    pub(crate) fn real_ino(&self, ino: Ino) -> Ino {
        if ino == constellation_fs_core::types::ROOT_INO {
            self.view_root
        } else {
            ino
        }
    }

    pub(crate) fn visible_attr(&self, mut attr: FileAttr) -> FileAttr {
        if attr.ino == self.view_root {
            attr.ino = constellation_fs_core::types::ROOT_INO;
        }
        attr
    }

    /// The watchdog this view's ops register with.
    pub fn op_watch(&self) -> &OpWatch {
        &self.watch
    }

    /// The capabilities of the frontend serving this view.
    pub fn caps(&self) -> &FrontendCaps {
        &self.caps
    }

    /// This view's readahead counters (`status`).
    pub fn prefetch_stats(&self) -> Arc<crate::prefetch::PrefetchStats> {
        self.prefetch.stats()
    }

    /// `attr` as the contract reports it: 512-byte blocks of the logical
    /// size, a 128 KiB I/O size (as `statfs`'s block), and the TTL the
    /// frontend may cache it for.
    pub(crate) fn attr_out(&self, attr: &FileAttr) -> Attr {
        Attr {
            ino: attr.ino,
            kind: kind_out(attr.kind),
            size: attr.size,
            blocks: attr.size.div_ceil(512),
            mode: attr.mode,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            rdev: attr.rdev,
            atime_ns: attr.atime_ns,
            mtime_ns: attr.mtime_ns,
            ctime_ns: attr.ctime_ns,
            blksize: BLOCK_SIZE,
            ttl: *self.ttl(),
        }
    }

    /// A name's resolution to `attr` (generation 0: inode numbers are
    /// never reused).
    pub(crate) fn entry_out(&self, attr: &FileAttr) -> Entry {
        self.note_reached(attr.ino);
        Entry {
            attr: self.attr_out(attr),
            generation: 0,
        }
    }

    /// Ask the sync task for an immediate round without waiting:
    /// close() is the close-to-open publication point, and the sooner
    /// the record ships, the sooner other nodes tail it.
    pub(crate) fn nudge_sync(&self) {
        if let Some(h) = &self.sync {
            let _ = h.tx.send(SyncRequest::Nudge);
        }
    }

    /// Plan 30 §M14: this view's cluster locks (`None`: `--locks local`,
    /// or no sync handle).
    pub(crate) fn cluster_locks(&self) -> Option<&Arc<crate::locks::ClusterLocks>> {
        self.sync.as_ref().and_then(|h| h.locks.as_ref())
    }

    /// The attribute and entry TTL the kernel may cache replies for: 1 s,
    /// or none under `--cto strict` (plan 30 §M8) — a cached entry or size
    /// would let the kernel answer an open without asking, and the strict
    /// open's freshness would not reach what it reads.
    pub(crate) fn ttl(&self) -> &'static Duration {
        static ZERO: Duration = Duration::ZERO;
        static LONE: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
        match &self.sync {
            Some(h) if h.cto_strict => {
                // A lone sequencer keeps a short kernel cache (see
                // `cto::lone_kernel_ttl`): strict costs a single node
                // nothing.
                if self.meta.read_delegations().is_alone() && h.lease.reads_locally() {
                    LONE.get_or_init(crate::cto::lone_kernel_ttl)
                } else {
                    &ZERO
                }
            }
            _ => &TTL,
        }
    }
}

/// The open-orphan hold writer's view of this filesystem (`crate::holds`).
impl crate::holds::OpenHandles for View {
    fn open_inos(&self) -> Vec<Ino> {
        self.opens.lock().unwrap().keys().copied().collect()
    }
}

/// An op slower than this is logged (`CONSTELLATION_SLOW_OP_MS`,
/// default 2000 ms; 0 turns it off).
pub(crate) fn slow_fuse_op() -> Duration {
    static D: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *D.get_or_init(|| {
        match std::env::var("CONSTELLATION_SLOW_OP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2000)
        {
            0 => Duration::MAX,
            ms => Duration::from_millis(ms),
        }
    })
}
