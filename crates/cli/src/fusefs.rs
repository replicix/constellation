//! FUSE filesystem: bridges metadata, staging, the chunk cache, and S3.
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

use crate::staging::{GenCounter, Staging, StagingBudget};
use anyhow::{Context, Result};
use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest, SparseChunks};
use constellation_fs_core::{ChunkHash, FileAttr, Ino, InodeKind, INLINE_CHUNKS_MAX};
use constellation_meta::{Meta, MetaError, MetaStore, ReadKey};
use constellation_store_s3::{ChunkStore, CompressionSetting, DecodePriority};
use fuser::{
    BsdFileFlags, Errno, FileHandle, FileType, Filesystem, INodeNo, InitFlags, KernelConfig,
    LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyLseek, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::io::{Read, Seek};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);
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

/// A cheap, non-cryptographic random value in `[0, 1)`, fresh per call.
/// `RandomState::new()` draws its keys from the OS RNG each time it is
/// constructed, so hashing nothing still yields a value that varies
/// call to call and thread to thread — exactly what jittering a retry
/// backoff needs, without pulling in a `rand` dependency for one call
/// site. Never used where actual unpredictability (security) matters.
fn jitter_fraction() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
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

/// In-flight write state for one inode: bytes live on disk in `staging`
/// (bounded RAM regardless of file size, plan 07), not in a `Vec` per
/// chunk. `dirty` tracks which chunk indices actually have staged
/// content; the rest of the file (up to `file_len`) is served from the
/// committed manifest in `base`.
struct WriteState {
    staging: Staging,
    file_len: u64,
    base: Option<Manifest>,
    sealed: HashMap<u64, ChunkHash>,
    holes: crate::staging::DirtyRuns,
    seal_buffer: Vec<u8>,
    high_water: u64,
    /// Absolute half-open byte ranges this open handle's `write()`
    /// calls actually delivered — as opposed to the padding bytes
    /// `do_write` seeds from the committed manifest so a partial
    /// write's untouched neighborhood reads back correctly within the
    /// handle. Kept separate because that seed is only as fresh as the
    /// manifest at the moment of the call: replaying just these ranges
    /// over whatever base a flush ultimately composes or rebases onto
    /// is what lets a chunk shared by several disjoint concurrent
    /// writers converge instead of each writer's seed silently
    /// clobbering the others' bytes with its own stale copy.
    written: Vec<(u64, u64)>,
}

const WRITE_SHARDS: usize = 256;

/// How many times a forwarded whole-file manifest commit may rebase
/// onto a concurrent update before giving up with `EAGAIN`. Each pass
/// costs one round trip to the holder and loses only to a writer that
/// committed in between, so a handful of attempts absorbs a conflict
/// storm across a realistic writer set without spinning forever.
const MANIFEST_COMMIT_ATTEMPTS: u32 = 8;

/// Per-inode write serialization without making unrelated files contend on
/// one process-wide mutex. FUSE can dispatch callbacks concurrently, while
/// operations on the same inode retain their previous ordering. The shard
/// count is deliberately larger than the maximum automatic FUSE worker count
/// so a slow write-through flush rarely stalls an unrelated inode.
struct WriteShards([Mutex<HashMap<Ino, WriteState>>; WRITE_SHARDS]);

impl WriteShards {
    fn new() -> Self {
        Self(std::array::from_fn(|_| Mutex::new(HashMap::new())))
    }

    fn lock(&self, ino: Ino) -> std::sync::MutexGuard<'_, HashMap<Ino, WriteState>> {
        self.0[ino as usize % WRITE_SHARDS].lock().unwrap()
    }
}

/// Outcome of a `SyncRequest::Acquire` attempt (see its doc).
#[derive(Debug, Clone, Copy, Default)]
pub struct AcquireProgress {
    pub acquired: bool,
    pub holder: u64,
    pub epoch: u64,
}

impl AcquireProgress {
    pub fn busy(holder: u64, epoch: u64) -> Self {
        Self {
            acquired: false,
            holder,
            epoch,
        }
    }
}

/// Outcome of a successful partition handoff (flush + lease release).
#[derive(Debug, Clone)]
pub struct HandoffResult {
    pub epoch: u64,
    pub etag: Option<String>,
    pub head_seq: Option<u64>,
}

/// Plan 30 §M9: how long a fast-path acknowledgement waits for
/// durability before the op is treated as in doubt (a backup that stops
/// answering is reconfigured out within `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS`;
/// a lost lease ends the wait at once).
const DURABLE_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// A request to the daemon's sync task — the authority core's driver
/// (`crate::authority_driver`), which turns each into a core event.
pub enum SyncRequest {
    /// Run a sync round soon; the sender does not wait.
    Nudge,
    /// The write-eligible roster from the registry poll (M13: who the
    /// holder polls).
    Roster(Vec<u64>),
    /// The continuation-epoch machine changed state (the driver
    /// re-reports it to the core).
    EpochChanged,
    /// Ship everything this node can, then publish a plan 28 metadata
    /// commit and reply with `(seq, root)` — the tree a snapshot taken
    /// now retains.
    Publish {
        reply: tokio::sync::oneshot::Sender<Result<(u64, constellation_mtree::NodeHash), String>>,
    },
    /// Upload `ino`'s chunks, run a sync round and report its outcome
    /// (fsync barrier).
    Barrier {
        ino: Ino,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    DrainInode {
        ino: Ino,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Tail to the log head without shipping or publishing anything
    /// (plan 29 M3a: in-daemon GC's liveness-freshness gate). Never
    /// touches a lease, so it is safe from a read-only member.
    TailToHead {
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Take the lease if it is free. `acquired: false` means a live
    /// foreign holder still owns it — `holder`/`epoch` are a best-effort
    /// snapshot of that holder (0/0 when unknown), letting a retrying
    /// caller tell forward progress from a genuinely stuck wait (plan 29
    /// M3c).
    Acquire {
        reply: tokio::sync::oneshot::Sender<Result<AcquireProgress, String>>,
    },
    /// A peer asked us to hand the lease over (M3.3 fast path): flush the
    /// journal to S3 and release. Replies with the epoch and last shipped
    /// seq we held, or `None` if we do not hold it or the flush failed —
    /// the requester then waits the lease out through S3.
    HandOff {
        requester: u64,
        reply: tokio::sync::oneshot::Sender<Option<HandoffResult>>,
    },
    /// A peer forwarded a mutation to this node as (believed) holder.
    /// The reply carries plan 30 §M6's `base` (see
    /// `constellation_authority::PeerMsg::MutateReply`).
    Mutate {
        requester: u64,
        op: Vec<u8>,
        /// Plan 30 §M2: the op's exactly-once identity, for holder-side
        /// dedup.
        rid: constellation_meta::Rid,
        /// Plan 30 §M2 GC: prune `recent` outcomes for `requester`'s
        /// current incarnation up to this seq.
        acked_through: u64,
        reply: tokio::sync::oneshot::Sender<(
            constellation_meta::MutateOutcome,
            Option<u64>,
            constellation_meta::Position,
        )>,
    },
    /// This node's own mutation, when the FUSE fast path could not
    /// execute it locally: the core forwards it, submits it through the
    /// holder's inbox, or takes the lease, per `policy`.
    Submit {
        op: constellation_meta::MutateOp,
        /// Plan 30 §M2: allocated once by the caller and kept across every
        /// retry this op goes through.
        rid: constellation_meta::Rid,
        policy: constellation_authority::Policy,
        /// Plan 30 §M9: a resubmission of an op the fast path executed
        /// here but could not acknowledge (the lease was lost while its
        /// acknowledgement waited for durability): in doubt from the
        /// start, resolved against `completed` by rid.
        in_doubt: bool,
        reply: tokio::sync::oneshot::Sender<constellation_authority::ClientReply>,
    },
    /// Plan 30 §M9: the fast path journaled a row under a durability
    /// gate; the core appends it to the backups now.
    Journaled,
    /// Plan 30 §M8: a strict open or lookup on this node needs to know
    /// how it may read (`constellation_authority::ReadAnswer`).
    ReadIndex {
        ino: Ino,
        dir: bool,
        name: Option<String>,
        reply: tokio::sync::oneshot::Sender<constellation_authority::ReadAnswer>,
    },
    /// Plan 30 §M8: a write the FUSE fast path executed here as the
    /// sequencer touched inodes other nodes hold read delegations on:
    /// answered once they are recalled (or outwaited).
    Recall {
        inos: Vec<Ino>,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// Plan 30 §M8: a peer's ReadIndex, to answer as the sequencer.
    PeerReadIndex {
        requester: u64,
        ino: Ino,
        dir: bool,
        name: Option<String>,
        reply: tokio::sync::oneshot::Sender<constellation_authority::ReadIndexOutcome>,
    },
    /// Plan 30 §M8: the sequencer recalls a read delegation this node
    /// holds; answered once it is no longer honoured.
    PeerRecall {
        holder: u64,
        ino: Ino,
        grant: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// Plan 30 §M9: the holder streams journal transactions to this
    /// node as its backup; answered with `(acked through, sealed)`.
    PeerBackupAppend {
        holder: u64,
        epoch: u64,
        config_version: u64,
        from: u64,
        txs: Vec<constellation_meta::BackupTx>,
        through: u64,
        reply: tokio::sync::oneshot::Sender<(u64, bool)>,
    },
    /// Plan 30 §M9: backup-acked transactions streamed ahead of S3 by
    /// the holder this node follows.
    PeerStreamAhead {
        from: u64,
        epoch: u64,
        base: u64,
        txs: Vec<constellation_meta::BackupTx>,
    },
    /// A peer's gossip says segment `seq` landed (plan 30 §M7: a hint
    /// with no payload). The core tails now unless its log stream from
    /// the holder delivers it.
    SegmentHint {
        seq: u64,
        epoch: u64,
    },
    /// Plan 30 §M7: a peer subscribes to this node's log stream. The
    /// driver hands it to the core and writes whatever the core streams
    /// to `requester` into `sink` (bounded: a subscriber that falls
    /// behind is dropped back to S3 tailing).
    LogSubscribe {
        requester: u64,
        req: u64,
        from: u64,
        sink: tokio::sync::mpsc::Sender<constellation_net::LogEvent>,
        /// Segment bytes queued in `sink` and not yet taken by the stream
        /// writer (the driver adds, the writer's relay subtracts).
        queued_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    },
    ClaimOffer {
        epoch: u64,
    },
    /// Run the deposition recovery now (the control API, or automatically
    /// after mounting a persisted deposed state dir).
    Reintegrate(tokio::sync::oneshot::Sender<Result<String, String>>),
    /// Permanently leave the cluster (self). Flushes, tombstones the
    /// registry record, marks the state dir spent, then the caller
    /// unmounts.
    Leave {
        force: bool,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
    /// Final flush + release on unmount; the core stops afterwards.
    Shutdown {
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

/// FUSE-side handle to the metadata sync task.
pub struct SyncHandle {
    pub tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    /// `--fsync-mode s3`: fsync() returns only once the journal is in S3.
    pub fsync_s3: bool,
    /// Plan 30 §M8: `--cto strict` (see `crate::cto`).
    pub cto_strict: bool,
    /// Lock-free lease view (the core's state, mirrored by the driver);
    /// the write gate reads it per mutating op.
    pub lease: Arc<crate::lease::LeaseView>,
    /// Bound on how long a mutation waits for a foreign holder.
    pub acquire_deadline: Duration,
    /// Offline designation (DESIGN.md §5.2). `None` when no designations
    /// exist for this mount — the write gate then behaves exactly as
    /// before phase 4a.
    pub designations: Option<Arc<crate::designation::DesignationManager>>,
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

/// Everything [`ConstellationFs::new`] needs besides the two filesystem
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
}

const SYNTHETIC_INO_BIT: u64 = 1 << 63;

#[derive(Debug, Clone)]
pub(crate) enum SyntheticNode {
    Constellation {
        path: String,
    },
    SnapshotDirectory {
        path: String,
    },
    Frozen {
        snapshot_id: String,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
        mtime_ns: i64,
        target: Option<String>,
        object: Option<crate::snapshot::FrozenObject>,
        xattrs: Vec<(String, Vec<u8>)>,
    },
}

struct SyntheticRegistry {
    nodes: HashMap<Ino, SyntheticNode>,
    keys: HashMap<String, Ino>,
    next: Ino,
}

pub struct ConstellationFs {
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    rt: Handle,
    chunk_size: u32,
    writes: WriteShards,
    /// Open handle counts per inode, for orphan reaping on last close.
    opens: Mutex<HashMap<Ino, u32>>,
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
}

fn staging_errno(e: &crate::staging::StagingError) -> i32 {
    match e {
        crate::staging::StagingError::Full { .. } => libc::ENOSPC,
        crate::staging::StagingError::Io(_) => libc::EIO,
    }
}

fn errno(e: &MetaError) -> i32 {
    match e {
        MetaError::NoEnt(_) | MetaError::NoEntry => libc::ENOENT,
        MetaError::Exists => libc::EEXIST,
        MetaError::NotDir => libc::ENOTDIR,
        MetaError::IsDir => libc::EISDIR,
        MetaError::NotEmpty => libc::ENOTEMPTY,
        MetaError::NoData => libc::ENODATA,
        MetaError::Invalid(_) => libc::EINVAL,
        MetaError::Conflict => libc::EAGAIN,
        MetaError::Fjall(_)
        | MetaError::Io(_)
        | MetaError::Record(_)
        | MetaError::Key(_)
        | MetaError::Json(_)
        | MetaError::Postcard(_) => libc::EIO,
    }
}

/// Why a mutation did not commit. Separate from a bare errno so that an
/// optimistic-concurrency rejection can carry the state to rebase onto.
pub(crate) enum MutateFail {
    Errno(i32),
    /// The base this update was composed on is no longer current.
    /// `manifest` is the holder's image when it came back over the wire;
    /// `None` means rebase from the local replica, which is
    /// authoritative whenever this node executed the mutation itself.
    Conflict {
        manifest: Option<Vec<u8>>,
    },
}

fn mutate_fail(e: MetaError) -> MutateFail {
    match e {
        MetaError::Conflict => MutateFail::Conflict { manifest: None },
        other => MutateFail::Errno(errno(&other)),
    }
}

const RSIZE_XATTR: &str = "user.constellation.rsize";
const RCOUNT_XATTR: &str = "user.constellation.rcount";

fn virtual_xattr(name: &str) -> bool {
    matches!(name, RSIZE_XATTR | RCOUNT_XATTR)
}

fn to_fuse_attr(a: &FileAttr) -> fuser::FileAttr {
    let kind = match a.kind {
        InodeKind::File => FileType::RegularFile,
        InodeKind::Dir => FileType::Directory,
        InodeKind::Symlink => FileType::Symlink,
        InodeKind::Fifo => FileType::NamedPipe,
        InodeKind::Socket => FileType::Socket,
        InodeKind::BlockDev => FileType::BlockDevice,
        InodeKind::CharDev => FileType::CharDevice,
    };
    let ts = |ns: i64| {
        if ns >= 0 {
            UNIX_EPOCH + Duration::from_nanos(ns as u64)
        } else {
            UNIX_EPOCH
        }
    };
    fuser::FileAttr {
        ino: INodeNo(a.ino),
        size: a.size,
        blocks: a.size.div_ceil(512),
        atime: ts(a.atime_ns),
        mtime: ts(a.mtime_ns),
        ctime: ts(a.ctime_ns),
        crtime: ts(a.ctime_ns),
        kind,
        perm: a.mode as u16,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: a.rdev as u32,
        blksize: 131072,
        flags: 0,
    }
}

fn time_or_now_ns(t: TimeOrNow) -> i64 {
    let st = match t {
        TimeOrNow::SpecificTime(st) => st,
        TimeOrNow::Now => SystemTime::now(),
    };
    st.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

impl ConstellationFs {
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
            opens: Mutex::new(HashMap::new()),
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
            usage_cache: Mutex::new(None),
            statfs_ttl: statfs_ttl_from_env(),
            atime: deps.atime,
            prune_stats: deps.prune_stats,
        }
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
    pub(crate) fn quota_check(&self, ino: Ino, new_file_len: u64) -> Result<(), i32> {
        let Some(cap) = self.cached_quota() else {
            return Ok(());
        };
        let (used, _) = self.meta.usage();
        // Only paid for when a cap is actually configured.
        let committed = self
            .meta
            .getattr(ino)
            .ok()
            .flatten()
            .map(|attr| attr.size)
            .unwrap_or(0);
        let pending_growth = new_file_len.saturating_sub(committed);
        if used.saturating_add(pending_growth) > cap {
            return Err(libc::ENOSPC);
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

    pub(crate) fn is_synthetic(ino: Ino) -> bool {
        ino & SYNTHETIC_INO_BIT != 0
    }

    pub(crate) fn synthetic_node(&self, ino: Ino) -> Option<SyntheticNode> {
        self.synthetic.lock().unwrap().nodes.get(&ino).cloned()
    }

    pub(crate) fn synthetic_active(&self, node: &SyntheticNode) -> bool {
        let SyntheticNode::Frozen { snapshot_id, .. } = node else {
            return true;
        };
        self.meta
            .snapshots(None)
            .map(|rows| rows.iter().any(|row| &row.id == snapshot_id))
            .unwrap_or(false)
    }

    pub(crate) fn intern_synthetic(&self, key: String, node: SyntheticNode) -> Ino {
        let mut registry = self.synthetic.lock().unwrap();
        if let Some(ino) = registry.keys.get(&key) {
            return *ino;
        }
        let ino = registry.next;
        registry.next += 1;
        registry.nodes.insert(ino, node);
        registry.keys.insert(key, ino);
        ino
    }

    pub(crate) fn synthetic_attr(&self, ino: Ino, node: &SyntheticNode) -> FileAttr {
        let (kind, mode, uid, gid, size, mtime_ns) = match node {
            SyntheticNode::Constellation { .. } | SyntheticNode::SnapshotDirectory { .. } => {
                (InodeKind::Dir, 0o555, 0, 0, 0, 0)
            }
            SyntheticNode::Frozen {
                kind,
                mode,
                uid,
                gid,
                size,
                mtime_ns,
                ..
            } => (*kind, *mode & !0o222, *uid, *gid, *size, *mtime_ns),
        };
        FileAttr {
            ino,
            kind,
            size,
            mode,
            uid,
            gid,
            nlink: if kind == InodeKind::Dir { 2 } else { 1 },
            atime_ns: mtime_ns,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: 0,
        }
    }

    pub(crate) fn synthetic_xattrs(&self, ino: Ino) -> Result<Vec<(String, Vec<u8>)>, i32> {
        let node = self.synthetic_node(ino).ok_or(libc::ESTALE)?;
        if !self.synthetic_active(&node) {
            return Err(libc::ESTALE);
        }
        match node {
            SyntheticNode::Frozen {
                kind: InodeKind::Dir,
                object: Some(tree_hash),
                xattrs,
                ..
            } if xattrs.is_empty() => Ok(self.snapshot_tree(tree_hash)?.xattrs),
            SyntheticNode::Frozen { xattrs, .. } => Ok(xattrs),
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) fn synthetic_recursive_size(&self, ino: Ino) -> Result<(u64, u64), i32> {
        let node = self.synthetic_node(ino).ok_or(libc::ESTALE)?;
        if !self.synthetic_active(&node) {
            return Err(libc::ESTALE);
        }
        self.synthetic_node_recursive_size(&node)
    }

    fn synthetic_node_recursive_size(&self, node: &SyntheticNode) -> Result<(u64, u64), i32> {
        match node {
            SyntheticNode::Frozen {
                kind: InodeKind::File,
                size,
                ..
            } => Ok((*size, 1)),
            SyntheticNode::Frozen {
                kind: InodeKind::Dir,
                object: Some(tree_hash),
                ..
            } => {
                let mut total = (0u64, 0u64);
                for entry in self.snapshot_tree(*tree_hash)?.entries {
                    match entry.kind {
                        InodeKind::File => {
                            total.0 = total.0.saturating_add(entry.size);
                            total.1 = total.1.saturating_add(1);
                        }
                        InodeKind::Dir => {
                            let child = SyntheticNode::Frozen {
                                snapshot_id: String::new(),
                                kind: entry.kind,
                                mode: entry.mode,
                                uid: entry.uid,
                                gid: entry.gid,
                                size: entry.size,
                                mtime_ns: entry.mtime_ns,
                                target: entry.target,
                                object: entry.object,
                                xattrs: entry.xattrs,
                            };
                            let subtotal = self.synthetic_node_recursive_size(&child)?;
                            total.0 = total.0.saturating_add(subtotal.0);
                            total.1 = total.1.saturating_add(subtotal.1);
                        }
                        _ => {}
                    }
                }
                Ok(total)
            }
            _ => Ok((0, 0)),
        }
    }

    pub(crate) fn snapshot_tree(
        &self,
        hash: crate::snapshot::FrozenObject,
    ) -> Result<crate::snapshot::FrozenDir, i32> {
        {
            let cache = self.tree_cache.lock().unwrap();
            if let Some(tree) = cache.0.get(&hash) {
                return Ok(tree.clone());
            }
        }
        let tree = self
            .rt
            .block_on(self.snapshots.list_frozen(&hash))
            .map_err(|_| libc::EIO)?;
        let mut cache = self.tree_cache.lock().unwrap();
        if cache.0.len() >= 128 {
            if let Some(oldest) = cache.1.pop_front() {
                cache.0.remove(&oldest);
            }
        }
        cache.0.insert(hash, tree.clone());
        cache.1.push_back(hash);
        Ok(tree)
    }

    pub(crate) fn lookup_synthetic(
        &self,
        parent: Ino,
        name: &str,
    ) -> Result<Option<(Ino, FileAttr)>, i32> {
        let node = if !Self::is_synthetic(parent) {
            if name != ".constellation" {
                return Ok(None);
            }
            let attr = self
                .meta
                .getattr(parent)
                .map_err(|error| errno(&error))?
                .ok_or(libc::ENOENT)?;
            if attr.kind != InodeKind::Dir {
                return Ok(None);
            }
            let path = self.meta.path_of(parent).map_err(|_| libc::EIO)?;
            SyntheticNode::Constellation { path }
        } else {
            let parent_node = self.synthetic_node(parent).ok_or(libc::ESTALE)?;
            if !self.synthetic_active(&parent_node) {
                return Err(libc::ESTALE);
            }
            match parent_node {
                SyntheticNode::Constellation { path } if name == "snapshot" => {
                    SyntheticNode::SnapshotDirectory { path }
                }
                SyntheticNode::SnapshotDirectory { path } => {
                    let (row, root) = self
                        .rt
                        .block_on(self.snapshots.covering(&path))
                        .map_err(|_| libc::EIO)?
                        .into_iter()
                        .find(|(row, _)| row.name == name)
                        .ok_or(libc::ENOENT)?;
                    SyntheticNode::Frozen {
                        snapshot_id: row.id,
                        kind: InodeKind::Dir,
                        mode: 0o555,
                        uid: 0,
                        gid: 0,
                        size: 0,
                        mtime_ns: row.created_unix_ms * 1_000_000,
                        target: None,
                        object: Some(root),
                        xattrs: Vec::new(),
                    }
                }
                SyntheticNode::Frozen {
                    snapshot_id,
                    kind: InodeKind::Dir,
                    object: Some(tree_hash),
                    ..
                } => {
                    let tree = self.snapshot_tree(tree_hash)?;
                    let Some(entry) = tree.entries.into_iter().find(|entry| entry.name == name)
                    else {
                        return Ok(None);
                    };
                    SyntheticNode::Frozen {
                        snapshot_id,
                        kind: entry.kind,
                        mode: entry.mode,
                        uid: entry.uid,
                        gid: entry.gid,
                        size: entry.size,
                        mtime_ns: entry.mtime_ns,
                        target: entry.target,
                        object: entry.object,
                        xattrs: entry.xattrs,
                    }
                }
                _ => return Ok(None),
            }
        };
        let key = format!("{parent}/{name}");
        let ino = self.intern_synthetic(key, node.clone());
        Ok(Some((ino, self.synthetic_attr(ino, &node))))
    }

    pub(crate) fn synthetic_entries(&self, ino: Ino) -> Result<Vec<(Ino, InodeKind, String)>, i32> {
        let node = self.synthetic_node(ino).ok_or(libc::ESTALE)?;
        if !self.synthetic_active(&node) {
            return Err(libc::ESTALE);
        }
        let children: Vec<(String, SyntheticNode)> = match node {
            SyntheticNode::Constellation { path } => {
                vec![("snapshot".into(), SyntheticNode::SnapshotDirectory { path })]
            }
            SyntheticNode::SnapshotDirectory { path } => self
                .rt
                .block_on(self.snapshots.covering(&path))
                .map_err(|_| libc::EIO)?
                .into_iter()
                .map(|(row, root)| {
                    Ok((
                        row.name,
                        SyntheticNode::Frozen {
                            snapshot_id: row.id,
                            kind: InodeKind::Dir,
                            mode: 0o555,
                            uid: 0,
                            gid: 0,
                            size: 0,
                            mtime_ns: row.created_unix_ms * 1_000_000,
                            target: None,
                            object: Some(root),
                            xattrs: Vec::new(),
                        },
                    ))
                })
                .collect::<Result<_, i32>>()?,
            SyntheticNode::Frozen {
                snapshot_id,
                kind: InodeKind::Dir,
                object: Some(tree_hash),
                ..
            } => self
                .snapshot_tree(tree_hash)?
                .entries
                .into_iter()
                .map(|entry| {
                    (
                        entry.name,
                        SyntheticNode::Frozen {
                            snapshot_id: snapshot_id.clone(),
                            kind: entry.kind,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size: entry.size,
                            mtime_ns: entry.mtime_ns,
                            target: entry.target,
                            object: entry.object,
                            xattrs: entry.xattrs,
                        },
                    )
                })
                .collect(),
            _ => return Err(libc::ENOTDIR),
        };
        Ok(children
            .into_iter()
            .map(|(name, node)| {
                let child = self.intern_synthetic(format!("{ino}/{name}"), node.clone());
                let kind = self.synthetic_attr(child, &node).kind;
                (child, kind, name)
            })
            .collect())
    }

    pub(crate) fn read_frozen(&self, ino: Ino, offset: u64, size: u64) -> Result<Vec<u8>, i32> {
        let node = self.synthetic_node(ino).ok_or(libc::ESTALE)?;
        if !self.synthetic_active(&node) {
            return Err(libc::ESTALE);
        }
        let SyntheticNode::Frozen {
            kind: InodeKind::File,
            object: Some(manifest_hash),
            ..
        } = node
        else {
            return Err(libc::EISDIR);
        };
        let manifest = self
            .rt
            .block_on(self.snapshots.load_manifest(&manifest_hash))
            .map_err(|error| {
                tracing::debug!(%error, ino, "frozen read: manifest load failed");
                libc::EIO
            })?;
        let hashes = self.chunk_list(&manifest)?;
        tracing::debug!(
            ino,
            file_len = manifest.file_len,
            chunks = hashes.len(),
            "frozen read: manifest loaded"
        );
        if offset >= manifest.file_len {
            return Ok(Vec::new());
        }
        let len = size.min(manifest.file_len - offset);
        let mut out = Vec::with_capacity(len as usize);
        for slice in manifest.layout.slices(offset, len) {
            let chunk = self
                .read_committed_chunk(ino, &hashes, slice.index)
                .inspect_err(|errno| {
                    tracing::debug!(
                        ino,
                        index = slice.index,
                        errno,
                        "frozen read: chunk read failed"
                    )
                })?;
            let start = slice.offset as usize;
            let end = (slice.offset + slice.len) as usize;
            if chunk.len() < end {
                return Err(libc::EIO);
            }
            out.extend_from_slice(&chunk[start..end]);
        }
        Ok(out)
    }

    /// Ask the sync task for an immediate round without waiting:
    /// close() is the close-to-open publication point, and the sooner
    /// the record ships, the sooner other nodes tail it.
    pub(crate) fn nudge_sync(&self) {
        if let Some(h) = &self.sync {
            let _ = h.tx.send(SyncRequest::Nudge);
        }
    }

    /// The write gate (DESIGN.md §5): every mutating op passes through
    /// here, reads never do. The fast path is a few atomic loads; only
    /// when the lease is not currently usable does this hand off to the
    /// sync task, and only the first mutation after an idle release
    /// pays a CAS round trip.
    #[allow(dead_code)]
    pub(crate) fn require_lease(&self) -> Result<(), i32> {
        self.require_lease_for(constellation_fs_core::types::ROOT_INO)
    }

    pub(crate) fn require_lease_for(&self, ino: Ino) -> Result<(), i32> {
        let Some(h) = &self.sync else { return Ok(()) };
        if h.read_only_member {
            return Err(libc::EROFS);
        }
        if let Some(departed) = &h.departed {
            if departed.load(std::sync::atomic::Ordering::Relaxed) {
                tracing::error!("refusing mutation: this node has left the cluster");
                return Err(libc::EIO);
            }
        }
        if let Some(frozen) = &h.epoch_frozen {
            if frozen.load(std::sync::atomic::Ordering::Relaxed) {
                tracing::error!("refusing mutation: continuation epoch frozen (lost a member)");
                return Err(libc::EROFS);
            }
        }
        // Offline designation gate (DESIGN.md §5.2), checked before the
        // ordinary lease: a designated path's write authority does not
        // come from the partition lease at all while the designee is
        // reachable — see the module doc on `designation::GateDecision`.
        if let Some(designations) = &h.designations {
            let path = self.meta.path_of(ino).unwrap_or_else(|_| "/".into());
            match self.rt.block_on(designations.check(&path)) {
                crate::designation::GateDecision::NoDesignation => {}
                crate::designation::GateDecision::Proceed => return Ok(()),
                crate::designation::GateDecision::ReadOnly { designee, path } => {
                    tracing::error!(
                        path,
                        designee,
                        "refusing mutation: path is offline-designated to another node \
                         and no delegation is available"
                    );
                    return Err(libc::EROFS);
                }
            }
        }
        if h.epoch_active
            .as_ref()
            .is_some_and(|active| active.load(std::sync::atomic::Ordering::Relaxed))
            && h.lease.usable()
        {
            h.lease.touch();
            return Ok(());
            // Otherwise the core performs a P2P-only handoff in epoch mode.
        }
        let part = "p0";
        if h.lease.open_for_new_mutation() {
            h.lease.touch();
            return Ok(());
        }
        if h.lease.is_lost() {
            tracing::error!(
                part,
                "refusing mutation: this node lost the partition lease"
            );
            return Err(libc::EIO);
        }
        let start = std::time::Instant::now();
        // Each retry is a classify GET (and, the first time round, a CAS
        // PUT registering this node in the holder's `wanted_by`). A fixed
        // 100 ms sleep meant ten of those per second per blocked thread for
        // as long as the wait lasts, which on a sticky lease is up to half
        // a TTL. Back off instead: the first few retries stay responsive
        // for the common case where the holder is about to let go, and a
        // long wait settles at one probe every 2 s.
        let mut backoff = Duration::from_millis(100);
        // `h.acquire_deadline` (2xTTL) bounds *stalled* time, not total
        // wait (plan 29 M3c): under sustained contention the lease can
        // legitimately need several dwell/grace cycles to reach this
        // node, each of which changes who holds it (or at least its
        // epoch) well before any single such cycle takes 2xTTL. Resetting
        // the clock on every observed change means this loop waits as
        // long as the system keeps visibly making progress, and only
        // gives up when the same (holder, epoch) has sat unchanged for a
        // full 2xTTL — a holder that is truly frozen or unreachable, not
        // one merely busy trading the lease among several waiters.
        let mut last_seen: Option<(u64, u64)> = None;
        let mut no_progress_since = start;
        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if h.tx.send(SyncRequest::Acquire { reply: tx }).is_err() {
                return Err(libc::EIO);
            }
            match rx.blocking_recv() {
                Ok(Ok(progress)) if progress.acquired => {
                    h.lease.touch();
                    return Ok(());
                }
                Ok(Ok(progress)) => {
                    let seen = (progress.holder, progress.epoch);
                    if last_seen != Some(seen) {
                        last_seen = Some(seen);
                        no_progress_since = std::time::Instant::now();
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "lease acquisition failed");
                    return Err(libc::EIO);
                }
                Err(_) => return Err(libc::EIO),
            }
            if no_progress_since.elapsed() >= h.acquire_deadline {
                tracing::error!(
                    waited = ?start.elapsed(),
                    stalled = ?no_progress_since.elapsed(),
                    part,
                    "another node holds the partition lease with no progress; \
                     failing the write with EIO"
                );
                return Err(libc::EIO);
            }
            // Jittered, not a plain `sleep(backoff)` (plan 29 M3c): every
            // blocked FUSE thread runs the exact same deterministic
            // backoff schedule (100, 200, 400, ... capped at 2s), and
            // several nodes mounted around the same moment start retrying
            // at nearly the same wall-clock instant. Unjittered, their
            // retries — and so their `register_wanted`/claim CAS attempts
            // — stay phase-locked indefinitely, repeatedly colliding with
            // each other rather than the holder: `create-storm-s3-only`
            // reproduced a clean, unchanging (holder, epoch) for a full
            // 2xTTL this way, which is a livelock between waiters, not
            // contention with the holder. A random 0-50% stretch on each
            // sleep breaks the lockstep after a handful of retries.
            std::thread::sleep(backoff + backoff.mul_f64(jitter_fraction() * 0.5));
            backoff = (backoff * 2).min(Duration::from_millis(2000));
        }
    }

    /// Execute a namespace mutation locally when we hold the partition,
    /// otherwise ask the holder to validate and journal it. Busy or stale
    /// holder information falls back to the ordinary lease acquisition path.
    pub(crate) fn mutate_op(
        &self,
        part_hint_ino: Ino,
        op: constellation_meta::MutateOp,
    ) -> Result<(), i32> {
        self.mutate_op_rebasable(part_hint_ino, op)
            .map_err(|failure| match failure {
                MutateFail::Errno(e) => e,
                // Callers that cannot rebase surface the conflict as a
                // retryable error rather than losing the update.
                MutateFail::Conflict { .. } => libc::EAGAIN,
            })
    }

    /// As [`Self::mutate_op`], but reporting an optimistic-concurrency
    /// rejection as [`MutateFail::Conflict`] so a caller holding the
    /// material to recompose its update can rebase and retry.
    ///
    /// Plan 30 §M2 GC: every completion path — success, an explicit
    /// refusal, or giving up entirely — marks this op's rid "acked"
    /// exactly once before returning (never left for only the forward
    /// path to do, which is what previously stalled `acked_through`
    /// forever behind the first op that executed locally, took a
    /// designation/read-only/departed refusal, or fell back to the
    /// lease path). A rid that this call never actually sends anywhere
    /// (an early refusal, or forwarding disabled) still gets marked: the
    /// holder never learned of it, so marking it is a no-op for GC
    /// purposes, but *skipping* it would leave a permanent hole in the
    /// contiguous `acked_through` floor for every later op's seq to
    /// stall behind. The one exception is the earliest checks
    /// (`is_synthetic`, no `self.sync`), which run *before* a rid is
    /// even allocated — there is nothing to mark yet.
    /// Plan 30 §M6: every local read path's session wait (read-your-
    /// writes and monotonic reads per node; see `constellation_meta::
    /// session`). A single-node mount (no sync handle) observes nothing
    /// and skips it. Bounded by `CONSTELLATION_SESSION_WAIT_MS`; a timeout
    /// answers from the replica anyway (degraded, not an error).
    pub(crate) fn session_wait(&self, keys: &[ReadKey]) {
        if self.sync.is_none() {
            return;
        }
        let _ = self.meta.session_wait(keys);
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

    /// Plan 30 §M8: the read wait of a `cto=strict` open (`dir: false`)
    /// or lookup (`dir: true`, `name`) of `ino`, reading `keys`. Bounded
    /// mode, and a mount without a sync handle, is M6's session wait.
    ///
    /// Strict: the sequencer reads its own replica (authoritative); a
    /// node holding a read delegation on `ino` reads locally once its
    /// replica has reached the grant's position (renewing it in the
    /// background past half its lifetime); anyone else asks the sequencer
    /// (`SyncRequest::ReadIndex`) and waits for the position it answers.
    /// Every path ends in the session wait, bounded as M6's is: a
    /// sequencer that does not answer degrades the read, never fails it.
    pub(crate) fn strict_read(&self, ino: Ino, dir: bool, name: Option<&str>, keys: &[ReadKey]) {
        let Some(h) = &self.sync else {
            return;
        };
        if !h.cto_strict {
            let _ = self.meta.session_wait(keys);
            return;
        }
        let deleg = self.meta.read_delegations();
        deleg.count(|s| s.strict_reads += 1);
        if h.lease.reads_locally() {
            deleg.count(|s| s.holder_local += 1);
            let _ = self.meta.session_wait(keys);
            return;
        }
        let now = constellation_store_s3::lease::now_unix_ms();
        if let Some((held, renew)) = deleg.valid(ino, now) {
            deleg.count(|s| s.delegation_local += 1);
            if renew {
                // In use and past half its life: renew in the background
                // (the answer installs the new grant; nobody waits).
                deleg.count(|s| s.renewals += 1);
                let (reply, _) = tokio::sync::oneshot::channel();
                let _ = h.tx.send(SyncRequest::ReadIndex {
                    ino,
                    dir,
                    name: name.map(str::to_string),
                    reply,
                });
            }
            let waited = self.meta.session_wait_at(keys, &held.position);
            tracing::debug!(
                target: "constellation::cto",
                ino,
                dir,
                name,
                position = ?held.position,
                ?waited,
                "strict read under a delegation"
            );
            return;
        }
        let started = std::time::Instant::now();
        let (reply, answer) = tokio::sync::oneshot::channel();
        let sent =
            h.tx.send(SyncRequest::ReadIndex {
                ino,
                dir,
                name: name.map(str::to_string),
                reply,
            })
            .is_ok();
        let answer = if sent {
            answer.blocking_recv().ok()
        } else {
            None
        };
        match answer {
            Some(constellation_authority::ReadAnswer::Holder) => {
                deleg.count(|s| s.holder_local += 1);
                let _ = self.meta.session_wait(keys);
            }
            Some(constellation_authority::ReadAnswer::Position {
                position,
                delegated,
            }) => {
                let waited = self.meta.session_wait_at(keys, &position);
                tracing::debug!(
                    target: "constellation::cto",
                    ino,
                    dir,
                    name,
                    ?position,
                    delegated,
                    ?waited,
                    applied = ?self.meta.session().applied(),
                    "strict read after a ReadIndex"
                );
                deleg.note_read_index(started.elapsed().as_millis() as u64);
            }
            Some(constellation_authority::ReadAnswer::Tailed) => {
                tracing::debug!(target: "constellation::cto", ino, dir, name, "strict read: tailed S3");
                deleg.count(|s| s.s3_tail += 1);
                let _ = self.meta.session_wait(keys);
            }
            Some(constellation_authority::ReadAnswer::Degraded) | None => {
                deleg.count(|s| s.degraded += 1);
                let _ = self.meta.session_wait(keys);
            }
        }
    }

    /// Plan 30 §M8: a write this node's FUSE fast path executed as the
    /// sequencer returns only once no other node honours a read
    /// delegation on what it touched. One lock and an empty map when
    /// nobody holds one (every single-node and bounded-only cluster).
    fn recall_after_local_write(&self, h: &SyncHandle, records: &[constellation_meta::LogRecord]) {
        self.recall_after_local_inos(h, constellation_meta::recall_inos(records));
    }

    /// [`Self::recall_after_local_write`] for writes that do not go
    /// through `execute_mutate` (the holder's own manifest commit).
    fn recall_after_local_inos(&self, h: &SyncHandle, inos: Vec<Ino>) {
        let now = constellation_store_s3::lease::now_unix_ms();
        if self
            .meta
            .read_delegations()
            .touching(&inos, None, now)
            .is_empty()
        {
            return;
        }
        let started = std::time::Instant::now();
        let (reply, done) = tokio::sync::oneshot::channel();
        if h.tx.send(SyncRequest::Recall { inos, reply }).is_ok() {
            let _ = done.blocking_recv();
        }
        let waited = started.elapsed().as_millis() as u64;
        self.meta.read_delegations().count(|s| {
            s.fuse_writes_recalled += 1;
            s.fuse_recall_wait_ms_total += waited;
        });
    }

    pub(crate) fn mutate_op_rebasable(
        &self,
        part_hint_ino: Ino,
        op: constellation_meta::MutateOp,
    ) -> Result<(), MutateFail> {
        if Self::is_synthetic(part_hint_ino) {
            return Err(MutateFail::Errno(libc::EROFS));
        }
        let Some(h) = &self.sync else {
            return constellation_meta::execute_mutate(&self.meta, &op, None)
                .map(|_| ())
                .map_err(mutate_fail);
        };
        // This op's exactly-once identity, allocated once, here, before
        // any forward or lease-acquisition attempt below, and kept
        // unchanged across every retry this call goes through (a
        // forward, a redirected forward, or the lease-path fallback). A
        // caller that needs a genuinely new op after a rebase (e.g.
        // `SetManifest`'s optimistic-concurrency retry) calls back into
        // this function again, which allocates a fresh one.
        let rid = constellation_meta::Rid {
            node: h.node_id,
            incarnation: h.incarnation,
            seq: h
                .next_rid_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        };
        let result = self.mutate_op_rebasable_with_rid(h, part_hint_ino, &op, rid);
        h.acked.lock().unwrap().push(rid.seq);
        result
    }

    fn mutate_op_rebasable_with_rid(
        &self,
        h: &SyncHandle,
        part_hint_ino: Ino,
        op: &constellation_meta::MutateOp,
        rid: constellation_meta::Rid,
    ) -> Result<(), MutateFail> {
        if h.read_only_member {
            return Err(MutateFail::Errno(libc::EROFS));
        }
        if h.departed
            .as_ref()
            .is_some_and(|departed| departed.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(MutateFail::Errno(libc::EIO));
        }
        if h.epoch_frozen
            .as_ref()
            .is_some_and(|frozen| frozen.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(MutateFail::Errno(libc::EROFS));
        }
        if let Some(designations) = &h.designations {
            let path = self
                .meta
                .path_of(part_hint_ino)
                .unwrap_or_else(|_| "/".into());
            match self.rt.block_on(designations.check(&path)) {
                crate::designation::GateDecision::NoDesignation => {}
                crate::designation::GateDecision::Proceed => {
                    let records = constellation_meta::execute_mutate(&self.meta, op, Some(rid))
                        .map_err(mutate_fail)?;
                    self.recall_after_local_write(h, &records);
                    return Ok(());
                }
                crate::designation::GateDecision::ReadOnly { .. } => {
                    return Err(MutateFail::Errno(libc::EROFS));
                }
            }
        }
        // Plan 30 §M3b: the fast path admits the op (counted in flight)
        // atomically with respect to a release's final flush + CAS — see
        // `lease.rs`'s module doc, "The releasing flag".
        if let Some(admitted) = h.lease.admit() {
            let result = constellation_meta::execute_mutate(&self.meta, op, Some(rid));
            // Plan 30 §M9: under a durability gate, the row this op
            // journaled (at or below the tip now) must reach the backups
            // or the log before the acknowledgement.
            let jseq = if h.lease.ack_gated() && result.is_ok() {
                Some(self.meta.journal_tip().unwrap_or(u64::MAX))
            } else {
                None
            };
            // The row is journaled: a release's quiescence wait need not
            // wait on the recall below (it recalls every grant itself).
            drop(admitted);
            return match result {
                Ok(records) => {
                    h.lease.touch();
                    if let Some(jseq) = jseq {
                        if let Some(outcome) = self.ack_when_durable(h, rid, op, jseq) {
                            return outcome;
                        }
                    }
                    self.recall_after_local_write(h, &records);
                    Ok(())
                }
                Err(e) => Err(mutate_fail(e)),
            };
        }
        if h.lease.is_lost() {
            return Err(MutateFail::Errno(libc::EIO));
        }
        // Plan 30 M5: everything else — forwarding with its same-rid
        // retries, the inbox when there is no P2P path, the lease path
        // with its in-doubt resolution against `completed` (and an inbox
        // refusal, which is an outcome too), the causal wait, the
        // deadline — is the authority core's client machine. One channel
        // round trip; the reply is the op's outcome or "in doubt".
        self.submit_to_core(h, op, rid, false)
    }

    /// Plan 30 §M9: the fast path's acknowledgement wait under a
    /// durability gate. `Some(outcome)` ends the op with it; `None`
    /// means acknowledged as usual (durable, or the gate went off with
    /// the lease still this node's — `Local` again). A lost lease
    /// leaves the op in doubt: resubmitted by rid through the core,
    /// which resolves it against `completed` (the stranded row is
    /// replayed by rid).
    fn ack_when_durable(
        &self,
        h: &SyncHandle,
        rid: constellation_meta::Rid,
        op: &constellation_meta::MutateOp,
        jseq: u64,
    ) -> Option<Result<(), MutateFail>> {
        let _ = h.tx.send(SyncRequest::Journaled);
        let session = self.meta.session();
        match session.wait_durable(jseq, DURABLE_WAIT_BUDGET) {
            constellation_meta::DurableWait::Durable(waited) => {
                session.count_fast_ack(waited);
                None
            }
            constellation_meta::DurableWait::Ungated => None,
            constellation_meta::DurableWait::Lost | constellation_meta::DurableWait::TimedOut => {
                session.count_fast_ack_in_doubt();
                Some(self.submit_to_core(h, op, rid, true))
            }
        }
    }

    fn submit_to_core(
        &self,
        h: &SyncHandle,
        op: &constellation_meta::MutateOp,
        rid: constellation_meta::Rid,
        in_doubt: bool,
    ) -> Result<(), MutateFail> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if h.tx
            .send(SyncRequest::Submit {
                op: op.clone(),
                rid,
                policy: constellation_authority::Policy::Client,
                in_doubt,
                reply: tx,
            })
            .is_err()
        {
            return Err(MutateFail::Errno(libc::EIO));
        }
        match rx.blocking_recv() {
            Ok(constellation_authority::ClientReply::Outcome(outcome)) => match outcome {
                constellation_meta::MutateOutcome::Accepted { .. } => Ok(()),
                // Never a client outcome: the core retries it.
                constellation_meta::MutateOutcome::Held { .. } => Err(MutateFail::Errno(libc::EIO)),
                constellation_meta::MutateOutcome::Errno(e) => Err(MutateFail::Errno(e)),
                // The name exists on the holder; the core installed the
                // entry it sent with the refusal, so the caller's next
                // lookup resolves here too.
                constellation_meta::MutateOutcome::Exists { .. } => {
                    Err(MutateFail::Errno(libc::EEXIST))
                }
                constellation_meta::MutateOutcome::Conflict { manifest } => {
                    Err(MutateFail::Conflict { manifest })
                }
                constellation_meta::MutateOutcome::Busy
                | constellation_meta::MutateOutcome::NotHolder { .. } => {
                    Err(MutateFail::Errno(libc::EIO))
                }
            },
            // Neither executed here nor answered by a holder within the
            // deadline: `EIO`, and the op stays retryable.
            Ok(constellation_authority::ClientReply::InDoubt) => Err(MutateFail::Errno(libc::EIO)),
            Err(_) => Err(MutateFail::Errno(libc::EIO)),
        }
    }

    /// fsync() barrier. In `--fsync-mode s3`, block until the journal
    /// (up to now) is durable in the shared log; otherwise just nudge.
    pub(crate) fn sync_barrier(&self, ino: Ino) -> Result<(), i32> {
        // Local durability first, in every mode: the metadata engine
        // commits to OS buffers, so fsync(2) must force them to disk.
        self.meta.sync().map_err(|e| errno(&e))?;
        let Some(h) = &self.sync else { return Ok(()) };
        if !h.fsync_s3 {
            let _ = h.tx.send(SyncRequest::Nudge);
            return Ok(());
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if h.tx
            .send(SyncRequest::Barrier {
                ino,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(libc::EIO);
        }
        match reply_rx.blocking_recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "fsync barrier: sync failed");
                Err(libc::EIO)
            }
            Err(_) => Err(libc::EIO),
        }
    }

    fn load_manifest(&self, ino: Ino) -> Result<Manifest, i32> {
        match self.meta.manifest(ino) {
            Ok(Some(bytes)) => Manifest::decode(&bytes).map_err(|_| libc::EIO),
            Ok(None) => match self.meta.scratch_manifest(ino) {
                Ok(Some(bytes)) => Manifest::decode(&bytes).map_err(|_| libc::EIO),
                Ok(None) => Ok(Manifest::empty(self.chunk_size)),
                Err(e) => Err(errno(&e)),
            },
            Err(e) => Err(errno(&e)),
        }
    }

    /// Resolve the sparse data-chunk map (following manifest spill).
    fn chunk_list(&self, m: &Manifest) -> Result<SparseChunks, i32> {
        match &m.chunks {
            ChunkInfo::Inline(v) => Ok(v.clone()),
            ChunkInfo::Spilled(h) => {
                let blob = self.fetch_chunk(h)?;
                decode_chunk_list(&blob).map_err(|_| libc::EIO)
            }
        }
    }

    /// Get one chunk: cache first, then object store (inserted clean).
    /// An in-flight prefetch for the same chunk is awaited rather than
    /// duplicated.
    fn fetch_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, i32> {
        self.fetch_chunk_for_inode(None, hash)
    }

    fn fetch_chunk_for_inode(&self, ino: Option<Ino>, hash: &ChunkHash) -> Result<Vec<u8>, i32> {
        if let Ok(Some(data)) = self.cache.get(hash) {
            return Ok(data);
        }
        if self.prefetch.claim_for_demand(hash) {
            if let Some(ino) = ino {
                self.prefetch.note_stall(ino);
                self.scan.note_stall(ino);
            }
        }
        while self.prefetch.is_inflight(hash) {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        if let Ok(Some(data)) = self.cache.get(hash) {
            return Ok(data);
        }
        if let Some(ino) = ino {
            self.prefetch.note_stall(ino);
            self.scan.note_stall(ino);
        }
        if let Some(coop) = &self.coop {
            return self.rt.block_on(coop.fetch(hash)).map_err(|error| {
                tracing::warn!(
                    hash = %hash.to_hex(),
                    error = %error,
                    "coop fetch failed"
                );
                libc::EIO
            });
        }
        let mut data = None;
        let mut last_error = None;
        for attempt in 0..3 {
            let fetched = (|| {
                let mut spill = self
                    .cache
                    .begin_spill()
                    .map_err(|e| format!("begin_spill (plain): {e}"))?;
                let result = if self.store.is_e2e() {
                    let mut cipher = self
                        .cache
                        .begin_spill()
                        .map_err(|e| format!("begin_spill (cipher): {e}"))?;
                    self.rt.block_on(self.store.get_chunk_to_writer_e2e(
                        hash,
                        &mut cipher,
                        &mut spill,
                        DecodePriority::Demand,
                    ))
                } else {
                    self.rt
                        .block_on(self.store.get_chunk_to_writer(hash, &mut spill))
                }
                .map_err(|e| format!("get_chunk_to_writer: {e}"))?;
                let _ = result;
                spill.rewind().map_err(|e| format!("spill rewind: {e}"))?;
                let mut bytes = Vec::new();
                spill
                    .read_to_end(&mut bytes)
                    .map_err(|e| format!("spill read_to_end: {e}"))?;
                let _ = self.cache.commit_spill(hash, spill, ChunkState::Clean);
                Ok::<_, String>(bytes)
            })();
            match fetched {
                Ok(bytes) => {
                    data = Some(bytes);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
            if attempt < 2 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        let data = data.ok_or_else(|| {
            tracing::warn!(
                hash = %hash.to_hex(),
                attempts = 3,
                error = last_error.as_deref().unwrap_or("unknown"),
                "direct S3 chunk fetch failed after retries"
            );
            libc::EIO
        })?;
        Ok(data)
    }

    /// Full content of committed chunk `idx`, zero-padded to `len`
    /// (cache/store fetch, or zero-fill for a hole/beyond-EOF chunk that
    /// was never written). Used to seed a staging chunk's untouched
    /// bytes before a partial (non-whole-chunk) write lands on top.
    fn committed_chunk_padded(
        &self,
        hashes: &SparseChunks,
        idx: u64,
        len: u32,
    ) -> Result<Vec<u8>, i32> {
        let mut data = match hashes.get(&idx) {
            Some(h) => self.fetch_chunk(h)?,
            None => Vec::new(),
        };
        data.resize(len as usize, 0);
        Ok(data)
    }

    /// Get-or-create the pending write state for `ino`, allocating a
    /// fresh staging file (bounded-RAM, plan 07) the first time this
    /// inode is touched since its last flush.
    fn write_state<'a>(
        &self,
        writes: &'a mut HashMap<Ino, WriteState>,
        ino: Ino,
        manifest: &Manifest,
    ) -> Result<&'a mut WriteState, i32> {
        if let std::collections::hash_map::Entry::Vacant(e) = writes.entry(ino) {
            let gen = self.staging_gen.next();
            let staging = Staging::create(&self.staging_dir, ino, gen, self.staging_budget.clone())
                .map_err(|e| staging_errno(&e))?;
            e.insert(WriteState {
                staging,
                file_len: manifest.file_len,
                base: Some(manifest.clone()),
                sealed: HashMap::new(),
                holes: crate::staging::DirtyRuns::default(),
                seal_buffer: Vec::new(),
                high_water: manifest.file_len,
                written: Vec::new(),
            });
        }
        Ok(writes.get_mut(&ino).unwrap())
    }

    fn drain_inode(&self, ino: Ino) -> Result<(), i32> {
        let Some(handle) = &self.sync else {
            return Ok(());
        };
        let (reply, receive) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(SyncRequest::DrainInode { ino, reply })
            .map_err(|_| libc::EIO)?;
        match receive.blocking_recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                tracing::warn!(%error, ino, "write-through upload failed");
                Err(libc::EIO)
            }
            Err(_) => Err(libc::EIO),
        }
    }

    /// Seal dirty full chunks below the contiguous write high-water
    /// mark. Byte-prefix continuity is used instead of borrowing the
    /// read prefetcher's signal: it directly proves a sequential
    /// writer has crossed the boundary. A later write into a sealed
    /// chunk re-admits and re-dirties that chunk.
    fn seal_crossed_chunks(&self, ino: Ino, ws: &mut WriteState) -> Result<(), i32> {
        let complete = ws.high_water / u64::from(self.chunk_size);
        let mut sealed_any = false;
        for idx in 0..complete {
            if !ws.staging.is_dirty(idx) || ws.sealed.contains_key(&idx) {
                continue;
            }
            let mut data = std::mem::take(&mut ws.seal_buffer);
            data.resize(self.chunk_size as usize, 0);
            ws.staging
                .read_at(idx * u64::from(self.chunk_size), &mut data)
                .map_err(|error| staging_errno(&error))?;
            let hash = self.store.hash(&data);
            if data.iter().all(|byte| *byte == 0) {
                ws.staging.release_chunk(idx, self.chunk_size);
                ws.holes.mark(idx);
                data.clear();
                ws.seal_buffer = data;
                sealed_any = true;
                continue;
            }
            let known_durable = self.cache.contains(&hash)
                && !self
                    .meta
                    .upload_pending_for_hash(&hash)
                    .map_err(|error| errno(&error))?;
            if !known_durable {
                self.cache
                    .insert(&hash, &data, ChunkState::Dirty)
                    .map_err(|_| libc::ENOSPC)?;
                self.meta
                    .add_pending_upload(&hash, ino)
                    .map_err(|error| errno(&error))?;
            }
            ws.staging.release_chunk(idx, self.chunk_size);
            ws.sealed.insert(idx, hash);
            data.clear();
            ws.seal_buffer = data;
            sealed_any = true;
        }
        if sealed_any {
            self.nudge_sync();
        }
        Ok(())
    }

    /// Insert content as Dirty unless the local durable-set rung proves
    /// S3 already has it. Returns whether this inode must enrol a
    /// pending row.
    fn cache_for_upload(&self, hash: &ChunkHash, data: &[u8]) -> Result<bool, i32> {
        let known_durable = self
            .cache
            .state_of(hash)
            .is_some_and(|state| matches!(state, ChunkState::Clean | ChunkState::Pinned))
            && !self
                .meta
                .upload_pending_for_hash(hash)
                .map_err(|error| errno(&error))?;
        if known_durable {
            return Ok(false);
        }
        self.cache
            .insert(hash, data, ChunkState::Dirty)
            .map_err(|_| libc::ENOSPC)?;
        Ok(true)
    }

    /// Flush an inode's pending writes: hash + upload chunks, write the
    /// new manifest. No-op when there is no pending state.
    ///
    /// The manifest commit is a namespace mutation, so it needs the
    /// lease — but the chunk uploads do not (content is immutable and
    /// content-addressed), which is why the gate sits just before the
    /// commit rather than at the top.
    ///
    /// Eager chunk upload is best-effort. The dirty cache entry is
    /// reserve-accounted and the sync task uploads all dirty chunks
    /// before publishing this manifest, so a transient reset on a
    /// healed S3 connection does not become application-visible EIO in
    /// local-fsync mode.
    fn flush_inode(&self, ino: Ino, force_through: bool) -> Result<(), i32> {
        // Keep this inode's shard locked until publication completes. That
        // preserves per-inode request ordering under fuser's concurrent
        // dispatch; unrelated inodes continue through the other shards.
        let mut writes = self.writes.lock(ino);
        let ws = match writes.remove(&ino) {
            Some(ws) => ws,
            None => return Ok(()),
        };
        let epoch_active = self.sync.as_ref().is_some_and(|h| {
            h.epoch_active
                .as_ref()
                .is_some_and(|active| active.load(std::sync::atomic::Ordering::Relaxed))
        });
        let base = match &ws.base {
            Some(m) => m.clone(),
            None => self.load_manifest(ino)?,
        };
        let (manifest_bytes, dirty_hashes) = self.compose_manifest(&ws, &base, ws.file_len)?;
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            for hash in &dirty_hashes {
                self.meta
                    .add_pending_upload(hash, ino)
                    .map_err(|e| errno(&e))?;
            }
            self.meta
                .scratch_set_manifest(ino, &manifest_bytes, ws.file_len)
                .map_err(|e| errno(&e))?;
            ws.staging.discard();
            if force_through {
                self.drain_inode(ino)?;
            }
            return Ok(());
        }
        self.finish_flush(
            ino,
            ws,
            &mut writes,
            base,
            manifest_bytes,
            dirty_hashes,
            force_through,
            epoch_active,
        )
    }

    /// Compose the whole-file manifest this flush publishes: `base`'s
    /// chunks, minus holes and anything past `file_len`, with this
    /// flush's sealed and staged chunks laid over the top. Also returns
    /// the chunks newly sealed into the cache as dirty, which the
    /// manifest commit enrols as pending uploads.
    ///
    /// Separate from [`Self::flush_inode`] so that a commit the holder
    /// rejects for a stale base can be recomposed against the manifest
    /// that *is* current, without a second copy of this logic.
    fn compose_manifest(
        &self,
        ws: &WriteState,
        base: &Manifest,
        file_len: u64,
    ) -> Result<(Vec<u8>, Vec<ChunkHash>), i32> {
        let old_hashes = self.chunk_list(base)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let n_chunks = layout.chunk_count(file_len);
        let mut new_hashes: SparseChunks = old_hashes
            .iter()
            .filter(|(index, _)| **index < n_chunks && !ws.holes.contains(**index))
            .map(|(index, hash)| (*index, *hash))
            .collect();
        // Every chunk this flush seals into the cache as Dirty: the
        // durable pending-upload set this manifest commit journals
        // (plan 07 step 1). Sealing one chunk at a time (read its
        // staged range, hash, insert, drop the buffer) keeps peak RSS
        // O(chunk_size), never O(file_len) — 07's whole point.
        let mut dirty_hashes: Vec<ChunkHash> = Vec::new();
        for (&idx, hash) in &ws.sealed {
            if idx >= n_chunks {
                continue;
            }
            if self
                .meta
                .upload_pending_for_hash(hash)
                .map_err(|error| errno(&error))?
            {
                dirty_hashes.push(*hash);
            }
            new_hashes.insert(idx, *hash);
        }
        let dirty_indices: Vec<_> = ws.staging.dirty_indices().collect();
        for idx in dirty_indices {
            if idx >= n_chunks {
                continue;
            }
            let expect_len = layout.chunk_len(file_len, idx) as usize;
            let chunk_start = idx * self.chunk_size as u64;
            let chunk_end = chunk_start + expect_len as u64;
            // Start from `base`'s committed content for this chunk —
            // not whatever `do_write` seeded the untouched neighborhood
            // with at write time, which is only as fresh as the
            // manifest at that moment — and replay just the byte
            // ranges this flush's own `write()` calls actually
            // delivered on top. Otherwise a chunk two nodes disjointly
            // patch converges on whichever writer commits last, since
            // that writer's seed silently reproduces the *other*
            // writer's bytes as zero: the earlier content it copied in
            // predates the sibling's patch landing.
            let mut data = match old_hashes.get(&idx) {
                Some(hash) => {
                    let mut fetched = self.fetch_chunk(hash)?;
                    fetched.resize(expect_len, 0);
                    fetched
                }
                None => vec![0u8; expect_len],
            };
            for &(start, end) in &ws.written {
                let start = start.max(chunk_start);
                let end = end.min(chunk_end);
                if end <= start {
                    continue;
                }
                let len = (end - start) as usize;
                let mut buf = vec![0u8; len];
                ws.staging
                    .read_at(start, &mut buf)
                    .map_err(|e| staging_errno(&e))?;
                let rel = (start - chunk_start) as usize;
                data[rel..rel + len].copy_from_slice(&buf);
            }
            if data.iter().all(|byte| *byte == 0) {
                new_hashes.remove(&idx);
            } else {
                let hash = self.store.hash(&data);
                if self.cache_for_upload(&hash, &data)? {
                    dirty_hashes.push(hash);
                }
                new_hashes.insert(idx, hash);
            }
        }
        // An untouched old tail chunk needs re-cutting after truncate-down.
        if n_chunks > 0 {
            let idx = n_chunks - 1;
            if !ws.staging.is_dirty(idx) && !ws.sealed.contains_key(&idx) && !ws.holes.contains(idx)
            {
                if let Some(h) = old_hashes.get(&idx) {
                    let expect_len = layout.chunk_len(file_len, idx) as usize;
                    let old_len = base.layout.chunk_len(base.file_len.max(1), idx) as usize;
                    if old_len != expect_len {
                        let mut data = self.fetch_chunk(h)?;
                        data.resize(expect_len, 0);
                        if data.iter().all(|byte| *byte == 0) {
                            new_hashes.remove(&idx);
                        } else {
                            let hash = self.store.hash(&data);
                            if self.cache_for_upload(&hash, &data)? {
                                dirty_hashes.push(hash);
                            }
                            new_hashes.insert(idx, hash);
                        }
                    }
                }
            }
        }
        let (manifest, spill) = Manifest::from_sparse_chunks(
            self.chunk_size,
            file_len,
            new_hashes,
            INLINE_CHUNKS_MAX,
            // Must match the identity the blob is stored under below, which
            // on an E2E mount is the keyed addressing hash, not a plain one.
            |blob| self.store.hash(blob),
        );
        if let Some(blob) = spill {
            let bh = self.store.hash(&blob);
            if self.cache_for_upload(&bh, &blob)? {
                dirty_hashes.push(bh);
            }
        }
        Ok((manifest.encode(), dirty_hashes))
    }

    /// Publish a composed manifest and retire the write state.
    #[allow(clippy::too_many_arguments)]
    fn finish_flush(
        &self,
        ino: Ino,
        ws: WriteState,
        writes: &mut HashMap<Ino, WriteState>,
        base: Manifest,
        manifest_bytes: Vec<u8>,
        dirty_hashes: Vec<ChunkHash>,
        force_through: bool,
        epoch_active: bool,
    ) -> Result<(), i32> {
        // Plan 30 §M3b: a local commit is admitted through the lease view
        // (counted in flight until the commit returns) so a release's final
        // flush cannot miss it — see `lease.rs`'s module doc, "The
        // releasing flag". The guard is dropped right after the commit:
        // the chunk drain below can take long and must not hold a release.
        let view = self.sync.as_ref().map(|handle| handle.lease.clone());
        let admitted = view.as_ref().and_then(|view| view.admit());
        let holds_lease = self.sync.is_none() || admitted.is_some();
        if holds_lease {
            let committed =
                self.commit_manifest_local(ino, &ws, base, manifest_bytes, dirty_hashes);
            drop(admitted);
            if let Err(error) = committed {
                writes.insert(ino, ws);
                return Err(error);
            }
            // Plan 30 §M8: the sequencer's own manifest commit bypasses
            // `execute_mutate`; it recalls read delegations on the file
            // before the close returns, like every other local write.
            if let Some(h) = &self.sync {
                self.recall_after_local_inos(h, vec![ino]);
            }
        } else if let Err(error) =
            self.commit_manifest_forwarded(ino, &ws, base, manifest_bytes, dirty_hashes)
        {
            writes.insert(ino, ws);
            return Err(error);
        }
        // The staged bytes now live in the durable chunk cache (and are
        // enrolled in `pending_upload`); the staging file is scratch
        // and safe to drop.
        ws.staging.discard();
        let through = self.sync.as_ref().is_none_or(|handle| {
            handle
                .write_mode
                .effective(force_through, false, handle.fsync_s3)
                == crate::writeback::WriteMode::Through
        });
        if through && !epoch_active {
            self.drain_inode(ino)?;
        }
        Ok(())
    }

    /// Commit a whole-file manifest as the lease holder, rebasing if our
    /// base turns out to be stale.
    ///
    /// A flush composes its image while it may not hold the lease; by
    /// the time an acquisition lets this commit through, a segment
    /// tailed from another node — or another flush forwarded through us
    /// while we *did* hold it — can have moved the manifest on. Without
    /// a check here, that lease thrash reproduces the disjoint-write
    /// lost update purely locally, with no forwarding involved: this
    /// node would overwrite whatever the tailed record wrote for the
    /// same inode with an image that never saw it.
    fn commit_manifest_local(
        &self,
        ino: Ino,
        ws: &WriteState,
        base: Manifest,
        manifest_bytes: Vec<u8>,
        dirty_hashes: Vec<ChunkHash>,
    ) -> Result<(), i32> {
        self.commit_manifest_with_rebase(
            ino,
            ws,
            base,
            manifest_bytes,
            dirty_hashes,
            |base, manifest_bytes, file_len, dirty_hashes| {
                self.meta
                    .set_manifest_dirty(
                        ino,
                        Some(&base.encode()),
                        manifest_bytes,
                        file_len,
                        dirty_hashes,
                    )
                    .map_err(mutate_fail)
            },
        )
    }

    /// Forward a whole-file manifest commit to the lease holder,
    /// rebasing if our base turns out to be stale.
    ///
    /// The holder rejects an image composed on a superseded manifest
    /// instead of installing it, because a whole-file image built on an
    /// old base drops whatever chunks landed in between — that is how
    /// concurrent disjoint `WriteAt`s from several nodes used to lose
    /// every patch but the last. On rejection, lay this flush's own
    /// chunks over the manifest that is current and try again.
    fn commit_manifest_forwarded(
        &self,
        ino: Ino,
        ws: &WriteState,
        base: Manifest,
        manifest_bytes: Vec<u8>,
        dirty_hashes: Vec<ChunkHash>,
    ) -> Result<(), i32> {
        self.commit_manifest_with_rebase(
            ino,
            ws,
            base,
            manifest_bytes,
            dirty_hashes,
            |base, manifest_bytes, file_len, dirty_hashes| {
                // Content-addressed chunks can be uploaded before
                // authority is obtained. Enrol them without journaling
                // the manifest, then drain, so the manifest this pass
                // forwards never names a hash S3 does not have yet —
                // including a hash a rebase just introduced by merging
                // this flush's bytes onto a fresher base, which is not
                // the same chunk the very first attempt (if any)
                // already drained.
                for hash in dirty_hashes {
                    self.meta
                        .add_pending_upload(hash, ino)
                        .map_err(mutate_fail)?;
                }
                self.drain_inode(ino).map_err(MutateFail::Errno)?;
                self.mutate_op_rebasable(
                    ino,
                    constellation_meta::MutateOp::SetManifest {
                        ino,
                        base_manifest: Some(base.encode()),
                        manifest: manifest_bytes.to_vec(),
                        size: file_len,
                    },
                )
            },
        )
    }

    /// Shared rebase-and-retry loop for a whole-file manifest commit.
    /// `attempt_commit` tries to install `manifest_bytes` (composed on
    /// `base` for length `file_len`, with `dirty_hashes` freshly sealed
    /// by this flush) and reports [`MutateFail::Conflict`] when that
    /// base has been superseded. On conflict this recomposes from the
    /// manifest that *is* current — the holder's reply, or this node's
    /// own replica when `manifest` comes back `None` because this node
    /// executed the mutation itself — and tries again, up to
    /// `MANIFEST_COMMIT_ATTEMPTS` times.
    fn commit_manifest_with_rebase(
        &self,
        ino: Ino,
        ws: &WriteState,
        mut base: Manifest,
        mut manifest_bytes: Vec<u8>,
        mut dirty_hashes: Vec<ChunkHash>,
        mut attempt_commit: impl FnMut(&Manifest, &[u8], u64, &[ChunkHash]) -> Result<(), MutateFail>,
    ) -> Result<(), i32> {
        // A flush that shortened the file relative to its own base is a
        // truncate, and must not be silently re-extended by a peer's
        // length. Any other flush adopts the longer of the two so a
        // concurrent extension survives the rebase.
        let truncates = ws.base.as_ref().is_some_and(|b| ws.file_len < b.file_len);
        let mut file_len = ws.file_len;
        for attempt in 1..=MANIFEST_COMMIT_ATTEMPTS {
            let current = match attempt_commit(&base, &manifest_bytes, file_len, &dirty_hashes) {
                Ok(()) => return Ok(()),
                Err(MutateFail::Errno(e)) => return Err(e),
                Err(MutateFail::Conflict { manifest }) => manifest,
            };
            // `None` means whichever node executed the mutation was us,
            // so our own replica already holds the authoritative image.
            base = match current {
                Some(bytes) => Manifest::decode(&bytes).map_err(|_| libc::EIO)?,
                None => self.load_manifest(ino)?,
            };
            file_len = if truncates {
                ws.file_len
            } else {
                ws.file_len.max(base.file_len)
            };
            // The chunks are already cached and enrolled for upload, so
            // recomposing only rebuilds the manifest; a rebased dirty
            // list that comes back shorter just means some are already
            // durable (already-uploaded content is not re-enrolled).
            let (rebased, rebased_dirty) = self.compose_manifest(ws, &base, file_len)?;
            manifest_bytes = rebased;
            dirty_hashes = rebased_dirty;
            tracing::debug!(
                ino,
                attempt,
                file_len,
                "manifest commit rebased onto a concurrent update"
            );
        }
        tracing::warn!(
            ino,
            attempts = MANIFEST_COMMIT_ATTEMPTS,
            "manifest commit kept losing its base; giving up"
        );
        Err(libc::EAGAIN)
    }
}

// (Filesystem impl in fusefs_ops.rs include)
include!("fusefs_ops.rs");

#[cfg(test)]
mod jitter_tests {
    use super::jitter_fraction;

    #[test]
    fn jitter_fraction_stays_in_unit_range_and_varies() {
        let samples: Vec<f64> = (0..64).map(|_| jitter_fraction()).collect();
        for &v in &samples {
            assert!((0.0..1.0).contains(&v), "{v} outside [0, 1)");
        }
        // Not a statistical test, just a guard against a constant
        // fallback silently defeating the whole point of jittering.
        assert!(
            samples.windows(2).any(|w| w[0] != w[1]),
            "jitter_fraction must not be constant: {samples:?}"
        );
    }
}

#[cfg(test)]
mod write_shard_tests {
    use super::*;

    #[test]
    fn same_inode_serializes_while_unrelated_inode_remains_available() {
        let writes = WriteShards::new();
        let _same_inode_guard = writes.lock(1);

        assert!(matches!(
            writes.0[1].try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert!(writes.0[2].try_lock().is_ok());
    }
}

#[cfg(test)]
mod quota_tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_fs_core::DEFAULT_CHUNK_SIZE;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    /// Plan 30 §M3b: `publish_now` refuses while the journal is non-empty
    /// (`SPECULATION_OUTSTANDING`) so it never publishes speculation.
    /// This test's bare `Meta` has no shipper acking it, so simulate one
    /// ship of everything journaled so far under `segment`, exactly as
    /// production does when a segment lands.
    fn ship_all(meta: &Meta, segment: u64) {
        let rows = meta.take_journal(usize::MAX).unwrap();
        let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
        meta.ack_journal_rows_at(&seqs, segment).unwrap();
    }

    fn test_fs(meta: Arc<Meta>) -> (ConstellationFs, TempDir) {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 1 << 30).unwrap());
        let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
        let snapshots = Arc::new(crate::snapshot::SnapshotManager::new(
            meta.clone(),
            store.clone(),
            DEFAULT_CHUNK_SIZE,
            1,
        ));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.handle().clone();
        let _enter = handle.enter();
        let fs = ConstellationFs::new(
            FsDependencies {
                meta,
                store,
                cache,
                rt: handle,
                sync: None,
                coop: None,
                staging_dir: dir.path().join("staging"),
                staging_budget: StagingBudget::new(1 << 30),
                snapshots,
                atime: Arc::new(crate::atime::AtimeAccumulator::new(
                    crate::atime::AtimeMode::Off,
                    crate::atime::AtimeStats::new(),
                )),
                prune_stats: crate::prune::PruneStats::new(),
            },
            DEFAULT_CHUNK_SIZE,
            CompressionSetting::RAW,
        );
        std::mem::forget(rt);
        (fs, dir)
    }

    #[test]
    fn quota_check_unlimited_always_ok() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let (fs, _tmpdir) = test_fs(meta);
        assert!(fs.quota_check(f.ino, 1 << 40).is_ok());
    }

    #[test]
    fn quota_check_under_cap_ok_over_cap_enospc() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_quota(Some(100)).unwrap();
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        meta.setattr(f.ino, None, None, None, Some(40), None, None)
            .unwrap();
        let (fs, _tmpdir) = test_fs(meta);
        ConstellationFs::invalidate_quota_cache(&fs.quota_cache);
        assert!(fs.quota_check(f.ino, 90).is_ok());
        assert!(fs.quota_check(f.ino, 100).is_ok());
        assert_eq!(fs.quota_check(f.ino, 101).unwrap_err(), libc::ENOSPC);
        // Shrinking never grows past the cap: pending growth is zero once
        // the intended length is at or below the committed size.
        assert!(fs.quota_check(f.ino, 10).is_ok());
    }

    /// The committed length is `inode.size`, not the manifest's: after a
    /// sparse `ftruncate` the manifest still reads 0, and charging growth
    /// against it would bill the same bytes twice.
    #[test]
    fn quota_check_does_not_double_count_a_sparse_truncate() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_quota(Some(100)).unwrap();
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        // `truncate -s 60` with no manifest committed yet.
        meta.setattr(f.ino, None, None, None, Some(60), None, None)
            .unwrap();
        assert_eq!(meta.usage(), (60, 1));
        assert!(meta.manifest(f.ino).unwrap().is_none());
        let (fs, _tmpdir) = test_fs(meta);
        ConstellationFs::invalidate_quota_cache(&fs.quota_cache);
        // Writing inside the truncated length adds nothing to the total.
        assert!(fs.quota_check(f.ino, 60).is_ok());
        // Growing to 100 fits exactly; 101 does not.
        assert!(fs.quota_check(f.ino, 100).is_ok());
        assert_eq!(fs.quota_check(f.ino, 101).unwrap_err(), libc::ENOSPC);
    }

    #[test]
    fn usage_counter_tracks_create_setattr_unlink() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        assert_eq!(meta.usage(), (0, 0));
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        assert_eq!(meta.usage(), (0, 1));
        meta.setattr(f.ino, None, None, None, Some(50), None, None)
            .unwrap();
        assert_eq!(meta.usage(), (50, 1));
        meta.set_manifest(f.ino, b"m", 80).unwrap();
        assert_eq!(meta.usage(), (80, 1));
        meta.unlink(ROOT_INO, "f").unwrap();
        assert_eq!(meta.usage(), (0, 0));
        let recomputed = meta.recursive_size(ROOT_INO).unwrap();
        assert_eq!(meta.usage(), recomputed);
    }

    /// A rename that replaces an existing file drops that file from the
    /// reachable set, so the counter has to shed it (replay's
    /// `evict_dentry` does the same on every peer).
    #[test]
    fn usage_counter_tracks_replacing_rename() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let a = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        meta.setattr(a.ino, None, None, None, Some(100), None, None)
            .unwrap();
        meta.setattr(b.ino, None, None, None, Some(7), None, None)
            .unwrap();
        assert_eq!(meta.usage(), (107, 2));
        meta.rename(ROOT_INO, "b", ROOT_INO, "a").unwrap();
        assert_eq!(meta.usage(), (7, 1));
        assert_eq!(meta.usage(), meta.recursive_size(ROOT_INO).unwrap());

        // A rename onto a still-linked target only unlinks one name.
        let c = meta.create(ROOT_INO, "c", 0o644, 0, 0).unwrap();
        meta.setattr(c.ino, None, None, None, Some(9), None, None)
            .unwrap();
        meta.link(c.ino, ROOT_INO, "c2").unwrap();
        let d = meta.create(ROOT_INO, "d", 0o644, 0, 0).unwrap();
        assert_eq!(meta.usage(), (16, 3));
        meta.rename(ROOT_INO, "d", ROOT_INO, "c").unwrap();
        assert_eq!(meta.usage(), (16, 3));
        assert_eq!(meta.usage(), meta.recursive_size(ROOT_INO).unwrap());
        let _ = d;
    }

    #[test]
    fn set_quota_round_trip_and_replay() {
        let src = Arc::new(Meta::open_in_memory().unwrap());
        src.set_quota(Some(1234)).unwrap();
        assert_eq!(src.quota().unwrap(), Some(1234));
        src.set_quota(None).unwrap();
        assert_eq!(src.quota().unwrap(), None);

        let src2 = Arc::new(Meta::open_in_memory().unwrap());
        src2.set_quota(Some(999)).unwrap();
        let records: Vec<_> = src2
            .take_journal(100)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        let dst = Arc::new(Meta::open_in_memory().unwrap());
        dst.apply_records(&records).unwrap();
        assert_eq!(dst.quota().unwrap(), Some(999));
    }

    #[test]
    fn statfs_reports_view_usage_against_whole_fs_headroom() {
        let block = 131072u64;
        // Whole-filesystem mount under a cap: total collapses to the cap.
        let (total, free) = statfs_blocks(40 * block, 40 * block, Some(100 * block), block);
        assert_eq!((total, free), (100, 60));
        assert_eq!(total - free, 40, "used is the view's own bytes");

        // Subtree mount holding 10 blocks of a filesystem using 40: used
        // scopes to the view, free still reflects the cluster-wide cap.
        let (total, free) = statfs_blocks(10 * block, 40 * block, Some(100 * block), block);
        assert_eq!((total - free, free), (10, 60));

        // Overshooting the cap reports full rather than negative free.
        let (total, free) = statfs_blocks(120 * block, 120 * block, Some(100 * block), block);
        assert_eq!((total, free), (120, 0));

        // Uncapped: effectively unbounded free space, exact used.
        let huge = u64::MAX / block / 2;
        let (total, free) = statfs_blocks(7 * block, 7 * block, None, block);
        assert_eq!((total, free), (huge + 7, huge));

        // Partial blocks round used up and free down.
        let (total, free) = statfs_blocks(1, 1, Some(2 * block), block);
        assert_eq!((total - free, free), (1, 1));
    }

    #[test]
    fn parse_statfs_ttl_defaults_to_five_seconds() {
        assert_eq!(parse_statfs_ttl_secs(None), Duration::from_secs(5));
        assert_eq!(parse_statfs_ttl_secs(Some("")), Duration::from_secs(5));
        assert_eq!(parse_statfs_ttl_secs(Some("bogus")), Duration::from_secs(5));
        assert_eq!(parse_statfs_ttl_secs(Some("0")), Duration::from_secs(0));
        assert_eq!(parse_statfs_ttl_secs(Some("12")), Duration::from_secs(12));
    }

    /// A whole-filesystem mount answers from the maintained counter, so it
    /// is exact and never serves a stale aggregate.
    #[test]
    fn view_usage_of_a_full_mount_tracks_the_counter() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let a = meta.create(dir.ino, "a", 0o644, 0, 0).unwrap();
        let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        meta.setattr(a.ino, None, None, None, Some(7), None, None)
            .unwrap();
        meta.setattr(b.ino, None, None, None, Some(100), None, None)
            .unwrap();

        let (fs, _tmpdir) = test_fs(meta.clone());
        assert_eq!(fs.view_usage(), (107, 2));
        meta.setattr(b.ino, None, None, None, Some(1), None, None)
            .unwrap();
        assert_eq!(fs.view_usage(), (8, 2), "no TTL between a write and df");
    }

    #[test]
    fn view_usage_scopes_to_subtree_mount() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let a = meta.create(dir.ino, "a", 0o644, 0, 0).unwrap();
        let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        meta.setattr(a.ino, None, None, None, Some(7), None, None)
            .unwrap();
        meta.setattr(b.ino, None, None, None, Some(100), None, None)
            .unwrap();

        let (mut fs, _tmpdir) = test_fs(meta);
        assert_eq!(fs.view_usage(), (107, 2));
        fs.set_subtree_root("/d").unwrap();
        assert_eq!(fs.view_usage(), (7, 1));
    }

    #[test]
    fn zero_ttl_disables_cache_while_positive_ttl_caches() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let f = meta.create(dir.ino, "f", 0o644, 0, 0).unwrap();
        meta.setattr(f.ino, None, None, None, Some(10), None, None)
            .unwrap();

        // Only a scoped mount pays for (and caches) the recursive walk.
        let (mut fs, _tmpdir) = test_fs(meta.clone());
        fs.set_subtree_root("/d").unwrap();
        fs.statfs_ttl = Duration::from_secs(60);
        assert_eq!(fs.view_usage(), (10, 1));
        meta.setattr(f.ino, None, None, None, Some(99), None, None)
            .unwrap();
        assert_eq!(
            fs.view_usage(),
            (10, 1),
            "positive TTL must serve the stale aggregate"
        );

        fs.statfs_ttl = Duration::from_secs(0);
        *fs.usage_cache.lock().unwrap() = None;
        assert_eq!(fs.view_usage(), (99, 1));
        meta.setattr(f.ino, None, None, None, Some(1), None, None)
            .unwrap();
        assert_eq!(
            fs.view_usage(),
            (1, 1),
            "TTL 0 must recompute on every call"
        );
    }

    #[test]
    fn view_usage_scopes_to_snapshot_mount() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let source = meta.mkdir(ROOT_INO, "source", 0o755, 0, 0).unwrap();
        let file = meta.create(source.ino, "file", 0o644, 0, 0).unwrap();
        meta.setattr(file.ino, None, None, None, Some(42), None, None)
            .unwrap();
        let outside = meta.create(ROOT_INO, "outside", 0o644, 0, 0).unwrap();
        meta.setattr(outside.ino, None, None, None, Some(1000), None, None)
            .unwrap();

        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 1 << 30).unwrap());
        let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
        let (manager, _nodes) =
            crate::snapshot::test_manager(meta.clone(), store.clone(), DEFAULT_CHUNK_SIZE);
        let snapshots = Arc::new(manager);
        ship_all(&meta, 1);
        // Create the snapshot on a throwaway runtime so the FUSE handle's
        // runtime is idle when view_usage later block_on's tree loads.
        {
            let setup = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            setup.block_on(snapshots.create("/source", "snap")).unwrap();
        }

        let fs_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut fs = ConstellationFs::new(
            FsDependencies {
                meta,
                store,
                cache,
                rt: fs_rt.handle().clone(),
                sync: None,
                coop: None,
                staging_dir: dir.path().join("staging"),
                staging_budget: StagingBudget::new(1 << 30),
                snapshots,
                atime: Arc::new(crate::atime::AtimeAccumulator::new(
                    crate::atime::AtimeMode::Off,
                    crate::atime::AtimeStats::new(),
                )),
                prune_stats: crate::prune::PruneStats::new(),
            },
            DEFAULT_CHUNK_SIZE,
            CompressionSetting::RAW,
        );
        std::mem::forget(fs_rt);
        fs.statfs_ttl = Duration::from_secs(0);
        assert_eq!(fs.view_usage(), (1042, 2));
        fs.set_snapshot_root("/source", "snap").unwrap();
        assert_eq!(fs.view_usage(), (42, 1));
    }
}
