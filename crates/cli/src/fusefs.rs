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
use constellation_meta::{MetaError, MetaStore, SqliteMeta};
use constellation_store_s3::{ChunkStore, CompressionSetting};
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);

/// In-flight write state for one inode: bytes live on disk in `staging`
/// (bounded RAM regardless of file size, plan 05a), not in a `Vec` per
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

/// Outcome of a successful partition handoff (flush + lease release).
#[derive(Debug, Clone)]
pub struct HandoffResult {
    pub epoch: u64,
    pub etag: Option<String>,
    pub head_seq: Option<u64>,
}

/// A request to the daemon's sync task.
pub enum SyncRequest {
    /// Run a sync round soon; the sender does not wait.
    Nudge,
    /// Run a sync round and report its outcome (fsync barrier).
    Barrier {
        ino: Ino,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    DrainInode {
        ino: Ino,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Take the lease for `part` if it is free. `Ok(false)` means a live
    /// foreign holder still owns it.
    Acquire {
        part: String,
        reply: tokio::sync::oneshot::Sender<Result<bool, String>>,
    },
    /// A peer asked us to hand `part`'s lease over (M3.3 fast path):
    /// flush that partition's journal to S3 and release the lease.
    /// Replies with the epoch, etag, and last shipped seq we held, or
    /// `None` if we do not hold it or the flush failed — in which case
    /// the requester falls back to waiting the lease out through S3,
    /// which is always correct.
    HandOff {
        part: String,
        reply: tokio::sync::oneshot::Sender<Option<HandoffResult>>,
    },
    Mutate {
        part: String,
        requester: u64,
        op: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<constellation_meta::MutateOutcome>,
    },
    Forward {
        part: String,
        op: constellation_meta::MutateOp,
        reply: tokio::sync::oneshot::Sender<Result<constellation_meta::MutateOutcome, String>>,
    },
    ApplyPushed {
        part: String,
        seq: u64,
        epoch: u64,
        holder_node: u64,
        payload: Vec<u8>,
    },
    ClaimOffer {
        part: String,
        epoch: u64,
    },
    /// Reintegrate a stranded branch, either from the control API or
    /// automatically after mounting a persisted deposed state dir.
    Reintegrate(tokio::sync::oneshot::Sender<Result<String, String>>),
    /// Permanently leave the cluster (self). Flushes, tombstones the
    /// registry record, marks the state dir spent, then the caller
    /// unmounts.
    Leave {
        force: bool,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
}

/// FUSE-side handle to the metadata sync task.
pub struct SyncHandle {
    pub tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    /// `--fsync-mode s3`: fsync() returns only once the journal is in S3.
    pub fsync_s3: bool,
    /// Lock-free lease state keyed by partition; the write gate reads
    /// the relevant view per mutating op.
    pub leases:
        Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<crate::lease::LeaseView>>>>,
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
}

/// Everything [`ConstellationFs::new`] needs besides the two filesystem
/// format knobs (`chunk_size`, `compression`). Grouped so a new
/// dependency cannot be silently swapped with a neighbour of the same type.
pub struct FsDependencies {
    pub meta: Arc<SqliteMeta>,
    pub store: Arc<ChunkStore>,
    pub cache: Arc<DiskCache>,
    pub rt: Handle,
    pub sync: Option<SyncHandle>,
    pub coop: Option<Arc<crate::coop::Coop>>,
    /// Root of `<state_dir>/staging`; write staging files live here
    /// (plan 05a). Callers GC this directory before mounting.
    pub staging_dir: PathBuf,
    /// Shared bound on in-flight (unflushed) write bytes across every
    /// open dirty inode, decoupled from the chunk cache budget: a
    /// staged write is not sealed into the cache until flush.
    pub staging_budget: Arc<StagingBudget>,
    pub snapshots: Arc<crate::snapshot::SnapshotManager>,
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
        object: Option<ChunkHash>,
        xattrs: Vec<(String, Vec<u8>)>,
    },
}

struct SyntheticRegistry {
    nodes: HashMap<Ino, SyntheticNode>,
    keys: HashMap<String, Ino>,
    next: Ino,
}

pub struct ConstellationFs {
    meta: Arc<SqliteMeta>,
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
        HashMap<ChunkHash, constellation_fs_core::Tree>,
        VecDeque<ChunkHash>,
    )>,
    view_root: Ino,
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
        MetaError::Sqlite(_) | MetaError::Json(_) | MetaError::Postcard(_) => libc::EIO,
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
        }
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
            object: Some(crate::snapshot::parse_hash(&row.root_hash)?),
            xattrs: Vec::new(),
        };
        self.view_root = self.intern_synthetic(format!("mount:{path}@{name}"), node);
        Ok(())
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
                                object: entry.manifest_or_tree_hash,
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
        hash: ChunkHash,
    ) -> Result<constellation_fs_core::Tree, i32> {
        {
            let cache = self.tree_cache.lock().unwrap();
            if let Some(tree) = cache.0.get(&hash) {
                return Ok(tree.clone());
            }
        }
        let tree = self
            .rt
            .block_on(self.snapshots.load_tree(hash))
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
                        object: entry.manifest_or_tree_hash,
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
                            object: entry.manifest_or_tree_hash,
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
            .block_on(self.snapshots.load_manifest(manifest_hash))
            .map_err(|_| libc::EIO)?;
        let hashes = self.chunk_list(&manifest)?;
        if offset >= manifest.file_len {
            return Ok(Vec::new());
        }
        let len = size.min(manifest.file_len - offset);
        let mut out = Vec::with_capacity(len as usize);
        for slice in manifest.layout.slices(offset, len) {
            let chunk = self.read_committed_chunk(ino, &hashes, slice.index)?;
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
        {
            let part = self.meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
            let map = h.leases.lock().unwrap();
            if let Some(view) = map.get(&part) {
                if view.usable() {
                    view.touch();
                    return Ok(());
                }
            }
            // The sync task performs a P2P-only handoff in epoch mode.
        }
        let part = self.meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
        {
            let map = h.leases.lock().unwrap();
            if let Some(view) = map.get(&part) {
                if view.usable() {
                    view.touch();
                    return Ok(());
                }
                if view.is_lost() {
                    tracing::error!(
                        part,
                        "refusing mutation: this node lost the partition lease"
                    );
                    return Err(libc::EIO);
                }
            }
        }
        let start = std::time::Instant::now();
        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if h.tx
                .send(SyncRequest::Acquire {
                    part: part.clone(),
                    reply: tx,
                })
                .is_err()
            {
                return Err(libc::EIO);
            }
            match rx.blocking_recv() {
                Ok(Ok(true)) => {
                    if let Some(view) = h.leases.lock().unwrap().get(&part) {
                        view.touch();
                    }
                    return Ok(());
                }
                Ok(Ok(false)) => {}
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "lease acquisition failed");
                    return Err(libc::EIO);
                }
                Err(_) => return Err(libc::EIO),
            }
            if start.elapsed() >= h.acquire_deadline {
                tracing::error!(
                    waited = ?start.elapsed(),
                    part,
                    "another node holds the partition lease; failing the write with EIO"
                );
                return Err(libc::EIO);
            }
            std::thread::sleep(Duration::from_millis(100));
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
    pub(crate) fn mutate_op_rebasable(
        &self,
        part_hint_ino: Ino,
        op: constellation_meta::MutateOp,
    ) -> Result<(), MutateFail> {
        if Self::is_synthetic(part_hint_ino) {
            return Err(MutateFail::Errno(libc::EROFS));
        }
        let Some(h) = &self.sync else {
            return constellation_meta::execute_mutate(&self.meta, &op)
                .map(|_| ())
                .map_err(mutate_fail);
        };
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
                    return constellation_meta::execute_mutate(&self.meta, &op)
                        .map(|_| ())
                        .map_err(mutate_fail);
                }
                crate::designation::GateDecision::ReadOnly { .. } => {
                    return Err(MutateFail::Errno(libc::EROFS));
                }
            }
        }
        let part = self
            .meta
            .partition_of(part_hint_ino)
            .unwrap_or_else(|_| "p0".into());
        if let Some(view) = h.leases.lock().unwrap().get(&part) {
            if view.usable() {
                let result = constellation_meta::execute_mutate(&self.meta, &op)
                    .map(|_| ())
                    .map_err(mutate_fail);
                if result.is_ok() {
                    view.touch();
                }
                return result;
            }
            if view.is_lost() {
                return Err(MutateFail::Errno(libc::EIO));
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        if crate::forward::forwarding_enabled()
            && h.tx
                .send(SyncRequest::Forward {
                    part: part.clone(),
                    op: op.clone(),
                    reply: tx,
                })
                .is_ok()
        {
            match rx.blocking_recv() {
                Ok(Ok(constellation_meta::MutateOutcome::Accepted { .. })) => return Ok(()),
                Ok(Ok(constellation_meta::MutateOutcome::Errno(e))) => {
                    return Err(MutateFail::Errno(e))
                }
                Ok(Ok(constellation_meta::MutateOutcome::Conflict { manifest })) => {
                    return Err(MutateFail::Conflict { manifest })
                }
                Ok(Ok(
                    constellation_meta::MutateOutcome::Busy
                    | constellation_meta::MutateOutcome::NotHolder { .. },
                )) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%error, part, "forwarded mutation failed; acquiring lease");
                }
                Err(_) => return Err(MutateFail::Errno(libc::EIO)),
            }
        }
        self.require_lease_for(part_hint_ino)
            .map_err(MutateFail::Errno)?;
        constellation_meta::execute_mutate(&self.meta, &op)
            .map(|_| ())
            .map_err(mutate_fail)
    }

    /// fsync() barrier. In `--fsync-mode s3`, block until the journal
    /// (up to now) is durable in the shared log; otherwise just nudge.
    pub(crate) fn sync_barrier(&self, ino: Ino) -> Result<(), i32> {
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
            return self.rt.block_on(coop.fetch(hash)).map_err(|_| libc::EIO);
        }
        let mut data = None;
        for attempt in 0..3 {
            let fetched = (|| {
                let mut spill = self.cache.begin_spill().map_err(|_| ())?;
                let result = if self.store.is_e2e() {
                    let mut cipher = self.cache.begin_spill().map_err(|_| ())?;
                    self.rt.block_on(self.store.get_chunk_to_writer_e2e(
                        hash,
                        &mut cipher,
                        &mut spill,
                    ))
                } else {
                    self.rt
                        .block_on(self.store.get_chunk_to_writer(hash, &mut spill))
                }
                .map_err(|_| ())?;
                let _ = result;
                spill.rewind().map_err(|_| ())?;
                let mut bytes = Vec::new();
                spill.read_to_end(&mut bytes).map_err(|_| ())?;
                let _ = self.cache.commit_spill(hash, spill, ChunkState::Clean);
                Ok::<_, ()>(bytes)
            })();
            if let Ok(bytes) = fetched {
                data = Some(bytes);
                break;
            }
            if attempt < 2 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        let data = data.ok_or(libc::EIO)?;
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
    /// fresh staging file (bounded-RAM, plan 05a) the first time this
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
        // (plan 05a step 1). Sealing one chunk at a time (read its
        // staged range, hash, insert, drop the buffer) keeps peak RSS
        // O(chunk_size), never O(file_len) — 05a's whole point.
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
        let holds_lease = self.sync.as_ref().is_none_or(|handle| {
            let part = self.meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
            handle
                .leases
                .lock()
                .unwrap()
                .get(&part)
                .is_some_and(|view| view.usable())
        });
        if holds_lease {
            if let Err(error) =
                self.commit_manifest_local(ino, &ws, base, manifest_bytes, dirty_hashes)
            {
                writes.insert(ino, ws);
                return Err(error);
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
                    .set_manifest_dirty(ino, Some(&base.encode()), manifest_bytes, file_len, dirty_hashes)
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
                    self.meta.add_pending_upload(hash, ino).map_err(mutate_fail)?;
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
