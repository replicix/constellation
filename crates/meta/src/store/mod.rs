//! fjall 3 metadata engine (plan 29 M1).
//!
//! Replaces the SQLite engine (`meta::sqlite`, removed) with
//! `fjall::SingleWriterTxDatabase`: one write transaction at a time
//! (`db.write_tx()`, serialized by fjall's own single-writer lock) gives
//! the same "namespace change + journal row in one transaction"
//! guarantee the SQLite writer mutex gave; readers take a lock-free
//! `db.read_tx()` snapshot (`fjall::Snapshot`, MVCC).
//!
//! Keyspaces (see `docs/plans/v1/wip/29-fjall-metadata-engine.md` and
//! plan 28 §P5/§P6):
//!
//! - `ns` — the replicated namespace, in *exactly* plan 28 §P6's key
//!   encoding (`constellation_mtree::keys`/`record`): `0x01` inode,
//!   `0x02` dentry, `0x03` spilled xattr, `0x04` reverse dentry, `0x30`
//!   subsystem (snapshots, quota). Only inodes with `nlink > 0` live
//!   here — an unlinked-but-open inode moves to `orphans` in the same
//!   transaction. `ns`'s key set is exactly the published tree's (M2
//!   makes publishing a delta of this keyspace); values are always
//!   `Payload::Inline` or `Payload::Spilled` to `blobs` (never to S3 —
//!   that only happens at publish).
//! - `atime` — `ino -> atime_ns` (i64 LE). Node-local: §P6 excludes
//!   atime from the tree entirely, and `constellation_mtree::record::Attrs`
//!   has no atime field, so unlike the old SQLite `inode.atime_ns`
//!   column this is a value-side overlay applied at read time by every
//!   method that returns a `FileAttr`.
//! - `orphans` — `ino -> InodeRecord` for `nlink == 0` inodes some file
//!   descriptor still has open. Xattrs are cleared before an inode
//!   moves here (mirroring the old behaviour), so no dentry/xattr keys
//!   are needed for an orphan. `reap_orphan` deletes the row (and its
//!   `atime` entry) once the kernel's last close arrives.
//! - `journal` — `seq: u64 BE -> postcard(LogRecord)`. `seq` is a
//!   persisted counter in `local` (`next_journal_seq`), incremented in
//!   the same write transaction as the journal insert — the
//!   replacement for SQLite's `AUTOINCREMENT` (monotonic, never reused
//!   even across deletes, since the counter itself is never rolled
//!   back).
//! - `atime_journal` — `ino: u64 BE -> postcard(atime_ns, time_ns)`,
//!   the not-yet-shipped atime outbox (distinct from `atime`, the live
//!   merged value). Plan 29 M0a removed namespace partitions, so unlike
//!   the old schema this has no `part` column — every row belongs to
//!   the one implicit partition.
//! - `local` — node-local string-keyed settings and counters:
//!   `node_prefix`, `next_ino`, `applied_seq`, `next_journal_seq`,
//!   `next_shadow_id`, `usage_bytes`/`usage_files` (the persisted
//!   mirror of `UsageTracker`, updated in the same transaction as the
//!   change that moves them — replacing the old O(namespace)
//!   `recursive_size(ROOT)` seed at open), quota's node-local mirror,
//!   `left`/`lease_lost`/`read_only_member`, and the mtree publisher's
//!   own bookkeeping keys (`cli::mtree_publish`).
//! - `pending_upload` — `hash(32) ++ ino(8 BE) -> ()`.
//! - `chunk_ref` / `chunk_ref_by_ino` — `hash(32) ++ ino(8 BE) -> ()`
//!   and its by-ino mirror `ino(8 BE) ++ hash(32) -> ()`, maintained in
//!   the same transaction as every manifest change.
//! - `xattr_by_name` — `name ++ 0x00 ++ ino(8 BE) -> value`, a
//!   redundant-but-simple index for prune-policy root discovery
//!   (`constellation_meta::prune`) over the *shared* namespace's
//!   xattrs (`ns`'s xattrs are ino-major, never name-major).
//! - `scratch` — a scratch directory's private content, same
//!   `constellation_mtree` key/value encoding as `ns` (so scratch → shared
//!   publish is a record copy), but never journaled or replicated.
//! - `pins`, `epochs`, `reintegration`, `shadow` — node-local, one
//!   reasonable key layout each (see their modules).
//! - `blobs` — `blake3(bytes) -> bytes`, the local content-addressed
//!   store `Payload::Spilled` values resolve against. §P6's spill rule
//!   (`VALUE_SPILL = 1024 B`) is applied locally exactly as it will be
//!   at publish time, but the spilled body is kept here (an ordinary
//!   fjall keyspace, unbounded value size) rather than pushed to a
//!   bucket blob — that upload only happens when M2's publisher spills
//!   a key it is about to ship. This is what lets an inline `Payload`
//!   never need to hold more than `VALUE_SPILL` bytes, so its `u16`
//!   length prefix (debug-assert-only guarded against a 64 KiB
//!   overflow in `constellation_mtree::record::Payload`) is never at risk
//!   locally either, for xattrs up to Linux's 64 KiB limit or for
//!   arbitrarily large manifests.

pub(crate) mod atime;
mod bootstrap;
pub(crate) mod journal;
pub(crate) mod misc;
pub(crate) mod ns;
mod reads;
mod scratch;
pub(crate) mod snapshot;
mod writes;

pub use bootstrap::BootstrapIndexBuilder;
pub use snapshot::{quota_record, snapshot_record};

use crate::error::MetaError;
use constellation_fs_core::types::{now_ns, ROOT_INO};
use constellation_mtree::keys;
use constellation_mtree::record::{Attrs, InodeRecord, Kind};
use fjall::config::{HashRatioPolicy, PinningPolicy};
use fjall::{
    KeyspaceCreateOptions, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
    SingleWriterWriteTx, Snapshot,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};

/// Inos are `node_prefix << 40 | counter`: 24 bits of node id, 40 bits
/// (~1.1e12) of per-node allocations. Prefix 0 belongs to `fs create`
/// genesis (the root inode is 1).
pub const INO_PREFIX_SHIFT: u32 = 40;

/// Size of the ino block a directory's children are drawn from (plan 28
/// §S1b). `alloc_ino_tx` gives every directory a lazily-allocated,
/// contiguous span of `INO_BLOCK_SIZE` counter values: the first child
/// created under a directory reserves a fresh block from the node's
/// global counter (`KV_NEXT_INO`), and subsequent children in that same
/// directory draw the next unused number in that block instead of
/// advancing the global counter — so a directory's children cluster in
/// one `0x01`-range span instead of scattering across the whole
/// per-node ino range as the filesystem ages (measured at ~7× write
/// amplification in §14.10 under the old global `counter++` policy).
///
/// 1024 matches §14.10's own "distinct 1024-ino buckets" statistic, so
/// a directory that stays under one block also stays in one bucket by
/// that measure. **Overflow rule:** once a directory's active block is
/// full (all `INO_BLOCK_SIZE` numbers handed out), the next child for
/// that directory reserves a brand-new block from the global counter —
/// there is no cross-directory reuse and no bound on how many blocks one
/// directory can accumulate, so a directory with 100k children simply
/// owns ~100 blocks, each internally clustered. This is pure allocation
/// *policy*: an ino never moves once assigned, nothing migrates when a
/// block fills, and existing (pre-locality) inos are unaffected.
pub const INO_BLOCK_SIZE: u64 = 1024;
pub const SCRATCH_XATTR: &str = "user.constellation.scratch";
pub const QUOTA_KV_KEY: &str = "quota_max_bytes";
/// Node-local mirror of `meta.json`'s creation-time cap. Never journaled:
/// every node re-derives it from `meta.json` at mount, so it needs no
/// replication and a node that has not yet tailed a live `SetQuota` still
/// enforces the cap the filesystem was created with.
pub const QUOTA_CREATION_KV_KEY: &str = "quota_creation_bytes";

pub(crate) const KV_NODE_PREFIX: &str = "node_prefix";
pub(crate) const KV_NEXT_INO: &str = "next_ino";
pub(crate) const KV_APPLIED_SEQ: &str = "applied_seq";
pub(crate) const KV_NEXT_JOURNAL_SEQ: &str = "next_journal_seq";
pub(crate) const KV_NEXT_SHADOW_ID: &str = "next_shadow_id";
pub(crate) const KV_USAGE_BYTES: &str = "usage_bytes";
pub(crate) const KV_USAGE_FILES: &str = "usage_files";
/// Monotonic counter behind the `dirty` keyspace (plan 29 M2): every key
/// written to `ns` records the counter value it was touched at, so
/// `clear_dirty_upto` can tell "still dirty at the counter a publish
/// observed" from "re-dirtied since".
pub(crate) const KV_NEXT_DIRTY_SEQ: &str = "next_dirty_seq";

pub type JournalBatch = Vec<(u64, crate::record::LogRecord)>;
pub type EpochRow = (
    String,
    Vec<u64>,
    std::collections::BTreeMap<String, u64>,
    i64,
    String,
);

/// Whole-FS logical usage: sum of reachable file `inode.size` values and
/// file count. Persisted in `local` (`usage_bytes`/`usage_files`),
/// updated inside the same write transaction as the change that moves
/// them, and mirrored here as an in-memory atomic pair for O(1) reads.
pub struct UsageTracker {
    bytes: AtomicI64,
    files: AtomicI64,
}

impl UsageTracker {
    fn new(bytes: u64, files: u64) -> Self {
        Self {
            bytes: AtomicI64::new(bytes as i64),
            files: AtomicI64::new(files as i64),
        }
    }

    pub fn load(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed).max(0) as u64,
            self.files.load(Ordering::Relaxed).max(0) as u64,
        )
    }

    pub fn adjust(&self, d_bytes: i64, d_files: i64) {
        if d_bytes != 0 {
            self.bytes.fetch_add(d_bytes, Ordering::Relaxed);
        }
        if d_files != 0 {
            self.files.fetch_add(d_files, Ordering::Relaxed);
        }
    }

    pub fn reseat(&self, bytes: u64, files: u64) {
        self.bytes.store(bytes as i64, Ordering::Relaxed);
        self.files.store(files as i64, Ordering::Relaxed);
    }

    /// A zeroed accumulator. Replay stages its deltas in one of these and
    /// only folds them into the live counter once its transaction commits,
    /// so a rolled-back batch cannot leave the counter shifted.
    pub(crate) fn staging() -> Self {
        Self::new(0, 0)
    }

    /// Fold staged deltas into `live`, resetting this accumulator.
    pub(crate) fn drain_into(&self, live: &UsageTracker) {
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let files = self.files.swap(0, Ordering::Relaxed);
        live.adjust(bytes, files);
    }

    /// The accumulated signed delta, unclamped (unlike [`Self::load`],
    /// which reports an absolute value and clamps negative to zero) —
    /// what a caller needs to persist a staging accumulator's net effect.
    pub(crate) fn raw_delta(&self) -> (i64, i64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.files.load(Ordering::Relaxed),
        )
    }
}

/// Fold `(d_bytes, d_files)` into the persisted `usage_bytes`/`usage_files`
/// counters in `local`, inside the caller's write transaction — the
/// durable half of every usage change; the in-memory [`UsageTracker`] is
/// updated separately, only after the transaction actually commits.
pub(crate) fn adjust_usage_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    d_bytes: i64,
    d_files: i64,
) -> Result<(), MetaError> {
    if d_bytes == 0 && d_files == 0 {
        return Ok(());
    }
    let bytes = kv_get_i64(tx, local, KV_USAGE_BYTES)?.unwrap_or(0) + d_bytes;
    let files = kv_get_i64(tx, local, KV_USAGE_FILES)?.unwrap_or(0) + d_files;
    kv_set_tx(tx, local, KV_USAGE_BYTES, &bytes.to_string());
    kv_set_tx(tx, local, KV_USAGE_FILES, &files.to_string());
    Ok(())
}

fn kv_get_i64(
    r: &impl Readable,
    ks: &SingleWriterTxKeyspace,
    key: &str,
) -> Result<Option<i64>, MetaError> {
    match kv_get_tx(r, ks, key)? {
        Some(s) => s
            .parse()
            .map(Some)
            .map_err(|_| MetaError::Invalid(format!("{key} is not an i64"))),
        None => Ok(None),
    }
}

/// `CONSTELLATION_META_CACHE_BYTES`, default 256 MiB (plan 29's
/// benchmarked configuration).
fn cache_bytes() -> u64 {
    std::env::var("CONSTELLATION_META_CACHE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256 * 1024 * 1024)
}

/// `min(available_parallelism, 16).max(4)` — fjall 3's own default
/// (`min(cores, 4)`) falls behind on compaction under churn (plan 29's
/// `RESULTS.md`); this is the benchmarked configuration.
fn worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
        .clamp(4, 16)
}

/// Point-read-heavy: `expect_point_read_hits` (skip building a filter on
/// the last, largest level), a non-zero hash-ratio for data blocks
/// (point reads that hit the hash index skip the binary search), and
/// L0-L2 filter/index blocks pinned resident.
fn ns_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .expect_point_read_hits(true)
        .data_block_hash_ratio_policy(HashRatioPolicy::all(0.5))
        .filter_block_pinning_policy(PinningPolicy::new([true, true, true, false]))
        .index_block_pinning_policy(PinningPolicy::new([true, true, true, false]))
}

/// fjall-backed [`crate::MetaStore`].
pub struct Meta {
    pub(crate) db: SingleWriterTxDatabase,
    pub(crate) ns: SingleWriterTxKeyspace,
    pub(crate) atime: SingleWriterTxKeyspace,
    pub(crate) orphans: SingleWriterTxKeyspace,
    pub(crate) journal_ks: SingleWriterTxKeyspace,
    pub(crate) atime_journal: SingleWriterTxKeyspace,
    pub(crate) local: SingleWriterTxKeyspace,
    pub(crate) pending_upload: SingleWriterTxKeyspace,
    pub(crate) chunk_ref: SingleWriterTxKeyspace,
    pub(crate) chunk_ref_by_ino: SingleWriterTxKeyspace,
    pub(crate) xattr_by_name: SingleWriterTxKeyspace,
    pub(crate) scratch: SingleWriterTxKeyspace,
    /// Plan 29 M2: `key -> counter: u64 BE`, the set of `ns` keys changed
    /// since the last publish observed them. Every write to `ns` records
    /// its key here in the same transaction (centralised in the `ns`
    /// write helpers — see `store::ns::Dirty`), so a publish's read set
    /// is exactly this keyspace rather than something re-derived from
    /// the journal.
    pub(crate) dirty: SingleWriterTxKeyspace,
    pub(crate) pins: SingleWriterTxKeyspace,
    pub(crate) epochs: SingleWriterTxKeyspace,
    pub(crate) reintegration: SingleWriterTxKeyspace,
    pub(crate) shadow: SingleWriterTxKeyspace,
    pub(crate) blobs: SingleWriterTxKeyspace,
    /// Plan 28 §S1b: `dir_ino(8 BE) -> block_start(8 BE) ++ used(4 BE)`,
    /// the per-directory ino allocation cursor `alloc_ino_tx` reads and
    /// advances. Node-local only — never journaled or replicated, since
    /// it is pure allocation policy and every node has its own disjoint
    /// ino-prefix range to draw blocks from.
    pub(crate) ino_alloc: SingleWriterTxKeyspace,
    usage: UsageTracker,
    #[allow(dead_code)]
    path: Option<PathBuf>,
}

impl Meta {
    /// Force every committed write to stable storage.
    ///
    /// Commits use `PersistMode::Buffer`: they reach the OS on commit, so
    /// a process crash loses nothing, but a power loss can drop the tail
    /// (as SQLite's `synchronous=NORMAL` did). An `fsync(2)` on the mount,
    /// and an orderly shutdown, call this to close that window.
    pub fn sync(&self) -> Result<(), MetaError> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetaError> {
        let path = path.as_ref();
        let db = SingleWriterTxDatabase::builder(path)
            .cache_size(cache_bytes())
            .worker_threads(worker_threads())
            .open()?;
        Self::init(db, Some(path.to_path_buf()))
    }

    /// A `temporary(true)` database in a fresh tempdir, cleaned up on
    /// drop. Kept under this name for source compatibility with the old
    /// SQLite `:memory:` engine's tests.
    pub fn open_in_memory() -> Result<Self, MetaError> {
        let dir = tempfile::Builder::new()
            .prefix("constellation-meta-")
            .tempdir()?
            .keep();
        let db = SingleWriterTxDatabase::builder(&dir)
            .cache_size(cache_bytes())
            .worker_threads(worker_threads())
            .temporary(true)
            .open()?;
        Self::init(db, None)
    }

    fn init(db: SingleWriterTxDatabase, path: Option<PathBuf>) -> Result<Self, MetaError> {
        let ns = db.keyspace("ns", ns_options)?;
        let atime = db.keyspace("atime", KeyspaceCreateOptions::default)?;
        let orphans = db.keyspace("orphans", KeyspaceCreateOptions::default)?;
        let journal_ks = db.keyspace("journal", KeyspaceCreateOptions::default)?;
        let atime_journal = db.keyspace("atime_journal", KeyspaceCreateOptions::default)?;
        let local = db.keyspace("local", KeyspaceCreateOptions::default)?;
        let pending_upload = db.keyspace("pending_upload", KeyspaceCreateOptions::default)?;
        let chunk_ref = db.keyspace("chunk_ref", KeyspaceCreateOptions::default)?;
        let chunk_ref_by_ino = db.keyspace("chunk_ref_by_ino", KeyspaceCreateOptions::default)?;
        let xattr_by_name = db.keyspace("xattr_by_name", KeyspaceCreateOptions::default)?;
        let scratch = db.keyspace("scratch", ns_options)?;
        let dirty = db.keyspace("dirty", KeyspaceCreateOptions::default)?;
        let pins = db.keyspace("pins", KeyspaceCreateOptions::default)?;
        let epochs = db.keyspace("epochs", KeyspaceCreateOptions::default)?;
        let reintegration = db.keyspace("reintegration", KeyspaceCreateOptions::default)?;
        let shadow = db.keyspace("shadow", KeyspaceCreateOptions::default)?;
        let blobs = db.keyspace("blobs", KeyspaceCreateOptions::default)?;
        let ino_alloc = db.keyspace("ino_alloc", KeyspaceCreateOptions::default)?;

        let meta = Meta {
            db,
            ns,
            atime,
            orphans,
            journal_ks,
            atime_journal,
            local,
            pending_upload,
            chunk_ref,
            chunk_ref_by_ino,
            xattr_by_name,
            scratch,
            dirty,
            pins,
            epochs,
            reintegration,
            shadow,
            blobs,
            ino_alloc,
            usage: UsageTracker::new(0, 0),
            path,
        };
        meta.bootstrap()?;
        Ok(meta)
    }

    fn bootstrap(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.ns, keys::inode(ROOT_INO))?.is_none() {
            let t = now_ns();
            let attrs = Attrs {
                kind: Kind::Dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                nlink: 2,
                size: 0,
                mtime_ns: t,
                ctime_ns: t,
                rdev: 0,
            };
            // Genesis (plan 29 M2): a brand-new filesystem has no commit
            // to bootstrap from, so the root inode's own creation has to
            // dirty itself — the mechanism a bootstrap-from-commit relies
            // on (the loaded root is already published, so ingestion
            // leaves `dirty` empty) does not apply here.
            ns::ns_insert(
                &mut tx,
                &self.ns,
                ns::Dirty::tracked(&self.dirty, &self.local),
                keys::inode(ROOT_INO),
                InodeRecord::new(attrs).encode(),
            )?;
            tx.insert(&self.atime, ROOT_INO.to_be_bytes(), t.to_le_bytes());
        }
        if kv_get_tx(&tx, &self.local, KV_NEXT_INO)?.is_none() {
            kv_set_tx(
                &mut tx,
                &self.local,
                KV_NEXT_INO,
                &(ROOT_INO + 1).to_string(),
            );
        }
        tx.commit()?;

        let r = self.db.read_tx();
        let bytes = kv_get_u64(&r, &self.local, KV_USAGE_BYTES)?.unwrap_or(0);
        let files = kv_get_u64(&r, &self.local, KV_USAGE_FILES)?.unwrap_or(0);
        self.usage.reseat(bytes, files);
        Ok(())
    }

    /// One snapshot for every read `f` makes: replaces `with_reader` +
    /// SQLite's begin-deferred-then-throwaway-read trick with fjall's
    /// native MVCC snapshot. `f` receives the snapshot explicitly (a
    /// `WriteTransaction` also implements `Readable`, so the same `f`
    /// can run mid-write-transaction call sites too).
    pub fn read_consistent<T, E>(&self, f: impl FnOnce(&Snapshot) -> Result<T, E>) -> Result<T, E>
    where
        E: From<MetaError>,
    {
        let snap = self.db.read_tx();
        f(&snap)
    }

    pub fn usage_bytes_files(&self) -> (u64, u64) {
        self.usage.load()
    }

    pub(crate) fn usage_tracker(&self) -> &UsageTracker {
        &self.usage
    }

    // ---- generic kv (the `local` keyspace) ----

    pub fn kv_get(&self, key: &str) -> Result<Option<String>, MetaError> {
        let r = self.db.read_tx();
        kv_get_tx(&r, &self.local, key)
    }

    pub fn kv_set(&self, key: &str, value: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        kv_set_tx(&mut tx, &self.local, key, value);
        tx.commit()?;
        Ok(())
    }

    pub fn kv_del(&self, key: &str) -> Result<(), MetaError> {
        self.local.remove(key.as_bytes())?;
        Ok(())
    }

    // ---- ino allocation / node prefix ----

    pub fn node_prefix(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        prefix_of(&r, &self.local)
    }

    pub fn set_node_prefix(&self, prefix: u64) -> Result<(), MetaError> {
        if prefix >= 1u64 << (64 - INO_PREFIX_SHIFT) {
            return Err(MetaError::Invalid("node prefix out of range".into()));
        }
        let mut tx = self.db.write_tx();
        if prefix_of(&tx, &self.local)? == prefix {
            return Ok(());
        }
        kv_set_tx(&mut tx, &self.local, KV_NODE_PREFIX, &prefix.to_string());
        kv_set_tx(&mut tx, &self.local, KV_NEXT_INO, "1");
        tx.commit()?;
        Ok(())
    }

    /// Allocate a new ino for a child of `dir` (plan 28 §S1b): draws the
    /// next number from `dir`'s active block, reserving a fresh block
    /// from the node's global counter if `dir` has none yet or its
    /// current block is full. `dir` is a locality *hint*, not a
    /// correctness requirement — passing the same value for every call
    /// degrades to one directory's worth of clustering, never a
    /// collision, since the block reservation itself is what guarantees
    /// disjoint ranges.
    pub fn allocate_ino(
        &self,
        dir: constellation_fs_core::Ino,
    ) -> Result<constellation_fs_core::Ino, MetaError> {
        let mut tx = self.db.write_tx();
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, dir)?;
        tx.commit()?;
        Ok(ino)
    }

    // ---- applied seq ----

    pub fn applied_seq(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        applied_seq_at(&r, &self.local)
    }

    pub fn applied_seq_at(&self, r: &impl Readable) -> Result<u64, MetaError> {
        applied_seq_at(r, &self.local)
    }

    pub fn set_applied_seq(&self, seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        kv_set_tx(&mut tx, &self.local, KV_APPLIED_SEQ, &seq.to_string());
        tx.commit()?;
        Ok(())
    }

    // ---- blob store (local Payload::Spilled bodies) ----

    pub(crate) fn hash_blob(bytes: &[u8]) -> constellation_mtree::record::BlobHash {
        constellation_mtree::record::BlobHash(*blake3::hash(bytes).as_bytes())
    }

    pub(crate) fn put_blob(
        tx: &mut SingleWriterWriteTx,
        blobs: &SingleWriterTxKeyspace,
        body: Vec<u8>,
    ) {
        let hash = Self::hash_blob(&body);
        tx.insert(blobs, hash.0.to_vec(), body);
    }

    pub(crate) fn get_blob(
        r: &impl Readable,
        blobs: &SingleWriterTxKeyspace,
        hash: &constellation_mtree::record::BlobHash,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        Ok(r.get(blobs, hash.0)?.map(|v| v.to_vec()))
    }

    // ---------------------------------------------- dirty tracking (M2)

    /// A `ns::Dirty` handle marking writes against this store's `ns`
    /// keyspace. The one thing every `ns`-mutating call site constructs
    /// to route its writes through the tracked path.
    pub(crate) fn dirty_for_ns(&self) -> ns::Dirty<'_> {
        ns::Dirty::tracked(&self.dirty, &self.local)
    }

    /// Every `(key, counter)` currently in `dirty`, under the caller's
    /// snapshot — the publisher's whole read set (plan 29 M2). `counter`
    /// is the value [`Self::clear_dirty_upto`] must be given back to
    /// retire the entry, so a key re-dirtied after the snapshot was taken
    /// (and therefore holding a higher counter by the time the clear
    /// runs) survives the clear.
    pub fn dirty_snapshot(&self, snap: &Snapshot) -> Result<Vec<(Vec<u8>, u64)>, MetaError> {
        let mut out = Vec::new();
        for guard in snap.iter(&self.dirty) {
            let (k, v) = guard.into_inner()?;
            let counter = u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .map_err(|_| MetaError::Invalid("dirty counter".into()))?,
            );
            out.push((k.to_vec(), counter));
        }
        Ok(out)
    }

    /// Remove each `(key, counter)` from `dirty`, but only if `dirty`
    /// still holds exactly the counter observed — a key whose stored
    /// counter has since moved on was re-dirtied after the snapshot this
    /// publish read, by a write this publish never saw, and must stay
    /// dirty for the next one.
    pub fn clear_dirty_upto(&self, keys: &[(Vec<u8>, u64)]) -> Result<(), MetaError> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut tx = self.db.write_tx();
        for (key, counter) in keys {
            if let Some(current) = tx.get(&self.dirty, key.clone())? {
                let current = u64::from_be_bytes(
                    current
                        .as_ref()
                        .try_into()
                        .map_err(|_| MetaError::Invalid("dirty counter".into()))?,
                );
                if current <= *counter {
                    tx.remove(&self.dirty, key.clone());
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Cheap non-emptiness probe: whether a publish has anything to do at
    /// all, without paying for a full [`Self::dirty_snapshot`].
    pub fn has_dirty(&self) -> bool {
        self.dirty.first_key_value().is_some()
    }
}

pub(crate) fn next_dirty_seq_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let next = kv_get_u64(tx, local, KV_NEXT_DIRTY_SEQ)?.unwrap_or(0) + 1;
    kv_set_tx(tx, local, KV_NEXT_DIRTY_SEQ, &next.to_string());
    Ok(next)
}

// ---------------------------------------------------------- free helpers

pub(crate) fn kv_get_tx(
    r: &impl Readable,
    ks: &SingleWriterTxKeyspace,
    key: &str,
) -> Result<Option<String>, MetaError> {
    Ok(r.get(ks, key.as_bytes())?
        .map(|v| String::from_utf8_lossy(&v).into_owned()))
}

pub(crate) fn kv_set_tx(
    tx: &mut SingleWriterWriteTx,
    ks: &SingleWriterTxKeyspace,
    key: &str,
    value: &str,
) {
    tx.insert(ks, key.as_bytes().to_vec(), value.as_bytes().to_vec());
}

fn kv_get_u64(
    r: &impl Readable,
    ks: &SingleWriterTxKeyspace,
    key: &str,
) -> Result<Option<u64>, MetaError> {
    match kv_get_tx(r, ks, key)? {
        Some(s) => s
            .parse()
            .map(Some)
            .map_err(|_| MetaError::Invalid(format!("{key} is not a u64"))),
        None => Ok(None),
    }
}

fn prefix_of(r: &impl Readable, local: &SingleWriterTxKeyspace) -> Result<u64, MetaError> {
    Ok(kv_get_u64(r, local, KV_NODE_PREFIX)?.unwrap_or(0))
}

fn applied_seq_at(r: &impl Readable, local: &SingleWriterTxKeyspace) -> Result<u64, MetaError> {
    Ok(kv_get_u64(r, local, KV_APPLIED_SEQ)?.unwrap_or(0))
}

/// Decode an `ino_alloc` cursor value: `block_start(8 BE) ++ used(4 BE)`.
fn decode_cursor(bytes: &[u8]) -> Result<(u64, u32), MetaError> {
    if bytes.len() != 12 {
        return Err(MetaError::Invalid("ino_alloc cursor".into()));
    }
    let block_start = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let used = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
    Ok((block_start, used))
}

fn encode_cursor(block_start: u64, used: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&block_start.to_be_bytes());
    out.extend_from_slice(&used.to_be_bytes());
    out
}

/// Allocate the next counter value for `dir`'s active ino block,
/// reserving a fresh `INO_BLOCK_SIZE` block from the node's global
/// counter if needed (see [`INO_BLOCK_SIZE`]). Crash-safe by
/// construction: the cursor update and the global counter bump (when a
/// new block is drawn) happen in the same write transaction the caller
/// commits along with the create itself, exactly as the old single
/// `next_ino++` did.
pub(crate) fn alloc_ino_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    ino_alloc: &SingleWriterTxKeyspace,
    dir: constellation_fs_core::Ino,
) -> Result<constellation_fs_core::Ino, MetaError> {
    let dir_key = dir.to_be_bytes();
    let cursor = tx
        .get(ino_alloc, dir_key)?
        .map(|v| decode_cursor(&v))
        .transpose()?;
    let (block_start, used) = match cursor {
        Some((block_start, used)) if used < INO_BLOCK_SIZE as u32 => (block_start, used),
        _ => {
            // No block yet, or `dir`'s block is full: reserve the next
            // free block from the global counter. Rounding the counter
            // up to a block boundary keeps every block aligned, which is
            // what makes `reclaim_ino_counter` below a cheap block-level
            // (not per-ino) operation.
            let next = kv_get_u64(tx, local, KV_NEXT_INO)?
                .ok_or_else(|| MetaError::Invalid("next_ino missing".into()))?;
            let block_start = next.div_ceil(INO_BLOCK_SIZE) * INO_BLOCK_SIZE;
            kv_set_tx(
                tx,
                local,
                KV_NEXT_INO,
                &(block_start + INO_BLOCK_SIZE).to_string(),
            );
            (block_start, 0)
        }
    };
    let counter = block_start + used as u64;
    if counter >= 1u64 << INO_PREFIX_SHIFT {
        return Err(MetaError::Invalid(
            "node ino space exhausted (40-bit counter overflow)".into(),
        ));
    }
    tx.insert(
        ino_alloc,
        dir_key.to_vec(),
        encode_cursor(block_start, used + 1),
    );
    let prefix = kv_get_u64(tx, local, KV_NODE_PREFIX)?.unwrap_or(0);
    Ok((prefix << INO_PREFIX_SHIFT) | counter)
}

/// Bump `next_ino` past the whole block containing `ino`, so replaying
/// local history can never let a future block allocation overlap a
/// number this node has already handed out under its own prefix. Blocks
/// are always aligned to `INO_BLOCK_SIZE` (see `alloc_ino_tx`), so
/// "protect this ino" and "protect its block" are the same operation.
/// Mirrors `apply_foreign`'s recompute in the old engine.
pub(crate) fn reclaim_ino_counter(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    ino: u64,
) -> Result<(), MetaError> {
    let prefix = kv_get_u64(tx, local, KV_NODE_PREFIX)?.unwrap_or(0);
    let lo = prefix << INO_PREFIX_SHIFT;
    let hi = (prefix + 1) << INO_PREFIX_SHIFT;
    if ino < lo || ino >= hi {
        return Ok(());
    }
    let counter = ino & ((1u64 << INO_PREFIX_SHIFT) - 1);
    let block_end = (counter / INO_BLOCK_SIZE + 1) * INO_BLOCK_SIZE;
    let next = kv_get_u64(tx, local, KV_NEXT_INO)?.unwrap_or(0);
    if block_end > next {
        kv_set_tx(tx, local, KV_NEXT_INO, &block_end.to_string());
    }
    Ok(())
}

impl Drop for Meta {
    fn drop(&mut self) {
        // Best effort: an orderly close leaves nothing only in OS buffers.
        let _ = self.db.persist(fjall::PersistMode::SyncAll);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    /// The crash-safety invariant every write path relies on: a
    /// namespace change and its journal row must appear together or not
    /// at all. Simulated directly (rather than actually crashing the
    /// process) by opening a write transaction, writing to two
    /// keyspaces, and dropping it without calling `commit()` — fjall's
    /// write transactions are entirely buffered until `commit()`, so a
    /// dropped transaction must leave neither write visible.
    #[test]
    fn an_uncommitted_write_transaction_leaves_no_keyspace_touched() {
        let meta = Meta::open_in_memory().unwrap();
        let ino = 12345u64;
        {
            let mut tx = meta.db.write_tx();
            tx.insert(&meta.ns, keys::inode(ino), b"bogus".to_vec());
            tx.insert(
                &meta.journal_ks,
                999u64.to_be_bytes().to_vec(),
                b"bogus".to_vec(),
            );
            // Dropped, never committed.
        }
        let r = meta.db.read_tx();
        assert!(r.get(&meta.ns, keys::inode(ino)).unwrap().is_none());
        assert!(r
            .get(&meta.journal_ks, 999u64.to_be_bytes())
            .unwrap()
            .is_none());
    }

    /// An ordinary mutation's namespace write and journal append are the
    /// same guarantee exercised through the public API: after `create`
    /// returns, both are present; nothing in between is ever observable.
    #[test]
    fn create_leaves_the_namespace_change_and_journal_row_together() {
        let meta = Meta::open_in_memory().unwrap();
        let before = meta.journal_len().unwrap();
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        assert!(meta.getattr(f.ino).unwrap().is_some());
        assert_eq!(meta.journal_len().unwrap(), before + 1);
    }
}

/// Plan 29 M3c: `recursive_size` (and readdirplus) read a file's attrs
/// from the §P6 dentry copy, so every mutation, local or replayed, must
/// keep each `0x02` copy equal to its `0x01` inode's attrs.
#[cfg(test)]
mod dentry_copy_tests {
    use super::*;
    use crate::{MetaStore, SetXattrMode};
    use constellation_fs_core::types::ROOT_INO;
    use constellation_mtree::record::{DentryRecord, InodeRecord};

    fn assert_copies_match(meta: &Meta, label: &str) {
        let r = meta.db.read_tx();
        let range = keys::whole_range(keys::RANGE_DENTRY);
        let mut n = 0;
        for guard in r.range(&meta.ns, ns::key_range_bounds(&range)) {
            let (k, v) = guard.into_inner().unwrap();
            let d = DentryRecord::decode(&v).unwrap();
            let rec: InodeRecord = ns::get_inode_record(&r, &meta.ns, d.ino)
                .unwrap()
                .unwrap_or_else(|| panic!("{label}: dangling dentry {k:?}"));
            assert_eq!(
                d.attrs, rec.attrs,
                "{label}: stale dentry copy for ino {}",
                d.ino
            );
            n += 1;
        }
        assert!(n > 0, "{label}: no dentries checked");
    }

    #[test]
    fn every_mutation_keeps_dentry_attr_copies_in_sync_locally_and_on_replay() {
        let a = Meta::open_in_memory().unwrap();
        let d = a.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
        let e = a.mkdir(ROOT_INO, "e", 0o755, 0, 0).unwrap().ino;
        let f = a.create(d, "f", 0o644, 0, 0).unwrap().ino;
        a.set_manifest(f, b"m1", 100).unwrap();
        a.link(f, e, "f-link").unwrap();
        a.set_manifest(f, b"m2", 250).unwrap();
        a.setattr(f, Some(0o600), Some(7), Some(8), Some(10), None, Some(5))
            .unwrap();
        a.set_xattr(f, "user.k", b"v", SetXattrMode::Set).unwrap();
        a.remove_xattr(f, "user.k").unwrap();
        a.set_xattr(d, "user.big", &vec![7u8; 8192], SetXattrMode::Set)
            .unwrap();
        a.rename(d, "f", e, "g").unwrap();
        a.symlink(e, "s", "target", 0, 0).unwrap();
        let g = a.create(e, "h", 0o644, 0, 0).unwrap().ino;
        a.rename(e, "h", e, "g").unwrap(); // replace
        a.setattr(g, None, None, None, Some(3), Some(1), Some(2))
            .unwrap();
        a.rename(ROOT_INO, "d", e, "d2").unwrap(); // move a directory
        assert_copies_match(&a, "local");

        let b = Meta::open_in_memory().unwrap();
        let records: Vec<_> = a
            .take_journal(10_000)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        b.apply_records(&records).unwrap();
        assert_copies_match(&b, "replay");
    }
}
