//! Metadata plane: local full replica in SQLite behind an engine trait
//! (DECISIONS.md ADR-9), log records and the local journal (DESIGN.md §4).

pub mod delegation;
pub mod error;
pub mod mutate;
pub mod prune;
pub mod readdeleg;
pub mod record;
pub mod reintegrate;
pub mod replay;
pub mod rid;
pub mod session;
pub mod store;

pub use error::MetaError;
pub use mutate::{execute as execute_mutate, MutateOp, MutateOutcome};
pub use prune::{Policy, PolicyError, PRUNE_XATTR};
pub use readdeleg::{
    recall_inos, recall_inos_of_op, CtoStats, HeldDelegation, ReadDelegations, ReadGrant,
    RecallNeed,
};
pub use record::{CloneNode, LogRecord};
pub use reintegrate::{conflict_dentry_name, CONFLICT_DIR};
pub use replay::TouchSet;
pub use rid::Rid;
pub use session::{
    DurableWait, JournalPos, KeySet, Position, ReadKey, SessionStats, SessionWait, Streams,
    STREAMS_CAP,
};
pub use store::backup::{BackupRole, BackupTx};
pub use store::held::{DroppedHeld, HeldInode, HeldSummary};
pub use store::inbox::{CompletedOutcome, InboxAck, InboxAckArmed};
pub use store::spec::{
    Refusal, SegmentApplied, ShippedRows, SpecKind, SpeculationCounts, Stranded, StrandedOp,
    LOCAL_REPLAY_INCARNATION,
};
pub use store::{
    BootstrapIndexBuilder, DelegateTx, ForeignApplyHook, JournalBatch, LogPrefixView, Meta,
    PublishBasis, SCRATCH_XATTR,
};

use constellation_fs_core::{FileAttr, Ino};

/// A directory entry as returned by `readdir`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub ino: Ino,
    pub kind: constellation_fs_core::InodeKind,
}

/// One child row used by the snapshot builder.  `snapshot_children` performs
/// the dentry/inode join in one indexed query per directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotNode {
    pub name: String,
    pub attr: FileAttr,
    pub target: Option<String>,
    pub manifest: Option<Vec<u8>>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

/// One inode as plan 28's `mtree` builder needs it: the attrs, the two
/// spillable bodies, and the whole xattr set, fetched together because
/// §P6's `plan_inode` decides the record's shape from all of them at
/// once.
///
/// Shaped like [`SnapshotNode`] minus the name, and not merged with it,
/// because the two answer different questions: a snapshot node is one
/// child of one directory, and this is one inode however many names
/// point at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeInode {
    pub attr: FileAttr,
    pub target: Option<String>,
    pub manifest: Option<Vec<u8>>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    pub id: String,
    pub path: String,
    pub name: String,
    pub root_hash: String,
    pub created_unix_ms: i64,
}

/// Parent-before-child description consumed by eager clone materialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloneSpec {
    /// `None` means the clone root; otherwise an index in the preceding
    /// portion of the same vector.
    pub parent_index: Option<usize>,
    pub name: String,
    pub kind: constellation_fs_core::InodeKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub target: Option<String>,
    pub manifest: Option<Vec<u8>>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetXattrMode {
    Set,
    Create,
    Replace,
}

/// The metadata engine interface (DESIGN.md §4 "Local store").
///
/// Every mutating call journals the corresponding log record in the same
/// transaction; `take_journal` drains records for flushing to S3.
pub trait MetaStore: Send + Sync {
    // --- namespace reads ---
    fn lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError>;
    fn getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError>;
    fn readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError>;
    fn readlink(&self, ino: Ino) -> Result<Option<String>, MetaError>;
    fn manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError>;
    fn get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>, MetaError>;
    fn list_xattrs(&self, ino: Ino) -> Result<Vec<String>, MetaError>;
    fn recursive_size(&self, ino: Ino) -> Result<(u64, u64), MetaError>;
    /// Whole-FS logical usage from the maintained counter: `(bytes, files)`.
    fn usage(&self) -> (u64, u64);
    /// Cluster-wide logical byte cap; `None` = unlimited.
    fn quota(&self) -> Result<Option<u64>, MetaError>;
    /// Journal `SetQuota` on p0 and apply locally.
    fn set_quota(&self, max_logical_bytes: Option<u64>) -> Result<(), MetaError>;

    // --- namespace writes (journaled) ---
    fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError>;
    fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError>;
    fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError>;
    /// Create a special node (fifo/socket/device). `kind.is_special()`.
    #[allow(clippy::too_many_arguments)]
    fn mknod(
        &self,
        parent: Ino,
        name: &str,
        kind: constellation_fs_core::InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
    ) -> Result<FileAttr, MetaError>;
    /// Hard link `ino` at `parent/name` (files and special nodes only).
    fn link(&self, ino: Ino, parent: Ino, name: &str) -> Result<FileAttr, MetaError>;
    fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError>;
    fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError>;
    fn rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError>;
    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<FileAttr, MetaError>;
    fn set_manifest(&self, ino: Ino, manifest: &[u8], size: u64) -> Result<(), MetaError>;
    fn set_xattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: SetXattrMode,
    ) -> Result<(), MetaError>;
    fn remove_xattr(&self, ino: Ino, name: &str) -> Result<(), MetaError>;

    // --- orphan lifecycle (unlink-while-open, DESIGN.md §3) ---
    /// Remove an orphaned inode (nlink == 0) after the last close.
    fn reap_orphan(&self, ino: Ino) -> Result<(), MetaError>;
    /// Inodes with nlink == 0 (candidates for reap at startup).
    fn orphans(&self) -> Result<Vec<Ino>, MetaError>;

    // --- journal ---
    fn take_journal(&self, max: usize) -> Result<Vec<(u64, LogRecord)>, MetaError>;
    fn ack_journal(&self, upto_seq: u64) -> Result<(), MetaError>;
    fn journal_len(&self) -> Result<u64, MetaError>;

    // --- read-time atime (plan 20) ---
    /// The one atime merge path: clamp to `now + skew`, guard on
    /// `ctime_ns < time_ns`, max-merge into `inode.atime_ns`. Never
    /// writes ctime, never journals. Each bump carries its own
    /// observation time so the guard is exact across a batch. Returns
    /// `(applied, skew_clamped)` counts. Shared by the local flush and
    /// the `AtimeBatch` handler so a locally applied value can never be
    /// one a replica rejects.
    fn apply_atime(&self, bumps: &[(Ino, i64, i64)]) -> Result<(u64, u64), MetaError>;
    /// Upsert atime bumps into `atime_journal`, coalescing per inode
    /// with max(). One row per inode regardless of read or flush count.
    fn queue_atime(&self, bumps: &[(Ino, i64, i64)]) -> Result<(), MetaError>;
    /// Pending atime rows for a partition (does not count toward
    /// `journal_len`, which must keep meaning real write backlog).
    fn atime_backlog_of(&self, part: &str) -> Result<u64, MetaError>;
    /// The oldest observation time (`time_ns`) among a partition's
    /// pending atime rows, or `None` if it has none. Feeds the standalone
    /// `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` ceiling: ride-along shipping
    /// and ship-then-release both piggyback on other lease activity, so a
    /// holder that only ever absorbs read-time bumps (never writes, never
    /// idles) needs this to notice its backlog has gone stale.
    fn atime_oldest_pending_ns(&self, part: &str) -> Result<Option<i64>, MetaError>;
    /// Read (without removing) up to `max` pending atime rows for a
    /// partition, as `(ino, atime_ns, time_ns)`. Delete only after the
    /// segment PUT succeeds, via `clear_atime`.
    fn take_atime_of(&self, part: &str, max: usize) -> Result<Vec<(Ino, i64, i64)>, MetaError>;
    /// Delete the named atime rows of a partition after a successful
    /// ship. Safe to lose: a re-ship is absorbed by the max-merge.
    fn clear_atime(&self, part: &str, inos: &[Ino]) -> Result<(), MetaError>;
    /// Drop all pending atime rows for a partition (epoch lost / drop
    /// path). Best effort, no ship.
    fn drop_atime_of(&self, part: &str) -> Result<(), MetaError>;
}
