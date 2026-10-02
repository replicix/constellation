//! fjall 3 metadata engine (plan 29 M1).
//!
//! Replaces the SQLite engine (`meta::sqlite`, removed) with
//! `fjall::SingleWriterTxDatabase`: one write transaction at a time
//! (`db.write_tx()`, serialized by fjall's own single-writer lock) gives
//! the same "namespace change + journal row in one transaction"
//! guarantee the SQLite writer mutex gave; readers take a lock-free
//! `db.read_tx()` snapshot (`fjall::Snapshot`, MVCC).
//!
//! Keyspaces (see `docs/plans/v1/done/29-fjall-metadata-engine.md` and
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
//!   `next_spec_seq`, `usage_bytes`/`usage_files` (the persisted
//!   mirror of `UsageTracker`, updated in the same transaction as the
//!   change that moves them — replacing the old O(namespace)
//!   `recursive_size(ROOT)` seed at open), quota's node-local mirror,
//!   `left`/`lease_lost`/`read_only_member`, the mtree publisher's
//!   own bookkeeping keys (`cli::mtree_publish`), and (plan 30 §M4) the
//!   `poisoned/<hash><ino>` marks of pending uploads whose chunk is gone
//!   from the local cache (`store::held`).
//! - `pending_upload` — `hash(32) ++ ino(8 BE) -> claims(u32 LE)`, the
//!   number of outstanding claims on uploading that chunk for that inode
//!   (an empty value, as older rows have, is one claim).
//! - `pending_upload_by_ino` — `ino(8 BE) ++ hash(32) -> ()`, the by-ino
//!   mirror of `pending_upload`'s rows (plan 39b: an `fsync` asks "does
//!   this inode have anything pending" and drains just that inode's rows,
//!   O(rows of the inode) rather than a scan of the whole queue). Written
//!   in the same transaction as every row it mirrors
//!   (`misc::add_pending_claim_tx` / `misc::remove_pending_row_tx`);
//!   built from `pending_upload` once, at open, on a store that predates
//!   it (`KV_PENDING_BY_INO`).
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
//! - `pins`, `epochs`, `reintegration` — node-local, one reasonable key
//!   layout each (see their modules).
//! - `spec`, `spec_live`, `pending_replay` — plan 30 §M3a's speculation
//!   log: every effect applied to `ns` ahead of the durable log, with
//!   its before-images; the index of outstanding entries; and the
//!   replay-by-rid queue of stranded ops. Node-local, never published.
//!   See `store::spec`.
//! - `journal_tx` — plan 30 §M3b: one row per journaled transaction,
//!   keyed by its first journal seq: where it ends (so a segment never
//!   splits it), the op and rid a replay re-executes, the epoch it ran
//!   under, and — when holder capture is on — the `spec` row holding its
//!   before-images. See `store::local`.
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
pub mod backup;
mod bootstrap;
pub mod held;
pub mod inbox;
pub(crate) mod journal;
pub(crate) mod local;
pub(crate) mod misc;
pub(crate) mod ns;
mod reads;
pub mod remote;
mod scratch;
pub(crate) mod snapshot;
pub mod spec;
pub(crate) mod writes;

pub use bootstrap::BootstrapIndexBuilder;
pub use local::{DelegateTx, LogPrefixView, PublishBasis};
pub use snapshot::{parse_quota_record, parse_snapshot_record, quota_record, snapshot_record};

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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

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
/// The journal position (`epoch:jseq`) the last applied or shipped
/// segment reached (`SessionState::advance`'s `through`), so a restarted
/// replica's session knows what it already holds (EC2 campaign 7 B-2: a
/// lock grant's `pending` part was unreachable for a whole incarnation,
/// the session's applied position being volatile).
pub(crate) const KV_APPLIED_POS: &str = "applied_pos";
pub(crate) const KV_NEXT_JOURNAL_SEQ: &str = "next_journal_seq";
/// Plan 30 §M3a: the speculation log's row counter (`store::spec`), raw
/// big-endian bytes like `KV_NEXT_JOURNAL_SEQ`. Never rolled back, so a
/// `spec_seq` is never reused.
pub(crate) const KV_NEXT_SPEC_SEQ: &str = "next_spec_seq";
/// Plan 30 §M2: this node's mount counter, bumped once at every mount
/// before serving any mutation (`bump_incarnation`). Persisted so it
/// survives a crash — the volatile per-incarnation rid `seq` counter
/// does not need to, since the incarnation bump alone is what keeps a
/// post-restart rid from ever colliding with a pre-crash one.
pub(crate) const KV_INCARNATION: &str = "incarnation";
pub(crate) const KV_USAGE_BYTES: &str = "usage_bytes";
pub(crate) const KV_USAGE_FILES: &str = "usage_files";
/// Plan 30 §M3b bookkeeping, raw big-endian `u64`s in `local`, each
/// maintained incrementally in the same transaction as the change it
/// describes, so that no hot path — a journaled write, a ship's ack, a
/// tailed segment, a `status` call — has to scan a keyspace to answer:
/// - `spec_live_count`: rows in `spec_live` (outstanding shadows/hints);
/// - `pending_replay_count`: rows in `pending_replay`;
/// - `local_spec_count`: `journal_tx` rows with a `spec_seq` (outstanding
///   captured `Local` transactions);
/// - `uncaptured_tx_count`: `journal_tx` rows without one;
/// - `spec_floor`: every `spec` row below it is deleted, so compaction
///   ranges from here instead of from the (tombstone-laden) start;
/// - `journal_acked`: every `journal` and `journal_tx` row at or below it
///   is deleted (acks always remove a head prefix), so journal and
///   `journal_tx` scans start past the shipped history's tombstones.
pub(crate) const KV_SPEC_LIVE_COUNT: &str = "spec_live_count";
pub(crate) const KV_PENDING_REPLAY_COUNT: &str = "pending_replay_count";
pub(crate) const KV_LOCAL_SPEC_COUNT: &str = "local_spec_count";
pub(crate) const KV_UNCAPTURED_TX_COUNT: &str = "uncaptured_tx_count";
pub(crate) const KV_SPEC_FLOOR: &str = "spec_floor";
pub(crate) const KV_JOURNAL_ACKED: &str = "journal_acked";
/// Plan 30 §M4: how many unrecoverable-chunk marks (`poisoned/` keys in
/// `local`, `store::held`) exist, so the ship path's "is anything
/// poisoned?" is one point read, not a range scan of a keyspace every
/// journaled write rewrites.
pub(crate) const KV_POISONED_COUNT: &str = "poisoned_count";
/// Set once `pending_upload_by_ino` mirrors every `pending_upload` row
/// (`Meta::build_pending_by_ino`).
pub(crate) const KV_PENDING_BY_INO: &str = "pending_upload_by_ino_built";
/// Monotonic counter behind the `dirty` keyspace (plan 29 M2): every key
/// written to `ns` records the counter value it was touched at, so
/// `clear_dirty_upto` can tell "still dirty at the counter a publish
/// observed" from "re-dirtied since".
pub(crate) const KV_NEXT_DIRTY_SEQ: &str = "next_dirty_seq";
/// The layout version of this store's persisted rows, written when the
/// store is created and checked by every open ([`Meta::check_format`]).
pub(crate) const KV_FORMAT: &str = "format";
/// The on-disk format this binary reads and writes. There is no migration:
/// a store of another format is refused at open. 2: a `spec` hint row
/// carries the reply's whole `Position` (`SpecKind::Hint::at`), not a
/// `seq` floor; 1 is every store written before the marker existed.
pub const META_FORMAT: u32 = 2;

pub type JournalBatch = Vec<(u64, crate::record::LogRecord)>;
/// Plan 30 §M2: `Meta::recent`'s value type — see that field's doc. Each
/// leaf carries the local wall-clock time (ms) it was recorded at, so a
/// periodic sweep can drop entries older than the completion retention
/// window independently of whether `acked_through` ever arrives for
/// them (a requester that crashes or never sends another op would
/// otherwise leave its rids in here forever).
pub(crate) type RecentOutcomes = std::collections::HashMap<
    (u64, u32),
    std::collections::BTreeMap<u64, (i64, Vec<crate::record::LogRecord>)>,
>;
/// Plan 30 §M2: cap on how many rids one (node, incarnation)'s `recent`
/// bucket keeps, independent of `acked_through` ever advancing — a
/// second line of defense (the first is fixing `acked_through` itself
/// to advance on every completion path, not just the forward one)
/// against an unbounded requester (crashed, or simply never sending
/// another op) pinning memory here forever. Oldest (lowest seq) entries
/// are dropped first when a bucket would exceed this.
pub(crate) const MAX_RECENT_PER_INCARNATION: usize = 4096;
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
        // Saturating, not `fetch_add`: atomics wrap silently on overflow
        // whatever the build profile, and a delta comes from a decoded
        // record's `size` (clamped to the i64 range by `replay::size_delta`,
        // but two hostile records still sum past it). A pinned counter
        // reads as an implausible usage; a wrapped one as a plausible
        // wrong one.
        let saturate = |counter: &AtomicI64, delta: i64| {
            if delta != 0 {
                let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(delta))
                });
            }
        };
        saturate(&self.bytes, d_bytes);
        saturate(&self.files, d_files);
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

/// Read one of the raw big-endian `u64` counters in `local` (0 if unset).
pub(crate) fn counter_get(
    r: &impl Readable,
    local: &SingleWriterTxKeyspace,
    key: &str,
) -> Result<u64, MetaError> {
    match r.get(local, key.as_bytes())? {
        Some(v) => {
            Ok(u64::from_be_bytes(v.as_ref().try_into().map_err(|_| {
                MetaError::Invalid(format!("{key} counter"))
            })?))
        }
        None => Ok(0),
    }
}

pub(crate) fn counter_set_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    key: &str,
    value: u64,
) {
    tx.insert(local, key.as_bytes().to_vec(), value.to_be_bytes().to_vec());
}

/// Move a counter by `delta`, saturating at zero.
pub(crate) fn counter_add_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    key: &str,
    delta: i64,
) -> Result<(), MetaError> {
    if delta == 0 {
        return Ok(());
    }
    let now = counter_get(tx, local, key)?;
    let next = if delta >= 0 {
        now.saturating_add(delta as u64)
    } else {
        now.saturating_sub(delta.unsigned_abs())
    };
    counter_set_tx(tx, local, key, next);
    Ok(())
}

thread_local! {
    static USAGE_NOTE: std::cell::Cell<Option<(i64, i64)>> = const { std::cell::Cell::new(None) };
}

/// Plan 30 §M3b: start noting every usage move `adjust_usage_tx` makes on
/// this thread, for a captured transaction's `Local` row
/// (`store::local`). Cheaper than reading the persisted counters before
/// and after: a thread-local add instead of four point reads and two
/// decimal parses per journaled write. A write transaction runs start to
/// commit on one call stack, and `Meta::begin_local` resets the note, so a
/// note left behind by a transaction that errored out is never read.
pub(crate) fn usage_note_begin() {
    USAGE_NOTE.with(|c| c.set(Some((0, 0))));
}

/// The usage moved since [`usage_note_begin`], ending the note.
pub(crate) fn usage_note_take() -> (i64, i64) {
    USAGE_NOTE.with(|c| c.take()).unwrap_or((0, 0))
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
    USAGE_NOTE.with(|c| {
        if let Some((b, f)) = c.get() {
            c.set(Some((b + d_bytes, f + d_files)));
        }
    });
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

/// `CONSTELLATION_HOLDER_CAPTURE` (plan 30 §M3b): `0`/`off`/`false`
/// turns holder-side speculation capture off (see `Meta::holder_capture`);
/// anything else, or unset, leaves it on. An internal switch for the
/// milestone's performance gate, not a tuning knob.
fn holder_capture_default() -> bool {
    !matches!(
        std::env::var("CONSTELLATION_HOLDER_CAPTURE")
            .ok()
            .as_deref(),
        Some("0" | "off" | "false")
    )
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
    pub(crate) pending_upload_by_ino: SingleWriterTxKeyspace,
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
    /// Plan 30 §M3a speculation log (`store::spec`): `spec_seq(8 BE) ->
    /// postcard(SpecRow)`, every captured application to `ns` ahead of
    /// (or, for `Foreign` rows, while older speculation is outstanding
    /// alongside) the durable log, with the before-images to undo it.
    pub(crate) spec: SingleWriterTxKeyspace,
    /// Plan 30 §M3a: `spec_seq(8 BE) -> postcard(LiveEntry)`, the
    /// outstanding (unretired, unstranded) shadow and hint entries —
    /// what the tailer's stranding check, the publish rule and `status`
    /// read, without decoding `spec` rows.
    pub(crate) spec_live: SingleWriterTxKeyspace,
    /// Plan 30 §M3a: `spec_seq(8 BE) -> postcard(QueuedReplay)`, stranded
    /// ops rolled back and queued for replay by rid, in original order.
    pub(crate) pending_replay: SingleWriterTxKeyspace,
    /// Plan 30 §M3b: `first_journal_seq(8 BE) -> postcard(JournalTx)`, one
    /// row per journaled transaction still in `journal` (see
    /// `store::local`). Removed with the transaction's journal rows when
    /// they ship, or when a deposition strands them.
    pub(crate) journal_tx: SingleWriterTxKeyspace,
    pub(crate) blobs: SingleWriterTxKeyspace,
    /// Plan 28 §S1b: `dir_ino(8 BE) -> block_start(8 BE) ++ used(4 BE)`,
    /// the per-directory ino allocation cursor `alloc_ino_tx` reads and
    /// advances. Node-local only — never journaled or replicated, since
    /// it is pure allocation policy and every node has its own disjoint
    /// ino-prefix range to draw blocks from.
    pub(crate) ino_alloc: SingleWriterTxKeyspace,
    /// Plan 30 §M2: `Rid::to_key() -> position(8 BE) ++
    /// recorded_at_ms(8 BE)`, plus `ROW_TAG_REFUSED ++ code(2 BE)` for a
    /// refused rid (`encode_completed_row`, `encode_refused_row`); position
    /// 0 means the outcome came from the log. Node-local
    /// and replicated-but-unpublished, like `spec`/`pins`/`epochs` —
    /// never touches `ns`/`dirty`, never appears in `dump_replicated`,
    /// but (unlike those ephemeral ones) is populated by replaying the
    /// durable log (`LogRecord::Completed`), so it survives a
    /// re-bootstrap the same way `ns` itself does.
    pub(crate) completed: SingleWriterTxKeyspace,
    /// Plan 30 §M9: the journal transactions this node holds as a
    /// synchronous backup for the holder it backs (`store::backup`).
    pub(crate) backup_tail: SingleWriterTxKeyspace,
    /// Plan 30 §M2 holder dedup: the in-memory "recent outcomes" map for
    /// ops this node executed as holder but has not yet shipped.
    /// Deliberately not a keyspace — it is volatile by design (lost on
    /// restart; that's fine, an already-shipped completion is covered by
    /// `completed`/the log instead).
    ///
    /// Keyed `(node, incarnation) -> (seq -> records)` rather than a
    /// flat list: `recent_outcome`/`remember_outcome` are on the hot
    /// path (every forwarded execution checks this before running), so
    /// a lookup must not scan every other requester's in-flight ops to
    /// find this one's — that turned an O(1)-ish operation into an
    /// O(total in-flight across every requester) one under concurrent
    /// load, which is exactly where the plan's own "forwarded latency
    /// within ±10%" measurement caught it (a flat `Vec` regressed
    /// several `meta-bench` 3-node configs by 20-45%). The inner
    /// `BTreeMap` also makes `forget_acked_through`'s "drop everything
    /// at or below this seq" a single `split_off`, not a linear
    /// `retain`.
    pub(crate) recent: std::sync::Mutex<RecentOutcomes>,
    /// Plan 30 §M3b: the lease epoch this node currently executes under
    /// as holder, 0 when it holds none. Written by the lease layer
    /// (`cli::lease::LeaseKeeper`, through [`Self::holder_epoch_cell`])
    /// the moment a CAS makes this node the holder — before the takeover
    /// gate runs — and cleared when it releases or is deposed. Two things
    /// read it: holder capture stamps every `Local` speculation row with
    /// it (`store::local`), and [`Self::install_shadow`] refuses to
    /// install a forward reply accepted at a lower epoch (the op is
    /// queued for replay instead).
    pub(crate) holder_epoch: Arc<AtomicU64>,
    /// Plan 30 §M3b's internal switch for holder-side capture
    /// (`CONSTELLATION_HOLDER_CAPTURE`, default on). Off is the plan's
    /// performance-gate fallback: a holder's own writes carry no
    /// before-images, a holder with an unshipped journal does not publish,
    /// and a deposed holder rebuilds its namespace from the head commit
    /// instead of rolling back (`cli::recovery::recover_deposed`).
    pub(crate) holder_capture: AtomicBool,
    /// Unit tests of the tree publisher only: publish `ns` as it stands
    /// though this node's unshipped journal holds uncaptured writes
    /// (`Meta::publish_basis_at`). Never set by the daemon.
    pub(crate) publish_unshipped: AtomicBool,
    /// Plan 30 §M12: whether the delegation table names any live
    /// delegation (maintained by `delegation::write_table_tx`): the
    /// root's FUSE fast path reads this atomic before it looks at the
    /// table at all, so a cluster without delegations pays nothing.
    pub(crate) deleg_any: AtomicBool,
    /// Plan 30 §M12: the root's FUSE fast path holds this *shared*
    /// from its ownership check through its execution; a transaction
    /// that journals a `Delegate` or `Recall` record here
    /// (`apply_records_journaled_completing`, the root's grant) holds it
    /// *exclusively*. So a fast-path write that checked the table
    /// before a grant is journaled before the grant's record, and one
    /// checking after it sees the grant — the root never executes,
    /// behind a delegate's back, a name the log has just given away
    /// (harness `chaos-soak-4` under the placement: double winners).
    pub(crate) deleg_gate: std::sync::RwLock<()>,
    /// Plan 30 §M4: what the last ship plan held back behind an
    /// unrecoverable pending chunk (`store::held`), for `status`.
    pub(crate) held: std::sync::Mutex<held::HeldSummary>,
    /// Whether `held` may be non-default, so a ship with nothing poisoned
    /// does not take its lock (plan 30 §M4 round 2).
    pub(crate) held_any: AtomicBool,
    /// Plan 30 §M5: the dentries and inodes this node's *unshipped*
    /// journal touched, kept in memory so a forward reply can say whether
    /// the holder evaluated the op behind unshipped work on the same keys
    /// (`Meta::unshipped_overlaps`). Fed by `mutate::execute` — every
    /// journaled op, the FUSE fast path included — and cleared the moment
    /// the journal ships out completely. Conservative when it is stale
    /// (a key stays until the next full ship), never permissive.
    pub(crate) unshipped: std::sync::Mutex<crate::replay::TouchSet>,
    /// Plan 30 §M9, per row: for each key of `unshipped`, an upper bound
    /// on the journal seq of the rows that touched it (see
    /// [`UnshippedSeqs`]), so a gated holder's read waits for the rows it
    /// would observe, not for every row. Cleared with `unshipped`.
    pub(crate) unshipped_seqs: std::sync::Mutex<UnshippedSeqs>,
    /// Plan 30 §M9 round 2: a lower bound on every live `backup_tail`
    /// key, `(epoch, first)`, so a trim scans the live tail and not the
    /// tombstones of everything trimmed before it (`Meta::backup_trim`).
    /// `None`: unknown (fresh open, or cleared), the next trim scans
    /// from the start and establishes it.
    pub(crate) backup_tail_floor: std::sync::Mutex<Option<(u64, u64)>>,
    /// How many times a ship plan, a retirement or an ack took a held-set
    /// path (plan 30 §M4 round 2). Only ever moves while something is
    /// poisoned or held; the cost tests in `store::local` pin that it stays
    /// at zero otherwise.
    pub(crate) held_work: AtomicU64,
    /// Plan 30 §M6: positions, the `observed` watermark and the read
    /// wait (`crate::session`).
    pub(crate) session: crate::session::SessionState,
    /// Plan 30 §M8: read delegations, both sides (`crate::readdeleg`).
    pub(crate) read_delegations: crate::readdeleg::ReadDelegations,
    /// Plan 30 §M14: lock grants (sequencer side), grants held and local
    /// locks (node side) (`crate::locks`).
    pub(crate) locks: crate::locks::LockTables,
    /// Plan 30 §M7: told the records of every foreign segment this replica
    /// applied (the daemon invalidates the kernel's FUSE caches for what
    /// they touched, so another node's write is visible without waiting
    /// out the attribute/entry TTL).
    foreign_apply_hook: std::sync::OnceLock<ForeignApplyHook>,
    /// Plan 32 §6.3: bumped (and the hook told) whenever a snapshot row
    /// may have appeared or gone, so the accounting index reconciles
    /// without polling the rows.
    snapshot_gen: AtomicU64,
    snapshot_hook: std::sync::RwLock<Option<SnapshotChangeHook>>,
    /// [`Meta::vacuum_churn`]: each churn keyspace's entry count right
    /// after its last vacuum.
    vacuum_baselines: std::sync::Mutex<std::collections::HashMap<&'static str, usize>>,
    usage: UsageTracker,
    /// How many times [`Meta::sync`] ran (M16: the tests pin that every
    /// safety-relevant write — a promise, an epoch join or ack, a seal, a
    /// grant horizon — requests one before it is acted on).
    syncs: AtomicU64,
    #[allow(dead_code)]
    path: Option<PathBuf>,
}

/// [`Meta::vacuum_churn`] leaves a keyspace alone below this many
/// entries (tombstones included): scanning that many is cheap.
pub const VACUUM_MIN_ENTRIES: usize = 4096;

/// See [`Meta::set_foreign_apply_hook`].
pub type ForeignApplyHook = Box<dyn Fn(&[crate::record::LogRecord]) + Send + Sync>;

/// See [`Meta::set_snapshot_change_hook`].
pub type SnapshotChangeHook = Arc<dyn Fn() + Send + Sync>;

impl Meta {
    /// Plan 30 §M7: call `hook` with the records of every foreign segment
    /// applied from now on ([`Meta::note_foreign_applied`]). Set once; a
    /// second call is ignored.
    pub fn set_foreign_apply_hook(&self, hook: ForeignApplyHook) {
        let _ = self.foreign_apply_hook.set(hook);
    }

    /// Plan 32 §6.3: call `hook` whenever a snapshot row may have been
    /// created or deleted (a local write, or any record replayed). It is
    /// a hint, called from inside the writer's path, so it must be cheap
    /// and must not block; it can fire before the change's transaction
    /// commits, so a listener reads the rows a moment later and keeps a
    /// periodic reconcile as the backstop. Replaces any previous hook.
    pub fn set_snapshot_change_hook(&self, hook: Option<SnapshotChangeHook>) {
        *self.snapshot_hook.write().expect("snapshot hook lock") = hook;
    }

    /// How many snapshot-row changes [`Meta::note_snapshot_change`] has
    /// seen: a cheap "did anything change since" check.
    pub fn snapshot_gen(&self) -> u64 {
        self.snapshot_gen.load(Ordering::Acquire)
    }

    pub(crate) fn note_snapshot_change(&self) {
        self.snapshot_gen.fetch_add(1, Ordering::AcqRel);
        let hook = self
            .snapshot_hook
            .read()
            .expect("snapshot hook lock")
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// A foreign segment carrying `records` was applied to this replica.
    pub fn note_foreign_applied(&self, records: &[crate::record::LogRecord]) {
        if let Some(hook) = self.foreign_apply_hook.get() {
            hook(records);
        }
    }

    /// Force every committed write to stable storage.
    ///
    /// Commits use `PersistMode::Buffer`: they reach the OS on commit, so
    /// a process crash loses nothing, but a power loss (or kernel crash)
    /// can drop the tail (as SQLite's `synchronous=NORMAL` did). An
    /// `fsync(2)` on the mount, and an orderly shutdown, call this to
    /// close that window. So does every write whose *safety* rests on
    /// being remembered (M16): a published promise and an epoch join
    /// (`promise_issue`, `promise_join_begin`), an epoch's persisted
    /// state (`persist_epoch`, the epoch hold), a backup's seal
    /// (`backup_seal`, `deleg_backup_seal`) and the read-grant horizon
    /// (`note_grant_horizon`) — each syncs before the caller acts on it.
    /// What a *durability* contract covers (a journal, a backup tail) is
    /// described in `docs/reference/features/durability-and-failover.md`.
    pub fn sync(&self) -> Result<(), MetaError> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// How many times [`Self::sync`] completed (tests).
    pub fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// [`Self::kv_set`], then [`Self::sync`]: for a value whose loss on
    /// power failure would be a safety problem, not just lost work.
    pub fn kv_set_durable(&self, key: &str, value: &str) -> Result<(), MetaError> {
        self.kv_set(key, value)?;
        self.sync()
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
        let pending_upload_by_ino =
            db.keyspace("pending_upload_by_ino", KeyspaceCreateOptions::default)?;
        let chunk_ref = db.keyspace("chunk_ref", KeyspaceCreateOptions::default)?;
        let chunk_ref_by_ino = db.keyspace("chunk_ref_by_ino", KeyspaceCreateOptions::default)?;
        let xattr_by_name = db.keyspace("xattr_by_name", KeyspaceCreateOptions::default)?;
        let scratch = db.keyspace("scratch", ns_options)?;
        let dirty = db.keyspace("dirty", KeyspaceCreateOptions::default)?;
        let pins = db.keyspace("pins", KeyspaceCreateOptions::default)?;
        let epochs = db.keyspace("epochs", KeyspaceCreateOptions::default)?;
        let reintegration = db.keyspace("reintegration", KeyspaceCreateOptions::default)?;
        let spec = db.keyspace("spec", KeyspaceCreateOptions::default)?;
        let spec_live = db.keyspace("spec_live", KeyspaceCreateOptions::default)?;
        let pending_replay = db.keyspace("pending_replay", KeyspaceCreateOptions::default)?;
        let journal_tx = db.keyspace("journal_tx", KeyspaceCreateOptions::default)?;
        let blobs = db.keyspace("blobs", KeyspaceCreateOptions::default)?;
        let ino_alloc = db.keyspace("ino_alloc", KeyspaceCreateOptions::default)?;
        let completed = db.keyspace("completed", KeyspaceCreateOptions::default)?;
        let backup_tail = db.keyspace("backup_tail", KeyspaceCreateOptions::default)?;

        let meta = Meta {
            db,
            ns,
            atime,
            orphans,
            journal_ks,
            atime_journal,
            local,
            pending_upload,
            pending_upload_by_ino,
            chunk_ref,
            chunk_ref_by_ino,
            xattr_by_name,
            scratch,
            dirty,
            pins,
            epochs,
            reintegration,
            spec,
            spec_live,
            pending_replay,
            journal_tx,
            blobs,
            ino_alloc,
            completed,
            backup_tail,
            recent: std::sync::Mutex::new(std::collections::HashMap::new()),
            holder_epoch: Arc::new(AtomicU64::new(0)),
            holder_capture: AtomicBool::new(holder_capture_default()),
            publish_unshipped: AtomicBool::new(false),
            deleg_any: AtomicBool::new(false),
            deleg_gate: std::sync::RwLock::new(()),
            held: std::sync::Mutex::new(held::HeldSummary::default()),
            held_any: AtomicBool::new(false),
            unshipped: std::sync::Mutex::new(crate::replay::TouchSet::default()),
            unshipped_seqs: std::sync::Mutex::new(UnshippedSeqs::default()),
            backup_tail_floor: std::sync::Mutex::new(None),
            held_work: AtomicU64::new(0),
            session: crate::session::SessionState::default(),
            read_delegations: crate::readdeleg::ReadDelegations::default(),
            locks: crate::locks::LockTables::default(),
            foreign_apply_hook: std::sync::OnceLock::new(),
            snapshot_gen: AtomicU64::new(0),
            snapshot_hook: std::sync::RwLock::new(None),
            vacuum_baselines: Default::default(),
            usage: UsageTracker::new(0, 0),
            syncs: AtomicU64::new(0),
            path,
        };
        meta.check_format()?;
        meta.bootstrap()?;
        meta.build_pending_by_ino()?;
        let table = meta.delegation_table();
        meta.deleg_any
            .store(!table.is_empty(), std::sync::atomic::Ordering::Release);
        // Plan 30 §M11: the session state's generations survive a
        // restart (see `SessionState::seed_generations`).
        let live: std::collections::BTreeSet<u64> = table.iter().map(|d| d.gen).collect();
        meta.session
            .seed_generations(table.max_gen(), &live, &|gen| {
                meta.log_stream_idx(gen).unwrap_or(0)
            });
        // The applied journal position is volatile too; what this
        // replica holds of the log is not. Without this a watermark
        // naming the holder's journal (a lock grant's floor) stays
        // unreached until the next segment arrives — for good on an idle
        // cluster (EC2 campaign 7 B-2).
        let applied_seq = meta.applied_seq().unwrap_or(0);
        meta.session.seed_applied(applied_seq, meta.applied_pos());
        Ok(meta)
    }

    /// Refuse a store of another on-disk format ([`META_FORMAT`]) before
    /// anything reads its rows, and mark a brand-new one (no root inode
    /// yet) with this binary's. A store with a root but no marker
    /// predates the marker: format 1.
    fn check_format(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let found = match kv_get_tx(&tx, &self.local, KV_FORMAT)? {
            Some(v) => v.parse::<u32>().ok(),
            None if tx.get(&self.ns, keys::inode(ROOT_INO))?.is_none() => {
                kv_set_tx(&mut tx, &self.local, KV_FORMAT, &META_FORMAT.to_string());
                tx.commit()?;
                return Ok(());
            }
            None => Some(1),
        };
        if found == Some(META_FORMAT) {
            return Ok(());
        }
        let found = found.map_or_else(|| "unreadable".to_string(), |f| f.to_string());
        let at = self
            .path
            .as_ref()
            .map_or_else(|| "(in memory)".to_string(), |p| p.display().to_string());
        Err(MetaError::Invalid(format!(
            "metadata store {at} has on-disk format {found}, this binary reads only \
             format {META_FORMAT} and has no migration: stop the node cleanly on the \
             binary that wrote it (its journal shipped), remove the store, and start \
             it again to rebuild from the log"
        )))
    }

    /// A store written before `pending_upload_by_ino` existed: build the
    /// mirror from the table, once (marked by `KV_PENDING_BY_INO`).
    fn build_pending_by_ino(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if kv_get_tx(&tx, &self.local, KV_PENDING_BY_INO)?.is_some() {
            return Ok(());
        }
        let rows: Vec<Vec<u8>> = tx
            .iter(&self.pending_upload)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in rows {
            if k.len() != 40 {
                return Err(MetaError::Invalid("pending_upload key length".into()));
            }
            let hash = constellation_fs_core::ChunkHash(k[..32].try_into().expect("32 bytes"));
            let ino = u64::from_be_bytes(k[32..].try_into().expect("8 bytes"));
            tx.insert(
                &self.pending_upload_by_ino,
                misc::cri_key(ino, &hash),
                Vec::new(),
            );
        }
        kv_set_tx(&mut tx, &self.local, KV_PENDING_BY_INO, "1");
        tx.commit()?;
        Ok(())
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
                rdev: constellation_types::Rdev::default(),
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

    // ---- holder state (plan 30 §M3b) ----

    /// The shared cell behind [`Self::holder_epoch`], for the lease layer
    /// to write (`LeaseKeeper::share_holder_epoch`).
    pub fn holder_epoch_cell(&self) -> Arc<AtomicU64> {
        self.holder_epoch.clone()
    }

    /// Plan 30 §M5: whether the unshipped journal touched any key `keys`
    /// names — the base a forward reply reports to its requester (see
    /// `constellation_authority`'s `PeerMsg::MutateReply::base`).
    pub fn unshipped_overlaps(&self, keys: &crate::replay::TouchSet) -> bool {
        let mine = self.unshipped.lock().unwrap();
        keys.dentries.iter().any(|d| mine.dentries.contains(d))
            || keys.inos.iter().any(|i| mine.inos.contains(i))
    }

    /// Plan 30 §M9: whether the unshipped journal touched what a read of
    /// `keys` reads (a holder under a non-`Local` acknowledgement policy
    /// answers such a read only once those rows are durable). A `Dir`
    /// key is touched by any unshipped dentry under it.
    pub fn unshipped_touches_keys(&self, keys: &[crate::session::ReadKey]) -> bool {
        let mine = self.unshipped.lock().unwrap();
        if mine.dentries.is_empty() && mine.inos.is_empty() {
            return false;
        }
        keys.iter().any(|k| match k {
            crate::session::ReadKey::Dentry(parent, name) => {
                mine.dentries.contains(&(*parent, name.clone()))
            }
            crate::session::ReadKey::Ino(ino) => mine.inos.contains(ino),
            crate::session::ReadKey::Dir(dir) => {
                mine.inos.contains(dir) || mine.dentries.iter().any(|(p, _)| p == dir)
            }
        })
    }

    /// Records just journaled: their keys join the unshipped set.
    pub(crate) fn note_unshipped(&self, records: &[crate::record::LogRecord]) {
        let touched = crate::replay::TouchSet::from_records(records.iter());
        {
            let mut mine = self.unshipped.lock().unwrap();
            mine.dentries.extend(touched.dentries.iter().cloned());
            mine.inos.extend(touched.inos.iter().copied());
        }
        self.note_unshipped_seqs(&touched);
    }

    /// An inode's record just journaled outside `mutate::execute` (the
    /// holder's own manifest commit): it joins the unshipped set.
    pub(crate) fn note_unshipped_ino(&self, ino: u64) {
        self.unshipped.lock().unwrap().inos.insert(ino);
        let mut touched = crate::replay::TouchSet::default();
        touched.inos.insert(ino);
        self.note_unshipped_seqs(&touched);
    }

    /// Plan 30 §M9, per row: `touched`'s rows are journaled (committed),
    /// so the journal tip read now bounds their seqs from above. The tip
    /// costs a point read, paid only while the durability gate is on;
    /// with it off the keys are marked untracked (compared against the
    /// tip at read time, as before) until the journal ships out — a gate
    /// that comes on later must not trust a bound that missed rows.
    fn note_unshipped_seqs(&self, touched: &crate::replay::TouchSet) {
        if touched.dentries.is_empty() && touched.inos.is_empty() {
            return;
        }
        let seq = if self.session.durable_gated() {
            self.journal_tip().unwrap_or(UNTRACKED)
        } else {
            UNTRACKED
        };
        self.unshipped_seqs.lock().unwrap().note(touched, seq);
    }

    /// Plan 30 §M9, per row: the journal seq through which the rows the
    /// unshipped journal wrote for `keys` reach — `None` when none of
    /// them is touched, [`UNTRACKED`] when some key's rows are not
    /// tracked (the caller compares the tip). The read semantics are
    /// [`Self::unshipped_touches_keys`]'s.
    pub(crate) fn unshipped_seq_for(&self, keys: &[crate::session::ReadKey]) -> Option<u64> {
        if !self.unshipped_touches_keys(keys) {
            return None;
        }
        let seqs = self.unshipped_seqs.lock().unwrap();
        let mut need = 0u64;
        for k in keys {
            let s = match k {
                crate::session::ReadKey::Dentry(parent, name) => seqs
                    .dentries
                    .get(&(*parent, name.clone()))
                    .copied()
                    .unwrap_or(0),
                crate::session::ReadKey::Ino(ino) => seqs.inos.get(ino).copied().unwrap_or(0),
                crate::session::ReadKey::Dir(dir) => seqs
                    .inos
                    .get(dir)
                    .copied()
                    .unwrap_or(0)
                    .max(seqs.dirs.get(dir).copied().unwrap_or(0)),
            };
            need = need.max(s);
        }
        // A key of the set with no bound at all (journaled before this
        // map existed for it): untracked.
        Some(if need == 0 { UNTRACKED } else { need })
    }

    /// The journal shipped out completely: nothing unshipped touches
    /// anything any more.
    pub(crate) fn clear_unshipped(&self) {
        *self.unshipped.lock().unwrap() = crate::replay::TouchSet::default();
        *self.unshipped_seqs.lock().unwrap() = UnshippedSeqs::default();
    }

    /// The epoch this node executes under as holder, 0 when it holds none.
    pub fn holder_epoch(&self) -> u64 {
        self.holder_epoch.load(Ordering::SeqCst)
    }

    /// Set (or, with 0, clear) the holder epoch directly. The daemon goes
    /// through [`Self::holder_epoch_cell`]; tests use this.
    pub fn set_holder_epoch(&self, epoch: u64) {
        self.holder_epoch.store(epoch, Ordering::SeqCst);
    }

    /// Whether holder-side capture is on (see the field's doc).
    pub fn holder_capture(&self) -> bool {
        self.holder_capture.load(Ordering::Relaxed)
    }

    /// Switch holder-side capture. Only meant for tests and for the
    /// performance gate's fallback; flipping it while this node holds a
    /// non-empty journal leaves that journal partly captured, which the
    /// publisher treats as uncaptured (it defers).
    pub fn set_holder_capture(&self, on: bool) {
        self.holder_capture.store(on, Ordering::Relaxed);
    }

    /// For unit tests of the tree publisher, which publish local writes
    /// nothing shipped: treat them as the log (see `publish_unshipped`).
    pub fn set_publish_unshipped_for_tests(&self, on: bool) {
        self.publish_unshipped.store(on, Ordering::Relaxed);
    }

    // ---- incarnation (plan 30 §M2) ----

    /// Bump and persist this node's incarnation, returning the new
    /// value. Called once per mount, before serving any mutation
    /// (`node_runtime.rs`), so every rid this mount allocates carries an
    /// incarnation strictly greater than any previous mount's — the
    /// property that keeps a rid from ever being reused even though the
    /// per-incarnation `seq` counter itself restarts at 0 every time.
    pub fn bump_incarnation(&self) -> Result<u32, MetaError> {
        let mut tx = self.db.write_tx();
        let current: u32 = kv_get_u64(&tx, &self.local, KV_INCARNATION)?.unwrap_or(0) as u32;
        let next = current + 1;
        kv_set_tx(&mut tx, &self.local, KV_INCARNATION, &next.to_string());
        tx.commit()?;
        Ok(next)
    }

    // ---- completed (plan 30 §M2 exactly-once) ----

    /// The position (journal seq) `rid` was completed at, if this
    /// replica has tailed a segment carrying its `Completed` record (or,
    /// for a rid this node itself executed as holder and has not yet
    /// shipped, the record it produced — see `recent_outcome`).
    ///
    /// Plan 30 §M13: an *executed* rid only. A rid the log refused
    /// (`LogRecord::Refused`, inbox path) has a `completed` row too but
    /// answers `None` here and `Some(code)` from [`Self::refused_code`];
    /// every dedup site asks both (`Self::completed_outcome`).
    pub fn completed_position(&self, rid: crate::rid::Rid) -> Result<Option<u64>, MetaError> {
        Ok(match self.completed_outcome(rid)? {
            Some(inbox::CompletedOutcome::Executed { position }) => Some(position),
            _ => None,
        })
    }

    /// Encode a `completed` row value: `position(8 BE) ++
    /// recorded_at_ms(8 BE)`. `recorded_at_ms` is this replica's own
    /// clock at apply time (not the writer's), matching the tolerance
    /// the rest of the system already gives cross-node clocks (e.g.
    /// atime's skew guard) — retention only needs a rough age, not a
    /// linearizable one.
    pub(crate) fn encode_completed_row(position: u64, recorded_at_ms: i64) -> Vec<u8> {
        let mut v = Vec::with_capacity(16);
        v.extend_from_slice(&position.to_be_bytes());
        v.extend_from_slice(&recorded_at_ms.to_be_bytes());
        v
    }

    /// Plan 30 §M13: a `completed` row for a rid the log *refused*:
    /// the executed row's 16 bytes (so retention reads the same
    /// `recorded_at`), then an outcome tag `1` and the refusal's portable
    /// [`constellation_types::Code`] wire number (plan 31 §7). An executed
    /// row is exactly 16 bytes; anything longer with tag `1` is a refusal.
    pub(crate) fn encode_refused_row(
        position: u64,
        recorded_at_ms: i64,
        code: constellation_types::Code,
    ) -> Vec<u8> {
        let mut v = Self::encode_completed_row(position, recorded_at_ms);
        v.push(inbox::ROW_TAG_REFUSED);
        v.extend_from_slice(&code.to_wire().to_be_bytes());
        v
    }

    /// The holder's in-memory answer for `rid`, if it executed it as
    /// holder and has not yet dropped it from `recent` (shipped-and-
    /// acked, or aged out — see `forget_acked_through`). Kept separate
    /// from `completed_position` because `recent` also carries *which*
    /// records the reply must repeat verbatim, not just "did this
    /// happen".
    pub fn recent_outcome(&self, rid: crate::rid::Rid) -> Option<Vec<crate::record::LogRecord>> {
        let recent = self.recent.lock().unwrap();
        recent
            .get(&(rid.node, rid.incarnation))?
            .get(&rid.seq)
            .map(|(_, recs)| recs.clone())
    }

    /// Record that the holder just executed `rid`, producing `records`,
    /// so a retry (same rid, same still-live holder) gets an identical
    /// reply instead of executing again. Refusals are never recorded
    /// here (per plan 30 §M2: "a retried refused op is re-evaluated").
    ///
    /// Bounds the per-`(node, incarnation)` bucket at
    /// [`MAX_RECENT_PER_INCARNATION`] regardless of whether
    /// `acked_through` ever advances for it — dropping the oldest
    /// (lowest-seq) entries first, since those are the ones a live
    /// requester would have acked first if it were still around. This
    /// is defense in depth: the primary fix is every completion path in
    /// `mutate_op_rebasable` calling `mark_acked`, so the requester's own
    /// `acked_through` should keep this bucket far under the cap in
    /// practice.
    pub fn remember_outcome(&self, rid: crate::rid::Rid, records: &[crate::record::LogRecord]) {
        let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
        let mut recent = self.recent.lock().unwrap();
        let bucket = recent.entry((rid.node, rid.incarnation)).or_default();
        let entry = bucket
            .entry(rid.seq)
            .or_insert_with(|| (now_ms, Vec::new()));
        entry.1.extend(records.iter().cloned());
        while bucket.len() > MAX_RECENT_PER_INCARNATION {
            bucket.pop_first();
        }
    }

    /// Forget the holder's answer for `rid` (plan 30 §M3b): its
    /// transaction was stranded and rolled back, so a retry must not be
    /// told it took effect. Called from inside the stranding transaction;
    /// if that transaction then fails to commit, the only cost is one
    /// retry answered from `completed` instead of from here.
    pub(crate) fn forget_recent(&self, rid: crate::rid::Rid) {
        let mut recent = self.recent.lock().unwrap();
        let key = (rid.node, rid.incarnation);
        let now_empty = match recent.get_mut(&key) {
            Some(bucket) => {
                bucket.remove(&rid.seq);
                bucket.is_empty()
            }
            None => false,
        };
        if now_empty {
            recent.remove(&key);
        }
    }

    /// Drop every `recent` entry, across every requester, whose recorded
    /// time is older than `retention_ms` — the age-based half of
    /// [`Self::remember_outcome`]'s size cap, for a requester that never
    /// sends another op (so `acked_through` never arrives) or crashes
    /// outright. Meant to run on a periodic timer (like
    /// [`Self::prune_completed`]), not per request: it walks every
    /// bucket, which `forget_acked_through`'s per-request, single-bucket
    /// `split_off` deliberately does not.
    pub fn prune_recent_older_than(&self, now_ms: i64, retention_ms: i64) -> u64 {
        let mut recent = self.recent.lock().unwrap();
        let mut pruned = 0u64;
        recent.retain(|_, bucket| {
            let before = bucket.len();
            bucket
                .retain(|_, (recorded_at, _)| now_ms.saturating_sub(*recorded_at) <= retention_ms);
            pruned += (before - bucket.len()) as u64;
            !bucket.is_empty()
        });
        pruned
    }

    /// Drop every `recent` entry for `node`'s incarnation at or below
    /// `acked_through` (plan 30 §M2 GC: a request's `acked_through` is
    /// the highest contiguous seq of its own incarnation whose reply the
    /// requester has already received, so the holder no longer needs to
    /// keep those outcomes around for a retry).
    pub fn forget_acked_through(&self, node: u64, incarnation: u32, acked_through: u64) {
        let mut recent = self.recent.lock().unwrap();
        let key = (node, incarnation);
        let now_empty = if let Some(inner) = recent.get_mut(&key) {
            *inner = inner.split_off(&(acked_through + 1));
            inner.is_empty()
        } else {
            false
        };
        if now_empty {
            recent.remove(&key);
        }
    }

    /// Remove every `completed` row whose value (recorded at apply time)
    /// is older than `retention_ms`. Returns the number of rows removed.
    /// `now_ms` is the caller's clock (unit-testable without sleeping).
    pub fn prune_completed(&self, now_ms: i64, retention_ms: i64) -> Result<u64, MetaError> {
        let mut tx = self.db.write_tx();
        let stale: Vec<Vec<u8>> = tx
            .iter(&self.completed)
            .map(|g| g.into_inner())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|(k, v)| {
                let recorded_at = i64::from_be_bytes(v.get(8..16)?.try_into().ok()?);
                (now_ms.saturating_sub(recorded_at) > retention_ms).then(|| k.to_vec())
            })
            .collect();
        let n = stale.len() as u64;
        for k in stale {
            tx.remove(&self.completed, k);
        }
        tx.commit()?;
        Ok(n)
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

    /// The journal position the last applied or shipped segment reached
    /// (`KV_APPLIED_POS`), if any was recorded.
    pub fn applied_pos(&self) -> Option<crate::session::JournalPos> {
        let r = self.db.read_tx();
        applied_pos_at(&r, &self.local)
    }

    /// Record `pos` as the applied journal position, if it is past the
    /// recorded one (the holder's own shipped segment: `ack_journal`).
    pub fn note_applied_pos(&self, pos: crate::session::JournalPos) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if applied_pos_at(&tx, &self.local).is_none_or(|cur| cur < pos) {
            kv_set_tx(
                &mut tx,
                &self.local,
                KV_APPLIED_POS,
                &format!("{}:{}", pos.epoch, pos.jseq),
            );
            tx.commit()?;
        }
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

    /// As [`Self::dirty_for_ns`], additionally recording every touched
    /// key's before-image into `capture` (plan 30 §M3a speculation log,
    /// `store::spec`).
    pub(crate) fn dirty_capturing<'a>(&'a self, capture: &'a spec::Capture) -> ns::Dirty<'a> {
        ns::Dirty::capturing(&self.dirty, &self.local, capture)
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

    /// Purge the tombstones of this node's churn keyspaces (EC2 campaign
    /// 5: a daemon that has served a while gets slower and slower).
    ///
    /// Several keyspaces hold only a small live set but see an insert
    /// and a delete for nearly every operation: `dirty` (every namespace
    /// write, cleared by each publish), `pending_upload` (every chunk
    /// written, cleared once uploaded), the journal and its transaction
    /// index (every op, deleted when shipped), the speculation rows, the
    /// `completed` retention. An LSM keeps the tombstones until a
    /// compaction reaches the last level, and the hot paths read these
    /// keyspaces from their start — `has_dirty` and `dirty_snapshot` on
    /// every sync round, the pending-upload view behind every stream and
    /// ship plan, the journal scans behind every ship — so each of them
    /// walked every delete since the keyspace was last compacted: a
    /// root at 125% CPU and turns of a git workload ten times slower an
    /// hour in (the `git-under-flock-rounds` harness scenario profile:
    /// `has_dirty` and `releasable_prefix` in skip-list and merge
    /// iteration). A keyspace whose entry count (tombstones included)
    /// has grown past four times what the previous vacuum left, and past
    /// [`VACUUM_MIN_ENTRIES`], has its memtable flushed and is compacted
    /// to its last level, which drops them. Returns the keyspaces vacuumed.
    pub fn vacuum_churn(&self) -> Result<Vec<&'static str>, MetaError> {
        let churn: [(&'static str, &SingleWriterTxKeyspace); 14] = [
            ("dirty", &self.dirty),
            ("pending_upload", &self.pending_upload),
            ("pending_upload_by_ino", &self.pending_upload_by_ino),
            ("journal", &self.journal_ks),
            ("journal_tx", &self.journal_tx),
            ("spec", &self.spec),
            ("spec_live", &self.spec_live),
            ("pending_replay", &self.pending_replay),
            ("completed", &self.completed),
            ("atime_journal", &self.atime_journal),
            ("backup_tail", &self.backup_tail),
            ("orphans", &self.orphans),
            ("chunk_ref_by_ino", &self.chunk_ref_by_ino),
            ("local", &self.local),
        ];
        let mut done = Vec::new();
        let mut baselines = self
            .vacuum_baselines
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (name, ks) in churn {
            let ks = ks.inner();
            let len = ks.approximate_len();
            let base = baselines.get(name).copied().unwrap_or(0);
            if len < VACUUM_MIN_ENTRIES || len < base.saturating_mul(4) {
                continue;
            }
            ks.rotate_memtable()
                .map_err(|e| MetaError::Invalid(format!("vacuum {name}: {e}")))?;
            ks.major_compact()
                .map_err(|e| MetaError::Invalid(format!("vacuum {name}: {e}")))?;
            baselines.insert(name, ks.approximate_len());
            done.push(name);
        }
        Ok(done)
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

pub(crate) fn applied_pos_at(
    r: &impl Readable,
    local: &SingleWriterTxKeyspace,
) -> Option<crate::session::JournalPos> {
    let raw = kv_get_tx(r, local, KV_APPLIED_POS).ok().flatten()?;
    let (epoch, jseq) = raw.split_once(':')?;
    Some(crate::session::JournalPos {
        epoch: epoch.parse().ok()?,
        jseq: jseq.parse().ok()?,
    })
}

/// `apply_segment_rows`' share of the above, inside its transaction.
pub(crate) fn note_applied_pos_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    pos: crate::session::JournalPos,
) {
    if applied_pos_at(tx, local).is_none_or(|cur| cur < pos) {
        kv_set_tx(
            tx,
            local,
            KV_APPLIED_POS,
            &format!("{}:{}", pos.epoch, pos.jseq),
        );
    }
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
mod usage_tests {
    use super::UsageTracker;

    /// Two deltas that together exceed the i64 range pin the counter at
    /// the bound instead of wrapping it to a plausible wrong value.
    #[test]
    fn usage_adjust_saturates_instead_of_wrapping() {
        let usage = UsageTracker::new(0, 0);
        usage.adjust(i64::MAX, 1);
        usage.adjust(i64::MAX, 1);
        assert_eq!(usage.load(), (i64::MAX as u64, 2));
        usage.adjust(i64::MIN, -1);
        usage.adjust(i64::MIN, -1);
        assert_eq!(usage.load(), (0, 0), "a negative counter reads as zero");
        usage.adjust(-1, 0);
        assert_eq!(usage.load().0, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    /// A truncate cuts the file's content, not just its size: the stored
    /// manifest's `file_len` drops to the new size and the chunks wholly
    /// past it go — on the node that executes the `setattr` and on every
    /// replica that applies its log record — so a later extension (a
    /// size increase, which leaves the manifest alone) cannot bring the
    /// cut bytes back.
    #[test]
    fn a_truncate_cuts_the_manifest_everywhere() {
        use constellation_fs_core::{ChunkHash, ChunkInfo, Manifest};
        let chunks = [(0u64, ChunkHash::of(b"a")), (1, ChunkHash::of(b"b"))]
            .into_iter()
            .collect();
        let manifest = Manifest::from_sparse_chunks(16, 30, chunks, 8, ChunkHash::of)
            .0
            .encode();
        let holder = Meta::open_in_memory().unwrap();
        holder.set_node_prefix(1).unwrap();
        let follower = Meta::open_in_memory().unwrap();
        follower.set_node_prefix(2).unwrap();
        let f = holder.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        holder.set_manifest(f.ino, &manifest, 30).unwrap();
        holder
            .setattr(f.ino, None, None, None, Some(5), None, None)
            .unwrap();
        holder
            .setattr(f.ino, None, None, None, Some(40), None, None)
            .unwrap();
        let records: Vec<crate::LogRecord> = holder
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        follower
            .apply_segment(1, 1, &records, &crate::replay::TouchSet::default())
            .unwrap();
        for (who, m) in [("holder", &holder), ("follower", &follower)] {
            let got = Manifest::decode(&m.manifest(f.ino).unwrap().unwrap()).unwrap();
            assert_eq!(
                got.file_len, 5,
                "{who}: the manifest's content ends at the cut"
            );
            let ChunkInfo::Inline(chunks) = got.chunks else {
                panic!("{who}: inline")
            };
            assert_eq!(chunks.keys().copied().collect::<Vec<_>>(), vec![0], "{who}");
            assert_eq!(
                m.getattr(f.ino).unwrap().unwrap().size,
                40,
                "{who}: the size"
            );
        }
        assert_eq!(crate::replay::clip_manifest(&manifest, 30), None);
    }

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

    /// Plan 30 §M2: the incarnation counter is what keeps a rid unique
    /// across a crash — `next_seq` itself (owned by `ForwardState`/
    /// `SyncHandle`, not `Meta`) restarts at 0 every mount, so the
    /// persisted incarnation must strictly increase, never repeat or
    /// reset, across a simulated "kill9-remount" (re-opening the same
    /// on-disk state and bumping again).
    #[test]
    fn bump_incarnation_survives_a_simulated_restart_and_never_repeats() {
        let dir = tempfile::Builder::new()
            .prefix("constellation-meta-incarnation-")
            .tempdir()
            .unwrap();
        let first = {
            let meta = Meta::open(dir.path()).unwrap();
            let a = meta.bump_incarnation().unwrap();
            let b = meta.bump_incarnation().unwrap();
            assert!(
                b > a,
                "incarnation must strictly increase within one process too"
            );
            b
        };
        // Simulate a crash + remount: fresh `Meta` handle over the same
        // on-disk state.
        let meta = Meta::open(dir.path()).unwrap();
        let after_restart = meta.bump_incarnation().unwrap();
        assert!(
            after_restart > first,
            "incarnation must never repeat or reset across a restart \
             (first={first}, after_restart={after_restart})"
        );
    }

    /// Plan 30 §M2 GC: a `completed` row older than the retention window
    /// is pruned; a fresher one, or one right at the boundary, survives.
    #[test]
    fn prune_completed_removes_only_rows_past_retention() {
        let meta = Meta::open_in_memory().unwrap();
        let old_rid = crate::rid::Rid {
            node: 1,
            incarnation: 1,
            seq: 1,
        };
        let fresh_rid = crate::rid::Rid {
            node: 1,
            incarnation: 1,
            seq: 2,
        };
        let now = 1_000_000i64;
        let retention_ms = 60_000i64;
        {
            let mut tx = meta.db.write_tx();
            tx.insert(
                &meta.completed,
                old_rid.to_key(),
                Meta::encode_completed_row(1, now - retention_ms - 1),
            );
            tx.insert(
                &meta.completed,
                fresh_rid.to_key(),
                Meta::encode_completed_row(2, now - retention_ms + 1),
            );
            tx.commit().unwrap();
        }
        let pruned = meta.prune_completed(now, retention_ms).unwrap();
        assert_eq!(pruned, 1);
        assert!(meta.completed_position(old_rid).unwrap().is_none());
        assert!(meta.completed_position(fresh_rid).unwrap().is_some());
    }

    /// Plan 30 §M2 coordinator review: `remember_outcome` must bound a
    /// `(node, incarnation)` bucket at `MAX_RECENT_PER_INCARNATION`
    /// *independent of whether anything ever acks it* — a requester that
    /// never sends `acked_through` (crashed, or simply idle) must not
    /// let this grow without bound. Oldest (lowest-seq) entries drop
    /// first.
    #[test]
    fn remember_outcome_caps_a_bucket_that_is_never_acked() {
        let meta = Meta::open_in_memory().unwrap();
        let over = MAX_RECENT_PER_INCARNATION as u64 + 10;
        for seq in 0..over {
            let rid = crate::rid::Rid {
                node: 1,
                incarnation: 1,
                seq,
            };
            meta.remember_outcome(
                rid,
                &[crate::record::LogRecord::Unlink {
                    parent: 1,
                    name: format!("f{seq}"),
                    time_ns: 0,
                }],
            );
        }
        // The oldest 10 were dropped to stay at the cap...
        for seq in 0..10 {
            let rid = crate::rid::Rid {
                node: 1,
                incarnation: 1,
                seq,
            };
            assert!(
                meta.recent_outcome(rid).is_none(),
                "seq {seq} should have been evicted"
            );
        }
        // ...but the most recent MAX_RECENT_PER_INCARNATION are intact.
        for seq in (over - MAX_RECENT_PER_INCARNATION as u64)..over {
            let rid = crate::rid::Rid {
                node: 1,
                incarnation: 1,
                seq,
            };
            assert!(
                meta.recent_outcome(rid).is_some(),
                "seq {seq} should still be cached"
            );
        }
    }

    /// A periodic age-based sweep (the other half of the bound) removes
    /// entries older than the retention window across every bucket, not
    /// just the one a request happens to name.
    #[test]
    fn prune_recent_older_than_removes_only_stale_buckets() {
        let meta = Meta::open_in_memory().unwrap();
        let old_rid = crate::rid::Rid {
            node: 1,
            incarnation: 1,
            seq: 1,
        };
        let fresh_rid = crate::rid::Rid {
            node: 2,
            incarnation: 1,
            seq: 1,
        };
        {
            let mut recent = meta.recent.lock().unwrap();
            recent.entry((1, 1)).or_default().insert(
                1,
                (
                    0,
                    vec![crate::record::LogRecord::Unlink {
                        parent: 1,
                        name: "old".into(),
                        time_ns: 0,
                    }],
                ),
            );
            recent.entry((2, 1)).or_default().insert(
                1,
                (
                    100_000,
                    vec![crate::record::LogRecord::Unlink {
                        parent: 1,
                        name: "fresh".into(),
                        time_ns: 0,
                    }],
                ),
            );
        }
        let pruned = meta.prune_recent_older_than(100_000, 60_000);
        assert_eq!(pruned, 1);
        assert!(meta.recent_outcome(old_rid).is_none());
        assert!(meta.recent_outcome(fresh_rid).is_some());
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

/// [`UnshippedSeqs`]: a key whose rows' seqs are not known (journaled
/// while the durability gate was off): compare the journal tip.
pub(crate) const UNTRACKED: u64 = u64::MAX;

/// Plan 30 §M9, per row: for the keys of the unshipped set, the highest
/// journal seq (an upper bound) of a row that touched them — per dentry,
/// per inode, and per directory over the dentries in it (a `Dir` read).
/// [`UNTRACKED`] is sticky until the journal ships out.
#[derive(Debug, Default)]
pub(crate) struct UnshippedSeqs {
    pub dentries: std::collections::HashMap<(u64, String), u64>,
    pub inos: std::collections::HashMap<u64, u64>,
    pub dirs: std::collections::HashMap<u64, u64>,
}

impl UnshippedSeqs {
    fn note(&mut self, touched: &crate::replay::TouchSet, seq: u64) {
        fn raise(slot: &mut u64, seq: u64) {
            *slot = (*slot).max(seq);
        }
        for (parent, name) in &touched.dentries {
            raise(
                self.dentries.entry((*parent, name.clone())).or_insert(0),
                seq,
            );
            raise(self.dirs.entry(*parent).or_insert(0), seq);
        }
        for ino in &touched.inos {
            raise(self.inos.entry(*ino).or_insert(0), seq);
        }
    }
}

/// The on-disk format marker: no migration, a store of another format
/// is refused at open with a message that says what to do.
#[cfg(test)]
mod format_tests {
    use super::*;

    fn refusal(dir: &Path) -> String {
        match Meta::open(dir) {
            Ok(_) => panic!("an old-format store opened"),
            Err(MetaError::Invalid(msg)) => msg,
            Err(other) => panic!("not a format refusal: {other}"),
        }
    }

    #[test]
    fn a_new_store_is_marked_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        drop(Meta::open(dir.path()).unwrap());
        let meta = Meta::open(dir.path()).unwrap();
        assert_eq!(
            meta.kv_get(KV_FORMAT).unwrap(),
            Some(META_FORMAT.to_string())
        );
    }

    /// A store from before the marker (format 1: `spec` hint rows with a
    /// `seq` floor, which this binary cannot decode) is refused at open,
    /// not when `apply_segment_rows` first reads such a row.
    #[test]
    fn a_store_from_before_the_marker_is_refused_at_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Meta::open(dir.path()).unwrap();
            meta.local.remove(KV_FORMAT).unwrap();
            meta.sync().unwrap();
        }
        let msg = refusal(dir.path());
        assert!(
            msg.contains("on-disk format 1") && msg.contains("no migration"),
            "{msg}"
        );
    }

    #[test]
    fn a_store_of_another_format_is_refused_at_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Meta::open(dir.path()).unwrap();
            meta.kv_set(KV_FORMAT, "3").unwrap();
            meta.sync().unwrap();
        }
        let msg = refusal(dir.path());
        assert!(
            msg.contains(&format!(
                "on-disk format 3, this binary reads only format {META_FORMAT}"
            )),
            "{msg}"
        );
    }
}
