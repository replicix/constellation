//! Metadata log records (DESIGN.md §4): the versioned, append-only op
//! registry. Serialized as postcard in the local journal and in zstd
//! S3 log segments (same codec as P2P gossip envelopes).

use crate::rid::Rid;
use constellation_fs_core::Ino;
use constellation_types::{Code, Rdev};
use serde::{Deserialize, Serialize};

/// One eagerly materialized clone inode.  Inode numbers are allocated by
/// the creator and carried in the single `clone` record so every replica
/// reconstructs exactly the same ordinary subtree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneNode {
    pub parent: Ino,
    pub name: String,
    pub ino: Ino,
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub rdev: Rdev,
    pub target: Option<String>,
    pub manifest: Option<Vec<u8>>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

/// One metadata operation.
///
/// Externally tagged so the same `Serialize` impl works for postcard
/// (non-self-describing). Do not add `skip_serializing_if`: postcard
/// would then mis-align fields on decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRecord {
    Mkdir {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        time_ns: i64,
    },
    Create {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        time_ns: i64,
    },
    Symlink {
        parent: Ino,
        name: String,
        ino: Ino,
        target: String,
        uid: u32,
        gid: u32,
        time_ns: i64,
    },
    Mknod {
        parent: Ino,
        name: String,
        ino: Ino,
        kind: u8,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: Rdev,
        time_ns: i64,
    },
    Link {
        ino: Ino,
        parent: Ino,
        name: String,
        time_ns: i64,
    },
    Unlink {
        parent: Ino,
        name: String,
        time_ns: i64,
    },
    Rmdir {
        parent: Ino,
        name: String,
        time_ns: i64,
    },
    Rename {
        parent: Ino,
        name: String,
        new_parent: Ino,
        new_name: String,
        time_ns: i64,
    },
    Setattr {
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
        time_ns: i64,
    },
    WriteManifest {
        ino: Ino,
        /// Manifest this edit was based on. Reintegration may append
        /// cleanly only when the shared winner still equals this value.
        base_manifest: Option<Vec<u8>>,
        /// Encoded `fs_core::Manifest` bytes.
        manifest: Vec<u8>,
        size: u64,
        /// The file's ctime as the commit leaves it, and the record's
        /// stamp: the file's last change — the writer's last data change
        /// or any change since — never above the sequencer's clock
        /// (`store::writes::commit_ctime`).
        time_ns: i64,
        /// The file's mtime (the writer's, or a time set before the
        /// commit), applied as is.
        mtime_ns: i64,
    },
    SetXattr {
        ino: Ino,
        name: String,
        value: Vec<u8>,
        time_ns: i64,
    },
    RemoveXattr {
        ino: Ino,
        name: String,
        time_ns: i64,
    },
    SnapCreate {
        id: String,
        path: String,
        name: String,
        root_hash: String,
        created_unix_ms: i64,
    },
    SnapDelete {
        id: String,
        path: String,
        name: String,
    },
    Clone {
        source_path: String,
        snapshot: String,
        root_hash: String,
        nodes: Vec<CloneNode>,
    },
    /// Cluster-wide logical byte cap. `None` clears the quota (unlimited).
    /// Journaled on `p0`; replay upserts `kv.quota_max_bytes`.
    SetQuota { max_logical_bytes: Option<u64> },
    /// A read-time access-time bump. Best-effort and order-free: replay
    /// merges with max(), so a duplicate, delayed, or out-of-order
    /// record can never make replicas disagree, and a dropped one only
    /// costs freshness. Deliberately *not* a `Setattr`: it must not
    /// touch ctime and must not participate in conflict detection (see
    /// `TouchSet::add` and `apply_one`).
    Atime {
        ino: Ino,
        /// The access time being claimed.
        atime_ns: i64,
        /// When the emitting node observed the read. Used to drop bumps
        /// that were already in flight when someone set atime
        /// explicitly (the `ctime_ns < time_ns` guard in replay).
        time_ns: i64,
    },
    /// Plan 30 §M2 (RIFL for forwarding): `rid` identifies the op whose
    /// own records this shipped alongside, in the same fjall
    /// transaction. Applying it inserts `rid -> position` into the
    /// node-local, unpublished `completed` keyspace on every replica
    /// that tails it — never the tree, never `dirty`. Touches no
    /// inode/dentry, so it never enters conflict detection (see
    /// `TouchSet::add`), exactly like `Atime`. Plan 30 §M3b: a
    /// transaction's records and its `Completed` always ship in the same
    /// segment (`Meta::whole_tx_prefix`).
    ///
    /// Appended last, like `Atime`: a peer too old to decode it would
    /// simply fail to parse this record, which the plan accepts (plan
    /// 30 explicitly waives wire/on-disk compatibility) but which this
    /// ordering makes moot in the one case that still matters — a
    /// mid-rollout mismatch is out of scope, not a live concern.
    Completed { rid: Rid },
    /// Plan 30 §M13: the holder refused the inbox-submitted op `rid` with
    /// `code`. An inbox op has no reply to carry its refusal, so it rides
    /// the log like a completion; applying it inserts `rid -> refused(code)`
    /// into `completed`, and every dedup site answers the rid with that
    /// code from then on — a refusal is an outcome, never re-evaluated
    /// (see `docs/reference/features/forwarded-mutations.md`, "The inbox").
    /// Touches no inode/dentry (see `TouchSet::add`). The inbox path is
    /// where it started; plan 30 §M9 made every definitive refusal an
    /// outcome, so the holder (`core::holder::record_refusal`) and a
    /// delegate (`core::delegate::record_delegate_refusal`) journal one
    /// for a P2P-forwarded op too, alongside the reply that carries it —
    /// a retry by rid after a takeover finds the outcome in the log.
    ///
    /// Plan 31 §7: `code` is the portable [`Code`], serialized as its own
    /// wire number (never an OS errno), so a refusal journaled on one OS
    /// means the same error when a replica on another tails it.
    Refused { rid: Rid, code: Code },
    /// Plan 30 §M13: position `(n, i)` of requester `node`'s inbox batch
    /// under `epoch` has an outcome (executed, refused or deduplicated)
    /// in this transaction. Every replica keeps the highest such position
    /// per `(epoch, node)` in a node-local watermark that retention never
    /// prunes, so a takeover drain older than the `completed` retention
    /// window still skips what was already answered. Touches nothing.
    InboxAck {
        epoch: u64,
        node: u64,
        n: u64,
        i: u32,
    },
    /// Plan 30 §M9 × §M6: carried by the epoch marker of a sealed
    /// backup's takeover. The successor re-ships the acknowledged tail of
    /// the predecessor's tenure (`prev_epoch`) in its own segments *after*
    /// this marker, so a replica's observation of a position of an older
    /// epoch is not satisfied by the marker itself — it waits until the
    /// successor's journal moves past the marker's `through` (M6's
    /// "an observer of stranded work stops waiting at the marker" holds
    /// for a TTL takeover, whose predecessor's unshipped work is lost,
    /// but not here: that work comes back). Touches nothing.
    TailFollows { prev_epoch: u64 },
    /// `renameat2(RENAME_EXCHANGE)`: the entries `parent/name` and
    /// `new_parent/new_name` swap the inodes they name, atomically — both
    /// must exist, either may be a directory, and nothing is unlinked.
    /// Name-based like `Rename` (the holder validated it: both entries
    /// present, neither directory moved beneath itself), so a replica
    /// swaps whatever the two names hold at its log position; replay skips
    /// it if either is gone (a conflict the log order already decided).
    /// Applying it swaps the two dentries (and their `0x04` reverse
    /// entries), moves a directory's `..` link between the parents when
    /// the kinds differ across directories, touches both parents' mtime
    /// and ctime and both inodes' ctime, all with `max` merges.
    ///
    /// `ino` and `new_ino` are what `name` and `new_name` named before the
    /// swap (plan 45 follow-up): an exchange is its own inverse, so unlike
    /// every other record it is not idempotent by name, and a replica
    /// that finds the two names already swapped (`name` → `new_ino`,
    /// `new_name` → `ino`) has it applied and skips it. The authority
    /// sim's `shared-dir-split+links` seed 498: the root appended a
    /// delegate's exchange on top of its own shadow of the same op and
    /// swapped the names back, then published that. That append now
    /// takes its shadow's place by rid (plan 45 §12 O1,
    /// `Meta::apply_delegate_tx`), which the skip by state could not do:
    /// a second exchange of the pair shadowed in between made the names
    /// look unswapped again. The skip stays as replay's tolerance of an
    /// exchange re-applied over its own effect, the same tolerance every
    /// other record has by name.
    Exchange {
        parent: Ino,
        name: String,
        new_parent: Ino,
        new_name: String,
        time_ns: i64,
        ino: Ino,
        new_ino: Ino,
    },
    /// Plan 32 §0.4: `SnapCreate` plus the row's optional trailing fields
    /// (origin, owning policy, creator, REFER at creation). Writers emit
    /// this; replay still accepts [`LogRecord::SnapCreate`], which a
    /// journal or a log segment written before this change carries.
    ///
    /// The hold is *not* here: a snapshot created held journals this and
    /// a [`LogRecord::SnapHold`] in the same transaction, so hold state
    /// has exactly one record shape however it was set.
    ///
    /// Appended at the end of the enum, as the postcard ordering comment
    /// on this type requires.
    SnapCreate2 {
        id: String,
        path: String,
        name: String,
        root_hash: String,
        created_unix_ms: i64,
        origin: u8,
        policy_ino: u64,
        creator: u64,
        refer_bytes: Option<u64>,
    },
    /// Plan 32 §0.4: the snapshot's retention hold was set or released.
    /// `by` is the owner namespace (`user:`/`csi:`; `policy:` reserved),
    /// `None` for a plain hold and always `None` on a release (an unheld
    /// snapshot has no owner). Replay rewrites the row's `held`/`held_by`
    /// fields and touches nothing else; a record for a snapshot that no
    /// longer has a row is a no-op (the delete won the log order).
    SnapHold {
        id: String,
        held: bool,
        by: Option<String>,
    },
    /// Plan 43 C1-3 (§5.8, Q1): the operator abandoned the group of epoch
    /// `epoch` (`gen` 0: the root group; C2 uses the other generations):
    /// the claimant that took the lease went on without that group's tail,
    /// because no member of it could be reached. Carried by that claimant's
    /// epoch marker instead of [`LogRecord::TailFollows`]; the log's
    /// permanent record that rows the group may have acknowledged are not
    /// re-shipped by this tenure (their clients' retries by rid are what
    /// remains). `through_known`: the highest journal seq of the group the
    /// claimant knew of (0: none). Touches nothing.
    ///
    /// Appended at the end of the enum, as the postcard ordering comment on
    /// this type requires.
    GroupAbandoned {
        epoch: u64,
        gen: u64,
        through_known: u64,
    },
    /// Plan 43 C1-4 (D10): the identity of a re-shipped root transaction —
    /// `(epoch, first)` of the journal that first held it. The first record
    /// of every transaction a takeover re-journals from its predecessor's
    /// tail (`apply_backup_tail`); a transaction that already carries one
    /// (re-shipped twice, A to B to C) keeps A's, so the identity is
    /// transitive by construction. It travels with the records (to backups,
    /// into segments, into the archive), which is what lets a member copy
    /// of `(epoch, jseq)` retire once the log carries it, a requester's or
    /// subscriber's committed entry retire instead of rolling back, and the
    /// oracle judge a re-shipped transaction by its origin. Touches nothing.
    ///
    /// Appended at the end of the enum, as the postcard ordering comment on
    /// this type requires (no envelope or `BackupTx` field: a trailing
    /// field would make every earlier segment undecodable).
    TailOf { epoch: u64, jseq: u64 },
    /// Plan 43 C1-4 (D10, Q11): the marker of a takeover that re-ships the
    /// predecessor's tail, with the extent of what it holds: the sealed
    /// tail's contiguous prefix reaches journal seq `through`. A member copy
    /// of `prev_epoch` past `through` was never committed (the sealer's
    /// write-all hold did not reach it) and is dropped. Replaces
    /// [`LogRecord::TailFollows`] for everything written from this change
    /// on; `TailFollows` stays decodable and keeps its meaning for logs
    /// written before (`through` unknown: nothing is dropped by it).
    TailFollowsThrough { prev_epoch: u64, through: u64 },
    /// ADR-59 part B (D1): the decision of `repair drop-held` for inode `ino`,
    /// whose lost `chunks` made the held transactions of `rolled_back`
    /// undeliverable: each is named by the identity a member keys a
    /// transaction by ([`LogRecord::TailOf`]'s pair if it began with one,
    /// else its tenure's `(epoch, first journal seq)`) and by the rid it
    /// completes, if it completes one. The drop decides nothing locally: it
    /// journals this record (an Archive-class row with no keys, which ships
    /// ahead of everything the seed holds) and changes no other state; every
    /// effect is the consequence of the record being in the log, the
    /// dropper's own included (it applies the drop when the archive covers
    /// the record).
    ///
    /// A replica that applies it from the log records the identities as
    /// *retired* (no path puts a retired row into the log: the reship gate
    /// and the kept merge skip it), replaces its member-copy row and its
    /// backup-tail transaction of each with a hole so that no successor
    /// re-ships a dropped row, and (from a committed stream) rolls back what
    /// it was streamed of them without queueing a replay: the dropper's
    /// queue owns the refusal or the replay. Touches nothing; an identity
    /// nobody holds is a no-op.
    ///
    /// Appended at the end of the enum, as the postcard ordering comment on
    /// this type requires.
    ///
    /// `fills` are the delegation stream positions `(gen, idx)` of the
    /// rolled-back transactions that had one: the record is the log's row
    /// at each of them, a no-op row of that stream. A stream's index order
    /// is contiguous in the log (ADR-55), and the drop keeps the later rows
    /// of the stream it does not roll back (redone in place, acknowledged:
    /// I5); without the fills they would land past a hole the successor's
    /// cursor waits at for good. Every reader of a stream's position in the
    /// log counts them ([`LogRecord::stream_fills`]), each at its place in
    /// the stream's order: once the stream reaches the index below it (a row
    /// the drop redid below a fill lands after the record).
    ///
    /// `queued` are the rids of the queued replays of the dropper's (rows an
    /// earlier deposition took out of its journal) that the drop refuses:
    /// there is no transaction to retire, only the rid.
    DropHeld {
        ino: Ino,
        chunks: Vec<replicix_chunkfmt::ChunkHash>,
        rolled_back: Vec<((u64, u64), Option<Rid>)>,
        fills: Vec<(u64, u64)>,
        queued: Vec<Rid>,
    },
    /// Plan 46 M1.2 step 8a (§5.2): the root grants `node` the subtrees
    /// `dirs` under generation `gen`: the generation opens with its first
    /// record, and a later record of the same `(node, gen)` *grows* the
    /// live stream's set (the node has one stream, one group, one cut). The
    /// replicated delegation table is the fold of these records (see
    /// `crate::delegation`); a commit carries it and a bootstrapped replica
    /// learns it. A delegate learns a growth from its table, never from a
    /// message. Touches no inode or dentry.
    DelegateNode {
        node: u64,
        gen: u64,
        dirs: Vec<SubtreeGrant>,
    },
    /// A row of the delegate's own stream (§5.2): the holder of `(node,
    /// gen)` releases the subtree `dir`; `at` is the last index of the
    /// stream that touched it, so the root sequences ops in `dir` only
    /// after appending through `at`. The rest of the node's set stays
    /// delegated. Touches no inode or dentry.
    SubtreeReleased {
        node: u64,
        gen: u64,
        dir: u64,
        at: u64,
    },
    /// A row of the delegate's own stream: the holder releases every
    /// subtree of `(node, gen)` (unmount, suspend, an explicit release);
    /// `through` is the stream's last index. The generation itself ends
    /// with the root's [`LogRecord::DelegateNodeEnded`]. Touches nothing.
    StreamReleased { node: u64, gen: u64, through: u64 },
    /// Plan 43 C2 under one stream: the configuration of `(node, gen)` at
    /// `version` (strictly increasing per generation from 1; no record is
    /// version 0, no backup: the group is the delegate and the root).
    /// Journaled by the root only, while the generation lives, when the
    /// delegate asks for a different backup. A delegate counts a backup's
    /// acknowledgements only once its own replica's table records this
    /// version (vertical Paxos). Touches no inode or dentry.
    DelegateNodeGroup {
        node: u64,
        gen: u64,
        version: u64,
        backups: Vec<u64>,
    },
    /// Plan 43 C2 under one stream: generation `gen` of `node` ended and
    /// its stream is cut at `through`: the root had appended the
    /// generation's rows `1..=through` before this record (its cursor, or
    /// further when the root held the generation itself), and every row of
    /// the generation past it is void: never in the log, never to be
    /// acknowledged. Every replica reads the cut from the record. Touches
    /// no inode or dentry.
    DelegateNodeEnded { node: u64, gen: u64, through: u64 },
}

/// What a granted subtree is (§5.2): `Placed` is the placement policy's
/// (TTL-ended); `Sticky` a grant that never ends by TTL (§5.4): it ends by
/// the holder's release or an operator action; `Designated` an
/// offline designation (plans 03-05), never
/// recalled by TTL or placement, only by `online`; a cross-subtree op
/// touching it is refused (`EXDEV`). `Sticky` and `Designated` are
/// non-stealable ([`SubtreeKind::non_stealable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SubtreeKind {
    Placed,
    Sticky,
    Designated,
}

impl SubtreeKind {
    /// The root cannot take the grant back by itself: no TTL end, no
    /// placement recall, `EROFS` at its root for anyone else and `EXDEV`
    /// across its boundary while the holder is out of reach. A stream is
    /// `Final` only when every grant it holds is.
    pub fn non_stealable(self) -> bool {
        matches!(self, SubtreeKind::Sticky | SubtreeKind::Designated)
    }
}

/// One subtree of a [`LogRecord::DelegateNode`] grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubtreeGrant {
    pub dir: u64,
    pub kind: SubtreeKind,
}

impl LogRecord {
    /// A `DelegateNode` of one subtree: what the core writes until step 9
    /// grows a stream in place (one directory per grant).
    pub fn delegate_one(node: u64, gen: u64, dir: u64, kind: SubtreeKind) -> Self {
        LogRecord::DelegateNode {
            node,
            gen,
            dirs: vec![SubtreeGrant { dir, kind }],
        }
    }

    /// ADR-59 part B (drop contiguity): the delegation stream positions
    /// `(gen, idx)` this record is the log's no-op row at (a `DropHeld`'s
    /// `fills`); empty for every other record. A replica that holds a
    /// segment counts them with the segment's own origins.
    pub fn stream_fills(&self) -> &[(u64, u64)] {
        match self {
            LogRecord::DropHeld { fills, .. } => fills,
            _ => &[],
        }
    }

    /// The generation a delegation record of this kind names, with its
    /// node (`None`: not a delegation record).
    pub fn delegation_stream(&self) -> Option<(u64, u64)> {
        match self {
            LogRecord::DelegateNode { node, gen, .. }
            | LogRecord::SubtreeReleased { node, gen, .. }
            | LogRecord::StreamReleased { node, gen, .. }
            | LogRecord::DelegateNodeGroup { node, gen, .. }
            | LogRecord::DelegateNodeEnded { node, gen, .. } => Some((*node, *gen)),
            _ => None,
        }
    }
}

impl LogRecord {
    /// Plan 30 §M12: the HLC stamp the record carries, if any.
    pub fn stamp_ns(&self) -> Option<i64> {
        match self {
            LogRecord::Mkdir { time_ns, .. } => Some(*time_ns),
            LogRecord::Create { time_ns, .. } => Some(*time_ns),
            LogRecord::Symlink { time_ns, .. } => Some(*time_ns),
            LogRecord::Mknod { time_ns, .. } => Some(*time_ns),
            LogRecord::Link { time_ns, .. } => Some(*time_ns),
            LogRecord::Unlink { time_ns, .. } => Some(*time_ns),
            LogRecord::Rmdir { time_ns, .. } => Some(*time_ns),
            LogRecord::Rename { time_ns, .. } => Some(*time_ns),
            LogRecord::Exchange { time_ns, .. } => Some(*time_ns),
            LogRecord::Setattr { time_ns, .. } => Some(*time_ns),
            LogRecord::WriteManifest { time_ns, .. } => Some(*time_ns),
            LogRecord::SetXattr { time_ns, .. } => Some(*time_ns),
            LogRecord::RemoveXattr { time_ns, .. } => Some(*time_ns),
            _ => None,
        }
    }
}

/// Decode a whole value from `bytes`, refusing leftover bytes. A file
/// system written before plan 46 M1.2 step 1 (whose `DelegateNode` record and
/// delegation row carried a range) must be reformatted; this makes a record
/// or row of the old shape fail loudly where it is detectable, instead of
/// decoding into the wrong op with its tail ignored.
pub fn from_bytes_exact<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, postcard::Error> {
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if rest.is_empty() {
        Ok(value)
    } else {
        Err(postcard::Error::SerdeDeCustom)
    }
}

impl LogRecord {
    /// Encode for the local journal BLOB and for S3 segment payloads.
    pub fn to_postcard(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Decode a journal / segment record.
    pub fn from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        from_bytes_exact(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `DelegateNode` record followed by leftover bytes (a record of an
    /// older shape): the decoder refuses it instead of decoding it with the
    /// tail ignored. File systems written before M1.2 are reformatted.
    #[test]
    fn a_record_with_leftover_bytes_is_refused() {
        let new = LogRecord::delegate_one(2, 5, 7, SubtreeKind::Placed);
        let new_bytes = new.to_postcard().unwrap();
        assert_eq!(LogRecord::from_postcard(&new_bytes).unwrap(), new);
        let mut old_bytes = new_bytes.clone();
        old_bytes.extend_from_slice(&[0, 0]);
        assert!(LogRecord::from_postcard(&old_bytes).is_err());
        // Inside a backup transaction (a journal row's records): the old
        // record shifts the rest of the stream, which must not decode.
        let tx = crate::store::backup::BackupTx {
            first: 1,
            last: 1,
            records: vec![new],
            origin: (0, 0),
        };
        let tx_bytes = postcard::to_allocvec(&tx).unwrap();
        assert_eq!(
            from_bytes_exact::<crate::store::backup::BackupTx>(&tx_bytes).unwrap(),
            tx
        );
        let at = tx_bytes
            .windows(new_bytes.len())
            .position(|w| w == new_bytes)
            .unwrap()
            + new_bytes.len();
        let mut old_tx = tx_bytes.clone();
        old_tx.splice(at..at, [0, 0]);
        assert!(from_bytes_exact::<crate::store::backup::BackupTx>(&old_tx).is_err());
    }

    #[test]
    fn postcard_roundtrip() {
        let r = LogRecord::Create {
            parent: 1,
            name: "hello.txt".into(),
            ino: 42,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            time_ns: 123,
        };
        let bytes = r.to_postcard().unwrap();
        assert_eq!(LogRecord::from_postcard(&bytes).unwrap(), r);
    }

    /// M1.2 step 8a: the §5.2 records round-trip, a grant carries several
    /// subtrees with their kinds, and each refuses a trailing byte.
    #[test]
    fn the_node_stream_records_round_trip() {
        let recs = [
            LogRecord::DelegateNode {
                node: 2,
                gen: 5,
                dirs: vec![
                    SubtreeGrant {
                        dir: 7,
                        kind: SubtreeKind::Placed,
                    },
                    SubtreeGrant {
                        dir: 9,
                        kind: SubtreeKind::Designated,
                    },
                    SubtreeGrant {
                        dir: 11,
                        kind: SubtreeKind::Sticky,
                    },
                ],
            },
            LogRecord::SubtreeReleased {
                node: 2,
                gen: 5,
                dir: 7,
                at: 11,
            },
            LogRecord::StreamReleased {
                node: 2,
                gen: 5,
                through: 12,
            },
            LogRecord::DelegateNodeGroup {
                node: 2,
                gen: 5,
                version: 1,
                backups: vec![3],
            },
            LogRecord::DelegateNodeEnded {
                node: 2,
                gen: 5,
                through: 12,
            },
        ];
        for r in recs {
            let mut bytes = r.to_postcard().unwrap();
            assert_eq!(LogRecord::from_postcard(&bytes).unwrap(), r);
            bytes.push(0);
            assert!(LogRecord::from_postcard(&bytes).is_err());
        }
    }

    #[test]
    fn postcard_write_manifest_keeps_binary() {
        let manifest = b"CMF1\0\0@\0".to_vec();
        let r = LogRecord::WriteManifest {
            ino: 7,
            base_manifest: None,
            manifest: manifest.clone(),
            size: 99,
            time_ns: 1,
            mtime_ns: 1,
        };
        let bytes = r.to_postcard().unwrap();
        // Binary payload must appear verbatim, not as a JSON number list.
        assert!(bytes.windows(manifest.len()).any(|w| w == manifest));
        assert_eq!(LogRecord::from_postcard(&bytes).unwrap(), r);
    }
}
