//! The node-local speculation log (plan 30 §M3a, the requester side;
//! §M3b, the holder side).
//!
//! # Why
//!
//! Every effect in `ns` should be either a prefix of the durable log or
//! something this node can take back. Before M3a a requester applied a
//! forwarded op's accepted records straight into `ns` and kept a
//! "shadow" row that retired only when identical records arrived from
//! the log. When the holder died before shipping, those records never
//! arrived, and the effect stayed in `ns` for good — published into the
//! commit chain, validated against by the next holder, copied to other
//! nodes by the `Exists` hint (plan 30 §1.1, bug B). A holder had the
//! same shape: its unshipped journal was in `ns` too, and a deposed
//! holder kept it until an operator reintegrated.
//!
//! This module makes that state explicit. Anything written to `ns` ahead
//! of the log goes through a *capture context* (`ns::Dirty::capturing`):
//! every `ns` key the write touches has its before-image recorded, and
//! the before-images, the records and the usage delta land as one row
//! here, in the same fjall transaction as the write itself. A crash
//! therefore never leaves an effect without the means to undo it.
//!
//! # Entries
//!
//! `spec` holds rows `spec_seq (8 BE) -> postcard(SpecRow)`, one per
//! captured application, in application order:
//! - [`SpecKind::Shadow`] — a forwarded op's accepted reply
//!   (`Meta::install_shadow`). It carries the op and its rid, so a
//!   stranded shadow can be replayed by rid.
//! - [`SpecKind::Hint`] — the plan 29 M6 `Exists` install
//!   (`Meta::install_hint`): the entry a holder's `EEXIST` refusal was
//!   about, applied early so the caller's next lookup finds it.
//! - [`SpecKind::Local`] — plan 30 §M3b: one of this node's own journaled
//!   transactions, captured while it holds the lease (see `store::local`).
//!   Its records are the journal rows themselves; its rid and op live in
//!   that transaction's `journal_tx` row.
//! - [`SpecKind::Foreign`] — a tailed log segment applied while an older
//!   entry was still outstanding. It is durable content, captured only so
//!   a rollback of that older entry can undo and then redo it. A `Local`
//!   row that ships while something older is still outstanding becomes
//!   one of these too.
//!
//! `spec_live` indexes the *outstanding* shadow and hint entries by the
//! same `spec_seq`. It is tiny (normally empty), so every hot-path
//! question — "is anything speculative?", "what does this segment
//! strand?" — is answered from it without decoding a row. Outstanding
//! `Local` entries are indexed by `journal_tx` instead (a captured
//! transaction's row names its `spec_seq`), because their lifetime is
//! exactly their journal rows'.
//!
//! `pending_replay` holds stranded ops awaiting replay by rid, keyed by
//! the stranded row's *origin* — the `spec_seq` it was first recorded
//! under — so the queue keeps original order even across the renumbering
//! a segment inserted before `Local` rows causes (see "Lifecycle").
//!
//! # Lifecycle
//!
//! - **Retirement.** A shadow retires when a tailed segment carries its
//!   `Completed { rid }`; a hint when the applied log holds everything
//!   its refusal was read at (the reply's position — not just the next
//!   segment, which need not carry the entry), or changes its entry
//!   after that; a `Local` entry when its journal rows ship. Retiring
//!   only drops the index entry. The row stays while anything older is
//!   outstanding, because rolling that older entry back unwinds
//!   everything after it too.
//! - **Compaction.** Every row older than the oldest outstanding entry is
//!   deleted (all rows, once nothing is outstanding): nothing can ever roll
//!   back past that point again.
//! - **Stranding.** Applying a segment whose epoch is higher than an
//!   outstanding entry's strands it: the holder that accepted it has been
//!   superseded, and the fencing rule guarantees its epoch can no longer
//!   add to the log. A takeover strands everything below the new epoch
//!   the same way (`Meta::strand_below_epoch`), and so does a holder
//!   learning it was deposed. A `Local` entry of epoch 0 (written while
//!   holding no lease) never strands by epoch.
//! - **Recovery** (`rewind_tx`), in the stranding transaction: restore
//!   before-images in reverse `spec_seq` order down to the earliest
//!   stranded row (usage deltas too), then walk the same rows forward:
//!   stranded shadows and `Local` transactions move to `pending_replay`
//!   (a `Local` one's journal rows are deleted, and its rid's `completed`
//!   row with them: it never took effect), stranded hints are dropped,
//!   retired speculation is dropped (its effect is carried by the later
//!   `Foreign` row that retired it), and everything else — foreign
//!   segments and still-outstanding speculation — is re-applied from its
//!   records under a fresh capture, so its row describes the state it now
//!   sits on. The caller then replays the queued ops by rid
//!   (`cli::recovery`).
//! - **A segment tailed under `Local` speculation** belongs *before* it:
//!   this node's unshipped transactions can only ship after anything it
//!   tails. So instead of capturing the segment at the end of the log,
//!   `Meta::apply_segment` rolls back from the oldest outstanding `Local`
//!   row, applies the segment (a fresh `Foreign` row), and redoes the
//!   rolled-back rows on top under fresh `spec_seq`s. Redoing a `Local`
//!   row re-applies its journal records through the replay path — exactly
//!   what every other replica will compute when that transaction ships
//!   after the segment. This replaces the pre-M3b "skip foreign records
//!   that touch pending local work" rule for captured transactions.
//!
//! # What a rollback restores
//!
//! Only `ns` is captured. The indexes derived from it — `chunk_ref`/
//! `chunk_ref_by_ino` (chunk GC's liveness set), `xattr_by_name` and
//! `orphans` — are brought back in line from each restored inode or
//! xattr key's old and new value ([`restore_key_tx`]), and the redo
//! re-derives them through the ordinary apply path. `atime` is left
//! alone: it is node-local, never published, and a stale row for an ino
//! that no longer exists is harmless. `completed` is never written by
//! requester speculation (`replay::ApplyCx::durable`); a `Local`
//! transaction does write its rid's row (it executed here), and stranding
//! it deletes that row, along with the holder's in-memory `recent` answer.

use crate::error::MetaError;
use crate::locks::LockTag;
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::replay::{apply_batch_tx, apply_record, ApplyCx, TouchSet};
use crate::rid::Rid;
use crate::session::{JournalPos, Position, Streams};
use crate::store::local::{self, LogPrefixView, RowCommit};
use crate::store::{
    adjust_usage_tx, counter_add_tx, counter_get, counter_set_tx, kv_get_tx, kv_set_tx, misc, ns,
    Meta, UsageTracker, KV_APPLIED_SEQ, KV_DELEG_SPEC_COUNT, KV_LOCAL_SPEC_COUNT, KV_NEXT_SPEC_SEQ,
    KV_NODE_PREFIX, KV_PENDING_REPLAY_COUNT, KV_PENDING_REPLAY_GEN, KV_SPEC_FLOOR,
    KV_SPEC_LIVE_COUNT, KV_UNCAPTURED_TX_COUNT,
};
use constellation_mtree::keys;
use constellation_mtree::record::{InodeRecord, Payload};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

/// The incarnation of the rid a stranded `Local` transaction without one
/// of its own (a local manifest commit, a snapshot row, a conflict copy)
/// is replayed under: `Rid { node: this node, incarnation: this, seq: the
/// transaction's first journal seq }`. Journal seqs are never reused on a
/// replica, and `u32::MAX` is taken by `cli::forward`'s system rids, so
/// this collides with nothing a mount allocates.
pub const LOCAL_REPLAY_INCARNATION: u32 = u32::MAX - 1;

/// Plan 43 C1-4: the incarnation of the rid an abandoned group's member
/// copy queues a transaction that completes no rid under (`GroupAbandoned`,
/// `queue_abandoned_tx`): `Rid { node: ABANDON_REPLAY_NODE | epoch,
/// incarnation: this, seq: jseq }` of the transaction's identity — the
/// `(epoch, first)` of the journal that first held it, one per transaction
/// across the group, so that every member queues the same rid. Below
/// `LOCAL_REPLAY_INCARNATION`, far above any mount's.
pub const ABANDON_REPLAY_INCARNATION: u32 = u32::MAX - 2;

/// The node part of such a rid is the identity's epoch with the top bit set:
/// never a node id (node ids are inode-number prefixes, below 2^24), so no
/// node takes it for one of its own requests.
const ABANDON_REPLAY_NODE: u64 = 1 << 63;

// ---------------------------------------------------------------- types

/// What one `spec` row is. See the module doc.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpecKind {
    /// A forwarded op's accepted reply, installed ahead of the log.
    /// `epoch` is the accepting holder's; `op` is what a replay re-sends.
    /// Plan 30 §M11: `gen` is the accepting delegate's generation (0: the
    /// root's); the log's `DelegateNodeEnded` of it strands the shadow.
    Shadow {
        rid: Rid,
        epoch: u64,
        op: MutateOp,
        gen: u64,
        /// Plan 30 §M14 phase 2: the op's fencing token, which a replay
        /// carries.
        lock_tag: LockTag,
        /// Plan 43 C1-4: the accepting holder answered it as committed
        /// (`MutateReply.committed`: its group or the archive held the row
        /// when it answered). A committed shadow is not stranded by a
        /// takeover of its epoch: the successor re-ships the row, and the
        /// shadow retires by the segment that completes its rid; only an
        /// operator's abandon of the group strands it. A delegate's
        /// committed answer (`gen` != 0, its generation's group held the
        /// row) is the same: the delegate re-streams the row to the next
        /// root, and only the end of the generation strands the shadow.
        gated: bool,
        /// Plan 46 M0.2 (R7, the drop-held proposal's variant 3 (i)): the
        /// answer was a non-stealable sequencer's — an epoch's hold owner
        /// (this node is a member), or a designee — uncommitted by design.
        /// Its owner's journal carries the row, and the owner's own queue
        /// is the one copy that executes it after the hold; a copy queued
        /// here as well by the next epoch's strand would be a second. So
        /// the shadow is not stranded by an epoch: it retires by the
        /// `Completed`/`Refused` of its rid that comes from the log.
        /// `Some(wall ms)` it was installed; `Meta::strand_stale_shadows`
        /// strands it once that is older than the bound (its owner's journal
        /// may be lost, its owner retired). (Plan 46 M1.4 removes this state:
        /// the hold's answers wait for a member.)
        non_stealable: Option<i64>,
    },
    /// The `Exists` early install: retires once the applied state
    /// reaches `at`, the position the refusal was evaluated at
    /// ([`hint_reached`]); strands if a segment above `epoch` arrives
    /// first (or, plan 30 §M11, the log recalls the answering delegate's
    /// generation `gen`).
    Hint {
        at: Position,
        epoch: u64,
        gen: u64,
        /// Plan 43 C1-4: see [`SpecKind::Shadow::gated`].
        gated: bool,
    },
    /// Plan 30 §M9: one of the holder's journal transactions (rows
    /// `first..=last` of its tenure at `epoch`), streamed to this
    /// subscriber once its backups held it, ahead of the log. Retires
    /// when a segment of `epoch` carries row `last` (or ships through
    /// it); strands if a segment above `epoch` arrives first — the
    /// tenure ended without shipping it, and its successor re-ships
    /// what its backup held.
    Streamed {
        epoch: u64,
        first: u64,
        last: u64,
        /// The node whose stream carried it (the holder, or an epoch's
        /// hold owner): the journal it stands for is that node's. A
        /// stranded foreign row queues its replay under it
        /// ([`StrandedOp::source`]).
        source: u64,
        /// Plan 43 C1-4: the holder streamed it as committed
        /// (`StreamAhead.committed`: a backup-held row, not an epoch hold's
        /// tip). Not stranded by a takeover of its epoch; retires when a
        /// segment carries the row (or, re-shipped, its `TailOf`).
        gated: bool,
        /// Plan 30 §M9: this node's own op. A requester subscribed to the
        /// holder's stream sees its op twice — on the stream and in its
        /// reply — and keeps *one* entry, this kind, whichever arrived
        /// first (`install_shadow` adopts a streamed entry; a shadow that
        /// was first becomes streamed in `install_streamed`). Retired by
        /// the segment's row skip like any streamed transaction, which
        /// is what keeps later streamed rows right; stranded, it replays
        /// by rid as this node's op — a conflict copy on refusal — rather
        /// than as a foreign row.
        own: Option<(Rid, MutateOp, LockTag)>,
    },
    /// A tailed segment applied while older speculation was outstanding
    /// (or a `Local` transaction that shipped while it was).
    Foreign { segment_seq: u64 },
    /// Plan 30 §M3b: this node's own journaled transaction whose rows
    /// start at journal seq `first`, executed under lease `epoch` (0: no
    /// lease). See `store::local`.
    Local { first: u64, epoch: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SpecRow {
    kind: SpecKind,
    /// What a redo re-applies: the records actually handed to the apply
    /// path (a foreign record suppressed by this node's own pending
    /// journal is left out, so the redo cannot resurrect it). Empty for a
    /// `Local` row, whose records are its journal rows.
    records: Vec<LogRecord>,
    /// `(key, value before this row first touched it)`, in touch order.
    before: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// `(bytes, files)` this row moved the usage counters by.
    usage: (i64, i64),
    /// The `spec_seq` this row was first recorded under. Survives the
    /// renumbering a segment inserted before `Local` rows causes, and keys
    /// the row's replay if it strands.
    origin: u64,
}

/// An outstanding entry's retirement/stranding parameters, without its
/// (possibly large) records. `Shadow` and `Hint` are what `spec_live`
/// stores; `Local` is derived from `journal_tx`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LiveEntry {
    Shadow {
        rid: Rid,
        epoch: u64,
        gen: u64,
        gated: bool,
        /// See [`SpecKind::Shadow::non_stealable`].
        non_stealable: Option<i64>,
    },
    Hint {
        at: Position,
        epoch: u64,
        gen: u64,
        gated: bool,
    },
    Streamed {
        epoch: u64,
        /// The transaction's identity: its `TailOf` if it is a re-shipped
        /// one, else `(epoch, first)`.
        ident: (u64, u64),
        last: u64,
        gated: bool,
    },
    /// Plan 30 §M11: `gen` is the delegation stream the transaction
    /// belongs to (0: this node's own row as a holder).
    Local { first: u64, epoch: u64, gen: u64 },
}

impl LiveEntry {
    fn epoch(&self) -> u64 {
        match self {
            LiveEntry::Shadow { epoch, .. }
            | LiveEntry::Hint { epoch, .. }
            | LiveEntry::Streamed { epoch, .. }
            | LiveEntry::Local { epoch, .. } => *epoch,
        }
    }

    /// Whether a segment from, or a takeover at, `epoch` strands this
    /// entry: the epoch that produced it can no longer reach the log. A
    /// `Local` entry written without a lease (epoch 0) is not tied to any
    /// epoch and only ever retires by shipping. Plan 30 §M11: a delegate
    /// stream's row is tied to its generation, not to any epoch — a root
    /// takeover strands nothing of it (the delegate re-streams to the
    /// successor); only the log's `DelegateNodeEnded` of the generation does (or, for
    /// a root's append, a later tenure's segment that lands the row's rids
    /// by another copy: [`superseded_root_appends_tx`]).
    ///
    /// Plan 43 C1-4: not a committed entry (`gated`): a shadow, hint or
    /// streamed transaction the holder answered or streamed as held by its
    /// group or the archive is re-shipped by the successor, so the segment
    /// of a later epoch does not roll it back; it retires by the rows that
    /// carry it ([`LiveEntry::superseded_by_takeover`] is the takeover
    /// gate's own rule, [`LiveEntry::stranded_by_abandon`] the operator's).
    fn stranded_by(&self, epoch: u64) -> bool {
        match self {
            // Plan 46 M0.2 (R7): another node's takeover (a tailed segment
            // of a later epoch) does not strand an uncommitted answer of a
            // non-stealable sequencer: it is its owner's to replay, and the
            // shadow retires by the rid's outcome in the log.
            LiveEntry::Shadow {
                non_stealable: Some(_),
                ..
            } => false,
            _ => self.stranded_by_takeover(epoch),
        }
    }

    /// [`Self::stranded_by`] for this node's own takeover (the gate, a
    /// deposition's recovery, the drain's leftovers): the replica must be
    /// the log prefix before this node sequences, so a non-stealable
    /// shadow is rolled back and replayed like any other (R7 keeps it only
    /// for a takeover by another node: a holder that evaluated new ops on
    /// the phantom effect of an answer its predecessor's lost journal
    /// never carried would log them where the spec refuses them —
    /// `flex-tight` seed 188 under the lease-read knob).
    fn stranded_by_takeover(&self, epoch: u64) -> bool {
        match self {
            LiveEntry::Local { epoch: 0, .. } => false,
            LiveEntry::Local { gen, .. } if *gen != 0 => false,
            entry if entry.gated() => false,
            entry => entry.epoch() < epoch,
        }
    }

    /// Plan 43 C1-4: a committed (answered or streamed as held by the
    /// group or the archive) requester or subscriber entry.
    fn gated(&self) -> bool {
        match self {
            LiveEntry::Shadow { gated, .. }
            | LiveEntry::Hint { gated, .. }
            | LiveEntry::Streamed { gated, .. } => *gated,
            LiveEntry::Local { .. } => false,
        }
    }

    /// Plan 43 C1-4: whether the takeover gate of a node taking the lease
    /// at `epoch` takes this committed entry out: the gate re-applies the
    /// predecessor's tail (which carries the row, write-all), so an entry of
    /// this node's own that applied it already would apply it twice. Rolled
    /// back and not queued ([`Self::stranded_by`] is false for it).
    ///
    /// A shadow of a delegate's committed answer (`gen` != 0) is the
    /// generation's row, not the root tenure's: the tail carries it only if
    /// the predecessor appended it (`carried`, the rids of the tail's
    /// delegate rows). One it does not carry stays: the delegate re-streams
    /// the row to this node, whose append takes the shadow out in its place
    /// (`unwind_shadow_for_append_tx`).
    fn superseded_by_takeover(&self, epoch: u64, carried: &HashSet<Rid>) -> bool {
        match self {
            LiveEntry::Shadow { rid, gen, .. } if *gen != 0 => {
                self.gated() && self.epoch() < epoch && carried.contains(rid)
            }
            _ => self.gated() && self.epoch() < epoch,
        }
    }

    /// Plan 43 C1-4: the operator abandoned the group of root epoch `e`
    /// (`LogRecord::GroupAbandoned`): no successor re-ships its rows, so
    /// even a committed entry of epoch `e` or older strands, and its rid
    /// replays (the only way a committed entry is rolled back).
    fn stranded_by_abandon(&self, e: u64) -> bool {
        match self {
            LiveEntry::Shadow { gen: 0, epoch, .. }
            | LiveEntry::Hint { gen: 0, epoch, .. }
            | LiveEntry::Streamed { epoch, .. } => *epoch <= e,
            _ => false,
        }
    }

    /// Plan 30 §M11: whether the log's recall of `gen` strands this
    /// entry: a delegate's unappended row of that generation, or a
    /// shadow/hint a requester installed from that delegate's reply.
    fn stranded_by_recall(&self, recalled: &HashSet<u64>) -> bool {
        match self {
            LiveEntry::Local { gen, .. }
            | LiveEntry::Shadow { gen, .. }
            | LiveEntry::Hint { gen, .. } => *gen != 0 && recalled.contains(gen),
            LiveEntry::Streamed { .. } => false,
        }
    }
}

/// Why a replay was refused, recorded before the conflict copy is
/// materialized so an interrupted materialization resumes with the same
/// name instead of making a second copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub reason: String,
    pub ts_unix: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct QueuedReplay {
    rid: Rid,
    op: MutateOp,
    refused: Option<Refusal>,
    /// Plan 30 §M9: another node's op, stranded here as pre-S3 streamed
    /// speculation. Queued so reads of its keys wait until the successor
    /// (which re-ships it, or dedups its replay) settles it; a refusal
    /// makes no conflict copy (the op's own requester replays it).
    foreign: bool,
    /// A foreign entry's [`SpecKind::Streamed::source`]: the node whose
    /// unshipped journal it came from. `None` for every other entry.
    source: Option<u64>,
    /// Plan 30 §M11 phase 2b round 2: the delegation generation whose
    /// stream the op's reply named (a shadow accepted by a delegate);
    /// 0: the root's. Held from replay while that generation is live in
    /// the table — the delegate re-streams it to the successor root, or
    /// the generation ends — so a successor never executes it ahead of
    /// the delegate's earlier transactions (long-delegated seed 70075).
    gen: u64,
    /// A size-only `setattr` (the FUSE truncate path, `O_TRUNC`
    /// included): the manifest its inode had just before it, from the
    /// stranded row's before-image (see
    /// [`StrandedOp::conditional_truncate`]).
    truncate_base: Option<Vec<u8>>,
    /// Plan 30 §M14 phase 2: the op's fencing token: the replay carries
    /// it, and is rejected wherever a grant it names has ended.
    lock_tag: LockTag,
    /// Plan 30 §M11: the delegate-stream rows a deposed root's stranded
    /// transaction was ordered after — every row of another generation
    /// its journal held ahead of it, which the successor appends from
    /// the delegate's re-stream, not from this replay
    /// ([`stranded_local_deps`]). The replay carries them as `deps`.
    deps: Streams,
    /// Plan 43 C1-4: the class of the answer the entry stands for: a row
    /// the client was told is committed (`gated`) is queued only by an
    /// operator's abandon of its group; every other entry is the lone
    /// branch's. The simulation's oracle (I2 (b)) judges the former.
    class: RowCommit,
    /// Plan 46 M0.2 (R1): this entry is a placeholder, not a copy of an
    /// op: see [`AwaitRetry`]. Its `op` is inert and never executed.
    await_retry: Option<AwaitRetry>,
    /// The refusal is `repair drop-held`'s own (`refuse_queued_tx`): the
    /// rid's `Refused` row was journaled by the dropper, and is in the log
    /// only if that journal shipped. Every other refusal here is a
    /// sequencer's answer, journaled where it was given.
    dropped: bool,
}

/// Plan 46 M0.2 (R1, the drop-held proposal's option B): what `repair
/// drop-held` leaves in the replay queue for another node's row whose
/// answer was never given. The row is rolled back and the op is *not*
/// queued again: its requester still holds the one copy that may execute
/// (its client entry retries by rid until its deadline), so a second
/// copy here would run twice. The placeholder keeps the row's *place* in
/// the queue, in journal order, with the keys it touched: the drain holds
/// every later entry on those keys behind it, an op arriving here on
/// them waits for it, and its requester's retry executes where the row
/// stood (`authority::core::replay`). A bound settles the rid when the
/// retry never comes (`Core::settle_await_retry`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwaitRetry {
    /// The dentries and inodes the dropped row touched.
    pub keys: TouchSet,
    /// When the row was dropped, in wall-clock ms (what `status.held`
    /// ages it by; the bound itself runs on the core's own clock).
    pub since_ms: i64,
}

/// Plan 43 C1-4: the operator abandoned the group of this member copy's
/// epoch (`LogRecord::GroupAbandoned`): no successor re-ships the row, so it
/// is queued for replay by its rid, as the lone branch would (the only way a
/// committed row is re-executed). `false`: nothing queued — the log carries
/// the rid already, or the row has no op to run.
pub(crate) fn queue_abandoned_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    epoch: u64,
    row: &super::member::MemberTx,
) -> Result<bool, MetaError> {
    // A transaction that completes no rid is queued under its identity
    // (`ABANDON_REPLAY_INCARNATION`): every member of the group that holds
    // it queues the same rid, and the holder runs it once. (Under
    // `replay_rid_for`, the node's own, each member that came back ran it
    // again.)
    let rid = match row.rid {
        Some(rid) => rid,
        None => match row.tx.records.iter().find_map(|r| match r {
            LogRecord::Completed { rid } => Some(*rid),
            _ => None,
        }) {
            Some(rid) => rid,
            None => {
                let (e, jseq) =
                    super::member::tail_identity(&row.tx.records).unwrap_or((epoch, row.tx.first));
                Rid {
                    node: ABANDON_REPLAY_NODE | e,
                    incarnation: ABANDON_REPLAY_INCARNATION,
                    seq: jseq,
                }
            }
        },
    };
    let in_log = tx
        .get(&meta.completed, rid.to_key())?
        .and_then(|v| {
            v.get(0..8)
                .map(|p| u64::from_be_bytes(p.try_into().expect("8 bytes")))
        })
        .is_some_and(|position| position == 0);
    if in_log {
        return Ok(false);
    }
    let op = match &row.op {
        Some(op) => Some(op.clone()),
        None => derive_replay_op(&row.tx.records),
    };
    let Some(op) = op else {
        return Ok(false);
    };
    let key = next_spec_seq_tx(tx, &meta.local)?;
    enqueue_replay_as_tx(
        tx,
        meta,
        key,
        rid,
        op,
        row.lock_tag.clone(),
        None,
        0,
        Streams::NONE,
        &[],
    )?;
    mark_queued_class_tx(tx, meta, key, row.class)?;
    Ok(true)
}

/// A stranded op waiting to be replayed by rid (plan 30 §M3a recovery
/// step 3). `queue_seq` identifies it for [`Meta::forget_replay`]/
/// [`Meta::mark_replay_refused`] and orders the queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrandedOp {
    pub queue_seq: u64,
    pub rid: Rid,
    pub op: MutateOp,
    pub refused: Option<Refusal>,
    /// Plan 30 §M9: see `QueuedReplay::foreign`.
    pub foreign: bool,
    /// See `QueuedReplay::source`.
    pub source: Option<u64>,
    /// Plan 30 §M11 phase 2b round 2: see `QueuedReplay::gen`.
    pub gen: u64,
    /// Plan 30 §M14 phase 2: see `QueuedReplay::lock_tag`.
    pub lock_tag: LockTag,
    /// Plan 30 §M11: see `QueuedReplay::deps`.
    pub deps: Streams,
    /// See `QueuedReplay::truncate_base`.
    pub truncate_base: Option<Vec<u8>>,
    /// See `QueuedReplay::class`.
    pub class: RowCommit,
    /// See [`AwaitRetry`]: set for a placeholder, whose `op` is inert.
    pub await_retry: Option<AwaitRetry>,
    /// See `QueuedReplay::dropped`.
    pub dropped: bool,
}

impl StrandedOp {
    /// Whether this entry is a placeholder ([`AwaitRetry`]): it holds a
    /// place in the queue, and has no op to execute.
    pub fn is_await_retry(&self) -> bool {
        self.await_retry.is_some()
    }

    /// A size-only truncate as a base-checked commit: the manifest its
    /// inode had just before it (`truncate_base`) to the manifest it cut
    /// that to, at its size. The replay drain and the takeover gate send
    /// a truncate folded into the manifest commit queued after it this
    /// way, by its own rid (`authority::core::replay`): it lands where the
    /// file is still what it cut — or already the cut, which the commit
    /// check accepts as done (a predecessor's re-journaled backup tail
    /// carried it) — and is refused where another node wrote the file
    /// since, which it must never cut. The commit after it keeps the base
    /// it was composed on, the cut. `None` when the op is no truncate, the
    /// row had no before-image of the inode, or the truncate cut nothing
    /// (a truncate up: the commit composed on the uncut manifest).
    pub fn conditional_truncate(&self) -> Option<MutateOp> {
        let ino = size_only_truncate(&self.op)?;
        let MutateOp::Setattr {
            size: Some(size), ..
        } = &self.op
        else {
            return None;
        };
        let pre = self.truncate_base.as_ref()?;
        let cut = crate::replay::clip_manifest(pre, *size)?;
        Some(MutateOp::SetManifest {
            ino,
            base_manifest: Some(pre.clone()),
            manifest: cut,
            size: *size,
            mtime_ns: None,
            ctime_ns: None,
        })
    }
}

/// The sync after [`Meta::reship_at_takeover`]'s commit (a test can fail it).
fn reship_sync(meta: &Meta) -> Result<(), MetaError> {
    #[cfg(any(test, feature = "test-util"))]
    if FAIL_RESHIP_SYNC.with(|c| c.replace(false)) {
        return Err(MetaError::Io(std::io::Error::other(
            "injected re-ship sync fault (testing only)",
        )));
    }
    meta.sync()
}

#[cfg(any(test, feature = "test-util"))]
thread_local! {
    static FAIL_RESHIP_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Test hook: the next sync after a re-ship's commit on this thread fails.
#[cfg(any(test, feature = "test-util"))]
pub fn fail_next_reship_sync_on_this_thread_for_tests() {
    FAIL_RESHIP_SYNC.with(|c| c.set(true));
}

/// The strand half of [`Meta::reship_at_takeover`] (see
/// [`Meta::strand_for_takeover_carrying`] for each field).
#[derive(Clone, Copy, Debug)]
pub struct StrandArgs<'a> {
    pub epoch: u64,
    pub own_gens: &'a [u64],
    pub abandoned: Option<u64>,
    pub carried: &'a HashSet<Rid>,
    pub proof: &'a OrphanProof,
}

/// The tail half of [`Meta::reship_at_takeover`]: the sealed tail of
/// `epoch` as this node holds it as the predecessor's backup, its
/// contiguous prefix reaching `through` (the sealer's hold, C1-4 D6), and
/// the member copy's configuration version.
#[derive(Clone, Copy, Debug)]
pub struct TailArgs {
    pub epoch: u64,
    pub through: u64,
    pub config_version: u64,
    /// The commit class the re-journaled rows carry (the successor's
    /// configuration); `None`: the default for an adopted tail
    /// ([`RowCommit::ROOT_GROUP`]).
    pub class: Option<RowCommit>,
}

/// What [`Meta::reship_at_takeover`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReshipReport {
    pub stranded: Stranded,
    /// Tail transactions journaled.
    pub tail_applied: usize,
    /// Transactions past the sealed hold's contiguous prefix, not re-shipped.
    pub tail_beyond_gap: usize,
    /// Transactions whose rid already had an outcome here, skipped.
    pub tail_done: usize,
    /// Rows kept as a member copy of the tail's epoch.
    pub tail_moved: usize,
    /// How long the store's writer lock was held (the transaction's open
    /// to its commit), in milliseconds: the takeover's stall of every
    /// other writer (c3-spec `reship.duration_ms_max`).
    pub hold_ms: u64,
    /// The commit stood, but the `fsync` that follows it (the member
    /// copy's) failed: logged here, counted by the caller. The rows are in
    /// the store; only a power loss before the next sync could drop them.
    pub sync_failed: bool,
}

#[derive(Default)]
struct StrandPart {
    stranded: Stranded,
    orphan_cuts: Vec<(u64, u64)>,
    work: bool,
    /// The generations this strand ended at the gate (marked,
    /// [`Meta::deleg_ended_at_gate`]).
    ended: Vec<u64>,
}

/// What a stranding pass rolled back.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stranded {
    /// Shadows rolled back and queued for replay by rid.
    pub shadows: usize,
    /// Hints rolled back and dropped (an `EEXIST` answer has nothing to
    /// replay; the entry it described never reached the log).
    pub hints: usize,
    /// Plan 30 §M3b: this node's own unshipped transactions rolled back
    /// (their journal rows deleted) and queued for replay by rid — a
    /// deposed holder's journal.
    pub locals: usize,
    /// Plan 43 C1-4: this node's own committed (gated) unshipped
    /// transactions, rolled back and moved to the member copy of their
    /// epoch instead of queued for replay (a deposed group tenure replays
    /// nothing it was told is committed). Not in `locals`.
    pub member_copied: usize,
    /// Plan 43 C1-4: committed requester and subscriber entries a
    /// takeover gate took out because the tail it re-applies carries their
    /// rows; rolled back, not queued. Not in `shadows`/`hints`.
    pub superseded: usize,
    /// Plan 43 C1-4: of `shadows`/`hints`, the entries the holder had
    /// answered or streamed as committed — rolled back only by an operator's
    /// abandon of the group (`speculation.committed_rolled_back`).
    pub committed_rolled_back: usize,
    /// Plan 43 C1-4: gated rows of the root's own journal a deposition
    /// queued for replay (must stay 0; also in `locals`).
    pub gated_queued: usize,
    /// Plan 43 C2 (§3.8): a delegate's gated (uncommitted) rows past the cut
    /// of the generation a recall ended, rolled back and dropped instead of
    /// queued for replay (`speculation.deleg_dropped`). Not in `locals`.
    pub deleg_dropped: usize,
    /// Plan 46 M1.2 step 7 (c3-spec §6.1): a delegate's row this node
    /// appended as the root, rolled back by a deposition and kept as a member
    /// copy of its generation (`backup_tail[DELEG_BACKUP_BASE + gen]`), never
    /// queued. Not in `locals`.
    pub deleg_member_copied: usize,
}

impl Stranded {
    pub fn any(&self) -> bool {
        self.shadows
            + self.hints
            + self.locals
            + self.member_copied
            + self.deleg_member_copied
            + self.superseded
            + self.deleg_dropped
            > 0
    }

    /// Entries rolled back, of any kind.
    pub fn total(&self) -> usize {
        self.shadows
            + self.hints
            + self.locals
            + self.member_copied
            + self.superseded
            + self.deleg_dropped
    }
}

/// ADR-43 (orphan grants): what a takeover's epoch marker proves about the
/// grants the log will never carry ([`Meta::orphaned_tx`]). `upto`: the
/// operator abandoned the root group of that epoch (`GroupAbandoned { gen:
/// 0 }`), so nothing of it or of an older epoch that the log does not carry
/// yet will ever be shipped. `no_tail`: the claimant re-ships nothing of the
/// epoch it took over (`TailFollowsThrough { prev_epoch, through: 0 }`, and
/// no tail announced besides): that epoch's journal was held by no group
/// member past what the log carries (a lone root's, or a sealed hold that
/// reached nothing), its later segments are fenced, and no later tenure
/// re-ships it (a successor re-ships its own predecessor's tail only). Only
/// that epoch: an older one's committed rows may still ride a tail a later
/// claim fetches (plan 43 D2), and its marker said what it carried then.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OrphanProof {
    pub upto: Option<u64>,
    pub no_tail: Option<u64>,
}

impl OrphanProof {
    /// What the marker records `records` prove.
    pub fn of(records: &[LogRecord]) -> Self {
        let upto = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::GroupAbandoned { epoch, gen: 0, .. } => Some(*epoch),
                _ => None,
            })
            .max();
        let tail = records.iter().any(|rec| {
            matches!(
                rec,
                LogRecord::TailFollows { .. } | LogRecord::TailFollowsThrough { through: 1.., .. }
            )
        });
        let no_tail = if tail {
            None
        } else {
            records.iter().find_map(|rec| match rec {
                LogRecord::TailFollowsThrough {
                    prev_epoch,
                    through: 0,
                } => Some(*prev_epoch),
                _ => None,
            })
        };
        OrphanProof { upto, no_tail }
    }

    /// Whether it proves nothing.
    pub fn is_empty(&self) -> bool {
        self.upto.is_none() && self.no_tail.is_none()
    }

    /// Whether it covers grants minted in epoch `e` (never the old mint's 0).
    pub fn covers(&self, e: u64) -> bool {
        e != 0 && (self.upto.is_some_and(|u| e <= u) || self.no_tail == Some(e))
    }
}

/// What [`strand_local_tx`] did with a transaction.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stranding {
    /// Its op (if any) was queued for replay.
    Queued,
    /// A gated row, moved to the member copy of its epoch.
    Member,
    /// Plan 46 M1.2 step 7: a delegate's row this root appended, moved to
    /// the member copy of its generation (`Stranded::deleg_member_copied`).
    DelegMember,
    /// A gated row queued for replay in a pass that must not (counted,
    /// `Stranded::gated_queued`; a debug build asserts).
    GatedQueued,
    /// Plan 43 C2 (§3.8): a delegate's gated row past its generation's cut,
    /// rolled back and not queued (`Stranded::deleg_dropped`).
    Dropped,
}

/// Plan 43 C1-4: how a stranding pass treats the committed work it meets.
#[derive(Clone, Copy)]
struct Strand<'a> {
    /// A gated (committed) unshipped `Local` row of the root's own
    /// journal (`gen` 0) goes to the member copy of its epoch, never to the
    /// replay queue: its client was told it is committed, and the successor
    /// re-ships it from the group.
    member: bool,
    /// Entries (by `spec_seq`) rolled back and not queued: committed
    /// entries a takeover gate's tail re-applies.
    quiet: Option<&'a HashSet<u64>>,
    /// Plan 43 C2 (§3.8): the pass is a generation's recall. A delegate's
    /// gated row (`gen` != 0) it strands is dropped, not replayed: its
    /// client was never answered (the answer waits for the row's commit,
    /// and a row at or below the cut is in the log, not stranded), so it
    /// retries by rid at the generation's new owner. An ungated (`Lone`)
    /// row was answered on the delegate's disk alone and is replayed as
    /// before.
    recall: bool,
    /// L2: the pass is a rebuild of the namespace from the log
    /// (`reset_for_rebuilt_ns_tx`): a deposed node's (`recover_deposed`)
    /// and a replica the log was pruned past (the retention rebuild, jobs.rs
    /// `after_gap_check`: a node behind the log's retained tail is no
    /// current root, so what it still journals is a past tenure's, as in a
    /// deposition). A root's append of a delegate's row (`gen` != 0,
    /// journaled under the root's lease epoch) of a gated class is dropped,
    /// not queued. The class is not the append's ADR-41 commit: a root
    /// journals such an append `Lone`, and it turns gated only when another
    /// node re-asks the rid (the answer is journaled `Archive`) or a
    /// predecessor's tail is adopted at a takeover (`RootGroup`). Either way
    /// another copy exists: the asker retries the rid by itself, the
    /// predecessor's members hold the tail, and the delegate re-streams the
    /// row to the successor root. A queued copy would be a second
    /// executable copy of a rid (I2). A `Lone` append is queued as before.
    /// (A third source of a gated root append is C3's re-append at a
    /// delegate's takeover gate, `RootGroup`; see L2b in `strand_local_tx`.)
    rebuild: bool,
    /// A committed row the pass moves to the member copy holds reads of its
    /// keys until the re-ship lands (`Meta::member_copy_touches`): a
    /// segment's apply, and `strand_below_epoch` (a deposition's recovery
    /// serves reads while its rebuild runs, with a delegate's row this root
    /// appended after the rolled-back one still applied). Not the takeover
    /// gate's strand: its tail re-applies the rows in the same step.
    hold: bool,
    /// Each stranded transaction's `deps` (by journal seq), taken from the
    /// intact journal before the pass strands anything: a pass that
    /// strands every row, oldest first, has deleted the rows ahead of a
    /// transaction by the time it is examined, and
    /// [`stranded_local_deps`] would find none. `None`: read the journal
    /// as it stands.
    deps_of: Option<&'a HashMap<u64, Streams>>,
    /// Seed 2560: this `Local` row is a root's append of a delegate's row
    /// whose outcome a segment of a later tenure carries under another
    /// identity ([`superseded_root_appends_tx`]): rolled back like any
    /// stranded row, then dropped. Set per row by `redo_row_tx` for a
    /// `Local` entry of the pass's `quiet` set.
    superseded: bool,
    /// Plan 46 M1.2 step 7 (c3-spec §6.1): a deposition's pass. A `Local` row
    /// of another node's generation (`gen` != 0, a lease epoch, `gen` not in
    /// this set: the root's append of a delegate's row) is rolled back and kept
    /// as a member copy of that generation, never queued. The set is this
    /// node's own generations as a delegate.
    deleg_copy: Option<&'a HashSet<u64>>,
}

impl Strand<'static> {
    /// Today's stranding: everything stranded is queued for replay.
    const PLAIN: Strand<'static> = Strand {
        member: false,
        quiet: None,
        recall: false,
        rebuild: false,
        hold: false,
        deps_of: None,
        superseded: false,
        deleg_copy: None,
    };
}

/// What [`Meta::apply_segment`] did besides applying the records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmentApplied {
    /// Records skipped (pending-journal conflict or a replay cascade).
    pub skipped: usize,
    /// Speculation this segment stranded before it was applied.
    pub stranded: Stranded,
    /// Outstanding entries this segment retired.
    pub retired: usize,
    /// Plan 43 C1-4: what the segment did to this node's member copies.
    pub member: super::member::MemberOutcome,
    /// Plan 30 §M3b: the segment was inserted before this node's own
    /// outstanding `Local` transactions (rolled back, applied, redone).
    pub inserted_before_local: bool,
}

/// Status counts: outstanding speculative entries and queued replays.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpeculationCounts {
    /// Outstanding shadows and hints (requester speculation).
    pub outstanding: u64,
    pub pending_replay: u64,
    /// Plan 30 §M3b: this node's own captured, unshipped transactions.
    pub local: u64,
}

// -------------------------------------------------------------- capture

/// The capture context `ns::Dirty::capturing` carries: the first
/// before-image of every `ns` key written while it is in use. A
/// `RefCell` because `Dirty` is `Copy` and is handed down every write
/// helper by value; it never crosses threads (one fjall transaction,
/// one call stack).
#[derive(Default)]
pub(crate) struct Capture {
    inner: RefCell<CaptureBuf>,
}

#[derive(Default)]
struct CaptureBuf {
    seen: HashSet<Vec<u8>>,
    before: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl Capture {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn has_seen(&self, key: &[u8]) -> bool {
        self.inner.borrow().seen.contains(key)
    }

    /// Record `key`'s value before the first write to it under this
    /// context. Later writes to the same key are covered by the first
    /// before-image: restoring it undoes all of them at once.
    pub(crate) fn note_before(&self, key: &[u8], before: Option<Vec<u8>>) {
        let mut buf = self.inner.borrow_mut();
        if buf.seen.insert(key.to_vec()) {
            buf.before.push((key.to_vec(), before));
        }
    }

    pub(crate) fn into_before(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.inner.into_inner().before
    }
}

// ------------------------------------------------------------- key/rows

fn seq_key(seq: u64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

fn decode_seq(k: &[u8]) -> Result<u64, MetaError> {
    let bytes: [u8; 8] = k
        .try_into()
        .map_err(|_| MetaError::Invalid("speculation log key".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn next_spec_seq_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let key = KV_NEXT_SPEC_SEQ.as_bytes();
    let next = match tx.get(local, key)? {
        Some(v) => decode_seq(&v)?,
        None => 1,
    };
    tx.insert(local, key.to_vec(), (next + 1).to_be_bytes().to_vec());
    Ok(next)
}

/// Every outstanding entry: shadows and hints from `spec_live`, `Local`
/// transactions from `journal_tx`. Plan 30 §M3b cost: two counter reads
/// when there is none (every follower between forwards, every tailed
/// segment on it), and otherwise proportional to what is outstanding —
/// both scans start at their floors, past the retired history.
fn read_live(r: &impl Readable, meta: &Meta) -> Result<Vec<(u64, LiveEntry)>, MetaError> {
    let mut out = Vec::new();
    if counter_get(r, &meta.local, KV_SPEC_LIVE_COUNT)? > 0 {
        let floor = counter_get(r, &meta.local, KV_SPEC_FLOOR)?;
        for guard in r.range(&meta.spec_live, seq_key(floor)..) {
            let (k, v) = guard.into_inner()?;
            out.push((decode_seq(&k)?, crate::record::from_bytes_exact(&v)?));
        }
    }
    if counter_get(r, &meta.local, KV_LOCAL_SPEC_COUNT)? == 0 {
        return Ok(out);
    }
    for (first, row) in local::read_journal_tx_heads(r, meta)? {
        if let Some(seq) = row.spec_seq {
            out.push((
                seq,
                LiveEntry::Local {
                    first,
                    epoch: row.epoch,
                    gen: row.gen,
                },
            ));
        }
    }
    Ok(out)
}

/// The outstanding shadows, hints and streamed transactions (`spec_live`,
/// normally empty): one counter read when there are none, and never a
/// scan of this node's own `Local` backlog.
fn live_non_local(r: &impl Readable, meta: &Meta) -> Result<Vec<(u64, LiveEntry)>, MetaError> {
    let mut out = Vec::new();
    if counter_get(r, &meta.local, KV_SPEC_LIVE_COUNT)? == 0 {
        return Ok(out);
    }
    let floor = counter_get(r, &meta.local, KV_SPEC_FLOOR)?;
    for guard in r.range(&meta.spec_live, seq_key(floor)..) {
        let (k, v) = guard.into_inner()?;
        out.push((decode_seq(&k)?, crate::record::from_bytes_exact(&v)?));
    }
    Ok(out)
}

/// Plan 30 §M9: the live `Streamed` entry whose records complete `rid`,
/// if any.
fn streamed_entry_completing(
    r: &impl Readable,
    meta: &Meta,
    rid: Rid,
) -> Result<Option<u64>, MetaError> {
    streamed_entry_with(
        r,
        meta,
        |rec| matches!(rec, LogRecord::Completed { rid: c } if *c == rid),
    )
}

/// Plan 30 §M6/§M9: the live `Streamed` entry carrying `rid`'s journaled
/// refusal (`Refused { rid }`), if any: its spec seq.
fn streamed_entry_refusing(
    r: &impl Readable,
    meta: &Meta,
    rid: Rid,
) -> Result<Option<u64>, MetaError> {
    streamed_entry_with(
        r,
        meta,
        |rec| matches!(rec, LogRecord::Refused { rid: c, .. } if *c == rid),
    )
}

/// Whether any live speculation (a shadow, a streamed transaction, a
/// hint, this node's own unretired work) touches a key of `touches`.
fn live_speculation_touches(
    r: &impl Readable,
    meta: &Meta,
    touches: &TouchSet,
) -> Result<bool, MetaError> {
    for (seq, _) in read_live(r, meta)? {
        if let Some(v) = r.get(&meta.spec, seq_key(seq))? {
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            let mut theirs = TouchSet::from_records(row.records.iter());
            // The inodes it changed but names no record by (plan 45
            // review of the follow-ups): a shadowed `unlink a` whose
            // inode the holder had linked to `b` meanwhile orphans the
            // inode here; a hint `Create { b, X }` installed over that
            // wrote a fresh `X` (size 0, no manifest) that reads of `b`
            // were answered from.
            captured_files(&row, &mut theirs);
            if theirs.dentries.iter().any(|d| touches.dentries.contains(d))
                || theirs.inos.iter().any(|i| touches.inos.contains(i))
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Whether a hint's entry (`Meta::entry_as_record`'s create) names an
/// inode `ns` holds while its own name does not name it yet: installing
/// it would replace the inode record (see `Meta::install_hint_from`).
fn hint_names_a_held_inode(
    r: &impl Readable,
    meta: &Meta,
    records: &[LogRecord],
) -> Result<bool, MetaError> {
    for rec in records {
        let (parent, name, ino) = match rec {
            LogRecord::Create {
                parent, name, ino, ..
            }
            | LogRecord::Mkdir {
                parent, name, ino, ..
            }
            | LogRecord::Symlink {
                parent, name, ino, ..
            }
            | LogRecord::Mknod {
                parent, name, ino, ..
            } => (*parent, name.as_str(), *ino),
            _ => continue,
        };
        // An orphan (no name left here, kept for its open handles or
        // its speculation's rollback) is held too.
        if ns::get_inode_any(r, &meta.ns, &meta.orphans, ino)?.is_some()
            && ns::get_dentry_record(r, &meta.ns, parent, name)?.map(|d| d.ino) != Some(ino)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn streamed_entry_with(
    r: &impl Readable,
    meta: &Meta,
    matches: impl Fn(&LogRecord) -> bool,
) -> Result<Option<u64>, MetaError> {
    for (seq, entry) in read_live(r, meta)? {
        if !matches!(entry, LiveEntry::Streamed { .. }) {
            continue;
        }
        if let Some(v) = r.get(&meta.spec, seq_key(seq))? {
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            if row.records.iter().any(&matches) {
                return Ok(Some(seq));
            }
        }
    }
    Ok(None)
}

/// Plan 30 §M9: when the holder's pre-S3 stream installed `rid`'s
/// transaction here already (a live `Streamed` entry completing it),
/// adopt the op as this node's own — stranded by a takeover, it replays
/// by rid as a shadow would — and report `true`: the op's effect is in
/// this replica, in the holder's order. `false`: no such entry.
fn adopt_streamed_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    rid: Rid,
    op: &MutateOp,
    tag: &LockTag,
) -> Result<bool, MetaError> {
    let Some(spec_seq) = streamed_entry_completing(tx, meta, rid)? else {
        return Ok(false);
    };
    if let Some(v) = tx.get(&meta.spec, seq_key(spec_seq))? {
        let mut row: SpecRow = crate::record::from_bytes_exact(&v)?;
        if let SpecKind::Streamed { own, .. } = &mut row.kind {
            *own = Some((rid, op.clone(), tag.clone()));
        }
        put_row(tx, &meta.spec, spec_seq, &row)?;
    }
    Ok(true)
}

/// Plan 30 §M9: the live `Shadow` for `rid`, if any: its spec seq, epoch
/// and op.
fn shadow_row_for(
    r: &impl Readable,
    meta: &Meta,
    rid: Rid,
) -> Result<Option<(u64, u64, MutateOp, LockTag)>, MetaError> {
    for (seq, entry) in read_live(r, meta)? {
        if !matches!(entry, LiveEntry::Shadow { rid: r2, .. } if r2 == rid) {
            continue;
        }
        if let Some(v) = r.get(&meta.spec, seq_key(seq))? {
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            if let SpecKind::Shadow {
                epoch,
                op,
                lock_tag,
                ..
            } = row.kind
            {
                return Ok(Some((seq, epoch, op, lock_tag)));
            }
        }
    }
    Ok(None)
}

fn read_rows_from(
    r: &impl Readable,
    spec: &SingleWriterTxKeyspace,
    from: u64,
) -> Result<Vec<(u64, SpecRow)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.range(spec, seq_key(from)..) {
        let (k, v) = guard.into_inner()?;
        out.push((decode_seq(&k)?, crate::record::from_bytes_exact(&v)?));
    }
    Ok(out)
}

fn put_row(
    tx: &mut SingleWriterWriteTx,
    spec: &SingleWriterTxKeyspace,
    seq: u64,
    row: &SpecRow,
) -> Result<(), MetaError> {
    tx.insert(spec, seq_key(seq), postcard::to_allocvec(row)?);
    Ok(())
}

/// Append a row for a just-captured application (and its `spec_live`
/// entry, for a shadow or hint).
fn record_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    kind: SpecKind,
    records: Vec<LogRecord>,
    before: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    usage: (i64, i64),
) -> Result<u64, MetaError> {
    let seq = next_spec_seq_tx(tx, &meta.local)?;
    let live = match &kind {
        SpecKind::Shadow {
            rid,
            epoch,
            gen,
            gated,
            non_stealable,
            ..
        } => Some(LiveEntry::Shadow {
            rid: *rid,
            epoch: *epoch,
            gen: *gen,
            gated: *gated,
            non_stealable: *non_stealable,
        }),
        SpecKind::Hint {
            at,
            epoch,
            gen,
            gated,
        } => Some(LiveEntry::Hint {
            at: *at,
            epoch: *epoch,
            gen: *gen,
            gated: *gated,
        }),
        SpecKind::Streamed {
            epoch,
            first,
            last,
            gated,
            ..
        } => Some(LiveEntry::Streamed {
            epoch: *epoch,
            ident: super::member::tail_identity(&records).unwrap_or((*epoch, *first)),
            last: *last,
            gated: *gated,
        }),
        SpecKind::Foreign { .. } | SpecKind::Local { .. } => None,
    };
    if let Some(live) = live {
        tx.insert(&meta.spec_live, seq_key(seq), postcard::to_allocvec(&live)?);
        counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, 1)?;
    }
    put_row(
        tx,
        &meta.spec,
        seq,
        &SpecRow {
            kind,
            records,
            before,
            usage,
            origin: seq,
        },
    )?;
    Ok(seq)
}

/// `store::local::Meta::finish_local`'s half: the `Local` row for a
/// captured transaction whose journal rows start at `first`. Returns its
/// `spec_seq`, for the transaction's `journal_tx` row.
pub(crate) fn record_local_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    first: u64,
    epoch: u64,
    capture: Capture,
    usage: (i64, i64),
) -> Result<u64, MetaError> {
    record_tx(
        tx,
        meta,
        SpecKind::Local { first, epoch },
        Vec::new(),
        capture.into_before(),
        usage,
    )
}

/// A row's before-images: `(key, value before the row first touched it)`.
type Before = [(Vec<u8>, Option<Vec<u8>>)];

fn enqueue_replay_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    op: MutateOp,
    tag: LockTag,
    before: &Before,
) -> Result<(), MetaError> {
    enqueue_replay_as_tx(tx, meta, key, rid, op, tag, None, 0, Streams::NONE, before)
}

/// [`enqueue_replay_tx`] for a shadow a delegate accepted under `gen`.
#[allow(clippy::too_many_arguments)]
fn enqueue_replay_gen_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    op: MutateOp,
    tag: LockTag,
    gen: u64,
    before: &Before,
) -> Result<(), MetaError> {
    enqueue_replay_as_tx(
        tx,
        meta,
        key,
        rid,
        op,
        tag,
        None,
        gen,
        Streams::NONE,
        before,
    )
}

/// Plan 43 C1-4: say what the queued entry at `key` stands for (see
/// `QueuedReplay::class`).
fn mark_queued_class_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    class: RowCommit,
) -> Result<(), MetaError> {
    if class.is_singleton() {
        return Ok(());
    }
    if let Some(v) = tx.get(&meta.pending_replay, seq_key(key))? {
        let mut row: QueuedReplay = crate::record::from_bytes_exact(&v)?;
        row.class = class;
        tx.insert(
            &meta.pending_replay,
            seq_key(key),
            postcard::to_allocvec(&row)?,
        );
        counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_GEN, 1)?;
    }
    Ok(())
}

/// Queue `op` for replay by `rid` under `key`. `before` is the stranded
/// row's before-images (empty when there are none): a truncate keeps the
/// manifest it cut from them ([`StrandedOp::conditional_truncate`]).
/// `foreign`: the node whose stream carried another node's entry (see
/// `QueuedReplay::source`); `deps`: see `QueuedReplay::deps`.
#[allow(clippy::too_many_arguments)]
fn enqueue_replay_as_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    op: MutateOp,
    lock_tag: LockTag,
    foreign: Option<u64>,
    gen: u64,
    deps: Streams,
    before: &Before,
) -> Result<(), MetaError> {
    let truncate_base = truncate_pre_manifest(tx, meta, &op, before);
    let row = QueuedReplay {
        rid,
        op,
        refused: None,
        foreign: foreign.is_some(),
        source: foreign,
        gen,
        truncate_base,
        lock_tag,
        deps,
        class: RowCommit::LONE,
        await_retry: None,
        dropped: false,
    };
    if tx.get(&meta.pending_replay, seq_key(key))?.is_none() {
        counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_COUNT, 1)?;
    }
    tx.insert(
        &meta.pending_replay,
        seq_key(key),
        postcard::to_allocvec(&row)?,
    );
    counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_GEN, 1)?;
    Ok(())
}

/// The inode of a size-only `setattr` — the FUSE truncate path
/// (`truncate`, `ftruncate`, `O_TRUNC`). The replay drain folds such an
/// op into a manifest commit for the same inode queued after it
/// (`authority::core::replay`'s `folded_into_later_manifest`, which
/// matches exactly this shape).
fn size_only_truncate(op: &MutateOp) -> Option<u64> {
    match op {
        MutateOp::Setattr {
            ino,
            mode: None,
            uid: None,
            gid: None,
            size: Some(_),
            atime_ns: None,
            mtime_ns: None,
        } => Some(*ino),
        _ => None,
    }
}

/// For a size-only `setattr`, the manifest its inode had just before it
/// (from the stranded row's before-images); `None` for any other op, or
/// when the inode had no manifest (nothing a truncate could cut).
fn truncate_pre_manifest(
    tx: &SingleWriterWriteTx,
    meta: &Meta,
    op: &MutateOp,
    before: &Before,
) -> Option<Vec<u8>> {
    let ino = size_only_truncate(op)?;
    let key = keys::inode(ino);
    let (_, value) = before.iter().find(|(k, _)| *k == key)?;
    let rec = InodeRecord::decode(value.as_deref()?).ok()?;
    manifest_bytes(tx, &meta.blobs, Some(&rec)).ok().flatten()
}

// --------------------------------------------------- stranded local work

/// The op a stranded transaction without an op of its own is replayed as
/// (`JournalTx::op` is `None`: a local manifest commit, a snapshot row, a
/// quota change, a clone). A lone manifest write becomes the ordinary
/// optimistic `SetManifest` (so an edit that lost to a newer one is
/// refused and materialized as a conflict copy, exactly like classify's
/// "edit-vs-edit"); anything else is re-applied through the replay path
/// as-is (`MutateOp::Records`).
fn derive_replay_op(records: &[LogRecord]) -> Option<MutateOp> {
    // Plan 30 §M13: a refusal or an inbox ack is an outcome of the lost
    // tenure, not an effect to redo. A transaction made only of those
    // (an inbox refusal, a deduplicated batch position) replays nothing:
    // the requester re-submits, or the successor's drain re-evaluates.
    // Plan 30 §M11: so is a lost tenure's `DelegateNode` or `DelegateNodeEnded`: the
    // delegation table is the root's own, decided by the tenure that
    // wrote it. Replayed as plain records through the successor, a
    // deposed root's re-delegation became a grant *to the successor
    // itself* that nothing tracked or ended, and every op of the
    // successor under it parked on a recall that never came
    // (long-delegated-backup seed 71792).
    let effective: Vec<&LogRecord> = records
        .iter()
        .filter(|rec| {
            !matches!(
                rec,
                LogRecord::Completed { .. }
                    | LogRecord::Atime { .. }
                    | LogRecord::Refused { .. }
                    | LogRecord::InboxAck { .. }
                    | LogRecord::DelegateNode { .. }
                    | LogRecord::SubtreeReleased { .. }
                    | LogRecord::StreamReleased { .. }
                    | LogRecord::DelegateNodeEnded { .. }
                    | LogRecord::DelegateNodeGroup { .. }
                    // Plan 43 C1-4: bookkeeping of a re-shipped tail, no
                    // effect to redo.
                    | LogRecord::TailOf { .. }
            )
        })
        .collect();
    match effective.as_slice() {
        [] => None,
        // The record's times go with it: the writer's mtime, and its
        // ctime (the file's last change as the speculative commit left
        // it), so a re-commit does not restamp the file with its own time.
        [LogRecord::WriteManifest {
            ino,
            base_manifest,
            manifest,
            size,
            time_ns,
            mtime_ns,
        }] => Some(MutateOp::SetManifest {
            ino: *ino,
            base_manifest: base_manifest.clone(),
            manifest: manifest.clone(),
            size: *size,
            mtime_ns: Some(*mtime_ns),
            ctime_ns: Some(*time_ns),
        }),
        _ => Some(MutateOp::Records {
            records: effective.into_iter().cloned().collect(),
        }),
    }
}

pub(crate) fn replay_rid_for(r: &impl Readable, meta: &Meta, first: u64) -> Result<Rid, MetaError> {
    let node = kv_get_tx(r, &meta.local, KV_NODE_PREFIX)?
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(Rid {
        node,
        incarnation: LOCAL_REPLAY_INCARNATION,
        seq: first,
    })
}

/// Take this node's journaled transaction starting at `first` back out of
/// the journal and queue its op for replay by rid under `key`: its
/// journal rows and `journal_tx` row are deleted (it will never ship from
/// here), and its rid's `completed` row and `recent` answer with them (it
/// never took effect in the log, so neither this node's own in-doubt
/// check nor its holder dedup may claim it did). The caller has already
/// rolled back its `ns` effects, if it was captured.
fn strand_local_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    first: u64,
    key: u64,
    before: &Before,
    undone: &HashSet<u64>,
    strand: Strand<'_>,
) -> Result<Stranding, MetaError> {
    let (member, recall) = (strand.member, strand.recall);
    let Some(row) = local::get_journal_tx(&*tx, meta, first)? else {
        return Ok(Stranding::Queued);
    };
    let deps = match strand.deps_of.and_then(|m| m.get(&first)) {
        Some(deps) => *deps,
        None => stranded_local_deps(&*tx, meta, first, undone)?,
    };
    let rows = local::journal_records(&*tx, meta, first, row.last)?;
    // A delegate's row this root appended raised the generation's log
    // index (`apply_delegate_tx`, `apply_adopted_records_from`; the
    // session's stream with it). The row now leaves the journal without
    // a segment carrying it (a deposed root's rebuild, a recall strand),
    // so the log does not hold it: the next root that inherits the
    // generation from this replica (`delegate.rs`: cursor =
    // `log_stream_idx`) or a re-stream from here must start below it, or
    // the row never reaches the log (locks-unlinked-delegated-dcrash-
    // pause seed 558, pinned: the rebuilt, re-elected root skipped rows
    // 7..9 of generation 2). A delegate's own row was never counted
    // there and is left alone. Whatever order a generation's rows strand
    // in, the lowest of them sets the index. As `forget_recent` below, the
    // session side is in memory: a failed commit leaves it lower than the
    // store, which only makes a wait, never an early execution.
    if local::retract_log_idx_tx(tx, &meta.local, row.gen, row.idx)? {
        meta.session.retract_stream(row.gen, row.idx - 1);
    }
    for (seq, _) in &rows {
        tx.remove(&meta.journal_ks, seq.to_be_bytes().to_vec());
    }
    tx.remove(&meta.journal_tx, local::tx_key(first));
    local::remove_journal_origin(tx, meta, row.gen, row.idx, first);
    let count = if row.spec_seq.is_some() {
        KV_LOCAL_SPEC_COUNT
    } else {
        KV_UNCAPTURED_TX_COUNT
    };
    counter_add_tx(tx, &meta.local, count, -1)?;
    if row.gen != 0 && row.spec_seq.is_some() {
        counter_add_tx(tx, &meta.local, KV_DELEG_SPEC_COUNT, -1)?;
    }
    // The rid's `completed` row goes with the transaction — unless the
    // log carries the rid's outcome already (a row applied from a
    // segment records position 0; a journaled one its journal seq): a
    // delegate's transaction stranded by a recall after the requester's
    // own replay of the same rid landed (plan 30 §M11 phase 2b,
    // long-delegated seeds 70039/70075/70078: the completion forgotten,
    // the replay's dedup reply installed a shadow nothing retired).
    let rid = match row.rid {
        Some(rid) => rid,
        None => replay_rid_for(&*tx, meta, first)?,
    };
    // (A rid-less transaction's replay rid carries a `completed` row too
    // when it journaled its own marker — the holder's manifest commit.)
    if !log_carries(&*tx, meta, &rid)? {
        tx.remove(&meta.completed, rid.to_key());
    }
    meta.forget_recent(rid);
    let records: Vec<LogRecord> = rows.into_iter().map(|(_, rec)| rec).collect();
    // Plan 30 §M13: a refusal this tenure journaled but never shipped was
    // evaluated against state that is being rolled back; its `completed`
    // row must go with it, or this node would answer the rid "refused"
    // from a decision the log never carried.
    for rec in &records {
        if let LogRecord::Refused { rid, .. } = rec {
            if !log_carries(&*tx, meta, rid)? {
                tx.remove(&meta.completed, rid.to_key());
            }
        }
        // Plan 43 C2: the same goes for a generation's end that this root
        // journaled but never shipped. Its persisted cut goes with it, or a
        // restart would read the cut from a record the log never carried.
        if let LogRecord::DelegateNodeEnded { gen, through, .. } = rec {
            local::retract_void_cut_tx(tx, &meta.local, *gen, *through)?;
        }
        // And the void its journaling set in the session: unless the log
        // ended the generation all the same (a successor re-shipped the
        // row: the table here, rolled back or rebuilt from the log, no
        // longer lists it), it is live again, and the void is lifted once
        // this transaction commits (`Meta::stage_unvoid`).
        if let LogRecord::DelegateNodeEnded { gen, .. } = rec {
            if crate::delegation::read_table_tx(&*tx, meta)?.has_gen(*gen) {
                meta.stage_unvoid(*gen);
            }
        }
    }
    // Plan 46 M1.2 step 7 (c3-spec §6.1, D12): a delegate's row this node
    // appended as the root (`gen` != 0 under a lease epoch, not one of its own
    // generations as a delegate) is another node's stream's row. It was rolled
    // back above (journal rows, `completed`, the generation's log index); it
    // is not this node's to re-execute and is never queued: the delegate's
    // answer waits for a group that may include this node's append, so the
    // row is kept verbatim as a member copy of its generation, which the next
    // root's seal of the generation reads (`DelegSeal`, `deleg_backup_tail`).
    if let Some(own) = strand.deleg_copy {
        if row.gen != 0 && row.epoch > 0 && !own.contains(&row.gen) {
            super::backup::deleg_member_copy_put_tx(
                tx,
                meta,
                row.gen,
                &super::DelegateTx {
                    idx: row.idx,
                    rid: row.rid,
                    records,
                    deps: row.deps,
                },
            )?;
            return Ok(Stranding::DelegMember);
        }
    }
    // Plan 43 C1-4 (§5.2 item 2, §5.6): a row this node's client was told
    // is committed (its group or the archive held it) is not this node's to
    // re-execute: it was rolled back above and goes to the member copy of
    // its epoch, to be served to the successor that seals or fetches the
    // group (which re-ships it) and dropped once the log carries it. Its
    // client's retry by rid dedups against the re-ship, or executes once
    // if the row turns out not to have been held (an unanswered row).
    if member && row.class.gated() && row.gen == 0 && row.epoch > 0 {
        // A segment's strand leaves the row's effect out of the namespace
        // while rows after it may stay: reads of its keys wait for the
        // re-ship (`Meta::member_copy_touches`).
        let hold = strand.hold.then(|| crate::KeySet::from_records(&records));
        let kept = super::member::put_row_tx(
            tx,
            meta,
            row.epoch,
            &super::member::MemberTx {
                tx: crate::BackupTx {
                    first,
                    last: row.last,
                    records,
                    origin: (row.gen, row.idx),
                },
                class: row.class,
                rid: row.rid,
                op: row.op,
                lock_tag: row.lock_tag.clone(),
            },
        )?;
        if let (true, Some(keys)) = (kept, hold) {
            meta.hold_copy_row(row.epoch, first, keys);
        }
        return Ok(Stranding::Member);
    }
    // Plan 43 C2 (§3.8): a delegate's row whose client was told nothing
    // (gated: the answer waits for the row's commit) and which the log's
    // cut leaves out is dropped: rolled back above, nothing queued. The
    // client retries by rid at the generation's new owner; the park that
    // waited for the row was aborted at the end (`drop_delegate_state`).
    // (A row with a lease epoch is a root's append of a delegate's row, not
    // this node's own execution as the delegate: it keeps today's rule. A
    // root append past the recall's cut is queued whatever its class: the
    // class is not the append's commit, and a lone tenure counts its appends
    // at once, so the queued copy can be the row's last server-side copy
    // (L2b, below).)
    //
    // L2b (known, deferred to plan 46 M1.2; PROGRESS, "L2b"). Two paths queue
    // a copy of another node's rid that the class gates, and both are kept:
    // (a) a delegate that takes the root strands its own rows (`gen` != 0,
    // `epoch` == 0, `member`, neither `recall` nor `rebuild`) at its takeover
    // gate: the gate's own call restores the committed ones from the tail
    // (the replay finds them completed) and re-appends (C3) the ones that
    // continue the stream, so what is left replays by rid through the
    // sequencer, which dedups any other copy. It is not dropped as the
    // recall drop above does: a `DelegateNodeGroup` row committed by the delegate's
    // own backups is answered and in no root tail, and a halted one would be
    // lost with the generation the gate ends. (b) the recall pass queues the
    // root appends past the cut (`recall`, `!member`, `epoch` > 0) with their
    // class, for the reason in the preceding parenthesis. Neither executes
    // twice (the sequencer answers the second copy from `completed`), but (b)
    // can sit beside the owner's own replay of the same rid, which the I2
    // ledger judges (long-delegated-backup seed 13016 under slow lease reads).
    // The defect is upstream: a committed delegate row that exists verbatim
    // only in the journal of a delegate-turned-root's lone tenure, which ends
    // unshipped below the next root's cut, so every copy left is a rescue by
    // rid. M1.2's member copies close it, and with it the pair.
    if recall && row.class.gated() && row.gen != 0 && row.epoch == 0 {
        return Ok(Stranding::Dropped);
    }
    // L2: a rebuild strands a root's appends of a delegate's rows (`gen`
    // != 0, `epoch` > 0): see `Strand::rebuild`. They are rolled back above
    // (journal rows, `completed`, the generation's log index); a gated one
    // is not queued. The class does not say the append committed: a root
    // journals a delegate's row `Lone` (`Meta::finish_local`; no class is
    // set around `apply_delegate_tx` in `on_delegate_stream`), whatever
    // ADR-41 counted for the delegate. Only two things make it gated, and
    // both leave another copy:
    // - another node's re-ask of the rid, answered from this journal
    //   (`journaled_answer_class_for` re-tags it `Archive`, and the answer
    //   waits for the archive): the asker holds an unanswered, executable
    //   copy and retries it by rid at the successor (this would be a second
    //   one, I2);
    // - a predecessor's tail adopted at a takeover
    //   (`apply_adopted_records_from`, `RootGroup` by default): its group
    //   committed it, and its members keep it as a member copy until the log
    //   carries it.
    // A third source is C3's re-append at a delegate's own takeover gate
    // (`reappend_own_delegate_rows` -> `apply_adopted_records_from`, class
    // `RootGroup`): it is the source in every L2b witness. The drop is safe
    // for it only while the requester holds the other copy (a persisted gated
    // shadow, a live retry or an in-doubt op); a requester restarted without
    // its journal leaves the recall pass's queued copy as the last one.
    // In both the delegate also re-streams the row while it holds it. An
    // append still `Lone` keeps today's rule (queued): for a lone tenure
    // (ADR-41 counts its journal at once) it is the row's last copy once the
    // delegate's is gone. Latent hazard: a change that journals a root's
    // appends with a gated class (`journal_class_for`: a lone root classes
    // another node's op `Archive`) would make ADR-41 count the append at
    // once, and this drop would lose an answered row.
    if strand.rebuild && row.class.gated() && row.gen != 0 && row.epoch > 0 {
        return Ok(Stranding::Dropped);
    }
    // Seed 2560: a root's append of a delegate's row (`gen` != 0, `epoch`
    // > 0) whose every outcome a segment of a later tenure carries under
    // another identity ([`superseded_root_appends_tx`]). Rolled back above
    // (journal rows, `completed`, the generation's log index); the
    // segment's own records apply in its place. Not queued: the log has the
    // rid's outcome, so a replay could only be answered from the dedup.
    if strand.superseded && row.gen != 0 && row.epoch > 0 {
        return Ok(Stranding::Dropped);
    }
    // (A gated row of this node's own tenure that a deposition queues for
    // replay is a bug: it would be re-executed by a node whose client was
    // told it is committed. Only an operator's drop of a held transaction
    // (`member` false) queues one, on purpose.)
    let gated_queue = member && row.class.gated() && row.gen == 0;
    let op = match row.op {
        Some(op) => Some(op),
        None => derive_replay_op(&records),
    };
    if let Some(op) = op {
        enqueue_replay_as_tx(tx, meta, key, rid, op, row.lock_tag, None, 0, deps, before)?;
        mark_queued_class_tx(tx, meta, key, row.class)?;
    }
    Ok(if gated_queue {
        Stranding::GatedQueued
    } else {
        Stranding::Queued
    })
}

/// Plan 30 §M11: what a stranded transaction of this node, starting at
/// journal seq `first`, was ordered after on the stream side: for every
/// delegation generation, the highest index of a row of it still in the
/// journal ahead of `first` — on a root, the delegates' transactions it
/// appended and never shipped. A deposition strands the root's own rows
/// (the transaction is replayed by rid) but not those (they belong to
/// their generation, and the delegate re-streams them to the successor
/// root), so the log would otherwise carry the two in whichever order
/// they reach the successor: a replayed forward ahead of the delegate's
/// row it had been ordered after (long-delegated-late-answers seed 2945:
/// a deposed root's marker, executed after it appended the data from
/// `d1`'s stream, replayed through the successor before `d1`'s delegate
/// re-streamed the data). The successor executes the replay only once
/// it has appended these (`reaches_streams`), as it does any forward's
/// `deps`. A table that does not fit keeps the highest generations
/// (the older a generation, the likelier the log has it already).
///
/// Not the generations whose grant this pass takes out of the journal too
/// (`undone`: a `DelegateNode` row of the stranded tenure, never shipped): the
/// log never had them, and a successor allocates above the log's highest
/// number, so the number it issues next is the very one such a grant
/// carried. A row recorded under it would be judged against the
/// successor's unrelated stream of that number, and the replay would wait
/// for it to reach the index. The set is taken before the pass strands
/// anything, as the pass goes oldest first and the grant's own row is
/// gone from the journal by the time a later transaction is examined. A
/// shipped grant's number is in the log, so it never lands here; its
/// unshipped `DelegateNodeEnded` does not put it there either (the 2945 case).
fn stranded_local_deps(
    r: &impl Readable,
    meta: &Meta,
    first: u64,
    undone: &HashSet<u64>,
) -> Result<Streams, MetaError> {
    let mut highest: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    for (start, head) in local::read_journal_tx_heads(r, meta)? {
        if start >= first {
            break;
        }
        if head.gen != 0 && !undone.contains(&head.gen) {
            let e = highest.entry(head.gen).or_insert(0);
            *e = (*e).max(head.idx);
        }
    }
    Ok(streams_of(&highest))
}

/// [`stranded_local_deps`] of every journal transaction, by its first
/// journal seq, in one pass over the journal as it stands.
fn stranded_deps_by_tx(
    r: &impl Readable,
    meta: &Meta,
    undone: &HashSet<u64>,
) -> Result<HashMap<u64, Streams>, MetaError> {
    let mut highest: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    let mut out = HashMap::new();
    for (start, head) in local::read_journal_tx_heads(r, meta)? {
        out.insert(start, streams_of(&highest));
        if head.gen != 0 && !undone.contains(&head.gen) {
            let e = highest.entry(head.gen).or_insert(0);
            *e = (*e).max(head.idx);
        }
    }
    Ok(out)
}

/// The dependency table for the highest stream index per generation (the
/// highest generations when it does not fit).
fn streams_of(highest: &std::collections::BTreeMap<u64, u64>) -> Streams {
    let mut deps = Streams::NONE;
    for (gen, idx) in highest.iter().rev().take(crate::session::STREAMS_CAP) {
        deps.raise(*gen, *idx);
    }
    deps
}

/// The delegation generations whose `DelegateNode` record sits in the journal
/// transactions starting at `firsts` — the grants a pass that strands
/// those transactions undoes (see [`stranded_local_deps`]).
fn undone_grants(
    r: &impl Readable,
    meta: &Meta,
    firsts: impl IntoIterator<Item = u64>,
) -> Result<HashSet<u64>, MetaError> {
    let mut gens = HashSet::new();
    for first in firsts {
        let Some(row) = local::get_journal_tx(r, meta, first)? else {
            continue;
        };
        for (_, rec) in local::journal_records(r, meta, first, row.last)? {
            if let LogRecord::DelegateNode { gen, .. } = rec {
                gens.insert(gen);
            }
        }
    }
    Ok(gens)
}

// ------------------------------------------------------------- rollback

/// The manifest bytes an inode record references, if any.
fn manifest_bytes(
    tx: &SingleWriterWriteTx,
    blobs: &SingleWriterTxKeyspace,
    rec: Option<&InodeRecord>,
) -> Result<Option<Vec<u8>>, MetaError> {
    match rec.and_then(|r| r.manifest.as_ref()) {
        Some(payload) => Ok(Some(ns::resolve_payload(tx, blobs, payload)?)),
        None => Ok(None),
    }
}

/// Put `key` back to `before`, keeping the indexes derived from `ns` in
/// line (see the module doc's "What a rollback restores"). Written
/// through the ordinary tracked path, so the key is dirtied and the next
/// publish (once one is allowed again) carries the restored value.
fn restore_key_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    key: &[u8],
    before: Option<&[u8]>,
) -> Result<(), MetaError> {
    let current: Option<Vec<u8>> = tx.get(&meta.ns, key)?.map(|v| v.to_vec());
    if current.as_deref() == before {
        return Ok(());
    }
    match keys::Key::parse(key) {
        Ok(keys::Key::Inode { ino }) => {
            let old = current.as_deref().map(InodeRecord::decode).transpose()?;
            let new = before.map(InodeRecord::decode).transpose()?;
            let old_manifest = manifest_bytes(tx, &meta.blobs, old.as_ref())?;
            let new_manifest = manifest_bytes(tx, &meta.blobs, new.as_ref())?;
            misc::track_manifest_transition_tx(
                tx,
                &meta.chunk_ref,
                &meta.chunk_ref_by_ino,
                ino,
                old_manifest.as_deref(),
                new_manifest.as_deref(),
            )?;
            if let Some(old) = &old {
                for (name, _) in &old.xattrs {
                    misc::xattr_by_name_del_tx(
                        tx,
                        &meta.xattr_by_name,
                        &String::from_utf8_lossy(name),
                        ino,
                    );
                }
            }
            if let Some(new) = &new {
                for (name, value) in &new.xattrs {
                    misc::xattr_by_name_put_tx(
                        tx,
                        &meta.xattr_by_name,
                        &String::from_utf8_lossy(name),
                        ino,
                        value,
                    );
                }
                // Linked again: whatever orphaned it is being undone (its
                // xattrs are the restored record's again).
                tx.remove(&meta.orphans, ino.to_be_bytes().to_vec());
                ns::clear_orphan_xattrs(tx, &meta.orphans, ino)?;
            }
        }
        Ok(keys::Key::Xattr { ino, name }) => {
            let name = String::from_utf8_lossy(name).into_owned();
            if current.is_some() {
                misc::xattr_by_name_del_tx(tx, &meta.xattr_by_name, &name, ino);
            }
            if let Some(bytes) = before {
                let value = ns::resolve_payload(tx, &meta.blobs, &Payload::decode(bytes)?)?;
                misc::xattr_by_name_put_tx(tx, &meta.xattr_by_name, &name, ino, &value);
            }
        }
        _ => {}
    }
    match before {
        Some(value) => ns::ns_insert(tx, &meta.ns, dirty, key.to_vec(), value.to_vec()),
        None => ns::ns_remove(tx, &meta.ns, dirty, key.to_vec()),
    }
}

/// A tailed segment to apply *before* the rows a rewind redoes (see the
/// module doc's last lifecycle point).
struct Inserted<'a> {
    segment_seq: u64,
    records: &'a [LogRecord],
    pending: &'a TouchSet,
}

/// Recovery steps 1 and 2 (plan 30 §M3a) from `cutoff` on, inside the
/// caller's transaction: roll every row from `cutoff` back, apply
/// `insert` (if any) in their place, then redo, oldest first, every row
/// that still stands. Rows in `stranded` are taken out instead (see the
/// module doc's "Lifecycle"). With `insert`, redone rows move to fresh
/// `spec_seq`s after the inserted one, keeping their relative order.
/// Usage moves are staged into `staged`; the caller persists and drains
/// it. Returns what stranded and how many inserted records were skipped.
///
/// Plan 30 §M4: with `insert`, `Foreign` rows in the range are redone
/// *before* the inserted segment. Such a row is one of this node's own
/// transactions that shipped while an older one was held back
/// (`store::held`), so it precedes the inserted segment in the log; it
/// touches none of the held rows' keys, so redoing it ahead of them is
/// the same state.
#[allow(clippy::too_many_arguments)]
fn rewind_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    live: &HashMap<u64, LiveEntry>,
    stranded: &HashSet<u64>,
    cutoff: u64,
    strand: Strand<'_>,
    insert: Option<Inserted<'_>>,
) -> Result<(Stranded, usize), MetaError> {
    let rows = read_rows_from(tx, &meta.spec, cutoff)?;
    let plain = meta.dirty_for_ns();
    let undone = undone_grants(
        &*tx,
        meta,
        rows.iter().filter_map(|(seq, row)| match row.kind {
            SpecKind::Local { first, .. } if stranded.contains(seq) => Some(first),
            _ => None,
        }),
    )?;

    // 1. Roll back, newest first, so each key ends at the value it had
    //    before the earliest rolled-back row touched it.
    for (_, row) in rows.iter().rev() {
        for (key, before) in row.before.iter().rev() {
            restore_key_tx(tx, meta, plain, key, before.as_deref())?;
        }
        staged.adjust(-row.usage.0, -row.usage.1);
    }

    let renumber = insert.is_some();
    let mut out = Stranded::default();
    let mut skipped = 0;
    let Some(ins) = insert else {
        // 3. Redo, oldest first, what still stands.
        for (seq, row) in rows {
            redo_row_tx(
                tx, meta, staged, live, stranded, &mut out, false, seq, row, &undone, strand,
            )?;
        }
        return Ok((out, skipped));
    };

    // 2a. Rows already in the log ahead of the inserted segment.
    let (durable, rest): (Vec<_>, Vec<_>) = rows
        .into_iter()
        .partition(|(_, row)| matches!(row.kind, SpecKind::Foreign { .. }));
    for (seq, row) in durable {
        redo_row_tx(
            tx, meta, staged, live, stranded, &mut out, true, seq, row, &undone, strand,
        )?;
    }

    // 2b. The inserted segment, in the rolled-back rows' place.
    {
        let capture = Capture::new();
        let row_staged = UsageTracker::staging();
        let (count, applied) = {
            let cx = ApplyCx {
                dirty: meta.dirty_capturing(&capture),
                durable: true,
            };
            apply_batch_tx(tx, meta, cx, ins.records, ins.pending, &row_staged, true)?
        };
        skipped = count;
        let usage = row_staged.raw_delta();
        staged.adjust(usage.0, usage.1);
        record_tx(
            tx,
            meta,
            SpecKind::Foreign {
                segment_seq: ins.segment_seq,
            },
            applied,
            capture.into_before(),
            usage,
        )?;
    }

    // 3. Redo, oldest first, what still stands.
    for (seq, row) in rest {
        redo_row_tx(
            tx, meta, staged, live, stranded, &mut out, renumber, seq, row, &undone, strand,
        )?;
    }
    Ok((out, skipped))
}

/// One row of [`rewind_tx`]'s redo: take it out if stranded, drop it if it
/// is retired speculation, otherwise re-apply its records under a fresh
/// capture (and, with `renumber`, move it to a fresh `spec_seq`).
#[allow(clippy::too_many_arguments)]
fn redo_row_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    live: &HashMap<u64, LiveEntry>,
    stranded: &HashSet<u64>,
    out: &mut Stranded,
    renumber: bool,
    seq: u64,
    mut row: SpecRow,
    undone: &HashSet<u64>,
    strand: Strand<'_>,
) -> Result<(), MetaError> {
    if stranded.contains(&seq) {
        // Plan 42 §5.1.5: the rows this entry wrote stay at their
        // before-images (step 1 restored them; nothing redoes them).
        meta.note_rolled_back(row.before.iter().map(|(key, _)| key.as_slice()));
        tx.remove(&meta.spec, seq_key(seq));
        if matches!(
            row.kind,
            SpecKind::Shadow { .. } | SpecKind::Hint { .. } | SpecKind::Streamed { .. }
        ) {
            tx.remove(&meta.spec_live, seq_key(seq));
            counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -1)?;
        }
        // Plan 43 C1-4: a committed entry the takeover gate's tail carries
        // is taken out and not queued.
        let quiet = strand.quiet.is_some_and(|q| q.contains(&seq));
        match row.kind {
            SpecKind::Shadow {
                rid,
                op,
                gen,
                lock_tag,
                gated,
                ..
            } => {
                if quiet {
                    out.superseded += 1;
                } else {
                    enqueue_replay_gen_tx(
                        tx,
                        meta,
                        row.origin,
                        rid,
                        op,
                        lock_tag,
                        gen,
                        &row.before,
                    )?;
                    if gated {
                        mark_queued_class_tx(tx, meta, row.origin, RowCommit::ROOT_GROUP)?;
                    }
                    out.shadows += 1;
                    out.committed_rolled_back += usize::from(gated);
                }
            }
            SpecKind::Local { first, .. } => {
                // (Seed 2560: a `Local` entry of the quiet set is a root's
                // append the segment being applied supersedes.)
                let strand = Strand {
                    superseded: quiet,
                    ..strand
                };
                match strand_local_tx(tx, meta, first, row.origin, &row.before, undone, strand)? {
                    Stranding::Member => out.member_copied += 1,
                    Stranding::DelegMember => out.deleg_member_copied += 1,
                    Stranding::Queued => out.locals += 1,
                    Stranding::GatedQueued => {
                        out.locals += 1;
                        out.gated_queued += 1;
                    }
                    Stranding::Dropped => out.deleg_dropped += 1,
                }
            }
            SpecKind::Streamed { gated, .. } if quiet => {
                let _ = gated;
                out.superseded += 1;
            }
            SpecKind::Streamed {
                own, source, gated, ..
            } => {
                out.committed_rolled_back += usize::from(gated);
                // Plan 30 §M9: the tenure ended without shipping it; its
                // successor re-ships what its backup held (or the op's
                // requester replays it). Until then reads of its keys
                // must not see the rolled-back state as final. This
                // node's own op (adopted by `install_shadow`) replays as
                // a shadow's would.
                if let Some((rid, op, tag)) = own {
                    enqueue_replay_tx(tx, meta, row.origin, rid, op, tag, &row.before)?;
                    if gated {
                        mark_queued_class_tx(tx, meta, row.origin, RowCommit::ROOT_GROUP)?;
                    }
                    out.shadows += 1;
                } else {
                    let rid = row.records.iter().find_map(|r| match r {
                        LogRecord::Completed { rid } => Some(*rid),
                        _ => None,
                    });
                    if let Some(rid) = rid {
                        let op = MutateOp::Records {
                            records: row.records.clone(),
                        };
                        enqueue_replay_as_tx(
                            tx,
                            meta,
                            row.origin,
                            rid,
                            op,
                            LockTag::NONE,
                            Some(source),
                            0,
                            Streams::NONE,
                            &[],
                        )?;
                        if gated {
                            mark_queued_class_tx(tx, meta, row.origin, RowCommit::ROOT_GROUP)?;
                        }
                    }
                    out.hints += 1;
                }
            }
            SpecKind::Hint { gated, .. } => {
                if quiet {
                    out.superseded += 1;
                } else {
                    out.hints += 1;
                    out.committed_rolled_back += usize::from(gated);
                }
            }
            SpecKind::Foreign { .. } => out.hints += 1,
        }
        return Ok(());
    }
    let durable = matches!(row.kind, SpecKind::Foreign { .. });
    if !durable && !live.contains_key(&seq) {
        // Retired speculation: the segment that retired it was
        // captured after it (it was outstanding then), and that
        // `Foreign` row's redo carries its effect. (A `Local` row that
        // shipped while older speculation was outstanding was turned
        // into a `Foreign` row at that moment — `retire_local_tx`.)
        tx.remove(&meta.spec, seq_key(seq));
        return Ok(());
    }
    let records: Vec<LogRecord> = match &row.kind {
        SpecKind::Local { first, .. } => {
            let last = local::get_journal_tx_head(&*tx, meta, *first)?
                .map(|t| t.last)
                .ok_or_else(|| {
                    MetaError::Invalid(format!(
                        "Local speculation row {seq} has no journal_tx row at {first}"
                    ))
                })?;
            local::journal_records(&*tx, meta, *first, last)?
                .into_iter()
                .map(|(_, rec)| rec)
                .collect()
        }
        _ => row.records.clone(),
    };
    let capture = Capture::new();
    let row_staged = UsageTracker::staging();
    {
        let cx = ApplyCx {
            dirty: meta.dirty_capturing(&capture),
            durable,
        };
        for rec in &records {
            if let Some(why) = apply_record(tx, meta, cx, rec, &row_staged)? {
                tracing::debug!(why, ?rec, "speculation redo: record skipped");
            }
        }
    }
    row.usage = row_staged.raw_delta();
    staged.adjust(row.usage.0, row.usage.1);
    row.before = capture.into_before();
    // A delegation origin's row may touch more than before (its captured
    // inodes are this redo's): what it touches joins `DelegRows`.
    if let Some(LiveEntry::Local { gen, .. }) = live.get(&seq) {
        if *gen != 0 {
            let mut touched = TouchSet::from_records(records.iter());
            captured_files(&row, &mut touched);
            note_deleg_touches(meta, *gen, &touched);
        }
    }
    if !renumber {
        put_row(tx, &meta.spec, seq, &row)?;
        return Ok(());
    }
    tx.remove(&meta.spec, seq_key(seq));
    let new_seq = next_spec_seq_tx(tx, &meta.local)?;
    put_row(tx, &meta.spec, new_seq, &row)?;
    match live.get(&seq) {
        Some(LiveEntry::Local { first, .. }) => {
            if let Some(mut jt) = local::get_journal_tx(&*tx, meta, *first)? {
                jt.spec_seq = Some(new_seq);
                local::put_journal_tx(tx, meta, *first, &jt)?;
            }
        }
        Some(entry) => {
            tx.remove(&meta.spec_live, seq_key(seq));
            tx.insert(
                &meta.spec_live,
                seq_key(new_seq),
                postcard::to_allocvec(entry)?,
            );
        }
        None => {}
    }
    Ok(())
}

/// A streamed transaction to put in its log place (see
/// [`Meta::install_streamed`]): its tenure position, its records, and the
/// shadow it confirms (`spec_seq`, rid, op), if it is this node's own op.
struct StreamedTx<'a> {
    epoch: u64,
    source: u64,
    first: u64,
    last: u64,
    records: &'a [LogRecord],
    adopt: Option<(u64, Rid, MutateOp, LockTag)>,
    /// Plan 43 C1-4: see [`SpecKind::Streamed::gated`].
    gated: bool,
}

/// Whether a row stays ahead of a transaction the stream delivers now: a
/// tailed segment (`Foreign`) or a streamed transaction precedes it in
/// the log. So does a hint, as far as the reordering goes: its refusal
/// may be streamed already (a hint is not converted when it is), and then
/// what the stream delivers now is later than the state the hint read
/// (`backup-hot` seed 90024: the hint of `create f1`, its streamed
/// refusal, then the streamed `rename f1 f0` — redone under the hint, the
/// rename was undone). A hint never overlaps an outstanding shadow
/// (`install_hint_from`), so leaving it where it is never misplaces one.
fn row_is_ordered(row: &SpecRow) -> bool {
    matches!(
        row.kind,
        SpecKind::Foreign { .. } | SpecKind::Streamed { .. } | SpecKind::Hint { .. }
    )
}

/// A delegate generation's [`local::deleg_streamed_tx`] marks, read once
/// per generation for one decision.
type DelegMarks = HashMap<u64, Option<(u64, u64)>>;

/// [`row_is_ordered`] for a transaction the root's stream of epoch `epoch`
/// delivers now (L1): also a delegate's own `Local` row whose root append
/// that stream delivered already ([`local::note_deleg_streamed_tx`]) — it
/// is earlier in the log, as a streamed row is. Only a `Local` row of a
/// delegate generation reads anything; `marks` caches the per-generation
/// read.
fn row_is_ordered_at(
    r: &impl Readable,
    meta: &Meta,
    row: &SpecRow,
    epoch: u64,
    marks: &mut DelegMarks,
) -> Result<bool, MetaError> {
    if row_is_ordered(row) {
        return Ok(true);
    }
    let SpecKind::Local { first, .. } = row.kind else {
        return Ok(false);
    };
    let Some(head) = local::get_journal_tx_head(r, meta, first)? else {
        return Ok(false);
    };
    if head.gen == 0 {
        return Ok(false);
    }
    // A stale mark (a `drop-held` rollback of the append, a hold stream that
    // died with its epoch, a number granted again) is benign: no root row
    // touches a live generation's keys, because a cross-subtree op recalls the
    // generation first, and the recall's segment strands every `Local` row
    // past the cut and redoes the streamed rows in log order. Anything that
    // lets a root write a delegated inode key while the generation lives
    // breaks this.
    let mark = match marks.entry(head.gen) {
        std::collections::hash_map::Entry::Occupied(e) => *e.get(),
        std::collections::hash_map::Entry::Vacant(e) => {
            *e.insert(local::deleg_streamed_tx(r, &meta.local, head.gen)?)
        }
    };
    Ok(mark.is_some_and(|(e, i)| e == epoch && head.idx <= i))
}

/// The keys a row's application touched (a `Local` row's records are its
/// journal rows), and every non-directory inode it changed
/// ([`captured_files`]).
fn row_touches(r: &impl Readable, meta: &Meta, row: &SpecRow) -> Result<TouchSet, MetaError> {
    let mut set = if let SpecKind::Local { first, .. } = &row.kind {
        meta.note_local_rows_read(1);
        let Some(head) = local::get_journal_tx_head(r, meta, *first)? else {
            return Ok(TouchSet::default());
        };
        let records = local::journal_records(r, meta, *first, head.last)?;
        TouchSet::from_records(records.iter().map(|(_, rec)| rec))
    } else {
        TouchSet::from_records(row.records.iter())
    };
    captured_files(row, &mut set);
    Ok(set)
}

/// Plan 45 follow-up: a record that removes or replaces a name (`Unlink`,
/// `Rmdir`, `Rename`, `Exchange`) names the name, not the inode, so its
/// [`TouchSet`] misses the inode it changed — harmless while every file
/// had one name, wrong for a hard-linked one: a `Link` of the inode under
/// another name and an `Unlink` of one of its names do not commute (the
/// link keeps the inode alive). The authority sim's `backup-hot+links`
/// seed 18: a shadow of `unlink f0` (its inode's only name *here*: the
/// holder's earlier `link f0 f1` was still in flight) removed the inode;
/// the streamed link, judged disjoint, went on top and was skipped
/// ("link endpoint missing"), and the replica lost the file for good.
/// A row's captured before-images name every inode it changed: each
/// non-directory one is held exclusively (directories keep their shared
/// hold, plan 30 §M12).
fn captured_files(row: &SpecRow, set: &mut TouchSet) {
    for (key, before) in &row.before {
        let (Ok(keys::Key::Inode { ino }), Some(value)) = (keys::Key::parse(key), before) else {
            continue;
        };
        if InodeRecord::decode(value)
            .is_ok_and(|rec| rec.attrs.kind != constellation_mtree::record::Kind::Dir)
        {
            set.inos.insert(ino);
        }
    }
}

/// What the outstanding journal transactions of a delegation origin
/// (`gen` != 0: a delegate's own stream rows, a root's appended ones) may
/// touch: the union of their [`row_touches`] and the generations they
/// belong to. A tailed segment that touches none of it commutes with all
/// of them and need not read one (xcheckfs two-node runs: a delegate
/// holding 142,000 unappended rows read every one of them for every
/// segment it tailed, 0.5 to 0.65 s a segment, and fell tens of thousands
/// of segments behind the log, so its rows retired ever later and the
/// next segment cost more).
///
/// A superset, never a subset. Every transaction journaled with an
/// origin is noted (its generation at once, its first journal seq for
/// later), and what the noted ones touch joins the union at the next use,
/// read from the committed store *and* from the using transaction's view;
/// a redo adds what it touches (its captured inodes are the redo's). The
/// summary is built on first use the same way, from both views (a
/// transaction that strands rows and then fails must not leave them out),
/// and rebuilt once it has grown by more than twice what it held then.
/// Nothing is taken out as rows retire: a stale key costs a needless
/// scan, never a missed overlap; the summary is dropped (built afresh at
/// the next use) once a row is journaled with no other one of a
/// delegation origin captured ([`note_deleg_tx`]). Noting runs inside the journaling
/// transaction, using inside the segment's: both under the store's
/// writer lock, so a note's transaction has ended (committed or not) by
/// the time a use reads it.
#[derive(Default)]
pub struct DelegRows {
    built: Option<DelegSummary>,
}

#[derive(Default)]
struct DelegSummary {
    gens: HashSet<u64>,
    touch: TouchSet,
    /// First journal seqs of transactions noted and not read yet.
    pending: Vec<u64>,
    added: usize,
    rebuild_after: usize,
}

/// The fewest additions after which a built [`DelegRows`] is rebuilt.
const DELEG_ROWS_REBUILD_MIN: usize = 4096;
/// The most noted transactions a built [`DelegRows`] keeps unread.
const DELEG_ROWS_PENDING_MAX: usize = 1 << 20;

impl DelegSummary {
    fn add(&mut self, touched: &TouchSet) {
        self.touch.dentries.extend(touched.dentries.iter().cloned());
        self.touch.inos.extend(touched.inos.iter().copied());
        self.touch.shared.extend(touched.shared.iter().copied());
        self.added += 1;
    }

    /// What the captured transaction at journal seq `first` touches, as
    /// `r` holds it (nothing: not there, or not captured).
    fn add_first(&mut self, r: &impl Readable, meta: &Meta, first: u64) -> Result<(), MetaError> {
        let Some(head) = local::get_journal_tx_head(r, meta, first)? else {
            return Ok(());
        };
        self.gens.insert(head.gen);
        let Some(spec_seq) = head.spec_seq else {
            return Ok(());
        };
        if let Some(v) = r.get(&meta.spec, seq_key(spec_seq))? {
            let touched = row_touches(r, meta, &crate::record::from_bytes_exact::<SpecRow>(&v)?)?;
            self.add(&touched);
        }
        Ok(())
    }

    /// Every outstanding transaction of an origin `r` holds. Returns how
    /// many captured ones it read.
    fn add_all(&mut self, r: &impl Readable, meta: &Meta) -> Result<usize, MetaError> {
        let mut rows = 0;
        if counter_get(r, &meta.local, KV_LOCAL_SPEC_COUNT)? == 0
            && counter_get(r, &meta.local, KV_UNCAPTURED_TX_COUNT)? == 0
        {
            return Ok(rows);
        }
        for (_, head) in local::read_journal_tx_heads(r, meta)? {
            if head.gen == 0 {
                continue;
            }
            self.gens.insert(head.gen);
            let Some(spec_seq) = head.spec_seq else {
                continue;
            };
            if let Some(v) = r.get(&meta.spec, seq_key(spec_seq))? {
                let touched =
                    row_touches(r, meta, &crate::record::from_bytes_exact::<SpecRow>(&v)?)?;
                self.add(&touched);
                rows += 1;
            }
        }
        Ok(rows)
    }
}

/// [`DelegRows`], built (or rebuilt), and with every noted transaction
/// read, as described there.
fn deleg_rows_ready<'a>(
    tx: &SingleWriterWriteTx,
    meta: &Meta,
    rows: &'a mut DelegRows,
) -> Result<&'a mut DelegSummary, MetaError> {
    let stale = rows
        .built
        .as_ref()
        .is_none_or(|s| s.added > s.rebuild_after);
    if stale {
        let mut summary = DelegSummary::default();
        let committed = summary.add_all(&meta.db.read_tx(), meta)?;
        let own = summary.add_all(tx, meta)?;
        summary.added = 0;
        summary.rebuild_after = DELEG_ROWS_REBUILD_MIN.max(2 * committed.max(own));
        rows.built = Some(summary);
    }
    let summary = rows.built.as_mut().expect("built");
    if !summary.pending.is_empty() {
        let pending = std::mem::take(&mut summary.pending);
        let snapshot = meta.db.read_tx();
        for first in pending {
            summary.add_first(tx, meta, first)?;
            summary.add_first(&snapshot, meta, first)?;
        }
    }
    Ok(summary)
}

/// A journal transaction of delegation origin `gen` was journaled at
/// journal seq `first` (in the calling transaction): noted in
/// [`DelegRows`] (nothing to do while the summary is not built).
///
/// `fresh`: it is the only captured row of a delegation origin the
/// calling transaction sees. Everything the summary holds is then of
/// rows gone since (retired or stranded), and it is dropped, to be built
/// at the next use from the store as it is then: otherwise a delegate
/// that once held a long stream would keep every key of it for good, and
/// a later segment on any of them would read its whole new stream again.
/// Dropping is always sound (the build reads both views, so rows the
/// calling transaction took out and does not commit are seen).
pub(crate) fn note_deleg_tx(meta: &Meta, first: u64, gen: u64, fresh: bool) {
    let mut rows = meta.deleg_rows.lock().unwrap();
    if fresh {
        rows.built = None;
        return;
    }
    if let Some(summary) = rows.built.as_mut() {
        summary.gens.insert(gen);
        summary.pending.push(first);
        // Nothing has tailed a segment for a long while: built afresh
        // at the next use instead.
        if summary.pending.len() > DELEG_ROWS_PENDING_MAX {
            rows.built = None;
        }
    }
}

/// A redo of a row of delegation origin `gen`: what it touches now joins
/// [`DelegRows`].
fn note_deleg_touches(meta: &Meta, gen: u64, touched: &TouchSet) {
    if let Some(summary) = meta.deleg_rows.lock().unwrap().built.as_mut() {
        summary.gens.insert(gen);
        summary.add(touched);
    }
}

/// Whether a journal transaction of a delegation origin may touch what
/// `touch` names (`false`: none does).
fn deleg_rows_may_overlap(
    tx: &SingleWriterWriteTx,
    meta: &Meta,
    touch: &TouchSet,
) -> Result<bool, MetaError> {
    let mut rows = meta.deleg_rows.lock().unwrap();
    let union = &deleg_rows_ready(tx, meta, &mut rows)?.touch;
    // `union.overlaps(touch)`, looked up from the segment's (small) side.
    Ok(touch.dentries.iter().any(|d| union.dentries.contains(d))
        || touch
            .inos
            .iter()
            .any(|i| union.inos.contains(i) || union.shared.contains(i))
        || touch.shared.iter().any(|i| union.inos.contains(i)))
}

/// The origins in `origins` whose generation may have a transaction in
/// this journal (the others have none).
pub(crate) fn deleg_origins_held(
    tx: &SingleWriterWriteTx,
    meta: &Meta,
    origins: &[(u64, u64)],
) -> Result<HashSet<(u64, u64)>, MetaError> {
    let mut rows = meta.deleg_rows.lock().unwrap();
    let summary = deleg_rows_ready(tx, meta, &mut rows)?;
    Ok(origins
        .iter()
        .filter(|(gen, _)| *gen != 0 && summary.gens.contains(gen))
        .copied()
        .collect())
}

/// [`TouchSet::from_records`] of records arriving from the log, plus the
/// non-directory inodes the names they remove or replace resolve to here
/// (see [`captured_files`]: the other side of the same gap).
fn incoming_touches(
    r: &impl Readable,
    meta: &Meta,
    records: &[LogRecord],
) -> Result<TouchSet, MetaError> {
    let mut set = TouchSet::from_records(records.iter());
    let target = |parent: u64, name: &str, set: &mut TouchSet| -> Result<(), MetaError> {
        if let Some(d) = ns::get_dentry_record(r, &meta.ns, parent, name)? {
            if d.kind != constellation_mtree::record::Kind::Dir {
                set.inos.insert(d.ino);
            }
        }
        Ok(())
    };
    for rec in records {
        match rec {
            LogRecord::Unlink { parent, name, .. } | LogRecord::Rmdir { parent, name, .. } => {
                target(*parent, name, &mut set)?
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            }
            | LogRecord::Exchange {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                target(*parent, name, &mut set)?;
                target(*new_parent, new_name, &mut set)?;
            }
            _ => {}
        }
    }
    Ok(set)
}

/// Plan 30 §M9: where [`Meta::install_streamed`] must rewind from to put
/// a streamed transaction in its log place, or `None` when installing it
/// at the end (or adopting the shadow `adopt` in place) gives the same
/// state. The stream is contiguous from the applied log, so every
/// outstanding shadow (its reply here, its transaction not streamed yet)
/// and `Local` row is later in the log than the transaction the stream
/// delivers now, except a delegate's own `Local` row whose append by this
/// root the stream delivered already (L1: [`row_is_ordered_at`], earlier
/// than everything the stream delivers after it), and everything tailed or
/// streamed is earlier than a shadow the stream confirms now (hints: see
/// [`row_is_ordered`]). Out of place:
/// - the transaction (not this node's) overlaps an outstanding shadow or
///   `Local` row at or after the oldest outstanding shadow;
/// - the confirmed shadow `adopt` overlaps a tailed, streamed or hint row
///   installed after it, or an outstanding shadow or `Local` row
///   installed before it.
fn streamed_out_of_place(
    r: &impl Readable,
    meta: &Meta,
    epoch: u64,
    adopt: Option<u64>,
    records: &[LogRecord],
) -> Result<Option<u64>, MetaError> {
    let Some(cutoff) = live_non_local(r, meta)?
        .into_iter()
        .filter(|(_, entry)| matches!(entry, LiveEntry::Shadow { .. }))
        .map(|(seq, _)| seq)
        .min()
    else {
        return Ok(None);
    };
    let live: HashMap<u64, LiveEntry> = read_live(r, meta)?.into_iter().collect();
    let rows = read_rows_from(r, &meta.spec, cutoff)?;
    let touches = match adopt {
        Some(x) => match rows.iter().find(|(seq, _)| *seq == x) {
            Some((_, row)) => TouchSet::from_records(row.records.iter()),
            None => return Ok(None),
        },
        None => incoming_touches(r, meta, records)?,
    };
    let mut marks = DelegMarks::new();
    for (seq, row) in &rows {
        // Lazily: a row's order is read only when the arm needs it, and
        // the arms that need it for live rows only test those.
        let misplaced = match adopt {
            Some(x) if *seq == x => false,
            Some(x) if *seq > x => row_is_ordered_at(r, meta, row, epoch, &mut marks)?,
            Some(_) | None => {
                live.contains_key(seq) && !row_is_ordered_at(r, meta, row, epoch, &mut marks)?
            }
        };
        if misplaced && row_touches(r, meta, row)?.overlaps(&touches) {
            return Ok(Some(cutoff));
        }
    }
    Ok(None)
}

/// The records that change the namespace: a transaction's identity as an
/// effect, without its outcome rows (`Completed`, `Refused`, `InboxAck`)
/// and `Atime`. A shadow's records are its reply's `applied` list; the
/// stream's are the holder's journal rows.
fn same_effect(a: &[LogRecord], b: &[LogRecord]) -> bool {
    let effect = |rec: &&LogRecord| {
        !matches!(
            rec,
            LogRecord::Completed { .. }
                | LogRecord::Refused { .. }
                | LogRecord::InboxAck { .. }
                | LogRecord::Atime { .. }
        )
    };
    a.iter().filter(effect).eq(b.iter().filter(effect))
}

/// [`Meta::install_streamed`]'s reordering, inside the caller's
/// transaction: roll every row from `cutoff` back, then redo in the log's
/// order — the tailed, streamed and hint rows (their relative order
/// kept), the streamed transaction (the confirmed shadow, now `Streamed`,
/// or a fresh row), then the outstanding shadows and `Local` rows (their
/// relative order kept). Every redone row moves to a fresh `spec_seq`.
/// Retired speculation is dropped as in [`rewind_tx`]. Usage moves are
/// staged into `staged`.
fn insert_streamed_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    cutoff: u64,
    t: StreamedTx<'_>,
) -> Result<(), MetaError> {
    let mut live: HashMap<u64, LiveEntry> = read_live(tx, meta)?.into_iter().collect();
    let rows = read_rows_from(tx, &meta.spec, cutoff)?;
    let plain = meta.dirty_for_ns();
    for (_, row) in rows.iter().rev() {
        for (key, before) in row.before.iter().rev() {
            restore_key_tx(tx, meta, plain, key, before.as_deref())?;
        }
        staged.adjust(-row.usage.0, -row.usage.1);
    }
    let adopt_seq = t.adopt.as_ref().map(|a| a.0);
    let mut adopted = None;
    let mut ahead = Vec::new();
    let mut behind = Vec::new();
    let mut marks = DelegMarks::new();
    for (seq, row) in rows {
        if Some(seq) == adopt_seq {
            adopted = Some((seq, row));
        } else if row_is_ordered_at(&*tx, meta, &row, t.epoch, &mut marks)? {
            ahead.push((seq, row));
        } else {
            behind.push((seq, row));
        }
    }
    let none = HashSet::new();
    let no_grants = HashSet::new();
    let mut out = Stranded::default();
    for (seq, row) in ahead {
        redo_row_tx(
            tx,
            meta,
            staged,
            &live,
            &none,
            &mut out,
            true,
            seq,
            row,
            &no_grants,
            Strand::PLAIN,
        )?;
    }
    match (adopted, t.adopt) {
        (Some((seq, mut row)), Some((_, rid, op, tag))) => {
            row.kind = SpecKind::Streamed {
                epoch: t.epoch,
                first: t.first,
                last: t.last,
                source: t.source,
                gated: t.gated || matches!(row.kind, SpecKind::Shadow { gated: true, .. }),
                own: Some((rid, op, tag)),
            };
            // The transaction's records are what the redo applies: the
            // shadow's were its reply's, which may carry less (a dedup
            // reply carries none; see `Meta::install_streamed`).
            row.records = t.records.to_vec();
            live.insert(
                seq,
                LiveEntry::Streamed {
                    epoch: t.epoch,
                    ident: super::member::tail_identity(t.records).unwrap_or((t.epoch, t.first)),
                    last: t.last,
                    gated: t.gated,
                },
            );
            redo_row_tx(
                tx,
                meta,
                staged,
                &live,
                &none,
                &mut out,
                true,
                seq,
                row,
                &no_grants,
                Strand::PLAIN,
            )?;
        }
        (None, None) => {
            let capture = Capture::new();
            let row_staged = UsageTracker::staging();
            let (_, applied) = {
                let cx = ApplyCx {
                    dirty: meta.dirty_capturing(&capture),
                    durable: false,
                };
                apply_batch_tx(
                    tx,
                    meta,
                    cx,
                    t.records,
                    &TouchSet::default(),
                    &row_staged,
                    true,
                )?
            };
            let usage = row_staged.raw_delta();
            staged.adjust(usage.0, usage.1);
            record_tx(
                tx,
                meta,
                SpecKind::Streamed {
                    epoch: t.epoch,
                    first: t.first,
                    last: t.last,
                    source: t.source,
                    gated: t.gated,
                    own: None,
                },
                applied,
                capture.into_before(),
                usage,
            )?;
        }
        _ => {
            return Err(MetaError::Invalid(format!(
                "streamed transaction {}..={}: the shadow it confirms is not at or after {cutoff}",
                t.first, t.last
            )))
        }
    }
    for (seq, row) in behind {
        redo_row_tx(
            tx,
            meta,
            staged,
            &live,
            &none,
            &mut out,
            true,
            seq,
            row,
            &no_grants,
            Strand::PLAIN,
        )?;
    }
    Ok(())
}

/// What [`unwind_shadow_for_append_tx`] leaves for the root's append to
/// redo once its own `Local` row is recorded: the shadows installed after
/// the one the append replaces, and the live entries as they were read
/// before the unwind (a redone row's entry moves with it).
pub(crate) struct AppendUnwind {
    live: HashMap<u64, LiveEntry>,
    behind: Vec<(u64, SpecRow)>,
}

/// Plan 45 §12 O1: the root is about to append a delegate's transaction
/// for `rid` (`Meta::apply_delegate_tx`) while it holds a live shadow of
/// that same rid — it forwarded the op to the delegate and installed the
/// reply. The shadow *is* that transaction's effect, installed ahead of
/// the stream; appending the records on top applies the op twice, over
/// whatever the root installed after the shadow (a second exchange of the
/// pair swapped back, a `create a` after `rename a b` moved over `b`).
/// The skip by state (`LogRecord::Exchange`'s inodes) only caught the
/// immediate repeat. So the shadow is taken out, by rid, and the append
/// goes in its place — the segment path's rewind (a shadow a segment
/// completes is rolled back and the segment applied in its place), for a
/// `Local` insertion. `install_streamed` adopts a shadow by rid the same
/// way.
///
/// The append's row goes after every other `Local` row (its journal rows
/// are the newest, and captured rows' `spec_seq`s must rise with journal
/// order: `local::first_captured_from`, hence compaction, relies on it)
/// and before the shadows that followed the replaced one. That is the
/// log's order at a root: its journal is the log's next rows in order
/// (tailed and shipped `Foreign` rows are in the log already), and a
/// live shadow is a delegate's row not appended yet, which joins the
/// journal after everything in it. Hints stay with the journaled rows,
/// in place: a hint never overlaps the speculation live at its install
/// (`Meta::install_hint_from`), so moving the shadows past it changes no
/// key it holds.
///
/// Two shapes, in the caller's transaction, before it opens its
/// `Local` bracket:
/// - nothing installed after the shadow touches its keys or the
///   append's: only the shadow's before-images are restored (the later
///   rows left those keys alone), and the append goes at the end;
/// - otherwise every row from the shadow on is rolled back, the
///   non-shadow rows are redone (fresh `spec_seq`s, their order kept),
///   and the later shadows are returned for [`redo_after_append_tx`],
///   which the caller runs once the append's row is recorded.
///
/// The shadow is dropped, not stranded: nothing is queued (the append is
/// its op, completing the rid) and nothing counts as rolled back. `None`:
/// no live shadow of `rid` here.
pub(crate) fn unwind_shadow_for_append_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    rid: Rid,
    records: &[LogRecord],
) -> Result<Option<AppendUnwind>, MetaError> {
    let Some((shadow_seq, ..)) = shadow_row_for(tx, meta, rid)? else {
        return Ok(None);
    };
    let Some(v) = tx.get(&meta.spec, seq_key(shadow_seq))? else {
        return Ok(None);
    };
    let shadow: SpecRow = crate::record::from_bytes_exact(&v)?;
    let later = read_rows_from(tx, &meta.spec, shadow_seq + 1)?;
    let shadow_keys: HashSet<&[u8]> = shadow.before.iter().map(|(k, _)| k.as_slice()).collect();
    let mut touches = incoming_touches(tx, meta, records)?;
    let theirs = row_touches(tx, meta, &shadow)?;
    touches.dentries.extend(theirs.dentries);
    touches.inos.extend(theirs.inos);
    touches.shared.extend(theirs.shared);
    let mut overlap = false;
    for (_, row) in &later {
        if row
            .before
            .iter()
            .any(|(k, _)| shadow_keys.contains(k.as_slice()))
            || row_touches(tx, meta, row)?.overlaps(&touches)
        {
            overlap = true;
            break;
        }
    }
    let drop_shadow = |tx: &mut SingleWriterWriteTx| -> Result<(), MetaError> {
        tx.remove(&meta.spec, seq_key(shadow_seq));
        tx.remove(&meta.spec_live, seq_key(shadow_seq));
        counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -1)
    };
    let live: HashMap<u64, LiveEntry> = read_live(tx, meta)?.into_iter().collect();
    if !overlap {
        let plain = meta.dirty_for_ns();
        for (key, before) in shadow.before.iter().rev() {
            restore_key_tx(tx, meta, plain, key, before.as_deref())?;
        }
        staged.adjust(-shadow.usage.0, -shadow.usage.1);
        drop_shadow(tx)?;
        tracing::debug!(?rid, shadow_seq, "root append replaces its shadow in place");
        return Ok(Some(AppendUnwind {
            live,
            behind: Vec::new(),
        }));
    }
    let plain = meta.dirty_for_ns();
    for (_, row) in later.iter().rev() {
        for (key, before) in row.before.iter().rev() {
            restore_key_tx(tx, meta, plain, key, before.as_deref())?;
        }
        staged.adjust(-row.usage.0, -row.usage.1);
    }
    for (key, before) in shadow.before.iter().rev() {
        restore_key_tx(tx, meta, plain, key, before.as_deref())?;
    }
    staged.adjust(-shadow.usage.0, -shadow.usage.1);
    drop_shadow(tx)?;
    let (behind, ahead): (Vec<_>, Vec<_>) = later
        .into_iter()
        .partition(|(_, row)| matches!(row.kind, SpecKind::Shadow { .. }));
    let none = HashSet::new();
    let mut out = Stranded::default();
    for (seq, row) in ahead {
        redo_row_tx(
            tx,
            meta,
            staged,
            &live,
            &none,
            &mut out,
            true,
            seq,
            row,
            &none,
            Strand::PLAIN,
        )?;
    }
    tracing::debug!(
        ?rid,
        shadow_seq,
        behind = behind.len(),
        "root append replaces its shadow under later speculation"
    );
    Ok(Some(AppendUnwind { live, behind }))
}

/// The second half of [`unwind_shadow_for_append_tx`]: redo the shadows
/// that followed the replaced one, on top of the append's row, under
/// fresh `spec_seq`s (their order kept).
pub(crate) fn redo_after_append_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    unwind: AppendUnwind,
) -> Result<(), MetaError> {
    let none = HashSet::new();
    let mut out = Stranded::default();
    for (seq, row) in unwind.behind {
        redo_row_tx(
            tx,
            meta,
            staged,
            &unwind.live,
            &none,
            &mut out,
            true,
            seq,
            row,
            &none,
            Strand::PLAIN,
        )?;
    }
    Ok(())
}

/// See [`Meta::streamed_tip`].
fn streamed_tip_tx(r: &impl Readable, meta: &Meta, epoch: u64) -> Result<Option<u64>, MetaError> {
    if counter_get(r, &meta.local, KV_SPEC_LIVE_COUNT)? == 0 {
        return Ok(None);
    }
    Ok(read_live(r, meta)?
        .into_iter()
        .filter_map(|(_, entry)| match entry {
            LiveEntry::Streamed { epoch: e, last, .. } if e == epoch => Some(last),
            _ => None,
        })
        .max())
}

/// Whether the log carries `rid`'s outcome already: a row applied from a
/// segment records position 0 in `completed`, a journaled one its journal seq.
fn log_carries(r: &impl Readable, meta: &Meta, rid: &Rid) -> Result<bool, MetaError> {
    Ok(r.get(&meta.completed, rid.to_key())?
        .and_then(|v| {
            v.get(0..8)
                .map(|p| u64::from_be_bytes(p.try_into().expect("8 bytes")))
        })
        .is_some_and(|position| position == 0))
}

/// Roll back and take out every outstanding entry `strands` selects (see
/// [`rewind_tx`]).
/// Seed 2560 (long-delegated-late-answers): the journal transactions (by
/// first journal seq) that are this node's appends, as the root of an
/// earlier tenure, of a delegate's rows (`gen` != 0, `epoch` > 0 and below
/// `epoch`, the tenure of the segment being applied) whose every outcome
/// (`Completed`, `Refused`) the segment carries (`completes`, `refused`)
/// under an identity other than the row's: not by its origin (`origins`;
/// a segment that carries the origin carries the row, and retires it as
/// its own delegate transaction).
///
/// Such a row stays in a deposed root's journal (`LiveEntry::stranded_by`:
/// the delegate re-streams it to the successor, whose segment retires it
/// here by origin, or the log's recall of the generation strands it). When
/// the successor lands the rid by another copy instead (a delegate that
/// took the root and replayed its own rows by rid at its gate, which ends
/// the generation with no record of it in the log), neither happens, and
/// the row would ship with this node's next tenure: the log would carry the
/// rid's outcome twice, and its second copy would apply over later
/// records. The log's outcome is the rid's (exactly once); the row goes
/// (rolled back, the segment applied in its place, not queued). A row of
/// the same transaction with an outcome the segment does not carry keeps
/// the whole transaction (it may be the outcome's only carrier).
///
/// A root's append of a delegate's row is one op with one outcome today
/// (`apply_delegate_tx`), so "every outcome settled" is the one outcome; an
/// outcome the log already carries counts as settled too, so a multi-outcome
/// append whose outcomes land in different segments would still go.
///
/// Found through `completed`: a root's append records its journal seq there
/// (position != 0; one applied from a segment records 0), so the cost is a
/// point read per outcome the segment carries.
fn superseded_root_appends_tx(
    r: &impl Readable,
    meta: &Meta,
    epoch: u64,
    origins: &[(u64, u64)],
    completes: &HashSet<Rid>,
    refused: &HashSet<Rid>,
) -> Result<HashSet<u64>, MetaError> {
    let mut out = HashSet::new();
    if (completes.is_empty() && refused.is_empty())
        || counter_get(r, &meta.local, KV_DELEG_SPEC_COUNT)? == 0
    {
        return Ok(out);
    }
    let settled = |rid: &Rid| -> Result<bool, MetaError> {
        Ok(completes.contains(rid) || refused.contains(rid) || log_carries(r, meta, rid)?)
    };
    for rid in completes.iter().chain(refused.iter()) {
        let position = r
            .get(&meta.completed, rid.to_key())?
            .and_then(|v| {
                v.get(0..8)
                    .map(|p| u64::from_be_bytes(p.try_into().expect("8 bytes")))
            })
            .unwrap_or(0);
        if position == 0 {
            continue;
        }
        let Some(guard) = r
            .range(&meta.journal_tx, ..=local::tx_key(position))
            .next_back()
        else {
            continue;
        };
        let (k, v) = guard.into_inner()?;
        let first = local::decode_tx_key(&k)?;
        let head: local::JournalTxHead = postcard::from_bytes(&v)?;
        if head.last < position
            || head.gen == 0
            || head.epoch == 0
            || head.epoch >= epoch
            || head.spec_seq.is_none()
            || origins.contains(&(head.gen, head.idx))
        {
            continue;
        }
        let records = local::journal_records(r, meta, first, head.last)?;
        let outcomes: Vec<&Rid> = records
            .iter()
            .filter_map(|(_, rec)| match rec {
                LogRecord::Completed { rid } | LogRecord::Refused { rid, .. } => Some(rid),
                _ => None,
            })
            .collect();
        // A root appends one delegate row as one op with one outcome
        // (`apply_delegate_tx`); a multi-outcome append would have to be
        // settled across segments (`settled` counts the log's), so fail
        // loudly in tests rather than ship twice.
        debug_assert!(
            outcomes.len() <= 1,
            "a root's append of a delegate's row carries one outcome"
        );
        let mut all = !outcomes.is_empty();
        for rid in &outcomes {
            if !settled(rid)? {
                all = false;
                break;
            }
        }
        if all {
            out.insert(first);
        }
    }
    if !out.is_empty() {
        tracing::info!(
            epoch,
            firsts = ?out,
            "a segment carries the outcomes of a past tenure's appends of delegate rows: superseded"
        );
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn strand_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    strands: impl Fn(&LiveEntry) -> bool,
    member: bool,
    recall: bool,
    hold: bool,
    quiet: impl Fn(&LiveEntry) -> bool,
) -> Result<Stranded, MetaError> {
    strand_tx_deleg(tx, meta, staged, strands, member, recall, hold, quiet, None)
}

/// [`strand_tx`] with `deleg_copy` ([`Strand::deleg_copy`]).
#[allow(clippy::too_many_arguments)]
fn strand_tx_deleg(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    strands: impl Fn(&LiveEntry) -> bool,
    member: bool,
    recall: bool,
    hold: bool,
    quiet: impl Fn(&LiveEntry) -> bool,
    deleg_copy: Option<&HashSet<u64>>,
) -> Result<Stranded, MetaError> {
    let live: HashMap<u64, LiveEntry> = read_live(tx, meta)?.into_iter().collect();
    let stranded: HashSet<u64> = live
        .iter()
        .filter(|(_, entry)| strands(entry))
        .map(|(seq, _)| *seq)
        .collect();
    let Some(&cutoff) = stranded.iter().min() else {
        return Ok(Stranded::default());
    };
    let quiet: HashSet<u64> = stranded
        .iter()
        .copied()
        .filter(|seq| live.get(seq).is_some_and(&quiet))
        .collect();
    let strand = Strand {
        member,
        quiet: Some(&quiet),
        recall,
        rebuild: false,
        hold,
        deps_of: None,
        superseded: false,
        deleg_copy,
    };
    Ok(rewind_tx(tx, meta, staged, &live, &stranded, cutoff, strand, None)?.0)
}

/// Whether a replica that has applied the log through `applied_seq` —
/// and, with it, the shipping tenures' journals through its recorded
/// journal position, joined with `segment`'s (the segment being applied,
/// whose position is recorded only at the end of its transaction) —
/// holds everything a hint's refusal was evaluated at (`at`), and so the
/// hint's entry, or what the log did to it since.
///
/// Not `at`'s next segment (`Position::hint_floor`), the rule before: a
/// holder answers with the last segment it *confirmed* shipped plus its
/// unshipped journal, so a segment already on its way to S3 — cut before
/// the winner's `Create` was journaled — is that next segment, and does
/// not carry the entry. Retired by it, the hint stayed only as long as
/// nothing rolled it back; the first rewind that reached it (the loser's
/// own earlier write, completed by that very segment) dropped it as
/// "carried by the segment", and the file the loser had just opened was
/// gone from its replica until the next segment: `fstat` answered
/// `ENOENT`, and a `close` meanwhile took the file for unlinked and never
/// published its write (harness `concurrent-create-no-excl`).
fn hint_reached(
    r: &impl Readable,
    meta: &Meta,
    at: &Position,
    applied_seq: u64,
    segment: Option<JournalPos>,
) -> Result<bool, MetaError> {
    if applied_seq < at.seq {
        return Ok(false);
    }
    if let Some(pending) = at.pending {
        let applied = crate::store::applied_pos_at(r, &meta.local).max(segment);
        if applied < Some(pending) {
            return Ok(false);
        }
    }
    // Plan 30 §M11: a delegate's refusal names the stream rows it held;
    // the log carries them once the root appended them (the persisted
    // per-generation index `apply_segment_rows` raises first).
    for (gen, idx) in at.streams.iter() {
        let logged = kv_get_tx(r, &meta.local, &local::deleg_log_idx_key(gen))?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if logged < idx {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The keys a segment's records change *after* position `at`: what a
/// replica that holds `at` has not seen yet. A segment at or below
/// `at.seq` is wholly inside it. Above it, a row of the answering
/// holder's tenure at or below `at.pending` was in its journal, and a
/// delegate's row at or below `at.streams`' index for its generation was
/// in the delegate's stream; anything else came later. (`rows`/`origins`
/// pair with the non-`Atime` records, in order; a segment without them
/// counts as within `at`, never as a later change.)
fn touched_after(
    at: &Position,
    seq: u64,
    epoch: u64,
    rows: &[u64],
    origins: &[(u64, u64)],
    records: &[LogRecord],
) -> TouchSet {
    let mut out = TouchSet::default();
    if seq <= at.seq || rows.is_empty() {
        return out;
    }
    let journaled = |jseq: u64| {
        at.pending
            .is_some_and(|p| epoch < p.epoch || (epoch == p.epoch && jseq <= p.jseq))
    };
    let mut row = 0;
    for rec in records {
        if matches!(rec, LogRecord::Atime { .. }) {
            continue;
        }
        let jseq = rows.get(row).copied();
        let (gen, idx) = origins.get(row).copied().unwrap_or((0, 0));
        row += 1;
        let streamed = gen != 0 && at.streams.get(gen).is_some_and(|held| idx <= held);
        if !(jseq.is_some_and(journaled) || streamed) {
            out.add(rec);
        }
    }
    out
}

/// Whether `later` (a segment's [`touched_after`]) changes the entry a
/// hint's `records` install: its name or its inode. Not its parent's
/// times — a create next to it says nothing about the entry.
fn supersedes(later: &TouchSet, records: &[LogRecord]) -> bool {
    let entry = TouchSet::from_records(records.iter());
    entry.dentries.iter().any(|d| later.dentries.contains(d))
        || entry.inos.iter().any(|i| later.inos.contains(i))
}

/// Retire every outstanding shadow whose rid `completes` names and every
/// hint the applied state has reached ([`hint_reached`]; `segment`: the
/// journal position of the segment being applied, if any) or that is in
/// `superseded` (the segment changes its entry after its position). A
/// streamed transaction retires only rows-confirmed
/// (`apply_segment_rows` converts it).
pub(crate) fn retire_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    completes: &HashSet<Rid>,
    applied_seq: u64,
    segment: Option<JournalPos>,
    superseded: &HashSet<u64>,
) -> Result<usize, MetaError> {
    let mut retired = 0;
    if counter_get(tx, &meta.local, KV_SPEC_LIVE_COUNT)? == 0 {
        return Ok(0);
    }
    let floor = counter_get(tx, &meta.local, KV_SPEC_FLOOR)?;
    let entries: Vec<(u64, LiveEntry)> = {
        let mut out = Vec::new();
        for guard in tx.range(&meta.spec_live, seq_key(floor)..) {
            let (k, v) = guard.into_inner()?;
            out.push((decode_seq(&k)?, crate::record::from_bytes_exact(&v)?));
        }
        out
    };
    for (seq, entry) in entries {
        let done = match entry {
            LiveEntry::Shadow { rid, .. } => completes.contains(&rid),
            LiveEntry::Hint { at, .. } => {
                superseded.contains(&seq) || hint_reached(&*tx, meta, &at, applied_seq, segment)?
            }
            // Rows-confirmed entries are converted by `apply_segment_rows`
            // (their records were skipped). One the tenure shipped past
            // without naming its rows was dropped on the holder, and is
            // stranded there before this runs (never confirmed: its
            // effects are not in the log).
            LiveEntry::Streamed { .. } => false,
            LiveEntry::Local { .. } => false,
        };
        if done {
            tx.remove(&meta.spec_live, seq_key(seq));
            retired += 1;
        }
    }
    counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -(retired as i64))?;
    Ok(retired)
}

/// The `spec_seq` of the oldest outstanding entry of any kind, looking at
/// `Local` transactions from journal seq `local_from` on (the caller knows
/// everything below is gone). Plan 30 §M3b cost: a counter read per kind,
/// plus one row read per kind that has anything outstanding — never a
/// scan of the retired history or of the outstanding backlog.
fn oldest_outstanding(
    r: &impl Readable,
    meta: &Meta,
    local_from: u64,
) -> Result<Option<u64>, MetaError> {
    let shadow = if counter_get(r, &meta.local, KV_SPEC_LIVE_COUNT)? > 0 {
        let floor = counter_get(r, &meta.local, KV_SPEC_FLOOR)?;
        match r.range(&meta.spec_live, seq_key(floor)..).next() {
            Some(guard) => Some(decode_seq(&guard.into_inner()?.0)?),
            None => None,
        }
    } else {
        None
    };
    // `journal_tx` is in journal order, and captured transactions' rows
    // are in the same order in `spec` (a renumbering keeps it), so the
    // first captured one is the oldest.
    let local = if counter_get(r, &meta.local, KV_LOCAL_SPEC_COUNT)? > 0 {
        local::first_captured_from(r, meta, local_from)?
    } else {
        None
    };
    Ok(match (shadow, local) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    })
}

/// Delete every row older than the oldest outstanding entry — all of
/// them when nothing is outstanding — and raise `spec_floor` to match.
/// Ranges from the floor, so the cost is the rows it deletes (plus any
/// tombstones a rollback or renumbering left in that window), never the
/// history deleted by earlier compactions.
fn compact_tx(tx: &mut SingleWriterWriteTx, meta: &Meta, local_from: u64) -> Result<(), MetaError> {
    let floor = counter_get(tx, &meta.local, KV_SPEC_FLOOR)?;
    let upto = match oldest_outstanding(tx, meta, local_from)? {
        Some(first_live) => first_live,
        // Nothing outstanding: every row may go, up to the next seq the
        // counter will hand out.
        None => match tx.get(&meta.local, KV_NEXT_SPEC_SEQ.as_bytes())? {
            Some(v) => decode_seq(&v)?,
            None => 1,
        },
    };
    if upto <= floor {
        return Ok(());
    }
    let doomed: Vec<Vec<u8>> = tx
        .range(&meta.spec, seq_key(floor)..seq_key(upto))
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for key in doomed {
        tx.remove(&meta.spec, key);
    }
    counter_set_tx(tx, &meta.local, KV_SPEC_FLOOR, upto);
    Ok(())
}

/// Where a `journal_tx` scan for outstanding transactions may start: past
/// the acked watermark.
fn journal_from(r: &impl Readable, meta: &Meta) -> Result<u64, MetaError> {
    Ok(crate::store::journal::acked_watermark(r, &meta.local)?.saturating_add(1))
}

/// Plan 30 §M3b retirement of this node's own work: the journal rows up
/// to `upto` just shipped (in the segment at `applied_seq`), so every
/// transaction ending at or before it is durable. Called in the ack's
/// transaction, *before* its journal rows are deleted: a retired `Local`
/// row that must survive compaction (something older is still
/// outstanding) is turned into a `Foreign` row carrying its records, so a
/// later rollback past it redoes it as the durable content it now is.
///
/// Plan 30 §M4 (poison-record isolation, `store::held`): a ship may skip
/// held-back transactions, so the shipped set is not always "everything
/// up to `upto`". With `only`, just the transactions it covers retire; a
/// held transaction below `upto` stays outstanding, and every shipped row
/// newer than it becomes a `Foreign` row — shipped out of order, but by
/// construction touching none of its keys. `None` means everything from
/// the acked watermark through `upto` (`Meta::ack_journal`).
pub(crate) fn retire_local_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    upto: u64,
    applied_seq: u64,
    only: Option<Shipped<'_>>,
) -> Result<Retired, MetaError> {
    // From the acked watermark (not the start of the keyspace): the cost
    // is what just shipped, not everything ever shipped.
    let from = journal_from(tx, meta)?;
    if upto < from {
        return Ok(Retired::default());
    }
    let mut shipped: Vec<(u64, local::JournalTxHead)> = Vec::new();
    // Plan 30 §M4: a transaction in the shipped range that did not ship
    // was held back (`store::held`). Noticed for free on the scan this
    // retirement does anyway, and it is what decides whether any of the
    // held-set work below is needed at all.
    let mut held_below = false;
    for guard in tx.range(&meta.journal_tx, local::tx_key(from)..=local::tx_key(upto)) {
        let (k, v) = guard.into_inner()?;
        let row: local::JournalTxHead = postcard::from_bytes(&v)?;
        let first = local::decode_tx_key(&k)?;
        let in_ship = match only {
            Some(Shipped::Run(start)) => first >= start,
            Some(Shipped::Set(set)) => set.contains(&first),
            None => true,
        };
        if row.last <= upto && in_ship {
            shipped.push((first, row));
        } else {
            held_below = true;
        }
    }
    if shipped.is_empty() {
        return Ok(Retired {
            retired: 0,
            held_below,
        });
    }
    let captured = shipped
        .iter()
        .filter(|(_, row)| row.spec_seq.is_some())
        .count();
    for (first, row) in &shipped {
        tx.remove(&meta.journal_tx, local::tx_key(*first));
        local::remove_journal_origin(tx, meta, row.gen, row.idx, *first);
    }
    counter_add_tx(tx, &meta.local, KV_LOCAL_SPEC_COUNT, -(captured as i64))?;
    let deleg = shipped
        .iter()
        .filter(|(_, row)| row.gen != 0 && row.spec_seq.is_some())
        .count();
    if deleg > 0 {
        counter_add_tx(tx, &meta.local, KV_DELEG_SPEC_COUNT, -(deleg as i64))?;
    }
    counter_add_tx(
        tx,
        &meta.local,
        KV_UNCAPTURED_TX_COUNT,
        -((shipped.len() - captured) as i64),
    )?;
    // A retired row outlives compaction when an older entry is still
    // outstanding: a requester shadow or hint, or (plan 30 §M4) one of
    // this node's own transactions held back below `upto`. Only then does
    // it need converting. `Local` rows otherwise retire in journal order,
    // so with nothing held and no requester entry outstanding there is no
    // boundary and nothing to look up — M3b's fast path, which plan 30 §M4
    // round 1 lost by always computing it from `from` (a counter read per
    // kind plus a `journal_tx` walk over the just-deleted range, twice
    // with the compaction below, on every ack).
    let (boundary, compact_from) = if held_below {
        (oldest_outstanding(tx, meta, from)?, from)
    } else if counter_get(tx, &meta.local, KV_SPEC_LIVE_COUNT)? > 0 {
        (
            oldest_outstanding(tx, meta, upto.saturating_add(1))?,
            upto.saturating_add(1),
        )
    } else {
        (None, upto.saturating_add(1))
    };
    let mut retired = 0;
    for (first, row) in &shipped {
        let Some(seq) = row.spec_seq else { continue };
        retired += 1;
        if !boundary.is_some_and(|b| seq > b) {
            continue;
        }
        if let Some(v) = tx.get(&meta.spec, seq_key(seq))? {
            let mut spec_row: SpecRow = crate::record::from_bytes_exact(&v)?;
            spec_row.kind = SpecKind::Foreign {
                segment_seq: applied_seq,
            };
            spec_row.records = local::journal_records(&*tx, meta, *first, row.last)?
                .into_iter()
                .map(|(_, rec)| rec)
                .collect();
            put_row(tx, &meta.spec, seq, &spec_row)?;
        }
    }
    compact_tx(tx, meta, compact_from)?;
    Ok(Retired {
        retired,
        held_below,
    })
}

/// Which journal rows a ship covered, for [`retire_local_tx`] (plan 30
/// §M4): the ordinary contiguous run — checked without building a set —
/// or, when held transactions were skipped, the exact set of seqs.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Shipped<'a> {
    /// Every row from this seq through `upto`.
    Run(u64),
    Set(&'a HashSet<u64>),
}

/// What [`retire_local_tx`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Retired {
    /// Captured transactions retired.
    pub retired: usize,
    /// A transaction inside the shipped range did not ship (it is held
    /// back, `store::held`): the journal's acked watermark must stop below
    /// it (`journal::ack_rows_at`).
    pub held_below: bool,
}

/// The earliest before-image of every key touched by the rows from
/// `first` (the oldest outstanding `Local` row) on, for the publisher
/// (`Meta::publish_basis_at`).
///
/// Plan 30 §M4: a `Foreign` row in that range is one of this node's own
/// transactions that shipped while an older one was held back
/// (`store::held`). It is in the log, so its keys keep their current
/// values — which is right only if it touched none of the keys an older
/// outstanding row did (the holdback rule guarantees exactly that: a
/// transaction touching a held key is held too). A later outstanding row
/// may touch its keys; that row's before-image then already includes it.
/// `None` if a `Foreign` row does overlap, or anything but `Local` and
/// `Foreign` rows sits in the range: the overlay would not be the log
/// prefix, so the publish defers.
pub(crate) fn local_before_images_from(
    r: &impl Readable,
    meta: &Meta,
    first: u64,
) -> Result<Option<LogPrefixView>, MetaError> {
    let mut view = LogPrefixView::default();
    for (_, row) in read_rows_from(r, &meta.spec, first)? {
        match row.kind {
            SpecKind::Local { .. } => {
                for (key, before) in &row.before {
                    view.note(key, before);
                }
            }
            SpecKind::Foreign { .. } => {
                if row.before.iter().any(|(key, _)| view.contains(key)) {
                    return Ok(None);
                }
            }
            SpecKind::Shadow { .. } | SpecKind::Hint { .. } | SpecKind::Streamed { .. } => {
                return Ok(None)
            }
        }
    }
    Ok(Some(view))
}

/// The namespace is about to be replaced wholesale from a side replica
/// built from the shared log (a deposed holder's fallback rebuild, plan
/// 30 §M3b): every before-image here is meaningless against it. Queue the
/// outstanding shadows and every one of this node's unshipped
/// transactions for replay by rid (their effects are not in the log-built
/// namespace; M2 dedup makes a replay of one that did land harmless),
/// except a gated append of a delegate's row, which its generation
/// re-delivers (`Strand::rebuild`), and drop everything else.
pub(crate) fn reset_for_rebuilt_ns_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    rebuilt_pos: Option<crate::session::JournalPos>,
) -> Result<(), MetaError> {
    let live = read_live(tx, meta)?;
    // Plan 43 C1-4: a committed shadow or streamed transaction is not this
    // node's to re-execute and not to drop: dropping it would show a
    // committed effect disappearing to this node's clients, and queueing it
    // would execute a row its group holds a second time. It is re-installed
    // on the rebuilt namespace below, unless the log the rebuild carries
    // completes it or (a streamed one) carries it by position: the rebuilt
    // log's applied position (`rebuilt_pos`) and what it says of each
    // epoch's tail (`member::log_carries_tx`).
    let mut kept: Vec<(u64, LiveEntry)> = Vec::new();
    for (seq, entry) in &live {
        // (Not a delegate's committed answer's shadow, `gen` != 0: the
        // rebuilt log may have ended its generation below the row already,
        // and no later segment would then strand it; it is queued as
        // before, held while the generation is live.)
        if entry.gated()
            && matches!(
                entry,
                LiveEntry::Shadow { gen: 0, .. } | LiveEntry::Streamed { .. }
            )
        {
            kept.push((*seq, *entry));
            continue;
        }
        if let LiveEntry::Shadow { .. } = entry {
            if let Some(v) = tx.get(&meta.spec, seq_key(*seq))? {
                let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                if let SpecKind::Shadow {
                    rid,
                    op,
                    gen,
                    lock_tag,
                    ..
                } = row.kind
                {
                    enqueue_replay_gen_tx(
                        tx,
                        meta,
                        row.origin,
                        rid,
                        op,
                        lock_tag,
                        gen,
                        &row.before,
                    )?;
                }
            }
            tx.remove(&meta.spec_live, seq_key(*seq));
        }
        if let LiveEntry::Hint { .. } | LiveEntry::Streamed { .. } = entry {
            tx.remove(&meta.spec_live, seq_key(*seq));
        }
    }
    counter_set_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, kept.len() as u64);
    let journal = local::read_journal_txs(&*tx, meta)?;
    let undone = undone_grants(&*tx, meta, journal.iter().map(|(first, _)| *first))?;
    // M2: every transaction's `deps` on the delegates' rows ahead of it,
    // from the whole journal. The loop below takes each row out before the
    // next is examined, so a pass that read them as it went would find
    // none; a root's own replay must still wait for the delegate rows it
    // was ordered after, above all the ones this pass drops
    // (`Strand::rebuild`), which no queue entry orders it behind. The
    // empty case is a deposed root's retention rebuild (no epoch strand
    // before it) and the own rows the epoch strand leaves; on
    // `recover_deposed` that strand already orders the captured own rows.
    let deps_of = stranded_deps_by_tx(&*tx, meta, &undone)?;
    for (first, row) in journal {
        let (key, before) = match row.spec_seq {
            Some(seq) => match tx.get(&meta.spec, seq_key(seq))? {
                Some(v) => {
                    let spec_row = crate::record::from_bytes_exact::<SpecRow>(&v)?;
                    (spec_row.origin, spec_row.before)
                }
                None => (seq, Vec::new()),
            },
            None => (next_spec_seq_tx(tx, &meta.local)?, Vec::new()),
        };
        strand_local_tx(
            tx,
            meta,
            first,
            key,
            &before,
            &undone,
            Strand {
                member: true,
                quiet: None,
                recall: false,
                rebuild: true,
                hold: false,
                deps_of: Some(&deps_of),
                superseded: false,
                deleg_copy: None,
            },
        )?;
    }
    // The committed entries, in their spec order, on the rebuilt namespace.
    kept.sort_by_key(|(seq, _)| *seq);
    let kept_live: HashMap<u64, LiveEntry> = kept.iter().copied().collect();
    let mut out = Stranded::default();
    for (seq, entry) in kept {
        let Some(v) = tx.get(&meta.spec, seq_key(seq))? else {
            tx.remove(&meta.spec_live, seq_key(seq));
            counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -1)?;
            continue;
        };
        let row: SpecRow = crate::record::from_bytes_exact(&v)?;
        let completing: Option<Rid> = match &row.kind {
            SpecKind::Shadow { rid, .. } => Some(*rid),
            _ => row.records.iter().find_map(|r| match r {
                LogRecord::Completed { rid } => Some(*rid),
                _ => None,
            }),
        };
        let in_log = match completing {
            Some(rid) => tx
                .get(&meta.completed, rid.to_key())?
                .and_then(|v| {
                    v.get(0..8)
                        .map(|p| u64::from_be_bytes(p.try_into().expect("8 bytes")))
                })
                .is_some_and(|position| position == 0),
            None => false,
        };
        // A streamed transaction the rebuilt log carries by position (its
        // own epoch's segments, or a successor's re-ship naming its
        // identity): retired too. Matched by rid alone, one that completes
        // none was redone over the namespace holding it already.
        let in_log = in_log
            || match entry {
                LiveEntry::Streamed {
                    epoch, ident, last, ..
                } => {
                    rebuilt_pos.is_some_and(|p| p.epoch == epoch && p.jseq >= last)
                        || super::member::log_carries_tx(&*tx, meta, epoch, ident, last)?
                }
                _ => false,
            };
        if in_log {
            // The rebuilt log carries it: retired.
            tx.remove(&meta.spec, seq_key(seq));
            tx.remove(&meta.spec_live, seq_key(seq));
            counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -1)?;
            continue;
        }
        redo_row_tx(
            tx,
            meta,
            &UsageTracker::staging(),
            &kept_live,
            &HashSet::new(),
            &mut out,
            false,
            seq,
            row,
            &HashSet::new(),
            Strand::PLAIN,
        )?;
    }
    // Every journaled transaction is gone now: drop every row.
    let from = journal_from(tx, meta)?;
    compact_tx(tx, meta, from)?;
    Ok(())
}

// ------------------------------------------------- poison isolation

/// The `ns` keys a captured row touched, and the `spec_seq` it was first
/// recorded under.
type RowKeysAndOrigin = Option<(Vec<Vec<u8>>, u64)>;

/// Plan 30 §M4 (`store::held`): the `ns` keys a captured row touched and
/// the `spec_seq` it was first recorded under, without its values.
pub(crate) fn row_keys_and_origin(
    r: &impl Readable,
    meta: &Meta,
    seq: u64,
) -> Result<RowKeysAndOrigin, MetaError> {
    match r.get(&meta.spec, seq_key(seq))? {
        Some(v) => {
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            Ok(Some((
                row.before.into_iter().map(|(key, _)| key).collect(),
                row.origin,
            )))
        }
        None => Ok(None),
    }
}

/// Plan 30 §M4's `repair drop-held`: roll back the captured transactions
/// whose `spec_seq`s are `seqs` (and nothing else), in the caller's
/// transaction. Exactly a deposition's stranding (`rewind_tx`), restricted
/// to these rows: their effects are undone, their journal rows deleted,
/// their ops queued for replay by rid under their origin; everything after
/// them that still stands is redone.
pub(crate) fn strand_seqs_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    seqs: &HashSet<u64>,
) -> Result<Stranded, MetaError> {
    let live: HashMap<u64, LiveEntry> = read_live(tx, meta)?.into_iter().collect();
    let stranded: HashSet<u64> = seqs
        .iter()
        .copied()
        .filter(|seq| matches!(live.get(seq), Some(LiveEntry::Local { .. })))
        .collect();
    let Some(&cutoff) = stranded.iter().min() else {
        return Ok(Stranded::default());
    };
    let out = rewind_tx(
        tx,
        meta,
        staged,
        &live,
        &stranded,
        cutoff,
        Strand::PLAIN,
        None,
    )?
    .0;
    let from = journal_from(tx, meta)?;
    compact_tx(tx, meta, from)?;
    Ok(out)
}

/// Plan 46 M0.2 (R1): turn the queued replay at `key` — the stranded
/// transaction whose records are `records` — into a placeholder
/// ([`AwaitRetry`]) when it stands for another node's op whose answer was
/// never given. Returns whether it did.
///
/// *Never given* is read from what the store knows, and only what it can
/// know for certain:
/// - the rid has no outcome anywhere: the strand removed the `completed`
///   row of the rolled-back transaction (`strand_local_tx`), and a row an
///   applied segment carries (position 0) stays: the log has the rid, the
///   drain finds it done, and nothing needs a place;
/// - the row was journaled `Archive`: a lone sequencer's answer to another
///   node waits for an applied segment, and a held transaction (the only
///   kind an operator drops) never ships. A `RootGroup`/`DelegateNodeGroup` row's
///   answer leaves once the group has the row, which only the core knows
///   (the group's acknowledgements are its memory): it is a placeholder
///   only when `unanswered(rid)` says the answer certainly has not left —
///   it still waits, parked in the core — and otherwise (it may have left;
///   after a restart nothing says) the entry stays the only copy that can
///   execute. A
///   `NonStealable` (hold) answer left at once; a `Lone` one is the
///   node's own. Those keep their op. A reclassed row
///   (`reclass_journal_tx`: the answer left with the gate off) is no
///   longer `Archive`, so it is kept too.
///
/// The entry keeps its place (`key`) in the queue, its rid, and the keys
/// its records touched; its op becomes inert and its class `Lone`, so it
/// is never an executable copy (the simulation's I2 (b) union, the
/// drain's execution) nor a gated one.
pub(crate) fn mark_await_retry_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    keys: TouchSet,
    since_ms: i64,
    unanswered: &dyn Fn(Rid) -> bool,
) -> Result<Option<Rid>, MetaError> {
    let Some(v) = tx.get(&meta.pending_replay, seq_key(key))? else {
        return Ok(None);
    };
    let mut row: QueuedReplay = crate::record::from_bytes_exact(&v)?;
    let node: u64 = kv_get_tx(&*tx, &meta.local, KV_NODE_PREFIX)?
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let never_given = if row.class.is_archive() {
        true
    } else if row.class.gated() {
        unanswered(row.rid)
    } else {
        false
    };
    if row.rid.node == node
        || !never_given
        || row.refused.is_some()
        || row.await_retry.is_some()
        || tx.get(&meta.completed, row.rid.to_key())?.is_some()
    {
        return Ok(None);
    }
    row.op = MutateOp::Records {
        records: Vec::new(),
    };
    row.await_retry = Some(AwaitRetry { keys, since_ms });
    row.class = RowCommit::LONE;
    row.truncate_base = None;
    row.lock_tag = LockTag::NONE;
    row.deps = Streams::NONE;
    row.gen = 0;
    row.foreign = false;
    row.source = None;
    tx.insert(
        &meta.pending_replay,
        seq_key(key),
        postcard::to_allocvec(&row)?,
    );
    counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_GEN, 1)?;
    Ok(Some(row.rid))
}

/// Replace the queued replay at `key` with `op`, already refused for
/// `refusal`: the drain then materializes `op`'s conflict copy instead of
/// replaying it (`cli::recovery`). Used by `repair drop-held`, whose
/// dropped transaction must never be re-executed.
pub(crate) fn refuse_queued_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    op: MutateOp,
    refusal: Refusal,
    dropped: bool,
) -> Result<(), MetaError> {
    if tx.get(&meta.pending_replay, seq_key(key))?.is_none() {
        counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_COUNT, 1)?;
    }
    let row = QueuedReplay {
        rid,
        op,
        refused: Some(refusal),
        foreign: false,
        source: None,
        gen: 0,
        truncate_base: None,
        lock_tag: LockTag::NONE,
        deps: Streams::NONE,
        class: RowCommit::LONE,
        await_retry: None,
        dropped,
    };
    tx.insert(
        &meta.pending_replay,
        seq_key(key),
        postcard::to_allocvec(&row)?,
    );
    counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_GEN, 1)?;
    Ok(())
}

/// Take every queued replay of a rid in `rids` out of the queue (the
/// log refused the rid: nothing to replay). Returns how many.
pub(crate) fn drop_queued_of_rids_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    rids: &HashSet<Rid>,
) -> Result<usize, MetaError> {
    let mut dropped = 0;
    for queued in read_pending_replays(&*tx, meta)? {
        if rids.contains(&queued.rid) {
            tx.remove(&meta.pending_replay, seq_key(queued.queue_seq));
            counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_GEN, 1)?;
            counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_COUNT, -1)?;
            dropped += 1;
        }
    }
    Ok(dropped)
}

/// The queued replay at `key`, if any: `(rid, op)`.
pub(crate) fn queued_at(
    r: &impl Readable,
    meta: &Meta,
    key: u64,
) -> Result<Option<(Rid, MutateOp)>, MetaError> {
    match r.get(&meta.pending_replay, seq_key(key))? {
        Some(v) => {
            let row: QueuedReplay = crate::record::from_bytes_exact(&v)?;
            Ok(Some((row.rid, row.op)))
        }
        None => Ok(None),
    }
}

/// Every queued replay, oldest first, read through `r` (see
/// [`Meta::pending_replays`]).
pub(crate) fn read_pending_replays(
    r: &impl Readable,
    meta: &Meta,
) -> Result<Vec<StrandedOp>, MetaError> {
    let mut out = Vec::new();
    if counter_get(r, &meta.local, KV_PENDING_REPLAY_COUNT)? == 0 {
        return Ok(out);
    }
    for guard in r.iter(&meta.pending_replay) {
        let (k, v) = guard.into_inner()?;
        let row: QueuedReplay = crate::record::from_bytes_exact(&v)?;
        out.push(StrandedOp {
            queue_seq: decode_seq(&k)?,
            rid: row.rid,
            op: row.op,
            refused: row.refused,
            foreign: row.foreign,
            source: row.source,
            gen: row.gen,
            lock_tag: row.lock_tag,
            deps: row.deps,
            truncate_base: row.truncate_base,
            class: row.class,
            await_retry: row.await_retry,
            dropped: row.dropped,
        });
    }
    Ok(out)
}

/// Plan 46 M0.2 review: the replay queue, decoded once per generation of
/// the queue (`KV_PENDING_REPLAY_GEN`, bumped in the transaction of every
/// change), with the keys each entry touches. A forward, an own op and a
/// fast-path write ask the queue whether it holds an entry ahead of them on
/// their keys; decoding the whole queue and computing every entry's keys on
/// each such question cost O(queue) per op for as long as any queue
/// existed (an ordinary post-deposition queue included).
///
/// The keys are the entry's own except for an op whose keys depend on the
/// namespace as it stands now (an unlink's or rename's target inode, which
/// the keys name when it resolves): those are computed at the question
/// ([`QueueSnapshot::keys_of`]), the rest are fixed with the entry.
#[derive(Debug, Default)]
pub struct QueueSnapshot {
    /// The generation this was read at.
    pub gen: u64,
    pub entries: Vec<StrandedOp>,
    keys: Vec<TouchSet>,
    lookup_dependent: Vec<bool>,
}

impl QueueSnapshot {
    fn build(gen: u64, entries: Vec<StrandedOp>) -> Self {
        let mut keys = Vec::with_capacity(entries.len());
        let mut lookup_dependent = Vec::with_capacity(entries.len());
        for e in &entries {
            let (k, dep) = match &e.await_retry {
                Some(a) => (a.keys.clone(), false),
                None => {
                    let mut k = TouchSet::from_op(&e.op);
                    k.inos.extend(created_ino_of(&e.op));
                    (k, op_keys_depend_on_lookup(&e.op))
                }
            };
            keys.push(k);
            lookup_dependent.push(dep);
        }
        QueueSnapshot {
            gen,
            entries,
            keys,
            lookup_dependent,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The keys entry `i` touches; `lookup` resolves a name to its inode
    /// for the ops whose keys name what they take away or move.
    pub fn keys_of(
        &self,
        i: usize,
        lookup: &dyn Fn(u64, &str) -> Option<u64>,
    ) -> std::borrow::Cow<'_, TouchSet> {
        if !self.lookup_dependent[i] {
            return std::borrow::Cow::Borrowed(&self.keys[i]);
        }
        let e = &self.entries[i];
        let mut k = TouchSet::from_op_in(&e.op, lookup);
        k.inos.extend(created_ino_of(&e.op));
        std::borrow::Cow::Owned(k)
    }
}

/// The inode `op` creates (its new name's): the op form names it, but its
/// keys leave it out (`TouchSet::from_op`; the `Records` form counts it).
pub fn created_ino_of(op: &MutateOp) -> Option<constellation_fs_core::Ino> {
    match op {
        MutateOp::Create { ino, .. }
        | MutateOp::Mkdir { ino, .. }
        | MutateOp::Symlink { ino, .. }
        | MutateOp::Mknod { ino, .. } => Some(*ino),
        _ => None,
    }
}

fn op_keys_depend_on_lookup(op: &MutateOp) -> bool {
    match op {
        MutateOp::Unlink { .. }
        | MutateOp::Rmdir { .. }
        | MutateOp::Rename { .. }
        | MutateOp::Exchange { .. } => true,
        MutateOp::Records { records } => records.iter().any(|r| {
            matches!(
                r,
                LogRecord::Unlink { .. }
                    | LogRecord::Rmdir { .. }
                    | LogRecord::Rename { .. }
                    | LogRecord::Exchange { .. }
            )
        }),
        _ => false,
    }
}

// ------------------------------------------------------------ Meta API

impl Meta {
    /// Install a forwarded op's accepted records ahead of the log, as a
    /// `Shadow` entry (`forward::apply_accepted`). Returns `false`, and
    /// installs nothing, when:
    /// - this replica's applied log already carries `rid`'s completion:
    ///   the reply lost a race with this node's own tail, so the effect is
    ///   already part of the log prefix, and re-applying it on top of
    ///   later records could move the replica backwards (and would leave
    ///   a shadow that nothing ever retires);
    /// - plan 30 §M3b: this node holds the lease at a higher epoch than
    ///   the one that accepted the op (the reply raced this node's own
    ///   takeover). That epoch can no longer reach the log, so a shadow
    ///   would only let this holder validate against phantom state until
    ///   something stranded it; the op is queued for replay by rid
    ///   instead, in the same transaction, and the replay runs it here.
    pub fn install_shadow(
        &self,
        rid: Rid,
        epoch: u64,
        op: &MutateOp,
        records: &[LogRecord],
    ) -> Result<bool, MetaError> {
        self.install_shadow_from(rid, epoch, 0, op, &LockTag::NONE, records)
    }

    /// Chunk metered-own-rows: which of `rids`' ops this replica carries
    /// the effect of — the log delivered it (`completed`), or speculation
    /// of it is outstanding (a shadow, or a streamed transaction
    /// completing it). A stranded one is neither: rolled back and queued
    /// for replay. One read of the live list answers them all (a reply
    /// names up to `OWN_ROWS_CAP` transactions).
    pub fn effects_installed(
        &self,
        rids: &[Rid],
    ) -> Result<std::collections::BTreeSet<Rid>, MetaError> {
        let r = self.db.read_tx();
        let mut found = std::collections::BTreeSet::new();
        let mut open = std::collections::BTreeSet::new();
        for rid in rids {
            if r.get(&self.completed, rid.to_key())?.is_some() {
                found.insert(*rid);
            } else {
                open.insert(*rid);
            }
        }
        if open.is_empty() {
            return Ok(found);
        }
        for (seq, entry) in read_live(&r, self)? {
            match entry {
                LiveEntry::Shadow { rid, .. } if open.contains(&rid) => {
                    // As `shadow_row_for`: the row must still be a shadow.
                    if let Some(v) = r.get(&self.spec, seq_key(seq))? {
                        let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                        if matches!(row.kind, SpecKind::Shadow { .. }) {
                            open.remove(&rid);
                            found.insert(rid);
                        }
                    }
                }
                LiveEntry::Streamed { .. } => {
                    if let Some(v) = r.get(&self.spec, seq_key(seq))? {
                        let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                        for rec in &row.records {
                            if let LogRecord::Completed { rid } = rec {
                                if open.remove(rid) {
                                    found.insert(*rid);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
            if open.is_empty() {
                break;
            }
        }
        Ok(found)
    }

    /// The OVH run's finding 4: an accepted forward this replica could
    /// not install (the holder evaluated it behind unshipped work on its
    /// keys) whose transaction the holder's pre-S3 stream has installed
    /// here since: adopt it (see `adopt_streamed_tx`) and report `true` —
    /// the op is answered now, not when its segment reaches S3. `false`:
    /// the stream has not carried it (or the log has: `completed`
    /// answers it then).
    pub fn adopt_streamed(
        &self,
        rid: Rid,
        op: &MutateOp,
        tag: &LockTag,
    ) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.completed, rid.to_key())?.is_some() {
            return Ok(false);
        }
        if adopt_streamed_tx(&mut tx, self, rid, op, tag)? {
            tx.commit()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// [`Self::install_shadow`] for a reply accepted by a delegate of
    /// generation `gen` (plan 30 §M11; 0: the root).
    pub fn install_shadow_from(
        &self,
        rid: Rid,
        epoch: u64,
        gen: u64,
        op: &MutateOp,
        tag: &LockTag,
        records: &[LogRecord],
    ) -> Result<bool, MetaError> {
        self.install_shadow_committed(rid, epoch, gen, op, tag, records, false)
    }

    /// [`Self::install_shadow_from`] for a reply the sequencer answered as
    /// `committed`, and, `non_stealable`, as a non-stealable sequencer's
    /// (plan 46 M0.2, R7): see [`SpecKind::Shadow::non_stealable`].
    #[allow(clippy::too_many_arguments)]
    pub fn install_shadow_answered(
        &self,
        rid: Rid,
        epoch: u64,
        gen: u64,
        op: &MutateOp,
        tag: &LockTag,
        records: &[LogRecord],
        (committed, non_stealable): (bool, Option<i64>),
    ) -> Result<bool, MetaError> {
        self.install_shadow_inner(
            rid,
            epoch,
            gen,
            op,
            tag,
            records,
            (committed, non_stealable),
        )
    }

    /// [`Self::install_shadow_from`] for a reply the holder answered as
    /// committed (`committed`, plan 43 C1-4: its group or the archive held
    /// the row): the shadow is not stranded by a takeover of its epoch.
    #[allow(clippy::too_many_arguments)]
    pub fn install_shadow_committed(
        &self,
        rid: Rid,
        epoch: u64,
        gen: u64,
        op: &MutateOp,
        tag: &LockTag,
        records: &[LogRecord],
        committed: bool,
    ) -> Result<bool, MetaError> {
        self.install_shadow_inner(rid, epoch, gen, op, tag, records, (committed, None))
    }

    #[allow(clippy::too_many_arguments)]
    fn install_shadow_inner(
        &self,
        rid: Rid,
        epoch: u64,
        gen: u64,
        op: &MutateOp,
        tag: &LockTag,
        records: &[LogRecord],
        (committed, non_stealable): (bool, Option<i64>),
    ) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.completed, rid.to_key())?.is_some() {
            return Ok(false);
        }
        // Plan 30 §M9: the holder's pre-S3 stream may have carried this
        // very op here before its reply (the requester is a subscriber
        // too): it is installed already, as `Streamed`. Applying the
        // records again on top of whatever was streamed *after* it would
        // not converge (backup seed 753: the shadow's unlink removed the
        // name a later streamed create had put back). The streamed entry
        // stays what it is — the segment's skip is the retirement that
        // converges for streamed rows — but adopts the op as this node's
        // own: stranded by a takeover, it replays by rid as a shadow
        // would (a conflict copy on refusal). The reply counts as
        // installed (backup seeds 1122 and 1407: "not installed" sent
        // the op down the lease path a second time).
        if adopt_streamed_tx(&mut tx, self, rid, op, tag)? {
            tx.commit()?;
            return Ok(true);
        }
        if self.holder_epoch() > epoch {
            let key = next_spec_seq_tx(&mut tx, &self.local)?;
            // A delegate's acceptance keeps its generation: the queued
            // replay is held while the generation is live (the delegate
            // re-streams the op to this new root in its stream order), as
            // a stranded shadow of it would be — replayed at once, the
            // takeover gate executed it ahead of the delegate's earlier
            // transactions (locks-released-delegated seed 198670).
            enqueue_replay_gen_tx(&mut tx, self, key, rid, op.clone(), tag.clone(), gen, &[])?;
            tx.commit()?;
            tracing::info!(
                ?rid,
                epoch,
                holder_epoch = self.holder_epoch(),
                "a forward reply from an older epoch arrived after this node's takeover; \
                 queued for replay instead of installed"
            );
            return Ok(false);
        }
        let kind = SpecKind::Shadow {
            rid,
            epoch,
            op: op.clone(),
            gen,
            lock_tag: tag.clone(),
            gated: committed,
            non_stealable,
        };
        self.install_speculative_tx(tx, UsageTracker::staging(), kind, records)?;
        Ok(true)
    }

    /// Install the entry behind an `Exists` refusal ahead of the log, as a
    /// `Hint` entry: it retires once the applied log holds everything the
    /// answering holder had (`at`, the reply's position; see
    /// `hint_reached`), and is rolled back if a segment from a later epoch
    /// than the answering holder's arrives first. `false`: not installed
    /// (see [`Self::install_hint_from`]).
    pub fn install_hint(
        &self,
        records: &[LogRecord],
        at: Position,
        epoch: u64,
    ) -> Result<bool, MetaError> {
        self.install_hint_from(None, records, at, epoch, 0)
    }

    /// [`Self::install_hint`] for the refusal of `rid` (when known),
    /// answered by a delegate of generation `gen` (plan 30 §M11; 0: the
    /// root). Returns `false`, and installs nothing, when this replica is
    /// already at or past the state the entry was read from:
    /// - the applied log carries `rid`'s refusal;
    /// - plan 30 §M9: the holder's pre-S3 stream carried the refusal here
    ///   before the reply (which waits for the backup's acknowledgement
    ///   of it), as a `Streamed` entry. The stream installs the holder's
    ///   transactions contiguously, so what it streamed *after* the
    ///   refusal — a rename onto the refused name, an unlink of it — is
    ///   applied already; the hint, read before those, would put the old
    ///   entry back over them, and the segment (skipping the streamed
    ///   rows, retiring the hint) would leave it there (backup sim seed
    ///   600396; M14's lock sim seed 194287: `f3` kept the hint's inode,
    ///   every other replica had the renamed one). The mirror of
    ///   `install_shadow`'s streamed rule;
    /// - live speculation here touches the entry's keys: the store cannot
    ///   order it against the hint, and a hint read *before* it (replies
    ///   overtake each other) would put the old entry back over it
    ///   (backup-crash-slow seed 601075: the `Exists` reply to `create
    ///   f0` arrived after the shadow of the later `rename f0 f1`, and the
    ///   hint left `f0` and `f1` on one inode);
    /// - the entry names an inode this replica holds under another name
    ///   (the holder linked or renamed it there; this replica has not
    ///   applied that yet). Replay's create is idempotent only for the
    ///   same name (`replay::insert_node`); here it would write a fresh
    ///   inode record over the held one — size 0, no manifest, `nlink`
    ///   1 — and the hint's coverage would let every read of either
    ///   name see it at once, until the segment carrying the link
    ///   arrived (plan 45 review).
    ///
    /// The caller then treats the refusal as uncovered (raises
    /// `observed`), which is always correct.
    pub fn install_hint_from(
        &self,
        rid: Option<Rid>,
        records: &[LogRecord],
        at: Position,
        epoch: u64,
        gen: u64,
    ) -> Result<bool, MetaError> {
        self.install_hint_committed(rid, records, at, epoch, gen, false)
    }

    /// [`Self::install_hint_from`] for a refusal the holder answered as
    /// committed (plan 43 C1-4): not stranded by a takeover of its epoch.
    pub fn install_hint_committed(
        &self,
        rid: Option<Rid>,
        records: &[LogRecord],
        at: Position,
        epoch: u64,
        gen: u64,
        committed: bool,
    ) -> Result<bool, MetaError> {
        let tx = self.db.write_tx();
        if let Some(rid) = rid {
            if tx.get(&self.completed, rid.to_key())?.is_some()
                || streamed_entry_refusing(&tx, self, rid)?.is_some()
            {
                return Ok(false);
            }
        }
        if live_speculation_touches(&tx, self, &TouchSet::from_records(records.iter()))? {
            return Ok(false);
        }
        if hint_names_a_held_inode(&tx, self, records)? {
            return Ok(false);
        }
        self.install_speculative_tx(
            tx,
            UsageTracker::staging(),
            SpecKind::Hint {
                at,
                epoch,
                gen,
                gated: committed,
            },
            records,
        )?;
        Ok(true)
    }

    /// The highest journal seq of `epoch` installed here as a `Streamed`
    /// entry still outstanding (`None`: nothing of that epoch): what a
    /// stream-ahead need not deliver again. A scan of the live set,
    /// which is small; one counter read when it is empty.
    pub fn streamed_tip(&self, epoch: u64) -> Result<Option<u64>, MetaError> {
        let r = self.db.read_tx();
        streamed_tip_tx(&r, self, epoch)
    }

    /// Plan 30 §M9: install one of the holder's backup-acked journal
    /// transactions (rows `first..=last` of its tenure at `epoch`, streamed
    /// by node `source`) ahead of the log, as a `Streamed` entry (see
    /// [`SpecKind::Streamed`]). Installed already (see the first check):
    /// nothing happens.
    pub fn install_streamed(
        &self,
        epoch: u64,
        source: u64,
        first: u64,
        last: u64,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        self.install_streamed_committed(epoch, source, first, last, records, false, (0, 0))
    }

    /// [`Self::install_streamed`] for a transaction the holder streamed as
    /// committed (plan 43 C1-4, `StreamAhead.committed`): the entry is not
    /// stranded by a takeover of its epoch, and retires when a segment
    /// carries the row or its re-shipped `TailOf`. `origin` is the
    /// transaction's delegation origin `(gen, idx)` (`(0, 0)` for none): when
    /// it names a row this node holds as its own, the stream's delivery of
    /// that row is recorded, in this transaction (L1:
    /// [`local::note_deleg_streamed_tx`]).
    #[allow(clippy::too_many_arguments)]
    pub fn install_streamed_committed(
        &self,
        epoch: u64,
        source: u64,
        first: u64,
        last: u64,
        records: &[LogRecord],
        committed: bool,
        origin: (u64, u64),
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        // L1: the root's append of this node's own delegate row (the
        // stream delivers it, the early return below installs nothing):
        // from here on, the row is earlier than what the stream delivers.
        let marked = local::note_deleg_streamed_tx(&mut tx, self, epoch, origin)?;
        // Fix "capture under an epoch hold": already installed here —
        // the holder streams its journal from the start again for a
        // (re)subscriber, and a subscriber that restarted lost the
        // in-memory cursor that skipped what it holds while its replica
        // kept the entries (flex-backup seed 1230: a member back from a
        // crash re-applied the epoch journal it already held on top of
        // itself — a rename of a name a later create had reused). The
        // stream is contiguous and in order, so a transaction at or
        // below the highest installed one of this epoch is one this
        // replica has (or a hole it stepped over).
        if streamed_tip_tx(&tx, self, epoch)?.is_some_and(|tip| first <= tip) {
            if marked {
                tx.commit()?;
                self.flush_held_releases();
            }
            return Ok(());
        }
        // The mirror of `install_shadow`'s rule: this node's own op,
        // whose reply installed a shadow first, is not applied a second
        // time from the stream. The shadow *becomes* the streamed entry
        // (its effect is in place already): the segment then retires it
        // by skipping its rows like any streamed transaction's — which
        // is what keeps later streamed rows on top of it right (seed
        // 1402: a shadow retired the ordinary way had the segment's
        // unlink re-applied over a later streamed create) — and a
        // takeover strands it as this node's own op.
        // Defect B: a transaction the holder streamed as committed (its
        // group holds it) that says it rolled others back (`repair
        // drop-held`) retracts what this replica was streamed of them, in
        // this very transaction. An uncommitted one retracts nothing: a
        // hold's or a lone tenure's stream is not durable anywhere, and a
        // holder deposed or dead before the drop's transaction commits
        // leaves the rows the member's copy alone may carry. The segment
        // that carries the record (the log) retracts then.
        let staged = UsageTracker::staging();
        if committed {
            let rolled = super::member::rolled_back_idents(records);
            if !rolled.is_empty() {
                Self::retract_rolled_back_tx(&mut tx, self, &staged, &rolled)?;
            }
        }
        let mut adopt: Option<(u64, Rid, MutateOp, LockTag)> = None;
        for rec in records {
            if let LogRecord::Completed { rid } = rec {
                // Plan 30 §M11 phase 2b: this node's own delegate
                // transaction, executed (and completed) here, comes back
                // on the root's pre-S3 stream; applying it again would
                // replay its effect over what this delegate did since
                // (backup-crash seed 68018: a streamed copy of an unlink
                // removed the file the delegate had created after it).
                // The segment retires it by origin.
                if tx.get(&self.completed, rid.to_key())?.is_some() {
                    if marked {
                        tx.commit()?;
                        self.flush_held_releases();
                    }
                    return Ok(());
                }
                if let Some((seq, _, op, tag)) = shadow_row_for(&tx, self, *rid)? {
                    adopt = Some((seq, *rid, op, tag));
                    break;
                }
            }
        }
        // Plan 45 §12 O1, the stream's side (`delegated-backup+links+
        // stalebase` seed 138): a shadow is adopted for the rid it names,
        // but it is the transaction's effect only if its records are the
        // transaction's. A replay of a stranded op answered by the
        // holder's dedup (`Accepted` with no records: the rid was done
        // there, at a position this replica lacks) installed a shadow that
        // applied nothing; adopted in place, the stream's create was never
        // applied here, the rename streamed after it found no entry, and
        // the segment skipped both as confirmed rows. A shadow whose
        // records are not the transaction's effect is rolled back and the
        // transaction's records applied in its place (the rewind below,
        // from the shadow on), still as this node's own op.
        let mut shadow_lacks_effect = None;
        if let Some((seq, ..)) = &adopt {
            if let Some(v) = tx.get(&self.spec, seq_key(*seq))? {
                let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                if !same_effect(&row.records, records) {
                    shadow_lacks_effect = Some(*seq);
                }
            }
        }
        // The stream is contiguous and in the holder's order, so every
        // shadow still outstanding here — a reply the stream has not
        // reached yet — belongs *after* this transaction, and a
        // shadow it confirms belongs *here*, after everything streamed
        // or tailed before it. A reply can overtake the stream (they
        // travel apart, and one backup acknowledgement releases both):
        // then this transaction, or one tailed or streamed before it,
        // sits on top of a shadow it precedes. So can a segment when the
        // reply's `base` fell short (a delegate's did, see
        // `constellation_authority`'s `delegate_execute_now`). Installed
        // at the end, or adopted in place, that order is permanent: the
        // segment skips streamed rows (chaos-soak-4 seed 42, `wf293`:
        // node 0 installed its rebased whole-file write as a shadow, node
        // 3's earlier write then landed on top of it, the stream's copy
        // of node 0's own write adopted the shadow without re-applying
        // it, and node 0 served node 3's content while the log and every
        // other replica ended on node 0's). When that order matters (the
        // keys overlap), roll back to the oldest outstanding shadow and
        // redo in the log's order.
        let out_of_place =
            streamed_out_of_place(&tx, self, epoch, adopt.as_ref().map(|a| a.0), records)?;
        let cutoff = match (out_of_place, shadow_lacks_effect) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if let Some(cutoff) = cutoff {
            insert_streamed_tx(
                &mut tx,
                self,
                &staged,
                cutoff,
                StreamedTx {
                    epoch,
                    source,
                    first,
                    last,
                    records,
                    adopt,
                    gated: committed,
                },
            )?;
            let (bytes, files) = staged.raw_delta();
            adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
            tx.commit()?;
            self.flush_held_releases();
            staged.drain_into(self.usage_tracker());
            return Ok(());
        }
        if let Some((seq, rid, op, tag)) = adopt {
            if let Some(v) = tx.get(&self.spec, seq_key(seq))? {
                let mut row: SpecRow = crate::record::from_bytes_exact(&v)?;
                let gated = committed || matches!(row.kind, SpecKind::Shadow { gated: true, .. });
                row.kind = SpecKind::Streamed {
                    epoch,
                    first,
                    last,
                    source,
                    gated,
                    own: Some((rid, op, tag)),
                };
                put_row(&mut tx, &self.spec, seq, &row)?;
                tx.insert(
                    &self.spec_live,
                    seq_key(seq),
                    postcard::to_allocvec(&LiveEntry::Streamed {
                        epoch,
                        ident: super::member::tail_identity(records).unwrap_or((epoch, first)),
                        last,
                        gated,
                    })?,
                );
            }
            let (bytes, files) = staged.raw_delta();
            adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
            tx.commit()?;
            self.report_rolled_back();
            staged.drain_into(self.usage_tracker());
            return Ok(());
        }
        self.install_speculative_tx(
            tx,
            staged,
            SpecKind::Streamed {
                epoch,
                first,
                last,
                source,
                gated: committed,
                own: None,
            },
            records,
        )
    }

    /// Plan 46 M0.2 follow-up (defect B): the holder's `repair drop-held`
    /// rolled back the journal transactions `idents` names
    /// (`LogRecord::RolledBack`). What this replica holds of them is
    /// retracted, in the caller's transaction: its live `Streamed` entry of
    /// each identity — committed (`gated`) or not, `own` or not — is rolled
    /// back from its before-images, and not queued for replay (the holder's
    /// queue owns the replay or the refusal, and a copy here would be the
    /// second one); its member-copy row and its backup-tail transaction
    /// become holes, so that no successor re-ships a dropped row. An
    /// identity nobody holds is a no-op.
    fn retract_rolled_back_tx(
        tx: &mut SingleWriterWriteTx,
        meta: &Meta,
        staged: &UsageTracker,
        idents: &std::collections::BTreeSet<(u64, u64)>,
    ) -> Result<Stranded, MetaError> {
        let stranded = strand_tx(
            tx,
            meta,
            staged,
            |entry| matches!(entry, LiveEntry::Streamed { ident, .. } if idents.contains(ident)),
            false,
            false,
            false,
            |_| true,
        )?;
        super::member::hole_rolled_back_tx(tx, meta, idents)?;
        super::backup::hole_tail_rolled_back_tx(tx, meta, idents)?;
        Ok(stranded)
    }

    /// `staged` may carry usage the caller's transaction already moved
    /// (a retraction); it is persisted with this install's.
    fn install_speculative_tx(
        &self,
        mut tx: SingleWriterWriteTx,
        staged: UsageTracker,
        kind: SpecKind,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        let capture = Capture::new();
        let (_, applied) = {
            let cx = ApplyCx {
                dirty: self.dirty_capturing(&capture),
                durable: false,
            };
            apply_batch_tx(
                &mut tx,
                self,
                cx,
                records,
                &TouchSet::default(),
                &staged,
                true,
            )?
        };
        let usage = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, usage.0, usage.1)?;
        record_tx(&mut tx, self, kind, applied, capture.into_before(), usage)?;
        tx.commit()?;
        self.flush_held_releases();
        staged.drain_into(self.usage_tracker());
        Ok(())
    }

    /// Apply one tailed, non-fenced log segment at `seq` (from a holder at
    /// `epoch`), in one transaction with every plan 30 §M3 rule around
    /// it:
    ///
    /// 1. strand every outstanding entry below `epoch` (rollback, redo,
    ///    shadows and this node's own unshipped transactions queued for
    ///    replay) — except a shadow this very segment completes, which
    ///    simply retires below;
    /// 2. apply the records: *before* this node's own outstanding `Local`
    ///    transactions when there are any (they can only ship after this
    ///    segment), otherwise at the end, captured as a `Foreign` row if
    ///    anything is still outstanding. Records colliding with `pending`
    ///    (this node's *uncaptured* unshipped journal —
    ///    [`Meta::pending_touches`]) are skipped;
    /// 3. retire the shadows it completes and the hints its position
    ///    passes, and compact;
    /// 4. advance `applied_seq` to `seq`.
    pub fn apply_segment(
        &self,
        seq: u64,
        epoch: u64,
        records: &[LogRecord],
        pending: &TouchSet,
    ) -> Result<SegmentApplied, MetaError> {
        self.apply_segment_rows(seq, epoch, 0, &[], &[], records, pending)
    }

    /// [`Self::apply_segment`] with the segment's journal bookkeeping
    /// (plan 30 §M9): the shipping tenure's `through` and the journal
    /// seqs of its rows, which retire the `Streamed` entries they
    /// confirm (step 3).
    /// Plan 30 §M11: `origins` are the delegation origins of the rows
    /// (parallel to `rows`; `(0, 0)` for the shipper's own). A row of a
    /// generation this node executed as the delegate is its own
    /// transaction coming back through the log: its records are skipped
    /// (applied here already) and the transaction retires like a shipped
    /// one. A `DelegateNodeEnded` record strands what is left of that generation
    /// here (the delegate's unappended rows, rolled back and queued for
    /// replay by rid).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_segment_rows(
        &self,
        seq: u64,
        epoch: u64,
        through: u64,
        rows: &[u64],
        origins: &[(u64, u64)],
        records: &[LogRecord],
        pending: &TouchSet,
    ) -> Result<SegmentApplied, MetaError> {
        // Plan 30 §M11: the generations this segment recalls, from all of
        // its records — a `DelegateNodeEnded` the pre-S3 stream installed here first
        // is among the rows skipped below, and still ends its generation
        // (long-delegated-backup seed 78172: a requester's shadow of the
        // recalled generation was never stranded, and stayed forever).
        let recalled: HashSet<u64> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::DelegateNodeEnded { gen, .. } => Some(*gen),
                _ => None,
            })
            .collect();
        // The same records' cuts, all of them also: kept below, in this
        // transaction.
        let ends: Vec<(u64, u64)> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::DelegateNodeEnded { gen, through, .. } => Some((*gen, *through)),
                _ => None,
            })
            .collect();
        let completes: HashSet<Rid> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::Completed { rid } => Some(*rid),
                _ => None,
            })
            .collect();
        // Fix "capture under an epoch hold": the log refuses a rid this
        // node holds a shadow of. The holder accepted the op, then let
        // it go — `repair drop-held` (poisoned or `--remote`) turned it
        // into a refused replay whose conflict copy the holder makes —
        // and journaled the outcome. The shadow's effect never reaches
        // the log: rolled back here, and its replay dropped rather than
        // queued (the outcome is final, the copy exists; a replay would
        // only be answered "refused" and make a second copy).
        let refused: HashSet<Rid> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::Refused { rid, .. } => Some(*rid),
                _ => None,
            })
            .collect();
        // The journal position this segment brings the replica to,
        // recorded at the end of the transaction; the hint retirement
        // below already counts it.
        let seg_pos = JournalPos {
            epoch,
            jseq: through,
        };
        // Plan 43 C1-4: the identities of re-shipped transactions this
        // segment carries (`TailOf`), and the groups it says the operator
        // abandoned.
        // Defect B: the transactions the holder's `repair drop-held` says it
        // rolled back (`RolledBack`).
        let rolled = super::member::rolled_back_idents(records);
        let tail_ids: HashSet<(u64, u64)> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::TailOf { epoch, jseq } => Some((*epoch, *jseq)),
                _ => None,
            })
            .collect();
        let abandoned: Vec<u64> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::GroupAbandoned { epoch, gen: 0, .. } => Some(*epoch),
                _ => None,
            })
            .collect();
        let prev_applied = crate::store::applied_pos_at(&self.db.read_tx(), &self.local);
        let first_of_epoch = prev_applied.is_none_or(|pos| pos.epoch < epoch);
        let mut tx = self.db.write_tx();
        // Seed 2560: this node's appends, as a root of an earlier tenure, of
        // delegates' rows whose outcomes this segment carries under another
        // identity (`superseded_root_appends_tx`): stranded below, quietly.
        let superseded_appends =
            superseded_root_appends_tx(&tx, self, epoch, origins, &completes, &refused)?;
        let staged = UsageTracker::staging();
        // ADR-43 (orphan grants): what the table lists before an operator's
        // abandon, or a takeover that carries no tail, strands the
        // speculation no successor re-ships (read only then; see
        // `orphaned_tx`).
        // Also the generations of the live shadows and hints of delegate
        // replies (`gen != 0`): a requester that never listed the grant
        // holds them, and the strand below may take the entry (queuing its
        // rid for replay, held while the generation lives) without ending
        // the generation.
        // A no-tail record proves nothing on a later marker of an epoch
        // (a root that re-adopts its own lease after a restart keeps its
        // epoch and ships a second marker, while its journal may still hold
        // the tail its first marker announced): only the first segment of
        // the epoch, the claim's own marker, carries the proof.
        let mut proof = OrphanProof::of(records);
        if !first_of_epoch {
            proof.no_tail = None;
        }
        let listed_before: std::collections::BTreeSet<u64> = if proof.is_empty() {
            Default::default()
        } else {
            self.named_generations_tx(&tx)?
        };
        let strands = |entry: &LiveEntry| {
            (entry.stranded_by(epoch)
                && !matches!(entry, LiveEntry::Shadow { rid, .. } if completes.contains(rid)))
                || abandoned.iter().any(|a| entry.stranded_by_abandon(*a))
                || matches!(entry, LiveEntry::Shadow { rid, .. } if refused.contains(rid))
                || matches!(entry, LiveEntry::Streamed { ident, .. } if rolled.contains(ident))
                || matches!(entry, LiveEntry::Local { first, .. } if superseded_appends.contains(first))
                // A streamed transaction of this tenure the segment ships
                // *past* (`through` at or beyond its last row) without
                // naming its rows is gone from the holder's journal: it
                // was dropped there (`repair drop-held`, the poisoned or
                // the `--remote` case), never shipped, and never will be —
                // `journal_through_after` stops below any row still held
                // back or deferred. Its effects go with it: rolled back
                // here like any stranded speculation (a later streamed
                // transaction on the same keys is rolled back and redone
                // by the rewind; on the holder it was requeued by rid and
                // comes back through the log). Retiring it as confirmed
                // (M9's first rule) kept the dropped write in the member's
                // replica for good.
                // (Plan 43 C1-4: not a committed one. A row the holder's group
                // held and streamed as committed is re-shipped by whoever
                // continues the tenure — a self-sealed holder re-claiming its
                // own epoch numbers its journal on from the old one, so a
                // later segment's `through` passes the committed rows without
                // naming them, and they are in its re-ship.)
                || matches!(entry, LiveEntry::Streamed { epoch: e, last, gated: false, .. }
                    if *e == epoch && through >= *last && !rows.contains(last))
        };
        // Only a holder's own captured transactions (`gen` 0) among the
        // `Local` rows can strand here (`LiveEntry::stranded_by`); with
        // none of them outstanding, the other entries decide, and a
        // delegate's own stream rows are read only if the segment needs
        // them (`DelegRows`, below).
        let holder_locals = counter_get(&tx, &self.local, KV_LOCAL_SPEC_COUNT)?
            > counter_get(&tx, &self.local, KV_DELEG_SPEC_COUNT)?;
        let stranded = if holder_locals
            || !superseded_appends.is_empty()
            || live_non_local(&tx, self)?
                .iter()
                .any(|(_, entry)| strands(entry))
        {
            // (What the holder rolled back is not queued for replay here:
            // its queue owns the replay or the refusal.)
            strand_tx(
                &mut tx,
                self,
                &staged,
                strands,
                true,
                false,
                true,
                |entry| {
                    matches!(entry, LiveEntry::Streamed { ident, .. } if rolled.contains(ident))
                        || matches!(entry, LiveEntry::Local { first, .. } if superseded_appends.contains(first))
                },
            )?
        } else {
            Stranded::default()
        };
        if !refused.is_empty() && stranded.shadows > 0 {
            drop_queued_of_rids_tx(&mut tx, self, &refused)?;
        }
        // Plan 30 §M11: this node's own delegate transactions the segment
        // carries — their segment rows are skipped below, and the
        // transactions retire after the apply.
        let own_txs = self.journal_seqs_of_origins(&tx, origins)?;
        for (gen, idx) in origins {
            local::bump_log_idx_tx(&mut tx, &self.local, *gen, *idx)?;
        }
        if origins.iter().any(|o| o.0 != 0) {
            tracing::trace!(
                seq,
                ?rows,
                ?origins,
                ?own_txs,
                "segment carries delegate origins"
            );
        }
        // Segment row -> the own transaction's `spec_seq` (its `Local`
        // speculation row, if captured).
        let own_rows: HashMap<u64, Option<u64>> = if own_txs.is_empty() {
            HashMap::new()
        } else {
            rows.iter()
                .zip(origins.iter())
                .filter(|(_, o)| o.0 != 0)
                .filter_map(|(r, o)| {
                    own_txs
                        .iter()
                        .find(|(first, last, _)| {
                            local::get_journal_tx_head(&tx, self, *first)
                                .ok()
                                .flatten()
                                .is_some_and(|h| (h.gen, h.idx) == *o && h.last == *last)
                        })
                        .map(|(_, _, spec_seq)| (*r, *spec_seq))
                })
                .collect()
        };
        // Every outstanding entry — or, with no holder's own captured
        // transaction outstanding, every one but the delegation origins'
        // `Local` rows, read below only where the segment needs them
        // (`live_full`). Nothing else here looks at a `Local` entry.
        let mut live_full = holder_locals;
        let mut live: HashMap<u64, LiveEntry> = if live_full {
            read_live(&tx, self)?.into_iter().collect()
        } else {
            live_non_local(&tx, self)?.into_iter().collect()
        };
        let any_local = counter_get(&tx, &self.local, KV_LOCAL_SPEC_COUNT)? > 0;
        // The hints this segment ends: the log now holds everything their
        // refusal was read at, or it changes their entry after that (the
        // later change ships only with or after the entry's own row: a
        // row touching a held-back row's keys is held too, plan 30 §M4).
        // Either way the log speaks for the entry from here on; a hint
        // kept past a later change would be redone over it by a rewind
        // (flex-zero seed 1000: `f0` put back over the rename that
        // replaced it, while rows held for their chunks kept `through`
        // below the refusal's position).
        let mut superseded: HashSet<u64> = HashSet::new();
        let mut hints_ending: HashSet<u64> = HashSet::new();
        for (spec_seq, entry) in &live {
            let LiveEntry::Hint { at, .. } = entry else {
                continue;
            };
            let later = touched_after(at, seq, epoch, rows, origins, records);
            let changed = !later.dentries.is_empty() || !later.inos.is_empty();
            if changed {
                if let Some(v) = tx.get(&self.spec, seq_key(*spec_seq))? {
                    let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                    if supersedes(&later, &row.records) {
                        superseded.insert(*spec_seq);
                        hints_ending.insert(*spec_seq);
                        continue;
                    }
                }
            }
            if hint_reached(&tx, self, at, seq, Some(seg_pos))? {
                hints_ending.insert(*spec_seq);
            }
        }
        // Plan 30 §M9: the streamed transactions this segment carries are
        // applied already. Re-applying their records would not converge
        // (a `Create` re-runs once a later speculation renamed the name
        // away: two dentries on one inode), so their rows are skipped —
        // the outcome rows excepted: `Completed`, `Refused` and
        // `InboxAck` touch no namespace key, and the streamed install
        // (not durable) recorded none of them — the `completed` row and
        // the inbox watermark only exist once the segment writes them.
        // (Long-backup seed 50412: a skipped `Refused` left the next
        // holder unaware of the refusal, and its inbox drain executed
        // the refused op.) The entry becomes a durable `Foreign` row
        // below: a later redo still carries its effect.
        let mut confirmed: Vec<(u64, u64, u64)> = Vec::new();
        for (spec_seq, entry) in &live {
            let LiveEntry::Streamed { epoch: e, last, .. } = entry else {
                continue;
            };
            if *e != epoch || !rows.contains(last) {
                continue;
            }
            if let Some(v) = tx.get(&self.spec, seq_key(*spec_seq))? {
                if let SpecKind::Streamed { first, .. } =
                    crate::record::from_bytes_exact::<SpecRow>(&v)?.kind
                {
                    confirmed.push((*spec_seq, first, *last));
                }
            }
        }
        // Plan 43 C3: a committed streamed transaction of an earlier tenure
        // that this segment completes (or refuses) by its rid under no
        // identity it names:
        // a root's append of a delegate's row that the next root appended
        // again from the delegate's re-stream (a successor's tail without
        // it: the tail a claimant fetched from a member copy). It is the
        // segment's record now, as one its `TailOf` names: rolled back and
        // applied in its place, and retired (left, it was redone over the
        // segment's own copy and waited for a `TailOf` for ever).
        let mut completed_streams: HashSet<u64> = HashSet::new();
        if !completes.is_empty() || !refused.is_empty() {
            for (spec_seq, entry) in &live {
                let LiveEntry::Streamed {
                    epoch: e,
                    ident,
                    gated: true,
                    ..
                } = entry
                else {
                    continue;
                };
                if *e >= epoch || tail_ids.contains(ident) {
                    continue;
                }
                if let Some(v) = tx.get(&self.spec, seq_key(*spec_seq))? {
                    let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                    // (A refusal's row too: its outcome is the record.)
                    if row.records.iter().any(|r| match r {
                        LogRecord::Completed { rid } => completes.contains(rid),
                        LogRecord::Refused { rid, .. } => refused.contains(rid),
                        _ => false,
                    }) {
                        completed_streams.insert(*spec_seq);
                    }
                }
            }
        }
        // Plan 30 §M11: with a `Local` row outstanding the segment is
        // inserted by a rewind (below), which rolls back every row from
        // the oldest one on. A streamed or own-delegate transaction that
        // the rewind rolls back is *not* applied any more: its records
        // apply from the segment, in log order (epoch seed 64010: redone
        // after the segment instead, a delegate's `Create` came back
        // after the rename the segment carried and the name diverged).
        //
        // A delegate's own stream transactions (`Local` rows of a
        // generation) are the exception: the segment goes under the
        // oldest one it conflicts with, not under the oldest one. Their
        // keys are the subtree's, which the log changes only through
        // this delegate's own rows (skipped below, or rolled back with
        // their row) or after a `DelegateNodeEnded` (which strands them); a segment
        // that touches none of them commutes with them, so it applies on
        // top, captured as a `Foreign` row — the state every replica
        // reaches when they ship after it (B-2's rule below, for the
        // delegate's rows). Rewinding them all for every segment made
        // each tailed segment cost the delegate's whole unappended stream
        // (`stress-ng-fs-nodes`: 110–240 transactions, 100–400 ms per
        // segment against ~150 segments a second; its core fell minutes
        // behind the holder's log, and its stream acknowledgements and
        // renewal answers with it). A holder's own unshipped rows keep
        // the rewind (gen 0: they may conflict with a late segment of
        // the previous tenure anywhere).
        let first_local = {
            let mut skipped: HashSet<u64> = confirmed
                .iter()
                .flat_map(|(_, first, last)| *first..=*last)
                .collect();
            skipped.extend(own_rows.keys());
            let mut jseqs = rows.iter();
            let touch = TouchSet::from_records(records.iter().filter(|rec| {
                if matches!(rec, LogRecord::Atime { .. }) {
                    return true;
                }
                let jseq = jseqs.next().copied();
                !jseq.is_some_and(|j| skipped.contains(&j))
            }));
            // Without a holder's own row, the delegation origins' rows are
            // all there is to look at: read only if one may touch the
            // segment's keys.
            if !live_full && any_local && deleg_rows_may_overlap(&tx, self, &touch)? {
                live.extend(read_live(&tx, self)?);
                live_full = true;
            }
            let holder_first = live
                .iter()
                .filter(|(_, entry)| matches!(entry, LiveEntry::Local { gen: 0, .. }))
                .map(|(seq, _)| *seq)
                .min();
            let mut delegate_rows: Vec<u64> = live
                .iter()
                .filter(|(seq, entry)| {
                    matches!(entry, LiveEntry::Local { gen, .. } if *gen != 0)
                        && holder_first.is_none_or(|h| **seq < h)
                })
                .map(|(seq, _)| *seq)
                .collect();
            delegate_rows.sort_unstable();
            let mut first = holder_first;
            for spec_seq in delegate_rows {
                let Some(v) = tx.get(&self.spec, seq_key(spec_seq))? else {
                    continue;
                };
                let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                if row_touches(&tx, self, &row)?.overlaps(&touch) {
                    first = Some(spec_seq);
                    break;
                }
            }
            first
        };
        // EC2 campaign 4 B-2: the same for outstanding speculation of any
        // other kind that this segment's records overlap (or that it
        // retires): the segment is earlier in the log than it, so its
        // records must apply *under* it, not on top. Applied on top, a
        // record re-runs against state that already holds its own effect
        // and later ones, and replay is not idempotent across that: git's
        // loose object — `create tmp`, `link tmp obj`, `unlink tmp` — shadowed
        // here before the segment with the `create` arrived, re-created the
        // inode with `nlink 1` (the tmp name was gone), the `link` found
        // `obj` already there and added no link, and the `unlink tmp`
        // then dropped the inode under `obj`. A dangling `obj` on this
        // replica, published by it as the next holder: every fresh node
        // lost the object. A shadow the segment completes and a streamed
        // row it confirms are its own records; the rest are later ops.
        let first_overlap = {
            let incoming = incoming_touches(&tx, self, records)?;
            let mut first: Option<u64> = None;
            for (spec_seq, entry) in &live {
                let candidate = match entry {
                    LiveEntry::Local { .. } => false,
                    LiveEntry::Streamed { .. }
                        if confirmed.iter().any(|(s, _, _)| s == spec_seq) =>
                    {
                        false
                    }
                    LiveEntry::Shadow { rid, .. } if completes.contains(rid) => true,
                    // Plan 43 C1-4: a committed streamed transaction the
                    // successor re-ships (its `TailOf` names it) is its
                    // segment's record now: rolled back and applied in its
                    // place, as a shadow this segment completes.
                    LiveEntry::Streamed { ident, .. }
                        if tail_ids.contains(ident) || completed_streams.contains(spec_seq) =>
                    {
                        true
                    }
                    _ => match tx.get(&self.spec, seq_key(*spec_seq))? {
                        Some(v) => {
                            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                            row_touches(&tx, self, &row)?.overlaps(&incoming)
                        }
                        None => false,
                    },
                };
                if candidate && first.is_none_or(|f| *spec_seq < f) {
                    first = Some(*spec_seq);
                }
            }
            first
        };
        let first_rewind = match (first_local, first_overlap) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let rolled_back = |spec_seq: u64| first_rewind.is_some_and(|cutoff| spec_seq >= cutoff);
        let mut skipped_rows: HashSet<u64> = confirmed
            .iter()
            .filter(|(spec_seq, _, _)| !rolled_back(*spec_seq))
            .flat_map(|(_, first, last)| *first..=*last)
            .collect();
        let own_rolled_back: HashSet<u64> = own_rows
            .iter()
            .filter(|(_, spec_seq)| spec_seq.is_some_and(&rolled_back))
            .map(|(r, _)| *r)
            .collect();
        skipped_rows.extend(own_rows.keys().filter(|r| !own_rolled_back.contains(r)));
        let filtered: Vec<LogRecord>;
        let records: &[LogRecord] = if skipped_rows.is_empty() {
            records
        } else {
            let mut jseqs = rows.iter();
            filtered = records
                .iter()
                .filter(|rec| {
                    if matches!(rec, LogRecord::Atime { .. }) {
                        return true;
                    }
                    let jseq = jseqs.next().copied();
                    matches!(
                        rec,
                        LogRecord::Completed { .. }
                            | LogRecord::Refused { .. }
                            | LogRecord::InboxAck { .. }
                    ) || !jseq.is_some_and(|j| skipped_rows.contains(&j))
                })
                .cloned()
                .collect();
            &filtered
        };
        let mut inserted_before_local = false;
        let skipped = if let Some(cutoff) = first_rewind {
            inserted_before_local = first_local.is_some();
            // Plan 30 §M11: a delegate holds `Local` rows (its unretired
            // stream transactions) older than its shadows and hints, so
            // the insert-and-redo path is its ordinary one. What this
            // segment retires — a shadow whose completion it carries, a
            // hint whose floor it reaches, a streamed row it confirms —
            // is retired speculation whose effect the segment carries:
            // redone after it, a hint would resurrect the entry the
            // segment's later records moved (epoch seed 64010).
            let own_spec: HashSet<u64> = own_txs.iter().filter_map(|(_, _, s)| *s).collect();
            if !live_full {
                live.extend(read_live(&tx, self)?);
            }
            let live_after: HashMap<u64, LiveEntry> = live
                .iter()
                .filter(|(spec_seq, entry)| match entry {
                    LiveEntry::Shadow { rid, .. } => !completes.contains(rid),
                    // Not "the segment reaches its floor": the next
                    // segment need not carry the entry, and one that does
                    // not must find it redone over itself.
                    LiveEntry::Hint { .. } => !hints_ending.contains(spec_seq),
                    LiveEntry::Streamed { ident, .. } => {
                        !confirmed.iter().any(|(s, _, _)| s == *spec_seq)
                            && !tail_ids.contains(ident)
                            && !completed_streams.contains(*spec_seq)
                    }
                    LiveEntry::Local { .. } => !own_spec.contains(spec_seq),
                })
                .map(|(s, e)| (*s, *e))
                .collect();
            rewind_tx(
                &mut tx,
                self,
                &staged,
                &live_after,
                &HashSet::new(),
                cutoff,
                Strand::PLAIN,
                Some(Inserted {
                    segment_seq: seq,
                    records,
                    pending,
                }),
            )?
            .1
        } else if !live.is_empty() || any_local {
            let capture = Capture::new();
            let row_staged = UsageTracker::staging();
            let (skipped, applied) = {
                let cx = ApplyCx {
                    dirty: self.dirty_capturing(&capture),
                    durable: true,
                };
                apply_batch_tx(&mut tx, self, cx, records, pending, &row_staged, true)?
            };
            let usage = row_staged.raw_delta();
            staged.adjust(usage.0, usage.1);
            record_tx(
                &mut tx,
                self,
                SpecKind::Foreign { segment_seq: seq },
                applied,
                capture.into_before(),
                usage,
            )?;
            skipped
        } else {
            let cx = ApplyCx {
                dirty: self.dirty_for_ns(),
                durable: true,
            };
            apply_batch_tx(&mut tx, self, cx, records, pending, &staged, false)?.0
        };
        let mut retired = retire_tx(&mut tx, self, &completes, seq, Some(seg_pos), &superseded)?;
        // Plan 43 C1-4: the committed streamed transactions this segment
        // re-ships (`TailOf`) are in the log now; their entries retire.
        for (spec_seq, entry) in &live {
            let LiveEntry::Streamed { ident, .. } = entry else {
                continue;
            };
            if (tail_ids.contains(ident) || completed_streams.contains(spec_seq))
                && tx.get(&self.spec_live, seq_key(*spec_seq))?.is_some()
            {
                tx.remove(&self.spec_live, seq_key(*spec_seq));
                counter_add_tx(&mut tx, &self.local, KV_SPEC_LIVE_COUNT, -1)?;
                retired += 1;
            }
        }
        for (spec_seq, _, _) in &confirmed {
            if let Some(v) = tx.get(&self.spec, seq_key(*spec_seq))? {
                let mut row: SpecRow = crate::record::from_bytes_exact(&v)?;
                row.kind = SpecKind::Foreign { segment_seq: seq };
                put_row(&mut tx, &self.spec, *spec_seq, &row)?;
            }
            if tx.get(&self.spec_live, seq_key(*spec_seq))?.is_some() {
                tx.remove(&self.spec_live, seq_key(*spec_seq));
                counter_add_tx(&mut tx, &self.local, KV_SPEC_LIVE_COUNT, -1)?;
                retired += 1;
            }
        }
        // Plan 30 §M11: the own delegate transactions this segment
        // carries are in the log now: retire them (journal rows deleted,
        // their `Local` rows converted or dropped, the acked watermark
        // moved as an out-of-order ship would, M4's rule).
        let mut own_seqs: Vec<u64> = own_txs
            .iter()
            .flat_map(|(first, last, _)| *first..=*last)
            .collect();
        own_seqs.sort_unstable();
        if !own_seqs.is_empty() {
            self.ack_rows_tx(&mut tx, &own_seqs, seq)?;
            retired += own_txs.len();
        }
        // The cut of every generation this segment ends, kept for a restart
        // and for an install that waits for the end
        // (`Meta::deleg_gens_awaiting_end`), in this transaction: a record
        // the root's stream installed first is among the rows skipped above
        // and is not applied again, and a cut written after the commit is
        // lost to a crash in between (the restart then voided at the log's
        // index of the generation, or not at all, below the record's cut).
        // Only raised, as the durable apply of the record does. (From every
        // record of the segment: `records` here is what was not skipped.)
        for (gen, through) in &ends {
            local::bump_void_cut_tx(&mut tx, &self.local, *gen, *through)?;
        }
        // ADR-43: a generation whose grant the segment proves will never be
        // in the log (an abandon, or a takeover that carries no tail) is
        // ended here as the log's `DelegateNodeEnded` would end it, at what the log
        // holds of it (`orphaned_tx`). The cut loop
        // above must run first: a generation this segment ends (the
        // stream installed the end before the marker) then has its cut key,
        // which the rule's exclusions read, and is not ended again at cut 0.
        let orphaned = if proof.is_empty() {
            Vec::new()
        } else {
            self.orphaned_tx(&tx, &proof, &listed_before, &recalled)?
        };
        let mut orphan_cuts: Vec<(u64, u64)> = Vec::new();
        for gen in &orphaned {
            let cut = local::bump_void_cut_at_log_idx_tx(&mut tx, &self.local, *gen)?;
            orphan_cuts.push((*gen, cut));
        }
        let recalled: HashSet<u64> = recalled.iter().chain(orphaned.iter()).copied().collect();
        // Plan 30 §M11: a generation the log recalls strands whatever of
        // it this node still holds unappended (past the cut: everything
        // appended is before the `DelegateNodeEnded` record, hence retired above).
        let mut stranded = stranded;
        if !recalled.is_empty() {
            let more = strand_tx(
                &mut tx,
                self,
                &staged,
                |entry| entry.stranded_by_recall(&recalled),
                false,
                true,
                false,
                |_| false,
            )?;
            stranded.shadows += more.shadows;
            stranded.hints += more.hints;
            stranded.locals += more.locals;
            stranded.deleg_dropped += more.deleg_dropped;
            // L1: the log ended these generations, so no `Local` row of
            // them is left to order (`local::note_deleg_streamed_tx`). Here
            // as well as in `replay::apply_one`: a `DelegateNodeEnded` the stream
            // installed first is among the rows skipped above, and the
            // streamed install must not clear the mark itself.
            for gen in &recalled {
                local::clear_deleg_streamed_tx(&mut tx, &self.local, *gen);
            }
            // Phase 2b: stranding a delegate's own transaction drops its
            // rid's `completed` row (it never reached the log *as that
            // transaction*) — but this very segment may carry the rid's
            // completion or refusal (the root executed the requester's
            // retry): that outcome is the log's and stays, or a later
            // reply for the rid would install a shadow nothing retires
            // (delegated-partition seed 63000).
            let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
            for rec in records {
                let (rid, refused) = match rec {
                    LogRecord::Completed { rid } => (*rid, None),
                    LogRecord::Refused { rid, code } => (*rid, Some(*code)),
                    _ => continue,
                };
                if tx.get(&self.completed, rid.to_key())?.is_some() {
                    continue;
                }
                let row = match refused {
                    Some(code) => Meta::encode_refused_row(0, now_ms, code),
                    None => Meta::encode_completed_row(0, now_ms),
                };
                tx.insert(&self.completed, rid.to_key(), row);
            }
        }
        // Plan 43 C1-4: what the log delivered tells the member copies
        // (coverage, `TailFollowsThrough`, an abandon, a newer marker).
        super::member::note_log_tail_tx(&mut tx, self, epoch, records, prev_applied)?;
        super::member::hole_rolled_back_tx(&mut tx, self, &rolled)?;
        super::backup::hole_tail_rolled_back_tx(&mut tx, self, &rolled)?;
        let member = super::member::member_rules_tx(
            &mut tx,
            self,
            &super::member::Facts {
                epoch,
                through,
                rows,
                records,
                first_of_epoch,
                own: false,
            },
        )?;
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        kv_set_tx(&mut tx, &self.local, KV_APPLIED_SEQ, &seq.to_string());
        crate::store::note_applied_pos_tx(&mut tx, &self.local, seg_pos);
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        for (gen, cut) in orphan_cuts {
            tracing::info!(
                gen = %crate::delegation::GenLabel::of(gen),
                cut,
                "a takeover marker ended a delegation generation whose grant never reached the log"
            );
            self.session.void_stream(gen, cut);
            self.session.note_void_cut(gen, cut);
        }
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        if !own_seqs.is_empty() {
            if let Some(&upto) = own_seqs.iter().max() {
                let _ = self.prune_recent_shipped(upto);
            }
        }
        if member != Default::default() || self.has_member_copies() {
            let _ = self.refresh_member_copy_flag();
        }
        Ok(SegmentApplied {
            skipped,
            stranded,
            retired,
            inserted_before_local,
            member,
        })
    }

    /// Strand every outstanding entry accepted (or executed) below `epoch`.
    /// Three callers:
    /// - the takeover gate (plan 30 §M3a), with the epoch this node is
    ///   about to hold. The caller has tailed to head, so anything such an
    ///   entry's holder shipped has already retired it; what is left can
    ///   never reach the log any more;
    /// - a deposed holder (plan 30 §M3b, `cli::recovery::recover_deposed`),
    ///   with an epoch above its own: its unshipped transactions are
    ///   rolled back and queued for replay through the new holder;
    /// - the replay drain on a holder, for older-epoch leftovers.
    ///
    /// Queued ops are replayed by rid by the caller (`cli::recovery`).
    pub fn strand_below_epoch(&self, epoch: u64) -> Result<Stranded, MetaError> {
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = strand_tx(
            &mut tx,
            self,
            &staged,
            |entry| entry.stranded_by_takeover(epoch),
            true,
            false,
            true,
            |_| false,
        )?;
        if !stranded.any() {
            return Ok(stranded);
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        Ok(stranded)
    }

    /// Plan 46 M1.2 step 7 (c3-spec §6.1): a deposition's strand.
    /// [`Self::strand_below_epoch`]'s fates, and a delegate's row this node
    /// appended as the root (a `Local` row of a generation not in `own_gens`,
    /// journaled under a lease epoch below `floor`) is rolled back from its
    /// before-images, leaves the journal (and the generation's log index
    /// retracts) and is kept verbatim as a member copy of its generation
    /// (`backup_tail[DELEG_BACKUP_BASE + gen]`, key `idx`), in the same
    /// transaction. Never queued: the delegate holds the row until the log
    /// carries it and the next root's `DelegSeal` of a backup-less generation
    /// reads this copy. `own_gens` are the generations this node is the
    /// delegate of (their rows are its live stream: left alone).
    pub fn strand_deposed(&self, floor: u64, own_gens: &[u64]) -> Result<Stranded, MetaError> {
        let own: HashSet<u64> = own_gens.iter().copied().collect();
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = strand_tx_deleg(
            &mut tx,
            self,
            &staged,
            |entry| {
                entry.stranded_by_takeover(floor)
                    || matches!(
                        entry,
                        LiveEntry::Local { epoch, gen, .. }
                            if *gen != 0 && *epoch != 0 && *epoch < floor && !own.contains(gen)
                    )
            },
            true,
            false,
            true,
            |_| false,
            Some(&own),
        )?;
        if !stranded.any() {
            return Ok(stranded);
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        Ok(stranded)
    }

    /// The takeover gate's strand (plan 30 §M3a × §M9 × §M11): everything
    /// [`Self::strand_below_epoch`] strands, and this node's own
    /// transactions as the *delegate* of generations `own_gens` (a
    /// `Local` row of one of them; the fast path may have executed some
    /// after the lease was acquired, under its epoch). A delegate that
    /// takes the root over has those rows in its journal *before* the predecessor's backup
    /// tail it is about to re-apply, but the log orders them the other
    /// way: the tail holds the predecessor's appends of them (after the
    /// `DelegateNode` that granted them, and before whatever it appended
    /// later), and the rest were never appended. Shipped in journal
    /// order, they overtook the predecessor's tail (delegated-holder-cut
    /// seed 772: a create in `d1` shipped ahead of the tail's rename of
    /// the same name out of `d1`). Stranded here, the tail re-journals
    /// the appended ones in their place (completing their rids) and the
    /// queued replay re-executes the rest by rid after it, in order.
    ///
    /// Every own generation is marked in the same transaction
    /// ([`Meta::deleg_ended_at_gate`], plan 46 M1.2 step 6, c3-spec D7): it
    /// ends at this gate, and this node never executes in it again. The
    /// rows the predecessor never appended leave the journal here (the
    /// tail re-journals the appended ones, the gate re-appends the rest
    /// verbatim under their generation when it can, and what does not go
    /// back is replayed by rid as root rows), so the stream has no hole:
    /// its counter is set to what the log holds of it at the end of this
    /// transaction, and again after the gate's re-append
    /// ([`Meta::end_own_streams_at_log`]). The generation ends at that,
    /// by this node once its gate opens, or by whichever root inherits it
    /// if this node crashes or loses the lease first
    /// (`locks-unlinked-delegated-dbackup-random` seeds 2477 and 2982: the
    /// delegate-turned-root crashed inside its takeover gate, restarted as
    /// the generation's delegate and streamed index 6 to a root waiting
    /// for 3, forever). The mark is never cleared: generations are never
    /// reused (ADR-43: a number carries the epoch of the tenure that
    /// minted it, so no two tenures mint the same one).
    ///
    /// The mark is also the gate's fence. The strand, the backup tail
    /// and the replay are separate store transactions, and nothing of
    /// this node may execute on the replica between the strand and the
    /// end of the replay; the delegate's own fast path executes straight
    /// against the store, admitted by the engine's mirror of the core's
    /// delegation table, which lags the core by an event. A write that
    /// had been admitted before the strand and reached the writer lock
    /// after it landed first, under the stranded rows' names (`f7`: the
    /// replay found the name taken, the stranded rename moved the fresh
    /// inode). [`Meta::begin_local`] therefore refuses a delegate's own
    /// execution under a marked generation (`MetaError::NotAdmitted`),
    /// unwritten, at the write under the writer lock, and the engine
    /// sends the op through the core, whose gate parks it until the
    /// replay is done.
    pub fn strand_for_takeover(
        &self,
        epoch: u64,
        own_gens: &[u64],
        abandoned: Option<u64>,
    ) -> Result<Stranded, MetaError> {
        let proof = OrphanProof {
            upto: abandoned,
            no_tail: None,
        };
        self.strand_for_takeover_carrying(epoch, own_gens, abandoned, &HashSet::new(), &proof)
    }

    /// [`Self::strand_for_takeover`], with the rids of the delegate rows the
    /// tail the gate re-applies carries (`carried`): a committed shadow of
    /// a delegate's answer is taken out only if the tail carries its row
    /// ([`LiveEntry::superseded_by_takeover`]).
    ///
    /// ADR-43 (orphan grants): `proof` is what this claim's own marker proves
    /// ([`OrphanProof::of`] its records). The claimant never applies its own
    /// marker as a segment, so the rule `apply_segment_rows` runs on every
    /// other replica runs here, in the strand's transaction: a generation the
    /// log will never grant is ended at what the log holds of it (0), and this
    /// node's own rows of it as its delegate are rolled back and replayed by
    /// rid. Left in the journal, they shipped as this root's first rows,
    /// rows of a generation with no `DelegateNode` in the log, ahead of the
    /// stranded root rows they were executed on.
    pub fn strand_for_takeover_carrying(
        &self,
        epoch: u64,
        own_gens: &[u64],
        abandoned: Option<u64>,
        carried: &HashSet<Rid>,
        proof: &OrphanProof,
    ) -> Result<Stranded, MetaError> {
        Ok(self
            .reship_at_takeover(
                Some(StrandArgs {
                    epoch,
                    own_gens,
                    abandoned,
                    carried,
                    proof,
                }),
                None,
            )?
            .stranded)
    }

    /// Plan 46 M1.2 step 5 (c3-spec D3, D4): a takeover's re-ship, in ONE
    /// store transaction: the strand (`strand`: everything
    /// [`Self::strand_for_takeover_carrying`] does, which includes the
    /// successor's own entries the tail carries — C1-4's `superseded` — and
    /// the fence mark of every own generation) and then the predecessor's
    /// sealed tail (`tail`), verbatim: each transaction of its contiguous
    /// prefix journaled with the records as the tail holds them, in order
    /// (an `Atime`, a `Refused` and an `InboxAck` row included), under its
    /// identity (`TailOf`) and its delegation origin, then the tail's move
    /// into the member copy of its epoch and its clear. Both halves or
    /// neither commit.
    ///
    /// A record that does not apply is [`MetaError::TailDiverged`]: the
    /// tail extends the archive prefix this replica holds (the strand took
    /// out what it carries), so every record applies exactly; one that does
    /// not is a broken premise, reported, never journaled, and nothing of
    /// the strand or of the tail's earlier rows is visible. The caller
    /// keeps the takeover gate closed. The same tail fails the same way on
    /// a retry, so the retry is idempotent and the node does not serve as
    /// root until the premise is repaired (the operator's decision: stop
    /// this node so another claimant fetches the tail, or abandon the
    /// group). There is no recovery that drops the rows.
    ///
    /// V3 (c3-spec §8): no reader and no writer of the successor sees a
    /// state between the rollback of a committed entry and its
    /// re-application, and a crash between the strand and the tail (two
    /// transactions before) cannot leave the strand without the tail it
    /// made room for. The cost is the writer lock for the whole tail
    /// (measured: `reship_lock_hold_over_a_large_tail`).
    ///
    /// A transaction of the tail whose rid already has an outcome here is
    /// skipped (`tail_done`: the log carried it), a hole or a transaction
    /// past the sealed hold's contiguous prefix (`through`; never
    /// acknowledged, its client retries by rid) is not re-shipped.
    pub fn reship_at_takeover(
        &self,
        strand: Option<StrandArgs<'_>>,
        tail: Option<TailArgs>,
    ) -> Result<ReshipReport, MetaError> {
        let txs: Vec<crate::BackupTx> = match &tail {
            Some(t) => self.backup_tail(t.epoch)?,
            None => Vec::new(),
        };
        // Plan 30 §M12: a grant or a recall changes who may execute what;
        // the fast path checks that under the shared side. Taken before the
        // write transaction (the fast path's order: gate, then database).
        let _gate = txs
            .iter()
            .flat_map(|t| t.records.iter())
            .any(|r| {
                matches!(
                    r,
                    LogRecord::DelegateNode { .. }
                        | LogRecord::SubtreeReleased { .. }
                        | LogRecord::StreamReleased { .. }
                        | LogRecord::DelegateNodeEnded { .. }
                )
            })
            .then(|| self.deleg_gate.write().unwrap_or_else(|e| e.into_inner()));
        let mut tx = self.db.write_tx();
        let held_since = std::time::Instant::now();
        let staged = UsageTracker::staging();
        let mut report = ReshipReport::default();
        let mut part = StrandPart::default();
        if let Some(args) = strand {
            part = self.strand_part_tx(&mut tx, &staged, args)?;
            report.stranded = part.stranded;
        }
        let mut journaled: Vec<((u64, u64), local::JournaledTx)> = Vec::new();
        if let Some(t) = &tail {
            let _class = t.class.map(crate::store::journal::PendingClass::set);
            let mut moved: Vec<crate::BackupTx> = Vec::new();
            for bt in &txs {
                if bt.records.is_empty() {
                    // A hole: rows the holder had shipped (or rolled back)
                    // before it streamed past them; nothing to re-apply.
                    continue;
                }
                if bt.last > t.through {
                    report.tail_beyond_gap += 1;
                    continue;
                }
                let rid = bt.records.iter().find_map(|r| match r {
                    LogRecord::Completed { rid } => Some(*rid),
                    _ => None,
                });
                let refused = bt.records.iter().find_map(|r| match r {
                    LogRecord::Refused { rid, .. } => Some(*rid),
                    _ => None,
                });
                let done = match (rid, refused) {
                    (Some(rid), _)
                        if matches!(
                            crate::store::inbox::completed_outcome_tx(&tx, self, rid)?,
                            Some(crate::store::inbox::CompletedOutcome::Executed { .. })
                        ) =>
                    {
                        true
                    }
                    (_, Some(rid))
                        if crate::store::inbox::completed_outcome_tx(&tx, self, rid)?.is_some() =>
                    {
                        true
                    }
                    _ => false,
                };
                if done {
                    tracing::warn!(
                        epoch = t.epoch,
                        first = bt.first,
                        last = bt.last,
                        origin = ?bt.origin,
                        rid = ?rid.or(refused),
                        "a row of the sealed tail already has an outcome here; not re-shipped"
                    );
                    report.tail_done += 1;
                    continue;
                }
                // Plan 43 C1-4 (D10): the transaction's identity travels with
                // it, as its first record: the journal that first held it.
                // One that carries one already keeps it (re-shipped twice,
                // A to B to C, it is A's still); a root's append of a
                // delegate's row too (the delegation's identity is its
                // origin, the `TailOf` the tenure's).
                let records: std::borrow::Cow<'_, [LogRecord]> =
                    if crate::store::member::tail_identity(&bt.records).is_none() {
                        let mut r = Vec::with_capacity(bt.records.len() + 1);
                        r.push(LogRecord::TailOf {
                            epoch: t.epoch,
                            jseq: bt.first,
                        });
                        r.extend(bt.records.iter().cloned());
                        std::borrow::Cow::Owned(r)
                    } else {
                        std::borrow::Cow::Borrowed(&bt.records)
                    };
                let one = self
                    .journal_tail_tx(&mut tx, &records, rid, bt.origin)
                    .map_err(|e| match e {
                        MetaError::TailDiverged { why, .. } => MetaError::TailDiverged {
                            prev: t.epoch,
                            first: bt.first,
                            last: bt.last,
                            why,
                        },
                        other => other,
                    })?;
                journaled.push((bt.origin, one));
                // This node's journal holds the row now; the group's copy
                // stays as a member copy until the log carries it.
                if bt.origin == (0, 0) {
                    moved.push(bt.clone());
                }
            }
            // Plan 43 C1-4 (S4): the successor keeps the committed rows it
            // re-ships as a member copy of the epoch they were committed in,
            // so that a claimant of its lease that fetches the inherited
            // tail finds them here whatever happens to its journal, until
            // the log carries their identities.
            if !moved.is_empty() {
                crate::store::member::put_rows_tx(
                    &mut tx,
                    self,
                    t.epoch,
                    t.config_version,
                    &moved,
                )?;
            }
            report.tail_applied = journaled.len();
            report.tail_moved = moved.len();
            crate::store::backup::backup_clear_tx(&mut tx, self)?;
        }
        // The streams this gate ended hold what the log holds of them from
        // here on (the tail's rows counted): no row past it stays in the
        // journal, so the counter has no hole.
        for gen in &part.ended {
            local::settle_deleg_idx_at_log_tx(&mut tx, &self.local, *gen)?;
        }
        if !part.work && tail.is_none() {
            // Nothing to strand and no tail: nothing to commit.
            return Ok(report);
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        report.hold_ms = u64::try_from(held_since.elapsed().as_millis()).unwrap_or(u64::MAX);
        for (gen, cut) in part.orphan_cuts {
            tracing::info!(
                gen = %crate::delegation::GenLabel::of(gen),
                cut,
                "this claim ended a delegation generation whose grant never reached the log"
            );
            self.session.void_stream(gen, cut);
            self.session.note_void_cut(gen, cut);
        }
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        if tail.is_some() {
            *self.backup_tail_floor.lock().unwrap() = None;
            for ((gen, idx), one) in journaled {
                one.staged.drain_into(self.usage_tracker());
                one.moved.drain_into(self.usage_tracker());
                self.note_unshipped(&one.applied);
                // A root's append of a delegate's row, re-shipped: the
                // session's per-generation position follows (the core's
                // `deps` checks and the delegate's acknowledgement read it).
                if gen != 0 {
                    self.session.note_stream(gen, idx);
                }
            }
            if report.tail_moved > 0 {
                self.note_member_copies(true);
                // The transaction has committed (it reached the OS, as every
                // commit does: a process crash loses nothing); this `fsync`
                // only closes the power-loss window of the member copy. Its
                // failure must not turn the call into an `Err`: `Err` means
                // nothing changed, the caller would retry over a cleared
                // tail and skip its bookkeeping (the strand's rollback
                // accounting, the stats). It is reported in the result and
                // the next sync (any other safety write's) covers the rows.
                if let Err(e) = reship_sync(self) {
                    tracing::warn!(
                        error = %e,
                        "the re-ship committed but the sync that follows it failed; the next sync covers it"
                    );
                    report.sync_failed = true;
                }
            }
        }
        Ok(report)
    }

    /// Plan 46 M1.2 step 6: the stream counters of the generations this
    /// node's takeover gate ended are the log's index of each, after the
    /// gate re-appended its own rows under them (`Core::reappend_own_delegate_rows`),
    /// which raised the log's index past what the tail left. Only
    /// generations the gate ended ([`Meta::deleg_ended_at_gate`]).
    pub fn end_own_streams_at_log(&self, gens: &[u64]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        for gen in gens {
            if local::deleg_ended_at_gate_tx(&tx, &self.local, *gen)? {
                local::settle_deleg_idx_at_log_tx(&mut tx, &self.local, *gen)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The strand of [`Self::reship_at_takeover`], in the caller's
    /// transaction (everything [`Self::strand_for_takeover`] documents).
    fn strand_part_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        staged: &UsageTracker,
        args: StrandArgs<'_>,
    ) -> Result<StrandPart, MetaError> {
        let StrandArgs {
            epoch,
            own_gens,
            abandoned,
            carried,
            proof,
        } = args;
        let listed_before = if proof.is_empty() {
            Default::default()
        } else {
            self.named_generations_tx(&*tx)?
        };
        // (Plan 46 M0.4 MUST-1, below: the table before the strand.)
        let table_before: std::collections::BTreeSet<u64> =
            crate::delegation::read_table_tx(&*tx, self)?
                .gens()
                .collect();
        let own = |entry: &LiveEntry| match entry {
            LiveEntry::Local { gen, .. } if *gen != 0 && own_gens.contains(gen) => Some(*gen),
            _ => None,
        };
        // Plan 43 C1-4: a committed entry of this node's own (a requester's
        // shadow, a subscriber's streamed transaction of the tenure being
        // taken over) is taken out here too: the gate re-applies the
        // predecessor's tail, which carries the row, so the entry would
        // apply it twice. Not queued — unless the operator abandoned the
        // group, whose rows no one re-ships.
        let abandons = |entry: &LiveEntry| abandoned.is_some_and(|a| entry.stranded_by_abandon(a));
        let stranded = strand_tx(
            tx,
            self,
            staged,
            |entry| {
                entry.stranded_by_takeover(epoch)
                    || own(entry).is_some()
                    || entry.superseded_by_takeover(epoch, carried)
                    || abandons(entry)
            },
            true,
            false,
            false,
            |entry| entry.superseded_by_takeover(epoch, carried) && !abandons(entry),
        )?;
        // Plan 46 M0.4 (MUST-1): this node's own delegate rows of a
        // generation the table does not list are taken out whatever the
        // claim carries, not only with a tail (`own_gens`). With no tail,
        // nothing the predecessors' journals held past the log ever ships,
        // and this tenure mints in its own block only: an unlisted
        // generation's rows can never follow a `DelegateNode` in the log. They
        // are rolled back, queued for replay by rid (deduped against the
        // rid's outcome in the log), and the generation ends here at what the
        // log holds of it.
        //
        // Unlisted both before the strand above and after it. Not after it
        // only: the strand takes out a committed streamed entry the log
        // carries (`superseded_by_takeover`), its `DelegateNode` among them,
        // and the log's copy still lists the generation. Not before it
        // only: an outstanding streamed-ahead end of the deposed tenure
        // unlists a generation the log granted, and the strand takes the
        // end out and lists the generation again (the orphan rule judges it
        // after the strand for the same reason); its rows then ship
        // verbatim under the log's grant. So a second pass, in the same
        // transaction; the replay queue is keyed by the entries' own
        // sequence, so the replay order is still the journal's.
        let unlisted_own: Vec<u64> = {
            let listed: std::collections::BTreeSet<u64> =
                crate::delegation::read_table_tx(&*tx, self)?
                    .gens()
                    .chain(table_before.iter().copied())
                    .collect();
            let mut out = Vec::new();
            for g in local::deleg_gens_awaiting_end_tx(&*tx, self, &listed)? {
                if !own_gens.contains(&g) && local::deleg_idx_assigned_tx(&*tx, &self.local, g)? {
                    out.push(g);
                }
            }
            out
        };
        let mut stranded = stranded;
        if !unlisted_own.is_empty() {
            let more = strand_tx(
                tx,
                self,
                staged,
                |entry| matches!(entry, LiveEntry::Local { gen, .. } if *gen != 0 && unlisted_own.contains(gen)),
                true,
                false,
                false,
                |_| false,
            )?;
            stranded.shadows += more.shadows;
            stranded.hints += more.hints;
            stranded.locals += more.locals;
            stranded.member_copied += more.member_copied;
            stranded.superseded += more.superseded;
            stranded.committed_rolled_back += more.committed_rolled_back;
            stranded.gated_queued += more.gated_queued;
            stranded.deleg_dropped += more.deleg_dropped;
        }
        let own_gens: Vec<u64> = own_gens.iter().chain(&unlisted_own).copied().collect();
        let own_gens = own_gens.as_slice();
        let orphaned = self.orphaned_tx(&*tx, proof, &listed_before, &HashSet::new())?;
        let mut orphan_cuts: Vec<(u64, u64)> = Vec::new();
        for gen in &unlisted_own {
            if orphaned.contains(gen) {
                continue;
            }
            let cut = local::bump_void_cut_at_log_idx_tx(tx, &self.local, *gen)?;
            orphan_cuts.push((*gen, cut));
        }
        if !orphaned.is_empty() {
            for gen in &orphaned {
                let cut = local::bump_void_cut_at_log_idx_tx(tx, &self.local, *gen)?;
                orphan_cuts.push((*gen, cut));
                // Never executed in again (`Meta::begin_local`).
                local::mark_deleg_ended_at_gate_tx(tx, &self.local, *gen);
            }
            let ended: HashSet<u64> = orphaned.iter().copied().collect();
            let more = strand_tx(
                tx,
                self,
                staged,
                |entry| entry.stranded_by_recall(&ended),
                false,
                true,
                false,
                |_| false,
            )?;
            stranded.shadows += more.shadows;
            stranded.hints += more.hints;
            stranded.locals += more.locals;
            stranded.deleg_dropped += more.deleg_dropped;
            for gen in &orphaned {
                local::clear_deleg_streamed_tx(tx, &self.local, *gen);
            }
        }
        if !stranded.any() && own_gens.is_empty() && orphan_cuts.is_empty() {
            return Ok(StrandPart {
                stranded,
                orphan_cuts,
                work: false,
                ended: Vec::new(),
            });
        }
        // Every generation this node holds as the delegate ends here,
        // whether or not the strand took rows of it: from this
        // transaction on, under the writer lock, its own delegate
        // execution is refused (`Meta::begin_local`), and the op goes
        // through the core, whose gate parks it until the replay is done.
        // The core's own `stopped` (set from this mark after the strand
        // returns) and the engine's mirror of it lag by an event; the
        // store is the fence. A write the fast path had admitted before
        // the strand and that reached the writer lock after it used to
        // land first, under the stranded rows' names: the replay then
        // found `f7` taken, the stranded rename moved the fresh inode,
        // and acknowledged writes were lost (load campaign
        // `load-probe-takeover` under `pause:lock-lapse+500:10000@b`,
        // seeds 55/69/75/77). Nothing of this node executes on the
        // replica between this strand and the end of the gate's replay.
        let mut ended: Vec<u64> = own_gens.to_vec();
        for gen in own_gens {
            local::mark_deleg_ended_at_gate_tx(tx, &self.local, *gen);
        }
        ended.extend(orphaned.iter().copied());
        ended.sort_unstable();
        ended.dedup();
        Ok(StrandPart {
            stranded,
            orphan_cuts,
            work: true,
            ended,
        })
    }

    /// ADR-43 (the restart path): the generations whose end only
    /// outstanding requester or subscriber speculation carries: a
    /// `DelegateNodeEnded` among the records of a live `Shadow` or
    /// `Streamed` entry, of a generation this replica has no durable end of
    /// (`deleg_void_cut`, written only by the log's apply of the record).
    /// Such an install writes the table row without the void
    /// (`replay::apply_one`, `durable = false`), and may strand, which
    /// relists the generation and lifts no void: the session seeding treats
    /// these as live. `Local` rows are not counted (this root's own
    /// journaled end voids at once, and its strand stages the unvoid).
    pub(crate) fn speculatively_ended_generations(
        &self,
    ) -> Result<std::collections::BTreeSet<u64>, MetaError> {
        self.speculatively_ended_generations_at(&self.db.read_tx())
    }

    /// ADR-43 (orphan grants): the generations a segment's proof
    /// ([`OrphanProof`]: an operator's abandon of the root group of an epoch,
    /// or a takeover marker that says no tail of the epoch it took over
    /// follows) shows the log will never grant, after the segment's strands.
    /// A number is minted, and its `DelegateNode` journaled, by the tenure of its
    /// epoch only (`gen_epoch`), and a row of a generation is appended only
    /// after its grant; the proof says that nothing of the epochs it covers
    /// that the log does not carry yet ever will be (the strand above already
    /// rolls back the outstanding entries of those epochs on that premise,
    /// `LiveEntry::stranded_by_abandon`, `LiveEntry::stranded_by`). So a
    /// generation of such an epoch that the table no longer lists, that this
    /// replica holds no trace of in the log (no `deleg_void_cut`, no
    /// `deleg_log_idx`), that no outstanding speculation ends and that this
    /// segment does not recall was granted only by speculation the proof
    /// stranded: the log holds nothing of it and never will. The candidates
    /// are the ones named before the strand (`listed_before`,
    /// [`Self::named_generations_tx`]) and the ones this node holds its own
    /// delegate rows of (`deleg_gens_awaiting_end`: a delegate that executed
    /// under the stranded grant; they would block its installs for good,
    /// `delegation_sync`, and a claimant would ship them as root rows of a
    /// generation the log never granted). The cut loop of the segment runs
    /// before this rule: that ordering keeps a generation the segment ends
    /// in the same transaction out, by its cut key. Numbers of the old mint
    /// (epoch 0) are left alone: their minting tenure is unknown.
    fn orphaned_tx(
        &self,
        tx: &SingleWriterWriteTx,
        proof: &OrphanProof,
        listed_before: &std::collections::BTreeSet<u64>,
        recalled: &HashSet<u64>,
    ) -> Result<Vec<u64>, MetaError> {
        if proof.is_empty() {
            return Ok(Vec::new());
        }
        let listed: std::collections::BTreeSet<u64> =
            crate::delegation::read_table_tx(tx, self)?.gens().collect();
        let mut candidates = listed_before.clone();
        candidates.extend(local::deleg_gens_awaiting_end_tx(tx, self, &listed)?);
        let candidates: Vec<u64> = candidates
            .into_iter()
            .filter(|g| {
                proof.covers(crate::delegation::gen_epoch(*g))
                    && !listed.contains(g)
                    && !recalled.contains(g)
            })
            .collect();
        if candidates.is_empty() {
            return Ok(candidates);
        }
        let speculative = self.speculatively_ended_generations_at(tx)?;
        let mut out = Vec::new();
        for g in candidates {
            if speculative.contains(&g)
                || kv_get_tx(tx, &self.local, &local::deleg_void_cut_key(g))?.is_some()
                || kv_get_tx(tx, &self.local, &local::deleg_log_idx_key(g))?.is_some()
            {
                continue;
            }
            out.push(g);
        }
        Ok(out)
    }

    /// ADR-43 (orphan grants): the generations this replica names before a
    /// strand that may take their grant ([`Self::orphaned_tx`]'s candidates):
    /// the table's (a streamed grant installs there), those of the live
    /// shadows and hints of delegate replies (`gen != 0`: a requester that
    /// never listed the grant holds them, and the strand may take the entry,
    /// queuing its rid for replay, held while the generation lives, without
    /// ending the generation), and those a live entry's records grant (a
    /// streamed `DelegateNode` whose `DelegateNodeEnded` streamed too: the install took
    /// the generation out of the table again, and a dependency on it waited
    /// for an end nothing applies, the N4 wait).
    fn named_generations_tx(
        &self,
        tx: &SingleWriterWriteTx,
    ) -> Result<std::collections::BTreeSet<u64>, MetaError> {
        let mut named: std::collections::BTreeSet<u64> =
            crate::delegation::read_table_tx(tx, self)?.gens().collect();
        for (seq, entry) in live_non_local(tx, self)? {
            if let LiveEntry::Shadow { gen, .. } | LiveEntry::Hint { gen, .. } = entry {
                if gen != 0 {
                    named.insert(gen);
                }
            }
            let Some(v) = tx.get(&self.spec, seq_key(seq))? else {
                continue;
            };
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            for rec in &row.records {
                if let LogRecord::DelegateNode { gen, .. } = rec {
                    named.insert(*gen);
                }
            }
        }
        Ok(named)
    }

    /// Plan 46 M1.2 step 8b (M1.0 answer 4): the generations whose
    /// `DelegateNode` record this replica holds only as a streamed entry the
    /// root did not stream as committed (`StreamAhead.committed` unset: its
    /// configuration does not hold the row, a lone root's journal above all).
    /// The table lists such a grant already, but it is not honoured: no
    /// archive and no group holds it, and a root deposed meanwhile is
    /// succeeded by a claimant that adopts the archive alone and ends the
    /// number as an orphan. The segment that carries the row retires the
    /// entry; the grant is committed from then on.
    pub fn uncommitted_streamed_grants(
        &self,
    ) -> Result<std::collections::BTreeSet<u64>, MetaError> {
        let r = self.db.read_tx();
        let mut out = std::collections::BTreeSet::new();
        for (seq, entry) in live_non_local(&r, self)? {
            if !matches!(entry, LiveEntry::Streamed { gated: false, .. }) {
                continue;
            }
            let Some(v) = r.get(&self.spec, seq_key(seq))? else {
                continue;
            };
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            for rec in &row.records {
                if let LogRecord::DelegateNode { gen, .. } = rec {
                    out.insert(*gen);
                }
            }
        }
        Ok(out)
    }

    /// [`Self::speculatively_ended_generations`] under `r` (the read path
    /// takes it in the snapshot it reads the table in).
    pub(crate) fn speculatively_ended_generations_at(
        &self,
        r: &impl Readable,
    ) -> Result<std::collections::BTreeSet<u64>, MetaError> {
        let mut out = std::collections::BTreeSet::new();
        for (seq, entry) in live_non_local(r, self)? {
            if matches!(entry, LiveEntry::Hint { .. }) {
                continue;
            }
            let Some(v) = r.get(&self.spec, seq_key(seq))? else {
                continue;
            };
            let row: SpecRow = crate::record::from_bytes_exact(&v)?;
            for rec in &row.records {
                if let LogRecord::DelegateNodeEnded { gen, .. } = rec {
                    if kv_get_tx(r, &self.local, &local::deleg_void_cut_key(*gen))?.is_none() {
                        out.insert(*gen);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Plan 30 §M11 on the root: it ended generation `gen` itself (its
    /// own `DelegateNodeEnded` journaled, never applied from a segment here), so the
    /// segment path's recall strand never runs on its replica. Its own
    /// shadows and hints from that delegate's replies are stranded here
    /// the same way (rolled back, queued for replay by rid once the
    /// generation is gone); its own `Local` rows of `gen` are the
    /// delegate's appended stream, before the `DelegateNodeEnded`, and stay.
    /// Without this, the root kept a shadow of its own create that the
    /// delegate never streamed (partitioned, outwaited), executed its
    /// next op on top of it and journaled an `unlink` of a name the log
    /// never created (long-delegated seed 72780).
    pub fn strand_recalled_speculation(&self, gen: u64) -> Result<Stranded, MetaError> {
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = strand_tx(
            &mut tx,
            self,
            &staged,
            |entry| {
                matches!(
                    entry,
                    LiveEntry::Shadow { gen: g, .. } | LiveEntry::Hint { gen: g, .. } if *g == gen
                )
            },
            false,
            false,
            false,
            |_| false,
        )?;
        if !stranded.any() {
            return Ok(stranded);
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        Ok(stranded)
    }

    /// Plan 45 §12 M2: the root appends a delegate's refusal of `rid`
    /// (`Meta::apply_delegate_tx`) over its own live shadow of that rid.
    /// The delegate had accepted the op, the root forwarded it and
    /// installed the reply, and then `repair drop-held` on the delegate
    /// let the transaction go and journaled the refusal; the shadow's
    /// effect never reaches the log. It is stranded by rid in the
    /// refusal's transaction, before the refusal's own `Local` bracket
    /// opens (what the root installed after it is redone on top, as in
    /// every rewind), and its queued replay is dropped: the outcome is
    /// final. This is the segment path's rule for a refused rid's shadow
    /// (`apply_segment_rows`), and the refusal's face of the append's
    /// unwind by rid (`unwind_shadow_for_append_tx`). Before, the shadow
    /// stayed live under the journaled refusal, and the refusal's ship
    /// retired it in place (`Meta::ack_rows_tx`): the refused op's effect
    /// stayed in the root's replica and reached its next publish.
    pub(crate) fn journal_refusal_over_shadow(
        &self,
        rid: Rid,
        code: constellation_types::Code,
    ) -> Result<Stranded, MetaError> {
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = if counter_get(&tx, &self.local, KV_SPEC_LIVE_COUNT)? > 0 {
            strand_tx(
                &mut tx,
                self,
                &staged,
                |entry| matches!(entry, LiveEntry::Shadow { rid: r, .. } if *r == rid),
                false,
                false,
                false,
                |_| false,
            )?
        } else {
            Stranded::default()
        };
        if stranded.shadows > 0 {
            drop_queued_of_rids_tx(&mut tx, self, &HashSet::from([rid]))?;
        }
        self.journal_refusal_tx(&mut tx, rid, code)?;
        if stranded.any() {
            let from = journal_from(&tx, self)?;
            compact_tx(&mut tx, self, from)?;
            let (bytes, files) = staged.raw_delta();
            adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        }
        tx.commit()?;
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        if stranded.any() {
            tracing::debug!(?rid, "root append of a refusal strands its shadow");
        }
        Ok(stranded)
    }

    /// Whether any shadow or hint is outstanding: this replica is then not
    /// a log prefix, and must not be published (plan 30 §M3a's publish
    /// rule). Queued replays do not count — their effects are already
    /// rolled back — and neither does this node's own `Local` speculation,
    /// which the publisher substitutes instead (`Meta::publish_basis_at`).
    ///
    /// One counter read (`store::KV_SPEC_LIVE_COUNT`): `spec_live`'s own
    /// first key would walk the tombstones of every retired shadow.
    /// Whether any stranded op is queued for replay (one counter read).
    pub fn has_pending_replays(&self) -> bool {
        let r = self.db.read_tx();
        counter_get(&r, &self.local, KV_PENDING_REPLAY_COUNT).is_ok_and(|n| n > 0)
    }

    /// Whether an outstanding streamed transaction, shadow or hint (not
    /// this node's own `Local` work: that is its unshipped journal)
    /// touches any of `keys`. A delegate's forward reply names no `base`
    /// then (`constellation_authority`'s `delegate_execute_now`): its
    /// replica evaluated the op on state ahead of the log that the
    /// requester may not have. One counter read when nothing is
    /// outstanding.
    pub fn speculation_touches(&self, keys: &TouchSet) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        for (seq, _) in live_non_local(&r, self)? {
            if let Some(v) = r.get(&self.spec, seq_key(seq))? {
                let row: SpecRow = crate::record::from_bytes_exact(&v)?;
                if TouchSet::from_records(row.records.iter()).overlaps(keys) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn has_outstanding_speculation(&self) -> bool {
        let r = self.db.read_tx();
        counter_get(&r, &self.local, KV_SPEC_LIVE_COUNT).is_ok_and(|n| n > 0)
    }

    /// Outstanding entries and queued replays, for `status` (polled as
    /// often as every few milliseconds): three counter reads under one
    /// snapshot, never a scan — plan 30 §M3b's hot-path rule.
    /// Every live speculation entry, for diagnostics.
    pub fn speculation_live_debug(&self) -> Vec<String> {
        let r = self.db.read_tx();
        read_live(&r, self)
            .map(|v| v.into_iter().map(|(s, e)| format!("{s}:{e:?}")).collect())
            .unwrap_or_default()
    }

    pub fn speculation_counts(&self) -> Result<SpeculationCounts, MetaError> {
        let r = self.db.read_tx();
        Ok(SpeculationCounts {
            outstanding: counter_get(&r, &self.local, KV_SPEC_LIVE_COUNT)?,
            pending_replay: counter_get(&r, &self.local, KV_PENDING_REPLAY_COUNT)?,
            local: counter_get(&r, &self.local, KV_LOCAL_SPEC_COUNT)?,
        })
    }

    /// Plan 46 M0.2 review (R7): strand the shadows of non-stealable
    /// answers installed before `before_ms` (wall clock of the installing
    /// core). A non-stealable shadow ends by the rid's outcome in the log or
    /// this node's own takeover; if its owner's journal was lost, or the
    /// owner retired, neither comes. After the bound it is rolled back and
    /// queued for replay by rid like any strand. A read of the live entries
    /// first: nothing is written unless one is that old.
    pub fn strand_stale_shadows(&self, before_ms: i64) -> Result<Stranded, MetaError> {
        let stale = |entry: &LiveEntry| matches!(entry, LiveEntry::Shadow { non_stealable: Some(at), .. } if *at < before_ms);
        {
            let r = self.db.read_tx();
            if counter_get(&r, &self.local, KV_SPEC_LIVE_COUNT)? == 0 {
                return Ok(Stranded::default());
            }
            let floor = counter_get(&r, &self.local, KV_SPEC_FLOOR)?;
            let mut any = false;
            for guard in r.range(&self.spec_live, seq_key(floor)..) {
                let (_, v) = guard.into_inner()?;
                let entry: LiveEntry = crate::record::from_bytes_exact(&v)?;
                any |= stale(&entry);
            }
            if !any {
                return Ok(Stranded::default());
            }
        }
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = strand_tx(&mut tx, self, &staged, stale, true, false, false, |_| false)?;
        if !stranded.any() {
            return Ok(stranded);
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        Ok(stranded)
    }

    /// The replay queue as a [`QueueSnapshot`], decoded once per
    /// generation of the queue. One counter read when it is empty.
    pub fn queue_snapshot(&self) -> Result<std::sync::Arc<QueueSnapshot>, MetaError> {
        let r = self.db.read_tx();
        if counter_get(&r, &self.local, KV_PENDING_REPLAY_COUNT)? == 0 {
            return Ok(std::sync::Arc::new(QueueSnapshot::default()));
        }
        let gen = counter_get(&r, &self.local, KV_PENDING_REPLAY_GEN)?;
        if let Some(cached) = &*self.queue_cache.lock().unwrap() {
            if cached.gen == gen {
                return Ok(cached.clone());
            }
        }
        let snap = std::sync::Arc::new(QueueSnapshot::build(gen, read_pending_replays(&r, self)?));
        *self.queue_cache.lock().unwrap() = Some(snap.clone());
        Ok(snap)
    }

    /// Whether the replay queue holds an entry, not refused and not already
    /// completed in the log, that touches a key of `op`: an own op (a FUSE
    /// write, the fast path) must not run ahead of it. The fast path asks
    /// before it executes (`root_fast_path`); a refused answer sends the op
    /// through the core, which parks it behind the entries ahead of it.
    pub fn queue_overlaps_op(&self, op: &MutateOp) -> bool {
        let Ok(snap) = self.queue_snapshot() else {
            return false;
        };
        if snap.is_empty() {
            return false;
        }
        let lookup = |p: u64, n: &str| {
            crate::MetaStore::lookup(self, p, n)
                .ok()
                .flatten()
                .map(|a| a.ino)
        };
        let mut keys = TouchSet::from_op_in(op, &lookup);
        keys.inos.extend(created_ino_of(op));
        (0..snap.entries.len()).any(|i| {
            let e = &snap.entries[i];
            e.refused.is_none()
                && snap.keys_of(i, &lookup).overlaps(&keys)
                && !matches!(self.completed_outcome(e.rid), Ok(Some(_)))
        })
    }

    /// Every queued replay, oldest (original acceptance order) first. The
    /// replay drain asks every 250 ms; an empty queue is one counter read.
    pub fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError> {
        let r = self.db.read_tx();
        read_pending_replays(&r, self)
    }

    /// Queue `op` for replay by `rid` directly, after everything already
    /// queued. For callers that learn of a stranded op outside any
    /// stranding pass.
    pub fn queue_replay(&self, rid: Rid, op: &MutateOp, tag: &LockTag) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let key = next_spec_seq_tx(&mut tx, &self.local)?;
        enqueue_replay_tx(&mut tx, self, key, rid, op.clone(), tag.clone(), &[])?;
        tx.commit()?;
        Ok(())
    }

    /// Queue `op` as a replay already refused for `refusal`, after
    /// everything queued: the drain materializes its conflict copy and
    /// never executes it. Plan 30 §M14 phase 2: a close whose commit the
    /// sequencer refused for a lock grant that had ended keeps its data
    /// this way instead of dropping it.
    pub fn queue_refused_replay(
        &self,
        rid: Rid,
        op: &MutateOp,
        refusal: Refusal,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let key = next_spec_seq_tx(&mut tx, &self.local)?;
        refuse_queued_tx(&mut tx, self, key, rid, op.clone(), refusal, false)?;
        tx.commit()?;
        Ok(())
    }

    /// Record that `queue_seq`'s replay was refused, before materializing
    /// the conflict copy (see [`Refusal`]).
    pub fn mark_replay_refused(&self, queue_seq: u64, refusal: Refusal) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let Some(v) = tx.get(&self.pending_replay, seq_key(queue_seq))? else {
            return Ok(());
        };
        let mut row: QueuedReplay = crate::record::from_bytes_exact(&v)?;
        row.refused = Some(refusal);
        tx.insert(
            &self.pending_replay,
            seq_key(queue_seq),
            postcard::to_allocvec(&row)?,
        );
        counter_add_tx(&mut tx, &self.local, KV_PENDING_REPLAY_GEN, 1)?;
        tx.commit()?;
        Ok(())
    }

    /// Plan 30 §M9: mark `queue_seq` foreign-like — its op was executed
    /// here but never acknowledged (its reply waited for durability when
    /// the tenure ended), so a refusal of its replay is an outcome for
    /// its client, not an effect to preserve as a conflict copy.
    pub fn mark_replay_unacked(&self, queue_seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let Some(v) = tx.get(&self.pending_replay, seq_key(queue_seq))? else {
            return Ok(());
        };
        let mut row: QueuedReplay = crate::record::from_bytes_exact(&v)?;
        if row.foreign {
            return Ok(());
        }
        row.foreign = true;
        tx.insert(
            &self.pending_replay,
            seq_key(queue_seq),
            postcard::to_allocvec(&row)?,
        );
        counter_add_tx(&mut tx, &self.local, KV_PENDING_REPLAY_GEN, 1)?;
        tx.commit()?;
        Ok(())
    }

    /// Drop `queue_seq` from the replay queue once it is resolved:
    /// accepted, found already completed, or refused and materialized.
    pub fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.pending_replay, seq_key(queue_seq))?.is_some() {
            tx.remove(&self.pending_replay, seq_key(queue_seq));
            counter_add_tx(&mut tx, &self.local, KV_PENDING_REPLAY_GEN, 1)?;
            counter_add_tx(&mut tx, &self.local, KV_PENDING_REPLAY_COUNT, -1)?;
        }
        tx.commit()?;
        // Plan 30 §M6: a read waiting on this replay's keys may go.
        self.session.notify();
        Ok(())
    }
}
