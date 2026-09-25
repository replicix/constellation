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
//!   `Completed { rid }`; a hint when the applied position reaches its
//!   floor; a `Local` entry when its journal rows ship. Retiring only
//!   drops the index entry. The row stays while anything older is
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
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::replay::{apply_batch_tx, apply_record, ApplyCx, TouchSet};
use crate::rid::Rid;
use crate::store::local::{self, LogPrefixView};
use crate::store::{
    adjust_usage_tx, counter_add_tx, counter_get, counter_set_tx, kv_get_tx, kv_set_tx, misc, ns,
    Meta, UsageTracker, KV_APPLIED_SEQ, KV_LOCAL_SPEC_COUNT, KV_NEXT_SPEC_SEQ, KV_NODE_PREFIX,
    KV_PENDING_REPLAY_COUNT, KV_SPEC_FLOOR, KV_SPEC_LIVE_COUNT, KV_UNCAPTURED_TX_COUNT,
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

// ---------------------------------------------------------------- types

/// What one `spec` row is. See the module doc.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpecKind {
    /// A forwarded op's accepted reply, installed ahead of the log.
    /// `epoch` is the accepting holder's; `op` is what a replay re-sends.
    /// Plan 30 §M11: `gen` is the accepting delegate's generation (0: the
    /// root's); the log's `Recall` of it strands the shadow.
    Shadow {
        rid: Rid,
        epoch: u64,
        op: MutateOp,
        gen: u64,
    },
    /// The `Exists` early install: retires once the applied position
    /// reaches `floor`, strands if a segment above `epoch` arrives first
    /// (or, plan 30 §M11, the log recalls the answering delegate's
    /// generation `gen`).
    Hint { floor: u64, epoch: u64, gen: u64 },
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
        /// Plan 30 §M9: this node's own op. A requester subscribed to the
        /// holder's stream sees its op twice — on the stream and in its
        /// reply — and keeps *one* entry, this kind, whichever arrived
        /// first (`install_shadow` adopts a streamed entry; a shadow that
        /// was first becomes streamed in `install_streamed`). Retired by
        /// the segment's row skip like any streamed transaction, which
        /// is what keeps later streamed rows right; stranded, it replays
        /// by rid as this node's op — a conflict copy on refusal — rather
        /// than as a foreign row.
        own: Option<(Rid, MutateOp)>,
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
    },
    Hint {
        floor: u64,
        epoch: u64,
        gen: u64,
    },
    Streamed {
        epoch: u64,
        last: u64,
    },
    /// Plan 30 §M11: `gen` is the delegation stream the transaction
    /// belongs to (0: this node's own row as a holder).
    Local {
        first: u64,
        epoch: u64,
        gen: u64,
    },
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
    /// successor); only the log's `Recall` of the generation does.
    fn stranded_by(&self, epoch: u64) -> bool {
        match self {
            LiveEntry::Local { epoch: 0, .. } => false,
            LiveEntry::Local { gen, .. } if *gen != 0 => false,
            entry => entry.epoch() < epoch,
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
    #[serde(default)]
    foreign: bool,
    /// Plan 30 §M11 phase 2b round 2: the delegation generation whose
    /// stream the op's reply named (a shadow accepted by a delegate);
    /// 0: the root's. Held from replay while that generation is live in
    /// the table — the delegate re-streams it to the successor root, or
    /// the generation ends — so a successor never executes it ahead of
    /// the delegate's earlier transactions (long-delegated seed 70075).
    #[serde(default)]
    gen: u64,
    /// A size-only `setattr` (the FUSE truncate path, `O_TRUNC`
    /// included): the manifest its inode had just before it, from the
    /// stranded row's before-image. A manifest commit for the same inode
    /// queued after it is rebased onto this (see [`rebase_on_truncate`]).
    #[serde(default)]
    truncate_base: Option<Vec<u8>>,
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
    /// Plan 30 §M11 phase 2b round 2: see `QueuedReplay::gen`.
    pub gen: u64,
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
}

impl Stranded {
    pub fn any(&self) -> bool {
        self.shadows + self.hints + self.locals > 0
    }

    /// Entries rolled back, of any kind.
    pub fn total(&self) -> usize {
        self.shadows + self.hints + self.locals
    }
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
            out.push((decode_seq(&k)?, postcard::from_bytes(&v)?));
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
            let row: SpecRow = postcard::from_bytes(&v)?;
            let theirs = TouchSet::from_records(row.records.iter());
            if theirs.dentries.iter().any(|d| touches.dentries.contains(d))
                || theirs.inos.iter().any(|i| touches.inos.contains(i))
            {
                return Ok(true);
            }
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
            let row: SpecRow = postcard::from_bytes(&v)?;
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
) -> Result<bool, MetaError> {
    let Some(spec_seq) = streamed_entry_completing(tx, meta, rid)? else {
        return Ok(false);
    };
    if let Some(v) = tx.get(&meta.spec, seq_key(spec_seq))? {
        let mut row: SpecRow = postcard::from_bytes(&v)?;
        if let SpecKind::Streamed { own, .. } = &mut row.kind {
            *own = Some((rid, op.clone()));
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
) -> Result<Option<(u64, u64, MutateOp)>, MetaError> {
    for (seq, entry) in read_live(r, meta)? {
        if !matches!(entry, LiveEntry::Shadow { rid: r2, .. } if r2 == rid) {
            continue;
        }
        if let Some(v) = r.get(&meta.spec, seq_key(seq))? {
            let row: SpecRow = postcard::from_bytes(&v)?;
            if let SpecKind::Shadow { epoch, op, .. } = row.kind {
                return Ok(Some((seq, epoch, op)));
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
        out.push((decode_seq(&k)?, postcard::from_bytes(&v)?));
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
            rid, epoch, gen, ..
        } => Some(LiveEntry::Shadow {
            rid: *rid,
            epoch: *epoch,
            gen: *gen,
        }),
        SpecKind::Hint { floor, epoch, gen } => Some(LiveEntry::Hint {
            floor: *floor,
            epoch: *epoch,
            gen: *gen,
        }),
        SpecKind::Streamed { epoch, last, .. } => Some(LiveEntry::Streamed {
            epoch: *epoch,
            last: *last,
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
    before: &Before,
) -> Result<(), MetaError> {
    enqueue_replay_as_tx(tx, meta, key, rid, op, false, 0, before)
}

/// [`enqueue_replay_tx`] for a shadow a delegate accepted under `gen`.
fn enqueue_replay_gen_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    op: MutateOp,
    gen: u64,
    before: &Before,
) -> Result<(), MetaError> {
    enqueue_replay_as_tx(tx, meta, key, rid, op, false, gen, before)
}

/// Queue `op` for replay by `rid` under `key`. `before` is the stranded
/// row's before-images (empty when there are none): a truncate keeps the
/// manifest it cut from them, and a manifest commit queued after one is
/// rebased onto it ([`rebase_on_truncate`]).
#[allow(clippy::too_many_arguments)]
fn enqueue_replay_as_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    mut op: MutateOp,
    foreign: bool,
    gen: u64,
    before: &Before,
) -> Result<(), MetaError> {
    let truncate_base = truncate_pre_manifest(tx, meta, &op, before);
    rebase_on_truncate(tx, meta, key, &mut op)?;
    let row = QueuedReplay {
        rid,
        op,
        refused: None,
        foreign,
        gen,
        truncate_base,
    };
    if tx.get(&meta.pending_replay, seq_key(key))?.is_none() {
        counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_COUNT, 1)?;
    }
    tx.insert(
        &meta.pending_replay,
        seq_key(key),
        postcard::to_allocvec(&row)?,
    );
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

/// A manifest commit queued right after a truncate of its inode is
/// rebased onto the manifest the truncate cut.
///
/// A truncate clips the stored manifest in its own transaction
/// (`replay::clip_manifest`), so a write session that follows it — the
/// write after an `O_TRUNC` open, say — commits with the *clipped*
/// manifest as its base. The replay drain folds the truncate into that
/// commit (executed alone it would have no base check, and would cut a
/// file another node wrote since), so the commit's base must stand for
/// the truncate too: the manifest the truncate started from. Left as
/// the clipped one, it is compared against the holder's uncut file —
/// the truncate never ran there — and every truncate-then-write of a
/// file nobody else touched was refused as a stale base and turned
/// into a conflict copy (harness `deposed-reintegration`'s `a-only`).
/// Rebased, a file changed elsewhere since still fails the check, as
/// the edit-vs-edit overlap it is.
///
/// Only when the commit composed on exactly that cut (its base is the
/// clipped pre-image), and only onto the nearest earlier queued size or
/// manifest change of the inode, which must be such a truncate.
fn rebase_on_truncate(
    tx: &SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    op: &mut MutateOp,
) -> Result<(), MetaError> {
    let MutateOp::SetManifest {
        ino,
        base_manifest: Some(base),
        ..
    } = op
    else {
        return Ok(());
    };
    if counter_get(tx, &meta.local, KV_PENDING_REPLAY_COUNT)? == 0 {
        return Ok(());
    }
    let mut nearest: Option<QueuedReplay> = None;
    for guard in tx.range(&meta.pending_replay, ..seq_key(key)).rev() {
        let (_, v) = guard.into_inner()?;
        let row: QueuedReplay = postcard::from_bytes(&v)?;
        let touches = match &row.op {
            MutateOp::SetManifest { ino: i, .. } => i == ino,
            MutateOp::Setattr {
                ino: i,
                size: Some(_),
                ..
            } => i == ino,
            _ => false,
        };
        if touches {
            nearest = Some(row);
            break;
        }
    }
    let Some(truncate) = nearest else {
        return Ok(());
    };
    let (
        MutateOp::Setattr {
            size: Some(size), ..
        },
        Some(pre),
        None,
    ) = (&truncate.op, &truncate.truncate_base, &truncate.refused)
    else {
        return Ok(());
    };
    if size_only_truncate(&truncate.op).is_none() {
        return Ok(());
    }
    let cut = crate::replay::clip_manifest(pre, *size);
    if cut.as_deref().unwrap_or(pre) == base.as_slice() {
        *base = pre.clone();
    }
    Ok(())
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
    let effective: Vec<&LogRecord> = records
        .iter()
        .filter(|rec| {
            !matches!(
                rec,
                LogRecord::Completed { .. }
                    | LogRecord::Atime { .. }
                    | LogRecord::Refused { .. }
                    | LogRecord::InboxAck { .. }
            )
        })
        .collect();
    match effective.as_slice() {
        [] => None,
        [LogRecord::WriteManifest {
            ino,
            base_manifest,
            manifest,
            size,
            ..
        }] => Some(MutateOp::SetManifest {
            ino: *ino,
            base_manifest: base_manifest.clone(),
            manifest: manifest.clone(),
            size: *size,
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
) -> Result<(), MetaError> {
    let Some(row) = local::get_journal_tx(&*tx, meta, first)? else {
        return Ok(());
    };
    let rows = local::journal_records(&*tx, meta, first, row.last)?;
    for (seq, _) in &rows {
        tx.remove(&meta.journal_ks, seq.to_be_bytes().to_vec());
    }
    tx.remove(&meta.journal_tx, local::tx_key(first));
    let count = if row.spec_seq.is_some() {
        KV_LOCAL_SPEC_COUNT
    } else {
        KV_UNCAPTURED_TX_COUNT
    };
    counter_add_tx(tx, &meta.local, count, -1)?;
    // The rid's `completed` row goes with the transaction — unless the
    // log carries the rid's outcome already (a row applied from a
    // segment records position 0; a journaled one its journal seq): a
    // delegate's transaction stranded by a recall after the requester's
    // own replay of the same rid landed (plan 30 §M11 phase 2b,
    // long-delegated seeds 70039/70075/70078: the completion forgotten,
    // the replay's dedup reply installed a shadow nothing retired).
    let log_carries = |tx: &SingleWriterWriteTx, rid: &Rid| -> Result<bool, MetaError> {
        Ok(tx
            .get(&meta.completed, rid.to_key())?
            .and_then(|v| {
                v.get(0..8)
                    .map(|p| u64::from_be_bytes(p.try_into().expect("8 bytes")))
            })
            .is_some_and(|position| position == 0))
    };
    let rid = match row.rid {
        Some(rid) => rid,
        None => replay_rid_for(&*tx, meta, first)?,
    };
    // (A rid-less transaction's replay rid carries a `completed` row too
    // when it journaled its own marker — the holder's manifest commit.)
    if !log_carries(tx, &rid)? {
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
            if !log_carries(tx, rid)? {
                tx.remove(&meta.completed, rid.to_key());
            }
        }
    }
    let op = match row.op {
        Some(op) => Some(op),
        None => derive_replay_op(&records),
    };
    if let Some(op) = op {
        enqueue_replay_tx(tx, meta, key, rid, op, before)?;
    }
    Ok(())
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
                // Linked again: whatever orphaned it is being undone.
                tx.remove(&meta.orphans, ino.to_be_bytes().to_vec());
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
fn rewind_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    live: &HashMap<u64, LiveEntry>,
    stranded: &HashSet<u64>,
    cutoff: u64,
    insert: Option<Inserted<'_>>,
) -> Result<(Stranded, usize), MetaError> {
    let rows = read_rows_from(tx, &meta.spec, cutoff)?;
    let plain = meta.dirty_for_ns();

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
            redo_row_tx(tx, meta, staged, live, stranded, &mut out, false, seq, row)?;
        }
        return Ok((out, skipped));
    };

    // 2a. Rows already in the log ahead of the inserted segment.
    let (durable, rest): (Vec<_>, Vec<_>) = rows
        .into_iter()
        .partition(|(_, row)| matches!(row.kind, SpecKind::Foreign { .. }));
    for (seq, row) in durable {
        redo_row_tx(tx, meta, staged, live, stranded, &mut out, true, seq, row)?;
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
            tx, meta, staged, live, stranded, &mut out, renumber, seq, row,
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
) -> Result<(), MetaError> {
    if stranded.contains(&seq) {
        tx.remove(&meta.spec, seq_key(seq));
        if matches!(
            row.kind,
            SpecKind::Shadow { .. } | SpecKind::Hint { .. } | SpecKind::Streamed { .. }
        ) {
            tx.remove(&meta.spec_live, seq_key(seq));
            counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -1)?;
        }
        match row.kind {
            SpecKind::Shadow { rid, op, gen, .. } => {
                enqueue_replay_gen_tx(tx, meta, row.origin, rid, op, gen, &row.before)?;
                out.shadows += 1;
            }
            SpecKind::Local { first, .. } => {
                strand_local_tx(tx, meta, first, row.origin, &row.before)?;
                out.locals += 1;
            }
            SpecKind::Streamed { own, .. } => {
                // Plan 30 §M9: the tenure ended without shipping it; its
                // successor re-ships what its backup held (or the op's
                // requester replays it). Until then reads of its keys
                // must not see the rolled-back state as final. This
                // node's own op (adopted by `install_shadow`) replays as
                // a shadow's would.
                if let Some((rid, op)) = own {
                    enqueue_replay_tx(tx, meta, row.origin, rid, op, &row.before)?;
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
                        enqueue_replay_as_tx(tx, meta, row.origin, rid, op, true, 0, &[])?;
                    }
                    out.hints += 1;
                }
            }
            SpecKind::Hint { .. } | SpecKind::Foreign { .. } => out.hints += 1,
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

/// Roll back and take out every outstanding entry `strands` selects (see
/// [`rewind_tx`]).
fn strand_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    strands: impl Fn(&LiveEntry) -> bool,
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
    Ok(rewind_tx(tx, meta, staged, &live, &stranded, cutoff, None)?.0)
}

/// Plan 30 §M9: what a segment says about the shipping tenure's journal,
/// for retiring `Streamed` entries: its epoch, the journal seq it ships
/// through, and the journal seqs of its rows.
#[derive(Clone, Copy, Debug)]
pub struct ShippedRows<'a> {
    pub epoch: u64,
    pub through: u64,
    pub rows: &'a [u64],
}

/// Retire every outstanding shadow whose rid `completes` names, every
/// hint whose floor `applied_seq` has reached, and every streamed
/// transaction `shipped` confirms.
pub(crate) fn retire_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    completes: &HashSet<Rid>,
    applied_seq: u64,
    shipped: Option<ShippedRows<'_>>,
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
            out.push((decode_seq(&k)?, postcard::from_bytes(&v)?));
        }
        out
    };
    for (seq, entry) in entries {
        let done = match entry {
            LiveEntry::Shadow { rid, .. } => completes.contains(&rid),
            LiveEntry::Hint { floor, .. } => applied_seq >= floor,
            // Rows-confirmed entries are converted by `apply_segment_rows`
            // (their records were skipped); one confirmed by `through`
            // alone shipped in a segment that did not name its rows.
            LiveEntry::Streamed { epoch, last } => shipped
                .is_some_and(|s| s.epoch == epoch && s.through >= last && !s.rows.contains(&last)),
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
    for (first, _) in &shipped {
        tx.remove(&meta.journal_tx, local::tx_key(*first));
    }
    counter_add_tx(tx, &meta.local, KV_LOCAL_SPEC_COUNT, -(captured as i64))?;
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
            let mut spec_row: SpecRow = postcard::from_bytes(&v)?;
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
/// namespace; M2 dedup makes a replay of one that did land harmless) and
/// drop everything else.
pub(crate) fn reset_for_rebuilt_ns_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
) -> Result<(), MetaError> {
    let live = read_live(tx, meta)?;
    for (seq, entry) in &live {
        if let LiveEntry::Shadow { .. } = entry {
            if let Some(v) = tx.get(&meta.spec, seq_key(*seq))? {
                let row: SpecRow = postcard::from_bytes(&v)?;
                if let SpecKind::Shadow { rid, op, gen, .. } = row.kind {
                    enqueue_replay_gen_tx(tx, meta, row.origin, rid, op, gen, &row.before)?;
                }
            }
            tx.remove(&meta.spec_live, seq_key(*seq));
        }
        if let LiveEntry::Hint { .. } | LiveEntry::Streamed { .. } = entry {
            tx.remove(&meta.spec_live, seq_key(*seq));
        }
    }
    counter_set_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, 0);
    for (first, row) in local::read_journal_txs(&*tx, meta)? {
        let (key, before) = match row.spec_seq {
            Some(seq) => match tx.get(&meta.spec, seq_key(seq))? {
                Some(v) => {
                    let spec_row = postcard::from_bytes::<SpecRow>(&v)?;
                    (spec_row.origin, spec_row.before)
                }
                None => (seq, Vec::new()),
            },
            None => (next_spec_seq_tx(tx, &meta.local)?, Vec::new()),
        };
        strand_local_tx(tx, meta, first, key, &before)?;
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
            let row: SpecRow = postcard::from_bytes(&v)?;
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
    let out = rewind_tx(tx, meta, staged, &live, &stranded, cutoff, None)?.0;
    let from = journal_from(tx, meta)?;
    compact_tx(tx, meta, from)?;
    Ok(out)
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
) -> Result<(), MetaError> {
    if tx.get(&meta.pending_replay, seq_key(key))?.is_none() {
        counter_add_tx(tx, &meta.local, KV_PENDING_REPLAY_COUNT, 1)?;
    }
    let row = QueuedReplay {
        rid,
        op,
        refused: Some(refusal),
        foreign: false,
        gen: 0,
        truncate_base: None,
    };
    tx.insert(
        &meta.pending_replay,
        seq_key(key),
        postcard::to_allocvec(&row)?,
    );
    Ok(())
}

/// The queued replay at `key`, if any: `(rid, op)`.
pub(crate) fn queued_at(
    r: &impl Readable,
    meta: &Meta,
    key: u64,
) -> Result<Option<(Rid, MutateOp)>, MetaError> {
    match r.get(&meta.pending_replay, seq_key(key))? {
        Some(v) => {
            let row: QueuedReplay = postcard::from_bytes(&v)?;
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
        let row: QueuedReplay = postcard::from_bytes(&v)?;
        out.push(StrandedOp {
            queue_seq: decode_seq(&k)?,
            rid: row.rid,
            op: row.op,
            refused: row.refused,
            foreign: row.foreign,
            gen: row.gen,
        });
    }
    Ok(out)
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
        self.install_shadow_from(rid, epoch, 0, op, records)
    }

    /// The OVH run's finding 4: an accepted forward this replica could
    /// not install (the holder evaluated it behind unshipped work on its
    /// keys) whose transaction the holder's pre-S3 stream has installed
    /// here since: adopt it (see `adopt_streamed_tx`) and report `true` —
    /// the op is answered now, not when its segment reaches S3. `false`:
    /// the stream has not carried it (or the log has: `completed`
    /// answers it then).
    pub fn adopt_streamed(&self, rid: Rid, op: &MutateOp) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.completed, rid.to_key())?.is_some() {
            return Ok(false);
        }
        if adopt_streamed_tx(&mut tx, self, rid, op)? {
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
        records: &[LogRecord],
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
        if adopt_streamed_tx(&mut tx, self, rid, op)? {
            tx.commit()?;
            return Ok(true);
        }
        if self.holder_epoch() > epoch {
            let key = next_spec_seq_tx(&mut tx, &self.local)?;
            enqueue_replay_tx(&mut tx, self, key, rid, op.clone(), &[])?;
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
        };
        self.install_speculative_tx(tx, kind, records)?;
        Ok(true)
    }

    /// Install the entry behind an `Exists` refusal ahead of the log, as a
    /// `Hint` entry: it retires once the applied position reaches `floor`
    /// (the holder's next ship position when it answered), and is rolled
    /// back if a segment from a later epoch than the answering holder's
    /// arrives first. `false`: not installed (see
    /// [`Self::install_hint_from`]).
    pub fn install_hint(
        &self,
        records: &[LogRecord],
        floor: u64,
        epoch: u64,
    ) -> Result<bool, MetaError> {
        self.install_hint_from(None, records, floor, epoch, 0)
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
    ///   hint left `f0` and `f1` on one inode).
    ///
    /// The caller then treats the refusal as uncovered (raises
    /// `observed`), which is always correct.
    pub fn install_hint_from(
        &self,
        rid: Option<Rid>,
        records: &[LogRecord],
        floor: u64,
        epoch: u64,
        gen: u64,
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
        self.install_speculative_tx(tx, SpecKind::Hint { floor, epoch, gen }, records)?;
        Ok(true)
    }

    /// Plan 30 §M9: install one of the holder's backup-acked journal
    /// transactions (rows `first..=last` of its tenure at `epoch`) ahead
    /// of the log, as a `Streamed` entry (see [`SpecKind::Streamed`]).
    pub fn install_streamed(
        &self,
        epoch: u64,
        first: u64,
        last: u64,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        // The mirror of `install_shadow`'s rule: this node's own op,
        // whose reply installed a shadow first, is not applied a second
        // time from the stream. The shadow *becomes* the streamed entry
        // (its effect is in place already): the segment then retires it
        // by skipping its rows like any streamed transaction's — which
        // is what keeps later streamed rows on top of it right (seed
        // 1402: a shadow retired the ordinary way had the segment's
        // unlink re-applied over a later streamed create) — and a
        // takeover strands it as this node's own op.
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
                    return Ok(());
                }
                if let Some((seq, own_epoch, op)) = shadow_row_for(&tx, self, *rid)? {
                    if let Some(v) = tx.get(&self.spec, seq_key(seq))? {
                        let mut row: SpecRow = postcard::from_bytes(&v)?;
                        row.kind = SpecKind::Streamed {
                            epoch,
                            first,
                            last,
                            own: Some((*rid, op)),
                        };
                        put_row(&mut tx, &self.spec, seq, &row)?;
                    }
                    let _ = own_epoch;
                    tx.insert(
                        &self.spec_live,
                        seq_key(seq),
                        postcard::to_allocvec(&LiveEntry::Streamed { epoch, last })?,
                    );
                    tx.commit()?;
                    return Ok(());
                }
            }
        }
        self.install_speculative_tx(
            tx,
            SpecKind::Streamed {
                epoch,
                first,
                last,
                own: None,
            },
            records,
        )
    }

    fn install_speculative_tx(
        &self,
        mut tx: SingleWriterWriteTx,
        kind: SpecKind,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        let staged = UsageTracker::staging();
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
    /// one. A `Recall` record strands what is left of that generation
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
        let completes: HashSet<Rid> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::Completed { rid } => Some(*rid),
                _ => None,
            })
            .collect();
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = strand_tx(&mut tx, self, &staged, |entry| {
            entry.stranded_by(epoch)
                && !matches!(entry, LiveEntry::Shadow { rid, .. } if completes.contains(rid))
        })?;
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
        let live: HashMap<u64, LiveEntry> = read_live(&tx, self)?.into_iter().collect();
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
            let LiveEntry::Streamed { epoch: e, last } = entry else {
                continue;
            };
            if *e != epoch || !rows.contains(last) {
                continue;
            }
            if let Some(v) = tx.get(&self.spec, seq_key(*spec_seq))? {
                if let SpecKind::Streamed { first, .. } = postcard::from_bytes::<SpecRow>(&v)?.kind
                {
                    confirmed.push((*spec_seq, first, *last));
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
        let first_local = live
            .iter()
            .filter(|(_, entry)| matches!(entry, LiveEntry::Local { .. }))
            .map(|(seq, _)| *seq)
            .min();
        let rolled_back = |spec_seq: u64| first_local.is_some_and(|cutoff| spec_seq >= cutoff);
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
        let skipped = if let Some(cutoff) = first_local {
            inserted_before_local = true;
            // Plan 30 §M11: a delegate holds `Local` rows (its unretired
            // stream transactions) older than its shadows and hints, so
            // the insert-and-redo path is its ordinary one. What this
            // segment retires — a shadow whose completion it carries, a
            // hint whose floor it reaches, a streamed row it confirms —
            // is retired speculation whose effect the segment carries:
            // redone after it, a hint would resurrect the entry the
            // segment's later records moved (epoch seed 64010).
            let own_spec: HashSet<u64> = own_txs.iter().filter_map(|(_, _, s)| *s).collect();
            let live_after: HashMap<u64, LiveEntry> = live
                .iter()
                .filter(|(spec_seq, entry)| match entry {
                    LiveEntry::Shadow { rid, .. } => !completes.contains(rid),
                    LiveEntry::Hint { floor, .. } => seq < *floor,
                    LiveEntry::Streamed { .. } => !confirmed.iter().any(|(s, _, _)| s == *spec_seq),
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
                Some(Inserted {
                    segment_seq: seq,
                    records,
                    pending,
                }),
            )?
            .1
        } else if !live.is_empty() {
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
        let mut retired = retire_tx(
            &mut tx,
            self,
            &completes,
            seq,
            Some(ShippedRows {
                epoch,
                through,
                rows,
            }),
        )?;
        for (spec_seq, _, _) in &confirmed {
            if let Some(v) = tx.get(&self.spec, seq_key(*spec_seq))? {
                let mut row: SpecRow = postcard::from_bytes(&v)?;
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
        // Plan 30 §M11: a generation the log recalls strands whatever of
        // it this node still holds unappended (past the cut: everything
        // appended is before the `Recall` record, hence retired above).
        let recalled: HashSet<u64> = records
            .iter()
            .filter_map(|rec| match rec {
                LogRecord::Recall { gen, .. } => Some(*gen),
                _ => None,
            })
            .collect();
        let mut stranded = stranded;
        if !recalled.is_empty() {
            let more = strand_tx(&mut tx, self, &staged, |entry| {
                entry.stranded_by_recall(&recalled)
            })?;
            stranded.shadows += more.shadows;
            stranded.hints += more.hints;
            stranded.locals += more.locals;
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
                    LogRecord::Refused { rid, errno } => (*rid, Some(*errno)),
                    _ => continue,
                };
                if tx.get(&self.completed, rid.to_key())?.is_some() {
                    continue;
                }
                let row = match refused {
                    Some(errno) => Meta::encode_refused_row(0, now_ms, errno),
                    None => Meta::encode_completed_row(0, now_ms),
                };
                tx.insert(&self.completed, rid.to_key(), row);
            }
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        kv_set_tx(&mut tx, &self.local, KV_APPLIED_SEQ, &seq.to_string());
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        if !own_seqs.is_empty() {
            if let Some(&upto) = own_seqs.iter().max() {
                let _ = self.prune_recent_shipped(upto);
            }
        }
        Ok(SegmentApplied {
            skipped,
            stranded,
            retired,
            inserted_before_local,
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
        let stranded = strand_tx(&mut tx, self, &staged, |entry| entry.stranded_by(epoch))?;
        if !stranded.any() {
            return Ok(stranded);
        }
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
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

    /// Every queued replay, oldest (original acceptance order) first. The
    /// replay drain asks every 250 ms; an empty queue is one counter read.
    pub fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError> {
        let r = self.db.read_tx();
        read_pending_replays(&r, self)
    }

    /// Queue `op` for replay by `rid` directly, after everything already
    /// queued. For callers that learn of a stranded op outside any
    /// stranding pass.
    pub fn queue_replay(&self, rid: Rid, op: &MutateOp) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let key = next_spec_seq_tx(&mut tx, &self.local)?;
        enqueue_replay_tx(&mut tx, self, key, rid, op.clone(), &[])?;
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
        let mut row: QueuedReplay = postcard::from_bytes(&v)?;
        row.refused = Some(refusal);
        tx.insert(
            &self.pending_replay,
            seq_key(queue_seq),
            postcard::to_allocvec(&row)?,
        );
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
        let mut row: QueuedReplay = postcard::from_bytes(&v)?;
        if row.foreign {
            return Ok(());
        }
        row.foreign = true;
        tx.insert(
            &self.pending_replay,
            seq_key(queue_seq),
            postcard::to_allocvec(&row)?,
        );
        tx.commit()?;
        Ok(())
    }

    /// Drop `queue_seq` from the replay queue once it is resolved:
    /// accepted, found already completed, or refused and materialized.
    pub fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.pending_replay, seq_key(queue_seq))?.is_some() {
            tx.remove(&self.pending_replay, seq_key(queue_seq));
            counter_add_tx(&mut tx, &self.local, KV_PENDING_REPLAY_COUNT, -1)?;
        }
        tx.commit()?;
        // Plan 30 §M6: a read waiting on this replay's keys may go.
        self.session.notify();
        Ok(())
    }
}
