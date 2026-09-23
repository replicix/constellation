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
    Shadow { rid: Rid, epoch: u64, op: MutateOp },
    /// The `Exists` early install: retires once the applied position
    /// reaches `floor`, strands if a segment above `epoch` arrives first.
    Hint { floor: u64, epoch: u64 },
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
    Shadow { rid: Rid, epoch: u64 },
    Hint { floor: u64, epoch: u64 },
    Local { first: u64, epoch: u64 },
}

impl LiveEntry {
    fn epoch(&self) -> u64 {
        match self {
            LiveEntry::Shadow { epoch, .. }
            | LiveEntry::Hint { epoch, .. }
            | LiveEntry::Local { epoch, .. } => *epoch,
        }
    }

    /// Whether a segment from, or a takeover at, `epoch` strands this
    /// entry: the epoch that produced it can no longer reach the log. A
    /// `Local` entry written without a lease (epoch 0) is not tied to any
    /// epoch and only ever retires by shipping.
    fn stranded_by(&self, epoch: u64) -> bool {
        match self {
            LiveEntry::Local { epoch: 0, .. } => false,
            entry => entry.epoch() < epoch,
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
                },
            ));
        }
    }
    Ok(out)
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
        SpecKind::Shadow { rid, epoch, .. } => Some(LiveEntry::Shadow {
            rid: *rid,
            epoch: *epoch,
        }),
        SpecKind::Hint { floor, epoch } => Some(LiveEntry::Hint {
            floor: *floor,
            epoch: *epoch,
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

fn enqueue_replay_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: u64,
    rid: Rid,
    op: MutateOp,
) -> Result<(), MetaError> {
    let row = QueuedReplay {
        rid,
        op,
        refused: None,
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

// --------------------------------------------------- stranded local work

/// The op a stranded transaction without an op of its own is replayed as
/// (`JournalTx::op` is `None`: a local manifest commit, a snapshot row, a
/// quota change, a clone). A lone manifest write becomes the ordinary
/// optimistic `SetManifest` (so an edit that lost to a newer one is
/// refused and materialized as a conflict copy, exactly like classify's
/// "edit-vs-edit"); anything else is re-applied through the replay path
/// as-is (`MutateOp::Records`).
fn derive_replay_op(records: &[LogRecord]) -> Option<MutateOp> {
    let effective: Vec<&LogRecord> = records
        .iter()
        .filter(|rec| !matches!(rec, LogRecord::Completed { .. } | LogRecord::Atime { .. }))
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

fn replay_rid_for(r: &impl Readable, meta: &Meta, first: u64) -> Result<Rid, MetaError> {
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
    if let Some(rid) = row.rid {
        tx.remove(&meta.completed, rid.to_key());
        meta.forget_recent(rid);
    }
    let records: Vec<LogRecord> = rows.into_iter().map(|(_, rec)| rec).collect();
    let rid = match row.rid {
        Some(rid) => rid,
        None => replay_rid_for(&*tx, meta, first)?,
    };
    let op = match row.op {
        Some(op) => Some(op),
        None => derive_replay_op(&records),
    };
    if let Some(op) = op {
        enqueue_replay_tx(tx, meta, key, rid, op)?;
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

    // 2. The inserted segment, in the rolled-back rows' place.
    let renumber = insert.is_some();
    let mut skipped = 0;
    if let Some(ins) = insert {
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
    let mut out = Stranded::default();
    for (seq, mut row) in rows {
        if stranded.contains(&seq) {
            tx.remove(&meta.spec, seq_key(seq));
            if matches!(row.kind, SpecKind::Shadow { .. } | SpecKind::Hint { .. }) {
                tx.remove(&meta.spec_live, seq_key(seq));
                counter_add_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, -1)?;
            }
            match row.kind {
                SpecKind::Shadow { rid, op, .. } => {
                    enqueue_replay_tx(tx, meta, row.origin, rid, op)?;
                    out.shadows += 1;
                }
                SpecKind::Local { first, .. } => {
                    strand_local_tx(tx, meta, first, row.origin)?;
                    out.locals += 1;
                }
                SpecKind::Hint { .. } | SpecKind::Foreign { .. } => out.hints += 1,
            }
            continue;
        }
        let durable = matches!(row.kind, SpecKind::Foreign { .. });
        if !durable && !live.contains_key(&seq) {
            // Retired speculation: the segment that retired it was
            // captured after it (it was outstanding then), and that
            // `Foreign` row's redo carries its effect. (A `Local` row that
            // shipped while older speculation was outstanding was turned
            // into a `Foreign` row at that moment — `retire_local_tx`.)
            tx.remove(&meta.spec, seq_key(seq));
            continue;
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
            continue;
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
    }
    Ok((out, skipped))
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

/// Retire every outstanding shadow whose rid `completes` names, and every
/// hint whose floor `applied_seq` has reached.
fn retire_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    completes: &HashSet<Rid>,
    applied_seq: u64,
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
pub(crate) fn retire_local_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    upto: u64,
    applied_seq: u64,
) -> Result<usize, MetaError> {
    // From the acked watermark (not the start of the keyspace): the cost
    // is what just shipped, not everything ever shipped.
    let from = journal_from(tx, meta)?;
    if upto < from {
        return Ok(0);
    }
    let mut shipped: Vec<(u64, local::JournalTxHead)> = Vec::new();
    for guard in tx.range(&meta.journal_tx, local::tx_key(from)..=local::tx_key(upto)) {
        let (k, v) = guard.into_inner()?;
        let row: local::JournalTxHead = postcard::from_bytes(&v)?;
        if row.last <= upto {
            shipped.push((local::decode_tx_key(&k)?, row));
        }
    }
    if shipped.is_empty() {
        return Ok(0);
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
    // A retired row only outlives compaction when an older requester
    // entry is still outstanding (`Local` rows retire in journal order):
    // only then does it need converting.
    let boundary = if counter_get(tx, &meta.local, KV_SPEC_LIVE_COUNT)? > 0 {
        oldest_outstanding(tx, meta, upto.saturating_add(1))?
    } else {
        None
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
    compact_tx(tx, meta, upto.saturating_add(1))?;
    Ok(retired)
}

/// The earliest before-image of every key touched by the rows from
/// `first` (the oldest outstanding `Local` row) on, for the publisher
/// (`Meta::publish_basis_at`). `None` if anything but `Local` rows sits in
/// that range — a state the insert-before rule should never leave, but in
/// which the overlay would not be the log prefix, so the publish defers.
pub(crate) fn local_before_images_from(
    r: &impl Readable,
    meta: &Meta,
    first: u64,
) -> Result<Option<LogPrefixView>, MetaError> {
    let mut view = LogPrefixView::default();
    for (_, row) in read_rows_from(r, &meta.spec, first)? {
        if !matches!(row.kind, SpecKind::Local { .. }) {
            return Ok(None);
        }
        for (key, before) in &row.before {
            view.note(key, before);
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
                if let SpecKind::Shadow { rid, op, .. } = row.kind {
                    enqueue_replay_tx(tx, meta, row.origin, rid, op)?;
                }
            }
            tx.remove(&meta.spec_live, seq_key(*seq));
        }
        if let LiveEntry::Hint { .. } = entry {
            tx.remove(&meta.spec_live, seq_key(*seq));
        }
    }
    counter_set_tx(tx, &meta.local, KV_SPEC_LIVE_COUNT, 0);
    for (first, row) in local::read_journal_txs(&*tx, meta)? {
        let key = match row.spec_seq {
            Some(seq) => match tx.get(&meta.spec, seq_key(seq))? {
                Some(v) => postcard::from_bytes::<SpecRow>(&v)?.origin,
                None => seq,
            },
            None => next_spec_seq_tx(tx, &meta.local)?,
        };
        strand_local_tx(tx, meta, first, key)?;
    }
    // Every journaled transaction is gone now: drop every row.
    let from = journal_from(tx, meta)?;
    compact_tx(tx, meta, from)?;
    Ok(())
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
        let mut tx = self.db.write_tx();
        if tx.get(&self.completed, rid.to_key())?.is_some() {
            return Ok(false);
        }
        if self.holder_epoch() > epoch {
            let key = next_spec_seq_tx(&mut tx, &self.local)?;
            enqueue_replay_tx(&mut tx, self, key, rid, op.clone())?;
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
        };
        self.install_speculative_tx(tx, kind, records)?;
        Ok(true)
    }

    /// Install the entry behind an `Exists` refusal ahead of the log, as a
    /// `Hint` entry: it retires once the applied position reaches `floor`
    /// (the holder's next ship position when it answered), and is rolled
    /// back if a segment from a later epoch than the answering holder's
    /// arrives first.
    pub fn install_hint(
        &self,
        records: &[LogRecord],
        floor: u64,
        epoch: u64,
    ) -> Result<(), MetaError> {
        let tx = self.db.write_tx();
        self.install_speculative_tx(tx, SpecKind::Hint { floor, epoch }, records)
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
        let live: HashMap<u64, LiveEntry> = read_live(&tx, self)?.into_iter().collect();
        let first_local = live
            .iter()
            .filter(|(_, entry)| matches!(entry, LiveEntry::Local { .. }))
            .map(|(seq, _)| *seq)
            .min();
        let mut inserted_before_local = false;
        let skipped = if let Some(cutoff) = first_local {
            inserted_before_local = true;
            rewind_tx(
                &mut tx,
                self,
                &staged,
                &live,
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
        let retired = retire_tx(&mut tx, self, &completes, seq)?;
        let from = journal_from(&tx, self)?;
        compact_tx(&mut tx, self, from)?;
        kv_set_tx(&mut tx, &self.local, KV_APPLIED_SEQ, &seq.to_string());
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
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
    pub fn has_outstanding_speculation(&self) -> bool {
        let r = self.db.read_tx();
        counter_get(&r, &self.local, KV_SPEC_LIVE_COUNT).is_ok_and(|n| n > 0)
    }

    /// Outstanding entries and queued replays, for `status` (polled as
    /// often as every few milliseconds): three counter reads under one
    /// snapshot, never a scan — plan 30 §M3b's hot-path rule.
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
        let mut out = Vec::new();
        if counter_get(&r, &self.local, KV_PENDING_REPLAY_COUNT)? == 0 {
            return Ok(out);
        }
        for guard in r.iter(&self.pending_replay) {
            let (k, v) = guard.into_inner()?;
            let row: QueuedReplay = postcard::from_bytes(&v)?;
            out.push(StrandedOp {
                queue_seq: decode_seq(&k)?,
                rid: row.rid,
                op: row.op,
                refused: row.refused,
            });
        }
        Ok(out)
    }

    /// Queue `op` for replay by `rid` directly, after everything already
    /// queued. For callers that learn of a stranded op outside any
    /// stranding pass.
    pub fn queue_replay(&self, rid: Rid, op: &MutateOp) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let key = next_spec_seq_tx(&mut tx, &self.local)?;
        enqueue_replay_tx(&mut tx, self, key, rid, op.clone())?;
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

    /// Drop `queue_seq` from the replay queue once it is resolved:
    /// accepted, found already completed, or refused and materialized.
    pub fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.pending_replay, seq_key(queue_seq))?.is_some() {
            tx.remove(&self.pending_replay, seq_key(queue_seq));
            counter_add_tx(&mut tx, &self.local, KV_PENDING_REPLAY_COUNT, -1)?;
        }
        tx.commit()?;
        Ok(())
    }
}
