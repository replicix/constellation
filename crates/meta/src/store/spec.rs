//! The node-local speculation log (plan 30 §M3a, the requester side).
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
//! nodes by the `Exists` hint (plan 30 §1.1, bug B).
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
//! - [`SpecKind::Foreign`] — a tailed log segment applied while an older
//!   entry was still outstanding. It is durable content, captured only so
//!   a rollback of that older entry can undo and then redo it.
//!
//! `spec_live` indexes the *outstanding* shadow and hint entries by the
//! same `spec_seq`. It is tiny (normally empty), so every hot-path
//! question — "is anything speculative?", "what does this segment
//! strand?" — is answered from it without decoding a row.
//!
//! `pending_replay` holds stranded ops awaiting replay by rid, keyed by
//! the stranded shadow's own `spec_seq` so the queue keeps original order.
//!
//! # Lifecycle
//!
//! - **Retirement.** A shadow retires when a tailed segment carries its
//!   `Completed { rid }`; a hint when the applied position reaches its
//!   floor. Retiring only drops the `spec_live` entry. The row stays while
//!   anything older is outstanding, because rolling that older entry back
//!   unwinds everything after it too.
//! - **Compaction.** Every row older than the oldest outstanding entry is
//!   deleted (all rows, once nothing is outstanding): nothing can ever roll
//!   back past that point again.
//! - **Stranding.** Applying a segment whose epoch is higher than an
//!   outstanding entry's strands it: the holder that accepted it has been
//!   superseded, and the fencing rule guarantees its epoch can no longer
//!   add to the log. A takeover strands everything below the new epoch
//!   the same way (`Meta::strand_below_epoch`).
//! - **Recovery** (`strand_tx`), in the stranding transaction: restore
//!   before-images in reverse `spec_seq` order down to the earliest
//!   stranded row (usage deltas too), then walk the same rows forward:
//!   stranded shadows move to `pending_replay`, stranded hints are
//!   dropped, retired speculation is dropped (its effect is carried by
//!   the later `Foreign` row that retired it), and everything else —
//!   foreign segments and still-outstanding speculation — is re-applied
//!   from its records under a fresh capture, so its row describes the
//!   state it now sits on. The caller then replays the queued ops by rid
//!   (`cli::recovery`).
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
//! speculative applications at all (`replay::ApplyCx::durable`), so it
//! needs no undo.

use crate::error::MetaError;
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::replay::{apply_batch_tx, apply_record, ApplyCx, TouchSet};
use crate::rid::Rid;
use crate::store::{
    adjust_usage_tx, kv_set_tx, misc, ns, Meta, UsageTracker, KV_APPLIED_SEQ, KV_NEXT_SPEC_SEQ,
};
use constellation_mtree::keys;
use constellation_mtree::record::{InodeRecord, Payload};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

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
    /// A tailed segment applied while older speculation was outstanding.
    Foreign { segment_seq: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SpecRow {
    kind: SpecKind,
    /// What a redo re-applies: the records actually handed to the apply
    /// path (a foreign record suppressed by this node's own pending
    /// journal is left out, so the redo cannot resurrect it).
    records: Vec<LogRecord>,
    /// `(key, value before this row first touched it)`, in touch order.
    before: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// `(bytes, files)` this row moved the usage counters by.
    usage: (i64, i64),
}

/// A `spec_live` value: the outstanding entry's retirement/stranding
/// parameters, without its (possibly large) records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LiveEntry {
    Shadow { rid: Rid, epoch: u64 },
    Hint { floor: u64, epoch: u64 },
}

impl LiveEntry {
    fn epoch(&self) -> u64 {
        match self {
            LiveEntry::Shadow { epoch, .. } | LiveEntry::Hint { epoch, .. } => *epoch,
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
}

impl Stranded {
    pub fn any(&self) -> bool {
        self.shadows + self.hints > 0
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
}

/// Status counts: outstanding speculative entries and queued replays.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpeculationCounts {
    pub outstanding: u64,
    pub pending_replay: u64,
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

    fn into_before(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
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

fn read_live(
    r: &impl Readable,
    live: &SingleWriterTxKeyspace,
) -> Result<Vec<(u64, LiveEntry)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.iter(live) {
        let (k, v) = guard.into_inner()?;
        out.push((decode_seq(&k)?, postcard::from_bytes(&v)?));
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
        SpecKind::Foreign { .. } => None,
    };
    if let Some(live) = live {
        tx.insert(&meta.spec_live, seq_key(seq), postcard::to_allocvec(&live)?);
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
        },
    )?;
    Ok(seq)
}

fn enqueue_replay_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    seq: u64,
    rid: Rid,
    op: MutateOp,
) -> Result<(), MetaError> {
    let row = QueuedReplay {
        rid,
        op,
        refused: None,
    };
    tx.insert(
        &meta.pending_replay,
        seq_key(seq),
        postcard::to_allocvec(&row)?,
    );
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

/// Recovery steps 1 and 2 (plan 30 §M3a) for every outstanding entry
/// `strands` selects, inside the caller's transaction: roll back down to
/// the earliest one, then redo everything after it that still stands.
/// Usage moves are staged into `staged`; the caller persists and drains
/// it. See the module doc's "Lifecycle".
fn strand_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    strands: impl Fn(&LiveEntry) -> bool,
) -> Result<Stranded, MetaError> {
    let live: HashMap<u64, LiveEntry> = read_live(tx, &meta.spec_live)?.into_iter().collect();
    let stranded: HashSet<u64> = live
        .iter()
        .filter(|(_, entry)| strands(entry))
        .map(|(seq, _)| *seq)
        .collect();
    let Some(&cutoff) = stranded.iter().min() else {
        return Ok(Stranded::default());
    };
    let rows = read_rows_from(tx, &meta.spec, cutoff)?;
    let plain = meta.dirty_for_ns();

    // 1. Roll back, newest first, so each key ends at the value it had
    //    before the earliest stranded row touched it.
    for (_, row) in rows.iter().rev() {
        for (key, before) in row.before.iter().rev() {
            restore_key_tx(tx, meta, plain, key, before.as_deref())?;
        }
        staged.adjust(-row.usage.0, -row.usage.1);
    }

    // 2. Redo, oldest first, what still stands.
    let mut out = Stranded::default();
    for (seq, mut row) in rows {
        if stranded.contains(&seq) {
            tx.remove(&meta.spec, seq_key(seq));
            tx.remove(&meta.spec_live, seq_key(seq));
            match row.kind {
                SpecKind::Shadow { rid, op, .. } => {
                    enqueue_replay_tx(tx, meta, seq, rid, op)?;
                    out.shadows += 1;
                }
                SpecKind::Hint { .. } | SpecKind::Foreign { .. } => out.hints += 1,
            }
            continue;
        }
        let durable = matches!(row.kind, SpecKind::Foreign { .. });
        if !durable && !live.contains_key(&seq) {
            // Retired speculation: the segment that retired it was
            // captured after it (it was outstanding then), and that
            // `Foreign` row's redo carries its effect.
            tx.remove(&meta.spec, seq_key(seq));
            continue;
        }
        let capture = Capture::new();
        let row_staged = UsageTracker::staging();
        {
            let cx = ApplyCx {
                dirty: meta.dirty_capturing(&capture),
                durable,
            };
            for rec in &row.records {
                if let Some(why) = apply_record(tx, meta, cx, rec, &row_staged)? {
                    tracing::debug!(why, ?rec, "speculation redo: record skipped");
                }
            }
        }
        row.usage = row_staged.raw_delta();
        staged.adjust(row.usage.0, row.usage.1);
        row.before = capture.into_before();
        put_row(tx, &meta.spec, seq, &row)?;
    }
    Ok(out)
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
    for (seq, entry) in read_live(tx, &meta.spec_live)? {
        let done = match entry {
            LiveEntry::Shadow { rid, .. } => completes.contains(&rid),
            LiveEntry::Hint { floor, .. } => applied_seq >= floor,
        };
        if done {
            tx.remove(&meta.spec_live, seq_key(seq));
            retired += 1;
        }
    }
    Ok(retired)
}

/// Delete every row older than the oldest outstanding entry — all of
/// them when nothing is outstanding.
fn compact_tx(tx: &mut SingleWriterWriteTx, meta: &Meta) -> Result<(), MetaError> {
    let boundary = match tx.iter(&meta.spec_live).next() {
        Some(guard) => Some(decode_seq(&guard.into_inner()?.0)?),
        None => None,
    };
    let doomed: Vec<Vec<u8>> = match boundary {
        Some(first_live) => tx
            .range(&meta.spec, Vec::new()..seq_key(first_live))
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?,
        None => tx
            .iter(&meta.spec)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?,
    };
    for key in doomed {
        tx.remove(&meta.spec, key);
    }
    Ok(())
}

/// The namespace is about to be replaced wholesale from a side replica
/// built from the shared log (reintegration): every before-image here is
/// meaningless against it. Queue the outstanding shadows for replay by
/// rid (their effects are not in the log-built namespace; M2 dedup makes
/// a replay of one that did land harmless) and drop everything else.
pub(crate) fn reset_for_rebuilt_ns_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
) -> Result<(), MetaError> {
    let live = read_live(tx, &meta.spec_live)?;
    for (seq, entry) in &live {
        if let LiveEntry::Shadow { .. } = entry {
            if let Some(v) = tx.get(&meta.spec, seq_key(*seq))? {
                let row: SpecRow = postcard::from_bytes(&v)?;
                if let SpecKind::Shadow { rid, op, .. } = row.kind {
                    enqueue_replay_tx(tx, meta, *seq, rid, op)?;
                }
            }
        }
        tx.remove(&meta.spec_live, seq_key(*seq));
    }
    let rows: Vec<Vec<u8>> = tx
        .iter(&meta.spec)
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for key in rows {
        tx.remove(&meta.spec, key);
    }
    Ok(())
}

// ------------------------------------------------------------ Meta API

impl Meta {
    /// Install a forwarded op's accepted records ahead of the log, as a
    /// `Shadow` entry (`forward::apply_accepted`). Returns `false`, and
    /// changes nothing, when this replica's applied log already carries
    /// `rid`'s completion: the reply lost a race with this node's own
    /// tail, so the effect is already part of the log prefix, and
    /// re-applying it on top of later records could move the replica
    /// backwards (and would leave a shadow that nothing ever retires).
    pub fn install_shadow(
        &self,
        rid: Rid,
        epoch: u64,
        op: &MutateOp,
        records: &[LogRecord],
    ) -> Result<bool, MetaError> {
        let tx = self.db.write_tx();
        if tx.get(&self.completed, rid.to_key())?.is_some() {
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
    /// `epoch`), in one transaction with every plan 30 §M3a rule around
    /// it:
    ///
    /// 1. strand every outstanding entry below `epoch` (rollback, redo,
    ///    shadows queued for replay) — except a shadow this very segment
    ///    completes, which simply retires below;
    /// 2. apply the records (skipping ones that collide with `pending`,
    ///    this node's own unshipped journal), captured as a `Foreign` row
    ///    if anything is still outstanding;
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
            entry.epoch() < epoch
                && !matches!(entry, LiveEntry::Shadow { rid, .. } if completes.contains(rid))
        })?;
        let speculating = tx.iter(&self.spec_live).next().is_some();
        let skipped = if speculating {
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
        compact_tx(&mut tx, self)?;
        kv_set_tx(&mut tx, &self.local, KV_APPLIED_SEQ, &seq.to_string());
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        Ok(SegmentApplied {
            skipped,
            stranded,
            retired,
        })
    }

    /// The takeover gate's rollback (plan 30 §M3a): strand every
    /// outstanding entry accepted below `epoch` — the epoch this node is
    /// about to hold. The caller has tailed to head, so anything such an
    /// entry's holder shipped has already retired it; what is left can
    /// never reach the log any more. Shadows are queued for replay, which
    /// the caller runs locally before serving anything else.
    pub fn strand_below_epoch(&self, epoch: u64) -> Result<Stranded, MetaError> {
        let mut tx = self.db.write_tx();
        let staged = UsageTracker::staging();
        let stranded = strand_tx(&mut tx, self, &staged, |entry| entry.epoch() < epoch)?;
        if !stranded.any() {
            return Ok(stranded);
        }
        compact_tx(&mut tx, self)?;
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        Ok(stranded)
    }

    /// Whether any shadow or hint is outstanding: this replica is then not
    /// a log prefix, and must not be published (plan 30 §M3a's publish
    /// rule). Queued replays do not count — their effects are already
    /// rolled back.
    pub fn has_outstanding_speculation(&self) -> bool {
        self.spec_live.first_key_value().is_some()
    }

    /// Outstanding entries and queued replays, for `status`.
    pub fn speculation_counts(&self) -> Result<SpeculationCounts, MetaError> {
        let r = self.db.read_tx();
        let mut counts = SpeculationCounts::default();
        for guard in r.iter(&self.spec_live) {
            guard.into_inner()?;
            counts.outstanding += 1;
        }
        for guard in r.iter(&self.pending_replay) {
            guard.into_inner()?;
            counts.pending_replay += 1;
        }
        Ok(counts)
    }

    /// Every queued replay, oldest (original acceptance order) first.
    pub fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
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
        tx.remove(&self.pending_replay, seq_key(queue_seq));
        tx.commit()?;
        Ok(())
    }
}
