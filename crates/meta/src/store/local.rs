//! A node's own journaled writes as speculation (plan 30 §M3b, the
//! holder side of the speculation log).
//!
//! # Why
//!
//! Every write this node journals — a holder executing its own FUSE op, a
//! peer's forwarded op, a replay after a takeover, a conflict copy — is in
//! `ns` before it is in the durable log. M3a made requester shadows
//! explicit; this module does the same for the holder's unshipped
//! journal. Two things need it:
//!
//! - **Publishing** (`cli::mtree_publish`). A commit claims to be the
//!   log-prefix state at its `applied` position. A holder's replica is that
//!   state plus its unshipped journal, so before M3b a holder either
//!   published speculation (bug B's holder side) or had to wait for an
//!   empty journal, which a busy holder rarely has. With the journal's
//!   before-images captured, the publisher substitutes each touched key's
//!   *earliest* before-image — exactly its value at the last shipped
//!   position ([`Meta::publish_basis_at`]).
//! - **Deposition.** A holder that loses its lease with an unshipped
//!   journal used to keep the journal's effects in its replica until an
//!   operator ran reintegration, which classified every record against a
//!   side replica. Now the rows are rolled back from their before-images
//!   and their ops replayed by rid through whoever holds the lease
//!   (`store::spec`'s stranding, `cli::recovery`), exactly like a stranded
//!   requester shadow.
//!
//! # Mechanism
//!
//! Every journaled write transaction brackets its `ns` writes with
//! [`Meta::begin_local`] and [`Meta::finish_local`]. `begin_local` notes
//! where the transaction's journal rows will start and, when capture
//! applies, opens a [`spec::Capture`] that every `ns` write in the
//! transaction records its before-image into (the same `ns::Dirty` funnel
//! M3a added). `finish_local` then writes, in the same transaction:
//!
//! - a `journal_tx` row keyed by the first journal seq: the last seq (so a
//!   shipped segment never splits a transaction — an op's records and its
//!   `Completed { rid }` always ship together), the epoch it ran under,
//!   the op and rid (`mutate::execute` hands them over through
//!   `journal::PendingLocalOp`), and the `spec_seq` of its before-images;
//! - with capture, a `spec` row of kind [`spec::SpecKind::Local`] holding
//!   the before-images and usage delta. Its records are not copied: they
//!   are the journal rows themselves.
//!
//! A `Local` entry retires when its journal rows ship
//! (`Meta::ack_journal_rows_at`), and strands when a segment from a later
//! epoch arrives or the node learns it was deposed.
//!
//! # When capture applies
//!
//! Capture is on (`Meta::holder_capture`, `CONSTELLATION_HOLDER_CAPTURE`)
//! and either this node holds the lease (`Meta::holder_epoch` is set) or
//! the speculation log is not empty. The second arm keeps M3a's invariant
//! — while any `spec` row exists, every `ns` write is captured, so a
//! rollback can never wipe an uncaptured write — for the rare local write
//! a non-holder makes (an offline designation's `Proceed`). Such a row
//! carries epoch 0 and never strands by epoch; it only retires when it
//! ships. With capture off, the `journal_tx` row is still written (the
//! transaction boundary and the op for a replay), and the fallback applies:
//! a holder with an unshipped journal defers its publishes, and a deposed
//! holder rebuilds its namespace from the head commit.
//!
//! # Cost
//!
//! The hot path adds, per journaled transaction, with capture on: one
//! point read of the journal counter at each end of the bracket, one
//! point read of each touched key's before-image (a create touches about
//! five), and three writes — the `spec` row, the `journal_tx` row and the
//! `spec_seq` counter — plus the `local_spec_count` counter's read and
//! write. The usage delta comes from a thread-local note
//! (`store::usage_note_begin`), not from reading the persisted counters.
//! With capture off: the two journal-counter reads, the `journal_tx` row,
//! and the `uncaptured_tx_count` counter. Shipping deletes the rows again
//! at a cost proportional to what shipped: the retirement and the `spec`
//! compaction range from the acked watermark and the `spec` floor, never
//! from the start of a keyspace, whose retired history the LSM keeps as
//! tombstones until compaction.
//!
//! Nothing on a hot path counts or scans: `status`
//! (`Meta::speculation_counts`), the tailer's stranding and suppression
//! checks, the replay drain's poll and the publish rule read the counters
//! in `store::KV_SPEC_LIVE_COUNT` and siblings, maintained in the same
//! transaction as every change they count (plan 30 §M3b's second coder
//! round; the first version scanned `journal_tx` on every `status` call
//! and every tailed segment, and compacted from the start of `spec` on
//! every ship, which starved a busy holder's ship loop —
//! `holder-ships-under-forward-load`). The `local.rs` unit tests pin this
//! by poisoning the regions those paths must not read.

use crate::error::MetaError;
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::replay::{apply_batch_tx, ApplyCx, TouchSet};
use crate::rid::Rid;
use crate::store::{
    adjust_usage_tx, counter_add_tx, counter_get, journal, kv_get_tx, kv_set_tx, ns, spec,
    usage_note_begin, usage_note_take, Meta, UsageTracker, KV_LOCAL_SPEC_COUNT, KV_SPEC_LIVE_COUNT,
    KV_UNCAPTURED_TX_COUNT,
};
use crate::TreeInode;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use constellation_mtree::record::{InodeRecord, Payload};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// A `journal_tx` value: one journaled transaction still in `journal`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct JournalTx {
    /// The transaction's last journal seq (the key is its first).
    pub last: u64,
    /// The `spec` row with its before-images, when it was captured.
    pub spec_seq: Option<u64>,
    /// The lease epoch it executed under; 0 when this node held none.
    pub epoch: u64,
    /// Plan 30 §M11: the delegation stream this transaction belongs to
    /// (`gen` 0: none) and its index in it — executed here as the
    /// delegate, or appended here as the root from a delegate's stream.
    /// A delegate's rows retire when a segment carries their origin and
    /// strand when the log recalls their generation; a root's rows of a
    /// delegate origin ship with it in the segment envelope.
    pub gen: u64,
    pub idx: u64,
    /// The rid `mutate::execute` ran it under, if any.
    pub rid: Option<Rid>,
    /// The op `mutate::execute` ran, if the transaction came from one. A
    /// replay after a deposition re-executes this; a transaction without
    /// one (a local manifest commit, a snapshot row) is replayed from its
    /// records instead (`spec::derive_replay_op`).
    pub op: Option<MutateOp>,
    /// Plan 30 §M11: the position the op's requester had observed when
    /// it was submitted (its causal dependencies), recorded as the
    /// record's `deps`.
    pub deps: crate::session::Position,
}

/// The leading fields of a [`JournalTx`], decoded without the (possibly
/// large) op. Postcard encodes fields in declaration order and does not
/// require the whole input to be consumed, so this reads a prefix of the
/// same bytes. Every hot path that only needs to know where a transaction
/// ends and whether it is captured — shipping, retirement, compaction,
/// stranding checks, `status` — uses this.
#[derive(Clone, Copy, Debug, Deserialize)]
pub(crate) struct JournalTxHead {
    pub last: u64,
    pub spec_seq: Option<u64>,
    pub epoch: u64,
    pub gen: u64,
    pub idx: u64,
}

pub(crate) fn tx_key(first: u64) -> Vec<u8> {
    first.to_be_bytes().to_vec()
}

pub(crate) fn decode_tx_key(k: &[u8]) -> Result<u64, MetaError> {
    let bytes: [u8; 8] = k
        .try_into()
        .map_err(|_| MetaError::Invalid("journal_tx key".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn get_journal_tx(
    r: &impl Readable,
    meta: &Meta,
    first: u64,
) -> Result<Option<JournalTx>, MetaError> {
    match r.get(&meta.journal_tx, tx_key(first))? {
        Some(v) => Ok(Some(postcard::from_bytes(&v)?)),
        None => Ok(None),
    }
}

pub(crate) fn get_journal_tx_head(
    r: &impl Readable,
    meta: &Meta,
    first: u64,
) -> Result<Option<JournalTxHead>, MetaError> {
    match r.get(&meta.journal_tx, tx_key(first))? {
        Some(v) => Ok(Some(postcard::from_bytes(&v)?)),
        None => Ok(None),
    }
}

pub(crate) fn put_journal_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    first: u64,
    row: &JournalTx,
) -> Result<(), MetaError> {
    tx.insert(&meta.journal_tx, tx_key(first), postcard::to_allocvec(row)?);
    Ok(())
}

/// Every `journal_tx` row's head, oldest first. Starts past the acked
/// watermark (the shipped history's tombstones), so the cost is the
/// unshipped backlog's; callers that only need a count read the counters
/// instead (`store::KV_LOCAL_SPEC_COUNT`/`KV_UNCAPTURED_TX_COUNT`).
pub(crate) fn read_journal_tx_heads(
    r: &impl Readable,
    meta: &Meta,
) -> Result<Vec<(u64, JournalTxHead)>, MetaError> {
    let from = journal::acked_watermark(r, &meta.local)?.saturating_add(1);
    let mut out = Vec::new();
    for guard in r.range(&meta.journal_tx, tx_key(from)..) {
        let (k, v) = guard.into_inner()?;
        out.push((decode_tx_key(&k)?, postcard::from_bytes(&v)?));
    }
    Ok(out)
}

/// Every `journal_tx` row, oldest first (see [`read_journal_tx_heads`]).
pub(crate) fn read_journal_txs(
    r: &impl Readable,
    meta: &Meta,
) -> Result<Vec<(u64, JournalTx)>, MetaError> {
    let from = journal::acked_watermark(r, &meta.local)?.saturating_add(1);
    let mut out = Vec::new();
    for guard in r.range(&meta.journal_tx, tx_key(from)..) {
        let (k, v) = guard.into_inner()?;
        out.push((decode_tx_key(&k)?, postcard::from_bytes(&v)?));
    }
    Ok(out)
}

/// The `spec_seq` of the oldest outstanding captured transaction at or
/// after journal seq `from`, if any: the first captured `journal_tx` row
/// (captured rows' `spec_seq`s rise with journal order, and a renumbering
/// keeps that). O(1) in steady state — a holder's journal is all captured
/// — and never past the first captured row.
pub(crate) fn first_captured_from(
    r: &impl Readable,
    meta: &Meta,
    from: u64,
) -> Result<Option<u64>, MetaError> {
    for guard in r.range(&meta.journal_tx, tx_key(from)..) {
        let (_, v) = guard.into_inner()?;
        let row: JournalTxHead = postcard::from_bytes(&v)?;
        if let Some(seq) = row.spec_seq {
            return Ok(Some(seq));
        }
    }
    Ok(None)
}

/// The journal rows `first..=last`, in order.
pub(crate) fn journal_records(
    r: &impl Readable,
    meta: &Meta,
    first: u64,
    last: u64,
) -> Result<Vec<(u64, LogRecord)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.range(&meta.journal_ks, tx_key(first)..=tx_key(last)) {
        let (k, v) = guard.into_inner()?;
        out.push((decode_tx_key(&k)?, LogRecord::from_postcard(&v)?));
    }
    Ok(out)
}

/// One journaled transaction in progress (see the module doc). Built by
/// [`Meta::begin_local`] right after `write_tx()`, consumed by
/// [`Meta::finish_local`] right before `commit()`.
pub(crate) struct LocalTx {
    /// The journal seq the transaction's first appended row gets.
    start: u64,
    epoch: u64,
    capture: Option<spec::Capture>,
}

impl LocalTx {
    /// The journal seq this transaction's first row gets (its
    /// `journal_tx` key).
    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    /// The `ns::Dirty` every `ns` write of this transaction goes through:
    /// plain dirty-tracking, plus before-image capture when it applies.
    pub(crate) fn dirty<'a>(&'a self, meta: &'a Meta) -> ns::Dirty<'a> {
        match &self.capture {
            Some(capture) => meta.dirty_capturing(capture),
            None => meta.dirty_for_ns(),
        }
    }
}

impl Meta {
    /// Open the local-write bracket for the transaction `tx` just started.
    pub(crate) fn begin_local(&self, tx: &SingleWriterWriteTx) -> Result<LocalTx, MetaError> {
        let start = journal::peek_next_seq(tx, &self.local)?;
        let epoch = self.holder_epoch.load(Ordering::SeqCst);
        // A holder always captures; a non-holder only while speculation
        // exists — answered from two counters, not a scan of `spec`.
        // Plan 30 §M11: a delegate's transactions are speculation like a
        // holder's (retired by the log, stranded by a recall), so they
        // are captured too.
        let capturing = self.holder_capture()
            && (epoch != 0
                || journal::PendingDelegate::peek().is_some()
                || counter_get(tx, &self.local, KV_SPEC_LIVE_COUNT)? > 0
                || counter_get(tx, &self.local, KV_LOCAL_SPEC_COUNT)? > 0);
        let capture = if capturing {
            usage_note_begin();
            Some(spec::Capture::new())
        } else {
            None
        };
        Ok(LocalTx {
            start,
            epoch,
            capture,
        })
    }

    /// Close the bracket: record the transaction's `journal_tx` row and,
    /// when captured, its `Local` speculation row. A transaction that
    /// journaled nothing (an early `Ok` that changed nothing) records
    /// nothing.
    pub(crate) fn finish_local(
        &self,
        tx: &mut SingleWriterWriteTx,
        local: LocalTx,
    ) -> Result<(), MetaError> {
        let end = journal::peek_next_seq(&*tx, &self.local)?;
        if end <= local.start {
            return Ok(());
        }
        let (rid, op) = match journal::PendingLocalOp::take() {
            Some((rid, op)) => (rid, Some(op)),
            None => (None, None),
        };
        let spec_seq = match local.capture {
            Some(capture) => {
                let usage = usage_note_take();
                counter_add_tx(tx, &self.local, KV_LOCAL_SPEC_COUNT, 1)?;
                Some(spec::record_local_tx(
                    tx,
                    self,
                    local.start,
                    local.epoch,
                    capture,
                    usage,
                )?)
            }
            None => {
                counter_add_tx(tx, &self.local, KV_UNCAPTURED_TX_COUNT, 1)?;
                None
            }
        };
        // Plan 30 §M11: the transaction's delegation origin. A delegate
        // takes the generation's next index (a persisted counter, so a
        // restart continues the stream); the root records the index the
        // delegate assigned.
        let (gen, idx, deps) = match journal::PendingDelegate::take() {
            Some(d) => {
                let idx = match d.idx {
                    Some(i) => i,
                    None => {
                        let key = deleg_idx_key(d.gen);
                        let next = kv_get_tx(tx, &self.local, &key)?
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(0)
                            + 1;
                        kv_set_tx(tx, &self.local, &key, &next.to_string());
                        next
                    }
                };
                (d.gen, idx, d.deps)
            }
            None => (0, 0, crate::session::Position::ZERO),
        };
        put_journal_tx(
            tx,
            self,
            local.start,
            &JournalTx {
                last: end - 1,
                spec_seq,
                epoch: local.epoch,
                gen,
                idx,
                rid,
                op,
                deps,
            },
        )
    }

    /// Plan 30 §M11: execute `op` here as the delegate of generation
    /// `gen` (the caller checked ownership and the grant): journaled with
    /// the generation's next stream index and the requester's `deps`.
    /// Returns the records and the index.
    pub fn delegate_execute(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        gen: u64,
        deps: crate::session::Position,
    ) -> Result<(Vec<LogRecord>, u64), MetaError> {
        let _d = journal::PendingDelegate::set(gen, None, deps);
        let records = crate::mutate::execute(self, op, rid)?;
        let idx = self.delegate_idx(gen)?;
        Ok((records, idx))
    }

    /// The highest stream index this node assigned under `gen` (0: none).
    pub fn delegate_idx(&self, gen: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_idx_key(gen))?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0))
    }

    /// Phase 2b: the highest index of `gen` in the log as this replica
    /// holds it (see [`deleg_log_idx_key`]).
    pub fn log_stream_idx(&self, gen: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_log_idx_key(gen))?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0))
    }

    fn note_log_idx(&self, gen: u64, idx: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        bump_log_idx_tx(&mut tx, &self.local, gen, idx)?;
        tx.commit()?;
        Ok(())
    }

    /// Plan 30 §M11: the root appends one of a delegate's transactions
    /// (validated by the delegate; no re-validation) into its own
    /// journal with the delegate's origin and `deps`, completing `rid`
    /// once. A rid the log or this journal already completed is skipped
    /// (`Ok(false)`).
    pub fn apply_delegate_tx(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        gen: u64,
        idx: u64,
        deps: crate::session::Position,
    ) -> Result<bool, MetaError> {
        // A delegate's refusal (see `delegate_refusal`) is a `Refused`
        // row of its own: journaled here the same way, under the
        // stream's origin.
        if let [LogRecord::Refused { rid: r, errno }] = records {
            if self.completed_position(*r)?.is_some() {
                return Ok(false);
            }
            let _d = journal::PendingDelegate::set(gen, Some(idx), deps);
            self.journal_refusal(*r, *errno)?;
            self.note_log_idx(gen, idx)?;
            return Ok(true);
        }
        if let Some(rid) = rid {
            if self.completed_position(rid)?.is_some() {
                self.note_log_idx(gen, idx)?;
                return Ok(false);
            }
        }
        let _d = journal::PendingDelegate::set(gen, Some(idx), deps);
        self.apply_records_journaled_completing(records, rid)?;
        self.note_log_idx(gen, idx)?;
        Ok(true)
    }

    /// Plan 30 §M11: a delegate's refusal of `rid` (plan 30 §M9's
    /// journaled `Refused` row), as the next transaction of stream
    /// `gen` — the root appends it like any other, so a retry by rid
    /// anywhere finds the same errno. Returns the stream index.
    pub fn delegate_refusal(
        &self,
        rid: Rid,
        errno: i32,
        gen: u64,
        deps: crate::session::Position,
    ) -> Result<u64, MetaError> {
        let _d = journal::PendingDelegate::set(gen, None, deps);
        self.journal_refusal(rid, errno)?;
        self.delegate_idx(gen)
    }

    /// Plan 30 §M11: this node's unretired transactions of delegation
    /// stream `gen` from index `from_idx` on, oldest first, at most
    /// `max_rows` journal rows (whole transactions).
    pub fn delegate_txs_from(
        &self,
        gen: u64,
        from_idx: u64,
        max_rows: usize,
    ) -> Result<Vec<DelegateTx>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        let mut rows = 0usize;
        for (first, row) in read_journal_txs(&r, self)? {
            if row.gen != gen || row.idx < from_idx {
                continue;
            }
            let records: Vec<LogRecord> = journal_records(&r, self, first, row.last)?
                .into_iter()
                .map(|(_, rec)| rec)
                .collect();
            rows += records.len();
            out.push(DelegateTx {
                idx: row.idx,
                rid: row.rid,
                records,
                deps: row.deps,
            });
            if rows >= max_rows {
                break;
            }
        }
        out.sort_by_key(|t| t.idx);
        Ok(out)
    }

    /// Plan 30 §M11: the delegation origin of each journal row in
    /// `seqs` (`(0, 0)` for this node's own rows), for the segment
    /// envelope.
    pub fn journal_origins(&self, seqs: &[u64]) -> Result<Vec<(u64, u64)>, MetaError> {
        let r = self.db.read_tx();
        let heads = read_journal_tx_heads(&r, self)?;
        Ok(seqs
            .iter()
            .map(|seq| {
                heads
                    .iter()
                    .find(|(first, h)| *first <= *seq && *seq <= h.last)
                    .map(|(_, h)| (h.gen, h.idx))
                    .unwrap_or((0, 0))
            })
            .collect())
    }

    /// Plan 30 §M11 phase 2b: whether transaction `(gen, idx)` of this
    /// delegate's stream is still in the journal (not yet carried by a
    /// segment the replica applied) — `ack=s3`'s wait.
    pub fn delegate_tx_pending(&self, gen: u64, idx: u64) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        Ok(read_journal_tx_heads(&r, self)?
            .iter()
            .any(|(_, h)| h.gen == gen && h.idx == idx))
    }

    /// Plan 30 §M11: whether the unshipped journal holds any transaction
    /// that is not a delegate-stream row (only those take the lease path
    /// before a forward, M5 round 3).
    pub fn journal_has_undelegated(&self) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        Ok(read_journal_tx_heads(&r, self)?
            .iter()
            .any(|(_, h)| h.gen == 0))
    }

    /// Plan 30 §M11: the journal seqs of this node's transactions whose
    /// origins are in `origins` (a segment carried them: they retire).
    pub(crate) fn journal_seqs_of_origins(
        &self,
        r: &impl Readable,
        origins: &[(u64, u64)],
    ) -> Result<Vec<(u64, u64, Option<u64>)>, MetaError> {
        if origins.iter().all(|(g, _)| *g == 0) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (first, h) in read_journal_tx_heads(r, self)? {
            if h.gen != 0 && origins.contains(&(h.gen, h.idx)) {
                out.push((first, h.last, h.spec_seq));
            }
        }
        Ok(out)
    }

    /// The largest prefix of `batch` (journal rows in order, starting at a
    /// transaction boundary) that is at most `want` rows long and ends on a
    /// transaction boundary — or, when even the first transaction is longer
    /// than `want`, that whole first transaction (a byte cap bounds
    /// batching; it cannot be allowed to wedge the stream). Rows without a
    /// `journal_tx` row count as one-row transactions.
    pub fn whole_tx_prefix(
        &self,
        batch: &[(u64, LogRecord)],
        want: usize,
    ) -> Result<usize, MetaError> {
        let want = want.min(batch.len());
        if want == 0 || want == batch.len() {
            return Ok(want);
        }
        let r = self.db.read_tx();
        let last_seq = batch[want - 1].0;
        // The transaction containing the cut row, if any: the one with the
        // greatest first seq at or below it.
        let owner = match r.range(&self.journal_tx, ..=tx_key(last_seq)).next_back() {
            Some(guard) => {
                let (k, v) = guard.into_inner()?;
                let row: JournalTxHead = postcard::from_bytes(&v)?;
                Some((decode_tx_key(&k)?, row.last))
            }
            None => None,
        };
        match owner {
            Some((first, last)) if first <= last_seq && last > last_seq => {
                // Mid-transaction: cut before it, or — when it is the first
                // one — after it.
                let before = batch.iter().take_while(|(seq, _)| *seq < first).count();
                if before > 0 {
                    Ok(before)
                } else {
                    Ok(batch.iter().take_while(|(seq, _)| *seq <= last).count())
                }
            }
            _ => Ok(want),
        }
    }

    /// Up to `max` journal rows from the head, extended to the end of the
    /// transaction the last one belongs to (so a caller that ships the
    /// whole batch ships whole transactions).
    pub(crate) fn take_journal_whole_txs(
        &self,
        max: usize,
    ) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let r = self.db.read_tx();
        let mut batch = journal::take(&r, &self.journal_ks, &self.local, max)?;
        let Some(&(last_seq, _)) = batch.last() else {
            return Ok(batch);
        };
        if let Some(guard) = r.range(&self.journal_tx, ..=tx_key(last_seq)).next_back() {
            let (k, v) = guard.into_inner()?;
            let row: JournalTxHead = postcard::from_bytes(&v)?;
            if decode_tx_key(&k)? <= last_seq && row.last > last_seq {
                batch.extend(journal_records(&r, self, last_seq + 1, row.last)?);
            }
        }
        Ok(batch)
    }

    /// The dentries and inos this node's *uncaptured* unshipped journal
    /// touches: what a tailed foreign record that collides with local work
    /// is suppressed against (`replay::TouchSet`, the pre-M3b leaseless
    /// convergence rule). Captured transactions are not included: a tailed
    /// segment is instead inserted *before* them, exactly
    /// (`Meta::apply_segment`), so nothing needs suppressing.
    ///
    /// Plan 30 §M3b cost: one counter read when nothing uncaptured is
    /// journaled (every holder with capture on, every follower), so a
    /// tailed segment never walks the journal.
    pub fn pending_touches(&self) -> Result<TouchSet, MetaError> {
        let r = self.db.read_tx();
        if counter_get(&r, &self.local, KV_UNCAPTURED_TX_COUNT)? == 0 {
            return Ok(TouchSet::default());
        }
        let captured: Vec<(u64, u64)> = read_journal_tx_heads(&r, self)?
            .into_iter()
            .filter(|(_, row)| row.spec_seq.is_some())
            .map(|(first, row)| (first, row.last))
            .collect();
        let rows = journal::take(&r, &self.journal_ks, &self.local, usize::MAX)?;
        Ok(TouchSet::from_records(
            rows.iter()
                .filter(|(seq, _)| !captured.iter().any(|(f, l)| f <= seq && seq <= l))
                .map(|(_, rec)| rec),
        ))
    }

    /// How many of this node's journaled transactions are outstanding
    /// `Local` speculation (captured and not yet shipped): one counter
    /// read.
    pub fn local_speculation_count(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        counter_get(&r, &self.local, KV_LOCAL_SPEC_COUNT)
    }

    /// Plan 30 §M3b: execute a stranded transaction that had no op of its
    /// own (`MutateOp::Records`): apply its records through the replay path
    /// — the same skip-on-conflict rules every tailing replica uses — and
    /// journal what applied, in one transaction.
    pub fn apply_records_journaled(&self, records: &[LogRecord]) -> Result<(), MetaError> {
        self.apply_records_journaled_completing(records, None)
    }

    /// [`Self::apply_records_journaled`] that also journals `Completed {
    /// rid }` for `rid` (and records the completion), as `execute` does
    /// for an op: plan 30 §M9's takeover re-applies the predecessor's
    /// backup tail this way, so the log carries every acknowledged op's
    /// completion exactly once and every requester's dedup finds it.
    pub fn apply_records_journaled_completing(
        &self,
        records: &[LogRecord],
        rid: Option<crate::Rid>,
    ) -> Result<(), MetaError> {
        self.apply_records_journaled_inner(records, rid, false)
    }

    /// Plan 30 §M9 × §M4: [`Self::apply_records_journaled_completing`]
    /// for a transaction adopted from the predecessor's backup tail.
    /// Every chunk an adopted `WriteManifest` names is enrolled as a
    /// pending upload for its inode in the same transaction, because this
    /// node cannot know whether the predecessor uploaded it: a write-back
    /// close acknowledges before its chunks reach S3, and a holder's own
    /// manifest commit is backed up (and so adopted) as soon as it is
    /// journaled. The ordinary machinery then decides per chunk — the
    /// upload pass acknowledges one that is durable in S3 (a HEAD), and
    /// records one that is neither in S3 nor in the local cache as
    /// unrecoverable, which holds the manifest's transaction and its
    /// dependents back (M4: `status` shows them held, `repair drop-held`
    /// turns them into a conflict copy); until a chunk is settled the
    /// ship planner defers the transaction (M7). So the log never names a
    /// chunk the bucket lacks, and neither does a published commit (held
    /// work is `Local` speculation the publisher substitutes). A spilled
    /// manifest enrolls its list blob and is marked for expansion (see
    /// [`Self::adopted_spills`]): its chunk list is only known once the
    /// blob can be read.
    pub fn apply_adopted_records(
        &self,
        records: &[LogRecord],
        rid: Option<crate::Rid>,
    ) -> Result<(), MetaError> {
        self.apply_records_journaled_inner(records, rid, true)
    }

    fn apply_records_journaled_inner(
        &self,
        records: &[LogRecord],
        rid: Option<crate::Rid>,
        adopted: bool,
    ) -> Result<(), MetaError> {
        // With a rid, the transaction is that op's (plan 30 §M9): a
        // stranding replays it *by rid* — where `completed` dedups —
        // never as anonymous records under a derived rid, which would
        // re-apply a stale effect out of order.
        let _op = rid.map(|rid| {
            journal::PendingLocalOp::set(
                Some(rid),
                &crate::mutate::MutateOp::Records {
                    records: records.to_vec(),
                },
            )
        });
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let staged = UsageTracker::staging();
        let (_, applied) = {
            let cx = ApplyCx {
                dirty: local.dirty(self),
                durable: true,
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
        let rows: Vec<&LogRecord> = applied
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
        let n = rows.len();
        for (i, rec) in rows.into_iter().enumerate() {
            // With a rid of its own, the completion rides the last row
            // (as in `execute`); without one, whatever `execute` armed for
            // the calling op is left alone.
            let _armed =
                (rid.is_some() && i + 1 == n).then(|| journal::PendingCompletion::set(rid));
            journal::append_tx(&mut tx, &self.journal_ks, &self.local, &self.completed, rec)?;
        }
        if n == 0 {
            if let Some(rid) = rid {
                // Nothing of it applied (every record lost to a newer
                // state), but the op completed here: the log must still
                // say so, once.
                journal::append_completion_tx(
                    &mut tx,
                    &self.journal_ks,
                    &self.local,
                    &self.completed,
                    rid,
                )?;
            }
        }
        if adopted {
            // Plan 30 §M9: the predecessor's outcome rows ride its tail
            // too — a journaled refusal (the rid's outcome, which every
            // later execution of it must dedup to) and an inbox position's
            // acknowledgement (the watermark a later drain skips by). They
            // touch no key, so the loop above does not journal them; left
            // out, the log never carried them (backup seed 607661: the
            // re-shipped tail had the rename but not the two `Refused`
            // rows that had observed it). Applied above already (the
            // `completed` row, the watermark); journaled here, after the
            // transaction's own rows, in order.
            for rec in records {
                match rec {
                    LogRecord::Refused { rid, errno } => {
                        let seq = journal::append_tx(
                            &mut tx,
                            &self.journal_ks,
                            &self.local,
                            &self.completed,
                            rec,
                        )?;
                        let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
                        tx.insert(
                            &self.completed,
                            rid.to_key(),
                            Meta::encode_refused_row(seq, now_ms, *errno),
                        );
                    }
                    LogRecord::InboxAck { .. } => {
                        journal::append_tx(
                            &mut tx,
                            &self.journal_ks,
                            &self.local,
                            &self.completed,
                            rec,
                        )?;
                    }
                    _ => {}
                }
            }
            for rec in &applied {
                if let LogRecord::WriteManifest { ino, manifest, .. } = rec {
                    crate::store::held::enroll_adopted_manifest_tx(&mut tx, self, *ino, manifest)?;
                }
            }
        }
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        // The rows journaled here are unshipped work of this tenure, like
        // `mutate::execute`'s: plan 30 §M5's forward-reply `base`, §M8's
        // ReadIndex positions and §M9's holder-read durability wait all
        // ask what the unshipped journal touched.
        self.note_unshipped(&applied);
        Ok(())
    }
}

// ------------------------------------------------------------ publishing

/// The earliest captured before-image of every `ns` key this node's
/// outstanding `Local` speculation touches. Overlaid on `ns`, it gives the
/// log-prefix state at `applied_seq` (see [`Meta::publish_basis_at`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogPrefixView {
    before: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl LogPrefixView {
    /// `Some(value)` when `key` carries unshipped local speculation: the
    /// log-prefix value to publish instead of the current one (`None`
    /// inside = the key did not exist).
    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.before.get(key).map(|v| v.as_deref())
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.before.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.before.is_empty()
    }

    pub fn len(&self) -> usize {
        self.before.len()
    }

    /// Every substituted key and its log-prefix value, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.before
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
    }

    /// Record `key`'s before-image unless an earlier row already did.
    pub(crate) fn note(&mut self, key: &[u8], before: &Option<Vec<u8>>) {
        self.before
            .entry(key.to_vec())
            .or_insert_with(|| before.clone());
    }

    fn range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        self.before.range(start.to_vec()..end.to_vec())
    }
}

/// What a publish may build its commit from (`Meta::publish_basis_at`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishBasis {
    /// `ns` as it stands is the log-prefix state at `applied_seq` (nothing
    /// outstanding), or — for a node that holds no lease — the pre-M3b
    /// behaviour for its uncaptured local writes.
    AsIs,
    /// `ns` with this overlay is the log-prefix state: the holder's
    /// unshipped journal, substituted by its before-images.
    Substituted(LogPrefixView),
    /// No commit can reflect this replica right now: requester speculation
    /// (a shadow or hint) is outstanding, or this holder's unshipped
    /// journal was not captured (holder capture off — the fallback — or a
    /// transaction from before it was switched on).
    Defer,
}

impl Meta {
    /// Plan 30 §M3b's publish rule, read under the publisher's snapshot so
    /// the overlay, the dirty set and `applied_seq` describe one instant.
    pub fn publish_basis_at(&self, snap: &Snapshot) -> Result<PublishBasis, MetaError> {
        if counter_get(snap, &self.local, KV_SPEC_LIVE_COUNT)? > 0 {
            return Ok(PublishBasis::Defer);
        }
        if counter_get(snap, &self.local, KV_UNCAPTURED_TX_COUNT)? > 0 {
            return Ok(if self.holder_epoch() != 0 {
                PublishBasis::Defer
            } else {
                PublishBasis::AsIs
            });
        }
        if counter_get(snap, &self.local, KV_LOCAL_SPEC_COUNT)? == 0 {
            return Ok(PublishBasis::AsIs);
        }
        let from = journal::acked_watermark(snap, &self.local)?.saturating_add(1);
        let Some(first) = first_captured_from(snap, self, from)? else {
            return Ok(PublishBasis::AsIs);
        };
        match spec::local_before_images_from(snap, self, first)? {
            Some(view) => Ok(PublishBasis::Substituted(view)),
            None => Ok(PublishBasis::Defer),
        }
    }

    /// Plan 30 §M4 item 3: whether `ns` under `snap` is exactly the log
    /// prefix at its `applied_seq` — nothing speculative outstanding and
    /// nothing of this node's own journaled. Only then may a follower
    /// treat a commit that covers its applied position as covering every
    /// key it has dirty (`cli::mtree_publish::TreePublisher::follow_head`).
    pub fn is_log_prefix_at(&self, snap: &Snapshot) -> Result<bool, MetaError> {
        Ok(counter_get(snap, &self.local, KV_SPEC_LIVE_COUNT)? == 0
            && counter_get(snap, &self.local, KV_LOCAL_SPEC_COUNT)? == 0
            && counter_get(snap, &self.local, KV_UNCAPTURED_TX_COUNT)? == 0
            && journal::take(snap, &self.journal_ks, &self.local, 1)?.is_empty())
    }

    /// `ns`'s value at `key` under `view` (see [`LogPrefixView::get`]).
    pub fn ns_get_via_at(
        &self,
        r: &impl Readable,
        view: &LogPrefixView,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, MetaError> {
        match view.get(key) {
            Some(before) => Ok(before.map(<[u8]>::to_vec)),
            None => self.ns_get_at(r, key),
        }
    }

    /// [`Meta::tree_inode_at`] under `view`: the inode record, and its
    /// spilled xattrs, as they stand at the log prefix.
    pub fn tree_inode_via_at(
        &self,
        r: &impl Readable,
        view: &LogPrefixView,
        ino: Ino,
    ) -> Result<Option<TreeInode>, MetaError> {
        if view.is_empty() {
            return self.tree_inode_at(r, ino);
        }
        let Some(raw) = self.ns_get_via_at(r, view, &keys::inode(ino))? else {
            return Ok(None);
        };
        let rec = InodeRecord::decode(&raw)?;
        let attr = ns::attrs_to_fileattr(
            ino,
            &rec.attrs,
            crate::store::atime::get_atime(r, &self.atime, ino)?,
        );
        let target = match &rec.symlink_target {
            Some(p) => {
                Some(String::from_utf8_lossy(&ns::resolve_payload(r, &self.blobs, p)?).into_owned())
            }
            None => None,
        };
        let manifest = match &rec.manifest {
            Some(p) => Some(ns::resolve_payload(r, &self.blobs, p)?),
            None => None,
        };
        let xattrs = if !rec.xattrs.is_empty() {
            rec.xattrs
                .iter()
                .map(|(n, v)| (String::from_utf8_lossy(n).into_owned(), v.clone()))
                .collect()
        } else {
            let range = keys::xattrs_of(ino);
            let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for guard in r.range(&self.ns, range.start().to_vec()..range.end().to_vec()) {
                let (k, v) = guard.into_inner()?;
                merged.insert(k.to_vec(), v.to_vec());
            }
            for (k, before) in view.range(range.start(), range.end()) {
                match before {
                    Some(v) => {
                        merged.insert(k.clone(), v.clone());
                    }
                    None => {
                        merged.remove(k);
                    }
                }
            }
            let mut out = Vec::with_capacity(merged.len());
            for (k, v) in merged {
                if let keys::Key::Xattr { name, .. } = keys::Key::parse(&k)? {
                    let value = ns::resolve_payload(r, &self.blobs, &Payload::decode(&v)?)?;
                    out.push((String::from_utf8_lossy(name).into_owned(), value));
                }
            }
            out
        };
        Ok(Some(TreeInode {
            attr,
            target,
            manifest,
            xattrs,
        }))
    }
}

/// Plan 30 §M3b's hot-path rule, pinned: no per-write, per-ack, per-segment
/// or per-`status` path may scan a keyspace from its start (the LSM keeps
/// the retired history there as tombstones until compaction) or scan the
/// outstanding backlog to answer a count. The tests "poison" the regions
/// those paths must not visit with undecodable rows: a path that reads
/// one fails to decode it and errors, so a pass is proof it never looked.
#[cfg(test)]
mod tests {
    use crate::mutate::{execute, MutateOp};
    use crate::rid::Rid;
    use crate::store::Meta;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    const GARBAGE: &[u8] = b"\xff\xff\xff\xff not a postcard row";

    fn create(meta: &Meta, n: u64) {
        execute(
            meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: format!("f{n}"),
                ino: (3 << 40) | n,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(Rid {
                node: 3,
                incarnation: 1,
                seq: n,
            }),
        )
        .unwrap();
    }

    fn ship_all(meta: &Meta, segment: u64) {
        let seqs: Vec<u64> = meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        meta.ack_journal_rows_at(&seqs, segment).unwrap();
    }

    fn poison(meta: &Meta, ks: &fjall::SingleWriterTxKeyspace, key: u64) {
        let mut tx = meta.db.write_tx();
        tx.insert(ks, key.to_be_bytes().to_vec(), GARBAGE.to_vec());
        tx.commit().unwrap();
    }

    /// `status` (`speculation_counts`, `has_outstanding_speculation`,
    /// `local_speculation_count`) reads three counters, not the rows: an
    /// undecodable row in every counted keyspace changes nothing.
    #[test]
    fn status_counts_read_counters_not_rows() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        for n in 1..=3 {
            create(&meta, n);
        }
        for ks in [&meta.journal_tx, &meta.spec_live, &meta.pending_replay] {
            poison(&meta, ks, u64::MAX);
        }
        let counts = meta.speculation_counts().unwrap();
        assert_eq!(
            (counts.outstanding, counts.pending_replay, counts.local),
            (0, 0, 3)
        );
        assert!(!meta.has_outstanding_speculation());
        assert_eq!(meta.local_speculation_count().unwrap(), 3);
        assert!(meta.pending_replays().unwrap().is_empty());
    }

    /// Shipping many transactions, then more: the ack's retirement and
    /// compaction, the journal reads, and every tailed segment's checks
    /// start past what already shipped. The shipped history (everything
    /// below the acked watermark and the spec floor) is poisoned; the next
    /// write, ship and tailed segment still work, and the poison is never
    /// deleted (compaction did not range over it).
    #[test]
    fn ship_path_cost_is_what_shipped_not_the_history() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        let mut segment = 0;
        for n in 1..=50 {
            create(&meta, n);
            segment += 1;
            ship_all(&meta, segment);
        }
        assert_eq!(meta.speculation_counts().unwrap().local, 0);
        // Key 0 is below every journal seq, `journal_tx` key and spec seq
        // ever handed out, and so below every watermark and floor.
        for ks in [&meta.journal_ks, &meta.journal_tx, &meta.spec] {
            poison(&meta, ks, 0);
        }
        for n in 51..=60 {
            create(&meta, n);
        }
        assert_eq!(
            meta.journal_len().unwrap(),
            20,
            "10 creates, op + Completed"
        );
        assert_eq!(meta.take_journal_grouped(3).unwrap()[0].1.len(), 4);
        assert!(meta.pending_touches().unwrap().inos.is_empty());
        let basis = meta
            .read_consistent(|snap| meta.publish_basis_at(snap))
            .unwrap();
        assert!(matches!(basis, crate::store::PublishBasis::Substituted(_)));
        segment += 1;
        ship_all(&meta, segment);
        assert_eq!(meta.speculation_counts().unwrap().local, 0);
        meta.set_holder_epoch(0);
        segment += 1;
        meta.apply_segment(segment, 1, &[], &crate::replay::TouchSet::default())
            .unwrap();
        let r = meta.db.read_tx();
        for ks in [&meta.journal_ks, &meta.journal_tx, &meta.spec] {
            assert_eq!(
                fjall::Readable::get(&r, ks, 0u64.to_be_bytes())
                    .unwrap()
                    .as_deref(),
                Some(GARBAGE),
                "a ship-path scan reached below its floor"
            );
        }
    }

    /// Plan 30 §M4 round 2: with nothing poisoned or held, the ship plan,
    /// the upload pass's bookkeeping, the retirement and the ack do no
    /// held-set work at all — M3b's ship path, unchanged. A malformed
    /// `poisoned/` mark is planted behind the counter's back: any path that
    /// scans the marks while the counter reads zero fails to decode it. The
    /// held-work counter stays at zero across many writes and ships, and
    /// moves once something is held (non-vacuity).
    #[test]
    fn ship_path_does_no_held_set_work_when_nothing_is_held() {
        use std::sync::atomic::Ordering;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        {
            let mut tx = meta.db.write_tx();
            tx.insert(&meta.local, b"poisoned/garbage".to_vec(), Vec::new());
            tx.commit().unwrap();
        }
        let mut segment = 0;
        for n in 1..=30 {
            create(&meta, n);
            meta.note_unrecoverable_chunks(&[], false).unwrap();
            meta.note_unrecoverable_chunks(&[], true).unwrap();
            let rows: Vec<u64> = meta
                .take_journal_grouped(usize::MAX)
                .unwrap()
                .into_iter()
                .flat_map(|(_, b)| b)
                .map(|(s, _)| s)
                .collect();
            assert_eq!(rows.len(), 2, "op + Completed");
            segment += 1;
            meta.ack_journal_rows_at(&rows, segment).unwrap();
        }
        assert_eq!(meta.journal_len().unwrap(), 0);
        assert_eq!(meta.speculation_counts().unwrap().local, 0);
        assert_eq!(meta.held_work.load(Ordering::Relaxed), 0);
        assert!(meta.unrecoverable_chunks().unwrap().is_empty());

        // Non-vacuity: once a chunk is recorded unrecoverable and a
        // manifest names it, the held-set paths run.
        let mut tx = meta.db.write_tx();
        tx.remove(&meta.local, b"poisoned/garbage".to_vec());
        tx.commit().unwrap();
        let lost = constellation_fs_core::ChunkHash::of(b"gone");
        let ino = (3 << 40) | 1;
        meta.set_manifest_dirty(ino, None, b"not a manifest", 4, &[lost])
            .unwrap();
        create(&meta, 31);
        meta.note_unrecoverable_chunks(&[(lost, ino)], true)
            .unwrap();
        let rows: Vec<u64> = meta
            .take_journal_grouped(usize::MAX)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .map(|(s, _)| s)
            .collect();
        segment += 1;
        meta.ack_journal_rows_at(&rows, segment).unwrap();
        assert!(meta.held_work.load(Ordering::Relaxed) >= 2, "plan and ack");
        assert_eq!(meta.held_summary().transactions, 1);
    }

    /// Retiring a ship's transactions deletes exactly their `spec` rows and
    /// leaves the unshipped ones' — compaction is proportional to what
    /// shipped, and the floor lands on the oldest outstanding row.
    #[test]
    fn a_partial_ship_compacts_exactly_the_shipped_rows() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        for n in 1..=4 {
            create(&meta, n);
        }
        let spec_rows = |meta: &Meta| {
            let r = meta.db.read_tx();
            fjall::Readable::iter(&r, &meta.spec).count()
        };
        assert_eq!(spec_rows(&meta), 4);
        // Ship the first two transactions (two rows each).
        let head: Vec<u64> = meta
            .take_journal(4)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        meta.ack_journal_rows_at(&head, 1).unwrap();
        assert_eq!(spec_rows(&meta), 2);
        assert_eq!(meta.speculation_counts().unwrap().local, 2);
    }
}

#[cfg(test)]
mod root_substitution_tests {
    use super::*;
    use crate::mutate::{execute, MutateOp};
    use crate::rid::Rid;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    /// Plan 30 M5 round 3: a substituted (log-prefix) publish with the
    /// root directory behind an unshipped local transaction must publish
    /// the root as it was *shipped* — owner included — never the genesis
    /// root. A fresh node bootstrapping from such a commit otherwise
    /// comes up with a `0:0` root and refuses its own user's writes
    /// (`fresh-node-bootstrap`).
    #[test]
    fn a_substituted_publish_keeps_the_shipped_root_owner() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        meta.setattr(ROOT_INO, None, Some(1000), Some(1000), None, None, None)
            .unwrap();
        let seqs: Vec<u64> = meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        meta.ack_journal_rows_at(&seqs, 1).unwrap();
        // An unshipped create in the root (its mtime moves): captured.
        execute(
            &meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "f".into(),
                ino: (3 << 40) | 1,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
            Some(Rid {
                node: 3,
                incarnation: 1,
                seq: 1,
            }),
        )
        .unwrap();
        let (basis, root) = meta
            .read_consistent(|snap| -> Result<_, MetaError> {
                let basis = meta.publish_basis_at(snap)?;
                let view = match &basis {
                    PublishBasis::Substituted(view) => view.clone(),
                    _ => LogPrefixView::default(),
                };
                let root = meta.tree_inode_via_at(snap, &view, ROOT_INO)?;
                Ok((basis, root))
            })
            .unwrap();
        assert!(
            matches!(basis, PublishBasis::Substituted(_)),
            "the create is unshipped and captured: {basis:?}"
        );
        let root = root.expect("the root is in the published tree");
        assert_eq!(
            (root.attr.uid, root.attr.gid),
            (1000, 1000),
            "the log-prefix root carries the shipped owner"
        );
    }
}

/// `local` kv: the next stream index a delegate assigns under a
/// generation.
fn deleg_idx_key(gen: u64) -> String {
    format!("deleg_idx:{gen}")
}

/// `local` kv: the highest index of generation `gen` this replica holds
/// *from the log* (appended in the root's journal here, or applied from
/// a segment) — never a shadow's (phase 2b: a successor root's cursor,
/// a delegate's re-stream start).
pub(crate) fn deleg_log_idx_key(gen: u64) -> String {
    format!("deleg_log_idx:{gen}")
}

/// Raise the persisted log index of `gen` to `idx`.
pub(crate) fn bump_log_idx_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
    idx: u64,
) -> Result<(), MetaError> {
    if gen == 0 {
        return Ok(());
    }
    let key = deleg_log_idx_key(gen);
    let cur = kv_get_tx(tx, local, &key)?
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if idx > cur {
        kv_set_tx(tx, local, &key, &idx.to_string());
    }
    Ok(())
}

/// Plan 30 §M11: one transaction of a delegate's stream.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DelegateTx {
    pub idx: u64,
    pub rid: Option<Rid>,
    pub records: Vec<LogRecord>,
    pub deps: crate::session::Position,
}
