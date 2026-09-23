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
    adjust_usage_tx, counter_add_tx, counter_get, journal, ns, spec, usage_note_begin,
    usage_note_take, Meta, UsageTracker, KV_LOCAL_SPEC_COUNT, KV_SPEC_LIVE_COUNT,
    KV_UNCAPTURED_TX_COUNT,
};
use crate::TreeInode;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use constellation_mtree::record::{InodeRecord, Payload};
use fjall::{Readable, SingleWriterWriteTx, Snapshot};
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
    /// The rid `mutate::execute` ran it under, if any.
    pub rid: Option<Rid>,
    /// The op `mutate::execute` ran, if the transaction came from one. A
    /// replay after a deposition re-executes this; a transaction without
    /// one (a local manifest commit, a snapshot row) is replayed from its
    /// records instead (`spec::derive_replay_op`).
    pub op: Option<MutateOp>,
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
        let capturing = self.holder_capture()
            && (epoch != 0
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
        put_journal_tx(
            tx,
            self,
            local.start,
            &JournalTx {
                last: end - 1,
                spec_seq,
                epoch: local.epoch,
                rid,
                op,
            },
        )
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
    pub(crate) fn apply_records_journaled(&self, records: &[LogRecord]) -> Result<(), MetaError> {
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
        for rec in applied
            .iter()
            .filter(|rec| !matches!(rec, LogRecord::Completed { .. } | LogRecord::Atime { .. }))
        {
            journal::append_tx(&mut tx, &self.journal_ks, &self.local, &self.completed, rec)?;
        }
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
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
