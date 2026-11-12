//! Plan 30 §M9: the backup tail — what a synchronous backup holds for
//! the holder it backs, and what a holder reads from its journal to
//! stream to its backups.
//!
//! # The `backup_tail` keyspace
//!
//! `(epoch BE, first journal seq BE) → BackupTx { last, records }`: one
//! row per holder journal transaction the backup has persisted and
//! acknowledged. A backup's acknowledgement means exactly "these rows are
//! committed here", so `backup_append` commits before the core answers.
//! Committed, not fsynced (M16's decision, plan 30 §3's contract): the
//! rows survive a crash of this process and any failure of the holder,
//! not a simultaneous power loss of the holder and every backup — see
//! `docs/reference/features/durability-and-failover.md`.
//! The tail is trimmed by the log itself: every segment a backup applies
//! names the journal seqs it carries (the envelope's `rows`, plan 30
//! §M9) and its `through`, and [`Meta::backup_trim`] deletes what they
//! confirm — a backup is an ordinary log subscriber, so the "shipped
//! notification" the plan asks for is the segment on its stream.
//!
//! At a takeover the backup tails to head first (trimming exactly what
//! the dead holder managed to ship), then applies what is left of the
//! tail, in journal order, into its *own* journal
//! ([`Meta::apply_records_journaled`]) inside its takeover gate, and
//! ships it under the new epoch like any other journal.
//!
//! # The role and the seal
//!
//! `local["backup_role"]` records whom this node backs (`holder:epoch:
//! config_version`) and `local["backup_sealed"]` the highest epoch this
//! node has sealed. Both are persisted before the core acts on them: a
//! sealed epoch stays sealed across a restart (the old holder must never
//! collect an ack from a backup that has sealed, however the backup's
//! process fared), and a restarted backup still knows whose tail it
//! holds. The seal is also fsynced before it is acted on (M16): it must
//! survive a power loss too, since safety, not just durability, rests on
//! it.

use super::journal;
use super::local::{tx_key, JournalTxHead};
use super::{kv_get_tx, kv_set_tx, Meta};
use crate::error::MetaError;
use crate::record::LogRecord;
use fjall::Readable;
use serde::{Deserialize, Serialize};

const KV_BACKUP_ROLE: &str = "backup_role";
const KV_BACKUP_SEALED: &str = "backup_sealed";
/// ADR-51: the `(epoch, source)` streams this node sealed for a claimant's
/// kept fetch, as `holder:epoch` pairs joined by commas.
const KV_SEALED_TENURES: &str = "sealed_tenures";

/// One holder journal transaction as streamed to a backup: its rows
/// `first..=last` and their records, in order. Whole transactions only
/// (plan 30 §M3b: an op's records and its `Completed` never split), so a
/// backup can re-execute one as a unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupTx {
    pub first: u64,
    pub last: u64,
    pub records: Vec<LogRecord>,
    /// Plan 30 §M11: the transaction's delegation origin `(gen, idx)`
    /// (`(0, 0)`: the holder's own) — a root's append of a delegate's
    /// stream row. A backup that takes over re-journals the row under
    /// it, so its segment still names the origin: the delegate retires
    /// its row by it, and every replica's (and the successor's own)
    /// per-generation index advances. Without it the re-shipped row
    /// went out as the successor's own, the delegate's row never
    /// retired, and every later segment's insert-before-`Local` redo
    /// re-applied it on top of newer state (delegated-holder-cut seed
    /// 1719: a stale `unlink f0` deleted the re-created `f0`).
    pub origin: (u64, u64),
}

/// Plan 46 M0.2 follow-up (defect B): the backup-tail transactions of the
/// identities `idents` (`LogRecord::RolledBack`: the holder's `repair
/// drop-held` rolled them back) become holes, in the caller's transaction:
/// the same journal seqs and no records, as the holder's own stream steps
/// over rows its journal no longer has, so the tail stays contiguous and a
/// successor applying it re-ships nothing of them. A transaction's
/// identity is its `TailOf` if it begins with one, else its place
/// `(epoch, first)` in the tail. The member copies are in the same
/// keyspace, past [`super::member::MEMBER_COPY_BASE`], and are dealt with
/// by `member::hole_rolled_back_tx`; so are the delegates' streams, past
/// them (`DELEG_BACKUP_BASE`), whose rows are not `BackupTx`. Returns how
/// many became holes.
pub(crate) fn hole_tail_rolled_back_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    idents: &std::collections::BTreeSet<(u64, u64)>,
) -> Result<usize, MetaError> {
    if idents.is_empty() {
        return Ok(0);
    }
    let mut doomed: Vec<(Vec<u8>, BackupTx)> = Vec::new();
    for guard in tx.range(
        &meta.backup_tail,
        tail_key(0, 0)..tail_key(super::member::MEMBER_COPY_BASE, 0),
    ) {
        let (k, v) = guard.into_inner()?;
        let t: BackupTx = crate::record::from_bytes_exact(&v)?;
        if t.records.is_empty() {
            continue;
        }
        let (epoch, first) = decode_tail_key(&k)?;
        let ident = super::member::tail_identity(&t.records).unwrap_or((epoch, first));
        if idents.contains(&ident) {
            doomed.push((k.to_vec(), t));
        }
    }
    let holed = doomed.len();
    for (k, t) in doomed {
        let hole = BackupTx {
            first: t.first,
            last: t.last,
            records: Vec::new(),
            origin: t.origin,
        };
        tx.insert(&meta.backup_tail, k, postcard::to_allocvec(&hole)?);
    }
    Ok(holed)
}

/// [`Meta::backup_clear`] in the caller's transaction. The caller resets
/// `backup_tail_floor` once the transaction has committed.
pub(crate) fn backup_clear_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
) -> Result<(), MetaError> {
    // The holder's tail only: the keys from `MEMBER_COPY_BASE` up are
    // the member copies and, past them, the delegates' streams this
    // node backs (`DELEG_BACKUP_BASE`), which outlive any holder
    // tenure (long-delegated-backup seed 70232: a node that
    // stopped being the holder's backup wiped a delegate's rows too;
    // its acknowledgement fell to 0 for good, the delegate re-sent
    // and was answered short in a loop, and its client's op waited
    // for a segment no holder was left to ship).
    let keys: Vec<Vec<u8>> = tx
        .range(
            &meta.backup_tail,
            tail_key(0, 0)..tail_key(super::member::MEMBER_COPY_BASE, 0),
        )
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(&meta.backup_tail, k);
    }
    tx.remove(&meta.local, KV_BACKUP_ROLE.as_bytes().to_vec());
    Ok(())
}

/// Whom this node backs (persisted).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupRole {
    pub holder: u64,
    pub epoch: u64,
    pub config_version: u64,
}

pub(super) fn tail_key(epoch: u64, first: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(16);
    k.extend_from_slice(&epoch.to_be_bytes());
    k.extend_from_slice(&first.to_be_bytes());
    k
}

pub(super) fn decode_tail_key(k: &[u8]) -> Result<(u64, u64), MetaError> {
    if k.len() != 16 {
        return Err(MetaError::Invalid("backup_tail key".into()));
    }
    let epoch = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
    let first = u64::from_be_bytes(k[8..].try_into().expect("8 bytes"));
    Ok((epoch, first))
}

impl Meta {
    // ---------------------------------------------------------- holder

    /// The holder's journal transactions starting at journal seq `from`
    /// (a transaction boundary: the seq after an acknowledged `last`),
    /// oldest first, at most `max_rows` rows (a transaction is never
    /// split) — every row, held-back and deferred ones included: a backup
    /// must hold everything the holder journaled, whether or not it is
    /// shippable yet.
    ///
    /// Contiguous from `from`: rows the journal no longer has (shipped
    /// and deleted out of order behind a held-back transaction, or rolled
    /// back) come out as *holes* — a transaction with no records
    /// spanning them — so a backup's contiguous hold and a subscriber's
    /// stream-ahead cursor step over them instead of waiting for rows
    /// that will never come (round 2: a burst's held-back manifest row
    /// kept a candidate backup at its first hole forever). A row outside
    /// any `journal_tx` head is a transaction of its own.
    pub fn journal_txs_from(&self, from: u64, max_rows: usize) -> Result<Vec<BackupTx>, MetaError> {
        let r = self.db.read_tx();
        let mut out: Vec<BackupTx> = Vec::new();
        let mut rows = 0usize;
        let mut expect = from;
        let mut cur: Option<BackupTx> = None;
        for guard in r.range(&self.journal_ks, journal::seq_key(from)..) {
            let (k, v) = guard.into_inner()?;
            let (seq, rec) = journal::decode_row(&k, &v)?;
            if let Some(t) = cur.as_mut() {
                if seq <= t.last {
                    t.records.push(rec);
                    rows += 1;
                    continue;
                }
                let done = cur.take().expect("present");
                expect = done.last + 1;
                out.push(done);
                if rows >= max_rows {
                    return Ok(out);
                }
            }
            if seq > expect {
                out.push(BackupTx {
                    first: expect,
                    last: seq - 1,
                    records: Vec::new(),
                    origin: (0, 0),
                });
            }
            let (last, origin) = match r.get(&self.journal_tx, tx_key(seq))? {
                Some(h) => {
                    let h = postcard::from_bytes::<JournalTxHead>(&h)?;
                    (h.last.max(seq), (h.gen, h.idx))
                }
                None => (seq, (0, 0)),
            };
            rows += 1;
            cur = Some(BackupTx {
                first: seq,
                last,
                records: vec![rec],
                origin,
            });
        }
        if let Some(t) = cur {
            out.push(t);
        }
        Ok(out)
    }

    /// The highest journal seq allocated so far (0: none), for the
    /// holder's "is there anything to stream" check: one counter read.
    pub fn journal_tip(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        journal::peek_next_seq(&r, &self.local).map(|n| n.saturating_sub(1))
    }

    // ---------------------------------------------------------- backup

    /// Persist the holder's transactions `txs` for `epoch`. Committed
    /// before returning: the acknowledgement the caller sends means
    /// exactly this.
    pub fn backup_append(&self, epoch: u64, txs: &[BackupTx]) -> Result<(), MetaError> {
        if txs.is_empty() {
            return Ok(());
        }
        // Every live key of the tail is at or above the floor (`backup_trim`
        // keeps it; a resend lowers it below, after the commit), so the
        // probe for a stored span reaching into a transaction starts there
        // and walks none of the tombstones the trims left below it (Round
        // 2): from the floor of this epoch, or from this batch's lowest key
        // if that is lower; past this epoch, nothing of it is left.
        let lowest = txs.iter().map(|t| t.first).min().unwrap_or(0);
        let probe_from = match *self.backup_tail_floor.lock().unwrap() {
            Some((e, _)) if e > epoch => None,
            Some((e, f)) if e == epoch => Some(f.min(lowest)),
            _ => Some(0),
        };
        let mut top = *self.backup_tail_top.lock().unwrap();
        let mut tx = self.db.write_tx();
        // ADR-59 part B (D2): what a `DropHeld` retired is stored as a hole,
        // where it lands on no stored span (the gate's `tail_retired` rule
        // covers the rest).
        let holed = self.holes_for_retired_tx(&tx, epoch, txs)?;
        let hole_of = |i: usize| holed.as_ref().map(|h| &h[i]);
        for (i, t) in txs.iter().enumerate() {
            // Past every stored span of the epoch (an append's steady
            // state): nothing to supersede, nothing that spans it.
            if top.is_some_and(|(e, last)| e == epoch && last < t.first) {
                tx.insert(
                    &self.backup_tail,
                    tail_key(epoch, t.first),
                    postcard::to_allocvec(hole_of(i).unwrap_or(t))?,
                );
                top = Some((epoch, t.last));
                continue;
            }
            // A resend from a boundary (a holder that restarted, a backup
            // brought up again) streams the rows its journal no longer has
            // as one hole, where an earlier send had their transactions:
            // the hole supersedes every copy it spans, or the copies keyed
            // inside it break the contiguous hold (`backup_acked`, the
            // sealed tail's `through`) at the hole's end, and a successor
            // drops the acknowledged rows past it as never acknowledged
            // (metered-shared+links seed 1479). Rows a journal no longer
            // has never come back under the same seq, so a transaction a
            // stored hole spans is a late copy of rows gone: not kept.
            //
            // Both rest on every span (a transaction, a hole) beginning and
            // ending on transaction boundaries: a stored span keyed inside
            // `t` ends inside it, and none below it reaches into it, except
            // a stored hole over a late copy or over the start of a hole
            // (merged). A producer that breaks that is a bug the debug build
            // stops at, not a hold that shrinks.
            let at = tail_key(epoch, t.first);
            let end = tail_key(epoch, t.last);
            let mut inside: Vec<(Vec<u8>, BackupTx)> = Vec::new();
            for guard in tx.range(&self.backup_tail, at.clone()..=end) {
                let (k, v) = guard.into_inner()?;
                inside.push((k.to_vec(), crate::record::from_bytes_exact(&v)?));
            }
            let below: Option<BackupTx> = match probe_from {
                Some(from) if from < t.first => {
                    match tx
                        .range(&self.backup_tail, tail_key(epoch, from)..at.clone())
                        .next_back()
                    {
                        Some(guard) => {
                            let (_, v) = guard.into_inner()?;
                            Some(crate::record::from_bytes_exact(&v)?)
                        }
                        None => None,
                    }
                }
                _ => None,
            };
            let reaches = below.as_ref().filter(|o| o.last >= t.first);
            if !t.records.is_empty()
                && (reaches.is_some_and(|o| o.records.is_empty())
                    || inside.iter().any(|(_, o)| o.records.is_empty()))
            {
                // A late copy of rows a stored hole already spans.
                debug_assert!(
                    reaches.is_none_or(|o| o.records.is_empty() && o.last >= t.last)
                        && inside
                            .iter()
                            .all(|(_, o)| o.records.is_empty() || o.last <= t.last),
                    "a late copy {}..={} of epoch {epoch} straddles a stored span",
                    t.first,
                    t.last
                );
                continue;
            }
            if t.records.is_empty() {
                if let Some(o) = reaches.filter(|o| o.records.is_empty()) {
                    // A hole that begins inside a stored one (a resend from a
                    // boundary inside a span its journal no longer has): one
                    // hole over both, keyed where the stored one begins.
                    let merged = BackupTx {
                        first: o.first,
                        last: o.last.max(t.last),
                        records: Vec::new(),
                        origin: o.origin,
                    };
                    debug_assert!(
                        inside.iter().all(|(_, i)| i.last <= merged.last),
                        "a stored span of epoch {epoch} keyed inside {}..={} ends past it",
                        t.first,
                        t.last
                    );
                    for (k, _) in inside {
                        tx.remove(&self.backup_tail, k);
                    }
                    tx.insert(
                        &self.backup_tail,
                        tail_key(epoch, merged.first),
                        postcard::to_allocvec(&merged)?,
                    );
                    top = None;
                    continue;
                }
            }
            debug_assert!(
                reaches.is_none(),
                "a stored span of epoch {epoch} reaches into {}..={} (hole {}): {:?}",
                t.first,
                t.last,
                t.records.is_empty(),
                reaches.map(|o| (o.first, o.last, o.records.len()))
            );
            debug_assert!(
                inside.iter().all(|(_, o)| o.last <= t.last),
                "a stored span of epoch {epoch} keyed inside {}..={} ends past it",
                t.first,
                t.last
            );
            let stored = if inside.is_empty() && reaches.is_none() {
                hole_of(i).unwrap_or(t)
            } else {
                t
            };
            for (k, _) in inside {
                if k != at {
                    tx.remove(&self.backup_tail, k);
                }
            }
            tx.insert(&self.backup_tail, at, postcard::to_allocvec(stored)?);
            top = None;
        }
        // Where the epoch's highest stored span ends (spans do not overlap,
        // so the highest key's): one seek, after a batch that probed.
        if top.is_none() {
            if let Some(guard) = tx
                .range(
                    &self.backup_tail,
                    tail_key(epoch, 0)..=tail_key(epoch, u64::MAX),
                )
                .next_back()
            {
                let (_, v) = guard.into_inner()?;
                let o: BackupTx = crate::record::from_bytes_exact(&v)?;
                top = Some((epoch, o.last));
            }
        }
        // (A `DropHeld` appended here holes nothing: the drop is decided when
        // the log carries the record, not when the backup is streamed it. A
        // holder deposed before the record ships leaves its tail's rows
        // standing; the log's segment that carries the record holes them,
        // `hole_tail_rolled_back_tx` from the segment apply.)
        tx.commit()?;
        // A resend can land below the floor (from the contiguous
        // acknowledgement, under a batch held ahead of a gap).
        let mut floor = self.backup_tail_floor.lock().unwrap();
        if let Some(f) = *floor {
            *floor = Some(f.min((epoch, lowest)));
        }
        *self.backup_tail_top.lock().unwrap() = top;
        Ok(())
    }

    /// The highest `last` held for `epoch` (0: nothing).
    pub fn backup_acked(&self, epoch: u64) -> Result<u64, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        // The highest `last` reached *contiguously* from the oldest held
        // transaction: the holder pipelines appends, so a batch can be
        // persisted ahead of a gap, and what a restart reports as held
        // must not skip the gap (the holder would never fill it).
        let r = self.db.read_tx();
        let mut acked = 0;
        let mut prev: Option<u64> = None;
        for guard in r.range(
            &self.backup_tail,
            tail_key(epoch, 0)..=tail_key(epoch, u64::MAX),
        ) {
            let (_, v) = guard.into_inner()?;
            let t: BackupTx = crate::record::from_bytes_exact(&v)?;
            if prev.is_some_and(|p| t.first != p + 1) {
                break;
            }
            acked = t.last;
            prev = Some(t.last);
        }
        Ok(acked)
    }

    /// The tail held for `epoch`, oldest first.
    pub fn backup_tail(&self, epoch: u64) -> Result<Vec<BackupTx>, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.range(
            &self.backup_tail,
            tail_key(epoch, 0)..=tail_key(epoch, u64::MAX),
        ) {
            let (_, v) = guard.into_inner()?;
            out.push(crate::record::from_bytes_exact(&v)?);
        }
        Ok(out)
    }

    /// A segment of `epoch` applied, carrying journal `rows` and shipping
    /// through `through`: delete every transaction of `epoch` it confirms
    /// (its `last` is a row, or at or below `through`), and every
    /// transaction of an older epoch (that tenure is over; what it did
    /// not ship, its successor already re-shipped or stranded). Returns
    /// how many rows went.
    ///
    /// Round 2: a backup is appended to and trimmed once per holder
    /// append, and a scan from the keyspace's start walks every
    /// tombstone the earlier trims left in the memtable (milliseconds,
    /// growing) before it reaches the live tail. The scan starts at the
    /// floor (`backup_tail_floor`) and stops at the first transaction of
    /// `epoch` that begins above everything the caller can trim: a
    /// transaction's `last` is at least its `first`, so nothing beyond
    /// it is doomed. Nothing to remove, nothing committed.
    pub fn backup_trim(&self, epoch: u64, through: u64, rows: &[u64]) -> Result<usize, MetaError> {
        let mut floor = self.backup_tail_floor.lock().unwrap();
        let start = match *floor {
            Some(f) if f <= (epoch, u64::MAX) => tail_key(f.0, f.1),
            Some(_) => return Ok(0),
            None => tail_key(0, 0),
        };
        let stop = rows.iter().copied().max().unwrap_or(0).max(through);
        let row_set: std::collections::BTreeSet<u64> = rows.iter().copied().collect();
        let mut tx = self.db.write_tx();
        let mut doomed = Vec::new();
        let mut holes: Vec<(Vec<u8>, u64, u64)> = Vec::new();
        let mut survivor: Option<(u64, u64)> = None;
        for guard in tx.range(&self.backup_tail, start..=tail_key(epoch, u64::MAX)) {
            let (k, v) = guard.into_inner()?;
            let (e, first) = decode_tail_key(&k)?;
            if e < epoch {
                doomed.push(k.to_vec());
                continue;
            }
            if first > stop {
                survivor = Some(survivor.map_or((e, first), |s| s.min((e, first))));
                break;
            }
            let t: BackupTx = crate::record::from_bytes_exact(&v)?;
            if t.last <= through {
                doomed.push(k.to_vec());
            } else if row_set.contains(&t.last) {
                // Shipped ahead of a row still deferred on the holder (above
                // `through`): in the log, so not part of what a successor
                // re-ships — but not a gap in the tail either. Plan 43 C1-4
                // (D6): the contiguous prefix a sealer's hold reaches
                // (`backup_acked`, the `through` of its marker) is counted
                // across it, so the row stays as a hole (the shape a shipped
                // or dropped row has in a holder's stream) until `through`
                // passes it. Trimmed away, the next transactions looked like
                // rows beyond a gap, never acknowledged, and a successor
                // dropped them from its re-ship.
                if !t.records.is_empty() {
                    holes.push((k.to_vec(), t.first, t.last));
                }
                survivor = Some(survivor.map_or((e, first), |s| s.min((e, first))));
            } else {
                survivor = Some(survivor.map_or((e, first), |s| s.min((e, first))));
            }
        }
        // Every live key of `epoch` (and no older epoch) is at or above
        // the lowest survivor; with none, a later append begins above
        // what the holder shipped through.
        *floor = Some(survivor.unwrap_or((epoch, through.saturating_add(1))));
        let n = doomed.len() + holes.len();
        if n == 0 {
            tx.rollback();
            return Ok(0);
        }
        for k in doomed {
            tx.remove(&self.backup_tail, k);
        }
        for (k, first, last) in holes {
            tx.insert(
                &self.backup_tail,
                k,
                postcard::to_allocvec(&BackupTx {
                    first,
                    last,
                    records: Vec::new(),
                    origin: (0, 0),
                })?,
            );
        }
        tx.commit()?;
        Ok(n)
    }

    /// Forget every tail row (the holder reconfigured this node out, the
    /// lease moved on, or the tail has been re-shipped).
    pub fn backup_clear(&self) -> Result<(), MetaError> {
        *self.backup_tail_floor.lock().unwrap() = None;
        *self.backup_tail_top.lock().unwrap() = None;
        let mut tx = self.db.write_tx();
        backup_clear_tx(&mut tx, self)?;
        tx.commit()?;
        Ok(())
    }

    pub fn backup_role(&self) -> Result<Option<BackupRole>, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        let Some(s) = kv_get_tx(&r, &self.local, KV_BACKUP_ROLE)? else {
            return Ok(None);
        };
        let mut it = s.split(':').map(|x| x.parse::<u64>());
        match (it.next(), it.next(), it.next()) {
            (Some(Ok(holder)), Some(Ok(epoch)), Some(Ok(config_version))) => Ok(Some(BackupRole {
                holder,
                epoch,
                config_version,
            })),
            _ => Ok(None),
        }
    }

    pub fn set_backup_role(&self, role: BackupRole) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        kv_set_tx(
            &mut tx,
            &self.local,
            KV_BACKUP_ROLE,
            &format!("{}:{}:{}", role.holder, role.epoch, role.config_version),
        );
        tx.commit()?;
        Ok(())
    }

    /// The highest epoch this node sealed (0: none). A sealed epoch's
    /// holder gets no acknowledgement from this node, ever again.
    pub fn backup_sealed_epoch(&self) -> Result<u64, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, KV_BACKUP_SEALED)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    /// ADR-51: the lease tenures `(holder, lease epoch)` this node sealed for
    /// a claimant's kept fetch, ascending. The refusal is keyed on the
    /// tenure, never on a stream: a successor's own tenure at a later epoch
    /// is not affected (spec §3.5 step 5, amended).
    pub fn sealed_tenures(&self) -> Result<Vec<(u64, u64)>, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        Ok(parse_sealed_tenures(
            kv_get_tx(&r, &self.local, KV_SEALED_TENURES)?.as_deref(),
        ))
    }

    /// ADR-51: whether the tenure `(holder, epoch)` is sealed here.
    pub fn tenure_sealed(&self, holder: u64, epoch: u64) -> Result<bool, MetaError> {
        Ok(self.sealed_tenures()?.contains(&(holder, epoch)))
    }

    /// ADR-51: seal the tenures `(holder, epoch)`: persisted and synced
    /// before the caller answers the claimant (K4: after the answer, the
    /// tenure's hold streams install nothing here).
    pub fn seal_tenures(&self, tenures: &[(u64, u64)]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let mut all =
            parse_sealed_tenures(kv_get_tx(&tx, &self.local, KV_SEALED_TENURES)?.as_deref());
        let before = all.len();
        all.extend_from_slice(tenures);
        all.sort_unstable();
        all.dedup();
        let grows = all.len() > before;
        if grows {
            let text: Vec<String> = all.iter().map(|(h, e)| format!("{h}:{e}")).collect();
            kv_set_tx(&mut tx, &self.local, KV_SEALED_TENURES, &text.join(","));
        }
        tx.commit()?;
        if grows {
            self.sync()?;
        }
        Ok(())
    }

    /// Seal `epoch`: persisted before the caller refuses its first append.
    pub fn backup_seal(&self, epoch: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let current: u64 = kv_get_tx(&tx, &self.local, KV_BACKUP_SEALED)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let raises = epoch > current;
        if raises {
            kv_set_tx(&mut tx, &self.local, KV_BACKUP_SEALED, &epoch.to_string());
        }
        tx.commit()?;
        // M16: on stable storage before the caller acts on it (refuses an
        // append, reads the lease to take over): a seal forgotten on a
        // power loss could let the deposed holder collect an ack.
        if raises {
            self.sync()?;
        }
        Ok(())
    }
}

// ------------------------------------------- plan 30 §M11 phase 2b: delegate backups

fn parse_sealed_tenures(text: Option<&str>) -> Vec<(u64, u64)> {
    text.unwrap_or("")
        .split(',')
        .filter_map(|p| {
            let (e, s) = p.split_once(':')?;
            Some((e.parse().ok()?, s.parse().ok()?))
        })
        .collect()
}

/// The `backup_tail` key space of a delegate's stream: `epoch` slots
/// above this base hold generation `epoch - base` (the M9 tail keys are
/// lease epochs, far below, and the member copies sit at
/// [`super::member::MEMBER_COPY_BASE`]). A generation number carries the
/// epoch of the tenure that minted it (`gen = epoch << 32 | n`, ADR-43), so
/// the slots reach `base + (GEN_MAX_EPOCH << 32 | u32::MAX)`: past the
/// member copies' and below `u64::MAX`, for every epoch that can mint
/// (`deleg_backup_slot_never_meets_a_member_copy_slot`).
const DELEG_BACKUP_BASE: u64 = 1 << 62;

pub(crate) fn deleg_epoch(gen: u64) -> u64 {
    DELEG_BACKUP_BASE + gen
}

/// The generation a [`deleg_epoch`] slot is the copy of (`None`: a member
/// copy slot).
pub(crate) fn gen_of_deleg_slot(slot: u64) -> Option<u64> {
    slot.checked_sub(DELEG_BACKUP_BASE)
}

fn deleg_prev_roots_key(gen: u64) -> String {
    format!("deleg_prev_roots:{gen}")
}

fn deleg_sealed_key(gen: u64) -> String {
    format!("deleg_backup_sealed:{gen}")
}

/// Plan 46 M1.2 step 7 (c3-spec §6.1): a deposed root's append of a
/// delegate's row, kept as a member copy of generation `gen`, in the
/// caller's transaction: the key and the bytes [`Meta::deleg_backup_append`]
/// writes, so a node that is also a recorded backup of `gen` shares the slot
/// with the same content.
pub(crate) fn deleg_member_copy_put_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    gen: u64,
    t: &super::DelegateTx,
) -> Result<(), MetaError> {
    let bytes =
        postcard::to_allocvec(t).map_err(|e| MetaError::Invalid(format!("delegate tx: {e}")))?;
    tx.insert(&meta.backup_tail, tail_key(deleg_epoch(gen), t.idx), bytes);
    Ok(())
}

impl Meta {
    /// A delegate's transactions `txs` of generation `gen`, persisted by
    /// its backup (idempotent: a retransmission rewrites the same keys).
    /// Returns [`Meta::deleg_backup_acked`] from `in_log`.
    pub fn deleg_backup_append(
        &self,
        gen: u64,
        in_log: u64,
        txs: &[super::DelegateTx],
    ) -> Result<u64, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let mut tx = self.db.write_tx();
        for t in txs {
            let bytes = postcard::to_allocvec(t)
                .map_err(|e| MetaError::Invalid(format!("delegate tx: {e}")))?;
            tx.insert(&self.backup_tail, tail_key(deleg_epoch(gen), t.idx), bytes);
        }
        tx.commit()?;
        self.deleg_backup_acked(gen, in_log)
    }

    /// The highest index through which every transaction of `gen` is
    /// either in the log as this replica holds it (`in_log`, its
    /// [`Meta::log_stream_idx`]) or held here, contiguously above it.
    ///
    /// Not contiguous from 1: a delegate re-sends only what it has not
    /// seen in the log (what it retired is gone from its journal), so a
    /// backup chosen mid-generation, or one that lost rows, could never
    /// be caught up that way — every answer short, every re-send the same
    /// suffix, the acknowledgements it gates waiting for a segment.
    pub fn deleg_backup_acked(&self, gen: u64, in_log: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        let e = deleg_epoch(gen);
        let mut acked = in_log;
        let from = in_log.saturating_add(1);
        for guard in r.range(&self.backup_tail, tail_key(e, from)..=tail_key(e, u64::MAX)) {
            let (k, _) = guard.into_inner()?;
            let (_, idx) = decode_tail_key(&k)?;
            if idx == acked + 1 {
                acked = idx;
            } else {
                break;
            }
        }
        Ok(acked)
    }

    /// Whether any transaction of `gen` is held here.
    pub fn deleg_backup_holds_any(&self, gen: u64) -> Result<bool, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        let e = deleg_epoch(gen);
        let mut it = r.range(&self.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX));
        match it.next() {
            Some(guard) => {
                guard.into_inner()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Every transaction held of generation `gen`, in index order.
    pub fn deleg_backup_tail(&self, gen: u64) -> Result<Vec<super::DelegateTx>, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        let e = deleg_epoch(gen);
        let mut out = Vec::new();
        for guard in r.range(&self.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX)) {
            let (_, v) = guard.into_inner()?;
            out.push(
                crate::record::from_bytes_exact(&v)
                    .map_err(|e| MetaError::Invalid(format!("delegate tx: {e}")))?,
            );
        }
        Ok(out)
    }

    /// Seal generation `gen`: nothing more of it is acknowledged here.
    pub fn deleg_backup_seal(&self, gen: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        kv_set_tx(&mut tx, &self.local, &deleg_sealed_key(gen), "1");
        tx.commit()?;
        // M16: synced before the seal is acknowledged, like `backup_seal`.
        self.sync()
    }

    pub fn deleg_backup_sealed(&self, gen: u64) -> Result<bool, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        read_fault::check()?;
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_sealed_key(gen))?.is_some())
    }

    /// Plan 46 M1.2 step 7 (c3-spec D6): the previous roots the next root of
    /// `gen` still has to seal (node-local, sorted). Empty: none.
    pub fn deleg_prev_roots(&self, gen: u64) -> Result<Vec<u64>, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_prev_roots_key(gen))?
            .map(|v| v.split(',').filter_map(|n| n.parse().ok()).collect())
            .unwrap_or_default())
    }

    /// Persist the set [`Self::deleg_prev_roots`] reads (`nodes` empty removes it).
    pub fn set_deleg_prev_roots(&self, gen: u64, nodes: &[u64]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let key = deleg_prev_roots_key(gen);
        if nodes.is_empty() {
            tx.remove(&self.local, key.as_bytes().to_vec());
        } else {
            let v: Vec<String> = nodes.iter().map(|n| n.to_string()).collect();
            kv_set_tx(&mut tx, &self.local, &key, &v.join(","));
        }
        tx.commit()?;
        Ok(())
    }

    /// The generation ended (its `DelegateNodeEnded` is in the log): drop its rows.
    pub fn deleg_backup_clear(&self, gen: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let e = deleg_epoch(gen);
        let keys: Vec<Vec<u8>> = tx
            .range(&self.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX))
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            tx.remove(&self.backup_tail, k);
        }
        // The rows are in the log: the reads a deposition held for them go.
        self.release_slot_tx(&mut tx, e)?;
        tx.commit()?;
        self.flush_held_releases();
        Ok(())
    }
}

/// Tests only (`test-util`): fail this thread's next `n` reads of a
/// backup's holdings or seal (a delegation backup's: [`Meta::deleg_backup_holds_any`],
/// [`Meta::deleg_backup_tail`], [`Meta::deleg_backup_sealed`],
/// [`Meta::deleg_backup_append`]'s acknowledgement; the root's group:
/// [`Meta::backup_role`], [`Meta::backup_acked`], [`Meta::backup_tail`], [`Meta::backup_sealed_epoch`],
/// [`Meta::journal_has_epoch_rows`]; and [`Meta::deleg_group`]) with an I/O
/// error.
#[cfg(any(test, feature = "test-util"))]
pub fn fail_deleg_backup_reads_on_this_thread_for_tests(n: u32) {
    read_fault::SKIP.with(|c| c.set(0));
    read_fault::LEFT.with(|c| c.set(n));
}

/// [`fail_deleg_backup_reads_on_this_thread_for_tests`] after letting the
/// next `skip` of those reads through.
#[cfg(any(test, feature = "test-util"))]
pub fn fail_store_reads_after_on_this_thread_for_tests(skip: u32, n: u32) {
    read_fault::SKIP.with(|c| c.set(skip));
    read_fault::LEFT.with(|c| c.set(n));
}

/// The test hook's check, for the store's other reads it covers.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn injected_read_fault() -> Result<(), MetaError> {
    read_fault::check()
}

#[cfg(any(test, feature = "test-util"))]
mod read_fault {
    use crate::MetaError;

    thread_local! {
        pub(super) static LEFT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        pub(super) static SKIP: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    pub(super) fn check() -> Result<(), MetaError> {
        let skipped = SKIP.with(|c| {
            let n = c.get();
            c.set(n.saturating_sub(1));
            n > 0
        });
        if skipped {
            return Ok(());
        }
        LEFT.with(|c| {
            let n = c.get();
            c.set(n.saturating_sub(1));
            if n > 0 {
                Err(MetaError::Io(std::io::Error::other(
                    "injected backup read fault (testing only)",
                )))
            } else {
                Ok(())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutate::{execute, MutateOp};
    use crate::Rid;
    use constellation_fs_core::types::ROOT_INO;

    fn create(meta: &Meta, name: &str, seq: u64) {
        let ino = meta.allocate_ino(ROOT_INO).unwrap();
        execute(
            meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: name.into(),
                ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(Rid {
                node: 1,
                incarnation: 1,
                seq,
            }),
        )
        .unwrap();
    }

    /// ADR-43: a generation number is `epoch << 32 | n`, so a delegate's tail
    /// slot `DELEG_BACKUP_BASE + gen` runs far past `2^48`: it must never
    /// land on a member copy's slot (`MEMBER_COPY_BASE + epoch`), whichever
    /// epoch minted it, and neither space's clear may reach the other's rows.
    #[test]
    fn deleg_backup_slot_never_meets_a_member_copy_slot() {
        use crate::delegation::{GEN_EPOCH_SHIFT, GEN_MAX_EPOCH};
        // The bounds: the delegates' slots start past every member copy and
        // the highest mintable number still fits.
        let top = (GEN_MAX_EPOCH << GEN_EPOCH_SHIFT) | u64::from(u32::MAX);
        const {
            assert!(DELEG_BACKUP_BASE > 3 << 48, "past every member copy slot");
        }
        assert!(DELEG_BACKUP_BASE
            .checked_add(top)
            .is_some_and(|s| s < u64::MAX));
        // The epoch that, in the old `1 << 48` layout, put its first number on
        // the member copy of epoch 1.
        let gen = (1u64 << 16 << GEN_EPOCH_SHIFT) | 1;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        create(&meta, "a", 1);
        let txs = meta.journal_txs_from(1, 1000).unwrap();
        meta.member_copy_put(1, 2, &txs).unwrap();
        let dtx = |idx| crate::store::DelegateTx {
            idx,
            rid: None,
            records: Vec::new(),
            deps: Default::default(),
        };
        meta.deleg_backup_append(gen, 0, &[dtx(1), dtx(2)]).unwrap();
        meta.deleg_backup_append(gen + 1, 0, &[dtx(1)]).unwrap();
        // Ending the generation drops its rows only; the member copy stays.
        meta.deleg_backup_clear(gen).unwrap();
        assert!(!meta.deleg_backup_holds_any(gen).unwrap());
        assert!(meta.deleg_backup_holds_any(gen + 1).unwrap());
        assert_eq!(meta.member_copy(1).unwrap().unwrap().1, txs);
        // The holder's tail clear reaches neither.
        meta.backup_clear().unwrap();
        assert!(meta.deleg_backup_holds_any(gen + 1).unwrap());
        assert_eq!(meta.member_copy(1).unwrap().unwrap().1, txs);
        // Dropping the member copy leaves the delegate's rows.
        meta.member_copy_drop(1).unwrap();
        assert!(meta.deleg_backup_holds_any(gen + 1).unwrap());
    }

    /// Plan 43 C1-3: the member copy (a sealed holder's rows) lives apart
    /// from the backup tail — neither a trim nor a clear of the tail reaches
    /// it — survives a restart, and goes only when dropped.
    #[test]
    fn the_member_copy_snapshot_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("m.db");
        let txs = {
            let meta = Meta::open(&db).unwrap();
            meta.set_holder_epoch(1);
            create(&meta, "a", 1);
            create(&meta, "b", 2);
            let txs = meta.journal_txs_from(1, 1000).unwrap();
            meta.member_copy_put(1, 2, &txs).unwrap();
            // A backup's tail of the same epoch number is another thing:
            // trimmed, cleared, it leaves the copy alone.
            meta.backup_append(1, &txs).unwrap();
            meta.backup_trim(1, u64::MAX, &[]).unwrap();
            meta.backup_clear().unwrap();
            assert!(meta.backup_tail(1).unwrap().is_empty());
            assert_eq!(meta.member_copy_epochs().unwrap(), vec![1]);
            meta.close().unwrap();
            txs
        };
        let meta = Meta::open(&db).unwrap();
        let (info, rows) = meta.member_copy(1).unwrap().expect("kept");
        assert_eq!(rows, txs);
        assert_eq!(
            (info.config_version, info.through),
            (2, txs.last().unwrap().last)
        );
        assert_eq!(meta.member_copy(2).unwrap(), None);
        // A second put (the same seal asked again) adds, never loses.
        meta.member_copy_put(1, 2, &txs[1..]).unwrap();
        assert_eq!(meta.member_copy(1).unwrap().unwrap().1, txs);
        meta.member_copy_drop(1).unwrap();
        assert_eq!(meta.member_copy(1).unwrap(), None);
        assert!(meta.member_copy_epochs().unwrap().is_empty());
    }

    #[test]
    fn a_holder_streams_whole_transactions_from_a_boundary() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        create(&meta, "a", 1);
        create(&meta, "b", 2);
        let txs = meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].first, 1);
        assert_eq!(txs[1].first, txs[0].last + 1);
        assert!(txs[1]
            .records
            .iter()
            .any(|r| matches!(r, LogRecord::Completed { rid } if rid.seq == 2)));
        assert_eq!(meta.journal_tip().unwrap(), txs[1].last);
        // From the second boundary: only the second transaction.
        let rest = meta.journal_txs_from(txs[1].first, 1000).unwrap();
        assert_eq!(rest, vec![txs[1].clone()]);
    }

    /// Plan 30 §M9 round 2: the holder pipelines appends, so a batch can
    /// be persisted ahead of a gap; what a restart reports as held stops
    /// at the gap (the holder would otherwise never fill it).
    #[test]
    fn what_a_backup_holds_is_the_contiguous_prefix() {
        let holder = Meta::open_in_memory().unwrap();
        holder.set_holder_epoch(3);
        for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
            create(&holder, name, i as u64 + 1);
        }
        let txs = holder.journal_txs_from(1, 1000).unwrap();
        let backup = Meta::open_in_memory().unwrap();
        // The first and the third and fourth transactions, not the second.
        backup.backup_append(3, &txs[0..1]).unwrap();
        backup.backup_append(3, &txs[2..4]).unwrap();
        assert_eq!(backup.backup_acked(3).unwrap(), txs[0].last);
        backup.backup_append(3, &txs[1..2]).unwrap();
        assert_eq!(backup.backup_acked(3).unwrap(), txs[3].last);
    }

    /// ADR-59 part A (metered-shared+links seed 1479): a backup that held a
    /// holder's transactions is sent them again from the first boundary (the
    /// holder restarted) after two of them shipped ahead of a held one: the
    /// resend carries them as one hole. The hole supersedes both copies, so
    /// the hold stays contiguous through the tip, and a late copy of a row
    /// the hole spans is not kept. (Before: the second copy, keyed inside
    /// the hole, ended the hold at the hole, and the takeover dropped the
    /// acknowledged rows past it.)
    #[test]
    fn a_resent_hole_supersedes_the_copies_it_spans() {
        let holder = Meta::open_in_memory().unwrap();
        holder.set_holder_epoch(3);
        for (i, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            create(&holder, name, i as u64 + 1);
        }
        let txs = holder.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 5);
        let backup = Meta::open_in_memory().unwrap();
        backup.backup_append(3, &txs).unwrap();
        assert_eq!(backup.backup_acked(3).unwrap(), txs[4].last);
        // The second and third ship ahead and leave the journal.
        let rows: Vec<u64> = (txs[1].first..=txs[2].last).collect();
        holder.ack_journal_rows_at(&rows, 7).unwrap();
        let resent = holder.journal_txs_from(1, 1000).unwrap();
        assert_eq!(resent.len(), 4);
        assert!(resent[1].records.is_empty());
        assert_eq!(
            (resent[1].first, resent[1].last),
            (txs[1].first, txs[2].last)
        );
        backup.backup_append(3, &resent).unwrap();
        assert_eq!(backup.backup_acked(3).unwrap(), txs[4].last);
        assert_eq!(backup.backup_tail(3).unwrap(), resent);
        // A late copy of either spanned transaction changes nothing.
        backup.backup_append(3, &txs[2..3]).unwrap();
        backup.backup_append(3, &txs[1..2]).unwrap();
        assert_eq!(backup.backup_tail(3).unwrap(), resent);
        assert_eq!(backup.backup_acked(3).unwrap(), txs[4].last);
    }

    /// Review 3 (N4): the shape main reaches. The backup already trimmed the
    /// two shipped-ahead transactions into two holes of their own, and the
    /// resend carries them as one: the merged hole supersedes both, and the
    /// hold stays contiguous through the tip.
    #[test]
    fn a_resent_hole_supersedes_the_trim_holes_it_spans() {
        let holder = Meta::open_in_memory().unwrap();
        holder.set_holder_epoch(3);
        for (i, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            create(&holder, name, i as u64 + 1);
        }
        let txs = holder.journal_txs_from(1, 1000).unwrap();
        let backup = Meta::open_in_memory().unwrap();
        backup.backup_append(3, &txs).unwrap();
        // The segment shipping the second and third ahead lands here.
        backup
            .backup_trim(3, 0, &[txs[1].last, txs[2].last])
            .unwrap();
        let tail = backup.backup_tail(3).unwrap();
        assert!(tail[1].records.is_empty() && tail[2].records.is_empty());
        assert_eq!(backup.backup_acked(3).unwrap(), txs[4].last);
        let rows: Vec<u64> = (txs[1].first..=txs[2].last).collect();
        holder.ack_journal_rows_at(&rows, 7).unwrap();
        let resent = holder.journal_txs_from(1, 1000).unwrap();
        assert_eq!(resent.len(), 4);
        backup.backup_append(3, &resent).unwrap();
        assert_eq!(backup.backup_tail(3).unwrap(), resent);
        assert_eq!(backup.backup_acked(3).unwrap(), txs[4].last);
    }

    /// Review 3 (S3): a hole that begins inside a stored one (a resend from a
    /// boundary inside a span the journal no longer has, metered-shared+links
    /// seeds 199 and 229 in a debug build) merges with it, and the hold stays
    /// contiguous past both. Before, it was keyed inside the stored hole and
    /// the hold ended at the stored hole's end.
    #[test]
    fn a_hole_that_begins_inside_a_stored_hole_merges_with_it() {
        let holder = Meta::open_in_memory().unwrap();
        holder.set_holder_epoch(3);
        create(&holder, "a", 1);
        let real = holder.journal_txs_from(1, 1000).unwrap().remove(0);
        let (a, z) = (real.last + 1, real.last + 4);
        let hole = |first, last| BackupTx {
            first,
            last,
            records: Vec::new(),
            origin: (0, 0),
        };
        let mut next = real.clone();
        next.first = z + 3;
        next.last = z + 3;
        let backup = Meta::open_in_memory().unwrap();
        backup
            .backup_append(3, &[real.clone(), hole(a, z)])
            .unwrap();
        backup
            .backup_append(3, &[hole(a + 2, z + 2), next.clone()])
            .unwrap();
        assert_eq!(
            backup.backup_tail(3).unwrap(),
            vec![real, hole(a, z + 2), next.clone()]
        );
        assert_eq!(backup.backup_acked(3).unwrap(), next.last);
    }

    /// Round 2: rows the journal no longer has stream as holes, and a
    /// row outside any transaction head streams as its own transaction —
    /// what a backup receives is contiguous from the boundary it asked
    /// for.
    #[test]
    fn a_holder_streams_holes_and_headless_rows_contiguously() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        create(&meta, "a", 1);
        create(&meta, "b", 2);
        create(&meta, "c", 3);
        let txs = meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 3);
        // A row appended outside the local-transaction bracket.
        {
            let mut tx = meta.db.write_tx();
            journal::append_tx(
                &mut tx,
                &meta.journal_ks,
                &meta.local,
                &meta.completed,
                &LogRecord::Atime {
                    ino: 1,
                    atime_ns: 1,
                    time_ns: 1,
                },
            )
            .unwrap();
            tx.commit().unwrap();
        }
        let tip = meta.journal_tip().unwrap();
        assert_eq!(tip, txs[2].last + 1);
        // The second transaction's rows ship out of order and go.
        let rows: Vec<u64> = (txs[1].first..=txs[1].last).collect();
        meta.ack_journal_rows_at(&rows, 7).unwrap();
        let streamed = meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(streamed.len(), 4);
        assert_eq!(streamed[0], txs[0]);
        assert_eq!(
            streamed[1],
            BackupTx {
                first: txs[1].first,
                last: txs[1].last,
                records: Vec::new(),
                origin: (0, 0),
            },
            "the shipped rows are a hole"
        );
        assert_eq!(streamed[2], txs[2]);
        assert_eq!((streamed[3].first, streamed[3].last), (tip, tip));
        assert!(matches!(streamed[3].records[..], [LogRecord::Atime { .. }]));
        // From inside the hole: the hole's remainder first.
        let rest = meta.journal_txs_from(txs[1].last, 1000).unwrap();
        assert_eq!((rest[0].first, rest[0].last), (txs[1].last, txs[1].last));
        assert!(rest[0].records.is_empty());
        assert_eq!(rest[1], txs[2]);
        // The row cap never splits a transaction, and stops at one.
        let capped = meta.journal_txs_from(1, 1).unwrap();
        assert_eq!(capped, vec![txs[0].clone()]);
    }

    #[test]
    fn a_trim_scans_from_the_floor_and_a_resend_lowers_it() {
        let holder = Meta::open_in_memory().unwrap();
        holder.set_holder_epoch(3);
        for (i, n) in ["a", "b", "c", "d"].iter().enumerate() {
            create(&holder, n, i as u64 + 1);
        }
        let txs = holder.journal_txs_from(1, 1000).unwrap();
        let backup = Meta::open_in_memory().unwrap();
        // The later batches land first; a trim below them removes
        // nothing and sets the floor at the lowest live key.
        backup.backup_append(3, &txs[2..4]).unwrap();
        assert_eq!(backup.backup_trim(3, txs[1].last, &[]).unwrap(), 0);
        assert_eq!(
            *backup.backup_tail_floor.lock().unwrap(),
            Some((3, txs[2].first))
        );
        // The resend of the earlier batches lands below the floor, and
        // the floor follows: the next trim still finds them.
        backup.backup_append(3, &txs[0..2]).unwrap();
        assert_eq!(
            *backup.backup_tail_floor.lock().unwrap(),
            Some((3, txs[0].first))
        );
        assert_eq!(backup.backup_trim(3, txs[1].last, &[]).unwrap(), 2);
        assert_eq!(backup.backup_tail(3).unwrap(), txs[2..4].to_vec());
        assert_eq!(
            *backup.backup_tail_floor.lock().unwrap(),
            Some((3, txs[2].first))
        );
        // A newer epoch's trim clears the older epoch from the floor up.
        assert_eq!(backup.backup_trim(4, 0, &[]).unwrap(), 2);
        assert!(backup.backup_tail(3).unwrap().is_empty());
        assert_eq!(*backup.backup_tail_floor.lock().unwrap(), Some((4, 1)));
        // Nothing live, nothing to scan: a trim of an older epoch than
        // the floor's is a no-op, and a clear forgets the floor.
        assert_eq!(backup.backup_trim(3, u64::MAX, &[]).unwrap(), 0);
        backup.backup_clear().unwrap();
        assert_eq!(*backup.backup_tail_floor.lock().unwrap(), None);
    }

    #[test]
    fn a_backup_persists_trims_by_the_log_and_seals() {
        let holder = Meta::open_in_memory().unwrap();
        holder.set_holder_epoch(3);
        create(&holder, "a", 1);
        create(&holder, "b", 2);
        create(&holder, "c", 3);
        let txs = holder.journal_txs_from(1, 1000).unwrap();
        let backup = Meta::open_in_memory().unwrap();
        backup.backup_append(3, &txs).unwrap();
        assert_eq!(backup.backup_acked(3).unwrap(), txs[2].last);
        assert_eq!(backup.backup_tail(3).unwrap(), txs);
        // The segment carrying the first two transactions' rows trims
        // them; the third stays.
        let rows: Vec<u64> = (txs[0].first..=txs[1].last).collect();
        assert_eq!(backup.backup_trim(3, txs[1].last, &rows).unwrap(), 2);
        assert_eq!(backup.backup_tail(3).unwrap(), vec![txs[2].clone()]);
        // A segment of a newer epoch trims everything older.
        assert_eq!(backup.backup_trim(4, 0, &[]).unwrap(), 1);
        assert!(backup.backup_tail(3).unwrap().is_empty());
        // Role and seal persist.
        backup
            .set_backup_role(BackupRole {
                holder: 7,
                epoch: 3,
                config_version: 2,
            })
            .unwrap();
        assert_eq!(backup.backup_role().unwrap().map(|r| r.holder), Some(7));
        assert_eq!(backup.backup_sealed_epoch().unwrap(), 0);
        backup.backup_seal(3).unwrap();
        backup.backup_seal(2).unwrap();
        assert_eq!(backup.backup_sealed_epoch().unwrap(), 3);
        backup.backup_clear().unwrap();
        assert!(backup.backup_role().unwrap().is_none());
    }

    fn deleg_tx(idx: u64) -> crate::DelegateTx {
        crate::DelegateTx {
            idx,
            rid: None,
            records: Vec::new(),
            deps: Default::default(),
        }
    }

    /// long-delegated-backup seed 70232: a node that stops being the
    /// holder's backup discards the holder's tail, never the delegate
    /// streams it backs.
    #[test]
    fn clearing_the_holder_tail_keeps_a_delegates_backup_rows() {
        let backup = Meta::open_in_memory().unwrap();
        backup
            .backup_append(
                3,
                &[BackupTx {
                    first: 5,
                    last: 6,
                    records: Vec::new(),
                    origin: (0, 0),
                }],
            )
            .unwrap();
        let txs: Vec<_> = (1..=3).map(deleg_tx).collect();
        assert_eq!(backup.deleg_backup_append(9, 0, &txs).unwrap(), 3);
        backup.backup_clear().unwrap();
        assert!(backup.backup_tail(3).unwrap().is_empty());
        assert_eq!(backup.deleg_backup_tail(9).unwrap(), txs);
        assert_eq!(backup.deleg_backup_acked(9, 0).unwrap(), 3);
        assert!(backup.deleg_backup_holds_any(9).unwrap());
        assert!(!backup.deleg_backup_holds_any(8).unwrap());
    }

    /// The seal flag persists, and a read of it that fails is an error, not
    /// "not sealed" (the test hook fails the next read).
    #[test]
    fn a_delegate_backups_seal_flag_read_fails_as_an_error() {
        let backup = Meta::open_in_memory().unwrap();
        backup.deleg_backup_seal(9).unwrap();
        super::fail_deleg_backup_reads_on_this_thread_for_tests(1);
        assert!(backup.deleg_backup_sealed(9).is_err());
        assert!(backup.deleg_backup_sealed(9).unwrap());
        assert!(!backup.deleg_backup_sealed(8).unwrap());
    }

    /// A delegate re-sends only what it has not seen in the log: a backup
    /// that holds a suffix acknowledges it from what the log gives it,
    /// and never skips a gap above that.
    #[test]
    fn a_delegate_backup_acknowledges_from_the_log() {
        let backup = Meta::open_in_memory().unwrap();
        // Rows 1..=12 are in the log here; only 13 is held.
        assert_eq!(
            backup.deleg_backup_append(9, 12, &[deleg_tx(13)]).unwrap(),
            13
        );
        // Behind the log: rows 11 and 12 are neither held nor applied.
        assert_eq!(backup.deleg_backup_acked(9, 10).unwrap(), 10);
        // A gap above the log is never skipped.
        assert_eq!(
            backup.deleg_backup_append(9, 12, &[deleg_tx(15)]).unwrap(),
            13
        );
        assert_eq!(
            backup.deleg_backup_append(9, 12, &[deleg_tx(14)]).unwrap(),
            15
        );
        // The log past everything held.
        assert_eq!(backup.deleg_backup_acked(9, 20).unwrap(), 20);
    }
}

#[cfg(test)]
mod durability_tests {
    use crate::store::Meta;
    use constellation_types::clock::{BootNow, HonourMs, Ms, Now, WaitMs};

    /// M16: every write whose safety rests on being remembered across a
    /// power loss is synced before the caller acts on it — and a no-op
    /// write (nothing raised) costs no fsync.
    #[test]
    fn safety_writes_request_a_sync() {
        let meta = Meta::open_in_memory().unwrap();
        let synced = |before: u64| meta.sync_count() - before;

        let n = meta.sync_count();
        assert!(meta.promise_issue(10_000, Now::same(0)).unwrap());
        assert_eq!(synced(n), 1, "a new promise is synced before its PUT");
        let n = meta.sync_count();
        assert!(meta.promise_issue(5_000, Now::same(0)).unwrap());
        assert_eq!(synced(n), 0, "a promise that extends nothing");

        let n = meta.sync_count();
        assert!(meta.promise_join_begin(Now::same(20_000)).unwrap());
        assert_eq!(synced(n), 1, "the join gate");

        let n = meta.sync_count();
        meta.backup_seal(3).unwrap();
        assert_eq!(synced(n), 1, "a seal");
        let n = meta.sync_count();
        meta.backup_seal(2).unwrap();
        assert_eq!(synced(n), 0, "a lower seal changes nothing");

        let n = meta.sync_count();
        meta.deleg_backup_seal(7).unwrap();
        assert_eq!(synced(n), 1, "a delegate backup's seal");

        let n = meta.sync_count();
        meta.note_grant_horizon(WaitMs(50_000), Now::same(0))
            .unwrap();
        assert_eq!(synced(n), 1, "the read-grant horizon");

        let base = std::collections::BTreeMap::new();
        let n = meta.sync_count();
        meta.persist_epoch("e1", &[1, 2], &base, 7, "promised", None)
            .unwrap();
        assert_eq!(synced(n), 1, "an epoch promise");
        let n = meta.sync_count();
        meta.persist_epoch("e1", &[1, 2], &base, 7, "promised", None)
            .unwrap();
        assert_eq!(synced(n), 0, "an unchanged epoch row");
        meta.persist_epoch("e1", &[1, 2], &base, 7, "active", None)
            .unwrap();
        assert_eq!(synced(n), 1, "an epoch state change");
    }

    /// ADR-51: an epoch row records the carrier its activation resolved,
    /// keeps it through later state changes, and `epochs_possibly_carrying`
    /// selects the rows that contain the holder and carry that tenure or
    /// record none. Fails on the parent: no carrier is recorded.
    #[test]
    fn an_epoch_row_records_its_carrier_and_is_selected_by_it() {
        let meta = Meta::open_in_memory().unwrap();
        let base = std::collections::BTreeMap::new();
        // Promised: no carrier yet; no evidence for any tenure (S5).
        meta.persist_epoch("e1", &[1, 2, 3], &base, 7, "promised", None)
            .unwrap();
        assert!(meta.epochs_possibly_carrying(1, 5).unwrap().is_empty());
        // Activated: carries (1, 5).
        meta.persist_epoch("e1", &[1, 2, 3], &base, 7, "active", Some((1, 5)))
            .unwrap();
        // A later state change does not forget the carrier.
        meta.persist_epoch("e1", &[1, 2, 3], &base, 7, "closed", None)
            .unwrap();
        assert_eq!(
            meta.epochs_possibly_carrying(1, 5).unwrap(),
            vec![vec![1, 2, 3]]
        );
        assert!(meta.epochs_possibly_carrying(1, 6).unwrap().is_empty());
        assert!(meta.epochs_possibly_carrying(2, 5).unwrap().is_empty());
        // Another epoch: carries (4, 9) among {3, 4}; and an unrecorded one.
        meta.persist_epoch("e2", &[4, 3], &base, 8, "active", Some((4, 9)))
            .unwrap();
        meta.persist_epoch("e3", &[4, 2], &base, 9, "promised", None)
            .unwrap();
        assert_eq!(
            meta.epochs_possibly_carrying(4, 9).unwrap(),
            vec![vec![3, 4]],
            "e3 records no carrier: it carried nothing"
        );
        assert!(meta.epochs_possibly_carrying(4, 8).unwrap().is_empty());
    }

    /// ADR-51 (S5): an epoch row with no recorded carrier (promised only, or
    /// activated carrying nothing) is no evidence for any tenure of its
    /// holder: it never names a member a kept fetch has to wait for. Fails
    /// on the parent: such a row counts as possibly carrying any tenure.
    #[test]
    fn a_carrierless_epoch_row_is_no_evidence() {
        let meta = Meta::open_in_memory().unwrap();
        let base = std::collections::BTreeMap::new();
        meta.persist_epoch("e1", &[1, 2, 3], &base, 7, "promised", None)
            .unwrap();
        meta.persist_epoch("e2", &[1, 4], &base, 8, "closed", None)
            .unwrap();
        for epoch in 0..8 {
            assert!(
                meta.epochs_possibly_carrying(1, epoch).unwrap().is_empty(),
                "tenure epoch {epoch}"
            );
        }
        meta.persist_epoch("e2", &[1, 4], &base, 8, "active", Some((1, 3)))
            .unwrap();
        assert_eq!(
            meta.epochs_possibly_carrying(1, 3).unwrap(),
            vec![vec![1, 4]]
        );
    }

    /// ADR-51 (K4, keyed on the tenure): a sealed tenure is persisted and
    /// listed; the same holder's next tenure, and another holder's tenure at
    /// the same epoch, are not sealed. Fails on the parent: there is no
    /// tenure seal.
    #[test]
    fn a_sealed_tenure_is_listed_and_the_next_tenure_of_the_same_node_is_not() {
        let meta = Meta::open_in_memory().unwrap();
        assert!(meta.sealed_tenures().unwrap().is_empty());
        meta.seal_tenures(&[(2, 5), (3, 6), (2, 5)]).unwrap();
        assert_eq!(meta.sealed_tenures().unwrap(), vec![(2, 5), (3, 6)]);
        assert!(meta.tenure_sealed(2, 5).unwrap());
        assert!(meta.tenure_sealed(3, 6).unwrap());
        assert!(
            !meta.tenure_sealed(2, 6).unwrap(),
            "the next tenure of node 2"
        );
        assert!(!meta.tenure_sealed(3, 5).unwrap(), "another node's epoch 5");
        // The role seal is separate.
        assert_eq!(meta.backup_sealed_epoch().unwrap(), 0);
    }

    /// Plan 43 C7-H1 (R9): a promiser waits its promise out on its wait
    /// clock: a forward wall step does not let it join an epoch early.
    /// Fails on the parent: the gate compared the wall clock with the
    /// persisted wall value.
    #[test]
    fn the_join_gate_waits_the_promise_out_on_the_wait_clock() {
        let meta = Meta::open_in_memory().unwrap();
        let at = |wall: i64, wait: i64| Now {
            wall: Ms(wall),
            honour: HonourMs(wall),
            wait: WaitMs(wait),
            ..Now::same(0)
        };
        // Issued at wall 1_000 / wait 5_000 for 10 s.
        assert!(meta.promise_issue(10_000, at(1_000, 5_000)).unwrap());
        // The wall clock stepped 9 s forward: past the promise on the wall
        // clock, 1 s into it on the wait clock.
        assert!(!meta.promise_join_begin(at(10_000, 6_000)).unwrap());
        assert!(!meta.promise_join_begin(at(10_000, 14_999)).unwrap());
        assert!(meta.promise_join_begin(at(10_000, 15_000)).unwrap());
    }

    /// A reading on `boot`, the raw clock at `raw` ms since boot, the wait
    /// clock at `wait`, the wall at `wall`.
    fn at(boot: u64, raw: i64, wait: i64, wall: i64) -> Now {
        Now {
            wall: Ms(wall),
            honour: HonourMs(wall),
            wait: WaitMs(wait),
            boot: BootNow {
                id: boot,
                raw_ms: raw,
                budget_ppm: 0,
            },
        }
    }

    /// Plan 43 C7a (fail-before: the wall fallback of `promise_join_begin`):
    /// a promiser that restarts on the same boot waits its last promise out
    /// on the raw clock, whatever the wall clock did meanwhile. The promise
    /// is issued at raw 100 000 for 15 s; the process restarts at raw 103 000
    /// with the wall 1 h on: the gate holds for 12 s more, not for nothing.
    /// On the parent a restart read the persisted wall value against the
    /// stepped wall and joined at once.
    #[test]
    fn the_promise_join_waits_its_stamp_after_a_restart_and_a_forward_step() {
        const WALL: i64 = 1_780_000_000_000;
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Meta::open(dir.path()).unwrap();
            let t0 = at(7, 100_000, 5_000, WALL);
            assert!(meta.promise_issue(15_000, t0).unwrap());
            assert_eq!(meta.promise_issued().unwrap(), WALL + 15_000);
            // The persisted value carries the boot: the wall form first.
            let stored = meta.kv_get("epoch_promise_issued_ms").unwrap().unwrap();
            assert!(
                stored.starts_with(&format!("{} 15000 ", WALL + 15_000)),
                "{stored}"
            );
        }
        let restarted = |boot: u64, raw: i64, wait: i64, wall_step: i64| {
            let meta = Meta::open(dir.path()).unwrap();
            let now = at(boot, raw, wait, WALL + (raw - 100_000) + wall_step);
            // Loaded at start, as the core does.
            meta.load_promise_issued(now, 16_000);
            (meta, now)
        };
        // Same boot, the wall stepped forward 1 h while the node was down.
        let (meta, now) = restarted(7, 103_000, 400, 3_600_000);
        assert_eq!(
            meta.promise_issued().unwrap(),
            WALL + 15_000,
            "token 0 is the wall"
        );
        assert!(
            !meta.promise_join_begin(now).unwrap(),
            "12 s of the promise are left"
        );
        assert!(!meta.promise_join_begin(now.plus(11_999)).unwrap());
        assert!(meta.promise_join_begin(now.plus(12_000)).unwrap());
        drop(meta);
        // Another boot: the whole span from the start, a wall that stepped
        // back or forward changes nothing.
        for step in [-3_600_000, 3_600_000] {
            let dir_check = Meta::open(dir.path()).unwrap();
            dir_check.promise_join_end().unwrap();
            drop(dir_check);
            let (meta, now) = restarted(8, 2_000, 400, step);
            assert!(
                !meta.promise_join_begin(now.plus(14_999)).unwrap(),
                "{step}"
            );
            assert!(meta.promise_join_begin(now.plus(15_000)).unwrap(), "{step}");
            meta.promise_join_end().unwrap();
        }
    }

    /// Expectation: a promise an older build persisted (the wall value
    /// alone) is waited out as the caller's default span from the start,
    /// not as a wall value: here the wall says it is long past.
    #[test]
    fn an_old_format_promise_waits_the_default_span() {
        const WALL: i64 = 1_780_000_000_000;
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Meta::open(dir.path()).unwrap();
            meta.kv_set("epoch_promise_issued_ms", &(WALL - 3_600_000).to_string())
                .unwrap();
        }
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 100_000, 400, WALL);
        assert_eq!(meta.promise_issued().unwrap(), WALL - 3_600_000);
        assert_eq!(meta.load_promise_issued(now, 16_000), Some(WaitMs(16_400)));
        assert!(!meta.promise_join_begin(now.plus(15_999)).unwrap());
        assert!(meta.promise_join_begin(now.plus(16_000)).unwrap());
    }
}
