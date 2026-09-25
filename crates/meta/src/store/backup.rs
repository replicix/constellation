//! Plan 30 §M9: the backup tail — what a synchronous backup holds for
//! the holder it backs, and what a holder reads from its journal to
//! stream to its backups.
//!
//! # The `backup_tail` keyspace
//!
//! `(epoch BE, first journal seq BE) → BackupTx { last, records }`: one
//! row per holder journal transaction the backup has persisted and
//! acknowledged. A backup's acknowledgement means exactly "these rows are
//! durable here", so `backup_append` commits before the core answers.
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
//! holds.

use super::journal;
use super::local::{tx_key, JournalTxHead};
use super::{kv_get_tx, kv_set_tx, Meta};
use crate::error::MetaError;
use crate::record::LogRecord;
use fjall::Readable;
use serde::{Deserialize, Serialize};

const KV_BACKUP_ROLE: &str = "backup_role";
const KV_BACKUP_SEALED: &str = "backup_sealed";

/// One holder journal transaction as streamed to a backup: its rows
/// `first..=last` and their records, in order. Whole transactions only
/// (plan 30 §M3b: an op's records and its `Completed` never split), so a
/// backup can re-execute one as a unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupTx {
    pub first: u64,
    pub last: u64,
    pub records: Vec<LogRecord>,
}

/// Whom this node backs (persisted).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupRole {
    pub holder: u64,
    pub epoch: u64,
    pub config_version: u64,
}

fn tail_key(epoch: u64, first: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(16);
    k.extend_from_slice(&epoch.to_be_bytes());
    k.extend_from_slice(&first.to_be_bytes());
    k
}

fn decode_tail_key(k: &[u8]) -> Result<(u64, u64), MetaError> {
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
                });
            }
            let last = match r.get(&self.journal_tx, tx_key(seq))? {
                Some(h) => postcard::from_bytes::<JournalTxHead>(&h)?.last.max(seq),
                None => seq,
            };
            rows += 1;
            cur = Some(BackupTx {
                first: seq,
                last,
                records: vec![rec],
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
        let mut tx = self.db.write_tx();
        for t in txs {
            tx.insert(
                &self.backup_tail,
                tail_key(epoch, t.first),
                postcard::to_allocvec(t)?,
            );
        }
        tx.commit()?;
        // A resend can land below the floor (from the contiguous
        // acknowledgement, under a batch held ahead of a gap).
        if let Some(lowest) = txs.iter().map(|t| t.first).min() {
            let mut floor = self.backup_tail_floor.lock().unwrap();
            if let Some(f) = *floor {
                *floor = Some(f.min((epoch, lowest)));
            }
        }
        Ok(())
    }

    /// The highest `last` held for `epoch` (0: nothing).
    pub fn backup_acked(&self, epoch: u64) -> Result<u64, MetaError> {
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
            let t: BackupTx = postcard::from_bytes(&v)?;
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
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.range(
            &self.backup_tail,
            tail_key(epoch, 0)..=tail_key(epoch, u64::MAX),
        ) {
            let (_, v) = guard.into_inner()?;
            out.push(postcard::from_bytes(&v)?);
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
            let t: BackupTx = postcard::from_bytes(&v)?;
            if t.last <= through || row_set.contains(&t.last) {
                doomed.push(k.to_vec());
            } else {
                survivor = Some(survivor.map_or((e, first), |s| s.min((e, first))));
            }
        }
        // Every live key of `epoch` (and no older epoch) is at or above
        // the lowest survivor; with none, a later append begins above
        // what the holder shipped through.
        *floor = Some(survivor.unwrap_or((epoch, through.saturating_add(1))));
        let n = doomed.len();
        if n == 0 {
            tx.rollback();
            return Ok(0);
        }
        for k in doomed {
            tx.remove(&self.backup_tail, k);
        }
        tx.commit()?;
        Ok(n)
    }

    /// Forget every tail row (the holder reconfigured this node out, the
    /// lease moved on, or the tail has been re-shipped).
    pub fn backup_clear(&self) -> Result<(), MetaError> {
        *self.backup_tail_floor.lock().unwrap() = None;
        let mut tx = self.db.write_tx();
        let keys: Vec<Vec<u8>> = tx
            .range(&self.backup_tail, tail_key(0, 0)..)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            tx.remove(&self.backup_tail, k);
        }
        tx.remove(&self.local, KV_BACKUP_ROLE.as_bytes().to_vec());
        tx.commit()?;
        Ok(())
    }

    pub fn backup_role(&self) -> Result<Option<BackupRole>, MetaError> {
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
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, KV_BACKUP_SEALED)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    /// Seal `epoch`: persisted before the caller refuses its first append.
    pub fn backup_seal(&self, epoch: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let current: u64 = kv_get_tx(&tx, &self.local, KV_BACKUP_SEALED)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if epoch > current {
            kv_set_tx(&mut tx, &self.local, KV_BACKUP_SEALED, &epoch.to_string());
        }
        tx.commit()?;
        Ok(())
    }
}

// ------------------------------------------- plan 30 §M11 phase 2b: delegate backups

/// The `backup_tail` key space of a delegate's stream: `epoch` slots
/// above this base hold generation `epoch - base` (the M9 tail keys are
/// lease epochs, far below).
const DELEG_BACKUP_BASE: u64 = 1 << 48;

fn deleg_epoch(gen: u64) -> u64 {
    DELEG_BACKUP_BASE + gen
}

fn deleg_sealed_key(gen: u64) -> String {
    format!("deleg_backup_sealed:{gen}")
}

impl Meta {
    /// A delegate's transactions `txs` of generation `gen`, persisted by
    /// its backup (idempotent: a retransmission rewrites the same keys).
    /// Returns the highest contiguous index held from 1.
    pub fn deleg_backup_append(
        &self,
        gen: u64,
        txs: &[super::DelegateTx],
    ) -> Result<u64, MetaError> {
        let mut tx = self.db.write_tx();
        for t in txs {
            let bytes = postcard::to_allocvec(t)
                .map_err(|e| MetaError::Invalid(format!("delegate tx: {e}")))?;
            tx.insert(&self.backup_tail, tail_key(deleg_epoch(gen), t.idx), bytes);
        }
        tx.commit()?;
        self.deleg_backup_acked(gen)
    }

    /// The highest index held contiguously from 1 (0: nothing).
    pub fn deleg_backup_acked(&self, gen: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        let e = deleg_epoch(gen);
        let mut acked = 0u64;
        for guard in r.range(&self.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX)) {
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

    /// Every transaction held of generation `gen`, in index order.
    pub fn deleg_backup_tail(&self, gen: u64) -> Result<Vec<super::DelegateTx>, MetaError> {
        let r = self.db.read_tx();
        let e = deleg_epoch(gen);
        let mut out = Vec::new();
        for guard in r.range(&self.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX)) {
            let (_, v) = guard.into_inner()?;
            out.push(
                postcard::from_bytes(&v)
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
        Ok(())
    }

    pub fn deleg_backup_sealed(&self, gen: u64) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_sealed_key(gen))?.is_some())
    }

    /// The generation ended (its `Recall` is in the log): drop its rows.
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
        tx.commit()?;
        Ok(())
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
                records: Vec::new()
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
}
