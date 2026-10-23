//! Plan 43 C1-3/C1-4: member copies — the rows a node holds as a member of a
//! group it no longer sequences for, kept outside its journal until the
//! archive carries them.
//!
//! # What a member copy is
//!
//! A row a client was told is committed (its group, or the archive, held it)
//! is committed whoever sequenced it. When the node that sequenced it stops
//! (deposed, sealed by a claimant, or a backup whose holder's lease moved
//! on) the row must not be re-executed by that node's replay — its
//! successor re-ships it from the group — and must not be forgotten either:
//! until the log carries it, this node's copy may be the last one. So the
//! rows move here, in the key range `MEMBER_COPY_BASE + epoch` of the
//! `backup_tail` keyspace (apart from the tail this node holds as a backup:
//! [`Meta::backup_trim`] drops every older epoch of that tail and
//! [`Meta::backup_clear`] wipes it, both on events unrelated to a member
//! copy), with `local["member_copy:{epoch}"]` = `config_version:through`.
//! A claimant that seals the epoch (`SealTail`) is answered from here.
//!
//! # When a row leaves (plan 43 §5.6, the drop rules)
//!
//! All of them are applied where the log reaches this node
//! ([`member_rules_tx`], inside `apply_segment_rows`'s transaction, and for
//! the node's own shipped rows in `ack_rows_tx`):
//!
//! 1. **Coverage.** A segment that carries `TailOf { epoch, jseq }` — a
//!    re-shipped transaction's identity — retires the row `(epoch, jseq)`;
//!    a segment of the copy's own epoch retires the rows it ships (named by
//!    its `rows`, or at or below its `through`).
//! 2. `TailFollowsThrough { prev_epoch, through }`: rows past `through` were
//!    never committed (the sealer's contiguous hold did not reach them).
//! 3. `GroupAbandoned { epoch, gen: 0 }`: the operator gave the group up; the
//!    rows that are not the lone branch's go to the replay queue (the only
//!    way a committed row is re-executed).
//! 4. The first segment of a newer epoch that announces neither a tail of
//!    this epoch nor an abandon drops the rows no one acknowledged
//!    (`Archive`, `Lone`); a `RootGroup` row stays until coverage — it may be
//!    the inherited tail of a tenure that has not shipped it yet (D2).

use super::backup::tail_key;
use super::local::RowClass;
use super::{kv_get_tx, kv_set_tx, Meta};
use crate::error::MetaError;
use crate::locks::LockTag;
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::rid::Rid;
use crate::BackupTx;
use fjall::{Readable, SingleWriterWriteTx};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// The `backup_tail` key space of the member copies: `epoch` slots above this
/// base hold the copy of lease epoch `epoch - base`.
const MEMBER_COPY_BASE: u64 = 2 << 48;

fn member_epoch(epoch: u64) -> u64 {
    MEMBER_COPY_BASE + epoch
}

fn member_info_key(epoch: u64) -> String {
    format!("member_copy:{epoch}")
}

// What the log has told this replica about an epoch's tail, kept apart from
// any member copy so that a copy made *after* the log said it (a deposed
// holder that restarts long after its successor re-shipped, a backup that
// reads the lease of a later epoch behind its log) is judged by it:
// - `member_shipped:{e}`: the journal seq epoch `e`'s own segments shipped
//   through (the last applied position of the epoch, kept when a segment of
//   a later epoch is the first applied);
// - `member_reship:{e}`: the highest journal seq of epoch `e` a re-shipped
//   transaction named (`TailOf`), in the log (a tail is re-journaled in
//   order, so everything of the epoch at or below it is carried);
// - `member_through:{e}`: how far the sealer's hold of epoch `e` reached
//   (`TailFollowsThrough`): a row past it was never committed.
fn shipped_key(epoch: u64) -> String {
    format!("member_shipped:{epoch}")
}
fn reship_key(epoch: u64) -> String {
    format!("member_reship:{epoch}")
}
fn through_key(epoch: u64) -> String {
    format!("member_through:{epoch}")
}

fn kv_u64(r: &impl Readable, meta: &Meta, key: &str) -> Result<Option<u64>, MetaError> {
    Ok(kv_get_tx(r, &meta.local, key)?.and_then(|s| s.parse().ok()))
}

fn kv_raise(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    key: &str,
    value: u64,
) -> Result<(), MetaError> {
    if kv_u64(&*tx, meta, key)?.is_none_or(|old| value > old) {
        kv_set_tx(tx, &meta.local, key, &value.to_string());
    }
    Ok(())
}

/// Remember what a segment the log delivered says of an epoch's tail
/// (module doc, above): `prev_applied` is the journal position this replica
/// had applied before this segment, kept as the end of its epoch when this
/// segment starts a later one.
pub(crate) fn note_log_tail_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    epoch: u64,
    records: &[LogRecord],
    prev_applied: Option<crate::session::JournalPos>,
) -> Result<(), MetaError> {
    if let Some(prev) = prev_applied.filter(|p| p.epoch < epoch) {
        kv_raise(tx, meta, &shipped_key(prev.epoch), prev.jseq)?;
    }
    for rec in records {
        match rec {
            LogRecord::TailOf { epoch, jseq } => kv_raise(tx, meta, &reship_key(*epoch), *jseq)?,
            LogRecord::TailFollowsThrough {
                prev_epoch,
                through,
            } => kv_raise(tx, meta, &through_key(*prev_epoch), *through)?,
            _ => {}
        }
    }
    Ok(())
}

/// Whether the log says a row of `epoch`'s tail needs no copy: it is carried
/// (shipped by its own epoch, or named by a re-ship), or it was never
/// committed (past the sealer's hold).
fn log_settled_tx(
    r: &impl Readable,
    meta: &Meta,
    epoch: u64,
    row: &MemberTx,
) -> Result<bool, MetaError> {
    let (first, last) = (row.tx.first, row.tx.last);
    if kv_u64(r, meta, &shipped_key(epoch))?.is_some_and(|s| last <= s) {
        return Ok(true);
    }
    let (ie, ij) = tail_identity(&row.tx.records).unwrap_or((epoch, first));
    if kv_u64(r, meta, &through_key(ie))?.is_some_and(|t| ij > t) {
        return Ok(true);
    }
    Ok(kv_u64(r, meta, &reship_key(ie))?.is_some_and(|m| ij <= m))
}

/// Whether the log this replica applied carries row `ident` of `epoch`'s
/// tenure, ending at `last`: shipped by its own epoch (at or below the
/// applied position of that epoch, or of `member_shipped`), or re-shipped by
/// a successor (`member_reship` reaches its identity). A committed requester
/// or subscriber entry a namespace rebuild finds is retired by it rather than
/// redone over a namespace that holds it already.
pub(crate) fn log_carries_tx(
    r: &impl Readable,
    meta: &Meta,
    epoch: u64,
    ident: (u64, u64),
    last: u64,
) -> Result<bool, MetaError> {
    if super::applied_pos_at(r, &meta.local).is_some_and(|p| p.epoch == epoch && p.jseq >= last) {
        return Ok(true);
    }
    if kv_u64(r, meta, &shipped_key(epoch))?.is_some_and(|s| last <= s) {
        return Ok(true);
    }
    Ok(kv_u64(r, meta, &reship_key(ident.0))?.is_some_and(|m| ident.1 <= m))
}

/// Take over what the log told `side` of every epoch's tail (module doc's
/// `member_shipped`/`member_reship`/`member_through`, each raised, never
/// lowered): a namespace rebuilt from `side` is a replica that applied that
/// log.
pub(crate) fn adopt_log_tail_facts_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    side_snap: &impl Readable,
    side: &Meta,
) -> Result<(), MetaError> {
    let mut facts: Vec<(String, u64)> = Vec::new();
    for prefix in ["member_shipped:", "member_reship:", "member_through:"] {
        for guard in side_snap.prefix(&side.local, prefix.as_bytes()) {
            let (k, v) = guard.into_inner()?;
            let (Ok(k), Some(v)) = (
                String::from_utf8(k.to_vec()),
                std::str::from_utf8(&v)
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok()),
            ) else {
                continue;
            };
            facts.push((k, v));
        }
    }
    for (k, v) in facts {
        kv_raise(tx, meta, &k, v)?;
    }
    Ok(())
}

/// One transaction of a member copy: the journal transaction as the group
/// held it, and what an abandon needs to queue it as the lone branch would.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemberTx {
    pub tx: BackupTx,
    /// What the row's answer waited for. A transaction that came from a
    /// backup's tail was held by the whole group: `RootGroup`.
    pub class: RowClass,
    pub rid: Option<Rid>,
    pub op: Option<MutateOp>,
    pub lock_tag: LockTag,
}

impl MemberTx {
    /// A transaction a backup held (the group held it): `RootGroup`.
    pub fn from_backup(tx: BackupTx) -> MemberTx {
        MemberTx {
            tx,
            class: RowClass::RootGroup,
            rid: None,
            op: None,
            lock_tag: LockTag::NONE,
        }
    }
}

/// What a member copy records besides its rows: the configuration version
/// the answer was given for and the last journal seq of the copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemberCopyInfo {
    pub config_version: u64,
    pub through: u64,
}

/// The identity of a re-shipped transaction: the `(epoch, jseq)` of its
/// first record if that is a `TailOf` (plan 43 C1-4).
pub fn tail_identity(records: &[LogRecord]) -> Option<(u64, u64)> {
    match records.first() {
        Some(LogRecord::TailOf { epoch, jseq }) => Some((*epoch, *jseq)),
        _ => None,
    }
}

/// What a segment (or a shipped run of this node's own rows) tells the member
/// copies.
pub(crate) struct Facts<'a> {
    /// The epoch of the tenure that shipped it.
    pub epoch: u64,
    /// Its `through` (the journal position it brings its tenure to).
    pub through: u64,
    /// The journal seqs of the rows it carries, if it names them.
    pub rows: &'a [u64],
    /// Its records.
    pub records: &'a [LogRecord],
    /// The first segment of a newer epoch than this replica had applied:
    /// the epoch's marker, or its first data segment when it needs none.
    pub first_of_epoch: bool,
    /// The segment of this node's own tenure, matched against its journal
    /// (`ack_rows_tx`): only coverage applies.
    pub own: bool,
}

/// What [`member_rules_tx`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemberOutcome {
    /// Rows retired by coverage.
    pub retired: usize,
    /// Rows dropped as never committed (past a `TailFollowsThrough`'s
    /// `through`, or unacknowledged at a newer epoch's marker).
    pub dropped: usize,
    /// Rows an abandon queued for replay.
    pub abandoned: usize,
}

impl Meta {
    /// Whether this node holds any member copy (one atomic read: the segment
    /// path asks it on every segment).
    pub(crate) fn has_member_copies(&self) -> bool {
        self.any_member_copy
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn note_member_copies(&self, any: bool) {
        self.any_member_copy
            .store(any, std::sync::atomic::Ordering::Relaxed);
    }

    /// Recompute the flag from the store (open, after a drop).
    pub(crate) fn refresh_member_copy_flag(&self) -> Result<(), MetaError> {
        let any = !self.member_copy_epochs()?.is_empty();
        self.note_member_copies(any);
        Ok(())
    }

    /// Plan 43 C1-3: this node's copy of epoch `epoch`'s transactions, as a
    /// member of that epoch's group that was sealed by a claimant's
    /// `SealTail` (a holder: its own journal rows). Persisted and synced
    /// before the caller answers: the answer is what a claimant's claim rests
    /// on, and the copy is what a later claimant of the same lease is told
    /// when this node is asked again. Idempotent: a second put of the same
    /// epoch adds the rows it brings (a row already held keeps its class).
    pub fn member_copy_put(
        &self,
        epoch: u64,
        config_version: u64,
        txs: &[BackupTx],
    ) -> Result<(), MetaError> {
        let rows: Vec<MemberTx> = txs.iter().cloned().map(MemberTx::from_backup).collect();
        self.member_copy_put_rows(epoch, config_version, &rows)
    }

    /// [`Self::member_copy_put`] for rows that carry their class.
    pub fn member_copy_put_rows(
        &self,
        epoch: u64,
        config_version: u64,
        rows: &[MemberTx],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        for row in rows {
            put_row_tx(&mut tx, self, epoch, row)?;
        }
        set_info_tx(&mut tx, epoch, self, Some(config_version), 0)?;
        tx.commit()?;
        self.note_member_copies(true);
        self.sync()
    }

    /// The unshipped journal rows from journal seq `from`, as member rows
    /// (class, rid and op included).
    pub fn journal_member_rows_from(&self, from: u64) -> Result<Vec<MemberTx>, MetaError> {
        let txs = self.journal_txs_from(from, usize::MAX)?;
        let r = self.db.read_tx();
        let mut out = Vec::with_capacity(txs.len());
        for t in txs {
            let row = super::local::get_journal_tx(&r, self, t.first)?;
            out.push(match row {
                Some(row) => MemberTx {
                    class: row.class,
                    rid: row.rid,
                    op: row.op,
                    lock_tag: row.lock_tag,
                    tx: t,
                },
                None => MemberTx {
                    tx: t,
                    class: RowClass::Lone,
                    rid: None,
                    op: None,
                    lock_tag: LockTag::NONE,
                },
            });
        }
        Ok(out)
    }

    /// The member copy of `epoch`: its info and rows, oldest first.
    pub fn member_copy_rows(
        &self,
        epoch: u64,
    ) -> Result<Option<(MemberCopyInfo, Vec<MemberTx>)>, MetaError> {
        let r = self.db.read_tx();
        let Some(info) = info_in(&r, self, epoch)? else {
            return Ok(None);
        };
        Ok(Some((info, rows_in(&r, self, epoch)?)))
    }

    /// [`Self::member_copy_rows`] as the transactions a claimant is served.
    pub fn member_copy(
        &self,
        epoch: u64,
    ) -> Result<Option<(MemberCopyInfo, Vec<BackupTx>)>, MetaError> {
        Ok(self
            .member_copy_rows(epoch)?
            .map(|(i, rows)| (i, rows.into_iter().map(|m| m.tx).collect())))
    }

    /// Every epoch this node holds a member copy of.
    pub fn member_copy_epochs(&self) -> Result<Vec<u64>, MetaError> {
        let r = self.db.read_tx();
        epochs_in(&r, self)
    }

    /// How many rows the member copies hold in all.
    pub fn member_copy_rows_total(&self) -> Result<usize, MetaError> {
        let r = self.db.read_tx();
        let mut n = 0;
        for e in epochs_in(&r, self)? {
            n += rows_in(&r, self, e)?.len();
        }
        Ok(n)
    }

    /// Whether a member copy carries a transaction of `rid` (its rid, or its
    /// `Completed` record): the row is held, not lost, and a refusal of it
    /// that is no longer in the journal is not rolled back (the simulation's
    /// oracle asks).
    pub fn member_copy_carries(&self, rid: Rid) -> Result<bool, MetaError> {
        if !self.has_member_copies() {
            return Ok(false);
        }
        let r = self.db.read_tx();
        for e in epochs_in(&r, self)? {
            for row in rows_in(&r, self, e)? {
                if row.rid == Some(rid)
                    || row.tx.records.iter().any(|rec| {
                        matches!(rec, LogRecord::Completed { rid: c } | LogRecord::Refused { rid: c, .. } if c == &rid)
                    })
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Forget the member copy of `epoch` (its rows are in the log, or the
    /// group was abandoned).
    pub fn member_copy_drop(&self, epoch: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        drop_epoch_tx(&mut tx, self, epoch)?;
        tx.commit()?;
        self.refresh_member_copy_flag()
    }
}

// ---------------------------------------------------------------- reads

fn info_in(
    r: &impl Readable,
    meta: &Meta,
    epoch: u64,
) -> Result<Option<MemberCopyInfo>, MetaError> {
    let Some(s) = kv_get_tx(r, &meta.local, &member_info_key(epoch))? else {
        return Ok(None);
    };
    let mut it = s.split(':').map(|x| x.parse::<u64>());
    let (Some(Ok(config_version)), Some(Ok(through))) = (it.next(), it.next()) else {
        return Ok(None);
    };
    Ok(Some(MemberCopyInfo {
        config_version,
        through,
    }))
}

fn rows_in(r: &impl Readable, meta: &Meta, epoch: u64) -> Result<Vec<MemberTx>, MetaError> {
    let e = member_epoch(epoch);
    let mut out = Vec::new();
    for guard in r.range(&meta.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX)) {
        let (_, v) = guard.into_inner()?;
        out.push(postcard::from_bytes(&v)?);
    }
    Ok(out)
}

fn epochs_in(r: &impl Readable, meta: &Meta) -> Result<Vec<u64>, MetaError> {
    let mut out = Vec::new();
    for guard in r.prefix(&meta.local, "member_copy:".as_bytes()) {
        let (k, _) = guard.into_inner()?;
        if let Some(e) = std::str::from_utf8(&k)
            .ok()
            .and_then(|k| k.strip_prefix("member_copy:"))
            .and_then(|e| e.parse::<u64>().ok())
        {
            out.push(e);
        }
    }
    Ok(out)
}

// --------------------------------------------------------------- writes

fn set_info_tx(
    tx: &mut SingleWriterWriteTx,
    epoch: u64,
    meta: &Meta,
    config_version: Option<u64>,
    through: u64,
) -> Result<(), MetaError> {
    let old = info_in(&*tx, meta, epoch)?;
    let config_version = match (config_version, old) {
        (Some(v), Some(o)) => v.max(o.config_version),
        (Some(v), None) => v,
        (None, Some(o)) => o.config_version,
        (None, None) => 0,
    };
    let through = through.max(old.map_or(0, |o| o.through));
    kv_set_tx(
        tx,
        &meta.local,
        &member_info_key(epoch),
        &format!("{config_version}:{through}"),
    );
    Ok(())
}

/// Put one row of `epoch`'s member copy in the caller's transaction (a
/// deposition's strand, a takeover moving the tail it re-journaled). A row
/// held already keeps its class and op: a snapshot taken at a seal and the
/// strand that follows it describe the same transaction.
pub(crate) fn put_row_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    epoch: u64,
    row: &MemberTx,
) -> Result<(), MetaError> {
    if log_settled_tx(&*tx, meta, epoch, row)? {
        tracing::debug!(
            epoch,
            first = row.tx.first,
            last = row.tx.last,
            "a member copy row the log settles already: not kept"
        );
        return Ok(());
    }
    tracing::debug!(
        epoch,
        first = row.tx.first,
        last = row.tx.last,
        class = ?row.class,
        rid = ?row.rid,
        "member copy row kept"
    );
    let key = tail_key(member_epoch(epoch), row.tx.first);
    let keep = match tx.get(&meta.backup_tail, &key)? {
        Some(v) => Some(postcard::from_bytes::<MemberTx>(&v)?),
        None => None,
    };
    let row = match keep {
        // (A row the strand adds to a copy that holds it already carries
        // more than the snapshot did: the class says what its client was
        // told.)
        Some(old) if old.class.gated() && !row.class.gated() => old,
        _ => row.clone(),
    };
    tx.insert(&meta.backup_tail, key, postcard::to_allocvec(&row)?);
    set_info_tx(tx, epoch, meta, None, row.tx.last)?;
    meta.note_member_copies(true);
    Ok(())
}

fn drop_epoch_tx(tx: &mut SingleWriterWriteTx, meta: &Meta, epoch: u64) -> Result<(), MetaError> {
    let e = member_epoch(epoch);
    let keys: Vec<Vec<u8>> = tx
        .range(&meta.backup_tail, tail_key(e, 0)..=tail_key(e, u64::MAX))
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(&meta.backup_tail, k);
    }
    tx.remove(&meta.local, member_info_key(epoch).into_bytes());
    Ok(())
}

/// The drop rules (module doc) for one segment the log delivered, or one
/// run of this node's own rows that shipped, in the caller's transaction.
pub(crate) fn member_rules_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    f: &Facts<'_>,
) -> Result<MemberOutcome, MetaError> {
    let mut out = MemberOutcome::default();
    if !meta.has_member_copies() {
        return Ok(out);
    }
    let mut tail_ids: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut through_of: std::collections::BTreeMap<u64, u64> = Default::default();
    let mut follows: BTreeSet<u64> = BTreeSet::new();
    let mut abandoned: BTreeSet<u64> = BTreeSet::new();
    for rec in f.records {
        match rec {
            LogRecord::TailOf { epoch, jseq } => {
                tail_ids.insert((*epoch, *jseq));
            }
            LogRecord::TailFollowsThrough {
                prev_epoch,
                through,
            } => {
                through_of.insert(*prev_epoch, *through);
                follows.insert(*prev_epoch);
            }
            LogRecord::TailFollows { prev_epoch } => {
                follows.insert(*prev_epoch);
            }
            LogRecord::GroupAbandoned { epoch, gen: 0, .. } => {
                abandoned.insert(*epoch);
            }
            _ => {}
        }
    }
    for e in epochs_in(&*tx, meta)? {
        let mut left = 0usize;
        for row in rows_in(&*tx, meta, e)? {
            let (first, last) = (row.tx.first, row.tx.last);
            let key = tail_key(member_epoch(e), first);
            let own_epoch = f.epoch == e;
            // A transaction's identity: that of the first journal that held
            // it — its `TailOf` if a takeover re-journaled it, else its own
            // place in this epoch's journal.
            let ident = tail_identity(&row.tx.records).unwrap_or((e, first));
            let covered = tail_ids.contains(&ident)
                || tail_ids.contains(&(e, first))
                || (own_epoch
                    && (f.rows.contains(&last)
                        || f.rows.contains(&first)
                        || (f.through > 0 && last <= f.through)));
            if covered {
                tx.remove(&meta.backup_tail, key);
                out.retired += 1;
                continue;
            }
            if f.own {
                left += 1;
                continue;
            }
            if abandoned.contains(&e) {
                if row.class != RowClass::Lone
                    && super::spec::queue_abandoned_tx(tx, meta, e, &row)?
                {
                    out.abandoned += 1;
                }
                tx.remove(&meta.backup_tail, key);
                continue;
            }
            if through_of.get(&ident.0).is_some_and(|th| ident.1 > *th) {
                tx.remove(&meta.backup_tail, key);
                out.dropped += 1;
                continue;
            }
            if f.first_of_epoch
                && f.epoch > e
                && !follows.contains(&e)
                && matches!(
                    row.class,
                    RowClass::Archive | RowClass::Lone | RowClass::NonStealable
                )
            {
                tx.remove(&meta.backup_tail, key);
                out.dropped += 1;
                continue;
            }
            left += 1;
        }
        if left == 0 {
            tx.remove(&meta.local, member_info_key(e).into_bytes());
        }
    }
    Ok(out)
}

impl Meta {
    /// The member copies' coverage for this node's own rows that shipped
    /// (`journal_seqs`, a segment of its own tenure, matched against the
    /// journal): a row carrying a `TailOf` retires the copy row it names, and
    /// a row of an epoch retires the copy rows of that epoch at its seq.
    pub(crate) fn member_cover_shipped_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        journal_seqs: &[u64],
    ) -> Result<(), MetaError> {
        if !self.has_member_copies() || journal_seqs.is_empty() {
            return Ok(());
        }
        let mut records: Vec<LogRecord> = Vec::new();
        let mut by_epoch: std::collections::BTreeMap<u64, Vec<u64>> = Default::default();
        for seq in journal_seqs {
            if let Some(v) = tx.get(&self.journal_ks, super::journal::seq_key(*seq))? {
                let rec = LogRecord::from_postcard(&v)?;
                if matches!(rec, LogRecord::TailOf { .. }) {
                    records.push(rec);
                }
            }
            if let Some(h) = super::local::get_journal_tx_head(&*tx, self, *seq)? {
                by_epoch.entry(h.epoch).or_default().push(*seq);
            }
        }
        if by_epoch.is_empty() {
            by_epoch.insert(0, Vec::new());
        }
        for (epoch, rows) in by_epoch {
            member_rules_tx(
                tx,
                self,
                &Facts {
                    epoch,
                    through: 0,
                    rows: &rows,
                    records: &records,
                    first_of_epoch: false,
                    own: true,
                },
            )?;
        }
        Ok(())
    }
}
