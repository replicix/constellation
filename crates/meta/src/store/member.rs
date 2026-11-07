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
use super::local::RowCommit;
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
pub(super) const MEMBER_COPY_BASE: u64 = 2 << 48;

/// How long a rolled-back member copy row holds reads of its keys at most.
/// The hold waits for the successor's re-ship, which lands within one S3
/// ship round (measured 35-350 ms under slow S3, 2 s at the worst of a
/// stalled successor); 5 s is well above that, and short enough that a row
/// which never leaves the copy (a leak) costs a read one wait for at most
/// this long, not for good. After the cap the read is answered from the
/// replica as it is when the session wait times out.
pub const HELD_COPY_MAX_AGE_MS: i64 = 5_000;

/// The least time between two warnings that a hold lapsed.
pub(crate) const HELD_COPY_WARN_EVERY_MS: i64 = 10_000;

/// A member copy row a segment's strand rolled back
/// ([`Meta::member_copy_touches`]). In memory, not persisted: after a
/// restart nothing is held. A segment's apply and a deposition's recovery
/// (`strand_below_epoch`) set one, on a node that serves reads while the
/// rows after the rolled-back one stay (the recovery's until its rebuild
/// lands); a takeover gate and a namespace rebuild do not (the gate
/// re-applies the tail in the same step, the rebuild derives the namespace
/// from the log, so no row that depends on a rolled-back one stays). The cost of not
/// persisting: a process that crashes inside the re-ship window (the 50-125 ms
/// measured) comes back without the hold and may serve the gap until the
/// re-ship lands; the window is bounded by the re-ship latency, not closed
/// after a crash.
#[derive(Clone, Debug)]
pub(crate) struct HeldCopy {
    pub(crate) epoch: u64,
    pub(crate) first: u64,
    pub(crate) keys: crate::KeySet,
    /// [`Meta::held_copy_now_ms`] when it was rolled back.
    pub(crate) since_ms: i64,
}

/// A release a transaction staged ([`Meta::flush_held_releases`]).
#[derive(Clone, Copy, Debug)]
pub(crate) enum StagedRelease {
    Row(u64, u64),
    Epoch(u64),
}

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

/// Plan 46 M1.4 step 4 (SHOULD-3): how far a re-ship of `epoch`'s own tail
/// stands, as `(announced, reached)`: `TailFollowsThrough` said the sealed
/// prefix reaches journal seq `announced`, and the log carries re-shipped
/// transactions of the epoch through `reached` (a tail is re-journaled in
/// order). A copy of a transaction at or below `announced` and past
/// `reached` is still to be named: a segment that passes it without naming
/// it is not the last word, the re-ship may span several segments (the
/// shipper cuts them by count and bytes, and a re-journaled transaction sits
/// after the old journal's seqs, so the first segment of the re-ship
/// already passes the old rows). `records` is the segment being applied,
/// whose facts are not in the store yet.
pub(crate) fn reship_progress_tx(
    r: &impl Readable,
    meta: &Meta,
    epoch: u64,
    records: &[LogRecord],
) -> Result<(Option<u64>, Option<u64>), MetaError> {
    let mut announced = kv_u64(r, meta, &through_key(epoch))?;
    let mut reached = kv_u64(r, meta, &reship_key(epoch))?;
    for rec in records {
        match rec {
            LogRecord::TailFollowsThrough {
                prev_epoch,
                through,
            } if *prev_epoch == epoch => {
                announced = Some(announced.map_or(*through, |o| o.max(*through)));
            }
            LogRecord::TailOf { epoch: e, jseq } if *e == epoch => {
                reached = Some(reached.map_or(*jseq, |o| o.max(*jseq)));
            }
            _ => {}
        }
    }
    Ok((announced, reached))
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

/// The rows of every member copy the log, as this replica now knows it,
/// settles ([`log_settled_tx`]) leave, in the caller's transaction: a
/// namespace rebuilt from the log adopted what that log says of each epoch's
/// tail ([`adopt_log_tail_facts_tx`]) without applying its segments here, so
/// no segment ran the drop rules for the re-ship it carries. A copy a
/// deposed holder made at its seal, whose successor re-shipped it before the
/// rebuild read the log, stayed for good (placement-hot seed 904: two
/// refusals of epoch 1 left at convergence).
pub(crate) fn drop_settled_rows_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
) -> Result<usize, MetaError> {
    if !meta.has_member_copies() {
        return Ok(0);
    }
    let mut dropped = 0usize;
    for e in epochs_in(&*tx, meta)? {
        let mut left = 0usize;
        for row in rows_in(&*tx, meta, e)? {
            if log_settled_tx(&*tx, meta, e, &row)? {
                tx.remove(&meta.backup_tail, tail_key(member_epoch(e), row.tx.first));
                meta.release_copy_row(e, row.tx.first);
                dropped += 1;
            } else {
                left += 1;
            }
        }
        if left == 0 {
            tx.remove(&meta.local, member_info_key(e).into_bytes());
        }
    }
    Ok(dropped)
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
    pub class: RowCommit,
    pub rid: Option<Rid>,
    pub op: Option<MutateOp>,
    pub lock_tag: LockTag,
}

impl MemberTx {
    /// A transaction a backup held (the group held it): `RootGroup`.
    pub fn from_backup(tx: BackupTx) -> MemberTx {
        MemberTx {
            tx,
            class: RowCommit::ROOT_GROUP,
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
                    class: RowCommit::LONE,
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

    /// Whether a member copy row this node's strand rolled back touches any
    /// of `keys`. A deposed holder's committed rows that a later epoch's
    /// segment strands are rolled back into its member copy, not queued for
    /// replay: the successor re-ships them, and until the log carries them
    /// (the drop rules retire the row) the namespace lacks them while later
    /// rows that depend on them may stay (a delegate's row this root
    /// appended after them). A read of their keys waits for the re-ship as
    /// it waits for a queued replay (`long-delegated-late-answers` seed 5839:
    /// a marker visible without its data on the deposed root).
    ///
    /// Answered from an in-memory set ([`HeldCopy`]), not from the member
    /// copy: only a row this process rolled back is missing from the
    /// namespace (the successor's copy of a re-shipped row, a claimant's
    /// seal snapshot and a backup's copy are applied or never were), and no
    /// row is held longer than [`HELD_COPY_MAX_AGE_MS`], so a row that never
    /// leaves the copy (a leak, `backup-crash-slow` seed 840) cannot stall
    /// reads for good. One atomic load when nothing is held.
    pub fn member_copy_touches(&self, keys: &[crate::ReadKey]) -> bool {
        // (The clock reads the lock table's mutex: not on every read.)
        if !self
            .any_held_copy
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return false;
        }
        self.member_copy_touches_at(keys, self.held_copy_now_ms())
    }

    /// The clock the hold's age is read on, ms: the node's wait clock (the
    /// lock table's, the simulation's virtual one there), or a monotonic host
    /// clock where none is set (unit tests).
    pub fn held_copy_now_ms(&self) -> i64 {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        match self.locks().clock_now() {
            Some(now) => now.wait.0,
            None => START
                .get_or_init(std::time::Instant::now)
                .elapsed()
                .as_millis() as i64,
        }
    }

    /// [`Self::member_copy_touches`] at `now` (a test steps past the cap
    /// without sleeping).
    pub fn member_copy_touches_at(&self, keys: &[crate::ReadKey], now_ms: i64) -> bool {
        if !self
            .any_held_copy
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return false;
        }
        let mut held = self.held_copies.lock().unwrap();
        let before = held.len();
        let mut oldest: Option<&HeldCopy> = None;
        for h in held.iter() {
            if now_ms.saturating_sub(h.since_ms) >= HELD_COPY_MAX_AGE_MS
                && oldest.is_none_or(|o| h.since_ms < o.since_ms)
            {
                oldest = Some(h);
            }
        }
        let lapsed_row = oldest.map(|h| (h.epoch, h.first));
        held.retain(|h| now_ms.saturating_sub(h.since_ms) < HELD_COPY_MAX_AGE_MS);
        let lapsed = (before - held.len()) as u64;
        if lapsed > 0 {
            self.note_held_copy_lapsed(lapsed, lapsed_row, now_ms);
        }
        self.any_held_copy
            .store(!held.is_empty(), std::sync::atomic::Ordering::Relaxed);
        held.iter().any(|h| keys.iter().any(|k| h.keys.touches(k)))
    }

    /// Plan 46 M0.4: `n` read holds ran out at their cap before the
    /// successor's re-ship released them. A reader then saw the replica
    /// without a committed row (a marker without its data), and the row may
    /// never leave the copy (`backup-crash-slow` seed 840). Counted
    /// (`status.ack.held_copy_lapsed`), and warned of at most once per
    /// [`HELD_COPY_WARN_EVERY_MS`].
    fn note_held_copy_lapsed(&self, n: u64, row: Option<(u64, u64)>, now_ms: i64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.held_copy_lapsed.fetch_add(n, Relaxed);
        let last = self.held_copy_last_warn_ms.load(Relaxed);
        if last != i64::MIN && now_ms.saturating_sub(last) < HELD_COPY_WARN_EVERY_MS {
            return;
        }
        self.held_copy_last_warn_ms.store(now_ms, Relaxed);
        tracing::warn!(
            lapsed = n,
            total = self.held_copy_lapsed.load(Relaxed),
            oldest_epoch = row.map(|r| r.0),
            oldest_first = row.map(|r| r.1),
            cap_ms = HELD_COPY_MAX_AGE_MS,
            "a member copy row's read hold lapsed at its cap before the successor's re-ship \
             released it: reads of its keys are answered without it"
        );
    }

    /// Read holds that lapsed at their cap since the start
    /// ([`Self::note_held_copy_lapsed`]).
    pub fn held_copy_lapsed(&self) -> u64 {
        self.held_copy_lapsed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Note a row the strand rolled back into `epoch`'s member copy.
    pub(crate) fn hold_copy_row(&self, epoch: u64, first: u64, keys: crate::KeySet) {
        let mut held = self.held_copies.lock().unwrap();
        held.retain(|h| (h.epoch, h.first) != (epoch, first));
        let since_ms = self.held_copy_now_ms();
        held.push(HeldCopy {
            epoch,
            first,
            keys,
            since_ms,
        });
        self.any_held_copy
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// The row `(epoch, first)` leaves the member copy in the caller's
    /// transaction: its keys are released once that transaction committed
    /// ([`Self::flush_held_releases`]), not now. Released inside the
    /// transaction, a reader polling before the commit would be let go and
    /// read the pre-commit namespace (the marker without its data).
    pub(crate) fn release_copy_row(&self, epoch: u64, first: u64) {
        if !self
            .any_held_copy
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.stage_release(StagedRelease::Row(epoch, first));
    }

    /// The whole member copy of `epoch` leaves in the caller's transaction
    /// (released after its commit, like [`Self::release_copy_row`]).
    pub(crate) fn release_copy_epoch(&self, epoch: u64) {
        if !self
            .any_held_copy
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.stage_release(StagedRelease::Epoch(epoch));
    }

    fn stage_release(&self, r: StagedRelease) {
        let me = std::thread::current().id();
        self.staged_releases.lock().unwrap().push((me, r));
    }

    /// Apply this thread's staged releases after its transaction committed.
    /// Each is checked against the committed member copy: a hold whose row
    /// is still there stays (the transaction aborted, or the row came back),
    /// so an abort releases nothing, and a stale staging left by a failed
    /// pass is harmless.
    pub(crate) fn flush_held_releases(&self) {
        let me = std::thread::current().id();
        let mine: Vec<StagedRelease> = {
            let mut all = self.staged_releases.lock().unwrap();
            if all.is_empty() {
                return;
            }
            let (mine, others): (Vec<_>, Vec<_>) = std::mem::take(&mut *all)
                .into_iter()
                .partition(|(t, _)| *t == me);
            *all = others;
            mine.into_iter().map(|(_, r)| r).collect()
        };
        if mine.is_empty() {
            return;
        }
        let snap = self.db.read_tx();
        let mut held = self.held_copies.lock().unwrap();
        held.retain(|h| {
            let named = mine.iter().any(|r| match r {
                StagedRelease::Row(e, f) => (h.epoch, h.first) == (*e, *f),
                StagedRelease::Epoch(e) => h.epoch == *e,
            });
            if !named {
                return true;
            }
            // Gone from the committed copy? (A read error keeps the hold.)
            !matches!(
                snap.get(&self.backup_tail, tail_key(member_epoch(h.epoch), h.first)),
                Ok(None)
            )
        });
        self.any_held_copy
            .store(!held.is_empty(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Forget the member copy of `epoch` (its rows are in the log, or the
    /// group was abandoned).
    pub fn member_copy_drop(&self, epoch: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        drop_epoch_tx(&mut tx, self, epoch)?;
        tx.commit()?;
        self.flush_held_releases();
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
        out.push(crate::record::from_bytes_exact(&v)?);
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

/// [`Meta::member_copy_put`] in the caller's transaction (the takeover
/// keeping the tail it re-journaled, [`Meta::reship_at_takeover`]).
pub(crate) fn put_rows_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    epoch: u64,
    config_version: u64,
    txs: &[BackupTx],
) -> Result<(), MetaError> {
    for t in txs {
        put_row_tx(tx, meta, epoch, &MemberTx::from_backup(t.clone()))?;
    }
    set_info_tx(tx, epoch, meta, Some(config_version), 0)
}

/// Put one row of `epoch`'s member copy in the caller's transaction (a
/// deposition's strand, a takeover moving the tail it re-journaled). A row
/// held already keeps its class and op: a snapshot taken at a seal and the
/// strand that follows it describe the same transaction. Whether the row
/// was kept (the log does not settle it).
pub(crate) fn put_row_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    epoch: u64,
    row: &MemberTx,
) -> Result<bool, MetaError> {
    if log_settled_tx(&*tx, meta, epoch, row)? {
        tracing::debug!(
            epoch,
            first = row.tx.first,
            last = row.tx.last,
            "a member copy row the log settles already: not kept"
        );
        return Ok(false);
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
        Some(v) => Some(crate::record::from_bytes_exact::<MemberTx>(&v)?),
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
    Ok(true)
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
    meta.release_copy_epoch(epoch);
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
                meta.release_copy_row(e, first);
                out.retired += 1;
                continue;
            }
            if f.own {
                left += 1;
                continue;
            }
            if abandoned.contains(&e) {
                if !row.class.is_singleton() && super::spec::queue_abandoned_tx(tx, meta, e, &row)?
                {
                    out.abandoned += 1;
                }
                tx.remove(&meta.backup_tail, key);
                meta.release_copy_row(e, first);
                continue;
            }
            if through_of.get(&ident.0).is_some_and(|th| ident.1 > *th) {
                tx.remove(&meta.backup_tail, key);
                meta.release_copy_row(e, first);
                out.dropped += 1;
                continue;
            }
            if f.first_of_epoch
                && f.epoch > e
                && !follows.contains(&e)
                && (row.class.is_archive() || row.class.is_singleton() || row.class.is_final())
            {
                tx.remove(&meta.backup_tail, key);
                meta.release_copy_row(e, first);
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

/// The identities a record list retracts (`LogRecord::RolledBack`).
pub(crate) fn rolled_back_idents<'a>(
    records: impl IntoIterator<Item = &'a LogRecord>,
) -> BTreeSet<(u64, u64)> {
    records
        .into_iter()
        .filter_map(|rec| match rec {
            LogRecord::RolledBack { idents } => Some(idents.iter().copied()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Plan 46 M0.2 follow-up (defect B): the member-copy rows of the
/// transactions `idents` names (`LogRecord::RolledBack`: the holder's
/// `repair drop-held` rolled them back) become holes, in the caller's
/// transaction: a hole spans the same journal seqs and carries nothing, so
/// the copy stays contiguous for the claimant that seals it and no
/// successor re-ships a dropped row. A row is kept by its identity as
/// [`member_rules_tx`] keys it. Returns how many rows became holes.
pub(crate) fn hole_rolled_back_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    idents: &BTreeSet<(u64, u64)>,
) -> Result<usize, MetaError> {
    if idents.is_empty() || !meta.has_member_copies() {
        return Ok(0);
    }
    let mut holed = 0;
    for e in epochs_in(&*tx, meta)? {
        for row in rows_in(&*tx, meta, e)? {
            let ident = tail_identity(&row.tx.records).unwrap_or((e, row.tx.first));
            if !idents.contains(&ident) || row.tx.records.is_empty() {
                continue;
            }
            let hole = MemberTx {
                tx: BackupTx {
                    first: row.tx.first,
                    last: row.tx.last,
                    records: Vec::new(),
                    origin: row.tx.origin,
                },
                class: row.class,
                rid: None,
                op: None,
                lock_tag: LockTag::NONE,
            };
            tx.insert(
                &meta.backup_tail,
                tail_key(member_epoch(e), row.tx.first),
                postcard::to_allocvec(&hole)?,
            );
            meta.release_copy_row(e, row.tx.first);
            holed += 1;
        }
    }
    Ok(holed)
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

#[cfg(test)]
mod release_tests {
    use super::*;
    use crate::ReadKey;

    fn row(first: u64) -> MemberTx {
        MemberTx {
            tx: BackupTx {
                first,
                last: first,
                records: vec![],
                origin: (0, 0),
            },
            class: RowCommit::ROOT_GROUP,
            rid: None,
            op: None,
            lock_tag: Default::default(),
        }
    }

    /// A row's hold ends with the commit that took the row out, not with the
    /// release inside the transaction; a transaction that aborts after the
    /// release leaves the hold, since the row is still in the copy.
    #[test]
    fn a_release_takes_effect_after_the_commit_only() {
        let meta = Meta::open_in_memory().unwrap();
        let key = [ReadKey::Dentry(1, "x".into())];
        let keys = || {
            crate::KeySet::from_records(&[crate::LogRecord::Create {
                parent: 1,
                name: "x".into(),
                ino: 7,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            }])
        };
        let mut tx = meta.db.write_tx();
        assert!(put_row_tx(&mut tx, &meta, 3, &row(5)).unwrap());
        tx.commit().unwrap();
        meta.hold_copy_row(3, 5, keys());
        assert!(meta.member_copy_touches(&key));

        // Aborted: released inside, never committed.
        {
            let mut tx = meta.db.write_tx();
            tx.remove(&meta.backup_tail, tail_key(member_epoch(3), 5));
            meta.release_copy_row(3, 5);
            assert!(
                meta.member_copy_touches(&key),
                "still held inside the transaction, before its commit"
            );
            drop(tx);
        }
        meta.flush_held_releases();
        assert!(meta.member_copy_touches(&key), "an abort releases nothing");

        // Committed: released once it has.
        let mut tx = meta.db.write_tx();
        tx.remove(&meta.backup_tail, tail_key(member_epoch(3), 5));
        meta.release_copy_row(3, 5);
        assert!(meta.member_copy_touches(&key));
        tx.commit().unwrap();
        assert!(meta.member_copy_touches(&key), "not before the flush");
        meta.flush_held_releases();
        assert!(!meta.member_copy_touches(&key));
    }
}
