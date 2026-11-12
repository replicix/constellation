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
//! 0. At the claimant's own takeover gate, after its marker landed, an own
//!    `Archive` row of an older epoch the log does not carry is not kept (it
//!    was never committed; rule 4's reason, which the claimant never applies
//!    to its own marker).
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

/// The least time between two warnings that a read of a held key was refused.
pub(crate) const READ_HOLD_WARN_EVERY_MS: i64 = 10_000;

/// The span a read waits for a held row to come back when nothing has set
/// one ([`Meta::set_read_hold_span`]): the daemon and the simulation set the
/// derived `read_hold_span` (ADR-48), unit tests keep this.
pub const DEFAULT_READ_HOLD_SPAN_MS: u64 = 60_000;

/// An acknowledged row this node took out of its served state while the log
/// lacks it ([`Meta::member_copy_touches`], ADR-57). Persisted in the strand's
/// own transaction (`read_hold:{slot}:{first}`, [`Meta::hold_row_tx`]) and
/// loaded at open, so a restart inside the re-ship window keeps it. `slot`
/// and `first` are the row's place in `backup_tail` (a member copy slot
/// [`member_epoch`], or a delegate generation's [`super::backup::deleg_epoch`]).
#[derive(Clone, Debug)]
pub(crate) struct HeldCopy {
    pub(crate) slot: u64,
    pub(crate) first: u64,
    pub(crate) keys: crate::KeySet,
    /// [`Meta::held_copy_now_ms`] when it was held (or loaded at open).
    pub(crate) since_ms: i64,
}

/// What a read of `keys` is admitted as ([`Meta::read_admission`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Ready,
    /// A pending acknowledged row touches the keys: not to be answered yet.
    Held,
}

/// [`Meta::read_hold_stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadHoldStats {
    pub holds: u64,
    pub oldest_ms: u64,
    pub refused: u64,
    pub abandoned: u64,
}

/// A release a transaction staged ([`Meta::flush_held_releases`]).
#[derive(Clone, Copy, Debug)]
pub(crate) enum StagedRelease {
    Row(u64, u64),
    Slot(u64),
    /// Rows of the slot with `first` at or below the bound.
    Through(u64, u64),
}

pub(super) fn read_hold_key(slot: u64, first: u64) -> Vec<u8> {
    format!("read_hold:{slot:020}:{first:020}").into_bytes()
}

/// `(slot, first)` of a `read_hold:` key.
fn parse_read_hold_key(k: &[u8]) -> Option<(u64, u64)> {
    let k = std::str::from_utf8(k).ok()?;
    let mut it = k.strip_prefix("read_hold:")?.split(':');
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

pub(super) fn member_epoch(epoch: u64) -> u64 {
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
                meta.release_row_tx(tx, member_epoch(e), row.tx.first);
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

    /// Whether a pending acknowledged row (a read hold, ADR-57) touches any
    /// of `keys`. A deposed holder's committed rows that a later epoch's
    /// segment strands are rolled back into its member copy, not queued for
    /// replay: the successor re-ships them, and until the log carries them
    /// the namespace lacks them while later rows that depend on them may
    /// stay (a delegate's row this root appended after them); a rebuild
    /// from the log and a takeover gate do the same. A read of their keys
    /// is not answered from this state: it waits for the row's return
    /// ([`Meta::session_wait`]'s hold phase) and is refused after
    /// [`Meta::read_hold_span`] (`long-delegated-late-answers` seed 5839: a
    /// marker visible without its data on the deposed root;
    /// `sticky-root-designee-inbox` seed 745: a node read its own
    /// acknowledged rename's target absent).
    ///
    /// Answered from the in-memory index of the persisted set. One atomic
    /// load when nothing is held.
    pub fn member_copy_touches(&self, keys: &[crate::ReadKey]) -> bool {
        if !self.any_held() {
            return false;
        }
        let held = self.held_copies.lock().unwrap();
        held.iter().any(|h| keys.iter().any(|k| h.keys.touches(k)))
    }

    /// [`Self::member_copy_touches`] as the admission a read gets
    /// (`Held`: it must not be answered yet).
    pub fn read_admission(&self, keys: &[crate::ReadKey]) -> Admission {
        if self.member_copy_touches(keys) {
            Admission::Held
        } else {
            Admission::Ready
        }
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

    /// How long a read of a held key waits for the row's return before it
    /// is refused (`EIO`): the derived `read_hold_span` (ADR-48), set by the
    /// daemon.
    pub fn read_hold_span(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.read_hold_span_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub fn set_read_hold_span(&self, span: std::time::Duration) {
        self.read_hold_span_ms.store(
            u64::try_from(span.as_millis()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// A read of held `keys` waited `waited` and was refused. Counted
    /// (`status.ack.read_holds_refused`), and warned of at most once per
    /// [`READ_HOLD_WARN_EVERY_MS`] with the oldest hold.
    pub(crate) fn note_read_hold_refused(&self, waited: std::time::Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        self.read_holds_refused.fetch_add(1, Relaxed);
        let now_ms = self.held_copy_now_ms();
        let last = self.held_copy_last_warn_ms.load(Relaxed);
        if last != i64::MIN && now_ms.saturating_sub(last) < READ_HOLD_WARN_EVERY_MS {
            return;
        }
        self.held_copy_last_warn_ms.store(now_ms, Relaxed);
        let oldest = self
            .held_copies
            .lock()
            .unwrap()
            .iter()
            .min_by_key(|h| h.since_ms)
            .map(|h| (h.slot, h.first, now_ms.saturating_sub(h.since_ms)));
        tracing::warn!(
            waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            refused = self.read_holds_refused.load(Relaxed),
            oldest_slot = oldest.map(|o| o.0),
            oldest_first = oldest.map(|o| o.1),
            oldest_age_ms = oldest.map(|o| o.2),
            "a read of a key an acknowledged row this node rolled back touches was refused \
             (EIO): no successor re-shipped the row within the read hold span"
        );
    }

    /// The read holds now (ADR-57): how many, the age of the oldest in ms,
    /// reads refused and holds an operator's abandon released, since the
    /// start.
    pub fn read_hold_stats(&self) -> ReadHoldStats {
        use std::sync::atomic::Ordering::Relaxed;
        let now_ms = self.held_copy_now_ms();
        let held = self.held_copies.lock().unwrap();
        ReadHoldStats {
            holds: held.len() as u64,
            oldest_ms: held
                .iter()
                .map(|h| u64::try_from(now_ms.saturating_sub(h.since_ms)).unwrap_or(0))
                .max()
                .unwrap_or(0),
            refused: self.read_holds_refused.load(Relaxed),
            abandoned: self.read_holds_abandoned.load(Relaxed),
        }
    }

    /// Hold the keys of the row `(slot, first)` the caller's transaction
    /// takes out of the served state: the hold is written in that
    /// transaction (atomic with the rollback) and indexed in memory. Indexed
    /// before the commit, so that no reader meets the rolled-back namespace
    /// without it; a hold whose transaction is then dropped has no key behind
    /// it, and [`Self::flush_held_releases`] (this thread's next flush) or
    /// [`Self::reconcile_read_holds`] takes it out of the index again.
    ///
    /// A row of a generation that ended (`deleg_void_cut`, in the
    /// transaction or before it) is not held: the log has the row, or it
    /// never will, and nothing would release the hold (the generation's end
    /// already ran).
    pub(crate) fn hold_row_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        slot: u64,
        first: u64,
        keys: crate::KeySet,
    ) -> Result<(), MetaError> {
        // A row that changes no name and no inode (an epoch marker) has
        // nothing for a read to miss.
        if keys.dentries.is_empty() && keys.inos.is_empty() {
            return Ok(());
        }
        if let Some(gen) = super::backup::gen_of_deleg_slot(slot) {
            if kv_get_tx(&*tx, &self.local, &super::local::deleg_void_cut_key(gen))?.is_some() {
                return Ok(());
            }
            // The release by the generation's log index
            // ([`super::local::bump_log_idx_tx`]) is exact only while the
            // index is below every row held: the strand that holds `first`
            // took the index back below it in this transaction.
            debug_assert!(
                kv_get_tx(&*tx, &self.local, &super::local::deleg_log_idx_key(gen))?
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
                    < first,
                "a delegate row is held while the generation's log index reaches it"
            );
        }
        tx.insert(&self.local, read_hold_key(slot, first), keys.encode()?);
        let mut held = self.held_copies.lock().unwrap();
        held.retain(|h| (h.slot, h.first) != (slot, first));
        let since_ms = self.held_copy_now_ms();
        held.push(HeldCopy {
            slot,
            first,
            keys,
            since_ms,
        });
        self.any_held_copy
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.hold_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        drop(held);
        self.staged_holds
            .lock()
            .unwrap()
            .push((std::thread::current().id(), slot, first));
        Ok(())
    }

    /// The hold epoch: a counter bumped whenever a hold is indexed. A read
    /// takes it before its session wait and asks [`Self::hold_raced`] after
    /// it read.
    pub fn hold_epoch(&self) -> u64 {
        self.hold_epoch.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether a hold touching `keys` was indexed since `epoch` (taken with
    /// [`Self::hold_epoch`] before the read's session wait): the read passed
    /// the wait before the hold and may have read the rolled-back namespace.
    /// It waits again and reads again (ADR-57, R is absolute).
    pub fn hold_raced(&self, epoch: u64, keys: &[crate::ReadKey]) -> bool {
        self.hold_epoch() != epoch && self.member_copy_touches(keys)
    }

    /// Take every in-memory hold whose key is not in the committed set out of
    /// the index (a transaction that indexed its hold was dropped and no
    /// flush followed). Under the writer lock, so that no transaction is in
    /// flight whose holds are not committed yet. Returns whether any went.
    pub fn reconcile_read_holds(&self) -> bool {
        if !self.any_held() {
            return false;
        }
        let tx = self.db.write_tx();
        self.staged_holds.lock().unwrap().clear();
        let present: std::collections::HashSet<(u64, u64)> = tx
            .prefix(&self.local, b"read_hold:")
            .filter_map(|g| g.into_inner().ok())
            .filter_map(|(k, _)| parse_read_hold_key(&k))
            .collect();
        drop(tx);
        let mut held = self.held_copies.lock().unwrap();
        let before = held.len();
        held.retain(|h| present.contains(&(h.slot, h.first)));
        let gone = held.len() != before;
        self.any_held_copy
            .store(!held.is_empty(), std::sync::atomic::Ordering::Relaxed);
        drop(held);
        if gone {
            self.session.wake();
        }
        gone
    }

    /// Tests only: index a hold of `(slot, first)` on the dentry `name` of the
    /// root in a transaction that is then dropped (a pass that failed after
    /// its strand).
    #[cfg(any(test, feature = "test-util"))]
    pub fn test_index_hold_in_dropped_tx(
        &self,
        slot: u64,
        first: u64,
        name: &str,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        self.hold_row_tx(
            &mut tx,
            slot,
            first,
            crate::KeySet {
                dentries: vec![(constellation_fs_core::types::ROOT_INO, name.into())],
                inos: Vec::new(),
            },
        )?;
        drop(tx);
        Ok(())
    }

    /// [`Self::test_index_hold_in_dropped_tx`] on the inode `ino`.
    #[cfg(any(test, feature = "test-util"))]
    pub fn test_index_ino_hold_in_dropped_tx(
        &self,
        slot: u64,
        first: u64,
        ino: u64,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        self.hold_row_tx(
            &mut tx,
            slot,
            first,
            crate::KeySet {
                dentries: Vec::new(),
                inos: vec![ino],
            },
        )?;
        drop(tx);
        Ok(())
    }

    /// Tests only: note the age of every hold at its release in `sink`
    /// (strand to release, ms on the node's wait clock).
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_hold_lifetime_sink(&self, sink: std::sync::Arc<std::sync::Mutex<Vec<u64>>>) {
        *self.hold_lifetime_sink.lock().unwrap() = Some(sink);
    }

    /// Tests only: this thread's flush after a commit.
    #[cfg(any(test, feature = "test-util"))]
    pub fn flush_held_releases_for_tests(&self) {
        self.flush_held_releases();
    }

    /// Load the persisted holds into the in-memory index: the open does, and
    /// a test drives it to model a process restart over the same store.
    pub fn reload_read_holds(&self) -> Result<(), MetaError> {
        self.load_read_holds()
    }

    /// Load the persisted holds into the in-memory index (open).
    pub(crate) fn load_read_holds(&self) -> Result<(), MetaError> {
        let r = self.db.read_tx();
        let since_ms = self.held_copy_now_ms();
        let mut held = Vec::new();
        let mut ended: Vec<Vec<u8>> = Vec::new();
        for guard in r.prefix(&self.local, b"read_hold:") {
            let (k, v) = guard.into_inner()?;
            let Some((slot, first)) = parse_read_hold_key(&k) else {
                return Err(MetaError::Invalid("read_hold key".into()));
            };
            // A generation that ended while the hold stayed (a crash between
            // the end and the release, before they shared a transaction)
            // has nothing to release it: the hold goes (and its key, below).
            if super::backup::gen_of_deleg_slot(slot).is_some_and(|gen| {
                matches!(
                    kv_get_tx(&r, &self.local, &super::local::deleg_void_cut_key(gen)),
                    Ok(Some(_))
                )
            }) {
                ended.push(k.to_vec());
                continue;
            }
            held.push(HeldCopy {
                slot,
                first,
                keys: crate::KeySet::decode(&v)?,
                since_ms,
            });
        }
        drop(r);
        if !ended.is_empty() {
            let mut tx = self.db.write_tx();
            for k in ended {
                tx.remove(&self.local, k);
            }
            tx.commit()?;
        }
        self.any_held_copy
            .store(!held.is_empty(), std::sync::atomic::Ordering::Relaxed);
        *self.held_copies.lock().unwrap() = held;
        Ok(())
    }

    /// The `(slot, first)` of every acknowledged row this node holds reads of
    /// (ADR-57): empty at quiescence.
    pub fn read_holds(&self) -> Vec<(u64, u64)> {
        self.held_copies
            .lock()
            .unwrap()
            .iter()
            .map(|h| (h.slot, h.first))
            .collect()
    }

    pub(crate) fn any_held(&self) -> bool {
        self.any_held_copy
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The row `(slot, first)` leaves the served-state gap in the caller's
    /// transaction (the log carries it, its holder rolled it back, the
    /// group was abandoned): its hold key goes with the transaction, and the
    /// in-memory hold once that committed ([`Self::flush_held_releases`]),
    /// not now: released inside the transaction, a reader polling before the
    /// commit would be let go and read the pre-commit namespace (the marker
    /// without its data).
    pub(crate) fn release_row_tx(&self, tx: &mut SingleWriterWriteTx, slot: u64, first: u64) {
        if !self.any_held() {
            return;
        }
        tx.remove(&self.local, read_hold_key(slot, first));
        self.stage_release(StagedRelease::Row(slot, first));
    }

    /// The holds of `slot` with `first <= through` leave in the caller's
    /// transaction (the generation's log index reached `through`).
    pub(crate) fn release_through_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        slot: u64,
        through: u64,
    ) -> Result<(), MetaError> {
        if !self.any_held() {
            return Ok(());
        }
        self.release_range_tx(tx, slot, through)?;
        self.stage_release(StagedRelease::Through(slot, through));
        Ok(())
    }

    /// The takeover gate re-journaled the transactions `idents` name (their
    /// identity as [`tail_identity`] gives it): the holds of member copy
    /// rows with one of those identities go, in the caller's transaction
    /// (the strand that set them is in it). A delegate's row goes with the
    /// generation's log index ([`super::local::bump_log_idx_tx`]).
    pub(crate) fn release_reshipped_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        idents: &std::collections::HashSet<(u64, u64)>,
    ) -> Result<(), MetaError> {
        if !self.any_held() || idents.is_empty() {
            return Ok(());
        }
        let member: Vec<(u64, u64)> = self
            .held_copies
            .lock()
            .unwrap()
            .iter()
            .filter(|h| (MEMBER_COPY_BASE..super::backup::deleg_epoch(0)).contains(&h.slot))
            .map(|h| (h.slot, h.first))
            .collect();
        for (slot, first) in member {
            let Some(v) = tx.get(&self.backup_tail, tail_key(slot, first))? else {
                continue;
            };
            let row: MemberTx = postcard::from_bytes(&v)?;
            let ident = tail_identity(&row.tx.records).unwrap_or((slot - MEMBER_COPY_BASE, first));
            if idents.contains(&ident) {
                self.release_row_tx(tx, slot, first);
            }
        }
        Ok(())
    }

    /// The rows of delegate generations whose rid the log carries leave the
    /// holds, in the caller's transaction (a segment's apply): the
    /// successor landed the row, under its own origin or another one
    /// (`(gen, idx)` reaching the generation's log index releases the
    /// re-ship under the same origin, [`super::local::bump_log_idx_tx`]). A
    /// row this root appended whose group never committed was never
    /// acknowledged and may never come back under its `(gen, idx)`: its
    /// rid's outcome in the log is what ends the wait for it.
    pub(crate) fn release_carried_deleg_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
    ) -> Result<(), MetaError> {
        if !self.any_held() {
            return Ok(());
        }
        let rows: Vec<(u64, u64)> = self
            .held_copies
            .lock()
            .unwrap()
            .iter()
            .filter(|h| super::backup::gen_of_deleg_slot(h.slot).is_some())
            .map(|h| (h.slot, h.first))
            .collect();
        for (slot, first) in rows {
            let Some(v) = tx.get(&self.backup_tail, tail_key(slot, first))? else {
                continue;
            };
            let row: super::DelegateTx = postcard::from_bytes(&v)?;
            if let Some(rid) = row.rid {
                if super::spec::log_carries(&*tx, self, &rid)? {
                    self.release_row_tx(tx, slot, first);
                }
            }
        }
        Ok(())
    }

    /// The generation `gen` ended in the caller's transaction (its
    /// `DelegateNodeEnded` applied, or an orphan end at a gate or a marker):
    /// the rows of it this node held are final, whatever the log carries.
    pub(crate) fn release_ended_gen_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        gen: u64,
    ) -> Result<(), MetaError> {
        self.release_slot_tx(tx, super::backup::deleg_epoch(gen))
    }

    /// [`Self::release_row_tx`] for a row an operator's abandon gave up: the
    /// row is lost by decision, counted (`status.ack.read_holds_abandoned`).
    pub(crate) fn release_abandoned_row_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        slot: u64,
        first: u64,
    ) {
        if self
            .held_copies
            .lock()
            .unwrap()
            .iter()
            .any(|h| (h.slot, h.first) == (slot, first))
        {
            self.read_holds_abandoned
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.release_row_tx(tx, slot, first);
    }

    /// Every hold of `slot` leaves in the caller's transaction.
    pub(crate) fn release_slot_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        slot: u64,
    ) -> Result<(), MetaError> {
        if !self.any_held() {
            return Ok(());
        }
        self.release_range_tx(tx, slot, u64::MAX)?;
        self.stage_release(StagedRelease::Slot(slot));
        Ok(())
    }

    fn release_range_tx(
        &self,
        tx: &mut SingleWriterWriteTx,
        slot: u64,
        through: u64,
    ) -> Result<(), MetaError> {
        let keys: Vec<Vec<u8>> = tx
            .range(
                &self.local,
                read_hold_key(slot, 0)..=read_hold_key(slot, through),
            )
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            tx.remove(&self.local, k);
        }
        Ok(())
    }

    fn stage_release(&self, r: StagedRelease) {
        let me = std::thread::current().id();
        self.staged_releases.lock().unwrap().push((me, r));
    }

    /// Apply this thread's staged releases after its transaction committed.
    /// Each is checked against the committed hold key: a hold whose key is
    /// still there stays (the transaction aborted, or the row was held
    /// again), so an abort releases nothing, and a stale staging left by a
    /// failed pass is harmless.
    pub(crate) fn flush_held_releases(&self) {
        let me = std::thread::current().id();
        let mine: Vec<StagedRelease> = {
            let mut all = self.staged_releases.lock().unwrap();
            let (mine, others): (Vec<_>, Vec<_>) = std::mem::take(&mut *all)
                .into_iter()
                .partition(|(t, _)| *t == me);
            *all = others;
            mine.into_iter().map(|(_, r)| r).collect()
        };
        let holds: Vec<(u64, u64)> = {
            let mut all = self.staged_holds.lock().unwrap();
            let (mine, others): (Vec<_>, Vec<_>) = std::mem::take(&mut *all)
                .into_iter()
                .partition(|(t, _, _)| *t == me);
            *all = others;
            mine.into_iter().map(|(_, s, f)| (s, f)).collect()
        };
        if mine.is_empty() && holds.is_empty() {
            return;
        }
        let snap = self.db.read_tx();
        let mut held = self.held_copies.lock().unwrap();
        #[cfg(any(test, feature = "test-util"))]
        let now_ms = self.held_copy_now_ms();
        #[cfg(any(test, feature = "test-util"))]
        let mut ended_ms: Vec<u64> = Vec::new();
        // A hold this thread's transaction indexed before its commit whose
        // key is not in the committed set was dropped with the transaction
        // (a read error keeps it).
        if !holds.is_empty() {
            held.retain(|h| {
                !holds.contains(&(h.slot, h.first))
                    || !matches!(
                        snap.get(&self.local, read_hold_key(h.slot, h.first)),
                        Ok(None)
                    )
            });
        }
        held.retain(|h| {
            let named = mine.iter().any(|r| match r {
                StagedRelease::Row(s, f) => (h.slot, h.first) == (*s, *f),
                StagedRelease::Slot(s) => h.slot == *s,
                StagedRelease::Through(s, t) => h.slot == *s && h.first <= *t,
            });
            if !named {
                return true;
            }
            // Gone from the committed set? (A read error keeps the hold.)
            let kept = !matches!(
                snap.get(&self.local, read_hold_key(h.slot, h.first)),
                Ok(None)
            );
            #[cfg(any(test, feature = "test-util"))]
            if !kept {
                ended_ms.push(u64::try_from(now_ms.saturating_sub(h.since_ms)).unwrap_or(0));
            }
            kept
        });
        #[cfg(any(test, feature = "test-util"))]
        if let Some(sink) = self.hold_lifetime_sink.lock().unwrap().as_ref() {
            sink.lock().unwrap().extend(ended_ms);
        }
        self.any_held_copy
            .store(!held.is_empty(), std::sync::atomic::Ordering::Relaxed);
        drop(held);
        // The readers waiting on a hold look again.
        self.session.wake();
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
    meta.release_slot_tx(tx, member_epoch(epoch))?;
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
    let mut abandoned: std::collections::BTreeMap<u64, u64> = Default::default();
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
            LogRecord::GroupAbandoned {
                epoch,
                gen: 0,
                through_known,
            } => {
                abandoned.insert(*epoch, *through_known);
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
                meta.release_row_tx(tx, member_epoch(e), first);
                out.retired += 1;
                continue;
            }
            if f.own {
                left += 1;
                continue;
            }
            if let Some(known) = abandoned.get(&e) {
                // ADR-51 (M5): a copy at or below what the claim carried is
                // in the tail the segments that follow name: it stays and
                // retires on carry; only a copy past it replays by rid.
                if ident.0 == e && ident.1 <= *known {
                    left += 1;
                    continue;
                }
                if !row.class.is_singleton() && super::spec::queue_abandoned_tx(tx, meta, e, &row)?
                {
                    out.abandoned += 1;
                }
                tx.remove(&meta.backup_tail, key);
                meta.release_abandoned_row_tx(tx, member_epoch(e), first);
                continue;
            }
            if through_of.get(&ident.0).is_some_and(|th| ident.1 > *th) {
                tx.remove(&meta.backup_tail, key);
                meta.release_row_tx(tx, member_epoch(e), first);
                out.dropped += 1;
                continue;
            }
            if f.first_of_epoch
                && f.epoch > e
                && !follows.contains(&e)
                && (row.class.is_archive() || row.class.is_singleton() || row.class.is_final())
            {
                tx.remove(&meta.backup_tail, key);
                meta.release_row_tx(tx, member_epoch(e), first);
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
            meta.release_row_tx(tx, member_epoch(e), row.tx.first);
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
        let slot = member_epoch(3);
        let mut tx = meta.db.write_tx();
        assert!(put_row_tx(&mut tx, &meta, 3, &row(5)).unwrap());
        meta.hold_row_tx(&mut tx, slot, 5, keys()).unwrap();
        tx.commit().unwrap();
        assert!(meta.member_copy_touches(&key));

        // Aborted: released inside, never committed.
        {
            let mut tx = meta.db.write_tx();
            tx.remove(&meta.backup_tail, tail_key(member_epoch(3), 5));
            meta.release_row_tx(&mut tx, slot, 5);
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
        meta.release_row_tx(&mut tx, slot, 5);
        assert!(meta.member_copy_touches(&key));
        tx.commit().unwrap();
        assert!(meta.member_copy_touches(&key), "not before the flush");
        meta.flush_held_releases();
        assert!(!meta.member_copy_touches(&key));
    }

    /// The release guard probes the hold itself, whatever slot it is in: a
    /// release staged for a delegate generation's slot whose hold key is
    /// still committed (nothing removed it) leaves the hold (it used to probe
    /// a member-copy slot that never exists for a delegate row, and always
    /// let go).
    #[test]
    fn a_staged_release_of_a_delegate_slot_keeps_a_hold_whose_key_is_still_there() {
        let meta = Meta::open_in_memory().unwrap();
        let key = [ReadKey::Dentry(1, "x".into())];
        let keys = crate::KeySet::from_records(&[crate::LogRecord::Create {
            parent: 1,
            name: "x".into(),
            ino: 7,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }]);
        let slot = super::super::backup::deleg_epoch(9);
        let mut tx = meta.db.write_tx();
        meta.hold_row_tx(&mut tx, slot, 4, keys).unwrap();
        tx.commit().unwrap();
        assert!(meta.member_copy_touches(&key));
        meta.stage_release(StagedRelease::Slot(slot));
        meta.flush_held_releases();
        assert!(
            meta.member_copy_touches(&key),
            "the hold key is still there"
        );
    }
}
