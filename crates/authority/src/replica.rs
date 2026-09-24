//! The core's port to the local metadata replica.
//!
//! Every method is a synchronous local call (a fjall read or write
//! transaction, sub-millisecond) and none of them can wait on anything the
//! core, a peer or S3 holds — that is the property that lets the core call
//! them from inside `handle` with no await between a decision and the
//! write it guards (the atomicity `dispatch_forward` needed between its
//! view check and its journal write). Production implements it for
//! [`constellation_meta::Meta`] (below); the model-checking stretch goal
//! (plan 30 M5) can implement it over an in-memory directory bitmap.
//!
//! The method set is exactly what the decision logic touches, and nothing
//! else in `Meta` is reachable from the core.

use crate::ids::{Epoch, Seq};
use constellation_fs_core::Ino;
use constellation_meta::{
    execute_mutate, CompletedOutcome, InboxAck, JournalBatch, JournalPos, KeySet, LogRecord, Meta,
    MetaError, MetaStore, MutateOp, Position, Rid, Stranded, StrandedOp, TouchSet,
};

/// What applying a foreign segment did (`Meta::apply_segment`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    pub stranded: Stranded,
    pub skipped: usize,
    pub retired: usize,
    pub inserted_before_local: bool,
}

pub trait Replica {
    // ---- executing ops (holder side, local fast path, lease path) ----

    /// Validate and journal `op` under `rid` (`execute_mutate`).
    fn execute(&self, op: &MutateOp, rid: Option<Rid>) -> Result<Vec<LogRecord>, MetaError>;
    /// The records a same-tenure execution of `rid` produced, if still
    /// held (`Meta::recent_outcome`).
    fn recent_outcome(&self, rid: Rid) -> Option<Vec<LogRecord>>;
    fn remember_outcome(&self, rid: Rid, records: &[LogRecord]);
    fn forget_acked_through(&self, node: u64, incarnation: u32, through: u64);
    /// The log position `rid` completed at, if this replica has applied
    /// (or journaled) its completion.
    fn completed_position(&self, rid: Rid) -> Result<Option<Seq>, MetaError>;
    /// M13: what the log says about `rid` — executed, or refused with an
    /// errno (an inbox refusal is an outcome too).
    fn completed_outcome(&self, rid: Rid) -> Result<Option<CompletedOutcome>, MetaError>;
    /// The entry behind an `EEXIST` refusal (`MutateOutcome::Exists`).
    fn entry_as_record(&self, parent: Ino, name: &str) -> Option<LogRecord>;
    fn manifest(&self, ino: Ino) -> Option<Vec<u8>>;
    fn entry_exists(&self, parent: Ino, name: &str) -> bool;
    /// The inode `parent/name` names here, for the requester-side
    /// ordering gate's conflict keys (`forward::conflict_keys`).
    fn lookup_ino(&self, parent: Ino, name: &str) -> Option<Ino>;
    /// Whether the unshipped journal (this tenure) touched any of `keys`
    /// (the stale-base rule on forward replies, `Meta::unshipped_overlaps`).
    fn unshipped_overlaps(&self, keys: &TouchSet) -> bool;

    // ---- M13: the inbox on the holder ----

    /// The position watermark: an earlier tenure already answered this
    /// op of this requester's batch.
    fn inbox_ack_covers(&self, ack: InboxAck) -> bool;
    /// Record the position of an op answered by dedup, without a row of
    /// its own.
    fn journal_inbox_ack(&self, ack: InboxAck) -> Result<(), MetaError>;
    /// Execute an inbox op with its position acked in the same
    /// transaction (`Meta::pending_inbox_ack` + `execute_mutate`).
    fn execute_inbox(
        &self,
        ack: InboxAck,
        op: &MutateOp,
        rid: Rid,
    ) -> Result<Vec<LogRecord>, MetaError>;
    /// Journal a refusal (`Refused { rid, errno }`) with its position.
    fn journal_inbox_refusal(&self, rid: Rid, errno: i32, ack: InboxAck) -> Result<(), MetaError>;
    /// The journal seq the next row lands at, and the highest shipped.
    fn journal_next_seq(&self) -> Result<u64, MetaError>;
    fn journal_acked_seq(&self) -> Result<u64, MetaError>;

    // ---- speculation (requester side) ----

    /// Install an accepted forward's records ahead of the log. `false`
    /// when not installed: the rid is already completed here, or this
    /// node now holds a higher epoch and queued the op for replay.
    fn install_shadow(
        &self,
        rid: Rid,
        epoch: Epoch,
        op: &MutateOp,
        records: &[LogRecord],
    ) -> Result<bool, MetaError>;
    fn install_hint(
        &self,
        records: &[LogRecord],
        floor: Seq,
        epoch: Epoch,
    ) -> Result<(), MetaError>;
    fn has_outstanding_speculation(&self) -> bool;
    /// Roll back every speculative entry accepted below `epoch` and
    /// queue its op for replay.
    fn strand_below_epoch(&self, epoch: Epoch) -> Result<Stranded, MetaError>;
    fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError>;
    fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError>;
    fn mark_replay_refused(&self, queue_seq: u64, reason: String) -> Result<(), MetaError>;
    /// Whether the holder captures before-images (a deposition then
    /// rolls back from them; without them the namespace is rebuilt).
    fn holder_capture(&self) -> bool;

    // ---- the journal and the applied position (ship / tail) ----

    fn applied_seq(&self) -> Result<Seq, MetaError>;
    fn journal_len(&self) -> Result<u64, MetaError>;
    /// The next unshipped journal rows, whole transactions only, at most
    /// `max` rows (plan 30 §M3b: an op's records and its `Completed`
    /// never split; M4: transactions held back behind an unrecoverable
    /// chunk are skipped).
    fn take_journal(&self, max: usize) -> Result<JournalBatch, MetaError>;
    /// The longest prefix of `batch` of at most `want` rows that ends on
    /// a transaction boundary (or the whole first transaction when even
    /// that is over `want`).
    fn whole_tx_prefix(&self, batch: &JournalBatch, want: usize) -> Result<usize, MetaError>;
    /// Own-segment recovery: if the journal head (or, with held-back
    /// transactions, a subsequence of whole transactions) equals
    /// `records` (atime rows aside), the journal seqs it covers; `None`
    /// otherwise.
    fn journal_head_matching(&self, records: &[LogRecord]) -> Result<Option<Vec<u64>>, MetaError>;
    /// The rows `seqs` shipped in segment `at`: delete them, advance the
    /// applied position (plan 30 §M6: to journal position `pos`, the
    /// segment's `(epoch, through)`), retire their speculation.
    fn ack_journal(&self, seqs: &[u64], at: Seq, pos: Option<JournalPos>) -> Result<(), MetaError>;
    /// Apply a foreign segment: strand what its epoch supersedes, apply,
    /// retire, advance the applied position (to `(epoch, through)` too).
    fn apply_segment(
        &self,
        seq: Seq,
        epoch: Epoch,
        through: u64,
        records: &[LogRecord],
    ) -> Result<Applied, MetaError>;
    /// A fenced segment (older epoch): advance the position past it
    /// without applying.
    fn skip_segment(&self, seq: Seq) -> Result<(), MetaError>;
    /// Whether the metadata tree has dirty keys to publish.
    fn has_dirty(&self) -> bool;

    // ---- plan 30 §M6: positions and the session watermark ----

    /// The acked watermark shipping `seqs` will leave (a segment's
    /// `through`).
    fn journal_through_after(&self, seqs: &[u64]) -> Result<u64, MetaError>;
    /// The unshipped journal this holder evaluates against right now
    /// (`None`: everything shipped).
    fn journal_position(&self, epoch: Epoch) -> Option<JournalPos>;
    /// A client-visible reply whose effects are not installed here
    /// observed `pos`: local reads wait for it.
    fn raise_observed(&self, pos: Position);
    /// Speculation for `keys` was installed from a reply at `pos`: reads of
    /// those keys need not wait for positions up to it.
    fn note_covering(&self, keys: KeySet, pos: Position);

    // ---- read-time atime (plan 20): ride-along and standalone ships ----

    /// Pending atime rows (read, not removed), oldest first.
    fn take_atime(&self, max: usize) -> Result<Vec<(Ino, i64, i64)>, MetaError>;
    fn clear_atime(&self, inos: &[Ino]) -> Result<(), MetaError>;
    fn drop_atime(&self) -> Result<(), MetaError>;
    fn atime_oldest_pending_ns(&self) -> Result<Option<i64>, MetaError>;

    // ---- holder state ----

    /// `Meta::holder_epoch`: written the instant a CAS makes this node the
    /// holder, cleared when it stops holding.
    fn set_holder_epoch(&self, epoch: Epoch);
    /// The persisted deposition flag (`local["lease_lost"]`).
    fn lost_persisted(&self) -> bool;
    fn persist_lost(&self, lost: bool) -> Result<(), MetaError>;
}

impl Replica for Meta {
    fn execute(&self, op: &MutateOp, rid: Option<Rid>) -> Result<Vec<LogRecord>, MetaError> {
        execute_mutate(self, op, rid)
    }

    fn recent_outcome(&self, rid: Rid) -> Option<Vec<LogRecord>> {
        Meta::recent_outcome(self, rid)
    }

    fn remember_outcome(&self, rid: Rid, records: &[LogRecord]) {
        Meta::remember_outcome(self, rid, records)
    }

    fn forget_acked_through(&self, node: u64, incarnation: u32, through: u64) {
        Meta::forget_acked_through(self, node, incarnation, through)
    }

    fn completed_position(&self, rid: Rid) -> Result<Option<Seq>, MetaError> {
        Meta::completed_position(self, rid)
    }

    fn completed_outcome(&self, rid: Rid) -> Result<Option<CompletedOutcome>, MetaError> {
        Meta::completed_outcome(self, rid)
    }

    fn entry_as_record(&self, parent: Ino, name: &str) -> Option<LogRecord> {
        Meta::entry_as_record(self, parent, name).ok().flatten()
    }

    fn manifest(&self, ino: Ino) -> Option<Vec<u8>> {
        MetaStore::manifest(self, ino).ok().flatten()
    }

    fn entry_exists(&self, parent: Ino, name: &str) -> bool {
        MetaStore::lookup(self, parent, name)
            .ok()
            .flatten()
            .is_some()
    }

    fn lookup_ino(&self, parent: Ino, name: &str) -> Option<Ino> {
        MetaStore::lookup(self, parent, name)
            .ok()
            .flatten()
            .map(|a| a.ino)
    }

    fn unshipped_overlaps(&self, keys: &TouchSet) -> bool {
        Meta::unshipped_overlaps(self, keys)
    }

    fn inbox_ack_covers(&self, ack: InboxAck) -> bool {
        Meta::inbox_ack(self, ack.epoch, ack.node)
            .ok()
            .flatten()
            .is_some_and(|w| w.covers(ack.n, ack.i))
    }

    fn journal_inbox_ack(&self, ack: InboxAck) -> Result<(), MetaError> {
        Meta::journal_inbox_ack(self, ack)
    }

    fn execute_inbox(
        &self,
        ack: InboxAck,
        op: &MutateOp,
        rid: Rid,
    ) -> Result<Vec<LogRecord>, MetaError> {
        let armed = Meta::pending_inbox_ack(self, ack);
        let r = execute_mutate(self, op, Some(rid));
        drop(armed);
        r
    }

    fn journal_inbox_refusal(&self, rid: Rid, errno: i32, ack: InboxAck) -> Result<(), MetaError> {
        Meta::journal_inbox_refusal(self, rid, errno, ack)
    }

    fn journal_next_seq(&self) -> Result<u64, MetaError> {
        Meta::journal_next_seq(self)
    }

    fn journal_acked_seq(&self) -> Result<u64, MetaError> {
        Meta::journal_acked_seq(self)
    }

    fn install_shadow(
        &self,
        rid: Rid,
        epoch: Epoch,
        op: &MutateOp,
        records: &[LogRecord],
    ) -> Result<bool, MetaError> {
        Meta::install_shadow(self, rid, epoch, op, records)
    }

    fn install_hint(
        &self,
        records: &[LogRecord],
        floor: Seq,
        epoch: Epoch,
    ) -> Result<(), MetaError> {
        Meta::install_hint(self, records, floor, epoch)
    }

    fn has_outstanding_speculation(&self) -> bool {
        Meta::has_outstanding_speculation(self)
    }

    fn strand_below_epoch(&self, epoch: Epoch) -> Result<Stranded, MetaError> {
        Meta::strand_below_epoch(self, epoch)
    }

    fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError> {
        Meta::pending_replays(self)
    }

    fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError> {
        Meta::forget_replay(self, queue_seq)
    }

    fn mark_replay_refused(&self, queue_seq: u64, reason: String) -> Result<(), MetaError> {
        Meta::mark_replay_refused(
            self,
            queue_seq,
            constellation_meta::Refusal {
                reason,
                ts_unix: constellation_fs_core::types::now_ns() / 1_000_000_000,
            },
        )
    }

    fn holder_capture(&self) -> bool {
        Meta::holder_capture(self)
    }

    fn applied_seq(&self) -> Result<Seq, MetaError> {
        Meta::applied_seq(self)
    }

    fn journal_len(&self) -> Result<u64, MetaError> {
        MetaStore::journal_len(self)
    }

    fn take_journal(&self, max: usize) -> Result<JournalBatch, MetaError> {
        Ok(Meta::take_journal_grouped(self, max)?
            .into_iter()
            .next()
            .map(|(_, batch)| batch)
            .unwrap_or_default())
    }

    fn whole_tx_prefix(&self, batch: &JournalBatch, want: usize) -> Result<usize, MetaError> {
        Meta::whole_tx_prefix(self, batch, want)
    }

    fn journal_head_matching(&self, records: &[LogRecord]) -> Result<Option<Vec<u64>>, MetaError> {
        let journaled: Vec<&LogRecord> = records
            .iter()
            .filter(|r| !matches!(r, LogRecord::Atime { .. }))
            .collect();
        Meta::match_own_segment(self, &journaled)
    }

    fn ack_journal(&self, seqs: &[u64], at: Seq, pos: Option<JournalPos>) -> Result<(), MetaError> {
        Meta::ack_journal_rows_at(self, seqs, at)?;
        self.session().advance(at, pos);
        Ok(())
    }

    fn apply_segment(
        &self,
        seq: Seq,
        epoch: Epoch,
        through: u64,
        records: &[LogRecord],
    ) -> Result<Applied, MetaError> {
        let pending: TouchSet = Meta::pending_touches(self)?;
        let applied = Meta::apply_segment(self, seq, epoch, records, &pending)?;
        self.session().advance(
            seq,
            Some(JournalPos {
                epoch,
                jseq: through,
            }),
        );
        Ok(Applied {
            stranded: applied.stranded,
            skipped: applied.skipped,
            retired: applied.retired,
            inserted_before_local: applied.inserted_before_local,
        })
    }

    fn skip_segment(&self, seq: Seq) -> Result<(), MetaError> {
        Meta::set_applied_seq(self, seq)?;
        self.session().advance(seq, None);
        Ok(())
    }

    fn journal_through_after(&self, seqs: &[u64]) -> Result<u64, MetaError> {
        Meta::journal_through_after(self, seqs)
    }

    fn journal_position(&self, epoch: Epoch) -> Option<JournalPos> {
        Meta::journal_position(self, epoch)
    }

    fn raise_observed(&self, pos: Position) {
        self.session().raise_observed(pos)
    }

    fn note_covering(&self, keys: KeySet, pos: Position) {
        self.session().note_covering(keys, pos)
    }

    fn has_dirty(&self) -> bool {
        Meta::has_dirty(self)
    }

    fn take_atime(&self, max: usize) -> Result<Vec<(Ino, i64, i64)>, MetaError> {
        MetaStore::take_atime_of(self, "p0", max)
    }

    fn clear_atime(&self, inos: &[Ino]) -> Result<(), MetaError> {
        MetaStore::clear_atime(self, "p0", inos)
    }

    fn drop_atime(&self) -> Result<(), MetaError> {
        MetaStore::drop_atime_of(self, "p0")
    }

    fn atime_oldest_pending_ns(&self) -> Result<Option<i64>, MetaError> {
        MetaStore::atime_oldest_pending_ns(self, "p0")
    }

    fn set_holder_epoch(&self, epoch: Epoch) {
        Meta::set_holder_epoch(self, epoch)
    }

    fn lost_persisted(&self) -> bool {
        matches!(
            Meta::kv_get(self, "lease_lost").ok().flatten().as_deref(),
            Some("1")
        )
    }

    fn persist_lost(&self, lost: bool) -> Result<(), MetaError> {
        Meta::kv_set(self, "lease_lost", if lost { "1" } else { "0" })
    }
}
