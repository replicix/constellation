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

use crate::ids::{Epoch, NodeId, Seq};
use constellation_fs_core::Ino;
use constellation_meta::delegation::{DelegationTable, Ownership};
use constellation_meta::{
    execute_mutate, BackupRole, BackupTx, CompletedOutcome, DelegateTx, InboxAck, JournalBatch,
    JournalPos, KeySet, LogRecord, Meta, MetaError, MetaStore, MutateOp, Position, ReadDelegations,
    Rid, Stranded, StrandedOp, TouchSet,
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
    /// Whether outstanding speculation that is not this node's own
    /// journal (a streamed transaction, a shadow, a hint) touches any of
    /// `keys`: state this replica holds ahead of its applied log, which a
    /// forward reply's `base` cannot name (`Meta::speculation_touches`).
    fn speculation_touches(&self, keys: &TouchSet) -> bool;

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
    /// Plan 30 §M9: journal a forwarded op's definitive refusal by rid
    /// (`Meta::journal_refusal`), so every later execution of the rid
    /// dedups to the same errno.
    fn journal_refusal(&self, rid: Rid, errno: i32) -> Result<(), MetaError>;
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
        gen: u64,
        op: &MutateOp,
        records: &[LogRecord],
    ) -> Result<bool, MetaError>;
    /// The pre-S3 stream installed `rid`'s transaction here already:
    /// adopt it as this node's own op (`Meta::adopt_streamed`).
    fn adopt_streamed(&self, rid: Rid, op: &MutateOp) -> Result<bool, MetaError>;
    /// Install the entry behind `rid`'s `Exists` refusal ahead of the
    /// log. `false` when not installed: this replica already has the
    /// refusal (applied, or streamed ahead of the log with whatever the
    /// holder streamed after it).
    fn install_hint(
        &self,
        rid: Rid,
        records: &[LogRecord],
        floor: Seq,
        epoch: Epoch,
        gen: u64,
    ) -> Result<bool, MetaError>;
    fn has_outstanding_speculation(&self) -> bool;
    /// Roll back every speculative entry accepted below `epoch` and
    /// queue its op for replay.
    fn strand_below_epoch(&self, epoch: Epoch) -> Result<Stranded, MetaError>;
    fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError>;
    fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError>;
    fn mark_replay_refused(&self, queue_seq: u64, reason: String) -> Result<(), MetaError>;
    /// Plan 30 §M9: a queued replay of an op this node executed but never
    /// acknowledged: refused, it is forgotten, not copied.
    fn mark_replay_unacked(&self, queue_seq: u64) -> Result<(), MetaError>;
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
    /// retire (plan 30 §M9: the streamed transactions `rows` confirm
    /// too), advance the applied position (to `(epoch, through)` too).
    fn apply_segment(
        &self,
        seq: Seq,
        epoch: Epoch,
        through: u64,
        rows: &[u64],
        origins: &[(u64, u64)],
        records: &[LogRecord],
    ) -> Result<Applied, MetaError>;
    /// A fenced segment (older epoch): advance the position past it
    /// without applying.
    fn skip_segment(&self, seq: Seq) -> Result<(), MetaError>;

    // ---- plan 30 §M11: delegated sub-sequencers ----

    /// The live delegation table as this replica knows it.
    fn delegation_table(&self) -> DelegationTable;
    /// Who executes an op touching `keys` (an ancestor walk on this
    /// replica's table; `Root` at once when the table is empty).
    fn resolve_ownership(&self, keys: &TouchSet) -> Ownership;
    /// Execute `op` here as the delegate of `gen` (journaled with the
    /// generation's next stream index and `deps`).
    fn delegate_execute(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        gen: u64,
        deps: Position,
    ) -> Result<(Vec<LogRecord>, u64), MetaError>;
    /// The root appends a delegate's transaction with its origin
    /// (`Ok(false)`: the rid was completed already).
    fn apply_delegate_tx(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        gen: u64,
        idx: u64,
        deps: Position,
    ) -> Result<bool, MetaError>;
    /// This node's unretired transactions of stream `gen` from `from_idx`.
    fn delegate_txs_from(&self, gen: u64, from_idx: u64, max_rows: usize) -> Vec<DelegateTx>;
    /// Plan 30 §M11: journal a delegate's refusal as the next transaction
    /// of stream `gen`.
    fn delegate_refusal(
        &self,
        rid: Rid,
        errno: i32,
        gen: u64,
        deps: Position,
    ) -> Result<(), MetaError>;
    /// The delegation origin of each journal row (for the segment).
    fn journal_origins(&self, seqs: &[u64]) -> Vec<(u64, u64)>;
    /// Whether the unshipped journal has rows that are not delegate
    /// stream rows.
    fn journal_has_undelegated(&self) -> bool;
    /// The next delegation generation (at least `at_least`).
    fn next_delegation_gen(&self, at_least: u64) -> Result<u64, MetaError>;
    /// The replica's applied position, streams included.
    fn applied_position(&self) -> Position;
    /// A replica-level "has everything `deps` names" with the void rule.
    fn reaches(&self, deps: &Position) -> bool;
    /// The streams part of [`Self::reaches`] (the root's check).
    fn reaches_streams(&self, deps: &Position) -> bool;
    /// This replica holds stream `gen` through `idx` (executed here or
    /// appended here).
    fn note_stream(&self, gen: u64, idx: u64);
    /// The stream index this replica holds of `gen`.
    fn stream_applied(&self, gen: u64) -> u64;
    /// Phase 2b: the stream index of `gen` this replica holds *from the
    /// log* (appended by a root here, applied from a segment), never a
    /// shadow's: a successor's cursor, a delegate's re-stream start.
    fn log_stream_idx(&self, gen: u64) -> u64;
    /// The highest stream index this node assigned as the delegate of
    /// `gen`.
    fn delegate_idx(&self, gen: u64) -> u64;
    /// Generation `gen` ended at `cut` (the void rule).
    fn void_stream(&self, gen: u64, cut: u64);
    /// Phase 2b (`ack=s3`): transaction `(gen, idx)` of this delegate's
    /// stream is still in its journal (no applied segment carries it).
    fn delegate_tx_pending(&self, gen: u64, idx: u64) -> bool;
    /// Phase 2b: a delegate's backup persists its transactions; returns
    /// the highest index held contiguously.
    fn deleg_backup_append(&self, gen: u64, txs: &[DelegateTx]) -> u64;
    fn deleg_backup_tail(&self, gen: u64) -> Vec<DelegateTx>;
    /// Persist (durably) that `gen` is sealed here; `false`: it could
    /// not be, and the seal must not be acknowledged.
    fn deleg_backup_seal(&self, gen: u64) -> bool;
    /// Whether this node holds anything of `gen` as its backup.
    fn deleg_backup_acked_any(&self, gen: u64) -> bool;
    fn deleg_backup_sealed(&self, gen: u64) -> bool;
    fn deleg_backup_clear(&self, gen: u64);
    /// The session watermark (what this node's client has observed).
    fn observed(&self) -> Position;
    /// Plan 30 §M11: the `deps` of a write submitted here (`observed`
    /// plus every delegation stream position this node holds or was
    /// answered with); `None` when they overflow `Streams`.
    fn deps(&self) -> Option<Position>;
    /// Plan 30 §M11: a sequencer answered this node's client at `pos`.
    fn note_frontier(&self, pos: &Position);
    /// The namespace, for ownership walks.
    fn namespace(&self) -> &dyn constellation_meta::delegation::Namespace;
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

    // ---- plan 30 §M8: read delegations (`cto=strict`) ----

    /// Both sides' delegation tables, shared with the FUSE threads (see
    /// `constellation_meta::readdeleg` for why they live in `Meta`).
    fn read_delegations(&self) -> &ReadDelegations;
    /// Plan 30 §M14: the lock tables (grants made, grants held, local
    /// locks), shared with the FUSE threads.
    fn locks(&self) -> &constellation_meta::locks::LockTables;
    /// Plan 30 §M14: whether `ino` is `dir` or below it (its primary
    /// link's ancestors), for a subtree grace.
    fn is_under(&self, ino: Ino, dir: Ino) -> bool;
    /// Persist, before answering a grant, that grants may be live until
    /// `until_ms` (the restart quarantine's horizon). `false`: it could not
    /// be persisted, and the grant must not be made.
    fn note_grant_horizon(&self, until_ms: i64) -> bool;
    /// M16: the inodes whose read delegations an op not yet executed must
    /// recall (`Meta::recall_inos_of_op_now`: an unlink's or rename's
    /// victims included).
    fn recall_inos_of_op(&self, op: &MutateOp) -> Vec<Ino>;
    /// At start: the previous incarnation's grants may be live until the
    /// returned time; the table is quarantined until then.
    fn load_grant_quarantine(&self, now_ms: i64) -> Option<i64>;
    /// Whether the unshipped journal touched what a ReadIndex for `ino`
    /// (a directory's entries with `dir`; the entry `name` → `child`)
    /// reads.
    fn unshipped_touches_read(
        &self,
        ino: Ino,
        dir: bool,
        child: Option<(&str, Option<Ino>)>,
    ) -> bool;

    // ---- plan 30 §M9: backups, streaming, the durability gate ----

    /// The holder's journal transactions from journal seq `from` on
    /// (whole transactions, at most `max_rows` rows), for a backup append
    /// or a pre-S3 stream batch.
    fn journal_txs_from(&self, from: u64, max_rows: usize) -> Vec<BackupTx>;
    /// How many of `txs` may be streamed to `subscriber` (`None`: to
    /// every subscriber) ahead of S3: the prefix before the first
    /// transaction whose manifest names a chunk still pending here that
    /// `subscriber` does not have (`Meta::releasable_prefix`).
    fn releasable_prefix(&self, txs: &[BackupTx], subscriber: Option<NodeId>) -> usize {
        let _ = subscriber;
        txs.len()
    }
    /// The highest journal seq allocated (0: none).
    fn journal_tip(&self) -> u64;
    /// Backup side: persist the holder's transactions (`true` when
    /// committed — the acknowledgement's meaning).
    fn backup_append(&self, epoch: Epoch, txs: &[BackupTx]) -> bool;
    fn backup_acked(&self, epoch: Epoch) -> u64;
    fn backup_tail(&self, epoch: Epoch) -> Vec<BackupTx>;
    /// A segment of `epoch` shipped through `through` with journal
    /// `rows`: trim what it confirms (and every older epoch).
    fn backup_trim(&self, epoch: Epoch, through: u64, rows: &[u64]);
    fn backup_clear(&self);
    fn backup_role(&self) -> Option<BackupRole>;
    fn set_backup_role(&self, role: BackupRole);
    /// Persist the seal (`true` when durable — only then is a takeover
    /// attempted).
    fn backup_seal(&self, epoch: Epoch) -> bool;
    fn backup_sealed_epoch(&self) -> Epoch;
    /// A takeover re-applies one of the predecessor's transactions into
    /// this node's own journal (plan 30 §M3b's replay-from-records path),
    /// completing `rid` once. Every chunk an applied manifest names is
    /// enrolled as a pending upload (plan 30 §M9 × §M4: the predecessor
    /// may never have uploaded it; see `Meta::apply_adopted_records`).
    fn apply_records_journaled(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
    ) -> Result<(), MetaError>;
    /// Install a streamed transaction ahead of the log
    /// (`SpecKind::Streamed`).
    fn install_streamed(
        &self,
        epoch: Epoch,
        first: u64,
        last: u64,
        records: &[LogRecord],
    ) -> Result<(), MetaError>;
    /// The holder's durability watermark for its own reads
    /// (`Meta::durability_pending`).
    fn set_durable(&self, gate: bool, jseq: u64, lost: bool);

    // ---- holder state ----

    /// `Meta::holder_epoch`: written the instant a CAS makes this node the
    /// holder, cleared when it stops holding.
    fn set_holder_epoch(&self, epoch: Epoch);
    /// The persisted deposition flag (`local["lease_lost"]`).
    fn lost_persisted(&self) -> bool;
    fn persist_lost(&self, lost: bool) -> Result<(), MetaError>;

    // ---- plan 30 §M10: heartbeat promises ----

    /// The last promise this node issued (`Meta::promise_issued`).
    fn promise_issued(&self) -> i64;
    /// Persist a promise before its PUT (`Meta::promise_issue`): `false`
    /// while a continuation-epoch join holds the gate (nothing written).
    fn issue_promise(&self, until_unix_ms: i64) -> Result<bool, MetaError>;
    /// The continuation-epoch hold this node owns (`local["epoch_hold"]`),
    /// so a restarted hold owner re-adopts it and can flush its journal.
    fn epoch_hold_persisted(&self) -> Option<Epoch>;
    fn persist_epoch_hold(&self, hold: Option<Epoch>) -> Result<(), MetaError>;
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

    fn speculation_touches(&self, keys: &TouchSet) -> bool {
        // An error reads as "touched": the reply then waits for the log.
        Meta::speculation_touches(self, keys).unwrap_or(true)
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

    fn journal_refusal(&self, rid: Rid, errno: i32) -> Result<(), MetaError> {
        Meta::journal_refusal(self, rid, errno)
    }

    fn journal_next_seq(&self) -> Result<u64, MetaError> {
        Meta::journal_next_seq(self)
    }

    fn journal_acked_seq(&self) -> Result<u64, MetaError> {
        Meta::journal_acked_seq(self)
    }

    fn adopt_streamed(&self, rid: Rid, op: &MutateOp) -> Result<bool, MetaError> {
        Meta::adopt_streamed(self, rid, op)
    }

    fn install_shadow(
        &self,
        rid: Rid,
        epoch: Epoch,
        gen: u64,
        op: &MutateOp,
        records: &[LogRecord],
    ) -> Result<bool, MetaError> {
        Meta::install_shadow_from(self, rid, epoch, gen, op, records)
    }

    fn install_hint(
        &self,
        rid: Rid,
        records: &[LogRecord],
        floor: Seq,
        epoch: Epoch,
        gen: u64,
    ) -> Result<bool, MetaError> {
        Meta::install_hint_from(self, Some(rid), records, floor, epoch, gen)
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

    fn mark_replay_unacked(&self, queue_seq: u64) -> Result<(), MetaError> {
        Meta::mark_replay_unacked(self, queue_seq)
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
        rows: &[u64],
        origins: &[(u64, u64)],
        records: &[LogRecord],
    ) -> Result<Applied, MetaError> {
        let pending: TouchSet = Meta::pending_touches(self)?;
        let applied =
            Meta::apply_segment_rows(self, seq, epoch, through, rows, origins, records, &pending)?;
        self.note_foreign_applied(records);
        // Plan 30 §M11: the per-generation applied index, and the void
        // rule for a recalled generation (its cut is what this replica
        // holds of it now: everything appended is before the record).
        for (gen, idx) in origins {
            self.session().note_stream(*gen, *idx);
        }
        for rec in records {
            if let LogRecord::Recall { gen, .. } = rec {
                let cut = self.session().stream_applied(*gen);
                self.session().void_stream(*gen, cut);
                // Phase 2b: the read delegations its delegate granted,
                // and the tail its backup held, are over.
                self.read_delegations()
                    .void_epoch(crate::core::DELEG_READ_EPOCH_BASE + *gen);
                let _ = Meta::deleg_backup_clear(self, *gen);
            }
        }
        // Plan 30 §M8: a newer epoch voids the delegations an older
        // holder granted (each was capped by that holder's lease, which is
        // the safety argument; this only stops honouring them sooner).
        self.read_delegations().void_below_epoch(epoch);
        // Plan 30 §M9 × §M6: a sealed backup's takeover marker; the
        // predecessor's acknowledged tail follows it.
        if records
            .iter()
            .any(|r| matches!(r, LogRecord::TailFollows { .. }))
        {
            self.session().owe(JournalPos {
                epoch,
                jseq: through,
            });
        }
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

    fn delegation_table(&self) -> DelegationTable {
        Meta::delegation_table(self)
    }

    fn resolve_ownership(&self, keys: &TouchSet) -> Ownership {
        Meta::resolve_ownership(self, keys)
    }

    fn delegate_execute(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        gen: u64,
        deps: Position,
    ) -> Result<(Vec<LogRecord>, u64), MetaError> {
        let (records, idx) = Meta::delegate_execute(self, op, rid, gen, deps)?;
        self.session().note_stream(gen, idx);
        Ok((records, idx))
    }

    fn apply_delegate_tx(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        gen: u64,
        idx: u64,
        deps: Position,
    ) -> Result<bool, MetaError> {
        let applied = Meta::apply_delegate_tx(self, records, rid, gen, idx, deps)?;
        self.session().note_stream(gen, idx);
        Ok(applied)
    }

    fn delegate_txs_from(&self, gen: u64, from_idx: u64, max_rows: usize) -> Vec<DelegateTx> {
        Meta::delegate_txs_from(self, gen, from_idx, max_rows).unwrap_or_default()
    }

    fn delegate_refusal(
        &self,
        rid: Rid,
        errno: i32,
        gen: u64,
        deps: Position,
    ) -> Result<(), MetaError> {
        let idx = Meta::delegate_refusal(self, rid, errno, gen, deps)?;
        self.session().note_stream(gen, idx);
        Ok(())
    }

    fn journal_origins(&self, seqs: &[u64]) -> Vec<(u64, u64)> {
        Meta::journal_origins(self, seqs).unwrap_or_else(|_| vec![(0, 0); seqs.len()])
    }

    fn journal_has_undelegated(&self) -> bool {
        Meta::journal_has_undelegated(self).unwrap_or(true)
    }

    fn next_delegation_gen(&self, at_least: u64) -> Result<u64, MetaError> {
        Meta::next_delegation_gen(self, at_least)
    }

    fn applied_position(&self) -> Position {
        self.session().applied()
    }

    fn reaches(&self, deps: &Position) -> bool {
        self.session().reaches(deps)
    }

    fn reaches_streams(&self, deps: &Position) -> bool {
        self.session().reaches_streams(deps)
    }

    fn note_stream(&self, gen: u64, idx: u64) {
        self.session().note_stream(gen, idx)
    }

    fn delegate_tx_pending(&self, gen: u64, idx: u64) -> bool {
        Meta::delegate_tx_pending(self, gen, idx).unwrap_or(true)
    }

    fn deleg_backup_append(&self, gen: u64, txs: &[DelegateTx]) -> u64 {
        Meta::deleg_backup_append(self, gen, txs).unwrap_or(0)
    }

    fn deleg_backup_tail(&self, gen: u64) -> Vec<DelegateTx> {
        Meta::deleg_backup_tail(self, gen).unwrap_or_default()
    }

    fn deleg_backup_seal(&self, gen: u64) -> bool {
        match Meta::deleg_backup_seal(self, gen) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, gen, "persisting a delegate backup's seal failed");
                false
            }
        }
    }

    fn deleg_backup_acked_any(&self, gen: u64) -> bool {
        Meta::deleg_backup_acked(self, gen).unwrap_or(0) > 0
    }

    fn deleg_backup_sealed(&self, gen: u64) -> bool {
        Meta::deleg_backup_sealed(self, gen).unwrap_or(false)
    }

    fn deleg_backup_clear(&self, gen: u64) {
        let _ = Meta::deleg_backup_clear(self, gen);
    }

    fn stream_applied(&self, gen: u64) -> u64 {
        self.session().stream_applied(gen)
    }

    fn log_stream_idx(&self, gen: u64) -> u64 {
        Meta::log_stream_idx(self, gen).unwrap_or(0)
    }

    fn delegate_idx(&self, gen: u64) -> u64 {
        Meta::delegate_idx(self, gen).unwrap_or(0)
    }

    fn void_stream(&self, gen: u64, cut: u64) {
        self.session().void_stream(gen, cut)
    }

    fn observed(&self) -> Position {
        self.session().observed()
    }

    fn deps(&self) -> Option<Position> {
        self.session().deps()
    }

    fn note_frontier(&self, pos: &Position) {
        self.session().note_frontier(pos)
    }

    fn namespace(&self) -> &dyn constellation_meta::delegation::Namespace {
        self
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

    fn read_delegations(&self) -> &ReadDelegations {
        Meta::read_delegations(self)
    }

    fn locks(&self) -> &constellation_meta::locks::LockTables {
        Meta::locks(self)
    }

    fn is_under(&self, ino: Ino, dir: Ino) -> bool {
        use constellation_meta::delegation::Namespace;
        let mut cur = ino;
        for _ in 0..4096 {
            if cur == dir {
                return true;
            }
            match self.primary_parent(cur) {
                Some(p) if p != cur => cur = p,
                _ => return false,
            }
        }
        false
    }

    fn note_grant_horizon(&self, until_ms: i64) -> bool {
        match Meta::note_grant_horizon(self, until_ms) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "persisting the read-grant horizon failed; not granting");
                false
            }
        }
    }

    fn load_grant_quarantine(&self, now_ms: i64) -> Option<i64> {
        Meta::load_grant_quarantine(self, now_ms)
    }

    fn recall_inos_of_op(&self, op: &MutateOp) -> Vec<Ino> {
        Meta::recall_inos_of_op_now(self, op)
    }

    fn unshipped_touches_read(
        &self,
        ino: Ino,
        dir: bool,
        child: Option<(&str, Option<Ino>)>,
    ) -> bool {
        Meta::unshipped_touches_read(self, ino, dir, child)
    }

    fn journal_txs_from(&self, from: u64, max_rows: usize) -> Vec<BackupTx> {
        Meta::journal_txs_from(self, from, max_rows).unwrap_or_default()
    }

    fn releasable_prefix(&self, txs: &[BackupTx], subscriber: Option<NodeId>) -> usize {
        // An error reads as "nothing is releasable": the S3 ship still
        // carries everything, gated by its own plan.
        Meta::releasable_prefix(self, txs, subscriber).unwrap_or(0)
    }

    fn journal_tip(&self) -> u64 {
        Meta::journal_tip(self).unwrap_or(0)
    }

    fn backup_append(&self, epoch: Epoch, txs: &[BackupTx]) -> bool {
        match Meta::backup_append(self, epoch, txs) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "persisting a backup append failed; not acknowledging");
                false
            }
        }
    }

    fn backup_acked(&self, epoch: Epoch) -> u64 {
        Meta::backup_acked(self, epoch).unwrap_or(0)
    }

    fn backup_tail(&self, epoch: Epoch) -> Vec<BackupTx> {
        Meta::backup_tail(self, epoch).unwrap_or_default()
    }

    fn backup_trim(&self, epoch: Epoch, through: u64, rows: &[u64]) {
        let _ = Meta::backup_trim(self, epoch, through, rows);
    }

    fn backup_clear(&self) {
        let _ = Meta::backup_clear(self);
    }

    fn backup_role(&self) -> Option<BackupRole> {
        Meta::backup_role(self).ok().flatten()
    }

    fn set_backup_role(&self, role: BackupRole) {
        let _ = Meta::set_backup_role(self, role);
    }

    fn backup_seal(&self, epoch: Epoch) -> bool {
        Meta::backup_seal(self, epoch).is_ok()
    }

    fn backup_sealed_epoch(&self) -> Epoch {
        Meta::backup_sealed_epoch(self).unwrap_or(0)
    }

    fn apply_records_journaled(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
    ) -> Result<(), MetaError> {
        // Adopted: an adopted manifest's chunks are enrolled as pending
        // uploads (`Meta::apply_adopted_records`); the other callers
        // (`Delegate`/`Recall` records) name no chunk.
        Meta::apply_adopted_records(self, records, rid)
    }

    fn install_streamed(
        &self,
        epoch: Epoch,
        first: u64,
        last: u64,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        Meta::install_streamed(self, epoch, first, last, records)
    }

    fn set_durable(&self, gate: bool, jseq: u64, lost: bool) {
        self.session().set_durable(gate, jseq, lost)
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

    fn promise_issued(&self) -> i64 {
        Meta::promise_issued(self).unwrap_or(0)
    }

    fn issue_promise(&self, until_unix_ms: i64) -> Result<bool, MetaError> {
        Meta::promise_issue(self, until_unix_ms)
    }

    fn epoch_hold_persisted(&self) -> Option<Epoch> {
        Meta::kv_get(self, "epoch_hold")
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
            .filter(|e| *e > 0)
    }

    fn persist_epoch_hold(&self, hold: Option<Epoch>) -> Result<(), MetaError> {
        // (Written only when it changes; synced like the epoch's other
        // persisted state, M16.)
        Meta::kv_set_durable(self, "epoch_hold", &hold.unwrap_or(0).to_string())
    }
}
