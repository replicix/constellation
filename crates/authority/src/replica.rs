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

use crate::ids::{Epoch, NodeId, Now, Seq, WaitMs};
use constellation_fs_core::Ino;
use constellation_meta::delegation::{DelegationTable, Ownership};
use constellation_meta::locks::LockTag;
use constellation_meta::{
    execute_tagged, BackupRole, BackupTx, CompletedOutcome, DelegateTx, InboxAck, JournalBatch,
    JournalPos, KeySet, LogRecord, Meta, MetaError, MetaStore, MutateOp, Position, ReadDelegations,
    Rid, Stranded, StrandedOp, TouchSet,
};
use constellation_types::Code;

/// What applying a foreign segment did (`Meta::apply_segment`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    pub stranded: Stranded,
    pub skipped: usize,
    pub retired: usize,
    pub inserted_before_local: bool,
    /// Plan 43 C1-4: what the segment did to this node's member copies.
    pub member: constellation_meta::MemberOutcome,
}

pub trait Replica {
    // ---- executing ops (holder side, local fast path, lease path) ----

    /// Validate and journal `op` under `rid` (`execute_mutate`). Plan 30
    /// §M14 phase 2: refused with `MetaError::LockLapsed`, executing
    /// nothing, when a grant the fencing token `tag` names is no longer
    /// live at `now` (`execute_tagged`).
    fn execute(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        tag: &LockTag,
        now: Now,
    ) -> Result<Vec<LogRecord>, MetaError>;
    /// The records a same-tenure execution of `rid` produced, if still
    /// held (`Meta::recent_outcome`).
    fn recent_outcome(&self, rid: Rid) -> Option<Vec<LogRecord>>;
    fn remember_outcome(&self, rid: Rid, records: &[LogRecord]);
    fn forget_acked_through(&self, node: u64, incarnation: u32, through: u64);
    /// The log position `rid` completed at, if this replica has applied
    /// (or journaled) its completion.
    fn completed_position(&self, rid: Rid) -> Result<Option<Seq>, MetaError>;
    /// M13: what the log says about `rid` — executed, or refused with an
    /// code (an inbox refusal is an outcome too).
    fn completed_outcome(&self, rid: Rid) -> Result<Option<CompletedOutcome>, MetaError>;
    /// Whether `rid`'s outcome row came from an applied segment, not this
    /// node's journal (`Meta::outcome_from_log`).
    fn outcome_from_log(&self, rid: Rid) -> bool;
    /// Plan 43 C2 (D6): the unshipped journal transaction behind `rid`'s
    /// outcome row (`Meta::outcome_row`).
    fn outcome_row(&self, rid: Rid) -> Option<constellation_meta::OutcomeRow>;
    /// The entry behind an `EEXIST` refusal (`MutateOutcome::Exists`).
    fn entry_as_record(&self, parent: Ino, name: &str) -> Option<LogRecord>;
    fn manifest(&self, ino: Ino) -> Option<Vec<u8>>;
    fn entry_exists(&self, parent: Ino, name: &str) -> bool;
    /// The inode `parent/name` names here, for the requester-side
    /// ordering gate's conflict keys (`forward::conflict_keys`).
    fn lookup_ino(&self, parent: Ino, name: &str) -> Option<Ino>;
    /// The non-directory inodes with more than one name here that `op`
    /// takes a name of away (an unlink, a rename's or an exchange's
    /// names): what the op changes beyond the names it carries
    /// (`Core::reply_base`).
    fn linked_victims(&self, op: &MutateOp) -> Vec<Ino>;
    /// Every name of `ino`, in key order, while the delegation table is not
    /// empty (`Meta::names_of_ino`): the keys of an op that removes an
    /// inode's primary name include the next name's
    /// (`TouchSet::from_op_in_names`).
    fn names_of_ino(&self, ino: Ino) -> Vec<(Ino, String)>;
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
        tag: &LockTag,
        now: Now,
    ) -> Result<Vec<LogRecord>, MetaError>;
    /// Journal a refusal (`Refused { rid, code }`) with its position.
    fn journal_inbox_refusal(
        &self,
        rid: Rid,
        code: Code,
        ack: InboxAck,
        op: Option<&MutateOp>,
    ) -> Result<(), MetaError>;
    /// Plan 30 §M9: journal a forwarded op's definitive refusal by rid
    /// (`Meta::journal_refusal`), so every later execution of the rid
    /// dedups to the same code.
    /// `op`: the refused op, whose observed keys keep the refusal behind
    /// a deferred transaction they depend on (`JournalTx::observed`).
    fn journal_refusal(&self, rid: Rid, code: Code, op: Option<&MutateOp>)
        -> Result<(), MetaError>;
    /// ADR-58 C3: whether the ship plan holds `rid`'s transaction for a
    /// chunk that exists nowhere, or defers it behind another node's pending
    /// upload (`Meta::rid_behind_lost_chunk`; a wait for this node's own
    /// upload does not count).
    fn rid_behind_lost_chunk(&self, rid: Rid) -> bool;
    /// The journal seq the next row lands at, and the highest shipped.
    fn journal_next_seq(&self) -> Result<u64, MetaError>;
    fn journal_acked_seq(&self) -> Result<u64, MetaError>;
    /// Whether a row at or below journal seq `upto` is still in the
    /// journal (unshipped or held back): `Control::Barrier`'s test.
    fn journal_unshipped_through(&self, upto: u64) -> Result<bool, MetaError>;
    /// ADR-55: an upper bound of the journal seq of the latest row that is
    /// not a delegate-stream row (the root's own).
    fn journal_root_bound(&self) -> u64;
    /// ADR-55: the journal seqs of the rows that are not delegate-stream
    /// rows, still in the journal, at or below `upto` (the root's own rows a
    /// deposition would roll back).
    fn journal_root_seqs_through(&self, upto: u64) -> Result<Vec<u64>, MetaError>;

    // ---- speculation (requester side) ----

    /// Install an accepted forward's records ahead of the log. `false`
    /// when not installed: the rid is already completed here, or this
    /// node now holds a higher epoch and queued the op for replay.
    ///
    /// Plan 43 C1-4: `committed` is the reply's own word that the holder's
    /// group (or the archive) held the row when it answered: such a shadow
    /// survives a takeover of its epoch and retires by the successor's
    /// re-ship.
    ///
    /// Plan 46 M0.2 (R7): `non_stealable`: the answer was a non-stealable
    /// sequencer's (a designee or a sticky holder), uncommitted by
    /// design: the shadow is not stranded by an epoch, and retires by the
    /// rid's outcome from the log.
    #[allow(clippy::too_many_arguments)]
    fn install_shadow(
        &self,
        rid: Rid,
        epoch: Epoch,
        gen: u64,
        op: &MutateOp,
        tag: &LockTag,
        records: &[LogRecord],
        answer: (bool, Option<i64>),
    ) -> Result<bool, MetaError>;
    /// The pre-S3 stream installed `rid`'s transaction here already:
    /// adopt it as this node's own op (`Meta::adopt_streamed`).
    fn adopt_streamed(&self, rid: Rid, op: &MutateOp, tag: &LockTag) -> Result<bool, MetaError>;
    /// Install the entry behind `rid`'s `Exists` refusal ahead of the
    /// log; it retires once the applied log reaches `at`, the reply's
    /// position. `false` when not installed: this replica already has the
    /// refusal (applied, or streamed ahead of the log with whatever the
    /// holder streamed after it).
    fn install_hint(
        &self,
        rid: Rid,
        records: &[LogRecord],
        at: Position,
        epoch: Epoch,
        gen: u64,
        committed: bool,
    ) -> Result<bool, MetaError>;
    fn has_outstanding_speculation(&self) -> bool;
    /// Roll back every speculative entry accepted below `epoch` and
    /// queue its op for replay.
    fn strand_below_epoch(&self, epoch: Epoch) -> Result<Stranded, MetaError>;
    /// A deposition's strand (plan 46 M1.2 step 7, `Meta::strand_deposed`):
    /// [`Self::strand_below_epoch`], and the delegates' rows this node
    /// appended as the root are kept as a member copy of their generation
    /// instead of being rebuilt and replayed.
    fn strand_deposed(&self, floor: Epoch, own_stream: bool) -> Result<Stranded, MetaError>;
    /// Unshipped journal rows without before-images (`Meta::uncaptured_tx_count`):
    /// the only rows a strand cannot roll back.
    fn uncaptured_tx_count(&self) -> Result<u64, MetaError>;
    /// The takeover gate's re-ship, in one store transaction (plan 46 M1.2
    /// step 5, `Meta::reship_at_takeover`): the strand
    /// ([`Self::strand_below_epoch`] plus this node's own transactions as the
    /// delegate of `own_gens`, `Meta::strand_for_takeover`; `abandoned`: the
    /// epoch of a group the operator gave up, whose committed entries strand
    /// and replay too, plan 43 C1-4; `carried`: the rids of the delegate rows
    /// the tail carries; `proof`: what this claim's marker proves the log
    /// will never grant, ADR-43's orphan rule, which the claimant runs here
    /// because it never applies its own marker as a segment) and the
    /// predecessor's sealed tail, verbatim, then its move into a member copy
    /// and its clear. `MetaError::TailDiverged`: a tail record does not
    /// apply; nothing changed.
    fn reship_at_takeover(
        &self,
        strand: Option<constellation_meta::StrandArgs<'_>>,
        tail: Option<constellation_meta::TailArgs>,
    ) -> Result<constellation_meta::ReshipReport, MetaError>;
    /// [`Self::reship_at_takeover`] for the tails of several epochs,
    /// ascending (ADR-51), in the one store transaction.
    fn reship_epochs_at_takeover(
        &self,
        strand: Option<constellation_meta::StrandArgs<'_>>,
        tails: &[constellation_meta::TailArgs],
    ) -> Result<constellation_meta::ReshipReport, MetaError>;
    /// The pre-S3 stream installed the holder's journal through `pos`
    /// (`SessionState::note_streamed`).
    fn note_streamed(&self, pos: JournalPos);
    /// Streamed speculation may have been rolled back
    /// (`SessionState::clear_streamed`).
    fn clear_streamed(&self);
    /// The root ended `gen` itself: strand its own shadows and hints of
    /// it (`Meta::strand_recalled_speculation`).
    fn strand_recalled_speculation(&self, gen: u64) -> Result<Stranded, MetaError>;
    /// Whether `deps` names a transaction of a generation that ended below
    /// it (`SessionState::deps_lost`).
    fn deps_lost(&self, deps: &Position) -> bool;
    /// The log's cut of voided generation `gen` (`SessionState::note_void_cut`).
    fn note_void_cut(&self, gen: u64, cut: u64);
    /// The cut of `gen` as this replica knows it, `None` while it is not
    /// voided here (`SessionState::void_cut`).
    fn void_cut(&self, gen: u64) -> Option<u64>;
    fn pending_replays(&self) -> Result<Vec<StrandedOp>, MetaError>;
    /// Plan 46 M0.2 review (R7): strand the shadows of non-stealable
    /// answers installed before `before_ms` (wall): the owner's row has not
    /// reached the log in the bound, so it may never (its journal lost, the
    /// node retired). Rolled back and queued for replay like any strand.
    fn strand_stale_shadows(&self, before_ms: i64) -> Result<Stranded, MetaError>;
    /// `constellation repair drop-held` (`Meta::drop_held_knowing`);
    /// `unanswered(rid)`: the core knows the answer of `rid` has not left;
    /// `live_ops(hash)`: the ops in flight here whose manifest of `ino`
    /// names `hash`; `sessions`: what the engine's write sessions name.
    #[allow(clippy::too_many_arguments)]
    fn drop_held(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
        unanswered: &dyn Fn(Rid) -> bool,
        live_ops: &dyn Fn(&replicix_chunkfmt::ChunkHash) -> Vec<Rid>,
        sessions: &constellation_meta::LiveSessions,
        exclude_rolled_back: bool,
    ) -> Result<constellation_meta::DroppedHeld, MetaError>;
    /// The replay queue decoded once per generation of the queue, with
    /// each entry's keys (`Meta::queue_snapshot`): what the park rules ask.
    fn queue_snapshot(
        &self,
    ) -> Result<std::sync::Arc<constellation_meta::QueueSnapshot>, MetaError>;
    fn forget_replay(&self, queue_seq: u64) -> Result<(), MetaError>;
    fn mark_replay_refused(&self, queue_seq: u64, reason: String) -> Result<(), MetaError>;
    /// Plan 30 §M9: a queued replay of an op this node executed but never
    /// acknowledged: refused, it is forgotten, not copied.
    fn mark_replay_unacked(&self, queue_seq: u64) -> Result<(), MetaError>;

    // ---- the journal and the applied position (ship / tail) ----

    fn applied_seq(&self) -> Result<Seq, MetaError>;
    /// The highest epoch of a segment this replica applied or shipped (the
    /// epoch of its recorded applied journal position, `KV_APPLIED_POS`;
    /// a fenced segment does not move it), 0 when none is recorded. The
    /// fencing floor a core starts from (plan 43 §11 Q20 (h)): "applied,
    /// unfenced" means the same after a restart or a rebuild as for a
    /// replica that tailed every segment.
    fn applied_epoch(&self) -> Epoch;
    /// Plan 43 C1-4: row `jseq` of `epoch`'s tenure is in the log this
    /// replica applied (`SessionState::row_in_log`).
    fn row_in_log(&self, epoch: Epoch, jseq: u64) -> bool;
    /// The journal seq of epoch `epoch`'s tenure this replica has applied
    /// the log through (0: none, or the log's last applied segment is of
    /// another epoch).
    fn applied_jseq_of(&self, epoch: Epoch) -> u64;
    fn journal_len(&self) -> Result<u64, MetaError>;
    /// What the last ship plan held back or deferred (`Meta::held_summary`).
    fn held_summary(&self) -> constellation_meta::HeldSummary;
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
    /// Plan 43 C2: generation `gen`'s configuration as this replica's table
    /// records it (`None`: no `DelegateNodeGroup` record applied: the group is the
    /// delegate and the root). `Err`: the table could not be read, which
    /// is not "no record": the caller keeps what it knew.
    fn deleg_group(
        &self,
        gen: u64,
    ) -> Result<Option<constellation_meta::delegation::NodeGroupRow>, MetaError>;
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
        tag: &LockTag,
        now: Now,
    ) -> Result<(Vec<LogRecord>, u64), MetaError>;
    /// Plan 46 M1.2 step 10: the delegate journals its release of subtrees
    /// (`SubtreeReleased` rows, or one `StreamReleased`) as the next
    /// transaction of its stream; returns its index.
    fn delegate_release(
        &self,
        records: &[LogRecord],
        gen: u64,
        deps: Position,
    ) -> Result<u64, MetaError>;
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
    /// Records the verbatim appends of delegate transactions could not apply
    /// since the last call (a tolerant apply; counted, and the sim fails a
    /// seed that has one).
    fn take_deleg_append_skipped(&self) -> u64;
    /// Appended delegate rows with a rid and no completion for it (must stay 0).
    fn take_deleg_append_uncompleted(&self) -> u64;
    /// This node's unretired transactions of stream `gen` from `from_idx`.
    fn delegate_txs_from(&self, gen: u64, from_idx: u64, max_rows: usize) -> Vec<DelegateTx>;
    /// Plan 30 §M11: journal a delegate's refusal as the next transaction
    /// of stream `gen`.
    fn delegate_refusal(
        &self,
        rid: Rid,
        code: Code,
        gen: u64,
        deps: Position,
    ) -> Result<(), MetaError>;
    /// The delegation origin of each journal row (for the segment).
    fn journal_origins(&self, seqs: &[u64]) -> Vec<(u64, u64)>;
    /// Whether the unshipped journal has rows that are not delegate
    /// stream rows.
    fn journal_has_undelegated(&self) -> bool;
    /// The next delegation generation number of the tenure holding lease
    /// epoch `epoch`, in its block `epoch << 32 | n` and at least
    /// `at_least` (ADR-43); refused for a number outside the block.
    fn next_delegation_gen(&self, epoch: u64, at_least: u64) -> Result<u64, MetaError>;
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
    /// The generations of this node's unshipped journal rows that it
    /// assigned the stream indices of, as their delegate
    /// (`Meta::journal_own_gens`); empty when it cannot be read.
    fn journal_own_gens(&self) -> Vec<u64>;
    /// This node assigned stream indices under `gen`: it is, or was, one of
    /// its generations as their delegate (`Meta::delegate_idx` above 0).
    /// `None`: unreadable.
    fn delegate_assigned(&self, gen: u64) -> Option<bool>;
    /// F1: wait until every tagged write that passed its fencing-token
    /// check at the write here, under a grant no longer live in this
    /// node's table, has committed (`Meta::settle_tagged_writes`) — before
    /// a position read next is reported as covering what this node
    /// executed. `true` if it waited (one writer-lock turn); nothing
    /// while no such write is in its transaction.
    fn settle_tagged_writes(&self) -> bool;
    /// This node's takeover gate ended its own generation `gen`
    /// (`Meta::deleg_ended_at_gate`): the generation is never executed in
    /// here again.
    fn deleg_ended_at_gate(&self, gen: u64) -> bool;
    /// Plan 46 M1.2 step 8b: the generations whose `DelegateNode` record this
    /// replica holds only as a streamed entry the root's configuration did
    /// not commit (`Meta::uncommitted_streamed_grants`). Such a grant is
    /// not honoured until the log carries it.
    fn deleg_grants_uncommitted(&self) -> Result<std::collections::BTreeSet<u64>, String>;
    /// The gate's re-append of its own rows under `gens` is done: their
    /// stream counters are the log's index of each, no hole
    /// (`Meta::end_own_streams_at_log`).
    fn end_own_streams_at_log(&self, gens: &[u64]) -> Result<(), String>;
    /// Plan 43 C2e follow-up: record (synced) that this node holds a grant of
    /// `gen` on boot `boot` (`Meta::deleg_note_held`); `false`: it could not
    /// be, and the grant must not be used.
    fn deleg_note_held(&self, gen: u64, boot: u64) -> bool;
    /// This node held a grant of `gen` on a boot other than `boot`: it
    /// rebooted since, and its journal may have lost the unsynced end of the
    /// generation (`Meta::deleg_held_boot`). An unreadable record counts as
    /// another boot, and so does an unknown one (id 0) on either side.
    fn deleg_held_on_another_boot(&self, gen: u64, boot: u64) -> bool;
    /// Path B: the generations `live` does not list whose rows this
    /// journal still holds, their end not yet applied from the log
    /// (`Meta::deleg_gens_awaiting_end`). `None`: unreadable.
    fn deleg_gens_awaiting_end(&self, live: &std::collections::BTreeSet<u64>) -> Option<Vec<u64>>;
    /// The `deps` of this delegate's journaled transactions of `gen` from
    /// index `from` on (`Meta::delegate_deps_from`); `None`: unreadable.
    fn delegate_deps_from(&self, gen: u64, from: u64) -> Option<Vec<(u64, Position)>>;
    /// Generation `gen` ended at `cut` (the void rule).
    fn void_stream(&self, gen: u64, cut: u64);
    /// Whether transaction `(gen, idx)` of this delegate's streams is
    /// still in its journal (no applied segment carries it), in one seek;
    /// `None` when it cannot be read (it counts as pending).
    fn delegate_tx_pending(&self, gen: u64, idx: u64) -> Option<bool>;
    /// Plan 43 C3: the last journal seq of this node's unshipped holder
    /// append of delegation row `(gen, idx)` (`Meta::holder_append_of`);
    /// `Err` when it cannot be read.
    fn holder_append_of(&self, gen: u64, idx: u64) -> Result<Option<u64>, MetaError>;
    /// Plan 43 C3: an upper bound of the journal seqs of this node's holder
    /// appends of delegation rows, 0 for none (`Meta::holder_append_tip`).
    fn holder_append_tip(&self) -> u64;
    /// The highest journal seq of this node's unshipped journal, 0 for
    /// none (`Meta::max_journal_seq`); `None` when it cannot be read.
    fn max_journal_seq(&self) -> Option<u64>;
    /// Plan 43 C3: this node's unshipped holder appends of generation `gen`,
    /// `(idx, last journal seq)` in index order (`Meta::holder_appends`).
    fn holder_appends(&self, gen: u64) -> Result<Vec<(u64, u64)>, MetaError>;
    /// [`Self::holder_appends`] from stream index `from` on
    /// (`Meta::holder_appends_from`).
    fn holder_appends_from(&self, gen: u64, from: u64) -> Result<Vec<(u64, u64)>, MetaError>;
    /// Phase 2b: a delegate's backup persists its transactions; returns
    /// the highest index through which every transaction is in the log
    /// as this replica holds it or held here, contiguously above that
    /// (`Meta::deleg_backup_acked`). `None`: not persisted, or the
    /// acknowledgement could not be read back; nothing is answered.
    fn deleg_backup_append(&self, gen: u64, txs: &[DelegateTx], in_log_cap: u64) -> Option<u64>;
    /// `None`: the store could not be read — the caller must not answer
    /// as if it held nothing (plan 43 C2e).
    fn deleg_backup_tail(&self, gen: u64) -> Option<Vec<DelegateTx>>;
    /// Persist (durably) that `gen` is sealed here; `false`: it could
    /// not be, and the seal must not be acknowledged.
    fn deleg_backup_seal(&self, gen: u64) -> bool;
    /// Whether this node holds anything of `gen` as its backup; `None`: the
    /// store could not be read (not "nothing", plan 43 C2e).
    fn deleg_backup_holds_any(&self, gen: u64) -> Option<bool>;
    /// Whether `gen` is sealed here (persisted, so a restart keeps it);
    /// `None`: the flag could not be read (not "not sealed": the caller
    /// takes no append, plan 43 C2e).
    fn deleg_backup_sealed(&self, gen: u64) -> Option<bool>;
    fn deleg_backup_clear(&self, gen: u64);
    /// Plan 46 M1.2 step 7 (c3-spec D6): the previous roots the next root of
    /// `gen` still has to seal (persisted); `None`: unreadable.
    fn deleg_prev_roots(&self, gen: u64) -> Option<Vec<u64>>;
    /// Persist that set (empty removes it); `false`: it could not be.
    fn set_deleg_prev_roots(&self, gen: u64, nodes: &[u64]) -> bool;
    /// The session watermark (what this node's client has observed).
    fn observed(&self) -> Position;
    /// Plan 30 §M11: the `deps` of a write submitted here (`observed`
    /// plus every delegation stream position this node holds or was
    /// answered with); `None` when they overflow `Streams`.
    fn deps(&self) -> Option<Position>;
    /// What a cluster lock's release carries: `deps`, never overflowing
    /// (`SessionState::frontier`).
    fn frontier(&self) -> Position;
    /// `records` changed this replica without passing through this
    /// node's kernel: another node's op executed here (as the holder, a
    /// delegate or from the inbox), a delegate's transaction appended
    /// here as the root, or the holder's transaction streamed here ahead
    /// of the log. The kernel's caches of what they touch are dropped, as
    /// for an applied segment (plan 30 §M7). Without it a holder served
    /// another node's replaced file (git's `refs/heads/master` after a
    /// `rename` over it) from its kernel caches for up to the FUSE TTL
    /// (EC2 campaign 4 B-1).
    fn note_foreign_executed(&self, records: &[LogRecord]);
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
    /// [`Self::raise_observed`], except for the journal rows `excused`
    /// (chunk metered-own-rows: this node's own transactions whose effects
    /// it carries; `SessionState::raise_observed_excusing`).
    fn raise_observed_excusing(&self, pos: Position, excused: &[(u64, u64)]) {
        let _ = excused;
        self.raise_observed(pos)
    }
    /// Whether local reads wait for nothing `observed` names.
    fn reaches_observed(&self) -> bool {
        false
    }
    /// Chunk metered-own-rows: which of this node's ops `rids` this
    /// replica carries the effect of (`Meta::effects_installed`).
    fn effects_installed(&self, rids: &[Rid]) -> std::collections::BTreeSet<Rid> {
        let _ = rids;
        std::collections::BTreeSet::new()
    }
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
    /// Whether any delegation is live in this replica's table (one atomic
    /// read: `Meta::delegations_any`; a `DelegateNode` record flips it inside the
    /// store write that applies it). The lock lane re-checks it live.
    fn delegations_any(&self) -> bool;
    /// Plan 30 §M14: whether `ino` is `dir` or below it (its primary
    /// link's ancestors), for a subtree grace.
    fn is_under(&self, ino: Ino, dir: Ino) -> bool;
    /// Persist, before answering a grant, that grants may be live until
    /// `until` (the restart quarantine's horizon, on this node's wait
    /// clock; stored as a boot stamp: plan 43 C7a). `false`: it could not
    /// be persisted, and the grant must not be made.
    fn note_grant_horizon(&self, until: WaitMs, now: Now) -> bool;
    /// [`Replica::note_grant_horizon`] for the lock grants' restart horizon,
    /// written here, on the core, and durable when it returns: for a grace
    /// that no grant of this node asks for (`Core::acquire_cas`: the grace
    /// of a released takeover). `false`: not persisted.
    fn note_lock_grant_horizon(&self, until: WaitMs, now: Now) -> bool;
    /// The read-grant horizon of `kind` known durable
    /// (`Meta::grant_horizon_known`): a grant may be made only up to it.
    fn grant_horizon_known(&self, kind: constellation_meta::readdeleg::GrantHorizon) -> WaitMs;
    /// At start: what the previous process's delegates may still have
    /// granted (`Meta::load_deleg_grant_quarantine`); `Unknown` for an
    /// absent or unreadable key, which the caller treats as a full span.
    fn load_deleg_grant_quarantine(
        &self,
        now: Now,
        default_span_ms: i64,
    ) -> constellation_meta::readdeleg::DelegHorizon;
    /// Every earlier delegate grant has passed: record it
    /// (`Meta::settle_deleg_grant_horizon`). A failure is logged and costs
    /// only a full span at a later restart.
    fn settle_deleg_grant_horizon(&self, now: Now);
    /// M16: the inodes whose read delegations an op not yet executed must
    /// recall (`Meta::recall_inos_of_op_now`: an unlink's or rename's
    /// victims included).
    fn recall_inos_of_op(&self, op: &MutateOp) -> Vec<Ino>;
    /// At start: the previous incarnation's grants may be live until the
    /// returned time (this node's wait clock, read from the persisted boot
    /// stamp: the raw clock on the same boot, the recorded span otherwise,
    /// `default_span_ms` for a value without one); the table is quarantined
    /// until then.
    fn load_grant_quarantine(&self, now: Now, default_span_ms: i64) -> Option<WaitMs>;
    /// At start: the previous incarnation's lock grants may be live until
    /// the returned time (this node's wait clock, as above); no new lock
    /// grant until then.
    fn load_lock_quarantine(&self, now: Now, default_span_ms: i64) -> Option<WaitMs>;
    /// Plan 43 C7a (commit 4): persist where this tenure's takeover floor
    /// ends (this node's wait clock; a boot stamp), so that a restart inside
    /// it is still inside it. `false`: it could not be persisted (the caller
    /// keeps the pending mark, below).
    fn note_takeover_floor(&self, until: WaitMs, now: Now) -> bool;
    /// At start: where the previous incarnation's takeover floor ends on this
    /// node's wait clock (`None`: none, or none left).
    fn load_takeover_floor(&self, now: Now, default_span_ms: i64) -> Option<WaitMs>;
    /// The tenure that armed the floor is over: the floor goes with it.
    fn clear_takeover_floor(&self);
    /// Durably record, before a fast claim CAS for `epoch` goes out, that it
    /// may arm a takeover floor when its marker lands. `false`: not
    /// recorded, and the CAS must not go out.
    fn set_takeover_floor_pending(&self, epoch: u64) -> bool;
    /// At start: the epoch of the claim CAS that recorded it (see above), or
    /// that the mark cannot be read (the floor may be owed).
    fn takeover_floor_pending(&self) -> constellation_meta::readdeleg::FloorMark;
    /// The floor's horizons are durable, or the CAS armed none.
    fn clear_takeover_floor_pending(&self);
    /// At start: the previous incarnation's last promise, on this node's
    /// wait clock (`Meta::load_promise_issued`), for the join gate.
    fn load_promise_issued(&self, now: Now, default_span_ms: i64) -> Option<WaitMs>;
    /// Whether the unshipped journal touched what a ReadIndex for `ino`
    /// (a directory's entries with `dir`; the entry `name` → `child`)
    /// reads.
    fn unshipped_touches_read(
        &self,
        ino: Ino,
        dir: bool,
        child: Option<(&str, Option<Ino>)>,
    ) -> bool;
    /// Whether the unshipped journal touched the record of any of `inos`
    /// (the children a read-ahead answer granted).
    fn unshipped_touches_inos(&self, inos: &[Ino]) -> bool;
    /// Up to `limit` children of `parent` in name order, `(ino, is a
    /// directory)`: from the entry `from` itself (when it exists), or from
    /// the first when `from` is `None`. The read-ahead of a ReadIndex
    /// answer.
    fn children_ahead(&self, parent: Ino, from: Option<&str>, limit: usize) -> Vec<(Ino, bool)>;

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
    /// The inodes whose chunks `node` forwarded as pending, and has not
    /// reported up, that `rid`'s transaction (executed here as stream
    /// `gen`'s, 0: the root's, with `records`) waits for — its own
    /// manifests' and those of what it depends on — and its last journal
    /// seq while unshipped (`Meta::remote_blockers`): `Core::own_chunks_for`.
    /// With `upto`, through that journal seq (the reply's position), and
    /// `node`'s own transactions through it.
    fn remote_blockers(
        &self,
        rid: Rid,
        gen: u64,
        records: &[LogRecord],
        node: NodeId,
        upto: Option<u64>,
    ) -> constellation_meta::RemoteBlockers {
        let _ = (rid, gen, records, node, upto);
        Default::default()
    }
    /// The highest journal seq allocated (0: none).
    fn journal_tip(&self) -> u64;
    /// Backup side: persist the holder's transactions (`true` when
    /// committed — the acknowledgement's meaning).
    fn backup_append(&self, epoch: Epoch, txs: &[BackupTx]) -> bool;
    /// The highest journal seq held contiguously for `epoch`; `None`: the
    /// store could not be read (not "nothing held": no answer is built on
    /// it).
    fn backup_acked(&self, epoch: Epoch) -> Option<u64>;
    /// The tail held for `epoch`; `None`: the store could not be read.
    fn backup_tail(&self, epoch: Epoch) -> Option<Vec<BackupTx>>;
    /// A segment of `epoch` shipped through `through` with journal
    /// `rows`: trim what it confirms (and every older epoch).
    fn backup_trim(&self, epoch: Epoch, through: u64, rows: &[u64]);
    fn backup_clear(&self);
    /// The role this node holds; `Err`: the store could not be read (not
    /// "no role": a claim built on that would re-ship no tail, ADR-43).
    fn backup_role(&self) -> Result<Option<BackupRole>, MetaError>;
    /// `Err`: the role is not durable; nothing may be acknowledged or
    /// claimed on it.
    fn set_backup_role(&self, role: BackupRole) -> Result<(), MetaError>;
    /// Persist the seal (`true` when durable — only then is a takeover
    /// attempted).
    fn backup_seal(&self, epoch: Epoch) -> bool;
    /// The highest epoch sealed here; `None`: the flag could not be read
    /// (not "none": the backup acknowledges nothing until it reads).
    fn backup_sealed_epoch(&self) -> Option<Epoch>;
    /// Plan 43 C1-3: this node's member copy of `epoch` (`Meta::member_copy*`):
    /// the rows a sealed holder handed a claimant, kept for the next claimant
    /// of the same lease. `put` is persisted and synced before it returns
    /// `true`.
    fn member_copy_put(&self, epoch: Epoch, config_version: u64, txs: &[BackupTx]) -> bool;
    /// Plan 43 C1-4: this node's unshipped journal rows from journal seq
    /// `from`, with the class each was journaled under, put as the member
    /// copy of `epoch` (a sealed holder's snapshot).
    fn member_copy_put_journal(
        &self,
        epoch: Epoch,
        config_version: u64,
        from: u64,
    ) -> Option<Vec<BackupTx>>;
    fn member_copy(&self, epoch: Epoch) -> Option<(u64, Vec<BackupTx>)>;
    /// Plan 46 M0.3 (G): whether a member copy of `epoch` is held; `None`:
    /// the store could not be read (not "none held").
    fn member_copy_held(&self, epoch: Epoch) -> Option<bool>;
    /// Plan 43 C1-4: whether a member copy carries a transaction of `rid`.
    fn member_copy_carries(&self, rid: Rid) -> bool;
    /// How many rows the member copies hold.
    fn member_copy_rows(&self) -> usize;
    fn member_copy_epochs(&self) -> Vec<Epoch>;
    fn member_copy_drop(&self, epoch: Epoch);
    /// Plan 43 C1-3: whether the journal holds an unshipped transaction
    /// that was executed under `epoch` (a restarted holder's rows, which it
    /// still owes a claimant that seals them).
    /// `None`: the journal could not be read.
    fn journal_has_epoch_rows(&self, epoch: Epoch) -> Option<bool>;
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
    /// [`Self::apply_records_journaled`] for a predecessor's backed-up
    /// transaction, under its delegation origin `(gen, idx)` (plan 30
    /// §M9 × §M11; `(0, 0)`: the predecessor's own row).
    fn apply_backup_tx_journaled(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        origin: (u64, u64),
    ) -> Result<(), MetaError>;
    /// Install a streamed transaction ahead of the log
    /// (`SpecKind::Streamed`), streamed by node `source`. `origin` is the
    /// transaction's delegation origin `(gen, idx)` (`(0, 0)` for none).
    #[allow(clippy::too_many_arguments)]
    fn install_streamed(
        &self,
        epoch: Epoch,
        source: NodeId,
        first: u64,
        last: u64,
        records: &[LogRecord],
        committed: bool,
        origin: (u64, u64),
    ) -> Result<(), MetaError>;
    /// The highest journal seq of `epoch` this replica holds as streamed
    /// speculation (`Meta::streamed_tip`): a stream-ahead below it is
    /// already here.
    fn streamed_tip(&self, epoch: Epoch) -> Result<Option<u64>, MetaError>;
    /// An epoch member's acknowledgement of `source`'s committed hold batch
    /// covers its earlier speculative streamed copies of `epoch` through
    /// `through`: they become committed (`Meta::promote_streamed`).
    fn promote_streamed(
        &self,
        epoch: Epoch,
        source: NodeId,
        through: u64,
    ) -> Result<usize, MetaError>;
    /// ADR-51: the committed streamed transactions of `epoch` that `source`
    /// streamed and this replica holds ahead of the log, in journal order
    /// (`Meta::kept_streamed`).
    fn kept_streamed(&self, epoch: Epoch, source: NodeId) -> Result<Vec<BackupTx>, MetaError>;
    /// ADR-51: the tenures `(holder, lease epoch)` this node sealed for a
    /// claimant's kept fetch (`None`: the store could not be read).
    fn sealed_tenures(&self) -> Option<Vec<(NodeId, Epoch)>>;
    /// ADR-51: seal the tenures, persisted and synced before it returns
    /// `true`.
    fn seal_tenures(&self, tenures: &[(NodeId, Epoch)]) -> bool;
    /// ADR-57: a pending acknowledged row this node rolled back touches
    /// `ino` (an attribute read of it or a listing of it): no read grant is
    /// made over it, the reader is answered the position and reads through.
    fn read_hold_touches_ino(&self, ino: Ino) -> bool;

    /// The holder's durability watermark for its own reads
    /// (`Meta::durability_pending`). `lone_ok` (plan 43 C2, §5.4): with the
    /// gate off, the node may answer its own clients on its disk alone.
    fn set_durable(
        &self,
        gate: bool,
        jseq: u64,
        lost: bool,
        class: constellation_meta::RowCommit,
        lone_ok: bool,
    );
    /// Plan 43 C2d: what the core knows of the delegation `gen` this node
    /// holds, for the delegate's FUSE fast path (`None`: not held).
    fn set_deleg_durable(&self, gen: u64, d: Option<constellation_meta::DelegDurable>);
    /// Plan 43 C1-2: how many journal rows were tagged with the session's
    /// published class instead of one the core set.
    fn class_from_session(&self) -> u64;
    /// Plan 43 C1-2: the class of the unshipped journal transaction that
    /// carries `rid` (the latest of them), if any.
    fn journal_class_of_rid(&self, rid: Rid) -> Option<(u64, constellation_meta::RowCommit)>;
    /// Plan 43 C1-4: the class of the journal transaction starting at
    /// journal seq `first`, if it is still in the journal.
    fn journal_tx_class(&self, first: u64) -> Option<constellation_meta::RowCommit>;
    /// Plan 43 C1-2 (D4): re-tag the journal row holding `seq`
    /// (`Meta::reclass_journal_tx`).
    fn reclass_journal_tx(
        &self,
        seq: u64,
        class: constellation_meta::RowCommit,
    ) -> Result<bool, MetaError>;

    // ---- holder state ----

    /// `Meta::holder_epoch`: written the instant a CAS makes this node the
    /// holder, cleared when it stops holding.
    fn set_holder_epoch(&self, epoch: Epoch);
    /// The persisted deposition flag (`local["lease_lost"]`).
    fn lost_persisted(&self) -> bool;
    fn persist_lost(&self, lost: bool) -> Result<(), MetaError>;

    /// ADR-51: the member sets of the persisted epoch rows that may have
    /// carried the lease tenure `(holder, lease_epoch)`
    /// (`Meta::epochs_possibly_carrying`).
    fn epochs_possibly_carrying(
        &self,
        holder: NodeId,
        lease_epoch: Epoch,
    ) -> Result<Vec<Vec<NodeId>>, MetaError>;

    // ---- plan 30 §M10: heartbeat promises ----

    /// Persist a promise before its PUT (`Meta::promise_issue`): the node
    /// joins no continuation epoch for `span_ms` from `now` on its wait
    /// clock (plan 43 C7b: a duration, never a wall instant). `false` while
    /// a continuation-epoch join holds the gate (nothing written).
    fn issue_promise(&self, span_ms: u64, now: Now) -> Result<bool, MetaError>;
    /// The continuation-epoch hold this node owns (`local["epoch_hold"]`),
    /// so a restarted hold owner re-adopts it and can flush its journal.
    fn epoch_hold_persisted(&self) -> Option<Epoch>;
    fn persist_epoch_hold(&self, hold: Option<Epoch>) -> Result<(), MetaError>;
    /// The carried lease (`epoch`, `expires_unix_ms`) whose epoch hold
    /// this node owned and let go — handed to a peer, or closed with its
    /// epoch (`local["epoch_hold_ended"]`): it never re-adopts that hold,
    /// even after a restart. The flag: it closed the epoch itself and
    /// still owes the re-claim of that lease (`Core::epoch_reclaim_due`).
    fn epoch_hold_ended(&self) -> Option<(Epoch, i64, bool)>;
    fn persist_epoch_hold_ended(&self, ended: Option<(Epoch, i64, bool)>) -> Result<(), MetaError>;
    /// The carried lease (`epoch`, `expires_unix_ms`), this node's own,
    /// whose epoch it owes the close of without holding it
    /// (`local["epoch_owes_move"]`, `PromiseState::owes_move`).
    fn epoch_owes_move(&self) -> Option<(Epoch, i64)>;
    fn persist_epoch_owes_move(&self, owed: Option<(Epoch, i64)>) -> Result<(), MetaError>;
    /// The lease (`epoch`, `expires_unix_ms`) this node last issued the
    /// release CAS for (`local["lease_released"]`, written before the CAS
    /// goes out; `PromiseState::released` after a restart).
    fn lease_released(&self) -> Option<(Epoch, i64)>;
    fn persist_lease_released(&self, released: Option<(Epoch, i64)>) -> Result<(), MetaError>;
    /// ADR-43 (orphan grants): the proof of the takeover claim this node
    /// made (`local["gate_orphan_proof"]`), written before its marker is
    /// shipped and read by the strand of a re-adoption of the same epoch.
    /// Only the epoch it names counts.
    fn gate_orphan_proof_persisted(&self) -> Option<(Epoch, constellation_meta::OrphanProof)>;
    fn persist_gate_orphan_proof(
        &self,
        proof: Option<(Epoch, constellation_meta::OrphanProof)>,
    ) -> Result<(), MetaError>;
    /// Review round 2 (SHOULD-2): the records of the takeover claim's marker
    /// (`local["gate_marker"]`), written with the orphan proof before the
    /// marker ships; the claimant's strand judges its own member copies by
    /// the marker that landed, also after a restart. Only the epoch it names
    /// counts.
    fn gate_marker_persisted(&self) -> Option<(Epoch, Vec<constellation_meta::LogRecord>)>;
    fn persist_gate_marker(
        &self,
        marker: Option<(Epoch, &[constellation_meta::LogRecord])>,
    ) -> Result<(), MetaError>;
}

impl Replica for Meta {
    fn execute(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        tag: &LockTag,
        now: Now,
    ) -> Result<Vec<LogRecord>, MetaError> {
        execute_tagged(self, op, rid, tag, now)
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

    fn outcome_from_log(&self, rid: Rid) -> bool {
        Meta::outcome_from_log(self, rid).unwrap_or(false)
    }

    fn outcome_row(&self, rid: Rid) -> Option<constellation_meta::OutcomeRow> {
        Meta::outcome_row(self, rid).ok().flatten()
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

    fn names_of_ino(&self, ino: Ino) -> Vec<(Ino, String)> {
        Meta::names_of_ino(self, ino)
    }

    fn linked_victims(&self, op: &MutateOp) -> Vec<Ino> {
        let names: Vec<(Ino, &str)> = match op {
            MutateOp::Unlink { parent, name } => vec![(*parent, name)],
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            }
            | MutateOp::Exchange {
                parent,
                name,
                new_parent,
                new_name,
            } => vec![(*parent, name), (*new_parent, new_name)],
            _ => return Vec::new(),
        };
        let mut out: Vec<Ino> = names
            .into_iter()
            .filter_map(|(parent, name)| MetaStore::lookup(self, parent, name).ok().flatten())
            .filter(|a| a.kind != constellation_fs_core::types::InodeKind::Dir && a.nlink > 1)
            .map(|a| a.ino)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
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
        tag: &LockTag,
        now: Now,
    ) -> Result<Vec<LogRecord>, MetaError> {
        let armed = Meta::pending_inbox_ack(self, ack);
        let r = execute_tagged(self, op, Some(rid), tag, now);
        drop(armed);
        r
    }

    fn journal_inbox_refusal(
        &self,
        rid: Rid,
        code: Code,
        ack: InboxAck,
        op: Option<&MutateOp>,
    ) -> Result<(), MetaError> {
        Meta::journal_inbox_refusal(self, rid, code, ack, op)
    }

    fn journal_refusal(
        &self,
        rid: Rid,
        code: Code,
        op: Option<&MutateOp>,
    ) -> Result<(), MetaError> {
        Meta::journal_refusal(self, rid, code, op)
    }

    fn rid_behind_lost_chunk(&self, rid: Rid) -> bool {
        Meta::rid_behind_lost_chunk(self, rid)
    }

    fn journal_next_seq(&self) -> Result<u64, MetaError> {
        Meta::journal_next_seq(self)
    }

    fn journal_acked_seq(&self) -> Result<u64, MetaError> {
        Meta::journal_acked_seq(self)
    }

    fn journal_unshipped_through(&self, upto: u64) -> Result<bool, MetaError> {
        Meta::journal_unshipped_through(self, upto)
    }
    fn journal_root_bound(&self) -> u64 {
        Meta::journal_root_bound(self)
    }
    fn journal_root_seqs_through(&self, upto: u64) -> Result<Vec<u64>, MetaError> {
        Meta::journal_root_seqs_through(self, upto)
    }

    fn adopt_streamed(&self, rid: Rid, op: &MutateOp, tag: &LockTag) -> Result<bool, MetaError> {
        Meta::adopt_streamed(self, rid, op, tag)
    }

    fn install_shadow(
        &self,
        rid: Rid,
        epoch: Epoch,
        gen: u64,
        op: &MutateOp,
        tag: &LockTag,
        records: &[LogRecord],
        answer: (bool, Option<i64>),
    ) -> Result<bool, MetaError> {
        Meta::install_shadow_answered(self, rid, epoch, gen, op, tag, records, answer)
    }

    fn install_hint(
        &self,
        rid: Rid,
        records: &[LogRecord],
        at: Position,
        epoch: Epoch,
        gen: u64,
        committed: bool,
    ) -> Result<bool, MetaError> {
        Meta::install_hint_committed(self, Some(rid), records, at, epoch, gen, committed)
    }

    fn has_outstanding_speculation(&self) -> bool {
        Meta::has_outstanding_speculation(self)
    }

    fn strand_deposed(&self, floor: Epoch, own_stream: bool) -> Result<Stranded, MetaError> {
        Meta::strand_deposed_with(self, floor, own_stream)
    }

    fn uncaptured_tx_count(&self) -> Result<u64, MetaError> {
        Meta::uncaptured_tx_count(self)
    }

    fn strand_below_epoch(&self, epoch: Epoch) -> Result<Stranded, MetaError> {
        Meta::strand_below_epoch(self, epoch)
    }

    fn reship_at_takeover(
        &self,
        strand: Option<constellation_meta::StrandArgs<'_>>,
        tail: Option<constellation_meta::TailArgs>,
    ) -> Result<constellation_meta::ReshipReport, MetaError> {
        Meta::reship_at_takeover(self, strand, tail)
    }

    fn reship_epochs_at_takeover(
        &self,
        strand: Option<constellation_meta::StrandArgs<'_>>,
        tails: &[constellation_meta::TailArgs],
    ) -> Result<constellation_meta::ReshipReport, MetaError> {
        Meta::reship_epochs_at_takeover(self, strand, tails)
    }

    fn strand_recalled_speculation(&self, gen: u64) -> Result<Stranded, MetaError> {
        Meta::strand_recalled_speculation(self, gen)
    }

    fn deps_lost(&self, deps: &Position) -> bool {
        self.deps_lost_deps(deps)
    }

    fn note_void_cut(&self, gen: u64, cut: u64) {
        self.session().note_void_cut(gen, cut)
    }

    fn void_cut(&self, gen: u64) -> Option<u64> {
        self.session().void_cut(gen)
    }

    fn note_streamed(&self, pos: JournalPos) {
        self.session().note_streamed(pos)
    }

    fn clear_streamed(&self) {
        self.session().clear_streamed()
    }

    fn queue_snapshot(
        &self,
    ) -> Result<std::sync::Arc<constellation_meta::QueueSnapshot>, MetaError> {
        Meta::queue_snapshot(self)
    }

    fn strand_stale_shadows(&self, before_ms: i64) -> Result<Stranded, MetaError> {
        Meta::strand_stale_shadows(self, before_ms)
    }

    fn drop_held(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
        unanswered: &dyn Fn(Rid) -> bool,
        live_ops: &dyn Fn(&replicix_chunkfmt::ChunkHash) -> Vec<Rid>,
        sessions: &constellation_meta::LiveSessions,
        exclude_rolled_back: bool,
    ) -> Result<constellation_meta::DroppedHeld, MetaError> {
        Meta::drop_held_knowing(
            self,
            ino,
            now_unix,
            remote,
            unanswered,
            live_ops,
            sessions,
            exclude_rolled_back,
        )
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

    fn applied_seq(&self) -> Result<Seq, MetaError> {
        Meta::applied_seq(self)
    }

    fn applied_epoch(&self) -> Epoch {
        Meta::applied_pos(self).map_or(0, |pos| pos.epoch)
    }

    fn row_in_log(&self, epoch: Epoch, jseq: u64) -> bool {
        self.session().row_in_log(epoch, jseq)
    }

    fn applied_jseq_of(&self, epoch: Epoch) -> u64 {
        Meta::applied_pos(self)
            .filter(|pos| pos.epoch == epoch)
            .map_or(0, |pos| pos.jseq)
    }

    fn held_summary(&self) -> constellation_meta::HeldSummary {
        Meta::held_summary(self)
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
        if let Some(pos) = pos {
            // Kept across a restart (see `Meta::open`); best effort, the
            // next segment records it again.
            let _ = Meta::note_applied_pos(self, pos);
        }
        // The rows this holder shipped are in the log as a subscriber's
        // segment names them: those above `pos` (shipped ahead of a held
        // one) read as carried after the epoch moves on, when this node
        // claims a later tenure and merges the kept rows of its own
        // (metered-shared+links seed 1781).
        self.session().advance_rows(at, pos, seqs);
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
            // A row of a voided generation the log carries after all is
            // not lost (see `note_void_cut`).
            if *gen != 0 && self.session().is_voided(*gen) {
                self.session().note_void_cut(*gen, *idx);
            }
        }
        for rec in records {
            let gen = match rec {
                // Plan 43 C2: the record carries the cut: every replica
                // voids the generation at it, not at what it holds.
                LogRecord::DelegateNodeEnded { gen, through, .. } => {
                    self.session().void_stream(*gen, *through);
                    self.session().note_void_cut(*gen, *through);
                    // The cut kept for a restart and for an install that
                    // waits for this end (`Meta::deleg_gens_awaiting_end`)
                    // is written in the segment's own transaction
                    // (`apply_segment_rows`), also when the record was
                    // installed from the root's stream before and the
                    // segment did not apply it again.
                    *gen
                }
                _ => continue,
            };
            // Phase 2b: the read delegations its delegate granted,
            // and the tail its backup held, are over.
            self.read_delegations()
                .void_gen_epoch(crate::core::DELEG_READ_EPOCH_BASE + gen);
            let _ = Meta::deleg_backup_clear(self, gen);
        }
        // Plan 30 §M8: a newer epoch voids the delegations an older
        // holder granted (each was capped by that holder's lease, which is
        // the safety argument; this only stops honouring them sooner).
        self.read_delegations().void_below_epoch(epoch);
        // Plan 30 §M9 × §M6: a sealed backup's takeover marker; the
        // predecessor's acknowledged tail follows it.
        if records.iter().any(|r| {
            // (A `TailFollowsThrough` reaching nothing declares that no tail
            // follows: nothing is owed.)
            matches!(
                r,
                LogRecord::TailFollows { .. } | LogRecord::TailFollowsThrough { through: 1.., .. }
            )
        }) {
            self.session().owe(JournalPos {
                epoch,
                jseq: through,
            });
        }
        self.session().advance_rows(
            seq,
            Some(JournalPos {
                epoch,
                jseq: through,
            }),
            rows,
        );
        Ok(Applied {
            stranded: applied.stranded,
            skipped: applied.skipped,
            retired: applied.retired,
            inserted_before_local: applied.inserted_before_local,
            member: applied.member,
        })
    }

    fn delegation_table(&self) -> DelegationTable {
        Meta::delegation_table(self)
    }

    fn deleg_group(
        &self,
        gen: u64,
    ) -> Result<Option<constellation_meta::delegation::NodeGroupRow>, MetaError> {
        Meta::deleg_group(self, gen).inspect_err(|error| {
            tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "reading a delegation's group failed");
        })
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
        tag: &LockTag,
        now: Now,
    ) -> Result<(Vec<LogRecord>, u64), MetaError> {
        let (records, idx) = Meta::delegate_execute(self, op, rid, gen, deps, tag, now)?;
        self.session().note_stream(gen, idx);
        Ok((records, idx))
    }

    fn delegate_release(
        &self,
        records: &[LogRecord],
        gen: u64,
        deps: Position,
    ) -> Result<u64, MetaError> {
        let idx = Meta::delegate_release(self, records, gen, deps)?;
        self.session().note_stream(gen, idx);
        Ok(idx)
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

    fn take_deleg_append_skipped(&self) -> u64 {
        Meta::take_deleg_append_skipped(self)
    }

    fn take_deleg_append_uncompleted(&self) -> u64 {
        Meta::take_deleg_append_uncompleted(self)
    }

    fn delegate_txs_from(&self, gen: u64, from_idx: u64, max_rows: usize) -> Vec<DelegateTx> {
        Meta::delegate_txs_from(self, gen, from_idx, max_rows).unwrap_or_default()
    }

    fn delegate_refusal(
        &self,
        rid: Rid,
        code: Code,
        gen: u64,
        deps: Position,
    ) -> Result<(), MetaError> {
        let idx = Meta::delegate_refusal(self, rid, code, gen, deps)?;
        self.session().note_stream(gen, idx);
        Ok(())
    }

    fn journal_origins(&self, seqs: &[u64]) -> Vec<(u64, u64)> {
        Meta::journal_origins(self, seqs).unwrap_or_else(|_| vec![(0, 0); seqs.len()])
    }

    fn journal_has_undelegated(&self) -> bool {
        Meta::journal_has_undelegated(self).unwrap_or(true)
    }

    fn next_delegation_gen(&self, epoch: u64, at_least: u64) -> Result<u64, MetaError> {
        Meta::next_delegation_gen(self, epoch, at_least)
    }

    fn applied_position(&self) -> Position {
        self.session().applied()
    }

    fn reaches(&self, deps: &Position) -> bool {
        self.reaches_deps(deps)
    }

    fn reaches_streams(&self, deps: &Position) -> bool {
        self.reaches_streams_deps(deps)
    }

    fn note_stream(&self, gen: u64, idx: u64) {
        self.session().note_stream(gen, idx)
    }

    fn delegate_tx_pending(&self, gen: u64, idx: u64) -> Option<bool> {
        Meta::delegate_tx_pending(self, gen, idx).ok()
    }

    fn holder_append_of(&self, gen: u64, idx: u64) -> Result<Option<u64>, MetaError> {
        Meta::holder_append_of(self, gen, idx)
    }

    fn holder_append_tip(&self) -> u64 {
        Meta::holder_append_tip(self)
    }

    fn max_journal_seq(&self) -> Option<u64> {
        Meta::max_journal_seq(self).ok()
    }

    fn holder_appends(&self, gen: u64) -> Result<Vec<(u64, u64)>, MetaError> {
        Meta::holder_appends(self, gen)
    }
    fn holder_appends_from(&self, gen: u64, from: u64) -> Result<Vec<(u64, u64)>, MetaError> {
        Meta::holder_appends_from(self, gen, from)
    }

    fn deleg_backup_append(&self, gen: u64, txs: &[DelegateTx], in_log_cap: u64) -> Option<u64> {
        // (A log index that cannot be read counts from 0: the
        // acknowledgement then covers less, never more. `in_log_cap`:
        // below this node's own unshipped appends of the stream, when it
        // is the generation's root as well.)
        let in_log = Meta::log_stream_idx(self, gen).unwrap_or(0).min(in_log_cap);
        match Meta::deleg_backup_append(self, gen, in_log, txs) {
            Ok(acked) => Some(acked),
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "persisting a delegate backup's append failed; not answering");
                None
            }
        }
    }

    fn deleg_backup_tail(&self, gen: u64) -> Option<Vec<DelegateTx>> {
        match Meta::deleg_backup_tail(self, gen) {
            Ok(tail) => Some(tail),
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "reading a delegate backup's tail failed");
                None
            }
        }
    }

    fn deleg_backup_seal(&self, gen: u64) -> bool {
        match Meta::deleg_backup_seal(self, gen) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "persisting a delegate backup's seal failed");
                false
            }
        }
    }

    fn deleg_backup_holds_any(&self, gen: u64) -> Option<bool> {
        match Meta::deleg_backup_holds_any(self, gen) {
            Ok(held) => Some(held),
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "reading what a delegate backup holds failed");
                None
            }
        }
    }

    fn deleg_backup_sealed(&self, gen: u64) -> Option<bool> {
        match Meta::deleg_backup_sealed(self, gen) {
            Ok(sealed) => Some(sealed),
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "reading a delegate backup's seal failed");
                None
            }
        }
    }

    fn deleg_backup_clear(&self, gen: u64) {
        let _ = Meta::deleg_backup_clear(self, gen);
    }

    fn deleg_prev_roots(&self, gen: u64) -> Option<Vec<u64>> {
        Meta::deleg_prev_roots(self, gen).ok()
    }

    fn set_deleg_prev_roots(&self, gen: u64, nodes: &[u64]) -> bool {
        match Meta::set_deleg_prev_roots(self, gen, nodes) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "persisting the previous roots to seal failed");
                false
            }
        }
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

    fn delegate_assigned(&self, gen: u64) -> Option<bool> {
        Meta::delegate_idx(self, gen).ok().map(|i| i > 0)
    }

    fn journal_own_gens(&self) -> Vec<u64> {
        Meta::journal_own_gens(self).unwrap_or_default()
    }

    fn settle_tagged_writes(&self) -> bool {
        Meta::settle_tagged_writes(self)
    }

    fn deleg_ended_at_gate(&self, gen: u64) -> bool {
        // An unreadable mark is taken as set: the cost is a generation
        // ended early, not a stream that never drains.
        Meta::deleg_ended_at_gate(self, gen).unwrap_or_else(|e| {
            tracing::warn!(gen = %constellation_meta::delegation::GenLabel::of(gen), error = %e, "gate-ended mark unreadable; taking it as set");
            true
        })
    }

    fn deleg_grants_uncommitted(&self) -> Result<std::collections::BTreeSet<u64>, String> {
        Meta::uncommitted_streamed_grants(self).map_err(|e| e.to_string())
    }

    fn end_own_streams_at_log(&self, gens: &[u64]) -> Result<(), String> {
        Meta::end_own_streams_at_log(self, gens).map_err(|e| e.to_string())
    }

    fn deleg_note_held(&self, gen: u64, boot: u64) -> bool {
        match Meta::deleg_note_held(self, gen, boot) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "recording a delegation's grant failed");
                false
            }
        }
    }

    fn deleg_held_on_another_boot(&self, gen: u64, boot: u64) -> bool {
        match Meta::deleg_held_boot(self, gen) {
            // Boot id 0 is unknown (no `/proc` boot id, a platform without
            // one): never the same boot, as a boot stamp's
            // (`Now::wait_of_stamp`).
            Ok(held) => held.is_some_and(|b| b == 0 || boot == 0 || b != boot),
            Err(error) => {
                tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "reading a delegation's grant record failed; taking it as another boot's");
                true
            }
        }
    }

    fn delegate_deps_from(&self, gen: u64, from: u64) -> Option<Vec<(u64, Position)>> {
        Meta::delegate_deps_from(self, gen, from)
            .map_err(|error| tracing::warn!(%error, gen = %constellation_meta::delegation::GenLabel::of(gen), "reading a stream's dependencies failed"))
            .ok()
    }

    fn deleg_gens_awaiting_end(&self, live: &std::collections::BTreeSet<u64>) -> Option<Vec<u64>> {
        Meta::deleg_gens_awaiting_end(self, live)
            .map_err(
                |error| tracing::warn!(%error, "reading the journal's ended generations failed"),
            )
            .ok()
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

    fn frontier(&self) -> Position {
        self.session().frontier()
    }

    fn note_foreign_executed(&self, records: &[LogRecord]) {
        self.note_foreign_applied(records);
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

    fn raise_observed_excusing(&self, pos: Position, excused: &[(u64, u64)]) {
        self.session().raise_observed_excusing(pos, excused)
    }

    fn reaches_observed(&self) -> bool {
        self.session().reaches_observed()
    }

    fn effects_installed(&self, rids: &[Rid]) -> std::collections::BTreeSet<Rid> {
        // An error reads as "not here": the ops' rows are then waited for.
        Meta::effects_installed(self, rids).unwrap_or_default()
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

    fn delegations_any(&self) -> bool {
        Meta::delegations_any(self)
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

    fn note_grant_horizon(&self, until: WaitMs, now: Now) -> bool {
        match Meta::note_grant_horizon(self, until, now) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "persisting the read-grant horizon failed; not granting");
                false
            }
        }
    }

    fn grant_horizon_known(&self, kind: constellation_meta::readdeleg::GrantHorizon) -> WaitMs {
        Meta::grant_horizon_known(self, kind)
    }

    fn load_deleg_grant_quarantine(
        &self,
        now: Now,
        default_span_ms: i64,
    ) -> constellation_meta::readdeleg::DelegHorizon {
        Meta::load_deleg_grant_quarantine(self, now, default_span_ms)
    }

    fn settle_deleg_grant_horizon(&self, now: Now) {
        if let Err(error) = Meta::settle_deleg_grant_horizon(self, now) {
            tracing::warn!(%error, "recording that the delegate read grants have passed failed");
        }
    }

    fn note_lock_grant_horizon(&self, until: WaitMs, now: Now) -> bool {
        match Meta::note_lock_grant_horizon(self, until, now) {
            Ok(_) => true,
            Err(error) => {
                tracing::warn!(%error, "persisting the lock-grant horizon failed");
                false
            }
        }
    }

    fn load_grant_quarantine(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        Meta::load_grant_quarantine(self, now, default_span_ms)
    }

    fn load_lock_quarantine(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        Meta::load_lock_quarantine(self, now, default_span_ms)
    }

    fn load_promise_issued(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        Meta::load_promise_issued(self, now, default_span_ms)
    }

    fn note_takeover_floor(&self, until: WaitMs, now: Now) -> bool {
        match Meta::note_takeover_floor(self, until, now) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "persisting the takeover floor failed");
                false
            }
        }
    }

    fn load_takeover_floor(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        Meta::load_takeover_floor(self, now, default_span_ms)
    }

    fn clear_takeover_floor(&self) {
        if let Err(error) = Meta::clear_takeover_floor(self) {
            tracing::warn!(%error, "removing the persisted takeover floor failed");
        }
    }

    fn set_takeover_floor_pending(&self, epoch: u64) -> bool {
        match Meta::set_takeover_floor_pending(self, epoch) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "recording the takeover floor's claim failed; not claiming");
                false
            }
        }
    }

    fn takeover_floor_pending(&self) -> constellation_meta::readdeleg::FloorMark {
        Meta::takeover_floor_mark(self)
    }

    fn clear_takeover_floor_pending(&self) {
        if let Err(error) = Meta::clear_takeover_floor_pending(self) {
            tracing::warn!(%error, "removing the takeover floor's claim mark failed");
        }
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

    fn unshipped_touches_inos(&self, inos: &[Ino]) -> bool {
        Meta::unshipped_touches_inos(self, inos)
    }

    fn children_ahead(&self, parent: Ino, from: Option<&str>, limit: usize) -> Vec<(Ino, bool)> {
        use constellation_fs_core::types::InodeKind;
        let mut out = Vec::with_capacity(limit.min(512));
        if let Some(name) = from {
            if let Ok(Some(attr)) = MetaStore::lookup(self, parent, name) {
                out.push((attr.ino, attr.kind == InodeKind::Dir));
            }
        }
        let rest = limit.saturating_sub(out.len());
        if rest > 0 {
            if let Ok(entries) =
                MetaStore::readdir_after(self, parent, from.map(str::as_bytes), rest)
            {
                out.extend(
                    entries
                        .into_iter()
                        .map(|e| (e.ino, e.kind == InodeKind::Dir)),
                );
            }
        }
        out
    }

    fn journal_txs_from(&self, from: u64, max_rows: usize) -> Vec<BackupTx> {
        Meta::journal_txs_from(self, from, max_rows).unwrap_or_default()
    }

    fn releasable_prefix(&self, txs: &[BackupTx], subscriber: Option<NodeId>) -> usize {
        // An error reads as "nothing is releasable": the S3 ship still
        // carries everything, gated by its own plan.
        Meta::releasable_prefix(self, txs, subscriber).unwrap_or(0)
    }

    fn remote_blockers(
        &self,
        rid: Rid,
        gen: u64,
        records: &[LogRecord],
        node: NodeId,
        upto: Option<u64>,
    ) -> constellation_meta::RemoteBlockers {
        // An error reads as "everything of `node`'s pending here": the
        // forwarder uploads what may be needed (a cost, never a stall).
        Meta::remote_blockers(self, rid, gen, records, node, upto).unwrap_or_else(|_| {
            let mut inos: Vec<_> = Meta::remote_chunks(self)
                .unwrap_or_default()
                .into_iter()
                .filter(|c| c.node == node)
                .map(|c| c.ino)
                .collect();
            inos.sort_unstable();
            inos.dedup();
            constellation_meta::RemoteBlockers {
                inos,
                through: None,
                own: None,
            }
        })
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

    fn backup_acked(&self, epoch: Epoch) -> Option<u64> {
        Meta::backup_acked(self, epoch)
            .inspect_err(
                |error| tracing::warn!(%error, epoch, "reading a backup's holdings failed"),
            )
            .ok()
    }

    fn backup_tail(&self, epoch: Epoch) -> Option<Vec<BackupTx>> {
        Meta::backup_tail(self, epoch)
            .inspect_err(|error| tracing::warn!(%error, epoch, "reading a backup's tail failed"))
            .ok()
    }

    fn backup_trim(&self, epoch: Epoch, through: u64, rows: &[u64]) {
        let _ = Meta::backup_trim(self, epoch, through, rows);
    }

    fn backup_clear(&self) {
        let _ = Meta::backup_clear(self);
    }

    fn backup_role(&self) -> Result<Option<BackupRole>, MetaError> {
        Meta::backup_role(self)
    }

    fn set_backup_role(&self, role: BackupRole) -> Result<(), MetaError> {
        Meta::set_backup_role(self, role)
    }

    fn backup_seal(&self, epoch: Epoch) -> bool {
        Meta::backup_seal(self, epoch).is_ok()
    }

    fn member_copy_put(&self, epoch: Epoch, config_version: u64, txs: &[BackupTx]) -> bool {
        Meta::member_copy_put(self, epoch, config_version, txs).is_ok()
    }

    fn member_copy_put_journal(
        &self,
        epoch: Epoch,
        config_version: u64,
        from: u64,
    ) -> Option<Vec<BackupTx>> {
        let rows = Meta::journal_member_rows_from(self, from).ok()?;
        Meta::member_copy_put_rows(self, epoch, config_version, &rows).ok()?;
        Some(rows.into_iter().map(|m| m.tx).collect())
    }

    fn member_copy_carries(&self, rid: Rid) -> bool {
        Meta::member_copy_carries(self, rid).unwrap_or(false)
    }

    fn member_copy_rows(&self) -> usize {
        Meta::member_copy_rows_total(self).unwrap_or(0)
    }

    fn member_copy(&self, epoch: Epoch) -> Option<(u64, Vec<BackupTx>)> {
        Meta::member_copy(self, epoch)
            .ok()
            .flatten()
            .map(|(info, txs)| (info.config_version, txs))
    }

    fn member_copy_held(&self, epoch: Epoch) -> Option<bool> {
        Meta::member_copy(self, epoch).ok().map(|c| c.is_some())
    }

    fn member_copy_epochs(&self) -> Vec<Epoch> {
        Meta::member_copy_epochs(self).unwrap_or_default()
    }

    fn member_copy_drop(&self, epoch: Epoch) {
        let _ = Meta::member_copy_drop(self, epoch);
    }

    fn journal_has_epoch_rows(&self, epoch: Epoch) -> Option<bool> {
        Meta::journal_has_epoch_rows(self, epoch)
            .inspect_err(
                |error| tracing::warn!(%error, epoch, "reading the journal's epochs failed"),
            )
            .ok()
    }

    fn backup_sealed_epoch(&self) -> Option<Epoch> {
        // (The core warns, rate-limited: every append reads it again while
        // it fails.)
        Meta::backup_sealed_epoch(self)
            .inspect_err(|error| tracing::debug!(%error, "reading a backup's seal failed"))
            .ok()
    }

    fn apply_records_journaled(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
    ) -> Result<(), MetaError> {
        // Adopted: an adopted manifest's chunks are enrolled as pending
        // uploads (`Meta::apply_adopted_records`); the other callers
        // (`DelegateNode`/`DelegateNodeEnded` records) name no chunk.
        Meta::apply_adopted_records(self, records, rid)
    }

    fn apply_backup_tx_journaled(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        origin: (u64, u64),
    ) -> Result<(), MetaError> {
        Meta::apply_adopted_records_from(self, records, rid, origin)?;
        if origin.0 != 0 {
            self.session().note_stream(origin.0, origin.1);
        }
        Ok(())
    }

    fn install_streamed(
        &self,
        epoch: Epoch,
        source: NodeId,
        first: u64,
        last: u64,
        records: &[LogRecord],
        committed: bool,
        origin: (u64, u64),
    ) -> Result<(), MetaError> {
        Meta::install_streamed_committed(
            self, epoch, source, first, last, records, committed, origin,
        )
    }

    fn streamed_tip(&self, epoch: Epoch) -> Result<Option<u64>, MetaError> {
        Meta::streamed_tip(self, epoch)
    }

    fn promote_streamed(
        &self,
        epoch: Epoch,
        source: NodeId,
        through: u64,
    ) -> Result<usize, MetaError> {
        Meta::promote_streamed(self, epoch, source, through)
    }

    fn kept_streamed(&self, epoch: Epoch, source: NodeId) -> Result<Vec<BackupTx>, MetaError> {
        Meta::kept_streamed(self, epoch, source)
    }

    fn sealed_tenures(&self) -> Option<Vec<(NodeId, Epoch)>> {
        Meta::sealed_tenures(self).ok()
    }

    fn seal_tenures(&self, tenures: &[(NodeId, Epoch)]) -> bool {
        Meta::seal_tenures(self, tenures).is_ok()
    }

    fn read_hold_touches_ino(&self, ino: Ino) -> bool {
        self.member_copy_touches(&[
            constellation_meta::ReadKey::Ino(ino),
            constellation_meta::ReadKey::Dir(ino),
        ])
    }

    fn set_durable(
        &self,
        gate: bool,
        jseq: u64,
        lost: bool,
        class: constellation_meta::RowCommit,
        lone_ok: bool,
    ) {
        self.session().set_durable(gate, jseq, lost, class, lone_ok)
    }

    fn set_deleg_durable(&self, gen: u64, d: Option<constellation_meta::DelegDurable>) {
        self.session().set_deleg_durable(gen, d)
    }

    fn journal_tx_class(&self, first: u64) -> Option<constellation_meta::RowCommit> {
        Meta::journal_tx_class(self, first).ok().flatten()
    }

    fn journal_class_of_rid(&self, rid: Rid) -> Option<(u64, constellation_meta::RowCommit)> {
        Meta::journal_rows_of_rid(self, rid)
            .ok()
            .and_then(|v| v.last().copied())
    }

    fn class_from_session(&self) -> u64 {
        constellation_meta::rows_from_session_on_this_thread()
    }

    fn reclass_journal_tx(
        &self,
        seq: u64,
        class: constellation_meta::RowCommit,
    ) -> Result<bool, MetaError> {
        Meta::reclass_journal_tx(self, seq, class)
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

    fn issue_promise(&self, span_ms: u64, now: Now) -> Result<bool, MetaError> {
        Meta::promise_issue(self, span_ms, now)
    }

    fn epoch_hold_persisted(&self) -> Option<Epoch> {
        Meta::kv_get(self, "epoch_hold")
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
            .filter(|e| *e > 0)
    }

    fn epochs_possibly_carrying(
        &self,
        holder: NodeId,
        lease_epoch: Epoch,
    ) -> Result<Vec<Vec<NodeId>>, MetaError> {
        Meta::epochs_possibly_carrying(self, holder, lease_epoch)
    }

    fn persist_epoch_hold(&self, hold: Option<Epoch>) -> Result<(), MetaError> {
        // (Written only when it changes; synced like the epoch's other
        // persisted state, M16.)
        Meta::kv_set_durable(self, "epoch_hold", &hold.unwrap_or(0).to_string())
    }

    fn epoch_hold_ended(&self) -> Option<(Epoch, i64, bool)> {
        let v = Meta::kv_get(self, "epoch_hold_ended").ok().flatten()?;
        let mut parts = v.split(':');
        let epoch = parts.next()?.parse().ok()?;
        let expires = parts.next()?.parse().ok()?;
        let reclaim = parts.next() == Some("1");
        Some((epoch, expires, reclaim))
    }

    fn persist_epoch_hold_ended(&self, ended: Option<(Epoch, i64, bool)>) -> Result<(), MetaError> {
        let v = ended
            .map(|(e, x, r)| format!("{e}:{x}:{}", u8::from(r)))
            .unwrap_or_default();
        Meta::kv_set_durable(self, "epoch_hold_ended", &v)
    }

    fn epoch_owes_move(&self) -> Option<(Epoch, i64)> {
        let v = Meta::kv_get(self, "epoch_owes_move").ok().flatten()?;
        let (epoch, expires) = v.split_once(':')?;
        Some((epoch.parse().ok()?, expires.parse().ok()?))
    }

    fn persist_epoch_owes_move(&self, owed: Option<(Epoch, i64)>) -> Result<(), MetaError> {
        let v = owed.map(|(e, x)| format!("{e}:{x}")).unwrap_or_default();
        Meta::kv_set_durable(self, "epoch_owes_move", &v)
    }

    fn lease_released(&self) -> Option<(Epoch, i64)> {
        let v = Meta::kv_get(self, "lease_released").ok().flatten()?;
        let (epoch, expires) = v.split_once(':')?;
        Some((epoch.parse().ok()?, expires.parse().ok()?))
    }

    fn persist_lease_released(&self, released: Option<(Epoch, i64)>) -> Result<(), MetaError> {
        let v = released
            .map(|(e, x)| format!("{e}:{x}"))
            .unwrap_or_default();
        Meta::kv_set_durable(self, "lease_released", &v)
    }

    fn gate_orphan_proof_persisted(&self) -> Option<(Epoch, constellation_meta::OrphanProof)> {
        let v = Meta::kv_get(self, "gate_orphan_proof").ok().flatten()?;
        let mut parts = v.split(':');
        let epoch: Epoch = parts.next()?.parse().ok()?;
        let upto: u64 = parts.next()?.parse().ok()?;
        let no_tail: u64 = parts.next()?.parse().ok()?;
        let upto_through: u64 = parts.next()?.parse().ok()?;
        Some((
            epoch,
            constellation_meta::OrphanProof {
                upto: (upto != 0).then_some(upto),
                upto_through,
                no_tail: (no_tail != 0).then_some(no_tail),
            },
        ))
    }

    fn persist_gate_orphan_proof(
        &self,
        proof: Option<(Epoch, constellation_meta::OrphanProof)>,
    ) -> Result<(), MetaError> {
        let v = proof
            .map(|(e, p)| {
                format!(
                    "{e}:{}:{}:{}",
                    p.upto.unwrap_or(0),
                    p.no_tail.unwrap_or(0),
                    p.upto_through
                )
            })
            .unwrap_or_default();
        Meta::kv_set_durable(self, "gate_orphan_proof", &v)
    }

    fn gate_marker_persisted(&self) -> Option<(Epoch, Vec<constellation_meta::LogRecord>)> {
        let v = Meta::kv_get(self, "gate_marker").ok().flatten()?;
        let (epoch, hex) = v.split_once(':')?;
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
            .collect::<Option<Vec<u8>>>()?;
        Some((epoch.parse().ok()?, postcard::from_bytes(&bytes).ok()?))
    }

    fn persist_gate_marker(
        &self,
        marker: Option<(Epoch, &[constellation_meta::LogRecord])>,
    ) -> Result<(), MetaError> {
        let v = match marker {
            None => String::new(),
            Some((e, records)) => {
                let bytes = postcard::to_allocvec(records)?;
                let mut v = format!("{e}:");
                for b in bytes {
                    v.push_str(&format!("{b:02x}"));
                }
                v
            }
        };
        Meta::kv_set_durable(self, "gate_marker", &v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_meta::delegation::GEN_EPOCH_SHIFT;

    /// ADR-59 part A (metered-shared+links seed 1781): a holder that shipped
    /// a transaction ahead of a held one, and then follows a later epoch,
    /// still reads that row as in the log; the held one it never shipped
    /// stays out. (Before: the holder's own ship advanced the position
    /// without the rows, so after the epoch moved on its kept-row merge as
    /// the next claimant found a gap below its own copy and dropped the
    /// acknowledged rows past it.)
    #[test]
    fn a_holders_row_shipped_ahead_stays_in_the_log_after_the_epoch_moves_on() {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        for name in ["a", "b", "c"] {
            meta.create(ROOT_INO, name, 0o644, 0, 0).unwrap();
        }
        let txs = meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 3);
        let rows = |i: usize| (txs[i].first..=txs[i].last).collect::<Vec<u64>>();
        let at = |jseq| Some(JournalPos { epoch: 1, jseq });
        Replica::ack_journal(&meta, &rows(0), 1, at(txs[0].last)).unwrap();
        // The third ships ahead of the second, which stays held.
        Replica::ack_journal(&meta, &rows(2), 2, at(txs[0].last)).unwrap();
        assert!(Replica::row_in_log(&meta, 1, txs[2].last));
        meta.session()
            .advance(3, Some(JournalPos { epoch: 2, jseq: 0 }));
        assert!(Replica::row_in_log(&meta, 1, txs[0].last));
        assert!(Replica::row_in_log(&meta, 1, txs[2].last), "shipped ahead");
        assert!(!Replica::row_in_log(&meta, 1, txs[1].last), "held");
    }

    /// ADR-43: the executors (`holder.rs` `mutate_request`, the delegate
    /// stream's append and drain) ask the replica's `deps_lost` before
    /// `reaches_streams`. A dependency past the cut of a key-less generation
    /// that ended before the open must be lost at that first question, or
    /// the op executes ahead of its cause once the second question voids it.
    #[test]
    fn the_replicas_deps_lost_voids_a_generation_that_ended_before_the_open() {
        let g = (5u64 << GEN_EPOCH_SHIFT) | 3;
        let rebuilt = Meta::open_in_memory().unwrap();
        rebuilt
            .apply_segment(
                1,
                1,
                &[
                    LogRecord::delegate_one(
                        4,
                        g,
                        ROOT_INO,
                        constellation_meta::SubtreeKind::Placed,
                    ),
                    LogRecord::DelegateNodeEnded {
                        node: 4,
                        gen: g,
                        through: 0,
                    },
                ],
                &TouchSet::default(),
            )
            .unwrap();
        // The generation ended before the rebuilt replica's own window:
        // it holds no key of it.
        rebuilt.kv_del(&format!("deleg_void_cut:{g}")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.db");
        {
            let meta = Meta::open(&path).unwrap();
            meta.replace_ns_from_rebuilt(&rebuilt, &Default::default())
                .unwrap();
            meta.close().unwrap();
        }
        let meta = Meta::open(&path).unwrap();
        let mut streams = constellation_meta::Streams::NONE;
        assert!(streams.raise(g, 2));
        let deps = Position {
            seq: 0,
            pending: None,
            streams,
        };
        assert!(!meta.session().is_voided(g), "not voided yet");
        assert!(Replica::deps_lost(&meta, &deps));
        assert!(meta.session().is_voided(g));
    }
}
