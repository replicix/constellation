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
//! Capture applies when this node holds the lease (`Meta::holder_epoch`
//! is set), executes as a delegate, or the speculation log is not empty.
//! (There is no switch: the plan 30 §M3b performance-gate fallback that
//! turned it off, `CONSTELLATION_HOLDER_CAPTURE`, was removed on
//! 2026-10-31 — with it off the commit rule lost and duplicated
//! acknowledged writes.) The second arm keeps M3a's invariant
//! — while any `spec` row exists, every `ns` write is captured, so a
//! rollback can never wipe an uncaptured write — for the rare local write
//! a non-holder makes (an offline designation's `Proceed`). Such a row
//! carries epoch 0 and never strands by epoch; it only retires when it
//! ships. A write outside all three (a non-holder's, nothing speculative)
//! journals its `journal_tx` row without before-images, counted
//! (`uncaptured_tx_count`): a journal holding one is not published, and
//! the ship plan holds everything after it.
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
//! Uncaptured: the two journal-counter reads, the `journal_tx` row, and
//! the `uncaptured_tx_count` counter. Shipping deletes the rows again
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
    adjust_usage_tx, counter_add_get_tx, counter_add_tx, counter_get, journal, kv_get_tx,
    kv_set_tx, ns, spec, usage_note_begin, usage_note_take, Meta, UsageTracker,
    KV_DELEG_SPEC_COUNT, KV_LOCAL_SPEC_COUNT, KV_SPEC_LIVE_COUNT, KV_UNCAPTURED_TX_COUNT,
};
use crate::TreeInode;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use constellation_mtree::record::{InodeRecord, Payload};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// Which journaled apply `Meta::apply_records_journaled_inner` runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Journaled {
    Plain,
    /// A takeover's adoption of its predecessor's backed-up transaction.
    Adopted,
    /// The root's append of a delegate's transaction (plan 45 §12 O1:
    /// its own live shadow of the rid is replaced, not applied over).
    Appended,
}

/// What a [`RowCommit::Group`]'s configuration waits on: the journal's
/// stored form of whose copies the group counts (it stays with the row
/// encoding until M1.2 makes one stream per node).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupOn {
    /// No member listed: an applied, unfenced log segment (`ack=s3`, a
    /// holder with no backup yet but one to be had).
    Archive,
    /// The root group: the holder's journal and every backup the lease
    /// object lists hold it.
    Root,
    /// The delegation's group (C2): a recorded backup's tail, or the root's
    /// appended row.
    Delegate,
}

/// Plan 46 M1.1 step 3 (§5.1): the commit rule a journal row was written
/// under, stamped with the configuration it was written in. A row carries
/// its configuration's version, not the class of one decision: what a
/// deposition or a takeover does to it (a committed row goes to the member
/// copy, a singleton's is replayed) follows what its client was told, which
/// is the rule of the configuration in force at its position. It is fixed
/// at the journaling; the one change after the fact is a row whose own
/// client was answered with the gate off: it is re-tagged `Singleton`
/// ([`Meta::reclass_journal_tx`]), so that its strand behaviour matches the
/// answer (S7: "gated implies committed at the answer").
///
/// Replaces the five-valued `RowClass` (Lone = `Singleton`, NonStealable =
/// `Final`, Archive, RootGroup and DelegGroup = `Group`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RowCommit {
    /// Committed once the configuration's members hold the row (or the
    /// archive, none listed): the answer waited for it.
    Group {
        /// The configuration's version: the lease's `config_version`, or
        /// the generation's `DelegGroup` version.
        config_version: u64,
        on: GroupOn,
    },
    /// A non-stealable configuration (a designee, an epoch hold): never
    /// stolen, never gated.
    Final,
    /// Nothing but this node's disk holds it: a singleton sequencer's row
    /// (kept as a variant until M2.1: the replay and the member-copy drop
    /// rules read it).
    Singleton { config_version: u64 },
}

impl Default for RowCommit {
    fn default() -> Self {
        RowCommit::LONE
    }
}

impl RowCommit {
    /// The forms of version 0: what code that does not know the
    /// configuration it journals under stamps (placeholders, tests).
    pub const LONE: RowCommit = RowCommit::Singleton { config_version: 0 };
    pub const ARCHIVE: RowCommit = RowCommit::group(0, GroupOn::Archive);
    pub const ROOT_GROUP: RowCommit = RowCommit::group(0, GroupOn::Root);
    pub const DELEG_GROUP: RowCommit = RowCommit::group(0, GroupOn::Delegate);
    pub const FINAL: RowCommit = RowCommit::Final;
    /// What the `per_row_class` negative control adds to a row's real
    /// version: no configuration reaches it (a CAS bumps a version by one).
    pub const PER_ROW_SKEW: u64 = 1_000;

    pub const fn group(config_version: u64, on: GroupOn) -> RowCommit {
        RowCommit::Group { config_version, on }
    }

    /// Q12's rule: the rows whose acknowledgement waited for a group or the
    /// archive, so the row is committed once it is answered. Not `Final`
    /// (its holder cannot be replaced, nothing waits) and not `Singleton`.
    pub fn gated(self) -> bool {
        matches!(self, RowCommit::Group { .. })
    }

    pub fn is_singleton(self) -> bool {
        matches!(self, RowCommit::Singleton { .. })
    }

    pub fn is_final(self) -> bool {
        matches!(self, RowCommit::Final)
    }

    /// A group that waits for the archive alone (no member listed).
    pub fn is_archive(self) -> bool {
        matches!(
            self,
            RowCommit::Group {
                on: GroupOn::Archive,
                ..
            }
        )
    }

    /// The configuration version the row carries (`Final` has none).
    pub fn config_version(self) -> Option<u64> {
        match self {
            RowCommit::Group { config_version, .. } | RowCommit::Singleton { config_version } => {
                Some(config_version)
            }
            RowCommit::Final => None,
        }
    }

    /// A singleton's row waits for its group instead: the delegation's
    /// group at the same configuration version. Any other rule is kept.
    pub fn demoted(self) -> RowCommit {
        match self {
            RowCommit::Singleton { config_version } => {
                RowCommit::group(config_version, GroupOn::Delegate)
            }
            other => other,
        }
    }

    /// The same rule at version 0: what a test that does not know the
    /// configuration compares.
    pub fn versionless(self) -> RowCommit {
        match self {
            RowCommit::Group { on, .. } => RowCommit::group(0, on),
            RowCommit::Final => RowCommit::Final,
            RowCommit::Singleton { .. } => RowCommit::LONE,
        }
    }

    /// The row re-tagged a singleton's, in the version it carries (a
    /// `Final` one in `version`).
    pub fn as_singleton(self, version: u64) -> RowCommit {
        RowCommit::Singleton {
            config_version: self.config_version().unwrap_or(version),
        }
    }

    /// The row re-tagged a group that waits for the archive, in the version
    /// it carries (a `Final` one in `version`).
    pub fn as_archive(self, version: u64) -> RowCommit {
        RowCommit::group(self.config_version().unwrap_or(version), GroupOn::Archive)
    }

    const VERSION_BITS: u32 = 60;

    /// The row's rule in one word (the session publishes it atomically): the
    /// variant in the top bits, the version below. Versions are far below
    /// 2^60 (one CAS bumps one).
    pub(crate) fn to_bits(self) -> u64 {
        let tag = |t: u64, v: u64| {
            assert!(
                v < 1 << Self::VERSION_BITS,
                "row version {v} exceeds 60 bits"
            );
            (t << Self::VERSION_BITS) | v
        };
        match self {
            RowCommit::Singleton { config_version } => tag(0, config_version),
            RowCommit::Group {
                config_version,
                on: GroupOn::Archive,
            } => tag(1, config_version),
            RowCommit::Group {
                config_version,
                on: GroupOn::Root,
            } => tag(2, config_version),
            RowCommit::Group {
                config_version,
                on: GroupOn::Delegate,
            } => tag(3, config_version),
            RowCommit::Final => tag(4, 0),
        }
    }

    pub(crate) fn from_bits(bits: u64) -> RowCommit {
        let v = bits & ((1 << Self::VERSION_BITS) - 1);
        match bits >> Self::VERSION_BITS {
            0 => RowCommit::Singleton { config_version: v },
            1 => RowCommit::group(v, GroupOn::Archive),
            2 => RowCommit::group(v, GroupOn::Root),
            3 => RowCommit::group(v, GroupOn::Delegate),
            4 => RowCommit::Final,
            tag => panic!("unknown row commit tag {tag} in {bits:#x}"),
        }
    }
}

/// A journal transaction as the simulation's row-version oracle reads it.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug)]
pub struct AuditRow {
    /// The transaction's first journal seq.
    pub first: u64,
    /// The lease epoch it executed under (0: this node held none).
    pub epoch: u64,
    /// The delegation generation (0: the root's own) and the index in it.
    pub gen: u64,
    pub idx: u64,
    /// The rule it was journaled under, with its configuration's version.
    pub class: RowCommit,
}

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
    /// delegate origin ship with it in the segment envelope (and, on a
    /// deposed root, go when a later tenure's segment carries their outcomes
    /// under another identity: `spec::superseded_root_appends_tx`).
    pub gen: u64,
    pub idx: u64,
    /// Plan 43 C1-2: the class the row was journaled under
    /// (`journal::PendingClass`, else the session's published class). It
    /// sits right after `idx`, before `rid`, so that [`JournalTxHead`]
    /// decodes it as part of its prefix.
    pub class: RowCommit,
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
    /// The `ns` keys the transaction observed without writing them — a
    /// refusal's op (`journal::PendingObserved`): the ship plan treats
    /// them as dependencies (`store::held`). Empty for a transaction
    /// that wrote what it read.
    pub observed: Vec<Vec<u8>>,
    /// Plan 30 §M14 phase 2: the grants the op was issued under (its
    /// fencing token; `journal::PendingLockTag`). A replay after a
    /// deposition carries it, so a replay under a grant that ended is
    /// rejected as the first execution would have been.
    pub lock_tag: crate::locks::LockTag,
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
    pub class: RowCommit,
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

/// The [`Meta::journal_origin`] key of the transaction at journal seq
/// `first` of delegation origin `(gen, idx)`.
fn journal_origin_key(gen: u64, idx: u64, first: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(24);
    k.extend_from_slice(&gen.to_be_bytes());
    k.extend_from_slice(&idx.to_be_bytes());
    k.extend_from_slice(&first.to_be_bytes());
    k
}

fn decode_journal_origin_key(k: &[u8]) -> Result<(u64, u64, u64), MetaError> {
    if k.len() != 24 {
        return Err(MetaError::Invalid("journal_origin key".into()));
    }
    let word = |i: usize| u64::from_be_bytes(k[i * 8..i * 8 + 8].try_into().unwrap());
    Ok((word(0), word(1), word(2)))
}

/// The transaction at journal seq `first` is journaled under delegation
/// origin `(gen, idx)`: indexed by origin ([`Meta::journal_origin`]),
/// in the transaction writing its `journal_tx` row. Nothing for `gen` 0.
pub(crate) fn put_journal_origin(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    gen: u64,
    idx: u64,
    first: u64,
) {
    if gen != 0 {
        tx.insert(
            &meta.journal_origin,
            journal_origin_key(gen, idx, first),
            [],
        );
    }
}

/// [`put_journal_origin`] undone: the `journal_tx` row at `first` leaves
/// (retired or stranded), in the transaction removing it.
pub(crate) fn remove_journal_origin(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    gen: u64,
    idx: u64,
    first: u64,
) {
    if gen != 0 {
        tx.remove(&meta.journal_origin, journal_origin_key(gen, idx, first));
    }
}

/// The outstanding transactions of delegation origin `gen` from stream
/// index `from_idx` on, as `(idx, first)` in index order (journal order
/// within one index), lazily: one seek, then a row per transaction
/// taken. Rows at or below the acked watermark are left out, as every
/// journal scan does ([`read_journal_tx_heads`]).
fn journal_origins_from<'a>(
    r: &'a impl Readable,
    meta: &'a Meta,
    gen: u64,
    from_idx: u64,
) -> Result<impl Iterator<Item = Result<(u64, u64), MetaError>> + 'a, MetaError> {
    let floor = journal::acked_watermark(r, &meta.local)?;
    let lo = journal_origin_key(gen, from_idx, 0);
    let hi = journal_origin_key(gen, u64::MAX, u64::MAX);
    Ok(r.range(&meta.journal_origin, lo..=hi)
        .filter_map(move |guard| {
            let row = guard
                .key()
                .map_err(MetaError::from)
                .and_then(|k| decode_journal_origin_key(&k));
            meta.note_local_rows_read(1);
            match row {
                Ok((_, idx, first)) if first > floor => Some(Ok((idx, first))),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            }
        }))
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
    meta.note_local_rows_read(out.len());
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
    meta.note_local_rows_read(out.len());
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
        // F1: what admitted the op, judged again now that it holds the
        // writer lock (it may have waited seconds for it): the fast path's
        // lease or delegation first (refused, the op goes through the
        // core), then its fencing token on this node's clock now (refused,
        // `LockLapsed`). Nothing is written yet, so a refusal drops the
        // transaction unwritten.
        journal::PendingAdmission::check_at_write()?;
        // This node's own execution as the delegate of a generation its
        // takeover gate ended (`Meta::strand_for_takeover` marks every own
        // generation in the strand's transaction, under this writer lock):
        // refused unwritten, and the op goes through the core, whose gate
        // parks it until the stranded rows are replayed. The engine's
        // mirror of the delegation table lags the core by an event, so an
        // op admitted before the strand reaches this lock after it; the
        // store is the fence. The root's append of a delegate's row
        // (`idx: Some`) is not an execution of this node's.
        if let Some(d) = journal::PendingDelegate::peek() {
            if d.idx.is_none() && kv_get_tx(tx, &self.local, &deleg_stranded_key(d.gen))?.is_some()
            {
                return Err(MetaError::NotAdmitted);
            }
        }
        journal::PendingLockTag::check_at_write(&self.locks)?;
        let start = journal::peek_next_seq(tx, &self.local)?;
        let epoch = self.holder_epoch.load(Ordering::SeqCst);
        // A holder always captures; a non-holder only while speculation
        // exists — answered from two counters, not a scan of `spec`.
        // Plan 30 §M11: a delegate's transactions are speculation like a
        // holder's (retired by the log, stranded by a recall), so they
        // are captured too.
        let capturing = epoch != 0
            || journal::PendingDelegate::peek().is_some()
            || counter_get(tx, &self.local, KV_SPEC_LIVE_COUNT)? > 0
            || counter_get(tx, &self.local, KV_LOCAL_SPEC_COUNT)? > 0;
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
        // Plan 43 C2d (§5.1), plan 46 M1.1 step 4: the delegate fast path's
        // row is stamped now, under the writer lock, with the rule the core
        // has published for the generation, if the version of the
        // configuration that rule was decided under is the one this store
        // records now (the latest `DelegGroup` record applied), and a
        // `Singleton` rule follows no open gated row of this node's own.
        // The row is not classed again by its place in the stream: a
        // changed version (a record applied since the publication), an open
        // gated row, or a generation no longer published refuses the op
        // unwritten, and the core decides it from what it reads then.
        // Decided before anything is recorded.
        let class_at_journal = match journal::PendingDelegate::peek() {
            Some(d)
                if d.idx.is_none()
                    && journal::ClassAtJournal::active()
                    && journal::PendingClass::current().is_none() =>
            {
                let version =
                    crate::delegation::read_group_tx(&*tx, self, d.gen)?.map_or(0, |g| g.version);
                let gated = kv_get_tx(tx, &self.local, &deleg_gated_key(d.gen))?
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                let idx = kv_get_tx(tx, &self.local, &deleg_idx_key(d.gen))?
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
                    + 1;
                let commit = self
                    .session()
                    .deleg_commit_at_journal(d.gen, version, gated, idx)
                    .ok_or(MetaError::NotAdmitted)?;
                // The same across this node's generations (a takeover's
                // strand ends every own generation together; the keys of one
                // generation's gated row can be another's by the time a
                // later row touches them): a `Singleton` rule behind an open
                // gated row of another generation is not stamped either.
                if commit.is_singleton() && gated_open_elsewhere_tx(tx, self, d.gen, true)? {
                    return Err(MetaError::NotAdmitted);
                }
                Some(commit)
            }
            _ => None,
        };
        let (rid, op) = match journal::PendingLocalOp::take() {
            Some((rid, op)) => (rid, op),
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
        let delegated = journal::PendingDelegate::peek().is_some();
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
        // The only captured row of a delegation origin in this view: what
        // `spec::DelegRows` holds is of rows gone since (it is never
        // shrunk), and it starts afresh.
        let deleg_first = gen != 0
            && spec_seq.is_some()
            && counter_add_get_tx(tx, &self.local, KV_DELEG_SPEC_COUNT, 1)? == 1;
        // Plan 43 C1-2: the row's class, decided at the journaling. The
        // core says it (`journal::PendingClass`) around every call of its
        // own; a transaction under a delegation origin is `Lone` at C1 (C2
        // makes the delegate's `DelegGroup`, C3 moves the root's appended
        // rows); the FUSE fast path's threads publish nothing of their own
        // and take the class the core last published with the gate, which
        // is what their wait follows.
        let class = match (journal::PendingClass::current(), class_at_journal) {
            (Some(c), _) | (None, Some(c)) => c,
            (None, None) if delegated => RowCommit::LONE,
            (None, None) => {
                journal::note_from_session();
                self.session().published_class()
            }
        };
        // Plan 46 M1.1 step 4 (review): the root's append of a delegate's
        // stream row is stamped with the generation's `DelegGroup` version
        // as this transaction reads it.
        let class = match (class, gen != 0 && journal::PendingClass::stream_version()) {
            (RowCommit::Singleton { .. }, true) => RowCommit::Singleton {
                config_version: crate::delegation::read_group_tx(&*tx, self, gen)?
                    .map_or(0, |g| g.version),
            },
            (class, _) => class,
        };
        // Plan 46 M1.1 step 4 (review, I2): the core's own `Singleton` row,
        // decided from the journal as it stood when the core read it, is
        // journaled only once the core holds the writer lock, and the fast
        // path's rows (a gated one, under a rule published before the
        // core's) went in meanwhile. A `Singleton` row journaled behind an
        // open gated row of the node's own is replayed without it at a
        // root's crash, after it was answered: here it waits for its group
        // instead (the rule at the same version), and the core reads the
        // outcome ([`journal::take_last_journaled`]) and waits too.
        let class = if gen != 0
            && journal::PendingClass::follows_open_gated_rows()
            && class.is_singleton()
            && (gated_mark_open(&*tx, self, gen)? || gated_open_elsewhere_tx(tx, self, gen, true)?)
        {
            class.demoted()
        } else {
            class
        };
        // Plan 43 C2 (hysteresis): a gated row of this delegate's own node's
        // clients is the generation's gated high-water: the core decides a
        // `Lone` row only after it has committed (`deleg_gated_key`; the
        // core's `deleg_lone_held_by_gated`). The row keeps the rule it
        // was given.
        if gen != 0 && (class_at_journal.is_some() || journal::PendingClass::own()) && class.gated()
        {
            let key = deleg_gated_key(gen);
            let held = kv_get_tx(tx, &self.local, &key)?
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            if idx > held {
                kv_set_tx(tx, &self.local, &key, &idx.to_string());
                // (The marks found closed leave the open list here: it stays
                // a few entries.)
                gated_open_elsewhere_tx(tx, self, gen, true)?;
                note_gated_open_tx(tx, self, gen)?;
            }
        }
        journal::note_journaled(journal::JournaledRow {
            first: local.start,
            last: end - 1,
            class,
        });
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
                class,
                rid,
                op,
                deps,
                observed: journal::PendingObserved::take().unwrap_or_default(),
                lock_tag: journal::PendingLockTag::current().unwrap_or_default(),
            },
        )?;
        put_journal_origin(tx, self, gen, idx, local.start);
        if gen != 0 && local.epoch != 0 {
            // (Raised before the commit: an over-estimate only costs a seek.)
            self.holder_append_tip
                .fetch_max(end - 1, std::sync::atomic::Ordering::SeqCst);
        }
        if gen != 0 {
            spec::note_deleg_tx(self, local.start, gen, deleg_first);
        }
        Ok(())
    }

    /// Plan 30 §M11: execute `op` here as the delegate of generation
    /// `gen` (the caller checked ownership and the grant): journaled with
    /// the generation's next stream index and the requester's `deps`.
    /// Returns the records and the index.
    ///
    /// `tag`, `now`: the op's fencing token, checked at `now` on this
    /// node's clock ([`crate::mutate::execute_tagged`]).
    pub fn delegate_execute(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        gen: u64,
        deps: crate::session::Position,
        tag: &crate::locks::LockTag,
        now: constellation_types::clock::Now,
    ) -> Result<(Vec<LogRecord>, u64), MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        journal::before_delegate_write_hook::fire(self);
        let _d = journal::PendingDelegate::set(gen, None, deps);
        let records = crate::mutate::execute_tagged(self, op, rid, tag, now)?;
        let idx = self.delegate_idx(gen)?;
        Ok((records, idx))
    }

    /// [`Self::delegate_execute`] on the delegate's FUSE fast path:
    /// `still_admitted` is asked again once the op's transaction holds the
    /// store's writer lock ([`crate::mutate::execute_admitted`]).
    #[allow(clippy::too_many_arguments)]
    pub fn delegate_execute_admitted(
        &self,
        op: &MutateOp,
        rid: Option<Rid>,
        gen: u64,
        deps: crate::session::Position,
        tag: &crate::locks::LockTag,
        now: constellation_types::clock::Now,
        still_admitted: Box<dyn Fn() -> bool>,
    ) -> Result<(Vec<LogRecord>, u64), MetaError> {
        let _admission = journal::PendingAdmission::set(still_admitted);
        let _class = journal::ClassAtJournal::set();
        self.delegate_execute(op, rid, gen, deps, tag, now)
    }

    /// Plan 43 C1-2 (D4): re-tag the journal transaction that holds journal
    /// seq `seq` (its first, or any later one) with `class`, in one write
    /// transaction: a row whose own client was answered with the gate off
    /// (`Lone`) must strand as the answer said. A row that is gone (shipped
    /// rows are durable anyway) or already of that class is left alone.
    /// `to` names the rule to re-tag to: the row keeps the configuration
    /// version it carries (`to`'s version applies only to a `Final` row,
    /// which has none). Returns whether a row changed.
    pub fn reclass_journal_tx(&self, seq: u64, to: RowCommit) -> Result<bool, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        if reclass_fault::take() {
            return Err(MetaError::Io(std::io::Error::other(
                "injected re-tag fault (testing only)",
            )));
        }
        let mut tx = self.db.write_tx();
        let owner = match tx.range(&self.journal_tx, ..=tx_key(seq)).next_back() {
            Some(guard) => {
                let (k, v) = guard.into_inner()?;
                Some((decode_tx_key(&k)?, postcard::from_bytes::<JournalTx>(&v)?))
            }
            None => None,
        };
        let Some((first, mut row)) = owner.filter(|(_, row)| row.last >= seq) else {
            return Ok(false);
        };
        let class = match to {
            RowCommit::Singleton { config_version } => row.class.as_singleton(config_version),
            RowCommit::Group {
                config_version,
                on: GroupOn::Archive,
            } => row.class.as_archive(config_version),
            other => other,
        };
        if row.class == class {
            return Ok(false);
        }
        row.class = class;
        put_journal_tx(&mut tx, self, first, &row)?;
        tx.commit()?;
        Ok(true)
    }

    /// Plan 43 C1-3: whether an unshipped journal transaction was executed
    /// under lease epoch `epoch`.
    pub fn journal_has_epoch_rows(&self, epoch: u64) -> Result<bool, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        super::backup::injected_read_fault()?;
        let r = self.db.read_tx();
        Ok(read_journal_tx_heads(&r, self)?
            .iter()
            .any(|(_, h)| h.epoch == epoch))
    }

    /// Plan 43 C1-2: the class of the journal transaction that holds
    /// journal seq `seq` (`None`: no such unshipped transaction).
    pub fn journal_tx_class(&self, seq: u64) -> Result<Option<RowCommit>, MetaError> {
        let r = self.db.read_tx();
        match r.range(&self.journal_tx, ..=tx_key(seq)).next_back() {
            Some(guard) => {
                let (_, v) = guard.into_inner()?;
                let head: JournalTxHead = postcard::from_bytes(&v)?;
                Ok((head.last >= seq).then_some(head.class))
            }
            None => Ok(None),
        }
    }

    /// Plan 43 C1-2: the classes of the unshipped journal transactions that
    /// carry `rid`, oldest first (the simulation's claim check reads the
    /// answering row's class; a shipped row is gone from the journal).
    pub fn journal_classes_of_rid(&self, rid: Rid) -> Result<Vec<RowCommit>, MetaError> {
        Ok(self
            .journal_rows_of_rid(rid)?
            .into_iter()
            .map(|(_, c)| c)
            .collect())
    }

    /// Plan 43 C1-2: `(first journal seq, class)` of the unshipped
    /// transactions that carry `rid`, oldest first.
    pub fn journal_rows_of_rid(&self, rid: Rid) -> Result<Vec<(u64, RowCommit)>, MetaError> {
        let r = self.db.read_tx();
        let rows = read_journal_txs(&r, self)?;
        let by_rid: Vec<(u64, RowCommit)> = rows
            .iter()
            .filter(|(_, row)| row.rid == Some(rid))
            .map(|(first, row)| (*first, row.class))
            .collect();
        if !by_rid.is_empty() {
            return Ok(by_rid);
        }
        // A journaled refusal records no rid of its own in its row (only
        // its `Refused` record names it).
        let mut out = Vec::new();
        for (first, row) in &rows {
            let named = journal_records(&r, self, *first, row.last)?.iter().any(|(_, rec)| {
                matches!(rec, LogRecord::Refused { rid: x, .. } | LogRecord::Completed { rid: x } if *x == rid)
            });
            if named {
                out.push((*first, row.class));
            }
        }
        Ok(out)
    }

    /// The generations of the unshipped journal's rows that this node
    /// assigned stream indices in (its own rows as their delegate; a root's
    /// append of another delegate's row assigns none), read from the
    /// transaction heads alone.
    pub fn journal_own_gens(&self) -> Result<Vec<u64>, MetaError> {
        let r = self.db.read_tx();
        let mut gens: Vec<u64> = read_journal_txs(&r, self)?
            .into_iter()
            .map(|(_, row)| row.gen)
            .filter(|g| *g != 0)
            .collect();
        gens.sort_unstable();
        gens.dedup();
        let mut own = Vec::new();
        for gen in gens {
            if kv_get_tx(&r, &self.local, &deleg_idx_key(gen))?
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|i| i > 0)
            {
                own.push(gen);
            }
        }
        Ok(own)
    }

    /// Plan 46 M0.4: the shipper's guard. A journal row of origin `(gen, idx)`
    /// that this node assigned (`idx` at most what it assigned under `gen`),
    /// whose generation the table does not list and that has no void cut,
    /// would reach the log under a generation the log never granted (an
    /// unstranded row of a grant the takeover took out). Every claim with a
    /// takeover strands such rows; this counts a path that missed one
    /// ([`Meta::unjustified_ships`], `status.ack.unjustified_ships`; the
    /// simulation fails a run that moved it) and trips a debug assertion. It
    /// refuses nothing: a root's append of another delegate's rows (no index
    /// assigned here) and rows after the node's own recall (a void cut) are
    /// not caught, and a missed path must not block the journal head.
    pub(crate) fn check_shippable_origins(
        &self,
        batch: &[(u64, LogRecord)],
    ) -> Result<(), MetaError> {
        if batch.is_empty() {
            return Ok(());
        }
        let r = self.db.read_tx();
        // (One seek when no delegate row is journaled: the common case.)
        if r.is_empty(&self.journal_origin)? {
            return Ok(());
        }
        drop(r);
        let seqs: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
        let mut origins = self.journal_origins(&seqs)?;
        origins.retain(|(g, _)| *g != 0);
        origins.sort_unstable();
        origins.dedup();
        if origins.is_empty() {
            return Ok(());
        }
        let r = self.db.read_tx();
        let listed: std::collections::BTreeSet<u64> = crate::delegation::read_table_tx(&r, self)?
            .iter()
            .map(|d| d.gen)
            .collect();
        for (gen, idx) in origins {
            if listed.contains(&gen) {
                continue;
            }
            let assigned = kv_get_tx(&r, &self.local, &deleg_idx_key(gen))?
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            if assigned < idx || kv_get_tx(&r, &self.local, &deleg_void_cut_key(gen))?.is_some() {
                continue;
            }
            self.ship_unjustified
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                gen,
                idx,
                "about to ship a delegate row of a generation the table does not list and that                  has no void cut: the log never granted it (a takeover missed its strand)"
            );
            debug_assert!(
                false,
                "shipping own delegate row ({gen}, {idx}) of an unlisted generation without a void cut"
            );
        }
        Ok(())
    }

    /// Ships counted by [`Self::check_shippable_origins`] (must stay 0).
    pub fn unjustified_ships(&self) -> u64 {
        self.ship_unjustified
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The highest stream index this node assigned under `gen` (0: none).
    pub fn delegate_idx(&self, gen: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_idx_key(gen))?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0))
    }

    /// Plan 43 C2 (hysteresis): the highest stream index of `gen` this node,
    /// the delegate, journaled for its own node's client with a class that
    /// waits for the group (0: none). Its `Lone` rows follow only once the
    /// prefix through it has committed: a root crash drops an uncommitted
    /// gated row at the strand and replays the later `Lone` one, which then
    /// runs without the effect it was answered on top of.
    pub fn delegate_gated_idx(&self, gen: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_gated_key(gen))?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0))
    }

    /// Plan 43 C2 (hysteresis across generations): a gated row of this
    /// node's own clients is open — journaled, not known committed, still in
    /// the journal — in a generation of this node other than `gen`, so an
    /// own row of `gen` is not `Lone` now ([`gated_open_elsewhere_tx`]).
    pub fn delegate_gated_open_elsewhere(&self, gen: u64) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        gated_open_elsewhere_r(&r, self, gen).map(|(open, _)| open)
    }

    /// Testing only (the simulation's oracles): every unretired journal row
    /// of generation `gen` as `(idx, class, rid)`, in index order.
    #[cfg(any(test, feature = "test-util"))]
    pub fn delegate_rows_for_audit(
        &self,
        gen: u64,
    ) -> Result<Vec<(u64, RowCommit, Option<Rid>)>, MetaError> {
        let r = self.db.read_tx();
        let mut rows: Vec<(u64, RowCommit, Option<Rid>)> = read_journal_txs(&r, self)?
            .into_iter()
            .filter(|(_, row)| row.gen == gen)
            .map(|(_, row)| (row.idx, row.class, row.rid))
            .collect();
        rows.sort_by_key(|(idx, ..)| *idx);
        Ok(rows)
    }

    /// Testing only (the simulation's row-version oracle): every unretired
    /// journal transaction, in journal order.
    #[cfg(any(test, feature = "test-util"))]
    pub fn journal_rows_for_audit(&self) -> Result<Vec<AuditRow>, MetaError> {
        let r = self.db.read_tx();
        Ok(read_journal_txs(&r, self)?
            .into_iter()
            .map(|(first, row)| AuditRow {
                first,
                epoch: row.epoch,
                gen: row.gen,
                idx: row.idx,
                class: row.class,
            })
            .collect())
    }

    /// Whether a takeover's strand took rows of this node's stream `gen`
    /// out of it (`Meta::strand_for_takeover`): the stream has a hole the
    /// generation cannot continue past.
    pub fn delegate_stranded(&self, gen: u64) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_stranded_key(gen))?.is_some())
    }

    /// Plan 43 C2e follow-up: this node, the delegate of `gen`, holds a grant
    /// of it on boot `boot`, synced before it executes under it. A process
    /// that restarts on the same boot finds its journal as the last one left
    /// it (commits reach the OS on commit, `Meta::sync`); one that restarts
    /// after a reboot may have lost its unsynced end, rows it acknowledged,
    /// and does not take the generation up again
    /// ([`Meta::deleg_held_boot`]).
    pub fn deleg_note_held(&self, gen: u64, boot: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        kv_set_tx(
            &mut tx,
            &self.local,
            &deleg_held_key(gen),
            &boot.to_string(),
        );
        tx.commit()?;
        self.sync()
    }

    /// The boot this node last held a grant of `gen` on (`None`: never).
    pub fn deleg_held_boot(&self, gen: u64) -> Result<Option<u64>, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_held_key(gen))?.and_then(|v| v.parse::<u64>().ok()))
    }

    /// Phase 2b: the highest index of `gen` in the log as this replica
    /// holds it (see [`deleg_log_idx_key`]).
    pub fn log_stream_idx(&self, gen: u64) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_log_idx_key(gen))?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0))
    }

    /// Plan 43 C2: the cut the log's `RecallCut` record of generation `gen`
    /// carried, as this replica applied it from the log: `None` for a
    /// generation not ended by one (live, or ended by an older `Recall`).
    pub fn void_cut(&self, gen: u64) -> Result<Option<u64>, MetaError> {
        let r = self.db.read_tx();
        Ok(kv_get_tx(&r, &self.local, &deleg_void_cut_key(gen))?
            .and_then(|v| v.parse::<u64>().ok()))
    }

    /// Every generation this replica keeps a log index
    /// ([`deleg_log_idx_key`]) or a void cut ([`deleg_void_cut_key`]) of:
    /// what the restart seeds the session from ([`Meta::open`]). Numbers
    /// are sparse (ADR-43), so the keys are scanned, not a range walked.
    pub(crate) fn persisted_generations(
        &self,
    ) -> Result<std::collections::BTreeSet<u64>, MetaError> {
        let r = self.db.read_tx();
        let mut out = std::collections::BTreeSet::new();
        for prefix in [DELEG_LOG_IDX_PREFIX, DELEG_VOID_CUT_PREFIX] {
            for guard in r.prefix(&self.local, prefix.as_bytes()) {
                let (k, _) = guard.into_inner()?;
                if let Some(gen) = std::str::from_utf8(&k)
                    .ok()
                    .and_then(|k| k.strip_prefix(prefix))
                    .and_then(|g| g.parse::<u64>().ok())
                {
                    out.insert(gen);
                }
            }
        }
        Ok(out)
    }

    /// L1: the `(epoch, idx)` of generation `gen`'s newest own row whose root
    /// append the pre-S3 stream delivered here ([`deleg_streamed_key`]);
    /// `None` when there is none, or after the log's `Recall`/`RecallCut`
    /// of the generation (never after a streamed one).
    pub fn deleg_streamed(&self, gen: u64) -> Result<Option<(u64, u64)>, MetaError> {
        let r = self.db.read_tx();
        deleg_streamed_tx(&r, &self.local, gen)
    }

    /// Path B: the generations not in `live` whose transactions this
    /// journal still holds (a delegation origin, unshipped) and whose end
    /// this replica has not applied from the log ([`Meta::void_cut`]: no
    /// cut persisted). A delegate can learn such an end ahead of the log
    /// (the root's stream installs its `RecallCut` speculatively); its rows
    /// past the cut leave the journal only when the record's segment is
    /// applied (the strand), and until then they are in this replica's
    /// state.
    pub fn deleg_gens_awaiting_end(
        &self,
        live: &std::collections::BTreeSet<u64>,
    ) -> Result<Vec<u64>, MetaError> {
        let r = self.db.read_tx();
        deleg_gens_awaiting_end_tx(&r, self, live)
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
    ///
    /// Plan 46 M1.2 step 4 (C3-1): the transaction is journaled verbatim,
    /// the records as streamed and in the order streamed (an `Atime` row,
    /// an atime-only transaction, a `Refused`, an `InboxAck` and each
    /// `Completed` included), under the delegate's `(gen, idx)`, with its
    /// rid and no op. The apply is tolerant: a record that does not apply
    /// is journaled all the same and counted
    /// ([`Meta::take_deleg_append_skipped`]).
    ///
    /// Plan 45 §12 O1: a rid this root holds a live shadow of (it
    /// forwarded the op to the delegate and installed the reply) is
    /// neither skipped nor applied over the shadow: the shadow is taken
    /// out by rid and the transaction journaled in its place, with what
    /// the root installed after it redone on top
    /// (`spec::unwind_shadow_for_append_tx`). A shadow writes no
    /// `completed` row, so the dedup above never saw it, and the records
    /// re-applied over later speculation of the same names: a second
    /// exchange of the pair swapped back, a `create a` after `rename a b`
    /// moved over `b`. A refusal of such a rid strands the shadow
    /// (M2, `Meta::journal_refusal_over_shadow`).
    pub fn apply_delegate_tx(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        gen: u64,
        idx: u64,
        deps: crate::session::Position,
    ) -> Result<bool, MetaError> {
        // Plan 46 M1.1 step 4: the root's append of a delegate's row is
        // stamped with the configuration of the generation in force where
        // it is journaled: the `DelegGroup` version this replica has applied
        // (the same `Singleton` tag as before: the replay and member-copy
        // rules read it, M2.1), not a version-0 placeholder.
        let _class = journal::PendingClass::current()
            .is_none()
            .then(|| journal::PendingClass::set_stream_version(RowCommit::LONE));
        // A delegate's refusal (see `delegate_refusal`) is a `Refused`
        // row of its own: journaled here the same way, under the
        // stream's origin.
        if let [LogRecord::Refused { rid: r, code }] = records {
            if self.completed_position(*r)?.is_some() {
                return Ok(false);
            }
            // Plan 45 §12 M2: a live shadow of the refused rid (the root
            // forwarded the op and installed the delegate's acceptance,
            // which `repair drop-held` on the delegate then let go) is
            // stranded by rid in the same transaction.
            let _d = journal::PendingDelegate::set(gen, Some(idx), deps);
            self.journal_refusal_over_shadow(*r, *code)?;
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
        self.apply_records_journaled_inner(records, rid, Journaled::Appended)?;
        self.note_log_idx(gen, idx)?;
        Ok(true)
    }

    /// Plan 30 §M9 × §M11: a takeover re-journals one of its
    /// predecessor's backed-up transactions ([`Self::apply_adopted_records`])
    /// under the delegation origin `(gen, idx)` the predecessor journaled
    /// it with (`gen` 0: the predecessor's own row). The successor's
    /// segment then names the origin, as the predecessor's would have:
    /// the delegate retires its row by it and every replica's
    /// per-generation index (this node's `log_stream_idx`, the inherited
    /// cursor, included) advances.
    pub fn apply_adopted_records_from(
        &self,
        records: &[LogRecord],
        rid: Option<Rid>,
        (gen, idx): (u64, u64),
    ) -> Result<(), MetaError> {
        if gen == 0 {
            return self.apply_adopted_records(records, rid);
        }
        {
            let _d = journal::PendingDelegate::set(gen, Some(idx), crate::session::Position::ZERO);
            self.apply_adopted_records(records, rid)?;
        }
        self.note_log_idx(gen, idx)
    }

    /// Plan 30 §M11: a delegate's refusal of `rid` (plan 30 §M9's
    /// journaled `Refused` row), as the next transaction of stream
    /// `gen` — the root appends it like any other, so a retry by rid
    /// anywhere finds the same code. Returns the stream index.
    pub fn delegate_refusal(
        &self,
        rid: Rid,
        code: constellation_types::Code,
        gen: u64,
        deps: crate::session::Position,
    ) -> Result<u64, MetaError> {
        let _d = journal::PendingDelegate::set(gen, None, deps);
        self.journal_refusal(rid, code, None)?;
        self.delegate_idx(gen)
    }

    /// Plan 30 §M11: this node's unretired transactions of delegation
    /// stream `gen` from index `from_idx` on, oldest first, at most
    /// `max_rows` journal rows (whole transactions).
    ///
    /// The batch stops before the first transaction whose manifest names
    /// a chunk still pending here (`store::remote`: a non-owner forwarded
    /// it under `--write-mode back`, or this node's own write-back): the
    /// root appends what it is sent without re-validating, and so does
    /// the root with a sealed backup's tail, so neither the stream nor
    /// the backup may carry a manifest before its chunks are in S3. The
    /// stream's order is kept: what follows waits too.
    pub fn delegate_txs_from(
        &self,
        gen: u64,
        from_idx: u64,
        max_rows: usize,
    ) -> Result<Vec<DelegateTx>, MetaError> {
        let r = self.db.read_tx();
        let mut pending = crate::store::remote::PendingView::new(&r, self)?;
        let mut out = Vec::new();
        let mut rows = 0usize;
        // In index order from `from_idx`, read by origin
        // (`journal_origin`): the cost is the batch's, not the whole
        // outstanding stream's (a delegate far ahead of the log read
        // every one of its rows, ops and all, for every batch).
        let txs: Box<dyn Iterator<Item = Result<(u64, u64), MetaError>>> = if gen == 0 {
            Box::new(
                self.journal_txs_of_gen_scan(&r, 0, from_idx)?
                    .into_iter()
                    .map(Ok),
            )
        } else {
            Box::new(journal_origins_from(&r, self, gen, from_idx)?)
        };
        for found in txs {
            let (_, first) = found?;
            let Some(row) = get_journal_tx(&r, self, first)? else {
                continue;
            };
            if row.gen != gen || row.idx < from_idx {
                continue;
            }
            self.note_local_rows_read(1);
            let records: Vec<LogRecord> = journal_records(&r, self, first, row.last)?
                .into_iter()
                .map(|(_, rec)| rec)
                .collect();
            if pending.names_pending(&records)? {
                break;
            }
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
        Ok(out)
    }

    /// `(idx, first)` of every transaction of generation `gen` at index
    /// `from_idx` or above, by a walk of the whole journal, in index order
    /// (journal order within one index): what [`Self::delegate_txs_from`]
    /// reads for `gen` 0, which `journal_origin` does not index.
    fn journal_txs_of_gen_scan(
        &self,
        r: &impl Readable,
        gen: u64,
        from_idx: u64,
    ) -> Result<Vec<(u64, u64)>, MetaError> {
        let mut txs: Vec<(u64, u64)> = read_journal_tx_heads(r, self)?
            .into_iter()
            .filter(|(_, h)| h.gen == gen && h.idx >= from_idx)
            .map(|(first, h)| (h.idx, first))
            .collect();
        txs.sort_by_key(|(idx, _)| *idx);
        Ok(txs)
    }

    /// Plan 30 §M11: the delegation origin of each journal row in
    /// `seqs` (`(0, 0)` for this node's own rows), for the segment
    /// envelope.
    ///
    /// A transaction's journal seqs are one run, so a seq's is the one
    /// starting at or below it (past the acked watermark) if it reaches
    /// that far. Ascending `seqs` over a dense run (a ship: whole
    /// transactions from the journal's head) are matched in one walk of
    /// the heads between their ends; others take a seek per transaction
    /// they fall in. Never a walk of every outstanding head per seq.
    pub fn journal_origins(&self, seqs: &[u64]) -> Result<Vec<(u64, u64)>, MetaError> {
        let r = self.db.read_tx();
        let from = journal::acked_watermark(&r, &self.local)?.saturating_add(1);
        let origin = |found: Option<(u64, JournalTxHead)>, seq: u64| {
            found
                .filter(|(first, h)| *first <= seq && seq <= h.last)
                .map(|(_, h)| (h.gen, h.idx))
                .unwrap_or((0, 0))
        };
        let decode = |guard: fjall::Guard| -> Result<(u64, JournalTxHead), MetaError> {
            let (k, v) = guard.into_inner()?;
            self.note_local_rows_read(1);
            Ok((decode_tx_key(&k)?, postcard::from_bytes(&v)?))
        };
        // The transaction starting at or below `seq`, past the watermark.
        let floor = |seq: u64| -> Result<Option<(u64, JournalTxHead)>, MetaError> {
            if seq < from {
                return Ok(None);
            }
            r.range(&self.journal_tx, tx_key(from)..=tx_key(seq))
                .next_back()
                .map(decode)
                .transpose()
        };
        let mut out = Vec::with_capacity(seqs.len());
        let (Some(&lo), Some(&hi)) = (seqs.first(), seqs.last()) else {
            return Ok(out);
        };
        let ascending = seqs.windows(2).all(|w| w[0] <= w[1]);
        if ascending && hi - lo < 2 * seqs.len() as u64 {
            // `at`: the transaction starting at or below the current seq;
            // `next`: the first one after it.
            let mut at = floor(lo)?;
            let start = at.map_or(from.max(lo), |(first, _)| first.saturating_add(1));
            // Nothing after `at` when it starts at `hi` (an empty range,
            // not an inverted one).
            let mut heads = (start <= hi)
                .then(|| r.range(&self.journal_tx, tx_key(start)..=tx_key(hi)))
                .into_iter()
                .flatten();
            let mut next = heads.next().map(decode).transpose()?;
            for &seq in seqs {
                while let Some(n) = next.filter(|(first, _)| *first <= seq) {
                    at = Some(n);
                    next = heads.next().map(decode).transpose()?;
                }
                out.push(origin(at, seq));
            }
            return Ok(out);
        }
        let mut at: Option<(u64, JournalTxHead)> = None;
        for &seq in seqs {
            if !at.is_some_and(|(first, h)| first <= seq && seq <= h.last) {
                at = floor(seq)?;
            }
            out.push(origin(at, seq));
        }
        Ok(out)
    }

    /// The `deps` of this delegate's transactions of `gen` still in the
    /// journal from index `from` on, in index order: where a dependency
    /// lost with another generation first appears (`Core::deleg_find_lost`).
    /// Each row's `journal_tx` entry is decoded (its op with it, which
    /// precedes the `deps`), not its records. The caller reads only the
    /// rows no batch has read yet.
    pub fn delegate_deps_from(
        &self,
        gen: u64,
        from: u64,
    ) -> Result<Vec<(u64, crate::session::Position)>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for found in journal_origins_from(&r, self, gen, from)? {
            let (idx, first) = found?;
            if let Some(row) = get_journal_tx(&r, self, first)? {
                if row.gen == gen {
                    out.push((idx, row.deps));
                }
            }
        }
        Ok(out)
    }

    /// Plan 43 C3: [`Meta::holder_append_tip`]'s value: an upper bound of
    /// the journal seqs of this node's delegation rows journaled as a lease
    /// holder (0: none). One atomic load.
    pub fn holder_append_tip(&self) -> u64 {
        self.holder_append_tip
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Plan 43 C3: this node's unshipped transactions of delegation origin
    /// `gen` journaled as a lease holder, as `(idx, last journal seq)` in
    /// index order: one range read over `journal_origin`, a head per row.
    pub fn holder_appends(&self, gen: u64) -> Result<Vec<(u64, u64)>, MetaError> {
        self.holder_appends_from(gen, 0)
    }

    /// [`Self::holder_appends`] from stream index `from` on: one seek past
    /// the rows below it (the generation's shipped history, whose deleted
    /// `journal_origin` keys a scan from 0 walks).
    pub fn holder_appends_from(&self, gen: u64, from: u64) -> Result<Vec<(u64, u64)>, MetaError> {
        if gen == 0 {
            return Ok(Vec::new());
        }
        self.holder_scans
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for found in journal_origins_from(&r, self, gen, from)? {
            let (idx, first) = found?;
            if let Some(head) = get_journal_tx_head(&r, self, first)? {
                if head.epoch != 0 {
                    out.push((idx, head.last));
                }
            }
        }
        Ok(out)
    }

    /// At open: the tip from the unshipped journal's heads.
    pub(crate) fn refresh_holder_append_tip(&self) -> Result<(), MetaError> {
        let r = self.db.read_tx();
        let tip = read_journal_tx_heads(&r, self)?
            .into_iter()
            .filter(|(_, h)| h.gen != 0 && h.epoch != 0)
            .map(|(_, h)| h.last)
            .max()
            .unwrap_or(0);
        self.holder_append_tip
            .fetch_max(tip, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Plan 43 C3: the last journal seq of this node's unshipped
    /// transaction of delegation origin `(gen, idx)` that it journaled as a
    /// lease holder (`epoch` set: the root's append of the row, a takeover
    /// gate's re-applied tail or re-append of its own rows), if any. Such a
    /// row raised [`Meta::log_stream_idx`] here without the log or the
    /// delegation's group holding it: what commits it is this node's root
    /// tenure. One seek (`journal_origin`).
    pub fn holder_append_of(&self, gen: u64, idx: u64) -> Result<Option<u64>, MetaError> {
        if gen == 0 {
            return Ok(None);
        }
        let r = self.db.read_tx();
        for found in journal_origins_from(&r, self, gen, idx)? {
            let (i, first) = found?;
            if i != idx {
                break;
            }
            if let Some(head) = get_journal_tx_head(&r, self, first)? {
                if head.epoch != 0 {
                    return Ok(Some(head.last));
                }
            }
        }
        Ok(None)
    }

    /// Plan 30 §M11 phase 2b: whether transaction `(gen, idx)` of this
    /// delegate's stream is still in the journal (not yet carried by a
    /// segment the replica applied). One seek (`journal_origin`) for a
    /// delegation origin.
    pub fn delegate_tx_pending(&self, gen: u64, idx: u64) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        if gen == 0 {
            return Ok(read_journal_tx_heads(&r, self)?
                .iter()
                .any(|(_, h)| h.gen == gen && h.idx == idx));
        }
        let first = journal_origins_from(&r, self, gen, idx)?.next();
        match first {
            Some(found) => Ok(found?.0 == idx),
            None => Ok(false),
        }
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
    ///
    /// An origin names one transaction: a delegate numbers its stream
    /// from a persisted counter, and a root appends index `cursor + 1`
    /// only. The scan ends once every origin this journal may hold
    /// ([`spec::deleg_origins_held`]) is found: a segment brings a
    /// delegate's rows back oldest first, so they are at the head of its
    /// journal, and the cost is theirs, not the whole stream's.
    pub(crate) fn journal_seqs_of_origins(
        &self,
        tx: &SingleWriterWriteTx,
        origins: &[(u64, u64)],
    ) -> Result<Vec<(u64, u64, Option<u64>)>, MetaError> {
        if origins.iter().all(|(g, _)| *g == 0) {
            return Ok(Vec::new());
        }
        let mut wanted = spec::deleg_origins_held(tx, self, origins)?;
        let mut out = Vec::new();
        if wanted.is_empty() {
            return Ok(out);
        }
        let from = journal::acked_watermark(tx, &self.local)?.saturating_add(1);
        for guard in tx.range(&self.journal_tx, tx_key(from)..) {
            let (k, v) = guard.into_inner()?;
            let h: JournalTxHead = postcard::from_bytes(&v)?;
            self.note_local_rows_read(1);
            if h.gen != 0 && wanted.remove(&(h.gen, h.idx)) {
                out.push((decode_tx_key(&k)?, h.last, h.spec_seq));
                if wanted.is_empty() {
                    break;
                }
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

    /// How many unshipped transactions were journaled without
    /// before-images (a non-holder's write outside speculation): one
    /// counter read. With any, the ship plan holds everything after the
    /// first held or deferred transaction (`store::held`'s opaque rule)
    /// and the publisher defers. A continuation epoch's hold owner
    /// captures like any holder, so an epoch journal counts none.
    pub fn uncaptured_tx_count(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        counter_get(&r, &self.local, KV_UNCAPTURED_TX_COUNT)
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
        self.apply_records_journaled_inner(records, rid, Journaled::Plain)
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
        self.apply_records_journaled_inner(records, rid, Journaled::Adopted)
    }

    fn apply_records_journaled_inner(
        &self,
        records: &[LogRecord],
        rid: Option<crate::Rid>,
        how: Journaled,
    ) -> Result<(), MetaError> {
        let adopted = how == Journaled::Adopted;
        // With a rid, the transaction is that op's (plan 30 §M9): a
        // stranding replays it *by rid* — where `completed` dedups —
        // never as anonymous records under a derived rid, which would
        // re-apply a stale effect out of order. A delegate's row the root
        // appends (plan 46 M1.2 step 4) is journaled as streamed, with its
        // rid and no op: it is not one this node can execute again.
        let verbatim = how == Journaled::Appended;
        let _op = rid.map(|rid| {
            if verbatim {
                journal::PendingLocalOp::set_rid_only(Some(rid))
            } else {
                journal::PendingLocalOp::set(
                    Some(rid),
                    &crate::mutate::MutateOp::Records {
                        records: records.to_vec(),
                    },
                )
            }
        });
        // Plan 43 C1-2: a predecessor's tail was committed by its group
        // (the sealer held it, write-all), whatever this tenure waits for.
        // (A control row the core journals through the same path — a grant,
        // a recall — carries the class the core set.)
        let _class = (adopted && journal::PendingClass::current().is_none())
            .then(|| journal::PendingClass::set(RowCommit::ROOT_GROUP));
        // Plan 30 §M12: a grant or a recall changes who may execute
        // what; the root's fast path checks that under the shared side
        // (`Meta::deleg_gate`). Taken before the write transaction, in
        // the same order as the fast path (gate, then the database).
        let _gate = records
            .iter()
            .any(|r| {
                matches!(
                    r,
                    LogRecord::Delegate { .. }
                        | LogRecord::Recall { .. }
                        | LogRecord::RecallCut { .. }
                )
            })
            .then(|| self.deleg_gate.write().unwrap_or_else(|e| e.into_inner()));
        let mut tx = self.db.write_tx();
        // Plan 45 §12 O1: the root's own live shadow of the rid it
        // appends is that transaction's effect already; it is taken out
        // by rid and the append goes in its place (see
        // `spec::unwind_shadow_for_append_tx`). Moved rows' usage is
        // staged apart from this transaction's, which its `Local` row
        // records.
        let moved = UsageTracker::staging();
        let unwind = match (how, rid) {
            (Journaled::Appended, Some(rid)) => {
                spec::unwind_shadow_for_append_tx(&mut tx, self, &moved, rid, records)?
            }
            _ => None,
        };
        let local = self.begin_local(&tx)?;
        let staged = UsageTracker::staging();
        let (skipped, applied) = {
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
        if verbatim {
            // The streamed records exactly, in order: an `Atime` row, an
            // atime-only transaction, a `Refused` and an `InboxAck` row
            // included, each `Completed` where the delegate journaled it.
            // The apply above is tolerant (every replica applies the same
            // records by the same rules), and a record it skipped is
            // counted: the sim fails a seed that has one.
            if skipped > 0 {
                self.deleg_append_skipped
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
            }
            debug_assert!(
                journal::PendingCompletion::take().is_none(),
                "a verbatim append carries its own completions"
            );
            for rec in &applied {
                match rec {
                    LogRecord::Completed { rid } => {
                        journal::append_completion_tx(
                            &mut tx,
                            &self.journal_ks,
                            &self.local,
                            &self.completed,
                            *rid,
                        )?;
                    }
                    LogRecord::Refused { rid, code } => {
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
                            Meta::encode_refused_row(seq, now_ms, *code),
                        );
                    }
                    _ => {
                        journal::append_tx(
                            &mut tx,
                            &self.journal_ks,
                            &self.local,
                            &self.completed,
                            rec,
                        )?;
                    }
                }
            }
        }
        let rows: Vec<&LogRecord> = applied
            .iter()
            .filter(|rec| {
                !verbatim
                    && !matches!(
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
        if n == 0 && !verbatim {
            // The rid is this transaction's own, or the one `execute`
            // armed for the `MutateOp::Records` op it runs (which no row
            // took above): a replay by rid whose records all lost to a
            // newer state, or a folded truncate's no-op replay
            // (`authority::core::replay`'s `as_folded`), completes its rid all the same, and
            // every other copy of the op queued by that rid dedups to it.
            if let Some(rid) = rid.or_else(journal::PendingCompletion::take) {
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
                    LogRecord::Refused { rid, code } => {
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
                            Meta::encode_refused_row(seq, now_ms, *code),
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
        if let Some(unwind) = unwind {
            spec::redo_after_append_tx(&mut tx, self, &moved, unwind)?;
        }
        let (bytes, files) = moved.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        moved.drain_into(self.usage_tracker());
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
    /// outstanding).
    AsIs,
    /// `ns` with this overlay is the log-prefix state: the holder's
    /// unshipped journal, substituted by its before-images.
    Substituted(LogPrefixView),
    /// No commit can reflect this replica right now: requester speculation
    /// (a shadow or hint) is outstanding, or this node's unshipped journal
    /// holds a transaction journaled without before-images (a non-holder's
    /// write outside speculation).
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
            // Unshipped work with no before-images: no commit can be the
            // log prefix. (This used to publish `ns` as it stands when
            // `holder_epoch` was 0, taking such writes for a non-holder's
            // pre-M3b leftovers; flex-crash seed 11719 published a
            // continuation epoch's journal, then uncaptured, as the prefix
            // at the last shipped seq. A hold owner captures now, under the
            // epoch it will ship: what is left is a write the engine's
            // lease view admitted while the core had just cleared the
            // holder epoch, at a release or a deposition.)
            if self.publish_unshipped.load(Ordering::Relaxed) && self.holder_epoch() == 0 {
                return Ok(PublishBasis::AsIs);
            }
            return Ok(PublishBasis::Defer);
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

    #[test]
    fn row_commit_bits_round_trip() {
        use crate::store::local::{GroupOn, RowCommit};
        let max = (1u64 << 60) - 1;
        for v in [0, 1, 7, max] {
            for rc in [
                RowCommit::Singleton { config_version: v },
                RowCommit::group(v, GroupOn::Archive),
                RowCommit::group(v, GroupOn::Root),
                RowCommit::group(v, GroupOn::Delegate),
            ] {
                assert_eq!(RowCommit::from_bits(rc.to_bits()), rc);
            }
        }
        assert_eq!(
            RowCommit::from_bits(RowCommit::Final.to_bits()),
            RowCommit::Final
        );
    }

    #[test]
    #[should_panic(expected = "unknown row commit tag")]
    fn row_commit_unknown_tag_panics() {
        crate::store::local::RowCommit::from_bits(9 << 60);
    }

    #[test]
    #[should_panic(expected = "exceeds 60 bits")]
    fn row_commit_version_too_big_panics() {
        crate::store::local::RowCommit::Singleton {
            config_version: 1 << 60,
        }
        .to_bits();
    }

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

    /// Plan 43 C1-2: a journal row records the class it was journaled
    /// under — the class the core sets around its call, else the one the
    /// session publishes with the gate — and the head decodes it.
    #[test]
    fn a_journal_row_records_its_class_and_the_head_reads_it() {
        use crate::session::SessionState;
        use crate::store::journal::PendingClass;
        use crate::store::local::{read_journal_tx_heads, RowCommit};
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        // Nothing published, nothing set: a lone row.
        create(&meta, 1);
        // The session publishes the gate with a class: the fast path's rows.
        let session: &SessionState = meta.session();
        session.set_durable(true, 0, false, RowCommit::ROOT_GROUP, false);
        create(&meta, 2);
        // The core's own class wins, scoped to its call.
        {
            let _c = PendingClass::set(RowCommit::ARCHIVE);
            create(&meta, 3);
            {
                let _inner = PendingClass::set(RowCommit::FINAL);
                create(&meta, 4);
            }
            create(&meta, 5);
        }
        create(&meta, 6);
        let r = meta.db.read_tx();
        let heads = read_journal_tx_heads(&r, &meta).unwrap();
        let classes: Vec<RowCommit> = heads.iter().map(|(_, h)| h.class).collect();
        // (Each create journals one transaction; read the rids' classes.)
        assert_eq!(
            classes,
            vec![
                RowCommit::LONE,
                RowCommit::ROOT_GROUP,
                RowCommit::ARCHIVE,
                RowCommit::FINAL,
                RowCommit::ARCHIVE,
                RowCommit::ROOT_GROUP,
            ]
        );
        // The classes that wait for something are the gated ones (Q12).
        assert!(RowCommit::ARCHIVE.gated() && RowCommit::ROOT_GROUP.gated());
        assert!(RowCommit::DELEG_GROUP.gated());
        assert!(!RowCommit::LONE.gated() && !RowCommit::FINAL.gated());
        // Only the fast path's threads fall back to the published class
        // (counted: the core's own thread must never).
        assert_eq!(crate::rows_from_session_on_this_thread(), 3);
        // The full row reads the same and the lookup by rid finds it.
        let rid = Rid {
            node: 3,
            incarnation: 1,
            seq: 3,
        };
        assert_eq!(
            meta.journal_classes_of_rid(rid).unwrap(),
            vec![RowCommit::ARCHIVE]
        );
    }

    /// Plan 43 C1-2 (D4): re-tagging a row rewrites its class and nothing
    /// else, finds the row by any journal seq it holds, and leaves a
    /// shipped (gone) row and a row of that class alone.
    #[test]
    fn reclass_rewrites_only_the_class() {
        use crate::store::journal::PendingClass;
        use crate::store::local::{get_journal_tx, read_journal_txs, RowCommit};
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        {
            let _c = PendingClass::set(RowCommit::ROOT_GROUP);
            create(&meta, 1);
            create(&meta, 2);
        }
        let before = {
            let r = meta.db.read_tx();
            read_journal_txs(&r, &meta).unwrap()
        };
        assert_eq!(before.len(), 2);
        let (first2, row2) = (before[1].0, before[1].1.clone());
        // Any seq the row holds finds it (its last, here).
        assert!(meta.reclass_journal_tx(row2.last, RowCommit::LONE).unwrap());
        let r = meta.db.read_tx();
        let after = read_journal_txs(&r, &meta).unwrap();
        assert_eq!(after[0].1.class, RowCommit::ROOT_GROUP, "the other row");
        let mut expect = row2.clone();
        expect.class = RowCommit::LONE;
        assert_eq!(
            format!("{:?}", get_journal_tx(&r, &meta, first2).unwrap().unwrap()),
            format!("{expect:?}"),
            "only the class changed"
        );
        drop(r);
        // Again: nothing changes.
        assert!(!meta.reclass_journal_tx(first2, RowCommit::LONE).unwrap());
        // A seq past every row finds none.
        assert!(!meta
            .reclass_journal_tx(row2.last + 100, RowCommit::LONE)
            .unwrap());
        // A shipped row is gone: a no-op.
        ship_all(&meta, 1);
        assert!(!meta.reclass_journal_tx(first2, RowCommit::ARCHIVE).unwrap());
    }

    /// Plan 46 M1.1 step 3: a row carries the version of the configuration
    /// it was journaled under, across a change of configuration: the row
    /// journaled before the change keeps its version, a row journaled after
    /// carries the new one, a re-tag changes the rule but not the version
    /// (a `Final` row has none and takes the re-tag's), and the version
    /// survives the session's one-word publication.
    #[test]
    fn a_rows_version_survives_a_change_of_configuration_and_a_retag() {
        use crate::store::journal::PendingClass;
        use crate::store::local::{read_journal_txs, GroupOn, RowCommit};
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        for (n, rule) in [
            (1, RowCommit::group(7, GroupOn::Root)),
            (2, RowCommit::group(8, GroupOn::Root)),
            (3, RowCommit::Singleton { config_version: 9 }),
            (4, RowCommit::Final),
        ] {
            let _c = PendingClass::set(rule);
            create(&meta, n);
        }
        let rows = |meta: &Meta| {
            let r = meta.db.read_tx();
            read_journal_txs(&r, meta)
                .unwrap()
                .into_iter()
                .map(|(first, row)| (first, row.class))
                .collect::<Vec<_>>()
        };
        let before = rows(&meta);
        assert_eq!(
            before.iter().map(|r| r.1).collect::<Vec<_>>(),
            vec![
                RowCommit::group(7, GroupOn::Root),
                RowCommit::group(8, GroupOn::Root),
                RowCommit::Singleton { config_version: 9 },
                RowCommit::Final,
            ]
        );
        // Re-tagged singleton / archive: the rule changes, the version stays.
        assert!(meta
            .reclass_journal_tx(before[0].0, RowCommit::LONE)
            .unwrap());
        assert!(meta
            .reclass_journal_tx(before[1].0, RowCommit::ARCHIVE)
            .unwrap());
        // A `Final` row has no version: the re-tag's applies.
        let hold = RowCommit::Singleton { config_version: 11 };
        assert!(meta.reclass_journal_tx(before[3].0, hold).unwrap());
        let after: Vec<_> = rows(&meta).into_iter().map(|r| r.1).collect();
        assert_eq!(
            after,
            vec![
                RowCommit::Singleton { config_version: 7 },
                RowCommit::group(8, GroupOn::Archive),
                RowCommit::Singleton { config_version: 9 },
                RowCommit::Singleton { config_version: 11 },
            ]
        );
        for rule in after.iter().chain([&RowCommit::Final]) {
            assert_eq!(RowCommit::from_bits(rule.to_bits()), *rule);
        }
        meta.session().set_durable(true, 3, false, after[1], false);
        assert_eq!(meta.session().published_class(), after[1]);
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
        meta.set_manifest_dirty(ino, None, b"not a manifest", 4, None, None, &[lost])
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
    use constellation_types::clock::Now;

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

    /// Plan 46 M1.1 step 4 (review, I2; fail-before: the store stamps the
    /// core's `Singleton` row as decided): the core decides an own row's rule
    /// from the journal as it stood, then waits for the writer lock, and the
    /// fast path's rows go in meanwhile. A `Singleton` row of the node's own
    /// clients that would follow an open gated row of its own (above the
    /// committed prefix the core published, row still in the journal) is
    /// stamped `Group` at the same version instead (the core reads that and
    /// waits for the group): it is replayed without that row at a root's
    /// crash otherwise, after its client was answered. A gated row of the
    /// node's own clients is the generation's gated mark; a forward's row sets
    /// none and follows nothing; a committed gated row holds nothing back.
    #[test]
    fn an_own_singleton_row_behind_an_open_gated_row_is_stamped_as_a_group_row() {
        use crate::session::DelegDurable;
        use crate::store::journal::{clear_last_journaled, take_last_journaled, PendingClass};
        let meta = Meta::open_in_memory().unwrap();
        let mut n = 0;
        let mut journal = |class: RowCommit, own: bool| -> (u64, RowCommit) {
            n += 1;
            let _class = if own {
                PendingClass::set_for_own_client(class)
            } else {
                PendingClass::set(class)
            };
            clear_last_journaled();
            let (_, idx) = meta
                .delegate_execute(
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
                    5,
                    Default::default(),
                    &crate::locks::LockTag::NONE,
                    Now::same(0),
                )
                .unwrap();
            (idx, take_last_journaled().unwrap().class)
        };
        let group = RowCommit::group(0, GroupOn::Delegate);
        assert_eq!(journal(RowCommit::LONE, true), (1, RowCommit::LONE));
        assert_eq!(meta.delegate_gated_idx(5).unwrap(), 0);
        assert_eq!(journal(RowCommit::DELEG_GROUP, true), (2, group));
        assert_eq!(meta.delegate_gated_idx(5).unwrap(), 2);
        // Behind the open gated row 2 the core's `Singleton` row waits for
        // its group (and is itself a gated row).
        assert_eq!(journal(RowCommit::LONE, true), (3, group));
        assert_eq!(meta.delegate_gated_idx(5).unwrap(), 3);
        // A forward's row sets no mark and is stamped as given.
        assert_eq!(journal(RowCommit::LONE, false), (4, RowCommit::LONE));
        assert_eq!(journal(RowCommit::DELEG_GROUP, false), (5, group));
        assert_eq!(meta.delegate_gated_idx(5).unwrap(), 3);
        // The core publishes the prefix through 3 committed: nothing is open.
        meta.session().set_deleg_durable(
            5,
            Some(DelegDurable {
                commit: RowCommit::LONE,
                through: 3,
                horizon: u64::MAX,
                lost: false,
            }),
        );
        assert_eq!(journal(RowCommit::LONE, true), (6, RowCommit::LONE));
    }

    /// The same across this node's generations: an own `Singleton` row of
    /// generation 6 behind an open gated row of generation 5.
    #[test]
    fn an_own_singleton_row_behind_an_open_gated_row_of_another_generation_waits_too() {
        use crate::store::journal::{clear_last_journaled, take_last_journaled, PendingClass};
        let meta = Meta::open_in_memory().unwrap();
        let mut n = 0;
        let mut journal = |gen: u64, class: RowCommit| -> RowCommit {
            n += 1;
            let _class = PendingClass::set_for_own_client(class);
            clear_last_journaled();
            meta.delegate_execute(
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
                gen,
                Default::default(),
                &crate::locks::LockTag::NONE,
                Now::same(0),
            )
            .unwrap();
            take_last_journaled().unwrap().class
        };
        assert_eq!(journal(5, RowCommit::DELEG_GROUP), RowCommit::DELEG_GROUP);
        assert_eq!(
            journal(6, RowCommit::LONE),
            RowCommit::group(0, GroupOn::Delegate)
        );
    }

    /// Plan 46 M1.1 step 4 (fail-before: the row is stamped with the
    /// published version whatever the store records): the delegate fast path
    /// stamps the rule the core published, and only while the version it names
    /// is the one the store records under the writer lock. A `DelegGroup`
    /// record applied between the publication and the journal refuses the op
    /// unwritten (the core stamps it), and the row is journaled once the core
    /// publishes the new version.
    #[test]
    fn the_fast_path_refuses_a_row_when_the_configuration_changed_since_the_publication() {
        use crate::session::DelegDurable;
        use crate::store::journal::{clear_last_journaled, take_last_journaled};
        let meta = Meta::open_in_memory().unwrap();
        let publish = |version| {
            meta.session().set_deleg_durable(
                5,
                Some(DelegDurable {
                    commit: RowCommit::group(version, GroupOn::Delegate),
                    through: 0,
                    horizon: u64::MAX,
                    lost: false,
                }),
            );
        };
        let mut n = 0;
        let mut fast = || {
            n += 1;
            clear_last_journaled();
            meta.delegate_execute_admitted(
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
                5,
                Default::default(),
                &crate::locks::LockTag::NONE,
                Now::same(0),
                Box::new(|| true),
            )
        };
        publish(0);
        assert!(fast().is_ok());
        assert_eq!(
            take_last_journaled().unwrap().class,
            RowCommit::group(0, GroupOn::Delegate)
        );
        // The store applies a `DelegGroup` record (version 1): the published
        // rule is of version 0, and the op is refused unwritten.
        meta.apply_records(&[LogRecord::DelegGroup {
            dir: ROOT_INO,
            gen: 5,
            version: 1,
            backups: vec![2],
        }])
        .unwrap();
        assert!(matches!(fast(), Err(MetaError::NotAdmitted)));
        assert_eq!(meta.delegate_idx(5).unwrap(), 1, "nothing was journaled");
        // The core publishes the version the store records: stamped with it.
        publish(1);
        assert!(fast().is_ok());
        assert_eq!(
            take_last_journaled().unwrap().class,
            RowCommit::group(1, GroupOn::Delegate)
        );
    }

    /// The simulation's row-version oracle (fail-before: `None` for a row
    /// journaled before the generation's first `DelegGroup` record, which the
    /// oracle then judged against the version applied by the end of the
    /// event, `long-delegated-backup` seed 1938 under slow lease reads): the
    /// version in force at a row's journal position is 0 ("no group") until
    /// the first record is applied, and `None` only for a generation with no
    /// recorded version.
    #[test]
    fn the_group_version_at_a_position_before_the_first_record_is_zero() {
        use crate::session::DelegDurable;
        use crate::store::journal::{clear_last_journaled, take_last_journaled};
        let meta = Meta::open_in_memory().unwrap();
        let mut n = 0;
        let mut fast = |version| {
            meta.session().set_deleg_durable(
                5,
                Some(DelegDurable {
                    commit: RowCommit::group(version, GroupOn::Delegate),
                    through: 0,
                    horizon: u64::MAX,
                    lost: false,
                }),
            );
            n += 1;
            clear_last_journaled();
            meta.delegate_execute_admitted(
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
                5,
                Default::default(),
                &crate::locks::LockTag::NONE,
                Now::same(0),
                Box::new(|| true),
            )
            .unwrap();
            take_last_journaled().unwrap().first
        };
        let before = fast(0);
        assert_eq!(meta.deleg_group_at(5, before), None, "nothing recorded");
        meta.apply_records(&[LogRecord::DelegGroup {
            dir: ROOT_INO,
            gen: 5,
            version: 1,
            backups: vec![2],
        }])
        .unwrap();
        let after = fast(1);
        assert_eq!(meta.deleg_group_at(5, before), Some(0));
        assert_eq!(meta.deleg_group_at(5, after), Some(1));
        assert_eq!(meta.deleg_group_at(6, after), None);
    }

    /// Plan 46 M1.1 step 4 (review; fail-before: the fast path stamps a
    /// `Singleton` row past the published lead): a lone delegate's own load is
    /// bounded by the lead (the committed prefix plus `delegation_stream_rows`,
    /// published as the horizon) under the writer lock, not by the fast
    /// path's rate times the core's step interval: past it the op is refused
    /// unwritten and goes through the core, which makes the row wait for the
    /// group. A group rule has no lead.
    #[test]
    fn the_fast_path_refuses_a_singleton_row_past_the_published_horizon() {
        use crate::session::DelegDurable;
        use crate::store::journal::{clear_last_journaled, take_last_journaled};
        let meta = Meta::open_in_memory().unwrap();
        let publish = |commit, through, horizon| {
            meta.session().set_deleg_durable(
                5,
                Some(DelegDurable {
                    commit,
                    through,
                    horizon,
                    lost: false,
                }),
            );
        };
        let mut n = 0;
        let mut fast = || {
            n += 1;
            clear_last_journaled();
            meta.delegate_execute_admitted(
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
                5,
                Default::default(),
                &crate::locks::LockTag::NONE,
                Now::same(0),
                Box::new(|| true),
            )
        };
        // Nothing committed, a lead of two rows: rows 1 and 2 are stamped.
        publish(RowCommit::LONE, 0, 2);
        assert!(fast().is_ok());
        assert!(fast().is_ok());
        assert_eq!(take_last_journaled().unwrap().class, RowCommit::LONE);
        // Row 3 is past the horizon: refused unwritten, through the core.
        assert!(matches!(fast(), Err(MetaError::NotAdmitted)));
        assert_eq!(meta.delegate_idx(5).unwrap(), 2, "nothing was journaled");
        // The prefix moves, the horizon with it: row 3 is within.
        publish(RowCommit::LONE, 1, 3);
        assert!(fast().is_ok());
        assert_eq!(meta.delegate_idx(5).unwrap(), 3);
        // A group rule waits for its group whatever the lead.
        publish(RowCommit::group(0, GroupOn::Delegate), 1, 3);
        assert!(fast().is_ok());
        assert_eq!(
            take_last_journaled().unwrap().class,
            RowCommit::group(0, GroupOn::Delegate)
        );
    }

    /// The gated mark spans this node's generations: an own gated row of
    /// one generation is open (above the prefix the core published as
    /// committed, and still in the journal) for every other generation's
    /// decision (`Meta::delegate_gated_open_elsewhere`, the core's
    /// hysteresis); the open list forgets a generation once its mark is
    /// committed or its row left the journal.
    #[test]
    fn a_gated_mark_is_open_to_the_other_generations_until_it_commits() {
        use crate::session::DelegDurable;
        use crate::store::journal::{clear_last_journaled, take_last_journaled, PendingClass};
        let meta = Meta::open_in_memory().unwrap();
        let mut n = 0;
        let mut journal = |gen: u64, class: RowCommit| -> (u64, RowCommit) {
            n += 1;
            let _class = PendingClass::set_for_own_client(class);
            clear_last_journaled();
            let (_, idx) = meta
                .delegate_execute(
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
                    gen,
                    Default::default(),
                    &crate::locks::LockTag::NONE,
                    Now::same(0),
                )
                .unwrap();
            (idx, take_last_journaled().unwrap().class)
        };
        // Generation 5's row 1 is gated, open.
        assert_eq!(
            journal(5, RowCommit::DELEG_GROUP),
            (1, RowCommit::DELEG_GROUP)
        );
        assert!(meta.delegate_gated_open_elsewhere(6).unwrap());
        assert!(
            !meta.delegate_gated_open_elsewhere(5).unwrap(),
            "not its own"
        );
        // The core publishes generation 5's row 1 committed: it is closed,
        // and 5 leaves the open list when the next gated row is marked.
        meta.session().set_deleg_durable(
            5,
            Some(DelegDurable {
                commit: RowCommit::LONE,
                through: 1,
                horizon: u64::MAX,
                lost: false,
            }),
        );
        assert!(!meta.delegate_gated_open_elsewhere(6).unwrap());
        assert_eq!(
            journal(6, RowCommit::DELEG_GROUP),
            (1, RowCommit::DELEG_GROUP)
        );
        let r = meta.db.read_tx();
        assert_eq!(read_gated_open(&r, &meta).unwrap(), vec![6]);
        drop(r);
        // A mark whose row left the journal (stranded with its generation's
        // end, or retired by the log) is not open either.
        assert_eq!(
            journal(5, RowCommit::DELEG_GROUP),
            (2, RowCommit::DELEG_GROUP)
        );
        assert!(meta.delegate_gated_open_elsewhere(6).unwrap());
        let r = meta.db.read_tx();
        let first = journal_origins_from(&r, &meta, 5, 2)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1;
        drop(r);
        let mut tx = meta.db.write_tx();
        remove_journal_origin(&mut tx, &meta, 5, 2, first);
        tx.commit().unwrap();
        assert!(!meta.delegate_gated_open_elsewhere(6).unwrap());
    }

    /// A takeover's strand of this node's own delegate rows leaves its
    /// stream counter past them; every generation this node holds is
    /// marked in the same transaction, holed or not: it ends at the gate.
    /// From the mark on, under the writer lock, the node's own delegate
    /// execution under it is refused unwritten (`NotAdmitted`: the fast
    /// path admitted it before the strand, the store is the fence), while
    /// the root's append of a delegate's row and the root's own writes are
    /// not.
    #[test]
    fn a_takeover_strand_ends_every_own_generation() {
        let meta = Meta::open_in_memory().unwrap();
        let tag = crate::locks::LockTag::NONE;
        let create = |name: &str, n: u64| MutateOp::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino: (3 << 40) | n,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let rid = |seq| Rid {
            node: 3,
            incarnation: 1,
            seq,
        };
        for n in 1..=3 {
            let (_, idx) = meta
                .delegate_execute(
                    &create(&format!("f{n}"), n),
                    Some(rid(n)),
                    5,
                    Default::default(),
                    &tag,
                    Now::same(0),
                )
                .unwrap();
            assert_eq!(idx, n);
        }
        // Before the strand the generation executes.
        assert!(!meta.delegate_stranded(5).unwrap());
        let stranded = meta.strand_for_takeover(2, &[5, 9], None).unwrap();
        assert_eq!(stranded.locals, 3, "{stranded:?}");
        assert!(meta.delegate_stranded(5).unwrap());
        assert!(
            meta.delegate_stranded(9).unwrap(),
            "a generation without a row here ends at the gate too"
        );
        assert!(
            !meta.delegate_stranded(11).unwrap(),
            "not one of this node's"
        );
        assert_eq!(meta.delegate_idx(5).unwrap(), 3, "the counter stays");
        assert_eq!(meta.log_stream_idx(5).unwrap(), 0);
        assert!(meta.delegate_txs_from(5, 1, usize::MAX).unwrap().is_empty());

        // The fence: the node's own execution under an ended generation is
        // refused, nothing written, no index consumed.
        let journal = meta.journal_len().unwrap();
        for gen in [5, 9] {
            let refused = meta.delegate_execute(
                &create("f4", 4),
                Some(rid(4)),
                gen,
                Default::default(),
                &tag,
                Now::same(0),
            );
            assert!(
                matches!(refused, Err(MetaError::NotAdmitted)),
                "gen {gen}: {refused:?}"
            );
            assert_eq!(meta.journal_len().unwrap(), journal);
            assert_eq!(meta.delegate_idx(5).unwrap(), 3);
            assert!(meta.lookup(ROOT_INO, "f4").unwrap().is_none());
        }
        // A generation this node still delegates is not fenced.
        meta.delegate_execute(
            &create("f4", 4),
            Some(rid(4)),
            11,
            Default::default(),
            &tag,
            Now::same(0),
        )
        .unwrap();

        // The root's append of a delegate's row of the ended generation is
        // not this node's execution: it applies.
        let delegate = Meta::open_in_memory().unwrap();
        let (records, idx) = delegate
            .delegate_execute(
                &create("f5", 5),
                Some(rid(5)),
                9,
                Default::default(),
                &tag,
                Now::same(0),
            )
            .unwrap();
        assert!(meta
            .apply_delegate_tx(&records, Some(rid(5)), 9, idx, Default::default())
            .unwrap());
        assert!(meta.lookup(ROOT_INO, "f5").unwrap().is_some());
        // And the root's own path keeps working.
        execute(&meta, &create("f6", 6), Some(rid(6))).unwrap();
        assert!(meta.lookup(ROOT_INO, "f6").unwrap().is_some());

        // Nothing stranded at all (the tail is the whole gate's work): the
        // generation still ends, or a write between the strand and the
        // tail's re-journal would precede it.
        let quiet = Meta::open_in_memory().unwrap();
        assert!(!quiet.strand_for_takeover(2, &[7], None).unwrap().any());
        assert!(quiet.delegate_stranded(7).unwrap());
        let refused = quiet.delegate_execute(
            &create("f1", 1),
            Some(rid(1)),
            7,
            Default::default(),
            &tag,
            Now::same(0),
        );
        assert!(
            matches!(refused, Err(MetaError::NotAdmitted)),
            "{refused:?}"
        );
        assert!(!quiet.strand_for_takeover(3, &[], None).unwrap().any());
    }
}

/// `local` kv: the next stream index a delegate assigns under a
/// generation.
fn deleg_idx_key(gen: u64) -> String {
    format!("deleg_idx:{gen}")
}

/// Whether this node assigned stream indices under `gen` (it executed rows
/// of it as its delegate), in the caller's snapshot or transaction.
pub(crate) fn deleg_idx_assigned_tx(
    r: &impl Readable,
    local: &SingleWriterTxKeyspace,
    gen: u64,
) -> Result<bool, MetaError> {
    Ok(kv_get_tx(r, local, &deleg_idx_key(gen))?
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|i| i > 0))
}

/// `local` kv: the highest index of generation `gen` this node, its
/// delegate, journaled for its own node's client with a gated class
/// (`Meta::delegate_gated_idx`).
fn deleg_gated_key(gen: u64) -> String {
    format!("deleg_gated:{gen}")
}

/// `local` kv: the generations whose gated mark ([`deleg_gated_key`]) may
/// still be open, comma-separated: added when a mark is set, taken out when
/// the mark is found committed or its row gone from the journal
/// ([`gated_open_elsewhere_tx`]). A few entries at most (the generations
/// this node holds), so a `Lone` row reads it in one point read.
const KV_DELEG_GATED_OPEN: &str = "deleg_gated_open";

fn read_gated_open(r: &impl Readable, meta: &Meta) -> Result<Vec<u64>, MetaError> {
    Ok(kv_get_tx(r, &meta.local, KV_DELEG_GATED_OPEN)?
        .map(|v| v.split(',').filter_map(|g| g.parse::<u64>().ok()).collect())
        .unwrap_or_default())
}

fn write_gated_open(tx: &mut SingleWriterWriteTx, meta: &Meta, gens: &[u64]) {
    if gens.is_empty() {
        tx.remove(&meta.local, KV_DELEG_GATED_OPEN.as_bytes().to_vec());
    } else {
        let v: Vec<String> = gens.iter().map(u64::to_string).collect();
        kv_set_tx(tx, &meta.local, KV_DELEG_GATED_OPEN, &v.join(","));
    }
}

/// `gen`'s gated mark was set: it may be open.
fn note_gated_open_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    gen: u64,
) -> Result<(), MetaError> {
    let mut open = read_gated_open(&*tx, meta)?;
    if !open.contains(&gen) {
        open.push(gen);
        write_gated_open(tx, meta, &open);
    }
    Ok(())
}

/// Whether generation `h`'s gated mark is open: above the prefix of `h`
/// the core last published as committed (`SessionState::deleg_covered`,
/// which only lags), and the marked row still in the journal. A mark at or
/// below the committed prefix is closed; so is one whose row left the
/// journal (the log carries it, or a recall's or a takeover's strand
/// dropped it with every row of `h` past the cut). Closed is for good: the
/// prefix only grows, and a later gated row of `h` notes it open again.
fn gated_mark_open(r: &impl Readable, meta: &Meta, h: u64) -> Result<bool, MetaError> {
    let mark = kv_get_tx(r, &meta.local, &deleg_gated_key(h))?
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if mark == 0 || mark <= meta.session().deleg_covered(h) {
        return Ok(false);
    }
    match journal_origins_from(r, meta, h, mark)?.next() {
        Some(Ok((idx, _))) => Ok(idx == mark),
        Some(Err(e)) => Err(e),
        None => Ok(false),
    }
}

/// [`Meta::delegate_gated_open_elsewhere`]'s read, and the entries found
/// closed.
fn gated_open_elsewhere_r(
    r: &impl Readable,
    meta: &Meta,
    gen: u64,
) -> Result<(bool, Vec<u64>), MetaError> {
    let mut closed = Vec::new();
    for h in read_gated_open(r, meta)? {
        if h == gen {
            continue;
        }
        if gated_mark_open(r, meta, h)? {
            return Ok((true, closed));
        }
        closed.push(h);
    }
    Ok((false, closed))
}

/// Plan 43 C2 (hysteresis across generations): whether an own row of `gen`
/// follows an open gated row of another generation of this node, under the
/// writer lock (the store only prunes the open list with it now; the core
/// decides from [`Meta::delegate_gated_open_elsewhere`]). At a takeover's strand every own generation ends together:
/// the uncommitted gated row is dropped (its client was not answered) and a
/// later `Lone` row of the other generation is replayed without it (a
/// `setxattr` of a file, its primary link unlinked, then a `removexattr`
/// under the next link's generation: the replay fails after its client was
/// told it succeeded). `prune`: the entries found closed for good are taken
/// out of the open list.
fn gated_open_elsewhere_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    gen: u64,
    prune: bool,
) -> Result<bool, MetaError> {
    let (open, closed) = gated_open_elsewhere_r(&*tx, meta, gen)?;
    if prune && !closed.is_empty() {
        let left: Vec<u64> = read_gated_open(&*tx, meta)?
            .into_iter()
            .filter(|g| !closed.contains(g))
            .collect();
        write_gated_open(tx, meta, &left);
    }
    Ok(open)
}

/// The `local` key prefixes of [`deleg_log_idx_key`] and [`deleg_void_cut_key`]:
/// scanned by the restart's seeding ([`Meta::persisted_generations`]) and
/// copied by the in-process rebuild ([`Meta::replace_ns_from_rebuilt`]).
pub(crate) const DELEG_LOG_IDX_PREFIX: &str = "deleg_log_idx:";
pub(crate) const DELEG_VOID_CUT_PREFIX: &str = "deleg_void_cut:";

/// `local` kv: the highest index of generation `gen` this replica holds
/// *from the log* (appended in the root's journal here, or applied from
/// a segment) — never a shadow's (phase 2b: a successor root's cursor,
/// a delegate's re-stream start). A row appended here counts only while
/// it is in the journal: one that leaves it unshipped takes the index
/// back below it ([`retract_log_idx_tx`], from `spec::strand_local_tx`).
pub(crate) fn deleg_log_idx_key(gen: u64) -> String {
    format!("{DELEG_LOG_IDX_PREFIX}{gen}")
}

/// `local` kv: the cut of generation `gen` the log's `RecallCut` record
/// carried (plan 43 C2): the highest stream index its root appended before
/// the generation ended. Raise-only, except that it goes when the journal
/// row of the record that set it is stranded unshipped
/// ([`retract_void_cut_tx`]). Absent for a generation ended by a
/// `Recall` (a log written before the cut was in the record): the cut is
/// then what the replica holds of it from the log (`deleg_log_idx_key`).
pub(crate) fn deleg_void_cut_key(gen: u64) -> String {
    format!("{DELEG_VOID_CUT_PREFIX}{gen}")
}

/// `local` kv: `epoch:idx`, the newest index of generation `gen` whose
/// root append the pre-S3 stream of root epoch `epoch` delivered here
/// while this node, the generation's delegate, still held the transaction
/// as its own `Local` row (L1). In that tenure's log such a row is earlier
/// than every transaction the stream delivers after it, so a streamed
/// transaction installed in its log place goes after it, not under it
/// (`spec::row_is_ordered_at`).
fn deleg_streamed_key(gen: u64) -> String {
    format!("deleg_streamed:{gen}")
}

/// What [`deleg_streamed_key`] holds for `gen`: `(epoch, idx)`.
pub(crate) fn deleg_streamed_tx(
    r: &impl Readable,
    local: &SingleWriterTxKeyspace,
    gen: u64,
) -> Result<Option<(u64, u64)>, MetaError> {
    Ok(
        kv_get_tx(r, local, &deleg_streamed_key(gen))?.and_then(|v| {
            let (e, i) = v.split_once(':')?;
            Some((e.parse().ok()?, i.parse().ok()?))
        }),
    )
}

/// The generation `gen` ended in the log: its [`deleg_streamed_key`] goes.
pub(crate) fn clear_deleg_streamed_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
) {
    tx.remove(local, deleg_streamed_key(gen).into_bytes());
}

/// The root's stream of epoch `epoch` delivered its append of delegation
/// origin `(gen, idx)`: if this node holds that transaction as its own
/// outstanding `Local` row, raise [`deleg_streamed_key`] to it. True when
/// it wrote.
pub(crate) fn note_deleg_streamed_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    epoch: u64,
    (gen, idx): (u64, u64),
) -> Result<bool, MetaError> {
    if gen == 0 {
        return Ok(false);
    }
    let held = match journal_origins_from(&*tx, meta, gen, idx)?.next() {
        Some(row) => row?.0 == idx,
        None => false,
    };
    if !held {
        return Ok(false);
    }
    match deleg_streamed_tx(&*tx, &meta.local, gen)? {
        Some((e, _)) if e > epoch => return Ok(false),
        Some((e, i)) if e == epoch && i >= idx => return Ok(false),
        _ => {}
    }
    kv_set_tx(
        tx,
        &meta.local,
        &deleg_streamed_key(gen),
        &format!("{epoch}:{idx}"),
    );
    Ok(true)
}

/// `local` kv: the boot on which this node, the delegate of `gen`, was
/// granted it ([`Meta::deleg_note_held`]).
fn deleg_held_key(gen: u64) -> String {
    format!("deleg_held_boot:{gen}")
}

/// `local` kv: generation `gen`'s stream lost rows to a takeover's
/// strand (`Meta::strand_for_takeover`); this node, its delegate, never
/// executes in it again.
fn deleg_stranded_key(gen: u64) -> String {
    format!("deleg_stranded:{gen}")
}

/// Mark generation `gen` stranded (see [`deleg_stranded_key`]).
pub(crate) fn mark_deleg_stranded_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
) {
    kv_set_tx(tx, local, &deleg_stranded_key(gen), "1");
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

/// Take the persisted log index of `gen` back below `idx`: the row at
/// `idx` was appended in this root's journal (`bump_log_idx_tx` counted
/// it) and now leaves the journal without a segment carrying it. Lowered
/// only when the index counted that row (`idx` at or below it); true
/// when it did. A row a segment carried retired with the segment's own
/// bump, so every journal row of `gen` sits above what the log holds,
/// and a delegate's own rows were never counted here at all.
pub(crate) fn retract_log_idx_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
    idx: u64,
) -> Result<bool, MetaError> {
    if gen == 0 || idx == 0 {
        return Ok(false);
    }
    let key = deleg_log_idx_key(gen);
    let cur = kv_get_tx(tx, local, &key)?
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if cur < idx {
        return Ok(false);
    }
    kv_set_tx(tx, local, &key, &(idx - 1).to_string());
    Ok(true)
}

/// Raise the persisted cut of the ended generation `gen` to `through`
/// (see [`deleg_void_cut_key`]).
pub(crate) fn bump_void_cut_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
    through: u64,
) -> Result<(), MetaError> {
    let key = deleg_void_cut_key(gen);
    let cur = kv_get_tx(tx, local, &key)?.and_then(|v| v.parse::<u64>().ok());
    if cur.is_none_or(|c| through > c) {
        kv_set_tx(tx, local, &key, &through.to_string());
    }
    Ok(())
}

/// A `Recall { gen }` of an older log (no cut in the record) applied from
/// the log: the cut kept is what the log holds of the generation, as the
/// session voids it (`Replica::apply_segment`), so a restart, and a
/// delegate's install that waits for the end of its old generation
/// ([`Meta::deleg_gens_awaiting_end`]), find the end applied. Returns that
/// cut.
pub(crate) fn bump_void_cut_at_log_idx_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
) -> Result<u64, MetaError> {
    let log = kv_get_tx(tx, local, &deleg_log_idx_key(gen))?
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    bump_void_cut_tx(tx, local, gen, log)?;
    Ok(log)
}

/// [`Meta::deleg_gens_awaiting_end`] in the caller's snapshot or
/// transaction.
pub(crate) fn deleg_gens_awaiting_end_tx(
    r: &impl Readable,
    meta: &Meta,
    live: &std::collections::BTreeSet<u64>,
) -> Result<Vec<u64>, MetaError> {
    let floor = journal::acked_watermark(r, &meta.local)?;
    let mut out = Vec::new();
    // One seek per generation the keyspace holds (it is ordered by
    // generation), not a walk of every outstanding row.
    let mut gen = 1u64;
    loop {
        let lo = journal_origin_key(gen, 0, 0);
        let mut next = None;
        for guard in r.range(&meta.journal_origin, lo..) {
            let (g, _, first) = decode_journal_origin_key(&guard.key()?)?;
            if g != next.map_or(g, |(n, _)| n) {
                break;
            }
            let held = first > floor;
            next = Some((g, held));
            if held {
                break;
            }
        }
        let Some((g, held)) = next else {
            break;
        };
        if held
            && !live.contains(&g)
            && kv_get_tx(r, &meta.local, &deleg_void_cut_key(g))?.is_none()
        {
            out.push(g);
        }
        let Some(after) = g.checked_add(1) else {
            break;
        };
        gen = after;
    }
    Ok(out)
}

/// Forget the persisted cut of `gen` set by a `RecallCut { through }` that
/// this node journaled and is now stranded unshipped, so the log never
/// carries it (`spec::strand_local_tx`). The cut is removed only while it
/// is that record's: a cut that the log's own record raised is left alone.
pub(crate) fn retract_void_cut_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    gen: u64,
    through: u64,
) -> Result<(), MetaError> {
    let key = deleg_void_cut_key(gen);
    if kv_get_tx(tx, local, &key)?.and_then(|v| v.parse::<u64>().ok()) == Some(through) {
        tx.remove(local, key.as_bytes().to_vec());
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

/// Tests only (`test-util`): fail this thread's next `n` calls of
/// [`Meta::reclass_journal_tx`] with an I/O error, nothing written — a store
/// that cannot record the class a gate-off answer would claim.
#[cfg(any(test, feature = "test-util"))]
pub fn fail_reclass_on_this_thread_for_tests(n: u32) {
    reclass_fault::LEFT.with(|c| c.set(n));
}

#[cfg(any(test, feature = "test-util"))]
mod reclass_fault {
    thread_local! {
        pub(super) static LEFT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    pub(super) fn take() -> bool {
        LEFT.with(|c| {
            let n = c.get();
            c.set(n.saturating_sub(1));
            n > 0
        })
    }
}

/// `journal_origin` against the walk of the whole journal it replaced:
/// on randomized journals (a delegate's own streams, a root's appends of
/// another's, this node's own rows, segments retiring stream rows, ships,
/// recall strands), every read by origin answers exactly what the walk
/// did.
#[cfg(test)]
mod origin_index_tests {
    use super::*;
    use crate::mutate::{execute, MutateOp};
    use crate::rid::Rid;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_types::clock::Now;
    use std::collections::HashSet;

    /// The walk `delegate_txs_from` did before `journal_origin`.
    fn txs_from_walk(meta: &Meta, gen: u64, from_idx: u64, max_rows: usize) -> Vec<DelegateTx> {
        let r = meta.db.read_tx();
        let mut pending = crate::store::remote::PendingView::new(&r, meta).unwrap();
        let mut out = Vec::new();
        let mut rows = 0usize;
        let mut txs: Vec<(u64, JournalTx)> = read_journal_txs(&r, meta)
            .unwrap()
            .into_iter()
            .filter(|(_, row)| row.gen == gen && row.idx >= from_idx)
            .collect();
        txs.sort_by_key(|(_, row)| row.idx);
        for (first, row) in txs {
            let records: Vec<LogRecord> = journal_records(&r, meta, first, row.last)
                .unwrap()
                .into_iter()
                .map(|(_, rec)| rec)
                .collect();
            if pending.names_pending(&records).unwrap() {
                break;
            }
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
        out
    }

    /// The walk `journal_origins` did.
    fn origins_walk(meta: &Meta, seqs: &[u64]) -> Vec<(u64, u64)> {
        let r = meta.db.read_tx();
        let heads = read_journal_tx_heads(&r, meta).unwrap();
        seqs.iter()
            .map(|seq| {
                heads
                    .iter()
                    .find(|(first, h)| *first <= *seq && *seq <= h.last)
                    .map(|(_, h)| (h.gen, h.idx))
                    .unwrap_or((0, 0))
            })
            .collect()
    }

    /// The set `delegate_txs_pending` read.
    fn pending_walk(meta: &Meta) -> HashSet<(u64, u64)> {
        let r = meta.db.read_tx();
        read_journal_tx_heads(&r, meta)
            .unwrap()
            .into_iter()
            .filter(|(_, h)| h.gen != 0)
            .map(|(_, h)| (h.gen, h.idx))
            .collect()
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    fn mkdir(name: &str, ino: u64) -> LogRecord {
        LogRecord::Mkdir {
            parent: ROOT_INO,
            name: name.into(),
            ino,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }
    }

    fn create(parent: u64, name: String, ino: u64) -> MutateOp {
        MutateOp::Create {
            parent,
            name,
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    }

    /// Checks every read; returns the outstanding rows of each origin.
    fn check(meta: &Meta, rng: &mut Rng, gens: &[u64], step: usize) -> HashSet<(u64, u64)> {
        let walk = pending_walk(meta);
        let top = walk.iter().map(|(_, i)| *i).max().unwrap_or(0) + 2;
        for &gen in gens {
            for from in [0, 1, rng.below(top), rng.below(top)] {
                for max in [1, 1 + rng.below(4) as usize, usize::MAX] {
                    assert_eq!(
                        meta.delegate_txs_from(gen, from, max).unwrap(),
                        txs_from_walk(meta, gen, from, max),
                        "step {step}: delegate_txs_from({gen}, {from}, {max})"
                    );
                }
            }
            for idx in 0..=top {
                assert_eq!(
                    meta.delegate_tx_pending(gen, idx).unwrap(),
                    if gen == 0 {
                        let r = meta.db.read_tx();
                        read_journal_tx_heads(&r, meta)
                            .unwrap()
                            .iter()
                            .any(|(_, h)| h.gen == 0 && h.idx == idx)
                    } else {
                        walk.contains(&(gen, idx))
                    },
                    "step {step}: delegate_tx_pending({gen}, {idx})"
                );
            }
        }
        let journal: Vec<u64> = meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        let tip = journal.last().copied().unwrap_or(0) + 3;
        let mut probes = vec![journal.clone()];
        if !journal.is_empty() {
            let a = rng.below(journal.len() as u64) as usize;
            let b = a + rng.below((journal.len() - a) as u64) as usize;
            probes.push(journal[a..=b].to_vec());
        }
        probes.push((0..8).map(|_| rng.below(tip)).collect());
        probes.push((0..tip).rev().collect());
        probes.push((0..tip).collect());
        let mut sparse: Vec<u64> = (0..6).map(|_| rng.below(tip)).collect();
        sparse.sort();
        probes.push(sparse);
        for seqs in probes {
            assert_eq!(
                meta.journal_origins(&seqs).unwrap(),
                origins_walk(meta, &seqs),
                "step {step}: journal_origins({seqs:?})"
            );
        }
        walk
    }

    fn run(seed: u64, steps: usize) {
        let meta = Meta::open_in_memory().unwrap();
        let tag = crate::locks::LockTag::NONE;
        let (d, d2, c, e, f) = (2 << 40, 5 << 40, 6 << 40, 3 << 40, 4 << 40);
        let mut seg = 1;
        meta.apply_segment(
            seg,
            1,
            &[
                mkdir("d", d | 1),
                mkdir("d2", d2 | 1),
                mkdir("c", c | 1),
                mkdir("e", e | 1),
                mkdir("f", f | 1),
            ],
            &TouchSet::default(),
        )
        .unwrap();
        let mut rng = Rng(seed | 1);
        let mut n = 100u64;
        let own_a = 7u64;
        let mut own_b = 9u64;
        let appended = 11u64;
        let mut appended_idx = 0u64;
        let mut gens = vec![0, own_a, own_b, appended, 99];
        let (mut seen, mut gone, mut last_held) = (HashSet::new(), HashSet::new(), HashSet::new());
        let mut oldest_a = 0;
        let rid = |seq: u64| Rid {
            node: 3,
            incarnation: 1,
            seq,
        };
        for step in 0..steps {
            n += 1;
            match rng.below(10) {
                0..=2 => {
                    meta.delegate_execute(
                        &create(d | 1, format!("a{n}"), d | n),
                        Some(rid(n)),
                        own_a,
                        Default::default(),
                        &tag,
                        Now::same(0),
                    )
                    .unwrap();
                }
                3 => {
                    meta.delegate_execute(
                        &create(d2 | 1, format!("b{n}"), d2 | n),
                        Some(rid(n)),
                        own_b,
                        Default::default(),
                        &tag,
                        Now::same(0),
                    )
                    .unwrap();
                }
                4 => {
                    // A root's append of another delegate's transaction,
                    // one to three records.
                    appended_idx += 1;
                    let records: Vec<LogRecord> = (0..1 + rng.below(3))
                        .map(|k| LogRecord::Create {
                            parent: c | 1,
                            name: format!("c{n}-{k}"),
                            ino: c | (n * 4 + k),
                            mode: 0o644,
                            uid: 0,
                            gid: 0,
                            time_ns: 5,
                        })
                        .collect();
                    let rid = (rng.below(2) == 0).then_some(Rid {
                        node: 4,
                        incarnation: 1,
                        seq: n,
                    });
                    meta.apply_delegate_tx(
                        &records,
                        rid,
                        appended,
                        appended_idx,
                        crate::session::Position::ZERO,
                    )
                    .unwrap();
                }
                5 => {
                    execute(&meta, &create(e | 1, format!("e{n}"), e | n), Some(rid(n))).unwrap();
                }
                6 => {
                    // A segment brings the oldest few of the own stream back.
                    let back = txs_from_walk(&meta, own_a, 0, 1 + rng.below(4) as usize);
                    seg += 1;
                    let mut records = vec![LogRecord::Create {
                        parent: f | 1,
                        name: format!("f{n}"),
                        ino: f | n,
                        mode: 0o644,
                        uid: 0,
                        gid: 0,
                        time_ns: 5,
                    }];
                    let mut rows = vec![seg * 1000];
                    let mut origins = vec![(0, 0)];
                    for t in back {
                        for r in t.records {
                            if !matches!(r, LogRecord::Atime { .. }) {
                                rows.push(rows.last().unwrap() + 1);
                                origins.push((own_a, t.idx));
                            }
                            records.push(r);
                        }
                    }
                    let through = *rows.last().unwrap();
                    meta.apply_segment_rows(
                        seg,
                        1,
                        through,
                        &rows,
                        &origins,
                        &records,
                        &TouchSet::default(),
                    )
                    .unwrap();
                }
                7 => {
                    // A ship of the journal's head, whole transactions.
                    let batch = meta.take_journal(64).unwrap();
                    let want = 1 + rng.below(batch.len() as u64) as usize;
                    let k = meta.whole_tx_prefix(&batch, want).unwrap();
                    let seqs: Vec<u64> = batch[..k].iter().map(|(s, _)| *s).collect();
                    seg += 1;
                    meta.ack_journal_rows_at(&seqs, seg).unwrap();
                }
                8 if rng.below(4) == 0 => {
                    // A segment recalls the second generation: its rows
                    // strand, and a later one takes its place.
                    seg += 1;
                    meta.apply_segment_rows(
                        seg,
                        1,
                        seg * 1000,
                        &[seg * 1000],
                        &[(0, 0)],
                        &[LogRecord::Recall {
                            dir: d2 | 1,
                            gen: own_b,
                        }],
                        &TouchSet::default(),
                    )
                    .unwrap();
                    own_b += 2 + rng.below(3) * 2;
                    gens.push(own_b);
                }
                9 if rng.below(40) == 0 => {
                    // A deposition strands everything journaled.
                    meta.strand_below_epoch(1).unwrap();
                }
                _ => {}
            }
            for (gen, idx) in check(&meta, &mut rng, &gens, step) {
                seen.insert(gen);
                if gen == own_a {
                    oldest_a = oldest_a.max(idx);
                }
            }
            let now_held = pending_walk(&meta);
            gone.extend(last_held.difference(&now_held).map(|(g, _)| *g));
            last_held = now_held;
        }
        // Every kind of row was outstanding, and left, at some point.
        assert!(gens.len() > 5, "seed {seed}: no recall strand");
        for g in [own_a, appended] {
            assert!(
                seen.contains(&g) && gone.contains(&g),
                "seed {seed}: gen {g}"
            );
        }
        assert!(
            gone.iter().any(|g| ![own_a, appended].contains(g)),
            "seed {seed}: no strand"
        );
        assert!(oldest_a > 0);
    }

    #[test]
    fn reads_by_origin_match_the_walk_of_the_journal() {
        for seed in [1, 0x9e37_79b9, 42, 7_777, 31_337, 0xdead_beef] {
            run(seed, 160);
        }
    }
}

/// Plan 46 M1.2 step 4 (C3-1, c3-spec D2): the root's append of a delegate's
/// row journals the streamed records as they are.
#[cfg(test)]
mod verbatim_append_tests {
    use super::*;
    use crate::rid::Rid;
    use constellation_fs_core::types::ROOT_INO;

    fn rid(seq: u64) -> Rid {
        Rid {
            node: 3,
            incarnation: 1,
            seq,
        }
    }

    fn create(name: &str, n: u64) -> LogRecord {
        LogRecord::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino: (3 << 40) | n,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 5,
        }
    }

    /// Every journal row, in order, with its transaction's head.
    fn journal(meta: &Meta) -> Vec<(JournalTx, Vec<LogRecord>)> {
        let r = meta.db.read_tx();
        let mut heads = read_journal_txs(&r, meta).unwrap();
        heads.sort_by_key(|(first, _)| *first);
        heads
            .into_iter()
            .map(|(first, row)| {
                let recs = journal_records(&r, meta, first, row.last)
                    .unwrap()
                    .into_iter()
                    .map(|(_, rec)| rec)
                    .collect();
                (row, recs)
            })
            .collect()
    }

    /// `[Create, Atime, Completed]`, an atime-only transaction, a
    /// transaction whose `Refused` precedes its `Completed`, and one that
    /// ends in an `InboxAck`: the root's journal carries each as streamed.
    /// (Before: the `Atime` was dropped, the atime-only transaction
    /// journaled nothing, the `Refused` moved behind the rows.)
    #[test]
    fn an_appended_row_is_journaled_verbatim() {
        let meta = Meta::open_in_memory().unwrap();
        let gen = 9;
        let atime = LogRecord::Atime {
            ino: (3 << 40) | 1,
            atime_ns: 7,
            time_ns: 7,
        };
        let txs: Vec<(Option<Rid>, Vec<LogRecord>)> = vec![
            (
                Some(rid(1)),
                vec![
                    create("a", 1),
                    atime.clone(),
                    LogRecord::Completed { rid: rid(1) },
                ],
            ),
            (
                Some(rid(2)),
                vec![atime.clone(), LogRecord::Completed { rid: rid(2) }],
            ),
            (
                Some(rid(3)),
                vec![
                    create("b", 2),
                    LogRecord::Refused {
                        rid: rid(4),
                        code: constellation_types::Code::NotFound,
                    },
                    LogRecord::Completed { rid: rid(3) },
                ],
            ),
            (
                Some(rid(5)),
                vec![
                    create("c", 3),
                    LogRecord::Completed { rid: rid(5) },
                    LogRecord::InboxAck {
                        epoch: 1,
                        node: 3,
                        n: 1,
                        i: 0,
                    },
                ],
            ),
        ];
        for (k, (r, records)) in txs.iter().enumerate() {
            assert!(meta
                .apply_delegate_tx(records, *r, gen, k as u64 + 1, Default::default())
                .unwrap());
        }
        let got = journal(&meta);
        assert_eq!(got.len(), txs.len());
        for (k, ((row, recs), (_, want))) in got.iter().zip(&txs).enumerate() {
            assert_eq!((row.gen, row.idx), (gen, k as u64 + 1));
            assert_eq!(recs, want, "transaction {}", k + 1);
        }
        assert_eq!(meta.take_deleg_append_skipped(), 0);
        // The streams read back the same (what a re-stream carries).
        let back = meta.delegate_txs_from(gen, 1, usize::MAX).unwrap();
        assert_eq!(
            back.iter().map(|t| t.records.clone()).collect::<Vec<_>>(),
            txs.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>()
        );
        // And a record the apply could not take is counted, not dropped.
        let orphan = LogRecord::Create {
            parent: (9 << 40) | 99,
            name: "x".into(),
            ino: (3 << 40) | 50,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 5,
        };
        let records = vec![orphan, LogRecord::Completed { rid: rid(6) }];
        meta.apply_delegate_tx(&records, Some(rid(6)), gen, 5, Default::default())
            .unwrap();
        assert_eq!(journal(&meta).last().unwrap().1, records);
        assert_eq!(meta.take_deleg_append_skipped(), 1);
    }

    /// The journal row of an appended transaction names its rid and no op:
    /// nothing may replay it as `MutateOp::Records`.
    #[test]
    fn an_appended_row_journals_no_replay_op() {
        let meta = Meta::open_in_memory().unwrap();
        let records = vec![create("a", 1), LogRecord::Completed { rid: rid(1) }];
        assert!(meta
            .apply_delegate_tx(&records, Some(rid(1)), 9, 1, Default::default())
            .unwrap());
        let rows = journal(&meta);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.rid, Some(rid(1)));
        assert!(rows[0].0.op.is_none(), "{:?}", rows[0].0.op);
    }
}
