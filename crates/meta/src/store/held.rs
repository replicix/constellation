//! Poison-record isolation (plan 30 §M4 item 2): one unrecoverable
//! pending chunk holds back only the journal records that need it.
//!
//! # The problem
//!
//! A manifest record may only ship once every chunk it names is in S3 —
//! otherwise every other node would bootstrap a file whose content the
//! bucket never received. `cli::upload_dirty_chunks` uploads the pending
//! chunks before each round ships the journal. When a pending chunk is
//! gone from the local cache (the plan 29 M6 "missing from local cache"
//! condition), it can never be uploaded, and before M4 the whole round
//! failed so that the manifest would not ship — and with it every other
//! record of every other file, forever (plan 30 §1.2, L7).
//!
//! # What is held
//!
//! The upload pass records the unrecoverable `(chunk, ino)` pairs here
//! ([`Meta::note_unrecoverable_chunks`], persisted under `local`'s
//! `poisoned/` prefix, so a restart holds them back before its first
//! upload pass). A ship then plans over the whole unshipped journal,
//! transaction by transaction, in journal order ([`plan`]):
//!
//! - a **seed** is a transaction with a `WriteManifest` for a poisoned
//!   inode that names one of its unrecoverable chunks (a spilled or
//!   undecodable manifest is assumed to);
//! - a transaction is **held** if it is a seed, or if it *conflicts* on an
//!   `ns` key with a held transaction — its key set is its captured
//!   before-image keys (`store::spec`), exactly what the M3b capture
//!   recorded, each with the way it was written ([`Mode`]). Held keys
//!   accumulate, so the rule is transitive;
//! - everything else ships, in journal order, skipping the held ones.
//!
//! Two accesses of a key conflict unless both are commutative ([`conflicts`],
//! ADR-59): an exclusive access (a name, an attribute set as is, a removal,
//! a manifest) orders everything on the key; a directory's *merged*
//! attributes (`mtime`/`ctime` = `max`, `nlink` += delta, written because a
//! child came or went) order nothing among themselves and nothing against
//! an observation. So a create, unlink or rename of one name does not wait
//! behind a held operation on another name of the same directory, though
//! both rewrite the directory's record and its name's attribute copy.
//!
//! Only earlier transactions can be depended on, and a shipped
//! transaction conflicts with no held key, so shipping it ahead of the held
//! ones yields the same state as shipping in journal order would once they
//! ship: the two commute. (Where they share a key it is a merge on both
//! sides, and merges commute as values; a merge and an observation
//! commute because no journaled answer reads a merged field. Pairs that
//! conflict keep journal order, by induction over the journal.) An **uncaptured** transaction (journaled
//! without before-images: a holder always captures, so only a write from
//! before this node held the lease) has no known key set: once anything is held, it and everything
//! after it is held — the pre-M4 behaviour, from that point on.
//!
//! # How held work stays speculative and consistent (M3b)
//!
//! Nothing new is needed for a held transaction to stay speculation: it
//! is an ordinary outstanding `Local` row (its `journal_tx` row and
//! before-images stay), so
//! - the **publisher** substitutes its keys' earliest before-images
//!   (`Meta::publish_basis_at`); a transaction that shipped past it becomes
//!   a `Foreign` row (`spec::retire_local_tx`), which the substitution
//!   skips because it conflicts with none of the held keys (a key they
//!   share is a merge on both sides: the substitution applies the shipped
//!   row's merge to the earlier value, `LogPrefixView::merge_shipped`);
//! - a **transaction never splits**: holding is per transaction, and a
//!   ship's byte cut still goes through `Meta::whole_tx_prefix`;
//! - a **deposition** strands it like any unshipped `Local` row (rollback,
//!   replay by rid through the new holder, where it is a seed again);
//! - a **segment inserted before local work** redoes the out-of-order
//!   shipped rows ahead of the inserted segment (`spec::rewind_tx`),
//!   because they precede it in the log;
//! - the journal's acked watermark stops below the oldest held row
//!   (`journal::ack_rows_at`), so every journal scan still sees it.
//!
//! The held journal still counts as backlog, so a holder with held records
//! does not idle-release its lease (nobody else could ship them); other
//! nodes keep writing through it by forwarding.
//!
//! # Dropping
//!
//! `constellation repair drop-held <ino>` ([`Meta::drop_held`]) rolls the
//! inode's seeds and every held transaction depending on them back from
//! their before-images (the deposition machinery, restricted to those
//! rows), queues the dependents for replay by rid, and turns each seed
//! into a refused replay whose conflict copy — the manifest with the
//! unrecoverable chunks as holes — the replay drain materializes under
//! `.constellation-conflict/`. The unrecoverable pending rows go with it.
//! ("Depends" is the ship rule's [`conflicts`].)

//! **A lost chunk an op still names keeps its row.** "No pending row"
//! means *durable* to the ship plan, to the pending list a forward
//! carries, and to the composition of a manifest by a write session that
//! re-flushes the hashes it sealed. The drop removes a row believing every
//! reference to the chunk became a hole — but it can see only the journal
//! and the replay queue, not an op of this node that has not executed yet
//! (a client op behind a handoff pause, a parked forward, a delegate's
//! parked op) or a write session that will commit later. Such a manifest
//! would ship naming a chunk S3 never received: a permanent `EIO` on every
//! node, an acknowledged write silently lost, no conflict copy. So the
//! core (the ops) and the engine (the sessions) name the chunks they
//! still use ([`LiveSessions`], `drop_held_knowing`'s `live_ops`), and the
//! drop keeps those chunks' pending rows and poison marks (the remote
//! mark too). The op that executes later is a seed of the next drop; the
//! engine fails a session naming a dropped chunk (`EIO` once).
//!
//! The queue keeps every rolled-back transaction in journal order, but
//! not every entry is a copy of its op (plan 46 M0.2, the drop-held
//! proposal's option B). An operator's drop re-executes only this node's
//! own rows: another node's row whose answer was never given (journaled
//! `Archive`, which a held, unshipped row never reaches, and with no
//! outcome in the log — [`spec::mark_await_retry_tx`]) belongs to its
//! requester, whose client entry retries it by rid. Queued as well, it
//! would run twice, and its dependents would run without its effect and
//! ahead of the retry. It becomes a *placeholder* (`spec::AwaitRetry`)
//! instead: its place in the queue, its rid and the keys its records
//! touched, no op. The replay drain holds every later entry on those keys
//! behind it; an op arriving on them waits for it; its requester's retry
//! executes in its place; after `drop_held_await_ms` the sequencer
//! settles the rid (`Refused { rid, ABANDONED }`). A row of another node
//! whose answer may have been given (a group's, a hold's) keeps its op:
//! its client is not retrying it, and this queue is its only copy.
//!
//! # Deferred: chunks still uploading (plan 30 §M7)
//!
//! The same planner defers a transaction whose manifest names a chunk
//! that is merely *pending* — not uploaded yet, not lost. Before M7 a
//! round uploaded every pending chunk before shipping anything, so after
//! a write-back burst the whole journal, however unrelated, waited for
//! the burst's upload backlog: a one-byte marker file written next to a
//! 384 MiB burst became visible on other nodes only once the whole burst
//! was in S3 (`visibility-after-burst`). Now a round waits for the
//! upload pass only briefly, and the ship plans around what is still
//! pending exactly as it plans around what is lost: pending seeds and
//! their dependents wait (a *deferred* transaction), everything else
//! ships. Deferred work is not held work — it is not reported under
//! `held` and needs no repair; it ships by itself once the pass acks its
//! chunks. It also closes a window the round's ordering left open: a
//! manifest journaled after the pass took its snapshot of the pending
//! rows used to ship with its chunks still unuploaded.
//!
//! # Cost
//!
//! Nothing when no chunk is poisoned: one counter read
//! (`store::KV_POISONED_COUNT`) per ship round and per full upload pass,
//! none per inode drain, and nothing on the ack (plan 30 §M4 round 2;
//! round 1 prefix-scanned `local` there and computed a conversion
//! boundary on every ack — see `spec::retire_local_tx`). With one, every round re-plans the whole unshipped journal,
//! reading each captured transaction's key set; that grows with what is
//! held, which `status` shows and `drop-held` ends.

use crate::error::MetaError;
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::store::{
    adjust_usage_tx, counter_add_get_tx, counter_add_tx, counter_get, counter_set_tx, journal,
    local, spec, Meta, UsageTracker, KV_ACKED_LOST_TOTAL, KV_DROP_HELD_UNSHIPPED,
    KV_LOCAL_SPEC_COUNT, KV_POISONED_COUNT, KV_UNCAPTURED_TX_COUNT,
};
use constellation_fs_core::{ChunkInfo, Ino, Manifest};
use fjall::Readable;
use replicix_chunkfmt::ChunkHash;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;

const POISON_PREFIX: &[u8] = b"poisoned/";

/// The code a dropped op's `Refused` record carries (`EIO`): the write
/// was acknowledged and then lost with its chunk; its conflict copy is
/// the artifact.
pub const DROPPED_CODE: constellation_types::Code = constellation_types::Code::Io;

/// Plan 30 §M9 × §M4: a spilled manifest adopted from a predecessor's
/// backup tail whose chunk list is not expanded into pending rows yet:
/// `adopted-spill/<ino><blob hash>`. While one exists its inode's
/// manifests are deferral seeds (`read_pending`).
const ADOPTED_SPILL_PREFIX: &[u8] = b"adopted-spill/";

fn adopted_spill_key(ino: Ino, blob: &ChunkHash) -> Vec<u8> {
    let mut k = ADOPTED_SPILL_PREFIX.to_vec();
    k.extend_from_slice(&ino.to_be_bytes());
    k.extend_from_slice(&blob.0);
    k
}

/// Enroll every chunk `manifest` names for `ino` as a pending upload
/// (see `Meta::apply_adopted_records`). An undecodable manifest enrolls
/// nothing it could name; the planner already treats it as naming
/// anything (`names_any`).
pub(crate) fn enroll_adopted_manifest_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    manifest: &[u8],
) -> Result<(), MetaError> {
    let Ok(m) = Manifest::decode(manifest) else {
        return Ok(());
    };
    match m.chunks {
        ChunkInfo::Inline(chunks) => {
            for hash in chunks.values() {
                crate::store::misc::add_pending_claim_tx(tx, meta, hash, ino)?;
            }
        }
        ChunkInfo::Spilled(blob) => {
            crate::store::misc::add_pending_claim_tx(tx, meta, &blob, ino)?;
            tx.insert(&meta.local, adopted_spill_key(ino, &blob), Vec::new());
        }
    }
    Ok(())
}

fn read_adopted_spills(r: &impl Readable, meta: &Meta) -> Result<Vec<(Ino, ChunkHash)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.prefix(&meta.local, ADOPTED_SPILL_PREFIX) {
        let (k, _) = guard.into_inner()?;
        let rest = &k[ADOPTED_SPILL_PREFIX.len()..];
        if rest.len() != 40 {
            return Err(MetaError::Invalid("malformed adopted-spill mark".into()));
        }
        let ino = u64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
        let blob = ChunkHash(rest[8..].try_into().expect("32 bytes"));
        out.push((ino, blob));
    }
    Ok(out)
}

impl Meta {
    /// Plan 30 §M9 × §M4: the adopted spilled manifests whose chunk lists
    /// still wait to be enrolled, `(ino, list blob)`. The upload pass
    /// reads each blob (cache, else S3) and calls
    /// [`Self::expand_adopted_spill`]; a blob it cannot read stays
    /// pending, and is recorded unrecoverable like any lost chunk.
    pub fn adopted_spills(&self) -> Result<Vec<(Ino, ChunkHash)>, MetaError> {
        let r = self.db.read_tx();
        read_adopted_spills(&r, self)
    }

    /// Enroll the chunk list of an adopted spilled manifest (`chunks`,
    /// decoded from its blob) and clear its mark, in one transaction.
    pub fn expand_adopted_spill(
        &self,
        ino: Ino,
        blob: &ChunkHash,
        chunks: &[ChunkHash],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.local, adopted_spill_key(ino, blob))?.is_none() {
            return Ok(());
        }
        for hash in chunks {
            crate::store::misc::add_pending_claim_tx(&mut tx, self, hash, ino)?;
        }
        tx.remove(&self.local, adopted_spill_key(ino, blob));
        tx.commit()?;
        Ok(())
    }
}

fn poison_key(hash: &ChunkHash, ino: Ino) -> Vec<u8> {
    let mut k = POISON_PREFIX.to_vec();
    k.extend_from_slice(&hash.0);
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

fn pending_key(hash: &ChunkHash, ino: Ino) -> Vec<u8> {
    let mut k = hash.0.to_vec();
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

/// Unrecoverable chunks per inode.
pub(crate) type PoisonMap = BTreeMap<Ino, BTreeSet<ChunkHash>>;

/// The recorded unrecoverable pairs that still have a pending-upload row
/// (a row acked or cancelled since — the chunk turned up after all, or
/// the file was rewritten — no longer poisons anything).
///
/// Plan 30 §M4 round 2: one point read (`KV_POISONED_COUNT`) when nothing
/// is recorded — the ship path's cost with no poison — and only then the
/// prefix scan. A malformed mark is an error, not skipped, so a path that
/// scans when it should not is caught by `local.rs`'s cost tests.
fn read_poisoned(r: &impl Readable, meta: &Meta) -> Result<PoisonMap, MetaError> {
    let mut out = PoisonMap::new();
    if counter_get(r, &meta.local, KV_POISONED_COUNT)? == 0 {
        return Ok(out);
    }
    for guard in r.prefix(&meta.local, POISON_PREFIX) {
        let (k, _) = guard.into_inner()?;
        let rest = &k[POISON_PREFIX.len()..];
        if rest.len() != 40 {
            return Err(MetaError::Invalid(format!(
                "malformed unrecoverable-chunk mark ({} bytes)",
                rest.len()
            )));
        }
        let hash = ChunkHash(rest[..32].try_into().expect("32 bytes"));
        let ino = u64::from_be_bytes(rest[32..].try_into().expect("8 bytes"));
        if r.get(&meta.pending_upload, pending_key(&hash, ino))?
            .is_some()
        {
            out.entry(ino).or_default().insert(hash);
        }
    }
    Ok(out)
}

/// Every pending (not yet uploaded) chunk per inode: the deferral seeds
/// (plan 30 §M7). One seek when nothing is pending — the ship path's cost
/// in write-through steady state. An adopted spilled manifest whose list
/// is not expanded yet counts as pending under its blob (so it waits for
/// the expansion even once the blob itself is acknowledged).
fn read_pending(r: &impl Readable, meta: &Meta) -> Result<PoisonMap, MetaError> {
    let mut out = PoisonMap::new();
    for (ino, blob) in read_adopted_spills(r, meta)? {
        out.entry(ino).or_default().insert(blob);
    }
    for guard in r.iter(&meta.pending_upload) {
        let (k, _) = guard.into_inner()?;
        if k.len() != 40 {
            return Err(MetaError::Invalid("pending_upload key length".into()));
        }
        let hash = ChunkHash(k[..32].try_into().expect("32 bytes"));
        let ino = u64::from_be_bytes(k[32..].try_into().expect("8 bytes"));
        out.entry(ino).or_default().insert(hash);
    }
    Ok(out)
}

/// What the last ship plan held back (`Meta::held_summary`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeldSummary {
    pub transactions: u64,
    pub records: u64,
    /// The oldest held journal seq.
    pub oldest_seq: Option<u64>,
    /// Every poisoned inode: its unrecoverable chunks and how many held
    /// transactions are seeds for it.
    pub inodes: BTreeMap<Ino, HeldInode>,
    /// An uncaptured transaction was held, so everything after it is too.
    pub opaque: bool,
    /// Plan 30 §M7: transactions deferred (not held) because a chunk
    /// their manifest names is still uploading.
    pub deferred: u64,
    /// ADR-58 C3: the rids of the transactions that may be behind a lost
    /// chunk (`Meta::rid_behind_lost_chunk`): the *held* ones (a poisoned
    /// chunk exists nowhere), and the deferred ones that wait for another
    /// node's pending upload (its uploader may never return). A row that
    /// waits for this node's own upload ships by itself and is not here.
    /// Shared, so that cloning the summary (`Meta::held`) costs nothing per
    /// rid.
    pub rids: std::sync::Arc<BTreeSet<crate::rid::Rid>>,
    /// ADR-59: acknowledged operations `repair drop-held` rolled back for
    /// good on this node, ever (`DroppedHeld::acked_lost`).
    pub acked_lost_total: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeldInode {
    pub missing: Vec<ChunkHash>,
    pub seeds: u64,
}

/// What `repair drop-held` did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DroppedHeld {
    /// The inode the drop was for.
    pub ino: Ino,
    /// Seed transactions dropped into conflict copies.
    pub dropped: usize,
    /// Dependent transactions rolled back and queued for replay by rid.
    pub requeued: usize,
    /// Plan 46 M0.2: of `requeued`, those of another node whose answer
    /// was never given: queued as placeholders awaiting their requester's
    /// retry (`AwaitRetry`), not as copies of their op.
    pub await_retry: usize,
    /// Unrecoverable pending-upload rows removed.
    pub pending_removed: usize,
    /// Queued replays (from an earlier deposition) of this inode's
    /// manifest, turned into conflict copies as well (those the decision
    /// named: ADR-59 part B, D1).
    pub queued_dropped: usize,
    /// Queued replays of this inode's manifest naming a lost chunk that the
    /// drop left alone, warned of one by one: a shadow a delegate accepted
    /// (`gen` other than 0: the delegate's row lands, or the entry replays
    /// here once the generation ends), or an entry queued after the
    /// decision (no record names it). Each keeps the chunk's pending row and
    /// poison mark, so the row it lands as is held at once and a later drop
    /// names it.
    pub queued_kept: usize,
    /// Every rid the drop rolled back or refused: the seeds', the
    /// dependents' and the queued replays' (what the oracle treats as the
    /// operator's action, and what a requester's retry may now meet).
    pub rolled_back: Vec<crate::rid::Rid>,
    /// Of `rolled_back`, the rids queued as placeholders (`await_retry`).
    pub placeholders: Vec<crate::rid::Rid>,
    /// The rids the drop refused (`Refused` rows, conflict copies): the
    /// seeds' and the queued replays of the manifest (ADR-59 part B, D3: a
    /// parked answer of one is settled with the refusal).
    pub refused: Vec<crate::rid::Rid>,
    /// ADR-59: held transactions that depend on a seed but need none of the
    /// lost content (a rename, chmod or unlink of the damaged file, a create
    /// or unlink in its directory): redone in place with their journal rows,
    /// rids and answers, not rolled back.
    pub redone: usize,
    /// The rids of the `redone` rows that have one. A redone row told in
    /// doubt (parked Archive/Group answer, settle window ended) lands when
    /// the operator's drop releases it, as a queued replay lands when the
    /// drain runs it: the sim's late-landing check exempts it. A redone row
    /// without a rid (a placeholder-less remote row) is in `redone` only.
    pub redone_rids: Vec<crate::rid::Rid>,
    /// ADR-59: every dependent was rolled back (the conservative control).
    pub strand_all: bool,
    /// ADR-59: the rows the drop rolled back whose answer may have left (the
    /// I1 loss it cannot avoid: an acknowledged write of bytes that are gone,
    /// or an acknowledged op that needs them), with why. Listed, counted and
    /// warned of, never excused silently.
    pub acked_lost: Vec<(crate::rid::Rid, String)>,
    /// ADR-59 part B: the decision is journaled (`DropHeld` at this journal
    /// seq) and takes effect when the log carries it ([`Meta::drop_held_apply`]);
    /// `None` on the final result.
    pub pending: Option<u64>,
    /// The epoch of the tenure that journaled the `pending` record: its
    /// place in the log is `(pending_epoch, pending)`.
    pub pending_epoch: u64,
    /// ADR-59 part B: follow-up decisions an apply needed (a redone row did
    /// not apply as it did). Expected 0.
    pub follow_ups: usize,
    /// ADR-59 part B (D1): journal transactions the record names that the
    /// apply found still in the journal after the stranding (stranded by
    /// journal row). Expected 0 (`drop_apply_leftovers`).
    pub leftovers: usize,
    /// The apply ran on a node without the lease (a deposed dropper, or one
    /// restarted after its record shipped): no `Refused` row was journaled,
    /// and the refusals are the successor's gate's (`tail_retired`).
    pub deposed: bool,
    /// The log segment this replica had applied when the drop took effect
    /// (the final result only): what lands past it is the drop's replay.
    pub applied_seq: u64,
    /// The inode's unrecoverable chunks when the drop ran.
    pub missing: BTreeSet<ChunkHash>,
    /// Of `missing`, the chunks whose pending row and poison mark the drop
    /// kept because an op or a write session of this node still names them
    /// (the module doc's "Dropping").
    pub kept_pending: Vec<ChunkHash>,
    /// The ops in flight here that keep those chunks held.
    pub kept_for: Vec<crate::rid::Rid>,
    /// Rolled-back transactions that could not be named in the `RolledBack`
    /// record (no journal head): members keep their copies of them.
    pub unidentified: usize,
}

/// The identity a member keys a rolled-back transaction by
/// (`LogRecord::DropHeld`): its `TailOf` pair if it began with one, else
/// `(its tenure's epoch, first journal seq)`. `None` without the journal
/// head that says the epoch.
fn rolled_back_ident(
    head_epoch: Option<u64>,
    first: u64,
    records: &[LogRecord],
) -> Option<(u64, u64)> {
    let epoch = head_epoch?;
    Some(super::member::tail_identity(records).unwrap_or((epoch, first)))
}

/// The identity a member keys `tx` by (as `rolled_back_ident`, but a
/// re-shipped transaction has its `TailOf` pair whether or not it has a
/// `journal_tx` row), given the epoch of that row.
fn tx_ident(tx: &Tx, epoch: Option<u64>) -> Option<(u64, u64)> {
    let (_, first_rec) = tx.rows.first()?;
    super::member::tail_identity(std::slice::from_ref(first_rec))
        .or_else(|| epoch.map(|e| (e, tx.first)))
}

/// ADR-59 part B (D1, D2): the transactions the ship plan holds because of
/// what a drop says of them, whatever else keeps them: those the record of a
/// decided drop names (until its apply strands them) and those whose identity
/// the log retired. Read once per plan, and only when a drop is decided here
/// or something is retired (`Meta::any_retired`).
struct Named {
    /// Identity -> the inode of the decided drop that names it.
    named: HashMap<(u64, u64), Ino>,
    any_retired: bool,
}

impl Named {
    /// `None` when no drop is decided and nothing is retired: the common
    /// case pays two atomic/counter reads.
    fn read(r: &impl Readable, meta: &Meta) -> Result<Option<Named>, MetaError> {
        let any_retired = meta.any_retired.load(Ordering::Relaxed);
        let unshipped = counter_get(r, &meta.local, KV_DROP_HELD_UNSHIPPED)? > 0;
        if !any_retired && !unshipped {
            return Ok(None);
        }
        let mut named = HashMap::new();
        if unshipped {
            for guard in r.prefix(&meta.local, DROP_PLAN_PREFIX) {
                let (k, v) = guard.into_inner()?;
                let Ok(ino) =
                    <[u8; 8]>::try_from(&k[DROP_PLAN_PREFIX.len()..]).map(Ino::from_be_bytes)
                else {
                    continue;
                };
                let plan: DropPlan = postcard::from_bytes(&v)?;
                named.extend(plan.idents.iter().map(|i| (*i, ino)));
            }
        }
        Ok(Some(Named { named, any_retired }))
    }

    /// The inode to hold `ident` under: the drop that names it, else `0`
    /// for an identity the log retired (no inode of its own), else `None`.
    fn holds(
        &self,
        r: &impl Readable,
        meta: &Meta,
        ident: (u64, u64),
    ) -> Result<Option<Ino>, MetaError> {
        if let Some(ino) = self.named.get(&ident) {
            return Ok(Some(*ino));
        }
        if self.any_retired && meta.row_retired_tx(r, ident.0, ident.1)? {
            return Ok(Some(0));
        }
        Ok(None)
    }
}

/// The write sessions of one inode that are open on this node (the engine
/// knows them, the core does not): `repair drop-held` keeps the lost
/// chunks they name held (`Meta::drop_held_knowing`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveSessions {
    /// The chunks the attached sessions have sealed.
    pub hashes: BTreeSet<ChunkHash>,
    /// A session is detached: its flush is composing or committing a
    /// manifest the engine cannot read. Every lost chunk is kept.
    pub in_flight: bool,
}

impl LiveSessions {
    /// Whether a session may name `hash`.
    pub fn names(&self, hash: &ChunkHash) -> bool {
        self.in_flight || self.hashes.contains(hash)
    }
}

/// Whether `manifest` (the bytes of a `SetManifest`, `Publish` or
/// `WriteManifest`) names `hash`; a spilled or undecodable manifest is
/// taken to name every chunk, as the ship plan does.
pub fn manifest_names(manifest: &[u8], hash: &ChunkHash) -> bool {
    names_any(manifest, &BTreeSet::from([*hash]))
}

/// The manifests `op` would write into `ino`.
pub fn op_manifests_of(op: &MutateOp, ino: Ino) -> Vec<&[u8]> {
    match op {
        MutateOp::SetManifest {
            ino: m, manifest, ..
        }
        | MutateOp::Publish {
            ino: m, manifest, ..
        } if *m == ino => vec![manifest.as_slice()],
        MutateOp::Records { records } => records
            .iter()
            .filter_map(|r| match r {
                LogRecord::WriteManifest {
                    ino: m, manifest, ..
                } if *m == ino => Some(manifest.as_slice()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

impl DroppedHeld {
    /// The operator's one-line account of the drop (`repair drop-held`).
    pub fn describe(&self) -> String {
        let mut text = format!(
            "inode {}: {} held transaction(s) dropped into a conflict copy under \
             {}/ (lost chunks become holes), {} dependent transaction(s) rolled back and queued \
             for replay ({} of them another node's unanswered row, kept as a placeholder for that \
             node's retry), {} queued replay(s) dropped, {} unrecoverable pending upload(s) removed",
            self.ino,
            self.dropped,
            crate::CONFLICT_DIR,
            self.requeued,
            self.await_retry,
            self.queued_dropped,
            self.pending_removed
        );
        if let Some(jseq) = self.pending {
            text = format!(
                "inode {}: drop decided (journal seq {jseq}): {} held transaction(s) will become a                  conflict copy under {}/ and {} dependent transaction(s) roll back when the log                  carries the decision; {} other held transaction(s) did not need the lost content                  and are kept",
                self.ino,
                self.dropped,
                crate::CONFLICT_DIR,
                self.requeued,
                self.redone
            );
        } else if self.redone > 0 {
            text.push_str(&format!(
                "; {} other held transaction(s) did not need the lost content and were kept \
                 as they were",
                self.redone
            ));
        }
        if self.queued_kept > 0 {
            text.push_str(&format!(
                "; {} queued replay(s) of the file naming a lost chunk were left alone (another \
                 node's accepted write, or queued after the decision): the chunk stays held for \
                 a later drop",
                self.queued_kept
            ));
        }
        if self.deposed {
            text.push_str(
                "; applied on a node that no longer holds the lease: the record is in the log \
                 and this replica followed it, but no refusal was journaled here (the \
                 successor's gate answers the rids from the record)",
            );
        }
        if self.follow_ups > 0 {
            text.push_str(&format!(
                "; {} follow-up decision(s): a transaction kept as it was did not replay as it \
                 had run and was rolled back too",
                self.follow_ups
            ));
        }
        if !self.acked_lost.is_empty() {
            text.push_str(&format!(
                "; {} acknowledged operation(s) were lost with the data (acked_lost: {})",
                self.acked_lost.len(),
                self.acked_lost
                    .iter()
                    .map(|(rid, why)| format!("{rid:?} {why}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if self.unidentified > 0 {
            text.push_str(&format!(
                "; {} rolled-back transaction(s) could not be named to the members, which keep their copies",
                self.unidentified
            ));
        }
        if !self.kept_pending.is_empty() {
            let rids: Vec<String> = self.kept_for.iter().map(|r| format!("{r:?}")).collect();
            text.push_str(&format!(
                "; {} lost chunk(s) are still named by op(s) or write session(s) in flight here \
                 (rids: {}): kept held; run drop-held again once they have journaled",
                self.kept_pending.len(),
                if rids.is_empty() {
                    "none, an open write session".to_string()
                } else {
                    rids.join(", ")
                }
            ));
        }
        text
    }
}

/// One unshipped transaction as the planner sees it.
struct Tx {
    first: u64,
    last: u64,
    spec_seq: Option<u64>,
    rows: Vec<(u64, LogRecord)>,
}

/// The unshipped journal grouped into transactions, in journal order.
/// Rows without a `journal_tx` row are one-row, uncaptured transactions.
fn transactions(r: &impl Readable, meta: &Meta) -> Result<Vec<Tx>, MetaError> {
    transactions_between(r, meta, 0, u64::MAX)
}

/// [`transactions`] for those starting after journal seq `after`, through
/// `upto` (each read whole).
fn transactions_between(
    r: &impl Readable,
    meta: &Meta,
    after: u64,
    upto: u64,
) -> Result<Vec<Tx>, MetaError> {
    let from = journal::acked_watermark(r, &meta.local)?
        .max(after)
        .saturating_add(1);
    if from > upto {
        return Ok(Vec::new());
    }
    let mut heads: BTreeMap<u64, local::JournalTxHead> = BTreeMap::new();
    for guard in r.range(&meta.journal_tx, local::tx_key(from)..=local::tx_key(upto)) {
        let (k, v) = guard.into_inner()?;
        heads.insert(local::decode_tx_key(&k)?, postcard::from_bytes(&v)?);
    }
    let end = heads.values().map(|h| h.last).fold(upto, u64::max);
    let mut out: Vec<Tx> = Vec::new();
    for guard in r.range(
        &meta.journal_ks,
        journal::seq_key(from)..=journal::seq_key(end),
    ) {
        let (k, v) = guard.into_inner()?;
        let (seq, rec) = journal::decode_row(&k, &v)?;
        if let Some(open) = out.last_mut() {
            if seq <= open.last {
                open.rows.push((seq, rec));
                continue;
            }
        }
        if seq > upto {
            break;
        }
        let (last, spec_seq) = match heads.get(&seq) {
            Some(head) => (head.last.max(seq), head.spec_seq),
            None => (seq, None),
        };
        out.push(Tx {
            first: seq,
            last,
            spec_seq,
            rows: vec![(seq, rec)],
        });
    }
    Ok(out)
}

/// Does `manifest` (a `WriteManifest` record's bytes) name any of
/// `missing`? A spilled or undecodable manifest cannot be checked here and
/// is assumed to.
fn names_any(manifest: &[u8], missing: &BTreeSet<ChunkHash>) -> bool {
    match Manifest::decode(manifest) {
        Ok(m) => match m.chunks {
            ChunkInfo::Inline(chunks) => chunks.values().any(|h| missing.contains(h)),
            ChunkInfo::Spilled(_) => true,
        },
        Err(_) => true,
    }
}

/// The chunks of `missing` that `manifest` names (all of them when it cannot
/// tell: a spilled or undecodable manifest, as [`names_any`]).
fn named_missing(manifest: &[u8], missing: &BTreeSet<ChunkHash>) -> Vec<ChunkHash> {
    match Manifest::decode(manifest) {
        Ok(Manifest {
            chunks: ChunkInfo::Inline(chunks),
            ..
        }) => chunks
            .values()
            .filter(|h| missing.contains(h))
            .copied()
            .collect(),
        _ => missing.iter().copied().collect(),
    }
}

/// The non-refused replays of `ino`'s manifest queued here as the root's
/// own (`gen` 0) that name a chunk of `missing`: `(queue key, rid)`.
fn queued_namers(
    r: &impl Readable,
    meta: &Meta,
    ino: Ino,
    missing: &BTreeSet<ChunkHash>,
) -> Result<Vec<(u64, crate::rid::Rid)>, MetaError> {
    let mut out = Vec::new();
    for queued in spec::read_pending_replays(r, meta)? {
        if queued.refused.is_some() || queued.gen != 0 {
            continue;
        }
        let (m, manifest) = match &queued.op {
            MutateOp::SetManifest {
                ino: m, manifest, ..
            }
            | MutateOp::Publish {
                ino: m, manifest, ..
            } => (*m, manifest),
            _ => continue,
        };
        if m == ino && names_any(manifest, missing) {
            out.push((queued.queue_seq, queued.rid));
        }
    }
    Ok(out)
}

/// The poisoned inodes `tx` is a seed for.
fn seed_inos(tx: &Tx, poisoned: &PoisonMap) -> Vec<Ino> {
    seed_inos_of(tx.rows.iter().map(|(_, rec)| rec), poisoned)
}

/// The inodes of `poisoned` whose chunks `records`' manifests name.
pub(crate) fn seed_inos_of<'a>(
    records: impl IntoIterator<Item = &'a LogRecord>,
    poisoned: &PoisonMap,
) -> Vec<Ino> {
    let mut out = Vec::new();
    for rec in records {
        if let LogRecord::WriteManifest { ino, manifest, .. } = rec {
            if let Some(missing) = poisoned.get(ino) {
                if names_any(manifest, missing) && !out.contains(ino) {
                    out.push(*ino);
                }
            }
        }
    }
    out
}

/// ADR-59: how a transaction accesses a key. Two accesses conflict unless
/// both are commutative with every other: the one rule of the ship order
/// ([`conflicts`]).
///
/// - `Excl`: a write that does not commute: a dentry's existence or target,
///   an inode attribute set as is (mode, owner, size, manifest, a file's
///   own times, explicit utimes), removal, xattrs, a rename of the inode
///   itself, a directory's subtree change.
/// - `Merge`: a write made only by the commutative merge of a directory
///   (`mtime`/`ctime` = `max(.., t)`, `nlink += delta`): the directory's
///   inode record and the attribute copy its name carries, written because
///   a child came or went in it. A key is `Merge` for a transaction only if
///   every write of it was a merge.
/// - `Obs`: observed without a write: a refusal's `JournalTx::observed`.
///   (The keys a completion-only row, a no-op success, observed are
///   `Excl` as ever: [`tx_keys`].)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Excl,
    Merge,
    Obs,
}

/// Whether a later access `access` of a key depends on an earlier one,
/// `taint`, that is held or deferred (the table of ADR-59):
///
/// | access \ taint | Excl | Merge | Obs  |
/// |---|---|---|---|
/// | Excl  | dep  | dep  | dep  |
/// | Merge | dep  | none | none |
/// | Obs   | dep  | none | none |
///
/// `max` and `+` commute with each other, and an observation reads no
/// merged field (a directory's times and link count are read by no
/// journaled answer: permission checks read mode, owner and existence;
/// emptiness is a dentry scan), so only an exclusive access on either side
/// orders two transactions. This is the only place the rule is written:
/// [`Walk::step`], the drop's dependency walk and the property test's model
/// all call it.
fn conflicts(access: Mode, taint: Mode) -> bool {
    access == Mode::Excl || taint == Mode::Excl
}

/// A transaction's keys by access mode (see [`Mode`]); the sets are
/// disjoint, an exclusive access winning over a merge. (A refusal's
/// observation is no key of the transaction: [`Walk::step`] takes it apart.)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TxKeys {
    excl: Vec<Vec<u8>>,
    merge: Vec<Vec<u8>>,
}

impl TxKeys {
    /// Every key, sorted (what a placeholder holds: [`placeholder_keys`]).
    fn all(&self) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = self.excl.iter().chain(&self.merge).cloned().collect();
        out.sort();
        out.dedup();
        out
    }

    /// The keys it accesses with `mode`.
    #[cfg(test)]
    fn of(&self, mode: Mode) -> &[Vec<u8>] {
        match mode {
            Mode::Excl => &self.excl,
            Mode::Merge => &self.merge,
            Mode::Obs => &[],
        }
    }
}

/// The keys tainted by held (or deferred) transactions, by the strongest
/// way they were reached ([`conflicts`] needs only whether a taint is
/// exclusive).
#[derive(Clone, Debug, Default)]
struct Taint {
    excl: HashSet<Vec<u8>>,
    soft: HashSet<Vec<u8>>,
}

impl Taint {
    /// Whether the access `mode` of `key` conflicts with what reached it.
    fn conflicts(&self, mode: Mode, key: &[u8]) -> bool {
        if self.excl.contains(key) {
            conflicts(mode, Mode::Excl)
        } else if self.soft.contains(key) {
            // (A merge and an observation taint alike: `conflicts` does not
            // tell them apart.)
            conflicts(mode, Mode::Merge)
        } else {
            false
        }
    }

    /// Whether any key `keys` accesses conflicts with a taint. `observed`
    /// is a refusal's observation: an `Obs` access that taints nothing.
    fn hit(&self, keys: &TxKeys, observed: &[Vec<u8>]) -> bool {
        keys.excl.iter().any(|k| self.conflicts(Mode::Excl, k))
            || keys.merge.iter().any(|k| self.conflicts(Mode::Merge, k))
            || observed.iter().any(|k| self.conflicts(Mode::Obs, k))
    }

    fn add(&mut self, keys: &TxKeys) {
        self.excl.extend(keys.excl.iter().cloned());
        self.soft.extend(keys.merge.iter().cloned());
    }
}

/// A held-back transaction: the transaction, the poisoned inodes it is a
/// seed for (empty for a dependent), and its key set (`None`: uncaptured).
type Held = (Tx, Vec<Ino>, Option<TxKeys>);

/// A ship plan: the transactions to ship (in order), those held back
/// behind lost chunks, and those deferred behind chunks still uploading.
struct Plan {
    ship: Vec<Tx>,
    held: Vec<Held>,
    /// The rid of each held transaction, and of each one deferred behind
    /// another node's pending upload, that has one.
    held_rids: Vec<crate::rid::Rid>,
    deferred: Vec<Tx>,
    opaque: bool,
    /// With [`Blame`]: the blame of every held or deferred transaction
    /// planned (by its first journal seq; see [`Blame`]).
    blame: BTreeMap<u64, BTreeSet<Ino>>,
    /// With [`Blame::rid`]: that transaction's journal span, the plan
    /// having stopped right after it; `None` when the journal holds no
    /// unshipped transaction of that rid.
    found: Option<(u64, u64)>,
    /// With [`Blame`]: the union of every blame planned (through the
    /// found transaction, or the whole journal).
    blame_through: BTreeSet<Ino>,
}

/// Chunk close-stall-metered: what the plan should also say about one
/// node's remote-pending chunks (`store::remote`). The **blame** of a held
/// or deferred transaction is the set of inodes of `theirs` it waits for:
/// those its own manifests name, plus the blame of every earlier held or
/// deferred transaction it depends on — by a key it touches or observed
/// that that one touched, or, where one of the two is uncaptured (its keys
/// unknown), unconditionally. It is computed by the same pass that decides
/// what is held and deferred, so the two cannot drift apart: a change to
/// the rule changes the blame with it (`deferral_blame`).
#[derive(Clone, Copy)]
struct Blame<'a> {
    theirs: &'a PoisonMap,
    /// Stop after this rid's transaction (the rest cannot change its
    /// blame: only earlier transactions are depended on).
    rid: Option<crate::rid::Rid>,
}

/// The ship rule's state after a prefix of the unshipped journal, in
/// journal order ([`plan`]); [`Walk::step`] classifies the next
/// transaction. Chunk metered-own-rows: kept between calls by the blame
/// cache ([`BlameCache`]), which resumes it where the journal grew.
#[derive(Clone, Default)]
struct Walk {
    tainted: Taint,
    deferred_keys: Taint,
    opaque: bool,
    opaque_deferred: bool,
    any_held: bool,
    any_deferred: bool,
    // ADR-55 (M3): the delegation streams (`gen` != 0) with a held or a
    // deferred transaction. A stream's index order is an exclusive key: its
    // later transactions are held (deferred) with it, whatever keys they
    // touch, so that no index ships ahead of a lower one that does not (a
    // hole in the log's stream is skipped for good by the successor's cursor).
    held_gens: BTreeSet<u64>,
    deferred_gens: BTreeSet<u64>,
    // The blame of each key a held or deferred transaction touched, split
    // like the taint (`Taint`): what an exclusive access of the key and a
    // merging or observing one inherit; that of every held or deferred
    // uncaptured transaction (which every later one depends on); that of
    // all of them (which an uncaptured one depends on).
    blame_excl: HashMap<Vec<u8>, BTreeSet<Ino>>,
    blame_soft: HashMap<Vec<u8>, BTreeSet<Ino>>,
    opaque_blame: BTreeSet<Ino>,
    all_blame: BTreeSet<Ino>,
}

/// What [`Walk::step`] made of a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Ship,
    Held,
    Deferred,
}

impl Walk {
    /// See the module doc's "What is held" and "Deferred". A transaction
    /// is *held* if it is a poisoned seed or depends on a held one;
    /// otherwise *deferred* if it is a pending seed or depends on anything
    /// held or deferred; otherwise it ships. Taint accumulates per kind,
    /// so a transaction behind both is reported as held. With `theirs`,
    /// also a held or deferred transaction's blame ([`Blame`]). `(gen,
    /// named)`: the transaction's delegation stream (`0`: none), and the
    /// inode of the decided drop that names it, or `0` if the log retired
    /// its identity (see [`Named`]). Returns its class, the poisoned inodes it is a seed for, and its blame.
    ///
    /// "Depends" is [`conflicts`] on the keys the two access, not the
    /// bare overlap of their key sets (ADR-59).
    fn step(
        &mut self,
        tx: &Tx,
        keys: Option<&TxKeys>,
        observed: &[Vec<u8>],
        (poisoned, pending): (&PoisonMap, &PoisonMap),
        theirs: Option<&PoisonMap>,
        (gen, named): (u64, Option<Ino>),
    ) -> (Class, Vec<Ino>, Option<BTreeSet<Ino>>) {
        let mut seeds = seed_inos(tx, poisoned);
        // ADR-59 part B (D1, D2): a transaction a decided drop names, or
        // whose identity the log retired, is held whatever else holds it (a
        // clean upload pass clears the marks that held it; the apply strands
        // it). Its dependents follow by the taint rule. The inode is the
        // drop's (`0`: the identity is retired, no drop of this node's).
        if let Some(ino) = named {
            if !seeds.contains(&ino) {
                seeds.push(ino);
            }
        }
        // What the transaction observed without writing (a refusal's op:
        // `JournalTx::observed`): a dependency like an observation of a
        // written key, but it taints nothing after it. A refusal shipped
        // ahead of the deferred transaction it was refused because of put a
        // refusal in the log that the log's own prefix could not explain
        // (flex-crash seed 481).
        let held = self.opaque
            || !seeds.is_empty()
            || (gen != 0 && self.held_gens.contains(&gen))
            || match keys {
                Some(keys) => self.tainted.hit(keys, observed),
                // Unknown keys: held once anything is.
                None => self.any_held,
            };
        let deferred = !held
            && (self.opaque_deferred
                || (gen != 0 && self.deferred_gens.contains(&gen))
                || !seed_inos(tx, pending).is_empty()
                || match keys {
                    Some(keys) => self.deferred_keys.hit(keys, observed),
                    None => self.any_deferred,
                });
        let mut blame = None;
        if let (Some(theirs), true) = (theirs, held || deferred) {
            let mut mine: BTreeSet<Ino> = seed_inos(tx, theirs).into_iter().collect();
            match keys {
                Some(keys) => {
                    // The blame flows only across a conflicting pair
                    // (`conflicts`): an access inherits that of the
                    // exclusive taints of its key, and, if it conflicts
                    // with a merge, that of the merging ones.
                    let accesses = keys
                        .excl
                        .iter()
                        .map(|k| (Mode::Excl, k))
                        .chain(keys.merge.iter().map(|k| (Mode::Merge, k)))
                        .chain(observed.iter().map(|k| (Mode::Obs, k)));
                    for (mode, k) in accesses {
                        if conflicts(mode, Mode::Excl) {
                            mine.extend(self.blame_excl.get(k).into_iter().flatten());
                        }
                        if conflicts(mode, Mode::Merge) {
                            mine.extend(self.blame_soft.get(k).into_iter().flatten());
                        }
                    }
                    mine.extend(&self.opaque_blame);
                    for k in &keys.excl {
                        self.blame_excl.entry(k.clone()).or_default().extend(&mine);
                    }
                    for k in &keys.merge {
                        self.blame_soft.entry(k.clone()).or_default().extend(&mine);
                    }
                }
                None => {
                    mine.extend(&self.all_blame);
                    self.opaque_blame.extend(&mine);
                }
            }
            self.all_blame.extend(&mine);
            blame = Some(mine);
        }
        let class = if held {
            match keys {
                Some(keys) => self.tainted.add(keys),
                None => self.opaque = true,
            }
            self.any_held = true;
            if gen != 0 {
                self.held_gens.insert(gen);
            }
            Class::Held
        } else if deferred {
            match keys {
                Some(keys) => self.deferred_keys.add(keys),
                None => self.opaque_deferred = true,
            }
            self.any_deferred = true;
            if gen != 0 {
                self.deferred_gens.insert(gen);
            }
            Class::Deferred
        } else {
            Class::Ship
        };
        (class, seeds, blame)
    }
}

/// A transaction's key set (`None`: uncaptured) and its `journal_tx` row.
type KeysAndRow = (Option<TxKeys>, Option<local::JournalTx>);

/// [`KeysAndRow`] of `tx`.
fn tx_keys_and_row(r: &impl Readable, meta: &Meta, tx: &Tx) -> Result<KeysAndRow, MetaError> {
    let captured = match tx.spec_seq {
        Some(seq) => spec::row_keys_and_origin(r, meta, seq)?.map(|row| (row.keys, row.merge)),
        None => None,
    };
    let row = local::get_journal_tx(r, meta, tx.first)?;
    let mut keys = tx_keys(tx, captured, row.as_ref());
    if meta.parent_taint_conservative.load(Ordering::Relaxed) {
        if let Some(keys) = keys.as_mut() {
            keys.excl.append(&mut keys.merge);
            keys.excl.sort();
            keys.excl.dedup();
        }
    }
    Ok((keys, row))
}

/// The keys a placeholder of `tx` holds (`AwaitRetry::keys`): what the
/// planner held it on (`keys`: what the transaction wrote, what its rows
/// name, what a completion alone observed — [`tx_keys`]) as dentries and
/// inodes, not only what its records name ([`TouchSet::from_records`]): the
/// inode an unlink takes away or a rename moves is written without being
/// named, and a no-op completion (a rename onto another name of the same
/// inode) names nothing at all. A directory the transaction names as a
/// parent stays *shared* (its attributes change by a commutative merge); any
/// other inode it wrote is exclusive.
fn placeholder_keys(tx: &Tx, keys: &TxKeys) -> crate::replay::TouchSet {
    use constellation_mtree::keys::Key;
    let mut out = crate::replay::TouchSet::from_records(tx.rows.iter().map(|(_, rec)| rec));
    for key in &keys.all() {
        match Key::parse(key) {
            Ok(Key::Inode { ino }) if !out.shared.contains(&ino) => {
                out.inos.insert(ino);
            }
            Ok(Key::Dentry { parent_ino, name }) => {
                out.dentries
                    .insert((parent_ino, String::from_utf8_lossy(name).into_owned()));
            }
            _ => {}
        }
    }
    out
}

/// What the capture says a transaction wrote: its keys, and the sorted
/// subset of them it wrote only by a directory's merge.
type WrittenKeys = (Vec<Vec<u8>>, Vec<Vec<u8>>);

/// A captured transaction's keys for the ship rule, by access mode
/// ([`Mode`]): what it wrote (the keys it wrote only by a directory's merge
/// are `Merge`, every other one `Excl`), plus what its rows name, plus for
/// a completion alone what it observed. `captured`: its before-image keys
/// and the sorted subset of them it wrote only by merge.
///
/// Plan 45 follow-up, the rows: a record that applied as a no-op writes
/// nothing (a replayed `unlink f1` of a stranded op, after `rename f1 f0`
/// had taken the name), yet the log replays it by name wherever it ships.
/// Shipped ahead of the deferred rename (the authority sim's
/// `metered-shared+links` seed 449), every replica unlinked `f1` and
/// skipped the rename, while the holder kept the renamed file. So the
/// names its records carry are `Excl`, written or not; the directory a
/// record names as a parent (`TouchSet::shared`) is `Merge`, unless the
/// transaction wrote it exclusively as well.
///
/// The observed keys: an op that executed and changed nothing (a rename
/// onto another name of the same inode: `writes::complete_noop_tx`) is a
/// `Completed` row and the keys it observed. Its `Ok` is a success the log
/// orders like any other, so it is ordered as a write of what it observed:
/// deferred behind what wrote it, and every later transaction of those
/// keys behind it, a create in the directory it named included (`flex+links`
/// seeds 191 and 335: a create invoked after the rename returned shipped
/// ahead of its completion). These keys stay exclusive, as ever: the spec's
/// weaker taint for them would let that create ship ahead again. (A
/// refusal's observed keys taint nothing: it is no success, and its
/// observation of a directory's merged fields orders nothing.)
fn tx_keys(
    tx: &Tx,
    captured: Option<WrittenKeys>,
    row: Option<&local::JournalTx>,
) -> Option<TxKeys> {
    let (written, merged) = captured?;
    let merged: HashSet<Vec<u8>> = merged.into_iter().collect();
    let mut excl: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut merge: BTreeSet<Vec<u8>> = BTreeSet::new();
    for key in written {
        if merged.contains(&key) {
            merge.insert(key);
        } else {
            excl.insert(key);
        }
    }
    let named = crate::replay::TouchSet::from_records(tx.rows.iter().map(|(_, rec)| rec));
    excl.extend(
        named
            .dentries
            .iter()
            .map(|(parent, name)| constellation_mtree::keys::dentry(*parent, name.as_bytes())),
    );
    excl.extend(
        named
            .inos
            .iter()
            .map(|ino| constellation_mtree::keys::inode(*ino)),
    );
    merge.extend(
        named
            .shared
            .iter()
            .map(|ino| constellation_mtree::keys::inode(*ino)),
    );
    merge.retain(|k| !excl.contains(k));
    let completion_only = tx
        .rows
        .iter()
        .any(|(_, rec)| matches!(rec, LogRecord::Completed { .. }))
        && tx.rows.iter().all(|(_, rec)| {
            matches!(
                rec,
                LogRecord::Completed { .. } | LogRecord::InboxAck { .. }
            )
        });
    if let (true, Some(row)) = (completion_only, row) {
        excl.extend(row.observed.iter().cloned());
        merge.retain(|k| !excl.contains(k));
    }
    Some(TxKeys {
        excl: excl.into_iter().collect(),
        merge: merge.into_iter().collect(),
    })
}

/// The ship rule over the whole unshipped journal ([`Walk::step`]). With
/// `blame`, also each held or deferred transaction's blame ([`Blame`]).
fn plan(
    r: &impl Readable,
    meta: &Meta,
    poisoned: &PoisonMap,
    pending: &PoisonMap,
    blame: Option<Blame<'_>>,
) -> Result<Plan, MetaError> {
    let mut walk = Walk::default();
    let named = Named::read(r, meta)?;
    let mut out = Plan {
        ship: Vec::new(),
        held: Vec::new(),
        held_rids: Vec::new(),
        deferred: Vec::new(),
        opaque: false,
        blame: BTreeMap::new(),
        found: None,
        blame_through: BTreeSet::new(),
    };
    for tx in transactions(r, meta)? {
        let (keys, row) = tx_keys_and_row(r, meta, &tx)?;
        let found = blame
            .and_then(|b| b.rid)
            .is_some_and(|rid| tx_rid(&tx, row.as_ref()) == Some(rid));
        let rid = tx_rid(&tx, row.as_ref());
        let gen = row.as_ref().map_or(0, |row| row.gen);
        let hold = match (&named, tx_ident(&tx, row.as_ref().map(|t| t.epoch))) {
            (Some(n), Some(ident)) => n.holds(r, meta, ident)?,
            _ => None,
        };
        let observed: Vec<Vec<u8>> = row.map(|row| row.observed).unwrap_or_default();
        let (class, seeds, mine) = walk.step(
            &tx,
            keys.as_ref(),
            &observed,
            (poisoned, pending),
            blame.map(|b| b.theirs),
            (gen, hold),
        );
        if let Some(mine) = mine {
            out.blame.insert(tx.first, mine);
        }
        if found {
            out.found = Some((tx.first, tx.last));
        }
        match class {
            Class::Held => {
                out.held_rids.extend(rid);
                out.held.push((tx, seeds, keys))
            }
            Class::Deferred => {
                // (Deferred for another node's pending upload: the blame on
                // the remote-pending rows is not empty. One that waits for
                // this node's own upload ships by itself and is not counted.)
                if out.blame.get(&tx.first).is_some_and(|b| !b.is_empty()) {
                    out.held_rids.extend(rid);
                }
                out.deferred.push(tx)
            }
            Class::Ship => out.ship.push(tx),
        }
        if found {
            break;
        }
    }
    out.opaque = walk.opaque;
    out.blame_through = walk.all_blame;
    Ok(out)
}

/// [`deferral_blame`]'s answer.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct DeferralBlame {
    /// The rid's unshipped transaction: its last journal seq and its
    /// blame (empty when it ships as it is). `None`: no unshipped
    /// transaction of the rid (shipped, or never journaled here).
    pub(crate) tx: Option<(u64, BTreeSet<Ino>)>,
    /// The blame of every held or deferred transaction through the rid's
    /// (the whole unshipped journal when it has none): what a node that
    /// observed this journal's position, right after the rid's
    /// transaction, waits for before its log reaches that position.
    pub(crate) through: BTreeSet<Ino>,
}

/// Chunk close-stall-metered: whether the ship plan holds or defers
/// `rid`'s unshipped transaction, and if so its blame on `theirs` — one
/// node's remote-pending chunks (`store::remote`): the inodes whose
/// upload it waits for ([`Blame`]), and that of everything through it
/// ([`DeferralBlame`]). A thin wrapper over [`plan`], so the answer
/// follows the ship rule exactly. Chunk metered-own-rows: the holder asks
/// [`blame_walk`] (the same rule, resumed); this is the property test's
/// statement of it.
#[cfg(test)]
pub(crate) fn deferral_blame(
    r: &impl Readable,
    meta: &Meta,
    rid: crate::rid::Rid,
    theirs: &PoisonMap,
) -> Result<DeferralBlame, MetaError> {
    let poisoned = read_poisoned(r, meta)?;
    let pending = read_pending(r, meta)?;
    let mut plan = plan(
        r,
        meta,
        &poisoned,
        &pending,
        Some(Blame {
            theirs,
            rid: Some(rid),
        }),
    )?;
    Ok(DeferralBlame {
        tx: plan
            .found
            .map(|(first, last)| (last, plan.blame.remove(&first).unwrap_or_default())),
        through: plan.blame_through,
    })
}

/// Chunk metered-own-rows: one unshipped transaction as
/// [`blame_walk`] reports it about one node: every held or deferred one,
/// and every one of that node's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Kept {
    pub(crate) first: u64,
    pub(crate) last: u64,
    spec_seq: Option<u64>,
    pub(crate) rid: Option<crate::rid::Rid>,
    /// Whether its rows change anything a read sees: a refusal's
    /// `Refused`, a `Completed` and an inbox acknowledgement only feed the
    /// dedup.
    pub(crate) effect: bool,
    /// Its blame ([`Blame`]) when it is held or deferred; `None` when it
    /// ships.
    pub(crate) waits: Option<BTreeSet<Ino>>,
}

impl Kept {
    /// Whether the journal still holds it as it was walked: its
    /// `journal_tx` row with the same capture (a rewind re-captures a
    /// rolled-back transaction under a fresh `spec_seq`), or, uncaptured,
    /// its row.
    fn still_there(&self, r: &impl Readable, meta: &Meta) -> Result<bool, MetaError> {
        Ok(match local::get_journal_tx_head(r, meta, self.first)? {
            Some(head) => head.spec_seq == self.spec_seq && head.last == self.last,
            None => {
                self.spec_seq.is_none()
                    && r.get(&meta.journal_ks, journal::seq_key(self.first))?
                        .is_some()
            }
        })
    }
}

/// Chunk metered-own-rows (the follow-up review's refusal cost): the
/// ship rule's walk for one node's blame, kept between the holder's
/// answers to that node and resumed where the journal grew. A refusal of
/// a node with chunks pending here walked the whole unshipped journal
/// inside the core step (24.8 ms at 10,000 transactions), and the walk
/// cannot start later: what the first deferred transaction taints decides
/// every one after it.
///
/// Valid while nothing it was computed from changed:
/// - the pending, poisoned and node's remote rows are the same (an ack,
///   an enrolment, a poisoning, a holder's own write-back: re-walked);
/// - every held or deferred transaction it walked is still in the journal,
///   captured the same way (a ship of one, a deposition's strand, a
///   rewind's re-capture, a `drop-held`: re-walked). A transaction that
///   ships taints nothing and blames nothing, so one leaving changes
///   nothing for those after it; only the node's own are dropped from the
///   list then;
/// - journal seqs only grow, so nothing appears before what was walked.
pub(crate) struct BlameCache {
    poisoned: PoisonMap,
    pending: PoisonMap,
    theirs: PoisonMap,
    walk: Walk,
    /// The last journal seq walked.
    through: u64,
    kept: Vec<Kept>,
}

/// Whether `rec` changes anything a read sees (see [`Kept::effect`]).
fn has_effect(rec: &LogRecord) -> bool {
    !matches!(
        rec,
        LogRecord::Completed { .. } | LogRecord::Refused { .. } | LogRecord::InboxAck { .. }
    )
}

/// The rid a transaction ran under: its `journal_tx` row's, or, for a
/// journaled refusal (which records none there), its `Refused` row's.
fn tx_rid(tx: &Tx, row: Option<&local::JournalTx>) -> Option<crate::rid::Rid> {
    row.and_then(|t| t.rid).or_else(|| {
        tx.rows.iter().find_map(|(_, rec)| match rec {
            LogRecord::Refused { rid, .. } => Some(*rid),
            _ => None,
        })
    })
}

/// Chunk metered-own-rows: [`Kept`] for every unshipped transaction
/// through journal seq `upto` that is held, deferred or `node`'s, with
/// its blame on `theirs` (`node`'s live remote rows). Exactly what
/// [`plan`] with [`Blame`] says, from the node's [`BlameCache`] when it
/// is still valid.
pub(crate) fn blame_walk(
    r: &impl Readable,
    meta: &Meta,
    node: u64,
    theirs: &PoisonMap,
    upto: u64,
) -> Result<Vec<Kept>, MetaError> {
    let poisoned = read_poisoned(r, meta)?;
    let pending = read_pending(r, meta)?;
    let mut caches = meta.blame_cache.lock().unwrap();
    let mut cache = match caches.remove(&node) {
        Some(c) if c.poisoned == poisoned && c.pending == pending && &c.theirs == theirs => {
            let mut valid = true;
            let mut kept = Vec::with_capacity(c.kept.len());
            for k in c.kept {
                if k.still_there(r, meta)? {
                    kept.push(k);
                } else if k.waits.is_some() {
                    valid = false;
                    break;
                }
            }
            valid.then_some(BlameCache { kept, ..c })
        }
        _ => None,
    };
    let mut c = match cache.take() {
        Some(c) => c,
        None => BlameCache {
            poisoned,
            pending,
            theirs: theirs.clone(),
            walk: Walk::default(),
            through: 0,
            kept: Vec::new(),
        },
    };
    for tx in transactions_between(r, meta, c.through, upto)? {
        let (keys, row) = tx_keys_and_row(r, meta, &tx)?;
        let rid = tx_rid(&tx, row.as_ref());
        let gen = row.as_ref().map_or(0, |row| row.gen);
        let observed = row.map(|row| row.observed).unwrap_or_default();
        let (class, _, waits) = c.walk.step(
            &tx,
            keys.as_ref(),
            &observed,
            (&c.poisoned, &c.pending),
            Some(&c.theirs),
            // (The metered blame is of chunks another node's upload waits
            // for; a named or retired transaction is held by the plan, which
            // this walk does not model.)
            (gen, None),
        );
        c.through = c.through.max(tx.last);
        if class != Class::Ship || rid.is_some_and(|rid| rid.node == node) {
            c.kept.push(Kept {
                first: tx.first,
                last: tx.last,
                spec_seq: tx.spec_seq,
                rid,
                effect: tx.rows.iter().any(|(_, rec)| has_effect(rec)),
                waits,
            });
        }
    }
    let out = c.kept.iter().filter(|k| k.first <= upto).cloned().collect();
    caches.insert(node, c);
    Ok(out)
}

fn summarize(plan: &Plan, poisoned: &PoisonMap) -> HeldSummary {
    let mut inodes: BTreeMap<Ino, HeldInode> = poisoned
        .iter()
        .map(|(ino, missing)| {
            (
                *ino,
                HeldInode {
                    missing: missing.iter().copied().collect(),
                    seeds: 0,
                },
            )
        })
        .collect();
    for (_, seeds, _) in &plan.held {
        for ino in seeds {
            if let Some(entry) = inodes.get_mut(ino) {
                entry.seeds += 1;
            }
        }
    }
    HeldSummary {
        transactions: plan.held.len() as u64,
        records: plan.held.iter().map(|(t, _, _)| t.rows.len() as u64).sum(),
        oldest_seq: plan.held.first().map(|(t, _, _)| t.first),
        inodes,
        opaque: plan.opaque,
        deferred: plan.deferred.len() as u64,
        acked_lost_total: 0,
        rids: std::sync::Arc::new(plan.held_rids.iter().copied().collect()),
    }
}

/// `manifest` with every chunk in `missing` turned into a hole: what a
/// dropped manifest's conflict copy keeps. A spilled manifest's chunk list
/// is not readable here, so its copy keeps only the length (all holes).
fn sanitize_manifest(manifest: &[u8], missing: &BTreeSet<ChunkHash>) -> Option<Vec<u8>> {
    let m = Manifest::decode(manifest).ok()?;
    let chunks = match m.chunks {
        ChunkInfo::Inline(chunks) => chunks
            .into_iter()
            .filter(|(_, h)| !missing.contains(h))
            .collect(),
        ChunkInfo::Spilled(_) => Default::default(),
    };
    Some(
        Manifest {
            layout: m.layout,
            file_len: m.file_len,
            chunks: ChunkInfo::Inline(chunks),
        }
        .encode(),
    )
}

impl Meta {
    /// Record the pending uploads whose chunks the upload pass found
    /// missing from the local cache. With `complete` (a full pass over
    /// every pending row) the set is replaced; otherwise (a pass limited
    /// to one inode) the pairs are added.
    pub fn note_unrecoverable_chunks(
        &self,
        missing: &[(ChunkHash, Ino)],
        complete: bool,
    ) -> Result<(), MetaError> {
        // Plan 30 §M4 round 2: the common case — nothing missing — costs
        // nothing on a limited pass (a write-through close's inode drain)
        // and one counter read on a full pass.
        if missing.is_empty() {
            if !complete {
                return Ok(());
            }
            let r = self.db.read_tx();
            if counter_get(&r, &self.local, KV_POISONED_COUNT)? == 0 {
                return Ok(());
            }
        }
        let r = self.db.read_tx();
        let existing: BTreeSet<Vec<u8>> = r
            .prefix(&self.local, POISON_PREFIX)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        drop(r);
        let wanted: BTreeSet<Vec<u8>> = missing.iter().map(|(h, i)| poison_key(h, *i)).collect();
        let stale: Vec<&Vec<u8>> = if complete {
            existing.difference(&wanted).collect()
        } else {
            Vec::new()
        };
        let fresh: Vec<&Vec<u8>> = wanted.difference(&existing).collect();
        if stale.is_empty() && fresh.is_empty() {
            return Ok(());
        }
        let mut tx = self.db.write_tx();
        let marks = existing.len() - stale.len() + fresh.len();
        for key in stale {
            tx.remove(&self.local, key.clone());
        }
        for key in &fresh {
            tx.insert(&self.local, (*key).clone(), Vec::new());
        }
        counter_set_tx(&mut tx, &self.local, KV_POISONED_COUNT, marks as u64);
        tx.commit()?;
        if !fresh.is_empty() {
            tracing::error!(
                newly_unrecoverable = fresh.len(),
                "pending upload chunk(s) missing from the local cache: the records that need \
                 them are held back (see `status`'s `held`; `constellation repair drop-held \
                 <ino>` discards them into a conflict copy); everything else keeps shipping"
            );
        }
        Ok(())
    }

    /// The first journal seq of every unshipped transaction the ship plan
    /// holds or defers now (planned afresh, not the last round's view): the
    /// rows a later transaction may ship ahead of. Empty, at the cost of two
    /// reads, when no chunk is poisoned or pending: then every unshipped
    /// transaction ships in journal order.
    pub fn journal_held_back(&self) -> Result<BTreeSet<u64>, MetaError> {
        let r = self.db.read_tx();
        let poisoned = read_poisoned(&r, self)?;
        let pending = read_pending(&r, self)?;
        if poisoned.is_empty() && pending.is_empty() {
            return Ok(BTreeSet::new());
        }
        let plan = plan(&r, self, &poisoned, &pending, None)?;
        Ok(plan
            .held
            .iter()
            .map(|(tx, ..)| tx.first)
            .chain(plan.deferred.iter().map(|tx| tx.first))
            .collect())
    }

    /// The unrecoverable pending uploads currently recorded.
    pub fn unrecoverable_chunks(&self) -> Result<Vec<(ChunkHash, Ino)>, MetaError> {
        let r = self.db.read_tx();
        Ok(read_poisoned(&r, self)?
            .into_iter()
            .flat_map(|(ino, hashes)| hashes.into_iter().map(move |h| (h, ino)))
            .collect())
    }

    /// Up to `max` journal rows to ship, from the head, whole transactions
    /// only (see `take_journal_whole_txs`) — skipping, when a chunk is
    /// unrecoverable, every held-back transaction (see the module doc).
    /// Refreshes [`Meta::held_summary`].
    pub(crate) fn take_shippable(&self, max: usize) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let r = self.db.read_tx();
        let poisoned = read_poisoned(&r, self)?;
        let pending = read_pending(&r, self)?;
        // ADR-59 part B (D1, D2): the plan also holds what a decided drop
        // names and what the log retired. Nothing of either (the common
        // case: two reads) leaves the fast path below as it was.
        let named = Named::read(&r, self)?;
        let r = if poisoned.is_empty() && pending.is_empty() {
            drop(r);
            let whole = self.take_journal_whole_txs(max)?;
            // With a drop decided or an identity retired the batch is
            // checked for a transaction either holds (one `journal_tx`
            // range read and a lookup per transaction of it); only then
            // does the plan run.
            let held = match &named {
                Some(named) => self.batch_names_a_drop(named, &whole)?,
                None => false,
            };
            if !held {
                if self.held_any.swap(false, Ordering::Relaxed) {
                    *self.held.lock().unwrap() = HeldSummary::default();
                }
                return Ok(whole);
            }
            self.db.read_tx()
        } else {
            r
        };
        self.held_work.fetch_add(1, Ordering::Relaxed);
        // ADR-58 C3: what is deferred for another node's pending upload
        // (its uploader may never return) is told apart from what waits for
        // this node's own upload, by the blame on the remote-pending rows.
        let remote = if pending.is_empty() {
            PoisonMap::new()
        } else {
            super::remote::remote_pending_all_tx(&r, self)?
        };
        let blame = (!remote.is_empty()).then_some(Blame {
            theirs: &remote,
            rid: None,
        });
        let plan = plan(&r, self, &poisoned, &pending, blame)?;
        // Whether rows may be skipped (the session counts a read that
        // timed out meanwhile as `degraded_held`).
        self.held_any.store(
            !poisoned.is_empty() || !plan.held.is_empty() || !plan.deferred.is_empty(),
            Ordering::Relaxed,
        );
        *self.held.lock().unwrap() = summarize(&plan, &poisoned);
        // ADR-59 part B (D2): what a `DropHeld` retired never ships. The plan
        // holds such a transaction ([`Named`]); this pins it.
        if cfg!(debug_assertions) && self.any_retired.load(Ordering::Relaxed) {
            for tx in &plan.ship {
                let Some((first, rec)) = tx.rows.first() else {
                    continue;
                };
                let head_epoch =
                    super::local::get_journal_tx_head(&r, self, *first)?.map(|h| h.epoch);
                if let Some((e, j)) =
                    rolled_back_ident(head_epoch, *first, std::slice::from_ref(rec))
                {
                    debug_assert!(
                        !self.row_retired(e, j),
                        "the ship plan carries the transaction ({e}, {j}), which a DropHeld retired"
                    );
                }
            }
        }
        let mut batch = Vec::new();
        for tx in plan.ship {
            if !batch.is_empty() && batch.len() >= max {
                break;
            }
            batch.extend(tx.rows);
        }
        Ok(batch)
    }

    /// Whether a transaction of `batch` (whole transactions from the head of
    /// the journal) is named by a decided drop or has a retired identity.
    fn batch_names_a_drop(
        &self,
        named: &Named,
        batch: &[(u64, LogRecord)],
    ) -> Result<bool, MetaError> {
        if !named.named.is_empty() {
            return Ok(true);
        }
        let (Some(&(from, _)), Some(&(to, _))) = (batch.first(), batch.last()) else {
            return Ok(false);
        };
        let r = self.db.read_tx();
        for guard in r.range(&self.journal_tx, local::tx_key(from)..=local::tx_key(to)) {
            let (k, v) = guard.into_inner()?;
            let first = local::decode_tx_key(&k)?;
            let head: local::JournalTxHead = postcard::from_bytes(&v)?;
            let Ok(at) = batch.binary_search_by_key(&first, |(seq, _)| *seq) else {
                continue;
            };
            let ident = super::member::tail_identity(std::slice::from_ref(&batch[at].1))
                .unwrap_or((head.epoch, first));
            if named.holds(&r, self, ident)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The journal rows making up `records`, a segment this node shipped
    /// but never acked (found again by a tail, typically after a restart):
    /// before plan 30 §M4 always the journal's head; with held-back
    /// transactions skipped, a subsequence of whole transactions, in
    /// order. `None` if `records` is not one.
    pub fn match_own_segment(&self, records: &[&LogRecord]) -> Result<Option<Vec<u64>>, MetaError> {
        let r = self.db.read_tx();
        let mut seqs = Vec::new();
        let mut at = 0;
        for tx in transactions(&r, self)? {
            if at == records.len() {
                break;
            }
            let n = tx.rows.len();
            if at + n <= records.len()
                && tx
                    .rows
                    .iter()
                    .zip(&records[at..at + n])
                    .all(|((_, mine), theirs)| mine == *theirs)
            {
                seqs.extend(tx.rows.iter().map(|(seq, _)| *seq));
                at += n;
            }
        }
        Ok((at == records.len()).then_some(seqs))
    }

    /// What the last ship round held back (empty when nothing is
    /// poisoned). In memory, refreshed by every ship plan.
    /// ADR-59's negative control: classify a directory's merged attributes
    /// as exclusive again, and roll back every transaction that depends on
    /// a seed (`parent_taint_conservative`). Only the
    /// simulation and the tests that pin the replay queue's machinery use it.
    pub fn set_parent_taint_conservative(&self, on: bool) {
        self.parent_taint_conservative.store(on, Ordering::Relaxed);
    }

    /// ADR-59: shipped rows the publisher's view could not place (expected
    /// 0: the planner never lets one past an outstanding row it conflicts
    /// with).
    pub fn publish_unplaced(&self) -> u64 {
        self.publish_unplaced.load(Ordering::Relaxed)
    }

    pub fn held_summary(&self) -> HeldSummary {
        HeldSummary {
            acked_lost_total: self.acked_lost.load(Ordering::Relaxed),
            ..self.held.lock().unwrap().clone()
        }
    }

    /// ADR-58 C3: whether the last ship plan *holds* `rid`'s transaction
    /// (its row is a poisoned seed or depends on one: the chunk exists
    /// nowhere), or defers it behind another node's pending upload (the
    /// uploader may never return). A row that waits for this node's own
    /// upload ships by itself and is not behind a lost chunk. The sim's
    /// oracle checks the claim against its chunk ledger. As fresh as the
    /// last ship round.
    pub fn rid_behind_lost_chunk(&self, rid: crate::rid::Rid) -> bool {
        self.held.lock().unwrap().rids.contains(&rid)
    }

    /// `constellation repair drop-held <ino>` (see the module doc's
    /// "Dropping"). `now_unix` stamps the conflict copies' names.
    pub fn drop_held(&self, ino: Ino, now_unix: i64) -> Result<DroppedHeld, MetaError> {
        self.drop_held_at_once(ino, now_unix, false)
    }

    /// The offline forms (`drop_held`, `drop_held_remote`) have no log to
    /// wait for (ADR-59 part B, D4): they decide and apply in one go.
    fn drop_held_at_once(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
    ) -> Result<DroppedHeld, MetaError> {
        let none = |_: crate::rid::Rid| false;
        let no_ops = |_: &ChunkHash| Vec::new();
        let sessions = LiveSessions::default();
        self.drop_held_knowing(ino, now_unix, remote, &none, &no_ops, &sessions, true)?;
        self.drop_held_apply(ino, now_unix, &none, &no_ops, &sessions, true)
    }

    /// `constellation repair drop-held <ino> --remote`: the same for a
    /// transaction *deferred* on chunks another node forwarded as pending
    /// (`store::remote`) whose owner is gone for good — a continuation
    /// epoch member that died with the only copy of its epoch write's
    /// chunk (nothing reaches S3 in an epoch). The operator declares
    /// those chunks unrecoverable here; from then on it is the held case:
    /// the inode's manifests naming them become refused replays whose
    /// conflict copies carry the missing chunks as holes, their
    /// dependents are rolled back and replayed by rid, and the pending
    /// rows go. Nothing is dropped silently: the requester's op is a
    /// refusal or a conflict copy, never lost.
    pub fn drop_held_remote(&self, ino: Ino, now_unix: i64) -> Result<DroppedHeld, MetaError> {
        self.drop_held_at_once(ino, now_unix, true)
    }

    /// `drop_held` / `drop_held_remote` with what only the core knows
    /// (plan 46 M0.2 review): `unanswered(rid)` says that the answer of
    /// another node's op, journaled `RootGroup`/`DelegateNodeGroup`, is certainly
    /// not given — it still waits in the core for its group — so the row
    /// may become a placeholder like an `Archive` one. The offline forms
    /// above know nothing: such a row keeps its op, the only copy that can
    /// execute if the answer left.
    ///
    /// ADR-59 part B (D1): this is the *decision*. It journals one
    /// `LogRecord::DropHeld` naming what the drop rolls back and persists the
    /// [`DropPlan`]; it changes no other state. [`Meta::drop_held_apply`] is
    /// the effect, run when the log carries the record (D4). `live_ops` and
    /// `sessions` are read by the apply.
    #[allow(clippy::too_many_arguments)]
    pub fn drop_held_knowing(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
        unanswered: &dyn Fn(crate::rid::Rid) -> bool,
        live_ops: &dyn Fn(&ChunkHash) -> Vec<crate::rid::Rid>,
        sessions: &LiveSessions,
        sequencer: bool,
    ) -> Result<DroppedHeld, MetaError> {
        let _ = now_unix;
        // A drop is a decision of the log (ADR-59 part B, D1): only the
        // sequencer in force can journal it. A node without the lease decides
        // nothing and changes nothing; its held rows are what a deposition
        // strands anyway, and the successor re-ships their copies. The seed
        // comes back to the sequencer, where the drop is a decision.
        if !sequencer {
            return Err(MetaError::Invalid(format!(
                "this node is not the sequencer in force: nothing was dropped; run `repair \
                 drop-held {ino}` on the holder (or `--remote` there) once the takeover is through"
            )));
        }
        // The `DropHeld` record is answered by the log alone — nobody is
        // told of it — so it is an `Archive` row, the class the core's check
        // wants set. (A caller that holds a configuration sets its own: the
        // core's drop is the archive's rule at the tenure's version.)
        let _class = super::journal::PendingClass::current()
            .is_none()
            .then(|| super::journal::PendingClass::set(super::local::RowCommit::ARCHIVE));
        let mut tx = self.db.write_tx();
        if tx.get(&self.local, drop_plan_key(ino))?.is_some() {
            return Err(MetaError::Invalid(format!(
                "a drop of inode {ino} is decided and waits for the log to carry it"
            )));
        }
        if remote {
            // The inode's remote-pending chunks are unrecoverable from
            // here on: marked in the same transaction, so the plan below
            // sees them as poisoned seeds.
            let hashes = super::remote::remote_pending_for_ino_tx(&tx, self, ino)?;
            if hashes.is_empty() {
                return Err(MetaError::Invalid(format!(
                    "nothing is deferred for inode {ino} on another node's chunks: no pending \
                     upload of it is marked remote (see `status`'s `held.remote`)"
                )));
            }
            let mut fresh = 0i64;
            for hash in &hashes {
                let key = poison_key(hash, ino);
                if tx.get(&self.local, &key)?.is_none() {
                    tx.insert(&self.local, key, Vec::new());
                    fresh += 1;
                }
            }
            counter_add_tx(&mut tx, &self.local, KV_POISONED_COUNT, fresh)?;
        }
        let poisoned = read_poisoned(&tx, self)?;
        let Some(missing) = poisoned.get(&ino).cloned() else {
            return Err(MetaError::Invalid(format!(
                "nothing is held for inode {ino}: no unrecoverable pending chunk is recorded \
                 for it (a transaction deferred on another node's pending chunks needs \
                 `--remote`)"
            )));
        };
        let strand_all = self.parent_taint_conservative.load(Ordering::Relaxed);
        let walked = walk_drop(
            &tx,
            self,
            ino,
            &missing,
            &poisoned,
            DropMode::Decide { strand_all },
        )?;
        let mut plan = DropPlan {
            jseq: 0,
            epoch: 0,
            remote,
            strand_all,
            missing: missing.iter().copied().collect(),
            idents: walked.idents.iter().map(|(ident, _)| *ident).collect(),
            fills: walked.fills.clone(),
            seeds: walked.seeds.iter().map(|(_, seq, _)| *seq).collect(),
            queued: Vec::new(),
            s_deps: walked.dependents.iter().map(|(seq, _, _)| *seq).collect(),
            redone: walked.redone.iter().map(|(seq, _)| *seq).collect(),
            redone_rids: walked.redone.iter().filter_map(|(_, rid)| *rid).collect(),
            follow_ups: 0,
            shipped: false,
            acked_lost: Vec::new(),
        };
        for (t, _, _) in &walked.seeds {
            // The journal row's own rid and class: what the writer was told.
            let Some(row) = super::local::get_journal_tx(&tx, self, t.first)? else {
                continue;
            };
            if let Some(rid) = row.rid {
                if !(row.class.gated() && unanswered(rid)) {
                    plan.acked_lost
                        .push((rid, format!("wrote chunk(s) of inode {ino} that are gone")));
                }
            }
        }
        // The replays of this inode's manifest an earlier deposition queued
        // here (the root's own: `gen` 0) are refused by this drop too, so the
        // record names them (D1), by rid (`DropHeld::queued`: the row left
        // this journal; nothing retires). O(queue): an operator's path.
        plan.queued = queued_namers(&tx, self, ino, &missing)?;
        if walked.seeds.is_empty() && plan.queued.is_empty() {
            // Nothing to carry in the log: a run after an earlier drop kept a
            // lost chunk's rows because an op or a write session still named
            // it (no transaction is left to roll back, no queued replay to
            // refuse; the marks and pending rows of the chunks no longer named
            // go now).
            tx.insert(
                &self.local,
                drop_plan_key(ino),
                postcard::to_allocvec(&plan)?,
            );
            tx.commit()?;
            return self.drop_held_apply(ino, now_unix, unanswered, live_ops, sessions, true);
        }
        let local = self.begin_local(&tx)?;
        let jseq = journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::DropHeld {
                ino,
                chunks: missing.iter().copied().collect(),
                rolled_back: walked.idents.clone(),
                fills: walked.fills.clone(),
                queued: plan.queued.iter().map(|(_, rid)| *rid).collect(),
            },
        )?;
        counter_add_tx(&mut tx, &self.local, KV_DROP_HELD_UNSHIPPED, 1)?;
        self.finish_local(&mut tx, local)?;
        plan.jseq = jseq;
        plan.epoch = super::local::get_journal_tx_head(&tx, self, jseq)?.map_or(0, |h| h.epoch);
        tx.insert(
            &self.local,
            drop_plan_key(ino),
            postcard::to_allocvec(&plan)?,
        );
        tx.commit()?;
        tracing::warn!(
            ino,
            jseq,
            dropped = walked.seeds.len(),
            requeued = walked.dependents.len(),
            redone = walked.redone.len(),
            fills = ?walked.fills,
            "drop-held decided: a DropHeld record is journaled; the drop takes effect when the \
             log carries it"
        );
        Ok(DroppedHeld {
            ino,
            dropped: walked.seeds.len(),
            requeued: walked.dependents.len(),
            redone: walked.redone.len(),
            redone_rids: plan.redone_rids.clone(),
            strand_all,
            missing,
            unidentified: walked.unidentified,
            pending: Some(jseq),
            pending_epoch: plan.epoch,
            ..DroppedHeld::default()
        })
    }

    /// Tests only: decide and apply at once (no log to wait for).
    #[cfg(any(test, feature = "test-util"))]
    #[allow(clippy::too_many_arguments)]
    pub fn drop_held_knowing_now(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
        unanswered: &dyn Fn(crate::rid::Rid) -> bool,
        live_ops: &dyn Fn(&ChunkHash) -> Vec<crate::rid::Rid>,
        sessions: &LiveSessions,
        sequencer: bool,
    ) -> Result<DroppedHeld, MetaError> {
        let decided = self.drop_held_knowing(
            ino, now_unix, remote, unanswered, live_ops, sessions, sequencer,
        )?;
        if decided.pending.is_none() {
            return Ok(decided);
        }
        self.drop_held_apply(ino, now_unix, unanswered, live_ops, sessions, sequencer)
    }

    /// The drops decided and not yet applied: `(inode, journal seq of the
    /// `DropHeld` record, epoch of the tenure that journaled it)`.
    pub fn drop_plans(&self) -> Result<Vec<(Ino, u64, u64, bool)>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.prefix(&self.local, DROP_PLAN_PREFIX) {
            let (k, v) = guard.into_inner()?;
            let ino = Ino::from_be_bytes(k[DROP_PLAN_PREFIX.len()..].try_into().map_err(|_| {
                MetaError::Invalid("a drop plan key is not an inode number".to_string())
            })?);
            let plan: DropPlan = postcard::from_bytes(&v)?;
            out.push((ino, plan.jseq, plan.epoch, plan.shipped));
        }
        Ok(out)
    }

    /// ADR-59 part B (D2, D4): a segment of this node's own that carries a
    /// `DropHeld` (alone or with companion rows) and matches no journal row (a
    /// deposition stranded the rows after the segment's PUT: the shipper's ack
    /// never found them) is the record in the log all the same. What the ack
    /// of its row would have done is done from the record: its identities are
    /// retired and this node's own copies of them holed, and the decided plan
    /// whose record's journal seq is among the segment's `rows` (if the drop is
    /// still waiting for it) is marked shipped. Whether `records` carried a
    /// `DropHeld`.
    pub fn own_drop_segment(&self, records: &[LogRecord], rows: &[u64]) -> Result<bool, MetaError> {
        let mut found = false;
        let mut all_fills: Vec<(u64, u64)> = Vec::new();
        let mut tx = self.db.write_tx();
        for rec in records {
            let LogRecord::DropHeld {
                ino,
                rolled_back,
                fills,
                ..
            } = rec
            else {
                continue;
            };
            found = true;
            self.note_drop_fills_tx(&mut tx, fills)?;
            all_fills.extend(fills.iter().copied());
            let idents: BTreeSet<(u64, u64)> =
                rolled_back.iter().map(|(ident, _)| *ident).collect();
            if let Some(raw) = tx.get(&self.local, drop_plan_key(*ino))? {
                let mut plan: DropPlan = postcard::from_bytes(&raw)?;
                if !plan.shipped && rows.contains(&plan.jseq) {
                    // (Its own row is in the log: it covers the copy of the
                    // record that the deposition kept.)
                    self.member_cover_own_rows_tx(&mut tx, plan.epoch, &[plan.jseq])?;
                    plan.shipped = true;
                    tx.insert(
                        &self.local,
                        drop_plan_key(*ino),
                        postcard::to_allocvec(&plan)?,
                    );
                    counter_add_tx(&mut tx, &self.local, KV_DROP_HELD_UNSHIPPED, -1)?;
                }
            }
            self.retire_and_hole_tx(&mut tx, &idents)?;
        }
        if found {
            tx.commit()?;
            self.note_drop_fills(&all_fills);
        }
        Ok(found)
    }

    /// ADR-59 part B (drop contiguity): a `DropHeld` whose `fills` the log
    /// carries is the stream's row at each of them: the persisted log index
    /// of each generation counts it at its place in the stream's order
    /// ([`local::note_stream_fill_tx`]), in the caller's transaction (the
    /// session follows with [`Self::note_drop_fills`] once it commits).
    pub(crate) fn note_drop_fills_tx(
        &self,
        tx: &mut fjall::SingleWriterWriteTx,
        fills: &[(u64, u64)],
    ) -> Result<(), MetaError> {
        for (gen, idx) in fills {
            local::note_stream_fill_tx(tx, self, *gen, *idx)?;
        }
        Ok(())
    }

    /// `(gen, log index)` of each generation `fills` names, in the caller's
    /// transaction.
    #[cfg(debug_assertions)]
    fn stream_counts_tx(
        &self,
        tx: &impl Readable,
        fills: &[(u64, u64)],
    ) -> Result<Vec<(u64, u64)>, MetaError> {
        let gens: BTreeSet<u64> = fills.iter().map(|(gen, _)| *gen).collect();
        let mut out = Vec::with_capacity(gens.len());
        for gen in gens {
            let log_idx = super::kv_get_tx(tx, &self.local, &local::deleg_log_idx_key(gen))?
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((gen, log_idx));
        }
        Ok(out)
    }

    /// The session half of [`Self::note_drop_fills_tx`].
    pub(crate) fn note_drop_fills(&self, fills: &[(u64, u64)]) {
        for (gen, idx) in fills {
            self.session.note_stream_fill(*gen, *idx);
        }
    }

    /// Whether the decided drop of `ino` has its record shipped from this node.
    pub fn drop_plan_shipped(&self, ino: Ino) -> bool {
        let r = self.db.read_tx();
        r.get(&self.local, drop_plan_key(ino))
            .ok()
            .flatten()
            .and_then(|raw| postcard::from_bytes::<DropPlan>(&raw).ok())
            .is_some_and(|plan| plan.shipped)
    }

    /// The `DropHeld` record of `ino` at journal seq `jseq` shipped (the
    /// caller's transaction acknowledges its row): the plan says so, for a
    /// restart that has to tell a record in the log from one that never left
    /// (ADR-59 part B, D4: "in the log" is "shipped by this node").
    pub(crate) fn note_drop_shipped_tx(
        &self,
        tx: &mut fjall::SingleWriterWriteTx,
        ino: Ino,
        jseq: u64,
    ) -> Result<(), MetaError> {
        if let Some(raw) = tx.get(&self.local, drop_plan_key(ino))? {
            let mut plan: DropPlan = postcard::from_bytes(&raw)?;
            if plan.jseq == jseq && !plan.shipped {
                plan.shipped = true;
                tx.insert(
                    &self.local,
                    drop_plan_key(ino),
                    postcard::to_allocvec(&plan)?,
                );
            }
        }
        Ok(())
    }

    /// The decided drop of `ino` never happened (its record is fenced: the
    /// dropper lost the lease before the record shipped): the plan goes.
    pub fn drop_held_cancel(&self, ino: Ino) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if tx.get(&self.local, drop_plan_key(ino))?.is_some() {
            tx.remove(&self.local, drop_plan_key(ino));
            // The record never ships from here.
            counter_add_tx(&mut tx, &self.local, KV_DROP_HELD_UNSHIPPED, -1)?;
            tx.commit()?;
        }
        Ok(())
    }

    /// ADR-59 part B (D4): the effect of the drop of `ino` decided by
    /// [`Meta::drop_held_knowing`], run when the log carries its record. The
    /// plan's seeds and the rows that need the lost content are stranded
    /// (their queued replays refused or made placeholders), the rest is
    /// redone in place; the `Refused` rows and `completed` entries, the
    /// pending rows and marks go. A redo that skips a record commits only a
    /// follow-up `DropHeld` naming it (`pending` is returned again).
    ///
    /// Without the lease (`sequencer` false: a deposed dropper whose record the
    /// log carries, or one restarted after it) the effect is the same on this
    /// replica but nothing is journaled: the `Refused` rows could never ship
    /// (the journal is fenced), and the successor's gate answers the rids with
    /// the log's own record (`tail_retired`).
    #[allow(clippy::too_many_arguments)]
    pub fn drop_held_apply(
        &self,
        ino: Ino,
        now_unix: i64,
        unanswered: &dyn Fn(crate::rid::Rid) -> bool,
        live_ops: &dyn Fn(&ChunkHash) -> Vec<crate::rid::Rid>,
        sessions: &LiveSessions,
        sequencer: bool,
    ) -> Result<DroppedHeld, MetaError> {
        let _class = super::journal::PendingClass::current()
            .is_none()
            .then(|| super::journal::PendingClass::set(super::local::RowCommit::ARCHIVE));
        let mut tx = self.db.write_tx();
        let Some(raw) = tx.get(&self.local, drop_plan_key(ino))? else {
            return Err(MetaError::Invalid(format!(
                "no decided drop of inode {ino} is waiting"
            )));
        };
        let plan: DropPlan = postcard::from_bytes(&raw)?;
        // A plan applied from the log had its record's ship counted (the ack of
        // its row, or the reader meeting its segment, took it off the count of
        // unshipped records): with no other plan waiting, the count is nil.
        debug_assert!(
            !plan.shipped
                || tx.prefix(&self.local, DROP_PLAN_PREFIX).count() > 1
                || counter_get(&tx, &self.local, KV_DROP_HELD_UNSHIPPED)? == 0,
            "the drop of inode {ino} is applied from the log with its record still counted unshipped"
        );
        let missing: BTreeSet<ChunkHash> = plan.missing.iter().copied().collect();
        let poisoned = read_poisoned(&tx, self)?;
        let walked = walk_drop(&tx, self, ino, &missing, &poisoned, DropMode::Apply(&plan))?;
        let mut seqs: HashSet<u64> = walked.dependents.iter().map(|(seq, _, _)| *seq).collect();
        seqs.extend(walked.seeds.iter().map(|(_, seq, _)| *seq));
        let staged = UsageTracker::staging();
        // A stranded stream row's index is taken back only below the first
        // position the log does not carry (`local::retract_log_idx_tx`): a row
        // the record fills is in the log as the record, so the strand leaves
        // the counts of a shipped record's streams as they were.
        #[cfg(debug_assertions)]
        let streams_before = self.stream_counts_tx(&tx, &plan.fills)?;
        let (_, redo_skipped) = spec::strand_seqs_tx(&mut tx, self, &staged, &seqs)?;
        if !redo_skipped.is_empty() {
            // A row redone in place did not apply as it did: it is rolled
            // back with the others, by a follow-up decision (the loop of
            // round 2, moved into the log). Nothing of this attempt stands.
            drop(tx);
            return self.drop_held_follow_up(ino, &redo_skipped);
        }
        // D1: nothing the record names stays in the journal: the planner
        // holds a named transaction, so what is left here is a copy the
        // spec_seq match did not reach (a rewind re-captured it).
        let (leftovers, leftover_skipped) =
            strand_named_leftovers(&mut tx, self, &staged, &plan.idents)?;
        if !leftover_skipped.is_empty() {
            // The same for a redo the leftovers' strand skipped.
            drop(tx);
            return self.drop_held_follow_up(ino, &leftover_skipped);
        }
        debug_assert!(
            leftovers == 0,
            "the apply of the drop of inode {ino} left {leftovers} journal transaction(s) the \
             DropHeld names"
        );
        #[cfg(debug_assertions)]
        if plan.shipped {
            for ((gen, before), (_, after)) in streams_before
                .iter()
                .zip(self.stream_counts_tx(&tx, &plan.fills)?)
            {
                debug_assert!(
                    after >= *before,
                    "the apply of the drop of inode {ino} took the log index of stream {gen} \
                     back from {before} to {after} below a fill the log carries"
                );
            }
        }
        // (The offline forms' record has not shipped: its ack counts the
        // fills.)
        if plan.shipped {
            self.note_drop_fills_tx(&mut tx, &plan.fills)?;
        }
        if leftovers > 0 {
            tracing::warn!(
                ino,
                leftovers,
                "drop_apply_leftovers: journal transaction(s) the DropHeld names were still \
                 there after the apply stranded its rows; stranded by journal row"
            );
        }
        let mut rolled_back: Vec<crate::rid::Rid> = Vec::new();
        let mut placeholders: Vec<crate::rid::Rid> = Vec::new();
        // The rows of `S` whose answer may have left (the I1 loss the drop
        // cannot avoid: an acknowledged write of bytes that are gone, or an
        // acknowledged op that needs them). Listed, counted and warned of,
        // never excused silently. (Only a parked answer certainly has not
        // left. A row whose class waits for the archive alone is no
        // exception: a configuration that formed after it was journaled, an
        // epoch's hold, can release its answer before it ships, and the row's
        // class keeps the rule it was journaled under; flex-crash+durability
        // seed 26.)
        let mut acked_lost: Vec<(crate::rid::Rid, String)> = Vec::new();
        let may_have_left = |rid: crate::rid::Rid, class: super::local::RowCommit| {
            !(class.gated() && unanswered(rid))
        };
        // Plan 46 M0.2 (R1): another node's dependent whose answer was
        // never given is no copy of its op any more, but a placeholder in
        // its place: its requester's retry is the one copy that executes.
        let mut await_retry = 0;
        for (_, origin, keys) in &walked.dependents {
            // The redo is a fresh execution: not under the identity the
            // record retired.
            spec::strip_retired_tail_of_tx(&mut tx, self, *origin)?;
            let queued = spec::queued_class_at(&tx, self, *origin)?;
            if let Some((rid, _)) = queued {
                rolled_back.push(rid);
            }
            match spec::mark_await_retry_tx(
                &mut tx,
                self,
                *origin,
                keys.clone(),
                now_unix.saturating_mul(1000),
                unanswered,
            )? {
                Some(rid) => {
                    await_retry += 1;
                    placeholders.push(rid);
                }
                None => {
                    if let Some((rid, _)) =
                        queued.filter(|(rid, class)| may_have_left(*rid, *class))
                    {
                        acked_lost.push((
                            rid,
                            format!("needs the content of inode {ino}, whose chunk(s) are gone"),
                        ));
                    }
                }
            }
        }
        // The rids whose ops are dropped: their outcome goes in the log
        // (`Refused`), so a requester's shadow of one is rolled back
        // and a retry by rid is answered the same way everywhere.
        let mut refused_rids: Vec<crate::rid::Rid> = Vec::new();
        let refusal = |hashes: &BTreeSet<ChunkHash>| spec::Refusal {
            reason: format!(
                "dropped by `constellation repair drop-held {ino}`: chunk(s) {} unrecoverable",
                hashes
                    .iter()
                    .map(|h| h.to_hex()[..12].to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ts_unix: now_unix,
        };
        // Each seed: its queued replay becomes a refused one carrying the
        // manifest with the lost chunks as holes, so the drain makes the
        // conflict copy and never re-executes the seed.
        for (t, _, origin) in &walked.seeds {
            let Some((manifest, size)) = t.rows.iter().rev().find_map(|(_, rec)| match rec {
                LogRecord::WriteManifest {
                    ino: m,
                    manifest,
                    size,
                    ..
                } if *m == ino => Some((manifest.clone(), *size)),
                _ => None,
            }) else {
                continue;
            };
            let queued = spec::queued_class_at(&tx, self, *origin)?;
            let rid = match queued {
                Some((rid, _)) => rid,
                None => spec::replay_rid_for(&tx, self, t.first)?,
            };
            // (A seed with no queued replay cannot be told: listed.)
            if queued.is_none_or(|(rid, class)| may_have_left(rid, class)) {
                acked_lost.push((rid, format!("wrote chunk(s) of inode {ino} that are gone")));
            }
            let op = MutateOp::SetManifest {
                ino,
                base_manifest: None,
                manifest: sanitize_manifest(&manifest, &missing).unwrap_or_default(),
                size,
                mtime_ns: None,
                ctime_ns: None,
            };
            spec::refuse_queued_tx(&mut tx, self, *origin, rid, op, refusal(&missing), true)?;
            refused_rids.push(rid);
            rolled_back.push(rid);
        }
        // Replays of this inode's manifest already queued by an earlier
        // deposition: the same treatment. Not a shadow a delegate accepted
        // (`gen` other than 0): its row is the delegate's, which re-streams
        // it to the root or ends the generation, and refusing the local copy
        // rolls nothing back that the record names. The log carries the
        // delegate's row, or the entry replays here once the generation
        // ends and its journaled row is a later drop's seed.
        let mut queued_dropped = 0;
        let mut queued_kept = 0;
        // The lost chunks a queued replay this drop leaves alone still names.
        let mut queued_names: BTreeSet<ChunkHash> = BTreeSet::new();
        for queued in spec::read_pending_replays(&tx, self)? {
            if queued.refused.is_some() {
                continue;
            }
            let (m, manifest, size) = match &queued.op {
                MutateOp::SetManifest {
                    ino: m,
                    manifest,
                    size,
                    ..
                }
                | MutateOp::Publish {
                    ino: m,
                    manifest,
                    size,
                    ..
                } => (*m, manifest, *size),
                _ => continue,
            };
            if m != ino || !names_any(manifest, &missing) {
                continue;
            }
            // D1: only what the record names. A delegate's shadow (`gen` other
            // than 0) is never named; an entry queued since the decision is
            // not either. Either keeps the chunk's marks (below).
            if queued.gen != 0
                || !plan
                    .queued
                    .iter()
                    .any(|(key, rid)| *key == queued.queue_seq && *rid == queued.rid)
            {
                queued_names.extend(named_missing(manifest, &missing));
                queued_kept += 1;
                tracing::warn!(
                    ino,
                    rid = ?queued.rid,
                    gen = %crate::delegation::GenLabel::of(queued.gen),
                    "drop-held left a queued replay of a write of the inode alone (a \
                     delegate's shadow, or queued after the decision): its row lands or \
                     replays later, held behind the chunk, for a later drop"
                );
                continue;
            }
            let op = MutateOp::SetManifest {
                ino,
                base_manifest: None,
                manifest: sanitize_manifest(manifest, &missing).unwrap_or_default(),
                size,
                mtime_ns: None,
                ctime_ns: None,
            };
            if may_have_left(queued.rid, queued.class) {
                acked_lost.push((
                    queued.rid,
                    format!("a queued replay of a write of inode {ino} whose chunk(s) are gone"),
                ));
            }
            spec::refuse_queued_tx(
                &mut tx,
                self,
                queued.queue_seq,
                queued.rid,
                op,
                refusal(&missing),
                true,
            )?;
            refused_rids.push(queued.rid);
            rolled_back.push(queued.rid);
            queued_dropped += 1;
        }
        // A seed gone from the journal by now (a dropper restarted after its
        // record shipped, the successor's re-ship or a deposition took it)
        // is still lost to its writer: the decision's list stands.
        if walked.seeds.len() < plan.seeds.len() {
            for (rid, why) in &plan.acked_lost {
                if !acked_lost.iter().any(|(r, _)| r == rid) {
                    acked_lost.push((*rid, why.clone()));
                }
                if !rolled_back.contains(rid) {
                    rolled_back.push(*rid);
                }
            }
        }
        // Fix "capture under an epoch hold": the dropped ops' outcome, in
        // the log. The `DropHeld` record is in the log already (this apply
        // runs when it is); its `Refused` rows follow.
        if !refused_rids.is_empty() && sequencer {
            let local = self.begin_local(&tx)?;
            let now_ms = now_unix.saturating_mul(1000);
            for rid in &refused_rids {
                let position = journal::append_tx(
                    &mut tx,
                    &self.journal_ks,
                    &self.local,
                    &self.completed,
                    &LogRecord::Refused {
                        rid: *rid,
                        code: DROPPED_CODE,
                    },
                )?;
                tx.insert(
                    &self.completed,
                    rid.to_key(),
                    Meta::encode_refused_row(position, now_ms, DROPPED_CODE),
                );
            }
            self.finish_local(&mut tx, local)?;
        }
        let mut pending_removed = 0;
        let mut marks_removed = 0i64;
        let mut kept_pending: Vec<ChunkHash> = Vec::new();
        let mut kept_for: Vec<crate::rid::Rid> = Vec::new();
        for hash in &missing {
            // An op in flight here, or an open session, still names the
            // chunk: its row and marks stay. (An op this drop rolled back
            // is not in flight any more: its row is a seed, refused above.)
            let mut live = live_ops(hash);
            // (Only while this node is the sequencer: a deposed dropper's
            // refusals were not journaled, so the op's retry by rid reaches the
            // successor with no outcome and names the chunk like any other op.)
            if sequencer {
                live.retain(|rid| !rolled_back.contains(rid));
            }
            // A queued replay the drop left alone still names the chunk too:
            // the row it lands as must be held at once.
            if !live.is_empty() || sessions.names(hash) || queued_names.contains(hash) {
                kept_pending.push(*hash);
                kept_for.extend(live);
                continue;
            }
            if tx
                .get(&self.pending_upload, pending_key(hash, ino))?
                .is_some()
            {
                crate::store::misc::remove_pending_row_tx(&mut tx, self, hash, ino);
                pending_removed += 1;
            }
            if tx.get(&self.local, poison_key(hash, ino))?.is_some() {
                tx.remove(&self.local, poison_key(hash, ino));
                marks_removed += 1;
            }
            // A row another node was to upload (`--remote`): its mark
            // goes with the row.
            super::remote::forget_remote_mark_tx(&mut tx, self, hash, ino)?;
        }
        counter_add_tx(&mut tx, &self.local, KV_POISONED_COUNT, -marks_removed)?;
        // An adopted spilled manifest of this inode (plan 30 §M9) is
        // dropped with it: nothing is left to expand.
        let mut prefix = ADOPTED_SPILL_PREFIX.to_vec();
        prefix.extend_from_slice(&ino.to_be_bytes());
        let spills: Vec<Vec<u8>> = tx
            .prefix(&self.local, &prefix)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for key in spills {
            tx.remove(&self.local, key);
        }
        let (bytes, files) = staged.raw_delta();
        adjust_usage_tx(&mut tx, &self.local, bytes, files)?;
        let lost_total = counter_add_get_tx(
            &mut tx,
            &self.local,
            KV_ACKED_LOST_TOTAL,
            acked_lost.len() as i64,
        )?;
        tx.remove(&self.local, drop_plan_key(ino));
        tx.commit()?;
        if plan.shipped {
            self.note_drop_fills(&plan.fills);
        }
        self.acked_lost.store(lost_total, Ordering::Relaxed);
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        *self.held.lock().unwrap() = HeldSummary::default();
        tracing::warn!(
            ino,
            dropped = walked.seeds.len(),
            requeued = walked.dependents.len(),
            redone = plan.redone.len(),
            await_retry,
            queued_dropped,
            queued_kept,
            pending_removed,
            kept = kept_pending.len(),
            acked_lost = acked_lost.len(),
            "held records dropped into conflict copies"
        );
        for (rid, why) in &acked_lost {
            tracing::warn!(
                ino,
                ?rid,
                why,
                "drop-held rolled back an operation whose answer may have left: \
                 the lost data is the conflict copy (acked_lost)"
            );
        }
        kept_for.sort();
        kept_for.dedup();
        let done = DroppedHeld {
            ino,
            dropped: walked.seeds.len(),
            requeued: walked.dependents.len(),
            redone: plan.redone.len(),
            redone_rids: plan.redone_rids.clone(),
            strand_all: plan.strand_all,
            acked_lost,
            await_retry,
            pending_removed,
            queued_dropped,
            queued_kept,
            rolled_back,
            refused: refused_rids,
            placeholders,
            missing,
            kept_pending,
            kept_for,
            unidentified: walked.unidentified,
            pending: None,
            pending_epoch: 0,
            follow_ups: plan.follow_ups as usize,
            leftovers,
            deposed: !sequencer,
            applied_seq: self.applied_seq()?,
        };
        {
            let mut sink = self.applied_drops.lock().unwrap();
            if sink.len() >= 64 {
                sink.remove(0);
            }
            sink.push(done.clone());
        }
        Ok(done)
    }

    /// Tests and the simulation: the drops applied since the last call, in
    /// order (bounded).
    pub fn take_applied_drops(&self) -> Vec<DroppedHeld> {
        std::mem::take(&mut *self.applied_drops.lock().unwrap())
    }

    /// A redo at the apply skipped a record of the rows `skipped` (their
    /// `spec_seq`s): they join the rolled-back set by a follow-up
    /// `DropHeld` naming them, and the plan waits for that record too
    /// (expected never; counted `follow_ups`).
    fn drop_held_follow_up(&self, ino: Ino, skipped: &[u64]) -> Result<DroppedHeld, MetaError> {
        let _class = super::journal::PendingClass::current()
            .is_none()
            .then(|| super::journal::PendingClass::set(super::local::RowCommit::ARCHIVE));
        let mut tx = self.db.write_tx();
        let Some(raw) = tx.get(&self.local, drop_plan_key(ino))? else {
            return Err(MetaError::Invalid(format!(
                "no decided drop of inode {ino} is waiting"
            )));
        };
        let mut plan: DropPlan = postcard::from_bytes(&raw)?;
        let moved: HashSet<u64> = skipped.iter().copied().collect();
        let missing: BTreeSet<ChunkHash> = plan.missing.iter().copied().collect();
        let poisoned = read_poisoned(&tx, self)?;
        let walked = walk_drop(
            &tx,
            self,
            ino,
            &missing,
            &poisoned,
            DropMode::Follow(&plan, &moved),
        )?;
        if walked.idents.is_empty() {
            return Err(MetaError::Invalid(format!(
                "drop-held {ino}: a redone transaction diverged and cannot be named"
            )));
        }
        let local = self.begin_local(&tx)?;
        let jseq = journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::DropHeld {
                ino,
                chunks: Vec::new(),
                rolled_back: walked.idents.clone(),
                fills: walked.fills.clone(),
                queued: Vec::new(),
            },
        )?;
        counter_add_tx(&mut tx, &self.local, KV_DROP_HELD_UNSHIPPED, 1)?;
        self.finish_local(&mut tx, local)?;
        plan.idents
            .extend(walked.idents.iter().map(|(ident, _)| *ident));
        plan.fills.extend(walked.fills.iter().copied());
        plan.redone.retain(|s| !moved.contains(s));
        plan.s_deps.extend(moved.iter().copied());
        plan.jseq = jseq;
        plan.shipped = false;
        plan.epoch = super::local::get_journal_tx_head(&tx, self, jseq)?.map_or(0, |h| h.epoch);
        plan.follow_ups += 1;
        tx.insert(
            &self.local,
            drop_plan_key(ino),
            postcard::to_allocvec(&plan)?,
        );
        tx.commit()?;
        tracing::warn!(
            ino,
            jseq,
            rows = skipped.len(),
            "drop-held: a transaction redone in place did not apply as it did (a record \
             skipped): a follow-up DropHeld rolls it back with the others"
        );
        Ok(DroppedHeld {
            ino,
            pending: Some(jseq),
            pending_epoch: plan.epoch,
            follow_ups: plan.follow_ups as usize,
            ..DroppedHeld::default()
        })
    }
}

/// ADR-59 part B (D1): strand every journal transaction whose identity
/// `idents` (a `DropHeld` record's) names: by its `spec_seq` if it is a live
/// `Local` entry, else (or if it is still there) by its journal rows. Returns
/// how many there were. The planner holds such a transaction until the apply,
/// so none is expected.
fn strand_named_leftovers(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    staged: &UsageTracker,
    idents: &[(u64, u64)],
) -> Result<(usize, Vec<u64>), MetaError> {
    if idents.is_empty() {
        return Ok((0, Vec::new()));
    }
    let named: HashSet<(u64, u64)> = idents.iter().copied().collect();
    let mut left: Vec<u64> = Vec::new();
    let mut seqs: HashSet<u64> = HashSet::new();
    for t in transactions(&*tx, meta)? {
        let epoch = local::get_journal_tx_head(&*tx, meta, t.first)?.map(|h| h.epoch);
        if tx_ident(&t, epoch).is_some_and(|i| named.contains(&i)) {
            left.push(t.first);
            seqs.extend(t.spec_seq);
        }
    }
    if left.is_empty() {
        return Ok((0, Vec::new()));
    }
    // (A redo that skipped a record is the caller's follow-up decision, as for
    // the rows the plan names.)
    let mut redo_skipped = Vec::new();
    if !seqs.is_empty() {
        redo_skipped = spec::strand_seqs_tx(tx, meta, staged, &seqs)?.1;
    }
    for first in &left {
        let Some(row) = local::get_journal_tx(&*tx, meta, *first)? else {
            // (An uncaptured one-row transaction has no `journal_tx` row.)
            if tx
                .get(&meta.journal_ks, journal::seq_key(*first))?
                .is_some()
            {
                tx.remove(&meta.journal_ks, journal::seq_key(*first));
            }
            continue;
        };
        // A live spec entry the strand did not take (not a `Local` one) still
        // stands for these rows: they stay, never a spec entry without them.
        if row
            .spec_seq
            .map(|seq| spec::row_keys_and_origin(&*tx, meta, seq))
            .transpose()?
            .flatten()
            .is_some()
        {
            continue;
        }
        for seq in *first..=row.last {
            tx.remove(&meta.journal_ks, journal::seq_key(seq));
        }
        tx.remove(&meta.journal_tx, local::tx_key(*first));
        local::remove_journal_origin(tx, meta, row.gen, row.idx, *first);
        let count = if row.spec_seq.is_some() {
            KV_LOCAL_SPEC_COUNT
        } else {
            KV_UNCAPTURED_TX_COUNT
        };
        counter_add_tx(tx, &meta.local, count, -1)?;
    }
    Ok((left.len(), redo_skipped))
}

/// ADR-59 part B: what a decided drop persists (`local["drop_plan:{ino}"]`)
/// until its record is in the log and the apply has run: the set of rows the
/// record names, so the apply rolls back exactly those whatever was held
/// since.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct DropPlan {
    /// The journal seq of the (latest) `DropHeld` record, and the epoch of
    /// the tenure that journaled it (its place in the log is
    /// `(epoch, jseq)`).
    jseq: u64,
    epoch: u64,
    remote: bool,
    strand_all: bool,
    missing: Vec<ChunkHash>,
    /// The identities the record names (`DropHeld::rolled_back`): the ship
    /// plan holds these transactions until the apply strands them
    /// ([`Named`]).
    idents: Vec<(u64, u64)>,
    /// The stream positions the record fills (`DropHeld::fills`): the
    /// apply counts them in the log again after its strand of the rows
    /// that held them (which takes a stranded row's index back).
    fills: Vec<(u64, u64)>,
    /// `spec_seq`s of the seed transactions.
    seeds: Vec<u64>,
    /// The queued replays the record names (`DropHeld::queued`), by queue
    /// key and rid: the apply refuses these and no other.
    queued: Vec<(u64, crate::rid::Rid)>,
    /// `spec_seq`s of the dependents rolled back with them.
    s_deps: Vec<u64>,
    /// `spec_seq`s of the dependents redone in place.
    redone: Vec<u64>,
    redone_rids: Vec<crate::rid::Rid>,
    follow_ups: u32,
    /// The record (at `jseq`) shipped from this node: the archive carries it
    /// (set in the transaction that acknowledges its row). Persisted, since
    /// the journal's acknowledged watermark stops below the held rows the
    /// record was journaled above, and the park's in-memory knowledge of the
    /// shipped rows does not survive a restart.
    shipped: bool,
    /// The seeds whose answer may have left, listed at the decision (a
    /// parked answer cannot leave while its row is held, so the list is
    /// exact then). The apply reports them if the seed is gone by then (a
    /// dropper restarted after its record shipped).
    acked_lost: Vec<(crate::rid::Rid, String)>,
}

const DROP_PLAN_PREFIX: &[u8] = b"drop_plan:";

fn drop_plan_key(ino: Ino) -> Vec<u8> {
    let mut k = DROP_PLAN_PREFIX.to_vec();
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

/// How [`walk_drop`] picks the rows.
enum DropMode<'a> {
    /// The decision: the seeds, and what depends on them; of those only the
    /// rows that need the lost content (or all of them, `strand_all`) are
    /// rolled back.
    Decide { strand_all: bool },
    /// The apply: exactly the plan's rows.
    Apply(&'a DropPlan),
    /// A follow-up: only the `moved` rows of the plan's redone set, named.
    Follow(&'a DropPlan, &'a HashSet<u64>),
}

/// What [`walk_drop`] found.
struct Walked {
    /// (transaction, spec_seq, origin)
    seeds: Vec<(Tx, u64, u64)>,
    /// (spec_seq, origin, the keys a placeholder of it holds)
    dependents: Vec<(u64, u64, crate::replay::TouchSet)>,
    /// (spec_seq, rid) of the rows redone in place.
    redone: Vec<(u64, Option<crate::rid::Rid>)>,
    /// The identity of every transaction the drop rolls back, as a member
    /// keys it, and the rid it completes.
    idents: Vec<((u64, u64), Option<crate::rid::Rid>)>,
    /// The delegation stream positions of the rolled-back transactions
    /// (`DropHeld::fills`), in walk order.
    fills: Vec<(u64, u64)>,
    unidentified: usize,
}

/// The inode's seeds, and every held transaction that depends on them (key
/// overlap, `conflicts`, transitively, in journal order), sorted into the
/// ones rolled back and the ones redone in place.
fn walk_drop(
    tx: &fjall::SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    missing: &BTreeSet<ChunkHash>,
    poisoned: &PoisonMap,
    mode: DropMode<'_>,
) -> Result<Walked, MetaError> {
    let held = plan(tx, meta, poisoned, &PoisonMap::new(), None)?;
    let mut chain = Taint::default();
    // The inodes whose content the loss reaches: the damaged file, and any
    // file a later write of it was copied into.
    let mut lost: HashSet<Ino> = HashSet::from([ino]);
    let mut out = Walked {
        seeds: Vec::new(),
        dependents: Vec::new(),
        redone: Vec::new(),
        idents: Vec::new(),
        fills: Vec::new(),
        unidentified: 0,
    };
    for (t, seed_inos, keys) in held.held {
        let is_seed;
        let in_s;
        match &mode {
            DropMode::Decide { strand_all } => {
                is_seed = seed_inos.contains(&ino);
                let depends = keys.as_ref().is_some_and(|keys| chain.hit(keys, &[]));
                if !is_seed && !depends {
                    continue;
                }
                in_s = is_seed || *strand_all || needs_lost_content(&t, &mut lost, missing);
            }
            DropMode::Apply(plan) => {
                let Some(seq) = t.spec_seq else { continue };
                is_seed = plan.seeds.contains(&seq);
                in_s = is_seed || plan.s_deps.contains(&seq);
                if !in_s && !plan.redone.contains(&seq) {
                    continue;
                }
            }
            DropMode::Follow(plan, moved) => {
                let Some(seq) = t.spec_seq else { continue };
                is_seed = false;
                in_s = moved.contains(&seq) && plan.redone.contains(&seq);
                if !in_s {
                    continue;
                }
            }
        }
        let (Some(spec_seq), Some(keys)) = (t.spec_seq, keys) else {
            return Err(MetaError::Invalid(format!(
                "held transaction at journal seq {} was journaled without before-images, \
                 so it cannot be rolled back; rebuild this replica",
                t.first
            )));
        };
        let row = local::get_journal_tx(tx, meta, t.first)?;
        let rid = tx_rid(&t, row.as_ref());
        chain.add(&keys);
        if !in_s {
            out.redone.push((spec_seq, rid));
            continue;
        }
        // The stream position the transaction holds (if it is a row of a
        // delegation stream): the record fills it, so the stream's later
        // rows the drop keeps land contiguous.
        if let Some(row) = row.as_ref().filter(|row| row.gen != 0) {
            out.fills.push((row.gen, row.idx));
        }
        let placeholder = placeholder_keys(&t, &keys);
        let records: Vec<LogRecord> = t.rows.iter().map(|(_, rec)| rec.clone()).collect();
        let head_epoch = super::local::get_journal_tx_head(tx, meta, t.first)?.map(|h| h.epoch);
        match rolled_back_ident(head_epoch, t.first, &records) {
            Some(ident) => out.idents.push((ident, rid)),
            // A captured transaction (it has before-images) has its head;
            // one without cannot be named to the members, which keep their
            // copy of it. Said, counted, never silent.
            None => {
                out.unidentified += 1;
                tracing::warn!(
                    first = t.first,
                    "a transaction the drop rolls back has no journal head: \
                     members cannot be told to retract it"
                );
            }
        }
        let origin = spec::row_keys_and_origin(tx, meta, spec_seq)?
            .map(|row| row.origin)
            .unwrap_or(spec_seq);
        if is_seed {
            out.seeds.push((t, spec_seq, origin));
        } else {
            out.dependents.push((spec_seq, origin, placeholder));
        }
    }
    Ok(out)
}

/// Whether `tx` writes or reads the content of an inode of `lost` (the
/// damaged file, and any file a write of it was copied into): a manifest of
/// it whatever the manifest says (these are data ops on the damaged file;
/// narrowing them to the chunks they name is a later refinement), or a
/// size change of it. A write naming a lost chunk of another inode copies
/// the damage there: that inode joins `lost`.
///
/// A row this does not select is redone in place: its journal row, `completed`
/// row, rid and answer are kept, so it can have been acknowledged and stays
/// so. If it was told in doubt and lands when the drop releases it, that is
/// the operator's action, as a queued replay's landing is (`redone_rids`
/// exempts it from the late-landing check). A kept in-doubt create keeps its
/// inode allocated: a requester's retry under a new rid allocates afresh.
fn needs_lost_content(tx: &Tx, lost: &mut HashSet<Ino>, missing: &BTreeSet<ChunkHash>) -> bool {
    let mut needs = false;
    for (_, rec) in &tx.rows {
        match rec {
            LogRecord::WriteManifest { ino, manifest, .. } => {
                if lost.contains(ino) {
                    needs = true;
                } else if matches!(
                    Manifest::decode(manifest),
                    Ok(Manifest { chunks: ChunkInfo::Inline(chunks), .. })
                        if chunks.values().any(|h| missing.contains(h))
                ) {
                    lost.insert(*ino);
                    needs = true;
                }
            }
            LogRecord::Setattr {
                ino, size: Some(_), ..
            } if lost.contains(ino) => needs = true,
            _ => {}
        }
    }
    needs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rolled_back_transaction_is_named_by_its_tail_or_its_place() {
        let recs = [LogRecord::Completed {
            rid: crate::rid::Rid {
                node: 1,
                incarnation: 1,
                seq: 1,
            },
        }];
        assert_eq!(rolled_back_ident(Some(3), 7, &recs), Some((3, 7)));
        let tail = [LogRecord::TailOf { epoch: 1, jseq: 4 }, recs[0].clone()];
        assert_eq!(rolled_back_ident(Some(3), 7, &tail), Some((1, 4)));
        assert_eq!(rolled_back_ident(None, 7, &recs), None, "no head: no name");
    }

    #[test]
    fn a_hole_replaces_each_lost_chunk() {
        let a = ChunkHash::of(b"a");
        let b = ChunkHash::of(b"b");
        let chunks = BTreeMap::from([(0u64, a), (1u64, b)]);
        let m = Manifest {
            layout: replicix_chunkfmt::ChunkLayout::new(4),
            file_len: 8,
            chunks: ChunkInfo::Inline(chunks),
        }
        .encode();
        assert!(names_any(&m, &BTreeSet::from([b])));
        assert!(!names_any(&m, &BTreeSet::from([ChunkHash::of(b"c")])));
        let clean = sanitize_manifest(&m, &BTreeSet::from([b])).unwrap();
        let clean = Manifest::decode(&clean).unwrap();
        assert_eq!(clean.file_len, 8);
        assert_eq!(clean.chunks, ChunkInfo::Inline(BTreeMap::from([(0u64, a)])));
        assert!(names_any(b"not a manifest", &BTreeSet::from([a])));
    }

    /// ADR-58 C1: a refusal journaled without observed keys (a settle's
    /// `ABANDONED`) ships past a held transaction on the very keys an
    /// errno refusal of the same op would have observed, which stays held.
    #[test]
    fn a_refusal_without_observed_keys_ships_past_a_held_transaction() {
        use crate::mutate::MutateOp;
        use crate::rid::Rid;
        use crate::MetaStore;
        use constellation_fs_core::types::ROOT_INO;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        let f0 = meta.create(ROOT_INO, "f0", 0o644, 0, 0).unwrap().ino;
        let shipped = |segment: u64| -> Vec<LogRecord> {
            let rows = meta.take_shippable(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
            if !seqs.is_empty() {
                meta.ack_journal_rows_at(&seqs, segment).unwrap();
            }
            rows.into_iter().map(|(_, r)| r).collect()
        };
        shipped(1);
        // A seed write whose chunk is lost, and a dependent on the same ino.
        let hash = ChunkHash::of(b"lost");
        let manifest = Manifest {
            layout: replicix_chunkfmt::ChunkLayout::new(4096),
            file_len: 7,
            chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
        }
        .encode();
        meta.set_manifest_dirty(f0, None, &manifest, 7, None, None, &[hash])
            .unwrap();
        meta.note_unrecoverable_chunks(&[(hash, f0)], false)
            .unwrap();
        crate::mutate::execute(
            &meta,
            &MutateOp::Setattr {
                ino: f0,
                mode: Some(0o600),
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns: None,
            },
            None,
        )
        .unwrap();
        let rid = |seq| Rid {
            node: 9,
            incarnation: 1,
            seq,
        };
        let op = MutateOp::Link {
            ino: f0,
            parent: ROOT_INO,
            name: "f0".into(),
        };
        let refusals = |rows: &[LogRecord]| {
            rows.iter()
                .filter(|r| matches!(r, LogRecord::Refused { .. }))
                .count()
        };
        // An errno refusal that observed the held keys stays held.
        meta.journal_refusal(rid(1), constellation_types::Code::Exists, Some(&op))
            .unwrap();
        assert_eq!(refusals(&shipped(2)), 0, "an errno refusal waits");
        // A refusal without observed keys (`ABANDONED`) ships.
        meta.journal_refusal(rid(2), constellation_types::Code::Canceled, None)
            .unwrap();
        let rows = shipped(3);
        assert!(
            rows.iter().any(|r| matches!(
                r,
                LogRecord::Refused { rid: r, .. } if *r == rid(2)
            )),
            "the settle's refusal ships past the held transaction: {rows:?}"
        );
        assert!(!rows.iter().any(|r| matches!(
            r,
            LogRecord::Refused { rid: r, .. } if *r == rid(1)
        )));
    }

    /// ADR-55 (M3): a stream's index order is an exclusive key of the ship
    /// plan. A held row of delegation stream `gen` holds every later row of
    /// `gen` too, though they touch other keys: shipping index 3 ahead of a
    /// held index 2 leaves a hole in the log's stream, and the successor's
    /// cursor, raised to the highest index it saw, skips the re-streamed
    /// row for good. Fails on the parent: the plan shipped index 3.
    #[test]
    fn a_held_stream_row_holds_the_later_rows_of_its_stream() {
        use crate::mutate::MutateOp;
        use crate::MetaStore;
        use constellation_fs_core::types::ROOT_INO;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        let files: Vec<Ino> = (1..=3)
            .map(|i| {
                meta.create(ROOT_INO, &format!("f{i}"), 0o644, 0, 0)
                    .unwrap()
                    .ino
            })
            .collect();
        let shipped = |segment: u64| -> Vec<LogRecord> {
            let rows = meta.take_shippable(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
            if !seqs.is_empty() {
                meta.ack_journal_rows_at(&seqs, segment).unwrap();
            }
            rows.into_iter().map(|(_, r)| r).collect()
        };
        shipped(1);
        let gen = (1u64 << 32) | 1;
        let setattr = |ino: Ino, mode: u32| MutateOp::Setattr {
            ino,
            mode: Some(mode),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
        };
        let run = |op: &MutateOp| {
            let _g = journal::PendingDelegate::set(gen, None, crate::session::Position::ZERO);
            crate::mutate::execute(&meta, op, None).unwrap();
        };
        // Index 1 ships; index 2 is a seed whose chunk is lost; index 3 is on
        // another file.
        run(&setattr(files[0], 0o600));
        let hash = ChunkHash::of(b"lost");
        let manifest = Manifest {
            layout: replicix_chunkfmt::ChunkLayout::new(4096),
            file_len: 7,
            chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
        }
        .encode();
        {
            let _g = journal::PendingDelegate::set(gen, None, crate::session::Position::ZERO);
            meta.set_manifest_dirty(files[1], None, &manifest, 7, None, None, &[hash])
                .unwrap();
        }
        run(&setattr(files[2], 0o600));
        assert_eq!(meta.delegate_idx(gen).unwrap(), 3);
        meta.note_unrecoverable_chunks(&[(hash, files[1])], false)
            .unwrap();
        shipped(2);
        let left: Vec<u64> = meta
            .delegate_rows_for_audit(gen)
            .unwrap()
            .into_iter()
            .map(|(idx, ..)| idx)
            .collect();
        assert_eq!(
            left,
            vec![2, 3],
            "index 3 shipped ahead of the held index 2: a hole in the log's stream"
        );
        // Released: the rest ships, in order.
        meta.note_unrecoverable_chunks(&[], true).unwrap();
        meta.cancel_pending_upload(&hash, files[1]).unwrap();
        shipped(3);
        assert!(meta.delegate_rows_for_audit(gen).unwrap().is_empty());
    }

    /// ADR-59 part B (drop contiguity, option (a)): a drop that rolls back
    /// rows of a delegation stream fills each of their indices with the
    /// `DropHeld` record's no-op row, so the rows of the stream it keeps land
    /// contiguous behind it. The root's own stream (the sim's
    /// `far-placement+lost-chunk` 96, `far-root-writes+lost-chunk` 557, ...):
    /// index 2 is the seed (its chunk is lost), index 3 changes the file's
    /// mode (redone in place: it does not need the lost bytes), index 4
    /// truncates it (rolled back: it does), index 5 is on another file. The
    /// log then carries index 1, the record (fills 2 and 4), then 3 and 5;
    /// a replica's index of the stream reaches 2 at the record and 5 at the
    /// last row, the fill at 4 counting once 3 is there (the 956 shape).
    /// Fails on the parent (no fills: the log skips 2 and 4).
    #[test]
    fn a_drop_fills_the_stream_indices_it_rolls_back() {
        use crate::mutate::MutateOp;
        use crate::MetaStore;
        use constellation_fs_core::types::ROOT_INO;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        let replica = Meta::open_in_memory().unwrap();
        replica.set_node_prefix(2).unwrap();
        let files: Vec<Ino> = (1..=3)
            .map(|i| {
                meta.create(ROOT_INO, &format!("f{i}"), 0o644, 0, 0)
                    .unwrap()
                    .ino
            })
            .collect();
        let gen = (1u64 << 32) | 1;
        // Ship what the plan releases, into the replica as a segment; the
        // stream positions the log carries, in order.
        let positions = std::cell::RefCell::new(Vec::<u64>::new());
        let next_seq = std::cell::Cell::new(1u64);
        let ship = || -> Vec<LogRecord> {
            let rows = meta.take_shippable(usize::MAX).unwrap();
            if rows.is_empty() {
                return Vec::new();
            }
            let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
            let origins = meta.journal_origins(&seqs).unwrap();
            let records: Vec<LogRecord> = rows.into_iter().map(|(_, r)| r).collect();
            let seq = next_seq.get();
            next_seq.set(seq + 1);
            replica
                .apply_segment_rows(
                    seq,
                    1,
                    *seqs.iter().max().unwrap(),
                    &seqs,
                    &origins,
                    &records,
                    &Default::default(),
                )
                .unwrap();
            meta.ack_journal_rows_at(&seqs, seq).unwrap();
            let mut p = positions.borrow_mut();
            p.extend(origins.iter().filter(|(g, _)| *g == gen).map(|(_, i)| *i));
            for rec in &records {
                p.extend(
                    rec.stream_fills()
                        .iter()
                        .filter(|(g, _)| *g == gen)
                        .map(|(_, i)| *i),
                );
            }
            records
        };
        ship();
        let setattr = |ino: Ino, mode: Option<u32>, size: Option<u64>| MutateOp::Setattr {
            ino,
            mode,
            uid: None,
            gid: None,
            size,
            atime_ns: None,
            mtime_ns: None,
        };
        let run = |op: &MutateOp| {
            let _g = journal::PendingDelegate::set(gen, None, crate::session::Position::ZERO);
            crate::mutate::execute(&meta, op, None).unwrap();
        };
        run(&setattr(files[0], Some(0o600), None));
        let hash = ChunkHash::of(b"lost");
        let manifest = Manifest {
            layout: replicix_chunkfmt::ChunkLayout::new(4096),
            file_len: 7,
            chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
        }
        .encode();
        {
            let _g = journal::PendingDelegate::set(gen, None, crate::session::Position::ZERO);
            meta.set_manifest_dirty(files[1], None, &manifest, 7, None, None, &[hash])
                .unwrap();
        }
        run(&setattr(files[1], Some(0o600), None));
        run(&setattr(files[1], None, Some(3)));
        run(&setattr(files[2], Some(0o600), None));
        assert_eq!(meta.delegate_idx(gen).unwrap(), 5);
        meta.note_unrecoverable_chunks(&[(hash, files[1])], false)
            .unwrap();
        ship();
        assert_eq!(
            *positions.borrow(),
            vec![1],
            "only index 1 ships ahead of the seed"
        );
        let none = |_: crate::rid::Rid| false;
        let no_ops = |_: &ChunkHash| Vec::new();
        let sessions = LiveSessions::default();
        let decided = meta
            .drop_held_knowing(files[1], 0, false, &none, &no_ops, &sessions, true)
            .unwrap();
        assert!(decided.pending.is_some(), "{decided:?}");
        // The decision ships alone: the record, with its fills.
        let records = ship();
        assert!(
            records
                .iter()
                .any(|r| matches!(r, LogRecord::DropHeld { .. })),
            "{records:?}"
        );
        assert_eq!(*positions.borrow(), vec![1, 2, 4]);
        assert_eq!(
            replica.log_stream_idx(gen).unwrap(),
            2,
            "the fill at 4 waits for index 3"
        );
        meta.drop_held_apply(files[1], 0, &none, &no_ops, &sessions, true)
            .unwrap();
        ship();
        assert_eq!(*positions.borrow(), vec![1, 2, 4, 3, 5]);
        assert!(meta.delegate_rows_for_audit(gen).unwrap().is_empty());
        // Every index of the stream is in the log, once.
        let mut sorted = positions.borrow().clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
        assert_eq!(replica.log_stream_idx(gen).unwrap(), 5);
        // The dropper's own count of the log's index.
        assert_eq!(meta.log_stream_idx(gen).unwrap(), 5);
    }

    /// ADR-59 part B (drop contiguity), review SHOULD-3: a fill is the retire
    /// key of a delegate's own outstanding row. The root's drop rolled back
    /// its append of the delegate's row 2 (a dependent that needs the lost
    /// bytes) and the log carries the record there: the delegate, applying
    /// the record's segment, strands its own row 2 quietly (the root's queue
    /// owns the replay or the refusal), its rid's completion with it, and
    /// keeps row 1. Fails on the parent: row 2 stayed outstanding for good,
    /// with its `completed` entry, under a log index that passed it.
    #[test]
    fn a_fill_retires_the_delegates_own_row_at_its_position() {
        use crate::mutate::MutateOp;
        use crate::MetaStore;
        use constellation_fs_core::types::ROOT_INO;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_node_prefix(2).unwrap();
        let files: Vec<Ino> = (1..=2)
            .map(|i| {
                meta.create(ROOT_INO, &format!("f{i}"), 0o644, 0, 0)
                    .unwrap()
                    .ino
            })
            .collect();
        let gen = (1u64 << 32) | 3;
        let rid = |seq: u64| crate::rid::Rid {
            node: 2,
            incarnation: 1,
            seq,
        };
        for (i, ino) in files.iter().enumerate() {
            let _g = journal::PendingDelegate::set(gen, None, crate::session::Position::ZERO);
            crate::mutate::execute(
                &meta,
                &MutateOp::Setattr {
                    ino: *ino,
                    mode: Some(0o600),
                    uid: None,
                    gid: None,
                    size: None,
                    atime_ns: None,
                    mtime_ns: None,
                },
                Some(rid(i as u64 + 1)),
            )
            .unwrap();
        }
        let idx = |m: &Meta| -> Vec<u64> {
            m.delegate_rows_for_audit(gen)
                .unwrap()
                .into_iter()
                .map(|(idx, ..)| idx)
                .collect()
        };
        assert_eq!(idx(&meta), vec![1, 2]);
        assert!(meta.completed_position(rid(2)).unwrap().is_some());
        meta.apply_segment_rows(
            1,
            1,
            0,
            &[9],
            &[(0, 0)],
            &[LogRecord::DropHeld {
                ino: 99,
                chunks: Vec::new(),
                rolled_back: Vec::new(),
                fills: vec![(gen, 2)],
                queued: Vec::new(),
            }],
            &Default::default(),
        )
        .unwrap();
        assert_eq!(idx(&meta), vec![1], "the filled row is still outstanding");
        assert_eq!(meta.completed_position(rid(2)).unwrap(), None);
        assert!(meta.completed_position(rid(1)).unwrap().is_some());
        let queued = meta.pending_replays().unwrap();
        assert!(queued.is_empty(), "nothing is queued here: {queued:?}");
    }

    /// ADR-58 M1 (review): a transaction waiting for this node's own upload is
    /// not behind a lost chunk. The refusal of an op that touched a file
    /// whose chunk is still uploading here is merely deferred: it ships by
    /// itself, and its rid is not `rid_behind_lost_chunk`. Deferred behind
    /// another node's pending upload (named explicitly) it is, and held
    /// behind a poisoned chunk it is.
    #[test]
    fn a_row_deferred_for_its_own_upload_is_not_behind_a_lost_chunk() {
        use crate::mutate::MutateOp;
        use crate::rid::Rid;
        use crate::MetaStore;
        use constellation_fs_core::types::ROOT_INO;
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        let f0 = meta.create(ROOT_INO, "f0", 0o644, 0, 0).unwrap().ino;
        meta.take_shippable(usize::MAX).unwrap();
        let hash = ChunkHash::of(b"slow");
        let manifest = Manifest {
            layout: replicix_chunkfmt::ChunkLayout::new(4096),
            file_len: 4,
            chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
        }
        .encode();
        meta.set_manifest_dirty(f0, None, &manifest, 4, None, None, &[hash])
            .unwrap();
        let rid = Rid {
            node: 9,
            incarnation: 1,
            seq: 1,
        };
        let op = MutateOp::Link {
            ino: f0,
            parent: ROOT_INO,
            name: "f0".into(),
        };
        meta.journal_refusal(rid, constellation_types::Code::Exists, Some(&op))
            .unwrap();
        // The chunk is still uploading: the planner defers the write and,
        // after it, the refusal.
        meta.take_shippable(usize::MAX).unwrap();
        assert_eq!(meta.held_summary().deferred, 2, "{:?}", meta.held_summary());
        assert!(
            !meta.rid_behind_lost_chunk(rid),
            "a row waiting for an upload is not behind a lost chunk"
        );
        // The chunk is another node's pending upload (its uploader may never
        // return): the refusal is still deferred, and now it counts.
        meta.enroll_remote_chunks(f0, &[hash], 5).unwrap();
        meta.take_shippable(usize::MAX).unwrap();
        assert_eq!(meta.held_summary().deferred, 2, "{:?}", meta.held_summary());
        assert!(
            meta.rid_behind_lost_chunk(rid),
            "deferred behind another node's pending upload: {:?}",
            meta.held_summary()
        );
        // The chunk is lost: the same refusal is held.
        meta.note_unrecoverable_chunks(&[(hash, f0)], false)
            .unwrap();
        meta.take_shippable(usize::MAX).unwrap();
        assert!(meta.rid_behind_lost_chunk(rid), "{:?}", meta.held_summary());
    }

    // ---- chunk close-stall-followup: the blame is the ship rule's ----

    mod blame {
        use super::*;
        use crate::mutate::{execute, MutateOp};
        use crate::rid::Rid;
        use crate::MetaStore;
        use constellation_fs_core::types::ROOT_INO;
        use proptest::prelude::*;

        const FILES: usize = 4;
        /// The forwarders whose chunks a close may name (node 7's is the
        /// blame under test; node 8's is pending but not theirs).
        const NODES: [u64; 2] = [7, 8];

        #[derive(Clone, Debug)]
        enum Step {
            /// A close of file `f` naming chunk `c`: pending here (`None`,
            /// the holder's own write-back), forwarded pending by a node,
            /// or already up.
            Close {
                f: usize,
                c: u8,
                from: Option<Option<usize>>,
            },
            Chmod {
                f: usize,
                node: usize,
            },
            Rename {
                f: usize,
                to: usize,
                node: usize,
            },
            /// A refusal that observed `f`'s name (a link onto it).
            Refuse {
                f: usize,
                node: usize,
            },
            /// ADR-59: a create, unlink or mkdir of another name in the
            /// same directory (`g{n}`), and an explicit utimes of it.
            Create {
                n: usize,
                node: usize,
            },
            Unlink {
                n: usize,
                node: usize,
            },
            Mkdir {
                n: usize,
                node: usize,
            },
            Utimes {
                node: usize,
            },
            /// Ack the `k`-th pending row enrolled so far.
            Ack(usize),
            /// Record the `k`-th pending row as unrecoverable.
            Poison(usize),
            Ship,
        }

        fn step() -> impl Strategy<Value = Step> {
            let f = 0..FILES;
            prop_oneof![
                4 => (f.clone(), 0..6u8, prop_oneof![
                    Just(None),
                    Just(Some(None)),
                    (0..NODES.len()).prop_map(|n| Some(Some(n))),
                ])
                    .prop_map(|(f, c, from)| Step::Close {
                        f,
                        c,
                        from: from.map(|n: Option<usize>| n),
                    }),
                3 => (f.clone(), 0..3usize).prop_map(|(f, node)| Step::Chmod { f, node }),
                2 => (f.clone(), 0..FILES, 0..3usize)
                    .prop_map(|(f, to, node)| Step::Rename { f, to, node }),
                1 => (f, 0..3usize).prop_map(|(f, node)| Step::Refuse { f, node }),
                2 => (0..3usize, 0..3usize).prop_map(|(n, node)| Step::Create { n, node }),
                1 => (0..3usize, 0..3usize).prop_map(|(n, node)| Step::Unlink { n, node }),
                1 => (0..3usize, 0..3usize).prop_map(|(n, node)| Step::Mkdir { n, node }),
                1 => (0..3usize).prop_map(|node| Step::Utimes { node }),
                2 => (0..16usize).prop_map(Step::Ack),
                1 => (0..16usize).prop_map(Step::Poison),
                1 => Just(Step::Ship),
            ]
        }

        fn manifest_naming(hash: ChunkHash) -> Vec<u8> {
            Manifest {
                layout: replicix_chunkfmt::ChunkLayout::new(4096),
                file_len: 7,
                chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
            }
            .encode()
        }

        /// Run `steps` on a fresh holder. Node 7's and 8's ops carry
        /// their rids; index 2 stands for the holder itself (no rid).
        /// What [`journal`] left: the holder and every record it shipped
        /// (in order).
        struct Journaled {
            meta: Meta,
            shipped: Vec<LogRecord>,
        }

        fn journal(steps: &[Step]) -> Journaled {
            let mut shipped_records: Vec<LogRecord> = Vec::new();
            let meta = Meta::open_in_memory().unwrap();
            meta.set_holder_epoch(1);
            let mut inos = Vec::new();
            for i in 0..FILES {
                inos.push(
                    meta.create(ROOT_INO, &format!("f{i}"), 0o644, 0, 0)
                        .unwrap()
                        .ino,
                );
            }
            let shipped = |meta: &Meta, segment: u64, out: &mut Vec<LogRecord>| {
                let rows = meta.take_shippable(usize::MAX).unwrap();
                let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
                out.extend(rows.into_iter().map(|(_, r)| r));
                if !seqs.is_empty() {
                    meta.ack_journal_rows_at(&seqs, segment).unwrap();
                }
            };
            shipped(&meta, 1, &mut shipped_records);
            let mut segment = 1;
            let mut seq = 0;
            let mut rid = |node: usize| {
                seq += 1;
                NODES.get(node).map(|&node| Rid {
                    node,
                    incarnation: 1,
                    seq,
                })
            };
            let mut rows: Vec<(ChunkHash, Ino)> = Vec::new();
            let run = |meta: &Meta, rid: Option<Rid>, op: MutateOp| {
                if let Err(e) = execute(meta, &op, rid) {
                    if let Some(rid) = rid {
                        meta.journal_refusal(rid, e.code(), Some(&op)).unwrap();
                    }
                }
            };
            for (i, s) in steps.iter().enumerate() {
                match s {
                    Step::Close { f, c, from } => {
                        let ino = inos[*f];
                        let hash = ChunkHash::of(&[*c, *f as u8]);
                        let manifest = manifest_naming(hash);
                        match from {
                            // A file renamed over is gone: nothing to close.
                            Some(None) => {
                                if meta
                                    .set_manifest_dirty(
                                        ino,
                                        None,
                                        &manifest,
                                        7,
                                        None,
                                        None,
                                        &[hash],
                                    )
                                    .is_ok()
                                {
                                    rows.push((hash, ino));
                                }
                            }
                            Some(Some(n)) => {
                                meta.enroll_remote_chunks(ino, &[hash], NODES[*n]).unwrap();
                                rows.push((hash, ino));
                                let op = MutateOp::SetManifest {
                                    ino,
                                    base_manifest: None,
                                    manifest,
                                    size: 7,
                                    mtime_ns: None,
                                    ctime_ns: None,
                                };
                                run(&meta, rid(*n), op);
                            }
                            None => {
                                let _ = meta
                                    .set_manifest_with_base(ino, None, &manifest, 7, None, None);
                            }
                        }
                    }
                    Step::Chmod { f, node } => {
                        let op = MutateOp::Setattr {
                            ino: inos[*f],
                            mode: Some(0o600),
                            uid: None,
                            gid: None,
                            size: None,
                            atime_ns: None,
                            mtime_ns: None,
                        };
                        run(&meta, rid(*node), op);
                    }
                    Step::Rename { f, to, node } => {
                        let op = MutateOp::Rename {
                            parent: ROOT_INO,
                            name: format!("f{f}"),
                            new_parent: ROOT_INO,
                            new_name: format!("f{to}"),
                            noreplace: false,
                        };
                        run(&meta, rid(*node), op);
                    }
                    Step::Refuse { f, node } => {
                        let op = MutateOp::Link {
                            ino: inos[*f],
                            parent: ROOT_INO,
                            name: format!("f{f}"),
                        };
                        // (The holder's own: a rid of its own per refusal.)
                        let r = rid(*node).unwrap_or(Rid {
                            node: 9,
                            incarnation: 1,
                            seq: i as u64,
                        });
                        meta.journal_refusal(r, constellation_types::Code::Exists, Some(&op))
                            .unwrap();
                    }
                    Step::Create { n, node } => {
                        let op = MutateOp::Create {
                            parent: ROOT_INO,
                            name: format!("g{n}"),
                            ino: (1 << 40) + i as u64,
                            mode: 0o644,
                            uid: 0,
                            gid: 0,
                        };
                        run(&meta, rid(*node), op);
                    }
                    Step::Unlink { n, node } => {
                        let op = MutateOp::Unlink {
                            parent: ROOT_INO,
                            name: format!("g{n}"),
                        };
                        run(&meta, rid(*node), op);
                    }
                    Step::Mkdir { n, node } => {
                        let op = MutateOp::Mkdir {
                            parent: ROOT_INO,
                            name: format!("g{n}"),
                            ino: (1 << 41) + i as u64,
                            mode: 0o755,
                            uid: 0,
                            gid: 0,
                        };
                        run(&meta, rid(*node), op);
                    }
                    Step::Utimes { node } => {
                        let op = MutateOp::Setattr {
                            ino: ROOT_INO,
                            mode: None,
                            uid: None,
                            gid: None,
                            size: None,
                            atime_ns: None,
                            mtime_ns: Some(1 + i as i64),
                        };
                        run(&meta, rid(*node), op);
                    }
                    Step::Ack(k) => {
                        if let Some((hash, ino)) = rows.get(*k) {
                            meta.ack_upload(hash, *ino).unwrap();
                        }
                    }
                    Step::Poison(k) => {
                        if let Some(&pair) = rows.get(*k) {
                            meta.note_unrecoverable_chunks(&[pair], false).unwrap();
                        }
                    }
                    Step::Ship => {
                        segment += 1;
                        shipped(&meta, segment, &mut shipped_records);
                    }
                }
                // Chunk metered-own-rows: node 7's blame walk is kept
                // between answers and resumed; ask after every step, as a
                // holder does after every forwarded op, through the tip
                // and through half of it.
                let r = meta.db.read_tx();
                let theirs = crate::store::remote::remote_pending_from_tx(&r, &meta, 7).unwrap();
                if !theirs.is_empty() {
                    let tip = journal::max_seq(&r, &meta.journal_ks, &meta.local).unwrap();
                    blame_walk(&r, &meta, 7, &theirs, tip / 2).unwrap();
                    blame_walk(&r, &meta, 7, &theirs, tip).unwrap();
                }
            }
            Journaled {
                meta,
                shipped: shipped_records,
            }
        }

        /// The rule written out pairwise, quadratic, as the module doc
        /// states it (an uncaptured transaction touching every key): a
        /// transaction waits if it is a pending seed or depends on an
        /// earlier one that waits; its blame is its own seeds in `theirs`
        /// and the blame of every waiting transaction it depends on.
        /// Returns, per transaction (first seq), its rid, last seq and
        /// `Some(blame)` if it waits.
        #[allow(clippy::type_complexity)]
        fn model(
            meta: &Meta,
            theirs: &PoisonMap,
        ) -> Vec<(u64, Option<Rid>, u64, Option<BTreeSet<Ino>>)> {
            let r = meta.db.read_tx();
            let pending = read_pending(&r, meta).unwrap();
            let mut out: Vec<(u64, Option<Rid>, u64, Option<BTreeSet<Ino>>)> = Vec::new();
            let mut written: Vec<Option<Vec<(Vec<u8>, Mode)>>> = Vec::new();
            for tx in transactions(&r, meta).unwrap() {
                let row = local::get_journal_tx(&r, meta, tx.first).unwrap();
                let keys: Option<TxKeys> = tx_keys(
                    &tx,
                    tx.spec_seq.and_then(|seq| {
                        spec::row_keys_and_origin(&r, meta, seq)
                            .unwrap()
                            .map(|row| (row.keys, row.merge))
                    }),
                    row.as_ref(),
                );
                // What it writes (the taint), and what it accesses: the
                // same with a refusal's observation added.
                let taint: Option<Vec<(Vec<u8>, Mode)>> = keys.as_ref().map(|k| {
                    [Mode::Excl, Mode::Merge]
                        .into_iter()
                        .flat_map(|m| k.of(m).iter().map(move |key| (key.clone(), m)))
                        .collect()
                });
                let mut touched = taint.clone();
                if let (Some(t), Some(row)) = (touched.as_mut(), row.as_ref()) {
                    t.extend(row.observed.iter().map(|k| (k.clone(), Mode::Obs)));
                }
                let mut waits = !seed_inos(&tx, &pending).is_empty();
                let mut blame: BTreeSet<Ino> = seed_inos(&tx, theirs).into_iter().collect();
                for (i, (_, _, _, earlier)) in out.iter().enumerate() {
                    let Some(earlier) = earlier else { continue };
                    let depends = match (&written[i], &touched) {
                        (Some(w), Some(t)) => w.iter().any(|(wk, wm)| {
                            t.iter().any(|(tk, tm)| wk == tk && conflicts(*tm, *wm))
                        }),
                        _ => true,
                    };
                    if depends {
                        waits = true;
                        blame.extend(earlier);
                    }
                }
                written.push(taint);
                out.push((
                    tx.first,
                    tx_rid(&tx, row.as_ref()),
                    tx.last,
                    waits.then_some(blame),
                ));
            }
            out
        }

        fn check(steps: &[Step]) {
            let meta = journal(steps).meta;
            let r = meta.db.read_tx();
            let poisoned = read_poisoned(&r, &meta).unwrap();
            let pending = read_pending(&r, &meta).unwrap();
            let node7 = crate::store::remote::remote_pending_from_tx(&r, &meta, 7).unwrap();
            // Chunk metered-own-rows: the kept walk, resumed across the
            // steps, says what the rule says: every waiting transaction
            // with its blame, and every one of node 7's.
            if !node7.is_empty() {
                let expect = model(&meta, &node7);
                let kept = blame_walk(&r, &meta, 7, &node7, u64::MAX).unwrap();
                type Row = (u64, u64, Option<Rid>, Option<BTreeSet<Ino>>);
                let want: Vec<Row> = expect
                    .iter()
                    .filter(|e| e.3.is_some() || e.1.is_some_and(|rid| rid.node == 7))
                    .map(|e| (e.0, e.2, e.1, e.3.clone()))
                    .collect();
                let got: Vec<Row> = kept
                    .iter()
                    .map(|k| (k.first, k.last, k.rid, k.waits.clone()))
                    .collect();
                assert_eq!(got, want, "the kept walk, {steps:?}");
                // A fresh walk agrees with the kept one.
                meta.blame_cache.lock().unwrap().clear();
                let fresh = blame_walk(&r, &meta, 7, &node7, u64::MAX).unwrap();
                assert_eq!(fresh, kept, "{steps:?}");
            }
            for theirs in [&node7, &pending] {
                let expect = model(&meta, theirs);
                let p = plan(
                    &r,
                    &meta,
                    &poisoned,
                    &pending,
                    Some(Blame { theirs, rid: None }),
                )
                .unwrap();
                let waiting: BTreeSet<u64> = p
                    .held
                    .iter()
                    .map(|(t, _, _)| t.first)
                    .chain(p.deferred.iter().map(|t| t.first))
                    .collect();
                let want: BTreeSet<u64> = expect
                    .iter()
                    .filter(|e| e.3.is_some())
                    .map(|e| e.0)
                    .collect();
                assert_eq!(waiting, want, "what waits, {steps:?}");
                assert_eq!(
                    p.blame.keys().copied().collect::<BTreeSet<u64>>(),
                    want,
                    "a blame for every waiting transaction, {steps:?}"
                );
                let all: BTreeSet<Ino> = expect
                    .iter()
                    .filter_map(|e| e.3.clone())
                    .flatten()
                    .collect();
                assert_eq!(p.blame_through, all, "{steps:?}");
                for (first, rid, last, blame) in &expect {
                    if let Some(blame) = blame {
                        assert_eq!(&p.blame[first], blame, "tx {first}, {steps:?}");
                        if std::ptr::eq(theirs, &pending) {
                            assert!(!blame.is_empty(), "every wait is some chunk's, {steps:?}");
                        }
                    }
                    if let (Some(rid), true) = (rid, std::ptr::eq(theirs, &node7)) {
                        // Everything waiting up to and including it.
                        let through: BTreeSet<Ino> = expect
                            .iter()
                            .take_while(|e| e.0 <= *first)
                            .filter_map(|e| e.3.clone())
                            .flatten()
                            .collect();
                        let got = deferral_blame(&r, &meta, *rid, theirs).unwrap();
                        assert_eq!(
                            got,
                            DeferralBlame {
                                tx: Some((*last, blame.clone().unwrap_or_default())),
                                through,
                            },
                            "rid {rid:?}, {steps:?}"
                        );
                    }
                }
            }
        }

        /// `ns` of a replica that applied `records` in order, without the
        /// root inode (two replicas' genesis timestamps differ).
        fn state_after(records: &[LogRecord]) -> BTreeMap<Vec<u8>, Vec<u8>> {
            let r = Meta::open_in_memory().unwrap();
            r.apply_records(records).unwrap();
            let mut map: BTreeMap<Vec<u8>, Vec<u8>> = r.ns_dump().unwrap().into_iter().collect();
            map.remove(&constellation_mtree::keys::inode(ROOT_INO));
            map
        }

        /// ADR-59, the commutation argument: what the plan ships ahead of
        /// what it holds or defers leaves the state that journal order
        /// does (the value of every `ns` key), and, once shipped, the
        /// publisher's view over the transactions still held is exactly
        /// the shipped log's prefix (never a deferral).
        fn check_ship_order(steps: &[Step]) {
            let j = journal(steps);
            let meta = &j.meta;
            {
                let r = meta.db.read_tx();
                let poisoned = read_poisoned(&r, meta).unwrap();
                let pending = read_pending(&r, meta).unwrap();
                let p = plan(&r, meta, &poisoned, &pending, None).unwrap();
                let rows = |txs: &mut dyn Iterator<Item = &Tx>| -> Vec<LogRecord> {
                    txs.flat_map(|t| t.rows.iter().map(|(_, r)| r.clone()))
                        .collect()
                };
                let journal_order = rows(&mut transactions(&r, meta).unwrap().iter());
                // What ships goes first; what is held or deferred follows
                // in journal order among themselves.
                let shipping: BTreeSet<u64> = p.ship.iter().map(|t| t.first).collect();
                let all = transactions(&r, meta).unwrap();
                let plan_order = rows(
                    &mut p
                        .ship
                        .iter()
                        .chain(all.iter().filter(|t| !shipping.contains(&t.first))),
                );
                let with = |tail: &[LogRecord]| -> Vec<LogRecord> {
                    j.shipped.iter().chain(tail).cloned().collect()
                };
                assert_eq!(
                    state_after(&with(&journal_order)),
                    state_after(&with(&plan_order)),
                    "shipping ahead of the held is journal order, {steps:?}"
                );
            }
            // Ship what ships, then compare the view with the log.
            let mut after = steps.to_vec();
            after.push(Step::Ship);
            let j = journal(&after);
            let meta = &j.meta;
            let basis = meta
                .read_consistent(|snap| meta.publish_basis_at(snap))
                .unwrap();
            let mut view: BTreeMap<Vec<u8>, Vec<u8>> =
                meta.ns_dump().unwrap().into_iter().collect();
            match &basis {
                crate::PublishBasis::AsIs => {}
                crate::PublishBasis::Substituted(overlay) => {
                    for (k, v) in overlay.iter() {
                        match v {
                            Some(v) => {
                                view.insert(k.to_vec(), v.to_vec());
                            }
                            None => {
                                view.remove(k);
                            }
                        }
                    }
                }
                crate::PublishBasis::Defer => panic!("the publisher defers, {after:?}"),
            }
            view.remove(&constellation_mtree::keys::inode(ROOT_INO));
            assert_eq!(view, state_after(&j.shipped), "the publish view, {after:?}");
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]
            #[test]
            fn every_deferred_transaction_is_blamed_on_exactly_what_defers_it(
                steps in proptest::collection::vec(step(), 1..40)
            ) {
                check(&steps);
            }

            #[test]
            fn ship_order_equals_journal_order(
                steps in proptest::collection::vec(step(), 1..40)
            ) {
                check_ship_order(&steps);
            }

        }
    }
}
