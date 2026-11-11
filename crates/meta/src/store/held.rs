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
    adjust_usage_tx, counter_add_tx, counter_get, counter_set_tx, journal, local, spec, Meta,
    UsageTracker, KV_POISONED_COUNT,
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
    /// manifest, turned into conflict copies as well.
    pub queued_dropped: usize,
    /// Every rid the drop rolled back or refused: the seeds', the
    /// dependents' and the queued replays' (what the oracle treats as the
    /// operator's action, and what a requester's retry may now meet).
    pub rolled_back: Vec<crate::rid::Rid>,
    /// Of `rolled_back`, the rids queued as placeholders (`await_retry`).
    pub placeholders: Vec<crate::rid::Rid>,
    /// ADR-59: of `rolled_back`, the rids that depend on the damaged inode's
    /// own keys: the seeds, the queued replays of its manifest, and the held
    /// transactions that write exclusively a key the seeds or such a
    /// transaction wrote exclusively (a rename, chmod or unlink of the file,
    /// a create of a name it vacated). The operator's cost of a lost chunk
    /// that this drop does not avoid (part B of the ADR is to avoid it). A
    /// rolled-back rid not listed here depended on the seeds only through a
    /// directory's merged attributes or a name unrelated to the file, which
    /// the ship rule no longer orders: the sibling class the ADR removes.
    /// Empty of dependents under the conservative control (nothing is
    /// attributed to the inode).
    pub rolled_back_on_inode: Vec<crate::rid::Rid>,
    /// The dropper was not the sequencer in force: what it rolled back is a
    /// deposed node's strand, whose refusals are fenced.
    pub non_sequencer: bool,
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
/// (`LogRecord::RolledBack`): its `TailOf` pair if it began with one, else
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
    /// also a held or deferred transaction's blame ([`Blame`]). Returns
    /// its class, the poisoned inodes it is a seed for, and its blame.
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
    ) -> (Class, Vec<Ino>, Option<BTreeSet<Ino>>) {
        let seeds = seed_inos(tx, poisoned);
        // What the transaction observed without writing (a refusal's op:
        // `JournalTx::observed`): a dependency like an observation of a
        // written key, but it taints nothing after it. A refusal shipped
        // ahead of the deferred transaction it was refused because of put a
        // refusal in the log that the log's own prefix could not explain
        // (flex-crash seed 481).
        let held = self.opaque
            || !seeds.is_empty()
            || match keys {
                Some(keys) => self.tainted.hit(keys, observed),
                // Unknown keys: held once anything is.
                None => self.any_held,
            };
        let deferred = !held
            && (self.opaque_deferred
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
            Class::Held
        } else if deferred {
            match keys {
                Some(keys) => self.deferred_keys.add(keys),
                None => self.opaque_deferred = true,
            }
            self.any_deferred = true;
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
        let observed: Vec<Vec<u8>> = row.map(|row| row.observed).unwrap_or_default();
        let (class, seeds, mine) = walk.step(
            &tx,
            keys.as_ref(),
            &observed,
            (poisoned, pending),
            blame.map(|b| b.theirs),
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
        let observed = row.map(|row| row.observed).unwrap_or_default();
        let (class, _, waits) = c.walk.step(
            &tx,
            keys.as_ref(),
            &observed,
            (&c.poisoned, &c.pending),
            Some(&c.theirs),
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
        if poisoned.is_empty() && pending.is_empty() {
            drop(r);
            if self.held_any.swap(false, Ordering::Relaxed) {
                *self.held.lock().unwrap() = HeldSummary::default();
            }
            return self.take_journal_whole_txs(max);
        }
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
        let mut batch = Vec::new();
        for tx in plan.ship {
            if !batch.is_empty() && batch.len() >= max {
                break;
            }
            batch.extend(tx.rows);
        }
        Ok(batch)
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
        self.held.lock().unwrap().clone()
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
        self.drop_held_knowing(
            ino,
            now_unix,
            false,
            &|_| false,
            &|_| Vec::new(),
            &LiveSessions::default(),
            true,
        )
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
        self.drop_held_knowing(
            ino,
            now_unix,
            true,
            &|_| false,
            &|_| Vec::new(),
            &LiveSessions::default(),
            true,
        )
    }

    /// `drop_held` / `drop_held_remote` with what only the core knows
    /// (plan 46 M0.2 review): `unanswered(rid)` says that the answer of
    /// another node's op, journaled `RootGroup`/`DelegateNodeGroup`, is certainly
    /// not given — it still waits in the core for its group — so the row
    /// may become a placeholder like an `Archive` one. The offline forms
    /// above know nothing: such a row keeps its op, the only copy that can
    /// execute if the answer left.
    ///
    /// `live_ops(hash)`: the rids of the ops in flight on this node (not
    /// journaled yet, or not executed) whose manifest of `ino` names
    /// `hash`; `sessions`: the chunks the node's open write sessions of
    /// `ino` have sealed. A lost chunk either names keeps its pending row,
    /// poison mark and remote mark (see "Dropping" in the module doc): no
    /// row means *durable* to every ship plan, forward list and manifest
    /// composition, so dropping it under such an op would let the op's
    /// manifest ship naming a chunk S3 never received. The offline forms
    /// have no op in flight.
    #[allow(clippy::too_many_arguments)]
    pub fn drop_held_knowing(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
        unanswered: &dyn Fn(crate::rid::Rid) -> bool,
        live_ops: &dyn Fn(&ChunkHash) -> Vec<crate::rid::Rid>,
        sessions: &LiveSessions,
        exclude_rolled_back: bool,
    ) -> Result<DroppedHeld, MetaError> {
        // The `Refused` records the drop journals are answered by the log
        // alone — nobody is told of them, and a retry meets the record —
        // so they are `Archive` rows, the class the core's check wants set.
        // (A caller that holds a configuration sets its own: the core's drop
        // is the archive's rule at the tenure's version.)
        let _class = super::journal::PendingClass::current()
            .is_none()
            .then(|| super::journal::PendingClass::set(super::local::RowCommit::ARCHIVE));
        let mut tx = self.db.write_tx();
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
        let held = plan(&tx, self, &poisoned, &PoisonMap::new(), None)?;
        // The inode's seeds, and every held transaction that depends on
        // them (key overlap, transitively, in journal order).
        let mut tainted = Taint::default();
        let mut seeds: Vec<(Tx, u64, u64)> = Vec::new(); // (tx, spec_seq, origin)
                                                         // (spec_seq, origin, the keys a placeholder of it holds)
                                                         // (the last: whether it depends on the damaged inode's own keys)
        let mut dependents: Vec<(u64, u64, crate::replay::TouchSet, bool)> = Vec::new();
        let mut rolled_back: Vec<crate::rid::Rid> = Vec::new();
        let mut on_inode: Vec<crate::rid::Rid> = Vec::new();
        // The exclusive keys of the seeds and of what depends on them through
        // such keys (`DroppedHeld::rolled_back_on_inode`). The conservative
        // control folds merges into exclusive keys, which would attribute a
        // directory's siblings to the inode: it attributes nothing.
        let mut own: HashSet<Vec<u8>> = HashSet::new();
        let attribute = !self.parent_taint_conservative.load(Ordering::Relaxed);
        let mut placeholders: Vec<crate::rid::Rid> = Vec::new();
        // The identity of every transaction the drop rolls back, as a
        // member keys it (`LogRecord::RolledBack`).
        let mut idents: Vec<(u64, u64)> = Vec::new();
        let mut unidentified = 0usize;
        for (t, seed_inos, keys) in held.held {
            let is_seed = seed_inos.contains(&ino);
            let depends = keys.as_ref().is_some_and(|keys| tainted.hit(keys, &[]));
            if !is_seed && !depends {
                continue;
            }
            let (Some(spec_seq), Some(keys)) = (t.spec_seq, keys) else {
                return Err(MetaError::Invalid(format!(
                    "held transaction at journal seq {} was journaled without before-images, \
                     so it cannot be rolled back; rebuild this replica",
                    t.first
                )));
            };
            let placeholder = placeholder_keys(&t, &keys);
            let records: Vec<LogRecord> = t.rows.iter().map(|(_, rec)| rec.clone()).collect();
            let head_epoch =
                super::local::get_journal_tx_head(&tx, self, t.first)?.map(|head| head.epoch);
            match rolled_back_ident(head_epoch, t.first, &records) {
                Some(ident) => idents.push(ident),
                // A captured transaction (it has before-images) has its
                // head; one without cannot be named to the members, which
                // keep their copy of it. Said, counted, never silent.
                None => {
                    unidentified += 1;
                    tracing::warn!(
                        first = t.first,
                        "a transaction the drop rolls back has no journal head: \
                         members cannot be told to retract it"
                    );
                }
            }
            let on_this_inode = attribute && (is_seed || keys.excl.iter().any(|k| own.contains(k)));
            if on_this_inode {
                own.extend(keys.excl.iter().cloned());
            }
            tainted.add(&keys);
            let origin = spec::row_keys_and_origin(&tx, self, spec_seq)?
                .map(|row| row.origin)
                .unwrap_or(spec_seq);
            if is_seed {
                seeds.push((t, spec_seq, origin));
            } else {
                dependents.push((spec_seq, origin, placeholder, on_this_inode));
            }
        }
        let mut seqs: HashSet<u64> = dependents.iter().map(|(seq, _, _, _)| *seq).collect();
        seqs.extend(seeds.iter().map(|(_, seq, _)| *seq));
        let staged = UsageTracker::staging();
        spec::strand_seqs_tx(&mut tx, self, &staged, &seqs)?;
        // Plan 46 M0.2 (R1): another node's dependent whose answer was
        // never given is no copy of its op any more, but a placeholder in
        // its place: its requester's retry is the one copy that executes.
        let mut await_retry = 0;
        for (_, origin, keys, on_this_inode) in &dependents {
            if let Some(rid) = spec::queued_at(&tx, self, *origin)?.map(|(rid, _)| rid) {
                rolled_back.push(rid);
                if *on_this_inode {
                    on_inode.push(rid);
                }
            }
            if let Some(rid) = spec::mark_await_retry_tx(
                &mut tx,
                self,
                *origin,
                keys.clone(),
                now_unix.saturating_mul(1000),
                unanswered,
            )? {
                await_retry += 1;
                placeholders.push(rid);
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
        for (t, _, origin) in &seeds {
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
            let rid = match spec::queued_at(&tx, self, *origin)? {
                Some((rid, _)) => rid,
                None => spec::replay_rid_for(&tx, self, t.first)?,
            };
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
            on_inode.push(rid);
        }
        // Replays of this inode's manifest already queued by an earlier
        // deposition: the same treatment.
        let mut queued_dropped = 0;
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
            let op = MutateOp::SetManifest {
                ino,
                base_manifest: None,
                manifest: sanitize_manifest(manifest, &missing).unwrap_or_default(),
                size,
                mtime_ns: None,
                ctime_ns: None,
            };
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
            on_inode.push(queued.rid);
            queued_dropped += 1;
        }
        // Fix "capture under an epoch hold": the dropped ops' outcome, in
        // the log. Before this the requester of a dropped op (a member
        // whose epoch write the owner dropped, or a deposed owner that
        // replayed it) kept its shadow for good: no `Completed` was ever
        // coming, and only a takeover would have stranded it.
        if !refused_rids.is_empty() || !idents.is_empty() {
            let local = self.begin_local(&tx)?;
            let now_ms = now_unix.saturating_mul(1000);
            // The members' and the backups' copies of what was rolled back
            // are retracted by this record (defect B): the first record of
            // the transaction, ahead of every refusal. Its place in the
            // journal is what makes a holder's death mid-drop safe: a
            // successor has the record or none of the drop's effects.
            if !idents.is_empty() {
                journal::append_tx(
                    &mut tx,
                    &self.journal_ks,
                    &self.local,
                    &self.completed,
                    &LogRecord::RolledBack {
                        idents: idents.clone(),
                    },
                )?;
            }
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
            if exclude_rolled_back {
                live.retain(|rid| !rolled_back.contains(rid));
            }
            if !live.is_empty() || sessions.names(hash) {
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
        tx.commit()?;
        self.report_rolled_back();
        staged.drain_into(self.usage_tracker());
        *self.held.lock().unwrap() = HeldSummary::default();
        tracing::warn!(
            ino,
            dropped = seeds.len(),
            requeued = dependents.len(),
            await_retry,
            queued_dropped,
            pending_removed,
            kept = kept_pending.len(),
            "held records dropped into conflict copies"
        );
        kept_for.sort();
        kept_for.dedup();
        Ok(DroppedHeld {
            ino,
            dropped: seeds.len(),
            requeued: dependents.len(),
            await_retry,
            pending_removed,
            queued_dropped,
            rolled_back,
            placeholders,
            rolled_back_on_inode: on_inode,
            non_sequencer: !exclude_rolled_back,
            missing,
            kept_pending,
            kept_for,
            unidentified,
        })
    }
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
