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
//! - a transaction is **held** if it is a seed, or if it touches an `ns`
//!   key a held transaction touched — its key set is its captured
//!   before-image keys (`store::spec`), exactly what the M3b capture
//!   recorded. Held keys accumulate, so the rule is transitive;
//! - everything else ships, in journal order, skipping the held ones.
//!
//! Only earlier transactions can be depended on, and a shipped
//! transaction touches no held key, so shipping it ahead of the held ones
//! yields the same state as shipping in journal order would once they
//! ship: the two commute. An **uncaptured** transaction (holder capture
//! off) has no known key set: once anything is held, it and everything
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
//!   skips because it touches none of the held keys;
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
use constellation_fs_core::{ChunkHash, ChunkInfo, Ino, Manifest};
use fjall::Readable;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::Ordering;

const POISON_PREFIX: &[u8] = b"poisoned/";

/// The code a dropped op's `Refused` record carries (`EIO`): the write
/// was acknowledged and then lost with its chunk; its conflict copy is
/// the artifact.
const DROPPED_CODE: constellation_types::Code = constellation_types::Code::Io;

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
                crate::store::misc::add_pending_claim_tx(tx, &meta.pending_upload, hash, ino)?;
            }
        }
        ChunkInfo::Spilled(blob) => {
            crate::store::misc::add_pending_claim_tx(tx, &meta.pending_upload, &blob, ino)?;
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
            crate::store::misc::add_pending_claim_tx(&mut tx, &self.pending_upload, hash, ino)?;
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
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeldInode {
    pub missing: Vec<ChunkHash>,
    pub seeds: u64,
}

/// What `repair drop-held` did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DroppedHeld {
    /// Seed transactions dropped into conflict copies.
    pub dropped: usize,
    /// Dependent transactions rolled back and queued for replay by rid.
    pub requeued: usize,
    /// Unrecoverable pending-upload rows removed.
    pub pending_removed: usize,
    /// Queued replays (from an earlier deposition) of this inode's
    /// manifest, turned into conflict copies as well.
    pub queued_dropped: usize,
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
    let rows = journal::take(r, &meta.journal_ks, &meta.local, usize::MAX)?;
    let heads: BTreeMap<u64, local::JournalTxHead> =
        local::read_journal_tx_heads(r, meta)?.into_iter().collect();
    let mut out: Vec<Tx> = Vec::new();
    for (seq, rec) in rows {
        if let Some(open) = out.last_mut() {
            if seq <= open.last {
                open.rows.push((seq, rec));
                continue;
            }
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
    let mut out = Vec::new();
    for (_, rec) in &tx.rows {
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

/// A held-back transaction: the transaction, the poisoned inodes it is a
/// seed for (empty for a dependent), and its key set (`None`: uncaptured).
type Held = (Tx, Vec<Ino>, Option<Vec<Vec<u8>>>);

/// A ship plan: the transactions to ship (in order), those held back
/// behind lost chunks, and those deferred behind chunks still uploading.
struct Plan {
    ship: Vec<Tx>,
    held: Vec<Held>,
    deferred: Vec<Tx>,
    opaque: bool,
}

/// See the module doc's "What is held" and "Deferred". A transaction is
/// *held* if it is a poisoned seed or depends on a held one; otherwise
/// *deferred* if it is a pending seed or depends on anything held or
/// deferred; otherwise it ships. Taint accumulates per kind, so a
/// transaction behind both is reported as held.
fn plan(
    r: &impl Readable,
    meta: &Meta,
    poisoned: &PoisonMap,
    pending: &PoisonMap,
) -> Result<Plan, MetaError> {
    let mut tainted: HashSet<Vec<u8>> = HashSet::new();
    let mut deferred_keys: HashSet<Vec<u8>> = HashSet::new();
    let mut opaque = false;
    let mut opaque_deferred = false;
    let mut out = Plan {
        ship: Vec::new(),
        held: Vec::new(),
        deferred: Vec::new(),
        opaque: false,
    };
    for tx in transactions(r, meta)? {
        let seeds = seed_inos(&tx, poisoned);
        let keys = match tx.spec_seq {
            Some(seq) => spec::row_keys_and_origin(r, meta, seq)?.map(|(keys, _)| keys),
            None => None,
        };
        // What the transaction observed without writing (a refusal's op:
        // `JournalTx::observed`): a dependency like a written key, but
        // it taints nothing after it. A refusal shipped ahead of the
        // deferred transaction it was refused because of put a refusal
        // in the log that the log's own prefix could not explain
        // (flex-crash seed 481).
        let observed: Vec<Vec<u8>> = local::get_journal_tx(r, meta, tx.first)?
            .map(|row| row.observed)
            .unwrap_or_default();
        let held = opaque
            || !seeds.is_empty()
            || observed.iter().any(|k| tainted.contains(k))
            || match &keys {
                Some(keys) => keys.iter().any(|k| tainted.contains(k)),
                // Unknown keys: held once anything is.
                None => !out.held.is_empty(),
            };
        if held {
            match &keys {
                Some(keys) => tainted.extend(keys.iter().cloned()),
                None => opaque = true,
            }
            out.held.push((tx, seeds, keys));
            continue;
        }
        let deferred = opaque_deferred
            || !seed_inos(&tx, pending).is_empty()
            || observed.iter().any(|k| deferred_keys.contains(k))
            || match &keys {
                Some(keys) => keys.iter().any(|k| deferred_keys.contains(k)),
                None => !out.deferred.is_empty(),
            };
        if deferred {
            match &keys {
                Some(keys) => deferred_keys.extend(keys.iter().cloned()),
                None => opaque_deferred = true,
            }
            out.deferred.push(tx);
            continue;
        }
        out.ship.push(tx);
    }
    out.opaque = opaque;
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
        let plan = plan(&r, self, &poisoned, &pending)?;
        // Whether rows may be skipped: `journal_through_after` must not
        // claim a skipped row shipped.
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
    pub fn held_summary(&self) -> HeldSummary {
        self.held.lock().unwrap().clone()
    }

    /// `constellation repair drop-held <ino>` (see the module doc's
    /// "Dropping"). `now_unix` stamps the conflict copies' names.
    pub fn drop_held(&self, ino: Ino, now_unix: i64) -> Result<DroppedHeld, MetaError> {
        self.drop_held_with(ino, now_unix, false)
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
        self.drop_held_with(ino, now_unix, true)
    }

    fn drop_held_with(
        &self,
        ino: Ino,
        now_unix: i64,
        remote: bool,
    ) -> Result<DroppedHeld, MetaError> {
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
        let held = plan(&tx, self, &poisoned, &PoisonMap::new())?;
        // The inode's seeds, and every held transaction that depends on
        // them (key overlap, transitively, in journal order).
        let mut tainted: HashSet<Vec<u8>> = HashSet::new();
        let mut seeds: Vec<(Tx, u64, u64)> = Vec::new(); // (tx, spec_seq, origin)
        let mut dependents: Vec<u64> = Vec::new(); // spec_seqs
        for (t, seed_inos, keys) in held.held {
            let is_seed = seed_inos.contains(&ino);
            let depends = keys
                .as_ref()
                .is_some_and(|keys| keys.iter().any(|k| tainted.contains(k)));
            if !is_seed && !depends {
                continue;
            }
            let (Some(spec_seq), Some(keys)) = (t.spec_seq, keys) else {
                return Err(MetaError::Invalid(format!(
                    "held transaction at journal seq {} was not captured (holder capture \
                     off), so it cannot be rolled back; turn CONSTELLATION_HOLDER_CAPTURE on \
                     and remount, or rebuild this replica",
                    t.first
                )));
            };
            tainted.extend(keys);
            let origin = spec::row_keys_and_origin(&tx, self, spec_seq)?
                .map(|(_, origin)| origin)
                .unwrap_or(spec_seq);
            if is_seed {
                seeds.push((t, spec_seq, origin));
            } else {
                dependents.push(spec_seq);
            }
        }
        let mut seqs: HashSet<u64> = dependents.iter().copied().collect();
        seqs.extend(seeds.iter().map(|(_, seq, _)| *seq));
        let staged = UsageTracker::staging();
        spec::strand_seqs_tx(&mut tx, self, &staged, &seqs)?;
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
            };
            spec::refuse_queued_tx(&mut tx, self, *origin, rid, op, refusal(&missing))?;
            refused_rids.push(rid);
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
            };
            spec::refuse_queued_tx(
                &mut tx,
                self,
                queued.queue_seq,
                queued.rid,
                op,
                refusal(&missing),
            )?;
            refused_rids.push(queued.rid);
            queued_dropped += 1;
        }
        // Fix "capture under an epoch hold": the dropped ops' outcome, in
        // the log. Before this the requester of a dropped op (a member
        // whose epoch write the owner dropped, or a deposed owner that
        // replayed it) kept its shadow for good: no `Completed` was ever
        // coming, and only a takeover would have stranded it.
        if !refused_rids.is_empty() {
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
        for hash in &missing {
            if tx
                .get(&self.pending_upload, pending_key(hash, ino))?
                .is_some()
            {
                tx.remove(&self.pending_upload, pending_key(hash, ino));
                pending_removed += 1;
            }
            if tx.get(&self.local, poison_key(hash, ino))?.is_some() {
                tx.remove(&self.local, poison_key(hash, ino));
                marks_removed += 1;
            }
            // A row another node was to upload (`--remote`): its mark
            // goes with the row.
            let remote = super::remote::remote_key(hash, ino);
            if tx.get(&self.local, &remote)?.is_some() {
                tx.remove(&self.local, remote);
            }
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
        staged.drain_into(self.usage_tracker());
        *self.held.lock().unwrap() = HeldSummary::default();
        tracing::warn!(
            ino,
            dropped = seeds.len(),
            requeued = dependents.len(),
            queued_dropped,
            pending_removed,
            "held records dropped into conflict copies"
        );
        Ok(DroppedHeld {
            dropped: seeds.len(),
            requeued: dependents.len(),
            pending_removed,
            queued_dropped,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hole_replaces_each_lost_chunk() {
        let a = ChunkHash::of(b"a");
        let b = ChunkHash::of(b"b");
        let chunks = BTreeMap::from([(0u64, a), (1u64, b)]);
        let m = Manifest {
            layout: constellation_fs_core::ChunkLayout::new(4),
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
}
