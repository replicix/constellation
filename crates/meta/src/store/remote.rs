//! Chunks a forwarded manifest names that are still uploading on the node
//! that forwarded it (the small-file write-path fix: `--write-mode back`
//! for non-owners).
//!
//! # Why
//!
//! A manifest may only reach the log once every chunk it names is in S3
//! (`store::held`'s module doc). A non-owner used to guarantee that by
//! uploading its chunks *before* forwarding the manifest to the
//! sequencer, in both write modes, so its `close()` always paid the S3
//! round trip. Under `--write-mode back` it now forwards at once and says
//! which of the manifest's chunks are still pending on it. The sequencer
//! enrolls exactly those as pending uploads of its own, marked **remote**
//! here, in one transaction, *before* it executes the op. From then on the
//! chunks are ordinary pending rows to every gate that already exists:
//!
//! - the root's ship plan defers the transaction (and whatever depends on
//!   it) until the rows are acked (`store::held`, plan 30 §M7);
//! - a delegate does not stream (or back up) a transaction naming a
//!   pending chunk ([`Meta::delegate_txs_from`]);
//! - the pre-S3 stream stops before such a transaction
//!   ([`Meta::releasable_prefix`]).
//!
//! What is new is only how the rows get acked: the sequencer cannot
//! upload bytes it does not have. The forwarding node, once its own
//! upload pass has put a chunk up, tells the node it forwarded to
//! ([`Meta::ack_remote_chunks`]); as a fallback (the message was lost, the
//! forwarder restarted) the sequencer's upload pass checks S3 for the chunk
//! itself, with backoff. A reader on the sequencer that meets such a
//! chunk before it is durable waits for it ([`Meta::awaits_remote_chunk`])
//! instead of failing its fetch.
//!
//! # Layout
//!
//! `local`'s `remote-chunk/<hash(32)><ino(8 BE)>` → `node(8 BE) ++
//! enrolled_ms(8 BE)`, beside the `pending_upload` row it qualifies. A
//! mark without its row is stale (the row was acked or withdrawn) and
//! means nothing; every reader checks both.
//!
//! `local`'s counter `remote_marks/<node>` counts the node's marks, stale
//! ones included (every mark is written and removed through
//! `put_mark_tx`/`remove_mark_tx`, which keep it). The sequencer asks "does
//! this forwarder have anything pending here?" for every op it answers
//! ([`Meta::remote_blockers`]); with nothing, the answer is that one point
//! read rather than a scan of every node's marks.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::held::{blame_walk, seed_inos_of, DeferralBlame, PoisonMap};
use crate::store::misc::{add_pending_claim_tx, cr_key, remove_pending_row_tx};
use crate::store::{counter_add_tx, counter_get, local, Meta};
use constellation_fs_core::{ChunkHash, ChunkInfo, Ino, Manifest};
use fjall::Readable;
use std::collections::BTreeSet;

const REMOTE_PREFIX: &[u8] = b"remote-chunk/";

pub(crate) fn remote_key(hash: &ChunkHash, ino: Ino) -> Vec<u8> {
    let mut k = REMOTE_PREFIX.to_vec();
    k.extend_from_slice(&hash.0);
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

/// The counter of `node`'s marks (a stale one included).
fn mark_count_key(node: u64) -> String {
    format!("{MARK_COUNT_PREFIX}{node}")
}

const MARK_COUNT_PREFIX: &str = "remote_marks/";

/// Write the mark of `(hash, ino)` as `node`'s, keeping the counts.
fn put_mark_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    hash: &ChunkHash,
    ino: Ino,
    node: u64,
    value: Vec<u8>,
) -> Result<(), MetaError> {
    let key = remote_key(hash, ino);
    if let Some(old) = tx.get(&meta.local, &key)? {
        if let Some(mark) = decode_mark(&key, &old) {
            counter_add_tx(tx, &meta.local, &mark_count_key(mark.node), -1)?;
        }
    }
    tx.insert(&meta.local, key, value);
    counter_add_tx(tx, &meta.local, &mark_count_key(node), 1)
}

/// Remove the mark under `key` (if any), keeping the counts. Returns it.
fn remove_mark_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    key: &[u8],
) -> Result<Option<RemoteChunk>, MetaError> {
    let Some(value) = tx.get(&meta.local, key)? else {
        return Ok(None);
    };
    tx.remove(&meta.local, key.to_vec());
    let mark = decode_mark(key, &value);
    if let Some(mark) = &mark {
        counter_add_tx(tx, &meta.local, &mark_count_key(mark.node), -1)?;
    }
    Ok(mark)
}

/// Remove `(hash, ino)`'s mark (if any) in `tx`, keeping the counts.
pub(crate) fn forget_remote_mark_tx(
    tx: &mut fjall::SingleWriterWriteTx,
    meta: &Meta,
    hash: &ChunkHash,
    ino: Ino,
) -> Result<(), MetaError> {
    remove_mark_tx(tx, meta, &remote_key(hash, ino)).map(|_| ())
}

fn remote_hash_prefix(hash: &ChunkHash) -> Vec<u8> {
    let mut k = REMOTE_PREFIX.to_vec();
    k.extend_from_slice(&hash.0);
    k
}

/// One remote-pending row: the chunk, the inode whose manifest names it,
/// the node expected to upload it, and when it was enrolled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteChunk {
    pub hash: ChunkHash,
    pub ino: Ino,
    pub node: u64,
    pub enrolled_ms: i64,
}

fn decode_mark(key: &[u8], value: &[u8]) -> Option<RemoteChunk> {
    let rest = key.get(REMOTE_PREFIX.len()..)?;
    if rest.len() != 40 || value.len() < 16 {
        return None;
    }
    Some(RemoteChunk {
        hash: ChunkHash(rest[..32].try_into().ok()?),
        ino: u64::from_be_bytes(rest[32..].try_into().ok()?),
        node: u64::from_be_bytes(value[..8].try_into().ok()?),
        enrolled_ms: i64::from_be_bytes(value[8..16].try_into().ok()?),
    })
}

/// Pending uploads, looked up per `(hash, ino)` row, and per inode
/// through the `pending_upload_by_ino` mirror when a spilled manifest
/// (whose chunk list is not in the record) needs "every pending row of
/// this inode".
pub(crate) struct PendingView<'a, R: Readable> {
    r: &'a R,
    meta: &'a Meta,
    empty: bool,
}

impl<'a, R: Readable> PendingView<'a, R> {
    pub(crate) fn new(r: &'a R, meta: &'a Meta) -> Result<Self, MetaError> {
        let empty = match r.iter(&meta.pending_upload).next() {
            None => true,
            Some(guard) => {
                guard.into_inner()?;
                false
            }
        };
        Ok(Self { r, meta, empty })
    }

    fn row(&self, hash: &ChunkHash, ino: Ino) -> Result<bool, MetaError> {
        Ok(self
            .r
            .get(&self.meta.pending_upload, cr_key(hash, ino))?
            .is_some())
    }

    /// Whether any `WriteManifest` in `records` names a chunk that still
    /// has a pending upload row for its inode (a spilled or undecodable
    /// manifest: whether its inode has any pending row at all).
    pub(crate) fn names_pending(&mut self, records: &[LogRecord]) -> Result<bool, MetaError> {
        self.names_pending_except(records, None)
    }

    /// A pending `(hash, ino)` row that `node` does not hold the bytes
    /// of: any row, unless it is marked remote *from* `node` (the node
    /// that forwarded the manifest while uploading them).
    fn foreign_row(
        &self,
        hash: &ChunkHash,
        ino: Ino,
        node: Option<u64>,
    ) -> Result<bool, MetaError> {
        if !self.row(hash, ino)? {
            return Ok(false);
        }
        let Some(node) = node else {
            return Ok(true);
        };
        let key = remote_key(hash, ino);
        Ok(match self.r.get(&self.meta.local, &key)? {
            Some(v) => decode_mark(&key, &v).is_none_or(|m| m.node != node),
            None => true,
        })
    }

    /// [`Self::names_pending`], ignoring the rows `node` has the bytes of
    /// (see [`Self::foreign_row`]).
    pub(crate) fn names_pending_except(
        &mut self,
        records: &[LogRecord],
        node: Option<u64>,
    ) -> Result<bool, MetaError> {
        if self.empty {
            return Ok(false);
        }
        for rec in records {
            let LogRecord::WriteManifest { ino, manifest, .. } = rec else {
                continue;
            };
            let named: Vec<ChunkHash> = match Manifest::decode(manifest).map(|m| m.chunks) {
                Ok(ChunkInfo::Inline(chunks)) => chunks.into_values().collect(),
                // The list is not in the record (or the record is
                // unreadable): every pending row of the inode counts.
                _ => crate::store::misc::pending_for_ino_tx(self.r, self.meta, *ino)?,
            };
            for hash in &named {
                if self.foreign_row(hash, *ino, node)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// The chunks `ino`'s live remote-pending rows name (marks with their
/// pending row), for `repair drop-held --remote`.
pub(crate) fn remote_pending_for_ino_tx(
    r: &impl Readable,
    meta: &Meta,
    ino: Ino,
) -> Result<Vec<ChunkHash>, MetaError> {
    let mut out = Vec::new();
    for guard in r.prefix(&meta.local, REMOTE_PREFIX) {
        let (k, v) = guard.into_inner()?;
        let Some(mark) = decode_mark(&k, &v) else {
            continue;
        };
        if mark.ino == ino
            && r.get(&meta.pending_upload, cr_key(&mark.hash, mark.ino))?
                .is_some()
        {
            out.push(mark.hash);
        }
    }
    Ok(out)
}

/// `node`'s live remote-pending rows (marks with their pending row), per
/// inode.
pub(crate) fn remote_pending_from_tx(
    r: &impl Readable,
    meta: &Meta,
    node: u64,
) -> Result<PoisonMap, MetaError> {
    let mut out = PoisonMap::new();
    if counter_get(r, &meta.local, &mark_count_key(node))? == 0 {
        return Ok(out);
    }
    for guard in r.prefix(&meta.local, REMOTE_PREFIX) {
        let (k, v) = guard.into_inner()?;
        let Some(mark) = decode_mark(&k, &v) else {
            continue;
        };
        if mark.node == node
            && r.get(&meta.pending_upload, cr_key(&mark.hash, mark.ino))?
                .is_some()
        {
            out.entry(mark.ino).or_default().insert(mark.hash);
        }
    }
    Ok(out)
}

/// What a forwarded op's transaction, and the position it was evaluated
/// at, wait for from the node that forwarded it
/// ([`Meta::remote_blockers`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteBlockers {
    /// The inodes whose chunks that node forwarded as pending, and has not
    /// reported up, that the transaction, or any transaction journaled
    /// before it, cannot leave this node before: empty when nothing there
    /// waits for that node.
    pub inos: Vec<Ino>,
    /// The transaction's last journal seq, when it is still unshipped
    /// here.
    pub through: Option<u64>,
    /// Chunk metered-own-rows: given the reply's position, the node's own
    /// transactions through it and the rest's blame
    /// (`constellation_meta::OwnRows`). `None`: not worked out (no
    /// position, a delegate's execution, nothing of the node's pending
    /// here, more than `OWN_ROWS_CAP` of its transactions).
    pub own: Option<crate::mutate::OwnRows>,
}

/// [`Meta::remote_blockers`] for a delegate: the inodes of `theirs` that
/// the transactions of stream `gen` up to `rid`'s (in stream order) name.
fn delegate_blame(
    r: &impl Readable,
    meta: &Meta,
    gen: u64,
    rid: crate::rid::Rid,
    theirs: &PoisonMap,
) -> Result<DeferralBlame, MetaError> {
    let mut txs: Vec<(u64, local::JournalTx)> = local::read_journal_txs(r, meta)?
        .into_iter()
        .filter(|(_, row)| row.gen == gen)
        .collect();
    txs.sort_by_key(|(_, row)| row.idx);
    let mut blame = BTreeSet::new();
    for (first, row) in txs {
        let records = local::journal_records(r, meta, first, row.last)?;
        blame.extend(seed_inos_of(records.iter().map(|(_, rec)| rec), theirs));
        if row.rid == Some(rid) {
            return Ok(DeferralBlame {
                tx: Some((row.last, blame.clone())),
                through: blame,
            });
        }
    }
    Ok(DeferralBlame {
        tx: None,
        through: blame,
    })
}

impl Meta {
    /// Enroll `hashes` — chunks node `node` forwarded a manifest of `ino`
    /// naming while they were still uploading there — as pending uploads
    /// of this node, marked remote. One transaction; call it before the op
    /// executes, so no journal row naming them can exist without them.
    pub fn enroll_remote_chunks(
        &self,
        ino: Ino,
        hashes: &[ChunkHash],
        node: u64,
    ) -> Result<(), MetaError> {
        if hashes.is_empty() {
            return Ok(());
        }
        let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
        let mut value = node.to_be_bytes().to_vec();
        value.extend_from_slice(&now_ms.to_be_bytes());
        let mut tx = self.db.write_tx();
        for hash in hashes {
            add_pending_claim_tx(&mut tx, self, hash, ino)?;
            put_mark_tx(&mut tx, self, hash, ino, node, value.clone())?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Chunk metered-own-rows (review nit): recount every node's marks and
    /// rewrite a count that disagrees (`Meta::open`). A count of 0 is the
    /// holder's "nothing of that node's is pending here" answer
    /// ([`Self::remote_blockers`]' one point read); with live marks behind
    /// it the forwarder would never be asked to upload them, and its ops
    /// would stall silently — the counts saturate at 0, so one lost
    /// increment would do it. Stale marks count, as everywhere. Returns
    /// how many counts were rewritten.
    pub(crate) fn verify_mark_counts(&self) -> Result<usize, MetaError> {
        let mut tx = self.db.write_tx();
        let mut want: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        for guard in tx.prefix(&self.local, REMOTE_PREFIX) {
            let (k, v) = guard.into_inner()?;
            if let Some(mark) = decode_mark(&k, &v) {
                *want.entry(mark.node).or_default() += 1;
            }
        }
        let mut have: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        for guard in tx.prefix(&self.local, MARK_COUNT_PREFIX) {
            let (k, _) = guard.into_inner()?;
            let node = std::str::from_utf8(&k[MARK_COUNT_PREFIX.len()..])
                .ok()
                .and_then(|n| n.parse::<u64>().ok());
            if let Some(node) = node {
                have.insert(node, counter_get(&tx, &self.local, &mark_count_key(node))?);
            }
        }
        let mut fixed = 0;
        for node in want
            .keys()
            .chain(have.keys())
            .copied()
            .collect::<BTreeSet<u64>>()
        {
            let (w, h) = (
                want.get(&node).copied().unwrap_or(0),
                have.get(&node).copied().unwrap_or(0),
            );
            if w != h {
                tracing::warn!(
                    node,
                    marks = w,
                    counted = h,
                    "a forwarder's remote-mark count disagreed with its marks: rewritten"
                );
                if w == 0 {
                    tx.remove(&self.local, mark_count_key(node).into_bytes());
                } else {
                    crate::store::counter_set_tx(&mut tx, &self.local, &mark_count_key(node), w);
                }
                fixed += 1;
            }
        }
        if fixed > 0 {
            tx.commit()?;
        }
        Ok(fixed)
    }

    /// The remote mark of `(hash, ino)`'s pending row, if the row exists
    /// and is marked.
    pub fn remote_chunk(
        &self,
        hash: &ChunkHash,
        ino: Ino,
    ) -> Result<Option<RemoteChunk>, MetaError> {
        let r = self.db.read_tx();
        let key = remote_key(hash, ino);
        let Some(value) = r.get(&self.local, &key)? else {
            return Ok(None);
        };
        if r.get(&self.pending_upload, cr_key(hash, ino))?.is_none() {
            return Ok(None);
        }
        Ok(decode_mark(&key, &value))
    }

    /// Whether some pending row of `hash` waits for another node's upload:
    /// the chunk is not in S3 yet, and whoever needs its bytes here waits
    /// for it rather than failing the fetch.
    pub fn awaits_remote_chunk(&self, hash: &ChunkHash) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        for guard in r.prefix(&self.local, remote_hash_prefix(hash)) {
            let (k, v) = guard.into_inner()?;
            if let Some(mark) = decode_mark(&k, &v) {
                if r.get(&self.pending_upload, cr_key(&mark.hash, mark.ino))?
                    .is_some()
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Every live remote-pending row (for `status`).
    pub fn remote_chunks(&self) -> Result<Vec<RemoteChunk>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.prefix(&self.local, REMOTE_PREFIX) {
            let (k, v) = guard.into_inner()?;
            if let Some(mark) = decode_mark(&k, &v) {
                if r.get(&self.pending_upload, cr_key(&mark.hash, mark.ino))?
                    .is_some()
                {
                    out.push(mark);
                }
            }
        }
        Ok(out)
    }

    /// Another node reports `hashes` durable in S3 (it uploaded them, or
    /// found them there under the condemned-pointer rule): ack every
    /// remote-marked row of them. Rows this node enrolled for its own
    /// uploads are left to its own pass. Returns the rows acked.
    pub fn ack_remote_chunks(
        &self,
        hashes: &[ChunkHash],
    ) -> Result<Vec<(ChunkHash, Ino)>, MetaError> {
        let mut acked = Vec::new();
        let mut tx = self.db.write_tx();
        for hash in hashes {
            let marks: Vec<Vec<u8>> = tx
                .prefix(&self.local, remote_hash_prefix(hash))
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
                .collect::<Result<_, _>>()?;
            for k in marks {
                if let Some(mark) = remove_mark_tx(&mut tx, self, &k)? {
                    let row = cr_key(&mark.hash, mark.ino);
                    if tx.get(&self.pending_upload, &row)?.is_some() {
                        remove_pending_row_tx(&mut tx, self, &mark.hash, mark.ino);
                        acked.push((mark.hash, mark.ino));
                    }
                }
            }
        }
        tx.commit()?;
        Ok(acked)
    }

    /// Drop `(hash, ino)`'s remote mark (its row was acked by an upload).
    pub(crate) fn forget_remote_mark(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        if self.local.get(remote_key(hash, ino))?.is_none() {
            return Ok(());
        }
        let mut tx = self.db.write_tx();
        forget_remote_mark_tx(&mut tx, self, hash, ino)?;
        tx.commit()?;
        Ok(())
    }

    /// Every remote mark (with the pending rows: see
    /// `Meta::clear_pending_uploads`).
    pub(crate) fn clear_remote_marks_tx(
        &self,
        tx: &mut fjall::SingleWriterWriteTx,
    ) -> Result<(), MetaError> {
        let marks: Vec<Vec<u8>> = tx
            .prefix(&self.local, REMOTE_PREFIX)
            .chain(tx.prefix(&self.local, MARK_COUNT_PREFIX))
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in marks {
            tx.remove(&self.local, k);
        }
        Ok(())
    }

    /// How many of `txs` (in journal order) may leave this node ahead of
    /// S3 — to the pre-S3 stream's subscriber `subscriber`, or to every
    /// subscriber (`None`) — before the first one whose manifest names a
    /// chunk that is still pending here: a subscriber that applied it
    /// would find the chunk in neither S3 nor any peer that serves it
    /// (dirty chunks are never served). A chunk marked remote from
    /// `subscriber` itself does not stop it: that node has the bytes.
    pub fn releasable_prefix(
        &self,
        txs: &[crate::store::backup::BackupTx],
        subscriber: Option<u64>,
    ) -> Result<usize, MetaError> {
        let r = self.db.read_tx();
        let mut view = PendingView::new(&r, self)?;
        for (i, tx) in txs.iter().enumerate() {
            if view.names_pending_except(&tx.records, subscriber)? {
                return Ok(i);
            }
        }
        Ok(txs.len())
    }

    /// Chunk close-stall-metered: the inodes whose chunks `node`
    /// forwarded as pending (its `back` close's manifest, enrolled by
    /// [`Self::enroll_remote_chunks`]) and has not reported up yet that
    /// `rid`'s transaction — just executed here for `node`, with
    /// `records` — cannot leave this node before. Those its own manifests
    /// name, and those of every earlier transaction it depends on:
    ///
    /// - as the root (`gen` 0), the ship plan defers a transaction that
    ///   touches a key a deferred one touched (`store::held`, plan 30
    ///   §M4/§M7), so a `chmod` right after a `back` close of the same file
    ///   waits for the close's chunks as much as the close does;
    /// - as a delegate (`gen` ≠ 0), the stream to the root stops at the
    ///   first transaction naming a pending chunk and keeps its order
    ///   ([`Self::delegate_txs_from`]): everything up to `rid`'s in the
    ///   generation's stream counts.
    ///
    /// Chunk close-stall-followup: and those of every transaction
    /// journaled before it that cannot leave this node before them, its
    /// dependency or not. The reply carries the position the op was
    /// evaluated at, all of this journal through its transaction, and a
    /// node that observes it (an op that completed through the log, a
    /// refusal) waits in every later read until its log reaches it. A
    /// close of `node`'s answered at once, its chunk still held there,
    /// keeps every segment's `through` below its row; `node`'s reads then
    /// waited out the session budget, again and again, until the
    /// watermark's TTL (4–10 s probes on AWS).
    ///
    /// The sequencer's answer to the forward names them (`OwnChunks`), so
    /// a forwarder whose uploads are held (a metered network) uploads
    /// them when it must wait for the transaction or its position. One
    /// point read (the node's mark count) when `node` has no mark here
    /// (the common case).
    ///
    /// Chunk metered-own-rows: with `upto` (the journal seq of the
    /// position the reply carries), through that position, and also which
    /// of those transactions are `node`'s own and what each waits for
    /// ([`RemoteBlockers::own`]); without it, through the rid's
    /// transaction, or the whole journal when it has none. The walk is
    /// kept per node between calls (`held::blame_walk`).
    pub fn remote_blockers(
        &self,
        rid: crate::rid::Rid,
        gen: u64,
        records: &[LogRecord],
        node: u64,
        upto: Option<u64>,
    ) -> Result<RemoteBlockers, MetaError> {
        let r = self.db.read_tx();
        let theirs = remote_pending_from_tx(&r, self, node)?;
        if theirs.is_empty() {
            // Nothing of the node's pending here: its walk is moot (and
            // would otherwise stay cached, see `held::blame_walk`).
            self.blame_cache.lock().unwrap().remove(&node);
            return Ok(RemoteBlockers::default());
        }
        let mut inos: BTreeSet<Ino> = seed_inos_of(records, &theirs).into_iter().collect();
        if gen != 0 {
            let found = delegate_blame(&r, self, gen, rid, &theirs)?;
            let through = found.tx.as_ref().map(|(last, _)| *last);
            inos.extend(found.through);
            return Ok(RemoteBlockers {
                inos: inos.into_iter().collect(),
                through,
                own: None,
            });
        }
        let kept = blame_walk(&r, self, node, &theirs, upto.unwrap_or(u64::MAX))?;
        let found = kept.iter().find(|k| k.rid == Some(rid));
        let through = found.map(|k| k.last);
        // Without a position, through the rid's transaction (or all).
        let bound = match (upto, found) {
            (Some(upto), _) => upto,
            (None, Some(k)) => k.last,
            (None, None) => u64::MAX,
        };
        let mut others: BTreeSet<Ino> = BTreeSet::new();
        let mut own = Vec::new();
        for k in kept.iter().filter(|k| k.first <= bound) {
            let waits = k.waits.clone().unwrap_or_default();
            inos.extend(&waits);
            match k.rid {
                Some(r) if r.node == node => own.push(crate::mutate::OwnTx {
                    rid: r,
                    first: k.first,
                    last: k.last,
                    effect: k.effect,
                    waits: waits.into_iter().collect(),
                }),
                _ => others.extend(waits),
            }
        }
        let own = (upto.is_some() && own.len() <= crate::mutate::OWN_ROWS_CAP).then(|| {
            crate::mutate::OwnRows {
                txs: own,
                others: others.into_iter().collect(),
            }
        });
        Ok(RemoteBlockers {
            inos: inos.into_iter().collect(),
            through,
            own,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct File {
        ino: Ino,
    }

    fn manifest_of(hashes: &[ChunkHash]) -> Vec<u8> {
        let chunks = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (i as u64, *h))
            .collect();
        Manifest::from_sparse_chunks(4096, 4096 * hashes.len() as u64, chunks, 8, |b| {
            ChunkHash::of(b)
        })
        .0
        .encode()
    }

    #[test]
    fn remote_rows_are_pending_until_another_node_reports_them() {
        let meta = Meta::open_in_memory().unwrap();
        let f = File { ino: 42 };
        let (a, b) = (ChunkHash::of(b"a"), ChunkHash::of(b"b"));
        meta.enroll_remote_chunks(f.ino, &[a, b], 7).unwrap();
        assert_eq!(meta.pending_upload_claims(&a, f.ino).unwrap(), 1);
        assert_eq!(meta.remote_chunk(&a, f.ino).unwrap().unwrap().node, 7);
        assert!(meta.awaits_remote_chunk(&b).unwrap());
        assert_eq!(meta.remote_chunks().unwrap().len(), 2);
        // A node-local row of other content is left alone.
        let own = ChunkHash::of(b"own");
        meta.add_pending_upload(&own, f.ino).unwrap();
        let acked = meta.ack_remote_chunks(&[a, own]).unwrap();
        assert_eq!(acked, vec![(a, f.ino)]);
        assert!(!meta.upload_pending_for_hash(&a).unwrap());
        assert!(meta.upload_pending_for_hash(&own).unwrap());
        assert!(!meta.awaits_remote_chunk(&a).unwrap());
        // An upload of `b` here (the content arrived another way) acks
        // the row and forgets the mark.
        meta.ack_upload(&b, f.ino).unwrap();
        assert!(!meta.awaits_remote_chunk(&b).unwrap());
        assert!(meta.remote_chunks().unwrap().is_empty());
    }

    /// Chunk metered-own-rows: a count that disagrees with the marks (here
    /// zeroed, as a saturated decrement would leave it) is rewritten at
    /// open, and the holder sees the node's pending chunks again.
    #[test]
    fn a_wrong_mark_count_is_rebuilt() {
        let meta = Meta::open_in_memory().unwrap();
        let (a, b) = (ChunkHash::of(b"a"), ChunkHash::of(b"b"));
        meta.enroll_remote_chunks(42, &[a, b], 7).unwrap();
        meta.enroll_remote_chunks(43, &[a], 8).unwrap();
        assert_eq!(meta.verify_mark_counts().unwrap(), 0, "consistent");
        let pending = |meta: &Meta, node: u64| {
            let r = meta.db.read_tx();
            remote_pending_from_tx(&r, meta, node).unwrap().len()
        };
        let mut tx = meta.db.write_tx();
        crate::store::counter_set_tx(&mut tx, &meta.local, &mark_count_key(7), 0);
        crate::store::counter_set_tx(&mut tx, &meta.local, &mark_count_key(9), 3);
        tx.commit().unwrap();
        assert_eq!(pending(&meta, 7), 0, "the stall: node 7 reads as clean");
        assert_eq!(meta.verify_mark_counts().unwrap(), 2);
        assert_eq!(pending(&meta, 7), 1);
        assert_eq!(pending(&meta, 8), 1);
        let r = meta.db.read_tx();
        assert_eq!(counter_get(&r, &meta.local, &mark_count_key(7)).unwrap(), 2);
        assert_eq!(counter_get(&r, &meta.local, &mark_count_key(9)).unwrap(), 0);
        drop(r);
        assert_eq!(meta.verify_mark_counts().unwrap(), 0);
    }

    /// Chunk close-stall-followup: each node's mark count follows every
    /// write and removal of its marks, so a count of 0 (one point read)
    /// stands for "nothing of that node's is pending here".
    #[test]
    fn each_nodes_mark_count_follows_its_marks() {
        let meta = Meta::open_in_memory().unwrap();
        let count = |node: u64| {
            let r = meta.db.read_tx();
            counter_get(&r, &meta.local, &mark_count_key(node)).unwrap()
        };
        let pending_from = |node: u64| {
            let r = meta.db.read_tx();
            remote_pending_from_tx(&r, &meta, node).unwrap()
        };
        let (a, b, c) = (
            ChunkHash::of(b"a"),
            ChunkHash::of(b"b"),
            ChunkHash::of(b"c"),
        );
        meta.enroll_remote_chunks(1, &[a, b], 7).unwrap();
        meta.enroll_remote_chunks(2, &[c], 8).unwrap();
        assert_eq!((count(7), count(8)), (2, 1));
        // The same row forwarded again, now by node 8: the mark moves.
        meta.enroll_remote_chunks(1, &[b], 8).unwrap();
        assert_eq!((count(7), count(8)), (1, 2));
        meta.enroll_remote_chunks(1, &[b], 8).unwrap();
        assert_eq!(count(8), 2, "re-marking is not a second mark");
        meta.ack_remote_chunks(&[a]).unwrap();
        assert_eq!(count(7), 0);
        assert!(pending_from(7).is_empty());
        meta.ack_upload(&b, 1).unwrap();
        assert_eq!(count(8), 1);
        // A mark whose row went another way stays counted (stale), and is
        // still filtered out by the scan behind the count.
        meta.cancel_pending_upload(&c, 2).unwrap();
        assert_eq!(count(8), 1);
        assert!(pending_from(8).is_empty());
        meta.forget_remote_mark(&c, 2).unwrap();
        assert_eq!(count(8), 0);
        meta.enroll_remote_chunks(3, &[a], 9).unwrap();
        meta.clear_pending_uploads().unwrap();
        assert_eq!(count(9), 0, "cleared with the marks");
        meta.enroll_remote_chunks(3, &[a], 9).unwrap();
        assert_eq!(count(9), 1);
        assert_eq!(pending_from(9).len(), 1);
    }

    #[test]
    fn a_manifest_naming_a_pending_chunk_is_not_releasable() {
        let meta = Meta::open_in_memory().unwrap();
        let f = File { ino: 42 };
        let (a, b) = (ChunkHash::of(b"a"), ChunkHash::of(b"b"));
        let tx = |hashes: &[ChunkHash]| crate::store::backup::BackupTx {
            first: 1,
            last: 1,
            records: vec![LogRecord::WriteManifest {
                ino: f.ino,
                base_manifest: None,
                manifest: manifest_of(hashes),
                size: 4096,
                time_ns: 0,
            }],
            origin: (0, 0),
        };
        let txs = vec![tx(&[a]), tx(&[b]), tx(&[a])];
        assert_eq!(
            meta.releasable_prefix(&txs, None).unwrap(),
            3,
            "nothing pending"
        );
        meta.enroll_remote_chunks(f.ino, &[b], 2).unwrap();
        assert_eq!(meta.releasable_prefix(&txs, None).unwrap(), 1);
        assert_eq!(
            meta.releasable_prefix(&txs, Some(2)).unwrap(),
            3,
            "node 2 forwarded it: it has the bytes"
        );
        assert_eq!(meta.releasable_prefix(&txs, Some(3)).unwrap(), 1);
        // This node's own pending chunk: nobody else has it.
        meta.add_pending_upload(&a, f.ino).unwrap();
        assert_eq!(meta.releasable_prefix(&txs, Some(2)).unwrap(), 0);
        meta.ack_upload(&a, f.ino).unwrap();
        meta.ack_remote_chunks(&[b]).unwrap();
        assert_eq!(meta.releasable_prefix(&txs, None).unwrap(), 3);
    }

    /// The sequencer's `OwnChunks` question for a transaction it does not
    /// hold unshipped (only the records count): do these records wait for
    /// `node`'s upload? Only a row marked remote from that node counts —
    /// not another node's, not this node's own pending chunk, and not
    /// once the node reported it up.
    #[test]
    fn records_wait_for_the_forwarders_upload_only_while_its_rows_are_pending() {
        let meta = Meta::open_in_memory().unwrap();
        let f = File { ino: 42 };
        let (a, b, c) = (
            ChunkHash::of(b"a"),
            ChunkHash::of(b"b"),
            ChunkHash::of(b"c"),
        );
        let records = |hashes: &[ChunkHash]| {
            vec![LogRecord::WriteManifest {
                ino: f.ino,
                base_manifest: None,
                manifest: manifest_of(hashes),
                size: 4096,
                time_ns: 0,
            }]
        };
        let rid = crate::rid::Rid {
            node: 2,
            incarnation: 1,
            seq: 1,
        };
        let waits = |records: &[LogRecord], node: u64| {
            let b = meta.remote_blockers(rid, 0, records, node, None).unwrap();
            assert_eq!(b.through, None, "never journaled here");
            !b.inos.is_empty()
        };
        assert!(!waits(&records(&[a, b]), 2));
        meta.enroll_remote_chunks(f.ino, &[b], 2).unwrap();
        meta.enroll_remote_chunks(f.ino, &[c], 3).unwrap();
        meta.add_pending_upload(&a, f.ino).unwrap();
        assert!(waits(&records(&[a, b]), 2));
        assert!(!waits(&records(&[a, b]), 3));
        assert!(!waits(&records(&[a]), 2));
        assert!(waits(&records(&[c]), 3));
        assert!(!waits(&[], 2));
        let cached = |node: u64| meta.blame_cache.lock().unwrap().contains_key(&node);
        assert!(cached(2) && cached(3), "each node's walk is kept");
        meta.ack_remote_chunks(&[b]).unwrap();
        assert!(!waits(&records(&[a, b]), 2));
        // Chunk metered-own-rows (review): node 2 has nothing pending
        // here any more, so its walk is dropped, not kept forever.
        assert!(!cached(2) && cached(3));
    }
}
