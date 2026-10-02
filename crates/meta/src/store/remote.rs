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

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::misc::{add_pending_claim_tx, cr_key, remove_pending_row_tx};
use crate::store::Meta;
use constellation_fs_core::{ChunkHash, ChunkInfo, Ino, Manifest};
use fjall::Readable;

const REMOTE_PREFIX: &[u8] = b"remote-chunk/";

pub(crate) fn remote_key(hash: &ChunkHash, ino: Ino) -> Vec<u8> {
    let mut k = REMOTE_PREFIX.to_vec();
    k.extend_from_slice(&hash.0);
    k.extend_from_slice(&ino.to_be_bytes());
    k
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
            tx.insert(&self.local, remote_key(hash, ino), value.clone());
        }
        tx.commit()?;
        Ok(())
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
            let marks: Vec<(Vec<u8>, Vec<u8>)> = tx
                .prefix(&self.local, remote_hash_prefix(hash))
                .map(|g| g.into_inner().map(|(k, v)| (k.to_vec(), v.to_vec())))
                .collect::<Result<_, _>>()?;
            for (k, v) in marks {
                tx.remove(&self.local, k.clone());
                if let Some(mark) = decode_mark(&k, &v) {
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
        let key = remote_key(hash, ino);
        if self.local.get(&key)?.is_some() {
            self.local.remove(key)?;
        }
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
}
