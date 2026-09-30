//! `chunk_ref`/`chunk_ref_by_ino`, `pending_upload`, `xattr_by_name`,
//! `pins`, `epochs` and the prune-policy root-discovery helpers.

use crate::error::MetaError;
use crate::store::{kv_get_tx, kv_set_tx, ns, EpochRow, Meta};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_mtree::record::{InodeRecord, Payload};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};
use std::collections::HashSet;

// ---------------------------------------------------------------- generic

/// Touch `mtime_ns`/`ctime_ns` on `ino`'s `0x01` record (parent-directory
/// timestamp bumps). A no-op if the inode is gone (a cascade of an
/// already-skipped conflict during replay).
pub(crate) fn touch_times_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    ino: Ino,
    t: i64,
) -> Result<(), MetaError> {
    if let Some(mut rec) = ns::get_inode_record(tx, ns_ks, ino)? {
        // Plan 30 §M12: a `max` merge — the parent's times commute
        // under any replay order of its children's records (HLC stamps,
        // `crate::hlc`), and never go backwards.
        rec.attrs.mtime_ns = rec.attrs.mtime_ns.max(t);
        rec.attrs.ctime_ns = rec.attrs.ctime_ns.max(t);
        ns::put_inode_record(tx, ns_ks, dirty, ino, &rec)?;
    }
    Ok(())
}

/// A *directory's* `nlink += delta` (saturating at 0) plus an mtime and
/// ctime touch, in one re-encode of the `0x01` record: a directory's
/// link count changes when a subdirectory (`..`) comes or goes, which is
/// a change of its content.
pub(crate) fn bump_nlink_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    ino: Ino,
    delta: i64,
    t: i64,
) -> Result<Option<InodeRecord>, MetaError> {
    bump_nlink_touching(tx, ns_ks, dirty, ino, delta, t, true)
}

/// A name added to or removed from a *non-directory* (`link`, `unlink`
/// or a rename over one of several names): `nlink += delta` and a ctime
/// touch only. POSIX: a link count change is a status change; the file's
/// data, and so its mtime, did not change (the directory gaining or
/// losing the name gets both, through [`touch_times_tx`]).
pub(crate) fn bump_file_nlink_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    ino: Ino,
    delta: i64,
    t: i64,
) -> Result<Option<InodeRecord>, MetaError> {
    bump_nlink_touching(tx, ns_ks, dirty, ino, delta, t, false)
}

fn bump_nlink_touching(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    ino: Ino,
    delta: i64,
    t: i64,
    mtime: bool,
) -> Result<Option<InodeRecord>, MetaError> {
    let Some(mut rec) = ns::get_inode_record(tx, ns_ks, ino)? else {
        return Ok(None);
    };
    // Plan 30 §M12: an additive delta and a `max` time merge, both
    // commutative across the replay order.
    rec.attrs.nlink = (rec.attrs.nlink as i64 + delta).max(0) as u32;
    if mtime {
        rec.attrs.mtime_ns = rec.attrs.mtime_ns.max(t);
    }
    rec.attrs.ctime_ns = rec.attrs.ctime_ns.max(t);
    ns::put_inode_record(tx, ns_ks, dirty, ino, &rec)?;
    Ok(Some(rec))
}

pub(crate) fn get_one_xattr(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    ino: Ino,
    name: &str,
) -> Result<Option<Vec<u8>>, MetaError> {
    let Some(rec) = ns::get_inode_record(r, ns_ks, ino)? else {
        return Ok(None);
    };
    if !rec.xattrs.is_empty() {
        return Ok(rec
            .xattrs
            .iter()
            .find(|(n, _)| n == name.as_bytes())
            .map(|(_, v)| v.clone()));
    }
    match r.get(
        ns_ks,
        constellation_mtree::keys::xattr(ino, name.as_bytes()),
    )? {
        Some(v) => Ok(Some(ns::resolve_payload(r, blobs, &Payload::decode(&v)?)?)),
        None => Ok(None),
    }
}

// ------------------------------------------------------------- chunk_ref

pub(crate) fn manifest_hashes(bytes: Option<&[u8]>) -> HashSet<ChunkHash> {
    use constellation_fs_core::manifest::ChunkInfo;
    let Some(bytes) = bytes else {
        return HashSet::new();
    };
    let Ok(manifest) = constellation_fs_core::manifest::Manifest::decode(bytes) else {
        return HashSet::new();
    };
    match manifest.chunks {
        ChunkInfo::Inline(hashes) => hashes.into_values().collect(),
        ChunkInfo::Spilled(hash) => [hash].into_iter().collect(),
    }
}

pub(crate) fn cr_key(hash: &ChunkHash, ino: Ino) -> Vec<u8> {
    let mut k = hash.0.to_vec();
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

/// A `pending_upload` row's claim count. Rows written before counts
/// existed carry an empty value and hold one claim.
pub(crate) fn pending_claims(value: &[u8]) -> u32 {
    value
        .get(..4)
        .map_or(1, |b| u32::from_le_bytes(b.try_into().unwrap()).max(1))
}

pub(crate) fn encode_claims(n: u32) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

/// Add one claim to `(hash, ino)`'s pending row inside `tx`.
pub(crate) fn add_pending_claim_tx(
    tx: &mut SingleWriterWriteTx,
    pending_upload: &SingleWriterTxKeyspace,
    hash: &ChunkHash,
    ino: Ino,
) -> Result<(), MetaError> {
    let key = cr_key(hash, ino);
    let n = tx
        .get(pending_upload, &key)?
        .map_or(0, |v| pending_claims(&v));
    tx.insert(pending_upload, key, encode_claims(n.saturating_add(1)));
    Ok(())
}

pub(crate) fn cri_key(ino: Ino, hash: &ChunkHash) -> Vec<u8> {
    let mut k = ino.to_be_bytes().to_vec();
    k.extend_from_slice(&hash.0);
    k
}

/// Maintain `chunk_ref`/`chunk_ref_by_ino` in the same transaction as an
/// inode's manifest change (incremental diff of old vs. new referenced
/// hashes).
pub(crate) fn track_manifest_transition_tx(
    tx: &mut SingleWriterWriteTx,
    chunk_ref: &SingleWriterTxKeyspace,
    chunk_ref_by_ino: &SingleWriterTxKeyspace,
    ino: Ino,
    old: Option<&[u8]>,
    new: Option<&[u8]>,
) -> Result<(), MetaError> {
    let old_set = manifest_hashes(old);
    let new_set = manifest_hashes(new);
    for hash in new_set.difference(&old_set) {
        tx.insert(chunk_ref, cr_key(hash, ino), Vec::new());
        tx.insert(chunk_ref_by_ino, cri_key(ino, hash), Vec::new());
    }
    for hash in old_set.difference(&new_set) {
        tx.remove(chunk_ref, cr_key(hash, ino));
        tx.remove(chunk_ref_by_ino, cri_key(ino, hash));
    }
    Ok(())
}

impl Meta {
    pub fn chunk_ref_exists(&self, hash: &ChunkHash) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        let prefix = hash.0.to_vec();
        for guard in r.prefix(&self.chunk_ref, prefix) {
            let (k, _) = guard.into_inner()?;
            if k.len() != 40 {
                continue;
            }
            let ino = u64::from_be_bytes(k[32..40].try_into().unwrap());
            if r.get(&self.pending_upload, cr_key(hash, ino))?.is_none() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn live_manifest_hashes(&self) -> Result<HashSet<ChunkHash>, MetaError> {
        let r = self.db.read_tx();
        let mut out = HashSet::new();
        for guard in r.range(
            &self.ns,
            ns::key_range_bounds(&constellation_mtree::keys::whole_range(
                constellation_mtree::keys::RANGE_INODE,
            )),
        ) {
            let (_, v) = guard.into_inner()?;
            let rec = InodeRecord::decode(&v)?;
            if let Some(m) = &rec.manifest {
                let bytes = ns::resolve_payload(&r, &self.blobs, m)?;
                out.extend(manifest_hashes(Some(&bytes)));
            }
        }
        Ok(out)
    }

    pub fn live_manifests(&self) -> Result<Vec<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.range(
            &self.ns,
            ns::key_range_bounds(&constellation_mtree::keys::whole_range(
                constellation_mtree::keys::RANGE_INODE,
            )),
        ) {
            let (_, v) = guard.into_inner()?;
            let rec = InodeRecord::decode(&v)?;
            if let Some(m) = &rec.manifest {
                out.push(ns::resolve_payload(&r, &self.blobs, m)?);
            }
        }
        Ok(out)
    }

    // --------------------------------------------------------- pending_upload

    pub fn pending_uploads(&self) -> Result<Vec<(ChunkHash, Ino)>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.iter(&self.pending_upload) {
            let (k, _) = guard.into_inner()?;
            if k.len() != 40 {
                return Err(MetaError::Invalid("pending_upload key length".into()));
            }
            let hash = ChunkHash(k[..32].try_into().unwrap());
            let ino = u64::from_be_bytes(k[32..40].try_into().unwrap());
            out.push((hash, ino));
        }
        Ok(out)
    }

    /// Enrol one claim on uploading `hash` for `ino`.
    ///
    /// A row is a *count* of claims (see [`pending_claims`]): one inode can
    /// need the same content for several reasons at once — two chunk
    /// indices with identical bytes, a sealed chunk of the open write
    /// session plus an earlier, still unshipped manifest — and
    /// [`Self::cancel_pending_upload`] must withdraw only the claim it
    /// made. With a plain presence row, rewriting one of two identical
    /// sealed chunks cancelled the row the other still needed, and its
    /// manifest was committed naming a chunk nothing would ever upload.
    /// An upload satisfies every claim at once ([`Self::ack_upload`]
    /// removes the row).
    pub fn add_pending_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        add_pending_claim_tx(&mut tx, &self.pending_upload, hash, ino)?;
        tx.commit()?;
        Ok(())
    }

    /// How many claims `(hash, ino)`'s pending row holds (0: no row).
    pub fn pending_upload_claims(&self, hash: &ChunkHash, ino: Ino) -> Result<u32, MetaError> {
        let r = self.db.read_tx();
        Ok(r.get(&self.pending_upload, cr_key(hash, ino))?
            .map_or(0, |v| pending_claims(&v)))
    }

    pub fn pending_upload_count(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        let mut n = 0u64;
        for guard in r.iter(&self.pending_upload) {
            guard.into_inner()?;
            n += 1;
        }
        Ok(n)
    }

    pub fn upload_pending_for_hash(&self, hash: &ChunkHash) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        Ok(r.prefix(&self.pending_upload, hash.0).next().is_some())
    }

    pub fn ack_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        self.pending_upload.remove(cr_key(hash, ino))?;
        // The row may have been another node's (`store::remote`): the
        // content is up, so its mark goes too.
        self.forget_remote_mark(hash, ino)
    }

    /// Withdraw one claim enrolled by [`Self::add_pending_upload`] (a
    /// sealed chunk the writer has since overwritten or punched). The row
    /// goes only with its last claim; other claims on the same content
    /// keep it, so the chunk still uploads.
    pub fn cancel_pending_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let key = cr_key(hash, ino);
        match tx
            .get(&self.pending_upload, &key)?
            .map(|v| pending_claims(&v))
        {
            None => return Ok(()),
            Some(n) if n <= 1 => tx.remove(&self.pending_upload, key),
            Some(n) => tx.insert(&self.pending_upload, key, encode_claims(n - 1)),
        }
        tx.commit()?;
        Ok(())
    }

    pub fn clear_pending_uploads(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let keys: Vec<Vec<u8>> = tx
            .iter(&self.pending_upload)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            tx.remove(&self.pending_upload, k);
        }
        // Plan 30 §M4: the unrecoverable marks describe pending rows; with
        // the rows gone they would only be filtered out, so drop them too.
        let marks: Vec<Vec<u8>> = tx
            .prefix(&self.local, b"poisoned/")
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in marks {
            tx.remove(&self.local, k);
        }
        crate::store::counter_set_tx(&mut tx, &self.local, crate::store::KV_POISONED_COUNT, 0);
        self.clear_remote_marks_tx(&mut tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn purge_foreign_pending_uploads(&self, prefix: u64) -> Result<u64, MetaError> {
        let mut tx = self.db.write_tx();
        let mut removed = 0u64;
        let keys: Vec<Vec<u8>> = tx
            .iter(&self.pending_upload)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            if k.len() == 40 {
                let ino = u64::from_be_bytes(k[32..40].try_into().unwrap());
                if (ino >> crate::store::INO_PREFIX_SHIFT) != prefix {
                    tx.remove(&self.pending_upload, k);
                    removed += 1;
                }
            }
        }
        tx.commit()?;
        Ok(removed)
    }

    // --------------------------------------------------------- xattr_by_name

    pub fn prune_roots(&self) -> Result<Vec<(Ino, String)>, MetaError> {
        let r = self.db.read_tx();
        let mut prefix = crate::prune::PRUNE_XATTR.as_bytes().to_vec();
        prefix.push(0);
        let mut out = Vec::new();
        for guard in r.prefix(&self.xattr_by_name, prefix.clone()) {
            let (k, v) = guard.into_inner()?;
            if k.len() < 8 {
                continue;
            }
            let ino = u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap());
            out.push((ino, String::from_utf8_lossy(&v).into_owned()));
        }
        out.sort_by_key(|(ino, _)| *ino);
        Ok(out)
    }

    pub fn effective_prune_policy(&self, ino: Ino) -> Result<Option<(Ino, String)>, MetaError> {
        let r = self.db.read_tx();
        let mut cursor = Some(ino);
        while let Some(cur) = cursor {
            if let Some(v) =
                get_one_xattr(&r, &self.ns, &self.blobs, cur, crate::prune::PRUNE_XATTR)?
            {
                return Ok(Some((cur, String::from_utf8_lossy(&v).into_owned())));
            }
            cursor = self.parent_of_at(&r, cur)?;
        }
        Ok(None)
    }
}

pub(crate) fn xattr_by_name_key(name: &str, ino: Ino) -> Vec<u8> {
    let mut k = name.as_bytes().to_vec();
    k.push(0);
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

pub(crate) fn xattr_by_name_put_tx(
    tx: &mut SingleWriterWriteTx,
    ks: &SingleWriterTxKeyspace,
    name: &str,
    ino: Ino,
    value: &[u8],
) {
    tx.insert(ks, xattr_by_name_key(name, ino), value.to_vec());
}

pub(crate) fn xattr_by_name_del_tx(
    tx: &mut SingleWriterWriteTx,
    ks: &SingleWriterTxKeyspace,
    name: &str,
    ino: Ino,
) {
    tx.remove(ks, xattr_by_name_key(name, ino));
}

pub(crate) fn xattr_by_name_del_all_tx(
    tx: &mut SingleWriterWriteTx,
    ks: &SingleWriterTxKeyspace,
    ino: Ino,
    names: impl IntoIterator<Item = Vec<u8>>,
) {
    for name in names {
        tx.remove(ks, {
            let mut k = name;
            k.push(0);
            k.extend_from_slice(&ino.to_be_bytes());
            k
        });
    }
}

// --------------------------------------------------------------- pins

#[derive(serde::Serialize, serde::Deserialize)]
struct PinRow {
    ino: Ino,
    pinned_at: i64,
}

impl Meta {
    pub fn add_pin(&self, path: &str, ino: Ino) -> Result<(), MetaError> {
        let row = PinRow {
            ino,
            pinned_at: constellation_fs_core::types::now_ns(),
        };
        self.pins
            .insert(path.as_bytes(), postcard::to_allocvec(&row)?)?;
        Ok(())
    }

    /// Returns whether `path` was pinned (and is now removed).
    pub fn remove_pin(&self, path: &str) -> Result<bool, MetaError> {
        Ok(self.pins.take(path.as_bytes())?.is_some())
    }

    pub fn pins(&self) -> Result<Vec<(String, Ino)>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.iter(&self.pins) {
            let (k, v) = guard.into_inner()?;
            let row: PinRow = postcard::from_bytes(&v)?;
            out.push((String::from_utf8_lossy(&k).into_owned(), row.ino));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

// -------------------------------------------------------------- epochs

const PROMISE_ISSUED: &str = "epoch_promise_issued_ms";
const PROMISE_JOINING: &str = "epoch_promise_joining";

#[derive(serde::Serialize, serde::Deserialize)]
struct EpochRowEnc {
    members: Vec<u64>,
    base: std::collections::BTreeMap<String, u64>,
    promised_at: i64,
    state: String,
}

impl Meta {
    pub fn persist_epoch(
        &self,
        epoch_id: &str,
        members: &[u64],
        base: &std::collections::BTreeMap<String, u64>,
        promised_at: i64,
        state: &str,
    ) -> Result<(), MetaError> {
        let row = EpochRowEnc {
            members: members.to_vec(),
            base: base.clone(),
            promised_at,
            state: state.to_string(),
        };
        let bytes = postcard::to_allocvec(&row)?;
        // Called after every epoch-machine step: write (and sync) only a
        // change. M16: an epoch promise or membership forgotten on a
        // power loss is a safety problem (the node could accept a second
        // proposal, or promise while still a member), so it is synced
        // before the machine acts on it further.
        if self
            .epochs
            .get(epoch_id.as_bytes())?
            .is_some_and(|v| *v == *bytes)
        {
            return Ok(());
        }
        self.epochs.insert(epoch_id.as_bytes(), bytes)?;
        self.sync()
    }

    pub fn load_open_epoch(&self) -> Result<Option<EpochRow>, MetaError> {
        let r = self.db.read_tx();
        let mut best: Option<(String, EpochRowEnc)> = None;
        for guard in r.iter(&self.epochs) {
            let (k, v) = guard.into_inner()?;
            let row: EpochRowEnc = postcard::from_bytes(&v)?;
            if row.state == "closed" {
                continue;
            }
            let id = String::from_utf8_lossy(&k).into_owned();
            if best
                .as_ref()
                .is_none_or(|(_, b)| row.promised_at > b.promised_at)
            {
                best = Some((id, row));
            }
        }
        Ok(best.map(|(id, row)| (id, row.members, row.base, row.promised_at, row.state)))
    }

    pub fn set_epoch_state(&self, epoch_id: &str, state: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if let Some(v) = tx.get(&self.epochs, epoch_id.as_bytes())? {
            let mut row: EpochRowEnc = postcard::from_bytes(&v)?;
            row.state = state.to_string();
            tx.insert(
                &self.epochs,
                epoch_id.as_bytes().to_vec(),
                postcard::to_allocvec(&row)?,
            );
        }
        tx.commit()?;
        Ok(())
    }

    // ------------------------------------------- plan 30 §M10: promises

    /// The last heartbeat promise this node *issued*
    /// (`no_epoch_until_unix_ms`, persisted before its PUT; 0: none).
    pub fn promise_issued(&self) -> Result<i64, MetaError> {
        Ok(self
            .kv_get(PROMISE_ISSUED)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    /// Persist a new promise before publishing it. Refused (`Ok(false)`,
    /// nothing written) while a continuation-epoch join holds the gate
    /// ([`Meta::promise_join_begin`]): a member publishes no promise. The
    /// check and the write are one write transaction, and the join gate
    /// takes the same (single) writer lock, so a promise can never be
    /// issued between a join's check and its gate.
    pub fn promise_issue(&self, until_unix_ms: i64) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        if kv_get_tx(&tx, &self.local, PROMISE_JOINING)?.as_deref() == Some("1") {
            return Ok(false);
        }
        let issued: i64 = kv_get_tx(&tx, &self.local, PROMISE_ISSUED)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let extends = until_unix_ms > issued;
        if extends {
            kv_set_tx(
                &mut tx,
                &self.local,
                PROMISE_ISSUED,
                &until_unix_ms.to_string(),
            );
        }
        tx.commit()?;
        // M16: on stable storage before it is published. A node that
        // forgot a promise after a power loss could join an epoch before
        // the time it promised, which a taker counted on.
        if extends {
            self.sync()?;
        }
        Ok(true)
    }

    /// A continuation-epoch join: allowed only once the last issued
    /// promise has expired in this node's clock (`now_ms`), and from then
    /// on no promise is issued until [`Meta::promise_join_end`].
    /// Idempotent while the gate is held.
    pub fn promise_join_begin(&self, now_ms: i64) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        if kv_get_tx(&tx, &self.local, PROMISE_JOINING)?.as_deref() == Some("1") {
            return Ok(true);
        }
        let issued: i64 = kv_get_tx(&tx, &self.local, PROMISE_ISSUED)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if now_ms < issued {
            return Ok(false);
        }
        kv_set_tx(&mut tx, &self.local, PROMISE_JOINING, "1");
        tx.commit()?;
        // M16: a member publishes no promise; that must survive a power
        // loss as it survives a process crash.
        self.sync()?;
        Ok(true)
    }

    /// The node is in no open epoch any more: promises may be issued.
    pub fn promise_join_end(&self) -> Result<(), MetaError> {
        if self.promise_joining()? {
            self.kv_set(PROMISE_JOINING, "0")?;
        }
        Ok(())
    }

    pub fn promise_joining(&self) -> Result<bool, MetaError> {
        Ok(self.kv_get(PROMISE_JOINING)?.as_deref() == Some("1"))
    }

    // ---------------------------------------------------------- reintegration

    pub fn unmarked_journal(&self) -> Result<crate::store::JournalBatch, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::unmarked(&r, &self.journal_ks, &self.reintegration)
    }

    pub fn unmarked_journal_len(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::unmarked_len(&r, &self.journal_ks, &self.reintegration)
    }

    pub fn unmarked_journal_parts(&self) -> Result<Vec<String>, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::unmarked_parts(&r, &self.journal_ks, &self.reintegration)
    }

    pub fn reintegration_conflict_count(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::conflict_count(&r, &self.reintegration)
    }
}

#[cfg(test)]
mod pending_claim_tests {
    use crate::store::Meta;
    use crate::MetaStore;
    use constellation_fs_core::ChunkHash;

    /// One inode needing the same content twice (identical bytes at two
    /// chunk indices, or a sealed chunk plus an earlier manifest) holds
    /// two claims; cancelling one leaves the row, so the chunk still
    /// uploads. An upload satisfies every claim at once.
    #[test]
    fn a_row_counts_claims_and_cancel_withdraws_only_one() {
        let meta = Meta::open_in_memory().unwrap();
        let h = ChunkHash::of(b"same bytes at two indices");
        meta.add_pending_upload(&h, 7).unwrap();
        meta.add_pending_upload(&h, 7).unwrap();
        assert_eq!(meta.pending_upload_claims(&h, 7).unwrap(), 2);
        meta.cancel_pending_upload(&h, 7).unwrap();
        assert_eq!(meta.pending_uploads().unwrap(), vec![(h, 7)]);
        assert!(meta.upload_pending_for_hash(&h).unwrap());
        meta.cancel_pending_upload(&h, 7).unwrap();
        assert!(meta.pending_uploads().unwrap().is_empty());
        // Cancelling what is not there is a no-op.
        meta.cancel_pending_upload(&h, 7).unwrap();

        // The manifest commit's claims count too, and other inodes' rows
        // are separate.
        meta.add_pending_upload(&h, 7).unwrap();
        let file = meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        meta.set_manifest_dirty(file.ino, None, b"M", 1, &[h])
            .unwrap();
        meta.add_pending_upload(&h, file.ino).unwrap();
        assert_eq!(meta.pending_upload_claims(&h, file.ino).unwrap(), 2);
        meta.cancel_pending_upload(&h, file.ino).unwrap();
        assert_eq!(meta.pending_upload_claims(&h, file.ino).unwrap(), 1);
        assert_eq!(meta.pending_upload_claims(&h, 7).unwrap(), 1);
        meta.ack_upload(&h, file.ino).unwrap();
        assert_eq!(meta.pending_upload_claims(&h, file.ino).unwrap(), 0);
        assert_eq!(meta.pending_uploads().unwrap(), vec![(h, 7)]);
    }

    /// Rows written before claims were counted have an empty value: one
    /// claim.
    #[test]
    fn a_legacy_empty_row_is_one_claim() {
        let meta = Meta::open_in_memory().unwrap();
        let h = ChunkHash::of(b"legacy");
        meta.pending_upload
            .insert(super::cr_key(&h, 3), Vec::new())
            .unwrap();
        assert_eq!(meta.pending_upload_claims(&h, 3).unwrap(), 1);
        meta.add_pending_upload(&h, 3).unwrap();
        assert_eq!(meta.pending_upload_claims(&h, 3).unwrap(), 2);
        meta.cancel_pending_upload(&h, 3).unwrap();
        meta.cancel_pending_upload(&h, 3).unwrap();
        assert!(meta.pending_uploads().unwrap().is_empty());
    }
}
