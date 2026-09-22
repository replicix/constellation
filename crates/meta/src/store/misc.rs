//! `chunk_ref`/`chunk_ref_by_ino`, `pending_upload`, `xattr_by_name`,
//! `pins`, `epochs` and the prune-policy root-discovery helpers.

use crate::error::MetaError;
use crate::store::{ns, EpochRow, Meta};
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
        rec.attrs.mtime_ns = t;
        rec.attrs.ctime_ns = t;
        ns::put_inode_record(tx, ns_ks, dirty, ino, &rec)?;
    }
    Ok(())
}

/// `nlink += delta` (saturating at 0) plus a timestamp touch, in one
/// re-encode of the `0x01` record.
pub(crate) fn bump_nlink_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    ino: Ino,
    delta: i64,
    t: i64,
) -> Result<Option<InodeRecord>, MetaError> {
    let Some(mut rec) = ns::get_inode_record(tx, ns_ks, ino)? else {
        return Ok(None);
    };
    rec.attrs.nlink = (rec.attrs.nlink as i64 + delta).max(0) as u32;
    rec.attrs.mtime_ns = t;
    rec.attrs.ctime_ns = t;
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

    pub fn add_pending_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        self.pending_upload.insert(cr_key(hash, ino), Vec::new())?;
        Ok(())
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
        Ok(())
    }

    pub fn cancel_pending_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        self.ack_upload(hash, ino)
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
        self.epochs
            .insert(epoch_id.as_bytes(), postcard::to_allocvec(&row)?)?;
        Ok(())
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

    pub fn shadow_insert(
        &self,
        _part: &str,
        epoch: u64,
        records: &[crate::record::LogRecord],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        crate::store::journal::shadow_insert_tx(
            &mut tx,
            &self.shadow,
            &self.local,
            epoch,
            records,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn shadow_retire_matching(
        &self,
        epoch: u64,
        records: &[crate::record::LogRecord],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        crate::store::journal::shadow_retire_matching_tx(&mut tx, &self.shadow, epoch, records)?;
        tx.commit()?;
        Ok(())
    }
}
