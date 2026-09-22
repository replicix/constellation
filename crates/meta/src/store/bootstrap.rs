//! Plan 29 M2's ingestion-based bootstrap-from-commit loader
//! (`ns_ingest_page`, `put_local_blob`, `apply_bootstrap_indexes`,
//! `clear_all_dirty`) and reintegration's atomic namespace swap
//! (`commit_reintegration_batch`).
//!
//! Bootstrap no longer reconstructs `TreeInode` rows and re-derives
//! `0x01`/`0x02` records through the ordinary write path: since M1,
//! `ns`'s key/value encoding *is* the published tree's (modulo the
//! local-vs-published spill of `0x01`/`0x03` payloads), so loading a
//! commit is a bulk copy of its keys, not a translation. The caller
//! (`cli::mtree_read::bootstrap_from_commit`) walks the tree with one
//! ordered cursor, converts each spilled payload's blob reference from
//! the bucket's addressing to this replica's local `blobs` keyspace, and
//! hands pages of `(key, value)` pairs to [`Meta::ns_ingest_page`], which
//! loads them with `fjall::Keyspace::start_ingestion` — bypassing the
//! write-transaction/dirty-tracking path entirely, which is exactly
//! right here: the loaded rows already equal the published tree, so
//! nothing about them is unpublished. [`Meta::clear_all_dirty`] wipes
//! whatever `Meta::open`'s genesis root insert speculatively dirtied, so
//! a bootstrapped replica's very first publish is the log tail's delta
//! alone, not a rebuild.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::{journal, misc, ns, Meta, KV_APPLIED_SEQ, KV_USAGE_BYTES, KV_USAGE_FILES};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_mtree::keys;
use constellation_mtree::record::{self, Attrs, BlobHash, InodeRecord, Kind, XattrPlacement};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

/// One inode's `0x01` record, re-encoded in this replica's *local*
/// spill scheme, plus any `0x03` entries the xattr set spilled to — the
/// bootstrap ingestion counterpart of `ns::put_inode`, which cannot be
/// used directly because ingestion never opens a write transaction.
pub struct LocalInodeEncoding {
    /// The `0x01` value.
    pub record: Vec<u8>,
    /// `(name, encoded Payload)` for each spilled xattr, empty when the
    /// set is inline (already folded into `record`).
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Accumulates `chunk_ref`/`chunk_ref_by_ino`/`xattr_by_name`/usage from
/// one pass over a bootstrap's inodes, fed by the *same* decode
/// `cli::mtree_read::load_tree` already pays for while converting each
/// inode to its local encoding (plan 29 M3a).
///
/// Before this existed, a bootstrap called a `rebuild_derived_from_ns`
/// method after ingestion, which re-read and re-decoded every `0x01`
/// value from `ns` twice more (once for the indexes, once for usage) —
/// measured at ~215 ms of a 100k-inode bootstrap that, after the M3a
/// xattr-probe fix elsewhere in this milestone, otherwise ran in
/// ~330 ms. [`Meta::apply_bootstrap_indexes`] takes this instead: the
/// same information, collected for free during the one pass that
/// already happens, applied with zero re-decoding.
#[derive(Default)]
pub struct BootstrapIndexBuilder {
    chunk_hashes: Vec<(ChunkHash, Ino)>,
    xattr_by_name: Vec<(String, Ino, Vec<u8>)>,
    usage_bytes: u64,
    usage_files: u64,
}

impl BootstrapIndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one inode's derived state. `manifest` and `xattrs` are the
    /// already-resolved plaintext the caller used (or is about to use)
    /// to build this inode's local encoding — never re-fetched or
    /// re-decoded here.
    pub fn observe(
        &mut self,
        ino: Ino,
        attrs: &Attrs,
        manifest: Option<&[u8]>,
        xattrs: &[(Vec<u8>, Vec<u8>)],
    ) {
        for hash in misc::manifest_hashes(manifest) {
            self.chunk_hashes.push((hash, ino));
        }
        for (name, value) in xattrs {
            self.xattr_by_name.push((
                String::from_utf8_lossy(name).into_owned(),
                ino,
                value.clone(),
            ));
        }
        if attrs.kind == Kind::File {
            self.usage_bytes += attrs.size;
            self.usage_files += 1;
        }
    }
}

/// Wholesale clear of a keyspace, inside the caller's write transaction.
fn clear_tx(tx: &mut SingleWriterWriteTx, ks: &SingleWriterTxKeyspace) -> Result<(), MetaError> {
    let keys: Vec<Vec<u8>> = tx
        .iter(ks)
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(ks, k);
    }
    Ok(())
}

/// Rebuild `chunk_ref`/`chunk_ref_by_ino`/`xattr_by_name` from whatever
/// is currently in `ns` — the fjall equivalent of the old engine's
/// `DELETE FROM chunk_ref; rebuild_chunk_ref(&tx)`, extended to the
/// `xattr_by_name` index this engine also maintains.
fn rebuild_indexes_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    chunk_ref: &SingleWriterTxKeyspace,
    chunk_ref_by_ino: &SingleWriterTxKeyspace,
    xattr_by_name: &SingleWriterTxKeyspace,
) -> Result<(), MetaError> {
    clear_tx(tx, chunk_ref)?;
    clear_tx(tx, chunk_ref_by_ino)?;
    clear_tx(tx, xattr_by_name)?;
    let range = keys::whole_range(keys::RANGE_INODE);
    let rows: Vec<(Ino, InodeRecord)> = tx
        .range(ns_ks, ns::key_range_bounds(&range))
        .map(|g| {
            g.into_inner().map_err(MetaError::from).and_then(|(k, v)| {
                let ino = match keys::Key::parse(&k)? {
                    keys::Key::Inode { ino } => ino,
                    _ => return Err(MetaError::Invalid("expected an inode key".into())),
                };
                Ok((ino, InodeRecord::decode(&v)?))
            })
        })
        .collect::<Result<_, MetaError>>()?;
    for (ino, rec) in &rows {
        let manifest = rec
            .manifest
            .as_ref()
            .map(|p| ns::resolve_payload(tx, blobs, p))
            .transpose()?;
        misc::track_manifest_transition_tx(
            tx,
            chunk_ref,
            chunk_ref_by_ino,
            *ino,
            None,
            manifest.as_deref(),
        )?;
        for (name, value) in ns::all_xattrs(tx, ns_ks, blobs, rec, *ino)? {
            misc::xattr_by_name_put_tx(tx, xattr_by_name, &name, *ino, &value);
        }
    }
    Ok(())
}

impl Meta {
    /// Bulk-load one page of already-ordered `ns` key/value pairs
    /// (plan 29 M2 bootstrap) via `fjall::Keyspace::start_ingestion`,
    /// bypassing the write-transaction/dirty-tracking path: these rows
    /// are exactly the published tree's bytes (the caller has already
    /// converted any spilled payload's blob reference to this replica's
    /// local `blobs` keyspace), so nothing about them is unpublished.
    ///
    /// `page` must be strictly ascending by key (ingestion's own
    /// requirement) and, across calls, each page's keys must sort after
    /// the previous page's — exactly what a single ordered tree cursor,
    /// paged, already produces. Never journals: there is nothing to
    /// journal, the rows *are* the shared state at the commit's vector.
    pub fn ns_ingest_page(&self, page: &[(Vec<u8>, Vec<u8>)]) -> Result<(), MetaError> {
        if page.is_empty() {
            return Ok(());
        }
        let mut ingestion = self.ns.inner().start_ingestion()?;
        for (k, v) in page {
            ingestion.write(k.clone(), v.clone())?;
        }
        ingestion.finish()?;
        Ok(())
    }

    /// Store `body` under its local (plain blake3) hash, for a bootstrap
    /// converting a published `Payload::Spilled` reference (fetched from
    /// the bucket's `blobs/`, under whatever hash the filesystem's E2E
    /// mode used) back to this replica's node-local spill scheme.
    pub fn put_local_blob(&self, body: Vec<u8>) -> Result<BlobHash, MetaError> {
        let hash = Self::hash_blob(&body);
        self.blobs.insert(hash.0, body)?;
        Ok(hash)
    }

    /// Re-encode one inode's plaintext content (already resolved by the
    /// caller from whatever the *published* side spilled it to) into
    /// this replica's local `0x01`/`0x03` encoding: the same
    /// `record::plan_inode`/local-hash spill decision `ns::put_inode`
    /// makes for an ordinary write, but returning the bytes rather than
    /// writing them — a bootstrap loads them via `ns_ingest_page`
    /// instead, which cannot share a write transaction with anything.
    pub fn encode_local_inode(
        &self,
        attrs: Attrs,
        manifest: Option<Vec<u8>>,
        symlink_target: Option<Vec<u8>>,
        xattrs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<LocalInodeEncoding, MetaError> {
        let planned = record::plan_inode(attrs, manifest, symlink_target, xattrs, Self::hash_blob);
        for blob in planned.blobs {
            self.put_local_blob(blob)?;
        }
        let mut spilled = Vec::new();
        if planned.xattrs == XattrPlacement::Spilled {
            for (name, value) in xattrs {
                let (payload, blob) = record::place_value(value.clone(), Self::hash_blob);
                if let Some(blob) = blob {
                    self.put_local_blob(blob)?;
                }
                spilled.push((name.clone(), payload.encode()));
            }
        }
        Ok(LocalInodeEncoding {
            record: planned.record.encode(),
            xattrs: spilled,
        })
    }

    /// After a bootstrap's `ns_ingest_page` calls are all in: apply the
    /// `chunk_ref`/`chunk_ref_by_ino`/`xattr_by_name`/usage state a
    /// [`BootstrapIndexBuilder`] accumulated while the caller walked the
    /// tree — the derived state an ordinary write path maintains
    /// incrementally, which bulk ingestion bypassed, applied here with
    /// no re-decoding of what was already decoded once.
    ///
    /// Clears the three index keyspaces first (idempotent: a bootstrap
    /// that retries lands on the same state either way), then inserts
    /// `builder`'s rows directly.
    pub fn apply_bootstrap_indexes(&self, builder: BootstrapIndexBuilder) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        clear_tx(&mut tx, &self.chunk_ref)?;
        clear_tx(&mut tx, &self.chunk_ref_by_ino)?;
        clear_tx(&mut tx, &self.xattr_by_name)?;
        for (hash, ino) in &builder.chunk_hashes {
            tx.insert(&self.chunk_ref, misc::cr_key(hash, *ino), Vec::new());
            tx.insert(
                &self.chunk_ref_by_ino,
                misc::cri_key(*ino, hash),
                Vec::new(),
            );
        }
        for (name, ino, value) in &builder.xattr_by_name {
            misc::xattr_by_name_put_tx(&mut tx, &self.xattr_by_name, name, *ino, value);
        }
        crate::store::kv_set_tx(
            &mut tx,
            &self.local,
            KV_USAGE_BYTES,
            &builder.usage_bytes.to_string(),
        );
        crate::store::kv_set_tx(
            &mut tx,
            &self.local,
            KV_USAGE_FILES,
            &builder.usage_files.to_string(),
        );
        tx.commit()?;
        self.usage_tracker()
            .reseat(builder.usage_bytes, builder.usage_files);
        Ok(())
    }

    /// Wipe `dirty` outright. `Meta::open`'s genesis root insert always
    /// dirties `ROOT_INO`'s key speculatively (a truly fresh filesystem
    /// has no commit to bootstrap from, so that insert has to be able to
    /// stand on its own); a bootstrap that then ingests a real commit
    /// over it must retract that guess; the ingested tree already equals
    /// the published one by construction, so the honest post-bootstrap
    /// dirty set is empty, not "whatever genesis guessed".
    pub fn clear_all_dirty(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        clear_tx(&mut tx, &self.dirty)?;
        tx.commit()?;
        Ok(())
    }

    /// The crash-safety boundary for reintegration: after commit the
    /// namespace and disposition ledger cannot disagree.
    ///
    /// Re-expressed from the old engine's `ATTACH DATABASE` + `INSERT
    /// ... SELECT` (fjall has no cross-database transaction — the
    /// reconciled state instead arrives as an already-open side `Meta`,
    /// typically bootstrapped into its own directory/tempdir from the
    /// shared log). One `write_tx` on `self`: wipe and bulk-copy `ns`
    /// and `orphans` from a snapshot of `side`, rebuild the derived
    /// `chunk_ref`/`xattr_by_name` indexes from the new `ns` (the old
    /// engine's "namespace replaced wholesale, so every reverse-index
    /// row is stale" reasoning still holds), adopt only `side`'s
    /// `applied_seq`, then mark every reconciled journal row's
    /// disposition and delete it, and journal the reconciliation's own
    /// `output` records — all inside the one transaction, so the
    /// namespace/disposition/journal surgery is one atomic unit exactly
    /// as it was under SQLite.
    pub fn commit_reintegration_batch(
        &self,
        side: &Meta,
        dispositions: &[(u64, String, String)],
        output: &[LogRecord],
    ) -> Result<(), MetaError> {
        let side_snap = side.db.read_tx();
        let mut tx = self.db.write_tx();
        // Reintegration replaces the whole namespace, so every key that
        // was live before or is live after has to be treated as changed
        // for the next publish's sake: dirty each existing key while
        // clearing it, then dirty each key the copy-in inserts. A key
        // the reconciliation left unchanged is simply dirtied twice,
        // which costs an extra harmless entry, not correctness.
        let old_ns_keys: Vec<Vec<u8>> = tx
            .iter(&self.ns)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in old_ns_keys {
            ns::ns_remove(&mut tx, &self.ns, self.dirty_for_ns(), k)?;
        }
        clear_tx(&mut tx, &self.orphans)?;
        clear_tx(&mut tx, &self.atime)?;
        for guard in side_snap.iter(&side.ns) {
            let (k, v) = guard.into_inner()?;
            ns::ns_insert(
                &mut tx,
                &self.ns,
                self.dirty_for_ns(),
                k.to_vec(),
                v.to_vec(),
            )?;
        }
        for guard in side_snap.iter(&side.orphans) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.orphans, k.to_vec(), v.to_vec());
        }
        for guard in side_snap.iter(&side.atime) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.atime, k.to_vec(), v.to_vec());
        }
        // `side`'s `ns` may reference `Payload::Spilled` bodies that only
        // exist in `side.blobs`; without this, resolving them against
        // `self.blobs` after the swap would silently come back empty.
        // Content-addressed, so a plain union copy is safe — any body
        // this replica no longer references becomes an ordinary orphan
        // (no local blob GC yet, same as everywhere else in M1).
        for guard in side_snap.iter(&side.blobs) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.blobs, k.to_vec(), v.to_vec());
        }
        rebuild_indexes_tx(
            &mut tx,
            &self.ns,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.xattr_by_name,
        )?;
        if let Some(v) = side_snap.get(&side.local, KV_APPLIED_SEQ.as_bytes())? {
            tx.insert(&self.local, KV_APPLIED_SEQ.as_bytes().to_vec(), v.to_vec());
        }
        for (seq, disposition, detail) in dispositions {
            journal::mark_disposition_tx(&mut tx, &self.reintegration, *seq, disposition, detail)?;
            tx.remove(&self.journal_ks, seq.to_be_bytes().to_vec());
        }
        for record in output {
            journal::append_tx(&mut tx, &self.journal_ks, &self.local, record)?;
        }
        tx.commit()?;
        Ok(())
    }
}
