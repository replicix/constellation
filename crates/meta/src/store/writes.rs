//! `impl MetaStore for Meta`, the `*_at` (pre-allocated-ino) variants
//! used by replay/reintegration, and `publish_file` (scratch → shared
//! publish).

use crate::error::MetaError;
use crate::hlc::now_ns;
use crate::record::LogRecord;
use crate::store::{alloc_ino_tx, atime, journal, misc, ns, Meta};
use crate::{DirEntry, MetaStore, RawDirEntry, SetXattrMode};
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::{FileAttr, Ino, InodeKind};
use constellation_mtree::keys;
use constellation_mtree::record::{self, Attrs, DentryRecord, InodeRecord, Kind};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

fn mask_mode(mode: u32) -> u32 {
    mode & 0o7777
}

/// The ctime a manifest commit at `t` leaves a file whose row says
/// `row_ctime`, `session_ctime` being the writer's last data change: the
/// file's last change, not the commit's time — a close (and its commit,
/// maybe seconds later under `--write-mode back`) is no change of its own
/// (ext4: `write; fchmod; close` keeps the `fchmod`'s ctime). Any change
/// since the last write (`fchmod`, `fchown`, `fsetxattr`, a `futimens`,
/// another node's) stamped the row itself, so the later of the two is the
/// last change. Never the mtime: a `futimens` may have set that anywhere,
/// an hour ahead included (`tar x`, `cp -p`, `touch -d`).
///
/// Capped at `t` because the result is also the record's stamp, and a
/// stamp above the sequencer's clock would let its next change of the
/// file (stamped from that clock, which never observes the records it
/// appends itself; [`crate::hlc::observe`] runs on replay) carry a lower
/// ctime — ctime going back for every observer. The caller observes
/// `session_ctime` first (`Meta::set_manifest_tx`), so a writer's clock
/// ahead of the sequencer's by less than the skew tolerance moves the
/// sequencer's up instead of being cut down by the cap. One further ahead
/// is not observed ([`crate::hlc::observe_within_tolerance`]: a bad
/// client clock must not drag every clock for good), and the cap cuts
/// that commit's ctime to `t` — only that writer sees its ctime drop, by
/// its own skew. Without a writer's change (a scan, fsck, recovery) the
/// commit's time, as before.
pub(crate) fn commit_ctime(row_ctime: i64, session_ctime: Option<i64>, t: i64) -> i64 {
    session_ctime.map_or(t, |session| row_ctime.max(session.min(t)))
}

/// The shared body of `mkdir`/`create`/`symlink`/`mknod` (and their
/// `_at` twins): existence checks, the `0x01`/`0x02`/`0x04` writes, the
/// `atime` seed, and the parent-directory touch (nlink+1 for a new
/// subdirectory, mtime/ctime only otherwise).
#[allow(clippy::too_many_arguments)]
fn insert_new_node(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    atime_ks: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
    ino: Ino,
    attrs: Attrs,
    manifest: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
) -> Result<(), MetaError> {
    ns::require_dir(tx, ns_ks, parent)?;
    if ns::child_ino(tx, ns_ks, parent, name)?.is_some() {
        return Err(MetaError::Exists);
    }
    if ns::get_inode_record(tx, ns_ks, ino)?.is_some() {
        return Err(MetaError::Exists);
    }
    ns::put_inode(
        tx,
        ns_ks,
        dirty,
        blobs,
        ino,
        attrs,
        manifest,
        symlink_target,
        &[],
        None,
    )?;
    ns::put_dentry(tx, ns_ks, dirty, parent, name, ino, attrs)?;
    atime::set_atime_tx(tx, atime_ks, ino, attrs.mtime_ns);
    if attrs.kind == Kind::Dir {
        misc::bump_nlink_tx(tx, ns_ks, dirty, parent, 1, attrs.mtime_ns)?;
    } else {
        misc::touch_times_tx(tx, ns_ks, dirty, parent, attrs.mtime_ns)?;
    }
    Ok(())
}

impl Meta {
    pub fn max_journal_seq(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::max_seq(&r, &self.journal_ks, &self.local)
    }

    pub fn peek_journal_after(&self, after_seq: u64) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::peek_after(&r, &self.journal_ks, after_seq)
    }

    /// The journal rows `journal_seqs` shipped in the segment at
    /// `applied_seq`: delete them, advance `applied_seq`, and retire the
    /// transactions they complete (plan 30 §M3b: their `journal_tx` rows,
    /// and their `Local` speculation — see `spec::retire_local_tx`).
    /// The transaction-level body of [`Self::ack_journal_rows_at`]:
    /// retire the shipped transactions and delete their rows. Plan 30
    /// §M11 also calls it for a delegate's own transactions that came back
    /// through a segment.
    pub(crate) fn ack_rows_tx(
        &self,
        tx: &mut fjall::SingleWriterWriteTx,
        journal_seqs: &[u64],
        applied_seq: u64,
    ) -> Result<(), MetaError> {
        use crate::store::spec::{retire_local_tx, retire_tx, Shipped};
        // Plan 43 C1-4: rows of this node's own tenure that shipped cover
        // member copies (a re-shipped transaction names the one it carries).
        self.member_cover_shipped_tx(tx, journal_seqs)?;
        // Plan 30 §M11: the root's own ops forwarded to a delegate are
        // shadows here (§M6) that no segment apply completes: the
        // completions this ship carries retire them, as the segment
        // would on any other replica.
        if crate::store::counter_get(tx, &self.local, crate::store::KV_SPEC_LIVE_COUNT)? > 0 {
            let mut completes = std::collections::HashSet::new();
            for seq in journal_seqs {
                if let Some(v) = tx.get(&self.journal_ks, crate::store::journal::seq_key(*seq))? {
                    match LogRecord::from_postcard(&v)? {
                        LogRecord::Completed { rid } | LogRecord::Refused { rid, .. } => {
                            completes.insert(rid);
                        }
                        _ => {}
                    }
                }
            }
            if !completes.is_empty() {
                retire_tx(tx, self, &completes, applied_seq, None, &Default::default())?;
            }
        }
        if let Some(&upto) = journal_seqs.iter().max() {
            // Plan 30 §M4: exactly these rows shipped — a held-back
            // transaction between them stays outstanding (`store::held`).
            // The ordinary ship is one contiguous run, checked without a
            // set (round 2: no per-ack allocation on the hot path).
            let first = journal_seqs.iter().copied().min().unwrap_or(upto);
            let set: std::collections::HashSet<u64>;
            let only = if upto - first + 1 == journal_seqs.len() as u64 {
                Shipped::Run(first)
            } else {
                set = journal_seqs.iter().copied().collect();
                Shipped::Set(&set)
            };
            let retired = retire_local_tx(tx, self, upto, applied_seq, Some(only))?;
            if retired.held_below {
                self.held_work
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        crate::store::journal::ack_rows_at(
            tx,
            &self.journal_ks,
            &self.local,
            journal_seqs,
            applied_seq,
        )
    }

    pub fn ack_journal_rows_at(
        &self,
        journal_seqs: &[u64],
        applied_seq: u64,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        // Under the write lock: a row this transaction does not see is
        // committed after it, and noted after that (`clear_unshipped_since`).
        let gen = self.unshipped_gen();
        self.ack_rows_tx(&mut tx, journal_seqs, applied_seq)?;
        // Plan 30 §M5: with nothing left unshipped, no key is behind an
        // unshipped record any more (`Meta::unshipped_overlaps`).
        // One seek, not a count of the backlog: this runs under the writer
        // lock on every ship.
        let empty = crate::store::journal::is_empty(&tx, &self.journal_ks, &self.local)?;
        tx.commit()?;
        #[cfg(test)]
        after_ack_commit::fire(self);
        if empty {
            self.clear_unshipped_since(gen);
        }
        // Plan 30 §M13: what shipped is in `completed`; `recent` need not
        // remember it any more (see `Meta::prune_recent_shipped`).
        if let Some(&upto) = journal_seqs.iter().max() {
            self.prune_recent_shipped(upto)?;
        }
        Ok(())
    }

    /// Plan 29 M0a removed namespace partitions, so there is exactly one
    /// implicit partition (`"p0"`) and this degenerates to `[("p0",
    /// batch)]` (or `[]` if the journal is empty) — kept for shipper
    /// call-site compatibility.
    ///
    /// Plan 30 §M3b: the batch always ends on a transaction boundary (it
    /// may run past `max_per_part` to reach one), so a caller shipping it
    /// whole never splits an op's records from its `Completed { rid }`.
    ///
    /// Plan 30 §M4: transactions held back behind an unrecoverable pending
    /// chunk are skipped, and whatever does not depend on them ships
    /// (`store::held`).
    pub fn take_journal_grouped(
        &self,
        max_per_part: usize,
    ) -> Result<Vec<(String, crate::store::JournalBatch)>, MetaError> {
        let batch = self.take_shippable(max_per_part)?;
        if batch.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![("p0".to_string(), batch)])
        }
    }

    // --------------------------------------------------- *_at (fixed ino)

    pub fn mkdir_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Dir,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 2,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mkdir {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    pub fn create_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::File,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Create {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, 0, 1)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(0, 1);
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    pub fn symlink_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Symlink,
            mode: 0o777,
            uid,
            gid,
            nlink: 1,
            size: target.len() as u64,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            Some(target.as_bytes().to_vec()),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Symlink {
                parent,
                name: name.to_string(),
                ino,
                target: target.to_string(),
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mknod_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: constellation_types::Rdev,
    ) -> Result<FileAttr, MetaError> {
        if !kind.is_special() {
            return Err(MetaError::Invalid("mknod kind".into()));
        }
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: ns::kind_to_mtree(kind),
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mknod {
                parent,
                name: name.to_string(),
                ino,
                kind: kind.as_u8(),
                mode: attrs.mode,
                uid,
                gid,
                rdev,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    // -------------------------------------------------------- manifest

    #[allow(clippy::too_many_arguments)]
    fn set_manifest_tx(
        tx: &mut SingleWriterWriteTx,
        ns_ks: &SingleWriterTxKeyspace,
        dirty: ns::Dirty,
        blobs: &SingleWriterTxKeyspace,
        chunk_ref: &SingleWriterTxKeyspace,
        chunk_ref_by_ino: &SingleWriterTxKeyspace,
        journal_ks: &SingleWriterTxKeyspace,
        local: &SingleWriterTxKeyspace,
        completed: &SingleWriterTxKeyspace,
        ino: Ino,
        expected_base: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
        mtime: Option<i64>,
        session_ctime: Option<i64>,
    ) -> Result<i64, MetaError> {
        let Some(rec) = ns::get_inode_record(tx, ns_ks, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let current = rec
            .manifest
            .as_ref()
            .map(|p| ns::resolve_payload(tx, blobs, p))
            .transpose()?;
        if let (Some(base), Some(cur)) = (expected_base, current.as_deref()) {
            if cur != base && cur != manifest {
                return Err(MetaError::Conflict);
            }
        }
        let journal_base = expected_base
            .map(<[u8]>::to_vec)
            .or_else(|| current.clone());
        // A writer's clock ahead of ours (a forwarded close): the
        // commit's stamp and every later one of ours stay above its
        // change, as they do above a peer's record we apply (plan 30
        // §M12) — within the skew tolerance only: the stamp is any
        // client's, and a bad clock's would drag every clock for good.
        if let Some(session) = session_ctime {
            crate::hlc::observe_within_tolerance(session);
        }
        let t = now_ns();
        let old_size = rec.attrs.size as i64;
        let mut attrs = rec.attrs;
        attrs.size = size;
        attrs.mtime_ns = mtime.unwrap_or(t);
        attrs.ctime_ns = commit_ctime(rec.attrs.ctime_ns, session_ctime, t);
        let target = rec
            .symlink_target
            .as_ref()
            .map(|p| ns::resolve_payload(tx, blobs, p))
            .transpose()?;
        ns::put_inode_keeping_xattrs(
            tx,
            ns_ks,
            dirty,
            blobs,
            ino,
            &rec,
            attrs,
            Some(manifest.to_vec()),
            target,
        )?;
        misc::track_manifest_transition_tx(
            tx,
            chunk_ref,
            chunk_ref_by_ino,
            ino,
            current.as_deref(),
            Some(manifest),
        )?;
        journal::append_tx(
            tx,
            journal_ks,
            local,
            completed,
            &LogRecord::WriteManifest {
                ino,
                base_manifest: journal_base,
                manifest: manifest.to_vec(),
                size,
                time_ns: attrs.ctime_ns,
                mtime_ns: attrs.mtime_ns,
            },
        )?;
        Ok(size as i64 - old_size)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_manifest_dirty(
        &self,
        ino: Ino,
        base_manifest: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
        mtime_ns: Option<i64>,
        ctime_ns: Option<i64>,
        dirty_hashes: &[constellation_fs_core::ChunkHash],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        // The holder's own manifest commit has no client rid, so a
        // deposition replays it by the transaction's local replay rid
        // (`spec::replay_rid_for`). It carries that rid's `Completed`
        // like any executed op, so the replay can tell whether the
        // commit reached the log anyway — plan 30 §M9: a backup that took
        // the lease over adopted it from its backup tail. Without the
        // marker the replay re-evaluated it against a file the successor
        // had written since and materialized a conflict copy of a version
        // that was never lost (harness `deposed-reintegration-backup`).
        let rid = crate::store::spec::replay_rid_for(&tx, self, local.start())?;
        let _completion = crate::store::journal::PendingCompletion::set(Some(rid));
        let delta = Self::set_manifest_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.journal_ks,
            &self.local,
            &self.completed,
            ino,
            base_manifest,
            manifest,
            size,
            mtime_ns,
            ctime_ns,
        )?;
        for hash in dirty_hashes {
            crate::store::misc::add_pending_claim_tx(&mut tx, self, hash, ino)?;
        }
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta, 0);
        // Plan 30 §M5's unshipped key set is fed by `mutate::execute`; a
        // holder's own whole-file manifest commit (the FUSE flush) comes
        // here instead and must join it too: a forward reply's `base`
        // (M5) and a ReadIndex position (M8) both ask whether unshipped
        // work touched this inode.
        self.note_unshipped_ino(ino);
        Ok(())
    }

    /// Commit `manifest` as `ino`'s content, if its current manifest is
    /// still `base_manifest` (or already `manifest`). `mtime_ns`: the
    /// file's mtime (the writer's last change, or a time set since);
    /// `ctime_ns`: the writer's last data change ([`commit_ctime`]).
    /// `None` stamps the commit's time.
    pub fn set_manifest_with_base(
        &self,
        ino: Ino,
        base_manifest: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
        mtime_ns: Option<i64>,
        ctime_ns: Option<i64>,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let delta = Self::set_manifest_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.journal_ks,
            &self.local,
            &self.completed,
            ino,
            base_manifest,
            manifest,
            size,
            mtime_ns,
            ctime_ns,
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta, 0);
        Ok(())
    }

    /// Atomic scratch → shared publish: creates `parent/name`, commits
    /// its manifest and xattrs, in one transaction. `noreplace`
    /// (`renameat2(RENAME_NOREPLACE)`): refuse with `Exists` if another
    /// inode holds `parent/name`, decided inside that transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_file(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_ns: i64,
        manifest: &[u8],
        size: u64,
        xattrs: &[(String, Vec<u8>)],
        noreplace: bool,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        ns::require_dir(&tx, &self.ns, parent)?;
        let mut delta_bytes: i64 = 0;
        let mut delta_files: i64 = 0;
        if let Some(old) = ns::get_dentry_record(&tx, &self.ns, parent, name)? {
            if noreplace {
                // This very publish, already executed (a retry): done.
                return if old.ino == ino {
                    Ok(())
                } else {
                    Err(MetaError::Exists)
                };
            }
            let Some(old_rec) = ns::get_inode_record(&tx, &self.ns, old.ino)? else {
                return Err(MetaError::NoEnt(old.ino));
            };
            if old_rec.attrs.kind == Kind::Dir {
                return Err(MetaError::IsDir);
            }
            let old_manifest = old_rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            if old_manifest.as_deref() == Some(manifest) {
                return Ok(());
            }
            let t = now_ns();
            ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, old.ino)?;
            if old_rec.attrs.nlink <= 1 {
                misc::track_manifest_transition_tx(
                    &mut tx,
                    &self.chunk_ref,
                    &self.chunk_ref_by_ino,
                    old.ino,
                    old_manifest.as_deref(),
                    None,
                )?;
                let mut orphan_rec = old_rec.clone();
                orphan_rec.attrs.nlink = 0;
                orphan_rec.attrs.ctime_ns = t;
                ns::orphan_inode(
                    &mut tx,
                    &self.ns,
                    dirty,
                    &self.blobs,
                    &self.xattr_by_name,
                    &self.orphans,
                    old.ino,
                    &old_rec,
                    orphan_rec,
                )?;
                ns::ns_remove(&mut tx, &self.ns, dirty, keys::inode(old.ino))?;
                if old_rec.attrs.kind == Kind::File {
                    delta_bytes -= old_rec.attrs.size as i64;
                    delta_files -= 1;
                }
            } else {
                misc::bump_file_nlink_tx(&mut tx, &self.ns, dirty, old.ino, -1, t)?;
            }
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::Unlink {
                    parent,
                    name: name.to_string(),
                    time_ns: t,
                },
            )?;
        }
        if ns::get_inode_record(&tx, &self.ns, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
        let ctime_ns = now_ns();
        let attrs = Attrs {
            kind: Kind::File,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size,
            mtime_ns,
            ctime_ns,
            rdev: constellation_types::Rdev::default(),
        };
        ns::put_dentry(&mut tx, &self.ns, dirty, parent, name, ino, attrs)?;
        misc::touch_times_tx(&mut tx, &self.ns, dirty, parent, ctime_ns)?;
        atime::set_atime_tx(&mut tx, &self.atime, ino, mtime_ns);
        let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
            .iter()
            .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
            .collect();
        ns::put_inode(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            ino,
            attrs,
            Some(manifest.to_vec()),
            None,
            &xattr_pairs,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Create {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                // Plan 30 §M12: the stamp the parent was touched with (its
                // replay merges the record's stamp into the parent).
                time_ns: ctime_ns,
            },
        )?;
        misc::track_manifest_transition_tx(
            &mut tx,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            ino,
            None,
            Some(manifest),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::WriteManifest {
                ino,
                base_manifest: None,
                manifest: manifest.to_vec(),
                size,
                time_ns: ctime_ns,
                mtime_ns,
            },
        )?;
        for (name, value) in xattrs {
            misc::xattr_by_name_put_tx(&mut tx, &self.xattr_by_name, name, ino, value);
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::SetXattr {
                    ino,
                    name: name.clone(),
                    value: value.clone(),
                    time_ns: mtime_ns,
                },
            )?;
        }
        delta_bytes += size as i64;
        delta_files += 1;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta_bytes, delta_files)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta_bytes, delta_files);
        Ok(())
    }
}

// ------------------------------------------------------------- rename

#[allow(clippy::too_many_arguments)]
fn rename_in_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    orphans_ks: &SingleWriterTxKeyspace,
    xattr_by_name_ks: &SingleWriterTxKeyspace,
    chunk_ref: &SingleWriterTxKeyspace,
    chunk_ref_by_ino: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
    new_parent: Ino,
    new_name: &str,
    noreplace: bool,
) -> Result<Option<(Ino, i64, i64, i64)>, MetaError> {
    let Some(src) = ns::get_dentry_record(tx, ns_ks, parent, name)? else {
        return Err(MetaError::NoEntry);
    };
    let ino = src.ino;
    let Some(src_rec) = ns::get_inode_record(tx, ns_ks, ino)? else {
        return Err(MetaError::NoEnt(ino));
    };
    ns::require_dir(tx, ns_ks, new_parent)?;

    if src_rec.attrs.kind == Kind::Dir && is_within(tx, ns_ks, new_parent, ino)? {
        return Err(MetaError::Invalid("rename into own subtree".into()));
    }

    let t = now_ns();
    let mut delta = (0i64, 0i64);
    if let Some(existing) = ns::get_dentry_record(tx, ns_ks, new_parent, new_name)? {
        // `RENAME_NOREPLACE`: any entry at the target refuses, even the
        // source's own other name or the source itself (Linux answers
        // `EEXIST` before its same-inode no-op). Decided here, inside the
        // committing transaction, so it is atomic against every other
        // mutation this replica (the sequencer, for a forwarded op) runs.
        if noreplace {
            return Err(MetaError::Exists);
        }
        if existing.ino == ino {
            return Ok(None);
        }
        let Some(existing_rec) = ns::get_inode_record(tx, ns_ks, existing.ino)? else {
            return Err(MetaError::NoEnt(existing.ino));
        };
        if src_rec.attrs.kind == Kind::Dir && existing_rec.attrs.kind != Kind::Dir {
            return Err(MetaError::NotDir);
        }
        if src_rec.attrs.kind != Kind::Dir && existing_rec.attrs.kind == Kind::Dir {
            return Err(MetaError::IsDir);
        }
        // The name goes first: an inode left with one name (2 → 1 below)
        // gets its copy from the `nlink` write, for that one name only.
        ns::remove_dentry(tx, ns_ks, dirty, new_parent, new_name, existing.ino)?;
        if existing_rec.attrs.kind == Kind::Dir {
            if ns::has_children(tx, ns_ks, existing.ino)? {
                return Err(MetaError::NotEmpty);
            }
            // An orphan, as `rmdir`'s (`ns::orphan_dir`).
            let mut orphan_rec = existing_rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            ns::orphan_inode(
                tx,
                ns_ks,
                dirty,
                blobs,
                xattr_by_name_ks,
                orphans_ks,
                existing.ino,
                &existing_rec,
                orphan_rec,
            )?;
            ns::ns_remove(tx, ns_ks, dirty, keys::inode(existing.ino))?;
            misc::bump_nlink_tx(tx, ns_ks, dirty, new_parent, -1, t)?;
        } else if existing_rec.attrs.nlink <= 1 {
            let manifest_bytes = existing_rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(tx, blobs, p))
                .transpose()?;
            misc::track_manifest_transition_tx(
                tx,
                chunk_ref,
                chunk_ref_by_ino,
                existing.ino,
                manifest_bytes.as_deref(),
                None,
            )?;
            let mut orphan_rec = existing_rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            ns::orphan_inode(
                tx,
                ns_ks,
                dirty,
                blobs,
                xattr_by_name_ks,
                orphans_ks,
                existing.ino,
                &existing_rec,
                orphan_rec,
            )?;
            ns::ns_remove(tx, ns_ks, dirty, keys::inode(existing.ino))?;
            if existing_rec.attrs.kind == Kind::File {
                delta.0 -= existing_rec.attrs.size as i64;
                delta.1 -= 1;
            }
        } else {
            misc::bump_file_nlink_tx(tx, ns_ks, dirty, existing.ino, -1, t)?;
        }
    }

    ns::ns_remove(tx, ns_ks, dirty, keys::dentry(parent, name.as_bytes()))?;
    ns::ns_remove(
        tx,
        ns_ks,
        dirty,
        keys::rdentry(ino, parent, name.as_bytes()),
    )?;
    ns::ns_insert(
        tx,
        ns_ks,
        dirty,
        keys::dentry(new_parent, new_name.as_bytes()),
        DentryRecord::for_inode(ino, &src_rec.attrs).encode(),
    )?;
    ns::ns_insert(
        tx,
        ns_ks,
        dirty,
        keys::rdentry(ino, new_parent, new_name.as_bytes()),
        record::RDENTRY_VALUE.to_vec(),
    )?;

    if src_rec.attrs.kind == Kind::Dir && parent != new_parent {
        misc::bump_nlink_tx(tx, ns_ks, dirty, parent, -1, t)?;
        misc::bump_nlink_tx(tx, ns_ks, dirty, new_parent, 1, t)?;
    }
    misc::touch_times_tx(tx, ns_ks, dirty, parent, t)?;
    misc::touch_times_tx(tx, ns_ks, dirty, new_parent, t)?;
    // The moved inode's ctime too, as Linux file systems do (and as
    // `exchange_in_tx` does for both): a zero `nlink` delta is a
    // ctime-only touch that rewrites every dentry copy, the new one
    // included.
    misc::bump_file_nlink_tx(tx, ns_ks, dirty, ino, 0, t)?;
    Ok(Some((ino, t, delta.0, delta.1)))
}

/// Whether directory `dir` is `ancestor` or lies beneath it (the walk up
/// `0x04` reverse entries that keeps a directory from moving into its own
/// subtree).
fn is_within(
    tx: &SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dir: Ino,
    ancestor: Ino,
) -> Result<bool, MetaError> {
    let mut cursor = dir;
    loop {
        if cursor == ancestor {
            return Ok(true);
        }
        if cursor == ROOT_INO {
            return Ok(false);
        }
        match ns::parent_of(tx, ns_ks, cursor)? {
            Some(p) => cursor = p,
            None => return Ok(false),
        }
    }
}

// ----------------------------------------------------------- exchange

/// What [`exchange_in_tx`] did.
pub(crate) enum Exchanged {
    /// The two names were swapped: `(src_ino, dst_ino)`.
    Swapped(Ino, Ino),
    /// One name, or two names of one inode: nothing to do (Linux's
    /// `vfs_rename` returns 0 for `source == target`).
    Same,
    /// A name or a parent is gone. Only reported with `validate: false`
    /// (replay skips); the local op refuses instead.
    Missing,
}

/// The body of `renameat2(RENAME_EXCHANGE)`, shared by the local op
/// (`validate: true`: a missing entry is `NoEntry`, a directory swapped
/// beneath itself `Invalid`) and replay (`validate: false`: the holder
/// already decided, so a replica swaps what the names hold and reports a
/// missing one for the caller to skip).
///
/// Swaps the two dentries and their reverse entries; when a directory and
/// a non-directory trade places across two parents, the directory's `..`
/// link moves with it (`nlink` -1 on the parent it left, +1 on the one it
/// entered; two directories swapped across parents leave both counts
/// unchanged). Both parents get an mtime/ctime touch and both inodes a
/// ctime touch (the kernel's `d_exchange` leaves them with new names, a
/// status change), all as `max` merges so the replay order commutes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn exchange_in_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    parent: Ino,
    name: &str,
    new_parent: Ino,
    new_name: &str,
    t: i64,
    validate: bool,
) -> Result<Exchanged, MetaError> {
    let missing = |e: MetaError| {
        if validate {
            Err(e)
        } else {
            Ok(Exchanged::Missing)
        }
    };
    if validate {
        ns::require_dir(tx, ns_ks, parent)?;
        ns::require_dir(tx, ns_ks, new_parent)?;
    } else if ns::get_inode_record(tx, ns_ks, parent)?.is_none()
        || ns::get_inode_record(tx, ns_ks, new_parent)?.is_none()
    {
        return Ok(Exchanged::Missing);
    }
    let Some(src) = ns::get_dentry_record(tx, ns_ks, parent, name)? else {
        return missing(MetaError::NoEntry);
    };
    let Some(dst) = ns::get_dentry_record(tx, ns_ks, new_parent, new_name)? else {
        return missing(MetaError::NoEntry);
    };
    if (parent == new_parent && name == new_name) || src.ino == dst.ino {
        return Ok(Exchanged::Same);
    }
    let Some(src_rec) = ns::get_inode_record(tx, ns_ks, src.ino)? else {
        return missing(MetaError::NoEnt(src.ino));
    };
    let Some(dst_rec) = ns::get_inode_record(tx, ns_ks, dst.ino)? else {
        return missing(MetaError::NoEnt(dst.ino));
    };
    let src_dir = src_rec.attrs.kind == Kind::Dir;
    let dst_dir = dst_rec.attrs.kind == Kind::Dir;
    // Neither directory may land beneath itself (Linux: `EINVAL` when one
    // entry is an ancestor of the other).
    if validate
        && ((src_dir && is_within(tx, ns_ks, new_parent, src.ino)?)
            || (dst_dir && is_within(tx, ns_ks, parent, dst.ino)?))
    {
        return Err(MetaError::Invalid("exchange into own subtree".into()));
    }
    ns::remove_dentry(tx, ns_ks, dirty, parent, name, src.ino)?;
    ns::remove_dentry(tx, ns_ks, dirty, new_parent, new_name, dst.ino)?;
    ns::put_dentry(tx, ns_ks, dirty, parent, name, dst.ino, dst_rec.attrs)?;
    ns::put_dentry(
        tx,
        ns_ks,
        dirty,
        new_parent,
        new_name,
        src.ino,
        src_rec.attrs,
    )?;
    if parent != new_parent && src_dir != dst_dir {
        let (left, entered) = if src_dir {
            (parent, new_parent)
        } else {
            (new_parent, parent)
        };
        misc::bump_nlink_tx(tx, ns_ks, dirty, left, -1, t)?;
        misc::bump_nlink_tx(tx, ns_ks, dirty, entered, 1, t)?;
    }
    misc::touch_times_tx(tx, ns_ks, dirty, parent, t)?;
    misc::touch_times_tx(tx, ns_ks, dirty, new_parent, t)?;
    // A zero `nlink` delta is a ctime-only touch (and rewrites every
    // dentry copy of the attributes, the two just swapped included).
    misc::bump_file_nlink_tx(tx, ns_ks, dirty, src.ino, 0, t)?;
    misc::bump_file_nlink_tx(tx, ns_ks, dirty, dst.ino, 0, t)?;
    Ok(Exchanged::Swapped(src.ino, dst.ino))
}

impl Meta {
    /// `renameat2(RENAME_NOREPLACE)`: [`MetaStore::rename`], refusing
    /// with [`MetaError::Exists`] when the target name exists — decided
    /// in the same transaction that commits the rename.
    pub fn rename_noreplace(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        self.rename_impl(parent, name, new_parent, new_name, true)
    }

    fn rename_impl(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        noreplace: bool,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        // M16: the entry a rename replaces, read in its own transaction
        // (for the read-delegation recall, `readdeleg::note_victim`).
        let replaced = ns::get_dentry_record(&tx, &self.ns, new_parent, new_name)?.map(|d| d.ino);
        let result = rename_in_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.orphans,
            &self.xattr_by_name,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.blobs,
            parent,
            name,
            new_parent,
            new_name,
            noreplace,
        )?;
        if let Some((_ino, t, db, df)) = result {
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::Rename {
                    parent,
                    name: name.to_string(),
                    new_parent,
                    new_name: new_name.to_string(),
                    time_ns: t,
                },
            )?;
            crate::store::adjust_usage_tx(&mut tx, &self.local, db, df)?;
        }
        let _observed = match result {
            Some(_) => None,
            None => complete_noop_tx(
                &mut tx,
                self,
                [(parent, name), (new_parent, new_name)],
                replaced,
            )?,
        };
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        if let Some((moved, _, db, df)) = result {
            crate::readdeleg::note_victim(moved);
            if let Some(r) = replaced.filter(|r| *r != moved) {
                crate::readdeleg::note_victim(r);
            }
            self.usage_tracker().adjust(db, df);
        }
        Ok(())
    }

    /// `setattr` on an unlinked-but-open inode (its record lives in
    /// `orphans`, DESIGN.md §3): applied to that record only, journaling
    /// nothing — the inode has no name, so no other replica can reach it
    /// and nothing may ship. POSIX lets `fchmod`/`fchown`/`ftruncate`/
    /// `futimens` on a descriptor of an unlinked file succeed; what the
    /// descriptor's `fstat` reports afterwards is this record (plus a
    /// pending write session's size, which the caller overlays). `None`
    /// when `ino` is not an orphan here.
    #[allow(clippy::too_many_arguments)]
    pub fn orphan_setattr(
        &self,
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<Option<FileAttr>, MetaError> {
        let mut tx = self.db.write_tx();
        if ns::get_inode_record(&tx, &self.ns, ino)?.is_some() {
            return Ok(None);
        }
        let Some(v) = tx.get(&self.orphans, ino.to_be_bytes())? else {
            return Ok(None);
        };
        let mut rec = InodeRecord::decode(&v)?;
        let t = now_ns();
        if let Some(m) = mode {
            rec.attrs.mode = mask_mode(m);
        }
        if let Some(u) = uid {
            rec.attrs.uid = u;
        }
        if let Some(g) = gid {
            rec.attrs.gid = g;
        }
        if let Some(s) = size {
            rec.attrs.size = s;
            rec.attrs.mtime_ns = t;
        }
        if let Some(m) = mtime_ns {
            rec.attrs.mtime_ns = m;
        }
        rec.attrs.ctime_ns = t;
        tx.insert(&self.orphans, ino.to_be_bytes().to_vec(), rec.encode());
        if let Some(a) = atime_ns {
            atime::set_atime_tx(&mut tx, &self.atime, ino, a);
        }
        let at = atime::get_atime(&tx, &self.atime, ino)?;
        tx.commit()?;
        Ok(Some(ns::attrs_to_fileattr(ino, &rec.attrs, at)))
    }

    /// `setxattr` on an unlinked inode a descriptor or the kernel still
    /// references (its record lives in `orphans`, DESIGN.md §3): applied
    /// to its orphan xattrs ([`ns::orphan_xattr_key`]) only, journaling
    /// nothing, as [`Self::orphan_setattr`] — the inode has no name, so
    /// no other replica can reach it. `Ok(false)` when `ino` is not an
    /// orphan here; the mode's refusals as for a named inode.
    pub fn orphan_set_xattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: SetXattrMode,
    ) -> Result<bool, MetaError> {
        self.orphan_xattr_change(ino, name, |existing| match mode {
            SetXattrMode::Create if existing => Err(MetaError::Exists),
            SetXattrMode::Replace if !existing => Err(MetaError::NoData),
            _ => Ok(Some(value.to_vec())),
        })
    }

    /// `removexattr` on an orphan, as [`Self::orphan_set_xattr`].
    pub fn orphan_remove_xattr(&self, ino: Ino, name: &str) -> Result<bool, MetaError> {
        self.orphan_xattr_change(ino, name, |existing| {
            if existing {
                Ok(None)
            } else {
                Err(MetaError::NoData)
            }
        })
    }

    /// One orphan xattr written (`Some`) or removed (`None`) as `change`
    /// decides from whether it exists, with the orphan's ctime.
    fn orphan_xattr_change(
        &self,
        ino: Ino,
        name: &str,
        change: impl FnOnce(bool) -> Result<Option<Vec<u8>>, MetaError>,
    ) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        if ns::get_inode_record(&tx, &self.ns, ino)?.is_some() {
            return Ok(false);
        }
        let Some(v) = tx.get(&self.orphans, ino.to_be_bytes())? else {
            return Ok(false);
        };
        let key = ns::orphan_xattr_key(ino, name.as_bytes());
        let existing = tx.get(&self.orphans, key.clone())?.is_some();
        match change(existing)? {
            Some(value) => tx.insert(&self.orphans, key, value),
            None => tx.remove(&self.orphans, key),
        }
        let mut rec = InodeRecord::decode(&v)?;
        rec.attrs.ctime_ns = now_ns();
        tx.insert(&self.orphans, ino.to_be_bytes().to_vec(), rec.encode());
        tx.commit()?;
        Ok(true)
    }

    /// `renameat2(RENAME_EXCHANGE)`: atomically swap the inodes
    /// `parent/name` and `new_parent/new_name` name (see
    /// [`exchange_in_tx`]), journaling one [`LogRecord::Exchange`].
    pub fn exchange(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let one = ns::get_dentry_record(&tx, &self.ns, parent, name)?.map(|d| d.ino);
        let swapped = exchange_in_tx(
            &mut tx, &self.ns, dirty, parent, name, new_parent, new_name, t, true,
        )?;
        if let Exchanged::Swapped(ino, new_ino) = swapped {
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::Exchange {
                    parent,
                    name: name.to_string(),
                    new_parent,
                    new_name: new_name.to_string(),
                    time_ns: t,
                    ino,
                    new_ino,
                },
            )?;
        }
        let _observed = match swapped {
            Exchanged::Same => {
                complete_noop_tx(&mut tx, self, [(parent, name), (new_parent, new_name)], one)?
            }
            _ => None,
        };
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        if let Exchanged::Swapped(a, b) = swapped {
            crate::readdeleg::note_victim(a);
            crate::readdeleg::note_victim(b);
        }
        Ok(())
    }
}

/// A rename or an exchange that changes nothing (one name onto another
/// name of the same inode, or onto itself: POSIX's no-op) journals no
/// record, so the rid `mutate::execute` armed for it completes on its
/// own: without a `Completed { rid }` row its `Ok` had no log position —
/// no durable dedup, and no wait for the state it read (the link that made
/// the two names one inode) to become durable. The names, their
/// directories and the inode are what it observed
/// (`journal::PendingObserved`, as a refusal's); unlike a refusal's, they
/// order it like a write of them in the ship plan (`store::held`'s
/// `tx_keys_and_row`): behind a deferred row that wrote them, and ahead of
/// a later one of the directory (`flex+links` seeds 4, 191 and 335: the
/// completion shipped ahead of the rename it had seen, and behind a create
/// invoked after it returned). The guard must outlive `finish_local`.
/// Nothing outside `execute`.
fn complete_noop_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    names: [(Ino, &str); 2],
    ino: Option<Ino>,
) -> Result<Option<journal::PendingObservedGuard>, MetaError> {
    let Some(rid) = journal::PendingCompletion::take() else {
        return Ok(None);
    };
    let mut observed: Vec<Vec<u8>> = Vec::new();
    for (parent, name) in names {
        observed.push(keys::dentry(parent, name.as_bytes()));
        observed.push(keys::inode(parent));
    }
    observed.extend(ino.map(keys::inode));
    observed.sort();
    observed.dedup();
    let guard = journal::PendingObserved::set(observed);
    journal::append_completion_tx(tx, &meta.journal_ks, &meta.local, &meta.completed, rid)?;
    Ok(Some(guard))
}

// ----------------------------------------------------------- MetaStore

impl MetaStore for Meta {
    fn lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError> {
        let r = self.db.read_tx();
        let Some(d) = ns::get_dentry_record(&r, &self.ns, parent, name)? else {
            return Ok(None);
        };
        // A name of a hard-linked inode holds no copy (plan 45): its
        // attributes are the inode's.
        let (attrs, read_inode) = ns::dentry_attrs(&r, &self.ns, &d)?;
        if read_inode {
            self.note_inode_read();
        }
        let at = atime::get_atime(&r, &self.atime, d.ino)?;
        Ok(Some(ns::attrs_to_fileattr(d.ino, &attrs, at)))
    }

    fn getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Ok(None);
        };
        let at = atime::get_atime(&r, &self.atime, ino)?;
        Ok(Some(ns::attrs_to_fileattr(ino, &rec.attrs, at)))
    }

    fn readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError> {
        let r = self.db.read_tx();
        ns::require_dir(&r, &self.ns, parent)?;
        ns::readdir_entries(&r, &self.ns, parent, &self.dentries_read)
    }

    fn readdir_after(
        &self,
        parent: Ino,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<RawDirEntry>, MetaError> {
        let r = self.db.read_tx();
        if after.is_none() {
            ns::require_dir(&r, &self.ns, parent)?;
        }
        ns::readdir_after_entries(&r, &self.ns, parent, after, limit, &self.dentries_read)
    }

    fn readlink(&self, ino: Ino) -> Result<Option<String>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Ok(None);
        };
        match &rec.symlink_target {
            Some(p) => Ok(Some(
                String::from_utf8_lossy(&ns::resolve_payload(&r, &self.blobs, p)?).into_owned(),
            )),
            None => Ok(None),
        }
    }

    fn manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Ok(None);
        };
        match &rec.manifest {
            Some(p) => Ok(Some(ns::resolve_payload(&r, &self.blobs, p)?)),
            None => Ok(None),
        }
    }

    fn get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_record(&r, &self.ns, ino)? else {
            if r.get(&self.orphans, ino.to_be_bytes())?.is_none() {
                return Err(MetaError::NoEnt(ino));
            }
            return Ok(r
                .get(&self.orphans, ns::orphan_xattr_key(ino, name.as_bytes()))?
                .map(|v| v.to_vec()));
        };
        if !rec.xattrs.is_empty() {
            return Ok(rec
                .xattrs
                .iter()
                .find(|(n, _)| n == name.as_bytes())
                .map(|(_, v)| v.clone()));
        }
        match r.get(&self.ns, keys::xattr(ino, name.as_bytes()))? {
            Some(v) => Ok(Some(ns::resolve_payload(
                &r,
                &self.blobs,
                &record::Payload::decode(&v)?,
            )?)),
            None => Ok(None),
        }
    }

    fn list_xattrs(&self, ino: Ino) -> Result<Vec<String>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_record(&r, &self.ns, ino)? else {
            if r.get(&self.orphans, ino.to_be_bytes())?.is_none() {
                return Err(MetaError::NoEnt(ino));
            }
            return Ok(ns::orphan_xattrs(&r, &self.orphans, ino)?
                .into_iter()
                .map(|(n, _)| n)
                .collect());
        };
        Ok(ns::all_xattrs(&r, &self.ns, &self.blobs, &rec, ino)?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    fn recursive_size(&self, ino: Ino) -> Result<(u64, u64), MetaError> {
        Meta::recursive_size(self, ino)
    }

    fn usage(&self) -> (u64, u64) {
        self.usage_bytes_files()
    }

    fn quota(&self) -> Result<Option<u64>, MetaError> {
        self.read_quota()
    }

    fn set_quota(&self, max_logical_bytes: Option<u64>) -> Result<(), MetaError> {
        self.write_quota(max_logical_bytes)
    }

    fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Dir,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 2,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mkdir {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::File,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Create {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, 0, 1)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(0, 1);
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Symlink,
            mode: 0o777,
            uid,
            gid,
            nlink: 1,
            size: target.len() as u64,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            Some(target.as_bytes().to_vec()),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Symlink {
                parent,
                name: name.to_string(),
                ino,
                target: target.to_string(),
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn mknod(
        &self,
        parent: Ino,
        name: &str,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: constellation_types::Rdev,
    ) -> Result<FileAttr, MetaError> {
        if !kind.is_special() {
            return Err(MetaError::Invalid("mknod kind".into()));
        }
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: ns::kind_to_mtree(kind),
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mknod {
                parent,
                name: name.to_string(),
                ino,
                kind: kind.as_u8(),
                mode: attrs.mode,
                uid,
                gid,
                rdev,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn link(&self, ino: Ino, parent: Ino, name: &str) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        ns::require_dir(&tx, &self.ns, parent)?;
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        if rec.attrs.kind == Kind::Dir {
            return Err(MetaError::IsDir);
        }
        // link(2): `EEXIST` for a taken name. Before, the new dentry
        // silently overwrote it — the other inode lost a name without its
        // link count or reverse entry following (the conformance kit's
        // `hard_link_refusals`; a kernel's own negative-dentry check hides
        // it on one node, not across nodes).
        if ns::child_ino(&tx, &self.ns, parent, name)?.is_some() {
            return Err(MetaError::Exists);
        }
        let t = now_ns();
        let at = atime::get_atime(&tx, &self.atime, ino)?;
        let rec2 =
            misc::bump_file_nlink_tx(&mut tx, &self.ns, dirty, ino, 1, t)?.expect("checked above");
        ns::put_dentry(&mut tx, &self.ns, dirty, parent, name, ino, rec2.attrs)?;
        misc::touch_times_tx(&mut tx, &self.ns, dirty, parent, t)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Link {
                ino,
                parent,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &rec2.attrs, at))
    }

    fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(d) = ns::get_dentry_record(&tx, &self.ns, parent, name)? else {
            return Err(MetaError::NoEntry);
        };
        let ino = d.ino;
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, ino)?;
            self.finish_local(&mut tx, local)?;
            tx.commit()?;
            return Ok(());
        };
        if rec.attrs.kind == Kind::Dir {
            return Err(MetaError::IsDir);
        }
        let t = now_ns();
        ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, ino)?;
        let mut usage_delta = (0i64, 0i64);
        if rec.attrs.nlink <= 1 {
            let manifest_bytes = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            misc::track_manifest_transition_tx(
                &mut tx,
                &self.chunk_ref,
                &self.chunk_ref_by_ino,
                ino,
                manifest_bytes.as_deref(),
                None,
            )?;
            let mut orphan_rec = rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            ns::orphan_inode(
                &mut tx,
                &self.ns,
                dirty,
                &self.blobs,
                &self.xattr_by_name,
                &self.orphans,
                ino,
                &rec,
                orphan_rec,
            )?;
            ns::ns_remove(&mut tx, &self.ns, dirty, keys::inode(ino))?;
            if rec.attrs.kind == Kind::File {
                usage_delta = (-(rec.attrs.size as i64), -1);
            }
        } else {
            misc::bump_file_nlink_tx(&mut tx, &self.ns, dirty, ino, -1, t)?;
        }
        misc::touch_times_tx(&mut tx, &self.ns, dirty, parent, t)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Unlink {
                parent,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, usage_delta.0, usage_delta.1)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        crate::readdeleg::note_victim(ino);
        self.usage_tracker().adjust(usage_delta.0, usage_delta.1);
        Ok(())
    }

    fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(d) = ns::get_dentry_record(&tx, &self.ns, parent, name)? else {
            return Err(MetaError::NoEntry);
        };
        let ino = d.ino;
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        if rec.attrs.kind != Kind::Dir {
            return Err(MetaError::NotDir);
        }
        if ns::has_children(&tx, &self.ns, ino)? {
            return Err(MetaError::NotEmpty);
        }
        let t = now_ns();
        ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, ino)?;
        ns::orphan_dir(&mut tx, self, dirty, ino, &rec, t)?;
        misc::bump_nlink_tx(&mut tx, &self.ns, dirty, parent, -1, t)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Rmdir {
                parent,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        crate::readdeleg::note_victim(ino);
        Ok(())
    }

    fn rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        self.rename_impl(parent, name, new_parent, new_name, false)
    }

    fn setattr(
        &self,
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(mut rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let t = now_ns();
        let old_size = rec.attrs.size;
        if let Some(m) = mode {
            rec.attrs.mode = mask_mode(m);
        }
        if let Some(u) = uid {
            rec.attrs.uid = u;
        }
        if let Some(g) = gid {
            rec.attrs.gid = g;
        }
        if let Some(s) = size {
            rec.attrs.size = s;
            rec.attrs.mtime_ns = t;
        }
        if let Some(m) = mtime_ns {
            rec.attrs.mtime_ns = m;
        }
        rec.attrs.ctime_ns = t;
        let attrs = rec.attrs;
        // `setattr` leaves `nlink` alone, so the dentries' form stands.
        ns::put_inode_record(&mut tx, &self.ns, dirty, ino, &rec, Some(attrs))?;
        if size.is_some() && attrs.kind == Kind::File {
            // The content past the new size is gone for good (see
            // `replay::clip_manifest`): the log's replay does the same.
            crate::replay::clip_manifest_to_size_tx(&mut tx, self, local.dirty(self), ino)?;
        }
        if let Some(a) = atime_ns {
            atime::set_atime_tx(&mut tx, &self.atime, ino, a);
        }
        let at = atime::get_atime(&tx, &self.atime, ino)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Setattr {
                ino,
                mode,
                uid,
                gid,
                size,
                atime_ns,
                mtime_ns,
                time_ns: t,
            },
        )?;
        // Persisted with the change like every other usage move (it used
        // to be in-memory only), so a rollback of this transaction (plan
        // 30 §M3b) restores the counter it moved.
        let size_delta = if size.is_some() && attrs.kind == Kind::File {
            attrs.size as i64 - old_size as i64
        } else {
            0
        };
        crate::store::adjust_usage_tx(&mut tx, &self.local, size_delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(size_delta, 0);
        Ok(ns::attrs_to_fileattr(ino, &attrs, at))
    }

    fn set_manifest(&self, ino: Ino, manifest: &[u8], size: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let delta = Self::set_manifest_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.journal_ks,
            &self.local,
            &self.completed,
            ino,
            None,
            manifest,
            size,
            None,
            None,
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta, 0);
        Ok(())
    }

    fn set_xattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: SetXattrMode,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let existing = misc::get_one_xattr(&tx, &self.ns, &self.blobs, ino, name)?;
        match mode {
            SetXattrMode::Create if existing.is_some() => return Err(MetaError::Exists),
            SetXattrMode::Replace if existing.is_none() => return Err(MetaError::NoData),
            _ => {}
        }
        let t = now_ns();
        let mut attrs = rec.attrs;
        attrs.ctime_ns = t;
        if !ns::put_spilled_xattr(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            ino,
            &rec,
            attrs,
            name,
            Some(value),
        )? {
            let mut xattrs = ns::all_xattrs(&tx, &self.ns, &self.blobs, &rec, ino)?;
            if let Some(slot) = xattrs.iter_mut().find(|(n, _)| n == name) {
                slot.1 = value.to_vec();
            } else {
                xattrs.push((name.to_string(), value.to_vec()));
            }
            let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
                .into_iter()
                .map(|(n, v)| (n.into_bytes(), v))
                .collect();
            let manifest = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            let target = rec
                .symlink_target
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            ns::put_inode(
                &mut tx,
                &self.ns,
                dirty,
                &self.blobs,
                ino,
                attrs,
                manifest,
                target,
                &xattr_pairs,
                Some(rec.attrs),
            )?;
        }
        misc::xattr_by_name_put_tx(&mut tx, &self.xattr_by_name, name, ino, value);
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::SetXattr {
                ino,
                name: name.to_string(),
                value: value.to_vec(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    fn remove_xattr(&self, ino: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let existing = misc::get_one_xattr(&tx, &self.ns, &self.blobs, ino, name)?;
        if existing.is_none() {
            return Err(MetaError::NoData);
        }
        let t = now_ns();
        let mut attrs = rec.attrs;
        attrs.ctime_ns = t;
        if !ns::put_spilled_xattr(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            ino,
            &rec,
            attrs,
            name,
            None,
        )? {
            let mut xattrs = ns::all_xattrs(&tx, &self.ns, &self.blobs, &rec, ino)?;
            xattrs.retain(|(n, _)| n != name);
            let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
                .into_iter()
                .map(|(n, v)| (n.into_bytes(), v))
                .collect();
            let manifest = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            let target = rec
                .symlink_target
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            ns::put_inode(
                &mut tx,
                &self.ns,
                dirty,
                &self.blobs,
                ino,
                attrs,
                manifest,
                target,
                &xattr_pairs,
                Some(rec.attrs),
            )?;
        }
        misc::xattr_by_name_del_tx(&mut tx, &self.xattr_by_name, name, ino);
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::RemoveXattr {
                ino,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    fn reap_orphan(&self, ino: Ino) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if let Some(v) = tx.get(&self.orphans, ino.to_be_bytes())? {
            let rec = InodeRecord::decode(&v)?;
            let manifest = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            misc::track_manifest_transition_tx(
                &mut tx,
                &self.chunk_ref,
                &self.chunk_ref_by_ino,
                ino,
                manifest.as_deref(),
                None,
            )?;
            tx.remove(&self.orphans, ino.to_be_bytes());
            ns::clear_orphan_xattrs(&mut tx, &self.orphans, ino)?;
            atime::remove_atime_tx(&mut tx, &self.atime, ino);
        }
        tx.commit()?;
        Ok(())
    }

    fn orphans(&self) -> Result<Vec<Ino>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.iter(&self.orphans) {
            let (k, _) = guard.into_inner()?;
            if k.len() == 8 {
                out.push(u64::from_be_bytes(k.as_ref().try_into().unwrap()));
            }
        }
        Ok(out)
    }

    fn take_journal(&self, max: usize) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let r = self.db.read_tx();
        journal::take(&r, &self.journal_ks, &self.local, max)
    }

    fn ack_journal(&self, upto_seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let applied = crate::store::applied_seq_at(&tx, &self.local)?;
        crate::store::spec::retire_local_tx(&mut tx, self, upto_seq, applied, None)?;
        journal::ack_upto(&mut tx, &self.journal_ks, &self.local, upto_seq)?;
        tx.commit()?;
        Ok(())
    }

    fn journal_len(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        journal::len(&r, &self.journal_ks, &self.local)
    }

    fn apply_atime(&self, bumps: &[(Ino, i64, i64)]) -> Result<(u64, u64), MetaError> {
        let mut tx = self.db.write_tx();
        let skew = atime::atime_skew_tolerance_ns();
        let mut applied = 0u64;
        let mut clamped = 0u64;
        for (ino, atime_ns, time_ns) in bumps {
            let (did_apply, was_clamped) = atime::apply_atime_one(
                &mut tx,
                &self.ns,
                &self.orphans,
                &self.atime,
                *ino,
                *atime_ns,
                *time_ns,
                skew,
            )?;
            if did_apply {
                applied += 1;
            }
            if was_clamped {
                clamped += 1;
            }
        }
        tx.commit()?;
        Ok((applied, clamped))
    }

    fn queue_atime(&self, bumps: &[(Ino, i64, i64)]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        for (ino, atime_ns, time_ns) in bumps {
            atime::queue_atime_tx(&mut tx, &self.atime_journal, *ino, *atime_ns, *time_ns)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn atime_backlog_of(&self, _part: &str) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        atime::atime_backlog(&r, &self.atime_journal)
    }

    fn atime_oldest_pending_ns(&self, _part: &str) -> Result<Option<i64>, MetaError> {
        let r = self.db.read_tx();
        atime::oldest_pending_time_ns(&r, &self.atime_journal)
    }

    fn take_atime_of(&self, _part: &str, max: usize) -> Result<Vec<(Ino, i64, i64)>, MetaError> {
        let r = self.db.read_tx();
        atime::take_atime(&r, &self.atime_journal, max)
    }

    fn clear_atime(&self, _part: &str, inos: &[Ino]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        atime::clear_atime_tx(&mut tx, &self.atime_journal, inos);
        tx.commit()?;
        Ok(())
    }

    fn drop_atime_of(&self, _part: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        atime::drop_atime_all(&mut tx, &self.atime_journal)?;
        tx.commit()?;
        Ok(())
    }
}

/// Tests: what runs on the acknowledging thread between a ship
/// acknowledgement's commit and its clear of the unshipped set — another
/// writer that got the store's write lock first.
#[cfg(test)]
pub(crate) mod after_ack_commit {
    use super::Meta;
    use std::cell::RefCell;

    type Hook = Box<dyn FnOnce(&Meta)>;

    thread_local! {
        pub(crate) static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn set(hook: impl FnOnce(&Meta) + 'static) {
        HOOK.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    }

    pub(super) fn fire(meta: &Meta) {
        if let Some(hook) = HOOK.with(|h| h.borrow_mut().take()) {
            hook(meta);
        }
    }
}

#[cfg(test)]
mod unshipped_tests {
    use super::after_ack_commit;
    use crate::mutate::{execute, MutateOp};
    use crate::rid::Rid;
    use crate::store::Meta;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    fn ino(n: u64) -> u64 {
        (3 << 40) | n
    }

    fn create(meta: &Meta, n: u64) {
        execute(
            meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: format!("f{n}"),
                ino: ino(n),
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

    fn journal_seqs(meta: &Meta) -> Vec<u64> {
        meta.take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect()
    }

    /// Plan 30 §M5/§M8: a row journaled after a ship's acknowledgement
    /// committed, and noted before that acknowledgement cleared the
    /// unshipped set, is still unshipped: the set must keep it, or a
    /// ReadIndex position leaves it out and a strict reader on another
    /// node reads the shipped state (`readdirplus-strict` under load: `b
    /// read "" after "round-7"`, the truncation shipped and the content
    /// not). The ack used to clear on "the journal was empty when I
    /// looked", whoever noted a row since.
    #[test]
    fn a_row_noted_between_a_ship_ack_and_its_clear_stays_unshipped() {
        let meta = Meta::open_in_memory().unwrap();
        create(&meta, 1);
        let shipped = journal_seqs(&meta);
        assert!(!shipped.is_empty());
        assert!(meta.unshipped_touches_read(ino(1), false, None));
        after_ack_commit::set(|meta| create(meta, 2));
        meta.ack_journal_rows_at(&shipped, 1).unwrap();
        assert!(
            !journal_seqs(&meta).is_empty(),
            "the second row is in the journal, unshipped"
        );
        assert!(
            meta.unshipped_touches_read(ino(2), false, None),
            "a ReadIndex of the second file would leave its unshipped row out"
        );
        let mut keys = crate::replay::TouchSet::default();
        keys.inos.insert(ino(2));
        assert!(
            meta.unshipped_overlaps(&keys),
            "a forward reply's base would leave it out"
        );
        // The next acknowledgement with no writer beside it clears.
        let rest = journal_seqs(&meta);
        meta.ack_journal_rows_at(&rest, 2).unwrap();
        assert!(!meta.unshipped_touches_read(ino(2), false, None));
        assert!(!meta.unshipped_touches_read(ino(1), false, None));
    }

    /// A ship's acknowledgement asks whether the journal is empty (plan 30
    /// §M5's clear of the unshipped set) under the store's writer lock: one
    /// seek, not a count of every row still unshipped behind the ones it
    /// ships (on a root with a backlog, a walk of thousands of rows on every
    /// ship, with every writer queued behind it).
    #[test]
    fn a_ship_ack_does_not_walk_the_rest_of_the_journal() {
        use crate::store::journal::ROWS_COUNTED;
        let meta = Meta::open_in_memory().unwrap();
        for n in 1..=2000 {
            create(&meta, n);
        }
        let all = journal_seqs(&meta);
        // The first transaction's `Create` and its `Completed`.
        let first: Vec<u64> = all.iter().copied().take(2).collect();
        ROWS_COUNTED.with(|c| c.set(0));
        meta.ack_journal_rows_at(&first, 1).unwrap();
        let walked = ROWS_COUNTED.with(|c| c.get());
        assert!(
            walked <= 2,
            "the ack stepped over {walked} journal rows to learn the journal is not empty"
        );
        // Still right at the end: the last rows' ack empties it.
        let rest: Vec<u64> = all.iter().copied().skip(2).collect();
        meta.ack_journal_rows_at(&rest, 2).unwrap();
        assert!(journal_seqs(&meta).is_empty());
        assert!(!meta.unshipped_touches_read(ino(2000), false, None));
    }
}
