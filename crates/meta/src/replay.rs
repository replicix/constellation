//! Convergent replay: applying a batch of [`LogRecord`]s — from a
//! shipped segment, a bootstrap replay of the whole log, or a
//! reintegration decision — to the local replica.
//!
//! The whole batch (every record plus the trailing ino-counter
//! recompute) is one fjall write transaction, exactly as it was one
//! SQLite transaction: a crash mid-batch must leave either the fully
//! applied batch or none of it. Idempotency and "skip, don't error" are
//! load-bearing here — the same segment can be tailed twice (at-least-
//! once delivery), and a record whose target was already removed by an
//! earlier-in-the-same-batch conflict must not abort the rest.

use crate::error::MetaError;
use crate::record::{CloneNode, LogRecord};
use crate::store::{atime, misc, ns, reclaim_ino_counter, Meta};
use constellation_fs_core::{Ino, InodeKind};
use constellation_mtree::keys;
use constellation_mtree::record::{self, Attrs, DentryRecord, Kind};
use constellation_types::Rdev;
use fjall::SingleWriterWriteTx;
use std::collections::BTreeSet;

/// Which dentries/inos a batch of records touches, used to suppress a
/// foreign record that collides with the caller's own not-yet-shipped
/// work. Atime is deliberately invisible here (see [`LogRecord::Atime`]):
/// it records neither a dentry nor an ino, so it can neither suppress
/// nor be suppressed.
///
/// `BTreeSet`s: [`crate::delegation::DelegationTable::resolve`] walks these
/// in order to list a cross-subtree op's `involved` delegations, and the
/// root recalls them in that order; with `HashSet`s the order (and so
/// the whole schedule after it) changed from process to process, and
/// the authority sim's `locks-delegated` seed 196102 replayed its
/// failure only about one run in two.
#[derive(Default, Debug)]
pub struct TouchSet {
    pub dentries: BTreeSet<(u64, String)>,
    /// Inodes held *exclusively*: their own attributes change, or (a
    /// directory) their subtree is removed, moved or re-attributed.
    pub inos: BTreeSet<u64>,
    /// Plan 30 §M12: directories held *shared* — a create, unlink or
    /// link in them changes their times and link count, but by a
    /// commutative merge (`max` times, additive nlink), so two such
    /// holds never conflict; an exclusive hold on the same directory
    /// (rmdir, a rename of it, setattr on it) conflicts with both.
    pub shared: BTreeSet<u64>,
}

impl TouchSet {
    pub fn from_records<'a>(records: impl Iterator<Item = &'a LogRecord>) -> Self {
        let mut set = TouchSet::default();
        for rec in records {
            set.add(rec);
        }
        set
    }

    pub fn add(&mut self, rec: &LogRecord) {
        match rec {
            LogRecord::Mkdir {
                parent, name, ino, ..
            }
            | LogRecord::Create {
                parent, name, ino, ..
            }
            | LogRecord::Symlink {
                parent, name, ino, ..
            }
            | LogRecord::Mknod {
                parent, name, ino, ..
            }
            | LogRecord::Link {
                parent, name, ino, ..
            } => {
                self.dentries.insert((*parent, name.clone()));
                self.inos.insert(*ino);
                self.shared.insert(*parent);
            }
            LogRecord::Unlink { parent, name, .. } | LogRecord::Rmdir { parent, name, .. } => {
                self.dentries.insert((*parent, name.clone()));
                self.shared.insert(*parent);
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                self.dentries.insert((*parent, name.clone()));
                self.dentries.insert((*new_parent, new_name.clone()));
                self.shared.insert(*parent);
                self.shared.insert(*new_parent);
            }
            LogRecord::Setattr { ino, .. }
            | LogRecord::WriteManifest { ino, .. }
            | LogRecord::SetXattr { ino, .. }
            | LogRecord::RemoveXattr { ino, .. } => {
                self.inos.insert(*ino);
            }
            LogRecord::SnapCreate { .. }
            | LogRecord::SnapDelete { .. }
            | LogRecord::SetQuota { .. }
            | LogRecord::Atime { .. }
            // Touches no dentry/ino: it can neither suppress nor be
            // suppressed, exactly like `Atime` (see this type's doc).
            | LogRecord::Completed { .. }
            | LogRecord::Refused { .. }
            | LogRecord::InboxAck { .. }
            | LogRecord::Delegate { .. }
            | LogRecord::Recall { .. }
            | LogRecord::TailFollows { .. } => {}
            LogRecord::Clone { nodes, .. } => {
                for node in nodes {
                    self.dentries.insert((node.parent, node.name.clone()));
                    self.inos.insert(node.ino);
                    self.shared.insert(node.parent);
                }
            }
        }
    }

    /// Plan 30 §M12: whether two sets conflict — a common dentry, a
    /// common exclusive inode, or an exclusive hold against any hold on
    /// the same inode. Shared holds never conflict with each other.
    pub fn overlaps(&self, other: &TouchSet) -> bool {
        self.dentries.iter().any(|d| other.dentries.contains(d))
            || self
                .inos
                .iter()
                .any(|i| other.inos.contains(i) || other.shared.contains(i))
            || self.shared.iter().any(|i| other.inos.contains(i))
    }

    /// The keys `op` reads or writes, before it runs (plan 30 §M11's
    /// ownership resolution and the core's reply base both use it).
    pub fn from_op(op: &crate::mutate::MutateOp) -> Self {
        use crate::mutate::MutateOp;
        let mut set = TouchSet::default();
        let mut dentry = |p: u64, n: &str| {
            set.dentries.insert((p, n.to_string()));
            set.shared.insert(p);
        };
        match op {
            MutateOp::Mkdir { parent, name, .. }
            | MutateOp::Create { parent, name, .. }
            | MutateOp::Symlink { parent, name, .. }
            | MutateOp::Mknod { parent, name, .. }
            | MutateOp::Unlink { parent, name }
            | MutateOp::Rmdir { parent, name } => dentry(*parent, name),
            MutateOp::Link { ino, parent, name } => {
                dentry(*parent, name);
                set.inos.insert(*ino);
            }
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
            } => {
                dentry(*parent, name);
                dentry(*new_parent, new_name);
            }
            MutateOp::Setattr { ino, .. }
            | MutateOp::SetManifest { ino, .. }
            | MutateOp::SetXattr { ino, .. }
            | MutateOp::RemoveXattr { ino, .. } => {
                set.inos.insert(*ino);
            }
            MutateOp::Publish {
                ino, parent, name, ..
            } => {
                dentry(*parent, name);
                set.inos.insert(*ino);
            }
            MutateOp::AtimeBatch { entries } => {
                for (i, _, _) in entries {
                    set.inos.insert(*i);
                }
            }
            MutateOp::Records { records } => {
                set = TouchSet::from_records(records.iter());
            }
        }
        set
    }

    /// [`Self::from_op`] plus the inodes the op takes away or moves,
    /// looked up through `lookup` (plan 30 §M12: an rmdir or a rename
    /// holds the directory it removes or moves *exclusively* — against
    /// the creates inside it, and against a delegation of it). The
    /// core's `keys_of_op_in` and the FUSE fast path compute the same.
    pub fn from_op_in(
        op: &crate::mutate::MutateOp,
        lookup: &dyn Fn(u64, &str) -> Option<u64>,
    ) -> Self {
        use crate::mutate::MutateOp;
        let mut set = TouchSet::from_op(op);
        match op {
            MutateOp::Rmdir { parent, name } => {
                if let Some(i) = lookup(*parent, name) {
                    set.inos.insert(i);
                }
            }
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
            } => {
                for (p, n) in [(parent, name), (new_parent, new_name)] {
                    if let Some(i) = lookup(*p, n) {
                        set.inos.insert(i);
                    }
                }
            }
            _ => {}
        }
        set
    }

    pub fn conflicts(&self, rec: &LogRecord) -> bool {
        let touched = TouchSet::from_records(std::iter::once(rec));
        self.overlaps(&touched)
    }
}

pub(crate) enum Applied {
    Done,
    Skipped(&'static str),
}

/// How one record is applied: which `Dirty` context its `ns` writes go
/// through (plain dirty-tracking, or dirty-tracking plus a speculation
/// capture — plan 30 §M3a), and whether it is durable log content.
///
/// `durable` only decides one thing: whether a `Completed { rid }` record
/// is written to the `completed` keyspace. That keyspace is the M2
/// coverage oracle ("did this rid take effect in the log?"), so a
/// requester's shadow or `Exists` hint — records applied *ahead of* the
/// log — must never populate it; only a tailed segment (or a redo of one)
/// may.
#[derive(Clone, Copy)]
pub(crate) struct ApplyCx<'a> {
    pub dirty: ns::Dirty<'a>,
    pub durable: bool,
}

/// Apply one record in the caller's transaction (the shared body of
/// [`Meta::apply_foreign`], `Meta::apply_segment` and the speculation
/// log's install/redo paths in `store::spec`). Returns whether it was
/// skipped, and why, for the caller to count or log.
pub(crate) fn apply_record(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    cx: ApplyCx<'_>,
    rec: &LogRecord,
    staged: &crate::store::UsageTracker,
) -> Result<Option<&'static str>, MetaError> {
    let applied = apply_one(tx, meta, cx.dirty, cx.durable, rec, staged)?;
    if let Some(ino) = primary_ino(rec) {
        reclaim_ino_counter(tx, &meta.local, ino)?;
    }
    Ok(match applied {
        Applied::Done => None,
        Applied::Skipped(why) => Some(why),
    })
}

impl Meta {
    pub fn apply_records(&self, records: &[LogRecord]) -> Result<(), MetaError> {
        self.apply_foreign(records, &TouchSet::default())?;
        Ok(())
    }

    /// Apply `records`, skipping any that collide with `pending` (the
    /// caller's own unshipped local journal — see the module doc).
    /// Returns how many were skipped (conflict or a downstream cascade).
    ///
    /// Durable content, applied without speculation capture. The tailer
    /// uses `Meta::apply_segment` instead, which also runs the plan 30
    /// §M3a stranding/retirement rules around the same application.
    pub fn apply_foreign(
        &self,
        records: &[LogRecord],
        pending: &TouchSet,
    ) -> Result<usize, MetaError> {
        let mut tx = self.db.write_tx();
        let staged = crate::store::UsageTracker::staging();
        let cx = ApplyCx {
            dirty: self.dirty_for_ns(),
            durable: true,
        };
        let (skipped, _) = apply_batch_tx(&mut tx, self, cx, records, pending, &staged, false)?;
        let (staged_bytes, staged_files) = staged.raw_delta();
        crate::store::adjust_usage_tx(&mut tx, &self.local, staged_bytes, staged_files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        Ok(skipped)
    }
}

/// Apply `records` in order inside `tx`, skipping (and counting) any that
/// collide with `pending` or that `apply_one` itself skips. Returns the
/// skip count and — when `keep` is set — the records that were actually
/// handed to `apply_one` (everything not suppressed by `pending`): what a
/// speculation-log entry must redo later. The plain tailing path passes
/// `keep: false` and pays for no copies.
pub(crate) fn apply_batch_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    cx: ApplyCx<'_>,
    records: &[LogRecord],
    pending: &TouchSet,
    staged: &crate::store::UsageTracker,
    keep: bool,
) -> Result<(usize, Vec<LogRecord>), MetaError> {
    let mut skipped = 0usize;
    let mut applied = Vec::with_capacity(if keep { records.len() } else { 0 });
    for rec in records {
        if pending.conflicts(rec) {
            tracing::warn!(
                ?rec,
                "replay: skipping record that conflicts with pending local work"
            );
            skipped += 1;
            continue;
        }
        if let Some(why) = apply_record(tx, meta, cx, rec, staged)? {
            tracing::warn!(why, ?rec, "replay: skipped");
            skipped += 1;
        }
        if keep {
            applied.push(rec.clone());
        }
    }
    Ok((skipped, applied))
}

fn primary_ino(rec: &LogRecord) -> Option<Ino> {
    match rec {
        LogRecord::Mkdir { ino, .. }
        | LogRecord::Create { ino, .. }
        | LogRecord::Symlink { ino, .. }
        | LogRecord::Mknod { ino, .. } => Some(*ino),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
fn apply_one(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    durable: bool,
    rec: &LogRecord,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    // Plan 30 §M12: a stamp applied here is one this node's clock stays
    // above from now on (the HLC's receive rule).
    if let Some(t) = rec.stamp_ns() {
        crate::hlc::observe(t);
    }
    // EC2 campaign 4 B-2: an inode is created once — inos are never
    // reused — so a durable create whose ino already exists here is this
    // very record applied again, over state that already holds it and
    // possibly later ops: a root appending a delegate's transaction over
    // its own shadow of the op, a record redone or re-shipped. Re-running
    // it re-created the inode with one link and put its name back: under
    // git's loose object (`create tmp`, `link tmp obj`, `unlink tmp`) the
    // `link` then found `obj` in place and added no link, and the `unlink
    // tmp` dropped the inode under `obj`. The first application stands.
    if durable {
        if let Some(ino) = primary_ino(rec) {
            if ns::get_inode_record(tx, &meta.ns, ino)?.is_some() {
                return Ok(Applied::Done);
            }
        }
    }
    match rec {
        LogRecord::Mkdir {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            meta,
            dirty,
            *parent,
            name,
            *ino,
            Kind::Dir,
            *mode,
            *uid,
            *gid,
            Rdev::default(),
            2,
            0,
            None,
            *time_ns,
            staged,
        ),
        LogRecord::Create {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            meta,
            dirty,
            *parent,
            name,
            *ino,
            Kind::File,
            *mode,
            *uid,
            *gid,
            Rdev::default(),
            1,
            0,
            None,
            *time_ns,
            staged,
        ),
        LogRecord::Symlink {
            parent,
            name,
            ino,
            target,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            meta,
            dirty,
            *parent,
            name,
            *ino,
            Kind::Symlink,
            0o777,
            *uid,
            *gid,
            Rdev::default(),
            1,
            target.len() as u64,
            Some(target.clone()),
            *time_ns,
            staged,
        ),
        LogRecord::Mknod {
            parent,
            name,
            ino,
            kind,
            mode,
            uid,
            gid,
            rdev,
            time_ns,
        } => {
            let Some(kind) = InodeKind::from_u8(*kind) else {
                return Err(MetaError::Invalid(format!("unknown mknod kind {kind}")));
            };
            insert_node(
                tx,
                meta,
                dirty,
                *parent,
                name,
                *ino,
                ns::kind_to_mtree(kind),
                *mode,
                *uid,
                *gid,
                *rdev,
                1,
                0,
                None,
                *time_ns,
                staged,
            )
        }
        LogRecord::Link {
            ino,
            parent,
            name,
            time_ns,
        } => apply_link(tx, meta, dirty, *ino, *parent, name, *time_ns),
        LogRecord::Unlink {
            parent,
            name,
            time_ns,
        } => apply_unlink(tx, meta, dirty, *parent, name, *time_ns),
        LogRecord::Rmdir {
            parent,
            name,
            time_ns,
        } => apply_rmdir(tx, meta, dirty, *parent, name, *time_ns),
        LogRecord::Rename {
            parent,
            name,
            new_parent,
            new_name,
            time_ns,
        } => apply_rename(
            tx,
            meta,
            dirty,
            *parent,
            name,
            *new_parent,
            new_name,
            *time_ns,
        ),
        LogRecord::Setattr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime_ns,
            mtime_ns,
            time_ns,
        } => apply_setattr(
            tx, meta, dirty, *ino, *mode, *uid, *gid, *size, *atime_ns, *mtime_ns, *time_ns, staged,
        ),
        LogRecord::WriteManifest {
            ino,
            manifest,
            size,
            time_ns,
            ..
        } => apply_write_manifest(tx, meta, dirty, *ino, manifest, *size, *time_ns, staged),
        LogRecord::SetXattr {
            ino,
            name,
            value,
            time_ns,
        } => apply_set_xattr(tx, meta, dirty, *ino, name, value, *time_ns),
        LogRecord::RemoveXattr { ino, name, time_ns } => {
            apply_remove_xattr(tx, meta, dirty, *ino, name, *time_ns)
        }
        LogRecord::SnapCreate {
            id,
            path,
            name,
            root_hash,
            created_unix_ms,
        } => {
            ns::ns_insert(
                tx,
                &meta.ns,
                dirty,
                keys::subsystem(keys::Subsystem::Snapshot, id.as_bytes()),
                crate::store::snapshot_record(&crate::SnapshotRow {
                    id: id.clone(),
                    path: path.clone(),
                    name: name.clone(),
                    root_hash: root_hash.clone(),
                    created_unix_ms: *created_unix_ms,
                }),
            )?;
            Ok(Applied::Done)
        }
        LogRecord::SnapDelete { id, .. } => {
            ns::ns_remove(
                tx,
                &meta.ns,
                dirty,
                keys::subsystem(keys::Subsystem::Snapshot, id.as_bytes()),
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Clone { nodes, .. } => apply_clone(tx, meta, dirty, nodes, staged),
        LogRecord::SetQuota { max_logical_bytes } => {
            ns::ns_insert(
                tx,
                &meta.ns,
                dirty,
                keys::subsystem(keys::Subsystem::Quota, b""),
                crate::store::quota_record(*max_logical_bytes),
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Atime {
            ino,
            atime_ns,
            time_ns,
        } => {
            let skew = atime::atime_skew_tolerance_ns();
            atime::apply_atime_one(
                tx,
                &meta.ns,
                &meta.orphans,
                &meta.atime,
                *ino,
                *atime_ns,
                *time_ns,
                skew,
            )?;
            Ok(Applied::Done)
        }
        // Plan 30 §M2: node-local and unpublished, exactly like `Atime`
        // above — writes directly to `completed`, never through
        // `ns`/`dirty`. Idempotent: replaying the same segment twice (or
        // a rid whose row this replica already has, e.g. because it was
        // the holder that recorded it in `recent` before shipping) just
        // overwrites the same key with the same value.
        // Plan 30 §M3a: only durable log content may say a rid took
        // effect. A speculative install (shadow, hint) carries its op's
        // `Completed` too, but recording it would make the M2 coverage
        // check believe an op the log may never contain had completed.
        LogRecord::Completed { .. } if !durable => Ok(Applied::Done),
        LogRecord::Completed { rid } => {
            // A row applied from a segment records position 0; the
            // writer's own `journal::append_tx` records its journal seq.
            // The retry-resolution path needs only presence, but the
            // distinction is load-bearing: 0 means "the log carries this
            // outcome", which is what `store::spec`'s `strand_local_tx`
            // checks before it drops a stranded transaction's `completed`
            // row (a nonzero position is this node's own unshipped
            // journal, which the strand takes back out).
            let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
            tx.insert(
                &meta.completed,
                rid.to_key(),
                crate::store::Meta::encode_completed_row(0, now_ms),
            );
            Ok(Applied::Done)
        }
        // Plan 30 §M13: a refusal is an outcome too, with the same
        // durable-only rule as `Completed` (an inbox op is never installed
        // speculatively, so `!durable` cannot happen for it in practice).
        LogRecord::Refused { .. } if !durable => Ok(Applied::Done),
        LogRecord::Refused { rid, code } => {
            let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
            tx.insert(
                &meta.completed,
                rid.to_key(),
                crate::store::Meta::encode_refused_row(0, now_ms, *code),
            );
            Ok(Applied::Done)
        }
        LogRecord::InboxAck { .. } if !durable => Ok(Applied::Done),
        LogRecord::InboxAck { epoch, node, n, i } => {
            crate::store::inbox::set_inbox_ack_tx(
                tx,
                &meta.local,
                crate::store::inbox::InboxAck {
                    epoch: *epoch,
                    node: *node,
                    n: *n,
                    i: *i,
                },
            )?;
            Ok(Applied::Done)
        }
        // Plan 30 §M11: the delegation table is one `0x30` row written
        // through the ordinary `ns` funnel — dirty-tracked (so the
        // publisher carries it) and captured (so a holder's unshipped
        // `Delegate`/`Recall` is substituted out of its commits like any
        // other unshipped effect).
        LogRecord::Delegate {
            dir,
            node,
            gen,
            designated,
            range,
        } => {
            let mut table = crate::delegation::read_table_tx(tx, meta)?;
            table.apply(crate::delegation::DelegationRecord::Delegate {
                dir: *dir,
                node: *node,
                gen: *gen,
                designated: *designated,
                range: crate::delegation::Range {
                    bits: range.0,
                    idx: range.1,
                },
            });
            crate::delegation::write_table_tx(tx, meta, dirty, &table)?;
            Ok(Applied::Done)
        }
        LogRecord::Recall { dir, gen } => {
            let mut table = crate::delegation::read_table_tx(tx, meta)?;
            table.apply(crate::delegation::DelegationRecord::Recall {
                dir: *dir,
                gen: *gen,
            });
            crate::delegation::write_table_tx(tx, meta, dirty, &table)?;
            // M12 round 2 (623da2c's holder-cut shape with delegations):
            // the generation is void from here for *every* replica — the
            // rule the record states (its rows not before it in the log
            // never take effect; a stranded one is replayed by rid), not
            // only for the root that wrote it. Before, only that root
            // voided it (`void_stream` at the recall): its successor,
            // inheriting the table from the log after the root died with
            // the generation's last rows unshipped, refused every batch
            // whose deps named them (`reaches_streams`: neither applied
            // nor void) and the delegate re-sent it forever (813 batches
            // in 9 s, sim seed 1604); and the delegate itself kept naming
            // the dead generation in its `deps`. Only from the durable log
            // — a streamed-ahead install of the record may strand, and
            // the generation would then be live again under the successor.
            if durable {
                let cut = meta.session().stream_applied(*gen);
                meta.session().void_stream(*gen, cut);
            }
            Ok(Applied::Done)
        }
        // The replica's session state reads it (`SessionState::owe`).
        LogRecord::TailFollows { .. } => Ok(Applied::Done),
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_node(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    parent: Ino,
    name: &str,
    ino: Ino,
    kind: Kind,
    mode: u32,
    uid: u32,
    gid: u32,
    rdev: Rdev,
    nlink: u32,
    size: u64,
    target: Option<String>,
    t: i64,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    if ns::get_inode_record(tx, &meta.ns, parent)?.is_none() {
        return Ok(Applied::Skipped("parent does not exist"));
    }
    if let Some(existing) = ns::get_dentry_record(tx, &meta.ns, parent, name)? {
        if existing.ino == ino {
            return Ok(Applied::Done);
        }
        match evict_dentry(tx, meta, dirty, parent, name, existing.ino, t)? {
            Applied::Done => {}
            skip => return Ok(skip),
        }
    }
    let prior_file =
        ns::get_inode_record(tx, &meta.ns, ino)?.is_some_and(|r| r.attrs.kind == Kind::File);
    let attrs = Attrs {
        kind,
        mode: mode & 0o7777,
        uid,
        gid,
        nlink,
        size,
        mtime_ns: t,
        ctime_ns: t,
        rdev,
    };
    ns::put_inode(
        tx,
        &meta.ns,
        dirty,
        &meta.blobs,
        ino,
        attrs,
        None,
        target.map(String::into_bytes),
        &[],
    )?;
    let is_new_dentry = ns::get_dentry_record(tx, &meta.ns, parent, name)?.is_none();
    ns::put_dentry(tx, &meta.ns, dirty, parent, name, ino, attrs)?;
    atime::set_atime_tx(tx, &meta.atime, ino, t);
    if is_new_dentry && kind == Kind::Dir {
        misc::bump_nlink_tx(tx, &meta.ns, dirty, parent, 1, t)?;
    }
    misc::touch_times_tx(tx, &meta.ns, dirty, parent, t)?;
    if kind == Kind::File && !prior_file {
        // A fresh file always starts at 0 bytes (Create/Mkdir/Symlink/
        // Mknod never carry file content), but the file *count* still
        // moves, mirroring the local `create()` path's `adjust(0, 1)`.
        staged.adjust(0, 1);
    }
    Ok(Applied::Done)
}

/// Evict whatever currently sits at `(parent, name)` so a later insert
/// can claim the name. Mirrors the old engine's rules: a non-empty
/// directory is left alone (skip, don't cascade-delete a subtree), an
/// empty directory or a dropped-to-zero non-directory is removed
/// outright (an orphan reaped later, exactly like a local `unlink`).
fn evict_dentry(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    parent: Ino,
    name: &str,
    ino: Ino,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        ns::remove_dentry(tx, &meta.ns, dirty, parent, name, ino)?;
        return Ok(Applied::Done);
    };
    if rec.attrs.kind == Kind::Dir {
        if ns::has_children(tx, &meta.ns, ino)? {
            return Ok(Applied::Skipped("name held by non-empty directory"));
        }
        ns::remove_dentry(tx, &meta.ns, dirty, parent, name, ino)?;
        ns::ns_remove(tx, &meta.ns, dirty, keys::inode(ino))?;
        // The parent lost a subdirectory (`..`): the same delta the
        // writer applied (`writes::rename_in_tx`); plan 30 §M12 makes
        // every parent delta additive, so replay must carry it too.
        misc::bump_nlink_tx(tx, &meta.ns, dirty, parent, -1, t)?;
        ns::clear_spilled_xattrs(tx, &meta.ns, dirty, ino)?;
        let names: Vec<Vec<u8>> = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?
            .into_iter()
            .map(|(n, _)| n.into_bytes())
            .collect();
        misc::xattr_by_name_del_all_tx(tx, &meta.xattr_by_name, ino, names);
        atime::remove_atime_tx(tx, &meta.atime, ino);
    } else {
        ns::remove_dentry(tx, &meta.ns, dirty, parent, name, ino)?;
        if rec.attrs.nlink <= 1 {
            let manifest_bytes = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
                .transpose()?;
            ns::clear_spilled_xattrs(tx, &meta.ns, dirty, ino)?;
            let names: Vec<Vec<u8>> = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?
                .into_iter()
                .map(|(n, _)| n.into_bytes())
                .collect();
            misc::xattr_by_name_del_all_tx(tx, &meta.xattr_by_name, ino, names);
            misc::track_manifest_transition_tx(
                tx,
                &meta.chunk_ref,
                &meta.chunk_ref_by_ino,
                ino,
                manifest_bytes.as_deref(),
                None,
            )?;
            let mut orphan_rec = rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            orphan_rec.xattrs.clear();
            tx.insert(
                &meta.orphans,
                ino.to_be_bytes().to_vec(),
                orphan_rec.encode(),
            );
            ns::ns_remove(tx, &meta.ns, dirty, keys::inode(ino))?;
        } else {
            misc::bump_nlink_tx(tx, &meta.ns, dirty, ino, -1, t)?;
        }
    }
    Ok(Applied::Done)
}

fn apply_link(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    ino: Ino,
    parent: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    if ns::get_inode_record(tx, &meta.ns, ino)?.is_none()
        || ns::get_inode_record(tx, &meta.ns, parent)?.is_none()
    {
        return Ok(Applied::Skipped("link endpoint missing"));
    }
    if let Some(existing) = ns::get_dentry_record(tx, &meta.ns, parent, name)? {
        if existing.ino == ino {
            return Ok(Applied::Done);
        }
        match evict_dentry(tx, meta, dirty, parent, name, existing.ino, t)? {
            Applied::Done => {}
            skip => return Ok(skip),
        }
    }
    let rec = misc::bump_nlink_tx(tx, &meta.ns, dirty, ino, 1, t)?.expect("checked above");
    ns::put_dentry(tx, &meta.ns, dirty, parent, name, ino, rec.attrs)?;
    misc::touch_times_tx(tx, &meta.ns, dirty, parent, t)?;
    Ok(Applied::Done)
}

fn apply_unlink(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    parent: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(d) = ns::get_dentry_record(tx, &meta.ns, parent, name)? else {
        return Ok(Applied::Done);
    };
    let result = evict_dentry(tx, meta, dirty, parent, name, d.ino, t)?;
    misc::touch_times_tx(tx, &meta.ns, dirty, parent, t)?;
    Ok(result)
}

fn apply_rmdir(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    parent: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(d) = ns::get_dentry_record(tx, &meta.ns, parent, name)? else {
        return Ok(Applied::Done);
    };
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, d.ino)? else {
        ns::remove_dentry(tx, &meta.ns, dirty, parent, name, d.ino)?;
        return Ok(Applied::Done);
    };
    if rec.attrs.kind != Kind::Dir {
        return Ok(Applied::Skipped("rmdir target is not a directory"));
    }
    if ns::has_children(tx, &meta.ns, d.ino)? {
        return Ok(Applied::Skipped("directory not empty locally"));
    }
    ns::remove_dentry(tx, &meta.ns, dirty, parent, name, d.ino)?;
    ns::ns_remove(tx, &meta.ns, dirty, keys::inode(d.ino))?;
    ns::clear_spilled_xattrs(tx, &meta.ns, dirty, d.ino)?;
    atime::remove_atime_tx(tx, &meta.atime, d.ino);
    misc::bump_nlink_tx(tx, &meta.ns, dirty, parent, -1, t)?;
    Ok(Applied::Done)
}

#[allow(clippy::too_many_arguments)]
fn apply_rename(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    parent: Ino,
    name: &str,
    new_parent: Ino,
    new_name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(src) = ns::get_dentry_record(tx, &meta.ns, parent, name)? else {
        return Ok(Applied::Done);
    };
    let ino = src.ino;
    if ns::get_inode_record(tx, &meta.ns, new_parent)?.is_none() {
        return Ok(Applied::Skipped("rename destination parent missing"));
    }
    let Some(src_rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    if let Some(existing) = ns::get_dentry_record(tx, &meta.ns, new_parent, new_name)? {
        if existing.ino == ino {
            return Ok(Applied::Done);
        }
        match evict_dentry(tx, meta, dirty, new_parent, new_name, existing.ino, t)? {
            Applied::Done => {}
            skip => return Ok(skip),
        }
    }
    ns::ns_remove(tx, &meta.ns, dirty, keys::dentry(parent, name.as_bytes()))?;
    ns::ns_remove(
        tx,
        &meta.ns,
        dirty,
        keys::rdentry(ino, parent, name.as_bytes()),
    )?;
    ns::ns_insert(
        tx,
        &meta.ns,
        dirty,
        keys::dentry(new_parent, new_name.as_bytes()),
        DentryRecord::new(ino, src.attrs).encode(),
    )?;
    ns::ns_insert(
        tx,
        &meta.ns,
        dirty,
        keys::rdentry(ino, new_parent, new_name.as_bytes()),
        record::RDENTRY_VALUE.to_vec(),
    )?;
    if src_rec.attrs.kind == Kind::Dir && parent != new_parent {
        misc::bump_nlink_tx(tx, &meta.ns, dirty, parent, -1, t)?;
        misc::bump_nlink_tx(tx, &meta.ns, dirty, new_parent, 1, t)?;
    }
    misc::touch_times_tx(tx, &meta.ns, dirty, parent, t)?;
    misc::touch_times_tx(tx, &meta.ns, dirty, new_parent, t)?;
    Ok(Applied::Done)
}

/// The signed byte-usage delta `new - old` between two sizes, computed
/// without the overflow an `i64` subtraction can hit. `size` fields come off
/// decoded log records, so a corrupt or hostile record can carry a value near
/// `u64::MAX`; casting each to `i64` and subtracting would panic under
/// `overflow-checks` and wrap silently in release. Widening to `i128` and
/// clamping to the `i64` range is exact for every legitimate size (all far
/// below `i64::MAX`) and merely bounds a nonsensical one instead of crashing
/// the replay.
fn size_delta(new: u64, old: u64) -> i64 {
    (new as i128 - old as i128).clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

#[allow(clippy::too_many_arguments)]
fn apply_setattr(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    ino: Ino,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime_ns: Option<i64>,
    mtime_ns: Option<i64>,
    t: i64,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    let Some(mut rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let old_size = rec.attrs.size;
    if let Some(m) = mode {
        rec.attrs.mode = m & 0o7777;
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
    ns::ns_insert(tx, &meta.ns, dirty, keys::inode(ino), rec.encode())?;
    for (parent, name) in ns::links_of(tx, &meta.ns, ino)? {
        ns::ns_insert(
            tx,
            &meta.ns,
            dirty,
            keys::dentry(parent, name.as_bytes()),
            DentryRecord::new(ino, attrs).encode(),
        )?;
    }
    if let Some(a) = atime_ns {
        atime::set_atime_tx(tx, &meta.atime, ino, a);
    }
    if size.is_some() && attrs.kind == Kind::File {
        staged.adjust(size_delta(attrs.size, old_size), 0);
        clip_manifest_to_size_tx(tx, meta, dirty, ino)?;
    }
    Ok(Applied::Done)
}

/// A truncate's effect on the file's content: its manifest keeps only
/// what lies below the new size — `file_len` lowered to it, chunks wholly
/// past it dropped (a spilled list keeps its blob; the chunks past
/// `file_len` in it are dead). `None`: nothing to cut.
///
/// A size change used to leave the manifest as it was. Readers stop at
/// the size, so the bytes past it looked gone — until the file was
/// extended again (a write past a gap, a truncate-up, a `fallocate`),
/// and they came back from the old manifest. The invariant now: **a
/// manifest's content is valid only below its `file_len`**, and a
/// truncate lowers it; every reader of manifest content clips there.
pub fn clip_manifest(manifest: &[u8], size: u64) -> Option<Vec<u8>> {
    let mut m = constellation_fs_core::Manifest::decode(manifest).ok()?;
    if size >= m.file_len {
        return None;
    }
    let cs = u64::from(m.layout.chunk_size);
    if let constellation_fs_core::ChunkInfo::Inline(chunks) = &mut m.chunks {
        chunks.retain(|index, _| *index * cs < size);
    }
    m.file_len = size;
    Some(m.encode())
}

/// Apply [`clip_manifest`] to `ino`'s stored manifest at its current
/// size, in the caller's transaction (a `setattr(size)` executed here or
/// replayed from the log: every replica cuts the same way).
pub(crate) fn clip_manifest_to_size_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    ino: Ino,
) -> Result<(), MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(());
    };
    if rec.attrs.kind != Kind::File {
        return Ok(());
    }
    let Some(payload) = rec.manifest.as_ref() else {
        return Ok(());
    };
    let current = ns::resolve_payload(tx, &meta.blobs, payload)?;
    let Some(clipped) = clip_manifest(&current, rec.attrs.size) else {
        return Ok(());
    };
    let symlink = rec
        .symlink_target
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    ns::put_inode(
        tx,
        &meta.ns,
        dirty,
        &meta.blobs,
        ino,
        rec.attrs,
        Some(clipped.clone()),
        symlink,
        &rec.xattrs,
    )?;
    misc::track_manifest_transition_tx(
        tx,
        &meta.chunk_ref,
        &meta.chunk_ref_by_ino,
        ino,
        Some(&current),
        Some(&clipped),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_write_manifest(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    ino: Ino,
    manifest: &[u8],
    size: u64,
    t: i64,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let old_size = rec.attrs.size;
    let current = rec
        .manifest
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    let mut attrs = rec.attrs;
    attrs.size = size;
    attrs.mtime_ns = t;
    attrs.ctime_ns = t;
    let xattrs = rec.xattrs.clone();
    ns::put_inode(
        tx,
        &meta.ns,
        dirty,
        &meta.blobs,
        ino,
        attrs,
        Some(manifest.to_vec()),
        rec.symlink_target
            .as_ref()
            .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
            .transpose()?,
        &xattrs,
    )?;
    for (parent, name) in ns::links_of(tx, &meta.ns, ino)? {
        ns::ns_insert(
            tx,
            &meta.ns,
            dirty,
            keys::dentry(parent, name.as_bytes()),
            DentryRecord::new(ino, attrs).encode(),
        )?;
    }
    misc::track_manifest_transition_tx(
        tx,
        &meta.chunk_ref,
        &meta.chunk_ref_by_ino,
        ino,
        current.as_deref(),
        Some(manifest),
    )?;
    staged.adjust(size_delta(size, old_size), 0);
    Ok(Applied::Done)
}

fn apply_set_xattr(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    ino: Ino,
    name: &str,
    value: &[u8],
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let mut xattrs = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?;
    if let Some(slot) = xattrs.iter_mut().find(|(n, _)| n == name) {
        slot.1 = value.to_vec();
    } else {
        xattrs.push((name.to_string(), value.to_vec()));
    }
    let mut attrs = rec.attrs;
    attrs.ctime_ns = t;
    let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
        .into_iter()
        .map(|(n, v)| (n.into_bytes(), v))
        .collect();
    let manifest = rec
        .manifest
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    let target = rec
        .symlink_target
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    ns::put_inode(
        tx,
        &meta.ns,
        dirty,
        &meta.blobs,
        ino,
        attrs,
        manifest,
        target,
        &xattr_pairs,
    )?;
    misc::xattr_by_name_put_tx(tx, &meta.xattr_by_name, name, ino, value);
    Ok(Applied::Done)
}

fn apply_remove_xattr(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    ino: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let mut xattrs = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?;
    xattrs.retain(|(n, _)| n != name);
    let mut attrs = rec.attrs;
    attrs.ctime_ns = t;
    let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
        .into_iter()
        .map(|(n, v)| (n.into_bytes(), v))
        .collect();
    let manifest = rec
        .manifest
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    let target = rec
        .symlink_target
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    ns::put_inode(
        tx,
        &meta.ns,
        dirty,
        &meta.blobs,
        ino,
        attrs,
        manifest,
        target,
        &xattr_pairs,
    )?;
    misc::xattr_by_name_del_tx(tx, &meta.xattr_by_name, name, ino);
    Ok(Applied::Done)
}

fn apply_clone(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    nodes: &[CloneNode],
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    for node in nodes {
        let Some(kind) = InodeKind::from_u8(node.kind) else {
            return Err(MetaError::Invalid(format!(
                "unknown clone node kind {}",
                node.kind
            )));
        };
        if ns::get_inode_record(tx, &meta.ns, node.parent)?.is_none() {
            return Ok(Applied::Skipped("clone parent does not exist"));
        }
        let prior = ns::get_inode_record(tx, &meta.ns, node.ino)?;
        let (prior_file, prior_size) = prior.as_ref().map_or((false, 0u64), |r| {
            (r.attrs.kind == Kind::File, r.attrs.size)
        });
        let attrs = Attrs {
            kind: ns::kind_to_mtree(kind),
            mode: node.mode,
            uid: node.uid,
            gid: node.gid,
            nlink: if kind == InodeKind::Dir { 2 } else { 1 },
            size: node.size,
            mtime_ns: node.mtime_ns,
            ctime_ns: node.mtime_ns,
            rdev: node.rdev,
        };
        let xattrs: Vec<(Vec<u8>, Vec<u8>)> = node
            .xattrs
            .iter()
            .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
            .collect();
        ns::put_inode(
            tx,
            &meta.ns,
            dirty,
            &meta.blobs,
            node.ino,
            attrs,
            node.manifest.clone(),
            node.target.clone().map(String::into_bytes),
            &xattrs,
        )?;
        ns::put_dentry(
            tx,
            &meta.ns,
            dirty,
            node.parent,
            &node.name,
            node.ino,
            attrs,
        )?;
        atime::set_atime_tx(tx, &meta.atime, node.ino, node.mtime_ns);
        misc::track_manifest_transition_tx(
            tx,
            &meta.chunk_ref,
            &meta.chunk_ref_by_ino,
            node.ino,
            None,
            node.manifest.as_deref(),
        )?;
        for (n, v) in &node.xattrs {
            misc::xattr_by_name_put_tx(tx, &meta.xattr_by_name, n, node.ino, v);
        }
        if kind == InodeKind::Dir {
            misc::bump_nlink_tx(tx, &meta.ns, dirty, node.parent, 1, node.mtime_ns)?;
        }
        let is_file = kind == InodeKind::File;
        match (prior_file, is_file) {
            // `size_delta` for the same reason as the subtraction sites: a
            // hostile record can carry `size` near `u64::MAX`, where
            // `node.size as i64` goes negative and `-(prior_size as i64)`
            // panics at `i64::MIN` under overflow-checks. Adding a file is
            // `size_delta(node.size, 0)`; removing one is `size_delta(0, prior_size)`.
            (false, true) => staged.adjust(size_delta(node.size, 0), 1),
            (true, false) => staged.adjust(size_delta(0, prior_size), -1),
            (true, true) => staged.adjust(size_delta(node.size, prior_size), 0),
            (false, false) => {}
        }
    }
    Ok(Applied::Done)
}

#[cfg(test)]
mod tests {
    use super::size_delta;

    #[test]
    fn size_delta_is_exact_for_real_sizes_and_bounded_for_absurd_ones() {
        // Ordinary sizes: exact, both directions.
        assert_eq!(size_delta(0, 0), 0);
        assert_eq!(size_delta(1_000, 400), 600);
        assert_eq!(size_delta(400, 1_000), -600);

        // A size near u64::MAX (a corrupt/hostile record) would panic under
        // `overflow-checks` if cast to i64 and subtracted; here it saturates
        // to the i64 bounds instead of aborting the replay.
        assert_eq!(size_delta(u64::MAX, 0), i64::MAX);
        assert_eq!(size_delta(0, u64::MAX), i64::MIN);
        assert_eq!(size_delta(u64::MAX, u64::MAX), 0);
        // The specific pair that would panic as `-(prior as i64)` on i64::MIN.
        assert_eq!(size_delta(0, 1 << 63), i64::MIN);
    }
}
