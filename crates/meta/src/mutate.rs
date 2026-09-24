//! Forwarded mutations: the operation a non-holder sends to the lease
//! holder for authoritative validate-and-journal (see DESIGN.md §4/§5).
//!
//! Wire encoding is postcard; the net crate carries these as opaque
//! bytes so it stays free of the metadata dependency.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::Meta;
use crate::{MetaStore, SetXattrMode};
use constellation_fs_core::{Ino, InodeKind};
use serde::{Deserialize, Serialize};

/// One mutation a requester asks the lease holder to execute.
///
/// Inode-creating variants carry the requester's already-allocated ino
/// (node-prefixed, cluster-unique) so the requester's kernel-visible
/// number is stable across the forward and a scratch `Publish` can
/// promote a local inode without remapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutateOp {
    Mkdir {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Create {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Symlink {
        parent: Ino,
        name: String,
        ino: Ino,
        target: String,
        uid: u32,
        gid: u32,
    },
    Mknod {
        parent: Ino,
        name: String,
        ino: Ino,
        kind: u8,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
    },
    Link {
        ino: Ino,
        parent: Ino,
        name: String,
    },
    Unlink {
        parent: Ino,
        name: String,
    },
    Rmdir {
        parent: Ino,
        name: String,
    },
    Rename {
        parent: Ino,
        name: String,
        new_parent: Ino,
        new_name: String,
    },
    Setattr {
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    },
    SetManifest {
        ino: Ino,
        base_manifest: Option<Vec<u8>>,
        manifest: Vec<u8>,
        size: u64,
    },
    SetXattr {
        ino: Ino,
        name: String,
        value: Vec<u8>,
        /// 0 = Set, 1 = Create, 2 = Replace (matches [`SetXattrMode`]).
        mode: u8,
    },
    RemoveXattr {
        ino: Ino,
        name: String,
    },
    /// Batched read-time atime bumps forwarded to the holder (plan 20).
    /// Each entry is `(ino, atime_ns, time_ns)`; `time_ns` is the
    /// emitter's observation time, carried so the holder can apply the
    /// ctime guard against the emitter's clock rather than its own.
    /// Best effort: a holder applies and queues it for shipping, and
    /// appends nothing to the write journal. Appended last so a peer too
    /// old to decode it falls back to `Busy` (a harmless rotation).
    AtimeBatch {
        entries: Vec<(Ino, i64, i64)>,
    },
    /// Scratch → shared publish: create the inode at `parent/name` with
    /// the given attrs and commit its manifest (and any xattrs staged on
    /// the scratch file) in one transaction.
    Publish {
        ino: Ino,
        parent: Ino,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_ns: i64,
        manifest: Vec<u8>,
        size: u64,
        xattrs: Vec<(String, Vec<u8>)>,
    },
    /// Plan 30 §M3b: re-apply already-decided records through the replay
    /// path and journal them — how a deposed holder's stranded
    /// transaction that had no op of its own (a snapshot row, a quota
    /// change, a clone) is replayed by rid on the current holder
    /// (`store::spec`'s `derive_replay_op`). The records apply with the
    /// same skip-on-conflict rules every tailing replica uses, so the
    /// result is whatever the log order makes of them.
    Records {
        records: Vec<LogRecord>,
    },
}

impl MutateOp {
    pub fn to_postcard(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    pub fn from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Holder's answer, before it is packed into a wire `Payload`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutateOutcome {
    Accepted {
        epoch: u64,
        records: Vec<LogRecord>,
    },
    Errno(i32),
    NotHolder {
        holder: u64,
    },
    Busy,
    /// The holder refused an optimistic whole-file manifest commit
    /// because its base is stale. Carries the manifest that *is*
    /// current, so the requester can rebase in the same round trip
    /// rather than wait for the holder's segment to ship.
    ///
    /// Appended last on purpose: a peer too old to decode this variant
    /// falls back to `Busy` (the documented handling of an undecodable
    /// outcome), which costs a lease rotation but stays correct.
    Conflict {
        manifest: Option<Vec<u8>>,
    },
    /// The holder refused a create-family op with `EEXIST`, and carries
    /// the entry that *is* there so the requester can install it in the
    /// same round trip.
    ///
    /// POSIX requires `mkdir`/`O_EXCL` on an existing name to fail, so
    /// the refusal itself stands (`mkdir` is a lock primitive). But the
    /// caller acts on that answer immediately — `mkdir -p` walks into
    /// the directory it was just told exists — and until the holder's
    /// segment reaches this replica, that lookup would fail with an
    /// `ENOENT` the holder never said. Replay is idempotent for an
    /// identical entry (`replay::insert_node` returns early when the
    /// dentry already names the same ino), so installing it early and
    /// applying the holder's segment later converge.
    ///
    /// Appended last, like `Conflict`: a peer too old to decode it falls
    /// back to `Busy`.
    Exists {
        records: Vec<LogRecord>,
        /// The lowest log sequence anything the holder does from now on
        /// can ship in (its next segment). The entry exists on the
        /// holder *now*, so any delete of it is still in the holder's
        /// future and can only ship at or above this sequence: a
        /// requester whose replay is below it cannot have seen one,
        /// which is what makes installing the record early safe. At or
        /// above it the record may already be stale, and installing it
        /// would resurrect a deleted entry that the log has nothing left
        /// to correct.
        ship_floor: u64,
        /// The answering holder's epoch. Plan 30 §M3a: the installed
        /// entry is speculation (`Meta::install_hint`) and is rolled back
        /// if a segment from a later epoch reaches this replica before
        /// the floor does — the entry may have been this holder's own
        /// unshipped work, stranded with it.
        epoch: u64,
    },
}

impl MutateOutcome {
    pub fn to_postcard(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    pub fn from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Execute `op` against the holder's authoritative replica and return
/// the journal records that were appended (including a trailing
/// `LogRecord::Completed { rid }` when `rid` is given — plan 30 §M2).
/// The caller is responsible for checking lease ownership first.
///
/// `rid` should be `Some` for every FUSE-issued mutation (local fast
/// path, holder-side forward execution, or a lease-path retry) and
/// `None` for op executions that are not part of the exactly-once
/// forwarding protocol at all (system-generated unlinks from retention
/// pruning, reintegration replay of already-decided records, tests that
/// do not exercise M2). The rid reaches `journal::append_tx` through a
/// thread-local set for the duration of this call
/// (`store::journal::PendingCompletion`), so the op's own transaction —
/// and only it — carries the completion marker.
pub fn execute(
    meta: &Meta,
    op: &MutateOp,
    rid: Option<crate::rid::Rid>,
) -> Result<Vec<LogRecord>, MetaError> {
    let _pending = crate::store::journal::PendingCompletion::set(rid);
    // Plan 30 §M3b: the op's own journaled transaction records `(rid, op)`
    // in its `journal_tx` row (`store::local`), so a deposition can replay
    // it by rid.
    let _op = crate::store::journal::PendingLocalOp::set(rid, op);
    let records = execute_inner(meta, op)?;
    meta.note_unshipped(&records);
    Ok(records)
}

fn execute_inner(meta: &Meta, op: &MutateOp) -> Result<Vec<LogRecord>, MetaError> {
    let before = meta.max_journal_seq()?;
    match op {
        MutateOp::Mkdir {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
        } => {
            meta.mkdir_at(*parent, name, *ino, *mode, *uid, *gid)?;
        }
        MutateOp::Create {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
        } => {
            meta.create_at(*parent, name, *ino, *mode, *uid, *gid)?;
        }
        MutateOp::Symlink {
            parent,
            name,
            ino,
            target,
            uid,
            gid,
        } => {
            meta.symlink_at(*parent, name, *ino, target, *uid, *gid)?;
        }
        MutateOp::Mknod {
            parent,
            name,
            ino,
            kind,
            mode,
            uid,
            gid,
            rdev,
        } => {
            let kind = InodeKind::from_u8(*kind)
                .ok_or_else(|| MetaError::Invalid(format!("unknown inode kind {kind}")))?;
            meta.mknod_at(*parent, name, *ino, kind, *mode, *uid, *gid, *rdev)?;
        }
        MutateOp::Link { ino, parent, name } => {
            meta.link(*ino, *parent, name)?;
        }
        MutateOp::Unlink { parent, name } => {
            meta.unlink(*parent, name)?;
        }
        MutateOp::Rmdir { parent, name } => {
            meta.rmdir(*parent, name)?;
        }
        MutateOp::Rename {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            meta.rename(*parent, name, *new_parent, new_name)?;
        }
        MutateOp::Setattr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime_ns,
            mtime_ns,
        } => {
            meta.setattr(*ino, *mode, *uid, *gid, *size, *atime_ns, *mtime_ns)?;
        }
        MutateOp::SetManifest {
            ino,
            base_manifest,
            manifest,
            size,
        } => {
            meta.set_manifest_with_base(*ino, base_manifest.as_deref(), manifest, *size)?;
        }
        MutateOp::SetXattr {
            ino,
            name,
            value,
            mode,
        } => {
            let mode = match mode {
                1 => SetXattrMode::Create,
                2 => SetXattrMode::Replace,
                _ => SetXattrMode::Set,
            };
            meta.set_xattr(*ino, name, value, mode)?;
        }
        MutateOp::RemoveXattr { ino, name } => {
            meta.remove_xattr(*ino, name)?;
        }
        MutateOp::Publish {
            ino,
            parent,
            name,
            mode,
            uid,
            gid,
            mtime_ns,
            manifest,
            size,
            xattrs,
        } => {
            meta.publish_file(
                *parent, name, *ino, *mode, *uid, *gid, *mtime_ns, manifest, *size, xattrs,
            )?;
        }
        MutateOp::Records { records } => {
            meta.apply_records_journaled(records)?;
        }
        MutateOp::AtimeBatch { entries } => {
            // Apply to the holder's own inode table (so its stat reflects
            // the read) and queue for shipping. Never writes the journal,
            // so the requester gets back an empty record set — it already
            // applied the bump locally before forwarding.
            meta.apply_atime(entries)?;
            meta.queue_atime(entries)?;
            return Ok(Vec::new());
        }
    }
    meta.peek_journal_after(before)
        .map(|rows| rows.into_iter().map(|(_, r)| r).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;

    #[test]
    fn atime_batch_applies_locally_queues_for_ship_and_journals_nothing() {
        let m = Meta::open_in_memory().unwrap();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let part = "p0".to_string();
        let before = m.journal_len().unwrap();
        let t = f.ctime_ns + 10;
        let records = execute(
            &m,
            &MutateOp::AtimeBatch {
                entries: vec![(f.ino, t, t)],
            },
            None,
        )
        .unwrap();
        // The holder appends nothing to the write journal...
        assert!(records.is_empty());
        assert_eq!(m.journal_len().unwrap(), before);
        // ...applies to its own inode table...
        assert_eq!(m.getattr(f.ino).unwrap().unwrap().atime_ns, t);
        // ...and queues exactly one atime row for the shipper to drain.
        assert_eq!(m.atime_backlog_of(&part).unwrap(), 1);
    }

    #[test]
    fn mutate_op_roundtrip() {
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "a".into(),
            ino: 42,
            mode: 0o644,
            uid: 1,
            gid: 1,
        };
        let bytes = op.to_postcard().unwrap();
        assert_eq!(MutateOp::from_postcard(&bytes).unwrap(), op);
    }

    #[test]
    fn execute_create_journals_one_record() {
        let m = Meta::open_in_memory().unwrap();
        let records = execute(
            &m,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "f".into(),
                ino: (1 << 40) | 7,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            None,
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(
            &records[0],
            LogRecord::Create { name, ino, .. } if name == "f" && *ino == (1 << 40) | 7
        ));
        assert!(m.lookup(ROOT_INO, "f").unwrap().is_some());
    }

    #[test]
    fn execute_publish_carries_xattrs_and_roundtrips() {
        let m = Meta::open_in_memory().unwrap();
        let ino = (1 << 40) | 9;
        let op = MutateOp::Publish {
            ino,
            parent: ROOT_INO,
            name: "published".into(),
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            mtime_ns: 123,
            manifest: b"MANIFEST".to_vec(),
            size: 42,
            xattrs: vec![("user.passsage.meta".into(), b"blob".to_vec())],
        };
        let bytes = op.to_postcard().unwrap();
        assert_eq!(MutateOp::from_postcard(&bytes).unwrap(), op);

        let records = execute(&m, &op, None).unwrap();
        assert_eq!(records.len(), 3);
        assert!(matches!(records[0], LogRecord::Create { .. }));
        assert!(matches!(records[1], LogRecord::WriteManifest { .. }));
        assert!(matches!(
            &records[2],
            LogRecord::SetXattr { name, .. } if name == "user.passsage.meta"
        ));
        assert_eq!(
            m.get_xattr(ino, "user.passsage.meta").unwrap().as_deref(),
            Some(b"blob".as_slice())
        );
    }

    fn rid(seq: u64) -> crate::rid::Rid {
        crate::rid::Rid {
            node: 1,
            incarnation: 1,
            seq,
        }
    }

    /// Plan 30 §M2: passing a rid appends `LogRecord::Completed` in the
    /// same call, and it lands in the `completed` keyspace immediately
    /// (no separate replay step needed on the writer's own replica).
    #[test]
    fn execute_with_rid_appends_completed_and_populates_completed_keyspace() {
        let m = Meta::open_in_memory().unwrap();
        let r = rid(1);
        let records = execute(
            &m,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "f".into(),
                ino: (1 << 40) | 7,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(r),
        )
        .unwrap();
        assert_eq!(records.len(), 2, "op record + Completed");
        assert!(matches!(records[0], LogRecord::Create { .. }));
        assert!(matches!(records[1], LogRecord::Completed { rid } if rid == r));
        assert!(m.completed_position(r).unwrap().is_some());
    }

    /// A multi-record op (`Publish` appends 3) still gets exactly one
    /// `Completed`, appended once, in the same transaction — not once
    /// per underlying record.
    #[test]
    fn execute_with_rid_on_multi_record_op_completes_once() {
        let m = Meta::open_in_memory().unwrap();
        let r = rid(2);
        let ino = (1 << 40) | 9;
        let op = MutateOp::Publish {
            ino,
            parent: ROOT_INO,
            name: "published".into(),
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            mtime_ns: 123,
            manifest: b"MANIFEST".to_vec(),
            size: 42,
            xattrs: vec![("user.passsage.meta".into(), b"blob".to_vec())],
        };
        let records = execute(&m, &op, Some(r)).unwrap();
        assert_eq!(records.len(), 4, "3 op records + one Completed");
        let completed_count = records
            .iter()
            .filter(|r| matches!(r, LogRecord::Completed { .. }))
            .count();
        assert_eq!(completed_count, 1);
        assert!(m.completed_position(r).unwrap().is_some());
    }

    /// A refused op (here: `Create` on an already-existing name) is
    /// never completed — plan 30 §M2: "refusals are not recorded."
    #[test]
    fn execute_with_rid_on_refusal_does_not_complete() {
        let m = Meta::open_in_memory().unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "dup".into(),
            ino: (1 << 40) | 11,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        execute(&m, &op, Some(rid(3))).unwrap();
        let r2 = rid(4);
        let err = execute(&m, &op, Some(r2)).unwrap_err();
        assert!(matches!(err, MetaError::Exists));
        assert!(m.completed_position(r2).unwrap().is_none());
    }
}
