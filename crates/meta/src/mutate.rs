//! Forwarded mutations: the operation a non-holder sends to the lease
//! holder for authoritative validate-and-journal (see DESIGN.md §4/§5).
//!
//! Wire encoding is postcard; the net crate carries these as opaque
//! bytes so it stays free of the metadata dependency.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::sqlite::SqliteMeta;
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
    /// Scratch → shared publish: create the inode at `parent/name` with
    /// the given attrs and commit its manifest in one transaction.
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
    Accepted { epoch: u64, records: Vec<LogRecord> },
    Errno(i32),
    NotHolder { holder: u64 },
    Busy,
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
/// the journal records that were appended. The caller is responsible
/// for checking lease ownership first.
pub fn execute(meta: &SqliteMeta, op: &MutateOp) -> Result<Vec<LogRecord>, MetaError> {
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
        } => {
            meta.publish_file(
                *parent, name, *ino, *mode, *uid, *gid, *mtime_ns, manifest, *size,
            )?;
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
        let m = SqliteMeta::open_in_memory().unwrap();
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
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(
            &records[0],
            LogRecord::Create { name, ino, .. } if name == "f" && *ino == (1 << 40) | 7
        ));
        assert!(m.lookup(ROOT_INO, "f").unwrap().is_some());
    }
}
