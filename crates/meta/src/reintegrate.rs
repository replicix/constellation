//! Stranded-journal reintegration (DESIGN.md §6 relaxed mode, §9).
//!
//! A deposed holder (or a delayed epoch flush) is left with journal
//! records that never reached the shared log. Reintegration walks those
//! records against the *current* replica (the cluster's view after
//! tailing) and either re-journals them as clean appends or materializes
//! the stranded version under `.constellation-conflict/`. Conflicts are
//! never silent.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::sqlite::SqliteMeta;
use crate::MetaStore;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::{Ino, InodeKind};

pub const CONFLICT_DIR: &str = ".constellation-conflict";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    Clean,
    Conflict { reason: String },
}

impl Disposition {
    pub fn is_conflict(&self) -> bool {
        matches!(self, Disposition::Conflict { .. })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Disposition::Clean => "clean",
            Disposition::Conflict { .. } => "conflict",
        }
    }

    pub fn detail(&self) -> String {
        match self {
            Disposition::Clean => String::new(),
            Disposition::Conflict { reason } => reason.clone(),
        }
    }
}

/// Classify one stranded record against `view` (the cluster replica,
/// typically a side copy bootstrapped from S3, or the live replica
/// after the winning side's records have been applied).
pub fn classify(view: &SqliteMeta, rec: &LogRecord) -> Result<Disposition, MetaError> {
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
        } => classify_create(view, *parent, name, *ino),
        LogRecord::Link {
            parent, name, ino, ..
        } => classify_create(view, *parent, name, *ino),
        LogRecord::Unlink { parent, name, .. } | LogRecord::Rmdir { parent, name, .. } => {
            // Idempotent: gone already is clean; present is a clean unlink.
            let _ = (parent, name);
            Ok(Disposition::Clean)
        }
        LogRecord::Rename {
            parent,
            name,
            new_parent,
            new_name,
            ..
        } => {
            if view.getattr(*new_parent)?.is_none() {
                return Ok(Disposition::Conflict {
                    reason: "rename dest parent missing".into(),
                });
            }
            if let Some(attr) = view.lookup(*new_parent, new_name)? {
                if let Some(src) = view.lookup(*parent, name)? {
                    if attr.ino != src.ino {
                        return Ok(Disposition::Conflict {
                            reason: "rename dest taken by another inode".into(),
                        });
                    }
                }
            }
            Ok(Disposition::Clean)
        }
        LogRecord::Setattr { ino, .. }
        | LogRecord::SetXattr { ino, .. }
        | LogRecord::RemoveXattr { ino, .. } => {
            if view.getattr(*ino)?.is_none() {
                Ok(Disposition::Conflict {
                    reason: "attribute target missing (edit-vs-delete)".into(),
                })
            } else {
                Ok(Disposition::Clean)
            }
        }
        LogRecord::WriteManifest {
            ino,
            base_manifest,
            manifest,
            ..
        } => match view.getattr(*ino) {
            Ok(None) => Ok(Disposition::Conflict {
                reason: "manifest target missing (edit-vs-delete)".into(),
            }),
            Ok(Some(_)) => {
                let existing = view.manifest(*ino)?;
                if existing == *base_manifest || existing.as_ref() == Some(manifest) {
                    Ok(Disposition::Clean)
                } else {
                    Ok(Disposition::Conflict {
                        reason: "edit-vs-edit: manifest diverged".into(),
                    })
                }
            }
            Err(e) => Err(e),
        },
        LogRecord::PartSplit { .. }
        | LogRecord::PartMerge { .. }
        | LogRecord::RenameXpartSrc { .. }
        | LogRecord::RenameXpartDst { .. }
        | LogRecord::RenameXpartAbort { .. }
        | LogRecord::SnapCreate { .. }
        | LogRecord::SnapDelete { .. }
        | LogRecord::Clone { .. }
        | LogRecord::SetQuota { .. }
        // Atime never enters the `journal`, so reintegration of stranded
        // journal records should not see one; if it somehow does, it is
        // droppable by definition — never a conflict.
        | LogRecord::Atime { .. } => Ok(Disposition::Clean),
    }
}

fn classify_create(
    view: &SqliteMeta,
    parent: Ino,
    name: &str,
    ino: Ino,
) -> Result<Disposition, MetaError> {
    if view.getattr(parent)?.is_none() {
        return Ok(Disposition::Conflict {
            reason: "parent deleted".into(),
        });
    }
    match view.child_ino(parent, name)? {
        None => Ok(Disposition::Clean),
        Some(existing) if existing == ino => Ok(Disposition::Clean),
        Some(_) => Ok(Disposition::Conflict {
            reason: "create-create: name taken by another inode".into(),
        }),
    }
}

pub fn conflict_dentry_name(name: &str, node_id: u64, ts_unix: i64) -> String {
    format!("{name}@{node_id}-{ts_unix}")
}

/// Write the stranded version under `<parent>/.constellation-conflict/`.
/// The conflict dir is ordinary namespace content. Returns the relative
/// path created (`<dir>/<name>@<node>-<ts>`).
pub fn materialize(
    live: &SqliteMeta,
    rec: &LogRecord,
    node_id: u64,
    ts_unix: i64,
) -> Result<String, MetaError> {
    let (parent, name, kind, manifest) = match rec {
        LogRecord::Mkdir { parent, name, .. } => (*parent, name.clone(), InodeKind::Dir, None),
        LogRecord::Create { parent, name, .. } => (*parent, name.clone(), InodeKind::File, None),
        LogRecord::Symlink { parent, name, .. } => (*parent, name.clone(), InodeKind::File, None),
        LogRecord::Mknod { parent, name, .. } => (*parent, name.clone(), InodeKind::File, None),
        LogRecord::WriteManifest {
            ino,
            manifest,
            size,
            ..
        } => {
            let parent = live.parent_of(*ino)?.unwrap_or(ROOT_INO);
            let path = live.path_of(*ino).unwrap_or_else(|_| format!("ino-{ino}"));
            let name = path.rsplit('/').next().unwrap_or("file").to_string();
            (
                parent,
                name,
                InodeKind::File,
                Some((manifest.clone(), *size)),
            )
        }
        LogRecord::Setattr { ino, .. } => {
            let parent = live.parent_of(*ino)?.unwrap_or(ROOT_INO);
            let path = live.path_of(*ino).unwrap_or_else(|_| format!("ino-{ino}"));
            let name = path.rsplit('/').next().unwrap_or("file").to_string();
            let man = live.manifest(*ino)?.map(|manifest| {
                let size = live
                    .getattr(*ino)
                    .ok()
                    .flatten()
                    .map(|attr| attr.size)
                    .unwrap_or(0);
                (manifest, size)
            });
            (parent, name, InodeKind::File, man)
        }
        LogRecord::Link { parent, name, .. } => (*parent, name.clone(), InodeKind::File, None),
        _ => (ROOT_INO, "unnamed".into(), InodeKind::File, None),
    };
    let parent = if live.getattr(parent)?.is_some() {
        parent
    } else {
        ROOT_INO
    };
    if live.child_ino(parent, CONFLICT_DIR)?.is_none() {
        live.mkdir(parent, CONFLICT_DIR, 0o755, 0, 0)?;
    }
    let dir = live
        .child_ino(parent, CONFLICT_DIR)?
        .ok_or(MetaError::NoEntry)?;
    let dest = conflict_dentry_name(&name, node_id, ts_unix);
    if live.child_ino(dir, &dest)?.is_some() {
        return Ok(format!("{CONFLICT_DIR}/{dest}"));
    }
    match kind {
        InodeKind::Dir => {
            live.mkdir(dir, &dest, 0o755, 0, 0)?;
        }
        _ => {
            let attr = live.create(dir, &dest, 0o644, 0, 0)?;
            if let Some((manifest, size)) = manifest {
                live.set_manifest(attr.ino, &manifest, size)?;
            }
        }
    }
    Ok(format!("{CONFLICT_DIR}/{dest}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;

    fn rec_create(parent: Ino, name: &str, ino: Ino) -> LogRecord {
        LogRecord::Create {
            parent,
            name: name.into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }
    }

    fn rec_manifest(ino: Ino, bytes: &[u8]) -> LogRecord {
        LogRecord::WriteManifest {
            ino,
            base_manifest: None,
            manifest: bytes.to_vec(),
            size: bytes.len() as u64,
            time_ns: 2,
        }
    }

    #[test]
    fn create_create_same_name_is_conflict() {
        let shared = SqliteMeta::open_in_memory().unwrap();
        let winner = shared.create(ROOT_INO, "foo", 0o644, 0, 0).unwrap();
        let stranded = rec_create(ROOT_INO, "foo", winner.ino + 99);
        match classify(&shared, &stranded).unwrap() {
            Disposition::Conflict { reason } => assert!(reason.contains("create-create")),
            other => panic!("expected conflict, got {other:?}"),
        }
    }

    #[test]
    fn create_into_free_name_is_clean() {
        let shared = SqliteMeta::open_in_memory().unwrap();
        let stranded = rec_create(ROOT_INO, "only-ours", 42);
        assert_eq!(classify(&shared, &stranded).unwrap(), Disposition::Clean);
    }

    #[test]
    fn parent_deleted_is_conflict() {
        let shared = SqliteMeta::open_in_memory().unwrap();
        let stranded = rec_create(999, "x", 42);
        match classify(&shared, &stranded).unwrap() {
            Disposition::Conflict { reason } => assert!(reason.contains("parent")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn edit_vs_edit_is_conflict() {
        let shared = SqliteMeta::open_in_memory().unwrap();
        let f = shared.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        shared.set_manifest(f.ino, b"winner", 6).unwrap();
        let stranded = rec_manifest(f.ino, b"loser-version");
        match classify(&shared, &stranded).unwrap() {
            Disposition::Conflict { reason } => assert!(reason.contains("edit-vs-edit")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn edit_with_unchanged_baseline_is_clean() {
        let shared = SqliteMeta::open_in_memory().unwrap();
        let f = shared.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        shared.set_manifest(f.ino, b"baseline", 8).unwrap();
        let stranded = LogRecord::WriteManifest {
            ino: f.ino,
            base_manifest: Some(b"baseline".to_vec()),
            manifest: b"ours".to_vec(),
            size: 4,
            time_ns: 3,
        };
        assert_eq!(classify(&shared, &stranded).unwrap(), Disposition::Clean);
    }

    #[test]
    fn edit_vs_delete_is_conflict() {
        let shared = SqliteMeta::open_in_memory().unwrap();
        let stranded = rec_manifest(42, b"orphaned");
        match classify(&shared, &stranded).unwrap() {
            Disposition::Conflict { reason } => assert!(reason.contains("edit-vs-delete")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn materialize_layout_under_conflict_dir() {
        let live = SqliteMeta::open_in_memory().unwrap();
        let stranded = rec_create(ROOT_INO, "foo", 99);
        let path = materialize(&live, &stranded, 7, 1_700_000_000).unwrap();
        assert_eq!(path, ".constellation-conflict/foo@7-1700000000");
        let dir = live
            .lookup(ROOT_INO, CONFLICT_DIR)
            .unwrap()
            .expect("conflict dir");
        assert_eq!(dir.kind, InodeKind::Dir);
        assert!(live.lookup(dir.ino, "foo@7-1700000000").unwrap().is_some());
    }

    #[test]
    fn materialize_manifest_keeps_stranded_bytes() {
        let live = SqliteMeta::open_in_memory().unwrap();
        let f = live.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        live.set_manifest(f.ino, b"stranded", 8).unwrap();
        let rec = rec_manifest(f.ino, b"stranded");
        materialize(&live, &rec, 1, 42).unwrap();
        let dir = live.lookup(ROOT_INO, CONFLICT_DIR).unwrap().unwrap();
        let copy = live.lookup(dir.ino, "f@1-42").unwrap().unwrap();
        assert_eq!(
            live.manifest(copy.ino).unwrap().as_deref(),
            Some(&b"stranded"[..])
        );
    }

    #[test]
    fn reintegrate_commit_is_idempotent() {
        let m = SqliteMeta::open_in_memory().unwrap();
        m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let (seq, rec) = m.take_journal(1).unwrap().into_iter().next().unwrap();
        m.reintegrate_commit(seq, "clean", "", Some(&rec)).unwrap();
        m.reintegrate_commit(seq, "clean", "", Some(&rec)).unwrap();
        // Original row is gone and exactly one replacement remains;
        // retrying a marked seq is a no-op.
        assert_eq!(m.reintegration_conflict_count().unwrap(), 0);
        assert_eq!(m.unmarked_journal().unwrap().len(), 1);
        assert_eq!(m.unmarked_journal_len().unwrap(), 1);
    }
}
