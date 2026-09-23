//! Convergence at quiescence (plan 30 §M4 item 5): once writers stop and
//! every node has caught up, every replica — including one bootstrapped
//! fresh from the bucket (head commit plus the log after it) — shows the
//! same tree.
//!
//! The per-step verify reads (`check::check_convergence_reads`) compare
//! the paths one step touched; this compares whole trees after the run,
//! which is what catches state that only some replicas carry (a phantom
//! entry, a lost rename, a commit that is not a log prefix — plan 30
//! bug B's end state).

use crate::check::CheckFailure;
use crate::op::hash_bytes;
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

/// What one path looks like on one replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Dir {
        mode: u32,
    },
    File {
        mode: u32,
        size: u64,
        nlink: u64,
        hash: String,
    },
    Symlink {
        target: String,
    },
    Other,
}

/// A whole tree, by path relative to its root.
pub type TreeSnapshot = BTreeMap<String, Entry>;

/// Walk `root` (without following symlinks) and describe every entry.
pub fn snapshot_tree(root: &Path) -> Result<TreeSnapshot> {
    let mut out = TreeSnapshot::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            let meta = std::fs::symlink_metadata(&path)
                .with_context(|| format!("stat {}", path.display()))?;
            let mode = meta.permissions().mode() & 0o7777;
            let ft = meta.file_type();
            let e = if ft.is_dir() {
                stack.push(path.clone());
                Entry::Dir { mode }
            } else if ft.is_symlink() {
                Entry::Symlink {
                    target: std::fs::read_link(&path)?.to_string_lossy().into_owned(),
                }
            } else if ft.is_file() {
                let data =
                    std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
                Entry::File {
                    mode,
                    size: meta.len(),
                    nlink: meta.nlink(),
                    hash: hash_bytes(&data),
                }
            } else {
                Entry::Other
            };
            out.insert(rel, e);
        }
    }
    Ok(out)
}

/// Every snapshot equals the first. `snaps` are `(replica name, tree)`.
pub fn check_convergence(snaps: &[(String, TreeSnapshot)]) -> Result<(), CheckFailure> {
    let Some((first_name, first)) = snaps.first() else {
        return Ok(());
    };
    for (name, snap) in &snaps[1..] {
        if snap == first {
            continue;
        }
        let mut diffs = Vec::new();
        for (path, e) in first {
            match snap.get(path) {
                None => diffs.push(format!("{path}: on {first_name}, missing on {name}")),
                Some(o) if o != e => {
                    diffs.push(format!("{path}: {first_name} {e:?} vs {name} {o:?}"))
                }
                Some(_) => {}
            }
        }
        for path in snap.keys() {
            if !first.contains_key(path) {
                diffs.push(format!("{path}: on {name}, missing on {first_name}"));
            }
        }
        let shown: Vec<&String> = diffs.iter().take(10).collect();
        return Err(CheckFailure {
            checker: "convergence_at_quiescence".into(),
            message: format!(
                "{name} differs from {first_name} in {} path(s) after quiescence: {shown:?}",
                diffs.len()
            ),
            op_ids: vec![],
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn file(hash: &str) -> Entry {
        Entry::File {
            mode: 0o644,
            size: 1,
            nlink: 1,
            hash: hash.into(),
        }
    }

    #[test]
    fn equal_trees_converge_and_a_phantom_does_not() {
        let a: TreeSnapshot = [
            ("d".to_string(), Entry::Dir { mode: 0o755 }),
            ("d/f".to_string(), file("x")),
        ]
        .into();
        check_convergence(&[("a".into(), a.clone()), ("b".into(), a.clone())]).unwrap();

        // A fresh replica carrying an entry the others do not (bug B).
        let mut fresh = a.clone();
        fresh.insert("d/phantom".into(), file("y"));
        let err =
            check_convergence(&[("a".into(), a.clone()), ("fresh".into(), fresh)]).unwrap_err();
        assert!(err.message.contains("d/phantom"), "{err}");

        // Same names, different content.
        let mut other = a.clone();
        other.insert("d/f".into(), file("z"));
        assert!(check_convergence(&[("a".into(), a), ("b".into(), other)]).is_err());
    }

    #[test]
    fn a_tree_snapshot_describes_files_links_and_dirs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("d")).unwrap();
        std::fs::write(dir.path().join("d/f"), b"hello").unwrap();
        std::fs::hard_link(dir.path().join("d/f"), dir.path().join("g")).unwrap();
        std::os::unix::fs::symlink("d/f", dir.path().join("s")).unwrap();
        let snap = snapshot_tree(dir.path()).unwrap();
        assert!(matches!(snap["d"], Entry::Dir { .. }));
        match &snap["d/f"] {
            Entry::File {
                size, nlink, hash, ..
            } => {
                assert_eq!((*size, *nlink), (5, 2));
                assert_eq!(hash, &hash_bytes(b"hello"));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            snap["s"],
            Entry::Symlink {
                target: "d/f".into()
            }
        );
        let copy = snapshot_tree(dir.path()).unwrap();
        check_convergence(&[("x".into(), snap), ("y".into(), copy)]).unwrap();
    }
}
