//! CLI name resolution (plan 21, step 3): turn `myfs:/data`, a bare
//! `myfs`, or a literal path/selector into either a registered
//! filesystem's entry or a raw value that needs an explicit
//! `--state-dir`/`--s3`.

use crate::registry::{FsEntry, Registry};
use anyhow::{bail, Result};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum Target {
    /// `name` is a registered filesystem; `path` is whatever followed
    /// the `:`, if any (`None` for a bare `myfs`).
    Named {
        name: String,
        path: Option<String>,
        entry: FsEntry,
    },
    /// A literal path/selector/positional, not a registry name. Needs an
    /// explicit `--state-dir`/`--s3` from the caller.
    Raw(String),
}

/// Split `raw` on `:` **only** when the part before it contains no `/`,
/// then look the candidate name up in `registry`. A leading `/` is never
/// reinterpreted (`/myfs:backup` stays one raw string; `myfs` cannot
/// contain `/`, so `part.before` containing one rules out the name
/// reading on its own, without a special case).
pub fn resolve(raw: &str, registry: &Registry) -> Target {
    if let Some((before, after)) = raw.split_once(':') {
        if !before.is_empty() && !before.contains('/') {
            if let Some(entry) = registry.entry(before) {
                let path = (!after.is_empty()).then(|| after.to_string());
                return Target::Named {
                    name: before.to_string(),
                    path,
                    entry: entry.clone(),
                };
            }
        }
    } else if !raw.is_empty() && !raw.contains('/') {
        // Bare token, no colon at all: still worth a registry lookup —
        // `mount myfs` (no subtree) is exactly this shape.
        if let Some(entry) = registry.entry(raw) {
            return Target::Named {
                name: raw.to_string(),
                path: None,
                entry: entry.clone(),
            };
        }
    }
    Target::Raw(raw.to_string())
}

/// Resolve the effective state dir: explicit `--state-dir` wins, else a
/// registered name's stored state dir, else an error naming what was
/// missing. There is exactly one state dir per name, independent of how
/// many views happen to be mounted.
pub fn state_dir(explicit: Option<PathBuf>, target: &Target) -> Result<PathBuf> {
    if let Some(dir) = explicit {
        return Ok(dir);
    }
    match target {
        Target::Named { entry, .. } => Ok(entry.state_dir.clone()),
        Target::Raw(raw) => bail!(
            "{raw:?} is not a registered filesystem name; pass --state-dir (or register it \
             with `constellation fs create`/`mount`)"
        ),
    }
}

/// Resolve the effective backend URL: explicit `--s3` wins, else a
/// registered name's stored `s3`, else an error. Used by commands that
/// talk to the backend directly (`gc`, `fsck`, `doctor`, `fs passwd`).
pub fn s3_url(explicit: Option<String>, target: &Target) -> Result<String> {
    if let Some(s3) = explicit {
        return Ok(s3);
    }
    match target {
        Target::Named { entry, .. } => Ok(entry.s3.clone()),
        Target::Raw(raw) => bail!(
            "{raw:?} is not a registered filesystem name; pass --s3 (or register it with \
             `constellation fs create`)"
        ),
    }
}

/// Same as [`state_dir`], but "opportunistic": a `Raw` target (or one
/// with no explicit `--state-dir`) is not an error, just `None` — used
/// by commands (`gc`, `fsck`, `doctor`, `fs passwd`) that always resolve
/// `--s3` but only use a state dir when one happens to be available.
pub fn state_dir_opt(explicit: Option<PathBuf>, target: &Target) -> Option<PathBuf> {
    explicit.or_else(|| match target {
        Target::Named { entry, .. } => Some(entry.state_dir.clone()),
        Target::Raw(_) => None,
    })
}

/// Effective path for path-taking control commands (`pin`, `unpin`,
/// `offline`, `online`, `inspect`, `snapshot ls`): a bare name means the
/// filesystem root.
pub fn effective_path(target: &Target) -> String {
    match target {
        Target::Named { path: None, .. } => "/".to_string(),
        Target::Named {
            path: Some(path), ..
        } => path.clone(),
        Target::Raw(raw) => raw.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::FsOverrides;

    fn registry_with(dir: &std::path::Path, name: &str) -> Registry {
        let mut reg = Registry::load_locked_at(dir.join("registry.toml")).unwrap();
        reg.merge_and_save(
            name,
            FsOverrides {
                s3: Some("s3://bucket/prefix".into()),
                ..Default::default()
            },
        )
        .unwrap();
        reg
    }

    #[test]
    fn named_with_subtree() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry_with(dir.path(), "myfs");
        match resolve("myfs:/data", &reg) {
            Target::Named { name, path, .. } => {
                assert_eq!(name, "myfs");
                assert_eq!(path.as_deref(), Some("/data"));
            }
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn bare_name() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry_with(dir.path(), "myfs");
        match resolve("myfs", &reg) {
            Target::Named { name, path, .. } => {
                assert_eq!(name, "myfs");
                assert_eq!(path, None);
            }
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn leading_slash_is_never_reinterpreted_even_with_a_colon() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry_with(dir.path(), "myfs");
        // Looks like it has a colon, but the part before it ("/myfs")
        // contains a slash, so this stays one raw string even though
        // "myfs" alone is registered.
        match resolve("/myfs:backup", &reg) {
            Target::Raw(raw) => assert_eq!(raw, "/myfs:backup"),
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    #[test]
    fn unregistered_name_falls_back_to_raw() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::load_locked_at(dir.path().join("registry.toml")).unwrap();
        match resolve("myfs:/data", &reg) {
            Target::Raw(raw) => assert_eq!(raw, "myfs:/data"),
            other => panic!("expected Raw (myfs is not registered), got {other:?}"),
        }
        match resolve("/data", &reg) {
            Target::Raw(raw) => assert_eq!(raw, "/data"),
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    #[test]
    fn state_dir_prefers_explicit_over_registry() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry_with(dir.path(), "myfs");
        let target = resolve("myfs", &reg);
        assert_eq!(
            state_dir(Some(PathBuf::from("/explicit")), &target).unwrap(),
            PathBuf::from("/explicit")
        );
        let from_registry = state_dir(None, &target).unwrap();
        assert_eq!(
            from_registry,
            crate::registry::default_named_state_dir("myfs").unwrap()
        );
    }

    #[test]
    fn state_dir_errors_for_an_unregistered_raw_target() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::load_locked_at(dir.path().join("registry.toml")).unwrap();
        let target = resolve("/some/path", &reg);
        assert!(state_dir(None, &target).is_err());
    }
}
