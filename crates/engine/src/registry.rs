//! Local, per-user registry of named filesystems (plan 21, step 2) —
//! `zpool.cache` for constellation. Maps a short name to the backend it
//! points at, the state dir that owns its node identity, and the views
//! last mounted for it. `mount` is the only thing that *writes* a live
//! entry (`merge_and_save`); `export` (step 6) is the only thing that
//! removes one (`remove`).
//!
//! Node-level settings (backend, cache, write mode, ...) live directly
//! under `[NAME]`; per-view settings that could legitimately differ
//! between two mountpoints of the same filesystem live in
//! `[[NAME.mounts]]` rows. See the plan's Step 2 for the schema
//! rationale: the process, cache, leases, and node identity are
//! name-level, so they cannot sit in a per-mount row where two rows
//! could disagree.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One view of a registered filesystem, as last mounted.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MountEntry {
    /// `""` means the root; may also be a `path@snapshot` selector.
    #[serde(default)]
    pub subtree: String,
    pub mountpoint: PathBuf,
    #[serde(default)]
    pub allow_other: bool,
    #[serde(default)]
    pub fs_name: String,
    #[serde(default)]
    pub rw: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone_name: Option<String>,
    #[serde(default)]
    pub ephemeral: bool,
}

/// One registered filesystem's node-level configuration plus its views.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FsEntry {
    pub s3: String,
    pub state_dir: PathBuf,
    /// `"10G"`, `"512MiB"`, ... — kept as the operator's own spelling
    /// (round-trips through the file unchanged) rather than a byte
    /// count; parsed with `parse_byte_size` where it is used.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cache_size: String,
    /// Optional override; default is `<state_dir>/cache` when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cache_dir: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fsync_mode: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub write_mode: String,
    #[serde(default)]
    pub read_only_member: bool,
    #[serde(default)]
    pub web_ui: u16,
    /// The S3 endpoint this name's `meta.json` was last read from (or
    /// created at). Diagnostics only: a later command that resolves a
    /// different endpoint and finds no filesystem says so, because that
    /// is a command run without the filesystem's `AWS_PROFILE` /
    /// `AWS_CONFIG_FILE` / `AWS_ENDPOINT_URL` environment.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,
    #[serde(default, rename = "mounts")]
    pub mounts: Vec<MountEntry>,
}

impl FsEntry {
    pub fn mount(&self, subtree: &str) -> Option<&MountEntry> {
        self.mounts.iter().find(|m| m.subtree == subtree)
    }
}

/// Explicit overrides for `merge_and_save`. `None` means "leave whatever
/// is already stored (or the type default for a brand-new entry)" — the
/// "any explicit argument overwrites the stored value" rule.
#[derive(Debug, Clone, Default)]
pub struct FsOverrides {
    pub s3: Option<String>,
    pub cache_size: Option<String>,
    pub cache_dir: Option<String>,
    pub fsync_mode: Option<String>,
    pub write_mode: Option<String>,
    pub read_only_member: Option<bool>,
    pub web_ui: Option<u16>,
    pub endpoint: Option<String>,
    /// The one view this `mount` invocation touched, if any. Bare
    /// `mount NAME` (no subtree/mountpoint given) supplies `None` here
    /// and only reads the registry back.
    pub mount: Option<MountEntry>,
}

pub struct Registry {
    path: PathBuf,
    entries: BTreeMap<String, FsEntry>,
    /// Held for the lifetime of the `Registry` value returned by
    /// `load_locked` so a whole read-modify-write cycle (as
    /// `merge_and_save`/`remove` need) is atomic across processes. Plain
    /// `load` does not take it — a read-only glance (`fs list`, `status`
    /// name resolution) does not need to serialize against writers.
    _lock: Option<constellation_platform::LockGuard>,
}

impl Registry {
    /// `$CONSTELLATION_REGISTRY`, else
    /// `$XDG_CONFIG_HOME/constellation/registry.toml`, else
    /// `~/.config/constellation/registry.toml`.
    pub fn path() -> Result<PathBuf> {
        if let Ok(p) = std::env::var("CONSTELLATION_REGISTRY") {
            if !p.is_empty() {
                return Ok(PathBuf::from(p));
            }
        }
        let config = constellation_platform::native()
            .dirs
            .config_dir()
            .context("locating the config dir")?;
        Ok(config.join("registry.toml"))
    }

    /// Read-only load: no cross-process lock, safe for concurrent
    /// readers (`fs list`, target resolution).
    pub fn load() -> Result<Self> {
        Self::load_at(Self::path()?)
    }

    /// Load holding an exclusive advisory lock on the registry file
    /// (not the per-name `daemon.lock` from Step 4 — this is only the
    /// convenience file's own writer lock) until the returned value is
    /// dropped. Use around a `merge_and_save`/`remove` so two `mount`
    /// invocations for different names cannot interleave writes and
    /// silently drop one.
    pub fn load_locked() -> Result<Self> {
        Self::load_locked_at(Self::path()?)
    }

    /// Same as [`Registry::load`], against an explicit path rather than
    /// the process-wide `CONSTELLATION_REGISTRY`/XDG resolution — this is
    /// what lets tests exercise the registry without mutating shared
    /// process environment (parallel `#[test]` fns would otherwise race
    /// on it).
    pub fn load_at(path: PathBuf) -> Result<Self> {
        let entries = Self::read(&path)?;
        Ok(Self {
            path,
            entries,
            _lock: None,
        })
    }

    /// Same as [`Registry::load_locked`], against an explicit path. See
    /// [`Registry::load_at`].
    pub fn load_locked_at(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let lock_file =
            constellation_platform::lock::open_lock_file(&path.with_extension("toml.lock"))
                .context("opening registry lock file")?;
        let lock = constellation_platform::native()
            .file_lock
            .lock(lock_file)
            .context("locking registry file")?;
        let entries = Self::read(&path)?;
        Ok(Self {
            path,
            entries,
            _lock: Some(lock),
        })
    }

    fn read(path: &Path) -> Result<BTreeMap<String, FsEntry>> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    fn write(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let text = toml::to_string_pretty(&self.entries).context("serializing registry")?;
        // Write-then-rename: a reader never observes a half-written file.
        let tmp = self.path.with_extension("toml.tmp");
        std::fs::write(&tmp, text).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("renaming into place: {}", self.path.display()))
    }

    pub fn entry(&self, name: &str) -> Option<&FsEntry> {
        self.entries.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &FsEntry)> {
        self.entries.iter()
    }

    /// The one place that *writes* a live entry. Any `Some(..)` in
    /// `overrides` replaces the stored value (or seeds a new entry);
    /// `None` keeps whatever is already there. `overrides.mount`, if
    /// given, upserts by `subtree` (matching an existing row's
    /// mountpoint/options, or appending a new row).
    ///
    /// Callers implement the "`--s3` is refused against a populated
    /// state dir" rule themselves (Step 4) — that check needs to look at
    /// the state dir's `meta.db`, which this module does not touch.
    pub fn merge_and_save(&mut self, name: &str, overrides: FsOverrides) -> Result<FsEntry> {
        if name.is_empty() {
            bail!("filesystem name must not be empty");
        }
        let mut entry = self.entries.remove(name).unwrap_or_default();
        if let Some(s3) = overrides.s3 {
            entry.s3 = s3;
        }
        if entry.state_dir.as_os_str().is_empty() {
            entry.state_dir = default_named_state_dir(name)?;
        }
        if let Some(v) = overrides.cache_size {
            entry.cache_size = v;
        }
        if let Some(v) = overrides.cache_dir {
            entry.cache_dir = v;
        }
        if let Some(v) = overrides.fsync_mode {
            entry.fsync_mode = v;
        }
        if let Some(v) = overrides.write_mode {
            entry.write_mode = v;
        }
        if let Some(v) = overrides.read_only_member {
            entry.read_only_member = v;
        }
        if let Some(v) = overrides.web_ui {
            entry.web_ui = v;
        }
        if let Some(v) = overrides.endpoint {
            entry.endpoint = v;
        }
        if let Some(view) = overrides.mount {
            match entry.mounts.iter_mut().find(|m| m.subtree == view.subtree) {
                Some(existing) => *existing = view,
                None => entry.mounts.push(view),
            }
        }
        if entry.s3.is_empty() {
            bail!("filesystem {name:?} has no --s3 on record; pass --s3 to register it");
        }
        self.entries.insert(name.to_string(), entry.clone());
        self.write()?;
        Ok(entry)
    }

    /// Remove a whole filesystem's row (`export`, Step 6). A no-op,
    /// successful call if the name was already absent.
    pub fn remove(&mut self, name: &str) -> Result<()> {
        self.entries.remove(name);
        self.write()
    }
}

/// `$XDG_DATA_HOME/constellation/<name>`, falling back to
/// `~/.local/share/constellation/<name>` (the host's `Dirs::state_dir`).
pub fn default_named_state_dir(name: &str) -> Result<PathBuf> {
    constellation_platform::native()
        .dirs
        .state_dir(name)
        .context("locating the data dir")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry_path(dir: &Path) -> PathBuf {
        dir.join("registry.toml")
    }

    #[test]
    fn merge_and_save_overwrites_only_explicit_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load_locked_at(registry_path(dir.path())).unwrap();
        reg.merge_and_save(
            "myfs",
            FsOverrides {
                s3: Some("s3://bucket/prefix".into()),
                write_mode: Some("through".into()),
                ..Default::default()
            },
        )
        .unwrap();
        // A second call that only touches cache_size must not clobber s3
        // or write_mode — the "any explicit argument overwrites" rule
        // cuts both ways: unset fields are left alone.
        let entry = reg
            .merge_and_save(
                "myfs",
                FsOverrides {
                    cache_size: Some("2G".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(entry.s3, "s3://bucket/prefix");
        assert_eq!(entry.write_mode, "through");
        assert_eq!(entry.cache_size, "2G");
    }

    #[test]
    fn node_level_and_per_view_settings_land_in_the_right_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = registry_path(dir.path());
        let mut reg = Registry::load_locked_at(path.clone()).unwrap();
        reg.merge_and_save(
            "myfs",
            FsOverrides {
                s3: Some("s3://bucket/prefix".into()),
                mount: Some(MountEntry {
                    subtree: String::new(),
                    mountpoint: "/mnt".into(),
                    fs_name: "myfs".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("s3 = "), "node-level field missing: {text}");
        assert!(text.contains("[[myfs.mounts]]"), "view row missing: {text}");

        // A second view (subtree) upserts a new row, not the first one.
        reg.merge_and_save(
            "myfs",
            FsOverrides {
                mount: Some(MountEntry {
                    subtree: "/data".into(),
                    mountpoint: "/mnt-data".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let entry = reg.entry("myfs").unwrap();
        assert_eq!(entry.mounts.len(), 2);
        assert_eq!(entry.mount("").unwrap().mountpoint, PathBuf::from("/mnt"));
        assert_eq!(
            entry.mount("/data").unwrap().mountpoint,
            PathBuf::from("/mnt-data")
        );

        // Re-mounting the same view (same subtree) at a new mountpoint
        // upserts in place rather than appending a duplicate row.
        reg.merge_and_save(
            "myfs",
            FsOverrides {
                mount: Some(MountEntry {
                    subtree: "/data".into(),
                    mountpoint: "/mnt-data-2".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let entry = reg.entry("myfs").unwrap();
        assert_eq!(entry.mounts.len(), 2);
        assert_eq!(
            entry.mount("/data").unwrap().mountpoint,
            PathBuf::from("/mnt-data-2")
        );
    }

    #[test]
    fn remove_deletes_the_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = registry_path(dir.path());
        let mut reg = Registry::load_locked_at(path.clone()).unwrap();
        reg.merge_and_save(
            "myfs",
            FsOverrides {
                s3: Some("s3://bucket/prefix".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(reg.entry("myfs").is_some());
        reg.remove("myfs").unwrap();
        assert!(reg.entry("myfs").is_none());
        // Round-trips through a fresh load.
        let reg2 = Registry::load_at(path).unwrap();
        assert!(reg2.entry("myfs").is_none());
    }

    #[test]
    fn toml_round_trip_preserves_every_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = registry_path(dir.path());
        let mut reg = Registry::load_locked_at(path.clone()).unwrap();
        reg.merge_and_save(
            "myfs",
            FsOverrides {
                s3: Some("s3://bucket/prefix".into()),
                cache_size: Some("10G".into()),
                fsync_mode: Some("local".into()),
                write_mode: Some("through".into()),
                read_only_member: Some(false),
                web_ui: Some(8080),
                mount: Some(MountEntry {
                    subtree: String::new(),
                    mountpoint: "/mnt".into(),
                    allow_other: true,
                    fs_name: "myfs".into(),
                    rw: false,
                    clone_name: None,
                    ephemeral: false,
                }),
                ..Default::default()
            },
        )
        .unwrap();
        drop(reg);
        let reg2 = Registry::load_at(path).unwrap();
        let entry = reg2.entry("myfs").unwrap();
        assert_eq!(entry.s3, "s3://bucket/prefix");
        assert_eq!(entry.cache_size, "10G");
        assert_eq!(entry.web_ui, 8080);
        assert_eq!(entry.mounts.len(), 1);
        assert!(entry.mounts[0].allow_other);
    }

    #[test]
    fn concurrent_writers_serialize_through_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = registry_path(dir.path());
        std::thread::scope(|scope| {
            for i in 0..8 {
                let path = path.clone();
                scope.spawn(move || {
                    let mut reg = Registry::load_locked_at(path).unwrap();
                    reg.merge_and_save(
                        &format!("fs{i}"),
                        FsOverrides {
                            s3: Some(format!("s3://bucket/fs{i}")),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                });
            }
        });
        let reg = Registry::load_at(path).unwrap();
        assert_eq!(
            reg.iter().count(),
            8,
            "a concurrent writer clobbered another's row"
        );
    }
}
