//! Local, per-user registry of named filesystems (plan 21, step 2) —
//! `zpool.cache` for constellation. Maps a short name to the backend it
//! points at, the state dir that owns its node identity, and the views
//! last mounted for it. Every write goes through `merge_and_save`
//! (`mount`, `fs create`, `fs.import`); `export` (step 6) is the only thing
//! that removes one (`remove`). This is the storage of the filesystem
//! [`Manager`](crate::manager::Manager), which owns every access to it and
//! the rules below (plan 31 §4).
//!
//! **Identity is pinned to the name** (plan 21): a name owns one state
//! dir, which owns one replica and one `node_id`. So a name's backend URL
//! changes only while its state dir is not in use
//! ([`Registry::check_repoint`]), a name's state dir moves only to a new
//! or empty dir that is nobody else's ([`Registry::explicit_state_dir`]),
//! and a name is a single path component ([`validate_name`]): its default
//! state dir is `<data dir>/<name>`, which `export` deletes.
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
    pub subtree: String,
    pub mountpoint: PathBuf,
    pub allow_other: bool,
    pub fs_name: String,
    pub rw: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone_name: Option<String>,
    pub ephemeral: bool,
    /// `--confine-links` (plan 31 §6.12).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub confine_links: bool,
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
    pub read_only_member: bool,
    pub web_ui: u16,
    /// The S3 endpoint this name's `meta.json` was last read from (or
    /// created at). Diagnostics only: a later command that resolves a
    /// different endpoint and finds no filesystem says so, because that
    /// is a command run without the filesystem's `AWS_PROFILE` /
    /// `AWS_CONFIG_FILE` / `AWS_ENDPOINT_URL` environment.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,
    #[serde(rename = "mounts")]
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
    /// `mount NAME --state-dir DIR`, checked by
    /// [`Registry::explicit_state_dir`] first.
    pub state_dir: Option<PathBuf>,
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
    /// `load_locked_at` so a whole read-modify-write cycle (as
    /// `merge_and_save`/`remove` need) is atomic across processes. Plain
    /// `load_at` does not take it — a read-only glance (`fs list`, `status`
    /// name resolution) does not need to serialize against writers.
    _lock: Option<constellation_platform::LockGuard>,
    /// Where a name's default state dir is (`<data dir>/<name>`): the
    /// manager's host's dirs, else this process's native ones.
    dirs: Option<std::sync::Arc<dyn constellation_platform::Dirs>>,
}

impl Registry {
    /// `$CONSTELLATION_REGISTRY`, else
    /// `$XDG_CONFIG_HOME/constellation/registry.toml`, else
    /// `~/.config/constellation/registry.toml`.
    pub fn path() -> Result<PathBuf> {
        Self::path_in(constellation_platform::native())
    }

    /// [`Registry::path`] in `host`'s config dir.
    pub fn path_in(host: &constellation_platform::HostServices) -> Result<PathBuf> {
        if let Ok(p) = std::env::var("CONSTELLATION_REGISTRY") {
            if !p.is_empty() {
                return Ok(PathBuf::from(p));
            }
        }
        let config = host.dirs.config_dir().context("locating the config dir")?;
        Ok(config.join("registry.toml"))
    }

    /// Read-only load of the registry at `path`: no cross-process lock,
    /// safe for concurrent readers (`fs list`, target resolution). The
    /// [`Manager`](crate::manager::Manager) resolves the path (`path_in`),
    /// or a test or embedder gives its own without touching the
    /// process-wide `CONSTELLATION_REGISTRY` (parallel `#[test]` fns would
    /// race on it).
    pub fn load_at(path: PathBuf) -> Result<Self> {
        let entries = Self::read(&path)?;
        Ok(Self {
            path,
            entries,
            _lock: None,
            dirs: None,
        })
    }

    /// Load holding an exclusive advisory lock on the registry file
    /// (not the per-name `daemon.lock` — this is only the convenience
    /// file's own writer lock) until the returned value is dropped. Use
    /// around a `merge_and_save`/`remove` so two writers for different
    /// names cannot interleave writes and silently drop one.
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
            dirs: None,
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
    /// `name` must be a valid name ([`validate_name`]). The identity rules
    /// are not applied here: a caller that changes a name's URL or state
    /// dir checks [`Registry::check_repoint`] / [`Registry::explicit_state_dir`]
    /// first, under this registry's lock (the filesystem
    /// [`Manager`](crate::manager::Manager) does).
    pub fn merge_and_save(&mut self, name: &str, overrides: FsOverrides) -> Result<FsEntry> {
        validate_name(name)?;
        let mut entry = self.entries.remove(name).unwrap_or_default();
        if let Some(s3) = overrides.s3 {
            entry.s3 = s3;
        }
        if let Some(dir) = overrides.state_dir {
            entry.state_dir = dir;
        }
        if entry.state_dir.as_os_str().is_empty() {
            entry.state_dir = match &self.dirs {
                Some(dirs) => dirs.state_dir(name).context("locating the data dir")?,
                None => default_named_state_dir(name)?,
            };
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

    /// The state dir `mount NAME --state-dir DIR` makes `name`'s: `explicit`,
    /// made absolute (the registry outlives the working directory). An
    /// explicit argument overwrites the stored value (plan 21), and an
    /// explicit `--state-dir` wins over the registered one in every other
    /// command (`target::state_dir`): a `mount` that kept the registered
    /// one would serve the name from a dir its caller never named, and
    /// `status NAME --state-dir DIR` would then look for a daemon where
    /// none runs.
    ///
    /// Identity is pinned to the name, as for `--s3`: a name owns one
    /// state dir, which owns its filesystem's replica and its `node_id`.
    /// So, unless `DIR` is the stored dir itself (however spelled):
    ///
    /// - refused while the stored dir is in use: populated (`meta.db`), or
    ///   its `daemon.lock` held — a daemon still bootstrapping has no
    ///   `meta.db` yet, and a second one in `DIR` would be a second
    ///   `node_id` for the name. Its replica and `node_id` would be
    ///   abandoned without leaving the cluster: `export NAME` retires them;
    /// - `DIR` must not exist yet or be an empty directory: `export NAME`
    ///   deletes the name's state dir with everything in it (`--state-dir .`
    ///   in `$HOME` must not make `$HOME` that), and a populated `DIR` is
    ///   some other replica, not this name's;
    /// - `DIR` must not be another name's state dir.
    pub fn explicit_state_dir(&self, name: &str, explicit: &Path) -> Result<PathBuf> {
        let dir = std::path::absolute(explicit)
            .with_context(|| format!("resolving --state-dir {}", explicit.display()))?;
        if let Some(current) = self.entry(name) {
            let stored = &current.state_dir;
            if !stored.as_os_str().is_empty() && same_dir(stored, &dir) {
                // (spelled as stored: a symlink or `..` to it moves nothing)
                return Ok(stored.clone());
            }
            if state_dir_in_use(stored) {
                bail!(
                    "{name}'s state dir is {}, which is in use; `--state-dir {}` would move it. \
                     Run `constellation export {name}` first if you really want to move it.",
                    stored.display(),
                    dir.display()
                );
            }
        }
        let new_or_empty = match std::fs::read_dir(&dir) {
            Err(e) => e.kind() == std::io::ErrorKind::NotFound,
            Ok(mut entries) => entries.next().is_none(),
        };
        if !new_or_empty {
            bail!(
                "--state-dir {} exists and is not an empty directory: give {name} a new or empty \
                 one (`constellation export {name}` deletes its state dir with everything in it)",
                dir.display()
            );
        }
        if let Some((other, _)) = self
            .entries
            .iter()
            .find(|(other, e)| *other != name && same_dir(&e.state_dir, &dir))
        {
            bail!("{} is already the state dir of {other}", dir.display());
        }
        Ok(dir)
    }

    /// Whether `name` may point at `s3`: refused while it points elsewhere
    /// and its state dir is in use ([`state_dir_in_use`]) — that replica
    /// and its `node_id` would serve, or be abandoned by, a different
    /// filesystem. A name that is not registered, already points there, or
    /// was registered but never mounted (`fs create`, then `fs create`
    /// again elsewhere — the test suites' fixed names) may. Plan 21's rule
    /// for `mount --s3`, applied by every writer of a name's URL.
    pub fn check_repoint(&self, name: &str, s3: &str) -> Result<(), Repoint> {
        let Some(current) = self.entry(name) else {
            return Ok(());
        };
        if current.s3.is_empty() || current.s3 == s3 || !state_dir_in_use(&current.state_dir) {
            return Ok(());
        }
        Err(Repoint {
            name: name.to_string(),
            current: current.s3.clone(),
            requested: s3.to_string(),
        })
    }

    /// Whether `export NAME` may delete `name`'s state dir with everything
    /// in it: never a dir that holds (or is) another registered name's
    /// state dir or cache dir, the registry itself, or one of the host's
    /// `dirs` (data, config, runtime) — `export` of a name registered
    /// before names were checked (`mount ..` made `<data dir>/..` one) must
    /// not delete them. A host dir that cannot be located refuses too: what
    /// the state dir holds is then not known.
    pub fn check_deletable(
        &self,
        name: &str,
        dirs: &[(&str, std::io::Result<PathBuf>)],
    ) -> Result<()> {
        let Some(entry) = self.entry(name) else {
            return Ok(());
        };
        let dir = &entry.state_dir;
        if dir.as_os_str().is_empty() {
            return Ok(());
        }
        let mut guarded = vec![("the registry".to_string(), self.path.clone())];
        for (what, located) in dirs {
            match located {
                Ok(path) => guarded.push((format!("{what} {}", path.display()), path.clone())),
                Err(e) => bail!(
                    "cannot locate {what} ({e}); refusing to delete {name}'s state dir {} \
                     without knowing it does not hold it",
                    dir.display()
                ),
            }
        }
        for (other, e) in self.entries.iter().filter(|(other, _)| *other != name) {
            guarded.push((format!("the state dir of {other}"), e.state_dir.clone()));
            if !e.cache_dir.is_empty() {
                guarded.push((
                    format!("the cache dir of {other}"),
                    PathBuf::from(&e.cache_dir),
                ));
            }
        }
        for (what, path) in guarded {
            if !path.as_os_str().is_empty() && contains(dir, &path) {
                bail!(
                    "{name}'s state dir {} holds {what}; refusing to delete it (remove {name}'s \
                     row from {} by hand if it is stale)",
                    dir.display(),
                    self.path.display()
                );
            }
        }
        Ok(())
    }

    /// Remove a whole filesystem's row (`export`, Step 6). A no-op,
    /// successful call if the name was already absent.
    pub fn remove(&mut self, name: &str) -> Result<()> {
        self.entries.remove(name);
        self.write()
    }

    /// Default state dirs under `dirs`' data dir (a manager's host).
    pub fn with_dirs(mut self, dirs: std::sync::Arc<dyn constellation_platform::Dirs>) -> Self {
        self.dirs = Some(dirs);
        self
    }

    /// Where this registry lives.
    pub fn file(&self) -> &Path {
        &self.path
    }
}

/// [`Registry::check_repoint`]'s refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repoint {
    pub name: String,
    pub current: String,
    pub requested: String,
}

impl std::fmt::Display for Repoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Repoint {
            name,
            current,
            requested,
        } = self;
        write!(
            f,
            "{name} already points at {current:?}, and its state dir is in use; pointing it at \
             {requested:?} would repoint that replica. Run `constellation export {name}` first \
             if you really want to re-point it."
        )
    }
}

impl std::error::Error for Repoint {}

/// A filesystem name: what `mount NAME`, `fs create NAME` and `fs.import`
/// register. One path component (its default state dir is
/// `<data dir>/<name>`, which `export` deletes, so `..` or `a/b` would put
/// it somewhere else), with no `:` (`NAME:/path` separates a name from a
/// path, so `a:b` could never be named) and no control characters.
pub fn validate_name(name: &str) -> Result<()> {
    let why = if name.is_empty() {
        "it is empty"
    } else if name == "." || name == ".." {
        "`.` and `..` are not names"
    } else if name.contains(['/', '\\']) {
        "it contains a path separator"
    } else if name.contains(':') {
        "it contains `:`, which separates a name from a path (`NAME:/path`)"
    } else if name.chars().any(char::is_control) {
        "it contains a control character"
    } else if name.len() > 255 {
        "it is longer than 255 bytes"
    } else {
        return Ok(());
    };
    bail!("invalid filesystem name {name:?}: {why}")
}

/// Whether `dir`, a name's state dir, is in use: populated (`meta.db`, a
/// replica with its `node_id`), or its `daemon.lock` held — a daemon still
/// bootstrapping has no `meta.db` yet.
pub fn state_dir_in_use(dir: &Path) -> bool {
    !dir.as_os_str().is_empty() && (dir.join("meta.db").exists() || daemon_lock_held(dir))
}

/// Whether `outer` is `inner` or one of its ancestors, however spelled
/// (resolved where they exist).
fn contains(outer: &Path, inner: &Path) -> bool {
    let resolve = |p: &Path| {
        std::fs::canonicalize(p)
            .or_else(|_| std::path::absolute(p))
            .unwrap_or_else(|_| p.to_path_buf())
    };
    resolve(inner).starts_with(resolve(outer))
}

/// Whether a process holds `dir`'s `daemon.lock` (the CLI's per-state-dir
/// daemon lock): probed by trying to take it.
pub fn daemon_lock_held(dir: &Path) -> bool {
    let lock = dir.join("daemon.lock");
    match std::fs::OpenOptions::new().write(true).open(&lock) {
        // Anything but getting the lock (released again as the guard
        // drops) counts as held.
        Ok(f) => !matches!(
            constellation_platform::native().file_lock.try_lock(f),
            Ok(Some(_))
        ),
        Err(_) => false,
    }
}

/// Whether `a` and `b` name one directory, however spelled (resolved where
/// they exist).
fn same_dir(a: &Path, b: &Path) -> bool {
    a == b
        || matches!(
            (std::fs::canonicalize(a), std::fs::canonicalize(b)),
            (Ok(x), Ok(y)) if x == y
        )
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
    fn an_explicit_state_dir_is_the_names_unless_it_moves_a_populated_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load_locked_at(registry_path(dir.path())).unwrap();
        let s3 = || Some("s3://bucket/prefix".to_string());
        // Registered (`fs create`), never mounted: the default is stored.
        let created = reg
            .merge_and_save(
                "myfs",
                FsOverrides {
                    s3: s3(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(!created.state_dir.as_os_str().is_empty());

        // `mount myfs --state-dir DIR`: DIR, made absolute, replaces it.
        let mine = dir.path().join("mine");
        let state_dir = Some(reg.explicit_state_dir("myfs", &mine).unwrap());
        let entry = reg
            .merge_and_save(
                "myfs",
                FsOverrides {
                    state_dir,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(entry.state_dir, mine);
        let relative = reg.explicit_state_dir("other", Path::new("rel")).unwrap();
        assert!(
            relative.is_absolute() && relative.ends_with("rel"),
            "{relative:?}"
        );
        // The same dir again (every mount of a script passes it) is fine.
        assert_eq!(reg.explicit_state_dir("myfs", &mine).unwrap(), mine);

        // Populated: identity is pinned to the name, as for `--s3`.
        std::fs::create_dir_all(mine.join("meta.db")).unwrap();
        // (another spelling of the same dir moves nothing)
        #[cfg(unix)]
        {
            let alias = dir.path().join("alias");
            std::os::unix::fs::symlink(&mine, &alias).unwrap();
            assert_eq!(reg.explicit_state_dir("myfs", &alias).unwrap(), mine);
        }
        let err = reg
            .explicit_state_dir("myfs", &dir.path().join("elsewhere"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("which is in use"), "{err}");
        assert_eq!(reg.entry("myfs").unwrap().state_dir, mine);
    }

    #[test]
    fn an_explicit_state_dir_is_new_or_empty_and_nobody_elses() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load_locked_at(registry_path(dir.path())).unwrap();
        let register = |reg: &mut Registry, name: &str, state_dir: &Path| {
            let state_dir = Some(reg.explicit_state_dir(name, state_dir).unwrap());
            reg.merge_and_save(
                name,
                FsOverrides {
                    s3: Some("s3://bucket/prefix".into()),
                    state_dir,
                    ..Default::default()
                },
            )
            .unwrap();
        };
        let refused = |reg: &Registry, name: &str, state_dir: &Path| {
            reg.explicit_state_dir(name, state_dir)
                .unwrap_err()
                .to_string()
        };

        // `cd ~; mount myfs ... --state-dir .`: `export myfs` would delete
        // $HOME. A populated dir is not this name's (another replica), and
        // a file is no dir at all.
        std::fs::write(dir.path().join("file"), b"x").unwrap();
        for not_empty in [dir.path().to_path_buf(), dir.path().join("file")] {
            let err = refused(&reg, "myfs", &not_empty);
            assert!(err.contains("is not an empty directory"), "{err}");
        }
        let replica = dir.path().join("replica");
        std::fs::create_dir_all(replica.join("meta.db")).unwrap();
        let err = refused(&reg, "myfs", &replica);
        assert!(err.contains("is not an empty directory"), "{err}");
        assert!(reg.entry("myfs").is_none());

        // New or empty: taken.
        let empty = dir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        register(&mut reg, "myfs", &empty);
        assert_eq!(reg.entry("myfs").unwrap().state_dir, empty);

        // Another name's state dir is never shared, empty or not.
        let err = refused(&reg, "second", &empty);
        assert!(err.contains("is already the state dir of myfs"), "{err}");

        // A daemon still bootstrapping holds daemon.lock in the stored dir
        // before it has a meta.db: the dir is in use, a second daemon
        // elsewhere would be a second node_id for the name.
        let lock =
            constellation_platform::lock::open_lock_file(&empty.join("daemon.lock")).unwrap();
        let held = constellation_platform::native()
            .file_lock
            .try_lock(lock)
            .unwrap()
            .expect("lock taken");
        let err = refused(&reg, "myfs", &dir.path().join("other"));
        assert!(err.contains("which is in use"), "{err}");
        drop(held);
        register(&mut reg, "myfs", &dir.path().join("other"));
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
                    confine_links: false,
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
