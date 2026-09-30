//! Where the control socket lives (plan 31 §9.6).
//!
//! The old socket was `<state_dir>/control.sock`: inside a directory whose
//! permissions were whatever the user's umask made them, and unrelated to
//! who may *talk* to the daemon. The new one lives in the per-user runtime
//! directory ([`Dirs::runtime_dir`]: `$XDG_RUNTIME_DIR/constellation` on
//! Linux, `$TMPDIR/constellation` on macOS, with a short `/tmp` fallback),
//! which this module creates **0700** and refuses to use if someone else
//! owns it. Even before peer-credential authorization looks at a connecting
//! process, only the owner can reach the socket.
//!
//! `runtime_dir()` already ends in `constellation`, so the socket is
//! `<runtime_dir>/<instance>.sock` — not `<runtime_dir>/constellation/…`,
//! which would repeat the segment and spend `sun_path` bytes twice.
//!
//! `sun_path` holds 104 bytes on macOS and the BSDs (108 on Linux) *including
//! the NUL*; a longer path makes `bind(2)` fail with `ENAMETOOLONG` or, worse
//! on some libcs, silently truncates. [`socket_path_for`] checks the whole
//! path against the smaller limit up front so a daemon fails at startup with
//! a message that names the path, not later with a bare errno.
//!
//! Note the consequence for multi-user setups: with a 0700 directory only the
//! daemon's owner can reach the socket at all, so an allowlist grant for
//! another uid or group is moot until the deployment widens the directory
//! and the socket (`UnixSocketListener::bind_with_mode`). The default is the
//! safe one; widening is a conscious act.
//!
//! The instance name is the per-daemon part (`control` for the default
//! daemon). It is restricted to `[A-Za-z0-9._-]` so it cannot climb out of
//! the directory.
//!
//! ## One daemon per state dir, found from the state dir
//!
//! A host runs one daemon per state dir (plan 21), several at once, so each
//! needs its own socket: [`instance_for_state_dir`] names it after the state
//! dir — a readable prefix of its last component and 16 hex digits of the
//! BLAKE3 of its canonical path (`myfs-3f2a9c1b0d4e5f60.sock`), which fits
//! the runtime dir's [`RUNTIME_NAME_BUDGET`] by construction.
//!
//! Clients never re-derive it. The runtime dir depends on the environment
//! (`$XDG_RUNTIME_DIR`, `$TMPDIR`), and a CLI or harness run with another
//! environment than the daemon's would compute another path. Instead the
//! daemon records the socket's path in its state dir ([`LOCATOR_FILE`],
//! `control.path`, written atomically once the socket is bound), and
//! [`locate_socket`] reads it back: the state dir is what every caller
//! already knows. A locator whose socket nobody listens on (a crashed
//! daemon) reads as "not running" at connect time, exactly as a stale
//! `control.sock` used to; [`forget_socket`] removes both on a clean exit
//! and on a takeover.

use constellation_platform::dirs::{fits_sun_path, Dirs, RUNTIME_NAME_BUDGET, SUN_PATH_MAX};
use std::path::{Path, PathBuf};

/// Appended to the instance name.
pub const SOCKET_SUFFIX: &str = ".sock";

/// The instance name [`default_socket_path`] uses.
pub const DEFAULT_INSTANCE: &str = "control";

#[derive(Debug, thiserror::Error)]
pub enum SocketPathError {
    #[error("invalid socket instance name {0:?} (use letters, digits, '.', '_' and '-')")]
    InvalidName(String),
    #[error("socket path {path} is {len} bytes; the limit is {} (sun_path)", SUN_PATH_MAX - 1)]
    TooLong { path: String, len: usize },
    #[error("locating the runtime directory: {0}")]
    RuntimeDir(#[source] std::io::Error),
    #[error("preparing {path}: {source}")]
    Dir {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "{path} is owned by another user (uid {owner}); refusing to put the control socket there"
    )]
    NotOwned { path: String, owner: u32 },
}

/// `<runtime_dir>/<instance>.sock`, checked against `sun_path`. Pure: nothing
/// is created.
pub fn socket_path_for(runtime_dir: &Path, instance: &str) -> Result<PathBuf, SocketPathError> {
    let valid = !instance.is_empty()
        && instance != "."
        && instance != ".."
        && instance
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if !valid {
        return Err(SocketPathError::InvalidName(instance.to_string()));
    }
    let path = runtime_dir.join(format!("{instance}{SOCKET_SUFFIX}"));
    if !fits_sun_path(&path) {
        return Err(SocketPathError::TooLong {
            len: path.as_os_str().len(),
            path: path.display().to_string(),
        });
    }
    Ok(path)
}

/// The default daemon's socket: `<runtime_dir>/control.sock`, with the
/// directory created 0700.
pub fn default_socket_path(dirs: &dyn Dirs) -> Result<PathBuf, SocketPathError> {
    let dir = dirs.runtime_dir().map_err(SocketPathError::RuntimeDir)?;
    let path = socket_path_for(&dir, DEFAULT_INSTANCE)?;
    ensure_socket_dir(&dir)?;
    Ok(path)
}

/// The file in a state dir naming its daemon's control socket.
pub const LOCATOR_FILE: &str = "control.path";

/// The socket instance name of the daemon serving `state_dir` (see the
/// module docs): `<prefix>-<16 hex>`, at most 25 bytes.
pub fn instance_for_state_dir(state_dir: &Path) -> String {
    let canonical = std::fs::canonicalize(state_dir).unwrap_or_else(|_| state_dir.to_path_buf());
    let hash = blake3::hash(canonical.as_os_str().as_encoded_bytes());
    let prefix: String = canonical
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        .take(8)
        .collect();
    let prefix = if prefix.is_empty() {
        "d".into()
    } else {
        prefix
    };
    let instance = format!("{prefix}-{}", &hash.to_hex()[..16]);
    debug_assert!(instance.len() + SOCKET_SUFFIX.len() <= RUNTIME_NAME_BUDGET);
    instance
}

/// The socket the daemon serving `state_dir` binds:
/// `<runtime_dir>/<instance_for_state_dir>.sock`, with the directory
/// created 0700.
pub fn socket_path_for_state_dir(
    dirs: &dyn Dirs,
    state_dir: &Path,
) -> Result<PathBuf, SocketPathError> {
    let dir = dirs.runtime_dir().map_err(SocketPathError::RuntimeDir)?;
    let path = socket_path_for(&dir, &instance_for_state_dir(state_dir))?;
    ensure_socket_dir(&dir)?;
    Ok(path)
}

/// Record `socket` as `state_dir`'s control socket ([`LOCATOR_FILE`]),
/// atomically: a reader sees the old path or the new one, never half.
pub fn record_socket(state_dir: &Path, socket: &Path) -> std::io::Result<()> {
    let tmp = state_dir.join(format!("{LOCATOR_FILE}.tmp-{}", std::process::id()));
    std::fs::write(&tmp, socket.as_os_str().as_encoded_bytes())?;
    std::fs::rename(&tmp, state_dir.join(LOCATOR_FILE))
}

/// The control socket recorded in `state_dir`, if any. `None` means no
/// daemon has served this state dir since the last clean exit.
pub fn locate_socket(state_dir: &Path) -> Option<PathBuf> {
    let raw = std::fs::read(state_dir.join(LOCATOR_FILE)).ok()?;
    let text = String::from_utf8(raw).ok()?;
    let text = text.trim_end_matches('\n');
    (!text.is_empty()).then(|| PathBuf::from(text))
}

/// Remove `state_dir`'s recorded socket (the socket file itself, then the
/// locator). Best effort: a daemon's clean exit, or a takeover of a dead
/// daemon's state dir.
pub fn forget_socket(state_dir: &Path) {
    if let Some(socket) = locate_socket(state_dir) {
        let _ = std::fs::remove_file(socket);
    }
    let _ = std::fs::remove_file(state_dir.join(LOCATOR_FILE));
}

/// Create `dir` (and missing parents) with mode 0700 and verify it is ours
/// and not open to group/other. An existing directory with looser
/// permissions that we own is tightened; one owned by someone else is an
/// error.
pub fn ensure_socket_dir(dir: &Path) -> Result<(), SocketPathError> {
    let err = |source| SocketPathError::Dir {
        path: dir.display().to_string(),
        source,
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(err)?;
        // `lstat`, not `stat`: in a shared parent (the `/tmp/constellation-
        // <uid>` fallback) another user can plant a symlink under our name
        // pointing at a directory we own, pass the ownership check, and
        // re-aim it at their own directory before we bind — putting our
        // socket (and the credentials clients send to it) in their hands.
        // A real directory owned by us cannot be swapped out from under us.
        let meta = std::fs::symlink_metadata(dir).map_err(err)?;
        if !meta.file_type().is_dir() {
            return Err(err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "exists and is not a directory (symlinks are refused)",
            )));
        }
        // SAFETY: geteuid has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        if meta.uid() != me {
            return Err(SocketPathError::NotOwned {
                path: dir.display().to_string(),
                owner: meta.uid(),
            });
        }
        if meta.permissions().mode() & 0o077 != 0 {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(err)?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(dir).map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedDirs(PathBuf);

    impl Dirs for FixedDirs {
        fn config_dir(&self) -> std::io::Result<PathBuf> {
            Ok(self.0.join("config"))
        }
        fn data_dir(&self) -> std::io::Result<PathBuf> {
            Ok(self.0.join("data"))
        }
        fn runtime_dir(&self) -> std::io::Result<PathBuf> {
            Ok(self.0.join("run/constellation"))
        }
    }

    #[test]
    fn path_is_dir_plus_instance_plus_suffix() {
        let p = socket_path_for(Path::new("/run/user/1000/constellation"), "control").unwrap();
        assert_eq!(
            p,
            PathBuf::from("/run/user/1000/constellation/control.sock")
        );
        let p = socket_path_for(Path::new("/r"), "fs-1.a_b").unwrap();
        assert_eq!(p, PathBuf::from("/r/fs-1.a_b.sock"));
    }

    #[test]
    fn hostile_or_empty_instance_names_are_rejected() {
        for bad in ["", ".", "..", "a/b", "../x", "a b", "a\0b", "é"] {
            assert!(
                matches!(
                    socket_path_for(Path::new("/r"), bad),
                    Err(SocketPathError::InvalidName(_))
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn overlong_paths_fail_with_the_path_in_the_message() {
        let dir = PathBuf::from(format!("/{}", "d".repeat(90)));
        let err = socket_path_for(&dir, "control").unwrap_err();
        assert!(matches!(err, SocketPathError::TooLong { .. }));
        assert!(err.to_string().contains("103"), "{err}");
        // Exactly at the limit is fine: 103 bytes + NUL = 104.
        let fill = 103 - "/control.sock".len();
        let dir = PathBuf::from(format!("/{}", "d".repeat(fill - 1)));
        let ok = socket_path_for(&dir, "control").unwrap();
        assert_eq!(ok.as_os_str().len(), 103);
    }

    #[cfg(unix)]
    #[test]
    fn default_path_creates_a_private_directory() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dirs = FixedDirs(tmp.path().to_path_buf());
        let path = default_socket_path(&dirs).unwrap();
        assert_eq!(path, tmp.path().join("run/constellation/control.sock"));
        let mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
        // A looser directory we own is tightened, not trusted.
        std::fs::set_permissions(
            path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        default_socket_path(&dirs).unwrap();
        let mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn state_dirs_get_distinct_short_stable_instances() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("my filesystem!");
        let b = tmp.path().join("other");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let ia = instance_for_state_dir(&a);
        assert_eq!(ia, instance_for_state_dir(&a), "stable");
        assert_ne!(ia, instance_for_state_dir(&b), "distinct");
        assert!(ia.starts_with("myfilesy-"), "{ia}");
        assert!(ia.len() + SOCKET_SUFFIX.len() <= RUNTIME_NAME_BUDGET);
        // The same directory through a different spelling is the same daemon.
        assert_eq!(
            instance_for_state_dir(&tmp.path().join("other/../other")),
            instance_for_state_dir(&b)
        );
        socket_path_for(Path::new("/r"), &ia).unwrap();
        let dirs = FixedDirs(tmp.path().to_path_buf());
        let sock = socket_path_for_state_dir(&dirs, &a).unwrap();
        assert!(sock.starts_with(tmp.path().join("run/constellation")));
    }

    #[test]
    fn the_locator_round_trips_and_is_forgotten() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(locate_socket(tmp.path()), None);
        let sock = tmp.path().join("s.sock");
        std::fs::write(&sock, b"").unwrap();
        record_socket(tmp.path(), &sock).unwrap();
        assert_eq!(locate_socket(tmp.path()), Some(sock.clone()));
        forget_socket(tmp.path());
        assert_eq!(locate_socket(tmp.path()), None);
        assert!(!sock.exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_or_non_directory_socket_dir_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        // A directory we own, reachable through a symlink someone planted.
        let real = tmp.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = tmp.path().join("constellation");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            ensure_socket_dir(&link),
            Err(SocketPathError::Dir { .. })
        ));
        // A plain file under the directory's name.
        let file = tmp.path().join("file");
        std::fs::write(&file, b"x").unwrap();
        assert!(ensure_socket_dir(&file).is_err());
        // The real directory itself is fine.
        ensure_socket_dir(&real).unwrap();
    }
}
