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
//! daemon; C5b decides how additional daemons are named). It is restricted
//! to `[A-Za-z0-9._-]` so it cannot climb out of the directory.

use constellation_platform::dirs::{fits_sun_path, Dirs, SUN_PATH_MAX};
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
