//! Where Constellation keeps its files on this host.
//!
//! Linux and macOS use the same XDG-style layout (plan 34 settled decision
//! 7: `~/Library/Application Support/…` would push the control socket past
//! macOS's 104-byte `sun_path` for any username longer than a few
//! characters, and would split the documented location from the code):
//!
//! - config: `$XDG_CONFIG_HOME/constellation`, else
//!   `~/.config/constellation` — the filesystem registry, the E2E pins,
//!   the host's P2P `node.key`.
//! - data: `$XDG_DATA_HOME/constellation`, else
//!   `~/.local/share/constellation` — one state dir per filesystem under
//!   it (`<data>/<name>` for a registered name, `<data>/<uuid>` otherwise).
//! - runtime: `$XDG_RUNTIME_DIR/constellation` — where plan 31 C5 moves
//!   the control socket. See [`runtime_dir_from`] for the fallback and the
//!   `sun_path` bound.
//!
//! An XDG variable that is set but empty counts as unset, as the XDG base
//! directory spec says.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

/// The directory Constellation's own files go in, under each base.
pub const APP_DIR: &str = "constellation";

/// The smallest `sun_path` among the targets: macOS and the BSDs have 104
/// bytes, Linux 108. A socket path must fit with its NUL terminator.
pub const SUN_PATH_MAX: usize = 104;

/// How many bytes of `sun_path` a runtime dir leaves for the socket's own
/// file name (`control.sock` today; C5 may add per-view sockets).
pub const RUNTIME_NAME_BUDGET: usize = 32;

pub trait Dirs: Send + Sync {
    /// Configuration shared by every filesystem on this host.
    fn config_dir(&self) -> io::Result<PathBuf>;

    /// The base under which each filesystem's state dir lives.
    fn data_dir(&self) -> io::Result<PathBuf>;

    /// The default state dir for `key` (a registered name, or a
    /// filesystem uuid): `<data_dir>/<key>`.
    fn state_dir(&self, key: &str) -> io::Result<PathBuf> {
        Ok(self.data_dir()?.join(key))
    }

    /// Per-user runtime files (sockets): short enough that any name of up
    /// to [`RUNTIME_NAME_BUDGET`] bytes inside it fits in `sun_path`. Not
    /// created here.
    fn runtime_dir(&self) -> io::Result<PathBuf>;
}

/// A set, non-empty environment variable.
#[cfg_attr(not(any(target_os = "linux", target_os = "macos")), allow(dead_code))]
pub(crate) fn env_nonempty(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|v| !v.is_empty())
}

/// `$<xdg>/constellation`, else `$HOME/<home_suffix>/constellation`, from
/// the variables' values (pure, for tests). Errors (`NotFound`) with
/// neither, as the pre-plan-31 code did ("HOME is not set").
pub fn xdg_dir_from(
    xdg: Option<OsString>,
    home: Option<OsString>,
    home_suffix: &str,
) -> io::Result<PathBuf> {
    let base = match (xdg.filter(|v| !v.is_empty()), home) {
        (Some(xdg), _) => PathBuf::from(xdg),
        (None, Some(home)) if !home.is_empty() => PathBuf::from(home).join(home_suffix),
        _ => return Err(io::Error::new(io::ErrorKind::NotFound, "HOME is not set")),
    };
    Ok(base.join(APP_DIR))
}

/// Whether `path` fits in `sun_path` (with its NUL terminator).
pub fn fits_sun_path(path: &Path) -> bool {
    path.as_os_str().len() < SUN_PATH_MAX
}

/// The runtime dir from its candidates (pure, for tests): the first of
/// `$XDG_RUNTIME_DIR/constellation` and `<tmp>/constellation` that leaves
/// [`RUNTIME_NAME_BUDGET`] bytes of `sun_path`, else
/// `/tmp/constellation-<uid>` (always short). `tmp` is macOS's per-user
/// `$TMPDIR`; Linux passes `None` (its `/tmp` is shared, hence the uid).
pub fn runtime_dir_from(xdg_runtime: Option<OsString>, tmp: Option<OsString>, uid: u32) -> PathBuf {
    let roomy = |dir: &Path| dir.as_os_str().len() + 1 + RUNTIME_NAME_BUDGET < SUN_PATH_MAX;
    for base in [xdg_runtime, tmp].into_iter().flatten() {
        if base.is_empty() {
            continue;
        }
        let dir = PathBuf::from(base).join(APP_DIR);
        if roomy(&dir) {
            return dir;
        }
    }
    PathBuf::from(format!("/tmp/{APP_DIR}-{uid}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xdg_wins_then_home_then_an_error() {
        let os = |s: &str| Some(OsString::from(s));
        assert_eq!(
            xdg_dir_from(os("/x/cfg"), os("/home/u"), ".config").unwrap(),
            PathBuf::from("/x/cfg/constellation")
        );
        assert_eq!(
            xdg_dir_from(None, os("/home/u"), ".config").unwrap(),
            PathBuf::from("/home/u/.config/constellation")
        );
        // Set-but-empty is unset (XDG base directory spec).
        assert_eq!(
            xdg_dir_from(os(""), os("/home/u"), ".local/share").unwrap(),
            PathBuf::from("/home/u/.local/share/constellation")
        );
        let err = xdg_dir_from(None, None, ".config").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(err.to_string(), "HOME is not set");
    }

    #[test]
    fn runtime_dir_leaves_room_for_a_socket_name() {
        let os = |s: &str| Some(OsString::from(s));
        assert_eq!(
            runtime_dir_from(os("/run/user/1000"), None, 1000),
            PathBuf::from("/run/user/1000/constellation")
        );
        // A macOS-shaped $TMPDIR is used when XDG_RUNTIME_DIR is absent.
        let tmp = "/var/folders/zz/zyxvpxvq6csfxvn_n0000000000000/T/";
        assert_eq!(
            runtime_dir_from(None, os(tmp), 501),
            PathBuf::from(tmp).join("constellation")
        );
        // Too long to leave room for a name: the short fallback.
        let long = format!("/{}", "d".repeat(80));
        let got = runtime_dir_from(os(&long), os(""), 1000);
        assert_eq!(got, PathBuf::from("/tmp/constellation-1000"));
        assert!(fits_sun_path(&got.join("x".repeat(RUNTIME_NAME_BUDGET))));
        assert!(!fits_sun_path(&PathBuf::from("a".repeat(SUN_PATH_MAX))));
    }
}
