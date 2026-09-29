//! The host's mount table: what is mounted where, unmounting (including
//! detaching a dead FUSE mount from userspace), and — on Linux — the FUSE
//! connection controls under `/sys/fs/fuse/connections` that end requests
//! a dead daemon can never answer (`crates/cli/src/daemon_lock.rs`,
//! `abort_stale_mounts`).
//!
//! [`MountOpts`] describes a FUSE mount for the privileged, fusermount3-free
//! `linux::fuse_mount_fd` (plan 31 §4, §6.11; plan 37's CSI node plugin).

use constellation_types::Rdev;
use std::io;
use std::path::{Path, PathBuf};

/// One mounted filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountEntry {
    pub mountpoint: PathBuf,
    /// The filesystem type (`fuse`, `fuse.<subtype>`, `ext4`, `nfs`, ...).
    pub fstype: String,
    /// What is mounted: a device, a server export, or a FUSE mount's
    /// `fsname`.
    pub source: String,
    /// The mounted superblock's device number, when the host reports one
    /// (Linux mountinfo's `major:minor`).
    pub device: Option<Rdev>,
}

impl MountEntry {
    /// The FUSE connection number of a FUSE mount (Linux: the minor of
    /// the anonymous device, major 0, that names
    /// `/sys/fs/fuse/connections/<n>`).
    pub fn fuse_connection(&self) -> Option<u32> {
        let fuse = self.fstype == "fuse" || self.fstype.starts_with("fuse.");
        match self.device {
            Some(dev) if fuse && dev.major == 0 => Some(dev.minor),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnmountMode {
    /// Fail while busy.
    Normal,
    /// Detach now and let the kernel finish when it is no longer busy
    /// (Linux `MNT_DETACH`, `fusermount3 -uz`): what drops a dead FUSE
    /// mount that answers `ENOTCONN`. macOS has no lazy detach; there it
    /// is a forced unmount, the nearest equivalent for a dead mount.
    Lazy,
    /// Force it even while busy (`MNT_FORCE`; privileged on Linux).
    Force,
}

pub trait MountTable: Send + Sync {
    /// Every mount visible to this process.
    fn list(&self) -> io::Result<Vec<MountEntry>>;

    /// Whether `path` is the mountpoint of some mount.
    fn is_mountpoint(&self, path: &Path) -> io::Result<bool> {
        // Resolve the parent only: looking the mountpoint itself up would
        // ask the mounted filesystem (a dead FUSE mount answers
        // `ENOTCONN`, a hung one never answers).
        let want = match (path.parent(), path.file_name()) {
            (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => {
                std::fs::canonicalize(parent)?.join(name)
            }
            _ => std::fs::canonicalize(path)?,
        };
        Ok(self.list()?.iter().any(|m| m.mountpoint == want))
    }

    /// Unmount `path`. On Linux this is for this user's own FUSE mounts
    /// (through the setuid `fusermount3`, as the pre-plan-31 code did);
    /// [`UnmountMode::Force`] needs privilege.
    fn unmount(&self, path: &Path, mode: UnmountMode) -> io::Result<()>;

    /// How many requests FUSE connection `connection` has sent that the
    /// daemon has not answered yet.
    fn fuse_waiting(&self, connection: u32) -> io::Result<u64>;

    /// Abort FUSE connection `connection`: every request waiting on it
    /// fails with `ECONNABORTED`, and every later one with `ENOTCONN`. The
    /// connection files belong to the user who mounted, so the daemon's
    /// own user may.
    fn abort_fuse(&self, connection: u32) -> io::Result<()>;
}

/// A FUSE mount, for `linux::fuse_mount_fd`. The kernel options it turns
/// into are `fd=<n>,rootmode=<type bits of the mountpoint>,user_id=,
/// group_id=` plus the ones below; the mount flags default to
/// `MS_NOSUID|MS_NODEV`, as fusermount3's do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountOpts {
    /// The mount's source as `mountinfo` shows it (fuser's `FSName`).
    pub fsname: String,
    /// `fuse.<subtype>` as the filesystem type when set.
    pub subtype: Option<String>,
    /// The owner the kernel admits by default (`user_id=`/`group_id=`):
    /// `None` is this process's effective ids. A CSI node plugin mounting
    /// for a pod's engine passes that engine's ids.
    pub user_id: Option<u32>,
    pub group_id: Option<u32>,
    /// Admit every user, not only `user_id` (and root).
    pub allow_other: bool,
    /// Let the kernel check permissions from the reported modes.
    pub default_permissions: bool,
    pub read_only: bool,
    /// Keep `MS_NOSUID` (default on).
    pub nosuid: bool,
    /// Keep `MS_NODEV` (default on).
    pub nodev: bool,
    /// The largest read the kernel sends (`max_read=`); `None` leaves the
    /// kernel's default.
    pub max_read: Option<u32>,
}

impl MountOpts {
    /// Defaults for a mount named `fsname`: owner-only,
    /// `default_permissions`, read-write, nosuid, nodev.
    pub fn new(fsname: impl Into<String>) -> MountOpts {
        MountOpts {
            fsname: fsname.into(),
            subtype: None,
            user_id: None,
            group_id: None,
            allow_other: false,
            default_permissions: true,
            read_only: false,
            nosuid: true,
            nodev: true,
            max_read: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_fuse_mount_on_an_anonymous_device_has_a_connection() {
        let entry = |fstype: &str, major, minor| MountEntry {
            mountpoint: PathBuf::from("/m"),
            fstype: fstype.into(),
            source: "s".into(),
            device: Some(Rdev::new(major, minor)),
        };
        assert_eq!(entry("fuse", 0, 48).fuse_connection(), Some(48));
        assert_eq!(entry("fuse.sshfs", 0, 49).fuse_connection(), Some(49));
        assert_eq!(entry("ext4", 0, 50).fuse_connection(), None);
        assert_eq!(entry("fuseblk", 8, 1).fuse_connection(), None);
        let mut none = entry("fuse", 0, 1);
        none.device = None;
        assert_eq!(none.fuse_connection(), None);
    }
}
