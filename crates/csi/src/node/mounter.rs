//! [`Mounter`]: every `mount(2)`-shaped thing the node plugin does, behind
//! one seam — the FUSE staging mount (`fuse_mount_fd`), the per-pod bind
//! mount, unmounting, and telling a live mount from a dead one.
//!
//! [`LinuxMounter`] is the real one. It needs `CAP_SYS_ADMIN` in the host's
//! mount namespace, which only the privileged node-plugin container has
//! (plan 37 settled decision 10); its mounts reach kubelet and the
//! workload pods through the `Bidirectional` propagation of the kubelet
//! and hostRoot volumes. [`FakeMounter`] records mounts in memory (and
//! creates the directories for real, since kubelet and `csi-sanity` look
//! for them): the unit tests and `--in-memory-backend` run on it.
//!
//! **Dead or alive.** A FUSE mount whose server is gone — an engine pod that
//! crashed, taking the only descriptor of the connection with it — stays in
//! the mount table and answers every access `ENOTCONN` (an aborted
//! connection: `ECONNABORTED` for requests that were in flight). That is
//! [`MountState::Dead`], the signal `NodePublishVolume` restages on (plan
//! 37 settled decision 12). A mount that is in the table and answers `stat`
//! is [`MountState::Alive`]; its device number tells a bind of the staging
//! mount from something else mounted at a target.

use std::collections::BTreeMap;
use std::io;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// What is at a path, as far as mounts go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountState {
    /// Nothing is mounted at the path (it may not even exist).
    NotMounted,
    /// Mounted and answering; `dev` is what `stat` reports for it (a bind
    /// mount shares its source's), `read_only` whether it is mounted `ro`.
    Alive { dev: u64, read_only: bool },
    /// Mounted, but whatever served it is gone (`ENOTCONN`).
    Dead,
}

/// How the staging mount is made. The kernel half of the options only:
/// the session half (`allow_other` in fuser, threads) travels with
/// `view.mount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseMountOptions {
    /// The mount's source as mount tables show it.
    pub fs_name: String,
    /// The owner the kernel records (`user_id=`/`group_id=`): the engine
    /// pod's uid, which serves the mount.
    pub uid: u32,
    pub gid: u32,
}

pub trait Mounter: Send + Sync {
    /// Mount a FUSE filesystem at `target` (`fuse_mount_fd`: `allow_other`,
    /// `default_permissions`, nosuid, nodev) and return the `/dev/fuse`
    /// descriptor that serves it, `FUSE_INIT` pending.
    fn fuse_mount(&self, target: &Path, opts: &FuseMountOptions) -> io::Result<OwnedFd>;
    /// Bind `source` at `target` (created when missing), read-only when
    /// asked.
    fn bind_mount(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()>;
    /// Unmount `target`; `Ok(false)` when nothing was mounted there. A busy
    /// mount is detached lazily (`MNT_DETACH`): every caller unmounts only
    /// what kubelet has stopped using.
    fn unmount(&self, target: &Path) -> io::Result<bool>;
    /// What is mounted at `path`. May block on a wedged FUSE server: call
    /// it with a bound ([`super::NodeService`] does).
    fn state(&self, path: &Path) -> MountState;
}

/// The real [`Mounter`] (module docs).
#[derive(Debug, Default)]
pub struct LinuxMounter;

impl LinuxMounter {
    /// Whether `path` is a mount point in this process's mount table, and
    /// if so whether its topmost mount is read-only (its per-mount `ro`).
    /// Compared by the path as given and by its canonical parent joined with
    /// its name — never by canonicalizing the path itself, which would
    /// `stat` into a mount that may be dead.
    fn mount_entry(path: &Path) -> io::Result<Option<bool>> {
        let table = std::fs::read_to_string("/proc/self/mountinfo")?;
        let wanted = normalized(path);
        let canonical = match (path.parent(), path.file_name()) {
            (Some(parent), Some(name)) => std::fs::canonicalize(parent).ok().map(|p| p.join(name)),
            _ => None,
        };
        // Later lines are mounted over earlier ones: the last one wins.
        Ok(table
            .lines()
            .rev()
            .find(|line| {
                let Some(mountpoint) = line.split(' ').nth(4) else {
                    return false;
                };
                let mountpoint = PathBuf::from(unescape_mountinfo(mountpoint));
                mountpoint == wanted || canonical.as_deref() == Some(mountpoint.as_path())
            })
            .map(|line| {
                line.split(' ')
                    .nth(5)
                    .is_some_and(|opts| opts.split(',').any(|o| o == "ro"))
            }))
    }
}

/// `path` without a trailing `/` (kubelet's paths have none; a test's may).
fn normalized(path: &Path) -> PathBuf {
    path.components().collect()
}

/// `/proc/self/mountinfo` escapes space, tab, newline and backslash as
/// `\ooo` octal.
fn unescape_mountinfo(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 4 <= bytes.len() {
            let octal = &bytes[i + 1..i + 4];
            if octal.iter().all(|b| (b'0'..=b'7').contains(b)) {
                let octal = std::str::from_utf8(octal).expect("octal digits are ASCII");
                if let Ok(b) = u8::from_str_radix(octal, 8) {
                    out.push(b);
                    i += 4;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn cstring(path: &Path) -> io::Result<std::ffi::CString> {
    std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a path contains a NUL byte"))
}

impl Mounter for LinuxMounter {
    fn fuse_mount(&self, target: &Path, opts: &FuseMountOptions) -> io::Result<OwnedFd> {
        let mut kernel = constellation_platform::MountOpts::new(opts.fs_name.clone());
        // `fuse.constellation`, so a node's mount table says what it is.
        kernel.subtype = Some("constellation".into());
        kernel.user_id = Some(opts.uid);
        kernel.group_id = Some(opts.gid);
        // Every pod publishing the volume reaches it, whatever its uid;
        // permissions are the engine's (and `default_permissions`').
        kernel.allow_other = true;
        constellation_platform::linux::fuse_mount_fd(target, &kernel)
    }

    fn bind_mount(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
        std::fs::create_dir_all(target)?;
        let (src, tgt) = (cstring(source)?, cstring(target)?);
        // SAFETY: valid NUL-terminated paths that outlive the calls; null
        // filesystem type and data, as a bind mount takes.
        let rc = unsafe {
            libc::mount(
                src.as_ptr(),
                tgt.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND,
                std::ptr::null(),
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        if read_only {
            // A bind mount takes `MS_RDONLY` only on a remount of itself.
            // SAFETY: as above.
            let rc = unsafe {
                libc::mount(
                    std::ptr::null(),
                    tgt.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
                    std::ptr::null(),
                )
            };
            if rc != 0 {
                let e = io::Error::last_os_error();
                // Never leave a writable bind where a read-only one was asked for.
                let _ = self.unmount(target);
                return Err(e);
            }
        }
        Ok(())
    }

    fn unmount(&self, target: &Path) -> io::Result<bool> {
        if Self::mount_entry(target)?.is_none() {
            return Ok(false);
        }
        let tgt = cstring(target)?;
        // SAFETY: a valid NUL-terminated path.
        if unsafe { libc::umount2(tgt.as_ptr(), 0) } == 0 {
            return Ok(true);
        }
        let e = io::Error::last_os_error();
        match e.kind() {
            // Raced with another unmount, or not a mount after all.
            io::ErrorKind::InvalidInput | io::ErrorKind::NotFound => Ok(false),
            io::ErrorKind::ResourceBusy => {
                tracing::warn!(target = %target.display(), "busy; detaching the mount lazily");
                // SAFETY: as above.
                if unsafe { libc::umount2(tgt.as_ptr(), libc::MNT_DETACH) } == 0 {
                    Ok(true)
                } else {
                    Err(io::Error::last_os_error())
                }
            }
            _ => Err(e),
        }
    }

    fn state(&self, path: &Path) -> MountState {
        let read_only = match Self::mount_entry(path) {
            Ok(Some(read_only)) => read_only,
            Ok(None) => return MountState::NotMounted,
            Err(e) => {
                tracing::warn!(error = %e, "reading the mount table");
                return MountState::NotMounted;
            }
        };
        use std::os::unix::fs::MetadataExt;
        match std::fs::metadata(path) {
            Ok(md) => MountState::Alive {
                dev: md.dev(),
                read_only,
            },
            Err(e) => {
                tracing::info!(path = %path.display(), error = %e, "a dead mount");
                MountState::Dead
            }
        }
    }
}

/// One mount [`FakeMounter`] holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeMount {
    pub dev: u64,
    pub dead: bool,
    pub read_only: bool,
    /// A bind's source; `None` for a FUSE mount.
    pub source: Option<PathBuf>,
}

/// An in-memory [`Mounter`] (module docs): FUSE mounts get a fresh device
/// number and hand out a `/dev/null` descriptor; binds share their
/// source's device. [`Self::kill`] plays an engine pod crashing.
#[derive(Debug, Default)]
pub struct FakeMounter {
    mounts: Mutex<BTreeMap<PathBuf, FakeMount>>,
    next_dev: Mutex<u64>,
}

impl FakeMounter {
    /// The FUSE connection behind `path` dies: it and every bind of it
    /// answer `ENOTCONN` from now on.
    pub fn kill(&self, path: &Path) {
        let mut mounts = self.mounts.lock().unwrap();
        let Some(dev) = mounts.get(&normalized(path)).map(|m| m.dev) else {
            return;
        };
        for m in mounts.values_mut().filter(|m| m.dev == dev) {
            m.dead = true;
        }
    }

    /// What is mounted at `path`, if anything.
    pub fn mount_at(&self, path: &Path) -> Option<FakeMount> {
        self.mounts.lock().unwrap().get(&normalized(path)).cloned()
    }

    /// Every mounted path.
    pub fn mounted(&self) -> Vec<PathBuf> {
        self.mounts.lock().unwrap().keys().cloned().collect()
    }
}

impl Mounter for FakeMounter {
    fn fuse_mount(&self, target: &Path, _opts: &FuseMountOptions) -> io::Result<OwnedFd> {
        std::fs::create_dir_all(target)?;
        let mut mounts = self.mounts.lock().unwrap();
        let key = normalized(target);
        if mounts.contains_key(&key) {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!("{} is already a mount point", key.display()),
            ));
        }
        let dev = {
            let mut next = self.next_dev.lock().unwrap();
            *next += 1;
            1000 + *next
        };
        mounts.insert(
            key,
            FakeMount {
                dev,
                dead: false,
                read_only: false,
                source: None,
            },
        );
        Ok(std::fs::File::open("/dev/null")?.into())
    }

    fn bind_mount(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
        std::fs::create_dir_all(target)?;
        let mut mounts = self.mounts.lock().unwrap();
        let Some(src) = mounts.get(&normalized(source)).cloned() else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("nothing is mounted at {}", source.display()),
            ));
        };
        mounts.insert(
            normalized(target),
            FakeMount {
                dev: src.dev,
                dead: src.dead,
                read_only,
                source: Some(normalized(source)),
            },
        );
        Ok(())
    }

    fn unmount(&self, target: &Path) -> io::Result<bool> {
        Ok(self
            .mounts
            .lock()
            .unwrap()
            .remove(&normalized(target))
            .is_some())
    }

    fn state(&self, path: &Path) -> MountState {
        match self.mounts.lock().unwrap().get(&normalized(path)) {
            None => MountState::NotMounted,
            Some(m) if m.dead => MountState::Dead,
            Some(m) => MountState::Alive {
                dev: m.dev,
                read_only: m.read_only,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mountinfo_escapes_are_undone() {
        assert_eq!(unescape_mountinfo("/a\\040b"), "/a b");
        assert_eq!(unescape_mountinfo("/x\\134y"), "/x\\y");
        assert_eq!(unescape_mountinfo("/plain"), "/plain");
        assert_eq!(unescape_mountinfo("/odd\\04"), "/odd\\04");
    }

    #[test]
    fn the_root_and_proc_are_mountpoints_and_a_temp_dir_is_not() {
        assert!(LinuxMounter::mount_entry(Path::new("/")).unwrap().is_some());
        assert!(LinuxMounter::mount_entry(Path::new("/proc/"))
            .unwrap()
            .is_some());
        let dir = tempfile::tempdir().unwrap();
        assert!(LinuxMounter::mount_entry(dir.path()).unwrap().is_none());
        assert_eq!(LinuxMounter.state(dir.path()), MountState::NotMounted);
        assert!(!LinuxMounter.unmount(dir.path()).unwrap());
        assert!(matches!(
            LinuxMounter.state(Path::new("/proc")),
            MountState::Alive { .. }
        ));
    }

    #[test]
    fn the_fake_shares_a_bind_sources_fate() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, target) = (dir.path().join("s"), dir.path().join("t/x"));
        let m = FakeMounter::default();
        let opts = FuseMountOptions {
            fs_name: "c".into(),
            uid: 1,
            gid: 1,
        };
        drop(m.fuse_mount(&staging, &opts).unwrap());
        assert!(m.fuse_mount(&staging, &opts).is_err(), "one mount per path");
        m.bind_mount(&staging, &target, true).unwrap();
        assert!(target.is_dir());
        let (MountState::Alive { dev: a, .. }, MountState::Alive { dev: b, .. }) =
            (m.state(&staging), m.state(&target))
        else {
            panic!("both alive");
        };
        assert_eq!(a, b);
        m.kill(&staging);
        assert_eq!(m.state(&target), MountState::Dead);
        assert!(m.unmount(&target).unwrap());
        assert!(!m.unmount(&target).unwrap());
        assert_eq!(m.state(&target), MountState::NotMounted);
    }
}
