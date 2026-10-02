//! The services of a host with no implementation yet: every question is
//! answered `io::ErrorKind::Unsupported` (or "unknown"), so the crate
//! compiles for every target and a port fills services in one at a time
//! (plans 35/36; FreeBSD whenever a FUSE port wants it). Lifecycle events
//! are the portable [`ManualLifecycle`].

// On the fully implemented hosts only the stub modules' tests reach this.
#![cfg_attr(any(target_os = "linux", target_os = "macos"), allow(dead_code))]

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::daemon::{Daemon, Detached};
use crate::dirs::Dirs;
use crate::fs::FsPrimitives;
use crate::lifecycle::ManualLifecycle;
use crate::lock::{FileLock, LockGuard};
use crate::mounts::{MountEntry, MountTable, UnmountMode};
use crate::process::{Process, ProcessFacts, ThreadRef};
use crate::secrets::{Secret, SecretStore, Update};
use crate::{unsupported, HostServices};

pub(crate) struct Unsupported;

pub(crate) fn host_services() -> HostServices {
    let host = Arc::new(Unsupported);
    HostServices {
        dirs: host.clone(),
        process: host.clone(),
        daemon: host.clone(),
        file_lock: host.clone(),
        fs: host.clone(),
        secrets: host.clone(),
        lifecycle: Arc::new(ManualLifecycle::new()),
        mounts: host,
    }
}

pub(crate) fn file_lock() -> Arc<dyn FileLock> {
    Arc::new(Unsupported)
}

impl Dirs for Unsupported {
    fn config_dir(&self) -> io::Result<PathBuf> {
        Err(unsupported("a config dir"))
    }
    fn data_dir(&self) -> io::Result<PathBuf> {
        Err(unsupported("a data dir"))
    }
    fn runtime_dir(&self) -> io::Result<PathBuf> {
        Err(unsupported("a runtime dir"))
    }
}

impl Process for Unsupported {
    fn hostname(&self) -> io::Result<String> {
        Err(unsupported("the hostname"))
    }
    fn is_alive(&self, _pid: u32) -> bool {
        false
    }
    fn facts(&self, _pid: u32) -> io::Result<ProcessFacts> {
        Err(unsupported("process facts"))
    }
    fn supplementary_groups(&self, _pid: u32) -> io::Result<Vec<u32>> {
        Err(unsupported("supplementary groups"))
    }
    fn lineage(&self, _pid: u32) -> io::Result<crate::Lineage> {
        Err(unsupported("process lineage"))
    }
    /// No POSIX ids: files are owned by 0/0 (plan 35's `IdentityMap`
    /// decides what a Windows owner is).
    fn effective_ids(&self) -> (u32, u32) {
        (0, 0)
    }
    fn thread_count(&self) -> Option<u64> {
        None
    }
    fn memory_budget(&self) -> Option<u64> {
        None
    }
    fn current_thread(&self) -> ThreadRef {
        ThreadRef::unknown()
    }
    fn enable_backtraces(&self, _label: &'static str) -> io::Result<()> {
        Err(unsupported("thread backtraces"))
    }
    fn request_backtrace(&self, _thread: ThreadRef) -> io::Result<()> {
        Err(unsupported("thread backtraces"))
    }
}

impl Daemon for Unsupported {
    unsafe fn detach(&self, _log: &Path) -> io::Result<Detached> {
        Err(unsupported("daemonizing"))
    }
}

impl FileLock for Unsupported {
    fn lock(&self, _file: File) -> io::Result<LockGuard> {
        Err(unsupported("file locks"))
    }
    fn try_lock(&self, _file: File) -> io::Result<Option<LockGuard>> {
        Err(unsupported("file locks"))
    }
    fn holder_pid(&self, _path: &Path) -> Option<u32> {
        None
    }
}

impl FsPrimitives for Unsupported {
    fn punch_hole(&self, _file: &File, _offset: u64, _len: u64) -> io::Result<()> {
        Err(unsupported("hole punching"))
    }
    fn preallocate(&self, _file: &File, _offset: u64, _len: u64, _keep: bool) -> io::Result<()> {
        Err(unsupported("preallocation"))
    }
    fn full_fsync(&self, _file: &File) -> io::Result<()> {
        Err(unsupported("full fsync"))
    }
    fn drop_cache(&self, _file: &File, _offset: u64, _len: u64) -> io::Result<()> {
        Err(unsupported("dropping cached pages"))
    }
}

impl SecretStore for Unsupported {
    fn get(&self, _name: &str) -> io::Result<Option<Secret>> {
        Err(unsupported("a persistent secret store"))
    }
    fn put(&self, _name: &str, _value: &[u8]) -> io::Result<()> {
        Err(unsupported("a persistent secret store"))
    }
    fn delete(&self, _name: &str) -> io::Result<()> {
        Err(unsupported("a persistent secret store"))
    }
    fn update(&self, _name: &str, _f: Update<'_>) -> io::Result<()> {
        Err(unsupported("a persistent secret store"))
    }
    fn describe(&self, name: &str) -> String {
        format!("{name} (no secret store on this host)")
    }
}

impl MountTable for Unsupported {
    fn list(&self) -> io::Result<Vec<MountEntry>> {
        Err(unsupported("the mount table"))
    }
    fn unmount(&self, _path: &Path, _mode: UnmountMode) -> io::Result<()> {
        Err(unsupported("unmounting"))
    }
    fn fuse_waiting(&self, _connection: u32) -> io::Result<u64> {
        Err(unsupported("FUSE connection control"))
    }
    fn abort_fuse(&self, _connection: u32) -> io::Result<()> {
        Err(unsupported("FUSE connection control"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_stub_answers_unsupported() {
        let host = host_services();
        let kind = |r: io::Result<()>| r.unwrap_err().kind();
        assert_eq!(
            host.dirs.config_dir().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            host.process.hostname().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            kind(host.secrets.put("x", b"y")),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            kind(host.mounts.unmount(Path::new("/x"), UnmountMode::Lazy)),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            kind(host.process.request_backtrace(ThreadRef::unknown())),
            io::ErrorKind::Unsupported
        );
        for r in [
            host.process.suspend(1),
            host.process.resume(1),
            host.process.kill(1),
        ] {
            assert_eq!(kind(r), io::ErrorKind::Unsupported);
        }
        assert!(!host.process.is_alive(1));
        // Lifecycle events still flow: the manual source is portable.
        assert_eq!(host.lifecycle.subscribe().try_recv(), None);
    }
}
