//! Host services: everything Constellation needs from the operating system
//! that is not a file read or write (plan 31 §3, §4).
//!
//! The storage engine is one implementation on every OS (plan 31 §2). What
//! differs between hosts is a short, enumerable list of capabilities —
//! where state and config live, whether a pid is alive and what state it
//! is in, how to detach into the background, advisory file locks, hole
//! punching and fsync strength, where secrets are kept, power/network
//! lifecycle, and the mount table — and this crate is that list. Each is a
//! trait; [`HostServices`] bundles one implementation of each, and
//! [`HostServices::native`] builds the set for the OS this binary targets.
//!
//! ## Why a bundle of `Arc<dyn Trait>`
//!
//! The bundle is injected (C3's `Engine::start` takes one), and the reason
//! to inject is to swap single services: plan 37's engine pods replace the
//! file-backed [`SecretStore`] with an [`EphemeralSecretStore`] so no
//! secret touches a container filesystem, tests push lifecycle events into
//! a [`ManualLifecycle`], a port replaces one service at a time while the
//! rest stay native. A struct of concrete per-OS types would make each of
//! those a different type and push a generic parameter through `Engine`,
//! every `View` and every frontend for no gain: every method here wraps a
//! system call or a file operation, so the virtual call is noise next to
//! it. The bundle is `Clone` (eight `Arc` bumps) so each engine can hold
//! its own.
//!
//! ## OS selection
//!
//! [`linux`] and [`macos`] are real implementations; `macos` compiles on
//! the cross-check (`make check-cross`) and is correct by reading, with
//! the pieces plan 34 M2 owns (libproc process facts, `posix_spawn`
//! daemonizing, a Keychain secret store) answering
//! [`std::io::ErrorKind::Unsupported`] until then, which every caller
//! already treats as "fact unknown". `windows`, `android`, `ios` and
//! `freebsd` are compile-only stubs answering `Unsupported` everywhere
//! (plans 35/36 fill them). Code shared by Linux and macOS lives in
//! `unix`. `libc` is a `cfg(unix)` dependency only.
//!
//! ## Reaching the services before C3
//!
//! Until `constellation-engine` exists (plan 31 C3) there is nothing to
//! inject into: the modules that will move there still live in
//! `crates/cli` with no engine value to hang a bundle on. They call
//! [`native()`], one process-wide native bundle, so every host-service
//! use is already behind the portable traits and C3 only has to swap
//! `constellation_platform::native()` for the engine's injected bundle
//! (grep for it). New engine code should not add more of them.
//!
//! The Linux/FUSE-only helpers that are not host services in the
//! cross-platform sense — the privileged `/dev/fuse` mount
//! ([`linux::fuse_mount_fd`]), and the `/proc` text parsers — are free
//! functions in [`linux`].

pub mod daemon;
pub mod dirs;
pub mod fs;
pub mod lifecycle;
pub mod lock;
pub mod mounts;
pub mod process;
pub mod secrets;

mod unsupported;

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix;

#[cfg(target_os = "android")]
pub mod android;
#[cfg(target_os = "freebsd")]
pub mod freebsd;
#[cfg(target_os = "ios")]
pub mod ios;
#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;
#[cfg(target_os = "windows")]
pub mod windows;

/// The implementation for this target: `sys::host_services()` and
/// `sys::file_lock()`.
#[cfg(target_os = "android")]
use android as sys;
#[cfg(target_os = "freebsd")]
use freebsd as sys;
#[cfg(target_os = "ios")]
use ios as sys;
#[cfg(target_os = "linux")]
use linux as sys;
#[cfg(target_os = "macos")]
use macos as sys;
#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows",
    target_os = "android",
    target_os = "ios",
    target_os = "freebsd"
)))]
use unsupported as sys;
#[cfg(target_os = "windows")]
use windows as sys;

use std::sync::{Arc, OnceLock};

pub use daemon::{Daemon, Detached};
pub use dirs::Dirs;
pub use fs::FsPrimitives;
pub use lifecycle::{LifecycleEvent, LifecycleSource, ManualLifecycle, Subscription};
pub use lock::{FileLock, LockGuard};
pub use mounts::{MountEntry, MountOpts, MountTable, UnmountMode};
pub use process::{Lineage, Process, ProcessFacts, TaskFacts, ThreadRef};
pub use secrets::{
    core_dumps_forbidden, forbid_core_dumps, Credential, CredentialSource, EphemeralSecretStore,
    FileSecretStore, Secret, SecretStore,
};

/// The rdev conversions at the Linux boundary (plan 31 §7): the portable
/// `(major, minor)` pair is what is stored and sent, and these pack and
/// unpack glibc's 64-bit `dev_t` and the kernel's 32-bit FUSE encoding.
/// Owned by `constellation-types` (pure arithmetic, testable on any host)
/// and re-exported here, where every other boundary conversion lives.
pub use constellation_types::rdev::{
    from_linux_fuse_rdev, from_linux_rdev, to_linux_fuse_rdev, to_linux_rdev, Rdev,
};

/// One implementation of every host service. See the crate docs for why
/// this is a bundle of trait objects.
#[derive(Clone)]
pub struct HostServices {
    pub dirs: Arc<dyn Dirs>,
    pub process: Arc<dyn Process>,
    pub daemon: Arc<dyn Daemon>,
    pub file_lock: Arc<dyn FileLock>,
    pub fs: Arc<dyn FsPrimitives>,
    pub secrets: Arc<dyn SecretStore>,
    pub lifecycle: Arc<dyn LifecycleSource>,
    pub mounts: Arc<dyn MountTable>,
}

impl HostServices {
    /// A fresh set of this OS's native services. Each call builds new
    /// values (a fresh [`ManualLifecycle`] with no subscribers, say); a
    /// process that wants one shared set uses [`native()`].
    pub fn native() -> HostServices {
        sys::host_services()
    }
}

impl std::fmt::Debug for HostServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostServices").finish_non_exhaustive()
    }
}

/// The process-wide native bundle, built on first use. For code that has
/// no injected [`HostServices`] yet (see the crate docs); it is the one
/// place C3 replaces with the engine's own bundle.
pub fn native() -> &'static HostServices {
    static NATIVE: OnceLock<HostServices> = OnceLock::new();
    NATIVE.get_or_init(HostServices::native)
}

/// `io::ErrorKind::Unsupported` naming what this host lacks.
pub(crate) fn unsupported(what: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("{what} is not supported on this host"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_native_bundle_is_shared_and_clonable() {
        let a = native() as *const HostServices;
        let b = native() as *const HostServices;
        assert_eq!(a, b);
        let clone = native().clone();
        assert!(Arc::ptr_eq(&clone.process, &native().process));
    }

    #[test]
    fn rdev_conversions_are_reexported() {
        let r = Rdev::new(8, 1);
        assert_eq!(to_linux_rdev(r), 0x801);
        assert_eq!(from_linux_rdev(0x801), r);
        assert_eq!(from_linux_fuse_rdev(to_linux_fuse_rdev(r)), r);
    }
}
