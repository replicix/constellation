//! Mounting a view: [`mount`] and the [`FuseSession`] it returns.

use crate::adapter::{FuseFs, KernelTuning};
use crate::notify::FuseNotifySink;
use constellation_vfs::{FrontendCaps, Vfs};
use std::path::Path;
use std::sync::Arc;

/// How a view is mounted.
#[derive(Debug, Clone)]
pub struct MountOptions {
    /// The source name mount tools report.
    pub fs_name: String,
    /// Other users may access the mount (`SessionACL::All`; else the
    /// mounting user only).
    pub allow_other: bool,
    /// Mounted read-only (a frozen snapshot view).
    pub read_only: bool,
    /// fuser worker threads dispatching this mount's requests.
    pub n_threads: usize,
    /// The kernel request queue `FUSE_INIT` negotiates.
    pub tuning: KernelTuning,
}

/// A mounted FUSE session, not yet serving. The daemon runs it on a
/// dedicated OS thread ([`FuseSession::run`]), keeping an
/// [`FuseUnmounter`] to end it from elsewhere and a [`FuseNotifySink`]
/// for the engine's cache invalidations.
pub struct FuseSession<V: Vfs> {
    session: fuser::Session<FuseFs<V>>,
}

/// Mount `view` at `mountpoint`: `DefaultPermissions` (the kernel checks
/// modes), the requested ACL and read-only flag, `n_threads` workers each
/// with its own `/dev/fuse` descriptor on Linux (`clone_fd`), and the
/// `FUSE_INIT` negotiation `caps` and `opts.tuning` ask for.
pub fn mount<V: Vfs>(
    view: Arc<V>,
    mountpoint: &Path,
    opts: &MountOptions,
    caps: FrontendCaps,
) -> std::io::Result<FuseSession<V>> {
    let mut options = vec![
        fuser::MountOption::FSName(opts.fs_name.clone()),
        fuser::MountOption::DefaultPermissions,
    ];
    if opts.read_only {
        options.push(fuser::MountOption::RO);
    }
    let mut config = fuser::Config::default();
    config.mount_options = options;
    config.acl = if opts.allow_other {
        fuser::SessionACL::All
    } else {
        fuser::SessionACL::Owner
    };
    config.n_threads = Some(opts.n_threads);
    config.clone_fd = cfg!(target_os = "linux") && config.n_threads != Some(1);
    let session = fuser::Session::new(FuseFs::new(view, caps, opts.tuning), mountpoint, &config)?;
    Ok(FuseSession { session })
}

impl<V: Vfs> FuseSession<V> {
    /// A handle that unmounts this session from any thread (the daemon's
    /// `remove_mount`, a signal).
    pub fn unmounter(&mut self) -> FuseUnmounter {
        FuseUnmounter(self.session.unmount_callable())
    }

    /// This mount's kernel cache, for the engine's invalidations.
    pub fn notifier(&self) -> FuseNotifySink {
        FuseNotifySink(self.session.notifier())
    }

    /// Serve requests until the mount ends (an unmount, from here or
    /// outside); blocks the calling thread.
    pub fn run(self) -> std::io::Result<()> {
        self.session.run()
    }
}

/// Ends a [`FuseSession`] from another thread.
pub struct FuseUnmounter(fuser::SessionUnmounter);

impl FuseUnmounter {
    pub fn unmount(&mut self) -> std::io::Result<()> {
        self.0.unmount()
    }
}
