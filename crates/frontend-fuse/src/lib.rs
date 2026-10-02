//! `constellation-frontend-fuse`: the Linux FUSE frontend (FreeBSD-ready:
//! fuser has a pure-Rust FreeBSD mount), plan 31 §4.
//!
//! A thin adapter over `constellation-vfs`: [`FuseFs`] implements
//! `fuser::Filesystem` by decoding each callback into an `OpCtx` and a
//! `Vfs` call, and completes each op's responder — a newtype around the
//! fuser reply ([`reply`]) — as that reply. All filesystem policy is the
//! view's, beneath the trait; this crate holds the FUSE protocol: flag
//! decoding, the attribute and errno encodings ([`reply_code`]), the
//! `FUSE_INIT` negotiation, the kernel's cache notifications
//! ([`FuseNotifySink`]), the dispatcher sizing ([`threads`]) and the
//! mount itself ([`mount`]). It is the only crate that depends on
//! `fuser`.

mod adapter;
mod notify;
mod reply;
mod session;
pub mod threads;

pub use adapter::{FuseFs, KernelTuning};
pub use constellation_vfs::FrontendCaps;
pub use fuser::{NegotiatedInit, Transport};
pub use notify::FuseNotifySink;
pub use reply::reply_code;
pub use session::{
    mount, mount_source, DetachError, FuseHandoff, FuseSession, FuseUnmounter, HandoverCapable,
    MountOptions, MountSource, SessionControl, SessionExit, SessionHandoff, TransportConfig,
    TransportPolicy, DEFAULT_URING_QUEUE_DEPTH, TRANSPORT_ENV, URING_QUEUE_DEPTH_ENV,
};

/// The capabilities this frontend declares: [`FrontendCaps::linux_fuse`]
/// (`cluster_locks`: the mount forwards POSIX/`flock` locks to the view).
pub fn caps(cluster_locks: bool) -> FrontendCaps {
    FrontendCaps::linux_fuse(cluster_locks)
}
