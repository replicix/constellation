//! The file-descriptor types the transports pass around, with a stand-in
//! on hosts that have none.
//!
//! `std::os::fd` exists on unix only. Rather than sprinkle `cfg(unix)` over
//! every signature that mentions an fd (`Frame`, `CallCtx`, the client's
//! `call_with_fd`), the crate names [`OwnedFd`]/[`BorrowedFd`] from here:
//! on unix they are std's, elsewhere they are uninhabited types, so an
//! `Option<OwnedFd>` is always `None` and the code that would use one is
//! statically dead. The `NamedPipe` transport (plan 35) therefore needs no
//! separate frame type: it simply never produces or accepts one.

#[cfg(unix)]
pub use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

#[cfg(not(unix))]
mod stub {
    use std::convert::Infallible;

    /// No descriptors on this host: cannot be constructed.
    #[derive(Debug)]
    pub struct OwnedFd(Infallible);

    /// No descriptors on this host: cannot be constructed.
    #[derive(Debug, Clone, Copy)]
    pub struct BorrowedFd<'a>(Infallible, std::marker::PhantomData<&'a ()>);

    pub trait AsFd {}
}
#[cfg(not(unix))]
pub use stub::{AsFd, BorrowedFd, OwnedFd};
