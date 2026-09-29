//! FreeBSD host services: a compile-only stub for a future FUSE port
//! (fuser mounts natively there, and most of the shared `unix` module
//! will apply once it is compiled for FreeBSD too). Every service answers
//! `io::ErrorKind::Unsupported`; lifecycle events are the portable
//! `ManualLifecycle`.

pub(crate) use crate::unsupported::{file_lock, host_services};
