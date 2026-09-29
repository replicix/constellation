//! iOS host services: a compile-only stub for a future port (plan 31
//! §13). Every service answers `io::ErrorKind::Unsupported`; lifecycle
//! events are the portable `ManualLifecycle`.

pub(crate) use crate::unsupported::{file_lock, host_services};
