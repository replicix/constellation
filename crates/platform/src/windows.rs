//! Windows host services: a compile-only stub until plan 35 fills it in
//! (`LockFileEx` locks, `%APPDATA%`/`%LOCALAPPDATA%` dirs, named-pipe
//! transport, WinFsp mounts). Every service answers
//! `io::ErrorKind::Unsupported`; lifecycle events are the portable
//! `ManualLifecycle`.

pub(crate) use crate::unsupported::{file_lock, host_services};
