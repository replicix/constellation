//! Android host services: a compile-only stub until plan 36 fills it in
//! (app-private dirs, a Keystore-backed secret store,
//! `ConnectivityManager` and activity lifecycle events). Every service
//! answers `io::ErrorKind::Unsupported`; lifecycle events are the portable
//! `ManualLifecycle` until then.

pub(crate) use crate::unsupported::{file_lock, host_services};
