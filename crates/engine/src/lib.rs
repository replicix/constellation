//! `constellation-engine`: the one implementation of Constellation's
//! storage algorithms, under every frontend (plan 31 §3, §4).
//!
//! Plan 31 C3 moved these modules out of `crates/cli` unchanged; the FUSE
//! adapter (`fusefs.rs`, whose `View` becomes an engine type in C4), the
//! kernel invalidation thread, the request watchdog and the daemon host
//! stay in `crates/cli` until C4. The seams they use are [`sync`] (the
//! sync task's request channel), [`events`] (what the engine tells the
//! frontend) and [`op_watch`] (the engine's waits, named on the
//! frontend's request watchdog).
//!
//! Public modules are the ones `crates/cli` uses; the rest are internal.
//! Inside a module, an item is `pub` because it already was (it was
//! crate-visible in the binary) or because the CLI needs it; nothing
//! here is a stability promise yet.

pub mod atime;
pub mod authority_driver;
pub mod backend;
pub mod coop;
pub mod cto;
pub mod designation;
pub mod doctor;
pub mod e2e_pin;
pub mod epoch;
pub mod events;
pub mod existence;
pub mod fault;
pub mod forward;
pub mod fsck;
pub mod gc;
pub mod held;
pub mod holds;
pub mod inbox;
pub mod lease;
pub mod leave;
pub mod locks;
pub mod log_buffer;
mod mtree_gc;
pub mod mtree_publish;
mod mtree_read;
pub mod op_watch;
pub mod paths;
pub mod pin;
pub mod placement;
pub mod prefetch;
pub mod prune;
mod recovery;
pub mod registry;
pub mod reintegrate;
pub mod scan;
pub mod shipper;
mod singleton;
pub mod snapshot;
mod sources;
pub mod staging;
pub mod sync;
pub mod target;
pub mod upload;
pub mod writeback;
