//! `constellation-engine`: the one implementation of Constellation's
//! storage algorithms, under every frontend (plan 31 §3, §4).
//!
//! Plan 31 C3 moved these modules out of `crates/cli` unchanged; C4 moved
//! the mounted view in ([`view::View`], `impl constellation_vfs::Vfs`),
//! with the kernel invalidation thread ([`kernel_inval`]), which delivers
//! to each view's `constellation_vfs::FrontendEvents`. The frontends (the
//! FUSE one is `constellation-frontend-fuse`) reach the engine only
//! through `constellation-vfs`; the engine's waits name themselves on its
//! watchdog (`constellation_vfs::watch::stage`).
//!
//! The node itself is [`Engine`] (plan 31 C4c: `Engine::start`,
//! `Engine::open_view`), hosted N-to-a-process by [`EngineHost`] within a
//! [`ResourceBudget`], run the way its [`EngineProfile`] says; a view is
//! opened from a [`ViewSpec`].
//!
//! Public modules are the ones `crates/cli` uses; the rest are internal.
//! Inside a module, an item is `pub` because it already was (it was
//! crate-visible in the binary) or because the CLI needs it; nothing
//! here is a stability promise yet.

pub mod atime;
pub mod authority_driver;
pub mod backend;
pub mod completion;
pub mod control;
pub mod coop;
pub mod cto;
pub mod designation;
pub mod doctor;
pub mod e2e_pin;
pub mod epoch;
pub mod existence;
pub mod fault;
pub mod forward;
pub mod fsck;
pub mod fsync_wait;
pub mod gc;
pub mod held;
pub mod holds;
mod host;
pub mod inbox;
pub mod kernel_inval;
pub mod lease;
pub mod leave;
pub mod lifecycle;
pub mod locks;
pub mod log_buffer;
mod mtree_gc;
pub mod mtree_publish;
mod mtree_read;
mod node;
pub mod p2p;
pub mod paths;
pub mod pin;
pub mod placement;
pub mod prefetch;
mod profile;
pub mod prune;
mod recovery;
pub mod registry;
pub mod reintegrate;
pub mod scan;
pub mod shipper;
mod singleton;
pub mod snapacct;
pub mod snapexpire;
pub mod snapsched;
pub mod snapshot;
pub mod snapshot_batch;
pub mod snapwalk;
mod sources;
pub mod staging;
pub mod sync;
pub mod target;
pub mod upload;
pub mod view;
pub mod writeback;

pub use host::{EngineHost, FsId, ResourceBudget};
pub use node::{
    default_state_dir, DeferredEvents, Engine, EngineConfig, PassphraseSource, PhaseHook, ViewInfo,
};
pub use profile::{
    BackgroundMode, EngineProfile, FuseTransportMode, LeaseMode, P2pMode, UploadMode,
    CACHE_VERIFY_ENV,
};
pub use view::{HandleTableSnapshot, PassthroughStatus, View, ViewHandoff, ViewQos, ViewSpec};
