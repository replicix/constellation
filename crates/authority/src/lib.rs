//! The sans-IO authority core (plan 30 M5) and the pieces the deterministic
//! simulation shares with it.
//!
//! `Core::handle(now, event, replica) -> Vec<Action>` is the whole
//! interface: every decision the daemon's sync task, lease keeper,
//! forwarder, recovery drain and shipper make today, as a pure state
//! machine over inputs ([`event::Event`]) and outputs ([`action::Action`]),
//! with the local replica reached synchronously through
//! [`replica::Replica`]. The IO driver (`node_runtime` in production, the
//! simulation in `tests/sim.rs`) owns the clocks, the sockets, the S3
//! clients and the FUSE reply channels, and nothing else.
//!
//! Why a crate of its own rather than `crates/cli/src/authority/`: the
//! `constellation` crate is a binary, so nothing else can link a module
//! inside it — neither the simulation's integration tests (which need
//! `stateright` as a dev-dependency without dragging it into the daemon)
//! nor the model-checking stretch goal (Stateright actors delegating to
//! the real core). A library crate also keeps the boundary honest by
//! construction: this crate depends on `constellation-meta` (the replica)
//! and `constellation-store-s3` (the `Lease`/`LeaseTag` types the S3
//! actions carry), and on nothing that performs IO.
//!
//! Phase 2 of the milestone moves the corresponding code out of
//! `crates/cli` (`lease.rs`, `forward.rs`, `recovery.rs`, the ship/tail/
//! publish sequencing of `shipper.rs`, the sync loop's arms in
//! `node_runtime.rs`/`main.rs`, M13's `inbox.rs` and M4's held-record
//! decisions) into [`core`], and makes `node_runtime` the driver. The
//! mapping from each of those decision points to its event and action is
//! the "Plan 30 M5 — phase 1" section of `docs/plans/v1/PROGRESS.md`.

pub mod action;
pub mod core;
pub mod event;
pub mod ids;
pub mod replica;
pub mod segment;

pub use action::{Action, ClientReply, ControlOk, ReadAnswer, S3Op, TimerKind};
pub use core::{
    lease_may_carry, resolve_epoch_claims, Config, Core, EpochClaimView, EpochState, InboxView,
    ShipState, Stats,
};
pub use event::{
    Carrier, CasFailure, Control, Event, PeerLink, PeerMsg, Policy, ReadGrantMsg, ReadIndexOutcome,
    S3Failure, S3Result, UploadResult,
};
pub use ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
pub use replica::Replica;
