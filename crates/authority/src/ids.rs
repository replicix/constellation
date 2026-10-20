//! Identifiers and the logical clocks the core reasons with.
//!
//! Nothing in this module reads a clock or mints anything non-deterministic:
//! [`OpId`]s and [`TimerId`]s come from the core's own counters, and every
//! time the core sees arrives as an input: `Core::handle`'s `now` is a
//! [`Now`], one sample of the node's three protocol clocks
//! (`constellation_types::clock`, plan 43 C7-H1).
//!
//! - [`Ms`] is WALL: unix milliseconds on the host's wall clock (in the
//!   simulation, the paused tokio clock offset from a fixed base plus the
//!   node's skew and steps). What is compared across nodes, written to S3,
//!   persisted, used as an identity key or shown in status.
//! - [`HonourMs`] is HONOUR: monotone, never under-counts, follows forward
//!   wall steps and ignores backward ones. A holder's stamps and the checks
//!   of its own rights.
//! - [`WaitMs`] is WAIT: monotone, never over-counts, ignores every wall
//!   step. A waiter's stamps and liveness timers.
//!
//! The holder's side (plan 43 C7-H1 2/4) reads `now.honour`: its lease
//! (`LeaseState::honour_until`, from the body's `holder_expires_ms` and
//! `holder_clock`), lock grants, delegation, read delegations, the S3
//! liveness proof. The waiter's side (3/4) reads `now.wait`: an owner's grant
//! records and lock tables, a root's generation outwaits, read-delegation
//! grants, quarantines, graces, the takeover floor, the beats' arrival and
//! the holder's beat (`sent`, the lease's end and the guard are readings of
//! its wait clock), the seal watch, and the persisted horizons (written as
//! wall values, read back as time left on the wait clock). The liveness
//! timers' sites still read `now.wall` until the last commit (4/4).

/// A node id, as in `nodes/<id>` and `Lease::holder`.
pub type NodeId = u64;
/// A lease epoch (`Lease::epoch`), stamped into every shipped segment.
pub type Epoch = u64;
/// A log sequence number (`log/<part>/<seq>`).
pub type Seq = u64;

pub use constellation_types::clock::{HonourMs, Ms, Now, WaitMs};

/// Correlates an issued action with its result: an S3 request with its
/// `Event::S3`, a peer request with the `PeerMsg` reply that echoes it (or
/// the `Event::PeerFailed` that stands in for one), an upload or publish
/// with its completion. Minted by the core; a result naming an id the core
/// no longer tracks is stale and is ignored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpId(pub u64);

/// Correlates `Action::SetTimer` with the `Event::Timer` that fires it.
/// The core forgets a timer it no longer wants (and tells the driver with
/// `Action::CancelTimer`, which is an optimisation: a stale fire is ignored).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId(pub u64);
