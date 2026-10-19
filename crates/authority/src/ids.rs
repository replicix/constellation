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
//! The core's own code does not use the second and third yet: every site
//! reads `now.wall` until the commits that convert them (plan 43 C7-H1,
//! 2/4 to 4/4), so the plumbing changes no behaviour without a step.

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
