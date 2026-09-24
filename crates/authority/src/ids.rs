//! Identifiers and the logical clock the core reasons with.
//!
//! Nothing in this module reads a clock or mints anything non-deterministic:
//! [`OpId`]s and [`TimerId`]s come from the core's own counters, and every
//! [`Ms`] the core sees arrives as an input (`Core::handle`'s `now`).

/// A node id, as in `nodes/<id>` and `Lease::holder`.
pub type NodeId = u64;
/// A lease epoch (`Lease::epoch`), stamped into every shipped segment.
pub type Epoch = u64;
/// A log sequence number (`log/<part>/<seq>`).
pub type Seq = u64;

/// Unix milliseconds, as `Lease::expires_unix_ms` and the lease helpers
/// (`Lease::is_expired`, `is_claimable`) use them. Wall-clock in
/// production; the simulation's paused tokio clock offset from a fixed
/// base in tests. The core never calls `now_unix_ms()` itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ms(pub i64);

impl Ms {
    pub fn plus(self, ms: u64) -> Ms {
        Ms(self.0.saturating_add(ms as i64))
    }

    /// `self - earlier`, in ms (negative when `earlier` is later).
    pub fn since(self, earlier: Ms) -> i64 {
        self.0 - earlier.0
    }
}

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
