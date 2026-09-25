//! P2P fast path: iroh endpoint + registry-based allowlist, gossip
//! (invalidation, digests, lease handoff), cooperative cache, and
//! latency-adaptive source selection.
//!
//! See docs/explanation/DESIGN.md §7 (cooperative cache) and §8 (security).
//!
//! # Why most of this cannot break the filesystem
//!
//! S3 is the source of truth and the commit point. Most mechanisms in
//! this crate accelerate something the S3 path already does on a timer:
//!
//! * `SegmentPublished` gossip makes a peer tail *now* instead of at its
//!   next poll — but the poll still happens.
//! * A lease handoff tells a waiting writer that the CAS is worth trying
//!   *now* — but authority still comes from the lease object's CAS, so a
//!   forged or replayed handoff just makes the requester's CAS fail.
//!
//! So for those, a peer that is unreachable, lying, or absent costs
//! latency and nothing else. `CONSTELLATION_P2P=off` disables the whole
//! crate, and the fault-injection harness asserts the S3-only bounds
//! still hold.
//!
//! **Delegation is the exception** ([`delegation`], DESIGN.md §5.2): a
//! foreign write under an offline-designated path is only legitimate
//! while a live delegation from the designee backs it, and the
//! designee's flush-ack requirement is what keeps its "provably holds
//! everything" invariant true. There, an unreachable or non-responding
//! designee correctly turns other nodes read-only rather than degrading
//! to some default — that IS the safety property, not a fallback from
//! one.

pub mod allowlist;
pub mod bloom;
pub mod endpoint;
pub mod epoch;
pub mod handoff;
pub mod identity;
pub mod message;
pub mod paths;
pub mod peers;
pub mod reconcile;
pub mod relay;

pub use allowlist::{Allowlist, Decision};
pub use bloom::Bloom;
pub use endpoint::{
    topic_for, ChunkFetch, DigestDelta, DigestSnapshot, LogEvent, P2p, PathKind, PeerService,
};
pub use epoch::{
    component_covers_roster, component_quorum, Activation as EpochActivation, EpochState,
    Machine as EpochMachine, Promise as EpochPromise,
};
pub use handoff::{handle_request, interpret_reply, Handoff, RequestOutcome};
pub use identity::{load_or_create, parse_pubkey, pubkey_hex};
pub use message::{
    ChunkDecline, ChunkStatus, EpochCarrier, EpochClaim, LockOutcomeWire, LockRenewResultWire,
    LockRenewWire, LockTestOutcomeWire, Payload, Signed, ALPN,
};
pub use paths::PathSummary;
pub use peers::{run_gossip, Peer, PeerEnrollment, Peers, Refresher};
pub use relay::RelayPolicy;

pub use iroh::EndpointAddr;
/// Re-exported so the daemon can name peer ids without depending on
/// `iroh` directly: this crate is the only place that knows the
/// transport.
pub use iroh::EndpointId;

/// Whether the P2P fast path is enabled. `CONSTELLATION_P2P=off`
/// (or `0`/`false`) is the kill switch the harness uses to prove the
/// S3-only path still works.
pub fn enabled() -> bool {
    match std::env::var("CONSTELLATION_P2P") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    /// The kill switch is read from the environment, so this test must
    /// not run concurrently with anything else reading it; it only
    /// touches values this test sets.
    #[test]
    fn kill_switch_parsing() {
        // SAFETY: single-threaded test scope, restored before returning.
        for (v, want) in [("off", false), ("0", false), ("FALSE", false), ("on", true)] {
            unsafe { std::env::set_var("CONSTELLATION_P2P", v) };
            assert_eq!(super::enabled(), want, "CONSTELLATION_P2P={v}");
        }
        unsafe { std::env::remove_var("CONSTELLATION_P2P") };
        assert!(super::enabled(), "default is enabled");
    }
}
