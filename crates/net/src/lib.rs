//! P2P fast path: iroh endpoint + registry-based allowlist, gossip
//! (invalidation, digests, lease handoff), cooperative cache, and
//! latency-adaptive source selection. Never load-bearing for correctness.
//!
//! See docs/DESIGN.md §7 (cooperative cache) and §8 (security).
//!
//! # Why nothing here can break the filesystem
//!
//! S3 is the source of truth and the commit point. Every mechanism in
//! this crate accelerates something the S3 path already does on a timer:
//!
//! * `SegmentPublished` gossip makes a peer tail *now* instead of at its
//!   next poll — but the poll still happens.
//! * A lease handoff tells a waiting writer that the CAS is worth trying
//!   *now* — but authority still comes from the lease object's CAS, so a
//!   forged or replayed handoff just makes the requester's CAS fail.
//!
//! So a peer that is unreachable, lying, or absent costs latency and
//! nothing else. `CONSTELLATION_P2P=off` disables the whole crate, and
//! the fault-injection harness asserts the S3-only bounds still hold.

pub mod allowlist;
pub mod endpoint;
pub mod handoff;
pub mod identity;
pub mod message;

pub use allowlist::{Allowlist, Decision};
pub use endpoint::{topic_for, P2p, PeerService};
pub use handoff::{handle_request, interpret_reply, Handoff, RequestOutcome};
pub use identity::{load_or_create, parse_pubkey, pubkey_hex};
pub use message::{Payload, Signed, ALPN};

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
