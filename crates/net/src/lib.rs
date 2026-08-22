//! P2P fast path: iroh endpoint + registry-based allowlist, gossip
//! (invalidation, digests, lease handoff), cooperative cache, and
//! latency-adaptive source selection. Never load-bearing for correctness.
//!
//! See docs/DESIGN.md §7 (cooperative cache) and §8 (security).
