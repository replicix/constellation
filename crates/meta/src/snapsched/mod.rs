//! Automatic snapshot schedules (plan 32): a directory carries its whole
//! snapshot policy in the `user.constellation.snapshots` xattr, and a
//! scheduler singleton creates and expires the snapshots it describes.
//!
//! This module holds the pure parts. Step 1 — the policy language
//! ([`policy`]): the tier/setting types, the hand-written parser, and the
//! canonical [`std::fmt::Display`] form. Retention (Step 2), the
//! scheduler and the accountant come later and consume these types; the
//! calendar types here are deliberately the only ones retention needs to
//! align a bucket in a timezone.
//!
//! Nothing here touches the clock, the environment or the locale. Same
//! bytes in, same policy out, on every node, forever: a node that parsed
//! a policy differently would delete different snapshots.

pub mod policy;

pub use policy::{Interval, Keep, SnapPolicy, Tier, WeekStart};

/// The xattr that binds a snapshot schedule to a directory. Set it, and
/// that directory becomes a *policy root*: one snapshot stream covering
/// exactly its subtree, with no inheritance (plan 32, "Settled
/// decisions"). Sibling of [`crate::PRUNE_XATTR`].
pub const SNAPSHOT_POLICY_XATTR: &str = "user.constellation.snapshots";
