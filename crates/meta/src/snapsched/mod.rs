//! Automatic snapshot schedules (plan 32): a directory carries its whole
//! snapshot policy in the `user.constellation.snapshots` xattr, and a
//! scheduler singleton creates and expires the snapshots it describes.
//!
//! This module holds the pure parts.
//!
//! * Step 1 — the policy language ([`policy`]): the tier/setting types,
//!   the hand-written parser, and the canonical [`std::fmt::Display`]
//!   form.
//! * Step 2 — where a bucket starts ([`calendar`]) and which snapshots
//!   survive ([`retention`]): [`retention::evaluate`] is the *only*
//!   implementation of the rule, and the scheduler, the CLI and the web
//!   UI all call it rather than restating it.
//!
//! The scheduler and the accountant come later and consume these types.
//!
//! Nothing here touches the clock, the environment or the locale. Same
//! bytes in, same policy out, on every node, forever: a node that parsed
//! a policy differently would delete different snapshots.

pub mod calendar;
pub mod policy;
pub mod retention;

pub use calendar::{add_keep, bucket_start, next_bucket_start, subtract_keep};
pub use policy::{Interval, Keep, SnapPolicy, Tier, WeekStart};
pub use retention::{
    due, evaluate, grace_first_seen, grace_intersection, simulate, Origin, Reason, SnapFacts,
    Timeline, Verdict,
};

/// The xattr that binds a snapshot schedule to a directory. Set it, and
/// that directory becomes a *policy root*: one snapshot stream covering
/// exactly its subtree, with no inheritance (plan 32, "Settled
/// decisions"). Sibling of [`crate::PRUNE_XATTR`].
pub const SNAPSHOT_POLICY_XATTR: &str = "user.constellation.snapshots";
