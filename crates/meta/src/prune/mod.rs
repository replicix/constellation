//! Prune policies (plan 22): retention expressed as a `Policy` stored in
//! the `user.constellation.prune` xattr, evaluated by a singleton pruner
//! that publishes ordinary `Unlink` mutations. This module holds the
//! pure policy language (`policy`) and the pure per-entry evaluation
//! (`eval`); all I/O, lease handling and partition fan-out live in the
//! cli crate's `prune` module.

pub mod eval;
pub mod policy;

pub use eval::{Candidate, EntryFacts, Verdict};
pub use policy::{Filter, Of, Policy, PolicyError, Rule, RuleClause, Watermark};

/// The xattr that binds a prune policy to a directory subtree.
pub const PRUNE_XATTR: &str = "user.constellation.prune";
