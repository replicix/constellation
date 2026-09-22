//! The sequential specification the checker validates client histories
//! against (`stateright::semantics::SequentialSpec`), plus the pure
//! evaluation function the protocol model itself uses to decide what a
//! `create_excl`/`unlink` does to a directory.
//!
//! This corresponds to `execute_mutate` in `crates/meta/src/lib.rs` for
//! the `MutateOp::Create`/`MutateOp::Unlink` cases, reduced to the two
//! POSIX outcomes plan 30 §M1 asks for (`EEXIST`, `ENOENT`) over a small
//! fixed set of names in one directory. Mkdir/rmdir/rename/link are not
//! modeled: they share the same create-or-unlink shape at this level of
//! abstraction, and adding them would only grow the state space without
//! exercising a new code path (see the crate-level simplifications list).

use stateright::semantics::SequentialSpec;

/// Number of names in the (single, flat) modeled directory.
pub const N_NAMES: u8 = 2;

/// A directory entry name, `0..N_NAMES`.
pub type Name = u8;

/// A snapshot of "which names currently exist", as a bitmask (bit `i` set
/// means name `i` is present). Doubles as the type replayed log records
/// and shadow/journal overlays are folded into (see `model::node_replica`
/// and `model::log_state_and_epoch`).
pub type DirState = u8;

/// The two client-issuable operations (plan 30 §M1: "ops `create_excl(name)`
/// and `unlink(name)` at minimum").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NsOp {
    CreateExcl(Name),
    Unlink(Name),
}

/// The POSIX errnos this model cares about, matching
/// `crates/cli/src/forward.rs::meta_errno`'s `Exists`/`NoEnt` arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Errno {
    Eexist,
    Enoent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NsRet {
    Ok,
    Err(Errno),
}

/// A durable record: what actually happened, applied unconditionally
/// (idempotent) on replay by every replica. Because `create_excl`/`unlink`
/// are always their own effect, a record and the op that produced it are
/// the same shape — this stands in for `LogRecord` (`crates/meta`).
pub type Record = NsOp;

/// Evaluate `op` against `dir`, validating preconditions the way a live
/// `execute_mutate` call does (holder-side or single-node fast path):
/// `create_excl` on an existing name is refused, `unlink` on a missing
/// one is refused.
pub fn eval(dir: DirState, op: NsOp) -> (NsRet, DirState) {
    match op {
        NsOp::CreateExcl(n) => {
            if dir & (1 << n) != 0 {
                (NsRet::Err(Errno::Eexist), dir)
            } else {
                (NsRet::Ok, dir | (1 << n))
            }
        }
        NsOp::Unlink(n) => {
            if dir & (1 << n) != 0 {
                (NsRet::Ok, dir & !(1 << n))
            } else {
                (NsRet::Err(Errno::Enoent), dir)
            }
        }
    }
}

/// Apply a durable record unconditionally, the way log replay
/// (`apply_foreign`/journal replay) does: no precondition check, because
/// a record in the log is a proven historical fact.
pub fn force_apply(dir: DirState, op: Record) -> DirState {
    match op {
        NsOp::CreateExcl(n) => dir | (1 << n),
        NsOp::Unlink(n) => dir & !(1 << n),
    }
}

/// The reference object the `LinearizabilityTester` checks recorded
/// client histories against.
#[derive(Clone, Debug, Default)]
pub struct NamespaceSpec {
    pub present: DirState,
}

impl SequentialSpec for NamespaceSpec {
    type Op = NsOp;
    type Ret = NsRet;

    fn invoke(&mut self, op: &NsOp) -> NsRet {
        let (ret, next) = eval(self.present, *op);
        self.present = next;
        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_create_is_eexist() {
        let (r1, d1) = eval(0, NsOp::CreateExcl(0));
        assert_eq!(r1, NsRet::Ok);
        let (r2, _) = eval(d1, NsOp::CreateExcl(0));
        assert_eq!(r2, NsRet::Err(Errno::Eexist));
    }

    #[test]
    fn unlink_on_missing_is_enoent() {
        let (r, _) = eval(0, NsOp::Unlink(0));
        assert_eq!(r, NsRet::Err(Errno::Enoent));
    }

    #[test]
    fn force_apply_is_unconditional() {
        // Re-applying a create on an already-present name must not panic
        // or error: replay is idempotent by construction.
        let d = force_apply(0, NsOp::CreateExcl(0));
        let d2 = force_apply(d, NsOp::CreateExcl(0));
        assert_eq!(d, d2);
    }
}
