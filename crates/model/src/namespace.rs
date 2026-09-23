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

impl NsOp {
    /// The one name this op (or the record it produces) touches. Every
    /// modeled op touches exactly one name, which is what lets plan 30
    /// §M3b's per-entry before-image be a single bit (see
    /// `protocol::JournalEntry::before`).
    pub fn name(self) -> Name {
        match self {
            NsOp::CreateExcl(n) | NsOp::Unlink(n) => n,
        }
    }
}

/// Whether `name` is present in `dir`.
pub fn present(dir: DirState, name: Name) -> bool {
    dir & (1 << name) != 0
}

/// `dir` with `name`'s presence set to `on`: restoring a captured
/// before-image (plan 30 §M3b's before-image substitution).
pub fn with_presence(dir: DirState, name: Name, on: bool) -> DirState {
    if on {
        dir | (1 << name)
    } else {
        dir & !(1 << name)
    }
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
    /// Plan 30 §M3b/§M13 (model round 3a): the op was acknowledged by a
    /// holder that was then deposed before the op reached the log — the
    /// documented acked-before-durable gap (L2/L3, closed by M9's
    /// `ack=s3`). Its acknowledgement is *tentative*: the op may still
    /// land later (its replay by rid executes on the new holder) or
    /// resolve as a conflict copy, and nothing in between may be required
    /// to see it. The history is rewritten to this the moment the tenure
    /// ends (`protocol::mark_tentative`, at the takeover CAS or the
    /// stranding), and `protocol::prop_linearizable` feeds such an op to
    /// the checker as an operation still in flight on a thread of its own:
    /// free to linearize anywhere after its invocation, or not at all.
    /// Only a deposed holder's own journaled ops get this; an op
    /// acknowledged to a requester from a shadow is checked strictly.
    Tentative,
    /// A tentative op whose replay by rid was refused: the real code
    /// materializes a `.constellation-conflict/` copy
    /// (`recovery::materialize_remote`), or counts an `ENOENT`ed unlink
    /// as satisfied. Fed to the checker exactly like [`NsRet::Tentative`]
    /// (an in-flight op that need not linearize); kept distinct so a
    /// history reads as what happened.
    Conflicted,
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
