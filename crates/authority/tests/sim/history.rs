//! Client histories and the sequential specification they are checked
//! against — the same shape as `constellation-model`'s `namespace.rs`
//! (one flat directory of file names; `create_excl`, `unlink` and, new
//! here, `rename`), with `stateright`'s `LinearizabilityTester` used as
//! a library.
//!
//! Plan 30 M13 round 3a's tentative-acknowledgement rule is applied at
//! check time: an op whose effect was rolled back at any point (its rid
//! turned up in a replay queue — a stranded shadow, or a deposed holder's
//! own unshipped transaction) is fed to the checker as an operation still
//! in flight on a synthetic thread of its own, so the tester may
//! linearize it wherever its replay lands, or leave it out (a conflict
//! copy), and never requires it to be visible in between.

use constellation_meta::Rid;
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NsOp {
    Create(String),
    Unlink(String),
    Rename(String, String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NsRet {
    Ok,
    Eexist,
    Enoent,
}

/// The reference object: the set of names present in the root directory.
#[derive(Clone, Debug, Default)]
pub struct NsSpec {
    pub present: BTreeSet<String>,
}

impl SequentialSpec for NsSpec {
    type Op = NsOp;
    type Ret = NsRet;

    fn invoke(&mut self, op: &NsOp) -> NsRet {
        match op {
            NsOp::Create(n) => {
                if self.present.contains(n) {
                    NsRet::Eexist
                } else {
                    self.present.insert(n.clone());
                    NsRet::Ok
                }
            }
            NsOp::Unlink(n) => {
                if self.present.remove(n) {
                    NsRet::Ok
                } else {
                    NsRet::Enoent
                }
            }
            NsOp::Rename(a, b) => {
                if !self.present.remove(a) {
                    NsRet::Enoent
                } else {
                    self.present.insert(b.clone());
                    NsRet::Ok
                }
            }
        }
    }
}

pub type ThreadId = u64;

#[derive(Clone, Debug)]
pub enum HistEvt {
    Invoke {
        thread: ThreadId,
        rid: Rid,
        op: NsOp,
    },
    Return {
        thread: ThreadId,
        rid: Rid,
        ret: NsRet,
    },
}

#[derive(Default)]
pub struct History {
    events: Mutex<Vec<HistEvt>>,
}

impl History {
    pub fn invoke(&self, thread: ThreadId, rid: Rid, op: NsOp) {
        self.events
            .lock()
            .unwrap()
            .push(HistEvt::Invoke { thread, rid, op });
    }

    pub fn ret(&self, thread: ThreadId, rid: Rid, ret: NsRet) {
        self.events
            .lock()
            .unwrap()
            .push(HistEvt::Return { thread, rid, ret });
    }

    pub fn events(&self) -> Vec<HistEvt> {
        self.events.lock().unwrap().clone()
    }

    /// Ops that returned, with their result.
    pub fn returned(&self) -> Vec<(Rid, NsRet)> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                HistEvt::Return { rid, ret, .. } => Some((rid, ret)),
                _ => None,
            })
            .collect()
    }

    pub fn invoked(&self) -> usize {
        self.events()
            .iter()
            .filter(|e| matches!(e, HistEvt::Invoke { .. }))
            .count()
    }
}

/// Thread ids above this are synthetic (one per tentative op).
const TENTATIVE_THREAD_BASE: ThreadId = 1 << 40;

/// Check the history for linearizability against [`NsSpec`], treating
/// `tentative` rids per the module doc. Returns the reason on failure.
pub fn check_linearizable(events: &[HistEvt], tentative: &HashSet<Rid>) -> Result<(), String> {
    let mut tester = LinearizabilityTester::<ThreadId, NsSpec>::new(NsSpec::default());
    for (i, evt) in events.iter().enumerate() {
        let r = match evt {
            HistEvt::Invoke { thread, rid, op } => {
                let thread = if tentative.contains(rid) {
                    TENTATIVE_THREAD_BASE + i as u64
                } else {
                    *thread
                };
                tester.on_invoke(thread, op.clone()).map(|_| ())
            }
            HistEvt::Return { thread, rid, ret } => {
                if tentative.contains(rid) {
                    // Left in flight forever: may linearize anywhere after
                    // its invocation, or not at all.
                    Ok(())
                } else {
                    tester.on_return(*thread, *ret).map(|_| ())
                }
            }
        };
        if let Err(e) = r {
            return Err(format!(
                "history is not a valid concurrent history at event {i}: {e}"
            ));
        }
    }
    if tester.is_consistent() {
        Ok(())
    } else {
        Err("history is not linearizable".to_string())
    }
}

/// Linearizability with the log as the witness. Every op that took effect
/// has a `Completed { rid }` at one log position, and the log is the
/// holder's execution order, so the successes' linearization order is
/// known; refusals (`EEXIST`/`ENOENT`) are pure reads and only need *some*
/// point in their real-time window at which the spec refuses the same
/// way. That makes the check exact for this spec and polynomial, where
/// the generic tester's search is exponential in overlapping ops (a
/// crash-heavy run leaves many tentative ops in flight forever, and the
/// tester never finishes). Checks:
/// 1. real time: a success that returned before another was invoked
///    precedes it in the log (tentative ops only need to follow what
///    returned before their invocation);
/// 2. values: running the spec in log order gives every non-tentative
///    success `Ok`;
/// 3. refusals: an op refused with `ret` (and absent from the log) has a
///    spec state in its window — after every success that returned before
///    it was invoked, before every success invoked after it returned —
///    that returns `ret`. A refusal with no such state is re-checked with
///    the tentative ops invoked before it returned applied on top of the
///    window's states: a holder that later died may have evaluated it
///    against an effect it had acked but not shipped, which its requester
///    replayed later (or gave up on). That is plan 30 §1.2's L2 window
///    seen by a third party, which M9 closes; it is counted as
///    [`Witness::observed_tentative`], not failed.
pub fn check_linearizable_witnessed(
    events: &[HistEvt],
    tentative: &HashSet<Rid>,
    completed_at: &std::collections::HashMap<Rid, (u64, usize)>,
) -> Result<Witness, String> {
    use std::collections::HashMap;
    let mut invoke_at: HashMap<Rid, (usize, NsOp)> = HashMap::new();
    let mut return_at: HashMap<Rid, (usize, NsRet)> = HashMap::new();
    for (i, e) in events.iter().enumerate() {
        match e {
            HistEvt::Invoke { rid, op, .. } => {
                invoke_at.insert(*rid, (i, op.clone()));
            }
            HistEvt::Return { rid, ret, .. } => {
                return_at.insert(*rid, (i, *ret));
            }
        }
    }
    // The successes in log order.
    let mut successes: Vec<(Rid, (u64, usize))> = invoke_at
        .keys()
        .filter_map(|rid| completed_at.get(rid).map(|p| (*rid, *p)))
        .collect();
    successes.sort_by_key(|(_, p)| *p);
    let index_of: HashMap<Rid, usize> = successes
        .iter()
        .enumerate()
        .map(|(i, (rid, _))| (*rid, i))
        .collect();
    // 1. Real-time order among successes.
    for (a, ia) in &index_of {
        let Some((ret_a, _)) = return_at.get(a) else {
            continue;
        };
        if tentative.contains(a) {
            continue;
        }
        for (b, ib) in &index_of {
            let (inv_b, _) = &invoke_at[b];
            if ret_a < inv_b && ia > ib {
                return Err(format!(
                    "rid {a:?} returned before rid {b:?} was invoked but follows it in the log"
                ));
            }
        }
    }
    // 2. Values along the log order; the spec states after each success.
    let mut spec = NsSpec::default();
    let mut states: Vec<NsSpec> = vec![spec.clone()];
    for (rid, _) in &successes {
        let (_, op) = &invoke_at[rid];
        let ret = spec.invoke(op);
        if ret != NsRet::Ok && !tentative.contains(rid) {
            return Err(format!(
                "rid {rid:?} ({op:?}) took effect in the log but the spec refuses it there: {ret:?}"
            ));
        }
        states.push(spec.clone());
    }
    // 3. Refusals: some state in the window returns the same refusal.
    let mut witness = Witness::default();
    for (rid, (ret_i, ret)) in &return_at {
        if *ret == NsRet::Ok || tentative.contains(rid) || index_of.contains_key(rid) {
            continue;
        }
        let (inv_i, op) = &invoke_at[rid];
        // After every success that returned before this op was invoked
        // (a tentative op's return is not binding: it may have landed
        // through its replay, later in the log).
        let lo = index_of
            .iter()
            .filter(|(b, _)| {
                !tentative.contains(*b) && return_at.get(*b).is_some_and(|(r, _)| r < inv_i)
            })
            .map(|(_, ib)| ib + 1)
            .max()
            .unwrap_or(0);
        // Before every success invoked after this op returned.
        let hi = index_of
            .iter()
            .filter(|(b, _)| invoke_at[*b].0 > *ret_i)
            .map(|(_, ib)| *ib)
            .min()
            .unwrap_or(states.len() - 1);
        let hi = hi.min(states.len() - 1);
        let feasible = (lo..=hi).any(|i| states[i].clone().invoke(op) == *ret);
        if feasible {
            continue;
        }
        // Tentative effects the holder may have shown this op before they
        // were rolled back: every tentative op invoked before it returned,
        // in invocation order, alone or together.
        let mut early: Vec<(usize, &NsOp)> = invoke_at
            .iter()
            .filter(|(t, (inv_t, _))| tentative.contains(*t) && inv_t < ret_i)
            .map(|(_, (inv_t, op_t))| (*inv_t, op_t))
            .collect();
        early.sort_by_key(|(i, _)| *i);
        let explained = (lo..=hi).any(|i| {
            let all = {
                let mut st = states[i].clone();
                for (_, t) in &early {
                    st.invoke(t);
                }
                st.invoke(op) == *ret
            };
            all || early.iter().any(|(_, t)| {
                let mut st = states[i].clone();
                st.invoke(t);
                st.invoke(op) == *ret
            })
        });
        if explained {
            witness.observed_tentative += 1;
            continue;
        }
        return Err(format!(
            "rid {rid:?} ({op:?}) returned {ret:?} but no state in its window \
             (log positions {lo}..={hi}) refuses it that way, even with tentative \
             effects applied"
        ));
    }
    Ok(witness)
}

/// What the witnessed check saw besides a pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Witness {
    /// Refusals explained only by an acked-then-rolled-back effect the
    /// refusing holder still had at the time (the L2 window).
    pub observed_tentative: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(seq: u64) -> Rid {
        Rid {
            node: 1,
            incarnation: 1,
            seq,
        }
    }

    #[test]
    fn a_double_create_that_both_succeed_is_not_linearizable() {
        let events = vec![
            HistEvt::Invoke {
                thread: 1,
                rid: rid(1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 1,
                rid: rid(1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 2,
                rid: rid(2),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 2,
                rid: rid(2),
                ret: NsRet::Ok,
            },
        ];
        assert!(check_linearizable(&events, &HashSet::new()).is_err());
        // Marking the first tentative lets the checker drop it.
        let mut tentative = HashSet::new();
        tentative.insert(rid(1));
        assert!(check_linearizable(&events, &tentative).is_ok());
        // The witnessed check agrees: both in the log is a double create;
        // only the second in the log (the first dropped) is fine.
        let both = [(rid(1), (1, 0)), (rid(2), (2, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(&events, &HashSet::new(), &both).is_err());
        let second_only = [(rid(2), (2, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(&events, &tentative, &second_only).is_ok());
    }

    #[test]
    fn the_witnessed_check_places_refusals_in_their_window() {
        // t1: create a → Ok (log 1). t2: create a → Eexist, invoked after
        // t1 returned: feasible. t3: unlink b → Enoent, concurrent with
        // everything: feasible. t4: create a → Ok returned, absent from the
        // log: caught by the value rule (returned success never landed).
        let events = vec![
            HistEvt::Invoke {
                thread: 1,
                rid: rid(1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 1,
                rid: rid(1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 2,
                rid: rid(2),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Invoke {
                thread: 3,
                rid: rid(3),
                op: NsOp::Unlink("b".into()),
            },
            HistEvt::Return {
                thread: 2,
                rid: rid(2),
                ret: NsRet::Eexist,
            },
            HistEvt::Return {
                thread: 3,
                rid: rid(3),
                ret: NsRet::Enoent,
            },
        ];
        let log = [(rid(1), (1, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(&events, &HashSet::new(), &log).is_ok());
        // A refusal that no state in its window explains.
        let mut bad = events.clone();
        bad[4] = HistEvt::Return {
            thread: 2,
            rid: rid(2),
            ret: NsRet::Enoent,
        };
        assert!(check_linearizable_witnessed(&bad, &HashSet::new(), &log).is_err());
        // A tentative success whose original `Ok` came before t2's
        // invocation but whose replay landed late does not bound t2's
        // window: t2's `Eexist` needs a state with `a` present, and the
        // log has it at position 0 — before the tentative op landed.
        let mut tentative = HashSet::new();
        tentative.insert(rid(1));
        let with_a = vec![
            HistEvt::Invoke {
                thread: 9,
                rid: rid(9),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 9,
                rid: rid(9),
                ret: NsRet::Ok,
            },
        ];
        let mut events2 = with_a;
        events2.extend(events.clone());
        let log2 = [(rid(9), (1, 0)), (rid(1), (5, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(&events2, &tentative, &log2).is_ok());
    }
}
