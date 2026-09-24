//! Plan 30 §M8: close-to-open over a simulated run's history, with the log
//! as the witness of the version order.
//!
//! The successes in log order (`Completed { rid }` positions) fix every
//! name's version sequence (as in `session.rs`). A read of name `x` that
//! was *invoked* after a write of `x` — from any node — *returned* `Ok`
//! must be explained by the state that write produced or a later one: its
//! floor is the write's log index + 1, and a read whose value no state at
//! or after the floor has is a stale read. Writes that were acked and then
//! rolled back (M13's `Tentative`) owe nothing; reads that answered
//! degraded (the ReadIndex or the session wait ran out) are counted, not
//! failed — both exactly as the session checker treats them.
//!
//! This is stronger than M6's per-node guarantees (a session only owes
//! its own writes and observations) and is what `cto=strict` promises: a
//! read that starts after another node's close completed sees it. Bounded
//! mode violates it by design (`bounded_mode_reads_stale`, the sim's
//! counterexample, like the model's).

use super::history::{HistEvt, NsOp, NsRet, ReadEvt};
use constellation_meta::Rid;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Clone)]
pub struct CtoReport {
    pub reads: usize,
    /// Reads that some completed foreign write constrained.
    pub constrained: usize,
    pub degraded: usize,
    /// Stale reads explained by a tentative op on the name.
    pub tentative: usize,
    /// Stale reads that answered degraded.
    pub degraded_violations: usize,
    pub violations: Vec<String>,
}

fn names_of(op: &NsOp) -> Vec<&str> {
    match op {
        NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => vec![n.as_str()],
        NsOp::Rename(a, b) => vec![a.as_str(), b.as_str()],
    }
}

pub fn check_cto(
    events: &[HistEvt],
    ticks: &[u64],
    reads: &[ReadEvt],
    tentative: &HashSet<Rid>,
    completed_at: &HashMap<Rid, (u64, usize)>,
) -> CtoReport {
    let mut report = CtoReport {
        reads: reads.len(),
        degraded: reads.iter().filter(|r| r.timed_out).count(),
        ..Default::default()
    };
    let mut invoke: HashMap<Rid, (u64, NsOp)> = HashMap::new();
    let mut ret: HashMap<Rid, (u64, NsRet)> = HashMap::new();
    for (e, t) in events.iter().zip(ticks) {
        match e {
            HistEvt::Invoke { rid, op, .. } => {
                invoke.insert(*rid, (*t, op.clone()));
            }
            HistEvt::Return { rid, ret: r, .. } => {
                ret.insert(*rid, (*t, *r));
            }
        }
    }
    let mut successes: Vec<(Rid, (u64, usize))> = invoke
        .keys()
        .filter_map(|rid| completed_at.get(rid).map(|p| (*rid, *p)))
        .collect();
    successes.sort_by_key(|(_, p)| *p);
    let mut names: Vec<String> = reads.iter().map(|r| r.name.clone()).collect();
    names.sort();
    names.dedup();
    let mut presence: HashMap<&str, Vec<bool>> = HashMap::new();
    for n in &names {
        let mut v = vec![false];
        let mut cur = false;
        for (rid, _) in &successes {
            match &invoke[rid].1 {
                NsOp::Create(x) | NsOp::Put(x) if x == n => cur = true,
                NsOp::Unlink(x) if x == n => cur = false,
                NsOp::Rename(a, b) => {
                    if a == n {
                        cur = false;
                    }
                    if b == n {
                        cur = true;
                    }
                }
                _ => {}
            }
            v.push(cur);
        }
        presence.insert(n.as_str(), v);
    }
    let mut tentative_names: HashSet<&str> = HashSet::new();
    for rid in tentative {
        if let Some((_, op)) = invoke.get(rid) {
            tentative_names.extend(names_of(op));
        }
    }
    for r in reads {
        // The floor: the latest (in log order) write of the name whose
        // client had its `Ok` before this read began.
        let floor = successes
            .iter()
            .enumerate()
            .filter(|(_, (rid, _))| {
                !tentative.contains(rid)
                    && ret
                        .get(rid)
                        .is_some_and(|(t, x)| *x == NsRet::Ok && *t < r.inv)
                    && names_of(&invoke[rid].1).contains(&r.name.as_str())
            })
            .map(|(k, (rid, _))| (k + 1, *rid))
            .max_by_key(|(k, _)| *k);
        let Some((floor, rid)) = floor else {
            continue;
        };
        report.constrained += 1;
        let states = &presence[r.name.as_str()];
        if states[floor..].contains(&r.present) {
            continue;
        }
        if tentative_names.contains(r.name.as_str()) {
            report.tentative += 1;
            continue;
        }
        if r.timed_out {
            report.degraded_violations += 1;
            continue;
        }
        report.violations.push(format!(
            "node {} read {:?} as {} at tick {}, but {:?} (rid {:?}, from node {}) \
             completed before the read began and every state from it on has {}",
            r.node,
            r.name,
            if r.present { "present" } else { "absent" },
            r.inv,
            invoke[&rid].1,
            rid,
            rid.node,
            if r.present { "it absent" } else { "it present" },
        ));
    }
    report
}
