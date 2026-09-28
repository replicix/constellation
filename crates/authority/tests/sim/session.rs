//! Plan 30 §M6: per-node read-your-writes and monotonic reads over a
//! simulated run's history, with the log as the witness of the version
//! order.
//!
//! The successes in log order (`Completed { rid }` positions) fix every
//! name's version sequence: after `k` successes, name `x` is present or
//! not. A session is one node incarnation (a restart is a new FUSE
//! session). Each observation the session makes constrains, per name,
//! how far along that sequence a later read of the name must be:
//! - its own write that returned `Ok` (not tentative) at log index `p`:
//!   at least `p + 1` (read-your-writes);
//! - a refusal (`EEXIST` — present, `ENOENT` — absent; its op never
//!   landed): at least the earliest state in its real-time window that
//!   refuses the same way (the holder evaluated it there);
//! - a read: at least the earliest state at or after its own floor that
//!   shows what it returned (monotonic reads).
//!
//! A read invoked after an observation returned must be explained by a
//! state at or after that floor; if no such state shows what it returned,
//! that is a violation — read-your-writes when the binding floor came from
//! the session's own write, monotonic reads otherwise. Violations on a
//! name some tentative op (acked, then rolled back — plan 30 M13's rule)
//! touched are counted, not failed: nothing may be required to see such
//! an effect, or to keep seeing it. A read whose session wait timed out
//! answered degraded by design; its violations are counted separately.

use super::history::{HistEvt, NsOp, NsRet, ReadEvt};
use constellation_meta::Rid;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Default, Clone)]
pub struct SessionReport {
    pub reads: usize,
    /// Reads whose wait timed out (answered degraded).
    pub degraded: usize,
    /// Violations explained by a tentative op on the name.
    pub tentative: usize,
    /// Violations of reads that timed out.
    pub degraded_violations: usize,
    /// Real violations: (checker, description).
    pub violations: Vec<(String, String)>,
}

fn names_of(op: &NsOp) -> Vec<&str> {
    match op {
        NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => vec![n.as_str()],
        NsOp::Rename(a, b) => vec![a.as_str(), b.as_str()],
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Own,
    Observed,
    /// Set by observing another node's write: everything that node had
    /// been acknowledged before it issued that write (EC2 campaign 7,
    /// finding B-1: git's objects before the ref that names them).
    Causal,
}

pub fn check_sessions(
    events: &[HistEvt],
    ticks: &[u64],
    reads: &[ReadEvt],
    tentative: &HashSet<Rid>,
    completed_at: &HashMap<Rid, (u64, usize)>,
) -> SessionReport {
    let mut report = SessionReport {
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
    // The successes in log order, and every name's presence after each.
    let mut successes: Vec<(Rid, (u64, usize))> = invoke
        .keys()
        .filter_map(|rid| completed_at.get(rid).map(|p| (*rid, *p)))
        .collect();
    successes.sort_by_key(|(_, p)| *p);
    let index_of: HashMap<Rid, usize> = successes
        .iter()
        .enumerate()
        .map(|(i, (rid, _))| (*rid, i))
        .collect();
    let mut names: Vec<String> = reads.iter().map(|r| r.name.clone()).collect();
    for (_, op) in invoke.values() {
        names.extend(names_of(op).into_iter().map(String::from));
    }
    names.sort();
    names.dedup();
    let mut presence: HashMap<String, Vec<bool>> = HashMap::new();
    // Per name and state, the success that last touched it: the op a
    // read of the name in that state observed.
    let mut touched: HashMap<String, Vec<Option<Rid>>> = HashMap::new();
    for n in &names {
        let mut v = vec![false];
        let mut by = vec![None];
        let mut cur = false;
        let mut last = None;
        for (rid, _) in &successes {
            let (_, op) = &invoke[rid];
            let hit = match op {
                NsOp::Create(x) | NsOp::Put(x) if x == n => {
                    cur = true;
                    true
                }
                NsOp::Unlink(x) if x == n => {
                    cur = false;
                    true
                }
                NsOp::Rename(a, b) => {
                    let mut hit = false;
                    if a == n {
                        cur = false;
                        hit = true;
                    }
                    if b == n {
                        cur = true;
                        hit = true;
                    }
                    hit
                }
                _ => false,
            };
            if hit {
                last = Some(*rid);
            }
            v.push(cur);
            by.push(last);
        }
        presence.insert(n.clone(), v);
        touched.insert(n.clone(), by);
    }
    let first_at_or_after = |name: &str, from: usize, want: bool| -> Option<usize> {
        let v = presence.get(name)?;
        (from..v.len()).find(|k| v[*k] == want)
    };
    // Names a tentative op touched, with the tick it was invoked at.
    let mut tentative_names: HashMap<String, u64> = HashMap::new();
    for rid in tentative {
        if let Some((t, op)) = invoke.get(rid) {
            for n in names_of(op) {
                let e = tentative_names.entry(n.to_string()).or_insert(*t);
                *e = (*e).min(*t);
            }
        }
    }
    let n_states = successes.len();
    // Causal floors (EC2 campaign 7, finding B-1). A session that
    // observed `name` as `want` in some state of `lo..=hi` observed the
    // write that last set it there — when that is one op `b` across
    // every such state (else nothing is attributed). `b`'s node had, by
    // the time it issued `b`, been acknowledged every earlier op of its
    // own that returned before `b` was invoked: they precede `b` in the
    // log (a delegate waits for the requester's `deps`; the root parks
    // on the delegate streams they name), and whatever installs `b`
    // ahead of the log (a contiguous streamed prefix, the delegate's own
    // execution after the same wait, a refusal's position the reader
    // waits for) carries them too. So the observer's later reads of
    // their names must reflect them: one floor per name, at the index
    // after the latest such op. The observer's own writes are covered
    // by read-your-writes; tentative ops are exempt.
    let causal_floors = |name: &str,
                         lo: usize,
                         hi: usize,
                         want: bool,
                         observer: (u64, u32)|
     -> Vec<(String, usize, String)> {
        let (Some(p), Some(t)) = (presence.get(name), touched.get(name)) else {
            return Vec::new();
        };
        let hi = hi.min(p.len() - 1);
        let mut setter: Option<Rid> = None;
        for s in lo..=hi {
            if p[s] != want {
                continue;
            }
            match (setter, t[s]) {
                // The initial state: nothing was observed.
                (_, None) => return Vec::new(),
                (None, Some(b)) => setter = Some(b),
                (Some(a), Some(b)) if a != b => return Vec::new(),
                _ => {}
            }
        }
        let Some(b) = setter else {
            return Vec::new();
        };
        if tentative.contains(&b) || (b.node, b.incarnation) == observer {
            return Vec::new();
        }
        let Some((inv_b, op_b)) = invoke.get(&b) else {
            return Vec::new();
        };
        let mut floors: BTreeMap<String, usize> = BTreeMap::new();
        for (a, (ret_a, r_a)) in &ret {
            if *r_a != NsRet::Ok
                || (a.node, a.incarnation) != (b.node, b.incarnation)
                || ret_a >= inv_b
                || tentative.contains(a)
            {
                continue;
            }
            let Some(ia) = index_of.get(a) else {
                continue;
            };
            let (_, op_a) = &invoke[a];
            for n in names_of(op_a) {
                let e = floors.entry(n.to_string()).or_insert(0);
                *e = (*e).max(ia + 1);
            }
        }
        floors
            .into_iter()
            .map(|(n, f)| {
                let by = format!(
                    "causal: {name} seen as {want} was set by {op_b:?} rid {b:?}, and its node had been acknowledged {n} (log index {}) before issuing it",
                    f - 1
                );
                (n, f, by)
            })
            .collect()
    };

    // Per session: (return tick, name, floor, kind, what set it).
    type Session = (u64, u32);
    /// (return tick, name, floor, kind, what set it)
    type Obs = (u64, String, usize, Kind, String);
    let mut obs: BTreeMap<Session, Vec<Obs>> = BTreeMap::new();
    for (rid, (rt, r)) in &ret {
        let Some((it, op)) = invoke.get(rid) else {
            continue;
        };
        if tentative.contains(rid) {
            continue;
        }
        let session = (rid.node, rid.incarnation);
        match (r, index_of.get(rid)) {
            (NsRet::Ok, Some(p)) => {
                for n in names_of(op) {
                    obs.entry(session).or_default().push((
                        *rt,
                        n.to_string(),
                        p + 1,
                        Kind::Own,
                        format!("own {op:?} rid {rid:?}"),
                    ));
                }
            }
            (NsRet::Eexist | NsRet::Enoent, None) => {
                let (name, want) = match (op, r) {
                    (NsOp::Create(n), NsRet::Eexist) => (n.as_str(), true),
                    (NsOp::Unlink(n), NsRet::Enoent) | (NsOp::Rename(n, _), NsRet::Enoent) => {
                        (n.as_str(), false)
                    }
                    (NsOp::Rename(_, _), NsRet::Eexist) => continue,
                    _ => continue,
                };
                // The refusal's window, as the witnessed check places it.
                let lo = index_of
                    .iter()
                    .filter(|(b, _)| {
                        !tentative.contains(*b) && ret.get(*b).is_some_and(|(t, _)| t < it)
                    })
                    .map(|(_, ib)| ib + 1)
                    .max()
                    .unwrap_or(0);
                let hi = index_of
                    .iter()
                    .filter(|(b, _)| invoke[*b].0 > *rt)
                    .map(|(_, ib)| *ib)
                    .min()
                    .unwrap_or(n_states)
                    .min(n_states);
                if let Some(k) = first_at_or_after(name, lo, want).filter(|k| *k <= hi) {
                    let causal = causal_floors(name, k, hi, want, session);
                    let list = obs.entry(session).or_default();
                    list.push((
                        *rt,
                        name.to_string(),
                        k,
                        Kind::Observed,
                        format!("refusal {op:?} -> {r:?} rid {rid:?}"),
                    ));
                    for (n, f, by) in causal {
                        list.push((*rt, n, f, Kind::Causal, by));
                    }
                }
            }
            _ => {}
        }
    }
    // Reads, in return order per session, each checked against the
    // observations that returned before it was invoked, then added as one.
    let mut by_session: BTreeMap<Session, Vec<&ReadEvt>> = BTreeMap::new();
    for r in reads {
        by_session
            .entry((r.node, r.incarnation))
            .or_default()
            .push(r);
    }
    for (session, mut rs) in by_session {
        rs.sort_by_key(|r| r.ret);
        for r in rs {
            let list = obs.entry(session).or_default();
            let binding = list
                .iter()
                .filter(|(t, n, ..)| *t < r.inv && *n == r.name)
                .max_by_key(|(_, _, k, ..)| *k)
                .cloned();
            let floor = binding.as_ref().map(|b| b.2).unwrap_or(0);
            match first_at_or_after(&r.name, floor, r.present) {
                // A degraded read (its session wait timed out: the state
                // it was owed never arrived, plan 30 §M6) is answered from
                // whatever the replica has; it binds no later read — it
                // would otherwise be "explained" by a far-future state and
                // make the next, fresh read look non-monotonic.
                Some(_) if r.timed_out => {}
                Some(k) => {
                    // The latest state the read can have seen: before
                    // every success invoked after it returned.
                    let hi = index_of
                        .iter()
                        .filter(|(b, _)| invoke[*b].0 > r.ret)
                        .map(|(_, ib)| *ib)
                        .min()
                        .unwrap_or(n_states);
                    let causal = causal_floors(&r.name, k, hi, r.present, session);
                    list.push((
                        r.ret,
                        r.name.clone(),
                        k,
                        Kind::Observed,
                        format!("read of {} by t{} -> {}", r.name, r.thread, r.present),
                    ));
                    for (n, f, by) in causal {
                        list.push((r.ret, n, f, Kind::Causal, by));
                    }
                }
                None => {
                    let tentative_name = tentative_names.get(&r.name).is_some_and(|t| *t < r.ret);
                    let Some((_, _, _, kind, by)) = binding else {
                        // No log state ever showed it: the read saw an
                        // effect that never landed.
                        if tentative_name {
                            report.tentative += 1;
                        } else {
                            report.violations.push((
                                "phantom_read".to_string(),
                                format!(
                                    "node {} t{} read {} as {} but no log state shows that",
                                    r.node, r.thread, r.name, r.present
                                ),
                            ));
                        }
                        continue;
                    };
                    let chain: Vec<String> = list
                        .iter()
                        .filter(|(_, n, ..)| *n == r.name)
                        .map(|(t, _, k, kind, by)| format!("[tick {t} index {k} {kind:?}: {by}]"))
                        .collect();
                    let what = format!(
                        "node {} (incarnation {}) t{} read {} as {} at tick {}, but it had \
                         already observed a later state (log index {floor}, set by {by}); \
                         observations of {}: {}",
                        r.node,
                        r.incarnation,
                        r.thread,
                        r.name,
                        if r.present { "present" } else { "absent" },
                        r.inv,
                        r.name,
                        chain.join(" "),
                    );
                    if tentative_name {
                        report.tentative += 1;
                    } else if r.timed_out {
                        report.degraded_violations += 1;
                    } else {
                        let checker = match kind {
                            Kind::Own => "read_your_writes",
                            Kind::Observed => "monotonic_reads",
                            Kind::Causal => "causal_order",
                        };
                        report.violations.push((checker.to_string(), what));
                    }
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(node: u64, seq: u64) -> Rid {
        Rid {
            node,
            incarnation: 1,
            seq,
        }
    }

    fn read(node: u64, name: &str, present: bool, inv: u64, ret: u64) -> ReadEvt {
        ReadEvt {
            node,
            incarnation: 1,
            thread: node * 16,
            name: name.into(),
            present,
            inv,
            ret,
            timed_out: false,
        }
    }

    /// Node 1 creates `a` (log 0); node 2's create is refused `EEXIST`;
    /// node 2 then reads `a` absent: a monotonic-reads violation. Node 1
    /// reading it absent after its own create: read-your-writes.
    #[test]
    fn finds_both_violations_and_accepts_a_clean_history() {
        let events = vec![
            HistEvt::Invoke {
                thread: 16,
                rid: rid(1, 1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 16,
                rid: rid(1, 1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 32,
                rid: rid(2, 1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 32,
                rid: rid(2, 1),
                ret: NsRet::Eexist,
            },
        ];
        let ticks = vec![0, 1, 2, 3];
        let log: HashMap<Rid, (u64, usize)> = [(rid(1, 1), (1, 0))].into_iter().collect();
        let none = HashSet::new();
        let bad = vec![read(2, "a", false, 4, 5), read(1, "a", false, 6, 7)];
        let r = check_sessions(&events, &ticks, &bad, &none, &log);
        let mut kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        kinds.sort();
        assert_eq!(kinds, vec!["monotonic_reads", "read_your_writes"], "{r:?}");
        // Reading `a` absent *before* the refusal is fine (cross-node
        // staleness), and present afterwards.
        let good = vec![read(2, "a", false, 2, 2), read(2, "a", true, 4, 5)];
        let r = check_sessions(&events, &ticks, &good, &none, &log);
        assert!(r.violations.is_empty(), "{r:?}");
        // A tentative create exempts the name.
        let tentative: HashSet<Rid> = [rid(1, 1)].into_iter().collect();
        let r = check_sessions(&events, &ticks, &bad, &tentative, &HashMap::new());
        assert!(r.violations.is_empty(), "{r:?}");
    }

    /// Campaign 7's B-1 shape: node 1 creates `d1/a` (an object), and
    /// once that returned, `d2/b` (the ref naming it) — different
    /// directories, possibly different owners. Node 2 that read `d2/b`
    /// present must read `d1/a` present afterwards; reading `d1/a`
    /// absent before it saw `d2/b` is plain staleness.
    #[test]
    fn observing_a_write_carries_what_its_node_wrote_before() {
        let events = vec![
            HistEvt::Invoke {
                thread: 16,
                rid: rid(1, 1),
                op: NsOp::Create("d1/a".into()),
            },
            HistEvt::Return {
                thread: 16,
                rid: rid(1, 1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 16,
                rid: rid(1, 2),
                op: NsOp::Create("d2/b".into()),
            },
            HistEvt::Return {
                thread: 16,
                rid: rid(1, 2),
                ret: NsRet::Ok,
            },
        ];
        let ticks = vec![0, 1, 2, 3];
        let log: HashMap<Rid, (u64, usize)> = [(rid(1, 1), (1, 0)), (rid(1, 2), (1, 1))]
            .into_iter()
            .collect();
        let none = HashSet::new();
        let bad = vec![read(2, "d2/b", true, 4, 5), read(2, "d1/a", false, 6, 7)];
        let r = check_sessions(&events, &ticks, &bad, &none, &log);
        let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, vec!["causal_order"], "{r:?}");
        let good = vec![
            read(2, "d1/a", false, 2, 3),
            read(2, "d2/b", true, 4, 5),
            read(2, "d1/a", true, 6, 7),
        ];
        let r = check_sessions(&events, &ticks, &good, &none, &log);
        assert!(r.violations.is_empty(), "{r:?}");
        // The two creates in flight together: `d2/b` carries nothing.
        let overlapping = vec![
            events[0].clone(),
            events[2].clone(),
            events[1].clone(),
            events[3].clone(),
        ];
        let r = check_sessions(&overlapping, &ticks, &bad, &none, &log);
        assert!(r.violations.is_empty(), "{r:?}");
        // A tentative `d1/a` carries nothing either.
        let tentative: HashSet<Rid> = [rid(1, 1)].into_iter().collect();
        let r = check_sessions(&events, &ticks, &bad, &tentative, &log);
        assert!(r.violations.is_empty(), "{r:?}");
    }
}
