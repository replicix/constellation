//! Plan 46 M1.2 step 3: `acked_at_or_below_cut`, the sim's form of the
//! model's property of that name (`crates/model/src/node_stream.rs`): every
//! answered row of a delegate stream is at or below the stream's cut. The
//! cut is what the log records: a `RecallCut { gen, through }` ends the
//! generation and voids every row of it past `through` ("never in the log,
//! and never to be acknowledged", `record.rs`). An answer whose transaction
//! has the origin `(gen, idx)` with `idx > through` was acknowledged for a
//! row the log will never carry.
//!
//! The exemption is the model's: the operator's abandon (the log carries a
//! `GroupAbandoned` of the generation) is the one end that may leave an
//! acknowledged row past the cut, because the abandon exports the rid
//! (I5's operator branch); such a rid is counted apart, not as a violation.
//!
//! Two modes ([`CutMode`]). On main the oracle runs in `Report`: the count
//! is in `Report::acked_past_cut` and the sweep totals, no run fails.
//! M1.2 step 16 turns it `Strict` for every configuration by changing
//! [`CutMode::default`]; a configuration or a sweep (`+cut`) can ask for
//! `Strict` before then.

use super::oracle::{AckEntry, OutcomeKind};
use constellation_meta::{LogRecord, Rid};
use std::collections::{BTreeMap, BTreeSet};

/// Whether a count of rows acknowledged past their cut fails the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutMode {
    /// Counted in the report, no failure.
    Report,
    /// A single acknowledged row past its cut fails the run.
    Strict,
}

impl Default for CutMode {
    /// Report on main; M1.2 step 16 makes this `Strict`.
    fn default() -> Self {
        CutMode::Report
    }
}

impl CutMode {
    pub fn is_strict(self) -> bool {
        self == CutMode::Strict
    }
}

/// What the log says about the ends of delegate streams.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamCuts {
    /// Generation -> the lowest `through` any `RecallCut` of it carries.
    pub cut: BTreeMap<u64, u64>,
    /// Generations an operator's `GroupAbandoned` names.
    pub abandoned: BTreeSet<u64>,
}

impl StreamCuts {
    pub fn of_records<'a>(records: impl IntoIterator<Item = &'a LogRecord>) -> Self {
        let mut c = StreamCuts::default();
        for r in records {
            match r {
                LogRecord::RecallCut { gen, through, .. } => {
                    let t = c.cut.entry(*gen).or_insert(*through);
                    *t = (*t).min(*through);
                }
                LogRecord::GroupAbandoned { gen, .. } if *gen != 0 => {
                    c.abandoned.insert(*gen);
                }
                _ => {}
            }
        }
        c
    }

    pub fn of_log(log: &super::check::Oracle) -> Self {
        Self::of_records(log.unfenced.values().flat_map(|s| s.records.iter()))
    }
}

/// What [`judge`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CutFindings {
    /// Answered rows of a stream that has a cut, judged.
    pub judged: usize,
    /// One description per rid answered for a row past its stream's cut.
    pub past_cut: Vec<String>,
    /// Of those, rids whose answer was a refusal (`EEXIST`, `ENOENT`, ...).
    pub refusals: usize,
    /// Those the operator's abandon excuses (counted, not judged).
    pub abandoned: usize,
}

impl CutFindings {
    /// Whether `mode` fails a run with these findings (`run.rs` fails the
    /// run on the first of `past_cut` in `Strict`).
    pub fn fails(&self, mode: CutMode) -> bool {
        mode.is_strict() && !self.past_cut.is_empty()
    }
}

/// Every non-relay answer whose transaction is a delegate stream row
/// (`origin.0 != 0`) is at or below the cut its stream's `RecallCut` gave.
pub fn judge(acks: &[AckEntry], cuts: &StreamCuts) -> CutFindings {
    let mut f = CutFindings::default();
    let mut seen: BTreeSet<Rid> = BTreeSet::new();
    for a in acks.iter().filter(|a| !a.is_relay()) {
        let Some(t) = a.judged.as_ref().and_then(|j| j.tx.as_ref()) else {
            continue;
        };
        let (gen, idx) = t.origin;
        if gen == 0 {
            continue;
        }
        let Some(through) = cuts.cut.get(&gen).copied() else {
            continue;
        };
        f.judged += 1;
        if idx <= through || !seen.insert(a.rid) {
            continue;
        }
        if cuts.abandoned.contains(&gen) {
            f.abandoned += 1;
            continue;
        }
        f.refusals += usize::from(a.kind != OutcomeKind::Ok);
        f.past_cut.push(format!(
            "acked_at_or_below_cut: rid {:?} answered {} by node {} at t={} ms (tick {}) for \
             row {idx} of generation {gen}, past the stream's cut {through}",
            a.rid,
            a.kind.name(),
            a.node,
            a.at_ms,
            a.tick
        ));
    }
    f
}

#[cfg(test)]
mod tests {
    use super::super::oracle::{Expected, Judgement, TxRef};
    use super::*;

    fn rid(seq: u64) -> Rid {
        Rid {
            node: 2,
            incarnation: 1,
            seq,
        }
    }

    /// An OK answer of `node` for the row `(gen, idx)` of a delegate stream.
    fn ack(r: Rid, node: u64, gen: u64, idx: u64) -> AckEntry {
        AckEntry {
            node,
            rid: r,
            tick: idx,
            at_ms: 10 * idx,
            kind: OutcomeKind::Ok,
            reply: false,
            relay_from: None,
            claimed: None,
            judged: Some(Judgement {
                class: None,
                expected: Expected::Unjournaled,
                tx: Some(TxRef {
                    node,
                    first: idx,
                    last: idx,
                    origin: (gen, idx),
                    epoch: 1,
                    records: Vec::new(),
                }),
                detail: String::new(),
                mismatch: None,
            }),
        }
    }

    fn cut(gen: u64, through: u64) -> LogRecord {
        LogRecord::RecallCut {
            dir: 10 + gen,
            gen,
            through,
        }
    }

    #[test]
    fn rows_at_or_below_the_cut_pass() {
        let cuts = StreamCuts::of_records(&[cut(7, 5)]);
        let acks = vec![ack(rid(1), 2, 7, 1), ack(rid(2), 2, 7, 5)];
        let f = judge(&acks, &cuts);
        assert_eq!(f.judged, 2);
        assert!(f.past_cut.is_empty(), "{:?}", f.past_cut);
    }

    /// The oracle's own negative control: an acknowledged row past the cut
    /// is a violation, and in strict mode it is the one that fails a run.
    #[test]
    fn an_acknowledged_row_past_the_cut_is_a_violation() {
        let cuts = StreamCuts::of_records(&[cut(7, 5)]);
        let acks = vec![ack(rid(1), 2, 7, 5), ack(rid(2), 2, 7, 6)];
        let f = judge(&acks, &cuts);
        assert_eq!(f.past_cut.len(), 1, "{:?}", f.past_cut);
        assert!(f.past_cut[0].contains("row 6 of generation 7"));
        assert_eq!(f.refusals, 0);
        assert!(f.fails(CutMode::Strict), "strict mode must fail the run");
        assert!(!f.fails(CutMode::Report), "report mode only counts");
        assert_eq!(CutMode::default(), CutMode::Report);
    }

    #[test]
    fn a_refusal_past_the_cut_is_counted_apart() {
        let cuts = StreamCuts::of_records(&[cut(7, 5)]);
        let mut refused = ack(rid(2), 2, 7, 7);
        refused.kind = OutcomeKind::Exists;
        let f = judge(&[ack(rid(1), 2, 7, 6), refused], &cuts);
        assert_eq!((f.past_cut.len(), f.refusals), (2, 1));
    }

    #[test]
    fn the_lowest_cut_of_a_generation_counts_and_other_generations_are_independent() {
        let cuts = StreamCuts::of_records(&[cut(7, 5), cut(7, 3), cut(8, 9)]);
        let acks = vec![ack(rid(1), 2, 7, 4), ack(rid(2), 2, 8, 9)];
        let f = judge(&acks, &cuts);
        assert_eq!(f.past_cut.len(), 1);
        assert!(f.past_cut[0].contains("generation 7"));
    }

    #[test]
    fn a_stream_with_no_cut_and_the_roots_own_rows_are_not_judged() {
        let cuts = StreamCuts::of_records(&[cut(7, 5)]);
        let acks = vec![ack(rid(1), 2, 8, 100), ack(rid(2), 1, 0, 100)];
        let f = judge(&acks, &cuts);
        assert_eq!(f.judged, 0);
        assert!(f.past_cut.is_empty());
    }

    #[test]
    fn an_operators_abandon_excuses_the_row_and_is_counted() {
        let cuts = StreamCuts::of_records(&[
            cut(7, 5),
            LogRecord::GroupAbandoned {
                epoch: 3,
                gen: 7,
                through_known: 6,
            },
        ]);
        let f = judge(&[ack(rid(1), 2, 7, 6)], &cuts);
        assert!(f.past_cut.is_empty());
        assert_eq!(f.abandoned, 1);
    }

    #[test]
    fn a_rid_answered_twice_is_one_violation() {
        let cuts = StreamCuts::of_records(&[cut(7, 5)]);
        let f = judge(&[ack(rid(1), 2, 7, 6), ack(rid(1), 3, 7, 6)], &cuts);
        assert_eq!(f.past_cut.len(), 1);
    }
}
