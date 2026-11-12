//! Plan 46 M1.3 step 4: `sticky_ends_only_by_holder_or_operator`, the sim's
//! form of the model's property (`crates/model/src/node_stream.rs`, chunk
//! C7): a sticky grant's stream ends only by the holder's release or an
//! operator's action. Two parts.
//!
//! The log part (this module): a sticky stream that ended must show the
//! holder's release (a `SubtreeReleased` for each subtree it was granted, or
//! one `StreamReleased`, in its own stream before the root's
//! `DelegateNodeEnded`; or a release note the sim saw it send that covers
//! every granted subtree, or carries `end_at`: a rowless release leaves
//! nothing in the log), the holder's own claim of the root lease (claim =
//! release, C11), or the operator's `GroupAbandoned` of the generation.
//! Otherwise it ended by something else (the TTL, a placement round): the
//! violation. This part cannot tell WHY a holder released: a holder that
//! releases because the root recalled it is excused here like any release.
//!
//! The recall part (`run.rs`, the core's `deleg_sticky_recalled_other`): the
//! root never starts a recall of a sticky stream (whole, or for some of its
//! subtrees) except for the operator's `undelegate` and a continuation
//! epoch's (`recall_all`, the one exception of plan 46 §5.4: the hold
//! sequences and the delegates stop). Any other cause is the violation, the
//! release it provokes excused or not. The cooperative handover of §5.4 is
//! not built yet; its cause joins the allowed ones with it.
//! Both parts are strict on every configuration; with no sticky grant
//! nothing is judged.

use constellation_meta::{LogRecord, SubtreeKind};
use std::collections::{BTreeMap, BTreeSet};

/// What a holder's release notes on one stream said, as the sim saw them
/// leave the node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SentRelease {
    /// A note carried `end_at`: the stream ends whole.
    pub whole: bool,
    /// The subtrees the notes named.
    pub dirs: BTreeSet<u64>,
}

/// What the log says about sticky streams.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StickyEnds {
    /// Generation -> the subtrees granted to it as sticky.
    granted: BTreeMap<u64, BTreeSet<u64>>,
    /// Generation -> the subtrees the holder released.
    released: BTreeMap<u64, BTreeSet<u64>>,
    /// Generations the holder released whole.
    released_whole: BTreeSet<u64>,
    /// Generations an operator's `GroupAbandoned` names.
    abandoned: BTreeSet<u64>,
    /// Generation -> the node that holds it.
    holder: BTreeMap<u64, u64>,
    /// Generation -> the node whose segment carried its end.
    ended: BTreeMap<u64, u64>,
    /// Generation -> its records in the order the log gives them (for the
    /// violation's message).
    trail: BTreeMap<u64, Vec<String>>,
    /// ADR-55 (R-I1): `DelegateNode` records that name `/`: `(gen, node,
    /// kind)`, any kind.
    root_grants: Vec<(u64, u64, String)>,
}

/// What [`StickyEnds::judge`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StickyFindings {
    /// Sticky streams the log carries.
    pub granted: usize,
    /// Of them, ended by the holder's release.
    pub ended_released: usize,
    /// Of them, ended by the operator's abandon.
    pub ended_abandoned: usize,
    /// Of them, ended by the holder itself as the root (it claimed the lease).
    pub ended_claimed: usize,
    /// One description per sticky stream that ended otherwise.
    pub violations: Vec<String>,
}

impl StickyEnds {
    /// `records`: each record with the node whose segment carries it.
    pub fn of_records<'a>(records: impl IntoIterator<Item = (u64, &'a LogRecord)>) -> Self {
        let mut s = StickyEnds::default();
        for (writer, r) in records {
            if let Some((_, gen)) = r.delegation_stream() {
                s.trail.entry(gen).or_default().push(match r {
                    LogRecord::DelegateNode { .. } => "DelegateNode".to_string(),
                    LogRecord::SubtreeReleased { dir, at, .. } => {
                        format!("SubtreeReleased(dir {dir} at {at})")
                    }
                    LogRecord::StreamReleased { through, .. } => {
                        format!("StreamReleased(through {through})")
                    }
                    LogRecord::DelegateNodeEnded { through, .. } => {
                        format!("DelegateNodeEnded(through {through})")
                    }
                    _ => "other".to_string(),
                });
            }
            match r {
                LogRecord::DelegateNode { node, gen, dirs } => {
                    for g in dirs
                        .iter()
                        .filter(|g| g.dir == constellation_fs_core::types::ROOT_INO)
                    {
                        s.root_grants.push((*gen, *node, format!("{:?}", g.kind)));
                    }
                    for g in dirs.iter().filter(|g| g.kind == SubtreeKind::Sticky) {
                        s.granted.entry(*gen).or_default().insert(g.dir);
                        s.holder.insert(*gen, *node);
                    }
                }
                LogRecord::SubtreeReleased { gen, dir, .. } => {
                    s.released.entry(*gen).or_default().insert(*dir);
                }
                LogRecord::StreamReleased { gen, .. } => {
                    s.released_whole.insert(*gen);
                }
                LogRecord::GroupAbandoned { gen, .. } if *gen != 0 => {
                    s.abandoned.insert(*gen);
                }
                LogRecord::DelegateNodeEnded { gen, .. } => {
                    s.ended.entry(*gen).or_insert(writer);
                }
                _ => {}
            }
        }
        s
    }

    pub fn of_log(log: &super::check::Oracle) -> Self {
        Self::of_records(
            log.unfenced
                .values()
                .flat_map(|s| s.records.iter().map(|r| (s.node, r))),
        )
    }

    pub fn judge(&self, holder_sent_release: &BTreeMap<u64, SentRelease>) -> StickyFindings {
        let mut f = StickyFindings {
            granted: self.granted.len(),
            ..Default::default()
        };
        // ADR-55, `no_grant_on_root_dir` (strict): `/` is the root
        // sequencer's, whatever the kind and whoever the holder.
        for (gen, node, kind) in &self.root_grants {
            f.violations.push(format!(
                "no_grant_on_root_dir: generation {gen} grants `/` ({kind}) to node {node}"
            ));
        }
        for (gen, dirs) in &self.granted {
            let Some(ender) = self.ended.get(gen) else {
                continue;
            };
            let released = self.released_whole.contains(gen)
                || holder_sent_release
                    .get(gen)
                    .is_some_and(|n| n.whole || dirs.iter().all(|d| n.dirs.contains(d)))
                || self
                    .released
                    .get(gen)
                    .is_some_and(|r| dirs.iter().all(|d| r.contains(d)));
            if released {
                f.ended_released += 1;
            } else if self.holder.get(gen) == Some(ender) {
                f.ended_claimed += 1;
            } else if self.abandoned.contains(gen) {
                f.ended_abandoned += 1;
            } else {
                f.violations.push(format!(
                    "sticky_ends_only_by_holder_or_operator: the sticky stream of node {} \
                     (generation {gen}, subtrees {dirs:?}) ended with no release of the \
                     holder and no operator's abandon; its records: {:?}",
                    self.holder.get(gen).copied().unwrap_or(0),
                    self.trail.get(gen)
                ));
            }
        }
        f
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_meta::SubtreeGrant;

    fn grant(node: u64, gen: u64, dir: u64) -> LogRecord {
        LogRecord::DelegateNode {
            node,
            gen,
            dirs: vec![SubtreeGrant {
                dir,
                kind: SubtreeKind::Sticky,
            }],
        }
    }

    fn end(node: u64, gen: u64) -> LogRecord {
        LogRecord::DelegateNodeEnded {
            node,
            gen,
            through: 3,
        }
    }

    #[test]
    fn a_ttl_end_is_a_violation_a_release_or_an_abandon_is_not() {
        let none = BTreeMap::new();
        let released = [
            grant(2, 5, 7),
            LogRecord::SubtreeReleased {
                node: 2,
                gen: 5,
                dir: 7,
                at: 3,
            },
            end(2, 5),
        ];
        let f = StickyEnds::of_records(released.iter().map(|r| (9, r))).judge(&none);
        assert_eq!((f.granted, f.ended_released, f.violations.len()), (1, 1, 0));
        let abandoned = [
            grant(2, 5, 7),
            LogRecord::GroupAbandoned {
                epoch: 0,
                gen: 5,
                through_known: 3,
            },
            end(2, 5),
        ];
        let f = StickyEnds::of_records(abandoned.iter().map(|r| (9, r))).judge(&none);
        assert_eq!((f.ended_abandoned, f.violations.len()), (1, 0));
        let ttl = [grant(2, 5, 7), end(2, 5)];
        let f = StickyEnds::of_records(ttl.iter().map(|r| (9, r))).judge(&none);
        assert_eq!(f.violations.len(), 1);
        // A rowless release of the holder (its note on the stream) is its own.
        let sent: BTreeMap<u64, SentRelease> = [(
            5,
            SentRelease {
                whole: false,
                dirs: [7].into(),
            },
        )]
        .into();
        let f = StickyEnds::of_records(ttl.iter().map(|r| (9, r))).judge(&sent);
        assert_eq!((f.ended_released, f.violations.len()), (1, 0));
        // A partial release does not excuse the end of the rest.
        let two = [
            LogRecord::DelegateNode {
                node: 2,
                gen: 5,
                dirs: vec![
                    SubtreeGrant {
                        dir: 7,
                        kind: SubtreeKind::Sticky,
                    },
                    SubtreeGrant {
                        dir: 8,
                        kind: SubtreeKind::Sticky,
                    },
                ],
            },
            LogRecord::SubtreeReleased {
                node: 2,
                gen: 5,
                dir: 7,
                at: 3,
            },
            end(2, 5),
        ];
        assert_eq!(
            StickyEnds::of_records(two.iter().map(|r| (9, r)))
                .judge(&none)
                .violations
                .len(),
            1
        );
        // The holder that claims the lease and ends its own stream (C11).
        let f = StickyEnds::of_records(ttl.iter().map(|r| (2, r))).judge(&none);
        assert_eq!((f.ended_claimed, f.violations.len()), (1, 0));
        // A live sticky stream is no violation.
        let live = [grant(2, 5, 7)];
        let f = StickyEnds::of_records(live.iter().map(|r| (9, r))).judge(&none);
        assert_eq!((f.granted, f.violations.len()), (1, 0));
    }

    /// Review of step 4: a partial release note (only some of the granted
    /// subtrees) does not excuse a later end of the rest; one with `end_at`
    /// does.
    #[test]
    fn a_partial_release_note_does_not_excuse_a_ttl_end() {
        let two = [
            LogRecord::DelegateNode {
                node: 2,
                gen: 5,
                dirs: vec![
                    SubtreeGrant {
                        dir: 7,
                        kind: SubtreeKind::Sticky,
                    },
                    SubtreeGrant {
                        dir: 8,
                        kind: SubtreeKind::Sticky,
                    },
                ],
            },
            end(2, 5),
        ];
        let partial: BTreeMap<u64, SentRelease> = [(
            5,
            SentRelease {
                whole: false,
                dirs: [7].into(),
            },
        )]
        .into();
        let f = StickyEnds::of_records(two.iter().map(|r| (9, r))).judge(&partial);
        assert_eq!((f.ended_released, f.violations.len()), (0, 1));
        let all: BTreeMap<u64, SentRelease> = [(
            5,
            SentRelease {
                whole: false,
                dirs: [7, 8].into(),
            },
        )]
        .into();
        let f = StickyEnds::of_records(two.iter().map(|r| (9, r))).judge(&all);
        assert_eq!((f.ended_released, f.violations.len()), (1, 0));
        let whole: BTreeMap<u64, SentRelease> = [(
            5,
            SentRelease {
                whole: true,
                dirs: BTreeSet::new(),
            },
        )]
        .into();
        let f = StickyEnds::of_records(two.iter().map(|r| (9, r))).judge(&whole);
        assert_eq!((f.ended_released, f.violations.len()), (1, 0));
    }
}
