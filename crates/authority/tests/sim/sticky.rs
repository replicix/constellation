//! Plan 46 M1.3 step 4: `sticky_ends_only_by_holder_or_operator`, the sim's
//! form of the model's property (`crates/model/src/node_stream.rs`, chunk
//! C7): a sticky grant's stream ends only by the holder's release or an
//! operator's action. The log says which: the holder's release is a row of
//! its own stream (`SubtreeReleased` for each subtree it was granted, or one
//! `StreamReleased`) that precedes the root's `DelegateNodeEnded` (an
//! operator's `undelegate` is answered by it too: the recall asks the holder
//! to release), or a note on its stream the sim saw it send (a rowless
//! release leaves nothing in the log); the holder that claims the root lease
//! itself ends its own stream (claim = release, C11; the segment that
//! carries the end is the holder's own); the operator's abandon is a
//! `GroupAbandoned` of the generation. A sticky stream whose
//! `DelegateNodeEnded` has none of these ended by something else (the TTL, a
//! placement round, a cross-subtree op): the violation. The oracle is strict
//! on every configuration; with no sticky grant it judges nothing.

use constellation_meta::{LogRecord, SubtreeKind};
use std::collections::{BTreeMap, BTreeSet};

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

    pub fn judge(&self, holder_sent_release: &BTreeSet<u64>) -> StickyFindings {
        let mut f = StickyFindings {
            granted: self.granted.len(),
            ..Default::default()
        };
        for (gen, dirs) in &self.granted {
            let Some(ender) = self.ended.get(gen) else {
                continue;
            };
            let released = self.released_whole.contains(gen)
                || holder_sent_release.contains(gen)
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
        let none = BTreeSet::new();
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
        let sent: BTreeSet<u64> = [5].into();
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
}
