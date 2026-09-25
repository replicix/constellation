//! Plan 30 §M12 — hot directories: commutative parent attributes under
//! an HLC, hash-range ownership of one directory's names, and the
//! shared/exclusive parent hold.
//!
//! One directory `D` whose names are split into two hash ranges (even
//! names: node 1, odd names: node 2 — the delegates of the ranges), a
//! subdirectory `S` of `D` wholly delegated to node 1, and the root
//! (node 0). Every node executes the ops it owns *locally* (a delegate's
//! transaction), the root *appends* them to the log in the order it
//! receives them, and every node *applies* the log — skipping its own
//! records, which it applied when it executed them (`install_streamed`'s
//! dedup). So the per-node application order of `D`'s records differs
//! from the log order exactly as it does in the system: a delegate sees
//! its own writes first.
//!
//! What the model checks (`properties`):
//!
//! - `converged`: once everything is appended and applied, every node's
//!   view of `D`'s attributes (mtime, nlink) and names is the same. Holds
//!   with `max` times and additive nlink (`merge_max`); the naive
//!   last-writer-wins parent mtime does not (each node ends on the stamp
//!   of the record *it* applied last).
//! - `mtime_monotone`: a node's view of `D`'s mtime never decreases.
//!   Holds with HLC stamps (`hlc`); with raw wall clocks and skew a
//!   later record carries a smaller stamp and the `max` merge stands
//!   still — visible under `merge_max: false` as a decrease.
//! - `no_double_create`: the log never carries two creates of a name
//!   without an unlink between (the root appends a create of a present
//!   name). Holds when only a range's owner executes creates in it
//!   (`range_owner_check`) and a cross-range rename goes through the
//!   root after both ranges drained (`rename_via_root`); the naive
//!   "the source range's delegate renames" races the destination
//!   range's owner.
//! - `acked_create_stands`: a create acknowledged by a delegate is never
//!   appended into a directory the log removed (the rmdir of `S` holds
//!   `S` exclusively — it waits for `S`'s pending creates to drain —
//!   `exclusive_rmdir`; the naive rmdir that takes only its dentry key
//!   lands before a create the delegate already acknowledged).

use stateright::{Model, Property};
use std::collections::BTreeSet;

pub const NODES: usize = 3;
/// Names in `D`: even ones are node 1's range, odd ones node 2's.
pub const N_NAMES: u8 = 4;

pub type Name = u8;
pub type NodeId = u8;

/// The owner (delegate) of `name`'s range in `D`.
pub fn range_owner(name: Name) -> NodeId {
    1 + (name % 2)
}

/// The delegate of `S`.
pub const S_OWNER: NodeId = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// A create of a name in `D`.
    Create(Name),
    /// An unlink of a name in `D`.
    Unlink(Name),
    /// A create of the one name in `S`.
    CreateInS,
    /// `rmdir S` (in `D`).
    RmdirS,
    /// `D/a -> D/b`, across ranges.
    Rename(Name, Name),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rec {
    pub origin: NodeId,
    pub kind: Kind,
    /// The HLC (or wall) stamp the executor wrote.
    pub stamp: i16,
}

/// One node's replica: `D`'s names and attributes, `S`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct View {
    pub names: u8,
    pub d_mtime: i16,
    pub d_nlink: u8,
    pub s_present: bool,
    pub s_has_name: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct State {
    pub t: i16,
    /// Each node's wall-clock offset from `t`.
    pub off: Vec<i16>,
    /// Each node's HLC: the last stamp it issued or applied.
    pub hlc: Vec<i16>,
    pub jumps: u8,
    pub log: Vec<Rec>,
    pub applied: Vec<u8>,
    /// Executed here, acknowledged, not yet in the log (a delegate's
    /// stream).
    pub pending: Vec<Vec<Rec>>,
    pub views: Vec<View>,
    /// Ops each node may still issue.
    pub budget: Vec<u8>,
    /// Creates acknowledged (by their executor) into `S`.
    pub acked_in_s: u8,
    /// Recalled by the root (a cross-range rename, `rmdir S`): executes
    /// nothing until it applied the log through the recall.
    pub stopped: Vec<bool>,
    pub violation: Option<&'static str>,
    pub saw: u8,
}

pub const SAW_CROSS_RENAME: u8 = 1;
pub const SAW_RMDIR: u8 = 2;
pub const SAW_SKEWED: u8 = 4;
pub const SAW_ALL_APPLIED: u8 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Action {
    Tick,
    /// Node `i`'s clock jumps by `d`.
    Jump(NodeId, i16),
    /// Node `i` executes `kind` (locally as the owner; the root for a
    /// cross-range rename or `rmdir S`).
    Exec(NodeId, Kind),
    /// The root appends node `i`'s oldest pending record.
    Append(NodeId),
    /// Node `i` applies the next log record.
    Apply(NodeId),
}

#[derive(Clone, Debug)]
pub struct HotDirModel {
    /// Stamps come from an HLC (never below what the node applied),
    /// else from the node's wall clock.
    pub hlc: bool,
    /// Parent times merge by `max` and nlink by delta; else the parent
    /// takes the record's stamp (last writer wins).
    pub merge_max: bool,
    /// Only a range's owner executes creates and unlinks in it; else any
    /// node executes any name (no ranges).
    pub range_owner_check: bool,
    /// A cross-range rename is executed by the root once both ranges
    /// drained; else by the source range's delegate at once.
    pub rename_via_root: bool,
    /// `rmdir S` holds `S` exclusively: it waits for `S`'s pending
    /// creates to reach the log; else it takes only its dentry.
    pub exclusive_rmdir: bool,
    pub ops_per_node: u8,
    pub max_tick: i16,
    pub max_jumps: u8,
    pub max_offset: i16,
    pub init_offsets: Vec<i16>,
    /// Whether the cross-range rename and `rmdir S` are enabled at all.
    pub with_rename: bool,
    pub with_rmdir: bool,
}

impl HotDirModel {
    /// The design: every rule on, clocks in step.
    pub fn design() -> HotDirModel {
        HotDirModel {
            hlc: true,
            merge_max: true,
            range_owner_check: true,
            rename_via_root: true,
            exclusive_rmdir: true,
            ops_per_node: 2,
            max_tick: 2,
            max_jumps: 0,
            max_offset: 1,
            init_offsets: vec![0; NODES],
            with_rename: true,
            with_rmdir: true,
        }
    }
}

impl State {
    fn wall(&self, i: NodeId) -> i16 {
        self.t + self.off[i as usize]
    }

    fn all_quiet(&self) -> bool {
        self.pending.iter().all(|p| p.is_empty())
            && self.applied.iter().all(|a| *a as usize == self.log.len())
    }

    /// The record's effect on a view (`merge_max`: the commutative
    /// rules; else LWW times and, for nlink, the same delta — the plan's
    /// "additive deltas" are what today's code already does).
    fn apply_to(&self, view: &mut View, rec: &Rec, merge_max: bool) -> Option<&'static str> {
        let touch = |v: &mut View| {
            if merge_max {
                v.d_mtime = v.d_mtime.max(rec.stamp);
            } else {
                v.d_mtime = rec.stamp;
            }
        };
        match rec.kind {
            Kind::Create(n) => {
                if view.names & (1 << n) != 0 {
                    return Some("name created twice");
                }
                view.names |= 1 << n;
                touch(view);
            }
            Kind::Unlink(n) => {
                view.names &= !(1 << n);
                touch(view);
            }
            Kind::Rename(a, b) => {
                if view.names & (1 << b) != 0 {
                    return Some("name created twice");
                }
                view.names &= !(1 << a);
                view.names |= 1 << b;
                touch(view);
            }
            Kind::CreateInS => {
                if !view.s_present {
                    return Some("create into a removed directory");
                }
                view.s_has_name = true;
            }
            Kind::RmdirS => {
                view.s_present = false;
                view.s_has_name = false;
                view.d_nlink = view.d_nlink.saturating_sub(1);
                touch(view);
            }
        }
        None
    }
}

impl Model for HotDirModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            t: 0,
            off: self.init_offsets.clone(),
            hlc: vec![0; NODES],
            jumps: 0,
            log: Vec::new(),
            applied: vec![0; NODES],
            pending: vec![Vec::new(); NODES],
            views: vec![
                View {
                    names: 0,
                    d_mtime: 0,
                    d_nlink: 3,
                    s_present: true,
                    s_has_name: false,
                };
                NODES
            ],
            budget: vec![self.ops_per_node; NODES],
            acked_in_s: 0,
            stopped: vec![false; NODES],
            violation: None,
            saw: 0,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.violation.is_some() {
            return;
        }
        if s.t < self.max_tick {
            actions.push(Action::Tick);
        }
        if s.jumps < self.max_jumps {
            for i in 0..NODES as u8 {
                for d in [-1i16, 1] {
                    let o = s.off[i as usize] + d;
                    if o.abs() <= self.max_offset {
                        actions.push(Action::Jump(i, d));
                    }
                }
            }
        }
        for i in 0..NODES as u8 {
            let iu = i as usize;
            if !s.pending[iu].is_empty() {
                actions.push(Action::Append(i));
            }
            if (s.applied[iu] as usize) < s.log.len() {
                actions.push(Action::Apply(i));
            }
            if s.budget[iu] == 0 || s.stopped[iu] {
                continue;
            }
            let v = &s.views[iu];
            for n in 0..N_NAMES {
                let owner_ok = !self.range_owner_check || range_owner(n) == i;
                if !owner_ok || i == 0 {
                    continue;
                }
                if v.names & (1 << n) == 0 {
                    actions.push(Action::Exec(i, Kind::Create(n)));
                } else {
                    actions.push(Action::Exec(i, Kind::Unlink(n)));
                }
            }
            if i == S_OWNER && v.s_present && !v.s_has_name {
                actions.push(Action::Exec(i, Kind::CreateInS));
            }
            if self.with_rename {
                // `D/0 -> D/1`: from node 1's range into node 2's.
                let executor = if self.rename_via_root {
                    0
                } else {
                    range_owner(0)
                };
                if i == executor && v.names & 1 != 0 && v.names & 2 == 0 {
                    let drained = !self.rename_via_root
                        || (s.pending[range_owner(0) as usize].is_empty()
                            && s.pending[range_owner(1) as usize].is_empty()
                            && s.applied[0] as usize == s.log.len());
                    if drained {
                        actions.push(Action::Exec(i, Kind::Rename(0, 1)));
                    }
                }
            }
            if self.with_rmdir && i == 0 && v.s_present && !v.s_has_name {
                // The root executes `rmdir S` on its own view; with the
                // exclusive hold it first recalls `S`'s delegate: every
                // create `S`'s owner acknowledged is in the log.
                let drained = !self.exclusive_rmdir
                    || (s.pending[S_OWNER as usize]
                        .iter()
                        .all(|r| r.kind != Kind::CreateInS)
                        && s.applied[0] as usize == s.log.len());
                if drained {
                    actions.push(Action::Exec(i, Kind::RmdirS));
                }
            }
        }
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut s = s.clone();
        match a {
            Action::Tick => s.t += 1,
            Action::Jump(i, d) => {
                s.off[i as usize] += d;
                s.jumps += 1;
                s.saw |= SAW_SKEWED;
            }
            Action::Exec(i, kind) => {
                let iu = i as usize;
                let stamp = if self.hlc {
                    let next = s.wall(i).max(s.hlc[iu] + 1);
                    s.hlc[iu] = next;
                    next
                } else {
                    s.wall(i)
                };
                let rec = Rec {
                    origin: i,
                    kind,
                    stamp,
                };
                let before = s.views[iu].d_mtime;
                let mut view = s.views[iu];
                if let Some(v) = s.apply_to(&mut view, &rec, self.merge_max) {
                    s.violation = Some(v);
                    return Some(s);
                }
                if view.d_mtime < before {
                    s.violation = Some("parent mtime went backwards");
                    return Some(s);
                }
                s.views[iu] = view;
                s.budget[iu] -= 1;
                match kind {
                    Kind::CreateInS => s.acked_in_s += 1,
                    Kind::Rename(a, b) => {
                        s.saw |= SAW_CROSS_RENAME;
                        if self.rename_via_root {
                            // The root recalled both ranges first: their
                            // delegates execute nothing until they applied
                            // the log through here (then re-delegated).
                            s.stopped[range_owner(a) as usize] = true;
                            s.stopped[range_owner(b) as usize] = true;
                        }
                    }
                    Kind::RmdirS => {
                        s.saw |= SAW_RMDIR;
                        if self.exclusive_rmdir {
                            s.stopped[S_OWNER as usize] = true;
                        }
                    }
                    _ => {}
                }
                if i == 0 {
                    // The root's own execution is in the log at once.
                    s.log.push(rec);
                    s.applied[0] += 1;
                } else {
                    s.pending[iu].push(rec);
                }
            }
            Action::Append(i) => {
                let rec = s.pending[i as usize].remove(0);
                // The root appends without re-validation, as the delegate
                // validated — but its own replica is the log's state, and
                // a record that cannot apply there is the violation.
                let mut view = s.views[0];
                let mut probe = s.clone();
                probe.views[0] = view;
                if let Some(v) = probe.apply_to(&mut view, &rec, self.merge_max) {
                    s.violation = Some(v);
                    return Some(s);
                }
                let before = s.views[0].d_mtime;
                if view.d_mtime < before {
                    s.violation = Some("parent mtime went backwards");
                    return Some(s);
                }
                s.views[0] = view;
                s.hlc[0] = s.hlc[0].max(rec.stamp);
                s.log.push(rec);
                s.applied[0] += 1;
            }
            Action::Apply(i) => {
                let iu = i as usize;
                let rec = s.log[s.applied[iu] as usize];
                s.applied[iu] += 1;
                if self.hlc {
                    s.hlc[iu] = s.hlc[iu].max(rec.stamp);
                }
                if s.applied[iu] as usize == s.log.len() {
                    // Caught up with the recall: delegated again.
                    s.stopped[iu] = false;
                }
                if rec.origin == i {
                    // Applied when executed.
                } else {
                    let before = s.views[iu].d_mtime;
                    let mut view = s.views[iu];
                    if let Some(v) = s.apply_to(&mut view, &rec, self.merge_max) {
                        s.violation = Some(v);
                        return Some(s);
                    }
                    if view.d_mtime < before {
                        s.violation = Some("parent mtime went backwards");
                        return Some(s);
                    }
                    s.views[iu] = view;
                }
            }
        }
        if s.all_quiet() && !s.log.is_empty() {
            s.saw |= SAW_ALL_APPLIED;
            let first = s.views[0];
            if s.views.iter().any(|v| *v != first) {
                s.violation = Some("replicas diverged at quiescence");
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("converged", |_, s: &State| {
                s.violation != Some("replicas diverged at quiescence")
            }),
            Property::always("mtime_monotone", |_, s: &State| {
                s.violation != Some("parent mtime went backwards")
            }),
            Property::always("no_double_create", |_, s: &State| {
                s.violation != Some("name created twice")
            }),
            Property::always("acked_create_stands", |_, s: &State| {
                s.violation != Some("create into a removed directory")
            }),
            Property::sometimes("all_applied", |_, s: &State| s.saw & SAW_ALL_APPLIED != 0),
            Property::sometimes("cross_rename_ran", |_, s: &State| {
                s.saw & SAW_CROSS_RENAME != 0
            }),
            Property::sometimes("rmdir_ran", |_, s: &State| s.saw & SAW_RMDIR != 0),
        ]
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick && s.log.len() <= 12
    }
}

/// The set of names present in a view (for tests).
pub fn names_of(v: &View) -> BTreeSet<Name> {
    (0..N_NAMES).filter(|n| v.names & (1 << n) != 0).collect()
}
