//! Plan 30 §M11: delegated sub-sequencers over one log
//! (`constellation_model::delegation`).
//!
//! The hand-built counterexample paths come first and are the primary
//! checks: each is walked step by step from the initial state (a step
//! that is not enabled panics naming what was), asserted to violate its
//! property exactly at its last step under the naive variant, and walked
//! again under the design, where the same steps are either not enabled
//! or lead to a state where every property holds. The bounded searches
//! then confirm the checker finds each counterexample on its own and
//! print the discovered path, and the design's configurations are
//! explored exhaustively.
//!
//! `cargo test -p constellation-model --release --test delegation -- --nocapture`

use constellation_model::delegation::{
    key, Action, DelegModel, Key, Msg, Op, State, WriteOp, NONE,
};
use stateright::{Checker, HasDiscoveries, Model};
use std::time::{Duration, Instant};

const CAP: (usize, Duration) = (5_000_000, Duration::from_secs(55));
/// The `#[ignore]`d run's budget: bounded, reported as "clean in the
/// explored region" (M8's deep test does the same).
const BIG_CAP: (usize, Duration) = (20_000_000, Duration::from_secs(900));

/// `/`, `/D1`, `/D2`.
const P3: [u8; 3] = [NONE, 0, 0];
/// `/`, `/D1`, `/D2`, `/D1/D3`.
const P4: [u8; 4] = [NONE, 0, 0, 1];

const RA: Key = key(0, 0);
const D1A: Key = key(1, 0);
const D1B: Key = key(1, 1);
const D2A: Key = key(2, 0);
const D3A: Key = key(3, 0);

const ALWAYS: [&str; 10] = [
    "per_key_linearizable",
    "causal_cut",
    "marker_order",
    "recall_safety",
    "log_records_valid",
    "exactly_once",
    "converged_at_quiescence",
    "read_your_writes",
    "stable_without_faults",
    "durable_acks_stand",
];

fn w(op: WriteOp) -> Op {
    Op::Write(op)
}

fn create(k: Key) -> Op {
    w(WriteOp::Create(k))
}

fn unlink(k: Key) -> Op {
    w(WriteOp::Unlink(k))
}

fn mv(a: Key, b: Key) -> Op {
    w(WriteOp::Move(a, b))
}

fn read(k: Key) -> Op {
    Op::Read(k)
}

fn pair(marker: Key, data: Key) -> Op {
    Op::ReadPair { marker, data }
}

// ------------------------------------------------------------- the walker

/// One step of a hand-built path: a concrete action, the delivery of the
/// first pending message matching a predicate, or its drop.
enum Step {
    A(Action),
    Deliver(&'static str, fn(&Msg) -> bool),
    Drop(&'static str, fn(&Msg) -> bool),
}

use Step::A;

fn find_msg(s: &State, label: &str, pred: fn(&Msg) -> bool) -> usize {
    s.net
        .iter()
        .position(pred)
        .unwrap_or_else(|| panic!("no pending message `{label}`; net: {:?}", s.net))
}

fn enabled(model: &DelegModel, s: &State) -> Vec<Action> {
    model.next_steps(s).into_iter().map(|(a, _)| a).collect()
}

fn holds(model: &DelegModel, name: &'static str, s: &State) -> bool {
    (model.property(name).condition)(model, s)
}

fn all_hold(model: &DelegModel, s: &State) -> Option<&'static str> {
    ALWAYS.iter().copied().find(|p| !holds(model, p, s))
}

fn initial(model: &DelegModel) -> State {
    model
        .init_states()
        .into_iter()
        .next()
        .expect("one initial state")
}

/// Walk `steps` strictly: every step must be enabled.
fn walk(model: &DelegModel, steps: &[Step]) -> State {
    let mut state = initial(model);
    for (i, step) in steps.iter().enumerate() {
        let want = match step {
            A(a) => a.clone(),
            Step::Deliver(label, pred) => Action::Deliver(find_msg(&state, label, *pred)),
            Step::Drop(label, pred) => Action::Drop(find_msg(&state, label, *pred)),
        };
        let steps_now = model.next_steps(&state);
        let Some((_, next)) = steps_now.into_iter().find(|(a, _)| *a == want) else {
            panic!(
                "step {i} ({want:?}) not enabled; enabled: {:?}",
                enabled(model, &state)
            );
        };
        state = next;
    }
    state
}

/// `steps` is a counterexample to `prop`: enabled from the initial
/// state, and `prop` fails at its end and not one step earlier.
fn assert_counterexample(label: &str, model: &DelegModel, prop: &'static str, steps: &[Step]) {
    let before = walk(model, &steps[..steps.len() - 1]);
    let end = walk(model, steps);
    assert!(
        holds(model, prop, &before),
        "{label}: `{prop}` already fails one step before the end"
    );
    assert!(
        !holds(model, prop, &end),
        "{label}: `{prop}` should fail at the path's last step; violation {:?}, history {:?}",
        end.violation,
        end.history
    );
    println!(
        "{label}: hand-built path violates `{prop}` at step {}",
        steps.len()
    );
}

/// Under the design, the same steps either stop being enabled or keep
/// every property; reports where the design diverged.
fn assert_design_survives(label: &str, model: &DelegModel, steps: &[Step]) {
    let mut state = initial(model);
    for (i, step) in steps.iter().enumerate() {
        let want = match step {
            A(a) => Some(a.clone()),
            Step::Deliver(_, pred) | Step::Drop(_, pred) => {
                let k = state.net.iter().position(pred);
                k.map(|k| match step {
                    Step::Drop(..) => Action::Drop(k),
                    _ => Action::Deliver(k),
                })
            }
        };
        let next = want.and_then(|want| {
            model
                .next_steps(&state)
                .into_iter()
                .find(|(a, _)| *a == want)
                .map(|(_, s)| s)
        });
        match next {
            Some(s) => state = s,
            None => {
                println!(
                    "{label}: the design has no step {i} ({:?}); path ends",
                    enabled(model, &state)
                );
                break;
            }
        }
        if let Some(p) = all_hold(model, &state) {
            panic!(
                "{label}: the design violates `{p}` at step {i}: {:?}",
                state.violation
            );
        }
    }
    println!("{label}: the design keeps every property along the path");
}

// -------------------------------------------------------------- searches

fn run(label: &str, model: &DelegModel, stop_at_failure: bool) -> impl Checker<DelegModel> {
    run_capped(label, model, stop_at_failure, CAP)
}

fn run_capped(
    label: &str,
    model: &DelegModel,
    stop_at_failure: bool,
    cap: (usize, Duration),
) -> impl Checker<DelegModel> {
    let started = Instant::now();
    let finish = if stop_at_failure {
        HasDiscoveries::AnyFailures
    } else {
        HasDiscoveries::All
    };
    let checker = model
        .clone()
        .checker()
        .finish_when(finish)
        .target_state_count(cap.0)
        .timeout(cap.1)
        .spawn_bfs()
        .join();
    println!(
        "{label}: {} states ({} unique), max depth {}, is_done={}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        checker.is_done(),
        started.elapsed()
    );
    checker
}

fn assert_search_finds(label: &str, model: &DelegModel, prop: &'static str) {
    let checker = run(label, model, true);
    let d = checker.discovery(prop).unwrap_or_else(|| {
        let others: Vec<String> = ALWAYS
            .iter()
            .filter_map(|p| {
                checker.discovery(p).map(|d| {
                    format!(
                        "{p}: {:?}\n  violation {:?}\n  history {:?}",
                        d.clone().into_actions(),
                        d.last_state().violation,
                        d.last_state().history
                    )
                })
            })
            .collect();
        panic!("{label}: expected the search to find a `{prop}` counterexample; found {others:#?}")
    });
    println!("{label}: search found `{prop}`:");
    for (k, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {k:2}: {a:?}");
    }
    let last = d.last_state();
    println!(
        "  final: violation {:?}\n  history {:?}",
        last.violation, last.history
    );
}

fn assert_clean(label: &str, model: &DelegModel, witnesses: &[&'static str]) {
    let started = Instant::now();
    let checker = run(label, model, false);
    // `is_done` is also true at the state cap or the timeout.
    assert!(
        checker.is_done() && checker.state_count() < CAP.0 && started.elapsed() < CAP.1,
        "{label}: expected exhaustive exploration within budget"
    );
    for p in ALWAYS {
        if let Some(d) = checker.discovery(p) {
            panic!(
                "{label}: `{p}` violated: {:?}\nhistory {:?}\npath {:?}",
                d.last_state().violation,
                d.last_state().history,
                d.clone().into_actions()
            );
        }
    }
    for p in witnesses {
        assert!(
            checker.discovery(p).is_some(),
            "{label}: `{p}` never witnessed (vacuous?)"
        );
    }
}

// ------------------------------------------------- message predicates

fn ack_to(to: u8) -> fn(&Msg) -> bool {
    match to {
        0 => |m| matches!(m, Msg::Ack { to: 0, .. }),
        1 => |m| matches!(m, Msg::Ack { to: 1, .. }),
        2 => |m| matches!(m, Msg::Ack { to: 2, .. }),
        3 => |m| matches!(m, Msg::Ack { to: 3, .. }),
        _ => unreachable!(),
    }
}

fn fwd_to(to: u8) -> fn(&Msg) -> bool {
    match to {
        0 => |m| matches!(m, Msg::Fwd { to: 0, .. }),
        1 => |m| matches!(m, Msg::Fwd { to: 1, .. }),
        2 => |m| matches!(m, Msg::Fwd { to: 2, .. }),
        _ => unreachable!(),
    }
}

fn stream_of_gen(gen: u8) -> fn(&Msg) -> bool {
    match gen {
        1 => |m| matches!(m, Msg::Stream { rec, .. } if rec.gen == 1),
        2 => |m| matches!(m, Msg::Stream { rec, .. } if rec.gen == 2),
        _ => unreachable!(),
    }
}

fn recall_msg(m: &Msg) -> bool {
    matches!(m, Msg::Recall { .. })
}

fn recalled_msg(m: &Msg) -> bool {
    matches!(m, Msg::Recalled { .. })
}

fn renew_req(m: &Msg) -> bool {
    matches!(m, Msg::RenewReq { .. })
}

fn renewed(m: &Msg) -> bool {
    matches!(m, Msg::Renewed { .. })
}

// ------------------------------------------------------ counterexamples

/// The root appends a forwarded op without waiting for its `deps`: a
/// delegate's data write, then a marker in a root-owned directory by the
/// same client; the marker reaches the log before the data, and a reader
/// tailing the log sees the marker alone.
fn append_without_deps_config() -> DelegModel {
    DelegModel {
        initial_delegations: vec![(1, 1)],
        deps_at_root: false,
        max_tick: 1,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A), create(RA)], vec![pair(RA, D1A)]],
        )
    }
}

fn append_without_deps_path() -> Vec<Step> {
    vec![
        A(Action::Start(1)),
        A(Action::Start(1)),
        Step::Deliver("marker forward to the root", fwd_to(0)),
        A(Action::Tail(2)),
        A(Action::Start(2)),
        A(Action::FinishRead(2)),
    ]
}

#[test]
fn appending_without_deps_violates_marker_order() {
    let naive = append_without_deps_config();
    assert_counterexample(
        "append-without-deps",
        &naive,
        "marker_order",
        &append_without_deps_path(),
    );
    let design = DelegModel {
        deps_at_root: true,
        ..naive.clone()
    };
    assert_design_survives("append-without-deps", &design, &append_without_deps_path());
    assert_search_finds("append-without-deps", &naive, "marker_order");
}

/// The delegate executes a forwarded op before its replica has the op's
/// `deps`: its own readers see the marker (its speculation) without the
/// data (another delegate's record, not yet tailed).
fn delegate_ignores_deps_config() -> DelegModel {
    DelegModel {
        initial_delegations: vec![(1, 1), (2, 2)],
        deps_at_delegate: false,
        max_tick: 1,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A), create(D2A)], vec![pair(D2A, D1A)]],
        )
    }
}

fn delegate_ignores_deps_path() -> Vec<Step> {
    vec![
        A(Action::Start(1)),
        A(Action::Start(1)),
        Step::Deliver("marker forward to delegate 2", fwd_to(2)),
        A(Action::Start(2)),
        A(Action::FinishRead(2)),
    ]
}

#[test]
fn a_delegate_ignoring_deps_violates_marker_order_and_the_causal_cut() {
    let naive = delegate_ignores_deps_config();
    assert_counterexample(
        "delegate-ignores-deps",
        &naive,
        "marker_order",
        &delegate_ignores_deps_path(),
    );
    // The causal cut is already broken one step earlier: the delegate's
    // replica holds a record whose deps it lacks.
    let mid = walk(&naive, &delegate_ignores_deps_path()[..3]);
    assert!(!holds(&naive, "causal_cut", &mid));
    let design = DelegModel {
        deps_at_delegate: true,
        ..naive.clone()
    };
    assert_design_survives(
        "delegate-ignores-deps",
        &design,
        &delegate_ignores_deps_path(),
    );
    assert_search_finds("delegate-ignores-deps", &naive, "causal_cut");
}

/// The root ends the generation and executes a cross-subtree move at
/// once instead of draining: the delegate's acknowledged create, whose
/// stream record is merely in flight, is cut and retracted in a run with
/// no fault at all (and the move then answers `ENOENT` for a name whose
/// create completed before it was invoked — consistent only because the
/// create is now tentative, which is exactly the stranding the drain
/// exists to avoid).
fn recall_without_drain_config() -> DelegModel {
    DelegModel {
        initial_delegations: vec![(1, 1)],
        drain_before_execute: false,
        max_tick: 1,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A)], vec![mv(D1A, RA)]],
        )
    }
}

fn recall_without_drain_path() -> Vec<Step> {
    vec![
        A(Action::Start(1)),
        A(Action::Start(2)),
        Step::Deliver("move forward to the root", fwd_to(0)),
    ]
}

#[test]
fn recall_without_draining_strands_an_acknowledged_op_without_any_fault() {
    let naive = recall_without_drain_config();
    assert_counterexample(
        "recall-without-drain",
        &naive,
        "stable_without_faults",
        &recall_without_drain_path(),
    );
    // The move then answers `ENOENT` against a view without the create.
    let mut steps = recall_without_drain_path();
    steps.push(Step::Deliver("move's ack to 2", ack_to(2)));
    let end = walk(&naive, &steps);
    assert!(end.history.iter().any(|e| matches!(
        e,
        constellation_model::delegation::HEvt::Return {
            ret: constellation_model::delegation::Ret::Enoent,
            ..
        }
    )));
    let design = DelegModel {
        drain_before_execute: true,
        ..naive.clone()
    };
    assert_design_survives(
        "recall-without-drain",
        &design,
        &recall_without_drain_path(),
    );
    assert_search_finds("recall-without-drain", &naive, "stable_without_faults");
}

/// The root ends a generation by timeout (the delegate's stream record
/// is still in flight), re-delegates the directory, the new delegate
/// creates the same name, and the stale record is appended without a
/// generation check: a create on a present name in the log.
fn redelegation_config(gen_check: bool) -> DelegModel {
    DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![RA],
        delegatable: vec![(1, 2)],
        allow_renew: true,
        gen_check,
        deleg_ttl: 2,
        max_tick: 3,
        ..DelegModel::design(
            P3.to_vec(),
            vec![
                vec![],
                vec![create(D1A)],
                vec![create(D1A)],
                vec![mv(RA, D1B)],
            ],
        )
    }
}

fn redelegation_path() -> Vec<Step> {
    vec![
        A(Action::Start(1)),
        A(Action::Start(3)),
        Step::Deliver("move forward to the root", fwd_to(0)),
        Step::Deliver("recall to 1", recall_msg),
        Step::Deliver("recalled to the root", recalled_msg),
        A(Action::Tick),
        A(Action::Tick),
        A(Action::Tick),
        A(Action::RecallTimeout(1)),
        A(Action::Delegate(1, 2)),
        A(Action::Tail(2)),
        A(Action::Tail(2)),
        A(Action::Tail(2)),
        A(Action::RenewReq(2)),
        Step::Deliver("renew request", renew_req),
        Step::Deliver("renewed", renewed),
        A(Action::Start(2)),
        Step::Deliver("gen 2 stream", stream_of_gen(2)),
        Step::Deliver("stale gen 1 stream", stream_of_gen(1)),
    ]
}

#[test]
fn redelegation_without_a_generation_check_appends_an_invalid_record() {
    let naive = redelegation_config(false);
    assert_counterexample(
        "redelegation-no-gen-check",
        &naive,
        "log_records_valid",
        &redelegation_path(),
    );
    assert_design_survives(
        "redelegation-no-gen-check",
        &redelegation_config(true),
        &redelegation_path(),
    );
    assert_search_finds("redelegation-no-gen-check", &naive, "log_records_valid");
}

/// Margins 0 under drift: the root's clock steps ahead, the delegate's
/// behind; the root outwaits the (dropped) recall and executes a move
/// into the subtree while the delegate still honours its grant and
/// acknowledges a create of the moved-to name.
fn expired_ack_config(deleg_margin: i16, root_margin: i16) -> DelegModel {
    DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![RA],
        deleg_margin,
        root_margin,
        init_offsets: Some(vec![-1, 1, 0]),
        max_offset: 1,
        max_jumps: 2,
        max_drops: 1,
        max_tick: 3,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A)], vec![mv(RA, D1A)]],
        )
    }
}

fn expired_ack_path() -> Vec<Step> {
    vec![
        A(Action::Jump(0, 1)),
        A(Action::Jump(1, -1)),
        A(Action::Start(2)),
        Step::Deliver("move forward to the root", fwd_to(0)),
        Step::Drop("recall to 1", recall_msg),
        A(Action::Tick),
        A(Action::Tick),
        A(Action::RecallTimeout(1)),
        Step::Deliver("move's ack to 2", ack_to(2)),
        A(Action::Start(1)),
    ]
}

#[test]
fn acking_under_an_expired_delegation_without_margin_violates_under_drift() {
    let naive = expired_ack_config(0, 0);
    assert_counterexample(
        "expired-ack-no-margin",
        &naive,
        "recall_safety",
        &expired_ack_path(),
    );
    // The stale acknowledgement (local, so returned at once) also breaks
    // the key's linearizability: the move's `Put` completed before the
    // create was invoked, and the create still returned `Ok`.
    let end = walk(&naive, &expired_ack_path());
    assert!(!holds(&naive, "per_key_linearizable", &end));
    let design = expired_ack_config(3, 3);
    assert_design_survives("expired-ack-no-margin", &design, &expired_ack_path());
    assert_search_finds("expired-ack-no-margin", &naive, "recall_safety");
}

/// A grant not capped by the root lease outlives it: the delegate's
/// clock steps behind, a new root takes over at the old lease's expiry,
/// treats the inherited grant as dead (no horizon), executes a move into
/// the subtree, and the delegate still acknowledges.
fn uncapped_config(cap_by_lease: bool, takeover_horizon: bool) -> DelegModel {
    DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![RA],
        cap_by_lease,
        takeover_horizon,
        lease_ttl: 3,
        max_epoch: 2,
        init_offsets: Some(vec![0, 1, 0]),
        max_offset: 1,
        max_jumps: 1,
        max_tick: 3,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A)], vec![mv(RA, D1A)]],
        )
    }
}

fn uncapped_path() -> Vec<Step> {
    vec![
        A(Action::Jump(1, -1)),
        A(Action::Tick),
        A(Action::Tick),
        A(Action::Tick),
        A(Action::Takeover(2)),
        A(Action::Start(2)),
        A(Action::RecallTimeout(1)),
        A(Action::Start(1)),
    ]
}

#[test]
fn an_uncapped_grant_outlives_the_root_lease_across_a_takeover() {
    let naive = uncapped_config(false, false);
    assert_counterexample("uncapped-grant", &naive, "recall_safety", &uncapped_path());
    assert_design_survives(
        "uncapped-grant (cap)",
        &uncapped_config(true, false),
        &uncapped_path(),
    );
    assert_design_survives(
        "uncapped-grant (horizon)",
        &uncapped_config(false, true),
        &uncapped_path(),
    );
    assert_search_finds("uncapped-grant", &naive, "recall_safety");
}

// ------------------------------------------------------- the design, clean

/// Two delegates writing locally in their subtrees, the root writing
/// locally in its own, everyone reading its own write.
#[test]
fn design_local_writes_are_clean() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1), (2, 2)],
        initial_present: vec![D2A],
        max_tick: 0,
        ..DelegModel::design(
            P3.to_vec(),
            vec![
                vec![create(RA), read(RA)],
                vec![create(D1A), read(D1A)],
                vec![unlink(D2A), read(D2A)],
            ],
        )
    };
    assert_clean(
        "local-writes",
        &m,
        &["all_ops_done", "delegate_wrote_locally"],
    );
}

/// A node with no delegation forwards into a subtree and reads its own
/// write (the M6 wait for the log), while the delegate writes locally.
#[test]
fn design_forwarded_writes_are_clean() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        max_tick: 0,
        ..DelegModel::design(
            P3.to_vec(),
            vec![
                vec![],
                vec![create(D1A), read(D1A)],
                vec![create(D1B), read(D1B)],
            ],
        )
    };
    assert_clean(
        "forwarded-writes",
        &m,
        &[
            "all_ops_done",
            "delegate_wrote_locally",
            "forwarded_to_delegate",
        ],
    );
}

/// A move between a delegated directory and the root directory recalls
/// the delegation, which drains; the requester's next write in the
/// subtree goes wherever its view says and is redirected if stale.
#[test]
fn design_cross_subtree_move_drains_the_delegation() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![RA],
        max_tick: 2,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A)], vec![mv(RA, D1B), create(D1A)]],
        )
    };
    assert_clean(
        "cross-subtree-move",
        &m,
        &["all_ops_done", "cross_subtree_recall", "recall_drained"],
    );
}

/// The same, with one message lost: a lost recall, recall ack or stream
/// record is outwaited by the root, and what the delegate acknowledged
/// past the cut is replayed by its requester through the new owner.
#[test]
fn design_unreachable_delegate_is_outwaited_and_replayed() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![RA],
        deleg_ttl: 2,
        max_drops: 1,
        max_tick: 4,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A)], vec![mv(RA, D1B)]],
        )
    };
    assert_clean(
        "unreachable-delegate",
        &m,
        &["all_ops_done", "recall_timed_out", "replay_landed"],
    );
}

/// The delegate crashes without a backup: the root reclaims the expired
/// grant, the requester's acknowledged create is retracted and replayed
/// through the root, and its later read is read-your-writes-exempt.
#[test]
fn design_delegate_crash_without_backup_replays_by_rid() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        reclaim_expired: true,
        deleg_ttl: 2,
        max_crashes: 1,
        max_tick: 4,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![], vec![create(D1A), read(D1A)]],
        )
    };
    assert_clean(
        "delegate-crash-no-backup",
        &m,
        &["all_ops_done", "recall_timed_out", "replay_landed"],
    );
}

/// The delegate crashes with a backup: the backup seals and drains its
/// tail to the root, so even the delegate's *own* acknowledged write
/// (whose requester died with it) reaches the log.
#[test]
fn design_delegate_crash_with_backup_keeps_every_durable_ack() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        backups: vec![(1, 2)],
        allow_renew: true,
        reclaim_expired: true,
        deleg_ttl: 2,
        max_crashes: 1,
        max_tick: 3,
        ..DelegModel::design(P3.to_vec(), vec![vec![], vec![create(D1A)], vec![]])
    };
    assert_clean(
        "delegate-crash-backup",
        &m,
        &["all_ops_done", "backup_drained"],
    );
}

/// Root failover with a live delegate: the root's lease expires
/// unrenewed (a stalled root; M9's seal is the same transition with an
/// earlier trigger), its journal is stranded by the takeover, the
/// delegate re-streams its unretired records to the new root and renews
/// with it, and the old root's own acknowledged op is replayed by rid.
#[test]
fn design_root_failover_with_a_live_delegate() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        journal: true,
        allow_renew: true,
        reclaim_expired: true,
        lease_ttl: 3,
        deleg_ttl: 2,
        max_epoch: 2,
        max_tick: 4,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![create(RA)], vec![create(D1A)], vec![]],
        )
    };
    assert_clean(
        "root-failover",
        &m,
        &["all_ops_done", "root_takeover", "restreamed"],
    );
}

/// A move inside one delegated subtree (`/D1/x` → `/D1/D3/y`) is the
/// delegate's own: the ancestor walk resolves both keys to it.
#[test]
fn design_nested_move_is_local_to_the_delegate() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![D1A],
        max_tick: 1,
        ..DelegModel::design(
            P4.to_vec(),
            vec![vec![create(RA)], vec![mv(D1A, D3A), read(D3A)]],
        )
    };
    assert_clean(
        "nested-move",
        &m,
        &["all_ops_done", "delegate_wrote_locally"],
    );
}

/// Markers after data, across a delegate's own readers and a reader
/// tailing the log: the deps wait is exercised and no marker is ever
/// seen without its data.
#[test]
fn design_marker_order_holds() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1), (2, 2)],
        max_tick: 1,
        ..DelegModel::design(
            P3.to_vec(),
            vec![
                vec![],
                vec![create(D1A), create(D2A), create(RA)],
                vec![pair(D2A, D1A)],
                vec![pair(RA, D1A), pair(RA, D2A)],
            ],
        )
    };
    assert_clean("marker-order", &m, &["all_ops_done", "deps_waited"]);
}

/// Re-delegation after a timed-out recall, with the generation check
/// (the candidate itself writes nothing here; the naive test above has
/// it write).
#[test]
fn design_redelegation_after_a_timed_out_recall() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1)],
        initial_present: vec![RA],
        delegatable: vec![(1, 2)],
        allow_renew: true,
        deleg_ttl: 2,
        max_tick: 3,
        ..DelegModel::design(
            P3.to_vec(),
            vec![vec![], vec![create(D1A)], vec![mv(RA, D1B)]],
        )
    };
    assert_clean(
        "redelegation",
        &m,
        &["all_ops_done", "recall_timed_out", "redelegated"],
    );
}

/// The recall override under drift with the lease's margin on both
/// sides (`> 2D` each, so their sum exceeds `4D`): clocks off by up to
/// one tick, stepping, one message lost.
#[test]
fn design_is_clean_under_drift_with_the_margin() {
    let m = DelegModel {
        deleg_margin: 3,
        root_margin: 3,
        lease_margin: 3,
        deleg_ttl: 6,
        max_tick: 10,
        ..expired_ack_config(3, 3)
    };
    assert_clean(
        "drift-with-margin",
        &m,
        &["all_ops_done", "recall_timed_out"],
    );
}

/// A bigger failover configuration: two delegates, the old root's
/// journal stranded with a forwarded op in it.
#[test]
#[ignore]
fn deep_root_failover_two_delegates() {
    let m = DelegModel {
        initial_delegations: vec![(1, 1), (2, 2)],
        journal: true,
        allow_renew: true,
        reclaim_expired: true,
        lease_ttl: 3,
        max_epoch: 2,
        max_crashes: 1,
        max_tick: 6,
        ..DelegModel::design(
            P3.to_vec(),
            vec![
                vec![create(RA)],
                vec![create(D1A), mv(D1A, D2A)],
                vec![create(D2A)],
                vec![create(D1B)],
            ],
        )
    };
    let checker = run_capped("deep-root-failover", &m, true, BIG_CAP);
    for p in ALWAYS {
        if let Some(d) = checker.discovery(p) {
            panic!(
                "deep-root-failover: `{p}` violated: {:?}\nhistory {:?}\npath {:?}",
                d.last_state().violation,
                d.last_state().history,
                d.clone().into_actions()
            );
        }
    }
    println!(
        "deep-root-failover: clean in the explored region (exhaustive: {})",
        checker.state_count() < BIG_CAP.0
    );
}
