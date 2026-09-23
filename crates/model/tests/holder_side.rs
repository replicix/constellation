//! Plan 30 §M3b's `Recovery` checks: the holder side of the speculation
//! log. A holder's unshipped journal is speculation with captured
//! before-images, published by before-image substitution; a deposed
//! holder rolls its journal back and replays it by rid; and a takeover
//! ships an empty epoch-marker segment before its gate, so third nodes
//! strand the old epoch's shadows as soon as they tail it.
//!
//! As in `today_bugs.rs`, every `checker()` call carries an explicit cap
//! (never an uncapped BFS), and every test pairs the search with an
//! explicit, hand-built path (one deterministic trace, not a search)
//! that pins the exact M3b behaviour it is about, so a regression shows
//! up as a precise assertion rather than only as a missing discovery.
//!
//! State-count figures below are estimates made by analogy with the
//! measured M3a configs (`recovery_fixes_bug_b_requester_takeover`: 1.43M
//! states in 0.7s; `exactly_once_is_linearizable`: 46M states in 21.6s at
//! 1.78GB), not measurements; the tests print the real numbers.

use constellation_model::protocol::{Action, AuthorityModel, Protocol, State};
use constellation_model::{NsOp, N_NAMES};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

fn name(n: u8) -> u8 {
    assert!(n < N_NAMES);
    n
}

/// A safety-net cap for configs expected to finish exhaustively (the
/// same numbers as `today_bugs.rs`'s `safety_net_cap`).
const EXHAUSTIVE_CAP: (usize, usize, Duration) = (60_000_000, 100, Duration::from_secs(180));

/// Run `model` under `cap` and assert that no safety property is violated
/// and that `progress` is witnessed. With `full`, also assert the
/// search finished (the cap is then only a safety net); without it, only
/// the explored region is covered, which the caller's doc comment must
/// say.
fn assert_clean(label: &str, model: AuthorityModel, cap: (usize, usize, Duration), full: bool) {
    let started = Instant::now();
    let checker = model
        .checker()
        .target_state_count(cap.0)
        .target_max_depth(cap.1)
        .timeout(cap.2)
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
    if full {
        assert!(
            checker.is_done(),
            "expected exhaustive exploration within budget ({label})"
        );
    } else {
        println!("{label}: bounded, non-exhaustive exploration (capped at the numbers above)");
    }
    for prop in [
        "linearizable",
        "converged_at_quiescence",
        "commits_are_log_prefixes",
    ] {
        assert!(
            checker.discovery(prop).is_none(),
            "Recovery must not violate {prop} ({label}): {:?}",
            checker.discovery(prop).map(|p| p.into_actions())
        );
    }
    assert!(
        checker.discovery("progress").is_some(),
        "expected at least one run where every op completes ({label})"
    );
}

/// Replay `actions` from `model`'s single initial state, returning the
/// state after each action (index `i` is the state after `actions[i]`).
/// Panics, naming the action and what was enabled instead, if any step
/// is not enabled.
fn walk(model: &AuthorityModel, actions: &[Action]) -> Vec<State> {
    let mut state = model
        .init_states()
        .into_iter()
        .next()
        .expect("one initial state");
    let mut out = Vec::new();
    for (i, want) in actions.iter().enumerate() {
        let steps = model.next_steps(&state);
        let enabled: Vec<Action> = steps.iter().map(|(a, _)| *a).collect();
        let Some((_, next)) = steps.into_iter().find(|(a, _)| a == want) else {
            panic!("step {i} ({want:?}) not enabled; enabled: {enabled:?}; path: {actions:?}");
        };
        state = next;
        out.push(state.clone());
    }
    out
}

/// How many times `rec` appears in the durable log.
fn log_count(s: &State, rec: NsOp) -> usize {
    s.log
        .iter()
        .flatten()
        .flat_map(|seg| seg.records.iter())
        .filter(|(_, r)| *r == rec)
        .count()
}

/// Node 0 holds the lease for the whole run (it never expires: `Tick` is
/// disabled) and runs a create then an unlink of the same name; node 1
/// forwards a create of another name to it. No crashes, pauses or loss.
fn holder_publish_model() -> AuthorityModel {
    AuthorityModel::new(2)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 255) // effectively never expires
        .with_max_tick(0)
        .with_max_seq(3)
        .with_lossy(false)
        .with_max_next_id(12)
        .with_op(0, NsOp::CreateExcl(name(0)))
        .with_op(0, NsOp::Unlink(name(0)))
        .with_op(1, NsOp::CreateExcl(name(1)))
}

/// Plan 30 §M3b: a holder publishes while its journal is non-empty, by
/// substituting each touched name's earliest before-image, then ships.
/// Every property holds exhaustively, and an explicit path pins the
/// published values: after an unshipped create the commit shows the name
/// absent; after the create ships and an unlink is journaled on top, it
/// shows the name *present* (the unlink's before-image), at the claimed
/// applied position each time.
///
/// Estimated well under 1M states and a second or two: tick disabled, no
/// faults, loss off, and at most one forward (plus handoff rounds capped
/// by `max_next_id`); the handoff path is the only way the lease moves.
#[test]
fn recovery_holder_publishes_log_prefix_with_journal() {
    let model = holder_publish_model();
    assert_clean(
        "recovery (holder publishes with a non-empty journal)",
        model.clone(),
        EXHAUSTIVE_CAP,
        true,
    );

    // create(0) journaled and published unshipped, shipped to slot 1 and
    // published; then unlink(0) journaled on top and published unshipped,
    // shipped to slot 2 and published.
    let path = [
        Action::ClientInvoke(0),
        Action::Publish(0),
        Action::Ship(0),
        Action::Publish(0),
        Action::ClientInvoke(0),
        Action::Publish(0),
        Action::Ship(0),
        Action::Publish(0),
    ];
    let states = walk(&model, &path);
    assert!(
        !states[0].nodes[0].journal.is_empty(),
        "create is journaled but not shipped"
    );
    assert_eq!(
        states[1].commit,
        Some((0b00, 0)),
        "unshipped create is substituted away: commit is the empty log prefix"
    );
    assert_eq!(states[3].commit, Some((0b01, 1)));
    assert!(
        !states[4].nodes[0].journal.is_empty(),
        "unlink is journaled but not shipped"
    );
    assert_eq!(
        states[5].commit,
        Some((0b01, 1)),
        "unshipped unlink is substituted by its before-image (present)"
    );
    assert_eq!(states[7].commit, Some((0b00, 2)));
    println!("recovery_holder_publishes_log_prefix_with_journal: explicit path verified");
}

/// Non-vacuity for the test above: the same config, but the holder
/// publishes its raw replica (speculative journal included) instead of
/// the before-image-substituted view. `commits_are_log_prefixes` must
/// then fail, which shows it genuinely constrains the holder's publish
/// rather than holding by construction. The same state space as the
/// test above (`linearizable` and `converged_at_quiescence` are never
/// discovered, so the search still runs to completion).
#[test]
fn recovery_raw_holder_publish_breaks_log_prefixes() {
    let started = Instant::now();
    let model = holder_publish_model().with_raw_holder_publish(true);
    let (states, depth, timeout) = EXHAUSTIVE_CAP;
    let checker = model
        .checker()
        .target_state_count(states)
        .target_max_depth(depth)
        .timeout(timeout)
        .spawn_bfs()
        .join();
    println!(
        "recovery_raw_holder_publish_breaks_log_prefixes: {} states ({} unique), max depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed()
    );
    let path = checker
        .discovery("commits_are_log_prefixes")
        .expect("raw holder publish must violate commits_are_log_prefixes");
    let actions = path.into_actions();
    println!(
        "recovery_raw_holder_publish_breaks_log_prefixes counterexample ({} actions):",
        actions.len()
    );
    for a in &actions {
        println!("  {a:?}");
    }
    assert!(
        actions.iter().any(|a| matches!(a, Action::Publish(_))),
        "counterexample must publish"
    );
    // The shortest one is the holder publishing straight after executing.
    checker.assert_discovery(
        "commits_are_log_prefixes",
        vec![Action::ClientInvoke(0), Action::Publish(0)],
    );
}

/// Node 0 holds epoch 1 (expiring at tick 1) and creates name 0; node 1
/// creates name 1. Pause is on, so node 0 can execute, stop with the
/// create unshipped, and come back after node 1 has taken over.
///
/// `max_tick(1)`: node 0's lease expires at tick 1 and node 1's takeover
/// lease (`lease_ttl(1)`, taken at tick 1) never does, which is enough
/// for the deposition shape and keeps the lease from ping-ponging.
/// `max_seq(3)`: node 1's marker, its own create, and the replayed create.
/// Disjoint names on the two nodes on purpose: with only one op each, no
/// path can return a result the rolled-back-then-replayed op would
/// contradict, so any `linearizable` discovery would be a real bug in
/// replay by rid (double or lost execution), not the known
/// ack-before-durability window of a holder whose journal dies with it.
fn deposed_holder_model() -> AuthorityModel {
    AuthorityModel::new(2)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(1)
        .with_lease_ttl(1)
        .with_max_seq(3)
        .with_pause(true)
        .with_lossy(false)
        .with_max_next_id(10)
        .with_op(0, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::CreateExcl(name(1)))
}

/// Plan 30 §M3b deposed holder: node 0 executes a create (unshipped),
/// pauses; node 1 takes over (marker at slot 1, gate, its own create);
/// node 0 resumes, learns it was deposed — by tailing the marker, or by a
/// `Renew` that finds the lease moved — rolls its journal back, and
/// replays the create by rid through node 1. All properties hold
/// exhaustively and `progress` is witnessed; two explicit paths (one per
/// way of learning) check the rollback and that the create lands in the
/// log exactly once.
///
/// Estimated a few million states, a few seconds: two nodes and one op
/// each like `recovery_fixes_bug_b_requester_takeover` (1.43M), with
/// pause toggles on both nodes in place of a one-shot crash and one more
/// log slot, but a tick horizon of 1 instead of 3.
#[test]
fn recovery_deposed_holder_rolls_back_and_replays() {
    let model = deposed_holder_model();
    assert_clean(
        "recovery (deposed holder rolls back and replays by rid)",
        model.clone(),
        EXHAUSTIVE_CAP,
        true,
    );

    let create0 = NsOp::CreateExcl(name(0));
    // Node 0 journals create(0) at epoch 1 and pauses; its lease expires;
    // node 1's forward (id 0) to the paused holder times out; node 1 takes
    // over (marker at slot 1, then its create(1)), ships it to slot 2;
    // node 0 resumes.
    let prefix = [
        Action::ClientInvoke(0),
        Action::Pause(0),
        Action::Tick,
        Action::ClientInvoke(1),
        Action::ForwardTimeout(1),
        Action::AcquireLease(1),
        Action::Ship(1),
        Action::Resume(0),
    ];

    for (how, learn) in [("tail", Action::Tail(0)), ("renew", Action::Renew(0))] {
        let mut path = prefix.to_vec();
        path.push(learn);
        let learned_at = path.len() - 1;
        // The replay request is id 1; node 1 executes it and replies with
        // id 2 (installed on node 0 as an epoch-2 shadow), then ships it to
        // slot 3; node 0 tails up to slot 3, which retires the shadow.
        path.extend([
            Action::ReplayStranded(0),
            Action::DeliverForwardRequest(1),
            Action::DeliverForwardReply(2),
            Action::Ship(1),
        ]);
        let tails_left = if how == "tail" { 2 } else { 3 };
        path.extend(std::iter::repeat_n(Action::Tail(0), tails_left));
        let states = walk(&model, &path);

        let marker = states[5].log[1].as_ref().expect("marker");
        assert!(
            marker.records.is_empty() && marker.epoch == 2 && marker.node == 1,
            "slot 1 is node 1's empty epoch-2 marker: {marker:?}"
        );
        let learned = &states[learned_at].nodes[0];
        assert!(
            learned.journal.is_empty() && learned.held_epoch.is_none(),
            "deposition ({how}) rolled node 0's journal back and dropped its lease: {learned:?}"
        );
        assert_eq!(
            learned.replays.iter().map(|r| r.op).collect::<Vec<_>>(),
            vec![create0],
            "the rolled-back create is queued for replay by rid ({how})"
        );
        let end = states.last().unwrap();
        let n0 = &end.nodes[0];
        assert!(
            n0.journal.is_empty() && n0.shadows.is_empty() && n0.replays.is_empty(),
            "node 0 has no speculation left ({how}): {n0:?}"
        );
        assert_eq!(n0.applied_seq, 3);
        assert_eq!(
            log_count(end, create0),
            1,
            "the deposed holder's create is in the log exactly once ({how}): {:?}",
            end.log
        );
        println!("recovery_deposed_holder_rolls_back_and_replays: explicit {how} path verified");
    }
}

/// Plan 30 §M3b's gap "a takeover whose own op is refused ships no
/// segment": node 0 holds, accepts node 1's forwarded create and crashes
/// before shipping it; node 2 takes over for an unlink of a name that
/// never existed, which is refused, so node 2 journals nothing. Before
/// M3b nothing would ever land in the log, and node 1's shadow of the
/// dead holder's create would never strand. With the marker, node 1
/// strands it on tailing the marker alone, replays it through node 2,
/// and converges.
///
/// Three nodes, so (as with `recovery_fixes_bug_b_third_node_takeover`)
/// the search is a deliberately bounded, non-exhaustive smoke check
/// (200k states / depth 20 / 15s), backed by the explicit path below
/// for the exact regression guarantee.
/// `recovery_marker_strands_third_node_shadow_deep` is the larger,
/// `#[ignore]`d sibling.
///
/// Estimated: the 200k cap is reached in about a second, as in the M3a
/// third-node test.
#[test]
fn recovery_marker_strands_third_node_shadow() {
    let model = third_node_marker_model();
    assert_clean(
        "recovery (marker strands a third node's shadow)",
        model.clone(),
        (200_000, 20, Duration::from_secs(15)),
        false,
    );
    check_third_node_marker_path(&model);
}

/// Node 0 holds epoch 1 (expiring at tick 1) with no op of its own and
/// may crash once; node 1 creates name 0; node 2's op is an unlink of a
/// name no op ever creates, so it is always refused.
fn third_node_marker_model() -> AuthorityModel {
    AuthorityModel::new(3)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(2)
        .with_lease_ttl(2)
        .with_max_seq(2)
        .with_max_crashes(1)
        .with_lossy(false)
        .with_max_next_id(10)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(2, NsOp::Unlink(name(1)))
}

/// The explicit trace behind `recovery_marker_strands_third_node_shadow`.
fn check_third_node_marker_path(model: &AuthorityModel) {
    let create0 = NsOp::CreateExcl(name(0));
    // Node 1 forwards create(0) (id 0); node 0 accepts it at epoch 1
    // (reply id 1) and crashes before shipping; node 1 installs the
    // epoch-1 shadow. Node 2 forwards unlink(1) to the dead holder (id 2),
    // times out, and after the tick takes over: marker at slot 1, and its
    // unlink(1) is refused (ENOENT), so it journals nothing. Node 1 tails
    // the marker alone, which strands its shadow; its replay (id 3) is
    // executed by node 2 (reply id 4), shipped to slot 2, and tailed.
    let path = [
        Action::ClientInvoke(1),
        Action::DeliverForwardRequest(0),
        Action::Crash(0),
        Action::DeliverForwardReply(1),
        Action::ClientInvoke(2),
        Action::ForwardTimeout(2),
        Action::Tick,
        Action::AcquireLease(2),
        Action::Tail(1),
        Action::ReplayStranded(1),
        Action::DeliverForwardRequest(3),
        Action::DeliverForwardReply(4),
        Action::Ship(2),
        Action::Tail(1),
    ];
    let states = walk(model, &path);

    let after_takeover = &states[7];
    assert!(
        after_takeover.nodes[2].journal.is_empty(),
        "node 2's own op was refused: it has nothing to ship"
    );
    let marker = after_takeover.log[1].as_ref().expect("marker");
    assert!(marker.records.is_empty() && marker.epoch == 2);
    let stranded = &states[8];
    assert!(
        stranded.log[2].is_none(),
        "only the marker exists when node 1 strands"
    );
    assert!(
        stranded.nodes[1].shadows.is_empty() && stranded.nodes[1].replays.len() == 1,
        "tailing the marker stranded node 1's shadow into the replay queue: {:?}",
        stranded.nodes[1]
    );
    let end = states.last().unwrap();
    let n1 = &end.nodes[1];
    assert!(
        n1.shadows.is_empty() && n1.replays.is_empty(),
        "node 1 has no outstanding speculation once recovery completes: {n1:?}"
    );
    assert_eq!(
        log_count(end, create0),
        1,
        "the dead holder's accepted create is in the log exactly once: {:?}",
        end.log
    );
    println!("recovery_marker_strands_third_node_shadow: explicit path verified");
}

/// A larger (still capped, still non-exhaustive) exploration of
/// `recovery_marker_strands_third_node_shadow`'s config. Run with `cargo
/// test -p constellation-model --release -- --ignored
/// recovery_marker_strands_third_node_shadow_deep --nocapture`.
#[test]
#[ignore]
fn recovery_marker_strands_third_node_shadow_deep() {
    assert_clean(
        "recovery (marker strands a third node's shadow, deep)",
        third_node_marker_model(),
        (20_000_000, 60, Duration::from_secs(60)),
        false,
    );
}

/// A larger deposed-holder config for manual runs: a longer tick horizon
/// (so the lease can move back and forth, deposing each holder in turn),
/// one more log slot for the second marker, and loss on. Capped, so not
/// necessarily exhaustive. Run with `cargo test -p constellation-model
/// --release -- --ignored recovery_deposed_holder_deep --nocapture`.
#[test]
#[ignore]
fn recovery_deposed_holder_deep() {
    let model = deposed_holder_model()
        .with_max_tick(3)
        .with_max_seq(4)
        .with_lossy(true)
        .with_max_next_id(14);
    assert_clean(
        "recovery (deposed holder, deep)",
        model,
        (20_000_000, 60, Duration::from_secs(120)),
        false,
    );
}
