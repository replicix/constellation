//! Plan 30 §M13: the S3 inbox (`AuthorityModel::with_inbox(true)`, see
//! `constellation_model::inbox`). Every test pairs a bounded or
//! exhaustive search with an explicit, hand-built path (one
//! deterministic trace, not a search) that pins the exact behaviour it
//! is about, as `today_bugs.rs` and `holder_side.rs` do, so a regression
//! shows up as a precise assertion and not only as a missing discovery.
//!
//! The two "naive variant" tests are this milestone's version of
//! `today_finds_bug_a`: the model must *find* the bug in the design one
//! would write first (a takeover drain without rid dedup; refusals not
//! deduplicated), and the same path must be clean under the real rules.
//!
//! State-count figures are not measured (this file was written before
//! it was first run); each test prints its own. Bounded searches say so
//! in their doc comment and never assert exhaustiveness.

use constellation_model::protocol::{Action, AuthorityModel, HistEvt, Protocol, State};
use constellation_model::{Errno, NsOp, NsRet, N_NAMES};
use stateright::{Checker, Model};
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

fn name(n: u8) -> u8 {
    assert!(n < N_NAMES);
    n
}

const EXHAUSTIVE_CAP: (usize, usize, Duration) = (60_000_000, 100, Duration::from_secs(180));
const BOUNDED_CAP: (usize, usize, Duration) = (2_000_000, 30, Duration::from_secs(90));

const SAFETY: [&str; 4] = [
    "linearizable",
    "converged_at_quiescence",
    "commits_are_log_prefixes",
    "no_rid_executes_twice",
];

fn base(n_nodes: u8) -> AuthorityModel {
    AuthorityModel::new(n_nodes)
        .with_protocol(Protocol::Recovery)
        .with_inbox(true)
        .with_forward_retries(0)
        .with_lossy(false)
}

fn run(
    label: &str,
    model: &AuthorityModel,
    cap: (usize, usize, Duration),
) -> impl Checker<AuthorityModel> {
    let started = Instant::now();
    let checker = model
        .clone()
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
    checker
}

/// No safety property violated in the explored region, and `progress`
/// witnessed. With `full`, the search must also have finished.
fn assert_clean(label: &str, model: &AuthorityModel, cap: (usize, usize, Duration), full: bool) {
    let checker = run(label, model, cap);
    if full {
        assert!(
            checker.is_done(),
            "expected exhaustive exploration within budget ({label})"
        );
    } else {
        println!("{label}: bounded, non-exhaustive exploration (capped at the numbers above)");
    }
    for prop in SAFETY {
        assert!(
            checker.discovery(prop).is_none(),
            "inbox must not violate {prop} ({label}): {:?}",
            checker.discovery(prop).map(|p| p.into_actions())
        );
    }
    assert!(
        checker.discovery("progress").is_some(),
        "expected at least one run where every op completes ({label})"
    );
}

/// Replay `actions` from the model's single initial state, returning
/// the final state; panics naming the first step that is not enabled
/// and what was enabled instead.
fn walk(model: &AuthorityModel, actions: &[Action]) -> State {
    let mut state = model
        .init_states()
        .into_iter()
        .next()
        .expect("one initial state");
    for (i, want) in actions.iter().enumerate() {
        let steps = model.next_steps(&state);
        let enabled: Vec<Action> = steps.iter().map(|(a, _)| *a).collect();
        let Some((_, next)) = steps.into_iter().find(|(a, _)| a == want) else {
            panic!("step {i} ({want:?}) not enabled; enabled: {enabled:?}; path: {actions:?}");
        };
        state = next;
    }
    state
}

/// Assert that `actions` is a valid counterexample to the `always`
/// property `name`: the path is enabled from the initial state and the
/// property fails in its final state. Deterministic — independent of
/// whether a bounded search happened to reach the same state.
fn assert_counterexample(model: &AuthorityModel, name: &'static str, actions: &[Action]) {
    let end = walk(model, actions);
    let property = model.property(name);
    assert!(
        !(property.condition)(model, &end),
        "{name} holds at the end of the path, so it is not a counterexample: {actions:?}"
    );
}

/// Plain BFS over every reachable state (no property checking), calling
/// `check` on each; for configurations small enough that the whole
/// space fits in `cap` states.
fn every_state(model: &AuthorityModel, cap: usize, mut check: impl FnMut(&State)) -> usize {
    let mut seen: HashSet<State> = HashSet::new();
    let mut queue: VecDeque<State> = model.init_states().into_iter().collect();
    while let Some(s) = queue.pop_front() {
        if !seen.insert(s.clone()) {
            continue;
        }
        assert!(
            seen.len() <= cap,
            "more than {cap} states; raise the cap or shrink the config"
        );
        check(&s);
        for (_, next) in model.next_steps(&s) {
            if !seen.contains(&next) {
                queue.push_back(next);
            }
        }
    }
    seen.len()
}

/// How many non-fenced log records carry `rid`-tagged `rec`.
fn log_count(s: &State, rec: NsOp) -> usize {
    s.log
        .iter()
        .flatten()
        .flat_map(|seg| seg.records.iter())
        .filter(|(rid, r)| rid.is_some() && *r == rec)
        .count()
}

fn returns(s: &State) -> Vec<(u8, NsRet)> {
    s.history
        .iter()
        .filter_map(|e| match e {
            HistEvt::Return(n, r, _) => Some((*n, *r)),
            _ => None,
        })
        .collect()
}

/// Plan 30 §2 constraint 3: a node with nobody to forward to never
/// touches the inbox, and with the inbox on nothing ever crosses the
/// (absent) P2P network. One node alone, and two nodes where only the
/// holder writes: every reachable state has an empty `inbox/` prefix
/// and an empty network, and every property holds.
#[test]
fn inbox_is_untouched_when_there_is_nobody_to_forward_to() {
    for n_nodes in [1u8, 2] {
        let model = base(n_nodes)
            .with_initial_holder(0, 255)
            .with_max_tick(0)
            .with_max_seq(2)
            .with_op(0, NsOp::CreateExcl(name(0)))
            .with_op(0, NsOp::Unlink(name(0)));
        let states = every_state(&model, 100_000, |s| {
            assert!(
                s.inbox().is_empty(),
                "inbox traffic with nobody to forward to: {s:?}"
            );
            assert!(s.network.is_empty(), "a P2P message with P2P off: {s:?}");
        });
        println!("inbox_is_untouched ({n_nodes} node(s)): {states} states, no inbox object ever");
        let checker = run("inbox untouched", &model, EXHAUSTIVE_CAP);
        assert!(checker.is_done());
        checker.assert_properties();
    }
}

/// The steady state: a requester forwards a create and then an unlink
/// through the holder's inbox, the holder polls, executes, ships, and
/// GCs the older batch while keeping the requester's newest one (its
/// LIST-last high-water mark); the requester learns each outcome from
/// the segment that carries it. No P2P message ever exists.
#[test]
fn inbox_two_nodes_is_clean() {
    let model = base(2)
        .with_initial_holder(0, 255) // never expires: Tick is disabled
        .with_max_tick(0)
        .with_max_seq(3)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::Unlink(name(0)));
    assert_clean(
        "inbox (2 nodes, steady state)",
        &model,
        EXHAUSTIVE_CAP,
        true,
    );
    every_state(&model, 5_000_000, |s| {
        assert!(s.network.is_empty(), "a P2P message with P2P off: {s:?}");
    });

    let path = vec![
        Action::ClientInvoke(1), // batch (epoch 1, node 1, n 0): create
        Action::PollInbox(0, 1),
        Action::Ship(0),
        Action::Tail(1),         // Completed{rid} arrives: the create returns
        Action::ClientInvoke(1), // batch n 1: unlink
        Action::PollInbox(0, 1),
        Action::Ship(0),
        Action::GcInbox(0), // batch 0 goes; batch 1 stays as the high-water mark
        Action::Tail(1),
    ];
    let end = walk(&model, &path);
    assert_eq!(
        returns(&end),
        vec![(1, NsRet::Ok), (1, NsRet::Ok)],
        "both ops returned through the log: {:?}",
        end.history
    );
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(0))), 1);
    assert_eq!(log_count(&end, NsOp::Unlink(name(0))), 1);
    assert_eq!(
        end.inbox()
            .iter()
            .map(|b| (b.epoch, b.node, b.n, b.executed))
            .collect::<Vec<_>>(),
        vec![(1, 1, 1, true)],
        "GC keeps the requester's newest consumed batch: {:?}",
        end.inbox()
    );
    assert!(end.nodes[1].client_op.is_none());
    assert_eq!(end.nodes[0].cursor(1), 2);
}

/// A refusal has no reply to ride on: it ships as a `Refused { rid,
/// errno }` record, and the requester returns the errno when it tails
/// that segment. Second create of the same name -> `EEXIST` through the
/// log.
#[test]
fn inbox_refusal_rides_the_log() {
    let model = base(2)
        .with_initial_holder(0, 255)
        .with_max_tick(0)
        .with_max_seq(3)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::CreateExcl(name(0)));
    assert_clean(
        "inbox (refusal through the log)",
        &model,
        EXHAUSTIVE_CAP,
        true,
    );

    let path = vec![
        Action::ClientInvoke(1),
        Action::PollInbox(0, 1),
        Action::Ship(0),
        Action::Tail(1),
        Action::ClientInvoke(1),
        Action::PollInbox(0, 1), // refused: a journal row, not a reply
        Action::Ship(0),         // the refusal ships like any other row
        Action::Tail(1),
    ];
    let end = walk(&model, &path);
    assert_eq!(
        returns(&end),
        vec![(1, NsRet::Ok), (1, NsRet::Err(Errno::Eexist))],
        "{:?}",
        end.history
    );
    let seg2 = end.log[2].as_ref().expect("the refusal's segment");
    assert!(
        seg2.records.is_empty(),
        "a refusal has no namespace records"
    );
    assert_eq!(seg2.refused().len(), 1);
    assert!(
        end.nodes[0].refusals().is_empty(),
        "shipped, so no longer unshipped"
    );
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(0))), 1);
}

/// The holder dies with a batch it never polled. The requester, whose
/// op is still pending, takes the lease itself once the register is
/// claimable; its takeover gate drains its own batch (executing the op
/// locally, exactly once) and the FUSE caller returns success from the
/// dedup answer, not from a second execution.
#[test]
fn inbox_requester_takes_over_a_dead_holders_pending_batch() {
    let model = base(2)
        .with_initial_holder(0, 1)
        .with_max_tick(2)
        .with_lease_ttl(1)
        .with_max_seq(2)
        .with_max_crashes(1)
        .with_op(1, NsOp::CreateExcl(name(0)));
    assert_clean("inbox (requester takes over)", &model, EXHAUSTIVE_CAP, true);

    let path = vec![
        Action::ClientInvoke(1), // batch (1, 1, 0) to node 0's inbox
        Action::Crash(0),        // never polled
        Action::Tick,            // node 0's lease expires
        Action::AcquireLease(1), // epoch 2: marker at slot 1, the gate drains the batch
        Action::Ship(1),         // slot 2
        Action::GcInbox(1),      // an old-epoch batch: deleted outright
    ];
    let end = walk(&model, &path);
    assert_eq!(returns(&end), vec![(1, NsRet::Ok)], "{:?}", end.history);
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(0))), 1);
    assert!(end.inbox().is_empty(), "{:?}", end.inbox());
    assert_eq!(end.nodes[1].held_epoch, Some(2));
    let marker = end.log[1].as_ref().unwrap();
    assert!(
        marker.epoch == 2 && marker.records.is_empty(),
        "the takeover marker"
    );
    assert_eq!(end.log[2].as_ref().unwrap().epoch, 2);
}

/// A requester whose lease-path attempt is overtaken by another node's
/// takeover does not wait out the new holder's TTL: it submits the same
/// rid to the new holder's inbox (`ResubmitInbox`) and gets its answer
/// through the log. Three nodes: 0 holds and dies, 1 and 2 both want
/// the lease, 2 wins.
///
/// Bounded, not exhaustive (three nodes; the explicit path is the
/// guarantee). `inbox_overtaken_lease_path_deep` is the `#[ignore]`d
/// exhaustive attempt.
#[test]
fn inbox_overtaken_lease_path_submits_to_the_new_holder() {
    let model = overtaken_model();
    assert_clean("inbox (overtaken lease path)", &model, BOUNDED_CAP, false);

    let path = vec![
        Action::Crash(0),
        Action::Tick,             // node 0's lease expires
        Action::ClientInvoke(1),  // claimable: NeedsLease, nothing submitted
        Action::ClientInvoke(2),  // same
        Action::AcquireLease(2),  // node 2 wins: marker at slot 1, executes its own op
        Action::ResubmitInbox(1), // node 1: batch (2, 1, 0) to node 2's inbox
        Action::PollInbox(2, 1),
        Action::Ship(2), // slot 2
        Action::Tail(1), // the marker: same epoch as the batch, nothing strands
        Action::Tail(1), // Completed{rid}
    ];
    let end = walk(&model, &path);
    assert_eq!(
        returns(&end),
        vec![(2, NsRet::Ok), (1, NsRet::Ok)],
        "{:?}",
        end.history
    );
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(0))), 1);
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(1))), 1);
    assert_eq!(
        end.inbox().len(),
        1,
        "node 1's batch under epoch 2 (the high-water mark)"
    );
    assert_eq!(end.inbox()[0].epoch, 2);
}

fn overtaken_model() -> AuthorityModel {
    base(3)
        .with_initial_holder(0, 1)
        .with_max_tick(2)
        .with_lease_ttl(2)
        .with_max_seq(3)
        .with_max_crashes(1)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(2, NsOp::CreateExcl(name(1)))
}

/// `cargo test -p constellation-model --release -- --ignored inbox_overtaken_lease_path_deep --nocapture`
#[test]
#[ignore]
fn inbox_overtaken_lease_path_deep() {
    assert_clean(
        "inbox (overtaken lease path, deep)",
        &overtaken_model(),
        EXHAUSTIVE_CAP,
        true,
    );
}

/// Plan 30 §M3b's takeover marker is what makes a stranded requester
/// notice a takeover before the new holder ships anything of its own:
/// node 1 submits to node 0, node 0 dies unpolled, node 2 takes over
/// (marker at slot 1, the gate drains node 1's batch into node 2's
/// journal). Node 1 tails the marker — a higher epoch, no outcome —
/// and re-submits the same rid under epoch 2; node 2's poll answers it
/// from its journal without executing again (dedup), ships, and node 1
/// gets its outcome from the log. Exactly one execution, and the stale
/// epoch-1 batch is gone.
#[test]
fn inbox_marker_strands_and_resubmits() {
    let model = base(3)
        .with_initial_holder(0, 1)
        .with_max_tick(2)
        .with_lease_ttl(2)
        .with_max_seq(3)
        .with_max_crashes(1)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(2, NsOp::CreateExcl(name(1)));
    assert_clean(
        "inbox (marker strands, requester re-submits)",
        &model,
        BOUNDED_CAP,
        false,
    );

    let path = vec![
        Action::ClientInvoke(1),  // batch (1, 1, 0) to node 0
        Action::Crash(0),         // never polled
        Action::Tick,             // node 0's lease expires
        Action::ClientInvoke(2),  // claimable: NeedsLease
        Action::AcquireLease(2),  // marker at slot 1; the gate drains node 1's batch
        Action::Tail(1),          // the marker strands node 1's pending op
        Action::ResubmitInbox(1), // same rid, batch (2, 1, 0); the epoch-1 batch is deleted
        Action::PollInbox(2, 1),  // deduplicated against node 2's journal
        Action::Ship(2),          // slot 2: both creates, node 1's once
        Action::Tail(1),          // Completed{rid} for the re-submitted op
    ];
    let end = walk(&model, &path);
    assert_eq!(
        returns(&end),
        vec![(2, NsRet::Ok), (1, NsRet::Ok)],
        "{:?}",
        end.history
    );
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(0))), 1);
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(1))), 1);
    assert_eq!(
        end.inbox()
            .iter()
            .map(|b| (b.epoch, b.node, b.n, b.executed))
            .collect::<Vec<_>>(),
        vec![(2, 1, 0, true)],
        "the epoch-1 batch was deleted at re-submission; the epoch-2 one is the high-water mark"
    );
    assert!(end.nodes[1].client_op.is_none());
    let marker = end.log[1].as_ref().unwrap();
    assert!(marker.epoch == 2 && marker.records.is_empty());
}

/// Model round 2: a deposed holder's own op, replayed by rid and
/// refused because the new holder independently took the name, is the
/// documented conflict-copy outcome, not a lost acknowledgement. Node 0
/// creates name 0 on the fast path (unshipped), expires; node 1's inbox
/// op for the same name takes over, is executed by its own gate and
/// returns `Ok`; node 0 learns of its deposition, rolls back, replays
/// through node 1 and is refused — its `Ok` becomes `Conflicted`, and the
/// history is linearizable. This is the shape the naive-variant tests'
/// fixed configurations reach, which the tester's gate run found.
#[test]
fn deposed_replay_refused_is_a_conflict_copy() {
    let model = base(2)
        .with_initial_holder(0, 2)
        .with_max_tick(2)
        .with_lease_ttl(2)
        .with_max_seq(3)
        .with_op(0, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::CreateExcl(name(0)));
    assert_clean(
        "deposed replay refused (conflict copy)",
        &model,
        BOUNDED_CAP,
        false,
    );

    let path = vec![
        Action::ClientInvoke(0), // fast path: Ok, unshipped
        Action::ClientInvoke(1), // batch (1, 1, 0) to node 0's inbox
        Action::Tick,
        Action::Tick,                     // node 0's lease expires
        Action::AcquireLease(1),          // marker; the gate drains node 1's batch: Ok
        Action::Renew(0), // node 0 learns it was deposed: rolls back, queues its create
        Action::ReplayStranded(0), // MutateReq 0 to node 1
        Action::DeliverForwardRequest(0), // refused: EEXIST
        Action::DeliverForwardReply(1), // node 0: a conflict copy
    ];
    // Right after the takeover, node 0's acknowledgement is tentative
    // and node 1 has answered the same name `Ok`: the state the round-2
    // gate run found unlinearizable, now accepted (round 3a) because a
    // tentative op need not be visible in between.
    let taken_over = walk(&model, &path[..5]);
    assert_eq!(
        returns(&taken_over),
        vec![(0, NsRet::Tentative), (1, NsRet::Ok)],
        "{:?}",
        taken_over.history
    );
    let lin = model.property("linearizable");
    assert!(
        (lin.condition)(&model, &taken_over),
        "a tentative acknowledgement may linearize later or resolve as a conflict copy"
    );
    let end = walk(&model, &path);
    assert_eq!(
        returns(&end),
        vec![(0, NsRet::Conflicted), (1, NsRet::Ok)],
        "{:?}",
        end.history
    );
    assert!(end.nodes[0].replays.is_empty() && end.nodes[0].journal.is_empty());
    assert_eq!(
        log_count(&end, NsOp::CreateExcl(name(0))),
        0,
        "nothing shipped yet"
    );
    assert!(
        end.nodes[1]
            .journal
            .iter()
            .filter(|e| e.rec == NsOp::CreateExcl(name(0)))
            .count()
            == 1
    );
    let lin = model.property("linearizable");
    assert!(
        (lin.condition)(&model, &end),
        "the conflicted history is linearizable"
    );
}

/// The configuration both naive-variant tests use: name 0 exists from
/// the start; node 0 holds with a lease that expires at tick 2; node 1
/// forwards through the inbox and later takes over. `max_seq` counts the
/// genesis slot.
fn naive_config() -> AuthorityModel {
    base(2)
        .with_genesis_present(name(0))
        .with_initial_holder(0, 2)
        .with_max_tick(2)
        .with_lease_ttl(2)
        .with_max_seq(4)
        .with_max_crashes(1)
}

/// The path both drain tests share up to the takeover: node 1's first
/// op goes through node 0's inbox and returns; node 0 runs its own op
/// (the fast path), ships, and dies before GC; the lease expires; node
/// 1's second op finds the register claimable and takes over — whose
/// gate drains node 1's own, already-answered batch.
fn drain_path_to_takeover() -> Vec<Action> {
    vec![
        Action::ClientInvoke(1), // batch (1, 1, 0)
        Action::PollInbox(0, 1),
        Action::Ship(0),         // slot 2 (slot 1 is genesis)
        Action::Tail(1),         // node 1's first op returns
        Action::ClientInvoke(0), // node 0's own op, fast path
        Action::Ship(0),         // slot 3
        Action::Crash(0),        // before GC: batch (1, 1, 0) is still there
        Action::Tail(1),
        Action::Tick,
        Action::Tick,            // node 0's lease expires
        Action::ClientInvoke(1), // claimable: NeedsLease
        Action::AcquireLease(1), // epoch 2; the gate drains batch (1, 1, 0)
    ]
}

/// The naive drain: a new holder executes every old-epoch batch it
/// lists **without** checking the rids against the log. Node 1's unlink,
/// already executed and shipped by node 0, runs again after node 0
/// re-created the name — so node 1's later `O_EXCL` create of that name
/// succeeds where linearizability demands `EEXIST`. The model finds the
/// double execution (`no_rid_executes_twice`) and the phantom
/// (`linearizable`); the identical path is clean with dedup on.
#[test]
fn naive_drain_without_rid_dedup_double_executes() {
    let ops = |m: AuthorityModel| {
        m.with_op(1, NsOp::Unlink(name(0)))
            .with_op(1, NsOp::CreateExcl(name(1)))
            .with_op(1, NsOp::CreateExcl(name(0)))
            .with_op(0, NsOp::CreateExcl(name(0)))
    };
    let naive = ops(naive_config()).with_inbox_drain_dedup(false);
    let fixed = ops(naive_config());

    let double = drain_path_to_takeover();
    let mut phantom = double.clone();
    phantom.push(Action::ClientInvoke(1)); // create(0) as holder: Ok instead of EEXIST

    // The naive variant, by hand: the drain re-executes the unlink.
    let end = walk(&naive, &double);
    assert_eq!(
        log_count(&end, NsOp::Unlink(name(0)))
            + end.nodes[1]
                .journal
                .iter()
                .filter(|e| e.rec == NsOp::Unlink(name(0)))
                .count(),
        2,
        "naive: the unlink executed twice (log + node 1's journal): {end:?}"
    );
    let end = walk(&naive, &phantom);
    assert_eq!(
        returns(&end).last(),
        Some(&(1, NsRet::Ok)),
        "naive: the phantom unlink let the create succeed: {:?}",
        end.history
    );

    // Both paths are genuine counterexamples of the naive model...
    assert_counterexample(&naive, "no_rid_executes_twice", &double);
    assert_counterexample(&naive, "linearizable", &phantom);
    // ...and the checker finds the double execution on its own (the
    // search is bounded; the explicit paths above are the guarantee,
    // the checker's own discovery the confirmation that the model, not
    // the author, finds the bug).
    let checker = run("naive drain (no rid dedup)", &naive, BOUNDED_CAP);
    let found = checker
        .discovery("no_rid_executes_twice")
        .map(|p| p.into_actions());
    println!("naive drain: checker's own shortest double execution: {found:?}");
    assert!(
        found.is_some(),
        "the checker must find the double execution itself"
    );

    // The real rule: the same path is clean, and the create is refused.
    let end = walk(&fixed, &phantom);
    assert_eq!(
        returns(&end).last(),
        Some(&(1, NsRet::Err(Errno::Eexist))),
        "dedup: the drain skipped the completed unlink: {:?}",
        end.history
    );
    assert_eq!(log_count(&end, NsOp::Unlink(name(0))), 1);
    assert!(end.nodes[1]
        .journal
        .iter()
        .all(|e| e.rec != NsOp::Unlink(name(0))));
    assert_clean("drain with rid dedup", &fixed, BOUNDED_CAP, false);
}

/// The naive refusal rule: plan 30 §M2's "refusals are not recorded"
/// carried over to a path where the *holder* re-reads a batch. Node 1's
/// create of an existing name is refused (`EEXIST` through the log);
/// node 0 then unlinks the name and dies; node 1's takeover drain
/// re-evaluates the old batch, the create now succeeds, and a name the
/// caller was told exists-so-refused springs into being — node 1's later
/// unlink then succeeds where linearizability demands `ENOENT`. With
/// refusals deduplicated the path is clean.
#[test]
fn naive_refusal_without_dedup_creates_phantoms() {
    let ops = |m: AuthorityModel| {
        m.with_op(1, NsOp::CreateExcl(name(0)))
            .with_op(1, NsOp::CreateExcl(name(1)))
            .with_op(1, NsOp::Unlink(name(0)))
            .with_op(0, NsOp::Unlink(name(0)))
    };
    let naive = ops(naive_config()).with_inbox_record_refusals(false);
    let fixed = ops(naive_config());

    let double = drain_path_to_takeover();
    let mut phantom = double.clone();
    phantom.push(Action::ClientInvoke(1)); // unlink(0) as holder: Ok instead of ENOENT

    let end = walk(&naive, &double);
    assert_eq!(
        returns(&end)[0],
        (1, NsRet::Err(Errno::Eexist)),
        "the refusal reached the caller through the log: {:?}",
        end.history
    );
    assert_eq!(
        end.nodes[1]
            .journal
            .iter()
            .filter(|e| e.rec == NsOp::CreateExcl(name(0)))
            .count(),
        1,
        "naive: the refused create was re-evaluated and executed: {end:?}"
    );
    let end = walk(&naive, &phantom);
    assert_eq!(
        returns(&end).last(),
        Some(&(1, NsRet::Ok)),
        "{:?}",
        end.history
    );

    assert_counterexample(&naive, "no_rid_executes_twice", &double);
    assert_counterexample(&naive, "linearizable", &phantom);
    let checker = run("naive refusals (not deduplicated)", &naive, BOUNDED_CAP);
    let found = checker
        .discovery("no_rid_executes_twice")
        .map(|p| p.into_actions());
    println!("naive refusals: checker's own shortest re-evaluation: {found:?}");
    assert!(
        found.is_some(),
        "the checker must find the re-evaluated refusal itself"
    );

    let end = walk(&fixed, &phantom);
    assert_eq!(
        returns(&end).last(),
        Some(&(1, NsRet::Err(Errno::Enoent))),
        "dedup: the refused rid stayed refused: {:?}",
        end.history
    );
    assert_eq!(log_count(&end, NsOp::CreateExcl(name(0))), 0);
    assert_clean("refusals deduplicated", &fixed, BOUNDED_CAP, false);
}

/// `cargo test -p constellation-model --release -- --ignored naive_drain_configs_deep --nocapture`:
/// the two naive-variant configurations with the correct rules,
/// explored exhaustively.
#[test]
#[ignore]
fn naive_drain_configs_deep() {
    let drain = naive_config()
        .with_op(1, NsOp::Unlink(name(0)))
        .with_op(1, NsOp::CreateExcl(name(1)))
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(0, NsOp::CreateExcl(name(0)));
    assert_clean("drain with rid dedup (deep)", &drain, EXHAUSTIVE_CAP, true);
    let refusals = naive_config()
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::CreateExcl(name(1)))
        .with_op(1, NsOp::Unlink(name(0)))
        .with_op(0, NsOp::Unlink(name(0)));
    assert_clean(
        "refusals deduplicated (deep)",
        &refusals,
        EXHAUSTIVE_CAP,
        true,
    );
}
