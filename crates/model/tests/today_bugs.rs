//! Plan 30 §M1's three required tests, §M2's `ExactlyOnce` check and
//! §M3a's `Recovery` checks. Each finishes within ~60s under
//! `cargo test -p constellation-model --release`; timings are printed so
//! a coordinator can see the margin. None of them assert a wall-clock
//! bound, so a slower `cargo test --workspace` debug build (this file's
//! tests are its slowest, `exactly_once_is_linearizable` especially —
//! ~80s single-threaded, see `safety_net_cap`'s doc comment) still
//! passes; it just takes longer.
//!
//! Every `checker()` call below carries an explicit
//! `target_state_count`/`target_max_depth`/`timeout` — never an
//! uncapped BFS, after a 3-node `Recovery` config with the then-default
//! `max_forward_retries` OOM'd a `cargo test --workspace` run at 27GB
//! (plan 30 M3a's PROGRESS.md entry has the story). For `Today`/
//! `ExactlyOnce` and the smaller `Recovery` configs the cap is a
//! generous safety net only (exhaustive completion is reached far below
//! it, so `checker.is_done()` still reflects true exhaustion).
//! `Recovery`'s 3-node third-node-takeover config is different: even at
//! the smallest bounds that still reach the bug B shape, its state space
//! (two independently-timed client ops plus the shadow/replay dimension)
//! did not finish exhaustively within a 20M state / 6GB probe, so its
//! default-run test is a deliberately bounded, non-exhaustive smoke
//! check — `is_done()` there just means "hit the cap", not "explored
//! everything" — backed by the same explicit, hand-built path check
//! every such test also has (cheap: one deterministic trace, not a
//! search) and by a separate `#[ignore]`d test with a much larger cap
//! for deeper (still non-exhaustive) manual exploration.

use constellation_model::protocol::{Action, AuthorityModel, Protocol};
use constellation_model::{NsOp, N_NAMES};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

/// A generous safety-net cap for configs expected to finish exhaustively
/// (`checker.is_done()` should reflect true completion, not this cap).
/// Sized with margin above `exactly_once_is_linearizable`'s bug-B config,
/// the largest legitimately-exhaustive search here: measured at 39.5M
/// states / 7.2M unique / depth 28, 1.9GB peak RSS, ~19s in release and
/// ~80s single-threaded in debug (this file's tests are `cargo
/// test --workspace`'s slowest, but no test here asserts a wall-clock
/// bound — only `is_done()` and the discoveries, so a slow-but-complete
/// debug run still passes; see the module doc comment).
fn safety_net_cap<M: Model>(
    builder: stateright::CheckerBuilder<M>,
) -> stateright::CheckerBuilder<M> {
    builder
        .target_state_count(60_000_000)
        .target_max_depth(100)
        .timeout(Duration::from_secs(180))
}

fn name(n: u8) -> u8 {
    assert!(n < N_NAMES);
    n
}

/// Bug A (plan 30 §1.1): a forwarded mutation is at-least-once. Node 0
/// holds the lease throughout (its lease never naturally expires within
/// the tick budget, so the *only* way node 1 can ever claim it is via a
/// handoff release — this forces the shortest counterexample through
/// exactly the "forward timeout -> handoff -> local re-execution" shape
/// bug A describes, rather than a same-length "wait out the TTL" path).
#[test]
fn today_finds_bug_a() {
    let started = Instant::now();
    let model = AuthorityModel::new(2)
        .with_initial_holder(0, 255) // effectively never expires (max_tick below is 0)
        .with_max_tick(0) // Tick disabled: expiry is unreachable
        .with_max_seq(2)
        .with_op(1, NsOp::CreateExcl(name(0)));

    let checker = safety_net_cap(model.checker()).spawn_bfs().join();
    println!(
        "today_finds_bug_a: {} states ({} unique), max depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed()
    );

    let path = checker
        .discovery("linearizable")
        .expect("expected a linearizability counterexample (bug A)");
    let actions = path.clone().into_actions();
    println!(
        "today_finds_bug_a counterexample ({} actions):",
        actions.len()
    );
    for a in &actions {
        println!("  {a:?}");
    }

    let has_timeout = actions
        .iter()
        .any(|a| matches!(a, Action::ForwardTimeout(_)));
    let has_handoff_req = actions.iter().any(|a| {
        matches!(
            a,
            Action::RequestHandoff(_) | Action::DeliverHandoffRequest(_)
        )
    });
    let has_local_reexec = actions.iter().any(|a| matches!(a, Action::AcquireLease(_)));
    assert!(
        has_timeout,
        "counterexample must go through a forward timeout"
    );
    assert!(has_handoff_req, "counterexample must go through a handoff");
    assert!(
        has_local_reexec,
        "counterexample must fall back to local re-execution via AcquireLease"
    );
}

/// Bug B (plan 30 §1.1): a holder crash strands an acked-but-unshipped
/// forwarded op in the requester's shadow, so after a takeover the
/// requester's replica never matches the log-derived state. Three nodes:
/// 0 is the initial holder, 1 forwards a create to it and gets Accepted
/// (shadow-installed) before 0 crashes without ever shipping the
/// journal entry; 2 has its own op, so it can drive its own takeover
/// once 0's lease naturally expires (mirroring the M0
/// `holder-crash-phantom-shadow` scenario, where C independently writes
/// `after` and takes over).
#[test]
fn today_finds_bug_b() {
    let started = Instant::now();
    let model = AuthorityModel::new(3)
        .with_initial_holder(0, 1)
        .with_max_tick(6)
        .with_lease_ttl(2)
        .with_max_seq(2)
        .with_max_crashes(1)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(2, NsOp::CreateExcl(name(1)));

    let checker = safety_net_cap(model.checker()).spawn_bfs().join();
    println!(
        "today_finds_bug_b: {} states ({} unique), max depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed()
    );

    let prop_name = ["converged_at_quiescence", "commits_are_log_prefixes"]
        .into_iter()
        .find(|n| checker.discovery(n).is_some())
        .expect(
            "expected a converged_at_quiescence or commits_are_log_prefixes \
             counterexample (bug B)",
        );
    let path = checker.discovery(prop_name).unwrap();
    let actions = path.clone().into_actions();
    println!(
        "today_finds_bug_b shortest counterexample for \"{prop_name}\" ({} actions):",
        actions.len()
    );
    for a in &actions {
        println!("  {a:?}");
    }
    let has_accept = actions
        .iter()
        .any(|a| matches!(a, Action::DeliverForwardRequest(_)));
    let has_crash = actions.iter().any(|a| matches!(a, Action::Crash(_)));
    assert!(
        has_accept,
        "counterexample must include a forwarded, accepted op"
    );
    assert!(has_crash, "counterexample must include the holder crashing");

    // The shortest path the checker finds does not need a takeover at
    // all (excluding the crashed node from the "every live node" check
    // is already enough), but plan 30 §M1 also specifically asks for the
    // shape "via holder crash ... and a takeover" (matching the M0
    // three-node scenario). Confirm that shape is independently
    // reachable and also a genuine counterexample, by hand-building it
    // and asking the checker to validate it as a discovery for the same
    // property.
    let takeover_path = vec![
        Action::ClientInvoke(1),
        Action::DeliverForwardRequest(0),
        Action::Crash(0),
        Action::DeliverForwardReply(1),
        Action::ClientInvoke(2),
        Action::ForwardTimeout(2),
        Action::Tick,
        Action::AcquireLease(2), // node 2's takeover, epoch bump included
        Action::Ship(2),
        Action::Tail(1),
    ];
    println!(
        "today_finds_bug_b explicit crash+takeover path ({} actions), verified as a \
         \"{prop_name}\" counterexample too:",
        takeover_path.len()
    );
    for a in &takeover_path {
        println!("  {a:?}");
    }
    checker.assert_discovery(prop_name, takeover_path);
}

/// Plan 30 §M2: the `ExactlyOnce` variant on bug A's exact configuration
/// (same nodes, same never-expiring initial holder, same workload) must
/// no longer violate linearizability — the fix is the rid dedup at the
/// holder (`DeliverForwardRequest`) plus the completed-table check before
/// `AcquireLease` re-executes (`rid_completed_record`). `progress` must
/// still be witnessed at least once, so the fix is not vacuous (e.g. by
/// accidentally making every op stall forever).
///
/// A second, crash-inclusive configuration reuses bug B's shape (a
/// holder accepts a forward, crashes before shipping, the requester's
/// shadow strands) to show the fix holds *with* crashes present too, not
/// just in the crash-free case above. Bounds are pared down hard from
/// `today_finds_bug_b`'s (2 nodes rather than 3, `max_seq`/`lease_ttl`
/// shrunk to 1, `with_lossy(false)`): `RetryForward`'s extra branch point
/// at every `NeedsLease`-while-busy state multiplies the reachable space
/// by roughly an order of magnitude per allowed attempt (measured while
/// tuning this test), so reproducing bug B's own 3-node bounds here would
/// blow well past the ~60s budget. This shape is still exactly bug B's:
/// `converged_at_quiescence`/`commits_are_log_prefixes` are deliberately
/// not asserted here — bug B (the shadow stranded by the crash) is still
/// expected to violate them under `ExactlyOnce` — recovering from that is
/// M3's job, not this milestone's. (Confirmed by hand while tuning this
/// test: both do still fail at these bounds — that is the expected,
/// unfixed state, not a gap in this test.)
#[test]
fn exactly_once_is_linearizable() {
    let started = Instant::now();
    let model = AuthorityModel::new(2)
        .with_protocol(Protocol::ExactlyOnce)
        .with_initial_holder(0, 255) // effectively never expires
        .with_max_tick(0)
        .with_max_seq(2)
        .with_op(1, NsOp::CreateExcl(name(0)));

    let checker = safety_net_cap(model.checker()).spawn_bfs().join();
    println!(
        "exactly_once_is_linearizable (bug A config): {} states ({} unique), max depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed()
    );
    assert!(
        checker.is_done(),
        "expected exhaustive exploration within budget"
    );
    assert!(
        checker.discovery("linearizable").is_none(),
        "ExactlyOnce must not violate linearizability on bug A's own path: {:?}",
        checker.discovery("linearizable").map(|p| p.into_actions())
    );
    assert!(
        checker.discovery("progress").is_some(),
        "expected at least one run where every op completes (non-vacuous fix)"
    );

    let started = Instant::now();
    let model_b = AuthorityModel::new(2)
        .with_protocol(Protocol::ExactlyOnce)
        .with_initial_holder(0, 1)
        .with_max_tick(3)
        .with_lease_ttl(1)
        .with_max_seq(1)
        .with_max_crashes(1)
        .with_lossy(false)
        .with_op(1, NsOp::CreateExcl(name(0)));
    let checker_b = safety_net_cap(model_b.checker()).spawn_bfs().join();
    println!(
        "exactly_once_is_linearizable (bug B config): {} states ({} unique), max depth {}, {:?}",
        checker_b.state_count(),
        checker_b.unique_state_count(),
        checker_b.max_depth(),
        started.elapsed()
    );
    assert!(
        checker_b.is_done(),
        "expected exhaustive exploration within budget"
    );
    assert!(
        checker_b.discovery("linearizable").is_none(),
        "ExactlyOnce must not violate linearizability even with a holder crash present: {:?}",
        checker_b
            .discovery("linearizable")
            .map(|p| p.into_actions())
    );
    assert!(
        checker_b.discovery("progress").is_some(),
        "expected at least one run where every op completes (non-vacuous fix)"
    );
    // Bug B itself is untouched by M2: convergence still fails when the
    // crashed holder's shadow is stranded. Not asserted as a failure
    // requirement (a future fix must not break this test by accident),
    // just noted for the record.
    let bug_b_still_open = checker_b.discovery("converged_at_quiescence").is_some()
        || checker_b.discovery("commits_are_log_prefixes").is_some();
    println!(
        "exactly_once_is_linearizable: bug B (convergence) still open under ExactlyOnce: {bug_b_still_open} (expected true; M3 fixes it)"
    );
}

/// Run `model` exhaustively (a generous safety-net cap only) and assert
/// that `Recovery` (plan 30 §M3a) holds every safety property there and
/// still reaches `progress`.
fn assert_recovery_clean(label: &str, model: AuthorityModel) {
    assert_recovery_bounded(label, model, None);
}

/// Like [`assert_recovery_clean`], but with an explicit
/// `(target_state_count, target_max_depth, timeout)` cap. When `cap` is
/// `Some`, exhaustiveness is not asserted (the cap may well be what
/// stopped the search) — only that no safety violation was *found*
/// within the explored region, which is a strictly weaker guarantee the
/// caller's doc comment must spell out.
fn assert_recovery_bounded(
    label: &str,
    model: AuthorityModel,
    cap: Option<(usize, usize, Duration)>,
) {
    let started = Instant::now();
    let mut builder = model.checker();
    builder = if let Some((states, depth, timeout)) = cap {
        builder
            .target_state_count(states)
            .target_max_depth(depth)
            .timeout(timeout)
    } else {
        safety_net_cap(builder)
    };
    let checker = builder.spawn_bfs().join();
    println!(
        "{label}: {} states ({} unique), max depth {}, is_done={}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        checker.is_done(),
        started.elapsed()
    );
    if cap.is_none() {
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

/// Plan 30 §M3a: `Recovery` on the exact configuration where
/// `today_finds_bug_b` finds bug B (three nodes; node 0 holds, crashes
/// after accepting node 1's forwarded create; node 2 takes over for its
/// own create).
///
/// The one bound changed from `today_finds_bug_b` is
/// `with_forward_retries(0)`: `Today` has no same-rid retry at all, and
/// `RetryForward` (bug A's fix, covered by `exactly_once_is_linearizable`)
/// multiplies the state space roughly tenfold per allowed attempt without
/// touching the stranding path this test is about. Dedup itself stays on
/// (`Recovery` is built on `ExactlyOnce`), which is what replay by rid
/// relies on.
///
/// Unlike every other `Recovery` test here, exhaustive coverage of this
/// exact, 3-node config is not attempted in the default run: it has two
/// independently-timed client ops (one on the stranded requester, one on
/// the node that takes over) on top of `Recovery`'s shadow/replay
/// dimension, and even at the smallest bounds that still reach the bug B
/// shape (`max_tick`/`lease_ttl`/`max_seq` all down at 1, `max_next_id`
/// down at 10), it did not finish exhaustively within a 20-million-state,
/// ~6GB probe (measured while tuning this test; see PROGRESS.md's plan
/// 30 M3a entry for the numbers) — so this is a deliberately bounded,
/// non-exhaustive smoke check of the exact scenario, backed by the
/// explicit hand-built path below (a single deterministic trace, not a
/// search, so it stays exact and cheap) for the real regression
/// guarantee. `recovery_fixes_bug_b_third_node_takeover_deep` is a larger
/// (still capped, still non-exhaustive) `#[ignore]`d sibling for manual,
/// deeper exploration.
#[test]
fn recovery_fixes_bug_b_third_node_takeover() {
    let model = AuthorityModel::new(3)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(6)
        .with_lease_ttl(2)
        // One more slot than `today_finds_bug_b`: plan 30 §M3b's takeover
        // epoch marker occupies one.
        .with_max_seq(3)
        .with_max_crashes(1)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(2, NsOp::CreateExcl(name(1)));
    assert_recovery_bounded(
        "recovery (bug B, third node takes over)",
        model.clone(),
        Some((200_000, 20, Duration::from_secs(15))),
    );

    // The concrete bug B path from `today_finds_bug_b`, continued with
    // recovery: node 1 tails node 2's takeover epoch marker (slot 1, plan
    // 30 §M3b), which strands its shadow; the replay by rid goes to node
    // 2, which executes it and ships it; node 1 tails node 2's own create
    // (slot 2) and then the replay (slot 3), which retires the replayed
    // shadow.
    let path = vec![
        Action::ClientInvoke(1),
        Action::DeliverForwardRequest(0),
        Action::Crash(0),
        Action::DeliverForwardReply(1),
        Action::ClientInvoke(2),
        Action::ForwardTimeout(2),
        Action::Tick,
        Action::AcquireLease(2),
        Action::Ship(2),
        Action::Tail(1),
        Action::ReplayStranded(1),
        // Ids 0/1 were the original request/reply and 2 node 2's own
        // (never delivered) forward to the dead holder; the replay
        // request is 3 and node 2's reply to it 4.
        Action::DeliverForwardRequest(3),
        Action::DeliverForwardReply(4),
        Action::Ship(2),
        Action::Tail(1),
        Action::Tail(1),
    ];
    let init = model.init_states().into_iter().next().unwrap();
    let end = stateright::Path::from_actions(&model, init, &path)
        .unwrap_or_else(|| panic!("recovery path not reachable: {path:?}"))
        .last_state()
        .clone();
    let node1 = &end.nodes[1];
    assert!(
        node1.shadows.is_empty() && node1.replays.is_empty(),
        "node 1 has no outstanding speculation once recovery completes: {node1:?}"
    );
    let head = end
        .log
        .iter()
        .flatten()
        .flat_map(|seg| seg.records.iter())
        .filter(|(_, rec)| *rec == NsOp::CreateExcl(name(0)))
        .count();
    assert_eq!(
        head, 1,
        "the stranded create is in the durable log exactly once: {:?}",
        end.log
    );
    println!("recovery_fixes_bug_b_third_node_takeover: explicit recovery path verified");
}

/// A larger (still explicitly capped, still non-exhaustive) exploration
/// of `recovery_fixes_bug_b_third_node_takeover`'s exact config, for
/// manual, deeper coverage than the default run's budget allows.
/// `#[ignore]`d: run with `cargo test -p constellation-model --release --
/// --ignored recovery_fixes_bug_b_third_node_takeover_deep --nocapture`.
#[test]
#[ignore]
fn recovery_fixes_bug_b_third_node_takeover_deep() {
    let model = AuthorityModel::new(3)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(6)
        .with_lease_ttl(2)
        // One more slot than `today_finds_bug_b`: plan 30 §M3b's takeover
        // epoch marker occupies one.
        .with_max_seq(3)
        .with_max_crashes(1)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(2, NsOp::CreateExcl(name(1)));
    assert_recovery_bounded(
        "recovery (bug B, third node takes over, deep)",
        model,
        Some((20_000_000, 60, Duration::from_secs(60))),
    );
}

/// Plan 30 §M3a: the other shape of bug B (M0's
/// `holder-crash-phantom-new-holder`): the stranded requester itself
/// takes over. Node 1's create is accepted by node 0, node 0 crashes, and
/// node 1's *next* op (an unlink of the same name) can only run after node
/// 1 takes the lease — so the takeover gate must replay the stranded
/// create first, or the unlink would either validate against phantom
/// state (bug B) or miss the create entirely (a linearizability failure).
///
/// `with_forward_retries(0)`, not `today_finds_bug_b`'s/plan 30 §M2's
/// three: measured while tuning this test, `RetryForward`'s branch point
/// under `Recovery` (stacked on top of `ReplayStranded`'s own retry
/// dimension) made even one allowed attempt blow past a 20-million-state
/// probe here, where it stayed uncapped at zero (328,836 unique states,
/// well under a second, at M3a's `max_seq` of 2; M3b's marker slot
/// raises `max_seq` to 3, so expect a few times more).
/// `recovery_fixes_bug_b_third_node_takeover`
/// already covers the same fix with `RetryForward` reachable via
/// `exactly_once_is_linearizable`'s own bug-A config, so nothing here
/// goes untested elsewhere.
#[test]
fn recovery_fixes_bug_b_requester_takeover() {
    let model = AuthorityModel::new(2)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(3)
        .with_lease_ttl(1)
        // 3, not M3a's 2: plan 30 §M3b's takeover epoch marker takes a
        // slot, and without the extra one the "holder shipped before
        // crashing" branch could no longer ship after the takeover.
        .with_max_seq(3)
        .with_max_crashes(1)
        .with_lossy(false)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::Unlink(name(0)));
    assert_recovery_clean("recovery (bug B, requester takes over)", model);
}

/// Plan 30 §M3a: `Recovery` on `exactly_once_is_linearizable`'s own
/// crash-inclusive configuration (bug B's shape pared down to two nodes),
/// where `ExactlyOnce` still fails convergence.
///
/// `with_forward_retries(0)`: unlike `exactly_once_is_linearizable`'s own
/// use of this exact shape (which keeps the full default retry count),
/// `Recovery`'s added replay dimension makes even one allowed
/// `RetryForward` attempt here explode well past budget (measured while
/// tuning this test: the uncapped checker exceeded 6GB within 19s at the
/// default three retries). At zero it is exhaustive (95,961 states,
/// under 50ms, at M3a's `max_seq` of 1; M3b's marker slot raises it to
/// 2).
#[test]
fn recovery_fixes_bug_b_exactly_once_config() {
    let model = AuthorityModel::new(2)
        .with_protocol(Protocol::Recovery)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(3)
        .with_lease_ttl(1)
        // 2, not `exactly_once_is_linearizable`'s 1: plan 30 §M3b's
        // takeover epoch marker takes a slot of its own.
        .with_max_seq(2)
        .with_max_crashes(1)
        .with_lossy(false)
        .with_op(1, NsOp::CreateExcl(name(0)));
    assert_recovery_clean("recovery (ExactlyOnce's bug B config)", model);
}

/// A single writer that never crashes, pauses, or gets forwarded to
/// (forwarding is simply never reachable: the sole writer already holds
/// the lease for the whole run) must satisfy every property throughout
/// the fully explored state space.
#[test]
fn single_writer_is_clean() {
    let started = Instant::now();
    let model = AuthorityModel::new(2)
        .with_initial_holder(0, 1)
        .with_max_tick(3)
        .with_lease_ttl(2)
        .with_max_seq(3)
        .with_op(0, NsOp::CreateExcl(name(0)))
        .with_op(0, NsOp::Unlink(name(0)));

    let checker = safety_net_cap(model.checker()).spawn_bfs().join();
    println!(
        "single_writer_is_clean: {} states ({} unique), max depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed()
    );

    assert!(
        checker.is_done(),
        "expected exhaustive exploration within budget"
    );
    checker.assert_properties();
}
