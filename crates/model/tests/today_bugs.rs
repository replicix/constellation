//! Plan 30 §M1's three required tests. Each must finish within ~60s
//! under `cargo test -p constellation-model --release`; timings are
//! printed so a coordinator can see the margin.

use constellation_model::protocol::{Action, AuthorityModel, Protocol};
use constellation_model::{NsOp, N_NAMES};
use stateright::{Checker, Model};
use std::time::Instant;

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

    let checker = model.checker().spawn_bfs().join();
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

    let checker = model.checker().spawn_bfs().join();
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

    let checker = model.checker().spawn_bfs().join();
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
    let checker_b = model_b.checker().spawn_bfs().join();
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

    let checker = model.checker().spawn_bfs().join();
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
