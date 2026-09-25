//! Plan 30 §M14: leased cross-node locks (see `constellation_model::locks`).
//!
//! The counterexamples first — no fencing, a grant before expiry plus
//! margin under drift, a fast takeover without replication or grace, a
//! delegation move that loses lock state, a delegate outwaited without
//! the cap — each asserted *found* by the checker, with the discovered
//! path printed. Then the design, clean: recall and release, recall by
//! expiry, cached re-locks, drift within the margin, TTL and fast
//! takeovers with reclaim, and delegation moves both ways.
//!
//! `cargo test -p constellation-model --release --test locks -- --nocapture`

use constellation_model::locks::{LockModel, Mode, Op, NONE};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

// `drift-margin-3` is the largest clean run: 43.7M states (13.9M unique)
// since parked requests carry their arrival time (PROGRESS.md "Fix: M14
// follow-ups"); 16.9M before.
const CAP: (usize, Duration) = (60_000_000, Duration::from_secs(150));

/// Clean runs go breadth-first (exhaustive, shortest paths); violation
/// runs go depth-first, which reaches the deep counterexamples (a
/// renewal chain past a grace period) within the budget.
fn run(label: &str, model: &LockModel) -> impl Checker<LockModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(CAP.0)
        .timeout(CAP.1)
        .spawn_bfs()
        .join();
    report(label, &checker, started);
    checker
}

fn run_dfs(label: &str, model: &LockModel) -> impl Checker<LockModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(CAP.0)
        .timeout(CAP.1)
        .spawn_dfs()
        .join();
    report(label, &checker, started);
    checker
}

fn report(label: &str, checker: &impl Checker<LockModel>, started: Instant) {
    println!(
        "{label}: {} states ({} unique), max depth {}, is_done={}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        checker.is_done(),
        started.elapsed()
    );
}

fn assert_violates(label: &str, model: &LockModel) {
    let checker = run_dfs(label, model);
    let d = checker
        .discovery("mutual_exclusion")
        .unwrap_or_else(|| panic!("{label}: expected a mutual-exclusion counterexample"));
    println!("{label}: counterexample (t = {}):", d.last_state().t);
    for (s, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {s:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
}

fn assert_clean(label: &str, model: &LockModel, sometimes: &[&'static str]) {
    let checker = run(label, model);
    assert!(
        checker.is_done() && checker.state_count() < CAP.0,
        "{label}: expected exhaustive exploration within budget"
    );
    if let Some(d) = checker.discovery("mutual_exclusion") {
        for (i, (a, st)) in d
            .clone()
            .into_actions()
            .into_iter()
            .zip(d.clone().into_states().into_iter().skip(1))
            .enumerate()
        {
            println!(
                "  {i:2}: {a:?} -> t={} owner={:?} net={:?}",
                st.t, st.owner, st.net
            );
            for (k, n) in st.nodes.iter().enumerate() {
                println!(
                    "        n{k}: cache={:?} held={:?} table={:?} floor={:?}",
                    n.cache, n.held, n.table, n.floor
                );
            }
        }
        panic!(
            "{label}: mutual exclusion violated: {:?}",
            d.last_state().violation
        );
    }
    for p in sometimes {
        assert!(
            checker.discovery(p).is_some(),
            "{label}: `{p}` never witnessed (vacuous?)"
        );
    }
}

use Mode::{Exclusive as X, Shared as S};
use Op::{Io, Lock, Unlock};

/// Root 0 never locks; 1 and 2 contend for `X`.
fn contend() -> Vec<Vec<Op>> {
    vec![vec![], vec![Lock(X), Io, Unlock], vec![Lock(X), Io]]
}

// ---- the naive variants ----

/// The application keeps going after its lease lapsed: the owner
/// outwaits the unanswered recall (a lost message) and grants the other
/// node, whose grant the first node's I/O then lands under.
#[test]
fn no_fencing_violates() {
    let m = LockModel {
        fencing: false,
        ..LockModel::design(contend())
    };
    assert_violates("no-fencing", &m);
}

/// Clocks within `D = 1` that may each step once: with margins of 1 on
/// both sides (`M_n + M_s < 4D`) the owner's outwait ends before the
/// holder's honoured window does.
fn drifting(margin: i16, lock_ttl: i16, max_tick: u8) -> LockModel {
    LockModel {
        seq_margin: margin,
        node_margin: margin,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1, -1]),
        max_jumps: 2,
        max_drops: 1,
        max_tick,
        max_renews: 0,
        ..LockModel::design(contend())
    }
}

#[test]
fn granting_before_expiry_plus_margin_violates_under_drift() {
    assert_violates("drift-margin-1", &drifting(1, 3, 6));
}

#[test]
fn the_lease_margin_is_clean_under_drift() {
    assert_clean(
        "drift-margin-3",
        &drifting(3, 4, 8),
        &["all_ops_done", "recall_expired"],
    );
}

/// A sealed backup (node 2) takes the live lease over without the
/// mirror and without the grace: it grants itself while node 1 still
/// honours the old root's grant.
#[test]
fn fast_takeover_without_replication_or_grace_violates() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: false,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_violates("fast-no-repl-no-grace", &m);
}

/// The mirror alone is not enough: it is asynchronous, so a grant made
/// after the last mirror is unknown to the successor.
#[test]
fn fast_takeover_with_only_the_async_mirror_violates() {
    let m = LockModel {
        backup: 2,
        replicate: true,
        grace: false,
        max_drops: 1,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_violates("fast-mirror-no-grace", &m);
}

/// The grace period alone is the safety argument; the mirror is an
/// availability optimisation (a known grant needs no reclaim).
#[test]
fn fast_takeover_with_grace_is_clean() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_clean(
        "fast-grace",
        &m,
        &["all_ops_done", "fast_takeover", "reclaimed"],
    );
}

#[test]
fn fast_takeover_with_grace_and_mirror_is_clean() {
    let m = LockModel {
        backup: 2,
        replicate: true,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        max_drops: 1,
        max_tick: 3,
        ..LockModel::design(contend())
    };
    // Too few ticks for the grace to pass (`fast-grace` above covers
    // completion); what this adds is the mirror's delivery, loss and
    // reordering against the takeover.
    assert_clean("fast-grace-mirror", &m, &["fast_takeover", "mirror_used"]);
}

/// Without probe freshness the deposed root (its own lease is long)
/// grants itself after the successor's grace, which grants node 1.
#[test]
fn fast_takeover_without_the_probe_violates() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        probe_freshness: false,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io], vec![]])
    };
    assert_violates("fast-no-probe", &m);
}

/// The same with the probe: the deposed root grants itself only within
/// `takeover_window` of its last probe, and its next probe deposes it.
#[test]
fn fast_takeover_with_the_probe_is_clean_for_the_old_root() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io], vec![]])
    };
    assert_clean("fast-probe", &m, &["all_ops_done", "fast_takeover"]);
}

/// A TTL takeover with grants that outlive the old lease.
#[test]
fn uncapped_grants_violate_across_a_ttl_takeover() {
    let m = LockModel {
        ttl_takeover: true,
        cap_by_lease: false,
        lease_ttl: 3,
        lock_ttl: 6,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(contend())
    };
    assert_violates("ttl-uncapped", &m);
}

#[test]
fn capped_grants_are_clean_across_a_ttl_takeover() {
    let m = LockModel {
        ttl_takeover: true,
        lease_ttl: 3,
        lock_ttl: 6,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(contend())
    };
    assert_clean(
        "ttl-capped",
        &m,
        &["all_ops_done", "ttl_takeover", "fenced"],
    );
}

/// The delegate (node 2) starts with an empty table and grants itself
/// while node 1 holds the root's grant.
#[test]
fn a_delegation_move_that_loses_state_violates() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        move_state: false,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_violates("move-no-state", &m);
}

/// The same on the way back: a recall whose answer carries no table.
#[test]
fn a_recall_that_loses_state_violates() {
    let m = LockModel {
        delegate: 1,
        max_moves: 2,
        move_state: false,
        max_drops: 0,
        max_tick: 5,
        ..LockModel::design(vec![vec![], vec![], vec![Lock(X), Io, Unlock, Lock(X), Io]])
    };
    // Node 2 locks at the root, the root delegates to 1 (state moves or
    // not), then recalls; a recall that drops the table lets the root
    // grant a second time while node 2's first grant is still honoured.
    let m = LockModel {
        scripts: vec![
            vec![],
            vec![Lock(X), Io],
            vec![Lock(X), Io, Unlock, Lock(X), Io],
        ],
        ..m
    };
    assert_violates("recall-no-state", &m);
}

#[test]
fn delegation_moves_with_state_are_clean() {
    let m = LockModel {
        delegate: 2,
        max_moves: 2,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_clean("move-state", &m, &["all_ops_done", "moved"]);
}

/// A delegate outwaited by the root: the grants moved to it at the
/// delegation carry the root's windows; without a grace the root grants
/// again while those are live.
#[test]
fn an_outwaited_delegate_without_grace_violates() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        recall_by_ttl: true,
        grace: false,
        deleg_ttl: 2,
        lock_ttl: 5,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io, Unlock], vec![]])
    };
    assert_violates("deleg-ttl-no-grace", &m);
}

#[test]
fn a_capped_delegate_outwaited_is_clean() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        recall_by_ttl: true,
        deleg_ttl: 2,
        lock_ttl: 5,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io, Unlock], vec![]])
    };
    assert_clean(
        "deleg-ttl-capped",
        &m,
        &["all_ops_done", "recall_by_ttl", "moved"],
    );
}

// ---- the design ----

/// Contention on a live owner: recall answered by a release, or
/// outwaited after a lost message; the app is fenced when it lapses.
#[test]
fn contention_is_clean() {
    let m = LockModel::design(contend());
    assert_clean(
        "contend",
        &m,
        &[
            "all_ops_done",
            "recall_released",
            "recall_expired",
            "fenced",
        ],
    );
}

/// A node re-locks under its cached grant without a message, and a
/// shared pair coexists.
#[test]
fn cached_relock_and_shared_pairs_are_clean() {
    let m = LockModel {
        max_drops: 0,
        max_tick: 4,
        ..LockModel::design(vec![
            vec![],
            vec![Lock(X), Io, Unlock, Lock(S), Io],
            vec![Lock(S), Io, Unlock, Lock(X), Io],
        ])
    };
    assert_clean("relock", &m, &["all_ops_done", "local_relock"]);
}

/// The owner is itself a lock user.
#[test]
fn the_owner_as_a_lock_user_is_clean() {
    let m = LockModel {
        max_tick: 6,
        ..LockModel::design(vec![vec![Lock(X), Io, Unlock], vec![Lock(X), Io], vec![]])
    };
    assert_clean("owner-user", &m, &["all_ops_done"]);
}

/// Larger: three contenders, a fast takeover, drift and steps. Bounded
/// by the state cap.
#[test]
#[ignore]
fn deep_fast_takeover_under_drift() {
    let m = LockModel {
        backup: 2,
        replicate: true,
        grace: true,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1, 0]),
        max_jumps: 2,
        seq_margin: 3,
        node_margin: 3,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl: 5,
        max_drops: 1,
        max_tick: 12,
        max_renews: 2,
        ..LockModel::design(vec![
            vec![Lock(S), Io, Unlock],
            vec![Lock(X), Io, Unlock, Lock(X), Io],
            vec![Lock(X), Io, Unlock],
        ])
    };
    let checker = run("deep", &m);
    assert!(checker.discovery("mutual_exclusion").is_none());
}

#[test]
fn none_is_no_node() {
    assert_eq!(NONE, u8::MAX);
}
