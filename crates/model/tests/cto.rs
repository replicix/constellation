//! Plan 30 §M8: `cto=strict` (see `constellation_model::cto`).
//!
//! The counterexamples first — bounded mode, delegations acked without a
//! recall, a recall override without the drift margin, a grant that
//! outlives the lease backing it, a release that does not wait for its
//! grants — each asserted *found* by the checker, with the discovered
//! path printed. Then the design, clean: ReadIndex alone, delegations
//! with recall (acked and outwaited), takeovers and releases, and clocks
//! that disagree and step within the lease's margin. Each search prints
//! its state counts and time.

use constellation_model::cto::{CtoModel, Op};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

const CAP: (usize, Duration) = (40_000_000, Duration::from_secs(55));

fn run(label: &str, model: &CtoModel) -> impl Checker<CtoModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(CAP.0)
        .timeout(CAP.1)
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

fn assert_violates(label: &str, model: &CtoModel) {
    let checker = run(label, model);
    let d = checker
        .discovery("close_to_open")
        .unwrap_or_else(|| panic!("{label}: expected a close-to-open counterexample"));
    println!("{label}: counterexample (t = {}):", d.last_state().t);
    for (s, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {s:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
}

fn assert_clean(label: &str, model: &CtoModel, sometimes: &[&'static str]) {
    let checker = run(label, model);
    assert!(
        checker.is_done(),
        "{label}: expected exhaustive exploration within budget"
    );
    if let Some(d) = checker.discovery("close_to_open") {
        panic!(
            "{label}: close-to-open violated: {:?}\n{:#?}",
            d.last_state().violation.clone(),
            d.clone().into_actions()
        );
    }
    for p in sometimes {
        assert!(
            checker.discovery(p).is_some(),
            "{label}: `{p}` never witnessed (vacuous?)"
        );
    }
}

use Op::{Read as R, Write as W};

/// The writer holds; the reader is another node. Its read starts after
/// the close completed and still reads its stale replica.
#[test]
fn bounded_mode_violates_close_to_open() {
    let m = CtoModel::bounded(vec![vec![W], vec![R]]);
    assert_violates("bounded", &m);
}

/// ReadIndex alone (no delegations): every strict read asks the holder.
#[test]
fn strict_readindex_without_delegations_is_clean() {
    let m = CtoModel {
        delegations: false,
        max_tick: 6,
        ..CtoModel::strict(vec![vec![W], vec![R, R], vec![W]])
    };
    assert_clean("readindex-only", &m, &["all_ops_done"]);
}

/// A grant, then a write acked without recalling it: the reader's next
/// open is local under the delegation and misses the close.
#[test]
fn delegations_without_recall_violate() {
    let m = CtoModel {
        recall: false,
        ..CtoModel::strict(vec![vec![W], vec![R, R]])
    };
    assert_violates("no-recall", &m);
}

/// A recall can overtake the reply carrying the grant it recalls (the
/// holder granted, then executed a write and recalled; the network
/// reordered): installing the grant anyway leaves a delegation the
/// holder believes gone.
#[test]
fn a_recall_overtaking_its_grant_violates_without_the_generation_check() {
    let m = CtoModel {
        recall_gen_check: false,
        ..CtoModel::strict(vec![vec![], vec![R, R], vec![W]])
    };
    assert_violates("recall-overtakes-grant", &m);
}

/// The design: a reader with two opens, a forwarded writer and the
/// holder's own write; one message may be lost (a lost recall or recall
/// ack is outwaited). No lease expiry within the horizon here — the
/// takeover and release tests below cover epoch changes.
#[test]
fn strict_with_delegations_is_clean() {
    let m = CtoModel {
        lease_ttl: 30,
        deleg_ttl: 2,
        max_tick: 3,
        ..CtoModel::strict(vec![vec![W], vec![R, R], vec![W]])
    };
    assert_clean(
        "strict",
        &m,
        &["all_ops_done", "read_under_delegation", "recall_acked"],
    );
}

/// The writer's own delegation is not recalled (its reads of its own
/// write are read-your-writes); every other node's is. The writer and a
/// second node both read before and after writing.
#[test]
fn the_writers_own_delegation_is_left_alone_safely() {
    let m = CtoModel {
        lease_ttl: 30,
        deleg_ttl: 2,
        max_tick: 3,
        max_drops: 0,
        ..CtoModel::strict(vec![vec![], vec![R, W, R], vec![R, W, R]])
    };
    assert_clean(
        "own-delegation",
        &m,
        &["all_ops_done", "read_under_delegation", "recall_acked"],
    );
}

/// A lost recall: the holder acks once the grant expired by its clock.
/// Both clocks honest, no margins at all: still safe, because the
/// delegate measures from before the grant (`sent ≤ granted`).
#[test]
fn outwaiting_an_unreachable_delegate_is_clean_without_drift() {
    let m = CtoModel {
        seq_margin: 0,
        deleg_margin: 0,
        max_drops: 2,
        max_tick: 7,
        ..CtoModel::strict(vec![vec![W], vec![R, R]])
    };
    assert_clean("outwait-no-drift", &m, &["all_ops_done"]);
}

/// Two nodes whose clocks start at opposite ends of `±1` (the holder
/// slow, the delegate fast) and may each step once to the other end:
/// the worst case for a grant — the holder's measurement of its lifetime
/// comes out 2 short, the delegate's 2 long. The lease itself stays safe
/// here (`lease_margin = 3 > 2D`) and does not expire.
fn drifting(margin: i16, deleg_ttl: i16, max_tick: u8) -> CtoModel {
    CtoModel {
        seq_margin: margin,
        deleg_margin: margin,
        lease_margin: 3,
        lease_ttl: 30,
        deleg_ttl,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1]),
        max_jumps: 2,
        max_drops: 1,
        max_tick,
        ..CtoModel::strict(vec![vec![W], vec![R, R]])
    }
}

/// With a margin of 1 on both sides (`M_d + M_s < 4D`), the delegate
/// still honours its grant after the holder outwaited a lost recall.
#[test]
fn recall_override_without_enough_margin_violates_under_drift() {
    assert_violates("drift-margin-1", &drifting(1, 2, 6));
}

/// The lease's margin (`M = 3 > 2D`) on both sides: clean, and the
/// delegation is still used (non-vacuous).
#[test]
fn recall_override_with_the_lease_margin_is_clean_under_drift() {
    assert_clean(
        "drift-margin-3",
        &drifting(3, 4, 6),
        &["all_ops_done", "read_under_delegation"],
    );
}

fn takeover(scripts: Vec<Vec<Op>>) -> CtoModel {
    CtoModel {
        lease_ttl: 4,
        deleg_ttl: 8,
        max_tick: 9,
        max_drops: 0,
        ..CtoModel::strict(scripts)
    }
}

/// A grant that outlives the old holder's lease: the old holder stops,
/// a new holder takes over (empty table) and acks a write, and the
/// delegate reads its stale replica under the old epoch's delegation.
#[test]
fn uncapped_grants_violate_across_a_takeover() {
    let m = CtoModel {
        cap_by_lease: false,
        void_on_epoch: false,
        ..takeover(vec![vec![], vec![R, R], vec![W]])
    };
    assert_violates("uncapped", &m);
}

/// Capped at the lease's usable end: clean across the takeover, even
/// with the delegate never voiding on the new epoch's marker (the cap
/// alone is the safety argument; the void is an optimization).
#[test]
fn capped_grants_are_clean_across_a_takeover() {
    let m = CtoModel {
        void_on_epoch: false,
        ..takeover(vec![vec![], vec![R, R], vec![W]])
    };
    assert_clean("capped", &m, &["all_ops_done"]);
}

fn releasing(scripts: Vec<Vec<Op>>) -> CtoModel {
    CtoModel {
        allow_release: true,
        lease_ttl: 8,
        deleg_ttl: 4,
        max_tick: 7,
        max_drops: 0,
        ..CtoModel::strict(scripts)
    }
}

/// A release lets a waiter take over at once: without recalling first,
/// the new holder acks a write under a live old-epoch delegation.
#[test]
fn release_without_recall_violates() {
    let m = CtoModel {
        recall_before_release: false,
        void_on_epoch: false,
        ..releasing(vec![vec![], vec![R, R], vec![W]])
    };
    assert_violates("release-no-recall", &m);
}

#[test]
fn release_after_recall_is_clean() {
    let m = releasing(vec![vec![], vec![R, R], vec![W]]);
    assert_clean("release", &m, &["all_ops_done"]);
}

/// Everything at once, larger: drift with steps, lost messages, a
/// takeover and a release, two readers. Bounded by the state cap.
#[test]
#[ignore]
fn deep_drift_takeover_release() {
    let m = CtoModel {
        allow_release: true,
        allow_renew: true,
        explore_offsets: true,
        init_offsets: None,
        lease_ttl: 8,
        max_tick: 11,
        max_epoch: 3,
        scripts: vec![vec![W], vec![R, R], vec![R, W]],
        ..drifting(3, 4, 11)
    };
    let checker = run("deep", &m);
    assert!(checker.discovery("close_to_open").is_none());
}
