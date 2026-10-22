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

use constellation_model::clk::ClaimRule;
use constellation_model::cto::{Action, CtoModel, Msg, Op, State};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

const CAP: (usize, Duration) = (40_000_000, Duration::from_secs(55));

fn run(label: &str, model: &CtoModel) -> impl Checker<CtoModel> {
    run_within(label, model, CAP)
}

fn run_within(label: &str, model: &CtoModel, cap: (usize, Duration)) -> impl Checker<CtoModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
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
    assert_clean_within(label, model, sometimes, CAP);
}

fn assert_clean_within(
    label: &str,
    model: &CtoModel,
    sometimes: &[&'static str],
    cap: (usize, Duration),
) {
    let checker = run_within(label, model, cap);
    assert!(
        checker.is_done(),
        "{label}: the search did not finish within the budget"
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

// ---- plan 30 §M9: a takeover before the lease expires ----

/// A `fast` tenure (a sealed backup / `ack=s3`): the successor takes the
/// live lease over while the reader still holds the old holder's
/// delegation and does not wait the horizon out — the reader's next open
/// is local and stale.
#[test]
fn fast_takeover_without_the_horizon_violates() {
    let m = CtoModel {
        horizon_wait: false,
        max_tick: 6,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_violates("fast-no-horizon", &m);
}

/// With the horizon but without the old holder's liveness probe: the
/// deposed holder keeps answering strict reads past the window the
/// successor waited for.
#[test]
fn fast_takeover_without_the_probe_violates() {
    let m = CtoModel {
        probe_freshness: false,
        max_tick: 8,
        deleg_ttl: 2,
        lease_ttl: 12,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_violates("fast-no-probe", &m);
}

/// Both rules: a fast takeover with a delegation outstanding is clean,
/// the successor's floor and the fast takeover both witnessed.
#[test]
fn fast_takeover_with_horizon_and_probe_is_clean() {
    let m = CtoModel {
        max_tick: 6,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_clean(
        "fast-both",
        &m,
        &["fast_takeover", "floor_waited", "read_under_delegation"],
    );
}

/// The same under clocks off by ±1 that step once each, with margins of
/// 3 (> 2D): still clean.
#[test]
fn fast_takeover_is_clean_under_drift_within_the_margin() {
    let m = CtoModel {
        max_offset: 1,
        max_jumps: 1,
        seq_margin: 3,
        deleg_margin: 3,
        lease_margin: 3,
        lease_ttl: 12,
        deleg_ttl: 4,
        takeover_window: 2,
        max_tick: 6,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_clean("fast-drift", &m, &["fast_takeover", "floor_waited"]);
}

/// Plan 43 C7-H2: the successor's clock is ahead of the old holder's by
/// more than the lease (offsets of 9 against a lease of 6): it reads every
/// lease as expired, but a takeover the permit allowed is a fast one all
/// the same and waits the whole floor. (Classified by the taker's clock
/// against the old expiry, as the model did, the takeover counted as a TTL
/// one: no floor, and the reader's delegation is read stale.)
#[test]
fn fast_takeover_is_clean_with_a_successor_far_ahead() {
    let m = CtoModel {
        init_offsets: Some(vec![0, 9, 9]),
        max_tick: 6,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_clean(
        "fast-far-ahead",
        &m,
        &["fast_takeover", "floor_waited", "read_under_delegation"],
    );
}

// ---- plan 43 C7-F F-a: the floor capped by the old holder's beats ----

/// The beat configurations' budget: a quarter of the others' (a breadth-
/// first search stores every state it generates: about 1.1 GB at this
/// budget, 4.5 GB at the others'). Bounded, like theirs: `is_done` is true
/// on reaching it, so a clean result is "no violation within the budget",
/// not exhaustive. Three of the four clean runs are `#[ignore]`d (the
/// binary already peaks at ~23 GB with all its tests at once): run them with
/// `cargo test -p constellation-model --test cto -- --ignored beat_cap
/// --test-threads=1` (TESTING.md); one cheaper representative stays on.
const BEAT_CAP: (usize, Duration) = (10_000_000, Duration::from_secs(40));
/// The representative's: it needs no `read_under_delegation` witness (the
/// older tests have it) and reaches `floor_capped` well inside it.
const BEAT_CAP_LIGHT: (usize, Duration) = (4_000_000, Duration::from_secs(40));

/// The old holder beats to the nodes (`beat_cap`); a successor that heard
/// them ends its floor where its own clock puts the holder's lease, plus a
/// margin, when that is before the bound. Clean, the cap witnessed.
#[test]
fn fast_takeover_with_beat_cap_is_clean() {
    let m = CtoModel {
        beat_cap: true,
        max_tick: 8,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_clean_within(
        "fast-beat-cap",
        &m,
        &["fast_takeover", "floor_waited", "floor_capped"],
        BEAT_CAP_LIGHT,
    );
}

/// The two C7-H2 configurations, with the cap: a successor far ahead of
/// the old holder (the beat's lag is large, so the cap is later than the
/// bound: the bound stands) and one far behind it (the lag is negative,
/// and the cap, on the successor's clock, still ends no earlier than the
/// holder's lease).
#[test]
#[ignore = "heavy (1 GB, ~20 s): cargo test -p constellation-model --test cto -- --ignored beat_cap --test-threads=1"]
fn fast_takeover_with_beat_cap_is_clean_far_ahead_and_far_behind() {
    for (label, offsets) in [
        ("far-ahead", vec![0, 9, 9]),
        ("far-behind", vec![0, -9, -9]),
    ] {
        let m = CtoModel {
            init_offsets: Some(offsets),
            beat_cap: true,
            max_tick: 6,
            max_drops: 0,
            ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
        };
        assert_clean_within(
            &format!("fast-beat-cap-{label}"),
            &m,
            // The cap is computed in both (the beats were heard): far
            // ahead its lag puts it past the bound, which stands.
            &["fast_takeover", "floor_waited", "cap_computed"],
            BEAT_CAP,
        );
    }
}

/// The cap with the lease renewed after the last beat and clocks off by
/// one that step once, margins of 3 (> 2D).
#[test]
#[ignore = "heavy (1 GB, ~20 s): cargo test -p constellation-model --test cto -- --ignored beat_cap --test-threads=1"]
fn fast_takeover_with_beat_cap_is_clean_under_drift_and_renewals() {
    let m = CtoModel {
        max_offset: 1,
        max_jumps: 1,
        seq_margin: 3,
        deleg_margin: 3,
        lease_margin: 3,
        lease_ttl: 12,
        deleg_ttl: 4,
        takeover_window: 2,
        allow_renew: true,
        beat_cap: true,
        max_tick: 6,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![R, R], vec![W], vec![W]])
    };
    assert_clean_within(
        "fast-beat-cap-drift",
        &m,
        &["fast_takeover", "floor_waited"],
        BEAT_CAP,
    );
}

/// The cap in the one configuration where the old holder's answers are
/// delegations to another node (node 0 holds and only answers; node 1
/// reads, node 2 writes and takes over), the holder renewing its lease
/// between its beats and the takeover.
fn delegating_holder() -> CtoModel {
    CtoModel {
        allow_renew: true,
        beat_cap: true,
        deleg_ttl: 4,
        max_tick: 9,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![], vec![R, R], vec![W]])
    }
}

#[test]
#[ignore = "heavy (1 GB, ~20 s): cargo test -p constellation-model --test cto -- --ignored beat_cap --test-threads=1"]
fn fast_takeover_with_beat_cap_is_clean_with_a_delegating_holder_that_renews() {
    assert_clean_within(
        "fast-beat-cap-delegating",
        &delegating_holder(),
        &[
            "fast_takeover",
            "floor_waited",
            "floor_capped",
            "read_under_delegation",
        ],
        BEAT_CAP,
    );
}

/// Executes `a` in `s`, which must be enabled there.
fn step(m: &CtoModel, s: &State, a: Action) -> State {
    let mut enabled = Vec::new();
    m.actions(s, &mut enabled);
    assert!(enabled.contains(&a), "{a:?} is not enabled at t={}", s.t);
    m.next_state(s, a).expect("a state")
}

/// Delivers the first message in flight that `is` matches.
fn deliver(m: &CtoModel, s: &State, is: impl Fn(&Msg) -> bool) -> State {
    let k = s.net.iter().position(is).expect("the message is in flight");
    step(m, s, Action::Deliver(k))
}

/// The scenario the naive cap gets wrong, as a trace (the checker's
/// search is too deep for it within budget: ~22 actions over 8 ticks):
/// node 0 holds (epoch 1, lease to 6) and beats to node 2 at t = 0; it
/// renews at t = 4 (to 10); at t = 5 it answers node 1's ReadIndex with a
/// delegation honoured until 8; node 2 takes over at t = 5. Returns the
/// state just after the takeover.
fn renewal_after_the_beat(m: &CtoModel) -> State {
    let mut s = m.init_states().remove(0);
    s = step(m, &s, Action::Beat(0, 2));
    for _ in 0..4 {
        s = step(m, &s, Action::Tick);
    }
    s = step(m, &s, Action::Renew(0));
    s = step(m, &s, Action::Tick);
    s = step(m, &s, Action::Start(1));
    s = step(m, &s, Action::SendReadIndex(1));
    s = deliver(m, &s, |x| matches!(x, Msg::ReadIndex { .. }));
    s = deliver(m, &s, |x| matches!(x, Msg::ReadIndexReply { .. }));
    assert_eq!(s.nodes[1].deleg.map(|d| d.until), Some(8));
    step(m, &s, Action::Takeover(2))
}

/// The naive cap, `X` = what the beats named: the holder renews after its
/// last beat, grants a delegation under the renewed lease, and the
/// successor's floor ends at the old expiry (plus the lag and a margin),
/// while the reader still honours the grant: a stale read. The cap must
/// count the lease the takeover replaced.
#[test]
fn naive_beat_cap_with_a_renewal_after_the_beat_splits() {
    let m = CtoModel {
        beat_cap_from_named_expiry: true,
        ..delegating_holder()
    };
    let mut s = renewal_after_the_beat(&m);
    // Naive floor: named 6 + lag 0 + margin 1 = 7, below the grant's end.
    assert_eq!(s.nodes[2].ack_floor, Some(7));
    s = step(&m, &s, Action::Start(2));
    s = step(&m, &s, Action::Tick);
    s = step(&m, &s, Action::Tick);
    s = step(&m, &s, Action::FloorPassed(2));
    assert_eq!(s.closed, 1, "the write was acknowledged");
    s = step(&m, &s, Action::FinishRead(1));
    s = step(&m, &s, Action::Start(1));
    s = step(&m, &s, Action::ReadUnderDelegation(1));
    s = step(&m, &s, Action::FinishRead(1));
    println!("naive cap: close-to-open violated: {:?}", s.violation);
    assert!(s.violation.is_some(), "the stale read happened");
}

/// The same trace under the real cap: the floor counts the renewed lease
/// the takeover replaced (10), so it outlasts the delegation; the write
/// is still held when the reader's delegation has lapsed.
#[test]
fn the_beat_cap_counts_the_lease_the_takeover_replaced() {
    let m = delegating_holder();
    let mut s = renewal_after_the_beat(&m);
    // max(10, 6) + lag 0 + margin 1.
    assert_eq!(s.nodes[2].ack_floor, Some(11));
    s = step(&m, &s, Action::Start(2));
    s = step(&m, &s, Action::Tick);
    s = step(&m, &s, Action::Tick);
    let mut enabled = Vec::new();
    m.actions(&s, &mut enabled);
    assert!(
        !enabled.contains(&Action::FloorPassed(2)),
        "the write was acknowledged inside the old holder's renewed lease"
    );
    assert_eq!(s.closed, 0);
}

/// A tenure without strict mounts and no strict reader costs its
/// successor nothing: no floor is set, and bounded-mode writes stay
/// clean (bounded reads make no promise).
#[test]
fn an_unmarked_tenure_sets_no_floor() {
    let m = CtoModel {
        max_tick: 6,
        fast_takeover: true,
        ..CtoModel::bounded(vec![vec![W], vec![W]])
    };
    let checker = run("fast-unmarked", &m);
    assert!(checker.is_done());
    assert!(checker.discovery("close_to_open").is_none());
    assert!(checker.discovery("fast_takeover").is_some());
    assert!(
        checker.discovery("floor_waited").is_none(),
        "a floor was set although nobody served a strict read"
    );
}

// ---- plan 43 C7-F F-a0: a tenure that inherits a floor is marked ----

/// Strict reads without strict mounts: a tenure's register says it serves
/// them only from its first answer. The first holder serves a reader
/// (marked), a successor seals it and waits the floor out, and is sealed
/// in turn inside that floor before it served anything (so its register
/// is unmarked): its own successor waits no floor, and acknowledges a
/// write while the first holder's delegation is still honoured at the
/// reader.
fn double_seal(inherit_mark: bool) -> CtoModel {
    CtoModel {
        mark_at_takeover: false,
        inherit_mark,
        max_tick: 5,
        max_epoch: 3,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![W], vec![R, R], vec![W]])
    }
}

#[test]
fn a_tenure_sealed_inside_its_floor_leaves_no_floor_without_the_inherited_mark() {
    assert_violates("double-seal-unmarked", &double_seal(false));
}

#[test]
fn a_tenure_that_inherits_a_floor_marks_its_register_and_the_double_seal_is_clean() {
    assert_clean(
        "double-seal-marked",
        &double_seal(true),
        &["fast_takeover", "floor_waited"],
    );
}

// ---- plan 43 C7a: rate, and the honour/wait/wall split ----
//
// The models' clocks (`constellation_model::clk`): a node's oscillator gains
// or loses `rate` ticks every `rate_period` real ticks; its wait reading
// runs slow, and its honour reading fast, by the design's `rate_budget`
// (the code's rho); `clock_split` puts each check on the reading the code
// takes it on. A disciplined wall (`wall_follows_rate` off) makes a slow
// oscillator's honour reading follow the wall, so the honour side needs an
// undisciplined host to show a rate (`tests/locks.rs`); here the waiter's
// side does. The budget covers the staircase's rounding up to a tick, so
// the clean variants keep a margin where the violating ones have none.

/// The holder (node 0) writes, the delegate (node 1) reads twice; the holder
/// outwaits a lost recall (one message may be dropped). Every oscillator
/// within a quarter of its rate; 8-tick delegations, the delegate's margin 0
/// and the holder's 1. With no budget the holder's wait reading runs fast
/// (a quarter) and ends its outwait `(8 + 1) / 1.25 = 7.2` ticks after the
/// grant, while the delegate honours until 8: the delegate reads its old
/// version after the write was acknowledged.
fn rated_delegation(budget: i16) -> CtoModel {
    CtoModel {
        rate_period: 4,
        max_rate: 1,
        explore_rates: true,
        rate_budget: budget,
        clock_split: true,
        seq_margin: 1,
        deleg_margin: 0,
        lease_margin: 3,
        lease_ttl: 30,
        deleg_ttl: 8,
        max_tick: 11,
        max_drops: 1,
        ..CtoModel::strict(vec![vec![W], vec![R, R]])
    }
}

#[test]
fn strict_rate_beyond_the_budget_violates() {
    assert_violates("rate-no-budget", &rated_delegation(0));
}

/// The same with the budget the oscillators are within: the wait reading
/// runs slow by it, so the holder's outwait ends no sooner than the
/// delegate's window, whatever the rates. Clean, the delegation used.
#[test]
fn strict_rate_within_the_budget_is_clean() {
    assert_clean(
        "rate-within-budget",
        &rated_delegation(1),
        &["all_ops_done", "read_under_delegation"],
    );
}

/// A fast takeover whose successor's oscillator is fast (node 2, half its
/// rate again every two ticks: the models' coarsest resolution) and whose
/// delegate is node 1: the floor is counted on the successor's wait clock.
/// With no budget it passes while the old holder's delegation is still
/// honoured.
fn rated_floor(budget: i16) -> CtoModel {
    CtoModel {
        rate_period: 2,
        max_rate: 1,
        init_rates: Some(vec![0, 0, 1]),
        rate_budget: budget,
        clock_split: true,
        seq_margin: 0,
        deleg_margin: 0,
        takeover_window: 1,
        lease_ttl: 30,
        deleg_ttl: 5,
        max_tick: 9,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![], vec![R, R], vec![W]])
    }
}

#[test]
fn fast_takeover_floor_rate_beyond_the_budget_violates() {
    assert_violates("floor-rate-no-budget", &rated_floor(0));
}

#[test]
fn fast_takeover_floor_rate_within_the_budget_is_clean() {
    assert_clean(
        "floor-rate-within-budget",
        &rated_floor(1),
        &["fast_takeover", "floor_waited", "read_under_delegation"],
    );
}

/// How many ticks before the old holder stops using its lease the
/// successor's floor ends, when the holder (node 0, slow oscillator) beat to
/// the successor (node 2, fast) at t = 0 and renewed its lease at tick
/// `t_r`, and the successor took the lease over two ticks before the
/// holder's own window ended. Positive: the cap ended the floor inside the
/// holder's window (the search for the end-to-end violation is too deep, as
/// for the naive cap below). `None` if the trace does not exist.
fn floor_shortfall(m: &CtoModel, t_r: u8) -> Option<i16> {
    fn enabled(m: &CtoModel, s: &State, a: &Action) -> bool {
        let mut e = Vec::new();
        m.actions(s, &mut e);
        e.contains(a)
    }
    fn to_tick(m: &CtoModel, mut s: State, t: u8) -> Option<State> {
        while s.t < t {
            if !enabled(m, &s, &Action::Tick) {
                return None;
            }
            s = step(m, &s, Action::Tick);
        }
        Some(s)
    }
    let mut s = m.init_states().remove(0);
    s = step(m, &s, Action::Beat(0, 2));
    s = to_tick(m, s, t_r)?;
    if !enabled(m, &s, &Action::Renew(0)) {
        return None;
    }
    s = step(m, &s, Action::Renew(0));
    // The holder stops using its lease when `Renew` is no longer offered.
    let mut probe = s.clone();
    while enabled(m, &probe, &Action::Renew(0)) {
        probe = step(m, &probe, Action::Tick);
    }
    let holder_end = probe.t;
    s = to_tick(m, s, holder_end.saturating_sub(2))?;
    s = step(m, &s, Action::Takeover(2));
    s.nodes[2].ack_floor?;
    s = step(m, &s, Action::Start(2));
    while !enabled(m, &s, &Action::FloorPassed(2)) {
        if !enabled(m, &s, &Action::Tick) {
            return None;
        }
        s = step(m, &s, Action::Tick);
    }
    Some(holder_end as i16 - s.t as i16)
}

fn capped_by_beats(span: Option<i16>) -> CtoModel {
    CtoModel {
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![-1, 0, 1]),
        rate_budget: 1,
        clock_split: true,
        lease_ttl: 10,
        deleg_ttl: 4,
        takeover_window: 2,
        allow_renew: true,
        beat_cap: true,
        beat_span: span,
        max_tick: 40,
        max_drops: 0,
        ..CtoModel::strict_fast(vec![vec![], vec![R, R], vec![W]])
    }
}

/// The beat cap counts `X - sent`, a difference of the holder's wait clock,
/// onto the successor's: a beat sent before the renewal that set `X`
/// measures an interval the holder's slow wait clock under-counts, which the
/// successor's fast one does not. With no span the cap, which takes the
/// least lag of all the beats, ends the floor before the holder's renewed
/// window ends, for some renewal tick, by the drift over the interval
/// (more than the margin pays for).
#[test]
fn beat_cap_rate_before_the_renewal_violates_without_the_span() {
    let m = capped_by_beats(None);
    let worst = (2..=9u8)
        .filter_map(|t_r| floor_shortfall(&m, t_r))
        .max()
        .expect("a trace");
    println!("beat cap, no span: the floor ends up to {worst} tick(s) before the holder's window");
    assert!(
        worst > 0,
        "the cap never ended the floor inside the holder's window (worst {worst})"
    );
}

/// With the span (`M * period / (4 * budget)` ticks at the design's margin
/// of 1: one tick) only a beat sent within it of `X` counts: the beats
/// sent before the renewal are left out, the cap is none for them and the
/// floor is the bound: it never ends inside the holder's window.
#[test]
fn beat_cap_rate_with_the_span_is_clean() {
    let m = capped_by_beats(Some(1));
    let worst = (2..=9u8)
        .filter_map(|t_r| floor_shortfall(&m, t_r))
        .max()
        .expect("a trace");
    assert!(
        worst <= 0,
        "the floor ended {worst} tick(s) inside the holder's window"
    );
}

/// The delegate's wall clock stepped back (one step of up to 3 ticks, the
/// design's margins of 1): with every check on the wall reading it honours
/// its delegation 3 ticks past the holder's outwait, and reads a stale
/// version. No takeover, so no wall compare across nodes is involved.
fn stepped(split: bool, node: u8, dir: i8) -> CtoModel {
    CtoModel {
        clock_split: split,
        max_offset: 3,
        max_jumps: 1,
        step_node: Some(node),
        step_dir: dir,
        seq_margin: 1,
        deleg_margin: 1,
        lease_margin: 3,
        lease_ttl: 30,
        deleg_ttl: 3,
        max_tick: 8,
        max_drops: 1,
        ..CtoModel::strict(vec![vec![W], vec![R, R]])
    }
}

#[test]
fn a_delegate_stepped_back_on_the_wall_clock_violates() {
    assert_violates("delegate-step-back-wall", &stepped(false, 1, -1));
}

/// On its honour clock the step moves nothing (HONOUR ignores a backward
/// step, WAIT every step): clean, with the same margins, the delegation used.
#[test]
fn a_delegate_stepped_back_on_its_honour_clock_is_clean() {
    assert_clean(
        "delegate-step-back-honour",
        &stepped(true, 1, -1),
        &["all_ops_done", "read_under_delegation"],
    );
}

// ---- plan 43 C7b: the claim rule by observation ----
//
// A node that judges the register's lease by comparing its own wall reading
// with the expiry another node wrote on its wall is safe only while every
// offset is below the margin. The observation rule (`ClaimRule::Observed`)
// has a claimant wait one lease TTL of its own WAIT reading from its first
// read of the register's current renewal key and claim on that read's tag:
// no cross-node compare, safe for any offset, for an oscillator within the
// budget.

/// Nodes 1 and 2 are `ahead` ticks ahead of the holder (node 0); node 0
/// reads, node 1 writes. A lease of 6 ticks, margins of 1.
fn claimant_offsets(rule: ClaimRule, offsets: Vec<i16>) -> CtoModel {
    CtoModel {
        init_offsets: Some(offsets),
        claim_rule: rule,
        clock_split: true,
        lease_ttl: 6,
        max_tick: 9,
        max_drops: 0,
        ..CtoModel::strict(vec![vec![R, R], vec![W], vec![W]])
    }
}

/// A claimant whose clock is ahead of the holder's by a whole lease reads
/// the live lease as expired and takes it; the old holder, still inside its
/// window, answers a read from its own replica after the new holder's write
/// completed.
#[test]
fn a_claimant_ahead_beyond_the_margin_violates() {
    assert_violates(
        "claimant-ahead-wall",
        &claimant_offsets(ClaimRule::Wall, vec![0, 6, 6]),
    );
}

/// The same with the holder behind: the same relative offset.
#[test]
fn a_holder_behind_beyond_the_margin_violates() {
    assert_violates(
        "holder-behind-wall",
        &claimant_offsets(ClaimRule::Wall, vec![-6, 0, 0]),
    );
}

/// The observation rule, with the same offsets (and the other way round):
/// clean, and the takeover happens (the claimant waits out the TTL).
#[test]
fn the_observed_claim_is_clean_with_offsets_up_to_a_ttl() {
    for (label, offsets) in [
        ("ahead", vec![0, 6, 6]),
        ("behind", vec![-6, 0, 0]),
        ("far-ahead", vec![0, 9, -9]),
    ] {
        assert_clean(
            &format!("claimant-observed-{label}"),
            &claimant_offsets(ClaimRule::Observed, offsets),
            &["all_ops_done", "takeover"],
        );
    }
}

/// The observation rule with oscillators at the rate budget's edge
/// (`rate_period` 4, budget 1: 25 %) and offsets of a whole lease: clean.
#[test]
fn the_observed_claim_is_clean_with_offsets_and_rates_within_the_budget() {
    let m = CtoModel {
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![1, -1, 0]),
        rate_budget: 1,
        lease_ttl: 8,
        max_tick: 12,
        ..claimant_offsets(ClaimRule::Observed, vec![0, 8, -8])
    };
    assert_clean("claimant-observed-rates", &m, &["all_ops_done", "takeover"]);
}

// (Appendix B row 28, a strict reader that tails S3 past a live holder's
// acknowledged, unshipped writes, is the sim's: this model's log is the
// shipped log, a write is in it when it is acknowledged, so a reader that
// tails misses nothing. `strict-reader-ahead` in the authority sim fails on
// it: 2 of 300 seeds.)
