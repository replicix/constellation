//! Plan 30 §M10: continuation epochs with flexible quorums
//! (`constellation_model::flex::FlexEpochs`).
//!
//! First the naive variants, each asserted *found* by the checker with
//! the path printed: `f > 0` without the takeover's promise check; a node
//! joining an epoch before its own promise expired; an epoch formed with
//! fewer than `N − f` members; and the extra rules this model found
//! load-bearing (persist the promise before the PUT, read the heartbeats
//! after the expiry, stay silent while a member, the lease margin under
//! drift). Then plan 30 §M9's fast takeovers against an epoch that
//! claims every lease (what M9 as built plus a naive M10 would do), and
//! the claim rule that keeps them apart. Then the full rule, clean, with
//! honest clocks and under drift, and `f = 0` (today's rule) clean.
//!
//! `cargo test -p constellation-model --release --test flex_epochs -- --nocapture`

use constellation_model::flex::{FlexEpochs, Policy};
use stateright::{Checker, HasDiscoveries, Model};
use std::time::{Duration, Instant};

const CAP: (usize, Duration) = (30_000_000, Duration::from_secs(55));
/// The `#[ignore]`d runs' budget.
const BIG_CAP: (usize, Duration) = (400_000_000, Duration::from_secs(3600));
const ALWAYS: [&str; 3] = [
    "single_authority",
    "linearizable",
    "converged_at_quiescence",
];

fn run(label: &str, model: &FlexEpochs, stop_at_failure: bool) -> impl Checker<FlexEpochs> {
    run_capped(label, model, stop_at_failure, CAP)
}

fn run_capped(
    label: &str,
    model: &FlexEpochs,
    stop_at_failure: bool,
    cap: (usize, Duration),
) -> impl Checker<FlexEpochs> {
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

fn assert_violates(label: &str, model: &FlexEpochs, property: &'static str) {
    let checker = run(label, model, true);
    let d = checker
        .discovery(property)
        .unwrap_or_else(|| panic!("{label}: expected a `{property}` counterexample"));
    println!("{label}: counterexample for `{property}`:");
    let s0 = d.clone().into_states();
    println!("  offsets {:?}", s0[0].off);
    for (k, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {k:2}: {a:?}");
    }
    let last = d.last_state();
    println!(
        "  final: t={} reg={:?} authorities={:?} epochs={:?} history={:?}",
        last.t,
        last.reg,
        last.authorities(model),
        last.epochs,
        last.history
    );
}

fn assert_clean(label: &str, model: &FlexEpochs, sometimes: &[&'static str]) {
    assert_clean_capped(label, model, sometimes, CAP)
}

fn assert_clean_capped(
    label: &str,
    model: &FlexEpochs,
    sometimes: &[&'static str],
    cap: (usize, Duration),
) {
    let checker = run_capped(label, model, false, cap);
    assert!(
        checker.is_done() && checker.state_count() < cap.0,
        "{label}: expected exhaustive exploration within budget"
    );
    for p in ALWAYS {
        if let Some(d) = checker.discovery(p) {
            let last = d.last_state();
            panic!(
                "{label}: `{p}` violated (authorities {:?}):\n{:#?}",
                last.authorities(model),
                d.clone().into_actions()
            );
        }
    }
    for p in sometimes {
        assert!(
            checker.discovery(p).is_some(),
            "{label}: `{p}` never witnessed (vacuous?)"
        );
    }
}

fn full() -> FlexEpochs {
    let m = FlexEpochs::new(3, 1);
    assert!(m.ttl_rule_holds());
    m
}

/// Drift `D = 1` with the lease's margin `M = 3 > 2D`; the initial
/// lease and promises placed so an epoch can still form inside the
/// holder's margin.
fn drifting(m: FlexEpochs) -> FlexEpochs {
    FlexEpochs {
        initial_expiry: 4,
        initial_promise: 0,
        max_tick: 6,
        ..m.with_drift(1, 3)
    }
}

// ---- the naive variants the plan names ----

/// `f = 1` without the takeover check: A and B form an epoch holding A's
/// lease while C is away; C comes back with S3 after the expiry and
/// takes the lease.
#[test]
fn naive_no_takeover_check_splits_brain() {
    let m = FlexEpochs {
        takeover_promise_check: false,
        ..full()
    };
    assert_violates("no-takeover-check", &m, "single_authority");
}

/// A node joins while its own promise is still out: C reads B's
/// unexpired promise after the expiry, counts it, and takes over the
/// lease the epoch holds.
#[test]
fn naive_join_before_own_promise_expired_splits_brain() {
    let m = FlexEpochs {
        join_after_own_promise: false,
        ..full()
    };
    assert_violates("join-early", &m, "single_authority");
}

/// An epoch of one (`< N − f`): B's live promise satisfies C's check.
#[test]
fn naive_fewer_than_n_minus_f_splits_brain() {
    let m = FlexEpochs {
        min_members: 1,
        ..full()
    };
    assert_violates("small-epoch", &m, "single_authority");
}

// ---- rules the model shows load-bearing beyond the plan's list ----

/// Persisting the promise only once its PUT acknowledged: a node joins
/// while a PUT it issued is still in flight, and the PUT lands after.
#[test]
fn naive_persist_after_publish_splits_brain() {
    let m = FlexEpochs {
        persist_before_publish: false,
        ..full()
    };
    assert_violates("persist-after-put", &m, "single_authority");
}

/// A heartbeat read taken before the lease expired, used for the CAS
/// after it.
#[test]
fn naive_heartbeat_read_before_expiry_splits_brain() {
    let m = FlexEpochs {
        read_after_expiry: false,
        ..full()
    };
    assert_violates("early-read", &m, "single_authority");
}

/// A member that keeps publishing promises while its epoch is open.
#[test]
fn naive_member_keeps_promising_splits_brain() {
    let m = FlexEpochs {
        silent_in_epoch: false,
        ..full()
    };
    assert_violates("chatty-member", &m, "single_authority");
}

/// Under drift `D = 1`, a lease margin `M = 1 < 2D` lets a taker in
/// while the old holder still uses its lease (the lease's own rule; the
/// promise check adds no margin requirement of its own — see
/// `full_rule_is_clean_under_drift`).
#[test]
fn margin_below_twice_the_drift_splits_brain() {
    let m = full().with_drift(1, 1);
    assert_violates("margin<2D", &m, "single_authority");
}

// ---- plan 30 §M9's fast takeovers ----

/// M9 as built: the epoch holder acknowledges `Local` whatever the
/// lease's policy. The listed backup C is the missing node; it seals and
/// takes over (no TTL, no promise check) while the epoch writes.
#[test]
fn m9_seal_takeover_against_an_unguarded_epoch_splits_brain() {
    let m = FlexEpochs {
        policy: Policy::Backup(2),
        fast_takeover: true,
        claim_rule: false,
        ..full()
    };
    assert_violates("seal-vs-epoch", &m, "single_authority");
}

/// M9 as built under `ack=s3`: anyone takes the silent holder's lease
/// over at once.
#[test]
fn m9_ack_s3_fast_takeover_against_an_unguarded_epoch_splits_brain() {
    let m = FlexEpochs {
        policy: Policy::S3,
        fast_takeover: true,
        claim_rule: false,
        ..full()
    };
    assert_violates("ack-s3-vs-epoch", &m, "single_authority");
}

/// The claim rule: a `Backup` lease is carried only by an epoch its
/// backup is a member of, so the backup (a member) never takes over
/// while it is open, and an epoch without it holds nothing.
#[test]
fn m9_seal_takeover_with_the_claim_rule_is_clean() {
    let m = FlexEpochs {
        policy: Policy::Backup(2),
        fast_takeover: true,
        ..full()
    };
    assert_clean(
        "seal+claim-rule",
        &m,
        &[
            "seal_takeover",
            "epoch_flushed",
            "all_ops_done",
            "checked_takeover",
        ],
    );
}

/// A member may believe it holds a lease that is gone: the old holder
/// whose listed backup sealed and took it over (M9, no TTL). An epoch
/// that takes the first claim it sees — ignoring the backup's own newer
/// claim, or, after the backup restarted without S3, its persisted seal
/// — carries the stale one; its holder executes on a replica missing the
/// new holder's writes.
#[test]
fn m9_stale_claim_without_epoch_resolution_breaks() {
    let m = FlexEpochs {
        policy: Policy::Backup(2),
        fast_takeover: true,
        resolve_by_epoch: false,
        ..full()
    };
    assert_violates("stale-claim", &m, "linearizable");
}

/// The claim rule: an `ack=s3` lease is never carried by an epoch.
#[test]
fn m9_ack_s3_with_the_claim_rule_is_clean() {
    let m = FlexEpochs {
        policy: Policy::S3,
        fast_takeover: true,
        ..full()
    };
    assert_clean(
        "ack-s3+claim-rule",
        &m,
        &["fast_s3_takeover", "all_ops_done"],
    );
}

// ---- the full rule ----

/// `N = 3`, `f = 1`, honest clocks, a crash with restart: never two
/// authorities; an epoch forms with a node missing, a takeover passes
/// the promise check, an epoch flushes its journal.
#[test]
fn full_rule_is_clean() {
    assert_clean(
        "full",
        &full(),
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
    );
}

/// The same under drift `D = 1` with the lease's margin `M = 3 > 2D`,
/// every clock at `±D` (`full_rule_is_clean_under_every_drift` takes
/// every offset).
#[test]
fn full_rule_is_clean_under_drift() {
    let m = FlexEpochs {
        inflight_puts: false,
        ..drifting(full())
    };
    assert_clean(
        "full+drift",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
    );
}

/// A simpler check: count a promise when its `no_epoch_until` outlasts
/// the lease's recorded `expires` — no read-time clock involved, so the
/// heartbeats may be read at any time (here: even before the expiry).
#[test]
fn expiry_based_check_is_clean_under_drift() {
    let m = FlexEpochs {
        compare_to_expiry: true,
        read_after_expiry: false,
        inflight_puts: false,
        ..drifting(full())
    };
    assert_clean(
        "expiry-check+drift",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
    );
}

/// The expiry-based check is no substitute for the join rule: a holder
/// that joins while its own promise (published past its expiry) is out
/// is counted by the taker.
#[test]
fn expiry_based_check_still_needs_the_join_rule() {
    let m = FlexEpochs {
        compare_to_expiry: true,
        read_after_expiry: false,
        join_after_own_promise: false,
        ..full()
    };
    assert_violates("expiry-check+join-early", &m, "single_authority");
}

/// The same with honest clocks, heartbeat PUTs in flight and a crash.
#[test]
fn expiry_based_check_is_clean() {
    let m = FlexEpochs {
        compare_to_expiry: true,
        read_after_expiry: false,
        ..full()
    };
    assert_clean(
        "expiry-check",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
    );
}

/// Plan 30 §M10 phase 2: promises published only on demand — after the
/// lease's expiry was observed (by the promiser, or by a taker asking
/// it) — with the expiry-based check, the heartbeats read at any time.
/// Safety never depended on the cadence; this checks it.
#[test]
fn on_demand_promises_are_clean() {
    let m = FlexEpochs {
        on_demand: true,
        compare_to_expiry: true,
        read_after_expiry: false,
        ..full()
    };
    assert_clean(
        "on-demand",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
    );
}

/// The same under drift (every clock at `±D`, PUTs landing at once, no
/// crash: the stale-observation field makes more too big for CI;
/// `on_demand_promises_are_clean_under_every_drift` adds PUTs in
/// flight).
#[test]
fn on_demand_promises_are_clean_under_drift() {
    let m = FlexEpochs {
        on_demand: true,
        compare_to_expiry: true,
        read_after_expiry: false,
        inflight_puts: false,
        extreme_offsets: true,
        max_crashes: 0,
        ..drifting(full())
    };
    assert_clean(
        "on-demand+drift",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
    );
}

/// Clocks at `±D` with PUTs in flight (with a crash as well it does not
/// fit 16 GB; the honest-clock run has the crash).
#[test]
#[ignore]
fn on_demand_promises_are_clean_under_every_drift() {
    let m = FlexEpochs {
        on_demand: true,
        compare_to_expiry: true,
        read_after_expiry: false,
        extreme_offsets: true,
        max_crashes: 0,
        ..drifting(full())
    };
    assert_clean_capped(
        "on-demand+every-drift",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
        BIG_CAP,
    );
}

/// On demand still needs the join rule, because an observation can be
/// stale: B saw an expiry, the holder renewed since, and B's clock passes
/// the old expiry just before the new one — its promise then outlasts the
/// current expiry while the holder still uses its lease. (A lease TTL of
/// 4 against a promise TTL of 2 — outside the TTL rule, which is a
/// liveness rule — keeps the renewal inside the tick horizon.)
#[test]
fn on_demand_promises_still_need_the_join_rule() {
    let m = FlexEpochs {
        on_demand: true,
        compare_to_expiry: true,
        read_after_expiry: false,
        join_after_own_promise: false,
        lease_ttl: 4,
        max_tick: 7,
        max_crashes: 0,
        ..full()
    };
    assert_violates("on-demand+join-early", &m, "single_authority");
}

/// `f = 0`: today's rule (every roster node a member, no promises, no
/// check) is clean.
#[test]
fn slack_zero_is_todays_rule_and_clean() {
    let m = FlexEpochs::new(3, 0);
    assert_clean("f=0", &m, &["epoch_flushed", "all_ops_done"]);
}

/// Four nodes, `f = 1`, honest clocks: bigger than the CI budget.
#[test]
#[ignore]
fn full_rule_four_nodes() {
    let m = FlexEpochs::new(4, 1);
    assert!(m.ttl_rule_holds());
    assert_clean_capped(
        "full-4",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
        BIG_CAP,
    );
}

/// Drift with heartbeat PUTs in flight across steps and crashes, every
/// clock at `±D` (the CI drift run lands each PUT at once instead).
#[test]
#[ignore]
fn full_rule_is_clean_under_drift_with_inflight_puts() {
    let m = FlexEpochs {
        extreme_offsets: true,
        ..drifting(full())
    };
    assert_clean_capped(
        "full+drift+inflight",
        &m,
        &[
            "epoch_with_missing_node",
            "checked_takeover",
            "epoch_flushed",
            "all_ops_done",
        ],
        BIG_CAP,
    );
}
