//! Plan 30 §M9: the backup / seal / `ack=s3` model
//! (`constellation_model::backup`).
//!
//! The counterexamples first — today's `Local` acknowledgement loses an
//! acknowledged write when the holder dies; `ack=s3` that acknowledges on
//! issuing the PUT instead of on landing; a holder that acknowledges a
//! write that needed a backup before the removal CAS landed; a backup
//! that takes over without being listed — each asserted *found* by the
//! checker, with the path printed. Then the design, clean: `Backup` with
//! a crash, a seal-based takeover, a removal and an addition; `S3` with a
//! crash and a fast takeover; every acknowledged write in the log, in
//! acknowledgement order.

use constellation_model::backup::{Action, BackupModel};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

const CAP: (usize, Duration) = (30_000_000, Duration::from_secs(55));

fn run(label: &str, model: &BackupModel) -> impl Checker<BackupModel> {
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

fn assert_violates(label: &str, model: &BackupModel, property: &'static str) {
    let checker = run(label, model);
    let d = checker
        .discovery(property)
        .unwrap_or_else(|| panic!("{label}: expected a `{property}` counterexample"));
    println!("{label}: counterexample for `{property}`:");
    for (s, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {s:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
}

fn assert_clean(label: &str, model: &BackupModel, sometimes: &[&'static str]) {
    let checker = run(label, model);
    assert!(
        checker.is_done() && checker.state_count() < CAP.0,
        "{label}: expected exhaustive exploration within budget"
    );
    let mut props = vec!["acked_never_lost", "ack_order_is_log_order"];
    // Without `commit_rule` a deposed holder replays what it journaled
    // (`today_a_deposed_holder_replays_committed_rows`).
    if model.commit_rule {
        props.push("acked_never_rolled_back");
    }
    for p in props {
        if let Some(d) = checker.discovery(p) {
            panic!(
                "{label}: `{p}` violated: {:?}\n{:#?}",
                d.last_state().violation,
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

/// Today: the holder acknowledges on journaling; a crash before the ship
/// loses the write (plan 30 §1.2's L2).
#[test]
fn local_policy_loses_an_acked_write_on_a_crash() {
    let m = BackupModel::local(2, 1);
    assert_violates("local", &m, "acked_never_lost");
}

/// `ack=s3` acknowledging when the PUT is *issued* rather than when it
/// landed: a crash in between loses the write.
#[test]
fn ack_s3_before_landing_loses_an_acked_write() {
    let m = BackupModel {
        ack_on_landing: false,
        ..BackupModel::s3(2, 1)
    };
    assert_violates("s3-before-landing", &m, "acked_never_lost");
}

/// The holder acknowledges a write that needed the backup it decided to
/// remove *before* the removal CAS landed, then dies; the still-listed
/// backup takes over without that write.
#[test]
fn acking_before_the_removal_cas_lands_loses_an_acked_write() {
    let m = BackupModel {
        wait_for_reconfig: false,
        ..BackupModel::backup(2, 1)
    };
    assert_violates("remove-before-cas", &m, "acked_never_lost");
}

/// Two backups; the holder removes one, and its acknowledgements then
/// rest on the listed one alone. The removed backup has not read the
/// register yet: if its takeover did not check the listing, its stale
/// tail would replace the log's holder without what the dead holder
/// acknowledged.
#[test]
fn an_unlisted_backup_taking_over_loses_an_acked_write() {
    let m = BackupModel {
        takeover_requires_listing: false,
        initial_backups: vec![1, 2],
        max_tick: 0,
        max_epoch: 2,
        ..BackupModel::backup(3, 1)
    };
    assert_violates("unlisted-takeover", &m, "acked_never_lost");
}

/// The design under `Backup`: two nodes, one backup, a crash, a seal and
/// a takeover with the tail re-shipped, a removal — every durably
/// acknowledged write is in the log (or on a live node), in
/// acknowledgement order.
#[test]
fn backup_with_a_crash_is_clean() {
    let m = BackupModel {
        max_tick: 2,
        max_epoch: 2,
        ..BackupModel::backup(2, 1)
    };
    assert_clean(
        "backup",
        &m,
        &[
            "seal_takeover",
            "tail_reshipped",
            "backup_removed",
            "all_acked_and_in_log",
        ],
    );
}

/// Three nodes: the third node's writes are forwarded, and the backup
/// takes over under them (no reconfiguration: that is the ignored
/// variant below, over the 30M-state budget).
#[test]
fn backup_three_nodes_is_clean() {
    let m = BackupModel {
        max_tick: 0,
        max_epoch: 2,
        max_crashes: 0,
        reconfig: false,
        ..BackupModel::backup(3, 1)
    };
    assert_clean("backup-3", &m, &["seal_takeover", "all_acked_and_in_log"]);
}

/// Three nodes with a reconfiguration (the third node added as a backup,
/// or the first removed) and a crash. Over the budget: run by hand with
/// `--ignored`.
#[test]
#[ignore]
fn backup_three_nodes_with_reconfiguration_is_clean() {
    let m = BackupModel {
        max_tick: 0,
        max_reconfigs: 1,
        max_epoch: 2,
        max_crashes: 1,
        ..BackupModel::backup(3, 1)
    };
    assert_clean(
        "backup-3-reconfig",
        &m,
        &["seal_takeover", "all_acked_and_in_log"],
    );
}

/// The design under `ack=s3`: no backups, acknowledgements on landing, a
/// crash and a fast takeover fenced by the log slot.
#[test]
fn ack_s3_with_a_crash_and_fast_takeover_is_clean() {
    let m = BackupModel {
        max_tick: 2,
        ..BackupModel::s3(3, 1)
    };
    assert_clean("s3", &m, &["s3_fast_takeover", "all_acked_and_in_log"]);
}

/// Plan 43 C0 (fail-before for C1): today a deposed holder replays, by rid,
/// the writes it journaled — including the ones its backup acknowledged,
/// which are committed. The backup seals and takes over; the old holder
/// tails the successor's marker and strands its journal into its replay
/// set. Violates `acked_never_rolled_back` with `commit_rule: false`.
#[test]
fn today_a_deposed_holder_replays_committed_rows() {
    let m = BackupModel::backup(2, 1);
    // The shortest counterexample, by hand: node 0 (the holder) executes
    // its write and sends it to its backup; the backup holds and
    // acknowledges it; the holder acknowledges durably; the backup seals
    // and takes over; the holder tails the marker.
    let path = vec![
        Action::Start(0),
        Action::Deliver(0), // Append reaches the backup
        Action::Deliver(0), // AppendAck reaches the holder: acknowledged
        Action::Seal(1),
        Action::Takeover(1),
        Action::Tail(0), // deposed: the journal becomes its replay set
    ];
    // Capped: the counterexample is shallow, the search need not finish.
    let started = Instant::now();
    let checker = m
        .clone()
        .checker()
        .target_state_count(2_000_000)
        .timeout(Duration::from_secs(30))
        .spawn_bfs()
        .join();
    println!(
        "deposed-replay: {} states ({} unique), max depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed()
    );
    let d = checker
        .discovery("acked_never_rolled_back")
        .expect("expected an `acked_never_rolled_back` counterexample");
    for (n, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {n:2}: {a:?}");
    }
    checker.assert_discovery("acked_never_rolled_back", path);
}

/// Plan 43 C1-3: three nodes, node 1 the only backup; the TTL path only (no
/// seal takeover, no reconfiguration) with one crash. The holder
/// acknowledges a write that only its backup holds, dies, and the lease
/// expires.
fn ttl_claim_model(tail_fetch: bool) -> BackupModel {
    BackupModel {
        commit_rule: true,
        tail_fetch,
        fast_takeover: false,
        reconfig: false,
        max_epoch: 2,
        max_tick: 1,
        lease_ttl: 1,
        ..BackupModel::backup(3, 1)
    }
}

/// Plan 43 C1-3 (fail-before): today a TTL claimant of a lease with a
/// listed backup carries no tail, so a third node that claims after the
/// holder died leaves the acknowledged write on the backup's unlisted copy.
/// The checker reports it as `acked_never_lost` (the write is on no node the
/// register names), not as a rollback: the old holder is dead, so nothing
/// replays it. The name is the plan's.
#[test]
fn a_ttl_claim_without_the_tail_fetch_rolls_back_an_acked_write() {
    assert_violates("ttl-claim", &ttl_claim_model(false), "acked_never_lost");
}

/// Plan 43 C1-3: the same configuration with `tail_fetch`: the claimant
/// fetches the backup's tail (and seals it) or waits; no acknowledged write
/// is lost.
#[test]
fn a_ttl_claim_with_the_tail_fetch_is_clean() {
    assert_clean(
        "ttl-claim-fetch",
        &ttl_claim_model(true),
        &["tail_fetched", "all_acked_and_in_log"],
    );
}

/// Plan 43 C1-3 (D2): four nodes, holder 0 with backups 1 and 2, a crash of
/// the holder and of its successor. Node 0 acknowledges a write held by 1
/// and 2; 1 seals and takes over, re-shipping the tail into a tenure with no
/// listed backups, and dies before shipping it. The write is then on node 2's
/// copy of the old group only.
fn chained_takeover_model(inherited_tail: bool) -> BackupModel {
    BackupModel {
        commit_rule: true,
        tail_fetch: true,
        inherited_tail,
        initial_backups: vec![1, 2],
        writers: 1,
        reconfig: false,
        max_crashes: 2,
        max_epoch: 3,
        max_tick: 1,
        lease_ttl: 1,
        ..BackupModel::backup(4, 1)
    }
}

/// Plan 43 C1-3 (D2's fail-before): without the inherited tail the
/// successor's lease names no group, so a claimant of it fetches nothing and
/// the old group's copy (which drops at the successor's marker) is
/// unreachable: `acked_never_lost` is violated.
#[test]
fn a_chained_takeover_without_the_inherited_tail_loses_an_acked_write() {
    assert_violates(
        "chained-no-inherited",
        &chained_takeover_model(false),
        "acked_never_lost",
    );
}

/// Plan 43 C1-3 (D2): with the inherited tail the lease names the old group,
/// its members keep their copy until the log carries the rows, and a third
/// node's TTL claim fetches from them: nothing is lost.
#[test]
fn a_chained_takeover_with_the_inherited_tail_is_clean() {
    assert_clean(
        "chained-inherited",
        &chained_takeover_model(true),
        &["tail_fetched", "inherited_tail_fetched"],
    );
}

/// Plan 43 C1-3: the whole design with both knobs — seal and TTL takeovers,
/// a removal and an addition, a crash, three nodes (one writer; with every
/// node writing it is over the budget).
#[test]
fn tail_fetch_and_inherited_tail_three_nodes_with_reconfiguration_is_clean() {
    let m = BackupModel {
        tail_fetch: true,
        inherited_tail: true,
        writers: 1,
        max_tick: 1,
        lease_ttl: 1,
        max_epoch: 3,
        ..BackupModel::backup(3, 1)
    };
    assert_clean(
        "fetch-inherited-3-reconfig",
        &m,
        &["seal_takeover", "tail_fetched", "all_acked_and_in_log"],
    );
}

/// Plan 43 C1-4: the design with every knob the real protocol has — a
/// backed holder deposed by its backup's seal takeover keeps its gated rows
/// as a member copy (never in its replay set), the successor re-ships, a
/// removal, a crash. Two nodes.
#[test]
fn commit_rule_two_nodes_is_clean() {
    let m = BackupModel {
        commit_rule: true,
        tail_fetch: true,
        inherited_tail: true,
        max_tick: 2,
        max_epoch: 2,
        ..BackupModel::backup(2, 1)
    };
    assert_clean(
        "commit-rule-2",
        &m,
        &[
            "seal_takeover",
            "tail_reshipped",
            "backup_removed",
            "member_copy_kept",
            "member_copy_retired",
            "all_acked_and_in_log",
        ],
    );
}

/// Plan 43 C1-4: three nodes (one writer with two writes; every node writing
/// is the ignored variant below), a removal and an addition, seal and TTL
/// takeovers, a crash, the chain of two takeovers.
#[test]
fn commit_rule_three_nodes_is_clean() {
    let m = BackupModel {
        commit_rule: true,
        tail_fetch: true,
        inherited_tail: true,
        writers: 1,
        max_tick: 1,
        lease_ttl: 1,
        max_epoch: 3,
        ..BackupModel::backup(3, 2)
    };
    assert_clean(
        "commit-rule-3",
        &m,
        &[
            "seal_takeover",
            "tail_fetched",
            "member_copy_kept",
            "member_copy_retired",
            "all_acked_and_in_log",
        ],
    );
}

/// Plan 43 C1-4: the chained takeover of D2 with the commit rule — the
/// successor dies before shipping and a TTL claimant fetches the old group's
/// tail from a member copy (a deposed holder's, or a backup's kept tail).
#[test]
fn commit_rule_chained_takeover_fetches_a_member_copy() {
    assert_clean(
        "commit-rule-chained",
        &chained_takeover_model(true),
        &[
            "tail_fetched",
            "inherited_tail_fetched",
            "member_copy_fetched",
        ],
    );
}

/// Plan 43 C1-4 (fail-before for rule 2 of spec §6.4): a successor's marker
/// that drops the whole member copy of the epoch it carries the tail of —
/// not only the rows past `through` — leaves the re-shipped rows with the
/// successor alone; when it dies before shipping, the acknowledged write is
/// lost. Rows are retired by the log carrying them, not by the marker.
#[test]
fn dropping_the_member_copy_at_the_marker_loses_an_acked_write() {
    let m = BackupModel {
        drop_member_at_tail_follows: true,
        ..chained_takeover_model(true)
    };
    assert_violates("drop-at-marker", &m, "acked_never_lost");
}

/// Plan 43 C1-4: the same chain with the three nodes all writing is over the
/// budget for the default run (about 25M states, 20 s).
#[test]
#[ignore]
fn commit_rule_three_nodes_all_writing_is_clean() {
    let m = BackupModel {
        commit_rule: true,
        tail_fetch: true,
        inherited_tail: true,
        max_tick: 1,
        lease_ttl: 1,
        max_epoch: 3,
        ..BackupModel::backup(3, 1)
    };
    assert_clean(
        "commit-rule-3-all-writers",
        &m,
        &["seal_takeover", "tail_fetched", "member_copy_kept"],
    );
}

/// Plan 43 C1-4: `S3` with the commit rule — an `Archive`-class row of a
/// deposed holder is never acknowledged, so the marker without a tail drops
/// it (rule 4); no acknowledged write is replayed or lost.
#[test]
fn commit_rule_s3_is_clean() {
    let m = BackupModel {
        commit_rule: true,
        max_tick: 1,
        ..BackupModel::s3(3, 1)
    };
    assert_clean(
        "commit-rule-s3",
        &m,
        &["s3_fast_takeover", "all_acked_and_in_log"],
    );
}
