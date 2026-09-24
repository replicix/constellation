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

use constellation_model::backup::BackupModel;
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
    for p in ["acked_never_lost", "ack_order_is_log_order"] {
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
