//! Plan 46 M1.0: one stream per delegate node, the commit rule per
//! configuration (`constellation_model::node_stream`).
//!
//! Chunk 1: the skeleton. The design's topologies are explored
//! exhaustively under a state cap and a timeout, with every `sometimes`
//! witness required (non-vacuity); each planted bug (a knob) must be
//! found by the checker, with the path printed.
//!
//! `cargo test -p constellation-model --release --test node_stream -- --nocapture`

use constellation_model::delegation::{key, Key, WriteOp};
use constellation_model::node_stream::{GrantKind, NodeGrant, NodeStreamModel};
use stateright::{Checker, HasDiscoveries, Model};
use std::time::{Duration, Instant};

/// Exhaustiveness is `is_done()` with the count below the cap; the
/// timeout bounds a loaded host (these runs take well under a second).
const CAP: (usize, Duration) = (2_000_000, Duration::from_secs(300));

const ALWAYS: [&str; 1] = ["ack_implies_committed_by_config"];

/// `/a`, `/D1/a`, `/D1/b`, `/D2/a`.
const RA: Key = key(0, 0);
const D1A: Key = key(1, 0);
const D1B: Key = key(1, 1);
const D2A: Key = key(2, 0);

const D1: u8 = 1 << 1;
const D2: u8 = 1 << 2;

fn create(k: Key) -> WriteOp {
    WriteOp::Create(k)
}

fn unlink(k: Key) -> WriteOp {
    WriteOp::Unlink(k)
}

fn placed(node: u8, dirs: u8, backups: u8) -> NodeGrant {
    NodeGrant {
        node,
        dirs,
        kind: GrantKind::Placed,
        backups,
    }
}

/// T1: a root with no backup (its group is the archive) and one delegate
/// node holding `/D1` and `/D2` in one stream, with no recorded backup (its
/// group is `{delegate, root}`). The root forwards into the delegate's
/// subtree; the delegate writes both of its subtrees.
fn t1_two_subtrees_via_root() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(D1B), create(RA)],
        vec![create(D1A), create(D2A)],
    ])
    .with_grant(placed(1, D1 | D2, 0))
}

/// T1b: T1 with a root backup (node 2), which also writes into the
/// delegate's subtree: the delegate's second copy is the root's append,
/// committed by the root's backup before the archive.
fn t1b_via_root_with_root_backup() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(RA)],
        vec![create(D1A), create(D2A)],
        vec![create(D1B)],
    ])
    .with_root_backups(1 << 2)
    .with_grant(placed(1, D1 | D2, 0))
}

/// T2: node 2 backs both the root and the delegate's stream.
fn t2_backed() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(RA)],
        vec![create(D1A), unlink(D1A)],
        vec![create(D2A)],
    ])
    .with_root_backups(1 << 2)
    .with_grant(placed(1, D1 | D2, 1 << 2))
}

/// T3: a sticky (non-stealable) stream on `/D1`; the root forwards into
/// it and is answered final.
fn t3_sticky() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(D1B), create(RA)],
        vec![create(D1A), unlink(D1B)],
    ])
    .with_grant(NodeGrant {
        node: 1,
        dirs: D1,
        kind: GrantKind::Sticky,
        backups: 0,
    })
}

fn run(
    label: &str,
    model: &NodeStreamModel,
    stop_at_failure: bool,
) -> impl Checker<NodeStreamModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .finish_when(if stop_at_failure {
            HasDiscoveries::AnyFailures
        } else {
            HasDiscoveries::All
        })
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

fn assert_clean(label: &str, model: &NodeStreamModel) {
    let started = Instant::now();
    let checker = run(label, model, false);
    assert!(
        checker.is_done() && checker.unique_state_count() < CAP.0 && started.elapsed() < CAP.1,
        "{label}: expected exhaustive exploration within budget"
    );
    for p in ALWAYS {
        if let Some(d) = checker.discovery(p) {
            panic!(
                "{label}: `{p}` violated: {:?}\npath {:?}",
                d.last_state().violation,
                d.clone().into_actions()
            );
        }
    }
    // Every `sometimes` the model registers for this topology is reached.
    for p in model.properties() {
        if ALWAYS.contains(&p.name) {
            continue;
        }
        assert!(
            checker.discovery(p.name).is_some(),
            "{label}: `{}` never reached (vacuous)",
            p.name
        );
    }
}

fn assert_finds(label: &str, model: &NodeStreamModel, prop: &'static str) {
    let checker = run(label, model, true);
    let d = checker
        .discovery(prop)
        .unwrap_or_else(|| panic!("{label}: expected a `{prop}` counterexample"));
    println!("{label}: counterexample for `{prop}`:");
    for (k, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {k:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
    assert_eq!(d.last_state().violation.map(|v| v.property), Some(prop));
}

// ------------------------------------------------------------- the design

#[test]
fn design_t1_two_subtrees_one_stream_via_root() {
    assert_clean("t1", &t1_two_subtrees_via_root());
}

#[test]
fn design_t1b_via_root_with_root_backup() {
    assert_clean("t1b", &t1b_via_root_with_root_backup());
}

#[test]
fn design_t2_backed_root_and_stream() {
    assert_clean("t2", &t2_backed());
}

#[test]
fn design_t3_sticky_stream_final() {
    assert_clean("t3", &t3_sticky());
}

// ------------------------------------------------ the negative controls

/// A placed delegate that answers from its own disk (the deleted `Lone`
/// class of a delegate with peers) acknowledges an uncommitted row.
#[test]
fn knob_lone_delegate_is_found() {
    let mut m = t1_two_subtrees_via_root();
    m.lone_delegate = true;
    assert_finds("t1+lone_delegate", &m, "ack_implies_committed_by_config");
    let mut m = t2_backed();
    m.lone_delegate = true;
    assert_finds("t2+lone_delegate", &m, "ack_implies_committed_by_config");
}

/// ADR-41's lone-journal branch: the root counts its journal's append of a
/// delegate row as the group's second copy before its own configuration
/// committed it (archive, or its backup).
#[test]
fn knob_root_journal_counts_is_found() {
    let mut m = t1_two_subtrees_via_root();
    m.root_journal_counts = true;
    assert_finds(
        "t1+root_journal_counts",
        &m,
        "ack_implies_committed_by_config",
    );
    let mut m = t1b_via_root_with_root_backup();
    m.root_journal_counts = true;
    assert_finds(
        "t1b+root_journal_counts",
        &m,
        "ack_implies_committed_by_config",
    );
}

/// The knobs are inert where they cannot apply: a stream with its own
/// backup does not count the root's copy, and a sticky stream is final.
#[test]
fn knobs_inert_where_they_do_not_apply() {
    let mut m = t2_backed();
    m.root_journal_counts = true;
    assert_clean("t2+root_journal_counts", &m);
    let mut m = t3_sticky();
    m.lone_delegate = true;
    m.root_journal_counts = true;
    assert_clean("t3+both", &m);
}
