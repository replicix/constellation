//! Plan 46 M1.0: one stream per delegate node, the commit rule per
//! configuration (`constellation_model::node_stream`).
//!
//! Chunk 1: the skeleton; chunk 2: crashes, root takeover, retry by rid;
//! chunk 3: reconfiguration under old ∪ new. The design's topologies are
//! explored exhaustively under a state cap and a timeout, with every
//! `sometimes` witness required (non-vacuity); each planted bug (a knob)
//! must be found by the checker, with the path printed.
//!
//! `cargo test -p constellation-model --release --test node_stream -- --nocapture`

use constellation_model::delegation::{key, Key, WriteOp};
use constellation_model::node_stream::ROOT;
use constellation_model::node_stream::{GrantKind, NodeGrant, NodeStreamModel};
use stateright::{Checker, HasDiscoveries, Model};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

/// The caps are on unique states. The checker's stop (`target_state_count`
/// counts every generated state) is [`GENERATED`] times the cap, and
/// exhaustiveness is `is_done()` with the generated count below that stop,
/// the unique count below the cap and the time below the timeout (which
/// bounds a loaded host). Chunk 1's topologies.
const CAP: (usize, Duration) = (2_000_000, Duration::from_secs(300));
/// Chunks 2 and 3 (faults, reconfiguration).
const CAP_FAULTS: (usize, Duration) = (5_000_000, Duration::from_secs(600));
/// The generated-state stop as a multiple of the unique cap.
const GENERATED: usize = 4;

const ALWAYS: [&str; 4] = [
    "ack_implies_committed_by_config",
    "acked_never_lost",
    "exactly_once",
    "row_version_matches_config",
];

/// `/a`, `/b`, `/D1/a`, `/D1/b`, `/D2/a`.
const RA: Key = key(0, 0);
const RB: Key = key(0, 1);
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
    cap: (usize, Duration),
    finish: HasDiscoveries,
) -> impl Checker<NodeStreamModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .finish_when(finish)
        .target_state_count(cap.0 * GENERATED)
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

fn assert_clean(label: &str, model: &NodeStreamModel) {
    assert_clean_within(label, model, CAP)
}

fn assert_clean_within(label: &str, model: &NodeStreamModel, cap: (usize, Duration)) {
    let started = Instant::now();
    let checker = run(label, model, cap, HasDiscoveries::All);
    assert!(
        checker.is_done()
            && checker.state_count() < cap.0 * GENERATED
            && checker.unique_state_count() < cap.0
            && started.elapsed() < cap.1,
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
    assert_finds_within(label, model, prop, CAP)
}

/// The checker finds a `prop` counterexample (stopping at the first one).
fn assert_finds_within(
    label: &str,
    model: &NodeStreamModel,
    prop: &'static str,
    cap: (usize, Duration),
) {
    let checker = run(
        label,
        model,
        cap,
        HasDiscoveries::AnyOf(BTreeSet::from([prop])),
    );
    let d = checker
        .discovery(prop)
        .unwrap_or_else(|| panic!("{label}: expected a `{prop}` counterexample"));
    println!("{label}: counterexample for `{prop}`:");
    for (k, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {k:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
    if prop == "ack_implies_committed_by_config" {
        assert_eq!(d.last_state().violation.map(|v| v.property), Some(prop));
    }
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

// ============================================ chunk 2: crashes and takeover

/// C2-T1: a lone root (no backup: its group is the archive), a delegate
/// (node 1) on `/D1` and `/D2` with no backup (its group `{delegate,
/// root}`), and a client (node 2, the only node that can claim the lease:
/// it holds no grant). One crash, one deposition.
fn c2_t1_lone_root() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(RA)],
        vec![create(D1A)],
        vec![create(D1B), create(RB)],
    ])
    .with_grant(placed(1, D1 | D2, 0))
    .with_faults(1, 1)
}

/// C2-T1b: C2-T1 with node 2 as the root's backup (the claimant seals).
fn c2_t1b_root_backup() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(RA)],
        vec![create(D1A)],
        vec![create(D1B), create(RB)],
    ])
    .with_root_backups(1 << 2)
    .with_grant(placed(1, D1 | D2, 0))
    .with_faults(1, 1)
}

/// C2-T2: node 2 backs both the root and the delegate's stream; a crashed
/// delegate's stream is sealed from node 2's copy.
fn c2_t2_backed() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(RA)],
        vec![create(D1A)],
        vec![create(D1B), create(RB)],
    ])
    .with_root_backups(1 << 2)
    .with_grant(placed(1, D1 | D2, 1 << 2))
    .with_faults(1, 1)
}

#[test]
fn c2_design_t1_lone_root_crash_and_takeover() {
    assert_clean_within("c2-t1", &c2_t1_lone_root(), CAP_FAULTS);
}

#[test]
fn c2_design_t1b_root_backup_crash_and_takeover() {
    assert_clean_within("c2-t1b", &c2_t1b_root_backup(), CAP_FAULTS);
}

#[test]
fn c2_design_t2_backed_crash_and_takeover() {
    assert_clean_within("c2-t2", &c2_t2_backed(), CAP_FAULTS);
}

/// ADR-41's lone-journal commit (the old rule) loses an acknowledged row:
/// the lone root reports the delegate's row committed once it is in its
/// journal; the delegate answers; the root pauses and node 2 claims the
/// lease with the archive alone; the delegate crashes. With the I1 oracle
/// off, the harm is shown by `acked_never_lost`, not by definition. The
/// same topology under the new rule is `c2_design_t1_*` (clean).
#[test]
fn c2_knob_root_journal_counts_loses_an_acked_row() {
    let mut m = c2_t1_lone_root();
    m.root_journal_counts = true;
    m.check_i1 = false;
    assert_finds_within(
        "c2-t1+root_journal_counts",
        &m,
        "acked_never_lost",
        CAP_FAULTS,
    );
    // With I1 on, the answer itself is found first.
    m.check_i1 = true;
    assert_finds_within(
        "c2-t1+root_journal_counts+i1",
        &m,
        "ack_implies_committed_by_config",
        CAP_FAULTS,
    );
}

/// A lone delegate's answer is lost when it crashes before the root
/// appends the row.
#[test]
fn c2_knob_lone_delegate_loses_an_acked_row() {
    let mut m = c2_t1_lone_root();
    m.lone_delegate = true;
    m.check_i1 = false;
    assert_finds_within("c2-t1+lone_delegate", &m, "acked_never_lost", CAP_FAULTS);
}

/// Without completed-dedup a retried rid whose row the claimant adopted
/// executes twice.
#[test]
fn c2_knob_no_dedup_executes_twice() {
    let mut m = c2_t1b_root_backup();
    m.no_dedup = true;
    assert_finds_within("c2-t1b+no_dedup", &m, "exactly_once", CAP_FAULTS);
}

// ====================================== chunk 3: reconfiguration (old ∪ new)

/// C3, a delegate's group: node 1 holds `/D1` and `/D2`; the root (node
/// 0, writing `/a`) has no backup; node 2 forwards into the delegate's
/// subtree. One crash.
fn c3_deleg(backups: u8, to: u8) -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(RA)],
        vec![create(D1A), create(D2A)],
        vec![create(D1B)],
    ])
    .with_grant(placed(1, D1 | D2, backups))
    .with_reconfig(1, to)
    .with_faults(1, 0)
}

/// The delegate adds node 2 to `{delegate, root}` (add-then-record).
fn c3_deleg_add() -> NodeStreamModel {
    c3_deleg(0, 1 << 2)
}

/// The delegate removes node 2 (its group becomes `{delegate, root}`);
/// the environment's "peer within budget" flag may drop (inert under the
/// design).
fn c3_deleg_remove() -> NodeStreamModel {
    c3_deleg(1 << 2, 0).with_peer_toggle()
}

/// C3, the lease's configuration: the root changes its backups from
/// `from` to `to`; node 1 is a delegate with the group `{delegate, root}`
/// (its rows commit by the root's configuration at their appended
/// position); node 2 forwards to the root. One crash.
fn c3_root(from: u8, to: u8) -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(RA)], vec![create(D1A)], vec![create(RB)]])
        .with_root_backups(from)
        .with_grant(placed(1, D1 | D2, 0))
        .with_reconfig(ROOT, to)
        .with_faults(1, 0)
}

#[test]
fn c3_design_delegate_adds_backup() {
    assert_clean_within("c3-deleg-add", &c3_deleg_add(), CAP_FAULTS);
}

#[test]
fn c3_design_delegate_removes_backup() {
    assert_clean_within("c3-deleg-remove", &c3_deleg_remove(), CAP_FAULTS);
}

/// The same with the root backed by node 2: the delegate's rows after the
/// record commit on the root's copy before the archive.
#[test]
fn c3_design_delegate_removes_backup_root_backed() {
    // One op at the delegate (the two-op variant passes 5M unique states).
    let m = NodeStreamModel::design(vec![vec![create(RA)], vec![create(D1A)], vec![create(D1B)]])
        .with_root_backups(1 << 2)
        .with_grant(placed(1, D1 | D2, 1 << 2))
        .with_reconfig(1, 0)
        .with_faults(1, 0);
    assert_clean_within("c3-deleg-remove-rb", &m, CAP_FAULTS);
}

#[test]
fn c3_design_root_adds_backup() {
    assert_clean_within("c3-root-add", &c3_root(0, 1 << 2), CAP_FAULTS);
}

#[test]
fn c3_design_root_removes_backup() {
    assert_clean_within("c3-root-remove", &c3_root(1 << 2, 0), CAP_FAULTS);
}

/// A row's class from the mutable "peer within budget" flag at journal
/// time: the stamp is not the configuration in force (M1.1's oracle), the
/// `Lone` row is answered uncommitted (I1), and lost when the delegate
/// crashes (`acked_never_lost`, I1 off).
#[test]
fn c3_knob_per_row_class_is_found() {
    let mut m = c3_deleg_remove();
    m.per_row_class = true;
    assert_finds_within(
        "c3-deleg-remove+per_row_class",
        &m,
        "row_version_matches_config",
        CAP_FAULTS,
    );
    assert_finds_within(
        "c3-deleg-remove+per_row_class+i1",
        &m,
        "ack_implies_committed_by_config",
        CAP_FAULTS,
    );
    m.check_i1 = false;
    assert_finds_within(
        "c3-deleg-remove+per_row_class+lost",
        &m,
        "acked_never_lost",
        CAP_FAULTS,
    );
}

/// Gating a row on the configuration at answer time, not at its journal
/// position: after an added backup's record, a row journaled under
/// `{delegate, root}` is answered on the new backup's copy alone.
#[test]
fn c3_knob_ack_by_latest_config_is_found() {
    let mut m = c3_deleg_add();
    m.ack_by_latest_config = true;
    assert_finds_within(
        "c3-deleg-add+ack_by_latest_config",
        &m,
        "ack_implies_committed_by_config",
        CAP_FAULTS,
    );
    // A removal: a row journaled under `{delegate, node 2}` is answered on
    // the root's committed copy (the root backed by node 2) without node 2
    // holding the delegate's row.
    let mut m = c3_deleg_remove().with_root_backups(1 << 2);
    m.ack_by_latest_config = true;
    assert_finds_within(
        "c3-deleg-remove+ack_by_latest_config",
        &m,
        "ack_implies_committed_by_config",
        CAP_FAULTS,
    );
}
