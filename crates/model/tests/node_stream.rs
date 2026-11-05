//! Plan 46 M1.0: one stream per delegate node, the commit rule per
//! configuration (`constellation_model::node_stream`).
//!
//! Chunk 1: the skeleton; chunk 2: crashes, root takeover, retry by rid;
//! chunk 3: reconfiguration under old ∪ new; chunk 4: subtree sets,
//! release, recall as a request (and the §9 verdict: one cut per stream);
//! chunk 5: per-key linearizability and the causal cut; chunk 6: the end
//! of a placed grant, C3's verbatim re-ship, L1 and L2b; chunk 7: sticky
//! grants, the abandon and ADR-43's no-tail orphan rule; chunk 8: the
//! singleton, `first_peer_join` (the roster's journaled bump) and the
//! root's own sticky grants; chunk 9: settlement (I4), S4 and no server
//! queue (I2(b)). The design's
//! topologies are explored exhaustively under a state cap and a timeout,
//! with every `always` property the model registers checked and every
//! `sometimes` witness required (non-vacuity); each planted bug (a knob)
//! must be found by the checker, with the path printed.
//!
//! `cargo test -p constellation-model --release --test node_stream -- --nocapture`
//!
//! The large topologies are `#[ignore]`d so that the default
//! `cargo test -p constellation-model` stays short; the exhaustive set:
//! `cargo test -p constellation-model --release --test node_stream -- --include-ignored --nocapture`
//!
//! Sim pins of the carried findings: L1 is
//! `l1_a_former_delegates_own_row_keeps_its_log_place`, L2b is
//! `l2b_findings_a_committed_delegate_row_lands_with_its_identity` (both in
//! `crates/authority/tests/sim.rs`).

use constellation_model::delegation::{key, Key, WriteOp};
use constellation_model::node_stream::ROOT;
use constellation_model::node_stream::{GrantKind, NodeGrant, NodeStreamModel};
use stateright::{Checker, Expectation, HasDiscoveries, Model};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

/// The caps are on unique states. The checker's stop (`target_state_count`
/// counts every generated state) is [`GENERATED`] times the cap, and
/// exhaustiveness is `is_done()` with the generated count below that stop,
/// the unique count below the cap and the time below the timeout (which
/// bounds a loaded host). Chunk 1's topologies.
const CAP: (usize, Duration) = (2_000_000, Duration::from_secs(300));
/// Chunks 2 to 4 and 6 (faults, reconfiguration, release, end).
const CAP_FAULTS: (usize, Duration) = (5_000_000, Duration::from_secs(600));
/// Chunks 5 and 6 with histories and replicas.
const CAP_LARGE: (usize, Duration) = (10_000_000, Duration::from_secs(900));
/// Chunk 5's deep variant.
const CAP_DEEP: (usize, Duration) = (20_000_000, Duration::from_secs(1800));
/// The generated-state stop as a multiple of the unique cap.
const GENERATED: usize = 4;

/// Properties checked as an event (a `Violation` in the state).
const EVENTS: [&str; 5] = [
    "ack_implies_committed_by_config",
    "recall_safety",
    "two_subtrees_one_order",
    "no_lone_answer_with_peer",
    "settle_after_reads_recalled",
];

/// `/a`, `/b`, `/D1/a`, `/D1/b`, `/D2/a`, `/D2/b`.
const RA: Key = key(0, 0);
const RB: Key = key(0, 1);
const D1A: Key = key(1, 0);
const D1B: Key = key(1, 1);
const D2A: Key = key(2, 0);
const D2B: Key = key(2, 1);

const D1: u8 = 1 << 1;
const D2: u8 = 1 << 2;

fn create(k: Key) -> WriteOp {
    WriteOp::Create(k)
}

fn unlink(k: Key) -> WriteOp {
    WriteOp::Unlink(k)
}

fn mv(a: Key, b: Key) -> WriteOp {
    WriteOp::Move(a, b)
}

fn present(keys: &[Key]) -> u16 {
    keys.iter().fold(0, |v, k| v | (1 << k))
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
    for p in model.properties() {
        if p.expectation != Expectation::Always {
            continue;
        }
        let p = p.name;
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
        if p.expectation != Expectation::Sometimes {
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
    if EVENTS.contains(&prop) {
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

// ============================== chunk 4: subtree sets, release, recall by request

/// C4-T1: node 1 holds `/D1` and `/D2` in one stream (group `{delegate,
/// root}`; the root has no backup). The root renames `/D1/a` to `/a`: a
/// cross-subtree op, parked while it requests `/D1`'s release. Node 1
/// unlinks `/D1/a`, then renames `/D2/a` to `/D1/b`: an ordinary op of its
/// stream while it holds both subtrees, a `NotOwner` while the root's table
/// lags its release, and a cross-subtree op (a second request, for `/D2`)
/// once the release is in the root's journal. `/D1/a` and `/D2/a` exist
/// initially. One crash.
fn c4_t1() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![mv(D1A, RA)], vec![unlink(D1A), mv(D2A, D1B)]])
        .with_initial(present(&[D1A, D2A]))
        .with_grant(placed(1, D1 | D2, 0))
        .with_faults(1, 0)
}

/// C4-T2: C4-T1 with node 2 backing the root and the stream; one crash and
/// one deposition (node 2 claims, adopts its copy and re-streams).
fn c4_t2() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![mv(D1A, RA)],
        vec![unlink(D1A), mv(D2A, D1B)],
        vec![],
    ])
    .with_initial(present(&[D1A, D2A]))
    .with_root_backups(1 << 2)
    .with_grant(placed(1, D1 | D2, 1 << 2))
    .with_faults(1, 1)
}

/// C4-T3: growth and `StreamReleased`. Node 1 holds `/D1`; the root grows
/// its set by `/D2` (a `DelegateNode` record, honoured once committed and
/// learned); node 1 may release every subtree at once. The root creates
/// `/D2/b` (its own, or forwarded once `/D2` is node 1's); node 1 creates
/// `/D2/a` and `/D1/a`.
fn c4_t3() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D2B)], vec![create(D2A), create(D1A)]])
        .with_grant(placed(1, D1, 0))
        .with_grow(1, D2)
        .with_stream_release(1)
}

/// C4-T4, the §9 question (one cut per stream): node 1 writes its kept
/// subtree `/D2` on both sides of `/D1`'s release (`create /D2/b`, `unlink
/// /D1/a`, `unlink /D2/b`), so the stream interleaves released and kept
/// rows around `at`; the root renames `/D1/a` to `/a`. One crash.
fn c4_t4_interleaved() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![mv(D1A, RA)],
        vec![create(D2B), unlink(D1A), unlink(D2B)],
    ])
    .with_initial(present(&[D1A]))
    .with_grant(placed(1, D1 | D2, 0))
    .with_faults(1, 0)
}

/// C4-T4 with node 2 backing the root and the stream, one crash and one
/// deposition, and the history checked.
fn c4_t4b_interleaved_backed() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![
        vec![mv(D1A, RA)],
        vec![create(D2B), unlink(D1A), unlink(D2B)],
        vec![],
    ])
    .with_initial(present(&[D1A]))
    .with_root_backups(1 << 2)
    .with_grant(placed(1, D1 | D2, 1 << 2))
    .with_faults(1, 1);
    m.history = true;
    m
}

#[test]
fn c4_design_t4_kept_subtree_interleaves_one_cut() {
    assert_clean_within("c4-t4", &c4_t4_interleaved(), CAP_FAULTS);
}

#[test]
#[ignore = "large: run with --include-ignored (release)"]
fn c4_design_t4b_one_cut_backed_crash_and_takeover() {
    assert_clean_within("c4-t4b", &c4_t4b_interleaved_backed(), CAP_LARGE);
}

#[test]
fn c4_design_t1_release_by_request_one_cut() {
    assert_clean_within("c4-t1", &c4_t1(), CAP_FAULTS);
}

#[test]
fn c4_design_t2_release_with_backup_crash_and_takeover() {
    assert_clean_within("c4-t2", &c4_t2(), CAP_FAULTS);
}

#[test]
fn c4_design_t3_growth_and_stream_release() {
    assert_clean_within("c4-t3", &c4_t3(), CAP_FAULTS);
}

/// The root takes `/D1` as released once the release row is in node 1's
/// stream (its reply), before appending through `at`: its rename runs on a
/// journal that lacks node 1's unlink. The structural check finds it, the
/// re-validation of the journal too.
#[test]
fn c4_knob_root_executes_before_at_is_found() {
    let mut m = c4_t1();
    m.root_executes_before_at = true;
    assert_finds_within(
        "c4-t1+root_executes_before_at",
        &m,
        "released_subtree_sequenced_after_cut",
        CAP_FAULTS,
    );
    assert_finds_within(
        "c4-t1+root_executes_before_at+journal",
        &m,
        "journal_valid",
        CAP_FAULTS,
    );
}

/// A delegate that ignores its own release executes a stale op in the
/// released subtree.
#[test]
fn c4_knob_execute_after_release_is_found() {
    let mut m = c4_t1();
    m.execute_after_release = true;
    assert_finds_within(
        "c4-t1+execute_after_release",
        &m,
        "recall_safety",
        CAP_FAULTS,
    );
}

/// Routing an op between two subtrees of one node as cross-subtree makes
/// the root request a release for it.
#[test]
fn c4_knob_per_subtree_streams_is_found() {
    let mut m = c4_t1();
    m.per_subtree_streams = true;
    assert_finds_within(
        "c4-t1+per_subtree_streams",
        &m,
        "two_subtrees_one_order",
        CAP_FAULTS,
    );
}

/// A delegate that honours a `Grow` record before it is committed.
#[test]
fn c4_knob_honour_uncommitted_grant_is_found() {
    let mut m = c4_t3();
    m.honour_uncommitted_grant = true;
    assert_finds_within(
        "c4-t3+honour_uncommitted_grant",
        &m,
        "recall_safety",
        CAP_FAULTS,
    );
}

// ======================= chunk 5: per-key linearizability and the causal cut

/// C5-T1: node 1 holds `/D1` and `/D2` with node 2 as its backup (answers
/// before the root appends); the root has no backup. Node 2 creates
/// `/D1/a` (the data, answered by node 1) then `/a` (the marker, at the
/// root, with `deps` on node 1's stream); the root renames `/D2/a` to
/// `/b` (cross-subtree: `/D2`'s release). Every replica is a reader of
/// `(marker /a, data /D1/a)`; the history is checked per key; replicas
/// converge at quiescence.
fn c5_t1() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![
        vec![mv(D2A, RB)],
        vec![],
        vec![create(D1A), create(RA)],
    ])
    .with_initial(present(&[D2A]))
    .with_grant(placed(1, D1 | D2, 1 << 2));
    m.history = true;
    m.causal = true;
    m.replicas = true;
    m.probe = Some((RA, D1A));
    m
}

#[test]
fn c5_design_t1_linearizable_causal_converged() {
    assert_clean_within("c5-t1", &c5_t1(), CAP_LARGE);
}

/// The deep variant: C5-T1 with node 2 also backing the root, one crash
/// and one deposition.
#[test]
#[ignore = "deep: run with --include-ignored (release)"]
fn c5_design_t1_deep_crash_and_takeover() {
    let m = c5_t1().with_root_backups(1 << 2).with_faults(1, 1);
    assert_clean_within("c5-t1-deep", &m, CAP_DEEP);
}

/// The root executes the marker without waiting for its `deps` (node 1's
/// row, answered by its backup, not yet appended): the root's journal, and
/// so the archive, carries the marker before the data.
#[test]
fn c5_knob_deps_ignored_is_found() {
    let mut m = c5_t1();
    m.deps_ignored = true;
    assert_finds_within("c5-t1+deps_ignored", &m, "causal_cut", CAP_LARGE);
    assert_finds_within("c5-t1+deps_ignored+reader", &m, "marker_order", CAP_LARGE);
}

/// The root sequencing a released subtree before appending through `at`
/// breaks the per-key history (C4-T1 with the history on).
#[test]
fn c5_knob_root_executes_before_at_breaks_linearizability() {
    let mut m = c4_t1();
    m.history = true;
    m.root_executes_before_at = true;
    assert_finds_within(
        "c4-t1+history+root_executes_before_at",
        &m,
        "per_key_linearizable",
        CAP_FAULTS,
    );
}

// ======== chunk 6: the end of a placed grant, C3's verbatim re-ship, L1, L2b

/// C6-T1, the TTL end: node 1 holds `/D1` with node 2 as its backup and may
/// stop honouring; the root then seals the stream from node 2's copy in one
/// step and journals the end at its cursor. Node 2 creates `/D1/b`
/// (forwarded; retried at the root after the end, which dedups the rid it
/// appended); node 1 creates `/D1/a`; the root unlinks `/D1/a` (forwarded,
/// or its own once the subtree is back).
fn c6_t1_ttl() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![unlink(D1A)],
        vec![create(D1A)],
        vec![create(D1B)],
    ])
    .with_grant(placed(1, D1, 1 << 2))
    .with_ttl_end(1)
}

/// C6-T2, L1: node 1 holds `/D1` (group `{delegate, root}`) and keeps a
/// replica. Node 1 creates `/D1/a`; the root renames it to `/a` (its
/// request, node 1's release, the root's row on the key node 1's own row
/// wrote, all streamed to node 1).
fn c6_t2_l1() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![mv(D1A, RA)], vec![create(D1A)]])
        .with_grant(placed(1, D1, 0));
    m.replicas = true;
    m
}

/// C6-T3, L1 through the end: as C6-T2, the subtree returning by the TTL
/// end; the root unlinks `/D1/a`.
fn c6_t3_l1_ttl() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![unlink(D1A)], vec![create(D1A)]])
        .with_grant(placed(1, D1, 0))
        .with_ttl_end(1);
    m.replicas = true;
    m
}

/// C6-T4, C3's re-ship and L2b: node 1 backs the root and holds `/D1` with
/// node 2 as its backup; node 1 may claim the lease (re-appending its own
/// rows verbatim, then ending its generation), and the next root (node 2,
/// after node 1 is deposed before it ships) finds the stream unended and
/// seals it from its member copy. Two depositions; node 1 creates `/D1/a`,
/// node 2 creates `/D1/b`; a requester whose answered row turned void
/// replays it by rid (the rescue, unreachable in the design).
fn c6_t4_claim() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![], vec![create(D1A)], vec![create(D1B)]])
        .with_root_backups(1 << 1)
        .with_grant(placed(1, D1, 1 << 2))
        .with_faults(0, 2);
    m.delegate_may_claim = true;
    m.shadow_replay = true;
    m
}

#[test]
fn c6_design_t1_ttl_end_seals_from_member() {
    assert_clean_within("c6-t1", &c6_t1_ttl(), CAP_FAULTS);
}

#[test]
fn c6_design_t2_l1_release() {
    assert_clean_within("c6-t2", &c6_t2_l1(), CAP_FAULTS);
}

#[test]
fn c6_design_t3_l1_ttl_end() {
    assert_clean_within("c6-t3", &c6_t3_l1_ttl(), CAP_FAULTS);
}

#[test]
fn c6_design_t4_delegate_claims_reships_and_l2b() {
    assert_clean_within("c6-t4", &c6_t4_claim(), CAP_FAULTS);
}

/// C6-T1 with one crash (the delegate's backup, the root's client or the
/// delegate itself). With the backup dead the end waits for the group
/// (plan 43 C2e): ending at the root's cursor would cut below the row the
/// backup committed and node 1 answered (found before the wait was
/// modeled).
#[test]
#[ignore = "large: run with --include-ignored (release)"]
fn c6_design_t1_ttl_end_with_a_crash() {
    assert_clean_within("c6-t1-crash", &c6_t1_ttl().with_faults(1, 0), CAP_LARGE);
}

/// C6-T4 with one crash on top of the two depositions.
#[test]
fn c6_design_t4_claim_with_a_crash() {
    assert_clean_within("c6-t4-crash", &c6_t4_claim().with_faults(1, 2), CAP_LARGE);
}

/// The TTL end cut at the root's cursor without the backup's seal drops a
/// row node 2's copy committed and node 1 answered.
#[test]
fn c6_knob_end_before_seal_is_found() {
    let mut m = c6_t1_ttl();
    m.end_before_seal = true;
    assert_finds_within(
        "c6-t1+end_before_seal",
        &m,
        "acked_at_or_below_cut",
        CAP_FAULTS,
    );
}

/// L1: the streamed install of the release (or of the end) clears the
/// mark; node 1's own create is redone on top of the root's later row and
/// the durable apply skips it: the replica keeps a name the log removed.
#[test]
fn c6_knob_l1_mark_cleared_by_stream_is_found() {
    let mut m = c6_t2_l1();
    m.l1_mark_cleared_by_stream = true;
    assert_finds_within(
        "c6-t2+l1_mark_cleared_by_stream",
        &m,
        "converged_at_quiescence",
        CAP_FAULTS,
    );
    let mut m = c6_t3_l1_ttl();
    m.l1_mark_cleared_by_stream = true;
    assert_finds_within(
        "c6-t3+l1_mark_cleared_by_stream",
        &m,
        "converged_at_quiescence",
        CAP_FAULTS,
    );
}

/// L2b: the members retire their copies when the delegate-turned-root
/// journals its generation's end; that root is deposed before shipping;
/// the next root ends the stream below the answered row, and the
/// requester's rescue lands it with a new identity.
#[test]
fn c6_knob_drop_member_copy_is_found() {
    let mut m = c6_t4_claim();
    m.drop_member_copy = true;
    assert_finds_within("c6-t4+drop_member_copy", &m, "verbatim_identity", CAP_LARGE);
}

/// A delegate taking the root without re-appending its own rows ends its
/// generation below a row its group committed and answered.
#[test]
fn c6_knob_no_reappend_is_found() {
    let mut m = c6_t4_claim();
    m.no_reappend = true;
    assert_finds_within("c6-t4+no_reappend", &m, "acked_at_or_below_cut", CAP_LARGE);
}

// ======== chunk 7: sticky grants, abandon, ADR-43's no-tail proof

fn sticky(node: u8, dirs: u8) -> NodeGrant {
    NodeGrant {
        node,
        dirs,
        kind: GrantKind::Sticky,
        backups: 0,
    }
}

/// C7-T1, the sticky lifecycle: node 1 holds `/D1` sticky (final, no
/// group) and node 2 holds `/D2` placed (group `{delegate, root}`). The
/// root forwards `/D1/b` to the holder; the holder creates `/D1/a`; node 2
/// creates `/D2/a`. The holder may release (only once drained: the root
/// has appended through its last row), and the operator may abandon the
/// sticky stream (at most once, at any time: a holder lost to the root may
/// still be answering).
fn c7_t1() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(D1B)],
        vec![create(D1A)],
        vec![create(D2A)],
    ])
    .with_grant(sticky(1, D1))
    .with_grant(placed(2, D2, 0))
    .with_stream_release(1)
    .with_abandon(1)
}

/// C7-T1 with one crash and no placed grant: the holder lost mid-write
/// blocks its subtree until the operator abandons it (or another node
/// crashes).
fn c7_t1_crash() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D1B)], vec![create(D1A)], vec![]])
        .with_grant(sticky(1, D1))
        .with_stream_release(1)
        .with_abandon(1)
        .with_faults(1, 0)
}

/// C7-T2, ADR-43's orphan rule against a sticky grant: the root (no
/// backup) mints node 1's sticky grant on `/D1` at run time (its
/// `DelegateNode` record); node 2 writes `/a` and may claim the lease from
/// the archive alone (a no-tail marker), which ends every number of the
/// deposed tenure's epoch its journal does not list.
fn c7_t2() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![], vec![create(D1A)], vec![create(RA)]])
        .with_grant(sticky(1, 0))
        .with_grow(1, D1)
        .with_faults(0, 1)
}

/// C7-T3, a grant minted in epoch e honoured by e + 1: the root (backed by
/// node 2) mints node 1's sticky grant (`/D1`) and grows it (`/D2`), each
/// possibly in the next tenure (a later tenure journals a record for an
/// older number); node 2 writes `/a` and claims with the tail. Node 1
/// writes `/D2/a`.
fn c7_t3() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![], vec![create(D2A)], vec![create(RA)]])
        .with_root_backups(1 << 2)
        .with_grant(sticky(1, 0))
        .with_grow(1, D1)
        .with_grow(1, D2)
        .with_faults(0, 1)
}

/// C7-T3 deep: four nodes, two depositions: node 2 claims with the tail
/// (its lone tenure inherits it), node 3 then claims from the archive (a
/// no-tail marker whose orphan rule covers the second tenure's numbers
/// only); node 1 writes `/D1/a`, node 3 writes `/a`.
fn c7_t3_deep() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![], vec![create(D1A)], vec![], vec![create(RA)]])
        .with_root_backups(1 << 2)
        .with_grant(sticky(1, 0))
        .with_grow(1, D1)
        .with_faults(0, 2)
}

/// C7-T4, a handover: node 1 holds `/D1` sticky, node 2 writes into it
/// (forwarded, answered final by the holder) until the holder releases
/// (the root's request for the dominant writer, abstracted to the
/// release's guard) and the root grants `/D1` to node 2 (a `DelegateNode`
/// record minting node 2's sticky grant).
fn c7_t4() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![], vec![create(D1A)], vec![create(D1B)]])
        .with_grant(sticky(1, D1))
        .with_grant(sticky(2, 0))
        .with_stream_release(1)
        .with_grow(2, D1)
}

/// C7-T5, a designation (`designation_final` re-proved): node 1 holds
/// `/D1` designated; the root forwards into it; the holder may release
/// (`online`); one crash.
fn c7_t5() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D1B)], vec![create(D1A)]])
        .with_grant(NodeGrant {
            node: 1,
            dirs: D1,
            kind: GrantKind::Designated,
            backups: 0,
        })
        .with_stream_release(1)
        .with_faults(1, 0)
}

#[test]
fn c7_design_t1_sticky_release_abandon() {
    assert_clean_within("c7-t1", &c7_t1(), CAP_LARGE);
}

#[test]
fn c7_design_t1_crash_abandon() {
    assert_clean_within("c7-t1-crash", &c7_t1_crash(), CAP_LARGE);
}

#[test]
fn c7_design_t2_orphan_rule_no_tail_claim() {
    assert_clean_within("c7-t2", &c7_t2(), CAP_LARGE);
}

#[test]
fn c7_design_t3_minted_in_e_honoured_by_e_plus_1() {
    assert_clean_within("c7-t3", &c7_t3(), CAP_LARGE);
}

#[test]
#[ignore = "large: run with --include-ignored (release)"]
fn c7_design_t3_deep_two_claims() {
    assert_clean_within("c7-t3-deep", &c7_t3_deep(), CAP_DEEP);
}

#[test]
fn c7_design_t4_handover() {
    assert_clean_within("c7-t4", &c7_t4(), CAP_LARGE);
}

#[test]
fn c7_design_t5_designation_final() {
    assert_clean_within("c7-t5", &c7_t5(), CAP_LARGE);
}

/// The TTL reclaims the sticky grant: the end has neither the holder's
/// release nor the operator's abandon, and (with that oracle's event
/// passed) the cut drops a final answer, uncounted (I5).
#[test]
fn c7_knob_ttl_reclaims_sticky_is_found() {
    let mut m = c7_t1();
    m.ttl_reclaims_sticky = true;
    assert_finds_within(
        "c7-t1+ttl_reclaims_sticky",
        &m,
        "sticky_ends_only_by_holder_or_operator",
        CAP_LARGE,
    );
    assert_finds_within(
        "c7-t1+ttl_reclaims_sticky+i5",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_LARGE,
    );
    let mut m = c7_t5();
    m.ttl_reclaims_sticky = true;
    assert_finds_within(
        "c7-t5+ttl_reclaims_sticky",
        &m,
        "designation_final",
        CAP_LARGE,
    );
}

/// The holder releases before the root has appended through its last row
/// (the release names the cut it knows): an answered final row lies past
/// the cut, a rollback no operator or singleton event counts.
#[test]
fn c7_knob_holder_releases_before_drain_is_found() {
    let mut m = c7_t1();
    m.holder_releases_before_drain = true;
    assert_finds_within(
        "c7-t1+holder_releases_before_drain",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_LARGE,
    );
    assert_finds_within(
        "c7-t1+holder_releases_before_drain+cut",
        &m,
        "acked_at_or_below_cut",
        CAP_LARGE,
    );
}

/// The sticky holder answers once the grant's record is in the root's
/// journal, before it is committed (the archive, for a root with no
/// backup): the root is deposed before shipping, the claimant's no-tail
/// marker proves the number never granted, and the orphan rule ends a
/// grant that answered (and I5 counts the rollback).
#[test]
fn c7_knob_sticky_answers_before_grant_committed_is_found() {
    let mut m = c7_t2();
    m.sticky_answers_before_grant_committed = true;
    // `recall_safety` finds the early honour by definition; off, the harm.
    m.check_recall = false;
    assert_finds_within(
        "c7-t2+sticky_answers_before_grant_committed",
        &m,
        "orphan_rule_never_ends_an_answering_grant",
        CAP_LARGE,
    );
    assert_finds_within(
        "c7-t2+sticky_answers_before_grant_committed+i5",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_LARGE,
    );
}

// ======== chunk 8: the root's own sticky grants, the singleton, first_peer_join

/// C8-T1, `first_peer_join`: node 0 is a singleton (node 1 not in the
/// roster) and answers `/a`, `/b` from its disk; node 1 joins, the root
/// sees the roster (and journals the lease's `config_version` bump), node
/// 1 unlinks `/a` (forwarded); node 1 may claim the lease from the archive
/// (the singleton's death-then-join: its unshipped answered rows are
/// exported, Q6).
fn c8_t1() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(RA), create(RB)], vec![unlink(RA)]])
        .with_roster(1, 1 << 1, 0)
        .with_faults(0, 1)
}

/// C8-T2, the root's own sticky grant: two peers, the root with no group
/// takes a sticky grant on `/D1` (never `/`), committed by the archive; its
/// ops in `/D1` are final at local speed, `/a` waits for the archive; node
/// 1 writes `/b` then `/D1/b` (forwarded to the root's grant); node 1 may
/// claim the lease (the root's sticky subtree then blocks).
fn c8_t2() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(D1A), create(RA)],
        vec![create(RB), create(D1B)],
    ])
    .with_grant(sticky(0, 0))
    .with_grow(1, D1)
    .with_faults(0, 1)
}

/// C8-T3, peer → singleton: node 1 is in the roster and may leave (and
/// join again); the root journals the bump to the singleton when it sees
/// the departure and answers alone once the bump is committed (the
/// archive); node 1 unlinks `/a` and may claim after its return.
fn c8_t3() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(RA), create(RB)], vec![unlink(RA)]])
        .with_roster(0b11, 1 << 1, 1 << 1)
        .with_faults(0, 1)
}

#[test]
fn c8_design_t1_first_peer_join() {
    assert_clean_within("c8-t1", &c8_t1(), CAP_FAULTS);
}

#[test]
fn c8_design_t2_root_own_sticky_grant() {
    assert_clean_within("c8-t2", &c8_t2(), CAP_FAULTS);
}

#[test]
fn c8_design_t3_peer_leaves_singleton_bump() {
    assert_clean_within("c8-t3", &c8_t3(), CAP_FAULTS);
}

/// The root keeps answering alone after its roster view shows the peer (no
/// bump): found by `no_lone_answer_with_peer`; with that oracle off, the
/// harm: node 1 claims from the archive and a row answered alone after the
/// root saw it is lost, which no singleton event covers (I5).
#[test]
fn c8_knob_singleton_after_join_is_found() {
    let mut m = c8_t1();
    m.singleton_after_join = true;
    assert_finds_within(
        "c8-t1+singleton_after_join",
        &m,
        "no_lone_answer_with_peer",
        CAP_FAULTS,
    );
    m.check_lone = false;
    assert_finds_within(
        "c8-t1+singleton_after_join+i5",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_FAULTS,
    );
}

/// The root answers a joined peer's forward alone (its singleton test
/// waits for the roster read): the peer claims from the archive and its
/// answered op is lost, which no singleton event covers (I5: a singleton's
/// rollback is of its own clients' rows).
#[test]
fn c8_knob_singleton_serves_forwards_is_found() {
    let mut m = c8_t1();
    m.singleton_serves_forwards = true;
    assert_finds_within(
        "c8-t1+singleton_serves_forwards",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_FAULTS,
    );
}

/// The singleton test at answer time (no journaled bump in the gate): rows
/// journaled while the peer was in the roster are answered alone once the
/// root sees it leave: I1 by configuration.
#[test]
fn c8_knob_bump_by_event_is_found() {
    let mut m = c8_t3();
    m.bump_by_event = true;
    assert_finds_within(
        "c8-t3+bump_by_event",
        &m,
        "ack_implies_committed_by_config",
        CAP_FAULTS,
    );
}

/// A sticky grant on `/` for the root: its `/` rows are final in its own
/// stream, and its successor (which sequences `/` as the lease holder)
/// orders `/` while the grant holds it.
#[test]
fn c8_knob_root_sticky_on_slash_is_found() {
    let mut m = c8_t2();
    m.root_sticky_on_slash = true;
    assert_finds_within(
        "c8-t2+root_sticky_on_slash",
        &m,
        "released_subtree_sequenced_after_cut",
        CAP_FAULTS,
    );
}

// ======== chunk 9: settlement (I4), S4, no server queue (I2(b))

/// C9-T1: node 1 holds `/D1` sticky and grants a read delegation on it;
/// node 2 moves `/D1/b` to `/b` (cross-subtree: the root parks it and
/// requests the release, which waits for the read recall) and may pass
/// its deadline (the settle parks at the root with the op and is answered
/// after the release: the op's outcome, or `Refused` superseding it).
fn c9_t1() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![], vec![create(D1A)], vec![mv(D1B, RB)]])
        .with_initial(present(&[D1B]))
        .with_grant(sticky(1, D1))
        .with_reads(1)
        .with_deadlines(1)
}

/// C9-T2: node 1 holds `/D1` placed (group `{delegate, root}`); node 2
/// creates `/D1/b` (forwarded) and may pass its deadline: the settle goes
/// to the sequencer in force, whose `Refused` is answered once committed.
fn c9_t2() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![], vec![], vec![create(D1B)]])
        .with_grant(placed(1, D1, 0))
        .with_deadlines(1)
}

/// C9-T3, the takeover gate's settle by request-release: node 1 holds
/// `/D1` sticky and moves `/D1/a` to `/b` (cross-subtree, parked at the
/// root); the root forwards `/D1/b` into the grant; node 2 may claim the
/// lease from the archive; node 1's retry and settle meet the new root,
/// which requests the release before it answers.
fn c9_t3() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D1B)], vec![mv(D1A, RB)], vec![]])
        .with_initial(present(&[D1A]))
        .with_grant(sticky(1, D1))
        .with_faults(0, 1)
        .with_deadlines(1)
}

#[test]
fn c9_design_t1_settle_by_release_after_read_recall() {
    assert_clean_within("c9-t1", &c9_t1(), CAP_LARGE);
}

#[test]
fn c9_design_t2_settle_at_the_delegate() {
    assert_clean_within("c9-t2", &c9_t2(), CAP_LARGE);
}

#[test]
fn c9_design_t3_settle_at_a_new_root() {
    assert_clean_within("c9-t3", &c9_t3(), CAP_LARGE);
}

/// S4: the release (the root's cut) before the read recall.
#[test]
fn c9_knob_cut_before_read_recall_is_found() {
    let mut m = c9_t1();
    m.cut_before_read_recall = true;
    assert_finds_within(
        "c9-t1+cut_before_read_recall",
        &m,
        "settle_after_reads_recalled",
        CAP_LARGE,
    );
}

/// The settle goes to the lease holder, which refuses a key a node stream
/// holds without the release: the forwarded op lands after the `EIO`.
#[test]
fn c9_knob_settle_unowned_is_found() {
    let mut m = c9_t2();
    m.settle_unowned = true;
    assert_finds_within(
        "c9-t2+settle_unowned",
        &m,
        "eio_never_lands_late",
        CAP_LARGE,
    );
}

/// A server-side queue: the deposed root's parked op is handed to the
/// claimant while the requester retries: two executable copies.
#[test]
fn c9_knob_server_replay_queue_is_found() {
    let mut m = c9_t3();
    m.server_replay_queue = true;
    assert_finds_within(
        "c9-t3+server_replay_queue",
        &m,
        "one_executable_copy",
        CAP_LARGE,
    );
}
