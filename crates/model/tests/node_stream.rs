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
//! queue (I2(b)); chunk 12: the root's roster object and stale views
//! (ADR-50: old and incomplete reads, a joiner's first message by the inbox
//! or to a node that is not the root, a live foreign generation). The design's
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
use constellation_model::node_stream::{GrantKind, NodeGrant, NodeStreamModel};
use constellation_model::node_stream::{CACHE_INBOX, ROOT};
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
    let vacuous: Vec<&str> = model
        .properties()
        .into_iter()
        .filter(|p| p.expectation == Expectation::Sometimes)
        .filter(|p| checker.discovery(p.name).is_none())
        .map(|p| p.name)
        .collect();
    assert!(
        vacuous.is_empty(),
        "{label}: `sometimes` witnesses never reached (vacuous): {vacuous:?}"
    );
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
    assert_trace(label, prop, d.into_actions().len());
}

/// The counterexample of each knob, checked in as its path length (plan 46
/// M1.0 chunk 11): `(label, property, actions)`. The checker is breadth
/// first on one thread and its hashing is fixed, so the length is
/// reproducible; a change of the model that moves it is a change of the
/// counterexample and is looked at (the path is printed with `--nocapture`).
/// Stateright's path is valid but not always the shortest one. A knob whose
/// label is missing here fails, so no knob goes unrecorded.
const TRACES: &[(&str, &str, usize)] = &[
    ("c12-t1+roster_installs_old_read", "roster_view_monotone", 7),
    (
        "c12-t1+roster_installs_old_read+i5",
        "rollback_only_singleton_or_operator",
        7,
    ),
    ("c12-t1+roster_partial_shrinks", "roster_view_monotone", 7),
    (
        "c12-t1+roster_partial_shrinks+i5",
        "rollback_only_singleton_or_operator",
        7,
    ),
    ("c12-t2+roster_partial_shrinks", "roster_view_monotone", 6),
    (
        "c12-t2+roster_partial_shrinks+i5",
        "rollback_only_singleton_or_operator",
        5,
    ),
    (
        "c12-t2+singleton_while_partial",
        "no_lone_answer_with_peer",
        7,
    ),
    (
        "c12-t1+evidence_only_on_forward",
        "rollback_only_singleton_or_operator",
        7,
    ),
    (
        "c12-t1+evidence_only_on_forward+lone",
        "no_lone_answer_with_peer",
        8,
    ),
    (
        "c12-t3+singleton_with_foreign_grant",
        "no_singleton_with_live_foreign_grant",
        6,
    ),
    (
        "c12-t3+singleton_with_foreign_grant+i1",
        "ack_implies_committed_by_config",
        9,
    ),
    ("f1-stale+unfenced_forward", "refused_never_completes", 8),
    (
        "f1-stale+root_ignores_touched",
        "refused_never_completes",
        10,
    ),
    (
        "c7-t1-crash+reads+abandon_skips_read_recall",
        "settle_after_reads_recalled",
        6,
    ),
    ("c11-x3b+leave_keeps_grants", "no_live_grant_off_roster", 7),
    (
        "c11-x4+claim_skips_read_recall",
        "settle_after_reads_recalled",
        5,
    ),
    (
        "r1-sticky-d1d2+partial_release_ends_stream",
        "sticky_ends_only_by_holder_or_operator",
        8,
    ),
    (
        "c11-x2r+abandon_skips_read_recall",
        "settle_after_reads_recalled",
        6,
    ),
    (
        "c11-x4+claim_ends_sticky_silently",
        "sticky_ends_only_by_holder_or_operator",
        6,
    ),
    ("c2-t1+lone_delegate", "acked_never_lost", 7),
    ("c2-t1+root_journal_counts", "acked_never_lost", 8),
    (
        "c2-t1+root_journal_counts+i1",
        "ack_implies_committed_by_config",
        8,
    ),
    ("c2-t1b+no_dedup", "exactly_once", 12),
    (
        "c3-deleg-add+ack_by_latest_config",
        "ack_implies_committed_by_config",
        7,
    ),
    (
        "c3-deleg-remove+ack_by_latest_config",
        "ack_implies_committed_by_config",
        6,
    ),
    (
        "c3-deleg-remove+per_row_class",
        "row_version_matches_config",
        6,
    ),
    (
        "c3-deleg-remove+per_row_class+i1",
        "ack_implies_committed_by_config",
        5,
    ),
    ("c3-deleg-remove+per_row_class+lost", "acked_never_lost", 5),
    ("c4-t1+execute_after_release", "recall_safety", 10),
    (
        "c4-t1+history+root_executes_before_at",
        "per_key_linearizable",
        11,
    ),
    ("c4-t1+per_subtree_streams", "two_subtrees_one_order", 10),
    (
        "c4-t1+root_executes_before_at",
        "released_subtree_sequenced_after_cut",
        14,
    ),
    ("c4-t1+root_executes_before_at+journal", "journal_valid", 13),
    ("c4-t3+honour_uncommitted_grant", "recall_safety", 7),
    ("c5-t1+deps_ignored", "causal_cut", 10),
    ("c5-t1+deps_ignored+reader", "marker_order", 10),
    ("c6-t1+end_before_seal", "acked_at_or_below_cut", 7),
    (
        "c6-t2+l1_mark_cleared_by_stream",
        "converged_at_quiescence",
        15,
    ),
    (
        "c6-t3+l1_mark_cleared_by_stream",
        "converged_at_quiescence",
        14,
    ),
    ("c6-t4+drop_member_copy", "verbatim_identity", 8),
    ("c6-t4+no_reappend", "acked_at_or_below_cut", 9),
    (
        "c7-t1+holder_releases_before_drain",
        "rollback_only_singleton_or_operator",
        6,
    ),
    (
        "c7-t1+holder_releases_before_drain+cut",
        "acked_at_or_below_cut",
        6,
    ),
    (
        "c7-t1+ttl_reclaims_sticky",
        "sticky_ends_only_by_holder_or_operator",
        6,
    ),
    (
        "c7-t1+ttl_reclaims_sticky+i5",
        "rollback_only_singleton_or_operator",
        6,
    ),
    (
        "c7-t2+sticky_answers_before_grant_committed",
        "orphan_rule_never_ends_an_answering_grant",
        7,
    ),
    (
        "c7-t2+sticky_answers_before_grant_committed+i5",
        "rollback_only_singleton_or_operator",
        7,
    ),
    ("c7-t5+ttl_reclaims_sticky", "designation_final", 7),
    ("c8-t1+singleton_after_join", "no_lone_answer_with_peer", 11),
    (
        "c8-t1+singleton_after_join+i5",
        "rollback_only_singleton_or_operator",
        12,
    ),
    (
        "c8-t1+singleton_serves_forwards",
        "rollback_only_singleton_or_operator",
        10,
    ),
    (
        "c8-t2+root_sticky_on_slash",
        "released_subtree_sequenced_after_cut",
        9,
    ),
    ("c8-t3+bump_by_event", "ack_implies_committed_by_config", 9),
    (
        "c9-t1+cut_before_read_recall",
        "settle_after_reads_recalled",
        9,
    ),
    ("c9-t2+settle_unowned", "eio_never_lands_late", 11),
    ("c9-t3+server_replay_queue", "one_executable_copy", 8),
    ("t1+lone_delegate", "ack_implies_committed_by_config", 17),
    (
        "t1+root_journal_counts",
        "ack_implies_committed_by_config",
        18,
    ),
    (
        "t1b+root_journal_counts",
        "ack_implies_committed_by_config",
        9,
    ),
    ("t2+lone_delegate", "ack_implies_committed_by_config", 8),
];

fn assert_trace(label: &str, prop: &str, len: usize) {
    println!("TRACE {label}|{prop}|{len}");
    // `RECORD_TRACES=1` only prints the `TRACE` lines (to regenerate the table).
    if std::env::var_os("RECORD_TRACES").is_some() {
        return;
    }
    let (_, _, want) = TRACES
        .iter()
        .find(|t| t.0 == label && t.1 == prop)
        .unwrap_or_else(|| panic!("{label}: `{prop}` path of {len} actions is not in TRACES"));
    assert_eq!(
        len, *want,
        "{label}: the counterexample for `{prop}` changed length"
    );
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

/// C7-T1 with one crash, the holder's read delegation and the abandon (M1.0
/// review SHOULD-1): a dead holder cannot recall, so the abandon waits for
/// the read grant's expiry, "recall, else expire".
#[test]
fn c7_design_t1_crash_reads_abandon() {
    assert_clean_within("c7-t1-crash+reads", &c7_t1_crash().with_reads(1), CAP_LARGE);
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

// ------------------------------------------------------- chunk 11: combined
//
// The chunks' topologies each switch one mechanism on. These switch
// several on at once, so that the mechanisms are checked against each
// other: every `always` property the model registers (the model registers
// each wherever the mechanism it needs is on) and every `sometimes`
// witness. A script is one or two ops per node; what a topology adds is
// composition, not length (a third op with a history, a crash and a
// deposition already exceeds 40M generated states; the `sometimes` witness
// of each topology is checked, so a script is chosen so that every one is
// reached).

/// X1, the placed stream with everything on it: node 1 holds `/D1` and
/// `/D2` placed (group `{delegate, node 2}`), node 2 also backs the root.
/// Node 2 writes `/D1/b` (the delegate's) and then `/b` (the root's, with
/// the delegate's row in its `deps`: the root waits for it); the TTL may
/// end the grant; per-key history and the causal cut; one crash and one
/// deposition.
fn c11_x1_placed_everything() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![], vec![], vec![create(D1B), create(RB)]])
        .with_initial(present(&[D1A]))
        .with_root_backups(1 << 2)
        .with_grant(placed(1, D1 | D2, 1 << 2))
        .with_ttl_end(1)
        .with_faults(1, 1);
    m.history = true;
    m.causal = true;
    m
}

/// X1r, the replicas: C5's topology (the delegate's replica, the archive's
/// durable apply, a marker/data reader on every replica, `deps`) with the
/// TTL end and one crash.
fn c11_x1r_replicas_and_ttl() -> NodeStreamModel {
    let mut m = c5_t1().with_ttl_end(1).with_faults(1, 0);
    m.history = true;
    m
}

/// X2, the kinds side by side: node 1 holds `/D1` sticky (final), node 2
/// holds `/D2` placed (group `{delegate, node 1}`); the holder may release
/// and the operator abandon. The root writes `/D2/a` (forwarded to the
/// delegate), node 1 writes `/D1/a`. (The read delegations, the deadline
/// and the move out of the sticky stream are X2r: with them a third op
/// exceeds the cap.)
fn c11_x2_sticky_and_placed() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D2A)], vec![create(D1A)], vec![]])
        .with_grant(sticky(1, D1))
        .with_grant(placed(2, D2, 1 << 1))
        .with_stream_release(1)
        .with_abandon(1)
}

/// X2r, the read delegations and the deadline on the sticky stream: the
/// root writes `/D1/a` (forwarded to the holder), node 2's move out of the
/// sticky stream is parked at the root and settled by a deadline; the
/// holder may release and the operator abandon; the stream's read
/// delegation is granted and recalled (the abandon waits for the recall,
/// maintainer decision 2026-11-06).
fn c11_x2r_reads_and_deadlines() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D1A)], vec![], vec![mv(D1B, RB)]])
        .with_initial(present(&[D1B]))
        .with_grant(sticky(1, D1))
        .with_stream_release(1)
        .with_abandon(1)
        .with_reads(1)
        .with_deadlines(1)
}

/// X3, the roster with the root's own grant: the root alone in the roster at
/// first, with a sticky grant of its own minted at run time on `/D1` (never
/// `/`); node 1 joins (the bump) and may leave (the bump back), and one
/// deposition. (The placed delegate that leaves is X3b.)
fn c11_x3_roster_and_grants() -> NodeStreamModel {
    NodeStreamModel::design(vec![
        vec![create(D1A), create(RA)],
        vec![create(RB), create(D1B)],
    ])
    .with_grant(sticky(0, 0))
    .with_grow(1, D1)
    .with_roster(0b01, 1 << 1, 1 << 1)
    .with_faults(0, 1)
}

/// X3b, "Leave ends grants first" (M1.0 review SHOULD-2): node 2 is in the
/// roster and holds `/D2` placed (no recorded backup: its group is
/// `{delegate, root}`); node 1 joins. Node 2 may leave, and only after its
/// grant is ended by its own release (`StreamReleased`, then the root's
/// `DelegateNodeEnded`); the root then sees a roster without it and is a
/// singleton again (the bump). The root writes `/D2/b` (forwarded to the
/// holder, or its own once released), node 2 writes `/D2/a`.
fn c11_x3b_leave_ends_grants() -> NodeStreamModel {
    NodeStreamModel::design(vec![vec![create(D2B)], vec![], vec![create(D2A)]])
        .with_grant(placed(2, D2, 0))
        .with_stream_release(1)
        .with_roster(0b101, 1 << 1, 1 << 2)
}

/// X4, the claim with a sticky grant: node 1 backs the root and holds
/// `/D1` placed with node 2 as its backup, may claim the lease
/// (re-appending verbatim) and end its generation; node 2 holds a sticky
/// grant minted at run time on `/D2`, writes into `/D1`, and may claim
/// next. Both streams grant a read delegation, which a claim's release
/// waits for (M1.0 review SHOULD-1). Two depositions.
fn c11_x4_claim_and_sticky() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![], vec![create(D1A)], vec![create(D2A)]])
        .with_root_backups(1 << 1)
        .with_grant(placed(1, D1, 1 << 2))
        .with_grant(sticky(2, 0))
        .with_grow(2, D2)
        .with_reads(1)
        .with_reads(2)
        .with_faults(0, 2);
    m.delegate_may_claim = true;
    m.shadow_replay = true;
    m
}

#[test]
fn c11_design_x1_placed_everything() {
    assert_clean_within("c11-x1", &c11_x1_placed_everything(), CAP_LARGE);
}

#[test]
fn c11_design_x1r_replicas_and_ttl() {
    assert_clean_within("c11-x1r", &c11_x1r_replicas_and_ttl(), CAP_LARGE);
}

#[test]
fn c11_design_x2_sticky_and_placed() {
    assert_clean_within("c11-x2", &c11_x2_sticky_and_placed(), CAP_LARGE);
}

#[test]
fn c11_design_x2r_reads_and_deadlines() {
    assert_clean_within("c11-x2r", &c11_x2r_reads_and_deadlines(), CAP_LARGE);
}

#[test]
fn c11_design_x3_roster_and_grants() {
    assert_clean_within("c11-x3", &c11_x3_roster_and_grants(), CAP_LARGE);
}

#[test]
fn c11_design_x3b_leave_ends_grants() {
    assert_clean_within("c11-x3b", &c11_x3b_leave_ends_grants(), CAP_LARGE);
}

#[test]
fn c11_design_x4_claim_and_sticky() {
    assert_clean_within("c11-x4", &c11_x4_claim_and_sticky(), CAP_LARGE);
}

/// The claim's end of a sticky holder's generation with no release of its
/// own (the model before "claim = release"): the second claimant is node
/// 2, the sticky holder, and its stream ends by nobody's hand.
#[test]
fn c11_knob_claim_ends_sticky_silently_is_found() {
    let mut m = c11_x4_claim_and_sticky();
    m.claim_ends_sticky_silently = true;
    assert_finds_within(
        "c11-x4+claim_ends_sticky_silently",
        &m,
        "sticky_ends_only_by_holder_or_operator",
        CAP_LARGE,
    );
}

/// The operator's abandon before the read recall: the root settles in the
/// abandoned directory while a reader holds its read grant.
#[test]
fn c11_knob_abandon_skips_read_recall_is_found() {
    let mut m = c11_x2r_reads_and_deadlines();
    m.abandon_skips_read_recall = true;
    assert_finds_within(
        "c11-x2r+abandon_skips_read_recall",
        &m,
        "settle_after_reads_recalled",
        CAP_LARGE,
    );
}

// ----------------------------------------------- the M1.0 review fixes

/// MUST-1: node 1 holds `/D1` and `/D2` sticky in one stream; the root's
/// move out of `/D1` requests the release of `/D1` only (a
/// `SubtreeReleased`); node 1 writes `/D2/a` and may claim the lease (the
/// claim's release covers what is left). A partial release is not the
/// holder's release of the stream.
fn r1_sticky_two_dirs_partial_release() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![mv(D1B, RB)], vec![create(D2A)]])
        .with_initial(present(&[D1B]))
        .with_grant(sticky(1, D1 | D2))
        .with_stream_release(1)
        .with_faults(0, 1);
    m.delegate_may_claim = true;
    m
}

#[test]
fn r1_design_partial_release_does_not_end_the_stream() {
    assert_clean_within(
        "r1-sticky-d1d2",
        &r1_sticky_two_dirs_partial_release(),
        CAP_LARGE,
    );
}

/// The old rule: any `Released` row is the holder's release of the stream
/// (the root's end after it, and the claim's skipped release).
#[test]
fn r1_knob_partial_release_ends_stream_is_found() {
    let mut m = r1_sticky_two_dirs_partial_release();
    m.partial_release_ends_stream = true;
    assert_finds_within(
        "r1-sticky-d1d2+partial_release_ends_stream",
        &m,
        "sticky_ends_only_by_holder_or_operator",
        CAP_LARGE,
    );
}

/// SHOULD-1: the operator's abandon of a dead holder's stream waits for the
/// holder's read delegation to expire; the knob abandons at once.
#[test]
fn r1_knob_abandon_skips_read_expiry_is_found() {
    let mut m = c7_t1_crash().with_reads(1);
    m.abandon_skips_read_recall = true;
    assert_finds_within(
        "c7-t1-crash+reads+abandon_skips_read_recall",
        &m,
        "settle_after_reads_recalled",
        CAP_LARGE,
    );
}

/// SHOULD-1: a claim's release does not wait for the claimant's read
/// delegation: the new root acts in the directory while it is granted.
#[test]
fn r1_knob_claim_skips_read_recall_is_found() {
    let mut m = c11_x4_claim_and_sticky();
    m.claim_skips_read_recall = true;
    assert_finds_within(
        "c11-x4+claim_skips_read_recall",
        &m,
        "settle_after_reads_recalled",
        CAP_LARGE,
    );
}

/// SHOULD-2: a node leaves the roster with its grant live; the root, a
/// singleton by the roster alone, answers a delegate row from its disk.
#[test]
fn r1_knob_leave_keeps_grants_is_found() {
    let mut m = c11_x3b_leave_ends_grants();
    m.leave_keeps_grants = true;
    assert_finds_within(
        "c11-x3b+leave_keeps_grants",
        &m,
        "no_live_grant_off_roster",
        CAP_LARGE,
    );
}

// ================ route fencing (ADR-47): an executor runs a forward only
// under the generation it was routed by

/// F1: node 1 holds `/D1` sticky; node 2's route cache is stale (its first
/// send goes to the holder routed `0`, its later ones to the lease holder).
/// One crash: the holder, with its disk kept, stands for one the root does
/// not reach.
fn f1_stale() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![], vec![], vec![create(D1A)]])
        .with_grant(sticky(1, D1))
        .with_faults(1, 0);
    m.route_fence = true;
    m.stale_routes = true;
    m
}

/// F1b: the requester's table is current (it routes by the grant), so the
/// forward arrives routed and touched; a second writer in the subtree.
fn f1_current() -> NodeStreamModel {
    let mut m = NodeStreamModel::design(vec![vec![], vec![create(D1B)], vec![create(D1A)]])
        .with_grant(sticky(1, D1))
        .with_faults(1, 0);
    m.route_fence = true;
    m
}

#[test]
fn f1_design_a_stale_route_is_fenced_and_never_refused_for_good() {
    assert_clean_within("f1-stale", &f1_stale(), CAP_FAULTS);
}

#[test]
fn f1_design_a_current_route_passes_the_fence() {
    assert_clean_within("f1-current", &f1_current(), CAP_FAULTS);
}

/// The executor runs a forward whatever it was routed by: the op runs at the
/// holder, which is lost before it answers; the retry reaches the root with
/// `touched == 0`, which refuses it for good while the holder's row lives.
#[test]
fn f1_knob_unfenced_forward_is_found() {
    let mut m = f1_stale();
    m.unfenced_forward = true;
    assert_finds_within(
        "f1-stale+unfenced_forward",
        &m,
        "refused_never_completes",
        CAP_FAULTS,
    );
}

/// The root refuses whatever the rid touched (the behaviour before the
/// fence): the fenced resend ran at the holder, and the root refuses the
/// retry.
#[test]
fn f1_knob_root_ignores_touched_is_found() {
    let mut m = f1_stale();
    m.root_ignores_touched = true;
    assert_finds_within(
        "f1-stale+root_ignores_touched",
        &m,
        "refused_never_completes",
        CAP_FAULTS,
    );
}

// ======== chunk 12 (plan 46 M1.3 step 10, ADR-50): the roster object and stale
// views. The root's roster reads can arrive out of order or incomplete, and a
// joining node's first message can land elsewhere or by the inbox. The oracle
// judges the root's singleton test against what the root was actually told
// (an installed complete read, a message from a node, an incomplete read), not
// against its own view.

/// C12-T1: node 0 is a singleton and answers `/a`, `/b` from its disk; node 1
/// joins; its cache of the lease holder is empty, so its first send of each op
/// goes by the root's inbox. Old and incomplete reads reach the root. Node 1
/// unlinks `/a` and may claim the lease from the archive (the singleton's
/// death-then-join, Q6: its unshipped answered rows are exported).
fn c12_t1() -> NodeStreamModel {
    let mut m = c8_t1();
    m.old_reads = true;
    m.partial_reads = true;
    m.told_messages = true;
    m.stale_root_cache = 1 << 1;
    m.cache_holder = CACHE_INBOX;
    m
}

/// C12-T2: nodes 0 and 1 are in the roster, node 1 may leave and node 2 may
/// join; node 2's cache names node 1, so its first send of each op bounces
/// there (nothing runs). Old and incomplete reads; one root claim.
fn c12_t2() -> NodeStreamModel {
    let mut m =
        NodeStreamModel::design(vec![vec![create(RA), create(RB)], vec![], vec![unlink(RA)]])
            .with_roster(0b011, 1 << 2, 1 << 1)
            .with_faults(0, 1);
    m.old_reads = true;
    m.partial_reads = true;
    m.told_messages = true;
    m.stale_root_cache = 1 << 2;
    m.cache_holder = 1;
    m
}

/// C12-T3: node 1 holds `/D1` (placed, its group `{holder, root}`) and may leave the roster (with
/// `leave_keeps_grants` on in the knob's runs, so that it leaves with its
/// generation live); node 2 may join (the runs with a claim add one
/// deposition). The root writes `/D1/b` (forwarded to the holder) and node 2
/// `/b`. (A claim after a leave is not in the clean run: a deposition drops an
/// unshipped end of the leaver's grant from the journal, which leaves the
/// grant live and the node off the roster: the open step 14 question.)
fn c12_t3() -> NodeStreamModel {
    let mut m =
        NodeStreamModel::design(vec![vec![create(D1B)], vec![create(D1A)], vec![create(RB)]])
            .with_grant(placed(1, D1, 0))
            .with_roster(0b011, 1 << 2, 1 << 1)
            .with_stream_release(1);
    m.told_messages = true;
    m
}

#[test]
fn c12_design_t1_stale_reads_and_inbox_first_message() {
    assert_clean_within("c12-t1", &c12_t1(), CAP_FAULTS);
}

#[test]
fn c12_design_t2_stale_cache_names_a_node() {
    assert_clean_within("c12-t2", &c12_t2(), CAP_FAULTS);
}

#[test]
fn c12_design_t3_grant_holder_leaves() {
    assert_clean_within("c12-t3", &c12_t3(), CAP_FAULTS);
}

/// An older read replaces the installed one: the root forgets the joiner it
/// had read and answers alone again. `roster_view_monotone` finds the view;
/// with that and the lone oracle off, the harm: the joiner claims from the
/// archive and a row answered alone is lost (I5).
#[test]
fn c12_knob_roster_installs_old_read_is_found() {
    let mut m = c12_t1();
    m.roster_installs_old_read = true;
    assert_finds_within(
        "c12-t1+roster_installs_old_read",
        &m,
        "roster_view_monotone",
        CAP_FAULTS,
    );
    m.check_lone = false;
    assert_finds_within(
        "c12-t1+roster_installs_old_read+i5",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_FAULTS,
    );
}

/// A partial read replaces the view and counts as complete (the core's
/// roster before step 10: a scan that lost one record emptied it).
#[test]
fn c12_knob_roster_partial_shrinks_is_found() {
    for (label, m) in [
        ("c12-t1+roster_partial_shrinks", c12_t1()),
        ("c12-t2+roster_partial_shrinks", c12_t2()),
    ] {
        let mut m = m;
        m.roster_partial_shrinks = true;
        assert_finds_within(label, &m, "roster_view_monotone", CAP_FAULTS);
        m.check_lone = false;
        assert_finds_within(
            &format!("{label}+i5"),
            &m,
            "rollback_only_singleton_or_operator",
            CAP_FAULTS,
        );
    }
}

/// The singleton test ignores that the latest read was incomplete: the root
/// that had read a roster of one stays a singleton after a read that may
/// have lost the joiner, and answers alone.
#[test]
fn c12_knob_singleton_while_partial_is_found() {
    let mut m = c12_t2();
    m.singleton_while_partial = true;
    assert_finds_within(
        "c12-t2+singleton_while_partial",
        &m,
        "no_lone_answer_with_peer",
        CAP_FAULTS,
    );
}

/// Only a first direct forward is evidence of a peer: the joiner's first
/// op arrives by the inbox, the root (whose roster read has not listed the
/// joiner yet) answers it alone, and the joiner's claim loses it (I5: a
/// singleton's rollback is of its own clients' rows).
#[test]
fn c12_knob_evidence_only_on_forward_is_found() {
    let mut m = c12_t1();
    m.evidence_only_on_forward = true;
    m.check_lone = false;
    assert_finds_within(
        "c12-t1+evidence_only_on_forward",
        &m,
        "rollback_only_singleton_or_operator",
        CAP_FAULTS,
    );
    m.check_lone = true;
    assert_finds_within(
        "c12-t1+evidence_only_on_forward+lone",
        &m,
        "no_lone_answer_with_peer",
        CAP_FAULTS,
    );
}

/// The singleton test ignores a live generation another node holds (the
/// node left the roster with its grant live): the root is a singleton with
/// a delegate.
#[test]
fn c12_knob_singleton_with_foreign_grant_is_found() {
    let mut m = c12_t3().with_faults(0, 1);
    m.leave_keeps_grants = true;
    m.singleton_with_foreign_grant = true;
    assert_finds_within(
        "c12-t3+singleton_with_foreign_grant",
        &m,
        "no_singleton_with_live_foreign_grant",
        CAP_FAULTS,
    );
    m.check_foreign_grant = false;
    assert_finds_within(
        "c12-t3+singleton_with_foreign_grant+i1",
        &m,
        "ack_implies_committed_by_config",
        CAP_FAULTS,
    );
}

/// With the grant clause in the test (the product), a node that left with
/// its grant live never makes the root a singleton.
#[test]
fn c12_a_live_foreign_grant_keeps_the_root_a_non_singleton() {
    let mut m = c12_t3().with_faults(0, 1);
    m.scripts[2].clear();
    m.leave_keeps_grants = true;
    let started = Instant::now();
    let checker = run(
        "c12-t3+leave_keeps_grants",
        &m,
        CAP_FAULTS,
        HasDiscoveries::AnyOf(BTreeSet::from(["no_singleton_with_live_foreign_grant"])),
    );
    assert!(
        checker.is_done()
            && checker.state_count() < CAP_FAULTS.0 * GENERATED
            && checker.unique_state_count() < CAP_FAULTS.0
            && started.elapsed() < CAP_FAULTS.1,
        "expected exhaustive exploration within budget"
    );
    assert!(checker
        .discovery("no_singleton_with_live_foreign_grant")
        .is_none());
}
