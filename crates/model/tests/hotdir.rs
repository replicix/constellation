//! Plan 30 §M12: hot directories — commutative parent attributes under
//! an HLC, hash-range ownership, the shared/exclusive parent hold.
//!
//! `cargo test -p constellation-model --release --test hotdir -- --nocapture`

use constellation_model::hotdir::{Action, HotDirModel, Kind, State, SAW_ALL_APPLIED};
use stateright::{Checker, HasDiscoveries, Model};
use std::time::{Duration, Instant};

const CAP: (usize, Duration) = (5_000_000, Duration::from_secs(55));

const ALWAYS: &[&str] = &[
    "converged",
    "mtime_monotone",
    "no_double_create",
    "acked_create_stands",
];

fn holds(model: &HotDirModel, name: &'static str, s: &State) -> bool {
    (model.property(name).condition)(model, s)
}

fn initial(model: &HotDirModel) -> State {
    model
        .init_states()
        .into_iter()
        .next()
        .expect("one initial state")
}

/// Walk `steps` strictly: every step must be enabled.
fn walk(model: &HotDirModel, steps: &[Action]) -> State {
    let mut state = initial(model);
    for (i, want) in steps.iter().enumerate() {
        let now = model.next_steps(&state);
        let Some((_, next)) = now.into_iter().find(|(a, _)| a == want) else {
            let mut enabled = Vec::new();
            model.actions(&state, &mut enabled);
            panic!("step {i} ({want:?}) not enabled; enabled: {enabled:?}");
        };
        state = next;
    }
    state
}

fn assert_counterexample(label: &str, model: &HotDirModel, prop: &'static str, steps: &[Action]) {
    let before = walk(model, &steps[..steps.len() - 1]);
    let end = walk(model, steps);
    assert!(
        holds(model, prop, &before),
        "{label}: `{prop}` already fails one step before the end"
    );
    assert!(
        !holds(model, prop, &end),
        "{label}: `{prop}` should fail at the last step; violation {:?}",
        end.violation
    );
    println!(
        "{label}: hand-built path violates `{prop}` at step {}",
        steps.len()
    );
}

/// Under the design the same steps either stop being enabled or keep
/// every property.
fn assert_design_survives(label: &str, model: &HotDirModel, steps: &[Action]) {
    let mut state = initial(model);
    for (i, want) in steps.iter().enumerate() {
        let now = model.next_steps(&state);
        match now.into_iter().find(|(a, _)| a == want) {
            Some((_, next)) => state = next,
            None => {
                println!("{label}: the design does not enable step {i} ({want:?})");
                return;
            }
        }
        for p in ALWAYS {
            assert!(
                holds(model, p, &state),
                "{label}: the design violates `{p}` at step {i}: {:?}",
                state.violation
            );
        }
    }
    println!("{label}: the design walks the whole path with every property intact");
}

fn run(label: &str, model: &HotDirModel, stop_at_failure: bool) -> impl Checker<HotDirModel> {
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

fn assert_search_finds(label: &str, model: &HotDirModel, prop: &'static str) {
    let checker = run(label, model, true);
    let d = checker.discovery(prop).unwrap_or_else(|| {
        panic!("{label}: expected the search to find a `{prop}` counterexample")
    });
    println!("{label}: search found `{prop}`:");
    for (k, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {k:2}: {a:?}");
    }
}

fn assert_clean(label: &str, model: &HotDirModel, witnesses: &[&'static str]) {
    let started = Instant::now();
    let checker = run(label, model, false);
    assert!(
        checker.is_done() && checker.state_count() < CAP.0 && started.elapsed() < CAP.1,
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
    for w in witnesses {
        assert!(
            checker.discovery(w).is_some(),
            "{label}: witness `{w}` never reached"
        );
    }
    println!("{label}: clean, witnesses {witnesses:?}");
}

/// Naive: last-writer-wins parent mtime from raw wall clocks. Node 1's
/// clock is one ahead; node 2 applies node 1's create, then its own
/// create stamps the parent *earlier*: the parent's mtime went
/// backwards after a create (pjdfstest's "mtime increases"). Under the
/// HLC node 2's stamp is above what it applied.
#[test]
fn lww_parent_mtime_without_hlc_goes_backwards() {
    let naive = HotDirModel {
        hlc: false,
        merge_max: false,
        init_offsets: vec![0, 1, 0],
        max_jumps: 0,
        ..HotDirModel::design()
    };
    let path = [
        Action::Exec(1, Kind::Create(0)),
        Action::Append(1),
        Action::Apply(2),
        Action::Exec(2, Kind::Create(1)),
    ];
    assert_counterexample("lww-no-hlc", &naive, "mtime_monotone", &path);
    let design = HotDirModel {
        init_offsets: vec![0, 1, 0],
        max_jumps: 0,
        ..HotDirModel::design()
    };
    assert_design_survives("lww-no-hlc", &design, &path);
    assert_search_finds("lww-no-hlc", &naive, "mtime_monotone");
    // With `max` merges alone (no HLC) the parent never goes backwards
    // either, and the replicas still converge: the HLC is what keeps a
    // node's *own* stamps increasing across what it applied.
    let max_only = HotDirModel {
        hlc: false,
        init_offsets: vec![0, 1, -1],
        with_rename: false,
        with_rmdir: false,
        ..design.clone()
    };
    assert_clean("max-merge-no-hlc", &max_only, &["all_applied"]);
}

/// Naive: last-writer-wins with HLC stamps. The root appends the two
/// delegates' streams in arrival order, not stamp order: its parent
/// mtime steps back, and the replicas end on different stamps.
#[test]
fn lww_parent_mtime_with_hlc_still_diverges() {
    let naive = HotDirModel {
        merge_max: false,
        max_jumps: 0,
        ..HotDirModel::design()
    };
    let path = [
        Action::Exec(1, Kind::Create(0)),
        Action::Tick,
        Action::Tick,
        Action::Exec(2, Kind::Create(1)),
        Action::Append(2),
        Action::Append(1),
    ];
    assert_counterexample("lww-hlc", &naive, "mtime_monotone", &path);
    assert_design_survives("lww-hlc", &HotDirModel::design(), &path);
}

/// The design: HLC stamps, `max` times, additive nlink — every
/// interleaving of executions, appends and applies (clock jumps
/// included) converges with monotone parent times.
#[test]
fn design_converges_under_reordering_and_skew() {
    // Skewed clocks from the start (node 1 ahead, node 2 behind)...
    let m = HotDirModel {
        with_rename: false,
        with_rmdir: false,
        init_offsets: vec![0, 1, -1],
        ..HotDirModel::design()
    };
    assert_clean("design-attrs-skew", &m, &["all_applied"]);
    // ...and clocks that jump mid-run (one op per node keeps the
    // product of interleavings and jumps within the budget).
    let m = HotDirModel {
        with_rename: false,
        with_rmdir: false,
        max_jumps: 1,
        ops_per_node: 1,
        ..HotDirModel::design()
    };
    assert_clean("design-attrs-jumps", &m, &["all_applied"]);
}

/// Naive: no range ownership — two delegates each execute a create of
/// the same name on their own view: both succeed, the log carries the
/// name twice.
#[test]
fn double_create_without_range_ownership() {
    let naive = HotDirModel {
        range_owner_check: false,
        with_rename: false,
        with_rmdir: false,
        max_jumps: 0,
        ..HotDirModel::design()
    };
    let path = [
        Action::Exec(1, Kind::Create(0)),
        Action::Exec(2, Kind::Create(0)),
        Action::Append(1),
        Action::Append(2),
    ];
    assert_counterexample("no-ranges", &naive, "no_double_create", &path);
    assert_design_survives("no-ranges", &HotDirModel::design(), &path);
    assert_search_finds("no-ranges", &naive, "no_double_create");
}

/// Naive: the source range's delegate executes a cross-range rename on
/// its own; the destination range's delegate creates the destination
/// name meanwhile: the name is created twice. The design executes it at
/// the root after both ranges were recalled and drained.
#[test]
fn cross_range_rename_by_the_delegate_races_the_destination() {
    let naive = HotDirModel {
        rename_via_root: false,
        with_rmdir: false,
        max_jumps: 0,
        ..HotDirModel::design()
    };
    let path = [
        Action::Exec(1, Kind::Create(0)),
        Action::Append(1),
        Action::Exec(1, Kind::Rename(0, 1)),
        Action::Exec(2, Kind::Create(1)),
        Action::Append(2),
        Action::Append(1),
    ];
    assert_counterexample("rename-by-delegate", &naive, "no_double_create", &path);
    assert_design_survives(
        "rename-by-delegate",
        &HotDirModel {
            with_rmdir: false,
            max_jumps: 0,
            ..HotDirModel::design()
        },
        &path,
    );
    assert_search_finds("rename-by-delegate", &naive, "no_double_create");
}

/// The design's ranges: every interleaving, the cross-range rename
/// included, keeps every name created once and converges.
#[test]
fn design_ranges_and_cross_range_rename_are_safe() {
    let m = HotDirModel {
        with_rmdir: false,
        max_jumps: 0,
        ..HotDirModel::design()
    };
    assert_clean("design-ranges", &m, &["all_applied", "cross_rename_ran"]);
}

/// Naive: `rmdir S` takes only its dentry `(D, "S")`. The delegate of
/// `S` acknowledged a create in it; the root removes `S` and then
/// appends the create into a directory the log no longer has. The
/// design's exclusive hold on `S` recalls its delegate first.
#[test]
fn rmdir_without_exclusive_hold_orphans_an_acked_create() {
    let naive = HotDirModel {
        exclusive_rmdir: false,
        with_rename: false,
        max_jumps: 0,
        ..HotDirModel::design()
    };
    let path = [
        Action::Exec(1, Kind::CreateInS),
        Action::Exec(0, Kind::RmdirS),
        Action::Append(1),
    ];
    assert_counterexample("rmdir-shared", &naive, "acked_create_stands", &path);
    assert_design_survives(
        "rmdir-shared",
        &HotDirModel {
            with_rename: false,
            max_jumps: 0,
            ..HotDirModel::design()
        },
        &path,
    );
    assert_search_finds("rmdir-shared", &naive, "acked_create_stands");
}

/// The design's holds: creates in `D` and `S` commute with each other
/// (shared holds on the parents), `rmdir S` is exclusive; every
/// interleaving keeps every acknowledged create and converges.
#[test]
fn design_shared_and_exclusive_holds_are_linearizable() {
    let m = HotDirModel {
        with_rename: false,
        max_jumps: 0,
        ..HotDirModel::design()
    };
    assert_clean("design-holds", &m, &["all_applied", "rmdir_ran"]);
}

/// The whole design at once: ranges, the cross-range rename, `rmdir S`,
/// clock jumps.
#[test]
fn design_everything() {
    let m = HotDirModel::design();
    let checker = run("design-all", &m, false);
    for p in ALWAYS {
        assert!(
            checker.discovery(p).is_none(),
            "design-all: `{p}` violated: {:?}",
            checker.discovery(p).map(|d| d.clone().into_actions())
        );
    }
    let s = checker
        .discovery("all_applied")
        .expect("quiescence reached");
    assert!(s.last_state().saw & SAW_ALL_APPLIED != 0);
}
