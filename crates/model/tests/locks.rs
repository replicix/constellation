//! Plan 30 §M14: leased cross-node locks (see `constellation_model::locks`).
//!
//! The counterexamples first — no fencing, a grant before expiry plus
//! margin under drift, a fast takeover without replication or grace, a
//! delegation move that loses lock state, a delegate outwaited without
//! the cap — each asserted *found* by the checker, with the discovered
//! path printed. Then the design, clean: recall and release, recall by
//! expiry, cached re-locks, drift within the margin, TTL and fast
//! takeovers with reclaim, and delegation moves both ways.
//!
//! `cargo test -p constellation-model --release --test locks -- --nocapture`

use constellation_model::locks::{Action, LockModel, Mode, MoveStamp, Msg, Op, State, NONE};
use stateright::{Checker, HasDiscoveries, Model};
use std::time::{Duration, Instant};

// `drift-margin-3` is the largest clean run: 43.7M states (13.9M unique)
// since parked requests carry their arrival time (PROGRESS.md "Fix: M14
// follow-ups"); 16.9M before.
const CAP: (usize, Duration) = (60_000_000, Duration::from_secs(150));

/// Clean runs go breadth-first (exhaustive, shortest paths); violation
/// runs go depth-first, which reaches the deep counterexamples (a
/// renewal chain past a grace period) within the budget.
fn run(label: &str, model: &LockModel) -> impl Checker<LockModel> {
    run_within(label, model, CAP)
}

fn run_within(label: &str, model: &LockModel, cap: (usize, Duration)) -> impl Checker<LockModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(cap.0)
        .timeout(cap.1)
        .spawn_bfs()
        .join();
    report(label, &checker, started);
    checker
}

/// As `run_within`, depth-first: exhaustive too, with a stack for a
/// frontier instead of a queue of whole states (a breadth-first run of
/// the F-c configurations holds tens of gigabytes), but counterexamples
/// are not shortest.
fn run_within_dfs(
    label: &str,
    model: &LockModel,
    cap: (usize, Duration),
) -> impl Checker<LockModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(cap.0)
        .timeout(cap.1)
        .spawn_dfs()
        .join();
    report(label, &checker, started);
    checker
}

fn run_dfs(label: &str, model: &LockModel) -> impl Checker<LockModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(CAP.0)
        .timeout(CAP.1)
        .spawn_dfs()
        .join();
    report(label, &checker, started);
    checker
}

fn report(label: &str, checker: &impl Checker<LockModel>, started: Instant) {
    println!(
        "{label}: {} states ({} unique), max depth {}, is_done={}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        checker.is_done(),
        started.elapsed()
    );
}

fn assert_violates(label: &str, model: &LockModel) {
    let checker = run_dfs(label, model);
    let d = checker
        .discovery("mutual_exclusion")
        .unwrap_or_else(|| panic!("{label}: expected a mutual-exclusion counterexample"));
    println!("{label}: counterexample (t = {}):", d.last_state().t);
    for (s, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {s:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
}

/// As `assert_violates`, but the search stops at the first failure: for
/// a state space whose counterexample a depth-first search may otherwise
/// reach only near the shared budget.
fn assert_violates_soon(label: &str, model: &LockModel) {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .finish_when(HasDiscoveries::AnyFailures)
        .target_state_count(CAP.0)
        .timeout(CAP.1)
        .spawn_dfs()
        .join();
    report(label, &checker, started);
    let d = checker
        .discovery("mutual_exclusion")
        .unwrap_or_else(|| panic!("{label}: expected a mutual-exclusion counterexample"));
    println!("{label}: counterexample (t = {}):", d.last_state().t);
    for (s, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {s:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
}

/// As [`assert_violates_soon`], breadth first: the shortest counterexample,
/// for a state space a depth-first search wanders in.
fn assert_violates_bfs(label: &str, model: &LockModel) {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .finish_when(HasDiscoveries::AnyFailures)
        .target_state_count(CAP.0)
        .timeout(CAP.1)
        .spawn_bfs()
        .join();
    report(label, &checker, started);
    let d = checker
        .discovery("mutual_exclusion")
        .unwrap_or_else(|| panic!("{label}: expected a mutual-exclusion counterexample"));
    println!("{label}: counterexample (t = {}):", d.last_state().t);
    for (s, a) in d.clone().into_actions().into_iter().enumerate() {
        println!("  {s:2}: {a:?}");
    }
    println!("  final: {:?}", d.last_state().violation);
}

fn assert_clean(label: &str, model: &LockModel, sometimes: &[&'static str]) {
    assert_clean_within(label, model, sometimes, CAP);
}

fn assert_clean_within(
    label: &str,
    model: &LockModel,
    sometimes: &[&'static str],
    cap: (usize, Duration),
) {
    let checker = run_within(label, model, cap);
    check_clean(label, model, sometimes, cap, checker);
}

fn assert_clean_dfs(label: &str, model: &LockModel, sometimes: &[&'static str]) {
    assert_clean_dfs_within(label, model, sometimes, CAP);
}

fn assert_clean_dfs_within(
    label: &str,
    model: &LockModel,
    sometimes: &[&'static str],
    cap: (usize, Duration),
) {
    let checker = run_within_dfs(label, model, cap);
    check_clean(label, model, sometimes, cap, checker);
}

fn check_clean(
    label: &str,
    _model: &LockModel,
    sometimes: &[&'static str],
    cap: (usize, Duration),
    checker: impl Checker<LockModel>,
) {
    assert!(
        checker.is_done() && checker.state_count() < cap.0,
        "{label}: expected exhaustive exploration within budget"
    );
    if let Some(d) = checker.discovery("table_consistent") {
        for a in d.clone().into_actions() {
            println!("  {a:?}");
        }
        panic!("{label}: an owner's table holds conflicting live grants");
    }
    if let Some(d) = checker.discovery("mutual_exclusion") {
        for (i, (a, st)) in d
            .clone()
            .into_actions()
            .into_iter()
            .zip(d.clone().into_states().into_iter().skip(1))
            .enumerate()
        {
            println!(
                "  {i:2}: {a:?} -> t={} owner={:?} net={:?}",
                st.t, st.owner, st.net
            );
            for (k, n) in st.nodes.iter().enumerate() {
                println!(
                    "        n{k}: cache={:?} held={:?} table={:?} floor={:?}",
                    n.cache, n.held, n.table, n.floor
                );
            }
        }
        panic!(
            "{label}: mutual exclusion violated: {:?}",
            d.last_state().violation
        );
    }
    for p in sometimes {
        assert!(
            checker.discovery(p).is_some(),
            "{label}: `{p}` never witnessed (vacuous?)"
        );
    }
}

use Mode::{Exclusive as X, Shared as S};
use Op::{Io, Lock, Unlock};

/// Root 0 never locks; 1 and 2 contend for `X`.
fn contend() -> Vec<Vec<Op>> {
    vec![vec![], vec![Lock(X), Io, Unlock], vec![Lock(X), Io]]
}

// ---- the naive variants ----

/// The application keeps going after its lease lapsed: the owner
/// outwaits the unanswered recall (a lost message) and grants the other
/// node, whose grant the first node's I/O then lands under.
#[test]
fn no_fencing_violates() {
    let m = LockModel {
        fencing: false,
        ..LockModel::design(contend())
    };
    assert_violates("no-fencing", &m);
}

/// F1 (`cst-load-review` §2): node 1's write is admitted while its grant
/// is honoured and lands at the owner later — it passed its fencing-token
/// check, then waited for the store's writer lock behind a slow writer.
/// Meanwhile its grant lapses, the owner outwaits it and grants node 2.
fn delayed_io(token_check_at_write: bool) -> LockModel {
    LockModel {
        delayed_io: true,
        token_check_at_write,
        ..LockModel::design(contend())
    }
}

/// Checked only at admission (the code before), the write lands under
/// node 2's grant: mutual exclusion violated (a lost update).
#[test]
fn delayed_io_without_the_write_check_violates() {
    assert_violates("delayed-io-no-write-check", &delayed_io(false));
}

/// Checked where it lands, against the owner's table: refused once the
/// grant is gone there, so it lands only before any conflicting grant.
#[test]
fn delayed_io_with_the_write_check_is_clean() {
    assert_clean(
        "delayed-io-write-check",
        &delayed_io(true),
        &["all_ops_done", "recall_expired", "recall_released"],
    );
}

/// Clocks within `D = 1` that may each step once: with margins of 1 on
/// both sides (`M_n + M_s < 4D`) the owner's outwait ends before the
/// holder's honoured window does.
fn drifting(margin: i16, lock_ttl: i16, max_tick: u8) -> LockModel {
    LockModel {
        seq_margin: margin,
        node_margin: margin,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1, -1]),
        max_jumps: 2,
        max_drops: 1,
        max_tick,
        max_renews: 0,
        ..LockModel::design(contend())
    }
}

#[test]
fn granting_before_expiry_plus_margin_violates_under_drift() {
    assert_violates("drift-margin-1", &drifting(1, 3, 6));
}

#[test]
fn the_lease_margin_is_clean_under_drift() {
    assert_clean(
        "drift-margin-3",
        &drifting(3, 4, 8),
        &["all_ops_done", "recall_expired"],
    );
}

/// A sealed backup (node 2) takes the live lease over without the
/// mirror and without the grace: it grants itself while node 1 still
/// honours the old root's grant.
#[test]
fn fast_takeover_without_replication_or_grace_violates() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: false,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_violates("fast-no-repl-no-grace", &m);
}

/// The mirror alone is not enough: it is asynchronous, so a grant made
/// after the last mirror is unknown to the successor.
#[test]
fn fast_takeover_with_only_the_async_mirror_violates() {
    let m = LockModel {
        backup: 2,
        replicate: true,
        grace: false,
        max_drops: 1,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_violates("fast-mirror-no-grace", &m);
}

/// The grace period alone is the safety argument; the mirror is an
/// availability optimisation (a known grant needs no reclaim).
#[test]
fn fast_takeover_with_grace_is_clean() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_clean(
        "fast-grace",
        &m,
        &["all_ops_done", "fast_takeover", "reclaimed"],
    );
}

/// Plan 43 C7-H2: the backup's clock is ahead of the old root's by more
/// than the lease (9 against 6), so it reads the lease as expired; its
/// takeover, a permitted one, is a fast one with the grace all the same.
/// (Classified by its clock against the old expiry, as the model did, it
/// counted as a TTL takeover: no grace, a grant over a honoured one.)
#[test]
fn fast_takeover_with_grace_is_clean_with_a_backup_far_ahead() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        init_offsets: Some(vec![0, 0, 9]),
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_clean("fast-grace-far-ahead", &m, &["fast_takeover", "reclaimed"]);
}

/// Plan 43 C7-F F-a: the root beats to the backup (`beat_cap`); the
/// backup's grace ends where its own clock puts the root's lease, plus a
/// margin, when that is before the bound. Clean with the backup's clock
/// level with, far ahead of and far behind the root's; the cap witnessed
/// where it can be (level).
#[test]
fn fast_takeover_with_beat_cap_is_clean() {
    for (label, offsets) in [
        ("level", vec![0, 0, 0]),
        ("far-ahead", vec![0, 0, 9]),
        ("far-behind", vec![0, 0, -9]),
    ] {
        let m = LockModel {
            backup: 2,
            replicate: false,
            grace: true,
            beat_cap: true,
            lease_ttl: 6,
            lock_ttl: 3,
            init_offsets: Some(offsets),
            max_drops: 0,
            max_tick: 6,
            ..LockModel::design(contend())
        };
        let expect: &[&'static str] = if label == "level" {
            &["fast_takeover", "reclaimed", "grace_capped"]
        } else {
            &["fast_takeover", "reclaimed"]
        };
        assert_clean(&format!("fast-beat-cap-{label}"), &m, expect);
    }
}

/// One locker on the old root, one on the backup, neither unlocking: the
/// smallest scripts that can show a grant honoured past a grace.
fn two_lockers() -> Vec<Vec<Op>> {
    vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]]
}

/// The same under clocks within `D = 1` that step once, margins of 3.
#[test]
fn fast_takeover_with_beat_cap_is_clean_under_drift() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        beat_cap: true,
        seq_margin: 3,
        node_margin: 3,
        lease_margin: 3,
        lease_ttl: 12,
        lock_ttl: 4,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1, -1]),
        max_jumps: 1,
        max_drops: 0,
        max_tick: 5,
        ..LockModel::design(two_lockers())
    };
    assert_clean("fast-beat-cap-drift", &m, &["fast_takeover"]);
}

/// Executes `a` in `s`, which must be enabled there.
fn step(m: &LockModel, s: &State, a: Action) -> State {
    let mut enabled = Vec::new();
    m.actions(s, &mut enabled);
    assert!(enabled.contains(&a), "{a:?} is not enabled at t={}", s.t);
    m.next_state(s, a).expect("a state")
}

/// Delivers the first message in flight that `is` matches.
fn deliver(m: &LockModel, s: &State, is: impl Fn(&Msg) -> bool) -> State {
    let k = s.net.iter().position(is).expect("the message is in flight");
    step(m, s, Action::Deliver(k))
}

/// The scenario the naive cap gets wrong, as a trace (the checker's
/// search for it exceeds the budget): the root (node 0, lease to 6) beats
/// to the backup (node 2) at t = 0, renews at t = 4 (to 10), grants node 1
/// an exclusive lock honoured until 8, and node 2 takes the lease over at
/// t = 4. Returns the state after the takeover.
fn renewal_after_the_beat(m: &LockModel) -> State {
    let mut s = m.init_states().remove(0);
    s = step(m, &s, Action::Beat(0));
    for _ in 0..4 {
        s = step(m, &s, Action::Tick);
    }
    s = step(m, &s, Action::RenewLease(0));
    s = step(m, &s, Action::Start(1));
    s = deliver(m, &s, |x| matches!(x, Msg::LockReq { .. }));
    s = deliver(m, &s, |x| matches!(x, Msg::LockReply { .. }));
    assert_eq!(s.nodes[1].cache.map(|c| c.until), Some(8));
    step(m, &s, Action::Takeover(2))
}

fn renewing_root(naive: bool) -> LockModel {
    LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        beat_cap: true,
        beat_cap_from_named_expiry: naive,
        allow_renew: true,
        lease_ttl: 6,
        lock_ttl: 5,
        max_drops: 0,
        max_tick: 8,
        ..LockModel::design(two_lockers())
    }
}

/// The naive cap (`X` = what the beats named) with a root that renews its
/// lease after the last beat: the grace ends (named 6 + lag 0 + margin 1
/// = 7) while the renewed lease still backs a grant honoured until 8, and
/// the backup grants over it.
#[test]
fn naive_beat_cap_with_a_renewal_after_the_beat_violates() {
    let m = renewing_root(true);
    let mut s = renewal_after_the_beat(&m);
    assert_eq!(s.nodes[2].floor, Some(7));
    s = step(&m, &s, Action::Start(2));
    s = deliver(&m, &s, |x| matches!(x, Msg::LockReq { .. }));
    for _ in 0..3 {
        s = step(&m, &s, Action::Tick);
    }
    // The new root probes the register (it answers within a window of a
    // probe), then its grace is over and its parked request is served.
    s = step(&m, &s, Action::Probe(2));
    s = step(&m, &s, Action::FloorPassed(2));
    s = step(&m, &s, Action::Start(1));
    println!("naive cap: mutual exclusion violated: {:?}", s.violation);
    assert!(
        s.violation.is_some(),
        "node 1 wrote under a superseded grant"
    );
}

/// The same trace under the real cap: the grace counts the renewed lease
/// the takeover replaced (10), so the backup grants nothing before 11.
#[test]
fn the_beat_cap_counts_the_lease_the_takeover_replaced() {
    let m = renewing_root(false);
    let mut s = renewal_after_the_beat(&m);
    assert_eq!(s.nodes[2].floor, Some(11));
    s = step(&m, &s, Action::Start(2));
    s = deliver(&m, &s, |x| matches!(x, Msg::LockReq { .. }));
    for _ in 0..3 {
        s = step(&m, &s, Action::Tick);
    }
    let mut enabled = Vec::new();
    m.actions(&s, &mut enabled);
    assert!(
        !enabled.contains(&Action::FloorPassed(2)),
        "the grace ended inside the old root's renewed lease"
    );
}

#[test]
fn fast_takeover_with_grace_and_mirror_is_clean() {
    let m = LockModel {
        backup: 2,
        replicate: true,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        max_drops: 1,
        max_tick: 3,
        ..LockModel::design(contend())
    };
    // Too few ticks for the grace to pass (`fast-grace` above covers
    // completion); what this adds is the mirror's delivery, loss and
    // reordering against the takeover.
    assert_clean("fast-grace-mirror", &m, &["fast_takeover", "mirror_used"]);
}

/// Without probe freshness the deposed root (its own lease is long)
/// grants itself after the successor's grace, which grants node 1. (The
/// old root has granted before, so its lease is marked and it needs no CAS
/// to answer: a root that never granted is fenced by its first mark CAS,
/// which finds the lease replaced.)
#[test]
fn fast_takeover_without_the_probe_violates() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        probe_freshness: false,
        marked_start: true,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io], vec![]])
    };
    assert_violates("fast-no-probe", &m);
}

/// The same with the probe: the deposed root grants itself only within
/// `takeover_window` of its last probe, and its next probe deposes it.
#[test]
fn fast_takeover_with_the_probe_is_clean_for_the_old_root() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io], vec![]])
    };
    assert_clean("fast-probe", &m, &["all_ops_done", "fast_takeover"]);
}

/// Plan 41 L0: the deposed root's renewals. Node 2 (the backup) takes
/// the lease over before it expires; root 0 has not probed since, still
/// believes it holds (its lease is long) and renews its **own** grant —
/// the `Route::Me` path of `on_lock_renew_tick` — until the successor's
/// grace has passed and the successor granted itself.
fn fast_takeover_renewals(renew_probe_freshness: bool, max_tick: u8, max_renews: u8) -> LockModel {
    LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        renew_probe_freshness,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick,
        max_renews,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![], vec![Lock(X)]])
    }
}

/// Renewal freshness off (the code before plan 41 L0: `lock_renew_one`
/// checked the lease's usability only, while new grants checked
/// `strict_answer_allowed`): violated. The checker's path: node 2 takes
/// over at t = 0 (grace to t = 6), root 0 renews its own grant at
/// t = 1..5, node 2 grants itself at t = 6, root 0 does `Io` under its
/// grant. Violated at the review's size (8 ticks, 6 renewals) too; this
/// is the size the fix is checked clean at below. The search stops at
/// the first failure (the whole space is 54.9M states).
#[test]
fn fast_takeover_renewals_without_the_probe_violate() {
    assert_violates_soon("fast-renew-no-probe", &fast_takeover_renewals(false, 7, 5));
}

/// The fix: a renewal at a fast root also needs a probe within
/// `takeover_window`, and the next probe deposes it. Exhaustive at 7
/// ticks and 5 renewals (30.1M states, 9.8M unique), a size at which the
/// old rule is violated.
#[test]
fn fast_takeover_renewals_with_the_probe_are_clean() {
    // The shared state budget, a longer clock: 132 s in a debug build
    // on a loaded host.
    assert_clean_within(
        "fast-renew-probe",
        &fast_takeover_renewals(true, 7, 5),
        &["fast_takeover"],
        (CAP.0, Duration::from_secs(600)),
    );
}

/// The same at the review's size, 8 ticks and 6 renewals: 77.0M states
/// (23.9M unique), over the shared budget (release: about 2 minutes;
/// run nightly).
#[test]
#[ignore]
fn fast_takeover_renewals_with_the_probe_are_clean_at_the_violations_size() {
    assert_clean_within(
        "fast-renew-probe-8-6",
        &fast_takeover_renewals(true, 8, 6),
        &["fast_takeover"],
        RENEW_CAP,
    );
}

/// The budget of the renewal-freshness runs over the shared one.
const RENEW_CAP: (usize, Duration) = (150_000_000, Duration::from_secs(900));

/// The remote holder's variant: node 1 holds a grant from root 0 and
/// renews where it was last answered (`cached_owner`, the code's
/// `lease.cached_holder`), not at the register's holder: the deposed
/// root renews its peer's grant past the successor's grace.
fn fast_takeover_remote_renewals(
    renew_probe_freshness: bool,
    max_tick: u8,
    max_renews: u8,
) -> LockModel {
    LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        renew_probe_freshness,
        cached_owner: true,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick,
        max_renews,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X)]])
    }
}

#[test]
fn fast_takeover_remote_renewals_without_the_probe_violate() {
    assert_violates_soon(
        "fast-remote-renew-no-probe",
        &fast_takeover_remote_renewals(false, 7, 5),
    );
}

/// Clean at the size the old rule is violated at: 115.2M states (34.1M
/// unique), over the shared budget (release: about 3 minutes; run
/// nightly).
#[test]
#[ignore]
fn fast_takeover_remote_renewals_with_the_probe_are_clean() {
    assert_clean_within(
        "fast-remote-renew-probe",
        &fast_takeover_remote_renewals(true, 7, 5),
        &["fast_takeover"],
        RENEW_CAP,
    );
}

/// A TTL takeover with grants that outlive the old lease.
#[test]
fn uncapped_grants_violate_across_a_ttl_takeover() {
    let m = LockModel {
        ttl_takeover: true,
        cap_by_lease: false,
        lease_ttl: 3,
        lock_ttl: 6,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(contend())
    };
    assert_violates("ttl-uncapped", &m);
}

#[test]
fn capped_grants_are_clean_across_a_ttl_takeover() {
    let m = LockModel {
        ttl_takeover: true,
        lease_ttl: 3,
        lock_ttl: 6,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(contend())
    };
    assert_clean(
        "ttl-capped",
        &m,
        &["all_ops_done", "ttl_takeover", "fenced"],
    );
}

/// The delegate (node 2) starts with an empty table and grants itself
/// while node 1 holds the root's grant.
#[test]
fn a_delegation_move_that_loses_state_violates() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        move_state: false,
        max_drops: 0,
        max_tick: 6,
        ..LockModel::design(contend())
    };
    assert_violates("move-no-state", &m);
}

/// The same on the way back: a recall whose answer carries no table.
#[test]
fn a_recall_that_loses_state_violates() {
    let m = LockModel {
        delegate: 1,
        max_moves: 2,
        move_state: false,
        max_drops: 0,
        max_tick: 5,
        ..LockModel::design(vec![vec![], vec![], vec![Lock(X), Io, Unlock, Lock(X), Io]])
    };
    // Node 2 locks at the root, the root delegates to 1 (state moves or
    // not), then recalls; a recall that drops the table lets the root
    // grant a second time while node 2's first grant is still honoured.
    let m = LockModel {
        scripts: vec![
            vec![],
            vec![Lock(X), Io],
            vec![Lock(X), Io, Unlock, Lock(X), Io],
        ],
        ..m
    };
    assert_violates("recall-no-state", &m);
}

#[test]
fn delegation_moves_with_state_are_clean() {
    let m = LockModel {
        delegate: 2,
        max_moves: 2,
        max_drops: 0,
        max_tick: 5,
        ..LockModel::design(contend())
    };
    assert_clean("move-state", &m, &["all_ops_done", "moved"]);
}

/// The handoff rides the delegate's first renewal reply, and the
/// generation's recall overtakes it: the delegate answers the recall
/// with nothing and drops the late handoff (it no longer serves the
/// generation). A root that forgets what it handed grants over node 1's
/// live grant at once (sim seed 196252's first form).
#[test]
fn a_handoff_overtaken_by_its_recall_violates_without_reinstatement() {
    let m = LockModel {
        delegate: 2,
        max_moves: 2,
        reinstate_handed: false,
        resend_handoff: false,
        max_deleg_renews: 0,
        max_drops: 0,
        max_tick: 2,
        max_renews: 0,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    };
    assert_violates("handoff-overtaken", &m);
}

/// The design before this fix: that generation ends with a grace on the
/// subtree at the root, so the root grants nothing there — but it
/// delegates the subtree again, with an empty table, and the new
/// delegate grants over the live grant (the sim's `locks-delegated`
/// seeds 196102, 96425 and 96805).
#[test]
fn a_root_grace_that_stays_behind_violates_on_redelegation() {
    let m = LockModel {
        delegate: 2,
        max_moves: 3,
        reinstate_handed: false,
        resend_handoff: false,
        handoff_grace: true,
        // The design before this fix (a grace carried by the delegation
        // would cover this path too, less precisely than reinstatement).
        grace_follows_delegation: false,
        max_deleg_renews: 0,
        max_drops: 0,
        max_tick: 2,
        max_renews: 0,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    };
    assert_violates("handoff-grace-stays", &m);
}

/// The handoff's reply is lost; the delegate renews again and serves
/// from that reply, which carries nothing: it grants over node 1's live
/// grant. Reinstating at the generation's end does not help (the
/// generation goes on); the root re-sends what it handed.
#[test]
fn a_lost_handoff_violates_without_the_resend() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        reinstate_handed: true,
        resend_handoff: false,
        max_deleg_renews: 1,
        max_drops: 1,
        max_tick: 2,
        max_renews: 0,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    };
    assert_violates("handoff-lost", &m);
}

/// The design: handoffs overtaken by their recall, the subtree delegated
/// again, contention.
#[test]
fn overtaken_handoffs_and_redelegation_are_clean() {
    let m = LockModel {
        delegate: 2,
        max_moves: 3,
        max_deleg_renews: 0,
        max_drops: 0,
        max_tick: 4,
        ..LockModel::design(contend())
    };
    assert_clean(
        "handoff-overtaken-design",
        &m,
        &["all_ops_done", "moved", "reinstated"],
    );
}

/// Reinstating a copy next to a conflicting grant: the copy's grant was
/// released at the delegate, which then granted the other node; the
/// root's table ends up with both live (the owner would re-affirm the
/// stale one without a recall: sim seed 196004 against a first version
/// of the fix).
#[test]
fn reinstating_next_to_a_conflicting_grant_breaks_the_table() {
    let m = LockModel {
        delegate: 2,
        max_moves: 3,
        max_deleg_renews: 0,
        max_drops: 0,
        max_tick: 4,
        copies_check_conflicts: false,
        ..LockModel::design(contend())
    };
    let checker = run_dfs("copies-over-conflicts", &m);
    assert!(
        checker.discovery("table_consistent").is_some(),
        "expected a table with conflicting live grants"
    );
}

/// The design: a handoff lost, re-sent with the next renewal.
#[test]
fn lost_handoffs_resent_are_clean() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        max_deleg_renews: 1,
        max_drops: 1,
        max_tick: 4,
        ..LockModel::design(contend())
    };
    assert_clean(
        "handoff-lost-design",
        &m,
        &["all_ops_done", "moved", "resent"],
    );
}

/// A released takeover (the root shuts down gracefully with node 1's
/// grant live), then the new root delegates the subtree inside its
/// grace. Left at the root, the grace does not bind the delegate, which
/// grants over node 1's grant.
#[test]
fn a_delegation_inside_a_released_takeovers_grace_violates_if_the_grace_stays() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        released_takeover: true,
        grace_follows_delegation: false,
        max_epoch: 1,
        max_deleg_renews: 0,
        max_drops: 0,
        max_tick: 2,
        max_renews: 0,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    };
    assert_violates("released-grace-stays", &m);
}

/// The design: the grace rides the delegation's first renewal; reclaims
/// go to the delegate.
#[test]
fn a_released_takeovers_grace_follows_the_delegation() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        released_takeover: true,
        max_epoch: 1,
        max_deleg_renews: 1,
        max_drops: 0,
        max_tick: 4,
        ..LockModel::design(contend())
    };
    assert_clean(
        "released-grace-follows",
        &m,
        &[
            "all_ops_done",
            "released_takeover",
            "grace_inherited",
            "moved",
        ],
    );
}

/// A delegate outwaited by the root: the grants moved to it at the
/// delegation carry the root's windows; without a grace the root grants
/// again while those are live.
#[test]
fn an_outwaited_delegate_without_grace_violates() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        recall_by_ttl: true,
        grace: false,
        reinstate_handed: false,
        deleg_ttl: 2,
        lock_ttl: 5,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io, Unlock], vec![]])
    };
    assert_violates("deleg-ttl-no-grace", &m);
}

/// The same with the root's copies of the handoff reinstated when the
/// generation ends: clean without the grace — the copies are exactly
/// the grants the grace protected (the code keeps the grace anyway).
#[test]
fn an_outwaited_delegate_with_reinstatement_needs_no_grace() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        recall_by_ttl: true,
        grace: false,
        deleg_ttl: 2,
        lock_ttl: 5,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io, Unlock], vec![]])
    };
    assert_clean(
        "deleg-ttl-reinstated",
        &m,
        &["all_ops_done", "recall_by_ttl", "moved", "reinstated"],
    );
}

#[test]
fn a_capped_delegate_outwaited_is_clean() {
    let m = LockModel {
        delegate: 2,
        max_moves: 1,
        recall_by_ttl: true,
        deleg_ttl: 2,
        lock_ttl: 5,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X), Io, Unlock], vec![]])
    };
    assert_clean(
        "deleg-ttl-capped",
        &m,
        &["all_ops_done", "recall_by_ttl", "moved"],
    );
}

// ---- a moved grant's window carried as a duration ----

/// Node 1 and the delegate (node 2) contend for `X` across delegation
/// moves. A grant goes unused when its reply is lost, or when a move
/// clears the parked request it answers and the node asks again (the
/// reply then answers a stale request).
fn carrying(
    stamp: MoveStamp,
    max_tick: u8,
    max_moves: u8,
    max_drops: u8,
    max_renews: u8,
    max_deleg_renews: u8,
) -> LockModel {
    LockModel {
        delegate: 2,
        max_moves,
        max_deleg_renews,
        max_drops,
        max_renews,
        max_tick,
        move_stamp: stamp,
        ..LockModel::design(contend())
    }
}

/// Delegated, recalled and delegated again (the sim's seed 15: a grant
/// carried delegate → root → delegate): unused and used grants and recalls
/// in flight at a move, records carried twice and outwaited.
fn across_three_moves(stamp: MoveStamp) -> LockModel {
    carrying(stamp, 5, 3, 0, 0, 0)
}

const ACROSS_THREE_MOVES: &[&str] = &[
    "all_ops_done",
    "moved",
    "reinstated",
    "carried",
    "carried_unused",
    "carried_used",
    "carried_recalling",
    "carried_outwaited",
    "carried_twice",
];

/// Delegated and recalled, with a renewal: renewals (and their answers)
/// in flight at a move.
fn with_a_renewal(stamp: MoveStamp) -> LockModel {
    carrying(stamp, 5, 2, 0, 1, 0)
}

const WITH_A_RENEWAL: &[&str] = &[
    "all_ops_done",
    "carried",
    "carried_unused",
    "carried_used",
    "carried_renewing",
    "carried_outwaited",
];

/// One delegation whose handoff may be lost: the copies re-sent with the
/// next renewal carry what is left at that send.
fn with_a_lost_handoff(stamp: MoveStamp) -> LockModel {
    carrying(stamp, 4, 1, 1, 0, 1)
}

const WITH_A_LOST_HANDOFF: &[&str] = &[
    "all_ops_done",
    "resent",
    "carried",
    "carried_unused",
    "carried_used",
    "carried_outwaited",
];

/// The rule: a grant never renewed carries what is left of its record,
/// plus the margin at the receipt.
#[test]
fn carrying_unproven_grants_across_three_moves_is_clean() {
    assert_clean(
        "carry-unproven-3-moves",
        &across_three_moves(MoveStamp::CarryUnproven),
        ACROSS_THREE_MOVES,
    );
}

#[test]
fn carrying_unproven_grants_with_a_renewal_is_clean() {
    assert_clean(
        "carry-unproven-renewal",
        &with_a_renewal(MoveStamp::CarryUnproven),
        WITH_A_RENEWAL,
    );
}

#[test]
fn carrying_unproven_grants_with_a_lost_handoff_is_clean() {
    assert_clean(
        "carry-unproven-lost-handoff",
        &with_a_lost_handoff(MoveStamp::CarryUnproven),
        WITH_A_LOST_HANDOFF,
    );
}

/// Renewed grants carried too ("carry every grant"): clean as well. The
/// moves give a grant up as they measure it (the root at the delegation,
/// the delegate at the recall), so a renewal the sender answered is in the
/// record it measures, and one that reaches it later is refused: whether
/// the grant was used is not what makes the carry safe.
#[test]
fn carrying_all_grants_across_three_moves_is_clean() {
    assert_clean(
        "carry-all-3-moves",
        &across_three_moves(MoveStamp::CarryAll),
        ACROSS_THREE_MOVES,
    );
}

#[test]
fn carrying_all_grants_with_a_renewal_is_clean() {
    assert_clean(
        "carry-all-renewal",
        &with_a_renewal(MoveStamp::CarryAll),
        WITH_A_RENEWAL,
    );
}

#[test]
fn carrying_all_grants_with_a_lost_handoff_is_clean() {
    assert_clean(
        "carry-all-lost-handoff",
        &with_a_lost_handoff(MoveStamp::CarryAll),
        WITH_A_LOST_HANDOFF,
    );
}

/// Clocks within `D = 1` that may step three times; margins of 2, the
/// least a grant made in place needs (`M_n + M_s ≥ 4D`: two clocks, a
/// step on each). One move (the handoff), so three clocks measure a
/// carried window: the holder's, the sender's (from the request's arrival
/// to the send) and the receiver's (from the receipt to the expiry). Each
/// is off by up to `2D`, which the margin at the receipt pays for. Two
/// ticks are enough for the counterexample without the margin and keep
/// the clean runs exhaustive (27.6M states, 8.8M unique).
fn carrying_under_drift(stamp: MoveStamp, max_tick: u8) -> LockModel {
    LockModel {
        delegate: 2,
        max_moves: 1,
        max_deleg_renews: 0,
        max_drops: 0,
        max_renews: 0,
        seq_margin: 2,
        node_margin: 2,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl: 4,
        deleg_ttl: 8,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1, -1]),
        max_jumps: 3,
        max_tick,
        move_stamp: stamp,
        ..LockModel::design(contend())
    }
}

#[test]
fn restamping_is_clean_under_drift() {
    assert_clean(
        "carry-drift-restamp",
        &carrying_under_drift(MoveStamp::Restamp, 2),
        &["all_ops_done", "moved"],
    );
}

#[test]
fn carrying_unproven_grants_is_clean_under_drift() {
    assert_clean(
        "carry-drift-unproven",
        &carrying_under_drift(MoveStamp::CarryUnproven, 2),
        &[
            "all_ops_done",
            "moved",
            "carried",
            "carried_unused",
            "carried_used",
            "carried_recalling",
        ],
    );
}

/// Without the margin at the receipt: the sender's clock steps forward
/// between the request's arrival and the send (less is left), the
/// receiver's while it counts that down, and the holder's back while it
/// honours the grant. The record ends inside the holder's window.
#[test]
fn carrying_without_the_margin_violates_under_drift() {
    assert_violates_soon(
        "carry-drift-no-margin",
        &carrying_under_drift(MoveStamp::CarryNoMargin, 2),
    );
}

/// The counterexample above as a trace, under both stamps: node 1 is
/// granted at the root (holder honours to 3 on its clock, the root's
/// record ends at 5 on the root's), the root's clock steps forward and it
/// delegates at once (4 left), the delegate (clock −1) records 3 without
/// the margin and its clock steps forward, node 1's steps back. At t = 2
/// the delegate's record has ended while node 1 still honours its grant.
fn carried_across_three_steps(m: &LockModel) -> State {
    let mut s = m.init_states().remove(0);
    s = step(m, &s, Action::Start(1));
    s = deliver(m, &s, |x| matches!(x, Msg::LockReq { .. }));
    s = deliver(m, &s, |x| matches!(x, Msg::LockReply { .. }));
    assert_eq!(s.nodes[1].cache.map(|c| c.until), Some(3));
    assert_eq!(s.nodes[0].table.first().map(|g| g.until), Some(5));
    s = step(m, &s, Action::Jump(0, 1));
    s = step(m, &s, Action::Delegate(2));
    s = deliver(m, &s, |x| matches!(x, Msg::Handoff { .. }));
    s = step(m, &s, Action::Jump(2, 1));
    s = step(m, &s, Action::Jump(1, -1));
    s = step(m, &s, Action::Tick);
    s = step(m, &s, Action::Tick);
    s = step(m, &s, Action::Start(2));
    deliver(m, &s, |x| matches!(x, Msg::LockReq { .. }))
}

#[test]
fn a_record_carried_without_the_margin_ends_inside_the_holders_window() {
    let m = carrying_under_drift(MoveStamp::CarryNoMargin, 2);
    let mut s = carried_across_three_steps(&m);
    assert_eq!(
        s.issued.len(),
        2,
        "the delegate granted itself over the carried record"
    );
    s = step(&m, &s, Action::Start(1));
    println!("no margin: mutual exclusion violated: {:?}", s.violation);
    assert!(
        s.violation.is_some(),
        "node 1 wrote under a superseded grant"
    );
}

#[test]
fn a_record_carried_with_the_margin_outlasts_the_holders_window() {
    let m = carrying_under_drift(MoveStamp::CarryUnproven, 2);
    let s = carried_across_three_steps(&m);
    assert_eq!(s.nodes[2].table.iter().map(|g| g.until).max(), Some(5));
    assert_eq!(
        s.issued.len(),
        1,
        "the delegate granted itself inside the carried record"
    );
}

/// Clocks `3` apart and steady (no step), margins of 2: any duration is
/// exact, so the rule is clean; the sender's `until` kept as is ends the
/// record `6` early on a receiver that far ahead.
fn offset_clocks(stamp: MoveStamp) -> LockModel {
    LockModel {
        max_jumps: 0,
        max_offset: 3,
        init_offsets: Some(vec![-3, 0, 3]),
        ..carrying_under_drift(stamp, 9)
    }
}

#[test]
fn keeping_the_senders_until_violates_under_offsets() {
    assert_violates_soon(
        "carry-keep-absolute",
        &offset_clocks(MoveStamp::KeepAbsolute),
    );
}

#[test]
fn carrying_unproven_grants_is_clean_under_offsets() {
    assert_clean(
        "carry-offsets-unproven",
        &offset_clocks(MoveStamp::CarryUnproven),
        &["all_ops_done", "moved", "carried"],
    );
}

// ---- the design ----

/// Contention on a live owner: recall answered by a release, or
/// outwaited after a lost message; the app is fenced when it lapses.
#[test]
fn contention_is_clean() {
    let m = LockModel::design(contend());
    assert_clean(
        "contend",
        &m,
        &[
            "all_ops_done",
            "recall_released",
            "recall_expired",
            "fenced",
        ],
    );
}

/// A node re-locks under its cached grant without a message, and a
/// shared pair coexists.
#[test]
fn cached_relock_and_shared_pairs_are_clean() {
    let m = LockModel {
        max_drops: 0,
        max_tick: 4,
        ..LockModel::design(vec![
            vec![],
            vec![Lock(X), Io, Unlock, Lock(S), Io],
            vec![Lock(S), Io, Unlock, Lock(X), Io],
        ])
    };
    assert_clean("relock", &m, &["all_ops_done", "local_relock"]);
}

/// The owner is itself a lock user.
#[test]
fn the_owner_as_a_lock_user_is_clean() {
    let m = LockModel {
        max_tick: 6,
        ..LockModel::design(vec![vec![Lock(X), Io, Unlock], vec![Lock(X), Io], vec![]])
    };
    assert_clean("owner-user", &m, &["all_ops_done"]);
}

/// Larger: three contenders, a fast takeover, drift and steps. Bounded
/// by the state cap.
#[test]
#[ignore]
fn deep_fast_takeover_under_drift() {
    let m = LockModel {
        backup: 2,
        replicate: true,
        grace: true,
        max_offset: 1,
        init_offsets: Some(vec![-1, 1, 0]),
        max_jumps: 2,
        seq_margin: 3,
        node_margin: 3,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl: 5,
        max_drops: 1,
        max_tick: 12,
        max_renews: 2,
        ..LockModel::design(vec![
            vec![Lock(S), Io, Unlock],
            vec![Lock(X), Io, Unlock, Lock(X), Io],
            vec![Lock(X), Io, Unlock],
        ])
    };
    let checker = run("deep", &m);
    assert!(checker.discovery("mutual_exclusion").is_none());
}

// ---- plan 43 C7-F F-c: a lapsed grant is asked for back ----

/// Node 1 locks and does I/O twice (its grant can lapse between them);
/// node 2 contends at the same owner.
fn late_scripts() -> Vec<Vec<Op>> {
    vec![vec![], vec![Lock(X), Io, Io], vec![Lock(X), Io]]
}

/// The clean base: a lapse at a live owner, no failover.
fn late_lone(scripts: Vec<Vec<Op>>) -> LockModel {
    LockModel {
        late_reclaim: true,
        lease_ttl: 8,
        lock_ttl: 3,
        max_drops: 1,
        max_tick: 8,
        ..LockModel::design(scripts)
    }
}

#[test]
fn late_reclaim_at_a_live_owner_is_clean() {
    assert_clean_dfs(
        "late-lone",
        &late_lone(late_scripts()),
        &["late_admitted", "late_io", "late_refused"],
    );
}

/// Node 1 locks and does I/O; node 2 (the backup) only asks for the lock:
/// what matters is whether it is *issued*.
fn late_issuers() -> Vec<Vec<Op>> {
    vec![vec![], vec![Lock(X), Io], vec![Lock(X)]]
}

/// A fast takeover of the root whose table the successor takes from the
/// old root's final mirror: the lapsed grant is admitted there.
fn late_failover(max_tick: u8, max_drops: u8) -> LockModel {
    LockModel {
        late_reclaim: true,
        backup: 2,
        replicate: true,
        grace: true,
        lease_ttl: 5,
        lock_ttl: 3,
        max_drops,
        max_tick,
        max_renews: 0,
        ..LockModel::design(late_issuers())
    }
}

#[test]
fn late_reclaim_across_a_fast_takeover_is_clean() {
    assert_clean_dfs(
        "late-failover",
        &late_failover(5, 0),
        &[
            "final_sent",
            "final_installed",
            "late_at_successor",
            "late_io",
        ],
    );
}

// The three naive variants, each found violating (the first is the
// counterexample the model recorded before F-c: "a late reclaim of a lapsed
// grant during a successor's grace": the old owner had outwaited the grant
// and granted another node, which the successor does not know).

/// (a) A late renewal of a grant the successor does not know is answered by
/// the grace's reclaim: node 1's grant lapsed and was outwaited at the old
/// root, which granted node 2 (the backup) before the takeover; the
/// successor has no table, reclaims node 1's grant, and node 1's I/O lands
/// after node 2's grant was issued.
#[test]
fn a_late_reclaim_of_an_unknown_grant_in_a_grace_violates() {
    let m = LockModel {
        late_reclaim: true,
        late_admit_unknown: true,
        backup: 2,
        replicate: false,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 6,
        max_renews: 0,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X)]])
    };
    assert_violates_soon("late-unknown-in-grace", &m);
}

/// The same under the design: the unknown grant is refused (`Lost`), clean.
#[test]
fn a_late_renewal_of_an_unknown_grant_is_refused_even_in_a_grace() {
    let m = LockModel {
        late_admit_unknown: false,
        ..LockModel {
            late_reclaim: true,
            backup: 2,
            replicate: false,
            grace: true,
            lease_ttl: 5,
            lock_ttl: 3,
            max_drops: 0,
            max_tick: 5,
            max_renews: 0,
            ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X)]])
        }
    };
    assert_clean_dfs("late-unknown-refused", &m, &["late_refused"]);
}

/// (b) The successor trusts a mirror that is not the old root's final one.
/// As a trace (the checker's search for it exceeds the budget): the backup
/// (node 2) holds the mirror that shows node 1's settled grant; the old
/// root (node 0) outwaits that grant and grants node 2, its later mirrors
/// still in flight when node 2 takes the lease over. Returns the state after
/// node 1's late renewal was answered.
fn late_after_a_stale_mirror(naive: bool) -> (LockModel, State) {
    let m = LockModel {
        late_reclaim: true,
        late_admit_nonfinal: naive,
        backup: 2,
        replicate: true,
        grace: true,
        lease_ttl: 6,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 6,
        max_renews: 0,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X)]])
    };
    let mut s = m.init_states().remove(0);
    s = step(&m, &s, Action::Start(1));
    s = deliver(&m, &s, |x| matches!(x, Msg::LockReq { .. }));
    s = deliver(&m, &s, |x| matches!(x, Msg::LockReply { .. }));
    // The first mirror (node 1's grant), the only one that arrives.
    s = deliver(&m, &s, |x| matches!(x, Msg::Mirror { ver: 1, .. }));
    for _ in 0..4 {
        s = step(&m, &s, Action::Tick);
    }
    // Node 1's window ended at 2; the owner's record, at 4, is outwaited.
    s = step(&m, &s, Action::Expire(0));
    s = step(&m, &s, Action::Probe(0));
    s = step(&m, &s, Action::Start(2));
    s = deliver(&m, &s, |x| matches!(x, Msg::LockReq { .. }));
    s = deliver(&m, &s, |x| matches!(x, Msg::LockReply { .. }));
    assert_eq!(s.issued.len(), 2, "node 2 was granted");
    s = step(&m, &s, Action::Takeover(2));
    s = step(&m, &s, Action::LateRenew(1));
    s = deliver(&m, &s, |x| matches!(x, Msg::Renew { .. }));
    s = deliver(&m, &s, |x| matches!(x, Msg::RenewAck { .. }));
    (m, s)
}

#[test]
fn trusting_a_non_final_mirror_violates() {
    let (m, mut s) = late_after_a_stale_mirror(true);
    assert!(s.nodes[1].cache.is_some(), "the late renewal was admitted");
    s = step(&m, &s, Action::Start(1));
    println!(
        "non-final mirror: mutual exclusion violated: {:?}",
        s.violation
    );
    assert!(s.violation.is_some(), "node 1 acted after node 2's grant");
}

/// The design: the stale mirror's record is unsettled at the successor, so
/// the late renewal is refused and node 1 never acts.
#[test]
fn a_late_renewal_after_a_non_final_mirror_is_refused() {
    let (m, s) = late_after_a_stale_mirror(false);
    assert!(s.nodes[1].cache.is_none(), "refused: Lost");
    let mut enabled = Vec::new();
    m.actions(&s, &mut enabled);
    assert!(
        !enabled.contains(&Action::Start(1)),
        "node 1 has no grant to do I/O under"
    );
    assert!(s.violation.is_none());
}

/// (c) The lapsed memory is not superseded by a conflicting grant: node 1's
/// lapsed grant is remembered, node 2 is granted over it, and node 1's late
/// renewal is admitted beside node 2's live grant.
#[test]
fn a_lapsed_memory_not_superseded_by_a_conflicting_grant_violates() {
    let m = LockModel {
        late_lapsed_unsuperseded: true,
        ..late_lone(vec![vec![], vec![Lock(X), Io], vec![Lock(X)]])
    };
    assert_violates("late-unsuperseded", &m);
}

/// The late action composes with the rest of the design: a lost message
/// between the old root and its backup (the successor falls back to the
/// unsettled mirror), a TTL takeover (nothing carries over), and the
/// delegation moves (which hand settled records over in one piece).
/// 195M states (49M unique), about 2 minutes in release: run nightly.
#[test]
#[ignore]
fn late_reclaim_with_a_lost_mirror_is_clean() {
    assert_clean_dfs_within(
        "late-failover-drop",
        &late_failover(5, 1),
        &["late_refused"],
        (200_000_000, Duration::from_secs(400)),
    );
}

#[test]
fn late_reclaim_across_a_ttl_takeover_is_clean() {
    let m = LockModel {
        late_reclaim: true,
        ttl_takeover: true,
        lease_ttl: 3,
        lock_ttl: 6,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(contend())
    };
    assert_clean_dfs("late-ttl-takeover", &m, &["ttl_takeover"]);
}

#[test]
fn late_reclaim_across_delegation_moves_is_clean() {
    let m = LockModel {
        late_reclaim: true,
        delegate: 2,
        max_moves: 1,
        max_drops: 0,
        max_tick: 5,
        max_renews: 0,
        ..LockModel::design(late_issuers())
    };
    assert_clean_dfs("late-moves", &m, &["moved"]);
}

/// Plan 43 C7a × C7-F F-c (review): the late reclaim across a fast takeover
/// on the split clocks, the old root's oscillator slow by a quarter within
/// the budget (its honour reading of its lease, the final mirror's moment,
/// and its table's wait reading drift apart; the key the successor compares
/// is the lease's wait-clock end, `Reg::expires_w`): clean, and the final
/// mirror is still sent, installed and the late I/O witnessed.
#[test]
fn late_reclaim_across_a_fast_takeover_is_clean_on_rated_split_clocks() {
    let m = LockModel {
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![-1, 0, 0]),
        rate_budget: 1,
        clock_split: true,
        ..late_failover(5, 0)
    };
    assert_clean_dfs(
        "late-failover-rated",
        &m,
        &["final_sent", "final_installed", "late_io"],
    );
}

/// The same with the locker stepped back once on the split clocks (its
/// honour reading ignores it): clean.
#[test]
fn late_reclaim_at_a_live_owner_is_clean_with_a_locker_step_on_split_clocks() {
    let m = LockModel {
        clock_split: true,
        max_offset: 1,
        max_jumps: 1,
        step_node: Some(1),
        step_dir: -1,
        max_drops: 0,
        ..late_lone(late_scripts())
    };
    assert_clean_dfs("late-lone-step", &m, &["late_admitted", "late_io"]);
}

/// The base without any of them, the same scripts: clean, and the late
/// renewal is witnessed admitted (the saw-bits: non-vacuity).
#[test]
fn the_lapsed_memory_is_superseded_by_a_conflicting_grant() {
    assert_clean_dfs(
        "late-superseded",
        &late_lone(vec![vec![], vec![Lock(X), Io], vec![Lock(X)]]),
        &[
            "late_admitted",
            "late_from_lapsed",
            "late_io",
            "late_refused",
        ],
    );
}

// ---- plan 43 C7-F F-a0: the lease's mark ----
//
// A successor arms its grace (a fast takeover) or waits out the grants of
// the tenure it takes over (a released lease) only if the lease it replaced
// is marked: `Lease::granted_delegations`. A tenure marks when it grants,
// when a release leaves something behind, and — the inherited marks — when
// it takes over a tenure it waits out the grants of, or re-claims its own
// released lease over grants it let go. Each pair is a counterexample of a
// design without one of them, and the exhaustive clean run with it.

/// Node 2 holds `X` under root 0 (node 0 never locks); node 1 asks for it
/// later. Root 0's backup is 1, root 1's is 2: the chain of a double seal.
fn double_seal(inherit_mark: bool) -> LockModel {
    LockModel {
        backup_of: vec![1, 2, NONE],
        inherit_mark,
        max_epoch: 2,
        max_drops: 0,
        max_renews: 0,
        max_tick: 3,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    }
}

/// The second seal inside the first's grace: root 0 granted, its backup 1
/// took over (the mirror may lack the grant, the grace covers it) and
/// granted nothing yet, so its lease says nothing; its own backup 2 seals
/// it, and, finding no mark, arms no grace: node 2 grants node 1 the lock
/// while node 2 honours the grant root 0 made it.
#[test]
fn a_tenure_sealed_inside_its_grace_leaves_no_grace_without_the_inherited_mark() {
    assert_violates("double-seal-unmarked", &double_seal(false));
}

#[test]
fn a_tenure_that_inherits_a_grace_marks_its_lease_and_the_double_seal_is_clean() {
    assert_clean(
        "double-seal-marked",
        &double_seal(true),
        &["double_takeover", "fast_takeover"],
    );
}

/// The chain of releases: root 0 shuts down with node 1's grant live, node
/// 2 takes the released lease over (a grace), and node 2's own backup 1
/// seals it before it has granted. A release that marks nothing leaves its
/// taker no grace at all.
fn released_chain(release_mark_duty: bool) -> LockModel {
    LockModel {
        backup_of: vec![NONE, 2, 1],
        released_takeover: true,
        release_mark_duty,
        max_epoch: 2,
        max_drops: 0,
        max_renews: 0,
        max_tick: 3,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    }
}

#[test]
fn a_release_that_never_marks_leaves_its_taker_no_grace() {
    assert_violates("released-chain-unmarked", &released_chain(false));
}

#[test]
fn a_release_that_marks_what_it_leaves_and_the_inherited_mark_are_clean_along_a_chain() {
    assert_clean(
        "released-chain-marked",
        &released_chain(true),
        &["double_takeover", "released_takeover", "fast_takeover"],
    );
}

/// Plan 43 C7-F F-a0 L3a: root 0 releases with node 1's grant live and
/// re-claims its own released lease (no takeover: it inherits no mark and
/// waits out the grants it dropped), its backup 2, listed from the
/// re-claim on, then seals it. The re-claim's lease is unmarked unless the
/// tenure marks it when it becomes sealable with a duty live: node 2 arms
/// no grace, its mirror is fresh, and it grants itself the lock over node
/// 1's grant.
fn own_reclaim(mark_at_sealable: bool) -> LockModel {
    LockModel {
        backup_of: vec![2, NONE, NONE],
        released_takeover: true,
        mark_at_sealable,
        max_epoch: 2,
        max_drops: 0,
        max_renews: 0,
        max_tick: 3,
        ..LockModel::design(vec![vec![], vec![Lock(X), Io], vec![Lock(X), Io]])
    }
}

#[test]
fn re_claiming_its_own_released_lease_unmarked_leaves_its_sealer_no_grace() {
    assert_violates("own-reclaim-unmarked", &own_reclaim(false));
}

#[test]
fn re_claiming_its_own_released_lease_marked_leaves_its_sealer_a_grace() {
    assert_clean(
        "own-reclaim-marked",
        &own_reclaim(true),
        &["double_takeover", "released_takeover", "fast_takeover"],
    );
}

#[test]
fn none_is_no_node() {
    assert_eq!(NONE, u8::MAX);
}

// ---- plan 43 C7a: rate, and the honour/wait/wall split ----
//
// The models' clocks (`constellation_model::clk`): a node's oscillator gains
// or loses `rate` ticks every `rate_period` real ticks; its wait reading
// runs slow, and its honour reading fast, by the design's `rate_budget`
// (the code's rho); `clock_split` puts each check on the reading the code
// takes it on. The budget covers the staircase's rounding up to a tick, so
// the clean variants keep a margin where the violating ones have none.

/// The owner (root, node 0) records and outwaits grants on its wait clock:
/// with every oscillator within a quarter of its rate and no budget, a fast
/// owner ends its outwait of a locker it cannot reach `(8 + 1) / 1.25 = 7.2`
/// ticks after the grant, while the locker honours its grant (margin 0) for 8:
/// the next node is granted the lock under the first one's I/O. One
/// message may be lost.
fn rated_owner(budget: i16) -> LockModel {
    LockModel {
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![1, 0, 0]),
        rate_budget: budget,
        clock_split: true,
        seq_margin: 1,
        node_margin: 0,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl: 8,
        max_drops: 1,
        max_tick: 11,
        max_renews: 0,
        ..LockModel::design(contend())
    }
}

#[test]
fn locks_rate_beyond_the_budget_violates() {
    assert_violates("locks-rate-no-budget", &rated_owner(0));
}

/// The same with the budget: the owner's wait reading runs slow by it.
#[test]
fn locks_rate_within_the_budget_is_clean() {
    assert_clean(
        "locks-rate-within-budget",
        &rated_owner(1),
        &["all_ops_done", "recall_expired"],
    );
}

/// An undisciplined host: its wall follows its oscillator, so nothing
/// catches a slow oscillator up. The locker (node 1) is slow by a quarter,
/// its margin 1 and the owner's 0: with no budget its honour reading
/// under-counts and it honours its grant `(8 - 1) / 0.75 = 9.3` ticks, past the
/// owner's outwait of 8, and writes under the next grant. No takeover, so no
/// compare across nodes.
fn undisciplined_locker(budget: i16) -> LockModel {
    LockModel {
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![0, -1, 0]),
        rate_budget: budget,
        clock_split: true,
        wall_follows_rate: true,
        seq_margin: 0,
        node_margin: 1,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl: 8,
        max_drops: 1,
        max_tick: 11,
        max_renews: 0,
        ..LockModel::design(contend())
    }
}

#[test]
fn locks_holder_rate_on_an_undisciplined_host_beyond_the_budget_violates() {
    assert_violates("locks-undisciplined-no-budget", &undisciplined_locker(0));
}

/// With the budget the honour reading runs fast by it: it never
/// under-counts, and the window ends in time.
#[test]
fn locks_holder_rate_on_an_undisciplined_host_within_the_budget_is_clean() {
    assert_clean(
        "locks-undisciplined-within-budget",
        &undisciplined_locker(1),
        &["all_ops_done", "recall_expired"],
    );
}

/// How many ticks before the old root stops using its lease the backup's
/// grace ends, when the root (node 0, slow oscillator) beat to the backup
/// (node 2, fast) at t = 0 and renewed its lease at tick `t_r`, and the
/// backup took the lease over two ticks before the root's own window ended.
/// Positive: the cap ended the grace inside the root's window (see
/// `cto.rs`'s `floor_shortfall`). `None` if the trace does not exist.
fn grace_shortfall(m: &LockModel, t_r: u8) -> Option<i16> {
    fn enabled(m: &LockModel, s: &State, a: &Action) -> bool {
        let mut e = Vec::new();
        m.actions(s, &mut e);
        e.contains(a)
    }
    fn to_tick(m: &LockModel, mut s: State, t: u8) -> Option<State> {
        while s.t < t {
            if !enabled(m, &s, &Action::Tick) {
                return None;
            }
            s = step(m, &s, Action::Tick);
        }
        Some(s)
    }
    let mut s = m.init_states().remove(0);
    s = step(m, &s, Action::Beat(0));
    s = to_tick(m, s, t_r)?;
    if !enabled(m, &s, &Action::RenewLease(0)) {
        return None;
    }
    s = step(m, &s, Action::RenewLease(0));
    // The root stops using its lease when `RenewLease` is no longer offered.
    let mut probe = s.clone();
    while enabled(m, &probe, &Action::RenewLease(0)) {
        probe = step(m, &probe, Action::Tick);
    }
    let root_end = probe.t;
    s = to_tick(m, s, root_end.saturating_sub(2))?;
    s = step(m, &s, Action::Takeover(2));
    s.nodes[2].floor?;
    while !enabled(m, &s, &Action::FloorPassed(2)) {
        if !enabled(m, &s, &Action::Tick) {
            return None;
        }
        s = step(m, &s, Action::Tick);
    }
    Some(root_end as i16 - s.t as i16)
}

fn capped_by_beats(span: Option<i16>) -> LockModel {
    LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        beat_cap: true,
        allow_renew: true,
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![-1, 0, 1]),
        rate_budget: 1,
        clock_split: true,
        beat_span: span,
        lease_ttl: 10,
        lock_ttl: 4,
        takeover_window: 2,
        max_drops: 0,
        max_tick: 40,
        // The trace's root beats and renews but grants nothing before the
        // takeover: it stands for a root that granted (the grace's case,
        // the model before the mark). Unmarked, the takeover arms no grace
        // and the trace has nothing to measure.
        marked_start: true,
        ..LockModel::design(two_lockers())
    }
}

/// As `cto.rs`'s pair: a beat sent before the renewal that set `X` measures an
/// interval the root's slow wait clock under-counts; with no span the cap
/// (the least lag of all the beats) ends the grace before the root's renewed
/// window ends.
#[test]
fn beat_cap_rate_before_the_renewal_violates_without_the_span() {
    let m = capped_by_beats(None);
    let worst = (2..=9u8)
        .filter_map(|t_r| grace_shortfall(&m, t_r))
        .max()
        .expect("a trace");
    println!("beat cap, no span: the grace ends up to {worst} tick(s) before the root's window");
    assert!(
        worst > 0,
        "the cap never ended the grace inside the root's window (worst {worst})"
    );
}

/// With the span the early beats are left out: the grace is the bound.
#[test]
fn beat_cap_rate_with_the_span_is_clean() {
    let m = capped_by_beats(Some(1));
    let worst = (2..=9u8)
        .filter_map(|t_r| grace_shortfall(&m, t_r))
        .max()
        .expect("a trace");
    assert!(
        worst <= 0,
        "the grace ended {worst} tick(s) inside the root's window"
    );
}

/// One step of up to 3 ticks of one node's wall clock, the design's margins
/// of 1 and no takeover (no wall compare across nodes).
fn stepped(split: bool, node: u8, dir: i8) -> LockModel {
    LockModel {
        clock_split: split,
        max_offset: 3,
        max_jumps: 1,
        step_node: Some(node),
        step_dir: dir,
        seq_margin: 1,
        node_margin: 1,
        lease_margin: 3,
        lease_ttl: 30,
        lock_ttl: 4,
        max_drops: 1,
        max_tick: 9,
        max_renews: 0,
        ..LockModel::design(contend())
    }
}

/// The locker (node 1) stepped back: on the wall clock it honours its grant 3
/// ticks past the owner's outwait and writes under the next grant (the model
/// of `locks-locker-step-back`).
#[test]
fn a_locker_stepped_back_on_the_wall_clock_violates() {
    assert_violates("locker-step-back-wall", &stepped(false, 1, -1));
}

/// On its honour clock a backward step moves nothing: clean.
#[test]
fn a_locker_stepped_back_on_the_split_clocks_is_clean() {
    assert_clean(
        "locker-step-back-split",
        &stepped(true, 1, -1),
        &["all_ops_done", "recall_expired"],
    );
}

/// The root (node 0) stepped forward: on the wall clock it ends its outwait of
/// a grant 3 ticks early and grants the next node under the locker's I/O
/// (the model of `locks-root-step-forward`).
#[test]
fn a_root_stepped_forward_on_the_wall_clock_violates() {
    assert_violates("root-step-forward-wall", &stepped(false, 0, 1));
}

/// On its wait clock no step moves anything: clean.
#[test]
fn a_root_stepped_forward_on_the_split_clocks_is_clean() {
    assert_clean(
        "root-step-forward-split",
        &stepped(true, 0, 1),
        &["all_ops_done", "recall_expired"],
    );
}

// ---- plan 43 C7b: the claim rule by observation ----

/// A TTL takeover judged by a claimant 3 ticks ahead of the old root (a
/// whole lease): it reads the live lease as expired and becomes the root
/// while the old root's grants are honoured (a grant over a held lock).
fn ttl_claim(rule: constellation_model::clk::ClaimRule, offsets: Vec<i16>) -> LockModel {
    LockModel {
        ttl_takeover: true,
        claim_rule: rule,
        clock_split: true,
        init_offsets: Some(offsets),
        lease_ttl: 3,
        lock_ttl: 6,
        max_drops: 0,
        max_tick: 7,
        max_renews: 0,
        ..LockModel::design(contend())
    }
}

// ------------------------------------------- plan 41 L2: the renewal lane

/// A lane whose root core is stuck: the lane renews the root's own grant
/// from the certificate its core published, while node 2 (the backup) takes
/// the lease over before it expires. The design's lane stops when the
/// tenure's freshness (a probe within `takeover_window`) ends; with
/// `lane_ignores_freshness` it renews through the successor's grace.
fn lane_fast_takeover(ignores_freshness: bool, max_tick: u8, max_renews: u8) -> LockModel {
    LockModel {
        backup: 1,
        replicate: false,
        grace: true,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick,
        max_renews,
        // (The lane renews with the core running too: no stall is needed
        // for the counterexample, and one would multiply the space; the cap
        // configuration below has them.)
        max_stalls: 0,
        lane: true,
        lane_ignores_freshness: ignores_freshness,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X)]])
    }
}

#[test]
fn a_claimant_ahead_beyond_the_margin_violates() {
    assert_violates(
        "ttl-claimant-ahead-wall",
        &ttl_claim(constellation_model::clk::ClaimRule::Wall, vec![0, 3, 3]),
    );
}

/// The observation rule with the same offsets (and the holder behind, a
/// node far ahead and one far behind): clean, the takeover witnessed.
#[test]
fn the_observed_claim_is_clean_with_offsets_up_to_a_ttl() {
    for (label, offsets) in [
        ("ahead", vec![0, 3, 3]),
        ("behind", vec![-3, 0, 0]),
        ("spread", vec![0, 5, -5]),
    ] {
        assert_clean(
            &format!("ttl-claimant-observed-{label}"),
            &ttl_claim(constellation_model::clk::ClaimRule::Observed, offsets),
            &["all_ops_done", "ttl_takeover"],
        );
    }
}

/// The same with oscillators within the rate budget.
#[test]
fn the_observed_claim_is_clean_with_offsets_and_rates_within_the_budget() {
    let m = LockModel {
        rate_period: 4,
        max_rate: 1,
        init_rates: Some(vec![1, -1, 0]),
        rate_budget: 1,
        lease_ttl: 4,
        max_tick: 9,
        ..ttl_claim(
            constellation_model::clk::ClaimRule::Observed,
            vec![0, 4, -4],
        )
    };
    assert_clean("ttl-claimant-observed-rates", &m, &["ttl_takeover"]);
}

#[test]
fn a_lane_ignoring_freshness_renews_through_a_fast_takeovers_grace() {
    assert_violates_bfs("lane-ignores-freshness", &lane_fast_takeover(true, 6, 5));
}

#[test]
fn design_a_lane_on_a_fast_tenure_is_clean() {
    assert_clean(
        "lane-fast",
        &lane_fast_takeover(false, 6, 5),
        &["fast_takeover"],
    );
}

/// The lane past the lease's usable end: a TTL takeover (an empty table, no
/// grace) grants at once, and the stuck root's lane extended its own grant
/// beyond the lease.
fn lane_ttl_takeover(ignores_cap: bool) -> LockModel {
    LockModel {
        ttl_takeover: true,
        lease_ttl: 5,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 6,
        max_renews: 4,
        max_stalls: 1,
        lane: true,
        lane_ignores_cap: ignores_cap,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X)], vec![]])
    }
}

#[test]
fn a_lane_ignoring_the_cap_renews_past_the_lease_across_a_ttl_takeover() {
    assert_violates_soon("lane-ignores-cap", &lane_ttl_takeover(true));
}

#[test]
fn design_a_lane_under_the_cap_is_clean_across_a_ttl_takeover() {
    assert_clean(
        "lane-cap",
        &lane_ttl_takeover(false),
        &["lane_renewed_while_stalled", "ttl_takeover"],
    );
}

/// A released takeover (a graceful shutdown with grants live): the old
/// root's lane after its release. The model's `Release` is atomic and drops
/// the owner's table, so a lane that merely kept its certificate has no
/// record to extend (the code's gap between deciding a release and the CAS
/// landing is the certificate's, revoked by the step that decides it); with
/// the records revived as well (`lane_extends_expired_records`) the kept
/// certificate renews the old grant through the successor's grace.
fn lane_released_takeover(keeps_cert: bool, revives: bool) -> LockModel {
    LockModel {
        released_takeover: true,
        lease_ttl: 30,
        lock_ttl: 3,
        max_drops: 0,
        max_tick: 6,
        max_renews: 5,
        max_stalls: 0,
        lane: true,
        lane_keeps_cert_after_release: keeps_cert,
        lane_extends_expired_records: revives,
        ..LockModel::design(vec![vec![Lock(X), Io], vec![Lock(X)]])
    }
}

#[test]
fn a_lane_that_keeps_its_certificate_and_revives_records_renews_through_the_successors_grace() {
    assert_violates_bfs("lane-keeps-cert", &lane_released_takeover(true, true));
}

#[test]
fn design_a_lane_after_a_release_is_clean() {
    assert_clean(
        "lane-release",
        &lane_released_takeover(false, false),
        &["released_takeover"],
    );
}
