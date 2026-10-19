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

use constellation_model::locks::{Action, LockModel, Mode, Msg, Op, State, NONE};
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
/// grants itself after the successor's grace, which grants node 1.
#[test]
fn fast_takeover_without_the_probe_violates() {
    let m = LockModel {
        backup: 2,
        replicate: false,
        grace: true,
        probe_freshness: false,
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

#[test]
fn none_is_no_node() {
    assert_eq!(NONE, u8::MAX);
}
