//! Plan 30 §M6: reads, the session properties `read_your_writes` and
//! `monotonic_reads`, and the `Positions` variant that satisfies them
//! (see `constellation_model::positions`).
//!
//! Like `today_bugs.rs` keeps bug A and bug B, this file keeps the
//! counterexamples: `Recovery` with reads (today's system: replies carry
//! no position, reads never wait) violates both properties, and so does
//! `Positions` with either of its two rules switched off (the read wait,
//! the reply base). Then the same configurations, and a few with crashes,
//! a handoff and the inbox, are clean under `Positions`.
//!
//! Every search carries an explicit cap. A counterexample test asserts
//! both that the checker *finds* one and that a hand-built path (one
//! deterministic trace) is one, so a regression shows up as a precise
//! assertion. Each test prints its state counts and time.

use constellation_model::protocol::{Action, AuthorityModel, HistEvt, Phase, Protocol, State};
use constellation_model::{NsOp, NsRet, N_NAMES};
use stateright::{Checker, Model};
use std::time::{Duration, Instant};

fn name(n: u8) -> u8 {
    assert!(n < N_NAMES);
    n
}

const EXHAUSTIVE_CAP: (usize, usize, Duration) = (60_000_000, 100, Duration::from_secs(180));

/// The three-node stranded-shadow configuration does not finish
/// exhaustively (40M states, depth 19, 1.6 GB before the cap in the
/// `#[ignore]`d deep run): a bounded search, sized to stay well under
/// 1 GB, backed by explicit paths.
const STRANDED_CAP: (usize, usize, Duration) = (3_000_000, 30, Duration::from_secs(50));

const SAFETY: [&str; 6] = [
    "linearizable",
    "converged_at_quiescence",
    "commits_are_log_prefixes",
    "no_rid_executes_twice",
    "read_your_writes",
    "monotonic_reads",
];

fn run(
    label: &str,
    model: &AuthorityModel,
    cap: (usize, usize, Duration),
) -> impl Checker<AuthorityModel> {
    let started = Instant::now();
    let checker = model
        .clone()
        .checker()
        .target_state_count(cap.0)
        .target_max_depth(cap.1)
        .timeout(cap.2)
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

/// No safety property (session properties included) violated in the
/// explored region, and `progress` witnessed. With `full`, the search
/// must also have finished.
fn assert_clean(label: &str, model: &AuthorityModel, cap: (usize, usize, Duration), full: bool) {
    let checker = run(label, model, cap);
    if full {
        assert!(
            checker.is_done(),
            "expected exhaustive exploration within budget ({label})"
        );
    } else {
        println!("{label}: bounded, non-exhaustive exploration (capped at the numbers above)");
    }
    for prop in SAFETY {
        assert!(
            checker.discovery(prop).is_none(),
            "{label} must not violate {prop}: {:?}",
            checker.discovery(prop).map(|p| p.into_actions())
        );
    }
    assert!(
        checker.discovery("progress").is_some(),
        "expected at least one run where every op completes ({label})"
    );
}

/// The checker finds a counterexample to `prop` (printed), and does not
/// find one to any safety property other than the session ones.
fn assert_finds(
    label: &str,
    model: &AuthorityModel,
    cap: (usize, usize, Duration),
    prop: &'static str,
) -> Vec<Action> {
    let checker = run(label, model, cap);
    let path = checker
        .discovery(prop)
        .unwrap_or_else(|| panic!("expected a {prop} counterexample ({label})"));
    let mut actions = path.into_actions();
    // A discovery may be reported at a successor of the violating state
    // (the violation flags are sticky); print the path up to the read
    // that violated.
    if let Some(k) = (1..=actions.len()).find(|k| !holds(model, prop, &walk(model, &actions[..*k])))
    {
        actions.truncate(k);
    }
    println!(
        "{label}: {prop} counterexample ({} actions):",
        actions.len()
    );
    for a in &actions {
        println!("  {a:?}");
    }
    for other in &SAFETY[..4] {
        assert!(
            checker.discovery(other).is_none(),
            "{label}: only the session properties may fail, but {other} did: {:?}",
            checker.discovery(other).map(|p| p.into_actions())
        );
    }
    actions
}

/// Replay `actions` from the model's single initial state; panics naming
/// the first step that is not enabled and what was enabled instead.
fn walk(model: &AuthorityModel, actions: &[Action]) -> State {
    let mut state = model
        .init_states()
        .into_iter()
        .next()
        .expect("one initial state");
    for (i, want) in actions.iter().enumerate() {
        let steps = model.next_steps(&state);
        let enabled: Vec<Action> = steps.iter().map(|(a, _)| *a).collect();
        let Some((_, next)) = steps.into_iter().find(|(a, _)| a == want) else {
            panic!("step {i} ({want:?}) not enabled; enabled: {enabled:?}; path: {actions:?}");
        };
        state = next;
    }
    state
}

fn enabled(model: &AuthorityModel, s: &State) -> Vec<Action> {
    model.next_steps(s).into_iter().map(|(a, _)| a).collect()
}

fn holds(model: &AuthorityModel, name: &'static str, s: &State) -> bool {
    (model.property(name).condition)(model, s)
}

/// `actions` is a counterexample to `prop`: enabled from the initial
/// state, and `prop` fails at its end (and not one step earlier: the
/// violating step is the read the path ends with).
fn assert_counterexample(model: &AuthorityModel, prop: &'static str, actions: &[Action]) {
    let before = walk(model, &actions[..actions.len() - 1]);
    let end = walk(model, actions);
    assert!(
        holds(model, prop, &before) && !holds(model, prop, &end),
        "{prop} should fail exactly at the path's last step: {actions:?}"
    );
}

// ----------------------------------------------------- configurations

/// Two nodes; node 0 holds for the whole run (it never expires: `Tick`
/// is disabled) and creates name 0 locally; node 1 tries to create the
/// same name through node 0 and then looks it up. The refusal is an
/// observation of node 0's (possibly still unshipped) create: plan 29
/// M6's `Exists` case, which production point-fixes with a causal wait
/// and M6 makes an instance of the general rule.
fn refusal_config(p: Protocol) -> AuthorityModel {
    AuthorityModel::new(2)
        .with_protocol(p)
        .with_forward_retries(0)
        .with_initial_holder(0, 255)
        .with_max_tick(0)
        .with_max_seq(2)
        .with_lossy(false)
        .with_max_next_id(12)
        .with_op(0, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_lookup(1, name(0))
}

/// Two nodes, same holder; node 0 creates name 0, node 1 unlinks it
/// through node 0 and looks it up, then lists the directory. An unlink
/// evaluated after node 0's still-unshipped create is accepted on a base
/// node 1 has not applied: M5's stale-base speculation (the model's
/// namespace keeps the *value* right; the read's write set is not).
fn stale_base_config(p: Protocol) -> AuthorityModel {
    AuthorityModel::new(2)
        .with_protocol(p)
        .with_forward_retries(0)
        .with_initial_holder(0, 255)
        .with_max_tick(0)
        .with_max_seq(2)
        .with_lossy(false)
        .with_max_next_id(12)
        .with_op(0, NsOp::CreateExcl(name(0)))
        .with_op(1, NsOp::Unlink(name(0)))
        .with_lookup(1, name(0))
        .with_readdir(1)
}

/// Three nodes, no crash: node 0 holds until its lease expires; node 1
/// creates name 0 through it and looks it up; node 2 creates name 1 and
/// has to take over to do so (after the expiry). Node 1 tails node 2's
/// takeover epoch marker, which strands its shadow: its own acknowledged
/// create is rolled back until the replay by rid lands.
fn stranded_config(p: Protocol) -> AuthorityModel {
    AuthorityModel::new(3)
        .with_protocol(p)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(1)
        .with_lease_ttl(1)
        .with_max_seq(3)
        .with_lossy(false)
        .with_max_next_id(8)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_lookup(1, name(0))
        .with_op(2, NsOp::CreateExcl(name(1)))
}

/// The refusal path: node 0 journals its create, node 1's forward is
/// refused with `EEXIST` (message 0 is the request, 1 the reply), and
/// node 1 looks the name up before node 0 has shipped.
fn refusal_path() -> Vec<Action> {
    vec![
        Action::ClientInvoke(0),
        Action::ClientInvoke(1),
        Action::DeliverForwardRequest(0),
        Action::DeliverForwardReply(1),
        Action::Read(1),
    ]
}

/// The stale-base path: node 0 journals its create, node 1's unlink is
/// accepted on top of it, node 1 looks the name up before node 0 ships.
fn stale_base_path() -> Vec<Action> {
    refusal_path()
}

/// The stranding path: node 1's create is accepted by node 0 (a shadow),
/// node 0's lease expires, node 2 forwards to it (message 2), times out,
/// takes over (epoch marker in slot 1), and node 1 tails the marker.
fn stranded_prefix() -> Vec<Action> {
    vec![
        Action::ClientInvoke(1),
        Action::DeliverForwardRequest(0),
        Action::DeliverForwardReply(1),
        Action::Tick,
        Action::ClientInvoke(2),
        Action::ForwardTimeout(2),
        Action::AcquireLease(2),
        Action::Tail(1),
    ]
}

// ----------------------------------------------------- counterexamples

/// Today's system (`Recovery`, reads never wait, replies carry no
/// position): the refusal is forgotten by the next read.
#[test]
fn recovery_reads_violate_monotonic_reads() {
    let model = refusal_config(Protocol::Recovery);
    let actions = assert_finds(
        "recovery reads (refusal)",
        &model,
        EXHAUSTIVE_CAP,
        "monotonic_reads",
    );
    assert!(
        actions.iter().any(|a| matches!(a, Action::Read(1))),
        "the counterexample ends in node 1's read"
    );
    let path = refusal_path();
    assert_counterexample(&model, "monotonic_reads", &path);
    let end = walk(&model, &path);
    assert!(
        end.history
            .iter()
            .any(|e| matches!(e, HistEvt::Return(1, NsRet::Err(_), _))),
        "node 1's create was refused before its read: {:?}",
        end.history
    );
}

/// Today's system: an acknowledged write is rolled back from its own
/// node's replica when a takeover strands its shadow, and the next read
/// misses it until the replay lands.
#[test]
fn recovery_reads_violate_read_your_writes() {
    let model = stranded_config(Protocol::Recovery);
    let actions = assert_finds(
        "recovery reads (stranded shadow)",
        &model,
        STRANDED_CAP,
        "read_your_writes",
    );
    assert!(
        actions.iter().any(|a| matches!(a, Action::AcquireLease(2))),
        "the counterexample goes through node 2's takeover"
    );
    let mut path = stranded_prefix();
    path.push(Action::Read(1));
    assert_counterexample(&model, "read_your_writes", &path);
    let end = walk(&model, &path);
    assert!(
        end.nodes[1].shadows.is_empty() && end.nodes[1].replays.len() == 1,
        "node 1's shadow was stranded and queued for replay: {:?}",
        end.nodes[1]
    );
}

/// `Positions` with the read wait switched off: positions alone do not
/// help; both counterexamples come back.
#[test]
fn positions_without_the_wait_violates_both() {
    let model = refusal_config(Protocol::Positions).with_session_wait(false);
    assert_finds(
        "positions, no wait (refusal)",
        &model,
        EXHAUSTIVE_CAP,
        "monotonic_reads",
    );
    assert_counterexample(&model, "monotonic_reads", &refusal_path());

    let model = stranded_config(Protocol::Positions).with_session_wait(false);
    let mut path = stranded_prefix();
    path.push(Action::Read(1));
    assert_counterexample(&model, "read_your_writes", &path);
    assert_finds(
        "positions, no wait (stranded shadow)",
        &model,
        STRANDED_CAP,
        "read_your_writes",
    );
}

/// `Positions` with the wait but without the reply base (M3a's install
/// rule, M5's `speculate_on_stale_base`): the shadow of an unlink
/// evaluated after an unshipped create covers the name, so the read runs
/// at once, and its write set lacks the create the reply observed.
#[test]
fn positions_with_stale_base_shadows_violates_monotonic_reads() {
    let model = stale_base_config(Protocol::Positions).with_stale_base_shadows(true);
    assert_finds(
        "positions, stale-base shadows",
        &model,
        EXHAUSTIVE_CAP,
        "monotonic_reads",
    );
    let path = stale_base_path();
    assert_counterexample(&model, "monotonic_reads", &path);
    let end = walk(&model, &path);
    assert_eq!(
        end.nodes[1].shadows.len(),
        1,
        "the unlink was installed as a shadow on the stale base"
    );
}

// ----------------------------------------------------- Positions holds

/// The refusal configuration is clean under `Positions`, exhaustively,
/// and the refusal path now makes the read wait for node 0's segment.
#[test]
fn positions_refusal_waits_for_the_observed_position() {
    let model = refusal_config(Protocol::Positions);
    assert_clean("positions (refusal)", &model, EXHAUSTIVE_CAP, true);

    let path = refusal_path();
    let at = walk(&model, &path[..path.len() - 1]);
    assert!(
        !enabled(&model, &at).contains(&Action::Read(1)),
        "the read waits while node 0's create is unshipped"
    );
    let mut path = path[..path.len() - 1].to_vec();
    path.extend([Action::Ship(0), Action::Tail(1), Action::Read(1)]);
    let end = walk(&model, &path);
    assert!(holds(&model, "monotonic_reads", &end));
}

/// The stale-base configuration (with a readdir) is clean under
/// `Positions`, exhaustively; on the stale-base path the unlink is not
/// installed but answered once the log carries it.
#[test]
fn positions_stale_base_waits_for_the_log() {
    let model = stale_base_config(Protocol::Positions);
    assert_clean("positions (stale base)", &model, EXHAUSTIVE_CAP, true);

    let path = stale_base_path();
    let at = walk(&model, &path[..path.len() - 1]);
    assert!(
        at.nodes[1].shadows.is_empty()
            && matches!(
                at.nodes[1].client_op.as_ref().map(|c| &c.phase),
                Some(Phase::AwaitingLog { .. })
            ),
        "the accepted unlink awaits the log instead of speculating: {:?}",
        at.nodes[1]
    );
    let mut path = path[..path.len() - 1].to_vec();
    path.extend([Action::Ship(0), Action::Tail(1), Action::Read(1)]);
    let end = walk(&model, &path);
    assert!(holds(&model, "monotonic_reads", &end) && holds(&model, "read_your_writes", &end));
}

/// The stranded-shadow configuration under `Positions`. Three nodes with
/// a takeover: bounded, not exhaustive (like `recovery_fixes_bug_b_
/// third_node_takeover`), backed by the explicit path: after the
/// stranding the read waits for the replay; the replay goes to node 2,
/// is accepted on a base node 1 has applied, and reinstalls the shadow,
/// after which the read sees the create.
#[test]
fn positions_stranded_shadow_read_waits_for_the_replay() {
    let model = stranded_config(Protocol::Positions);
    assert_clean("positions (stranded shadow)", &model, STRANDED_CAP, false);

    let path = stranded_prefix();
    let at = walk(&model, &path);
    assert!(
        !enabled(&model, &at).contains(&Action::Read(1)),
        "the read waits while the stranded create is queued for replay"
    );
    let mut path = path;
    // Message ids: 0/1 node 1's request/reply, 2 node 2's forward to the
    // expired holder (never delivered), 3/4 the replay and its answer.
    path.extend([
        Action::ReplayStranded(1),
        Action::DeliverForwardRequest(3),
        Action::DeliverForwardReply(4),
        Action::Read(1),
    ]);
    let end = walk(&model, &path);
    assert_eq!(end.nodes[1].shadows.len(), 1, "the replay reinstalled it");
    assert!(holds(&model, "read_your_writes", &end) && holds(&model, "monotonic_reads", &end));
}

/// Deeper (still bounded) exploration of the stranded-shadow config.
/// `#[ignore]`d: `cargo test -p constellation-model --release --test
/// positions -- --ignored --nocapture`.
#[test]
#[ignore]
fn positions_stranded_shadow_deep() {
    let model = stranded_config(Protocol::Positions);
    assert_clean(
        "positions (stranded shadow, deep)",
        &model,
        (40_000_000, 60, Duration::from_secs(600)),
        false,
    );
}

/// Bug B's requester-takeover shape (`recovery_fixes_bug_b_requester_
/// takeover`) with reads: node 1's create is accepted by node 0, node 0
/// may crash, node 1 unlinks the name (possibly after taking over) and
/// reads the directory. Exhaustive.
#[test]
fn positions_holds_across_a_crash_and_takeover() {
    let model = AuthorityModel::new(2)
        .with_protocol(Protocol::Positions)
        .with_forward_retries(0)
        .with_initial_holder(0, 1)
        .with_max_tick(3)
        .with_lease_ttl(1)
        .with_max_seq(3)
        .with_max_crashes(1)
        .with_lossy(false)
        .with_op(1, NsOp::CreateExcl(name(0)))
        .with_lookup(1, name(0))
        .with_op(1, NsOp::Unlink(name(0)))
        .with_readdir(1);
    assert_clean("positions (crash + takeover)", &model, EXHAUSTIVE_CAP, true);
}

/// Plan 30 §M13's inbox (no P2P): outcomes ride the log, so reads need
/// no watermark from it, and the session properties hold even under
/// `Recovery` — the requester has applied everything the holder
/// evaluated against by the time its op returns. Exhaustive.
#[test]
fn inbox_outcomes_keep_sessions_without_a_watermark() {
    for p in [Protocol::Recovery, Protocol::Positions] {
        let model = AuthorityModel::new(2)
            .with_protocol(p)
            .with_inbox(true)
            .with_forward_retries(0)
            .with_lossy(false)
            .with_initial_holder(0, 255)
            .with_max_tick(0)
            .with_max_seq(3)
            .with_op(0, NsOp::CreateExcl(name(0)))
            .with_op(1, NsOp::CreateExcl(name(0)))
            .with_lookup(1, name(0))
            .with_readdir(1);
        assert_clean(&format!("inbox ({p:?})"), &model, EXHAUSTIVE_CAP, true);
    }
}
