//! The deterministic simulation (plan 30 M5): N real `Core`s over real
//! `Meta`, `LogStore`, `LeaseStore` and `CommitChain`, on a simulated
//! bucket and bus with seeded latency, drops, partitions, crashes, pauses
//! and scripted S3 error codes; seeded workloads; histories checked with
//! Stateright's `LinearizabilityTester`, plus convergence at quiescence,
//! commit-is-log-prefix and exactly-once over the log.
//!
//! A failing seed prints its replay command:
//! `AUTHORITY_SIM_SEED=<seed> cargo test -p constellation-authority --test sim replay_seed -- --nocapture --exact`.

mod sim {
    pub mod bus;
    pub mod check;
    pub mod clock;
    pub mod cto;
    pub mod history;
    pub mod node;
    pub mod run;
    pub mod session;
    pub mod store;
}

use sim::bus::StreamFaults;
use sim::run::{run_seed, FaultKind, Report, ScheduledFault, SimConfig};
use sim::store::{Fault, OpKind, Rule, When};

/// Seeds per CI shard; four shards run in parallel under `cargo test`.
const CI_SEEDS_PER_SHARD: u64 = 250;

/// Plan 30 §M7: the log-stream counters summed over a run's nodes.
#[derive(Debug, Default, Clone, Copy)]
struct StreamTotals {
    applied: u64,
    segments_applied: u64,
    tail_skips: u64,
    subscribes: u64,
    gaps: u64,
    lost: u64,
    timeouts: u64,
    ended: u64,
    refused: u64,
    served: u64,
    dropped_subscribers: u64,
    s3_gets: u64,
}

impl StreamTotals {
    fn add(&mut self, r: &Report) {
        for s in r.stats.values() {
            self.applied += s.stream_applied;
            self.segments_applied += s.segments_applied;
            self.tail_skips += s.stream_tail_skips;
            self.subscribes += s.stream_subscribes;
            self.gaps += s.stream_gaps;
            self.lost += s.stream_lost;
            self.timeouts += s.stream_timeouts;
            self.ended += s.stream_ended;
            self.refused += s.stream_refused;
            self.served += s.stream_served;
            self.dropped_subscribers += s.stream_subscribers_dropped;
        }
        self.s3_gets += r.s3_gets;
    }
}

fn run_shard(shard: u64) {
    let mut failures = Vec::new();
    let mut summary = (0usize, 0usize, 0usize, 0usize);
    let mut streams = StreamTotals::default();
    for seed in (shard * CI_SEEDS_PER_SHARD)..((shard + 1) * CI_SEEDS_PER_SHARD) {
        // Plan 30 §M6: the CI seeds read too (session checks enforced).
        let cfg = SimConfig {
            read_ratio: 0.3,
            ..SimConfig::default()
        };
        match run_seed(seed, cfg) {
            Ok(report) => {
                assert_eq!(report.seed, seed);
                summary.0 += report.ops_returned;
                summary.1 += report.segments;
                summary.2 += usize::from(report.converged_checked);
                summary.3 += usize::from(report.stateright_checked);
                streams.add(&report);
            }
            Err(e) => failures.push(format!("seed {seed}: {e}")),
        }
    }
    eprintln!(
        "shard {shard}: {} seeds, {} ops returned, {} segments, {} converged-checked runs, \
         {} also checked with Stateright's tester; streams: {streams:?}",
        CI_SEEDS_PER_SHARD, summary.0, summary.1, summary.2, summary.3
    );
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn seeds_shard_0() {
    run_shard(0);
}

#[test]
fn seeds_shard_1() {
    run_shard(1);
}

#[test]
fn seeds_shard_2() {
    run_shard(2);
}

#[test]
fn seeds_shard_3() {
    run_shard(3);
}

/// The scripted S3 rules `regression_scripted_s3_error_codes` runs, by
/// index (`AUTHORITY_SIM_CONFIG=s3:<i>` replays one).
fn s3_rules() -> Vec<(OpKind, &'static str, When, Fault)> {
    vec![
        (
            OpKind::Put,
            "log/",
            When::Random(0.15),
            Fault::AppliedThen(500),
        ),
        (
            OpKind::Put,
            "log/",
            When::Random(0.15),
            Fault::AppliedThenTimeout,
        ),
        (
            OpKind::Put,
            "leases/",
            When::Random(0.15),
            Fault::AppliedThenTimeout,
        ),
        (
            OpKind::Put,
            "leases/",
            When::Random(0.15),
            Fault::Status(500),
        ),
        (OpKind::Get, "log/", When::Random(0.15), Fault::Timeout),
        (OpKind::Put, "log/", When::Random(0.15), Fault::Status(409)),
        // The very first lease CAS lands but its reply is lost: the node
        // does not know it holds, and re-adopts its own live lease through
        // the gate (M4's `readopts`).
        (
            OpKind::Put,
            "leases/",
            When::Nth(1),
            Fault::AppliedThenTimeout,
        ),
    ]
}

fn s3_rule_config(i: usize) -> SimConfig {
    let (op, pattern, when, fault) = s3_rules()[i];
    SimConfig {
        random_faults: 0,
        faults: vec![ScheduledFault {
            at_ms: 100,
            kind: FaultKind::S3Rule(Rule::new(None, op, pattern, when, fault)),
        }],
        ..SimConfig::default()
    }
}

fn bug_a_config() -> SimConfig {
    SimConfig {
        nodes: 2,
        clients_per_node: 2,
        ops_per_client: 8,
        names: 3,
        random_faults: 0,
        faults: vec![
            ScheduledFault {
                at_ms: 300,
                kind: FaultKind::SlowReplies {
                    delay_ms: 900,
                    for_ms: 6_000,
                },
            },
            ScheduledFault {
                at_ms: 4_000,
                kind: FaultKind::SlowReplies {
                    delay_ms: 1_500,
                    for_ms: 4_000,
                },
            },
        ],
        ..SimConfig::default()
    }
}

fn bug_b_config() -> SimConfig {
    SimConfig {
        nodes: 3,
        clients_per_node: 2,
        ops_per_client: 8,
        names: 3,
        random_faults: 0,
        // Plan 30 §M6: slower S3 widens the window in which the holder
        // has acknowledged forwards it has not shipped yet. (With M6's
        // exact hint floor, hints no longer strand in this config — they
        // used to make up most of the rollbacks this test counted.)
        s3_latency: (40, 160),
        faults: vec![ScheduledFault {
            at_ms: 1_200,
            kind: FaultKind::CrashHolder {
                restart_ms: Some(9_000),
                keep_journal: true,
            },
        }],
        ..SimConfig::default()
    }
}

/// Replay one seed with `AUTHORITY_SIM_SEED` (and `AUTHORITY_SIM_CONFIG`
/// = `default` | `buga` | `bugb` | `s3:<i>` | `single` | `long` | `inbox` |
/// `long-sessions` | `sessions` | `sessions-inbox` | `plain` (the default
/// without reads)); a
/// no-op without it.
#[test]
fn replay_seed() {
    let Some(seed) = std::env::var("AUTHORITY_SIM_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return;
    };
    let config = match std::env::var("AUTHORITY_SIM_CONFIG").as_deref() {
        Ok("buga") => bug_a_config(),
        Ok("bugb") => bug_b_config(),
        Ok(s) if s.starts_with("s3:") => s3_rule_config(s[3..].parse().expect("rule index")),
        Ok("single") => SimConfig {
            nodes: 1,
            clients_per_node: 2,
            ops_per_client: 10,
            random_faults: 0,
            ..SimConfig::default()
        },
        Ok("long") => long_config(),
        Ok("long-sessions") => long_sessions_config(),
        Ok("inbox") => SimConfig {
            core: std::sync::Arc::new(sim::run::inbox_core_config),
            ..SimConfig::default()
        },
        // Plan 30 §M6's session tests.
        Ok("sessions") => SimConfig {
            read_ratio: 0.7,
            ..SimConfig::default()
        },
        Ok("sessions-inbox") => SimConfig {
            read_ratio: 0.7,
            core: std::sync::Arc::new(sim::run::inbox_core_config),
            ..SimConfig::default()
        },
        Ok("plain") => SimConfig::default(),
        // Plan 30 §M8.
        Ok("strict") => strict_config(),
        Ok("long-strict") => long_strict_config(),
        Ok("strict-skew") => SimConfig {
            clock_skew_ms: 200,
            p2p_drop: 0.03,
            ..strict_config()
        },
        // Plan 30 §M7.
        Ok("streams-off") => streams_off_config(),
        Ok("stream-faults") => stream_faults_config(),
        Ok("stream-gap") => stream_gap_config(),
        // The CI shards' configuration (reads included since M6).
        _ => SimConfig {
            read_ratio: 0.3,
            ..SimConfig::default()
        },
    };
    match run_seed(seed, config) {
        Ok(report) => eprintln!("seed {seed} passed: {report:#?}"),
        Err(e) => panic!("seed {seed} failed: {e}"),
    }
}

/// Determinism: the same seed twice yields the same history and the same
/// per-node counters.
#[test]
fn a_seed_replays_identically() {
    let a = run_seed(7, SimConfig::default()).expect("run a");
    let b = run_seed(7, SimConfig::default()).expect("run b");
    assert_eq!(a.stats, b.stats, "per-node counters differ between replays");
    assert_eq!(a.ops_returned, b.ops_returned);
    assert_eq!(a.segments, b.segments);
}

/// Bug A's shape (plan 30 §1.1): the holder executes a forwarded op but
/// its reply is slower than the forward timeout. The requester retries
/// the same rid (dedup answers it) or falls to the lease path (the
/// coverage rule finds the rid completed). Either way the op takes
/// effect once and the client is never told `EEXIST`/`ENOENT` for its
/// own effect.
#[test]
fn regression_bug_a_slow_holder_replies() {
    let cfg = bug_a_config();
    let mut saw_dedup = false;
    for seed in 100..112 {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let dedup: u64 = report
            .stats
            .values()
            .map(|s| s.forward_dedup_hits + s.forward_indoubt_resolved)
            .sum();
        saw_dedup |= dedup > 0;
    }
    assert!(saw_dedup, "no seed exercised the same-rid dedup path");
}

/// Bug B's shape (plan 30 §1.1): the holder accepts a forward and dies
/// before shipping. Another node takes over; the requester's shadow is
/// stranded by the new epoch's marker, rolled back and replayed by rid;
/// commits are log prefixes; every live replica converges to the log.
#[test]
fn regression_bug_b_holder_dies_with_unshipped_forwards() {
    let cfg = bug_b_config();
    let mut saw_strand = false;
    for seed in 200..212 {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let rolled: u64 = report
            .stats
            .values()
            .map(|s| s.speculation_rolled_back + s.local_rolled_back)
            .sum();
        saw_strand |= rolled > 0;
        assert!(
            report.converged_checked,
            "seed {seed}: convergence not checked"
        );
    }
    assert!(saw_strand, "no seed stranded speculation");
}

/// Scripted S3 error codes on every CAS site (M4's `faulty.rs` rules):
/// lost replies after a landed PUT, 500s and timeouts on the lease and the
/// log. Nothing may double-execute or diverge.
#[test]
fn regression_scripted_s3_error_codes() {
    for (i, (_, _, _, fault)) in s3_rules().iter().enumerate() {
        let cfg = s3_rule_config(i);
        for seed in 0..4 {
            let seed = 300 + i as u64 * 10 + seed;
            run_seed(seed, cfg.clone())
                .unwrap_or_else(|e| panic!("rule {fault:?} seed {seed}: {e}"));
        }
    }
}

/// Plan 30 §M3a's requester speculation as shipped: an accepted forward's
/// records are installed ahead of the log even when the holder evaluated
/// the op on state this replica has not applied yet. The simulation finds
/// the divergence (the core's default refuses such a base and waits for
/// the log instead — `PeerMsg::MutateReply::base`; plan 30 §M6 is where
/// positions on replies land for good). Like `today_finds_bug_a` in the
/// model crate, this asserts the checker *finds* it.
#[test]
fn stale_base_speculation_is_found() {
    let cfg = SimConfig {
        random_faults: 0,
        ops_per_client: 10,
        core: std::sync::Arc::new(|node, inc| {
            let mut c = sim::run::sim_core_config(node, inc);
            c.speculate_on_stale_base = true;
            c
        }),
        ..SimConfig::default()
    };
    let mut found = None;
    for seed in 500..560 {
        if let Err(e) = run_seed(seed, cfg.clone()) {
            found = Some((seed, e));
            break;
        }
    }
    let (seed, e) = found.expect("no seed diverged with stale-base speculation on");
    eprintln!(
        "stale-base speculation found at seed {seed}:\n{}",
        e.lines().next().unwrap_or("")
    );
    assert!(
        e.contains("did not converge")
            || e.contains("not the log prefix")
            || e.contains("linearizable"),
        "unexpected failure shape: {e}"
    );
}

/// Plan 30 §M6: clients read the names they write (and others) from
/// their local replica through the session wait; every run is checked for
/// per-node read-your-writes and monotonic reads against the log's version
/// order (`sim/session.rs`), with the default random faults (crashes,
/// takeovers, partitions, pauses, slow replies, S3 errors). The P2P
/// configuration and the M13 inbox configuration both.
#[test]
fn session_guarantees_hold() {
    for (label, core) in [
        (
            "p2p",
            std::sync::Arc::new(sim::run::sim_core_config)
                as std::sync::Arc<
                    dyn Fn(u64, u32) -> constellation_authority::Config + Send + Sync,
                >,
        ),
        ("inbox", std::sync::Arc::new(sim::run::inbox_core_config)),
    ] {
        let cfg = SimConfig {
            read_ratio: 0.7,
            core,
            ..SimConfig::default()
        };
        let mut totals = (0usize, 0usize, 0usize, 0usize);
        for seed in 600..660 {
            let report =
                run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("{label} seed {seed}: {e}"));
            totals.0 += report.sessions.reads;
            totals.1 += report.sessions.degraded;
            totals.2 += report.sessions.tentative;
            totals.3 += report.sessions.degraded_violations;
        }
        eprintln!(
            "session_guarantees_hold ({label}): {} reads, {} degraded (timed out), {} \
             violations exempt as tentative, {} on degraded reads",
            totals.0, totals.1, totals.2, totals.3
        );
        assert!(totals.0 > 500, "{label}: too few reads to mean anything");
    }
}

/// Plan 30 §M6's non-vacuity seed: the same workload with the session
/// wait off (reads never wait, as before M6) must violate a session
/// guarantee somewhere — the checker finds what the wait prevents.
#[test]
fn session_wait_off_is_found() {
    let cfg = SimConfig {
        read_ratio: 0.9,
        session_wait: false,
        ..SimConfig::default()
    };
    let mut found = None;
    for seed in 600..700 {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        if let Some(v) = report.sessions.violations.first() {
            found = Some((seed, v.clone()));
            break;
        }
    }
    let (seed, (checker, what)) =
        found.expect("no seed violated a session guarantee with the wait off");
    eprintln!("session wait off: seed {seed}: {checker}: {what}");
}

/// Plan 30 §M8: the read-delegation counters summed over a run's nodes.
#[derive(Debug, Default, Clone, Copy)]
struct CtoTotals {
    reads: usize,
    constrained: usize,
    degraded: usize,
    tentative: usize,
    stale: usize,
    grants: u64,
    recalls_sent: u64,
    recalls_acked: u64,
    recalls_expired: u64,
    recall_waits: u64,
    held_replies: u64,
    read_index_sent: u64,
    read_index_degraded: u64,
    read_index_tailed: u64,
}

impl CtoTotals {
    fn add(&mut self, r: &Report) {
        self.reads += r.cto.reads;
        self.constrained += r.cto.constrained;
        self.degraded += r.cto.degraded;
        self.tentative += r.cto.tentative;
        self.stale += r.cto.violations.len();
        for s in r.stats.values() {
            self.grants += s.read_grants;
            self.recalls_sent += s.recalls_sent;
            self.recalls_acked += s.recalls_acked;
            self.recalls_expired += s.recalls_expired;
            self.recall_waits += s.recall_waits;
            self.held_replies += s.held_replies;
            self.read_index_sent += s.read_index_sent;
            self.read_index_degraded += s.read_index_degraded;
            self.read_index_tailed += s.read_index_tailed;
        }
    }
}

fn strict_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.7,
        strict: true,
        ..SimConfig::default()
    }
}

fn run_strict(label: &str, cfg: SimConfig, seeds: std::ops::Range<u64>) -> CtoTotals {
    let mut t = CtoTotals::default();
    for seed in seeds {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| {
            panic!(
                "{label} seed {seed}: {e}\n  replay: AUTHORITY_SIM_SEED={seed} \
                 AUTHORITY_SIM_CONFIG=strict cargo test -p constellation-authority \
                 --test sim replay_seed -- --nocapture --exact"
            )
        });
        t.add(&report);
    }
    eprintln!("{label}: {t:?}");
    t
}

/// Plan 30 §M8: `cto=strict` reads — delegations on the directory,
/// recalls on every write into it, ReadIndex otherwise — keep
/// close-to-open across nodes under the CI faults (crashes, restarts,
/// pauses, partitions, S3 errors), with the check enforced in the run.
#[test]
fn strict_close_to_open_holds() {
    let t = run_strict("strict", strict_config(), 800..860);
    assert!(
        t.reads > 500 && t.constrained > 200,
        "too few reads to mean anything"
    );
    assert!(
        t.grants > 0 && t.recalls_sent > 0 && t.recalls_acked > 0,
        "vacuous: {t:?}"
    );
}

/// The same with every node's clock off by up to ±200 ms (the sim's
/// lease margin is 500 ms: `2D < margin`, the lease's own assumption),
/// and 3 % P2P loss so recalls are lost and outwaited.
#[test]
fn strict_close_to_open_holds_with_clock_skew_and_loss() {
    let cfg = SimConfig {
        clock_skew_ms: 200,
        p2p_drop: 0.03,
        ..strict_config()
    };
    let t = run_strict("strict-skew", cfg, 900..950);
    assert!(t.recalls_sent > 0, "vacuous: {t:?}");
}

/// ReadIndex alone (the sequencer grants nothing): still close-to-open,
/// one round trip per strict read.
#[test]
fn strict_without_delegations_holds() {
    let cfg = SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::sim_core_config(id, inc);
            c.read_delegations = false;
            c
        }),
        ..strict_config()
    };
    let t = run_strict("strict-no-delegations", cfg, 1000..1030);
    assert_eq!(t.grants, 0);
    assert!(t.read_index_sent > 0);
}

/// P2P off: no ReadIndex, strict reads tail S3 (reported, not enforced:
/// the sequencer's own writes are acknowledged before they ship).
#[test]
fn strict_with_p2p_off_tails_s3() {
    let cfg = SimConfig {
        core: std::sync::Arc::new(sim::run::inbox_core_config),
        ..strict_config()
    };
    let t = run_strict("strict-p2p-off", cfg, 1100..1130);
    assert_eq!(t.grants, 0);
    assert_eq!(t.read_index_sent, 0);
    assert!(t.read_index_tailed > 0);
}

/// Non-vacuity for the recall: the same strict workload with the
/// sequencer acknowledging writes without recalling delegations must
/// break close-to-open somewhere (a delegate reads its directory locally
/// after another node's create or unlink completed).
#[test]
fn delegations_without_recall_are_found() {
    let cfg = SimConfig {
        read_ratio: 0.9,
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::sim_core_config(id, inc);
            c.recall_before_ack = false;
            c
        }),
        ..strict_config()
    };
    let mut found = None;
    for seed in 800..900 {
        if let Err(e) = run_seed(seed, cfg.clone()) {
            assert!(e.contains("close-to-open violated"), "seed {seed}: {e}");
            found = Some((seed, e));
            break;
        }
    }
    let (seed, e) = found.expect("no seed broke close-to-open without recalls");
    eprintln!(
        "without recalls, seed {seed}: {}",
        e.lines().next().unwrap_or("")
    );
}

/// The counterexample: bounded mode (reads never ask the sequencer) reads
/// stale state after another node's write completed — the checker finds
/// it, so it is not vacuous.
#[test]
fn bounded_mode_reads_stale() {
    let cfg = SimConfig {
        read_ratio: 0.9,
        ..SimConfig::default()
    };
    let mut found = None;
    for seed in 800..900 {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        if let Some(v) = report.cto.violations.first() {
            found = Some((seed, v.clone()));
            break;
        }
    }
    let (seed, what) = found.expect("bounded mode never read stale: the checker is vacuous");
    eprintln!("bounded mode, seed {seed}: {what}");
}

/// Plan 30 §M13: P2P off, so every non-holder write goes through the
/// holder's S3 inbox (submitted under its epoch, executed in order by the
/// holder's poll, answered through the log), sustained demand escalates
/// to a lease request, and a takeover drains the older epoch's batches
/// inside its gate. The same faults as the default configuration.
#[test]
fn regression_inbox_p2p_off() {
    let cfg = SimConfig {
        core: std::sync::Arc::new(sim::run::inbox_core_config),
        ..SimConfig::default()
    };
    let mut inbox_ops = 0u64;
    let mut escalations = 0u64;
    let mut drained = 0u64;
    for seed in 600..640 {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert_eq!(report.p2p_sent, 0, "P2P is off");
        for s in report.stats.values() {
            inbox_ops += s.inbox_answered;
            escalations += s.inbox_escalations;
            drained += s.inbox_drained_batches;
        }
    }
    eprintln!(
        "inbox: {inbox_ops} ops answered through the log, {escalations} escalations,          {drained} batches drained at takeover"
    );
    assert!(inbox_ops > 0, "no op went through the inbox");
    assert!(escalations > 0, "no sustained demand escalated");
}

/// Plan 30 M5 phase 2: an op submitted to the holder's inbox during an
/// outage must be withdrawn (its batch deleted) before it is forwarded
/// over P2P once the holder is back. Seed 794 found the gap: the
/// restarted holder re-acquired its own epoch (no takeover, no drain),
/// answered the forward with EEXIST (no `completed` witness), and the
/// next takeover's drain executed the stale batch a second time.
///
/// Plan 30 M7: log streams moved seed 794's interleaving off the path
/// (the fix is structural, the seed only reached it), so the test now
/// runs 794 and the seeds after it until one withdraws a batch — every
/// run passes every check either way.
#[test]
fn regression_inbox_batch_withdrawn_before_p2p_forward() {
    let mut withdrawn = 0;
    let mut seed = 794;
    while withdrawn == 0 && seed < 794 + 200 {
        let report =
            run_seed(seed, SimConfig::default()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        withdrawn += report
            .stats
            .values()
            .map(|s| s.inbox_withdrawn_ops)
            .sum::<u64>();
        seed += 1;
    }
    eprintln!(
        "seed {}: an inbox batch withdrawn before a P2P forward",
        seed - 1
    );
    assert!(
        withdrawn > 0,
        "no seed in 794..994 withdraws a batch before forwarding"
    );
}

/// Plan 30 M5 phase 2: a resubmitted in-doubt rid that waited behind an
/// earlier op of its node (the key gate) must still consult `completed`
/// before executing. Long-config seed 1207 found the gap: the takeover
/// gate's inbox drain executed the rid, the gated resubmission then ran
/// it again and answered ENOENT for an op the log had completed.
#[test]
fn regression_gated_resubmission_checks_completed() {
    for seed in [1207, 2007, 2328] {
        let report = run_seed(seed, long_config()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert!(report.converged_checked, "seed {seed} did not converge");
    }
}

/// Plan 30 M5 phase 2: an in-doubt rid's durable inbox batch survives
/// its resubmission (`in_doubt_batches`), so the resubmitted op still
/// withdraws it before a P2P forward. Long-config seeds 2267 and 2277
/// found the gap: the resubmission was a fresh client op with no batch
/// key, its forward was refused, and the requester's own later takeover
/// drained the stale batch and executed the op.
#[test]
fn regression_resubmitted_rid_withdraws_its_batch() {
    // 10476: the M8 coder found it failing linearizability on the M5+M6+M7
    // stack (b906e90); it passes on M5 with round 3's fixes and is pinned
    // here so the stack can bisect against it.
    for seed in [2267, 2277, 10476] {
        let report = run_seed(seed, long_config()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert!(report.converged_checked, "seed {seed} did not converge");
    }
}

/// Plan 30 M5 round 4: one op submitted to the inbox twice under the
/// same epoch (its inbox deadline sends it down the lease path, the
/// acquisition loses to the live holder, and it goes back through the
/// inbox as a second batch) used to remember only its latest batch; the
/// P2P forward that answered it `EEXIST` withdrew only that one, and the
/// next takeover drained the forgotten batch and created the file after
/// the client had been told it existed (seed 10476 on the M5+M6+M7 stack,
/// found by the M8 coder, bisected by the M7 coder). Seeds 10247 and
/// 10507 reached the multi-batch withdraw on M5's timing; plan 30 M6's
/// core changes (no causal-wait timers) shift every schedule, and on this
/// tree 10507 and 10396 reach it (`find_multi_batch_withdraw_seeds`
/// re-pins). 10247 stays as a convergence seed; 10476 is pinned above.
/// Plan 30 M7: log streams shift every schedule again; on this tree
/// 10981 and 11066 reach the multi-batch withdraw (re-pinned with the
/// helper over 10000–11999).
#[test]
fn regression_every_inbox_batch_of_a_rid_is_withdrawn() {
    let report = run_seed(10247, long_config()).unwrap_or_else(|e| panic!("seed 10247: {e}"));
    assert!(report.converged_checked, "seed 10247 did not converge");
    for seed in [10981, 11066] {
        let report = run_seed(seed, long_config()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let multi: u64 = report
            .stats
            .values()
            .map(|s| s.inbox_multi_batch_withdrawals)
            .sum();
        assert!(
            multi >= 1,
            "seed {seed} no longer forwards an op with several batches to withdraw ({multi})"
        );
        assert!(report.converged_checked, "seed {seed} did not converge");
    }
}

/// Re-pin helper for `regression_every_inbox_batch_of_a_rid_is_withdrawn`:
/// lists the long-configuration seeds that reach the multi-batch withdraw
/// on this tree's timing (a core change shifts every schedule).
/// `AUTHORITY_SIM_START`/`AUTHORITY_SIM_SEEDS` as for `long_random`.
#[test]
#[ignore]
fn find_multi_batch_withdraw_seeds() {
    let start: u64 = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let seeds: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    for seed in start..start + seeds {
        if let Ok(report) = run_seed(seed, long_config()) {
            let multi: u64 = report
                .stats
                .values()
                .map(|s| s.inbox_multi_batch_withdrawals)
                .sum();
            if multi > 0 {
                eprintln!("multi-batch withdraw: seed {seed} ({multi})");
            }
        }
    }
}

/// Plan 30 M5 round 5: `release_gated` re-enters itself through `finish`,
/// so an op later in its ready list can already be finished by the
/// nested pass; the outer loop then indexed a removed entry (`no entry
/// found for key`, long seed 11932) and the panicking node task left the
/// run hanging. The loop now skips what is gone, and a panic anywhere in
/// a run fails the seed at once.
#[test]
fn regression_nested_gate_release_does_not_index_a_finished_op() {
    let report = run_seed(11932, long_config()).unwrap_or_else(|e| panic!("seed 11932: {e}"));
    assert!(report.converged_checked, "seed 11932 did not converge");
}

/// Plan 30 M5 round 5: a panic in any task of a run fails the seed at
/// once with the panic's message, instead of leaving the run waiting for
/// a quiescence the dead node can never reach (seed 11932 sat at 0 % CPU
/// for minutes after its panic).
#[test]
fn a_panicking_node_fails_the_seed_promptly() {
    let cfg = SimConfig {
        random_faults: 0,
        panic_after_events: Some(40),
        ..SimConfig::default()
    };
    let started = std::time::Instant::now();
    let err = run_seed(7, cfg).expect_err("the injected panic must fail the seed");
    assert!(
        err.contains("a task panicked") && err.contains("injected panic after 40 events"),
        "unexpected failure: {err}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "the panic took {:?} to surface",
        started.elapsed()
    );
}

/// A single node with no peers: today's single-node behaviour, no
/// forwarding, no takeover, every op local.
#[test]
fn single_node_is_clean() {
    let cfg = SimConfig {
        nodes: 1,
        clients_per_node: 2,
        ops_per_client: 10,
        random_faults: 0,
        ..SimConfig::default()
    };
    for seed in 400..410 {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert_eq!(report.p2p_sent, 0, "a lone node sent P2P messages");
        assert!(report.converged_checked);
    }
}

// ---- plan 30 §M7: log streams ----

fn streams_off_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.3,
        core: std::sync::Arc::new(|node, inc| {
            let mut c = sim::run::sim_core_config(node, inc);
            c.log_streams = false;
            c
        }),
        ..SimConfig::default()
    }
}

fn stream_faults_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.3,
        stream_faults: StreamFaults {
            drop_p: 0.04,
            reorder_p: 0.04,
            cut_p: 0.02,
            drop_segment_frames: Vec::new(),
        },
        ..SimConfig::default()
    }
}

/// Two nodes, no random faults, and the stream loses segment-carrying
/// frames 2 and 5: the subscriber must notice each loss at the next
/// frame, read the missing sequence from S3, resubscribe, and converge.
fn stream_gap_config() -> SimConfig {
    SimConfig {
        nodes: 2,
        clients_per_node: 2,
        ops_per_client: 10,
        random_faults: 0,
        read_ratio: 0.3,
        stream_faults: StreamFaults {
            drop_segment_frames: vec![2, 5],
            ..StreamFaults::default()
        },
        ..SimConfig::default()
    }
}

/// Streams carry the log: subscribers apply most segments from the
/// holder's stream, rounds skip the S3 tail, and the same seeds with
/// streams off need more S3 GETs. Every run passes every check either
/// way (the streams change latency and traffic, never what is applied).
#[test]
fn streams_carry_the_log_and_save_tail_gets() {
    let mut on = StreamTotals::default();
    let mut off = StreamTotals::default();
    for seed in 500..560 {
        let cfg = SimConfig {
            read_ratio: 0.3,
            random_faults: 0,
            ..SimConfig::default()
        };
        let r = run_seed(seed, cfg).unwrap_or_else(|e| panic!("seed {seed} (streams on): {e}"));
        assert!(r.converged_checked, "seed {seed} did not converge");
        on.add(&r);
        let cfg = SimConfig {
            random_faults: 0,
            ..streams_off_config()
        };
        let r = run_seed(seed, cfg).unwrap_or_else(|e| panic!("seed {seed} (streams off): {e}"));
        off.add(&r);
    }
    eprintln!("streams on:  {on:?}\nstreams off: {off:?}");
    assert!(
        on.applied > 0 && on.tail_skips > 0,
        "no segment came over a stream: {on:?}"
    );
    assert_eq!(
        off.applied + off.subscribes + off.served,
        0,
        "streams off still streamed"
    );
    assert!(
        on.s3_gets < off.s3_gets,
        "streams did not reduce S3 GETs: on {} vs off {}",
        on.s3_gets,
        off.s3_gets
    );
}

/// Lost, reordered and cut stream frames on top of the CI seeds' random
/// faults: every check still holds, and each fault was actually met.
#[test]
fn stream_faults_are_survived() {
    let mut totals = StreamTotals::default();
    for seed in 600..750 {
        let r =
            run_seed(seed, stream_faults_config()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        totals.add(&r);
    }
    eprintln!("stream faults: {totals:?}");
    assert!(
        totals.gaps > 0,
        "no frame gap was ever detected: {totals:?}"
    );
    assert!(totals.lost > 0, "no stream was ever cut: {totals:?}");
    assert!(
        totals.dropped_subscribers > 0,
        "no subscriber was dropped: {totals:?}"
    );
    assert!(totals.applied > 0, "nothing streamed: {totals:?}");
}

/// Regression for gap detection: a lost segment frame is noticed at the
/// next frame (`stream_gaps`), the missing sequence comes from S3, the
/// subscriber resubscribes and keeps streaming, and every replica
/// converges to the log — no sequence skipped, none applied twice.
#[test]
fn regression_stream_gap_detected() {
    let mut detected = 0;
    for seed in 800..820 {
        let r = run_seed(seed, stream_gap_config()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert!(r.converged_checked, "seed {seed} did not converge");
        let mut t = StreamTotals::default();
        t.add(&r);
        if t.gaps > 0 {
            detected += 1;
            assert!(
                t.subscribes >= 2,
                "seed {seed}: no resubscription after the gap: {t:?}"
            );
        }
    }
    assert!(detected >= 10, "only {detected}/20 seeds met a gap");
}

/// The long configuration M5's pinned regression seeds were found and
/// bisected under (no reads: reads change a seed's schedule).
fn long_config() -> SimConfig {
    SimConfig {
        ops_per_client: 12,
        random_faults: 4,
        p2p_drop: 0.02,
        ..SimConfig::default()
    }
}

/// Plan 30 M6: the long configuration with reads through the session
/// wait (and the session checks enforced).
fn long_sessions_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.3,
        ..long_config()
    }
}

/// The long randomized run: `cargo test -p constellation-authority --test sim -- --ignored long_random`.
#[test]
#[ignore]
fn long_random() {
    let seeds: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000);
    let start: u64 = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    // Plan 30 M6: `AUTHORITY_SIM_READS=0` sweeps the read-free
    // configuration M5's seeds are pinned under; the default reads.
    let cfg = match std::env::var("AUTHORITY_SIM_READS").as_deref() {
        Ok("0") => long_config(),
        _ => long_sessions_config(),
    };
    let mut failures = Vec::new();
    for seed in start..start + seeds {
        let started = std::time::Instant::now();
        match run_seed(seed, cfg.clone()) {
            Ok(report) => eprintln!(
                "seed {seed}: ok in {:.2}s ({} ms simulated, {} ops, {} segments, converged {}, \
                 observed-tentative refusals {})",
                started.elapsed().as_secs_f64(),
                report.simulated_ms,
                report.ops_returned,
                report.segments,
                report.converged_checked,
                report.observed_tentative
            ),
            Err(e) => {
                eprintln!("seed {seed}: {e}");
                failures.push(seed);
            }
        }
    }
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}

/// Plan 30 §M8: the long configuration with `cto=strict` reads (0.6 per
/// op), ±200 ms clock skew and 2 % P2P loss:
/// `cargo test -p constellation-authority --release --test sim -- --ignored long_strict`
/// (`AUTHORITY_SIM_SEEDS`, `AUTHORITY_SIM_START`; replay one with
/// `AUTHORITY_SIM_CONFIG=long-strict`).
fn long_strict_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.6,
        strict: true,
        clock_skew_ms: 200,
        ..long_config()
    }
}

#[test]
#[ignore]
fn long_strict() {
    let seeds: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let start: u64 = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40_000);
    let cfg = long_strict_config();
    let mut failures = Vec::new();
    let mut totals = CtoTotals::default();
    for seed in start..start + seeds {
        match run_seed(seed, cfg.clone()) {
            Ok(report) => totals.add(&report),
            Err(e) => {
                eprintln!("seed {seed}: {e}");
                failures.push(seed);
            }
        }
    }
    eprintln!("long_strict: {totals:?}");
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}
