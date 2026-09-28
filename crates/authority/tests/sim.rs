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
    pub mod chunks;
    pub mod clock;
    pub mod cto;
    pub mod epochs;
    pub mod history;
    pub mod locks;
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
        // Plan 30 §M9.
        Ok("backup") => backup_config(),
        Ok("acks3") => ack_s3_config(),
        Ok("backup-far") => far_config(),
        Ok("backup-strict") => backup_strict_config(),
        Ok("backup-crash") => backup_crash_config(),
        Ok("holder-cut") => holder_cut_crash_config(true),
        Ok("delegated-holder-cut") => delegated_holder_cut_config(),
        Ok("holder-cut-join") => holder_cut_crash_config(false),
        Ok("backup-crash-slow") => SimConfig {
            s3_latency: (60, 200),
            ..backup_crash_config()
        },
        Ok("acks3-crash") => ack_s3_crash_config(),
        Ok("backup-departs") => backup_departs_config(),
        Ok("backup-partition") => backup_partition_config(),
        Ok("long-backup") => long_backup_config(),
        Ok("backup-hot") => backup_hot_config(),
        Ok("placement-hot") => placement_hot_config(),
        // `long_backup`'s odd seeds: the same, under `ack=s3`.
        Ok("long-acks3") => SimConfig {
            core: std::sync::Arc::new(sim::run::ack_s3_core_config),
            ..long_backup_config()
        },
        // Plan 30 §M10.
        Ok("flex") => flex_config(),
        Ok("flex-unchecked") => flex_unchecked_config(),
        Ok("flex-crash") => flex_crash_config(),
        Ok("flex-long") => flex_config(),
        Ok("flex-backup") => flex_backup_config(),
        Ok("flex-zero") => flex_zero_config(),
        // Plan 30 §M11.
        Ok("delegated") => delegated_config(),
        Ok("delegated-marker") => delegated_marker_config(),
        Ok("delegated-crash") => delegated_crash_config(),
        Ok("delegated-partition") => delegated_partition_config(),
        Ok("delegated-epoch") => delegated_epoch_config(),
        Ok("delegated-faults") => delegated_faults_config(),
        Ok("delegated-root-crash") => delegated_root_crash_config(),
        Ok("delegated-backup") => delegated_backup_config(),
        Ok("delegated-backup-crash") => delegated_backup_crash_config(),
        Ok("delegated-designated") => delegated_designated_config(),
        Ok("delegated-placement") => delegated_placement_config(),
        Ok("delegated-strict") => delegated_strict_config(),
        Ok("shared-dir") => shared_dir_config(),
        Ok("shared-dir-split") => shared_dir_split_config(),
        Ok("shared-dir-fast-path") => shared_dir_fast_path_config(),
        Ok("shared-dir-fast-path-unchecked") => shared_dir_fast_path_unchecked_config(),
        Ok("shared-dir-ranges") => shared_dir_ranges_config(),
        Ok("shared-dir-faults") => shared_dir_faults_config(),
        Ok("long-delegated") => long_delegated_config(),
        Ok("long-delegated-backup") => long_delegated_backup_config(),
        // Plan 30 §M14.
        Ok("locks") => locks_config(),
        Ok("locks-partition") => locks_partition_config(),
        Ok("locks-skew") => locks_skew_config(),
        Ok("locks-failover") => locks_failover_config(),
        Ok("locks-failover-backup") => locks_failover_backup_config(),
        Ok("locks-faults") => locks_faults_config(),
        Ok("locks-pause") => locks_pause_config(),
        Ok("locks-delegated") => locks_delegated_config(),
        Ok("locks-released-delegated") => locks_released_delegated_config(),
        Ok("locks-delegated-writes") => with_lock_writes(locks_delegated_config(), "d2"),
        Ok("locks-released-writes") => with_lock_writes(locks_released_delegated_config(), "d2"),
        Ok("locks-failover-backup-writes") => with_lock_writes(locks_failover_backup_config(), ""),
        Ok("locks-writes") => with_lock_writes(locks_config(), ""),
        Ok("locks-nofence") => SimConfig {
            lock_ignore_fence: true,
            ..locks_partition_config()
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
    // Plan 30 §M14: `AUTHORITY_SIM_LOCK_WRITES=1` adds the lock-to-unlock
    // coherence check (turns written under exclusive locks) to any
    // lock configuration.
    // `AUTHORITY_SIM_LOCK_DATA_DIR=<dir>|root` writes the turns into data
    // files there instead of into the lock files.
    let config = SimConfig {
        lock_writes: config.lock_writes
            || std::env::var("AUTHORITY_SIM_LOCK_WRITES").as_deref() == Ok("1"),
        lock_data_dir: match std::env::var("AUTHORITY_SIM_LOCK_DATA_DIR").as_deref() {
            Ok("root") => Some(String::new()),
            Ok(d) => Some(d.to_string()),
            Err(_) => config.lock_data_dir.clone(),
        },
        ..config
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
/// the op on state this replica has not applied yet. The simulation used
/// to find the divergence here (the core's default refuses such a base
/// and waits for the log instead — `PeerMsg::MutateReply::base`). Every
/// one it found was a tailed segment applied *on top of* speculation that
/// is later in the log than it; since EC2 campaign 4 B-2 the segment goes
/// in under the speculation it overlaps, and 600 seeds (500..1100) found
/// none. The window is closed at the meta layer as well: this now asserts
/// convergence.
#[test]
fn stale_base_speculation_converges() {
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
    for seed in 500..560 {
        run_seed(seed, cfg.clone())
            .unwrap_or_else(|e| panic!("stale-base speculation, seed {seed}: {e}"));
    }
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
/// outage must be withdrawn (its batch overwritten with a tombstone;
/// deleted, before the withdraw-hole fix) before it is forwarded
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

/// Withdraw hole: node 2's P2P link to the initial holder flaps (cut
/// 2.5 s, up 1 s, five times) while every client writes. A write of 2's
/// goes to the inbox during a cut, is withdrawn and forwarded when the
/// link comes back, and 2's next writes go to the inbox at the next cut.
fn withdraw_hole_config() -> SimConfig {
    SimConfig {
        ops_per_client: 16,
        faults: (0..5)
            .map(|i| ScheduledFault {
                at_ms: 500 + i * 3_500,
                kind: FaultKind::Partition {
                    a: 1,
                    b: 2,
                    for_ms: 2_500,
                },
            })
            .collect(),
        ..SimConfig::default()
    }
}

/// Withdraw hole: a withdrawal overwrites the batch with a tombstone
/// instead of deleting it, and a holder that reads one steps past it to
/// the requester's later batches (a deleted key stopped its GET-next for
/// the rest of the epoch, and those batches' ops waited for the in-doubt
/// deadline and the lease path). Every seed passes every check
/// (exactly-once by rid, linearizability, convergence); the sweep must
/// reach a holder reading a tombstone.
#[test]
fn regression_a_holder_steps_past_a_withdrawn_inbox_batch() {
    let (mut read, mut withdrawn, mut unavailable, mut answered, mut rtt) = (0, 0, 0, 0, 0);
    for seed in 0..SEEDS_WITHDRAW_HOLE {
        let report =
            run_seed(seed, withdraw_hole_config()).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert!(report.converged_checked, "seed {seed} did not converge");
        for s in report.stats.values() {
            read += s.inbox_tombstones_read;
            withdrawn += s.inbox_withdrawn_ops;
            unavailable += s.inbox_unavailable;
            answered += s.inbox_answered;
            rtt += s.inbox_round_trip_ms_total;
        }
    }
    eprintln!(
        "withdraw hole: {withdrawn} withdrawals, {read} tombstones read by a holder, \
         {unavailable} inbox ops that took the lease path; {answered} answered through the inbox, \
         mean round trip {} ms",
        rtt / answered.max(1)
    );
    assert!(
        read > 0,
        "no holder read a withdrawn batch ({withdrawn} withdrawals)"
    );
}

const SEEDS_WITHDRAW_HOLE: u64 = 40;

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
    // Re-pinned after plan 30 M9's rebase (the holder journals refusals
    // now, which shifts every schedule): `find_multi_batch_withdraw_seeds`
    // over 10000–11999 found 13 seeds on this tree; these two reach it
    // twice each. EC2 follow-up (an inbox-waiting op is forwarded once the
    // holder is reachable) moved the schedules again: 10396 and 11750
    // reach it four times each now (10729 and 11608 no longer do).
    for seed in [10396, 11750] {
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

/// Seeds of `flex_crash_config` whose run adopts a carried hold late
/// (`flex_crash_seed_30299_restarted_member_adopts_the_carried_hold`'s
/// property); a finder for when a schedule change moves that seed.
/// `AUTHORITY_SIM_START`/`AUTHORITY_SIM_SEEDS` as for `long_random`.
#[test]
#[ignore]
fn find_late_adopted_hold_seeds() {
    let start: u64 = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);
    let seeds: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);
    for seed in start..start + seeds {
        match run_seed(seed, flex_crash_config()) {
            Ok(report) => {
                let adopted: u64 = report
                    .stats
                    .values()
                    .map(|s| s.epoch_holds_adopted_late)
                    .sum();
                if adopted > 0 {
                    eprintln!("late-adopted hold: seed {seed} ({adopted})");
                }
            }
            Err(e) => eprintln!("seed {seed} failed: {e}"),
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
    for seed in 500..1100 {
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

// ---- plan 30 §M9: backups, seal-based failover, `ack=s3` ----

/// Plan 30 §M9's counters summed over a run's nodes.
#[derive(Debug, Default, Clone)]
struct M9Totals {
    backups_added: u64,
    backups_removed: u64,
    reconfig_cas: u64,
    appends: u64,
    acks: u64,
    ack_timeouts: u64,
    acks_waited: u64,
    ack_wait_ms: u64,
    acks_aborted: u64,
    streamed_ahead: u64,
    streamed_installed: u64,
    streamed_dropped: u64,
    seals: u64,
    backup_takeovers: u64,
    tail_applied: u64,
    s3_fast_takeovers: u64,
    ack_floor_waits: u64,
    stale_refusals: u64,
    takeovers: u64,
    acked_rolled_back: usize,
    failovers: Vec<u64>,
}

impl M9Totals {
    fn add(&mut self, r: &Report) {
        for s in r.stats.values() {
            self.backups_added += s.backups_added;
            self.backups_removed += s.backups_removed;
            self.reconfig_cas += s.reconfig_cas;
            self.appends += s.backup_appends;
            self.acks += s.backup_acks;
            self.ack_timeouts += s.backup_ack_timeouts;
            self.acks_waited += s.acks_waited;
            self.ack_wait_ms += s.ack_wait_ms_total;
            self.acks_aborted += s.acks_aborted;
            self.streamed_ahead += s.streamed_ahead;
            self.streamed_installed += s.streamed_installed;
            self.streamed_dropped += s.streamed_dropped;
            self.seals += s.seals;
            self.backup_takeovers += s.backup_takeovers;
            self.tail_applied += s.backup_tail_applied;
            self.s3_fast_takeovers += s.s3_fast_takeovers;
            self.ack_floor_waits += s.ack_floor_waits;
            self.stale_refusals += s.stale_liveness_refusals;
            self.takeovers += s.takeovers;
        }
        self.acked_rolled_back += r.acked_rolled_back;
        self.failovers.extend(r.failover_ms.iter().copied());
    }

    fn failover_dist(&self) -> String {
        if self.failovers.is_empty() {
            return "none".into();
        }
        let mut v = self.failovers.clone();
        v.sort_unstable();
        let p = |q: f64| v[((v.len() - 1) as f64 * q).round() as usize];
        format!(
            "n={} p50={}ms p90={}ms max={}ms",
            v.len(),
            p(0.5),
            p(0.9),
            v[v.len() - 1]
        )
    }
}

fn backup_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.3,
        strict_durability: true,
        core: std::sync::Arc::new(sim::run::backup_core_config),
        ..SimConfig::default()
    }
}

fn ack_s3_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::ack_s3_core_config),
        ..backup_config()
    }
}

/// Every pair's RTT is above the budget: no backup, today's behaviour.
fn far_config() -> SimConfig {
    SimConfig {
        rtts: vec![((1, 2), 120), ((1, 3), 120), ((2, 3), 120)],
        strict_durability: false,
        ..backup_config()
    }
}

/// A holder crash mid-burst with the backup configuration: the backup
/// seals and takes over.
fn backup_crash_config() -> SimConfig {
    SimConfig {
        ops_per_client: 8,
        random_faults: 1,
        faults: vec![
            // The holder loses S3 first: what it acknowledges through its
            // backup from here on is exactly the tail the backup must
            // re-ship after the crash.
            ScheduledFault {
                at_ms: 1_100,
                kind: FaultKind::CutS3Holder { for_ms: 2_000 },
            },
            ScheduledFault {
                at_ms: 1_500,
                kind: FaultKind::CrashHolder {
                    restart_ms: Some(7_000),
                    keep_journal: true,
                },
            },
        ],
        ..backup_config()
    }
}

fn ack_s3_crash_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::ack_s3_core_config),
        ..backup_crash_config()
    }
}

fn backup_departs_config() -> SimConfig {
    SimConfig {
        ops_per_client: 8,
        random_faults: 0,
        faults: vec![ScheduledFault {
            at_ms: 1_800,
            kind: FaultKind::CrashBackup {
                restart_ms: Some(4_000),
            },
        }],
        ..backup_config()
    }
}

fn backup_partition_config() -> SimConfig {
    SimConfig {
        ops_per_client: 8,
        random_faults: 0,
        faults: vec![ScheduledFault {
            at_ms: 1_800,
            kind: FaultKind::PartitionBackup { for_ms: 2_500 },
        }],
        ..backup_config()
    }
}

fn backup_strict_config() -> SimConfig {
    SimConfig {
        read_ratio: 0.6,
        strict: true,
        ..backup_crash_config()
    }
}

/// Runs the seeds on up to 8 threads (the backup ranges are a few
/// hundred seeds each since the `Exists`-hint fix: that divergence showed
/// in ~0.15% of `backup` seeds and the ranges of 30–60 never met it);
/// the totals are summed in seed order.
fn run_m9(label: &str, cfg: SimConfig, seeds: std::ops::Range<u64>) -> M9Totals {
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .clamp(1, 8) as u64;
    let mut results: Vec<(u64, Result<Report, String>)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let (cfg, seeds) = (cfg.clone(), seeds.clone());
                s.spawn(move || {
                    seeds
                        .skip(t as usize)
                        .step_by(threads as usize)
                        .map(|seed| (seed, run_seed(seed, cfg.clone())))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("sim thread"))
            .collect()
    });
    results.sort_by_key(|(seed, _)| *seed);
    let mut totals = M9Totals::default();
    for (seed, result) in results {
        let report = result.unwrap_or_else(|e| {
            panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
        });
        totals.add(&report);
    }
    eprintln!("{label}: {totals:?}; failover {}", totals.failover_dist());
    totals
}

/// Plan 30 §M9: three LAN nodes under the backup configuration with the
/// CI faults: a backup is chosen and the lease says so; every
/// acknowledgement is durable, so no acknowledged op is ever rolled back
/// and no refusal ever observed a tentative effect (`strict_durability`
/// makes both hard failures); linearizable; converged.
#[test]
fn backup_no_acked_op_lost() {
    let t = run_m9("backup", backup_config(), 700..1300);
    assert!(t.backups_added >= 40, "backups rarely chosen: {t:?}");
    assert!(t.acks_waited > 100, "acknowledgements never waited: {t:?}");
    // `acked_rolled_back` counts *transient* rollbacks too: a requester's
    // shadow is stranded by the successor's marker and completed again by
    // the re-shipped tail moments later (plan 30 §M3's rule; reads of the
    // keys wait meanwhile). What must hold — and `strict_durability`
    // checks — is that every acknowledged op is in the log once, in
    // acknowledgement order.
}

/// Plan 30 §M9: the holder is killed mid-burst; the backup seals its
/// epoch, takes the lease over before the TTL, re-ships the tail it held,
/// and every acknowledged op is in the log exactly once.
#[test]
fn backup_failover_reships_the_tail() {
    let t = run_m9("backup-crash", backup_crash_config(), 800..1400);
    assert!(t.seals >= 20, "the backup rarely sealed: {t:?}");
    assert!(
        t.backup_takeovers >= 20,
        "the backup rarely took over: {t:?}"
    );
    assert!(t.tail_applied >= 10, "no tail was ever re-shipped: {t:?}");
    let ttl = sim::run::backup_core_config(1, 1).ttl_ms;
    let fast = t.failovers.iter().filter(|ms| **ms < ttl).count();
    assert!(
        fast * 2 > t.failovers.len(),
        "failover mostly waited for the TTL: {}",
        t.failover_dist()
    );
}

/// Plan 30 §M9: `ack=s3` — no backups, acknowledgements wait for the
/// segment, and a peer takes an unexpired lease over on holder silence;
/// the log-slot CAS is the only fence.
#[test]
fn ack_s3_no_acked_op_lost() {
    let t = run_m9("acks3-crash", ack_s3_crash_config(), 900..1100);
    assert_eq!(t.backups_added, 0, "ack=s3 uses no backups: {t:?}");
    assert!(t.acks_waited > 100, "acknowledgements never waited: {t:?}");
    assert!(
        t.s3_fast_takeovers >= 20,
        "fast takeover rarely happened: {t:?}"
    );
    // (`acked_rolled_back` may be non-zero even here: a dead holder's
    // stale journal copy of a row that had landed in S3 is rolled back at
    // its restart and its replay dedups — the log-level checks are what
    // says nothing was lost.)
    run_m9("acks3", ack_s3_config(), 940..1040);
}

/// Plan 30 §M9: no peer within the RTT budget means today's behaviour —
/// no backup, no appends, TTL failover (and acknowledged ops may be
/// rolled back, as today).
#[test]
fn no_peer_in_budget_is_todays_behaviour() {
    let cfg = SimConfig {
        faults: backup_crash_config().faults,
        ops_per_client: 8,
        random_faults: 0,
        ..far_config()
    };
    let t = run_m9("backup-far", cfg, 1000..1020);
    assert_eq!(t.backups_added, 0, "{t:?}");
    assert_eq!(t.appends, 0, "{t:?}");
    assert_eq!(t.seals, 0, "{t:?}");
    assert_eq!(t.acks_waited, 0, "{t:?}");
    // A reply already on its way when the holder died counts as an
    // acknowledgement after the crash (within a round trip or two: the
    // RTTs here are 120 ms); the rest wait for the TTL. (Stated per
    // failover since EC2 campaign 8 A-1's fix: every node now reads the
    // lease at start, and the shifted schedule gave 3 of the 20 seeds
    // such an in-flight reply, one more than the "at most 10 % early"
    // this used to allow.)
    let ttl = sim::run::backup_core_config(1, 1).ttl_ms;
    let early: Vec<u64> = t
        .failovers
        .iter()
        .copied()
        .filter(|ms| *ms >= 250 && *ms < ttl / 2)
        .collect();
    assert!(
        early.is_empty(),
        "failovers happened before the TTL: {} (early: {early:?})",
        t.failover_dist()
    );
}

/// Plan 30 §M9: the backup unmounts (dies); the holder removes it by a
/// lease CAS and writes continue; when it returns it is brought back.
#[test]
fn backup_departs_reconfigures() {
    let t = run_m9("backup-departs", backup_departs_config(), 1100..1300);
    assert!(
        t.backups_removed >= 20,
        "the backup was rarely removed: {t:?}"
    );
    assert!(t.backups_added > t.backups_removed, "never re-added: {t:?}");
}

/// Plan 30 §M9: the holder is partitioned from its backup (both keep S3).
/// Either the holder removes it (an ack timeout, then a CAS) or the backup
/// seals and takes over — never both acknowledging; the checks (strict
/// durability, linearizability, convergence) say so.
#[test]
fn backup_partition_reconfigures_or_seals() {
    let t = run_m9("backup-partition", backup_partition_config(), 1200..1400);
    assert!(
        t.backups_removed + t.backup_takeovers >= 20,
        "the partition was rarely resolved either way: {t:?}"
    );
}

/// Plan 30 §M9 + §M8: `cto=strict` readers hold delegations when the
/// holder dies and its backup takes the lease over before the old lease
/// expired. The successor waits the predecessor's grant horizon out
/// before acknowledging mutations (`ack_floor_waits`), and close-to-open
/// holds (enforced).
#[test]
fn fast_failover_with_delegations_keeps_close_to_open() {
    let t = run_m9("backup-strict", backup_strict_config(), 1300..1500);
    assert!(t.backup_takeovers >= 15, "{t:?}");
    assert!(t.ack_floor_waits >= 10, "the successor never waited: {t:?}");
}

/// Plan 30 §M9: backup-acked transactions reach subscribers ahead of S3
/// and are retired by the segments that carry them (or stranded by a
/// takeover); slow S3 makes the window visible.
#[test]
fn pre_s3_streaming_installs_and_retires() {
    let cfg = SimConfig {
        s3_latency: (60, 200),
        ..backup_crash_config()
    };
    let t = run_m9("backup-crash-slow", cfg, 1400..1700);
    assert!(t.streamed_ahead > 20, "{t:?}");
    assert!(t.streamed_installed > 20, "{t:?}");
}

/// Slow S3 and no fault at all (slow-s3-no-seal): every S3 request
/// takes about a twelfth of the lease TTL and the clients keep the
/// holder's journal non-empty, so its rounds ship back to back. No
/// backup ever seals the live holder and nobody takes its lease over:
/// the holder renews between two segments (it used to renew only as a
/// round opened, and a round that never ran dry let the lease lapse).
#[test]
fn slow_s3_never_seals_a_live_holder() {
    let cfg = SimConfig {
        s3_latency: (400, 600),
        clients_per_node: 3,
        ops_per_client: 40,
        random_faults: 0,
        join_fresh: false,
        ..backup_config()
    };
    let t = run_m9("slow-s3-live", cfg, 3000..3016);
    assert!(t.appends > 100, "no backup streaming happened: {t:?}");
    assert_eq!(t.seals, 0, "a live holder was sealed: {t:?}");
    assert_eq!(t.backup_takeovers, 0, "{t:?}");
}

/// Plan 30 §M9's checks are not vacuous: today's `Local` policy with a
/// holder crash does roll acknowledged ops back (plan 30 §1.2's L2
/// window), and the strict-durability check catches it.
#[test]
fn local_policy_rollbacks_are_found() {
    let cfg = SimConfig {
        strict_durability: true,
        ..bug_b_config()
    };
    let mut found = None;
    for seed in 200..260 {
        if let Err(e) = run_seed(seed, cfg.clone()) {
            found = Some((seed, e));
            break;
        }
    }
    let (seed, e) = found.expect("no seed rolled an acknowledged op back under Local");
    eprintln!(
        "local_policy_rollbacks_are_found: seed {seed}: {}",
        e.lines().next().unwrap_or("")
    );
}

fn long_backup_config() -> SimConfig {
    SimConfig {
        strict_durability: true,
        core: std::sync::Arc::new(sim::run::backup_core_config),
        ..long_config()
    }
}

/// Plan 30 §M9 rebase: a forwarded op refused by the holder (ENOENT /
/// EEXIST) was executed a *second* time — by the deposed requester's
/// replay by rid (50126) or by a later holder draining an inbox batch
/// (50064) — and succeeded, after its client had been told the refusal
/// (the strict acknowledgement-order check found both). The holder now
/// journals definitive refusals as outcomes (`Refused { rid, errno }`),
/// so every later execution of the rid dedups to the same errno.
#[test]
fn regression_refused_forward_is_not_re_executed() {
    let acks3 = || SimConfig {
        core: std::sync::Arc::new(sim::run::ack_s3_core_config),
        ..long_backup_config()
    };
    // 50277 (`ack=s3`): the holder's *own* client refused from a stale
    // replica after a fast takeover, because a refusal with nothing
    // unshipped left at once; the local path journals refusals too.
    // 753 (`backup`): the requester's own op arrived on the holder's
    // pre-S3 stream before its reply; the reply's shadow re-applied it
    // over a later streamed create. 1328 (`backup-strict`): the sealed
    // successor's takeover gate waited the delegation horizon out
    // without shipping the re-applied tail, and crashed inside it.
    // 1122 (`backup-departs`), 1407 (`backup-crash-slow`): the
    // requester's own op streamed ahead must count as installed, or the
    // reply sends it down the lease path a second time.
    // Round 2: 50064 stopped journaling a refusal once the holder
    // streamed holes and resent once (the schedule moved); it stays for
    // its strict checks, and 50068 pinned the refusal-journaling path —
    // until the OVH fix answered an accepted forward from the pre-S3
    // stream (the schedule moved again: 50068 journals none now); it
    // stays for its strict checks and 50069 pins the path.
    for (seed, cfg, alias) in [
        (50064u64, long_backup_config(), "long-backup"),
        (50068, long_backup_config(), "long-backup"),
        (50069, long_backup_config(), "long-backup"),
        (50126, long_backup_config(), "long-backup"),
        (50277, acks3(), "long-acks3"),
        (753, backup_config(), "backup"),
        (1328, backup_strict_config(), "backup-strict"),
        (1122, backup_departs_config(), "backup-departs"),
        (
            1407,
            SimConfig {
                s3_latency: (60, 200),
                ..backup_crash_config()
            },
            "backup-crash-slow",
        ),
        // 1402: the mirror case — the reply's shadow arrived first, and
        // the stream's copy must turn it into a streamed entry (retired
        // by the segment's skip), not be dropped.
        (
            1402,
            SimConfig {
                s3_latency: (60, 200),
                ..backup_crash_config()
            },
            "backup-crash-slow",
        ),
        // 50412: a streamed `Refused` / `InboxAck` must still be written
        // (completed row, inbox watermark) when its segment lands, or
        // the next holder's inbox drain re-executes the refused op.
        (50412, long_backup_config(), "long-backup"),
    ] {
        let report = run_seed(seed, cfg).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={alias}")
        });
        assert!(report.converged_checked, "seed {seed} did not converge");
        let journaled: u64 = report.stats.values().map(|s| s.refusals_journaled).sum();
        // (The refusal-journaling seeds; the others pin different paths.)
        assert!(
            journaled >= 1 || !matches!(seed, 50069 | 50126 | 50277),
            "seed {seed} no longer refuses a forward ({journaled})"
        );
    }
}

/// Plan 30 §M6/§M9: an `Exists` hint installed after the holder's pre-S3
/// stream had already carried the refusal here — and what the holder did
/// after it. The requester (a backup, so its reply waited for its own
/// acknowledgement) had `Refused { rid }` and a later rename onto the
/// refused name streamed in; the hint, read at the refusal, put the old
/// entry back over the rename, and the segment (skipping the streamed
/// rows, retiring the hint) left the replica diverged. A hint whose
/// refusal is already streamed is not installed now
/// (`Meta::install_hint_from`; the meta test
/// `a_hint_whose_refusal_was_streamed_first_is_not_installed`). Found by
/// M14's lock sim (seed 194287, pinned in that tree); ~0.15% of `backup`
/// seeds, no fault needed.
#[test]
fn regression_exists_hint_after_its_streamed_refusal() {
    for seed in [600_596u64, 603_050] {
        let report = run_seed(seed, backup_config()).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=backup")
        });
        assert!(report.converged_checked, "seed {seed} did not converge");
    }
}

/// Plan 30 §M9, the backup-crash sweeps (`fix-backup-crash`): one seed
/// per class, each failing before its fix. The core and meta tests named
/// in each comment pin the mechanisms deterministically; these pin the
/// whole-system symptom.
#[test]
fn regression_backup_crash_classes() {
    let slow = || SimConfig {
        s3_latency: (60, 200),
        ..backup_crash_config()
    };
    let long_acks3 = || SimConfig {
        core: std::sync::Arc::new(sim::run::ack_s3_core_config),
        ..long_backup_config()
    };
    for (seed, cfg, alias, what) in [
        // Acknowledged under `Local` while a backup could be had: an
        // eligible peer and no candidate yet (606255), or eligibility
        // not yet assessed in a tenure's first event (603322).
        // `nothing_is_acknowledged_locally_while_a_backup_could_be_had`.
        (
            606_255u64,
            backup_crash_config(),
            "backup-crash",
            "acked, then lost",
        ),
        (603_322, slow(), "backup-crash-slow", "acked, then lost"),
        // The ordering violation a lost acknowledgement leaves.
        (1_454, slow(), "backup-crash-slow", "ordering"),
        // A takeover deposed while it retried its marker finishes its
        // acquisition (the job held the slot for good; nothing tailed).
        // `a_takeover_deposed_while_retrying_its_marker_finishes_the_acquisition`.
        (727_961, backup_config(), "backup", "no quiescence"),
        // The sealed backup's own lease at the next epoch is its landed
        // takeover: its CAS applied, then timed out (603631), or it
        // restarted inside its gate (601692); the tail is re-shipped.
        // `a_sealed_backups_own_lease_at_the_next_epoch_reships_its_tail`,
        // `a_restarted_successor_reships_the_tail_it_sealed`.
        (603_631, slow(), "backup-crash-slow", "acked, then lost"),
        (601_692, slow(), "backup-crash-slow", "acked, then lost"),
        // A replay answers the client retrying its rid.
        // `a_client_retrying_a_rid_under_replay_is_answered_by_the_replay`.
        (50_557, long_acks3(), "long-acks3", "never answered"),
        // The re-shipped tail keeps older observations waiting past the
        // marker, and carries its refusals (607661, 600066).
        // `an_announced_tail_keeps_older_observations_waiting_past_the_marker`,
        // `an_adopted_tails_refusal_and_inbox_ack_are_journaled`.
        (607_661, backup_config(), "backup", "monotonic reads"),
        (600_066, slow(), "backup-crash-slow", "monotonic reads"),
        // A hint is not installed over live speculation on its keys.
        // `a_hint_is_not_installed_over_live_speculation_on_its_keys`.
        (601_075, slow(), "backup-crash-slow", "divergence"),
        // A deadline answers from the applied log, not in doubt.
        // `a_deadline_answers_from_the_applied_log`.
        (700_087, ack_s3_config(), "acks3", "in doubt"),
        // A shipped row is acknowledged though the holder's lease lapsed.
        // `a_shipped_row_is_acknowledged_though_the_lease_lapsed`.
        (801_715, long_acks3(), "long-acks3", "in doubt"),
        // Only a listed backup may claim an expired `Backup` lease until
        // the grace has passed: it holds the acknowledged tail (802943: a
        // non-backup claimed at the expiry; 602011: the backup came back
        // from a crash in time to claim within the grace).
        // `a_non_backup_waits_the_grace_before_claiming_a_backup_lease`.
        (802_943, long_backup_config(), "long-backup", "ordering"),
        (602_011, backup_crash_config(), "backup-crash", "ordering"),
    ] {
        let report = run_seed(seed, cfg).unwrap_or_else(|e| {
            panic!("seed {seed} ({what}): {e}\n  replay with AUTHORITY_SIM_CONFIG={alias}")
        });
        assert!(report.converged_checked, "seed {seed} did not converge");
        assert!(
            report.durability_budget_exceeded.is_empty(),
            "seed {seed} must pass under the strict checks: {:?}",
            report.durability_budget_exceeded
        );
    }
    // Two failures that took every copy of acknowledged rows down across
    // the lease's lapse are beyond one backup's budget (the holder
    // crashed; its sealed successor was paused past its own lease before
    // re-shipping the tail, 609417; its backup crashed and came back too
    // late to claim, 802797): recorded, and the strict checks relaxed for
    // that run only (`Cluster::note_takedown`).
    for seed in [609_417u64, 802_797] {
        let report = run_seed(seed, backup_crash_config()).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=backup-crash")
        });
        assert!(
            !report.durability_budget_exceeded.is_empty(),
            "seed {seed}: the double fault was not recognised"
        );
    }
}

/// The OVH run's finding 4: a forward accepted behind unshipped work on
/// its keys (a non-owning node's close after its create) is answered
/// once the holder's pre-S3 stream installed its transaction here, not
/// when its segment comes back from S3 — and every such seed still
/// passes the linearizability, convergence and strict acknowledgement
/// checks (`run_seed`). The seeds are `long-backup` ones where the path
/// runs.
#[test]
fn an_accepted_forward_is_answered_from_the_pre_s3_stream() {
    let mut answered = 0;
    for seed in [50065u64, 50069, 50073, 50079, 50080] {
        let report = run_seed(seed, long_backup_config()).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=long-backup")
        });
        assert!(report.converged_checked, "seed {seed} did not converge");
        answered += report
            .stats
            .values()
            .map(|s| s.awaited_log_streamed)
            .sum::<u64>();
    }
    assert!(answered > 0, "no forward was answered from the stream");
}

/// Plan 30 §M9 × `chaos-soak-4` (seed 42, `wf293`): backups on, so the
/// holder streams backup-acknowledged transactions ahead of S3 (after a
/// 30 ms hold-off), and every client of four nodes contends on two names
/// without faults. Replies overtake the stream frames of transactions
/// the holder ordered before them, so streamed transactions land under
/// live shadows and hints all the time (`Meta::install_streamed`'s
/// reordering; a root holder's reply `base` keeps an *overlapping*
/// shadow from being installed ahead of the stream, so the reorder
/// itself is pinned by the meta test).
fn backup_hot_config() -> SimConfig {
    SimConfig {
        nodes: 4,
        names: 2,
        clients_per_node: 3,
        ops_per_client: 10,
        random_faults: 0,
        core: std::sync::Arc::new(sim::run::backup_hot_core_config),
        ..backup_config()
    }
}

/// `backup-hot` with the placement on over two directories the four
/// nodes share unevenly (renames crossing between them): the root
/// executes names in a directory before the placement delegates it, and
/// the delegate then answers ops on the same names for nodes whose
/// replicas have not caught up with the root's.
fn placement_hot_config() -> SimConfig {
    SimConfig {
        ops_per_client: 16,
        dirs: vec!["d1".into(), "d2".into()],
        cross_ratio: 0.3,
        check_range_bits: 2,
        read_ratio: 0.3,
        core: std::sync::Arc::new(sim::run::placement_backup_core_config),
        ..backup_hot_config()
    }
}

/// `Meta::install_streamed`'s reordering (chaos-soak-4 seed 42) must
/// leave a hint where it is: its refusal may be streamed already, so what
/// the stream delivers now is later than the state the hint read. A first
/// version redid hints after the streamed transaction and failed these
/// seeds (the hint of `create f1` undid a streamed `rename f1 f0`).
#[test]
fn regression_streamed_reorder_keeps_hints_in_place() {
    for (seed, cfg, alias) in [
        (90_024u64, backup_hot_config(), "backup-hot"),
        (90_039, placement_hot_config(), "placement-hot"),
    ] {
        let report = run_seed(seed, cfg).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={alias}")
        });
        assert!(report.converged_checked, "seed {seed} did not converge");
    }
}

/// The `backup-hot` sweep: `cargo test -p constellation-authority
/// --release --test sim -- --ignored long_backup_hot`
/// (`AUTHORITY_SIM_SEEDS`, `AUTHORITY_SIM_START`; replay one with
/// `AUTHORITY_SIM_CONFIG=backup-hot`).
#[test]
#[ignore]
fn long_backup_hot() {
    let seeds: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let start: u64 = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(90_000);
    let mut failures = Vec::new();
    for seed in start..start + seeds {
        let (cfg, alias) = if seed % 2 == 0 {
            (backup_hot_config(), "backup-hot")
        } else {
            (placement_hot_config(), "placement-hot")
        };
        if let Err(e) = run_seed(seed, cfg) {
            eprintln!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={alias}");
            failures.push(seed);
        }
    }
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}

/// Plan 30 §M9: the long configuration with backups (and, every other
/// seed, `ack=s3`): `cargo test -p constellation-authority --release
/// --test sim -- --ignored long_backup` (`AUTHORITY_SIM_SEEDS`,
/// `AUTHORITY_SIM_START`; replay with `AUTHORITY_SIM_CONFIG=long-backup`).
#[test]
#[ignore]
fn long_backup() {
    let seeds: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let start: u64 = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    let mut failures = Vec::new();
    let mut totals = M9Totals::default();
    for seed in start..start + seeds {
        let (cfg, alias) = if seed % 2 == 0 {
            (long_backup_config(), "long-backup")
        } else {
            (
                SimConfig {
                    core: std::sync::Arc::new(sim::run::ack_s3_core_config),
                    ..long_backup_config()
                },
                "long-acks3",
            )
        };
        match run_seed(seed, cfg) {
            Ok(report) => totals.add(&report),
            Err(e) => {
                eprintln!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={alias}");
                failures.push(seed);
            }
        }
    }
    eprintln!(
        "long_backup: {totals:?}; failover {}",
        totals.failover_dist()
    );
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}

// ---- plan 30 §M10: flexible-quorum continuation epochs ----

/// Three nodes, `f = 1`: the holder and one other lose S3 (and P2P to
/// the third) for 5 s mid-workload; they form an epoch of 2/3 and keep
/// writing, while the third keeps S3 and keeps trying to write — its
/// takeover of the expired lease must be refused (no promise outlasts
/// the expiry: the members are silent). Then S3 returns, the epoch
/// flushes, everyone converges.
///
/// Fix "capture under an epoch hold": clients write the files they
/// create (one chunk each, dirty on the writer: `sim::chunks`), and once
/// the epoch is active a member other than the hold owner dies — with
/// the only copy of a chunk the owner's epoch journal names, when it
/// has one — and returns with its disk 6 s later, after the outage
/// heals. The owner's flush defers exactly that transaction and its
/// dependents, ships the rest, and the write ships once the member's
/// chunk is up. Every run checks that no segment ever names a chunk S3
/// lacks and that the log never stalls behind a deferred transaction.
fn flex_config() -> SimConfig {
    SimConfig {
        ops_per_client: 8,
        random_faults: 0,
        epoch_slack: 1,
        chunk_writes: 0.3,
        core: std::sync::Arc::new(sim::run::flex_core_config),
        faults: vec![
            ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::EpochOutage {
                    members: 2,
                    for_ms: 5_000,
                },
            },
            ScheduledFault {
                at_ms: 2_500,
                kind: FaultKind::CrashEpochMember {
                    restart_ms: Some(6_000),
                },
            },
        ],
        ..SimConfig::default()
    }
}

/// The same with the promise check off: the third node takes the lease
/// the epoch carries, and the sampler sees two authorities.
fn flex_unchecked_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::flex_unchecked_core_config),
        ..flex_config()
    }
}

/// The outage plus a crash of a random node with restart, and the CI's
/// random faults on top. The epoch member that dies (see `flex_config`)
/// never returns here: the run's end applies the operator procedure,
/// `repair drop-held --remote` on every node awaiting its chunks, and
/// the dropped writes become conflict copies (refused replays), never
/// silent loss; everything else converges.
fn flex_crash_config() -> SimConfig {
    let mut faults = flex_config().faults;
    for f in &mut faults {
        if let FaultKind::CrashEpochMember { restart_ms } = &mut f.kind {
            *restart_ms = None;
        }
    }
    SimConfig {
        random_faults: 2,
        read_ratio: 0.3,
        join_fresh: false,
        faults,
        ..flex_config()
    }
}

/// `f = 0` under the same outage: two of three is not a quorum, no epoch
/// forms, the cut nodes stall and the third takes over as today.
fn flex_zero_config() -> SimConfig {
    SimConfig {
        epoch_slack: 0,
        core: std::sync::Arc::new(sim::run::sim_core_config),
        ..flex_config()
    }
}

#[derive(Debug, Default)]
struct FlexTotals {
    seeds: u64,
    epochs: usize,
    missing_node: usize,
    refused: u64,
    promise_checks: u64,
    promise_puts: u64,
    promise_answers: u64,
    flush_exempt: u64,
    samples: u64,
    /// Fix "capture under an epoch hold" (`sim::chunks`).
    chunk_writes: u64,
    chunks_uploaded: u64,
    remote_enrolled: u64,
    remote_acked: u64,
    deferred_seen: u64,
    remote_dropped: u64,
    members_gone: u64,
    converged: u64,
}

fn run_flex(label: &str, cfg: SimConfig, seeds: std::ops::Range<u64>) -> FlexTotals {
    let mut t = FlexTotals::default();
    for seed in seeds {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| {
            panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
        });
        t.seeds += 1;
        t.epochs += report.epochs_formed;
        t.missing_node += report.epochs_missing_node;
        t.samples += report.authority_samples;
        t.chunk_writes += report.chunk_writes;
        t.chunks_uploaded += report.chunks_uploaded;
        t.remote_enrolled += report.remote_enrolled;
        t.remote_acked += report.remote_acked;
        t.deferred_seen += report.deferred_seen;
        t.remote_dropped += report.remote_dropped;
        t.members_gone += report.member_gone as u64;
        t.converged += report.converged_checked as u64;
        for s in report.stats.values() {
            t.refused += s.takeovers_refused_promises;
            t.promise_checks += s.promise_checks;
            t.promise_puts += s.promise_puts;
            t.promise_answers += s.promise_requests_answered;
            t.flush_exempt += s.promise_flush_exempt;
        }
    }
    eprintln!("{label}: {t:?}");
    t
}

/// Plan 30 §M10: the epoch of 2/3 forms and writes, the third node's
/// takeover is refused, nobody ever sees two authorities, the history is
/// linearizable and the replicas converge.
#[test]
fn flex_epoch_with_a_missing_node_is_single_authority() {
    let t = run_flex("flex", flex_config(), 1000..1040);
    assert!(t.epochs >= 30, "epochs rarely formed: {t:?}");
    assert_eq!(t.missing_node, t.epochs, "every epoch had a node missing");
    assert!(
        t.refused > 0,
        "the third node never tried a takeover: {t:?}"
    );
    assert!(t.samples > 0);
}

/// Non-vacuity: without the promise check the sampler finds the third
/// node's S3 lease alongside the epoch's hold.
#[test]
fn flex_without_the_promise_check_is_found() {
    // Either the sampler sees both authorities, or — when the split brain
    // shows in the history first — the log-witnessed linearizability
    // check does (the third node's writes and the epoch's collide).
    let (mut sampled, mut history) = (0, 0);
    for seed in 1000..1040 {
        match run_seed(seed, flex_unchecked_config()) {
            Err(e) if e.contains("two authorities at once") => sampled += 1,
            Err(e) if e.contains("quiescence") => {
                panic!("flex-unchecked seed {seed} failed otherwise: {e}")
            }
            Err(_) => history += 1,
            Ok(_) => {}
        }
    }
    eprintln!(
        "flex-unchecked: {sampled}/40 seeds sampled two authorities, {history}/40 more \
         broke linearizability first"
    );
    assert!(sampled > 0, "the sampler never saw the split brain");
}

/// Plan 30 §M10 with crashes, reads and the random CI faults.
#[test]
fn flex_epochs_survive_crashes_and_faults() {
    let t = run_flex("flex-crash", flex_crash_config(), 1100..1160);
    assert!(t.epochs > 0, "{t:?}");
}

/// Fix "capture under an epoch hold": in `flex_config` a member dies
/// with the only copy of a chunk the hold owner's epoch journal names
/// and returns after the outage. The owner journals captured under the
/// hold, so its flush defers only that write and its dependents and the
/// log keeps moving (every run fails on a stall behind a deferred
/// transaction, and on a segment naming a chunk S3 lacks); the write
/// ships once the member's chunk is up, and everything converges.
#[test]
fn flex_a_member_dies_with_the_only_copy_of_a_chunk_and_returns() {
    let t = run_flex("flex", flex_config(), 1000..1040);
    assert!(t.chunk_writes > 0 && t.chunks_uploaded > 0, "{t:?}");
    assert!(
        t.remote_enrolled > 0,
        "no forwarded manifest ever named a chunk pending on its sender: {t:?}"
    );
    assert!(
        t.deferred_seen > 0,
        "no ship plan ever deferred a transaction on a member's chunk: {t:?}"
    );
    assert_eq!(t.members_gone, 0, "{t:?}");
    assert_eq!(t.remote_dropped, 0, "nothing needs dropping: {t:?}");
    assert_eq!(t.converged, t.seeds, "every seed converges: {t:?}");
}

/// The same member never returns (`flex_crash_config`): the log still
/// moves, and at the end the operator procedure drops what waited for
/// its chunks into conflict copies — the cluster is then quiescent and
/// converged on the log, with no silent loss (each dropped write is a
/// refused replay).
#[test]
fn flex_crash_a_member_gone_for_good_is_dropped_by_the_operator() {
    let t = run_flex("flex-crash", flex_crash_config(), 1100..1160);
    assert!(t.members_gone > 0, "{t:?}");
    assert!(
        t.remote_dropped > 0,
        "no run ever had a transaction to drop for a departed member: {t:?}"
    );
    assert!(t.deferred_seen > 0, "{t:?}");
    assert_eq!(t.converged, t.seeds, "every seed converges: {t:?}");
}

/// Plan 30 §M10's documented gap: a node enrolled *during* an epoch is
/// outside the roster the epoch formed under, so its promise counts for a
/// taker although the epoch's members never saw it — the intersection
/// needs `f + 1` promisers from the formation roster. Here node 4 enrolls
/// during the outage, the epoch's hold owner dies, node 3 counts node 4's
/// promise, takes the lease over, and the hold owner comes back. Kept as
/// a repro (`#[ignore]`): it is expected to FAIL until the gap is closed.
#[test]
#[ignore]
fn flex_node_enrolled_during_an_epoch_is_the_known_gap() {
    let cfg = SimConfig {
        faults: vec![
            ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::EpochOutage {
                    members: 2,
                    for_ms: 5_000,
                },
            },
            ScheduledFault {
                at_ms: 3_500,
                kind: FaultKind::JoinFresh,
            },
            ScheduledFault {
                at_ms: 4_800,
                kind: FaultKind::CrashHolder {
                    restart_ms: Some(6_000),
                    keep_journal: true,
                },
            },
        ],
        ..flex_config()
    };
    let mut found = 0;
    for seed in 30_000..30_040 {
        if let Err(e) = run_seed(seed, cfg.clone()) {
            eprintln!("seed {seed}: {}", e.lines().next().unwrap_or(""));
            found += 1;
        }
    }
    eprintln!("enrolled-during-epoch: {found}/40 seeds failed");
    assert!(found > 0, "the known gap did not show");
}

/// Plan 30 §M10 × M9: backups in budget during the outage.
fn flex_backup_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::flex_backup_core_config),
        read_ratio: 0.3,
        ..flex_config()
    }
}

/// The outage with M9's backups in budget: the epoch carries the holder's
/// `Backup` lease only when its backup is the other member, and then
/// acknowledges locally (an epoch hold is never gated on the backups —
/// found by `node-leave` after M9's round 2).
#[test]
fn flex_epochs_with_backups() {
    let t = run_flex("flex-backup", flex_backup_config(), 1200..1240);
    assert!(t.epochs > 0, "{t:?}");
}

/// `f = 0` is today's rule: two of three never form an epoch.
#[test]
fn flex_zero_forms_no_partial_epoch() {
    let t = run_flex("flex-zero", flex_zero_config(), 1000..1020);
    assert_eq!(t.epochs, 0, "{t:?}");
}

/// Plan 30 §M9 × §M10, the shape `deposed-reintegration` found: only
/// the holder loses S3 (P2P stays up), it proposes a continuation epoch,
/// and it dies (restarted only 9 s later). Its backup must take the
/// lease over well inside that — which it can only while it is not a
/// member of an open epoch (a member runs no seal watch and no S3
/// acquisition, and a frozen epoch answers `EROFS`).
fn holder_cut_crash_config(decline: bool) -> SimConfig {
    SimConfig {
        // Enough work that the other nodes' clients are still writing
        // when the holder dies: the failover is measured to the next
        // answered op, and with 8 each they had all finished before it
        // in seed 1513 once the pre-S3 stream answered their forwards
        // sooner — the next op was the restarted holder's, at 9 s.
        ops_per_client: 12,
        random_faults: 0,
        faults: vec![
            ScheduledFault {
                at_ms: 1_100,
                kind: FaultKind::HolderCutEpoch {
                    for_ms: 8_000,
                    decline,
                },
            },
            ScheduledFault {
                at_ms: 2_000,
                kind: FaultKind::CrashHolder {
                    restart_ms: Some(9_000),
                    keep_journal: true,
                },
            },
        ],
        ..backup_config()
    }
}

/// With the member rule (a member that reaches S3 declines the
/// proposal), no epoch forms and the backup seals and fails over in
/// about `backup_takeover_ms`; without it (`decline: false`) the backup
/// joins, freezes when the holder dies, and waits for the holder's
/// return — the stuck peer, reproduced (non-vacuity).
#[test]
fn a_holder_alone_cut_from_s3_forms_no_epoch_and_fails_over() {
    let restart = 9_000;
    let mut epochs = 0;
    let mut failovers = Vec::new();
    for seed in 1500..1520 {
        let report = run_seed(seed, holder_cut_crash_config(true)).unwrap_or_else(|e| {
            panic!("holder-cut seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=holder-cut")
        });
        epochs += report.epochs_formed;
        failovers.extend(report.failover_ms.iter().copied());
    }
    eprintln!("holder-cut: epochs {epochs}, failovers {failovers:?}");
    assert_eq!(epochs, 0, "a member that reaches S3 joined an epoch");
    assert!(!failovers.is_empty(), "no failover measured");
    let ttl = sim::run::backup_core_config(1, 1).ttl_ms;
    assert!(
        failovers.iter().all(|ms| *ms < ttl.min(restart)),
        "a failover waited for the TTL or the holder's return: {failovers:?}"
    );

    let mut epochs = 0;
    let mut stuck = 0;
    for seed in 1500..1510 {
        let report = run_seed(seed, holder_cut_crash_config(false)).unwrap_or_else(|e| {
            panic!(
                "holder-cut-join seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=holder-cut-join"
            )
        });
        epochs += report.epochs_formed;
        stuck += report
            .failover_ms
            .iter()
            .filter(|ms| **ms >= restart - 1_000)
            .count();
    }
    eprintln!("holder-cut-join: epochs {epochs}, stuck failovers {stuck}");
    assert!(epochs > 0, "without the rule the peers never joined");
    assert!(stuck > 0, "without the rule no peer was ever stuck");
}

/// M12 round 2 × 623da2c: the holder-cut shape with live delegations
/// (the delegates are the members that reach S3 and decline the cut
/// root's proposal; the root dies; its backup takes over inside the
/// TTL and inherits the delegation table from the log, the delegates
/// re-stream to it). No epoch, a failover well inside the restart,
/// every directory's history linearizable (`run_seed`'s checks).
fn delegated_holder_cut_config() -> SimConfig {
    SimConfig {
        faults: holder_cut_crash_config(true).faults,
        read_ratio: 0.0,
        ..delegated_backup_config()
    }
}

#[test]
fn a_delegating_root_cut_from_s3_forms_no_epoch_and_fails_over() {
    let restart = 9_000;
    let mut epochs = 0;
    let mut failovers = Vec::new();
    let mut executed = 0;
    for seed in 1600..1612 {
        let report = run_seed(seed, delegated_holder_cut_config()).unwrap_or_else(|e| {
            panic!(
                "delegated-holder-cut seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=delegated-holder-cut"
            )
        });
        epochs += report.epochs_formed;
        failovers.extend(report.failover_ms.iter().copied());
        executed += report.stats.values().map(|s| s.deleg_executed).sum::<u64>();
    }
    eprintln!(
        "delegated-holder-cut: epochs {epochs}, failovers {failovers:?}, executed {executed}"
    );
    assert_eq!(epochs, 0, "a delegate that reaches S3 joined an epoch");
    assert!(executed > 0, "the delegates never executed");
    assert!(!failovers.is_empty(), "no failover measured");
    let ttl = sim::run::backup_core_config(1, 1).ttl_ms;
    assert!(
        failovers.iter().all(|ms| *ms < ttl.min(restart)),
        "a failover waited for the TTL or the holder's return: {failovers:?}"
    );
}

/// The `delegated-holder-cut` seeds outside the CI range that failed on
/// main (1528 of 0..20000: 1200 never quiescent, the rest history
/// violations), one per failure shape. The root's backup is also a
/// delegate (three nodes), so its takeover re-ships a tail holding the
/// predecessor's appends of delegate streams:
/// - the re-shipped rows lost their delegation origin (`BackupTx` had
///   none), so the log carried them as the successor's own: the delegate
///   never retired its rows (seed 2: never quiescent), and every later
///   segment's insert-before-`Local` redo re-applied them over newer
///   state (seed 1719: a stale `unlink f0` deleted the re-created `f0`;
///   22, 330);
/// - the successor's own rows as a delegate sat in its journal ahead of
///   the tail it re-applied and shipped before it (seeds 772, 1039: a
///   create in `d1` ahead of the tail's rename of the name out of `d1`).
#[test]
fn regression_delegated_holder_cut_takeover_keeps_delegate_rows_in_log_order() {
    let mut rolled_back = 0;
    for seed in [2, 22, 330, 1719, 772, 1039] {
        let report = run_seed(seed, delegated_holder_cut_config()).unwrap_or_else(|e| {
            panic!(
                "delegated-holder-cut seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=delegated-holder-cut"
            )
        });
        if seed == 772 || seed == 1039 {
            rolled_back += report
                .stats
                .values()
                .map(|s| s.local_rolled_back)
                .sum::<u64>();
        }
    }
    assert!(
        rolled_back > 0,
        "the takeover never stranded the successor's own delegate rows"
    );
}

/// Many seeds of a named configuration, run in parallel
/// (`AUTHORITY_SIM_CONFIG`, default `delegated-holder-cut`;
/// `AUTHORITY_SIM_START`, `AUTHORITY_SIM_SEEDS`, `AUTHORITY_SIM_THREADS`).
/// Prints every failing seed's first lines and fails at the end.
#[test]
#[ignore]
fn sweep_config() {
    let env = |k: &str, d: u64| {
        std::env::var(k)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(d)
    };
    let start = env("AUTHORITY_SIM_START", 0);
    let seeds = env("AUTHORITY_SIM_SEEDS", 2_000);
    let threads = env("AUTHORITY_SIM_THREADS", 8);
    let label = std::env::var("AUTHORITY_SIM_CONFIG").unwrap_or("delegated-holder-cut".into());
    let config = |label: &str| match label {
        "delegated-holder-cut" => delegated_holder_cut_config(),
        "long-delegated" => long_delegated_config(),
        "long-backup" => long_backup_config(),
        "delegated-backup" => delegated_backup_config(),
        "long-delegated-backup" => long_delegated_backup_config(),
        "placement-hot" => placement_hot_config(),
        "backup-hot" => backup_hot_config(),
        "flex" => flex_config(),
        "flex-crash" => flex_crash_config(),
        other => panic!("sweep_config: unknown config {other}"),
    };
    let streamed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let streamed_deleg = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let epoch_streamed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Fix "capture under an epoch hold": [remote rows enrolled, ship
    // plans seen deferring, transactions dropped for a departed member].
    let chunks = std::sync::Arc::new([
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
    ]);
    let next = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(start));
    let failures = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let (next, failures, label) = (next.clone(), failures.clone(), label.clone());
        let (streamed, streamed_deleg) = (streamed.clone(), streamed_deleg.clone());
        let epoch_streamed = epoch_streamed.clone();
        let chunks = chunks.clone();
        handles.push(std::thread::spawn(move || loop {
            let seed = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if seed >= start + seeds {
                break;
            }
            match run_seed(seed, config(&label)) {
                Ok(report) => {
                    let ord = std::sync::atomic::Ordering::Relaxed;
                    for s in report.stats.values() {
                        streamed.fetch_add(s.awaited_log_streamed, ord);
                        streamed_deleg.fetch_add(s.awaited_log_streamed_deleg, ord);
                        epoch_streamed.fetch_add(s.epoch_streamed_installed, ord);
                    }
                    chunks[0].fetch_add(report.remote_enrolled, ord);
                    chunks[1].fetch_add(report.deferred_seen, ord);
                    chunks[2].fetch_add(report.remote_dropped, ord);
                }
                Err(e) => {
                    let head: String = e.lines().take(3).collect::<Vec<_>>().join(" | ");
                    eprintln!("SWEEP-FAIL {label} seed {seed}: {head}");
                    failures.lock().unwrap().push(seed);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let mut f = failures.lock().unwrap().clone();
    f.sort_unstable();
    eprintln!(
        "sweep {label} {start}..{}: {} failing: {f:?}; answered from the stream: {} ({} a delegate's); \
         installed from an epoch's stream: {}; remote chunks enrolled: {}, ship plans deferring: {}, \
         dropped for a departed member: {}",
        start + seeds,
        f.len(),
        streamed.load(std::sync::atomic::Ordering::Relaxed),
        streamed_deleg.load(std::sync::atomic::Ordering::Relaxed),
        epoch_streamed.load(std::sync::atomic::Ordering::Relaxed),
        chunks[0].load(std::sync::atomic::Ordering::Relaxed),
        chunks[1].load(std::sync::atomic::Ordering::Relaxed),
        chunks[2].load(std::sync::atomic::Ordering::Relaxed),
    );
    assert!(f.is_empty(), "failing seeds: {f:?}");
}

/// Plan 30 §M10: many more seeds of the epoch configurations.
#[test]
#[ignore]
fn long_flex() {
    let n: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    run_flex("flex", flex_config(), 20_000..20_000 + n);
    run_flex("flex-crash", flex_crash_config(), 30_000..30_000 + n);
}

// ===================================================================
// Plan 30 §M11: delegated sub-sequencers.

/// Three nodes; node 1 holds the lease (the root) and delegates `d1`
/// to node 2 and `d2` to node 3 before the clients start. Each node's
/// clients write in their home directory (node 1 in the root
/// directory), a third of the renames cross into another directory
/// (recalled to the root), reads follow the session wait.
fn delegated_config() -> SimConfig {
    SimConfig {
        nodes: 3,
        clients_per_node: 2,
        ops_per_client: 8,
        names: 4,
        random_faults: 0,
        read_ratio: 0.5,
        dirs: vec!["d1".into(), "d2".into()],
        delegations: vec![("d1".into(), 2), ("d2".into(), 3)],
        cross_ratio: 0.3,
        ..SimConfig::default()
    }
}

/// Marker pairs: every node writes data in its home directory and a
/// marker in the next one; watchers on every node check.
fn delegated_marker_config() -> SimConfig {
    SimConfig {
        marker_pairs: 3,
        cross_ratio: 0.0,
        ..delegated_config()
    }
}

/// The delegate of `d1` dies mid-burst (no backup): the root reclaims
/// its expired grant, the requesters' acknowledged ops replay by rid
/// through the root, and the node comes back with its journal.
fn delegated_crash_config() -> SimConfig {
    SimConfig {
        // No cross-subtree ops: the grant is live when the node dies.
        cross_ratio: 0.0,
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::CrashNode {
                node: 2,
                restart_ms: Some(9_000),
                keep_journal: true,
            },
        }],
        ..delegated_config()
    }
}

/// The delegate of `d1` is cut from the root for longer than its grant:
/// recalls are outwaited, its later writes are refused locally and go
/// through the root, no divergence.
fn delegated_partition_config() -> SimConfig {
    SimConfig {
        cross_ratio: 0.0,
        faults: vec![ScheduledFault {
            at_ms: 1_000,
            kind: FaultKind::Partition {
                a: 1,
                b: 2,
                for_ms: 9_000,
            },
        }],
        ..delegated_config()
    }
}

/// A continuation epoch forms under delegation (`f = 1`, the M10
/// outage): the root recalls every generation, delegates stop, and
/// everything stays linearizable and converged.
fn delegated_epoch_config() -> SimConfig {
    SimConfig {
        epoch_slack: 1,
        core: std::sync::Arc::new(sim::run::flex_core_config),
        faults: vec![ScheduledFault {
            at_ms: 1_200,
            kind: FaultKind::EpochOutage {
                members: 2,
                for_ms: 5_000,
            },
        }],
        ..delegated_config()
    }
}

/// The CI's random faults on top of delegation (root crashes included:
/// a successor root learns the generations from the log).
fn delegated_faults_config() -> SimConfig {
    SimConfig {
        random_faults: 2,
        ..delegated_config()
    }
}

// ---- phase 2b configurations ----

/// The root dies mid-stream with two live delegates (restarts with its
/// journal): the successor learns the table from the log, the delegates
/// re-stream, nothing acknowledged is lost.
fn delegated_root_crash_config() -> SimConfig {
    SimConfig {
        cross_ratio: 0.1,
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::CrashHolder {
                restart_ms: Some(5_000),
                keep_journal: true,
            },
        }],
        ..delegated_config()
    }
}

/// Delegates with backups (the M9 backup configuration): every
/// acknowledgement waits for the backup.
fn delegated_backup_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::deleg_backup_core_config),
        ..delegated_config()
    }
}

/// The delegate of `d1` dies with a backup: the root seals the backup,
/// drains its tail, ends the generation and delegates `d1` to the
/// backup; every backup-acknowledged write is in the log.
fn delegated_backup_crash_config() -> SimConfig {
    SimConfig {
        cross_ratio: 0.0,
        faults: vec![
            // The delegate loses the root first: what it acknowledges
            // through its backup from here on is exactly the tail the
            // root must drain from the backup after the crash.
            ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::Partition {
                    a: 1,
                    b: 2,
                    for_ms: 2_000,
                },
            },
            ScheduledFault {
                at_ms: 1_500,
                kind: FaultKind::CrashNode {
                    node: 2,
                    restart_ms: Some(9_000),
                    keep_journal: true,
                },
            },
        ],
        ..delegated_backup_config()
    }
}

/// `d1` is an offline designation of node 2: never recalled, cross ops
/// touching it refused (`EXDEV`), the designee's writes local; node 2
/// is cut from the root for longer than a grant and keeps writing.
fn delegated_designated_config() -> SimConfig {
    SimConfig {
        dirs: vec!["d1".into(), "d2".into()],
        delegations: vec![("d2".into(), 3)],
        designations: vec![("d1".into(), 2)],
        cross_ratio: 0.2,
        faults: vec![ScheduledFault {
            at_ms: 1_000,
            kind: FaultKind::Partition {
                a: 1,
                b: 2,
                for_ms: 9_000,
            },
        }],
        ..delegated_config()
    }
}

/// No operator: the placement delegates each node's home directory to
/// it once it dominates the writes there, and recalls when the pattern
/// changes.
fn delegated_placement_config() -> SimConfig {
    SimConfig {
        delegations: Vec::new(),
        cross_ratio: 0.0,
        ops_per_client: 16,
        // M12: the placement may split a shared directory; per range.
        check_range_bits: 2,
        core: std::sync::Arc::new(sim::run::placement_core_config),
        ..delegated_config()
    }
}

/// Plan 30 §M12: four nodes, every client writing names of its own in
/// one shared directory; no delegation at all (the single sequencer):
/// the commutative parent attributes alone.
fn shared_dir_config() -> SimConfig {
    SimConfig {
        nodes: 4,
        clients_per_node: 2,
        ops_per_client: 8,
        names: 3,
        dirs: vec!["shared".into(), "other".into()],
        delegations: Vec::new(),
        shared_dir: true,
        cross_ratio: 0.0,
        ..delegated_config()
    }
}

/// Plan 30 §M12: the same workload with the placement on: the root
/// splits `shared` into hash ranges delegated to the writers.
fn shared_dir_split_config() -> SimConfig {
    SimConfig {
        ops_per_client: 20,
        names: 6,
        check_range_bits: 2,
        core: std::sync::Arc::new(sim::run::placement_core_config),
        ..shared_dir_config()
    }
}

/// M12 round 2: the split workload with the daemon's FUSE fast path
/// modelled — the root executes its own ops outside the core, admitted
/// by `Meta::root_fast_path` (harness `chaos-soak-4`: four nodes racing
/// on the same names, the placement splitting the directory under them).
fn shared_dir_fast_path_config() -> SimConfig {
    SimConfig {
        fast_path: sim::run::FastPath::Checked,
        ..shared_dir_split_config()
    }
}

/// The same with the admission skipped: the daemon before round 2.
fn shared_dir_fast_path_unchecked_config() -> SimConfig {
    SimConfig {
        fast_path: sim::run::FastPath::Unchecked,
        ..shared_dir_split_config()
    }
}

/// Plan 30 §M12: `shared` split two ways by hand (node 2 the low range,
/// node 3 the high one), renames within the directory crossing ranges:
/// the root's recall path.
fn shared_dir_ranges_config() -> SimConfig {
    SimConfig {
        range_delegations: vec![("shared".into(), 2, (1, 0)), ("shared".into(), 3, (1, 1))],
        ops_per_client: 10,
        check_range_bits: 1,
        ..shared_dir_config()
    }
}

/// The hand split under random faults (crashes, partitions, a root
/// failover with live range delegates).
fn shared_dir_faults_config() -> SimConfig {
    SimConfig {
        random_faults: 2,
        ..shared_dir_ranges_config()
    }
}

/// Plan 30 §M8 under delegation: strict reads are answered by the
/// owning delegate (a ReadIndex, or a read delegation from it).
fn delegated_strict_config() -> SimConfig {
    SimConfig {
        strict: true,
        read_ratio: 0.6,
        ..delegated_config()
    }
}

fn long_delegated_config() -> SimConfig {
    SimConfig {
        ops_per_client: 12,
        random_faults: 3,
        p2p_drop: 0.02,
        marker_pairs: 2,
        ..delegated_config()
    }
}

/// `long-delegated` with backups in budget for the root and every
/// delegate: the root streams its journal — its appends of the delegate
/// streams included — ahead of S3, and a delegate's reply a requester
/// cannot install is answered from that stream.
fn long_delegated_backup_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::deleg_backup_core_config),
        ..long_delegated_config()
    }
}

#[derive(Debug, Default, Clone)]
struct M11Totals {
    seeds: u64,
    fast_path_executed: u64,
    executed: u64,
    forwarded: u64,
    appended: u64,
    streamed: u64,
    deps_waits: u64,
    cross_subtree: u64,
    recalls_sent: u64,
    recalls_drained: u64,
    recalls_expired: u64,
    reclaimed: u64,
    ended: u64,
    installed: u64,
    not_owner: u64,
    exec_parked: u64,
    stranded: u64,
    marker_checks: u64,
    dirs_checked: usize,
    epochs: usize,
    // ---- phase 2b ----
    inherited: u64,
    restreams: u64,
    seals_sent: u64,
    sealed_drained: u64,
    backup_appends: u64,
    backup_acks: u64,
    acks_parked: u64,
    place_delegated: u64,
    place_recalled: u64,
    place_splits: u64,
    place_range_recalls: u64,
    redelegated: u64,
    refused_designated: u64,
    designated: u64,
    deleg_reads: u64,
    deleg_read_grants: u64,
}

impl M11Totals {
    fn add(&mut self, r: &Report) {
        self.fast_path_executed += r.fast_path_executed;
        self.seeds += 1;
        for s in r.stats.values() {
            self.executed += s.deleg_executed;
            self.forwarded += s.deleg_forwarded;
            self.appended += s.deleg_appended_txs;
            self.streamed += s.deleg_streamed_txs;
            self.deps_waits += s.deleg_deps_waits;
            self.cross_subtree += s.deleg_cross_subtree;
            self.recalls_sent += s.deleg_recalls_sent;
            self.recalls_drained += s.deleg_recalls_drained;
            self.recalls_expired += s.deleg_recalls_expired;
            self.reclaimed += s.deleg_reclaimed;
            self.ended += s.deleg_ended;
            self.installed += s.deleg_installed;
            self.not_owner += s.deleg_not_owner;
            self.exec_parked += s.deleg_exec_parked;
            self.stranded += s.local_rolled_back;
            self.inherited += s.deleg_inherited;
            self.restreams += s.deleg_restreams;
            self.seals_sent += s.deleg_seals_sent;
            self.sealed_drained += s.deleg_sealed_drained;
            self.backup_appends += s.deleg_backup_appends;
            self.backup_acks += s.deleg_backup_acks;
            self.acks_parked += s.deleg_acks_parked;
            self.place_delegated += s.place_delegated;
            self.place_recalled += s.place_recalled;
            self.place_splits += s.place_splits;
            self.place_range_recalls += s.place_range_recalls;
            self.redelegated += s.deleg_redelegated;
            self.refused_designated += s.deleg_refused_designated;
            self.designated += s.deleg_designated;
            self.deleg_reads += s.deleg_read_index_served;
            self.deleg_read_grants += s.deleg_read_grants;
        }
        self.marker_checks += r.marker_checks;
        self.dirs_checked += r.dirs_checked;
        self.epochs += r.epochs_formed;
    }
}

fn run_m11(label: &str, cfg: SimConfig, seeds: std::ops::Range<u64>) -> M11Totals {
    let mut totals = M11Totals::default();
    for seed in seeds {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| {
            panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
        });
        totals.add(&report);
    }
    eprintln!("{label}: {totals:?}");
    totals
}

/// Plan 30 §M11: delegates execute their subtrees locally, the root
/// appends their streams and recalls for cross-subtree renames; every
/// directory's history is linearizable, the log valid, replicas
/// converge, and the root never appends a batch whose deps it lacks.
#[test]
fn delegated_subtrees_are_linearizable_per_directory() {
    let t = run_m11("delegated", delegated_config(), 60_000..60_040);
    assert!(t.executed > 200, "delegates rarely executed: {t:?}");
    assert!(t.appended > 200, "the root rarely appended: {t:?}");
    assert!(t.cross_subtree > 20, "no cross-subtree ops: {t:?}");
    assert!(t.recalls_drained > 10, "recalls never drained: {t:?}");
    assert!(
        t.dirs_checked >= 3 * t.seeds as usize,
        "per-directory checks missing: {t:?}"
    );
}

/// Plan 30 §M11: data in one delegated directory, a marker in another;
/// no replica ever shows the marker without the data.
#[test]
fn delegated_marker_order_holds() {
    let t = run_m11(
        "delegated-marker",
        delegated_marker_config(),
        61_000..61_030,
    );
    assert!(t.marker_checks > 1_000, "markers rarely checked: {t:?}");
    assert!(t.deps_waits > 0, "the deps wait was never exercised: {t:?}");
}

/// Plan 30 §M11: the delegate crashes without a backup; the root
/// reclaims the grant, requesters replay by rid, the node returns.
#[test]
fn delegated_delegate_crash_replays_by_rid() {
    let t = run_m11("delegated-crash", delegated_crash_config(), 62_000..62_030);
    assert!(
        t.reclaimed + t.recalls_expired > 10,
        "the grant was never reclaimed: {t:?}"
    );
    assert!(t.ended > 10, "no generation ended: {t:?}");
}

/// Plan 30 §M11: the delegate is partitioned from the root; a recall is
/// outwaited by the grant's expiry, the delegate stops on its own clock
/// first, its later writes go through the root.
#[test]
fn delegated_partition_outwaits_the_recall() {
    let t = run_m11(
        "delegated-partition",
        delegated_partition_config(),
        63_000..63_030,
    );
    assert!(
        t.recalls_expired + t.reclaimed > 5,
        "no recall was outwaited: {t:?}"
    );
}

/// Plan 30 §M10 × §M11: an epoch forms; every delegation is recalled.
#[test]
fn delegated_epoch_recalls_every_generation() {
    let t = run_m11("delegated-epoch", delegated_epoch_config(), 64_000..64_020);
    assert!(t.epochs > 0, "no epoch formed: {t:?}");
    assert!(
        t.ended >= t.epochs as u64,
        "delegations survived an epoch: {t:?}"
    );
}

/// Plan 30 §M11: the CI's random faults on top of delegation.
#[test]
fn delegated_under_random_faults() {
    let t = run_m11(
        "delegated-faults",
        delegated_faults_config(),
        65_000..65_040,
    );
    assert!(t.executed > 100, "delegates rarely executed: {t:?}");
}

/// Phase 2b: root failover with live delegates.
#[test]
fn delegated_root_failover_keeps_every_ack() {
    let t = run_m11(
        "delegated-root-crash",
        delegated_root_crash_config(),
        66_000..66_030,
    );
    assert!(
        t.inherited > 0,
        "no successor inherited a generation: {t:?}"
    );
    assert!(t.restreams > 0, "no delegate re-streamed: {t:?}");
}

/// Phase 2b: delegates with backups; acknowledgements wait for them.
#[test]
fn delegated_backups_gate_acknowledgements() {
    let t = run_m11(
        "delegated-backup",
        delegated_backup_config(),
        67_000..67_020,
    );
    assert!(
        t.backup_appends > 0 && t.backup_acks > 0,
        "no backup traffic: {t:?}"
    );
    assert!(
        t.acks_parked > 0,
        "no acknowledgement waited for a backup: {t:?}"
    );
}

/// Phase 2b: a delegate with a backup dies: seal, drain, re-delegate.
#[test]
fn delegated_delegate_crash_drains_the_backup() {
    let t = run_m11(
        "delegated-backup-crash",
        delegated_backup_crash_config(),
        68_000..68_020,
    );
    assert!(t.seals_sent > 0, "no backup was sealed: {t:?}");
    assert!(t.sealed_drained > 0, "no sealed tail was drained: {t:?}");
}

/// Phase 2b: designations are never recalled by time; cross ops touching
/// them are refused; the isolated designee keeps writing.
#[test]
fn delegated_designation_is_never_reclaimed() {
    let t = run_m11(
        "delegated-designated",
        delegated_designated_config(),
        69_000..69_020,
    );
    assert!(t.designated > 0, "nothing was designated: {t:?}");
    assert!(t.refused_designated > 0, "no cross op was refused: {t:?}");
    assert_eq!(
        t.reclaimed + t.recalls_expired,
        0,
        "a designation was reclaimed or outwaited: {t:?}"
    );
}

/// Phase 2b: the placement delegates dominated subtrees by itself.
#[test]
fn delegated_placement_delegates_dominated_subtrees() {
    let t = run_m11(
        "delegated-placement",
        delegated_placement_config(),
        70_500..70_520,
    );
    assert!(
        t.place_delegated > 0,
        "the placement never delegated: {t:?}"
    );
    assert!(t.executed > 0, "placed delegates never executed: {t:?}");
}

/// Plan 30 §M12: a shared directory with the single sequencer — the
/// commutative parent attributes converge on every replica (the
/// convergence check compares the parents' times and link counts).
#[test]
fn shared_dir_single_sequencer_converges() {
    let t = run_m11("shared-dir", shared_dir_config(), 72_000..72_020);
    assert!(
        t.dirs_checked >= t.seeds as usize,
        "per-directory checks missing: {t:?}"
    );
}

/// Plan 30 §M12: the placement splits the hot shared directory into
/// hash ranges; the delegates execute their ranges locally; every
/// directory's history is linearizable and the replicas converge.
#[test]
fn shared_dir_is_split_into_ranges() {
    let t = run_m11(
        "shared-dir-split",
        shared_dir_split_config(),
        73_000..73_020,
    );
    assert!(t.place_splits > 0, "the placement never split: {t:?}");
    assert!(t.executed > 0, "range delegates never executed: {t:?}");
}

/// M12 round 2: the root's fast path executes what the root owns and
/// sends what a range's delegate owns through the core; every
/// directory's history stays linearizable under the split.
#[test]
fn shared_dir_fast_path_respects_the_ranges() {
    let t = run_m11(
        "shared-dir-fast-path",
        shared_dir_fast_path_config(),
        76_000..76_020,
    );
    assert!(t.place_splits > 0, "the placement never split: {t:?}");
    assert!(t.executed > 0, "range delegates never executed: {t:?}");
    assert!(
        t.fast_path_executed > 0,
        "the fast path never executed: {t:?}"
    );
}

/// M12 round 2: without the admission the root executes, on its fast
/// path, names the log has given to a range's delegate — two winners
/// for one create or unlink (the tester's `chaos-soak-4` under the
/// placement). The checker catches it on these seeds (a few in sixty:
/// which seeds hit the window moves with any change of schedule, such as
/// the mid-ship lease renewal, so the range is wide).
#[test]
fn shared_dir_unchecked_fast_path_is_caught() {
    let mut caught = 0;
    for seed in 77_000..77_060 {
        if let Err(e) = run_seed(seed, shared_dir_fast_path_unchecked_config()) {
            eprintln!("seed {seed}: {}", e.lines().next().unwrap_or(""));
            caught += 1;
        }
    }
    assert!(caught > 0, "the unchecked fast path was never caught");
}

/// Plan 30 §M12: a hand split with cross-range renames: recalled by the
/// root, re-delegated after.
#[test]
fn shared_dir_ranges_survive_cross_range_renames() {
    let t = run_m11(
        "shared-dir-ranges",
        shared_dir_ranges_config(),
        74_000..74_020,
    );
    assert!(t.executed > 0, "range delegates never executed: {t:?}");
    assert!(t.cross_subtree > 0, "no cross-range op: {t:?}");
    assert!(t.redelegated > 0, "ranges never re-delegated: {t:?}");
}

/// Plan 30 §M12: the hand split under random faults.
#[test]
fn shared_dir_ranges_under_random_faults() {
    let t = run_m11(
        "shared-dir-faults",
        shared_dir_faults_config(),
        75_000..75_030,
    );
    assert!(t.executed > 0, "range delegates never executed: {t:?}");
}

/// Phase 2b (M8): strict reads under delegation are served by the owning
/// delegate.
#[test]
fn delegated_strict_reads_are_served_by_the_delegate() {
    let t = run_m11(
        "delegated-strict",
        delegated_strict_config(),
        71_000..71_020,
    );
    assert!(
        t.deleg_reads > 0,
        "no strict read reached a delegate: {t:?}"
    );
}

/// Plan 30 §M11 (single-node-unchanged / p2p-off-no-delegation): the
/// ordinary configurations never write a `Delegate` record and never
/// park an execution on a delegation.
#[test]
fn no_delegation_without_a_delegate_record() {
    for (label, cfg) in [
        (
            "single",
            SimConfig {
                nodes: 1,
                clients_per_node: 2,
                ops_per_client: 10,
                random_faults: 0,
                ..SimConfig::default()
            },
        ),
        (
            "inbox",
            SimConfig {
                core: std::sync::Arc::new(sim::run::inbox_core_config),
                ..SimConfig::default()
            },
        ),
        ("plain", SimConfig::default()),
    ] {
        for seed in 66_000..66_010 {
            let r =
                run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("{label} seed {seed}: {e}"));
            for (id, s) in &r.stats {
                assert_eq!(s.deleg_delegated, 0, "{label} node {id} delegated");
                assert_eq!(
                    s.deleg_installed, 0,
                    "{label} node {id} installed a delegation"
                );
                assert_eq!(
                    s.deleg_exec_parked, 0,
                    "{label} node {id} parked an execution"
                );
                assert_eq!(s.deleg_deps_waits, 0, "{label} node {id} waited for deps");
            }
        }
    }
}

/// Plan 30 §M11 phase 2b round 2: the seed whose recall was never
/// outwaited (the root's expiry timer dropped while it was momentarily
/// unusable), which left a root-local op parked forever and the clock
/// racing. The schedule is not deterministic run to run, so the seed
/// runs several times.
#[test]
fn long_delegated_seed_70051_finishes() {
    for _ in 0..4 {
        let t = run_m11("long-delegated", long_delegated_config(), 70_051..70_052);
        assert!(t.executed > 0, "{t:?}");
    }
}

/// Long-delegated seeds that failed on main:
/// - 70162 (1 in 15 runs, by the key sets' hash order): a delegate's
///   reply `base` came from an older window entry below the floor its
///   grant set, missing the segment (applied between two generations)
///   that renamed the name away; the requester installed the create
///   under that rename. Deterministic since `TouchSet` iterates in
///   order; it failed 5 in 100 runs with hash order and without the
///   floor, 0 in 100 with it;
/// - 70705: the successor's takeover gate opened in a later round (its
///   marker PUT had failed), which never learned the predecessor's live
///   generations: the delegate's stream was refused for good and its
///   acknowledged create never reached the log;
/// - 72780: the root ended a generation itself and kept its own shadow of
///   a create the (partitioned) delegate never streamed; its next op, an
///   `unlink` of that name, took effect in the log where the name did
///   not exist;
/// - 73964: a successor's takeover gate replayed a stranded create in a
///   directory delegated to another node, ahead of that delegate's
///   earlier acknowledged unlink (the gate's local replay ignored
///   ownership);
/// - 75808: a root whose lease was held but unusable for a moment
///   dropped its generations and never learned them again; it then
///   executed ops under a live delegation beside the delegate;
/// - 79725: a root's own op parked on a recall was answered `EIO` at its
///   deadline although the delegate's re-stream had appended its
///   completion;
/// - 79689 (a checker artifact): a delegate's tentative acknowledgement
///   stopped counting as tentative once its node restarted;
/// - 74189, 76967 (`marker-order`): the data write was a tentative
///   acknowledgement (its generation ended before the root appended it);
///   the marker, whose `deps` named it, was executed under the void rule
///   and reached the log (and a watcher) before the data's replay. Now a
///   lost dependency is never executed (`Held`, or left in the inbox and
///   withdrawn), and a requester's writes wait for its own replays.
#[test]
fn regression_long_delegated_seeds() {
    let t = run_m11("long-delegated", long_delegated_config(), 70_162..70_163);
    assert!(t.executed > 0, "{t:?}");
    for seed in [
        70_705, 72_780, 73_964, 75_808, 79_725, 79_689, 74_189, 76_967,
    ] {
        run_m11("long-delegated", long_delegated_config(), seed..seed + 1);
    }
}

/// `long-delegated-backup` seeds (the root and every delegate with a
/// backup in budget) that failed on main:
/// - 71251: an unanswered seal of a crashed delegate's backup set the
///   recall back to "none", the restarted delegate's renewals were
///   granted again, and the root's op parked on the recall ended `EIO`;
/// - 71792: a deposed root's stranded `Delegate` record was replayed as
///   plain records through its successor — a grant to the successor
///   itself that nothing ended, every op under it parked forever;
/// - 75504: a paused, then deposed root's late stream acknowledgement
///   counted for its successor, and the delegate never streamed its last
///   transaction to it;
/// - 78172: a `Recall` installed first by the pre-S3 stream was skipped
///   with its segment's streamed rows and never stranded a requester's
///   shadow of the recalled generation;
/// - 77901: the `marker-order` class of 74189 under backups;
/// - 74035: a paused root, deposed without knowing it, drained a sealed
///   delegate backup's marker whose `deps` named its successor's journal
///   (where the data was); its replica showed the marker without the
///   data until the deposition stranded it;
/// - 70232: a node that stopped being the holder's backup (the holder
///   crashed with none listed) discarded its holder tail and a
///   delegate's backup rows with it; the delegate's backup answered
///   `acked=0` for good (contiguous from 1, and the delegate re-sends
///   only its unshipped suffix), 22k append round trips followed, and the
///   delegate's client op waited 540 s for a segment no holder shipped.
#[test]
fn regression_long_delegated_backup_seeds() {
    for seed in [71_251, 71_792, 75_504, 78_172, 77_901, 74_035, 70_232] {
        run_m11(
            "long-delegated-backup",
            long_delegated_backup_config(),
            seed..seed + 1,
        );
    }
}

/// Item 3 of the delegation-safety fix: a delegate's reply that the
/// requester cannot install (the delegate evaluated it behind its own
/// unappended rows: `base: None`) is answered once the root's pre-S3
/// stream has installed the root's append of that transaction here —
/// not when its segment comes back from S3. The runs pass every check
/// (linearizability per directory, convergence, exactly-once, marker
/// order, strict acknowledgement order); zero such answers without the
/// change.
#[test]
fn a_delegate_reply_is_answered_from_the_root_pre_s3_stream() {
    let mut deleg = 0u64;
    for seed in 70_000..70_010 {
        let report = run_seed(seed, delegated_backup_config()).unwrap_or_else(|e| {
            panic!("delegated-backup seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=delegated-backup")
        });
        deleg += report
            .stats
            .values()
            .map(|s| s.awaited_log_streamed_deleg)
            .sum::<u64>();
    }
    assert!(deleg > 0, "no delegate reply was answered from the stream");
}

/// M12 round 2, long-sessions seed 10146: the holder recovered a
/// segment by re-reading its own timed-out PUT, and that path left the
/// segment out of the window forward replies' `base` is computed from;
/// a create's reply named a base below a rename the segment carried,
/// the requester's shadow installed on a state where the name still
/// existed, and its replica ended with a dentry to an inode it did not
/// have. Now the recovered segment's touches are noted like a shipped
/// one's.
#[test]
fn long_sessions_seed_10146_recovered_segment_is_in_the_base_window() {
    run_seed(10146, long_sessions_config())
        .unwrap_or_else(|e| panic!("long-sessions seed 10146: {e}"));
}

/// M12 round 2, flex-crash seed 30299: a member paused across its
/// epoch's activation, crashed, and restarted into the open epoch that
/// carried its own lease with no hold adopted; nobody could hold or
/// close the epoch and no taker got promises. Now a member restarting
/// into an open epoch whose carrier is its own lease adopts the hold.
///
/// The retention gap check (an extra S3 LIST before a takeover CAS and
/// on a follower's first empty probe) changed the schedule, and seed
/// 30299 no longer restarts a member into its own carried epoch; seed
/// 30908 does (`find_late_adopted_hold_seeds`). 30299 still runs clean.
#[test]
fn flex_crash_seed_30299_restarted_member_adopts_the_carried_hold() {
    run_seed(30299, flex_crash_config()).unwrap_or_else(|e| panic!("flex-crash seed 30299: {e}"));
    let report = run_seed(30908, flex_crash_config())
        .unwrap_or_else(|e| panic!("flex-crash seed 30908: {e}"));
    let adopted: u64 = report
        .stats
        .values()
        .map(|s| s.epoch_holds_adopted_late)
        .sum();
    assert!(adopted > 0, "the carried hold was never adopted late");
}

/// Every flex-crash seed below 20 000 that failed on `27f919c` (35, all
/// but six on `71dc7e7` too): a hold re-adopted after it was handed away
/// or closed (166, 2236), an epoch handoff answered with an S3 release
/// (2236), a member closing before the carrier left the epoch (4309 and
/// most of the "two authorities" seeds), an op refused `EROFS` although
/// sent (10248), a deadline answered in doubt for a landed write (2140),
/// the epoch journal published as the log prefix (11719, 19585). And
/// the seeds the epoch stream first livelocked (a frozen member nudged
/// into a zero-delay poll loop by the stream's gap rule, growing to
/// gigabytes): 1072, 2247, 3098, 11548, 12286, 14556. And 16755: a hold
/// owner that closed with nothing to flush left the expired carried lease
/// standing, and its member waited for good.
#[test]
fn flex_crash_regression_seeds() {
    for seed in [
        1072, 2247, 3098, 11548, 12286, 14556, 16755, 166, 2140, 2236, 3007, 3863, 4100, 4127,
        4309, 5597, 6234, 6556, 6975, 7111, 8777, 9232, 10248, 10403, 10717, 10995, 11719, 13836,
        16803, 17036, 18055, 18145, 18266, 18299, 18316, 18318, 18584, 19324, 19470, 19497, 19585,
        19856,
    ] {
        run_seed(seed, flex_crash_config())
            .unwrap_or_else(|e| panic!("flex-crash seed {seed}: {e}"));
    }
}

/// Plan 30 §M10 × §M9: the members of a continuation epoch keep following
/// the hold owner's log stream, and it streams its epoch journal ahead.
/// The flex workload has clients on every node, so the member forwards
/// its writes to the hold owner; a reply whose `base` names the holder's
/// unshipped epoch journal used to wait for a log that could not arrive
/// before S3 returned (`continuation-epoch`'s 40 s stall, then in doubt).
/// Now the stream delivers it: installed ahead of the log, answered, and
/// every seed still checks out (the streamed transactions are retired by
/// the segments the flush ships at the close, or stranded).
///
/// On `flex_config` without its member crash (fix "capture under an
/// epoch hold"): the sim's counters are per incarnation, so the crashed
/// member's installs die with its restart, and its re-streamed journal
/// is (rightly) skipped after it — the crash variant has its own tests.
#[test]
fn flex_members_follow_the_epoch_holders_stream() {
    let mut cfg = flex_config();
    cfg.faults
        .retain(|f| !matches!(f.kind, FaultKind::CrashEpochMember { .. }));
    let (mut installed, mut answered, mut ahead) = (0, 0, 0);
    for seed in 20_000..20_040 {
        let report =
            run_seed(seed, cfg.clone()).unwrap_or_else(|e| panic!("flex seed {seed}: {e}"));
        for s in report.stats.values() {
            installed += s.epoch_streamed_installed;
            answered += s.epoch_forwards_streamed;
            ahead += s.epoch_streamed_ahead;
        }
    }
    assert!(ahead > 0, "no hold owner streamed its epoch journal");
    assert!(installed > 0, "no member installed the epoch stream");
    assert!(
        answered > 0,
        "no member's forward was answered from the epoch stream"
    );
}

/// flex-crash seed 30702: node 1 was partitioned from holder 3 while 3
/// shipped seq 14–18, then both lost S3 and formed an epoch carrying 3's
/// lease. 3 handed its (journal-empty) hold to node 1 over P2P, and node
/// 1, still at seq 13, executed against a state the log had moved past:
/// its `Unlink(f3)` took effect after seq 18 had removed `f3`. Now an
/// epoch hold goes only to a requester that applied the holder's whole
/// log; node 1 forwards instead.
#[test]
fn flex_crash_seed_30702_an_epoch_hold_goes_only_to_a_caught_up_member() {
    let report = run_seed(30702, flex_crash_config())
        .unwrap_or_else(|e| panic!("flex-crash seed 30702: {e}"));
    let behind: u64 = report.stats.values().map(|s| s.epoch_handoffs_behind).sum();
    assert!(
        behind > 0,
        "no handoff to a member behind the log was declined"
    );
}

/// Plan 30 §M11 phase 2b round 2: the seed whose successor root replayed
/// a delegate-accepted shadow ahead of the delegate's earlier
/// transactions (a per-directory linearizability violation); now the
/// replay is held while the generation is live. Not deterministic run
/// to run: several runs.
#[test]
fn long_delegated_seed_70075_keeps_stream_order() {
    for _ in 0..4 {
        let t = run_m11("long-delegated", long_delegated_config(), 70_075..70_076);
        assert!(t.executed > 0, "{t:?}");
    }
}

/// `cargo test -p constellation-authority --release --test sim -- --ignored long_delegated`
#[test]
#[ignore]
fn long_delegated() {
    let seeds = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(300);
    let start = std::env::var("AUTHORITY_SIM_START")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(70_000);
    let t = run_m11(
        "long-delegated",
        long_delegated_config(),
        start..start + seeds,
    );
    assert!(t.executed > 0);
}

// ===================================================================
// Plan 30 §M14: cross-node `flock`/`fcntl`.

/// Three nodes, no faults: every client mixes namespace ops with whole-
/// file locks on two shared files (a quarter shared, a tenth
/// non-blocking), a few I/O steps under each. Node 1 takes the lease at
/// setup and owns every grant; its own clients lock through the same
/// path.
fn locks_config() -> SimConfig {
    SimConfig {
        nodes: 3,
        clients_per_node: 2,
        ops_per_client: 6,
        names: 4,
        random_faults: 0,
        read_ratio: 0.2,
        lock_files: 2,
        lock_ratio: 0.7,
        ..SimConfig::default()
    }
}

/// A node inside a long critical section is cut from every other node
/// over P2P for longer than a grant: its renewals fail, the grant lapses
/// under the section (its I/O is fenced) and the owner outwaits it (TTL
/// + margin) before granting the next node. Plus one random fault.
fn locks_partition_config() -> SimConfig {
    SimConfig {
        random_faults: 1,
        lock_ios: (6, 20),
        lock_io_ms: (40, 200),
        faults: vec![
            ScheduledFault {
                at_ms: 600,
                kind: FaultKind::PartitionLocker { for_ms: 3_000 },
            },
            ScheduledFault {
                at_ms: 5_000,
                kind: FaultKind::PartitionLocker { for_ms: 2_500 },
            },
        ],
        ..locks_config()
    }
}

/// Clocks off by up to ±200 ms (the margin is 500 ms: `margin > 2 ×
/// skew`), a little P2P loss.
fn locks_skew_config() -> SimConfig {
    SimConfig {
        clock_skew_ms: 200,
        p2p_drop: 0.02,
        ..locks_config()
    }
}

/// The lease holder (every grant's owner) dies while another node is
/// inside a critical section, and a lock-holding node dies inside one:
/// the successor waits the old grants out (TTL takeover), a dead
/// holder's grant is outwaited.
fn locks_failover_config() -> SimConfig {
    SimConfig {
        ops_per_client: 8,
        lock_ios: (4, 12),
        lock_io_ms: (20, 150),
        faults: vec![
            ScheduledFault {
                at_ms: 800,
                kind: FaultKind::CrashHolderWhileLocked {
                    restart_ms: Some(5_000),
                },
            },
            ScheduledFault {
                at_ms: 9_000,
                kind: FaultKind::CrashLocker {
                    restart_ms: Some(3_000),
                },
            },
        ],
        ..locks_config()
    }
}

/// The same under M9's backups: the holder's backup seals and takes over
/// before the TTL with the mirrored grant table (restamped) and the
/// acknowledgement floor.
fn locks_failover_backup_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::backup_core_config),
        ..locks_failover_config()
    }
}

/// Nodes stop (their events queue, their clients keep running) while
/// their clients wait for or hold locks: renewals stall, grants lapse
/// under critical sections, and replies and pushes are handled late —
/// a late grant must never be honoured past its window.
fn locks_pause_config() -> SimConfig {
    SimConfig {
        lock_ios: (4, 12),
        lock_io_ms: (20, 150),
        faults: vec![
            ScheduledFault {
                at_ms: 700,
                kind: FaultKind::Pause {
                    node: 3,
                    for_ms: 4_500,
                },
            },
            ScheduledFault {
                at_ms: 2_500,
                kind: FaultKind::Pause {
                    node: 2,
                    for_ms: 3_000,
                },
            },
        ],
        ..locks_config()
    }
}

/// The CI faults (two random ones per seed) under the lock workload.
fn locks_faults_config() -> SimConfig {
    SimConfig {
        random_faults: 2,
        ..locks_config()
    }
}

/// The lock files live in `d1`, delegated to node 2 (its grants are
/// node 2's); a third of the renames cross subtrees, so the root recalls
/// the delegation and the lock table moves with the subtree.
fn locks_delegated_config() -> SimConfig {
    SimConfig {
        lock_files: 2,
        lock_ratio: 0.7,
        lock_dir: Some("d1".into()),
        ..delegated_config()
    }
}

/// Plan 30 §M14: a takeover of a *released* lease, then a delegation
/// inside the new root's lock grace. The lock files live in `d1`, which
/// is the root's (`d2` is delegated to node 3); at 1.5 s the holder shuts
/// down gracefully — it releases its lease with its lock grants live —
/// and 0.2 s later `d1` is delegated through whoever holds the lease
/// then. The grants of the old tenure are known to nobody: the new root
/// covers them with a grace on the whole namespace (reclaims only), and
/// its delegate of `d1` must honour what is left of it.
fn locks_released_delegated_config() -> SimConfig {
    SimConfig {
        lock_files: 2,
        lock_ratio: 0.7,
        lock_dir: Some("d1".into()),
        delegations: vec![("d2".into(), 3)],
        faults: vec![
            ScheduledFault {
                at_ms: 1_500,
                kind: FaultKind::ShutdownHolder { restart_ms: 3_000 },
            },
            ScheduledFault {
                at_ms: 1_700,
                kind: FaultKind::DelegateDir {
                    dir: "d1".into(),
                    within_ms: 3_000,
                },
            },
        ],
        ..delegated_config()
    }
}

/// `locks-released-delegated` seed 198670: a delegate's acceptance of
/// the successor's own op reached it just after its takeover; the reply
/// was queued for replay without its generation, so the takeover gate
/// executed it at once — ahead of the delegate's earlier acknowledged
/// op. The queued replay keeps the generation now and is held while it
/// lives (the delegate re-streams the op).
#[test]
fn regression_locks_released_delegated_seed_198670() {
    run_seed(198_670, locks_released_delegated_config()).unwrap_or_else(|e| {
        panic!(
            "locks-released-delegated seed 198670: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=locks-released-delegated"
        )
    });
}

/// Plan 30 §M14, lock-to-unlock coherence across owner changes: every
/// exclusive holder writes a turn under its lock into a data file of
/// *another* owner's directory (`d2`, delegated to node 3, while the lock
/// files are in `d1`), so only the grant's floor — what the previous
/// holder had been acknowledged — orders that write before the next
/// holder's read; the log streams drop, reorder and cut frames, so
/// replicas lag. `base` is the lock configuration it extends.
fn with_lock_writes(base: SimConfig, data_dir: &str) -> SimConfig {
    SimConfig {
        lock_writes: true,
        lock_data_dir: Some(data_dir.into()),
        stream_faults: StreamFaults {
            drop_p: 0.04,
            reorder_p: 0.04,
            cut_p: 0.02,
            drop_segment_frames: Vec::new(),
        },
        ..base
    }
}

/// Plan 30 §M14: the lock counters summed over a run's nodes.
#[derive(Debug, Default)]
struct M14Totals {
    seeds: u64,
    clients: sim::locks::LockCounters,
    grants: u64,
    requests: u64,
    waiters_parked: u64,
    waiting_replies: u64,
    recalls_sent: u64,
    recalls_released: u64,
    recalls_expired: u64,
    renewals: u64,
    renewals_served: u64,
    reclaimed: u64,
    moved: u64,
    reinstated: u64,
    graces_inherited: u64,
    lost: u64,
    released: u64,
    granted_recalled: u64,
    grace_refusals: u64,
    grace_periods: u64,
    idle_released: u64,
    backup_takeovers: u64,
    unavailable: u64,
    wait_ms_total: u64,
    max_simulated_ms: u64,
}

impl M14Totals {
    fn add(&mut self, r: &Report) {
        self.seeds += 1;
        let c = &mut self.clients;
        let l = r.locks;
        c.steps += l.steps;
        c.acquired += l.acquired;
        c.ios += l.ios;
        c.fenced_ios += l.fenced_ios;
        c.local_conflicts += l.local_conflicts;
        c.granted += l.granted;
        c.would_block += l.would_block;
        c.unavailable += l.unavailable;
        c.regrants += l.regrants;
        c.idle_sent += l.idle_sent;
        c.position_timeouts += l.position_timeouts;
        c.abandoned += l.abandoned;
        c.max_wait_ms = c.max_wait_ms.max(l.max_wait_ms);
        c.visibility_checks += l.visibility_checks;
        c.visibility_gaps += l.visibility_gaps;
        c.turns_written += l.turns_written;
        c.turn_reads += l.turn_reads;
        c.stale_turn_reads += l.stale_turn_reads;
        c.late_unacked_turns += l.late_unacked_turns;
        for s in r.stats.values() {
            self.grants += s.lock_grants;
            self.requests += s.lock_requests;
            self.waiters_parked += s.lock_waiters_parked;
            self.waiting_replies += s.lock_waiting_replies;
            self.recalls_sent += s.lock_recalls_sent;
            self.recalls_released += s.lock_recalls_released;
            self.recalls_expired += s.lock_recalls_expired;
            self.renewals += s.lock_renewals;
            self.renewals_served += s.lock_renewals_served;
            self.reclaimed += s.lock_reclaimed;
            self.moved += s.lock_moved;
            self.reinstated += s.lock_reinstated;
            self.graces_inherited += s.lock_graces_inherited;
            self.lost += s.lock_lost;
            self.released += s.lock_released;
            self.granted_recalled += s.lock_granted_recalled;
            self.grace_refusals += s.lock_grace_refusals;
            self.grace_periods += s.lock_grace_periods;
            self.idle_released += s.lock_idle_released;
            self.backup_takeovers += s.backup_takeovers;
            self.unavailable += s.lock_unavailable;
            self.wait_ms_total += s.lock_wait_ms_total;
        }
        self.max_simulated_ms = self.max_simulated_ms.max(r.simulated_ms);
    }
}

fn run_m14(label: &str, cfg: SimConfig, seeds: std::ops::Range<u64>) -> M14Totals {
    let mut totals = M14Totals::default();
    for seed in seeds {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| {
            panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
        });
        totals.add(&report);
    }
    eprintln!("{label}: {totals:#?}");
    totals
}

/// Plan 30 §M14: no two nodes ever perform I/O under conflicting locks;
/// contended grants are recalled and released; every client finishes and
/// no request, waiter or recall is left behind.
#[test]
fn locks_are_mutually_exclusive() {
    let t = run_m14("locks", locks_config(), 90_000..90_200);
    assert!(t.grants > 500, "grants rarely made: {t:?}");
    assert!(t.recalls_sent > 100, "grants rarely contended: {t:?}");
    assert!(t.recalls_released > 100, "recalls never released: {t:?}");
    assert!(t.clients.ios > 1_000, "little I/O under locks: {t:?}");
    assert!(t.clients.local_conflicts > 0, "no local contention: {t:?}");
}

/// Plan 30 §M14: a partitioned lock holder's grant lapses under its
/// critical section — its I/O is fenced — and the owner outwaits the
/// grant before the next node's lock (mutual exclusion holds).
#[test]
fn locks_partitioned_holder_is_fenced() {
    let t = run_m14("locks-partition", locks_partition_config(), 91_000..91_120);
    assert!(t.clients.fenced_ios > 0, "no I/O was ever fenced: {t:?}");
    assert!(t.recalls_expired > 0, "no recall was outwaited: {t:?}");
}

/// Plan 30 §M14: clock skew within the margin keeps mutual exclusion.
#[test]
fn locks_hold_under_clock_skew() {
    let t = run_m14("locks-skew", locks_skew_config(), 92_000..92_120);
    assert!(t.recalls_released > 50, "{t:?}");
}

/// Plan 30 §M14: the owner dies while grants are held (TTL takeover) and
/// a lock holder dies inside its section.
#[test]
fn locks_survive_holder_failover() {
    let t = run_m14("locks-failover", locks_failover_config(), 93_000..93_080);
    assert!(t.grants > 500, "{t:?}");
    assert!(
        t.clients.abandoned > 0,
        "no node ever died inside a critical section: {t:?}"
    );
    assert!(t.recalls_expired > 0, "no dead holder was outwaited: {t:?}");
}

/// Plan 30 §M14: the same with M9's backups (seal takeover with the
/// mirrored table).
#[test]
fn locks_survive_backup_failover() {
    let t = run_m14(
        "locks-failover-backup",
        locks_failover_backup_config(),
        94_000..94_080,
    );
    assert!(t.grants > 500, "{t:?}");
    assert!(
        t.backup_takeovers > 20,
        "the backup rarely took over (with the mirror): {t:?}"
    );
}

/// Plan 30 §M14: the CI's random faults under the lock workload.
#[test]
fn locks_under_random_faults() {
    let t = run_m14("locks-faults", locks_faults_config(), 95_000..95_120);
    assert!(t.grants > 500, "{t:?}");
}

/// Plan 30 §M14: paused nodes (late replies and pushes, stalled
/// renewals) keep mutual exclusion.
#[test]
fn locks_survive_paused_nodes() {
    let t = run_m14("locks-pause", locks_pause_config(), 97_000..97_080);
    assert!(t.grants > 500, "{t:?}");
    assert!(
        t.clients.fenced_ios > 0,
        "no paused holder was fenced: {t:?}"
    );
}

/// Plan 30 §M14: lock files in a delegated subtree: the delegate owns the
/// grants, recalls of the delegation move the table.
#[test]
fn locks_in_a_delegated_subtree() {
    let t = run_m14("locks-delegated", locks_delegated_config(), 96_000..96_080);
    assert!(t.grants > 500, "{t:?}");
    assert!(
        t.moved > 0,
        "the lock table never moved with the subtree: {t:?}"
    );
    assert!(
        t.reinstated > 0,
        "no handoff was ever overtaken by its recall (the root's copies never reinstated): {t:?}"
    );
}

/// Plan 30 §M14: a subtree delegated inside the lock grace a released
/// takeover left at the new root: its delegate inherits what is left of
/// the grace (PROGRESS.md "Fix: locks-faults 195356 and the delegated
/// grace gap"), and mutual exclusion holds.
#[test]
fn locks_released_takeover_then_delegation() {
    let t = run_m14(
        "locks-released-delegated",
        locks_released_delegated_config(),
        98_000..98_060,
    );
    assert!(t.grants > 300, "{t:?}");
    assert!(
        t.grace_periods > 0,
        "no released takeover left a grace: {t:?}"
    );
    assert!(
        t.graces_inherited > 0,
        "no delegation started inside the grace: {t:?}"
    );
}

/// `locks-released-delegated` seeds that violated mutual exclusion while
/// the root's grace stayed at the root (the delegate of `d1` granted over
/// the released tenure's grants).
#[test]
fn regression_locks_released_takeover_grace_follows_the_delegation() {
    for seed in [198_012, 198_043, 198_056, 198_103] {
        run_seed(seed, locks_released_delegated_config()).unwrap_or_else(|e| {
            panic!(
                "locks-released-delegated seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-released-delegated"
            )
        });
    }
}

/// `locks-faults` seed 195356: a refusal answered from an effect its
/// holder had acknowledged under `Local` and then lost to a crash (the
/// L2 window, allowed without a backup) failed the generic tester, which
/// had no such exemption, while the log-witnessed check counted it as
/// `observed_tentative` (PROGRESS.md "Fix: locks-faults 195356 and the
/// delegated grace gap"). The lock floors changed its schedule (it no
/// longer reaches that window); the checkers' agreement is pinned by
/// `history::tests::a_refusal_that_observed_a_tentative_effect_is_in_flight_for_both_checkers`.
#[test]
fn regression_locks_faults_refusal_observed_a_rolled_back_effect() {
    run_seed(195_356, locks_faults_config())
        .unwrap_or_else(|e| panic!("locks-faults seed 195356: {e}"));
}

/// `locks-delegated` seeds where two nodes held exclusive grants on one
/// file (PROGRESS.md "Fix: long_locks seed 196102"): a delegation's lock
/// handoff rides its first renewal reply, the generation's recall
/// overtook it, and the root — which kept only a subtree grace, at the
/// root — delegated the subtree again with an empty table; the new
/// delegate granted over a live grant. 196102 is the `long_locks` seed
/// (it failed about one run in two while `TouchSet` was `HashSet`s; with
/// the order fixed it takes the passing branch); 96425 and 96805 fail
/// that way deterministically without the fix; 196004 failed against a
/// first version of it (a released grant's copy reinstated next to a
/// conflicting grant).
#[test]
fn regression_locks_delegated_handoff_overtaken_by_recall() {
    for seed in [196_102, 96_425, 96_805, 196_004] {
        run_seed(seed, locks_delegated_config()).unwrap_or_else(|e| {
            panic!("locks-delegated seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-delegated")
        });
    }
}

/// Plan 30 §M14, lock-to-unlock coherence: every exclusive holder writes
/// a turn under its lock into a data file of another owner, and every
/// later holder on another node reads that turn or a later one once its
/// grant's session wait is over — with the grant floors moved by
/// delegation moves and recalls, and mirrored to a fast successor
/// (PROGRESS.md "Fix: lock floors survive owner changes"). A stale read
/// fails the seed. Without any floor, 16–23 of 150 seeds per
/// configuration read a stale turn.
#[test]
fn locks_writes_are_visible_to_the_next_holder() {
    for (label, cfg, seeds) in [
        (
            "locks-writes",
            with_lock_writes(locks_config(), ""),
            99_000..99_040,
        ),
        (
            "locks-delegated-writes",
            with_lock_writes(locks_delegated_config(), "d2"),
            99_100..99_140,
        ),
        (
            "locks-released-writes",
            with_lock_writes(locks_released_delegated_config(), "d2"),
            99_200..99_240,
        ),
        (
            "locks-failover-backup-writes",
            with_lock_writes(locks_failover_backup_config(), ""),
            99_300..99_340,
        ),
    ] {
        let t = run_m14(label, cfg, seeds);
        assert!(t.clients.turns_written > 200, "{label}: {t:?}");
        assert!(t.clients.turn_reads > 100, "{label}: {t:?}");
        assert_eq!(t.clients.stale_turn_reads, 0, "{label}: {t:?}");
    }
}

/// Seeds of the lock-writes configurations that failed while this was
/// built (PROGRESS.md "Fix: lock floors survive owner changes"): a grant
/// floor past the session's streams cap dropped (200981), a delegate's
/// own executions missing from its release (200981), a new tenure
/// granting before its inherited delegates re-streamed (201666), an
/// exclusive local lock left unfenced under a shared grant (211727), a
/// superseded grant id installed late (211029), and the checker's own
/// refinements (201890, 201959, 210344).
#[test]
fn regression_lock_writes_seeds() {
    for (label, cfg, seed) in [
        (
            "locks-delegated-writes",
            with_lock_writes(locks_delegated_config(), "d2"),
            200_981,
        ),
        (
            "locks-delegated-writes",
            with_lock_writes(locks_delegated_config(), "d2"),
            210_344,
        ),
        (
            "locks-released-writes",
            with_lock_writes(locks_released_delegated_config(), "d2"),
            201_666,
        ),
        (
            "locks-released-writes",
            with_lock_writes(locks_released_delegated_config(), "d2"),
            211_727,
        ),
        (
            "locks-released-writes",
            with_lock_writes(locks_released_delegated_config(), "d2"),
            211_029,
        ),
        (
            "locks-released-writes",
            with_lock_writes(locks_released_delegated_config(), "d2"),
            201_890,
        ),
        (
            "locks-released-writes",
            with_lock_writes(locks_released_delegated_config(), "d2"),
            201_959,
        ),
    ] {
        run_seed(seed, cfg).unwrap_or_else(|e| {
            panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
        });
    }
}

/// Plan 30 §M14: a lock seed replays identically (the lock tables and
/// the ghost must not add nondeterminism; nor may the delegation
/// table's resolution order — seed 196102's `HashSet`s).
#[test]
fn locks_seed_replays_identically() {
    for seed in [90_003, 91_004, 196_102] {
        let cfg = if seed < 91_000 {
            locks_config()
        } else if seed < 92_000 {
            locks_partition_config()
        } else {
            locks_delegated_config()
        };
        let a = run_seed(seed, cfg.clone()).expect("run a");
        let b = run_seed(seed, cfg).expect("run b");
        assert_eq!(a.stats, b.stats, "per-node counters differ between replays");
        assert_eq!(format!("{:?}", a.locks), format!("{:?}", b.locks));
        assert_eq!(a.simulated_ms, b.simulated_ms);
    }
}

/// Plan 30 §M14's non-vacuity check: clients that keep doing I/O after
/// their grant lapsed (ignoring the fence) are caught by the checker on
/// some partition seed.
#[test]
fn locks_without_the_fence_are_found() {
    let cfg = SimConfig {
        lock_ignore_fence: true,
        ..locks_partition_config()
    };
    let caught = (91_000..91_060)
        .filter_map(|seed| run_seed(seed, cfg.clone()).err())
        .filter(|e| e.contains("mutual exclusion violated"))
        .count();
    assert!(caught > 0, "ignoring the fence was never caught");
}

/// `cargo test -p constellation-authority --release --test sim -- --ignored long_locks`
#[test]
#[ignore]
fn long_locks() {
    let n: u64 = std::env::var("AUTHORITY_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    run_m14("locks", locks_config(), 190_000..190_000 + n);
    run_m14(
        "locks-partition",
        locks_partition_config(),
        191_000..191_000 + n,
    );
    run_m14("locks-skew", locks_skew_config(), 192_000..192_000 + n);
    run_m14(
        "locks-failover",
        locks_failover_config(),
        193_000..193_000 + n,
    );
    run_m14(
        "locks-failover-backup",
        locks_failover_backup_config(),
        194_000..194_000 + n,
    );
    run_m14("locks-faults", locks_faults_config(), 195_000..195_000 + n);
    run_m14("locks-pause", locks_pause_config(), 197_000..197_000 + n);
    run_m14(
        "locks-delegated",
        locks_delegated_config(),
        196_000..196_000 + n,
    );
    run_m14(
        "locks-released-delegated",
        locks_released_delegated_config(),
        198_000..198_000 + n,
    );
    run_m14(
        "locks-writes",
        with_lock_writes(locks_config(), ""),
        199_000..199_000 + n,
    );
    run_m14(
        "locks-delegated-writes",
        with_lock_writes(locks_delegated_config(), "d2"),
        200_000..200_000 + n,
    );
    run_m14(
        "locks-released-writes",
        with_lock_writes(locks_released_delegated_config(), "d2"),
        201_000..201_000 + n,
    );
    run_m14(
        "locks-failover-backup-writes",
        with_lock_writes(locks_failover_backup_config(), ""),
        202_000..202_000 + n,
    );
}
