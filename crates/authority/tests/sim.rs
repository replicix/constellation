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
    pub mod oracle;
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
    let label = std::env::var("AUTHORITY_SIM_CONFIG");
    // `<config>+nomove+solo`: see `config_variant`.
    let (base_label, variants) = match label.as_deref() {
        Ok(l) => {
            let (b, v) = l.split_once('+').unwrap_or((l, ""));
            (Ok(b), v)
        }
        Err(e) => (Err(e), ""),
    };
    let config = match base_label {
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
        Ok("backup-far") | Ok("far") => far_config(),
        Ok("backup-two") => backup_two_config(),
        Ok("backup-strict") => backup_strict_config(),
        Ok("backup-strict-inbox-off") => backup_strict_inbox_off_config(),
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
        Ok("backup-reconfig-doubt") => backup_reconfig_doubt_config(),
        Ok("long-backup") => long_backup_config(),
        Ok("backup-runtime-lag") => backup_runtime_lag_config(),
        Ok("backup-lag-at-expiry") => backup_lag_at_expiry_config(false),
        Ok("backup-lag-at-expiry-beats-lost") => backup_lag_at_expiry_config(true),
        Ok("backup-lag-keeps-its-backup") => backup_lag_keeps_its_backup_config(),
        Ok("backup-lag-in-margin") => backup_lag_in_margin_config(),
        Ok("backup-lag-in-margin-cut") => backup_lag_in_margin_cut_config(),
        Ok("backup-lag-in-margin-unaware") => unaware(backup_lag_in_margin_config()),
        Ok("backup-runtime-lag-then-crash") => backup_runtime_lag_then_crash_config(),
        Ok("acks3-runtime-lag") => acks3_runtime_lag_config(),
        Ok("backup-runtime-lag-unaware") => unaware(backup_runtime_lag_config()),
        Ok("acks3-runtime-lag-unaware") => unaware(acks3_runtime_lag_config()),
        Ok("backup-lag-at-expiry-beats-lost-unaware") => unaware(backup_lag_at_expiry_config(true)),
        Ok("backup-lag-at-expiry-timers-first") => timers_first(backup_lag_at_expiry_config(false)),
        Ok("backup-lag-at-expiry-beats-lost-timers-first") => {
            timers_first(backup_lag_at_expiry_config(true))
        }
        Ok("backup-runtime-lag-timers-first") => timers_first(backup_runtime_lag_config()),
        Ok("acks3-runtime-lag-timers-first") => timers_first(acks3_runtime_lag_config()),
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
        Ok("flex-tight") => flex_tight_config(),
        Ok("flex-long") => flex_config(),
        Ok("flex-backup") => flex_backup_config(),
        Ok("flex-zero") => flex_zero_config(),
        Ok("metered-shared") => metered_shared_config(),
        Ok("metered-shared-unbacked") => metered_shared_unbacked_config(),
        // Plan 30 §M11.
        Ok("delegated") => delegated_config(),
        Ok("delegated-marker") => delegated_marker_config(),
        Ok("delegated-crash") => delegated_crash_config(),
        Ok("delegated-partition") => delegated_partition_config(),
        Ok("delegated-epoch") => delegated_epoch_config(),
        Ok("delegated-faults") => delegated_faults_config(),
        Ok("delegated-root-crash") => delegated_root_crash_config(),
        Ok("delegated-root-gone") => delegated_root_gone_config(),
        Ok("delegated-delegate-restart") => delegated_delegate_restart_config(),
        Ok("delegated-two-gens-root-crash") => delegated_two_gens_root_crash_config(),
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
        Ok("long-delegated-late-answers") => long_delegated_late_answers_config(),
        Ok("delegated-core-stall") => delegated_core_stall_config(),
        Ok("delegated-core-stall-lease6") => delegated_core_stall_lease6_config(),
        Ok("delegated-core-freeze") => delegated_core_freeze_config(),
        Ok("delegated-core-stall-forwarded") => delegated_core_stall_forwarded_config(),
        Ok("delegated-core-stall-unlaned") => delegated_core_stall_unlaned_config(),
        Ok("delegated-core-hang") => delegated_core_hang_config(),
        // Plan 30 §M14.
        Ok("locks") => locks_config(),
        Ok("locks-partition") => locks_partition_config(),
        Ok("locks-skew") => locks_skew_config(),
        Ok("locks-failover") => locks_failover_config(),
        Ok("locks-failover-backup") => locks_failover_backup_config(),
        Ok("locks-failover-backup-renew") => locks_failover_backup_renew_config(),
        Ok("locks-long-grants") => locks_long_grants_config(),
        Ok("locks-long-grants-seal") => locks_long_grants_seal_config(),
        Ok("fast-takeover-skewed-successor") => fast_takeover_skewed_successor_config(),
        Ok("double-seal-inside-the-grace") => double_seal_inside_the_grace_config(),
        Ok("double-seal-after-the-grace") => double_seal_after_the_grace_config(),
        Ok("locks-restart-reclaim-seal") => locks_restart_reclaim_seal_config(),
        Ok("fast-takeover-skewed-successor-nobeats") => {
            fast_takeover_skewed_successor_nobeats_config()
        }
        Ok("fast-takeover-skewed-successor-credited") => {
            fast_takeover_skewed_successor_credited_config()
        }
        Ok("fast-takeover-skewed-holder") => fast_takeover_skewed_holder_config(),
        Ok("locks-s3-cut-holder-alive") => locks_s3_cut_holder_alive_config(),
        Ok("locks-s3-cut-holder-lapse") => locks_s3_cut_holder_lapse_config(),
        Ok("locks-late-stale-mirror") => locks_late_stale_mirror_config(),
        Ok("locks-late-stale-mirror-naive") => locks_late_stale_mirror_naive_config(),
        Ok("strict-isolated-reader") => strict_isolated_reader_config(),
        Ok("strict-isolated-reader-beats") => strict_isolated_reader_beats_config(),
        Ok("strict-isolated-reader-skewed") => strict_isolated_reader_skewed_config(),
        Ok("strict-isolated-reader-skewed-credited") => {
            strict_isolated_reader_skewed_credited_config()
        }
        Ok("locks-faults") => locks_faults_config(),
        Ok("locks-blips") => locks_blips_config(),
        Ok("locks-blips-tight") => locks_blips_tight_config(),
        Ok("locks-blips-tight-faults") => locks_blips_tight_faults_config(),
        Ok("locks-blips-tight-delegated") => locks_blips_tight_delegated_config(),
        Ok("locks-blips-tight-single") => locks_blips_tight_single_config(),
        Ok("locks-blips-tight-long-lease") => locks_blips_tight_long_lease_config(),
        Ok("locks-blips-tight-in-doubt") => locks_blips_tight_in_doubt_config(),
        Ok("locks-pause") => locks_pause_config(),
        Ok("locks-delegated") => locks_delegated_config(),
        Ok("locks-delegated-production-ttl") => locks_delegated_production_ttl_config(),
        Ok("locks-delegated-production-ttl-heavy") => locks_delegated_production_ttl_heavy_config(),
        Ok("locks-production-ttl") => locks_production_ttl_config(),
        Ok("locks-production-ttl-heavy") => locks_production_ttl_heavy_config(),
        Ok("locks-released-delegated") => locks_released_delegated_config(),
        Ok("locks-delegated-writes") => with_lock_writes(locks_delegated_config(), "d2"),
        Ok("locks-unlinked-delegated") => locks_unlinked_delegated_config(),
        Ok("locks-unlinked-delegated-hcrash") => locks_unlinked_hcrash_config(false),
        Ok("locks-unlinked-delegated-hcrash-backup") => locks_unlinked_hcrash_config(true),
        Ok("locks-unlinked-delegated-dcrash") => locks_unlinked_dcrash_config(true),
        Ok("locks-unlinked-delegated-dcrash-nb") => locks_unlinked_dcrash_config(false),
        Ok("locks-unlinked-delegated-dcrash-pause") => locks_unlinked_dcrash_pause_config(),
        Ok("locks-unlinked-delegated-random") => locks_unlinked_random_config(false),
        Ok("locks-unlinked-delegated-dbackup-random") => locks_unlinked_random_config(true),
        Ok("locks-unlinked-delegated-blips") => locks_unlinked_blips_config(),
        Ok("locks-unlinked-delegated-partition") => locks_unlinked_partition_config(),
        Ok("locks-delegated-partition") => locks_delegated_partition_config(),
        Ok("locks-released-writes") => with_lock_writes(locks_released_delegated_config(), "d2"),
        Ok("locks-failover-backup-writes") => with_lock_writes(locks_failover_backup_config(), ""),
        Ok("locks-writes") => with_lock_writes(locks_config(), ""),
        Ok("locks-partition-writes") => with_lock_writes(locks_partition_config(), ""),
        Ok("locks-pause-writes") => with_lock_writes(locks_pause_config(), ""),
        Ok("locks-writer-hold") => locks_writer_hold_config(),
        Ok("locks-writer-hold-unchecked") => locks_writer_hold_unchecked_config(),
        Ok("locks-writer-hold-delegated") => locks_writer_hold_delegated_config(),
        Ok("locks-writer-hold-delegated-unsettled") => {
            locks_writer_hold_delegated_unsettled_config()
        }
        Ok("locks-writer-hold-delegated-doubt") => locks_writer_hold_delegated_doubt_config(),
        Ok("locks-stall-step") => locks_stall_step_config(),
        Ok("locks-stall-step-delegated") => locks_stall_step_delegated_config(),
        Ok("locks-root-step-forward") => locks_root_step_forward_config(),
        Ok("locks-locker-step-back") => locks_locker_step_back_config(),
        Ok("locks-holder-step-back-sealed") => locks_holder_step_back_sealed_config(),
        Ok("fast-takeover-holder-step-forward") => fast_takeover_holder_step_forward_config(),
        Ok("strict-isolated-reader-holder-step-forward") => {
            strict_isolated_reader_holder_step_forward_config()
        }
        Ok("fast-takeover-successor-step-forward") => fast_takeover_successor_step_forward_config(),
        Ok("delegated-root-step-forward") => delegated_root_step_forward_config(),
        Ok("delegated-delegate-step-back") => delegated_delegate_step_back_config(),
        Ok("strict-reader-step-back") => strict_reader_step_back_config(),
        Ok("locks-root-step-back") => locks_root_step_back_config(),
        Ok("locks-holder-step-forward") => locks_holder_step_forward_config(),
        Ok("locks-root-rate-fast") => locks_root_rate_fast_config(),
        Ok("locks-root-rate-skew") => locks_root_rate_skew_config(),
        Ok("locks-partition-long-grants") => locks_partition_long_grants_config(),
        Ok("fast-takeover-long-grants") => fast_takeover_long_grants_config(),
        Ok("fast-takeover-capped-grants") => fast_takeover_capped_grants_config(),
        Ok("delegated-long-delegation") => delegated_long_delegation_config(),
        Ok("strict-isolated-reader-long") => strict_isolated_reader_long_config(),
        Ok("fast-takeover-successor-rate-fast") => fast_takeover_successor_rate_fast_config(),
        Ok("fast-takeover-holder-rate-fast") => fast_takeover_holder_rate_fast_config(),
        Ok("delegated-root-rate-fast") => delegated_root_rate_fast_config(),
        Ok("strict-reader-rate-fast") => strict_reader_rate_fast_config(),
        Ok("locks-rate-within-budget") => locks_rate_within_budget_config(),
        Ok("locks-locker-suspend") => locks_locker_suspend_config(),
        Ok("strict-holder-suspend") => strict_holder_suspend_config(),
        Ok("locks-root-restart-step-forward") => locks_root_restart_step_forward_config(),
        Ok("locks-root-step-forward-then-restart") => locks_root_step_forward_then_restart_config(),
        Ok("locks-root-reboot-step-forward") => locks_root_reboot_step_forward_config(),
        Ok("locks-root-reboot") => locks_root_reboot_config(),
        Ok("strict-holder-restart-step-forward") => strict_holder_restart_step_forward_config(),
        Ok("flex-promiser-restart-step-forward") => flex_promiser_restart_step_forward_config(),
        Ok("fast-takeover-successor-restart-in-floor") => {
            fast_takeover_successor_restart_in_floor_config()
        }
        Ok("fast-takeover-successor-restart-in-floor-strict") => {
            fast_takeover_successor_restart_in_floor_strict_config()
        }
        Ok("fast-takeover-successor-restart-before-marker") => {
            fast_takeover_successor_restart_before_marker_config()
        }
        Ok("holder-restart-step-forward-readopt") => holder_restart_step_forward_readopt_config(),
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
    let config = variants
        .split('+')
        .filter(|v| !v.is_empty())
        .fold(config, config_variant);
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
    // `AUTHORITY_SIM_BEATS=thread`: as for `sweep_config`.
    let config = SimConfig {
        holder_beats: if std::env::var("AUTHORITY_SIM_BEATS").as_deref() == Ok("thread")
            && config.holder_beats == sim::run::HolderBeats::Off
        {
            sim::run::HolderBeats::Thread
        } else {
            config.holder_beats
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

/// Plan 43 C6's baseline: `CONSTELLATION_BACKUPS=2` on three LAN nodes — the
/// holder lists up to two backups, so a second one is added while the
/// first holds the acknowledged rows (the add window of plan 43 §4's I1).
fn backup_two_config() -> SimConfig {
    SimConfig {
        nodes: 3,
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.backups_max = 2;
            c
        }),
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

/// Plan 43 C7-F F-a0 (the ack side of the review's F1): two nodes, node 1
/// the holder (node 2's clients start late). Node 1's second lease CAS —
/// the reconfiguration that lists its caught-up candidate, node 2, after
/// its claim — lands in S3 with its answer lost (applied, then timed out),
/// and its lease CASes after it time out unapplied for 5 s, so no adoption
/// settles the doubt (its lease reads and the log still work). Then the
/// two are cut apart (S3 stays reachable to both). Node 1 drops the
/// candidate and nobody else is in budget: as a `Local` tenure it would
/// acknowledge its clients' writes on its disk alone while S3 lists node
/// 2, which seals it and takes over without those rows (I1 at the answer:
/// `root-group/…` uncommitted; the durability check at the end).
fn backup_reconfig_doubt_config() -> SimConfig {
    let lease_put = |when, fault| Rule::new(Some(1), OpKind::Put, "leases/", when, fault);
    SimConfig {
        nodes: 2,
        ops_per_client: 12,
        random_faults: 0,
        client_plan: vec![(2, 6_000, 4)],
        faults: vec![
            ScheduledFault {
                at_ms: 0,
                kind: FaultKind::S3Rule(lease_put(When::Nth(2), Fault::AppliedThenTimeout)),
            },
            ScheduledFault {
                at_ms: 0,
                kind: FaultKind::S3RuleFor {
                    rule: lease_put(When::From(3), Fault::Timeout),
                    for_ms: 5_000,
                },
            },
            ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::Partition {
                    a: 1,
                    b: 2,
                    for_ms: 3_000,
                },
            },
        ],
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

/// `backup-strict` with the inbox off (`CONSTELLATION_INBOX=off`): the
/// takeover gate then does not wait out the read quarantine, so nothing but
/// the takeover floor itself keeps the successor from handing the lease on
/// inside it (plan 43 C7-H2, review S3).
fn backup_strict_inbox_off_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c
        }),
        ..backup_strict_config()
    }
}

/// Plan 43 C7-F F-a (review F3): the acknowledgement/read-delegation side
/// of the floor under load. Node 3, a reader (its clients only read), holds
/// a strict read delegation from the holder (node 1) and is cut from both
/// others and from S3 at 1.4 s; the holder is crashed at 1.5 s, its backup seals and takes
/// over (the inbox off: the takeover gate does not wait the read quarantine
/// out), and the reader, which learns nothing of it, serves its reads from
/// the delegation until it lapses (about 5 s in; it would otherwise learn
/// the new epoch from S3 and drop it within half a second). The successor's floor is all that
/// keeps it from acknowledging a write the reader then misses: on a build
/// with no floor this fails (close-to-open), with the real floor and cap
/// it passes. The holder's beats are off here: the floor is C7-H2's bound
/// (`strict-isolated-reader-beats` has them on, the cap in play).
fn strict_isolated_reader_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c
        }),
        faults: vec![
            ScheduledFault {
                at_ms: 1_400,
                kind: FaultKind::IsolateReader {
                    node: 3,
                    for_ms: 8_000,
                },
            },
            ScheduledFault {
                at_ms: 1_400,
                kind: FaultKind::CutS3 {
                    node: 3,
                    for_ms: 8_000,
                },
            },
            ScheduledFault {
                at_ms: 1_500,
                kind: FaultKind::CrashHolder {
                    restart_ms: Some(7_000),
                    keep_journal: true,
                },
            },
        ],
        random_faults: 0,
        ops_per_client: 40,
        read_ratio: 0.5,
        reader_nodes: vec![3],
        // Node 1 writes a little before the cut and then not at all (a
        // write recalls the readers' delegations); node 2, which will
        // hold, writes from just after the crash.
        client_plan: vec![(1, 0, 2), (2, 1_600, 25), (3, 600, 40)],
        ..backup_strict_config()
    }
}

/// `strict-isolated-reader` with the holder's off-core beat on (as the
/// daemon has it): the floor is capped by the beats the successor heard.
fn strict_isolated_reader_beats_config() -> SimConfig {
    SimConfig {
        holder_beats: sim::run::HolderBeats::Thread,
        ..strict_isolated_reader_config()
    }
}

/// `strict-isolated-reader-beats` with the successor's clock (node 2, the
/// backup) 4 s ahead of the holder's: the cap's lag is large and the beats'
/// arrival stamps are on the successor's clock, as in
/// `fast-takeover-skewed-successor` but on the strict side, where a floor
/// ended early lets the successor acknowledge a write inside the reader's
/// delegation. Clean with the arrival; with the credit
/// (`strict-isolated-reader-skewed-credited`) it fails on a large share of
/// seeds: the control for the cap's choice of stamp that does not hang on
/// a narrow window.
fn strict_isolated_reader_skewed_config() -> SimConfig {
    SimConfig {
        clock_offsets: vec![(2, 4_000)],
        ..strict_isolated_reader_beats_config()
    }
}

/// The negative control of `strict-isolated-reader-skewed`: every
/// numbered beat's arrival stamp replaced by its credit
/// (`SimConfig::beats_credited`).
fn strict_isolated_reader_skewed_credited_config() -> SimConfig {
    SimConfig {
        beats_credited: true,
        ..strict_isolated_reader_skewed_config()
    }
}

// ---- Plan 43 C7-H1: a wall-clock step of one node (`FaultKind::ClockStep`).
//
// The step moves one node's WALL reading only; its honour clock follows a
// forward step once and ignores a backward one, its wait clock never moves
// (`sim/clock.rs`). Until the commits that move a protocol site onto the
// honour or the wait clock, every site reads WALL, so each configuration
// below that targets a site fails here at the rate PROGRESS records (the
// "fail-before" of the commit that fixes it). They are not in `long_locks`
// or the CI tests until then.

fn step_fault(at_ms: u64, who: sim::run::StepOf, ms: i64) -> ScheduledFault {
    ScheduledFault {
        at_ms,
        kind: FaultKind::ClockStep { who, ms },
    }
}

/// Plan 43 C7-H1: the simulated clock model. A step moves one node's WALL
/// reading only, as the daemon's does (`types::clock`'s P4): HONOUR follows
/// a forward step once, a backward step moves neither HONOUR nor WAIT, and
/// no other node's clock moves. The step belongs to the node, so a new
/// incarnation (a new `ProtocolClock` with a new id) starts all three
/// readings at the stepped wall.
#[tokio::test(start_paused = true)]
async fn a_simulated_clock_step_moves_wall_only_and_outlives_the_incarnation() {
    use std::sync::atomic::Ordering;
    let truth = sim::clock::Clock::start();
    let state = sim::clock::NodeClockState::new(3, 0, false);
    let step = state.step();
    let node = truth.for_node(0, state.clone(), (3 << 32) | 1, 0);
    let other = truth.for_node(
        0,
        sim::clock::NodeClockState::new(2, 0, false),
        (2 << 32) | 1,
        0,
    );
    tokio::time::advance(std::time::Duration::from_millis(1_000)).await;
    let (n0, o0) = (node.now(), other.now());
    assert_eq!(
        (n0.wall.0, n0.honour.0, n0.wait.0),
        (o0.wall.0, o0.honour.0, o0.wait.0)
    );
    // Backward: wall drops by 3 s; honour and wait go on with time.
    step.fetch_add(-3_000, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_millis(500)).await;
    let n1 = node.now();
    assert_eq!(n1.wall.0, n0.wall.0 - 3_000 + 500);
    assert_eq!(n1.honour.0, n0.honour.0 + 500);
    assert_eq!(n1.wait.0, n0.wait.0 + 500);
    // Forward by 5 s: honour jumps 2 s past where time alone put it (the
    // 3 s back-step is undone first), and wait never moves.
    step.fetch_add(5_000, Ordering::SeqCst);
    let n2 = node.now();
    assert_eq!(n2.wall.0, n1.wall.0 + 5_000);
    assert_eq!(n2.honour.0, n1.honour.0 + 2_000);
    assert_eq!(n2.wait.0, n1.wait.0);
    let st = node.step_stats();
    assert_eq!((st.steps_backward, st.steps_forward), (1, 1));
    // Nobody else moved.
    let o1 = other.now();
    assert_eq!(o1.wall.0, o0.wall.0 + 500);
    assert_eq!(other.step_stats(), Default::default());
    // A restart: a new clock over the same step. All three start at the
    // node's wall; its id is new.
    let reborn = truth.for_node(0, state.clone(), (3 << 32) | 2, 0);
    assert_ne!(reborn.id(), node.id());
    let r = reborn.now();
    assert_eq!(r.wall, node.wall());
    assert_eq!((r.honour.0, r.wait.0), (r.wall.0, r.wall.0));
}

/// The review's case (plan 43 C7-F F-a review F2): the lease holder, which
/// owns every lock grant, has its wall clock stepped forward 2.5 s, twice,
/// while grants are held across the steps. It outwaits a grant by its wall
/// clock (`LockGrantExpiry`, `conflicting`'s `drop_expired`), so a step
/// ends the wait 2.5 s early: it grants the lock again while the first
/// holder, whose own clock did not step, still honours its grant (mutual
/// exclusion). Fails on 1 821 of 2 000 seeds before plan 43 C7-H1's
/// wait-side commit.
fn locks_root_step_forward_config() -> SimConfig {
    SimConfig {
        lock_ios: (6, 20),
        lock_io_ms: (40, 200),
        faults: vec![
            step_fault(3_000, sim::run::StepOf::Holder, 2_500),
            step_fault(7_000, sim::run::StepOf::Holder, 2_500),
        ],
        ..locks_config()
    }
}

/// A locker is stepped back 3 s and then cut from every other node for
/// 4 s inside its critical section (`PartitionLockerStepped`): its grant
/// lapses at the owner, which outwaits it and grants the next node, while
/// the locker, whose clock is 3 s behind, still honours it
/// (`HeldGrant.until` on the wall clock): two holders of one lock. Fails on
/// 479 of 2 000 seeds before the honour-side commit.
fn locks_locker_step_back_config() -> SimConfig {
    SimConfig {
        random_faults: 0,
        faults: vec![ScheduledFault {
            at_ms: 2_000,
            kind: FaultKind::PartitionLockerStepped {
                step_ms: -3_000,
                dur_ms: 4_000,
            },
        }],
        ..locks_partition_config()
    }
}

/// Plan 41 L0 under a backward step: the holder is cut from S3 and from its
/// backup at 1.2 s (`locks-failover-backup-renew`), strict reads on, and
/// its clock is stepped back 8 s as it is cut (the step has to outlast what
/// the successor's floor leaves: at 3 s, 6 of 2 000 seeds fail). The
/// holder's last S3 liveness stamp is a pre-step wall value and its lease
/// is read on the stepped wall, so it keeps honouring both long after its
/// successor has taken over: its own clients' lock grants are honoured past
/// the successor's (the failures seen are mutual exclusion), and so should
/// its strict answers be. No variant that isolated the strict read path
/// (no locks; the holder's clients only reading; the holder isolated from
/// every node) produced a stale read: the holder's reads block once it is
/// cut (PROGRESS has the variants).
fn locks_holder_step_back_sealed_config() -> SimConfig {
    SimConfig {
        strict: true,
        read_ratio: 0.6,
        holder_beats: sim::run::HolderBeats::Thread,
        faults: [step_fault(1_200, sim::run::StepOf::Holder, -8_000)]
            .into_iter()
            .chain(locks_failover_backup_renew_config().faults)
            .collect(),
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-F F-a's forward step (the review's F2): `locks-long-grants-
/// seal` (5 s grants, the inbox off, the holder cut from S3 and its backup
/// at 1.2 s, beats on a thread), with the holder's wall clock stepped
/// forward 3 s shortly before the cut. The beats' `sent` is on the stepped
/// wall and the expiry they name is not: the cap counts `X - sent`, 3 s
/// early, and the successor's floor ends early. Two classes fail here, 18 of
/// 2 000 seeds in all (81 before plan 44's ask-again fix): the old root's own
/// grant table, which a forward step of 3 s makes drop records early (16 of
/// them, between 1.2 and 1.55 s, within 0.35 s of the step: the wait-side
/// commit), and, after the takeover, the successor's grant (2: the cap, or the old root still
/// granting). The mirror of the old root's grants (`LockMirror`, restamped
/// at the successor) covers most of what the cap alone would not.
fn fast_takeover_holder_step_forward_config() -> SimConfig {
    SimConfig {
        faults: [step_fault(1_000, sim::run::StepOf::Holder, 3_000)]
            .into_iter()
            .chain(locks_long_grants_seal_config().faults)
            .collect(),
        holder_beats: sim::run::HolderBeats::Thread,
        ..locks_long_grants_seal_config()
    }
}

/// Plan 43 C7-H1 (R7), the beat cap across a forward step of the holder, on
/// the strict side (`strict-isolated-reader-beats`): the holder is cut from
/// S3 only (it keeps beating until its lease ends) instead of crashing, and
/// its wall clock steps forward 4 s just after the cut, so no renewal after
/// the step can land and the lease body keeps the pre-step end. Its honour
/// clock follows the step: the stamps of its beats after the step jump by 4
/// s while the lease end they are compared with does not, and the cap a
/// successor counts from them (`X - sent`) is 4 s early for the read
/// delegations granted before the step. Its backup seals when the holder
/// stops beating (the honour end comes 4 s early), the floor is capped out
/// before its own marker lands, and the successor acknowledges a write
/// inside a window the isolated reader still honours: close-to-open. The
/// beats and the lease's end on the holder's wait clock (which no step
/// moves) close it.
fn strict_isolated_reader_holder_step_forward_config() -> SimConfig {
    let mut faults: Vec<ScheduledFault> = strict_isolated_reader_beats_config()
        .faults
        .into_iter()
        .filter(|f| !matches!(f.kind, FaultKind::CrashHolder { .. }))
        .collect();
    faults.push(ScheduledFault {
        at_ms: 1_500,
        kind: FaultKind::CutS3Holder { for_ms: 9_000 },
    });
    faults.push(step_fault(1_550, sim::run::StepOf::Holder, 4_000));
    SimConfig {
        faults,
        ..strict_isolated_reader_beats_config()
    }
}

/// A successor stepped forward inside its own floor: `locks-long-grants-
/// seal` (5 s grants, one seal at 1.2 s) with long critical sections (8 to
/// 12 steps of 300 to 600 ms, so a section outlives the old root's window)
/// and the successor's wall clock stepped forward 4 s at 3 s. By then the
/// backup has sealed and is the lease holder, so the step is `StepOf::Holder`
/// (`StepOf::Backup` reads the lease at the fault's time, where it names the
/// *new* backup). The floor (`floor_until`, a wall deadline) ends 4 s early
/// and the successor grants a lock inside the old root's window: mutual
/// exclusion, about 5.4 s in. Fails on 20 of 2 000 seeds before the wait-side
/// commit.
fn fast_takeover_successor_step_forward_config() -> SimConfig {
    SimConfig {
        lock_ios: (8, 12),
        lock_io_ms: (300, 600),
        faults: locks_long_grants_seal_config()
            .faults
            .into_iter()
            .chain([step_fault(3_000, sim::run::StepOf::Holder, 4_000)])
            .collect(),
        ..locks_long_grants_seal_config()
    }
}

/// The root of a delegated tree stepped forward 3 s mid-run
/// (`delegated-holder-cut`'s tree): it outwaits a generation by its wall
/// clock (`GenState.until`), reclaims 3 s early and serves the directory
/// while the delegate still honours the generation. Fails on 4 of 2 000
/// seeds (an ordering violation, a rid the log refuses, a run that does not
/// reach quiescence) before the wait-side commit.
fn delegated_root_step_forward_config() -> SimConfig {
    SimConfig {
        faults: delegated_holder_cut_config()
            .faults
            .into_iter()
            .chain([step_fault(1_000, sim::run::StepOf::Holder, 3_000)])
            .collect(),
        ..delegated_holder_cut_config()
    }
}

/// A delegate stepped back 3 s while cut from its root for 12 s: it should
/// keep executing under its generation's `until` (its wall clock) past the
/// root's reclaim. Clean on this tree (0 of 2 000 seeds, and none of the
/// stronger variants PROGRESS lists): kept as a regression configuration
/// for the honour-side commit, not a fail-before.
fn delegated_delegate_step_back_config() -> SimConfig {
    SimConfig {
        ops_per_client: 30,
        faults: vec![
            ScheduledFault {
                at_ms: 1_000,
                kind: FaultKind::Partition {
                    a: 1,
                    b: 2,
                    for_ms: 12_000,
                },
            },
            step_fault(1_000, sim::run::StepOf::Node(2), -3_000),
        ],
        ..delegated_config()
    }
}

/// The isolated strict reader (`strict-isolated-reader-skewed` without the
/// constant offset) stepped back 3 s while it holds a read delegation and
/// is cut off: it honours `HeldDelegation.until` on its wall clock, 3 s
/// past the root's outwait, and serves a stale strict read
/// (close-to-open). Fails on 91 of 2 000 seeds before the honour-side
/// commit.
fn strict_reader_step_back_config() -> SimConfig {
    SimConfig {
        clock_offsets: Vec::new(),
        faults: strict_isolated_reader_beats_config()
            .faults
            .into_iter()
            .chain([step_fault(1_400, sim::run::StepOf::Node(3), -3_000)])
            .collect(),
        ..strict_isolated_reader_beats_config()
    }
}

/// Control: the root stepped back 3 s. Only longer waits on the wait side
/// (the owner's outwait counts from a wall stamp that is now 3 s behind);
/// nothing breaks, nothing hangs, no `EIO`. Guards "a backward step only
/// lengthens waits".
fn locks_root_step_back_config() -> SimConfig {
    SimConfig {
        faults: vec![step_fault(3_000, sim::run::StepOf::Holder, -3_000)],
        ..locks_config()
    }
}

/// Control: a locker stepped forward 3 s inside its section. A forward step
/// ends the honour windows early (D2): its grant lapses, so its I/O can be
/// fenced (`EIO` from the owner fence), counted and not a violation (2 140
/// I/Os fenced in 1 885 of 2 000 seeds, no failure).
fn locks_holder_step_forward_config() -> SimConfig {
    SimConfig {
        random_faults: 0,
        faults: vec![step_fault(2_000, sim::run::StepOf::Locker, 3_000)],
        ..locks_partition_config()
    }
}

// ---- Plan 43 C7a (commit 1): rate, suspend and reboot of one node
// (`FaultKind::RateSkew`, `Suspend`, `RebootNode`, `CrashHolderStepped`,
// `CrashSuccessorInFloor`, `SimConfig::node_rates`).
//
// The rate configurations budget the rate they inject
// (`SimConfig::rate_budget_ppm`), which the readings do not use until C7a's
// second commit: each fails here, at the rate PROGRESS records (the
// "fail-before"), and is clean once the clocks scale by the budget. The
// restart configurations fail until the boot-stamped horizons (third commit)
// and the persisted floor (fourth). They are not in `long_locks` or the CI
// tests until the commit that fixes them.

/// `lock_ttl_ms` 20 s grants under a 30 s lease: a grant is not capped by
/// the lease (the rate configurations' arithmetic wants whole grants).
fn long_grants_core(
    base: fn(u64, u32) -> constellation_authority::core::Config,
) -> std::sync::Arc<dyn Fn(u64, u32) -> constellation_authority::core::Config + Send + Sync> {
    std::sync::Arc::new(move |id, inc| {
        let mut c = base(id, inc);
        c.ttl_ms = 30_000;
        c.lock_ttl_ms = 20_000;
        c
    })
}

/// A few long lockers (`plan`: `(node, start_ms)`, one exclusive lock each on
/// one file, `ios` steps of `io_ms` under it) and no other client: the rate
/// and restart configurations' workload. A section outlives a 20 s grant,
/// and the one that waits is granted only once the other's window is over,
/// so the overlap of two windows that a clock error opens is what the
/// checker sees.
fn long_lockers(
    cfg: SimConfig,
    plan: &[(u64, u64)],
    ios: (u32, u32),
    io_ms: (u64, u64),
) -> SimConfig {
    let mut client_plan: Vec<(u64, u64, usize)> = (1..=cfg.nodes)
        .map(|n| match plan.iter().find(|(node, _)| *node == n) {
            Some((_, start)) => (n, *start, 1),
            None => (n, 0, 0),
        })
        .collect();
    client_plan.sort();
    SimConfig {
        clients_per_node: 1,
        ops_per_client: 1,
        lock_files: 1,
        lock_ratio: 1.0,
        lock_shared_ratio: 0.0,
        lock_nonblocking_ratio: 0.0,
        client_plan,
        lock_ios: ios,
        lock_io_ms: io_ms,
        ..cfg
    }
}

/// The workload of the lock-rate configurations: two lockers with critical
/// sections of 11 to 37 s (a grant is 20 s), one of them cut from every other
/// node for 25 s at 2 s. The control (no rate) is `locks-partition-long-grants`.
fn locks_partition_long_grants_config() -> SimConfig {
    long_lockers(
        SimConfig {
            core: long_grants_core(sim::run::sim_core_config),
            random_faults: 0,
            faults: vec![ScheduledFault {
                at_ms: 2_000,
                kind: FaultKind::PartitionLocker { for_ms: 25_000 },
            }],
            ..locks_partition_config()
        },
        &[(2, 0), (3, 0)],
        (110, 150),
        (100, 250),
    )
}

/// The root (node 1) runs 8 % fast, budgeted for it, and a locker inside a
/// critical section is cut from every other node for 25 s: its grant (20 s)
/// is honoured until 19.5 s after it was sent, while the owner outwaits it
/// by its own wait clock, which over-counts by 8 %: `(20 000 + 500) / 1.08 =
/// 18 981` ms of real time. The overlap (about 0.5 s) is a second holder of
/// the lock. It needs a rate beyond `2M / (T - M) = 5.1 %`, hence the
/// budget of 80 000 ppm (no real oscillator is that far off; the budget is
/// what the clocks must scale by). Clean once the wait clock runs slow by
/// the budget.
fn locks_root_rate_fast_config() -> SimConfig {
    SimConfig {
        rate_budget_ppm: 80_000,
        node_rates: vec![(1, 80_000)],
        ..locks_partition_long_grants_config()
    }
}

/// The workload of the successor configurations: `locks-failover-backup-
/// renew`'s first cut (the holder is cut from S3 and from its backup at 1.2 s
/// and sealed at about 2 s), 20 s lock grants under a 30 s lease, two lockers
/// (node 3 first, then the backup, node 2, which is the successor) with
/// critical sections of 18 to 40 s, and the first locker cut from every other
/// node for 25 s at 3 s, after the takeover: its grant can no longer be
/// renewed, and the successor (which grants its own request) outwaits it.
fn fast_takeover_long_grants_config() -> SimConfig {
    long_lockers(
        SimConfig {
            core: long_grants_core(sim::run::backup_core_config),
            faults: locks_failover_backup_renew_config().faults[..2]
                .iter()
                .cloned()
                .chain([ScheduledFault {
                    at_ms: 3_000,
                    kind: FaultKind::PartitionLocker { for_ms: 25_000 },
                }])
                .collect(),
            ..locks_failover_backup_renew_config()
        },
        &[(3, 0), (2, 600)],
        (180, 200),
        (100, 200),
    )
}

/// `locks-root-rate-fast` with the rate arriving mid-run (`RateSkew`, from
/// 1 s) instead of from the start: the same failure.
fn locks_root_rate_skew_config() -> SimConfig {
    let mut faults = locks_partition_long_grants_config().faults;
    faults.push(ScheduledFault {
        at_ms: 1_000,
        kind: FaultKind::RateSkew {
            who: sim::run::StepOf::Holder,
            ppm: 80_000,
        },
    });
    SimConfig {
        rate_budget_ppm: 80_000,
        faults,
        ..locks_partition_long_grants_config()
    }
}

/// `fast-takeover-long-grants` with the first backup's oscillator 30 % fast:
/// the successor's lock floor `W + lock + 2M = 22 000` ms on its wait clock
/// ends after 16 900 ms of real time from its marker (about 19 000 ms), and
/// the table it installed from its backup's mirror (restamped `lock + M`
/// at the takeover) after 15 800 ms (about 17 900 ms), while the cut locker
/// honours the grant it was sent at 0.1 s to 19 600 ms. It needs a rate of
/// about 25 %: at the plan's 10 % the floor ends at about 22 100 ms, past
/// every grant of the old tenure.
fn fast_takeover_successor_rate_fast_config() -> SimConfig {
    SimConfig {
        rate_budget_ppm: 300_000,
        node_rates: vec![(2, 300_000)],
        ..fast_takeover_long_grants_config()
    }
}

/// The workload of the cap configurations: `fast-takeover-long-grants` with a
/// 10 s lease (the grants of 20 s are capped by it: what the successor's
/// floor protects ends with the old lease) and the holder's beats on a
/// thread, so the floor is capped by what the beats say of that lease. The
/// control (no rate) of `fast-takeover-holder-rate-fast`.
fn fast_takeover_capped_grants_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.ttl_ms = 10_000;
            c.lock_ttl_ms = 20_000;
            c
        }),
        holder_beats: sim::run::HolderBeats::Thread,
        ..fast_takeover_long_grants_config()
    }
}

/// `fast-takeover-capped-grants` with the holder 25 % slow and its backup 25 %
/// fast: the cap counts `X - sent` on the holder's wait clock and puts it on
/// the successor's, so it ends early by `(1 - c_h/c_b)(X - sent)`, 40 % of
/// the lease's 10 s: more than `M`. (Both nodes fast by the same rate, as the
/// plan's table has it, cancels: the two wait clocks keep one ratio.)
fn fast_takeover_holder_rate_fast_config() -> SimConfig {
    SimConfig {
        rate_budget_ppm: 250_000,
        node_rates: vec![(1, -250_000), (2, 250_000)],
        ..fast_takeover_capped_grants_config()
    }
}

/// `delegated-delegate-step-back`'s shape with 20 s delegations: the delegate
/// of `d1` (node 2) is cut from the root for 25 s while its clients keep
/// working, and the root (node 1) is 10 % fast: its generation outwait
/// (`GenState.until`) ends after `(20 000 + 500) / 1.1 = 18 636` ms of real
/// time while the cut delegate honours its `until` to 19 500 ms. The control
/// (no rate) is `delegated-long-delegation`.
fn delegated_long_delegation_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::sim_core_config(id, inc);
            c.ttl_ms = 30_000;
            c.delegation_ttl_ms = 20_000;
            c
        }),
        ops_per_client: 60,
        faults: vec![ScheduledFault {
            at_ms: 1_000,
            kind: FaultKind::Partition {
                a: 1,
                b: 2,
                for_ms: 25_000,
            },
        }],
        ..delegated_config()
    }
}

/// `delegated-holder-cut` with 20 s delegations and the root (node 1) 10 %
/// fast: its generation outwait (`GenState.until`) ends after `(20 000 +
/// 500) / 1.1 = 18 636` ms of real time while a cut delegate honours its
/// `until` to 19 500 ms. Measured clean on the parent (the root is crashed
/// in this configuration, so no delegate outlives its generation: see
/// PROGRESS).
fn delegated_root_rate_fast_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.delegation_ttl_ms = 20_000;
            c
        }),
        rate_budget_ppm: 100_000,
        node_rates: vec![(1, 100_000)],
        ..delegated_holder_cut_config()
    }
}

/// `strict-isolated-reader-beats` without its offset, with a 30 s lease and
/// 30 s read delegations, the reader isolated for 40 s and reading on (the
/// workload of the strict-reader rate configuration, without a rate).
fn strict_isolated_reader_long_config() -> SimConfig {
    let base = strict_isolated_reader_beats_config();
    let mut faults: Vec<ScheduledFault> = Vec::new();
    for f in base.faults {
        faults.push(match f.kind {
            FaultKind::IsolateReader { node, .. } => ScheduledFault {
                at_ms: f.at_ms,
                kind: FaultKind::IsolateReader {
                    node,
                    for_ms: 40_000,
                },
            },
            FaultKind::CutS3 { node, .. } => ScheduledFault {
                at_ms: f.at_ms,
                kind: FaultKind::CutS3 {
                    node,
                    for_ms: 40_000,
                },
            },
            _ => f,
        });
    }
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c.ttl_ms = 30_000;
            c.read_delegation_ttl_ms = 30_000;
            c
        }),
        clock_offsets: Vec::new(),
        faults,
        client_plan: vec![(1, 0, 2), (2, 1_600, 25), (3, 600, 400)],
        ops_per_client: 400,
        ..base
    }
}

/// `strict-isolated-reader-long` with the node that acknowledges the write
/// after the seal (node 2, the successor) 20 % fast: its floor `W + read +
/// 2M = 32 000` ms ends after 26 700 ms of real time from its marker, while
/// the isolated reader honours the delegation it last sent (before the
/// crash) to 30 000 - 500 ms after that.
fn strict_reader_rate_fast_config() -> SimConfig {
    SimConfig {
        rate_budget_ppm: 200_000,
        node_rates: vec![(2, 200_000)],
        ..strict_isolated_reader_long_config()
    }
}

/// Control: every node's rate within a 1 000 ppm budget (alternating signs).
/// Clean before and after: the rates are below the sim's resolution.
fn locks_rate_within_budget_config() -> SimConfig {
    SimConfig {
        rate_budget_ppm: 1_000,
        node_rates: vec![(1, 1_000), (2, -1_000), (3, 1_000)],
        ..locks_config()
    }
}

/// A locker inside a critical section is suspended for 3 s (its raw and
/// monotonic clocks stop, its wall and boot clocks run on). Clean on the
/// parent: the honour clock counts the suspend (`S`) and the grant lapses
/// under it. With `S` left out of the honour reading (a mutant) the locker
/// honours its grant across the suspend while the owner has outwaited it.
fn locks_locker_suspend_config() -> SimConfig {
    SimConfig {
        random_faults: 0,
        lock_ios: (20, 40),
        lock_io_ms: (40, 200),
        faults: vec![ScheduledFault {
            at_ms: 2_000,
            kind: FaultKind::Suspend {
                who: sim::run::StepOf::Locker,
                ms: 3_000,
            },
        }],
        ..locks_partition_config()
    }
}

/// The strict holder is suspended for 2.5 s, beats on a thread: its beats
/// and answers stop, a backup seals it, and on resume its liveness window
/// (`liveness_fresh`, honour clock, counting the suspend) must not answer a
/// strict read after the seal. Clean on the parent; a mutant that drops `S`
/// answers (close-to-open).
fn strict_holder_suspend_config() -> SimConfig {
    SimConfig {
        holder_beats: sim::run::HolderBeats::Thread,
        random_faults: 0,
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::Suspend {
                who: sim::run::StepOf::Holder,
                ms: 2_500,
            },
        }],
        ..backup_strict_config()
    }
}

/// `locks-root-step-forward`'s workload with the root restarted (300 ms, its
/// disk) and its wall stepped forward 3 s while it is down, twice: the
/// restarted root re-adopts its lease and loads its lock quarantine from a
/// persisted wall deadline, which the step shortens (the horizon is at most
/// `lock + M + 1 s` ahead): it grants over a grant still honoured.
fn locks_root_restart_step_forward_config() -> SimConfig {
    let restart = |at_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::CrashHolderStepped {
            restart_ms: 300,
            step_ms: 3_000,
        },
    };
    SimConfig {
        lock_ios: (6, 20),
        lock_io_ms: (40, 200),
        faults: vec![restart(3_000), restart(7_000)],
        ..locks_config()
    }
}

/// The same through a step while the root is up, after its last horizon
/// write, followed by a restart.
fn locks_root_step_forward_then_restart_config() -> SimConfig {
    let step_then_restart = |at_ms| {
        [
            step_fault(at_ms, sim::run::StepOf::Holder, 3_000),
            ScheduledFault {
                at_ms: at_ms + 600,
                kind: FaultKind::CrashHolder {
                    restart_ms: Some(300),
                    keep_journal: true,
                },
            },
        ]
    };
    SimConfig {
        lock_ios: (6, 20),
        lock_io_ms: (40, 200),
        faults: step_then_restart(3_000)
            .into_iter()
            .chain(step_then_restart(7_000))
            .collect(),
        ..locks_config()
    }
}

/// The same across a reboot of the root (a new boot id, the elapsed clocks
/// from 0) with the wall stepped forward 3 s while it is down.
fn locks_root_reboot_step_forward_config() -> SimConfig {
    let reboot = |at_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::RebootNode {
            node: 0,
            restart_ms: 300,
            step_ms: 3_000,
        },
    };
    SimConfig {
        lock_ios: (6, 20),
        lock_io_ms: (40, 200),
        faults: vec![reboot(3_000), reboot(7_000)],
        ..locks_config()
    }
}

/// Cost control: the root rebooted with no step. Clean before and after;
/// after the boot-stamped horizons the first grant after the reboot waits
/// about the lock span (a persisted horizon of another boot is waited out
/// whole).
fn locks_root_reboot_config() -> SimConfig {
    let reboot = |at_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::RebootNode {
            node: 0,
            restart_ms: 300,
            step_ms: 0,
        },
    };
    SimConfig {
        lock_ios: (6, 20),
        lock_io_ms: (40, 200),
        faults: vec![reboot(3_000), reboot(7_000)],
        ..locks_config()
    }
}

/// `strict-isolated-reader`'s reader (node 3, reading only, cut from the others
/// and from S3 for 8 s, holding a read delegation) and the holder restarted
/// (300 ms) with its wall stepped forward 7 s while it is down: the read
/// horizon it persisted (`read_ttl + M + 1 s`, about 6.5 s ahead) is
/// shortened by the step, the restarted holder acknowledges a write while the
/// isolated reader still serves from its delegation.
fn strict_holder_restart_step_forward_config() -> SimConfig {
    let base = strict_isolated_reader_config();
    SimConfig {
        faults: base
            .faults
            .iter()
            .filter(|f| !matches!(f.kind, FaultKind::CrashHolder { .. }))
            .cloned()
            .chain([ScheduledFault {
                at_ms: 1_500,
                kind: FaultKind::CrashHolderStepped {
                    restart_ms: 300,
                    step_ms: 7_000,
                },
            }])
            .collect(),
        ..base
    }
}

/// A promiser (flex configuration, `epoch_slack 1`) crashed with its wall
/// stepped forward by the promise's TTL while it is down, restarted, then an
/// epoch outage: it joins an epoch before its persisted promise ran out
/// (`promise_join_begin`'s wall fallback). Hard to reach; the meta unit test
/// of C7a's third commit is its fail-before.
fn flex_promiser_restart_step_forward_config() -> SimConfig {
    let promise_ttl = sim::run::flex_core_config(1, 1).promise_ttl_ms as i64;
    SimConfig {
        random_faults: 0,
        faults: vec![
            ScheduledFault {
                at_ms: 1_000,
                kind: FaultKind::RebootNode {
                    node: 2,
                    restart_ms: 300,
                    step_ms: promise_ttl,
                },
            },
            ScheduledFault {
                at_ms: 1_700,
                kind: FaultKind::EpochOutage {
                    members: 2,
                    for_ms: 3_000,
                },
            },
        ],
        ..flex_config()
    }
}

/// `locks-failover-backup-renew`; the first node to arm a takeover floor (the
/// successor, after its marker) is killed, restarted 300 ms later with its
/// disk, re-adopts its own lease and (the floor is in memory only) may grant
/// inside the predecessor's window. Measured clean on the parent: the lock
/// mirror the successor installed (and persisted as a lock horizon) covers
/// the grants of this geometry (see PROGRESS), so it stays a regression
/// configuration until the floor is persisted.
fn fast_takeover_successor_restart_in_floor_config() -> SimConfig {
    SimConfig {
        faults: locks_failover_backup_renew_config()
            .faults
            .into_iter()
            .chain([ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::CrashSuccessorInFloor {
                    restart_ms: 300,
                    after_marker: true,
                },
            }])
            .collect(),
        ..locks_failover_backup_renew_config()
    }
}

/// The strict variant: acknowledgements instead of grants. The isolated
/// reader (`strict-isolated-reader`) holds a read delegation from the
/// crashed holder; the successor, killed inside its floor and restarted,
/// acknowledges a write the reader then misses.
fn fast_takeover_successor_restart_in_floor_strict_config() -> SimConfig {
    SimConfig {
        faults: strict_isolated_reader_config()
            .faults
            .into_iter()
            .chain([ScheduledFault {
                at_ms: 1_600,
                kind: FaultKind::CrashSuccessorInFloor {
                    restart_ms: 300,
                    after_marker: true,
                },
            }])
            .collect(),
        ..strict_isolated_reader_config()
    }
}

/// The same through the window between the claim CAS (the gate, the marker
/// not landed) and the marker.
fn fast_takeover_successor_restart_before_marker_config() -> SimConfig {
    SimConfig {
        faults: locks_failover_backup_renew_config()
            .faults
            .into_iter()
            .chain([ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::CrashSuccessorInFloor {
                    restart_ms: 300,
                    after_marker: false,
                },
            }])
            .collect(),
        ..locks_failover_backup_renew_config()
    }
}

/// M4's own-lease arm (plan 43 C7a, third commit): `backup` with the holder
/// restarted (300 ms) and its wall stepped forward 7 s (the lease's 6 s TTL
/// passes on the stepped wall).
fn holder_restart_step_forward_readopt_config() -> SimConfig {
    SimConfig {
        random_faults: 0,
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::CrashHolderStepped {
                restart_ms: 300,
                step_ms: 7_000,
            },
        }],
        ..backup_config()
    }
}

/// Plan 43 C7a: the simulated clock model. A rate moves a node's boot, raw
/// and monotonic clocks and not its wall (here with no budget, which reads
/// the oscillator as it is). A node at +50 000 ppm for a second reads
/// 1 050 ms on WAIT and HONOUR and 1 000 ms on the wall; set to -100 000
/// ppm it reads 900 ms for the next second.
#[tokio::test(start_paused = true)]
async fn a_simulated_rate_moves_the_elapsed_clocks_and_not_the_wall() {
    let truth = sim::clock::Clock::start();
    let state = sim::clock::NodeClockState::new(2, 50_000, false);
    let node = truth.for_node(0, state.clone(), (2 << 32) | 1, 0);
    let n0 = node.now();
    tokio::time::advance(std::time::Duration::from_millis(1_000)).await;
    let n1 = node.now();
    assert_eq!(n1.wall.0 - n0.wall.0, 1_000);
    assert_eq!(n1.wait.0 - n0.wait.0, 1_050);
    assert_eq!(n1.honour.0 - n0.honour.0, 1_050);
    state.set_rate(-100_000, truth.elapsed_ns());
    tokio::time::advance(std::time::Duration::from_millis(1_000)).await;
    let n2 = node.now();
    assert_eq!(n2.wall.0 - n1.wall.0, 1_000);
    assert_eq!(n2.wait.0 - n1.wait.0, 900);
    // HONOUR follows a wall that runs ahead of the slow oscillator (P5:
    // HONOUR >= WALL), by the 50 ms it lagged in the first second and no
    // more: 900 + 50.
    assert_eq!(n2.honour.0 - n1.honour.0, 950);
    assert!(n2.honour.0 >= n2.wall.0);
    assert_eq!(state.ppm_at(truth.elapsed_ns()), -100_000);
    // The rate is the host's: a new incarnation reads it too.
    let reborn = truth.for_node(0, state.clone(), (2 << 32) | 2, 0);
    let (r0, t0) = (reborn.now(), truth.elapsed_ms());
    tokio::time::advance(std::time::Duration::from_millis(1_000)).await;
    let r1 = reborn.now();
    assert_eq!(truth.elapsed_ms() - t0, 1_000);
    assert_eq!(r1.wait.0 - r0.wait.0, 900);
}

/// Plan 43 C7a: a suspended host's raw and monotonic clocks stop while its
/// boot clock and wall run on: WAIT does not count the suspend, HONOUR does
/// (through `S`), and a timer armed across it fires after the node resumes.
#[tokio::test(start_paused = true)]
async fn a_simulated_suspend_stops_wait_and_not_honour_or_the_wall() {
    let truth = sim::clock::Clock::start();
    let state = sim::clock::NodeClockState::new(2, 0, false);
    let node = truth.for_node(0, state.clone(), (2 << 32) | 1, 0);
    tokio::time::advance(std::time::Duration::from_millis(500)).await;
    let n0 = node.now();
    state.suspend(truth.elapsed_ns(), 3_000);
    // A timer for 1 s of wait time, armed as the suspend begins: it is due
    // 3 s late.
    let due = node.instant_at_wait(n0.wait.plus(1_000));
    assert_eq!(
        due - tokio::time::Instant::now(),
        std::time::Duration::from_millis(4_000)
    );
    tokio::time::advance(std::time::Duration::from_millis(5_000)).await;
    let n1 = node.now();
    assert_eq!(n1.wall.0 - n0.wall.0, 5_000);
    assert_eq!(n1.honour.0 - n0.honour.0, 5_000);
    assert_eq!(n1.wait.0 - n0.wait.0, 2_000);
}

/// Plan 43 C7a: a reboot restarts boot, raw and monotonic from 0 and
/// changes the boot id; a restart of the process on the same boot does not.
#[tokio::test(start_paused = true)]
async fn a_simulated_reboot_changes_the_boot_id_and_restarts_the_elapsed_clocks() {
    let truth = sim::clock::Clock::start();
    let state = sim::clock::NodeClockState::new(3, 0, false);
    tokio::time::advance(std::time::Duration::from_millis(2_000)).await;
    let a = truth.for_node(0, state.clone(), (3 << 32) | 1, 0);
    let again = truth.for_node(0, state.clone(), (3 << 32) | 2, 0);
    assert_eq!(a.boot_id(), again.boot_id());
    assert_ne!(a.boot_id(), 0);
    state.reboot(truth.elapsed_ns());
    tokio::time::advance(std::time::Duration::from_millis(700)).await;
    let b = truth.for_node(0, state.clone(), (3 << 32) | 3, 0);
    assert_ne!(a.boot_id(), b.boot_id());
    assert_eq!(b.boot_id() >> 32 & 0x7fff_ffff, 3);
    // The elapsed sources restart: the new boot's raw clock reads the 700
    // ms since it began (a timer for 1 s of its wait clock is due after 1 s).
    let n = b.now();
    let due = b.instant_at_wait(n.wait.plus(1_000));
    assert_eq!(
        due - tokio::time::Instant::now(),
        std::time::Duration::from_millis(1_000)
    );
    // The wall runs on through the reboot.
    assert_eq!(b.wall(), truth.wall());
}

/// Plan 43 C7a: a node's WAIT inverts through its rate (a timer for 1 100
/// ms of wait time on an oscillator 10 % fast is due after 1 000 ms), and a
/// node with no fault reads exactly the paused clock's whole milliseconds, as
/// before C7a.
#[tokio::test(start_paused = true)]
async fn a_simulated_timer_inverts_the_rate_and_a_plain_clock_is_the_paused_one() {
    let truth = sim::clock::Clock::start();
    let fast = truth.for_node(
        0,
        sim::clock::NodeClockState::new(2, 100_000, false),
        (2 << 32) | 1,
        0,
    );
    let plain = truth.for_node(
        0,
        sim::clock::NodeClockState::new(3, 0, false),
        (3 << 32) | 1,
        0,
    );
    tokio::time::advance(std::time::Duration::from_millis(1_000)).await;
    let (f, p) = (fast.now(), plain.now());
    let t0 = tokio::time::Instant::now();
    assert_eq!(
        fast.instant_at_wait(f.wait.plus(1_100)) - t0,
        std::time::Duration::from_millis(1_000)
    );
    assert_eq!(
        plain.instant_at_wait(p.wait.plus(1_100)) - t0,
        std::time::Duration::from_millis(1_100)
    );
    // Whole milliseconds of true time, wall included.
    let base_ms = truth.wall().0 - truth.elapsed_ms() as i64;
    assert_eq!(p.wait.0, base_ms + 1_000);
    assert_eq!(p.honour.0, p.wait.0);
    assert_eq!(p.wall.0, base_ms + 1_000);
}

/// Plan 43 C7a (commit 2): a budgeted clock's WAIT runs slow and its HONOUR
/// fast by the budget, and a timer's instant inverts both the rate and that
/// scaling: whatever the oscillator (within the budget or not), the node's
/// WAIT reads the target at the instant, and it is not early by more than
/// the millisecond of the fixed point's rounding. (With a budget of 0 the
/// instant is the raw clock's, as before.)
#[tokio::test(start_paused = true)]
async fn a_simulated_timer_inverts_the_budget_scaling() {
    let truth = sim::clock::Clock::start();
    for (rate, budget) in [
        (0i64, 1_000u32),
        (1_000, 1_000),
        (-1_000, 1_000),
        (80_000, 80_000),
        (100_000, 100_000),
        (0, 300_000),
    ] {
        let state = sim::clock::NodeClockState::new(2, rate, false);
        let node = truth.for_node(0, state, (2 << 32) | 1, budget);
        tokio::time::advance(std::time::Duration::from_millis(777)).await;
        for after in [1u64, 1_000, 20_000] {
            let target = node.now().wait.plus(after);
            let due = node.instant_at_wait(target);
            let t0 = tokio::time::Instant::now();
            tokio::time::sleep_until(due).await;
            assert!(
                node.wait() >= target,
                "rate {rate} budget {budget} after {after}: WAIT {} < {}",
                node.wait().0,
                target.0
            );
            // The instant is where the scaling puts it, give or take the
            // rounding.
            let late = (due - t0).as_secs_f64() * 1_000.0;
            let exact = after as f64 * (1e6 + budget as f64) / (1e6 + rate as f64);
            assert!(
                (late - exact).abs() <= 3.0,
                "rate {rate} budget {budget} after {after}: {late} {exact}"
            );
        }
    }
    // HONOUR and WAIT of a node on an exact oscillator.
    let node = truth.for_node(
        0,
        sim::clock::NodeClockState::new(3, 0, false),
        (3 << 32) | 1,
        1_000,
    );
    let n0 = node.now();
    tokio::time::advance(std::time::Duration::from_millis(100_000)).await;
    let n1 = node.now();
    assert_eq!(n1.wait.0 - n0.wait.0, 99_900);
    assert_eq!(n1.honour.0 - n0.honour.0, 100_100);
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

// ---- runtime lag: a live holder is late, not dead ----

/// A host that holds a node's runtime up (busy workers, a starved
/// process) without stopping it: the lease holder's, then its backup's,
/// for 1.0–2.5 s each (the takeover window here is 1 s), with the
/// holder writing throughout. Lag is latency, never a failover: a seal
/// or a takeover of the live holder fails the seed (`live_holder`),
/// and every acknowledged op stays (`strict_durability`). The holder's
/// off-core heartbeat runs on its own thread, as the daemon's does.
fn backup_runtime_lag_config() -> SimConfig {
    SimConfig {
        clients_per_node: 2,
        ops_per_client: 30,
        random_faults: 0,
        join_fresh: false,
        holder_beats: sim::run::HolderBeats::Thread,
        live_holder: true,
        faults: runtime_lag_faults(sim::run::LagOf::Backup),
        ..backup_config()
    }
}

/// The same under `ack=s3`: the holder's runtime, then a subscriber's
/// (no backups; any peer watches the holder's silence on its stream).
fn acks3_runtime_lag_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::ack_s3_core_config),
        faults: runtime_lag_faults(sim::run::LagOf::NonHolder),
        ..backup_runtime_lag_config()
    }
}

/// The backup lags (1.0–2.5 s, its peers stamped up to 0.6 s after) and
/// the holder dies in the middle of it: the lapse discount must not keep
/// a dead holder. The backup still seals and takes over — within its
/// bound, a window plus one of its own running time plus its lapse, and
/// in time for the claim grace (2T past the lease's expiry), so nothing
/// acknowledged is lost (`strict_durability`).
fn backup_runtime_lag_then_crash_config() -> SimConfig {
    SimConfig {
        live_holder: false,
        faults: vec![
            ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::RuntimeLag {
                    who: sim::run::LagOf::Backup,
                    for_ms: (1_000, 2_500),
                    stamp_lag_ms: (0, 600),
                },
            },
            ScheduledFault {
                at_ms: 1_700,
                kind: FaultKind::CrashHolder {
                    restart_ms: None,
                    keep_journal: true,
                },
            },
        ],
        ..backup_runtime_lag_config()
    }
}

/// See [`backup_runtime_lag_then_crash_config`]: the dead holder is
/// sealed by its lagging backup, within the bound.
#[test]
fn a_lagging_backup_still_seals_a_dead_holder() {
    let t = run_m9(
        "backup-runtime-lag-then-crash",
        backup_runtime_lag_then_crash_config(),
        0..120,
    );
    assert!(t.seals >= 60, "the lagging backup rarely sealed: {t:?}");
    // 2T of the backup's running time plus its lapse (at most 2.5 s, and
    // its peers' 0.6 s), then the read, the seal and the takeover.
    let bound = 2 * 1_000 + 2_500 + 600 + 1_500;
    let late: Vec<u64> = t
        .failovers
        .iter()
        .copied()
        .filter(|ms| *ms > bound)
        .collect();
    assert!(
        late.is_empty(),
        "failovers past {bound} ms: {late:?}; {}",
        t.failover_dist()
    );
}

/// Lag is no reconfiguration either: the backup's runtime held up for
/// 0.3–1.0 s, its peers stamped up to 0.2 s after (with a beat interval
/// on top, under the ack timeout plus a window: 1.6 s here), then the
/// holder's for 1.0–2.5 s, then the backup's again. The holder keeps its
/// backup throughout: no acknowledgement timeout, so no reconfiguration
/// CAS and no re-sync from zero.
fn backup_lag_keeps_its_backup_config() -> SimConfig {
    let lag = |at_ms, who, for_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::RuntimeLag {
            who,
            for_ms,
            stamp_lag_ms: (0, 200),
        },
    };
    SimConfig {
        faults: vec![
            lag(1_200, sim::run::LagOf::Backup, (300, 1_000)),
            lag(4_500, sim::run::LagOf::Holder, (1_000, 2_500)),
            lag(8_000, sim::run::LagOf::Backup, (300, 1_000)),
        ],
        keeps_backups: true,
        ..backup_runtime_lag_config()
    }
}

#[test]
fn lag_does_not_cost_the_holder_its_backup() {
    let mut dropped = Vec::new();
    let mut added = 0;
    for seed in 0..120u64 {
        let r = run_seed(seed, backup_lag_keeps_its_backup_config()).unwrap_or_else(|e| {
            panic!(
                "seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=backup-lag-keeps-its-backup"
            )
        });
        added += r.stats.values().map(|s| s.backups_added).sum::<u64>();
        let timeouts: u64 = r.stats.values().map(|s| s.backup_ack_timeouts).sum();
        if timeouts > 0 {
            dropped.push((seed, timeouts, r.faults.clone()));
        }
    }
    assert!(added >= 60, "backups rarely chosen: {added}");
    assert!(
        dropped.is_empty(),
        "lagging backups were dropped: {dropped:#?}"
    );
}

/// The holder's lag and the watcher's (`other`), each 1.0–2.5 s, the
/// peers' backlog stamped up to 0.6 s after.
fn runtime_lag_faults(other: sim::run::LagOf) -> Vec<ScheduledFault> {
    let lag = |at_ms, who| ScheduledFault {
        at_ms,
        kind: FaultKind::RuntimeLag {
            who,
            for_ms: (1_000, 2_500),
            stamp_lag_ms: (0, 600),
        },
    };
    vec![
        lag(1_200, other),
        lag(4_500, sim::run::LagOf::Holder),
        lag(8_000, other),
    ]
}

/// The claim-grace corner of the lapse discount: the holder is cut from
/// S3 (it cannot renew, its lease runs out while it is alive and its
/// backup still gets appends), and its backup's runtime is held up
/// across that expiry (1.5–2.0 s, from about 0.5 s before it). The
/// backup must still read, seal and claim inside the 2T a non-listed
/// node waits past the expiry — or the other nodes' writes take the
/// lease over by TTL from a log without the backup's tail, and
/// acknowledged writes are lost or reordered (`strict_durability`,
/// long-backup seed 802943's class). `beats_lost`: the liveness lane is
/// broken all along, so no beat ever names the expiry and only the
/// lease the backup read does (the seal watch's unknown-expiry arm
/// before that fallback: it waited a window past the lapse's end).
fn backup_lag_at_expiry_config(beats_lost: bool) -> SimConfig {
    SimConfig {
        clients_per_node: 2,
        ops_per_client: 30,
        random_faults: 0,
        join_fresh: false,
        holder_beats: sim::run::HolderBeats::Thread,
        beats_lost,
        faults: vec![
            ScheduledFault {
                at_ms: 1_100,
                kind: FaultKind::CutS3Holder { for_ms: 9_000 },
            },
            ScheduledFault {
                at_ms: 5_600,
                kind: FaultKind::RuntimeLag {
                    who: sim::run::LagOf::Backup,
                    for_ms: (1_500, 2_000),
                    stamp_lag_ms: (0, 400),
                },
            },
        ],
        ..backup_config()
    }
}

/// See [`backup_lag_at_expiry_config`]: nothing acknowledged is lost, with
/// the expiry known from beats or from a lease read, the P2P handlers'
/// stamps handed over before the timers after the lapse (the daemon's
/// order) or after them, and the backup seals in time (in at least 100
/// of each 120 seeds; before, with no beat naming the expiry and timers
/// first, it sealed in 1: in the others it waited a window past its
/// lapse and the expired lease was claimed first — by itself, by TTL,
/// unsealed, or by another node). The seeds of 0..2000 that lost the
/// order of acknowledged writes before are pinned, 1788 and 1791 of
/// `backup-lag-at-expiry-beats-lost` among them: with the liveness lane
/// broken for the whole tenure and no read of the lease since the holder
/// took it, nothing but the appends named the expiry (they carry it).
#[test]
fn a_backup_lapsing_at_its_holders_expiry_still_claims_in_time() {
    let configs = |label: &str| match label {
        "backup-lag-at-expiry" => backup_lag_at_expiry_config(false),
        "backup-lag-at-expiry-timers-first" => timers_first(backup_lag_at_expiry_config(false)),
        "backup-lag-at-expiry-beats-lost" => backup_lag_at_expiry_config(true),
        _ => timers_first(backup_lag_at_expiry_config(true)),
    };
    for label in [
        "backup-lag-at-expiry",
        "backup-lag-at-expiry-timers-first",
        "backup-lag-at-expiry-beats-lost",
        "backup-lag-at-expiry-beats-lost-timers-first",
    ] {
        let t = run_m9(label, configs(label), 0..120);
        assert!(t.seals >= 100, "{label}: the backup rarely sealed: {t:?}");
    }
    let pinned: [(&str, &[u64]); 3] = [
        // Stamps first: the backlog's credit re-armed the watch a window
        // past the lapse's end, past the claim grace (before the watch
        // read the lease at the expiry it knew of: here its beats named
        // it, there only a read of the lease did).
        (
            "backup-lag-at-expiry",
            &[60, 260, 609, 644, 830, 1034, 1106, 1252, 1624, 1852, 1863],
        ),
        // (1788 and 1791: the beats lost for the whole tenure and no read
        // of the lease, so only the appends named the expiry.)
        (
            "backup-lag-at-expiry-beats-lost",
            &[655, 837, 981, 1025, 1788, 1791],
        ),
        // Timers first: the unknown-expiry arm waiting a window past the
        // lapse, or (655) the backlog heard during the lease read
        // re-arming the watch past the expired lease.
        (
            "backup-lag-at-expiry-beats-lost-timers-first",
            &[655, 837, 981, 1025, 1788, 1791],
        ),
    ];
    for (label, seeds) in pinned {
        for &seed in seeds {
            if let Err(e) = run_seed(seed, configs(label)) {
                panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}");
            }
        }
    }
}

/// The lag-at-expiry configurations with a `Pause` of the cut holder's
/// backup on top (plan 43 C7a's clock-fault mix found them as a suspend:
/// seeds 5, 275 and 277): the backup seals, or is about to, and stands
/// still past the claim grace; the third node claims the lapsed lease by
/// TTL from a log without the tail, and the rows both copies held come
/// back as the deposed holder's replays by rid, after ops invoked later
/// ("returned before ... but follows it in the log"). Two failures — the
/// holder cut from S3 and its backup stopped — are beyond one backup's
/// budget: the run records them (`Cluster::note_s3_cut`) and waives strict
/// durability, so the acknowledged ops rolled back are tentative (they
/// need not be in the log, nor in real-time order); every other check
/// stays. Before, the cut holder counted as a live
/// copy and these runs failed. The same pause of the holder itself or of
/// the third node is no second failure: nothing is recorded. (Plan 43 C1
/// deletes the TTL claim without the tail and this relaxation with it:
/// then these seeds must pass strictly.)
#[test]
fn a_backup_paused_while_its_holder_is_cut_from_s3_is_beyond_the_budget() {
    let configs = |label: &str| match label {
        "backup-lag-at-expiry" => backup_lag_at_expiry_config(false),
        "backup-lag-at-expiry-timers-first" => timers_first(backup_lag_at_expiry_config(false)),
        "backup-lag-at-expiry-beats-lost" => backup_lag_at_expiry_config(true),
        "backup-lag-at-expiry-beats-lost-timers-first" => {
            timers_first(backup_lag_at_expiry_config(true))
        }
        "backup-lag-in-margin-cut" => backup_lag_in_margin_cut_config(),
        other => panic!("unknown configuration {other}"),
    };
    // (configuration, seed, the pause, whether it is a second failure)
    let cases: [(&str, u64, &str, bool); 9] = [
        (
            "backup-lag-at-expiry-beats-lost",
            5,
            "pause1@7363:2416",
            true,
        ),
        ("backup-lag-at-expiry", 275, "pause1@6500:2000", true),
        ("backup-lag-at-expiry", 277, "pause2@7000:3000", true),
        (
            "backup-lag-at-expiry-timers-first",
            275,
            "pause1@5500:3000",
            true,
        ),
        (
            "backup-lag-at-expiry-beats-lost-timers-first",
            5,
            "pause1@7000:2000",
            true,
        ),
        ("backup-lag-in-margin-cut", 277, "pause2@7500:3000", true),
        // The third node; the cut holder itself.
        (
            "backup-lag-at-expiry-beats-lost",
            5,
            "pause3@7363:2416",
            false,
        ),
        ("backup-lag-at-expiry", 277, "pause3@7000:3000", false),
        ("backup-lag-at-expiry", 277, "pause1@7000:3000", false),
    ];
    for (label, seed, pause, second) in cases {
        let r = run_seed(seed, config_variant(configs(label), pause)).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}+{pause}")
        });
        assert_eq!(
            !r.durability_budget_exceeded.is_empty(),
            second,
            "{label} seed {seed} +{pause}: {:?}",
            r.durability_budget_exceeded
        );
    }
}

/// 2026-10-16 `snapsched` under host load: the holder's runtime is held
/// up across a renewal (2.3-2.6 s from 6.0 s; the lease here is 6 s, its
/// margin 0.5 s and the renewal due 3 s before the expiry), so the expiry
/// its beats named (about 9.1-9.8 s for the lease the holder took at
/// 0.2-0.8 s and renewed once) comes within the margin before it has
/// renewed again; its backup's runtime is held up (1.5-1.9 s from 6.8 s)
/// so that the lapse ends inside that margin with the holder still
/// silent, and the liveness lane to the backup is closed meanwhile
/// (`beats_lost_in_lag`), so no backlog of beats follows the lapse. The
/// backup's silence, counted plain inside the margin, is more than a
/// window; counted in its own attentive time it is none. A listed backup
/// that finds the lease unexpired must not seal it: the holder renews as
/// soon as its runtime runs again.
fn backup_lag_in_margin_config() -> SimConfig {
    let lag = |at_ms, who, for_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::RuntimeLag {
            who,
            for_ms,
            stamp_lag_ms: (0, 200),
        },
    };
    SimConfig {
        beats_lost_in_lag: true,
        faults: vec![
            lag(6_000, sim::run::LagOf::Holder, (2_300, 2_600)),
            lag(6_800, sim::run::LagOf::Backup, (1_500, 1_900)),
        ],
        ..backup_runtime_lag_config()
    }
}

/// The twin of [`backup_lag_in_margin_config`] with a holder that is
/// truly cut from S3 (it cannot renew, its lease runs out) in place of
/// the holder's lag: the same backup lapse across the expiry, the same
/// lost beats. The backup still seals in time.
fn backup_lag_in_margin_cut_config() -> SimConfig {
    SimConfig {
        beats_lost_in_lag: true,
        ..backup_lag_at_expiry_config(false)
    }
}

/// A backup whose runtime is held up with its live holder's lease at the
/// expiry margin does not seal it ([`backup_lag_in_margin_config`]); the
/// same lapse with a holder that is cut off still seals in time (in at
/// least 100 of 120 seeds, as `backup-lag-at-expiry`), and the backup
/// that counts plain silence does seal the live holder
/// (`backup-lag-in-margin-unaware`: the non-vacuity half).
#[test]
fn a_backup_frozen_with_its_holders_lease_at_the_margin_does_not_seal_it() {
    let t = run_m9(
        "backup-lag-in-margin",
        backup_lag_in_margin_config(),
        0..120,
    );
    assert_eq!(t.seals, 0, "{t:?}");
    assert!(t.backups_added >= 60, "backups rarely chosen: {t:?}");
    let t = run_m9(
        "backup-lag-in-margin-cut",
        backup_lag_in_margin_cut_config(),
        0..120,
    );
    assert!(t.seals >= 100, "the backup rarely sealed: {t:?}");
    let cfg = unaware(backup_lag_in_margin_config());
    let deposed = (0..60u64)
        .filter(|seed| {
            run_seed(*seed, cfg.clone())
                .err()
                .is_some_and(|e| e.contains("a live holder was sealed or taken over"))
        })
        .count();
    eprintln!("backup-lag-in-margin-unaware: {deposed}/60 seeds deposed a live holder");
    assert!(
        deposed >= 5,
        "only {deposed}/60 seeds deposed a live holder"
    );
}

/// After a lag, the timers before what the P2P handlers stamped
/// meanwhile (`SimConfig::timers_first_after_lag`): the other order a
/// daemon's driver can see, swept beside the usual one.
fn timers_first(cfg: SimConfig) -> SimConfig {
    SimConfig {
        timers_first_after_lag: true,
        ..cfg
    }
}

/// Plain silence, and the heartbeat a task of the runtime: the watches
/// and the holder as they were (`Config::liveness_lag_aware` off). The
/// non-vacuity half of the runtime-lag configurations.
fn unaware(cfg: SimConfig) -> SimConfig {
    let core = cfg.core.clone();
    SimConfig {
        holder_beats: sim::run::HolderBeats::Runtime,
        core: std::sync::Arc::new(move |id, inc| {
            let mut c = core(id, inc);
            c.liveness_lag_aware = false;
            c
        }),
        ..cfg
    }
}

/// A host's lag on the holder and on its backup seals no live holder,
/// loses nothing acknowledged. (The sweep: `AUTHORITY_SIM_CONFIG=
/// backup-runtime-lag … --ignored sweep_config`.)
#[test]
fn backup_runtime_lag_never_seals_a_live_holder() {
    let t = run_m9("backup-runtime-lag", backup_runtime_lag_config(), 0..120);
    assert!(t.backups_added >= 60, "backups rarely chosen: {t:?}");
    assert_eq!(t.seals, 0, "{t:?}");
}

/// The same under `ack=s3`: no fast takeover of a live holder.
#[test]
fn acks3_runtime_lag_never_takes_a_live_holder_over() {
    let t = run_m9("acks3-runtime-lag", acks3_runtime_lag_config(), 0..120);
    assert_eq!(t.s3_fast_takeovers, 0, "{t:?}");
}

/// The runtime-lag checks are not vacuous: with the watches measuring
/// plain silence and the heartbeat on the runtime, the same lags seal
/// (or, under `ack=s3`, take over) live holders.
#[test]
fn runtime_lag_without_lag_awareness_deposes_live_holders() {
    for (label, cfg) in [
        (
            "backup-runtime-lag-unaware",
            unaware(backup_runtime_lag_config()),
        ),
        (
            "acks3-runtime-lag-unaware",
            unaware(acks3_runtime_lag_config()),
        ),
    ] {
        let deposed = (0..60u64)
            .filter(|seed| {
                run_seed(*seed, cfg.clone())
                    .err()
                    .is_some_and(|e| e.contains("a live holder was sealed or taken over"))
            })
            .count();
        eprintln!("{label}: {deposed}/60 seeds deposed a live holder");
        assert!(
            deposed >= 10,
            "{label}: only {deposed}/60 seeds deposed a live holder"
        );
    }
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

/// `long_backup` seed 52088 (fix "a release in doubt", PROGRESS.md): the
/// holder's release CAS timed out after it had landed (an
/// applied-then-timeout `leases/` PUT), and the holder kept the lease.
/// Another node claimed the released object at once; the old holder,
/// under `Local`, went on executing and acknowledging its own clients'
/// writes until a foreign segment told it, and they were replayed by rid
/// after the new holder's ops (the strict acknowledgement-order check).
/// The release's re-read now says whether it landed, and the node admits
/// nothing meanwhile.
///
/// 56774 (fix "a deposed journal answered at the deadline"): a deposed
/// holder whose recovery had not run yet answered its client from its
/// own journaled refusal at the deadline (a deposed node holds no lease,
/// and the not-holding branch took every `completed` row for the log's);
/// the rename then ran by rid under the next holder and succeeded.
#[test]
fn regression_long_backup_release_in_doubt_and_deposed_journal() {
    for seed in [52_088u64, 56_774] {
        let report = run_seed(seed, long_backup_config()).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=long-backup")
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

/// Chunk epoch-liveness-gap (sweep only): `flex`'s two-member outages
/// (`f = 1`, the third node keeps S3) in `locks-blips-tight`'s
/// back-to-back pairs, so a closed hold owner's fresh epoch replaces the
/// open one beside a node that may take the lease over through the
/// promise check, with the CI's random faults (crashes, partitions,
/// pauses, S3 cuts and errors) on top.
fn flex_tight_config() -> SimConfig {
    let mut faults = Vec::new();
    let mut at = 700;
    for gap in [300, 500, 700, 900] {
        for start in [at, at + 1_500 + gap] {
            faults.push(ScheduledFault {
                at_ms: start,
                kind: FaultKind::EpochOutage {
                    members: 2,
                    for_ms: 1_500,
                },
            });
        }
        at += 1_500 + gap + 1_500 + 2_500;
    }
    SimConfig {
        random_faults: 2,
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
    /// Chunk metered-own-rows: replies whose position was excused rows of
    /// the requester's own (`CoreStats::own_rows_excused`).
    own_rows_excused: u64,
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
            t.own_rows_excused += s.own_rows_excused;
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
    // Chunk metered-own-rows: a member's write deferred on the hold
    // owner's chunk wait sits below positions the member's other ops
    // observe; those are excused, not waited for.
    assert!(
        t.own_rows_excused > 0,
        "no reply ever excused the requester's own rows: {t:?}"
    );
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

// ---- chunk metered-own-rows: a non-owner's held writes ----

/// Chunk metered-own-rows: every node writes the files it creates (one
/// chunk each) on the four names all nodes share, and a metered upload
/// hold keeps each chunk on its writer for 3 s unless an op's or an
/// observed position's durability need uploads it
/// (`Action::UploadAwaited`). A non-owner's write therefore stays
/// deferred on the sequencer across the other nodes' creates, unlinks,
/// renames and reads of the same names, and across its own node's later
/// ops, whose replies excuse its own rows below their positions
/// (`Core::settle_own_rows`): its reads see its own writes, other nodes
/// still wait for the log. Backups stream ahead of S3, as in
/// `long-backup`; the CI's random faults on top.
fn metered_shared_config() -> SimConfig {
    SimConfig {
        ops_per_client: 8,
        read_ratio: 0.3,
        chunk_writes: 0.6,
        upload_hold_ms: 3_000,
        core: std::sync::Arc::new(sim::run::backup_core_config),
        ..SimConfig::default()
    }
}

/// The same without backups: every wait on a held write is an upload
/// (`OwnChunks::Upload`), never the stream.
fn metered_shared_unbacked_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::sim_core_config),
        ..metered_shared_config()
    }
}

/// Chunk metered-own-rows: linearizable, read-your-writes and converged
/// with writes held back on their writers, and the excuse fires (replies
/// excuse own rows; chunks are held back by the hold).
#[test]
fn metered_shared_writes_held_across_other_nodes_ops() {
    for (label, cfg) in [
        ("metered-shared", metered_shared_config()),
        ("metered-shared-unbacked", metered_shared_unbacked_config()),
    ] {
        let (mut excused, mut held, mut converged) = (0, 0, 0);
        for seed in 1200..1230 {
            let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| {
                panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
            });
            excused += report
                .stats
                .values()
                .map(|s| s.own_rows_excused)
                .sum::<u64>();
            held += report.held_back;
            converged += u64::from(report.converged_checked);
        }
        eprintln!("{label}: excused {excused}, held back {held}, converged {converged}/30");
        assert!(held > 0, "{label}: the hold never kept a chunk back");
        assert!(excused > 0, "{label}: no reply ever excused own rows");
    }
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
/// re-stream to it). No epoch, a failover by seal (the successor's
/// delegation floor, not the holder's return), every directory's history
/// linearizable (`run_seed`'s checks).
fn delegated_holder_cut_config() -> SimConfig {
    SimConfig {
        faults: holder_cut_crash_config(true).faults,
        read_ratio: 0.0,
        ..delegated_backup_config()
    }
}

#[test]
fn a_delegating_root_cut_from_s3_forms_no_epoch_and_fails_over() {
    // The successor waits its predecessor's delegations out for the whole
    // floor (plan 43 C7-H2: the takeover window, the read-delegation TTL,
    // the write delegations' reclaim horizon and two margins, from its
    // marker; it used to stop at the old lease's expiry, inside the TTL),
    // after the window the seal itself takes: a failover by seal lands
    // between the two bounds below, which neither the holder's return (9 s
    // after the crash, under the lower one) nor a TTL failover can meet.
    // This configuration models no holder beat (`holder_beats` is off), so
    // the cap of plan 43 C7-F F-a is inert here and the bounds are C7-H2's.
    let c = sim::run::backup_core_config(1, 1);
    let floor = c.backup_takeover_ms
        + c.read_delegation_ttl_ms
        + c.delegation_ttl_ms
        + 3 * c.expiry_margin_ms;
    let (lowest, highest) = (
        floor - 2 * c.backup_takeover_ms,
        floor + 2 * c.backup_takeover_ms + 1_000,
    );
    let mut epochs = 0;
    let mut failovers = Vec::new();
    let mut executed = 0;
    // (Seed 1610 left the set with plan 43 C7-F F-a0's live-delegation mark
    // (L3): its trajectory shifted, and the first mutation answered after
    // the crash came 1.2 s in, before the seal (a failover the measure
    // counts but no seal-and-floor one); the same 1.1 to 1.2 s class shows
    // on the parent at 1615 and 1628. 1612 takes its place.)
    for seed in (1600..1610).chain(1611..1613) {
        let report = run_seed(seed, delegated_holder_cut_config()).unwrap_or_else(|e| {
            panic!(
                "delegated-holder-cut seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=delegated-holder-cut"
            )
        });
        epochs += report.epochs_formed;
        failovers.extend(report.failover_ms.iter().copied());
        executed += report.stats.values().map(|s| s.deleg_executed).sum::<u64>();
        let seals: u64 = report.stats.values().map(|s| s.backup_takeovers).sum();
        for ms in report.failover_ms.iter().copied().filter(|ms| *ms >= 1_000) {
            assert!(
                seals > 0,
                "seed {seed}: a failover of {ms} ms with no seal-based takeover"
            );
            assert!(
                (lowest..=highest).contains(&ms),
                "seed {seed}: a failover of {ms} ms is not a seal and its floor \
                 ({lowest}..={highest} ms)"
            );
        }
    }
    eprintln!(
        "delegated-holder-cut: epochs {epochs}, failovers {failovers:?}, executed {executed}"
    );
    assert_eq!(epochs, 0, "a delegate that reaches S3 joined an epoch");
    assert!(executed > 0, "the delegates never executed");
    assert!(
        failovers.iter().any(|ms| *ms >= 1_000),
        "no failover by seal measured: {failovers:?}"
    );
}

/// Plan 43 C7-H2: `delegated-holder-cut` seeds 65 and 97. The backup that
/// seals the cut root waits out its predecessor's delegations for the whole
/// bound (it used to stop at the old lease's expiry), longer than the
/// dwell: with a node wanting the lease it idle-released the lease while
/// its takeover gate was still pending. A successor adopts the
/// predecessor's delegation table only once it can ship, which a pending
/// gate cannot, so the "root stays while a delegation is live" rule saw no
/// generation; the lease was let go with the log still naming this node
/// root of live generations, and the delegate's forwards were refused
/// `NotHolder` for ever (the run never ended). `round_release` now keeps the
/// lease through the gate and the takeover floor.
#[test]
fn regression_a_sealing_successor_keeps_the_lease_through_its_floor() {
    for seed in [65, 97] {
        run_seed(seed, delegated_holder_cut_config()).unwrap_or_else(|e| {
            panic!(
                "delegated-holder-cut seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=delegated-holder-cut"
            )
        });
    }
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
    let base_config = |label: &str| match label {
        "delegated-holder-cut" => delegated_holder_cut_config(),
        "long-delegated" => long_delegated_config(),
        "long-backup" => long_backup_config(),
        "backup-runtime-lag" => backup_runtime_lag_config(),
        "backup-lag-at-expiry" => backup_lag_at_expiry_config(false),
        "backup-lag-at-expiry-beats-lost" => backup_lag_at_expiry_config(true),
        "backup-lag-keeps-its-backup" => backup_lag_keeps_its_backup_config(),
        "backup-lag-in-margin" => backup_lag_in_margin_config(),
        "backup-lag-in-margin-cut" => backup_lag_in_margin_cut_config(),
        "backup-lag-in-margin-unaware" => unaware(backup_lag_in_margin_config()),
        "backup-runtime-lag-then-crash" => backup_runtime_lag_then_crash_config(),
        "acks3-runtime-lag" => acks3_runtime_lag_config(),
        "backup-runtime-lag-unaware" => unaware(backup_runtime_lag_config()),
        "acks3-runtime-lag-unaware" => unaware(acks3_runtime_lag_config()),
        "backup-lag-at-expiry-beats-lost-unaware" => unaware(backup_lag_at_expiry_config(true)),
        "backup-lag-at-expiry-timers-first" => timers_first(backup_lag_at_expiry_config(false)),
        "backup-lag-at-expiry-beats-lost-timers-first" => {
            timers_first(backup_lag_at_expiry_config(true))
        }
        "backup-runtime-lag-timers-first" => timers_first(backup_runtime_lag_config()),
        "acks3-runtime-lag-timers-first" => timers_first(acks3_runtime_lag_config()),
        "backup" => backup_config(),
        "acks3" => ack_s3_config(),
        "far" | "backup-far" => far_config(),
        "backup-departs" => backup_departs_config(),
        "backup-partition" => backup_partition_config(),
        "backup-reconfig-doubt" => backup_reconfig_doubt_config(),
        "backup-two" => backup_two_config(),
        "backup-strict" => backup_strict_config(),
        "backup-strict-inbox-off" => backup_strict_inbox_off_config(),
        "backup-crash" => backup_crash_config(),
        "backup-crash-slow" => SimConfig {
            s3_latency: (60, 200),
            ..backup_crash_config()
        },
        "acks3-crash" => ack_s3_crash_config(),
        // `long_backup`'s odd seeds.
        "long-acks3" => SimConfig {
            core: std::sync::Arc::new(sim::run::ack_s3_core_config),
            ..long_backup_config()
        },
        "long-sessions" => long_sessions_config(),
        "long-strict" => long_strict_config(),
        "delegated-backup" => delegated_backup_config(),
        "delegated-two-gens-root-crash" => delegated_two_gens_root_crash_config(),
        "delegated-root-gone" => delegated_root_gone_config(),
        "delegated-delegate-restart" => delegated_delegate_restart_config(),
        "long-delegated-backup" => long_delegated_backup_config(),
        "long-delegated-late-answers" => long_delegated_late_answers_config(),
        "delegated-core-stall" => delegated_core_stall_config(),
        "delegated-core-stall-lease6" => delegated_core_stall_lease6_config(),
        "delegated-core-freeze" => delegated_core_freeze_config(),
        "delegated-core-stall-forwarded" => delegated_core_stall_forwarded_config(),
        "delegated-core-stall-unlaned" => delegated_core_stall_unlaned_config(),
        "delegated-core-hang" => delegated_core_hang_config(),
        "placement-hot" => placement_hot_config(),
        "backup-hot" => backup_hot_config(),
        "flex" => flex_config(),
        "flex-crash" => flex_crash_config(),
        "flex-tight" => flex_tight_config(),
        "metered-shared" => metered_shared_config(),
        "metered-shared-unbacked" => metered_shared_unbacked_config(),
        // The lock configurations (`long_locks` runs them in sequence).
        "locks" => locks_config(),
        "locks-partition" => locks_partition_config(),
        "locks-skew" => locks_skew_config(),
        "locks-failover" => locks_failover_config(),
        "locks-failover-backup" => locks_failover_backup_config(),
        "locks-failover-backup-renew" => locks_failover_backup_renew_config(),
        "locks-long-grants" => locks_long_grants_config(),
        "locks-long-grants-seal" => locks_long_grants_seal_config(),
        "fast-takeover-skewed-successor" => fast_takeover_skewed_successor_config(),
        "double-seal-inside-the-grace" => double_seal_inside_the_grace_config(),
        "double-seal-after-the-grace" => double_seal_after_the_grace_config(),
        "locks-restart-reclaim-seal" => locks_restart_reclaim_seal_config(),
        "fast-takeover-skewed-successor-nobeats" => fast_takeover_skewed_successor_nobeats_config(),
        "fast-takeover-skewed-successor-credited" => {
            fast_takeover_skewed_successor_credited_config()
        }
        "fast-takeover-skewed-holder" => fast_takeover_skewed_holder_config(),
        "locks-s3-cut-holder-alive" => locks_s3_cut_holder_alive_config(),
        "locks-s3-cut-holder-lapse" => locks_s3_cut_holder_lapse_config(),
        "locks-late-stale-mirror" => locks_late_stale_mirror_config(),
        "locks-late-stale-mirror-naive" => locks_late_stale_mirror_naive_config(),
        "strict-isolated-reader" => strict_isolated_reader_config(),
        "strict-isolated-reader-beats" => strict_isolated_reader_beats_config(),
        "strict-isolated-reader-skewed" => strict_isolated_reader_skewed_config(),
        "strict-isolated-reader-skewed-credited" => strict_isolated_reader_skewed_credited_config(),
        "locks-faults" => locks_faults_config(),
        "locks-blips" => locks_blips_config(),
        "locks-blips-tight" => locks_blips_tight_config(),
        "locks-blips-tight-single" => locks_blips_tight_single_config(),
        "locks-blips-tight-long-lease" => locks_blips_tight_long_lease_config(),
        "locks-blips-tight-in-doubt" => locks_blips_tight_in_doubt_config(),
        "locks-blips-tight-faults" => locks_blips_tight_faults_config(),
        "locks-blips-tight-delegated" => locks_blips_tight_delegated_config(),
        "locks-pause" => locks_pause_config(),
        "locks-delegated" => locks_delegated_config(),
        "locks-delegated-production-ttl" => locks_delegated_production_ttl_config(),
        "locks-delegated-production-ttl-heavy" => locks_delegated_production_ttl_heavy_config(),
        "locks-production-ttl" => locks_production_ttl_config(),
        "locks-production-ttl-heavy" => locks_production_ttl_heavy_config(),
        "locks-unlinked-delegated" => locks_unlinked_delegated_config(),
        "locks-unlinked-delegated-hcrash" => locks_unlinked_hcrash_config(false),
        "locks-unlinked-delegated-hcrash-backup" => locks_unlinked_hcrash_config(true),
        "locks-unlinked-delegated-dcrash" => locks_unlinked_dcrash_config(true),
        "locks-unlinked-delegated-dcrash-nb" => locks_unlinked_dcrash_config(false),
        "locks-unlinked-delegated-dcrash-pause" => locks_unlinked_dcrash_pause_config(),
        "locks-unlinked-delegated-random" => locks_unlinked_random_config(false),
        "locks-unlinked-delegated-dbackup-random" => locks_unlinked_random_config(true),
        "locks-unlinked-delegated-blips" => locks_unlinked_blips_config(),
        "locks-unlinked-delegated-partition" => locks_unlinked_partition_config(),
        "locks-delegated-partition" => locks_delegated_partition_config(),
        "locks-released-delegated" => locks_released_delegated_config(),
        "locks-writes" => with_lock_writes(locks_config(), ""),
        "locks-delegated-writes" => with_lock_writes(locks_delegated_config(), "d2"),
        "locks-released-writes" => with_lock_writes(locks_released_delegated_config(), "d2"),
        "locks-failover-backup-writes" => with_lock_writes(locks_failover_backup_config(), ""),
        "locks-writer-hold" => locks_writer_hold_config(),
        "locks-writer-hold-unchecked" => locks_writer_hold_unchecked_config(),
        "locks-writer-hold-delegated" => locks_writer_hold_delegated_config(),
        "locks-writer-hold-delegated-unsettled" => locks_writer_hold_delegated_unsettled_config(),
        "locks-writer-hold-delegated-doubt" => locks_writer_hold_delegated_doubt_config(),
        "locks-stall-step" => locks_stall_step_config(),
        "locks-stall-step-delegated" => locks_stall_step_delegated_config(),
        "locks-root-step-forward" => locks_root_step_forward_config(),
        "locks-locker-step-back" => locks_locker_step_back_config(),
        "locks-holder-step-back-sealed" => locks_holder_step_back_sealed_config(),
        "fast-takeover-holder-step-forward" => fast_takeover_holder_step_forward_config(),
        "strict-isolated-reader-holder-step-forward" => {
            strict_isolated_reader_holder_step_forward_config()
        }
        "fast-takeover-successor-step-forward" => fast_takeover_successor_step_forward_config(),
        "delegated-root-step-forward" => delegated_root_step_forward_config(),
        "delegated-delegate-step-back" => delegated_delegate_step_back_config(),
        "strict-reader-step-back" => strict_reader_step_back_config(),
        "locks-root-step-back" => locks_root_step_back_config(),
        "locks-holder-step-forward" => locks_holder_step_forward_config(),
        "locks-root-rate-fast" => locks_root_rate_fast_config(),
        "locks-root-rate-skew" => locks_root_rate_skew_config(),
        "locks-partition-long-grants" => locks_partition_long_grants_config(),
        "fast-takeover-long-grants" => fast_takeover_long_grants_config(),
        "fast-takeover-capped-grants" => fast_takeover_capped_grants_config(),
        "delegated-long-delegation" => delegated_long_delegation_config(),
        "strict-isolated-reader-long" => strict_isolated_reader_long_config(),
        "fast-takeover-successor-rate-fast" => fast_takeover_successor_rate_fast_config(),
        "fast-takeover-holder-rate-fast" => fast_takeover_holder_rate_fast_config(),
        "delegated-root-rate-fast" => delegated_root_rate_fast_config(),
        "strict-reader-rate-fast" => strict_reader_rate_fast_config(),
        "locks-rate-within-budget" => locks_rate_within_budget_config(),
        "locks-locker-suspend" => locks_locker_suspend_config(),
        "strict-holder-suspend" => strict_holder_suspend_config(),
        "locks-root-restart-step-forward" => locks_root_restart_step_forward_config(),
        "locks-root-step-forward-then-restart" => locks_root_step_forward_then_restart_config(),
        "locks-root-reboot-step-forward" => locks_root_reboot_step_forward_config(),
        "locks-root-reboot" => locks_root_reboot_config(),
        "strict-holder-restart-step-forward" => strict_holder_restart_step_forward_config(),
        "flex-promiser-restart-step-forward" => flex_promiser_restart_step_forward_config(),
        "fast-takeover-successor-restart-in-floor" => {
            fast_takeover_successor_restart_in_floor_config()
        }
        "fast-takeover-successor-restart-in-floor-strict" => {
            fast_takeover_successor_restart_in_floor_strict_config()
        }
        "fast-takeover-successor-restart-before-marker" => {
            fast_takeover_successor_restart_before_marker_config()
        }
        "holder-restart-step-forward-readopt" => holder_restart_step_forward_readopt_config(),
        other => panic!("sweep_config: unknown config {other}"),
    };
    // `<config>+nomove+solo`: see `config_variant`.
    let config = move |label: &str| {
        let (base, variants) = label.split_once('+').unwrap_or((label, ""));
        variants
            .split('+')
            .filter(|v| !v.is_empty())
            .fold(base_config(base), config_variant)
    };
    // Plan 43 C7-F F-a: `AUTHORITY_SIM_BEATS=thread` runs any configuration
    // with the holder's off-core heartbeat on a thread of its own, as the
    // daemon has it: the floors of its fast takeovers are then capped by
    // the beats (the configurations that predate the cap model none).
    let config = move |label: &str| {
        let mut c = config(label);
        if std::env::var("AUTHORITY_SIM_BEATS").as_deref() == Ok("thread")
            && c.holder_beats == sim::run::HolderBeats::Off
        {
            c.holder_beats = sim::run::HolderBeats::Thread;
        }
        // Plan 43 C7a: `AUTHORITY_SIM_RATES=1:-250000,2:250000` sets the
        // nodes' oscillator rates (ppm) of any configuration, and the
        // budget to the largest of them (`AUTHORITY_SIM_BUDGET=<ppm>`
        // overrides it): for tuning a rate configuration without
        // rebuilding.
        if let Ok(rates) = std::env::var("AUTHORITY_SIM_RATES") {
            c.node_rates = rates
                .split(',')
                .filter_map(|p| p.split_once(':'))
                .map(|(n, ppm)| (n.parse().expect("node"), ppm.parse().expect("ppm")))
                .collect();
            c.rate_budget_ppm = c
                .node_rates
                .iter()
                .map(|(_, p)| p.unsigned_abs() as u32)
                .max()
                .unwrap_or(0);
        }
        if let Some(b) = std::env::var("AUTHORITY_SIM_BUDGET")
            .ok()
            .and_then(|b| b.parse().ok())
        {
            c.rate_budget_ppm = b;
        }
        c
    };
    let streamed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let streamed_deleg = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let epoch_streamed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Fix "capture under an epoch hold": [remote rows enrolled, ship
    // plans seen deferring, transactions dropped for a departed member].
    // Chunk metered-own-rows: [replies excusing own rows, own chunks a
    // pass left held, own chunks uploaded past the hold].
    let chunks = std::sync::Arc::new([
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
    ]);
    // The lock configurations: [I/Os fenced, grants lost], and the seeds
    // with any (`SWEEP-FENCED`; not failures: a lease nobody can renew
    // fences its grants by design).
    let fenced = std::sync::Arc::new([
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
    ]);
    let fenced_seeds = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    // Plan 43 C7-F F-c (`SWEEP-PARKS`): [I/O steps parked, resumed, refused
    // after a park, lock requests parked, late renewals admitted, refused,
    // final mirrors begun, installed by a successor, mismatched, ms of the
    // longest park], and the seeds with a resume.
    let parks: std::sync::Arc<[std::sync::atomic::AtomicU64; 10]> =
        std::sync::Arc::new(Default::default());
    let park_seeds = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    // Plan 43 C7-F: [takeovers, takeovers by a seal, floors armed, floors
    // the cap shortened, takeovers whose cap had passed, ms saved by the
    // cap], over the run's nodes, and the length of every floor armed.
    let floors = std::sync::Arc::new([
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
        std::sync::atomic::AtomicU64::new(0),
    ]);
    let floor_ms = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let guard_rises = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let guard_base_rises = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    // The lock configurations, per seed `(seed, longest acquisition,
    // longest handoff gap, overtakes, asks, passed over, reserved)`: the
    // distribution the ask-again rule is measured by (`SWEEP-LOCKS`).
    let lock_seeds: std::sync::Arc<std::sync::Mutex<Vec<SeedLocks>>> = Default::default();
    // Open epochs replaced by their closed carrier's (`epochs::try_form`).
    let superseded = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let next = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(start));
    let failures = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    // Plan 43 C0: the commit-rule oracles' counters over the sweep
    // (`SWEEP-ORACLES` per seed, `SWEEP-ORACLE-TOTALS` at the end), and
    // `AUTHORITY_SIM_FINGERPRINT=1`'s `SWEEP-FP` line per seed.
    let oracle_totals: std::sync::Arc<std::sync::Mutex<OracleTotals>> = Default::default();
    let fingerprint_on = std::env::var("AUTHORITY_SIM_FINGERPRINT").as_deref() == Ok("1");
    let mut handles = Vec::new();
    for _ in 0..threads {
        let (next, failures, label) = (next.clone(), failures.clone(), label.clone());
        let (streamed, streamed_deleg) = (streamed.clone(), streamed_deleg.clone());
        let epoch_streamed = epoch_streamed.clone();
        let chunks = chunks.clone();
        let (fenced, fenced_seeds) = (fenced.clone(), fenced_seeds.clone());
        let (parks, park_seeds) = (parks.clone(), park_seeds.clone());
        let (floors, floor_ms) = (floors.clone(), floor_ms.clone());
        let guard_rises = guard_rises.clone();
        let guard_base_rises = guard_base_rises.clone();
        let superseded = superseded.clone();
        let lock_seeds = lock_seeds.clone();
        let oracle_totals = oracle_totals.clone();
        handles.push(std::thread::spawn(move || loop {
            let seed = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if seed >= start + seeds {
                break;
            }
            let result = run_seed(seed, config(&label));
            if fingerprint_on {
                eprintln!("SWEEP-FP {label} seed {seed}: {}", fingerprint(&result));
                // The runs whose strict durability the failure budget
                // waived (`Cluster::note_takedown`).
                if let Ok(r) = &result {
                    if !r.durability_budget_exceeded.is_empty() {
                        eprintln!("SWEEP-BUDGET {label} seed {seed}");
                    }
                }
            }
            match result {
                Ok(report) if core_stall_check(&label, &report).is_err() => {
                    let e = core_stall_check(&label, &report).unwrap_err();
                    eprintln!("SWEEP-FAIL {label} seed {seed}: {e}");
                    failures.lock().unwrap().push(seed);
                }
                Ok(report) => {
                    let ord = std::sync::atomic::Ordering::Relaxed;
                    oracle_totals.lock().unwrap().add(&label, seed, &report);
                    for s in report.stats.values() {
                        streamed.fetch_add(s.awaited_log_streamed, ord);
                        streamed_deleg.fetch_add(s.awaited_log_streamed_deleg, ord);
                        epoch_streamed.fetch_add(s.epoch_streamed_installed, ord);
                        chunks[3].fetch_add(s.own_rows_excused, ord);
                        floors[0].fetch_add(s.takeovers, ord);
                        floors[1].fetch_add(s.backup_takeovers, ord);
                        floors[2].fetch_add(s.ack_floor_waits, ord);
                        floors[3].fetch_add(s.ack_floor_capped, ord);
                        floors[4].fetch_add(s.ack_floor_capped_out, ord);
                        floors[5].fetch_add(s.ack_floor_cap_saved_ms, ord);
                    }
                    floor_ms.lock().unwrap().extend(report.floor_ms.iter().copied());
                    guard_rises.fetch_add(report.guard_rises, ord);
                    guard_base_rises.fetch_add(report.guard_base_rises, ord);
                    if report.guard_rises > 0 {
                        eprintln!(
                            "SWEEP-GUARD-RISE {label} seed {seed}: {} rises",
                            report.guard_rises
                        );
                    }
                    chunks[0].fetch_add(report.remote_enrolled, ord);
                    chunks[1].fetch_add(report.deferred_seen, ord);
                    chunks[2].fetch_add(report.remote_dropped, ord);
                    chunks[4].fetch_add(report.held_back, ord);
                    chunks[5].fetch_add(report.awaited_uploaded, ord);
                    let lost: u64 = report.stats.values().map(|s| s.lock_lost).sum();
                    // Seeds that exercise `Core::lock_install_returned`'s
                    // late-answer rule (unlinked-exclusion-7455).
                    let late: u64 = report
                        .stats
                        .values()
                        .map(|s| s.lock_returned_after_end)
                        .sum();
                    if late > 0 {
                        eprintln!(
                            "SWEEP-LATE-HANDBACK {label} seed {seed}: {late} grants of an ended generation dropped"
                        );
                    }
                    superseded.fetch_add(report.epochs_superseded as u64, ord);
                    if report.locks.steps > 0 {
                        let sum = |f: fn(&constellation_authority::Stats) -> u64| -> u64 {
                            report.stats.values().map(f).sum()
                        };
                        if report.locks.max_handoff_gap_ms > 3_000 || report.locks.overtaken > 0 {
                            eprintln!(
                                "SWEEP-SLOW {label} seed {seed}: longest acquisition {} ms, longest \
                                 handoff gap {} ms, {} overtakes; grants outwaited {}, short \
                                 authority waits {}, grants moved {} (carried {}), asks {}, passed over {}",
                                report.locks.max_wait_ms,
                                report.locks.max_handoff_gap_ms,
                                report.locks.overtaken,
                                sum(|s| s.lock_recalls_expired),
                                sum(|s| s.lock_short_authority_waits),
                                sum(|s| s.lock_moved),
                                sum(|s| s.lock_moved_carried),
                                sum(|s| s.lock_asked_again),
                                sum(|s| s.lock_ask_again_passed_over),
                            );
                        }
                        lock_seeds.lock().unwrap().push(SeedLocks {
                            seed,
                            max_wait_ms: report.locks.max_wait_ms,
                            max_wait_owner_ms: report.locks.max_wait_owner_ms,
                            max_wait_rest_ms: report.locks.max_wait_rest_ms,
                            max_gap_ms: report.locks.max_handoff_gap_ms,
                            overtaken: report.locks.overtaken,
                            asked_again: sum(|s| s.lock_asked_again),
                            passed_over: sum(|s| s.lock_ask_again_passed_over),
                            reserved_waits: sum(|s| s.lock_reserved_waits),
                            carried: sum(|s| s.lock_moved_carried),
                            acquired: report.locks.acquired,
                            ios: report.locks.ios,
                        });
                    }
                    {
                        let l = &report.locks;
                        let sum = |f: fn(&constellation_authority::Stats) -> u64| -> u64 {
                            report.stats.values().map(f).sum()
                        };
                        parks[0].fetch_add(l.parked_ios, ord);
                        parks[1].fetch_add(l.resumed_ios, ord);
                        parks[2].fetch_add(l.park_refused_ios, ord);
                        parks[3].fetch_add(l.parked_requests, ord);
                        parks[4].fetch_add(sum(|s| s.lock_late_admitted), ord);
                        parks[5].fetch_add(sum(|s| s.lock_late_refused), ord);
                        parks[6].fetch_add(sum(|s| s.lock_mirror_final_sent), ord);
                        parks[7].fetch_add(sum(|s| s.lock_mirror_final_installed), ord);
                        parks[8].fetch_add(sum(|s| s.lock_mirror_final_mismatch), ord);
                        parks[9].fetch_max(l.max_park_ms, ord);
                        if l.parked_ios + l.parked_requests > 0 {
                            park_seeds.lock().unwrap().push(seed);
                            eprintln!(
                                "SWEEP-PARK {label} seed {seed}: {} I/O steps parked, {} resumed, \
                                 {} refused; {} lock requests parked; longest park {} ms",
                                l.parked_ios,
                                l.resumed_ios,
                                l.park_refused_ios,
                                l.parked_requests,
                                l.max_park_ms
                            );
                        }
                    }
                    fenced[0].fetch_add(report.locks.fenced_ios, ord);
                    fenced[1].fetch_add(lost, ord);
                    if report.locks.fenced_ios > 0 || lost > 0 {
                        eprintln!(
                            "SWEEP-FENCED {label} seed {seed}: {} I/Os fenced, {lost} grants lost",
                            report.locks.fenced_ios
                        );
                        fenced_seeds.lock().unwrap().push(seed);
                    }
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
         dropped for a departed member: {}; replies excusing own rows: {}, own chunks held \
         back: {}, uploaded past the hold: {}; epochs replaced by their closed carrier's: {}; lock I/Os fenced: {}, grants lost: {}, in seeds {:?}",
        start + seeds,
        f.len(),
        streamed.load(std::sync::atomic::Ordering::Relaxed),
        streamed_deleg.load(std::sync::atomic::Ordering::Relaxed),
        epoch_streamed.load(std::sync::atomic::Ordering::Relaxed),
        chunks[0].load(std::sync::atomic::Ordering::Relaxed),
        chunks[1].load(std::sync::atomic::Ordering::Relaxed),
        chunks[2].load(std::sync::atomic::Ordering::Relaxed),
        chunks[3].load(std::sync::atomic::Ordering::Relaxed),
        chunks[4].load(std::sync::atomic::Ordering::Relaxed),
        chunks[5].load(std::sync::atomic::Ordering::Relaxed),
        superseded.load(std::sync::atomic::Ordering::Relaxed),
        fenced[0].load(std::sync::atomic::Ordering::Relaxed),
        fenced[1].load(std::sync::atomic::Ordering::Relaxed),
        {
            let mut s = fenced_seeds.lock().unwrap().clone();
            s.sort_unstable();
            s
        },
    );
    let ord = std::sync::atomic::Ordering::Relaxed;
    {
        let mut seeds = park_seeds.lock().unwrap().clone();
        seeds.sort_unstable();
        eprintln!(
            "SWEEP-PARKS {label}: I/O steps parked {}, resumed {}, refused after a park {}, lock \
             requests parked {}; late renewals admitted {}, refused {}; final mirrors begun {}, \
             installed {}, mismatched {}; longest park {} ms; {} seeds with a park",
            parks[0].load(ord),
            parks[1].load(ord),
            parks[2].load(ord),
            parks[3].load(ord),
            parks[4].load(ord),
            parks[5].load(ord),
            parks[6].load(ord),
            parks[7].load(ord),
            parks[8].load(ord),
            parks[9].load(ord),
            seeds.len(),
        );
    }
    let mut lengths = floor_ms.lock().unwrap().clone();
    lengths.sort_unstable();
    let pct = |p: usize| {
        lengths
            .get((lengths.len().saturating_sub(1)) * p / 100)
            .copied()
    };
    {
        let seeds = lock_seeds.lock().unwrap();
        if !seeds.is_empty() {
            // Percentiles over the seeds of each seed's maximum.
            let pct = |k: fn(&SeedLocks) -> u64| {
                let mut v: Vec<u64> = seeds.iter().map(k).collect();
                v.sort_unstable();
                let at = |p: usize| v[((v.len() - 1) * p) / 100];
                (at(50), at(90), at(99), v[v.len() - 1])
            };
            let (w50, w90, w99, wmax) = pct(|e| e.max_wait_ms);
            let (g50, g90, g99, gmax) = pct(|e| e.max_gap_ms);
            eprintln!(
                "SWEEP-LOCKS {label}: {} seeds; per-seed longest acquisition p50/p90/p99/max \
                 {w50}/{w90}/{w99}/{wmax} ms; longest handoff gap {g50}/{g90}/{g99}/{gmax} ms; \
                 overtakes {} (in {} seeds); asked again {}; passed over {} (in {} seeds); \
                 reserved waits {}; grants carried across a move {} (in {} seeds); locks acquired {}, \
                 I/Os {}; seeds with the owner node's \
                 longest wait over 45 s / 50 s: {} / {}, the other nodes': {} / {}",
                seeds.len(),
                seeds.iter().map(|e| e.overtaken).sum::<u64>(),
                seeds.iter().filter(|e| e.overtaken > 0).count(),
                seeds.iter().map(|e| e.asked_again).sum::<u64>(),
                seeds.iter().map(|e| e.passed_over).sum::<u64>(),
                seeds.iter().filter(|e| e.passed_over > 0).count(),
                seeds.iter().map(|e| e.reserved_waits).sum::<u64>(),
                seeds.iter().map(|e| e.carried).sum::<u64>(),
                seeds.iter().filter(|e| e.carried > 0).count(),
                seeds.iter().map(|e| e.acquired).sum::<u64>(),
                seeds.iter().map(|e| e.ios).sum::<u64>(),
                seeds
                    .iter()
                    .filter(|e| e.max_wait_owner_ms > 45_000)
                    .count(),
                seeds
                    .iter()
                    .filter(|e| e.max_wait_owner_ms > 50_000)
                    .count(),
                seeds.iter().filter(|e| e.max_wait_rest_ms > 45_000).count(),
                seeds.iter().filter(|e| e.max_wait_rest_ms > 50_000).count(),
            );
        }
    }
    oracle_totals.lock().unwrap().print(&label, seeds);
    eprintln!(
        "SWEEP-FLOORS {label}: takeovers {}, by a seal {}, floors armed {} (capped {}, saved {} ms \
         in all), cap already past {}; floor ms p50 {:?} p90 {:?} max {:?} over {} floors; \
         beat guards that rose after being published: {} (of them base: {})",
        floors[0].load(ord),
        floors[1].load(ord),
        floors[2].load(ord),
        floors[3].load(ord),
        floors[5].load(ord),
        floors[4].load(ord),
        pct(50),
        pct(90),
        lengths.last().copied(),
        lengths.len(),
        guard_rises.load(ord),
        guard_base_rises.load(ord),
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

/// The K5a fix round's minutes-long `fsync`: the root dies with live
/// delegates and no backup, so only a TTL takeover replaces it. A
/// delegate's own writes parked on renewals the dead root never answered
/// and never took the path that starts the takeover; now a grant left
/// unrenewed past the root's earliest reclaim lapses and its writes
/// take the ordinary route (forward, inbox, lease). The client deadline
/// (2 × TTL, which ended the parked wait in doubt) is raised far past
/// the TTL takeover, so a wait on a renewal shows as one.
fn delegated_root_gone_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::sim_core_config(id, inc);
            c.acquire_deadline_ms = 60_000;
            c
        }),
        cross_ratio: 0.0,
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            // Back long after the TTL takeover (so its own unshipped
            // writes, lost with it under `Local`, replay as they do when
            // a holder returns: forever gone, the session checker could
            // not tell them from a violation).
            kind: FaultKind::CrashHolder {
                restart_ms: Some(40_000),
                keep_journal: true,
            },
        }],
        ..delegated_config()
    }
}

/// The K5a fix round's post-handoff stall: the delegate of `d1`
/// restarts with its journal well inside its grant (a handoff, an
/// upgrade). It re-adopts the generation at start and renews it, so the
/// root never reclaims it.
fn delegated_delegate_restart_config() -> SimConfig {
    SimConfig {
        cross_ratio: 0.0,
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::CrashNode {
                node: 2,
                restart_ms: Some(200),
                keep_journal: true,
            },
        }],
        ..delegated_config()
    }
}

/// `git-under-flock-faults` fault #14: one delegate holds two
/// generations (all of `d1`, half of `d2`'s names: a node holds one
/// whole delegation, the harness's delegate held a whole directory and
/// a range of `/`) and its marker writer
/// alternates between them, so each generation's transactions depend
/// on the other's; the root dies once rows of both are in the log (the
/// delegate retired them) and a successor inherits the table.
fn delegated_two_gens_root_crash_config() -> SimConfig {
    SimConfig {
        nodes: 3,
        delegations: vec![("d1".into(), 2)],
        range_delegations: vec![("d2".into(), 2, (1, 0))],
        // `d2`'s halves have different owners: linearizable per range.
        check_range_bits: 1,
        marker_pairs: 10,
        cross_ratio: 0.0,
        faults: vec![ScheduledFault {
            at_ms: 2_000,
            kind: FaultKind::CrashHolder {
                restart_ms: Some(6_000),
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

/// delegate-stream-acks (`stress-ng-fs-nodes`): `long-delegated-backup`
/// with a root that answers every delegate batch and renewal 0.4–2.5 s
/// late — past the delegate's request timeout (400 ms here; the
/// production transport gave up after 2 s and 1 s), so most answers come
/// after the batch was given up on and re-sent, and in any order.
fn long_delegated_late_answers_config() -> SimConfig {
    SimConfig {
        deleg_answer_delay: (400, 2_500),
        ..long_delegated_backup_config()
    }
}

/// `delegate-store-stall` in the simulation: a delegate whose core is
/// stuck in a step (`StallStep`: a priority store write behind a slow
/// journal write) for longer than its renewal's slack, but below the hung
/// bound. Delegations: `d1` to node 2, `d2` to node 3; each node's one
/// client writes in its own directory, so nothing forwards to a delegate
/// or recalls its generation (the lane's integrity is the question, not
/// the recall's). Node 2's core is stuck 6.5 s, 9 s and 12.5 s (1.3, 1.8
/// and 2.5 × the 5 s ttl; the renewal is due at half of it, the grant is
/// honoured 4.5 s, the root reclaims 5.5 s after the last renewal), node 3's
/// 8 s. With the lane no grant is reclaimed, none lapses and none ends;
/// without it (`delegated-core-stall-unlaned`, the code before) the root
/// reclaims.
fn delegated_core_stall_config() -> SimConfig {
    let stall = |at_ms, node, for_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::StallStep { node, for_ms },
    };
    SimConfig {
        clients_per_node: 1,
        ops_per_client: 60,
        cross_ratio: 0.0,
        // A lease of 12 s, long against the 5 s grant: no cap by the root's
        // own lease (`delegated-core-stall-lease6` has it).
        core: std::sync::Arc::new(|node, incarnation| {
            let mut c = sim::run::sim_core_config(node, incarnation);
            c.ttl_ms = 12_000;
            c
        }),
        faults: vec![
            stall(1_500, 2, 6_500),
            stall(4_000, 3, 8_000),
            stall(11_000, 2, 9_000),
            stall(22_000, 2, 12_500),
        ],
        settle_ms: 40_000,
        ..delegated_config()
    }
}

/// [`delegated_core_stall_config`] on the sim's default 6 s root lease,
/// shorter than a grant plus its margin plus half a lease: the root caps a
/// grant by what is left of its own lease (`expires - margin`), to a second
/// or so when the lease's renewal comes late. Production has the same cap,
/// with the margins scaled up (a 60 s lease, a 20 s grant: a lease renewal
/// about 9 s late, a slow S3 lease PUT or a root core stuck behind the same
/// disk, or a grant inherited after a failover). Seeds 1447, 2079, 6242,
/// 6915, 7730, 8896, 9285, 12071, 14434, 16813, 18153, 18622, 19444 and 19884
/// lost a grant to the 12.5 s stall while the lane renewed at the period of
/// the last full grant (seed 2079: the renewal at 31.5 s was answered with a
/// ttl capped to 1.07 s, the next lane renewal came 2.4 s later, after the
/// root's reclaim at 33.08 s); the lane now follows the granted ttl
/// (`deleg_lane::with_granted_ttl`, the answers tapped off the bus), and the
/// sweep of this config is clean. `delegated-core-stall` runs a 12 s lease
/// (no cap) as the main configuration.
fn delegated_core_stall_lease6_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(sim::run::sim_core_config),
        ..delegated_core_stall_config()
    }
}

/// [`delegated_core_stall_config`] with every node's second client writing
/// in the other delegate's directory: the forwards to a stalled delegate
/// wait for it (or reach the root, which may recall the generation). The
/// lane must still lose no grant to the stall: none reclaimed, none
/// lapsed.
fn delegated_core_stall_forwarded_config() -> SimConfig {
    SimConfig {
        clients_per_node: 2,
        ops_per_client: 30,
        ..delegated_core_stall_config()
    }
}

/// [`delegated_core_stall_config`] without the lane: what stock did.
fn delegated_core_stall_unlaned_config() -> SimConfig {
    SimConfig {
        deleg_lane: false,
        ..delegated_core_stall_config()
    }
}

/// A delegate's whole process is frozen (`Pause`: stopped, as `SIGSTOP`, a
/// starved host or a saturated disk does) for 7, 8 and 9 s, beyond the 5 s
/// grant's reach: nothing in the process, the lane's thread included, can
/// renew, and the root reclaims. The delegate's core must say so when it
/// gives the grant up (`deleg_lapses_frozen`), so that no one has to guess a
/// freeze from a lateness record: each loss is counted as one in a freeze.
fn delegated_core_freeze_config() -> SimConfig {
    let pause = |at_ms, node, for_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::Pause { node, for_ms },
    };
    SimConfig {
        faults: vec![
            pause(2_500, 2, 7_000),
            pause(14_000, 3, 8_000),
            pause(26_000, 2, 9_000),
        ],
        settle_ms: 40_000,
        ..delegated_core_stall_config()
    }
}

/// One stall past the hung bound (`CONSTELLATION_HOLDER_STALL_MS`, 15 s):
/// the lane stops there, its last grant runs out, and the root reclaims
/// (a hung delegate is a dead one, as it always was).
fn delegated_core_hang_config() -> SimConfig {
    SimConfig {
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::StallStep {
                node: 2,
                for_ms: 26_000,
            },
        }],
        settle_ms: 60_000,
        ..delegated_core_stall_config()
    }
}

/// Plan 43 C0 (`AUTHORITY_SIM_FINGERPRINT=1`): a hash of what a run did, over
/// the fields that predate the commit-rule oracles and the run's text — two
/// trees whose runs are the same have the same fingerprint seed by seed (the
/// identity sweep: the oracles' plumbing must not change a schedule).
fn fingerprint(result: &Result<Report, String>) -> String {
    let text = match result {
        Ok(r) => format!(
            "ok {} {} {} {} {} {} {} {} {} {:?} {:?}",
            r.ops_invoked,
            r.ops_returned,
            r.tentative,
            r.segments,
            r.fenced,
            r.commits,
            r.simulated_ms,
            r.acked_rolled_back,
            r.in_doubt_answers,
            r.failover_ms,
            r.stats
        ),
        Err(e) => format!("err {e}"),
    };
    blake3::hash(text.as_bytes()).to_hex().to_string()[..16].to_string()
}

/// Plan 43 C0: the oracles' counters summed over a sweep.
#[derive(Debug, Default)]
struct OracleTotals {
    seeds: u64,
    acks_by_class: std::collections::BTreeMap<String, usize>,
    uncommitted: std::collections::BTreeMap<String, usize>,
    unjournaled: std::collections::BTreeMap<String, usize>,
    i2: usize,
    i2c: usize,
    i4: usize,
    i5: usize,
    seeds_with: [u64; 5],
    unmapped: usize,
    rebuilds: (u64, u64),
    rebuild_seeds: u64,
    max_backups: usize,
}

impl OracleTotals {
    fn add(&mut self, label: &str, seed: u64, r: &Report) {
        self.seeds += 1;
        let uncommitted: usize = r.uncommitted_acks.values().sum();
        let unjournaled: usize = r.unjournaled_refusals.values().sum();
        for (k, v) in &r.acks_by_class {
            *self.acks_by_class.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &r.uncommitted_acks {
            *self.uncommitted.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &r.unjournaled_refusals {
            *self.unjournaled.entry(k.clone()).or_default() += v;
        }
        self.i2 += r.identity_copies;
        self.i2c += r.non_verbatim;
        self.i4 += r.late_landings;
        self.i5 += r.non_lone_rollbacks;
        for (i, n) in [
            uncommitted,
            r.identity_copies,
            r.non_verbatim,
            r.late_landings,
            r.non_lone_rollbacks,
        ]
        .into_iter()
        .enumerate()
        {
            self.seeds_with[i] += u64::from(n > 0);
        }
        self.unmapped += r.acks_unmapped;
        self.rebuilds.0 += r.rebuilds.0;
        self.rebuilds.1 += r.rebuilds.1;
        self.max_backups = self.max_backups.max(r.max_backups);
        if r.rebuilds.0 + r.rebuilds.1 > 0 {
            self.rebuild_seeds += 1;
            eprintln!(
                "SWEEP-REBUILD {label} seed {seed}: rebuilds {}/{} (ok/failed)",
                r.rebuilds.0, r.rebuilds.1
            );
        }
        if uncommitted
            + r.identity_copies
            + r.non_verbatim
            + r.late_landings
            + r.non_lone_rollbacks
            + unjournaled
            > 0
        {
            eprintln!(
                "SWEEP-ORACLES {label} seed {seed}: i1={uncommitted} {:?} i2={} i2c={} i4={} i5={} \
                 unjournaled={unjournaled} {:?} rebuilds={}/{}",
                r.uncommitted_acks,
                r.identity_copies,
                r.non_verbatim,
                r.late_landings,
                r.non_lone_rollbacks,
                r.unjournaled_refusals,
                r.rebuilds.0,
                r.rebuilds.1
            );
        }
    }

    fn print(&self, label: &str, seeds: u64) {
        let i1: usize = self.uncommitted.values().sum();
        let unjournaled: usize = self.unjournaled.values().sum();
        eprintln!(
            "SWEEP-ORACLE-TOTALS {label} seeds {}/{seeds}: i1={i1} {:?} (in {} seeds) i2={} (in {}) \
             i2c={} (in {}) i4={} (in {}) i5={} (in {}) unjournaled={unjournaled} {:?} unmapped={} \
             rebuilds={}/{} (in {} seeds) max-backups={}; acks by class {:?}",
            self.seeds,
            self.uncommitted,
            self.seeds_with[0],
            self.i2,
            self.seeds_with[1],
            self.i2c,
            self.seeds_with[2],
            self.i4,
            self.seeds_with[3],
            self.i5,
            self.seeds_with[4],
            self.unjournaled,
            self.unmapped,
            self.rebuilds.0,
            self.rebuilds.1,
            self.rebuild_seeds,
            self.max_backups,
            self.acks_by_class
        );
    }
}

/// What the core-stall configurations demand of a seed (`label`; any other
/// configuration: nothing).
fn core_stall_check(label: &str, r: &Report) -> Result<(), String> {
    let sum =
        |f: fn(&constellation_authority::Stats) -> u64| -> u64 { r.stats.values().map(f).sum() };
    let (reclaimed, lapsed) = (sum(|s| s.deleg_reclaimed), sum(|s| s.deleg_lapsed));
    let (ended, off_core) = (sum(|s| s.deleg_ended), sum(|s| s.deleg_renewals_off_core));
    let lost = || {
        format!(
            "reclaimed {reclaimed}, lapsed {lapsed}, ended {ended}, lane renewals {off_core}, \
             installed {}, recalls {}: a delegate stalled below the hung bound lost its grant",
            sum(|s| s.deleg_installed),
            sum(|s| s.deleg_recalls_sent),
        )
    };
    match label {
        // (The unlaned variant is judged as the laned one: it fails.)
        "delegated-core-stall" | "delegated-core-stall-lease6" | "delegated-core-stall-unlaned"
            if reclaimed + lapsed + ended > 0 || off_core == 0 =>
        {
            Err(lost())
        }
        "delegated-core-stall-forwarded" if reclaimed + lapsed > 0 || off_core == 0 => Err(lost()),
        "delegated-core-freeze" => {
            let frozen = sum(|s| s.deleg_lapses_frozen);
            if reclaimed == 0 || frozen < reclaimed {
                Err(format!(
                    "reclaimed {reclaimed}, losses counted as frozen {frozen}, lapsed {lapsed}: \
                     a delegate whose process was frozen past its grant lost it, and each loss \
                     must be counted as one in a freeze"
                ))
            } else {
                Ok(())
            }
        }
        "delegated-core-hang" if reclaimed == 0 => Err(format!(
            "a delegate hung past the bound was never reclaimed (lapsed {lapsed}, ended {ended}, \
             lane renewals {off_core}, recalls {})",
            sum(|s| s.deleg_recalls_sent),
        )),
        _ => Ok(()),
    }
}

fn run_core_stall(label: &str, config: fn() -> SimConfig, seed: u64) -> Report {
    let r = run_seed(seed, config()).unwrap_or_else(|e| {
        panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
    });
    core_stall_check(label, &r).unwrap_or_else(|e| {
        panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
    });
    r
}

#[test]
fn a_delegate_stuck_in_a_step_keeps_its_grant() {
    let seeds = 24u64;
    let (mut lane, mut unlaned) = (0, (0, 0));
    for seed in 0..seeds {
        let r = run_core_stall("delegated-core-stall", delegated_core_stall_config, seed);
        lane += r
            .stats
            .values()
            .map(|s| s.deleg_renewals_off_core)
            .sum::<u64>();
        run_core_stall(
            "delegated-core-stall-forwarded",
            delegated_core_stall_forwarded_config,
            seed,
        );
        // The naive variant, the same seeds: the grant is lost.
        let r = run_seed(seed, delegated_core_stall_unlaned_config()).unwrap_or_else(|e| {
            panic!(
                "seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=delegated-core-stall-unlaned"
            )
        });
        assert_eq!(
            r.stats
                .values()
                .map(|s| s.deleg_renewals_off_core)
                .sum::<u64>(),
            0,
            "the lane sent with the lane off"
        );
        unlaned.0 += r.stats.values().map(|s| s.deleg_reclaimed).sum::<u64>();
        unlaned.1 += r.stats.values().map(|s| s.deleg_lapsed).sum::<u64>();
    }
    eprintln!("delegated-core-stall: lane renewals {lane}, unlaned reclaimed/lapsed {unlaned:?}");
    assert!(lane > 0, "the lane never sent");
    assert!(
        unlaned.0 >= seeds && unlaned.1 >= seeds,
        "without the lane the stalled delegates kept their grants: the configuration is \
         vacuous ({unlaned:?})"
    );
}

/// A grant the root capped to a second or so (its own lease was about to end)
/// before a stall: the lane learns the granted ttl from the answers that reach
/// the node's P2P service, off the core, and renews at a quarter of it, not at
/// the period of the last uncapped grant (`delegated_core_stall_lease6_config`:
/// seeds 1447 and 2079 lost their grant to the 12.5 s stall: the lane's next
/// renewal came 2.4 s after one answered with 1.07 s).
#[test]
fn a_capped_grant_before_a_stall_is_renewed_at_the_capped_period_on_a_short_lease() {
    for seed in [1447u64, 2079] {
        let r = run_seed(seed, delegated_core_stall_lease6_config()).unwrap_or_else(|e| {
            panic!(
                "seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=delegated-core-stall-lease6"
            )
        });
        core_stall_check("delegated-core-stall-lease6", &r).unwrap_or_else(|e| {
            panic!(
                "seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=delegated-core-stall-lease6"
            )
        });
    }
}

#[test]
fn a_delegate_frozen_past_its_grant_counts_its_loss_as_one_in_a_freeze() {
    let mut frozen_total = 0;
    for seed in 0..30u64 {
        let r = run_core_stall("delegated-core-freeze", delegated_core_freeze_config, seed);
        frozen_total += r.stats.values().map(|s| s.deleg_lapses_frozen).sum::<u64>();
    }
    assert!(frozen_total > 0);
    // The stalls that are no freeze (the core stuck, the process alive) are
    // never counted as one: the lane is to keep those grants.
    for seed in 0..10u64 {
        let r = run_core_stall("delegated-core-stall", delegated_core_stall_config, seed);
        assert_eq!(
            r.stats.values().map(|s| s.deleg_lapses_frozen).sum::<u64>(),
            0,
            "seed {seed}: a stall with the process alive was counted as a freeze"
        );
    }
}

#[test]
fn a_delegate_hung_past_the_bound_is_reclaimed() {
    for seed in 0..20u64 {
        run_core_stall("delegated-core-hang", delegated_core_hang_config, seed);
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
    lapsed: u64,
    dead_root_acquires: u64,
    in_doubt: usize,
    longest_submit_ms: u64,
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
            self.lapsed += s.deleg_lapsed;
            self.dead_root_acquires += s.dead_root_acquires;
        }
        self.marker_checks += r.marker_checks;
        self.in_doubt += r.in_doubt_answers;
        self.longest_submit_ms = self.longest_submit_ms.max(r.longest_submit_ms);
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

/// The root dies with live delegates and no backup (back only after
/// 40 s): the delegates' unrenewed grants lapse, the ops waiting for the
/// dead root's appends take the root over by TTL (the dead-root check),
/// every
/// acknowledged op survives and every op completes within the TTL
/// takeover's bound.
#[test]
fn delegated_root_loss_hands_the_writes_to_a_ttl_successor() {
    let t = run_m11(
        "delegated-root-gone",
        delegated_root_gone_config(),
        66_500..66_530,
    );
    assert!(t.lapsed > 0, "no delegate's grant lapsed: {t:?}");
    assert!(
        t.dead_root_acquires > 0,
        "no op waiting for the dead root's append took the root over: {t:?}"
    );
    assert!(
        t.inherited > 0,
        "no successor inherited a generation: {t:?}"
    );
    // The TTL takeover's bound: the lease's expiry (6 s at most after
    // the crash), the 3 s a non-backup waits past it, the takeover and
    // its gate; the grant's lapse (ttl + 2 × margin) falls inside it.
    assert!(
        t.longest_submit_ms <= 15_000,
        "a write waited {} ms for the dead root: {t:?}",
        t.longest_submit_ms
    );
    assert_eq!(t.in_doubt, 0, "writes ended in doubt: {t:?}");
}

/// A delegate restarts inside its grant: its new incarnation re-adopts
/// the generation at start (its install is counted; before, it
/// installed nothing until the log ended the generation), the root
/// never reclaims it unrenewed, and every check holds (requesters'
/// ops that reached the root while it was down still recall it).
#[test]
fn delegated_delegate_restart_readopts_its_generation() {
    let t = run_m11(
        "delegated-delegate-restart",
        delegated_delegate_restart_config(),
        66_700..66_730,
    );
    // One install per seed by `d2`'s delegate, one by `d1`'s restarted
    // delegate (the stats are its new incarnation's).
    assert!(
        t.installed >= t.seeds * 2,
        "the restarted delegate did not re-adopt: {t:?}"
    );
    assert_eq!(
        t.reclaimed, 0,
        "the root reclaimed a restarted delegate's generation unrenewed: {t:?}"
    );
}

/// `git-under-flock-faults` fault #14: a root failover under a delegate
/// of two generations whose rows depend on each other. The re-stream
/// restarted both from 0 while their early rows were already retired
/// (the log carried them): each generation's first unretired row waited
/// for the other's acknowledgement, no batch was ever sent, and every op
/// parked on the new root until its deadline (seeds 18, 85 and 92 never
/// quiesced). The re-stream now starts where the log is.
#[test]
fn delegated_two_generations_re_stream_after_a_root_failover() {
    let t = run_m11(
        "delegated-two-gens-root-crash",
        delegated_two_gens_root_crash_config(),
        0..100,
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
/// 30908 did (`find_late_adopted_hold_seeds`). 30299 still runs clean.
/// Since the review of chunk epoch-liveness-gap, a node paused for longer
/// than a formation attempt's interval is no member (it could not have
/// acked); 30908's member was paused longer, and seed 32708 adopts late.
#[test]
fn flex_crash_seed_30299_restarted_member_adopts_the_carried_hold() {
    run_seed(30299, flex_crash_config()).unwrap_or_else(|e| panic!("flex-crash seed 30299: {e}"));
    run_seed(30908, flex_crash_config()).unwrap_or_else(|e| panic!("flex-crash seed 30908: {e}"));
    let report = run_seed(32708, flex_crash_config())
        .unwrap_or_else(|e| panic!("flex-crash seed 32708: {e}"));
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

/// The flex-crash seeds of 1000..30 000 that failed on `de53bd1`
/// (sweep by the metered-own-rows review, then this fix's own). 16364:
/// an activation carrying the lease its holder had begun idle-releasing
/// (paused across the formation) — the hold it adopted was wiped by the
/// release's in-doubt give-up, nobody held the epoch and it never closed;
/// now no hold is adopted on a lease being (or already) released, and
/// its carrier owes the close and the re-claim once S3 is back. 4373: the sim's inbox PUT did
/// not upload the batch's manifest chunks first, as the daemon does, so
/// a sender that died for good left a batch naming a chunk only its disk
/// held. 19013, 21497, 28752: the stall sampler counted a paused hold
/// owner as sequencing (it ships nothing until it resumes).
#[test]
fn flex_crash_seeds_de53bd1() {
    for seed in [16364, 4373, 19013, 21497, 28752] {
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

/// delegate-stream-acks: `long-delegated-late-answers` passes, and is
/// not vacuous: batches were given up on and re-sent, answers to them
/// counted anyway, and every row reached the log (the run's own
/// quiescence and history checks).
#[test]
fn delegate_answers_later_than_the_request_timeout_still_count() {
    let (mut timeouts, mut unmatched, mut appended) = (0, 0, 0);
    for seed in 0..12 {
        let report = run_seed(seed, long_delegated_late_answers_config())
            .unwrap_or_else(|e| panic!("long-delegated-late-answers seed {seed}: {e}"));
        for s in report.stats.values() {
            timeouts += s.deleg_stream_timeouts;
            unmatched += s.deleg_stream_acks_unmatched;
            appended += s.deleg_appended_txs;
        }
    }
    eprintln!(
        "late answers: {timeouts} batches given up on, {unmatched} answers to them counted, \
         {appended} rows appended"
    );
    assert!(timeouts > 0 && unmatched > 0 && appended > 0);
}

/// `long-delegated-late-answers` seed 2945 (marker before its data): the
/// root, paused and then deposed, had appended `d1`'s stream rows 6–10
/// (the data among them) and executed a forwarded marker in `d2` after
/// them; neither shipped. The deposition queued the marker for replay by
/// rid with the deposed node's own session `deps`, which no longer named
/// `d1`'s rows, and the successor executed it before `d1`'s delegate,
/// restarted, re-streamed them. A stranded transaction's replay now
/// names the delegate rows its journal held ahead of it
/// (`StrandedOp::deps`).
#[test]
fn regression_long_delegated_late_answers_seed_2945() {
    run_seed(2_945, long_delegated_late_answers_config()).unwrap_or_else(|e| {
        panic!(
            "long-delegated-late-answers seed 2945: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=long-delegated-late-answers"
        )
    });
}

/// `long-delegated-backup` seed 3585 (a replay held forever): the paused
/// root ended two delegations itself after appending their rows and
/// executed a cross-subtree rename after them, then was deposed with the
/// `Recall`s unshipped; its successor ended the same generations lower.
/// The rename's replay (by rid) named the rows between the two cuts: the
/// deposed node's session did not call them lost (its own cut was above
/// them), the successor refused them as lost, `Held`, forever, and the
/// node never went quiet. A replay's recorded rows are kept only for the
/// generations the log's table still lists.
#[test]
fn regression_long_delegated_backup_seed_3585() {
    run_seed(3_585, long_delegated_backup_config()).unwrap_or_else(|e| {
        panic!(
            "long-delegated-backup seed 3585: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=long-delegated-backup"
        )
    });
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

/// Plan 41 L0: while locks are held and renewed, the holder loses S3
/// and is cut from its backup (P2P to the other nodes stays): the backup
/// seals and takes the lease over before it expires, and the old root,
/// still reached by the lock holders and unable to learn of it, must
/// answer no renewal (its own grants' at the tick, or its peers') past
/// what its successor's grace covers. No crash: the deposed root stays
/// up. (A partition from the backup alone is mostly resolved by the
/// holder removing the backup from the lease: no takeover.)
fn locks_failover_backup_renew_config() -> SimConfig {
    let cut = |at_ms| {
        [
            ScheduledFault {
                at_ms,
                kind: FaultKind::CutS3Holder { for_ms: 6_000 },
            },
            ScheduledFault {
                at_ms,
                kind: FaultKind::PartitionBackup { for_ms: 6_000 },
            },
        ]
    };
    SimConfig {
        core: std::sync::Arc::new(sim::run::backup_core_config),
        ops_per_client: 8,
        lock_ios: (4, 12),
        lock_io_ms: (20, 150),
        faults: cut(1_200).into_iter().chain(cut(9_000)).collect(),
        ..locks_config()
    }
}

/// Grants of 5 s (the sim's are 1.5 s) under the sim's 6 s lease, which
/// caps the root's grants at what is left of it; nine ops on one file,
/// half of them shared, with critical sections up to 3.6 s. A node often
/// has two ops parked on the file in two modes (two waiters, each
/// refreshed by its own re-sends, every 2.25 s), and the two are served in
/// one pass when the lock frees: the second re-affirms the first one's
/// grant in place, from its own older arrival, and must not end the
/// node's record sooner than the window the node honours under the first
/// (on `c9bee29`: 11 of seeds 0..1000 granted the lock over a holder
/// inside its section, with no fault).
fn locks_long_grants_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::sim_core_config(id, inc);
            c.lock_ttl_ms = 5_000;
            c
        }),
        clients_per_node: 3,
        ops_per_client: 8,
        lock_files: 1,
        lock_shared_ratio: 0.5,
        lock_ios: (4, 12),
        lock_io_ms: (50, 300),
        ..locks_config()
    }
}

/// `locks-failover-backup-renew` with 5 s grants, the inbox off and one
/// seal (at 1.2 s): the successor serves the rest of the run with grants
/// capped by its lease, as `locks-long-grants`, after a takeover.
fn locks_long_grants_seal_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c.lock_ttl_ms = 5_000;
            c
        }),
        faults: locks_failover_backup_renew_config().faults[..2].to_vec(),
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-H2: `locks-failover-backup-renew` with the clocks of nodes 2
/// and 3 ahead of node 1's by 4 s (the sim's lease is 6 s), so a backup
/// that seals a holder and takes its lease over before it expires reads
/// the lease's expiry (the holder's clock) as past, or too near, while the
/// lock holders still honour what the old root granted. The successor's
/// grace is its own bound from its own marker, however its clock compares
/// with the expiry: before, a takeover its clock read as expired set no
/// grace at all (`acquire_won` took it for a TTL takeover) and one it read
/// as about to expire cut the grace short (`min(expiry, bound)`); new
/// grants then landed inside an old grant's window (two holders of one
/// lock). Offsets of more than the whole lease also let a node whose
/// clock is ahead claim a live lease it reads as expired: plan 43's C7b,
/// not this chunk.
///
/// Plan 43 C7-F F-a: with the holder's off-core heartbeat on a thread of
/// its own (as the daemon has it), so the successor's floor is capped by
/// the beats it heard: the beats' arrival stamps are on the successor's
/// clock, 4 s ahead of the holder's, and a cap counted from their credit
/// (`fast-takeover-skewed-successor-credited`) would end the floor 3 s
/// early. `fast-takeover-skewed-successor-nobeats` is the configuration as
/// C7-H2 left it (no beats: the floor is the bound alone).
fn fast_takeover_skewed_successor_config() -> SimConfig {
    SimConfig {
        holder_beats: sim::run::HolderBeats::Thread,
        ..fast_takeover_skewed_successor_nobeats_config()
    }
}

fn fast_takeover_skewed_successor_nobeats_config() -> SimConfig {
    SimConfig {
        clock_offsets: vec![(2, 4_000), (3, 4_000)],
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-F F-a0: `locks-failover-backup-renew` twice in a row. The
/// holder is cut from S3 and from its backup and sealed by the backup; the
/// new holder, 1.5 s later and still inside the floor it inherited, is
/// cut in turn (it has granted nothing yet, and without strict mounts its
/// lease was not marked as granting) and sealed by its own backup. That
/// second successor must wait out the first holder's grants too, the ones
/// its backup's lock mirror never carried (the first holder's partition
/// from its backup began with its S3 cut). A lock grant lives 5 s here:
/// with the sim's 1.5 s the first holder's last unmirrored grant has
/// lapsed by the time the second seal can happen (a seal waits a silence
/// window after its cut). The inbox is off, as in `backup-strict-inbox-off`:
/// the takeover gate then does not wait the read quarantine out either.
fn double_seal_inside_the_grace_config() -> SimConfig {
    let cut = |at_ms| {
        [
            ScheduledFault {
                at_ms,
                kind: FaultKind::CutS3Holder { for_ms: 6_000 },
            },
            ScheduledFault {
                at_ms,
                kind: FaultKind::PartitionBackup { for_ms: 6_000 },
            },
        ]
    };
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c.lock_ttl_ms = 5_000;
            c
        }),
        faults: cut(1_200).into_iter().chain(cut(2_700)).collect(),
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-F F-a0 (b): `double-seal-inside-the-grace`'s two seals, the
/// second one past the first successor's floor, and nothing granted in
/// between. The first successor inherited its lease's mark from its
/// predecessor and served no lock request or strict read since (only node
/// 3 locks, once, before the first cut), so once its floor (the lock TTL
/// plus the takeover window and the margins, from its marker) has passed
/// and nothing is live (not even the record of node 3's cached grant) it
/// clears the mark; its own seal, at 12.5 s, then
/// arms no floor. Before the clear it armed a second one for a tenure that
/// had nothing to protect.
fn double_seal_after_the_grace_config() -> SimConfig {
    let cut = |at_ms| {
        [
            ScheduledFault {
                at_ms,
                kind: FaultKind::CutS3Holder { for_ms: 6_000 },
            },
            ScheduledFault {
                at_ms,
                kind: FaultKind::PartitionBackup { for_ms: 6_000 },
            },
        ]
    };
    SimConfig {
        // The cached grant is let go 0.3 s after its unlock (the sim's 15 s
        // would leave its record in the tables for the whole run, and a
        // record, live or not, keeps the mark).
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c.lock_ttl_ms = 5_000;
            c.lock_cache_idle_ms = 300;
            c
        }),
        clients_per_node: 1,
        ops_per_client: 4,
        lock_files: 1,
        lock_ratio: 1.0,
        lock_shared_ratio: 0.0,
        lock_ios: (2, 4),
        lock_io_ms: (20, 60),
        lock_nodes: vec![3],
        client_plan: vec![(3, 0, 1)],
        faults: cut(2_000).into_iter().chain(cut(12_500)).collect(),
        ..double_seal_inside_the_grace_config()
    }
}

/// Plan 43 C7-F F-a0 L3a/L3c: node 3 holds a lock under node 1's tenure
/// when node 1 shuts down gracefully (it releases its lease, marked: a
/// grant is live) and restarts. Nodes 2 and 3 cannot reach S3 meanwhile,
/// so node 1 re-claims its own released lease: no takeover, and the claim
/// CAS writes it unmarked. Node 3's renewal is a reclaim under the
/// quarantine (L3c), a record in node 1's table before its first backup is
/// listed, so the lock mirror, sent when the table changes, never carries
/// it to that backup. Node 1's clients take no locks (a lock request would
/// mark the lease: `ensure_granting_marked`), so the lease stays unmarked
/// until the reconfiguration that lists node 2, which must mark it. Node 1
/// is then cut from S3 and from node 2, which seals it; node 2's own late
/// request for the lock must be refused while node 3 still honours its
/// grant: the sealer arms a floor only for a marked lease.
fn locks_restart_reclaim_seal_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.inbox = false;
            c.lock_ttl_ms = 5_000;
            c
        }),
        clients_per_node: 1,
        ops_per_client: 2,
        lock_files: 1,
        lock_ratio: 1.0,
        lock_shared_ratio: 0.0,
        // Node 3's one critical section outlasts the whole sequence; node 1
        // (the restarted holder) sends its tenure no lock request; node 2's
        // late one is what the sealer must refuse.
        lock_ios: (14, 20),
        lock_io_ms: (250, 400),
        lock_nodes: vec![2, 3],
        client_plan: vec![(1, 2_700, 1), (2, 5_000, 1), (3, 0, 1)],
        faults: vec![
            // The others cannot claim the released lease meanwhile.
            ScheduledFault {
                at_ms: 1_100,
                kind: FaultKind::CutS3 {
                    node: 2,
                    for_ms: 1_900,
                },
            },
            ScheduledFault {
                at_ms: 1_100,
                kind: FaultKind::CutS3 {
                    node: 3,
                    for_ms: 1_900,
                },
            },
            ScheduledFault {
                at_ms: 1_200,
                kind: FaultKind::ShutdownHolder { restart_ms: 200 },
            },
            ScheduledFault {
                at_ms: 3_500,
                kind: FaultKind::CutS3Holder { for_ms: 6_000 },
            },
            ScheduledFault {
                at_ms: 3_500,
                kind: FaultKind::PartitionBackup { for_ms: 6_000 },
            },
        ],
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-F F-a's negative control: `fast-takeover-skewed-successor`
/// with every numbered beat's arrival stamp replaced by its credit
/// (`SimConfig::beats_credited`), the instant `sent + M` of the holder's
/// clock the seal watch counts. The cap of the successor's floor then
/// ends early by the 4 s the holder's clock is behind: this configuration
/// must FAIL (mutual exclusion), which `a_credited_arrival_cap_splits`
/// asserts; the real cap (the raw arrival) passes the one above.
fn fast_takeover_skewed_successor_credited_config() -> SimConfig {
    SimConfig {
        beats_credited: true,
        ..fast_takeover_skewed_successor_config()
    }
}

/// Plan 43 C7-F F-a: the other way round, the old holder's clock ahead of
/// its successors' by 4 s (the cap's lag is negative: the beats' arrival
/// is on the successors' earlier clocks). One failover only: with a second
/// cut at 9 s, node 1, ahead of whichever node holds, reads a live lease
/// as expired and claims it by TTL (plan 43's C7b, outside this chunk: 5
/// of 2 000 seeds violate mutual exclusion that way with the cap, 15
/// without it).
fn fast_takeover_skewed_holder_config() -> SimConfig {
    let first = locks_failover_backup_renew_config().faults;
    SimConfig {
        holder_beats: sim::run::HolderBeats::Thread,
        clock_offsets: vec![(1, 4_000)],
        faults: first.into_iter().take(2).collect(),
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-F F-a: the holder is cut from S3 only (no partition from its
/// backup): it keeps beating, its 6 s lease runs out, and its backup seals
/// at the expiry it reads. The successor's floor is capped by the beats
/// (about one margin past the expiry) instead of the whole bound.
fn locks_s3_cut_holder_alive_config() -> SimConfig {
    SimConfig {
        holder_beats: sim::run::HolderBeats::Thread,
        faults: vec![ScheduledFault {
            at_ms: 1_200,
            kind: FaultKind::CutS3Holder { for_ms: 9_000 },
        }],
        ..locks_failover_backup_renew_config()
    }
}

/// Plan 43 C7-F F-c: `locks-s3-cut-holder-alive` with long critical
/// sections (10 to 30 steps of 40 to 160 ms: seconds), so that the clients'
/// grants lapse inside them across the gap between the old root's last
/// renewal and its successor's existence. Before F-c the lock owner's I/O
/// was refused (`EIO`) from the lapse; now it waits (parks) and resumes when
/// the old root, or the successor from the old root's final mirror, admits a
/// late renewal of the same grant — nothing conflicting was granted since —
/// and only a grant that was granted over (or cannot be proved) is refused.
fn locks_s3_cut_holder_lapse_config() -> SimConfig {
    SimConfig {
        lock_ios: (10, 30),
        lock_io_ms: (40, 160),
        ..locks_s3_cut_holder_alive_config()
    }
}

/// Plan 43 C7-F F-c: the old root is cut from S3 for a while (alive, as
/// `locks-s3-cut-holder-lapse`), a locker is cut from everyone before that
/// (its grant lapses, the old root outwaits it and grants the lock to
/// another node), and every lock mirror after the first second is lost (the
/// final one's re-sends included): the backup seals with an older mirror
/// than the old root's last table, in which the outwaited grant is still
/// listed and the grant that replaced it is not. A
/// late renewal of it must be refused there (it is not the final mirror):
/// 0 violations. The same run with the successor trusting such a mirror
/// (`Config::lock_trust_nonfinal_mirror`) is the negative control
/// (`locks-late-stale-mirror-naive`: violations).
fn locks_late_stale_mirror_config() -> SimConfig {
    SimConfig {
        lock_mirror_drop: 1.0,
        lock_mirror_drop_from_ms: 1_000,
        faults: vec![
            ScheduledFault {
                at_ms: 300,
                kind: FaultKind::PartitionLocker { for_ms: 11_000 },
            },
            ScheduledFault {
                at_ms: 4_000,
                kind: FaultKind::CutS3Holder { for_ms: 9_000 },
            },
        ],
        lock_ios: (10, 30),
        lock_io_ms: (40, 160),
        ..locks_s3_cut_holder_alive_config()
    }
}

fn locks_late_stale_mirror_naive_config() -> SimConfig {
    SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::backup_core_config(id, inc);
            c.lock_trust_nonfinal_mirror = true;
            c
        }),
        ..locks_late_stale_mirror_config()
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

/// F1 (`cst-load-review` §2 F1, §5.2): the lease holder's FUSE fast path
/// writes a turn under its lock and passes the fencing-token check, then
/// waits for the store's writer lock behind a slow writer
/// (`WriterLockHold`). Its core waits behind the same writer, and, served
/// first after it (priority writes), outwaits the holder's own grant,
/// grants the lock to the next node and executes that node's turn. The
/// held write's own transaction comes last: it must be refused there
/// (`LockLapsed`). Landed — the code before the check at the write, which
/// `locks-writer-hold-unchecked` reproduces — it overwrote the next
/// holder's turn and was answered OK (`strict_lock_turns` fails the seed:
/// a later holder reads the older turn). Holds shorter than a grant
/// window must land.
fn locks_writer_hold_config() -> SimConfig {
    let hold = |at_ms, core_ms, after_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::WriterLockHold {
            node: 0,
            core_ms,
            after_ms,
        },
    };
    SimConfig {
        fast_path: sim::run::FastPath::Checked,
        strict_lock_turns: true,
        ops_per_client: 10,
        lock_ios: (2, 8),
        faults: vec![
            hold(400, 2_600, 1_200),
            hold(4_000, 3_400, 2_500),
            hold(9_000, 400, 200),
            hold(12_000, 2_200, 700),
            hold(16_000, 5_000, 1_000),
        ],
        ..with_lock_writes(locks_config(), "")
    }
}

/// F1 (review T1), the executor that did not mint the grant: the lock
/// files are the root's, the turns go to `d2`, which node 3 sequences as
/// its delegate. Node 3's own client writes its turn on the delegate's
/// fast path; the write passes its check at the write and stays in its
/// transaction, pinned, holding the writer lock (`DelegateWriteHold`),
/// while node 3 is cut off past its grant's window. The root outwaits the
/// grant and may grant on only after a cut with node 3's stream head as of
/// the outwait — which node 3 reports only after its pinned write
/// committed (`settle_tagged_writes`). The landing check
/// (`LockGhost::check_landings`) and the turn checks fail a write that
/// landed inside the next holder's critical section.
fn locks_writer_hold_delegated_config() -> SimConfig {
    let hold = |at_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::DelegateWriteHold {
            node: 3,
            partition_ms: 2_800,
            after_ms: 8_000,
        },
    };
    SimConfig {
        lock_files: 2,
        lock_ratio: 0.8,
        lock_dir: None,
        strict_lock_turns: true,
        ops_per_client: 10,
        lock_ios: (2, 8),
        faults: vec![hold(400), hold(8_000), hold(16_000)],
        ..with_lock_writes(delegated_config(), "d2")
    }
}

/// [`locks_writer_hold_delegated_config`] with the settle off: positions
/// are reported while a pinned write is still committing.
fn locks_writer_hold_delegated_unsettled_config() -> SimConfig {
    SimConfig {
        settle_tagged_writes: false,
        ..locks_writer_hold_delegated_config()
    }
}

/// F1 (review T2): as [`locks_writer_hold_delegated_config`], but nobody
/// is cut off and the client gives up on its held write after 700 ms — in
/// doubt — unlocks and is recalled: the grant is left to the root's
/// outwait (the doubt lapse) and the next holder is granted only after a
/// cut that settles the write. (In this simulation a pinned commit ends
/// at its node's next store write — a core step cannot block behind it —
/// so a write still committing when the release would go out is not
/// produced here; the release decision itself is pinned by
/// `core::tests::locks::a_release_waits_for_the_ops_tagged_with_its_grant`.)
fn locks_writer_hold_delegated_doubt_config() -> SimConfig {
    let hold = |at_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::DelegateWriteHold {
            node: 3,
            partition_ms: 0,
            after_ms: 4_000,
        },
    };
    SimConfig {
        lock_write_wait_ms: Some(700),
        faults: vec![hold(400), hold(6_000), hold(12_000), hold(18_000)],
        ..locks_writer_hold_delegated_config()
    }
}

/// F1's non-vacuity knob: [`locks_writer_hold_config`] with the token
/// check at the write off (the code before it).
fn locks_writer_hold_unchecked_config() -> SimConfig {
    SimConfig {
        tag_check_at_write: false,
        ..locks_writer_hold_config()
    }
}

/// F1, F8 (`cst-load-review` §5.2 `locks-stall-step`): steps that take
/// their `now`, then wait (in the store, or stopped) before deciding with
/// it, on the lease holder (every grant's owner, and the executor of the
/// turns) and on lock holders — around half a grant window, a window, and
/// past one. A step's stale `now` must never let a write past its grant's
/// end land (the check at the write reads the clock there), nor shorten a
/// window anyone honours.
fn locks_stall_step_config() -> SimConfig {
    let stall = |at_ms, node, for_ms| ScheduledFault {
        at_ms,
        kind: FaultKind::StallStep { node, for_ms },
    };
    SimConfig {
        strict_lock_turns: true,
        ops_per_client: 10,
        lock_ios: (4, 12),
        lock_io_ms: (20, 150),
        faults: vec![
            stall(500, 0, 500),
            stall(2_000, 2, 1_000),
            stall(4_000, 0, 2_100),
            stall(6_500, 3, 2_100),
            stall(9_000, 0, 1_100),
            stall(11_000, 2, 3_000),
        ],
        ..with_lock_writes(locks_config(), "")
    }
}

/// [`locks_stall_step_config`] with the lock files in `d1` (node 2's) and
/// the turns in `d2` (node 3's): the turns' executor never minted the
/// grants and judges their tokens by the window, on a clock its stalled
/// step read before it waited.
fn locks_stall_step_delegated_config() -> SimConfig {
    SimConfig {
        faults: locks_stall_step_config().faults,
        strict_lock_turns: true,
        lock_ios: (4, 12),
        lock_io_ms: (20, 150),
        ..with_lock_writes(locks_delegated_config(), "d2")
    }
}

/// `stress-ng-fs-faults` (bug B): short S3 outages of the whole cluster
/// (P2P stays up), each long enough for a continuation epoch carrying
/// the holder's lease, under the lock workload with long critical
/// sections. Every close used to drop the holder's lock grants with the
/// lease it re-claimed a moment later: every critical section across a
/// blip was fenced (`EIO`), and a lock asked for before the re-claim
/// was refused (`ENOLCK`). Some lease PUTs land but answer a timeout
/// (an S3 cut beginning while the re-claim's CAS is in flight): the
/// re-claim in doubt that landed is the same tenure (the review's
/// must-fix 1: it dropped the grants and released the lease).
fn locks_blips_config() -> SimConfig {
    let mut faults: Vec<ScheduledFault> = (0..4)
        .map(|i| ScheduledFault {
            at_ms: 700 + i * 2_600,
            kind: FaultKind::EpochOutage {
                members: 3,
                for_ms: 1_500,
            },
        })
        .collect();
    faults.push(ScheduledFault {
        at_ms: 0,
        kind: FaultKind::S3Rule(Rule::new(
            None,
            OpKind::Put,
            "leases/",
            When::Random(0.1),
            Fault::AppliedThenTimeout,
        )),
    });
    SimConfig {
        ops_per_client: 8,
        lock_ios: (6, 16),
        lock_io_ms: (40, 200),
        faults,
        ..locks_config()
    }
}

/// `locks-blips` with outages in back-to-back pairs: the gaps (300 to
/// 900 ms) put some second cuts between an epoch's close and the
/// lease's re-claim. The node claims the lease the close let go, and the
/// next epoch carries it (the review's should-fix 4: it formed an epoch
/// carrying no lease, which refused every write). S3 is back for 2.5 s
/// between pairs: a lease no re-claim reached for longer than its ttl
/// (every window too short) has expired, and anyone could take it, so
/// nothing carries it and its grants lapse — not this schedule's case.
/// Without `locks-blips`' in-doubt lease PUTs: under back-to-back cuts
/// they reach two holes that are not this one's (an acquisition CAS in
/// doubt that landed is re-adopted by no lock request until the lease
/// expires, and a lock waiter times out; a directory history out of
/// order — main fails so too, and most seeds on mutual exclusion first).
fn locks_blips_tight_config() -> SimConfig {
    let mut c = locks_blips_config();
    c.faults.clear();
    let mut at = 700;
    for gap in [300, 500, 700, 900] {
        for start in [at, at + 1_500 + gap] {
            c.faults.push(ScheduledFault {
                at_ms: start,
                kind: FaultKind::EpochOutage {
                    members: 3,
                    for_ms: 1_500,
                },
            });
        }
        at += 1_500 + gap + 1_500 + 2_500;
    }
    c
}

/// `locks-blips-tight` on one node (`stress-ng-fs-faults`' topology): its
/// every second cut that falls inside the re-claim window begins an epoch
/// carrying the lease its close let go. With three, the members of the
/// last epoch stay in it until the carried lease has moved, and the next
/// epoch begins only once the hold owner has closed and proposes it
/// (`Machine::persist_promise_proposed`).
fn locks_blips_tight_single_config() -> SimConfig {
    SimConfig {
        nodes: 1,
        ..locks_blips_tight_config()
    }
}

/// `locks-blips-tight` with a 10 s lease. Before chunk
/// epoch-liveness-gap, a pair of cuts whose gap no re-claim reached (the
/// hold owner closed in a short S3 window and was cut again) had no next
/// epoch: the other members were still in the last one, waiting for that
/// re-claim. The grants the close kept are renewed under the closed lease
/// up to its expiry, and with the sim's 6 s lease, last renewed up to half
/// a TTL before the pair, that expiry could fall inside the pair: the
/// grants lapsed. Now the hold owner's fresh epoch replaces the open one
/// (`Machine::persist_promise_proposed`), so the 6 s lease survives too;
/// this run keeps the lease out of the picture.
fn locks_blips_tight_long_lease_config() -> SimConfig {
    let c = locks_blips_tight_config();
    let core = c.core.clone();
    SimConfig {
        core: std::sync::Arc::new(move |id, incarnation| {
            let mut k = core(id, incarnation);
            k.ttl_ms = 10_000;
            k
        }),
        ..c
    }
}

/// Chunk epoch-liveness-gap (sweep only): `locks-blips-tight` with the
/// CI's random faults on top: a hold owner that crashes or pauses
/// between its close and its fresh epoch, members partitioned from it,
/// S3 errors on the re-claim.
fn locks_blips_tight_faults_config() -> SimConfig {
    SimConfig {
        random_faults: 3,
        join_fresh: false,
        ..locks_blips_tight_config()
    }
}

/// Review of chunk epoch-liveness-gap (sweep only): `locks-blips-tight`'s
/// back-to-back cuts over `locks-delegated` (the lock files in `d1`,
/// delegated to node 2, `d2` to node 3). Every epoch replaced by its
/// closed carrier's has delegates among its members, which stay stopped
/// through the transient promised state (`Core::epoch_replacing`).
fn locks_blips_tight_delegated_config() -> SimConfig {
    let c = locks_blips_tight_config();
    SimConfig {
        ops_per_client: c.ops_per_client,
        lock_ios: c.lock_ios,
        lock_io_ms: c.lock_io_ms,
        faults: c.faults,
        ..locks_delegated_config()
    }
}

/// Chunk delegate-fenced-io: `locks-blips-tight-delegated` fenced 482
/// lock I/Os in 0..4000 on main (`locks-blips-tight`: 0), every one a live
/// holder whose grant lapsed during an S3 cut while the root renewing it
/// was reachable over P2P:
///
/// - 1, 15, 36, 41, 67: a continuation epoch's recall-all took the
///   delegates' grants back to the root, which ended the generations in
///   its own journal; the `Recall` record reached the other tables only
///   after the cut, so the holders (the delegate itself among them) sent
///   their renewals to the recalled delegate, answered `NotOwner { 0 }`,
///   and the lease read that followed failed in the cut. The delegate now
///   names the root its recall went back to (`Core::deleg_handed_back_to`),
///   and the holder renews there (`LockState::deleg_moved`).
///
/// Fenced once that was fixed (main's code has each path; these seeds
/// reach it only with the redirect's timings):
/// - 293: the same, for a generation the delegate never installed (the
///   epoch was active when its table named it).
/// - 157: the holder's table no longer named a delegate and nothing named
///   the root; the failed lease read now falls back to the active
///   epoch's carrier (`Core::lock_owner_without_s3`).
/// - 3117: a recalled delegate's `NotHolder` reply made it the other
///   former delegate's cached lease holder, and the two sent each other
///   their renewals (`Core::on_mutate_reply` no longer caches a refusal's
///   sender).
/// - `locks-delegated-writes` 463, 510, 1203, and 1704, fenced on main
///   the same way: a pushed grant whose window counted from a request
///   sent 300–700 ms before arrived with 5–20 ms left under a delegate's
///   capped ttl; it is now asked for again.
#[test]
fn delegate_fenced_io_seeds_keep_their_grants() {
    let cases: [(&str, &[u64]); 2] = [
        (
            "locks-blips-tight-delegated",
            &[1, 15, 36, 41, 67, 157, 293, 3117],
        ),
        ("locks-delegated-writes", &[463, 510, 1203, 1704]),
    ];
    for (name, seeds) in cases {
        for &seed in seeds {
            let config = match name {
                "locks-blips-tight-delegated" => locks_blips_tight_delegated_config(),
                _ => with_lock_writes(locks_delegated_config(), "d2"),
            };
            let report = run_seed(seed, config).unwrap_or_else(|e| {
                panic!("{name} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={name}")
            });
            let mut t = M14Totals::default();
            t.add(&report);
            assert_eq!(t.lost, 0, "{name} seed {seed}: grants lost: {t:?}");
            assert_eq!(
                t.clients.fenced_ios, 0,
                "{name} seed {seed}: I/O fenced: {t:?}"
            );
        }
    }
}

/// `locks-delegated` (no faults) fenced a delegated lock holder's I/O in
/// seeds 1432 and 1528: the root's poll ran up to a quarter TTL after its
/// lease renewal fell due, the root capped the delegation's renewals by
/// that short remainder, and the delegate capped its lock renewals by the
/// delegation's (down to 501 ms against a 500 ms margin). The poll now
/// fires when the renewal is due (`Core::renew_due_in_ms`).
#[test]
fn lease_cap_fenced_seeds_keep_their_grants() {
    for seed in [1432, 1528] {
        let report = run_seed(seed, locks_delegated_config()).unwrap_or_else(|e| {
            panic!("locks-delegated seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-delegated")
        });
        let mut t = M14Totals::default();
        t.add(&report);
        assert_eq!(t.lost, 0, "locks-delegated seed {seed}: grants lost: {t:?}");
        assert_eq!(
            t.clients.fenced_ios, 0,
            "locks-delegated seed {seed}: I/O fenced: {t:?}"
        );
    }
}

/// The same class at the production margin (1 s) and lock TTL (20 s),
/// no fault: under the sim's 6 s lease every layer's margin adds up to
/// four (the lease's usable end, the delegation's window, the delegate's
/// grant cap, the holder's window), past the root's half-TTL renewal, so
/// a delegated holder's window closed before the lease was renewed while
/// the delegate's renewals came back no longer (seed 10 of `-heavy`: 1738
/// … 1003 ms, fenced at t=2311). The root's own grants end two margins
/// before the lease (`locks-production-ttl-heavy`). The root now renews
/// its lease at the next round when it caps a delegation below a lock
/// grant's need, or a lock grant of its own below `2 × margin`
/// (`Core::lease_renew_for_grants`).
#[test]
fn production_margin_lease_cap_seeds_keep_their_grants() {
    let cases: [(&str, bool, bool, &[u64]); 3] = [
        (
            "locks-delegated-production-ttl",
            true,
            false,
            &[2, 424, 791],
        ),
        (
            "locks-delegated-production-ttl-heavy",
            true,
            true,
            &[10, 72, 337, 1881],
        ),
        ("locks-production-ttl-heavy", false, true, &[113, 1010]),
    ];
    for (name, deleg, heavy, seeds) in cases {
        for &seed in seeds {
            let report = run_seed(seed, production_ttl(deleg, heavy)).unwrap_or_else(|e| {
                panic!("{name} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={name}")
            });
            let mut t = M14Totals::default();
            t.add(&report);
            assert_eq!(t.lost, 0, "{name} seed {seed}: grants lost: {t:?}");
            assert_eq!(
                t.clients.fenced_ios, 0,
                "{name} seed {seed}: I/O fenced: {t:?}"
            );
        }
    }
}

/// `locks-delegated-production-ttl-heavy`, no fault: a cross-subtree
/// rename recalls a delegate's generation and the root re-delegates the
/// subtree to the same node (seed 1434: `d1`, gen 3 then gen 5, node 2).
/// The delegate's lock grant under it still renews with the root, whose
/// table names the requester as the subtree's owner and so answers
/// `NotOwner { 0 }`; the delegate read that as a deposed root, forgot its
/// cached root, started a new stream round and dropped the answer to its
/// new generation's first renewal as stale, while the renewal in flight
/// kept any other from going out until the forward timeout: the
/// generation was never renewed for 400 ms, the node answered the whole
/// subtree `NotOwner { 0 }` (or "no reachable owner", seed 1155, which
/// lost the renewal the same way) and the holders' windows lapsed: I/O
/// fenced with no fault. A subtree delegated here keeps the cached root
/// (`Core::lock_delegated_here`), and a root change re-sends the renewal
/// in flight at once (`delegation_sync`). Seed 3850, once those were
/// fixed: a grant capped at 2.1 s whose renewal was in flight (the tick
/// armed at the 5 s cadence) was extended by a late reply
/// (`Core::lock_late_reply`) to a renewal point no tick was armed for,
/// and lapsed under the application's I/O; the late reply re-arms it.
#[test]
fn a_redelegation_to_the_same_node_keeps_its_first_renewal() {
    for seed in [1434, 691, 1683, 604, 1155, 3850] {
        let report = run_seed(seed, production_ttl(true, true)).unwrap_or_else(|e| {
            panic!(
                "locks-delegated-production-ttl-heavy seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-delegated-production-ttl-heavy"
            )
        });
        let mut t = M14Totals::default();
        t.add(&report);
        assert_eq!(
            t.lost, 0,
            "locks-delegated-production-ttl-heavy seed {seed}: grants lost: {t:?}"
        );
        assert_eq!(
            t.clients.fenced_ios, 0,
            "locks-delegated-production-ttl-heavy seed {seed}: I/O fenced: {t:?}"
        );
    }
}

/// `locks-blips-tight` with `locks-blips`' in-doubt lease PUTs back:
/// back-to-back cuts with acquisition and release CASes that land but
/// answer a timeout. On main before chunk lock-release-drop it failed
/// seeds 403486, 405329, 412350, 413006 and 422266 of 403000..423000 (an
/// acquisition in doubt that landed, adopted by nobody until it expired)
/// and then 400925 (two non-holders naming each other the lock owner).
fn locks_blips_tight_in_doubt_config() -> SimConfig {
    let mut c = locks_blips_tight_config();
    c.faults.extend(
        locks_blips_config()
            .faults
            .into_iter()
            .filter(|f| matches!(f.kind, FaultKind::S3Rule(_))),
    );
    c
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

/// `locks-delegated` at the production lock TTL (20 s, margin 1 s; the
/// delegation's default 5 s): a delegate's grants are capped at about 2–3 s
/// while waiters re-send every 9.5 s (the ask-again rule).
fn locks_delegated_production_ttl_config() -> SimConfig {
    production_ttl(true, false)
}

/// ... and with nine ops on one file, as `locks-long-grants` has.
fn locks_delegated_production_ttl_heavy_config() -> SimConfig {
    production_ttl(true, true)
}

/// The same at the root (the sim's 6 s lease caps the root's grants too).
fn locks_production_ttl_config() -> SimConfig {
    production_ttl(false, false)
}

fn locks_production_ttl_heavy_config() -> SimConfig {
    production_ttl(false, true)
}

/// A variant of a configuration, named after a `+` in its label
/// (`locks-delegated-production-ttl+nomove+solo`):
/// - `nomove`: no rename crossing subtrees, so a delegation is not recalled
///   for one and its lock table does not move (a move restamps the grants
///   it carries to a full lock ttl and margin: a waiter then waits that
///   long for a grant that lapsed at its holder meanwhile);
/// - `solo`: one lock client per node (and three times the ops), so a node
///   never has two ops waiting for one file, whose first grant covers the
///   second (an op the node's push does not wake: it waits for its own
///   re-send, 9.5 s at the production lock ttl).
fn config_variant(cfg: SimConfig, variant: &str) -> SimConfig {
    match variant {
        "nomove" => SimConfig {
            cross_ratio: 0.0,
            ..cfg
        },
        "solo" => SimConfig {
            clients_per_node: 1,
            ops_per_client: cfg.ops_per_client * 3,
            ..cfg
        },
        // Plan 43 C0: the commit-rule oracles' strict flags (they always
        // count; these make a count fail the run).
        "commit" => SimConfig {
            strict_commit: true,
            ..cfg
        },
        "identity" => SimConfig {
            strict_identity: true,
            ..cfg
        },
        "settle" => SimConfig {
            strict_settle: true,
            ..cfg
        },
        "lone" => SimConfig {
            strict_lone: true,
            ..cfg
        },
        "oracles" => SimConfig {
            strict_commit: true,
            strict_identity: true,
            strict_settle: true,
            strict_lone: true,
            ..cfg
        },
        // The negative control of the sim's rebuild (plan 43 C0): the
        // stub that fails every deposition rebuild.
        "rebuild-stub" => SimConfig {
            rebuild_stub: true,
            ..cfg
        },
        // `pause<node>@<at_ms>:<for_ms>`: one more scheduled `Pause`.
        other if other.starts_with("pause") => {
            let parse = || -> Option<ScheduledFault> {
                let (node, rest) = other["pause".len()..].split_once('@')?;
                let (at, for_ms) = rest.split_once(':')?;
                Some(ScheduledFault {
                    at_ms: at.parse().ok()?,
                    kind: FaultKind::Pause {
                        node: node.parse().ok()?,
                        for_ms: for_ms.parse().ok()?,
                    },
                })
            };
            let fault = parse().unwrap_or_else(|| panic!("bad variant +{other}"));
            let mut faults = cfg.faults.clone();
            faults.push(fault);
            SimConfig { faults, ..cfg }
        }
        other => panic!("unknown configuration variant +{other}"),
    }
}

fn production_ttl(deleg: bool, heavy: bool) -> SimConfig {
    let base = SimConfig {
        core: std::sync::Arc::new(|id, inc| {
            let mut c = sim::run::sim_core_config(id, inc);
            c.lock_ttl_ms = 20_000;
            c.expiry_margin_ms = 1_000;
            c
        }),
        lock_dir: if deleg { Some("d1".into()) } else { None },
        ..locks_delegated_config()
    };
    if !heavy {
        return base;
    }
    SimConfig {
        clients_per_node: 3,
        ops_per_client: 8,
        lock_files: 1,
        lock_shared_ratio: 0.5,
        lock_ios: (4, 12),
        lock_io_ms: (50, 300),
        ..base
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
    moved_carried: u64,
    leaving_installed: u64,
    reinstated: u64,
    graces_inherited: u64,
    lost: u64,
    released: u64,
    granted_recalled: u64,
    grace_refusals: u64,
    /// Plan 43 F-b: non-blocking requests told to wait, for a grace and
    /// for a floor, a barrier or a delegate's authority; grace ends that
    /// served parked waiters.
    grace_waits: u64,
    not_yet_waits: u64,
    stale_owner_waits: u64,
    grace_end_serves: u64,
    /// The ask-again rule: waiters asked to send their request again,
    /// those passed over for not doing so, requests refused for a turn
    /// held for an asked waiter, asks answered by the nodes.
    asked_again: u64,
    ask_again_passed_over: u64,
    reserved_waits: u64,
    asks_answered: u64,
    grace_periods: u64,
    idle_released: u64,
    backup_takeovers: u64,
    /// Plan 43 C7-F F-a0 (b): floors armed, inherited marks cleared.
    ack_floor_waits: u64,
    mark_clears: u64,
    unavailable: u64,
    wait_ms_total: u64,
    max_simulated_ms: u64,
    /// Plan 41 L0: renewal answers held for a fast tenure's liveness,
    /// the lease reads they waited for, the ones refused.
    renew_held: u64,
    renew_probes: u64,
    renew_refused: u64,
    /// F1: see `Report::held_writes`; recalled grants left to the
    /// outwait (an op tagged with them in doubt).
    held_writes: u64,
    held_refused: u64,
    held_accepted: u64,
    doubt_lapses: u64,
    token_rejections_at_write: u64,
    settle_waits: u64,
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
        c.owner_fenced_ios += l.owner_fenced_ios;
        c.local_conflicts += l.local_conflicts;
        c.granted += l.granted;
        c.would_block += l.would_block;
        c.grace_waits += l.grace_waits;
        c.unavailable += l.unavailable;
        c.regrants += l.regrants;
        c.idle_sent += l.idle_sent;
        c.position_timeouts += l.position_timeouts;
        c.abandoned += l.abandoned;
        c.max_wait_ms = c.max_wait_ms.max(l.max_wait_ms);
        c.max_wait_owner_ms = c.max_wait_owner_ms.max(l.max_wait_owner_ms);
        c.max_wait_rest_ms = c.max_wait_rest_ms.max(l.max_wait_rest_ms);
        c.max_handoff_gap_ms = c.max_handoff_gap_ms.max(l.max_handoff_gap_ms);
        c.visibility_checks += l.visibility_checks;
        c.visibility_gaps += l.visibility_gaps;
        c.turns_written += l.turns_written;
        c.turn_reads += l.turn_reads;
        c.stale_turn_reads += l.stale_turn_reads;
        c.late_unacked_turns += l.late_unacked_turns;
        c.token_refused += l.token_refused;
        c.overtaken += l.overtaken;
        c.parked_ios += l.parked_ios;
        c.resumed_ios += l.resumed_ios;
        c.park_refused_ios += l.park_refused_ios;
        c.parked_requests += l.parked_requests;
        c.max_park_ms = c.max_park_ms.max(l.max_park_ms);
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
            self.moved_carried += s.lock_moved_carried;
            self.leaving_installed += s.lock_leaving_installed;
            self.reinstated += s.lock_reinstated;
            self.graces_inherited += s.lock_graces_inherited;
            self.lost += s.lock_lost;
            self.released += s.lock_released;
            self.granted_recalled += s.lock_granted_recalled;
            self.grace_refusals += s.lock_grace_refusals;
            self.grace_waits += s.lock_grace_waits;
            self.not_yet_waits += s.lock_not_yet_waits;
            self.stale_owner_waits += s.lock_stale_owner_waits;
            self.grace_end_serves += s.lock_grace_end_serves;
            self.asked_again += s.lock_asked_again;
            self.ask_again_passed_over += s.lock_ask_again_passed_over;
            self.reserved_waits += s.lock_reserved_waits;
            self.asks_answered += s.lock_asks_answered;
            self.grace_periods += s.lock_grace_periods;
            self.idle_released += s.lock_idle_released;
            self.backup_takeovers += s.backup_takeovers;
            self.ack_floor_waits += s.ack_floor_waits;
            self.mark_clears += s.mark_clears;
            self.unavailable += s.lock_unavailable;
            self.wait_ms_total += s.lock_wait_ms_total;
            self.renew_held += s.renew_held;
            self.renew_probes += s.renew_probes;
            self.renew_refused += s.renew_refused;
            self.doubt_lapses += s.lock_doubt_lapses;
        }
        self.held_writes += r.held_writes;
        self.held_refused += r.held_refused;
        self.held_accepted += r.held_accepted;
        self.token_rejections_at_write += r.token_rejections_at_write;
        self.settle_waits += r.settle_waits;
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
    // Fairness (EC2 campaign 8): without faults nobody is granted while
    // a client of another node has waited a whole grant window longer.
    assert_eq!(t.clients.overtaken, 0, "a waiter was overtaken: {t:?}");
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

/// Plan 30 §M14, the owner fence (the git-under-flock follow-up): every
/// exclusive holder writes its turn into a data file — another file the
/// lock guards, as git's objects and refs are — while holders are cut
/// off or paused past their grant. The lapsed owner is fenced on that
/// file too (the lock file's own fence covers only the lock file), so no
/// later holder ever reads a turn older than one written under the lock
/// before it.
#[test]
fn locks_lapsed_owner_is_fenced_on_other_files() {
    for (label, cfg, seeds) in [
        (
            "locks-partition-writes",
            with_lock_writes(locks_partition_config(), ""),
            98_000..98_040,
        ),
        (
            "locks-pause-writes",
            with_lock_writes(locks_pause_config(), ""),
            98_100..98_140,
        ),
    ] {
        let t = run_m14(label, cfg, seeds);
        assert!(t.clients.fenced_ios > 0, "{label}: nothing fenced: {t:?}");
        assert!(t.clients.turns_written > 100, "{label}: {t:?}");
        assert_eq!(t.clients.stale_turn_reads, 0, "{label}: {t:?}");
        // Plan 30 §M14 phase 2: a turn written under a grant that lapsed
        // before it landed is refused where it lands (the fencing token),
        // never applied over a later holder's turn.
        assert_eq!(t.clients.late_unacked_turns, 0, "{label}: {t:?}");
    }
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

/// Plan 41 L0: a root its backup sealed while lock holders still reach
/// it renews nothing past its successor's grace (the ghost's mutual
/// exclusion), and the backup does take over.
#[test]
fn locks_survive_a_backup_seal_under_renewals() {
    let t = run_m14(
        "locks-failover-backup-renew",
        locks_failover_backup_renew_config(),
        94_500..94_560,
    );
    assert!(t.grants > 300, "{t:?}");
    assert!(
        t.backup_takeovers > 10,
        "the backup rarely took over: {t:?}"
    );
    assert!(t.renew_held > 0, "no renewal waited for liveness: {t:?}");
}

/// One seed's lock measures (`SWEEP-LOCKS`,
/// `capped_grants_hand_a_lock_over_within_a_round_trip`).
struct SeedLocks {
    seed: u64,
    max_wait_ms: u64,
    /// The longest wait of a client of node 1 (the owner) and of one of the
    /// other nodes.
    max_wait_owner_ms: u64,
    max_wait_rest_ms: u64,
    max_gap_ms: u64,
    overtaken: u64,
    asked_again: u64,
    passed_over: u64,
    reserved_waits: u64,
    /// Moved grants that carried what was left of their record.
    carried: u64,
    /// Locks acquired and I/Os done in the critical sections (fenced ones
    /// apart).
    acquired: u64,
    ios: u64,
}

/// `seeds` of `cfg`, the lock measures of each (a seed that fails, a client
/// that does not get its lock in 60 s of simulated time included, panics),
/// and the sums over the nodes of the counters `f` picks.
fn run_lock_seeds(
    label: &str,
    cfg: SimConfig,
    seeds: std::ops::Range<u64>,
    sums: &[fn(&constellation_authority::Stats) -> u64],
) -> (Vec<SeedLocks>, Vec<u64>) {
    let mut out = Vec::new();
    let mut totals = vec![0; sums.len()];
    for seed in seeds {
        let report = run_seed(seed, cfg.clone()).unwrap_or_else(|e| {
            panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
        });
        for (t, f) in totals.iter_mut().zip(sums) {
            *t += report.stats.values().map(f).sum::<u64>();
        }
        out.push(SeedLocks {
            seed,
            max_wait_ms: report.locks.max_wait_ms,
            max_wait_owner_ms: report.locks.max_wait_owner_ms,
            max_wait_rest_ms: report.locks.max_wait_rest_ms,
            max_gap_ms: report.locks.max_handoff_gap_ms,
            overtaken: report.locks.overtaken,
            asked_again: 0,
            passed_over: 0,
            reserved_waits: 0,
            carried: report.stats.values().map(|s| s.lock_moved_carried).sum(),
            acquired: report.locks.acquired,
            ios: report.locks.ios,
        });
    }
    let q = |k: fn(&SeedLocks) -> u64, p: usize| {
        let mut v: Vec<u64> = out.iter().map(k).collect();
        v.sort_unstable();
        v[(v.len() - 1) * p / 100]
    };
    eprintln!(
        "{label}: {} seeds; per-seed longest acquisition p50/p90/max {}/{}/{} ms; longest handoff \
         gap {}/{}/{} ms; overtakes {}; stats {totals:?}",
        out.len(),
        q(|s| s.max_wait_ms, 50),
        q(|s| s.max_wait_ms, 90),
        q(|s| s.max_wait_ms, 100),
        q(|s| s.max_gap_ms, 50),
        q(|s| s.max_gap_ms, 90),
        q(|s| s.max_gap_ms, 100),
        out.iter().map(|s| s.overtaken).sum::<u64>(),
    );
    (out, totals)
}

/// The ask-again rule (cluster-locks.md): at the production lock TTL (20 s,
/// waiters re-sending every 9.5 s) and a short delegation, a delegate's
/// grants are capped by its delegation (about 2–3 s), so a grant pushed to a
/// waiter whose request is seconds old reached its holder lapsed. Waits ran
/// to a re-send interval and a restamped outwait (21 s); 122 of 2000 seeds of
/// the delegated config never got a lock in 60 s (seed 15 the first), the
/// sim's lease-capped root had waits of 9.8 s in a tenth of its seeds, and
/// the owner node's own clients lost the local lottery for a minute
/// (`locks-long-grants`). No fault: a handoff to a stale waiter now costs a
/// round trip, the clients of a node keep their place in the queue whatever
/// round they lose (`waited_ms`), and a grant answers every op of its node
/// that it covers.
///
/// The delegated configuration has no rename crossing subtrees (`+nomove`: a
/// delegation's move restamps its grants to a full lock ttl, 21 s for a
/// grant that lapsed at its holder meanwhile: a follow-up in PROGRESS, not
/// the rule's). Its remaining gaps, up to 5 s, are a grant its own node, or
/// a cancelled non-blocking wait, left unused: outwaited (`ttl + margin`, 3
/// to 4.5 s at its 2 to 3 s authority) and the barrier that follows. The
/// nine-op configurations are one client per node (`+solo`: sections add up
/// there). Before, on the delegated one the longest wait per seed was 20 s
/// at the median and 60 s at most, the gap 11 and 30 s, and 201 handoffs
/// were overtaken; on the root 9.8 s at the 90th percentile.
#[test]
fn capped_grants_hand_a_lock_over_within_a_round_trip() {
    use constellation_authority::Stats;
    let sums: [fn(&Stats) -> u64; 5] = [
        |s| s.lock_asked_again,
        |s| s.lock_ask_again_passed_over,
        |s| s.lock_grants,
        |s| s.lock_recalls_sent,
        |s| s.lock_asks_answered,
    ];
    // (label, config, seeds, heavy: nine ops on a file, asked: an ask is
    // expected in these seeds)
    for (label, cfg, seeds, heavy, asked) in [
        (
            "locks-delegated-production-ttl+nomove",
            config_variant(locks_delegated_production_ttl_config(), "nomove"),
            0..40,
            false,
            true,
        ),
        (
            "locks-production-ttl",
            locks_production_ttl_config(),
            0..60,
            false,
            false,
        ),
        (
            "locks-production-ttl-heavy+solo",
            config_variant(locks_production_ttl_heavy_config(), "solo"),
            0..40,
            true,
            true,
        ),
        (
            "locks-delegated-production-ttl-heavy+solo+nomove",
            config_variant(
                config_variant(locks_delegated_production_ttl_heavy_config(), "solo"),
                "nomove",
            ),
            0..40,
            true,
            true,
        ),
    ] {
        let (per_seed, totals) = run_lock_seeds(label, cfg, seeds, &sums);
        for s in &per_seed {
            assert!(
                s.max_gap_ms <= 6_000,
                "{label} seed {}: a handoff idled {} ms (replay with AUTHORITY_SIM_CONFIG={label})",
                s.seed,
                s.max_gap_ms
            );
            if !heavy {
                // Nobody waits a re-send interval for a free lock, and no
                // one is overtaken. (With nine ops on a file, sections add
                // up, and an in-place re-affirmation of a node's shared
                // grant may pass a parked exclusive waiter: reported.)
                assert!(
                    s.max_wait_ms < 9_500 && s.overtaken == 0,
                    "{label} seed {}: waited {} ms, {} overtakes",
                    s.seed,
                    s.max_wait_ms,
                    s.overtaken
                );
            }
        }
        let [asked_again, passed_over, grants, recalls, answered] = totals[..] else {
            unreachable!()
        };
        if asked {
            assert!(asked_again > 0, "{label}: no waiter was ever asked");
            assert!(answered > 0, "{label}: no ask was ever answered");
        }
        assert_eq!(passed_over, 0, "{label}: an ask went unanswered");
        assert!(grants > 200 && recalls > 50, "{label}: little contention");
    }
    // The delegate with its renames crossing subtrees and two clients per
    // node: nobody waits for ever (122 of 2000 seeds before, seed 15 the
    // first).
    run_lock_seeds(
        "locks-delegated-production-ttl",
        locks_delegated_production_ttl_config(),
        0..60,
        &[],
    );
    // The owner node's own clients take a node's grant in one pass and race
    // for it; the ones that lost used to join the queue again at the back,
    // and a few lost every round for 50 s and more (these seeds: 51 to 56 s
    // before; 11 of 15000 seeds over 50 s, 32 over 45 s). Their place is kept
    // now (`waited_ms`).
    for seed in [5324, 5731, 6261, 6549, 6923, 10042] {
        let report = run_seed(seed, locks_long_grants_config())
            .unwrap_or_else(|e| panic!("locks-long-grants seed {seed}: {e}"));
        assert!(
            report.locks.max_wait_owner_ms <= 45_000,
            "locks-long-grants seed {seed}: the owner node's client waited {} ms",
            report.locks.max_wait_owner_ms
        );
    }
}

/// A delegation move carries what is left of a grant's record instead of
/// restamping it to a full lock ttl (`Core::carried_until`; the core tests
/// `a_*_hands_*_what_was_left_of_it`). At the production TTL, with nine ops on
/// a file and renames crossing subtrees, a waiter behind a grant whose holder
/// was gone (a push that lapsed, a holder fenced under a delegate's short
/// authority, a release that reached the old owner after the move) waited 21 s
/// per move: on `7780c8e` 729 of the 2000 seeds of the configuration never got
/// a lock in 60 s (28 without moves), 33 do with the carry. This range of the
/// configuration has none of the 33; 18 of its 39 seeds fail on `7780c8e`.
#[test]
fn delegation_moves_carry_what_is_left_of_a_grant() {
    let (per_seed, totals) = run_lock_seeds(
        "locks-delegated-production-ttl-heavy",
        locks_delegated_production_ttl_heavy_config(),
        25..64,
        &[|s| s.lock_moved_carried, |s| s.lock_moved],
    );
    let [carried, moved] = totals[..] else {
        unreachable!()
    };
    assert!(
        moved > 0 && carried > 0,
        "vacuous: {moved} moved, {carried} carried"
    );
    assert!(
        per_seed.iter().filter(|s| s.carried > 0).count() >= 20,
        "carried in too few seeds: {carried} grants"
    );
}

/// Seeds 1249 and 1774 of the delegated production-TTL configuration: the two
/// of 2000 that still never got a lock in 60 s after the ask-again rule
/// (`7780c8e`). Each moved a grant twice, and the move that restamped it left
/// its record to be outwaited 21 s later (1249: a grant of the delegate's own
/// node; 1774: the grant of the root's node, restamped by the second move).
#[test]
fn regression_locks_delegated_production_ttl_moved_grants_seeds_1249_1774() {
    for seed in [1249, 1774] {
        run_seed(seed, locks_delegated_production_ttl_config()).unwrap_or_else(|e| {
            panic!(
                "locks-delegated-production-ttl seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-delegated-production-ttl"
            )
        });
    }
}

/// Seed 2 of the heavy configuration: three grants answered directly (not
/// from a queue) lapsed at their holders (fenced at 2.2 s) just before a move
/// at 2.4 s, and the restamp kept them 21 s: the first lock was granted at
/// 24 s and a client waited past 60 s.
#[test]
fn regression_locks_delegated_production_ttl_heavy_moved_lapsed_grants_seed_2() {
    run_seed(2, locks_delegated_production_ttl_heavy_config()).unwrap_or_else(|e| {
        panic!(
            "locks-delegated-production-ttl-heavy seed 2: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=locks-delegated-production-ttl-heavy"
        )
    });
}

/// Two waiters of one node on one file, served in one pass, the second
/// re-affirming the first one's grant in place from an older request: the
/// owner's record of the node's grant never ends before the window the
/// node honours (on `c9bee29`: seeds 10 and 32 of this range granted the
/// lock to another node while the holder was inside its section).
/// A recall that adopts a newer id of the owner raises the newest id the
/// node has seen of it: the reply that carries an id in between, which the
/// recall overtook, is refused after the adopted one is released (it was
/// installed beside the owner's next grantee: two exclusive holders, seed
/// 32392 once a node re-sent all of its idle ops at once; the core test
/// `a_reply_older_than_an_adopted_and_released_id_installs_nothing` and the
/// meta one are the reproductions that fail without the fix).
#[test]
fn regression_locks_long_grants_a_recall_overtaking_an_older_reply_seed_32392() {
    run_seed(32392, locks_long_grants_config()).unwrap_or_else(|e| panic!("seed 32392: {e}"));
}

#[test]
fn a_grant_reaffirmed_in_place_never_shortens_the_record() {
    let t = run_m14("locks-long-grants", locks_long_grants_config(), 0..60);
    assert!(t.grants > 500, "{t:?}");
    assert!(t.recalls_sent > 100, "grants rarely contended: {t:?}");
}

/// Plan 43 C7-H2: a backup whose clock is ahead of its holder's seals and
/// takes the lease over; no lock is granted inside the window of a grant
/// the old root made (mutual exclusion), however the successor's clock
/// compares with the old lease's expiry. (On `e797ca3`: about one seed in
/// 25 of this range, a lock granted over a holder still in its section.)
#[test]
fn a_skewed_successor_grants_no_lock_inside_the_old_roots_window() {
    let t = run_m14(
        "fast-takeover-skewed-successor",
        fast_takeover_skewed_successor_config(),
        193_000..193_200,
    );
    assert!(t.grants > 3_000, "{t:?}");
    assert!(
        t.backup_takeovers > 100,
        "the backup rarely took over: {t:?}"
    );
}

/// Plan 43 C7-F F-a0: a backup that seals a holder still inside the floor
/// it inherited from its own seal waits out the first holder's grants
/// too, though its predecessor's lease was never marked as granting (no
/// strict mounts; it granted nothing in its own tenure). On `ccebe74`
/// seed 548 of `double-seal-inside-the-grace` shows two nodes inside
/// conflicting lock sections (as do seeds 27 and 1751 at a 1.25 s gap and
/// 368 and 1091 at 1.75 s).
#[test]
fn a_second_seal_inside_the_grace_waits_out_the_first_holders_grants() {
    let t = run_m14(
        "double-seal-inside-the-grace",
        double_seal_inside_the_grace_config(),
        548..648,
    );
    assert!(
        t.backup_takeovers > 100,
        "the backups rarely took over: {t:?}"
    );
}

/// Plan 43 C7-F F-a0 L3: a backup that seals a holder whose lease was
/// written unmarked (its re-claim of its own released lease) while that
/// holder still waits out grants of its earlier tenure, one of them
/// reclaimed before the backup was listed (the lock mirror never carried
/// it), waits them out too: the reconfiguration that lists the backup marks
/// the lease. On `4a0fb20` 5 of seeds 0..100 of `locks-restart-reclaim-seal`
/// (5, 15, 52, 77, 96) show two nodes inside conflicting lock sections
/// (56 of 0..2000).
#[test]
fn a_sealer_of_a_reclaimed_lease_waits_out_the_grants_that_lease_waited_out() {
    let t = run_m14(
        "locks-restart-reclaim-seal",
        locks_restart_reclaim_seal_config(),
        0..100,
    );
    assert!(
        t.backup_takeovers > 90,
        "the backups rarely took over: {t:?}"
    );
}

/// Plan 43 C7-F F-a0 (b): the first successor of `double-seal-after-the-grace`
/// serves nothing after its floor and clears the mark it inherited, so the
/// seal after the second cut (12.5 s) arms no second floor. On the L3 tree
/// each of the two seals arms one (two floors a seed).
#[test]
fn a_second_seal_after_the_grace_arms_no_second_floor() {
    let t = run_m14(
        "double-seal-after-the-grace",
        double_seal_after_the_grace_config(),
        0..60,
    );
    assert!(
        t.backup_takeovers >= 100,
        "the backups rarely took over: {t:?}"
    );
    assert!(t.mark_clears >= 50, "the mark was rarely cleared: {t:?}");
    assert!(
        t.ack_floor_waits * 10 <= t.backup_takeovers * 6,
        "a floor for the second seal as well: {t:?}"
    );
}

/// Plan 43 C7-F F-a: the same configuration with the cap counted from the
/// beats' *credit* (the holder's clock: `min(at, sent + M)`) instead of
/// their arrival must split: the holder's clock is 4 s behind its
/// successors', and the cap ends their floor 3 s early (a lock granted
/// inside an old grant's window). The control that makes the clean sweeps
/// of the real cap mean something: 1 of the 2 000 seeds 193000..195000 and
/// 7 of the 8 000 seeds 195000..203000 fail (three pinned here), and these
/// three pass with the real cap. (The ask-again traffic changed the
/// trajectories: the earlier pins, 194557 and 194714, no longer fail; nor
/// do 197625 and 200749 since plan 43 C7-F F-a0 (b): the credited cap ends
/// the first successor's floor early, it clears the mark it inherited, and
/// the CAS it then needs, then the mark CAS before its first grant, delay
/// that grant past the old grant's window: 7 of the 10 000 seeds
/// 193000..203000 still fail.) The
/// real cap is not clean over every range: it violates at the *second*
/// failover (about 15 s, a clock beyond the envelope), a different and
/// older failure (`fast_takeover_skewed_successor_known_gaps`), so the
/// control also requires the violation to be inside the first failover's
/// floor window, where the cap ends the floor early.
#[test]
fn a_credited_arrival_cap_splits() {
    for seed in [193_262, 198_196, 198_507] {
        let e = run_seed(seed, fast_takeover_skewed_successor_credited_config())
            .expect_err("a cap counted from the credit never failed: no control");
        assert!(e.contains("mutual exclusion violated"), "seed {seed}: {e}");
        let at = violation_ms(&e).unwrap_or_else(|| panic!("seed {seed}: no time in {e}"));
        assert!(
            at < FIRST_FAILOVER_FLOOR_END_MS,
            "seed {seed}: the violation is at t={at}, not inside the first failover's floor \
             window (a failure of the second failover is not this control's): {e}"
        );
        run_seed(seed, fast_takeover_skewed_successor_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// The end of the first failover's floor window in
/// `fast-takeover-skewed-successor` (the cut at 1 s, the successor's floor a
/// lease TTL and margin later: about 3 to 4 s of simulated time; the second
/// failover comes at about 15 s).
const FIRST_FAILOVER_FLOOR_END_MS: u64 = 6_000;

/// The simulated time (ms) a mutual-exclusion violation message names (its
/// last ` at t=` or ` landed at t=`).
fn violation_ms(e: &str) -> Option<u64> {
    let i = e.rfind(" at t=")? + " at t=".len();
    e[i..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

/// Known gap, not a regression (plan 43 C7b, the cross-node lease-expiry
/// compare under a clock offset beyond the envelope): the *real* cap of
/// `fast-takeover-skewed-successor` violates mutual exclusion at its second
/// failover (t about 15 to 16 s; nodes 2 and 3 run 4 s ahead) in about one
/// seed of 15 000, on 63da672 (seeds 11087 and 18138, which pass with the
/// ask-again traffic) and with it (5570, 11942, 202261): a node whose clock
/// is ahead reads the live holder's lease as expired and seals its epoch, the
/// next holder takes the lease and grants 0.35 to 0.8 s later, and the old
/// holder, alive, keeps admitting I/O under its own grants. Pinned here so
/// that C7b sees them fail and then pass; run with `--ignored`.
#[test]
#[ignore = "known gap: plan 43 C7b (a clock offset beyond the envelope)"]
fn fast_takeover_skewed_successor_known_gaps() {
    for seed in [5_570, 11_942, 202_261] {
        let e = run_seed(seed, fast_takeover_skewed_successor_config())
            .expect_err("fixed? then move the seed to the regression list");
        assert!(e.contains("mutual exclusion violated"), "seed {seed}: {e}");
        let at = violation_ms(&e).unwrap_or_else(|| panic!("seed {seed}: no time in {e}"));
        assert!(
            at >= FIRST_FAILOVER_FLOOR_END_MS,
            "seed {seed}: a violation at t={at}: {e}"
        );
    }
}

/// Plan 43 C7-F F-a: the holder's clock 4 s ahead of its successors'
/// (the cap's lag is negative): the first failover is clean (one failover
/// only, see the configuration), over a dozen seeds in the default run
/// (`long_locks` and `AUTHORITY_SIM_CONFIG=fast-takeover-skewed-holder`
/// run more: 0 of 2 000 at 196000.. and 310000.., 27 of 2 000 with no floor
/// at all).
#[test]
fn a_holder_ahead_of_its_successors_costs_no_exclusion() {
    let mut t = M14Totals::default();
    for seed in 196_000..196_012u64 {
        let report = run_seed(seed, fast_takeover_skewed_holder_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        t.add(&report);
    }
    assert!(t.backup_takeovers > 0, "nobody sealed: {t:?}");
}

/// Plan 43 C7-H1 (2/4): a locker whose wall clock stepped back 3 s while it
/// was cut off honours its lock grant until the grant's end on its honour
/// clock, which the step does not move: it stops when its owner's outwait
/// lets another node have the lock. `locks-locker-step-back`: 479 of 2 000
/// seeds failed before (the first at seeds 0, 4 and 5); the grant's window
/// was a wall value then.
#[test]
fn a_locker_stepped_back_stops_honouring_its_grant_on_time() {
    for seed in 0..12u64 {
        run_seed(seed, locks_locker_step_back_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-H1 (2/4): the holder cut off at 1.2 s and stepped back 8 s
/// stops answering on its honour clock: the lock grants it makes and
/// renews, its strict answers and its lease's use all end where its honour
/// clock says, not 8 s later. `locks-holder-step-back-sealed`: 430 of 2 000
/// seeds failed before (the first at seeds 0 and 6); one fails now (seed
/// 1892: a stepped-back holder's wall expiry, which claimants read, is 8 s
/// short: a node's clock offset beyond the margin, plan 43 C7b).
#[test]
fn a_holder_stepped_back_while_cut_off_stops_answering_on_time() {
    for seed in 0..12u64 {
        run_seed(seed, locks_holder_step_back_sealed_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-H1 (2/4): a reader partitioned from its root, stepped back 3
/// s, stops serving strict reads from its read delegation at the
/// delegation's honour end, before the root's outwait ends. `strict-
/// reader-step-back`: 91 of 2 000 seeds failed before (the first at 9, 40
/// and 54).
#[test]
fn a_reader_stepped_back_stops_serving_its_read_delegation_on_time() {
    for seed in 8..20u64 {
        run_seed(seed, strict_reader_step_back_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-H1 (3/4): the lock owner, stepped forward 2.5 s twice, keeps
/// its grant records on its wait clock and outwaits a grant only when the
/// holder's window has ended. `locks-root-step-forward`: 1 821 of 2 000
/// seeds failed before (the first at 0, 1 and 2).
#[test]
fn a_lock_owner_stepped_forward_does_not_outwait_a_grant_early() {
    for seed in 0..12u64 {
        run_seed(seed, locks_root_step_forward_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-F F-c: a lock owner whose grant lapses across the gap between
/// the old root's last renewal and its successor's existence waits (its
/// I/O parks) and resumes when the old root, or the successor from the old
/// root's final mirror, admits a late renewal of the same grant. Before
/// F-c every one of those I/Os was refused (`EIO`): 127 of them in the first
/// 300 seeds of `locks-s3-cut-holder-alive`, 35 after (the grants that were
/// granted over, or cannot be proved); `locks-s3-cut-holder-lapse` has the
/// longer sections. No mutual-exclusion violation, whatever the park did.
#[test]
fn a_lapsed_locker_resumes_when_the_owner_admits_a_late_renewal() {
    for (label, config) in [
        (
            "locks-s3-cut-holder-alive",
            locks_s3_cut_holder_alive_config(),
        ),
        (
            "locks-s3-cut-holder-lapse",
            locks_s3_cut_holder_lapse_config(),
        ),
    ] {
        let (mut parked, mut resumed, mut refused, mut fenced, mut installed) = (0, 0, 0, 0, 0);
        for seed in 0..60u64 {
            let report = run_seed(seed, config.clone()).unwrap_or_else(|e| {
                panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
            });
            parked += report.locks.parked_ios;
            resumed += report.locks.resumed_ios;
            refused += report.locks.park_refused_ios;
            fenced += report.locks.fenced_ios;
            installed += report
                .stats
                .values()
                .map(|s| s.lock_mirror_final_installed)
                .sum::<u64>();
        }
        assert!(
            resumed > 0,
            "{label}: no parked I/O ever resumed: {parked} parked"
        );
        assert!(
            resumed > refused,
            "{label}: more refused ({refused}) than resumed ({resumed})"
        );
        assert!(
            fenced >= refused,
            "{label}: a refused park fences ({fenced} fenced, {refused} refused)"
        );
        assert!(
            installed > 10,
            "{label}: few final mirrors installed: {installed}"
        );
    }
}

/// Plan 43 C7-F F-c: the successor of a fast takeover trusts only the old
/// root's *final* mirror. Every lock mirror after the first second is lost,
/// a locker is cut from everyone (its grant lapses and is outwaited, the
/// old root grants the lock to another node) and the old root is cut from
/// S3 until its backup seals: the successor's mirror lists the outwaited
/// grant but not the grant over it, and a late renewal of it must be
/// refused. `locks-late-stale-mirror`: 0 violations in 2 000 seeds; the same
/// run with a successor that trusts such a mirror
/// (`Config::lock_trust_nonfinal_mirror`; `-naive`) violates mutual
/// exclusion — in 8 of the first 500 seeds, which are pinned.
#[test]
fn a_late_renewal_against_a_stale_mirror_is_refused_and_the_naive_successor_violates() {
    for seed in [34u64, 51, 106, 163, 203, 318, 336, 363] {
        run_seed(seed, locks_late_stale_mirror_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let e = run_seed(seed, locks_late_stale_mirror_naive_config())
            .expect_err("a successor trusting a stale mirror never violated: no control");
        assert!(
            e.contains("mutual exclusion violated") && e.contains("after a late renewal"),
            "seed {seed}: {e}"
        );
    }
}

/// Plan 43 C7-H1 (3/4): a successor stepped forward inside its takeover
/// floor keeps the floor on its wait clock. `fast-takeover-successor-step-
/// forward`: 21 of 2 000 seeds failed before (the first at 0, 39 and 231).
#[test]
fn a_successor_stepped_forward_keeps_its_takeover_floor() {
    for seed in [0u64, 1, 2, 38, 39, 40, 230, 231, 232] {
        run_seed(seed, fast_takeover_successor_step_forward_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-H1 (3/4): a root stepped forward 3 s outwaits a delegation on
/// its wait clock. `delegated-root-step-forward`: 4 of 2 000 seeds failed
/// before (the first at 401, 954 and 1230).
#[test]
fn a_root_stepped_forward_does_not_outwait_a_delegation_early() {
    for seed in [400u64, 401, 402, 953, 954, 955, 1229, 1230, 1231] {
        run_seed(seed, delegated_root_step_forward_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-H1 (3/4, R7): the beat cap across a forward step of a holder
/// cut from S3 only (it keeps beating) is counted from beats and a lease end
/// on the holder's wait clock, so it is not early by the step for the read
/// delegations granted before it. `strict-isolated-reader-holder-step-
/// forward`: 3 of 2 000 seeds failed before (605, 710, 1319: close-to-open).
#[test]
fn a_cut_holder_stepped_forward_does_not_cap_its_successors_floor_early() {
    for seed in [604u64, 605, 606, 709, 710, 711, 1318, 1319, 1320] {
        run_seed(seed, strict_isolated_reader_holder_step_forward_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (2/6): a lock owner whose oscillator runs 8 % fast, under a
/// rate budget of 80 000 ppm, outwaits a cut locker on its wait clock, which
/// counts `1/1.08` of the raw clock: not before the locker's grant ended.
/// `locks-root-rate-fast` (and `-rate-skew`, the same rate set by a fault
/// at 1 s): 1 364 and 1 356 of 2 000 seeds failed before (the first at 0, 1
/// and 4: two exclusive lockers for about half a second).
#[test]
fn a_fast_lock_owner_does_not_outwait_a_cut_locker_early() {
    for seed in 0..12u64 {
        run_seed(seed, locks_root_rate_fast_config())
            .unwrap_or_else(|e| panic!("rate-fast seed {seed}: {e}"));
        run_seed(seed, locks_root_rate_skew_config())
            .unwrap_or_else(|e| panic!("rate-skew seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (2/6): a successor whose oscillator is 30 % fast keeps its
/// takeover floor for the predecessor's last lock grant: the floor counts
/// on its wait clock, `1/1.3` of what the raw clock says.
/// `fast-takeover-successor-rate-fast`: 1 984 of 2 000 seeds failed before
/// (the first at 0, 1 and 2).
#[test]
fn a_fast_successor_keeps_its_takeover_floor() {
    for seed in 0..12u64 {
        run_seed(seed, fast_takeover_successor_rate_fast_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (2/6): the beat cap, counted from a slow holder's wait clock
/// onto a fast backup's, is early only for beats sent before the renewal
/// that set the lease's end (the holder 25 % slow, its backup 25 % fast); the
/// span filter (`M/(4ρ)`, from the budget) leaves those out, so the floor is not capped early.
/// `fast-takeover-holder-rate-fast`: 224 of 2 000 seeds failed before (the
/// first at 24, 27 and 44).
#[test]
fn a_cap_from_a_slow_holders_beats_does_not_end_the_floor_early() {
    for seed in [23u64, 24, 25, 26, 27, 28, 43, 44, 45] {
        run_seed(seed, fast_takeover_holder_rate_fast_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (2/6): a holder whose oscillator is 20 % fast outwaits an
/// isolated reader's read delegation on its wait clock, as long as the
/// reader honours it. `strict-reader-rate-fast`: 613 of 2 000 seeds failed
/// before (the first at 3, 4 and 16: a stale strict read).
#[test]
fn a_fast_holder_does_not_outwait_a_read_delegation_early() {
    for seed in [2u64, 3, 4, 5, 6, 15, 16, 17] {
        run_seed(seed, strict_reader_rate_fast_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-F F-a (review F3): the strict side of the floor. A reader
/// cut from the holder and its backup serves from its read delegation
/// while the backup, having sealed the crashed holder, takes over: with the
/// floor (C7-H2's bound with no beats, the cap with them) no write is
/// acknowledged inside the delegation, and none is missed. On a build with
/// no floor at all the same seeds violate close-to-open: counts in
/// PROGRESS (`strict-isolated-reader`, 2 000 seeds), where the sweep is
/// `AUTHORITY_SIM_CONFIG=strict-isolated-reader[-beats]`.
#[test]
fn a_reader_cut_off_inside_the_takeover_floor_misses_no_acknowledged_write() {
    for (label, config) in [
        ("strict-isolated-reader", strict_isolated_reader_config()),
        (
            "strict-isolated-reader-beats",
            strict_isolated_reader_beats_config(),
        ),
        (
            "strict-isolated-reader-skewed",
            strict_isolated_reader_skewed_config(),
        ),
    ] {
        let mut sealed = 0;
        for seed in 0..40u64 {
            let report = run_seed(seed, config.clone()).unwrap_or_else(|e| {
                panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
            });
            sealed += report
                .stats
                .values()
                .map(|s| s.backup_takeovers)
                .sum::<u64>();
        }
        assert!(sealed > 10, "{label}: the backups rarely sealed: {sealed}");
    }
}

/// Plan 43 C7a (3/6): the root restarted on the same boot with its wall
/// stepped 3 s forward while it was down, or while it was up after its last
/// horizon write, reads its lock horizon on the raw clock (a boot stamp), so
/// it grants nothing over a grant still honoured. `locks-root-restart-step-
/// forward`: 1 143 of 2 000 seeds failed before (the first at 0, 1 and 2),
/// `locks-root-step-forward-then-restart` 774 (3, 4 and 6).
#[test]
fn a_restarted_root_keeps_its_lock_horizon_across_a_forward_step() {
    for seed in 0..12u64 {
        run_seed(seed, locks_root_restart_step_forward_config())
            .unwrap_or_else(|e| panic!("restart seed {seed}: {e}"));
        run_seed(seed, locks_root_step_forward_then_restart_config())
            .unwrap_or_else(|e| panic!("step-then-restart seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (3/6): the same across a reboot (a new boot id: the horizon
/// is waited out as the span it was written with, whatever the new boot's
/// raw clock says). `locks-root-reboot-step-forward`: 1 143 of 2 000 seeds
/// failed before (the first at 0, 1 and 2); the cost control `locks-root-
/// reboot` (no step) is clean before and after.
#[test]
fn a_rebooted_root_waits_its_lock_horizon_out_as_a_span() {
    for seed in 0..12u64 {
        run_seed(seed, locks_root_reboot_step_forward_config())
            .unwrap_or_else(|e| panic!("reboot-step seed {seed}: {e}"));
        run_seed(seed, locks_root_reboot_config())
            .unwrap_or_else(|e| panic!("reboot seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (3/6): a holder restarted with its wall stepped 7 s forward
/// reads its read-grant horizon on the raw clock: it does not acknowledge a
/// write while a reader still serves from its read delegation.
/// `strict-holder-restart-step-forward`: 212 of 2 000 seeds failed before
/// (the first at 9, 40 and 47: a stale strict read).
#[test]
fn a_restarted_holder_keeps_its_read_horizon_across_a_forward_step() {
    for seed in [8u64, 9, 10, 39, 40, 41, 46, 47, 48] {
        run_seed(seed, strict_holder_restart_step_forward_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7a (3/6, M4): an own lease the restarted holder's wall reads as
/// expired is re-adopted through the gate like a live one (the tail, the
/// marker, the strand and replay): `holder-restart-step-forward-readopt` was
/// clean on the parent too (the restart quarantine covers it); a regression
/// configuration for the rule the core test pins.
#[test]
fn a_holder_restarted_past_its_lease_readopts_it_through_the_gate() {
    for seed in 0..12u64 {
        run_seed(seed, holder_restart_step_forward_readopt_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-F F-a (review F3): the credited-arrival control on the
/// strict side, where the window it needs is wide: with the successor's
/// clock 4 s ahead, a cap counted from the beats' credit ends the floor
/// about 3 s early and the successor acknowledges writes inside the cut-off
/// reader's delegation: 209 of 2 000 seeds fail (10 %), none with the
/// arrival. Three of them are pinned.
#[test]
fn a_credited_arrival_cap_splits_on_the_strict_side() {
    for seed in [9, 40, 47] {
        let e = run_seed(seed, strict_isolated_reader_skewed_credited_config())
            .expect_err("a cap counted from the credit never failed: no control");
        assert!(e.contains("close-to-open violated"), "seed {seed}: {e}");
        run_seed(seed, strict_isolated_reader_skewed_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// Plan 43 C7-F F-a: the holder is cut from S3 and keeps beating until its
/// lease ends; the backup seals at the expiry it reads and its floor ends
/// about one margin after it, not after the whole bound. Every floor armed
/// is capped (or capped out), and none outlasts the margin plus two
/// takeover windows past its marker. On the parent every floor is the
/// bound (`ack_floor_capped` does not exist).
#[test]
fn an_s3_cut_holder_that_beats_on_costs_its_successor_about_a_margin() {
    let mut t = M14Totals::default();
    let mut floors = Vec::new();
    let mut capped = 0;
    let mut capped_out = 0;
    for seed in 207_000..207_012u64 {
        let report = run_seed(seed, locks_s3_cut_holder_alive_config())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        t.add(&report);
        for s in report.stats.values() {
            capped += s.ack_floor_capped;
            capped_out += s.ack_floor_capped_out;
        }
        floors.extend(report.floor_ms.iter().copied());
    }
    let cfg = sim::run::backup_core_config(2, 1);
    let limit = cfg.expiry_margin_ms + 2 * cfg.backup_takeover_ms;
    eprintln!(
        "locks-s3-cut-holder-alive: floors {floors:?}, capped {capped}, capped out {capped_out}"
    );
    assert!(
        capped + capped_out > 0,
        "no floor was capped: {t:?}, floors {floors:?}"
    );
    assert!(
        floors.iter().all(|f| *f <= limit),
        "a floor outlasted the margin and two windows ({limit} ms): {floors:?}"
    );
    assert!(t.backup_takeovers > 0, "nobody sealed: {t:?}");
}

/// `stress-ng-fs-faults` (bug B): a node that keeps its lease through an
/// S3 blip keeps its lock grants. Epochs form and close around critical
/// sections, yet no grant is lost, no I/O under a lock is fenced, and no
/// lock is refused for want of a sequencer.
#[test]
fn locks_survive_s3_blips() {
    let mut epochs = 0;
    let mut t = M14Totals::default();
    for seed in 99_000..99_060 {
        let report = run_seed(seed, locks_blips_config()).unwrap_or_else(|e| {
            panic!("locks-blips seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips")
        });
        epochs += report.epochs_formed;
        t.add(&report);
    }
    eprintln!("locks-blips: {epochs} epochs, {t:#?}");
    assert!(epochs > 120, "epochs rarely formed: {epochs}");
    assert!(t.grants > 500, "{t:?}");
    assert_eq!(t.lost, 0, "grants lost across blips: {t:?}");
    assert_eq!(t.clients.fenced_ios, 0, "I/O fenced across blips: {t:?}");
    assert_eq!(
        t.clients.unavailable, 0,
        "locks refused across blips: {t:?}"
    );
    // Cuts in back-to-back pairs: some begin before the last epoch has
    // closed everywhere, and that epoch goes on; once its hold owner has
    // closed (its re-claim cut off), its fresh epoch replaces the open one
    // and carries the lease the close let go, so no grant lapses with the
    // 6 s lease either (chunk epoch-liveness-gap: 4 fenced I/Os here
    // before).
    let mut continued = 0;
    let mut superseded = 0;
    let mut reheld = 0;
    let mut t = M14Totals::default();
    for seed in 99_100..99_160 {
        let report = run_seed(seed, locks_blips_tight_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight"
            )
        });
        continued += report.epochs_continued;
        superseded += report.epochs_superseded as u64;
        reheld += report
            .stats
            .values()
            .map(|s| s.epoch_closed_leases_reheld)
            .sum::<u64>();
        t.add(&report);
    }
    eprintln!(
        "locks-blips-tight: {continued} epochs continued, {superseded} replaced by the \
         carrier's, {reheld} closed leases held again, {t:#?}"
    );
    assert!(continued > 0, "no cut fell before an epoch's close");
    assert!(
        superseded > 0,
        "no closed carrier's epoch replaced an open one"
    );
    assert!(reheld > 0, "no cut fell inside a re-claim window");
    assert_eq!(t.lost, 0, "grants lost across blips: {t:?}");
    assert_eq!(t.clients.fenced_ios, 0, "I/O fenced across blips: {t:?}");
    assert_eq!(
        t.clients.unavailable, 0,
        "locks refused across blips: {t:?}"
    );
    // The same with a 10 s lease.
    let mut t = M14Totals::default();
    for seed in 99_100..99_160 {
        let report = run_seed(seed, locks_blips_tight_long_lease_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight-long-lease seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight-long-lease"
            )
        });
        t.add(&report);
    }
    eprintln!("locks-blips-tight-long-lease: {t:#?}");
    assert_eq!(t.lost, 0, "grants lost across blips: {t:?}");
    assert_eq!(t.clients.fenced_ios, 0, "I/O fenced across blips: {t:?}");
    assert_eq!(
        t.clients.unavailable, 0,
        "locks refused across blips: {t:?}"
    );
    // Cuts between a close and the lease's re-claim: the next epoch
    // carries the lease the close let go (one node: see
    // `locks_blips_tight_single_config`).
    let mut reheld = 0;
    let mut t = M14Totals::default();
    for seed in 99_100..99_160 {
        let report = run_seed(seed, locks_blips_tight_single_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight-single seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight-single"
            )
        });
        reheld += report
            .stats
            .values()
            .map(|s| s.epoch_closed_leases_reheld)
            .sum::<u64>();
        t.add(&report);
    }
    eprintln!("locks-blips-tight-single: {reheld} closed leases held again, {t:#?}");
    assert!(reheld > 0, "no cut fell inside a re-claim window");
    assert_eq!(t.lost, 0, "grants lost across blips: {t:?}");
    assert_eq!(t.clients.fenced_ios, 0, "I/O fenced across blips: {t:?}");
    assert_eq!(
        t.clients.unavailable, 0,
        "locks refused across blips: {t:?}"
    );
}

/// `locks-blips-tight` seed 2723: a lease request admitted with no lock
/// grant out was served after two exclusive grants were made while the
/// handoff flushed, and the release dropped them under their holders'
/// I/O; the successor's grace went with its own epoch-flush release, and
/// its re-claim of that lease granted over the live exclusive grant.
///
/// No I/O is fenced with either lease. With the 6 s one, one I/O was
/// before chunk epoch-liveness-gap: the hold owner closed its epoch at
/// t=8701 on a probe that landed as S3 was cut again (t=8500), the other
/// members stayed in that epoch waiting for its re-claim, so none formed,
/// and the closed lease, last renewed before the cut, expired at t≈9.8 s
/// inside it with the grants it kept. The owner's fresh epoch now
/// replaces the open one and carries that lease.
#[test]
fn locks_blips_tight_seed_2723_release_keeps_exclusion() {
    let report = run_seed(2723, locks_blips_tight_config()).unwrap_or_else(|e| {
        panic!("locks-blips-tight seed 2723: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight")
    });
    let mut t = M14Totals::default();
    t.add(&report);
    assert_eq!(t.lost, 0, "grants lost: {t:?}");
    assert_eq!(t.clients.fenced_ios, 0, "I/O fenced: {t:?}");
    let report = run_seed(2723, locks_blips_tight_long_lease_config()).unwrap_or_else(|e| {
        panic!(
            "locks-blips-tight-long-lease seed 2723: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight-long-lease"
        )
    });
    let mut t = M14Totals::default();
    t.add(&report);
    assert_eq!(t.lost, 0, "grants lost: {t:?}");
    assert_eq!(t.clients.fenced_ios, 0, "I/O fenced: {t:?}");
}

/// `locks-failover` seed 5297: node 2's recall of grant 558 (the owner
/// served a stale waiter of its with it after node 2 released 557)
/// overtook the push of 558; nothing was held and no op waited, so node
/// 2 released it at once. Its next op on the inode then installed the
/// late push, and node 3 entered I/O under 559 beside it: two exclusive
/// holders. The fast-path release tombstones the id, so the late push is
/// refused and the op asks again.
#[test]
fn locks_failover_seed_5297_a_push_behind_its_released_recall_is_refused() {
    run_seed(5297, locks_failover_config()).unwrap_or_else(|e| {
        panic!("locks-failover seed 5297: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-failover")
    });
}

/// `locks-blips-tight-in-doubt` seed 1383: node 3 released its grant and
/// asked again at once; the request overtook the release at the owner,
/// which re-affirmed the grant under a new id, then took the release of
/// the old id for the end of the new one and granted the inode to itself
/// beside node 3's: two exclusive holders. Exposed once the sim stopped
/// forming an epoch over an open one (see the next test).
#[test]
fn locks_blips_tight_in_doubt_seed_1383_an_overtaken_release_keeps_exclusion() {
    run_seed(1383, locks_blips_tight_in_doubt_config()).unwrap_or_else(|e| {
        panic!(
            "locks-blips-tight-in-doubt seed 1383: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight-in-doubt"
        )
    });
}

/// `locks-blips-tight` seeds 200 and 8398: the next outage came before
/// the last continuation epoch (carrying node 1's lease, node 1 owning
/// its hold) had closed. The sim's coordinator formed a second epoch
/// over the open one, which the daemon never does (an open member
/// neither proposes nor accepts another epoch's promise); its carrier
/// resolved to `None`, since the hold's owner claims nothing, so the
/// other members closed on S3's return as members of an epoch carrying
/// no lease, and one of them took the expired lease over (seed 200: the
/// dead-root check, seed 8398: a lock acquisition) beside node 1's hold.
/// The epoch now goes on instead, and its members wait for the carried
/// lease to move — or, once its carrier has closed, the carrier's fresh
/// epoch replaces it (chunk epoch-liveness-gap: seed 8398's outage at
/// t=2.5 s does). Each seed must meet an open epoch.
#[test]
fn locks_blips_tight_an_open_epoch_is_not_formed_over() {
    for seed in [200, 8398] {
        let report = run_seed(seed, locks_blips_tight_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight"
            )
        });
        assert!(
            report.epochs_continued > 0,
            "seed {seed}: no outage met an open epoch"
        );
    }
}

/// Chunk epoch-liveness-gap, `locks-blips-tight` seed 8: the hold owner
/// (node 1) closed the epoch formed at t=12.8 s at t=14825, after S3 was
/// cut again at t=14.7 s and before its re-claim could land. Nodes 2 and
/// 3 were still in that epoch, waiting for the carried lease to move, so
/// no epoch formed, and node 3's exclusive grant, kept by the close and
/// renewable only up to the closed lease's expiry, lapsed under its I/O
/// at t=15709. Node 1's fresh epoch now replaces the open one at every
/// member and carries the closed lease, which node 1 holds again.
///
/// Seed 66: the same pair of cuts, with node 1's re-claim CAS in flight
/// as S3 was cut again at t=8.5 s (answered in doubt, its re-read cut
/// off). Node 1 now claims the object that CAS may have written, the
/// fresh epoch carries it, and nodes 2 and 3 stay in it whichever of the
/// two objects S3 shows. While a claim in doubt was carried by no epoch
/// with others, the epoch at t=8.8 s carried no lease and two I/Os were
/// fenced at the closed lease's expiry.
///
/// Seed 568: a lock renewal crossing a recall. Node 3's grant came by a
/// waiter's push (its window counted from the request 770 ms earlier), a
/// recall moved it to the owner's re-affirmed id, and the answer to the
/// renewal sent under the old id ("renewed, now the new id") was dropped,
/// so the grant lapsed under I/O 350 ms after its install
/// (`LockTables::renewed`).
#[test]
fn locks_blips_tight_seeds_8_66_and_568_no_grant_lapses() {
    for seed in [8, 66, 568] {
        let report = run_seed(seed, locks_blips_tight_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight"
            )
        });
        let mut t = M14Totals::default();
        t.add(&report);
        assert_eq!(t.lost, 0, "seed {seed}: grants lost: {t:?}");
        assert_eq!(t.clients.fenced_ios, 0, "seed {seed}: I/O fenced: {t:?}");
    }
    // The mechanism itself (a closed carrier's fresh epoch replacing an open
    // one, and the closed lease held again) is asserted over a seed range
    // rather than per seed: which seed reaches it depends on lock timing.
    let (mut superseded, mut reheld) = (0u64, 0u64);
    for seed in 0..64 {
        let report = run_seed(seed, locks_blips_tight_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-blips-tight"
            )
        });
        superseded += report.epochs_superseded as u64;
        reheld += report
            .stats
            .values()
            .map(|s| s.epoch_closed_leases_reheld)
            .sum::<u64>();
    }
    assert!(
        superseded > 0,
        "seeds 0..64: no closed carrier's epoch replaced an open one"
    );
    assert!(reheld > 0, "seeds 0..64: no closed lease was held again");
}

/// Chunk epoch-liveness-gap, with random faults on top of the paired
/// cuts (sweep-only configs `flex-tight`, `locks-blips-tight-faults`):
///
/// - `flex-tight` 1913: node 3 closed (t=2495) and was paused (t=2769)
///   as its fresh epoch replaced the open one (t=2800); the activation
///   reached it at t=5662, past the closed lease's expiry margin, and it
///   held nothing (the clock check on the closed claim), so nobody held
///   the epoch, it never closed, and an unlink answered `ENOENT` against
///   the history. An activation carrying the closed lease now holds it
///   whenever it arrives, as one carrying a held lease does.
/// - `locks-blips-tight-faults` 409: the re-claim's CAS landed while the
///   hold owner held its fresh epoch, its members closed on the moved
///   object, and the owner, crashed meanwhile, re-adopted its hold beside
///   their takeover: two authorities. No acquisition CAS goes out once an
///   epoch is open, and none is out under a carried closed claim.
/// - `locks-blips-tight-faults` 945: the sim replaced the open epoch in
///   the name of a carrier paused for 4.8 s; a daemon proposes only
///   while it runs.
/// - `locks-blips-tight-faults` 5379 (failed on main too, with the
///   config): the sim formed an epoch with node 1 paused, acking in its
///   name; it carried node 1's held lease, node 1 resumed past that
///   lease's expiry and adopted no hold, and the epoch never closed (a
///   lock waiter timed out after 60 s). A paused node is no member: it
///   answers no ping.
#[test]
fn epoch_liveness_gap_fault_seeds() {
    for (config, seed) in [
        ("flex-tight", 1913),
        ("locks-blips-tight-faults", 409),
        ("locks-blips-tight-faults", 945),
        ("locks-blips-tight-faults", 5379),
    ] {
        let cfg = match config {
            "flex-tight" => flex_tight_config(),
            _ => locks_blips_tight_faults_config(),
        };
        let report = run_seed(seed, cfg).unwrap_or_else(|e| {
            panic!("{config} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={config}")
        });
        let mut t = M14Totals::default();
        t.add(&report);
        assert_eq!(t.lost, 0, "{config} seed {seed}: grants lost: {t:?}");
    }
}

/// `locks-blips-tight` with in-doubt lease PUTs: an acquisition CAS that
/// landed but answered a timeout left the lease nobody's in practice
/// (403486, 405329, 412350, 413006, 422266 on main: a lock waiter timed
/// out after 60 s), and two non-holders that cached each other bounced
/// every lock request between them (400925).
#[test]
fn locks_blips_tight_in_doubt_seeds_acquire() {
    for seed in [400_925, 403_486, 405_329, 412_350, 413_006, 422_266] {
        run_seed(seed, locks_blips_tight_in_doubt_config()).unwrap_or_else(|e| {
            panic!(
                "locks-blips-tight-in-doubt seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-blips-tight-in-doubt"
            )
        });
    }
}

/// Plan 30 §M14: the CI's random faults under the lock workload.
#[test]
fn locks_under_random_faults() {
    let t = run_m14("locks-faults", locks_faults_config(), 95_000..95_120);
    assert!(t.grants > 500, "{t:?}");
}

/// F1: a fast-path write held behind a slow writer past its grant's end
/// is refused at the write; the next holder's turn survives
/// (`strict_lock_turns`), mutual exclusion holds.
#[test]
fn locks_writer_hold_refuses_the_held_write() {
    let t = run_m14(
        "locks-writer-hold",
        locks_writer_hold_config(),
        230_000..230_060,
    );
    assert!(t.held_writes > 60, "writes were rarely held: {t:?}");
    assert!(
        t.held_refused > 0,
        "no held write was refused at the write: {t:?}"
    );
    assert!(t.held_accepted > 0, "no short hold landed: {t:?}");
    assert_eq!(t.clients.stale_turn_reads, 0, "{t:?}");
}

/// F1 (review T1): a delegate's write held in its transaction under a
/// lock the root minted, while the delegate is cut off past the grant's
/// window: the next holder never gets the lock before the write is under
/// its floor.
#[test]
fn locks_writer_hold_delegated_settles_before_the_head() {
    let t = run_m14(
        "locks-writer-hold-delegated",
        locks_writer_hold_delegated_config(),
        233_000..233_040,
    );
    assert!(t.held_writes > 20, "writes were rarely held: {t:?}");
    assert!(
        t.settle_waits > 0,
        "no position waited for a held write: {t:?}"
    );
    assert_eq!(t.clients.stale_turn_reads, 0, "{t:?}");
}

/// F1 (review T1), the settle's fail-before: the seeds where, with
/// `settle_tagged_writes` off, the delegate reported its stream head while
/// its pinned write was still committing, the root granted on from it,
/// and the write landed in the next holder's critical section (found by a
/// 3000-seed sweep of `locks-writer-hold-delegated-unsettled`: 4 seeds
/// with the ask-again traffic, which changed the earlier trajectories).
/// Each passes with the settle. Plan 43 C7-F F-c moved the trajectories (a
/// parked op instead of a fenced one): of these four only 745 and 1081 failed
/// on the parent in 0..2000 and here only 1282 does, so 1282 joins the list.
#[test]
fn locks_writer_hold_delegated_without_the_settle_is_caught() {
    let seeds = [745, 1081, 1282, 2045, 2369];
    let mut caught = 0;
    for seed in seeds {
        if let Err(e) = run_seed(seed, locks_writer_hold_delegated_unsettled_config()) {
            eprintln!("seed {seed}: {}", e.lines().next().unwrap_or(""));
            // The intended signature, not any error: the held write landed
            // inside the next holder's critical section.
            assert!(
                e.contains("held write") && e.contains("inside"),
                "seed {seed}: another failure: {e}"
            );
            caught += 1;
        }
        run_seed(seed, locks_writer_hold_delegated_config())
            .unwrap_or_else(|e| panic!("locks-writer-hold-delegated seed {seed}: {e}"));
    }
    assert!(
        caught > 0,
        "no write landed under the next holder without the settle"
    );
}

/// F1 (review T2): the held delegate write's client gives up on it (in
/// doubt) and unlocks; the recalled grant is left to the root's outwait.
#[test]
fn locks_writer_hold_delegated_doubt_lapses_the_grant() {
    let t = run_m14(
        "locks-writer-hold-delegated-doubt",
        locks_writer_hold_delegated_doubt_config(),
        234_000..234_040,
    );
    assert!(t.held_writes > 20, "writes were rarely held: {t:?}");
    assert!(
        t.doubt_lapses > 0,
        "no grant was left to the outwait: {t:?}"
    );
}

/// F1's fail-before: without the check at the write, a held write lands
/// after the next holder's turn and its application is told it succeeded.
#[test]
fn locks_writer_hold_without_the_check_at_write_is_caught() {
    let mut caught = 0;
    for seed in 230_000..230_060 {
        if let Err(e) = run_seed(seed, locks_writer_hold_unchecked_config()) {
            eprintln!("seed {seed}: {}", e.lines().next().unwrap_or(""));
            caught += 1;
        }
    }
    assert!(
        caught > 0,
        "a held write past its grant never landed over the next holder's"
    );
}

/// The seeds the first sweeps of the F1 configurations failed, on two
/// older bugs: a late lock reply honoured from its arrival (stall-step
/// 1934: two exclusive holders) and an own grant dropped unreleased
/// without a floor (stall-step 826, 1843; writer-hold 195, 1139: a stale
/// turn read).
#[test]
fn regression_f1_stall_seeds() {
    for (label, cfg, seed) in [
        ("locks-stall-step", locks_stall_step_config(), 1934),
        ("locks-stall-step", locks_stall_step_config(), 826),
        ("locks-stall-step", locks_stall_step_config(), 1843),
        ("locks-writer-hold", locks_writer_hold_config(), 195),
        ("locks-writer-hold", locks_writer_hold_config(), 1139),
    ] {
        run_seed(seed, cfg).unwrap_or_else(|e| panic!("{label} seed {seed}: {e}"));
    }
}

/// F1, F8: stalled steps (a `now` read before a wait in the store) on
/// grant owners, executors and holders keep mutual exclusion and the
/// turns, the executor judging tokens by the window included.
#[test]
fn locks_survive_stalled_steps() {
    for (label, cfg, seeds) in [
        (
            "locks-stall-step",
            locks_stall_step_config(),
            231_000..231_040,
        ),
        (
            "locks-stall-step-delegated",
            locks_stall_step_delegated_config(),
            232_000..232_040,
        ),
    ] {
        let t = run_m14(label, cfg, seeds);
        assert!(t.grants > 300, "{label}: {t:?}");
        assert!(t.clients.turn_reads > 0, "{label}: {t:?}");
    }
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

/// overload-cascade-2: lock files in a delegated subtree (`d1`, node 2)
/// unlinked while other clients hold them open and locked, as
/// `stress-ng`'s lock stressors do; every client keeps locking the
/// unlinked inode, whose owner is the root by location from the unlink
/// on. The delegate's grants on it go to the root with the batch that
/// carries the unlink; the mutual-exclusion ghost and the turn checks
/// (writes in `d2`) catch a root that grants over one it never got.
fn locks_unlinked_delegated_config() -> SimConfig {
    SimConfig {
        lock_unlink_ratio: 0.2,
        ..with_lock_writes(locks_delegated_config(), "d2")
    }
}

/// overload-cascade-2 review round 2 (sweep only, seed 41 pinned):
/// `locks-unlinked-delegated` with the root holder crashing at 1.5 s
/// (back at 6.5 s with its journal), with or without the M9 backups.
/// With them, node 2 (the delegate of `d1`) is often the root's backup:
/// it takes the root over and ends its own generation, whose grants on
/// unlinked inodes are now the root's.
fn locks_unlinked_hcrash_config(backup: bool) -> SimConfig {
    let base = locks_unlinked_delegated_config();
    SimConfig {
        core: if backup {
            std::sync::Arc::new(sim::run::backup_core_config)
        } else {
            base.core.clone()
        },
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::CrashHolder {
                restart_ms: Some(5_000),
                keep_journal: true,
            },
        }],
        ..base
    }
}

/// The delegate of `d1` (node 2) dies at 1.5 s and is back at 10.5 s with
/// its journal, with or without delegate backups: its streamed unlinks
/// reach the root from its stream, its backup's drain or its journal.
fn locks_unlinked_dcrash_config(backup: bool) -> SimConfig {
    let base = locks_unlinked_delegated_config();
    SimConfig {
        core: if backup {
            std::sync::Arc::new(sim::run::deleg_backup_core_config)
        } else {
            base.core.clone()
        },
        faults: vec![ScheduledFault {
            at_ms: 1_500,
            kind: FaultKind::CrashNode {
                node: 2,
                restart_ms: Some(9_000),
                keep_journal: true,
            },
        }],
        ..base
    }
}

/// [`locks_unlinked_dcrash_config`] (with backups) after a pause of the
/// root holder (node 1) at 0.559 s for 1.3 s: node 2 seals and takes the
/// lease and crashes at once, and node 1 wakes still believing it is the
/// root, appends a batch of delegate node 3's stream to its journal, then
/// learns it is deposed and rebuilds. Dropping those rows left the
/// generation's log index where the append had raised it, so when node 1
/// took the root over again it inherited the generation past the rows it
/// no longer had, and the delegate's re-stream skipped them.
fn locks_unlinked_dcrash_pause_config() -> SimConfig {
    let mut c = locks_unlinked_dcrash_config(true);
    c.faults.insert(
        0,
        ScheduledFault {
            at_ms: 559,
            kind: FaultKind::Pause {
                node: 1,
                for_ms: 1_300,
            },
        },
    );
    c
}

/// The CI's two random faults, with or without delegate backups.
fn locks_unlinked_random_config(backup: bool) -> SimConfig {
    let base = locks_unlinked_delegated_config();
    SimConfig {
        core: if backup {
            std::sync::Arc::new(sim::run::deleg_backup_core_config)
        } else {
            base.core.clone()
        },
        random_faults: 2,
        ..base
    }
}

/// `locks-blips`' S3 outages and in-doubt lease PUTs.
fn locks_unlinked_blips_config() -> SimConfig {
    let c = locks_blips_config();
    SimConfig {
        ops_per_client: c.ops_per_client,
        lock_ios: c.lock_ios,
        lock_io_ms: c.lock_io_ms,
        faults: c.faults,
        ..locks_unlinked_delegated_config()
    }
}

/// `locks-partition`'s cut lockers (plus its random fault).
fn locks_unlinked_partition_config() -> SimConfig {
    let c = locks_partition_config();
    SimConfig {
        random_faults: c.random_faults,
        lock_ios: c.lock_ios,
        lock_io_ms: c.lock_io_ms,
        faults: c.faults,
        ..locks_unlinked_delegated_config()
    }
}

/// `locks-unlinked-delegated-partition` without the unlinks: the lock
/// files stay in `d1` (node 2's), so every grant's owner is a delegate,
/// and the turns go to `d2` (node 3's). A locker cut from both is
/// outwaited by node 2 while its acknowledged turn is in its own `d2`
/// stream, which the root has not appended (stale-read-outwaited: 57 of
/// 3000 seeds read an older turn before the fix).
fn locks_delegated_partition_config() -> SimConfig {
    SimConfig {
        lock_unlink_ratio: 0.0,
        ..locks_unlinked_partition_config()
    }
}

/// overload-cascade-2 review, must-fix 1: see
/// [`locks_unlinked_delegated_config`]. Non-vacuous: the root installs
/// grants that left a subtree with its unlink.
#[test]
fn locks_on_files_unlinked_in_a_delegated_subtree() {
    let t = run_m14(
        "locks-unlinked-delegated",
        locks_unlinked_delegated_config(),
        99_500..99_580,
    );
    assert!(t.grants > 300, "{t:?}");
    assert!(
        t.leaving_installed > 0,
        "no grant ever left a subtree with its unlink: {t:?}"
    );
    assert_eq!(t.clients.stale_turn_reads, 0, "{t:?}");
    assert_eq!(t.clients.late_unacked_turns, 0, "{t:?}");
}

/// `locks-unlinked-delegated-hcrash-backup` seed 41 (this chunk's
/// round-2 review): node 2, `d1`'s delegate and the root's backup,
/// granted node 3 an exclusive lock on `lk0` and unlinked it; the root
/// died before it applied that batch. Node 2 took the root over, ended
/// its own inherited generation and dropped the grant (no grace covered
/// the unlinked inode), then granted itself `lk0` under node 3's I/O.
/// Now the grant stays in its table (`Core::drop_delegate_state`), and a
/// subtree grace covers unlinked inodes (`Core::lock_in_grace`).
#[test]
fn regression_locks_unlinked_delegated_hcrash_backup_seed_41() {
    run_seed(41, locks_unlinked_hcrash_config(true)).unwrap_or_else(|e| {
        panic!(
            "locks-unlinked-delegated-hcrash-backup seed 41: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=locks-unlinked-delegated-hcrash-backup"
        )
    });
}

/// `locks-unlinked-delegated-dbackup-random` seeds 2477 and 2982
/// (overload-cascade-2 round 3, should-fix 2): node 2, a delegate and
/// the root's backup, took the root over while it held rows of its
/// generation the dead root never appended; the takeover's strand made
/// them root rows, and node 2 crashed inside its gate, before it ended
/// the generation itself. Restarted as the generation's delegate, its
/// persisted stream counter still counted the stranded rows: its next
/// row followed the hole, the new root's cursor waited below it, and the
/// same batch was re-sent for the rest of the run. Now the strand marks
/// the generation (`Meta::delegate_stranded`): it executes nothing more
/// and answers a recall at the log's index of it, so the root ends it.
#[test]
fn regression_locks_unlinked_delegated_dbackup_random_seeds_2477_2982() {
    for seed in [2_477, 2_982] {
        run_seed(seed, locks_unlinked_random_config(true)).unwrap_or_else(|e| {
            panic!(
                "locks-unlinked-delegated-dbackup-random seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-unlinked-delegated-dbackup-random"
            )
        });
    }
}

/// delegate-stream-acks: `long-delegated-late-answers` seeds 45, 1381
/// and 3498. 45: a takeover whose lease became usable only after the acquisition
/// (its PUT slow) never learned the delegation table's generations, so
/// the root executed ops under a live delegation as its own and refused
/// the delegate's stream for good (`Core::deleg_root_sync_due`). 1381: a
/// backup took the lease over 120 ms before its own forwarded create's
/// deadline; the create was at log seq 15 but the new holder's journal
/// was not durable yet, and the client heard `EIO` for a write that had
/// landed (`Core::settled_outcome`). 3498 (main too): the marker
/// watcher read a stream-ahead row stranded by a takeover, waiting for
/// its re-ship, as missing, where a read of it waits (`marker_watcher`).
#[test]
fn regression_long_delegated_late_answers_seeds_45_1381_3498() {
    for seed in [45, 1381, 3498] {
        run_seed(seed, long_delegated_late_answers_config()).unwrap_or_else(|e| {
            panic!(
                "long-delegated-late-answers seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=long-delegated-late-answers"
            )
        });
    }
}

/// `long-delegated-late-answers` seed 9287 (in doubt for a landed
/// write): the root's own create, executed by `d1`'s delegate, was in the
/// root's journal as an appended row of `d1`'s generation when a backup
/// took the root over; a deposition does not strand a generation's rows,
/// so the journal kept it while the successor's segment carrying the
/// create was applied. At the op's deadline the deposed node read "my
/// journal carries the outcome" as "not the log's", and the client heard
/// `EIO`. A `completed` row an applied segment wrote is the log's
/// (`Core::settled_outcome`).
#[test]
fn regression_long_delegated_late_answers_seed_9287() {
    run_seed(9_287, long_delegated_late_answers_config()).unwrap_or_else(|e| {
        panic!(
            "long-delegated-late-answers seed 9287: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=long-delegated-late-answers"
        )
    });
}

/// `locks-unlinked-delegated-dbackup-random` seed 5681 (mutual
/// exclusion): `d1`'s generation was outwaited while grants the root had
/// moved to it were still undelivered (the root and the delegate were
/// partitioned), and the step that ended it re-delegated the subtree.
/// The undelivered grants came back to the root's table only after that,
/// restamped: lapsed ones revived, on inodes the new generation served.
/// Later the new delegate granted one of those inodes to the same holder,
/// which a client then unlinked; the grant came to the root with the
/// unlink, and the root's stale record of that holder (expired by then,
/// but never dropped) kept it out. The delegate dropped its copy once the
/// root acknowledged the batch, and the root granted the inode to another
/// node inside the holder's section. Now the generation's end returns
/// undelivered grants as recorded, live ones only, before the
/// re-delegation takes them along (`Core::lock_on_generation_outwaited`),
/// and an expired record never keeps a grant out
/// (`LockTables::install_if_consistent`); either alone passes this seed.
#[test]
fn regression_locks_unlinked_delegated_dbackup_random_seed_5681() {
    run_seed(5_681, locks_unlinked_random_config(true)).unwrap_or_else(|e| {
        panic!(
            "locks-unlinked-delegated-dbackup-random seed 5681: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=locks-unlinked-delegated-dbackup-random"
        )
    });
}

/// `locks-unlinked-delegated-dcrash-pause` seed 558: the root holder was
/// paused while node 2 (its backup, `d1`'s delegate) took the lease and
/// crashed; woken
/// and deposed, the old root had appended a batch of generation 2 (7..9)
/// and a row of generation 1 (7) to its journal. The rebuild dropped them
/// but left the generations' log indexes at 9 and 7, so retaking the root
/// at epoch 3 it inherited the generations at those cursors and the
/// delegate's re-stream skipped the rows: an unlink of `d2/f1` took effect
/// in the log while the spec refused it there (`ENOENT`). Now a row that
/// leaves the journal unshipped takes the index back below it
/// (`local::retract_log_idx_tx` from `spec::strand_local_tx`), both in the
/// store and in the session's stream, and the cursor is 6.
#[test]
fn regression_locks_unlinked_delegated_dcrash_pause_seed_558() {
    run_seed(558, locks_unlinked_dcrash_pause_config()).unwrap_or_else(|e| {
        panic!(
            "locks-unlinked-delegated-dcrash-pause seed 558: {e}\n  replay with \
             AUTHORITY_SIM_CONFIG=locks-unlinked-delegated-dcrash-pause"
        )
    });
}

/// unlinked-exclusion-7455: the root outwaited and ended `d1`'s
/// generation while its delegate (node 2) was paused; awake, the delegate
/// answered the recall it found queued with grants that had lapsed during
/// the pause, and the root installed them restamped. One, node 1's on
/// `lk0`, later kept the next generation's leaving grant (node 2's,
/// exclusive) out at the unlink, and the root upgraded node 1's revived
/// copy: both exclusive. Now a recall answer after its generation ended
/// brings no grants back (`Core::lock_install_returned`). 7455 fails on
/// `c465b07` and passes on later main by another trajectory (the outwait
/// barriers move its timings); 1144, 1682 and 1945 take the late-answer
/// path here, and the test asserts that some seed does.
#[test]
fn regression_locks_unlinked_delegated_partition_late_recall_answer() {
    let mut late = 0;
    for seed in [7_455, 1_144, 1_682, 1_945] {
        let report = run_seed(seed, locks_unlinked_partition_config()).unwrap_or_else(|e| {
            panic!(
                "locks-unlinked-delegated-partition seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-unlinked-delegated-partition"
            )
        });
        late += report
            .stats
            .values()
            .map(|s| s.lock_returned_after_end)
            .sum::<u64>();
    }
    assert!(late > 0, "vacuous: no recall answer came after its end");
}

/// stale-read-outwaited: a locker cut off from the lock's owner is
/// outwaited while what it was acknowledged under the lock is in a
/// delegate's stream the root has not appended (here its own `d2`
/// stream), and the next holder read the older turn: the grant carried
/// only the owner's position. Now the outwait is a barrier the next grant
/// waits on until a cut of every live stream as of the grant record's end
/// is its floor (`Core::lock_barrier_ready`). 276 and 297: the root owns
/// the unlinked lock file; 935 and 2716 the same without delegate
/// backups; 68 and 81: a delegate owns it and takes the cut from its
/// root's renewal answers; 597: a delegate outwaits, the root's cut waits
/// for the cut-off delegate's generation to end; 4067: the delegate
/// outwaits, then the file is unlinked and the barrier goes to the root
/// with the unlink (`Core::deleg_leaving_barriers`). Each fails without
/// the barrier. 390 (review round): the outwaited holder itself was
/// granted shared again, which must not end its barrier — another node's
/// shared grant beside it read the older turn.
#[test]
fn regression_stale_read_after_an_outwait() {
    type Config = fn() -> SimConfig;
    let seeds: [(&str, Config, u64); 9] = [
        (
            "locks-unlinked-delegated-dbackup-random",
            || locks_unlinked_random_config(true),
            276,
        ),
        (
            "locks-unlinked-delegated-dbackup-random",
            || locks_unlinked_random_config(true),
            297,
        ),
        (
            "locks-unlinked-delegated-random",
            || locks_unlinked_random_config(false),
            935,
        ),
        (
            "locks-unlinked-delegated-random",
            || locks_unlinked_random_config(false),
            2_716,
        ),
        (
            "locks-delegated-partition",
            locks_delegated_partition_config,
            68,
        ),
        (
            "locks-delegated-partition",
            locks_delegated_partition_config,
            81,
        ),
        (
            "locks-unlinked-delegated-partition",
            locks_unlinked_partition_config,
            597,
        ),
        (
            "locks-unlinked-delegated-partition",
            locks_unlinked_partition_config,
            4_067,
        ),
        (
            "locks-unlinked-delegated-partition",
            locks_unlinked_partition_config,
            390,
        ),
    ];
    let (mut barriers, mut waits) = (0, 0);
    for (config, cfg, seed) in seeds {
        let report = run_seed(seed, cfg()).unwrap_or_else(|e| {
            panic!("{config} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={config}")
        });
        barriers += report
            .stats
            .values()
            .map(|s| s.lock_outwait_barriers)
            .sum::<u64>();
        waits += report
            .stats
            .values()
            .map(|s| s.lock_barrier_waits)
            .sum::<u64>();
    }
    assert!(
        barriers > 0 && waits > 0,
        "vacuous: {barriers} barriers, {waits} waits"
    );
}

/// `locks-unlinked-delegated` seeds 292 and 6475 (this chunk's first
/// cut of the move): a recall and re-delegation left the grant in the
/// root's handoff for the new generation, whose delegate executed the
/// unlink before its first renewal — nothing to carry, and the root
/// granted the unlinked inode over its own pending copy (now
/// `Core::lock_take_back_left`); and a recall answer handed back the
/// delegate's stale shared copy of a grant the root had upgraded to
/// exclusive since the unlink, over the root's record (now the leaving
/// path in `Core::lock_install_returned`).
#[test]
fn regression_locks_unlinked_delegated_seeds_292_6475() {
    for seed in [292, 6_475] {
        run_seed(seed, locks_unlinked_delegated_config()).unwrap_or_else(|e| {
            panic!(
                "locks-unlinked-delegated seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=locks-unlinked-delegated"
            )
        });
    }
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

/// `locks-faults` seeds 196041 and 204913 (fix "a dropped refusal is
/// tentative", PROGRESS.md; a simulation fix): a `Local` holder cut from
/// S3 (or crashed) past its lease had refused ops — its own client's and
/// forwarded ones — against effects it had acknowledged and not shipped.
/// Its rollback dropped those refusals (they are never replayed), as it
/// rolls back the effects, which the checks treat as tentative; the
/// refusals were not, and the generic tester could place them nowhere.
/// The witnessed check had passed: it checks each refusal's window alone.
#[test]
fn regression_locks_faults_dropped_refusals_are_tentative() {
    for seed in [196_041, 204_913] {
        run_seed(seed, locks_faults_config()).unwrap_or_else(|e| {
            panic!("locks-faults seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG=locks-faults")
        });
    }
}

/// `delegated-long-delegation` seeds 11, 86 and 200 (fix "a refusal whose rid
/// the log carries executed is tentative", PROGRESS.md; a simulation fix):
/// the delegate of `d1` is cut from the root past its delegation. A
/// requester forwarded an op to the root, which parked it behind the
/// generation's recall; the client's call went in doubt and its
/// resubmission reached the delegate, which refused it (`EEXIST`/`ENOENT`)
/// in a stream position the generation never carried. When the generation
/// ended the root ran the parked copy: the log carries the rid executed,
/// so the rolled-back refusal kept a `completed` row and was not tentative.
#[test]
fn regression_delegated_long_delegation_rolled_back_refusals_are_tentative() {
    for seed in [11, 86, 200] {
        run_seed(seed, delegated_long_delegation_config()).unwrap_or_else(|e| {
            panic!(
                "delegated-long-delegation seed {seed}: {e}\n  replay with \
                 AUTHORITY_SIM_CONFIG=delegated-long-delegation"
            )
        });
    }
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
    run_m14(
        "locks-failover-backup-renew",
        locks_failover_backup_renew_config(),
        203_000..203_000 + n,
    );
    run_m14(
        "fast-takeover-skewed-successor",
        fast_takeover_skewed_successor_config(),
        206_000..206_000 + n,
    );
    run_m14(
        "locks-long-grants",
        locks_long_grants_config(),
        207_000..207_000 + n,
    );
    run_m14(
        "locks-long-grants-seal",
        locks_long_grants_seal_config(),
        208_000..208_000 + n,
    );
    run_m14(
        "fast-takeover-skewed-successor-nobeats",
        fast_takeover_skewed_successor_nobeats_config(),
        211_000..211_000 + n,
    );
    run_m14(
        "fast-takeover-skewed-holder",
        fast_takeover_skewed_holder_config(),
        212_000..212_000 + n,
    );
    run_m14(
        "locks-s3-cut-holder-alive",
        locks_s3_cut_holder_alive_config(),
        213_000..213_000 + n,
    );
    // Plan 43 C7-H1 (3/4): wall steps on the wait side.
    run_m14(
        "locks-root-step-forward",
        locks_root_step_forward_config(),
        214_000..214_000 + n,
    );
    run_m14(
        "fast-takeover-holder-step-forward",
        fast_takeover_holder_step_forward_config(),
        215_000..215_000 + n,
    );
    run_m14(
        "fast-takeover-successor-step-forward",
        fast_takeover_successor_step_forward_config(),
        216_000..216_000 + n,
    );
    run_m14(
        "strict-isolated-reader-holder-step-forward",
        strict_isolated_reader_holder_step_forward_config(),
        217_000..217_000 + n,
    );
    // Plan 43 C7a (2/6): oscillators off by their budget.
    run_m14(
        "locks-root-rate-fast",
        locks_root_rate_fast_config(),
        218_000..218_000 + n,
    );
    run_m14(
        "fast-takeover-successor-rate-fast",
        fast_takeover_successor_rate_fast_config(),
        219_000..219_000 + n,
    );
    run_m14(
        "fast-takeover-holder-rate-fast",
        fast_takeover_holder_rate_fast_config(),
        220_000..220_000 + n,
    );
    run_m14(
        "locks-rate-within-budget",
        locks_rate_within_budget_config(),
        221_000..221_000 + n,
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
    // Plan 43 C7a (3/6): restart horizons read on the boot they were written on.
    run_m14(
        "locks-root-restart-step-forward",
        locks_root_restart_step_forward_config(),
        222_000..222_000 + n,
    );
    run_m14(
        "locks-root-step-forward-then-restart",
        locks_root_step_forward_then_restart_config(),
        223_000..223_000 + n,
    );
    run_m14(
        "locks-root-reboot-step-forward",
        locks_root_reboot_step_forward_config(),
        224_000..224_000 + n,
    );
    run_m14(
        "locks-root-reboot",
        locks_root_reboot_config(),
        225_000..225_000 + n,
    );
}

// ===================================================================
// Plan 43 C0: the commit-rule oracles (`sim/oracle.rs`) — fail-before
// evidence on today's code, positive controls, a planted violation, the
// sim's rebuild. The strict flags are off by default; a test that needs
// one turns it on with `strict` (the `+commit` … `+oracles` variants).

/// `cfg` with the strict flags the `+`-joined `variants` name.
fn strict(cfg: SimConfig, variants: &str) -> SimConfig {
    variants
        .split('+')
        .filter(|v| !v.is_empty())
        .fold(cfg, config_variant)
}

/// A seed that must pass, with the oracle plumbing checked: every answer
/// the sequencers gave was matched to its request.
fn c0_run(label: &str, seed: u64, cfg: SimConfig) -> Report {
    let r = run_seed(seed, cfg).unwrap_or_else(|e| {
        panic!("{label} seed {seed}: {e}\n  replay with AUTHORITY_SIM_CONFIG={label}")
    });
    assert_eq!(
        r.acks_unmapped, 0,
        "{label} seed {seed}: answers with no recorded request"
    );
    r
}

fn c0_count(map: &std::collections::BTreeMap<String, usize>, key: &str) -> usize {
    map.get(key).copied().unwrap_or(0)
}

/// The replay-seed names the strict variants attach to.
#[test]
fn c0_variants_set_their_flags() {
    let c = strict(SimConfig::default(), "commit");
    assert!(c.strict_commit && !c.strict_identity && !c.strict_settle && !c.strict_lone);
    let c = strict(SimConfig::default(), "identity+settle+lone");
    assert!(!c.strict_commit && c.strict_identity && c.strict_settle && c.strict_lone);
    let c = strict(SimConfig::default(), "oracles");
    assert!(c.strict_commit && c.strict_identity && c.strict_settle && c.strict_lone);
    assert!(strict(SimConfig::default(), "rebuild-stub").rebuild_stub);
}

/// Plan 43 C0 §10.3, positive control: where every answer waits for the
/// archive (`ack=s3`) the I1 oracle finds nothing — it does not cry wolf.
#[test]
fn c0_i1_is_clean_where_every_answer_waits_for_the_archive() {
    for seed in 940..960 {
        let r = c0_run("acks3", seed, strict(ack_s3_config(), "commit"));
        assert!(
            r.uncommitted_acks.is_empty(),
            "acks3 seed {seed}: {:?}",
            r.uncommitted_acks
        );
    }
    for seed in 900..915 {
        let r = c0_run("acks3-crash", seed, strict(ack_s3_crash_config(), "commit"));
        assert!(
            r.uncommitted_acks.is_empty(),
            "acks3-crash seed {seed}: {:?}",
            r.uncommitted_acks
        );
    }
}

/// Plan 43 C0 §10.3, positive control: under `backup` an answer the core
/// gave on a backup's acknowledgement is committed in fact — no
/// `root-group` or `deleg-group` answer is ever uncommitted. (What the
/// oracle does find, and what the baseline counts, are answers of a fresh
/// tenure that has no backup yet — a holder's first 200 ms, or after a
/// takeover — to *another node's* forward: a lone root's Layer-A answer,
/// plan 43 C2's `Archive` class, `uncommitted_acks["archive/…"]`.)
#[test]
fn c0_i1_finds_no_uncommitted_group_answer_under_backup() {
    let (mut seen, mut groups) = (0usize, 0usize);
    for seed in 700..760 {
        let r = c0_run("backup", seed, backup_config());
        groups += c0_count(&r.acks_by_class, "root-group");
        for (k, n) in &r.uncommitted_acks {
            assert!(
                k.starts_with("archive/"),
                "backup seed {seed}: an answer expected under {k} was uncommitted ({n}): {:?}",
                r.oracle_examples
            );
            seen += n;
        }
    }
    assert!(
        groups > 100,
        "the control never judged a root-group answer: {groups}"
    );
    println!("backup 700..760: {seen} archive-class answers uncommitted, {groups} root-group answers judged");
}

/// A single node answers its own clients: Lone (or Archive), never a
/// violation; every counter of every oracle is zero.
#[test]
fn c0_the_oracles_are_clean_on_one_node() {
    let cfg = || SimConfig {
        nodes: 1,
        clients_per_node: 2,
        ops_per_client: 10,
        random_faults: 0,
        ..SimConfig::default()
    };
    for seed in 0..20 {
        let r = c0_run("single", seed, strict(cfg(), "oracles"));
        assert!(
            r.uncommitted_acks.is_empty()
                && r.unjournaled_refusals.is_empty()
                && r.identity_copies == 0
                && r.non_verbatim == 0
                && r.late_landings == 0
                && r.non_lone_rollbacks == 0,
            "single seed {seed}: {r:?}"
        );
        assert!(
            c0_count(&r.acks_by_class, "lone") > 0,
            "single seed {seed}: no lone answer judged"
        );
    }
}

/// Plan 43 C0 §10.3: the F1 hold configurations (a write parked behind a
/// slow writer while the node's core steps; the oracles read other nodes'
/// stores from inside a driver) still pass (`-unchecked`, the non-vacuity
/// knob, fails by design and is left out) — no deadlock between the
/// oracle's snapshot reads and a held write's transaction.
#[test]
fn c0_the_oracles_do_not_deadlock_with_held_writes() {
    for (label, cfg) in [
        ("locks-writer-hold", locks_writer_hold_config()),
        (
            "locks-writer-hold-delegated",
            locks_writer_hold_delegated_config(),
        ),
        ("locks-delegated", locks_delegated_config()),
    ] {
        for seed in 0..3 {
            c0_run(label, seed, cfg.clone());
        }
    }
}

/// Plan 43 C0 §10.5, checker strength: the sim's fast path with
/// `Core::ack_gated` ignored answers a holder's own op from its local disk
/// while a backup is committed. The I1 oracle must see it (a `root-group`
/// answer no backup holds) and fail under `+commit`.
#[test]
fn c0_i1_catches_a_planted_ungated_fast_path() {
    let planted = || SimConfig {
        fast_path: sim::run::FastPath::Checked,
        plant_ungated_fast_path: true,
        ..backup_config()
    };
    let mut caught = 0usize;
    for seed in 700..712 {
        // (Not `strict_durability`: the planted holder is allowed to lose
        // what it acknowledged; only the oracle is under test.)
        let cfg = SimConfig {
            strict_durability: false,
            ..planted()
        };
        let r = c0_run("backup+planted", seed, cfg.clone());
        let n = c0_count(&r.uncommitted_acks, "root-group/ok");
        if n > 0 {
            caught += 1;
            let e = run_seed(seed, strict(cfg, "commit"))
                .expect_err("the planted answer must fail +commit");
            assert!(e.contains("I1 acked_implies_committed"), "seed {seed}: {e}");
        }
    }
    assert!(
        caught >= 6,
        "the planted ungated fast path was caught on only {caught} of 12 seeds"
    );
    // Without the plant the same seeds are clean of root-group violations.
    for seed in 700..712 {
        let cfg = SimConfig {
            fast_path: sim::run::FastPath::Checked,
            strict_durability: false,
            ..backup_config()
        };
        let r = c0_run("backup+fast-path", seed, cfg);
        assert_eq!(
            c0_count(&r.uncommitted_acks, "root-group/ok"),
            0,
            "seed {seed}"
        );
    }
}

/// Plan 43 C0 (C6's baseline): `CONSTELLATION_BACKUPS=2` on three LAN nodes
/// runs today's checks, and a second backup is listed at least once.
#[test]
fn backup_two_config_runs() {
    let mut two = 0usize;
    for seed in 700..716 {
        let r = c0_run("backup-two", seed, backup_two_config());
        two += usize::from(r.max_backups >= 2);
    }
    assert!(two > 0, "no run ever listed a second backup");
}

/// Plan 43 C0, the sim's rebuild: a deposed root whose journal rows were
/// not captured rebuilds its replica from the log. With the old stub (every
/// rebuild failed: "rebuilding the deposed replica from the log failed") the
/// recovery never finished; with the real rebuild it does, and every
/// existing check still holds. The seeds are the paused configurations of
/// `long-delegated-late-answers` that were fixed on main (9287, 2945).
#[test]
fn c0_the_real_rebuild_recovers_a_deposed_root() {
    let cfg = long_delegated_late_answers_config;
    for seed in REBUILD_SEEDS {
        let real = c0_run("long-delegated-late-answers", seed, cfg());
        assert!(
            real.rebuilds.0 > 0,
            "seed {seed}: no rebuild ran: {:?}",
            real.rebuilds
        );
        assert_eq!(real.rebuilds.1, 0, "seed {seed}: a rebuild failed");
        let stub = c0_run(
            "long-delegated-late-answers+rebuild-stub",
            seed,
            strict(cfg(), "rebuild-stub"),
        );
        assert!(
            stub.rebuilds.1 > 0,
            "seed {seed}: the stub never answered: {:?}",
            stub.rebuilds
        );
        assert_eq!(stub.rebuilds.0, 0);
        // The recovery the stub kept failing (every retry of the round
        // asks again) ends after the real rebuild's first success.
        assert!(
            stub.rebuilds.1 > 5 * real.rebuilds.0,
            "seed {seed}: the stub failed {} times, the real rebuild ran {}",
            stub.rebuilds.1,
            real.rebuilds.0
        );
    }
}

// ---- fail-before: each oracle finds today's states (plan 43 C0 §10.2) ----
// Each pinned seed names the configuration and the counter it was found by.

/// I1, `long-delegated`: a delegate with no backup acknowledges alone
/// (`uncommitted_acks["deleg-group/ok"] > 0`); `+commit` fails with the I1
/// text. (Not `delegated-backup`, which the plan names too: every delegate
/// there has a backup, and ×2000 seeds give no such answer — PROGRESS,
/// plan 43 C0, "Found, not fixed here".)
#[test]
fn c0_i1_finds_a_delegate_acknowledging_alone() {
    {
        let (label, cfg, seeds) = ("long-delegated", long_delegated_config(), I1_LONG_DELEGATED);
        for &seed in seeds {
            let r = c0_run(label, seed, cfg.clone());
            assert!(
                c0_count(&r.uncommitted_acks, "deleg-group/ok") > 0,
                "{label} seed {seed}: {:?}",
                r.uncommitted_acks
            );
            let e = run_seed(seed, strict(cfg.clone(), "commit")).expect_err("+commit must fail");
            assert!(
                e.contains("I1 acked_implies_committed"),
                "{label} seed {seed}: {e}"
            );
        }
    }
}

/// I1, `far_config` (`no_peer_in_budget_is_todays_behaviour`'s 1000..1020):
/// a lone root answers another node's forward (`archive/ok`).
#[test]
fn c0_i1_finds_lone_answers_to_remote_requesters() {
    let cfg = || SimConfig {
        faults: backup_crash_config().faults,
        ops_per_client: 8,
        random_faults: 0,
        ..far_config()
    };
    let mut found = 0;
    for seed in 1000..1004 {
        let r = c0_run("far", seed, cfg());
        found += c0_count(&r.uncommitted_acks, "archive/ok");
        let e = run_seed(seed, strict(cfg(), "commit")).expect_err("+commit must fail");
        assert!(
            e.contains("I1 acked_implies_committed"),
            "far seed {seed}: {e}"
        );
    }
    assert!(found > 0);
}

/// Plan 43 C7-F F-a0 (the ack side of the review's F1): a `Local` holder
/// whose reconfiguration CAS listing its candidate landed with its answer
/// lost (`backup-reconfig-doubt`) acknowledges nothing on its disk alone
/// once the candidate is cut off: every seed's candidate, listed in S3,
/// seals it and takes over. Without `durable_jseq`'s doubted backups 481
/// of seeds 0..500 (all of these but 17) answer under `root-group` or
/// `lone` with no class covering the answer (I1: the listed candidate's
/// tail lacks the row); none with them.
#[test]
fn a_reconfig_in_doubt_acknowledges_nothing_its_listed_candidate_lacks() {
    let mut sealed = 0usize;
    for seed in 0..20 {
        let r = c0_run(
            "backup-reconfig-doubt",
            seed,
            backup_reconfig_doubt_config(),
        );
        for (k, n) in &r.uncommitted_acks {
            assert!(
                k.starts_with("archive/"),
                "backup-reconfig-doubt seed {seed}: an answer expected under {k} was \
                 uncommitted ({n}): {:?}",
                r.oracle_examples
            );
        }
        sealed += usize::from(r.stats.values().any(|s| s.backup_takeovers > 0));
    }
    assert_eq!(sealed, 20, "the listed candidate did not always take over");
}

/// I2 (b): a rid queued on two nodes — a deposed holder's queue and the
/// requester's retry, or a successor's replay of stranded delegate rows.
#[test]
fn c0_i2_finds_a_rid_queued_twice() {
    for (label, cfg, seeds) in [
        ("backup-crash", backup_crash_config(), I2_BACKUP_CRASH),
        (
            "delegated-holder-cut",
            delegated_holder_cut_config(),
            I2_DELEGATED_HOLDER_CUT,
        ),
    ] {
        for &seed in seeds {
            let r = c0_run(label, seed, cfg.clone());
            assert!(
                r.identity_copies > 0,
                "{label} seed {seed}: {:?}",
                r.oracle_examples
            );
            let e =
                run_seed(seed, strict(cfg.clone(), "identity")).expect_err("+identity must fail");
            assert!(e.contains("I2"), "{label} seed {seed}: {e}");
        }
    }
}

/// I2 (c): a successor re-journals a re-shipped tail under its own epoch and
/// new journal rows, so the log does not carry what the client was told
/// verbatim (`backup_failover_reships_the_tail`'s configuration).
#[test]
fn c0_i2c_finds_a_re_shipped_tail() {
    for &seed in I2C_BACKUP_CRASH {
        let r = c0_run("backup-crash", seed, backup_crash_config());
        assert!(
            r.non_verbatim > 0,
            "backup-crash seed {seed}: {:?}",
            r.oracle_examples
        );
        let e = run_seed(seed, strict(backup_crash_config(), "identity")).expect_err("+identity");
        assert!(e.contains("I2c verbatim"), "seed {seed}: {e}");
    }
}

/// I4: an op answered in doubt lands after an op its node invoked later.
#[test]
fn c0_i4_finds_a_late_landing() {
    for &(label, cfg, seeds) in I4_SEEDS {
        for &seed in seeds {
            let r = c0_run(label, seed, cfg());
            assert!(
                r.late_landings > 0,
                "{label} seed {seed}: {:?}",
                r.oracle_examples
            );
            let e = run_seed(seed, strict(cfg(), "settle")).expect_err("+settle must fail");
            assert!(
                e.contains("I4 unacked_fails_never_lands_late"),
                "{label} seed {seed}: {e}"
            );
        }
    }
}

/// I5: under `backup` an acknowledged op is rolled back (the transient
/// shadow rollbacks `backup_no_acked_op_lost`'s comment describes) and its
/// answer was not Lone.
#[test]
fn c0_i5_finds_a_non_lone_rollback() {
    for &seed in I5_BACKUP {
        let r = c0_run("backup", seed, backup_config());
        assert!(
            r.non_lone_rollbacks > 0,
            "backup seed {seed}: {:?}",
            r.oracle_examples
        );
        let e = run_seed(seed, strict(backup_config(), "lone")).expect_err("+lone must fail");
        assert!(e.contains("I5 speculation_only_lone"), "seed {seed}: {e}");
    }
}

// Pinned seeds, each found by the sweep of its configuration with the counter
// the test asserts (plan 43 C0 §10.2).
/// `long-delegated` (no backup anywhere): `uncommitted_acks["deleg-group/ok"]`.
const I1_LONG_DELEGATED: &[u64] = &[0, 1, 2];
/// `backup-crash`: `identity_copies`.
const I2_BACKUP_CRASH: &[u64] = &[815, 858, 889, 892];
/// `delegated-holder-cut`: `identity_copies`.
/// (22, whose trajectory plan 43 C7-F F-a0's live-delegation mark shifted to
/// no copy, gave its place to 3: 3 copies, `+identity` fails with I2.)
const I2_DELEGATED_HOLDER_CUT: &[u64] = &[0, 3, 4];
/// `backup-crash`: `non_verbatim`.
const I2C_BACKUP_CRASH: &[u64] = &[800, 802, 803];
/// `backup`: `non_lone_rollbacks`.
const I5_BACKUP: &[u64] = &[703, 746, 819, 931];
/// The paused configurations of `long-delegated-late-answers` where the
/// sim's `RebuildReplica` fires (9287 and 2945 still do).
const REBUILD_SEEDS: [u64; 2] = [9287, 2945];
type I4Seeds = (&'static str, fn() -> SimConfig, &'static [u64]);
/// `long-delegated-late-answers`: `late_landings`.
const I4_SEEDS: &[I4Seeds] = &[(
    "long-delegated-late-answers",
    long_delegated_late_answers_config,
    // (6, whose trajectory plan 43 C7-F F-a0's live-delegation mark shifted
    // to no late landing, gave its place to 15: 2 late landings, `+settle`
    // fails with I4.)
    &[1, 8, 15],
)];
