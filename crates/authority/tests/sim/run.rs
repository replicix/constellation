//! One simulated run: N nodes on a current-thread runtime with paused
//! time, a seeded workload, a seeded fault plan, then quiescence and the
//! checks.

use super::bus::{Bus, StreamFaults};
use super::check;
use super::chunks::{ChunkWorld, CHUNK_SIZE};
use super::clock::Clock;
use super::history::dir_of;
use super::history::{check_linearizable, check_linearizable_witnessed, History, NsOp, NsRet};
use super::node::{read_lease, CommitRecord, NodeEnv, NodeHandle};
use super::store::{Bucket, Fault, OpKind, Rule, When};
use constellation_authority::{ClientReply, Config, NodeId, Stats};
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::ChunkHash;
use constellation_meta::{Meta, MutateOp, MutateOutcome, Rid};
use constellation_types::Code;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A fault the plan schedules at a simulated time.
#[derive(Debug, Clone)]
pub enum FaultKind {
    /// Kill the current lease holder; restart it after `restart_ms`
    /// (`None`: never) with its journal (`keep_journal`) or fresh.
    CrashHolder {
        restart_ms: Option<u64>,
        keep_journal: bool,
    },
    /// Plan 30 §M14: shut the current lease holder down gracefully
    /// (`Control::Shutdown`: flush, then *release* the lease, live lock
    /// grants and delegations or not), stop it, and restart it with its
    /// journal after `restart_ms`. The next holder takes over a released,
    /// unexpired lease: a lock grace on the whole namespace.
    ShutdownHolder { restart_ms: u64 },
    /// Plan 30 §M14: delegate directory `dir` (by name under the root)
    /// to the lowest live node that is not the lease holder, through the
    /// holder, retrying until it answers (up to `within_ms`).
    DelegateDir { dir: String, within_ms: u64 },
    CrashNode {
        node: NodeId,
        restart_ms: Option<u64>,
        keep_journal: bool,
    },
    /// Cut P2P between two nodes for `for_ms`.
    Partition { a: NodeId, b: NodeId, for_ms: u64 },
    /// Cut one node's path to S3 for `for_ms`.
    CutS3 { node: NodeId, for_ms: u64 },
    /// Stop a node for `for_ms` while its timers keep running.
    Pause { node: NodeId, for_ms: u64 },
    /// Plan 30 M0's fault knob: delay the holder's forward replies by
    /// `delay_ms` for `for_ms` (bug A's trigger).
    SlowReplies { delay_ms: u64, for_ms: u64 },
    /// A brand-new node (fresh id, no durable state, no clients) joins
    /// and bootstraps from the log: the "freshly bootstrapped replica" of
    /// M4's convergence check.
    JoinFresh,
    /// Scripted S3 error codes from now on (M4's `faulty.rs` idea).
    S3Rule(Rule),
    /// Plan 30 §M9: kill the holder's first listed backup (as the lease
    /// object names it); restart it after `restart_ms`.
    CrashBackup { restart_ms: Option<u64> },
    /// Plan 30 §M9: cut P2P between the holder and its first listed
    /// backup (S3 stays reachable to both) for `for_ms`.
    PartitionBackup { for_ms: u64 },
    /// Fix "capture under an epoch hold": once a continuation epoch is
    /// active, kill a member that is not the hold owner — preferably one
    /// whose epoch write's chunk the owner awaits (`chunks.rs`), so the
    /// owner's transaction stays deferred at the close. Restart it with
    /// its disk after `restart_ms` (its chunks then upload and the write
    /// ships), or never (`None`: the run's end applies the operator
    /// procedure, `repair drop-held --remote`).
    CrashEpochMember { restart_ms: Option<u64> },
    /// Plan 30 §M9: cut the current holder's path to S3 for `for_ms` (it
    /// keeps acknowledging through its backup; what it journals meanwhile
    /// is the backup's tail).
    CutS3Holder { for_ms: u64 },
    /// Plan 30 §M14: once a node other than the lease holder is inside a
    /// lock's critical section (waiting up to 5 s for one), cut it from
    /// every other node over P2P for `for_ms`: its renewals fail, its
    /// grant lapses under the section (I/O fenced) and the owner outwaits
    /// it before granting anyone else.
    PartitionLocker { for_ms: u64 },
    /// Plan 30 §M14: once a node other than the lease holder is inside a
    /// critical section, kill the lease holder (the grants' owner).
    CrashHolderWhileLocked { restart_ms: Option<u64> },
    /// Plan 30 §M14: kill a node other than the lease holder while it is
    /// inside a critical section (its grant is outwaited).
    CrashLocker { restart_ms: Option<u64> },
    /// Plan 30 §M10: an S3 outage for the current holder and `members −
    /// 1` other nodes (the lowest ids), which are also cut from every
    /// other node over P2P; the others keep S3. The sim's epoch
    /// coordinator (`epochs.rs`) forms a continuation epoch among the cut
    /// nodes when the flexible-quorum rule allows. Healed after `for_ms`.
    EpochOutage { members: usize, for_ms: u64 },
    /// Only the current holder loses S3 (P2P stays up) for `for_ms`; it
    /// proposes a continuation epoch to every peer, and each joins by the
    /// member rule (`decline`: a member that reaches S3 declines; false:
    /// the rule's absence, every live peer joins). See
    /// `epochs::holder_cut_epoch`.
    HolderCutEpoch { for_ms: u64, decline: bool },
}

#[derive(Debug, Clone)]
pub struct ScheduledFault {
    pub at_ms: u64,
    pub kind: FaultKind,
}

#[derive(Clone)]
pub struct SimConfig {
    pub nodes: u64,
    pub clients_per_node: u64,
    pub ops_per_client: u64,
    pub names: usize,
    pub s3_latency: (u64, u64),
    pub p2p_delay: (u64, u64),
    pub p2p_drop: f64,
    /// Random faults drawn from the seed, in addition to `faults`.
    pub random_faults: usize,
    pub faults: Vec<ScheduledFault>,
    /// Simulated time to wait for quiescence after the workload.
    pub settle_ms: u64,
    pub core: Arc<dyn Fn(NodeId, u32) -> Config + Send + Sync>,
    /// Test-only: node 1's driver panics after handling this many
    /// events, to pin that a panic fails the seed instead of hanging it.
    pub panic_after_events: Option<u64>,
    /// Plan 30 §M6: after each op, a client reads the name it touched
    /// with this probability (and a random name with a quarter of it).
    pub read_ratio: f64,
    /// Plan 30 §M6: reads go through the session wait
    /// (`Meta::session_ready`, polled), bounded by `session_wait_ms`.
    /// `false` is the non-vacuity knob: reads never wait.
    pub session_wait: bool,
    pub session_wait_ms: u64,
    /// Plan 30 §M7: faults on log-stream frames (loss, reorder, the
    /// holder dropping a subscriber).
    pub stream_faults: StreamFaults,
    /// Plan 30 §M8: reads are `cto=strict` (a delegation, or a ReadIndex
    /// through the core, then the wait at its position), and the
    /// close-to-open check is enforced.
    pub strict: bool,
    /// Plan 30 §M8: each node's clock is off by a seeded constant within
    /// `±clock_skew_ms` (leases and delegations are judged on it).
    pub clock_skew_ms: i64,
    /// Plan 30 §M9: the run's acknowledgements are durable (a backup
    /// within budget, or `ack=s3`): an acknowledged op is never
    /// tentative — its rollback is a failure, not an exemption — and a
    /// refusal explained only by a tentative effect is a failure.
    pub strict_durability: bool,
    /// Plan 30 §M9: pairs whose reported RTT is `ms` (out of budget when
    /// above it).
    pub rtts: Vec<((NodeId, NodeId), u64)>,
    /// Plan 30 §M10: the slack the sim's epoch coordinator forms epochs
    /// under (the cores' `Config::epoch_slack` must agree).
    pub epoch_slack: u32,
    /// Plan 30 §M11: directories under `/` created before the clients
    /// start; a client's names live in its node's home directory
    /// (`dirs[(node − 1) % dirs.len()]`, `""` when empty) with
    /// `cross_ratio` of its renames crossing into another one.
    pub dirs: Vec<String>,
    /// Plan 30 §M11: `(dir, node)` delegated by the initial holder
    /// (node 1) before the clients start.
    pub delegations: Vec<(String, NodeId)>,
    /// Plan 30 §M11 phase 2b: offline designations `(dir, designee)`,
    /// synced into the table by the root at setup.
    pub designations: Vec<(String, NodeId)>,
    /// Plan 30 §M12: `(dir, node, (bits, idx))` hash ranges delegated by
    /// the initial holder before the clients start.
    pub range_delegations: Vec<(String, NodeId, (u8, u32))>,
    /// Plan 30 §M12: every client writes in `dirs[0]` with names of its
    /// own (`<dir>/n<node>-f<i>`): a hot shared directory with no
    /// colliding names, the workload the hash-range split serves.
    pub shared_dir: bool,
    /// Plan 30 §M12: check linearizability per hash range of this many
    /// bits (0: per directory) — a split directory is sequenced per
    /// range, so its history is linearizable per range only.
    pub check_range_bits: u8,
    pub cross_ratio: f64,
    /// Plan 30 §M11: a writer per node writes data in its home
    /// directory then a marker in another; watchers on every node must
    /// never see a marker without its data (`marker_order`).
    pub marker_pairs: u64,
    /// Random faults may include `JoinFresh`. Off for the M10 configs: a
    /// node enrolled during an epoch is plan 30 §M10's documented gap
    /// (`flex_node_enrolled_during_an_epoch_is_the_known_gap`).
    pub join_fresh: bool,
    /// Fix "capture under an epoch hold": after a `Create`, with this
    /// probability the client writes the file — one fresh chunk, dirty on
    /// its node until its upload pass (`chunks.rs`).
    pub chunk_writes: f64,
    /// Chunk metered-own-rows: the metered upload hold, in ms
    /// (`ChunkWorld::hold_ms`; 0: none). A non-owner's write stays
    /// deferred on the sequencer while its chunk is held, across the
    /// other nodes' ops on the same names.
    pub upload_hold_ms: u64,
    /// M12 round 2: model the daemon's FUSE fast path — a node holding a
    /// usable lease executes its own client ops outside the core
    /// (`view::View::mutate_op_rebasable_with_rid`), asking
    /// `Meta::root_fast_path` first (`Checked`), or not at all
    /// (`Unchecked`: the daemon before round 2, which executed names a
    /// range's delegate owned behind its back — `chaos-soak-4`'s double
    /// winners). `Off`: every op goes through the core.
    pub fast_path: FastPath,
    /// Plan 30 §M14: files `lk<i>` (in `lock_dir`, else the root)
    /// created at setup; after each op a client takes a whole-file lock on
    /// one of them with probability `lock_ratio` (`lock_shared_ratio` of
    /// them shared, `lock_nonblocking_ratio` non-blocking), performs
    /// `lock_ios` I/O steps `lock_io_ms` apart and unlocks (see
    /// `locks.rs`). Off with `lock_files: 0`.
    pub lock_files: usize,
    pub lock_ratio: f64,
    pub lock_shared_ratio: f64,
    pub lock_nonblocking_ratio: f64,
    pub lock_ios: (u32, u32),
    /// Below twice the expiry margin, so a lapse between two I/O steps is
    /// seen before any other node can be granted.
    pub lock_io_ms: (u64, u64),
    pub lock_dir: Option<String>,
    /// overload-cascade-2: after a lock step, a client unlinks that lock
    /// file with this probability (`stress-ng`'s lock stressors unlink
    /// their files while other processes hold them open and locked).
    /// Every lock client keeps locking the inode the file had
    /// ([`super::locks::LockGhost::inos`]), as a process with the file
    /// open would; with `lock_dir` delegated, the grants on it move from
    /// the delegate to the root with the unlink.
    pub lock_unlink_ratio: f64,
    /// Plan 30 §M14's non-vacuity knob: clients perform I/O even when
    /// `fenced` says the grant lapsed (the checker must catch it).
    pub lock_ignore_fence: bool,
    /// Plan 30 §M14 (lock-to-unlock coherence): an exclusive holder
    /// writes a turn number into the lock file's mtime under its lock,
    /// and a later holder on another node, once its grant's session wait
    /// is over, must read that turn or a later one. Gaps fail the seed.
    pub lock_writes: bool,
    /// `lock_writes`: where the turn is written — a data file per lock
    /// file (`dk<i>`) in this directory (`""`: the root), a different
    /// owner's than the lock files' puts the write where only the grant's
    /// floor (the releaser's frontier) orders it before the next holder
    /// (git's refs under its `flock` turn file). `None`: the lock file.
    pub lock_data_dir: Option<String>,
    /// delegate-stream-acks: extra delay, drawn per message in this range
    /// (ms), on every `DelegateStreamAck` and `DelegRenewed` — a root
    /// whose core answers a delegate's batches and renewals late, past
    /// the delegate's request timeout (`stress-ng-fs-nodes`). `(0, 0)`:
    /// none.
    pub deleg_answer_delay: (u64, u64),
}

/// See `SimConfig::fast_path`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FastPath {
    #[default]
    Off,
    Checked,
    Unchecked,
}

/// Core tunables scaled down for simulation (seconds, not minutes).
pub fn sim_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = Config::defaults(node_id, incarnation);
    c.ttl_ms = 6_000;
    c.expiry_margin_ms = 500;
    c.idle_release_ms = 3_000;
    c.dwell_ms = 1_500;
    c.wanted_grace_ms = 1_500;
    c.handoff_pause_ms = 800;
    c.forward_timeout_ms = 400;
    c.forward_retries = 3;
    c.forward_backoff_ms = 150;
    c.acquire_deadline_ms = 12_000;
    c.acquire_retry_min_ms = 50;
    c.acquire_retry_max_ms = 1_000;
    c.handoff_request_timeout_ms = 2_000;
    c.sync_interval_ms = 150;
    c.idle_max_ms = 2_000;
    c.tail_width = 8;
    c.publish_every = 4;
    c.publish_idle_ms = 5_000;
    c.replay_drain_ms = 200;
    c.replay_lease_fallback_ms = 5_000;
    c.held_tail_staleness_ms = 2_000;
    c.atime_ship_max_delay_ms = 60_000;
    c.inbox_warm_max_ms = 1_000;
    c.inbox_cold_max_ms = 2_000;
    c.inbox_hot_ms = 20;
    c.inbox_recheck_ms = 300;
    c.inbox_deadline_ms = 8_000;
    c.inbox_p2p_grace_ms = 600;
    c.escalate_window_ms = 4_000;
    // (`escalate_ops` / `escalate_wait_ms`: the production defaults,
    // `Config::defaults`, one source of truth.)
    c.escalate_retry_ms = 800;
    c.stream_heartbeat_ms = 300;
    c.stream_timeout_ms = 1_000;
    c.stream_backstop_ms = 2_000;
    c.stream_ring_segments = 16;
    c.stream_buffer_segments = 32;
    c.stream_retry_min_ms = 150;
    c.stream_retry_max_ms = 2_000;
    // Plan 30 §M14: a lock grant lives 1.5 s (renewed every 750 ms); the
    // expiry margin (500 ms) stays below half of it.
    c.lock_ttl_ms = 1_500;
    c.lock_cache_idle_ms = 15_000;
    // Plan 30 §M11 phase 2b: the placement is opted into per configuration
    // (`placement_core_config`); the 2a configurations are what they were.
    c.placement = false;
    c
}

/// The M13 configuration: P2P off, every non-holder write through the
/// holder's S3 inbox, escalation on sustained demand.
pub fn inbox_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.p2p = false;
    c
}

/// Plan 30 §M9: the backup configuration. The bus's default RTT (twice
/// the max delay: 30 ms) is within the budget, so a LAN backup is chosen;
/// the seal/takeover, ack-timeout and heartbeat clocks are scaled like
/// the rest (the lease TTL is 6 s here).
pub fn backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.backup_rtt_budget_ms = 50;
    c.backup_takeover_ms = 1_000;
    c.backup_ack_timeout_ms = 600;
    c.backup_heartbeat_ms = 200;
    c.backup_stable_ms = 200;
    c.backup_reconfig_min_ms = 500;
    c
}

/// `chaos-soak-4` seed 42 (`wf293`): the backup configuration with a
/// longer pre-S3 stream hold-off, so a forward's reply (sent the moment
/// its row is backup-acknowledged) often overtakes the stream frame that
/// carries the rows before it (held for the hold-off): streamed
/// transactions then arrive under live shadows and hints.
pub fn backup_hot_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = backup_core_config(node_id, incarnation);
    c.stream_ahead_holdoff_ms = 30;
    c
}

/// The placement on over the backup configuration (the root and every
/// delegate stream ahead of S3), with the `backup-hot` hold-off.
pub fn placement_backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = placement_core_config(node_id, incarnation);
    let b = backup_hot_core_config(node_id, incarnation);
    c.backup_rtt_budget_ms = b.backup_rtt_budget_ms;
    c.backup_takeover_ms = b.backup_takeover_ms;
    c.backup_ack_timeout_ms = b.backup_ack_timeout_ms;
    c.backup_heartbeat_ms = b.backup_heartbeat_ms;
    c.backup_stable_ms = b.backup_stable_ms;
    c.backup_reconfig_min_ms = b.backup_reconfig_min_ms;
    c.stream_ahead_holdoff_ms = b.stream_ahead_holdoff_ms;
    c
}

/// Plan 30 §M11 phase 2b: the placement on, with thresholds a short
/// workload reaches (a 2 s window, 6 ops, a 1.5 s dwell, a 1 s
/// cool-down).
pub fn placement_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.placement = true;
    c.placement_window_ms = 2_000;
    c.placement_min_ops = 6;
    c.placement_dwell_ms = 1_500;
    c.placement_cooldown_ms = 1_000;
    c
}

/// Plan 30 §M11 phase 2b: delegates with backups (M9's backup
/// configuration: the bus RTT is in budget, so every delegate picks one).
pub fn deleg_backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    backup_core_config(node_id, incarnation)
}

/// Plan 30 §M9: `ack=s3` — no backups, every acknowledgement waits for
/// the segment, fast takeover on holder silence.
pub fn ack_s3_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = backup_core_config(node_id, incarnation);
    c.ack_s3 = true;
    c
}

/// Plan 30 §M10: flexible-quorum continuation epochs with `f = 1`
/// (the promise TTL a quarter of the 6 s lease; P2P requests for
/// promises answered within 400 ms).
pub fn flex_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.epoch_slack = 1;
    c.promise_ttl_ms = c.ttl_ms / 4;
    c.promise_wait_ms = 400;
    c.promise_watch_ms = 1_000;
    c
}

/// Plan 30 §M10 with M9's backups in budget: the claim rule decides
/// whether the epoch carries the holder's `Backup` lease (its backup a
/// member), and an epoch hold acknowledges locally.
pub fn flex_backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let b = backup_core_config(node_id, incarnation);
    let mut c = flex_core_config(node_id, incarnation);
    c.backup_rtt_budget_ms = b.backup_rtt_budget_ms;
    c.backup_takeover_ms = b.backup_takeover_ms;
    c.backup_ack_timeout_ms = b.backup_ack_timeout_ms;
    c.backup_heartbeat_ms = b.backup_heartbeat_ms;
    c.backup_stable_ms = b.backup_stable_ms;
    c.backup_reconfig_min_ms = b.backup_reconfig_min_ms;
    c
}

/// The same with the takeover's promise check off (the non-vacuity knob).
pub fn flex_unchecked_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = flex_core_config(node_id, incarnation);
    c.takeover_promise_check = false;
    c
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            clients_per_node: 2,
            ops_per_client: 6,
            names: 4,
            s3_latency: (5, 40),
            p2p_delay: (1, 15),
            p2p_drop: 0.0,
            random_faults: 2,
            faults: Vec::new(),
            settle_ms: 90_000,
            core: Arc::new(sim_core_config),
            panic_after_events: None,
            fast_path: FastPath::Off,
            read_ratio: 0.0,
            session_wait: true,
            session_wait_ms: 2_000,
            stream_faults: StreamFaults::default(),
            strict: false,
            clock_skew_ms: 0,
            strict_durability: false,
            rtts: Vec::new(),
            epoch_slack: 0,
            join_fresh: true,
            chunk_writes: 0.0,
            upload_hold_ms: 0,
            dirs: Vec::new(),
            delegations: Vec::new(),
            designations: Vec::new(),
            range_delegations: Vec::new(),
            shared_dir: false,
            check_range_bits: 0,
            cross_ratio: 0.0,
            marker_pairs: 0,
            lock_files: 0,
            lock_ratio: 0.0,
            lock_shared_ratio: 0.25,
            lock_nonblocking_ratio: 0.1,
            lock_ios: (2, 6),
            lock_io_ms: (5, 60),
            lock_dir: None,
            lock_unlink_ratio: 0.0,
            lock_ignore_fence: false,
            lock_writes: false,
            lock_data_dir: None,
            deleg_answer_delay: (0, 0),
        }
    }
}

#[derive(Debug, Default)]
pub struct Report {
    pub seed: u64,
    /// Plan 30 §M9: faults that exceeded the one-failure durability
    /// budget (`Cluster::note_takedown`); non-empty relaxes the
    /// strict-durability checks for the run.
    pub durability_budget_exceeded: Vec<String>,
    pub ops_invoked: usize,
    pub ops_returned: usize,
    pub tentative: usize,
    pub segments: usize,
    pub fenced: usize,
    pub commits: usize,
    pub s3_puts: u64,
    pub s3_gets: u64,
    pub p2p_sent: u64,
    pub converged_checked: bool,
    /// M12 round 2: ops the modelled FUSE fast path executed
    /// (`SimConfig::fast_path`).
    pub fast_path_executed: u64,
    /// The generic tester ran too (few tentative ops).
    pub stateright_checked: bool,
    /// Refusals explained only by an acked-then-rolled-back effect (L2).
    pub observed_tentative: usize,
    /// Plan 30 §M6: the per-node session checks.
    pub sessions: super::session::SessionReport,
    /// Plan 30 §M8: close-to-open (enforced under `strict`).
    pub cto: super::cto::CtoReport,
    pub stats: BTreeMap<NodeId, Stats>,
    pub faults: Vec<String>,
    pub simulated_ms: u64,
    /// Plan 30 §M9: acknowledged ops that were rolled back at some point
    /// (exempted as tentative unless `strict_durability`).
    pub acked_rolled_back: usize,
    /// Plan 30 §M9: per holder crash, simulated ms from the crash to the
    /// first mutation any node acknowledged afterwards.
    pub failover_ms: Vec<u64>,
    /// The longest a client's single submit waited (simulated ms).
    pub longest_submit_ms: u64,
    /// Plan 30 §M10: continuation epochs formed (and with a roster node
    /// missing), and the authority samples taken.
    pub epochs_formed: usize,
    pub epochs_missing_node: usize,
    /// Outages that found a member's previous epoch still open, so no
    /// epoch formed and that one went on (`epochs::try_form`), or, once
    /// its carrier had closed, the carrier's fresh epoch replaced it.
    pub epochs_continued: usize,
    /// Of those, the open epochs replaced by their closed carrier's fresh
    /// epoch (`epochs::try_form`, chunk epoch-liveness-gap).
    pub epochs_superseded: usize,
    pub authority_samples: u64,
    /// Plan 30 §M11: marker pairs checked and violations seen.
    pub marker_checks: u64,
    pub marker_violations: u64,
    /// Plan 30 §M11: per-directory checks run.
    pub dirs_checked: usize,
    /// `InDoubt` answers sent (every one checked against the log by
    /// `check::check_in_doubt_answers`).
    pub in_doubt_answers: usize,
    /// Plan 30 §M14: what the lock clients saw.
    pub locks: super::locks::LockCounters,
    /// Fix "capture under an epoch hold": the chunk model's counters
    /// (`chunks.rs`): writes, chunks uploaded, remote rows enrolled and
    /// acked, ship plans seen deferring, transactions dropped by the
    /// end-of-run operator procedure, and whether a member died for good.
    pub chunk_writes: u64,
    pub chunks_uploaded: u64,
    pub remote_enrolled: u64,
    pub remote_acked: u64,
    pub deferred_seen: u64,
    pub remote_dropped: u64,
    pub member_gone: bool,
    /// Chunk metered-own-rows: own chunks a round's pass left held, and
    /// those `Action::UploadAwaited` put up past the hold.
    pub held_back: u64,
    pub awaited_uploaded: u64,
}

pub struct Cluster {
    pub nodes: Mutex<BTreeMap<NodeId, Arc<NodeHandle>>>,
    pub env: NodeEnv,
    /// Plan 30 §M10: epochs formed (members), and single-authority
    /// violations seen by the sampler.
    pub epochs: Mutex<Vec<Vec<NodeId>>>,
    /// Outages that met a member's still-open previous epoch.
    pub epochs_continued: Mutex<usize>,
    /// Open epochs their closed carrier's fresh epoch replaced.
    pub epochs_superseded: Mutex<usize>,
    pub split_brains: Mutex<Vec<String>>,
    pub slack: u32,
    /// Plan 30 §M9: holder crashes (simulated ms) and, once known, the
    /// first acknowledgement after each.
    pub failovers: Mutex<Vec<(u64, Option<u64>)>>,
    /// Plan 30 §M9's failure budget: faults that took down the last live
    /// copy of acknowledged, unshipped rows for longer than a lease TTL
    /// (see `note_takedown`).
    pub budget_exceeded: Mutex<Vec<String>>,
    /// When each node taken down by a fault is back (simulated unix ms).
    pub down: Mutex<BTreeMap<NodeId, i64>>,
    /// What a backup that is back needs, past its silence window, to
    /// claim the lease: a lease read, a tail and the CAS (four S3 round
    /// trips at the configured worst latency).
    pub claim_ms: i64,
    /// Plan 30 §M14: the mutual-exclusion ghost and the lock files.
    pub locks: Arc<super::locks::LockGhost>,
    /// The longest a client's single submit waited for its answer
    /// (simulated ms).
    pub longest_submit_ms: std::sync::atomic::AtomicU64,
}

impl Cluster {
    pub fn get(&self, id: NodeId) -> Arc<NodeHandle> {
        self.nodes.lock().unwrap()[&id].clone()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.lock().unwrap().keys().copied().collect()
    }

    /// Plan 30 §M9: one backup makes an acknowledged row survive *one*
    /// failure — the holder's or the backup's. `node` is being taken down
    /// (a crash, or a pause) until `back` (simulated unix ms; `None`:
    /// for good). The rows it shares with its partner — the holder it
    /// backs (sealed or not), or the backups of the lease it holds — stay
    /// recoverable only if one of the copies is back before the current
    /// lease expires: after that anyone may claim it by TTL and start
    /// from a log without them (backup-crash seed 609417: the holder
    /// crashed, and its sealed backup — mid-takeover, the tail not yet
    /// re-shipped — was paused for 6.9 s against a 6 s TTL; 602011: the
    /// backup crashed before sealing, and came back after the dead
    /// holder's lease had lapsed and been claimed — 209 ms before it
    /// could seal; 701598: it sealed 28 ms before the lease lapsed and
    /// lost the claim race to a TTL takeover; 609380: the sealed successor re-applied the tail, then
    /// crashed before shipping it, until past its own lease). Such a
    /// fault is
    /// recorded, and the run's strict-durability checks then treat
    /// acknowledged ops like `Local`'s (a replay by rid may become a
    /// conflict copy); every other check still applies.
    pub async fn note_takedown(&self, node: NodeId, back: Option<i64>, at_ms: u64) {
        if !self.ids().contains(&node) {
            return;
        }
        let now = self.env.clock.now().0;
        let back = back.unwrap_or(i64::MAX);
        self.down.lock().unwrap().insert(node, back);
        let Some(lease) = read_lease(&self.env.bucket).await else {
            return;
        };
        let v = self.get(node).view();
        // The copies that can still put the rows in the log before the
        // lease lapses: the node the lease names (it ships them) and the
        // backups that could seal and take over from it. A deposed
        // holder's own journal only comes back as replays by rid, which
        // run after the next holder's own ops (and may conflict).
        let copies: Vec<NodeId> = if v.ack.backing_holder != 0 {
            if lease.holder == node {
                vec![node]
            } else {
                vec![v.ack.backing_holder, node]
            }
        } else if v.held_epoch.is_some()
            && v.journal_len > 0
            && (!v.ack.backups.is_empty() || v.stats.backup_takeovers > 0)
        {
            // A holder with unshipped rows: its backups have them too —
            // or, as the sealed successor that re-applied its
            // predecessor's tail and has no backup of its own yet, nobody
            // does (seed 609380: crashed before shipping it). (A holder
            // that acknowledged under `Local` because no peer was in
            // budget is not a budget matter: that is `Local`.)
            let mut c = v.ack.backups.clone();
            c.push(node);
            c
        } else {
            return;
        };
        if !copies.contains(&lease.holder) {
            return;
        }
        let ids = self.ids();
        let back_at = |id: NodeId| -> i64 {
            if !ids.contains(&id) {
                return i64::MAX;
            }
            let down = self.down.lock().unwrap().get(&id).copied();
            let n = self.get(id);
            match down {
                Some(b) if b > now => b,
                _ if !n.alive() => i64::MAX,
                _ => now,
            }
        };
        // One failure is within the budget: only a takedown while every
        // other copy is already down can exceed it.
        if copies.iter().any(|c| *c != node && back_at(*c) <= now) {
            return;
        }
        // A backup that is back must still hear nothing from the holder
        // for `backup_takeover_ms` before it may seal (its only evidence
        // of the holder's death); the lease's own holder re-adopts it.
        let seal_ms = (self.env.config)(node, 0).backup_takeover_ms as i64 + self.claim_ms;
        let first_back = copies
            .iter()
            .map(|c| {
                let b = back_at(*c);
                if *c == lease.holder {
                    b
                } else {
                    b.saturating_add(seal_ms)
                }
            })
            .min()
            .unwrap_or(now);
        // Nobody else may claim a `Backup` lease before its expiry plus
        // the backups' grace (`LeaseState::classify`).
        let claimable_by_others = if lease.ack_policy == constellation_store_s3::AckPolicy::Backup
            && !lease.backups.is_empty()
        {
            lease.expires_unix_ms
                + constellation_authority::core::backup_claim_grace_ms(&(self.env.config)(node, 0))
        } else {
            lease.expires_unix_ms
        };
        if first_back > claimable_by_others {
            self.budget_exceeded.lock().unwrap().push(format!(
                "t={at_ms} node {node} taken down; the copies {copies:?} of holder {}'s acknowledged rows are all down until {} ms past the point others may claim its lease",
                lease.holder,
                first_back.saturating_sub(claimable_by_others)
            ));
        }
    }

    pub fn restart(&self, id: NodeId, meta: Option<Arc<Meta>>) {
        let meta_kept = meta.is_some();
        // Plan 30 §M10: the epoch state is persisted with the journal.
        let epoch = match (&meta, self.nodes.lock().unwrap().get(&id)) {
            (Some(_), Some(old)) => old.shared.epoch.lock().unwrap().clone(),
            _ => Default::default(),
        };
        // The rids the crashed incarnation found rolled back stay tentative
        // (long-delegated seed 79689: a delegate's acknowledged create,
        // stranded by the root's recall and replayed as a conflict copy,
        // was checked as durable once its node had restarted — the new
        // incarnation's set was empty).
        let (tentative, refusals) = self
            .nodes
            .lock()
            .unwrap()
            .get(&id)
            .map(|old| {
                (
                    old.shared.tentative.lock().unwrap().clone(),
                    old.shared.journaled_refusals.lock().unwrap().clone(),
                )
            })
            .unwrap_or_default();
        let handle = NodeHandle::start_with(&self.env, id, meta, epoch);
        handle.shared.tentative.lock().unwrap().extend(tentative);
        // (Kept only with the journal: a fresh replica has none to drop.)
        if meta_kept {
            *handle.shared.journaled_refusals.lock().unwrap() = refusals;
        }
        self.nodes.lock().unwrap().insert(id, Arc::new(handle));
    }

    pub fn crash(&self, id: NodeId) -> Arc<Meta> {
        let node = self.get(id);
        node.crash(&self.env.bus);
        // Plan 30 §M14: the process's lock state dies with it.
        self.locks.drop_node(id, self.env.clock.elapsed_ms());
        super::locks::reset_node(&node.meta, &self.locks.inos(), node.clock.now().0);
        node.meta.clone()
    }

    /// A mutation was answered now — accepted or definitively refused,
    /// either of which only a sequencer can do — so the open failover,
    /// if any, ends. (M12 round 2, holder-cut seed 1515: with the shared
    /// parent hold a node's creates no longer serialize on the
    /// directory, the schedule shifted, and the only ops left after the
    /// crash were refusals on a four-name pool; measured on `Ok` alone,
    /// a 1.3 s failover read as 9 s — the next `Ok` was the restarted
    /// holder's.)
    fn note_answered(&self) {
        let now = self.env.clock.elapsed_ms();
        if let Some(f) = self.failovers.lock().unwrap().last_mut() {
            if f.1.is_none() {
                f.1 = Some(now.saturating_sub(f.0));
            }
        }
    }
}

fn ns_ret(outcome: &MutateOutcome) -> Result<NsRet, String> {
    match outcome {
        MutateOutcome::Accepted { .. } => Ok(NsRet::Ok),
        MutateOutcome::Exists { .. } => Ok(NsRet::Eexist),
        MutateOutcome::Errno(Code::Exists) => Ok(NsRet::Eexist),
        MutateOutcome::Errno(Code::NotFound) => Ok(NsRet::Enoent),
        MutateOutcome::Errno(Code::ReadOnly | Code::CrossDevice) => Ok(NsRet::Erofs),
        other => Err(format!("unexpected client outcome {other:?}")),
    }
}

/// Plan 30 §M11: `"d1/x"` is name `x` in directory `d1` (created under
/// the root before the clients start); a bare name is in the root.
pub fn split_name(meta: &Meta, name: &str) -> (u64, String) {
    match name.rfind('/') {
        // `"/x"`: a name in the root directory written with its (empty)
        // directory — the marker workload's home or other directory is
        // the root's `""`. Looking `""` up trips `keys::dentry`'s debug
        // assertion (five sim tests panicked in debug builds only).
        Some(0) => (ROOT_INO, name[1..].to_string()),
        Some(i) => {
            let dir = &name[..i];
            let parent = meta
                .child_ino(ROOT_INO, dir)
                .ok()
                .flatten()
                .unwrap_or(ROOT_INO);
            (parent, name[i + 1..].to_string())
        }
        None => (ROOT_INO, name.to_string()),
    }
}

fn mutate_op(meta: &Meta, op: &NsOp) -> MutateOp {
    match op {
        NsOp::Create(name) => {
            let (parent, name) = split_name(meta, name);
            MutateOp::Create {
                parent,
                name,
                ino: meta.allocate_ino(parent).expect("ino"),
                mode: 0o644,
                uid: 0,
                gid: 0,
            }
        }
        NsOp::Unlink(name) => {
            let (parent, name) = split_name(meta, name);
            MutateOp::Unlink { parent, name }
        }
        NsOp::Rename(a, b) => {
            let (parent, name) = split_name(meta, a);
            let (new_parent, new_name) = split_name(meta, b);
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
                noreplace: false,
            }
        }
        NsOp::Put(_) => unreachable!("Put is a projection-only op"),
    }
}

/// How many times a client resubmits an in-doubt op (same rid) before
/// leaving it in flight.
const MAX_RESUBMITS: u32 = 6;

/// The first panic of the current seed (any task: a node's core, a
/// client, a fault), recorded by the hook `run_seed` installs. A
/// panicking node task used to leave the run waiting for a reply or a
/// quiescence that never came (M5 round 5, long seed 11932: the test
/// sat at 0 % CPU); now the run ends with the panic as its failure.
///
/// Keyed by thread: every task of a run executes on the run's own
/// thread (a current-thread runtime under `block_on`), and seeds run in
/// parallel test threads, so one seed's panic must not fail another.
static PANICKED: Mutex<Vec<(std::thread::ThreadId, String)>> = Mutex::new(Vec::new());
static PANIC_HOOK: std::sync::Once = std::sync::Once::new();

fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let me = std::thread::current().id();
            let mut first = PANICKED.lock().unwrap();
            if !first.iter().any(|(t, _)| *t == me) {
                first.push((me, info.to_string()));
            }
            drop(first);
            previous(info);
        }));
    });
}

/// Resolves with the panic message once any task of this run panicked.
async fn panic_watch() -> String {
    let me = std::thread::current().id();
    loop {
        let hit = PANICKED
            .lock()
            .unwrap()
            .iter()
            .find(|(t, _)| *t == me)
            .map(|(_, m)| m.clone());
        if let Some(msg) = hit {
            return msg;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Stateright's `LinearizabilityTester` runs on a history only while it
/// has at most this many events and this many tentative ops (each a
/// synthetic thread in flight forever): its search is exponential in
/// overlapping ops, and a 72-op crash-heavy history did not finish in
/// minutes. The witnessed check is exact for this spec and always runs.
const STATERIGHT_EVENT_BOUND: usize = 80;
const STATERIGHT_TENTATIVE_BOUND: usize = 6;

/// One step of a simulated client.
#[derive(Clone, Debug)]
pub enum Step {
    Op(NsOp),
    /// Plan 30 §M6: look a name up in the local replica.
    Read(String),
    /// Fix "capture under an epoch hold": write the file (a manifest
    /// naming one fresh chunk, dirty on this node).
    Write(String),
    /// Plan 30 §M14: a lock, a critical section, the unlock.
    Lock(super::locks::LockStep),
}

/// Plan 30 §M6: a client read — the session wait (polled: this runtime
/// is single-threaded, so the blocking `Meta::session_wait` would stall
/// it), then the lookup.
async fn client_read(
    handle: &NodeHandle,
    history: &History,
    thread: u64,
    name: String,
    wait: Option<u64>,
    strict: bool,
) {
    let inv = history.tick();
    let (parent, leaf) = split_name(&handle.meta, &name);
    let keys = [constellation_meta::ReadKey::Dentry(parent, leaf.clone())];
    let mut timed_out = false;
    // Plan 30 §M8: a strict lookup — local under a delegation on the
    // directory, else a ReadIndex through the core (answered `Holder` on
    // the sequencer) — then the session wait at the answer's position.
    let mut floor = constellation_meta::Position::ZERO;
    if strict {
        let now = handle.clock.now().0;
        let ask = |handle: &NodeHandle| {
            handle.control(constellation_authority::Control::ReadIndex {
                ino: parent,
                dir: true,
                name: Some(leaf.clone()),
            })
        };
        match handle.meta.read_delegations().valid(parent, now) {
            Some((held, renew)) => {
                floor = held.position;
                if renew {
                    drop(ask(handle));
                }
            }
            None => match ask(handle).await {
                Ok(Ok(constellation_authority::ControlOk::ReadIndex(
                    constellation_authority::ReadAnswer::Position { position, .. },
                ))) => floor = position,
                Ok(Ok(constellation_authority::ControlOk::ReadIndex(
                    constellation_authority::ReadAnswer::Degraded,
                )))
                | Ok(Err(_))
                | Err(_) => timed_out = true,
                // The sequencer's own replica, or tailed to head.
                Ok(Ok(_)) => {}
            },
        }
    }
    if let Some(budget) = wait {
        let mut waited = 0;
        while !handle.meta.session_ready_at(&keys, &floor) {
            if waited >= budget || !handle.alive() {
                timed_out = waited >= budget;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            waited += 5;
        }
    }
    if !handle.alive() {
        return;
    }
    let present = constellation_meta::MetaStore::lookup(&*handle.meta, parent, &leaf)
        .ok()
        .flatten()
        .is_some();
    tracing::debug!(
        node = handle.id,
        now = handle.clock.now().0,
        name,
        present,
        timed_out,
        ?floor,
        held = handle.meta.read_delegations().valid(parent, handle.clock.now().0).is_some(),
        replays = handle.meta.pending_replays().map(|q| q.len()).unwrap_or(0),
        speculation = ?handle.meta.speculation_counts().ok(),
        "client read"
    );
    history.read(super::history::ReadEvt {
        node: handle.id,
        incarnation: handle
            .shared
            .incarnation
            .load(std::sync::atomic::Ordering::SeqCst),
        thread,
        name,
        present,
        inv,
        ret: history.tick(),
        timed_out,
    });
}

#[allow(clippy::too_many_arguments)]
async fn client_thread(
    cluster: Arc<Cluster>,
    history: Arc<History>,
    node: NodeId,
    thread: u64,
    steps: Vec<Step>,
    abandoned: Arc<Mutex<HashSet<Rid>>>,
    failures: Arc<Mutex<Vec<String>>>,
    pace_ms: u64,
    wait: Option<u64>,
    strict: bool,
    ignore_fence: bool,
) {
    for step in steps {
        let (op, write) = match step {
            Step::Op(op) => (Some(op), None),
            Step::Write(name) => (None, Some(name)),
            Step::Read(name) => {
                let handle = cluster.get(node);
                if handle.alive() {
                    client_read(&handle, &history, thread, name, wait, strict).await;
                }
                continue;
            }
            Step::Lock(lock) => {
                tokio::time::sleep(Duration::from_millis(pace_ms / 2)).await;
                super::locks::client_lock(
                    cluster.clone(),
                    node,
                    thread,
                    lock,
                    wait.unwrap_or(2_000),
                    ignore_fence,
                    failures.clone(),
                )
                .await;
                continue;
            }
        };
        tokio::time::sleep(Duration::from_millis(pace_ms)).await;
        let handle = cluster.get(node);
        if !handle.alive() {
            // Wait for a restart before issuing anything new.
            let mut waited = 0;
            while !cluster.get(node).alive() && waited < 60_000 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                waited += 100;
            }
            if !cluster.get(node).alive() {
                return;
            }
        }
        let handle = cluster.get(node);
        let rid = handle.next_rid();
        let mop = match (&op, &write) {
            (Some(op), _) => mutate_op(&handle.meta, op),
            (None, Some(name)) => match write_op(&cluster, node, rid, name) {
                Some(mop) => mop,
                // The name is not here (unlinked, or its create refused).
                None => continue,
            },
            (None, None) => unreachable!(),
        };
        if let Some(op) = &op {
            history.invoke(thread, rid, op.clone());
        }
        let mut attempts = 0u32;
        loop {
            let handle = cluster.get(node);
            super::node::note_waiting(
                &format!("client t{thread}"),
                format!("submit rid {:?} attempt {attempts}", rid),
            );
            if !handle.alive()
                || handle
                    .shared
                    .incarnation
                    .load(std::sync::atomic::Ordering::SeqCst)
                    != rid.incarnation
            {
                // The process died with the call in flight: the op is
                // neither confirmed nor denied to anyone, ever.
                abandoned.lock().unwrap().insert(rid);
                break;
            }
            let submitted = cluster.env.clock.elapsed_ms();
            let answer = handle.submit(rid, mop.clone()).await;
            cluster.longest_submit_ms.fetch_max(
                cluster.env.clock.elapsed_ms().saturating_sub(submitted),
                std::sync::atomic::Ordering::Relaxed,
            );
            super::node::note_waiting(&format!("client t{thread}"), "answered".into());
            match answer {
                // Plan 30 §M10: a frozen continuation epoch refuses writes
                // with `EROFS` before executing them; the FUSE caller
                // retries (here: the same rid, like an in-doubt answer).
                Ok(ClientReply::Outcome(MutateOutcome::Errno(Code::ReadOnly))) => {
                    attempts += 1;
                    if attempts > MAX_RESUBMITS {
                        abandoned.lock().unwrap().insert(rid);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                // A write's outcome is not part of the namespace history
                // (its effect is checked by convergence and the chunk
                // rule); any definitive answer ends it.
                Ok(ClientReply::Outcome(_)) if op.is_none() => {
                    cluster.note_answered();
                }
                Ok(ClientReply::Outcome(outcome)) => match ns_ret(&outcome) {
                    Ok(ret) => {
                        cluster.note_answered();
                        // `AUTHORITY_SIM_TRACE_OPS=1`: every client return
                        // with its simulated time (M12 round 2: the
                        // failover measurement ends at the next `Ok`).
                        if std::env::var_os("AUTHORITY_SIM_TRACE_OPS").is_some() {
                            eprintln!(
                                "t={} client t{thread} rid({},{},{}) {ret:?} attempts={attempts}",
                                cluster.env.clock.elapsed_ms(),
                                rid.node,
                                rid.incarnation,
                                rid.seq
                            );
                        }
                        history.ret(thread, rid, ret);
                    }
                    Err(e) => failures.lock().unwrap().push(e),
                },
                Ok(ClientReply::InDoubt) | Err(_) => {
                    attempts += 1;
                    if attempts > MAX_RESUBMITS {
                        abandoned.lock().unwrap().insert(rid);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            break;
        }
    }
}

/// Plan 30 §M11: ops on `home`'s names (`home/f<i>`), with `cross_ratio`
/// of the renames moving a name into another directory of `dirs`.
fn gen_ops_in(
    rng: &mut StdRng,
    n: u64,
    names: usize,
    home: &str,
    dirs: &[String],
    cross_ratio: f64,
) -> Vec<NsOp> {
    let prefix = |d: &str, i: usize| {
        if d.is_empty() {
            format!("f{i}")
        } else {
            format!("{d}/f{i}")
        }
    };
    let pool: Vec<String> = (0..names).map(|i| prefix(home, i)).collect();
    (0..n)
        .map(|_| match rng.random_range(0..10) {
            0..=4 => NsOp::Create(pool.choose(rng).unwrap().clone()),
            5..=7 => NsOp::Unlink(pool.choose(rng).unwrap().clone()),
            _ => {
                let a = pool.choose(rng).unwrap().clone();
                let others: Vec<&String> = dirs.iter().filter(|d| d.as_str() != home).collect();
                if !others.is_empty() && rng.random_bool(cross_ratio.min(1.0)) {
                    let d = others.choose(rng).unwrap();
                    let b = prefix(d, rng.random_range(0..names));
                    return NsOp::Rename(a, b);
                }
                let mut b = pool.choose(rng).unwrap().clone();
                if b == a {
                    b = pool[(pool.iter().position(|x| *x == a).unwrap() + 1) % pool.len()].clone();
                }
                NsOp::Rename(a, b)
            }
        })
        .collect()
}

/// Plan 30 §M12: ops on `home`'s names of node `node`'s own
/// (`home/n<node>-f<i>`): creates, unlinks and renames among them — no
/// two nodes touch a name, the whole directory is shared.
fn gen_ops_unique(
    rng: &mut StdRng,
    n: u64,
    names: usize,
    home: &str,
    node: NodeId,
    range: (u8, u32),
) -> Vec<NsOp> {
    use constellation_meta::delegation::Range;
    let prefix = |i: usize| {
        if home.is_empty() {
            format!("n{node}-f{i}")
        } else {
            format!("{home}/n{node}-f{i}")
        }
    };
    // M12 round 2: each node's names have locality — all but one of
    // them hash into the node's own range (`bits`, `idx`), the way a
    // writer with a naming scheme of its own does; the placement splits
    // only ranges a node dominates. The one name outside keeps the
    // cross-range renames.
    let (bits, idx) = range;
    let mut pool: Vec<String> = Vec::new();
    let mut i = 0usize;
    while pool.len() + 1 < names.max(2) {
        let name = format!("n{node}-f{i}");
        i += 1;
        if Range::of(bits, &name).idx == idx {
            pool.push(prefix(i - 1));
        }
    }
    loop {
        let name = format!("n{node}-f{i}");
        i += 1;
        if Range::of(bits, &name).idx != idx {
            pool.push(prefix(i - 1));
            break;
        }
    }
    (0..n)
        .map(|_| match rng.random_range(0..10) {
            0..=5 => NsOp::Create(pool.choose(rng).unwrap().clone()),
            6..=8 => NsOp::Unlink(pool.choose(rng).unwrap().clone()),
            _ => {
                let a = pool.choose(rng).unwrap().clone();
                let mut b = pool.choose(rng).unwrap().clone();
                if b == a {
                    b = pool[(pool.iter().position(|x| *x == a).unwrap() + 1) % pool.len()].clone();
                }
                NsOp::Rename(a, b)
            }
        })
        .collect()
}

/// Plan 30 §M6: interleave reads with `ops`.
fn with_reads(rng: &mut StdRng, ops: Vec<NsOp>, names: usize, ratio: f64) -> Vec<Step> {
    let mut out = Vec::new();
    for op in ops {
        let touched = match &op {
            NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => n.clone(),
            NsOp::Rename(a, b) => {
                if rng.random_bool(0.5) {
                    a.clone()
                } else {
                    b.clone()
                }
            }
        };
        out.push(Step::Op(op));
        if ratio > 0.0 && rng.random_bool(ratio.min(1.0)) {
            out.push(Step::Read(touched.clone()));
        }
        if ratio > 0.0 && rng.random_bool((ratio / 4.0).min(1.0)) {
            let random = format!("f{}", rng.random_range(0..names));
            let random = match dir_of(&touched) {
                "" => random,
                d => format!("{d}/{random}"),
            };
            out.push(Step::Read(random));
        }
    }
    out
}

/// Fix "capture under an epoch hold": after each `Create`, with `ratio`,
/// a write of the file (`Step::Write`). Drawn from its own generator, so
/// a config without writes replays exactly the schedules it had.
fn with_writes(rng: &mut StdRng, steps: Vec<Step>, ratio: f64) -> Vec<Step> {
    if ratio <= 0.0 {
        return steps;
    }
    let mut out = Vec::new();
    for step in steps {
        let created = match &step {
            Step::Op(NsOp::Create(n)) => Some(n.clone()),
            _ => None,
        };
        out.push(step);
        if let Some(name) = created {
            if rng.random_bool(ratio.min(1.0)) {
                out.push(Step::Write(name));
            }
        }
    }
    out
}

/// The op behind a `Step::Write`: the file's manifest naming one fresh
/// chunk, dirty on `node` (the world's dirty set plus the node's own
/// `pending_upload` row, as the FUSE close leaves them). `None` when the
/// name is not in the local replica.
fn write_op(cluster: &Cluster, node: NodeId, rid: Rid, name: &str) -> Option<MutateOp> {
    let handle = cluster.get(node);
    let (parent, leaf) = split_name(&handle.meta, name);
    let ino = handle.meta.child_ino(parent, &leaf).ok().flatten()?;
    let hash = ChunkHash::of(
        format!(
            "chunk:{}:{}:{}:{}",
            cluster.env.seed, rid.node, rid.incarnation, rid.seq
        )
        .as_bytes(),
    );
    cluster.env.world.write(node, hash);
    handle.meta.add_pending_upload(&hash, ino).ok()?;
    Some(MutateOp::SetManifest {
        ino,
        base_manifest: None,
        manifest: ChunkWorld::manifest(hash),
        size: CHUNK_SIZE as u64,
    })
}

/// Plan 30 §M14: interleave lock steps with `steps` (after each op, with
/// `lock_ratio`); a config without locks draws nothing.
fn with_locks(rng: &mut StdRng, steps: Vec<Step>, cfg: &SimConfig) -> Vec<Step> {
    if cfg.lock_files == 0 || cfg.lock_ratio <= 0.0 {
        return steps;
    }
    let mut out = Vec::new();
    for step in steps {
        let op = matches!(step, Step::Op(_));
        out.push(step);
        if op && rng.random_bool(cfg.lock_ratio.min(1.0)) {
            let mode = if rng.random_bool(cfg.lock_shared_ratio.clamp(0.0, 1.0)) {
                constellation_meta::locks::LockMode::Shared
            } else {
                constellation_meta::locks::LockMode::Exclusive
            };
            let file = rng.random_range(0..cfg.lock_files);
            out.push(Step::Lock(super::locks::LockStep {
                file,
                mode,
                blocking: !rng.random_bool(cfg.lock_nonblocking_ratio.clamp(0.0, 1.0)),
                ios: rng.random_range(cfg.lock_ios.0..=cfg.lock_ios.1),
                io_ms: rng.random_range(cfg.lock_io_ms.0..=cfg.lock_io_ms.1),
                write: cfg.lock_writes && mode == constellation_meta::locks::LockMode::Exclusive,
            }));
            if cfg.lock_unlink_ratio > 0.0 && rng.random_bool(cfg.lock_unlink_ratio.min(1.0)) {
                out.push(Step::Op(NsOp::Unlink(lock_file_name(cfg, file))));
            }
        }
    }
    out
}

fn gen_faults(rng: &mut StdRng, cfg: &SimConfig, horizon_ms: u64) -> Vec<ScheduledFault> {
    let mut out = Vec::new();
    for _ in 0..cfg.random_faults {
        let at_ms = rng.random_range(500..horizon_ms.max(600));
        let node = rng.random_range(1..=cfg.nodes);
        let other = if cfg.nodes > 1 {
            let mut o = rng.random_range(1..=cfg.nodes);
            while o == node {
                o = rng.random_range(1..=cfg.nodes);
            }
            o
        } else {
            node
        };
        let kind = match rng.random_range(0..8) {
            // A restart always keeps its state dir: a node whose journal
            // is gone comes back under a new id (`JoinFresh`), never as
            // the same node with an empty journal — which production
            // refuses as "state dir reuse or id collision".
            0 | 1 => FaultKind::CrashHolder {
                restart_ms: Some(rng.random_range(1_000..8_000)),
                keep_journal: true,
            },
            2 => {
                if rng.random_bool(0.3) && cfg.join_fresh {
                    FaultKind::JoinFresh
                } else {
                    FaultKind::CrashNode {
                        node,
                        restart_ms: Some(rng.random_range(1_000..8_000)),
                        keep_journal: true,
                    }
                }
            }
            3 => FaultKind::Partition {
                a: node,
                b: other,
                for_ms: rng.random_range(500..6_000),
            },
            4 => FaultKind::CutS3 {
                node,
                for_ms: rng.random_range(300..3_000),
            },
            5 => FaultKind::Pause {
                node,
                for_ms: rng.random_range(300..7_000),
            },
            6 => FaultKind::SlowReplies {
                delay_ms: rng.random_range(500..1_500),
                for_ms: rng.random_range(1_000..5_000),
            },
            _ => {
                let fault = *[
                    Fault::Status(500),
                    Fault::Timeout,
                    Fault::AppliedThen(500),
                    Fault::AppliedThenTimeout,
                    Fault::Status(409),
                ]
                .choose(rng)
                .unwrap();
                let (op, pattern) = *[
                    (OpKind::Put, "log/"),
                    (OpKind::Put, "leases/"),
                    (OpKind::Get, "log/"),
                    (OpKind::Get, "leases/"),
                ]
                .choose(rng)
                .unwrap();
                FaultKind::S3Rule(Rule::new(
                    None,
                    op,
                    pattern,
                    When::Random(rng.random_range(0.02..0.25)),
                    fault,
                ))
            }
        };
        out.push(ScheduledFault { at_ms, kind });
    }
    out
}

async fn run_fault(cluster: Arc<Cluster>, fault: ScheduledFault, log: Arc<Mutex<Vec<String>>>) {
    tokio::time::sleep(Duration::from_millis(fault.at_ms)).await;
    let note = |s: String| log.lock().unwrap().push(s);
    match fault.kind {
        FaultKind::ShutdownHolder { restart_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} shutdown-holder: no lease yet", fault.at_ms));
                return;
            };
            let holder = lease.holder;
            if holder == 0 || !cluster.ids().contains(&holder) || !cluster.get(holder).alive() {
                note(format!(
                    "t={} shutdown-holder: holder {holder} not alive",
                    fault.at_ms
                ));
                return;
            }
            let h = cluster.get(holder);
            let done = h.control(constellation_authority::Control::Shutdown);
            let released = matches!(
                tokio::time::timeout(Duration::from_secs(10), done).await,
                Ok(Ok(Ok(_)))
            );
            note(format!(
                "t={} shut down holder {holder} (released {released}; restart after {restart_ms}ms)",
                cluster.env.clock.elapsed_ms()
            ));
            if !h.alive() {
                return;
            }
            let now = cluster.env.clock.now().0;
            cluster
                .note_takedown(holder, Some(now + restart_ms as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(holder);
            tokio::time::sleep(Duration::from_millis(restart_ms)).await;
            cluster.restart(holder, Some(meta));
        }
        FaultKind::DelegateDir { dir, within_ms } => {
            let t0 = cluster.env.clock.elapsed_ms();
            while cluster.env.clock.elapsed_ms() < t0 + within_ms {
                let holder = read_lease(&cluster.env.bucket)
                    .await
                    .map(|l| l.holder)
                    .filter(|h| *h != 0 && cluster.ids().contains(h) && cluster.get(*h).alive());
                if let Some(holder) = holder {
                    let h = cluster.get(holder);
                    let to = cluster
                        .ids()
                        .into_iter()
                        .find(|n| *n != holder && cluster.get(*n).alive());
                    let ino = h.meta.child_ino(ROOT_INO, &dir).ok().flatten();
                    if let (Some(to), Some(ino)) = (to, ino) {
                        let r = tokio::time::timeout(
                            Duration::from_millis(500),
                            h.control(constellation_authority::Control::Delegate {
                                range: (0, 0),
                                dir: ino,
                                node: to,
                            }),
                        )
                        .await;
                        if let Ok(Ok(Ok(_))) = r {
                            note(format!(
                                "t={} delegated {dir} to node {to} through holder {holder}",
                                cluster.env.clock.elapsed_ms()
                            ));
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            note(format!("t={} could not delegate {dir}", fault.at_ms));
        }
        FaultKind::CrashHolder {
            restart_ms,
            keep_journal,
        } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} crash-holder: no lease yet", fault.at_ms));
                return;
            };
            let holder = lease.holder;
            if holder == 0 || !cluster.ids().contains(&holder) || !cluster.get(holder).alive() {
                note(format!(
                    "t={} crash-holder: holder {holder} not alive",
                    fault.at_ms
                ));
                return;
            }
            note(format!(
                "t={} crash holder {holder} (restart {restart_ms:?}, keep_journal {keep_journal})",
                fault.at_ms
            ));
            cluster
                .failovers
                .lock()
                .unwrap()
                .push((cluster.env.clock.elapsed_ms(), None));
            let now = cluster.env.clock.now().0;
            cluster
                .note_takedown(holder, restart_ms.map(|r| now + r as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(holder);
            if let Some(after) = restart_ms {
                tokio::time::sleep(Duration::from_millis(after)).await;
                cluster.restart(holder, keep_journal.then_some(meta));
            }
        }
        FaultKind::CrashNode {
            node,
            restart_ms,
            keep_journal,
        } => {
            if !cluster.get(node).alive() {
                return;
            }
            note(format!(
                "t={} crash node {node} (restart {restart_ms:?}, keep_journal {keep_journal})",
                fault.at_ms
            ));
            let now = cluster.env.clock.now().0;
            cluster
                .note_takedown(node, restart_ms.map(|r| now + r as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(node);
            if let Some(after) = restart_ms {
                tokio::time::sleep(Duration::from_millis(after)).await;
                cluster.restart(node, keep_journal.then_some(meta));
            }
        }
        FaultKind::Partition { a, b, for_ms } => {
            note(format!(
                "t={} partition {a}<->{b} for {for_ms}ms",
                fault.at_ms
            ));
            cluster.env.bus.set_partition(a, b, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bus.set_partition(a, b, false);
        }
        FaultKind::CutS3 { node, for_ms } => {
            note(format!(
                "t={} cut S3 for node {node} for {for_ms}ms",
                fault.at_ms
            ));
            cluster.env.bucket.set_cut(node, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.set_cut(node, false);
        }
        FaultKind::Pause { node, for_ms } => {
            note(format!(
                "t={} pause node {node} for {for_ms}ms",
                fault.at_ms
            ));
            let now = cluster.env.clock.now().0;
            cluster
                .note_takedown(node, Some(now + for_ms as i64), fault.at_ms)
                .await;
            let until = tokio::time::Instant::now() + Duration::from_millis(for_ms);
            cluster.get(node).pause_until(until);
        }
        FaultKind::SlowReplies { delay_ms, for_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                return;
            };
            note(format!(
                "t={} slow replies from holder {} by {delay_ms}ms for {for_ms}ms",
                fault.at_ms, lease.holder
            ));
            cluster
                .env
                .bus
                .set_reply_delay(lease.holder, Some(delay_ms));
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bus.set_reply_delay(lease.holder, None);
        }
        FaultKind::S3Rule(rule) => {
            note(format!("t={} S3 rule {rule:?}", fault.at_ms));
            cluster.env.bucket.script(rule);
        }
        FaultKind::JoinFresh => {
            let id = cluster.ids().into_iter().max().unwrap_or(0) + 1;
            note(format!("t={} fresh node {id} joins", fault.at_ms));
            cluster.restart(id, None);
        }
        FaultKind::CrashBackup { restart_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} crash-backup: no lease yet", fault.at_ms));
                return;
            };
            let Some(&backup) = lease.backups.first() else {
                note(format!("t={} crash-backup: no backup listed", fault.at_ms));
                return;
            };
            if !cluster.ids().contains(&backup) || !cluster.get(backup).alive() {
                return;
            }
            note(format!(
                "t={} crash backup {backup} of holder {} (restart {restart_ms:?})",
                fault.at_ms, lease.holder
            ));
            let now = cluster.env.clock.now().0;
            cluster
                .note_takedown(backup, restart_ms.map(|r| now + r as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(backup);
            if let Some(after) = restart_ms {
                tokio::time::sleep(Duration::from_millis(after)).await;
                cluster.restart(backup, Some(meta));
            }
        }
        FaultKind::CutS3Holder { for_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                return;
            };
            note(format!(
                "t={} cut S3 for holder {} for {for_ms}ms",
                fault.at_ms, lease.holder
            ));
            cluster.env.bucket.set_cut(lease.holder, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.set_cut(lease.holder, false);
        }
        FaultKind::PartitionLocker { for_ms } => {
            let Some(node) = wait_for_locker(&cluster).await else {
                note(format!("t={} partition-locker: no locker", fault.at_ms));
                return;
            };
            let others: Vec<NodeId> = cluster.ids().into_iter().filter(|n| *n != node).collect();
            note(format!(
                "t={} partition locker {node} from {others:?} for {for_ms}ms",
                cluster.env.clock.elapsed_ms()
            ));
            for o in &others {
                cluster.env.bus.set_partition(node, *o, true);
            }
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            for o in &others {
                cluster.env.bus.set_partition(node, *o, false);
            }
        }
        FaultKind::CrashHolderWhileLocked { restart_ms } => {
            if wait_for_locker(&cluster).await.is_none() {
                note(format!(
                    "t={} crash-holder-while-locked: no locker",
                    fault.at_ms
                ));
                return;
            }
            let at_ms = cluster.env.clock.elapsed_ms();
            note(format!(
                "t={at_ms} a locker is in its critical section: crash the holder"
            ));
            Box::pin(run_fault(
                cluster,
                ScheduledFault {
                    at_ms: 0,
                    kind: FaultKind::CrashHolder {
                        restart_ms,
                        keep_journal: true,
                    },
                },
                log.clone(),
            ))
            .await;
        }
        FaultKind::CrashLocker { restart_ms } => {
            let Some(node) = wait_for_locker(&cluster).await else {
                note(format!("t={} crash-locker: no locker", fault.at_ms));
                return;
            };
            note(format!(
                "t={} node {node} is in its critical section: crash it",
                cluster.env.clock.elapsed_ms()
            ));
            Box::pin(run_fault(
                cluster,
                ScheduledFault {
                    at_ms: 0,
                    kind: FaultKind::CrashNode {
                        node,
                        restart_ms,
                        keep_journal: true,
                    },
                },
                log.clone(),
            ))
            .await;
        }
        FaultKind::EpochOutage { members, for_ms } => {
            super::epochs::epoch_outage(cluster.clone(), fault.at_ms, members, for_ms, log.clone())
                .await;
        }
        FaultKind::HolderCutEpoch { for_ms, decline } => {
            super::epochs::holder_cut_epoch(
                cluster.clone(),
                fault.at_ms,
                for_ms,
                decline,
                log.clone(),
            )
            .await;
        }
        FaultKind::PartitionBackup { for_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} partition-backup: no lease yet", fault.at_ms));
                return;
            };
            let Some(&backup) = lease.backups.first() else {
                note(format!(
                    "t={} partition-backup: no backup listed",
                    fault.at_ms
                ));
                return;
            };
            note(format!(
                "t={} partition holder {} <-> backup {backup} for {for_ms}ms",
                fault.at_ms, lease.holder
            ));
            cluster.env.bus.set_partition(lease.holder, backup, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bus.set_partition(lease.holder, backup, false);
        }
        FaultKind::CrashEpochMember { restart_ms } => {
            crash_epoch_member(&cluster, fault.at_ms, restart_ms, &note).await;
        }
    }
}

/// `FaultKind::CrashEpochMember`: waits up to 15 s for an active epoch
/// whose hold owner is known; the victim is a live member other than the
/// owner — first choice, one whose chunk the owner already awaits
/// (`Meta::remote_chunks`), then one with dirty chunks, then any, the
/// lower tiers only after the epoch has been active for 3 s.
async fn crash_epoch_member(
    cluster: &Arc<Cluster>,
    at_ms: u64,
    restart_ms: Option<u64>,
    note: &impl Fn(String),
) {
    let world = cluster.env.world.clone();
    let mut active_for = 0u32;
    let mut picked: Option<(NodeId, NodeId, &'static str)> = None;
    for _ in 0..150 {
        let ids = cluster.ids();
        let live = |id: &NodeId| cluster.get(*id).alive();
        let owner = ids
            .iter()
            .copied()
            .find(|id| live(id) && cluster.get(*id).view().epoch_held);
        if let Some(owner) = owner {
            active_for += 1;
            let members: Vec<NodeId> = ids
                .iter()
                .copied()
                .filter(|id| {
                    *id != owner && live(id) && cluster.get(*id).shared.epoch.lock().unwrap().active
                })
                .collect();
            let awaited: BTreeSet<NodeId> = cluster
                .get(owner)
                .meta
                .remote_chunks()
                .unwrap_or_default()
                .into_iter()
                .map(|r| r.node)
                .collect();
            if let Some(m) = members.iter().copied().find(|m| awaited.contains(m)) {
                picked = Some((owner, m, "a chunk the owner awaits"));
                break;
            }
            if active_for >= 30 {
                let dirty = members
                    .iter()
                    .copied()
                    .filter(|m| world.dirty_on(*m) > 0)
                    .max_by_key(|m| world.dirty_on(*m));
                if let Some(m) = dirty {
                    picked = Some((owner, m, "dirty chunks"));
                    break;
                }
                if let Some(m) = members.first().copied() {
                    picked = Some((owner, m, "no chunks"));
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let Some((owner, node, why)) = picked else {
        note(format!(
            "t={at_ms} crash-epoch-member: no active epoch with a hold owner"
        ));
        return;
    };
    note(format!(
        "t={} crash epoch member {node} ({why}; hold owner {owner}; restart {restart_ms:?})",
        cluster.env.clock.elapsed_ms()
    ));
    let now = cluster.env.clock.now().0;
    cluster
        .note_takedown(node, restart_ms.map(|r| now + r as i64), at_ms)
        .await;
    let meta = cluster.crash(node);
    match restart_ms {
        Some(after) => {
            tokio::time::sleep(Duration::from_millis(after)).await;
            note(format!(
                "t={} epoch member {node} returns with its disk",
                cluster.env.clock.elapsed_ms()
            ));
            cluster.restart(node, Some(meta));
        }
        None => {
            world.gone.lock().unwrap().insert(node);
        }
    }
}

/// Plan 30 §M14: a live node other than the lease holder inside a
/// critical section, waited for up to 5 s.
async fn wait_for_locker(cluster: &Arc<Cluster>) -> Option<NodeId> {
    for _ in 0..1_000 {
        let holder = read_lease(&cluster.env.bucket).await.map(|l| l.holder);
        let found = cluster
            .locks
            .nodes_in_io()
            .into_iter()
            .find(|n| Some(*n) != holder && cluster.get(*n).alive());
        if found.is_some() {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

/// Fix "capture under an epoch hold": the operator procedure. On every
/// live node, a remote-pending row whose chunk is never coming
/// (`ChunkWorld::hopeless`: its uploader is gone for good, or awaits it
/// in turn from one that is) and every inode with an unrecoverable
/// (poisoned) chunk are dropped — `constellation repair drop-held <ino>
/// --remote` and `drop-held <ino>` — once they have been seen hopeless
/// for `after_ms` of simulated time (`None`: at once), as an operator
/// watching `status.held.remote`'s ages would. The dropped writes become
/// refused replays and conflict copies, their outcome goes in the log,
/// and everything that waited on them moves on.
fn operator_drop_hopeless(
    cluster: &Arc<Cluster>,
    world: &Arc<ChunkWorld>,
    fault_log: &Arc<Mutex<Vec<String>>>,
    after_ms: Option<u64>,
    first_seen: &mut BTreeMap<(NodeId, u64, bool), u64>,
) {
    let now_ms = cluster.env.clock.elapsed_ms();
    for id in cluster.ids() {
        let n = cluster.get(id);
        if !n.alive() {
            continue;
        }
        let mut inos: BTreeSet<(u64, bool)> = n
            .meta
            .remote_chunks()
            .unwrap_or_default()
            .into_iter()
            .filter(|r| world.hopeless(&r.hash, r.node))
            .map(|r| (r.ino, true))
            .collect();
        inos.extend(
            n.meta
                .unrecoverable_chunks()
                .unwrap_or_default()
                .into_iter()
                .map(|(_, ino)| (ino, false)),
        );
        for (ino, remote) in inos {
            let since = *first_seen.entry((id, ino, remote)).or_insert(now_ms);
            if after_ms.is_some_and(|after| now_ms < since + after) {
                continue;
            }
            let now_unix = n.clock.now().0 / 1000;
            let dropped = if remote {
                n.meta.drop_held_remote(ino, now_unix)
            } else {
                n.meta.drop_held(ino, now_unix)
            };
            match dropped {
                Ok(d) => {
                    world.remote_dropped.fetch_add(
                        (d.dropped + d.queued_dropped) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    // What the drop rolled back is tentative for the
                    // checkers, as a deposition's rollback is (the core's
                    // counters do not move for an operator's drop): the
                    // dropped writes (refused replays) and their
                    // dependents, re-executed after later ops.
                    if let Ok(queue) = n.meta.pending_replays() {
                        let mut t = n.shared.tentative.lock().unwrap();
                        for q in queue {
                            t.insert(q.rid);
                        }
                    }
                    fault_log.lock().unwrap().push(format!(
                        "t={now_ms} node {id}: repair drop-held {ino:#x}{}: {d:?}",
                        if remote { " --remote" } else { "" }
                    ));
                }
                Err(e) => fault_log.lock().unwrap().push(format!(
                    "t={now_ms} node {id}: repair drop-held {ino:#x} refused: {e}"
                )),
            }
        }
    }
}

/// Run one seed. `Err` carries the failure and the replay command.
pub fn run_seed(seed: u64, cfg: SimConfig) -> Result<Report, String> {
    // `AUTHORITY_SIM_WATCHDOG=1`: an OS thread reports any event whose
    // handling has taken more than two seconds of real time.
    if std::env::var_os("AUTHORITY_SIM_WATCHDOG").is_some() {
        std::thread::spawn(|| loop {
            std::thread::sleep(Duration::from_secs(2));
            if let Some((clock, hist)) = super::node::EVENT_HISTOGRAM.lock().unwrap().as_ref() {
                let mut top: Vec<(&String, &u64)> = hist.iter().collect();
                top.sort_by(|a, b| b.1.cmp(a.1));
                eprintln!(
                    "WATCHDOG: simulated clock {clock}, top events: {:?}",
                    &top[..top.len().min(6)]
                );
            }
            if let Some(waiting) = super::node::WAITING.lock().unwrap().as_ref() {
                eprintln!("WATCHDOG: waiting: {waiting:?}");
            }
            if let Some(gaps) = super::node::TIMER_GAPS.lock().unwrap().as_ref() {
                eprintln!("WATCHDOG: timer gaps (last, count, min gap ms): {gaps:?}");
            }
            if let Some((node, event, since)) = super::node::IN_FLIGHT.lock().unwrap().clone() {
                if since.elapsed() > Duration::from_secs(2) {
                    eprintln!(
                        "WATCHDOG: node {node} has been handling an event for {:?}: {}",
                        since.elapsed(),
                        &event[..event.len().min(400)]
                    );
                }
            }
        });
    }
    // `RUST_LOG=constellation_authority=debug` narrates a replay, each
    // line stamped with the simulated time (`t=` ms, as the fault log).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .with_timer(SimTime)
        .try_init();
    install_panic_hook();
    let me = std::thread::current().id();
    PANICKED.lock().unwrap().retain(|(t, _)| *t != me);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let result = rt.block_on(async {
        tokio::select! {
            r = run_inner(seed, cfg) => r,
            msg = panic_watch() => Err(format!("a task panicked: {msg}")),
        }
    });
    result.map_err(|e| {
        format!(
            "{e}\n  replay: AUTHORITY_SIM_SEED={seed} cargo test -p constellation-authority --test sim replay_seed -- --nocapture --exact"
        )
    })
}

async fn run_inner(seed: u64, cfg: SimConfig) -> Result<Report, String> {
    let mut rng = StdRng::seed_from_u64(seed);
    let clock = Clock::start();
    SIM_START.with(|t| t.set(Some(tokio::time::Instant::now())));
    let bucket = Bucket::new(seed, cfg.s3_latency);
    let bus = Bus::new(seed, cfg.p2p_delay, cfg.p2p_drop);
    bus.set_stream_faults(cfg.stream_faults.clone());
    bus.set_deleg_answer_delay(cfg.deleg_answer_delay);
    let commits: Arc<Mutex<Vec<CommitRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let in_doubt: super::node::InDoubtLog = Arc::new(Mutex::new(Vec::new()));
    let env = NodeEnv {
        bucket: bucket.clone(),
        bus: bus.clone(),
        clock,
        commits: commits.clone(),
        in_doubt: in_doubt.clone(),
        config: cfg.core.clone(),
        panic_after_events: cfg.panic_after_events,
        fast_path: cfg.fast_path,
        clock_skew_ms: cfg.clock_skew_ms,
        seed,
        world: ChunkWorld::new(),
    };
    env.world
        .hold_ms
        .store(cfg.upload_hold_ms, std::sync::atomic::Ordering::Relaxed);
    let cluster = Arc::new(Cluster {
        nodes: Mutex::new(BTreeMap::new()),
        env,
        failovers: Mutex::new(Vec::new()),
        longest_submit_ms: Default::default(),
        budget_exceeded: Mutex::new(Vec::new()),
        down: Mutex::new(BTreeMap::new()),
        claim_ms: 4 * cfg.s3_latency.1 as i64,
        epochs: Mutex::new(Vec::new()),
        epochs_continued: Mutex::new(0),
        epochs_superseded: Mutex::new(0),
        split_brains: Mutex::new(Vec::new()),
        slack: cfg.epoch_slack,
        locks: Arc::new(super::locks::LockGhost::new(seed)),
    });
    for ((a, b), ms) in &cfg.rtts {
        bus.set_rtt(*a, *b, *ms);
    }
    for id in 1..=cfg.nodes {
        cluster.restart(id, None);
    }
    let history = Arc::new(History::default());
    let abandoned = Arc::new(Mutex::new(HashSet::new()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let fault_log = Arc::new(Mutex::new(Vec::new()));

    // Plan 30 §M11: the directories, then the delegations (node 1 takes
    // the lease with the first mkdir and delegates as the root).
    let markers: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let marker_stats: Arc<Mutex<(u64, u64)>> = Arc::new(Mutex::new((0, 0)));
    if !cfg.dirs.is_empty() {
        setup_dirs(&cluster, &cfg, &failures).await;
    }
    if cfg.lock_files > 0 {
        setup_lock_files(&cluster, &cfg, &history, &abandoned, &failures).await;
    }

    // The workload.
    let mut clients = Vec::new();
    for node in 1..=cfg.nodes {
        for k in 0..cfg.clients_per_node {
            // Plan 30 §M11: node 1 (the root) writes in the root
            // directory, node `n ≥ 2` in `dirs[n − 2]` (its delegation,
            // when one is configured); a second client writes in the
            // next directory (a forward to its delegate).
            let home = if cfg.shared_dir {
                cfg.dirs.first().cloned().unwrap_or_default()
            } else {
                home_dir(&cfg.dirs, node, k)
            };
            let ops = if cfg.shared_dir {
                {
                    let bits = if cfg.check_range_bits > 0 {
                        cfg.check_range_bits
                    } else {
                        2
                    };
                    let idx = ((node.max(1) - 1) % (1u64 << bits)) as u32;
                    gen_ops_unique(
                        &mut rng,
                        cfg.ops_per_client,
                        cfg.names,
                        &home,
                        node,
                        (bits, idx),
                    )
                }
            } else {
                gen_ops_in(
                    &mut rng,
                    cfg.ops_per_client,
                    cfg.names,
                    &home,
                    &cfg.dirs,
                    cfg.cross_ratio,
                )
            };
            let pace = rng.random_range(20..400);
            // Reads draw from their own generator, so a config without
            // reads replays exactly the schedules it had before M6.
            let mut read_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0x5e55);
            let steps = with_reads(&mut read_rng, ops, cfg.names, cfg.read_ratio);
            let mut write_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0xc4a1);
            let steps = with_writes(&mut write_rng, steps, cfg.chunk_writes);
            // Plan 30 §M14: locks from their own generator too.
            let mut lock_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0x10c5);
            let steps = with_locks(&mut lock_rng, steps, &cfg);
            clients.push(tokio::spawn(client_thread(
                cluster.clone(),
                history.clone(),
                node,
                node * 16 + k,
                steps,
                abandoned.clone(),
                failures.clone(),
                pace,
                cfg.session_wait.then_some(cfg.session_wait_ms),
                cfg.strict,
                cfg.lock_ignore_fence,
            )));
        }
    }
    // Plan 30 §M11: marker writers and watchers.
    let mut marker_tasks = Vec::new();
    if cfg.marker_pairs > 0 && cfg.dirs.len() >= 2 {
        for node in 1..=cfg.nodes {
            let home = home_dir(&cfg.dirs, node, 0);
            let other = home_dir(&cfg.dirs, node, 1);
            marker_tasks.push(tokio::spawn(marker_writer(
                cluster.clone(),
                history.clone(),
                node,
                (1 << 16) + node,
                home,
                other,
                cfg.marker_pairs,
                markers.clone(),
                abandoned.clone(),
            )));
        }
        for node in 1..=cfg.nodes {
            marker_tasks.push(tokio::spawn(marker_watcher(
                cluster.clone(),
                node,
                markers.clone(),
                marker_stats.clone(),
                failures.clone(),
            )));
        }
    }
    // The faults.
    let horizon = cfg.ops_per_client * 400 + 2_000;
    let mut faults = cfg.faults.clone();
    faults.extend(gen_faults(&mut rng, &cfg, horizon));
    let mut fault_tasks = Vec::new();
    // Plan 30 §M10: the single-authority sampler.
    let sampler = tokio::spawn(super::epochs::sample_authority(cluster.clone()));
    for f in faults {
        fault_tasks.push(tokio::spawn(run_fault(
            cluster.clone(),
            f,
            fault_log.clone(),
        )));
    }
    // Fix "capture under an epoch hold": through the whole run, per
    // node, consecutive samples with shippable rows left unshipped behind
    // a deferred transaction while it holds the lease with S3 up — the
    // log must not stall on one absent member's chunk.
    let sampler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stall_violations: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    if cfg.chunk_writes > 0.0 {
        let (cluster, stop, violations) = (
            cluster.clone(),
            sampler_stop.clone(),
            stall_violations.clone(),
        );
        let fault_log = fault_log.clone();
        tokio::spawn(async move {
            let world = cluster.env.world.clone();
            let mut stalled: BTreeMap<NodeId, u32> = BTreeMap::new();
            let mut first_seen = BTreeMap::new();
            let mut ticks = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(250)).await;
                ticks += 1;
                // The operator, watching `status`: a write whose chunk is
                // never coming is dropped 20 s after it shows.
                if ticks.is_multiple_of(4) {
                    operator_drop_hopeless(
                        &cluster,
                        &world,
                        &fault_log,
                        Some(20_000),
                        &mut first_seen,
                    );
                }
                for id in cluster.ids() {
                    let n = cluster.get(id);
                    if !n.alive() {
                        stalled.remove(&id);
                        continue;
                    }
                    // A paused process ships nothing until it resumes, and
                    // its view is from before the pause (flex-crash seeds
                    // 19013, 21497, 28752: the hold owner paused through
                    // the heal; chunk metered-own-rows: with the upload
                    // hold, a deferral outlasts a pause often enough). Its
                    // count is frozen, not reset: a stall split by a pause
                    // still adds up.
                    if n.paused() {
                        continue;
                    }
                    let held = n.meta.held_summary();
                    if held.deferred == 0 {
                        stalled.remove(&id);
                        continue;
                    }
                    world
                        .deferred_seen
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let v = n.view();
                    let sequencing = v.held_epoch.is_some()
                        && !v.epoch_held
                        && !v.gate_pending
                        && !v.lost
                        && !cluster.env.bucket.is_cut(id);
                    let shippable =
                        constellation_authority::Replica::take_journal(&*n.meta, 10_000)
                            .map(|b| b.len())
                            .unwrap_or(0);
                    if sequencing && shippable > 0 {
                        let c = stalled.entry(id).or_insert(0);
                        *c += 1;
                        if *c == 20 {
                            violations.lock().unwrap().push(format!(
                                "t={} node {id} kept {shippable} shippable journal row(s) \
                                 unshipped for 5 s behind {} deferred transaction(s) (held \
                                 {held:?})",
                                cluster.env.clock.elapsed_ms(),
                                held.deferred
                            ));
                        }
                    } else {
                        stalled.remove(&id);
                    }
                }
            }
        });
    }
    super::node::note_waiting("run", "clients".into());
    // A client whose op is never answered would run the clock forever
    // (paused time races: 85 000 simulated seconds in 30 s of wall for
    // long-delegated seed 70051 before the fix): a bound, and a failure
    // that names it.
    let client_cap = Duration::from_millis(cfg.settle_ms * 6);
    for c in clients {
        match tokio::time::timeout(client_cap, c).await {
            Ok(r) => r.expect("client task"),
            Err(_) => {
                failures.lock().unwrap().push(format!(
                    "a client op was never answered within {client_cap:?} of simulated time; \
                     waiting: {:?}",
                    super::node::WAITING.lock().unwrap()
                ));
                break;
            }
        }
    }
    for m in marker_tasks {
        // Watchers loop until the writers are done and the markers
        // settled; the writer tasks end on their own.
        let _ = tokio::time::timeout(Duration::from_secs(120), m).await;
    }
    super::node::note_waiting("run", "faults".into());
    for f in fault_tasks {
        f.await.expect("fault task");
    }
    super::node::note_waiting("run", "settle".into());
    // Every node that is still down and would come back has come back
    // by now (restart timers are inside the fault tasks). A node left
    // dead stays dead.

    // Settle: one more write from a live node. If the lease names a dead
    // node this brings the takeover forward (the model's
    // `failover_pending` exemption is only for the case nobody writes
    // again); and it ships one more segment, past the floor of any hint
    // an `EEXIST` refusal left outstanding (a hint retires only when the
    // applied position reaches the holder's next ship position, so an
    // idle cluster would otherwise keep it forever).
    let settle_thread = 1 << 20;
    if let Some(id) = cluster
        .ids()
        .into_iter()
        .find(|id| cluster.get(*id).alive())
    {
        let handle = cluster.get(id);
        let rid = handle.next_rid();
        let op = NsOp::Create("settle".into());
        let mop = mutate_op(&handle.meta, &op);
        history.invoke(settle_thread, rid, op);
        let mut done = false;
        for _ in 0..3 {
            match cluster.get(id).submit(rid, mop.clone()).await {
                Ok(ClientReply::Outcome(o)) => {
                    if let Ok(ret) = ns_ret(&o) {
                        history.ret(settle_thread, rid, ret);
                    }
                    done = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        if !done {
            abandoned.lock().unwrap().insert(rid);
        }
    }

    // An explicit publish from whoever holds the lease now (a snapshot's
    // `PublishNow`): the commit-prefix check then always has a commit at
    // the head to compare, and the control path is exercised.
    if let Some(lease) = read_lease(&bucket).await {
        if cluster.ids().contains(&lease.holder) && cluster.get(lease.holder).alive() {
            let _ = cluster
                .get(lease.holder)
                .control(constellation_authority::Control::PublishNow)
                .await;
        }
    }

    super::node::note_waiting("run", "quiescence".into());
    // Quiescence: no client op in flight, every live replica exactly the
    // log prefix at head, no job in the slot.
    let log_raw = constellation_store_s3::LogStore::new(bucket.raw());
    let mut waited = 0u64;
    let mut quiescent = false;
    let mut extra_settles = 0u64;
    let world = cluster.env.world.clone();
    while waited < cfg.settle_ms {
        tokio::time::sleep(Duration::from_millis(250)).await;
        waited += 250;
        if cfg.chunk_writes > 0.0 {
            // The operator procedure (see `operator_drop_hopeless`), again
            // at the settle's cadence for what appeared after the
            // clients finished.
            if waited.is_multiple_of(3_000) {
                operator_drop_hopeless(&cluster, &world, &fault_log, None, &mut BTreeMap::new());
            }
        }
        // A hint installed after the settle write (a replayed op refused
        // with `EEXIST` after the last segment shipped) retires only when
        // the log moves past its floor: write again, as a busy cluster
        // would. Production leaves such an idle hint be (M9's exit list).
        if waited.is_multiple_of(5_000) {
            let hinted = cluster
                .ids()
                .into_iter()
                .map(|id| cluster.get(id))
                .filter(|n| n.alive())
                .any(|n| n.view().speculation.outstanding > 0 && n.view().clients_in_flight == 0);
            if hinted {
                if let Some(id) = cluster
                    .ids()
                    .into_iter()
                    .find(|id| cluster.get(*id).alive())
                {
                    extra_settles += 1;
                    let handle = cluster.get(id);
                    let rid = handle.next_rid();
                    let op = NsOp::Create(format!("settle-{extra_settles}"));
                    let mop = mutate_op(&handle.meta, &op);
                    history.invoke(settle_thread + extra_settles, rid, op);
                    match handle.submit(rid, mop).await {
                        Ok(ClientReply::Outcome(o)) => {
                            if let Ok(ret) = ns_ret(&o) {
                                history.ret(settle_thread + extra_settles, rid, ret);
                            }
                        }
                        _ => {
                            abandoned.lock().unwrap().insert(rid);
                        }
                    }
                }
            }
        }
        let head = log_raw
            .list_segments()
            .await
            .map(|s| s.into_iter().max().unwrap_or(0))
            .unwrap_or(0);
        let live: Vec<Arc<NodeHandle>> = cluster
            .ids()
            .into_iter()
            .map(|id| cluster.get(id))
            .filter(|n| n.alive())
            .collect();
        let all = live.iter().all(|n| {
            let v = n.view();
            v.clients_in_flight == 0
                && v.job.is_none()
                && super::node::is_quiescent(&n.meta)
                && constellation_authority::Replica::applied_seq(&*n.meta).unwrap_or(0) >= head
        });
        if all {
            quiescent = true;
            break;
        }
    }

    // Plan 30 §M14: no lock request, waiter or recall is left: every
    // parked request was served and every recall answered or outwaited.
    let mut locks_stuck = None;
    if cfg.lock_files > 0 {
        let mut drained = false;
        for _ in 0..100 {
            let busy: Vec<(NodeId, usize, usize, usize)> = cluster
                .ids()
                .into_iter()
                .map(|id| cluster.get(id))
                .filter(|n| n.alive())
                .map(|n| {
                    let v = n.view();
                    (n.id, v.lock_requests, v.lock_waiters, v.lock_recalls)
                })
                .filter(|(_, a, b, c)| a + b + c > 0)
                .collect();
            if busy.is_empty() {
                drained = true;
                break;
            }
            locks_stuck = Some(busy);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if drained {
            locks_stuck = None;
        }
    }

    sampler.abort();
    // Checks.
    let mut report = Report {
        seed,
        ..Default::default()
    };
    {
        let formed = cluster.epochs.lock().unwrap();
        report.epochs_formed = formed.len();
        report.epochs_missing_node = formed
            .iter()
            .filter(|m| m.len() < cfg.nodes as usize)
            .count();
    }
    report.epochs_continued = *cluster.epochs_continued.lock().unwrap();
    report.epochs_superseded = *cluster.epochs_superseded.lock().unwrap();
    report.authority_samples = super::epochs::SAMPLES.with(|s| s.get());
    {
        let (checks, violations) = *marker_stats.lock().unwrap();
        report.marker_checks = checks;
        report.marker_violations = violations;
    }
    if let Some(what) = cluster.split_brains.lock().unwrap().first() {
        return Err(format!(
            "two authorities at once (plan 30 §M10): {what}\n  faults: {:?}",
            fault_log.lock().unwrap()
        ));
    }
    // Plan 30 §M14: mutual exclusion.
    report.locks = cluster.locks.counters();
    let lock_violations = cluster.locks.violations();
    if let Some(what) = lock_violations.first() {
        return Err(format!(
            "mutual exclusion violated (plan 30 §M14): {what} ({} violation(s))\n  faults: {:?}\n  \
             lock trace (last events):\n    {}",
            lock_violations.len(),
            fault_log.lock().unwrap(),
            cluster.locks.trace().join("\n    ")
        ));
    }
    let gaps = cluster.locks.visibility_gaps();
    if !gaps.is_empty() && std::env::var_os("AUTHORITY_SIM_VISIBILITY").is_some() {
        return Err(format!(
            "lock visibility gap (plan 30 §M14): {} ({} gap(s))\n  faults: {:?}\n  \
             lock trace (last events):\n    {}",
            gaps[0],
            gaps.len(),
            fault_log.lock().unwrap(),
            cluster.locks.trace().join("\n    ")
        ));
    }
    // Fairness: reported with the counters (`LockCounters::overtaken`);
    // the fault-free lock configurations assert none.
    for o in cluster.locks.overtakes().iter().take(3) {
        eprintln!("  lock fairness: {o}");
    }
    report.faults = fault_log.lock().unwrap().clone();
    report.simulated_ms = clock.elapsed_ms();
    if let Some(f) = failures.lock().unwrap().first() {
        return Err(format!("{f}\n  faults: {:?}", report.faults));
    }
    // Plan 30 §M9: under a durable acknowledgement policy an acknowledged
    // op is never exempt — a rollback of one is exactly what the backup
    // (or `ack=s3`) exists to make impossible, so only ops the client
    // abandoned (its process died with the call in flight) are tentative.
    let budget_exceeded = cluster.budget_exceeded.lock().unwrap().clone();
    report.durability_budget_exceeded = budget_exceeded.clone();
    let strict_durability = cfg.strict_durability && budget_exceeded.is_empty();
    let mut tentative: HashSet<Rid> = abandoned.lock().unwrap().clone();
    let acked: HashSet<Rid> = history.returned().into_iter().map(|(rid, _)| rid).collect();
    let mut rolled_back_acked = 0usize;
    for id in cluster.ids() {
        let n = cluster.get(id);
        for rid in n.shared.tentative.lock().unwrap().iter() {
            if acked.contains(rid) {
                rolled_back_acked += 1;
            }
            if !strict_durability {
                tentative.insert(*rid);
            }
        }
        report.stats.insert(id, n.view().stats);
        report.fast_path_executed += n
            .shared
            .fast_path_executed
            .load(std::sync::atomic::Ordering::Relaxed);
    }
    report.acked_rolled_back = rolled_back_acked;
    // Plan 30 §M14: a later holder read an older turn than the one
    // written under the previous exclusive lock — unless that write's
    // acknowledgement was itself rolled back (tentative: `Local`'s L2
    // window, a crashed sequencer's unshipped journal).
    let stale: Vec<String> = cluster
        .locks
        .stale_reads()
        .into_iter()
        .filter(|(missed, read, _)| {
            // The missed turn's acknowledgement rolled back, or the value
            // read is an older turn replayed late over the newer ones
            // (`Local`: a crashed sequencer's stranded op, replayed by rid
            // — its effect lands where the replay does).
            !tentative.contains(missed) && !read.is_some_and(|r| tentative.contains(&r))
        })
        .map(|(_, _, s)| s)
        .collect();
    if !stale.is_empty() && cfg.lock_writes {
        return Err(format!(
            "stale read under a lock (plan 30 §M14, lock-to-unlock coherence): {} ({} stale \
             read(s))\n  faults: {:?}\n  lock trace (last events):\n    {}",
            stale[0],
            stale.len(),
            fault_log.lock().unwrap(),
            cluster.locks.trace().join("\n    ")
        ));
    }
    if let Some(busy) = locks_stuck {
        return Err(format!(
            "lock state never drained after the workload (node, requests, waiters, recalls): \
             {busy:?}\n  faults: {:?}\n  lock trace (last events):\n    {}",
            report.faults,
            cluster.locks.trace().join("\n    ")
        ));
    }
    if cfg.lock_files > 0 && cfg.lock_ratio > 0.0 {
        // The core's counter restarts with a restarted node (the one that
        // made every grant may have crashed since): the clients' `Granted`
        // answers count too.
        let grants: u64 = report.stats.values().map(|s| s.lock_grants).sum();
        if (grants == 0 && report.locks.granted == 0) || report.locks.acquired == 0 {
            return Err(format!(
                "a lock workload that never took a lock (vacuous): {grants} grants, {:?}\n  faults: {:?}",
                report.locks, report.faults
            ));
        }
    }
    report.longest_submit_ms = cluster
        .longest_submit_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    report.failover_ms = cluster
        .failovers
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, d)| *d)
        .collect();
    let commit_list = commits.lock().unwrap().clone();
    let snapshots: BTreeSet<u64> = commit_list.iter().map(|c| c.applied).collect();
    let oracle = check::replay_log(bucket.raw(), &snapshots).await;
    report.segments = oracle.segments;
    report.fenced = oracle.fenced;
    report.commits = commit_list.len();
    let events = history.events();
    report.ops_invoked = history.invoked();
    report.ops_returned = history.returned().len();
    report.tentative = tentative.len();
    let lin_context = |e: String| {
        let listing: Vec<String> = events
            .iter()
            .map(|e| match e {
                super::history::HistEvt::Invoke { thread, rid, op } => format!(
                    "  invoke t{thread} rid({},{},{}) {op:?}{}",
                    rid.node,
                    rid.incarnation,
                    rid.seq,
                    if tentative.contains(rid) {
                        " [tentative]"
                    } else {
                        ""
                    }
                ),
                super::history::HistEvt::Return { thread, rid, ret } => format!(
                    "  return t{thread} rid({},{},{}) {ret:?}",
                    rid.node, rid.incarnation, rid.seq
                ),
            })
            .collect();
        format!(
            "{e}\n  faults: {:?}\n  history:\n{}\n  log: {:#?}\n  stats: {:#?}",
            report.faults,
            listing.join("\n"),
            oracle.describe,
            report.stats
        )
    };
    // The log-witnessed check is exact for this spec and always runs; the
    // generic tester's search is exponential in ops left in flight, so it
    // runs only while few ops are tentative (see `history.rs`).
    // Plan 30 §M11: under delegation the log orders keys of different
    // owners independently, so linearizability is per directory: every
    // check runs on the history projected to one directory (a rename
    // across two appears in both as its halves).
    let dirs = super::history::dirs_of(&events);
    let per_dir = !cfg.dirs.is_empty();
    let mut labels: Vec<String> = Vec::new();
    let projections: Vec<Vec<super::history::HistEvt>> = if per_dir && cfg.check_range_bits > 0 {
        // Plan 30 §M12: per hash range of every directory.
        let mut v = Vec::new();
        for d in &dirs {
            for idx in 0..(1u32 << cfg.check_range_bits) {
                labels.push(format!(
                    "{d:?} range {idx}/{}",
                    1u32 << cfg.check_range_bits
                ));
                v.push(super::history::project_range(
                    &events,
                    d,
                    cfg.check_range_bits,
                    idx,
                ));
            }
        }
        v
    } else if per_dir {
        labels = dirs.iter().map(|d| format!("{d:?}")).collect();
        dirs.iter()
            .map(|d| super::history::project_dir(&events, d))
            .collect()
    } else {
        vec![events.clone()]
    };
    report.dirs_checked = projections.len();
    let mut witness = super::history::Witness::default();
    for (i, proj) in projections.iter().enumerate() {
        let w =
            check_linearizable_witnessed(proj, &tentative, &oracle.completed_at).map_err(|e| {
                lin_context(format!(
                    "[directory {}] {e}",
                    labels.get(i).cloned().unwrap_or_default()
                ))
            })?;
        witness.observed_tentative += w.observed_tentative;
        witness.observers.extend(w.observers);
    }
    report.observed_tentative = witness.observed_tentative;
    // EC2 campaign 7, finding B-1: across directories and owners, the
    // log keeps every node's program order (see `history.rs`).
    super::history::check_session_order(&events, &tentative, &oracle.completed_at)
        .map_err(lin_context)?;
    if strict_durability && witness.observed_tentative > 0 {
        return Err(lin_context(format!(
            "{} refusal(s) observed an acknowledged effect that was later rolled back              (impossible under a durable acknowledgement policy)",
            witness.observed_tentative
        )));
    }
    // The generic tester enforces the same contract: a refusal that
    // observed a tentative effect (allowed above unless
    // `strict_durability`) is in flight forever, like that effect.
    let in_flight: HashSet<Rid> = tentative.union(&witness.observers).copied().collect();
    if events.len() <= STATERIGHT_EVENT_BOUND && in_flight.len() <= STATERIGHT_TENTATIVE_BOUND {
        for proj in &projections {
            check_linearizable(proj, &in_flight).map_err(lin_context)?;
        }
        report.stateright_checked = true;
    }
    // Plan 30 §M11: the causal-cut assertion at the root (a delegate
    // waited for its deps before executing, so the root never sees a
    // batch whose deps it lacks), and no marker without its data.
    for (id, stats) in &report.stats {
        if stats.deleg_deps_unsatisfied_at_append > 0 {
            return Err(format!(
                "node {id} appended {} delegate batches whose deps it lacked (causal cut)\n  stats: {:#?}",
                stats.deleg_deps_unsatisfied_at_append, report.stats
            ));
        }
    }
    if report.marker_violations > 0 {
        return Err(format!(
            "marker order violated {} times (of {} checks)\n  faults: {:?}",
            report.marker_violations, report.marker_checks, report.faults
        ));
    }
    // Fix "capture under an epoch hold": the chunk rule and the log's
    // liveness behind a deferred transaction.
    {
        let violations = world.violations.lock().unwrap().clone();
        if let Some(what) = violations.first() {
            return Err(format!(
                "a segment named a chunk S3 does not hold: {what} ({} violation(s))\n  faults: {:?}",
                violations.len(),
                fault_log.lock().unwrap()
            ));
        }
        sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let stall_violations = stall_violations.lock().unwrap().clone();
        if let Some(what) = stall_violations.first() {
            return Err(format!(
                "the log stalled behind a deferred transaction: {what}\n  faults: {:?}",
                fault_log.lock().unwrap()
            ));
        }
        let ord = std::sync::atomic::Ordering::Relaxed;
        report.chunk_writes = world.writes.load(ord);
        report.chunks_uploaded = world.uploaded.load(ord);
        report.remote_enrolled = world.remote_enrolled.load(ord);
        report.remote_acked = world.remote_acked.load(ord);
        report.deferred_seen = world.deferred_seen.load(ord);
        report.remote_dropped = world.remote_dropped.load(ord);
        report.member_gone = !world.gone.lock().unwrap().is_empty();
        report.held_back = world.held_back.load(ord);
        report.awaited_uploaded = world.awaited_uploaded.load(ord);
    }
    let counts = bucket.counts();
    report.s3_puts = counts.puts;
    report.s3_gets = counts.gets;
    report.p2p_sent = *bus.sent.lock().unwrap();
    let context = format!(
        "\n  faults: {:?}\n  log: {:#?}\n  stats: {:#?}",
        report.faults, oracle.describe, report.stats
    );
    // A convergence miss names an inode; the history says which ops
    // made it (M12 round 2, seed 10146 of the long-sessions configuration).
    check::check_commits(&commit_list, &oracle).map_err(|e| format!("{e}{context}"))?;

    let lease = read_lease(&bucket).await;
    let failover_pending = lease.as_ref().is_some_and(|l| {
        l.holder != 0
            && !l.released
            && !(cluster.ids().contains(&l.holder) && cluster.get(l.holder).alive())
    });
    let converged_checkable = quiescent && !failover_pending;
    if converged_checkable {
        let replicas: Vec<(u64, check::Dump)> = cluster
            .ids()
            .into_iter()
            .map(|id| cluster.get(id))
            .filter(|n| n.alive())
            .map(|n| (n.id, n.meta.ns_dump().expect("dump")))
            .collect();
        check::check_convergence(&replicas, &oracle).map_err(|e| lin_context(e.to_string()))?;
        report.converged_checked = true;
    } else if !quiescent {
        // What keeps each node from quiescence (phase 2b diagnostics).
        let why: Vec<String> = cluster
            .ids()
            .into_iter()
            .map(|id| cluster.get(id))
            .filter(|n| n.alive())
            .map(|n| {
                let rows = constellation_authority::Replica::take_journal(&*n.meta, 12)
                    .map(|b| {
                        b.into_iter()
                            .map(|(s, r)| format!("{s}:{r:?}"))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                // The chunk model: pending rows (hash, in S3, inode),
                // remote marks, chunks only this node's disk holds.
                let pending: Vec<_> = n
                    .meta
                    .pending_uploads()
                    .unwrap_or_default()
                    .iter()
                    .map(|(h, i)| (format!("{h}"), world.in_s3(h), *i))
                    .collect();
                let remote: Vec<_> = n
                    .meta
                    .remote_chunks()
                    .unwrap_or_default()
                    .iter()
                    .map(|c| format!("{c:?}"))
                    .collect();
                format!(
                    "node {}: pending {:?} remote {:?} dirty {} job {:?} journal {:?} held {:?} \
                     speculation {:?} live {:?} replays {}",
                    n.id,
                    pending,
                    remote,
                    world.dirty_on(n.id),
                    n.view().job_phase,
                    rows,
                    n.meta.held_summary(),
                    n.meta.speculation_counts().ok(),
                    n.meta.speculation_live_debug(),
                    n.meta.pending_replays().map(|q| q.len()).unwrap_or(0)
                )
            })
            .collect();
        return Err(format!(
            "the cluster did not reach quiescence within {}ms of simulated time\n  why: {:#?}\n  faults: {:?}\n  views: {:?}",
            cfg.settle_ms,
            why,
            report.faults,
            cluster
                .ids()
                .into_iter()
                .map(|id| (id, cluster.get(id).alive(), cluster.get(id).view()))
                .collect::<Vec<_>>()
        ));
    }
    // Plan 30 §M6: per-node read-your-writes and monotonic reads, with the
    // log as the witness (see `session.rs`). Enforced when reads wait.
    report.sessions = super::session::check_sessions(
        &events,
        &history.ticks(),
        &history.reads(),
        &tentative,
        &oracle.completed_at,
    );
    if cfg.session_wait {
        if let Some((checker, what)) = report.sessions.violations.first() {
            return Err(lin_context(format!(
                "session guarantee violated ({checker}): {what}"
            )));
        }
    }
    // Plan 30 §M8: close-to-open across nodes (see `cto.rs`); enforced
    // under `strict`, reported otherwise.
    report.cto = super::cto::check_cto(
        &events,
        &history.ticks(),
        &history.reads(),
        &tentative,
        &oracle.completed_at,
    );
    // With P2P off there is no ReadIndex: a strict read tails S3, which
    // covers closes whose records are in the log (inbox writes) but not a
    // holder's own acked-unshipped writes (see `core::readindex`, "P2P
    // off"). Reported there, enforced with P2P.
    let p2p = (cfg.core)(1, 1).p2p;
    if cfg.strict && p2p {
        if let Some(what) = report.cto.violations.first() {
            return Err(format!(
                "close-to-open violated (cto=strict): {what}\n  faults: {:?}\n  stats: {:#?}",
                report.faults, report.stats
            ));
        }
    }
    report.in_doubt_answers = in_doubt.lock().unwrap().len();
    check::check_in_doubt_answers(&in_doubt.lock().unwrap(), &oracle).map_err(|e| {
        format!(
            "{e}\n  faults: {:?}\n  log: {:#?}\n  stats: {:#?}",
            report.faults, oracle.describe, report.stats
        )
    })?;
    check::check_exactly_once(
        &history.returned(),
        &tentative,
        &oracle,
        converged_checkable,
    )
    .map_err(|e| {
        format!(
            "{e}\n  faults: {:?}\n  log: {:#?}\n  stats: {:#?}",
            report.faults, oracle.describe, report.stats
        )
    })?;
    Ok(report)
}

/// Plan 30 §M11: create `cfg.dirs` under the root through node 1 (it
/// takes the lease), then delegate `cfg.delegations` from it; wait until
/// every delegate has installed its generation.
async fn setup_dirs(cluster: &Arc<Cluster>, cfg: &SimConfig, failures: &Arc<Mutex<Vec<String>>>) {
    let handle = cluster.get(1);
    for d in &cfg.dirs {
        let rid = handle.next_rid();
        let op = MutateOp::Mkdir {
            parent: ROOT_INO,
            name: d.clone(),
            ino: handle.meta.allocate_ino(ROOT_INO).expect("ino"),
            mode: 0o755,
            uid: 0,
            gid: 0,
        };
        let mut ok = false;
        for _ in 0..20 {
            match handle.submit(rid, op.clone()).await {
                Ok(ClientReply::Outcome(MutateOutcome::Accepted { .. })) => {
                    ok = true;
                    break;
                }
                Ok(ClientReply::Outcome(MutateOutcome::Errno(Code::Exists))) => {
                    ok = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        if !ok {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: could not create directory {d}"));
            return;
        }
    }
    // Every node applies the directories before anything is delegated
    // (a delegate resolves ownership on its own replica).
    for _ in 0..200 {
        let all = cluster.ids().into_iter().all(|id| {
            let n = cluster.get(id);
            cfg.dirs
                .iter()
                .all(|d| n.meta.child_ino(ROOT_INO, d).ok().flatten().is_some())
        });
        if all {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for (dir, node) in &cfg.delegations {
        let Some(ino) = handle.meta.child_ino(ROOT_INO, dir).ok().flatten() else {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: directory {dir} missing on node 1"));
            return;
        };
        let mut done = false;
        for _ in 0..30 {
            match handle
                .control(constellation_authority::Control::Delegate {
                    range: (0, 0),
                    dir: ino,
                    node: *node,
                })
                .await
            {
                Ok(Ok(_)) => {
                    done = true;
                    break;
                }
                Ok(Err(e)) if e.contains("already holds") => {
                    done = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        if !done {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: could not delegate {dir} to node {node}"));
            return;
        }
    }
    for (dir, node, range) in &cfg.range_delegations {
        let Some(ino) = handle.meta.child_ino(ROOT_INO, dir).ok().flatten() else {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: directory {dir} missing on node 1"));
            return;
        };
        let mut done = false;
        for _ in 0..30 {
            match handle
                .control(constellation_authority::Control::Delegate {
                    range: *range,
                    dir: ino,
                    node: *node,
                })
                .await
            {
                Ok(Ok(_)) => {
                    done = true;
                    break;
                }
                Ok(Err(e)) if e.contains("is delegated") => {
                    done = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        if !done {
            failures.lock().unwrap().push(format!(
                "setup: could not delegate range {range:?} of {dir} to node {node}"
            ));
            return;
        }
    }
    // Plan 30 §M11 phase 2b: the designations, through the root's sync.
    if !cfg.designations.is_empty() {
        let mut entries = Vec::new();
        for (dir, node) in &cfg.designations {
            match handle.meta.child_ino(ROOT_INO, dir).ok().flatten() {
                Some(ino) => entries.push((ino, *node)),
                None => {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("setup: directory {dir} missing on node 1"));
                    return;
                }
            }
        }
        let mut done = false;
        for _ in 0..30 {
            if let Ok(Ok(_)) = handle
                .control(constellation_authority::Control::SyncDesignations {
                    entries: entries.clone(),
                })
                .await
            {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if !done {
            failures
                .lock()
                .unwrap()
                .push("setup: could not sync the designations".into());
            return;
        }
    }
    // Installed and renewed on every delegate.
    for _ in 0..400 {
        let ranged: Vec<(String, NodeId)> = cfg
            .range_delegations
            .iter()
            .map(|(d, n, _)| (d.clone(), *n))
            .collect();
        let all = cfg
            .delegations
            .iter()
            .chain(cfg.designations.iter())
            .chain(ranged.iter())
            .all(|(_, node)| {
                let v = cluster.get(*node).view();
                v.delegation
                    .mine
                    .iter()
                    .any(|(_, _, until, stopped, ..)| !*stopped && *until > 0)
            });
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    failures
        .lock()
        .unwrap()
        .push("setup: delegations were not installed on the delegates".into());
}

/// Plan 30 §M14: lock file `i`'s name.
fn lock_file_name(cfg: &SimConfig, i: usize) -> String {
    match &cfg.lock_dir {
        Some(d) => format!("{d}/lk{i}"),
        None => format!("lk{i}"),
    }
}

/// Plan 30 §M14: create the lock files through node 1 (recorded in the
/// history like any client op), then wait until every node has them.
async fn setup_lock_files(
    cluster: &Arc<Cluster>,
    cfg: &SimConfig,
    history: &Arc<History>,
    abandoned: &Arc<Mutex<HashSet<Rid>>>,
    failures: &Arc<Mutex<Vec<String>>>,
) {
    let handle = cluster.get(1);
    let mut names: Vec<String> = (0..cfg.lock_files)
        .map(|i| lock_file_name(cfg, i))
        .collect();
    let data_dir = cfg.lock_data_dir.as_ref().filter(|_| cfg.lock_writes);
    if let Some(d) = data_dir {
        names.extend((0..cfg.lock_files).map(|i| {
            if d.is_empty() {
                format!("dk{i}")
            } else {
                format!("{d}/dk{i}")
            }
        }));
    }
    for (i, name) in names.iter().enumerate() {
        let thread = (1 << 21) + i as u64;
        let rid = handle.next_rid();
        let op = NsOp::Create(name.clone());
        let mop = mutate_op(&handle.meta, &op);
        history.invoke(thread, rid, op);
        let mut ok = false;
        for _ in 0..20 {
            match handle.submit(rid, mop.clone()).await {
                Ok(ClientReply::Outcome(o)) => {
                    if let Ok(ret) = ns_ret(&o) {
                        history.ret(thread, rid, ret);
                        ok = true;
                    }
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        if !ok {
            abandoned.lock().unwrap().insert(rid);
            failures
                .lock()
                .unwrap()
                .push(format!("setup: could not create lock file {name}"));
            return;
        }
    }
    for _ in 0..200 {
        let inos: Option<Vec<u64>> = names
            .iter()
            .map(|name| {
                let (parent, leaf) = split_name(&handle.meta, name);
                handle.meta.child_ino(parent, &leaf).ok().flatten()
            })
            .collect();
        let everywhere = cluster.ids().into_iter().all(|id| {
            let n = cluster.get(id);
            names.iter().all(|name| {
                let (parent, leaf) = split_name(&n.meta, name);
                n.meta.child_ino(parent, &leaf).ok().flatten().is_some()
            })
        });
        if let (Some(inos), true) = (inos, everywhere) {
            let n = cfg.lock_files;
            let mut files: Vec<(String, u64)> = names.iter().cloned().zip(inos).collect();
            let data: Vec<u64> = files.split_off(n).into_iter().map(|(_, i)| i).collect();
            cluster.locks.set_files(files);
            cluster.locks.set_data(data);
            cluster.locks.set_by_ino(cfg.lock_unlink_ratio > 0.0);
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    failures
        .lock()
        .unwrap()
        .push("setup: the lock files never reached every node".into());
}

/// Plan 30 §M11: write `pairs` data names in `home`, each followed by a
/// marker in `other` registered for the watchers between the two writes.
#[allow(clippy::too_many_arguments)]
async fn marker_writer(
    cluster: Arc<Cluster>,
    history: Arc<History>,
    node: NodeId,
    thread: u64,
    home: String,
    other: String,
    pairs: u64,
    markers: Arc<Mutex<Vec<(String, String)>>>,
    abandoned: Arc<Mutex<HashSet<Rid>>>,
) {
    for k in 0..pairs {
        let data = format!("{home}/m{node}-{k}-data");
        let marker = format!("{other}/m{node}-{k}-marker");
        for name in [data.clone(), marker.clone()] {
            let handle = cluster.get(node);
            if !handle.alive() {
                return;
            }
            let rid = handle.next_rid();
            let op = NsOp::Create(name.clone());
            let mop = mutate_op(&handle.meta, &op);
            history.invoke(thread, rid, op);
            let mut acked = false;
            for _ in 0..MAX_RESUBMITS {
                let handle = cluster.get(node);
                if !handle.alive() {
                    break;
                }
                match handle.submit(rid, mop.clone()).await {
                    Ok(ClientReply::Outcome(outcome)) => {
                        if let Ok(ret) = ns_ret(&outcome) {
                            history.ret(thread, rid, ret);
                            acked = ret == NsRet::Ok;
                        }
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
            if !acked {
                abandoned.lock().unwrap().insert(rid);
                return;
            }
            if name == data {
                markers.lock().unwrap().push((marker.clone(), data.clone()));
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Plan 30 §M11: on `node`, every registered marker that is visible must
/// have its data visible in the same snapshot.
async fn marker_watcher(
    cluster: Arc<Cluster>,
    node: NodeId,
    markers: Arc<Mutex<Vec<(String, String)>>>,
    stats: Arc<Mutex<(u64, u64)>>,
    failures: Arc<Mutex<Vec<String>>>,
) {
    for _ in 0..600 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let handle = cluster.get(node);
        if !handle.alive() {
            continue;
        }
        let pairs = markers.lock().unwrap().clone();
        for (marker, data) in pairs {
            let (mp, ml) = split_name(&handle.meta, &marker);
            let (dp, dl) = split_name(&handle.meta, &data);
            let marker_seen = handle.meta.child_ino(mp, &ml).ok().flatten().is_some();
            let data_seen = handle.meta.child_ino(dp, &dl).ok().flatten().is_some();
            // A read of the data waits while a stranded op queued for
            // replay touches it, as a FUSE read does (`session_wait`): a
            // stream-ahead transaction of a deposed holder is rolled back
            // here until its successor re-ships it from the backup tail,
            // and a reader never observes that gap (long-delegated-late-
            // answers seed 3498: the delegate executed the marker on the
            // streamed data; the takeover's marker segment stranded the
            // data, and the successor, paused, re-shipped it 4.5 s later;
            // main fails the seed the same way).
            let data_waits = !data_seen
                && handle
                    .meta
                    .replay_touches(&[constellation_meta::ReadKey::Dentry(dp, dl.clone())]);
            let mut s = stats.lock().unwrap();
            s.0 += 1;
            if marker_seen && !data_seen && !data_waits {
                s.1 += 1;
                failures.lock().unwrap().push(format!(
                    "node {node} saw marker {marker} without its data {data}"
                ));
            }
        }
    }
}

/// Plan 30 §M11: a client's home directory (see the workload loop).
fn home_dir(dirs: &[String], node: NodeId, client: u64) -> String {
    if dirs.is_empty() {
        return String::new();
    }
    let base = if node == 1 {
        None
    } else {
        Some(((node - 2) as usize) % dirs.len())
    };
    match (base, client) {
        (None, 0) => String::new(),
        (None, k) => dirs[((k - 1) as usize) % dirs.len()].clone(),
        (Some(b), 0) => dirs[b].clone(),
        (Some(b), k) => dirs[(b + k as usize) % dirs.len()].clone(),
    }
}

thread_local! {
    /// This thread's run start, for [`SimTime`].
    static SIM_START: std::cell::Cell<Option<tokio::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

/// Log timestamps in simulated milliseconds since the run started.
struct SimTime;

impl tracing_subscriber::fmt::time::FormatTime for SimTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        match SIM_START.with(|t| t.get()) {
            Some(start) if tokio::runtime::Handle::try_current().is_ok() => {
                write!(w, "t={}", start.elapsed().as_millis())
            }
            _ => write!(w, "t=?"),
        }
    }
}
