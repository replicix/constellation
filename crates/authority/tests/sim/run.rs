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
use constellation_meta::{LogRecord, Meta, MutateOp, MutateOutcome, Rid};
use constellation_types::Code;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    /// F1 (`cst-load-review` §5.2): the next event `node` handles (0: the
    /// lease holder) takes its `now`, then waits `for_ms` before the core
    /// decides with it — a step that waited for the store's writer lock,
    /// or a process stopped between its clock read and its writes. The
    /// node's other events queue meanwhile.
    StallStep { node: NodeId, for_ms: u64 },
    /// F1: on `node` (0: the lease holder), the next fast-path write that
    /// carries a fencing token passes its check and then waits for the
    /// store's writer lock behind a slow writer for `core_ms + after_ms`;
    /// the core's next step waits `core_ms` behind the same writer and
    /// goes first after it (priority writes): it handles its backlog —
    /// grant expiries, the next holder's request and writes — before the
    /// held write's transaction runs.
    WriterLockHold {
        node: NodeId,
        core_ms: u64,
        after_ms: u64,
    },
    /// F1, the executor that did not mint the grant: delegate `node`'s
    /// next tagged write on its own fast path (under a lock its root
    /// minted) passes its check at the write and stays in its transaction,
    /// pinned, for `after_ms` — a slow commit holding the writer lock;
    /// the first other writer on the node lets it commit first, as it
    /// would wait for it. Once it is held the node is cut from its root
    /// for `partition_ms`: the grant lapses and the root outwaits it, and
    /// the delegate's stream head must cover the held write before the
    /// root can grant on (the settle on its renewal).
    DelegateWriteHold {
        node: NodeId,
        partition_ms: u64,
        after_ms: u64,
    },
    /// Plan 30 M0's fault knob: delay the holder's forward replies by
    /// `delay_ms` for `for_ms` (bug A's trigger).
    SlowReplies { delay_ms: u64, for_ms: u64 },
    /// A brand-new node (fresh id, no durable state, no clients) joins
    /// and bootstraps from the log: the "freshly bootstrapped replica" of
    /// M4's convergence check.
    JoinFresh,
    /// Scripted S3 error codes from now on (M4's `faulty.rs` idea).
    S3Rule(Rule),
    /// Scripted S3 error codes for `for_ms`: then every rule on the same
    /// node, operation and pattern is dropped (`Bucket::unscript`).
    S3RuleFor { rule: Rule, for_ms: u64 },
    /// Plan 30 §M9: kill the holder's first listed backup (as the lease
    /// object names it); restart it after `restart_ms`.
    CrashBackup { restart_ms: Option<u64> },
    /// Plan 30 §M9: cut P2P between the holder and its first listed
    /// backup (S3 stays reachable to both) for `for_ms`.
    PartitionBackup { for_ms: u64 },
    /// Plan 43 C7-F F-a (review F3): cut P2P between `node` (a reader, with
    /// a strict read delegation from the holder: `SimConfig::reader_nodes`)
    /// and every other node for `for_ms`: it learns nothing of a seal and
    /// keeps serving from its delegation until it lapses, so the
    /// successor's floor is all that keeps it from acknowledging a write
    /// the reader then misses.
    IsolateReader { node: NodeId, for_ms: u64 },
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
    /// Plan 43 C1-3 (`backup-chain-ttl`): kill `node` the moment it holds a
    /// lease whose tenure took a predecessor's tail over (a seal) and has
    /// that tail journaled but unshipped (waiting up to `within_ms` for it);
    /// restart it after `restart_ms`. The tail then rests on its disk and
    /// in the old group's copies only (D2).
    CrashWhenTailUnshipped {
        node: NodeId,
        within_ms: u64,
        restart_ms: Option<u64>,
    },
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
    /// `who`'s runtime is held up for a seeded `for_ms` (a busy host, or
    /// every worker inside a slow blocking call) while its clock, its
    /// other threads and everyone else run on: its core handles nothing,
    /// its timers fire late and are handled before what peers sent it
    /// meanwhile, which is stamped (handed to its core) a seeded
    /// `stamp_lag_ms` later still — the daemon's P2P receive path runs on
    /// the same runtime, several tasks deep. Its driver tells its core
    /// once it runs again (`Event::OwnLag`, the daemon's lag probe). Its
    /// off-core heartbeat keeps beating unless `SimConfig::holder_beats`
    /// puts it on the runtime. Nothing is lost or stopped: a run under
    /// `SimConfig::live_holder` must see no seal and no fast takeover.
    RuntimeLag {
        who: LagOf,
        for_ms: (u64, u64),
        stamp_lag_ms: (u64, u64),
    },
    /// Plan 43 C7-H1: step one node's wall clock by `ms` (an NTP or
    /// operator step, a VM restore). Only its WALL reading moves: its
    /// honour clock follows a forward step once and ignores a backward
    /// one, its wait clock never moves (`sim/clock.rs`; `types::clock`'s
    /// P4). The step is the node's, so it survives the node's restarts.
    /// Not in `gen_faults`: the default mix is C7a's.
    ClockStep { who: StepOf, ms: i64 },
    /// Plan 43 C7-H1: once a node other than the lease holder is inside a
    /// lock's critical section, step its wall clock by `step_ms` and then
    /// cut it from every other node over P2P for `dur_ms`: the step makes
    /// it honour its grant on (backward) or lapse it early (forward), and
    /// the partition stops it renewing. `PartitionLocker` and
    /// `ClockStep { Locker }` would each pick a locker, possibly
    /// different ones; this picks one.
    PartitionLockerStepped { step_ms: i64, dur_ms: u64 },
    /// Plan 43 C7a: set `who`'s oscillator rate (ppm, signed) from now on: its
    /// boot, raw and monotonic clocks run at `1 + ppm/1e6` of true time; its
    /// wall clock does not (an NTP-disciplined host) unless
    /// `SimConfig::wall_follows_rate`. The rate is the host's, so it
    /// survives the node's restarts (`sim/clock.rs`).
    RateSkew { who: StepOf, ppm: i64 },
    /// Plan 43 C7a: `who`'s host suspends for `ms`: its core, its beat loop
    /// and its delegation lane loop handle nothing (they stand as for
    /// `Pause`, which models a stopped process whose clocks run on), its
    /// timers fire only after it resumes, and its raw and monotonic clocks
    /// stop while its boot clock and its wall run on: HONOUR counts the
    /// suspend (through `S`), WAIT does not.
    Suspend { who: StepOf, ms: u64 },
    /// Plan 43 C7a: kill `node` (0: the lease holder) and restart it after
    /// `restart_ms` on a new boot, with its disk: the elapsed clocks start
    /// again from 0, the boot id changes, and its wall is stepped by
    /// `step_ms` meanwhile.
    RebootNode {
        node: NodeId,
        restart_ms: u64,
        step_ms: i64,
    },
    /// Plan 43 C7a: kill the lease holder, step its wall by `step_ms` while it
    /// is down, and restart it after `restart_ms` on the same boot, with
    /// its disk (what a persisted wall deadline does not survive).
    CrashHolderStepped { restart_ms: u64, step_ms: i64 },
    /// The harness's `lock-failover` under `clock:step:+3000:6000,
    /// clock:step:-3000:12000`: once a node other than the lease holder is
    /// inside a lock's critical section, kill the lease holder (restart it
    /// after `restart_ms`), `after_ms` later step the wall clock of every
    /// other node by `step_ms`, and `back_after_ms` after that step them
    /// back. A forward step ends the windows the survivors honour early
    /// (their honour clock follows it): the locker has that much less time
    /// to find the successor and renew there.
    CrashHolderWhileLockedStepped {
        restart_ms: Option<u64>,
        after_ms: u64,
        step_ms: i64,
        back_after_ms: u64,
    },
    /// Plan 43 C7a (Appendix B row 14's floor note): kill the first node
    /// (within 10 s) that is a fast successor inside its takeover floor
    /// (`after_marker`: its marker has landed, the floor is armed) or that
    /// has claimed the lease and not yet finished its gate (the marker, the
    /// tail, the quarantine), and restart it after `restart_ms` with its
    /// disk, on the same boot.
    CrashSuccessorInFloor { restart_ms: u64, after_marker: bool },
}

/// Whose wall clock a [`FaultKind::ClockStep`] steps, resolved at the
/// fault's time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// `Backup` and `NonHolder` are there for the configurations of the later
// commits of plan 43 C7-H1; the present ones use `Node`, `Holder` and
// `Locker`.
#[allow(dead_code)]
pub enum StepOf {
    Node(NodeId),
    /// The lease holder (the lock owner, the root).
    Holder,
    /// The first listed backup.
    Backup,
    /// A node other than the holder that is inside a lock's critical
    /// section (waiting up to 5 s for one: `wait_for_locker`).
    Locker,
    /// The lowest live node that is not the holder.
    NonHolder,
}

/// Whose runtime a [`FaultKind::RuntimeLag`] holds up, as the lease
/// object names them at the fault's time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagOf {
    Holder,
    /// The first listed backup.
    Backup,
    /// The lowest live node that is not the holder (under `ack=s3`, a
    /// subscriber of its log stream).
    NonHolder,
}

/// Plan 41 D3 non-vacuity: a probe that does not do what the design says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeMutant {
    /// The design.
    None,
    /// Whatever the read shows counts as a proof (a takeover racing the
    /// read, or already done, is not noticed).
    IgnoresVerdict,
    /// The proof is dated at the read's return, not at its send (a read
    /// served before a takeover and returned long after it would justify a
    /// window that outlasts the successor's quarantine).
    DatedAtReturn,
}

/// Where a node's off-core heartbeat runs (the daemon's
/// `holder_alive_loop`), in the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HolderBeats {
    /// Not modelled: a holder's liveness is its core's appends and
    /// stream frames (every configuration before the runtime-lag ones).
    /// Their seal watches never learn a lease expiry from a beat, so
    /// they are always in the unknown-expiry arm of `Core::lag_discount`;
    /// with no `RuntimeLag` in them, no lapse is ever handed over and
    /// the arm is inert.
    #[default]
    Off,
    /// On a thread of its own, sending on the liveness lane: a
    /// [`FaultKind::RuntimeLag`] of the node does not hold it up (a
    /// `Pause`, the whole process stopped, does).
    Thread,
    /// A task of the node's runtime, as it was: held up by a
    /// [`FaultKind::RuntimeLag`] with everything else.
    Runtime,
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
    /// Plan 41 D3 review: how much slower the root probe's lease GETs are
    /// than the bucket's `s3_latency`, before being served and on their way
    /// back (`Bucket::probe_handle`). All of `s3_latency` lies before the
    /// request is served; the reply half of a slow read is what makes the
    /// time a proof is dated at matter.
    pub probe_reads: super::store::SlowReads,
    /// How much slower than `s3_latency` the lease GETs of every node's
    /// handle are, before being served and on their way back
    /// (`Bucket::set_lease_reads`).
    pub lease_reads: super::store::SlowReads,
    /// Plan 41 D3 review: fail a run in which a lane answered on the root's own
    /// proof of liveness a freshness window or more after the tenure was
    /// taken over (`Bucket::probe_late`).
    pub check_probe_dating: bool,
    pub p2p_delay: (u64, u64),
    pub p2p_drop: f64,
    /// Plan 43 C7-F F-c: the probability that a lock mirror (any, the old
    /// root's final one included) is lost, whatever the link: the
    /// successor then often holds an older table than the old root's last.
    pub lock_mirror_drop: f64,
    /// ... from this many ms into the run on (the mirrors before arrive).
    pub lock_mirror_drop_from_ms: u64,
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
    /// Plan 43 C7-H2: nodes whose clock is off by exactly this many ms
    /// (on top of any `clock_skew_ms` draw): a node ahead of its
    /// predecessor by more than the lease it takes over has left.
    pub clock_offsets: Vec<(NodeId, i64)>,
    /// Plan 43 C7a: the bound on every node's oscillator rate error that its
    /// protocol clock budgets, ppm (`Config::clock_rate_budget_ppm`): the
    /// production default, 1 000. The rate configurations set the budget they
    /// are fixed with (and fail on a tree that does not read it); 0 is the
    /// unbudgeted clock of before C7a.
    pub rate_budget_ppm: u32,
    /// Plan 43 C7a (commit 6, D7), C7b (commit 9): the default clock-fault
    /// mix, drawn from the seed on streams of their own (so the existing
    /// draws of a seed do not move): per node a constant oscillator rate
    /// within `min(rate_budget_ppm, 1000)` and a constant wall offset in
    /// `[-2 TTL, +2 TTL]` (`gen_clock_offsets`); 0 to 2 wall steps (each at
    /// most 10 s, at most 10 s in sum per node: no cross-node compare assumes
    /// an offset bound since C7b); at most one suspend of 0.3 to 3 s (several
    /// nodes only) and at most one reboot of a node (restart in 0.3 to 3 s on
    /// a new boot, its wall stepped within the same bound meanwhile). Where
    /// directories are delegated and locks taken, the lock cut and barriers
    /// across a delegation still compare walls (rows 9 and 30, C7b's commit
    /// 8 after C4): no offsets there, steps at most 150 ms and 200 ms in sum
    /// per node (`mix_step_bound_ms`). A configuration
    /// that injects clock faults, skew, offsets or rates of its own, or sets
    /// `live_holder` (no seal allowed), is left as it is, whatever this says;
    /// one that asserts exact timing sets `false`.
    pub clock_faults: bool,
    /// Plan 43 C7b (commit 9): the default mix's constant per-node wall
    /// offsets (`gen_clock_offsets`); `false` leaves them out (a tuning
    /// knob: `AUTHORITY_SIM_MIX_OFFSETS=0` on `sweep_config`).
    pub clock_mix_offsets: bool,
    /// Plan 43 C7b (commit 9): the most a wall step of the default mix
    /// takes, and the most a node's steps add up to, in ms
    /// (`AUTHORITY_SIM_MIX_STEP_MS` on `sweep_config`).
    pub clock_mix_step_ms: i64,
    /// Plan 43 C7a: `(node, ppm)`: that node's oscillator runs at
    /// `1 + ppm/1e6` of true time from the start (its wall does not).
    pub node_rates: Vec<(NodeId, i64)>,
    /// Plan 43 C7a: every node's wall follows its oscillator too (a host
    /// with no time service). Offsets then grow with the rate, which the
    /// cross-node compares (C7b) cannot take: not for sweeps.
    pub wall_follows_rate: bool,
    /// Plan 43 C7b (commit 7's evidence): each suspend of a node
    /// (`FaultKind::Suspend`) is counted short by this much (ms, at most
    /// the suspend's length) by the node's boot clock, which then counts a
    /// suspend only once the host resumes: the x86 RTC path, where no
    /// clocksource runs through suspend and the kernel times the sleep in
    /// whole seconds. HONOUR's `S` then under-reports the suspend by up to
    /// this. 0: the boot clock counts a suspend as it runs (every other
    /// configuration).
    pub suspend_accounting_short_ms: u64,
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
    /// Plan 45 follow-up: after an op, with this probability the client
    /// also links one of its names to another (`NsOp::Link`) or exchanges
    /// two (`NsOp::Exchange`), in the op's directory: hard links under
    /// every fault the configuration has (`+links`, `config_variant`).
    /// From its own generator, so a configuration without links replays
    /// exactly the schedules it had.
    pub link_ratio: f64,
    /// Plan 45 §12 O2: of the ops `link_ratio` adds, this share goes
    /// across directories: a `Link`, `Rename` or `Exchange` (a third
    /// each) between one of the client's names and a name of another
    /// directory of the workload (`/`, `dirs`, `link_dirs`), hence across
    /// a delegation boundary where one is configured. 0 (`+links-local`)
    /// keeps `+links`' schedules from before cross-directory links.
    pub cross_links: f64,
    /// Plan 45 §12 O2: directories under `/` created at setup only as the
    /// far side of cross-directory links (`cross_links`): nobody's home,
    /// never delegated. `+links` adds one to a configuration with no
    /// `dirs`, which otherwise has the root directory alone.
    pub link_dirs: Vec<String>,
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
    /// Plan 43 C7b: every forwarded op (`MutateRequest`) that carries a lock
    /// token and is sent to this node is delayed by an extra seeded `(lo,
    /// hi)` ms: an op that leaves its sender inside the window of the lock
    /// grants it carries and reaches its executor after the owner's outwait.
    pub request_delay_to: Option<(NodeId, (u64, u64))>,
    /// F1's non-vacuity knob: `false` switches the fencing-token check at
    /// the write off on every node (`LockTables::set_tag_check_at_write_for_tests`),
    /// which is the code before it — a write that waited for the writer
    /// lock past its grant's end lands.
    pub tag_check_at_write: bool,
    /// F1: a turn whose write was *acknowledged* (`Accepted`) is never
    /// excused as an unacknowledged late landing when a later holder reads
    /// an older turn than the newest acknowledged one — the application
    /// was told its write under the lock succeeded (the lock's lost
    /// update). The client counts its turn acknowledged only while its
    /// grant was honoured, which a write held past its grant's end is
    /// not.
    pub strict_lock_turns: bool,
    /// F1's other non-vacuity knob: `false` makes every node's
    /// `Meta::settle_tagged_writes` wait for nothing (the code before it):
    /// a position is reported while a tagged write that passed its check
    /// is still committing.
    pub settle_tagged_writes: bool,
    /// How long a lock client waits for its turn write's answer before it
    /// leaves it in doubt (`None`: 3 s, 30 s under `strict_lock_turns`).
    pub lock_write_wait_ms: Option<u64>,
    /// Where the holder's off-core heartbeat runs ([`HolderBeats`]).
    pub holder_beats: HolderBeats,
    /// Plan 43 C7-F F-a (review F3): nodes whose clients only read (the
    /// names their ops would have touched), so a node cut off from the
    /// others (`FaultKind::IsolateReader`) goes on reading from its
    /// delegation instead of blocking on a write.
    pub reader_nodes: Vec<NodeId>,
    /// Plan 43 C7-F F-a (review F3): `(node, start_ms, ops)`: that node's
    /// clients start after `start_ms` and run only the first `ops` of the
    /// `ops_per_client` they would have (the draws stay the same), so one
    /// node can write early and another late.
    pub client_plan: Vec<(NodeId, u64, usize)>,
    /// Plan 43 C7-F F-a0 L3: only the clients of these nodes take locks
    /// (`lock_ratio`); empty: every node's do. A node whose clients take
    /// none sends its tenure no lock request, which would mark its lease
    /// (`ensure_granting_marked`) whatever the grace says.
    pub lock_nodes: Vec<NodeId>,
    /// Every holder stays alive: a seal, a backup takeover or an
    /// `ack=s3` fast takeover fails the seed (the runtime-lag
    /// configurations: lag is latency, not death).
    pub live_holder: bool,
    /// Every beat of the off-core heartbeat is lost on the bus (its lane
    /// broken while appends get through): a backup never learns the
    /// holder's lease expiry from a beat.
    pub beats_lost: bool,
    /// After a runtime lag, the timers go to the core before what the
    /// node's P2P handlers stamped meanwhile (the order a driver that
    /// wakes for its timer before the handlers run would see). Off, the
    /// daemon's usual order: the handlers' stamps first
    /// (`Driver::hand_heard`), as unnumbered beats.
    pub timers_first_after_lag: bool,
    /// The driver before it handed the core its own pauses and stuck steps
    /// as lapses (`Event::OwnLag`, the daemon's lag probe and long-step
    /// hand-off): a holder that wakes from one takes its backups' queued
    /// acknowledgements for silence and removes them. For a pin whose
    /// scenario is such a removal.
    pub own_lapses_unreported: bool,
    /// Plan 43 C7-F F-a's negative control: every numbered beat's arrival
    /// stamp is replaced by its credit, `min(at, sent + M)` (what the seal
    /// watch counts: an instant of the *holder's* clock), before the core
    /// sees it, so the cap of a fast takeover's floor is computed from the
    /// credit. A holder whose clock is behind the successor's by more than
    /// M then ends the successor's floor early: this must fail
    /// `fast-takeover-skewed-successor`.
    pub beats_credited: bool,
    /// The numbered beats sent to a node while its runtime lags are lost
    /// (the daemon's shape when the holder's beat runtime closes the
    /// liveness lane to a frozen peer and re-dials it: what it sent
    /// meanwhile never arrives, and the backup's lapse is followed by
    /// silence, not by a backlog of beats). Appends, which the sync lane
    /// carries, are queued and stamped at the lapse's end as before.
    pub beats_lost_in_lag: bool,
    /// No holder drops a backup for want of acknowledgements
    /// (`backup_ack_timeouts`): lag costs no reconfiguration either.
    pub keeps_backups: bool,
    /// The off-core delegation renewal lane is modelled (a delegate's
    /// driver stuck in a step has its renewals sent for it,
    /// `deleg_lane_loop`). `false` is the code before it, the non-vacuity
    /// knob of the core-stall configurations.
    pub deleg_lane: bool,
    /// Plan 41 R2: the root's P2P service answers a delegation renewal off
    /// its stuck core (`deleg_lane::answer_root`, in the bus tap of
    /// `node.rs`). `false` is the code before it, the non-vacuity knob of
    /// the root-stall configurations (`delegated-core-stall-root-unlaned`).
    pub deleg_root_lane: bool,
    /// Plan 41 D3: a root refused for stale liveness reads its lease object
    /// off the core (`root_probe`, the bus tap of `node.rs`). `false` is the
    /// code before it: the fail-before of the backed-root configurations
    /// (`delegated-core-stall-root-backup-unprobed`).
    pub deleg_root_probe: bool,
    /// Test-only non-vacuity of the probe (never set by the daemon).
    pub root_probe_mutant: ProbeMutant,
    /// Plan 41 L2: the lock renewal lane (`lock_lane_loop`) renews each
    /// node's grants beside its core, from the certificate the core
    /// publishes. `false` is the code before it: the fail-before of the
    /// core-stall configurations (`locks-core-stall`).
    pub lock_lane: bool,
    /// Test-only non-vacuity: the lock lane ignores a fast tenure's
    /// freshness rule (`locks-core-stall-backup-unfresh` must find a
    /// violation).
    pub lock_lane_ignores_freshness: bool,
    /// Test-only non-vacuity: a recall does not close the slot of its
    /// generation (the transition R1's rule 2 requires), so a renewal
    /// answered off the core after the recall began extends the window
    /// the recall outwaits (`delegated-core-stall-root-recall`).
    pub skip_close_on_recall: bool,
    /// Plan 43 C0, I1 `acked_implies_committed`: an answer that no class
    /// of the plan's §4 covers (judged at its emission, `oracle.rs`) fails
    /// the run. Always counted (`Report::uncommitted_acks`); the flag only
    /// decides failure. Default off until C2 makes it hold everywhere.
    pub strict_commit: bool,
    /// I2 `one_identity_per_op`: a rid queued for execution on two nodes at
    /// once (b), or answered under a group class yet not completed
    /// verbatim in the log (c), fails the run.
    pub strict_identity: bool,
    /// I4 `unacked_fails_never_lands_late`: an op answered in doubt that
    /// lands in the log after an op its node invoked later fails the run.
    pub strict_settle: bool,
    /// I5 `speculation_only_lone`: an answered op rolled back whose answer
    /// was not of class Lone fails the run (`strict_durability` turns it
    /// on from C1).
    pub strict_lone: bool,
    /// A lone delegate's own `Lone` rows are within the lead of the
    /// committed prefix and behind no uncommitted gated row of its own
    /// (`Core::deleg_lone_held`, oracle `LoneLead`): a run with one that is
    /// not fails. On everywhere; counted either way.
    pub strict_lone_lead: bool,
    /// I6 `no_ttl_claim_of_a_live_lease` (plan 43 C7b): a lease CAS that
    /// takes another node's unreleased, unfenced lease without a permit
    /// while the old holder's own process still honours it (its
    /// `holder_expires_ms` less its margin, on its honour clock) fails the
    /// run. Always counted (`Report::lease_ttl_claims`,
    /// `lease_live_claims`); the flag only decides failure. On (the default)
    /// since the observation rule (plan 43 C7b, commit 3).
    pub strict_lease_claims: bool,
    /// Negative control: the stub that answered every `RebuildReplica`
    /// with `ok: false` (the sim's rebuild before plan 43 C0).
    pub rebuild_stub: bool,
    /// Test-only (`c0_i1_catches_a_planted_ungated_fast_path`): the sim's
    /// fast path ignores `Core::ack_gated`, so a holder answers its own op
    /// from its local disk while a backup is committed.
    pub plant_ungated_fast_path: bool,
    /// Plan 43 C1-3 (§5.8, Q1): the operator abandons the group a claim has
    /// waited for this long (simulated ms), as `constellation repair
    /// abandon-group --epoch E` does (`Control::AbandonGroup` on the node
    /// that waits); C2e: and the group of a delegation generation the root
    /// cannot end (`--gen G`, on the root). `None`: nobody does; a claim or a
    /// generation whose group is gone for good then ends the run
    /// `waiting_for_group`.
    pub abandon_after_ms: Option<u64>,
    /// Plan 43 C2e: the run is not quiescent while the root has a generation
    /// whose delegate is dead and which has neither ended nor been found
    /// waiting for its group (`delegation.gens[].waiting_for_group`; with an
    /// operator, `abandon_after_ms`, until it has ended): the settle lasts
    /// until the root has decided.
    pub await_dead_delegates: bool,
    /// Test-only (`c1_the_claim_check_catches_a_planted_commit_claim`): the
    /// sim's fast path claims `RootGroup` for a row that waited for nothing
    /// (a lone one).
    pub plant_claim_root_group_on_lone: bool,
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
    // Automatic hash-range splits are off by default (ADR-38); the
    // sims that cover them opt in, like the daemon's
    // `CONSTELLATION_DELEGATION_SPLIT=20`.
    c.placement_split_pct = 20;
    c
}

/// The placement on with the production default for splits: off.
pub fn placement_default_split_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = placement_core_config(node_id, incarnation);
    c.placement_split_pct = 0;
    c
}

/// Plan 30 §M11 phase 2b: delegates with backups (M9's backup
/// configuration: the bus RTT is in budget, so every delegate picks one).
pub fn deleg_backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    backup_core_config(node_id, incarnation)
}

/// The backup configuration with at most one backed generation per node
/// (`delegation_backed_gens_max`): `delegated-two-gens-one-backed`.
pub fn deleg_one_backed_gen_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = deleg_backup_core_config(node_id, incarnation);
    c.delegation_backed_gens_max = 1;
    c
}

/// [`deleg_one_backed_gen_core_config`] with one row per stream batch and
/// per backup append: a delegate's batches back up behind a slow or
/// absent peer (`delegated-two-gens-backlog`).
pub fn deleg_one_backed_gen_backlog_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = deleg_one_backed_gen_core_config(node_id, incarnation);
    c.delegation_stream_rows = 1;
    c
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
            probe_reads: super::store::SlowReads::default(),
            lease_reads: super::store::SlowReads::default(),
            check_probe_dating: false,
            p2p_delay: (1, 15),
            p2p_drop: 0.0,
            lock_mirror_drop: 0.0,
            lock_mirror_drop_from_ms: 0,
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
            clock_offsets: Vec::new(),
            rate_budget_ppm: 1_000,
            clock_faults: true,
            clock_mix_offsets: true,
            clock_mix_step_ms: 10_000,
            node_rates: Vec::new(),
            wall_follows_rate: false,
            suspend_accounting_short_ms: 0,
            strict_durability: false,
            rtts: Vec::new(),
            epoch_slack: 0,
            join_fresh: true,
            chunk_writes: 0.0,
            link_ratio: 0.0,
            cross_links: 0.0,
            link_dirs: Vec::new(),
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
            request_delay_to: None,
            tag_check_at_write: true,
            strict_lock_turns: false,
            settle_tagged_writes: true,
            lock_write_wait_ms: None,
            holder_beats: HolderBeats::Off,
            reader_nodes: Vec::new(),
            client_plan: Vec::new(),
            lock_nodes: Vec::new(),
            live_holder: false,
            beats_lost: false,
            timers_first_after_lag: false,
            own_lapses_unreported: false,
            beats_credited: false,
            beats_lost_in_lag: false,
            keeps_backups: false,
            deleg_lane: true,
            deleg_root_lane: true,
            deleg_root_probe: true,
            root_probe_mutant: ProbeMutant::None,
            // `AUTHORITY_SIM_NO_LOCK_LANE=1`: the code without the lock lane.
            lock_lane: std::env::var_os("AUTHORITY_SIM_NO_LOCK_LANE").is_none(),
            lock_lane_ignores_freshness: false,
            skip_close_on_recall: false,
            strict_commit: false,
            strict_identity: false,
            strict_settle: false,
            strict_lone: false,
            strict_lone_lead: true,
            strict_lease_claims: true,
            rebuild_stub: false,
            plant_ungated_fast_path: false,
            plant_claim_root_group_on_lone: false,
            abandon_after_ms: None,
            await_dead_delegates: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct Report {
    pub seed: u64,
    /// Plan 30 §M9: faults that exceeded the one-failure durability
    /// budget (`Cluster::note_takedown`): classified and printed
    /// (`SWEEP-BUDGET`), never waived since plan 43 C1-3 (the claim of a
    /// lease whose group may hold acknowledged rows carries a member's
    /// sealed tail or waits for one).
    pub double_faults: Vec<String>,
    /// Plan 43 C1-3: the run ended with a claim still waiting for the tail
    /// of a group none of whose members answers (after the end-of-run heal
    /// and settle): the convergence checks are skipped and every
    /// acknowledged rid must be held somewhere instead
    /// (`acked_held_somewhere`).
    pub waiting_for_group: bool,
    /// Plan 43 C2e: acknowledged rows missing from the log in a run where
    /// the operator abandoned a delegation's group (`abandon_after_ms`).
    pub operator_lost: usize,
    pub ops_invoked: usize,
    pub ops_returned: usize,
    /// Plan 45 follow-up (`SimConfig::link_ratio`): links and exchanges
    /// that returned `Ok`.
    pub links_ok: usize,
    pub exchanges_ok: usize,
    /// Plan 45 §12 O2: links, renames and exchanges between two
    /// directories that returned `Ok`.
    pub cross_dir_ok: usize,
    pub tentative: usize,
    pub segments: usize,
    pub fenced: usize,
    pub commits: usize,
    pub s3_puts: u64,
    /// Plan 41 D3 review: lane answers decided on the root's own proof of
    /// liveness after the lease object named a successor (the race the
    /// send-time dating of the proof exists for).
    pub probe_races: u64,
    pub s3_gets: u64,
    pub p2p_sent: u64,
    pub converged_checked: bool,
    /// M12 round 2: ops the modelled FUSE fast path executed
    /// (`SimConfig::fast_path`).
    pub fast_path_executed: u64,
    /// F1 (`WriterLockHold`): fast-path writes held behind the slow
    /// writer, and those then refused at the write or executed.
    pub held_writes: u64,
    pub held_refused: u64,
    pub held_accepted: u64,
    /// Plan 43 C2d: delegate fast-path writes that waited for their group,
    /// and those answered once it held the row.
    pub fast_waited: u64,
    pub fast_answered: u64,
    /// F1: tagged writes refused only by the check at the write, and
    /// positions that waited for a tagged write in its transaction
    /// (`LockStats`, summed over the nodes' last incarnations).
    pub token_rejections_at_write: u64,
    pub settle_waits: u64,
    /// Plan 43 C7b: tokens judged exactly (the grant's record live here) and
    /// by their window alone, by path (`TokenPath`: p2p, local, inbox,
    /// replay), summed over the nodes' last incarnations.
    pub tokens_judged_exact: [u64; 4],
    pub tokens_judged_window: [u64; 4],
    /// Plan 43 C7b, I6: claims of another node's unreleased, unfenced lease
    /// without a permit, those inside the old holder's honour window (the
    /// violations), permitted claims of a `Backup` lease whose holder still
    /// honoured it (seals of a live holder), and all permitted claims.
    pub lease_ttl_claims: u64,
    pub lease_live_claims: u64,
    pub seals_of_a_live_holder: u64,
    pub permitted_claims: u64,
    /// The generic tester ran too (few tentative ops).
    pub stateright_checked: bool,
    /// Refusals explained only by an acked-then-rolled-back effect (L2).
    pub observed_tentative: usize,
    /// Plan 30 §M6: the per-node session checks.
    pub sessions: super::session::SessionReport,
    /// Plan 30 §M8: close-to-open (enforced under `strict`).
    pub cto: super::cto::CtoReport,
    pub stats: BTreeMap<NodeId, Stats>,
    /// Plan 41 L2: what each node's lock lane did (`RenewLane::stats`).
    pub lane: BTreeMap<NodeId, constellation_authority::lane::LaneStats>,
    pub faults: Vec<String>,
    pub simulated_ms: u64,
    /// Plan 30 §M9: acknowledged ops that were rolled back at some point
    /// (exempted as tentative unless `strict_durability`).
    pub acked_rolled_back: usize,
    /// Plan 30 §M9: per holder crash, simulated ms from the crash to the
    /// first mutation any node acknowledged afterwards.
    pub failover_ms: Vec<u64>,
    /// Plan 43 C7-F: the length of every takeover floor armed in the run
    /// (simulated ms from the marker's landing), over all nodes and
    /// incarnations.
    pub floor_ms: Vec<u64>,
    /// Plan 43 C7-F: times a beat guard that had been published as a
    /// finite non-zero value rose later in its epoch (see
    /// `Shared::guard_seen`).
    pub guard_rises: u64,
    /// Of those, rises of the guard's base (its floor and quarantines),
    /// not a subtree grace or a hold-era grant: asserted 0.
    pub guard_base_rises: u64,
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
    /// Plan 43 C0: the commit-rule oracles' counters (`oracle.rs`); each is
    /// always counted, and fails the run only under its strict flag.
    ///
    /// I1: non-relay answers by the class that held at their emission
    /// (`"none"`: none did), the answers no class covered by
    /// `"{expected class}/{answer}"`, and refusals that journaled no
    /// `Refused` by code (D6: nothing executed, so not an I1 violation).
    pub acks_by_class: BTreeMap<String, usize>,
    pub uncommitted_acks: BTreeMap<String, usize>,
    pub unjournaled_refusals: BTreeMap<String, usize>,
    /// Plan 43 C2a, I1: judged answers whose rid a transaction carries with
    /// another outcome (by answer kind); relays with no judged answer of
    /// the sequencer they name behind them; answers whose claimed class was
    /// not the class judged (informational); outcomes no sequencer decided
    /// (a node's own error, not judged); and unjournaled refusals whose rid
    /// the log completes (D6 of C0: fails under `strict_settle`).
    pub mismatched_acks: BTreeMap<String, usize>,
    pub unverified_relays: usize,
    pub class_mismatch: usize,
    /// Plan 43 C1-2: answers that claim a commit no class bears out, or
    /// whose row is tagged with another class than the answer names
    /// (fails under `strict_commit`).
    pub class_claim_mismatch: usize,
    pub acks_unsourced: usize,
    pub unjournaled_landed: usize,
    /// Answers with no recorded request (must be 0).
    pub acks_unmapped: usize,
    /// I2 (b): rids queued on two nodes at once; (c): answered under a
    /// group class but not completed verbatim in the log.
    pub identity_copies: usize,
    /// Plan 43 C1-4: rids queued on two nodes where no copy stands for a
    /// committed answer (the lone branch's, the delegations': C2/C3).
    pub identity_copies_ungated: usize,
    /// Plan 43 C1-4: nodes holding member-copy rows at convergence (must be
    /// 0; fails a `strict_durability` run).
    pub member_copy_rows_left: usize,
    pub non_verbatim: usize,
    /// I4: in-doubt rids that landed after an op invoked later.
    pub late_landings: usize,
    /// I5: acknowledged, rolled-back ops whose answer was not Lone.
    pub non_lone_rollbacks: usize,
    /// Deposition rebuilds run, `(succeeded, failed)`, over every
    /// incarnation of every node.
    pub rebuilds: (u64, u64),
    /// The most backups any node's lease listed at once.
    pub max_backups: usize,
    /// The first five violations of each oracle.
    pub oracle_examples: Vec<String>,
    /// A lone delegate's own `Lone` rows more than the lead past the
    /// committed prefix, and behind an uncommitted gated row of its own
    /// (`LoneLead`); and the distinct own rows judged, `Lone` and gated.
    pub lone_past_lead: usize,
    pub lone_behind_gated: usize,
    pub lone_rows_judged: usize,
    pub gated_rows_judged: usize,
    /// Non-vacuity of the gated mark: own ops it alone held back from
    /// `Lone` at the core's decision, and `Lone` decisions the store made
    /// `DelegGroup` where it journaled the row.
    pub lone_held_by_gate: u64,
    pub lone_demoted_at_journal: u64,
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
    /// When each node a fault cut from S3 reaches it again (simulated
    /// unix ms): a live copy that cannot put its rows in the log.
    pub s3_cut: Mutex<BTreeMap<NodeId, i64>>,
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
        let back = back.unwrap_or(i64::MAX);
        self.down.lock().unwrap().insert(node, back);
        self.check_failure_budget(node, "taken down", at_ms).await;
    }

    /// `node` is cut from S3 until `until` (simulated unix ms): alive, but
    /// a copy that can put nothing in the log — the second failure when
    /// the other copy is taken down meanwhile, as one taken down is
    /// (backup-lag-at-expiry seeds 5, 275, 277 with a `Pause` of the
    /// backup: the holder cut for 9 s, its backup sealed and then paused
    /// past the claim grace; the next holder claimed by TTL, and the
    /// rows both held came back only as replays by rid).
    pub async fn note_s3_cut(&self, node: NodeId, until: i64, at_ms: u64) {
        if !self.ids().contains(&node) {
            return;
        }
        self.s3_cut.lock().unwrap().insert(node, until);
        self.check_failure_budget(node, "cut from S3", at_ms).await;
    }

    /// A cut of `node` ended: the bucket's cut is one flag, so this heals
    /// any overlapping cut of it too.
    pub fn note_s3_healed(&self, node: NodeId) {
        let now = self.env.clock.wall().0;
        self.s3_cut.lock().unwrap().insert(node, now);
    }

    /// See [`Cluster::note_takedown`]: `node` just failed (`what`); record
    /// the run as beyond the budget if every copy of the rows it shares is
    /// now unable to put them in the log until past the point others may
    /// claim the lease.
    async fn check_failure_budget(&self, node: NodeId, what: &str, at_ms: u64) {
        let now = self.env.clock.wall().0;
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
        // When `id` is back up, and when it reaches S3 again.
        let back_at = |id: NodeId| -> (i64, i64) {
            if !ids.contains(&id) {
                return (i64::MAX, i64::MAX);
            }
            let down = self.down.lock().unwrap().get(&id).copied();
            let cut = self.s3_cut.lock().unwrap().get(&id).copied();
            let n = self.get(id);
            let up = match down {
                Some(b) if b > now => b,
                _ if !n.alive() => i64::MAX,
                _ => now,
            };
            (up, cut.filter(|c| *c > now).unwrap_or(now))
        };
        // One failure is within the budget: only a failure while every
        // other copy is already down (or cut from S3) can exceed it.
        if copies.iter().any(|c| {
            let (up, s3) = back_at(*c);
            *c != node && up.max(s3) <= now
        }) {
            return;
        }
        // A backup that is back must still hear nothing from the holder
        // for `backup_takeover_ms` before it may seal (its only evidence
        // of the holder's death); the lease's own holder re-adopts it. A
        // backup that is up and only lost S3 may have heard that silence
        // all along: it is counted as claiming once it reaches S3 again
        // (the earliest it could, so a cut never overstates its absence).
        let seal_ms = (self.env.config)(node, 0).backup_takeover_ms as i64 + self.claim_ms;
        let first_back = copies
            .iter()
            .map(|c| {
                let (up, s3) = back_at(*c);
                if *c == lease.holder {
                    up.max(s3)
                } else if up > now || s3 <= now {
                    up.saturating_add(seal_ms)
                        .max(s3.saturating_add(self.claim_ms))
                } else {
                    s3.saturating_add(self.claim_ms)
                }
            })
            .min()
            .unwrap_or(now);
        // The expiry is a stamp of the holder's HONOUR clock (C7-H1): in true
        // time (this check's `now`) it is that less how far the holder's honour
        // reading stands above true time, its offset and its forward steps (a
        // backward step moves it not at all). Plan 43 C7b, commit 9: the
        // default mix offsets nodes by up to 2 TTL.
        let honour_error = if ids.contains(&lease.holder) {
            self.get(lease.holder).clock.peek_honour().0 - now
        } else {
            0
        };
        let expires_true = lease.expires_unix_ms - honour_error;
        // Nobody else may claim a `Backup` lease before its expiry plus
        // the backups' grace (`LeaseState::classify`).
        let claimable_by_others = if lease.ack_policy == constellation_store_s3::AckPolicy::Backup
            && !lease.backups.is_empty()
        {
            expires_true
                + constellation_authority::core::backup_claim_grace_ms(&(self.env.config)(node, 0))
        } else {
            expires_true
        };
        if first_back > claimable_by_others {
            self.budget_exceeded.lock().unwrap().push(format!(
                "t={at_ms} node {node} {what}; the copies {copies:?} of holder {}'s acknowledged rows are all down until {} ms past the point others may claim its lease",
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
        let (tentative, refusals, floors) = self
            .nodes
            .lock()
            .unwrap()
            .get(&id)
            .map(|old| {
                (
                    old.shared.tentative.lock().unwrap().clone(),
                    old.shared.journaled_refusals.lock().unwrap().clone(),
                    old.shared.floor_ms.lock().unwrap().clone(),
                )
            })
            .unwrap_or_default();
        let rebuilds = self.nodes.lock().unwrap().get(&id).map(|old| {
            (
                old.shared
                    .rebuilds_ok
                    .load(std::sync::atomic::Ordering::Relaxed),
                old.shared
                    .rebuilds_failed
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
        });
        let handle = NodeHandle::start_with(&self.env, id, meta, epoch);
        if let Some((ok, failed)) = rebuilds {
            let ord = std::sync::atomic::Ordering::Relaxed;
            handle.shared.rebuilds_ok.store(ok, ord);
            handle.shared.rebuilds_failed.store(failed, ord);
        }
        handle.shared.tentative.lock().unwrap().extend(tentative);
        *handle.shared.floor_ms.lock().unwrap() = floors;
        // (Kept only with the journal: a fresh replica has none to drop.)
        if meta_kept {
            *handle.shared.journaled_refusals.lock().unwrap() = refusals;
        }
        self.nodes.lock().unwrap().insert(id, Arc::new(handle));
    }

    pub fn crash(&self, id: NodeId) -> Arc<Meta> {
        let node = self.get(id);
        node.crash(&self.env.bus);
        // Plan 43 C0: the process's executing set dies with it; its
        // persisted replay queue runs again at restart.
        self.env.oracles.identity.lock().unwrap().crash(id);
        // Plan 30 §M14: the process's lock state dies with it.
        self.locks.drop_node(id, self.env.clock.elapsed_ms());
        super::locks::reset_node(&node.meta, &self.locks.inos(), node.clock.now());
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

/// `None`: a `Link` whose source name this replica does not have (the
/// kernel's `link` fails its lookup before anything is sent).
fn mutate_op(meta: &Meta, op: &NsOp) -> Option<MutateOp> {
    Some(match op {
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
        NsOp::Link(a, b) => {
            let (from, leaf) = split_name(meta, a);
            let ino = meta.child_ino(from, &leaf).ok().flatten()?;
            let (parent, name) = split_name(meta, b);
            MutateOp::Link { ino, parent, name }
        }
        NsOp::Exchange(a, b) => {
            let (parent, name) = split_name(meta, a);
            let (new_parent, new_name) = split_name(meta, b);
            MutateOp::Exchange {
                parent,
                name,
                new_parent,
                new_name,
            }
        }
        NsOp::Put(_) => unreachable!("Put is a projection-only op"),
        NsOp::Same(..) => unreachable!("Same is a check-only op"),
    })
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
    /// `SimConfig::reader_nodes`: a read after the client's pace.
    PacedRead(String),
    /// `SimConfig::client_plan`: the client starts after this many ms.
    Delay(u64),
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
        let now = handle.clock.honour();
        let ask = |handle: &NodeHandle| {
            handle.control(constellation_authority::Control::ReadIndex {
                ino: parent,
                dir: true,
                name: Some(leaf.clone()),
                // With the read-ahead on, the sim's recalls and
                // quarantines also cover the children a lookup's
                // answer delegates.
                ahead: constellation_authority::ReadAhead::Listing,
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
        now = handle.clock.wall().0,
        name,
        present,
        timed_out,
        ?floor,
        held = handle.meta.read_delegations().valid(parent, handle.clock.honour()).is_some(),
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
            Step::Delay(ms) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                continue;
            }
            Step::PacedRead(name) => {
                tokio::time::sleep(Duration::from_millis(pace_ms)).await;
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
            (Some(op), _) => match mutate_op(&handle.meta, op) {
                Some(mop) => mop,
                None => continue,
            },
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
                                "t={} client t{thread} rid({},{},{}) {ret:?} attempts={attempts} {op:?}",
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

/// `SimConfig::reader_nodes`: a read of every name `ops` would have
/// touched.
fn reads_only(ops: Vec<NsOp>) -> Vec<Step> {
    ops.into_iter()
        .map(|op| match op {
            NsOp::Create(n)
            | NsOp::Unlink(n)
            | NsOp::Put(n)
            | NsOp::Rename(n, _)
            | NsOp::Link(_, n)
            | NsOp::Exchange(n, _)
            | NsOp::Same(n, _) => Step::PacedRead(n),
        })
        .collect()
}

/// Plan 30 §M6: interleave reads with `ops`.
fn with_reads(rng: &mut StdRng, ops: Vec<NsOp>, names: usize, ratio: f64) -> Vec<Step> {
    let mut out = Vec::new();
    for op in ops {
        let touched = match &op {
            NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => n.clone(),
            NsOp::Rename(a, b) | NsOp::Link(a, b) | NsOp::Exchange(a, b) | NsOp::Same(a, b) => {
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

/// Plan 45 §12 O2: the journal row each completed rid's `Completed`
/// record was shipped from (`history::JournalOrder`): a segment's `rows`
/// pair with its non-`Atime` records.
fn journal_order(oracle: &check::Oracle) -> super::history::JournalOrder {
    let mut out = super::history::JournalOrder::new();
    for (rid, (seq, idx)) in &oracle.completed_at {
        let Some(seg) = oracle.unfenced.get(seq) else {
            continue;
        };
        let j = seg.records[..*idx]
            .iter()
            .filter(|rec| !matches!(rec, LogRecord::Atime { .. }))
            .count();
        if let Some(row) = seg.rows.get(j) {
            out.insert(*rid, (seg.epoch, seg.node, *row));
        }
    }
    out
}

/// Plan 45 follow-up: a rename that completed in the log without its
/// `Rename` record found both names on one inode (a hard link, POSIX's
/// no-op): it becomes `NsOp::Same` for the checks, which cannot tell
/// names of one inode apart from names of two. The log is the witness:
/// an executed rename's record sits directly before its `Completed`.
fn same_inode_renames(
    events: Vec<super::history::HistEvt>,
    oracle: &check::Oracle,
) -> Vec<super::history::HistEvt> {
    use super::history::HistEvt;
    let leaf = |n: &str| n.rsplit('/').next().unwrap_or(n).to_string();
    let executed = |rid: &Rid, a: &str, b: &str| -> bool {
        let Some((seq, idx)) = oracle.completed_at.get(rid) else {
            return true;
        };
        let Some(seg) = oracle.unfenced.get(seq) else {
            return true;
        };
        idx.checked_sub(1)
            .and_then(|i| seg.records.get(i))
            .is_some_and(|rec| {
                matches!(rec, LogRecord::Rename { name, new_name, .. }
                    if *name == leaf(a) && *new_name == leaf(b))
            })
    };
    events
        .into_iter()
        .map(|e| match e {
            HistEvt::Invoke {
                thread,
                rid,
                op: NsOp::Rename(a, b),
            } if !executed(&rid, &a, &b) => HistEvt::Invoke {
                thread,
                rid,
                op: NsOp::Same(a, b),
            },
            other => other,
        })
        .collect()
}

/// Plan 45 follow-up (`SimConfig::link_ratio`): after each op, with
/// `ratio`, a `Link` or an `Exchange` (half each) of two distinct names
/// of this client's plan in the op's directory. Drawn from its own
/// generator, like `with_writes`.
///
/// Plan 45 §12 O2: with `cross` of those, the op goes across directories
/// instead: a `Link`, `Rename` or `Exchange` (a third each, either way
/// round) between the op's name and `f<i>` (`i < names`, the names
/// `gen_ops_in` gives every directory's clients) of another directory in
/// `dirs`. `cross` 0 draws nothing more, so its schedules are the ones
/// `+links` had before.
fn with_links(
    rng: &mut StdRng,
    steps: Vec<Step>,
    ratio: f64,
    cross: f64,
    dirs: &[String],
    names: usize,
) -> Vec<Step> {
    if ratio <= 0.0 {
        return steps;
    }
    let mut pool: Vec<String> = steps
        .iter()
        .filter_map(|s| match s {
            Step::Op(NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Rename(n, _)) => Some(n.clone()),
            _ => None,
        })
        .collect();
    pool.sort();
    pool.dedup();
    let mut out = Vec::new();
    for step in steps {
        let at = match &step {
            Step::Op(NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Rename(n, _)) => Some(n.clone()),
            _ => None,
        };
        out.push(step);
        let Some(a) = at else {
            continue;
        };
        if !rng.random_bool(ratio.min(1.0)) {
            continue;
        }
        let others: Vec<&String> = dirs.iter().filter(|d| d.as_str() != dir_of(&a)).collect();
        if cross > 0.0 && !others.is_empty() && rng.random_bool(cross.min(1.0)) {
            let d = others.choose(rng).unwrap();
            let i = rng.random_range(0..names.max(1));
            let b = if d.is_empty() {
                format!("f{i}")
            } else {
                format!("{d}/f{i}")
            };
            let (a, b) = if rng.random_bool(0.5) { (a, b) } else { (b, a) };
            out.push(Step::Op(match rng.random_range(0..3) {
                0 => NsOp::Link(a, b),
                1 => NsOp::Rename(a, b),
                _ => NsOp::Exchange(a, b),
            }));
            continue;
        }
        let siblings: Vec<&String> = pool
            .iter()
            .filter(|n| **n != a && dir_of(n) == dir_of(&a))
            .collect();
        let Some(b) = siblings.choose(rng).map(|b| (*b).clone()) else {
            continue;
        };
        let (a, b) = if rng.random_bool(0.5) { (a, b) } else { (b, a) };
        out.push(Step::Op(if rng.random_bool(0.5) {
            NsOp::Link(a, b)
        } else {
            NsOp::Exchange(a, b)
        }));
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
        mtime_ns: None,
        ctime_ns: None,
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

/// Whether a configuration injects clock faults, skew, offsets or rates of
/// its own, or allows no seal at all (`live_holder`: a rebooted or suspended
/// holder is silent and is rightly sealed): the default mix leaves it alone.
fn injects_clock_faults(cfg: &SimConfig) -> bool {
    cfg.live_holder
        || !cfg.node_rates.is_empty()
        || cfg.clock_skew_ms != 0
        || !cfg.clock_offsets.is_empty()
        || cfg.wall_follows_rate
        || cfg.faults.iter().any(|f| {
            matches!(
                f.kind,
                FaultKind::ClockStep { .. }
                    | FaultKind::RateSkew { .. }
                    | FaultKind::Suspend { .. }
                    | FaultKind::RebootNode { .. }
                    | FaultKind::CrashHolderStepped { .. }
                    | FaultKind::CrashHolderWhileLockedStepped { .. }
                    | FaultKind::PartitionLockerStepped { .. }
            )
        })
}

/// Plan 43 C7b (commit 9): the largest wall step the default mix draws, and
/// the most a node's steps add up to, in ms. Since C7b no cross-node compare
/// assumes an offset bound, so a step is limited by what the run can take
/// (ten seconds: longer than a lease's TTL); where directories are delegated
/// and locks taken, the lock cut and the barriers across a delegation
/// (plan 43 Appendix B rows 9 and 30) still compare walls until C7b's
/// commit 8 (after C4), and the steps and offsets stay within the margin's
/// half as C7a left them.
pub fn mix_step_bound_ms(cfg: &SimConfig) -> (i64, i64) {
    if offsets_wait_for_the_root_anchored_cut(cfg) {
        (150, 200)
    } else {
        (cfg.clock_mix_step_ms, cfg.clock_mix_step_ms)
    }
}

/// Whether the configuration has a lock cut or barriers across a delegation
/// (rows 9 and 30: C7b's commit 8, after C4).
fn offsets_wait_for_the_root_anchored_cut(cfg: &SimConfig) -> bool {
    !cfg.dirs.is_empty() && cfg.lock_files > 0
}

/// Plan 43 C7b (commit 9): the default mix's constant clock offsets of one
/// run: every node's wall off by a draw in `[-2 TTL, +2 TTL]` (the lease's TTL
/// from the configuration), from a stream of its own so the draws of
/// [`gen_clock_mix`] and `gen_faults` do not move. Empty where the mix is
/// off (a configuration with clock faults, skew, offsets or rates of its own,
/// or `clock_faults: false`) and where the lock cut still compares walls
/// across a delegation (see [`mix_step_bound_ms`]).
pub fn gen_clock_offsets(cfg: &SimConfig, seed: u64) -> Vec<(NodeId, i64)> {
    if !cfg.clock_faults
        || !cfg.clock_mix_offsets
        || cfg.nodes == 0
        || injects_clock_faults(cfg)
        || offsets_wait_for_the_root_anchored_cut(cfg)
    {
        return Vec::new();
    }
    let mut rng = StdRng::seed_from_u64(seed ^ 0xC7B0_0FF5);
    let ttl = (cfg.core)(1, 1).ttl_ms as i64;
    (1..=cfg.nodes)
        .map(|n| (n, rng.random_range(-2 * ttl..=2 * ttl)))
        .collect()
}

/// Plan 43 C7a (commit 6): the default clock-fault mix of one run
/// ([`SimConfig::clock_faults`]): the nodes' constant rates and the timed
/// faults. Drawn from a stream of its own, seeded from the run's, so the
/// draws `gen_faults` makes do not move.
pub fn gen_clock_mix(
    cfg: &SimConfig,
    seed: u64,
    horizon_ms: u64,
) -> (Vec<(NodeId, i64)>, Vec<ScheduledFault>) {
    let mut rates = Vec::new();
    let mut out = Vec::new();
    if !cfg.clock_faults || cfg.nodes == 0 || injects_clock_faults(cfg) {
        return (rates, out);
    }
    let mut rng = StdRng::seed_from_u64(seed ^ 0xC7A0_C10C);
    let budget = cfg.rate_budget_ppm.min(1_000) as i64;
    if budget > 0 {
        for n in 1..=cfg.nodes {
            rates.push((n, rng.random_range(-budget..=budget)));
        }
    }
    let (step_max, sum_max) = mix_step_bound_ms(cfg);
    let at = |rng: &mut StdRng| rng.random_range(500..horizon_ms.max(600));
    // The steps in force per node, cumulative: |sum| <= `sum_max`.
    let mut stepped: BTreeMap<NodeId, i64> = BTreeMap::new();
    let mut step = |rng: &mut StdRng, node: NodeId| -> i64 {
        let cur = stepped.entry(node).or_default();
        let ms = rng
            .random_range(-step_max..=step_max)
            .clamp(-sum_max - *cur, sum_max - *cur);
        *cur += ms;
        ms
    };
    for _ in 0..rng.random_range(0..=2) {
        let node = rng.random_range(1..=cfg.nodes);
        let ms = step(&mut rng, node);
        if ms != 0 {
            out.push(ScheduledFault {
                at_ms: at(&mut rng),
                kind: FaultKind::ClockStep {
                    who: StepOf::Node(node),
                    ms,
                },
            });
        }
    }
    if cfg.nodes > 1 && rng.random_bool(0.5) {
        let node = rng.random_range(1..=cfg.nodes);
        out.push(ScheduledFault {
            at_ms: at(&mut rng),
            kind: FaultKind::Suspend {
                who: StepOf::Node(node),
                ms: rng.random_range(300..=3_000),
            },
        });
    }
    let reboot = rng.random_bool(0.5);
    let node = rng.random_range(1..=cfg.nodes);
    let step_ms = step(&mut rng, node);
    let restart_ms = rng.random_range(300..=3_000);
    let at_ms = at(&mut rng);
    if reboot {
        out.push(ScheduledFault {
            at_ms,
            kind: FaultKind::RebootNode {
                node,
                restart_ms,
                step_ms,
            },
        });
    }
    (rates, out)
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
            let now = cluster.env.clock.wall().0;
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
            let now = cluster.env.clock.wall().0;
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
            let now = cluster.env.clock.wall().0;
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
            let now = cluster.env.clock.wall().0;
            cluster
                .note_s3_cut(node, now + for_ms as i64, fault.at_ms)
                .await;
            cluster.env.bucket.set_cut(node, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.set_cut(node, false);
            cluster.note_s3_healed(node);
        }
        FaultKind::RuntimeLag {
            who,
            for_ms,
            stamp_lag_ms,
        } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} runtime-lag: no lease yet", fault.at_ms));
                return;
            };
            let ids = cluster.ids();
            let live = |n: &NodeId| ids.contains(n) && cluster.get(*n).alive();
            let node = match who {
                LagOf::Holder => Some(lease.holder).filter(live),
                LagOf::Backup => lease.backups.first().copied().filter(live),
                LagOf::NonHolder => ids.iter().copied().find(|n| *n != lease.holder && live(n)),
            };
            let Some(node) = node else {
                note(format!(
                    "t={} runtime-lag {who:?}: nobody (holder {}, backups {:?})",
                    fault.at_ms, lease.holder, lease.backups
                ));
                return;
            };
            // Drawn from the seed and the fault's time: the same on every
            // replay, different per fault.
            let mut rng =
                StdRng::seed_from_u64(cluster.env.seed ^ fault.at_ms.wrapping_mul(0x9e37));
            let draw = |rng: &mut StdRng, (lo, hi): (u64, u64)| {
                if hi > lo {
                    rng.random_range(lo..=hi)
                } else {
                    lo
                }
            };
            let lag = draw(&mut rng, for_ms);
            let stamp = draw(&mut rng, stamp_lag_ms);
            note(format!(
                "t={} runtime lag on node {node} ({who:?} of holder {} epoch {}) for {lag}ms, \
                 peers stamped {stamp}ms after",
                fault.at_ms, lease.holder, lease.epoch
            ));
            let until = tokio::time::Instant::now() + Duration::from_millis(lag);
            cluster
                .get(node)
                .runtime_lag(until, until + Duration::from_millis(stamp));
        }
        FaultKind::Pause { node, for_ms } => {
            note(format!(
                "t={} pause node {node} for {for_ms}ms",
                fault.at_ms
            ));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(node, Some(now + for_ms as i64), fault.at_ms)
                .await;
            let until = tokio::time::Instant::now() + Duration::from_millis(for_ms);
            cluster.get(node).pause_until(until);
        }
        FaultKind::StallStep { node, for_ms } => {
            let node = match node {
                0 => match read_lease(&cluster.env.bucket).await {
                    Some(lease) => lease.holder,
                    None => return,
                },
                n => n,
            };
            if !cluster.ids().contains(&node) || !cluster.get(node).alive() {
                return;
            }
            note(format!(
                "t={} stall node {node}'s next step for {for_ms}ms",
                fault.at_ms
            ));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(node, Some(now + for_ms as i64), fault.at_ms)
                .await;
            cluster.get(node).stall_next_step(for_ms);
        }
        FaultKind::WriterLockHold {
            node,
            core_ms,
            after_ms,
        } => {
            let node = match node {
                0 => match read_lease(&cluster.env.bucket).await {
                    Some(lease) => lease.holder,
                    None => return,
                },
                n => n,
            };
            if !cluster.ids().contains(&node) || !cluster.get(node).alive() {
                return;
            }
            note(format!(
                "t={} hold node {node}'s next tagged fast-path write {}ms behind a slow writer (its core {core_ms}ms)",
                fault.at_ms,
                core_ms + after_ms
            ));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(
                    node,
                    Some(now + (core_ms + after_ms) as i64 + 5_000),
                    fault.at_ms,
                )
                .await;
            cluster
                .get(node)
                .hold_next_tagged_write(core_ms, core_ms + after_ms);
        }
        FaultKind::DelegateWriteHold {
            node,
            partition_ms,
            after_ms,
        } => {
            if !cluster.ids().contains(&node) || !cluster.get(node).alive() {
                return;
            }
            let root = read_lease(&cluster.env.bucket)
                .await
                .map_or(1, |lease| lease.holder);
            note(format!(
                "t={} hold delegate {node}'s next tagged write {after_ms}ms in its transaction, \
                 then cut it from root {root} for {partition_ms}ms",
                fault.at_ms
            ));
            cluster
                .get(node)
                .hold_next_delegate_write(root, partition_ms, after_ms);
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
        FaultKind::S3RuleFor { rule, for_ms } => {
            note(format!("t={} S3 rule for {for_ms}ms {rule:?}", fault.at_ms));
            let (node, op, pattern) = (rule.node, rule.op, rule.pattern.clone());
            cluster.env.bucket.script(rule);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.unscript(node, op, &pattern);
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
            let now = cluster.env.clock.wall().0;
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
            let now = cluster.env.clock.wall().0;
            cluster
                .note_s3_cut(lease.holder, now + for_ms as i64, fault.at_ms)
                .await;
            cluster.env.bucket.set_cut(lease.holder, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.set_cut(lease.holder, false);
            cluster.note_s3_healed(lease.holder);
        }
        FaultKind::ClockStep { who, ms } => {
            let Some(node) = resolve_step_of(&cluster, who, fault.at_ms, "clock step", &note).await
            else {
                return;
            };
            note(format!(
                "t={} clock step node {node} ({who:?}) by {ms}ms (wall)",
                cluster.env.clock.elapsed_ms()
            ));
            cluster
                .env
                .clock_state_of(node)
                .step()
                .fetch_add(ms, std::sync::atomic::Ordering::SeqCst);
        }
        FaultKind::RateSkew { who, ppm } => {
            let Some(node) = resolve_step_of(&cluster, who, fault.at_ms, "rate skew", &note).await
            else {
                return;
            };
            note(format!(
                "t={} rate skew node {node} ({who:?}) to {ppm:+} ppm",
                cluster.env.clock.elapsed_ms()
            ));
            cluster
                .env
                .clock_state_of(node)
                .set_rate(ppm, cluster.env.clock.elapsed_ns());
        }
        FaultKind::Suspend { who, ms } => {
            let Some(node) = resolve_step_of(&cluster, who, fault.at_ms, "suspend", &note).await
            else {
                return;
            };
            note(format!(
                "t={} suspend node {node} ({who:?}) for {ms}ms",
                cluster.env.clock.elapsed_ms()
            ));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(node, Some(now + ms as i64), fault.at_ms)
                .await;
            // The oscillator's raw and mono clocks stand still for the
            // interval; the process is held as for a `Pause`, and its lock
            // clients stand still too (`NodeHandle::suspend_until`).
            cluster
                .env
                .clock_state_of(node)
                .suspend(cluster.env.clock.elapsed_ns(), ms);
            let until = tokio::time::Instant::now() + Duration::from_millis(ms);
            cluster.get(node).suspend_until(until);
        }
        FaultKind::RebootNode {
            node,
            restart_ms,
            step_ms,
        } => {
            let node = match node {
                0 => match read_lease(&cluster.env.bucket).await {
                    Some(lease) => lease.holder,
                    None => {
                        note(format!("t={} reboot: no lease yet", fault.at_ms));
                        return;
                    }
                },
                n => n,
            };
            if node == 0 || !cluster.ids().contains(&node) || !cluster.get(node).alive() {
                note(format!("t={} reboot: node {node} not alive", fault.at_ms));
                return;
            }
            note(format!(
                "t={} reboot node {node} (back after {restart_ms}ms, wall stepped {step_ms}ms)",
                fault.at_ms
            ));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(node, Some(now + restart_ms as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(node);
            tokio::time::sleep(Duration::from_millis(restart_ms)).await;
            let state = cluster.env.clock_state_of(node);
            state
                .step()
                .fetch_add(step_ms, std::sync::atomic::Ordering::SeqCst);
            state.reboot(cluster.env.clock.elapsed_ns());
            cluster.restart(node, Some(meta));
        }
        FaultKind::CrashHolderStepped {
            restart_ms,
            step_ms,
        } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!(
                    "t={} crash-holder-stepped: no lease yet",
                    fault.at_ms
                ));
                return;
            };
            let holder = lease.holder;
            if holder == 0 || !cluster.ids().contains(&holder) || !cluster.get(holder).alive() {
                note(format!(
                    "t={} crash-holder-stepped: holder {holder} not alive",
                    fault.at_ms
                ));
                return;
            }
            note(format!(
                "t={} crash holder {holder} (back after {restart_ms}ms with its journal, wall stepped {step_ms}ms meanwhile)",
                fault.at_ms
            ));
            cluster
                .failovers
                .lock()
                .unwrap()
                .push((cluster.env.clock.elapsed_ms(), None));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(holder, Some(now + restart_ms as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(holder);
            tokio::time::sleep(Duration::from_millis(restart_ms)).await;
            cluster
                .env
                .clock_state_of(holder)
                .step()
                .fetch_add(step_ms, std::sync::atomic::Ordering::SeqCst);
            cluster.restart(holder, Some(meta));
        }
        FaultKind::CrashSuccessorInFloor {
            restart_ms,
            after_marker,
        } => {
            // The first live node that has armed a takeover floor
            // (`after_marker`) or has claimed the lease and not finished its
            // gate (the marker, the tail, the quarantine). Polled up to
            // 10 s, as `wait_for_locker` does.
            let mut victim = None;
            for _ in 0..2_000 {
                victim = cluster.ids().into_iter().find(|n| {
                    let h = cluster.get(*n);
                    let v = h.view();
                    h.alive()
                        && if after_marker {
                            h.in_takeover_floor()
                        } else {
                            v.gate_pending && v.held_epoch.is_some()
                        }
                });
                if victim.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let Some(node) = victim else {
                note(format!(
                    "t={} crash-successor-in-floor (after_marker {after_marker}): nobody",
                    fault.at_ms
                ));
                return;
            };
            note(format!(
                "t={} crash successor {node} (after_marker {after_marker}; back after {restart_ms}ms with its journal)",
                cluster.env.clock.elapsed_ms()
            ));
            let now = cluster.env.clock.wall().0;
            cluster
                .note_takedown(node, Some(now + restart_ms as i64), fault.at_ms)
                .await;
            let meta = cluster.crash(node);
            tokio::time::sleep(Duration::from_millis(restart_ms)).await;
            cluster.restart(node, Some(meta));
        }
        FaultKind::PartitionLockerStepped { step_ms, dur_ms } => {
            let Some(node) = wait_for_locker(&cluster).await else {
                note(format!(
                    "t={} partition-locker-stepped: no locker",
                    fault.at_ms
                ));
                return;
            };
            let others: Vec<NodeId> = cluster.ids().into_iter().filter(|n| *n != node).collect();
            note(format!(
                "t={} clock step locker {node} by {step_ms}ms (wall), then partition it from \
                 {others:?} for {dur_ms}ms",
                cluster.env.clock.elapsed_ms()
            ));
            cluster
                .env
                .clock_state_of(node)
                .step()
                .fetch_add(step_ms, std::sync::atomic::Ordering::SeqCst);
            for o in &others {
                cluster.env.bus.set_partition(node, *o, true);
            }
            tokio::time::sleep(Duration::from_millis(dur_ms)).await;
            for o in &others {
                cluster.env.bus.set_partition(node, *o, false);
            }
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
        FaultKind::CrashHolderWhileLockedStepped {
            restart_ms,
            after_ms,
            step_ms,
            back_after_ms,
        } => {
            if wait_for_locker(&cluster).await.is_none() {
                note(format!(
                    "t={} crash-holder-while-locked-stepped: no locker",
                    fault.at_ms
                ));
                return;
            }
            let holder = read_lease(&cluster.env.bucket)
                .await
                .map_or(0, |l| l.holder);
            note(format!(
                "t={} a locker is in its critical section: crash the holder {holder}, step \
                 the others by {step_ms}ms {after_ms}ms later",
                cluster.env.clock.elapsed_ms()
            ));
            let stepper = {
                let (cluster, log) = (cluster.clone(), log.clone());
                tokio::spawn(async move {
                    let step_all = |ms: i64| {
                        for n in cluster.ids().into_iter().filter(|n| *n != holder) {
                            cluster
                                .env
                                .clock_state_of(n)
                                .step()
                                .fetch_add(ms, std::sync::atomic::Ordering::SeqCst);
                        }
                        log.lock().unwrap().push(format!(
                            "t={} clock step of every node but {holder} by {ms}ms (wall)",
                            cluster.env.clock.elapsed_ms()
                        ));
                    };
                    tokio::time::sleep(Duration::from_millis(after_ms)).await;
                    step_all(step_ms);
                    tokio::time::sleep(Duration::from_millis(back_after_ms)).await;
                    step_all(-step_ms);
                })
            };
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
            let _ = stepper.await;
        }
        FaultKind::CrashWhenTailUnshipped {
            node,
            within_ms,
            restart_ms,
        } => {
            let mut waited = 0;
            let found = loop {
                let n = cluster.get(node);
                let v = n.view();
                if n.alive()
                    && v.held_epoch.is_some()
                    && v.stats.backup_tail_applied > 0
                    && v.journal_len > 0
                {
                    break true;
                }
                if waited >= within_ms {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                waited += 5;
            };
            if !found {
                note(format!(
                    "t={} crash-when-tail-unshipped: node {node} never held a tail",
                    fault.at_ms
                ));
                return;
            }
            note(format!(
                "t={} node {node} holds the predecessor's tail unshipped: crash it",
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
        FaultKind::IsolateReader { node, for_ms } => {
            let others: Vec<NodeId> = cluster
                .nodes
                .lock()
                .unwrap()
                .keys()
                .copied()
                .filter(|n| *n != node)
                .collect();
            note(format!(
                "t={} isolate reader {node} from {others:?} for {for_ms}ms",
                fault.at_ms
            ));
            for o in &others {
                cluster.env.bus.set_partition(node, *o, true);
            }
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            for o in &others {
                cluster.env.bus.set_partition(node, *o, false);
            }
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
    let now = cluster.env.clock.wall().0;
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

/// Resolves a [`StepOf`] at the fault's time to a live node (`what` names the
/// fault in the log lines); `None` (noted) if there is nobody.
async fn resolve_step_of(
    cluster: &Arc<Cluster>,
    who: StepOf,
    at_ms: u64,
    what: &str,
    note: &impl Fn(String),
) -> Option<NodeId> {
    let node = match who {
        StepOf::Node(n) => Some(n).filter(|n| cluster.ids().contains(n)),
        StepOf::Locker => wait_for_locker(cluster).await,
        StepOf::Holder | StepOf::Backup | StepOf::NonHolder => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={at_ms} {what}: no lease yet"));
                return None;
            };
            let ids = cluster.ids();
            let live = |n: &NodeId| ids.contains(n) && cluster.get(*n).alive();
            match who {
                StepOf::Holder => Some(lease.holder).filter(live),
                StepOf::Backup => lease.backups.first().copied().filter(live),
                _ => ids.iter().copied().find(|n| *n != lease.holder && live(n)),
            }
        }
    };
    if node.is_none() {
        note(format!("t={at_ms} {what} {who:?}: nobody"));
    }
    node
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
            let now_unix = n.clock.wall().0 / 1000;
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
    bucket.set_lease_reads(cfg.lease_reads);
    let bus = Bus::new(seed, cfg.p2p_delay, cfg.p2p_drop);
    bus.set_stream_faults(cfg.stream_faults.clone());
    bus.set_beats_lost(cfg.beats_lost);
    bus.set_mirror_drop(cfg.lock_mirror_drop, cfg.lock_mirror_drop_from_ms);
    bus.set_deleg_answer_delay(cfg.deleg_answer_delay);
    bus.set_request_delay_to(cfg.request_delay_to);
    let commits: Arc<Mutex<Vec<CommitRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let in_doubt: super::node::InDoubtLog = Arc::new(Mutex::new(Vec::new()));
    let oracles = super::oracle::Oracles::new(bucket.clone());
    // Plan 43 C7a (commit 6): the default clock-fault mix.
    let horizon = cfg.ops_per_client * 400 + 2_000;
    let (mix_rates, mix_faults) = gen_clock_mix(&cfg, seed, horizon);
    // Plan 43 C7b (commit 9): and the nodes' constant offsets.
    let mix_offsets = gen_clock_offsets(&cfg, seed);
    let env = NodeEnv {
        bucket: bucket.clone(),
        bus: bus.clone(),
        clock: clock.clone(),
        commits: commits.clone(),
        in_doubt: in_doubt.clone(),
        config: cfg.core.clone(),
        panic_after_events: cfg.panic_after_events,
        fast_path: cfg.fast_path,
        clock_skew_ms: cfg.clock_skew_ms,
        clock_offsets: cfg
            .clock_offsets
            .iter()
            .copied()
            .chain(mix_offsets)
            .collect(),
        clocks: Mutex::new(HashMap::new()),
        rate_budget_ppm: cfg.rate_budget_ppm,
        node_rates: cfg.node_rates.iter().copied().chain(mix_rates).collect(),
        wall_follows_rate: cfg.wall_follows_rate,
        suspend_accounting_short_ms: cfg.suspend_accounting_short_ms,
        seed,
        world: ChunkWorld::new(),
        tag_check_at_write: cfg.tag_check_at_write,
        settle_tagged_writes: cfg.settle_tagged_writes,
        holder_beats: cfg.holder_beats,
        timers_first_after_lag: cfg.timers_first_after_lag,
        own_lapses_unreported: cfg.own_lapses_unreported,
        beats_credited: cfg.beats_credited,
        beats_lost_in_lag: cfg.beats_lost_in_lag,
        deleg_lane: cfg.deleg_lane,
        deleg_root_lane: cfg.deleg_root_lane,
        deleg_root_probe: cfg.deleg_root_probe,
        root_probe_mutant: cfg.root_probe_mutant,
        probe_reads: cfg.probe_reads,
        lock_lane: cfg.lock_lane,
        lock_lane_ignores_freshness: cfg.lock_lane_ignores_freshness,
        skip_close_on_recall: cfg.skip_close_on_recall,
        oracles: oracles.clone(),
        rebuild_stub: cfg.rebuild_stub,
        plant_ungated_fast_path: cfg.plant_ungated_fast_path,
        plant_claim_root_group_on_lone: cfg.plant_claim_root_group_on_lone,
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
        s3_cut: Mutex::new(BTreeMap::new()),
        claim_ms: 4 * cfg.s3_latency.1 as i64,
        epochs: Mutex::new(Vec::new()),
        epochs_continued: Mutex::new(0),
        epochs_superseded: Mutex::new(0),
        split_brains: Mutex::new(Vec::new()),
        slack: cfg.epoch_slack,
        locks: Arc::new(super::locks::LockGhost::new(seed)),
    });
    cluster.locks.set_strict_turns(cfg.strict_lock_turns);
    cluster.locks.set_write_wait_ms(cfg.lock_write_wait_ms);
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
    if !cfg.dirs.is_empty() || !cfg.link_dirs.is_empty() {
        setup_dirs(&cluster, &cfg, &failures).await;
    }
    if cfg.lock_files > 0 {
        setup_lock_files(&cluster, &cfg, &history, &abandoned, &failures).await;
    }

    // The workload.
    let mut clients = Vec::new();
    // Plan 45 §12 O2: every directory of the workload, the far sides of
    // a cross-directory link.
    let link_targets: Vec<String> = std::iter::once(String::new())
        .chain(cfg.dirs.iter().cloned())
        .chain(cfg.link_dirs.iter().cloned())
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();
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
            let plan = cfg.client_plan.iter().find(|(n, _, _)| *n == node);
            let mut ops = ops;
            if let Some((_, _, n)) = plan {
                ops.truncate(*n);
            }
            let pace = rng.random_range(20..400);
            // Reads draw from their own generator, so a config without
            // reads replays exactly the schedules it had before M6.
            let mut read_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0x5e55);
            let steps = if cfg.reader_nodes.contains(&node) {
                reads_only(ops)
            } else {
                with_reads(&mut read_rng, ops, cfg.names, cfg.read_ratio)
            };
            let mut write_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0xc4a1);
            let steps = with_writes(&mut write_rng, steps, cfg.chunk_writes);
            let mut link_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0x11c4);
            let steps = with_links(
                &mut link_rng,
                steps,
                cfg.link_ratio,
                cfg.cross_links,
                &link_targets,
                cfg.names,
            );
            // Plan 30 §M14: locks from their own generator too.
            let mut lock_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0x10c5);
            let mut steps = if cfg.lock_nodes.is_empty() || cfg.lock_nodes.contains(&node) {
                with_locks(&mut lock_rng, steps, &cfg)
            } else {
                steps
            };
            if let Some((_, start, _)) = plan {
                steps.insert(0, Step::Delay(*start));
            }
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
    let mut faults = cfg.faults.clone();
    faults.extend(gen_faults(&mut rng, &cfg, horizon));
    faults.extend(mix_faults);
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
    // Plan 43 C1-3: the operator who abandons a group a claim has waited
    // for too long.
    let operator_abandoned: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    // Plan 43 C2e review: abandons the root refused, or forced, where the
    // group was not gone: a seed failure.
    let operator_failures: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    if let Some(after_ms) = cfg.abandon_after_ms {
        let (cluster, stop, fault_log) = (cluster.clone(), sampler_stop.clone(), fault_log.clone());
        let operator_abandoned = operator_abandoned.clone();
        let operator_failures = operator_failures.clone();
        tokio::spawn(async move {
            let mut done: BTreeSet<(NodeId, u64)> = BTreeSet::new();
            // The generations the operator saw waiting on the root, and
            // since when (C2e).
            let mut gen_since: BTreeMap<(NodeId, u64), u64> = BTreeMap::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(250)).await;
                for id in cluster.ids() {
                    let n = cluster.get(id);
                    if !n.alive() {
                        continue;
                    }
                    let now_ms = cluster.env.clock.elapsed_ms();
                    let view = n.view();
                    if let Some(w) = view.ack.waiting_for_group {
                        if w.waited_ms >= after_ms && done.insert((id, w.epoch)) {
                            fault_log.lock().unwrap().push(format!(
                                "t={now_ms} node {id}: repair abandon-group --epoch {}",
                                w.epoch
                            ));
                            drop(n.control(constellation_authority::Control::AbandonGroup {
                                epoch: w.epoch,
                                gen: 0,
                                force: false,
                            }));
                        }
                    }
                    let gens: Vec<(NodeId, u64)> = view
                        .delegation
                        .gens
                        .iter()
                        .filter(|g| g.7 && !g.6)
                        .map(|g| (g.1, g.2))
                        .collect();
                    for (delegate, gen) in gens {
                        let since = *gen_since.entry((id, gen)).or_insert(now_ms);
                        if now_ms.saturating_sub(since) < after_ms || !done.insert((id, gen)) {
                            continue;
                        }
                        fault_log.lock().unwrap().push(format!(
                            "t={now_ms} node {id}: repair abandon-group --gen {gen}"
                        ));
                        let answer = |force| {
                            n.control(constellation_authority::Control::AbandonGroup {
                                epoch: 0,
                                gen,
                                force,
                            })
                        };
                        let wait = |rx| tokio::time::timeout(Duration::from_secs(5), rx);
                        let result = match wait(answer(false)).await {
                            Ok(Ok(r)) => r,
                            _ => continue,
                        };
                        let result = match result {
                            Err(e) if e.contains("--force") => {
                                // Refused: the root heard from the delegate or
                                // a strict member lately. The operator forces
                                // only once it has confirmed what the command
                                // says to confirm: the delegate is dead, or
                                // alive with the generation stopped (it
                                // rebooted since its grant), and every strict
                                // member is dead. Any other refusal of this
                                // kind is the rule misjudging, a failure.
                                let dead = |node: NodeId| !cluster.get(node).alive();
                                let delegate_gone = dead(delegate)
                                    || cluster
                                        .get(delegate)
                                        .view()
                                        .delegation
                                        .mine
                                        .iter()
                                        .any(|m| m.1 == gen && m.3);
                                let members_dead = view
                                    .delegation
                                    .waiting
                                    .iter()
                                    .filter(|w| w.gen == gen)
                                    .flat_map(|w| w.members.iter())
                                    .filter(|m| m.strict)
                                    .all(|m| dead(m.node));
                                if !(delegate_gone && members_dead) {
                                    operator_failures.lock().unwrap().push(format!(
                                        "t={now_ms} node {id}: abandon-group --gen {gen} refused while the group \
                                         is not gone (delegate {delegate} gone: {delegate_gone}, strict members \
                                         dead: {members_dead}): {e}"
                                    ));
                                    continue;
                                }
                                fault_log.lock().unwrap().push(format!(
                                    "t={now_ms} node {id}: repair abandon-group --gen {gen} --force (refused: {e})"
                                ));
                                match wait(answer(true)).await {
                                    Ok(Ok(r)) => r,
                                    _ => continue,
                                }
                            }
                            r => r,
                        };
                        match result {
                            // The run-wide allowance: only an abandon that
                            // ended the generation.
                            Ok(_) => operator_abandoned.lock().unwrap().push(gen),
                            Err(e) => operator_failures.lock().unwrap().push(format!(
                                "t={now_ms} node {id}: abandon-group --gen {gen} refused: {e}"
                            )),
                        }
                    }
                }
            }
        });
    }
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
                // Plan 43 C2e (Q1): a root that cannot end a delegation's
                // generation without its group's tail makes the writes under
                // the subtree wait, an op of its own clients' included: that
                // is the rule, and the run ends `waiting_for_group` (no
                // operator acts in it). Anywhere else it is a hang.
                let gen_waits = cfg.abandon_after_ms.is_none()
                    && cluster
                        .ids()
                        .into_iter()
                        .map(|id| cluster.get(id))
                        .any(|n| n.alive() && n.view().delegation.gens.iter().any(|g| g.7 && !g.6));
                if gen_waits {
                    break;
                }
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
        let mop = mutate_op(&handle.meta, &op).expect("a create");
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
                    let mop = mutate_op(&handle.meta, &op).expect("a create");
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
                && (!cfg.await_dead_delegates
                    || v.delegation.gens.iter().all(|g| {
                        g.6 || (g.7 && cfg.abandon_after_ms.is_none()) || cluster.get(g.1).alive()
                    }))
                && v.job.is_none()
                && super::node::is_quiescent(&n.meta)
                && constellation_authority::Replica::applied_seq(&*n.meta).unwrap_or(0) >= head
        });
        // Plan 43 C2e: a delegation of a dead node that no live node knows as
        // a generation yet (the lease is nobody's: the root went away or
        // never claimed it back): the next op of any client would claim it
        // and find the generation waiting. The run asks for that claim,
        // and is not quiescent until a root has decided about it.
        let undecided = cfg.await_dead_delegates
            && !live
                .iter()
                .any(|n| n.view().ack.waiting_for_group.is_some())
            && live.first().is_some_and(|first| {
                first.meta.delegation_table().iter().any(|d| {
                    !cluster.get(d.node).alive()
                        && !live.iter().any(|n| {
                            n.view().delegation.gens.iter().any(|g| {
                                g.2 == d.gen && (g.6 || (g.7 && cfg.abandon_after_ms.is_none()))
                            })
                        })
                })
            });
        if undecided && waited.is_multiple_of(4_000) {
            // A write at a live node: it claims the root lease, learns the
            // generations its table lists, and finds them waiting. Bounded:
            // the op may be parked behind the wait itself.
            if let Some(n) = live.first() {
                extra_settles += 1;
                let handle = n.clone();
                let rid = handle.next_rid();
                let op = NsOp::Create(format!("settle-{extra_settles}"));
                let mop = mutate_op(&handle.meta, &op).expect("a create");
                history.invoke(settle_thread + extra_settles, rid, op);
                match tokio::time::timeout(Duration::from_secs(3), handle.submit(rid, mop)).await {
                    Ok(Ok(ClientReply::Outcome(o))) => {
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
        if all && !undecided {
            quiescent = true;
            break;
        }
    }

    // Plan 30 §M14: no lock request, waiter or recall is left: every
    // parked request was served and every recall answered or outwaited.
    // A waiter nobody asks for any more is dropped after `4 × lock ttl`
    // of silence (`lock_serve_waiters`): the wait covers that, and is at
    // least 10 s.
    let mut locks_stuck = None;
    if cfg.lock_files > 0 {
        let mut drained = false;
        let lock_ttl_ms = (cfg.core)(1, 1).lock_ttl_ms;
        for _ in 0..(4 * lock_ttl_ms / 100 + 20).max(100) {
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
    // F1: no held write landed inside another node's critical section,
    // nor over another node's newer acknowledged turn (unless rolled back
    // since).
    let rolled_back: HashSet<Rid> = cluster
        .ids()
        .into_iter()
        .flat_map(|id| {
            cluster
                .get(id)
                .shared
                .tentative
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    for id in cluster.ids() {
        let landed = cluster.get(id).shared.landed.lock().unwrap().clone();
        cluster.locks.check_landings(&landed, &rolled_back);
    }
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
    // Plan 41 D3 review: no lane answer rests on the root's own proof of
    // liveness a freshness window after its takeover.
    {
        let late = bucket.probe_late.lock().unwrap();
        if let Some(what) = late.first().filter(|_| cfg.check_probe_dating) {
            return Err(format!(
                "probe dating violated (plan 41 D3): {what} ({} violation(s))\n  faults: {:?}",
                late.len(),
                fault_log.lock().unwrap(),
            ));
        }
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
    {
        // A sweep greps this (`SWEEP-LEASE`): printed for passing and
        // failing seeds alike, before any check can fail the run.
        let ord = std::sync::atomic::Ordering::Relaxed;
        let (live, seals) = (
            oracles.leases.live_claims.load(ord),
            oracles.leases.seals_of_a_live_holder.load(ord),
        );
        if live + seals > 0 {
            eprintln!(
                "SWEEP-LEASE seed {seed}: ttl claims {} (of a live holder {live}), seals of a \
                 live holder {seals}, permitted claims {}",
                oracles.leases.ttl_claims.load(ord),
                oracles.leases.permitted_claims.load(ord)
            );
        }
    }
    if let Some(f) = failures.lock().unwrap().first() {
        return Err(format!("{f}\n  faults: {:?}", report.faults));
    }
    // Plan 30 §M9: under a durable acknowledgement policy an acknowledged
    // op is never exempt — a rollback of one is exactly what the backup
    // (or `ack=s3`) exists to make impossible, so only ops the client
    // abandoned (its process died with the call in flight) are tentative.
    // Plan 43 C1-3: a double fault (every copy of the rows down, or cut
    // from S3, past the point others may claim the lease) is still
    // classified and printed (`SWEEP-BUDGET`), but waives nothing: a claim
    // of a lease whose group may hold acknowledged rows fetches the sealed
    // tail of a member, or waits for one (`waiting_for_group`), so what was
    // acknowledged is never rolled back by the claim.
    let double_faults = cluster.budget_exceeded.lock().unwrap().clone();
    report.double_faults = double_faults;
    let strict_durability = cfg.strict_durability;
    // Plan 43 C1-3: what the failure budget used to excuse by classifying the
    // run is excused by what the plan keeps, for the class that keeps it: an
    // acknowledgement a lone node gave its own node's client (`Lone`: its
    // group is itself, no peer within the RTT budget) may be rolled back by a
    // deposition (plan 43 §5.7, C5 documents the lone branch); every other
    // class is a commit. What is excused is what the oracle judged `Lone`
    // from the lease and links it observed at the answer, not the core's
    // claim (the class-claim check judges that).
    let lone_answered = super::oracle::lone_answered(&oracles.acks.lock().unwrap());
    // A sweep greps this (`SWEEP-LONE-JUDGE`): the rolled-back rids whose
    // excuse the core's `Lone` claim and the oracle's judgement disagree on.
    let lone_claimed: HashSet<Rid> = oracles
        .acks
        .lock()
        .unwrap()
        .iter()
        .filter(|a| {
            a.claimed == Some(constellation_authority::core::AckClass::Lone) && !a.is_relay()
        })
        .map(|a| a.rid)
        .collect();
    let mut lone_disagree: Vec<String> = Vec::new();
    let mut tentative: HashSet<Rid> = abandoned.lock().unwrap().clone();
    let acked: HashSet<Rid> = history.returned().into_iter().map(|(rid, _)| rid).collect();
    let mut rolled_back_acked = 0usize;
    for id in cluster.ids() {
        let n = cluster.get(id);
        for rid in n.shared.tentative.lock().unwrap().iter() {
            if acked.contains(rid) {
                rolled_back_acked += 1;
            }
            if !strict_durability || lone_answered.contains(rid) {
                tentative.insert(*rid);
            }
            if strict_durability && lone_claimed.contains(rid) != lone_answered.contains(rid) {
                lone_disagree.push(format!(
                    "node {id} rid {rid:?} claimed-lone {} judged-lone {}",
                    lone_claimed.contains(rid),
                    lone_answered.contains(rid)
                ));
            }
        }
        report.stats.insert(id, n.view().stats);
        report.lane.insert(id, n.shared.lane.lock().unwrap().stats);
        report
            .floor_ms
            .extend(n.shared.floor_ms.lock().unwrap().iter().copied());
        report.guard_rises += n
            .shared
            .guard_rises
            .load(std::sync::atomic::Ordering::Relaxed);
        report.guard_base_rises += n
            .shared
            .guard_base_rises
            .load(std::sync::atomic::Ordering::Relaxed);
        report.fast_path_executed += n
            .shared
            .fast_path_executed
            .load(std::sync::atomic::Ordering::Relaxed);
        let load = |a: &std::sync::atomic::AtomicU64| a.load(std::sync::atomic::Ordering::Relaxed);
        report.held_writes += load(&n.shared.held_writes);
        report.held_refused += load(&n.shared.held_refused);
        report.held_accepted += load(&n.shared.held_accepted);
        report.fast_waited += load(&n.shared.fast_waited);
        report.fast_answered += load(&n.shared.fast_answered);
        let ls = n.meta.locks().stats();
        report.token_rejections_at_write += ls.token_rejections_at_write;
        report.settle_waits += ls.settle_waits;
        for i in 0..4 {
            report.tokens_judged_exact[i] += ls.tokens_judged_exact[i];
            report.tokens_judged_window[i] += ls.tokens_judged_window[i];
        }
        report.max_backups = report.max_backups.max(load(&n.shared.max_backups) as usize);
        report.rebuilds.0 += load(&n.shared.rebuilds_ok);
        report.rebuilds.1 += load(&n.shared.rebuilds_failed);
    }
    if !lone_disagree.is_empty() {
        lone_disagree.sort();
        eprintln!("SWEEP-LONE-JUDGE seed {seed}: {}", lone_disagree.join("; "));
    }
    report.acked_rolled_back = rolled_back_acked;
    if cfg.keeps_backups {
        let dropped: u64 = report.stats.values().map(|s| s.backup_ack_timeouts).sum();
        if dropped > 0 {
            return Err(format!(
                "a backup was dropped for want of acknowledgements ({dropped} time(s)) though \
                 it only lagged\n  faults: {:?}",
                report.faults
            ));
        }
    }
    // The runtime-lag configurations: every holder was alive throughout,
    // only late; a seal or a fast takeover deposed a live one. (Not
    // `backup_takeovers`: that counts any acquisition that re-applies a
    // backup tail, a released lease's included.)
    if cfg.live_holder {
        let taken: Vec<String> = report
            .stats
            .iter()
            .filter(|(_, s)| s.seals + s.s3_fast_takeovers > 0)
            .map(|(id, s)| {
                format!(
                    "node {id}: seals {} s3_fast_takeovers {}",
                    s.seals, s.s3_fast_takeovers
                )
            })
            .collect();
        if !taken.is_empty() {
            return Err(format!(
                "a live holder was sealed or taken over (it was only late): {taken:?}\n  faults: {:?}",
                report.faults
            ));
        }
    }
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
    // Plan 43 C1-3 (G4): a claim that still waits for the tail of a group
    // none of whose members answers (no operator abandoned it, or none
    // could): the cluster is not meant to converge. What is owed instead:
    // every rid a client was acknowledged is carried by the log, a
    // journal, a backup tail or a member copy somewhere — nothing was
    // answered that no copy holds.
    if std::env::var_os("AUTHORITY_SIM_ORACLE_DEBUG").is_some() {
        for id in cluster.ids() {
            let n = cluster.get(id);
            if n.alive() {
                let v = n.view();
                eprintln!(
                    "END-VIEW node {id}: gens {:?} lease-wait {:?} held_epoch {:?} lost {} s3_held {:?} table {:?}",
                    v.delegation.gens,
                    v.ack.waiting_for_group,
                    v.held_epoch,
                    v.lost,
                    v.s3_held,
                    n.meta
                        .delegation_table()
                        .iter()
                        .map(|d| (d.gen, d.node))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    let waiting_for_group = cluster
        .ids()
        .into_iter()
        .map(|id| cluster.get(id))
        .filter(|n| n.alive())
        .any(|n| {
            let v = n.view();
            // A claim waits for the tail of its lease's group, or a root
            // for the tail of a delegation's (plan 43 C2e).
            v.ack.waiting_for_group.is_some() || v.delegation.gens.iter().any(|g| g.7 && !g.6)
        });
    if waiting_for_group {
        report.waiting_for_group = true;
        let nowhere: Vec<Rid> = history
            .returned()
            .into_iter()
            .map(|(rid, _)| rid)
            .filter(|rid| !oracle.completed_at.contains_key(rid))
            .filter(|rid| {
                !cluster
                    .ids()
                    .into_iter()
                    .any(|id| super::node::meta_holds_rid(&cluster.get(id).meta, *rid))
            })
            .collect();
        if !nowhere.is_empty() {
            return Err(format!(
                "acked_held_somewhere: {} acknowledged rid(s) are in no log, journal, backup tail \
                 or member copy while a claim waits for the group: {nowhere:?}\n  faults: {:?}",
                nowhere.len(),
                report.faults
            ));
        }
        return Ok(report);
    }
    // Plan 43 C2e: the operator ended a delegation generation whose group
    // had not handed over its tail (`abandon-group --gen`): the rows only
    // that group held were acknowledged and are not in the log. That is what
    // the command costs, and what it says it costs; these runs count them
    // (`operator_lost`) and judge them tentative. Only runs the operator
    // acted in get the allowance.
    {
        let failures = operator_failures.lock().unwrap();
        if !failures.is_empty() {
            return Err(format!(
                "operator_abandon: {failures:?}\n  faults: {:?}",
                report.faults
            ));
        }
    }
    if !operator_abandoned.lock().unwrap().is_empty() {
        let lost: Vec<Rid> = history
            .returned()
            .into_iter()
            .map(|(rid, _)| rid)
            .filter(|rid| !oracle.completed_at.contains_key(rid))
            .collect();
        report.operator_lost = lost.len();
        tentative.extend(lost);
    }
    report.segments = oracle.segments;
    report.fenced = oracle.fenced;
    report.commits = commit_list.len();
    let events = same_inode_renames(history.events(), &oracle);
    report.ops_invoked = history.invoked();
    report.ops_returned = history.returned().len();
    {
        let ok: HashSet<Rid> = history
            .returned()
            .into_iter()
            .filter(|(_, ret)| *ret == NsRet::Ok)
            .map(|(rid, _)| rid)
            .collect();
        for e in &events {
            if let super::history::HistEvt::Invoke { rid, op, .. } = e {
                match op {
                    NsOp::Link(..) if ok.contains(rid) => report.links_ok += 1,
                    NsOp::Exchange(..) if ok.contains(rid) => report.exchanges_ok += 1,
                    _ => {}
                }
                if let NsOp::Link(a, b) | NsOp::Rename(a, b) | NsOp::Exchange(a, b) = op {
                    if ok.contains(rid) && dir_of(a) != dir_of(b) {
                        report.cross_dir_ok += 1;
                    }
                }
            }
        }
    }
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
    let journal = journal_order(&oracle);
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
        let w = check_linearizable_witnessed(proj, &tentative, &oracle.completed_at, &journal)
            .map_err(|e| {
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
    //
    // (Plan 43 C1-4 had left the rebuild stub out of this check: a deposed
    // root that never recovers let its clients' later ops go ahead of the
    // acknowledgements a delegation gave them before the root changed.
    // Plan 43 C2 closes that: a delegate acknowledges once its group holds
    // the row, so a root change cannot overtake an acknowledged row.)
    super::history::check_session_order(&events, &tentative, &oracle.completed_at, &journal)
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
        if stats.deleg_lost_after_ack > 0 {
            return Err(format!(
                "node {id} stopped a generation at {} rows it had answered under the group (a lost dependency)\n  stats: {:#?}",
                stats.deleg_lost_after_ack, report.stats
            ));
        }
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
    report.probe_races = bucket
        .probe_races
        .load(std::sync::atomic::Ordering::Relaxed);
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
        // Plan 43 C1-4 (`member_copies_drained`): at convergence every row a
        // node kept as a member of a group the archive carries is gone. A
        // row of a group-committed transaction left over is either a commit
        // that was lost or a drop the rules missed; both fail.
        let mut left: Vec<String> = Vec::new();
        for id in cluster.ids() {
            let n = cluster.get(id);
            if !n.alive() {
                continue;
            }
            let rows = constellation_authority::Replica::member_copy_rows(&*n.meta);
            if rows > 0 {
                let epochs = constellation_authority::Replica::member_copy_epochs(&*n.meta);
                // Which rows: their journal place, origin and records.
                let what: Vec<String> = epochs
                    .iter()
                    .filter_map(|e| n.meta.member_copy_rows(*e).ok().flatten())
                    .flat_map(|(_, rows)| rows)
                    .map(|m| {
                        format!(
                            "rows {}..={} origin {:?} class {:?} {:?}",
                            m.tx.first, m.tx.last, m.tx.origin, m.class, m.tx.records
                        )
                    })
                    .collect();
                left.push(format!(
                    "node {id}: {rows} row(s) of epoch(s) {epochs:?}: {}",
                    what.join("; ")
                ));
            }
        }
        report.member_copy_rows_left = left.len();
        if !left.is_empty() && cfg.strict_durability {
            return Err(lin_context(format!(
                "member_copies_drained: member copies outlive the archive at convergence: {left:?}"
            )));
        }
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
    // Plan 43 C7-F F-a: a holder's beat guard is meant to be fixed once its
    // takeover is over, save for what protects a lease-capped right (a
    // subtree grace, a hold-era grant): a base that rose after a successor
    // may have heard the lower value would let it cap its floor below what
    // the holder waits out.
    if report.guard_base_rises > 0 {
        return Err(format!(
            "a beat guard's base (floor, quarantines) rose after it was published \
             {} time(s)\n  faults: {:?}",
            report.guard_base_rises, report.faults
        ));
    }
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
    // Plan 43 C0: the commit-rule oracles. Always counted; each fails the
    // run only under its strict flag (the counters of a violating seed are
    // in the report's failure text).
    let in_doubt_answers = in_doubt.lock().unwrap().clone();
    // Each rolled-back rid's latest rollback (I5 judges the answers before
    // it); one the replay queue never named (a journaled refusal's) is
    // judged against every answer.
    let rolled_back_at: HashMap<Rid, u64> = {
        let mut at: HashMap<Rid, u64> = rolled_back.iter().map(|r| (*r, 0)).collect();
        let mut named: HashSet<Rid> = HashSet::new();
        for id in cluster.ids() {
            for (rid, tick) in cluster.get(id).shared.rolled_back_at.lock().unwrap().iter() {
                if let Some(t) = at.get_mut(rid) {
                    *t = (*t).max(*tick);
                    named.insert(*rid);
                }
            }
        }
        for (rid, t) in at.iter_mut() {
            if !named.contains(rid) {
                *t = u64::MAX;
            }
        }
        at
    };
    let found = super::oracle::end_of_run(
        &oracles,
        &oracle,
        &in_doubt_answers,
        &rolled_back_at,
        converged_checkable,
    );
    // `AUTHORITY_SIM_ORACLE_DEBUG=1`: what the end of the run found (the
    // relays no judged answer backs, the unjournaled refusals that landed).
    if std::env::var_os("AUTHORITY_SIM_ORACLE_DEBUG").is_some() {
        for text in found
            .i1
            .iter()
            .filter(|t| t.contains("relay of rid"))
            .chain(&found.unjournaled_landed)
        {
            eprintln!("ORACLE-END {text}");
        }
    }
    report.acks_by_class = found.acks_by_class.clone();
    report.uncommitted_acks = found.uncommitted_acks.clone();
    report.unjournaled_refusals = found.unjournaled_refusals.clone();
    report.mismatched_acks = found.mismatched_acks.clone();
    report.unverified_relays = found.unverified_relays;
    report.class_mismatch = found.class_mismatch;
    report.class_claim_mismatch = found.class_claim.len();
    report.unjournaled_landed = found.unjournaled_landed.len();
    report.acks_unsourced = oracles
        .acks_unsourced
        .load(std::sync::atomic::Ordering::Relaxed) as usize;
    report.acks_unmapped = oracles
        .acks_unmapped
        .load(std::sync::atomic::Ordering::Relaxed) as usize;
    report.identity_copies = found.i2.len();
    report.identity_copies_ungated = found.i2_ungated.len();
    report.non_verbatim = found.i2c.len();
    report.late_landings = found.i4.len();
    report.non_lone_rollbacks = found.i5.len();
    {
        let ord = std::sync::atomic::Ordering::Relaxed;
        report.lease_ttl_claims = oracles.leases.ttl_claims.load(ord);
        report.lease_live_claims = oracles.leases.live_claims.load(ord);
        report.seals_of_a_live_holder = oracles.leases.seals_of_a_live_holder.load(ord);
        report.permitted_claims = oracles.leases.permitted_claims.load(ord);
    }
    let lone_lead_examples = {
        let ll = oracles.lone_lead.lock().unwrap();
        report.lone_past_lead = ll.past_lead.len();
        report.lone_behind_gated = ll.behind_gated.len();
        report.lone_rows_judged = ll.lone_rows_seen.len();
        report.gated_rows_judged = ll.gated_rows_seen.len();
        report.lone_held_by_gate = ll.held_by_gate;
        report.lone_demoted_at_journal = ll.demoted_at_journal;
        ll.examples.clone()
    };
    report.oracle_examples = found.examples();
    let counters = format!(
        "oracle counters: acks by class {:?}, uncommitted {:?}, unjournaled refusals {:?}, \
         mismatched {:?}, unverified relays {}, identity copies {}, non-verbatim {}, \
         late landings {}, non-lone rollbacks {}",
        report.acks_by_class,
        report.uncommitted_acks,
        report.unjournaled_refusals,
        report.mismatched_acks,
        report.unverified_relays,
        report.identity_copies,
        report.non_verbatim,
        report.late_landings,
        report.non_lone_rollbacks
    );
    for (strict, what, list) in [
        (cfg.strict_commit, "I1", &found.i1),
        (cfg.strict_commit, "class claim", &found.class_claim),
        // Plan 43 C1-4: a durable acknowledgement policy is a commit: its
        // rows are never queued twice (I2 (b), for rows standing for a
        // committed answer), complete verbatim (I2 (c)), and are rolled back
        // only if the node answered them as a lone one's (I5).
        (
            cfg.strict_identity || cfg.strict_durability,
            "I2",
            &found.i2,
        ),
        (
            cfg.strict_identity || cfg.strict_durability,
            "I2c",
            &found.i2c,
        ),
        (cfg.strict_settle, "I4", &found.i4),
        (cfg.strict_settle, "I4", &found.unjournaled_landed),
        (cfg.strict_lone || cfg.strict_durability, "I5", &found.i5),
        (cfg.strict_lone_lead, "lone lead", &lone_lead_examples),
        // Last: a seed that also fails another check reports that one, so
        // the failing seeds without the oracle are the ones it does not
        // head.
        (cfg.strict_lease_claims, "I6", &found.i6),
    ] {
        if strict {
            if let Some(first) = list.first() {
                return Err(format!(
                    "{first}\n  ({} {what} violation(s))\n  {counters}\n  faults: {:?}",
                    list.len(),
                    report.faults
                ));
            }
        }
    }
    Ok(report)
}

/// Plan 30 §M11: create `cfg.dirs` under the root through node 1 (it
/// takes the lease), then delegate `cfg.delegations` from it; wait until
/// every delegate has installed its generation.
async fn setup_dirs(cluster: &Arc<Cluster>, cfg: &SimConfig, failures: &Arc<Mutex<Vec<String>>>) {
    let handle = cluster.get(1);
    for d in cfg.dirs.iter().chain(&cfg.link_dirs) {
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
                .chain(&cfg.link_dirs)
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
    // A root change inside the setup's window (a backup that sealed a live
    // holder, a crashed holder's takeover: the faults of the random-fault
    // configurations reach it, and which seeds do depends on how fast the
    // host runs the store's real writes against the simulated clock) ends
    // the delegations the setup made, and the workload goes on under the new
    // root; the setup's expectation holds only while the first root does.
    let failed_over = (1..=cfg.nodes as NodeId).any(|n| {
        cluster
            .get(n)
            .view()
            .held_epoch
            .is_some_and(|epoch| epoch > 1)
    });
    if failed_over {
        return;
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
        let mop = mutate_op(&handle.meta, &op).expect("a create");
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
            let mop = mutate_op(&handle.meta, &op).expect("a create");
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
