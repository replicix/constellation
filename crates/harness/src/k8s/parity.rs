//! The `linux-csi` parity lane (plan 37 §12, K7): `harness k8s-scenario
//! --parity`. It answers `tests/parity.py`'s question — does every `harness
//! run` scenario come out the same as in the `linux-fuse` reference lane? —
//! for volumes mounted through the CSI driver, and writes one results-file
//! entry per [`SCENARIOS`](crate::scenarios::SCENARIOS) name, so the checker compares the whole catalog
//! and a scenario cannot drop out of the comparison unnoticed.
//!
//! # What runs
//!
//! Every `harness run` scenario is written against the harness's local
//! [`Client`](crate::client::Client): a `constellation` process the
//! scenario starts with the flags and environment it chooses, kills,
//! pauses, remounts, asks over its control socket and reaches S3 through
//! toxiproxy. A CSI-mounted volume has none of that — its engine is a pod
//! the node plugin starts, configures and replaces, and the only way in is
//! a PV in a workload pod. So a scenario runs in this lane only once it is
//! *ported*: [`PORTED`] re-expresses it through pods (`kubectl exec`), with
//! the same operations, the same oracle and the same assertions, and every
//! departure from the local original stated in its doc comment. Its result
//! goes into the file under the scenario's own name, so `tests/parity.py`
//! compares it with the reference lane's run of the original.
//!
//! Every other scenario is listed in [`SKIPPED`], by hand, with what it
//! needs, and reported `skipped` with that reason ([`Skip::reason`]).
//! A scenario that pulls a lever on its own local clients the driver does
//! not give a pod (a SIGKILL, toxiproxy in front of one client, an
//! environment knob the StorageClass does not expose, a third node) names
//! [`LOCAL_CLIENT`] as a whole word — the parity file's capability-scoped
//! wildcard binds to it — and the levers it pulls; one whose behaviour a
//! `k8s-scenario` covers through the driver also names that scenario, and
//! the parity file lists it by name. A scenario that needs no such lever
//! is reported "not yet ported" (follow-up F6) and excused in the parity
//! file by name only, so porting it turns its entry stale.
//!
//! The reference lane's run of a ported scenario stays the original: the
//! two lanes run different code under one name, and what makes that a
//! comparison is that the port keeps the original's operations and checks
//! (`parity_tests` pins the list against the catalog).

use super::remote::{RemoteWorkload, POD_TOOL};
use super::scenarios::{verify, K8sScenario, CONVERGE};
use super::{data_dir, fs_uuid_of, Env, Scope};
use crate::model::Model;
use crate::scenarios::eventually;
use anyhow::{ensure, Context, Result};
use std::collections::BTreeMap;
use std::time::Duration;

/// The lane's name in results files and `tests/platform-parity.toml`.
pub const LANE: &str = "linux-csi";

/// The capability this lane lacks, as the skip reasons spell it: a
/// scenario-owned local `constellation` process.
pub const LOCAL_CLIENT: &str = "LocalClient";

/// The scenarios this lane runs, under their `harness run` names.
pub const PORTED: &[K8sScenario] = &[
    K8sScenario {
        name: "baseline",
        desc: "one pod on one PV: 5 blocks of 60 seeded operations, the tree verified against the model after each",
        workers: 1,
        run: baseline,
        gate: None,
    },
    K8sScenario {
        name: "two-clients-disjoint",
        desc: "two pods on two workers, each on a PV of its own pool: 3 blocks of 40 seeded operations each, both trees verified after each",
        workers: 2,
        run: two_clients_disjoint,
        gate: None,
    },
    K8sScenario {
        name: "two-clients-shared",
        desc: "one RWX PV, pods on both workers writing disjoint subtrees: each sees its own writes at once and the other's within the bound; no conflicts on either engine",
        workers: 2,
        run: two_clients_shared,
        gate: None,
    },
    K8sScenario {
        name: "git-workflow",
        desc: "stage-and-rename publishing ping-pong between pods on two workers sharing an RWX PV, exact contents each round, no conflicts on either engine",
        workers: 2,
        run: git_workflow,
        gate: None,
    },
    K8sScenario {
        name: "truncate-never-resurrects",
        desc: "ftruncate/O_TRUNC/truncate-up/fallocate followed by writes past the gap, on and across two workers: bytes past a truncate read back as zeros live, and again through fresh engine pods",
        workers: 2,
        run: truncate_never_resurrects,
        gate: None,
    },
    K8sScenario {
        name: "xattr-roundtrip",
        desc: "two pods on two workers sharing an RWX PV: file and directory xattrs replicate, a removal too; rsize/rcount count a 1 GiB sparse file logically, at the directory and at the PV's root (the volume's own totals, not the pool's)",
        workers: 2,
        run: xattr_roundtrip,
        gate: None,
    },
    K8sScenario {
        name: "fallocate-sparse",
        desc: "a 256 MiB sparse file: a punched hole reads as zeros, SEEK_HOLE/SEEK_DATA find it, a rewrite in it persists, a few chunk objects in the bucket; a fresh engine pod on the other worker reads it all back",
        workers: 2,
        run: fallocate_sparse,
        gate: None,
    },
    K8sScenario {
        name: "append-setattr-size",
        desc: "an O_APPEND writer under attribute changes and fsyncs (the kubelet's fsGroup pass) on each worker in turn: every acknowledged block in place on the writer, the other worker and a fresh engine pod",
        workers: 2,
        run: append_setattr_size,
        gate: None,
    },
];

/// Why a [`SCENARIOS`](crate::scenarios::SCENARIOS) entry does not run in this lane. Every name the
/// lane does not run is listed in [`SKIPPED`] by hand, with what it needs
/// (`parity_tests` fails on a catalog name in neither [`PORTED`] nor
/// [`SKIPPED`], so a new scenario is classified by whoever adds it).
#[derive(Clone, Copy, Debug)]
pub enum Skip {
    /// It pulls levers on its own local clients that a CSI-mounted volume
    /// does not have (the phrases below): the reason names
    /// [`LOCAL_CLIENT`], and the parity file's capability wildcard binds
    /// to that word.
    Local(&'static [&'static str]),
    /// As [`Skip::Local`], and the behaviour is covered through the driver
    /// by the `k8s-scenario`s named (`" and "`-separated); the parity file
    /// names the scenario, pointing at the coverage.
    Covered(&'static [&'static str], &'static str),
    /// It needs no such lever — mounts, file operations, and status reads
    /// an engine pod answers too: portable to pods, not ported yet
    /// (follow-up F6, PROGRESS). The reason does not name
    /// [`LOCAL_CLIENT`]; the parity file excuses it by name, so porting it
    /// turns that entry stale.
    Unported(&'static str),
}

// The levers, as the skip reasons spell them.
const KILL: &str = "SIGKILLs a client and remounts it on the same state directory";
const STOP: &str = "freezes a client with SIGSTOP, then resumes it";
const S3_FAULTS: &str =
    "injects S3 latency, bandwidth limits or cuts through the toxiproxy in front of a client (engine pods reach S3 directly)";
const P2P_CUT: &str =
    "cuts a client's P2P links through its CONSTELLATION_FAULT_P2P_DENY_FILE hook";
const HOLD_SYNC: &str =
    "holds a holder's shipping through its CONSTELLATION_FAULT_HOLD_SYNC_FILE hook";
const LOSE_CHUNKS: &str = "drops pending chunks through the CONSTELLATION_FAULT_LOSE_CHUNKS hook";
const P2P_OFF: &str = "runs clients with CONSTELLATION_P2P=off";
const PLACEMENT: &str =
    "pins the lease or a delegation with CONSTELLATION_LEASE_PLACEMENT / CONSTELLATION_DELEGATION_PLACEMENT=off";
const TIMERS: &str =
    "shortens lease and sync timers (CONSTELLATION_LEASE_TTL_MS, _SYNC_INTERVAL_MS) to fail over in scenario time";
const ADMIN: &str = "runs the admin CLI (gc, fsck, prune, repair, retire) as a local process against the filesystem";
const GC_KNOBS: &str = "shortens the GC horizon (CONSTELLATION_GC_HORIZON_S)";
const COUNT: &str = "counts each client's S3 requests through a counting proxy of its own";
const CACHE: &str = "wipes or shrinks a client's chunk cache (drop_cache, --cache-size)";
const RSS: &str = "samples or limits a client process's memory (RSS, RLIMIT_AS)";
const WRITE_MODE: &str =
    "sets write-back and write-through per client or switches it live (writeMode is one per StorageClass)";
const UPGRADE: &str = "runs `constellation daemon --upgrade` on a local daemon";
const TRANSPORT: &str =
    "chooses a mount's FUSE transport, passthrough or zero-copy (CONSTELLATION_FUSE_*; an engine pod's are the node's) and checks the kernel side";
const CTO_STRICT: &str = "mounts with --cto strict (the driver mounts the default, bounded)";
const LIFECYCLE: &str = "drives a local daemon's node.lifecycle (suspend, resume, metered network)";
const SNAPSCHED: &str =
    "runs the snapshot scheduler on scenario timescales (CONSTELLATION_SNAPSCHED_*)";
const NODES3: &str =
    "runs three clients of one filesystem (the kind lane has two workers: a pool has two node-owned engine pods)";
const NODES4: &str =
    "runs four clients of one filesystem (the kind lane has two workers: a pool has two node-owned engine pods)";
const CHAOS: &str = "drives local mounts with the constellation-chaos tool";
const GIT: &str =
    "runs git in the client's mount namespace (the lane's pods run the driver image: no git)";

/// Every [`SCENARIOS`](crate::scenarios::SCENARIOS) name the lane does not run, in catalog order, with
/// what it needs.
pub const SKIPPED: &[(&str, Skip)] = &[
    ("latency", Skip::Local(&[S3_FAULTS])),
    ("slow-network", Skip::Local(&[S3_FAULTS])),
    ("s3-outage", Skip::Local(&[S3_FAULTS])),
    ("s3-flap", Skip::Local(&[S3_FAULTS])),
    ("kill9-remount", Skip::Local(&[KILL])),
    ("cold-cache", Skip::Local(&[CACHE])),
    (
        "atime-eventual",
        Skip::Local(&[
            S3_FAULTS,
            "tunes atime flushing (CONSTELLATION_ATIME, _ATIME_FLUSH_MS, _ATIME_GRANULARITY_S)",
        ]),
    ),
    (
        "quota-enforcement",
        Skip::Covered(
            &["sets and clears a quota live with quota.set on a local daemon"],
            "csi-many-pvs-one-pool and csi-human-cli-mount",
        ),
    ),
    (
        "prune",
        Skip::Local(&["arms retention prune on scenario timescales (CONSTELLATION_PRUNE_*) and runs prune passes on a local daemon"]),
    ),
    (
        "lease-handover",
        Skip::Local(&["shortens the lease TTL (CONSTELLATION_LEASE_TTL_MS=10000) so the handover fits its bound"]),
    ),
    ("lease-fencing", Skip::Local(&[STOP, TIMERS])),
    ("continuation-epoch", Skip::Local(&[S3_FAULTS, TIMERS])),
    ("epoch-member-lost", Skip::Local(&[STOP, S3_FAULTS, TIMERS])),
    ("deposed-reintegration", Skip::Local(&[STOP, S3_FAULTS, WRITE_MODE])),
    (
        "backup-takeover-holds-missing-chunks",
        Skip::Local(&[KILL, S3_FAULTS, "hands chunks off early (CONSTELLATION_CHUNK_HANDOFF_AFTER_MS)"]),
    ),
    (
        "backup-takeover-drops-held-chunks",
        Skip::Local(&[KILL, S3_FAULTS, "runs `repair drop-held` on a local daemon"]),
    ),
    ("epoch-peer-reaching-s3-declines", Skip::Local(&[STOP, S3_FAULTS])),
    ("deposed-reintegration-backup", Skip::Local(&[KILL, STOP, HOLD_SYNC])),
    (
        "node-leave",
        Skip::Covered(
            &[S3_FAULTS, "runs `constellation leave` on a local daemon"],
            "csi-node-drain",
        ),
    ),
    ("p2p-invalidation", Skip::Local(&[P2P_OFF])),
    (
        "p2p-handover",
        Skip::Local(&["turns forwarding off (CONSTELLATION_FORWARD=off) to measure the takeover path"]),
    ),
    (
        "forwarded-mutations",
        Skip::Unported("two mounts and the lease's status (its CONSTELLATION_LEASE_IDLE_RELEASE_MS is the default)"),
    ),
    (
        "scratch-publish",
        Skip::Unported("two mounts, the user.constellation.scratch marker and a publishing rename"),
    ),
    ("p2p-partition-tolerance", Skip::Local(&[P2P_OFF])),
    ("p2p-same-identity-restart", Skip::Local(&[KILL, NODES3])),
    (
        "p2p-cluster-restart",
        Skip::Local(&[KILL, NODES4, "takes a docker bridge under the nodes down and up"]),
    ),
    ("coop-cache-hit", Skip::Local(&[S3_FAULTS])),
    (
        "s3-retry",
        Skip::Local(&[
            S3_FAULTS,
            "shrinks the S3 retry budget (CONSTELLATION_S3_MAX_RETRIES, _S3_RETRY_TIMEOUT_MS)",
        ]),
    ),
    ("coop-fallback", Skip::Local(&[STOP, S3_FAULTS])),
    ("web-fleet", Skip::Local(&[NODES3, COUNT])),
    (
        "coop-exact-churn",
        Skip::Local(&[NODES3, CACHE, "sets the cooperative digest mode (CONSTELLATION_COOP_DIGEST)"]),
    ),
    (
        "coop-digest-compare",
        Skip::Local(&[NODES3, CACHE, "sets the cooperative digest mode (CONSTELLATION_COOP_DIGEST)"]),
    ),
    (
        "web-ui-smoke",
        Skip::Local(&["serves a local daemon's web UI and HTTP control adapter on a host port"]),
    ),
    ("gc-lifecycle", Skip::Local(&[ADMIN, GC_KNOBS])),
    (
        "csi-credential-revocation",
        Skip::Covered(
            &["starts a native versitygw and `serve --await-unlock` engines as local processes"],
            "csi-secret-rotation",
        ),
    ),
    (
        "snapacct",
        Skip::Local(&[ADMIN, "refreshes snapshot accounting on scenario timescales (CONSTELLATION_SNAPACCT_*)"]),
    ),
    ("gc-dedup-race", Skip::Local(&[ADMIN, GC_KNOBS])),
    ("gc-open-orphan-hold", Skip::Local(&[ADMIN, GC_KNOBS])),
    (
        "log-retention-gap-follower",
        Skip::Local(&[STOP, ADMIN, P2P_OFF, "shortens log retention (CONSTELLATION_LOG_RETENTION_SEGMENTS and kin)"]),
    ),
    (
        "log-retention-gap-taker",
        Skip::Local(&["stops a client while GC prunes the log", ADMIN, P2P_OFF]),
    ),
    (
        "log-retention-gap-open-orphan",
        Skip::Local(&[STOP, ADMIN, P2P_OFF, "shortens log retention (CONSTELLATION_LOG_RETENTION_SEGMENTS and kin)"]),
    ),
    (
        "fsck-repair",
        Skip::Local(&[ADMIN, "damages the bucket behind a client (a missing chunk, an orphan, a torn segment)"]),
    ),
    (
        "fsck-while-mounted",
        Skip::Local(&["runs `constellation fsck` from the host against a running local daemon's control socket"]),
    ),
    (
        "snapshot-lifecycle",
        Skip::Covered(
            &["takes, holds and deletes snapshots through a local client's CLI"],
            "csi-snapshot-clone-mount",
        ),
    ),
    (
        "snapshot-busy-latency",
        Skip::Local(&[S3_FAULTS, "times snapshots through a local client's CLI"]),
    ),
    (
        "clone-workflow",
        Skip::Covered(
            &["clones snapshots through a local client's CLI"],
            "csi-snapshot-clone-mount and csi-clone-cross-pool-refused",
        ),
    ),
    (
        "snapshot-mount",
        Skip::Covered(
            &["mounts snapshot views of a local mount"],
            "csi-snapshot-clone-mount",
        ),
    ),
    (
        "snapshot-churn",
        Skip::Local(&[KILL, S3_FAULTS, P2P_CUT]),
    ),
    ("snapsched", Skip::Local(&[KILL, SNAPSCHED, COUNT])),
    ("snapsched-s3-outage", Skip::Local(&[S3_FAULTS, SNAPSCHED])),
    ("snapsched-grace", Skip::Local(&[SNAPSCHED])),
    ("snapsched-budget", Skip::Local(&[SNAPSCHED, NODES3])),
    (
        "snapsched-write-overhead",
        Skip::Local(&[SNAPSCHED, "times fio on a local mount against a 100k-file tree"]),
    ),
    (
        "e2e-basic",
        Skip::Unported("an e2e=true class, a cold remount, the bucket scanned for plaintext, and a wrong passphrase refused"),
    ),
    ("e2e-two-nodes", Skip::Local(&[S3_FAULTS])),
    (
        "passwd-live-cluster",
        Skip::Local(&["runs `constellation fs passwd` from a local client against a live cluster", P2P_CUT]),
    ),
    ("fresh-node-bootstrap", Skip::Local(&[KILL])),
    ("commit-strips-pending-upload", Skip::Local(&[KILL, S3_FAULTS, WRITE_MODE])),
    ("readahead", Skip::Local(&[S3_FAULTS, CACHE])),
    ("readahead-adaptive", Skip::Local(&[S3_FAULTS, CACHE])),
    ("e2e-spilled-manifest", Skip::Local(&[S3_FAULTS, CACHE])),
    ("e2e-decode-priority", Skip::Local(&[S3_FAULTS, CACHE])),
    ("scan-ahead", Skip::Local(&[S3_FAULTS, CACHE])),
    ("distant-bigfile-stable", Skip::Local(&[S3_FAULTS, CACHE])),
    ("distant-bigfile-stable-e2e", Skip::Local(&[S3_FAULTS, CACHE])),
    ("prefetch-abandon", Skip::Local(&[S3_FAULTS, CACHE])),
    ("prefetch-abandon-e2e", Skip::Local(&[S3_FAULTS, CACHE])),
    ("prefetch-fairness", Skip::Local(&[S3_FAULTS, CACHE])),
    ("fio-latency", Skip::Local(&[S3_FAULTS])),
    ("fio-blips", Skip::Local(&[S3_FAULTS])),
    ("fsync-hard-outage", Skip::Local(&[S3_FAULTS])),
    (
        "fsync-soft-timeout",
        Skip::Local(&[S3_FAULTS, "mounts with --fsync-timeout 2s"]),
    ),
    ("fsync-interrupt", Skip::Local(&[S3_FAULTS])),
    (
        "fsyncdir-barrier",
        Skip::Local(&[KILL, "mounts with --fsync-mode s3"]),
    ),
    (
        "stress-ng-fs",
        Skip::Unported("stress-ng on one mount (the lane's pods have no stress-ng) with VolumeSnapshots for its mid-run snapshots"),
    ),
    ("stress-ng-flap", Skip::Local(&[S3_FAULTS])),
    ("big-file-write", Skip::Local(&[RSS, CACHE])),
    ("staging-crash", Skip::Local(&[KILL])),
    ("unmount-drain", Skip::Local(&[S3_FAULTS])),
    ("writeback-latency", Skip::Local(&[S3_FAULTS, WRITE_MODE])),
    ("writeback-bigfile", Skip::Local(&[RSS, CACHE, WRITE_MODE])),
    ("writeback-drain", Skip::Local(&[WRITE_MODE])),
    ("writeback-fsync", Skip::Local(&[KILL, PLACEMENT])),
    (
        "writeback-backpressure",
        Skip::Local(&[S3_FAULTS, "shrinks the staging budget (CONSTELLATION_STAGING_BUDGET)"]),
    ),
    (
        "existence-bloom-dedup",
        Skip::Unported("two mounts in turn and the bucket's chunk count"),
    ),
    (
        "existence-peer-hint",
        Skip::Local(&["turns the digest interval down (CONSTELLATION_DIGEST_INTERVAL_S=1)"]),
    ),
    ("chaos-ci", Skip::Local(&[NODES3, CHAOS])),
    ("git-under-flock", Skip::Local(&[NODES4, GIT])),
    ("git-under-flock-gc", Skip::Local(&[NODES4, GIT])),
    ("git-under-flock-b2b", Skip::Local(&[NODES4, GIT])),
    ("git-under-flock-rounds", Skip::Local(&[NODES4, GIT])),
    ("git-under-flock-causal", Skip::Local(&[NODES3, GIT])),
    (
        "git-under-flock-faults",
        Skip::Local(&[KILL, STOP, P2P_CUT, S3_FAULTS, NODES4, GIT]),
    ),
    ("lock-grant-dead-generation", Skip::Local(&[NODES3])),
    ("chaos-soak-4", Skip::Local(&[NODES4, CHAOS])),
    ("disjoint-write-4", Skip::Local(&[NODES4, CHAOS])),
    ("mkdir-p-race", Skip::Local(&[NODES4])),
    ("create-storm-s3-only", Skip::Local(&[NODES3, P2P_OFF])),
    (
        "mtree-gc-plateau",
        Skip::Local(&[ADMIN, "shortens commit retention and compaction (CONSTELLATION_COMMIT_RETENTION*, _COMPACT_BYTES_PER_S)"]),
    ),
    ("idle-cluster-is-quiet", Skip::Local(&[NODES3, COUNT])),
    ("wan-writer-ships-put-only", Skip::Local(&[S3_FAULTS, COUNT, P2P_OFF])),
    ("sticky-lease-handoff-over-s3", Skip::Local(&[P2P_OFF])),
    (
        "named-shared-daemon",
        Skip::Local(&["mounts NAME and NAME:/sub from two CLI calls that share one local daemon", KILL]),
    ),
    (
        "forward-timeout-reexec",
        Skip::Local(&["delays forwarded replies through the CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS hook"]),
    ),
    ("holder-ships-under-forward-load", Skip::Local(&[NODES3])),
    ("holder-crash-phantom-shadow", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    ("holder-crash-phantom-new-holder", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    ("takeover-marker-strands-promptly", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    ("holder-publishes-log-prefix", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    ("holder-publishes-log-prefix-backup", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    ("poison-record-isolation", Skip::Local(&[HOLD_SYNC, LOSE_CHUNKS])),
    (
        "unmount-with-held-records",
        Skip::Local(&[LOSE_CHUNKS, S3_FAULTS, "stalls a local daemon's shutdown (CONSTELLATION_SHUTDOWN_STALL_S)"]),
    ),
    ("publish-only-holder", Skip::Local(&[NODES3, COUNT])),
    ("stale-base-rename-divergence", Skip::Local(&[HOLD_SYNC, NODES3])),
    ("session-exists-observed", Skip::Local(&[HOLD_SYNC])),
    ("session-forwarded-ryw", Skip::Local(&[HOLD_SYNC, PLACEMENT])),
    ("takeover-resolves-awaiting-close", Skip::Local(&[HOLD_SYNC, STOP])),
    ("session-stale-base-rename", Skip::Local(&[HOLD_SYNC, NODES3])),
    ("session-ryw-after-holder-kill", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    (
        "session-wait-degrades",
        Skip::Local(&[HOLD_SYNC, "shortens the session wait budget (CONSTELLATION_SESSION_WAIT_MS)"]),
    ),
    ("session-idle-latency", Skip::Local(&[NODES3])),
    ("visibility-after-burst", Skip::Local(&[NODES3, COUNT])),
    ("chaos-ci-strict", Skip::Local(&[NODES3, CHAOS, CTO_STRICT])),
    ("cto-strict", Skip::Local(&[CTO_STRICT, NODES3])),
    ("cto-bounded", Skip::Local(&[NODES3])),
    ("cto-delegation-recall", Skip::Local(&[CTO_STRICT, NODES3])),
    ("cto-recall-unreachable", Skip::Local(&[STOP, CTO_STRICT, NODES3])),
    ("cto-latency", Skip::Local(&[CTO_STRICT])),
    ("cto-second-node-joins", Skip::Local(&[CTO_STRICT])),
    ("cto-strict-root", Skip::Local(&[CTO_STRICT])),
    ("s3-cut-one-node", Skip::Local(&[S3_FAULTS])),
    ("s3-cut-create-holder-restart", Skip::Local(&[KILL, S3_FAULTS, P2P_CUT])),
    ("p2p-partition-one-node", Skip::Local(&[P2P_CUT, NODES4])),
    ("idle-cost", Skip::Local(&[COUNT, NODES4])),
    (
        "inbox-withdraw-hole",
        Skip::Local(&[P2P_CUT, "pauses inbox polling through the CONSTELLATION_FAULT_INBOX_POLL_PAUSE_FILE hook"]),
    ),
    ("idle-cost-link-flap", Skip::Local(&[P2P_CUT, COUNT, NODES4])),
    ("backup-failover", Skip::Local(&[KILL, NODES3])),
    ("backup-departs", Skip::Local(&[NODES3, COUNT])),
    (
        "no-peer-in-budget",
        Skip::Local(&[KILL, "turns backups off (CONSTELLATION_BACKUP_RTT_BUDGET_MS=0)", NODES3]),
    ),
    (
        "ack-s3-failover",
        Skip::Local(&[STOP, "creates the filesystem with CONSTELLATION_ACK=s3", NODES3]),
    ),
    ("single-node-unchanged", Skip::Local(&[COUNT])),
    ("backup-failover-with-delegation", Skip::Local(&[KILL, CTO_STRICT, NODES3])),
    ("holder-kill-rejoin", Skip::Local(&[KILL, NODES4])),
    ("fuse-inval-storm", Skip::Local(&[KILL, NODES3])),
    (
        "stale-daemon-lock",
        Skip::Local(&["holds a local daemon.lock and control socket with a process that never answers (CONSTELLATION_FAULT_ASSUME_WEDGED_PID)"]),
    ),
    ("backup-partition", Skip::Local(&[P2P_CUT, NODES3])),
    ("epoch-missing-node", Skip::Local(&[S3_FAULTS, P2P_OFF, NODES3])),
    ("epoch-member-dies-with-chunk", Skip::Local(&[STOP, S3_FAULTS, NODES3])),
    ("epoch-holder-retired", Skip::Local(&[KILL, S3_FAULTS, ADMIN])),
    (
        "epoch-slack-zero-unchanged",
        Skip::Local(&[COUNT, "sets epoch_slack on a local client"]),
    ),
    ("delegated-subtrees", Skip::Local(&[NODES3, COUNT])),
    ("cross-subtree-rename", Skip::Local(&[NODES3])),
    ("delegate-crash", Skip::Local(&[KILL, NODES3])),
    ("delegate-crash-default-ttl", Skip::Local(&[KILL, NODES3])),
    ("marker-order", Skip::Local(&[NODES3])),
    ("delegate-partition", Skip::Local(&[P2P_CUT, NODES3])),
    ("p2p-off-no-delegation", Skip::Local(&[P2P_OFF])),
    (
        "flock-cross-node",
        Skip::Covered(
            &["its second phase mounts with CONSTELLATION_LOCKS=local"],
            "csi-cross-pod-locks",
        ),
    ),
    ("concurrent-create-no-excl", Skip::Local(&[S3_FAULTS, NODES4])),
    ("nonowner-op-latency", Skip::Local(&[S3_FAULTS, NODES4])),
    ("delegated-op-latency", Skip::Local(&[S3_FAULTS, PLACEMENT, NODES4])),
    ("slow-s3-no-seal", Skip::Local(&[S3_FAULTS, NODES3])),
    ("visibility-s3-latency", Skip::Local(&[S3_FAULTS, NODES3])),
    ("sqlite-first-touch-latency", Skip::Local(&[S3_FAULTS, NODES3])),
    ("sqlite-two-nodes", Skip::Local(&[CTO_STRICT])),
    (
        "lock-holder-partitioned",
        Skip::Local(&[P2P_CUT, NODES3, "shortens the grant TTL (CONSTELLATION_LOCK_TTL_MS)"]),
    ),
    ("lock-failover", Skip::Local(&[KILL, NODES4])),
    ("lock-holder-killed-contention", Skip::Local(&[KILL, NODES4])),
    ("lock-fence-at-close", Skip::Local(&[P2P_CUT, NODES3])),
    (
        "lock-latency",
        Skip::Local(&["compares against mounts with CONSTELLATION_LOCKS=local"]),
    ),
    ("root-failover-with-delegates", Skip::Local(&[KILL, NODES4])),
    ("delegate-crash-backup", Skip::Local(&[KILL, NODES3])),
    ("delegate-root-loss", Skip::Local(&[KILL])),
    ("delegate-root-blackhole", Skip::Local(&[STOP])),
    (
        "delegate-root-loss-ttl",
        Skip::Local(&[STOP, "turns backups off (CONSTELLATION_BACKUP_RTT_BUDGET_MS)"]),
    ),
    ("delegate-handoff-renewal", Skip::Local(&[UPGRADE])),
    ("delegate-backup-handoff-failover", Skip::Local(&[UPGRADE, KILL])),
    (
        "auto-placement",
        Skip::Local(&[NODES3, "tunes automatic delegation (CONSTELLATION_DELEGATION_*)"]),
    ),
    (
        "designation-as-delegation",
        Skip::Local(&[NODES3, "takes a subtree offline (`offline /site`) through a local client"]),
    ),
    ("shared-dir-multi-writer", Skip::Local(&[NODES4, COUNT])),
    (
        "hash-range-split-merge",
        Skip::Local(&[NODES4, "tunes automatic delegation (CONSTELLATION_DELEGATION_*)"]),
    ),
    ("cross-range-rename", Skip::Local(&[NODES3])),
    ("inbox-create-storm-p2p-off", Skip::Local(&[P2P_OFF, NODES3])),
    ("dedup-write-storm", Skip::Local(&[P2P_OFF, WRITE_MODE])),
    ("inbox-sporadic-write-p2p-off", Skip::Local(&[P2P_OFF, NODES3])),
    ("inbox-requester-crash-mid-batch", Skip::Local(&[KILL, P2P_OFF, NODES3])),
    ("small-file-write-path", Skip::Local(&[S3_FAULTS, COUNT, NODES3])),
    ("nonowner-back-crash", Skip::Local(&[KILL, S3_FAULTS, NODES3])),
    ("inbox-holder-takeover-pending-batch", Skip::Local(&[KILL, P2P_OFF, NODES3])),
    (
        "subtree-confinement",
        Skip::Unported("`..`, symlinks and hard links at a view root, which is what a pool PV is; its maintenance view would be a static PV of the pool root"),
    ),
    (
        "session-handover-idle",
        Skip::Covered(
            &[UPGRADE],
            "csi-engine-pod-handoff-under-load and csi-plugin-restart-survives",
        ),
    ),
    (
        "upgrade-under-load",
        Skip::Covered(&[UPGRADE], "csi-engine-pod-handoff-under-load"),
    ),
    ("transport-detach-refused", Skip::Local(&[TRANSPORT, UPGRADE])),
    (
        "transport-refused-registration",
        Skip::Local(&[TRANSPORT, "makes the kernel refuse ring registration (CONSTELLATION_FUSE_URING_FAULT)"]),
    ),
    (
        "transport-seccomp-denied",
        Skip::Local(&[TRANSPORT, "runs a local daemon under a seccomp filter of the harness's"]),
    ),
    ("transport-enomem-ring", Skip::Local(&[TRANSPORT, RSS])),
    (
        "transport-abort-while-armed",
        Skip::Local(&[TRANSPORT, "aborts the FUSE connection through fusectl"]),
    ),
    ("passthrough-eviction-while-open", Skip::Local(&[TRANSPORT, CACHE])),
    ("passthrough-remote-write-cto", Skip::Local(&[TRANSPORT])),
    ("passthrough-local-writer", Skip::Local(&[TRANSPORT])),
    ("passthrough-odirect", Skip::Local(&[TRANSPORT])),
    ("passthrough-handover", Skip::Local(&[TRANSPORT, UPGRADE])),
    ("passthrough-disabled-by-verify-always", Skip::Local(&[TRANSPORT])),
    ("passthrough-default-by-mount-mode", Skip::Local(&[TRANSPORT])),
    ("passthrough-on-every-transport", Skip::Local(&[TRANSPORT])),
    ("zero-copy-single-chunk", Skip::Local(&[TRANSPORT])),
    ("zero-copy-chunk-spanning-fallback", Skip::Local(&[TRANSPORT])),
    ("zero-copy-eviction-while-inflight", Skip::Local(&[TRANSPORT, CACHE])),
    ("zero-copy-disabled-by-verify-always", Skip::Local(&[TRANSPORT])),
    ("transport-cluster-locks-auto", Skip::Local(&[TRANSPORT])),
    ("transport-lock-wait-budget", Skip::Local(&[TRANSPORT])),
    ("lifecycle-suspend-mid-write", Skip::Local(&[LIFECYCLE, NODES3])),
    ("lifecycle-resume-rejoin", Skip::Local(&[LIFECYCLE, NODES3])),
    ("lifecycle-metered-uploads", Skip::Local(&[LIFECYCLE])),
    ("writeback-close-metered-nonowner", Skip::Local(&[LIFECYCLE, S3_FAULTS])),
];

impl Skip {
    /// The skip reason the results file carries.
    pub fn reason(&self) -> String {
        match self {
            Skip::Local(levers) => {
                format!("requires capability {LOCAL_CLIENT}: {}", levers.join("; "))
            }
            Skip::Covered(levers, k8s) => format!(
                "requires capability {LOCAL_CLIENT}: {}; covered through the driver by {k8s}",
                levers.join("; ")
            ),
            Skip::Unported(what) => {
                format!("not yet ported to pods (follow-up F6): needs only {what}")
            }
        }
    }

    /// The `k8s-scenario`s covering it, if any.
    pub fn covered_by(&self) -> Vec<&'static str> {
        match self {
            Skip::Covered(_, k8s) => k8s.split(" and ").collect(),
            _ => Vec::new(),
        }
    }
}

/// Every [`SCENARIOS`](crate::scenarios::SCENARIOS) entry the lane skips, with its reason, in catalog
/// order.
pub fn skipped() -> Vec<(&'static str, String)> {
    SKIPPED
        .iter()
        .map(|(name, skip)| (*name, skip.reason()))
        .collect()
}

/// `pod`'s view of directory `dir` (absolute, in the pod) equals `model`.
fn verify_at(s: &Scope, pod: &str, dir: &str, model: &Model) -> Result<()> {
    let seen = s.listing(pod, dir)?;
    model
        .verify_observed(&seen)
        .with_context(|| format!("{pod}'s view of {dir}"))
}

/// `spool.conflicts` of every node-owned engine pod serving `claim`'s
/// pool: the leaseless conflict path must never fire (the local scenarios'
/// `conflicts_of`, asked of the engine pods instead of local daemons).
fn ensure_no_conflicts(s: &Scope, claim: &str) -> Result<()> {
    let uuid = fs_uuid_of(&s.volume_handle(claim)?)?;
    let pods = s.engine_pods(&uuid, "node")?;
    ensure!(!pods.is_empty(), "no node-owned engine pod serves {claim}");
    for pod in &pods {
        let status = s.env.kube.engine_call(
            &s.env.driver_ns,
            &pod.name,
            "node.status",
            serde_json::json!({}),
        )?;
        ensure!(
            status["spool"]["conflicts"].as_u64() == Some(0),
            "engine pod {} on {}: the leaseless conflict path fired: {}",
            pod.name,
            pod.node,
            status["spool"]
        );
    }
    Ok(())
}

/// `'...'` for `sh`.
fn q(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// The contents of `files` (relative to `dir`) as `pod` reads them.
fn read_files(
    s: &Scope,
    pod: &str,
    dir: &str,
    files: &[&str],
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut script = format!("set -eu\ncd {}\n", q(dir));
    for f in files {
        script.push_str(&format!("echo '== {f}'\nod -An -v -tx1 {}\n", q(f)));
    }
    let out = s.exec(pod, &script)?;
    let mut got: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut cur: Option<String> = None;
    for line in out.lines() {
        if let Some(name) = line.strip_prefix("== ") {
            got.insert(name.to_string(), Vec::new());
            cur = Some(name.to_string());
        } else if let Some(name) = &cur {
            let bytes = got.get_mut(name).expect("inserted above");
            for b in line.split_whitespace() {
                bytes.push(u8::from_str_radix(b, 16).with_context(|| format!("od byte {b:?}"))?);
            }
        }
    }
    Ok(got)
}

/// `baseline`: one mount, five blocks of 60 seeded operations, the tree
/// verified after each. Port: the block runs as one shell script in the
/// pod ([`RemoteWorkload`]: the same op mix as `Workload`, smaller file
/// contents because they travel in the script).
fn baseline(env: &Env, seed: u64) -> Result<()> {
    let w1 = &env.workers[0];
    let mut s = Scope::new(env, "baseline")?;
    s.pvc("w", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("c", w1, &["w"])?;
    let mut model = Model::default();
    let mut wl = RemoteWorkload::new(seed, "w");
    for block in 0..5 {
        let script = wl.block(&data_dir("w"), &mut model, 60);
        s.exec("c", &script)
            .with_context(|| format!("block {block}"))?;
        verify(&s, "c", "w", &model).with_context(|| format!("block {block}"))?;
    }
    eprintln!(
        "   5 blocks, {} entries, verified per block",
        model.nodes.len()
    );
    s.finish()
}

/// `two-clients-disjoint`: two mounts of two filesystems, each with its own
/// seeded workload, three blocks of 40, both verified after each. Port:
/// the two filesystems are two pools (one StorageClass each), the two
/// mounts pods on the two workers.
fn two_clients_disjoint(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "two-clients-disjoint")?;
    let other = s.add_class("b")?;
    s.pvc("a", "1Gi", false)?;
    s.pvc_of_class("b", "1Gi", false, &other)?;
    s.wait_bound(Duration::from_secs(300))?;
    ensure!(
        fs_uuid_of(&s.volume_handle("a")?)? != fs_uuid_of(&s.volume_handle("b")?)?,
        "the two PVs share a filesystem"
    );
    s.pod("c0", w1, &["a"])?;
    s.pod("c1", w2, &["b"])?;
    let (mut m0, mut m1) = (Model::default(), Model::default());
    let mut wl0 = RemoteWorkload::new(seed, "a");
    let mut wl1 = RemoteWorkload::new(seed.wrapping_add(1), "b");
    for block in 0..3 {
        let script = wl0.block(&data_dir("a"), &mut m0, 40);
        s.exec("c0", &script)
            .with_context(|| format!("c0 block {block}"))?;
        let script = wl1.block(&data_dir("b"), &mut m1, 40);
        s.exec("c1", &script)
            .with_context(|| format!("c1 block {block}"))?;
        verify(&s, "c0", "a", &m0).with_context(|| format!("c0 block {block}"))?;
        verify(&s, "c1", "b", &m1).with_context(|| format!("c1 block {block}"))?;
    }
    s.finish()
}

/// `two-clients-shared`: two mounts of one filesystem, each writing its own
/// subtree; each sees its own writes at once, the other's within the
/// close-to-open bound, and neither node's conflict path fires. Port: one
/// RWX PV, a pod on each worker (two engine pods, two nodes of the pool);
/// the local scenario's 20 s and 30 s waits are [`CONVERGE`] here, the
/// bound the other k8s scenarios use (a `kubectl exec` per poll).
fn two_clients_shared(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "two-clients-shared")?;
    s.pvc("shared", "1Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("c0", w1, &["shared"])?;
    s.pod("c1", w2, &["shared"])?;
    let root = data_dir("shared");
    s.exec("c0", &format!("mkdir {root}/a"))?;
    s.exec("c1", &format!("mkdir {root}/b"))?;
    eventually("subtrees visible on both", CONVERGE, || {
        s.exec("c0", &format!("test -d {root}/b"))
            .context("b not on c0")?;
        s.exec("c1", &format!("test -d {root}/a"))
            .context("a not on c1")?;
        Ok(())
    })?;
    let (da, db) = (format!("{root}/a"), format!("{root}/b"));
    let (mut m0, mut m1) = (Model::default(), Model::default());
    let mut wl0 = RemoteWorkload::new(seed, "a");
    let mut wl1 = RemoteWorkload::new(seed.wrapping_add(1), "b");
    for block in 0..3 {
        let script = wl0.block(&da, &mut m0, 30);
        s.exec("c0", &script)
            .with_context(|| format!("c0 block {block}"))?;
        let script = wl1.block(&db, &mut m1, 30);
        s.exec("c1", &script)
            .with_context(|| format!("c1 block {block}"))?;
        verify_at(&s, "c0", &da, &m0).with_context(|| format!("c0 local, block {block}"))?;
        verify_at(&s, "c1", &db, &m1).with_context(|| format!("c1 local, block {block}"))?;
        eventually(
            &format!("cross-node convergence, block {block}"),
            CONVERGE,
            || {
                verify_at(&s, "c1", &da, &m0).context("c0's tree via c1")?;
                verify_at(&s, "c0", &db, &m1).context("c1's tree via c0")?;
                Ok(())
            },
        )?;
    }
    ensure_no_conflicts(&s, "shared")?;
    s.finish()
}

/// `git-workflow`: A stages a tree and publishes it with an atomic rename;
/// B sees exactly it, edits, restructures and publishes back; A sees
/// exactly that; a last overwrite shrinks a file. Leases serialize the
/// ping-pong, so neither node's conflict path fires. Port: pods on two
/// workers sharing an RWX PV; the waits are [`CONVERGE`].
fn git_workflow(env: &Env, _seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "git-workflow")?;
    s.pvc("repo", "1Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("c0", w1, &["repo"])?;
    s.pod("c1", w2, &["repo"])?;
    let root = data_dir("repo");

    // A: stage, then publish atomically.
    s.exec(
        "c0",
        &format!(
            "set -eu\ncd {root}\nmkdir -p stage/src\n\
             printf 'int main(){{return 0;}}\\n' > stage/src/main.c\n\
             printf '# demo v1\\n' > stage/README.md\nmv stage repo"
        ),
    )?;
    // B: sees the published tree, exactly.
    eventually("repo published on B", CONVERGE, || {
        s.exec("c1", &format!("test ! -e {root}/stage"))
            .context("stage leaked")?;
        let f = read_files(&s, "c1", &root, &["repo/README.md", "repo/src/main.c"])?;
        ensure!(f["repo/README.md"] == b"# demo v1\n", "README content");
        ensure!(
            f["repo/src/main.c"] == b"int main(){return 0;}\n",
            "main.c content"
        );
        Ok(())
    })?;
    // B: edit, restructure, publish back.
    s.exec(
        "c1",
        &format!(
            "set -eu\ncd {root}/repo\nprintf 'edited on B\\n' >> README.md\n\
             printf 'cc src/*.c\\n' > BUILD\nrm src/main.c\nmv src lib"
        ),
    )?;
    // A: sees B's edits, exactly.
    eventually("B's edits visible on A", CONVERGE, || {
        let f = read_files(&s, "c0", &root, &["repo/README.md", "repo/BUILD"])?;
        ensure!(
            f["repo/README.md"] == b"# demo v1\nedited on B\n",
            "README round 2"
        );
        ensure!(f["repo/BUILD"] == b"cc src/*.c\n", "BUILD content");
        s.exec(
            "c0",
            &format!("set -eu\ncd {root}/repo\ntest ! -e src\ntest -d lib\ntest ! -e lib/main.c"),
        )
        .context("src renamed to lib, main.c deleted")?;
        Ok(())
    })?;
    // A: final round trip (the overwrite shrinks the file).
    s.exec("c0", &format!("printf 'v3\\n' > {root}/repo/README.md"))?;
    eventually("round 3 on B", CONVERGE, || {
        let f = read_files(&s, "c1", &root, &["repo/README.md"])?;
        ensure!(
            f["repo/README.md"] == b"v3\n",
            "README round 3: {:?}",
            String::from_utf8_lossy(&f["repo/README.md"])
        );
        Ok(())
    })?;
    ensure_no_conflicts(&s, "repo")?;
    s.finish()
}

/// `truncate-never-resurrects`: bytes cut by a truncate never come back
/// when a later write extends the file past them — `ftruncate` then a
/// write past the gap on each node, the truncate on one node and the write
/// on the other, `O_TRUNC` then a write past a gap, truncate down then up,
/// and `fallocate` up after a truncate down. Every file reads back exactly,
/// on both nodes, live; then again after a cold remount, and on a fresh
/// node.
///
/// Port: pods on two workers sharing an RWX PV (two engine pods). The
/// operations are coreutils': `truncate -s` and the write past the gap
/// (`dd conv=notrunc seek=`) open the file separately where the original
/// used one descriptor (`sh` has no `ftruncate`), and `fallocate -l` is
/// `fallocate(2)` mode 0 from offset 0 as in the original. The original's
/// clients run with lease placement off; the engine pods run the driver's
/// defaults. The cold remount and the fresh node are one step here: the
/// pods go, their volume unstages on both workers, the engine pods are
/// collected idle (the chart's TTL), and new pods start new engine pods —
/// new node identities with empty caches, bootstrapped from the bucket.
fn truncate_never_resurrects(env: &Env, _seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "truncate-never-resurrects")?;
    s.pvc("t", "1Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("a", w1, &["t"])?;
    s.pod("b", w2, &["t"])?;
    let root = data_dir("t");
    s.exec("a", &format!("mkdir {root}/t"))?;
    eventually("t on B", CONVERGE, || {
        s.exec("b", &format!("test -d {root}/t")).map(|_| ())
    })?;
    let gap = |head: &[u8], at: usize, tail: &[u8]| -> Vec<u8> {
        let mut v = head.to_vec();
        v.resize(at, 0);
        v.extend_from_slice(tail);
        v
    };
    // `printf DATA | dd` into `file` at `seek`, without truncating it.
    let write_at = |file: &str, seek: usize, data: &str| {
        format!("printf '{data}' | dd of={file} bs=1 seek={seek} conv=notrunc status=none\n")
    };
    let mut expect: Vec<(String, Vec<u8>)> = Vec::new();
    let sh = |pod: &str, body: String| s.exec(pod, &format!("set -eu\ncd {root}\n{body}"));

    // 1. Truncate then a write past the gap, on each node.
    for who in ["a", "b"] {
        let name = format!("t/ftrunc-{who}");
        sh(
            who,
            format!(
                "printf abcdefgh > {name}\ntruncate -s 2 {name}\n{}",
                write_at(&name, 5, "X")
            ),
        )?;
        expect.push((name, gap(b"ab", 5, b"X")));
    }
    // 2. The truncate on B, the write past the gap on A.
    sh("a", "printf abcdefghijklmnop > t/cross".into())?;
    eventually("cross on B", CONVERGE, || {
        let f = read_files(&s, "b", &root, &["t/cross"])?;
        ensure!(f["t/cross"] == b"abcdefghijklmnop");
        Ok(())
    })?;
    sh("b", "truncate -s 3 t/cross".into())?;
    eventually("B's truncate on A", CONVERGE, || {
        let size = s.exec("a", &format!("stat -c %s {root}/t/cross"))?;
        ensure!(size.trim() == "3", "size {}", size.trim());
        Ok(())
    })?;
    sh("a", write_at("t/cross", 10, "Y"))?;
    expect.push(("t/cross".into(), gap(b"abc", 10, b"Y")));
    // 3. O_TRUNC, then a write past a gap (on B).
    sh(
        "b",
        format!(
            "printf 0123456789 > t/otrunc\n: > t/otrunc\n{}",
            write_at("t/otrunc", 5, "Z")
        ),
    )?;
    expect.push(("t/otrunc".into(), gap(b"", 5, b"Z")));
    // 4. Truncate down then up; then fallocate up (on A).
    sh(
        "a",
        "printf abcdefgh > t/updown\ntruncate -s 2 t/updown\ntruncate -s 8 t/updown".into(),
    )?;
    expect.push(("t/updown".into(), gap(b"ab", 8, b"")));
    sh(
        "a",
        "printf abcdefgh > t/falloc\ntruncate -s 2 t/falloc\nfallocate -l 8 t/falloc".into(),
    )?;
    expect.push(("t/falloc".into(), gap(b"ab", 8, b"")));

    let names: Vec<&str> = expect.iter().map(|(n, _)| n.as_str()).collect();
    let check = |pod: &str, when: &str| -> Result<()> {
        eventually(
            &format!("{pod} reads every file ({when})"),
            CONVERGE,
            || {
                let got = read_files(&s, pod, &root, &names)?;
                for (name, want) in &expect {
                    let got = &got[name];
                    ensure!(
                        got == want,
                        "{pod} ({when}): {name} is {:?}, want {:?}",
                        String::from_utf8_lossy(got),
                        String::from_utf8_lossy(want)
                    );
                }
                Ok(())
            },
        )
    };
    check("a", "live")?;
    check("b", "live")?;

    // Cold remount on fresh nodes: both pods go, the volume unstages, the
    // pool's node-owned engine pods are collected, new pods start new ones.
    let uuid = fs_uuid_of(&s.volume_handle("t")?)?;
    let old: Vec<String> = s
        .engine_pods(&uuid, "node")?
        .into_iter()
        .map(|p| p.uid)
        .collect();
    s.delete_pods(&["a", "b"])?;
    eventually(
        "the pool's node-owned engine pods collected idle",
        Duration::from_secs(super::IDLE_TTL_S * 3 + 60),
        || {
            let left = s.engine_pods(&uuid, "node")?;
            ensure!(left.is_empty(), "{} engine pod(s) left", left.len());
            Ok(())
        },
    )?;
    s.pod("a2", w1, &["t"])?;
    s.pod("b2", w2, &["t"])?;
    let fresh = s.engine_pods(&uuid, "node")?;
    ensure!(
        fresh.len() == 2 && fresh.iter().all(|p| !old.contains(&p.uid)),
        "expected two new engine pods, got {:?}",
        fresh.iter().map(|p| (&p.name, &p.node)).collect::<Vec<_>>()
    );
    check("a2", "fresh engine pod, cold cache")?;
    check("b2", "fresh engine pod, cold cache")?;
    s.finish()
}

/// `pod`'s answer to `perl` [`POD_TOOL`] `ARGS`, trimmed.
fn tool(s: &Scope, pod: &str, args: &str) -> Result<String> {
    Ok(s.exec(pod, &format!("perl {POD_TOOL} {args}"))?
        .trim()
        .to_string())
}

/// The pool's node-owned engine pods serving `claim` collected idle (the
/// chart's TTL), once every pod using it is gone: the next pod starts a
/// new engine pod, a fresh node with an empty cache.
fn await_engine_pods_collected(s: &Scope, claim: &str) -> Result<()> {
    let uuid = fs_uuid_of(&s.volume_handle(claim)?)?;
    eventually(
        &format!("{claim}'s node-owned engine pods collected idle"),
        Duration::from_secs(super::IDLE_TTL_S * 3 + 60),
        || {
            let left = s.engine_pods(&uuid, "node")?;
            ensure!(left.is_empty(), "{} engine pod(s) left", left.len());
            Ok(())
        },
    )
}

/// `xattr-roundtrip`: a file's and a directory's `user.` xattrs set on one
/// node reach the other; the directory's `user.constellation.rsize` and
/// `rcount` (recursive logical bytes and files) count a 1 GiB sparse file
/// at its logical size; a removal reaches the other node.
///
/// Port: pods on two workers sharing an RWX PV; the xattr calls are the
/// pod tool's (`setxattr(2)` and kin from `perl`), the sparse file is
/// `truncate -s 1G`, and the waits are [`CONVERGE`] (20 s locally).
///
/// A pool PV is a subtree view of the pool's filesystem, so the port also
/// settles what the recursive totals report through one: those of the
/// directory asked, in the filesystem — `tree` reports what it reports on
/// a plain mount, and the PV's root reports the volume's own bytes and
/// files, never the pool's. A file in a second PV of the same pool, staged
/// on the same engine pod, must not count.
fn xattr_roundtrip(env: &Env, _seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "xattr-roundtrip")?;
    s.pvc("x", "2Gi", true)?;
    s.pvc("other", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    ensure!(
        fs_uuid_of(&s.volume_handle("x")?)? == fs_uuid_of(&s.volume_handle("other")?)?,
        "the two PVs of one pool class are in different filesystems"
    );
    s.pod("a", w1, &["x", "other"])?;
    s.pod("b", w2, &["x"])?;
    s.install_pod_tool("a")?;
    s.install_pod_tool("b")?;
    let (root, other) = (data_dir("x"), data_dir("other"));
    s.exec(
        "a",
        &format!(
            "set -eu\ncd {root}\nmkdir tree\nprintf 'seven!!' > tree/file\n\
             truncate -s 1073741824 tree/sparse\n\
             perl {POD_TOOL} xset tree/file user.foo file-value\n\
             perl {POD_TOOL} xset tree user.foo dir-value\n\
             head -c 4096 /dev/zero > {other}/pad"
        ),
    )?;
    eventually("xattrs visible on b", CONVERGE, || {
        let f = tool(&s, "b", &format!("xget {root}/tree/file user.foo"))?;
        ensure!(f == "file-value", "file xattr on b: {f:?}");
        let d = tool(&s, "b", &format!("xget {root}/tree user.foo"))?;
        ensure!(d == "dir-value", "directory xattr on b: {d:?}");
        Ok(())
    })?;
    let expected = (1u64 << 30) + 7;
    let totals = |dir: &str| -> Result<(u64, u64)> {
        let rsize = tool(&s, "b", &format!("xget {dir} user.constellation.rsize"))?;
        let rcount = tool(&s, "b", &format!("xget {dir} user.constellation.rcount"))?;
        Ok((
            rsize
                .parse()
                .with_context(|| format!("rsize of {dir}: {rsize:?}"))?,
            rcount
                .parse()
                .with_context(|| format!("rcount of {dir}: {rcount:?}"))?,
        ))
    };
    let (rsize, rcount) = totals(&format!("{root}/tree"))?;
    ensure!(rsize == expected, "rsize of tree {rsize} != {expected}");
    ensure!(rcount == 2, "rcount of tree {rcount} != 2");
    let top = s.exec("b", &format!("ls -A {root}"))?;
    ensure!(
        top.trim() == "tree",
        "the PV's root holds {top:?}, not just tree"
    );
    let (root_size, root_count) = totals(&root)?;
    ensure!(
        (root_size, root_count) == (expected, 2),
        "the PV root's totals are ({root_size}, {root_count}), want the volume's own ({expected}, 2)"
    );

    tool(&s, "a", &format!("xrm {root}/tree/file user.foo"))?;
    eventually("xattr removal visible on b", CONVERGE, || {
        let f = tool(&s, "b", &format!("xget {root}/tree/file user.foo"))?;
        // ENODATA.
        ensure!(f == "ERRNO 61", "removed xattr on b: {f:?}");
        Ok(())
    })?;
    eprintln!("    xattr-roundtrip: rsize={rsize} rcount={rcount}, the PV root the same");
    s.finish()
}

/// `fallocate-sparse`: a 256 MiB sparse file with `HEAD` and `TAIL`
/// markers and three 4 MiB runs of data in its middle; the middle run is
/// punched out (`FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`) and reads
/// back as zeros, `SEEK_HOLE` / `SEEK_DATA` find the hole's bounds, a byte
/// rewritten into the hole persists; the bucket holds a few chunk objects,
/// not a materialised file; a fresh node reads the markers, the rewrite
/// and the zeros.
///
/// Port: one pod on an RWO PV, then one on the other worker. The
/// operations are coreutils' and util-linux's (`truncate`, `dd
/// conv=notrunc`, `fallocate -p`, `sync FILE`) and the pod tool's `seek`;
/// the middle runs are random bytes (the original's seeded patterns are
/// not read back either). The upload drain is the engine pods'
/// `writeback.pending_uploads`, the chunk count the pool's `chunks/` keys
/// in the bucket. Not ported: the original's 32 MiB `--cache-size`, 16 MiB
/// staging budget and RSS ceiling — an engine pod's cache is the chart's
/// engine profile and its memory the pod's. The fresh node: the first pod
/// goes and its engine pod is collected idle before the second pod's
/// engine pod starts, so it reads through the bucket, as the original's
/// second client does after the first unmounted.
fn fallocate_sparse(env: &Env, _seed: u64) -> Result<()> {
    const LEN: u64 = 256 << 20;
    const RUN: u64 = 4 << 20;
    const PUNCH: u64 = 9 * RUN;
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "fallocate-sparse")?;
    s.pvc("s", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("a", w1, &["s"])?;
    s.install_pod_tool("a")?;
    let f = format!("{}/sparse.bin", data_dir("s"));
    s.exec(
        "a",
        &format!(
            "set -eu\nf={f}\n: > $f\ntruncate -s {LEN} $f\n\
             printf HEAD | dd of=$f conv=notrunc status=none\n\
             printf TAIL | dd of=$f bs=1 seek={tail} conv=notrunc status=none\n\
             for i in 8 9 10; do\n  head -c {RUN} /dev/urandom \
             | dd of=$f bs={RUN} seek=$i conv=notrunc iflag=fullblock status=none\ndone\n\
             sync $f\nfallocate -p -o {PUNCH} -l {RUN} $f\nsync $f",
            tail = LEN - 4
        ),
    )
    .context("writing and punching")?;
    let nonzero = s.exec(
        "a",
        &format!(
            "dd if={f} bs={RUN} skip={} count=1 status=none | tr -d '\\000' | wc -c",
            PUNCH / RUN
        ),
    )?;
    ensure!(
        nonzero.trim() == "0",
        "punched run holds {} non-zero bytes",
        nonzero.trim()
    );
    let seek = tool(&s, "a", &format!("seek {f} {PUNCH}"))?;
    ensure!(
        seek == format!("{PUNCH} {}", PUNCH + RUN),
        "SEEK_HOLE / SEEK_DATA from {PUNCH}: {seek}, want {PUNCH} {}",
        PUNCH + RUN
    );
    s.exec(
        "a",
        &format!(
            "set -eu\nprintf R | dd of={f} bs=1 seek={} conv=notrunc status=none\nsync {f}",
            PUNCH + 123
        ),
    )?;
    let uuid = fs_uuid_of(&s.volume_handle("s")?)?;
    eventually("sparse uploads drain", CONVERGE, || {
        for pod in s.engine_pods(&uuid, "node")? {
            let st = s.env.kube.engine_call(
                &s.env.driver_ns,
                &pod.name,
                "node.status",
                serde_json::json!({}),
            )?;
            let pending = &st["writeback"]["pending_uploads"];
            ensure!(
                pending.as_u64() == Some(0),
                "{}: pending_uploads {pending}",
                pod.name
            );
        }
        Ok(())
    })?;
    let chunks = s
        .env
        .bucket_keys(&format!("{}/chunks", s.pool_prefix()))?
        .len();
    ensure!(
        (1..24).contains(&chunks),
        "the sparse file left {chunks} chunk objects (want 1..24)"
    );
    let size = s.exec("a", &format!("stat -c %s {f}"))?;
    ensure!(
        size.trim() == LEN.to_string(),
        "size {} after the punch",
        size.trim()
    );

    s.delete_pods(&["a"])?;
    await_engine_pods_collected(&s, "s")?;
    s.pod("b", w2, &["s"])?;
    let read = |skip: u64, count: u64| -> Result<Vec<u8>> {
        let out = s.exec(
            "b",
            &format!("dd if={f} bs=1 skip={skip} count={count} status=none | od -An -v -tx1"),
        )?;
        out.split_whitespace()
            .map(|b| u8::from_str_radix(b, 16).with_context(|| format!("od byte {b:?}")))
            .collect()
    };
    ensure!(read(0, 4)? == b"HEAD", "HEAD on the fresh node");
    ensure!(read(LEN - 4, 4)? == b"TAIL", "TAIL on the fresh node");
    ensure!(
        read(PUNCH + 122, 3)? == [0, b'R', 0],
        "the rewrite in the hole did not persist"
    );
    ensure!(
        read(PUNCH + 4096, 64)? == [0u8; 64],
        "the fresh node did not read the hole as zeros"
    );
    eprintln!("    fallocate-sparse: 256 MiB file, {chunks} chunk objects");
    s.finish()
}

/// `append-setattr-size` (the K5 kind lane's busy-writer loss): one
/// `O_APPEND` descriptor appends 64 KiB blocks for 8 s while a second
/// descriptor `fdatasync`s the file every second and its attributes change
/// every ~100 ms (a chown to the caller's group, `chmod 664`, `utimes`, a
/// hard link made and removed — the kubelet's fsGroup pass); first on one
/// node, then on the other. Every acknowledged block is in the file, in
/// order, on the writer, on the other node and on a fresh node.
///
/// Port: pods on two workers sharing an RWX PV, `b`'s writing first. The
/// appender and its checker are the pod tool's `append` and `verify` (the
/// original's block format); the attribute changer and the syncer are
/// shell loops beside it (`chgrp`, `chmod`, `touch`, `ln`/`rm`, `sync -d`),
/// and a leg stops at 12288 blocks (768 MiB), inside the PV's 2 GiB quota. The
/// original pins the lease on `a` (lease placement off) so that `b`'s
/// publications are forwarded, as on kind; here the engine pods run the
/// driver's defaults — the K5 topology itself. The fresh node is a new
/// engine pod, as in `truncate-never-resurrects`' port.
fn append_setattr_size(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "append-setattr-size")?;
    s.pvc("busy", "2Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("a", w1, &["busy"])?;
    s.pod("b", w2, &["busy"])?;
    s.install_pod_tool("a")?;
    s.install_pod_tool("b")?;
    let root = data_dir("busy");
    let verify = |pod: &str, name: &str, seed: u64, blocks: u64| -> Result<()> {
        let got = tool(&s, pod, &format!("verify {root}/{name} {seed} {blocks}"))?;
        ensure!(got == "OK", "{pod}: {got}");
        Ok(())
    };
    let mut legs: Vec<(String, u64, u64)> = Vec::new();
    for (writer, leg_seed) in [("b", seed), ("a", seed.wrapping_add(1))] {
        let name = format!("busy-{writer}");
        let out = s
            .exec(
                writer,
                &format!(
                    "set -eu\ncd {root}\nf={name}\n: > $f\nrm -f /tmp/stop\n\
                     ( n=0; while [ ! -e /tmp/stop ]; do\n\
                       case $((n % 4)) in\n\
                       0) chgrp $(id -g) $f ;;\n1) chmod 664 $f ;;\n2) touch $f ;;\n\
                       *) ln $f $f.lnk; rm $f.lnk ;;\n\
                       esac; n=$((n + 1)); sleep 0.1\n\
                     done; echo $n > /tmp/setattrs ) &\nch=$!\n\
                     ( n=0; while [ ! -e /tmp/stop ]; do\n\
                       sleep 1; sync -d $f; n=$((n + 1))\n\
                     done; echo $n > /tmp/fsyncs ) &\nsy=$!\n\
                     blocks=$(perl {POD_TOOL} append $f {leg_seed} 8 12288) || {{ touch /tmp/stop; exit 1; }}\n\
                     touch /tmp/stop\nwait $ch\nwait $sy\n\
                     echo $blocks $(cat /tmp/setattrs) $(cat /tmp/fsyncs)"
                ),
            )
            .with_context(|| format!("{writer}'s leg"))?;
        let n: Vec<u64> = out
            .split_whitespace()
            .map(|v| v.parse().with_context(|| format!("leg report {out:?}")))
            .collect::<Result<_>>()?;
        ensure!(n.len() == 3, "leg report {out:?}");
        let (blocks, setattrs, fsyncs) = (n[0], n[1], n[2]);
        eprintln!(
            "    append-setattr-size: {writer} appended {blocks} blocks ({} MiB) under \
             {setattrs} attribute changes and {fsyncs} fsyncs",
            blocks >> 4
        );
        ensure!(blocks > 0 && setattrs > 0, "{writer}: the leg did not run");
        verify(writer, &name, leg_seed, blocks).with_context(|| format!("{writer} (writer)"))?;
        legs.push((name, leg_seed, blocks));
    }
    for (other, (name, leg_seed, blocks)) in [("a", &legs[0]), ("b", &legs[1])] {
        eventually(&format!("{other} reads {name} whole"), CONVERGE, || {
            verify(other, name, *leg_seed, *blocks)
        })?;
    }
    s.delete_pods(&["a", "b"])?;
    await_engine_pods_collected(&s, "busy")?;
    s.pod("c", w1, &["busy"])?;
    s.install_pod_tool("c")?;
    for (name, leg_seed, blocks) in &legs {
        eventually(&format!("c reads {name} whole"), CONVERGE, || {
            verify("c", name, *leg_seed, *blocks).context("c (fresh engine pod)")
        })?;
    }
    s.finish()
}

#[cfg(test)]
mod parity_tests {
    use super::*;
    use crate::scenarios::SCENARIOS;

    #[test]
    fn every_ported_scenario_is_a_harness_run_scenario() {
        for p in PORTED {
            assert!(
                SCENARIOS.iter().any(|s| s.name == p.name),
                "{} is not in harness list",
                p.name
            );
            assert!(
                !super::super::K8S_SCENARIOS.iter().any(|k| k.name == p.name),
                "{} would collide with a k8s-scenario",
                p.name
            );
        }
    }

    /// Every catalog name is classified by hand: ported, or in
    /// [`SKIPPED`] — never both, never twice, in catalog order — so a
    /// scenario added to the catalog fails here until someone decides what
    /// this lane does with it.
    #[test]
    fn every_harness_scenario_is_ported_or_listed_by_hand() {
        let unlisted: Vec<&str> = SCENARIOS
            .iter()
            .map(|s| s.name)
            .filter(|n| {
                !PORTED.iter().any(|p| p.name == *n) && !SKIPPED.iter().any(|(k, _)| k == n)
            })
            .collect();
        assert!(
            unlisted.is_empty(),
            "classify these for the linux-csi lane (parity.rs PORTED or SKIPPED): {unlisted:?}"
        );
        for (name, _) in SKIPPED {
            assert!(
                SCENARIOS.iter().any(|s| s.name == *name),
                "{name} is not in harness list"
            );
            assert!(
                !PORTED.iter().any(|p| p.name == *name),
                "{name} is both ported and skipped"
            );
        }
        let catalog: Vec<&str> = SCENARIOS
            .iter()
            .map(|s| s.name)
            .filter(|n| SKIPPED.iter().any(|(k, _)| k == n))
            .collect();
        let listed: Vec<&str> = SKIPPED.iter().map(|(k, _)| *k).collect();
        assert_eq!(listed, catalog, "SKIPPED: once each, in catalog order");
        assert_eq!(skipped().len() + PORTED.len(), SCENARIOS.len());
    }

    /// The reasons: a lever-less scenario never names [`LOCAL_CLIENT`]
    /// (the wildcard must not excuse it), every other one does, as a
    /// whole word; none reads as a missing-tool skip.
    #[test]
    fn skip_reasons_name_local_client_exactly_when_a_lever_is_missing() {
        let word = format!("capability {LOCAL_CLIENT}:");
        for (name, skip) in SKIPPED {
            let why = skip.reason();
            match skip {
                Skip::Local(levers) | Skip::Covered(levers, _) => {
                    assert!(!levers.is_empty(), "{name}: no lever named");
                    assert!(why.contains(&word), "{name}: {why}");
                }
                Skip::Unported(_) => {
                    assert!(!why.contains(LOCAL_CLIENT), "{name}: {why}");
                    assert!(why.contains("(follow-up F6)"), "{name}: {why}");
                }
            }
            assert!(!why.ends_with(" not installed"), "{why}");
        }
    }

    /// `tests/platform-parity.toml` names exactly the covered and the
    /// unported skips, and its only wildcard for this lane is
    /// [`LOCAL_CLIENT`]'s.
    #[test]
    fn the_parity_file_names_the_covered_and_unported_scenarios() {
        let file = include_str!("../../../../tests/platform-parity.toml");
        let mut named: Vec<&str> = Vec::new();
        let mut caps: Vec<&str> = Vec::new();
        let mut scenario = "";
        for line in file.lines() {
            if let Some(v) = line.strip_prefix("scenario = ") {
                scenario = v.trim_matches('"');
            } else if line.starts_with("lanes = ") && line.contains(&format!("\"{LANE}\"")) {
                if scenario != "*" {
                    named.push(scenario);
                }
            } else if let Some(v) = line.strip_prefix("cap = ") {
                caps.push(v.trim_matches('"'));
            }
        }
        let mut want: Vec<&str> = SKIPPED
            .iter()
            .filter(|(_, skip)| !matches!(skip, Skip::Local(_)))
            .map(|(name, _)| *name)
            .collect();
        named.sort_unstable();
        want.sort_unstable();
        assert_eq!(named, want);
        assert_eq!(caps, [LOCAL_CLIENT]);
    }

    #[test]
    fn covering_scenarios_exist() {
        for (name, skip) in SKIPPED {
            for k8s in skip.covered_by() {
                assert!(
                    super::super::K8S_SCENARIOS.iter().any(|k| k.name == k8s),
                    "{name}: {k8s} is not a k8s-scenario"
                );
            }
        }
        let flock = SKIPPED
            .iter()
            .find(|(n, _)| *n == "flock-cross-node")
            .unwrap();
        assert_eq!(flock.1.covered_by(), ["csi-cross-pod-locks"]);
    }
}
