//! Fault-injection scenarios. Each runs a fresh environment: floci S3
//! behind toxiproxy, one or more constellation clients on the host,
//! seeded workloads verified against the model oracle.

use crate::caps::Cap;
use crate::client::Client;
use crate::model::Model;
use crate::reqlog::CountingProxy;
use crate::s3env::{S3Env, BUCKET};
use crate::suites;
use crate::workload::Workload;
use anyhow::{bail, Context, Result};
use constellation_types::Code;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// The K5 lane's busy-writer loss: an `O_APPEND` writer whose file's
/// attributes change under it.
mod appendsize;
/// A P2P cluster's nodes `kill -9`ed and restarted together.
mod cluster_restart;
/// Plan 31 §6.12: subtree confinement through the kernel.
mod confinement;
mod coop_churn;
mod credrot;
/// The K5a fix round: a delegate that is the root's backup loses the
/// root, or is handed off.
mod delegroot;
mod ec2;
/// Plan 39: `fsync` under S3 outages (hard, soft, interrupted) and
/// `fsyncdir`.
mod fsync;
/// EC2 campaign 4 B-1/B-2: a git repository committed to by two nodes
/// taking turns under `flock`.
mod gitflock;
/// Plan 31 C4b: FUSE session handover (`daemon --upgrade`).
mod handover;
/// EC2 campaign 6 B-1: FUSE reverse invalidations under sustained
/// directory mutation from every node, with the holder `kill -9`ed.
mod inval_storm;
/// Plan 31 C8: the engine's host lifecycle (suspend, resume, metered).
mod lifecycle;
/// Plan 30 §M10: flexible-quorum continuation epochs.
mod m10;
/// Plan 30 §M11: delegated sub-sequencers, one log.
mod m11;
/// Plan 30 §M14: cross-node `flock`/`fcntl` (`--locks cluster`).
mod m14;
/// Plan 30 §M4's scenarios and the chaos runs' whole-cluster checks.
mod m4;
/// Plan 30 §M5 phase 2: the stale-base rule on the wire.
mod m5;
mod m6;
/// Plan 30 §M7: log streams and cross-node visibility.
mod m7;
/// Plan 30 §M8: `--cto strict`, read delegations and recalls.
mod m8;
/// Plan 30 §M9: backup peers, seal-based failover, `ack=s3`.
mod m9;
/// The OVH real-S3 run's findings: the create race, a non-owner's
/// per-op S3 round trip.
mod ovh;
/// Plan 38 §6/Z3b: FUSE passthrough on a real kernel.
mod passthrough;
/// Campaign 6 B-1: a lease holder's `kill -9` and its rejoin, and a
/// `daemon.lock` still held by a daemon the kernel has killed.
mod rejoin;
mod slowseal;
/// Plan 32 §11: the reclaim estimate against what GC deletes.
mod snapacct;
/// Plan 32 Step 8: `budget=` with the schedule leader off the lease holder.
mod snapbudget;
/// Fix snap-drain-busy: snapshots of a busy holder under S3 latency.
mod snapbusy;
/// Plan 32 M3 + M4: automatic snapshot creation and expiry (`snapsched*`).
mod snapsched;
/// Plan 32 §11: an active policy's cost to the sequencer's writes.
mod snapwrite;
/// stress-ng's filesystem stressors, all at once, with verification.
mod stressfs;
/// Plan 38 §6/§3(e): the transport a mount negotiates, and whether a
/// session on it can be handed over.
pub mod transport;
mod watermark;
/// The small-file write path: S3 round trips per close, `back` for
/// non-owners.
mod writepath;
/// Plan 38 §3(d)/Z4b: zero-copy reads on a 7.3 kernel.
mod zerocopy;

pub struct Scenario {
    pub name: &'static str,
    pub desc: &'static str,
    /// Host binaries the scenario needs; missing ones cause a loud skip.
    pub requires: &'static [&'static str],
    /// Frontend capabilities the scenario needs (derived from the
    /// frontend's `FrontendCaps`, `crate::caps`); `harness run` skips it,
    /// naming the capability, on a `--frontend` that lacks one.
    pub caps: &'static [crate::caps::Cap],
    pub run: fn(seed: u64) -> Result<()>,
}

pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "baseline",
        desc: "workload with a healthy network; model verified per block",
        requires: &[],
        caps: &[],
        run: baseline,
    },
    Scenario {
        name: "latency",
        desc: "workload under 150ms +/-50ms S3 latency",
        requires: &[],
        caps: &[],
        run: latency,
    },
    Scenario {
        name: "slow-network",
        desc: "workload under 256 KB/s S3 bandwidth + sliced packets",
        requires: &[],
        caps: &[],
        run: slow_network,
    },
    Scenario {
        name: "s3-outage",
        desc: "cut S3 mid-workload: cached reads keep working, writes recover after heal",
        requires: &[],
        caps: &[],
        run: s3_outage,
    },
    Scenario {
        name: "s3-flap",
        desc: "S3 connection cut/heal every block; workload must stay correct",
        requires: &[],
        caps: &[],
        run: s3_flap,
    },
    Scenario {
        name: "kill9-remount",
        desc: "SIGKILL the daemon between blocks; remount must recover all committed state",
        requires: &[],
        caps: &[],
        run: kill9_remount,
    },
    Scenario {
        name: "cold-cache",
        desc: "wipe the chunk cache between blocks; reads must re-fetch from S3",
        requires: &[],
        caps: &[],
        run: cold_cache,
    },
    Scenario {
        name: "two-clients-disjoint",
        desc: "two clients, one bucket: disjoint namespaces must not corrupt each other (phase-1 scope)",
        requires: &[],
        caps: &[],
        run: two_clients_disjoint,
    },
    Scenario {
        name: "two-clients-shared",
        desc: "two clients, ONE filesystem: writes on each propagate to the other (close-to-open)",
        requires: &[],
        caps: &[],
        run: two_clients_shared,
    },
    Scenario {
        name: "atime-eventual",
        desc: "read-time atime (plan 20): a read on one node eventually bumps atime on the other; under an S3 cut reads keep succeeding and atime is simply lost",
        requires: &[],
        caps: &[],
        run: atime_eventual,
    },
    Scenario {
        name: "quota-enforcement",
        desc: "live quota set blocks growth with ENOSPC; clearing resumes; replicates to a second node",
        requires: &[],
        caps: &[],
        run: quota_enforcement,
    },
    Scenario {
        name: "prune",
        desc: "retention prune (plan 22): an armed age policy removes stale files on one node and both replicas converge; dry-run deletes nothing",
        requires: &[],
        caps: &[],
        run: prune_retention,
    },
    Scenario {
        name: "git-workflow",
        desc: "stage/publish/edit ping-pong between two nodes of one filesystem",
        requires: &[],
        caps: &[],
        run: git_workflow,
    },
    Scenario {
        name: "lease-handover",
        desc: "S3-only pair: each node's sustained block is answered while the other holds, then it takes the lease (plan 30 M13 hybrid) and both converge",
        requires: &[],
        caps: &[],
        run: lease_handover,
    },
    Scenario {
        name: "lease-fencing",
        desc: "freeze the lease holder (SIGSTOP), let B take over after expiry, then resume A: A must detect deposition, never ship under its old epoch, and replay its stranded write through B exactly once",
        requires: &[],
        caps: &[],
        run: lease_fencing,
    },
    Scenario {
        name: "continuation-epoch",
        desc: "cut S3 with all writers on P2P: both keep writing, then flush and converge after heal",
        requires: &[],
        caps: &[],
        run: continuation_epoch,
    },
    Scenario {
        name: "epoch-member-lost",
        desc: "pause one continuation-epoch member: survivors immediately freeze writes with EROFS",
        requires: &[],
        caps: &[],
        run: epoch_member_lost,
    },
    Scenario {
        name: "deposed-reintegration",
        desc: "plan 30 M3b: a deposed holder recovers automatically; its non-overlapping stranded edits replay cleanly and only the true edit-vs-edit overlap materializes a conflict copy (Layer A: no backup, A alone cut from S3)",
        requires: &[],
        caps: &[],
        run: deposed_reintegration,
    },
    Scenario {
        name: "backup-takeover-holds-missing-chunks",
        desc: "plan 30 M9 x M4: a write-back holder cut from S3 dies after acknowledging; its backup adopts the manifests and holds them (never ships a missing chunk; a fresh node sees the files empty); when the holder returns and uploads, the held records ship and everyone reads its bytes",
        requires: &[],
        caps: &[],
        run: backup_takeover_holds_missing_chunks,
    },
    Scenario {
        name: "backup-takeover-drops-held-chunks",
        desc: "plan 30 M9 x M4: as backup-takeover-holds-missing-chunks, but the old holder never returns: `repair drop-held` on the successor turns the held manifest into a conflict copy with the lost chunk as a hole",
        requires: &[],
        caps: &[],
        run: backup_takeover_drops_held_chunks,
    },
    Scenario {
        name: "epoch-peer-reaching-s3-declines",
        desc: "plan 30 M10: only the holder loses S3; its peer reaches S3 (and says so), so the holder proposes no continuation epoch, and when it then dies the peer (its backup) seals and writes within seconds instead of staying frozen (EROFS)",
        requires: &[],
        caps: &[],
        run: epoch_peer_reaching_s3_declines,
    },
    Scenario {
        name: "deposed-reintegration-backup",
        desc: "plan 30 M9's side of deposed-reintegration: with the default LAN backup, a holder whose ship is held has its edits acknowledged by the backup, which seals and takes over inside the TTL with them — nothing strands, no conflict copy, every adopted chunk is in S3",
        requires: &[],
        caps: &[],
        run: deposed_reintegration_backup,
    },
    Scenario {
        name: "node-leave",
        desc: "three writers; C leaves; A+B can open a continuation epoch under S3 cut (unmount alone cannot)",
        requires: &[],
        caps: &[],
        run: node_leave,
    },
    Scenario {
        name: "p2p-invalidation",
        desc: "gossip push makes a write visible on the peer far faster than the S3 poll bound; P2P=off restores the old bound",
        requires: &[],
        caps: &[],
        run: p2p_invalidation,
    },
    Scenario {
        name: "p2p-handover",
        desc: "a blocked writer takes the lease from an ACTIVE holder in ~1 RTT instead of waiting out the idle window",
        requires: &[],
        caps: &[],
        run: p2p_handover,
    },
    Scenario {
        name: "forwarded-mutations",
        desc: "non-holder writes forward to the sticky lease holder; lease epoch does not thrash",
        requires: &[],
        caps: &[],
        run: forwarded_mutations,
    },
    Scenario {
        name: "scratch-publish",
        desc: "scratch dir contents are local; publish rename makes the file cluster-visible",
        requires: &[],
        caps: &[],
        run: scratch_publish,
    },
    Scenario {
        name: "p2p-partition-tolerance",
        desc: "with P2P disabled on one node, shared-filesystem correctness still holds on the S3 slow path",
        requires: &[],
        caps: &[],
        run: p2p_partition_tolerance,
    },
    Scenario {
        name: "p2p-same-identity-restart",
        desc: "SIGKILL the lease holder of three and remount it with the same state and node key: the others see it connected again and forward to it over P2P within seconds, not after the dead connection's QUIC idle timeout",
        requires: &[],
        caps: &[],
        run: p2p_same_identity_restart,
    },
    Scenario {
        name: "p2p-cluster-restart",
        desc: "fix p2p-restart-auth: kill -9 all four nodes at once (then one, then two), restart them in a seeded order while a docker bridge comes and goes (iroh rebinds its sockets): every node that does not hold the lease forwards a write to the root over P2P within 10 s of the last remount",
        requires: &[],
        caps: &[],
        run: cluster_restart::p2p_cluster_restart,
    },
    Scenario {
        name: "coop-cache-hit",
        desc: "cold reader fetches most chunks from a warm peer while S3 is delayed 200ms",
        requires: &[],
        caps: &[],
        run: coop_cache_hit,
    },
    Scenario {
        name: "s3-retry",
        desc: "cold read survives a first-attempt S3 cut via S3-leg-only retries",
        requires: &[],
        caps: &[],
        run: s3_retry,
    },
    Scenario {
        name: "coop-fallback",
        desc: "paused warm peer: reader still completes from S3 with hash-verified content",
        requires: &[],
        caps: &[],
        run: coop_fallback,
    },
    Scenario {
        name: "web-fleet",
        desc: "one writer, two cold readers: aggregate S3 GETs stay near the unique-chunk count",
        requires: &[],
        caps: &[],
        run: web_fleet,
    },
    Scenario {
        name: "coop-exact-churn",
        desc: "3 small-cache nodes add and evict chunks while reading each other: zero false-positive peer fetches, exact mirrors",
        requires: &[],
        caps: &[],
        run: coop_churn::coop_exact_churn,
    },
    Scenario {
        name: "coop-digest-compare",
        desc: "same churn in bloom and exact digest mode: prints digest bytes/s, false positives, CPU per round",
        requires: &[],
        caps: &[],
        run: coop_churn::coop_digest_compare,
    },
    Scenario {
        name: "web-ui-smoke",
        desc: "localhost HTTP control adapter, embedded UI, and Prometheus metrics",
        requires: &[],
        caps: &[],
        run: web_ui_smoke,
    },
    Scenario {
        name: "gc-lifecycle",
        desc: "reference GC removes dead chunks while preserving snapshot and live roots",
        requires: &[],
        caps: &[],
        run: gc_lifecycle,
    },
    Scenario {
        name: "csi-credential-revocation",
        desc: "plan 37 K6a: a serve --await-unlock engine on a signature-checking versitygw rotated from account A to B with A then revoked, under a writer: zero errors; rotations to the revoked or a wrong pair refused, the pair in use kept; a fresh engine reads every file back",
        requires: &[],
        caps: &[],
        run: credrot::csi_credential_revocation,
    },
    Scenario {
        name: "snapacct",
        desc: "snapshot accounting: --verify clean on two nodes; a dry-run reclaim set equals what GC then deletes",
        requires: &[],
        caps: &[],
        run: snapacct::snapacct,
    },
    Scenario {
        name: "gc-dedup-race",
        desc: "a writer reuses condemned content during the TTL wait without creating a dangle",
        requires: &[],
        caps: &[],
        run: gc_dedup_race,
    },
    Scenario {
        name: "gc-open-orphan-hold",
        desc: "DESIGN §3: a file unlinked on A while open on B is claimed by B's holds/<node>.json; GC keeps its chunk until B closes it, then reclaims it",
        requires: &[],
        caps: &[Cap::KeepOpenUnlinked],
        run: gc_open_orphan_hold,
    },
    Scenario {
        name: "log-retention-gap-follower",
        desc: "DESIGN §14: a running follower frozen while GC pruned the log past its position rebuilds from the head commit on resume and converges; it then takes the lease and appends only above the head",
        requires: &[],
        caps: &[],
        run: log_retention_gap_follower,
    },
    Scenario {
        name: "log-retention-gap-taker",
        desc: "DESIGN §14: a node stopped while GC pruned the log past its position rebuilds at mount, converges, and its lease takeover never creates a segment in a pruned slot",
        requires: &[],
        caps: &[],
        run: log_retention_gap_taker,
    },
    Scenario {
        name: "log-retention-gap-open-orphan",
        desc: "DESIGN §3+§14: a frozen follower with an unlinked file open goes through a retention-gap rebuild; its handle keeps reading the right bytes until close, its hold keeps GC off the chunk, and the close reclaims it",
        requires: &[],
        caps: &[Cap::KeepOpenUnlinked],
        run: log_retention_gap_open_orphan,
    },
    Scenario {
        name: "fsck-repair",
        desc: "fsck detects and repairs a missing chunk, orphan, and torn segment",
        requires: &[],
        caps: &[],
        run: fsck_repair,
    },
    Scenario {
        name: "fsck-while-mounted",
        desc: "fsck routes through the running daemon's control socket instead of racing the fjall lock",
        requires: &[],
        caps: &[],
        run: fsck_while_mounted,
    },
    Scenario {
        name: "snapshot-lifecycle",
        desc: "snapshot remains frozen behind hidden .constellation view, then becomes stale; \
               a second node snapshots, holds and deletes while the first writes, and the \
               write lease never moves; then plan 32 Step 5's CLI from that node: the ls table, \
               ranges across a chain, globs, dry run, prompt, per-item hold refusals",
        requires: &[],
        caps: &[],
        run: snapshot_lifecycle,
    },
    Scenario {
        name: "snapshot-busy-latency",
        desc: "fix snap-drain-busy: +25 ms per S3 request each way and a writer rewriting a \
               file on the lease holder every 5 ms; snapshots on the holder and forwarded \
               from a second node, and a clone on the holder, each finish within a few round \
               trips of an idle snapshot, hold every write acknowledged before them and none \
               after, and the lease never moves",
        requires: &[],
        caps: &[],
        run: snapbusy::snapshot_busy_latency,
    },
    Scenario {
        name: "clone-workflow",
        desc: "eager metadata clone diverges without changing its source snapshot",
        requires: &[],
        caps: &[],
        run: clone_workflow,
    },
    Scenario {
        name: "snapshot-mount",
        desc: "snapshot subtree mounts read-only, ephemeral rw clone is removed, listing survives rename/replace",
        requires: &[],
        caps: &[],
        run: snapshot_mount,
    },
    Scenario {
        name: "snapshot-churn",
        desc: "concurrent multi-round snapshot and clone churn with a SQLite oracle and replay",
        requires: &[],
        caps: &[],
        run: crate::snapchurn::run,
    },
    Scenario {
        name: "snapsched",
        desc: "plan 32 M3+M4: `10s:1m 1m:4m; last=2` on /proj, two nodes, a writer, 120 s grace: \
               ~6 min of one auto-<UTC> snapshot per bucket with expiry, a manual and two held \
               (plain, csi:) snapshots; kill -9 the scheduler leader mid-run (no bucket twice, \
               no gap over TTL + tick + margin); skip-empty; then the survivors equal \
               retention::evaluate over the audit journal's creations on both mounts, every \
               deletion was expired by evaluate at its tick, none before the grace window closed",
        requires: &[],
        caps: &[],
        run: snapsched::snapsched,
    },
    Scenario {
        name: "snapsched-s3-outage",
        desc: "plan 32 M3+M4: a 10s policy, expiring, through a 90 s S3 cut: create_failed rises, \
               nothing appears and nothing expires during the cut, exactly one catch-up after \
               the heal (no backfill), expiry resumes, survivors equal retention::evaluate",
        requires: &[],
        caps: &[],
        run: snapsched::snapsched_s3_outage,
    },
    Scenario {
        name: "snapsched-grace",
        desc: "plan 32 M4: ~10 snapshots under 10s:5m, shortened to 10s:1m by setxattr: nothing \
               is deleted until the (30 s) grace window state.json records closes, then exactly \
               what retention::evaluate says under the new policy",
        requires: &[],
        caps: &[],
        run: snapsched::snapsched_grace,
    },
    Scenario {
        name: "snapsched-budget",
        desc: "plan 32 Step 8: `budget=3M` on /proj, three nodes, the schedule leader not the \
               root lease holder; after the grace window one leader run deletes exactly the \
               shortest prefix of `budget_order` that the leader's index says meets the budget \
               (nothing `last`, held, manual or in grace), `budget_used_bytes` from the leader; \
               the leader killed at its run: the new leader deletes nothing twice",
        requires: &[],
        caps: &[],
        run: snapbudget::snapsched_budget,
    },
    Scenario {
        name: "snapsched-write-overhead",
        desc: "plan 32 §11: a 100k-file tree under /proj on two nodes; fio sequential writes on \
               the root lease holder, alternating no policy and `10s:1h` on /proj (the \
               scheduler on the other node), 3 runs each: the root lease (holder, epoch) never \
               moves; MB/s per run, the medians and the regression printed (the <= 3% bound is \
               judged on them, not asserted)",
        requires: &["fio"],
        caps: &[],
        run: snapwrite::snapsched_write_overhead,
    },
    Scenario {
        name: "e2e-basic",
        desc: "passphrase mount encrypts chunks and logs, cold-remounts, and rejects a wrong passphrase",
        requires: &[],
        caps: &[],
        run: e2e_basic,
    },
    Scenario {
        name: "e2e-two-nodes",
        desc: "two passphrase nodes converge and use cooperative cache without exposing plaintext",
        requires: &[],
        caps: &[],
        run: e2e_two_nodes,
    },
    Scenario {
        name: "passwd-live-cluster",
        desc: "fs passwd on a live E2E cluster: both nodes keep converging (incl. across a split) with no remount",
        requires: &[],
        caps: &[],
        run: passwd_live_cluster,
    },
    Scenario {
        name: "fresh-node-bootstrap",
        desc: "node B rebuilds the namespace purely from S3 (plan 28 commit + log replay) and must match the model",
        requires: &[],
        caps: &[],
        run: fresh_node_bootstrap,
    },
    Scenario {
        name: "commit-strips-pending-upload",
        desc: "a mid-write-back metadata commit must not give a fresh joiner the writer's pending_upload backlog; the joiner's view is a log position (every file its content or empty, never wrong bytes) and exact once it tails the writer's final ship",
        requires: &[],
        caps: &[],
        run: commit_strips_pending_upload,
    },
    Scenario {
        name: "readahead",
        desc: "cold sequential read of a multi-chunk file under S3 latency must pipeline (prefetcher)",
        requires: &[],
        caps: &[],
        run: readahead,
    },
    Scenario {
        name: "readahead-adaptive",
        desc: "128-chunk cold sequential read under 200ms S3 latency expands the adaptive window",
        requires: &[],
        caps: &[],
        run: readahead_adaptive,
    },
    Scenario {
        name: "e2e-spilled-manifest",
        desc: "cold read of an E2E file whose manifest spilled its chunk list",
        requires: &[],
        caps: &[],
        run: e2e_spilled_manifest_cold_read,
    },
    Scenario {
        name: "e2e-decode-priority",
        desc: "a demand read stays fast while a concurrent bulk E2E download saturates the decode gate",
        requires: &[],
        caps: &[],
        run: e2e_decode_priority,
    },
    Scenario {
        name: "scan-ahead",
        desc: "ordered cold reads of many small files pipeline across file boundaries",
        requires: &[],
        caps: &[],
        run: scan_ahead,
    },
    Scenario {
        name: "distant-bigfile-stable",
        desc: "sequential cold download of a big file over a 200ms 'distant S3' path stays fast and doesn't swing wildly",
        requires: &[],
        caps: &[],
        run: distant_bigfile_stable,
    },
    Scenario {
        name: "distant-bigfile-stable-e2e",
        desc: "distant-bigfile-stable, but through an E2E-encrypted filesystem",
        requires: &[],
        caps: &[],
        run: distant_bigfile_stable_e2e,
    },
    Scenario {
        name: "prefetch-abandon",
        desc: "a reader that stops mid-file must not keep pulling the rest over the network into cache",
        requires: &[],
        caps: &[],
        run: prefetch_abandon,
    },
    Scenario {
        name: "prefetch-abandon-e2e",
        desc: "prefetch-abandon, but through an E2E-encrypted filesystem",
        requires: &[],
        caps: &[],
        run: prefetch_abandon_e2e,
    },
    Scenario {
        name: "prefetch-fairness",
        desc: "a big-file prefetch saturating a capped link must not stall concurrent small-file reads",
        requires: &[],
        caps: &[],
        run: prefetch_fairness,
    },
    Scenario {
        name: "fio-latency",
        desc: "fio randwrite + crc32c verify under 80ms S3 latency",
        requires: &["fio"],
        caps: &[],
        run: fio_latency,
    },
    Scenario {
        name: "fio-blips",
        desc: "fio verify while S3 blips on/off: retries must absorb transient cuts",
        requires: &["fio"],
        caps: &[],
        run: fio_blips,
    },
    Scenario {
        name: "fsync-hard-outage",
        desc: "plan 39: S3 cut for 20 s under an fsync of unuploaded data; the fsync waits (no EIO, no early return), returns 0 after the heal, node.status.fsync shows the wait, and a fresh node reads the bytes back",
        requires: &[],
        caps: &[],
        run: fsync::fsync_hard_outage,
    },
    Scenario {
        name: "fsync-soft-timeout",
        desc: "plan 39: --fsync-timeout 2s under an S3 cut answers EIO after ~2 s with the data still pending; after the heal it uploads by itself (a fresh node reads it) and the descriptor's next fsync returns 0",
        requires: &[],
        caps: &[],
        run: fsync::fsync_soft_timeout,
    },
    Scenario {
        name: "fsync-interrupt",
        desc: "plan 39: during an S3 cut, a handled SIGINT leaves a process blocked in fsync waiting (no EINTR); SIGKILL ends the wait and the process is reaped within seconds; its data still uploads after the heal",
        requires: &["python3"],
        caps: &[],
        run: fsync::fsync_interrupt,
    },
    Scenario {
        name: "fsyncdir-barrier",
        desc: "plan 39: under --fsync-mode s3, fsync of a directory after a rename into it returns with the journal shipped; after kill -9 a fresh node sees every rename",
        requires: &[],
        caps: &[],
        run: fsync::fsyncdir_barrier,
    },
    Scenario {
        name: "stress-ng-fs",
        desc: "every applicable stress-ng filesystem stressor at once with --verify on one mount (120 s, snapshots mid-run); per-stressor verdicts against tests/stress-ng-baseline.txt, no panic/ERROR, mount healthy, spool drains, snapshot space --verify clean",
        requires: &["stress-ng"],
        caps: &[],
        run: stressfs::stress_ng_fs,
    },
    Scenario {
        name: "stress-ng-flap",
        desc: "stress-ng metadata churn while S3 flaps; mount healthy + spool drains",
        requires: &["stress-ng"],
        caps: &[],
        run: stress_ng_flap,
    },
    Scenario {
        name: "big-file-write",
        desc: "write a file several times --cache-size and sample RSS: must stay flat, not track bytes written (plan 07)",
        requires: &[],
        caps: &[],
        run: big_file_write,
    },
    Scenario {
        name: "staging-crash",
        desc: "kill -9 mid-write, remount: staging/ is empty after GC and the file is at its last closed size",
        requires: &[],
        caps: &[],
        run: staging_crash,
    },
    Scenario {
        name: "unmount-drain",
        desc: "fail the eager upload, then unmount cleanly: a second node must read the file with no missing chunk",
        requires: &[],
        caps: &[],
        run: unmount_drain,
    },
    Scenario {
        name: "writeback-latency",
        desc: "150ms S3 latency: write-back small-file closes beat write-through by at least 3x",
        requires: &[],
        caps: &[],
        run: writeback_latency,
    },
    Scenario {
        name: "writeback-bigfile",
        desc: "write-back streams a file ten times cache budget within RSS and cache ceilings",
        requires: &[],
        caps: &[],
        run: writeback_bigfile,
    },
    Scenario {
        name: "writeback-drain",
        desc: "back-to-through switch drains pending uploads before returning",
        requires: &[],
        caps: &[],
        run: writeback_drain,
    },
    Scenario {
        name: "writeback-fsync",
        desc: "fsync under write-back reaches S3 before kill -9; an fsync from a new descriptor after a back close makes the file readable on the sequencer with the writer dead",
        requires: &[],
        caps: &[],
        run: writeback_fsync,
    },
    Scenario {
        name: "writeback-backpressure",
        desc: "S3 cut throttles then returns ENOSPC at the dirty hard limit and recovers",
        requires: &[],
        caps: &[],
        run: writeback_backpressure,
    },
    Scenario {
        name: "existence-bloom-dedup",
        desc: "fresh node hints from its replica's chunk_ref and deduplicates without a bucket LIST",
        requires: &[],
        caps: &[],
        run: existence_bloom_dedup,
    },
    Scenario {
        name: "existence-peer-hint",
        desc: "uploader uses live peer cache digests as confirming probe hints ahead of the replica",
        requires: &[],
        caps: &[],
        run: existence_peer_hint,
    },
    Scenario {
        name: "xattr-roundtrip",
        desc: "file and directory xattrs replicate; removal and logical recursive size are correct",
        requires: &[],
        caps: &[Cap::Xattrs],
        run: xattr_roundtrip,
    },
    Scenario {
        name: "truncate-never-resurrects",
        desc: "a truncate never resurrects the bytes it cut: ftruncate / O_TRUNC / a cross-node truncate, then an extension past the cut (write past a gap, truncate-up, fallocate), on the holder and a forwarding node, checked live, after a cold-cache remount and on a fresh node",
        requires: &[],
        caps: &[],
        run: truncate_never_resurrects,
    },
    Scenario {
        name: "fallocate-sparse",
        desc: "large sparse extend, hole punch, SEEK_HOLE/DATA, rewrite, and fresh-node verification",
        requires: &[],
        caps: &[Cap::Fallocate, Cap::SeekHole],
        run: fallocate_sparse,
    },
    Scenario {
        name: "chaos-ci",
        desc: "same-path conflict races across 3 local mounts (constellation-chaos Ci profile)",
        requires: &[],
        caps: &[],
        run: chaos_ci,
    },
    Scenario {
        name: "git-under-flock",
        desc: "two nodes alternately git commit under an flock turn file; every node and a fresh one agree on every .git file, fsck is clean, no acknowledged commit is lost",
        requires: &["git"],
        caps: &[],
        run: gitflock::git_under_flock,
    },
    Scenario {
        name: "git-under-flock-gc",
        desc: "git-under-flock with git gc (pack + prune loose objects) under the lock every 8 commits",
        requires: &["git"],
        caps: &[],
        run: gitflock::git_under_flock_gc,
    },
    Scenario {
        name: "git-under-flock-b2b",
        desc: "campaign 5's back-to-back git-under-flock: 5-20 files per commit, marker-file stale check, no overlapping turns, bounded turn duration",
        requires: &["git"],
        caps: &[],
        run: gitflock::git_under_flock_b2b,
    },
    Scenario {
        name: "git-under-flock-rounds",
        desc: "git-under-flock-b2b several times in a row, each in a new repository, against the same daemons",
        requires: &["git"],
        caps: &[],
        run: gitflock::git_under_flock_rounds,
    },
    Scenario {
        name: "git-under-flock-causal",
        desc: "git-under-flock-b2b with reading nodes: a reflog line or ref naming a commit means every object it needs is visible (causal order), the ref never regresses, fsck under the turn lock is clean, a reader survives kill -9",
        requires: &["git"],
        caps: &[],
        run: gitflock::git_under_flock_causal,
    },
    Scenario {
        name: "git-under-flock-faults",
        desc: "git-under-flock under kill -9 (holder, committers, whole cluster), SIGSTOP, P2P isolation and S3 cuts",
        requires: &["git"],
        caps: &[Cap::FuseAbort],
        run: gitflock::git_under_flock_faults,
    },
    Scenario {
        name: "lock-grant-dead-generation",
        desc: "EC2 campaign 7 B-2: a lock grant whose floor names a delegation generation that ended before the grantee's incarnation must not leave every read on that node waiting the whole session budget (the 'hung' git add / cat)",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: watermark::lock_grant_dead_generation,
    },
    Scenario {
        name: "chaos-soak-4",
        desc: "4 local mounts, soak profile (repro fleet write_disjoint; write-back + fsync s3)",
        requires: &[],
        caps: &[],
        run: chaos_soak_4,
    },
    Scenario {
        name: "disjoint-write-4",
        desc: "4 local mounts, data-only soak schedule (hits write_disjoint hard)",
        requires: &[],
        caps: &[],
        run: disjoint_write_4,
    },
    Scenario {
        name: "mkdir-p-race",
        desc: "4 nodes race `mkdir -p` of the same fresh tree: a refusal the holder based on state this node lacks must not surface as ENOENT",
        requires: &[],
        caps: &[],
        run: mkdir_p_race,
    },
    Scenario {
        name: "create-storm-s3-only",
        desc: "3-way create/write/unlink storm in one shared dir, P2P off: a healthy but contended holder must never starve a waiter into EIO",
        requires: &[],
        caps: &[],
        run: create_storm_s3_only,
    },
    Scenario {
        name: "mtree-gc-plateau",
        desc: "plan 28 S7b: rewrite rounds with a metadata commit and a GC round each; the metadata pack footprint must plateau, and a fresh node still bootstraps",
        requires: &[],
        caps: &[],
        run: mtree_gc_plateau,
    },
    Scenario {
        name: "idle-cluster-is-quiet",
        desc: "plan 26: 3 idle nodes issue no LIST of log/ and stay within the lease/heartbeat/probe budget",
        requires: &[],
        caps: &[],
        run: idle_cluster_is_quiet,
    },
    Scenario {
        name: "wan-writer-ships-put-only",
        desc: "plan 26: over a 200ms path the holder never lists its own stream, and a P2P-less follower converges within the idle ceiling",
        requires: &[],
        caps: &[],
        run: wan_writer_ships_put_only,
    },
    Scenario {
        name: "sticky-lease-handoff-over-s3",
        desc: "plan 26: with P2P off, a blocked writer registers wanted_by and takes the lease cooperatively instead of EIO",
        requires: &[],
        caps: &[],
        run: sticky_lease_handoff_over_s3,
    },
    Scenario {
        name: "named-shared-daemon",
        desc: "plan 21: mount NAME then NAME:/sub from a second CLI call share one daemon/node_id; umount tears down views one at a time, then the process",
        requires: &[],
        caps: &[],
        run: named_shared_daemon,
    },
    Scenario {
        name: "forward-timeout-reexec",
        desc: "plan 30 M2 (fixes bug A): a forwarded mutation's reply races the requester's forward timeout; exactly-once forwarding resolves it via dedup or the completed table instead of re-executing",
        requires: &[],
        caps: &[],
        run: forward_timeout_reexec,
    },
    Scenario {
        name: "holder-ships-under-forward-load",
        desc: "plan 30 M2b: a sustained forwarded-create burst from 2 non-holders must not starve the holder's own ship rounds; journal backlog stays bounded and followers converge within 2s of the burst ending",
        requires: &[],
        caps: &[],
        run: holder_ships_under_forward_load,
    },
    Scenario {
        name: "holder-crash-phantom-shadow",
        desc: "plan 30 M3a (fixes bug B): a holder stranded (S3 cut, then killed) after acking a forwarded create; a third node takes over, the requester's shadow is rolled back and replayed by rid, and b, c and a fresh d agree the create exists",
        requires: &[],
        caps: &[],
        run: holder_crash_phantom_shadow,
    },
    Scenario {
        name: "holder-crash-phantom-new-holder",
        desc: "plan 30 M3a (fixes bug B): the requester of a stranded forwarded create becomes the next holder; its takeover gate replays the create before serving, so another node's O_EXCL create of the name gets EEXIST and b, c and a fresh d agree",
        requires: &[],
        caps: &[],
        run: holder_crash_phantom_new_holder,
    },
    Scenario {
        name: "takeover-marker-strands-promptly",
        desc: "plan 30 M3b: a takeover whose own op is refused still strands a third node's shadow promptly (the new holder's epoch marker); the stranded create then replays exactly once",
        requires: &[],
        caps: &[],
        run: takeover_marker_strands_promptly,
    },
    Scenario {
        name: "holder-publishes-log-prefix",
        desc: "plan 30 M3b: a holder publishes while its journal is non-empty; every commit equals the log prefix, so after a mid-burst kill a fresh node matches the log-tailing follower exactly (CONSTELLATION_BACKUP_RTT_BUDGET_MS=0: today's ack=local, where the kill catches an unshipped tail)",
        requires: &[],
        caps: &[],
        run: holder_publishes_log_prefix,
    },
    Scenario {
        name: "holder-publishes-log-prefix-backup",
        desc: "plan 30 M9: the same burst with B as A's backup; A's acknowledged tail is re-shipped by B after it seals and takes over, so every acknowledged mkdir reaches the log and the fresh node still matches the follower exactly",
        requires: &[],
        caps: &[],
        run: holder_publishes_log_prefix_backup,
    },
    Scenario {
        name: "poison-record-isolation",
        desc: "plan 30 M4: a pending chunk lost from the holder's cache holds back only its manifest and what depends on it; everything else ships, status lists it, and `repair drop-held` drops it into a conflict copy and replays the dependents",
        requires: &[],
        caps: &[],
        run: m4::poison_record_isolation,
    },
    Scenario {
        name: "unmount-with-held-records",
        desc: "a final flush that cannot finish (records held behind a lost pending chunk, then also with S3 cut) still lets the daemon exit promptly and non-zero, saying why; the journal and held rows survive for the next mount, where drop-held still recovers and the unmount is clean",
        requires: &[],
        caps: &[],
        run: m4::unmount_with_held_records,
    },
    Scenario {
        name: "publish-only-holder",
        desc: "plan 30 M4: S3 requests per node by area on an idle and a busy 3-node cluster; only the lease holder PUTs commits or reads condemned lists",
        requires: &[],
        caps: &[],
        run: m4::publish_only_holder,
    },
    Scenario {
        name: "stale-base-rename-divergence",
        desc: "plan 30 M5: a holder's accepted reply names the unshipped base it was evaluated on; the requester waits for the log instead of installing a rename-over-existing-name as a shadow that the holder's unshipped unlink then removes, so A, B and C agree `f2` is the renamed `f1`",
        requires: &[],
        caps: &[],
        run: m5::stale_base_rename_divergence,
    },
    Scenario {
        name: "session-exists-observed",
        desc: "plan 30 M6: a refused create observes the holder's unshipped state; the requester's lookups of the refused name AND of another name the holder had wait for that position (not a per-name causal wait) and both find their files once the holder ships",
        requires: &[],
        caps: &[],
        run: m6::session_exists_observed,
    },
    Scenario {
        name: "session-forwarded-ryw",
        desc: "plan 30 M6 (decision 2): `touch a; ls; stat .; stat a; cat a` right after forwarded creates (the holder's shipping held), then with content and shipping running, never waits: installed shadows raise nothing; A holds throughout with B as its M9 backup; then the same on the holder, whose close waits for its manifest row to be durable so its reads never do",
        requires: &[],
        caps: &[],
        run: m6::session_forwarded_ryw,
    },
    Scenario {
        name: "takeover-resolves-awaiting-close",
        desc: "a forwarded close waits for the log (the holder's sync held), the holder stalls, the requester (its M9 backup) takes the lease over: the close returns at once and succeeds, answered from the completion the takeover brought (was: EIO at the 120 s deadline)",
        requires: &[],
        caps: &[],
        run: m6::takeover_resolves_awaiting_close,
    },
    Scenario {
        name: "session-stale-base-rename",
        desc: "plan 30 M6: the stale-base half (M5's stale-base-rename-divergence): a rename accepted on an unapplied base waits for the log; A, B and C agree",
        requires: &[],
        caps: &[],
        run: m6::session_stale_base_rename,
    },
    Scenario {
        name: "session-ryw-after-holder-kill",
        desc: "plan 30 M6: C's forwarded create is acked by A, A dies before shipping, B takes over; C stats its own file every 20 ms through the stranding and replay and never misses it",
        requires: &[],
        caps: &[],
        run: m6::session_ryw_after_holder_kill,
    },
    Scenario {
        name: "session-wait-degrades",
        desc: "plan 30 M6: a requester whose observed position cannot be reached (holder's shipping held) answers reads after the budget from its replica — no EIO, one warning, timeouts counted — and recovers once the holder ships",
        requires: &[],
        caps: &[],
        run: m6::session_wait_degrades,
    },
    Scenario {
        name: "session-idle-latency",
        desc: "plan 30 M6: after a 3-node write burst, a read-only phase never waits (status.session.waited flat); a single node never waits at all; prints the wait histograms",
        requires: &[],
        caps: &[],
        run: m6::session_idle_latency,
    },
    Scenario {
        name: "visibility-after-burst",
        desc: "plan 30 M7: 3 nodes; A writes a large write-back burst, then a paced series of fsync'd markers while B and C poll for each; cross-node visibility p99 < 2 s with log streams off and on, and with streams on the pollers issue ~0 S3 tail GETs during the markers",
        requires: &[],
        caps: &[],
        run: m7::visibility_after_burst,
    },
    Scenario {
        name: "chaos-ci-strict",
        desc: "plan 30 M8: chaos-ci with every mount --cto strict; the history records it and the close-to-open checker (a read issued after another worker's write completed must not show an older state) is enforced",
        requires: &[],
        caps: &[],
        run: chaos_ci_strict,
    },
    Scenario {
        name: "cto-strict",
        desc: "plan 30 M8: 3 nodes with --cto strict; a writer (forwarding, and the holder itself) closes, the harness at once opens on the reader: every read sees the close (overwrites and new names); prints ReadIndex/delegation counters and latencies",
        requires: &[],
        caps: &[],
        run: m8::cto_strict,
    },
    Scenario {
        name: "cto-bounded",
        desc: "plan 30 M8: the cto-strict loop under --cto bounded (the default), documenting the staleness strict removes: how many reads right after a close elsewhere miss it, and how long until they see it",
        requires: &[],
        caps: &[],
        run: m8::cto_bounded,
    },
    Scenario {
        name: "cto-delegation-recall",
        desc: "plan 30 M8: a reader's repeated opens cost one ReadIndex, then are local under a read delegation; a writer's close recalls it (acked) before returning and the reader sees the new content; writer latency with and without an outstanding delegation",
        requires: &[],
        caps: &[],
        run: m8::cto_delegation_recall,
    },
    Scenario {
        name: "cto-recall-unreachable",
        desc: "plan 30 M8: the delegate is frozen (SIGSTOP): a forwarded write's and the holder's own write's close each return only after the sequencer waited out TTL + drift margin; the thawed delegate reads the new content",
        requires: &[],
        caps: &[],
        run: m8::cto_recall_unreachable,
    },
    Scenario {
        name: "cto-latency",
        desc: "plan 30 M8: single-node strict vs bounded open latency (no ReadIndex on a single node), and a LAN non-sequencer's first strict open against its delegated opens",
        requires: &[],
        caps: &[],
        run: m8::cto_latency,
    },
    Scenario {
        name: "cto-second-node-joins",
        desc: "plan 30 M8: a lone strict node keeps a kernel cache (free on a single node); a second node joins and writes, and the first node's very next open sees it (the latch turns the cache off and drains it before acking the newcomer)",
        requires: &[],
        caps: &[],
        run: m8::cto_second_node_joins,
    },
    Scenario {
        name: "cto-strict-root",
        desc: "EC2 finding R2-4: two nodes mount a fresh filesystem at once (strict, bounded) and in order (strict); every node's root must be owned by the mounting user and writable",
        requires: &[],
        caps: &[],
        run: m8::cto_strict_root,
    },
    Scenario {
        name: "s3-cut-one-node",
        desc: "EC2 finding 1 / R2-1: a non-holder's S3 is black-holed (P2P intact, product S3 retry budget); create+write+close with and without fsync under --write-mode back and through completes during the cut and is visible elsewhere, and unrelated ls -la / stat on that node answer at once while a close is in flight",
        requires: &[],
        caps: &[],
        run: ec2::s3_cut_one_node,
    },
    Scenario {
        name: "s3-cut-create-holder-restart",
        desc: "EC2 campaign 8 A-1: a non-holder's S3 black-holed like the campaign's `--dport 443 ! -d <subnet> DROP` (P2P intact, product S3 retry budget); right after the holder is kill -9ed and restarted, then while the holder freezes past the forward retries, a create on the cut node completes within 10 s (the restarted holder re-adopts its lease for the forward; the cut node keeps forwarding instead of waiting on S3); with P2P cut too the create fails with EIO within the S3-less bound, not the 120 s in-doubt deadline",
        requires: &[],
        caps: &[],
        run: ec2::s3_cut_create_holder_restart,
    },
    Scenario {
        name: "p2p-partition-one-node",
        desc: "EC2 finding 2: one of four writing nodes loses P2P to the others (S3 everywhere); its sustained inbox demand must not move the lease off the majority, whose writes keep forwarding at LAN latency; the isolated node's writes complete through the S3 inbox; everything converges after the heal",
        requires: &[],
        caps: &[],
        run: ec2::p2p_partition_one_node,
    },
    Scenario {
        name: "idle-cost",
        desc: "EC2 finding R2-2: four converged nodes idle for two minutes with the product's default intervals; S3 requests per node per minute, by kind and key area, on each node's own relay and in status.s3",
        requires: &[],
        caps: &[],
        run: ec2::idle_cost,
    },
    Scenario {
        name: "inbox-withdraw-hole",
        desc: "withdraw hole: a requester's unread inbox batch is withdrawn when its P2P link to the holder comes back (a tombstone, not a DELETE); cut again, its next inbox writes behind that batch number each complete within 20 s, not the 60 s in-doubt deadline; everything converges",
        requires: &[],
        caps: &[],
        run: ec2::inbox_withdraw_hole,
    },
    Scenario {
        name: "idle-cost-link-flap",
        desc: "idle-cost with the lease holder's P2P links flagged down for 5 s every 15 s (a late ping round under host load): the holder polls idle requesters' inboxes cold, never hot, and the per-node budget holds",
        requires: &[],
        caps: &[],
        run: ec2::idle_cost_link_flap,
    },
    Scenario {
        name: "backup-failover",
        desc: "plan 30 M9: 3 nodes, 20 s lease TTL; three rounds of: 30 files written on the holder, the holder killed, its backup peer seals and holds within seconds (never the TTL), every acknowledged file is on it, the dead node remounts and converges; prints the failover-time distribution and the backup-policy write latency",
        requires: &[],
        caps: &[],
        run: m9::backup_failover,
    },
    Scenario {
        name: "backup-departs",
        desc: "plan 30 M9: the backup unmounts; the holder reconfigures it out and the third node in while writes keep completing; prints the S3 requests (lease PUTs) the reconfiguration cost and the write latency during it",
        requires: &[],
        caps: &[],
        run: m9::backup_departs,
    },
    Scenario {
        name: "no-peer-in-budget",
        desc: "plan 30 M9: CONSTELLATION_BACKUP_RTT_BUDGET_MS=0: no backup, local acknowledgements, the fast path open; a dead holder is replaced only when its lease expires (no seal, no fast takeover)",
        requires: &[],
        caps: &[],
        run: m9::no_peer_in_budget,
    },
    Scenario {
        name: "ack-s3-failover",
        desc: "plan 30 M9: a filesystem with ack_policy s3 (CONSTELLATION_ACK=s3 at fs create): every acknowledgement waits for the log (a follower through S3 alone sees each acknowledged file); the holder is frozen and a peer takes over well inside the 20 s lease; the thawed holder is deposed without conflicts",
        requires: &[],
        caps: &[],
        run: m9::ack_s3_failover,
    },
    Scenario {
        name: "single-node-unchanged",
        desc: "plan 30 M9: one node, default knobs: local policy, no backup or candidate, the fast path open, nothing waited, no reconfiguration CAS; prints the write latency and the S3 requests of 200 writes",
        requires: &[],
        caps: &[],
        run: m9::single_node_unchanged,
    },
    Scenario {
        name: "backup-failover-with-delegation",
        desc: "plan 30 M9 (strict): a reader holds a read delegation from the holder, which dies; the backup takes over inside the lease and its first write of that file is acknowledged only past the delegation horizon: no read the reader starts after the acknowledgement is stale, and no strict read degrades",
        requires: &[],
        caps: &[],
        run: m9::backup_failover_with_delegation,
    },
    Scenario {
        name: "holder-kill-rejoin",
        desc: "campaign 6 B-1: four nodes, backups on; HOLDER_KILL_ROUNDS (10) rounds of kill -9 the lease holder (plain, with a flock held on it, or a backup instead) and remount it with P2P on within 60 s, then every node converges; prints the remount-time distribution",
        requires: &[],
        caps: &[],
        run: rejoin::holder_kill_rejoin,
    },
    Scenario {
        name: "fuse-inval-storm",
        desc: "campaign 6 B-1 (the deadlock): three nodes create, rename and unlink in one shared directory at full speed for INVAL_STORM_SECS (12) s per round, listing it ls -l style every 8th cycle (so for an entry TTL after each listing a kernel holds dentries for the others' names), so every node's kernel is invalidated for the others' ops while it has requests in flight on that directory; INVAL_STORM_ROUNDS (3) rounds, each with kill -9 of the lease holder mid-load: no op exceeds its bound, no worker hangs, the killed daemon exits within 5 s, unaided or released by its zombie reaper (no zombie left wedged in fuse_reverse_inval_entry), it remounts within 60 s and the directory converges everywhere",
        requires: &[],
        caps: &[Cap::FuseAbort, Cap::PushInvalFull],
        run: inval_storm::fuse_inval_storm,
    },
    Scenario {
        name: "stale-daemon-lock",
        desc: "campaign 6 B-1: daemon.lock and control.sock held by a process that never answers: a mount fails within the attach timeout naming the live holder (no takeover of anything that can run), `status` fails within the control timeout, and once the holder counts as killed by the kernel the next mount takes the state dir over and serves",
        requires: &[],
        caps: &[],
        run: rejoin::stale_daemon_lock,
    },
    Scenario {
        name: "backup-partition",
        desc: "plan 30 M9: the holder and its backup are partitioned from each other (both keep S3 and the third node, which writes throughout): the holder reconfigures the backup out or the sealed backup takes over, never two holders; every acknowledged write is kept and the cluster converges after the heal",
        requires: &[],
        caps: &[],
        run: m9::backup_partition,
    },
    Scenario {
        name: "epoch-missing-node",
        desc: "plan 30 M10: 3 nodes, epoch_slack 1; C unmounts, A and B lose S3 and form an epoch of 2/3 that keeps writing; C returns with S3 but no P2P: its takeover of the expired lease is refused (no promise outlasts it), then S3 returns, the epoch flushes and everyone converges with no conflict",
        requires: &[],
        caps: &[],
        run: m10::epoch_missing_node,
    },
    Scenario {
        name: "epoch-member-dies-with-chunk",
        desc: "plan 30 M10 x M9: 3 nodes, f = 0, all lose S3; C writes two files in the epoch (manifests streamed to B, chunks only on C), B reads the first from C; C stops: B's read of the second fails (never other bytes), S3 returns and A ships everything but the manifests naming C's chunks (deferred; a fresh S3-only node reads the files empty, never a missing chunk), B keeps them as speculation; C returns and every node reads both files",
        requires: &[],
        caps: &[],
        run: m10::epoch_member_dies_with_chunk,
    },
    Scenario {
        name: "epoch-holder-retired",
        desc: "plan 30 M10: f = 1; A and B form an epoch carrying A's lease, A writes and is killed; B's frozen epoch keeps everyone out until the operator retires A (leave --node-id: the lease is fenced), B abandons the epoch, writes resume through a takeover with a promise, and A's state dir can never mount again",
        requires: &[],
        caps: &[],
        run: m10::epoch_holder_retired,
    },
    Scenario {
        name: "epoch-slack-zero-unchanged",
        desc: "plan 30 M10: epoch_slack 0 (the default) never touches heartbeat/; with epoch_slack 1 the steady state publishes only the mount's slack advertisement (promises are on demand); prints heartbeat PUTs per node per day",
        requires: &[],
        caps: &[],
        run: m10::epoch_slack_zero_unchanged,
    },
    Scenario {
        name: "delegated-subtrees",
        desc: "plan 30 M11: three nodes behind counting proxies; d1 delegated to b and d2 to c: each node's writes into its subtree are executed locally by the delegate and appended by the root, everything converges, the root appends nothing whose deps it lacks, the delegates make no S3 request for their writes; prints each node's latency on its delegated subtree vs its forwarded writes and the root's local ones, the aggregate throughput vs the single sequencer, the cross-subtree rename's latency and the S3 requests per phase",
        requires: &[],
        caps: &[],
        run: m11::delegated_subtrees,
    },
    Scenario {
        name: "cross-subtree-rename",
        desc: "plan 30 M11: a rename from d1 (delegated to b) into d2 (delegated to c) while both delegates write: the root recalls and drains both generations, executes the rename after their streams and ends them; the file is exactly where the rename put it on every node, nothing is lost or duplicated, and the directories can be delegated again and undelegated",
        requires: &[],
        caps: &[],
        run: m11::cross_subtree_rename,
    },
    Scenario {
        name: "delegate-crash",
        desc: "plan 30 M11: the delegate of d1 is killed mid-burst without a backup; the root reclaims its unrenewable grant within the grant TTL, a third node's write into d1 completes through the root, the dead node remounts with its journal and its acknowledged writes replay by rid; everything converges and d1 is delegated again at a higher generation",
        requires: &[],
        caps: &[],
        run: m11::delegate_crash,
    },
    Scenario {
        name: "delegate-crash-default-ttl",
        desc: "overload-cascade: delegate-crash at the default grant TTL (the lock grant TTL, 20 s): the root reclaims the dead delegate's subtree about ttl + margin after its last renewal, so a third node's write into d1 waits more than the short test TTL's whole life and less than ttl + 10 s; then the same recovery as delegate-crash",
        requires: &[],
        caps: &[],
        run: m11::delegate_crash_default_ttl,
    },
    Scenario {
        name: "marker-order",
        desc: "plan 30 M11: three writers each write data into d1 (delegated to b) then a marker into d2 (delegated to c); three watchers list d2 continuously: no node ever shows a marker without its data (the marker's deps carry the data's stream position)",
        requires: &[],
        caps: &[],
        run: m11::marker_order,
    },
    Scenario {
        name: "delegate-partition",
        desc: "plan 30 M11: the delegate of d1 loses its P2P link to the root (CONSTELLATION_FAULT_P2P_DENY_FILE; S3 and the third node stay): it stops on its own clock when it cannot renew, the root outwaits its recall or reclaims the grant, the third node's and the delegate's later writes go through the root; after the heal everything is everywhere with no conflict and d1 is delegated again",
        requires: &[],
        caps: &[],
        run: m11::delegate_partition,
    },
    Scenario {
        name: "p2p-off-no-delegation",
        desc: "plan 30 M11: CONSTELLATION_P2P=off on two nodes: delegation reports itself off, `delegate` is refused, nothing is ever delegated, appended or executed by a delegate, and both nodes' writes complete as before",
        requires: &[],
        caps: &[],
        run: m11::p2p_off_no_delegation,
    },
    Scenario {
        name: "flock-cross-node",
        desc: "plan 30 M14: two nodes under --locks cluster; an exclusive flock on one refuses (EWOULDBLOCK) and blocks the other until the unlock, shared locks coexist, fcntl ranges conflict across nodes and F_GETLK sees the remote holder, a write under the lock is read by the next holder; then --locks local for the record (both nodes hold LOCK_EX at once)",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: m14::flock_cross_node,
    },
    Scenario {
        name: "append-setattr-size",
        desc: "the K5 kind lane's busy-writer loss: two nodes (a holds, b its backup, 1 MiB chunks); on b, then on a, one O_APPEND descriptor appends 64 KiB blocks while a second descriptor fsyncs every second and the file is chown'd, chmod'ed, touched and hard-linked every 100 ms (kubelet's fsGroup pass on every republish); every acknowledged block is in the file, in order, on both nodes and on a fresh third node (was: a setattr/link reply carried the committed size, the kernel appended there, the file came up short with every call and close() succeeding)",
        requires: &[],
        caps: &[],
        run: appendsize::append_setattr_size,
    },
    Scenario {
        name: "concurrent-create-no-excl",
        desc: "OVH finding 1: four nodes open(O_CREAT) the same new name at once, without O_EXCL: every open succeeds on the one inode (and every racer's byte lands in it); with O_EXCL exactly one wins and the rest get EEXIST; on the sequencer and forwarding nodes, in a delegated subtree and through the S3 inbox; SQLite first-touching a new database from two nodes never fails",
        requires: &[],
        caps: &[],
        run: ovh::concurrent_create_no_excl,
    },
    Scenario {
        name: "nonowner-op-latency",
        desc: "OVH findings 4 and 6: under injected S3 latency, each of four nodes in turn runs the per-entry syscalls of an untar into one shared directory (open(O_CREAT|O_EXCL)+write+close, mkdir, symlink, link, chmod, chown, utimensat); a non-owner's median stays under half an S3 round trip (forwarded over P2P) like the sequencer's, except the write-through close of a new chunk",
        requires: &[],
        caps: &[],
        run: ovh::nonowner_op_latency,
    },
    Scenario {
        name: "delegated-op-latency",
        desc: "nonowner-op-latency with the shared directory delegated to b (placement off, backups on): the other nodes' ops go to the delegate, and a reply it evaluated behind its own unappended rows is answered from the root's pre-S3 stream of its append, not from S3; every non-owner's median stays under half an S3 round trip except the write-through close of a new chunk",
        requires: &[],
        caps: &[],
        run: ovh::delegated_op_latency,
    },
    Scenario {
        name: "slow-s3-no-seal",
        desc: "every S3 request >= 1.5 s (SLOWSEAL_LAT_MS 750 each way), product lease/sync/retry defaults, root lease pinned: three nodes write small files, rename and mkdir for SLOWSEAL_SECS (180) s; no backup ever seals the live holder, the holder keeps its lease and epoch and (almost always) a backup",
        requires: &[],
        caps: &[],
        run: slowseal::slow_s3_no_seal,
    },
    Scenario {
        name: "visibility-s3-latency",
        desc: "campaign 6 D2-OVH: every S3 request >= 300 ms; a non-holder, then the holder, writes a paced series of small files (write+fsync+close, write-through) while two other nodes poll for each in order; cross-node visibility p99 < 2 s (it travels over P2P, never waits for S3)",
        requires: &[],
        caps: &[],
        run: ovh::visibility_s3_latency,
    },
    Scenario {
        name: "sqlite-first-touch-latency",
        desc: "campaign 6 A-1: every S3 request >= 300 ms; 50 rounds of two nodes (two non-holders, then the holder and a non-holder) running CREATE TABLE IF NOT EXISTS + INSERT on one new SQLite database at once: no round fails (no disk I/O error), every database holds both rows on every node",
        requires: &["sqlite3"],
        caps: &[],
        run: ovh::sqlite_first_touch_latency,
    },
    Scenario {
        name: "sqlite-two-nodes",
        desc: "plan 30 M14: concurrent sqlite3 writers on one database from two nodes (rollback journal, fcntl locks, busy_timeout); PRAGMA integrity_check ok on both, every committed row present",
        requires: &["sqlite3"],
        caps: &[Cap::ClusterLocks],
        run: m14::sqlite_two_nodes,
    },
    Scenario {
        name: "lock-holder-partitioned",
        desc: "plan 30 M14: B holds LOCK_EX and is cut from the owner: its writes under the lock get EIO once its grant lapses; C, waiting, is granted only after the owner outwaited B's grant (ttl + margin) and never before B was fenced; after the heal B locks again",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: m14::lock_holder_partitioned,
    },
    Scenario {
        name: "lock-failover",
        desc: "plan 30 M14: four nodes with an M9 backup; B holds LOCK_EX and writes under it while the holder is killed; the backup takes over by seal, B's grant is reclaimed (no EIO), the contender's non-blocking attempts are refused throughout and it is granted once B unlocks",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: m14::lock_failover,
    },
    Scenario {
        name: "lock-holder-killed-contention",
        desc: "plan 30 M14 follow-up: four nodes increment one flock'ed counter (read, add, write, fsync); the holder is kill -9'd with the lock held: the survivors stall only until its grant is outwaited (ttl + margin), hand the lock on with no further outwait, and the count is exact; then a node killed while parked first in line costs the next waiter nothing",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: m14::lock_holder_killed_contention,
    },
    Scenario {
        name: "lock-fence-at-close",
        desc: "plan 30 M14 follow-up: B writes under a lock and is cut from the owner past its grant; C takes the lock and writes; B's close (fcntl: still locked; flock: unlocked first) returns EIO and the file holds C's data on every node",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: m14::lock_fence_at_close,
    },
    Scenario {
        name: "lock-latency",
        desc: "plan 30 M14 measurements: first lock on a file from a non-sequencer, cached re-locks, the sequencer's own locks, a contended handoff; and a lone node under --locks cluster against --locks local",
        requires: &[],
        caps: &[Cap::ClusterLocks],
        run: m14::lock_latency,
    },
    Scenario {
        name: "root-failover-with-delegates",
        desc: "plan 30 M11 phase 2b: four nodes with M9 backups; d1 and d2 delegated to b and c, both writing; the root is killed mid-burst; its backup takes the lease over by seal, learns the table from the log, the delegates re-stream what the old root never shipped; every acknowledged file is everywhere, the dead root remounts and converges",
        requires: &[],
        caps: &[],
        run: m11::root_failover_with_delegates,
    },
    Scenario {
        name: "delegate-crash-backup",
        desc: "plan 30 M11 phase 2b: the delegate of d1 has a backup (c) and is killed mid-burst; the root seals the backup, drains its tail, ends the generation and delegates d1 to c; every write b acknowledged is in the log, c writes locally, b remounts and converges",
        requires: &[],
        caps: &[],
        run: m11::delegate_crash_with_backup,
    },
    Scenario {
        name: "delegate-root-loss",
        desc: "two nodes, b the root's backup and the delegate of /d1, writing and fsyncing \
               there; the root is kill -9ed: b takes the root over and no write + fsync waits \
               past the seal-based failover bound; every write survives",
        requires: &[],
        caps: &[],
        run: delegroot::delegate_root_loss,
    },
    Scenario {
        name: "delegate-root-blackhole",
        desc: "delegate-root-loss with the root frozen (SIGSTOP) instead of killed, as a \
               force-deleted pod's vanished address answers nothing: b takes the root over \
               within the same bound and every write survives",
        requires: &[],
        caps: &[],
        run: delegroot::delegate_root_blackhole,
    },
    Scenario {
        name: "delegate-root-loss-ttl",
        desc: "two nodes, no backup (20 s lease), b the delegate of /d1 writing and fsyncing \
               there; the root is frozen: b's grant lapses unrenewed, its writes take the \
               lease path and b takes the root over by TTL within the lease TTL + 3 s (+ \
               slack); every write survives",
        requires: &[],
        caps: &[],
        run: delegroot::delegate_root_loss_ttl,
    },
    Scenario {
        name: "delegate-handoff-renewal",
        desc: "two nodes, b the root's backup and the delegate of /d1, writing and fsyncing \
               there; b is handed off three times (daemon --upgrade): the resumed image \
               re-adopts its delegation, its first writes wait for no renewal or reclaim",
        requires: &[],
        caps: &[],
        run: delegroot::delegate_handoff_renewal,
    },
    Scenario {
        name: "delegate-backup-handoff-failover",
        desc: "two nodes, b the root's backup and the delegate of /d1, writing and fsyncing \
               there; b is handed off (daemon --upgrade) and the root lists it as its backup \
               again within seconds (b never seals the live root's epoch); the root is then \
               kill -9ed and b takes it over by seal, within the seal-based bound",
        requires: &[],
        caps: &[],
        run: delegroot::delegate_backup_handoff_failover,
    },
    Scenario {
        name: "auto-placement",
        desc: "plan 30 M11 phase 2b: no operator; b dominates the writes under d1 for a window and the root delegates d1 to b by itself; c takes the writes over and b stops; after the dwell the placement recalls b's generation and after the cool-down delegates d1 to c, with no flapping",
        requires: &[],
        caps: &[],
        run: m11::auto_placement,
    },
    Scenario {
        name: "designation-as-delegation",
        desc: "plan 30 M11 phase 2b (plans 03–05): `offline /site` on b becomes a designated delegation in the root's table; c's and the root's writes under it are forwarded to b; b cut from everyone keeps writing locally while c gets EROFS under /site; after the heal everything converges; `online` recalls it and c's writes go through the root again",
        requires: &[],
        caps: &[],
        run: m11::designation_as_delegation,
    },
    Scenario {
        name: "shared-dir-multi-writer",
        desc: "plan 30 M12: four nodes creating unique names in one directory; the single sequencer (three nodes forwarding every create) against the directory split into four hash ranges (three delegated, one the root's): throughput, every name on every node, an identical listing everywhere, no S3 request added per file",
        requires: &[],
        caps: &[],
        run: m11::shared_dir_multi_writer,
    },
    Scenario {
        name: "hash-range-split-merge",
        desc: "plan 30 M12: the placement (on by default) splits a hot shared directory written by four nodes into hash ranges delegated to them; when the writers stop the ranges are recalled after the dwell and the directory is whole again; everything converges",
        requires: &[],
        caps: &[],
        run: m11::hash_range_split_merge,
    },
    Scenario {
        name: "cross-range-rename",
        desc: "plan 30 M12: a directory split two ways by hand; a rename from one range into the other is a cross-range op: the root recalls both ranges, executes it after their streams and delegates both again; the file is where the rename put it everywhere, the delegates execute locally again",
        requires: &[],
        caps: &[],
        run: m11::cross_range_rename,
    },
    Scenario {
        name: "inbox-create-storm-p2p-off",
        desc: "plan 30 M13 (hybrid): with P2P off, two non-holders sustain a create/unlink storm; sustained inbox demand escalates to a lease request, every op gets its errno right, and throughput is never worse than lease ping-pong (>= 41 ops/s)",
        requires: &[],
        caps: &[],
        run: inbox_create_storm_p2p_off,
    },
    Scenario {
        name: "dedup-write-storm",
        desc: "storm-hang regression: 16 threads per round write the same bytes to different files at once (one chunk hash raced into the cache), read back and unlink, in write-through then write-back; every read matches, nothing is held, no upload reports a pending chunk missing from the cache, and the unmount exits 0",
        requires: &[],
        caps: &[],
        run: dedup_write_storm,
    },
    Scenario {
        name: "inbox-sporadic-write-p2p-off",
        desc: "plan 30 M13 (hybrid): with P2P off, a non-holder writes one file every few seconds for a minute through the holder's inbox; zero lease handoffs, no escalation, every write visible on the holder, p50/p99 latency bounded by the warm poll tier",
        requires: &[],
        caps: &[],
        run: inbox_sporadic_write_p2p_off,
    },
    Scenario {
        name: "inbox-requester-crash-mid-batch",
        desc: "plan 30 M13: a requester dies right after submitting a batch the holder has not read; the batch executes exactly once anyway, and the remounted requester resumes its numbering (LIST-last) so its next batches are polled",
        requires: &[],
        caps: &[],
        run: inbox_requester_crash_mid_batch,
    },
    Scenario {
        name: "small-file-write-path",
        desc: "OVH finding 3: under injected S3 latency the sequencer and a non-owner close small unique files, in write-through then write-back; through: one S3 request per file on the writer (no condemned-pointer read, no HEAD) and one S3 round trip per close; back: no S3 round trip per close on either node (the non-owner forwards with its chunks still uploading, the sequencer holds the manifest until they are reported up); every file reads back right on a third node",
        requires: &[],
        caps: &[],
        run: writepath::small_file_write_path,
    },
    Scenario {
        name: "nonowner-back-crash",
        desc: "under slow S3 a non-owner closes files under write-back and is killed with its uploads in flight, then remounted: the sequencer never ships a manifest naming the missing chunks meanwhile (its own reader of one waits for the bytes), ships them once they are up after the restart, a third node reads every file right, and fsck finds no dangling reference",
        requires: &[],
        caps: &[],
        run: writepath::nonowner_back_crash,
    },
    Scenario {
        name: "inbox-holder-takeover-pending-batch",
        desc: "plan 30 M13: the holder dies with an unread inbox batch; the next holder's takeover gate drains it before serving, the blocked requester's create returns success (not EIO), and every node sees one inode",
        requires: &[],
        caps: &[],
        run: inbox_holder_takeover_pending_batch,
    },
    Scenario {
        name: "subtree-confinement",
        desc: "plan 31 §6.12: one daemon serves the whole tree, a volume view and a maintenance view (both --confine-links, volumes marked as link domains); `..` at the volume root, `.constellation` history, and hard links (in, across mounts, through a handle moved out, between volumes) stay confined through the kernel",
        requires: &["fusermount3"],
        caps: &[Cap::HardLinks, Cap::Xattrs],
        run: confinement::subtree_confinement,
    },
    Scenario {
        name: "session-handover-idle",
        desc: "plan 31 C4b: `daemon --upgrade` with no op in flight; the mount never \
               disappears (same st_dev, no error), held descriptors keep working, \
               contents intact, the resumed image serves",
        requires: &[],
        caps: &[],
        run: handover::session_handover_idle,
    },
    Scenario {
        name: "upgrade-under-load",
        desc: "plan 31 C4b: three `daemon --upgrade`s in a row under a writer, a \
               creator and a reader with descriptors held open; zero errors \
               (no ENOTCONN/EIO), every write lands (verified after a remount)",
        requires: &[],
        caps: &[],
        run: handover::upgrade_under_load,
    },
    Scenario {
        name: "transport-detach-refused",
        desc: "plan 38 §3(e): a mount that asked for the transport ladder (auto) under load \
               while `daemon --upgrade` runs -- refused with an error naming the transport on \
               a ring session (no request lost, the mount keeps serving, node.status still \
               reports uring), served on a session that fell back to dev_fuse",
        requires: &[],
        caps: &[],
        run: transport::transport_detach_refused,
    },
    Scenario {
        name: "transport-refused-registration",
        desc: "plan 38 §2.4: the kernel refuses the io_uring queues' registration after FUSE_INIT \
               committed the connection to rings (CONSTELLATION_FUSE_URING_FAULT=malformed-register) \
               -- the mount comes up on dev_fuse anyway, serves, and logs the downgrade once",
        requires: &[crate::suites::FUSE_URING],
        caps: &[],
        run: transport::transport_refused_registration,
    },
    Scenario {
        name: "transport-seccomp-denied",
        desc: "plan 38 §2.4/§8: the daemon runs under a seccomp filter refusing io_uring_setup(2) \
               (EPERM), as a container's default profile does -- auto falls back to dev_fuse, the \
               mount serves, the downgrade is logged once; passes on every host",
        requires: &[],
        caps: &[],
        run: transport::transport_seccomp_denied,
    },
    Scenario {
        name: "transport-enomem-ring",
        desc: "plan 38 §2.4: RLIMIT_AS shaped so the ring's buffer reservation fails (ENOMEM) -- \
               auto falls back to dev_fuse without a crash, the mount serves, logged once",
        requires: &[crate::suites::FUSE_URING],
        caps: &[],
        run: transport::transport_enomem_ring,
    },
    Scenario {
        name: "transport-abort-while-armed",
        desc: "plan 38 §6: a fusectl abort of a ring session with its entries armed and a reader \
               running -- the reader fails at once, the daemon unwinds and exits cleanly, nothing \
               leaks, the mountpoint takes a fresh ring mount",
        requires: &[crate::suites::FUSE_URING],
        caps: &[Cap::FuseAbort],
        run: transport::transport_abort_while_armed,
    },
    Scenario {
        name: "passthrough-eviction-while-open",
        desc: "plan 38 §6/Z3b: a single-chunk file held open read-only is served by FUSE \
               passthrough; node.status counts each descriptor and the disk cache holds one \
               open pin per descriptor; the cache filled 3x past its budget and pruned to zero \
               keeps the chunk; after the close it is evicted like any other",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::eviction_while_open,
    },
    Scenario {
        name: "passthrough-remote-write-cto",
        desc: "plan 38 §6/Z3b: two nodes; a passthrough handle open on A keeps the bytes it \
               was opened on after B rewrites the file (close-to-open), while a fresh open on \
               A sees B's bytes even with the old handle open; after the close a new open is \
               passthrough on the new chunk",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::remote_write_cto,
    },
    Scenario {
        name: "passthrough-local-writer",
        desc: "plan 38 §3(c)/Z3b: on one mount, a read-write open of a passthrough-open file \
               is refused ETXTBSY; a write-only open is served and every later open sees its \
               write, while the passthrough handle opened before it keeps its bytes until \
               closed",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::local_writer,
    },
    Scenario {
        name: "passthrough-odirect",
        desc: "plan 38 §6/Z3b: an O_DIRECT read of a passthrough-open file fetches every byte \
               from the cache's block device even with the chunk warm in the page cache \
               (/proc/thread-self/io), and no passthrough read, buffered or direct, reaches \
               the daemon",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::odirect,
    },
    Scenario {
        name: "passthrough-handover",
        desc: "plan 38 Z3b: a passthrough handle held across `daemon --upgrade`: the new image \
               counts it and holds its chunk's pin (a prune keeps the chunk), a new open of the \
               file reuses the handed-over backing id, and the close in the new image releases \
               both",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::handover,
    },
    Scenario {
        name: "passthrough-disabled-by-verify-always",
        desc: "plan 38 §2.3/Z3b: a mount with --cache-verify always reports passthrough off \
               with reason cache_verify_always (privileged or not), holds no open pin, and \
               every read reaches the daemon",
        requires: &[],
        caps: &[],
        run: passthrough::disabled_by_verify_always,
    },
    Scenario {
        name: "passthrough-default-by-mount-mode",
        desc: "plan 38 §3(c)/Z3b: without CONSTELLATION_FUSE_PASSTHROUGH a writable mount \
               does not ask (reason writable_mount; a read-write open beside a reader is \
               ordinary), while a read-only mount of a snapshot of the same file negotiates it; \
               with the memory tier on the chunk its first read admits is not handed over, and \
               with the tier off the frozen file is served by passthrough (counted, pinned, \
               byte-exact, no daemon read) once its chunk is verified, a read-write open is \
               EROFS and the close releases the pin",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::default_by_mount_mode,
    },
    Scenario {
        name: "passthrough-on-every-transport",
        desc: "plan 38 Z2c with Z3b/Z3c: a read-only snapshot mount (no cluster locks) under \
               dev-fuse and auto reports the transport and fallback the ladder gives it \
               (auto: the ring on a ring host, never a cluster_locks fallback) and \
               serves a verified chunk's open by passthrough on each: counted, pinned, \
               byte-exact, no daemon read",
        requires: &[suites::CAP_SYS_ADMIN, suites::LINUX_6_9],
        caps: &[],
        run: passthrough::on_every_transport,
    },
    Scenario {
        name: "zero-copy-single-chunk",
        desc: "plan 38 §3(d)/Z4b, 7.3 lane: on a uring_zc mount, reads inside one chunk of a \
               multi-chunk file are answered by one READ_FIXED from the chunk file -- buffered \
               (every data-carrying read counted) and O_DIRECT (each pread exactly one daemon \
               read and one zero-copy read: first chunk, middle, short tail) -- byte-exact",
        requires: &[suites::FUSE_URING_ZERO_COPY],
        caps: &[],
        run: zerocopy::single_chunk,
    },
    Scenario {
        name: "zero-copy-chunk-spanning-fallback",
        desc: "plan 38 §3(d)/Z4b, 7.3 lane: one O_DIRECT read crossing the 1 MiB chunk \
               boundary is one daemon read answered by the memory-cache path (zero-copy count \
               unchanged, bytes exact), and the same handle's next read inside one chunk is \
               zero-copy again",
        requires: &[suites::FUSE_URING_ZERO_COPY],
        caps: &[],
        run: zerocopy::chunk_spanning_fallback,
    },
    Scenario {
        name: "zero-copy-eviction-while-inflight",
        desc: "plan 38 §3(d)/Z4b, 7.3 lane: a reader hammering one chunk with zero-copy \
               O_DIRECT reads while the cache is overfilled and pruned to nothing twice: the \
               chunk stays (one open pin), every read is byte-exact and zero-copy; after the \
               close the pin goes, the chunk is evictable and the file reads again",
        requires: &[suites::FUSE_URING_ZERO_COPY],
        caps: &[],
        run: zerocopy::eviction_while_inflight,
    },
    Scenario {
        name: "zero-copy-disabled-by-verify-always",
        desc: "plan 38 §2.3/Z4b, 7.3 lane: a mount with --cache-verify always on a zero-copy \
               host negotiates plain uring (no fallback), reports passthrough off with reason \
               cache_verify_always, and under buffered and O_DIRECT reads counts no zero-copy \
               read while the memory cache's hits move",
        requires: &[suites::FUSE_URING_ZERO_COPY],
        caps: &[],
        run: zerocopy::disabled_by_verify_always,
    },
    Scenario {
        name: "transport-cluster-locks-auto",
        desc: "plan 38 Z2c as decided 2026-10-05: under auto a mount with cluster locks takes \
               the ring with the deeper queue (32; an explicit depth wins), --locks local the \
               ordinary depth (8), uring the same as auto, no fallback and no lock-wait \
               downgrade, and each serves; daemon --upgrade of the auto mount is refused on the \
               ring and served on a fallback (the resumed mount keeps its first rung); off a \
               ring host every leg falls back for a rung other than the locks; every host",
        requires: &[],
        caps: &[],
        run: transport::transport_cluster_locks_auto,
    },
    Scenario {
        name: "transport-lock-wait-budget",
        desc: "plan 38 Z2c, real kernel: a cluster-lock mount on the ring under auto, depth+3 \
               processes pinned to one CPU blocking in F_SETLKW while a process on that CPU \
               holds the lock -- depth-1 wait, the rest get ENOLCK at once, the holder's write \
               and unlock go through, every waiter that waited is granted, and \
               lock_wait_downgrades counts the refusals (depth 4, and the default 32)",
        requires: &[crate::suites::FUSE_URING, "taskset"],
        caps: &[],
        run: transport::transport_lock_wait_budget,
    },
    Scenario {
        name: "lifecycle-suspend-mid-write",
        desc: "plan 31 C8: a writer on the lease holder records every fsync'd file; the holder is suspended (node.lifecycle, 15 s deadline) mid-stream: every view published, journal shipped, lease released, P2P quiet, all within the deadline; another node takes the writes over and every node reads every acknowledged file byte-exact; the holder resumes and the writer goes on: no acknowledged write lost",
        requires: &[],
        caps: &[],
        run: lifecycle::lifecycle_suspend_mid_write,
    },
    Scenario {
        name: "lifecycle-resume-rejoin",
        desc: "plan 31 C8: a suspended lease holder's lease moves to another node, which writes, renames and deletes while it sleeps (its local reads still served); resumed, it catches up, rejoins P2P and writes again",
        requires: &[],
        caps: &[],
        run: lifecycle::lifecycle_resume_rejoin,
    },
    Scenario {
        name: "lifecycle-metered-uploads",
        desc: "plan 31 C8: an unmetered-only node on a metered network (NetworkChanged) uploads no chunk for plain closes (S3 chunk PUT counter still, pending queue grows, the peer sees none of the content), an fsync still uploads at once; unmetered again, everything drains to both nodes intact",
        requires: &[],
        caps: &[],
        run: lifecycle::lifecycle_metered_uploads,
    },
    Scenario {
        name: "writeback-close-metered-nonowner",
        desc: "plan 31 C8 + write-back: a non-owner's back close answered at once keeps its chunk held through a later refusal whose position only its own deferred rows hold back (no upload, no read wait); a non-owner's back close with its uploads held (metered), forwarded before it applied the holder's create of the file, returns promptly by uploading its own chunks when nothing else can bring its record back (unbacked holder: was a 120 s stall ending in doubt; ack=s3: likewise) and keeps them held when a backed holder's stream answers; then a chmod and a rename right after a back close that returned with its chunk held (ops depending on that close: also a 120 s stall before), in all three modes; content readable on the writer at once and on both nodes once unmetered",
        requires: &[],
        caps: &[],
        run: lifecycle::writeback_close_metered_nonowner,
    },
];

/// Plan 30 M0: scenarios that reproduce a known, not-yet-fixed bug
/// (`docs/plans/v1/done/30-write-path-resilience-and-scale-out.md` §1.1).
/// Kept out of [`SCENARIOS`] so a bare `harness run` (no names) never
/// treats a documented bug as a regression: these are expected to FAIL
/// until the milestone that fixes the underlying bug moves them into
/// `SCENARIOS`, unchanged, as its regression test. `harness list` prints
/// them under their own heading; `harness run <name>` resolves a name in
/// either list.
///
/// `stress-ng-fs-nodes`: under three nodes' worth of stress-ng the lease
/// holder's core stalls for seconds at a time; non-holders' lock grants
/// lapse (`EIO` under their locks), backups seal the live holder's epoch,
/// forwarded mutations wait out their 120 s timeout, and a non-holder's
/// FUSE workers all end up waiting, leaving stress-ng processes unkillable
/// (TESTING.md, "stress-ng-fs").
///
/// `stress-ng-fs-faults` (fails about one run in four): every S3 cut opens
/// a continuation epoch on the single node, and its close is handled as
/// the lease going away — every cluster lock grant is dropped (`lease
/// gone: lock grants dropped`), the lock holders' writes fail with `EIO`
/// and new locks with `ENOLCK` (`fcntl`: `F_OFD_SETLK` `ENOLCK`) until the
/// lease is back; once the epoch froze instead and a create after the run
/// failed with `EROFS`.
pub const KNOWN_BUG_REPROS: &[Scenario] = &[
    Scenario {
        name: "stress-ng-fs-nodes",
        desc: "stress-ng-fs on 3 P2P nodes of one filesystem at once, each in its own directory (shared lease, journal, cluster locks, forwarding); every node's verdicts, cross-node canaries",
        requires: &["stress-ng"],
        caps: &[],
        run: stressfs::stress_ng_fs_nodes,
    },
    Scenario {
        name: "stress-ng-fs-faults",
        desc: "stress-ng-fs under 40+/-20 ms S3 latency and a 1.5 s S3 cut every 4-7 s",
        requires: &["stress-ng"],
        caps: &[],
        run: stressfs::stress_ng_fs_faults,
    },
];

fn setup(name: &str) -> Result<(S3Env, tempfile::TempDir)> {
    setup_in(name, &std::env::temp_dir())
}

/// [`setup`] with the scenario's root (mounts, state, caches) under `dir`.
fn setup_in(name: &str, dir: &std::path::Path) -> Result<(S3Env, tempfile::TempDir)> {
    let env = S3Env::start().context("starting S3 environment")?;
    let mut root = tempfile::Builder::new()
        .prefix(&format!("harness-{name}-"))
        .tempdir_in(dir)?;
    // CHAOS_KEEP_TMP=1 keeps mount logs + state dirs around after the
    // scenario returns, for offline inspection of failures.
    if std::env::var_os("CHAOS_KEEP_TMP").is_some_and(|v| v != "0") {
        root.disable_cleanup(true);
        eprintln!(
            "CHAOS_KEEP_TMP: artifacts kept at {}",
            root.path().display()
        );
    }
    Ok((env, root))
}

fn one_client(env: &S3Env, root: &std::path::Path, prefix: &str) -> Result<Client> {
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root, "c0", &env.endpoint, &backend)?;
    c.fs_create()?;
    c.mount()?;
    Ok(c)
}

fn ts() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// `open(O_CREAT | O_EXCL)`: succeeds only if `name` does not already
/// exist under `mnt`. Used by the plan 30 M0 known-bug repros, which
/// care about the exact POSIX outcome of a fresh create rather than
/// merely writing some content.
fn create_new(mnt: &std::path::Path, name: &str) -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(mnt.join(name))
        .map(|_| ())
}

/// The kernel inode number `path` currently resolves to, or `None` if it
/// does not exist. Two nodes agreeing a name exists is not the same as
/// agreeing *what* exists there — a bug that creates two independent
/// entries under one name only shows up as an inode mismatch.
fn ino_of(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

// --- scenarios ---

fn baseline(seed: u64) -> Result<()> {
    let (env, root) = setup("baseline")?;
    let _proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("base-{}", ts()))?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    for block in 0..5 {
        wl.run_block(&c.mnt, &mut model, 60)?;
        model
            .verify(&c.mnt)
            .with_context(|| format!("block {block}"))?;
    }
    c.unmount()?;
    Ok(())
}

fn bucket_objects(endpoint: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let response = crate::s3auth::get(&format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}"))
        .call()
        .context("listing raw E2E objects")?
        .into_string()?;
    let mut keys = Vec::new();
    let mut rest = response.as_str();
    while let Some(start) = rest.find("<Key>") {
        rest = &rest[start + 5..];
        let Some(end) = rest.find("</Key>") else {
            break;
        };
        keys.push(rest[..end].to_string());
        rest = &rest[end + 6..];
    }
    // Plan 28 added the metadata tree: packs (nodes whose keys are file
    // names), spilled blobs and commits are all sealed on E2E, so they
    // are scanned with the rest.
    keys.into_iter()
        .filter(|key| {
            ["/chunks/", "/log/", "/packs/", "/blobs/", "/commits/"]
                .iter()
                .any(|kind| key.contains(kind))
        })
        .map(|key| {
            let mut bytes = Vec::new();
            crate::s3auth::get(&format!("{endpoint}/{BUCKET}/{key}"))
                .call()
                .with_context(|| format!("fetching raw object {key}"))?
                .into_reader()
                .read_to_end(&mut bytes)?;
            Ok((key, bytes))
        })
        .collect()
}

fn e2e_basic(seed: u64) -> Result<()> {
    let (env, root) = setup("e2e-basic")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("e2e-basic-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut writer = Client::new(root.path(), "writer", &env.endpoint, &backend)?.with_e2e();
    writer.fs_create()?;
    writer.mount()?;

    let marker = vec![b'A'; 2 << 20];
    std::fs::write(writer.mnt.join("known-marker"), &marker)?;
    std::fs::create_dir(writer.mnt.join("model"))?;
    let mut model = Model::default();
    let mut workload = Workload::new(seed, "w");
    workload.run_block(&writer.mnt.join("model"), &mut model, 40)?;
    model.verify(&writer.mnt.join("model"))?;
    writer.unmount()?;

    let objects = bucket_objects(&env.direct_endpoint, &format!("{prefix}/"))?;
    anyhow::ensure!(
        !objects.is_empty(),
        "E2E filesystem produced no chunks/logs"
    );
    anyhow::ensure!(
        objects.iter().any(|(key, _)| key.contains("/packs/"))
            && objects.iter().any(|(key, _)| key.contains("/commits/")),
        "E2E filesystem published no metadata tree, so the scan below proves nothing about it"
    );
    let name = b"known-marker";
    for (key, bytes) in &objects {
        anyhow::ensure!(
            !bytes.windows(name.len()).any(|window| window == name),
            "a file name leaked into {key}"
        );
        anyhow::ensure!(
            !bytes
                .windows(64)
                .any(|window| window.iter().all(|b| *b == b'A')),
            "plaintext marker leaked into {key}"
        );
        anyhow::ensure!(
            !bytes.starts_with(b"CCH1") && !bytes.starts_with(b"\x28\xb5\x2f\xfd"),
            "unencrypted object format visible in {key}"
        );
    }

    writer.assert_wrong_passphrase_rejected()?;
    let mut cold = Client::new(root.path(), "cold", &env.endpoint, &backend)?.with_e2e();
    cold.mount()?;
    anyhow::ensure!(
        std::fs::read(cold.mnt.join("known-marker"))? == marker,
        "cold E2E remount returned different bytes"
    );
    eventually("cold E2E model restore", Duration::from_secs(20), || {
        model.verify(&cold.mnt.join("model"))
    })?;
    cold.unmount()
}

fn e2e_two_nodes(_seed: u64) -> Result<()> {
    let (env, root) = setup("e2e-two-nodes")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/e2e-two-{}", ts());
    let mut a = coop_client(root.path(), "e2e-a", &env.endpoint, &backend)?.with_e2e();
    let mut b = coop_client(root.path(), "e2e-b", &env.endpoint, &backend)?.with_e2e();
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    let (data, expected) = blob(8);
    std::fs::write(a.mnt.join("shared"), &data)?;
    eventually("E2E metadata visible", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("shared").exists(), "shared file missing");
        Ok(())
    })?;
    std::thread::sleep(Duration::from_secs(2));
    proxy.latency(200, 0)?;
    let got = std::fs::read(b.mnt.join("shared"))?;
    anyhow::ensure!(
        blake3::hash(&got) == expected,
        "E2E peer read corrupted data"
    );
    let status = b.control_status()?;
    anyhow::ensure!(
        status["coop"]["peer_hits"].as_u64().unwrap_or(0) > 0,
        "E2E cold reader did not use its encrypted peer path: {status}"
    );
    proxy.heal()?;
    a.unmount()?;
    b.unmount()
}

/// `fs passwd` must be a live operation: with two E2E nodes mounted,
/// changing the passphrase rewraps only the master key, so both keep
/// deriving the same DEKs and gossip seed and keep converging with no
/// remount. Afterwards a fresh mount needs the new passphrase and the
/// old one is refused.
fn passwd_live_cluster(seed: u64) -> Result<()> {
    let (env, root) = setup("passwd-live-cluster")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("passwd-live-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    // `with_e2e` sets this passphrase; `passwd` rotates it.
    let old = "harness-correct-passphrase";
    let new = "harness-rotated-passphrase";

    let mk = |name: &str| -> Result<Client> {
        Ok(coop_client(root.path(), name, &env.endpoint, &backend)?.with_e2e())
    };
    let mut a = mk("passwd-a")?;
    let mut b = mk("passwd-b")?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;

    let marker = vec![b'Z'; 2 << 20];
    std::fs::write(a.mnt.join("known-marker"), &marker)?;
    std::fs::create_dir(a.mnt.join("hot"))?;
    for i in 0..10 {
        std::fs::write(a.mnt.join("hot").join(format!("f{i}")), format!("v{i}"))?;
    }
    eventually(
        "pre-passwd writes visible on B",
        Duration::from_secs(40),
        || {
            anyhow::ensure!(
                b.mnt.join("hot").join("f9").exists(),
                "pre-passwd tail incomplete"
            );
            Ok(())
        },
    )?;

    // Change the passphrase with BOTH nodes still mounted.
    a.passwd(old, new)?;
    anyhow::ensure!(
        a.is_mounted() && b.is_mounted(),
        "a node dropped during passwd"
    );

    // The live nodes must keep working with the keys they already hold:
    // A writes new metadata (into a fresh model dir and more of `hot`),
    // B tails and decrypts it. No remount anywhere.
    let mut model = Model::default();
    let mut workload = Workload::new(seed, "w");
    std::fs::create_dir(a.mnt.join("model"))?;
    workload.run_block(&a.mnt.join("model"), &mut model, 40)?;
    for i in 10..20 {
        std::fs::write(a.mnt.join("hot").join(format!("f{i}")), format!("v{i}"))?;
    }
    eventually(
        "post-passwd convergence on B without remount",
        Duration::from_secs(40),
        || {
            anyhow::ensure!(a.is_mounted() && b.is_mounted(), "a node was remounted");
            anyhow::ensure!(
                b.mnt.join("hot").join("f19").exists(),
                "post-passwd tail incomplete"
            );
            model.verify(&b.mnt.join("model"))
        },
    )?;

    // The passphrase really changed: the old one is refused, and a fresh
    // cold mount with the new one reads the data.
    a.assert_passphrase_rejected(old)?;
    let mut cold = coop_client(root.path(), "passwd-cold", &env.endpoint, &backend)?
        .with_e2e()
        .with_env("CONSTELLATION_PASSPHRASE", new);
    cold.mount()?;
    anyhow::ensure!(
        std::fs::read(cold.mnt.join("known-marker"))? == marker,
        "cold mount with the new passphrase returned different bytes"
    );
    cold.unmount()?;

    a.unmount()?;
    b.unmount()
}

fn latency(seed: u64) -> Result<()> {
    let (env, root) = setup("latency")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("lat-{}", ts()))?;
    proxy.latency(150, 50)?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    for block in 0..3 {
        wl.run_block(&c.mnt, &mut model, 40)?;
        model
            .verify(&c.mnt)
            .with_context(|| format!("block {block}"))?;
    }
    proxy.heal()?;
    c.unmount()?;
    Ok(())
}

fn slow_network(seed: u64) -> Result<()> {
    let (env, root) = setup("slow-network")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("slow-{}", ts()))?;
    proxy.bandwidth(256)?;
    proxy.slicer(1024, 100)?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    for block in 0..2 {
        wl.run_block(&c.mnt, &mut model, 25)?;
        model
            .verify(&c.mnt)
            .with_context(|| format!("block {block}"))?;
    }
    proxy.heal()?;
    c.unmount()?;
    Ok(())
}

fn s3_outage(seed: u64) -> Result<()> {
    let (env, root) = setup("s3-outage")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("out-{}", ts()))?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");

    // Healthy warm-up so the cache holds data.
    wl.run_block(&c.mnt, &mut model, 50)?;
    model.verify(&c.mnt)?;

    // Cut S3. Reads of cached data must keep working.
    proxy.cut()?;
    for rel in model.files().iter().take(10) {
        let _ = std::fs::read(c.mnt.join(rel))
            .with_context(|| format!("cached read of {rel:?} during outage"))?;
    }
    // Metadata ops are local in phase 1 and must also work offline.
    std::fs::create_dir(c.mnt.join("during-outage"))?;
    model.mkdir(std::path::Path::new("during-outage"));

    // Spool observability (DESIGN.md §12): the control API must report
    // the metadata backlog and the shipping error while S3 is down.
    std::thread::sleep(Duration::from_secs(3)); // let a flush attempt fail
    let status = c.control_status()?;
    let backlog = status["spool"]["journal_backlog"].as_u64().unwrap_or(0);
    anyhow::ensure!(
        backlog > 0,
        "expected journal backlog during outage, got {status}"
    );
    anyhow::ensure!(
        !status["spool"]["last_ship_error"].is_null(),
        "expected last_ship_error during outage, got {status}"
    );

    // Heal and verify everything still converges — including the spool
    // draining back to zero.
    proxy.heal()?;
    std::thread::sleep(Duration::from_secs(1));
    wl.run_block(&c.mnt, &mut model, 30)?;
    model.verify(&c.mnt)?;
    std::thread::sleep(Duration::from_secs(3)); // >= 1 shipper tick
    let status = c.control_status()?;
    anyhow::ensure!(
        status["spool"]["journal_backlog"].as_u64() == Some(0)
            && status["spool"]["last_ship_error"].is_null(),
        "spool did not drain after heal: {status}"
    );
    c.unmount()?;
    Ok(())
}

fn s3_flap(seed: u64) -> Result<()> {
    let (env, root) = setup("s3-flap")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("flap-{}", ts()))?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    for block in 0..4 {
        if block % 2 == 1 {
            proxy.cut()?;
            std::thread::sleep(Duration::from_millis(300));
            proxy.heal()?;
        }
        if let Err(error) = wl.run_block(&c.mnt, &mut model, 30) {
            anyhow::bail!("{error:#}; daemon log:\n{}", c.tail_log_n(120));
        }
        model
            .verify(&c.mnt)
            .with_context(|| format!("block {block}"))?;
    }
    c.unmount()?;
    Ok(())
}

fn kill9_remount(seed: u64) -> Result<()> {
    let (env, root) = setup("kill9")?;
    let _proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("kill-{}", ts()))?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    for round in 0..3 {
        wl.run_block(&c.mnt, &mut model, 40)?;
        // Block boundary: all files closed => everything is committed.
        model.verify(&c.mnt)?;
        c.kill9()?;
        c.mount()
            .with_context(|| format!("remount after kill #{round}"))?;
        // Everything committed before the crash must survive it.
        model
            .verify(&c.mnt)
            .with_context(|| format!("post-crash verification #{round}"))?;
    }
    c.unmount()?;
    Ok(())
}

fn cold_cache(seed: u64) -> Result<()> {
    let (env, root) = setup("cold-cache")?;
    let _proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("cold-{}", ts()))?;
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    for block in 0..3 {
        wl.run_block(&c.mnt, &mut model, 40)?;
        c.unmount()?;
        c.drop_cache()?;
        c.mount()?;
        // All content must be reconstructible purely from S3.
        model
            .verify(&c.mnt)
            .with_context(|| format!("cold block {block}"))?;
    }
    c.unmount()?;
    Ok(())
}

/// Phase-1 scope: metadata is per-node, so two clients get disjoint
/// filesystems (different prefixes) in one bucket. Verifies the shared
/// chunk plane doesn't cross-corrupt. Upgraded to a shared-namespace
/// scenario when metadata log shipping lands (phase 2).
/// Poll until `f` succeeds or `deadline` passes (cross-node
/// propagation is asynchronous: sync interval + FUSE TTLs). An
/// interrupted run ([`crate::interrupt::aborting`]) stops polling at the
/// first failure.
pub fn eventually(what: &str, deadline: Duration, mut f: impl FnMut() -> Result<()>) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
        match f() {
            Ok(()) => return Ok(()),
            Err(e) if start.elapsed() > deadline => {
                return Err(e.context(format!("'{what}' not reached within {deadline:?}")))
            }
            Err(e) if crate::interrupt::aborting() => {
                return Err(e.context(format!("'{what}' interrupted")))
            }
            Err(_) => std::thread::sleep(Duration::from_millis(250)),
        }
    }
}

fn raw_key(endpoint: &str, key: &str) -> String {
    format!("{endpoint}/{BUCKET}/{key}")
}

fn chunk_key(prefix: &str, data: &[u8]) -> String {
    let hash = blake3::hash(data).to_hex().to_string();
    format!("{prefix}/chunks/{}/{}/{}", &hash[..2], &hash[2..4], hash)
}

fn raw_exists(endpoint: &str, key: &str) -> bool {
    crate::s3auth::head(&raw_key(endpoint, key)).call().is_ok()
}

fn gc_lifecycle(_seed: u64) -> Result<()> {
    let (env, root) = setup("gc-lifecycle")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("gc-life-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut client = Client::new(root.path(), "gc", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_LEASE_TTL_MS", "200")
        .with_env("CONSTELLATION_GC_HORIZON_S", "0");
    client.fs_create()?;
    client.mount()?;
    let doomed = b"doomed-unique-content";
    let frozen = b"snapshot-only-content";
    let live = b"still-live-content";
    std::fs::create_dir(client.mnt.join("tree"))?;
    std::fs::write(client.mnt.join("tree/frozen"), frozen)?;
    client.snapshot_create("/tree@keep")?;
    std::fs::write(client.mnt.join("tree/doomed"), doomed)?;
    std::fs::remove_file(client.mnt.join("tree/frozen"))?;
    std::fs::remove_file(client.mnt.join("tree/doomed"))?;
    std::fs::write(client.mnt.join("tree/live"), live)?;

    let output = client.gc_run()?;
    let gc_stdout = String::from_utf8_lossy(&output.stdout);
    anyhow::ensure!(
        output.status.success(),
        "gc run failed: {}{}",
        gc_stdout,
        String::from_utf8_lossy(&output.stderr)
    );
    anyhow::ensure!(
        !raw_exists(&env.direct_endpoint, &chunk_key(&prefix, doomed)),
        "dereferenced unique chunk survived GC: {gc_stdout}"
    );
    anyhow::ensure!(
        raw_exists(&env.direct_endpoint, &chunk_key(&prefix, frozen)),
        "snapshot-rooted chunk was collected"
    );
    anyhow::ensure!(
        raw_exists(&env.direct_endpoint, &chunk_key(&prefix, live)),
        "live chunk was collected"
    );
    anyhow::ensure!(
        std::fs::read(client.mnt.join("tree/live"))? == live,
        "live file changed after GC"
    );
    let listing = crate::s3auth::get(&format!(
        "{}/{BUCKET}?list-type=2&prefix={prefix}/gc/journal/",
        env.direct_endpoint
    ))
    .call()?
    .into_string()?;
    anyhow::ensure!(listing.contains("<Key>"), "GC journal is empty");
    client.unmount()
}

fn gc_dedup_race(_seed: u64) -> Result<()> {
    let (env, root) = setup("gc-dedup-race")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("gc-race-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut client = Client::new(root.path(), "race", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_LEASE_TTL_MS", "1000")
        .with_env("CONSTELLATION_GC_HORIZON_S", "0");
    client.fs_create()?;
    client.mount()?;
    let bytes = b"dedup-race-content";
    std::fs::write(client.mnt.join("old"), bytes)?;
    std::fs::remove_file(client.mnt.join("old"))?;
    let child = client.gc_process()?;
    eventually(
        "condemned pointer publication",
        Duration::from_secs(10),
        || {
            anyhow::ensure!(
                raw_exists(&env.direct_endpoint, &format!("{prefix}/gc/condemned.json")),
                "not published"
            );
            Ok(())
        },
    )?;
    std::fs::write(client.mnt.join("resurrected"), bytes)?;
    let output = child.wait_with_output()?;
    anyhow::ensure!(
        output.status.success(),
        "race GC failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    anyhow::ensure!(
        std::fs::read(client.mnt.join("resurrected"))? == bytes,
        "GC deleted content committed during condemned wait"
    );
    client.unmount()
}

/// `Client::gc_run` with a deadline: a GC round that has not finished by
/// then is killed and its output reported, rather than the scenario
/// hanging on it.
fn gc_run_bounded(client: &Client, deadline: Duration) -> Result<std::process::Output> {
    let mut child = client.gc_process()?;
    let start = std::time::Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if start.elapsed() > deadline {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            anyhow::bail!(
                "gc run did not finish within {deadline:?}; killed. stdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Object keys under `prefix` (one LIST page; the scenarios below list a
/// few dozen keys at most).
fn raw_keys(endpoint: &str, prefix: &str) -> Result<Vec<String>> {
    let body = crate::s3auth::get(&format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}"))
        .call()
        .with_context(|| format!("listing {prefix}"))?
        .into_string()?;
    let mut keys = Vec::new();
    let mut rest = body.as_str();
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        let Some(end) = after.find("</Key>") else {
            break;
        };
        keys.push(after[..end].to_string());
        rest = &after[end..];
    }
    Ok(keys)
}

/// The log segment sequence numbers under `prefix`, ascending.
fn log_segments(endpoint: &str, prefix: &str) -> Result<Vec<u64>> {
    let mut seqs: Vec<u64> = raw_keys(endpoint, &format!("{prefix}/log/p0/"))?
        .iter()
        .filter_map(|k| u64::from_str_radix(k.rsplit('/').next()?.strip_suffix(".zst")?, 16).ok())
        .collect();
    seqs.sort_unstable();
    Ok(seqs)
}

/// DESIGN.md §3 "Unlink while open", across nodes. B opens a file; A
/// unlinks it. Nothing in the bucket names the chunk any more, and its
/// age passes the (zeroed) horizon, so only B's `holds/<node>.json` keeps
/// a GC round from deleting it under B's open handle. Once B closes the
/// file the hold is withdrawn and the next round reclaims the chunk.
fn gc_open_orphan_hold(_seed: u64) -> Result<()> {
    let (env, root) = setup("gc-open-orphan-hold")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("gc-hold-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mk = |name: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, &env.endpoint, &backend)?
            .with_env("CONSTELLATION_LEASE_TTL_MS", "1000")
            .with_env("CONSTELLATION_GC_HORIZON_S", "0")
            .with_env("CONSTELLATION_HOLD_REFRESH_MS", "300")
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000"))
    };
    let mut a = mk("hold-a")?;
    let mut b = mk("hold-b")?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    let content = b"unlinked on a while open on b".to_vec();
    std::fs::write(a.mnt.join("held"), &content)?;
    eventually("B sees the file", Duration::from_secs(30), || {
        anyhow::ensure!(std::fs::read(b.mnt.join("held"))? == content);
        Ok(())
    })?;
    let holds_prefix = format!("{prefix}/holds/");
    anyhow::ensure!(
        raw_keys(&env.direct_endpoint, &holds_prefix)?.is_empty(),
        "no hold before anything is unlinked"
    );

    // B holds the file open; A unlinks it.
    let mut handle = std::fs::File::open(b.mnt.join("held"))?;
    std::fs::remove_file(a.mnt.join("held"))?;
    eventually("B publishes its hold", Duration::from_secs(30), || {
        let keys = raw_keys(&env.direct_endpoint, &holds_prefix)?;
        anyhow::ensure!(keys.len() == 1, "holds: {keys:?}");
        Ok(())
    })?;
    let output = a.gc_run()?;
    anyhow::ensure!(
        output.status.success(),
        "gc run failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    anyhow::ensure!(
        raw_exists(&env.direct_endpoint, &chunk_key(&prefix, &content)),
        "GC deleted a chunk B holds open: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let mut through_handle = Vec::new();
    {
        use std::io::{Read, Seek};
        handle.seek(std::io::SeekFrom::Start(0))?;
        handle.read_to_end(&mut through_handle)?;
    }
    anyhow::ensure!(
        through_handle == content,
        "B's handle reads the unlinked file"
    );
    anyhow::ensure!(
        std::fs::metadata(b.mnt.join("held")).is_err(),
        "the name is gone on B"
    );

    // B closes the file: the hold is withdrawn and the chunk reclaimed.
    drop(handle);
    eventually("B withdraws its hold", Duration::from_secs(30), || {
        let keys = raw_keys(&env.direct_endpoint, &holds_prefix)?;
        anyhow::ensure!(keys.is_empty(), "holds: {keys:?}");
        Ok(())
    })?;
    let output = a.gc_run()?;
    anyhow::ensure!(
        output.status.success(),
        "second gc run failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    anyhow::ensure!(
        !raw_exists(&env.direct_endpoint, &chunk_key(&prefix, &content)),
        "the closed orphan's chunk survived GC: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    b.unmount()?;
    a.unmount()
}

/// How the lagging node misses the log in [`log_retention_gap`].
#[derive(Clone, Copy)]
enum GapMode {
    /// B keeps running, frozen (SIGSTOP) while the log moves and is
    /// pruned; on resume its running tail must find the gap.
    Frozen,
    /// B is cleanly unmounted (its replica stays on disk) and remounted
    /// after the prune: the mount-time check must find the gap, and B is
    /// the lease taker right away.
    Restarted,
    /// `Frozen`, with a file B has open that A unlinked before the
    /// freeze: the rebuild must keep the orphan for B's handle (and B's
    /// hold must keep GC off its chunk) until B closes it.
    FrozenWithOpenOrphan,
}

fn log_retention_gap_follower(seed: u64) -> Result<()> {
    log_retention_gap(seed, GapMode::Frozen)
}

fn log_retention_gap_open_orphan(seed: u64) -> Result<()> {
    log_retention_gap(seed, GapMode::FrozenWithOpenOrphan)
}

fn log_retention_gap_taker(seed: u64) -> Result<()> {
    log_retention_gap(seed, GapMode::Restarted)
}

/// DESIGN.md §14 "Falling behind segment GC". A short retention window
/// (2 segments, no completion floor) and frequent commits let a GC round
/// prune the log past B's position while B is away. B must rebuild its
/// replica from the head commit and converge on the model, and — the
/// safety half — B's later lease takeover must never create a segment
/// in a pruned slot: every segment the log ends up with is above the
/// floor the prune left, and a fresh node C bootstraps to the same
/// state, which a forked log could not give.
fn log_retention_gap(seed: u64, mode: GapMode) -> Result<()> {
    let name = match mode {
        GapMode::Frozen => "log-retention-gap-follower",
        GapMode::Restarted => "log-retention-gap-taker",
        GapMode::FrozenWithOpenOrphan => "log-retention-gap-open-orphan",
    };
    let (env, root) = setup(name)?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("gap-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mk = |name: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, &env.endpoint, &backend)?
            .with_env("CONSTELLATION_LEASE_TTL_MS", "1000")
            .with_env("CONSTELLATION_LOG_RETENTION_SEGMENTS", "2")
            .with_env("CONSTELLATION_COMPLETION_RETENTION_S", "0")
            .with_env("CONSTELLATION_GC_HORIZON_S", "0")
            .with_env("CONSTELLATION_PUBLISH_IDLE_S", "1")
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")
            .with_env("CONSTELLATION_LOG_GAP_CHECK_MS", "2000")
            // The open-orphan mode freezes B for several seconds: its
            // hold must outlive the freeze (three refreshes = 15 s).
            .with_env("CONSTELLATION_HOLD_REFRESH_MS", "5000")
            // The S3 slow path only: the fork this guards against is an
            // S3 takeover's, and hints would find the gap sooner.
            .with_env("CONSTELLATION_P2P", "off"))
    };
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");
    let mut a = mk("gap-a")?;
    let mut b = mk("gap-b")?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wl.run_block(&a.mnt, &mut model, 20)?;
    eventually(
        "B converges before the gap",
        Duration::from_secs(60),
        || model.verify(&b.mnt),
    )?;
    // The open-orphan mode: B opens a file, A unlinks it, B applies the
    // unlink (its hold appears) — then B is frozen with the handle open.
    let orphan_bytes = b"unlinked on a, open on b across the rebuild".to_vec();
    let holds_prefix = format!("{prefix}/holds/");
    let mut orphan_handle = None;
    eprintln!("    {name}: step: converged before the gap");
    if matches!(mode, GapMode::FrozenWithOpenOrphan) {
        std::fs::write(a.mnt.join("orphan"), &orphan_bytes)?;
        eventually("B sees the file", Duration::from_secs(30), || {
            anyhow::ensure!(std::fs::read(b.mnt.join("orphan"))? == orphan_bytes);
            Ok(())
        })?;
        orphan_handle = Some(std::fs::File::open(b.mnt.join("orphan"))?);
        eprintln!("    {name}: step: orphan open on B");
        std::fs::remove_file(a.mnt.join("orphan"))?;
        eprintln!("    {name}: step: unlinked on A");
        eventually("B publishes its hold", Duration::from_secs(30), || {
            let keys = raw_keys(&env.direct_endpoint, &holds_prefix)?;
            anyhow::ensure!(keys.len() == 1, "holds: {keys:?}");
            Ok(())
        })?;
    }
    eprintln!("    {name}: step: hold published; B goes away");
    match mode {
        GapMode::Frozen | GapMode::FrozenWithOpenOrphan => b.pause()?,
        GapMode::Restarted => b.unmount()?,
    }

    // A ships a segment per close (paced past the shipper's round trip)
    // and commits every second, so the head commit's applied position
    // runs far past B's; GC then prunes everything below it but two.
    let before = log_segments(&env.direct_endpoint, &prefix)?;
    for i in 0..30 {
        let p = format!("gap-{i}");
        let bytes = format!("segment {i} while B is away").into_bytes();
        std::fs::write(a.mnt.join(&p), &bytes)?;
        model.write_file(std::path::Path::new(&p), bytes);
        std::thread::sleep(Duration::from_millis(120));
    }
    std::thread::sleep(Duration::from_secs(3));
    eprintln!("    {name}: step: files written; running gc");
    // While B is frozen, any early return must thaw it first: dropping
    // a paused client hangs in its unmount (the daemon cannot answer).
    let thaw = |b: &Client, e: anyhow::Error| -> anyhow::Error {
        if matches!(mode, GapMode::Frozen | GapMode::FrozenWithOpenOrphan) {
            let _ = b.resume();
        }
        e
    };
    // With B frozen and a FUSE fd of B's held by this process (the
    // open-orphan mode), GC must not be a forked process: see
    // `Client::gc_run_control`.
    if orphan_handle.is_some() {
        a.gc_run_control().map_err(|e| thaw(&b, e))?;
    } else {
        let output = gc_run_bounded(&a, Duration::from_secs(120)).map_err(|e| thaw(&b, e))?;
        if !output.status.success() {
            return Err(thaw(
                &b,
                anyhow::anyhow!(
                    "gc run failed: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
            ));
        }
    }
    eprintln!("    {name}: step: gc done");
    let pruned = log_segments(&env.direct_endpoint, &prefix).map_err(|e| thaw(&b, e))?;
    let floor = *pruned
        .first()
        .context("an empty log after the prune")
        .map_err(|e| thaw(&b, e))?;
    let b_position = *before
        .last()
        .context("no segment before the gap")
        .map_err(|e| thaw(&b, e))?;
    if floor <= b_position + 1 {
        return Err(thaw(
            &b,
            anyhow::anyhow!(
                "the prune did not pass B's position: B at {b_position}, log {pruned:?}"
            ),
        ));
    }
    eprintln!(
        "    {name}: B at {b_position}, log pruned to {}..={} ({} segments)",
        floor,
        pruned.last().unwrap(),
        pruned.len()
    );

    // B comes back and must converge (a rebuild from the head commit).
    match mode {
        GapMode::Frozen | GapMode::FrozenWithOpenOrphan => b.resume()?,
        GapMode::Restarted => {
            // A leaves first: B is the taker at its very first round.
            a.unmount()?;
            b.mount()?;
        }
    }
    eventually("B converges after the gap", Duration::from_secs(90), || {
        model.verify(&b.mnt)
    })
    .with_context(|| {
        format!(
            "--- B log ---
{}",
            b.tail_log_n(60)
        )
    })?;
    anyhow::ensure!(
        b.log_text().contains("rebuilding"),
        "B did not report a rebuild:
{}",
        b.tail_log_n(60)
    );
    if let Some(mut handle) = orphan_handle.take() {
        // The orphan came through the rebuild: the handle still reads the
        // right bytes (the name is gone), the hold still claims the
        // chunk against a GC round, and the close reclaims it.
        use std::io::{Read, Seek};
        let mut got = Vec::new();
        handle.seek(std::io::SeekFrom::Start(0))?;
        handle.read_to_end(&mut got)?;
        anyhow::ensure!(got == orphan_bytes, "B's handle after the rebuild");
        anyhow::ensure!(std::fs::metadata(b.mnt.join("orphan")).is_err());
        a.gc_run_control().context("gc run with the orphan open")?;
        anyhow::ensure!(
            raw_exists(&env.direct_endpoint, &chunk_key(&prefix, &orphan_bytes)),
            "GC deleted the chunk of an orphan held open across a rebuild"
        );
        got.clear();
        handle.seek(std::io::SeekFrom::Start(0))?;
        handle.read_to_end(&mut got)?;
        anyhow::ensure!(got == orphan_bytes, "B's handle after GC");
        drop(handle);
        eventually("B withdraws its hold", Duration::from_secs(30), || {
            let keys = raw_keys(&env.direct_endpoint, &holds_prefix)?;
            anyhow::ensure!(keys.is_empty(), "holds: {keys:?}");
            Ok(())
        })?;
        a.gc_run_control().context("gc run after the close")?;
        anyhow::ensure!(
            !raw_exists(&env.direct_endpoint, &chunk_key(&prefix, &orphan_bytes)),
            "the closed orphan's chunk survived GC"
        );
    }
    if matches!(mode, GapMode::Frozen | GapMode::FrozenWithOpenOrphan) {
        a.unmount()?;
    }

    // B writes: it takes the lease and appends only above the head.
    let proof = b"written by b after the gap".to_vec();
    std::fs::write(b.mnt.join("from-b"), &proof)?;
    model.write_file(std::path::Path::new("from-b"), proof);
    eventually("B's write ships", Duration::from_secs(60), || {
        let now = log_segments(&env.direct_endpoint, &prefix)?;
        anyhow::ensure!(now.last() > pruned.last(), "log unchanged: {now:?}");
        Ok(())
    })?;
    let after = log_segments(&env.direct_endpoint, &prefix)?;
    anyhow::ensure!(
        after.iter().all(|&s| s >= floor),
        "a segment appeared below the pruned floor {floor}: {after:?}"
    );
    anyhow::ensure!(
        after.first() == pruned.first() && after.len() > pruned.len(),
        "B's segments did not append above the head: before {pruned:?}, after {after:?}"
    );
    b.unmount()?;

    // A fresh node sees one history: the model's.
    let mut c = mk("gap-c")?;
    c.mount()?;
    model.verify(&c.mnt).with_context(|| {
        format!(
            "--- C log ---
{}",
            c.tail_log_n(40)
        )
    })?;
    c.unmount()
}

fn fsck_repair(_seed: u64) -> Result<()> {
    let (env, root) = setup("fsck-repair")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("fsck-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut client = Client::new(root.path(), "fsck", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_LEASE_TTL_MS", "200")
        .with_env("CONSTELLATION_GC_HORIZON_S", "0");
    client.fs_create()?;
    client.mount()?;
    let bytes = b"recover-from-local-cache";
    std::fs::write(client.mnt.join("recoverable"), bytes)?;
    client.unmount()?;

    let data_key = chunk_key(&prefix, bytes);
    crate::s3auth::delete(&raw_key(&env.direct_endpoint, &data_key)).call()?;
    let orphan_hash = blake3::hash(b"orphan-name").to_hex().to_string();
    let orphan_key = format!(
        "{prefix}/chunks/{}/{}/{}",
        &orphan_hash[..2],
        &orphan_hash[2..4],
        orphan_hash
    );
    crate::s3auth::put(&raw_key(&env.direct_endpoint, &orphan_key)).send_bytes(b"orphan-object")?;
    let torn = format!("{prefix}/log/p0/fffffffffffffffe.zst");
    crate::s3auth::put(&raw_key(&env.direct_endpoint, &torn)).send_bytes(b"torn")?;

    let detect = client.fsck(false)?;
    anyhow::ensure!(
        detect.status.code() == Some(1),
        "fsck detection exit was not 1: {:?}\n{}{}",
        detect.status.code(),
        String::from_utf8_lossy(&detect.stdout),
        String::from_utf8_lossy(&detect.stderr)
    );
    let repair = client.fsck(true)?;
    anyhow::ensure!(
        repair.status.code() == Some(2),
        "fsck repair exit was not 2: {:?}\n{}{}",
        repair.status.code(),
        String::from_utf8_lossy(&repair.stdout),
        String::from_utf8_lossy(&repair.stderr)
    );
    let clean = client.fsck(false)?;
    anyhow::ensure!(
        clean.status.code() == Some(0),
        "post-repair fsck not clean: {:?}\n{}{}",
        clean.status.code(),
        String::from_utf8_lossy(&clean.stdout),
        String::from_utf8_lossy(&clean.stderr)
    );
    client.mount()?;
    anyhow::ensure!(
        std::fs::read(client.mnt.join("recoverable"))? == bytes,
        "repaired file did not model-verify"
    );
    client.unmount()
}

/// Plan 29 M3a: `fjall` refuses a second process's open of the metadata
/// store while a mount daemon holds it, so `constellation fsck` must
/// route through the running daemon's control socket rather than fail
/// with a raw lock error — the same routing `constellation gc` already
/// has (M1). Runs `fsck` *while still mounted*, unlike `fsck-repair`
/// (which always unmounts first and so never exercises this path).
fn fsck_while_mounted(_seed: u64) -> Result<()> {
    let (env, root) = setup("fsck-while-mounted")?;
    let _proxy = env.s3_proxy()?;
    let mut client = one_client(&env, root.path(), &format!("fsck-live-{}", ts()))?;
    std::fs::write(
        client.mnt.join("still-mounted"),
        b"routed-through-the-daemon",
    )?;

    let report = client.fsck(false)?;
    anyhow::ensure!(
        report.status.code() == Some(0),
        "fsck while mounted did not come back clean (a lock error would exit non-zero \
         with no daemon-routed report): {:?}\n{}{}",
        report.status.code(),
        String::from_utf8_lossy(&report.stdout),
        String::from_utf8_lossy(&report.stderr)
    );
    // The report is real JSON from the daemon (not a locked-store
    // failure the CLI swallowed), and it reflects the daemon's own live
    // replica: the file just written is visible with no issues.
    let parsed: serde_json::Value = serde_json::from_slice(&report.stdout)?;
    anyhow::ensure!(
        parsed["clean"] == serde_json::Value::Bool(true),
        "expected a clean fsck report from the daemon: {parsed}"
    );

    client.unmount()
}

fn snapshot_lifecycle(_seed: u64) -> Result<()> {
    let (env, root) = setup("snapshot-lifecycle")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/snap-life-{}", ts());
    // Two nodes on one filesystem: the second phase snapshots from c1
    // while c0 holds the lease and writes. A long idle release keeps the
    // lease with c0 between its writes.
    let tune = |c: Client, key: &str| {
        c.with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "30000")
            .with_env("CONSTELLATION_NODE_KEY", key)
    };
    let mut client = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "snap-life-c0"),
    );
    client.fs_create()?;
    client.mount()?;
    std::fs::create_dir(client.mnt.join("project"))?;
    std::fs::write(client.mnt.join("project/data"), b"frozen")?;
    client.snapshot_create("/project@first")?;
    std::fs::write(client.mnt.join("project/data"), b"live-moved")?;

    let names: Vec<_> = std::fs::read_dir(client.mnt.join("project"))?
        .map(|entry| entry.unwrap().file_name())
        .collect();
    anyhow::ensure!(
        !names.iter().any(|name| name == ".constellation"),
        "synthetic control directory leaked into readdir"
    );
    let frozen = client
        .mnt
        .join("project/.constellation/snapshot/first/data");
    anyhow::ensure!(
        std::fs::read(&frozen)? == b"frozen",
        "snapshot was not frozen"
    );
    client.snapshot_delete("/project@first")?;
    std::thread::sleep(Duration::from_millis(1200));
    anyhow::ensure!(
        std::fs::read(&frozen).is_err(),
        "deleted snapshot still accepted new reads"
    );
    anyhow::ensure!(
        std::fs::read(client.mnt.join("project/data"))? == b"live-moved",
        "snapshot deletion changed the live tree"
    );

    // Plan 32 Step 0.1: a snapshot taken, held and deleted on a node that
    // does not hold the write lease runs at the holder; the lease stays
    // where it is (holder and epoch), while the holder keeps writing.
    let mut other = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "snap-life-c1"),
    );
    other.mount()?;
    wait_for_p2p(&[&client, &other])?;
    std::fs::write(client.mnt.join("project/counter"), b"0")?;
    eventually("c0 holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&client)?;
        anyhow::ensure!(lease["held"] == true, "c0 does not hold the lease: {lease}");
        Ok(())
    })?;
    let before = lease_of(&client)?;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (mnt, stop) = (client.mnt.clone(), stop.clone());
        std::thread::spawn(move || -> Result<u64> {
            let mut k = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                k += 1;
                // Write-then-rename, so every point in time holds a
                // number. An in-place rewrite's truncate is a state of its
                // own (an empty counter) that a snapshot may freeze; only
                // the old empty-journal barrier (fix snap-drain-busy) kept
                // snapshots between whole steps.
                std::fs::write(mnt.join("project/.next"), k.to_string())?;
                std::fs::rename(mnt.join("project/.next"), mnt.join("project/counter"))?;
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(k)
        })
    };
    let mut seen = Vec::new();
    let phase = (|| -> Result<()> {
        for i in 0..3 {
            std::thread::sleep(Duration::from_millis(200));
            other.snapshot_create(&format!("/project@busy{i}"))?;
        }
        other.snapshot_hold("/project@busy0", true)?;
        anyhow::ensure!(
            other.snapshot_delete("/project@busy0").is_err(),
            "a held snapshot was deleted"
        );
        other.snapshot_hold("/project@busy0", false)?;
        other.snapshot_delete("/project@busy0")?;
        Ok(())
    })();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let written = writer
        .join()
        .map_err(|_| anyhow::anyhow!("the writer panicked"))??;
    phase?;
    let after = lease_of(&client)?;
    eprintln!("    snapshot-lifecycle: lease before {before} after {after}, {written} writes");
    anyhow::ensure!(
        after["held"] == true
            && after["holder"] == before["holder"]
            && after["epoch"] == before["epoch"],
        "snapshots from a non-holder moved the lease: before {before}, after {after}"
    );
    anyhow::ensure!(lease_of(&other)?["held"] != true, "c1 took the lease");
    // Each surviving snapshot froze some point of the holder's counter,
    // in creation order; the deleted one is gone on both nodes.
    for i in 1..3 {
        let path = other
            .mnt
            .join(format!("project/.constellation/snapshot/busy{i}/counter"));
        let mut value = 0;
        eventually(
            &format!("busy{i} is visible on c1"),
            Duration::from_secs(20),
            || {
                value = std::fs::read_to_string(&path)?.trim().parse::<u64>()?;
                Ok(())
            },
        )?;
        seen.push(value);
    }
    anyhow::ensure!(
        seen.windows(2).all(|w| w[0] <= w[1]) && seen.iter().all(|v| *v <= written),
        "snapshot counters {seen:?} are not a prefix of the holder's {written} writes"
    );
    eventually("busy0 is gone on c0", Duration::from_secs(20), || {
        anyhow::ensure!(
            !client
                .mnt
                .join("project/.constellation/snapshot/busy0")
                .exists(),
            "busy0 still listed"
        );
        Ok(())
    })?;
    snapshot_selectors_from_the_cli(&client, &other)?;
    // The CLI phase's forwarded multi-deletes and holds never moved the
    // lease either.
    let last = lease_of(&client)?;
    eprintln!("    snapshot-lifecycle: lease after the CLI phase {last}");
    anyhow::ensure!(
        last["held"] == true
            && last["holder"] == before["holder"]
            && last["epoch"] == before["epoch"],
        "the CLI's snapshot selectors from a non-holder moved the lease: \
         before {before}, after {last}"
    );
    anyhow::ensure!(
        lease_of(&other)?["held"] != true,
        "c1 took the lease in the CLI phase"
    );
    other.unmount()?;
    client.unmount()
}

/// Plan 32 Step 5 through the real CLI, from `other` (not the lease
/// holder, so every delete and hold runs at `holder` as one batch): the
/// table, a range across a chain with another directory's snapshot taken
/// in the middle, the dry run, the prompt, per-item hold refusals, globs
/// and multi-target hold/release. Selectors resolve against the asking
/// node's replica, so each step first waits for `other` to see the state
/// the previous one made.
fn snapshot_selectors_from_the_cli(holder: &Client, other: &Client) -> Result<()> {
    let named = |rows: &[serde_json::Value], name: &str| {
        rows.iter()
            .find(|r| {
                format!(
                    "{}@{}",
                    r["path"].as_str().unwrap_or(""),
                    r["name"].as_str().unwrap_or("")
                ) == name
            })
            .cloned()
    };
    let wait_for = |what: &str, check: &dyn Fn(&[serde_json::Value]) -> bool| {
        eventually(what, Duration::from_secs(30), || {
            let rows = other.snapshot_rows()?;
            anyhow::ensure!(check(&rows), "c1 lists {rows:?}");
            Ok(())
        })
    };
    std::fs::create_dir(holder.mnt.join("side"))?;
    holder.snapshot_create("/project@r1")?;
    holder.snapshot_create("/side@mid")?;
    holder.snapshot_create("/project@r2")?;
    holder.snapshot_create("/project@r3")?;
    wait_for("c1 sees r1..r3 and side@mid", &|rows| {
        ["/project@r1", "/project@r2", "/project@r3", "/side@mid"]
            .iter()
            .all(|n| named(rows, n).is_some())
    })?;

    let (ok, out, err) =
        other.snapshot_cli(&["hold", "/project@r2", "/side@mid", "--by", "user:harness"])?;
    anyhow::ensure!(ok, "multi-target hold failed: {out}{err}");
    wait_for("c1 sees the holds", &|rows| {
        ["/project@r2", "/side@mid"]
            .iter()
            .all(|n| named(rows, n).is_some_and(|r| r["held"] == true))
    })?;
    let (ok, table, err) = other.snapshot_cli(&["ls", "/"])?;
    anyhow::ensure!(ok, "snapshot ls failed: {table}{err}");
    let header = table.lines().next().unwrap_or("");
    for column in [
        "NAME",
        "CREATED (UTC)",
        "ORIGIN",
        "USED",
        "WRITTEN",
        "REFER",
        "KEPT BY",
        "EXPIRES",
    ] {
        anyhow::ensure!(header.contains(column), "no {column} column in:\n{table}");
    }
    let r2 = table
        .lines()
        .find(|l| l.starts_with("/project@r2 "))
        .unwrap_or("");
    anyhow::ensure!(
        r2.contains('⚑') && r2.contains("held: user") && r2.trim_end().ends_with("never"),
        "the held row reads wrong:\n{table}"
    );

    // The range is /project's chain only: /side@mid, taken between r1
    // and r2, is not in it. r2 is held.
    let (ok, out, err) = other.snapshot_cli(&["delete", "/project@r1%r3", "--dry-run"])?;
    anyhow::ensure!(ok, "dry run failed: {out}{err}");
    anyhow::ensure!(
        out.contains("would delete /project@r1")
            && out.contains("would refuse: snapshot /project@r2 is held by user:harness")
            && out.contains("would delete /project@r3")
            && !out.contains("/side@mid")
            // Plus plan 32 M5c's estimate of what the two deletable ones
            // give back (from the asking node's accounting index; its
            // numbers are the smoke test's and the unit tests').
            && out.lines().count() == 4
            && out
                .lines()
                .last()
                .is_some_and(|l| l.starts_with("would reclaim")),
        "dry run listed:\n{out}"
    );
    // More than one: it asks, and stdin's EOF declines.
    let (ok, out, err) = other.snapshot_cli(&["delete", "/project@r1%r3"])?;
    anyhow::ensure!(
        !ok && err.contains("delete 3 snapshots? [y/N]"),
        "an unconfirmed multi-delete went ahead: {out}{err}"
    );
    // The prompt shows the dry run's estimate line.
    anyhow::ensure!(
        err.contains("would reclaim"),
        "the confirmation shows no reclaim estimate: {err}"
    );
    anyhow::ensure!(
        named(&other.snapshot_rows()?, "/project@r1").is_some(),
        "declined, yet r1 is gone"
    );
    let (ok, out, err) = other.snapshot_cli(&["delete", "/project@r1%r3", "--yes"])?;
    anyhow::ensure!(
        !ok && out.contains("deleted snapshot /project@r1")
            && out.contains("deleted snapshot /project@r3")
            && err.contains("/project@r2 is held by user:harness; `snapshot release` first"),
        "the held one must be refused and the rest deleted: {out}{err}"
    );
    wait_for("c1 sees r1 and r3 deleted", &|rows| {
        named(rows, "/project@r1").is_none() && named(rows, "/project@r3").is_none()
    })?;

    let (ok, out, err) =
        other.snapshot_cli(&["release", "/project@r2", "/side@*", "--by", "user:harness"])?;
    anyhow::ensure!(ok, "multi-target release failed: {out}{err}");
    wait_for("c1 sees the releases", &|rows| {
        rows.iter().all(|r| r["held"] != true)
    })?;
    let (ok, out, err) = other.snapshot_cli(&[
        "delete",
        "/project@busy*",
        "/project@r2",
        "/side@mid",
        "--yes",
    ])?;
    anyhow::ensure!(ok, "glob delete failed: {out}{err}");
    anyhow::ensure!(
        out.lines().count() == 4,
        "busy1, busy2, r2, side@mid: {out}"
    );
    wait_for("c1 lists nothing", &|rows| rows.is_empty())?;
    eventually("c0 lists nothing", Duration::from_secs(30), || {
        anyhow::ensure!(holder.snapshot_count()? == 0, "c0 still lists snapshots");
        Ok(())
    })
}

fn clone_workflow(_seed: u64) -> Result<()> {
    let (env, root) = setup("clone-workflow")?;
    let _proxy = env.s3_proxy()?;
    let mut client = one_client(&env, root.path(), &format!("clone-{}", ts()))?;
    std::fs::create_dir(client.mnt.join("origin"))?;
    std::fs::write(client.mnt.join("origin/data"), b"base")?;
    client.snapshot_create("/origin@base")?;
    client.clone_snapshot("/origin@base", "/clone")?;
    std::fs::write(client.mnt.join("origin/data"), b"origin-new")?;
    std::fs::write(client.mnt.join("clone/data"), b"clone-new")?;
    anyhow::ensure!(
        std::fs::read(client.mnt.join("origin/data"))? == b"origin-new",
        "origin did not diverge"
    );
    anyhow::ensure!(
        std::fs::read(client.mnt.join("clone/data"))? == b"clone-new",
        "clone did not diverge"
    );
    anyhow::ensure!(
        std::fs::read(client.mnt.join("origin/.constellation/snapshot/base/data"))? == b"base",
        "clone write touched the snapshot"
    );
    client.snapshot_delete("/origin@base")?;
    anyhow::ensure!(
        std::fs::read(client.mnt.join("clone/data"))? == b"clone-new",
        "deleting source snapshot broke clone"
    );
    client.unmount()
}

fn snapshot_mount(_seed: u64) -> Result<()> {
    let (env, root) = setup("snapshot-mount")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/snapshot-mount-{}", ts());
    let mut source = Client::new(root.path(), "source", &env.endpoint, &backend)?;
    source.fs_create()?;
    source.mount()?;
    std::fs::create_dir(source.mnt.join("project"))?;
    std::fs::write(source.mnt.join("project/data"), b"mounted-snapshot")?;
    source.snapshot_create("/project@release")?;
    std::thread::sleep(Duration::from_secs(1));

    let mut frozen = Client::new(root.path(), "frozen", &env.endpoint, &backend)?;
    frozen.mount_view(Some("/project@release"), &[])?;
    anyhow::ensure!(
        std::fs::read(frozen.mnt.join("data"))? == b"mounted-snapshot",
        "snapshot mount returned wrong content"
    );
    let error = std::fs::write(frozen.mnt.join("data"), b"no").unwrap_err();
    anyhow::ensure!(
        Code::from_os_error(&error) == Some(Code::ReadOnly),
        "snapshot mutation returned {error}, expected EROFS"
    );
    frozen.unmount()?;

    let mut writable = Client::new(root.path(), "writable", &env.endpoint, &backend)?;
    writable.mount_view(Some("/project@release"), &["--rw", "--ephemeral"])?;
    std::fs::write(writable.mnt.join("data"), b"branch")?;
    writable.unmount()?;
    eventually("ephemeral clone removed", Duration::from_secs(20), || {
        let leaked = std::fs::read_dir(&source.mnt)?
            .filter_map(Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".constellation-ephemeral-")
            });
        anyhow::ensure!(!leaked, "ephemeral clone remains visible");
        Ok(())
    })?;

    // Plan 32 §0.5: a renamed directory keeps its history (the snapshot
    // covers the directory's inode, not only the path it was taken at)…
    // Listed first, so the `.constellation` nodes the mount keeps were
    // made before the rename and must follow it.
    let listing = |dir: &str| -> Result<Vec<String>> {
        let mut names = std::fs::read_dir(source.mnt.join(dir).join(".constellation/snapshot"))?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    };
    anyhow::ensure!(
        listing("project")? == ["release"],
        "before the rename: {:?}",
        listing("project")?
    );
    std::fs::rename(source.mnt.join("project"), source.mnt.join("renamed"))?;
    eventually(
        "renamed directory lists its snapshot",
        Duration::from_secs(20),
        || {
            let data = std::fs::read(
                source
                    .mnt
                    .join("renamed/.constellation/snapshot/release/data"),
            )?;
            anyhow::ensure!(data == b"mounted-snapshot", "wrong frozen content");
            Ok(())
        },
    )?;
    // A snapshot under the new name joins the history; one of a new
    // directory at the old path does not.
    source.snapshot_create("/renamed@after")?;
    eventually(
        "renamed directory lists a snapshot taken after the rename",
        Duration::from_secs(20),
        || {
            let names = listing("renamed")?;
            anyhow::ensure!(names == ["after", "release"], "{names:?}");
            Ok(())
        },
    )?;
    std::fs::create_dir(source.mnt.join("project"))?;
    source.snapshot_create("/project@newp")?;
    eventually(
        "a new directory at the old path lists its own snapshot",
        Duration::from_secs(20),
        || {
            let names = listing("project")?;
            anyhow::ensure!(names == ["newp", "release"], "{names:?}");
            Ok(())
        },
    )?;
    let names = listing("renamed")?;
    anyhow::ensure!(
        names == ["after", "release"],
        "the renamed directory lists another directory's snapshot: {names:?}"
    );
    // …and a replaced one keeps the path rule: the old directory's
    // snapshot still shows, frozen, under the new directory at its path.
    std::fs::create_dir(source.mnt.join("replaced"))?;
    std::fs::write(source.mnt.join("replaced/old"), b"before-replace")?;
    source.snapshot_create("/replaced@old")?;
    std::fs::remove_dir_all(source.mnt.join("replaced"))?;
    std::fs::create_dir(source.mnt.join("replaced"))?;
    eventually(
        "replaced directory lists its predecessor's snapshot",
        Duration::from_secs(20),
        || {
            let data = std::fs::read(source.mnt.join("replaced/.constellation/snapshot/old/old"))?;
            anyhow::ensure!(data == b"before-replace", "wrong frozen content");
            Ok(())
        },
    )?;
    source.unmount()
}

/// Two clients mount the SAME filesystem. Each works in its own
/// subtree; after every block, each node must observe the other's
/// subtree exactly (model-verified through the foreign mount).
fn two_clients_shared(seed: u64) -> Result<()> {
    let (env, root) = setup("two-shared")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/shared-{}", ts());
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?; // fresh state dir: bootstraps the same namespace

    std::fs::create_dir(c0.mnt.join("a"))?;
    std::fs::create_dir(c1.mnt.join("b"))?;
    eventually("subtrees visible on both", Duration::from_secs(20), || {
        anyhow::ensure!(c0.mnt.join("b").is_dir(), "b not on c0");
        anyhow::ensure!(c1.mnt.join("a").is_dir(), "a not on c1");
        Ok(())
    })?;

    let mut m0 = Model::default();
    let mut m1 = Model::default();
    let mut w0 = Workload::new(seed, "a");
    let mut w1 = Workload::new(seed.wrapping_add(1), "b");
    for block in 0..3 {
        w0.run_block(&c0.mnt.join("a"), &mut m0, 30)?;
        w1.run_block(&c1.mnt.join("b"), &mut m1, 30)?;
        // Local view is immediate.
        m0.verify(&c0.mnt.join("a"))
            .with_context(|| format!("c0 local, block {block}"))?;
        m1.verify(&c1.mnt.join("b"))
            .with_context(|| format!("c1 local, block {block}"))?;
        // Remote view converges (close-to-open through S3 alone).
        eventually(
            &format!("cross-node convergence, block {block}"),
            Duration::from_secs(30),
            || {
                m0.verify(&c1.mnt.join("a")).context("c0's tree via c1")?;
                m1.verify(&c0.mnt.join("b")).context("c1's tree via c0")?;
                Ok(())
            },
        )?;
    }

    let status = c0.control_status()?;
    anyhow::ensure!(
        status["spool"]["conflicts"].as_u64() == Some(0),
        "disjoint subtrees must produce zero conflicts: {status}"
    );
    let status1 = c1.control_status()?;
    anyhow::ensure!(
        status1["spool"]["conflicts"].as_u64() == Some(0),
        "disjoint subtrees must produce zero conflicts: {status1}"
    );
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Read-time atime (plan 20). Two nodes share a filesystem with
/// `--atime relatime` and a zero granularity so any cold read bumps.
/// A read on the reader must eventually become visible on the writer's
/// node, and — with S3 cut — reads must keep succeeding while the atime
/// updates are simply lost (never blocking a read).
fn atime_eventual(seed: u64) -> Result<()> {
    let _ = seed;
    let (env, root) = setup("atime-eventual")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/atime-{}", ts());
    // relatime + granularity 0 so any cold read bumps; fast flush and a
    // short ship-max-delay so a bump reaches S3 without waiting.
    let atime_env = |c: Client| {
        c.with_env("CONSTELLATION_ATIME", "relatime")
            .with_env("CONSTELLATION_ATIME_GRANULARITY_S", "0")
            .with_env("CONSTELLATION_ATIME_FLUSH_MS", "500")
    };
    // Distinct P2P identities: this scenario's whole premise is the
    // reader (a non-holder) *forwarding* its atime bump to the holder,
    // which needs a real dial between the two nodes. Without its own key
    // a client falls back to the harness's shared default node key path,
    // so both clients register the same iroh identity; `refresh_registry`
    // then refuses to dial "itself" and P2P silently degrades to the
    // S3-only slow path for this pair (see its "peer registered with OUR
    // node key" warning) — no mechanism ever ships a non-holder's
    // atime-only bump over the S3 path, so the holder would never see it
    // no matter how long the scenario waited.
    let mut writer =
        atime_env(Client::new(root.path(), "w", &env.endpoint, &backend)?.with_own_node_key());
    let mut reader =
        atime_env(Client::new(root.path(), "r", &env.endpoint, &backend)?.with_own_node_key());
    writer.fs_create()?;
    writer.mount()?;
    reader.mount()?;

    // The writer (partition holder) creates a file; the reader must see it.
    std::fs::write(writer.mnt.join("shared.txt"), b"hello atime")?;
    eventually(
        "shared file visible on reader",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(reader.mnt.join("shared.txt").is_file(), "not yet on reader");
            Ok(())
        },
    )?;

    // Cold read on the reader (it has never opened this file, so the read
    // reaches FUSE rather than the page cache) → a bump forwarded to the
    // holder. Force the holder to ship by writing a sibling so the bump
    // rides along a segment.
    let baseline = std::fs::metadata(reader.mnt.join("shared.txt"))?
        .accessed()
        .ok();
    std::thread::sleep(Duration::from_secs(1));
    let _ = std::fs::read(reader.mnt.join("shared.txt")).context("cold read on reader")?;
    std::fs::write(writer.mnt.join("marker.txt"), b"x")?;

    // The writer's replica (authoritative) must observe atime advance.
    // Compared there rather than via the reader's kernel attr cache.
    eventually(
        "atime advanced on the holder",
        Duration::from_secs(40),
        || {
            let now = std::fs::metadata(writer.mnt.join("shared.txt"))?.accessed()?;
            if let Some(b) = baseline {
                anyhow::ensure!(now > b, "atime not advanced yet");
            }
            Ok(())
        },
    )?;

    // The reader's forward path must not have produced namespace conflicts.
    let s = reader.control_status()?;
    anyhow::ensure!(
        s["spool"]["conflicts"].as_u64() == Some(0),
        "atime must not cause conflicts: {s}"
    );

    // S3 cut: cached reads keep succeeding at full speed; atime updates
    // are simply lost, never blocking or failing a read.
    proxy.cut()?;
    for _ in 0..25 {
        let _ = std::fs::read(reader.mnt.join("shared.txt"))
            .context("read during S3 cut must still succeed")?;
    }
    proxy.heal()?;

    reader.unmount()?;
    writer.unmount()?;
    Ok(())
}

/// Retention pruning (plan 22). Two nodes share a filesystem; an armed
/// `age` policy on a marked directory removes a stale file, and the
/// second node's namespace converges. A dry-run pass first proves it
/// deletes nothing.
fn prune_retention(_seed: u64) -> Result<()> {
    let (env, root) = setup("prune")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/prune-{}", ts());
    // Grace 0 so a just-installed policy acts immediately; a large
    // interval so only the on-demand run fires; a generous lag ceiling.
    let prune_env = |c: Client| {
        c.with_env("CONSTELLATION_PRUNE_GRACE_S", "0")
            .with_env("CONSTELLATION_PRUNE_INTERVAL_S", "100000")
            .with_env("CONSTELLATION_PRUNE_MAX_LAG_S", "3600")
    };
    let mut a = prune_env(Client::new(root.path(), "pa", &env.endpoint, &backend)?);
    let mut b = prune_env(Client::new(root.path(), "pb", &env.endpoint, &backend)?);
    a.fs_create()?;
    a.mount()?;
    b.mount()?;

    // A marked directory with two files: one stale, one fresh.
    std::fs::create_dir(a.mnt.join("data"))?;
    std::fs::write(a.mnt.join("data/old.log"), b"stale")?;
    std::fs::write(a.mnt.join("data/fresh.log"), b"fresh")?;
    // Backdate old.log's mtime 60 days so age(30d) selects it.
    let status = std::process::Command::new("touch")
        .args(["-d", "60 days ago"])
        .arg(a.mnt.join("data/old.log"))
        .status()?;
    anyhow::ensure!(status.success(), "touch -d failed");

    // Install an armed age policy on the directory.
    set_xattr(
        &a.mnt.join("data"),
        "user.constellation.prune",
        b"age(30d); !",
    )?;

    // The reader must first see the directory and both files.
    eventually("files visible on b", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("data/old.log").is_file(), "old.log not yet on b");
        anyhow::ensure!(
            b.mnt.join("data/fresh.log").is_file(),
            "fresh.log not yet on b"
        );
        Ok(())
    })?;

    // A dry-run pass deletes nothing.
    a.prune_run(true)?;
    anyhow::ensure!(
        a.mnt.join("data/old.log").is_file(),
        "dry-run must not delete old.log"
    );

    // The armed run removes the stale file, keeps the fresh one. The
    // unlink lands in the replica immediately; the local kernel dentry
    // cache clears within the FUSE entry TTL (~1s), so poll briefly.
    a.prune_run(false)?;
    eventually("old.log gone on a", Duration::from_secs(10), || {
        anyhow::ensure!(
            !a.mnt.join("data/old.log").exists(),
            "old.log still cached on a"
        );
        Ok(())
    })?;
    anyhow::ensure!(
        a.mnt.join("data/fresh.log").is_file(),
        "fresh.log must survive on a"
    );

    // The second node converges on the same namespace.
    eventually("b converges after prune", Duration::from_secs(30), || {
        anyhow::ensure!(
            !b.mnt.join("data/old.log").exists(),
            "old.log still visible on b"
        );
        anyhow::ensure!(
            b.mnt.join("data/fresh.log").is_file(),
            "fresh.log must remain on b"
        );
        Ok(())
    })?;

    // The prune counters reflect exactly one deletion.
    let s = a.control_status()?;
    anyhow::ensure!(
        s["prune"]["deleted"].as_u64().unwrap_or(0) >= 1,
        "prune deleted counter should be >= 1: {s}"
    );

    b.unmount()?;
    a.unmount()?;
    Ok(())
}

fn quota_enforcement(_seed: u64) -> Result<()> {
    let (env, root) = setup("quota")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/quota-{}", ts());
    let mut a = Client::new(root.path(), "qa", &env.endpoint, &backend)?;
    let mut b = Client::new(root.path(), "qb", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;

    // Tiny cap so a single write overshoots.
    a.set_quota(Some(64 * 1024))?;
    let (max, _) = a.get_quota()?;
    anyhow::ensure!(
        max == Some(64 * 1024),
        "quota not visible on setter: {max:?}"
    );

    eventually("quota replicates to peer", Duration::from_secs(30), || {
        let (max, _) = b.get_quota()?;
        anyhow::ensure!(max == Some(64 * 1024), "peer still sees {max:?}");
        Ok(())
    })?;

    let path = a.mnt.join("big.bin");
    let big = vec![b'Q'; 128 * 1024];
    let err = std::fs::write(&path, &big).expect_err("write past quota must fail");
    anyhow::ensure!(
        Code::from_os_error(&err) == Some(Code::NoSpace),
        "expected ENOSPC, got {err}"
    );

    a.set_quota(None)?;
    eventually("cleared quota replicates", Duration::from_secs(30), || {
        let (max, _) = b.get_quota()?;
        anyhow::ensure!(max.is_none(), "peer still capped: {max:?}");
        Ok(())
    })?;
    std::fs::write(&path, &big).context("write after clearing quota")?;
    a.unmount()?;
    b.unmount()?;
    Ok(())
}

/// The git-style workflow from the roadmap: node A stages a tree and
/// publishes it with an atomic rename; node B consumes it, edits, and
/// publishes back. Ping-pong with exact content verification.
fn git_workflow(_seed: u64) -> Result<()> {
    let (env, root) = setup("git-workflow")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/gitwf-{}", ts());
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;

    // A: stage, then publish atomically.
    let stage = c0.mnt.join("stage");
    std::fs::create_dir_all(stage.join("src"))?;
    std::fs::write(stage.join("src/main.c"), b"int main(){return 0;}\n")?;
    std::fs::write(stage.join("README.md"), b"# demo v1\n")?;
    std::fs::rename(&stage, c0.mnt.join("repo"))?;

    // B: sees the published tree, exactly.
    eventually("repo published on B", Duration::from_secs(20), || {
        let repo = c1.mnt.join("repo");
        anyhow::ensure!(!c1.mnt.join("stage").exists(), "stage leaked");
        let readme = std::fs::read(repo.join("README.md")).context("README")?;
        anyhow::ensure!(readme == b"# demo v1\n", "README content");
        let main = std::fs::read(repo.join("src/main.c")).context("main.c")?;
        anyhow::ensure!(main == b"int main(){return 0;}\n", "main.c content");
        Ok(())
    })?;

    // B: edit, restructure, publish back.
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(c1.mnt.join("repo/README.md"))?;
        f.write_all(b"edited on B\n")?;
    }
    std::fs::write(c1.mnt.join("repo/BUILD"), b"cc src/*.c\n")?;
    std::fs::remove_file(c1.mnt.join("repo/src/main.c"))?;
    std::fs::rename(c1.mnt.join("repo/src"), c1.mnt.join("repo/lib"))?;

    // A: sees B's edits, exactly.
    eventually("B's edits visible on A", Duration::from_secs(20), || {
        let repo = c0.mnt.join("repo");
        let readme = std::fs::read(repo.join("README.md")).context("README")?;
        anyhow::ensure!(readme == b"# demo v1\nedited on B\n", "README round 2");
        anyhow::ensure!(
            std::fs::read(repo.join("BUILD"))? == b"cc src/*.c\n",
            "BUILD content"
        );
        anyhow::ensure!(!repo.join("src").exists(), "src should be renamed");
        anyhow::ensure!(repo.join("lib").is_dir(), "lib missing");
        anyhow::ensure!(!repo.join("lib/main.c").exists(), "main.c deleted on B");
        Ok(())
    })?;

    // A: final round-trip (overwrite shrinks the file).
    std::fs::write(c0.mnt.join("repo/README.md"), b"v3\n")?;
    eventually("round 3 on B", Duration::from_secs(20), || {
        let readme = std::fs::read(c1.mnt.join("repo/README.md")).context("README")?;
        anyhow::ensure!(readme == b"v3\n", "README round 3: {readme:?}");
        Ok(())
    })?;

    // Leases serialize the ping-pong, so the leaseless conflict path
    // must never have fired on either node.
    for c in [&c0, &c1] {
        anyhow::ensure!(
            conflicts_of(c)? == 0,
            "lease-serialized ping-pong must produce zero conflicts on {}: {}",
            c.name,
            c.control_status()?["spool"]
        );
    }

    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Push invalidation (DESIGN.md §12): with peers connected, a write on A
/// reaches B from a gossip hint rather than B's next poll.
///
/// The sync interval is set deliberately long (3 s) so polling cannot
/// explain a fast result: anything well under that must have come from a
/// push. The same workload is then re-run with `CONSTELLATION_P2P=off`,
/// which must fall back to the poll bound — proving the fast path is an
/// accelerator and not a correctness dependency.
fn p2p_invalidation(_seed: u64) -> Result<()> {
    let interval_ms = 3_000u64;
    let slow = |c: Client| {
        c.with_env("CONSTELLATION_SYNC_INTERVAL_MS", &interval_ms.to_string())
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "200")
    };

    // --- P2P on ---
    let (env, root) = setup("p2p-invalidation")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/p2pinval-{}", ts());
    // Distinct host keys: the node key is per host, and both "hosts"
    // here share one machine.
    let mut c0 = slow(Client::new(root.path(), "c0", &env.endpoint, &backend)?).with_env(
        "CONSTELLATION_NODE_KEY",
        &scenario_key(root.path(), "harness-c0"),
    );
    let mut c1 = slow(Client::new(root.path(), "c1", &env.endpoint, &backend)?).with_env(
        "CONSTELLATION_NODE_KEY",
        &scenario_key(root.path(), "harness-c1"),
    );
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;

    let mut pushed = Vec::new();
    for i in 0..5 {
        pushed.push(visibility_ms(&c0, &c1, &format!("push-{i}"))?);
    }
    pushed.sort_unstable();
    let push_median = pushed[pushed.len() / 2];
    eprintln!("    p2p-invalidation: push median {push_median} ms (samples {pushed:?})");
    c0.unmount()?;
    c1.unmount()?;
    drop(root);
    // The first S3Env (with its docker-prefix lock) must go too: the
    // second `setup()` below acquires the same prefix lock and would
    // otherwise always fail with "another harness run is already using
    // the docker prefix", not a flake but a guaranteed self-conflict.
    drop(env);

    // --- P2P off: the old bound must still hold ---
    let (env2, root2) = setup("p2p-invalidation-off")?;
    let _proxy2 = env2.s3_proxy()?;
    let backend2 = format!("s3://{BUCKET}/p2poff-{}", ts());
    let off = |c: Client| slow(c).with_env("CONSTELLATION_P2P", "off");
    let mut d0 = off(Client::new(root2.path(), "d0", &env2.endpoint, &backend2)?);
    let mut d1 = off(Client::new(root2.path(), "d1", &env2.endpoint, &backend2)?);
    d0.fs_create()?;
    d0.mount()?;
    d1.mount()?;
    anyhow::ensure!(
        p2p_of(&d0)?["enabled"] == false,
        "CONSTELLATION_P2P=off must disable the fast path: {}",
        p2p_of(&d0)?
    );
    let mut polled = Vec::new();
    for i in 0..3 {
        polled.push(visibility_ms(&d0, &d1, &format!("poll-{i}"))?);
    }
    polled.sort_unstable();
    let poll_median = polled[polled.len() / 2];
    eprintln!("    p2p-invalidation: poll median {poll_median} ms (samples {polled:?})");
    d0.unmount()?;
    d1.unmount()?;

    // The push path must be decisively faster than the poll bound.
    anyhow::ensure!(
        push_median * 2 < poll_median,
        "push invalidation must beat the poll bound by a wide margin, \
         got push {push_median} ms vs poll {poll_median} ms"
    );
    anyhow::ensure!(
        push_median < interval_ms as u128,
        "push median {push_median} ms is not below the {interval_ms} ms sync interval, \
         so the speedup cannot be attributed to gossip"
    );
    Ok(())
}
/// Lease state from a node's control API.
fn lease_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["lease"].clone())
}

/// Wait until `c` holds the lease a write just before took. A scenario
/// that cuts S3 and then writes on a node must have that node hold the
/// lease first: none can be had without S3, and the write would wait out
/// its 120 s client deadline and fail `EIO`. Unprivileged, the first
/// mount's root-owner adoption (`engine::node::adopt_root`, a `Setattr`
/// through the lease path) took it as a side effect; as root that
/// adoption is skipped, so only an explicit write before the cut does.
fn hold_lease_before_cut(c: &Client) -> Result<()> {
    eventually(
        &format!("{} holds the lease before the cut", c.name),
        Duration::from_secs(30),
        || {
            let lease = lease_of(c)?;
            anyhow::ensure!(lease["held"] == true, "{lease}");
            Ok(())
        },
    )
}

/// P2P state from a node's control API.
fn p2p_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["p2p"].clone())
}

/// Wait until both nodes report a live fast path with a peer enrolled
/// from the registry, so a latency measurement is not just racing
/// startup.
/// Wait until every node reports a live fast path with the other
/// participants enrolled from the registry.
fn wait_for_peers(clients: &[&Client]) -> Result<()> {
    let need = clients.len().saturating_sub(1);
    for c in clients {
        eventually(
            &format!("{} reports {need} live P2P peer(s)", c.name),
            Duration::from_secs(30),
            || {
                let p = p2p_of(c)?;
                anyhow::ensure!(p["enabled"] == true, "{} has no fast path: {p}", c.name);
                let n = node_peers(&p).count();
                anyhow::ensure!(n >= need, "{} sees {n} peers, want {need}: {p}", c.name);
                Ok(())
            },
        )?;
    }
    Ok(())
}

/// The other *nodes* in a `status` `p2p` object's `peers`: every entry
/// but the S3 pseudo-peer, which `peers` lists first for the operator
/// table (26586cd). A node listed here was read from the registry, so it
/// is on this node's accept allowlist too.
///
/// Counting S3 as a peer made [`wait_for_peers`] one short: with three
/// nodes, the holder "saw two peers" — S3 and B — before its registry
/// read had admitted C, and a scenario that cut the holder's S3 path
/// next left it unable ever to admit C: C's forwards were rejected, went
/// to the S3 inbox, and C took the lease over itself
/// (`session-ryw-after-holder-kill`, `takeover-marker-strands-promptly`).
pub(crate) fn node_peers(p2p: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    p2p["peers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|peer| peer["s3"] != true)
}

/// As [`wait_for_peers`], for the common 2-node call shape; also usable
/// directly with a slice for 3+ nodes (plan 30 M0).
fn wait_for_p2p(clients: &[&Client]) -> Result<()> {
    wait_for_peers(clients)
}

/// A node key path private to this scenario run: `<root>/.<tag>.key`,
/// handed to the daemon as `CONSTELLATION_NODE_KEY` (the daemon creates
/// the key on first mount and reuses it on later mounts, so a node keeps
/// its identity across restarts within the scenario).
///
/// The scenarios used fixed `/tmp/.constellation-*.key` paths before,
/// shared by every run and every user on the host. With
/// `fs.protected_regular=1` in the sticky `/tmp`, a key left behind by a
/// root run could not be opened by an unprivileged run (nor the reverse),
/// so the daemon came up without a fast path ("p2p.enabled: false"); and
/// two concurrent runs gave their nodes the same identity. The scenario's
/// own tempdir is per run and owned by whoever runs it, and starts empty,
/// so no stale key needs deleting first.
fn scenario_key(root: &std::path::Path, tag: &str) -> String {
    root.join(format!(".{tag}.key")).display().to_string()
}

fn coop_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["coop"].clone())
}

fn coop_client(
    root: &std::path::Path,
    name: &str,
    endpoint: &str,
    backend: &str,
) -> Result<Client> {
    let key = scenario_key(root, &format!("coop-{name}"));
    Ok(Client::new(root, name, endpoint, backend)?
        .with_env("CONSTELLATION_NODE_KEY", &key)
        .with_env("CONSTELLATION_DIGEST_INTERVAL_S", "1"))
}

fn blob(n_chunks: usize) -> (Vec<u8>, blake3::Hash) {
    let mut data = vec![0u8; n_chunks * (1 << 20)];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let h = blake3::hash(&data);
    (data, h)
}

fn raw_chunk_count(env: &S3Env, prefix: &str) -> Result<usize> {
    let url = format!(
        "{}/{}?list-type=2&prefix={}/chunks/",
        env.direct_endpoint, BUCKET, prefix
    );
    let mut body = String::new();
    crate::s3auth::get(&url)
        .call()
        .context("raw bucket LIST for chunk count")?
        .into_reader()
        .read_to_string(&mut body)?;
    Ok(body.matches("<Key>").count())
}

fn existence_fixture_files() -> Vec<Vec<u8>> {
    (0..300u32)
        .map(|i| {
            let marker = format!("existence-{i:04}-");
            marker
                .as_bytes()
                .iter()
                .copied()
                .cycle()
                .take(4096)
                .collect()
        })
        .collect()
}

/// A replica that replayed the writer's manifests turns a cold duplicate
/// import into confirming HEADs on hits, with no bucket LIST at mount.
fn existence_bloom_dedup(_seed: u64) -> Result<()> {
    let (env, root) = setup("existence-bloom-dedup")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("existence-bloom-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let files = existence_fixture_files();
    let mut model = Model::default();
    model.mkdir(std::path::Path::new("source"));
    for (i, data) in files.iter().enumerate() {
        model.write_file(
            std::path::Path::new(&format!("source/{i:04}")),
            data.clone(),
        );
    }

    let mut a = Client::new(root.path(), "a", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    std::fs::create_dir(a.mnt.join("source"))?;
    for (i, data) in files.iter().enumerate() {
        std::fs::write(a.mnt.join(format!("source/{i:04}")), data)?;
    }
    a.unmount()?;
    let before = raw_chunk_count(&env, &prefix)?;

    let mut b = Client::new(root.path(), "b", &env.endpoint, &backend)?;
    b.mount()?;
    eventually("A's tree reaches B", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("source/0299").is_file());
        Ok(())
    })?;
    std::fs::create_dir(b.mnt.join("duplicate"))?;
    model.mkdir(std::path::Path::new("duplicate"));
    for (i, data) in files.iter().enumerate() {
        std::fs::write(b.mnt.join(format!("duplicate/{i:04}")), data)?;
        model.write_file(
            std::path::Path::new(&format!("duplicate/{i:04}")),
            data.clone(),
        );
    }
    eventually(
        "duplicate upload decision drains",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(
                b.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0)
            );
            Ok(())
        },
    )?;
    model.verify(&b.mnt)?;
    let status = b.control_status()?;
    let wb = &status["writeback"];
    let bloom = wb["existence_bloom_hits"].as_u64().unwrap_or(0);
    let chunk_ref = wb["existence_chunk_ref_hits"].as_u64().unwrap_or(0);
    let misses = wb["existence_misses"].as_u64().unwrap_or(u64::MAX);
    eprintln!(
        "    existence-bloom-dedup: chunk_ref_hits={chunk_ref} bloom_hits={bloom} misses={misses}"
    );
    anyhow::ensure!(
        bloom + chunk_ref >= before as u64,
        "duplicate did not hit the replica hint: {wb}"
    );
    anyhow::ensure!(misses <= 1, "duplicate unexpectedly went unhinted: {wb}");
    anyhow::ensure!(
        raw_chunk_count(&env, &prefix)? == before,
        "duplicate import created extra chunk objects"
    );
    b.unmount()?;
    Ok(())
}

/// A clean peer digest is consulted before the replica and gets the credit
/// for selecting the probe. Disabling cooperative cache removes that hint
/// without changing correctness.
fn existence_peer_hint(_seed: u64) -> Result<()> {
    let (env, root) = setup("existence-peer-hint")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/existence-peer-{}", ts());
    let mut a = coop_client(root.path(), "hint-a", &env.endpoint, &backend)?;
    let mut b = coop_client(root.path(), "hint-b", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;

    let (data, _) = blob(8);
    std::fs::write(a.mnt.join("source"), &data)?;
    eventually("peer source is durable", Duration::from_secs(20), || {
        anyhow::ensure!(a.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0));
        Ok(())
    })?;
    eventually(
        "peer source metadata reaches B",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(b.mnt.join("source").is_file());
            Ok(())
        },
    )?;
    std::thread::sleep(Duration::from_secs(2));
    std::fs::write(b.mnt.join("peer-copy"), &data)?;
    eventually("peer-hinted copy drains", Duration::from_secs(20), || {
        anyhow::ensure!(b.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0));
        Ok(())
    })?;
    let status = b.control_status()?;
    let hints = status["writeback"]["existence_peer_hints"]
        .as_u64()
        .unwrap_or(0);
    eprintln!(
        "    existence-peer-hint: peer_hints={hints} bloom_hits={} chunk_ref_hits={}",
        status["writeback"]["existence_bloom_hits"],
        status["writeback"]["existence_chunk_ref_hits"]
    );
    anyhow::ensure!(hints >= 1, "peer digest never selected a probe: {status}");
    let mut model = Model::default();
    model.write_file(std::path::Path::new("source"), data.clone());
    model.write_file(std::path::Path::new("peer-copy"), data.clone());
    model.verify(&b.mnt)?;
    a.unmount()?;
    b.unmount()?;

    let mut c = Client::new(root.path(), "coop-off", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_COOP", "off");
    c.mount()?;
    std::fs::write(c.mnt.join("coop-off-copy"), &data)?;
    model.write_file(std::path::Path::new("coop-off-copy"), data);
    eventually("coop-off copy drains", Duration::from_secs(20), || {
        anyhow::ensure!(c.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0));
        Ok(())
    })?;
    model.verify(&c.mnt)?;
    let off = c.control_status()?;
    anyhow::ensure!(
        off["writeback"]["existence_peer_hints"].as_u64() == Some(0),
        "CONSTELLATION_COOP=off reported peer hints: {off}"
    );
    c.unmount()?;
    Ok(())
}

/// The mounted production path always has `Coop`, so its S3 leg must
/// preserve the old bounded read retry without re-running peer selection.
/// Disable object_store's own retries to isolate that application layer.
fn s3_retry(_seed: u64) -> Result<()> {
    let (env, root) = setup("s3-retry")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/s3retry-{}", ts());
    let mut c = Client::new(root.path(), "retry", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_S3_MAX_RETRIES", "0")
        .with_env("CONSTELLATION_S3_RETRY_TIMEOUT_MS", "500");
    c.fs_create()?;
    c.mount()?;
    let (data, expected) = blob(4);
    std::fs::write(c.mnt.join("big"), &data)?;
    eventually("write is durable", Duration::from_secs(20), || {
        anyhow::ensure!(c.control_status()?["spool"]["journal_backlog"].as_u64() == Some(0));
        Ok(())
    })?;
    c.unmount()?;
    c.drop_cache()?;
    c.mount()?;

    proxy.cut()?;
    let read = std::thread::scope(|scope| -> Result<Vec<u8>> {
        scope.spawn(|| -> Result<()> {
            // First GET fails while cut. The S3-only retry waits 50 ms,
            // so healing here makes its second attempt deterministic.
            std::thread::sleep(Duration::from_millis(25));
            proxy.heal()
        });
        std::fs::read(c.mnt.join("big")).context("cold read must retry S3")
    })?;
    anyhow::ensure!(blake3::hash(&read) == expected, "retried read was corrupt");
    let coop = coop_of(&c)?;
    anyhow::ensure!(
        coop["peer_hits"].as_u64() == Some(0) && coop["hedges_fired"].as_u64() == Some(0),
        "S3 retry unexpectedly contacted or hedged a peer: {coop}"
    );
    anyhow::ensure!(
        coop["s3_fetches"].as_u64() == Some(4),
        "successful logical S3 fetches should equal chunks: {coop}"
    );
    c.unmount()?;
    Ok(())
}

/// Cooperative cache (DESIGN.md §7): A warms a multi-chunk file; B
/// (cold) reads it while S3 is 200 ms away. Most of B's fetches must
/// come from A, and the read must beat the all-S3 serial bound.
fn coop_cache_hit(_seed: u64) -> Result<()> {
    let (env, root) = setup("coop-cache-hit")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/coophit-{}", ts());
    let mut a = coop_client(root.path(), "a", &env.endpoint, &backend)?;
    let mut b = coop_client(root.path(), "b", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;

    let n_chunks = 8usize;
    let (data, expected) = blob(n_chunks);
    std::fs::write(a.mnt.join("big"), &data)?;
    eventually("A shipped the write", Duration::from_secs(20), || {
        let s = a.control_status()?;
        anyhow::ensure!(
            s["spool"]["journal_backlog"].as_u64() == Some(0),
            "A still has a journal backlog: {s}"
        );
        Ok(())
    })?;
    eventually("B sees the file", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("big").is_file(), "big not on B");
        Ok(())
    })?;
    // Digest interval is 1s; wait for a snapshot of A's now-clean cache.
    std::thread::sleep(Duration::from_secs(2));
    proxy.latency(200, 0)?;

    let t0 = std::time::Instant::now();
    let read = std::fs::read(b.mnt.join("big"))?;
    let elapsed = t0.elapsed();
    proxy.heal()?;
    anyhow::ensure!(blake3::hash(&read) == expected, "B's read was corrupt");

    let coop = coop_of(&b)?;
    let hits = coop["peer_hits"].as_u64().unwrap_or(0);
    let s3 = coop["s3_fetches"].as_u64().unwrap_or(0);
    let hedges = coop["hedges_fired"].as_u64().unwrap_or(0);
    eprintln!(
        "    coop-cache-hit: B peer_hits={hits} s3_fetches={s3} hedges={hedges} in {elapsed:.1?}"
    );
    anyhow::ensure!(
        hits > s3 && hits >= (n_chunks as u64) / 2,
        "B should have fetched most chunks from A, got peer_hits={hits} s3_fetches={s3}: {coop}"
    );
    // A hedge is meant to rescue a late transfer, not to accompany every
    // healthy one: hedging unconditionally doubles the request load and
    // sends the second copy to the source we just decided against.
    anyhow::ensure!(
        hedges < n_chunks as u64,
        "every fetch hedged ({hedges} for {n_chunks} chunks): the deadline is not a late-transfer signal"
    );
    let serial = Duration::from_millis((n_chunks as u64) * 200);
    anyhow::ensure!(
        elapsed < serial,
        "read took {elapsed:.1?} which is not under the all-S3 serial bound {serial:.1?}"
    );
    a.unmount()?;
    b.unmount()?;
    Ok(())
}

/// Same shape as coop-cache-hit, but A is frozen mid-read. B must still
/// finish from S3; content is hash-verified; the dead peer is evicted
/// (errors / hedges show up on B's selector).
fn coop_fallback(_seed: u64) -> Result<()> {
    let (env, root) = setup("coop-fallback")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/coopfb-{}", ts());
    let mut a = coop_client(root.path(), "a", &env.endpoint, &backend)?;
    let mut b = coop_client(root.path(), "b", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;

    let n_chunks = 8usize;
    let (data, expected) = blob(n_chunks);
    std::fs::write(a.mnt.join("big"), &data)?;
    eventually("A shipped the write", Duration::from_secs(20), || {
        let s = a.control_status()?;
        anyhow::ensure!(s["spool"]["journal_backlog"].as_u64() == Some(0), "{s}");
        Ok(())
    })?;
    eventually("B sees the file", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("big").is_file());
        Ok(())
    })?;
    std::thread::sleep(Duration::from_secs(2));
    proxy.latency(200, 0)?;
    a.pause()?;

    let read = std::fs::read(b.mnt.join("big")).context("B must complete the read from S3")?;
    proxy.heal()?;
    anyhow::ensure!(blake3::hash(&read) == expected, "fallback read was corrupt");

    let coop = coop_of(&b)?;
    let s3 = coop["s3_fetches"].as_u64().unwrap_or(0);
    let hedges = coop["hedges_fired"].as_u64().unwrap_or(0);
    eprintln!(
        "    coop-fallback: B s3_fetches={s3} hedges_fired={hedges} peer_misses={} peer_errors={}",
        coop["peer_misses"],
        coop["peer_errors"].as_u64().unwrap_or(0)
    );
    anyhow::ensure!(
        s3 >= 1,
        "B must have fallen back to S3 after A was paused: {coop}"
    );
    a.resume()?;
    a.unmount()?;
    b.unmount()?;
    Ok(())
}

/// Web-fleet shape: one node writes, two "servers" with cold caches read
/// repeatedly under S3 latency. Aggregate S3 fetches stay near the
/// unique-chunk count (each chunk pulled from S3 ~once, then peer-served).
fn web_fleet(_seed: u64) -> Result<()> {
    let (env, root) = setup("web-fleet")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/webfleet-{}", ts());
    let mut a = coop_client(root.path(), "a", &env.endpoint, &backend)?;
    let mut b = coop_client(root.path(), "b", &env.endpoint, &backend)?;
    let mut c = coop_client(root.path(), "c", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_peers(&[&a, &b, &c])?;

    let n_chunks = 8usize;
    let (data, expected) = blob(n_chunks);
    std::fs::write(a.mnt.join("site"), &data)?;
    eventually("A shipped", Duration::from_secs(20), || {
        anyhow::ensure!(a.control_status()?["spool"]["journal_backlog"].as_u64() == Some(0));
        Ok(())
    })?;
    eventually("file visible on B and C", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("site").is_file() && c.mnt.join("site").is_file());
        Ok(())
    })?;
    std::thread::sleep(Duration::from_secs(2));
    proxy.latency(200, 0)?;

    for reader in [&b, &c] {
        for _ in 0..2 {
            let got = std::fs::read(reader.mnt.join("site"))?;
            anyhow::ensure!(
                blake3::hash(&got) == expected,
                "{} read corrupt",
                reader.name
            );
        }
    }
    proxy.heal()?;

    let cb = coop_of(&b)?;
    let cc = coop_of(&c)?;
    let s3 = cb["s3_fetches"].as_u64().unwrap_or(0) + cc["s3_fetches"].as_u64().unwrap_or(0);
    let hits = cb["peer_hits"].as_u64().unwrap_or(0) + cc["peer_hits"].as_u64().unwrap_or(0);
    eprintln!(
        "    web-fleet: aggregate s3_fetches={s3} peer_hits={hits} (unique chunks {n_chunks})"
    );
    anyhow::ensure!(
        s3 <= n_chunks as u64 + 4,
        "S3 fetches {s3} should stay near the unique-chunk count {n_chunks}: b={cb} c={cc}"
    );
    anyhow::ensure!(
        hits >= n_chunks as u64,
        "readers should have served each other / A: peer_hits={hits}"
    );
    a.unmount()?;
    b.unmount()?;
    c.unmount()?;
    Ok(())
}

/// HTTP-level phase-7 smoke test. The listener is deliberately localhost-only;
/// remote administration is expected to use a tunnel rather than widening an
/// unauthenticated control endpoint.
fn web_ui_smoke(_seed: u64) -> Result<()> {
    let (env, root) = setup("web-ui-smoke")?;
    let _proxy = env.s3_proxy()?;
    let port = std::net::TcpListener::bind(("127.0.0.1", 0))?
        .local_addr()?
        .port();
    let backend = format!("s3://{BUCKET}/web-ui-smoke-{}", ts());
    let mut client = Client::new(root.path(), "web", &env.endpoint, &backend)?.with_web_ui(port);
    client.fs_create()?;
    client.mount()?;
    std::fs::create_dir(client.mnt.join("through-http"))?;

    let base = format!("http://127.0.0.1:{port}");
    let status: serde_json::Value = ureq::get(&format!("{base}/api/status"))
        .timeout(Duration::from_secs(10))
        .call()
        .context("GET /api/status")?
        .into_json()?;
    anyhow::ensure!(status["fs_uuid"].is_string(), "bad HTTP status: {status}");
    let round_trip: serde_json::Value = serde_json::from_str(&serde_json::to_string(&status)?)?;
    anyhow::ensure!(round_trip == status, "HTTP status serde round-trip changed");

    // The control protocol over HTTP (plan 31 C5): `{method, params}` in,
    // `{"ok": result}` out (an error is a 4xx/5xx with `{"error": ...}`).
    let request = |method: &str, params: serde_json::Value| -> Result<serde_json::Value> {
        let reply: serde_json::Value = ureq::post(&format!("{base}/api"))
            .timeout(Duration::from_secs(30))
            .send_json(serde_json::json!({"method": method, "params": params}))
            .with_context(|| format!("POST /api {method}"))?
            .into_json()?;
        anyhow::ensure!(reply.get("ok").is_some(), "{method}: {reply}");
        Ok(reply["ok"].clone())
    };
    let directory = request("browse.readdir", serde_json::json!({"path":"/"}))?;
    anyhow::ensure!(
        directory["entries"]
            .as_array()
            .is_some_and(|entries| entries.iter().any(|e| e["name"] == "through-http")),
        "HTTP ReadDir omitted created directory: {directory}"
    );
    let created = request(
        "snapshot.create",
        serde_json::json!({"selector":"/@web-ui-smoke"}),
    )?;
    anyhow::ensure!(
        created["snapshot"]["name"] == "web-ui-smoke",
        "snapshot create failed: {created}"
    );
    let listed = request("snapshot.list", serde_json::json!({"path":null}))?;
    anyhow::ensure!(
        listed["snapshots"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["name"] == "web-ui-smoke")),
        "snapshot not listed through HTTP: {listed}"
    );
    // Plan 32 Step 7: what the snapshots page's editor, timeline and space
    // views call, over HTTP, each answer well-formed. An invalid
    // expression is a *result* (`ok: false` with the byte offset the
    // editor puts its caret under), not a failed call. The accounting
    // index is built on first use, so `snapshot.space` is polled until it
    // has caught up; `snapshot.reclaim` then answers for the snapshot
    // above.
    type Check = fn(&serde_json::Value) -> bool;
    eventually("snapshot.space built", Duration::from_secs(60), || {
        let space = request("snapshot.space", serde_json::json!({}))?;
        anyhow::ensure!(space["building"] == false, "still building: {space}");
        Ok(())
    })?;
    let step7: [(&str, serde_json::Value, Check); 5] = [
        (
            "snapshot.policy.check",
            serde_json::json!({"expr": "1d:7d 1h:1d"}),
            |r| {
                r["ok"] == true
                    && r["canonical"] == "1h:1d 1d:7d"
                    && r["steady_state_bound"] == 31
                    && r["simulated_count"].is_u64()
            },
        ),
        (
            "snapshot.policy.check",
            serde_json::json!({"expr": "5m:1d 7m:1d"}),
            |r| {
                r["ok"] == false
                    && r["error"]["offset"] == 6
                    && r["error"]["message"]
                        .as_str()
                        .is_some_and(|m| m.contains("7m"))
                    && r["canonical"].is_null()
            },
        ),
        (
            "snapshot.policy.simulate",
            serde_json::json!({"path": "/", "expr": "1h:1d", "horizon_ms": 86_400_000}),
            |r| {
                let snaps = r["snapshots"].as_array();
                r["policy"] == "1h:1d"
                    && r["cadence"] == "1h"
                    && r["horizon_unix_ms"]
                        .as_i64()
                        .zip(r["now_unix_ms"].as_i64())
                        .is_some_and(|(h, n)| h - n == 86_400_000)
                    && r["created"].as_u64().is_some_and(|n| n >= 24)
                    && r["counts"].as_array().is_some_and(|c| !c.is_empty())
                    // The real manual snapshot is carried through, never
                    // this policy's to expire; the rest are synthetic.
                    && snaps.is_some_and(|s| {
                        s.iter().any(|e| {
                            e["synthetic"] == false && e["candidate"] == false && e["keep"] == true
                        }) && s.iter().any(|e| e["synthetic"] == true)
                    })
            },
        ),
        ("snapshot.space", serde_json::json!({}), |r| {
            r["building"] == false
                && ["live_logical", "gc_horizon_ms", "as_of_seq"]
                    .iter()
                    .all(|k| r[*k].is_u64())
                && [
                    "snapshots_total",
                    "unique",
                    "shared_snapshots_only",
                    "shared_with_live",
                    "awaiting_gc",
                ]
                .iter()
                .all(|k| r[*k]["bytes"].is_u64() && r[*k]["chunks"].is_u64())
        }),
        (
            "snapshot.reclaim",
            serde_json::json!({"selectors": ["/@web-ui-smoke"]}),
            |r| {
                r["building"] == false
                    && ["bytes", "chunks", "as_of_seq"]
                        .iter()
                        .all(|k| r[*k].is_u64())
            },
        ),
    ];
    for (method, params, ok) in step7 {
        let reply = request(method, params.clone())?;
        anyhow::ensure!(ok(&reply), "{method} {params}: malformed answer {reply}");
    }
    request(
        "snapshot.delete",
        serde_json::json!({"selector":"/@web-ui-smoke"}),
    )
    .context("snapshot delete failed")?;
    // Unix-socket only: HTTP refuses the in-place upgrade outright.
    let refused = ureq::post(&format!("{base}/api"))
        .timeout(Duration::from_secs(10))
        .send_json(serde_json::json!({"method": "node.handoff", "params": {}}));
    anyhow::ensure!(
        matches!(refused, Err(ureq::Error::Status(403, _))),
        "node.handoff over HTTP must be refused: {refused:?}"
    );

    let metrics = ureq::get(&format!("{base}/metrics"))
        .timeout(Duration::from_secs(10))
        .call()?
        .into_string()?;
    for gauge in [
        "constellation_spool_backlog",
        "constellation_cache_used_bytes",
        "constellation_lease_held",
    ] {
        anyhow::ensure!(metrics.contains(gauge), "/metrics omitted {gauge}");
    }
    // Plan 38 §5: the op metrics of this real mount carry the transport
    // its connection negotiated (`dev_fuse` unless the lane asked for the
    // ring and got it), and the FUSE transport families are exported.
    let transport = status["mounts"][0]["transport"]
        .as_str()
        .context("status names no transport")?
        .to_string();
    let label = format!("transport=\"{transport}\"");
    anyhow::ensure!(
        metrics
            .lines()
            .any(|l| l.starts_with("constellation_vfs_ops_total{")
                && l.contains("frontend=\"fuse\"")
                && l.contains(&label)),
        "/metrics has no FUSE op row labelled {label}"
    );
    anyhow::ensure!(
        metrics
            .lines()
            .filter(|l| l.starts_with("constellation_vfs_op") && l.contains("frontend=\"fuse\""))
            .all(|l| l.contains(&label)),
        "/metrics has a FUSE op row not labelled {label}"
    );
    for family in [
        "# TYPE constellation_fuse_transport_fallbacks_total counter",
        "# TYPE constellation_fuse_zero_copy_reads_total counter",
        "# TYPE constellation_fuse_passthrough_opens gauge",
        "# TYPE constellation_fuse_uring_queue_depth gauge",
    ] {
        anyhow::ensure!(
            metrics.lines().any(|l| l == family),
            "/metrics omitted {family}"
        );
    }
    anyhow::ensure!(
        metrics
            .lines()
            .any(|l| l.starts_with("constellation_fuse_uring_queue_depth{") && l.contains(&label)),
        "/metrics has no queue depth for the mount"
    );
    client.unmount()?;
    Ok(())
}

/// How long a write on `from` takes to appear on `to`.
fn visibility_ms(from: &Client, to: &Client, tag: &str) -> Result<u128> {
    let started = std::time::Instant::now();
    let body = format!("body-{tag}");
    std::fs::write(from.mnt.join(tag), body.as_bytes())?;
    let dst = to.mnt.join(tag);
    loop {
        if matches!(std::fs::read(&dst), Ok(v) if v == body.as_bytes()) {
            return Ok(started.elapsed().as_millis());
        }
        anyhow::ensure!(
            started.elapsed() < Duration::from_secs(60),
            "{tag} never became visible on {}",
            to.name
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn conflicts_of(c: &Client) -> Result<u64> {
    Ok(c.control_status()?["spool"]["conflicts"]
        .as_u64()
        .unwrap_or(u64::MAX))
}

/// Assert the leaseless conflict path never fired: with leases the
/// authority holder is the only writer, so a skipped foreign record
/// would mean the invariant broke somewhere. Takes a slice (plan 30 M0)
/// so 3- and 4-node scenarios can use it too.
fn ensure_no_conflicts(clients: &[&Client]) -> Result<()> {
    for c in clients {
        let n = conflicts_of(c)?;
        anyhow::ensure!(
            n == 0,
            "lease-serialized writes must produce zero conflicts, {} saw {n}: {}\n--- {} log ---\n{}",
            c.name,
            c.control_status()?["spool"],
            c.name,
            c.tail_log_n(80),
        );
    }
    Ok(())
}

/// Write authority handover on the single shared partition (DESIGN.md
/// §4) between two nodes with no P2P path (they share one node key, so
/// every dial fails "connecting to ourself" — the S3-only cluster). Plan
/// 30 M13's hybrid changed what the first write from the non-holder
/// does: it is served through the holder's inbox at once, without
/// moving the lease (the round-4 note in PROGRESS.md records why the
/// pre-M13 shape of this scenario — "B's first write blocks until A's
/// idle release, then B holds" — passed only because that write blocked
/// for up to half a TTL inside `eventually`). What must hold now, per
/// round and in both directions: a node that writes a sustained block
/// while the other holds gets every op answered (through the inbox, then
/// locally), escalates, and holds the lease itself within one dwell plus
/// the holder's next renew (half the TTL) plus the wanted grace; both
/// nodes then see both sets, the epoch advances across every handover,
/// and neither node recorded a conflict (with leases the leaseless
/// conflict path must be unreachable).
fn lease_handover(seed: u64) -> Result<()> {
    let (env, root) = setup("lease-handover")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/lease-{}", ts());
    // A holder learns of a waiter at its next renew, half a TTL away:
    // 10 s keeps the handover inside `HANDOVER` without touching the
    // dwell/grace rules being exercised.
    let tune = |c: Client| c.with_env("CONSTELLATION_LEASE_TTL_MS", "10000");
    let mut c0 = tune(Client::new(root.path(), "c0", &env.endpoint, &backend)?);
    let mut c1 = tune(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;

    /// Dwell (5 s) + half the TTL (5 s) + wanted grace (5 s) + the
    /// escalator's retry (2 s), with margin.
    const HANDOVER: Duration = Duration::from_secs(30);

    let mut m = Model::default();
    let mut w0 = Workload::new(seed, "a");
    let mut w1 = Workload::new(seed.wrapping_add(1), "b");
    // The model's root is the shared subtree both nodes write into.
    std::fs::create_dir(c0.mnt.join("shared"))?;
    eventually("shared subtree on B", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("shared").is_dir(), "shared not on c1");
        Ok(())
    })?;

    // One node's turn: a sustained block of writes (answered whether or
    // not it holds yet), then it must hold the lease — probing with a
    // write+remove pair so its demand stays sustained until the holder's
    // renew sees `wanted_by` and hands over.
    let turn =
        |c: &Client, w: &mut Workload, m: &mut Model, who: &str, round: usize| -> Result<u64> {
            // The other node's block has to be visible here first: the
            // workload renames and removes what the model says exists.
            eventually(
                &format!("{who} sees the other node's block, round {round}"),
                Duration::from_secs(30),
                || m.verify(&c.mnt.join("shared")),
            )?;
            w.run_block(&c.mnt.join("shared"), m, 25)?;
            eventually(
                &format!("{who} holds the lease after its block, round {round}"),
                HANDOVER,
                || {
                    let probe = c.mnt.join(format!("shared/probe-{who}-{round}"));
                    std::fs::write(&probe, b"p")?;
                    std::fs::remove_file(&probe)?;
                    let l = lease_of(c)?;
                    anyhow::ensure!(
                        l["held"] == true && l["lost"] == false,
                        "{who} does not hold the lease: {l}"
                    );
                    Ok(())
                },
            )?;
            let inbox = inbox_of(c)?;
            eprintln!(
                "    lease-handover: round {round} {who} holds; escalations {} lease_requests {} \
             inbox_ops {} local_ops {}",
                inbox["escalations"],
                inbox["lease_requests"],
                inbox["inbox_ops"],
                inbox["local_ops"]
            );
            Ok(lease_of(c)?["epoch"].as_u64().unwrap_or(0))
        };

    let mut epochs = Vec::new();
    for round in 0..3 {
        epochs.push(turn(&c0, &mut w0, &mut m, "A", round)?);
        epochs.push(turn(&c1, &mut w1, &mut m, "B", round)?);

        // Both nodes converge on the union of both nodes' writes.
        eventually(
            &format!("both views converge, round {round}"),
            Duration::from_secs(30),
            || {
                m.verify(&c1.mnt.join("shared")).context("via c1")?;
                m.verify(&c0.mnt.join("shared")).context("via c0")?;
                Ok(())
            },
        )?;
    }

    anyhow::ensure!(
        epochs.windows(2).all(|w| w[1] > w[0]),
        "lease epoch must advance across every handover, saw {epochs:?}"
    );
    // Non-vacuity: B never held before its first block, so its lease
    // came from the hybrid's escalation, not from a pre-M13 lease-path
    // write.
    let b_inbox = inbox_of(&c1)?;
    anyhow::ensure!(
        b_inbox["escalations"].as_u64().unwrap_or(0) >= 1
            && b_inbox["inbox_ops"].as_u64().unwrap_or(0) >= 1,
        "B's lease must have come from sustained inbox demand: {b_inbox}"
    );
    ensure_no_conflicts(&[&c0, &c1])?;
    eprintln!("    lease-handover: epochs {epochs:?}");
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Fencing (DESIGN.md §4): A holds the lease with unshipped records and
/// is frozen with SIGSTOP, so it stops renewing. After expiry B takes
/// over — legally, having applied everything A flushed — and writes.
/// Resumed, A must discover it was deposed and never ship under its old
/// epoch. Plan 30 §M3b: A's unshipped write is then rolled back and
/// replayed by rid through B (exactly once, no conflict copy), A becomes
/// an ordinary node that writes through B, and nothing *shared* is
/// damaged: both namespaces stay exactly model-correct and a third,
/// fresh node rebuilds the same world from the log alone.
fn lease_fencing(_seed: u64) -> Result<()> {
    eprintln!("    lease-fencing: starting S3 env");
    let (env, root) = setup("lease-fencing")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/fence-{}", ts());
    // Short TTL so expiry is observable inside a test.
    let ttl_ms = 5_000u64;
    let short = |c: Client| {
        c.with_env("CONSTELLATION_LEASE_TTL_MS", &ttl_ms.to_string())
            // Long enough that A never releases voluntarily here.
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
    };
    // Plan 30 §M3b: A's deposition recovery now replays its stranded
    // journal through B over the ordinary forward path
    // (`recovery::drain_pending_replays`), which needs live P2P to be
    // fast; without it every attempt falls through to the S3-only slow
    // path until `recovery::LEASE_FALLBACK` gives up and A just acquires
    // the lease itself, costing this scenario ~30s it does not need to
    // spend. Give each client its own P2P identity (see
    // `Client::with_own_node_key`'s doc for the shared-key failure mode).
    let mut c0 =
        short(Client::new(root.path(), "c0", &env.endpoint, &backend)?.with_own_node_key());
    let mut c1 =
        short(Client::new(root.path(), "c1", &env.endpoint, &backend)?.with_own_node_key());
    c0.fs_create()?;
    eprintln!("    lease-fencing: mounting");
    c0.mount()?;
    c1.mount()?;

    let mut model = Model::default();
    std::fs::create_dir(c0.mnt.join("shared"))?;
    model.mkdir(std::path::Path::new("shared"));
    std::fs::write(c0.mnt.join("shared/from-a"), b"a1")?;
    model.write_file(std::path::Path::new("shared/from-a"), b"a1".to_vec());
    eventually(
        "A's write flushed and visible on B",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(
                std::fs::read(c1.mnt.join("shared/from-a"))? == b"a1",
                "from-a not on B yet"
            );
            Ok(())
        },
    )?;
    eprintln!("    lease-fencing: from-a visible on B");
    let a_epoch = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

    // Journal a metadata-only mutation, then freeze A before the 200ms
    // shipper tick can flush it. Do not cut S3 first: mkdir while the
    // proxy is down makes require_lease/ship retries block the FUSE
    // thread for object_store's ~180s retry budget. Query the control
    // socket before SIGSTOP — a paused daemon cannot answer.
    std::fs::create_dir(c0.mnt.join("shared/stranded"))?;
    let a_spool_before = c0.control_status()?["spool"].clone();
    anyhow::ensure!(
        a_spool_before["journal_backlog"].as_u64().unwrap_or(0) > 0,
        "A should hold unshipped records before being deposed: {a_spool_before}"
    );
    eprintln!("    lease-fencing: pausing A (SIGSTOP)");
    c0.pause()?;

    // Wait out A's lease, then force B to acquire with a mutation.
    std::thread::sleep(Duration::from_millis(ttl_ms + 1_500));
    eprintln!("    lease-fencing: B taking over");
    std::fs::write(c1.mnt.join("shared/from-b"), b"b1")
        .context("B must be able to take over an expired lease")?;
    model.write_file(std::path::Path::new("shared/from-b"), b"b1".to_vec());
    let b_lease = lease_of(&c1)?;
    anyhow::ensure!(
        b_lease["held"] == true && b_lease["epoch"].as_u64().unwrap_or(0) > a_epoch,
        "B must hold a strictly newer epoch than A's {a_epoch}: {b_lease}"
    );
    // The takeover applied everything A had flushed.
    anyhow::ensure!(
        std::fs::read(c1.mnt.join("shared/from-a"))? == b"a1",
        "takeover must not lose the predecessor's flushed writes"
    );

    // Resume A: it learns it was deposed (its renewal CAS fails, or it
    // tails B's epoch marker and renews at once). Plan 30 §M3b: nothing is
    // left stranded any more — A rolls its unshipped `mkdir` back from its
    // captured before-image and replays it by rid through B, exactly once,
    // with no operator step. What must hold is that nothing reached the log
    // behind B's back under A's old epoch: the mkdir arrives only as B's
    // own execution of the replay.
    c0.resume()?;
    eventually(
        "A recovers from the deposition by itself",
        Duration::from_secs(40),
        || {
            let status = c0.control_status()?;
            let spec = &status["speculation"];
            anyhow::ensure!(
                spec["depositions"].as_u64().unwrap_or(0) >= 1
                    && spec["local_rolled_back"].as_u64().unwrap_or(0) >= 1,
                "A has not rolled back its stranded journal: {spec}"
            );
            anyhow::ensure!(
                status["lease"]["lost"] == false,
                "the recovery must clear `lost`: {}",
                status["lease"]
            );
            anyhow::ensure!(
                status["spool"]["journal_backlog"].as_u64() == Some(0)
                    && spec["pending_replay"].as_u64() == Some(0),
                "A still holds stranded work: spool {} speculation {spec}",
                status["spool"]
            );
            Ok(())
        },
    )?;
    model.mkdir(std::path::Path::new("shared/stranded"));
    // A is an ordinary node again: its next write goes through B.
    std::fs::write(c0.mnt.join("shared/after-deposition"), b"ok")
        .context("a recovered node must accept writes again (through the holder)")?;
    model.write_file(
        std::path::Path::new("shared/after-deposition"),
        b"ok".to_vec(),
    );
    let b_lease = lease_of(&c1)?;
    anyhow::ensure!(
        b_lease["epoch"].as_u64().unwrap_or(0) > a_epoch,
        "nothing may run under A's deposed epoch again: {b_lease}"
    );

    // B's view is exactly the model — A's replayed mkdir included, once.
    eventually(
        "B's namespace is model-correct",
        Duration::from_secs(30),
        || {
            model.verify(&c1.mnt)?;
            model.verify(&c0.mnt)
        },
    )?;
    anyhow::ensure!(
        c0.control_status()?["speculation"]["replay_conflicts"].as_u64() == Some(0),
        "a non-overlapping stranded op must replay without a conflict copy"
    );

    // The shared log is uncorrupted: a fresh node rebuilds B's world.
    c0.kill9()?;
    c1.unmount()?;
    let mut c2 = short(Client::new(root.path(), "c2", &env.endpoint, &backend)?);
    c2.mount().context("fresh node bootstrap after fencing")?;
    model
        .verify(&c2.mnt)
        .context("shared log must reconstruct exactly the surviving namespace")?;
    anyhow::ensure!(
        conflicts_of(&c2)? == 0,
        "fencing must not surface as a data conflict: {}",
        c2.control_status()?["spool"]
    );
    c2.unmount()?;
    Ok(())
}

fn epoch_clients(env: &S3Env, root: &std::path::Path, backend: &str) -> Result<(Client, Client)> {
    let tune = |client: Client, key: &str| {
        client
            .with_env("CONSTELLATION_NODE_KEY", key)
            // Plan 30 §M10: an epoch carries the holder's lease only while
            // the lease is usable when the members ack (`EpochClaimView`),
            // and these scenarios write inside the epoch, which needs the
            // carried lease. With the 5 s TTL they used before M10, the
            // window from the S3 cut to the end of the lease's usability
            // was as short as 5 − 3.3 (renewed at about half TTL) − 1
            // (margin) ≈ 0.7 s, less than a formation takes (a failed
            // round, the proposal grace, pings, acks): whenever the cut
            // fell just before a renewal the epoch formed carrying nothing,
            // and `epoch-member-lost`'s write after the resume failed.
            // 20 s leaves at least 9 s, as the M10 scenarios have it.
            .with_env("CONSTELLATION_LEASE_TTL_MS", "20000")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
    };
    Ok((
        tune(
            Client::new(root, "c0", &env.endpoint, backend)?,
            &scenario_key(root, "epoch-c0"),
        ),
        tune(
            Client::new(root, "c1", &env.endpoint, backend)?,
            &scenario_key(root, "epoch-c1"),
        ),
    ))
}

fn wait_for_epoch(clients: [&Client; 2]) -> Result<()> {
    for client in clients {
        eventually(
            &format!("{} activates the continuation epoch", client.name),
            Duration::from_secs(20),
            || {
                let status = client.control_status()?;
                anyhow::ensure!(
                    status["epoch"]["active"] == true,
                    "{} epoch not active: {}",
                    client.name,
                    status["epoch"]
                );
                anyhow::ensure!(
                    status["epoch"]["members"]
                        .as_array()
                        .is_some_and(|m| m.len() == 2),
                    "{} epoch does not cover both writers: {}",
                    client.name,
                    status["epoch"]
                );
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn continuation_epoch(_seed: u64) -> Result<()> {
    let (env, root) = setup("continuation-epoch")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/epoch-{}", ts());
    let (mut c0, mut c1) = epoch_clients(&env, root.path(), &backend)?;
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;

    // Make A the current p0 holder before S3 disappears.
    std::fs::create_dir(c0.mnt.join("a"))?;
    std::fs::create_dir(c0.mnt.join("b"))?;
    eventually("epoch roots visible on B", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("a").is_dir());
        anyhow::ensure!(c1.mnt.join("b").is_dir());
        Ok(())
    })?;

    proxy.cut()?;
    wait_for_epoch([&c0, &c1])?;

    std::fs::write(c0.mnt.join("a/from-a"), b"epoch-a")?;
    let b_forwarded_before = c1.control_status()?["forwarded_ok"].as_u64().unwrap_or(0);
    // B's write reaches the epoch authority over P2P only: either a
    // P2P-only p0 handoff from A (B then journals it locally), or — when B
    // already knows A as the holder (A's pre-cut segments were pushed to
    // it, which caches the holder) — a forward that A executes and
    // journals. Which one depends on whether B learned of A through the
    // push or through its own S3 tail, a race this scenario does not
    // control; both are correct. What must hold is that nothing shipped:
    // every write is sitting in some journal while S3 is cut. (This used
    // to require B's own backlog to be non-empty, which the forward path
    // legitimately leaves at zero: a flaky assertion, not an ordering bug.)
    // Plan 30 §M10 × §M9: B keeps following A's log stream through the
    // epoch, and A streams its epoch journal ahead, so a forward whose
    // reply names A's unshipped journal (B's own create before this
    // write, above all) is answered as soon as the stream delivers it.
    // Before, it waited for a log that could not arrive until S3
    // returned: 40 s, then in doubt and retried. The bound is generous
    // (a loaded, frequency-limited runner); the stall it rules out is
    // the forward deadline.
    let started = std::time::Instant::now();
    std::fs::write(c1.mnt.join("b/from-b"), b"epoch-b").context("B's write in the epoch")?;
    let took = started.elapsed();
    anyhow::ensure!(
        took < Duration::from_secs(10),
        "B's write took {took:?} inside the epoch (the member waited for the log instead of \
         the holder's stream): B status {}",
        c1.control_status()?
    );
    let b_epoch = c1.control_status()?["epoch"].clone();
    println!(
        "continuation-epoch: B's write took {took:?}; B installed {} transactions from A's \
         stream, {} of its forwards answered by it",
        b_epoch["streamed_installed"], b_epoch["forwards_streamed"]
    );
    let a_status = c0.control_status()?;
    let b_status = c1.control_status()?;
    anyhow::ensure!(
        a_status["spool"]["journal_backlog"].as_u64().unwrap_or(0) > 0,
        "A's epoch write must be journaled locally: {}",
        a_status["spool"]
    );
    let b_backlog = b_status["spool"]["journal_backlog"].as_u64().unwrap_or(0);
    let b_forwarded = b_status["forwarded_ok"].as_u64().unwrap_or(0) > b_forwarded_before;
    anyhow::ensure!(
        b_backlog > 0 || b_forwarded,
        "B's epoch write was neither journaled on B (handoff) nor forwarded to A: \
         B spool {}, forwarded_ok {}",
        b_status["spool"],
        b_status["forwarded_ok"]
    );

    proxy.heal()?;
    eventually(
        "epoch journals flush and both trees converge",
        Duration::from_secs(40),
        || {
            for client in [&c0, &c1] {
                let status = client.control_status()?;
                let from_a = std::fs::read(client.mnt.join("a/from-a"));
                let from_b = std::fs::read(client.mnt.join("b/from-b"));
                if !matches!(&from_a, Ok(bytes) if bytes == b"epoch-a")
                    || !matches!(&from_b, Ok(bytes) if bytes == b"epoch-b")
                {
                    anyhow::bail!(
                        "{} has a={from_a:?}, b={from_b:?}; status={status}; \
                         peer status={}; logs:\n--- c0 ---\n{}\n--- c1 ---\n{}",
                        client.name,
                        if client.name == "c0" {
                            c1.control_status()?
                        } else {
                            c0.control_status()?
                        },
                        c0.tail_log_n(100),
                        c1.tail_log_n(100)
                    );
                }
                anyhow::ensure!(status["epoch"]["active"] == false);
                anyhow::ensure!(
                    status["spool"]["journal_backlog"].as_u64() == Some(0),
                    "journal did not drain: {status}"
                );
                anyhow::ensure!(
                    status["reintegration"]["conflicts_materialized"].as_u64() == Some(0)
                );
            }
            Ok(())
        },
    )?;
    ensure_no_conflicts(&[&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

fn epoch_member_lost(_seed: u64) -> Result<()> {
    let (env, root) = setup("epoch-member-lost")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/epoch-lost-{}", ts());
    let (mut c0, mut c1) = epoch_clients(&env, root.path(), &backend)?;
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;
    std::fs::create_dir(c0.mnt.join("shared")).context("mkdir shared")?;
    eventually("shared visible", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("shared").is_dir());
        Ok(())
    })?;

    proxy.cut()?;
    wait_for_epoch([&c0, &c1])?;
    c1.pause()?;
    eventually("A freezes after losing B", Duration::from_secs(10), || {
        let status = c0.control_status()?;
        anyhow::ensure!(status["epoch"]["active"] == false, "not frozen: {status}");
        let error = std::fs::create_dir(c0.mnt.join("shared/refused"))
            .expect_err("frozen epoch must refuse writes");
        anyhow::ensure!(
            Code::from_os_error(&error) == Some(Code::ReadOnly),
            "expected EROFS, got {error}"
        );
        Ok(())
    })?;

    c1.resume()?;
    eventually(
        "epoch resumes when B returns",
        Duration::from_secs(15),
        || {
            anyhow::ensure!(c0.control_status()?["epoch"]["active"] == true);
            anyhow::ensure!(c1.control_status()?["epoch"]["active"] == true);
            Ok(())
        },
    )?;
    std::fs::write(c0.mnt.join("shared/after-resume"), b"ok")
        .context("A's write after the epoch resumed")?;
    proxy.heal()?;
    eventually("resumed write converges", Duration::from_secs(40), || {
        anyhow::ensure!(std::fs::read(c1.mnt.join("shared/after-resume"))? == b"ok");
        Ok(())
    })?;
    ensure_no_conflicts(&[&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Plan 30 §M3b: a deposed holder recovers on its own. A holds the lease
/// and journals three stranded edits while its S3 path is cut — a new
/// file (`clean-from-a`), an overwrite of a baseline file B never touches
/// (`a-only`), and an overwrite of a file B then overwrites too (`same`)
/// — and is frozen before any of it ships. B takes the expired lease
/// (shipping an epoch marker, since A never released) and writes `same`.
/// Resumed, A learns it was deposed at its next renewal (or from B's
/// marker), rolls its unshipped journal back from before-images and
/// replays every transaction by rid through B — with no operator
/// `reintegrate` call. Only the genuine overlap is refused: `same` keeps
/// B's winner and A's bytes land as exactly one
/// `.constellation-conflict/same@<node>-<ts>-<seq>` copy, while `clean-from-a`
/// and `a-only` replay cleanly with no conflict copy at all.
///
/// **Layer A only** (plan 30 §3's durability table, "no backup"): the
/// premise is a holder that keeps acknowledging locally while cut off
/// from S3, so its acknowledged writes strand when it is deposed. Plan
/// 30 §M9 auto-selects a LAN peer as a synchronous backup
/// (`CONSTELLATION_BACKUP_RTT_BUDGET_MS`, default 5 ms), and with one the
/// premise no longer holds: while the backup is being brought up nothing
/// is acknowledged without S3 (the writes block, then fail in doubt),
/// and once it is committed the writes are acknowledged by the backup,
/// which seals A's epoch and adopts them — nothing strands. So this
/// scenario pins the budget to 0 (today's `Local`), and
/// `deposed-reintegration-backup` covers the M9 path.
fn deposed_reintegration(_seed: u64) -> Result<()> {
    let (env, root) = setup("deposed-reintegration")?;
    // A reaches S3 through its own switch, so the cut below isolates A
    // alone, as the scenario means to: through the shared toxiproxy route
    // it cut B too — an S3 outage for the whole cluster, in which A and
    // B form a continuation epoch (plan 30 §M10) and the premise (a
    // holder acknowledging locally while the rest of the cluster carries
    // on) no longer holds.
    let _route = env.s3_proxy()?;
    let a_s3 = env.counting_proxy()?;
    let backend = format!("s3://{BUCKET}/reintegrate-{}", ts());
    let tune = |client: Client| {
        client
            .with_own_node_key()
            .with_env("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0")
            .with_env("CONSTELLATION_LEASE_TTL_MS", "5000")
            // Keep B holding for a while after its takeover write, so
            // resumed A's renewal finds B's lease (the deposition path
            // under test) and its replay drain forwards to a live holder.
            // If B has released by then anyway, the drain's own lease
            // fallback replays locally instead; either converges.
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "10000")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "3000")
    };
    // Each node its own key in its state dir: the fixed `/tmp` key paths
    // this used before were shared by every concurrent run on the host.
    // Write-back on A: its stranded writes journal and return at once, as
    // the premise says. Under the default write-through each close first
    // tries to upload its chunk to the cut S3 (with retries and backoff,
    // and behind the rounds' own upload attempts of the same chunks), and
    // three of them can outlast A's lease view (5 s TTL): the last write
    // then fails with EIO before A is even frozen (1 run in ~6).
    let mut c0 =
        tune(Client::new(root.path(), "c0", &a_s3.endpoint(), &backend)?).with_write_mode("back");
    let mut c1 = tune(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;

    std::fs::create_dir(c0.mnt.join("shared"))?;
    std::fs::write(c0.mnt.join("shared/same"), b"baseline")?;
    std::fs::write(c0.mnt.join("shared/a-only"), b"baseline-a")?;
    eventually("baseline visible on B", Duration::from_secs(30), || {
        anyhow::ensure!(std::fs::read(c1.mnt.join("shared/same"))? == b"baseline");
        anyhow::ensure!(std::fs::read(c1.mnt.join("shared/a-only"))? == b"baseline-a");
        Ok(())
    })?;
    let a_epoch = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

    // Three stranded changes: a new file and an overwrite nobody else
    // touches (both must replay cleanly), and a deliberate edit conflict.
    //
    // Cut A's path to S3 *before* making them: a FUSE write only journals
    // locally and returns immediately, but the sync task's background
    // ship is nudged right away too, on its own tokio worker thread — with
    // nothing to stop it, it can (and, empirically, about half the time
    // did) win the race against the `pause()` call a few lines down,
    // landing this "stranded" segment in S3 as ordinary history *before*
    // SIGSTOP actually freezes the process. That defeats the scenario's
    // premise (these records must still be sitting in A's local journal,
    // unshipped, when A is deposed) without technically breaking
    // anything — the log stays perfectly ordered either way — so it
    // never produced a wrong-data failure, only an intermittent one
    // further down where the recovery legitimately finds nothing left
    // to replay. The cut removes the race outright: with S3 unreachable
    // the ship cannot possibly succeed, so the records are guaranteed to
    // still be local when A is frozen a moment later.
    a_s3.cut();
    std::fs::write(c0.mnt.join("shared/clean-from-a"), b"clean")
        .context("A's stranded create of clean-from-a")?;
    std::fs::write(c0.mnt.join("shared/a-only"), b"stranded-from-a")
        .context("A's stranded overwrite of a-only")?;
    std::fs::write(c0.mnt.join("shared/same"), b"loser-from-a")
        .context("A's stranded overwrite of same")?;
    let a_before = c0.control_status()?;
    anyhow::ensure!(
        a_before["spool"]["journal_backlog"].as_u64().unwrap_or(0) > 0,
        "A did not retain stranded records: {}",
        a_before["spool"]
    );
    // Plan 30 §M3b holder capture: the holder's own unshipped
    // transactions are speculation with before-images — what the
    // deposition recovery below rolls back with.
    anyhow::ensure!(
        a_before["speculation"]["local"].as_u64().unwrap_or(0) > 0,
        "A's unshipped writes were not captured as local speculation: {}",
        a_before["speculation"]
    );
    c0.pause()?;
    // Safe to restore now: A cannot act on it while stopped, and B needs
    // it to take over below.
    a_s3.heal();

    std::thread::sleep(Duration::from_millis(6500));
    std::fs::write(c1.mnt.join("shared/same"), b"winner-from-b")
        .context("B takes over expired lease")?;
    eventually("B winner is durable", Duration::from_secs(15), || {
        anyhow::ensure!(std::fs::read(c1.mnt.join("shared/same"))? == b"winner-from-b");
        anyhow::ensure!(c1.control_status()?["spool"]["journal_backlog"].as_u64() == Some(0));
        let lease = lease_of(&c1)?;
        anyhow::ensure!(
            lease["held"] == true && lease["epoch"].as_u64().unwrap_or(0) > a_epoch,
            "B must hold a newer epoch than A's {a_epoch}: {lease}"
        );
        Ok(())
    })?;

    // No `reintegrate` call from here on: A notices the deposition by
    // itself (its renewal finds B's lease, or it tails B's epoch marker),
    // rolls back and queues its ops, and the replay drain (every 250 ms)
    // forwards them to B. `lost` is not polled for `true`: the recovery
    // clears it within one sync round, so a `true` sample is a race.
    c0.resume()?;
    eventually(
        "A recovers from the deposition and drains its replay queue",
        Duration::from_secs(60),
        || {
            let status = c0.control_status()?;
            let spec = &status["speculation"];
            anyhow::ensure!(
                spec["depositions"].as_u64().unwrap_or(0) >= 1
                    && spec["local_rolled_back"].as_u64().unwrap_or(0) >= 1,
                "A has not run a deposition recovery yet: {spec}"
            );
            anyhow::ensure!(
                status["lease"]["lost"] == false,
                "the recovery must clear `lost`: {}",
                status["lease"]
            );
            anyhow::ensure!(
                spec["pending_replay"].as_u64() == Some(0)
                    && spec["outstanding"].as_u64() == Some(0),
                "A is still replaying: {spec}"
            );
            anyhow::ensure!(
                spec["replay_conflicts"].as_u64().unwrap_or(0) >= 1,
                "A's replay of `same` must have been refused and materialized: {spec}"
            );
            Ok(())
        },
    )
    .with_context(|| format!("--- c0 log ---\n{}", c0.tail_log_n(80)))?;

    eventually(
        "clean replays and the one conflict copy converge on both nodes",
        Duration::from_secs(40),
        || {
            for client in [&c0, &c1] {
                let current = std::fs::read(client.mnt.join("shared/same"))?;
                anyhow::ensure!(
                    current == b"winner-from-b",
                    "{} kept wrong winner {:?}; status {}; log:\n{}",
                    client.name,
                    String::from_utf8_lossy(&current),
                    client.control_status()?,
                    client.tail_log_n(80)
                );
                anyhow::ensure!(
                    std::fs::read(client.mnt.join("shared/clean-from-a"))? == b"clean",
                    "{}: clean-from-a did not replay",
                    client.name
                );
                let a_only = std::fs::read(client.mnt.join("shared/a-only"))?;
                anyhow::ensure!(
                    a_only == b"stranded-from-a",
                    "{}: a-only (never touched by B) did not replay A's content: holds {:?}; status {}",
                    client.name,
                    String::from_utf8_lossy(&a_only),
                    client.control_status()?
                );
                // Conflicts only for true overlaps: exactly one copy, for
                // `same` — none for `clean-from-a` or `a-only`.
                let dir = client.mnt.join("shared/.constellation-conflict");
                let entries: Vec<std::fs::DirEntry> =
                    std::fs::read_dir(&dir)?.collect::<std::io::Result<Vec<_>>>()?;
                let names: Vec<String> = entries
                    .iter()
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .collect();
                anyhow::ensure!(
                    entries.len() == 1 && names[0].starts_with("same@"),
                    "{} must hold exactly one conflict copy, same@<node>-<ts>-<seq>; found {names:?}",
                    client.name
                );
                anyhow::ensure!(
                    std::fs::read(entries[0].path())? == b"loser-from-a",
                    "{}: {} does not carry A's losing bytes",
                    client.name,
                    names[0]
                );
            }
            let status = c0.control_status()?;
            anyhow::ensure!(
                status["reintegration"]["conflicts_materialized"]
                    .as_u64()
                    .unwrap_or(0)
                    >= 1,
                "conflicts_materialized must mirror replay_conflicts: {}",
                status["reintegration"]
            );
            Ok(())
        },
    )?;
    eprintln!(
        "    deposed-reintegration: c0 speculation after recovery: {}",
        speculation_of(&c0)?
    );
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Plan 30 §M9's side of `deposed-reintegration`, in the default
/// configuration (A's LAN peer B is its synchronous backup). A holds;
/// its ship rounds are held (the sync-hold fault: S3 stays reachable,
/// nothing A journals ships), and it makes the same three edits —
/// `clean-from-a`, an overwrite of `a-only`, an overwrite of `same` —
/// which are acknowledged once B has them. A is then frozen. B, its
/// backup, seals A's epoch and takes the lease over well inside the TTL
/// (20 s here), with A's journal adopted from its backup tail: nothing
/// strands. B overwrites `same`. Resumed, A is deposed and finds every
/// op of its queue completed in the log, so no conflict copy is made;
/// both nodes converge on A's two files and B's `same`. Finally A is
/// killed and B re-reads everything with an empty cache: the adopted
/// records name chunks that are in the bucket.
///
/// Why a hold and not `deposed-reintegration`'s S3 cut: a holder cut
/// from S3 with a live LAN peer forms a continuation epoch with it
/// within a round interval (plan 30 §M10), and a member backup does not
/// seal while its epoch is open — see PROGRESS, "Fix:
/// deposed-reintegration regression", for what that does when the
/// holder is then lost.
fn deposed_reintegration_backup(_seed: u64) -> Result<()> {
    const NAME: &str = "deposed-reintegration-backup";
    let (env, root) = setup(NAME)?;
    let _route = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/reintegrate-backup-{}", ts());
    let hold_dir = tempfile::tempdir()?;
    let hold = hold_dir.path().join("hold-a");
    let held = hold_dir.path().join("hold-a.held");
    let tune = |client: Client| {
        client
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", "20000")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "60000")
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
    };
    let mut c0 = tune(Client::new(root.path(), "c0", &env.endpoint, &backend)?).with_env(
        "CONSTELLATION_FAULT_HOLD_SYNC_FILE",
        hold.to_str().context("hold path")?,
    );
    let mut c1 = tune(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    let mut paused = false;
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&c0, &c1])?;
        let b_id = c1.control_status()?["node_id"]
            .as_u64()
            .context("B reports no node id")?;
        std::fs::create_dir(c0.mnt.join("shared"))?;
        std::fs::write(c0.mnt.join("shared/same"), b"baseline")?;
        std::fs::write(c0.mnt.join("shared/a-only"), b"baseline-a")?;
        eventually("baseline visible on B", Duration::from_secs(30), || {
            anyhow::ensure!(std::fs::read(c1.mnt.join("shared/same"))? == b"baseline");
            anyhow::ensure!(std::fs::read(c1.mnt.join("shared/a-only"))? == b"baseline-a");
            Ok(())
        })?;
        eventually("A lists B as its backup", Duration::from_secs(30), || {
            let ack = c0.control_status()?["ack"].clone();
            let backups: Vec<u64> = ack["backups"]
                .as_array()
                .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
                .unwrap_or_default();
            anyhow::ensure!(
                ack["policy"] == "backup" && backups == [b_id],
                "A's backup set is not [B]: {ack}"
            );
            Ok(())
        })?;
        let a_epoch = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

        std::fs::write(&hold, b"hold")?;
        eventually(
            "A's round observes the hold",
            Duration::from_secs(20),
            || {
                anyhow::ensure!(held.is_file(), "no round has observed the hold yet");
                Ok(())
            },
        )?;
        for (name, bytes) in [
            ("clean-from-a", &b"clean"[..]),
            ("a-only", &b"stranded-from-a"[..]),
            ("same", &b"loser-from-a"[..]),
        ] {
            std::fs::write(c0.mnt.join("shared").join(name), bytes)
                .with_context(|| format!("A's write of {name}"))?;
        }
        let spool = c0.control_status()?["spool"].clone();
        anyhow::ensure!(
            spool["journal_backlog"].as_u64().unwrap_or(0) > 0,
            "A shipped its edits although its rounds are held: {spool}"
        );
        c0.pause()?;
        paused = true;
        let frozen = std::time::Instant::now();

        eventually(
            "B seals A's epoch and takes the lease over",
            Duration::from_secs(10),
            || {
                let lease = lease_of(&c1)?;
                anyhow::ensure!(
                    lease["held"] == true && lease["epoch"].as_u64().unwrap_or(0) > a_epoch,
                    "B does not hold a newer epoch than A's {a_epoch} yet: {lease}"
                );
                Ok(())
            },
        )?;
        let took = frozen.elapsed();
        let ack = c1.control_status()?["ack"].clone();
        eprintln!(
            "    {NAME}: B took over {took:?} after A froze (TTL 20 s); seals {} takeovers {} \
             tail-applied {}",
            ack["seals"], ack["backup_takeovers"], ack["backup_tail_applied"]
        );
        anyhow::ensure!(
            ack["seals"].as_u64().unwrap_or(0) >= 1,
            "B took over without sealing (by expiry?): {ack}"
        );
        // A's acknowledged edits are B's once its takeover gate has
        // applied the backup tail — before A is back. (Not linearizable
        // with A's acknowledgements: B's own session never observed them,
        // and a read during the gate may still miss them.)
        eventually(
            "B has A's acknowledged edits while A is frozen",
            Duration::from_secs(10),
            || {
                for (name, bytes) in [
                    ("clean-from-a", &b"clean"[..]),
                    ("a-only", &b"stranded-from-a"[..]),
                    ("same", &b"loser-from-a"[..]),
                ] {
                    let got = std::fs::read(c1.mnt.join("shared").join(name))
                        .with_context(|| format!("B reads A's {name}"))?;
                    anyhow::ensure!(
                        got == bytes,
                        "B has {name} as {:?}",
                        String::from_utf8_lossy(&got)
                    );
                }
                Ok(())
            },
        )?;
        let ack = c1.control_status()?["ack"].clone();
        anyhow::ensure!(
            ack["backup_tail_applied"].as_u64().unwrap_or(0) >= 1,
            "B has A's edits but applied no backup tail: {ack}"
        );
        std::fs::write(c1.mnt.join("shared/same"), b"winner-from-b")
            .context("B's overwrite of same")?;

        let _ = std::fs::remove_file(&hold);
        c0.resume()?;
        paused = false;
        let expect: [(&str, &[u8]); 3] = [
            ("clean-from-a", b"clean"),
            ("a-only", b"stranded-from-a"),
            ("same", b"winner-from-b"),
        ];
        eventually(
            "A recovers and both nodes converge with no conflict copy",
            Duration::from_secs(60),
            || {
                let spec = speculation_of(&c0)?;
                anyhow::ensure!(
                    spec["depositions"].as_u64().unwrap_or(0) >= 1,
                    "A has not noticed the deposition yet: {spec}"
                );
                anyhow::ensure!(
                    spec["pending_replay"].as_u64() == Some(0)
                        && spec["outstanding"].as_u64() == Some(0),
                    "A is still recovering: {spec}"
                );
                for client in [&c0, &c1] {
                    for (name, bytes) in expect {
                        let got = std::fs::read(client.mnt.join("shared").join(name))
                            .with_context(|| format!("{} reads {name}", client.name))?;
                        anyhow::ensure!(
                            got == bytes,
                            "{}: {name} is {:?}",
                            client.name,
                            String::from_utf8_lossy(&got)
                        );
                    }
                    let copies =
                        std::fs::read_dir(client.mnt.join("shared/.constellation-conflict"))
                            .map(|d| d.count())
                            .unwrap_or(0);
                    anyhow::ensure!(
                        copies == 0,
                        "{}: {copies} conflict copies, but nothing stranded",
                        client.name
                    );
                }
                Ok(())
            },
        )
        .with_context(|| format!("--- c0 log ---\n{}", c0.tail_log_n(40)))?;
        let spec = speculation_of(&c0)?;
        eprintln!("    {NAME}: A after recovery: {spec}");
        anyhow::ensure!(
            spec["replay_conflicts"].as_u64().unwrap_or(0) == 0,
            "A materialized a conflict although B adopted its writes: {spec}"
        );

        // Durability without A.
        c0.kill9()?;
        c1.unmount()?;
        c1.drop_cache()?;
        c1.mount()?;
        for (name, bytes) in expect {
            let got = std::fs::read(c1.mnt.join("shared").join(name))
                .with_context(|| format!("B re-reads {name} from S3 with A gone"))?;
            anyhow::ensure!(
                got == bytes,
                "B re-reads {name} as {:?}",
                String::from_utf8_lossy(&got)
            );
        }
        Ok(())
    })();
    if paused {
        let _ = c0.resume();
    }
    let _ = std::fs::remove_file(&hold);
    let unmounted = c1.unmount();
    let _ = c0.unmount();
    result?;
    unmounted
}

/// Plan 30 §M9 × §M4: a backup takeover never publishes a manifest whose
/// chunks the bucket lacks. A holds (write-back mode) with B as its M9
/// backup; A alone is cut from S3 (its own switch — B declines the
/// continuation epoch A proposes, since B reaches S3). Then:
///
/// - a write-through close on A returns an error: its upload failed, so
///   the write was never acknowledged (the manifest row, already backed
///   up, is not lost data: it is held like the next one);
/// - a write-back close on A succeeds at once: acknowledged, its bytes
///   only on A;
/// - A freezes; B seals and takes the lease over, adopting both manifests
///   from its backup tail. Neither ships: B's upload pass finds the
///   chunks neither in S3 nor in its cache, and holds them (`status` →
///   `held`, the path listed). A fresh node sees both files as they were
///   before the manifests — created, empty — never a manifest naming a
///   missing chunk.
///
/// `returns`: A comes back (S3 healed), is deposed, uploads its pending
/// chunks; B's next pass finds them in S3, the held records ship, and
/// every node — a fresh one included — reads A's bytes; no conflict copy.
/// Otherwise A is killed for good and the operator drops the held record
/// on B (`repair drop-held`): a conflict copy with the lost chunk as a
/// hole, the file itself as the log had it.
fn backup_takeover_held_chunks(name: &str, returns: bool) -> Result<()> {
    let (env, root) = setup(name)?;
    let _route = env.s3_proxy()?;
    let a_s3 = env.counting_proxy()?;
    let backend = format!("s3://{BUCKET}/{name}-{}", ts());
    let tune = |client: Client| {
        client
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", "20000")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "60000")
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
    };
    // A's chunks must stay unreachable: no handing them to B, which
    // reaches S3 and would upload them (the EC2 fix's chunk handoff —
    // `s3-cut-one-node` covers that path).
    let mut c0 = tune(Client::new(root.path(), "c0", &a_s3.endpoint(), &backend)?)
        .with_write_mode("back")
        .with_env("CONSTELLATION_CHUNK_HANDOFF_AFTER_MS", "0");
    let mut c1 = tune(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    let mut paused = false;
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&c0, &c1])?;
        let b_id = c1.control_status()?["node_id"]
            .as_u64()
            .context("B reports no node id")?;
        std::fs::create_dir(c0.mnt.join("shared"))?;
        std::fs::write(c0.mnt.join("shared/keep"), b"baseline")?;
        eventually("baseline on B", Duration::from_secs(30), || {
            anyhow::ensure!(std::fs::read(c1.mnt.join("shared/keep"))? == b"baseline");
            Ok(())
        })?;
        eventually("A lists B as its backup", Duration::from_secs(30), || {
            let ack = c0.control_status()?["ack"].clone();
            let backups: Vec<u64> = ack["backups"]
                .as_array()
                .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
                .unwrap_or_default();
            anyhow::ensure!(
                ack["policy"] == "backup" && backups == [b_id],
                "A's backup set is not [B]: {ack}"
            );
            Ok(())
        })?;
        let a_epoch = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

        let close_after_write = |path: std::path::PathBuf, bytes: &[u8]| -> std::io::Result<()> {
            use std::io::Write;
            use std::os::fd::IntoRawFd;
            let mut f = std::fs::File::create(path)?;
            f.write_all(bytes)?;
            let fd = f.into_raw_fd();
            // SAFETY: `fd` is ours, just taken out of the `File`.
            if unsafe { libc::close(fd) } == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        };
        a_s3.cut();
        c0.set_write_mode("through")?;
        let wt = close_after_write(c0.mnt.join("shared/wt"), b"write-through, S3 cut");
        eprintln!("    {name}: A's write-through close with S3 cut: {wt:?}");
        anyhow::ensure!(
            wt.is_err(),
            "a write-through close succeeded although its chunk cannot reach S3"
        );
        c0.set_write_mode("back")?;
        let wb_bytes = b"write-back, acknowledged, only on A".to_vec();
        close_after_write(c0.mnt.join("shared/wb"), &wb_bytes)
            .context("A's write-back close (acknowledged at once)")?;
        let pending = c0.control_status()?["writeback"]["pending_uploads"]
            .as_u64()
            .unwrap_or(0);
        anyhow::ensure!(
            pending >= 2,
            "A has {pending} pending uploads, expected both files'"
        );
        c0.pause()?;
        paused = true;

        eventually("B seals and takes over", Duration::from_secs(15), || {
            let lease = lease_of(&c1)?;
            anyhow::ensure!(
                lease["held"] == true && lease["epoch"].as_u64().unwrap_or(0) > a_epoch,
                "B does not hold a newer epoch yet: {lease}"
            );
            let ack = c1.control_status()?["ack"].clone();
            anyhow::ensure!(
                ack["backup_tail_applied"].as_u64().unwrap_or(0) >= 1,
                "no tail adopted: {ack}"
            );
            Ok(())
        })?;
        let epoch = c1.control_status()?["epoch"].clone();
        anyhow::ensure!(
            epoch["active"] != true && epoch["frozen"] != true,
            "B joined a continuation epoch although it reaches S3: {epoch}"
        );
        let mut held_inos = Vec::new();
        eventually(
            "B holds both adopted manifests",
            Duration::from_secs(30),
            || {
                let held = c1.control_status()?["held"].clone();
                let paths: Vec<String> = held["inodes"]
                    .as_array()
                    .map(|v| {
                        v.iter()
                            .filter_map(|i| i["path"].as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                anyhow::ensure!(
                    paths.iter().any(|p| p.ends_with("wb"))
                        && paths.iter().any(|p| p.ends_with("wt")),
                    "not both held yet: {held}"
                );
                held_inos = held["inodes"]
                    .as_array()
                    .map(|v| v.iter().filter_map(|i| i["ino"].as_u64()).collect())
                    .unwrap_or_default();
                eprintln!("    {name}: B holds {held}");
                Ok(())
            },
        )?;
        // What the bucket says (commits and log): the files exist, empty.
        let mut d = Client::new(root.path(), "d", &env.endpoint, &backend)?.with_own_node_key();
        d.mount()?;
        let seen = eventually("D sees the files' creates", Duration::from_secs(30), || {
            for f in ["wb", "wt"] {
                let data = std::fs::read(d.mnt.join("shared").join(f))
                    .with_context(|| format!("D reads {f}"))?;
                anyhow::ensure!(data.is_empty(), "D sees {f} as {} bytes", data.len());
            }
            Ok(())
        });
        d.unmount()?;
        seen?;

        if returns {
            a_s3.heal();
            c0.resume()?;
            paused = false;
            eventually(
                "A uploads, B's held records ship, everyone reads A's bytes",
                Duration::from_secs(90),
                || {
                    let held = c1.control_status()?["held"].clone();
                    anyhow::ensure!(
                        held["transactions"].as_u64() == Some(0),
                        "B still holds: {held}"
                    );
                    for c in [&c0, &c1] {
                        anyhow::ensure!(
                            std::fs::read(c.mnt.join("shared/wb"))? == wb_bytes,
                            "{} reads other wb bytes",
                            c.name
                        );
                        anyhow::ensure!(
                            !c.mnt.join("shared/.constellation-conflict").exists(),
                            "{}: a conflict copy for an adopted write",
                            c.name
                        );
                    }
                    Ok(())
                },
            )
            .with_context(|| format!("--- c1 log ---\n{}", c1.tail_log_n(40)))?;
            let mut d =
                Client::new(root.path(), "d2", &env.endpoint, &backend)?.with_own_node_key();
            d.mount()?;
            let fresh = eventually("a fresh node reads wb", Duration::from_secs(60), || {
                anyhow::ensure!(std::fs::read(d.mnt.join("shared/wb"))? == wb_bytes);
                Ok(())
            });
            d.unmount()?;
            fresh?;
        } else {
            c0.kill9()?;
            paused = false;
            for ino in &held_inos {
                c1.control("locks.drop_held", serde_json::json!({ "ino": ino }))
                    .with_context(|| format!("drop-held {ino} failed"))?;
            }
            eventually(
                "the conflict copies land and the held set drains",
                Duration::from_secs(60),
                || {
                    let status = c1.control_status()?;
                    anyhow::ensure!(
                        status["held"]["transactions"].as_u64() == Some(0),
                        "still held: {}",
                        status["held"]
                    );
                    let dir = c1.mnt.join("shared/.constellation-conflict");
                    let names: Vec<String> = std::fs::read_dir(&dir)?
                        .filter_map(|e| e.ok())
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .collect();
                    let copy = names
                        .iter()
                        .find(|n| n.starts_with("wb@"))
                        .with_context(|| format!("no wb@ copy yet: {names:?}"))?;
                    let data = std::fs::read(dir.join(copy))?;
                    anyhow::ensure!(
                        data.len() == wb_bytes.len() && data.iter().all(|b| *b == 0),
                        "the conflict copy keeps the length with the lost chunk as a hole"
                    );
                    anyhow::ensure!(
                        std::fs::read(c1.mnt.join("shared/wb"))?.is_empty(),
                        "wb is not as the log had it"
                    );
                    Ok(())
                },
            )?;
        }
        Ok(())
    })();
    if paused {
        let _ = c0.resume();
    }
    a_s3.heal();
    let unmounted = c1.unmount();
    if returns {
        let _ = c0.unmount();
    }
    result?;
    unmounted
}

fn backup_takeover_holds_missing_chunks(_seed: u64) -> Result<()> {
    backup_takeover_held_chunks("backup-takeover-holds-missing-chunks", true)
}

fn backup_takeover_drops_held_chunks(_seed: u64) -> Result<()> {
    backup_takeover_held_chunks("backup-takeover-drops-held-chunks", false)
}

/// Plan 30 §M10's member rule: only the holder loses S3 (its own switch;
/// P2P stays up). It proposes a continuation epoch; its peer B reaches S3
/// and declines. Then A dies (frozen): B — its M9 backup — seals and
/// takes the lease over within seconds, and B's own writes succeed. Before
/// the rule B joined the epoch, froze with it when A died, and answered
/// `EIO` then `EROFS` for as long as A was away, with S3 reachable the
/// whole time.
fn epoch_peer_reaching_s3_declines(_seed: u64) -> Result<()> {
    const NAME: &str = "epoch-peer-reaching-s3-declines";
    let (env, root) = setup(NAME)?;
    let _route = env.s3_proxy()?;
    let a_s3 = env.counting_proxy()?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let tune = |client: Client| {
        client
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", "20000")
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
    };
    let mut c0 = tune(Client::new(root.path(), "c0", &a_s3.endpoint(), &backend)?);
    let mut c1 = tune(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    let mut paused = false;
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&c0, &c1])?;
        std::fs::create_dir(c0.mnt.join("shared"))?;
        eventually("shared on B", Duration::from_secs(30), || {
            anyhow::ensure!(c1.mnt.join("shared").is_dir());
            Ok(())
        })?;
        let b_id = c1.control_status()?["node_id"]
            .as_u64()
            .context("B reports no node id")?;
        eventually("A lists B as its backup", Duration::from_secs(30), || {
            let ack = c0.control_status()?["ack"].clone();
            let backups: Vec<u64> = ack["backups"]
                .as_array()
                .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
                .unwrap_or_default();
            anyhow::ensure!(
                ack["policy"] == "backup" && backups == [b_id],
                "A's backup set is not [B]: {ack}"
            );
            Ok(())
        })?;
        let a_epoch = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);
        a_s3.cut();
        // EC2 follow-up 3c: A's rounds fail, but B answers A's liveness
        // ping saying its own S3 works: A's outage is its own, not the
        // bucket's, and A proposes no epoch at all (before, it proposed —
        // holding its epoch open, no S3 acquisition, while B declined —
        // every backoff). B's decline stays the second line of defence
        // (`handle_propose_checked`'s unit tests).
        eventually(
            "A sees its S3 outage is its own",
            Duration::from_secs(20),
            || {
                let e = c0.control_status()?["epoch"].clone();
                anyhow::ensure!(e["own_s3_outage"] == true, "not yet: {e}");
                Ok(())
            },
        )?;
        std::thread::sleep(Duration::from_secs(3));
        let a_e = c0.control_status()?["epoch"].clone();
        let proposed = c0.log_text().contains("epoch propose not acked")
            || c0.log_text().contains("continuation epoch active")
            || a_e["proposals"].as_u64().unwrap_or(0) > 0;
        let declined = c1.log_text().contains("not a bucket outage");
        eprintln!("    {NAME}: A proposed: {proposed}; B declined: {declined}; A epoch {a_e}");
        anyhow::ensure!(
            !proposed && !declined,
            "A proposed a continuation epoch though B reaches S3: {a_e}"
        );
        c0.pause()?;
        paused = true;
        let frozen = std::time::Instant::now();
        let mut attempts = Vec::new();
        eventually("B writes again", Duration::from_secs(30), || {
            let r = std::fs::write(c1.mnt.join("shared/after"), b"B writes");
            attempts.push(format!(
                "{:?}: {:?}",
                frozen.elapsed(),
                r.as_ref().map_err(|e| e.raw_os_error())
            ));
            r?;
            let lease = lease_of(&c1)?;
            anyhow::ensure!(
                lease["held"] == true && lease["epoch"].as_u64().unwrap_or(0) > a_epoch,
                "B does not hold: {lease}"
            );
            Ok(())
        })
        .with_context(|| format!("B's attempts: {attempts:?}"))?;
        let took = frozen.elapsed();
        eprintln!(
            "    {NAME}: B writes {took:?} after A froze (TTL 20 s); attempts {}",
            attempts.len()
        );
        anyhow::ensure!(
            took < Duration::from_secs(10),
            "B took {took:?} to write after A froze"
        );
        Ok(())
    })();
    if paused {
        let _ = c0.resume();
    }
    a_s3.heal();
    let unmounted = c1.unmount();
    let _ = c0.unmount();
    result?;
    unmounted
}

/// A truncate never resurrects the bytes it cut. Every shape runs on two
/// nodes — A holds the lease, B forwards — and each file is checked on
/// both, then again after both remount with an empty chunk cache (so the
/// content comes from the committed manifests in S3), and on a fresh
/// node:
///
/// - write `abcdefgh`, reopen, `ftruncate(2)`, write `X` at 5 — on the
///   holder and on a forwarding node;
/// - a truncate on one node (committed as `setattr`), then the write past
///   the gap on the other node;
/// - `O_TRUNC` then a write past a gap;
/// - truncate down, then truncate up; and then `fallocate` up.
///
/// Expected everywhere: the cut bytes read as zeros.
fn truncate_never_resurrects(_seed: u64) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;
    const NAME: &str = "truncate-never-resurrects";
    let (env, root) = setup(NAME)?;
    let _route = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut a = Client::new(root.path(), "a", &env.endpoint, &backend)?
        .with_own_node_key()
        .with_env("CONSTELLATION_LEASE_PLACEMENT", "off");
    let mut b = Client::new(root.path(), "b", &env.endpoint, &backend)?
        .with_own_node_key()
        .with_env("CONSTELLATION_LEASE_PLACEMENT", "off");
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;
    std::fs::create_dir(a.mnt.join("t"))?;
    eventually("t on B", Duration::from_secs(30), || {
        anyhow::ensure!(b.mnt.join("t").is_dir());
        Ok(())
    })?;
    let gap = |head: &[u8], at: usize, tail: &[u8]| -> Vec<u8> {
        let mut v = head.to_vec();
        v.resize(at, 0);
        v.extend_from_slice(tail);
        v
    };
    let rw = |p: std::path::PathBuf| std::fs::OpenOptions::new().read(true).write(true).open(p);
    let mut expect: Vec<(String, Vec<u8>)> = Vec::new();

    // 1. ftruncate then a write past the gap, on each node.
    for (who, c) in [("a", &a), ("b", &b)] {
        let name = format!("t/ftrunc-{who}");
        std::fs::write(c.mnt.join(&name), b"abcdefgh")?;
        let mut f = rw(c.mnt.join(&name))?;
        f.set_len(2)?;
        f.seek(SeekFrom::Start(5))?;
        f.write_all(b"X")?;
        drop(f);
        expect.push((name, gap(b"ab", 5, b"X")));
    }
    // 2. The truncate on B, the write past the gap on A.
    std::fs::write(a.mnt.join("t/cross"), b"abcdefghijklmnop")?;
    eventually("cross on B", Duration::from_secs(30), || {
        anyhow::ensure!(std::fs::read(b.mnt.join("t/cross"))? == b"abcdefghijklmnop");
        Ok(())
    })?;
    rw(b.mnt.join("t/cross"))?.set_len(3)?;
    eventually("B's truncate on A", Duration::from_secs(30), || {
        anyhow::ensure!(std::fs::metadata(a.mnt.join("t/cross"))?.len() == 3);
        Ok(())
    })?;
    {
        let mut f = rw(a.mnt.join("t/cross"))?;
        f.seek(SeekFrom::Start(10))?;
        f.write_all(b"Y")?;
    }
    expect.push(("t/cross".into(), gap(b"abc", 10, b"Y")));
    // 3. O_TRUNC, then a write past a gap (on B).
    std::fs::write(b.mnt.join("t/otrunc"), b"0123456789")?;
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(b.mnt.join("t/otrunc"))?;
        f.seek(SeekFrom::Start(5))?;
        f.write_all(b"Z")?;
    }
    expect.push(("t/otrunc".into(), gap(b"", 5, b"Z")));
    // 4. Truncate down then up; then fallocate up (on A).
    std::fs::write(a.mnt.join("t/updown"), b"abcdefgh")?;
    {
        let f = rw(a.mnt.join("t/updown"))?;
        f.set_len(2)?;
        f.set_len(8)?;
    }
    expect.push(("t/updown".into(), gap(b"ab", 8, b"")));
    std::fs::write(a.mnt.join("t/falloc"), b"abcdefgh")?;
    {
        let f = rw(a.mnt.join("t/falloc"))?;
        f.set_len(2)?;
        // SAFETY: a plain `fallocate(2)` on an fd we own.
        let r = unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, 8) };
        anyhow::ensure!(r == 0, "fallocate: {}", std::io::Error::last_os_error());
    }
    expect.push(("t/falloc".into(), gap(b"ab", 8, b"")));

    let verify = |c: &Client, when: &str| -> Result<()> {
        eventually(
            &format!("{} reads every file ({when})", c.name),
            Duration::from_secs(30),
            || {
                for (name, want) in &expect {
                    let got = std::fs::read(c.mnt.join(name))
                        .with_context(|| format!("{} reads {name}", c.name))?;
                    anyhow::ensure!(
                        &got == want,
                        "{} ({when}): {name} is {:?}, want {:?}",
                        c.name,
                        String::from_utf8_lossy(&got),
                        String::from_utf8_lossy(want)
                    );
                }
                Ok(())
            },
        )
    };
    verify(&a, "live")?;
    verify(&b, "live")?;
    b.unmount()?;
    a.unmount()?;
    a.drop_cache()?;
    b.drop_cache()?;
    a.mount()?;
    b.mount()?;
    verify(&a, "remounted, cold cache")?;
    verify(&b, "remounted, cold cache")?;
    let mut d = Client::new(root.path(), "d", &env.endpoint, &backend)?.with_own_node_key();
    d.mount()?;
    let fresh = verify(&d, "fresh node");
    d.unmount()?;
    fresh?;
    b.unmount()?;
    a.unmount()?;
    Ok(())
}

/// Phase 4c: unmount ≠ leave. With C merely unmounted, A+B cannot open
/// a continuation epoch (C still write-eligible). After C self-leaves —
/// or after admin leave of an unmounted C — A+B can.
fn node_leave(_seed: u64) -> Result<()> {
    let (env, root) = setup("node-leave")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/leave-{}", ts());
    let tune = |client: Client, key: &str| {
        client
            .with_env("CONSTELLATION_NODE_KEY", key)
            .with_env("CONSTELLATION_LEASE_TTL_MS", "5000")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
    };
    let mut c0 = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "leave-c0"),
    );
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "leave-c1"),
    );
    let mut c2 = tune(
        Client::new(root.path(), "c2", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "leave-c2"),
    );
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    c2.mount()?;
    for c in [&c0, &c1, &c2] {
        eventually(
            &format!("{} reports live P2P peers", c.name),
            Duration::from_secs(30),
            || {
                let p = p2p_of(c)?;
                anyhow::ensure!(p["enabled"] == true);
                anyhow::ensure!(node_peers(&p).count() >= 2);
                Ok(())
            },
        )?;
    }

    std::fs::create_dir(c0.mnt.join("a")).context("A mkdir a")?;
    std::fs::create_dir(c1.mnt.join("b")).context("B mkdir b")?;
    eventually("dirs visible on C", Duration::from_secs(20), || {
        anyhow::ensure!(c2.mnt.join("a").is_dir());
        anyhow::ensure!(c2.mnt.join("b").is_dir());
        Ok(())
    })?;

    let c2_id = c2.control_status()?["node_id"]
        .as_u64()
        .context("C has no node_id")?;

    // --- Half 1: unmount alone must NOT unlock epochs ---
    c2.unmount()?;
    // Give A/B a roster refresh cycle while C is still enrolled.
    std::thread::sleep(Duration::from_secs(6));
    proxy.cut()?;
    std::thread::sleep(Duration::from_secs(8));
    for c in [&c0, &c1] {
        let status = c.control_status()?;
        anyhow::ensure!(
            status["epoch"]["active"] != true,
            "{} opened an epoch while unmounted C was still enrolled: {}",
            c.name,
            status["epoch"]
        );
    }
    proxy.heal()?;
    // Let A/B re-sync with S3 before admin-leave.
    std::thread::sleep(Duration::from_secs(3));

    // Admin-retire the unmounted C from A.
    c0.leave(Some(c2_id), true)?;
    eventually(
        "A and B see a two-node roster after admin leave",
        Duration::from_secs(20),
        || {
            // Peers list omits retired C; wait a refresh.
            for c in [&c0, &c1] {
                let p = p2p_of(c)?;
                let peers = p["peers"].as_array().cloned().unwrap_or_default();
                anyhow::ensure!(
                    peers.iter().all(|p| p["node_id"].as_u64() != Some(c2_id)),
                    "{} still lists retired C: {p}",
                    c.name
                );
            }
            Ok(())
        },
    )?;

    proxy.cut()?;
    wait_for_epoch([&c0, &c1])?;
    std::fs::write(c0.mnt.join("a/after-leave"), b"ok")
        .context("A writes a/after-leave in the post-leave epoch")?;
    std::fs::write(c1.mnt.join("b/after-leave"), b"ok")
        .context("B writes b/after-leave in the post-leave epoch")?;
    proxy.heal()?;
    eventually(
        "post-leave epoch writes converge",
        Duration::from_secs(40),
        || {
            anyhow::ensure!(std::fs::read(c0.mnt.join("b/after-leave"))? == b"ok");
            anyhow::ensure!(std::fs::read(c1.mnt.join("a/after-leave"))? == b"ok");
            Ok(())
        },
    )?;
    ensure_no_conflicts(&[&c0, &c1])?;

    // --- Half 2: self-leave of a live third writer ---
    // Remount C under a *fresh* state dir so it claims a new id, then leave.
    let mut c3 = tune(
        Client::new(root.path(), "c3", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "leave-c3"),
    );
    c3.mount()?;
    eventually("C3 peers with A+B", Duration::from_secs(30), || {
        let p = p2p_of(&c3)?;
        anyhow::ensure!(node_peers(&p).count() >= 2);
        Ok(())
    })?;
    let c3_id = c3.control_status()?["node_id"].as_u64().unwrap();
    c3.leave(None, false)?;
    anyhow::ensure!(!c3.is_mounted(), "self-leave must unmount");
    eventually("A sees C3 gone from peers", Duration::from_secs(20), || {
        let p = p2p_of(&c0)?;
        let peers = p["peers"].as_array().cloned().unwrap_or_default();
        anyhow::ensure!(peers.iter().all(|p| p["node_id"].as_u64() != Some(c3_id)));
        Ok(())
    })?;
    proxy.cut()?;
    wait_for_epoch([&c0, &c1])?;
    proxy.heal()?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

fn two_clients_disjoint(seed: u64) -> Result<()> {
    let (env, root) = setup("two-clients")?;
    let _proxy = env.s3_proxy()?;
    let t = ts();
    let b0 = format!("s3://{BUCKET}/two-{t}/a");
    let b1 = format!("s3://{BUCKET}/two-{t}/b");
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &b0)?;
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &b1)?;
    c0.fs_create()?;
    c1.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    let mut m0 = Model::default();
    let mut m1 = Model::default();
    let mut w0 = Workload::new(seed, "a");
    let mut w1 = Workload::new(seed.wrapping_add(1), "b");
    for block in 0..3 {
        w0.run_block(&c0.mnt, &mut m0, 40)?;
        w1.run_block(&c1.mnt, &mut m1, 40)?;
        m0.verify(&c0.mnt)
            .with_context(|| format!("c0 block {block}"))?;
        m1.verify(&c1.mnt)
            .with_context(|| format!("c1 block {block}"))?;
    }
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// The metadata log is the source of truth: after clean unmounts and a
/// crash, a brand-new node must reconstruct the exact namespace and
/// data from the bucket alone.
fn fresh_node_bootstrap(seed: u64) -> Result<()> {
    let (env, root) = setup("bootstrap")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/boot-{}", ts());
    let mut model = Model::default();
    let mut wl = Workload::new(seed, "w");

    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    c0.fs_create()?;
    c0.mount()?;
    wl.run_block(&c0.mnt, &mut model, 50)?;
    // Clean unmount ships the journal tail and publishes a commit.
    c0.unmount()?;

    // Second epoch exercises the replay path past the commit: ops
    // ship on the 2 s ticker, then the daemon is SIGKILLed so no final
    // commit covers them.
    c0.mount()?;
    wl.run_block(&c0.mnt, &mut model, 40)?;
    std::thread::sleep(Duration::from_secs(5)); // >= 2 shipper ticks, journal drained
    c0.kill9()?;

    // A brand-new node with an empty state dir sees the same world.
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    c1.mount().context("bootstrap mount on fresh node")?;
    anyhow::ensure!(
        c1.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0),
        "fresh bootstrap must not inherit pending_upload rows"
    );
    model
        .verify(&c1.mnt)
        .context("fresh node namespace/data vs model")?;
    // And it is writable: allocation continues past replayed inos.
    std::fs::write(c1.mnt.join("bootstrap-proof"), b"hello from c1")?;
    model.write_file(
        std::path::Path::new("bootstrap-proof"),
        b"hello from c1".to_vec(),
    );
    model.verify(&c1.mnt)?;
    c1.unmount()?;
    Ok(())
}

/// Mid-write-back checkpoints (now commits) used to embed the writer's
/// `pending_upload` table; a joiner then ERROR-looped trying to upload
/// chunks it never had (plan 25). Crash the writer while pending > 0 and
/// a commit exists, then assert a fresh node bootstraps with pending == 0
/// and readable data.
///
/// `sync_one`'s ship loop drains the *entire* current journal backlog into
/// one segment every time it runs (bounded only by `SEGMENT_BATCH`/
/// `SEGMENT_MAX_BYTES`, both far above this scenario's 48 tiny records) —
/// so a tight write loop that outruns the shipper's round trip produces
/// one or two large segments, never 48 small ones, and the
/// `PUBLISH_EVERY` (32) segment floor is never reached. A little added S3
/// latency plus pacing the writes wider than one shipper round trip is
/// what makes each close's `nudge_sync` land its own segment instead —
/// the same latency also keeps the later files' chunk uploads (which
/// cannot start before their file is written) pending past the point the
/// mid-flight commit fires, which is the window the scenario needs.
///
/// The joiner runs S3-only (the harness's shared node key), so after
/// the writer's remount finishes its uploads and unmounts cleanly, the
/// joiner sees that final ship — the deferred manifests of the files
/// whose chunks were pending at the kill (plan 30 §M7) — on its next S3
/// tail poll. Until then every one of those files reads as its shipped
/// create left it, empty; nothing may ever read as anything but its
/// content or empty. The content check waits for the joiner's applied
/// position to reach the log head first (the gate on `5437fa6` read a
/// 0-byte file there and reported it as corruption).
fn commit_strips_pending_upload(seed: u64) -> Result<()> {
    let (env, root) = setup("ckpt-pending")?;
    let proxy = env.s3_proxy()?;
    proxy.latency(80, 10)?;
    let prefix = format!("ckpt-pending-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut a = Client::new(root.path(), "a", &env.endpoint, &backend)?.with_write_mode("back");
    a.fs_create()?;
    a.mount()?;

    // > PUBLISH_EVERY (32) small files, paced wider than one segment's
    // round trip, so a mid-flight publish is due while write-back still
    // has a pending queue (the not-yet-written later files' chunks).
    let mut expected = Vec::new();
    for i in 0..48u64 {
        let data = pattern(seed.wrapping_add(i), 4096);
        let name = format!("f{i:03}");
        std::fs::write(a.mnt.join(&name), &data)?;
        expected.push((name, data));
        std::thread::sleep(Duration::from_millis(120));
    }
    eventually(
        "mid-flight commit while pending_uploads > 0",
        Duration::from_secs(90),
        || {
            let pending = a.control_status()?["writeback"]["pending_uploads"]
                .as_u64()
                .unwrap_or(0);
            anyhow::ensure!(pending > 0, "pending already drained ({pending})");
            let n = count_commit_objects(&env.direct_endpoint, &prefix)?;
            anyhow::ensure!(n > 0, "no commit object yet");
            Ok(())
        },
    )?;
    // Crash before clean drain so S3's head commit is still the
    // mid-flight one (a clean unmount would drain pending first).
    a.kill9()?;

    let mut b = Client::new(root.path(), "b", &env.endpoint, &backend)?;
    b.mount().context("fresh joiner after mid-flight commit")?;
    let status = b.control_status()?;
    anyhow::ensure!(
        status["writeback"]["pending_uploads"].as_u64() == Some(0),
        "joiner inherited pending_upload: {status}"
    );
    let log = b.tail_log_n(500);
    let spam = log.matches("pending upload chunk missing").count()
        + log.matches("pending upload chunks missing").count();
    anyhow::ensure!(
        spam == 0,
        "joiner log has {spam} missing-chunk lines; tail:\n{log}"
    );

    // Finish uploading any chunks that died with A, then verify B can read.
    a.mount().context("writer remount to finish uploads")?;
    a.set_write_mode("through")?;
    eventually("writer pending drained", Duration::from_secs(120), || {
        anyhow::ensure!(a.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0));
        Ok(())
    })?;
    a.unmount()?;

    // B runs the S3-only slow path here (the harness's shared node key:
    // no log stream, no gossip hint), so it learns of A's final ship on
    // its next S3 tail poll — due up to the idle backoff's ceiling after
    // A's release. That ship carries the manifests of the files whose
    // chunks were still pending at the kill: plan 30 §M7 ships every
    // create at once and defers a manifest until its chunks are up, so
    // until B tails it, such a file is exactly what its shipped create
    // left it — empty. A stale view is a position of the log, never a
    // third thing: no file may read as anything but its content or empty.
    for (name, data) in &expected {
        let got =
            std::fs::read(b.mnt.join(name)).with_context(|| format!("reading {name} on joiner"))?;
        anyhow::ensure!(
            got == *data || got.is_empty(),
            "before tailing the final ship: {}",
            describe_mismatch(name, &got, data, &expected)
        );
    }
    eventually(
        "joiner tails the writer's final ship",
        Duration::from_secs(60),
        || {
            let head = log_segment_seqs(&env.direct_endpoint, &prefix, "p0")?
                .last()
                .copied()
                .unwrap_or(0);
            let at = b.control_status()?["spool"]["head_seq"]
                .as_u64()
                .unwrap_or(0);
            anyhow::ensure!(at >= head, "joiner applied through {at}, log head {head}");
            Ok(())
        },
    )?;
    for (name, data) in &expected {
        let got =
            std::fs::read(b.mnt.join(name)).with_context(|| format!("reading {name} on joiner"))?;
        anyhow::ensure!(
            got == *data,
            "after tailing the final ship: {}",
            describe_mismatch(name, &got, data, &expected)
        );
    }
    std::thread::sleep(Duration::from_secs(2));
    let log2 = b.tail_log_n(200);
    let spam2 = log2.matches("pending upload chunk missing").count()
        + log2.matches("pending upload chunks missing").count();
    anyhow::ensure!(spam2 == 0, "spam after idle: {spam2}\n{log2}");
    b.unmount()?;
    Ok(())
}

fn count_commit_objects(endpoint: &str, prefix: &str) -> Result<usize> {
    Ok(raw_objects(endpoint, &format!("{prefix}/commits/"))?.len())
}

/// What a wrong read looked like: its length, which written file's
/// content it was (if any), and the first differing offset — the
/// difference between a stale view, a torn one and wrong bytes.
fn describe_mismatch(name: &str, got: &[u8], want: &[u8], all: &[(String, Vec<u8>)]) -> String {
    let looks_like = all
        .iter()
        .find(|(_, other)| other.as_slice() == got)
        .map(|(other, _)| format!("the content of {other}"))
        .unwrap_or_else(|| "no file's content".to_string());
    let first_diff = got.iter().zip(want).position(|(a, b)| a != b);
    format!(
        "content mismatch on {name}: read {} bytes (expected {}), {looks_like}, first \
         differing offset {first_diff:?}",
        got.len(),
        want.len()
    )
}

/// With 60 ms of injected S3 latency and a cold cache, a sequential
/// read of a 32-chunk file takes >= 32 * latency when chunks are
/// fetched one-by-one. The prefetcher must pipeline fetches and land
/// well under that; content integrity is verified too.
fn readahead(_seed: u64) -> Result<()> {
    let (env, root) = setup("readahead")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("ra-{}", ts()))?;

    // 32 chunks of 1 MiB (chunk size set by the harness fs_create).
    let n_chunks = 32u64;
    let mut data = vec![0u8; (n_chunks * (1 << 20)) as usize];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let expected = blake3::hash(&data);
    std::fs::write(c.mnt.join("big"), &data)?;
    drop(data);

    c.unmount()?;
    c.drop_cache()?;
    c.mount()?;
    let latency_ms = 60u64;
    proxy.latency(latency_ms, 0)?;

    let t0 = std::time::Instant::now();
    let read = std::fs::read(c.mnt.join("big"))?;
    let elapsed = t0.elapsed();
    proxy.heal()?;
    anyhow::ensure!(
        blake3::hash(&read) == expected,
        "content corrupted on cold read"
    );

    // Serial worst case: one round-trip per chunk. Pipelining must beat
    // half of it (generous bound to avoid CI flakiness).
    let serial = Duration::from_millis(n_chunks * latency_ms);
    anyhow::ensure!(
        elapsed < serial / 2,
        "cold sequential read took {elapsed:.1?}; serial fetch estimate is {serial:.1?} — \
         prefetcher is not pipelining"
    );
    eprintln!(
        "    readahead: {n_chunks} chunks under {latency_ms}ms latency read in {elapsed:.1?} \
         (serial estimate {serial:.1?})"
    );
    c.unmount()?;
    Ok(())
}

/// A long high-BDP read must grow beyond the minimum byte window and
/// keep enough S3 requests in flight to beat a serial transfer decisively.
fn readahead_adaptive(_seed: u64) -> Result<()> {
    let (env, root) = setup("readahead-adaptive")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("raa-{}", ts()))?;

    let n_chunks = 128u64;
    let mut data = vec![0u8; (n_chunks * (1 << 20)) as usize];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    let expected = blake3::hash(&data);
    std::fs::write(c.mnt.join("big"), &data)?;
    drop(data);

    c.unmount()?;
    c.drop_cache()?;
    c.mount()?;
    let latency_ms = 200u64;
    proxy.latency(latency_ms, 0)?;

    let started = std::time::Instant::now();
    let read = std::fs::read(c.mnt.join("big"))?;
    let elapsed = started.elapsed();
    proxy.heal()?;
    anyhow::ensure!(
        blake3::hash(&read) == expected,
        "content corrupted on adaptive cold read"
    );
    let serial = Duration::from_millis(n_chunks * latency_ms);
    anyhow::ensure!(
        elapsed < serial / 6,
        "adaptive cold read took {elapsed:.1?}; serial estimate is {serial:.1?}"
    );
    eprintln!(
        "    readahead-adaptive: {n_chunks} chunks under {latency_ms}ms latency read in \
         {elapsed:.1?} (serial estimate {serial:.1?})"
    );
    c.unmount()?;
    Ok(())
}

/// A cold read of an E2E file large enough to spill its manifest must work.
///
/// Regression: the spilled chunk-list reference was minted with a plain
/// `ChunkHash::of` while the blob was stored under the E2E keyed addressing
/// hash, so the manifest pointed at an object that did not exist. Every cold
/// read of a >`INLINE_CHUNKS_MAX`-chunk file failed with EIO on a fresh mount,
/// while the same file read fine from a warm cache and non-E2E filesystems
/// were unaffected — which is why the existing E2E coverage, all of it under
/// the inline-manifest limit, never saw it.
fn e2e_spilled_manifest_cold_read(_seed: u64) -> Result<()> {
    let (env, root) = setup("e2e-spilled-manifest")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("e2e-spill-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_e2e()
        // E2E cold reads spill ciphertext and plaintext separately, so the
        // budget must cover well over the file size.
        .with_cache_size(1 << 30);
    c.fs_create()?;
    c.mount()?;

    // Comfortably above INLINE_CHUNKS_MAX (8) so the manifest must spill.
    let n_chunks = 16u64;
    let mut data = vec![0u8; (n_chunks * (1 << 20)) as usize];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    let expected = blake3::hash(&data);
    std::fs::write(c.mnt.join("spilled"), &data).context("writing spilled-manifest E2E file")?;
    drop(data);

    c.unmount()?;
    c.drop_cache()?;
    c.mount().context("cold remount")?;
    proxy.latency(20, 0)?;

    let read = std::fs::read(c.mnt.join("spilled")).context("cold read of spilled E2E manifest")?;
    proxy.heal()?;
    anyhow::ensure!(
        blake3::hash(&read) == expected,
        "cold E2E spilled-manifest read returned different bytes"
    );
    eprintln!(
        "    e2e-spilled-manifest: {n_chunks}-chunk E2E file cold-read intact \
         ({} bytes)",
        read.len()
    );
    c.unmount()?;
    Ok(())
}

/// A demand read of an unrelated file must not queue behind a concurrent
/// bulk E2E download's decrypts.
///
/// Whole-object E2E decrypt is serialized (`decode_gate` in `store.rs`) to
/// bound peak RSS: only one chunk's plaintext may be materializing at a
/// time. Before this fix that gate was a plain FIFO semaphore, so a demand
/// read landing after a deep background prefetch backlog had already
/// queued its decrypts would wait its turn behind all of them. The gate
/// now gives a demand (foreground) decrypt priority over background
/// (prefetch) ones — see `decode_gate.rs` for the unit-level proof of the
/// ordering; this scenario is the end-to-end sanity check that the wiring
/// (prefetch, the direct fetch fallback, and cooperative-cache fetch all
/// pass the right priority) actually works and nothing deadlocks or
/// corrupts data under real concurrent load.
fn e2e_decode_priority(_seed: u64) -> Result<()> {
    let (env, root) = setup("e2e-decode-priority")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("e2e-decode-prio-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_e2e()
        .with_cache_size(1 << 30);
    c.fs_create()?;
    c.mount()?;

    // A wide bulk file so the prefetcher's adaptive window ramps up and
    // keeps a non-trivial number of chunks in flight/decoding at once.
    let n_bulk_chunks = 64u64;
    let mut bulk = vec![0u8; (n_bulk_chunks * (1 << 20)) as usize];
    for (i, byte) in bulk.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    let bulk_expected = blake3::hash(&bulk);
    std::fs::write(c.mnt.join("bulk"), &bulk)?;
    drop(bulk);

    // A small, unrelated file. Its chunk is never touched by the bulk
    // file's readahead, so this is a clean read of "someone else's data"
    // contending only for the decode gate, not for a shared prefetch
    // stream or the same bytes.
    let small = b"a demand read must not wait behind a bulk backlog".repeat(64);
    let small_expected = blake3::hash(&small);
    std::fs::write(c.mnt.join("small"), &small)?;

    c.unmount()?;
    c.drop_cache()?;
    c.mount().context("cold remount")?;
    let latency_ms = 80u64;
    proxy.latency(latency_ms, 0)?;

    let (bulk_read, small_read, small_elapsed) = std::thread::scope(|scope| {
        let bulk_task = scope.spawn(|| -> Result<Vec<u8>> {
            std::fs::read(c.mnt.join("bulk")).context("bulk background read")
        });
        // Give the bulk prefetcher a moment to ramp concurrency and start
        // queuing decrypts before the demand read arrives.
        std::thread::sleep(Duration::from_millis(latency_ms * 2));
        let started = std::time::Instant::now();
        let small_read = std::fs::read(c.mnt.join("small")).context("demand read of small file");
        let small_elapsed = started.elapsed();
        let bulk_read = bulk_task.join().unwrap();
        (bulk_read, small_read, small_elapsed)
    });
    proxy.heal()?;

    let bulk_read = bulk_read?;
    let small_read = small_read?;
    anyhow::ensure!(
        blake3::hash(&bulk_read) == bulk_expected,
        "bulk background read was corrupted by concurrent demand traffic"
    );
    anyhow::ensure!(
        blake3::hash(&small_read) == small_expected,
        "demand read returned the wrong bytes"
    );

    // The demand read is one chunk: one GET RTT plus a near-instant
    // decrypt if it did not queue behind the bulk backlog. A generous
    // multiple of the injected RTT (not of the bulk file's total transfer
    // time) tells apart "waited its turn" from "queued behind dozens of
    // background decrypts."
    let bound = Duration::from_millis(latency_ms * 6);
    anyhow::ensure!(
        small_elapsed < bound,
        "demand read of an unrelated file took {small_elapsed:.1?} under a {n_bulk_chunks}-chunk \
         concurrent bulk download ({latency_ms}ms RTT); bound was {bound:.1?} — the decode gate is \
         not prioritizing demand reads"
    );
    eprintln!(
        "    e2e-decode-priority: demand read finished in {small_elapsed:.1?} while a \
         {n_bulk_chunks}-chunk bulk E2E download was in flight (bound {bound:.1?})"
    );
    c.unmount()?;
    Ok(())
}

/// A tar-like lexicographic walk should trigger directory scan-ahead after
/// its first few files and pipeline the remaining single-chunk objects.
fn scan_ahead(_seed: u64) -> Result<()> {
    let (env, root) = setup("scan-ahead")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("scan-{}", ts()))?;

    let directory = c.mnt.join("walk");
    std::fs::create_dir(&directory)?;
    let files = 200u64;
    let file_size = 256usize << 10;
    let mut expected = Vec::with_capacity(files as usize);
    for index in 0..files {
        let name = format!("{index:04}");
        let marker = format!("scan-{index:04}-").into_bytes();
        let mut data = Vec::with_capacity(file_size);
        while data.len() < file_size {
            data.extend_from_slice(&marker);
        }
        data.truncate(file_size);
        expected.push((name.clone(), blake3::hash(&data)));
        std::fs::write(directory.join(&name), data)
            .with_context(|| format!("writing scan-ahead fixture {name}"))?;
    }

    c.unmount()
        .context("unmounting scan-ahead fixture writer")?;
    c.drop_cache().context("dropping scan-ahead cache")?;
    c.mount().context("remounting scan-ahead reader")?;
    let latency_ms = 60u64;
    proxy
        .latency(latency_ms, 0)
        .context("installing scan-ahead latency toxic")?;

    let started = std::time::Instant::now();
    let mut entries = std::fs::read_dir(c.mnt.join("walk"))
        .context("opening scan-ahead directory")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("collecting scan-ahead directory entries")?;
    entries.sort_by_key(|entry| entry.file_name());
    for (entry, (name, hash)) in entries.into_iter().zip(&expected) {
        anyhow::ensure!(
            entry.file_name().to_string_lossy() == name.as_str(),
            "directory order mismatch"
        );
        let data = std::fs::read(entry.path())
            .with_context(|| format!("reading scan-ahead file {name}"))?;
        anyhow::ensure!(
            blake3::hash(&data) == *hash,
            "content corrupted in scan-ahead file {name}"
        );
    }
    let elapsed = started.elapsed();
    proxy.heal().context("healing scan-ahead proxy")?;
    let status = c
        .control_status()
        .context("reading scan-ahead daemon status")?;
    eprintln!(
        "    scan-ahead status: {}",
        serde_json::to_string(&status["prefetch"])?
    );

    let serial = Duration::from_millis(files * latency_ms);
    anyhow::ensure!(
        elapsed < serial / 4,
        "ordered small-file walk took {elapsed:.1?}; serial estimate is {serial:.1?}"
    );
    eprintln!(
        "    scan-ahead: {files} files under {latency_ms}ms latency read in {elapsed:.1?} \
         (serial estimate {serial:.1?})"
    );
    c.unmount()?;
    Ok(())
}

/// Shared implementation for `distant-bigfile-stable[-e2e]`: a sequential
/// cold download of a sizable file over an emulated "distant" S3 path (200ms
/// latency, no bandwidth cap) must reach high throughput and *hold* it —
/// no long stalls followed by bursts. We sample per-block (4 MiB) wall time
/// through the read and require both a high overall rate and a bounded
/// worst-case/best-case ratio among the post-ramp-up samples.
fn distant_bigfile_stable_inner(e2e: bool) -> Result<()> {
    let label = if e2e {
        "distant-bigfile-stable-e2e"
    } else {
        "distant-bigfile-stable"
    };
    let (env, root) = setup(label)?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("{label}-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?.with_cache_size(1 << 30);
    if e2e {
        c = c.with_e2e();
    }
    c.fs_create()?;
    c.mount()?;

    // 192 MiB: big enough that the adaptive window reaches steady state and
    // a real stall shows up as more than timing noise, small enough to stay
    // CI-sane at 200ms RTT.
    let n_chunks = 192u64;
    let data = pattern(1, (n_chunks * (1 << 20)) as usize);
    let expected = blake3::hash(&data);
    std::fs::write(c.mnt.join("distant"), &data)?;
    drop(data);

    c.unmount()?;
    c.drop_cache()?;
    c.mount().context("cold remount")?;
    let latency_ms = 200u64;
    proxy.latency(latency_ms, 0)?;

    let mut file = std::fs::File::open(c.mnt.join("distant")).context("opening distant file")?;
    let mut block = vec![0u8; 4 << 20];
    let mut read = Vec::with_capacity((n_chunks << 20) as usize);
    let mut block_secs = Vec::new();
    let started = std::time::Instant::now();
    loop {
        let block_t0 = std::time::Instant::now();
        let n = std::io::Read::read(&mut file, &mut block).context("reading distant file")?;
        if n == 0 {
            break;
        }
        block_secs.push(block_t0.elapsed().as_secs_f64());
        read.extend_from_slice(&block[..n]);
    }
    let elapsed = started.elapsed();
    proxy.heal()?;

    anyhow::ensure!(
        blake3::hash(&read) == expected,
        "distant-bigfile-stable: content corrupted on cold read"
    );

    let mib_per_sec: Vec<f64> = block_secs
        .iter()
        .skip(1) // cold TTFB is one RTT, not a "swing"
        .map(|secs| (4.0) / secs.max(1e-9))
        .collect();
    anyhow::ensure!(
        mib_per_sec.len() >= 4,
        "not enough post-ramp-up samples to judge stability ({} blocks)",
        block_secs.len()
    );
    let min_rate = mib_per_sec.iter().copied().fold(f64::INFINITY, f64::min);
    let max_rate = mib_per_sec.iter().copied().fold(0.0, f64::max);
    let mean_rate: f64 = mib_per_sec.iter().sum::<f64>() / mib_per_sec.len() as f64;

    let overall_mib_s = (n_chunks as f64) / elapsed.as_secs_f64().max(1e-9);
    eprintln!(
        "    {label}: {n_chunks} MiB in {elapsed:.1?} ({overall_mib_s:.1} MiB/s overall); \
         per-block min={min_rate:.1} mean={mean_rate:.1} max={max_rate:.1} MiB/s"
    );

    // Serial one-RTT-per-chunk floor: pipelining must beat it decisively.
    let serial = Duration::from_millis(n_chunks * latency_ms);
    anyhow::ensure!(
        elapsed < serial / 8,
        "{label}: cold read took {elapsed:.1?}; naive serial estimate is {serial:.1?} — the \
         prefetcher is not pipelining enough to call this a healthy distant path"
    );
    // "No big swings" means no *stalls* — a block that was already sitting
    // fully prefetched can legitimately be served at memory speed, so a
    // high max is a feature, not a bug. What must not happen is a block
    // whose bytes were not yet in flight taking many RTTs to arrive one at
    // a time instead of the pipeline having kept multiple chunks in
    // flight. Floor: a 4 MiB block split across four 1 MiB chunks, even if
    // every one of them had to be fetched from a cold start with no
    // parallelism at all, still finishes inside 2 RTTs if the pipeline
    // keeps only two requests concurrently — well under what a genuinely
    // stalled/serialized path would show.
    let min_floor_mib_s = 4.0 / (2.0 * latency_ms as f64 / 1000.0);
    anyhow::ensure!(
        min_rate > min_floor_mib_s,
        "{label}: worst post-ramp-up block ran at {min_rate:.1} MiB/s (floor {min_floor_mib_s:.1} \
         MiB/s for a 4 MiB block under {latency_ms}ms RTT) — that's a stall, not steady \
         pipelining (mean was {mean_rate:.1} MiB/s)"
    );
    // Drop must happen before unmount: Rust only frees the fd at scope end,
    // and `file` is otherwise still alive here — an open fd on the mount
    // makes fusermount3 report "Device or resource busy" and hang the
    // harness for the full unmount timeout.
    drop(file);
    c.unmount()?;
    Ok(())
}

fn distant_bigfile_stable(_seed: u64) -> Result<()> {
    distant_bigfile_stable_inner(false)
}

fn distant_bigfile_stable_e2e(_seed: u64) -> Result<()> {
    distant_bigfile_stable_inner(true)
}

/// Shared implementation for `prefetch-abandon[-e2e]`: a reader that reads
/// the first slice of a big sequential file and then goes quiet (without
/// closing the fd) must not keep costing us S3 GETs and cache space for the
/// rest of the file. See `PREFETCH_ABANDON_IDLE` in `prefetch.rs` for the
/// mechanism under test.
fn prefetch_abandon_inner(e2e: bool) -> Result<()> {
    let label = if e2e {
        "prefetch-abandon-e2e"
    } else {
        "prefetch-abandon"
    };
    let (env, root) = setup(label)?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("{label}-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?.with_cache_size(2 << 30);
    if e2e {
        c = c.with_e2e();
    }
    c.fs_create()?;
    c.mount()?;

    // 1 GiB, deliberately much larger than any plausible concurrency
    // ceiling. toxiproxy's bandwidth toxic caps each *connection*
    // independently (there is no proxy-wide aggregate limiter), so with
    // `INITIAL_CONCURRENCY` (32) simultaneous GETs the effective aggregate
    // rate is ~32x the configured cap. If the file were small enough that
    // its whole chunk count fit within one concurrency generation (e.g.
    // 128 MiB / 4 MiB chunks = 32 chunks == INITIAL_CONCURRENCY), every
    // chunk would be dispatched (moved from "queued" to "in flight") the
    // instant the window is scheduled — leaving nothing in the queue for
    // the idle-abandon sweep to cancel, no matter how slow each individual
    // connection is. At 1 GiB / 4 MiB chunks = 256 chunks, only a fraction
    // can be in flight at once; the rest are genuinely queued long enough
    // for PREFETCH_ABANDON_IDLE to catch them.
    let n_chunks = 1024u64;
    let data = pattern(2, (n_chunks << 20) as usize);
    let expected = blake3::hash(&data);
    std::fs::write(c.mnt.join("abandoned"), &data)?;

    c.unmount()?;
    c.drop_cache()?;
    c.mount().context("cold remount")?;
    // Modest latency plus a real bandwidth cap. toxiproxy's bandwidth toxic
    // caps each *connection* independently — there is no aggregate/proxy-
    // wide limiter — so the *effective* aggregate rate scales with
    // concurrency. The prefetcher's AIMD controller ramps concurrency into
    // the dozens within a few successful fetches, so a cap that looks
    // "slow" per-connection (e.g. 16 Mbps) becomes gigabit-class in
    // aggregate and drains the whole scheduled window in a few hundred
    // milliseconds — faster than the idle-sweep tick can react, which
    // would make this test pass or fail on pure timing luck. Use a per-
    // connection rate slow enough that even at the concurrency ceiling
    // (ABSOLUTE_MAX_CONCURRENCY) a chunk takes seconds, not milliseconds,
    // to complete, so the sweep reliably catches real queued backlog.
    proxy.latency(20, 0)?;
    proxy.bandwidth(50)?; // ~400 Kbps per connection

    let cache_before = c.control_status()?["cache"]["used_bytes"]
        .as_u64()
        .unwrap_or(0);

    // Read the first slice sequentially, keep the fd open, then go quiet.
    // A real reader that paused a video/scrub or a killed-but-not-yet-
    // reaped process looks exactly like this: readable fd, no more reads.
    let mut file = std::fs::File::open(c.mnt.join("abandoned")).context("opening file")?;
    let mut prefix_buf = vec![0u8; 8 << 20];
    std::io::Read::read_exact(&mut file, &mut prefix_buf).context("reading prefix")?;

    // Let a couple of abandon-sweep ticks pass (PREFETCH_ABANDON_IDLE is
    // 2s; give it a healthy multiple so this isn't a timing coin-flip).
    std::thread::sleep(Duration::from_secs(5));
    let cache_after_idle = c.control_status()?["cache"]["used_bytes"]
        .as_u64()
        .unwrap_or(0);
    let abandoned_chunks = c.control_status()?["prefetch"]["abandoned_chunks"]
        .as_u64()
        .unwrap_or(0);

    let file_bytes = data.len() as u64;
    let fetched_while_idle = cache_after_idle.saturating_sub(cache_before);
    eprintln!(
        "    {label}: {file_bytes} B file, read {} B, fetched {fetched_while_idle} B into cache \
         while idle, prefetch reported {abandoned_chunks} abandoned chunks",
        prefix_buf.len()
    );

    anyhow::ensure!(
        abandoned_chunks > 0,
        "{label}: prefetcher reported zero abandoned chunks; the idle-abandon sweep did not \
         trim the queued backlog for this stream"
    );
    // The whole point: an idle reader must not cause the whole file to
    // land in cache. Bound generously above the read prefix plus one full
    // window's worth of legitimately-in-flight chunks, but nowhere near
    // "the entire file" — that's exactly the waste being tested for.
    let waste_ceiling = file_bytes / 2;
    anyhow::ensure!(
        fetched_while_idle < waste_ceiling,
        "{label}: {fetched_while_idle} B landed in cache while the reader was idle, out of a \
         {file_bytes} B file (ceiling {waste_ceiling} B) — the abandoned stream's backlog was \
         not cancelled in time"
    );

    // Cache should also settle — no straggling background fetches still
    // trickling in chunks well after the sweep has run.
    std::thread::sleep(Duration::from_millis(500));
    let cache_settled = c.control_status()?["cache"]["used_bytes"]
        .as_u64()
        .unwrap_or(0);
    anyhow::ensure!(
        cache_settled == cache_after_idle,
        "{label}: cache kept growing after the idle sweep ({cache_after_idle} -> \
         {cache_settled} B) — background fetches did not actually stop"
    );

    // Correctness after abandonment: resuming the read must still produce
    // exactly the right bytes, proving the trimmed backlog is re-offered
    // rather than silently skipped. Heal the proxy *before* the resumed
    // read, not after: at the throttled per-connection rate used above to
    // force real queuing, reading the remaining ~1 GiB through the toxic
    // would take minutes (aggregate throughput is bounded by
    // gate_target * rate, not by wall-clock patience). The abandon/resume
    // behavior under test only concerns the idle window; the resumed read
    // itself just needs to prove correctness, which it can do at full
    // speed.
    proxy.heal()?;
    let mut rest = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut rest).context("resuming read after abandonment")?;
    let mut full = prefix_buf;
    full.extend_from_slice(&rest);
    anyhow::ensure!(
        blake3::hash(&full) == expected,
        "{label}: resumed read returned different bytes than were written — abandonment must \
         not corrupt or skip data, only defer fetching it"
    );
    drop(file);
    c.unmount()?;
    Ok(())
}

fn prefetch_abandon(_seed: u64) -> Result<()> {
    prefetch_abandon_inner(false)
}

fn prefetch_abandon_e2e(_seed: u64) -> Result<()> {
    prefetch_abandon_inner(true)
}

/// A big-file prefetcher saturating a bandwidth-capped S3 path must not
/// starve small, unrelated foreground reads. Continuous small-file reads
/// run on a background thread while a big file is cold-read in the
/// foreground; small-file latency must stay bounded throughout.
fn prefetch_fairness(_seed: u64) -> Result<()> {
    let (env, root) = setup("prefetch-fairness")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("fair-{}", ts()))?;

    let small_dir = c.mnt.join("small");
    std::fs::create_dir(&small_dir)?;
    let n_small = 64u64;
    let small_size = 8usize << 10;
    let mut small_expected = Vec::with_capacity(n_small as usize);
    for i in 0..n_small {
        let name = format!("s{i:04}");
        let data = pattern(100 + i, small_size);
        small_expected.push((name.clone(), blake3::hash(&data)));
        std::fs::write(small_dir.join(&name), &data)?;
    }

    let n_chunks = 96u64;
    let big_data = pattern(3, (n_chunks << 20) as usize);
    let big_expected = blake3::hash(&big_data);
    std::fs::write(c.mnt.join("big"), &big_data)?;
    drop(big_data);

    c.unmount()?;
    c.drop_cache()?;
    c.mount().context("cold remount")?;
    // 100 Mbps cap shared between the big prefetcher and the small reads —
    // toxiproxy's bandwidth toxic takes the rate in KB/s.
    proxy.bandwidth(12_500)?;

    let mnt = c.mnt.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_reader = stop.clone();
    let small_expected_reader = small_expected.clone();
    let reader = std::thread::spawn(move || -> Result<Vec<Duration>> {
        let mut latencies = Vec::new();
        let mut i = 0usize;
        while !stop_reader.load(std::sync::atomic::Ordering::Relaxed) {
            let (name, hash) = &small_expected_reader[i % small_expected_reader.len()];
            let t0 = std::time::Instant::now();
            let data = std::fs::read(mnt.join("small").join(name))
                .with_context(|| format!("small read of {name}"))?;
            latencies.push(t0.elapsed());
            anyhow::ensure!(blake3::hash(&data) == *hash, "small file {name} corrupted");
            i += 1;
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(latencies)
    });

    // Give the small-file reader a moment to establish a quiet baseline
    // before the big prefetcher starts contending for the capped link.
    std::thread::sleep(Duration::from_millis(500));
    let big_started = std::time::Instant::now();
    let big_read = std::fs::read(c.mnt.join("big")).context("big-file read under bandwidth cap")?;
    let big_elapsed = big_started.elapsed();
    anyhow::ensure!(
        blake3::hash(&big_read) == big_expected,
        "big file corrupted under contended bandwidth cap"
    );

    // Let the small reader keep going a bit past the big read so we can
    // see it return to a quiet baseline, then stop it.
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let latencies = reader.join().unwrap()?;
    proxy.heal()?;
    c.unmount()?;

    anyhow::ensure!(
        latencies.len() > 20,
        "small-file reader only completed {} reads; scenario didn't run long enough",
        latencies.len()
    );
    let mut sorted: Vec<Duration> = latencies.clone();
    sorted.sort();
    let p50 = sorted[sorted.len() / 2];
    let p95 = sorted[(sorted.len() * 95 / 100).min(sorted.len() - 1)];
    let max = *sorted.last().unwrap();
    eprintln!(
        "    prefetch-fairness: big file ({} MiB) read in {big_elapsed:.1?} under a 100 Mbps \
         cap; concurrent small reads n={} p50={p50:.1?} p95={p95:.1?} max={max:.1?}",
        big_read.len() / (1 << 20),
        latencies.len()
    );

    // A small 8 KiB read sharing a 100 Mbps link with a saturating big-file
    // prefetch should still land well under a second — if it doesn't, the
    // prefetcher's concurrency gate is starving demand-path traffic instead
    // of yielding to it (this is exactly what the coop decode-priority gate
    // and the per-stream fairness in the scheduler's `pop()` are for).
    anyhow::ensure!(
        p95 < Duration::from_millis(1500),
        "prefetch-fairness: small-file p95 latency was {p95:.1?} while a big-file prefetch \
         saturated a 100 Mbps link — small reads are stalling behind the prefetcher"
    );
    anyhow::ensure!(
        max < Duration::from_secs(5),
        "prefetch-fairness: worst small-file read took {max:.1?} — far outside a healthy \
         fairness bound even accounting for scheduling jitter"
    );
    Ok(())
}

/// fio random writes with crc32c verification while every S3 round
/// trip carries 80 ms of latency: slow but perfectly correct.
fn fio_latency(_seed: u64) -> Result<()> {
    let (env, root) = setup("fio-latency")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("fiolat-{}", ts()))?;
    proxy.latency(80, 20)?;
    suites::fio_verify(&c.mnt, "8M", 1)?;
    proxy.heal()?;
    c.unmount()?;
    Ok(())
}

/// fio verify while a background flapper cuts S3 for 800 ms every ~4 s.
/// The object-store retry layer must absorb the blips: no I/O errors,
/// no verification failures.
fn fio_blips(_seed: u64) -> Result<()> {
    let (env, root) = setup("fio-blips")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("fioblip-{}", ts()))?;

    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let flapper = scope.spawn(|| -> Result<()> {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                proxy.cut()?;
                std::thread::sleep(Duration::from_millis(800));
                proxy.heal()?;
                std::thread::sleep(Duration::from_secs(4));
            }
            Ok(())
        });
        let fio = suites::fio_verify(&c.mnt, "16M", 2);
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        flapper.join().expect("flapper panicked")?;
        fio
    })?;
    proxy.heal()?;
    c.unmount()?;
    Ok(())
}

/// stress-ng metadata churn while S3 flaps: namespace ops are local in
/// phase 1 and must be entirely unaffected; afterwards the mount is
/// healthy and the metadata spool drains to zero.
fn stress_ng_flap(_seed: u64) -> Result<()> {
    let (env, root) = setup("stress-flap")?;
    let proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("sng-{}", ts()))?;

    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let flapper = scope.spawn(|| -> Result<()> {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                proxy.cut()?;
                std::thread::sleep(Duration::from_millis(1500));
                proxy.heal()?;
                std::thread::sleep(Duration::from_secs(2));
            }
            Ok(())
        });
        let churn = suites::stress_ng(&c.mnt, &["dentry", "rename", "open"], 8);
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        flapper.join().expect("flapper panicked")?;
        churn
    })?;
    proxy.heal()?;

    // Mount still healthy, and the churned metadata ships out. Churn
    // produces hundreds of thousands of records; require progress, not
    // a fixed deadline.
    std::fs::write(c.mnt.join("canary"), b"ok")?;
    anyhow::ensure!(std::fs::read(c.mnt.join("canary"))? == b"ok");
    let mut last = u64::MAX;
    let mut stalled = 0;
    for _ in 0..300 {
        let status = c.control_status()?;
        let backlog = status["spool"]["journal_backlog"]
            .as_u64()
            .unwrap_or(u64::MAX);
        if backlog == 0 {
            c.unmount()?;
            return Ok(());
        }
        if backlog < last {
            last = backlog;
            stalled = 0;
        } else {
            stalled += 1;
            anyhow::ensure!(
                stalled < 15,
                "spool drain stalled at backlog {backlog}: {status}"
            );
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    anyhow::bail!(
        "spool did not drain after stress-ng churn: {}",
        c.control_status()?
    )
}

/// Lease handoff in ~1 RTT (roadmap M3.3 exit criterion).
///
/// A holds the lease and keeps writing, so it never goes write-idle. B
/// then writes: without P2P it would have to wait out A's idle window or
/// TTL, but with the fast path it asks A directly, A flushes and
/// releases, and B's CAS succeeds. The idle window is set long (30 s) so
/// a fast result cannot be explained by A releasing on its own.
fn p2p_handover(_seed: u64) -> Result<()> {
    let (env, root) = setup("p2p-handover")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/p2phand-{}", ts());
    let idle_ms = 30_000u64;
    // Forwarding off: this scenario measures the *takeover* path — a
    // non-holder write escalating to a ~1 RTT P2P lease handoff. With
    // forwarding on (the default), B's write would be validated by the
    // holder instead and the lease would rightly stay at A; that path
    // is `forwarded-mutations`' job.
    let tune = |c: Client, key: &str| {
        c.with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", &idle_ms.to_string())
            .with_env("CONSTELLATION_FORWARD", "off")
            .with_env("CONSTELLATION_NODE_KEY", key)
    };
    let mut c0 = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "hand-c0"),
    );
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        &scenario_key(root.path(), "hand-c1"),
    );
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;

    // A becomes the active holder and stays busy.
    std::fs::write(c0.mnt.join("a-owns"), b"a")?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        let l = lease_of(&c0)?;
        anyhow::ensure!(l["held"] == true, "A does not hold the lease: {l}");
        Ok(())
    })?;
    let epoch_before = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

    // B writes while A is still the holder: this is the handoff path.
    let started = std::time::Instant::now();
    std::fs::write(c1.mnt.join("b-wants"), b"b")?;
    let took = started.elapsed();
    eprintln!(
        "    p2p-handover: B's first write under a live holder took {} ms (idle window {idle_ms} ms)",
        took.as_millis()
    );
    anyhow::ensure!(
        took < Duration::from_millis(idle_ms / 2),
        "handover took {:?}, which is not decisively faster than the {idle_ms} ms idle window",
        took
    );
    eventually("B holds the lease", Duration::from_secs(20), || {
        let l = lease_of(&c1)?;
        anyhow::ensure!(l["held"] == true, "B never took the lease: {l}");
        Ok(())
    })?;
    let epoch_after = lease_of(&c1)?["epoch"].as_u64().unwrap_or(0);
    anyhow::ensure!(
        epoch_after > epoch_before,
        "the lease epoch must advance across a handoff ({epoch_before} -> {epoch_after})"
    );

    // Both nodes converge on both writes, and nothing conflicted.
    let mut model = Model::default();
    model.write_file(std::path::Path::new("a-owns"), b"a".to_vec());
    model.write_file(std::path::Path::new("b-wants"), b"b".to_vec());
    eventually("both nodes converge", Duration::from_secs(30), || {
        model.verify(&c0.mnt).context("via c0")?;
        model.verify(&c1.mnt).context("via c1")?;
        Ok(())
    })?;
    ensure_no_conflicts(&[&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

fn forwarded_mutations(_seed: u64) -> Result<()> {
    let (env, root) = setup("forwarded-mutations")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/forwarded-{}", ts());
    let idle_ms = 30_000u64;
    let tune = |c: Client, key: &str| {
        c.with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", &idle_ms.to_string())
            .with_env("CONSTELLATION_NODE_KEY", key)
    };
    let c0_key = &scenario_key(root.path(), "forward-c0");
    let c1_key = &scenario_key(root.path(), "forward-c1");
    let mut c0 = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        c0_key,
    );
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        c1_key,
    );
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;

    std::fs::write(c0.mnt.join("a-owns"), b"a")?;
    eventually("c0 holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&c0)?;
        anyhow::ensure!(lease["held"] == true, "c0 does not hold the lease: {lease}");
        Ok(())
    })?;
    let epoch_before = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

    let mut model = Model::default();
    model.write_file(std::path::Path::new("a-owns"), b"a".to_vec());
    for i in 0..20 {
        let name = format!("b-{i}");
        let data = format!("value-{i}").into_bytes();
        std::fs::write(c1.mnt.join(&name), &data)?;
        model.write_file(std::path::Path::new(&name), data);
    }

    let c0_lease = lease_of(&c0)?;
    let c1_lease = lease_of(&c1)?;
    anyhow::ensure!(
        c0_lease["held"] == true || c1_lease["held"] != true,
        "forward burst handed the lease to c1: c0={c0_lease}, c1={c1_lease}"
    );
    let epoch_after = c0_lease["epoch"]
        .as_u64()
        .unwrap_or(0)
        .max(c1_lease["epoch"].as_u64().unwrap_or(0));
    anyhow::ensure!(
        epoch_after <= epoch_before + 3,
        "20 forwarded writes thrashed the lease epoch ({epoch_before} -> {epoch_after})"
    );
    eventually(
        "requester records successful forwards",
        Duration::from_secs(20),
        || {
            let status = c1.control_status()?;
            anyhow::ensure!(
                status["forwarded_ok"].as_u64().unwrap_or(0) > 0,
                "c1 has no successful forwarded mutations: {status}"
            );
            Ok(())
        },
    )?;
    eventually("both nodes converge", Duration::from_secs(30), || {
        model.verify(&c0.mnt).context("via c0")?;
        model.verify(&c1.mnt).context("via c1")?;
        Ok(())
    })?;
    ensure_no_conflicts(&[&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// A node SIGKILLed and remounted with the same state dir keeps its node
/// key, so its new incarnation has the same iroh `EndpointId`. The nodes
/// that knew it still hold connections to the dead incarnation, and
/// iroh keeps routing new dials to its old address while any of them is
/// open (see `constellation_net::endpoint::Pool`). Before the fix they
/// could not reach it for 30 s or more: pings failed, `connected` stayed
/// false, and forwards to it hung on the dead connection.
///
/// Three nodes; c2 holds the lease so c0 and c1 have warm, pooled P2P
/// connections to it (their forwards). Crash c2, wait until both see it
/// down, remount it from the same state, and require (1) both report it
/// `connected` again within [`RESTART_REACH_BOUND`] of the remount, and
/// (2) once it holds the lease again, a write on each of c0 and c1 is
/// forwarded to it over P2P (not the S3 inbox), promptly.
///
/// No backups (`CONSTELLATION_BACKUPS=0`, a `Local` lease): with one, the
/// crash is a seal-based failover by design (the listed backup seals the
/// silent holder after 1.5 s and takes the lease at the next epoch), and
/// c2's writes after its restart are forwarded to that new holder, which
/// never moves the lease back. Whether the scenario met a backup depended
/// on whether c2's backup tick ran before the crash, so it failed about
/// one run in three. Unbacked, the dead holder's lease stays in force and
/// c2's first write after the restart retakes it at the same epoch.
fn p2p_same_identity_restart(_seed: u64) -> Result<()> {
    const RESTART_REACH_BOUND: Duration = Duration::from_secs(5);
    const FORWARD_BOUND: Duration = Duration::from_secs(5);
    let (env, root) = setup("p2p-same-identity-restart")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/p2p-restart-{}", ts());
    // c2 creates the filesystem and writes first, so it takes the lease
    // and keeps it (a sticky holder the others forward to), through its
    // crash too: no backup may take it over.
    let tune = |c: Client| {
        c.with_own_node_key()
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "60000")
            .with_env("CONSTELLATION_BACKUPS", "0")
    };
    let mut c0 = tune(Client::new(root.path(), "c0", &env.endpoint, &backend)?);
    let mut c1 = tune(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    let mut c2 = tune(Client::new(root.path(), "c2", &env.endpoint, &backend)?);
    c2.fs_create()?;
    c2.mount()?;
    std::fs::write(c2.mnt.join("c2-created"), b"c2")?;
    c0.mount()?;
    c1.mount()?;
    wait_for_peers(&[&c0, &c1, &c2])?;
    let victim = c2.control_status()?["node_id"]
        .as_u64()
        .context("c2 reports no node id")?;
    let sees = |c: &Client, want: bool| -> Result<()> {
        let p = p2p_of(c)?;
        let up = p["peers"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|peer| peer["node_id"] == victim && peer["connected"] == true);
        anyhow::ensure!(
            up == want,
            "{} sees c2 connected={up}, want {want}: {p}",
            c.name
        );
        Ok(())
    };
    let mut model = Model::default();
    model.write_file(std::path::Path::new("c2-created"), b"c2".to_vec());
    let mut write = |c: &Client, name: &str| -> Result<()> {
        let data = format!("{name} via {}", c.name).into_bytes();
        std::fs::write(c.mnt.join(name), &data)?;
        model.write_file(std::path::Path::new(name), data);
        Ok(())
    };
    // One write from `c`, which must reach the holder as a P2P forward.
    let forward = |c: &Client, name: &str, write: &mut dyn FnMut(&Client, &str) -> Result<()>| {
        let ok = c.control_status()?["forwarded_ok"].as_u64().unwrap_or(0);
        let inbox = inbox_counter(c, "inbox_ops")?;
        let started = std::time::Instant::now();
        write(c, name)?;
        let took = started.elapsed();
        let ok_after = c.control_status()?["forwarded_ok"].as_u64().unwrap_or(0);
        let inbox_after = inbox_counter(c, "inbox_ops")?;
        anyhow::Ok((ok_after > ok && inbox_after == inbox, took))
    };
    let c2_holds = |c2: &Client, what: &str, write: &mut dyn FnMut(&Client, &str) -> Result<()>| {
        write(c2, what)?;
        eventually(
            &format!("c2 holds the lease ({what})"),
            Duration::from_secs(30),
            || {
                let lease = lease_of(c2)?;
                anyhow::ensure!(lease["held"] == true, "c2 does not hold the lease: {lease}");
                Ok(())
            },
        )
    };

    c2_holds(&c2, "c2-first", &mut write)?;
    let epoch = lease_of(&c2)?["epoch"].clone();
    for c in [&c0, &c1] {
        // A cold start may still have c2 refusing a node it has not read
        // from the registry yet (its allowlist refresh is rate-limited),
        // so the warm-up retries until a forward goes over P2P.
        let mut attempt = 0;
        eventually(
            &format!("{}'s warm-up write is a P2P forward", c.name),
            Duration::from_secs(30),
            || {
                attempt += 1;
                let (p2p, took) = forward(c, &format!("warm-{}-{attempt}", c.name), &mut write)?;
                anyhow::ensure!(p2p, "not a P2P forward ({took:?})");
                Ok(())
            },
        )?;
        eventually(
            &format!("{} sees c2 connected", c.name),
            Duration::from_secs(20),
            || sees(c, true),
        )?;
    }

    c2.kill9()?;
    for c in [&c0, &c1] {
        // The registry tick's probe (every 5 s) notices.
        eventually(
            &format!("{} sees c2 down", c.name),
            Duration::from_secs(20),
            || sees(c, false),
        )?;
    }
    c2.mount().context("remounting c2 with the same state")?;
    let remounted = std::time::Instant::now();
    for c in [&c0, &c1] {
        let left = RESTART_REACH_BOUND.saturating_sub(remounted.elapsed());
        eventually(
            &format!("{} sees the restarted c2 connected", c.name),
            left,
            || sees(c, true),
        )
        .with_context(|| format!("c2's log: {}", c2.tail_log()))?;
    }
    eprintln!(
        "    p2p-same-identity-restart: c0 and c1 see c2 connected {} ms after its remount",
        remounted.elapsed().as_millis()
    );

    c2_holds(&c2, "c2-again", &mut write)?;
    // Nobody else held it meanwhile: unbacked and with no write pending
    // elsewhere, nothing may claim a crashed holder's lease before it
    // expires, so c2 retook its own at the same epoch.
    let again = lease_of(&c2)?;
    anyhow::ensure!(
        again["epoch"] == epoch,
        "the lease moved while c2 was down (epoch {epoch} before the crash): {again}"
    );
    for c in [&c0, &c1] {
        let (p2p, took) = forward(c, &format!("after-{}", c.name), &mut write)?;
        eprintln!(
            "    p2p-same-identity-restart: {}'s forward to the restarted holder: p2p={p2p} in {} ms",
            c.name,
            took.as_millis()
        );
        anyhow::ensure!(
            p2p && took <= FORWARD_BOUND,
            "{}'s write to the restarted holder was not a prompt P2P forward \
             (p2p={p2p}, {took:?}, bound {FORWARD_BOUND:?})",
            c.name
        );
    }
    eventually("all three converge", Duration::from_secs(30), || {
        model.verify(&c0.mnt).context("via c0")?;
        model.verify(&c1.mnt).context("via c1")?;
        model.verify(&c2.mnt).context("via c2")?;
        Ok(())
    })?;
    ensure_no_conflicts(&[&c0, &c1, &c2])?;
    c0.unmount()?;
    c1.unmount()?;
    c2.unmount()?;
    Ok(())
}

fn scratch_publish(_seed: u64) -> Result<()> {
    let (env, root) = setup("scratch-publish")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/scratch-publish-{}", ts());
    let c0_key = &scenario_key(root.path(), "scratch-c0");
    let c1_key = &scenario_key(root.path(), "scratch-c1");
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_NODE_KEY", c0_key);
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_NODE_KEY", c1_key);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;

    let scratch = c0.mnt.join("tmp");
    std::fs::create_dir(&scratch)?;
    set_xattr(&scratch, "user.constellation.scratch", b"1")?;
    eventually(
        "scratch marker visible on c1",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(
                get_xattr(&c1.mnt.join("tmp"), "user.constellation.scratch")? == b"1",
                "scratch marker not visible"
            );
            Ok(())
        },
    )?;

    std::fs::write(scratch.join("local-only"), b"private")?;
    std::fs::write(scratch.join("obj"), b"published")?;
    anyhow::ensure!(
        !c1.mnt.join("tmp/local-only").exists(),
        "scratch file was immediately visible on c1"
    );

    std::fs::rename(scratch.join("obj"), c0.mnt.join("cache-hash"))?;
    eventually(
        "published file visible but scratch file private",
        Duration::from_secs(30),
        || {
            anyhow::ensure!(
                std::fs::read(c1.mnt.join("cache-hash"))? == b"published",
                "published content mismatch"
            );
            anyhow::ensure!(
                !c1.mnt.join("tmp/local-only").exists(),
                "node-private scratch file appeared on c1"
            );
            Ok(())
        },
    )?;
    anyhow::ensure!(
        std::fs::read(c0.mnt.join("tmp/local-only"))? == b"private",
        "local scratch content changed"
    );
    ensure_no_conflicts(&[&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// A P2P partition must be survivable: the fast path is an accelerator,
/// so disabling it on one node only slows things down.
///
/// toxiproxy only fronts S3, so the P2P path is cut with the kill switch
/// on one node instead (documented choice from plan 02): that node can
/// neither gossip nor be asked for a handoff, which is exactly the
/// "peer unreachable" case. Everything must still converge over S3.
fn p2p_partition_tolerance(seed: u64) -> Result<()> {
    let (env, root) = setup("p2p-partition")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/p2ppart-{}", ts());
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    // Only c1 has the fast path; c0 cannot participate at all.
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?.with_env(
        "CONSTELLATION_NODE_KEY",
        &scenario_key(root.path(), "part-c1"),
    );
    c0 = c0.with_env("CONSTELLATION_P2P", "off");
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    anyhow::ensure!(
        p2p_of(&c0)?["enabled"] == false,
        "c0 must have no fast path"
    );

    // The two-clients-shared workload: each node owns a subtree and must
    // observe the other's exactly, purely over S3.
    let mut m0 = Model::default();
    let mut m1 = Model::default();
    let mut w0 = Workload::new(seed, "a");
    let mut w1 = Workload::new(seed.wrapping_add(1), "b");
    for dir in ["from-a", "from-b"] {
        std::fs::create_dir(c0.mnt.join(dir))?;
    }
    eventually("subtrees visible on both", Duration::from_secs(30), || {
        anyhow::ensure!(
            c1.mnt.join("from-a").is_dir() && c1.mnt.join("from-b").is_dir(),
            "subtrees not on c1"
        );
        Ok(())
    })?;
    for _ in 0..2 {
        w0.run_block(&c0.mnt.join("from-a"), &mut m0, 15)?;
        w1.run_block(&c1.mnt.join("from-b"), &mut m1, 15)?;
        eventually("cross-node convergence", Duration::from_secs(60), || {
            m0.verify(&c0.mnt.join("from-a")).context("a via c0")?;
            m0.verify(&c1.mnt.join("from-a")).context("a via c1")?;
            m1.verify(&c1.mnt.join("from-b")).context("b via c1")?;
            m1.verify(&c0.mnt.join("from-b")).context("b via c0")?;
            Ok(())
        })?;
    }
    ensure_no_conflicts(&[&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Deterministic, non-repeating content for a `len`-byte file, so a
/// stale (partially overwritten) buffer cannot pass as correct.
fn pattern(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut i: u64 = 0;
    while out.len() < len {
        out.extend_from_slice(blake3::hash(&(seed ^ i).to_le_bytes()).as_bytes());
        i += 1;
    }
    out.truncate(len);
    out
}

/// Plan 07's exit criterion: daemon RSS must not scale with the size
/// of the file being written. A small `--cache-size` makes the old
/// (pre-staging) behavior's failure mode obvious — without bounded
/// staging, an in-flight write several times the cache budget would
/// have to inflate RSS by roughly that much.
fn big_file_write(seed: u64) -> Result<()> {
    let (env, root) = setup("big-file-write")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/bigwrite-{}", ts());
    let cache_size = 64 * 1024 * 1024u64; // 64 MiB
    let mut c =
        Client::new(root.path(), "c0", &env.endpoint, &backend)?.with_cache_size(cache_size);
    c.fs_create()?;
    c.mount()?;

    // A few hundred MiB is enough to prove the slope is flat while
    // staying CI-sane; several times the cache budget either way.
    let file_len = 300usize * 1024 * 1024;
    let data = pattern(seed, file_len);
    let path = c.mnt.join("big.bin");

    let mut peak_rss = c.rss_bytes().context("baseline RSS")?;
    let write = std::thread::scope(|scope| -> Result<()> {
        let writer = scope.spawn(|| -> Result<()> {
            use std::io::Write;
            let mut f = std::fs::File::create(&path)?;
            // Write in modest pieces so the sampler below gets several
            // readings while the write is still in flight.
            for chunk in data.chunks(4 * 1024 * 1024) {
                f.write_all(chunk)?;
            }
            f.sync_all()?;
            Ok(())
        });
        while !writer.is_finished() {
            if let Ok(rss) = c.rss_bytes() {
                peak_rss = peak_rss.max(rss);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        writer.join().expect("writer panicked")
    });
    write.context("writing the big file")?;
    peak_rss = peak_rss.max(c.rss_bytes().context("post-write RSS")?);

    // The whole point: peak RSS stays within a fixed ceiling well below
    // the file size, and specifically below the cache budget plus a
    // generous fixed overhead for the rest of the daemon (metadata,
    // thread stacks, etc.) — not O(file_len).
    let ceiling = cache_size + 200 * 1024 * 1024;
    anyhow::ensure!(
        peak_rss < ceiling,
        "peak RSS {peak_rss} bytes exceeded the {ceiling}-byte ceiling for a \
         {file_len}-byte write with a {cache_size}-byte cache budget; \
         daemon log:\n{}",
        c.tail_log_n(60)
    );
    eprintln!(
        "big-file-write: peak RSS {} MiB, cache budget {} MiB, file {} MiB",
        peak_rss / 1024 / 1024,
        cache_size / 1024 / 1024,
        file_len / 1024 / 1024
    );

    let back = std::fs::read(&path)?;
    anyhow::ensure!(back == data, "readback mismatch after the big write");
    c.unmount()?;
    Ok(())
}

fn xattr_roundtrip(_seed: u64) -> Result<()> {
    let (env, root) = setup("xattr-roundtrip")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("xattr-roundtrip-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut a = Client::new(root.path(), "a", &env.endpoint, &backend)?;
    let mut b = Client::new(root.path(), "b", &env.endpoint, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;

    let dir = a.mnt.join("tree");
    std::fs::create_dir(&dir)?;
    let file = dir.join("file");
    std::fs::write(&file, b"seven!!")?;
    let sparse = dir.join("sparse");
    std::fs::File::create(&sparse)?.set_len(1 << 30)?;
    set_xattr(&file, "user.foo", b"file-value")?;
    set_xattr(&dir, "user.foo", b"dir-value")?;

    eventually(
        "xattrs visible on second node",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(
                get_xattr(&b.mnt.join("tree/file"), "user.foo")? == b"file-value",
                "file xattr not yet visible"
            );
            anyhow::ensure!(
                get_xattr(&b.mnt.join("tree"), "user.foo")? == b"dir-value",
                "directory xattr not yet visible"
            );
            Ok(())
        },
    )?;
    let expected = (1u64 << 30) + 7;
    let rsize = String::from_utf8(get_xattr(&b.mnt.join("tree"), "user.constellation.rsize")?)?
        .parse::<u64>()?;
    let rcount = String::from_utf8(get_xattr(&b.mnt.join("tree"), "user.constellation.rcount")?)?
        .parse::<u64>()?;
    anyhow::ensure!(rsize == expected, "rsize {rsize} != {expected}");
    anyhow::ensure!(rcount == 2, "rcount {rcount} != 2");

    remove_xattr(&file, "user.foo")?;
    eventually(
        "xattr removal visible",
        Duration::from_secs(20),
        || match get_xattr(&b.mnt.join("tree/file"), "user.foo") {
            Err(error) if Code::from_os_error(&error) == Some(Code::NoData) => Ok(()),
            Ok(_) => anyhow::bail!("removed xattr still visible"),
            Err(error) => Err(error.into()),
        },
    )?;
    a.unmount()?;
    b.unmount()?;
    eprintln!("    xattr-roundtrip: rsize={rsize} rcount={rcount} sparse_holes=logical");
    Ok(())
}

fn c_path(path: &std::path::Path) -> Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(std::ffi::CString::new(path.as_os_str().as_bytes())?)
}

fn set_xattr(path: &std::path::Path, name: &str, value: &[u8]) -> std::io::Result<()> {
    let path = c_path(path).map_err(std::io::Error::other)?;
    let name = std::ffi::CString::new(name).map_err(std::io::Error::other)?;
    let result = unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn get_xattr(path: &std::path::Path, name: &str) -> std::io::Result<Vec<u8>> {
    let path = c_path(path).map_err(std::io::Error::other)?;
    let name = std::ffi::CString::new(name).map_err(std::io::Error::other)?;
    let size = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut value = vec![0u8; size as usize];
    let read = unsafe {
        libc::getxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
        )
    };
    if read < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        value.truncate(read as usize);
        Ok(value)
    }
}

fn remove_xattr(path: &std::path::Path, name: &str) -> std::io::Result<()> {
    let path = c_path(path).map_err(std::io::Error::other)?;
    let name = std::ffi::CString::new(name).map_err(std::io::Error::other)?;
    let result = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn fallocate_sparse(seed: u64) -> Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;

    let (env, root) = setup("fallocate-sparse")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("fallocate-sparse-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let cache_size = 32 * 1024 * 1024u64;
    let file_len = 256 * 1024 * 1024u64;
    let chunk = 4 * 1024 * 1024u64;
    let mut a = Client::new(root.path(), "a", &env.endpoint, &backend)?
        .with_cache_size(cache_size)
        .with_env("CONSTELLATION_STAGING_BUDGET", "16777216");
    a.fs_create()?;
    a.mount()?;
    let path = a.mnt.join("sparse.bin");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;

    file.set_len(file_len)?;
    file.write_all_at(b"HEAD", 0)?;
    file.write_all_at(b"TAIL", file_len - 4)?;
    for index in 0..3 {
        let middle = pattern(seed.wrapping_add(index), chunk as usize);
        file.write_all_at(&middle, (8 + index) * chunk)?;
    }
    file.sync_all()?;

    let punch_offset = 9 * chunk;
    let rc = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            punch_offset as libc::off_t,
            chunk as libc::off_t,
        )
    };
    anyhow::ensure!(
        rc == 0,
        "hole punch failed: {}\ndaemon log:\n{}",
        std::io::Error::last_os_error(),
        a.tail_log_n(30)
    );
    file.sync_all()?;
    let mut punched = vec![1u8; chunk as usize];
    file.read_exact_at(&mut punched, punch_offset)?;
    anyhow::ensure!(
        punched.iter().all(|byte| *byte == 0),
        "punched chunk was not zero"
    );

    let hole = unsafe { libc::lseek(file.as_raw_fd(), punch_offset as i64, libc::SEEK_HOLE) };
    let next_data = unsafe { libc::lseek(file.as_raw_fd(), punch_offset as i64, libc::SEEK_DATA) };
    anyhow::ensure!(hole == punch_offset as i64, "SEEK_HOLE returned {hole}");
    anyhow::ensure!(
        next_data == (10 * chunk) as i64,
        "SEEK_DATA returned {next_data}"
    );

    file.write_all_at(b"R", punch_offset + 123)?;
    file.sync_all()?;
    drop(file);
    eventually("sparse uploads drain", Duration::from_secs(30), || {
        anyhow::ensure!(a.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0));
        Ok(())
    })?;
    let peak_rss = a.rss_bytes()?;
    let chunk_objects = raw_chunk_count(&env, &prefix)?;
    anyhow::ensure!(
        chunk_objects < 24,
        "sparse file materialized {chunk_objects} chunk objects"
    );
    anyhow::ensure!(
        peak_rss < cache_size + 200 * 1024 * 1024,
        "sparse file RSS {} MiB exceeded ceiling",
        peak_rss / 1024 / 1024
    );
    anyhow::ensure!(std::fs::metadata(&path)?.len() == file_len);
    a.unmount()?;

    let mut b = Client::new(root.path(), "b", &env.endpoint, &backend)?.with_cache_size(cache_size);
    b.mount()?;
    let second = std::fs::File::open(b.mnt.join("sparse.bin"))?;
    let mut marker = [0u8; 4];
    second.read_exact_at(&mut marker, 0)?;
    anyhow::ensure!(&marker == b"HEAD");
    second.read_exact_at(&mut marker, file_len - 4)?;
    anyhow::ensure!(&marker == b"TAIL");
    let mut rewritten = [0u8; 3];
    second.read_exact_at(&mut rewritten, punch_offset + 122)?;
    anyhow::ensure!(rewritten == [0, b'R', 0], "rewrite in hole did not persist");
    let mut zero = [1u8; 64];
    second.read_exact_at(&mut zero, punch_offset + 4096)?;
    anyhow::ensure!(zero == [0; 64], "fresh node did not observe sparse zeros");
    drop(second);
    b.unmount()?;

    eprintln!(
        "    fallocate-sparse: file_size={} MiB chunk_objects={} rss={} MiB cache={} MiB",
        file_len / 1024 / 1024,
        chunk_objects,
        peak_rss / 1024 / 1024,
        cache_size / 1024 / 1024
    );
    Ok(())
}

/// `kill -9` mid-write must not corrupt the file: staging is scratch,
/// so the file lands at whatever size/content its last successful
/// `close`/`fsync` committed, and `staging/` is empty after the next
/// mount's GC. "Absent or short is a pass; corrupt is not" (plan 07).
fn staging_crash(seed: u64) -> Result<()> {
    let (env, root) = setup("staging-crash")?;
    let _proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("stagecrash-{}", ts()))?;

    // A committed baseline the crash must not disturb.
    let committed = pattern(seed, 8 * 1024 * 1024);
    let path = c.mnt.join("f.bin");
    std::fs::write(&path, &committed)?;

    // Start a second, larger write and kill mid-flight, well before it
    // could plausibly close (no fsync => nothing beyond `committed` is
    // guaranteed durable — the same POSIX contract as today's RAM
    // buffer).
    let extra = pattern(seed.wrapping_add(1), 64 * 1024 * 1024);
    let state_dir = root.path().join("c0").join("state");
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            use std::io::Write;
            // Open for append-in-place: a fresh handle so partial bytes
            // never get past this scope on their own.
            if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open(&path) {
                for chunk in extra.chunks(1024 * 1024) {
                    if f.write_all(chunk).is_err() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        });
        std::thread::sleep(Duration::from_millis(30));
        let _ = c.kill9();
        let _ = writer.join();
    });

    c.mount().context("remount after staging crash")?;
    let staging_dir = state_dir.join("staging");
    let leftover = std::fs::read_dir(&staging_dir)
        .map(|it| it.count())
        .unwrap_or(0);
    anyhow::ensure!(
        leftover == 0,
        "staging/ must be empty after mount-time GC, found {leftover} entries"
    );

    let after = std::fs::read(&path)?;
    anyhow::ensure!(
        after == committed || after.len() < committed.len() + extra.len(),
        "post-crash content is neither the last committed state nor visibly short: len={}",
        after.len()
    );
    // The prefix that *is* present must be uncorrupted, whichever of
    // the two shapes above it turned out to be.
    let n = after.len().min(committed.len());
    anyhow::ensure!(
        after[..n] == committed[..n],
        "post-crash content diverges from the committed baseline within the shared prefix"
    );
    c.unmount()?;
    Ok(())
}

/// Regression test for prerequisite 2 (plan 07): today's tree ships
/// the journal on clean unmount without draining `pending_upload`
/// first, so a chunk that only made it into the local cache (S3 PUT cut
/// by the proxy) gets published as if it were durable in S3. This must
/// fail on today's tree and pass once unmount gates on the upload.
fn unmount_drain(seed: u64) -> Result<()> {
    let (env, root) = setup("unmount-drain")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/unmount-drain-{}", ts());
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    c0.fs_create()?;
    c0.mount()?;

    let data = pattern(seed, 2 * 1024 * 1024);
    let path = c0.mnt.join("f.bin");
    // The empty file first, while S3 is up, so the node holds the lease
    // when it goes (`hold_lease_before_cut`).
    std::fs::File::create(&path)?;
    hold_lease_before_cut(&c0)?;

    // Cut S3 before the write so the eager upload in `flush_inode`
    // cannot succeed; the chunk lands in the local cache Dirty and in
    // `pending_upload`, exactly the state an orderly unmount must not
    // ship over.
    proxy.cut()?;
    std::fs::write(&path, &data).context("writing while S3 is cut")?;

    // Unmount while S3 is still down: on the fixed tree this must
    // refuse to finish cleanly (skip_ship) rather than shipping a
    // manifest for a chunk that was never PUT. `unmount()` only checks
    // that the daemon process exits, not its exit code, so failure here
    // shows up as the second node never seeing the file below.
    let _ = c0.unmount();
    proxy.heal()?;

    // Remount and let the drain run, then unmount cleanly for real this
    // time — the second unmount (S3 healthy) is what actually ships.
    c0.mount()
        .context("remount after the drained unmount attempt")?;
    std::thread::sleep(Duration::from_secs(2));
    c0.unmount()?;

    // A second, independent node must be able to read the file with no
    // missing chunk — the regression this scenario targets.
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    c1.mount()?;
    let back = std::fs::read(c1.mnt.join("f.bin"))
        .context("second node reading the file written under the cut")?;
    anyhow::ensure!(back == data, "second node saw corrupt/partial content");
    c1.unmount()?;
    Ok(())
}

fn timed_small_file_import(mode: &str, tag: &str) -> Result<Duration> {
    let (env, root) = setup(tag)?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/{tag}-{}", ts());
    let mut client = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_write_mode(mode)
        .with_env("CONSTELLATION_UPLOAD_CONCURRENCY", "8");
    client.fs_create()?;
    client.mount()?;
    proxy.latency(150, 0)?;
    let started = std::time::Instant::now();
    for i in 0..24 {
        let data = pattern(i, 8192);
        std::fs::write(client.mnt.join(format!("f-{i:03}")), data)?;
    }
    let elapsed = started.elapsed();
    proxy.heal()?;
    client.set_write_mode("through")?;
    for i in 0..24 {
        anyhow::ensure!(
            std::fs::read(client.mnt.join(format!("f-{i:03}")))? == pattern(i, 8192),
            "{mode} import content mismatch at file {i}"
        );
    }
    client.unmount()?;
    Ok(elapsed)
}

fn writeback_latency(_seed: u64) -> Result<()> {
    let through = timed_small_file_import("through", "writeback-latency-through")?;
    let back = timed_small_file_import("back", "writeback-latency-back")?;
    eprintln!("    writeback-latency: through={through:.2?} back={back:.2?}");
    anyhow::ensure!(
        back * 3 < through,
        "write-back must beat write-through by 3x, through={through:?} back={back:?}"
    );
    Ok(())
}

fn writeback_bigfile(seed: u64) -> Result<()> {
    let (env, root) = setup("writeback-bigfile")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/writeback-big-{}", ts());
    let cache_size = 32 * 1024 * 1024u64;
    let mut client = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_cache_size(cache_size)
        .with_write_mode("back");
    client.fs_create()?;
    client.mount()?;
    let data = pattern(seed, (cache_size * 10) as usize);
    let expected = blake3::hash(&data);
    let path = client.mnt.join("ten-x.bin");
    let mut peak_rss = 0;
    let mut peak_cache = 0;
    std::thread::scope(|scope| -> Result<()> {
        let writer = scope.spawn(|| -> Result<()> {
            use std::io::Write;
            let mut file = std::fs::File::create(&path)?;
            for piece in data.chunks(1024 * 1024) {
                file.write_all(piece)?;
            }
            Ok(())
        });
        while !writer.is_finished() {
            peak_rss = peak_rss.max(client.rss_bytes().unwrap_or(0));
            peak_cache = peak_cache.max(
                client.control_status()?["cache"]["used_bytes"]
                    .as_u64()
                    .unwrap_or(0),
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        writer.join().expect("writer panicked")
    })?;
    client.set_write_mode("through")?;
    let rss_ceiling = cache_size + 240 * 1024 * 1024;
    anyhow::ensure!(peak_rss < rss_ceiling, "RSS {peak_rss} >= {rss_ceiling}");
    anyhow::ensure!(
        peak_cache <= cache_size,
        "cache {peak_cache} exceeded budget {cache_size}"
    );
    anyhow::ensure!(blake3::hash(&std::fs::read(&path)?) == expected);
    eprintln!(
        "    writeback-bigfile: file={} MiB peak_rss={} MiB peak_cache={} MiB",
        data.len() / 1024 / 1024,
        peak_rss / 1024 / 1024,
        peak_cache / 1024 / 1024
    );
    client.unmount()?;
    Ok(())
}

fn writeback_drain(seed: u64) -> Result<()> {
    let (env, root) = setup("writeback-drain")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/writeback-drain-{}", ts());
    let mut a = Client::new(root.path(), "a", &env.endpoint, &backend)?.with_write_mode("back");
    a.fs_create()?;
    a.mount()?;
    let data = pattern(seed, 24 * 1024 * 1024);
    std::fs::write(a.mnt.join("payload"), &data)?;
    a.set_write_mode("through")?;
    let status = a.control_status()?;
    anyhow::ensure!(
        status["writeback"]["pending_uploads"].as_u64() == Some(0),
        "back-to-through returned with pending uploads: {status}"
    );
    a.unmount()?;
    let mut b = Client::new(root.path(), "b", &env.endpoint, &backend)?;
    b.mount()?;
    anyhow::ensure!(std::fs::read(b.mnt.join("payload"))? == data);
    b.unmount()?;
    Ok(())
}

fn writeback_fsync(seed: u64) -> Result<()> {
    let (env, root) = setup("writeback-fsync")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/writeback-fsync-{}", ts());
    let mut client =
        Client::new(root.path(), "c0", &env.endpoint, &backend)?.with_write_mode("back");
    client.fs_create()?;
    client.mount()?;
    let committed = pattern(seed, 4 * 1024 * 1024);
    {
        use std::io::Write;
        let mut file = std::fs::File::create(client.mnt.join("f"))?;
        file.write_all(&committed)?;
        file.sync_all()?;
        file.write_all(b"unsynced-tail")?;
        std::mem::forget(file);
    }
    client.kill9()?;
    client.mount()?;
    let got = std::fs::read(client.mnt.join("f"))?;
    anyhow::ensure!(
        got.starts_with(&committed),
        "fsynced prefix was lost or corrupted"
    );
    client.unmount()?;
    writeback_fsync_after_close(seed, &env, root.path())
}

/// `writeback-fsync`'s second phase (plan 39b): an `fsync` from a *new*
/// descriptor, after the writer closed under `--write-mode back`, makes
/// the file durable in S3 under the default `--fsync-mode local` — the
/// PostgreSQL checkpointer shape. B holds the lease; A (not the
/// sequencer) writes `d/f` under `back` with its background uploads held
/// (a metered network, plan 31 C8), so the close leaves the chunks queued
/// on A and B awaits them. A opens the file again, `fsync`s, and is
/// killed at once: B must read the content straight away, from S3, with
/// no `CONSTELLATION_REMOTE_CHUNK_WAIT_S` stall (30 s here). Before 39b
/// the `fsync` returned 0 without uploading, and B's read waited out the
/// 30 s and failed: the only copy died with A.
fn writeback_fsync_after_close(seed: u64, env: &S3Env, root: &std::path::Path) -> Result<()> {
    const WAIT_S: u64 = 30;
    let backend = format!("s3://{BUCKET}/writeback-fsync-close-{}", ts());
    let mk = |name: &str| -> Result<Client> {
        Ok(Client::new(root, name, &env.endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
            .with_env("CONSTELLATION_REMOTE_CHUNK_WAIT_S", &WAIT_S.to_string()))
    };
    let mut b = mk("b")?;
    let mut a = mk("a")?
        .with_write_mode("back")
        .with_env("CONSTELLATION_PROFILE_UPLOADS", "unmetered-only");
    b.fs_create()?;
    b.mount()?;
    a.mount()?;
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&a, &b])?;
        std::fs::create_dir(b.mnt.join("d"))?;
        eventually("B holds the lease", Duration::from_secs(30), || {
            anyhow::ensure!(lease_of(&b)?["held"] == true, "{}", lease_of(&b)?);
            Ok(())
        })?;
        eventually("d/ visible on A", Duration::from_secs(30), || {
            anyhow::ensure!(a.mnt.join("d").is_dir(), "not yet");
            Ok(())
        })?;
        let report = a.control(
            "node.lifecycle",
            serde_json::json!({ "event": { "NetworkChanged": { "reachable": true, "metered": true } } }),
        )?;
        anyhow::ensure!(
            report["status"]["uploads_held"] == true,
            "A's uploads are not held: {report}"
        );
        let data = pattern(seed ^ 0x39b, 3 * 1024 * 1024 + 12_345);
        // The writer: write, close. No fsync.
        std::fs::write(a.mnt.join("d/f"), &data)?;
        let pending = a.control_status()?["writeback"]["pending_uploads"]
            .as_u64()
            .unwrap_or(0);
        anyhow::ensure!(pending > 0, "the back close left nothing queued on A");
        eventually("B awaits A's chunks", Duration::from_secs(10), || {
            let awaited = b.control_status()?["writeback"]["remote_chunks_awaited"]
                .as_u64()
                .unwrap_or(0);
            anyhow::ensure!(awaited > 0, "B awaits nothing");
            Ok(())
        })?;
        // The checkpointer: a new descriptor, fsync.
        let t = std::time::Instant::now();
        std::fs::File::open(a.mnt.join("d/f"))?.sync_all()?;
        let fsync_took = t.elapsed();
        let a_status = a.control_status()?;
        let b_awaits = b.control_status()?["writeback"]["remote_chunks_awaited"].clone();
        eprintln!(
            "    writeback-fsync: fsync after a back close took {fsync_took:?}; A pending {}, \
             B awaits {b_awaits}",
            a_status["writeback"]["pending_uploads"]
        );
        anyhow::ensure!(
            a_status["writeback"]["pending_uploads"].as_u64() == Some(0),
            "A's fsync returned with chunks still queued: {}",
            a_status["writeback"]
        );
        anyhow::ensure!(
            b_awaits.as_u64() == Some(0),
            "A's fsync returned before B learned the chunks are up: {b_awaits}"
        );
        // A's disk is gone: nothing but S3 has the bytes now.
        a.kill9()?;
        let t = std::time::Instant::now();
        let got = std::fs::read(b.mnt.join("d/f"))?;
        let read_took = t.elapsed();
        eprintln!("    writeback-fsync: B's read with A dead took {read_took:?}");
        anyhow::ensure!(
            got == data,
            "B read {} bytes, not A's fsynced content",
            got.len()
        );
        anyhow::ensure!(
            read_took < Duration::from_secs(5),
            "B's read stalled {read_took:?} (CONSTELLATION_REMOTE_CHUNK_WAIT_S = {WAIT_S})"
        );
        Ok(())
    })();
    if result.is_err() {
        for c in [&a, &b] {
            eprintln!("--- {} log ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    let _ = a.kill9();
    let _ = b.unmount();
    result
}

fn writeback_backpressure(seed: u64) -> Result<()> {
    let (env, root) = setup("writeback-backpressure")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/writeback-pressure-{}", ts());
    let cache_size = 8 * 1024 * 1024u64;
    let mut client = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_cache_size(cache_size)
        .with_write_mode("back")
        .with_env("CONSTELLATION_STAGING_BUDGET", "2097152");
    client.fs_create()?;
    client.mount()?;
    let path = client.mnt.join("pressure");
    let mut file = std::fs::File::create(&path)?;
    hold_lease_before_cut(&client)?;
    proxy.cut()?;
    let started = std::time::Instant::now();
    let mut saw_enospc = false;
    for i in 0..32 {
        use std::io::Write;
        let block = pattern(seed.wrapping_add(i), 1024 * 1024);
        if let Err(error) = file.write_all(&block) {
            saw_enospc = Code::from_os_error(&error) == Some(Code::NoSpace);
            break;
        }
    }
    anyhow::ensure!(saw_enospc, "dirty hard limit did not return ENOSPC");
    anyhow::ensure!(
        started.elapsed() >= Duration::from_millis(100),
        "ENOSPC arrived without observable throttling"
    );
    drop(file);
    proxy.heal()?;
    eventually("write-back queue recovers", Duration::from_secs(30), || {
        anyhow::ensure!(
            client.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0)
        );
        Ok(())
    })?;
    std::fs::write(client.mnt.join("after-heal"), b"ok")?;
    client.unmount()?;
    Ok(())
}

/// Same-path conflict races via constellation-chaos Ci profile on three
/// local mounts of one filesystem (no VMs).
fn chaos_ci(seed: u64) -> Result<()> {
    chaos_ci_with(seed, false)
}

/// Plan 30 §M8: `chaos-ci` with every mount `--cto strict`; the history
/// records it, so the close-to-open checker is enforced on it.
fn chaos_ci_strict(seed: u64) -> Result<()> {
    chaos_ci_with(seed, true)
}

fn chaos_ci_with(seed: u64, strict: bool) -> Result<()> {
    use constellation_chaos::{Coordinator, LocalCluster, Profile};

    let name = if strict {
        "chaos-ci-strict"
    } else {
        "chaos-ci"
    };
    let (env, root) = setup(name)?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/chaos-{}", ts());
    // Distinct P2P identities: three real nodes creating/writing
    // concurrently in one namespace need the forward/handoff fast path to
    // avoid piling onto the S3-only lease CAS/TTL wait — without its own
    // key each client falls back to the harness's shared default node
    // key, so all three register the *same* iroh identity and
    // `refresh_registry` refuses to dial any of the others as "ourself"
    // (see its "peer registered with OUR node key" warning). That leaves
    // every non-holder's write with no path but the slow one, and with
    // three-way contention on one partition it can starve past the FUSE
    // acquire deadline (2×TTL) and surface as EIO — which is exactly
    // "unexpected errno EIO on worker N during create_storm" (the sibling
    // scenarios `chaos-soak-4`/`disjoint-write-4` already call this for
    // the same reason).
    let mode = if strict { "strict" } else { "bounded" };
    let mk = |who: &str| -> Result<Client> {
        Ok(Client::new(root.path(), who, &env.endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_CTO", mode))
    };
    let mut c0 = mk("c0")?;
    let mut c1 = mk("c1")?;
    let mut c2 = mk("c2")?;
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    c2.mount()?;

    // Brief settle so all three see the shared namespace.
    eventually("three mounts live", Duration::from_secs(20), || {
        std::fs::create_dir_all(c0.mnt.join(".chaos-probe")).ok();
        anyhow::ensure!(
            c1.mnt.join(".chaos-probe").is_dir()
                || c2.mnt.join(".chaos-probe").is_dir()
                || c0.mnt.join(".chaos-probe").is_dir()
        );
        Ok(())
    })
    .ok();

    let store = root.path().join("chaos-store");
    std::fs::create_dir_all(&store)?;
    let mounts = vec![c0.mnt.clone(), c1.mnt.clone(), c2.mnt.clone()];
    let mut cluster = LocalCluster::new(mounts)?;
    let mut profile = Profile::ci(seed, 3);
    profile.cto_strict = strict;
    let work_root = profile.work_root.clone();
    if let Err(e) = Coordinator::run(&mut cluster, profile, &store) {
        diagnose_divergence(name, &format!("{e:#}"), &[&c0, &c1, &c2]);
        return Err(e).with_context(|| format!("{name} artifacts under {}", store.display()));
    }
    // Plan 30 M4: convergence at quiescence (a fresh replica included)
    // and exactly-once in the log.
    m4::after_chaos(&env, root.path(), &backend, &[&c0, &c1, &c2], &work_root)?;

    c0.unmount()?;
    c1.unmount()?;
    c2.unmount()?;
    Ok(())
}

/// Plan 29 M3c: `chaos-ci`'s EIO ("unexpected errno EIO on worker N during
/// create_storm") turned out to be P2P being dead (M3b gave every client
/// its own node key, closing that gap). But with P2P genuinely unavailable
/// — `CONSTELLATION_P2P=off`, or peers unreachable — a healthy, merely
/// *contended* cluster must still never surface EIO: a non-holder's
/// mutation falls back to the S3 lease CAS/TTL path, and under sustained
/// 3-way contention the holder's own write-idle timer never fires (it
/// never goes idle) and the sticky-lease `wanted_by` handoff can be starved
/// by a continuously non-empty local journal — see the M3c fix in
/// `crates/cli/src/lease.rs` for the mechanism.
///
/// Three clients, each with its own node key and `CONSTELLATION_P2P=off`
/// (so forwarding/handoff can never mask a lease problem), hammer
/// create/write(/read)/unlink of their own uniquely-named files in one
/// shared directory for `CHAOS_CREATE_STORM_SECS` (default 30s). A lease
/// TTL of 20s (FUSE acquire deadline 2xTTL = 40s) leaves real headroom
/// over `LEASE_MIN_DWELL_MS`/`LEASE_WANTED_GRACE_MS` (5s each, fixed
/// regardless of TTL): the sticky-lease notice delay is up to TTL/2 on
/// its own, and a self-reclaim by the just-released holder (bounded by
/// `HANDOFF_PAUSE_MS`, not eliminated by it) can chain a waiter through
/// more than one dwell+grace cycle before it finally wins the CAS — an
/// aggressively short TTL leaves too little margin over that compounded
/// worst case, which is a test-tuning concern given the fixed floors,
/// not something a shorter TTL is entitled to assume away. Every
/// worker's storm result is checked for an unexpected errno (EIO chief
/// among them); a final marker file per client proves the three mounts
/// converge to the same listing and contents.
fn create_storm_s3_only(seed: u64) -> Result<()> {
    use rand::{rngs::StdRng, Rng, SeedableRng};

    let (env, root) = setup("create-storm-s3-only")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/create-storm-s3-only-{}", ts());
    let ttl_ms = 20_000u64;
    let mk = |c: Client| {
        c.with_own_node_key()
            .with_env("CONSTELLATION_P2P", "off")
            .with_env("CONSTELLATION_LEASE_TTL_MS", &ttl_ms.to_string())
    };
    let mut c0 = mk(Client::new(root.path(), "c0", &env.endpoint, &backend)?);
    let mut c1 = mk(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
    let mut c2 = mk(Client::new(root.path(), "c2", &env.endpoint, &backend)?);
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;
    c2.mount()?;

    let dir = "storm";
    std::fs::create_dir_all(c0.mnt.join(dir))?;
    eventually(
        "shared storm dir visible on all mounts",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(c1.mnt.join(dir).is_dir());
            anyhow::ensure!(c2.mnt.join(dir).is_dir());
            Ok(())
        },
    )?;

    let storm_secs = std::env::var("CHAOS_CREATE_STORM_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let mounts = [c0.mnt.clone(), c1.mnt.clone(), c2.mnt.clone()];
    let deadline = std::time::Instant::now() + Duration::from_secs(storm_secs);
    let errors: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut rng = StdRng::seed_from_u64(seed);
    let mut handles = Vec::new();
    for (idx, mnt) in mounts.iter().cloned().enumerate() {
        let errors = errors.clone();
        let dir = dir.to_string();
        let mut wrng = StdRng::seed_from_u64(rng.random());
        handles.push(std::thread::spawn(move || -> u64 {
            let mut n = 0u64;
            while std::time::Instant::now() < deadline {
                n += 1;
                let path = mnt.join(&dir).join(format!("w{idx}-{n}"));
                let len = wrng.random_range(16..256);
                let mut content = format!("worker {idx} file {n} ").into_bytes();
                content.resize(len, b'x');
                if let Err(e) = std::fs::write(&path, &content) {
                    errors
                        .lock()
                        .unwrap()
                        .push(format!("worker {idx} write #{n} ({path:?}): {e}"));
                    continue;
                }
                match std::fs::read(&path) {
                    Ok(got) if got == content => {}
                    Ok(got) => errors.lock().unwrap().push(format!(
                        "worker {idx} read #{n} ({path:?}): content mismatch, got {} bytes want {}",
                        got.len(),
                        content.len()
                    )),
                    Err(e) => errors
                        .lock()
                        .unwrap()
                        .push(format!("worker {idx} read #{n} ({path:?}): {e}")),
                }
                if let Err(e) = std::fs::remove_file(&path) {
                    errors
                        .lock()
                        .unwrap()
                        .push(format!("worker {idx} unlink #{n} ({path:?}): {e}"));
                }
                if wrng.random_bool(0.1) {
                    std::thread::sleep(Duration::from_millis(wrng.random_range(0..5)));
                }
            }
            n
        }));
    }
    let mut totals = Vec::new();
    for h in handles {
        totals.push(
            h.join()
                .map_err(|_| anyhow::anyhow!("create-storm-s3-only: worker thread panicked"))?,
        );
    }
    let errs = errors.lock().unwrap().clone();
    if !errs.is_empty() {
        eprintln!(
            "create-storm-s3-only FAILED: {} unexpected errno(s)",
            errs.len()
        );
        for c in [&c0, &c1, &c2] {
            eprintln!("--- {} mount.log (tail) ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    anyhow::ensure!(
        errs.is_empty(),
        "create-storm-s3-only: {} unexpected errno(s) during the storm (totals {totals:?}):\n{}",
        errs.len(),
        errs.join("\n")
    );
    eprintln!("create-storm-s3-only: completed op counts per worker = {totals:?}");

    // Convergence: one persistent marker per client, then every mount must
    // agree on the shared directory's final listing and contents.
    let mut markers = Vec::new();
    for (idx, mnt) in mounts.iter().enumerate() {
        let name = format!("marker-{idx}");
        let content = format!("final marker from worker {idx}").into_bytes();
        std::fs::write(mnt.join(dir).join(&name), &content)?;
        markers.push((name, content));
    }
    let mut want: Vec<String> = markers.iter().map(|(n, _)| n.clone()).collect();
    want.sort();
    eventually(
        "final storm-dir listing converges on all mounts",
        Duration::from_secs(30),
        || {
            for mnt in &mounts {
                let mut names: Vec<String> = std::fs::read_dir(mnt.join(dir))?
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                names.sort();
                anyhow::ensure!(
                    names == want,
                    "{}: storm-dir listing {:?} != expected {:?}",
                    mnt.display(),
                    names,
                    want
                );
                for (name, content) in &markers {
                    let got = std::fs::read(mnt.join(dir).join(name))?;
                    anyhow::ensure!(
                        &got == content,
                        "{}: marker {name} content mismatch",
                        mnt.display()
                    );
                }
            }
            Ok(())
        },
    )?;

    c0.unmount()?;
    c1.unmount()?;
    c2.unmount()?;
    Ok(())
}

/// The path a chaos convergence failure names: the coordinator reports
/// `convergence not reached within .. after <tag>: <key>: [(worker,
/// observation), ..]`, the key being the verified path (or
/// `read_at:<path>@<off>+<len>`).
fn diverged_path(err: &str) -> Option<String> {
    let rest = err.split("convergence not reached within ").nth(1)?;
    let (_, after) = rest.split_once(" after ")?;
    let (_tag, detail) = after.split_once(": ")?;
    let key = detail.split(": [").next()?;
    let key = key.strip_prefix("read_at:").unwrap_or(key);
    let key = key.split('@').next()?;
    (!key.is_empty()).then(|| key.to_string())
}

/// On a chaos convergence failure, what each node serves for the path
/// the coordinator could not get agreement on, and whether it converges
/// if given longer: the content (length, BLAKE3 as the chaos checker
/// hashes it), the node's metadata view (`inspect`: inode, size, mtime,
/// the manifest's digest, length and chunk ids), and its position (the
/// log sequence it holds, the lease as it sees it) — sampled now and 30,
/// 60 and 120 s later. A late convergence is slowness; a node still
/// serving other content after two minutes is a divergence.
fn diagnose_divergence(scenario: &str, err: &str, clients: &[&Client]) {
    let Some(path) = diverged_path(err) else {
        return;
    };
    eprintln!("    {scenario}: diagnosing the divergence on {path}");
    let started = std::time::Instant::now();
    for wait in [0u64, 30, 60, 120] {
        let at = started + Duration::from_secs(wait);
        if let Some(d) = at.checked_duration_since(std::time::Instant::now()) {
            std::thread::sleep(d);
        }
        let mut contents = Vec::new();
        for c in clients {
            let content = match std::fs::read(c.mnt.join(&path)) {
                Ok(b) => format!(
                    "len {} blake3 {}",
                    b.len(),
                    &blake3::hash(&b).to_hex()[..16]
                ),
                Err(e) => format!("read error: {e}"),
            };
            let entry = c
                .control(
                    "browse.inspect",
                    serde_json::json!({"path": format!("/{path}")}),
                )
                .map(|v| {
                    let e = &v;
                    let m = &e["manifest"];
                    format!(
                        "ino {:#x} size {} mtime_ns {} manifest {{len {} digest {} chunks {}}}",
                        e["ino"].as_u64().unwrap_or(0),
                        e["size"],
                        e["mtime_ns"],
                        m["file_len"],
                        m["digest"]
                            .as_str()
                            .map(|d| &d[..d.len().min(16)])
                            .unwrap_or("-"),
                        m["chunks"]
                    )
                })
                .unwrap_or_else(|e| format!("inspect failed: {e:#}"));
            let position = c
                .control_status()
                .map(|v| {
                    format!(
                        "head_seq {} lease {{holder {} epoch {} held {}}}",
                        v["spool"]["head_seq"],
                        v["lease"]["holder"],
                        v["lease"]["epoch"],
                        v["lease"]["held"]
                    )
                })
                .unwrap_or_else(|e| format!("status failed: {e:#}"));
            eprintln!(
                "      +{wait:>3}s {}: {content} | {entry} | {position}",
                c.name
            );
            contents.push(content);
        }
        if contents.windows(2).all(|w| w[0] == w[1]) {
            eprintln!(
                "    {scenario}: {path} converged {:?} after the failure (slow, not diverged)",
                started.elapsed()
            );
            return;
        }
    }
    eprintln!(
        "    {scenario}: {path} still differs {:?} after the failure: a divergence",
        started.elapsed()
    );
}

/// Fleet soak shape on one host: four write-back mounts, soak profile,
/// seed 42. Used to reproduce `disjoint_write` / `write_disjoint:wd41`
/// without EC2.
fn chaos_soak_4(seed: u64) -> Result<()> {
    use constellation_chaos::{Coordinator, LocalCluster, Profile};

    let (env, root) = setup("chaos-soak-4")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/chaos-soak4-{}", ts());
    let duration_secs = std::env::var("CHAOS_SOAK_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let mut clients = Vec::new();
    for i in 0..4 {
        let mut c = Client::new(root.path(), &format!("c{i}"), &env.endpoint, &backend)?
            .with_own_node_key()
            .with_write_mode("back");
        if i == 0 {
            c.fs_create()?;
        }
        c.mount_view(None, &["--fsync-mode", "s3"])?;
        clients.push(c);
    }

    eventually("four mounts live", Duration::from_secs(30), || {
        let probe = clients[0].mnt.join(".chaos-probe");
        std::fs::create_dir_all(&probe).ok();
        anyhow::ensure!(clients.iter().any(|c| c.mnt.join(".chaos-probe").is_dir()));
        Ok(())
    })
    .ok();

    let store = PathBuf::from(format!("/tmp/chaos-soak-4-{}-{}", seed, std::process::id()));
    std::fs::create_dir_all(&store)?;
    let mounts: Vec<_> = clients.iter().map(|c| c.mnt.clone()).collect();
    eprintln!(
        "chaos-soak-4: seed={seed} duration={duration_secs}s mounts={mounts:?} store={}",
        store.display()
    );
    let mut cluster = LocalCluster::new(mounts)?;
    let profile = Profile::soak(seed, 4, duration_secs);
    let work_root = profile.work_root.clone();
    let result = Coordinator::run(&mut cluster, profile, &store).and_then(|()| {
        // Plan 30 M4: convergence at quiescence (a fresh replica
        // included) and exactly-once in the log.
        let refs: Vec<&Client> = clients.iter().collect();
        m4::after_chaos(&env, root.path(), &backend, &refs, &work_root)
    });
    if let Err(ref e) = result {
        eprintln!("chaos-soak-4 FAILED: {e}");
        let refs: Vec<&Client> = clients.iter().collect();
        diagnose_divergence("chaos-soak-4", &format!("{e:#}"), &refs);
        eprintln!("artifacts kept at: {}", store.display());
        for c in &clients {
            eprintln!("--- {} mount.log (tail) ---\n{}", c.name, c.tail_log());
            // The whole mount log next to the history: the tail above is
            // rarely enough to root-cause a double winner.
            let path = store.join(format!("{}.mount.log", c.name));
            let _ = std::fs::write(&path, c.tail_log_n(50_000_000));
            eprintln!("    {}'s log kept at {}", c.name, path.display());
        }
    }
    for c in &mut clients {
        let _ = c.unmount();
    }
    // The history is only worth keeping for a failure; a pass would leave
    // ~600 MiB per run in /tmp (a tmpfs on the CI hosts).
    if result.is_ok() {
        let _ = std::fs::remove_dir_all(&store);
    }
    result.with_context(|| format!("chaos-soak-4 artifacts under {}", store.display()))?;
    Ok(())
}

/// Focused local repro of the fleet `disjoint_write` failure: four
/// write-back mounts repeatedly run the write_disjoint shape (seeded
/// zero file, then four concurrent disjoint WriteAts, then verify).
/// Plan 29 M6: every node runs `mkdir -p <fresh>/<per-thread>` for the
/// same fresh tree at once, then creates a file inside. Only one node's
/// `mkdir` of each shared parent wins; the losers get `EEXIST` from the
/// holder and must then be able to *resolve* that parent locally — the
/// real-S3 benchmark (`bench/remote`, row 1) instead saw `ENOENT` from
/// the very next step, because the holder's refusal reached the caller
/// before the record it was based on reached the caller's replica.
/// `create_dir_all` reproduces it exactly as `os.makedirs` did: on
/// `EEXIST` it asks whether the path is a directory, which is a local
/// lookup.
fn mkdir_p_race(_seed: u64) -> Result<()> {
    let (env, root) = setup("mkdir-p-race")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/mkdirrace-{}", ts());
    let mut clients = Vec::new();
    for i in 0..4 {
        let mut c = Client::new(root.path(), &format!("m{i}"), &env.endpoint, &backend)?
            .with_own_node_key();
        if i == 0 {
            c.fs_create()?;
        }
        c.mount()?;
        clients.push(c);
    }
    eventually("four mounts live", Duration::from_secs(30), || {
        let probe = clients[0].mnt.join(".probe");
        std::fs::create_dir_all(&probe).ok();
        anyhow::ensure!(clients.iter().all(|c| c.mnt.join(".probe").is_dir()));
        Ok(())
    })?;

    let rounds = 8usize;
    let threads = 4usize;
    let mut failures = Vec::new();
    for round in 0..rounds {
        let base = format!("race{round}");
        let errors = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        std::thread::scope(|scope| {
            for c in &clients {
                for t in 0..threads {
                    let dir = c.mnt.join(&base).join(format!("t{t}"));
                    let name = c.name.clone();
                    let errors = errors.clone();
                    scope.spawn(move || {
                        if let Err(e) = std::fs::create_dir_all(&dir) {
                            errors
                                .lock()
                                .unwrap()
                                .push(format!("{name}: create_dir_all({dir:?}): {e}"));
                            return;
                        }
                        let file = dir.join(format!("f-{name}"));
                        if let Err(e) = std::fs::write(&file, b"x") {
                            errors
                                .lock()
                                .unwrap()
                                .push(format!("{name}: write({file:?}): {e}"));
                        }
                    });
                }
            }
        });
        let errors = std::sync::Arc::try_unwrap(errors)
            .unwrap()
            .into_inner()
            .unwrap();
        if !errors.is_empty() {
            failures.push(format!(
                "round {round}: {} error(s): {errors:?}",
                errors.len()
            ));
        }
    }
    anyhow::ensure!(
        failures.is_empty(),
        "concurrent `mkdir -p` of a shared tree must never fail: {failures:?}"
    );

    // Every node must end up with the same tree.
    let expected: std::collections::BTreeSet<String> = (0..rounds)
        .flat_map(|r| {
            (0..threads).flat_map(move |t| (0..4).map(move |n| format!("race{r}/t{t}/f-m{n}")))
        })
        .collect();
    for c in &clients {
        eventually(
            &format!("{} sees every file", c.name),
            Duration::from_secs(60),
            || {
                for rel in &expected {
                    anyhow::ensure!(c.mnt.join(rel).is_file(), "{} missing {rel}", c.name);
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn disjoint_write_4(seed: u64) -> Result<()> {
    use constellation_chaos::cluster::Cluster;
    use constellation_chaos::op::{hash_bytes, Op, Outcome};
    use constellation_chaos::LocalCluster;
    use rand::{rngs::StdRng, RngCore, SeedableRng};
    use std::sync::atomic::{AtomicU64, Ordering};

    let (env, root) = setup("disjoint-write-4")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/disjoint4-{}", ts());
    let rounds: usize = std::env::var("CHAOS_DISJOINT_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let mut clients = Vec::new();
    for i in 0..4 {
        let mut c = Client::new(root.path(), &format!("c{i}"), &env.endpoint, &backend)?
            .with_own_node_key()
            .with_write_mode("back");
        if i == 0 {
            c.fs_create()?;
        }
        // Match the fleet mount flags that produced the failure.
        c.mount_view(None, &["--fsync-mode", "s3"])?;
        clients.push(c);
    }

    eventually("four mounts live", Duration::from_secs(30), || {
        let probe = clients[0].mnt.join(".chaos-probe");
        std::fs::create_dir_all(&probe).ok();
        anyhow::ensure!(clients.iter().any(|c| c.mnt.join(".chaos-probe").is_dir()));
        Ok(())
    })
    .ok();

    let work = "chaos-soak";
    let mounts: Vec<_> = clients.iter().map(|c| c.mnt.clone()).collect();
    let mut cluster = LocalCluster::new(mounts)?;
    cluster.prepare("disjoint-write-4", work)?;

    let mut rng = StdRng::seed_from_u64(seed);
    let op_ids = AtomicU64::new(1);
    let patch_len = 32usize;
    let n = 4usize;
    eprintln!("disjoint-write-4: seed={seed} rounds={rounds} workers={n}");

    for round in 0..rounds {
        let file_id = op_ids.fetch_add(1, Ordering::Relaxed);
        let path = format!("{work}/wd{file_id}");
        let t0 = std::time::Instant::now();

        // Seed a zeroed file (same as chaos prep for write_disjoint).
        let seed_id = op_ids.fetch_add(1, Ordering::Relaxed);
        let seed_op = Op::WriteFull {
            path: path.clone(),
            content: vec![0u8; n * patch_len],
        };
        let seed_c = cluster.invoke(0, seed_id, &seed_op)?;
        anyhow::ensure!(
            seed_c.outcome == Outcome::Ok,
            "round {round}: seed WriteFull failed: {seed_c:?}"
        );

        // Wait until every mount can see the seeded file. Without this,
        // WriteAt's create(true) races create on cold mounts and returns
        // EEXIST — a different failure mode than the fleet's lost patch.
        let visible_deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let mut visible = true;
            for w in 0..n {
                let id = op_ids.fetch_add(1, Ordering::Relaxed);
                let c = cluster.invoke(w, id, &Op::Stat { path: path.clone() })?;
                if c.outcome != Outcome::Ok {
                    visible = false;
                    break;
                }
            }
            if visible {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < visible_deadline,
                "round {round}: seeded file {path} not visible on all mounts within 30s"
            );
            std::thread::sleep(Duration::from_millis(100));
        }

        // Four disjoint patches, one per worker — same shape as duel_write_disjoint.
        let mut patches = Vec::with_capacity(n);
        let mut jobs = Vec::with_capacity(n);
        for i in 0..n {
            let mut patch = format!("wd{i}:{file_id}:").into_bytes();
            while patch.len() < patch_len {
                patch.push((rng.next_u32() & 0xff) as u8);
            }
            patch.truncate(patch_len);
            let expected = hash_bytes(&patch);
            let id = op_ids.fetch_add(1, Ordering::Relaxed);
            jobs.push((
                i,
                id,
                Op::WriteAt {
                    path: path.clone(),
                    offset: (i * patch_len) as u64,
                    patch: patch.clone(),
                },
            ));
            patches.push(expected);
        }
        for result in cluster.invoke_parallel(&jobs) {
            let (w, id, complete) =
                result.with_context(|| format!("round {round}: WriteAt invoke"))?;
            anyhow::ensure!(
                complete.outcome == Outcome::Ok,
                "round {round}: WriteAt worker {w} op {id} failed: {complete:?}"
            );
        }

        // Quiesce: every worker must see every patch (close-to-open).
        let deadline = std::time::Instant::now()
            + Duration::from_secs(
                std::env::var("CHAOS_DISJOINT_CONVERGE_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30),
            );
        let mut last_mismatch = String::new();
        loop {
            let mut ok = true;
            last_mismatch.clear();
            for w in 0..n {
                for (i, expected) in patches.iter().enumerate() {
                    let id = op_ids.fetch_add(1, Ordering::Relaxed);
                    let op = Op::ReadAt {
                        path: path.clone(),
                        offset: (i * patch_len) as u64,
                        len: patch_len as u64,
                    };
                    let c = cluster.invoke(w, id, &op)?;
                    let got = c.value_hash.as_deref().unwrap_or("");
                    if got != expected.as_str() {
                        ok = false;
                        last_mismatch = format!(
                            "worker {w} @{i}: got {got} want {expected} (outcome {:?} errno={:?}/{:?})",
                            c.outcome, c.errno, c.errno_name
                        );
                        break;
                    }
                }
                if !ok {
                    break;
                }
            }
            if ok {
                break;
            }
            if std::time::Instant::now() >= deadline {
                // Dump what each worker sees for the offline diagnosis.
                for w in 0..n {
                    for i in 0..n {
                        let id = op_ids.fetch_add(1, Ordering::Relaxed);
                        let op = Op::ReadAt {
                            path: path.clone(),
                            offset: (i * patch_len) as u64,
                            len: patch_len as u64,
                        };
                        let c = cluster.invoke(w, id, &op)?;
                        eprintln!(
                            "  final w{w}@{i} -> {:?} hash={:?} errno={:?} name={:?}",
                            c.outcome, c.value_hash, c.errno, c.errno_name
                        );
                    }
                }
                for c in &clients {
                    eprintln!(
                        "--- {} mount.log (tail) ---\n{}",
                        c.name,
                        c.tail_log_n(2000)
                    );
                }
                bail!(
                    "round {round}: disjoint WriteAt {path} did not converge within 30s: {last_mismatch}"
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        eprintln!(
            "disjoint-write-4: round {round} ok path={path} in {}ms",
            t0.elapsed().as_millis()
        );
    }

    for c in &mut clients {
        let _ = c.unmount();
    }
    Ok(())
}

/// Plan 21: a named filesystem's views (root + a subtree) share one
/// daemon process and one `node_id` — mounting a second view from a
/// *fresh* CLI invocation must attach to the already-running daemon over
/// its control socket instead of starting a second process. Exercises
/// the registry, name resolution, daemonization (real backgrounding, not
/// `--foreground`), multi-view `view.mount`/`view.unmount`, and the
/// daemon's own clean exit once its last view is detached.
fn named_shared_daemon(_seed: u64) -> Result<()> {
    use std::process::Command;
    use std::time::Instant;

    fn constellation_bin() -> PathBuf {
        std::env::var_os("CONSTELLATION_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let target = std::env::var_os("CARGO_TARGET_DIR")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("target"));
                target.join("release/constellation")
            })
    }

    fn is_mountpoint(p: &std::path::Path) -> bool {
        Command::new("mountpoint")
            .arg("-q")
            .arg(p)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn read_status(state_dir: &std::path::Path) -> Result<serde_json::Value> {
        crate::client::control_call_at(
            state_dir,
            "node.status",
            serde_json::json!({}),
            Duration::from_secs(10),
        )
    }

    fn pid_alive(pid: &str) -> bool {
        pid.trim()
            .parse::<u32>()
            .ok()
            .is_some_and(|pid| std::path::Path::new(&format!("/proc/{pid}")).exists())
    }

    let (env, root) = setup("named-daemon")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/named-{}", ts());

    let registry = root.path().join("registry.toml");
    let data_home = root.path().join("data");
    let mnt1 = root.path().join("mnt1");
    let mnt2 = root.path().join("mnt2");
    std::fs::create_dir_all(&mnt1)?;
    std::fs::create_dir_all(&mnt2)?;

    let cmd = |args: &[&str]| -> Command {
        let mut c = Command::new(constellation_bin());
        c.args(args)
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ENDPOINT", &env.endpoint)
            .env("AWS_ALLOW_HTTP", "true")
            .env("CONSTELLATION_REGISTRY", &registry)
            .env("XDG_DATA_HOME", &data_home)
            .env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
            .env("CONSTELLATION_S3_MAX_RETRIES", "2")
            .env("CONSTELLATION_S3_RETRY_TIMEOUT_MS", "2000");
        c
    };

    let state_dir = data_home.join("constellation").join("myfs");
    let pid_path = state_dir.join("daemon.pid");

    // Runs the scenario body; teardown below always attempts a clean
    // `umount myfs` and, failing that, a direct kill of any leftover
    // daemon.pid — this is the harness scenario's answer to "clients
    // umount in all paths" when there's no `Client`/`Drop` to lean on.
    let body = || -> Result<()> {
        let out = cmd(&[
            "fs",
            "create",
            "myfs",
            "--s3",
            &backend,
            "--chunk-size",
            "1048576",
        ])
        .output()?;
        anyhow::ensure!(
            out.status.success(),
            "fs create failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        // First mount: this invocation forks, becomes the daemon, and
        // the parent (what `.output()` waits on) exits once the child
        // reports "control socket bound and view attached" — real
        // daemonization, not `--foreground`.
        let out = cmd(&["mount", "myfs", mnt1.to_str().unwrap()]).output()?;
        anyhow::ensure!(
            out.status.success(),
            "first mount failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline && !is_mountpoint(&mnt1) {
            std::thread::sleep(Duration::from_millis(100));
        }
        anyhow::ensure!(is_mountpoint(&mnt1), "mnt1 did not come up");

        let pid1 = std::fs::read_to_string(&pid_path)
            .context("reading daemon.pid after the first mount")?
            .trim()
            .to_string();
        anyhow::ensure!(pid_alive(&pid1), "daemon.pid {pid1} is not a live process");

        // A subtree the second view will mount must exist first.
        std::fs::create_dir(mnt1.join("sub")).context("creating /sub before mounting it")?;

        // Second mount, same name, different view, from a *fresh* CLI
        // invocation: must attach to the daemon above, not start a
        // second one.
        let out = cmd(&["mount", "myfs:/sub", mnt2.to_str().unwrap()]).output()?;
        anyhow::ensure!(
            out.status.success(),
            "second mount failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline && !is_mountpoint(&mnt2) {
            std::thread::sleep(Duration::from_millis(100));
        }
        anyhow::ensure!(is_mountpoint(&mnt2), "mnt2 did not come up");

        let pid2 = std::fs::read_to_string(&pid_path)
            .context("reading daemon.pid after the second mount")?
            .trim()
            .to_string();
        anyhow::ensure!(
            pid1 == pid2,
            "a second daemon.pid appeared ({pid1} vs {pid2}); views did not share one process"
        );

        let status = read_status(&state_dir)?;
        anyhow::ensure!(
            status["mounts"].as_array().map(|m| m.len()) == Some(2),
            "daemon should report exactly 2 mounted views: {status}"
        );

        // Both mountpoints usable, through the one shared replica.
        std::fs::write(mnt1.join("a.txt"), b"root-view")?;
        std::fs::write(mnt2.join("b.txt"), b"sub-view")?;
        anyhow::ensure!(
            std::fs::read(mnt1.join("sub/b.txt"))? == b"sub-view",
            "subtree view content not visible through the root view"
        );

        // Detach the subtree view: root keeps working, daemon stays up.
        let out = cmd(&["umount", "myfs:/sub"]).output()?;
        anyhow::ensure!(
            out.status.success(),
            "umount myfs:/sub failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline && is_mountpoint(&mnt2) {
            std::thread::sleep(Duration::from_millis(100));
        }
        anyhow::ensure!(!is_mountpoint(&mnt2), "mnt2 still mounted after umount");
        anyhow::ensure!(
            is_mountpoint(&mnt1) && std::fs::read(mnt1.join("a.txt"))? == b"root-view",
            "root view stopped working after detaching the subtree view"
        );
        anyhow::ensure!(
            pid_alive(&pid1),
            "daemon exited after detaching a non-last view"
        );

        // Detach the last view: the daemon must run its clean-shutdown
        // sequence and exit, removing its own PID file.
        let out = cmd(&["umount", "myfs"]).output()?;
        anyhow::ensure!(
            out.status.success(),
            "umount myfs failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline && (is_mountpoint(&mnt1) || pid_alive(&pid1)) {
            std::thread::sleep(Duration::from_millis(100));
        }
        anyhow::ensure!(
            !is_mountpoint(&mnt1),
            "mnt1 still mounted after umount myfs"
        );
        anyhow::ensure!(
            !pid_alive(&pid1),
            "daemon {pid1} still alive after its last view was detached"
        );
        anyhow::ensure!(!pid_path.exists(), "daemon.pid not removed on clean exit");
        Ok(())
    };

    let result = body();

    // Best-effort teardown regardless of where `body` failed.
    let _ = cmd(&["umount", "myfs"]).output();
    if let Ok(pid) = std::fs::read_to_string(&pid_path) {
        if let Ok(pid) = pid.trim().parse::<i32>() {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }
    let _ = Command::new("fusermount3")
        .args(["-u", "-z"])
        .arg(&mnt1)
        .status();
    let _ = Command::new("fusermount3")
        .args(["-u", "-z"])
        .arg(&mnt2)
        .status();

    result
}

// --- plan 26: metadata-plane S3 efficiency ---
//
// These five assert on *S3 request classes and counts*, which no other
// lane can see: toxiproxy is a TCP fault injector with no notion of HTTP
// and floci logs bucket lifecycle only. `reqlog::CountingProxy` is
// chained in front of toxiproxy (client -> counter -> toxiproxy ->
// floci) so a scenario can say "zero LISTs of `log/p0` during the burst"
// about the actual wire, with every toxic still applied.

/// Every object under `prefix` with its size, via a direct (unproxied)
/// ListObjectsV2. One page (1000 keys); every caller here stays well
/// under that.
fn raw_objects(endpoint: &str, prefix: &str) -> Result<Vec<(String, u64)>> {
    // Every page: a listing is capped at 1000 keys per response, and a
    // busy writer's log passes that within a minute (plan 30 M5).
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}");
        if let Some(t) = &token {
            url.push_str("&continuation-token=");
            url.push_str(&url_encode(t));
        }
        let mut body = String::new();
        crate::s3auth::get(&url)
            .call()
            .with_context(|| format!("listing {prefix}"))?
            .into_reader()
            .read_to_string(&mut body)?;
        let mut rest = body.as_str();
        while let Some(start) = rest.find("<Contents>") {
            rest = &rest[start..];
            let Some(end) = rest.find("</Contents>") else {
                break;
            };
            let entry = &rest[..end];
            if let (Some(key), Some(size)) = (
                xml_field(entry, "Key"),
                xml_field(entry, "Size").and_then(|s| s.parse().ok()),
            ) {
                out.push((key.to_string(), size));
            }
            rest = &rest[end..];
        }
        let truncated = xml_field(&body, "IsTruncated") == Some("true");
        token = xml_field(&body, "NextContinuationToken").map(|t| t.to_string());
        if !truncated || token.is_none() {
            break;
        }
    }
    Ok(out)
}

/// Percent-encode a continuation token for a query string.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn xml_field<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let start = xml.find(&format!("<{name}>"))? + name.len() + 2;
    let end = xml[start..].find(&format!("</{name}>"))? + start;
    Some(&xml[start..end])
}

/// Segment sequence numbers present for one partition.
fn log_segment_seqs(endpoint: &str, prefix: &str, part: &str) -> Result<Vec<u64>> {
    let mut seqs: Vec<u64> = raw_objects(endpoint, &format!("{prefix}/log/{part}/"))?
        .into_iter()
        .filter_map(|(key, _)| {
            let name = key.rsplit('/').next()?.strip_suffix(".zst")?;
            u64::from_str_radix(name, 16).ok()
        })
        .collect();
    seqs.sort_unstable();
    Ok(seqs)
}

fn journal_drained(c: &Client) -> Result<()> {
    let status = c.control_status()?;
    let backlog = status["spool"]["journal_backlog"].as_u64().unwrap_or(1);
    let pending = status["writeback"]["pending_uploads"].as_u64().unwrap_or(1);
    anyhow::ensure!(
        backlog == 0 && pending == 0,
        "journal_backlog={backlog} pending_uploads={pending}"
    );
    Ok(())
}

/// Plan 28 S7b's gate: the steady-state metadata footprint plateaus.
///
/// Every round rewrites a third of a fixed file set — the shape that
/// supersedes leaves without growing the namespace — forces a metadata
/// commit (a snapshot is a retained root, so taking one publishes; it is
/// deleted again straight away so it roots nothing), and runs a GC round
/// with a two-commit retention window. Without GC the `packs/` footprint
/// grows every round; with it, what the retired commits alone kept alive
/// is deleted or compacted away, so the late rounds must sit near the
/// early ones rather than keep climbing. A fresh node then bootstraps
/// from the surviving commit and must match the oracle, which is the
/// part that proves GC removed only garbage.
fn mtree_gc_plateau(seed: u64) -> Result<()> {
    const FILES: usize = 300;
    const ROUNDS: usize = 12;
    let (env, root) = setup("mtree-gc-plateau")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("mtree-gc-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let gc_env = [
        ("CONSTELLATION_LEASE_TTL_MS", "200"),
        ("CONSTELLATION_GC_HORIZON_S", "0"),
        ("CONSTELLATION_COMMIT_RETENTION", "2"),
        ("CONSTELLATION_COMMIT_RETENTION_S", "0"),
        ("CONSTELLATION_COMPACT_BYTES_PER_S", "0"),
    ];
    let mut a = Client::new(root.path(), "gc-a", &env.endpoint, &backend)?;
    for (key, value) in gc_env {
        a = a.with_env(key, value);
    }
    a.fs_create()?;
    a.mount()?;

    let mut model = Model::default();
    std::fs::create_dir(a.mnt.join("set"))?;
    model.mkdir(std::path::Path::new("set"));
    let write = |a: &Client, model: &mut Model, i: usize, salt: u64| -> Result<()> {
        let rel = format!("set/f{i:04}");
        let data = pattern(seed.wrapping_add(salt), 256 + (i % 5) * 32);
        std::fs::write(a.mnt.join(&rel), &data)?;
        model.write_file(std::path::Path::new(&rel), data);
        Ok(())
    };
    for i in 0..FILES {
        write(&a, &mut model, i, i as u64)?;
    }

    let packs_bytes = || -> Result<u64> {
        Ok(
            raw_objects(&env.direct_endpoint, &format!("{prefix}/packs/"))?
                .iter()
                .map(|(_, size)| *size)
                .sum(),
        )
    };
    let mut series = Vec::new();
    for round in 0..ROUNDS {
        for i in (round % 3..FILES).step_by(3) {
            write(&a, &mut model, i, (round * FILES + i) as u64 + 1_000_000)?;
        }
        eventually("round ships", Duration::from_secs(60), || {
            journal_drained(&a)
        })?;
        let name = format!("/set@round{round}");
        a.snapshot_create(&name)?;
        a.snapshot_delete(&name)?;
        let output = a.gc_run()?;
        anyhow::ensure!(
            output.status.success(),
            "gc round {round} failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        series.push(packs_bytes()?);
    }
    eprintln!("    mtree-gc-plateau: packs/ bytes per round {series:?}");
    let early = *series[2..6].iter().max().unwrap_or(&0);
    let late = *series[ROUNDS - 4..].iter().max().unwrap_or(&0);
    anyhow::ensure!(early > 0, "no metadata packs were written: {series:?}");
    anyhow::ensure!(
        late <= early + early / 2,
        "the metadata footprint kept growing across GC rounds: {series:?}"
    );
    model.verify(&a.mnt)?;
    a.unmount()?;

    let mut b = Client::new(root.path(), "gc-b", &env.endpoint, &backend)?;
    b.mount().context("fresh bootstrap after metadata GC")?;
    eventually(
        "fresh node matches the oracle",
        Duration::from_secs(60),
        || model.verify(&b.mnt),
    )?;
    b.unmount()?;
    Ok(())
}

/// How many poll deadlines elapse in `window_ms` when every round is
/// idle: the interval doubles per idle round, clamped to the ceiling
/// (mirrors `cli::node_runtime::next_poll_ms`).
fn idle_poll_rounds(interval_ms: u64, idle_max_ms: u64, window_ms: u64) -> u64 {
    let (mut elapsed, mut rounds, mut idle) = (0u64, 0u64, 0u32);
    loop {
        let next = interval_ms
            .saturating_mul(1u64 << idle.min(32))
            .clamp(interval_ms, idle_max_ms);
        elapsed += next;
        if elapsed > window_ms {
            return rounds;
        }
        rounds += 1;
        idle = idle.saturating_add(1);
    }
}

/// In-flight probe GETs per idle poll round, per partition
/// (`cli::shipper::TAIL_GET_CONCURRENCY`).
const TAIL_GET_CONCURRENCY: u64 = 16;

/// Steady-state cost of sitting still (plan 26 steps 3, 4b and 4c).
///
/// Before this plan an idle node polled every 500 ms and every round was
/// one `LIST` per partition: ten idle nodes were ~1.7M LISTs a day for
/// nothing, in the most expensive request class there is (12.5x a GET on
/// AWS). Three nodes converge, go quiet for a minute, and every request
/// each one makes is counted by class on its own relay.
///
/// The structural assertion is that **nothing lists `log/` at all** — the
/// holder does not list a stream it is the only legal appender of (step
/// 3), and a follower probes with speculative GETs instead (step 4b). The
/// budget then bounds the total against what the remaining periodic work
/// can explain: lease renewal, the 5 s registry/membership poll, the 10 s
/// designation poll, and the backed-off metadata probe itself.
fn idle_cluster_is_quiet(_seed: u64) -> Result<()> {
    const IDLE_S: u64 = 60;
    const INTERVAL_MS: u64 = 500;
    const IDLE_MAX_MS: u64 = 30_000;
    const TTL_MS: u64 = 60_000;
    const NODES: u64 = 3;

    let (env, root) = setup("idle-cluster-quiet")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/idle-quiet-{}", ts());
    let counters = [
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    ];
    let mk = |name: &str, endpoint: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, endpoint, &backend)?
            // Distinct P2P identities: three "hosts" on one machine.
            .with_own_node_key()
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", &INTERVAL_MS.to_string())
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", &IDLE_MAX_MS.to_string())
            .with_env("CONSTELLATION_LEASE_TTL_MS", &TTL_MS.to_string()))
    };
    let mut a = mk("quiet-a", &counters[0].endpoint())?;
    let mut b = mk("quiet-b", &counters[1].endpoint())?;
    let mut c = mk("quiet-c", &counters[2].endpoint())?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;

    // Converge first: a node that has never seen the tree is catching up,
    // not idling, and catch-up legitimately lists.
    std::fs::write(a.mnt.join("marker"), b"quiet")?;
    for follower in [&b, &c] {
        eventually(
            &format!("marker reaches {}", follower.name),
            Duration::from_secs(60),
            || {
                anyhow::ensure!(
                    std::fs::read(follower.mnt.join("marker"))? == b"quiet",
                    "marker not visible yet"
                );
                Ok(())
            },
        )?;
    }
    eventually("writer's journal drains", Duration::from_secs(60), || {
        journal_drained(&a)
    })?;
    // Let the backoff start from a clean slate, then measure.
    for counter in &counters {
        counter.reset();
    }
    std::thread::sleep(Duration::from_secs(IDLE_S));

    // Budget, per node, from the periods that are actually configured.
    // This is the plan's `lease renewals + heartbeats + 3` with
    // "heartbeats" expanded into what a heartbeat costs in requests: the
    // 5 s registry poll reads the roster and the peer directory (a LIST
    // plus a GET per registered node each) and re-reads this node's own
    // record, and the 10 s designation poll lists its prefix.
    let renewals = (IDLE_S / (TTL_MS / 2_000) + 1) * 2;
    let heartbeats = (IDLE_S / 5) * (2 + 2 * NODES + 1) + (IDLE_S / 10) * 2;
    let probes = idle_poll_rounds(INTERVAL_MS, IDLE_MAX_MS, IDLE_S * 1_000) * TAIL_GET_CONCURRENCY;
    let per_node = renewals + heartbeats + probes + 3;
    let budget = NODES * per_node;

    let mut total = crate::reqlog::Tally::default();
    for (counter, client) in counters.iter().zip([&a, &b, &c]) {
        counter.ensure_sane()?;
        let requests = counter.requests();
        let t = crate::reqlog::tally(&requests);
        eprintln!(
            "    idle-cluster-is-quiet: {:>7} {t}\n        {:>7} by area: {}",
            client.name,
            client.name,
            crate::reqlog::breakdown(&requests)
        );
        let log_lists: Vec<&str> = requests
            .iter()
            .filter(|r| r.lists_prefix("/log/") || r.lists_prefix("log/"))
            .map(|r| r.target.as_str())
            .collect();
        anyhow::ensure!(
            log_lists.is_empty(),
            "{} listed log/ while idle ({} times): {log_lists:?}",
            client.name,
            log_lists.len()
        );
        // Nothing but membership and designations may list at all.
        let stray: Vec<&str> = requests
            .iter()
            .filter(|r| r.is_list() && !r.lists_prefix("nodes") && !r.lists_prefix("designations"))
            .map(|r| r.target.as_str())
            .collect();
        anyhow::ensure!(
            stray.is_empty(),
            "{} issued {} LIST(s) outside the membership/designation polls: {stray:?}",
            client.name,
            stray.len()
        );
        total.list += t.list;
        total.get += t.get;
        total.head += t.head;
        total.put += t.put;
        total.post += t.post;
        total.delete += t.delete;
        total.other += t.other;
    }
    // What the pre-plan fixed 500 ms LIST poll alone would have cost the
    // same three nodes over the same minute, priced in GET-equivalents
    // (AWS: a LIST is 12.5x a GET).
    let before = NODES * (IDLE_S * 1_000 / INTERVAL_MS);
    eprintln!(
        "    idle-cluster-is-quiet: {IDLE_S}s idle, {NODES} nodes: {total} (budget {budget}); \
         priced in GET-equivalents {} vs {} for the pre-plan fixed-interval LIST poll alone",
        total.total() - total.list + total.list * 12,
        before * 12
    );
    anyhow::ensure!(
        total.total() <= budget,
        "an idle cluster issued {} requests over {IDLE_S}s, budget {budget}: {total}",
        total.total()
    );
    a.unmount()?;
    b.unmount()?;
    c.unmount()?;
    Ok(())
}

/// A far writer's ship loop is PUT-only (plan 26 step 3), and a follower
/// with no fast path is bounded by the idle poll ceiling (step 4c).
///
/// The holder used to run one `LIST` of its own partition before every
/// `ship_all`, asking a stream it is the sole legal appender of whether
/// anyone else had appended. Measured HU->AWS, dropping it takes a writer
/// from 2.6 to 5.1 shipped segments/s. With 200 ms injected on the S3
/// path, count every request the writer makes during a 500-file burst and
/// require that none of them lists `log/p0`.
///
/// The follower runs with `CONSTELLATION_P2P=off`, so no gossip `Nudge`
/// can reset its backoff: convergence has to come from the poll alone,
/// within the documented `idle_max_ms` bound.
fn wan_writer_ships_put_only(seed: u64) -> Result<()> {
    const FILES: usize = 500;
    const IDLE_MAX_MS: u64 = 10_000;
    let (env, root) = setup("wan-writer-put-only")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("wan-put-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let writer_counter = env.counting_proxy()?;
    let mk = |name: &str, endpoint: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, endpoint, &backend)?
            .with_env("CONSTELLATION_P2P", "off")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "500")
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", &IDLE_MAX_MS.to_string())
            // The holder must keep the lease across the whole burst.
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000"))
    };
    let mut a = mk("wan-a", &writer_counter.endpoint())?.with_write_mode("back");
    let mut b = mk("wan-b", &env.endpoint)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;

    let mut model = Model::default();
    // Take the lease before the measurement starts: acquisition tails to
    // head, and that tail legitimately lists.
    std::fs::write(a.mnt.join("warm"), b"warm")?;
    model.write_file(std::path::Path::new("warm"), b"warm".to_vec());
    eventually("warm-up ships", Duration::from_secs(60), || {
        journal_drained(&a)
    })?;
    eventually(
        "warm-up reaches the follower",
        Duration::from_secs(60),
        || {
            anyhow::ensure!(b.mnt.join("warm").is_file(), "warm not on the follower");
            Ok(())
        },
    )?;
    anyhow::ensure!(
        lease_of(&a)?["held"] == true,
        "the writer must hold p0 before the burst: {}",
        lease_of(&a)?
    );

    proxy.latency(200, 0)?;
    writer_counter.reset();
    let burst_start = std::time::Instant::now();
    std::fs::create_dir(a.mnt.join("burst"))?;
    model.mkdir(std::path::Path::new("burst"));
    for i in 0..FILES {
        let data = pattern(seed.wrapping_add(i as u64), 256);
        let rel = format!("burst/f{i:04}");
        std::fs::write(a.mnt.join(&rel), &data)?;
        model.write_file(std::path::Path::new(&rel), data);
    }
    eventually("the burst ships", Duration::from_secs(300), || {
        journal_drained(&a)
    })?;
    let burst = burst_start.elapsed();

    writer_counter.ensure_sane()?;
    let requests = writer_counter.requests();
    let t = crate::reqlog::tally(&requests);
    let self_lists: Vec<&str> = requests
        .iter()
        .filter(|r| r.lists_prefix("/log/p0") || r.lists_prefix("log/p0"))
        .map(|r| r.target.as_str())
        .collect();
    let segments = log_segment_seqs(&env.direct_endpoint, &prefix, "p0")?.len();
    let existence = a.control_status()?["writeback"].clone();
    eprintln!(
        "    wan-writer-ships-put-only: {FILES} files in {burst:?} over a 200ms path, \
         {segments} segments on p0; writer {t}\n        by area: {}\n        \
         existence hints: chunk_ref={} bloom={} misses={}",
        crate::reqlog::breakdown(&requests),
        existence["existence_chunk_ref_hits"],
        existence["existence_bloom_hits"],
        existence["existence_misses"]
    );
    anyhow::ensure!(
        self_lists.is_empty(),
        "the holder listed its own stream {} time(s) during the burst: {self_lists:?}",
        self_lists.len()
    );

    // With no fast path the follower's freshness bound is the poll
    // ceiling and nothing else.
    let converge_deadline = Duration::from_millis(IDLE_MAX_MS) + Duration::from_secs(5);
    let converge_start = std::time::Instant::now();
    eventually(
        "follower converges over S3 alone",
        converge_deadline,
        || model.verify(&b.mnt),
    )?;
    eprintln!(
        "    wan-writer-ships-put-only: follower converged {:?} after the writer drained \
         (bound {converge_deadline:?}, P2P off)",
        converge_start.elapsed()
    );
    proxy.heal()?;
    ensure_no_conflicts(&[&a, &b])?;
    a.unmount()?;
    b.unmount()?;
    Ok(())
}

/// Sticky leases hand over through S3 alone (plan 26 step 7).
///
/// An idle holder no longer gives a lease back to nobody: it releases
/// only once a requester has recorded itself in the lease object's
/// `wanted_by` list, and never inside `LEASE_MIN_DWELL_MS` of taking it.
/// With `CONSTELLATION_P2P=off` there is no `HandOff` message, so the
/// whole negotiation has to happen over conditional writes: B registers
/// on its first blocked `Acquire`, A picks that up at its next renewal
/// (at most TTL/2 away), finishes its batch and releases, and B's write
/// completes rather than returning EIO.
///
/// A must not be *deposed* along the way — that would be the old TTL
/// expiry path rather than the cooperative one — so the scenario checks
/// that A never reports `lost`, that the epoch strictly advanced (A's
/// fencing token is stale, so anything it shipped late would be
/// rejected), and that both nodes converge on the union.
fn sticky_lease_handoff_over_s3(_seed: u64) -> Result<()> {
    const TTL_MS: u64 = 10_000;
    const DWELL_MS: u64 = 5_000; // cli::lease::LEASE_MIN_DWELL_MS
    let (env, root) = setup("sticky-lease-handoff")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/sticky-{}", ts());
    let mk = |name: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, &env.endpoint, &backend)?
            .with_env("CONSTELLATION_P2P", "off")
            // Plan 30 M13's inbox answers a lone write through the holder
            // without any handoff; this scenario is about the S3-only
            // lease handoff itself, so it runs with the inbox off.
            .with_env("CONSTELLATION_INBOX", "off")
            .with_env("CONSTELLATION_LEASE_TTL_MS", &TTL_MS.to_string())
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "1000"))
    };
    let mut a = mk("sticky-a")?;
    let mut b = mk("sticky-b")?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    anyhow::ensure!(
        p2p_of(&a)?["enabled"] == false && p2p_of(&b)?["enabled"] == false,
        "this scenario must exercise the S3-only handoff"
    );

    let mut model = Model::default();
    std::fs::create_dir(a.mnt.join("shared"))?;
    model.mkdir(std::path::Path::new("shared"));
    for i in 0..10 {
        let rel = format!("shared/from-a-{i}");
        let data = format!("a{i}").into_bytes();
        std::fs::write(a.mnt.join(&rel), &data)?;
        model.write_file(std::path::Path::new(&rel), data);
    }
    eventually("A's writes ship", Duration::from_secs(60), || {
        journal_drained(&a)
    })?;
    let a_lease = lease_of(&a)?;
    anyhow::ensure!(
        a_lease["held"] == true && a_lease["lost"] == false,
        "A must hold p0 after writing: {a_lease}"
    );
    let a_epoch = a_lease["epoch"].as_u64().unwrap_or(0);
    // Stickiness: with nobody asking, an idle holder keeps the lease well
    // past `CONSTELLATION_LEASE_IDLE_RELEASE_MS` (1 s here).
    std::thread::sleep(Duration::from_secs(6));
    let idle_lease = lease_of(&a)?;
    anyhow::ensure!(
        idle_lease["held"] == true && idle_lease["epoch"].as_u64() == Some(a_epoch),
        "an idle holder with no requester released the lease anyway: {idle_lease}"
    );

    // B's first write registers `wanted_by` and then has to wait for A to
    // notice at its next renewal.
    let bound = Duration::from_millis(TTL_MS / 2 + DWELL_MS) + Duration::from_secs(10);
    let handoff_start = std::time::Instant::now();
    let rel = "shared/from-b";
    std::fs::write(b.mnt.join(rel), b"b0").with_context(|| {
        format!(
            "B's write failed instead of waiting for the handoff; A log:\n{}\nB log:\n{}",
            a.tail_log_n(40),
            b.tail_log_n(40)
        )
    })?;
    let handoff = handoff_start.elapsed();
    model.write_file(std::path::Path::new(rel), b"b0".to_vec());
    eprintln!(
        "    sticky-lease-handoff-over-s3: B's first write completed in {handoff:?} \
         (bound {bound:?} = TTL/2 + dwell + 10s, P2P off)"
    );
    anyhow::ensure!(
        handoff <= bound,
        "S3-only handoff took {handoff:?}, over the {bound:?} bound"
    );

    let b_lease = lease_of(&b)?;
    anyhow::ensure!(
        b_lease["held"] == true,
        "B wrote without holding p0: {b_lease}"
    );
    let b_epoch = b_lease["epoch"].as_u64().unwrap_or(0);
    anyhow::ensure!(
        b_epoch > a_epoch,
        "the fencing token must advance across a handoff: A {a_epoch} -> B {b_epoch}"
    );
    let a_after = lease_of(&a)?;
    anyhow::ensure!(
        a_after["lost"] == false,
        "A was deposed instead of releasing cooperatively: {a_after}; log:\n{}",
        a.tail_log_n(40)
    );

    eventually("both nodes converge", Duration::from_secs(60), || {
        model.verify(&a.mnt).context("via A")?;
        model.verify(&b.mnt).context("via B")?;
        Ok(())
    })?;
    ensure_no_conflicts(&[&a, &b])?;
    a.unmount()?;
    b.unmount()?;
    Ok(())
}

// --- Plan 30 M0: known-bug reproductions ---

/// `control_status()["forwarded_err"]`: how many of this node's forwarded
/// mutations came back Busy/NotHolder/timed-out/undecodable. The plan 30
/// M0 repros use its *rise* across one op as proof the fault they
/// configured actually engaged, rather than trusting that a slow CI host
/// reproduces the exact race by accident.
fn forwarded_err(c: &Client) -> Result<u64> {
    Ok(c.control_status()?["forwarded_err"].as_u64().unwrap_or(0))
}

/// Plan 30 §M2 non-vacuity: the sum of every way an in-doubt retry can
/// resolve *without* re-executing the op — the holder answering a
/// retried rid from `recent`/`completed` (`forward_dedup_hits`) or the
/// requester's own lease-path finding the rid already in `completed`
/// after a takeover (`forward_indoubt_resolved`). Summed across *both*
/// nodes: either one could be the node that actually resolves a given
/// round's retry, depending on which path the fault race takes.
fn dedup_evidence(clients: &[&Client]) -> Result<u64> {
    let mut total = 0u64;
    for c in clients {
        let status = c.control_status()?;
        total += status["forward_dedup_hits"].as_u64().unwrap_or(0);
        total += status["forward_indoubt_resolved"].as_u64().unwrap_or(0);
    }
    Ok(total)
}

/// Plan 30 M2b's own motivating measurement, as a regression scenario.
///
/// Before this milestone, `node_runtime`'s sync task dropped its
/// in-flight `run_managed_sync_round` for *every* `SyncRequest` other
/// than `Nudge` — including the holder's own `SyncRequest::Mutate` for
/// each forwarded mutation. Under a sustained forwarding burst (a
/// request every ~0.7ms, a round taking ~2ms — one S3 PUT) the holder
/// almost never finished a round: the coordinator's own instrumented run
/// entered ~1,200 rounds and only 5 ran `sync_all` to completion, with
/// the journal backlog sitting at 1,000-1,600 records throughout.
///
/// This reproduces that load shape directly against the real sync task
/// — no fault injection needed, since the starvation is structural
/// (every forward cancels the round), not S3-latency-dependent — and
/// checks the fix's two externally observable effects:
/// - the holder's own `journal_backlog` stays small *throughout* the
///   burst (bounded batching/shipping, not unbounded accumulation from
///   starved rounds) — sampled continuously, not just checked at the
///   end, since a round that starves for the whole burst and then
///   catches up right at the end would otherwise pass a single
///   end-of-burst check while still exhibiting exactly the bug;
/// - the non-holders converge on every created file within 2s of the
///   burst ending (bounded end-to-end latency from a holder that is
///   actually shipping, not just "eventually" via the idle-poll
///   fallback).
///
/// The backlog bound (`MAX_BACKLOG_DURING_BURST`) is chosen from measured
/// pre-fix vs. post-fix behavior on this exact load shape, on this host
/// (see the M2b PROGRESS.md entry): the pre-M2b binary (post-M2) reaches
/// 12,799 — the same "thousands" shape the coordinator's own
/// instrumentation found — while the post-M2b binary stays at 186-226
/// across repeated runs. 500 sits comfortably above that measured
/// post-fix range (room for host jitter) while remaining more than 25x
/// tighter than the pre-fix failure mode, so a regression back to
/// round-cancelling starvation still fails this scenario immediately
/// rather than needing to reach four digits first.
fn holder_ships_under_forward_load(_seed: u64) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    const MAX_BACKLOG_DURING_BURST: u64 = 500;
    const CREATES_PER_THREAD: u64 = 800;
    const THREADS_PER_NODE: usize = 4;

    let (env, root) = setup("holder-ships-under-forward-load")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/holder-ships-{}", ts());
    let mut holder =
        Client::new(root.path(), "holder", &env.endpoint, &backend)?.with_own_node_key();
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?.with_own_node_key();
    let mut c2 = Client::new(root.path(), "c2", &env.endpoint, &backend)?.with_own_node_key();
    holder.fs_create()?;
    holder.mount()?;
    c1.mount()?;
    c2.mount()?;
    wait_for_p2p(&[&holder, &c1, &c2])?;

    let dir = "burst";
    std::fs::create_dir_all(holder.mnt.join(dir))?;
    eventually(
        "shared burst dir visible on all mounts",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(c1.mnt.join(dir).is_dir());
            anyhow::ensure!(c2.mnt.join(dir).is_dir());
            Ok(())
        },
    )?;

    // Establish `holder` as the lease holder *before* the burst starts,
    // so every forwarded create below has a fixed target — the property
    // under test is what a forwarding-heavy holder does, not who ends up
    // holding the lease.
    std::fs::write(holder.mnt.join(dir).join(".establish-holder"), b"x")
        .context("establishing the holder")?;
    eventually("holder holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&holder)?;
        anyhow::ensure!(lease["held"] == true, "holder not holding: {lease}");
        Ok(())
    })?;

    // Sample the holder's own `journal_backlog` throughout the burst on a
    // dedicated thread — tight enough (5ms) to catch a starved holder's
    // backlog sitting in the thousands for the burst's whole duration,
    // the shape plan 30 M2's own measurement found.
    let sampling = std::sync::atomic::AtomicBool::new(true);
    let max_backlog = AtomicU64::new(0);
    let last_sample_error: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

    let own = holder.mnt.join(dir).join(".establish-holder");
    let reading = std::sync::atomic::AtomicBool::new(true);
    let holder_reads = AtomicU64::new(0);

    std::thread::scope(|scope| -> Result<()> {
        let sampler = scope.spawn(|| {
            while sampling.load(Ordering::Relaxed) {
                match holder.control_status() {
                    Ok(status) => {
                        let backlog = status["spool"]["journal_backlog"].as_u64().unwrap_or(0);
                        max_backlog.fetch_max(backlog, Ordering::Relaxed);
                    }
                    Err(e) => {
                        *last_sample_error.lock().unwrap() = Some(e.to_string());
                    }
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        // Plan 30 §M9 per-row durability, measured: throughout the burst
        // the holder rewrites one file of its own and reads it back
        // (open + read, both through the session check). Its row is
        // durable before the close returns, but the journal rarely ships
        // out completely under this load, so the file stays in the
        // unshipped key set while other clients' rows are in flight to
        // the backup — the coarse rule held such a read for them, the
        // per-row rule does not.
        let session_before = holder.control_status()?["session"].clone();
        let reader = scope.spawn(|| {
            let mut i = 0u64;
            while reading.load(Ordering::Relaxed) {
                i += 1;
                if std::fs::write(&own, i.to_le_bytes()).is_ok()
                    && std::fs::read(&own).is_ok_and(|b| b == i.to_le_bytes())
                {
                    holder_reads.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        });

        // The sustained forwarded-create burst: several threads on each
        // of the two non-holder nodes, all creating into the holder's
        // shared directory — every single create is a forward (plan 29
        // ADR-14), which is exactly the load shape that used to cancel
        // the holder's round on every op.
        let t0 = Instant::now();
        let mut handles = Vec::new();
        for (node_idx, client) in [&c1, &c2].into_iter().enumerate() {
            for tid in 0..THREADS_PER_NODE {
                let mnt = client.mnt.clone();
                let dir = dir.to_string();
                handles.push(scope.spawn(move || -> Result<u64> {
                    for n in 0..CREATES_PER_THREAD {
                        let path = mnt.join(&dir).join(format!("n{node_idx}-t{tid}-{n}"));
                        std::fs::File::create(&path)
                            .with_context(|| format!("creating {path:?}"))?;
                    }
                    Ok(CREATES_PER_THREAD)
                }));
            }
        }
        let mut created = 0u64;
        for h in handles {
            created += h
                .join()
                .map_err(|_| anyhow::anyhow!("burst worker thread panicked"))??;
        }
        let burst_elapsed = t0.elapsed();
        reading.store(false, Ordering::Relaxed);
        reader
            .join()
            .map_err(|_| anyhow::anyhow!("holder reader thread panicked"))?;
        if let Ok(status) = holder.control_status() {
            let (a, b) = (&session_before, &status["session"]);
            let d = |k: &str| b[k].as_u64().unwrap_or(0) - a[k].as_u64().unwrap_or(0);
            eprintln!(
                "holder-ships-under-forward-load: holder rewrote and read back its own file {} \
                 times during the burst: session reads {} waited {} timeouts {} wait-ms {}; \
                 reads durability-blocked {}",
                holder_reads.load(Ordering::Relaxed),
                d("reads"),
                d("waited"),
                d("timeouts"),
                d("wait_ms_total"),
                status["ack"]["reads_durability_blocked"]
                    .as_u64()
                    .unwrap_or(0)
            );
        }

        sampling.store(false, Ordering::Relaxed);
        sampler
            .join()
            .map_err(|_| anyhow::anyhow!("sampler thread panicked"))?;

        eprintln!(
            "holder-ships-under-forward-load: {created} forwarded creates in {burst_elapsed:?}, \
             max journal_backlog observed = {}",
            max_backlog.load(Ordering::Relaxed)
        );
        // Plan 30 M2b measurement: completed vs. cancelled sync rounds on
        // the holder for this exact run (`shipper::SpoolInfo`'s doc has
        // the full explanation) — printed so a coordinator comparing
        // before/after binaries can read it straight from the scenario's
        // own output rather than a separate ad hoc script.
        if let Ok(status) = holder.control_status() {
            eprintln!(
                "holder-ships-under-forward-load: holder ship_rounds_completed={} \
                 ship_rounds_cancelled={}",
                status["spool"]["ship_rounds_completed"]
                    .as_u64()
                    .unwrap_or(0),
                status["spool"]["ship_rounds_cancelled"]
                    .as_u64()
                    .unwrap_or(0),
            );
        }
        if let Some(e) = last_sample_error.lock().unwrap().take() {
            eprintln!("holder-ships-under-forward-load: a status sample failed: {e}");
        }
        anyhow::ensure!(
            created == THREADS_PER_NODE as u64 * CREATES_PER_THREAD * 2,
            "not every create completed: {created}"
        );
        anyhow::ensure!(
            max_backlog.load(Ordering::Relaxed) <= MAX_BACKLOG_DURING_BURST,
            "holder's journal_backlog reached {} during the burst (bound {}): the round-\
             cancelling starvation plan 30 M2b fixes appears to be back",
            max_backlog.load(Ordering::Relaxed),
            MAX_BACKLOG_DURING_BURST
        );
        Ok(())
    })?;

    // Convergence within 2s of the burst ending: every created file must
    // be visible (and readable) from *both* non-holder mounts — the
    // requesters of the very forwards the burst above generated.
    let expect_names: Vec<String> = (0..THREADS_PER_NODE)
        .flat_map(|tid| (0..CREATES_PER_THREAD).map(move |n| (tid, n)))
        .flat_map(|(tid, n)| [format!("n0-t{tid}-{n}"), format!("n1-t{tid}-{n}")])
        .collect();
    eventually(
        "followers see every forwarded create within 2s of the burst ending",
        Duration::from_secs(2),
        || {
            for mnt in [&holder.mnt, &c1.mnt, &c2.mnt] {
                for name in &expect_names {
                    anyhow::ensure!(
                        mnt.join(dir).join(name).exists(),
                        "{}: {name} not visible yet",
                        mnt.display()
                    );
                }
            }
            Ok(())
        },
    )?;
    ensure_no_conflicts(&[&holder, &c1, &c2])?;

    holder.unmount()?;
    c1.unmount()?;
    c2.unmount()?;
    Ok(())
}

/// Bug A (`docs/plans/v1/done/30-write-path-resilience-and-scale-out.md`
/// §1.1), fixed by plan 30 M2's exactly-once forwarding: without a rid,
/// `request_mutate_with` maps a forward timeout to `Busy`, and
/// `mutate_op_rebasable`'s fallback (`crates/cli/src/fusefs.rs`) then
/// acquires the lease and executes the very op the holder already
/// applied. `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS` (1500ms) makes
/// the holder's reply arrive well after the requester's
/// `CONSTELLATION_FORWARD_TIMEOUT_MS` (default 500ms) gives up, so this
/// reproduces deterministically without racing real scheduler timing.
/// With M2, the requester retries the same rid to the same holder
/// (which answers from `recent` without re-executing — the fault delay
/// applies to every reply, so these retries time out too) until it
/// falls to the lease path, where the coverage rule resolves it against
/// `completed` instead of executing again.
///
/// Five rounds alternate which node holds the lease (a, b, a, b, a) and
/// exercise the five create-family/delete-family ops POSIX distinguishes
/// by idempotency: `O_EXCL` create and `mkdir` (EEXIST on replay),
/// `unlink` (ENOENT on replay), `rename` (source already gone) and
/// `link` (EEXIST on replay). The requester's own busy-fallback handoff
/// request is what flips the holder for the next round — no separate
/// mechanism is needed to alternate it.
fn forward_timeout_reexec(_seed: u64) -> Result<()> {
    let (env, root) = setup("forward-timeout-reexec")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/forward-timeout-{}", ts());
    let tune = |c: Client| {
        c.with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", "10000")
            .with_env("CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS", "1500")
    };
    let mut a = tune(Client::new(root.path(), "a", &env.endpoint, &backend)?);
    let mut b = tune(Client::new(root.path(), "b", &env.endpoint, &backend)?);
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;

    let mut anomalies: Vec<String> = Vec::new();
    let dedup_at_start = dedup_evidence(&[&a, &b])?;

    for round in 1..=5u32 {
        let (holder, requester): (&Client, &Client) =
            if round % 2 == 1 { (&a, &b) } else { (&b, &a) };

        // Establish the intended holder: a no-op if the previous round's
        // fallback already left it holding (the common case from round 2
        // on), otherwise force it via a local write of its own.
        if lease_of(holder)?["held"] != true {
            std::fs::write(holder.mnt.join(format!(".establish-holder-{round}")), b"x")
                .with_context(|| {
                    format!("round {round}: establishing {} as holder", holder.name)
                })?;
        }
        eventually(
            &format!("round {round}: {} holds the lease", holder.name),
            Duration::from_secs(30),
            || {
                let lease = lease_of(holder)?;
                anyhow::ensure!(
                    lease["held"] == true,
                    "{} not holding: {lease}",
                    holder.name
                );
                Ok(())
            },
        )?;

        // Seed any input the round's op needs, from the holder, and wait
        // for the requester to see it — this must be visible before the
        // fault-affected op below, or the op's own failure would be an
        // ordinary race rather than the bug this scenario targets.
        let seed = |name: &str, content: &[u8]| -> Result<()> {
            std::fs::write(holder.mnt.join(name), content)
                .with_context(|| format!("round {round}: seeding {name} on {}", holder.name))?;
            eventually(
                &format!("round {round}: {name} visible on {}", requester.name),
                Duration::from_secs(15),
                || {
                    anyhow::ensure!(requester.mnt.join(name).exists(), "{name} not visible yet");
                    Ok(())
                },
            )
        };

        let op_name;
        let target: String;
        let src: Option<String>;
        let before = forwarded_err(requester)?;
        let dedup_before = dedup_evidence(&[&a, &b])?;
        let result: std::io::Result<()> = match round {
            1 => {
                op_name = "O_EXCL-create";
                target = format!("excl-{round}");
                src = None;
                create_new(&requester.mnt, &target)
            }
            2 => {
                op_name = "mkdir";
                target = format!("dir-{round}");
                src = None;
                std::fs::create_dir(requester.mnt.join(&target))
            }
            3 => {
                op_name = "unlink";
                target = format!("unlink-me-{round}");
                src = None;
                seed(&target, b"doomed")?;
                std::fs::remove_file(requester.mnt.join(&target))
            }
            4 => {
                op_name = "rename";
                let from = format!("rename-src-{round}");
                let to = format!("rename-dst-{round}");
                seed(&from, b"movable")?;
                let result = std::fs::rename(requester.mnt.join(&from), requester.mnt.join(&to));
                target = to;
                src = Some(from);
                result
            }
            5 => {
                op_name = "link";
                let from = format!("link-src-{round}");
                let to = format!("link-dst-{round}");
                seed(&from, b"linkable")?;
                let result = std::fs::hard_link(requester.mnt.join(&from), requester.mnt.join(&to));
                target = to;
                src = Some(from);
                result
            }
            _ => unreachable!(),
        };
        let after = forwarded_err(requester)?;
        let dedup_after = dedup_evidence(&[&a, &b])?;

        if after <= before {
            anomalies.push(format!(
                "round {round} {op_name} {target}: fault injection did not engage \
                 ({}'s forwarded_err stayed at {before})",
                requester.name
            ));
        }
        // Diagnostic only, not a per-round requirement: dedup/in-doubt-
        // resolution evidence rising shows *this* round's retry landed
        // on an already-completed op and was recognized as a duplicate
        // rather than re-executed. It rising is sufficient proof of
        // that, but it *not* rising is not by itself a bug — a race can
        // just as correctly resolve the other way, with the lease path
        // discovering the op was never durably completed anywhere yet
        // (nobody executed it before the holder handed off) and
        // legitimately executing it fresh, exactly once, with no
        // duplicate to detect. Either shape is fine; what must never
        // happen is a *second* execution, which `ino_agrees` below
        // catches directly by comparing inode numbers. The aggregate
        // check after the loop confirms the dedup path is not simply
        // dead code across the whole run.
        eprintln!(
            "    forward-timeout-reexec round {round} {op_name} {target}: holder={} \
             requester={} forwarded_err {before}->{after} dedup_evidence {dedup_before}->{dedup_after} \
             result={result:?}",
            holder.name, requester.name
        );
        if let Err(e) = &result {
            anomalies.push(format!(
                "round {round} {op_name} {target}: returned {e} (expected success); \
                 holder executed it, the reply timed out, the requester re-executed it \
                 (plan 30 bug A)"
            ));
        }

        // The intended effect must hold on both nodes regardless of what
        // errno the requester's own re-execution saw — the holder's
        // original execution is never in question here, only whether the
        // requester's second attempt corrupted or duplicated it.
        let post_check = |what: &str, f: &dyn Fn(&Client) -> bool| -> Result<()> {
            eventually(
                &format!("round {round}: {what}"),
                Duration::from_secs(20),
                || {
                    for c in [&a, &b] {
                        anyhow::ensure!(f(c), "{what} not satisfied on {}", c.name);
                    }
                    Ok(())
                },
            )
        };
        // Existence converging on both nodes is necessary but not
        // sufficient: a create-family op whose local re-execution raced
        // ahead of the holder's shipped record (rather than seeing it
        // and failing EEXIST) creates a *second*, independent inode
        // under the same name, which later log replay resolves via
        // ordinary same-name-conflict handling — no errno reaches the
        // caller, but the namespace briefly held two executions instead
        // of one. `ino_agrees` is the plan's own "verify the namespace
        // shows exactly one execution" check for that path.
        let ino_agrees = |what: &str, name: &str| -> Result<()> {
            eventually(
                &format!("round {round}: {what} names one execution"),
                Duration::from_secs(20),
                || {
                    let (ia, ib) = (ino_of(&a.mnt.join(name)), ino_of(&b.mnt.join(name)));
                    anyhow::ensure!(
                        ia.is_some() && ia == ib,
                        "{name} has inode {ia:?} on a and {ib:?} on b"
                    );
                    Ok(())
                },
            )
        };
        let mut post_errs: Vec<String> = Vec::new();
        match round {
            1 | 2 => {
                if let Err(e) = post_check(&format!("{target} exists"), &|c| {
                    c.mnt.join(&target).exists()
                }) {
                    post_errs.push(format!("{e:#}"));
                } else if let Err(e) = ino_agrees("the created name", &target) {
                    post_errs.push(format!(
                        "{e:#}: the requester's local re-execution created a second entry \
                         instead of converging on the holder's (plan 30 bug A)"
                    ));
                }
            }
            3 => {
                if let Err(e) = post_check(&format!("{target} is gone"), &|c| {
                    !c.mnt.join(&target).exists()
                }) {
                    post_errs.push(format!("{e:#}"));
                }
            }
            // rename: the source name must be gone.
            4 => {
                let src = src.clone().unwrap();
                if let Err(e) =
                    post_check(&format!("{src} is gone"), &|c| !c.mnt.join(&src).exists())
                {
                    post_errs.push(format!("{e:#}"));
                }
                if let Err(e) = post_check(&format!("{target} exists"), &|c| {
                    c.mnt.join(&target).exists()
                }) {
                    post_errs.push(format!("{e:#}"));
                } else if let Err(e) = ino_agrees("the rename target", &target) {
                    post_errs.push(format!(
                        "{e:#}: the requester's local re-execution created a second entry \
                         instead of converging on the holder's (plan 30 bug A)"
                    ));
                }
            }
            // link: the source name must still exist too (unlike rename).
            5 => {
                let src = src.clone().unwrap();
                if let Err(e) = post_check(&format!("{src} still exists"), &|c| {
                    c.mnt.join(&src).exists()
                }) {
                    post_errs.push(format!("{e:#}"));
                }
                if let Err(e) = post_check(&format!("{target} exists"), &|c| {
                    c.mnt.join(&target).exists()
                }) {
                    post_errs.push(format!("{e:#}"));
                } else if let Err(e) = ino_agrees("the link target", &target) {
                    post_errs.push(format!(
                        "{e:#}: the requester's local re-execution created a second entry \
                         instead of converging on the holder's (plan 30 bug A)"
                    ));
                }
            }
            _ => unreachable!(),
        }
        if !post_errs.is_empty() {
            anomalies.push(format!(
                "round {round} {op_name} {target}: {}",
                post_errs.join("; ")
            ));
        }
    }

    let dedup_at_end = dedup_evidence(&[&a, &b])?;
    a.unmount()?;
    b.unmount()?;
    // Plan 30 M2 non-vacuity for the fix itself (distinct from the
    // per-round `forwarded_err` check, which only proves the *fault*
    // engaged): across the whole run, the dedup/in-doubt-resolution
    // path must have fired at least once, or every round resolved by
    // coincidence rather than by the mechanism plan 30 M2 adds.
    if dedup_at_end <= dedup_at_start {
        anomalies.push(format!(
            "no round showed dedup/in-doubt-resolution evidence anywhere \
             (forward_dedup_hits + forward_indoubt_resolved stayed at {dedup_at_start} \
             across both nodes for the whole run) — exactly-once forwarding may not be engaging"
        ));
    }
    if !anomalies.is_empty() {
        bail!(
            "{} of 5 round(s) anomalous:\n{}",
            anomalies.len(),
            anomalies.join("\n")
        );
    }
    Ok(())
}

/// Common setup for the two bug-B scenarios: three nodes, A's S3 behind its
/// own switchable proxy, B and C on the ordinary shared one. Returns the
/// three mounted clients plus the switch, positioned right after A has
/// taken the lease and both B and C see its marker — i.e. right before
/// the caller cuts A's S3 and does the stranding write.
fn phantom_setup(scenario: &str) -> Result<PhantomRig> {
    phantom_setup_with(scenario, &[])
}

/// What [`phantom_setup`] hands back: the environment, the scratch root,
/// nodes A, B and C, and A's S3 switch.
type PhantomRig = (
    S3Env,
    tempfile::TempDir,
    Client,
    Client,
    Client,
    CountingProxy,
);

/// As [`phantom_setup`], with `extra` environment on all three nodes
/// (plan 30 M3b: `takeover-marker-strands-promptly` shortens the idle
/// poll ceiling so its promptness bound does not ride on the P2P push).
fn phantom_setup_with(scenario: &str, extra: &[(&str, &str)]) -> Result<PhantomRig> {
    let (env, root) = setup(scenario)?;
    let _proxy = env.s3_proxy()?;
    let sw = env.counting_proxy()?;
    let backend = format!("s3://{BUCKET}/{scenario}-{}", ts());
    let tune = |c: Client| {
        let mut c = c
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", "6000");
        for (key, value) in extra {
            c = c.with_env(key, value);
        }
        c
    };
    let mut a = tune(Client::new(root.path(), "a", &sw.endpoint(), &backend)?);
    let mut b = tune(Client::new(root.path(), "b", &env.endpoint, &backend)?);
    let mut c = tune(Client::new(root.path(), "c", &env.endpoint, &backend)?);
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;

    std::fs::write(a.mnt.join("marker"), b"a")?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&a)?;
        anyhow::ensure!(lease["held"] == true, "A does not hold: {lease}");
        Ok(())
    })?;
    eventually("marker visible on B and C", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("marker").is_file(), "marker missing on B");
        anyhow::ensure!(c.mnt.join("marker").is_file(), "marker missing on C");
        Ok(())
    })?;
    Ok((env, root, a, b, c, sw))
}

/// Strand B's forwarded create by cutting A's S3 immediately before B
/// issues it (A still holds a valid, unexpired lease and acks purely
/// from memory), then crash A so the ack can never ship.
fn strand_bs_forwarded_phantom(a: &mut Client, b: &Client, sw: &CountingProxy) -> Result<()> {
    sw.cut();
    create_new(&b.mnt, "phantom")
        .context("B's forwarded create must be acked while A's lease is still valid")?;
    eventually("phantom visible on B", Duration::from_secs(10), || {
        anyhow::ensure!(b.mnt.join("phantom").exists(), "phantom not yet on B");
        Ok(())
    })?;
    a.kill9()?;
    Ok(())
}

/// Plan 30 M3a's speculation counters from a node's `status`, for the
/// scenario log and the non-vacuity checks.
fn speculation_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["speculation"].clone())
}

/// Wait until `phantom` exists on every client in `nodes`, as one and the
/// same inode.
fn phantom_everywhere(nodes: &[(&str, &Client)], deadline: Duration) -> Result<()> {
    eventually(
        "the stranded create is visible everywhere",
        deadline,
        || {
            let mut inos = Vec::new();
            for (name, c) in nodes {
                let ino = ino_of(&c.mnt.join("phantom"))
                    .with_context(|| format!("phantom missing on {name}"))?;
                inos.push((*name, ino));
            }
            anyhow::ensure!(
                inos.iter().all(|(_, ino)| *ino == inos[0].1),
                "nodes disagree on phantom's inode: {inos:?}"
            );
            Ok(())
        },
    )
}

/// Mount a fresh node `d` on `backend`, which bootstraps from the head
/// commit plus the log tail, and wait until it sees the post-takeover
/// write `after` (content `expect`).
fn fresh_node(env: &S3Env, root: &std::path::Path, backend: &str, expect: &[u8]) -> Result<Client> {
    let mut d = Client::new(root, "d", &env.endpoint, backend)?
        .with_own_node_key()
        .with_env("CONSTELLATION_LEASE_TTL_MS", "6000");
    d.mount()?;
    eventually(
        "D sees the post-takeover write",
        Duration::from_secs(60),
        || {
            anyhow::ensure!(std::fs::read(d.mnt.join("after"))? == expect);
            Ok(())
        },
    )?;
    Ok(d)
}

/// Bug B (`docs/plans/v1/done/30-write-path-resilience-and-scale-out.md`
/// §1.1), third-node-takeover shape, as plan 30 M3a's regression test. A
/// requester (B) applies an accepted forwarded create ahead of the log
/// (a speculation-log shadow); the holder (A) dies before shipping it, and
/// a third node (C) takes over. Before M3a the effect stayed on B for good
/// and B published it into the commit chain. Now C's first segment
/// strands B's shadow: B rolls it back and replays the create by rid
/// through C, so the create B's application was told succeeded becomes
/// durable, and B, C and a fresh node D bootstrapped from the bucket all
/// agree it exists — as one inode.
fn holder_crash_phantom_shadow(_seed: u64) -> Result<()> {
    let (env, root, mut a, mut b, mut c, sw) = phantom_setup("holder-crash-phantom-shadow")?;

    strand_bs_forwarded_phantom(&mut a, &b, &sw)?;

    // C's write forwards to (now-dead) A, fails, and C takes over once
    // A's lease expires — legitimately several seconds (up to the
    // remainder of A's TTL from its last renewal).
    std::fs::write(c.mnt.join("after"), b"c").context("C must take over once A's lease expires")?;
    eventually(
        "B sees C's post-takeover write",
        Duration::from_secs(30),
        || {
            anyhow::ensure!(std::fs::read(b.mnt.join("after"))? == b"c");
            Ok(())
        },
    )?;

    let checked = (|| -> Result<Client> {
        phantom_everywhere(&[("b", &b), ("c", &c)], Duration::from_secs(30)).context(
            "after C's takeover, B's stranded create must be replayed through C (plan 30 bug B)",
        )?;
        let spec = speculation_of(&b)?;
        eprintln!("    holder-crash-phantom-shadow: b speculation after recovery: {spec}");
        anyhow::ensure!(
            spec["rolled_back"].as_u64().unwrap_or(0) >= 1
                && spec["stranded_replayed"].as_u64().unwrap_or(0) >= 1,
            "b's status shows no rollback/replay — the recovery path did not engage: {spec}"
        );
        anyhow::ensure!(
            spec["replay_conflicts"].as_u64().unwrap_or(0) == 0,
            "the replay was refused, but nothing else ever took the name: {spec}"
        );
        // The replayed create is itself a shadow on B until C ships it;
        // wait for it to retire so B's unmount can publish a commit.
        eventually("b's speculation retires", Duration::from_secs(30), || {
            let spec = speculation_of(&b)?;
            anyhow::ensure!(
                spec["outstanding"].as_u64() == Some(0)
                    && spec["pending_replay"].as_u64() == Some(0),
                "b still speculating: {spec}"
            );
            Ok(())
        })?;
        // Clean unmount of B publishes a metadata commit; a fresh node D
        // then bootstraps purely from the bucket (head commit + log tail).
        b.unmount()?;
        let d = fresh_node(&env, root.path(), &c.backend, b"c")?;
        phantom_everywhere(&[("c", &c), ("d", &d)], Duration::from_secs(30))
            .context("a fresh node must see the replayed create exactly as c does")?;
        eprintln!("    holder-crash-phantom-shadow: b, c and fresh d agree: phantom exists (replayed by rid)");
        Ok(d)
    })();

    c.unmount()?;
    let mut d = checked?;
    d.unmount()?;
    Ok(())
}

/// Bug B, requester-takeover shape, as plan 30 M3a's regression test:
/// same stranding as [`holder_crash_phantom_shadow`], but the *requester*
/// (B) becomes the next holder. Before M3a, B then validated new creates
/// against its own phantom entry. Now B's takeover gate rolls the shadow
/// back and replays the create locally, by rid, before B serves anything
/// as holder. The create B's application was told succeeded is therefore
/// durable, so C's `O_EXCL` create of the same name must fail with
/// `EEXIST` — the linearizable answer — and B, C and a fresh node D agree
/// the name exists, as one inode.
fn holder_crash_phantom_new_holder(_seed: u64) -> Result<()> {
    let (env, root, mut a, mut b, mut c, sw) = phantom_setup("holder-crash-phantom-new-holder")?;

    strand_bs_forwarded_phantom(&mut a, &b, &sw)?;

    // B (the requester who applied the stranded ack) takes over as the
    // new holder this time, instead of C.
    std::fs::write(b.mnt.join("after"), b"b")
        .context("B must take over as the new holder once A's lease expires")?;
    eventually(
        "C sees B's post-takeover write",
        Duration::from_secs(30),
        || {
            anyhow::ensure!(std::fs::read(c.mnt.join("after"))? == b"b");
            Ok(())
        },
    )?;

    let checked = (|| -> Result<Client> {
        let spec = speculation_of(&b)?;
        eprintln!("    holder-crash-phantom-new-holder: b speculation after takeover: {spec}");
        anyhow::ensure!(
            spec["rolled_back"].as_u64().unwrap_or(0) >= 1
                && spec["stranded_replayed"].as_u64().unwrap_or(0) >= 1,
            "b's takeover gate did not roll back and replay the stranded create: {spec}"
        );
        match create_new(&c.mnt, "phantom") {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Ok(()) => bail!(
                "c's O_EXCL create of \"phantom\" succeeded: b's acknowledged create was lost \
                 instead of replayed at b's takeover (plan 30 bug B)"
            ),
            Err(e) => bail!("c's create of \"phantom\" returned {e} (expected EEXIST)"),
        }
        phantom_everywhere(&[("b", &b), ("c", &c)], Duration::from_secs(30))?;
        b.unmount()?;
        let d = fresh_node(&env, root.path(), &c.backend, b"b")?;
        phantom_everywhere(&[("c", &c), ("d", &d)], Duration::from_secs(30))
            .context("a fresh node must see the replayed create exactly as c does")?;
        eprintln!(
            "    holder-crash-phantom-new-holder: c's create got EEXIST; b, c and fresh d agree: \
             phantom exists (replayed at b's takeover)"
        );
        Ok(d)
    })();

    c.unmount()?;
    let mut d = checked?;
    d.unmount()?;
    Ok(())
}

/// The envelope of log segment `seq` in partition `part`, straight from
/// the bucket: `(node, epoch, record count)`. A non-E2E segment is a zstd
/// postcard `SegmentEnvelope { v, node, epoch, records }`
/// (`crates/cli/src/shipper.rs`); every field up to the record vector's
/// length prefix is a plain varint, so the header decodes without the
/// record types. Plan 30 M3b uses it to identify an epoch marker (an
/// empty segment at the new holder's epoch).
fn segment_header(endpoint: &str, prefix: &str, part: &str, seq: u64) -> Result<(u64, u64, u64)> {
    let key = format!("{prefix}/log/{part}/{seq:016x}.zst");
    let mut compressed = Vec::new();
    crate::s3auth::get(&raw_key(endpoint, &key))
        .call()
        .with_context(|| format!("fetching {key}"))?
        .into_reader()
        .read_to_end(&mut compressed)?;
    let payload =
        zstd::decode_all(&compressed[..]).with_context(|| format!("decompressing {key}"))?;
    let ((v, node, epoch, records), _) =
        postcard::take_from_bytes::<(u32, u64, u64, u64)>(&payload)
            .with_context(|| format!("decoding {key}'s envelope"))?;
    // Plan 30 §M7 bumped the wire envelope to v3 (`through`) and §M9
    // added `rows`; both are appended fields this prefix decode never
    // reads, so only the version constant itself needed updating.
    anyhow::ensure!(v == 3, "{key}: unexpected segment envelope version {v}");
    Ok((node, epoch, records))
}

/// Every commit object's key under `prefix`, oldest first (`commits/`
/// keys are zero-padded hex sequence numbers, so lexical order is chain
/// order).
fn commit_keys(endpoint: &str, prefix: &str) -> Result<Vec<String>> {
    let mut keys: Vec<String> = raw_objects(endpoint, &format!("{prefix}/commits/"))?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    keys.sort();
    Ok(keys)
}

/// One commit object, parsed (a non-E2E commit is plain JSON:
/// `store-s3::commits::Commit`).
fn read_commit(endpoint: &str, key: &str) -> Result<serde_json::Value> {
    let mut body = Vec::new();
    crate::s3auth::get(&raw_key(endpoint, key))
        .call()
        .with_context(|| format!("fetching {key}"))?
        .into_reader()
        .read_to_end(&mut body)?;
    serde_json::from_slice(&body).with_context(|| format!("parsing commit {key}"))
}

/// Plan 30 §M3b's epoch marker, as a regression test. A holds the lease;
/// C's forwarded create is accepted by A from memory (A's S3 is cut) and
/// sits on C as a speculation-log shadow; A dies without shipping it. B
/// then takes the expired lease through an op that is *refused* — `rmdir`
/// of a non-empty directory, `ENOTEMPTY` — so B's own op ships nothing.
/// (Not `mkdir` of an existing name: the kernel can answer that `EEXIST`
/// from its dentry cache without ever calling the daemon, so it might not
/// take the lease at all. `rmdir`'s emptiness check is the filesystem's.)
/// Before M3b C's shadow stayed in place until B next happened to write
/// something; now B's takeover ships an empty segment at its new epoch
/// right after the CAS, and C must strand the shadow within a few seconds
/// of B's `rmdir` returning. The first segment of B's epoch in the bucket
/// must be that marker (B's node, zero records), and the stranded create
/// is then replayed by rid through B exactly once: B, C and a fresh node
/// D all see `phantom` as one inode.
fn takeover_marker_strands_promptly(_seed: u64) -> Result<()> {
    use std::time::Instant;
    /// How long after B's takeover C may take to strand its shadow. The
    /// marker is pushed over P2P at once, and the idle poll ceiling is
    /// lowered to 1 s as the fallback; nothing else B does in this window
    /// ships anything that could strand it.
    const PROMPT: Duration = Duration::from_secs(5);
    let (env, root, mut a, mut b, mut c, sw) = phantom_setup_with(
        "takeover-marker-strands-promptly",
        &[("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")],
    )?;
    let prefix = b
        .backend
        .strip_prefix(&format!("s3://{BUCKET}/"))
        .context("backend is not on the harness bucket")?
        .to_string();
    let a_epoch = lease_of(&a)?["epoch"].as_u64().unwrap_or(0);
    let b_node = b.control_status()?["node_id"]
        .as_u64()
        .context("b reports no node_id")?;

    // The non-empty directory B's refused op targets, made while A's S3 is
    // still up so it is ordinary shipped history everywhere.
    std::fs::create_dir_all(a.mnt.join("full/child"))?;
    eventually(
        "full/child visible on B and C",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(b.mnt.join("full/child").is_dir(), "full/child missing on B");
            anyhow::ensure!(c.mnt.join("full/child").is_dir(), "full/child missing on C");
            Ok(())
        },
    )?;
    let rolled_before = speculation_of(&c)?["rolled_back"].as_u64().unwrap_or(0);

    // Strand C's forwarded create the way the M3a scenarios strand B's:
    // cut A's S3 (A still holds a valid lease and acks from memory), then
    // crash A so the ack can never ship.
    sw.cut();
    create_new(&c.mnt, "phantom")
        .context("C's forwarded create must be acked while A's lease is still valid")?;
    eventually("phantom visible on C", Duration::from_secs(10), || {
        anyhow::ensure!(c.mnt.join("phantom").exists(), "phantom not yet on C");
        Ok(())
    })?;
    a.kill9()?;
    let spec = speculation_of(&c)?;
    anyhow::ensure!(
        spec["outstanding"].as_u64().unwrap_or(0) >= 1,
        "C must hold the accepted create as an outstanding shadow: {spec}"
    );

    let checked = (|| -> Result<Client> {
        // B's refused op: it forwards to dead A, fails, waits out A's
        // lease and takes over (CAS, epoch marker, takeover gate), then
        // executes locally and is refused. Nothing of its own ships.
        match std::fs::remove_dir(b.mnt.join("full")) {
            Ok(()) => bail!("B's rmdir of the non-empty `full` succeeded"),
            Err(e) if Code::from_os_error(&e) == Some(Code::NotEmpty) => {}
            Err(e) => bail!("B's rmdir of the non-empty `full` returned {e}, expected ENOTEMPTY"),
        }
        let took_over = Instant::now();
        let lease = lease_of(&b)?;
        let b_epoch = lease["epoch"].as_u64().unwrap_or(0);
        anyhow::ensure!(
            lease["held"] == true && b_epoch > a_epoch,
            "B's refused op must still have taken the lease at an epoch newer than A's \
             {a_epoch}: {lease}"
        );
        eventually("C's shadow strands after B's takeover", PROMPT, || {
            let spec = speculation_of(&c)?;
            anyhow::ensure!(
                spec["rolled_back"].as_u64().unwrap_or(0) > rolled_before,
                "C has not rolled its shadow back: {spec}"
            );
            Ok(())
        })
        .context(
            "B's own op was refused and shipped nothing, so only B's epoch marker can strand \
             C's shadow this soon (plan 30 M3b)",
        )?;
        let strand_ms = took_over.elapsed().as_millis();
        let b_spec = speculation_of(&b)?;
        anyhow::ensure!(
            b_spec["epoch_markers"].as_u64().unwrap_or(0) >= 1,
            "B took over from a holder that never released but shipped no epoch marker: {b_spec}"
        );
        // The first segment of B's epoch is B's empty marker.
        let mut marker = None;
        for seq in log_segment_seqs(&env.direct_endpoint, &prefix, "p0")? {
            let (node, epoch, records) = segment_header(&env.direct_endpoint, &prefix, "p0", seq)?;
            if epoch > a_epoch {
                marker = Some((seq, node, epoch, records));
                break;
            }
        }
        let (seq, node, epoch, records) =
            marker.context("no log segment carries an epoch newer than A's")?;
        anyhow::ensure!(
            node == b_node && epoch == b_epoch && records == 0,
            "the first segment of the new epoch (seq {seq}) must be B's (node {b_node}) empty \
             epoch-{b_epoch} marker; it is node {node}, epoch {epoch}, {records} record(s)"
        );
        eprintln!(
            "    takeover-marker-strands-promptly: c stranded its shadow {strand_ms} ms after b's \
             refused takeover op; marker at seq {seq}"
        );

        // The stranded create is replayed by rid through B exactly once.
        phantom_everywhere(&[("b", &b), ("c", &c)], Duration::from_secs(40))
            .context("C's stranded create must be replayed through B")?;
        let spec = speculation_of(&c)?;
        anyhow::ensure!(
            spec["stranded_replayed"].as_u64().unwrap_or(0) >= 1,
            "C's status shows no replay — the recovery path did not engage: {spec}"
        );
        anyhow::ensure!(
            spec["replay_conflicts"].as_u64().unwrap_or(0) == 0,
            "the replay was refused, but nothing else ever took the name: {spec}"
        );
        eventually("c's speculation retires", Duration::from_secs(30), || {
            let spec = speculation_of(&c)?;
            anyhow::ensure!(
                spec["outstanding"].as_u64() == Some(0)
                    && spec["pending_replay"].as_u64() == Some(0),
                "c still speculating: {spec}"
            );
            Ok(())
        })?;
        // A fresh node bootstraps from the bucket alone (head commit plus
        // the log after it) and must agree too. C's clean unmount
        // publishes a commit first, as in the M3a scenarios.
        std::fs::write(b.mnt.join("after"), b"b")?;
        eventually("C sees B's later write", Duration::from_secs(30), || {
            anyhow::ensure!(std::fs::read(c.mnt.join("after"))? == b"b");
            Ok(())
        })?;
        c.unmount()?;
        let d = fresh_node(&env, root.path(), &b.backend, b"b")?;
        phantom_everywhere(&[("b", &b), ("d", &d)], Duration::from_secs(30))
            .context("a fresh node must see the replayed create exactly as b does")?;
        Ok(d)
    })();

    // A no-op when the closure already unmounted C.
    c.unmount()?;
    b.unmount()?;
    let mut d = checked?;
    d.unmount()?;
    Ok(())
}

/// Name of the burst writer's `i`-th directory in
/// `holder-publishes-log-prefix`: zero-padded, so a sorted listing is
/// creation order.
fn burst_name(i: u64) -> String {
    format!("d{i:06}")
}

/// `burst/`'s entries under `mnt`, sorted.
fn burst_listing(mnt: &std::path::Path) -> Result<Vec<String>> {
    let mut names = std::fs::read_dir(mnt.join("burst"))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

/// Plan 30 §M3b's publish rule, as a regression test: a holder publishes
/// while its journal is non-empty, and every commit equals the log prefix
/// at its `applied` position (unshipped keys are published at their
/// before-images). Before M3b a commit could also carry the author's
/// unshipped journal suffix — harmless while the author lived to ship it,
/// but if it died first, a node bootstrapping from that commit showed
/// effects the log never received.
///
/// Holder A runs a paced `mkdir` burst (metadata only: no close nudge,
/// no chunk uploads) with a 50 ms sync interval under 25 ms of injected S3
/// latency each way, so every ship round leaves fresh transactions in the
/// journal behind it and the 32-segment publish cadence fires every few
/// seconds mid-burst. (`CONSTELLATION_PUBLISH_IDLE_S` is deliberately not
/// set: the idle publish only runs on a round whose journal is empty, so
/// it is not what publishes mid-burst, and its commits would only blur the
/// sampling below.) Non-vacuity: at least one status sample must see a
/// new A-authored commit appear while A's `speculation.local` is non-zero.
/// A is then SIGKILLed right after its next commit becomes visible in the
/// bucket — before its round, which sees the CAS reply 25 ms later, can
/// ship what was journaled meanwhile. B has tailed the log from the start;
/// a fresh node D bootstraps from the head commit plus the log after it.
/// D and B must list exactly the same `burst/` entries, those must be a
/// contiguous prefix of A's mkdir sequence, and A must have acknowledged
/// more mkdirs than that prefix (the kill caught an unshipped tail, which
/// neither node may show).
///
/// What this cannot check directly is "commit == log prefix at `applied`"
/// in isolation: the harness cannot decode log records, and D replays the
/// log after `applied` on top of the commit. The two differ observably
/// only when the commit holds something the log never received — exactly
/// the crash case above, which D == B catches.
fn holder_publishes_log_prefix(seed: u64) -> Result<()> {
    holder_publishes_log_prefix_mode(seed, false)
}

/// Plan 30 §M9: the same burst with B as A's backup. A's acknowledged
/// tail is on B when A dies, so B (sealing, taking over) re-ships it:
/// *every* acknowledged mkdir reaches the log, and a fresh node still
/// matches the follower exactly.
fn holder_publishes_log_prefix_backup(seed: u64) -> Result<()> {
    holder_publishes_log_prefix_mode(seed, true)
}

fn holder_publishes_log_prefix_mode(_seed: u64, backup: bool) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;
    /// Pause between the writer's mkdirs: ~200/s, a few dozen per ship
    /// round (sync interval plus one PUT under the injected latency).
    const PACE: Duration = Duration::from_millis(5);
    let name = if backup {
        "holder-publishes-log-prefix-backup"
    } else {
        "holder-publishes-log-prefix"
    };
    let (env, root) = setup(name)?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("log-prefix-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    // A 1 s idle poll ceiling so B and D reach the final head promptly.
    // Plan 30 §M9: without a backup (today's `ack=local`, the scenario's
    // original premise: an acknowledged tail can die with the holder),
    // or with B as A's backup on this LAN.
    let tune = |c: Client| {
        c.with_own_node_key()
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")
            .with_env(
                "CONSTELLATION_BACKUP_RTT_BUDGET_MS",
                if backup { "5" } else { "0" },
            )
    };
    let mut a = tune(Client::new(root.path(), "a", &env.endpoint, &backend)?)
        .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "50");
    let mut b = tune(Client::new(root.path(), "b", &env.endpoint, &backend)?);
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;
    let a_node = a.control_status()?["node_id"]
        .as_u64()
        .context("a reports no node_id")?;
    if backup {
        eventually("A lists B as its backup", Duration::from_secs(30), || {
            let ack = a.control_status()?["ack"].clone();
            anyhow::ensure!(
                ack["policy"] == "backup"
                    && ack["backups"].as_array().is_some_and(|b| !b.is_empty()),
                "A has no backup yet: {ack}"
            );
            Ok(())
        })?;
    }
    std::fs::create_dir(a.mnt.join("burst"))?;
    eventually("burst/ visible on B", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("burst").is_dir(), "burst/ missing on B");
        Ok(())
    })?;
    proxy.latency(25, 0)?;

    // The writer counts only mkdirs that returned success. After the kill
    // its next mkdir fails (ENOTCONN, or ENOENT once the dead mount is
    // detached: the bare mountpoint has no `burst/`), which ends it.
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let writer = {
        let dir = a.mnt.join("burst");
        let (stop, acked) = (stop.clone(), acked.clone());
        std::thread::spawn(move || -> Option<String> {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Err(e) = std::fs::create_dir(dir.join(burst_name(i))) {
                    return Some(format!("mkdir #{i}: {e}"));
                }
                i += 1;
                acked.store(i, Ordering::Relaxed);
                std::thread::sleep(PACE);
            }
            None
        })
    };

    // Whether the head commit is new since `seen` and authored by A (B
    // may publish too, from what it tailed).
    let new_a_commit = |seen: &mut Option<String>| -> Result<bool> {
        let Some(head) = commit_keys(&env.direct_endpoint, &prefix)?.pop() else {
            return Ok(false);
        };
        if seen.as_deref() == Some(head.as_str()) {
            return Ok(false);
        }
        let commit = read_commit(&env.direct_endpoint, &head)?;
        *seen = Some(head);
        Ok(commit["author"].as_u64() == Some(a_node))
    };
    let burst = (|| -> Result<(u32, u32, u64)> {
        let mut seen = commit_keys(&env.direct_endpoint, &prefix)?.pop();
        let (mut samples, mut hits, mut max_local) = (0u32, 0u32, 0u64);
        let sampling = Instant::now();
        while hits < 2 && sampling.elapsed() < Duration::from_secs(60) {
            std::thread::sleep(Duration::from_millis(100));
            anyhow::ensure!(!writer.is_finished(), "the burst writer stopped early");
            let local = speculation_of(&a)?["local"].as_u64().unwrap_or(0);
            max_local = max_local.max(local);
            samples += 1;
            if new_a_commit(&mut seen)? && local > 0 {
                hits += 1;
            }
        }
        anyhow::ensure!(
            hits >= 1,
            "no sample saw a new commit from A while A's journal held captured local work \
             ({samples} samples, max speculation.local {max_local}) — the publish-while-\
             unshipped path was never exercised"
        );
        // Kill A the moment its next commit is visible (the harness reads
        // the bucket directly, with no injected latency).
        let deadline = Instant::now() + Duration::from_secs(60);
        while !new_a_commit(&mut seen)? {
            anyhow::ensure!(!writer.is_finished(), "the burst writer stopped early");
            anyhow::ensure!(
                Instant::now() < deadline,
                "A published no further commit within 60 s"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        a.kill9()?;
        Ok((samples, hits, max_local))
    })();
    stop.store(true, Ordering::Relaxed);
    let writer_end = writer
        .join()
        .map_err(|_| anyhow::anyhow!("the burst writer panicked"))?;
    let (samples, hits, max_local) =
        burst.with_context(|| format!("burst writer ended with: {writer_end:?}"))?;
    let acked_n = acked.load(Ordering::Relaxed);
    proxy.remove_all_toxics()?;

    let mut d = tune(Client::new(root.path(), "d", &env.endpoint, &backend)?);
    let checked = (|| -> Result<()> {
        d.mount()
            .context("fresh node bootstrap from the head commit plus the log after it")?;
        let log_head_now = || -> Result<u64> {
            Ok(log_segment_seqs(&env.direct_endpoint, &prefix, "p0")?
                .last()
                .copied()
                .unwrap_or(0))
        };
        if backup {
            // The loss check below is only meaningful once B has sealed A
            // and taken over (re-shipping A's acknowledged tail) and the
            // log has stopped growing; under host load that takes a while.
            eventually(
                "B seals A and takes over as its backup",
                Duration::from_secs(90),
                || {
                    let b_ack = b.control_status()?["ack"].clone();
                    anyhow::ensure!(
                        b_ack["backup_takeovers"].as_u64().unwrap_or(0) >= 1
                            && b_ack["seals"].as_u64().unwrap_or(0) >= 1,
                        "B has not yet sealed and taken over: {b_ack}"
                    );
                    Ok(())
                },
            )?;
            let (mut last, mut since) = (log_head_now()?, Instant::now());
            let settle_deadline = Instant::now() + Duration::from_secs(90);
            while since.elapsed() < Duration::from_secs(2) {
                anyhow::ensure!(
                    Instant::now() < settle_deadline,
                    "the log head kept moving for 90 s after B's takeover (now {last})"
                );
                std::thread::sleep(Duration::from_millis(100));
                let now = log_head_now()?;
                if now != last {
                    (last, since) = (now, Instant::now());
                }
            }
        }
        let mut listing = Vec::new();
        eventually(
            "b and the fresh d converge on the shipped log",
            Duration::from_secs(90),
            || {
                // Re-read every time: a PUT already on the wire when A
                // died may still land.
                let head = log_segment_seqs(&env.direct_endpoint, &prefix, "p0")?
                    .last()
                    .copied()
                    .unwrap_or(0);
                for c in [&b, &d] {
                    let at = c.control_status()?["spool"]["head_seq"]
                        .as_u64()
                        .unwrap_or(0);
                    anyhow::ensure!(
                        at >= head,
                        "{} applied through {at}, log head {head}",
                        c.name
                    );
                }
                let on_b = burst_listing(&b.mnt)?;
                let on_d = burst_listing(&d.mnt)?;
                anyhow::ensure!(
                    on_b == on_d,
                    "b lists {} burst dirs, the fresh d {} (first difference at {:?})",
                    on_b.len(),
                    on_d.len(),
                    on_b.iter().zip(&on_d).position(|(x, y)| x != y)
                );
                listing = on_b;
                Ok(())
            },
        )
        .context("a node bootstrapped from the commit chain must see exactly the log")?;
        let shipped = listing.len() as u64;
        for (i, name) in listing.iter().enumerate() {
            anyhow::ensure!(
                *name == burst_name(i as u64),
                "the visible burst is not a contiguous prefix of A's mkdir sequence: \
                 position {i} holds {name}"
            );
        }
        anyhow::ensure!(shipped > 0, "none of A's burst reached the log");
        if backup {
            // Plan 30 §M9: an acknowledgement rested on B's copy; B
            // re-shipped A's tail after sealing and taking over.
            // (`>=`: the mkdir in flight at the kill may be on B — journaled
            // and backup-acked — without its return having reached the
            // writer.)
            if shipped < acked_n {
                let have: std::collections::HashSet<&String> = listing.iter().collect();
                let missing: Vec<String> = (0..acked_n)
                    .map(burst_name)
                    .filter(|n| !have.contains(n))
                    .collect();
                let seq = |c: &Client| -> u64 {
                    c.control_status()
                        .ok()
                        .and_then(|s| s["spool"]["head_seq"].as_u64())
                        .unwrap_or(0)
                };
                anyhow::bail!(
                    "A acknowledged {acked_n} mkdirs but only {shipped} are visible: the backup \
                     lost an acknowledged mkdir; missing {} names (first {:?}); b head_seq {}, \
                     d head_seq {}, log head {}",
                    missing.len(),
                    &missing[..missing.len().min(10)],
                    seq(&b),
                    seq(&d),
                    log_head_now()?
                );
            }
            let b_ack = b.control_status()?["ack"].clone();
            anyhow::ensure!(
                b_ack["backup_takeovers"].as_u64().unwrap_or(0) >= 1
                    && b_ack["seals"].as_u64().unwrap_or(0) >= 1,
                "B did not seal and take over as A's backup: {b_ack}"
            );
            eprintln!(
                "    {name}: B sealed and took over; tail rows re-applied {}",
                b_ack["backup_tail_applied"]
            );
        } else {
            anyhow::ensure!(
                shipped < acked_n,
                "A acknowledged {acked_n} mkdirs and all {shipped} are visible: the kill caught no \
                 unshipped tail, so nothing here tested a commit published over one"
            );
        }
        let head_key = commit_keys(&env.direct_endpoint, &prefix)?
            .pop()
            .context("no commit was ever published")?;
        let head = read_commit(&env.direct_endpoint, &head_key)?;
        let applied = head["applied"].as_u64().unwrap_or(0);
        let log_head = log_segment_seqs(&env.direct_endpoint, &prefix, "p0")?
            .last()
            .copied()
            .unwrap_or(0);
        anyhow::ensure!(
            applied <= log_head,
            "head commit {head_key} claims log position {applied}, past the log head {log_head}"
        );
        eprintln!(
            "    {name}: {hits}/{samples} samples saw a new commit over unshipped work (max \
             local {max_local}); A acked {acked_n} mkdirs, {shipped} reached the log, {} \
             unshipped tail never visible; head commit applied {applied}, log head {log_head}",
            acked_n.saturating_sub(shipped)
        );
        Ok(())
    })();

    b.unmount()?;
    d.unmount()?;
    checked
}

// ---------------------------------------------------------------------------
// Plan 30 M13 — forwarding through S3 when P2P is unavailable.
//
// Three scenarios, in [`SCENARIOS`] above: the P2P-off storm (the
// ping-pong replacement, measured), a requester crash mid-batch, and a
// holder takeover with a pending batch. Every M13 status field is read
// through `serde_json::Value` indexing (`status["inbox"]`, see
// `constellation_api::InboxStatus`), so a renamed field reads as zero and
// the non-vacuity checks fail loudly.
// ---------------------------------------------------------------------------

/// Plan 30 M13's `status["inbox"]` block, `Null` (every field zero)
/// until phase 2 lands.
fn inbox_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["inbox"].clone())
}

fn inbox_counter(c: &Client, field: &str) -> Result<u64> {
    Ok(inbox_of(c)?[field].as_u64().unwrap_or(0))
}

/// Batch objects under `<prefix>/inbox/` right now, by key.
fn inbox_objects(env: &S3Env, prefix: &str) -> Result<Vec<String>> {
    Ok(
        raw_objects(&env.direct_endpoint, &format!("{prefix}/inbox/"))?
            .into_iter()
            .map(|(key, _)| key)
            .collect(),
    )
}

/// Three P2P-off nodes with a short TTL, the holder on a counting
/// relay of its own and the two requesters on one each, so requests
/// per op can be attributed per role. Returns `(env, root, prefix,
/// holder, r1, r2, counters)`.
#[allow(clippy::type_complexity)]
fn inbox_cluster(
    scenario: &str,
    ttl_ms: u64,
) -> Result<(
    S3Env,
    tempfile::TempDir,
    String,
    Client,
    Client,
    Client,
    [CountingProxy; 3],
)> {
    let (env, root) = setup(scenario)?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("{scenario}-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let counters = [
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    ];
    let mk = |name: &str, endpoint: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_P2P", "off")
            .with_env("CONSTELLATION_LEASE_TTL_MS", &ttl_ms.to_string())
            // The holder must keep the lease across the whole run: the
            // point of the inbox is that nobody needs to pull it away.
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000"))
    };
    let mut holder = mk("holder", &counters[0].endpoint())?;
    let mut r1 = mk("r1", &counters[1].endpoint())?;
    let mut r2 = mk("r2", &counters[2].endpoint())?;
    holder.fs_create()?;
    holder.mount()?;
    r1.mount()?;
    r2.mount()?;

    std::fs::write(holder.mnt.join(".establish-holder"), b"x")
        .context("establishing the holder")?;
    eventually("holder holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&holder)?;
        anyhow::ensure!(lease["held"] == true, "holder not holding: {lease}");
        Ok(())
    })?;
    for r in [&r1, &r2] {
        eventually(
            &format!("{} sees the holder's marker", r.name),
            Duration::from_secs(30),
            || {
                anyhow::ensure!(r.mnt.join(".establish-holder").is_file());
                Ok(())
            },
        )?;
    }
    Ok((env, root, prefix, holder, r1, r2, counters))
}

/// The storm under the hybrid (plan 30 M13 round 3b). Two requesters
/// run a create/read/unlink storm into one shared directory with P2P
/// off, the shape of `create-storm-s3-only`. Linux serializes creates in
/// one directory (the parent's `i_rwsem`), so each requester thread's
/// ops are one sequential inbox round trip each and batches cannot form;
/// the requesters' demand is therefore *sustained*, they escalate to a
/// lease request (`wanted_by`, plan 26), and the lease moves between
/// them at the holder's dwell — today's ping-pong, with the non-holder
/// of the moment still served by the inbox instead of blocked. The bar
/// is the revised M13 target: never worse than ping-pong, i.e. at least
/// `PING_PONG_FLOOR_OPS_PER_S` (the bottom of the 41–57 ops/s band plan
/// 29 M6 and the round-2 gate measured for `CONSTELLATION_INBOX=off` on
/// this shape). An absolute floor rather than a same-run baseline: a
/// baseline needs a second three-node cluster and another storm-length
/// run, doubling the scenario, for a number the meta-bench sweep already
/// reports on every gate. Every op must still get its errno right, and
/// the round-2 breakdown plus escalations/handoffs are printed for the
/// milestone's measurement table.
///
/// Several threads per requester (`THREADS_PER_REQUESTER`), so a
/// requester keeps the inbox busy in between handoffs and its demand
/// window fills the way a real multi-process writer's would.
fn inbox_create_storm_p2p_off(seed: u64) -> Result<()> {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    const PING_PONG_FLOOR_OPS_PER_S: f64 = 41.0;
    const THREADS_PER_REQUESTER: usize = 16;
    /// Metadata ops per worker round: create, the close's manifest
    /// commit, unlink.
    const OPS_PER_FILE: u64 = 3;

    let (_env, _root, _prefix, holder, r1, r2, counters) =
        inbox_cluster("inbox-create-storm-p2p-off", 20_000)?;
    let dir = "storm";
    std::fs::create_dir_all(holder.mnt.join(dir))?;
    eventually(
        "storm dir visible on the requesters",
        Duration::from_secs(30),
        || {
            anyhow::ensure!(r1.mnt.join(dir).is_dir() && r2.mnt.join(dir).is_dir());
            Ok(())
        },
    )?;
    let epoch_before = lease_of(&holder)?["epoch"].as_u64().unwrap_or(0);
    for c in &counters {
        c.reset();
    }

    let storm_secs = std::env::var("CHAOS_CREATE_STORM_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let deadline = std::time::Instant::now() + Duration::from_secs(storm_secs);
    let errors: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut rng = StdRng::seed_from_u64(seed);
    let started = std::time::Instant::now();
    let mut handles = Vec::new();
    for (idx, mnt) in [r1.mnt.clone(), r2.mnt.clone()].into_iter().enumerate() {
        for tid in 0..THREADS_PER_REQUESTER {
            let mnt = mnt.clone();
            let errors = errors.clone();
            let dir = dir.to_string();
            let mut wrng = StdRng::seed_from_u64(rng.random());
            handles.push(std::thread::spawn(move || -> u64 {
                let mut n = 0u64;
                while std::time::Instant::now() < deadline {
                    n += 1;
                    let path = mnt.join(&dir).join(format!("w{idx}-t{tid}-{n}"));
                    let len = wrng.random_range(16..256);
                    let mut content = format!("worker {idx} file {n} ").into_bytes();
                    content.resize(len, b'x');
                    if let Err(e) = std::fs::write(&path, &content) {
                        errors
                            .lock()
                            .unwrap()
                            .push(format!("worker {idx} write #{n} ({path:?}): {e}"));
                        continue;
                    }
                    match std::fs::read(&path) {
                        Ok(got) if got == content => {}
                        Ok(got) => errors.lock().unwrap().push(format!(
                            "worker {idx} read #{n}: content mismatch, got {} bytes want {}",
                            got.len(),
                            content.len()
                        )),
                        Err(e) => errors
                            .lock()
                            .unwrap()
                            .push(format!("worker {idx} read #{n} ({path:?}): {e}")),
                    }
                    if let Err(e) = std::fs::remove_file(&path) {
                        errors
                            .lock()
                            .unwrap()
                            .push(format!("worker {idx} unlink #{n} ({path:?}): {e}"));
                    }
                }
                n
            }));
        }
    }
    // A storm that stops making progress (records held back behind a
    // lost pending chunk stall every dependent op) fails here with the
    // held set, instead of hanging until the harness timeout.
    join_storm_workers(
        "inbox-create-storm-p2p-off",
        &handles,
        Duration::from_secs(storm_secs + 120),
        &[&holder, &r1, &r2],
    )?;
    let mut totals = Vec::new();
    for h in handles {
        totals.push(
            h.join()
                .map_err(|_| anyhow::anyhow!("inbox-create-storm-p2p-off: worker panicked"))?,
        );
    }
    let elapsed = started.elapsed();
    let errs = errors.lock().unwrap().clone();
    if !errs.is_empty() {
        for c in [&holder, &r1, &r2] {
            eprintln!("--- {} mount.log (tail) ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    anyhow::ensure!(
        errs.is_empty(),
        "inbox-create-storm-p2p-off: {} unexpected errno(s) (totals {totals:?}):\n{}",
        errs.len(),
        errs.join("\n")
    );

    let files: u64 = totals.iter().sum();
    let ops_per_s = (files * OPS_PER_FILE) as f64 / elapsed.as_secs_f64();
    let roles = ["holder", "r1", "r2"];
    for (counter, role) in counters.iter().zip(roles) {
        counter.ensure_sane()?;
        let requests = counter.requests();
        let t = crate::reqlog::tally(&requests);
        eprintln!(
            "    inbox-create-storm-p2p-off: {role:>6} {t} ({:.2} req/file)\n           by area: {}",
            t.total() as f64 / files.max(1) as f64,
            crate::reqlog::breakdown(&requests)
        );
    }
    eprintln!(
        "    inbox-create-storm-p2p-off: {files} files ({} ops) in {elapsed:?} = {ops_per_s:.1} \
         ops/s with {THREADS_PER_REQUESTER} threads per requester (ping-pong floor \
         {PING_PONG_FLOOR_OPS_PER_S} ops/s); per worker {totals:?}",
        files * OPS_PER_FILE
    );
    // The round-2 breakdown (plan 30 M13): where an inbox op's time goes.
    for r in [&r1, &r2] {
        let i = inbox_of(r)?;
        eprintln!(
            "    inbox-create-storm-p2p-off: {:>3} batches {} ops {} (avg {:.1}/batch, max {}) \
             queue {:.1}ms + outcome {:.1}ms = {:.1}ms round trip; unavailable {}",
            r.name,
            i["submitted_batches"],
            i["submitted_ops"],
            i["avg_batch_ops"].as_f64().unwrap_or(0.0),
            i["largest_batch_ops"],
            i["avg_queue_wait_ms"].as_f64().unwrap_or(0.0),
            i["avg_outcome_wait_ms"].as_f64().unwrap_or(0.0),
            i["avg_round_trip_ms"].as_f64().unwrap_or(0.0),
            i["unavailable"]
        );
    }
    let h = inbox_of(&holder)?;
    eprintln!(
        "    inbox-create-storm-p2p-off: holder polls {} hits {} executed {} refused {} deduped {} \
         pickup {:.1}ms execute {:.2}ms/hit",
        h["polls"],
        h["poll_hits"],
        h["executed_ops"],
        h["refused_ops"],
        h["deduped_ops"],
        h["avg_pickup_ms"].as_f64().unwrap_or(0.0),
        h["avg_execute_ms"].as_f64().unwrap_or(0.0)
    );

    // The hybrid's shape: escalations on the requesters, handoffs as the
    // epoch delta (each move bumps it), local vs inbox ops per node.
    let epoch_after = [&holder, &r1, &r2]
        .iter()
        .filter_map(|c| lease_of(c).ok()?["epoch"].as_u64())
        .max()
        .unwrap_or(epoch_before);
    let handoffs = epoch_after.saturating_sub(epoch_before);
    let mut escalations = 0u64;
    for c in [&holder, &r1, &r2] {
        let i = inbox_of(c)?;
        escalations += i["escalations"].as_u64().unwrap_or(0);
        eprintln!(
            "    inbox-create-storm-p2p-off: {:>6} escalations {} lease requests {} inbox ops {} \
             local ops {}",
            c.name, i["escalations"], i["lease_requests"], i["inbox_ops"], i["local_ops"]
        );
    }
    eprintln!(
        "    inbox-create-storm-p2p-off: {handoffs} lease handoff(s) (epoch {epoch_before} -> \
         {epoch_after}), {escalations} escalation(s)"
    );
    // Non-vacuity: the requesters submitted batches and the holder
    // executed ops from them (before any escalation moved the lease).
    let submitted = inbox_counter(&r1, "submitted_ops")? + inbox_counter(&r2, "submitted_ops")?;
    let executed: u64 = [&holder, &r1, &r2]
        .iter()
        .map(|c| inbox_counter(c, "executed_ops").unwrap_or(0))
        .sum();
    anyhow::ensure!(
        submitted > 0 && executed > 0,
        "no inbox traffic: submitted_ops={submitted} executed_ops={executed} (status.inbox: {} / {})",
        inbox_of(&r1)?,
        inbox_of(&holder)?
    );
    // Requester inbox PUTs, for the record (batches cannot amortize on a
    // VFS-serialized directory; this is not asserted).
    for (counter, r) in counters[1..].iter().zip([&r1, &r2]) {
        let puts = counter
            .requests()
            .iter()
            .filter(|req| req.method == "PUT" && req.area() == "inbox")
            .count() as u64;
        eprintln!(
            "    inbox-create-storm-p2p-off: {:>6} {puts} inbox PUTs for {} submitted ops",
            r.name,
            inbox_counter(r, "submitted_ops")?
        );
    }
    anyhow::ensure!(
        ops_per_s >= PING_PONG_FLOOR_OPS_PER_S,
        "inbox-create-storm-p2p-off: {ops_per_s:.1} ops/s is worse than lease ping-pong's \
         {PING_PONG_FLOOR_OPS_PER_S} ops/s floor"
    );

    // Convergence, as in create-storm-s3-only.
    let mut want = Vec::new();
    for (idx, mnt) in [&r1.mnt, &r2.mnt].into_iter().enumerate() {
        let name = format!("marker-{idx}");
        std::fs::write(mnt.join(dir).join(&name), format!("marker {idx}"))?;
        want.push(name);
    }
    want.sort();
    eventually("final listing converges", Duration::from_secs(60), || {
        for mnt in [&holder.mnt, &r1.mnt, &r2.mnt] {
            let mut names: Vec<String> = std::fs::read_dir(mnt.join(dir))?
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            anyhow::ensure!(names == want, "{}: {names:?} != {want:?}", mnt.display());
        }
        Ok(())
    })?;
    ensure_no_conflicts(&[&holder, &r1, &r2])?;
    ensure_no_lost_chunks(&[&holder, &r1, &r2])?;
    let (mut holder, mut r1, mut r2) = (holder, r1, r2);
    for c in [&mut holder, &mut r1, &mut r2] {
        let status = c.unmount_exit(Duration::from_secs(120))?;
        anyhow::ensure!(
            status.success(),
            "inbox-create-storm-p2p-off: {} exited {status} after the unmount: {}",
            c.name,
            c.tail_log_n(20)
        );
    }
    ensure_no_lost_chunks(&[&holder, &r1, &r2])?;
    Ok(())
}

/// The storm-hang regression, distilled: many threads on one node write
/// the *same* bytes to different files at the same moment (a barrier per
/// round), read them back and unlink them. Identical content is one chunk
/// hash, so every round races several inserts of one hash into the chunk
/// cache; the old cache published the entry before its file and a racing
/// reader (or uploader) forgot it, stranding a pending upload ("missing
/// from local cache", records held back, the storm wedged). Every round
/// must read back what it wrote, nothing may be held, no upload may report
/// a missing chunk, and the unmount must be clean.
fn dedup_write_storm(_seed: u64) -> Result<()> {
    const THREADS: usize = 16;
    let (env, root) = setup("dedup-write-storm")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/dedup-write-storm-{}", ts());
    let mut modes = Vec::new();
    let wanted = std::env::var("CHAOS_DEDUP_STORM_MODES").unwrap_or_else(|_| "through,back".into());
    for mode in wanted.split(',').filter(|m| !m.is_empty()) {
        let mut c = Client::new(
            root.path(),
            &format!("dedup-{mode}"),
            &env.endpoint,
            &backend,
        )?
        .with_own_node_key()
        .with_write_mode(mode)
        .with_env("CONSTELLATION_P2P", "off");
        // A tiny cache keeps eviction turning over the very hashes the
        // writers race on.
        if let Some(bytes) = std::env::var("CHAOS_DEDUP_STORM_CACHE")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            c = c.with_cache_size(bytes);
        }
        if modes.is_empty() {
            c.fs_create()?;
        }
        c.mount()?;
        let dir = c.mnt.join(format!("dedup-{mode}"));
        std::fs::create_dir_all(&dir)?;
        let secs = std::env::var("CHAOS_DEDUP_STORM_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15u64);
        let deadline = std::time::Instant::now() + Duration::from_secs(secs);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let errors: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let dir = dir.clone();
                let stop = stop.clone();
                let barrier = barrier.clone();
                let errors = errors.clone();
                std::thread::spawn(move || -> u64 {
                    let mut round = 0u64;
                    loop {
                        // Thread 0 decides for everyone, so no thread
                        // waits at a barrier the others have left.
                        if t == 0 && std::time::Instant::now() >= deadline {
                            stop.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                        barrier.wait();
                        if stop.load(std::sync::atomic::Ordering::SeqCst) {
                            return round;
                        }
                        round += 1;
                        let content = format!("dedup round {round} ").into_bytes();
                        let path = dir.join(format!("t{t}-{round}"));
                        let result = std::fs::write(&path, &content)
                            .map_err(|e| format!("write: {e}"))
                            .and_then(|()| std::fs::read(&path).map_err(|e| format!("read: {e}")))
                            .and_then(|got| {
                                if got == content {
                                    Ok(())
                                } else {
                                    Err(format!("read back {} bytes", got.len()))
                                }
                            })
                            .and_then(|()| {
                                std::fs::remove_file(&path).map_err(|e| format!("unlink: {e}"))
                            });
                        if let Err(e) = result {
                            errors
                                .lock()
                                .unwrap()
                                .push(format!("thread {t} round {round}: {e}"));
                        }
                        barrier.wait();
                    }
                })
            })
            .collect();
        join_storm_workers(
            "dedup-write-storm",
            &handles,
            Duration::from_secs(secs + 120),
            &[&c],
        )?;
        let rounds = handles
            .into_iter()
            .map(|h| h.join().unwrap_or(0))
            .max()
            .unwrap_or(0);
        let errs = errors.lock().unwrap().clone();
        anyhow::ensure!(
            errs.is_empty(),
            "dedup-write-storm ({mode}): {} error(s) in {rounds} rounds, first: {}\n{}",
            errs.len(),
            errs[0],
            c.tail_log_n(20)
        );
        anyhow::ensure!(
            rounds > 10,
            "dedup-write-storm ({mode}): only {rounds} rounds"
        );
        eprintln!(
            "    dedup-write-storm: write-mode {mode}: {rounds} rounds x {THREADS} same-content writers"
        );
        eventually("the journal drains", Duration::from_secs(60), || {
            journal_drained(&c)
        })?;
        ensure_no_lost_chunks(&[&c])?;
        let status = c.unmount_exit(Duration::from_secs(120))?;
        anyhow::ensure!(
            status.success(),
            "dedup-write-storm ({mode}): exited {status}: {}",
            c.tail_log_n(20)
        );
        ensure_no_lost_chunks(&[&c])?;
        modes.push(c);
    }
    Ok(())
}

/// Wait for a storm's worker threads, failing (with every node's held
/// set and log tail) if they are not done within `within`. The workers
/// are left blocked in their syscalls; dropping the clients kills the
/// daemons, which releases them.
fn join_storm_workers<T>(
    scenario: &str,
    handles: &[std::thread::JoinHandle<T>],
    within: Duration,
    nodes: &[&Client],
) -> Result<()> {
    let deadline = std::time::Instant::now() + within;
    while handles.iter().any(|h| !h.is_finished()) {
        if std::time::Instant::now() >= deadline {
            let mut report = String::new();
            for c in nodes {
                let held = c
                    .control_status()
                    .map(|s| s["held"].clone())
                    .unwrap_or_default();
                report.push_str(&format!(
                    "\n--- {} held {held}\n{}",
                    c.name,
                    c.tail_log_n(30)
                ));
            }
            anyhow::bail!(
                "{scenario}: {} of {} workers still blocked {within:?} after the start{report}",
                handles.iter().filter(|h| !h.is_finished()).count(),
                handles.len()
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

/// No node lost a pending chunk: nothing is held back and no upload
/// round ever reported a pending chunk missing from the local cache
/// (the storm-hang regression: a dedup race in the chunk cache forgot a
/// dirty entry whose file was still on disk). Works on unmounted nodes
/// too, from their logs alone.
fn ensure_no_lost_chunks(nodes: &[&Client]) -> Result<()> {
    for c in nodes {
        let log = c.log_text();
        let missing = log.matches("missing from local cache").count();
        let held_logs = log
            .lines()
            .filter(|l| l.contains("constellation_meta::store::held"))
            .count();
        anyhow::ensure!(
            missing == 0 && held_logs == 0,
            "{}: {missing} missing-pending-chunk warning(s), {held_logs} held-record error(s): {}",
            c.name,
            log.lines()
                .find(|l| l.contains("missing from local cache"))
                .unwrap_or("")
        );
        if let Ok(status) = c.control_status() {
            let held = &status["held"];
            anyhow::ensure!(
                held["transactions"].as_u64().unwrap_or(0) == 0
                    && held["inodes"].as_array().is_none_or(|i| i.is_empty()),
                "{}: records held back: {held}",
                c.name
            );
        }
    }
    Ok(())
}

/// The sporadic case the hybrid keeps on the inbox (plan 30 M13 round
/// 3b). With P2P off, `r1` writes one file every ~3 s for about a minute
/// while the holder stays put: no lease handoff (the epoch never moves),
/// no escalation on `r1`, every file visible on the holder, and the
/// per-write latency printed and bounded. The first write pays the
/// first-contact tax (the holder learns of `r1` from the 5 s registry
/// poll) and is printed separately; after it, `r1` is a *warm* requester
/// on the holder's poll schedule (`CONSTELLATION_INBOX_IDLE_MAX_MS`,
/// 2 s), so a write waits at most that plus a ship and a hot tail: the
/// bound is p50 within the warm ceiling and p99 within the warm ceiling
/// plus a second. Today's path for the same write registers `wanted_by`
/// and waits for the holder's next lease round (up to TTL/4, 5 s here),
/// moves the lease, and moves it back on the holder's next write.
fn inbox_sporadic_write_p2p_off(seed: u64) -> Result<()> {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    const WRITES: usize = 16;
    const WARM_CEILING: Duration = Duration::from_millis(2_000);
    let (_env, _root, _prefix, holder, r1, r2, _counters) =
        inbox_cluster("inbox-sporadic-write-p2p-off", 20_000)?;
    let epoch_before = lease_of(&holder)?["epoch"].as_u64().unwrap_or(0);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut latencies: Vec<Duration> = Vec::with_capacity(WRITES);
    for i in 0..WRITES {
        let name = format!("sporadic-{i}");
        let started = std::time::Instant::now();
        create_new(&r1.mnt, &name).with_context(|| format!("r1's write {i}"))?;
        latencies.push(started.elapsed());
        std::thread::sleep(Duration::from_millis(rng.random_range(2_500..3_500)));
    }
    let first = latencies[0];
    let mut steady: Vec<Duration> = latencies[1..].to_vec();
    steady.sort();
    let p50 = steady[steady.len() / 2];
    let p99 = steady[(steady.len() * 99 / 100).min(steady.len() - 1)];
    eprintln!(
        "    inbox-sporadic-write-p2p-off: {WRITES} writes, first {first:?} (first contact), then \
         p50 {p50:?} p99 {p99:?} max {:?}; r1 {}",
        steady.last().copied().unwrap_or_default(),
        inbox_of(&r1)?
    );

    eventually(
        "every sporadic write is on the holder",
        Duration::from_secs(30),
        || {
            for i in 0..WRITES {
                anyhow::ensure!(
                    holder.mnt.join(format!("sporadic-{i}")).is_file(),
                    "sporadic-{i} missing on the holder"
                );
            }
            Ok(())
        },
    )?;
    let epoch_after = lease_of(&holder)?["epoch"].as_u64().unwrap_or(0);
    anyhow::ensure!(
        lease_of(&holder)?["held"] == true && epoch_after == epoch_before,
        "the lease moved for sporadic writes (epoch {epoch_before} -> {epoch_after})"
    );
    anyhow::ensure!(
        inbox_counter(&r1, "escalations")? == 0 && inbox_counter(&r1, "lease_requests")? == 0,
        "a sporadic writer escalated: {}",
        inbox_of(&r1)?
    );
    anyhow::ensure!(
        inbox_counter(&r1, "inbox_ops")? >= WRITES as u64,
        "not every write went through the inbox: {}",
        inbox_of(&r1)?
    );
    anyhow::ensure!(
        p50 <= WARM_CEILING,
        "p50 {p50:?} exceeds the warm poll ceiling {WARM_CEILING:?}"
    );
    anyhow::ensure!(
        p99 <= WARM_CEILING + Duration::from_secs(1),
        "p99 {p99:?} exceeds the warm poll ceiling plus a second"
    );
    ensure_no_conflicts(&[&holder, &r1, &r2])?;
    let (mut holder, mut r1, mut r2) = (holder, r1, r2);
    holder.unmount()?;
    r1.unmount()?;
    r2.unmount()?;
    Ok(())
}

/// A requester that dies between submitting a batch and learning its
/// outcome. The holder's S3 is cut so it cannot poll; `r1` submits (its
/// FUSE thread blocks on the outcome), the batch is observed in the
/// bucket, `r1` is killed. The holder heals, polls, and executes the
/// batch — exactly once, though nobody is waiting for it. `r1` remounts
/// under a new incarnation, resumes its numbering past the batch the
/// holder kept as its high-water mark, and its next write is polled and
/// executed, so nothing it submits is ever stranded behind a stale
/// cursor.
fn inbox_requester_crash_mid_batch(_seed: u64) -> Result<()> {
    let (env, _root, prefix, holder, r1, r2, counters) =
        inbox_cluster("inbox-requester-crash-mid-batch", 20_000)?;
    let (mut holder, mut r1, mut r2) = (holder, r1, r2);
    let holder_s3 = &counters[0];

    // The holder cannot read its inbox (or renew) while cut; the TTL is
    // long enough that nobody takes over meanwhile.
    holder_s3.cut();
    let mnt = r1.mnt.clone();
    let writer = std::thread::spawn(move || create_new(&mnt, "orphan"));
    eventually(
        "r1's batch reaches the bucket",
        Duration::from_secs(20),
        || {
            let objects = inbox_objects(&env, &prefix)?;
            anyhow::ensure!(!objects.is_empty(), "no inbox object yet");
            Ok(())
        },
    )?;
    let batches_before = inbox_objects(&env, &prefix)?;
    r1.kill9()?;
    // The FUSE call died with its mount; whatever it returned is moot.
    let _ = writer.join();
    holder_s3.heal();

    // The batch executes without its requester: the name appears on the
    // holder and on r2, once.
    eventually(
        "the orphaned batch executes",
        Duration::from_secs(30),
        || {
            let h = ino_of(&holder.mnt.join("orphan")).context("orphan missing on the holder")?;
            let o = ino_of(&r2.mnt.join("orphan")).context("orphan missing on r2")?;
            anyhow::ensure!(
                h == o,
                "holder and r2 disagree on orphan's inode: {h} vs {o}"
            );
            Ok(())
        },
    )?;
    anyhow::ensure!(
        inbox_counter(&holder, "executed_ops")? >= 1,
        "the holder did not count an inbox execution: {}",
        inbox_of(&holder)?
    );

    // Remount r1: a new incarnation, numbering resumed by LIST-last, so
    // its next batch lands where the holder's cursor is.
    r1.mount()?;
    eventually(
        "r1 sees its orphaned create",
        Duration::from_secs(30),
        || {
            anyhow::ensure!(r1.mnt.join("orphan").is_file(), "orphan missing on r1");
            Ok(())
        },
    )?;
    let ino_r1 = ino_of(&r1.mnt.join("orphan")).context("orphan on r1")?;
    let ino_h = ino_of(&holder.mnt.join("orphan")).context("orphan on the holder")?;
    anyhow::ensure!(
        ino_r1 == ino_h,
        "one inode everywhere: r1 {ino_r1} vs holder {ino_h}"
    );
    create_new(&r1.mnt, "after-restart").context("r1's first write after the restart")?;
    eventually(
        "r1's post-restart write executes",
        Duration::from_secs(30),
        || {
            anyhow::ensure!(
                holder.mnt.join("after-restart").is_file(),
                "not on the holder yet"
            );
            anyhow::ensure!(r2.mnt.join("after-restart").is_file(), "not on r2 yet");
            Ok(())
        },
    )?;
    let batches_after = inbox_objects(&env, &prefix)?;
    eprintln!(
        "    inbox-requester-crash-mid-batch: batches before the kill {batches_before:?}, \
         after the restart {batches_after:?}; r1 next_n={}",
        inbox_counter(&r1, "next_n")?
    );
    anyhow::ensure!(
        inbox_counter(&r1, "next_n")? >= 2,
        "r1 restarted its numbering from zero instead of resuming past its orphaned batch: {}",
        inbox_of(&r1)?
    );
    ensure_no_conflicts(&[&holder, &r1, &r2])?;
    holder.unmount()?;
    r1.unmount()?;
    r2.unmount()?;
    Ok(())
}

/// The holder dies with an unread batch. The holder's S3 is cut (it
/// can neither poll nor renew), `r1` submits a create and blocks on the
/// outcome, the batch is observed in the bucket, the holder is killed.
/// `r2` writes, which makes it want the lease once the TTL runs out.
/// Whoever wins the takeover — `r2` for its own write, or `r1` itself
/// once its inbox wait finds the register claimable and takes the lease
/// path — drains the old epoch's inbox inside its takeover gate before
/// serving, so `r1`'s create returns success (not `EIO`) exactly once,
/// and the surviving nodes agree on one inode. Non-vacuity: some node's
/// `drained_batches` rose, and the epoch-1 batch is gone from the bucket
/// afterwards.
fn inbox_holder_takeover_pending_batch(_seed: u64) -> Result<()> {
    const TTL_MS: u64 = 6_000;
    let (env, _root, prefix, holder, r1, r2, counters) =
        inbox_cluster("inbox-holder-takeover-pending-batch", TTL_MS)?;
    let (mut holder, mut r1, mut r2) = (holder, r1, r2);
    let holder_s3 = &counters[0];

    holder_s3.cut();
    let mnt = r1.mnt.clone();
    let writer = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        (create_new(&mnt, "pending"), started.elapsed())
    });
    eventually(
        "r1's batch reaches the bucket",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(
                !inbox_objects(&env, &prefix)?.is_empty(),
                "no inbox object yet"
            );
            Ok(())
        },
    )?;
    holder.kill9()?;

    // r2's own write forces a takeover once the dead holder's lease
    // expires (r1's pending inbox wait may win it instead); the winner's
    // gate drains r1's batch first.
    std::fs::write(r2.mnt.join("after"), b"r2").context("r2's post-crash write")?;
    eventually(
        "someone took the lease over",
        Duration::from_secs(30),
        || {
            let (l1, l2) = (lease_of(&r1)?, lease_of(&r2)?);
            anyhow::ensure!(
                l1["held"] == true || l2["held"] == true,
                "nobody holds yet: r1 {l1} r2 {l2}"
            );
            Ok(())
        },
    )?;
    let (result, waited) = writer
        .join()
        .map_err(|_| anyhow::anyhow!("r1's writer thread panicked"))?;
    result.with_context(|| {
        format!("r1's create must succeed through the new holder's drain (waited {waited:?})")
    })?;
    eprintln!("    inbox-holder-takeover-pending-batch: r1's create returned after {waited:?}");
    eventually(
        "pending visible everywhere as one inode",
        Duration::from_secs(30),
        || {
            let a = ino_of(&r1.mnt.join("pending")).context("pending missing on r1")?;
            let b = ino_of(&r2.mnt.join("pending")).context("pending missing on r2")?;
            anyhow::ensure!(a == b, "r1 and r2 disagree on pending's inode: {a} vs {b}");
            anyhow::ensure!(r1.mnt.join("after").is_file(), "r2's write not on r1 yet");
            Ok(())
        },
    )?;
    let drained = inbox_counter(&r1, "drained_batches")? + inbox_counter(&r2, "drained_batches")?;
    anyhow::ensure!(
        drained >= 1,
        "no takeover gate drained the old epoch's inbox: r1 {} r2 {}",
        inbox_of(&r1)?,
        inbox_of(&r2)?
    );
    // The stale batch is gone once its outcome shipped.
    eventually(
        "old-epoch batches are GC'd",
        Duration::from_secs(30),
        || {
            let left: Vec<String> = inbox_objects(&env, &prefix)?
                .into_iter()
                .filter(|k| k.contains("/inbox/0000000000000001/"))
                .collect();
            anyhow::ensure!(left.is_empty(), "epoch-1 batches still present: {left:?}");
            Ok(())
        },
    )?;
    ensure_no_conflicts(&[&r1, &r2])?;
    let _ = holder.unmount();
    r1.unmount()?;
    r2.unmount()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_convergence_failure_names_its_path() {
        let err = "convergence not reached within 60s after write_full_duel:wf293: \
                   chaos-soak/wf293: [(0, \"hash:1beb3151\"), (1, \"hash:a2764f1b\")]";
        assert_eq!(
            super::diverged_path(err).as_deref(),
            Some("chaos-soak/wf293")
        );
        let err = "x: convergence not reached within 60s after write_disjoint:wd41: \
                   read_at:chaos-soak/wd41@32+32: [(0, \"hash:aa\")]";
        assert_eq!(
            super::diverged_path(err).as_deref(),
            Some("chaos-soak/wd41")
        );
        assert_eq!(super::diverged_path("something else"), None);
    }
}
