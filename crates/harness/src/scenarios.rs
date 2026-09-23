//! Fault-injection scenarios. Each runs a fresh environment: floci S3
//! behind toxiproxy, one or more constellation clients on the host,
//! seeded workloads verified against the model oracle.

use crate::client::Client;
use crate::model::Model;
use crate::reqlog::CountingProxy;
use crate::s3env::{S3Env, BUCKET};
use crate::suites;
use crate::workload::Workload;
use anyhow::{bail, Context, Result};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod coop_churn;

pub struct Scenario {
    pub name: &'static str,
    pub desc: &'static str,
    /// Host binaries the scenario needs; missing ones cause a loud skip.
    pub requires: &'static [&'static str],
    pub run: fn(seed: u64) -> Result<()>,
}

pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "baseline",
        desc: "workload with a healthy network; model verified per block",
        requires: &[],
        run: baseline,
    },
    Scenario {
        name: "latency",
        desc: "workload under 150ms +/-50ms S3 latency",
        requires: &[],
        run: latency,
    },
    Scenario {
        name: "slow-network",
        desc: "workload under 256 KB/s S3 bandwidth + sliced packets",
        requires: &[],
        run: slow_network,
    },
    Scenario {
        name: "s3-outage",
        desc: "cut S3 mid-workload: cached reads keep working, writes recover after heal",
        requires: &[],
        run: s3_outage,
    },
    Scenario {
        name: "s3-flap",
        desc: "S3 connection cut/heal every block; workload must stay correct",
        requires: &[],
        run: s3_flap,
    },
    Scenario {
        name: "kill9-remount",
        desc: "SIGKILL the daemon between blocks; remount must recover all committed state",
        requires: &[],
        run: kill9_remount,
    },
    Scenario {
        name: "cold-cache",
        desc: "wipe the chunk cache between blocks; reads must re-fetch from S3",
        requires: &[],
        run: cold_cache,
    },
    Scenario {
        name: "two-clients-disjoint",
        desc: "two clients, one bucket: disjoint namespaces must not corrupt each other (phase-1 scope)",
        requires: &[],
        run: two_clients_disjoint,
    },
    Scenario {
        name: "two-clients-shared",
        desc: "two clients, ONE filesystem: writes on each propagate to the other (close-to-open)",
        requires: &[],
        run: two_clients_shared,
    },
    Scenario {
        name: "atime-eventual",
        desc: "read-time atime (plan 20): a read on one node eventually bumps atime on the other; under an S3 cut reads keep succeeding and atime is simply lost",
        requires: &[],
        run: atime_eventual,
    },
    Scenario {
        name: "quota-enforcement",
        desc: "live quota set blocks growth with ENOSPC; clearing resumes; replicates to a second node",
        requires: &[],
        run: quota_enforcement,
    },
    Scenario {
        name: "prune",
        desc: "retention prune (plan 22): an armed age policy removes stale files on one node and both replicas converge; dry-run deletes nothing",
        requires: &[],
        run: prune_retention,
    },
    Scenario {
        name: "git-workflow",
        desc: "stage/publish/edit ping-pong between two nodes of one filesystem",
        requires: &[],
        run: git_workflow,
    },
    Scenario {
        name: "lease-handover",
        desc: "A writes then goes idle; B must take the partition lease within seconds and write too",
        requires: &[],
        run: lease_handover,
    },
    Scenario {
        name: "lease-fencing",
        desc: "freeze the lease holder (SIGSTOP), let B take over after expiry, then resume A: A must detect deposition, never ship under its old epoch, and replay its stranded write through B exactly once",
        requires: &[],
        run: lease_fencing,
    },
    Scenario {
        name: "continuation-epoch",
        desc: "cut S3 with all writers on P2P: both keep writing, then flush and converge after heal",
        requires: &[],
        run: continuation_epoch,
    },
    Scenario {
        name: "epoch-member-lost",
        desc: "pause one continuation-epoch member: survivors immediately freeze writes with EROFS",
        requires: &[],
        run: epoch_member_lost,
    },
    Scenario {
        name: "deposed-reintegration",
        desc: "plan 30 M3b: a deposed holder recovers automatically; its non-overlapping stranded edits replay cleanly and only the true edit-vs-edit overlap materializes a conflict copy",
        requires: &[],
        run: deposed_reintegration,
    },
    Scenario {
        name: "node-leave",
        desc: "three writers; C leaves; A+B can open a continuation epoch under S3 cut (unmount alone cannot)",
        requires: &[],
        run: node_leave,
    },
    Scenario {
        name: "p2p-invalidation",
        desc: "gossip push makes a write visible on the peer far faster than the S3 poll bound; P2P=off restores the old bound",
        requires: &[],
        run: p2p_invalidation,
    },
    Scenario {
        name: "p2p-handover",
        desc: "a blocked writer takes the lease from an ACTIVE holder in ~1 RTT instead of waiting out the idle window",
        requires: &[],
        run: p2p_handover,
    },
    Scenario {
        name: "forwarded-mutations",
        desc: "non-holder writes forward to the sticky lease holder; lease epoch does not thrash",
        requires: &[],
        run: forwarded_mutations,
    },
    Scenario {
        name: "scratch-publish",
        desc: "scratch dir contents are local; publish rename makes the file cluster-visible",
        requires: &[],
        run: scratch_publish,
    },
    Scenario {
        name: "p2p-partition-tolerance",
        desc: "with P2P disabled on one node, shared-filesystem correctness still holds on the S3 slow path",
        requires: &[],
        run: p2p_partition_tolerance,
    },
    Scenario {
        name: "coop-cache-hit",
        desc: "cold reader fetches most chunks from a warm peer while S3 is delayed 200ms",
        requires: &[],
        run: coop_cache_hit,
    },
    Scenario {
        name: "s3-retry",
        desc: "cold read survives a first-attempt S3 cut via S3-leg-only retries",
        requires: &[],
        run: s3_retry,
    },
    Scenario {
        name: "coop-fallback",
        desc: "paused warm peer: reader still completes from S3 with hash-verified content",
        requires: &[],
        run: coop_fallback,
    },
    Scenario {
        name: "web-fleet",
        desc: "one writer, two cold readers: aggregate S3 GETs stay near the unique-chunk count",
        requires: &[],
        run: web_fleet,
    },
    Scenario {
        name: "coop-exact-churn",
        desc: "3 small-cache nodes add and evict chunks while reading each other: zero false-positive peer fetches, exact mirrors",
        requires: &[],
        run: coop_churn::coop_exact_churn,
    },
    Scenario {
        name: "coop-digest-compare",
        desc: "same churn in bloom and exact digest mode: prints digest bytes/s, false positives, CPU per round",
        requires: &[],
        run: coop_churn::coop_digest_compare,
    },
    Scenario {
        name: "web-ui-smoke",
        desc: "localhost HTTP control adapter, embedded UI, and Prometheus metrics",
        requires: &[],
        run: web_ui_smoke,
    },
    Scenario {
        name: "gc-lifecycle",
        desc: "reference GC removes dead chunks while preserving snapshot and live roots",
        requires: &[],
        run: gc_lifecycle,
    },
    Scenario {
        name: "gc-dedup-race",
        desc: "a writer reuses condemned content during the TTL wait without creating a dangle",
        requires: &[],
        run: gc_dedup_race,
    },
    Scenario {
        name: "fsck-repair",
        desc: "fsck detects and repairs a missing chunk, orphan, and torn segment",
        requires: &[],
        run: fsck_repair,
    },
    Scenario {
        name: "fsck-while-mounted",
        desc: "fsck routes through the running daemon's control socket instead of racing the fjall lock",
        requires: &[],
        run: fsck_while_mounted,
    },
    Scenario {
        name: "snapshot-lifecycle",
        desc: "snapshot remains frozen behind hidden .constellation view, then becomes stale",
        requires: &[],
        run: snapshot_lifecycle,
    },
    Scenario {
        name: "clone-workflow",
        desc: "eager metadata clone diverges without changing its source snapshot",
        requires: &[],
        run: clone_workflow,
    },
    Scenario {
        name: "snapshot-mount",
        desc: "snapshot subtree mounts read-only and ephemeral rw clone is removed",
        requires: &[],
        run: snapshot_mount,
    },
    Scenario {
        name: "snapshot-churn",
        desc: "concurrent multi-round snapshot and clone churn with a SQLite oracle and replay",
        requires: &[],
        run: crate::snapchurn::run,
    },
    Scenario {
        name: "e2e-basic",
        desc: "passphrase mount encrypts chunks and logs, cold-remounts, and rejects a wrong passphrase",
        requires: &[],
        run: e2e_basic,
    },
    Scenario {
        name: "e2e-two-nodes",
        desc: "two passphrase nodes converge and use cooperative cache without exposing plaintext",
        requires: &[],
        run: e2e_two_nodes,
    },
    Scenario {
        name: "passwd-live-cluster",
        desc: "fs passwd on a live E2E cluster: both nodes keep converging (incl. across a split) with no remount",
        requires: &[],
        run: passwd_live_cluster,
    },
    Scenario {
        name: "fresh-node-bootstrap",
        desc: "node B rebuilds the namespace purely from S3 (plan 28 commit + log replay) and must match the model",
        requires: &[],
        run: fresh_node_bootstrap,
    },
    Scenario {
        name: "commit-strips-pending-upload",
        desc: "a mid-write-back metadata commit must not give a fresh joiner the writer's pending_upload backlog",
        requires: &[],
        run: commit_strips_pending_upload,
    },
    Scenario {
        name: "readahead",
        desc: "cold sequential read of a multi-chunk file under S3 latency must pipeline (prefetcher)",
        requires: &[],
        run: readahead,
    },
    Scenario {
        name: "readahead-adaptive",
        desc: "128-chunk cold sequential read under 200ms S3 latency expands the adaptive window",
        requires: &[],
        run: readahead_adaptive,
    },
    Scenario {
        name: "e2e-spilled-manifest",
        desc: "cold read of an E2E file whose manifest spilled its chunk list",
        requires: &[],
        run: e2e_spilled_manifest_cold_read,
    },
    Scenario {
        name: "e2e-decode-priority",
        desc: "a demand read stays fast while a concurrent bulk E2E download saturates the decode gate",
        requires: &[],
        run: e2e_decode_priority,
    },
    Scenario {
        name: "scan-ahead",
        desc: "ordered cold reads of many small files pipeline across file boundaries",
        requires: &[],
        run: scan_ahead,
    },
    Scenario {
        name: "distant-bigfile-stable",
        desc: "sequential cold download of a big file over a 200ms 'distant S3' path stays fast and doesn't swing wildly",
        requires: &[],
        run: distant_bigfile_stable,
    },
    Scenario {
        name: "distant-bigfile-stable-e2e",
        desc: "distant-bigfile-stable, but through an E2E-encrypted filesystem",
        requires: &[],
        run: distant_bigfile_stable_e2e,
    },
    Scenario {
        name: "prefetch-abandon",
        desc: "a reader that stops mid-file must not keep pulling the rest over the network into cache",
        requires: &[],
        run: prefetch_abandon,
    },
    Scenario {
        name: "prefetch-abandon-e2e",
        desc: "prefetch-abandon, but through an E2E-encrypted filesystem",
        requires: &[],
        run: prefetch_abandon_e2e,
    },
    Scenario {
        name: "prefetch-fairness",
        desc: "a big-file prefetch saturating a capped link must not stall concurrent small-file reads",
        requires: &[],
        run: prefetch_fairness,
    },
    Scenario {
        name: "fio-latency",
        desc: "fio randwrite + crc32c verify under 80ms S3 latency",
        requires: &["fio"],
        run: fio_latency,
    },
    Scenario {
        name: "fio-blips",
        desc: "fio verify while S3 blips on/off: retries must absorb transient cuts",
        requires: &["fio"],
        run: fio_blips,
    },
    Scenario {
        name: "stress-ng-flap",
        desc: "stress-ng metadata churn while S3 flaps; mount healthy + spool drains",
        requires: &["stress-ng"],
        run: stress_ng_flap,
    },
    Scenario {
        name: "big-file-write",
        desc: "write a file several times --cache-size and sample RSS: must stay flat, not track bytes written (plan 07)",
        requires: &[],
        run: big_file_write,
    },
    Scenario {
        name: "staging-crash",
        desc: "kill -9 mid-write, remount: staging/ is empty after GC and the file is at its last closed size",
        requires: &[],
        run: staging_crash,
    },
    Scenario {
        name: "unmount-drain",
        desc: "fail the eager upload, then unmount cleanly: a second node must read the file with no missing chunk",
        requires: &[],
        run: unmount_drain,
    },
    Scenario {
        name: "writeback-latency",
        desc: "150ms S3 latency: write-back small-file closes beat write-through by at least 3x",
        requires: &[],
        run: writeback_latency,
    },
    Scenario {
        name: "writeback-bigfile",
        desc: "write-back streams a file ten times cache budget within RSS and cache ceilings",
        requires: &[],
        run: writeback_bigfile,
    },
    Scenario {
        name: "writeback-drain",
        desc: "back-to-through switch drains pending uploads before returning",
        requires: &[],
        run: writeback_drain,
    },
    Scenario {
        name: "writeback-fsync",
        desc: "fsync under write-back reaches S3 before kill -9",
        requires: &[],
        run: writeback_fsync,
    },
    Scenario {
        name: "writeback-backpressure",
        desc: "S3 cut throttles then returns ENOSPC at the dirty hard limit and recovers",
        requires: &[],
        run: writeback_backpressure,
    },
    Scenario {
        name: "existence-bloom-dedup",
        desc: "fresh node hints from its replica's chunk_ref and deduplicates without a bucket LIST",
        requires: &[],
        run: existence_bloom_dedup,
    },
    Scenario {
        name: "existence-peer-hint",
        desc: "uploader uses live peer cache digests as confirming probe hints ahead of the replica",
        requires: &[],
        run: existence_peer_hint,
    },
    Scenario {
        name: "xattr-roundtrip",
        desc: "file and directory xattrs replicate; removal and logical recursive size are correct",
        requires: &[],
        run: xattr_roundtrip,
    },
    Scenario {
        name: "fallocate-sparse",
        desc: "large sparse extend, hole punch, SEEK_HOLE/DATA, rewrite, and fresh-node verification",
        requires: &[],
        run: fallocate_sparse,
    },
    Scenario {
        name: "chaos-ci",
        desc: "same-path conflict races across 3 local mounts (constellation-chaos Ci profile)",
        requires: &[],
        run: chaos_ci,
    },
    Scenario {
        name: "chaos-soak-4",
        desc: "4 local mounts, soak profile (repro fleet write_disjoint; write-back + fsync s3)",
        requires: &[],
        run: chaos_soak_4,
    },
    Scenario {
        name: "disjoint-write-4",
        desc: "4 local mounts, data-only soak schedule (hits write_disjoint hard)",
        requires: &[],
        run: disjoint_write_4,
    },
    Scenario {
        name: "mkdir-p-race",
        desc: "4 nodes race `mkdir -p` of the same fresh tree: a refusal the holder based on state this node lacks must not surface as ENOENT",
        requires: &[],
        run: mkdir_p_race,
    },
    Scenario {
        name: "create-storm-s3-only",
        desc: "3-way create/write/unlink storm in one shared dir, P2P off: a healthy but contended holder must never starve a waiter into EIO",
        requires: &[],
        run: create_storm_s3_only,
    },
    Scenario {
        name: "mtree-gc-plateau",
        desc: "plan 28 S7b: rewrite rounds with a metadata commit and a GC round each; the metadata pack footprint must plateau, and a fresh node still bootstraps",
        requires: &[],
        run: mtree_gc_plateau,
    },
    Scenario {
        name: "idle-cluster-is-quiet",
        desc: "plan 26: 3 idle nodes issue no LIST of log/ and stay within the lease/heartbeat/probe budget",
        requires: &[],
        run: idle_cluster_is_quiet,
    },
    Scenario {
        name: "wan-writer-ships-put-only",
        desc: "plan 26: over a 200ms path the holder never lists its own stream, and a P2P-less follower converges within the idle ceiling",
        requires: &[],
        run: wan_writer_ships_put_only,
    },
    Scenario {
        name: "sticky-lease-handoff-over-s3",
        desc: "plan 26: with P2P off, a blocked writer registers wanted_by and takes the lease cooperatively instead of EIO",
        requires: &[],
        run: sticky_lease_handoff_over_s3,
    },
    Scenario {
        name: "named-shared-daemon",
        desc: "plan 21: mount NAME then NAME:/sub from a second CLI call share one daemon/node_id; umount tears down views one at a time, then the process",
        requires: &[],
        run: named_shared_daemon,
    },
    Scenario {
        name: "forward-timeout-reexec",
        desc: "plan 30 M2 (fixes bug A): a forwarded mutation's reply races the requester's forward timeout; exactly-once forwarding resolves it via dedup or the completed table instead of re-executing",
        requires: &[],
        run: forward_timeout_reexec,
    },
    Scenario {
        name: "holder-ships-under-forward-load",
        desc: "plan 30 M2b: a sustained forwarded-create burst from 2 non-holders must not starve the holder's own ship rounds; journal backlog stays bounded and followers converge within 2s of the burst ending",
        requires: &[],
        run: holder_ships_under_forward_load,
    },
    Scenario {
        name: "holder-crash-phantom-shadow",
        desc: "plan 30 M3a (fixes bug B): a holder stranded (S3 cut, then killed) after acking a forwarded create; a third node takes over, the requester's shadow is rolled back and replayed by rid, and b, c and a fresh d agree the create exists",
        requires: &[],
        run: holder_crash_phantom_shadow,
    },
    Scenario {
        name: "holder-crash-phantom-new-holder",
        desc: "plan 30 M3a (fixes bug B): the requester of a stranded forwarded create becomes the next holder; its takeover gate replays the create before serving, so another node's O_EXCL create of the name gets EEXIST and b, c and a fresh d agree",
        requires: &[],
        run: holder_crash_phantom_new_holder,
    },
    Scenario {
        name: "takeover-marker-strands-promptly",
        desc: "plan 30 M3b: a takeover whose own op is refused still strands a third node's shadow promptly (the new holder's epoch marker); the stranded create then replays exactly once",
        requires: &[],
        run: takeover_marker_strands_promptly,
    },
    Scenario {
        name: "holder-publishes-log-prefix",
        desc: "plan 30 M3b: a holder publishes while its journal is non-empty; every commit equals the log prefix, so after a mid-burst kill a fresh node matches the log-tailing follower exactly",
        requires: &[],
        run: holder_publishes_log_prefix,
    },
];

/// Plan 30 M0: scenarios that reproduce a known, not-yet-fixed bug
/// (`docs/plans/v1/wip/30-write-path-resilience-and-scale-out.md` §1.1).
/// Kept out of [`SCENARIOS`] so a bare `harness run` (no names) never
/// treats a documented bug as a regression: these are expected to FAIL
/// until the milestone that fixes the underlying bug moves them into
/// `SCENARIOS`, unchanged, as its regression test. `harness list` prints
/// them under their own heading; `harness run <name>` resolves a name in
/// either list.
pub const KNOWN_BUG_REPROS: &[Scenario] = &[];

fn setup(name: &str) -> Result<(S3Env, tempfile::TempDir)> {
    let env = S3Env::start().context("starting S3 environment")?;
    let mut root = tempfile::Builder::new()
        .prefix(&format!("harness-{name}-"))
        .tempdir()?;
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
    let response = ureq::get(&format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}"))
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
            ureq::get(&format!("{endpoint}/{BUCKET}/{key}"))
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
/// propagation is asynchronous: sync interval + FUSE TTLs).
fn eventually(what: &str, deadline: Duration, mut f: impl FnMut() -> Result<()>) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
        match f() {
            Ok(()) => return Ok(()),
            Err(e) if start.elapsed() > deadline => {
                return Err(e.context(format!("'{what}' not reached within {deadline:?}")))
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
    ureq::head(&raw_key(endpoint, key)).call().is_ok()
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
    let listing = ureq::get(&format!(
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
    ureq::delete(&raw_key(&env.direct_endpoint, &data_key)).call()?;
    let orphan_hash = blake3::hash(b"orphan-name").to_hex().to_string();
    let orphan_key = format!(
        "{prefix}/chunks/{}/{}/{}",
        &orphan_hash[..2],
        &orphan_hash[2..4],
        orphan_hash
    );
    ureq::put(&raw_key(&env.direct_endpoint, &orphan_key)).send_bytes(b"orphan-object")?;
    let torn = format!("{prefix}/log/p0/fffffffffffffffe.zst");
    ureq::put(&raw_key(&env.direct_endpoint, &torn)).send_bytes(b"torn")?;

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
    let mut client = one_client(&env, root.path(), &format!("snap-life-{}", ts()))?;
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
    client.unmount()
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
        error.raw_os_error() == Some(libc::EROFS),
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
        err.raw_os_error() == Some(libc::ENOSPC),
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
    let mut c0 = slow(Client::new(root.path(), "c0", &env.endpoint, &backend)?).with_env(
        "CONSTELLATION_NODE_KEY",
        "/tmp/.constellation-harness-c0.key",
    );
    let mut c1 = slow(Client::new(root.path(), "c1", &env.endpoint, &backend)?).with_env(
        "CONSTELLATION_NODE_KEY",
        "/tmp/.constellation-harness-c1.key",
    );
    // Distinct host keys: the node key is per host, and both "hosts"
    // here share one machine.
    let _ = std::fs::remove_file("/tmp/.constellation-harness-c0.key");
    let _ = std::fs::remove_file("/tmp/.constellation-harness-c1.key");
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
                let n = p["peers"].as_array().map(|a| a.len()).unwrap_or(0);
                anyhow::ensure!(n >= need, "{} sees {n} peers, want {need}: {p}", c.name);
                Ok(())
            },
        )?;
    }
    Ok(())
}

/// As [`wait_for_peers`], for the common 2-node call shape; also usable
/// directly with a slice for 3+ nodes (plan 30 M0).
fn wait_for_p2p(clients: &[&Client]) -> Result<()> {
    wait_for_peers(clients)
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
    let key = format!("/tmp/.constellation-coop-{name}.key");
    let _ = std::fs::remove_file(&key);
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
    ureq::get(&url)
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
    anyhow::ensure!(status["resp"] == "status", "bad HTTP status: {status}");
    let round_trip: serde_json::Value = serde_json::from_str(&serde_json::to_string(&status)?)?;
    anyhow::ensure!(round_trip == status, "HTTP status serde round-trip changed");

    let request = |body: serde_json::Value| -> Result<serde_json::Value> {
        Ok(ureq::post(&format!("{base}/api"))
            .timeout(Duration::from_secs(30))
            .send_json(body)?
            .into_json()?)
    };
    let directory = request(serde_json::json!({"cmd":"read_dir","path":"/"}))?;
    anyhow::ensure!(
        directory["entries"]
            .as_array()
            .is_some_and(|entries| entries.iter().any(|e| e["name"] == "through-http")),
        "HTTP ReadDir omitted created directory: {directory}"
    );
    let created = request(serde_json::json!({
        "cmd":"snapshot_create",
        "selector":"/@web-ui-smoke"
    }))?;
    anyhow::ensure!(created["resp"] == "ok", "snapshot create failed: {created}");
    let listed = request(serde_json::json!({"cmd":"list_snapshots","path":null}))?;
    anyhow::ensure!(
        listed["snapshots"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["name"] == "web-ui-smoke")),
        "snapshot not listed through HTTP: {listed}"
    );
    let deleted = request(serde_json::json!({
        "cmd":"snapshot_delete",
        "selector":"/@web-ui-smoke"
    }))?;
    anyhow::ensure!(deleted["resp"] == "ok", "snapshot delete failed: {deleted}");

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
/// §4): A writes, goes write-idle and cooperatively releases the lease;
/// B must take it within a couple of seconds — no 60 s TTL wait — and
/// write its own files. Both nodes then see both sets, the epoch has
/// advanced across the handover, and neither node recorded a conflict
/// (with leases the leaseless conflict path must be unreachable).
fn lease_handover(seed: u64) -> Result<()> {
    let (env, root) = setup("lease-handover")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/lease-{}", ts());
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;

    let mut m = Model::default();
    let mut w0 = Workload::new(seed, "a");
    let mut w1 = Workload::new(seed.wrapping_add(1), "b");
    // The model's root is the shared subtree both nodes write into.
    std::fs::create_dir(c0.mnt.join("shared"))?;
    eventually("shared subtree on B", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("shared").is_dir(), "shared not on c1");
        Ok(())
    })?;

    let mut epochs = Vec::new();
    for round in 0..3 {
        // A takes (or keeps) authority and writes.
        w0.run_block(&c0.mnt.join("shared"), &mut m, 25)?;
        let a_lease = lease_of(&c0)?;
        anyhow::ensure!(
            a_lease["held"] == true && a_lease["lost"] == false,
            "A should hold the lease while writing, got {a_lease}"
        );
        epochs.push(a_lease["epoch"].as_u64().unwrap_or(0));

        // A goes write-idle: the cooperative idle release must hand the
        // lease over quickly, and B's first mutation must then succeed.
        eventually(
            &format!("B acquires the lease, round {round}"),
            Duration::from_secs(15),
            || {
                let probe = c1.mnt.join(format!("shared/probe-{round}"));
                std::fs::write(&probe, b"b")?;
                std::fs::remove_file(&probe)?;
                let l = lease_of(&c1)?;
                anyhow::ensure!(l["held"] == true, "B does not hold the lease: {l}");
                Ok(())
            },
        )?;
        let b_lease = lease_of(&c1)?;
        epochs.push(b_lease["epoch"].as_u64().unwrap_or(0));
        w1.run_block(&c1.mnt.join("shared"), &mut m, 25)?;

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
        epochs.windows(2).all(|w| w[1] >= w[0]) && epochs.last() > epochs.first(),
        "lease epoch must advance across handovers, saw {epochs:?}"
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
            .with_env("CONSTELLATION_LEASE_TTL_MS", "5000")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
    };
    Ok((
        tune(
            Client::new(root, "c0", &env.endpoint, backend)?,
            "/tmp/.constellation-epoch-c0.key",
        ),
        tune(
            Client::new(root, "c1", &env.endpoint, backend)?,
            "/tmp/.constellation-epoch-c1.key",
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
    std::fs::write(c1.mnt.join("b/from-b"), b"epoch-b")?;
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
    std::fs::create_dir(c0.mnt.join("shared"))?;
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
            error.raw_os_error() == Some(libc::EROFS),
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
    std::fs::write(c0.mnt.join("shared/after-resume"), b"ok")?;
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
/// `.constellation-conflict/same@<node>-<ts>` copy, while `clean-from-a`
/// and `a-only` replay cleanly with no conflict copy at all.
fn deposed_reintegration(_seed: u64) -> Result<()> {
    let (env, root) = setup("deposed-reintegration")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/reintegrate-{}", ts());
    let tune = |client: Client, key: &str| {
        client
            .with_env("CONSTELLATION_NODE_KEY", key)
            .with_env("CONSTELLATION_LEASE_TTL_MS", "5000")
            // Keep B holding for a while after its takeover write, so
            // resumed A's renewal finds B's lease (the deposition path
            // under test) and its replay drain forwards to a live holder.
            // If B has released by then anyway, the drain's own lease
            // fallback replays locally instead; either converges.
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "10000")
            .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "3000")
    };
    let mut c0 = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        "/tmp/.constellation-reintegrate-c0.key",
    );
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        "/tmp/.constellation-reintegrate-c1.key",
    );
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
    proxy.cut()?;
    std::fs::write(c0.mnt.join("shared/clean-from-a"), b"clean")?;
    std::fs::write(c0.mnt.join("shared/a-only"), b"stranded-from-a")?;
    std::fs::write(c0.mnt.join("shared/same"), b"loser-from-a")?;
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
    proxy.heal()?;

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
                anyhow::ensure!(
                    std::fs::read(client.mnt.join("shared/a-only"))? == b"stranded-from-a",
                    "{}: a-only (never touched by B) did not replay A's content",
                    client.name
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
                    "{} must hold exactly one conflict copy, same@<node>-<ts>; found {names:?}",
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
        "/tmp/.constellation-leave-c0.key",
    );
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        "/tmp/.constellation-leave-c1.key",
    );
    let mut c2 = tune(
        Client::new(root.path(), "c2", &env.endpoint, &backend)?,
        "/tmp/.constellation-leave-c2.key",
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
                anyhow::ensure!(p["peers"].as_array().map(|a| a.len()).unwrap_or(0) >= 2);
                Ok(())
            },
        )?;
    }

    std::fs::create_dir(c0.mnt.join("a"))?;
    std::fs::create_dir(c1.mnt.join("b"))?;
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
    std::fs::write(c0.mnt.join("a/after-leave"), b"ok")?;
    std::fs::write(c1.mnt.join("b/after-leave"), b"ok")?;
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
        "/tmp/.constellation-leave-c3.key",
    );
    c3.mount()?;
    eventually("C3 peers with A+B", Duration::from_secs(30), || {
        let p = p2p_of(&c3)?;
        anyhow::ensure!(p["peers"].as_array().map(|a| a.len()).unwrap_or(0) >= 2);
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

    for (name, data) in &expected {
        let got =
            std::fs::read(b.mnt.join(name)).with_context(|| format!("reading {name} on joiner"))?;
        anyhow::ensure!(got == *data, "content mismatch on {name}");
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
    let _ = std::fs::remove_file("/tmp/.constellation-hand-c0.key");
    let _ = std::fs::remove_file("/tmp/.constellation-hand-c1.key");
    let mut c0 = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        "/tmp/.constellation-hand-c0.key",
    );
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        "/tmp/.constellation-hand-c1.key",
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
    let c0_key = "/tmp/.constellation-forward-c0.key";
    let c1_key = "/tmp/.constellation-forward-c1.key";
    let _ = std::fs::remove_file(c0_key);
    let _ = std::fs::remove_file(c1_key);
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

fn scratch_publish(_seed: u64) -> Result<()> {
    let (env, root) = setup("scratch-publish")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/scratch-publish-{}", ts());
    let c0_key = "/tmp/.constellation-scratch-c0.key";
    let c1_key = "/tmp/.constellation-scratch-c1.key";
    let _ = std::fs::remove_file(c0_key);
    let _ = std::fs::remove_file(c1_key);
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
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_NODE_KEY", "/tmp/.constellation-part-c1.key");
    c0 = c0.with_env("CONSTELLATION_P2P", "off");
    let _ = std::fs::remove_file("/tmp/.constellation-part-c1.key");
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
            Err(error) if error.raw_os_error() == Some(libc::ENODATA) => Ok(()),
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
    Ok(())
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
    proxy.cut()?;
    let path = client.mnt.join("pressure");
    let mut file = std::fs::File::create(&path)?;
    let started = std::time::Instant::now();
    let mut saw_enospc = false;
    for i in 0..32 {
        use std::io::Write;
        let block = pattern(seed.wrapping_add(i), 1024 * 1024);
        if let Err(error) = file.write_all(&block) {
            saw_enospc = error.raw_os_error() == Some(libc::ENOSPC);
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
    use constellation_chaos::{Coordinator, LocalCluster, Profile};

    let (env, root) = setup("chaos-ci")?;
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
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?.with_own_node_key();
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?.with_own_node_key();
    let mut c2 = Client::new(root.path(), "c2", &env.endpoint, &backend)?.with_own_node_key();
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
    let profile = Profile::ci(seed, 3);
    Coordinator::run(&mut cluster, profile, &store)
        .with_context(|| format!("chaos-ci artifacts under {}", store.display()))?;

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
    let result = Coordinator::run(&mut cluster, profile, &store);
    if let Err(ref e) = result {
        eprintln!("chaos-soak-4 FAILED: {e}");
        eprintln!("artifacts kept at: {}", store.display());
        for c in &clients {
            eprintln!("--- {} mount.log (tail) ---\n{}", c.name, c.tail_log());
        }
    }
    for c in &mut clients {
        let _ = c.unmount();
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
/// `--foreground`), multi-view `MountAdd`/`MountRemove`, and the
/// daemon's own clean exit once its last view is detached.
fn named_shared_daemon(_seed: u64) -> Result<()> {
    use std::os::unix::net::UnixStream;
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

    fn read_status(sock: &std::path::Path) -> Result<serde_json::Value> {
        use std::io::{BufRead, BufReader, Write};
        let mut stream = UnixStream::connect(sock)
            .with_context(|| format!("connecting to {}", sock.display()))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.write_all(b"{\"cmd\":\"status\"}\n")?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        Ok(serde_json::from_str(&line)?)
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
    let sock_path = state_dir.join("control.sock");

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

        let status = read_status(&sock_path)?;
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
    let mut body = String::new();
    ureq::get(&format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}"))
        .call()
        .with_context(|| format!("listing {prefix}"))?
        .into_reader()
        .read_to_string(&mut body)?;
    let mut out = Vec::new();
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
    Ok(out)
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

/// Bug A (`docs/plans/v1/wip/30-write-path-resilience-and-scale-out.md`
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

/// Bug B (`docs/plans/v1/wip/30-write-path-resilience-and-scale-out.md`
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
    ureq::get(&raw_key(endpoint, &key))
        .call()
        .with_context(|| format!("fetching {key}"))?
        .into_reader()
        .read_to_end(&mut compressed)?;
    let payload =
        zstd::decode_all(&compressed[..]).with_context(|| format!("decompressing {key}"))?;
    let ((v, node, epoch, records), _) =
        postcard::take_from_bytes::<(u32, u64, u64, u64)>(&payload)
            .with_context(|| format!("decoding {key}'s envelope"))?;
    anyhow::ensure!(v == 2, "{key}: unexpected segment envelope version {v}");
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
    ureq::get(&raw_key(endpoint, key))
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
            Err(e) if e.raw_os_error() == Some(libc::ENOTEMPTY) => {}
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
fn holder_publishes_log_prefix(_seed: u64) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;
    /// Pause between the writer's mkdirs: ~200/s, a few dozen per ship
    /// round (sync interval plus one PUT under the injected latency).
    const PACE: Duration = Duration::from_millis(5);
    let (env, root) = setup("holder-publishes-log-prefix")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("log-prefix-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    // A 1 s idle poll ceiling so B and D reach the final head promptly.
    let tune = |c: Client| {
        c.with_own_node_key()
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")
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
        anyhow::ensure!(
            shipped < acked_n,
            "A acknowledged {acked_n} mkdirs and all {shipped} are visible: the kill caught no \
             unshipped tail, so nothing here tested a commit published over one"
        );
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
            "    holder-publishes-log-prefix: {hits}/{samples} samples saw a new commit over \
             unshipped work (max local {max_local}); A acked {acked_n} mkdirs, {shipped} \
             reached the log, {} unshipped tail never visible; head commit applied {applied}, \
             log head {log_head}",
            acked_n - shipped
        );
        Ok(())
    })();

    b.unmount()?;
    d.unmount()?;
    checked
}
