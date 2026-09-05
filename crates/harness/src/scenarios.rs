//! Fault-injection scenarios. Each runs a fresh environment: floci S3
//! behind toxiproxy, one or more constellation clients on the host,
//! seeded workloads verified against the model oracle.

use crate::client::Client;
use crate::model::Model;
use crate::s3env::{S3Env, BUCKET};
use crate::suites;
use crate::workload::Workload;
use anyhow::{bail, Context, Result};
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

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
        desc: "freeze the lease holder (SIGSTOP), let B take over after expiry, then resume A: A must detect deposition and refuse to ship",
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
        desc: "reintegrate a deposed holder: clean writes append and a deliberate edit conflict materializes",
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
        name: "partition-split",
        desc: "two nodes, low split threshold: /hot becomes its own partition, then idles into a merge",
        requires: &[],
        run: partition_split,
    },
    Scenario {
        name: "rename-across-partitions",
        desc: "force a split, rename files between partitions from both nodes, kill9 the renamer and recover",
        requires: &[],
        run: rename_across_partitions,
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
        name: "fresh-node-bootstrap",
        desc: "node B rebuilds the namespace purely from S3 (checkpoint + log replay) and must match the model",
        requires: &[],
        run: fresh_node_bootstrap,
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
        desc: "write a file several times --cache-size and sample RSS: must stay flat, not track bytes written (plan 05a)",
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
        desc: "fresh node LIST-seeds S3 existence and deduplicates without per-chunk HEAD misses",
        requires: &[],
        run: existence_bloom_dedup,
    },
    Scenario {
        name: "existence-peer-hint",
        desc: "LIST-disabled uploader uses live peer cache digests only as confirming probe hints",
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
];

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
    keys.into_iter()
        .filter(|key| key.contains("/chunks/") || key.contains("/log/"))
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
    for (key, bytes) in &objects {
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

    let output = client.gc_run(false)?;
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
    let child = client.gc_process(false)?;
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
    wait_for_p2p([&c0, &c1])?;

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

fn wait_for_p2p(clients: [&Client; 2]) -> Result<()> {
    wait_for_peers(&[clients[0], clients[1]])
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

/// A complete LIST seed turns a cold duplicate import into confirming HEADs
/// on hits, while a disabled second pass proves the optimization is optional.
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
    let seed_started = std::time::Instant::now();
    b.mount()?;
    eventually(
        "existence LIST seed completes",
        Duration::from_secs(20),
        || {
            let status = b.control_status()?;
            anyhow::ensure!(
                status["writeback"]["existence_complete"] == true,
                "seed not complete: {status}"
            );
            Ok(())
        },
    )?;
    let seed_elapsed = seed_started.elapsed();
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
    let listed = wb["existence_listed"].as_u64().unwrap_or(0);
    let hits = wb["existence_bloom_hits"].as_u64().unwrap_or(0);
    let misses = wb["existence_bloom_misses"].as_u64().unwrap_or(u64::MAX);
    eprintln!(
        "    existence-bloom-dedup: listed={listed} complete=true bloom_hits={hits} bloom_misses={misses} list_seed={seed_elapsed:.1?}"
    );
    anyhow::ensure!(listed >= before as u64, "LIST omitted chunk keys: {wb}");
    anyhow::ensure!(
        hits >= before as u64,
        "duplicate did not hit the seed: {wb}"
    );
    anyhow::ensure!(misses <= 1, "duplicate unexpectedly missed the seed: {wb}");
    anyhow::ensure!(
        raw_chunk_count(&env, &prefix)? == before,
        "duplicate import created extra chunk objects"
    );
    b.unmount()?;

    let mut c = Client::new(root.path(), "list-off", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_EXISTENCE_LIST", "off");
    c.mount()?;
    std::fs::create_dir(c.mnt.join("without-list"))?;
    model.mkdir(std::path::Path::new("without-list"));
    for (i, data) in files.iter().enumerate() {
        std::fs::write(c.mnt.join(format!("without-list/{i:04}")), data)?;
        model.write_file(
            std::path::Path::new(&format!("without-list/{i:04}")),
            data.clone(),
        );
    }
    eventually("LIST-disabled copy drains", Duration::from_secs(20), || {
        anyhow::ensure!(c.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0));
        Ok(())
    })?;
    model.verify(&c.mnt)?;
    let off = c.control_status()?;
    anyhow::ensure!(
        off["writeback"]["existence_listed"].as_u64() == Some(0)
            && off["writeback"]["existence_complete"] == false,
        "LIST kill switch still seeded: {off}"
    );
    c.unmount()?;
    Ok(())
}

/// With LIST disabled, a clean peer digest gets sole credit for selecting
/// the probe. Disabling cooperative cache removes that hint without changing
/// correctness.
fn existence_peer_hint(_seed: u64) -> Result<()> {
    let (env, root) = setup("existence-peer-hint")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/existence-peer-{}", ts());
    let mut a = coop_client(root.path(), "hint-a", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_EXISTENCE_LIST", "off");
    let mut b = coop_client(root.path(), "hint-b", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_EXISTENCE_LIST", "off");
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p([&a, &b])?;

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
        "    existence-peer-hint: peer_hints={hints} list_enabled=false bloom_hits={} bloom_misses={}",
        status["writeback"]["existence_bloom_hits"],
        status["writeback"]["existence_bloom_misses"]
    );
    anyhow::ensure!(hints >= 1, "peer digest never selected a probe: {status}");
    let mut model = Model::default();
    model.write_file(std::path::Path::new("source"), data.clone());
    model.write_file(std::path::Path::new("peer-copy"), data.clone());
    model.verify(&b.mnt)?;
    a.unmount()?;
    b.unmount()?;

    let mut c = Client::new(root.path(), "coop-off", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_EXISTENCE_LIST", "off")
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
    wait_for_p2p([&a, &b])?;

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
    wait_for_p2p([&a, &b])?;

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
/// would mean the invariant broke somewhere.
fn ensure_no_conflicts(clients: [&Client; 2]) -> Result<()> {
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
    ensure_no_conflicts([&c0, &c1])?;
    eprintln!("    lease-handover: epochs {epochs:?}");
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Fencing (DESIGN.md §4): A holds the lease with unshipped records and
/// is frozen with SIGSTOP, so it stops renewing. After expiry B takes
/// over — legally, having applied everything A flushed — and writes.
/// Resumed, A must discover it was deposed, refuse to ship, and say so
/// through the control API. A's unshipped writes are expected to be
/// stranded (phase-4 reintegration); what must hold is that nothing
/// *shared* is damaged: B's namespace stays exactly model-correct and a
/// third, fresh node rebuilds the same world from the log alone.
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
    let mut c0 = short(Client::new(root.path(), "c0", &env.endpoint, &backend)?);
    let mut c1 = short(Client::new(root.path(), "c1", &env.endpoint, &backend)?);
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

    // Resume A: its next renew CAS fails and it must declare itself out.
    c0.resume()?;
    eventually("A reports the lost lease", Duration::from_secs(30), || {
        let l = lease_of(&c0)?;
        anyhow::ensure!(l["lost"] == true, "A does not report lost: {l}");
        anyhow::ensure!(l["held"] == false, "A still claims to hold it: {l}");
        Ok(())
    })?;
    // Its stranded records stay in the journal — neither shipped behind
    // B's back nor silently dropped (phase-4 reintegration).
    let a_spool = c0.control_status()?["spool"].clone();
    anyhow::ensure!(
        a_spool["journal_backlog"].as_u64().unwrap_or(0) > 0,
        "a deposed node must keep its unshipped journal: {a_spool}"
    );
    // And it refuses further mutations rather than writing behind B.
    let refused = std::fs::write(c0.mnt.join("shared/after-deposition"), b"nope");
    anyhow::ensure!(
        refused.is_err(),
        "a deposed node must not accept new mutations"
    );

    // B's view is exactly the model, and A's stranded records never
    // reached the shared log.
    eventually(
        "B's namespace is model-correct",
        Duration::from_secs(20),
        || model.verify(&c1.mnt),
    )?;
    anyhow::ensure!(
        !c1.mnt.join("shared/stranded").exists(),
        "a deposed holder's unshipped write must not appear on the new holder"
    );

    // The shared log is uncorrupted: a fresh node rebuilds B's world.
    c0.kill9()?; // A's journal stays stranded on disk, by design
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
    wait_for_p2p([&c0, &c1])?;

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
    // B's write forces a P2P-only p0 handoff from A.
    std::fs::write(c1.mnt.join("b/from-b"), b"epoch-b")?;
    anyhow::ensure!(
        c0.control_status()?["spool"]["journal_backlog"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    anyhow::ensure!(
        c1.control_status()?["spool"]["journal_backlog"]
            .as_u64()
            .unwrap_or(0)
            > 0
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
    ensure_no_conflicts([&c0, &c1])?;
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
    wait_for_p2p([&c0, &c1])?;
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
    ensure_no_conflicts([&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

fn deposed_reintegration(_seed: u64) -> Result<()> {
    let (env, root) = setup("deposed-reintegration")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/reintegrate-{}", ts());
    let tune = |client: Client, key: &str| {
        client
            .with_env("CONSTELLATION_NODE_KEY", key)
            .with_env("CONSTELLATION_LEASE_TTL_MS", "5000")
            // Keep B's takeover live long enough for resumed A to
            // deterministically observe the fencing CAS failure. We
            // explicitly wait for B's later idle release before asking A
            // to reintegrate.
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
    wait_for_p2p([&c0, &c1])?;

    std::fs::create_dir(c0.mnt.join("shared"))?;
    std::fs::write(c0.mnt.join("shared/same"), b"baseline")?;
    eventually("baseline visible on B", Duration::from_secs(30), || {
        anyhow::ensure!(std::fs::read(c1.mnt.join("shared/same"))? == b"baseline");
        Ok(())
    })?;
    let a_epoch = lease_of(&c0)?["epoch"].as_u64().unwrap_or(0);

    // Two stranded changes: one clean path and one deliberate edit conflict.
    std::fs::write(c0.mnt.join("shared/clean-from-a"), b"clean")?;
    std::fs::write(c0.mnt.join("shared/same"), b"loser-from-a")?;
    anyhow::ensure!(
        c0.control_status()?["spool"]["journal_backlog"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "A did not retain stranded records"
    );
    c0.pause()?;

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

    c0.resume()?;
    eventually("A reports deposition", Duration::from_secs(20), || {
        anyhow::ensure!(lease_of(&c0)?["lost"] == true);
        Ok(())
    })?;
    eventually(
        "B releases the takeover lease for reintegration",
        Duration::from_secs(20),
        || {
            let lease = lease_of(&c1)?;
            anyhow::ensure!(lease["held"] == false, "B still holds the lease: {lease}");
            Ok(())
        },
    )?;
    c0.reintegrate()?;

    eventually(
        "clean branch and conflict materialization converge",
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
                anyhow::ensure!(std::fs::read(client.mnt.join("shared/clean-from-a"))? == b"clean");
                let dir = client.mnt.join("shared/.constellation-conflict");
                let conflict = std::fs::read_dir(&dir)?
                    .filter_map(|entry| entry.ok())
                    .find(|entry| entry.file_name().to_string_lossy().starts_with("same@"))
                    .context("same@ conflict file missing")?;
                anyhow::ensure!(std::fs::read(conflict.path())? == b"loser-from-a");
                let status = client.control_status()?;
                anyhow::ensure!(
                    status["reintegration"]["conflicts_materialized"]
                        .as_u64()
                        .unwrap_or(0)
                        >= 1
                        || client.name == "c1"
                );
            }
            Ok(())
        },
    )?;
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
    ensure_no_conflicts([&c0, &c1])?;

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
    // Clean unmount ships the journal tail and writes a checkpoint.
    c0.unmount()?;

    // Second epoch exercises the replay path past the checkpoint: ops
    // ship on the 2 s ticker, then the daemon is SIGKILLed so no final
    // checkpoint covers them.
    c0.mount()?;
    wl.run_block(&c0.mnt, &mut model, 40)?;
    std::thread::sleep(Duration::from_secs(5)); // >= 2 shipper ticks, journal drained
    c0.kill9()?;

    // A brand-new node with an empty state dir sees the same world.
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    c1.mount().context("bootstrap mount on fresh node")?;
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

fn partition_ids(c: &Client) -> Result<Vec<String>> {
    Ok(partition_ids_from(&c.control_status()?))
}

fn partition_ids_from(status: &serde_json::Value) -> Vec<String> {
    status["partitions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|p| p["id"].as_str().map(|s| s.to_string()))
        .collect()
}

fn part_env(c: Client, split_ops: u64, merge_idle_s: u64) -> Client {
    c.with_env("CONSTELLATION_PART_SPLIT_OPS", &split_ops.to_string())
        .with_env("CONSTELLATION_PART_MERGE_IDLE_S", &merge_idle_s.to_string())
        // Heat-driven splitting is off by default (forwarded mutations
        // made directory heat a poor proxy for lease contention). These
        // scenarios test the split/merge machinery, so they ask for it.
        .with_env("CONSTELLATION_PART_AUTOSPLIT", "on")
        .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "60000")
}

/// Hammer `/hot` with enough close()s to close two split windows, then
/// wait until the control API shows a child partition rooted there.
fn wait_for_split(c: &Client, hot: &std::path::Path, model: &mut Model, tag: &str) -> Result<()> {
    for wave in 0..3 {
        for i in 0..8 {
            let rel = std::path::PathBuf::from(format!("hot/{tag}-w{wave}-{i}"));
            let data = format!("{tag}-{wave}-{i}").into_bytes();
            std::fs::write(hot.join(rel.file_name().unwrap()), &data)?;
            model.write_file(&rel, data);
        }
        eventually(
            &format!("wave {wave} shipped"),
            Duration::from_secs(10),
            || {
                let b = c.control_status()?["spool"]["journal_backlog"]
                    .as_u64()
                    .unwrap_or(1);
                anyhow::ensure!(b == 0, "backlog {b}");
                Ok(())
            },
        )?;
    }
    eventually(
        "split appears on control API",
        Duration::from_secs(20),
        || {
            let status = c.control_status()?;
            let ids = partition_ids_from(&status);
            anyhow::ensure!(
                ids.iter().any(|id| id != "p0"),
                "still one partition: {ids:?} partitions={} log=\n{}",
                status["partitions"],
                c.tail_log_n(50)
            );
            Ok(())
        },
    )
}

/// Two nodes, one FS: node A hammers `/hot` until a split is visible on
/// the control API; both trees match the model; then `/hot` goes idle
/// and the child merges back into p0.
fn partition_split(_seed: u64) -> Result<()> {
    let (env, root) = setup("partition-split")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/psplit-{}", ts());
    let mut c0 = part_env(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        4,
        8,
    );
    let mut c1 = part_env(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        4,
        8,
    );
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;

    let mut model = Model::default();
    std::fs::create_dir(c0.mnt.join("hot"))?;
    model.mkdir(std::path::Path::new("hot"));
    eventually("hot visible on B", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("hot").is_dir(), "hot not on c1");
        Ok(())
    })?;

    wait_for_split(&c0, &c0.mnt.join("hot"), &mut model, "a")?;
    // A write after the split acquires the child lease so the parent
    // holder can merge it once /hot goes idle.
    std::fs::write(c0.mnt.join("hot/post"), b"post")?;
    model.write_file(std::path::Path::new("hot/post"), b"post".to_vec());
    eprintln!("    partition-split: after split {:?}", partition_ids(&c0)?);
    eventually("trees match after split", Duration::from_secs(30), || {
        model.verify(&c0.mnt).context("via c0")?;
        model.verify(&c1.mnt).context("via c1")?;
        Ok(())
    })?;

    // Keep p0 busy with an unrelated dir so the parent lease stays held
    // while /hot goes idle long enough to merge.
    std::fs::create_dir(c0.mnt.join("keep"))?;
    model.mkdir(std::path::Path::new("keep"));
    std::thread::sleep(Duration::from_secs(10));
    std::fs::write(c0.mnt.join("keep/tick"), b"1")?;
    model.write_file(std::path::Path::new("keep/tick"), b"1".to_vec());
    eventually("child merged back into p0", Duration::from_secs(20), || {
        let ids = partition_ids(&c0)?;
        anyhow::ensure!(
            ids == ["p0".to_string()] || (ids.len() == 1 && ids[0] == "p0"),
            "still split: {ids:?}"
        );
        Ok(())
    })?;
    eventually("trees match after merge", Duration::from_secs(20), || {
        model.verify(&c0.mnt).context("via c0")?;
        model.verify(&c1.mnt).context("via c1")?;
        Ok(())
    })?;
    ensure_no_conflicts([&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
}

/// Force a split, then rename across the two partitions from both
/// nodes. Kill the renamer between operations; remount must keep the
/// namespace correct (abort recovery if a half-committed xpart was
/// stranded).
fn rename_across_partitions(_seed: u64) -> Result<()> {
    let (env, root) = setup("rename-xpart")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/xpart-{}", ts());
    let mut c0 = part_env(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        4,
        3600,
    );
    let mut c1 = part_env(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        4,
        3600,
    );
    c0.fs_create()?;
    c0.mount()?;
    c1.mount()?;

    let mut model = Model::default();
    std::fs::create_dir(c0.mnt.join("hot"))?;
    std::fs::create_dir(c0.mnt.join("cold"))?;
    model.mkdir(std::path::Path::new("hot"));
    model.mkdir(std::path::Path::new("cold"));
    eventually("dirs on B", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("hot").is_dir() && c1.mnt.join("cold").is_dir());
        Ok(())
    })?;
    wait_for_split(&c0, &c0.mnt.join("hot"), &mut model, "s")?;
    anyhow::ensure!(
        partition_ids(&c0)?.iter().any(|id| id != "p0"),
        "expected a split before cross-partition rename"
    );

    std::fs::write(c0.mnt.join("hot/x"), b"from-a")?;
    model.write_file(std::path::Path::new("hot/x"), b"from-a".to_vec());
    std::fs::rename(c0.mnt.join("hot/x"), c0.mnt.join("cold/y"))?;
    model.rename(
        std::path::Path::new("hot/x"),
        std::path::Path::new("cold/y"),
    );
    eventually("rename A->B visible", Duration::from_secs(20), || {
        anyhow::ensure!(c1.mnt.join("cold/y").is_file(), "y not on c1");
        anyhow::ensure!(!c1.mnt.join("hot/x").exists(), "x still on c1");
        Ok(())
    })?;

    std::fs::write(c1.mnt.join("cold/p"), b"from-b")?;
    model.write_file(std::path::Path::new("cold/p"), b"from-b".to_vec());
    std::fs::rename(c1.mnt.join("cold/p"), c1.mnt.join("hot/q"))?;
    model.rename(
        std::path::Path::new("cold/p"),
        std::path::Path::new("hot/q"),
    );
    eventually("rename B->A visible", Duration::from_secs(20), || {
        anyhow::ensure!(c0.mnt.join("hot/q").is_file(), "q not on c0");
        Ok(())
    })?;

    // Kill the renamer between operations; remount must recover.
    std::fs::write(c0.mnt.join("hot/z"), b"z")?;
    model.write_file(std::path::Path::new("hot/z"), b"z".to_vec());
    c0.kill9()?;
    c0.mount()?;
    eventually("post-kill9 namespace", Duration::from_secs(30), || {
        model.verify(&c0.mnt).context("via c0 after remount")?;
        model.verify(&c1.mnt).context("via c1")?;
        Ok(())
    })?;
    std::fs::rename(c0.mnt.join("hot/z"), c0.mnt.join("cold/z"))?;
    model.rename(
        std::path::Path::new("hot/z"),
        std::path::Path::new("cold/z"),
    );
    eventually("final xpart rename", Duration::from_secs(20), || {
        model.verify(&c0.mnt).context("via c0")?;
        model.verify(&c1.mnt).context("via c1")?;
        Ok(())
    })?;
    ensure_no_conflicts([&c0, &c1])?;
    c0.unmount()?;
    c1.unmount()?;
    Ok(())
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
    wait_for_p2p([&c0, &c1])?;

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
    ensure_no_conflicts([&c0, &c1])?;
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
    wait_for_p2p([&c0, &c1])?;

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
    ensure_no_conflicts([&c0, &c1])?;
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
    wait_for_p2p([&c0, &c1])?;

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
    ensure_no_conflicts([&c0, &c1])?;
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
    ensure_no_conflicts([&c0, &c1])?;
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

/// Plan 05a's exit criterion: daemon RSS must not scale with the size
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
/// mount's GC. "Absent or short is a pass; corrupt is not" (plan 05a).
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

/// Regression test for prerequisite 2 (plan 05a): today's tree ships
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
    let mut c0 = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    let mut c1 = Client::new(root.path(), "c1", &env.endpoint, &backend)?;
    let mut c2 = Client::new(root.path(), "c2", &env.endpoint, &backend)?;
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
