//! Fault-injection scenarios. Each runs a fresh environment: floci S3
//! behind toxiproxy, one or more constellation clients on the host,
//! seeded workloads verified against the model oracle.

use crate::client::Client;
use crate::model::Model;
use crate::s3env::{S3Env, BUCKET};
use crate::suites;
use crate::workload::Workload;
use anyhow::{Context, Result};
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
        name: "p2p-partition-tolerance",
        desc: "with P2P disabled on one node, shared-filesystem correctness still holds on the S3 slow path",
        requires: &[],
        run: p2p_partition_tolerance,
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
];

fn setup(name: &str) -> Result<(S3Env, tempfile::TempDir)> {
    let env = S3Env::start().context("starting S3 environment")?;
    let root = tempfile::Builder::new()
        .prefix(&format!("harness-{name}-"))
        .tempdir()?;
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
fn wait_for_p2p(clients: [&Client; 2]) -> Result<()> {
    for c in clients {
        eventually(
            &format!("{} reports a live P2P peer", c.name),
            Duration::from_secs(30),
            || {
                let p = p2p_of(c)?;
                anyhow::ensure!(p["enabled"] == true, "{} has no fast path: {p}", c.name);
                let n = p["peers"].as_array().map(|a| a.len()).unwrap_or(0);
                anyhow::ensure!(n >= 1, "{} sees no peers yet: {p}", c.name);
                Ok(())
            },
        )?;
    }
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
    let tune = |c: Client, key: &str| {
        c.with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", &idle_ms.to_string())
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
