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
        wl.run_block(&c.mnt, &mut model, 30)?;
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

/// Lease state from a node's control API.
fn lease_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["lease"].clone())
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
