//! Fault-injection scenarios. Each runs a fresh environment: floci S3
//! behind toxiproxy, one or more constellation clients on the host,
//! seeded workloads verified against the model oracle.

use crate::client::Client;
use crate::model::Model;
use crate::s3env::{S3Env, BUCKET};
use crate::workload::Workload;
use anyhow::{Context, Result};
use std::time::Duration;

pub struct Scenario {
    pub name: &'static str,
    pub desc: &'static str,
    pub run: fn(seed: u64) -> Result<()>,
}

pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "baseline",
        desc: "workload with a healthy network; model verified per block",
        run: baseline,
    },
    Scenario {
        name: "latency",
        desc: "workload under 150ms +/-50ms S3 latency",
        run: latency,
    },
    Scenario {
        name: "slow-network",
        desc: "workload under 256 KB/s S3 bandwidth + sliced packets",
        run: slow_network,
    },
    Scenario {
        name: "s3-outage",
        desc: "cut S3 mid-workload: cached reads keep working, writes recover after heal",
        run: s3_outage,
    },
    Scenario {
        name: "s3-flap",
        desc: "S3 connection cut/heal every block; workload must stay correct",
        run: s3_flap,
    },
    Scenario {
        name: "kill9-remount",
        desc: "SIGKILL the daemon between blocks; remount must recover all committed state",
        run: kill9_remount,
    },
    Scenario {
        name: "cold-cache",
        desc: "wipe the chunk cache between blocks; reads must re-fetch from S3",
        run: cold_cache,
    },
    Scenario {
        name: "two-clients-disjoint",
        desc: "two clients, one bucket: disjoint namespaces must not corrupt each other (phase-1 scope)",
        run: two_clients_disjoint,
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

    // Heal and verify everything still converges.
    proxy.heal()?;
    std::thread::sleep(Duration::from_secs(1));
    wl.run_block(&c.mnt, &mut model, 30)?;
    model.verify(&c.mnt)?;
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
