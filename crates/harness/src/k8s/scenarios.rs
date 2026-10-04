//! The `k8s-scenario` catalog (plan 37 §12): Constellation behaviour that
//! only exists behind the CSI driver — PVs staged into node-owned engine
//! pods, published into workload pods, shared across nodes, many to a
//! pool. Each scenario owns a [`Scope`] (namespace + pool StorageClass)
//! and ends with [`Scope::finish`], which also proves every volume it used
//! unstaged.

use super::remote::RemoteWorkload;
use super::{
    data_dir, fs_uuid_of, rotating_key, rotating_secret, EnginePod, Env, Scope, ROTATING_SECRET,
};
use crate::docker::docker;
use crate::model::Observed;
use crate::model::{Model, Node};
use crate::scenarios::eventually;
use anyhow::{bail, ensure, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct K8sScenario {
    pub name: &'static str,
    pub desc: &'static str,
    /// Schedulable workers the scenario needs (a cluster with fewer skips
    /// it, saying so).
    pub workers: usize,
    pub run: fn(&Env, u64) -> Result<()>,
    /// A measurement ([`Env::measure`]) whose p99 across every run of
    /// `--repeat` must stay under a bound: the run fails otherwise.
    pub gate: Option<Gate>,
}

/// [`K8sScenario::gate`].
pub struct Gate {
    pub metric: &'static str,
    pub p99_under_ms: u64,
}

pub const K8S_SCENARIOS: &[K8sScenario] = &[
    K8sScenario {
        name: "csi-pod-rw",
        desc: "a pod writes a seeded workload through a PVC, verified against the model; a pod on the other worker remounts it and sees the same tree",
        workers: 2,
        run: csi_pod_rw,
        gate: None,
    },
    K8sScenario {
        name: "csi-rwx-across-nodes",
        desc: "two pods on two workers share one RWX PVC: alternating and concurrent writes converge to the model on both (close-to-open, bounded: eventual, nothing stronger)",
        workers: 2,
        run: csi_rwx_across_nodes,
        gate: None,
    },
    K8sScenario {
        name: "csi-many-pvs-one-pool",
        desc: "50 PVs of one pool StorageClass across both workers: each its own tree, one engine pod per (pool, node), each PV's quota enforced on its own",
        workers: 2,
        run: csi_many_pvs_one_pool,
        gate: None,
    },
    K8sScenario {
        name: "csi-plugin-restart-survives",
        desc: "node-plugin and controller pods deleted under a writing pod: mounts survive, zero errors, the engine pods untouched, the restarted plugins stage, publish and unstage",
        workers: 2,
        run: csi_plugin_restart_survives,
        gate: None,
    },
    K8sScenario {
        name: "csi-snapshot-clone-mount",
        desc: "VolumeSnapshot of a written PVC, then a PVC restored from it and a PVC cloned from the source, both mounted on the other worker: the restore is the tree at the snapshot, the clone the tree now, both metadata-only clones in the source's pool, independent of the source",
        workers: 2,
        run: csi_snapshot_clone_mount,
        gate: None,
    },
    K8sScenario {
        name: "csi-clone-cross-pool-refused",
        desc: "clone and restore into the source's own pool bind and hold its data; the same clone and restore into another pool's StorageClass are refused (INVALID_ARGUMENT naming both filesystems) and stay Pending",
        workers: 2,
        run: csi_clone_cross_pool_refused,
        gate: None,
    },
    K8sScenario {
        name: "csi-secret-rotation",
        desc: "a `refreshing` class's Secret rotated twice under a writing pod and a reader on the other worker: no remount, no engine restart, zero errors; every engine pod's S3 clients sign with the new pair (fs.list generations), each push is in its audit log under the node plugin's service principal, and no pod spec, environment or hostPath file holds a credential",
        workers: 2,
        run: csi_secret_rotation,
        gate: None,
    },
    K8sScenario {
        name: "csi-engine-pod-handoff-under-load",
        desc: "upgrade-under-load's writer, creator and reader in a pod on an RWO PV while the chart's image tag changes: the node plugin hands the engine pod's FUSE session to a replacement (credentials over the handoff), the staging mount stays; zero errors of any kind, every acknowledged byte read back on the other worker, a snapshot asked for as the replacement waits as a standby restores to a state the trio went through holding everything acknowledged before it was asked for; the trio's longest call overlapping the plugin's handoff window (pause_ms) is gated at p99 < 2 s across --repeat",
        workers: 2,
        run: csi_engine_pod_handoff_under_load,
        gate: Some(Gate {
            metric: "pause_ms",
            p99_under_ms: PAUSE_P99_MS,
        }),
    },
    K8sScenario {
        name: "csi-node-drain",
        desc: "kubectl drain of the worker holding a mounted PV: the pod moves to the other worker with its data, the drained worker's engine pod leaves the pool's registry before it goes (no ghost roster entry), no engine pod or node identity is left on it; then the worker running the pool's controller-owned engine pod is drained right after a delete into the trash made with the StorageClass already deleted: the pool is recorded from the PV itself, and the purge worker brings the pod back elsewhere, purges through it and retires the drained incarnation's record",
        workers: 2,
        run: super::lifecycle::csi_node_drain,
        gate: None,
    },
    K8sScenario {
        name: "csi-trash-purge-under-load",
        desc: "three PVs of one pool deleted (a few small files, 64 MiB, 100 000 empty files in 100 directories) while two others are written on both workers: every trash entry purged within the purge budgets, the writers never fail nor stall, GC finds the purged chunks unreferenced only after the purge, which itself deleted no S3 object",
        workers: 2,
        run: super::lifecycle::csi_trash_purge_under_load,
        gate: None,
    },
    K8sScenario {
        name: "csi-pod-security",
        desc: "the privilege split of plan 37 §9, as PodSecurity admission judges it (server dry runs in a `restricted` namespace): controller and controller-owned engine pod admitted, node-owned engine pods refused for their hostPaths only, the node plugin refused as privileged",
        workers: 1,
        run: csi_pod_security,
        gate: None,
    },
];

/// Cross-node visibility (log shipping + FUSE attribute TTLs) is
/// asynchronous; this bounds how long a reader may lag.
const CONVERGE: Duration = Duration::from_secs(120);

/// `pod`'s view of `claim` equals `model` now.
fn verify(s: &Scope, pod: &str, claim: &str, model: &Model) -> Result<()> {
    let seen = s.listing(pod, &data_dir(claim))?;
    model
        .verify_observed(&seen)
        .with_context(|| format!("{pod}'s view of {claim}"))
}

/// `pod`'s view of `claim` reaches `model`. Close-to-open in its default
/// `bounded` form (DESIGN §"Close-to-open"): an open after another node's
/// close sees it within the visibility bound, so a reader is given time —
/// and nothing about what it sees in the meantime is asserted (a read
/// racing the replica's catch-up is not covered by close-to-open).
fn converge(s: &Scope, pod: &str, claim: &str, model: &Model) -> Result<()> {
    eventually(
        &format!("{pod}'s view of {claim} matches the model"),
        CONVERGE,
        || verify(s, pod, claim, model),
    )
}

/// Plan 37 K3 gate: a pod mounts a PV and reads and writes through it.
fn csi_pod_rw(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "csi-pod-rw")?;
    s.pvc("rw", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("writer", w1, &["rw"])?;
    // The positive control of `Scope::finish`'s unstage check.
    let handle = s.volume_handle("rw")?;
    ensure!(
        s.staged_on(w1, &handle)? && !s.staged_on(w2, &handle)?,
        "{handle} is not staged on {w1} (only), where its pod runs"
    );
    let mut model = Model::default();
    let mut w = RemoteWorkload::new(seed, "rw");
    for block in 0..4 {
        let script = w.block(&data_dir("rw"), &mut model, 40);
        s.exec("writer", &script)
            .with_context(|| format!("block {block}"))?;
        verify(&s, "writer", "rw", &model).with_context(|| format!("block {block}"))?;
    }
    eprintln!(
        "   writer on {w1}: 4 blocks, {} entries, model verified per block",
        model.nodes.len()
    );
    // RWO: the next pod comes after the first is gone, on the other node —
    // an unstage on w1 and a fresh stage on w2.
    s.delete_pods(&["writer"])?;
    s.pod("reader", w2, &["rw"])?;
    converge(&s, "reader", "rw", &model)?;
    let script = w.block(&data_dir("rw"), &mut model, 20);
    s.exec("reader", &script)?;
    verify(&s, "reader", "rw", &model)?;
    eprintln!("   reader on {w2}: the same tree after a restage, then 20 more ops");
    s.finish()
}

/// Plan 37 K3 gate: RWX across nodes, under close-to-open.
fn csi_rwx_across_nodes(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "csi-rwx")?;
    s.pvc("shared", "1Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("a", w1, &["shared"])?;
    s.pod("b", w2, &["shared"])?;
    let root = data_dir("shared");

    // Phase 1: one writer at a time, alternating nodes. A writer starts
    // only once its own node shows everything the other closed (that is
    // the close-to-open contract: an open after the close). The other node
    // must then converge to the model.
    let mut model = Model::default();
    let mut w = RemoteWorkload::new(seed, "x");
    for round in 0..6 {
        let (writer, reader) = if round % 2 == 0 {
            ("a", "b")
        } else {
            ("b", "a")
        };
        converge(&s, writer, "shared", &model)
            .with_context(|| format!("round {round}: {writer} catching up"))?;
        let script = w.block(&root, &mut model, 25);
        s.exec(writer, &script)
            .with_context(|| format!("round {round}: {writer} writing"))?;
        verify(&s, writer, "shared", &model)
            .with_context(|| format!("round {round}: {writer} reading its own writes"))?;
        converge(&s, reader, "shared", &model)
            .with_context(|| format!("round {round}: {reader} catching up"))?;
    }
    eprintln!(
        "   6 alternating rounds a({w1})/b({w2}): {} entries, both converged",
        model.nodes.len()
    );

    // Phase 2: both write at once, each in its own directory (disjoint,
    // so their interleaving is free), then both see both.
    s.exec("a", &format!("set -e; cd {root}; mkdir a b"))?;
    model.mkdir(Path::new("a"));
    model.mkdir(Path::new("b"));
    converge(&s, "b", "shared", &model)?;
    let (mut ma, mut mb) = (Model::default(), Model::default());
    let mut wa = RemoteWorkload::new(seed ^ 0xa, "a");
    let mut wb = RemoteWorkload::new(seed ^ 0xb, "b");
    for round in 0..3 {
        let sa = wa.block(&format!("{root}/a"), &mut ma, 20);
        let sb = wb.block(&format!("{root}/b"), &mut mb, 20);
        let (ra, rb) = std::thread::scope(|t| {
            let ha = t.spawn(|| s.exec("a", &sa));
            let hb = t.spawn(|| s.exec("b", &sb));
            (ha.join(), hb.join())
        });
        ra.map_err(|_| anyhow::anyhow!("writer a panicked"))?
            .with_context(|| format!("concurrent round {round}: a"))?;
        rb.map_err(|_| anyhow::anyhow!("writer b panicked"))?
            .with_context(|| format!("concurrent round {round}: b"))?;
    }
    ma.graft(&mut model, Path::new("a"));
    mb.graft(&mut model, Path::new("b"));
    converge(&s, "a", "shared", &model)?;
    converge(&s, "b", "shared", &model)?;
    eprintln!(
        "   3 concurrent rounds into a/ and b/: both pods converged on {} entries",
        model.nodes.len()
    );
    s.finish()
}

/// Plan 37 K3 gate: many PVs in one pool, across nodes.
fn csi_many_pvs_one_pool(env: &Env, seed: u64) -> Result<()> {
    const PVS: usize = 50;
    const PER_POD: usize = 5;
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "csi-many-pvs")?;
    let claim = |i: usize| format!("pv-{i:02}");
    // Every PV has a quota (its capacity); three of them are filled below.
    // pv-00 (16 MiB) and pv-01 (64 MiB) share pod 0 on w1, so the same
    // engine pod and the same pool filesystem; pv-05 (32 MiB) is in pod 1
    // on w2.
    let size = |i: usize| match i {
        0 => "16Mi",
        5 => "32Mi",
        _ => "64Mi",
    };
    for i in 0..PVS {
        s.pvc(&claim(i), size(i), true)?;
    }
    s.wait_bound(Duration::from_secs(600))?;
    let handles: Vec<String> = (0..PVS)
        .map(|i| s.volume_handle(&claim(i)))
        .collect::<Result<_>>()?;
    let uuids: BTreeSet<String> = handles
        .iter()
        .map(|h| fs_uuid_of(h))
        .collect::<Result<_>>()?;
    ensure!(
        uuids.len() == 1,
        "50 PVs of one unsharded pool class landed in {} filesystems",
        uuids.len()
    );
    let fs = uuids.into_iter().next().unwrap();
    eprintln!("   {PVS} PVCs Bound, all in pool filesystem {fs}");

    // Pods alternate between the workers, five PVs each.
    let pods: Vec<(String, &String, Vec<String>)> = (0..PVS / PER_POD)
        .map(|p| {
            (
                format!("p{p}"),
                if p % 2 == 0 { w1 } else { w2 },
                (p * PER_POD..(p + 1) * PER_POD).map(claim).collect(),
            )
        })
        .collect();
    for (name, node, claims) in &pods {
        let c: Vec<&str> = claims.iter().map(String::as_str).collect();
        s.spawn_pod(name, node, &c, None)?;
    }
    for (name, _, _) in &pods {
        s.wait_ready(name, Duration::from_secs(600))?;
    }

    // Every PV gets its own seeded tree (names carry the PV, so a write
    // that lands in the wrong PV shows up as an extra entry there).
    let mut models: Vec<Model> = vec![Model::default(); PVS];
    for (p, (pod, _, claims)) in pods.iter().enumerate() {
        let mut script = String::new();
        for (j, c) in claims.iter().enumerate() {
            let i = p * PER_POD + j;
            let mut w = RemoteWorkload::new(seed.wrapping_add(i as u64), c);
            script.push_str(&format!(
                "(\n{}\n)\n",
                w.block(&data_dir(c), &mut models[i], 15)
            ));
        }
        s.exec(pod, &format!("set -e\n{script}"))?;
    }
    let verify_all = |models: &[Model]| -> Result<()> {
        for (p, (pod, _, claims)) in pods.iter().enumerate() {
            for (j, c) in claims.iter().enumerate() {
                verify(&s, pod, c, &models[p * PER_POD + j])?;
            }
        }
        Ok(())
    };
    verify_all(&models)?;
    eprintln!("   {PVS} PVs written and verified independently, 10 pods on {w1} and {w2}");

    // One engine pod per (pool, node in use) — two — not one per PV; each
    // serving its node's 25 views. Plus the pool's one controller-owned pod.
    eventually(
        "one node engine pod per (pool, node), 25 views each",
        Duration::from_secs(60),
        || {
            let e = s.engine_pods(&fs, "node")?;
            let nodes: BTreeSet<&str> = e.iter().map(|p| p.node.as_str()).collect();
            ensure!(
                e.len() == 2 && nodes == BTreeSet::from([w1.as_str(), w2.as_str()]),
                "node engine pods of the pool: {e:?}"
            );
            ensure!(
                e.iter().all(|p| p.views == Some((PVS / 2) as u64)),
                "view counts: {:?}",
                e.iter().map(|p| (&p.node, p.views)).collect::<Vec<_>>()
            );
            Ok(())
        },
    )?;
    let ctl = s.engine_pods(&fs, "controller")?;
    ensure!(
        ctl.len() == 1,
        "controller engine pods of the pool: {ctl:?}"
    );
    eprintln!("   engine pods: 2 node-owned (25 views each) + 1 controller-owned, for 50 PVs");

    // Quotas, each PV's own: pv-00 refuses past 16 MiB while pv-01 next to
    // it (same pod, same engine, same filesystem) takes 24 MiB, and pv-05
    // takes more than pv-00's limit and refuses past its own 32 MiB.
    let fill = |pod: &str, c: &str, name: &str, mib: u32| -> Result<(bool, u64)> {
        let out = s.exec(
            pod,
            &format!(
                "cd {d}; if dd if=/dev/zero of={name} bs=1M count={mib} conv=fsync 2>/tmp/dd.err; \
                 then echo ok; else grep -q 'No space left on device' /tmp/dd.err && echo enospc \
                 || {{ cat /tmp/dd.err >&2; echo other; }}; fi; stat -c %s {name}",
                d = data_dir(c)
            ),
        )?;
        let mut l = out.lines();
        let verdict = l.next().unwrap_or_default().to_string();
        let size: u64 = l.next().unwrap_or("0").trim().parse()?;
        match verdict.as_str() {
            "ok" => Ok((true, size)),
            "enospc" => Ok((false, size)),
            v => bail!("dd into {c} failed with neither success nor ENOSPC ({v})"),
        }
    };
    // Subtree quotas are soft (TESTING.md, "Kubernetes CSI driver"): what
    // lands past the cap is bounded by the writes in flight across a walk,
    // here dd's 1 MiB writes, so a refusal may come up to SLACK late.
    // Nor may a refusal come early: past the seeded tree's bytes, all but
    // the last (refused, 1 MiB) write's worth of the cap is writable.
    const SLACK: u64 = 2 << 20;
    let tree_bytes = |m: &Model| -> u64 {
        m.nodes
            .values()
            .map(|n| match n {
                Node::File { data } => data.len() as u64,
                _ => 0,
            })
            .sum()
    };
    let (ok, n) = fill("p0", &claim(0), "fill", 24)?;
    eprintln!("   pv-00: ENOSPC={} after {n} bytes", !ok);
    ensure!(
        !ok && n <= (16 << 20) + SLACK,
        "pv-00 (16 MiB) took {n} bytes of a 24 MiB write without ENOSPC"
    );
    let floor = (16u64 << 20).saturating_sub(tree_bytes(&models[0]) + SLACK);
    ensure!(
        n >= floor,
        "pv-00 (16 MiB) refused at {n} bytes, below {floor} (its tree holds {} bytes)",
        tree_bytes(&models[0])
    );
    let (ok, n) = fill("p0", &claim(1), "fill", 24)?;
    ensure!(
        ok && n == 24 << 20,
        "pv-01 (64 MiB) refused 24 MiB ({n} written) while pv-00 was full"
    );
    let (ok, n) = fill("p1", &claim(5), "fill", 24)?;
    ensure!(ok, "pv-05 (32 MiB) refused 24 MiB ({n} written)");
    let (ok, n) = fill("p1", &claim(5), "fill2", 16)?;
    eprintln!("   pv-05: ENOSPC={} after {n} more bytes", !ok);
    ensure!(
        !ok && n <= (8 << 20) + SLACK,
        "pv-05 (32 MiB, 24 used) took {n} bytes of 16 more without ENOSPC"
    );
    let floor = (8u64 << 20).saturating_sub(tree_bytes(&models[5]) + SLACK);
    ensure!(
        n >= floor,
        "pv-05 (32 MiB, 24 used) refused at {n} more bytes, below {floor}"
    );
    eprintln!("   quotas: pv-00 full at 16 MiB, pv-01 beside it took 24 MiB, pv-05 full at 32 MiB");
    // The refusals left every tree intact.
    s.exec(
        "p0",
        &format!(
            "rm {}/fill {}/fill",
            data_dir(&claim(0)),
            data_dir(&claim(1))
        ),
    )?;
    s.exec(
        "p1",
        &format!("rm {d}/fill {d}/fill2", d = data_dir(&claim(5))),
    )?;
    verify_all(&models)?;
    // Unstaging all 50 leaves both engine pods annotated idle.
    let names: Vec<&str> = pods.iter().map(|(n, _, _)| n.as_str()).collect();
    s.delete_pods(&names)?;
    eventually(
        "both engine pods annotated with 0 views",
        Duration::from_secs(120),
        || {
            let e = s.engine_pods(&fs, "node")?;
            ensure!(
                e.len() == 2 && e.iter().all(|p| p.views == Some(0)),
                "{e:?}"
            );
            Ok(())
        },
    )?;
    s.finish()
}

/// What each line of the writer's log and each of its files holds.
fn writer_model(tag: &str, n: u64) -> Model {
    let mut m = Model::default();
    m.mkdir(Path::new("w"));
    let mut log = Vec::new();
    for i in 0..n {
        let line = format!("{tag}-{i}\n");
        let data: Vec<u8> = line.bytes().cycle().take(4096).collect();
        m.write_file(&Path::new("w").join(format!("f-{i}")), data);
        log.extend_from_slice(line.as_bytes());
    }
    m.write_file(Path::new("w/log"), log);
    m
}

/// A writer that runs until `/tmp/stop`: each iteration writes a 4 KiB
/// file, appends a log line and reads the file back, recording any failure
/// in `/tmp/errors`; `/tmp/count` is its progress, `/tmp/final` its total.
fn writer_script(claim: &str, tag: &str) -> String {
    format!(
        r#"d={d}/w
mkdir -p $d || echo mkdir >> /tmp/errors
i=0
while [ ! -e /tmp/stop ]; do
  if ! yes "{tag}-$i" | head -c 4096 > $d/f-$i; then echo "write $i" >> /tmp/errors; fi
  if ! echo "{tag}-$i" >> $d/log; then echo "append $i" >> /tmp/errors; fi
  want=$(yes "{tag}-$i" | head -c 4096 | sha256sum)
  if ! got=$(sha256sum < $d/f-$i); then echo "read $i" >> /tmp/errors; fi
  [ "$got" = "$want" ] || echo "readback $i" >> /tmp/errors
  i=$((i+1))
  echo $i > /tmp/count
  sleep 0.1
done
echo $i > /tmp/final
exec sleep infinity
"#,
        d = data_dir(claim)
    )
}

/// A reader on the other node: reads the writer's log every 200 ms until
/// `/tmp/stop`, recording failures (not lag: the log may be shorter than
/// the writer's) in `/tmp/errors`.
fn reader_script(claim: &str) -> String {
    format!(
        r#"d={d}/w
n=0
while [ ! -e /tmp/stop ]; do
  if [ -e $d/log ] && ! cat $d/log > /dev/null; then echo "read log" >> /tmp/errors; fi
  n=$((n+1)); echo $n > /tmp/count
  sleep 0.2
done
exec sleep infinity
"#,
        d = data_dir(claim)
    )
}

fn count(s: &Scope, pod: &str) -> Result<u64> {
    let out = s.exec(pod, "cat /tmp/count 2>/dev/null || echo 0")?;
    Ok(out.trim().parse().unwrap_or(0))
}

/// The pods of a driver component, `(name, uid, ready)`.
fn component_pods(env: &Env, component: &str) -> Result<Vec<(String, String, bool)>> {
    let list = env.kube.get(&[
        "pods",
        "-n",
        &env.driver_ns,
        "-l",
        &format!("app.kubernetes.io/component={component}"),
    ])?;
    Ok(list["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["metadata"]["deletionTimestamp"].is_null())
        .map(|p| {
            let ready = p["status"]["conditions"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|c| c["type"] == "Ready" && c["status"] == "True");
            (
                p["metadata"]["name"].as_str().unwrap_or_default().into(),
                p["metadata"]["uid"].as_str().unwrap_or_default().into(),
                ready,
            )
        })
        .collect())
}

/// Plan 37 §12: CSI plugin (node/controller) restart — mounts survive.
fn csi_plugin_restart_survives(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let tag = format!("s{seed}");
    let mut s = Scope::new(env, "csi-plugin-restart")?;
    s.pvc("vol", "1Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    let fs = fs_uuid_of(&s.volume_handle("vol")?)?;
    s.spawn_pod("writer", w1, &["vol"], Some(&writer_script("vol", &tag)))?;
    s.wait_ready("writer", Duration::from_secs(300))?;
    s.spawn_pod("reader", w2, &["vol"], Some(&reader_script("vol")))?;
    s.wait_ready("reader", Duration::from_secs(300))?;
    eventually("the writer under way", Duration::from_secs(60), || {
        ensure!(
            count(&s, "writer")? >= 20,
            "writer not at 20 iterations yet"
        );
        Ok(())
    })?;
    let engines_before = s.engine_pods(&fs, "node")?;
    ensure!(
        engines_before.len() == 2,
        "expected the pool's engine pods on {w1} and {w2}: {engines_before:?}"
    );

    // Every node plugin and every controller replica, at once.
    let nodes_before = component_pods(env, "node")?;
    let ctls_before = component_pods(env, "controller")?;
    let at_kill = count(&s, "writer")?;
    for component in ["node", "controller"] {
        env.kube.run(&[
            "delete",
            "pods",
            "-n",
            &env.driver_ns,
            "-l",
            &format!("app.kubernetes.io/component={component}"),
            "--wait=false",
        ])?;
    }
    eprintln!(
        "   deleted {} node-plugin and {} controller pods with the writer at {at_kill}",
        nodes_before.len(),
        ctls_before.len()
    );
    for (component, before) in [("node", &nodes_before), ("controller", &ctls_before)] {
        eventually(
            &format!("every {component} pod replaced and Ready"),
            Duration::from_secs(240),
            || {
                let now = component_pods(env, component)?;
                ensure!(
                    now.len() == before.len()
                        && now
                            .iter()
                            .all(|(_, uid, ready)| *ready && before.iter().all(|b| &b.1 != uid)),
                    "{component} pods: {now:?}"
                );
                Ok(())
            },
        )?;
    }
    let at_ready = count(&s, "writer")?;
    eprintln!("   all replaced and Ready; the writer went on: {at_kill} -> {at_ready}");
    eventually("the writer going on", Duration::from_secs(60), || {
        ensure!(count(&s, "writer")? >= at_ready + 20, "writer stalled");
        Ok(())
    })?;
    let reads = count(&s, "reader")?;
    eventually("the reader going on", Duration::from_secs(60), || {
        ensure!(count(&s, "reader")? >= reads + 10, "reader stalled");
        Ok(())
    })?;

    // The restarted controller provisions; the restarted plugins publish
    // (a second pod on w1: the volume is staged there already) and stage
    // (a new PV on w2).
    s.pvc("after", "64Mi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("late-w1", w1, &["vol"])?;
    s.pod("late-w2", w2, &["after"])?;
    s.exec("late-w2", &format!("echo after > {}/x", data_dir("after")))?;

    for pod in ["writer", "reader"] {
        s.exec(pod, "touch /tmp/stop")?;
    }
    let n: u64 = {
        let mut n = 0;
        eventually("the writer stopped", Duration::from_secs(60), || {
            let out = s.exec("writer", "cat /tmp/final")?;
            n = out.trim().parse()?;
            Ok(())
        })?;
        n
    };
    for pod in ["writer", "reader"] {
        let errors = s.exec(pod, "cat /tmp/errors 2>/dev/null || true")?;
        ensure!(
            errors.trim().is_empty(),
            "{pod} saw I/O errors across the restart:\n{errors}"
        );
    }
    let model = writer_model(&tag, n);
    verify(&s, "writer", "vol", &model)?;
    verify(&s, "late-w1", "vol", &model)?;
    converge(&s, "reader", "vol", &model)?;
    eprintln!("   {n} iterations, zero errors on {w1} and {w2}; every view matches the model");

    // The mounts were never re-made: the engine pods holding the FUSE
    // sessions are the same incarnations, never restarted.
    let engines_after = s.engine_pods(&fs, "node")?;
    let key = |v: &[EnginePod]| -> Vec<(String, String, u64)> {
        let mut k: Vec<_> = v
            .iter()
            .map(|p| (p.node.clone(), p.uid.clone(), p.restarts))
            .collect();
        k.sort();
        k
    };
    ensure!(
        key(&engines_before) == key(&engines_after),
        "the engine pods changed across the plugin restart: {engines_before:?} -> {engines_after:?}"
    );
    eprintln!("   engine pods unchanged (same uids, no restarts)");
    s.finish()
}

/// `pod`'s view of `dir` equals `model` now.
fn verify_dir(s: &Scope, pod: &str, dir: &str, model: &Model) -> Result<()> {
    let seen = s.listing(pod, dir)?;
    model
        .verify_observed(&seen)
        .with_context(|| format!("{pod}'s view of {dir}"))
}

/// A `dataSource` naming VolumeSnapshot `name`.
fn snapshot_source(name: &str) -> serde_json::Value {
    serde_json::json!({"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": name})
}

/// A `dataSource` naming PVC `name` (a clone).
fn pvc_source(name: &str) -> serde_json::Value {
    serde_json::json!({"kind": "PersistentVolumeClaim", "name": name})
}

/// The engine's `clone.create` latency the controller logged for the
/// volume at `subtree`, in ms (`cloned (metadata only) … subtree=… ms=…`).
fn logged_clone_ms(log: &str, subtree: &str) -> Option<u64> {
    let plain = strip_ansi(log);
    plain
        .lines()
        .filter(|l| l.contains("cloned (metadata only)"))
        .filter(|l| {
            l.split_whitespace().any(|w| {
                w.strip_prefix("subtree=")
                    .is_some_and(|v| v.trim_matches('"') == subtree)
            })
        })
        .find_map(|l| {
            l.split_whitespace()
                .find_map(|w| w.strip_prefix("ms=")?.parse().ok())
        })
}

pub(super) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The volume subtree of a pool handle (`/volumes/<pv>`).
fn subtree_of(handle: &str) -> Result<String> {
    handle
        .find("/volumes/")
        .map(|i| handle[i..].to_string())
        .with_context(|| format!("not a pool volume handle: {handle:?}"))
}

/// Plan 37 K4 gate: snapshot → clone → mount.
fn csi_snapshot_clone_mount(env: &Env, seed: u64) -> Result<()> {
    const BIG_MIB: u64 = 64;
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "csi-snap-clone")?;
    s.pvc("src", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    let src_handle = s.volume_handle("src")?;
    let src_fs = fs_uuid_of(&src_handle)?;
    s.pod("writer", w1, &["src"])?;
    // A seeded tree under tree/ (the model's), and one big file beside it,
    // so a clone that copied data would have something to copy.
    let tree = |claim: &str| format!("{}/tree", data_dir(claim));
    s.exec("writer", &format!("mkdir -p {}", tree("src")))?;
    let mut model = Model::default();
    let mut w = RemoteWorkload::new(seed, "src");
    for block in 0..3 {
        let script = w.block(&tree("src"), &mut model, 30);
        s.exec("writer", &script)
            .with_context(|| format!("block {block}"))?;
    }
    let big_sum = s
        .exec(
            "writer",
            &format!(
                "set -eu; cd {d}; head -c {n} /dev/urandom > big; sync; sha256sum big | cut -d' ' -f1",
                d = data_dir("src"),
                n = BIG_MIB << 20
            ),
        )?
        .trim()
        .to_string();
    verify_dir(&s, "writer", &tree("src"), &model)?;
    eprintln!(
        "   src on {w1}: {} entries and a {BIG_MIB} MiB file",
        model.nodes.len()
    );

    s.volume_snapshot("snap", "src")?;
    let (handle, restore_size) = s.wait_snapshot_ready("snap", Duration::from_secs(300))?;
    ensure!(
        handle.starts_with(&format!("{src_handle}@snapshot-")),
        "snapshot handle {handle:?} is not <source volume>@snapshot-<uid>"
    );
    ensure!(
        restore_size >= BIG_MIB << 20,
        "restoreSize {restore_size} is below the {BIG_MIB} MiB the volume held"
    );
    eprintln!("   snapshot {handle}: ready, restoreSize {restore_size}");
    let at_snapshot = model.clone();
    // The source moves on; the snapshot must not.
    let script = w.block(&tree("src"), &mut model, 20);
    s.exec("writer", &script)?;
    s.exec("writer", &format!("rm {}/big", data_dir("src")))?;
    verify_dir(&s, "writer", &tree("src"), &model)?;
    let now = model.clone();

    let t0 = std::time::Instant::now();
    s.pvc_from("restored", "1Gi", None, snapshot_source("snap"))?;
    s.pvc_from("clone", "1Gi", None, pvc_source("src"))?;
    s.wait_bound_claims(&["restored", "clone"], Duration::from_secs(300))?;
    let bound = t0.elapsed();
    let mut clone_ms = Vec::new();
    for claim in ["restored", "clone"] {
        let h = s.volume_handle(claim)?;
        ensure!(
            fs_uuid_of(&h)? == src_fs,
            "{claim} ({h}) is not in the source's pool filesystem {src_fs}"
        );
        let subtree = subtree_of(&h)?;
        // Metadata only: the driver cloned with the engine's clone.create,
        // and it took no time a 64 MiB copy would.
        let log = s.controller_log()?;
        let ms = logged_clone_ms(&log, &subtree)
            .with_context(|| format!("no `cloned (metadata only)` log line for {subtree}"))?;
        ensure!(ms < 10_000, "clone.create of {claim} took {ms} ms");
        clone_ms.push(ms);
    }
    eprintln!(
        "   restored + clone Bound {bound:.1?} after creation; clone.create took {clone_ms:?} ms"
    );

    s.pod("reader", w2, &["restored", "clone"])?;
    let wait = |claim: &str, model: &Model| {
        eventually(
            &format!("reader's view of {claim} matches the model"),
            CONVERGE,
            || verify_dir(&s, "reader", &tree(claim), model),
        )
    };
    wait("restored", &at_snapshot)?;
    wait("clone", &now)?;
    let sums = s.exec(
        "reader",
        "cd /data; sha256sum restored/big | cut -d' ' -f1; [ ! -e clone/big ] && echo absent",
    )?;
    let lines: Vec<&str> = sums.lines().map(str::trim).collect();
    ensure!(
        lines == [big_sum.as_str(), "absent"],
        "big file: restored {lines:?}, expected [{big_sum}, absent]"
    );
    eprintln!(
        "   on {w2}: restored = the tree at the snapshot (big file intact), clone = the tree now"
    );

    // Independent copies: writes to the clone and the restore stay there.
    let mut clone_model = now.clone();
    let mut wc = RemoteWorkload::new(seed ^ 0x5eed, "cl");
    let script = wc.block(&tree("clone"), &mut clone_model, 30);
    s.exec("reader", &script)?;
    s.exec("reader", &format!("rm -r {}/*", tree("restored")))?;
    verify_dir(&s, "reader", &tree("clone"), &clone_model)?;
    verify_dir(&s, "reader", &tree("restored"), &Model::default())?;
    verify_dir(&s, "writer", &tree("src"), &now)?;
    eprintln!("   writes to the clone and the restore left the source as it was");

    // Deleting the VolumeSnapshot must remove the engine's snapshot (the
    // hold released, then deleted): asked of the engine itself, not of the
    // content object. The clone's transient snapshot must be gone too.
    let mine = |rows: &[serde_json::Value]| -> Vec<String> {
        rows.iter()
            .filter(|r| {
                r["name"]
                    .as_str()
                    .is_some_and(|n| n.starts_with("snapshot-") || n.starts_with("csi-clone-"))
            })
            .map(|r| r.to_string())
            .collect()
    };
    let rows = s.engine_snapshots(&src_fs)?;
    ensure!(
        mine(&rows).len() == 1,
        "before the delete the engine holds exactly the VolumeSnapshot's snapshot: {rows:?}"
    );
    s.kube().run(&[
        "delete",
        "volumesnapshot",
        "snap",
        "-n",
        &s.ns,
        "--wait=true",
        "--timeout=180s",
    ])?;
    eventually(
        "the engine's snapshot is gone",
        Duration::from_secs(180),
        || {
            let left = mine(&s.engine_snapshots(&src_fs)?);
            ensure!(left.is_empty(), "engine snapshot rows left: {left:?}");
            Ok(())
        },
    )?;
    eprintln!("   VolumeSnapshot deleted: the engine's snapshot row is gone");
    s.expect_no_engine_return();
    s.finish()
}

/// Plan 37 K4 gate: clone/restore within a pool, and refusal across pools.
fn csi_clone_cross_pool_refused(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "csi-clone-xpool")?;
    let other = s.add_class("other")?;
    s.pvc("src", "1Gi", false)?;
    // A volume of the other pool, so that pool's filesystem exists.
    s.pvc_of_class("other-vol", "1Gi", false, &other)?;
    s.wait_bound(Duration::from_secs(300))?;
    let src_handle = s.volume_handle("src")?;
    let src_fs = fs_uuid_of(&src_handle)?;
    let other_fs = fs_uuid_of(&s.volume_handle("other-vol")?)?;
    ensure!(
        src_fs != other_fs,
        "the two classes share filesystem {src_fs}"
    );

    s.pod("writer", w1, &["src"])?;
    let mut model = Model::default();
    let mut w = RemoteWorkload::new(seed, "x");
    let script = w.block(&data_dir("src"), &mut model, 40);
    s.exec("writer", &script)?;
    verify(&s, "writer", "src", &model)?;
    s.volume_snapshot("snap", "src")?;
    s.wait_snapshot_ready("snap", Duration::from_secs(300))?;

    // Within the pool: both bind, in the source's filesystem, with its data.
    s.pvc_from("clone-in", "1Gi", None, pvc_source("src"))?;
    s.pvc_from("restore-in", "1Gi", None, snapshot_source("snap"))?;
    s.wait_bound_claims(&["clone-in", "restore-in"], Duration::from_secs(300))?;
    for claim in ["clone-in", "restore-in"] {
        let h = s.volume_handle(claim)?;
        ensure!(
            fs_uuid_of(&h)? == src_fs,
            "{claim} ({h}) left the source's pool"
        );
    }
    s.pod("reader", w2, &["clone-in", "restore-in"])?;
    converge(&s, "reader", "clone-in", &model)?;
    converge(&s, "reader", "restore-in", &model)?;
    eprintln!(
        "   within the pool: clone and restore Bound in {src_fs}, both hold the source's tree"
    );

    // Across pools: refused, and they stay Pending.
    s.pvc_from("clone-x", "1Gi", Some(&other), pvc_source("src"))?;
    s.pvc_from("restore-x", "1Gi", Some(&other), snapshot_source("snap"))?;
    for claim in ["restore-x", "clone-x"] {
        let mut why = String::new();
        eventually(
            &format!("{claim} refused with InvalidArgument"),
            Duration::from_secs(180),
            || {
                let warnings = s.pvc_warnings(claim)?;
                match warnings
                    .iter()
                    .find(|m| m.contains("InvalidArgument") && m.contains(&src_fs))
                {
                    Some(m) => {
                        why = m.clone();
                        Ok(())
                    }
                    None => bail!("warnings so far: {warnings:?}"),
                }
            },
        )?;
        ensure!(
            why.contains(&other_fs),
            "{claim}'s refusal does not name the destination filesystem {other_fs}: {why}"
        );
        ensure!(
            s.pvc_phase(claim)? == "Pending",
            "{claim} is not Pending after its refusal"
        );
        eprintln!(
            "   {claim}: refused: {}",
            why.chars().take(300).collect::<String>()
        );
    }
    s.finish()
}

/// An engine pod's own filesystem entry in `fs.list`: (generation set by
/// `fs.unlock`, generation its S3 clients last signed with).
fn credential_generations(env: &Env, pod: &str) -> Result<(u64, u64)> {
    let listing = env
        .kube
        .engine_call(&env.driver_ns, pod, "fs.list", serde_json::json!({}))?;
    let own = listing["filesystems"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|f| f["name"].is_null())
        .with_context(|| format!("{pod} lists no filesystem of its own"))?;
    Ok((
        own["credentials_generation"].as_u64().unwrap_or(0),
        own["credentials_in_use"].as_u64().unwrap_or(0),
    ))
}

/// An engine pod's audit log (its state dir's `control-audit.jsonl`).
fn audit_log(env: &Env, pod: &str) -> Result<Vec<serde_json::Value>> {
    let out = env.kube.run(&[
        "exec",
        "-n",
        &env.driver_ns,
        pod,
        "-c",
        "engine",
        "--",
        "cat",
        "/var/lib/constellation/state/control-audit.jsonl",
    ])?;
    out.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).context("parsing an audit line"))
        .collect()
}

/// Whether `haystack` holds the rotating Secret's `generation`th pair —
/// said without ever printing either value.
fn holds_pair(haystack: &str, generation: u32) -> bool {
    haystack.contains(&rotating_key(generation))
        || haystack.contains(&format!("k8s-harness-rotation-secret-{generation}"))
}

/// Whether any file under the workers' hostRoot holds a key id or secret
/// of the rotating Secret's `generations` (named, never printed).
fn host_root_holds_no_pair(
    workers: &[&String],
    generations: std::ops::RangeInclusive<u32>,
) -> Result<()> {
    for w in workers {
        for g in generations.clone() {
            let found = docker(&[
                "exec",
                w,
                "sh",
                "-c",
                &format!(
                    "grep -rlF -e {} -e k8s-harness-rotation-secret-{g} /var/lib/constellation-csi \
                     2>/dev/null | head -n 3; true",
                    rotating_key(g)
                ),
            ])?;
            ensure!(
                found.trim().is_empty(),
                "files under {w}'s hostRoot hold generation {g}'s key pair: {}",
                found.trim()
            );
        }
    }
    Ok(())
}

/// Core dumps are off (plan 37 K6a review) in every engine pod of
/// `engines` and in both plugins: `RLIMIT_CORE` 0 for each one's PID 1,
/// and the unprivileged ones are not dumpable (the kernel makes a
/// non-dumpable process's `/proc` entries root's, as their own uid sees).
fn no_core_dumps(env: &Env, engines: &[EnginePod]) -> Result<()> {
    let ns = env.driver_ns.as_str();
    let mut checked: Vec<(String, &str, bool)> = engines
        .iter()
        .map(|e| (e.name.clone(), "engine", true))
        .collect();
    for (component, unprivileged) in [("node", false), ("controller", true)] {
        let pods = env.kube.run(&[
            "get",
            "pods",
            "-n",
            ns,
            "-l",
            &format!("app.kubernetes.io/component={component}"),
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ])?;
        for pod in pods.split_whitespace() {
            checked.push((pod.to_string(), "constellation-csi", unprivileged));
        }
    }
    for (pod, container, unprivileged) in &checked {
        let out = env.kube.run(&[
            "exec",
            "-n",
            ns,
            pod,
            "-c",
            container,
            "--",
            "sh",
            "-c",
            "grep '^Max core file size' /proc/1/limits; stat -c %u /proc/1/status",
        ])?;
        let mut lines = out.lines();
        let limits: Vec<&str> = lines.next().unwrap_or("").split_whitespace().collect();
        ensure!(
            limits.get(4..6) == Some(&["0", "0"][..]),
            "{pod}: core dumps are on ({limits:?})"
        );
        let owner = lines.next().unwrap_or("").trim();
        ensure!(
            !unprivileged || owner == "0",
            "{pod}: its PID 1 is dumpable (/proc/1 owned by uid {owner})"
        );
    }
    Ok(())
}

/// Plan 37 §12 / K6a: `Refreshing(callback)` — the Secret rotates under a
/// writing pod and nothing is remounted.
fn csi_secret_rotation(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let tag = format!("r{seed}");
    let ns = env.driver_ns.as_str();
    // Back to the first pair, whatever an earlier run on this cluster left.
    env.kube.apply(&rotating_secret(ns, 1))?;
    let mut s = Scope::with_class(
        env,
        "csi-secret-rotation",
        ROTATING_SECRET,
        &[
            ("credentialSource", "refreshing".into()),
            ("credentialSecretName", ROTATING_SECRET.into()),
            ("credentialSecretNamespace", ns.into()),
        ],
    )?;
    s.pvc("vol", "1Gi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    let handle = s.volume_handle("vol")?;
    let fs = fs_uuid_of(&handle)?;
    s.spawn_pod("writer", w1, &["vol"], Some(&writer_script("vol", &tag)))?;
    s.wait_ready("writer", Duration::from_secs(300))?;
    s.spawn_pod("reader", w2, &["vol"], Some(&reader_script("vol")))?;
    s.wait_ready("reader", Duration::from_secs(300))?;
    eventually("the writer under way", Duration::from_secs(60), || {
        ensure!(
            count(&s, "writer")? >= 20,
            "writer not at 20 iterations yet"
        );
        Ok(())
    })?;
    let nodes = s.engine_pods(&fs, "node")?;
    ensure!(
        nodes.len() == 2,
        "expected engine pods on {w1} and {w2}: {nodes:?}"
    );
    let ctl = s.engine_pods(&fs, "controller")?;
    ensure!(
        ctl.len() == 1,
        "controller engine pods of the pool: {ctl:?}"
    );
    let engines: Vec<EnginePod> = nodes.iter().chain(ctl.iter()).cloned().collect();
    let mounts_before = [
        s.staging_mount_id(w1, &handle)?,
        s.staging_mount_id(w2, &handle)?,
    ];
    ensure!(
        mounts_before.iter().all(Option::is_some),
        "the volume is not staged on both workers: {mounts_before:?}"
    );
    let mut generation: Vec<u64> = Vec::new();
    for e in &engines {
        let (set, used) = credential_generations(env, &e.name)?;
        ensure!(set >= 1, "{} was never unlocked (generation {set})", e.name);
        generation.push(set);
        eprintln!("   {}: credentials generation {set}, in use {used}", e.name);
    }

    // §9: nothing outside the engines' memory holds a credential — not a
    // pod spec, an environment, a log line, nor a file under hostRoot.
    for e in &engines {
        let spec = env
            .kube
            .run(&["get", "pod", "-n", ns, &e.name, "-o", "json"])?;
        ensure!(
            !holds_pair(&spec, 1),
            "{}'s pod spec holds the key pair",
            e.name
        );
        let log = env.kube.run(&["logs", "-n", ns, &e.name, "-c", "engine"])?;
        ensure!(!holds_pair(&log, 1), "{}'s log holds the key pair", e.name);
    }
    // The engine processes' environments, read on the nodes as root: the
    // engines are not dumpable, so not even their own uid may read them
    // (`no_core_dumps`).
    for w in [w1, w2] {
        let environ = docker(&[
            "exec",
            w,
            "sh",
            "-c",
            "for d in /proc/[0-9]*; do \
               if tr '\\0' ' ' < $d/cmdline 2>/dev/null | grep -q '^/usr/local/bin/constellation serve '; \
               then echo \"pid ${d#/proc/}\"; cat $d/environ; echo; fi; \
             done",
        ])?;
        ensure!(
            environ.contains("pid "),
            "no engine process found on {w} to check its environment"
        );
        ensure!(
            !holds_pair(&environ, 1),
            "an engine process on {w} has the key pair in its environment"
        );
    }
    host_root_holds_no_pair(&[w1, w2], 1..=1)?;
    eprintln!("   no pod spec, environment, engine log or hostRoot file holds the key pair");
    no_core_dumps(env, &engines)?;
    eprintln!("   core dumps are off in every engine pod and both plugins");

    for round in 2..=3u32 {
        let at = count(&s, "writer")?;
        env.kube.apply(&rotating_secret(ns, round))?;
        let want: Vec<u64> = generation.iter().map(|g| g + 1).collect();
        eventually(
            &format!("rotation {round} reached every engine pod's S3 clients"),
            Duration::from_secs(180),
            || {
                for (e, want) in engines.iter().zip(&want) {
                    let (set, used) = credential_generations(env, &e.name)?;
                    ensure!(
                        set == *want && used == set,
                        "{}: generation {set} (want {want}), in use {used}",
                        e.name
                    );
                }
                Ok(())
            },
        )?;
        generation = want;
        eventually("the writer going on", Duration::from_secs(60), || {
            ensure!(count(&s, "writer")? >= at + 10, "writer stalled");
            Ok(())
        })?;
        eprintln!(
            "   rotation {round}: every engine signs with it (generations {generation:?}); writer at {}",
            count(&s, "writer")?
        );
    }

    // The pushes, in each node engine's audit log: `fs.unlock`, granted
    // to the node plugin's service principal; and the stage's `view.mount`
    // under the same principal, attributed to the PV.
    let pv = s.kube().run(&[
        "get",
        "pvc",
        "-n",
        &s.ns,
        "vol",
        "-o",
        "jsonpath={.spec.volumeName}",
    ])?;
    let pv = pv.trim().to_string();
    for e in &nodes {
        let lines = audit_log(env, &e.name)?;
        let service = |r: &serde_json::Value| {
            r["principal"]["kind"] == "service"
                && r["principal"]["uid"] == 0
                && r["principal"]["label"] == "csi-node-plugin"
        };
        let unlocks: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|r| r["method"] == "fs.unlock" && r["outcome"] == "ok")
            .collect();
        ensure!(
            unlocks.len() >= 3 && unlocks.iter().all(|r| service(r)),
            "{}: {} successful fs.unlock lines (want the start and two rotations), all by \
             the csi-node-plugin service principal: {unlocks:?}",
            e.name,
            unlocks.len()
        );
        // The watch's pushes name the Secret they came from.
        let from_secret = format!("secret:{ns}/{ROTATING_SECRET}");
        ensure!(
            unlocks
                .iter()
                .filter(|r| r["on_behalf_of"] == from_secret.as_str())
                .count()
                >= 2,
            "{}: fewer than two fs.unlock lines on behalf of {from_secret}: {unlocks:?}",
            e.name
        );
        ensure!(
            unlocks
                .iter()
                .all(|r| r["params_digest"] == "withheld:secret-params"),
            "{}: an fs.unlock line carries a params digest",
            e.name
        );
        ensure!(
            lines.iter().any(|r| r["method"] == "view.mount"
                && service(r)
                && r["on_behalf_of"] == pv.as_str()),
            "{}: no view.mount line by the csi-node-plugin service principal naming {pv}",
            e.name
        );
        let raw = serde_json::to_string(&lines)?;
        ensure!(
            !(1..=3).any(|g| holds_pair(&raw, g)),
            "{}'s audit log holds a credential",
            e.name
        );
        eprintln!(
            "   {}: audit log has {} fs.unlock by the csi-node-plugin service principal, view.mount for {pv}",
            e.name,
            unlocks.len()
        );
    }

    // The controller-owned engine pod: its relay is the csi-controller
    // service (the image's allowlist), for the pushes and for the CSI
    // calls, which name the PV.
    for e in &ctl {
        let lines = audit_log(env, &e.name)?;
        let service = |r: &serde_json::Value| {
            r["principal"]["kind"] == "service"
                && r["principal"]["uid"] == 65532
                && r["principal"]["label"] == "csi-controller"
        };
        ensure!(
            !lines.is_empty() && lines.iter().all(service),
            "{}: an audit line not by the csi-controller service principal: {lines:?}",
            e.name
        );
        ensure!(
            lines
                .iter()
                .any(|r| r["method"] != "fs.unlock" && r["on_behalf_of"] == pv.as_str()),
            "{}: no CSI call attributed to {pv}: {lines:?}",
            e.name
        );
        let unlocks = lines
            .iter()
            .filter(|r| r["method"] == "fs.unlock" && r["outcome"] == "ok")
            .count();
        ensure!(
            unlocks >= 3,
            "{}: {unlocks} successful fs.unlock lines (want the start and two rotations)",
            e.name
        );
        let raw = serde_json::to_string(&lines)?;
        ensure!(
            !(1..=3).any(|g| holds_pair(&raw, g)),
            "{}'s audit log holds a credential",
            e.name
        );
        eprintln!(
            "   {}: {} audit lines, all by the csi-controller service principal; {unlocks} fs.unlock",
            e.name,
            lines.len()
        );
    }
    // No pair of any generation on either worker's hostRoot.
    host_root_holds_no_pair(&[w1, w2], 1..=3)?;

    // Nor did a plugin log one, whatever it pushed.
    for component in ["node", "controller"] {
        let logs = env.kube.run(&[
            "logs",
            "-n",
            ns,
            "-l",
            &format!("app.kubernetes.io/component={component}"),
            "-c",
            "constellation-csi",
            "--tail=-1",
        ])?;
        ensure!(
            !(1..=3).any(|g| holds_pair(&logs, g)),
            "a {component} plugin's log holds a credential"
        );
    }

    // The controller provisions with the rotated pair.
    s.pvc("after", "64Mi", true)?;
    s.wait_bound(Duration::from_secs(300))?;

    for pod in ["writer", "reader"] {
        s.exec(pod, "touch /tmp/stop")?;
    }
    let mut n = 0;
    eventually("the writer stopped", Duration::from_secs(60), || {
        n = s.exec("writer", "cat /tmp/final")?.trim().parse()?;
        Ok(())
    })?;
    for pod in ["writer", "reader"] {
        let errors = s.exec(pod, "cat /tmp/errors 2>/dev/null || true")?;
        ensure!(
            errors.trim().is_empty(),
            "{pod} saw I/O errors across the rotations:\n{errors}"
        );
    }
    let model = writer_model(&tag, n);
    verify(&s, "writer", "vol", &model)?;
    converge(&s, "reader", "vol", &model)?;
    // No remount: the same staging mounts, the same engine incarnations.
    let mounts_after = [
        s.staging_mount_id(w1, &handle)?,
        s.staging_mount_id(w2, &handle)?,
    ];
    ensure!(
        mounts_before == mounts_after,
        "the staging mounts changed: {mounts_before:?} -> {mounts_after:?}"
    );
    let key = |v: &[EnginePod]| -> Vec<(String, String, u64)> {
        let mut k: Vec<_> = v
            .iter()
            .map(|p| (p.name.clone(), p.uid.clone(), p.restarts))
            .collect();
        k.sort();
        k
    };
    let after: Vec<EnginePod> = s
        .engine_pods(&fs, "node")?
        .into_iter()
        .chain(s.engine_pods(&fs, "controller")?)
        .collect();
    ensure!(
        key(&engines) == key(&after),
        "engine pods changed across the rotations: {engines:?} -> {after:?}"
    );
    eprintln!("   {n} iterations, zero errors; same staging mounts and engine incarnations");
    s.finish()
}

/// What PodSecurity `restricted` says of `pod` (its spec, re-created by a
/// server dry run in `ns`, a namespace that enforces `restricted`):
/// `None` when admitted, else the violations.
fn psa_violations(env: &Env, ns: &str, pod: &serde_json::Value) -> Result<Option<String>> {
    let mut clone = pod.clone();
    let name = pod["metadata"]["name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    clone["metadata"] = serde_json::json!({"name": name, "namespace": ns});
    clone["status"] = serde_json::Value::Null;
    if let Some(spec) = clone["spec"].as_object_mut() {
        // Not PodSecurity's business, and not present in that namespace.
        spec.remove("serviceAccountName");
        spec.remove("serviceAccount");
        spec.remove("nodeName");
        spec.remove("priorityClassName");
        spec.remove("priority");
    }
    match env.kube.run_stdin(
        &["create", "--dry-run=server", "-f", "-"],
        clone.to_string().as_bytes(),
        Duration::from_secs(60),
    ) {
        Ok(_) => Ok(None),
        Err(e) => {
            let msg = format!("{e:#}");
            match msg.split_once("violates PodSecurity") {
                Some((_, v)) => Ok(Some(v.trim().to_string())),
                None => bail!("dry run of {name} failed, but not for PodSecurity: {msg}"),
            }
        }
    }
}

/// Plan 37 §9 / K6 gate: the privilege split, as admission judges it.
fn csi_pod_security(env: &Env, _seed: u64) -> Result<()> {
    let w1 = &env.workers[0];
    let mut s = Scope::new(env, "csi-pod-security")?;
    s.pvc("vol", "64Mi", true)?;
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("p", w1, &["vol"])?;
    let fs = fs_uuid_of(&s.volume_handle("vol")?)?;
    let node_engine = s.engine_pods(&fs, "node")?;
    let ctl_engine = s.engine_pods(&fs, "controller")?;
    ensure!(
        node_engine.len() == 1 && ctl_engine.len() == 1,
        "engine pods: {node_engine:?} {ctl_engine:?}"
    );
    let probe_ns = format!("k8s-psa-{}", env.scope_id());
    env.kube.apply(&serde_json::json!({
        "apiVersion": "v1", "kind": "Namespace",
        "metadata": {"name": probe_ns, "labels": {
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": "latest",
        }}
    }))?;
    let judged = (|| -> Result<()> {
        // Admission resolves the default ServiceAccount before PodSecurity
        // sees the pod: wait for the namespace to have one.
        eventually(
            "the probe namespace's default ServiceAccount",
            Duration::from_secs(60),
            || {
                env.kube
                    .run(&["get", "serviceaccount", "default", "-n", &probe_ns])
                    .map(drop)
            },
        )?;
        let pods = env.kube.get(&["pods", "-n", &env.driver_ns])?;
        let mut seen = BTreeSet::new();
        for pod in pods["items"].as_array().into_iter().flatten() {
            if !pod["metadata"]["deletionTimestamp"].is_null() {
                continue;
            }
            let name = pod["metadata"]["name"].as_str().unwrap_or_default();
            let labels = &pod["metadata"]["labels"];
            let role = match (
                labels["app.kubernetes.io/component"].as_str(),
                labels["constellation.dev/owner"].as_str(),
            ) {
                (Some("controller"), _) => "controller",
                (Some("node"), _) => "node plugin",
                (Some("engine"), Some("controller")) => "controller-owned engine",
                (Some("engine"), Some("node")) => "node-owned engine",
                _ => continue,
            };
            let verdict = psa_violations(env, &probe_ns, pod)?;
            eprintln!(
                "   {role:<24} {name}: {}",
                verdict.as_deref().unwrap_or("admitted under restricted")
            );
            match role {
                "controller" | "controller-owned engine" => ensure!(
                    verdict.is_none(),
                    "{role} pod {name} is not PodSecurity-restricted: {verdict:?}"
                ),
                "node-owned engine" => {
                    let v = verdict.clone().unwrap_or_default();
                    // Its only violation: the hostPaths §7 needs
                    // ("restricted volume types (… hostPath)"; the message
                    // names every violated check).
                    ensure!(
                        v.contains("restricted volume types")
                            && v.contains("\"hostPath\"")
                            && !v.contains("privileged")
                            && !v.contains("host namespaces")
                            && !v.contains("hostPort")
                            && !v.contains("runAsUser")
                            && !v.contains("allowPrivilegeEscalation")
                            && !v.contains("unrestricted capabilities")
                            && !v.contains("runAsNonRoot")
                            && !v.contains("seccompProfile"),
                        "node-owned engine pod {name}: want only its hostPath volumes refused, got {v:?}"
                    );
                }
                _ => ensure!(
                    verdict.as_deref().is_some_and(|v| v.contains("privileged")),
                    "the node plugin {name} should be privileged: {verdict:?}"
                ),
            }
            seen.insert(role);
        }
        ensure!(seen.len() == 4, "did not judge every role (saw {seen:?})");
        Ok(())
    })();
    let _ = env
        .kube
        .run(&["delete", "namespace", &probe_ns, "--wait=false"]);
    judged?;
    s.finish()
}

/// Where the load pod's trio keeps its control files (the pod's own
/// `/tmp`, not the PV).
const LOAD_CTL: &str = "/tmp/load";

/// How long an image upgrade may take to roll onto the load's worker:
/// kubelet can take minutes to roll the node DaemonSet on a loaded host
/// (`tests/csi/k5-handoff.sh`'s `K5_ROLLOUT_S`).
const ROLLOUT: Duration = Duration::from_secs(900);

/// How long the trio goes on after the cutover: past the replacement's
/// first `fsync`s, which wait out the delegation the old engine held
/// (plan 37 K5 notes, "a restarted delegate's first writes").
const AFTER_CUTOVER: Duration = Duration::from_secs(10);

/// Plan 37 §8 step 6 / K5 gate: the client-visible pause's p99 bound.
const PAUSE_P99_MS: u64 = 2000;

/// `2026-10-03T11:04:42.123456Z` (tracing's UTC timestamps) in ms since
/// the epoch.
fn rfc3339_ms(ts: &str) -> Option<u64> {
    let ts = ts.strip_suffix('Z')?;
    let (date, time) = ts.split_once('T')?;
    let mut d = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut t = hms.splitn(3, ':').map(|p| p.parse::<i64>().ok());
    let (hh, mm, ss) = (t.next()??, t.next()??, t.next()??);
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let ms: i64 = format!("{frac:0<3}")[..3].parse().ok()?;
    u64::try_from(((days * 24 + hh) * 60 + mm) * 60 * 1000 + ss * 1000 + ms).ok()
}

/// A `Duration`'s `Debug` text (`342.1ms`, `1.35s`, `980µs`) in ms.
fn debug_duration_ms(text: &str) -> Option<f64> {
    for (suffix, scale) in [
        ("ns", 1e-6),
        ("µs", 1e-3),
        ("us", 1e-3),
        ("ms", 1.0),
        ("s", 1e3),
    ] {
        if let Some(n) = text.strip_suffix(suffix) {
            return n.parse::<f64>().ok().map(|v| v * scale);
        }
    }
    None
}

/// `key=value` of a tracing log line (the value unquoted).
fn log_field<'l>(line: &'l str, key: &str) -> Option<&'l str> {
    line.split_whitespace()
        .find_map(|w| w.strip_prefix(key)?.strip_prefix('='))
        .map(|v| v.trim_matches('"'))
}

/// What the node plugin logged about the handoff to `to`: the outcome line's
/// end (ms since the epoch) and its `elapsed`, and every line about the
/// same unit that is not a success.
#[derive(Debug, PartialEq)]
struct PluginHandoff {
    end_ms: u64,
    elapsed_ms: f64,
    unit: String,
    failed: Vec<String>,
}

fn plugin_handoff(log: &str, to: &str) -> Option<PluginHandoff> {
    let plain = strip_ansi(log);
    let line = plain
        .lines()
        .filter(|l| l.contains("engine-pod handoff succeeded"))
        .find(|l| log_field(l, "to") == Some(to))?;
    let unit = log_field(line, "unit")?.to_string();
    let failed = plain
        .lines()
        .filter(|l| l.contains("engine-pod handoff") && !l.contains("handoff succeeded"))
        .filter(|l| log_field(l, "unit") == Some(unit.as_str()))
        .map(str::to_string)
        .collect();
    Some(PluginHandoff {
        end_ms: rfc3339_ms(line.split_whitespace().next()?)?,
        elapsed_ms: debug_duration_ms(log_field(line, "elapsed")?)?,
        unit,
        failed,
    })
}

/// The node plugin on `node` running `image` (not a terminating
/// predecessor).
fn node_plugin_on(env: &Env, node: &str, image: &str) -> Result<Option<String>> {
    let list = env.kube.get(&[
        "pods",
        "-n",
        &env.driver_ns,
        "-l",
        "app.kubernetes.io/component=node",
        "--field-selector",
        &format!("spec.nodeName={node}"),
    ])?;
    Ok(list["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["metadata"]["deletionTimestamp"].is_null())
        .filter(|p| {
            p["spec"]["containers"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|c| c["name"] == "constellation-csi" && c["image"] == image)
        })
        .find_map(|p| p["metadata"]["name"].as_str().map(str::to_string)))
}

/// An engine pod's log (`""` while it has none to give).
fn engine_log(env: &Env, pod: &str) -> String {
    env.kube
        .run(&["logs", "-n", &env.driver_ns, pod, "-c", "engine"])
        .map(|l| strip_ansi(&l))
        .unwrap_or_default()
}

/// A pod's log followed from now on (`kubectl logs -f` into a file), so
/// what a pod said is still there after the pod is gone: the handoff's old
/// engine pod is deleted before the trio's outcome is known.
struct LogTap {
    pod: String,
    child: std::process::Child,
    file: PathBuf,
    _dir: tempfile::TempDir,
}

impl LogTap {
    fn start(env: &Env, pod: &str, container: &str) -> Result<LogTap> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("log");
        let out = std::fs::File::create(&file)?;
        let mut cmd = env.kube.kubectl_command();
        cmd.args(["logs", "-f", "-n", &env.driver_ns, pod, "-c", container])
            .stdin(std::process::Stdio::null())
            .stdout(out)
            .stderr(std::process::Stdio::null());
        let child = cmd.spawn().context("spawning kubectl logs -f")?;
        Ok(LogTap {
            pod: pod.to_string(),
            child,
            file,
            _dir: dir,
        })
    }

    /// The lines it logged between `from_ms` and `to_ms` (ms since the
    /// epoch), at most `max`.
    fn between(&self, from_ms: u64, to_ms: u64, max: usize) -> Vec<String> {
        self.matching(from_ms, to_ms, max, |_| true)
    }

    /// [`LogTap::between`], only the lines `keep` takes.
    fn matching(
        &self,
        from_ms: u64,
        to_ms: u64,
        max: usize,
        keep: impl Fn(&str) -> bool,
    ) -> Vec<String> {
        let text = std::fs::read_to_string(&self.file).unwrap_or_default();
        strip_ansi(&text)
            .lines()
            .filter(|l| {
                l.split_whitespace()
                    .next()
                    .and_then(rfc3339_ms)
                    .is_some_and(|t| (from_ms..=to_ms).contains(&t))
                    && keep(l)
            })
            .take(max)
            .map(|l| l.chars().take(300).collect())
            .collect()
    }
}

impl LogTap {
    /// Its `kubectl logs -f` ended without a line (the pod had not started,
    /// or the API refused): worth starting again.
    fn died_empty(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(Some(_)))
            && std::fs::metadata(&self.file).map_or(true, |m| m.len() == 0)
    }
}

/// Follow the engine container of every pod of `pods` not followed yet (a
/// tap that died empty is started again). A tap that cannot start is said,
/// never dropped silently.
fn tap_engines(env: &Env, taps: &mut Vec<LogTap>, pods: impl IntoIterator<Item = String>) {
    for pod in pods {
        if let Some(i) = taps.iter().position(|t| t.pod == pod) {
            if !taps[i].died_empty() {
                continue;
            }
            eprintln!("   following {pod}'s log ended with nothing; again");
            taps.remove(i);
        }
        match LogTap::start(env, &pod, "engine") {
            Ok(tap) => taps.push(tap),
            Err(e) => eprintln!("   cannot follow {pod}'s log: {e:#}"),
        }
    }
}

impl Drop for LogTap {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The running controller-owned engine pods of the driver's namespace: a
/// pool's root lease holder (a scope's own, as every scope deletes its
/// pools' engine pods at its end).
fn controller_engines(env: &Env) -> Vec<String> {
    env.kube
        .run(&[
            "get",
            "pods",
            "-n",
            &env.driver_ns,
            "-l",
            "app.kubernetes.io/component=engine,constellation.dev/owner=controller",
            "--field-selector=status.phase=Running",
            "-o",
            "name",
        ])
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim_start_matches("pod/").to_string())
        .collect()
}

/// The trio's files as they must be after `summary` (`fixed`, every record
/// of `appended`, `created/f0..`).
fn trio_model(summary: &constellation_pod_load::Summary) -> Model {
    use constellation_pod_load as load;
    let mut model = Model::default();
    model.mkdir(Path::new("created"));
    model.write_file(Path::new("fixed"), load::fixed_data(summary.seed));
    model.write_file(
        Path::new("appended"),
        load::appended_data(summary.seed, summary.appended),
    );
    for i in 0..summary.created {
        model.write_file(
            &Path::new("created").join(format!("f{i}")),
            load::created_data(summary.seed, i),
        );
    }
    model
}

/// A snapshot of the trio taken while it ran holds a state it went
/// through: every file one of `model`'s (the final one) cut at some
/// length — `fixed` whole (written before the trio started) — and nothing
/// else; and at least what was acknowledged before it was cut: the first
/// `records` records of `appended` (an `fsync` returned) and `created/f0`
/// .. `f<files - 1>` whole (a `close` returned). Returns the snapshot's
/// `appended` length.
fn check_trio_snapshot(
    seen: &BTreeMap<PathBuf, Observed>,
    model: &Model,
    records: u64,
    files: u64,
) -> Result<u64> {
    let mut appended = 0;
    for (rel, o) in seen {
        let node = model
            .nodes
            .get(rel)
            .with_context(|| format!("the snapshot holds {rel:?}, which the trio never made"))?;
        match (node, o) {
            (Node::Dir, Observed::Dir) => {}
            (Node::File { data }, Observed::File { size, sha256 }) => {
                let size = *size as usize;
                ensure!(
                    size <= data.len() && crate::model::sha256_of(&data[..size]) == *sha256,
                    "the snapshot's {rel:?} ({size} bytes) is not a prefix of what the trio wrote \
                     ({} bytes)",
                    data.len()
                );
                if rel == Path::new("fixed") {
                    ensure!(size == data.len(), "the snapshot's fixed is {size} bytes");
                }
                if rel == Path::new("appended") {
                    appended = size as u64;
                }
            }
            (n, o) => bail!("the snapshot's {rel:?} is {o:?}, the trio's is {n:?}"),
        }
    }
    ensure!(
        seen.contains_key(Path::new("fixed")),
        "the snapshot has no fixed"
    );
    let acked = records * constellation_pod_load::RECORD as u64;
    ensure!(
        appended >= acked,
        "the snapshot's appended is {appended} bytes; {records} records ({acked} bytes) were \
         fsynced before it was cut"
    );
    for i in 0..files {
        let rel = Path::new("created").join(format!("f{i}"));
        let whole = match (model.nodes.get(&rel), seen.get(&rel)) {
            (Some(Node::File { data }), Some(Observed::File { size, .. })) => {
                *size == data.len() as u64
            }
            _ => false,
        };
        ensure!(
            whole,
            "the snapshot's {rel:?} is {:?}; it was closed before the snapshot was cut",
            seen.get(&rel)
        );
    }
    Ok(appended)
}

/// Plan 37 K5's headline gate (§8, §12, §15): an engine-pod image upgrade
/// under `upgrade-under-load`'s writer, creator and reader is a pause,
/// never an error, and the pause stays under 2 s.
fn csi_engine_pod_handoff_under_load(env: &Env, seed: u64) -> Result<()> {
    use constellation_pod_load::{now_ms, Summary};
    const CLAIM: &str = "load";
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    // `static-ephemeral` (37-k6a): the upgraded node plugin holds no
    // credential, so the replacement must get the old pod's over the
    // handoff's `Credentials` step.
    let mut s = Scope::with_params(
        env,
        "csi-handoff",
        &[("credentialSource", "static-ephemeral".into())],
    )?;
    s.pvc(CLAIM, "8Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    let handle = s.volume_handle(CLAIM)?;
    let fs = fs_uuid_of(&handle)?;
    let trio = format!("{}/trio", data_dir(CLAIM));
    s.spawn_pod(
        "load",
        w1,
        &[CLAIM],
        Some(&format!(
            "constellation-pod-load run --dir {trio} --ctl {LOAD_CTL} --seed {seed} \
             --write-kib-s 512 --creates-s 10 --reads-s 10 --slow-ms 100; exec sleep infinity"
        )),
    )?;
    s.wait_ready("load", Duration::from_secs(300))?;
    let progress = |s: &Scope| -> Result<u64> {
        let out = s.exec(
            "load",
            &format!(
                "if [ -e {LOAD_CTL}/failed ]; then cat {LOAD_CTL}/failed; exit 3; fi; \
                 cat {LOAD_CTL}/progress 2>/dev/null || echo 0"
            ),
        )?;
        Ok(out.trim().parse().unwrap_or(0))
    };
    eventually("the trio under way", Duration::from_secs(120), || {
        ensure!(progress(&s)? >= 200, "fewer than 200 calls so far");
        Ok(())
    })?;
    let on_w1 = |s: &Scope| -> Result<Vec<EnginePod>> {
        Ok(s.engine_pods(&fs, "node")?
            .into_iter()
            .filter(|p| &p.node == w1)
            .collect())
    };
    let old = match on_w1(&s)?.as_slice() {
        [one] => one.clone(),
        other => bail!("expected one engine pod of {fs} on {w1}: {other:?}"),
    };
    let mount_before = s
        .staging_mount_id(w1, &handle)?
        .with_context(|| format!("{handle} is not staged on {w1}"))?;
    let target = if env.chart_image() == env.image {
        env.next_image()?
    } else {
        env.image.clone()
    };
    ensure!(
        old.image != target,
        "the engine pod {} already runs {target}",
        old.name
    );

    eprintln!(
        "   trio under way on {w1}; engine pod {} on {}; helm upgrade to {target}",
        old.name, old.image
    );
    // What the old engine pod and the pool's root say from here on, kept
    // for the verdict (the old pod is gone by then); a root recreated
    // meanwhile is followed from when it appears.
    let mut taps: Vec<LogTap> = Vec::new();
    tap_engines(env, &mut taps, [old.name.clone()]);
    tap_engines(env, &mut taps, controller_engines(env));
    let upgraded_ms = now_ms();
    env.set_chart_image(&target)?;
    // Watch the rollout reach the load's worker. A `VolumeSnapshot` is
    // asked for as soon as the replacement waits as a standby (the node
    // plugin starts the handoff right after), so it is usually cut during
    // or just after the handoff: when is measured, not enforced.
    let mut snapshot_asked = None;
    let mut new = None;
    eventually(
        &format!("an engine pod on {target} Ready on {w1}, {} gone", old.name),
        ROLLOUT,
        || {
            tap_engines(env, &mut taps, controller_engines(env));
            let pods = on_w1(&s)?;
            if snapshot_asked.is_none() {
                if let Some(r) = pods.iter().find(|p| p.name != old.name) {
                    if r.ready || engine_log(env, &r.name).contains("waiting as a handoff standby")
                    {
                        // Taken before the request: the cut comes after.
                        let asked = now_ms();
                        s.volume_snapshot("across", CLAIM)?;
                        snapshot_asked = Some(asked);
                    }
                }
            }
            let gone = env
                .kube
                .run(&[
                    "get",
                    "pod",
                    "-n",
                    &env.driver_ns,
                    &old.name,
                    "--ignore-not-found",
                    "-o",
                    "name",
                ])?
                .trim()
                .is_empty();
            match pods
                .iter()
                .find(|p| p.name != old.name && p.image == target && p.ready)
            {
                Some(p) if gone => {
                    new = Some(p.clone());
                    Ok(())
                }
                found => bail!("replacement {found:?}, {} gone: {gone}", old.name),
            }
        },
    )?;
    let new = new.expect("set when the wait ends");
    let rollout_s = (now_ms() - upgraded_ms) as f64 / 1000.0;
    eprintln!(
        "   {} -> {} in {rollout_s:.1} s after the upgrade",
        old.name, new.name
    );
    // `kubectl logs -f` starts at the container's first line: the
    // replacement's log is whole, and outlives its pod.
    tap_engines(env, &mut taps, [new.name.clone()]);

    // The handoff as the node plugin logged it: its window is prepare sent
    // to `Resumed` seen, an upper bound of the client-visible pause (plan
    // 37 K5 notes).
    let mut handoff = None;
    eventually(
        &format!("the node plugin on {w1} logged the handoff to {}", new.name),
        Duration::from_secs(60),
        || {
            let plugin = node_plugin_on(env, w1, &target)?
                .with_context(|| format!("no node plugin on {target} runs on {w1}"))?;
            let log = env.kube.run(&[
                "logs",
                "-n",
                &env.driver_ns,
                &plugin,
                "-c",
                "constellation-csi",
            ])?;
            handoff = Some(
                plugin_handoff(&log, &new.name)
                    .with_context(|| format!("{plugin} logged no handoff to {}", new.name))?,
            );
            Ok(())
        },
    )?;
    let handoff = handoff.expect("set when the wait ends");
    ensure!(
        handoff.failed.is_empty(),
        "the handoff of {} did not succeed at the first attempt:\n{}",
        handoff.unit,
        handoff.failed.join("\n")
    );
    let window = (
        handoff.end_ms - handoff.elapsed_ms.round() as u64,
        handoff.end_ms,
    );
    let new_log = engine_log(env, &new.name);
    ensure!(
        new_log.contains("handed-over credentials accepted"),
        "the replacement {} did not take its credentials from the handoff",
        new.name
    );
    let mount_after = s.staging_mount_id(w1, &handle)?;
    ensure!(
        mount_after.as_deref() == Some(mount_before.as_str()),
        "the staging mount changed: {mount_before} -> {mount_after:?} (remounted, not handed over)"
    );
    eprintln!(
        "   handed over in place (mount id {mount_before}); the plugin's window {:.0} ms",
        handoff.elapsed_ms
    );

    let at_cutover = progress(&s)?;
    std::thread::sleep(AFTER_CUTOVER);
    let after = progress(&s)?;
    ensure!(
        after >= at_cutover + 100,
        "the trio stalled after the cutover ({at_cutover} -> {after} calls)"
    );
    tap_engines(env, &mut taps, controller_engines(env));
    s.exec("load", &format!("touch {LOAD_CTL}/stop"))?;
    let mut summary = Summary::default();
    eventually("the trio's summary", Duration::from_secs(180), || {
        let out = s.exec("load", &format!("cat {LOAD_CTL}/summary.json"))?;
        summary = serde_json::from_str(&out).context("parsing the trio's summary")?;
        Ok(())
    })?;
    // The gated pause, as the workload saw it: its longest call caught by
    // the handoff (counted whole, so it can exceed the window; 0 when none
    // took `--slow-ms` 100).
    let pause_ms = summary.longest_within(window.0, window.1);
    let longest = summary
        .longest
        .clone()
        .unwrap_or(constellation_pod_load::SlowCall {
            op: "none".into(),
            start_ms: 0,
            ms: 0,
        });
    // A call caught by the handoff that outlasted its window waited on
    // something after the resume (37-k5a saw a restarted delegate's first
    // forwarded mutations wait out the delegation its predecessor held):
    // what the replacement and the pool's root (the controller-owned engine
    // pod) logged while it waited says what.
    if let Some(c) = summary
        .slow
        .iter()
        .find(|c| c.start_ms <= window.1 && c.start_ms + c.ms > window.1 + 1000)
    {
        eprintln!(
            "   {} (started {:+} ms, {} ms) outlasted the handoff's window by {} ms; what \
             the engines logged meanwhile:",
            c.op,
            c.start_ms as i64 - window.0 as i64,
            c.ms,
            c.start_ms + c.ms - window.1
        );
        let (from, to) = (c.start_ms.saturating_sub(500), c.start_ms + c.ms + 500);
        // The roots and the replacement (the old pod, first, is gone),
        // without the replacement's meta.db recovery, which would fill
        // the budget.
        let notable = |l: &str| !l.contains(" lsm_tree::") && !l.contains(" fjall::");
        for tap in taps.iter().skip(1) {
            for line in tap.matching(from, to, 40, notable) {
                eprintln!("     {}: {line}", tap.pod);
            }
        }
    }
    env.measure("handoff_ms", handoff.elapsed_ms.round());
    env.measure("pause_ms", pause_ms);
    env.measure("longest_call_ms", longest.ms);
    env.measure(
        "longest_call",
        format!(
            "{} at {:+} ms from the handoff's start",
            longest.op,
            longest.start_ms as i64 - window.0 as i64
        ),
    );
    env.measure("rollout_s", (rollout_s * 10.0).round() / 10.0);
    env.measure("calls", summary.calls);
    env.measure("errors", summary.error_count());
    if summary.error_count() > 0 {
        eprintln!("   what the old engine pod and the pool's root logged around the errors:");
        for e in summary.errors.iter().take(5) {
            eprintln!(
                "   - {} {} at {:+} ms:",
                e.op,
                e.code,
                e.at_ms as i64 - window.0 as i64
            );
            for tap in &taps {
                for line in tap.between(e.at_ms.saturating_sub(10_000), e.at_ms + 3_000, 60) {
                    eprintln!("     {}: {line}", tap.pod);
                }
            }
        }
        let first: Vec<String> = summary
            .errors
            .iter()
            .take(20)
            .map(|e| {
                format!(
                    "  {:+} ms {} {}: {}",
                    e.at_ms as i64 - window.0 as i64,
                    e.op,
                    e.code,
                    e.message
                )
            })
            .collect();
        bail!(
            "the trio saw {} error(s) {:?} (times from the handoff's start):\n{}",
            summary.error_count(),
            summary.errors_by_code,
            first.join("\n")
        );
    }
    let slow: Vec<String> = summary
        .slow
        .iter()
        .filter(|c| c.ms >= 500)
        .map(|c| {
            format!(
                "{} {}ms@{:+}",
                c.op,
                c.ms,
                c.start_ms as i64 - window.0 as i64
            )
        })
        .collect();
    eprintln!(
        "   trio: {} calls, 0 errors; {} records, {} files, {} reads; pause {pause_ms} ms \
         within the window, longest call {} ms ({}); calls >= 500 ms: {slow:?}",
        summary.calls, summary.appended, summary.created, summary.reads, longest.ms, longest.op
    );

    // Every acknowledged byte, read on the other worker (its own engine
    // pod) once the load's pod is gone and the volume unstaged there.
    s.delete_pods(&["load"])?;
    eventually(
        &format!("{handle} unstaged from {w1}"),
        Duration::from_secs(180),
        || {
            ensure!(!s.staged_on(w1, &handle)?, "still staged");
            Ok(())
        },
    )?;
    s.wait_snapshot_ready("across", Duration::from_secs(300))?;
    let asked = snapshot_asked.context("the rollout ended before a snapshot was asked for")?;
    // When the snapshot was cut, bracketed: not before the request, nor
    // before the snapshot controller made its content object (which the
    // CSI sidecar's `CreateSnapshot` waits for; whole seconds, rounded
    // down); not after the engine's snapshot row was made, after its
    // barrier (the content's `status.creationTime`, in ns).
    let content = s.snapshot_content("across")?;
    let cut_from = content["metadata"]["creationTimestamp"]
        .as_str()
        .and_then(rfc3339_ms)
        .map_or(asked, |t| t.max(asked));
    let cut_by = content["status"]["creationTime"]
        .as_u64()
        .context("the snapshot's content has no status.creationTime")?
        / 1_000_000;
    ensure!(
        cut_by >= cut_from,
        "the snapshot's row ({cut_by}) predates its request ({cut_from})"
    );
    // A `snapshot.create` that raced the handoff once waited out its 30 s
    // deadline and was cut on the sidecar's retry: what the engines said.
    if cut_by > asked + 5_000 {
        eprintln!(
            "   the snapshot was cut {} ms after it was asked for; what the engines logged \
             meanwhile about snapshots, or as warnings:",
            cut_by - asked
        );
        let notable = |l: &str| {
            l.contains("snapshot")
                || l.contains("barrier")
                || l.contains(" WARN ")
                || l.contains(" ERROR ")
        };
        for tap in &taps {
            for line in tap.matching(asked.saturating_sub(1_000), cut_by + 1_000, 60, notable) {
                eprintln!("     {}: {line}", tap.pod);
            }
        }
    }
    let (acked_records, acked_files) = summary.acked_before(cut_from);
    let rel = |t: u64| t as i64 - window.0 as i64;
    env.measure("snapshot_asked_ms", rel(asked));
    env.measure("snapshot_cut_from_ms", rel(cut_from));
    env.measure("snapshot_cut_by_ms", rel(cut_by));
    s.pvc_from("restored", "8Gi", None, snapshot_source("across"))?;
    s.wait_bound_claims(&["restored"], Duration::from_secs(300))?;
    s.pod("verify", w2, &[CLAIM, "restored"])?;
    let model = trio_model(&summary);
    verify_dir(&s, "verify", &trio, &model)
        .context("every acknowledged byte, read on the other worker")?;
    let seen = s.listing("verify", &format!("{}/trio", data_dir("restored")))?;
    let snap_records = check_trio_snapshot(&seen, &model, acked_records, acked_files)?
        / constellation_pod_load::RECORD as u64;
    env.measure("snapshot_records", snap_records);
    env.measure("snapshot_acked_records", acked_records);
    let span = |t: u64| -> String {
        if t < window.0 {
            format!("{} ms before", window.0 - t)
        } else if t <= window.1 {
            format!("{} ms into", t - window.0)
        } else {
            format!("{} ms after", t - window.1)
        }
    };
    eprintln!(
        "   on {w2}: the volume holds every byte the trio wrote; the snapshot, cut between {} \
         and {} the handoff's window, is a state it went through: {snap_records} of {} \
         records ({acked_records} fsynced before its earliest cut), every one of the \
         {acked_files} files closed by then",
        span(cut_from),
        span(cut_by),
        summary.appended
    );
    s.finish()
}

#[cfg(test)]
mod k5_tests {
    use super::*;

    #[test]
    fn the_plugins_handoff_line_gives_the_window() {
        let log = "\u{1b}[2m2026-10-03T11:04:42.250000Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m constellation_csi::node::rollout: engine-pod handoff succeeded unit=u1 views=1 elapsed=342.5ms from=e-a to=e-a-g1\n\
                   2026-10-03T11:09:00.000000Z  WARN constellation_csi::node::rollout: engine-pod handoff rolled back; the old pod serves unit=u2 step=\"prepare\" error=x restored=true\n\
                   2026-10-03T11:10:00.000000Z  INFO constellation_csi::node::rollout: engine-pod handoff succeeded unit=u2 views=2 elapsed=1.5s from=e-b to=e-b-g1\n";
        let a = plugin_handoff(log, "e-a-g1").unwrap();
        assert_eq!(a.end_ms, 1_791_025_482_250);
        assert_eq!(a.elapsed_ms, 342.5);
        assert!(a.failed.is_empty());
        let b = plugin_handoff(log, "e-b-g1").unwrap();
        assert_eq!(b.elapsed_ms, 1500.0);
        assert_eq!(b.failed.len(), 1, "{b:?}");
        assert!(plugin_handoff(log, "e-c").is_none());
        assert_eq!(rfc3339_ms("1970-01-01T00:00:01Z"), Some(1000));
        assert_eq!(rfc3339_ms("2000-03-01T00:00:00.5Z"), Some(951_868_800_500));
        assert_eq!(debug_duration_ms("980µs"), Some(0.98));
    }

    #[test]
    fn a_snapshot_of_the_trio_is_a_state_it_went_through() {
        let summary = constellation_pod_load::Summary {
            seed: 3,
            appended: 10,
            created: 4,
            ..Default::default()
        };
        let model = trio_model(&summary);
        let obs = |data: &[u8]| Observed::File {
            size: data.len() as u64,
            sha256: crate::model::sha256_of(data),
        };
        let app = constellation_pod_load::appended_data(3, 10);
        let mut seen = BTreeMap::new();
        seen.insert(PathBuf::from("created"), Observed::Dir);
        seen.insert(
            PathBuf::from("fixed"),
            obs(&constellation_pod_load::fixed_data(3)),
        );
        seen.insert(PathBuf::from("appended"), obs(&app[..5000]));
        let f1 = constellation_pod_load::created_data(3, 1);
        seen.insert(PathBuf::from("created/f1"), obs(&f1[..10]));
        let f0 = constellation_pod_load::created_data(3, 0);
        seen.insert(PathBuf::from("created/f0"), obs(&f0));
        assert_eq!(check_trio_snapshot(&seen, &model, 1, 1).unwrap(), 5000);
        // Bytes the trio never wrote there.
        let mut bad = seen.clone();
        bad.insert(PathBuf::from("appended"), obs(&app[1..5000]));
        assert!(check_trio_snapshot(&bad, &model, 0, 0).is_err());
        // A file it never made.
        let mut bad = seen.clone();
        bad.insert(PathBuf::from("created/f9"), obs(b""));
        assert!(check_trio_snapshot(&bad, &model, 0, 0).is_err());
        // `fixed` cut short.
        let mut bad = seen.clone();
        bad.insert(
            PathBuf::from("fixed"),
            obs(&constellation_pod_load::fixed_data(3)[..9]),
        );
        assert!(check_trio_snapshot(&bad, &model, 0, 0).is_err());
        // Fewer records than were fsynced before the cut: 5000 bytes hold
        // one whole record, not two.
        assert!(check_trio_snapshot(&seen, &model, 2, 1).is_err());
        // A file closed before the cut missing (f1 is only a prefix), or
        // missing altogether (f2, f3).
        assert!(check_trio_snapshot(&seen, &model, 1, 2).is_err());
        let mut gone = seen;
        gone.remove(Path::new("created/f0"));
        assert!(check_trio_snapshot(&gone, &model, 1, 1).is_err());
        assert!(check_trio_snapshot(&gone, &model, 1, 0).is_ok());
    }
}

#[cfg(test)]
mod k4_tests {
    use super::*;

    #[test]
    fn the_clone_log_line_is_found_by_subtree() {
        let log = "\u{1b}[2m2026-10-02T21:23:15Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m constellation_csi::controller::snapshots: cloned (metadata only) selector=/volumes/a@s subtree=/volumes/pvc-b ms=12\n\
                   x INFO cloned (metadata only) selector=/volumes/a@s subtree=/volumes/pvc-bb ms=99\n\
                   y INFO cloned (metadata only) selector=/volumes/a@s subtree=\"/volumes/pvc-q\" ms=7\n";
        assert_eq!(logged_clone_ms(log, "/volumes/pvc-b"), Some(12));
        assert_eq!(logged_clone_ms(log, "/volumes/pvc-bb"), Some(99));
        assert_eq!(logged_clone_ms(log, "/volumes/pvc-q"), Some(7));
        assert_eq!(logged_clone_ms(log, "/volumes/pvc"), None);
        assert_eq!(
            subtree_of("v1/pool/0/u/volumes/pvc-1").unwrap(),
            "/volumes/pvc-1"
        );
    }
}
