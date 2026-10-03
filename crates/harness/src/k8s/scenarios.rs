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
use crate::model::{Model, Node};
use crate::scenarios::eventually;
use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

pub struct K8sScenario {
    pub name: &'static str,
    pub desc: &'static str,
    /// Schedulable workers the scenario needs (a cluster with fewer skips
    /// it, saying so).
    pub workers: usize,
    pub run: fn(&Env, u64) -> Result<()>,
}

pub const K8S_SCENARIOS: &[K8sScenario] = &[
    K8sScenario {
        name: "csi-pod-rw",
        desc: "a pod writes a seeded workload through a PVC, verified against the model; a pod on the other worker remounts it and sees the same tree",
        workers: 2,
        run: csi_pod_rw,
    },
    K8sScenario {
        name: "csi-rwx-across-nodes",
        desc: "two pods on two workers share one RWX PVC: alternating and concurrent writes converge to the model on both (close-to-open, bounded: eventual, nothing stronger)",
        workers: 2,
        run: csi_rwx_across_nodes,
    },
    K8sScenario {
        name: "csi-many-pvs-one-pool",
        desc: "50 PVs of one pool StorageClass across both workers: each its own tree, one engine pod per (pool, node), each PV's quota enforced on its own",
        workers: 2,
        run: csi_many_pvs_one_pool,
    },
    K8sScenario {
        name: "csi-plugin-restart-survives",
        desc: "node-plugin and controller pods deleted under a writing pod: mounts survive, zero errors, the engine pods untouched, the restarted plugins stage, publish and unstage",
        workers: 2,
        run: csi_plugin_restart_survives,
    },
    K8sScenario {
        name: "csi-snapshot-clone-mount",
        desc: "VolumeSnapshot of a written PVC, then a PVC restored from it and a PVC cloned from the source, both mounted on the other worker: the restore is the tree at the snapshot, the clone the tree now, both metadata-only clones in the source's pool, independent of the source",
        workers: 2,
        run: csi_snapshot_clone_mount,
    },
    K8sScenario {
        name: "csi-clone-cross-pool-refused",
        desc: "clone and restore into the source's own pool bind and hold its data; the same clone and restore into another pool's StorageClass are refused (INVALID_ARGUMENT naming both filesystems) and stay Pending",
        workers: 2,
        run: csi_clone_cross_pool_refused,
    },
    K8sScenario {
        name: "csi-secret-rotation",
        desc: "a `refreshing` class's Secret rotated twice under a writing pod and a reader on the other worker: no remount, no engine restart, zero errors; every engine pod's S3 clients sign with the new pair (fs.list generations), each push is in its audit log under the node plugin's service principal, and no pod spec, environment or hostPath file holds a credential",
        workers: 2,
        run: csi_secret_rotation,
    },
    K8sScenario {
        name: "csi-pod-security",
        desc: "the privilege split of plan 37 §9, as PodSecurity admission judges it (server dry runs in a `restricted` namespace): controller and controller-owned engine pod admitted, node-owned engine pods refused for their hostPaths only, the node plugin refused as privileged",
        workers: 1,
        run: csi_pod_security,
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

fn strip_ansi(s: &str) -> String {
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
    let probe_ns = format!("k8s-psa-{}", env.run_id);
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
