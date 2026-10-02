//! The `k8s-scenario` catalog (plan 37 §12): Constellation behaviour that
//! only exists behind the CSI driver — PVs staged into node-owned engine
//! pods, published into workload pods, shared across nodes, many to a
//! pool. Each scenario owns a [`Scope`] (namespace + pool StorageClass)
//! and ends with [`Scope::finish`], which also proves every volume it used
//! unstaged.

use super::remote::RemoteWorkload;
use super::{data_dir, fs_uuid_of, EnginePod, Env, Scope};
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
