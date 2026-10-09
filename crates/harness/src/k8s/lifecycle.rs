//! Plan 37 K6b's engine-pod lifecycle scenarios: a node drain leaves the
//! roster clean (`csi-node-drain`), and the controller's purge worker
//! empties a pool's trash within its budgets while the pool's other
//! volumes are written (`csi-trash-purge-under-load`).

use super::{data_dir, fs_uuid_of, Env, Scope, BUCKET, HARNESS_PURGE};
use crate::docker::docker;
use crate::model::Model;
use crate::scenarios::eventually;
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

/// A cordoned worker, uncordoned again however the scenario ends.
struct Cordoned<'a> {
    env: &'a Env,
    node: String,
}

impl Drop for Cordoned<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.env.kube.run(&["uncordon", &self.node]) {
            eprintln!("   uncordon {}: {e:#}", self.node);
        }
    }
}

/// A StorageClass deleted by the scenario, put back however it ends (the
/// scope's teardown and later scenarios expect it).
struct ClassBack<'a> {
    env: &'a Env,
    class: Value,
}

impl Drop for ClassBack<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.env.kube.apply(&self.class) {
            eprintln!("   re-creating StorageClass: {e:#}");
        }
    }
}

/// The registry ids `peers.list` reports through engine pod `pod`.
fn registry_ids(env: &Env, pod: &str) -> Result<BTreeMap<u64, String>> {
    let peers = env
        .kube
        .engine_call(&env.driver_ns, pod, "peers.list", json!({}))?;
    Ok(peers["peers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["s3"] != true && p["node_id"].as_u64().unwrap_or(0) != 0)
        .map(|p| {
            (
                p["node_id"].as_u64().unwrap_or(0),
                p["hostname"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect())
}

fn node_id_of(env: &Env, pod: &str) -> Result<u64> {
    let status = env
        .kube
        .engine_call(&env.driver_ns, pod, "node.status", json!({}))?;
    status["node_id"]
        .as_u64()
        .with_context(|| format!("node.status of {pod} has no node_id"))
}

/// The pods of Deployment-managed app `app` in the scope: `(name, node)`
/// of each running, not terminating one.
fn app_pods(s: &Scope, app: &str) -> Result<Vec<(String, String, bool)>> {
    let list = s
        .kube()
        .get(&["pods", "-n", &s.ns, "-l", &format!("app={app}")])?;
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
                p["metadata"]["name"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                p["spec"]["nodeName"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                ready,
            )
        })
        .collect())
}

/// The one Ready pod of app `app`, waited for.
fn ready_app_pod(s: &Scope, app: &str, deadline: Duration) -> Result<(String, String)> {
    let mut found = (String::new(), String::new());
    eventually(&format!("one Ready pod of {app}"), deadline, || {
        let pods = app_pods(s, app)?;
        match pods.as_slice() {
            [(name, node, true)] => {
                found = (name.clone(), node.clone());
                Ok(())
            }
            other => bail!("pods of {app}: {other:?}"),
        }
    })?;
    Ok(found)
}

/// Node-owned engine pods (of any pool) on `node`, by name.
fn engine_pods_on(env: &Env, node: &str) -> Result<Vec<String>> {
    let list = env.kube.get(&[
        "pods",
        "-n",
        &env.driver_ns,
        "-l",
        "app.kubernetes.io/component=engine,constellation.replicix.com/owner=node",
    ])?;
    Ok(list["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["spec"]["nodeName"] == node)
        .filter_map(|p| p["metadata"]["name"].as_str().map(str::to_string))
        .collect())
}

/// Plan 37 §12 "Node drain → `node.leave`": `kubectl drain` of the worker
/// holding a mounted PV moves its pod to the other worker; the engine pod
/// on the drained worker leaves the pool's registry before it goes (no
/// ghost roster entry), no engine pod is left on it, its node identity is
/// gone from the host, and the data moved with the pod.
///
/// Then (the 37-k6b review) the worker hosting the pool's
/// controller-owned engine pod is drained right after a volume of the pool
/// was deleted into its trash: the drain takes that bare, emptyDir pod with
/// no leave and nobody asks for it again, yet the purge worker brings it
/// back on another node, purges the trashed volume through it, and retires
/// the dead incarnation's registry record.
pub fn csi_node_drain(env: &Env, seed: u64) -> Result<()> {
    let (w0, w1) = (env.workers[0].clone(), env.workers[1].clone());
    let mut s = Scope::new(env, "csi-node-drain")?;
    s.pvc("vol", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    let handle = s.volume_handle("vol")?;
    let fs = fs_uuid_of(&handle)?;

    // A Deployment, so the drain's eviction reschedules it; it prefers
    // w0, so it starts there.
    let deployment = json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "mover", "namespace": s.ns},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "mover"}},
            "template": {
                "metadata": {"labels": {"app": "mover"}},
                "spec": {
                    "terminationGracePeriodSeconds": 1,
                    "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
                    "affinity": {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 100,
                        "preference": {"matchExpressions": [{
                            "key": "kubernetes.io/hostname", "operator": "In", "values": [w0]
                        }]}
                    }]}},
                    "containers": [{
                        "name": "c",
                        "image": env.image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sleep", "infinity"],
                        "volumeMounts": [{"name": "vol", "mountPath": data_dir("vol")}],
                    }],
                    "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": "vol"}}],
                }
            }
        }
    });
    env.kube.apply(&deployment)?;
    let result = (|| -> Result<()> {
        let (pod, node) = ready_app_pod(&s, "mover", Duration::from_secs(300))?;
        ensure!(node == w0, "the mover started on {node}, not {w0}");
        let mut model = Model::default();
        let dir = data_dir("vol");
        for i in 0..20u64 {
            let data = format!("drain-{seed}-{i}\n").repeat(100 + i as usize);
            s.exec(&pod, &format!("printf '%s' '{data}' > {dir}/f-{i}"))?;
            model.write_file(Path::new(&format!("f-{i}")), data.into_bytes());
        }
        let seen = s.listing(&pod, &dir)?;
        model.verify_observed(&seen).context("before the drain")?;

        // The engine pod on w0, its registry id, and the roster with it.
        let engines = s.engine_pods(&fs, "node")?;
        let engine = engines
            .iter()
            .find(|e| e.node == w0)
            .with_context(|| format!("no engine pod on {w0}: {engines:?}"))?
            .name
            .clone();
        let id = node_id_of(env, &engine)?;
        let unit = env.kube.get(&["pod", "-n", &env.driver_ns, &engine])?["metadata"]["labels"]
            ["constellation.replicix.com/unit"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        eprintln!("   {pod} on {w0}, served by {engine} (registry id {id}, unit {unit})");

        // kubectl drain: engine pods are bare pods, so --force; the
        // budget holds them until the plugin has collected them.
        let cordoned = Cordoned {
            env,
            node: w0.clone(),
        };
        let t0 = Instant::now();
        env.kube
            .run(&[
                "drain",
                &w0,
                "--ignore-daemonsets",
                "--delete-emptydir-data",
                "--force",
                "--timeout=270s",
            ])
            .context("kubectl drain")?;
        let took = t0.elapsed();
        eprintln!("   kubectl drain {w0} completed in {took:.1?}");
        ensure!(
            engine_pods_on(env, &w0)?.is_empty(),
            "engine pods left on the drained {w0}: {:?}",
            engine_pods_on(env, &w0)?
        );

        // The node plugin collected it (the budget refused the drain's
        // eviction): it left the pool's registry because the node drains.
        let plugin_log = super::scenarios::strip_ansi(&env.kube.run(&[
            "-n",
            &env.driver_ns,
            "logs",
            "-l",
            "app.kubernetes.io/component=node",
            "-c",
            "constellation-csi",
            "--tail=-1",
            "--prefix",
        ])?);
        ensure!(
            plugin_log.lines().any(|l| l.contains(&engine)
                && l.contains("left the pool's registry")
                && l.contains("draining")),
            "no node-plugin log line says {engine} left the registry for the drain"
        );

        // The pod moved, and the data with it.
        let (pod, node) = ready_app_pod(&s, "mover", Duration::from_secs(300))?;
        ensure!(node == w1, "the mover went to {node}, not {w1}");
        let seen = s.listing(&pod, &dir)?;
        model.verify_observed(&seen).context("after the drain")?;
        s.exec(&pod, &format!("echo after > {dir}/after"))?;
        model.write_file(Path::new("after"), b"after\n".to_vec());
        s.listing(&pod, &dir)
            .and_then(|seen| model.verify_observed(&seen))
            .context("written after the drain")?;

        // The roster is clean: w0's engine left (its record tombstoned,
        // so peers.list no longer reports it), as seen from w1's engine.
        let engines = s.engine_pods(&fs, "node")?;
        let survivor = engines
            .iter()
            .find(|e| e.node == w1)
            .with_context(|| format!("no engine pod on {w1}: {engines:?}"))?
            .name
            .clone();
        eventually(
            &format!("registry id {id} gone from the roster"),
            Duration::from_secs(60),
            || {
                let ids = registry_ids(env, &survivor)?;
                ensure!(!ids.contains_key(&id), "the roster still has {id}: {ids:?}");
                ensure!(
                    !ids.values().any(|h| h == &engine),
                    "a record of {engine} is still live: {ids:?}"
                );
                Ok(())
            },
        )?;
        // The spent identity is gone from the drained worker's disk.
        let listing = docker(&[
            "exec",
            &w0,
            "ls",
            "-a",
            "/var/lib/constellation-csi/node-identity",
        ])
        .unwrap_or_default();
        ensure!(
            !listing.lines().any(|l| l == unit
                || l == format!(".leaving-{unit}")
                || l.starts_with(&format!(".left-{unit}-"))),
            "the node identity of {unit} is still on {w0}: {listing}"
        );
        eprintln!("   registry id {id} left the roster; no engine pod or identity left on {w0}");

        drop(cordoned);
        // Uncordoned, the worker stays free of engine pods of the pool
        // until a volume is staged there again.
        std::thread::sleep(Duration::from_secs(10));
        ensure!(
            engine_pods_on(env, &w0)?.is_empty(),
            "an engine pod came back on {w0}"
        );
        controller_pod_drained(env, &mut s, &fs, seed)
    })();
    // The Deployment would replace the pods the teardown deletes.
    let _ = env.kube.run(&[
        "delete",
        "deployment",
        "mover",
        "-n",
        &s.ns,
        "--wait=true",
        "--timeout=120s",
    ]);
    result?;
    s.finish()
}

/// The pool's controller-owned engine pod `(name, node, uid, created)`,
/// Ready and labelled with its filesystem (a pod the purge worker just
/// brought back is labelled once the controller reached it).
fn controller_pod(s: &Scope, fs: &str) -> Result<(String, String, String, String)> {
    let pods = s.kube().get(&[
        "pods",
        "-n",
        &s.env.driver_ns,
        "-l",
        &format!(
            "app.kubernetes.io/component=engine,constellation.replicix.com/owner=controller,constellation.replicix.com/fs-uuid={fs}"
        ),
    ])?;
    let pod = pods["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["metadata"]["deletionTimestamp"].is_null())
        .find(|p| {
            p["status"]["conditions"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")
        })
        .with_context(|| format!("no Ready controller-owned engine pod of {fs}"))?;
    let field = |v: &Value| v.as_str().unwrap_or_default().to_string();
    Ok((
        field(&pod["metadata"]["name"]),
        field(&pod["spec"]["nodeName"]),
        field(&pod["metadata"]["uid"]),
        field(&pod["metadata"]["creationTimestamp"]),
    ))
}

/// The `/.trash` entries of the pool behind controller-owned pod `pod`.
fn trash_entries(env: &Env, pod: &str) -> Result<Vec<String>> {
    let trash = env.kube.engine_call(
        &env.driver_ns,
        pod,
        "browse.readdir",
        json!({"path": "/.trash"}),
    )?;
    Ok(trash["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e["path"].as_str().map(str::to_string))
        .collect())
}

/// `csi-node-drain`'s second half (see there).
fn controller_pod_drained(env: &Env, s: &mut Scope, fs: &str, seed: u64) -> Result<()> {
    let (w0, w1) = (&env.workers[0], &env.workers[1]);
    // The first drain may have taken it already (the worker brings it back).
    let mut found = Default::default();
    eventually(
        "the pool's controller-owned pod Ready",
        Duration::from_secs(240),
        || {
            found = controller_pod(s, fs)?;
            Ok(())
        },
    )?;
    let (ctl, ctl_node, ctl_uid, _) = found;
    let ctl_id = node_id_of(env, &ctl)?;
    eprintln!("   the pool's controller-owned pod {ctl} runs on {ctl_node} (registry id {ctl_id})");

    // A second volume of the pool, written, then deleted into the trash —
    // after its StorageClass is gone (an app's manifests deleted all at
    // once): the pool is recorded from the PV itself, and brought back
    // below without any class to rebuild it from.
    s.pvc("doomed", "1Gi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    let mut class = s.kube().get(&["storageclass", &s.sc])?;
    if let Some(meta) = class["metadata"].as_object_mut() {
        for k in [
            "resourceVersion",
            "uid",
            "creationTimestamp",
            "managedFields",
        ] {
            meta.remove(k);
        }
    }
    s.kube()
        .run(&["delete", "storageclass", &s.sc, "--wait=true"])?;
    let class_back = ClassBack { env, class };
    let pv = s
        .volume_handle("doomed")?
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string();
    let writer_node = if &ctl_node == w0 { w1 } else { w0 };
    s.pod("doomed-writer", writer_node, &["doomed"])?;
    s.exec(
        "doomed-writer",
        &format!(
            "set -e; for i in $(seq 8); do yes doomed-{seed}-$i | head -c 65536 > {}/f-$i; done",
            data_dir("doomed")
        ),
    )?;
    s.delete_pods(&["doomed-writer"])?;
    s.kube()
        .run(&["delete", "pvc", "-n", &s.ns, "doomed", "--wait=true"])?;
    eventually("the doomed PV gone", Duration::from_secs(180), || {
        let gone = s.kube().run(&["get", "pv", &pv, "--ignore-not-found"])?;
        ensure!(gone.trim().is_empty(), "PV {pv} still there");
        Ok(())
    })?;
    let trashed: Vec<String> = trash_entries(env, &ctl)?
        .into_iter()
        .filter(|e| e.starts_with(&format!("/.trash/{pv}-")))
        .collect();
    ensure!(
        trashed.len() == 1,
        "the deleted volume is not in the trash: {trashed:?}"
    );
    let entry = trashed[0].clone();
    let records = s
        .kube()
        .get(&["configmap", "-n", &env.driver_ns, "constellation-csi-pools"])?;
    let record: Value = serde_json::from_str(
        records["data"][ctl.as_str()]
            .as_str()
            .with_context(|| format!("no pool record of {ctl}: {records}"))?,
    )?;
    ensure!(
        record["fs_uuid"] == fs
            && record["secret"].is_array()
            && record["parameters"]["bucket"] == BUCKET,
        "the pool record of {ctl}, written with its StorageClass gone, is incomplete: {record}"
    );
    eprintln!(
        "   {pv} deleted into {entry} (class gone, pool recorded from the PV); draining {ctl_node}"
    );

    // Drain the node: the controller-owned pod has no budget, so it is
    // evicted at once — no leave, and its emptyDir state is gone.
    let cordoned = Cordoned {
        env,
        node: ctl_node.clone(),
    };
    env.kube
        .run(&[
            "drain",
            &ctl_node,
            "--ignore-daemonsets",
            "--delete-emptydir-data",
            "--force",
            "--timeout=270s",
        ])
        .context("kubectl drain of the controller-owned pod's node")?;

    // The purge worker brings the pod back (elsewhere), purges the entry
    // through the new incarnation, and sweeps the dead one's record.
    let deadline = Duration::from_secs(HARNESS_PURGE.grace_s + 3 * HARNESS_PURGE.interval_s + 180);
    let mut back = (String::new(), String::new(), String::new(), String::new());
    eventually(
        "the controller-owned pod back on another node",
        deadline,
        || {
            let now = controller_pod(s, fs)?;
            ensure!(now.2 != ctl_uid, "still the drained incarnation");
            ensure!(now.1 != ctl_node, "back on the drained {ctl_node}");
            back = now;
            Ok(())
        },
    )?;
    let (new_ctl, new_node, _, created) = back;
    let mut purged_at = String::new();
    eventually("the trashed volume purged", deadline, || {
        let log = super::scenarios::strip_ansi(&s.controller_log()?);
        let line = log
            .lines()
            .find(|l| l.contains("purged trash entry") && fields(l).get("entry") == Some(&entry))
            .with_context(|| format!("no purge of {entry} logged yet"))?;
        purged_at = line
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        ensure!(
            trash_entries(env, &new_ctl)?.iter().all(|e| *e != entry),
            "{entry} is still in the trash"
        );
        Ok(())
    })?;
    // Purged by the new incarnation, not raced by the old one before the
    // drain took it (RFC 3339 UTC: compared to the second as text).
    let second = |t: &str| t.chars().take(19).collect::<String>();
    ensure!(
        second(&purged_at) >= second(&created),
        "{entry} was purged at {purged_at}, before the new incarnation ({created}): the \
         drain proved nothing"
    );
    eventually(
        &format!("the drained incarnation's record {ctl_id} retired"),
        deadline,
        || {
            let ids = registry_ids(env, &new_ctl)?;
            ensure!(!ids.contains_key(&ctl_id), "still in the roster: {ids:?}");
            Ok(())
        },
    )?;
    eprintln!(
        "   {new_ctl} back on {new_node}; {entry} purged through it at {purged_at}; \
         registry id {ctl_id} retired"
    );
    drop(cordoned);
    drop(class_back);
    Ok(())
}

/// A writer that runs until `/tmp/stop`, as `scenarios::writer_script`,
/// and also keeps the longest iteration in `/tmp/max_ms` (a stall) and
/// when it started in `/tmp/max_at` (unix ms).
fn timed_writer_script(claim: &str, tag: &str) -> String {
    format!(
        r#"d={d}/w
mkdir -p $d || echo mkdir >> /tmp/errors
i=0
max=0
while [ ! -e /tmp/stop ]; do
  t0=$(date +%s%3N)
  if ! yes "{tag}-$i" | head -c 4096 > $d/f-$i; then echo "write $i" >> /tmp/errors; fi
  if ! echo "{tag}-$i" >> $d/log; then echo "append $i" >> /tmp/errors; fi
  t1=$(date +%s%3N)
  dt=$((t1-t0))
  if [ $dt -gt $max ]; then max=$dt; echo $max > /tmp/max_ms; echo $t0 > /tmp/max_at; fi
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

/// The longest one writer iteration (a 4 KiB write and an append) may
/// take while the purge runs: "no write interruption". The final kind
/// runs saw at most 2.9 s on a loaded host.
const MAX_STALL_MS: u64 = 10_000;

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

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn read_u64(s: &Scope, pod: &str, file: &str) -> Result<u64> {
    let out = s.exec(pod, &format!("cat {file} 2>/dev/null || echo 0"))?;
    Ok(out.trim().parse().unwrap_or(0))
}

/// `key=value` fields of a tracing line (values may be quoted).
fn fields(line: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for part in line.split_whitespace() {
        if let Some((k, v)) = part.split_once('=') {
            out.insert(k.to_string(), v.trim_matches('"').to_string());
        }
    }
    out
}

/// One `purged trash entry` line of the controller's log.
#[derive(Debug, Clone)]
struct Purged {
    entry: String,
    ops: u64,
    bytes: u64,
    files: u64,
    took_ms: u64,
}

fn purged_entries(s: &Scope) -> Result<Vec<Purged>> {
    let log = super::scenarios::strip_ansi(&s.controller_log()?);
    Ok(log
        .lines()
        .filter(|l| l.contains("purged trash entry"))
        .map(fields)
        .map(|f| {
            let n = |k: &str| f.get(k).and_then(|v| v.parse().ok()).unwrap_or(0);
            Purged {
                entry: f.get("entry").cloned().unwrap_or_default(),
                ops: n("ops"),
                bytes: n("bytes"),
                files: n("files"),
                took_ms: n("took_ms"),
            }
        })
        .collect())
}

/// `constellation gc verify` of `prefix` from the host (plan 37 settled
/// decision 18: anybody with the bucket's credentials may), with the GC
/// horizon at 0 so a chunk unreferenced now counts: `(candidates, chunk
/// objects, the metadata tree's live nodes)`, the candidates as their
/// chunk keys.
fn gc_verify(env: &Env, prefix: &str) -> Result<(BTreeSet<String>, u64, u64)> {
    let dir = tempfile::tempdir()?;
    let url = format!("s3://{BUCKET}/{prefix}");
    let out = std::process::Command::new(crate::client::constellation_bin())
        .args(["gc", "verify", &url, "--s3", &url, "--state-dir"])
        .arg(dir.path().join("state"))
        .env("HOME", dir.path())
        .env("XDG_CONFIG_HOME", dir.path().join("config"))
        .env("XDG_RUNTIME_DIR", dir.path())
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_ENDPOINT", &env.endpoint)
        .env("AWS_ALLOW_HTTP", "true")
        .env("CONSTELLATION_GC_HORIZON_S", "0")
        .output()
        .context("running constellation gc verify")?;
    ensure!(
        out.status.success(),
        "constellation gc verify failed ({}): {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).with_context(|| {
        format!(
            "parsing the gc report {:?}",
            String::from_utf8_lossy(&out.stdout)
        )
    })?;
    let chunk_candidates = report["candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["key"].as_str())
        .filter_map(|k| k.find("chunks/").map(|at| k[at..].to_string()))
        .collect();
    Ok((
        chunk_candidates,
        report["census"]["chunk_objects"].as_u64().unwrap_or(0),
        report["metadata"]["live_nodes"].as_u64().unwrap_or(0),
    ))
}

/// Plan 37 §12 "Trash purge under load": three PVs of one pool deleted —
/// a few small files, 64 MiB in 1 MiB chunks, and 100 000 empty files in
/// 100 directories — while two other PVs of the pool are written
/// on both workers. Every trash entry is purged (the large-many-small-files
/// one included) within the purge budgets, each entry's measured rate at
/// or under them; the writers never fail nor stall (no iteration over
/// [`MAX_STALL_MS`]); their data is intact; GC finds the purged volumes'
/// chunks still referenced while they sit in the trash (checked within the
/// grace window, the entry seen whole before and after) and unreferenced
/// only once the purge removed them, while the purge itself deleted no S3
/// object.
pub fn csi_trash_purge_under_load(env: &Env, seed: u64) -> Result<()> {
    let (w0, w1) = (env.workers[0].clone(), env.workers[1].clone());
    let scenario = "csi-trash-purge-under-load";
    let prefix = format!("k8s-harness/{}/{scenario}", env.run_id);
    let tag = format!("p{seed}");
    let mut s = Scope::new(env, scenario)?;
    let doomed = ["del-small", "del-big", "del-many"];
    for claim in ["live-a", "live-b"].iter().chain(doomed.iter()) {
        s.pvc(claim, "2Gi", false)?;
    }
    s.wait_bound(Duration::from_secs(300))?;
    let mut doomed_pvs = BTreeMap::new();
    for claim in doomed {
        let handle = s.volume_handle(claim)?;
        let pv = handle.rsplit('/').next().unwrap_or_default().to_string();
        doomed_pvs.insert(claim, pv);
    }

    // Fill the doomed volumes from one pod on w0. With `fsGroupChangePolicy:
    // OnRootMismatch`: kubelet re-applies a pod's fsGroup on every
    // republish (the driver's `requiresRepublish`), and by default walks
    // the whole volume to do it — 100 000 chowns through FUSE each time,
    // racing the fill.
    let mounts: Vec<Value> = doomed
        .iter()
        .map(|c| json!({"name": c, "mountPath": data_dir(c)}))
        .collect();
    let vols: Vec<Value> = doomed
        .iter()
        .map(|c| json!({"name": c, "persistentVolumeClaim": {"claimName": c}}))
        .collect();
    env.kube.apply(&json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "filler", "namespace": s.ns, "labels": {"app": "k8s-harness"}},
        "spec": {
            "nodeName": w0,
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 1,
            "securityContext": {
                "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
                "fsGroupChangePolicy": "OnRootMismatch",
            },
            "containers": [{
                "name": "c",
                "image": env.image,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sleep", "infinity"],
                "volumeMounts": mounts,
            }],
            "volumes": vols,
        }
    }))?;
    s.wait_ready("filler", Duration::from_secs(300))?;
    let t0 = Instant::now();
    s.exec(
        "filler",
        &format!(
            "set -e; d={}; for i in $(seq 10); do head -c 4096 /dev/urandom > $d/s-$i; done",
            data_dir("del-small")
        ),
    )?;
    s.exec(
        "filler",
        &format!(
            "set -e; d={}; for i in $(seq 64); do yes big-{seed}-$i | head -c 1048576 > $d/b-$i; done",
            data_dir("del-big")
        ),
    )?;
    // 100 directories of 1000 empty files, 5 directories at a time: the
    // case is the per-entry cost (a non-empty file would add a chunk PUT
    // each, which the purge never pays).
    for batch in 0..10 {
        let t = Instant::now();
        s.exec(
            "filler",
            &format!(
                r#"set -e; d={dir}
for g in 0 1; do
  for k in 0 1 2 3 4; do
    n=$(({batch} * 10 + g * 5 + k))
    ( mkdir -p $d/d$n && cd $d/d$n && seq -f f%03g 0 999 | xargs touch ) &
  done
  wait
done
"#,
                dir = data_dir("del-many")
            ),
        )?;
        eprintln!("   del-many: 10000 more files in {:.1?}", t.elapsed());
    }
    let many = s.exec(
        "filler",
        &format!("find {} -type f | wc -l", data_dir("del-many")),
    )?;
    ensure!(
        many.trim() == "100000",
        "del-many holds {} files, not 100000",
        many.trim()
    );
    eprintln!(
        "   filled del-small (10), del-big (64 MiB), del-many (100000 empty files) in {:.1?}",
        t0.elapsed()
    );

    // Writers on both workers, through the whole purge.
    s.spawn_pod(
        "writer-a",
        &w0,
        &["live-a"],
        Some(&timed_writer_script("live-a", &format!("{tag}a"))),
    )?;
    s.spawn_pod(
        "writer-b",
        &w1,
        &["live-b"],
        Some(&timed_writer_script("live-b", &format!("{tag}b"))),
    )?;
    s.wait_ready("writer-a", Duration::from_secs(300))?;
    s.wait_ready("writer-b", Duration::from_secs(300))?;
    eventually("both writers under way", Duration::from_secs(60), || {
        ensure!(read_u64(&s, "writer-a", "/tmp/count")? >= 10, "writer-a");
        ensure!(read_u64(&s, "writer-b", "/tmp/count")? >= 10, "writer-b");
        Ok(())
    })?;

    // GC's view before anything is deleted. del-big's files are one 1 MiB
    // chunk each (the class's chunkSize), named by the BLAKE3 of their
    // bytes (a filesystem without E2E): the harness knows all 64 keys.
    let big_keys: BTreeSet<String> = (1..=64)
        .map(|i| {
            let line = format!("big-{seed}-{i}\n");
            let data: Vec<u8> = line.bytes().cycle().take(1 << 20).collect();
            let hex = blake3::hash(&data).to_hex().to_string();
            format!("chunks/{}/{}/{hex}", &hex[0..2], &hex[2..4])
        })
        .collect();
    let (c0, n0, live0) = gc_verify(env, &prefix)?;
    eprintln!(
        "   before: {} unreferenced chunks, {n0} chunk objects, {live0} live tree nodes",
        c0.len()
    );
    ensure!(
        c0.is_disjoint(&big_keys),
        "del-big's chunks are unreferenced before the delete: {:?}",
        c0.intersection(&big_keys).collect::<Vec<_>>()
    );

    // Delete the three volumes.
    s.delete_pods(&["filler"])?;
    let fs_live = fs_uuid_of(&s.volume_handle("live-a")?)?;
    let purge_start = Instant::now();
    let delete_ms = unix_ms();
    let counts_at_delete = (
        read_u64(&s, "writer-a", "/tmp/count")?,
        read_u64(&s, "writer-b", "/tmp/count")?,
    );
    let mut args = vec!["delete", "pvc", "-n", &s.ns, "--wait=true"];
    args.extend_from_slice(&doomed);
    s.kube().run(&args)?;
    eventually("the deleted PVs gone", Duration::from_secs(180), || {
        let pvs = s.kube().get(&["pv"])?;
        let left: Vec<&str> = pvs["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["metadata"]["name"].as_str())
            .filter(|n| doomed_pvs.values().any(|pv| pv == n))
            .collect();
        ensure!(left.is_empty(), "PVs left: {left:?}");
        Ok(())
    })?;

    // Trashed but not purged yet (the grace window): GC still finds every
    // chunk of del-big referenced — the trash keeps them — and the entry
    // is whole before and after that look, so the look saw it whole.
    let controller = s
        .engine_pods(&fs_live, "controller")?
        .first()
        .map(|p| p.name.clone())
        .context("no controller-owned engine pod")?;
    let big_entry = trash_entries(env, &controller)?
        .into_iter()
        .find(|e| e.starts_with(&format!("/.trash/{}-", doomed_pvs["del-big"])))
        .context("del-big is not in the trash")?;
    let whole = |when: &str| -> Result<()> {
        let files = env.kube.engine_call(
            &env.driver_ns,
            &controller,
            "browse.readdir",
            json!({"path": big_entry}),
        )?["entries"]
            .as_array()
            .map_or(0, |e| e.len());
        ensure!(
            files == 64,
            "{big_entry} holds {files} of its 64 files {when} the GC look: the purge began \
             inside the {}s grace",
            HARNESS_PURGE.grace_s
        );
        Ok(())
    };
    whole("before")?;
    let look = unix_ms();
    let (ct, _, _) = gc_verify(env, &prefix)?;
    eprintln!(
        "   GC looked at the trashed pool {:+.1}s..{:+.1}s after the delete",
        (look - delete_ms) as f64 / 1000.0,
        (unix_ms() - delete_ms) as f64 / 1000.0
    );
    whole("after")?;
    ensure!(
        ct.is_disjoint(&big_keys),
        "{} of del-big's chunks are unreferenced while it is only trashed",
        ct.intersection(&big_keys).count()
    );
    eprintln!("   trashed, not purged: all 64 of del-big's chunks still referenced");

    // Every trash entry purged. The budget: all three take at least
    // ops/ops_per_s or bytes/bytes_per_s; allow three times that, plus
    // the grace and two intervals, plus a minute.
    let ops_total = 10 + 64 + 100_000 + 100 + 3;
    let bytes_total: u64 = 10 * 4096 + 64 * (1 << 20);
    let budget = (ops_total as f64 / HARNESS_PURGE.ops_per_s as f64)
        .max(bytes_total as f64 / HARNESS_PURGE.bytes_per_s as f64);
    let deadline = Duration::from_secs_f64(budget * 3.0)
        + Duration::from_secs(HARNESS_PURGE.grace_s + 2 * HARNESS_PURGE.interval_s + 60);
    let mut purged = BTreeMap::new();
    eventually(
        "every deleted volume purged from the trash",
        deadline,
        || {
            for p in purged_entries(&s)? {
                for (claim, pv) in &doomed_pvs {
                    if p.entry.starts_with(&format!("/.trash/{pv}-")) {
                        purged.insert(*claim, p.clone());
                    }
                }
            }
            let missing: Vec<_> = doomed.iter().filter(|c| !purged.contains_key(*c)).collect();
            ensure!(missing.is_empty(), "not purged yet: {missing:?}");
            Ok(())
        },
    )?;
    let purge_took = purge_start.elapsed();
    for (claim, p) in &purged {
        let secs = (p.took_ms as f64 / 1000.0).max(0.001);
        eprintln!(
            "   {claim}: {} ops, {} files, {} bytes in {:.1}s ({:.0} ops/s, {:.0} B/s)",
            p.ops,
            p.files,
            p.bytes,
            secs,
            p.ops as f64 / secs,
            p.bytes as f64 / secs
        );
    }
    let many = &purged["del-many"];
    ensure!(
        many.files == 100_000 && many.ops == 100_101,
        "del-many purged {} files in {} ops, not 100000 in 100101 (a recursive fallback?)",
        many.files,
        many.ops
    );
    ensure!(
        purged["del-big"].bytes == 64 << 20,
        "del-big purged {} bytes",
        purged["del-big"].bytes
    );
    // Within the budgets: the pace lets an op through every 1/ops_per_s
    // and a byte every 1/bytes_per_s, so no entry may beat either (a 2 %
    // allowance for the clock the log rounds to).
    for (claim, p) in &purged {
        let secs = p.took_ms as f64 / 1000.0;
        let min_secs = ((p.ops.saturating_sub(1)) as f64 / HARNESS_PURGE.ops_per_s as f64)
            .max(p.bytes.saturating_sub(1 << 20) as f64 / HARNESS_PURGE.bytes_per_s as f64);
        ensure!(
            secs >= min_secs * 0.98,
            "{claim} was purged faster than its budget: {secs:.2}s < {min_secs:.2}s"
        );
    }
    eprintln!("   all three purged {purge_took:.1?} after the delete (deadline {deadline:.0?})");
    // The trash is empty through the controller-owned pod.
    let controller = s
        .engine_pods(&fs_live, "controller")?
        .first()
        .map(|p| p.name.clone())
        .context("no controller-owned engine pod")?;
    let trash = trash_entries(env, &controller)?;
    ensure!(trash.is_empty(), "the trash is not empty: {trash:?}");

    // The writers never failed nor stalled, and went on throughout.
    let counts_after = (
        read_u64(&s, "writer-a", "/tmp/count")?,
        read_u64(&s, "writer-b", "/tmp/count")?,
    );
    ensure!(
        counts_after.0 > counts_at_delete.0 + 20 && counts_after.1 > counts_at_delete.1 + 20,
        "the writers stalled during the purge: {counts_at_delete:?} -> {counts_after:?}"
    );
    for pod in ["writer-a", "writer-b"] {
        s.exec(pod, "touch /tmp/stop")?;
    }
    let mut finals = Vec::new();
    for (pod, claim, t) in [
        ("writer-a", "live-a", format!("{tag}a")),
        ("writer-b", "live-b", format!("{tag}b")),
    ] {
        let mut n = 0;
        eventually(&format!("{pod} stopped"), Duration::from_secs(60), || {
            n = s.exec(pod, "cat /tmp/final")?.trim().parse()?;
            Ok(())
        })?;
        let errors = s.exec(pod, "cat /tmp/errors 2>/dev/null || true")?;
        ensure!(
            errors.trim().is_empty(),
            "{pod} saw I/O errors during the purge:\n{errors}"
        );
        let max = read_u64(&s, pod, "/tmp/max_ms")?;
        let at = read_u64(&s, pod, "/tmp/max_at")?;
        eprintln!(
            "   {pod}: longest iteration {max} ms, from {:+.1}s after the delete",
            (at as f64 - delete_ms as f64) / 1000.0
        );
        ensure!(
            max < MAX_STALL_MS,
            "{pod} stalled for {max} ms in one iteration during the purge"
        );
        let seen = s.listing(pod, &data_dir(claim))?;
        writer_model(&t, n)
            .verify_observed(&seen)
            .with_context(|| format!("{pod}'s volume after the purge"))?;
        finals.push((pod, n, max));
    }
    eprintln!("   writers (iterations, longest ms): {finals:?}, zero errors, data intact");

    // GC: the purge deleted no S3 object (the census did not shrink),
    // and the purged volumes' chunks — 64 for del-big alone — are now
    // unreferenced, for the engine's GC to reclaim.
    let (c1, n1, live1) = gc_verify(env, &prefix)?;
    eprintln!(
        "   after: {} unreferenced chunks, {n1} chunk objects, {live1} live tree nodes",
        c1.len()
    );
    ensure!(
        n1 >= n0,
        "chunk objects went {n0} -> {n1}: the purge must delete none (GC's job)"
    );
    let missing: Vec<_> = big_keys.difference(&c1).collect();
    ensure!(
        missing.is_empty(),
        "{} of del-big's 64 chunks are not unreferenced (or not there) after the purge: {missing:?}",
        missing.len()
    );
    s.finish()
}
