//! Plan 37 K7's pool-access scenarios (§12, settled decisions 7, 8, 17,
//! 18): a pool reached other than by `CreateVolume` — a statically
//! provisioned PV naming a path a human seeded (`csi-static-provisioning`),
//! a human mounting a PV's subtree with the ordinary CLI while a pod uses it
//! (`csi-human-cli-mount`) — and a sharded pool's routing
//! (`csi-shard-routing`).
//!
//! **The human** is a [`Workstation`]: a container on the kind docker
//! network (so outside Kubernetes, reaching the run's S3 as the cluster
//! does) running the driver image's `constellation` CLI as root, with
//! `/dev/fuse`. It registers the pool by its S3 location and mounts
//! `pool:<subtree>` exactly as plan 37 settled decision 18 writes it; it
//! joins the pool's cluster as one more node, and leaves it
//! (`constellation leave`) when the scenario ends. Its file operations run
//! as uid/gid 1000, the workload pods' user, so either side may change what
//! the other made.
//!
//! **The volume condition** (plan 37 §11) is read from the node plugin of
//! the node staging the volume: `constellation-csi --volume-health`
//! `kubectl exec`ed in its container calls `NodeGetVolumeHealth` on its own
//! socket. Kubelet surfaces nothing of it on its own here (CSI 1.13 moved
//! the condition out of `NodeGetVolumeStats`).

use super::remote::{listing_script, parse_listing, RemoteWorkload};
use super::scenarios::{converge, pvc_source, verify, CONVERGE};
use super::{data_dir, fs_uuid_of, run_cmd, Env, Scope, BUCKET, IDLE_TTL_S};
use crate::docker::Container;
use crate::model::Model;
use crate::scenarios::eventually;
use anyhow::{bail, ensure, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

/// The name the human registers the pool under.
const POOL: &str = "pool";
/// The workload pods' (and the human's file operations') uid and gid.
const USER: &str = "1000:1000";

/// A human's machine outside the cluster (module docs).
pub struct Workstation {
    name: String,
    /// Filesystems this machine mounted, left at the end.
    joined: BTreeSet<String>,
    _container: Container,
}

impl Workstation {
    /// Start the machine for scope `ns`.
    pub fn up(env: &Env, ns: &str) -> Result<Self> {
        let name = format!("{}-human-{ns}", env.cluster);
        let endpoint = format!("AWS_ENDPOINT={}", env.endpoint);
        let endpoint_url = format!("AWS_ENDPOINT_URL={}", env.endpoint);
        let container = Container::run_with_cmd(
            &name,
            &env.image,
            &[
                "--network",
                "kind",
                "--privileged",
                "--security-opt",
                "label=disable",
                "--user",
                "0",
                "--entrypoint",
                "sleep",
                "-e",
                "AWS_ACCESS_KEY_ID=test",
                "-e",
                "AWS_SECRET_ACCESS_KEY=test",
                "-e",
                &endpoint,
                "-e",
                &endpoint_url,
                "-e",
                "AWS_ALLOW_HTTP=true",
                "-e",
                "AWS_REGION=us-east-1",
                "-e",
                "AWS_DEFAULT_REGION=us-east-1",
                "-e",
                "HOME=/root",
                "-e",
                "XDG_RUNTIME_DIR=/root/run",
            ],
            &["infinity"],
        )?;
        let me = Self {
            name,
            joined: BTreeSet::new(),
            _container: container,
        };
        me.root("mkdir -p /root/run && chmod 700 /root/run")?;
        Ok(me)
    }

    fn sh(&self, user: &str, script: &str) -> Result<String> {
        let mut c = Command::new("docker");
        c.args(["exec", "-i", "-u", user, &self.name, "sh", "-s"]);
        run_cmd(c, Some(script.as_bytes()), Duration::from_secs(300))
            .with_context(|| format!("on the workstation {}", self.name))
    }

    /// `script` as root (the CLI: mount, leave).
    pub fn root(&self, script: &str) -> Result<String> {
        self.sh("0:0", script)
    }

    /// `script` as the workload's user (file operations).
    pub fn user(&self, script: &str) -> Result<String> {
        self.sh(USER, script)
    }

    /// `constellation mount pool:<subtree> <dir>`, the pool at `prefix` of
    /// the run's bucket (registered by that first mount, `--s3`).
    pub fn mount(&mut self, prefix: &str, subtree: &str, dir: &str) -> Result<()> {
        self.root(&format!(
            "set -e\nmkdir -p '{dir}'\nconstellation mount '{POOL}:{subtree}' '{dir}' \
             --s3 's3://{BUCKET}/{prefix}' --allow-other\ngrep -q ' {dir} ' /proc/mounts"
        ))
        .with_context(|| format!("mounting {POOL}:{subtree} at {dir}"))?;
        self.joined.insert(POOL.to_string());
        Ok(())
    }

    /// The tree under `dir`, as the human sees it.
    pub fn listing(
        &self,
        dir: &str,
    ) -> Result<BTreeMap<std::path::PathBuf, crate::model::Observed>> {
        parse_listing(&self.user(&listing_script(dir))?)
    }

    pub fn verify(&self, dir: &str, model: &Model) -> Result<()> {
        model
            .verify_observed(&self.listing(dir)?)
            .with_context(|| format!("the human's view of {dir}"))
    }

    /// The human's view of `dir` reaches `model` (close-to-open, bounded,
    /// as [`converge`] for a pod).
    pub fn converge(&self, dir: &str, model: &Model) -> Result<()> {
        eventually(
            &format!("the human's view of {dir} matches the model"),
            CONVERGE,
            || self.verify(dir, model),
        )
    }

    /// Leave every pool this machine joined and unmount it.
    pub fn leave(&mut self) -> Result<()> {
        while let Some(fs) = self.joined.pop_first() {
            self.root(&format!(
                "set -e\nconstellation leave '{fs}'\nconstellation umount '{fs}'"
            ))
            .with_context(|| format!("leaving {fs}"))?;
        }
        Ok(())
    }
}

impl Drop for Workstation {
    fn drop(&mut self) {
        // A failed scenario still takes its node out of the pool's roster.
        if let Err(e) = self.leave() {
            eprintln!("   workstation {}: {e:#}", self.name);
        }
    }
}

/// `/volumes/<pv>`: the subtree of a pool volume handle.
fn subtree_of(handle: &str) -> Result<String> {
    let (_, name) = handle
        .rsplit_once("/volumes/")
        .with_context(|| format!("not a pool volume handle: {handle:?}"))?;
    Ok(format!("/volumes/{name}"))
}

/// `NodeGetVolumeHealth` of `volume_id` from `worker`'s node plugin:
/// `{"volume_id", "abnormal", "statuses": [{"status", "reason", "message"}]}`.
fn volume_health(env: &Env, worker: &str, volume_id: &str) -> Result<Value> {
    let pods = env.kube.get(&[
        "pods",
        "-n",
        &env.driver_ns,
        "-l",
        "app.kubernetes.io/component=node",
        "--field-selector",
        &format!("spec.nodeName={worker}"),
    ])?;
    let pod = pods["items"][0]["metadata"]["name"]
        .as_str()
        .with_context(|| format!("no node plugin pod on {worker}"))?
        .to_string();
    let out = env.kube.run(&[
        "exec",
        "-n",
        &env.driver_ns,
        &pod,
        "-c",
        "constellation-csi",
        "--",
        "/usr/local/bin/constellation-csi",
        "--endpoint",
        "unix:///csi/csi.sock",
        "--volume-health",
        volume_id,
    ])?;
    let line = out
        .lines()
        .rev()
        .find(|l| l.starts_with('{'))
        .with_context(|| format!("--volume-health printed no JSON: {out:?}"))?;
    serde_json::from_str(line).context("parsing --volume-health's answer")
}

/// `{ cmd; } 2>&1` run by `run`, refused with `ENOSPC`/`EDQUOT`: the
/// output, which must not report success.
fn refused_for_space(what: &str, out: &str) -> Result<()> {
    ensure!(
        !out.contains("WROTE"),
        "{what}: the write past the volume's quota succeeded"
    );
    ensure!(
        out.contains("No space left on device") || out.contains("Disk quota exceeded"),
        "{what}: refused, but not for space: {out:?}"
    );
    Ok(())
}

/// Plan 37 K7 (settled decision 17): a PV naming a path a human seeded
/// inside a pool, outside `/volumes/`, bound by name, mounted read-only on
/// both workers with no `CreateVolume`.
pub fn csi_static_provisioning(env: &Env, seed: u64) -> Result<()> {
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::new(env, "csi-static")?;
    // A dynamic volume first: once it binds, the pool filesystem exists
    // (its controller-owned engine pod made it).
    s.pvc("anchor", "64Mi", false)?;
    s.wait_bound(Duration::from_secs(300))?;
    let anchor = s.volume_handle("anchor")?;
    let fs = fs_uuid_of(&anchor)?;

    // The dataset, seeded by a human through the CLI.
    let mut human = Workstation::up(env, &s.ns)?;
    human.mount(s.pool_prefix(), "/", "/mnt/pool")?;
    let dataset = "/mnt/pool/datasets/set1";
    human.root(&format!(
        "set -e\nmkdir -p {dataset}\nchown {USER} {dataset}"
    ))?;
    let mut model = Model::default();
    let mut w = RemoteWorkload::new(seed, "ds");
    for block in 0..3 {
        human
            .user(&w.block(dataset, &mut model, 30))
            .with_context(|| format!("seeding block {block}"))?;
    }
    human.verify(dataset, &model)?;
    eprintln!(
        "   the human seeded {dataset} ({} entries) in pool filesystem {fs}",
        model.nodes.len()
    );

    // The PV names it: `<fs-uuid>/datasets/set1`, its pool in
    // `volumeAttributes`; ROX, `Retain`, bound by name.
    let pv = format!("{}-dataset", s.ns);
    let handle = format!("{fs}/datasets/set1");
    let attributes = s.pool_attributes();
    s.static_pv(&pv, &handle, attributes, "ReadOnlyMany", "1Gi")?;
    s.pvc_for_pv("dataset", &pv, "ReadOnlyMany", "1Gi")?;
    s.wait_bound_claims(&["dataset"], Duration::from_secs(120))?;
    ensure!(
        s.volume_handle("dataset")? == handle,
        "the claim is not bound to the static PV {pv}"
    );
    s.spawn_pod_with("r1", w1, &["dataset"], None, true)?;
    s.spawn_pod_with("r2", w2, &["dataset"], None, true)?;
    s.wait_ready("r1", Duration::from_secs(300))?;
    s.wait_ready("r2", Duration::from_secs(300))?;
    ensure!(
        s.staged_on(w1, &handle)? && s.staged_on(w2, &handle)?,
        "{handle} is not staged on both workers"
    );
    converge(&s, "r1", "dataset", &model)?;
    converge(&s, "r2", "dataset", &model)?;
    for pod in ["r1", "r2"] {
        let out = s.exec(
            pod,
            &format!(
                "{{ touch {}/nope; }} 2>&1 && echo WROTE || true",
                data_dir("dataset")
            ),
        )?;
        ensure!(
            !out.contains("WROTE") && out.contains("Read-only file system"),
            "{pod}: a write to the read-only static volume was not refused EROFS: {out:?}"
        );
    }
    eprintln!("   static PV {pv} ({handle}): both workers see the dataset, read-only (EROFS)");

    // No `CreateVolume` made it: the controller never heard its name, and
    // the pool's /volumes holds the anchor alone.
    ensure!(
        !s.controller_log()?.contains(&pv),
        "the controller's log names the static PV {pv}"
    );
    let volumes: Vec<String> = human
        .root("ls /mnt/pool/volumes")?
        .lines()
        .map(str::to_string)
        .collect();
    ensure!(
        volumes
            == [subtree_of(&anchor)?
                .trim_start_matches("/volumes/")
                .to_string()],
        "the pool's /volumes is {volumes:?}: the static PV made a volume"
    );

    // The human extends the dataset; the pods see it on their next open.
    human.user(&w.block(dataset, &mut model, 20))?;
    converge(&s, "r1", "dataset", &model)?;
    converge(&s, "r2", "dataset", &model)?;
    eprintln!(
        "   20 more ops by the human: both pods converged ({} entries)",
        model.nodes.len()
    );
    human.leave()?;
    s.finish()
}

/// Plan 37 K7 (settled decision 18): a human mounts a PV's subtree with
/// the CLI while a pod uses the PV. Both see each other's writes under
/// close-to-open; the PV's quota refuses the human as it does the pod, and
/// counts the human's bytes where the pod sees them; a human deleting a
/// volume is not prevented but surfaces as an abnormal volume condition.
pub fn csi_human_cli_mount(env: &Env, seed: u64) -> Result<()> {
    let w1 = &env.workers[0];
    let mut s = Scope::new(env, "csi-human-cli")?;
    for (claim, size) in [("shared", "1Gi"), ("small", "32Mi"), ("victim", "64Mi")] {
        s.pvc(claim, size, false)?;
    }
    s.wait_bound(Duration::from_secs(300))?;
    s.pod("app", w1, &["shared", "small", "victim"])?;
    let shared = s.volume_handle("shared")?;
    let mut human = Workstation::up(env, &s.ns)?;
    human.mount(s.pool_prefix(), &subtree_of(&shared)?, "/mnt/shared")?;

    // 1. Close-to-open, both ways: one writer at a time, the other
    //    converging before it writes in turn.
    let mut model = Model::default();
    let mut by_pod = RemoteWorkload::new(seed, "pod");
    let mut by_human = RemoteWorkload::new(seed ^ 0x4855_4d41, "human");
    for round in 0..4 {
        converge(&s, "app", "shared", &model)
            .with_context(|| format!("round {round}: the pod catching up"))?;
        s.exec("app", &by_pod.block(&data_dir("shared"), &mut model, 25))
            .with_context(|| format!("round {round}: the pod writing"))?;
        verify(&s, "app", "shared", &model)?;
        human
            .converge("/mnt/shared", &model)
            .with_context(|| format!("round {round}: the human catching up"))?;
        human
            .user(&by_human.block("/mnt/shared", &mut model, 25))
            .with_context(|| format!("round {round}: the human writing"))?;
        human.verify("/mnt/shared", &model)?;
    }
    converge(&s, "app", "shared", &model)?;
    eprintln!(
        "   4 rounds pod({w1}) / human (CLI, outside Kubernetes): both converged on {} entries",
        model.nodes.len()
    );

    // 2. The PV's quota (32 MiB) governs the human's writes: 24 MiB fit,
    //    16 MiB more are refused; the pod sees the human's bytes in its
    //    volume's usage, and is refused too.
    let small = s.volume_handle("small")?;
    human.mount(s.pool_prefix(), &subtree_of(&small)?, "/mnt/small")?;
    human.user("head -c 25165824 /dev/urandom > /mnt/small/human-24m")?;
    let out = human.user(
        "{ head -c 16777216 /dev/urandom > /mnt/small/human-more; } 2>&1 && echo WROTE || true",
    )?;
    refused_for_space("the human", &out)?;
    // What the refused write got in before the cap goes: the volume holds
    // the human's 24 MiB.
    human.user("rm -f /mnt/small/human-more")?;
    let df = format!(
        "df -B1 --output=size,used {} | tail -n 1",
        data_dir("small")
    );
    let mut seen = String::new();
    eventually(
        "the pod's view of the volume counts the human's 24 MiB",
        CONVERGE,
        || {
            seen = s.exec("app", &df)?;
            let v: Vec<u64> = seen
                .split_whitespace()
                .map(|n| n.parse().context("df"))
                .collect::<Result<_>>()?;
            match v.as_slice() {
                [size, used] if *size == 32 << 20 && (22 << 20..=26 << 20).contains(used) => Ok(()),
                _ => bail!("df size/used: {seen:?}"),
            }
        },
    )?;
    let out = s.exec(
        "app",
        &format!(
            "{{ head -c 16777216 /dev/urandom > {}/pod-more; }} 2>&1 && echo WROTE || true",
            data_dir("small")
        ),
    )?;
    refused_for_space("the pod", &out)?;
    eprintln!(
        "   quota: the human's 24 MiB fit the 32 MiB PV, 16 MiB more refused for the human and \
         the pod alike; the pod's df: {}",
        seen.trim()
    );

    // 3. Deleting a volume behind the driver's back: not prevented, but
    //    the node staging it reports it abnormal (and only it).
    let victim = s.volume_handle("victim")?;
    for h in [&victim, &shared] {
        let health = volume_health(env, w1, h)?;
        ensure!(
            health["abnormal"] == false,
            "{h} is abnormal before anything happened: {health}"
        );
    }
    human.mount(s.pool_prefix(), "/", "/mnt/pool")?;
    human
        .root(&format!("rm -rf /mnt/pool{}", subtree_of(&victim)?))
        .context("the human's rm -rf of a volume was refused")?;
    let mut health = Value::Null;
    eventually(
        "the removed volume reported abnormal by its node",
        Duration::from_secs(120),
        || {
            health = volume_health(env, w1, &victim)?;
            let removed = health["statuses"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|e| e["reason"] == "VolumeRemoved" && e["status"] == "DATA_LOSS");
            ensure!(removed, "not yet: {health}");
            Ok(())
        },
    )?;
    let other = volume_health(env, w1, &shared)?;
    ensure!(
        other["abnormal"] == false,
        "the untouched volume is abnormal too: {other}"
    );
    eprintln!(
        "   rm -rf of /volumes/<victim> by the human: done, and reported: {}",
        health["statuses"][0]["message"]
    );
    verify(&s, "app", "shared", &model)?;
    human.leave()?;
    s.finish()
}

/// The shard a pool volume named `name` lands on in a class of `shards`
/// shards: FNV-1a over the name (a copy of `constellation_csi::params::
/// ClassParams::shard_for`; both are pinned to the same values by tests).
fn shard_for(name: &str, shards: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(shards)) as u32
}

/// `(shard, fs uuid, pv name)` of a pool volume handle
/// `v1/pool/<shard>/<fs-uuid>/volumes/<pv>`.
fn parse_pool_handle(handle: &str) -> Result<(u32, String, String)> {
    match handle.split('/').collect::<Vec<_>>().as_slice() {
        ["v1", "pool", shard, uuid, "volumes", name] => Ok((
            shard.parse().context("shard")?,
            uuid.to_string(),
            name.to_string(),
        )),
        _ => bail!("not a pool volume handle: {handle:?}"),
    }
}

/// A node-owned engine pod of a pool shard.
struct ShardEngine {
    pod: String,
    /// Its `constellation.dev/fs-uuid` label.
    uuid: String,
    /// Its `constellation.dev/s3` annotation: the filesystem's location.
    s3: String,
}

/// The scope pool's node-owned engine pods by `(shard, node)`. Two for
/// one `(shard, node)` is an error.
fn node_engines(s: &Scope) -> Result<BTreeMap<(u32, String), ShardEngine>> {
    let list = s.kube().get(&[
        "pods",
        "-n",
        &s.env.driver_ns,
        "-l",
        &format!(
            "app.kubernetes.io/component=engine,constellation.dev/owner=node,constellation.dev/pool={}",
            s.pool_label()
        ),
    ])?;
    let mut out = BTreeMap::new();
    for p in list["items"].as_array().into_iter().flatten() {
        if !p["metadata"]["deletionTimestamp"].is_null() {
            continue;
        }
        let m = &p["metadata"];
        let name = m["name"].as_str().unwrap_or_default().to_string();
        let shard: u32 = m["labels"]["constellation.dev/shard"]
            .as_str()
            .and_then(|v| v.parse().ok())
            .with_context(|| format!("{name} has no shard label"))?;
        let node = p["spec"]["nodeName"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let uuid = m["labels"]["constellation.dev/fs-uuid"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let s3 = m["annotations"]["constellation.dev/s3"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let engine = ShardEngine {
            pod: name.clone(),
            uuid,
            s3,
        };
        if let Some(other) = out.insert((shard, node.clone()), engine) {
            bail!(
                "two engine pods for shard {shard} on {node}: {} and {name}",
                other.pod
            );
        }
    }
    Ok(out)
}

/// Plan 37 K7 (§2.3, settled decisions 7, 8): a `shards: 4` class. Every
/// PV lands on the shard its name hashes to, which its id encodes, in that
/// shard's own filesystem; node-owned engine pods exist exactly for the
/// `(shard, node)` pairs in use, before and after every PV restages on the
/// other worker; a clone stays in its source's shard, and a clone through a
/// class whose pool cannot hold that shard is refused.
pub fn csi_shard_routing(env: &Env, seed: u64) -> Result<()> {
    const PVS: usize = 16;
    const SHARDS: u32 = 4;
    let (w1, w2) = (&env.workers[0], &env.workers[1]);
    let mut s = Scope::with_params(env, "csi-shards", &[("shards", SHARDS.to_string())])?;
    let claim = |i: usize| format!("s-{i:02}");
    for i in 0..PVS {
        s.pvc(&claim(i), "64Mi", false)?;
    }
    s.wait_bound(Duration::from_secs(600))?;

    // 1. Routing: the id's shard is the name's hash, and each shard is one
    //    filesystem of its own.
    let mut shard_of = Vec::new();
    let mut uuid_of_shard: BTreeMap<u32, String> = BTreeMap::new();
    for i in 0..PVS {
        let handle = s.volume_handle(&claim(i))?;
        let (shard, uuid, name) = parse_pool_handle(&handle)?;
        ensure!(
            shard == shard_for(&name, SHARDS),
            "{handle}: shard {shard}, but {name} hashes to shard {}",
            shard_for(&name, SHARDS)
        );
        if let Some(seen) = uuid_of_shard.insert(shard, uuid.clone()) {
            ensure!(
                seen == uuid,
                "shard {shard} is two filesystems: {seen} and {uuid}"
            );
        }
        shard_of.push(shard);
    }
    let uuids: BTreeSet<&String> = uuid_of_shard.values().collect();
    ensure!(
        uuids.len() == uuid_of_shard.len(),
        "two shards share a filesystem: {uuid_of_shard:?}"
    );
    ensure!(
        uuid_of_shard.len() >= 2,
        "{PVS} PVs all hashed to one shard ({uuid_of_shard:?}): nothing to route"
    );
    eprintln!(
        "   {PVS} PVs over shards {:?}, each shard its own filesystem",
        uuid_of_shard.keys().collect::<Vec<_>>()
    );

    // 2. Half the PVs in a pod on each worker, each written with a tree of
    //    its own; the engine pods are exactly the (shard, node) pairs used.
    let half: [Vec<String>; 2] = [
        (0..PVS / 2).map(claim).collect(),
        (PVS / 2..PVS).map(claim).collect(),
    ];
    let mut models = vec![Model::default(); PVS];
    let place = |s: &Scope, round: usize, nodes: [&String; 2]| -> Result<BTreeSet<(u32, String)>> {
        let mut want = BTreeSet::new();
        for (p, claims) in half.iter().enumerate() {
            let pod = format!("p{p}-{round}");
            let c: Vec<&str> = claims.iter().map(String::as_str).collect();
            s.spawn_pod(&pod, nodes[p], &c, None)?;
            for (j, _) in claims.iter().enumerate() {
                want.insert((shard_of[p * PVS / 2 + j], nodes[p].clone()));
            }
        }
        for p in 0..2 {
            s.wait_ready(&format!("p{p}-{round}"), Duration::from_secs(600))?;
        }
        Ok(want)
    };
    let check_engines = |s: &Scope, want: &BTreeSet<(u32, String)>, exact: bool| -> Result<()> {
        let have = node_engines(s)?;
        for (shard, node) in want {
            let ShardEngine { pod, uuid, s3 } = have
                .get(&(*shard, node.clone()))
                .with_context(|| format!("no engine pod for shard {shard} on {node}"))?;
            ensure!(
                uuid == &uuid_of_shard[shard],
                "{pod} (shard {shard}) serves {uuid}, not the shard's {}",
                uuid_of_shard[shard]
            );
            ensure!(
                s3.ends_with(&format!("/shard-{shard}")),
                "{pod} (shard {shard}) reads {s3}"
            );
        }
        let extra: Vec<_> = have.keys().filter(|k| !want.contains(*k)).collect();
        ensure!(
            !exact || extra.is_empty(),
            "engine pods for unused (shard, node) pairs: {extra:?}"
        );
        Ok(())
    };
    let want = place(&s, 0, [w1, w2])?;
    check_engines(&s, &want, true)?;
    for (p, claims) in half.iter().enumerate() {
        let mut script = String::new();
        for (j, c) in claims.iter().enumerate() {
            let i = p * PVS / 2 + j;
            let mut w = RemoteWorkload::new(seed.wrapping_add(i as u64), c);
            script.push_str(&format!(
                "(\n{}\n)\n",
                w.block(&data_dir(c), &mut models[i], 12)
            ));
        }
        s.exec(&format!("p{p}-0"), &format!("set -e\n{script}"))?;
    }
    eprintln!(
        "   engine pods: exactly the {} (shard, node) pairs in use: {want:?}",
        want.len()
    );

    // 3. Every PV restaged on the other worker: the same shard's
    //    filesystem (its tree is there), the engine pods follow, and the
    //    unused ones go once idle.
    s.delete_pods(&["p0-0", "p1-0"])?;
    let moved = place(&s, 1, [w2, w1])?;
    for (p, claims) in half.iter().enumerate() {
        for (j, c) in claims.iter().enumerate() {
            converge(&s, &format!("p{p}-1"), c, &models[p * PVS / 2 + j])?;
        }
    }
    check_engines(&s, &moved, false)?;
    eventually(
        "only the (shard, node) pairs in use keep an engine pod",
        Duration::from_secs(IDLE_TTL_S * 3),
        || check_engines(&s, &moved, true),
    )?;
    eprintln!("   restaged on the other workers: every tree intact, engine pods {moved:?}");

    // 4. Clones stay in their source's shard (not their own name's), one
    //    from each of two shards.
    let mut sources: Vec<usize> = Vec::new();
    for (i, shard) in shard_of.iter().enumerate() {
        if sources.iter().all(|&j| shard_of[j] != *shard) && sources.len() < 2 {
            sources.push(i);
        }
    }
    let mut discriminating = 0;
    for (n, &i) in sources.iter().enumerate() {
        let clone = format!("clone-{n}");
        s.pvc_from(&clone, "64Mi", None, pvc_source(&claim(i)))?;
        s.wait_bound_claims(&[&clone], Duration::from_secs(300))?;
        let (shard, uuid, name) = parse_pool_handle(&s.volume_handle(&clone)?)?;
        ensure!(
            shard == shard_of[i] && uuid == uuid_of_shard[&shard_of[i]],
            "{clone} of {} (shard {}) landed in shard {shard} ({uuid})",
            claim(i),
            shard_of[i]
        );
        if shard_for(&name, SHARDS) != shard {
            discriminating += 1;
        }
    }
    let clones: Vec<String> = (0..sources.len()).map(|n| format!("clone-{n}")).collect();
    let c: Vec<&str> = clones.iter().map(String::as_str).collect();
    s.pod("clones", w1, &c)?;
    for (n, &i) in sources.iter().enumerate() {
        converge(&s, "clones", &clones[n], &models[i])?;
    }
    eprintln!(
        "   {} clones Bound in their sources' shards with their trees ({discriminating} of them \
         named for another shard)",
        sources.len()
    );

    // 5. The same pool through a class of 2 shards: a source in shard 2 or
    //    3 has no shard there, and its clone is refused.
    let two = s.add_class_of_pool("two", &[("shards", "2".to_string())])?;
    let i = (0..PVS)
        .find(|&i| shard_of[i] >= 2)
        .context("no PV in shard 2 or 3 to clone across")?;
    let src_fs = &uuid_of_shard[&shard_of[i]];
    s.pvc_from("clone-x", "64Mi", Some(&two), pvc_source(&claim(i)))?;
    let mut why = String::new();
    eventually(
        "clone-x refused with InvalidArgument",
        Duration::from_secs(180),
        || {
            let warnings = s.pvc_warnings("clone-x")?;
            match warnings
                .iter()
                .find(|m| m.contains("InvalidArgument") && m.contains(src_fs.as_str()))
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
        s.pvc_phase("clone-x")? == "Pending",
        "clone-x is not Pending after its refusal"
    );
    eprintln!(
        "   clone of {} (shard {}) through a 2-shard class of the same pool: refused: {}",
        claim(i),
        shard_of[i],
        why.chars().take(300).collect::<String>()
    );
    s.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pinned to the same values as `constellation_csi::params`'s test of
    /// `shard_for`: the copy must route as the driver does.
    #[test]
    fn shard_for_matches_the_driver() {
        let got: Vec<u32> = ["pvc-0", "pvc-1", "pvc-2", "pvc-3", "pvc-7a1c"]
            .iter()
            .map(|n| shard_for(n, 4))
            .collect();
        assert_eq!(got, [1, 2, 3, 0, 3]);
        assert_eq!(shard_for("anything", 1), 0);
    }

    #[test]
    fn pool_handles_parse() {
        let h = "v1/pool/3/7ed281d6-a873-4852-a0f5-a8664aeedfa0/volumes/pvc-1";
        assert_eq!(
            parse_pool_handle(h).unwrap(),
            (
                3,
                "7ed281d6-a873-4852-a0f5-a8664aeedfa0".into(),
                "pvc-1".into()
            )
        );
        assert_eq!(subtree_of(h).unwrap(), "/volumes/pvc-1");
        assert!(parse_pool_handle("7ed2/datasets/x").is_err());
    }
}
