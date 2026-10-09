//! `harness k8s-scenario`: scenarios driven through a Kubernetes cluster
//! instead of local `Client`s (plan 37 §12). Every file operation runs in a
//! workload pod (`kubectl exec`), every mount is the CSI driver's
//! (`NodeStageVolume` into a node-owned engine pod, `NodePublishVolume` into
//! the pod), and the oracle is the same [`Model`](crate::model::Model) the
//! local scenarios use, compared against a listing taken inside the pod
//! ([`Model::verify_observed`](crate::model::Model::verify_observed)).
//! Cross-node expectations wait with [`eventually`](crate::scenarios::eventually).
//!
//! # The cluster
//!
//! A kind cluster, because the scenarios reach into the nodes (`docker exec`
//! into a node container to read its mount table) and load the image with
//! `kind load`. `--kubeconfig` (or `$KUBECONFIG`) naming a kind cluster
//! reuses it and leaves it running. With neither, the harness creates
//! `kind-harness` (`$KIND_CLUSTER` names another) from
//! `tests/csi/kind-config.yaml` (`$KIND_CONFIG_DRAFT`, `$KIND_NODE_IMAGE` as
//! for `tests/csi/kind-up.sh`), with a private kubeconfig so
//! `~/.kube/config` is not touched, and deletes it at the end (`--keep`
//! leaves it). A cluster of that name that exists already is refused, not
//! deleted: on a shared host it may be another run's. The cluster must
//! have two schedulable workers for the cross-node scenarios; the
//! control-plane node never runs a workload.
//!
//! A reused cluster is taken over, not shared: the run reinstalls the
//! driver's release with only its own values (`helm upgrade --install`
//! without `--reuse-values` resets any others), and
//! `csi-plugin-restart-survives` deletes every node- and controller-plugin
//! pod in the driver namespace. Do not point two runs, or a run and other
//! work, at one cluster. The run's own S3 container and namespaces carry
//! the run id, so those do not collide.
//!
//! The run holds the docker prefix lock (`/tmp/.<prefix>.lock`, the
//! prefix `$CONSTELLATION_HARNESS_DOCKER_PREFIX`, default
//! `constellation-harness`) from before it creates anything until it
//! returns, as `harness run` does: its floci carries the prefix label, so
//! a `harness run` on the same prefix would otherwise sweep it as a
//! leftover mid-run. The flip side: two runs (or a run and a `harness
//! run`) on one prefix are refused; give each its own prefix. `--keep`
//! keeps only the cluster: the floci container is removed when the run
//! ends and the lock is released then (a killed run drops the lock with
//! the process and leaves its floci for the next sweep of the prefix).
//!
//! SIGINT or SIGTERM stops the run in order ([`crate::interrupt`]): the
//! running scenario fails at its next command, and the run removes what
//! it created — its namespaces, StorageClasses and engine pods on a
//! reused or `--keep` cluster, its floci container, and a cluster it
//! created (whose deletion takes everything in it, so the per-scenario
//! teardown is skipped then). A second signal exits at once.
//!
//! # What the run sets up
//!
//! 1. The image (`--image`, default `constellation-csi:dev`): built with
//!    `make csi-image` when it is not in the local docker or `--build` is
//!    given, then `kind load`ed.
//! 2. The chart (`--chart`, default `deploy/helm/constellation-csi`),
//!    `helm upgrade --install`ed into `--namespace` (labelled
//!    PodSecurity `privileged`: the node plugin is).
//! 3. The snapshot CRDs and the snapshot-controller
//!    (`tests/csi/snapshot-crds.sh`, before the chart: its csi-snapshotter
//!    sidecar needs them).
//! 4. A private floci S3 (`<cluster>-k8s-floci-<run id>`) on the `kind`
//!    docker network, in memory, removed at the end; its credentials as
//!    one Secret in the driver namespace, and a second one
//!    (`constellation-k8s-harness-rotating`) that `csi-secret-rotation`
//!    rotates, which the chart lets the plugins watch
//!    (`credentials.watchedSecrets`). floci accepts any key pair, so the
//!    rotation scenario proves the swap from the engines' side (the
//!    generation their S3 clients sign with, the audit log) rather than by
//!    the old pair being refused.
//!
//! Each scenario then gets its own namespace and its own pool StorageClass
//! whose prefix carries the run id. A new prefix is a new pool filesystem,
//! so a reused cluster never meets an engine pod holding state of a
//! previous run's filesystem (an engine pod reopening a filesystem whose
//! S3 vanished crash-loops on its registry record). A scenario ends by
//! deleting its pods and checking that every one of its volumes unstaged
//! (no FUSE mount left at the volume's staging path on any worker), then
//! its namespace (its `VolumeSnapshot`s with it), waiting for its PVs and
//! `VolumeSnapshotContent`s to be deleted, then its StorageClasses and
//! `VolumeSnapshotClass`, and finally removing its pools' engine pods
//! (the driver's own idle GC and purge-worker reap would, minutes later:
//! [`IDLE_TTL_S`], [`HARNESS_PURGE`]). Once deleted
//! they stay deleted: a delete the sidecars repeat after their object is
//! gone starts no engine pod (plan 37 K4 fixed the controller, which used
//! to recreate one to answer it).
//!
//! Without `kubectl`, `helm`, `docker` or `kind` every selected scenario
//! is reported SKIPPED with the missing tool, as `harness run` does for a
//! missing `fio`.

pub mod lifecycle;
pub mod parity;
pub mod pool_access;
pub mod remote;
pub mod scenarios;

use crate::docker::{docker, Container};
use crate::interrupt;
use crate::model::Observed;
use crate::results::{Outcome, RunResults};
use crate::s3env::FLOCI_IMAGE;
use crate::spawn::TiedSpawn;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub use scenarios::{K8sScenario, K8S_SCENARIOS};

/// The cluster the harness creates when it is given none (`$KIND_CLUSTER`
/// overrides).
pub const HARNESS_CLUSTER: &str = "kind-harness";
/// Results-file lane of a `k8s-scenario` run.
pub const DEFAULT_LANE: &str = "linux-k8s-kind";
pub(crate) const BUCKET: &str = "k8s-harness";

/// The engine-pod idle TTL the harness installs the chart with (plan 37
/// K6b): an engine pod idle this long leaves its pool and goes, so a
/// scenario that unstages everything sees its engine pods collected.
pub const IDLE_TTL_S: u64 = 60;

/// The purge worker's settings the harness installs the chart with.
pub struct PurgeSettings {
    pub interval_s: u64,
    pub grace_s: u64,
    pub ops_per_s: u64,
    pub bytes_per_s: u64,
    pub concurrency: u64,
}

/// Fast enough for a scenario, slow enough that the budgets show:
/// `csi-trash-purge-under-load` checks every purged entry kept to them.
pub const HARNESS_PURGE: PurgeSettings = PurgeSettings {
    interval_s: 10,
    // Long enough for `csi-trash-purge-under-load` to look at a trashed,
    // unpurged volume, and for `csi-node-drain` to drain the controller-
    // owned pod's node before the worker purges.
    grace_s: 60,
    ops_per_s: 500,
    bytes_per_s: 16 << 20,
    concurrency: 8,
};
const CREDS_SECRET: &str = "constellation-k8s-harness-creds";
/// The Secret `csi-secret-rotation` rotates (`credentialSource: refreshing`).
pub const ROTATING_SECRET: &str = "constellation-k8s-harness-rotating";
const RELEASE: &str = "constellation-csi";
/// How long one `kubectl exec` may take before the harness calls the
/// mount hung.
const EXEC_TIMEOUT: Duration = Duration::from_secs(300);

/// `harness k8s-scenario`'s options (see `main.rs`).
pub struct Opts {
    pub names: Vec<String>,
    pub all: bool,
    /// The `linux-csi` parity lane ([`parity`]).
    pub parity: bool,
    pub kubeconfig: Option<PathBuf>,
    pub context: Option<String>,
    pub chart: Option<PathBuf>,
    pub image: String,
    pub build: bool,
    pub namespace: String,
    pub seed: u64,
    pub results_json: Option<PathBuf>,
    pub lane: Option<String>,
    pub keep: bool,
    /// Rounds of the selected scenarios (`--repeat`, at least 1).
    pub repeat: u32,
}

/// The repository root the run reads its chart, kind config and scripts
/// from ([`find_repo_root`] of this binary and the working directory).
fn repo_root() -> Result<PathBuf> {
    let exe = std::env::current_exe().ok();
    let cwd = std::env::current_dir().ok();
    find_repo_root(exe.as_deref(), cwd.as_deref()).with_context(|| {
        format!(
            "no Constellation checkout above the harness binary ({}) or the working directory \
             ({}): run it from one",
            exe.as_deref().unwrap_or(Path::new("?")).display(),
            cwd.as_deref().unwrap_or(Path::new("?")).display()
        )
    })
}

/// The checkout the binary `exe` was built in (`<root>/target/<profile>/
/// harness`), else the working directory `cwd` or the nearest parent of it
/// that is one. Never the compile-time `CARGO_MANIFEST_DIR`: on a host with
/// several worktrees that names whichever checkout compiled the crate, and
/// a run then took another branch's chart and kind config.
fn find_repo_root(exe: Option<&Path>, cwd: Option<&Path>) -> Option<PathBuf> {
    let is_checkout = |d: &&Path| {
        d.join("deploy/helm/constellation-csi").is_dir() && d.join("tests/csi").is_dir()
    };
    exe.into_iter()
        .flat_map(|e| e.ancestors().skip(1))
        .chain(cwd.into_iter().flat_map(Path::ancestors))
        .find(is_checkout)
        .map(Path::to_path_buf)
}

fn kind_bin() -> String {
    std::env::var("KIND_BIN").unwrap_or_else(|_| "kind".into())
}

/// Run `cmd` to completion (feeding it `stdin`), killing it after
/// `timeout`; a non-zero exit is an error carrying its stderr.
fn run_cmd(mut cmd: Command, stdin: Option<&[u8]>, timeout: Duration) -> Result<String> {
    let what = format!("{cmd:?}");
    let what: String = what.chars().take(300).collect();
    interrupt::check().with_context(|| format!("not starting {what}"))?;
    cmd.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    })
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let mut child = cmd
        .spawn_tied()
        .with_context(|| format!("spawning {what}"))?;
    let feeder = stdin.map(|data| {
        let mut pipe = child.stdin.take().expect("piped stdin");
        let data = data.to_vec();
        std::thread::spawn(move || {
            let _ = pipe.write_all(&data);
        })
    });
    // Drain both pipes on threads so a chatty child cannot block on a
    // full pipe while we wait for it.
    let mut out_pipe = child.stdout.take().expect("piped stdout");
    let mut err_pipe = child.stderr.take().expect("piped stderr");
    let out_t = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut out_pipe, &mut v);
        v
    });
    let err_t = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut err_pipe, &mut v);
        v
    });
    let start = Instant::now();
    let status = loop {
        if let Some(st) = child.try_wait()? {
            break st;
        }
        if start.elapsed() > timeout {
            let _ = child.kill();
            let _ = child.wait();
            bail!("{what} did not finish within {timeout:?} (killed)");
        }
        if interrupt::aborting() {
            let _ = child.kill();
            let _ = child.wait();
            bail!("{what} interrupted (killed)");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    if let Some(f) = feeder {
        let _ = f.join();
    }
    let out = String::from_utf8_lossy(&out_t.join().unwrap_or_default()).into_owned();
    let err = String::from_utf8_lossy(&err_t.join().unwrap_or_default()).into_owned();
    if !status.success() {
        bail!("{what} failed ({status}): {}{}", err.trim(), {
            let o = out.trim();
            if o.is_empty() {
                String::new()
            } else {
                format!("\nstdout: {}", o.chars().take(2000).collect::<String>())
            }
        });
    }
    Ok(out)
}

/// `kubectl`/`helm` against one cluster, never the ambient context.
#[derive(Clone)]
pub struct Kube {
    kubeconfig: PathBuf,
    context: Option<String>,
}

impl Kube {
    /// A `kubectl` command against this cluster, for a caller that runs it
    /// itself (a follower that outlives one call).
    pub fn kubectl_command(&self) -> Command {
        self.kubectl()
    }

    fn kubectl(&self) -> Command {
        let mut c = Command::new("kubectl");
        c.arg("--kubeconfig").arg(&self.kubeconfig);
        if let Some(ctx) = &self.context {
            c.arg("--context").arg(ctx);
        }
        c
    }

    pub fn run(&self, args: &[&str]) -> Result<String> {
        let mut c = self.kubectl();
        c.args(args);
        run_cmd(c, None, EXEC_TIMEOUT)
    }

    pub fn run_stdin(&self, args: &[&str], stdin: &[u8], timeout: Duration) -> Result<String> {
        let mut c = self.kubectl();
        c.args(args);
        run_cmd(c, Some(stdin), timeout)
    }

    /// `kubectl get ... -o json`.
    pub fn get(&self, args: &[&str]) -> Result<Value> {
        let mut a = vec!["get"];
        a.extend_from_slice(args);
        a.extend_from_slice(&["-o", "json"]);
        let out = self.run(&a)?;
        serde_json::from_str(&out).context("parsing kubectl get output")
    }

    pub fn apply(&self, obj: &Value) -> Result<()> {
        self.run_stdin(
            &["apply", "-f", "-"],
            obj.to_string().as_bytes(),
            EXEC_TIMEOUT,
        )
        .map(|_| ())
    }

    /// Run `script` with `sh` in container `c` of `pod`, the script on
    /// stdin (so it may carry file contents of any size).
    pub fn exec(&self, ns: &str, pod: &str, script: &str) -> Result<String> {
        self.run_stdin(
            &["exec", "-i", "-n", ns, pod, "-c", "c", "--", "sh", "-s"],
            script.as_bytes(),
            EXEC_TIMEOUT,
        )
        .with_context(|| format!("exec in {ns}/{pod}"))
    }

    /// One control call to an engine pod's daemon, over `kubectl exec` of
    /// `constellation control-relay` (as the controller reaches its pods):
    /// the relay runs as the engine's uid, the daemon's owner.
    pub fn engine_call(&self, ns: &str, pod: &str, method: &str, params: Value) -> Result<Value> {
        use constellation_control::transport::StreamTransport;
        use constellation_control::{Client, ClientOptions, Principal};
        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.arg("--kubeconfig").arg(&self.kubeconfig);
        if let Some(ctx) = &self.context {
            cmd.arg("--context").arg(ctx);
        }
        cmd.args(["exec", "-i", "-n", ns, pod, "-c", "engine", "--"])
            .args([
                "/usr/local/bin/constellation",
                "control-relay",
                "--socket",
                "/run/constellation-csi/control.sock",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        crate::client::control_runtime().block_on(async {
            let call = async {
                let mut relay = cmd.spawn().context("spawning kubectl exec")?;
                let stream = tokio::io::join(
                    relay.stdout.take().context("relay stdout")?,
                    relay.stdin.take().context("relay stdin")?,
                );
                let transport =
                    std::sync::Arc::new(StreamTransport::new(stream, Principal::InProcess));
                let client = Client::from_transport(transport, ClientOptions::default())
                    .await
                    .map_err(|e| anyhow::anyhow!("handshake with {pod}: {}", e.message))?;
                let out = client
                    .call_json(method, params)
                    .await
                    .map_err(|e| anyhow::anyhow!("{method} on {pod}: {}", e.message));
                drop(client);
                let _ = relay.kill().await;
                out
            };
            tokio::time::timeout(Duration::from_secs(60), call)
                .await
                .map_err(|_| anyhow::anyhow!("{method} on {pod}: no answer within 60 s"))?
        })
    }

    fn helm(&self) -> Command {
        let mut c = Command::new("helm");
        c.arg("--kubeconfig").arg(&self.kubeconfig);
        if let Some(ctx) = &self.context {
            c.arg("--kube-context").arg(ctx);
        }
        c
    }
}

/// The kind cluster of a run: reused (left running) or created (deleted
/// on drop unless `--keep`).
pub struct Cluster {
    pub name: String,
    pub kube: Kube,
    pub workers: Vec<String>,
    created: bool,
    keep: bool,
    _kubeconfig_dir: Option<tempfile::TempDir>,
}

impl Cluster {
    fn acquire(opts: &Opts, root: &Path) -> Result<Self> {
        let given = opts
            .kubeconfig
            .clone()
            .or_else(|| std::env::var_os("KUBECONFIG").map(PathBuf::from))
            .filter(|p| !p.as_os_str().is_empty());
        if opts.kubeconfig.is_none() {
            if let Some(list) = given
                .as_ref()
                .filter(|p| std::env::split_paths(p).count() > 1)
            {
                bail!(
                    "$KUBECONFIG is a list ({}): name the one file with --kubeconfig",
                    list.display()
                );
            }
        }
        if let Some(kubeconfig) = given {
            let kube = Kube {
                kubeconfig: kubeconfig.clone(),
                context: opts.context.clone(),
            };
            let (name, workers) = kind_cluster_of(&kube).with_context(|| {
                format!("the cluster {} names is not usable", kubeconfig.display())
            })?;
            eprintln!(
                "=== reusing kind cluster {name} ({}), workers {}",
                kubeconfig.display(),
                workers.join(", ")
            );
            return Ok(Self {
                name,
                kube,
                workers,
                created: false,
                keep: true,
                _kubeconfig_dir: None,
            });
        }
        let dir = tempfile::tempdir()?;
        let kubeconfig = dir.path().join("kubeconfig");
        let kind = kind_bin();
        let name = std::env::var("KIND_CLUSTER")
            .ok()
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| HARNESS_CLUSTER.into());
        if docker(&["inspect", &format!("{name}-control-plane")]).is_ok() {
            // Not deleted: on a shared host it may be another run's.
            bail!(
                "a kind cluster {name} exists already: reuse it (--kubeconfig <(kind get kubeconfig \
                 --name {name})), delete it (kind delete cluster --name {name}), or name another \
                 one with $KIND_CLUSTER"
            );
        }
        let config = std::env::var_os("KIND_CONFIG_DRAFT")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join("tests/csi/kind-config.yaml"));
        eprintln!("=== creating kind cluster {name} from {}", config.display());
        let mut c = Command::new(&kind);
        c.args(["create", "cluster", "--name", &name, "--config"])
            .arg(&config)
            .arg("--kubeconfig")
            .arg(&kubeconfig)
            .args(["--wait", "180s"]);
        if let Ok(img) = std::env::var("KIND_NODE_IMAGE") {
            if !img.is_empty() {
                c.args(["--image", &img]);
            }
        }
        if let Err(e) = run_cmd(c, None, Duration::from_secs(600)) {
            // A failed `kind create` deletes its partial cluster itself
            // (no `--retain`), and one that failed because the name
            // exists (another run's, created since the check above) must
            // not be deleted: no cleanup, except for a create this run
            // killed, which was past kind's own existence check.
            if interrupt::interrupted() {
                let _t = interrupt::Teardown::begin();
                eprintln!("=== deleting the half-created kind cluster {name}");
                let mut d = Command::new(&kind);
                d.args(["delete", "cluster", "--name", &name]);
                if let Err(e) = run_cmd(d, None, Duration::from_secs(300)) {
                    eprintln!("kind delete cluster {name}: {e:#}");
                }
            }
            return Err(e.context("kind create cluster"));
        }
        // From here on the cluster exists and is this run's: `Drop`
        // deletes it.
        let mut cluster = Self {
            name,
            kube: Kube {
                kubeconfig,
                context: None,
            },
            workers: Vec::new(),
            created: true,
            keep: opts.keep,
            _kubeconfig_dir: Some(dir),
        };
        let (_, workers) = kind_cluster_of(&cluster.kube)?;
        cluster.workers = workers;
        Ok(cluster)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.created {
            return;
        }
        if self.keep {
            eprintln!(
                "=== keeping kind cluster {} (kubeconfig {})",
                self.name,
                self.kube.kubeconfig.display()
            );
            // The kubeconfig lives in a temp dir: keep it too.
            if let Some(d) = self._kubeconfig_dir.take() {
                let _ = d.keep();
            }
            return;
        }
        let _t = interrupt::Teardown::begin();
        eprintln!("=== deleting kind cluster {}", self.name);
        let mut c = Command::new(kind_bin());
        c.args(["delete", "cluster", "--name", &self.name]);
        if let Err(e) = run_cmd(c, None, Duration::from_secs(300)) {
            eprintln!("kind delete cluster {}: {e:#}", self.name);
        }
    }
}

/// The kind cluster `kube` reaches (from its nodes' `kind://` provider
/// ids) and its workers, sorted.
fn kind_cluster_of(kube: &Kube) -> Result<(String, Vec<String>)> {
    let nodes = kube.get(&["nodes"])?;
    let mut cluster = None;
    let mut workers = Vec::new();
    for n in nodes["items"].as_array().into_iter().flatten() {
        let name = n["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let provider = n["spec"]["providerID"].as_str().unwrap_or_default();
        // kind://docker/<cluster>/<node>
        let parts: Vec<&str> = provider.split('/').collect();
        if !provider.starts_with("kind://") || parts.len() < 5 {
            bail!("node {name} is not a kind node (providerID {provider:?})");
        }
        cluster = Some(parts[3].to_string());
        let labels = &n["metadata"]["labels"];
        if labels
            .get("node-role.kubernetes.io/control-plane")
            .is_none()
        {
            workers.push(name);
        }
    }
    workers.sort();
    let cluster = cluster.context("the cluster has no nodes")?;
    Ok((cluster, workers))
}

/// Everything the scenarios share: the cluster, the installed driver, an
/// S3 the cluster reaches.
pub struct Env {
    pub kube: Kube,
    pub cluster: String,
    pub workers: Vec<String>,
    /// The driver's namespace.
    pub driver_ns: String,
    /// The driver image; workload pods run it too (it has `sh` and
    /// coreutils, and it is already on every node).
    pub image: String,
    pub endpoint: String,
    /// Unique per run: prefixes, StorageClass, namespace and floci
    /// container names carry it.
    pub run_id: String,
    /// The cluster goes with the run (created, no `--keep`): an
    /// interrupted scenario leaves its objects to the cluster's deletion.
    disposable: bool,
    /// The chart and the image it runs now, which
    /// `csi-engine-pod-handoff-under-load` moves between `image` and
    /// [`Env::next_image`].
    chart: PathBuf,
    chart_image: Mutex<String>,
    next_image: OnceLock<String>,
    /// The `--repeat` round running now (0 without `--repeat`): a scope's
    /// namespace and pool carry it, so every round starts on a filesystem
    /// of its own.
    round: AtomicU32,
    /// What the running scenario measured ([`Env::measure`]).
    measurements: Mutex<serde_json::Map<String, Value>>,
    floci: Container,
}

impl Env {
    fn up(cluster: &Cluster, opts: &Opts, root: &Path) -> Result<Self> {
        let kube = cluster.kube.clone();
        let image = opts.image.clone();
        let run_id = format!(
            "{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            std::process::id()
        );
        if opts.build || docker(&["image", "inspect", &image]).is_err() {
            eprintln!("=== building {image} (make csi-image)");
            let mut make = Command::new("make")
                .arg("-C")
                .arg(root)
                .arg("csi-image")
                .arg(format!("CSI_IMAGE={image}"))
                .spawn_tied()
                .context("running make csi-image")?;
            let st = loop {
                if let Some(st) = make.try_wait()? {
                    break st;
                }
                if interrupt::aborting() {
                    let _ = make.kill();
                    let _ = make.wait();
                    bail!("make csi-image interrupted (killed)");
                }
                std::thread::sleep(Duration::from_millis(200));
            };
            if !st.success() {
                bail!("make csi-image CSI_IMAGE={image} failed ({st})");
            }
        }
        eprintln!("=== loading {image} into {}", cluster.name);
        let mut c = Command::new(kind_bin());
        c.args(["load", "docker-image", &image, "--name", &cluster.name]);
        run_cmd(c, None, Duration::from_secs(600)).context("kind load docker-image")?;

        let ns = opts.namespace.clone();
        kube.apply(&json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": ns, "labels": {"pod-security.kubernetes.io/enforce": "privileged"}}
        }))?;
        eprintln!("=== snapshot CRDs and snapshot-controller (tests/csi/snapshot-crds.sh)");
        let mut crds = Command::new("bash");
        crds.arg(root.join("tests/csi/snapshot-crds.sh"))
            .env("KUBECONFIG", &kube.kubeconfig);
        if let Some(ctx) = &kube.context {
            crds.env("KUBE_CONTEXT", ctx);
        }
        run_cmd(crds, None, Duration::from_secs(600)).context("tests/csi/snapshot-crds.sh")?;
        let chart = opts
            .chart
            .clone()
            .unwrap_or_else(|| root.join("deploy/helm/constellation-csi"));
        let (repo, tag) = split_image(&image);
        eprintln!("=== helm upgrade --install {RELEASE} {}", chart.display());
        let mut h = kube.helm();
        h.args(["upgrade", "--install", RELEASE])
            .arg(&chart)
            .args(["-n", &ns])
            .args(["--set", &format!("image.repository={repo}")])
            .args(["--set", &format!("image.tag={tag}")])
            .args([
                "--set",
                &format!("credentials.watchedSecrets[0].namespace={ns}"),
                "--set",
                &format!("credentials.watchedSecrets[0].name={ROTATING_SECRET}"),
            ])
            // Plan 37 K6b: the idle GC, the drain watch and the purge
            // worker on scenario timescales ([`HARNESS_PURGE`]).
            .args(["--set", &format!("engineProfile.idleTtl={IDLE_TTL_S}s")])
            .args(["--set", "engineProfile.idleGcInterval=5"])
            .args([
                "--set",
                &format!("purge.interval={}s", HARNESS_PURGE.interval_s),
            ])
            .args(["--set", &format!("purge.grace={}s", HARNESS_PURGE.grace_s)])
            .args([
                "--set",
                &format!("purge.opsPerSecond={}", HARNESS_PURGE.ops_per_s),
                "--set",
                &format!("purge.bytesPerSecond={}", HARNESS_PURGE.bytes_per_s),
                "--set",
                &format!("purge.maxConcurrentDeletes={}", HARNESS_PURGE.concurrency),
            ])
            .args(["--wait", "--timeout", "300s"]);
        run_cmd(h, None, Duration::from_secs(420)).context("helm upgrade --install")?;
        for what in [
            "deploy/constellation-csi-controller",
            "ds/constellation-csi-node",
        ] {
            kube.run(&["-n", &ns, "rollout", "status", what, "--timeout=300s"])?;
        }

        let floci_name = format!("{}-k8s-floci-{run_id}", cluster.name);
        eprintln!("=== S3 (floci, in memory) {floci_name} on the kind network");
        let floci = Container::run(
            &floci_name,
            FLOCI_IMAGE,
            &[
                "--network",
                "kind",
                "--security-opt",
                "label=disable",
                "-e",
                "FLOCI_STORAGE_MODE=memory",
            ],
        )?;
        crate::scenarios::eventually("floci healthy", Duration::from_secs(90), || {
            docker(&[
                "exec",
                &floci_name,
                "curl",
                "-sf",
                "http://localhost:4566/_floci/health",
            ])
            .map(|_| ())
        })?;
        docker(&[
            "exec",
            "-e",
            "AWS_ACCESS_KEY_ID=test",
            "-e",
            "AWS_SECRET_ACCESS_KEY=test",
            "-e",
            "AWS_DEFAULT_REGION=us-east-1",
            &floci_name,
            "aws",
            "--endpoint-url",
            "http://localhost:4566",
            "s3",
            "mb",
            &format!("s3://{BUCKET}"),
        ])?;
        let ip = docker(&[
            "inspect",
            "-f",
            "{{(index .NetworkSettings.Networks \"kind\").IPAddress}}",
            &floci_name,
        ])?;
        let endpoint = format!("http://{ip}:4566");
        kube.apply(&json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": CREDS_SECRET, "namespace": ns},
            "stringData": {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
        }))?;
        kube.apply(&rotating_secret(&ns, 1))?;
        Ok(Self {
            kube,
            cluster: cluster.name.clone(),
            workers: cluster.workers.clone(),
            driver_ns: ns,
            chart_image: Mutex::new(image.clone()),
            image,
            endpoint,
            run_id,
            disposable: cluster.created && !cluster.keep,
            chart,
            next_image: OnceLock::new(),
            round: AtomicU32::new(0),
            measurements: Mutex::new(serde_json::Map::new()),
            floci,
        })
    }

    /// The suffix of this round's names: the run id, and the round when
    /// the scenarios repeat.
    pub fn scope_id(&self) -> String {
        match self.round.load(Ordering::Relaxed) {
            0 => self.run_id.clone(),
            r => format!("{}-r{r}", self.run_id),
        }
    }

    /// The keys under `prefix/` in the harness bucket, as the S3 server
    /// (floci) lists them.
    pub fn bucket_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let out = docker(&[
            "exec",
            "-e",
            "AWS_ACCESS_KEY_ID=test",
            "-e",
            "AWS_SECRET_ACCESS_KEY=test",
            "-e",
            "AWS_DEFAULT_REGION=us-east-1",
            &self.floci.name,
            "aws",
            "--endpoint-url",
            "http://localhost:4566",
            "s3api",
            "list-objects-v2",
            "--bucket",
            BUCKET,
            "--prefix",
            &format!("{}/", prefix.trim_end_matches('/')),
            "--query",
            "Contents[].Key",
            "--output",
            "text",
        ])?;
        // `None` when nothing matches; keys are tab-separated, a line per
        // page.
        Ok(out
            .split_whitespace()
            .filter(|k| *k != "None")
            .map(str::to_string)
            .collect())
    }

    /// Record a measurement of the running scenario (reported with its
    /// outcome, and in `--results-json`).
    pub fn measure(&self, key: &str, value: impl Into<Value>) {
        let value = value.into();
        eprintln!("   measured {key} = {value}");
        self.measurements
            .lock()
            .unwrap()
            .insert(key.to_string(), value);
    }

    fn take_measurements(&self) -> Option<Value> {
        let m = std::mem::take(&mut *self.measurements.lock().unwrap());
        (!m.is_empty()).then_some(Value::Object(m))
    }

    /// The image the chart runs now.
    pub fn chart_image(&self) -> String {
        self.chart_image.lock().unwrap().clone()
    }

    /// The driver image under a second tag (`<tag>-next`), loaded into the
    /// cluster on first use: to the chart an image change, with the very
    /// same binary (what a rollout reacts to is the spec, not the bytes).
    pub fn next_image(&self) -> Result<String> {
        if let Some(i) = self.next_image.get() {
            return Ok(i.clone());
        }
        let (repo, tag) = split_image(&self.image);
        let next = format!("{repo}:{tag}-next");
        docker(&["tag", &self.image, &next])?;
        eprintln!("=== loading {next} into {}", self.cluster);
        let mut c = Command::new(kind_bin());
        c.args(["load", "docker-image", &next, "--name", &self.cluster]);
        run_cmd(c, None, Duration::from_secs(600)).context("kind load docker-image")?;
        Ok(self.next_image.get_or_init(|| next).clone())
    }

    /// `helm upgrade` the driver's release onto `image`, every other value
    /// kept. No `--wait`: the subject of the caller is the rollout that
    /// follows (kubelet may take minutes to roll the node DaemonSet on a
    /// loaded host), which it polls for itself.
    pub fn set_chart_image(&self, image: &str) -> Result<()> {
        let (repo, tag) = split_image(image);
        let mut h = self.kube.helm();
        h.args(["upgrade", RELEASE])
            .arg(&self.chart)
            .args(["-n", &self.driver_ns, "--reuse-values"])
            .args(["--set", &format!("image.repository={repo}")])
            .args(["--set", &format!("image.tag={tag}")]);
        run_cmd(h, None, Duration::from_secs(300)).context("helm upgrade")?;
        *self.chart_image.lock().unwrap() = image.to_string();
        Ok(())
    }

    /// What a failed scenario leaves for the reader: pods everywhere, the
    /// plugins' and the engines' recent logs.
    fn diagnostics(&self, ns: Option<&str>) {
        eprintln!("--- diagnostics");
        if let Ok(out) = self.kube.run(&["get", "pods", "-A", "-o", "wide"]) {
            eprintln!("{out}");
        }
        if let Some(ns) = ns {
            if let Ok(out) = self.kube.run(&["-n", ns, "get", "pvc"]) {
                eprintln!("{out}");
            }
            if let Ok(out) = self.kube.run(&[
                "-n",
                ns,
                "get",
                "events",
                "--field-selector",
                "type=Warning",
            ]) {
                eprintln!("{out}");
            }
        }
        for (sel, c) in [
            ("app.kubernetes.io/component=node", "constellation-csi"),
            (
                "app.kubernetes.io/component=controller",
                "constellation-csi",
            ),
            ("app.kubernetes.io/component=engine", "engine"),
        ] {
            if let Ok(out) = self.kube.run(&[
                "-n",
                &self.driver_ns,
                "logs",
                "-l",
                sel,
                "-c",
                c,
                "--tail=40",
                "--prefix",
            ]) {
                eprintln!("{out}");
            }
        }
        eprintln!("--- end of diagnostics");
    }
}

/// `repo[:tag]` of an image reference; a ':' before the last '/' is a
/// registry port.
fn split_image(image: &str) -> (&str, &str) {
    match image.rsplit_once('/') {
        Some((_, last)) if last.contains(':') => image.rsplit_once(':').unwrap(),
        None if image.contains(':') => image.rsplit_once(':').unwrap(),
        _ => (image, "latest"),
    }
}

/// The rotating Secret's `generation`th key pair: fixture values for floci,
/// which takes any. Never printed by the scenarios all the same.
pub fn rotating_secret(ns: &str, generation: u32) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": ROTATING_SECRET, "namespace": ns},
        "stringData": {
            "aws_access_key_id": rotating_key(generation),
            "aws_secret_access_key": format!("k8s-harness-rotation-secret-{generation}"),
        }
    })
}

/// The access key id of [`rotating_secret`]'s `generation`th pair.
pub fn rotating_key(generation: u32) -> String {
    format!("k8s-harness-rotation-key-{generation}")
}

/// One scenario's Kubernetes objects: a namespace for its PVCs and pods,
/// a pool StorageClass of its own. `finish` tears them down and checks
/// the teardown; `Drop` is the backstop when a scenario fails first.
pub struct Scope<'a> {
    pub env: &'a Env,
    pub ns: String,
    /// The scope's first pool StorageClass.
    pub sc: String,
    /// Its bucket prefix (the pool's, or its shards' parent).
    prefix: String,
    /// Every StorageClass of the scope ([`Scope::add_class`]), the first
    /// included, with the driver's `constellation.replicix.com/pool` label of its
    /// pool.
    classes: Vec<(String, String)>,
    /// The Secret every class of the scope reads (provisioner and
    /// node-stage), and the parameters they carry besides.
    secret: String,
    extra: Vec<(String, String)>,
    /// The scope's `VolumeSnapshotClass`, once made.
    snapshot_class: Option<String>,
    pvcs: Vec<String>,
    /// Statically provisioned PVs ([`Scope::static_pv`]): `Retain`, so
    /// the teardown deletes them itself.
    static_pvs: Vec<String>,
    /// Set by [`Scope::expect_no_engine_return`]: after the teardown, wait
    /// and check no engine pod of the scope's pools comes back.
    no_engine_return: bool,
    done: bool,
}

/// How long [`Scope::expect_no_engine_return`] watches: the external
/// provisioner's repeated `DeleteVolume` was seen 18 s after the PV went, and
/// its retry backoff grows from seconds to minutes, so 60 s is over three
/// times the observed repeat and past the first backoff steps.
const NO_ENGINE_RETURN: Duration = Duration::from_secs(60);

/// A pod's volume: PVC `claim` at `/data/<claim>`.
pub fn data_dir(claim: &str) -> String {
    format!("/data/{claim}")
}

impl<'a> Scope<'a> {
    pub fn new(env: &'a Env, scenario: &str) -> Result<Self> {
        Self::with_class(env, scenario, CREDS_SECRET, &[])
    }

    /// [`Scope::new`] whose class carries `extra` parameters besides.
    pub fn with_params(env: &'a Env, scenario: &str, extra: &[(&str, String)]) -> Result<Self> {
        Self::with_class(env, scenario, CREDS_SECRET, extra)
    }

    /// [`Scope::new`] whose class reads `secret` (provisioner and
    /// node-stage) and carries `extra` parameters besides.
    pub fn with_class(
        env: &'a Env,
        scenario: &str,
        secret: &str,
        extra: &[(&str, String)],
    ) -> Result<Self> {
        let ns = format!("k8s-{scenario}-{}", env.scope_id());
        env.kube.apply(&json!({
            "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}
        }))?;
        // From here on `Drop` removes the namespace (and the classes made
        // so far).
        let prefix = format!("k8s-harness/{}/{scenario}", env.scope_id());
        let mut scope = Self {
            env,
            sc: ns.clone(),
            prefix: prefix.clone(),
            ns,
            classes: Vec::new(),
            secret: secret.to_string(),
            extra: extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            snapshot_class: None,
            pvcs: Vec::new(),
            static_pvs: Vec::new(),
            no_engine_return: false,
            done: false,
        };
        let sc = scope.sc.clone();
        scope.class(&sc, &prefix, &[])?;
        Ok(scope)
    }

    /// A pool StorageClass `name` of its own pool at `prefix`, its
    /// parameters the scope's with `overrides` on top.
    fn class(&mut self, name: &str, prefix: &str, overrides: &[(&str, String)]) -> Result<()> {
        let env = self.env;
        let mut params = serde_json::Map::new();
        for (k, v) in [
            ("bucket", BUCKET.to_string()),
            ("prefix", prefix.to_string()),
            ("endpoint", env.endpoint.clone()),
            ("region", "us-east-1".into()),
            ("layout", "pool".into()),
            ("chunkSize", "1MiB".into()),
            (
                "csi.storage.k8s.io/provisioner-secret-name",
                self.secret.clone(),
            ),
            (
                "csi.storage.k8s.io/provisioner-secret-namespace",
                env.driver_ns.clone(),
            ),
            (
                "csi.storage.k8s.io/node-stage-secret-name",
                self.secret.clone(),
            ),
            (
                "csi.storage.k8s.io/node-stage-secret-namespace",
                env.driver_ns.clone(),
            ),
        ] {
            params.insert(k.into(), Value::String(v));
        }
        for (k, v) in &self.extra {
            params.insert(k.clone(), Value::String(v.clone()));
        }
        for (k, v) in overrides {
            params.insert(k.to_string(), Value::String(v.clone()));
        }
        self.classes
            .push((name.to_string(), pool_label(&env.endpoint, BUCKET, prefix)));
        env.kube.apply(&json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": name},
            "provisioner": "constellation.csi.replicix.com",
            "parameters": params,
            "reclaimPolicy": "Delete",
            "volumeBindingMode": "Immediate",
            "allowVolumeExpansion": true,
        }))
    }

    /// Another pool StorageClass of the scope, `<sc>-<suffix>`: a pool of
    /// its own (its prefix differs), so a filesystem of its own.
    pub fn add_class(&mut self, suffix: &str) -> Result<String> {
        let name = format!("{}-{suffix}", self.sc);
        let prefix = format!("k8s-harness/{}/{}-{suffix}", self.env.run_id, self.ns);
        self.class(&name, &prefix, &[])?;
        Ok(name)
    }

    /// Another StorageClass `<sc>-<suffix>` of the scope's *own* pool (the
    /// same bucket and prefix), its parameters the scope's with
    /// `overrides` on top: e.g. the same pool seen with another `shards`.
    pub fn add_class_of_pool(
        &mut self,
        suffix: &str,
        overrides: &[(&str, String)],
    ) -> Result<String> {
        let name = format!("{}-{suffix}", self.sc);
        let prefix = self.prefix.clone();
        self.class(&name, &prefix, overrides)?;
        Ok(name)
    }

    /// The scope's pool's bucket prefix (a sharded pool's shards live
    /// under it, `shard-<k>`).
    pub fn pool_prefix(&self) -> &str {
        &self.prefix
    }

    /// The driver's `constellation.replicix.com/pool` label of the scope's pool
    /// (shared by all its shards).
    pub fn pool_label(&self) -> &str {
        &self.classes[0].1
    }

    /// The scope's pool as a static PV's `volumeAttributes` spell it
    /// (plan 37 settled decision 17): the class parameters without the
    /// sidecars' secret references.
    pub fn pool_attributes(&self) -> serde_json::Map<String, Value> {
        let mut attrs = serde_json::Map::new();
        for (k, v) in [
            ("bucket", BUCKET.to_string()),
            ("prefix", self.prefix.clone()),
            ("endpoint", self.env.endpoint.clone()),
            ("region", "us-east-1".into()),
            ("layout", "pool".into()),
            ("chunkSize", "1MiB".into()),
        ] {
            attrs.insert(k.into(), Value::String(v));
        }
        for (k, v) in &self.extra {
            attrs.insert(k.clone(), Value::String(v.clone()));
        }
        attrs
    }

    /// A statically provisioned PV `name` (cluster-scoped: the caller makes
    /// it unique) whose `volumeHandle` is `handle`, located by
    /// `attributes`, staged with the scope's Secret; `Retain`, no
    /// StorageClass. Deleted by the teardown.
    pub fn static_pv(
        &mut self,
        name: &str,
        handle: &str,
        attributes: serde_json::Map<String, Value>,
        mode: &str,
        size: &str,
    ) -> Result<()> {
        self.static_pvs.push(name.to_string());
        self.env.kube.apply(&json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name},
            "spec": {
                "accessModes": [mode],
                "capacity": {"storage": size},
                "persistentVolumeReclaimPolicy": "Retain",
                "storageClassName": "",
                "csi": {
                    "driver": "constellation.csi.replicix.com",
                    "volumeHandle": handle,
                    "volumeAttributes": attributes,
                    "nodeStageSecretRef": {"name": self.secret, "namespace": self.env.driver_ns},
                },
            }
        }))
    }

    /// PVC `name` bound to PV `pv` by name (static provisioning: no class).
    pub fn pvc_for_pv(&mut self, name: &str, pv: &str, mode: &str, size: &str) -> Result<()> {
        self.env.kube.apply(&json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": self.ns},
            "spec": {
                "accessModes": [mode],
                "storageClassName": "",
                "volumeName": pv,
                "resources": {"requests": {"storage": size}},
            }
        }))?;
        self.pvcs.push(name.to_string());
        Ok(())
    }

    /// The scope's `VolumeSnapshotClass` (made on first use).
    pub fn snapshot_class(&mut self) -> Result<String> {
        if let Some(c) = &self.snapshot_class {
            return Ok(c.clone());
        }
        let name = self.ns.clone();
        self.snapshot_class = Some(name.clone());
        self.env.kube.apply(&json!({
            "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
            "metadata": {"name": name},
            "driver": "constellation.csi.replicix.com",
            "deletionPolicy": "Delete",
        }))?;
        Ok(name)
    }

    /// `VolumeSnapshot` `name` of PVC `pvc`, of the scope's snapshot class.
    pub fn volume_snapshot(&mut self, name: &str, pvc: &str) -> Result<()> {
        let class = self.snapshot_class()?;
        self.env.kube.apply(&json!({
            "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
            "metadata": {"name": name, "namespace": self.ns},
            "spec": {
                "volumeSnapshotClassName": class,
                "source": {"persistentVolumeClaimName": pvc},
            }
        }))
    }

    /// Wait for `VolumeSnapshot` `name` to be ready to use: its content's
    /// snapshot handle and its restore size.
    pub fn wait_snapshot_ready(&self, name: &str, deadline: Duration) -> Result<(String, u64)> {
        let mut found = (String::new(), 0);
        crate::scenarios::eventually(
            &format!("VolumeSnapshot {name} ready to use"),
            deadline,
            || {
                let vs = self.kube().get(&["volumesnapshot", "-n", &self.ns, name])?;
                let status = &vs["status"];
                if let Some(e) = status["error"]["message"].as_str() {
                    bail!("error: {e}");
                }
                if status["readyToUse"] != true {
                    bail!("not ready: {status}");
                }
                let content = status["boundVolumeSnapshotContentName"]
                    .as_str()
                    .context("no bound content")?;
                let c = self.kube().get(&["volumesnapshotcontent", content])?;
                let handle = c["status"]["snapshotHandle"]
                    .as_str()
                    .context("the content has no snapshot handle")?;
                let size = c["status"]["restoreSize"].as_u64().unwrap_or(0);
                found = (handle.to_string(), size);
                Ok(())
            },
        )?;
        Ok(found)
    }

    /// The `VolumeSnapshotContent` bound to `VolumeSnapshot` `name`.
    pub fn snapshot_content(&self, name: &str) -> Result<serde_json::Value> {
        let vs = self.kube().get(&["volumesnapshot", "-n", &self.ns, name])?;
        let content = vs["status"]["boundVolumeSnapshotContentName"]
            .as_str()
            .with_context(|| format!("VolumeSnapshot {name} has no bound content"))?;
        self.kube().get(&["volumesnapshotcontent", content])
    }

    pub fn kube(&self) -> &Kube {
        &self.env.kube
    }

    /// Create PVC `name` of this scope's class (`size` like `1Gi`).
    pub fn pvc(&mut self, name: &str, size: &str, rwx: bool) -> Result<()> {
        let class = self.sc.clone();
        self.pvc_of_class(name, size, rwx, &class)
    }

    /// Create PVC `name` of StorageClass `class`.
    pub fn pvc_of_class(&mut self, name: &str, size: &str, rwx: bool, class: &str) -> Result<()> {
        let mode = if rwx {
            "ReadWriteMany"
        } else {
            "ReadWriteOnce"
        };
        self.env.kube.apply(&json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": self.ns},
            "spec": {
                "accessModes": [mode],
                "storageClassName": class,
                "resources": {"requests": {"storage": size}},
            }
        }))?;
        self.pvcs.push(name.to_string());
        Ok(())
    }

    /// Create PVC `name` of StorageClass `class` (the scope's own when
    /// `None`) populated from `data_source` (a `dataSource` object: a
    /// `VolumeSnapshot` or a `PersistentVolumeClaim`).
    pub fn pvc_from(
        &mut self,
        name: &str,
        size: &str,
        class: Option<&str>,
        data_source: Value,
    ) -> Result<()> {
        self.env.kube.apply(&json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": self.ns},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": class.unwrap_or(&self.sc),
                "resources": {"requests": {"storage": size}},
                "dataSource": data_source,
            }
        }))?;
        self.pvcs.push(name.to_string());
        Ok(())
    }

    /// Wait until PVCs `claims` are Bound.
    pub fn wait_bound_claims(&self, claims: &[&str], deadline: Duration) -> Result<()> {
        crate::scenarios::eventually(
            &format!("PVCs {} Bound", claims.join(", ")),
            deadline,
            || {
                let mut pending = Vec::new();
                for c in claims {
                    let phase = self.kube().run(&[
                        "get",
                        "pvc",
                        "-n",
                        &self.ns,
                        c,
                        "-o",
                        "jsonpath={.status.phase}",
                    ])?;
                    if phase.trim() != "Bound" {
                        pending.push(*c);
                    }
                }
                if pending.is_empty() {
                    Ok(())
                } else {
                    bail!("not Bound yet: {}", pending.join(", "))
                }
            },
        )
    }

    /// The messages of the `Warning` events on PVC `pvc`.
    pub fn pvc_warnings(&self, pvc: &str) -> Result<Vec<String>> {
        let ev = self.kube().get(&[
            "events",
            "-n",
            &self.ns,
            "--field-selector",
            &format!(
                "involvedObject.kind=PersistentVolumeClaim,involvedObject.name={pvc},type=Warning"
            ),
        ])?;
        Ok(ev["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| e["message"].as_str().map(str::to_string))
            .collect())
    }

    /// PVC `pvc`'s phase.
    pub fn pvc_phase(&self, pvc: &str) -> Result<String> {
        Ok(self
            .kube()
            .run(&[
                "get",
                "pvc",
                "-n",
                &self.ns,
                pvc,
                "-o",
                "jsonpath={.status.phase}",
            ])?
            .trim()
            .to_string())
    }

    /// The controller plugin's log lines (both replicas, all of them).
    pub fn controller_log(&self) -> Result<String> {
        self.kube().run(&[
            "-n",
            &self.env.driver_ns,
            "logs",
            "-l",
            "app.kubernetes.io/component=controller",
            "-c",
            "constellation-csi",
            "--tail=-1",
        ])
    }

    /// Wait until every PVC of this scope is Bound.
    pub fn wait_bound(&self, deadline: Duration) -> Result<()> {
        crate::scenarios::eventually("every PVC Bound", deadline, || {
            let list = self.kube().get(&["pvc", "-n", &self.ns])?;
            let pending: Vec<&str> = list["items"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|p| p["status"]["phase"] != "Bound")
                .filter_map(|p| p["metadata"]["name"].as_str())
                .collect();
            if pending.is_empty() {
                Ok(())
            } else {
                bail!("not Bound yet: {}", pending.join(", "))
            }
        })
    }

    /// The CSI volume handle of a Bound PVC.
    pub fn volume_handle(&self, pvc: &str) -> Result<String> {
        let pv = self.kube().run(&[
            "get",
            "pvc",
            "-n",
            &self.ns,
            pvc,
            "-o",
            "jsonpath={.spec.volumeName}",
        ])?;
        let h = self.kube().run(&[
            "get",
            "pv",
            pv.trim(),
            "-o",
            "jsonpath={.spec.csi.volumeHandle}",
        ])?;
        Ok(h.trim().to_string())
    }

    /// Create pod `name` pinned to `node`, every claim mounted at
    /// [`data_dir`], running `script` (`sleep infinity` when `None`). Not
    /// root: the volume becomes writable through the driver's
    /// `fsGroupPolicy: File` (kubelet's recursive chgrp through FUSE).
    pub fn spawn_pod(
        &self,
        name: &str,
        node: &str,
        claims: &[&str],
        script: Option<&str>,
    ) -> Result<()> {
        self.spawn_pod_with(name, node, claims, script, false)
    }

    /// [`Scope::spawn_pod`], every claim mounted read-only when
    /// `read_only` (the pod volume's `persistentVolumeClaim.readOnly`).
    pub fn spawn_pod_with(
        &self,
        name: &str,
        node: &str,
        claims: &[&str],
        script: Option<&str>,
        read_only: bool,
    ) -> Result<()> {
        let mounts: Vec<Value> = claims
            .iter()
            .map(|c| json!({"name": c, "mountPath": data_dir(c), "readOnly": read_only}))
            .collect();
        let vols: Vec<Value> = claims
            .iter()
            .map(|c| {
                json!({"name": c, "persistentVolumeClaim": {"claimName": c, "readOnly": read_only}})
            })
            .collect();
        let command = match script {
            Some(s) => json!(["sh", "-c", s]),
            None => json!(["sleep", "infinity"]),
        };
        self.env.kube.apply(&json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": self.ns, "labels": {"app": "k8s-harness"}},
            "spec": {
                "nodeName": node,
                "restartPolicy": "Never",
                "terminationGracePeriodSeconds": 1,
                "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
                "containers": [{
                    "name": "c",
                    "image": self.env.image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": command,
                    "volumeMounts": mounts,
                }],
                "volumes": vols,
            }
        }))
    }

    pub fn wait_ready(&self, pod: &str, deadline: Duration) -> Result<()> {
        self.kube()
            .run(&[
                "wait",
                "-n",
                &self.ns,
                "--for=condition=Ready",
                &format!("pod/{pod}"),
                &format!("--timeout={}s", deadline.as_secs()),
            ])
            .map(|_| ())
            .with_context(|| format!("pod {pod} not Ready"))
    }

    /// [`Scope::spawn_pod`] and wait for it to be Ready.
    pub fn pod(&self, name: &str, node: &str, claims: &[&str]) -> Result<()> {
        self.spawn_pod(name, node, claims, None)?;
        self.wait_ready(name, Duration::from_secs(300))
    }

    pub fn exec(&self, pod: &str, script: &str) -> Result<String> {
        self.env.kube.exec(&self.ns, pod, script)
    }

    /// Install the pod tool ([`remote::POD_TOOL`]) in `pod`.
    pub fn install_pod_tool(&self, pod: &str) -> Result<()> {
        self.exec(pod, &remote::pod_tool_script())
            .map(|_| ())
            .with_context(|| format!("installing the pod tool in {pod}"))
    }

    pub fn delete_pods(&self, pods: &[&str]) -> Result<()> {
        let mut args = vec!["delete", "pod", "-n", &self.ns, "--wait=true"];
        args.extend_from_slice(pods);
        self.kube().run(&args).map(|_| ())
    }

    /// The tree under `dir` inside `pod`, as [`Observed`] entries keyed
    /// by their path relative to `dir`.
    pub fn listing(&self, pod: &str, dir: &str) -> Result<BTreeMap<PathBuf, Observed>> {
        let out = self.exec(pod, &remote::listing_script(dir))?;
        remote::parse_listing(&out)
    }

    /// Engine pods of the pool filesystem `fs_uuid` owned by `owner`
    /// (`node` or `controller`): `(name, node, uid, restarts, views)`.
    pub fn engine_pods(&self, fs_uuid: &str, owner: &str) -> Result<Vec<EnginePod>> {
        let list = self.kube().get(&[
            "pods",
            "-n",
            &self.env.driver_ns,
            "-l",
            &format!(
                "app.kubernetes.io/component=engine,constellation.replicix.com/owner={owner},constellation.replicix.com/fs-uuid={fs_uuid}"
            ),
        ])?;
        Ok(list["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| p["metadata"]["deletionTimestamp"].is_null())
            .map(EnginePod::from_json)
            .collect())
    }

    /// The engine's `snapshot.list` rows of filesystem `fs_uuid`, through
    /// its controller-owned pod (`constellation snapshot ls --json`): what
    /// the engine holds, not what Kubernetes says.
    pub fn engine_snapshots(&self, fs_uuid: &str) -> Result<Vec<Value>> {
        let pods = self.engine_pods(fs_uuid, "controller")?;
        let pod = pods
            .first()
            .with_context(|| format!("no controller-owned engine pod for {fs_uuid}"))?;
        let out = self.kube().run(&[
            "exec",
            "-n",
            &self.env.driver_ns,
            &pod.name,
            "-c",
            "engine",
            "--",
            "/usr/local/bin/constellation",
            "snapshot",
            "ls",
            "--state-dir",
            "/var/lib/constellation/state",
            "--json",
        ])?;
        serde_json::from_str::<Vec<Value>>(out.trim())
            .with_context(|| format!("parsing snapshot ls output {out:?}"))
    }

    /// Drop the scope's pools from the purge worker's records (the
    /// driver's `constellation-csi-pools` ConfigMap; keys are the
    /// controller-owned pods' names, `constellation-engine-<pool>[-shard-<k>]-controller`).
    fn forget_pool_records(&self, ns: &str) -> Result<()> {
        let cm = self.kube().run(&[
            "get",
            "configmap",
            "-n",
            ns,
            "constellation-csi-pools",
            "--ignore-not-found",
            "-o",
            "json",
        ])?;
        if cm.trim().is_empty() {
            return Ok(());
        }
        let cm: Value = serde_json::from_str(&cm)?;
        let mine: serde_json::Map<String, Value> = cm["data"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, _)| k)
            .filter(|k| {
                self.classes.iter().any(|(_, pool)| {
                    **k == format!("constellation-engine-{pool}-controller")
                        || k.starts_with(&format!("constellation-engine-{pool}-shard-"))
                })
            })
            .map(|k| (k.clone(), Value::Null))
            .collect();
        if mine.is_empty() {
            return Ok(());
        }
        let patch = json!({"data": mine}).to_string();
        self.kube()
            .run(&[
                "patch",
                "configmap",
                "-n",
                ns,
                "constellation-csi-pools",
                "--type=merge",
                "-p",
                &patch,
            ])
            .map(|_| ())
    }

    /// Have `finish` also check that, once the scope's engine pods are
    /// deleted, none comes back: a `DeleteVolume`/`DeleteSnapshot` the
    /// sidecars repeat for an object that is gone must start nothing
    /// (plan 37 K4). Waits [`NO_ENGINE_RETURN`] in `finish`.
    pub fn expect_no_engine_return(&mut self) {
        self.no_engine_return = true;
    }

    /// Whether `handle` is staged on `worker`: a FUSE mount at kubelet's
    /// staging path for it (`<plugins>/kubernetes.io/csi/<driver>/
    /// <sha256(volume handle)>/globalmount`) in the node's mount table.
    pub fn staged_on(&self, worker: &str, handle: &str) -> Result<bool> {
        let hash: String = crate::model::sha256_of(handle.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let mounts = docker(&["exec", worker, "cat", "/proc/self/mountinfo"])?;
        Ok(mounts
            .lines()
            .any(|l| l.contains(&format!("/{hash}/globalmount ")) && l.contains(" - fuse")))
    }

    /// The mount id (`/proc/self/mountinfo`'s first field) of `handle`'s
    /// staging mount on `worker`, if it is staged there: a remount is a new
    /// id.
    pub fn staging_mount_id(&self, worker: &str, handle: &str) -> Result<Option<String>> {
        let hash: String = crate::model::sha256_of(handle.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let mounts = docker(&["exec", worker, "cat", "/proc/self/mountinfo"])?;
        Ok(mounts
            .lines()
            .find(|l| l.contains(&format!("/{hash}/globalmount ")) && l.contains(" - fuse"))
            .and_then(|l| l.split_whitespace().next())
            .map(str::to_string))
    }

    /// Delete the scope's pods, check every volume unstaged on every
    /// worker, delete its PVCs (and wait for the PVs to go), its
    /// StorageClass, and its pool's engine pods.
    pub fn finish(mut self) -> Result<()> {
        let r = self.teardown(true);
        // An interrupt cut the teardown short: `Drop` finishes it.
        self.done = !(r.is_err() && interrupt::interrupted());
        r
    }

    fn teardown(&mut self, check: bool) -> Result<()> {
        let kube = self.env.kube.clone();
        // Each phase's time, printed when slow: a teardown that hangs on
        // one step (a finalizer, a sidecar's backoff) shows which.
        let mut phase = Instant::now();
        let mut lap = |what: &str| {
            let took = phase.elapsed();
            if took > Duration::from_secs(15) {
                eprintln!("   teardown: {what} took {took:.1?}");
            }
            phase = Instant::now();
        };
        let mut handles = Vec::new();
        for p in &self.pvcs {
            if let Ok(h) = self.volume_handle(p) {
                if !h.is_empty() {
                    handles.push(h);
                }
            }
        }
        let _ = kube.run(&[
            "delete",
            "pods",
            "--all",
            "-n",
            &self.ns,
            "--wait=true",
            "--timeout=180s",
        ]);
        let mut result = Ok(());
        if check {
            result = crate::scenarios::eventually(
                "every volume unstaged after its pods were deleted",
                Duration::from_secs(180),
                || {
                    for w in &self.env.workers {
                        for h in &handles {
                            if self.staged_on(w, h)? {
                                bail!("{h} is still staged on {w}");
                            }
                        }
                    }
                    Ok(())
                },
            );
        }
        lap("deleting the pods and the unstage check");
        let _ = kube.run(&[
            "delete",
            "namespace",
            &self.ns,
            "--wait=true",
            "--timeout=300s",
        ]);
        lap("deleting the namespace");
        let classes: Vec<&str> = self.classes.iter().map(|(c, _)| c.as_str()).collect();
        let gone = crate::scenarios::eventually(
            "the scope's PVs and snapshot contents deleted",
            Duration::from_secs(300),
            || {
                let pvs = kube.get(&["pv"])?;
                let left = pvs["items"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|p| {
                        p["spec"]["storageClassName"]
                            .as_str()
                            .is_some_and(|c| classes.contains(&c))
                    })
                    .count();
                if left > 0 {
                    bail!("{left} PV(s) of {} left", classes.join(", "));
                }
                if let Some(vsc) = &self.snapshot_class {
                    let contents = kube.get(&["volumesnapshotcontent"])?;
                    let left = contents["items"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|c| c["spec"]["volumeSnapshotClassName"] == vsc.as_str())
                        .count();
                    if left > 0 {
                        bail!("{left} VolumeSnapshotContent(s) of {vsc} left");
                    }
                }
                Ok(())
            },
        );
        if result.is_ok() && check {
            result = gone;
        }
        // Static PVs are `Retain`: Released once their claim went, and
        // removed here (`DeleteVolume` is never called for one).
        for pv in &self.static_pvs {
            let _ = kube.run(&[
                "delete",
                "pv",
                pv,
                "--ignore-not-found",
                "--wait=true",
                "--timeout=120s",
            ]);
        }
        lap("waiting for the PVs and snapshot contents to go");
        for c in &classes {
            let _ = kube.run(&["delete", "storageclass", c, "--ignore-not-found"]);
        }
        if let Some(vsc) = &self.snapshot_class {
            let _ = kube.run(&["delete", "volumesnapshotclass", vsc, "--ignore-not-found"]);
        }
        // Every engine pod of the scope's pools, node- and controller-owned
        // (the latter carries no `fs-uuid` label). Deleted once: the
        // controller starts no engine pod for a delete the provisioner
        // repeats after the PV is gone (plan 37 K4), so none comes back —
        // once the purge worker's records of the pools are gone too (it
        // brings back the pod of a recorded pool, plan 37 K6b).
        let ns = self.env.driver_ns.as_str();
        if let Err(e) = self.forget_pool_records(ns) {
            eprintln!("   forgetting the scope's pool records: {e:#}");
        }
        for (_, pool) in &self.classes {
            let sel = format!("constellation.replicix.com/pool={pool}");
            let _ = kube.run(&[
                "delete",
                "pods",
                "-n",
                ns,
                "-l",
                &sel,
                "--grace-period=0",
                "--force",
                "--ignore-not-found",
                "--wait=true",
                "--timeout=120s",
            ]);
        }
        lap("deleting the classes and the engine pods");
        if check && self.no_engine_return {
            let seen = (|| -> Result<()> {
                let deadline = Instant::now() + NO_ENGINE_RETURN;
                while Instant::now() < deadline {
                    for (_, pool) in &self.classes {
                        let sel = format!("constellation.replicix.com/pool={pool}");
                        let pods = kube.get(&["pods", "-n", ns, "-l", &sel])?;
                        let names: Vec<&str> = pods["items"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|p| p["metadata"]["name"].as_str())
                            .collect();
                        if !names.is_empty() {
                            bail!("engine pod(s) {names:?} of pool {pool} came back after the teardown");
                        }
                    }
                    std::thread::sleep(Duration::from_secs(5));
                }
                Ok(())
            })();
            if result.is_ok() {
                result = seen;
            }
            lap("checking no engine pod came back");
        }
        result
    }
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if !interrupt::interrupted() {
            self.env.diagnostics(Some(&self.ns));
        } else if self.env.disposable {
            // The cluster's deletion takes all of it, and fast: a CI
            // cancellation's second signal comes seconds after the first.
            return;
        }
        let _t = interrupt::Teardown::begin();
        let _ = self.teardown(false);
    }
}

/// A node- or controller-owned engine pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnginePod {
    pub name: String,
    pub node: String,
    pub uid: String,
    pub restarts: u64,
    /// `constellation.replicix.com/last-view-count`: the views it served when the
    /// node plugin last staged or unstaged through it.
    pub views: Option<u64>,
    /// Its engine container's image.
    pub image: String,
    /// Its `Ready` condition.
    pub ready: bool,
}

impl EnginePod {
    fn from_json(p: &Value) -> Self {
        Self {
            name: p["metadata"]["name"].as_str().unwrap_or_default().into(),
            node: p["spec"]["nodeName"].as_str().unwrap_or_default().into(),
            uid: p["metadata"]["uid"].as_str().unwrap_or_default().into(),
            restarts: p["status"]["containerStatuses"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| c["restartCount"].as_u64().unwrap_or(0))
                .sum(),
            views: p["metadata"]["annotations"]["constellation.replicix.com/last-view-count"]
                .as_str()
                .and_then(|v| v.parse().ok()),
            image: p["spec"]["containers"][0]["image"]
                .as_str()
                .unwrap_or_default()
                .into(),
            ready: p["status"]["conditions"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|c| c["type"] == "Ready" && c["status"] == "True"),
        }
    }
}

/// The driver's `constellation.replicix.com/pool` label of the pool at `endpoint`,
/// `bucket`, `prefix` (a copy of `constellation_csi::engine_pods::
/// pool_label`, which pins it as a versioned name; the test below pins
/// this copy to a value that function gives).
fn pool_label(endpoint: &str, bucket: &str, prefix: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [endpoint, bucket, prefix] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    let hash = hasher.finalize().to_hex();
    let base = prefix.rsplit('/').find(|c| !c.is_empty()).unwrap_or(bucket);
    let slug: String = base
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(16)
        .collect();
    let slug = slug.trim_matches('-');
    let slug = if slug.is_empty() { "pool" } else { slug };
    format!("{slug}-{}", &hash[..10])
}

/// The pool filesystem of a pool volume handle,
/// `v1/pool/<shard>/<fs-uuid>/volumes/<pv>`.
pub fn fs_uuid_of(handle: &str) -> Result<String> {
    let parts: Vec<&str> = handle.split('/').collect();
    match parts.as_slice() {
        ["v1", "pool", _, uuid, "volumes", _] => Ok(uuid.to_string()),
        _ => bail!("not a pool volume handle: {handle:?}"),
    }
}

/// The nearest-rank `p` quantile of `values` (non-empty): at 20 runs the
/// p99 is the slowest one.
fn percentile(values: &[f64], p: f64) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let rank = ((p * v.len() as f64).ceil() as usize).clamp(1, v.len());
    v[rank - 1]
}

/// `harness k8s-scenario`.
pub fn run(opts: Opts) -> Result<()> {
    let selected: Vec<&K8sScenario> = if opts.parity {
        if opts.all || !opts.names.is_empty() {
            bail!("--parity runs the whole lane: no --all, no scenario names");
        }
        // One entry per scenario: `tests/parity.py` refuses a name twice.
        if opts.repeat > 1 {
            bail!("--parity runs each scenario once: no --repeat");
        }
        parity::PORTED.iter().collect()
    } else if opts.all {
        if !opts.names.is_empty() {
            bail!("--all and scenario names are exclusive");
        }
        K8S_SCENARIOS.iter().collect()
    } else if opts.names.is_empty() {
        bail!("name a scenario (see `harness k8s-scenario --list`) or pass --all");
    } else {
        let mut v = Vec::new();
        for n in &opts.names {
            match K8S_SCENARIOS.iter().find(|s| s.name == n) {
                Some(s) => v.push(s),
                None => bail!("unknown k8s scenario {n:?} (try `harness k8s-scenario --list`)"),
            }
        }
        v
    };
    let default_lane = if opts.parity {
        parity::LANE
    } else {
        DEFAULT_LANE
    };
    let lane = opts.lane.clone().unwrap_or_else(|| default_lane.into());
    let mut report = RunResults::new(&lane, opts.seed, None);
    // The parity lane reports every `harness run` scenario: the ones it
    // does not run, up front, with the reason. They stay out of the run's
    // skipped list, which names what could not run here; these never run
    // in this lane.
    if opts.parity {
        let lane_skips = parity::skipped();
        let unported = parity::SKIPPED
            .iter()
            .filter(|(_, skip)| matches!(skip, parity::Skip::Unported(_)))
            .count();
        eprintln!(
            "=== lane {lane}: {} ported scenario(s) run, {} reported skipped ({} needing {}, \
             {unported} not yet ported)",
            selected.len(),
            lane_skips.len(),
            lane_skips.len() - unported,
            parity::LOCAL_CLIENT
        );
        for (name, why) in lane_skips {
            report.push(name, Outcome::Skipped, 0.0, Some(why));
        }
    }
    let finish = |report: RunResults, failures: Vec<String>, skipped: Vec<String>| -> Result<()> {
        if let Some(p) = &opts.results_json {
            report.write(p)?;
            eprintln!("results written to {}", p.display());
        }
        if !failures.is_empty() {
            bail!(
                "{} k8s scenario(s) failed: {}",
                failures.len(),
                failures.join(", ")
            );
        }
        if skipped.is_empty() {
            eprintln!("ALL K8S SCENARIOS PASSED");
        } else {
            eprintln!(
                "ALL RUN K8S SCENARIOS PASSED ({} skipped: {})",
                skipped.len(),
                skipped.join(", ")
            );
        }
        Ok(())
    };

    let kind = kind_bin();
    if let Some(missing) = ["kubectl", "helm", "docker", kind.as_str()]
        .into_iter()
        .find(|b| !crate::suites::have(b))
    {
        let why = format!("{missing} not installed");
        let mut skipped = Vec::new();
        for s in &selected {
            eprintln!("=== {} SKIPPED ({why})", s.name);
            report.push(s.name, Outcome::Skipped, 0.0, Some(why.clone()));
            skipped.push(s.name.to_string());
        }
        return finish(report, Vec::new(), skipped);
    }

    // Its floci carries the docker prefix label, so without the prefix
    // lock any `harness run` of the default prefix sweeps it mid-run.
    let _prefix_lock = crate::s3env::hold_prefix()?;
    interrupt::install().context("installing the SIGINT/SIGTERM handler")?;
    let setup = repo_root()
        .and_then(|root| {
            eprintln!("=== checkout {}", root.display());
            Cluster::acquire(&opts, &root).map(|c| (c, root))
        })
        .and_then(|(cluster, root)| Env::up(&cluster, &opts, &root).map(|env| (cluster, env)));
    let (cluster, env) = match setup {
        Ok(ce) => ce,
        Err(e) => {
            // Every selected scenario failed with the setup, so the
            // results file still says what happened.
            let why = format!("setup failed: {e:#}");
            for s in &selected {
                report.push(s.name, Outcome::Failed, 0.0, Some(why.clone()));
            }
            let names = selected.iter().map(|s| s.name.to_string()).collect();
            if let Err(fe) = finish(report, names, Vec::new()) {
                eprintln!("{fe:#}");
            }
            return Err(e.context("k8s-scenario setup"));
        }
    };
    let mut failures: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    // Every round's gated measurement, per scenario.
    let mut gated: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for round in 1..=opts.repeat {
        if opts.repeat > 1 {
            env.round.store(round, Ordering::Relaxed);
            eprintln!("=== round {round}/{}", opts.repeat);
        }
        for s in &selected {
            let label = if opts.repeat > 1 {
                format!("{} (round {round}/{})", s.name, opts.repeat)
            } else {
                s.name.to_string()
            };
            if interrupt::interrupted() {
                eprintln!("=== {label} SKIPPED (interrupted)");
                report.push(s.name, Outcome::Skipped, 0.0, Some("interrupted".into()));
                skipped.push(label);
                continue;
            }
            if env.workers.len() < s.workers {
                let why = format!(
                    "needs {} workers, the cluster has {}",
                    s.workers,
                    env.workers.len()
                );
                eprintln!("=== {label} SKIPPED ({why})");
                report.push(s.name, Outcome::Skipped, 0.0, Some(why));
                skipped.push(label);
                continue;
            }
            let t0 = Instant::now();
            eprintln!("=== {label} (seed {}) ===", opts.seed);
            let result = (s.run)(&env, opts.seed);
            let mut measured = env.take_measurements();
            if opts.repeat > 1 {
                let m = measured.get_or_insert_with(|| json!({}));
                m["round"] = json!(round);
            }
            if let Some(v) = s
                .gate
                .as_ref()
                .and_then(|g| measured.as_ref()?[g.metric].as_f64())
            {
                gated.entry(s.name).or_default().push(v);
            }
            let secs = t0.elapsed().as_secs_f64();
            match result {
                Ok(()) => {
                    eprintln!("=== {label} PASSED in {:.1?}", t0.elapsed());
                    report.push_measured(s.name, Outcome::Passed, secs, None, measured);
                }
                Err(e) => {
                    eprintln!("=== {label} FAILED in {:.1?}: {e:#}", t0.elapsed());
                    report.push_measured(
                        s.name,
                        Outcome::Failed,
                        secs,
                        Some(format!("{e:#}")),
                        measured,
                    );
                    failures.push(label);
                }
            }
        }
    }
    for s in &selected {
        let (Some(gate), Some(values)) = (&s.gate, gated.get(s.name)) else {
            continue;
        };
        let (p50, p99, max) = (
            percentile(values, 0.50),
            percentile(values, 0.99),
            percentile(values, 1.0),
        );
        eprintln!(
            "=== {}: {} over {} run(s): p50 {p50:.0} ms, p99 {p99:.0} ms, max {max:.0} ms \
             (gate: p99 under {} ms)",
            s.name,
            gate.metric,
            values.len(),
            gate.p99_under_ms
        );
        if p99 >= gate.p99_under_ms as f64 {
            failures.push(format!(
                "{} (p99 {} {p99:.0} ms, not under {} ms)",
                s.name, gate.metric, gate.p99_under_ms
            ));
        }
    }
    drop(env);
    drop(cluster);
    if interrupt::interrupted() {
        if let Err(e) = finish(report, failures, skipped) {
            eprintln!("{e:#}");
        }
        bail!("interrupted; what this run created is torn down");
    }
    finish(report, failures, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checkout_is_the_binarys_then_the_working_directorys() {
        let tmp = tempfile::tempdir().unwrap();
        let checkout = |name: &str| {
            let root = tmp.path().join(name);
            std::fs::create_dir_all(root.join("deploy/helm/constellation-csi")).unwrap();
            std::fs::create_dir_all(root.join("tests/csi")).unwrap();
            std::fs::create_dir_all(root.join("target/release")).unwrap();
            root
        };
        let (built, other) = (checkout("built"), checkout("other"));
        let elsewhere = tmp.path().join("bin");
        std::fs::create_dir_all(&elsewhere).unwrap();
        let exe = built.join("target/release/harness");
        // The binary's own checkout wins over the working directory's.
        assert_eq!(
            find_repo_root(Some(&exe), Some(&other.join("tests"))),
            Some(built.clone())
        );
        // A binary outside any checkout: the working directory, or the
        // checkout above it.
        let copied = elsewhere.join("harness");
        assert_eq!(
            find_repo_root(Some(&copied), Some(&other.join("tests/csi"))),
            Some(other.clone())
        );
        assert_eq!(find_repo_root(Some(&copied), Some(&elsewhere)), None);
        assert_eq!(find_repo_root(None, None), None);
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let v: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(percentile(&v, 0.5), 10.0);
        assert_eq!(percentile(&v, 0.99), 20.0);
        assert_eq!(percentile(&v, 1.0), 20.0);
        assert_eq!(percentile(&[7.0], 0.99), 7.0);
    }

    #[test]
    fn image_references_split_at_the_tag() {
        assert_eq!(
            split_image("constellation-csi:dev"),
            ("constellation-csi", "dev")
        );
        assert_eq!(split_image("reg:5000/csi"), ("reg:5000/csi", "latest"));
        assert_eq!(split_image("reg:5000/csi:k5"), ("reg:5000/csi", "k5"));
    }

    #[test]
    fn pool_handles_name_their_filesystem() {
        assert_eq!(
            fs_uuid_of("v1/pool/0/03c1ea2a-3b6c-4bec-a302-b3b3cddab581/volumes/pvc-1").unwrap(),
            "03c1ea2a-3b6c-4bec-a302-b3b3cddab581"
        );
        assert!(fs_uuid_of("v1/dedicated/x").is_err());
    }

    #[test]
    fn pool_labels_match_the_drivers() {
        // From `constellation_csi::engine_pods::pool_label` for a class
        // with these parameters (2026-10-02).
        assert_eq!(
            pool_label(
                "http://172.18.0.9:4566",
                "k8s-harness",
                "k8s-harness/1790972453-295061/csi-plugin-restart-survives"
            ),
            "csi-plugin-resta-fd0ea13369"
        );
    }
}
