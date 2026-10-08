//! `constellation serve` + `control-relay` end to end (plan 37 K2's engine
//! pod, minus Kubernetes): a headless daemon on the local file backend,
//! reached the way the CSI controller reaches a controller-owned engine
//! pod — the control protocol over a relay process's stdin/stdout — and
//! the subtree quota `quota.set{subtree}` the controller's `CreateVolume`
//! sends.

use constellation_control::methods::{
    BrowseMkdir, FsCreate, FsList, FsUnlock, NodePing, QuotaGet, QuotaSet,
};
use constellation_control::proto::types::{
    FsCreateParams, FsUnlockParams, MkdirParams, QuotaGetParams, SetQuotaParams, UnlockCredentials,
};
use constellation_control::proto::{ErrorKind, Secret};
use constellation_control::transport::StreamTransport;
use constellation_control::{Client, ClientOptions, Principal};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// nextest remaps binary paths when a test runs from an extracted archive
/// (`--archive-file`/`--extract-to`): it exposes the remapped path as
/// `NEXTEST_BIN_EXE_constellation` and also rewrites `CARGO_BIN_EXE_constellation`
/// in the environment, so read those at runtime instead of trusting the
/// path `env!` baked in at compile time.
fn bin() -> &'static str {
    static BIN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        std::env::var("NEXTEST_BIN_EXE_constellation")
            .or_else(|_| std::env::var("CARGO_BIN_EXE_constellation"))
            .unwrap_or_else(|_| env!("CARGO_BIN_EXE_constellation").to_string())
    })
}

fn ping(socket: &Path) -> bool {
    Command::new(bin())
        .args(["control-relay", "--ping", "--socket"])
        .arg(socket)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Kills the daemon if the test fails before stopping it.
struct Daemon(std::process::Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn serve_answers_the_control_protocol_through_a_relay() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = dir.path().join("s3");
    let socket = dir.path().join("sockets/pool/control.sock");
    let child = Command::new(bin())
        .arg("serve")
        .arg("--s3")
        .arg(&s3)
        .arg("--state-dir")
        .arg(dir.path().join("state"))
        .arg("--control-socket")
        .arg(&socket)
        .args(["--create", "--chunk-size", "1048576"])
        .env("XDG_RUNTIME_DIR", dir.path().join("run"))
        .env("XDG_CONFIG_HOME", dir.path().join("config"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut daemon = Daemon(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ping(&socket) {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "serve exited before answering node.ping"
        );
        assert!(Instant::now() < deadline, "serve never answered node.ping");
        std::thread::sleep(Duration::from_millis(100));
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut relay = tokio::process::Command::new(bin())
            .args(["control-relay", "--socket"])
            .arg(&socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stream = tokio::io::join(relay.stdout.take().unwrap(), relay.stdin.take().unwrap());
        let transport = Arc::new(StreamTransport::new(stream, Principal::InProcess));
        let client = Client::from_transport(transport, ClientOptions::default())
            .await
            .unwrap();

        // `--create` made the filesystem; `fs.create` with the same
        // parameters answers its uuid, other parameters a conflict.
        let bucket = s3.display().to_string();
        let created = client
            .call::<FsCreate>(FsCreateParams {
                bucket: bucket.clone(),
                chunk_size: Some(1 << 20),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!created.created);
        assert!(!created.uuid.is_empty());
        // With an empty registry, `fs.list` names exactly the served
        // filesystem (how the CSI controller labels an engine pod).
        let listing = client.call::<FsList>(Default::default()).await.unwrap();
        let own: Vec<_> = listing
            .filesystems
            .iter()
            .filter(|f| f.name.is_none())
            .collect();
        assert_eq!(own.len(), 1, "{listing:?}");
        assert_eq!(own[0].uuid, created.uuid);
        let conflict = client
            .call::<FsCreate>(FsCreateParams {
                bucket,
                chunk_size: Some(4 << 20),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(conflict.kind, ErrorKind::Conflict, "{conflict:?}");

        client
            .call::<BrowseMkdir>(MkdirParams {
                path: "/volumes/pv-a".into(),
                mode: None,
                parents: true,
            })
            .await
            .unwrap();
        let set = client
            .call::<QuotaSet>(SetQuotaParams {
                max_bytes: Some(1 << 30),
                subtree: Some("/volumes/pv-a".into()),
            })
            .await
            .unwrap();
        assert_eq!((set.max_bytes, set.used_bytes), (Some(1 << 30), 0));
        let got = client
            .call::<QuotaGet>(QuotaGetParams {
                subtree: Some("/volumes/pv-a".into()),
                cap_only: false,
            })
            .await
            .unwrap();
        assert_eq!(got.max_bytes, Some(1 << 30));
        // The subtree cap is not the filesystem's.
        let whole = client.call::<QuotaGet>(Default::default()).await.unwrap();
        assert_eq!(whole.max_bytes, None);
        let missing = client
            .call::<QuotaGet>(QuotaGetParams {
                subtree: Some("/volumes/nope".into()),
                cap_only: true,
            })
            .await
            .unwrap_err();
        assert_eq!(missing.kind, ErrorKind::NotFound, "{missing:?}");
        let missing = client
            .call::<QuotaSet>(SetQuotaParams {
                max_bytes: Some(0),
                subtree: Some("/volumes/nope".into()),
            })
            .await
            .unwrap_err();
        assert_eq!(missing.kind, ErrorKind::NotFound, "{missing:?}");
    });

    // SIGTERM ends a headless node cleanly (no view to unmount first).
    unsafe {
        libc::kill(daemon.0.id() as i32, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "serve ignored SIGTERM");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success(), "{status:?}");
}

/// Plan 37 K6a: `serve --await-unlock` takes its credentials (and an E2E
/// passphrase) from `fs.unlock` on the control socket only — nothing in
/// its environment — answers nothing else until then, starts once they
/// open the filesystem, and takes a rotation later without a restart;
/// both unlocks are in the audit log, with the caller's attribution and
/// no digest of the secrets.
#[test]
fn serve_await_unlock_takes_its_credentials_from_the_control_socket() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = dir.path().join("s3");
    let url = s3.display().to_string();
    let socket = dir.path().join("sockets/pool/control.sock");
    let state = dir.path().join("state");
    let child = Command::new(bin())
        .arg("serve")
        .arg("--s3")
        .arg(&s3)
        .arg("--state-dir")
        .arg(&state)
        .arg("--control-socket")
        .arg(&socket)
        .args([
            "--create",
            "--e2e",
            "--await-unlock",
            "--chunk-size",
            "1048576",
        ])
        .env("XDG_RUNTIME_DIR", dir.path().join("run"))
        .env("XDG_CONFIG_HOME", dir.path().join("config"))
        .env_remove("CONSTELLATION_PASSPHRASE")
        .env_remove("AWS_ACCESS_KEY_ID")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut daemon = Daemon(child);
    // The readiness probe passes on the gate.
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ping(&socket) {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "serve exited before answering node.ping"
        );
        assert!(Instant::now() < deadline, "serve never answered node.ping");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(!s3.join("meta.json").exists(), "created before fs.unlock");
    let unlock = |fs: &str, key: &str, passphrase: Option<&str>| FsUnlockParams {
        fs: fs.to_string(),
        credentials: UnlockCredentials {
            access_key_id: Some(Secret::new(key)),
            secret_access_key: Some(Secret::new(format!("{key}-secret"))),
            session_token: None,
            e2e_passphrase: passphrase.map(Secret::new),
        },
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let gate = Client::connect_unix(&socket).await.unwrap();
        gate.call::<NodePing>(Default::default()).await.unwrap();
        let waiting = gate.call::<FsList>(Default::default()).await.unwrap_err();
        assert_eq!(waiting.kind, ErrorKind::Unavailable, "{waiting:?}");
        assert!(waiting.message.contains("fs.unlock"), "{waiting:?}");
        let wrong = gate
            .call::<FsUnlock>(unlock("s3://elsewhere/x", "K1", Some("pw")))
            .await
            .unwrap_err();
        assert_eq!(wrong.kind, ErrorKind::Invalid, "{wrong:?}");
        // An E2E filesystem cannot be made without its passphrase: refused,
        // and the gate keeps waiting.
        let refused = gate
            .call::<FsUnlock>(unlock(&url, "K1", None))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, ErrorKind::Unavailable, "{refused:?}");
        assert!(refused.message.contains("passphrase"), "{refused:?}");
        let started = constellation_control::client::on_behalf_of(
            "pvc-k6a",
            gate.call::<FsUnlock>(unlock(&url, "K1", Some("correct horse"))),
        )
        .await
        .unwrap();
        assert!(
            started.detail.contains("credentials accepted"),
            "{started:?}"
        );
        assert!(s3.join("meta.json").exists());

        // The daemon proper, on a new connection, on the same socket.
        let client = Client::connect_unix(&socket).await.unwrap();
        let own = |l: &constellation_control::proto::types::FsListing| {
            l.filesystems
                .iter()
                .find(|f| f.name.is_none())
                .cloned()
                .unwrap()
        };
        let listing = client.call::<FsList>(Default::default()).await.unwrap();
        let fs = own(&listing);
        assert!(fs.e2e, "{fs:?}");
        assert_eq!(fs.credentials_generation, 1, "{fs:?}");
        // A rotation: the running engine's source, in place.
        let rotated = constellation_control::client::on_behalf_of(
            "pvc-k6a",
            client.call::<FsUnlock>(unlock(&url, "K2", None)),
        )
        .await
        .unwrap();
        assert!(rotated.detail.contains("rotated"), "{rotated:?}");
        let fs = own(&client.call::<FsList>(Default::default()).await.unwrap());
        assert_eq!(fs.credentials_generation, 2, "{fs:?}");
        // The gate's connection is closed shortly after the start.
        let deadline = Instant::now() + Duration::from_secs(30);
        while gate.is_connected() {
            assert!(
                Instant::now() < deadline,
                "the gate's connection stayed open"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let audit = std::fs::read_to_string(state.join("control-audit.jsonl")).unwrap();
    let unlocks: Vec<serde_json::Value> = audit
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .filter(|r| r["method"] == "fs.unlock")
        .collect();
    // Two refused at the gate, the one that started the engine, the rotation.
    assert_eq!(unlocks.len(), 4, "{audit}");
    for r in &unlocks {
        assert_eq!(r["params_digest"], "withheld:secret-params", "{r}");
    }
    let attributed: Vec<_> = unlocks
        .iter()
        .filter(|r| r["on_behalf_of"] == "pvc-k6a")
        .map(|r| r["outcome"].clone())
        .collect();
    assert_eq!(
        attributed,
        [serde_json::json!("ok"), serde_json::json!("ok")],
        "{audit}"
    );
    for secret in ["K1-secret", "K2-secret", "correct horse"] {
        assert!(!audit.contains(secret), "the audit log holds a secret");
    }

    unsafe {
        libc::kill(daemon.0.id() as i32, libc::SIGTERM);
    }
    let status = exit_status(&mut daemon, "serve --await-unlock");
    assert!(status.success(), "{status:?}");
}

/// `serve --await-unlock` on the local backend at `s3`, its state in
/// `dir`, with nothing in its environment that could unlock it.
fn awaiting_serve(dir: &Path, s3: &Path, socket: &Path, extra: &[&str]) -> Daemon {
    awaiting_serve_env(dir, s3, socket, extra, &[])
}

/// [`awaiting_serve`] with extra environment.
fn awaiting_serve_env(
    dir: &Path,
    s3: &Path,
    socket: &Path,
    extra: &[&str],
    envs: &[(&str, &Path)],
) -> Daemon {
    let child = Command::new(bin())
        .arg("serve")
        .arg("--s3")
        .arg(s3)
        .arg("--state-dir")
        .arg(dir.join("state"))
        .arg("--control-socket")
        .arg(socket)
        .args(["--await-unlock", "--chunk-size", "1048576"])
        .args(extra)
        .envs(envs.iter().copied())
        .env("XDG_RUNTIME_DIR", dir.join("run"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env_remove("CONSTELLATION_PASSPHRASE")
        .env_remove("AWS_ACCESS_KEY_ID")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut daemon = Daemon(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ping(socket) {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "serve exited before answering node.ping"
        );
        assert!(Instant::now() < deadline, "serve never answered node.ping");
        std::thread::sleep(Duration::from_millis(100));
    }
    daemon
}

/// `fs.unlock` of `fs` with a fixture key pair and `passphrase`.
fn unlock_with(fs: &str, passphrase: Option<&str>) -> FsUnlockParams {
    FsUnlockParams {
        fs: fs.to_string(),
        credentials: UnlockCredentials {
            access_key_id: Some(Secret::new("K1")),
            secret_access_key: Some(Secret::new("K1-secret")),
            session_token: None,
            e2e_passphrase: passphrase.map(Secret::new),
        },
    }
}

/// Plan 37 K6a review: an engine waiting for `fs.unlock` stops at once on
/// `SIGTERM` (and `SIGINT`), exiting 0 — as its container's PID 1 it would
/// otherwise ignore the signal and hold up a pod deletion for its whole
/// grace period — and its memory never goes into a core dump.
#[test]
fn serve_waiting_for_unlock_stops_on_a_signal_and_dumps_no_core() {
    for sig in [libc::SIGTERM, libc::SIGINT] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("sockets/pool/control.sock");
        let mut daemon = awaiting_serve(dir.path(), &dir.path().join("s3"), &socket, &["--create"]);
        let pid = daemon.0.id();
        let limits = std::fs::read_to_string(format!("/proc/{pid}/limits")).unwrap();
        let core = limits
            .lines()
            .find(|l| l.starts_with("Max core file size"))
            .unwrap();
        let fields: Vec<&str> = core.split_whitespace().collect();
        assert_eq!(&fields[4..6], ["0", "0"], "{core}");
        // Not dumpable: the kernel makes its /proc entries root's.
        if unsafe { libc::geteuid() } != 0 {
            use std::os::unix::fs::MetadataExt;
            let owner = std::fs::metadata(format!("/proc/{pid}/status"))
                .unwrap()
                .uid();
            assert_eq!(owner, 0, "serve --await-unlock is still dumpable");
        }
        let sent = Instant::now();
        signal(&daemon, sig);
        let status = exit_status(&mut daemon, "on a signal while waiting for fs.unlock");
        assert!(status.success(), "signal {sig}: {status:?}");
        assert!(
            sent.elapsed() < Duration::from_secs(10),
            "signal {sig}: took {:?} to stop",
            sent.elapsed()
        );
    }
}

/// Plan 37 K6a review: a wrong E2E passphrase is refused at the gate
/// (`Unavailable`, naming the passphrase) and the gate keeps waiting —
/// the engine never starts on it and crash-loops — and the right one
/// then starts it.
#[test]
fn serve_refuses_a_wrong_passphrase_at_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = dir.path().join("s3");
    let url = s3.display().to_string();
    let socket = dir.path().join("sockets/pool/control.sock");
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Made, then stopped.
    let mut daemon = awaiting_serve(dir.path(), &s3, &socket, &["--create", "--e2e"]);
    rt.block_on(async {
        let gate = Client::connect_unix(&socket).await.unwrap();
        gate.call::<FsUnlock>(unlock_with(&url, Some("correct horse")))
            .await
            .unwrap();
    });
    signal(&daemon, libc::SIGTERM);
    assert!(exit_status(&mut daemon, "after the first start").success());

    let mut daemon = awaiting_serve(dir.path(), &s3, &socket, &[]);
    rt.block_on(async {
        let gate = Client::connect_unix(&socket).await.unwrap();
        for (passphrase, why) in [
            (None, "e2e_passphrase"),
            (Some("battery staple"), "passphrase"),
        ] {
            let refused = gate
                .call::<FsUnlock>(unlock_with(&url, passphrase))
                .await
                .unwrap_err();
            assert_eq!(refused.kind, ErrorKind::Denied, "{refused:?}");
            assert!(refused.message.contains(why), "{refused:?}");
            assert!(!refused.message.contains("battery"), "{refused:?}");
        }
        let waiting = gate.call::<FsList>(Default::default()).await.unwrap_err();
        assert!(waiting.message.contains("fs.unlock"), "{waiting:?}");
        gate.call::<FsUnlock>(unlock_with(&url, Some("correct horse")))
            .await
            .unwrap();
    });
    assert!(daemon.0.try_wait().unwrap().is_none(), "serve exited");
    signal(&daemon, libc::SIGTERM);
    assert!(exit_status(&mut daemon, "after the second start").success());
}

/// A root-only test's `serve` daemon with one preopened view (plan 37's
/// `NodeStageVolume` shape): this process plays the CSI node plugin — it
/// mounts with `fuse_mount_fd` and sends the descriptor with
/// `view.mount{PreopenedFd}` naming its mountpoint. `None` (skipped
/// loudly) without root or `/dev/fuse`.
struct Preopened {
    daemon: Daemon,
    socket: std::path::PathBuf,
    staging: std::path::PathBuf,
    rt: tokio::runtime::Runtime,
    client: Client,
    _dir: tempfile::TempDir,
}

fn serve_with_a_preopened_view() -> Option<Preopened> {
    serve_with_a_preopened_view_and(&[], &[])
}

/// [`serve_with_a_preopened_view`] with extra `serve` arguments and
/// environment.
fn serve_with_a_preopened_view_and(args: &[&str], envs: &[(&str, &str)]) -> Option<Preopened> {
    serve_with_a_preopened_view_in(tempfile::tempdir().unwrap(), args, envs, None)
}

/// [`serve_with_a_preopened_view_and`] under a control allowlist written
/// to `policy`: the node plugin's grant ([`node_plugin_policy`]) on the
/// unit's control sockets.
fn serve_with_a_preopened_view_and_policy(
    args: &[&str],
    envs: &[(&str, &str)],
    policy: &Path,
) -> Option<Preopened> {
    serve_with_a_preopened_view_in(tempfile::tempdir().unwrap(), args, envs, Some(policy))
}

/// The node plugin's `kind = "service"` grant for `uid` (as
/// `constellation_csi::engine_pods::node_engine_policy` writes it), with
/// `label`, on the control socket `control{-name}.sock` of each of `names`
/// under `dir` (`""`: `control.sock`).
fn node_plugin_policy(dir: &Path, uid: u32, label: &str, names: &[&str]) -> String {
    let dir = std::fs::canonicalize(dir).unwrap();
    names
        .iter()
        .map(|name| {
            let file = match *name {
                "" => "control.sock".to_string(),
                name => format!("control-{name}.sock"),
            };
            format!(
                "[[grant]]\nkind = \"service\"\nprincipal = \"uid:{uid}\"\nsocket = {:?}\n\
                 role = \"admin\"\nlabel = {label:?}\n",
                dir.join("sockets/unit").join(file).display().to_string()
            )
        })
        .collect()
}

fn serve_with_a_preopened_view_in(
    dir: tempfile::TempDir,
    args: &[&str],
    envs: &[(&str, &str)],
    policy: Option<&Path>,
) -> Option<Preopened> {
    // SAFETY: no preconditions.
    if unsafe { libc::geteuid() } != 0 || !Path::new("/dev/fuse").exists() {
        eprintln!("skipping: needs root and /dev/fuse (mount(2))");
        return None;
    }
    use constellation_control::methods::ViewMount;
    use constellation_control::proto::types::{MountSource, MountViewOpts, ViewMountParams};

    let socket = dir.path().join("sockets/unit/control.sock");
    if let Some(policy) = policy {
        std::fs::write(
            policy,
            node_plugin_policy(dir.path(), 0, "csi-node-plugin", &["", "b", "c"]),
        )
        .unwrap();
    }
    let policy_env: Vec<(&str, &Path)> = policy
        .map(|p| ("CONSTELLATION_CONTROL_POLICY", p))
        .into_iter()
        .collect();
    let child = Command::new(bin())
        .arg("serve")
        .arg("--s3")
        .arg(dir.path().join("s3"))
        .arg("--state-dir")
        .arg(dir.path().join("state"))
        .arg("--control-socket")
        .arg(&socket)
        .arg("--create")
        .args(args)
        .envs(envs.iter().copied())
        .envs(policy_env)
        .env("XDG_RUNTIME_DIR", dir.path().join("run"))
        .env("XDG_CONFIG_HOME", dir.path().join("config"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut daemon = Daemon(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ping(&socket) {
        assert!(daemon.0.try_wait().unwrap().is_none(), "serve exited early");
        assert!(Instant::now() < deadline, "serve never answered node.ping");
        std::thread::sleep(Duration::from_millis(100));
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    if args.contains(&"--await-unlock") {
        // The plugin's first call to a pod that waits for its credentials.
        let url = dir.path().join("s3").display().to_string();
        rt.block_on(async {
            let gate = Client::connect_unix(&socket).await.unwrap();
            gate.call::<FsUnlock>(unlock_with(&url, None))
                .await
                .unwrap();
        });
    }

    let staging = dir.path().join("staging/pv-a/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    let mut kernel = constellation_platform::MountOpts::new("constellation-k3a-test");
    kernel.allow_other = true;
    let fd = constellation_platform::linux::fuse_mount_fd(&staging, &kernel).unwrap();

    let client = rt
        .block_on(Client::connect_unix(&socket))
        .expect("the daemon's unix socket");
    let view = rt
        .block_on(client.call_with_fd::<ViewMount>(
            ViewMountParams {
                subtree: "/".into(),
                source: MountSource::PreopenedFd {
                    mountpoint: Some(staging.clone()),
                    opts: MountViewOpts {
                        allow_other: true,
                        ..Default::default()
                    },
                },
                labels: [("pv".to_string(), "pv-a".to_string())].into(),
                qos: Default::default(),
                confine_links: true,
            },
            fd,
        ))
        .expect("view.mount with the preopened descriptor");
    assert_eq!(Path::new(&view.mountpoint), staging, "known by its name");
    std::fs::write(staging.join("f"), b"through the engine").unwrap();
    assert_eq!(
        std::fs::read(staging.join("f")).unwrap(),
        b"through the engine"
    );
    Some(Preopened {
        daemon,
        socket,
        staging,
        rt,
        client,
        _dir: dir,
    })
}

fn signal(daemon: &Daemon, sig: i32) {
    // SAFETY: signalling our own child.
    unsafe {
        libc::kill(daemon.0.id() as i32, sig);
    }
}

/// The daemon's exit status, within a minute.
fn exit_status(daemon: &mut Daemon, what: &str) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "serve did not exit {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The maker's unmount of a staging mount the daemon let go of.
fn umount(path: &Path) {
    let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: a valid NUL-terminated path.
    assert_eq!(
        unsafe { libc::umount2(c.as_ptr(), 0) },
        0,
        "the maker unmounts"
    );
}

/// Plan 37's `NodeUnstageVolume` shape against a real `serve` daemon
/// ([`serve_with_a_preopened_view`]): the plugin asks for `view.unmount`
/// by the name it mounted at. K0's gap 3: that call used to hang in the
/// daemon's `thread.join()` on a session nothing ended; it must answer,
/// end the session (the connection is closed, so the mount answers
/// `ENOTCONN`) and leave the unmount to the maker.
#[test]
fn a_preopened_view_is_unmounted_by_its_name_without_hanging() {
    use constellation_control::methods::{ViewList, ViewUnmount};
    use constellation_control::proto::types::ViewUnmountParams;
    let Some(Preopened {
        mut daemon,
        socket,
        staging,
        rt,
        client,
        _dir,
    }) = serve_with_a_preopened_view()
    else {
        return;
    };

    let listed = rt
        .block_on(client.call::<ViewList>(Default::default()))
        .unwrap();
    assert!(
        listed
            .views
            .iter()
            .any(|v| Path::new(&v.mountpoint) == staging),
        "{listed:?}"
    );
    // Plan 37 §7: SIGTERM while a view is served is not an exit; the node
    // goes once its last view does.
    signal(&daemon, libc::SIGTERM);
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        daemon.0.try_wait().unwrap().is_none(),
        "serve exited on SIGTERM with a view mounted"
    );
    assert!(ping(&socket), "serve stopped answering after SIGTERM");
    assert_eq!(
        std::fs::read(staging.join("f")).unwrap(),
        b"through the engine",
        "the view is still served"
    );

    rt.block_on(client.call_bounded::<ViewUnmount>(
        ViewUnmountParams {
            mountpoint: staging.clone(),
        },
        Duration::from_secs(30),
    ))
    .expect("view.unmount of a preopened view answers");
    // The daemon closed the connection and left the mount to its maker.
    let err = std::fs::metadata(staging.join("f")).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOTCONN), "{err}");
    umount(&staging);
    // The deferred SIGTERM: with its last view gone, the node exits, 0.
    let status = exit_status(&mut daemon, "after its last view");
    assert!(status.success(), "{status:?}");
}

/// Only the first `SIGTERM` is deferred: a second one drains the views now
/// (ends their sessions, flushes) and exits; `SIGINT` (an interactive
/// Ctrl-C) never waits for the views at all.
#[test]
fn sigint_and_a_second_sigterm_are_not_deferred() {
    for signals in [&[libc::SIGINT][..], &[libc::SIGTERM, libc::SIGTERM]] {
        let Some(Preopened {
            mut daemon,
            staging,
            _dir,
            ..
        }) = serve_with_a_preopened_view()
        else {
            return;
        };
        for (i, sig) in signals.iter().enumerate() {
            if i > 0 {
                std::thread::sleep(Duration::from_millis(1500));
                assert!(
                    daemon.0.try_wait().unwrap().is_none(),
                    "the first SIGTERM is deferred"
                );
            }
            signal(&daemon, *sig);
        }
        let status = exit_status(&mut daemon, &format!("on {signals:?} with a view mounted"));
        assert!(status.success(), "{signals:?}: {status:?}");
        // The session ended; the mount is still the maker's.
        let err = std::fs::metadata(staging.join("f")).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOTCONN), "{err}");
        umount(&staging);
    }
}

// ---- plan 37 §8: handing the sessions to a second `serve` ----

use constellation_control::methods::NodeHandoff;
use constellation_control::proto::types::{
    HandoffParams, HandoffPhase, HandoffReport, HandoffState, HandoffTarget,
};

fn phase(phase: HandoffPhase) -> HandoffParams {
    HandoffParams {
        target: HandoffTarget::Socket,
        phase: Some(phase),
        ..Default::default()
    }
}

/// A second `serve` on the same state dir (the replacement engine pod):
/// the state dir is held, so it waits as a handoff standby.
fn standby(dir: &Path, name: &str) -> (Daemon, std::path::PathBuf, std::path::PathBuf) {
    standby_with(dir, name, &[])
}

fn standby_with(
    dir: &Path,
    name: &str,
    args: &[&str],
) -> (Daemon, std::path::PathBuf, std::path::PathBuf) {
    standby_with_env(dir, name, args, &[])
}

/// [`standby_with`] with extra environment.
fn standby_with_env(
    dir: &Path,
    name: &str,
    args: &[&str],
    envs: &[(&str, &Path)],
) -> (Daemon, std::path::PathBuf, std::path::PathBuf) {
    let socket = dir.join(format!("sockets/unit/control-{name}.sock"));
    let handoff = dir.join(format!("sockets/unit/handoff-{name}.sock"));
    let child = Command::new(bin())
        .arg("serve")
        .arg("--s3")
        .arg(dir.join("s3"))
        .arg("--state-dir")
        .arg(dir.join("state"))
        .arg("--control-socket")
        .arg(&socket)
        .arg("--handoff-socket")
        .arg(&handoff)
        .args(args)
        .envs(envs.iter().copied())
        .env("XDG_RUNTIME_DIR", dir.join(format!("run-{name}")))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (Daemon(child), socket, handoff)
}

fn standby_client(rt: &tokio::runtime::Runtime, daemon: &mut Daemon, handoff: &Path) -> Client {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(daemon.0.try_wait().unwrap().is_none(), "the standby exited");
        if let Ok(client) = rt.block_on(Client::connect_unix(handoff)) {
            let state = rt
                .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Status)))
                .unwrap()
                .state;
            assert_eq!(state, Some(HandoffState::Standby { received: 0 }));
            return client;
        }
        assert!(Instant::now() < deadline, "the standby never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A writer on the mount for the whole test: appends blocks through a
/// descriptor opened before the handoff (the handle table must travel),
/// `fsync`ing on a cadence; counts every error and keeps the longest any
/// one call took (the pause a client sees).
struct Writer {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: std::thread::JoinHandle<Written>,
}

struct Written {
    blocks: u64,
    errors: Vec<String>,
    longest: Duration,
}

/// Block `n`'s bytes (its own fill, so a block written twice or out of
/// place shows).
fn block_bytes(n: u64, len: usize) -> Vec<u8> {
    vec![(n % 251) as u8; len]
}

impl Writer {
    /// 4 KiB blocks back to back, an `fsync` every 16.
    fn start(path: std::path::PathBuf) -> Writer {
        Writer::start_with(path, 4096, Duration::ZERO, Cadence::Blocks(16))
    }

    fn start_with(
        path: std::path::PathBuf,
        block: usize,
        pace: Duration,
        fsync: Cadence,
    ) -> Writer {
        use std::io::Write;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let mut file = std::fs::File::create(&path).unwrap();
        let thread = std::thread::spawn(move || {
            let mut w = Written {
                blocks: 0,
                errors: Vec::new(),
                longest: Duration::ZERO,
            };
            let mut synced = Instant::now();
            let timed = |w: &mut Written, f: &mut dyn FnMut() -> std::io::Result<()>| {
                let at = Instant::now();
                let r = f();
                w.longest = w.longest.max(at.elapsed());
                r
            };
            while !stopping.load(std::sync::atomic::Ordering::SeqCst) {
                let bytes = block_bytes(w.blocks, block);
                match timed(&mut w, &mut || file.write_all(&bytes)) {
                    Ok(()) => w.blocks += 1,
                    Err(e) => w.errors.push(format!("write {}: {e}", w.blocks)),
                }
                let due = match fsync {
                    Cadence::Blocks(n) => w.blocks.is_multiple_of(n),
                    Cadence::Every(t) => synced.elapsed() >= t,
                };
                if due {
                    synced = Instant::now();
                    if let Err(e) = timed(&mut w, &mut || file.sync_data()) {
                        w.errors.push(format!("fsync at {}: {e}", w.blocks));
                    }
                }
                if w.errors.len() > 10 {
                    break;
                }
                if !pace.is_zero() {
                    std::thread::sleep(pace);
                }
            }
            w
        });
        Writer { stop, thread }
    }

    fn finish(self) -> (u64, Vec<String>) {
        let w = self.finish_written();
        (w.blocks, w.errors)
    }

    fn finish_written(self) -> Written {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        self.thread.join().unwrap()
    }
}

#[derive(Clone, Copy)]
enum Cadence {
    Blocks(u64),
    Every(Duration),
}

/// Every block of `path` is the writer's, in place, `blocks` of them.
fn check_written(path: &Path, blocks: u64, block: usize) {
    let data = std::fs::read(path).unwrap();
    assert_eq!(
        data.len() as u64,
        blocks * block as u64,
        "{}",
        path.display()
    );
    for (i, got) in data.chunks(block).enumerate() {
        assert!(
            got == block_bytes(i as u64, block).as_slice(),
            "block {i} of {}",
            path.display()
        );
    }
}

/// §8 steps 1-4 against `old`, relayed to `new`: prepare, transfer onto a
/// socketpair, every record relayed with one `Receive` on another.
fn prepare_and_relay(rt: &tokio::runtime::Runtime, old: &Client, new: &Client) -> HandoffReport {
    let prepared = rt
        .block_on(old.call::<NodeHandoff>(HandoffParams {
            drain_timeout_ms: Some(5000),
            deadline_ms: Some(60_000),
            ..phase(HandoffPhase::Prepare)
        }))
        .expect("prepare");
    assert_eq!(prepared.state, Some(HandoffState::Prepared));
    assert!(
        prepared.views.iter().all(|v| v.transport == "dev_fuse"),
        "{prepared:?}"
    );
    let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let reader = std::thread::spawn(move || {
        let mut records = Vec::new();
        while let Some(r) = constellation_control::handoff_wire::read_record(&mut ours).unwrap() {
            records.push(r);
        }
        records
    });
    rt.block_on(old.call_with_fd::<NodeHandoff>(phase(HandoffPhase::Transfer), theirs.into()))
        .expect("transfer");
    let records = reader.join().unwrap();
    assert_eq!(records.len(), prepared.views.len());
    for (_, fd) in &records {
        // The handed-out connection is blocking (K0 gap 4).
        // SAFETY: F_GETFL on a live descriptor.
        let flags = unsafe { libc::fcntl(std::os::fd::AsRawFd::as_raw_fd(fd), libc::F_GETFL) };
        assert_eq!(flags & libc::O_NONBLOCK, 0);
    }
    let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let writer = std::thread::spawn(move || {
        for (record, fd) in &records {
            constellation_control::handoff_wire::write_record(
                &mut ours,
                record,
                std::os::fd::AsFd::as_fd(fd),
            )
            .unwrap();
        }
        constellation_control::handoff_wire::write_end(&mut ours).unwrap();
    });
    let received = rt
        .block_on(new.call_with_fd::<NodeHandoff>(phase(HandoffPhase::Receive), theirs.into()))
        .expect("receive");
    writer.join().unwrap();
    assert_eq!(
        received.state,
        Some(HandoffState::Standby {
            received: prepared.views.len() as u64
        })
    );
    prepared
}

/// Poll the standby's `Status` until it resumed every view.
fn wait_resumed(rt: &tokio::runtime::Runtime, new: &Client) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let state = rt
            .block_on(new.call::<NodeHandoff>(phase(HandoffPhase::Status)))
            .unwrap()
            .state;
        match state {
            Some(HandoffState::Resumed { failed }) => {
                assert!(failed.is_empty(), "{failed:?}");
                return;
            }
            Some(HandoffState::Failed { reason }) => panic!("the standby failed: {reason}"),
            _ => {}
        }
        assert!(Instant::now() < deadline, "the standby never resumed");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Plan 37 §8 end to end, minus Kubernetes: a writer keeps appending
/// through the staging mount while its session moves from one `serve` to
/// another on the same state dir — and sees no error at all; the first
/// exits 0 after its commit, the second serves the same mount (and the
/// writer's open descriptor) on.
#[test]
fn a_preopened_view_is_handed_to_a_second_serve_without_an_error() {
    use constellation_control::methods::{ViewList, ViewUnmount};
    use constellation_control::proto::types::ViewUnmountParams;
    let Some(Preopened {
        mut daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view()
    else {
        return;
    };
    let (mut next, next_socket, handoff) = standby(_dir.path(), "b");
    let new = standby_client(&rt, &mut next, &handoff);
    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(500));

    let started = Instant::now();
    prepare_and_relay(&rt, &client, &new);
    rt.block_on(new.call::<NodeHandoff>(HandoffParams {
        deadline_ms: Some(60_000),
        ..phase(HandoffPhase::Seal)
    }))
    .expect("seal");
    let committed = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Commit)))
        .expect("commit");
    assert_eq!(committed.state, Some(HandoffState::Committed));
    wait_resumed(&rt, &new);
    eprintln!("handoff: {:?}", started.elapsed());
    let status = exit_status(&mut daemon, "after its commit");
    assert!(status.success(), "{status:?}");

    std::thread::sleep(Duration::from_millis(1000));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    assert!(written > 0);
    check_written(&staging.join("w"), written, 4096);
    assert_eq!(
        std::fs::read(staging.join("f")).unwrap(),
        b"through the engine"
    );
    // The new daemon is the view's server, by the same name.
    assert!(ping(&next_socket));
    let client = rt.block_on(Client::connect_unix(&next_socket)).unwrap();
    let listed = rt
        .block_on(client.call::<ViewList>(Default::default()))
        .unwrap();
    assert!(listed
        .views
        .iter()
        .any(|v| Path::new(&v.mountpoint) == staging));
    rt.block_on(client.call_bounded::<ViewUnmount>(
        ViewUnmountParams {
            mountpoint: staging.clone(),
        },
        Duration::from_secs(30),
    ))
    .expect("view.unmount on the new daemon");
    umount(&staging);
}

/// 37-k6a with K5a: the standby of an engine that got its credentials
/// through `fs.unlock` waits for them too (`--await-unlock`: nothing in
/// its environment), gets them from the old engine itself in the
/// `Credentials` step — before any session stops, and before which it
/// refuses `Receive` — and serves the handed-over session on them with no
/// error for a writer. Its memory never goes into a core dump either. The
/// caller is the node plugin's `kind = "service"` grant (root here, as in
/// a node pod).
#[test]
fn an_unlocked_engines_standby_gets_its_credentials_over_the_handoff() {
    use constellation_control::methods::FsList;
    let policy_dir = tempfile::tempdir().unwrap();
    let policy = policy_dir.path().join("control-allow.toml");
    let policy_env = [("CONSTELLATION_CONTROL_POLICY", policy.as_path())];
    let Some(Preopened {
        mut daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view_and_policy(&["--await-unlock"], &[], &policy)
    else {
        return;
    };
    let (mut next, next_socket, handoff) =
        standby_with_env(_dir.path(), "b", &["--await-unlock"], &policy_env);
    let new = standby_client(&rt, &mut next, &handoff);
    assert_no_core(next.0.id());
    // No credentials yet: no `Receive`.
    let (_ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let early = rt
        .block_on(new.call_with_fd::<NodeHandoff>(phase(HandoffPhase::Receive), theirs.into()))
        .unwrap_err();
    assert!(early.message.contains("Credentials"), "{early:?}");

    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(300));
    // Step 0: old → this process → new, one opaque frame.
    let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let reader = std::thread::spawn(move || {
        constellation_control::handoff_wire::read_secret(&mut ours).unwrap()
    });
    rt.block_on(old_credentials(&client, theirs))
        .expect("the old engine's credentials step");
    let frame = reader
        .join()
        .unwrap()
        .expect("it holds its fs.unlock credentials");
    let text = String::from_utf8_lossy(&frame).to_string();
    assert!(
        text.contains("K1-secret"),
        "the very pair fs.unlock gave it"
    );
    let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let relay = std::thread::spawn(move || {
        constellation_control::handoff_wire::write_secret(&mut ours, Some(&frame)).unwrap()
    });
    let got = rt
        .block_on(new.call_with_fd::<NodeHandoff>(phase(HandoffPhase::Credentials), theirs.into()))
        .expect("the standby takes them");
    relay.join().unwrap();
    assert!(!got.detail.contains("K1"), "{got:?}");

    prepare_and_relay(&rt, &client, &new);
    rt.block_on(new.call::<NodeHandoff>(HandoffParams {
        deadline_ms: Some(60_000),
        ..phase(HandoffPhase::Seal)
    }))
    .expect("seal");
    rt.block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Commit)))
        .expect("commit");
    wait_resumed(&rt, &new);
    assert!(exit_status(&mut daemon, "after its commit").success());
    std::thread::sleep(Duration::from_millis(500));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    check_written(&staging.join("w"), written, 4096);

    // It serves on the handed-over credentials (a static source, one
    // generation), and its own successor could get them the same way.
    let client = rt.block_on(Client::connect_unix(&next_socket)).unwrap();
    let listing = rt
        .block_on(client.call::<FsList>(Default::default()))
        .unwrap();
    let own = listing
        .filesystems
        .iter()
        .find(|f| f.name.is_none())
        .unwrap();
    assert_eq!(own.credentials_generation, 1, "{own:?}");
    // Only to a standby of its own: with none waiting, none are handed out.
    let (_ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let none = rt.block_on(old_credentials(&client, theirs)).unwrap_err();
    assert!(none.message.contains("no handoff is pending"), "{none:?}");
    let (mut third, _, third_handoff) =
        standby_with_env(_dir.path(), "c", &["--await-unlock"], &policy_env);
    standby_client(&rt, &mut third, &third_handoff);
    let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let reader = std::thread::spawn(move || {
        constellation_control::handoff_wire::read_secret(&mut ours).unwrap()
    });
    rt.block_on(old_credentials(&client, theirs)).unwrap();
    assert!(reader.join().unwrap().is_some());
    drop(third);
    let audit = std::fs::read_to_string(_dir.path().join("state/control-audit.jsonl")).unwrap();
    assert!(!audit.contains("K1-secret"), "the audit log holds a secret");
    use constellation_control::methods::ViewUnmount;
    use constellation_control::proto::types::ViewUnmountParams;
    rt.block_on(client.call_bounded::<ViewUnmount>(
        ViewUnmountParams {
            mountpoint: staging.clone(),
        },
        Duration::from_secs(30),
    ))
    .expect("view.unmount on the new daemon");
    umount(&staging);
}

/// A serving engine's `Credentials` step onto `theirs`.
async fn old_credentials(
    client: &Client,
    theirs: std::os::unix::net::UnixStream,
) -> Result<HandoffReport, constellation_control::proto::ControlError> {
    client
        .call_with_fd::<NodeHandoff>(phase(HandoffPhase::Credentials), theirs.into())
        .await
}

/// `pid` dumps no core (`forbid_core_dumps`).
fn assert_no_core(pid: u32) {
    let limits = std::fs::read_to_string(format!("/proc/{pid}/limits")).unwrap();
    let core = limits
        .lines()
        .find(|l| l.starts_with("Max core file size"))
        .unwrap();
    let fields: Vec<&str> = core.split_whitespace().collect();
    assert_eq!(&fields[4..6], ["0", "0"], "{core}");
}

/// A standby that waits for its credentials stops at once on `SIGTERM`
/// (and `SIGINT`) while nothing is committed — as its container's PID 1
/// it would otherwise ignore it for the pod's whole grace period — and
/// dumps no core.
#[test]
fn an_awaiting_standby_stops_on_a_signal_before_any_commit() {
    for sig in [libc::SIGTERM, libc::SIGINT] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("sockets/unit/control.sock");
        let s3 = dir.path().join("s3");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _old = awaiting_serve(dir.path(), &s3, &socket, &["--create"]);
        let url = s3.display().to_string();
        rt.block_on(async {
            let gate = Client::connect_unix(&socket).await.unwrap();
            gate.call::<FsUnlock>(unlock_with(&url, None))
                .await
                .unwrap();
        });
        let (mut next, _, handoff) = standby_with(dir.path(), "b", &["--await-unlock"]);
        standby_client(&rt, &mut next, &handoff);
        assert_no_core(next.0.id());
        let sent = Instant::now();
        signal(&next, sig);
        let status = exit_status(&mut next, "on a signal as a standby");
        assert!(status.success(), "signal {sig}: {status:?}");
        assert!(
            sent.elapsed() < Duration::from_secs(10),
            "signal {sig}: took {:?}",
            sent.elapsed()
        );
    }
}

/// Must-fix 2 of 37-k6a's review: a serving engine hands its
/// `fs.unlock` credentials to the node plugin only — the caller that
/// matched its `kind = "service"` grant labelled `csi-node-plugin` — never
/// to its owner (anybody who can exec into its pod runs as that uid) nor
/// to another admin grant, and only while a standby waits on its state
/// dir.
#[test]
fn a_serving_engines_credentials_go_to_the_node_plugin_only() {
    // SAFETY: no preconditions.
    let uid = unsafe { libc::geteuid() };
    for label in [None, Some("csi-other"), Some("csi-node-plugin")] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("sockets/unit/control.sock");
        let s3 = dir.path().join("s3");
        let policy = dir.path().join("control-allow.toml");
        let grants = match label {
            Some(label) => node_plugin_policy(dir.path(), uid, label, &["", "b"]),
            None => String::new(),
        };
        std::fs::write(&policy, grants).unwrap();
        let policy_env = [("CONSTELLATION_CONTROL_POLICY", policy.as_path())];
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _old = awaiting_serve_env(dir.path(), &s3, &socket, &["--create"], &policy_env);
        let url = s3.display().to_string();
        let client = rt.block_on(async {
            let gate = Client::connect_unix(&socket).await.unwrap();
            gate.call::<FsUnlock>(unlock_with(&url, None))
                .await
                .unwrap();
            Client::connect_unix(&socket).await.unwrap()
        });
        let refused = |what: &str| {
            let (_ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
            let err = rt
                .block_on(old_credentials(&client, theirs))
                .expect_err(what);
            assert!(!err.message.contains("K1"), "{err:?}");
            err
        };
        if label != Some("csi-node-plugin") {
            // Not even with a standby waiting.
            let (mut next, _, handoff) =
                standby_with_env(dir.path(), "b", &["--await-unlock"], &policy_env);
            standby_client(&rt, &mut next, &handoff);
            let err = refused("the owner or another grant");
            assert_eq!(err.kind, ErrorKind::Denied, "{label:?}: {err:?}");
            assert!(
                err.message.contains("only the CSI node plugin"),
                "{label:?}: {err:?}"
            );
            continue;
        }
        let err = refused("no standby waits");
        assert!(err.message.contains("no handoff is pending"), "{err:?}");
        let (mut next, _, handoff) =
            standby_with_env(dir.path(), "b", &["--await-unlock"], &policy_env);
        standby_client(&rt, &mut next, &handoff);
        let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
        let reader = std::thread::spawn(move || {
            ours.set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            constellation_control::handoff_wire::read_secret(&mut ours).unwrap()
        });
        rt.block_on(old_credentials(&client, theirs))
            .expect("the node plugin takes them for a pending handoff");
        assert!(reader.join().unwrap().is_some());
        // The standby's marker goes with its wait.
        signal(&next, libc::SIGTERM);
        assert!(exit_status(&mut next, "on SIGTERM as a standby").success());
        let err = refused("the standby is gone");
        assert!(err.message.contains("no handoff is pending"), "{err:?}");
    }
}

/// 37-k6a review nit: an engine on its environment's keys keeps only an
/// unlocked E2E passphrase for its successor, and a later passphrase-only
/// `fs.unlock` replaces that one — the handoff hands over the current
/// passphrase, not the one it started with.
#[test]
fn a_passphrase_rotation_reaches_the_handoff() {
    // SAFETY: no preconditions.
    let uid = unsafe { libc::geteuid() };
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("sockets/unit/control.sock");
    let s3 = dir.path().join("s3");
    let policy = dir.path().join("control-allow.toml");
    std::fs::write(
        &policy,
        node_plugin_policy(dir.path(), uid, "csi-node-plugin", &[""]),
    )
    .unwrap();
    let policy_env = [("CONSTELLATION_CONTROL_POLICY", policy.as_path())];
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _old = awaiting_serve_env(
        dir.path(),
        &s3,
        &socket,
        &["--create", "--e2e"],
        &policy_env,
    );
    let url = s3.display().to_string();
    let passphrase_only = |p: &str| FsUnlockParams {
        fs: url.clone(),
        credentials: UnlockCredentials {
            e2e_passphrase: Some(Secret::new(p)),
            ..Default::default()
        },
    };
    let client = rt.block_on(async {
        let gate = Client::connect_unix(&socket).await.unwrap();
        gate.call::<FsUnlock>(passphrase_only("first passphrase"))
            .await
            .unwrap();
        let client = Client::connect_unix(&socket).await.unwrap();
        client
            .call::<FsUnlock>(passphrase_only("second passphrase"))
            .await
            .unwrap();
        client
    });
    let (mut next, _, handoff) =
        standby_with_env(dir.path(), "b", &["--await-unlock"], &policy_env);
    standby_client(&rt, &mut next, &handoff);
    let (mut ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let reader = std::thread::spawn(move || {
        ours.set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        constellation_control::handoff_wire::read_secret(&mut ours).unwrap()
    });
    rt.block_on(old_credentials(&client, theirs)).unwrap();
    let frame = reader.join().unwrap().expect("it holds a passphrase");
    let held: UnlockCredentials = serde_json::from_slice(&frame).unwrap();
    assert!(
        held.access_key_id.is_none(),
        "the environment's keys stay out"
    );
    assert!(
        held.e2e_passphrase
            .as_ref()
            .is_some_and(|p| p.expose() == "second passphrase"),
        "the rotated passphrase is handed over"
    );
}

/// Every step before the commit is undone by `Abort`: the old daemon
/// serves again in place (the writer sees no error), the standby drops
/// what it received and exits 0.
#[test]
fn an_aborted_handoff_leaves_the_old_serve_serving() {
    let Some(Preopened {
        daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view()
    else {
        return;
    };
    let (mut next, _, handoff) = standby(_dir.path(), "b");
    let new = standby_client(&rt, &mut next, &handoff);
    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(300));
    prepare_and_relay(&rt, &client, &new);
    let aborted = rt
        .block_on(new.call::<NodeHandoff>(phase(HandoffPhase::Abort)))
        .expect("the standby's abort");
    assert!(matches!(aborted.state, Some(HandoffState::Failed { .. })));
    let back = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Abort)))
        .expect("the old daemon's abort");
    assert_eq!(back.state, Some(HandoffState::Serving));
    let status = exit_status(&mut next, "after its abort");
    assert!(status.success(), "{status:?}");
    std::thread::sleep(Duration::from_millis(500));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    assert_eq!(
        std::fs::metadata(staging.join("w")).unwrap().len(),
        written * 4096
    );
    // Still the old daemon's, and a second prepare is possible again.
    drop(daemon);
    std::thread::sleep(Duration::from_millis(200));
    let _ = std::fs::metadata(&staging);
    umount(&staging);
}

/// Must-fix 1 of 37-k5a's review: once the sender has committed, the
/// standby holds the only copies of the sessions, so its seal deadline no
/// longer applies — a sender slow to let go of the state dir (here: it
/// exits 4 s after its commit, the seal allowed 1 s) delays the takeover,
/// never ends the mounts. An `Abort` is refused from then on.
#[test]
fn a_committed_handoff_outlasts_the_seal_deadline() {
    let Some(Preopened {
        mut daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view_and(
        &[],
        &[("CONSTELLATION_FAULT_HANDOFF_EXIT_DELAY_MS", "4000")],
    )
    else {
        return;
    };
    let (mut next, _, handoff) = standby(_dir.path(), "b");
    let new = standby_client(&rt, &mut next, &handoff);
    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(300));
    prepare_and_relay(&rt, &client, &new);
    rt.block_on(new.call::<NodeHandoff>(HandoffParams {
        deadline_ms: Some(1000),
        ..phase(HandoffPhase::Seal)
    }))
    .expect("seal");
    let committed = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Commit)))
        .expect("commit");
    assert_eq!(committed.state, Some(HandoffState::Committed));
    std::thread::sleep(Duration::from_millis(2000));
    assert!(
        daemon.0.try_wait().unwrap().is_none(),
        "the sender is still exiting"
    );
    let state = rt
        .block_on(new.call::<NodeHandoff>(phase(HandoffPhase::Status)))
        .unwrap()
        .state;
    assert_eq!(
        state,
        Some(HandoffState::Sealed),
        "past its seal deadline, still waiting"
    );
    let refused = rt
        .block_on(new.call::<NodeHandoff>(phase(HandoffPhase::Abort)))
        .unwrap_err();
    assert!(refused.message.contains("committed"), "{refused:?}");
    wait_resumed(&rt, &new);
    let status = exit_status(&mut daemon, "after its commit");
    assert!(status.success(), "{status:?}");
    std::thread::sleep(Duration::from_millis(500));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    check_written(&staging.join("w"), written, 4096);
    assert!(
        !_dir.path().join("state/handoff.committed").exists(),
        "the receiver removed the commit marker"
    );
    drop(next);
    std::thread::sleep(Duration::from_millis(200));
    umount(&staging);
}

/// Must-fix 1 of 37-k6a's review: a signal that reaches a sealed standby
/// (a node drain signals both pods) does not end it while the sender may
/// still commit — the plugin sends `Commit` right after `Seal` answers,
/// without asking whether the standby lives. The commit lands, the
/// standby serves the sessions (the writer sees no error), and the signal
/// is not lost either (should-fix 1): the node takes it as its own, so
/// with a view mounted it waits for the view (§7), then exits 0.
#[test]
fn a_signal_between_seal_and_commit_loses_no_session() {
    use constellation_control::methods::ViewUnmount;
    use constellation_control::proto::types::ViewUnmountParams;
    let Some(Preopened {
        mut daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view()
    else {
        return;
    };
    let (mut next, next_socket, handoff) = standby(_dir.path(), "b");
    let new = standby_client(&rt, &mut next, &handoff);
    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(300));
    prepare_and_relay(&rt, &client, &new);
    rt.block_on(new.call::<NodeHandoff>(HandoffParams {
        deadline_ms: Some(60_000),
        ..phase(HandoffPhase::Seal)
    }))
    .expect("seal");
    signal(&next, libc::SIGTERM);
    std::thread::sleep(Duration::from_millis(1000));
    assert!(
        next.0.try_wait().unwrap().is_none(),
        "a sealed standby outlives a signal while the sender may commit"
    );
    let committed = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Commit)))
        .expect("commit");
    assert_eq!(committed.state, Some(HandoffState::Committed));
    wait_resumed(&rt, &new);
    assert!(exit_status(&mut daemon, "after its commit").success());
    std::thread::sleep(Duration::from_millis(500));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    check_written(&staging.join("w"), written, 4096);
    // The signal was handed to the node: deferred while the view is
    // mounted, then the process ends with it.
    std::thread::sleep(Duration::from_millis(1000));
    assert!(
        next.0.try_wait().unwrap().is_none(),
        "the SIGTERM is deferred while a view is mounted"
    );
    let client = rt.block_on(Client::connect_unix(&next_socket)).unwrap();
    rt.block_on(client.call_bounded::<ViewUnmount>(
        ViewUnmountParams {
            mountpoint: staging.clone(),
        },
        Duration::from_secs(30),
    ))
    .expect("view.unmount on the new daemon");
    let status = exit_status(&mut next, "on the signal it got while sealed");
    assert!(status.success(), "{status:?}");
    umount(&staging);
}

/// The other half of the same fix: a sealed standby that got a signal and
/// sees no commit exits at the seal's deadline (not later), and the sender
/// serves its sessions again on `Abort`.
#[test]
fn a_signalled_sealed_standby_exits_at_its_deadline_without_a_commit() {
    let Some(Preopened {
        daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view()
    else {
        return;
    };
    let (mut next, _, handoff) = standby(_dir.path(), "b");
    let new = standby_client(&rt, &mut next, &handoff);
    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(300));
    prepare_and_relay(&rt, &client, &new);
    rt.block_on(new.call::<NodeHandoff>(HandoffParams {
        deadline_ms: Some(3000),
        ..phase(HandoffPhase::Seal)
    }))
    .expect("seal");
    let sent = Instant::now();
    signal(&next, libc::SIGTERM);
    let status = exit_status(&mut next, "at its seal deadline");
    assert!(status.success(), "{status:?}");
    assert!(
        sent.elapsed() >= Duration::from_millis(2500) && sent.elapsed() < Duration::from_secs(15),
        "exited after {:?}",
        sent.elapsed()
    );
    let back = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Abort)))
        .expect("the old daemon's abort");
    assert_eq!(back.state, Some(HandoffState::Serving));
    std::thread::sleep(Duration::from_millis(500));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    assert_eq!(
        std::fs::metadata(staging.join("w")).unwrap().len(),
        written * 4096
    );
    drop(daemon);
    std::thread::sleep(Duration::from_millis(200));
    let _ = std::fs::metadata(&staging);
    umount(&staging);
}

/// Should-fix 1 of 37-k5a's review: a prepare held up (here by a test
/// hook; in production by an op on a FUSE worker, which the drain timeout
/// does not bound) is aborted while it runs. The abort is recorded, the
/// prepare serves its sessions again the moment it ends — not after the
/// watchdog's deadline — and the next prepare is not refused as busy.
#[test]
fn an_abort_during_a_slow_prepare_serves_again_at_once() {
    let Some(Preopened {
        daemon,
        socket,
        staging,
        rt,
        client,
        _dir,
    }) = serve_with_a_preopened_view_and(
        &[],
        &[("CONSTELLATION_FAULT_HANDOFF_PREPARE_DELAY_MS", "3000")],
    )
    else {
        return;
    };
    let writer = Writer::start(staging.join("w"));
    std::thread::sleep(Duration::from_millis(300));
    let preparing = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(Client::connect_unix(&socket)).unwrap();
        let started = Instant::now();
        let result = rt.block_on(client.call::<NodeHandoff>(HandoffParams {
            drain_timeout_ms: Some(5000),
            deadline_ms: Some(60_000),
            ..phase(HandoffPhase::Prepare)
        }));
        (result, started.elapsed())
    });
    std::thread::sleep(Duration::from_millis(800));
    let aborted = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Abort)))
        .expect("the abort");
    assert!(aborted.detail.contains("under way"), "{aborted:?}");
    let (result, took) = preparing.join().unwrap();
    let err = result.expect_err("an aborted prepare fails");
    assert!(
        err.message.contains("aborted while it was preparing"),
        "{err:?}"
    );
    assert!(
        took < Duration::from_secs(20),
        "served again at once, not at the deadline: {took:?}"
    );
    let status = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Status)))
        .unwrap();
    assert_eq!(status.state, Some(HandoffState::Serving));
    // Not left "upgrading": the next attempt prepares.
    let again = rt
        .block_on(client.call::<NodeHandoff>(HandoffParams {
            drain_timeout_ms: Some(5000),
            deadline_ms: Some(60_000),
            ..phase(HandoffPhase::Prepare)
        }))
        .expect("a second prepare");
    assert_eq!(again.state, Some(HandoffState::Prepared));
    let back = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Abort)))
        .unwrap();
    assert_eq!(back.state, Some(HandoffState::Serving));
    std::thread::sleep(Duration::from_millis(300));
    let (written, errors) = writer.finish();
    assert!(errors.is_empty(), "the writer saw errors: {errors:?}");
    check_written(&staging.join("w"), written, 4096);
    drop(daemon);
    std::thread::sleep(Duration::from_millis(200));
    umount(&staging);
}

/// The busy writer of 37-k5a's review: `--write-mode back`, 64 KiB
/// appends, an `fsync` every 2 s, across a handoff — no error, every byte
/// in place, and the longest any one call waited (the client-visible
/// pause: from the drain's start to the new process serving) reported.
/// The commit stops the old engine without a drain, so what it left
/// (journal, pending uploads) is shipped by the new one: its clean exit at
/// the end (`SIGTERM` with no view: the final drain must leave nothing)
/// says so.
#[test]
fn a_busy_write_back_writer_crosses_a_handoff() {
    use constellation_control::methods::ViewUnmount;
    use constellation_control::proto::types::ViewUnmountParams;
    let Some(Preopened {
        mut daemon,
        staging,
        rt,
        client,
        _dir,
        ..
    }) = serve_with_a_preopened_view_and(&["--write-mode", "back"], &[])
    else {
        return;
    };
    let (mut next, next_socket, handoff) =
        standby_with(_dir.path(), "b", &["--write-mode", "back"]);
    let new = standby_client(&rt, &mut next, &handoff);
    const BLOCK: usize = 64 * 1024;
    let writer = Writer::start_with(
        staging.join("w"),
        BLOCK,
        Duration::from_millis(1),
        Cadence::Every(Duration::from_secs(2)),
    );
    std::thread::sleep(Duration::from_secs(3));
    let started = Instant::now();
    prepare_and_relay(&rt, &client, &new);
    rt.block_on(new.call::<NodeHandoff>(HandoffParams {
        deadline_ms: Some(60_000),
        ..phase(HandoffPhase::Seal)
    }))
    .expect("seal");
    let committed = rt
        .block_on(client.call::<NodeHandoff>(phase(HandoffPhase::Commit)))
        .expect("commit");
    eprintln!("commit: {} ({} ms)", committed.detail, committed.elapsed_ms);
    wait_resumed(&rt, &new);
    eprintln!("handoff, prepare to resumed: {:?}", started.elapsed());
    assert!(exit_status(&mut daemon, "after its commit").success());
    std::thread::sleep(Duration::from_secs(3));
    let written = writer.finish_written();
    assert!(
        written.errors.is_empty(),
        "the writer saw errors: {:?}",
        written.errors
    );
    eprintln!(
        "busy writer: {} block(s) of {BLOCK} B; client-visible pause (longest call) {:?}",
        written.blocks, written.longest
    );
    check_written(&staging.join("w"), written.blocks, BLOCK);
    let client = rt.block_on(Client::connect_unix(&next_socket)).unwrap();
    rt.block_on(client.call_bounded::<ViewUnmount>(
        ViewUnmountParams {
            mountpoint: staging.clone(),
        },
        Duration::from_secs(30),
    ))
    .expect("view.unmount on the new daemon");
    umount(&staging);
    signal(&next, libc::SIGTERM);
    let status = exit_status(&mut next, "on SIGTERM with no view");
    assert!(
        status.success(),
        "the final drain left something: {status:?}"
    );
}
