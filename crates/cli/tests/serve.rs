//! `constellation serve` + `control-relay` end to end (plan 37 K2's engine
//! pod, minus Kubernetes): a headless daemon on the local file backend,
//! reached the way the CSI controller reaches a controller-owned engine
//! pod — the control protocol over a relay process's stdin/stdout — and
//! the subtree quota `quota.set{subtree}` the controller's `CreateVolume`
//! sends.

use constellation_control::methods::{BrowseMkdir, FsCreate, FsList, QuotaGet, QuotaSet};
use constellation_control::proto::types::{
    FsCreateParams, MkdirParams, QuotaGetParams, SetQuotaParams,
};
use constellation_control::proto::ErrorKind;
use constellation_control::transport::StreamTransport;
use constellation_control::{Client, ClientOptions, Principal};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_constellation");

fn ping(socket: &Path) -> bool {
    Command::new(BIN)
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
    let child = Command::new(BIN)
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
        let mut relay = tokio::process::Command::new(BIN)
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
    // SAFETY: no preconditions.
    if unsafe { libc::geteuid() } != 0 || !Path::new("/dev/fuse").exists() {
        eprintln!("skipping: needs root and /dev/fuse (mount(2))");
        return None;
    }
    use constellation_control::methods::ViewMount;
    use constellation_control::proto::types::{MountSource, MountViewOpts, ViewMountParams};

    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("sockets/unit/control.sock");
    let child = Command::new(BIN)
        .arg("serve")
        .arg("--s3")
        .arg(dir.path().join("s3"))
        .arg("--state-dir")
        .arg(dir.path().join("state"))
        .arg("--control-socket")
        .arg(&socket)
        .arg("--create")
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

    let staging = dir.path().join("staging/pv-a/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    let mut kernel = constellation_platform::MountOpts::new("constellation-k3a-test");
    kernel.allow_other = true;
    let fd = constellation_platform::linux::fuse_mount_fd(&staging, &kernel).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
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
