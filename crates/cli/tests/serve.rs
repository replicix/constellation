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
    serve_with_a_preopened_view_and(&[], &[])
}

/// [`serve_with_a_preopened_view`] with extra `serve` arguments and
/// environment.
fn serve_with_a_preopened_view_and(args: &[&str], envs: &[(&str, &str)]) -> Option<Preopened> {
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
        .args(args)
        .envs(envs.iter().copied())
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
    let socket = dir.join(format!("sockets/unit/control-{name}.sock"));
    let handoff = dir.join(format!("sockets/unit/handoff-{name}.sock"));
    let child = Command::new(BIN)
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
