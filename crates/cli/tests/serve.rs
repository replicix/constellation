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
