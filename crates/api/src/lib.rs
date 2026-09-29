//! Control plane (DESIGN.md §10): a unix-socket JSON API the mount
//! daemon serves from its state dir. The CLI (and later the web UI)
//! queries it for live status — including spool observability: how much
//! unflushed metadata is waiting for S3.
//!
//! Wire format: one JSON request per line, one JSON response per line.

pub mod types;
#[cfg(feature = "web")]
pub mod web;

pub use types::HandoverStatus;
pub use types::{
    AckStatus, CasProbeStatus, CtoStatus, FuseRequestsStatus, HeldInodeStatus, HeldStatus,
    LockStatus, LogStreamStatus, OwnS3Status, PeerPathsStatus, RemoteChunkStatus, S3RequestStatus,
    SessionStatus, StalledFuseRequest,
};
pub use types::{
    AtimeStatus, CacheEntryStatus, CacheStatus, CoopStatus, DelegationReport, DelegationStatus,
    DesignationStatus, DirectoryEntry, DoctorStatus, DownloadSession, EpochStatus, InboxStatus,
    InspectStatus, LeaseStatus, ManifestStatus, MountInfo, MountViewOpts, P2pStatus, PeerStatus,
    PinStatus, PrefetchStatus, PruneRootStatus, PruneStatus, QuotaStatus, ReintegrationStatus,
    Request, Response, SnapshotStatus, SourceStatus, SpeculationStatus, SpoolStatus, StatusReport,
    WritebackStatus,
};

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

pub const SOCKET_NAME: &str = "control.sock";

/// Providers answer API requests with live daemon state.
///
/// `status` is read-only; the pin operations mutate node-local state and
/// may do I/O (admission checks, eager fetch scheduling), so they return
/// a result the caller reports back over the socket. Default
/// implementations refuse, so a provider that does not support pinning
/// (tests, older daemons) stays valid.
pub trait StatusSource: Send + Sync + 'static {
    fn status(&self) -> StatusReport;

    fn pin(&self, _path: &str) -> std::result::Result<String, String> {
        Err("pinning is not supported by this daemon".into())
    }

    fn unpin(&self, _path: &str) -> std::result::Result<String, String> {
        Err("pinning is not supported by this daemon".into())
    }

    fn list_pins(&self) -> Vec<PinStatus> {
        Vec::new()
    }

    fn offline(&self, _path: &str, _read_only: bool) -> std::result::Result<String, String> {
        Err("offline designation is not supported by this daemon".into())
    }

    fn online(&self, _path: &str) -> std::result::Result<String, String> {
        Err("offline designation is not supported by this daemon".into())
    }

    fn list_designations(&self) -> Vec<DesignationStatus> {
        Vec::new()
    }

    /// Plan 30 §M11.
    fn delegate(
        &self,
        _path: &str,
        _node: u64,
        _range: Option<&str>,
    ) -> std::result::Result<String, String> {
        Err("delegation is not supported by this daemon".into())
    }

    fn undelegate(&self, _path: &str) -> std::result::Result<String, String> {
        Err("delegation is not supported by this daemon".into())
    }

    fn list_delegations(&self) -> Vec<DelegationStatus> {
        Vec::new()
    }

    fn reintegrate(&self) -> std::result::Result<String, String> {
        Err("reintegration is not supported by this daemon".into())
    }

    fn leave(&self, _node_id: Option<u64>, _force: bool) -> std::result::Result<String, String> {
        Err("leave is not supported by this daemon".into())
    }

    fn set_write_mode(&self, _mode: &str) -> std::result::Result<String, String> {
        Err("write-mode switching is not supported by this daemon".into())
    }

    fn snapshot_create(&self, _selector: &str) -> std::result::Result<String, String> {
        Err("snapshots are not supported by this daemon".into())
    }

    fn snapshot_list(
        &self,
        _path: Option<&str>,
    ) -> std::result::Result<Vec<SnapshotStatus>, String> {
        Err("snapshots are not supported by this daemon".into())
    }

    fn snapshot_delete(&self, _selector: &str) -> std::result::Result<String, String> {
        Err("snapshots are not supported by this daemon".into())
    }

    fn clone_snapshot(
        &self,
        _selector: &str,
        _destination: &str,
    ) -> std::result::Result<String, String> {
        Err("clones are not supported by this daemon".into())
    }

    fn snap_refs(&self, _id: &str) -> std::result::Result<Vec<String>, String> {
        Err("snapshot references are not supported by this daemon".into())
    }

    fn read_dir(&self, _path: &str) -> std::result::Result<Vec<DirectoryEntry>, String> {
        Err("directory browsing is not supported by this daemon".into())
    }

    fn inspect(&self, _path: &str) -> std::result::Result<InspectStatus, String> {
        Err("inspection is not supported by this daemon".into())
    }

    /// Open a streaming download of a regular file. Default refuses.
    ///
    /// Implementations must produce at most one chunk buffer at a time
    /// (bounded channel) so large files are not buffered whole in RAM.
    fn open_download(&self, _path: &str) -> std::result::Result<DownloadSession, String> {
        Err("file download is not supported by this daemon".into())
    }

    fn force_release(&self, _part: &str) -> std::result::Result<String, String> {
        Err("lease release is not supported by this daemon".into())
    }

    fn log_tail(&self, _lines: usize) -> Vec<String> {
        Vec::new()
    }

    fn doctor(&self) -> std::result::Result<DoctorStatus, String> {
        Err("backend probes are not supported by this daemon".into())
    }

    /// Plan 30 §M4: discard the records held back behind `ino`'s
    /// unrecoverable chunk(s) into a conflict copy.
    fn drop_held(&self, _ino: u64, _remote: bool) -> std::result::Result<String, String> {
        Err("drop-held is not supported by this daemon".into())
    }

    fn cache_list(&self) -> Vec<CacheEntryStatus> {
        Vec::new()
    }

    fn cache_prune(&self, _target_bytes: u64) -> std::result::Result<String, String> {
        Err("cache prune is not supported by this daemon".into())
    }

    fn set_quota(&self, _max_bytes: Option<u64>) -> std::result::Result<String, String> {
        Err("quota is not supported by this daemon".into())
    }

    fn get_quota(&self) -> std::result::Result<(Option<u64>, u64), String> {
        Err("quota is not supported by this daemon".into())
    }

    /// Run one retention-prune pass (plan 22). Default refuses.
    fn prune_run(
        &self,
        _path: Option<&str>,
        _dry_run: bool,
    ) -> std::result::Result<String, String> {
        Err("pruning is not supported by this daemon".into())
    }

    /// List marked prune roots and their effective policies.
    fn prune_ls(&self) -> std::result::Result<Vec<PruneRootStatus>, String> {
        Err("pruning is not supported by this daemon".into())
    }

    /// Run chunk + metadata-tree GC in this process (see
    /// [`crate::Request::GcRun`]). Default refuses.
    fn gc_run(&self, _verify_only: bool) -> std::result::Result<serde_json::Value, String> {
        Err("gc is not supported by this daemon".into())
    }

    /// Run `fsck` in this process (see [`crate::Request::FsckRun`]).
    /// Default refuses.
    fn fsck_run(
        &self,
        _repair: bool,
        _force_release: Option<&str>,
    ) -> std::result::Result<serde_json::Value, String> {
        Err("fsck is not supported by this daemon".into())
    }

    /// Attach a new view. Default refuses so a provider (tests, older
    /// daemons) that predates multi-view mounts stays valid.
    fn mount_add(
        &self,
        _subtree: &str,
        _mountpoint: &Path,
        _opts: &MountViewOpts,
    ) -> std::result::Result<String, String> {
        Err("mount-add is not supported by this daemon".into())
    }

    /// Detach the view mounted at `mountpoint`.
    fn mount_remove(&self, _mountpoint: &Path) -> std::result::Result<String, String> {
        Err("mount-remove is not supported by this daemon".into())
    }

    /// Every view currently mounted by this daemon.
    fn mount_list(&self) -> Vec<MountInfo> {
        Vec::new()
    }

    /// Plan 31 C4b: hand the daemon's views over to `binary`
    /// ([`Request::Upgrade`]).
    fn upgrade(&self, _binary: Option<&Path>) -> std::result::Result<String, String> {
        Err("in-place upgrade is not supported by this daemon".into())
    }
}

/// The single request dispatcher shared by unix sockets and HTTP. Keeping
/// transport code outside this match is the parity guarantee: both adapters
/// deserialize the same enum and invoke this exact function.
pub fn dispatch(source: &dyn StatusSource, request: Request) -> Response {
    let result = |result: std::result::Result<String, String>| match result {
        Ok(detail) => Response::Ok { detail },
        Err(message) => Response::Error { message },
    };
    match request {
        Request::Status => Response::Status(Box::new(source.status())),
        Request::Ping => Response::Pong,
        Request::Pin { path } => result(source.pin(&path)),
        Request::Unpin { path } => result(source.unpin(&path)),
        Request::ListPins => Response::Pins {
            pins: source.list_pins(),
        },
        Request::Offline { path, read_only } => result(source.offline(&path, read_only)),
        Request::Online { path } => result(source.online(&path)),
        Request::ListDesignations => Response::Designations {
            designations: source.list_designations(),
        },
        Request::Delegate { path, node, range } => {
            result(source.delegate(&path, node, range.as_deref()))
        }
        Request::Undelegate { path } => result(source.undelegate(&path)),
        Request::ListDelegations => Response::Delegations {
            delegations: source.list_delegations(),
        },
        Request::Reintegrate => result(source.reintegrate()),
        Request::Leave { node_id, force } => result(source.leave(node_id, force)),
        Request::SetWriteMode { mode } => result(source.set_write_mode(&mode)),
        Request::SnapshotCreate { selector } => result(source.snapshot_create(&selector)),
        Request::PruneRun { path, dry_run } => result(source.prune_run(path.as_deref(), dry_run)),
        Request::PruneList => match source.prune_ls() {
            Ok(roots) => Response::PruneRoots { roots },
            Err(message) => Response::Error { message },
        },
        Request::GcRun { verify_only } => match source.gc_run(verify_only) {
            Ok(report) => Response::GcReport { report },
            Err(message) => Response::Error { message },
        },
        Request::FsckRun {
            repair,
            force_release,
        } => match source.fsck_run(repair, force_release.as_deref()) {
            Ok(report) => Response::FsckReport { report },
            Err(message) => Response::Error { message },
        },
        Request::SnapshotList { path } | Request::ListSnapshots { path } => {
            match source.snapshot_list(path.as_deref()) {
                Ok(snapshots) => Response::Snapshots { snapshots },
                Err(message) => Response::Error { message },
            }
        }
        Request::SnapshotDelete { selector } => result(source.snapshot_delete(&selector)),
        Request::Clone {
            selector,
            destination,
        } => result(source.clone_snapshot(&selector, &destination)),
        Request::SnapRefs { id } => match source.snap_refs(&id) {
            Ok(hashes) => Response::Refs { hashes },
            Err(message) => Response::Error { message },
        },
        Request::ReadDir { path } => match source.read_dir(&path) {
            Ok(entries) => Response::Directory { path, entries },
            Err(message) => Response::Error { message },
        },
        Request::Inspect { path } => match source.inspect(&path) {
            Ok(entry) => Response::Inspection { entry },
            Err(message) => Response::Error { message },
        },
        Request::ForceRelease { part } => result(source.force_release(&part)),
        Request::LogTail { lines } => Response::Logs {
            lines: source.log_tail(lines.min(10_000)),
        },
        Request::Doctor => match source.doctor() {
            Ok(report) => Response::Doctor { report },
            Err(message) => Response::Error { message },
        },
        Request::CacheList => Response::CacheEntries {
            entries: source.cache_list(),
        },
        Request::CachePrune { target_bytes } => result(source.cache_prune(target_bytes)),
        Request::SetQuota { max_bytes } => result(source.set_quota(max_bytes)),
        Request::GetQuota => match source.get_quota() {
            Ok((max_bytes, used_bytes)) => Response::Quota {
                max_bytes,
                used_bytes,
            },
            Err(message) => Response::Error { message },
        },
        Request::MountAdd {
            subtree,
            mountpoint,
            opts,
        } => result(source.mount_add(&subtree, &mountpoint, &opts)),
        Request::MountRemove { mountpoint } => result(source.mount_remove(&mountpoint)),
        Request::MountList => Response::Mounts {
            mounts: source.mount_list(),
        },
        Request::DropHeld { ino, remote } => result(source.drop_held(ino, remote)),
        Request::Upgrade { binary } => result(source.upgrade(binary.as_deref())),
    }
}

/// Serve the control API on `<state_dir>/control.sock` until the task
/// is dropped. Returns the socket path.
pub fn serve(state_dir: &Path, source: Arc<dyn StatusSource>) -> Result<PathBuf> {
    let sock = state_dir.join(SOCKET_NAME);
    let _ = std::fs::remove_file(&sock); // stale socket from a crash
    let listener = UnixListener::bind(&sock).context("binding control socket")?;
    serve_on(listener, source);
    Ok(sock)
}

/// [`serve`] on a listener already bound — plan 31 C4b: one a previous
/// image of this daemon bound and handed over across `exec`, so a client
/// connecting during the handover waits in the backlog instead of finding
/// no daemon.
pub fn serve_listener(
    listener: std::os::unix::net::UnixListener,
    source: Arc<dyn StatusSource>,
) -> Result<()> {
    listener
        .set_nonblocking(true)
        .context("the handed-over control socket")?;
    let listener = UnixListener::from_std(listener).context("the handed-over control socket")?;
    serve_on(listener, source);
    Ok(())
}

fn serve_on(listener: UnixListener, source: Arc<dyn StatusSource>) {
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let source = source.clone();
            tokio::spawn(async move {
                if let Err(e) = handle(stream, source).await {
                    tracing::debug!(error = %e, "control connection error");
                }
            });
        }
    });
}

/// Ceiling on the bytes we will buffer for one control-socket request
/// line. A valid request (one JSON object per line) is tiny; this cap
/// exists only so a client that connects and then sends bytes without ever
/// sending a newline cannot make us grow an unbounded buffer — a trivial
/// local denial of service. 8 MiB is far above any real request and well
/// below anything that threatens the daemon's memory.
const MAX_REQUEST_LINE: u64 = 8 * 1024 * 1024;

async fn handle(stream: UnixStream, source: Arc<dyn StatusSource>) -> Result<()> {
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        // `take` yields EOF after the cap, so `read_until` returns without
        // a newline once a line runs long instead of buffering forever.
        // Reading one extra byte lets us tell a line that is exactly at the
        // cap from one that overruns it.
        let n = (&mut reader)
            .take(MAX_REQUEST_LINE + 1)
            .read_until(b'\n', &mut buf)
            .await?;
        if n == 0 {
            break; // clean EOF: the peer closed the connection
        }
        // No delimiter within the cap: either an over-long line the peer
        // is holding the connection open with (tell it why, then stop
        // reading), or the peer's last request, sent without a trailing
        // newline before it closed its side — answered like any other,
        // as the line reader this replaced did.
        let last = !buf.ends_with(b"\n");
        if last && buf.len() as u64 > MAX_REQUEST_LINE {
            let resp = Response::Error {
                message: format!(
                    "request line exceeds the {MAX_REQUEST_LINE}-byte limit; closing connection"
                ),
            };
            let mut out = serde_json::to_vec(&resp)?;
            out.push(b'\n');
            let _ = w.write_all(&out).await;
            break;
        }
        let line = String::from_utf8_lossy(&buf);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Request>(line) {
            Ok(request) => dispatch(source.as_ref(), request),
            Err(e) => Response::Error {
                message: format!("bad request: {e}"),
            },
        };
        let mut out = serde_json::to_vec(&resp)?;
        out.push(b'\n');
        w.write_all(&out).await?;
        if last {
            break;
        }
    }
    Ok(())
}

/// One-shot client call against a daemon's control socket. Unbounded:
/// some requests (prune, GC, fsck, leave) are answered only when the
/// work is done. A caller that must not hang on a daemon that never
/// answers uses [`call_bounded`], or [`ping`] first.
pub async fn call(sock: &Path, req: &Request) -> Result<Response> {
    let stream = UnixStream::connect(sock)
        .await
        .with_context(|| format!("connecting to {} (is the mount running?)", sock.display()))?;
    let (r, mut w) = stream.into_split();
    let mut buf = serde_json::to_vec(req)?;
    buf.push(b'\n');
    w.write_all(&buf).await?;
    w.shutdown().await?;
    let mut lines = BufReader::new(r).lines();
    let line = lines
        .next_line()
        .await?
        .context("daemon closed the connection without a response")?;
    Ok(serde_json::from_str(&line)?)
}

/// [`call`] with a deadline on the whole exchange (connect, send, wait
/// for the answer). A daemon whose listener is still open but that will
/// never answer — the campaign 6 B-1 shape: a `kill -9`ed daemon whose
/// last thread is stuck in the kernel keeps its socket and its lock —
/// accepts the connection and then says nothing; without a bound the
/// caller parks forever.
pub async fn call_bounded(sock: &Path, req: &Request, within: Duration) -> Result<Response> {
    match tokio::time::timeout(within, call(sock, req)).await {
        Ok(resp) => resp,
        Err(_) => bail!(
            "the daemon at {} did not answer within {within:?}",
            sock.display()
        ),
    }
}

/// Is the daemon behind `sock` alive *and answering*? A bare `connect`
/// succeeding proves only that a listener exists (a wedged process keeps
/// its listener); a `Ping` answered within `within` proves the daemon's
/// runtime is serving. `Ok(false)` when there is no listener at all
/// (no socket file, or a stale one nobody listens on).
pub async fn ping(sock: &Path, within: Duration) -> Result<bool> {
    if !sock.exists() {
        return Ok(false);
    }
    match tokio::time::timeout(within, call(sock, &Request::Ping)).await {
        Ok(Ok(Response::Pong)) => Ok(true),
        Ok(Ok(other)) => bail!("unexpected answer to ping: {other:?}"),
        Ok(Err(e)) if is_connect_error(&e) => Ok(false),
        Ok(Err(e)) => Err(e),
        Err(_) => bail!(
            "the daemon at {} accepted the connection but did not answer a ping within {within:?}",
            sock.display()
        ),
    }
}

fn is_connect_error(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    impl StatusSource for Fake {
        fn status(&self) -> StatusReport {
            StatusReport {
                delegation: Default::default(),
                handover: Default::default(),
                fs_uuid: "test-uuid".into(),
                backend: "s3://bucket/prefix".into(),
                mounts: vec![MountInfo {
                    id: 1,
                    subtree: String::new(),
                    mountpoint: "/mnt/x".into(),
                    mounted_ms_ago: 1000,
                }],
                node_id: 1,
                version: "1.0.0-test".into(),
                enrolled: true,
                uptime_s: 12,
                spool: SpoolStatus {
                    journal_backlog: 3,
                    head_seq: 7,
                    conflicts: 0,
                    last_ship_error: None,
                    ship_rounds_completed: 5,
                    ship_rounds_cancelled: 1,
                },
                cache: CacheStatus {
                    used_bytes: 100,
                    budget_bytes: 1000,
                    chunks: 5,
                    pinned_bytes: 0,
                    staging_bytes: 0,
                    staging_budget_bytes: 250,
                },
                lease: LeaseStatus {
                    held: true,
                    holder: 1,
                    epoch: 4,
                    expires_in_ms: 30_000,
                    lost: false,
                },
                p2p: P2pStatus::default(),
                pins: Vec::new(),
                designations: Vec::new(),
                epoch: EpochStatus::default(),
                reintegration: ReintegrationStatus::default(),
                speculation: SpeculationStatus::default(),
                held: HeldStatus::default(),
                session: SessionStatus::default(),
                cto: crate::CtoStatus::default(),
                locks: crate::LockStatus::default(),
                ack: crate::AckStatus::default(),
                coop: CoopStatus::default(),
                prefetch: PrefetchStatus::default(),
                writeback: WritebackStatus::default(),
                forwarded_ok: 0,
                forwarded_err: 0,
                forward_p50_ms: None,
                log_stream: LogStreamStatus::default(),
                forward_dedup_hits: 0,
                forward_retries: 0,
                forward_indoubt_resolved: 0,
                own_s3: OwnS3Status::default(),
                placement_reason: None,
                quota: QuotaStatus::default(),
                atime: AtimeStatus::default(),
                prune: PruneStatus::default(),
                inbox: InboxStatus::default(),
                s3: Default::default(),
                fuse_requests: Default::default(),
            }
        }

        fn list_pins(&self) -> Vec<PinStatus> {
            vec![PinStatus {
                path: "/data".into(),
                bytes: 42,
                chunks_cached: 1,
                chunks_total: 2,
            }]
        }

        fn list_designations(&self) -> Vec<DesignationStatus> {
            vec![DesignationStatus {
                path: "/site".into(),
                designee: 1,
                read_only: false,
            }]
        }

        fn snapshot_list(
            &self,
            _path: Option<&str>,
        ) -> std::result::Result<Vec<SnapshotStatus>, String> {
            Ok(vec![SnapshotStatus {
                id: "snap-id".into(),
                path: "/".into(),
                name: "nightly".into(),
                root_hash: "00".repeat(32),
                created_unix_ms: 1,
            }])
        }

        fn read_dir(&self, path: &str) -> std::result::Result<Vec<DirectoryEntry>, String> {
            Ok(vec![DirectoryEntry {
                name: "file".into(),
                path: format!("{}/file", path.trim_end_matches('/')),
                ino: 2,
                kind: "file".into(),
            }])
        }

        fn inspect(&self, path: &str) -> std::result::Result<InspectStatus, String> {
            Ok(InspectStatus {
                path: path.into(),
                ino: 2,
                kind: "file".into(),
                size: 4,
                ..InspectStatus::default()
            })
        }

        fn force_release(&self, part: &str) -> std::result::Result<String, String> {
            Ok(format!("released {part}"))
        }

        fn log_tail(&self, lines: usize) -> Vec<String> {
            vec![format!("last {lines}")]
        }

        fn doctor(&self) -> std::result::Result<DoctorStatus, String> {
            Ok(DoctorStatus {
                create_if_absent: true,
                etag_cas: true,
                ..Default::default()
            })
        }

        fn cache_list(&self) -> Vec<CacheEntryStatus> {
            vec![CacheEntryStatus {
                hash: "00".repeat(32),
                size: 4,
                state: "clean".into(),
            }]
        }

        fn cache_prune(&self, target_bytes: u64) -> std::result::Result<String, String> {
            Ok(format!("pruned to {target_bytes} bytes"))
        }
    }

    /// A client's last request may arrive without a trailing newline,
    /// followed by end of stream; it is answered like any other.
    #[tokio::test]
    async fn a_final_request_without_a_newline_is_answered() {
        let dir = tempfile::tempdir().unwrap();
        let sock = serve(dir.path(), Arc::new(Fake)).unwrap();
        let stream = UnixStream::connect(&sock).await.unwrap();
        let (r, mut w) = stream.into_split();
        w.write_all(&serde_json::to_vec(&Request::Ping).unwrap())
            .await
            .unwrap();
        w.shutdown().await.unwrap();
        let line = BufReader::new(r).lines().next_line().await.unwrap();
        let line = line.expect("the unterminated request must still be answered");
        assert!(matches!(
            serde_json::from_str::<Response>(&line).unwrap(),
            Response::Pong
        ));
    }

    #[tokio::test]
    async fn status_roundtrip_over_unix_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock = serve(dir.path(), Arc::new(Fake)).unwrap();
        match call(&sock, &Request::Status).await.unwrap() {
            Response::Status(s) => {
                assert_eq!(s.fs_uuid, "test-uuid");
                assert_eq!(s.spool.journal_backlog, 3);
                assert_eq!(s.cache.chunks, 5);
                assert_eq!((s.lease.epoch, s.lease.held), (4, true));
            }
            other => panic!("unexpected response {other:?}"),
        }
        match call(&sock, &Request::Ping).await.unwrap() {
            Response::Pong => {}
            other => panic!("unexpected response {other:?}"),
        }
    }

    /// `ListPins` must round-trip end to end. Regression: `Response::Pins`
    /// was a bare newtype variant (`Pins(Vec<PinStatus>)`). Serde's
    /// internally-tagged representation (`tag = "resp"`) cannot serialize
    /// a newtype variant whose payload is a sequence — `to_vec` returned
    /// `Err`, `handle()` propagated it via `?`, and the connection closed
    /// having written zero bytes. The only trace was a `debug!`-level log
    /// line, invisible under the daemon's default `info` filter, so a
    /// caller just saw "daemon closed the connection without a response"
    /// with nothing in the daemon's log to explain why. The fix wraps the
    /// vec in a struct variant (`Pins { pins: Vec<PinStatus> }`).
    #[tokio::test]
    async fn list_pins_round_trips_over_the_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock = serve(dir.path(), Arc::new(Fake)).unwrap();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            call(&sock, &Request::ListPins),
        )
        .await
        .expect("list_pins must not hang")
        .expect("list_pins must not close the connection with no response");
        match resp {
            Response::Pins { pins } => {
                assert_eq!(pins.len(), 1);
                assert_eq!(pins[0].path, "/data");
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    /// `ListDesignations` uses the same struct-variant shape as `Pins`;
    /// verify it round-trips too rather than assuming the pattern was
    /// applied correctly by analogy.
    #[tokio::test]
    async fn list_designations_round_trips_over_the_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock = serve(dir.path(), Arc::new(Fake)).unwrap();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            call(&sock, &Request::ListDesignations),
        )
        .await
        .expect("list_designations must not hang")
        .expect("list_designations must not close the connection with no response");
        match resp {
            Response::Designations { designations } => {
                assert_eq!(designations.len(), 1);
                assert_eq!(designations[0].path, "/site");
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    /// Every additive request variant must use the same dispatcher on both
    /// transports. Adding a variant makes this table visibly incomplete in
    /// review, while the central exhaustive match prevents either adapter
    /// from quietly inventing different semantics.
    #[cfg(feature = "web")]
    #[tokio::test]
    async fn unix_and_http_adapters_have_request_parity() {
        let requests = vec![
            Request::Ping,
            Request::Status,
            Request::Pin { path: "/x".into() },
            Request::Unpin { path: "/x".into() },
            Request::ListPins,
            Request::Offline {
                path: "/x".into(),
                read_only: false,
            },
            Request::Online { path: "/x".into() },
            Request::ListDesignations,
            Request::Reintegrate,
            Request::Leave {
                node_id: Some(9),
                force: false,
            },
            Request::SetWriteMode {
                mode: "back".into(),
            },
            Request::SnapshotCreate {
                selector: "/@x".into(),
            },
            Request::SnapshotList { path: None },
            Request::ListSnapshots { path: None },
            Request::SnapshotDelete {
                selector: "/@x".into(),
            },
            Request::Clone {
                selector: "/@x".into(),
                destination: "/clone".into(),
            },
            Request::SnapRefs { id: "x".into() },
            Request::ReadDir { path: "/".into() },
            Request::Inspect { path: "/".into() },
            Request::ForceRelease { part: "p0".into() },
            Request::LogTail { lines: 10 },
            Request::Doctor,
            Request::CacheList,
            Request::CachePrune { target_bytes: 0 },
            Request::MountAdd {
                subtree: "/".into(),
                mountpoint: "/mnt/y".into(),
                opts: MountViewOpts::default(),
            },
            Request::MountRemove {
                mountpoint: "/mnt/y".into(),
            },
            Request::MountList,
        ];
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Fake);
        let sock = serve(dir.path(), source.clone()).unwrap();
        for request in requests {
            let unix = call(&sock, &request).await.unwrap();
            let http = web::adapt(source.as_ref(), request.clone());
            assert_eq!(
                serde_json::to_value(unix).unwrap(),
                serde_json::to_value(http).unwrap(),
                "transport mismatch for {request:?}"
            );
        }
    }

    #[test]
    fn new_handlers_return_structured_results_from_an_in_memory_source() {
        let source = Fake;
        assert!(matches!(
            dispatch(&source, Request::ReadDir { path: "/".into() }),
            Response::Directory { entries, .. } if entries.len() == 1
        ));
        assert!(matches!(
            dispatch(&source, Request::Inspect { path: "/file".into() }),
            Response::Inspection { entry } if entry.ino == 2
        ));
        assert!(matches!(
            dispatch(&source, Request::ListSnapshots { path: None }),
            Response::Snapshots { snapshots } if snapshots.len() == 1
        ));
        assert!(matches!(
            dispatch(&source, Request::ForceRelease { part: "p0".into() }),
            Response::Ok { .. }
        ));
        assert!(matches!(
            dispatch(&source, Request::LogTail { lines: 7 }),
            Response::Logs { lines } if lines == ["last 7"]
        ));
        assert!(matches!(
            dispatch(&source, Request::Doctor),
            Response::Doctor { report } if report.etag_cas
        ));
        assert!(matches!(
            dispatch(&source, Request::CacheList),
            Response::CacheEntries { entries } if entries.len() == 1
        ));
        assert!(matches!(
            dispatch(&source, Request::CachePrune { target_bytes: 0 }),
            Response::Ok { detail } if detail.contains("pruned")
        ));
    }
}
