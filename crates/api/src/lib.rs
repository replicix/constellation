//! Control plane (DESIGN.md §10): a unix-socket JSON API the mount
//! daemon serves from its state dir. The CLI (and later the web UI)
//! queries it for live status — including spool observability: how much
//! unflushed metadata is waiting for S3.
//!
//! Wire format: one JSON request per line, one JSON response per line.

pub mod types;

pub use types::{
    CacheStatus, CoopStatus, DesignationStatus, EpochStatus, LeaseStatus, P2pStatus,
    PartitionStatus, PeerStatus, PinStatus, ReintegrationStatus, Request, Response, SourceStatus,
    SpoolStatus, StatusReport,
};

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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

    fn reintegrate(&self) -> std::result::Result<String, String> {
        Err("reintegration is not supported by this daemon".into())
    }

    fn leave(&self, _node_id: Option<u64>, _force: bool) -> std::result::Result<String, String> {
        Err("leave is not supported by this daemon".into())
    }
}

/// Serve the control API on `<state_dir>/control.sock` until the task
/// is dropped. Returns the socket path.
pub fn serve(state_dir: &Path, source: Arc<dyn StatusSource>) -> Result<PathBuf> {
    let sock = state_dir.join(SOCKET_NAME);
    let _ = std::fs::remove_file(&sock); // stale socket from a crash
    let listener = UnixListener::bind(&sock).context("binding control socket")?;
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
    Ok(sock)
}

async fn handle(stream: UnixStream, source: Arc<dyn StatusSource>) -> Result<()> {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Request>(&line) {
            Ok(Request::Status) => Response::Status(Box::new(source.status())),
            Ok(Request::Ping) => Response::Pong,
            Ok(Request::Pin { path }) => match source.pin(&path) {
                Ok(detail) => Response::Ok { detail },
                Err(message) => Response::Error { message },
            },
            Ok(Request::Unpin { path }) => match source.unpin(&path) {
                Ok(detail) => Response::Ok { detail },
                Err(message) => Response::Error { message },
            },
            Ok(Request::ListPins) => Response::Pins {
                pins: source.list_pins(),
            },
            Ok(Request::Offline { path, read_only }) => match source.offline(&path, read_only) {
                Ok(detail) => Response::Ok { detail },
                Err(message) => Response::Error { message },
            },
            Ok(Request::Online { path }) => match source.online(&path) {
                Ok(detail) => Response::Ok { detail },
                Err(message) => Response::Error { message },
            },
            Ok(Request::ListDesignations) => Response::Designations {
                designations: source.list_designations(),
            },
            Ok(Request::Reintegrate) => match source.reintegrate() {
                Ok(detail) => Response::Ok { detail },
                Err(message) => Response::Error { message },
            },
            Ok(Request::Leave { node_id, force }) => match source.leave(node_id, force) {
                Ok(detail) => Response::Ok { detail },
                Err(message) => Response::Error { message },
            },
            Err(e) => Response::Error {
                message: format!("bad request: {e}"),
            },
        };
        let mut buf = serde_json::to_vec(&resp)?;
        buf.push(b'\n');
        w.write_all(&buf).await?;
    }
    Ok(())
}

/// One-shot client call against a daemon's control socket.
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

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    impl StatusSource for Fake {
        fn status(&self) -> StatusReport {
            StatusReport {
                fs_uuid: "test-uuid".into(),
                backend: "s3://bucket/prefix".into(),
                mountpoint: "/mnt/x".into(),
                node_id: 1,
                enrolled: true,
                uptime_s: 12,
                spool: SpoolStatus {
                    journal_backlog: 3,
                    head_seq: 7,
                    conflicts: 0,
                    last_ship_error: None,
                },
                cache: CacheStatus {
                    used_bytes: 100,
                    budget_bytes: 1000,
                    chunks: 5,
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
                partitions: vec![],
                p2p: P2pStatus::default(),
                pins: Vec::new(),
                designations: Vec::new(),
                epoch: EpochStatus::default(),
                reintegration: ReintegrationStatus::default(),
                coop: CoopStatus::default(),
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
    }

    /// A `StatusSource` whose lease views live behind ONE mutex that both
    /// the `lease` and `partitions` fields must read — exactly the shape
    /// of the real daemon's `DaemonStatus`. In a struct literal every
    /// temporary lives until the whole expression ends, so taking the
    /// lock twice inside it self-deadlocks on the non-reentrant
    /// `std::sync::Mutex` and `status` never answers (while `ping`,
    /// which touches nothing, still does).
    struct MultiPartition {
        leases: std::sync::Mutex<std::collections::HashMap<String, LeaseStatus>>,
    }

    impl MultiPartition {
        fn new() -> Self {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "p0".to_string(),
                LeaseStatus {
                    held: true,
                    holder: 1,
                    epoch: 4,
                    expires_in_ms: 30_000,
                    lost: false,
                },
            );
            m.insert(
                "p1".to_string(),
                LeaseStatus {
                    held: true,
                    holder: 1,
                    epoch: 2,
                    expires_in_ms: 30_000,
                    lost: false,
                },
            );
            Self {
                leases: std::sync::Mutex::new(m),
            }
        }
    }

    impl StatusSource for MultiPartition {
        fn status(&self) -> StatusReport {
            // Read everything the report needs BEFORE building it, so no
            // two guards are ever alive inside the struct literal.
            let lease = self.leases.lock().unwrap().get("p0").cloned();
            let partitions: Vec<PartitionStatus> = {
                let views = self.leases.lock().unwrap();
                let mut v: Vec<PartitionStatus> = views
                    .iter()
                    .map(|(id, l)| PartitionStatus {
                        id: id.clone(),
                        root_path: if id == "p0" {
                            "/".into()
                        } else {
                            "/hot".into()
                        },
                        lease: l.clone(),
                    })
                    .collect();
                v.sort_by(|a, b| a.id.cmp(&b.id));
                v
            };
            StatusReport {
                fs_uuid: "multi".into(),
                backend: "s3://bucket/prefix".into(),
                mountpoint: "/mnt/x".into(),
                node_id: 1,
                enrolled: true,
                uptime_s: 1,
                spool: SpoolStatus {
                    journal_backlog: 0,
                    head_seq: 9,
                    conflicts: 0,
                    last_ship_error: None,
                },
                cache: CacheStatus {
                    used_bytes: 0,
                    budget_bytes: 1,
                    chunks: 0,
                    staging_bytes: 0,
                    staging_budget_bytes: 0,
                },
                lease: lease.unwrap_or_default(),
                partitions,
                p2p: P2pStatus::default(),
                pins: Vec::new(),
                designations: Vec::new(),
                epoch: EpochStatus::default(),
                reintegration: ReintegrationStatus::default(),
                coop: CoopStatus::default(),
            }
        }
    }

    /// `status` must answer promptly even when the report exposes the
    /// whole partition map. Regression: the daemon locked its lease map
    /// once per field inside the `StatusReport` literal and deadlocked,
    /// which hung every control-API caller (`ping` kept working, so the
    /// daemon looked alive).
    #[tokio::test]
    async fn status_with_partitions_does_not_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        let sock = serve(dir.path(), Arc::new(MultiPartition::new())).unwrap();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            call(&sock, &Request::Status),
        )
        .await
        .expect("status deadlocked: the daemon never answered")
        .unwrap();
        match resp {
            Response::Status(s) => {
                let ids: Vec<&str> = s.partitions.iter().map(|p| p.id.as_str()).collect();
                assert_eq!(ids, ["p0", "p1"]);
                assert_eq!(s.lease.epoch, 4, "legacy lease field is p0");
                assert_eq!(s.partitions[1].root_path, "/hot");
            }
            other => panic!("unexpected response {other:?}"),
        }
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
}
