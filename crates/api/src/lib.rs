//! Control plane (DESIGN.md §10): a unix-socket JSON API the mount
//! daemon serves from its state dir. The CLI (and later the web UI)
//! queries it for live status — including spool observability: how much
//! unflushed metadata is waiting for S3.
//!
//! Wire format: one JSON request per line, one JSON response per line.

pub mod types;

pub use types::{CacheStatus, Request, Response, SpoolStatus, StatusReport};

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

pub const SOCKET_NAME: &str = "control.sock";

/// Providers answer API requests with live daemon state.
pub trait StatusSource: Send + Sync + 'static {
    fn status(&self) -> StatusReport;
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
                uptime_s: 12,
                spool: SpoolStatus {
                    journal_backlog: 3,
                    shipped_seq: 7,
                    last_ship_error: None,
                },
                cache: CacheStatus {
                    used_bytes: 100,
                    budget_bytes: 1000,
                    chunks: 5,
                },
            }
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
            }
            other => panic!("unexpected response {other:?}"),
        }
        match call(&sock, &Request::Ping).await.unwrap() {
            Response::Pong => {}
            other => panic!("unexpected response {other:?}"),
        }
    }
}
