//! TCP worker: listen and execute ops against a local mount.

use crate::op::execute_op;
use crate::proto::{read_msg, write_msg, Request, Response};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

pub async fn serve(listen: SocketAddr, mount: PathBuf) -> Result<()> {
    anyhow::ensure!(
        mount.is_dir(),
        "mount is not a directory: {}",
        mount.display()
    );
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    info!(%listen, mount = %mount.display(), "chaos worker listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        info!(%peer, "worker connection");
        let mount = mount.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, mount).await {
                warn!(error = %e, "worker connection error");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, mount: PathBuf) -> Result<()> {
    loop {
        let req: Request = match read_msg(&mut stream).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "client disconnected");
                return Ok(());
            }
        };
        let resp = match req {
            Request::Hello => Response::HelloOk {
                worker_id: hostname(),
                mount: mount.display().to_string(),
            },
            Request::Prepare { run_id, work_root } => match prepare(&mount, &work_root) {
                Ok(()) => {
                    info!(%run_id, %work_root, "prepared");
                    Response::PrepareOk
                }
                Err(e) => Response::Error {
                    message: format!("{e:#}"),
                },
            },
            Request::Invoke { op_id, op } => match execute_op(&mount, &op) {
                Ok(complete) => Response::Complete { op_id, complete },
                Err(e) => Response::Error {
                    message: format!("{e:#}"),
                },
            },
            Request::Barrier { name } => {
                // Coordinator collects acks from all workers before proceeding.
                Response::BarrierOk { name }
            }
            Request::Abort | Request::Shutdown => {
                let _ = write_msg(
                    &mut stream,
                    &Response::BarrierOk {
                        name: "shutdown".into(),
                    },
                )
                .await;
                return Ok(());
            }
        };
        write_msg(&mut stream, &resp).await?;
    }
}

fn prepare(mount: &Path, work_root: &str) -> Result<()> {
    let p = mount.join(work_root);
    std::fs::create_dir_all(&p).with_context(|| format!("mkdir {}", p.display()))?;
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "worker".into())
}
