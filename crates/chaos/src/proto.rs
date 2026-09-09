//! Length-prefixed JSON messages for TcpCluster / chaos worker.

use crate::op::{Complete, Op};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Hello,
    Prepare { run_id: String, work_root: String },
    Invoke { op_id: u64, op: Op },
    Barrier { name: String },
    Abort,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    HelloOk { worker_id: String, mount: String },
    PrepareOk,
    Complete { op_id: u64, complete: Complete },
    BarrierOk { name: String },
    Error { message: String },
}

pub async fn write_msg<T: Serialize>(stream: &mut TcpStream, msg: &T) -> Result<()> {
    let bytes = serde_json::to_vec(msg)?;
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_msg<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("reading message length")?;
    let len = u32::from_be_bytes(len_buf) as usize;
    anyhow::ensure!(len < 64 * 1024 * 1024, "message too large: {len}");
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .context("reading message body")?;
    Ok(serde_json::from_slice(&buf)?)
}
