//! TCP client cluster talking to remote `chaos worker` processes.

use super::Cluster;
use crate::op::{Complete, Op};
use crate::proto::{read_msg, write_msg, Request, Response};
use anyhow::{bail, Context, Result};
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

struct WorkerConn {
    addr: SocketAddr,
    stream: Mutex<TcpStream>,
}

pub struct TcpCluster {
    rt: Runtime,
    workers: Vec<WorkerConn>,
}

impl TcpCluster {
    pub fn connect(addrs: &[SocketAddr]) -> Result<Self> {
        anyhow::ensure!(!addrs.is_empty(), "need at least one worker address");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("tokio runtime")?;
        let mut workers = Vec::with_capacity(addrs.len());
        for &addr in addrs {
            let mut stream = rt
                .block_on(TcpStream::connect(addr))
                .with_context(|| format!("connect {addr}"))?;
            rt.block_on(write_msg(&mut stream, &Request::Hello))?;
            let resp: Response = rt.block_on(read_msg(&mut stream))?;
            match resp {
                Response::HelloOk { .. } => {}
                Response::Error { message } => bail!("hello from {addr}: {message}"),
                other => bail!("unexpected hello response from {addr}: {other:?}"),
            }
            workers.push(WorkerConn {
                addr,
                stream: Mutex::new(stream),
            });
        }
        Ok(Self { rt, workers })
    }

    pub fn parse_workers(specs: &[String]) -> Result<Vec<SocketAddr>> {
        specs
            .iter()
            .map(|s| {
                s.parse::<SocketAddr>()
                    .with_context(|| format!("invalid worker address: {s}"))
            })
            .collect()
    }

    async fn invoke_one(
        stream: &Mutex<TcpStream>,
        addr: SocketAddr,
        op_id: u64,
        op: &Op,
    ) -> Result<Complete> {
        let req = Request::Invoke {
            op_id,
            op: op.clone(),
        };
        let mut guard = stream.lock().expect("worker mutex");
        write_msg(&mut guard, &req)
            .await
            .with_context(|| format!("invoke {addr}"))?;
        let resp: Response = read_msg(&mut guard).await?;
        match resp {
            Response::Complete {
                op_id: id,
                complete,
            } => {
                anyhow::ensure!(id == op_id, "op_id mismatch: sent {op_id} got {id}");
                Ok(complete)
            }
            Response::Error { message } => bail!("invoke {addr}: {message}"),
            other => bail!("unexpected invoke response: {other:?}"),
        }
    }
}

impl Cluster for TcpCluster {
    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn prepare(&mut self, run_id: &str, work_root: &str) -> Result<()> {
        let req = Request::Prepare {
            run_id: run_id.to_string(),
            work_root: work_root.to_string(),
        };
        for w in &self.workers {
            let mut stream = w.stream.lock().expect("worker mutex");
            self.rt
                .block_on(write_msg(&mut stream, &req))
                .with_context(|| format!("prepare {}", w.addr))?;
            let resp: Response = self.rt.block_on(read_msg(&mut stream))?;
            match resp {
                Response::PrepareOk => {}
                Response::Error { message } => bail!("prepare {}: {message}", w.addr),
                other => bail!("unexpected prepare response: {other:?}"),
            }
        }
        Ok(())
    }

    fn invoke(&self, worker: usize, op_id: u64, op: &Op) -> Result<Complete> {
        let w = self
            .workers
            .get(worker)
            .with_context(|| format!("worker {worker}"))?;
        self.rt
            .block_on(Self::invoke_one(&w.stream, w.addr, op_id, op))
    }

    fn invoke_parallel(&self, jobs: &[(usize, u64, Op)]) -> Vec<Result<(usize, u64, Complete)>> {
        let handle = self.rt.handle().clone();
        std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .iter()
                .map(|(w, id, op)| {
                    let handle = handle.clone();
                    scope.spawn(move || {
                        let worker = self
                            .workers
                            .get(*w)
                            .with_context(|| format!("worker {w}"))?;
                        let complete = handle
                            .block_on(Self::invoke_one(&worker.stream, worker.addr, *id, op))?;
                        Ok((*w, *id, complete))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("tcp worker thread panicked"))
                .collect()
        })
    }

    fn barrier(&mut self, name: &str) -> Result<()> {
        let req = Request::Barrier {
            name: name.to_string(),
        };
        for w in &self.workers {
            let mut stream = w.stream.lock().expect("worker mutex");
            self.rt
                .block_on(write_msg(&mut stream, &req))
                .with_context(|| format!("barrier send {}", w.addr))?;
        }
        for w in &self.workers {
            let mut stream = w.stream.lock().expect("worker mutex");
            let resp: Response = self.rt.block_on(read_msg(&mut stream))?;
            match resp {
                Response::BarrierOk { name: n } => {
                    anyhow::ensure!(n == name, "barrier name mismatch");
                }
                Response::Error { message } => bail!("barrier {}: {message}", w.addr),
                other => bail!("unexpected barrier response: {other:?}"),
            }
        }
        Ok(())
    }
}
