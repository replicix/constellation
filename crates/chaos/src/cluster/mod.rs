//! Cluster backends: local in-process mounts and remote TCP workers.

mod local;
mod tcp;

pub use local::LocalCluster;
pub use tcp::TcpCluster;

use crate::op::{Complete, Op};
use anyhow::Result;

/// Transport-agnostic worker pool used by [`crate::Coordinator`].
pub trait Cluster: Send + Sync {
    fn worker_count(&self) -> usize;

    /// Ensure `work_root` exists on every worker mount.
    fn prepare(&mut self, run_id: &str, work_root: &str) -> Result<()>;

    /// Execute `op` on `worker` and return the completion.
    /// Safe to call concurrently for distinct `worker` indices on LocalCluster.
    fn invoke(&self, worker: usize, op_id: u64, op: &Op) -> Result<Complete>;

    /// Synchronize all workers on a named barrier.
    fn barrier(&mut self, name: &str) -> Result<()>;

    /// Run many invokes, preferably in parallel across workers.
    fn invoke_parallel(&self, jobs: &[(usize, u64, Op)]) -> Vec<Result<(usize, u64, Complete)>> {
        std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .iter()
                .map(|(w, id, op)| {
                    scope.spawn(move || {
                        let complete = self.invoke(*w, *id, op)?;
                        Ok((*w, *id, complete))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("worker thread panicked"))
                .collect()
        })
    }
}
