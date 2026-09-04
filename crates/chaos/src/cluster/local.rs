//! In-process cluster: each worker is a mount path on this host.

use super::Cluster;
use crate::op::{execute_op, Complete, Op};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub struct LocalCluster {
    mounts: Vec<PathBuf>,
}

impl LocalCluster {
    pub fn new(mounts: Vec<PathBuf>) -> Result<Self> {
        anyhow::ensure!(!mounts.is_empty(), "LocalCluster needs at least one mount");
        for m in &mounts {
            anyhow::ensure!(m.is_dir(), "mount is not a directory: {}", m.display());
        }
        Ok(Self { mounts })
    }

    pub fn from_paths<P: AsRef<Path>>(paths: impl IntoIterator<Item = P>) -> Result<Self> {
        Self::new(
            paths
                .into_iter()
                .map(|p| p.as_ref().to_path_buf())
                .collect(),
        )
    }
}

impl Cluster for LocalCluster {
    fn worker_count(&self) -> usize {
        self.mounts.len()
    }

    fn prepare(&mut self, _run_id: &str, work_root: &str) -> Result<()> {
        for m in &self.mounts {
            let p = m.join(work_root);
            // With several mounts of one filesystem, a sibling's mkdir
            // can land at the lease holder between our lookup and our
            // own mkdir: the mkdir comes back EEXIST while the local
            // replica has not tailed the entry yet, so `is_dir()` is
            // briefly false and `create_dir_all` reports failure.
            // Close-to-open consistency allows that; retry until the
            // directory is visible locally.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                match std::fs::create_dir_all(&p) {
                    Ok(()) => break,
                    Err(_) if p.is_dir() => break,
                    Err(e) if std::time::Instant::now() >= deadline => {
                        return Err(e).with_context(|| format!("mkdir {}", p.display()));
                    }
                    Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
                }
            }
        }
        Ok(())
    }

    fn invoke(&self, worker: usize, _op_id: u64, op: &Op) -> Result<Complete> {
        let root = self
            .mounts
            .get(worker)
            .with_context(|| format!("worker index {worker} out of range"))?;
        execute_op(root, op)
    }

    fn barrier(&mut self, _name: &str) -> Result<()> {
        // Invokes for a step complete before the coordinator calls barrier
        // when using the parallel step helper; local barriers are instantaneous.
        Ok(())
    }
}
