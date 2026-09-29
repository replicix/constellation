//! Host-aware thread sizing for the async runtime and FUSE dispatcher.
//!
//! The FUSE numbers (how many workers a host gets, the kernel queue they
//! negotiate) are the FUSE frontend's (`constellation_frontend_fuse::
//! threads`, plan 31 C4); this module gathers the host facts and the
//! overrides, and sizes the runtime's own pools.

use std::num::NonZeroUsize;

pub(crate) use constellation_frontend_fuse::threads::FUSE_THREAD_HARD_MAX;
const TOKIO_THREAD_HARD_MAX: usize = 32;
const BLOCKING_THREAD_HARD_MAX: usize = 256;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ThreadPlan {
    pub(crate) cpus: usize,
    pub(crate) fuse: usize,
    pub(crate) tokio: usize,
    pub(crate) blocking: usize,
}

pub(crate) fn thread_plan() -> ThreadPlan {
    let cpus = std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1);
    let memory = host_memory_bytes();
    let fuse = if cfg!(target_os = "linux") {
        env_override("CONSTELLATION_FUSE_THREADS", FUSE_THREAD_HARD_MAX).unwrap_or_else(|| {
            constellation_frontend_fuse::threads::recommended_workers(cpus, memory)
        })
    } else {
        1
    };
    let tokio = env_override("CONSTELLATION_TOKIO_THREADS", TOKIO_THREAD_HARD_MAX)
        .unwrap_or_else(|| recommended_tokio_threads(cpus));
    let blocking = env_override("CONSTELLATION_BLOCKING_THREADS", BLOCKING_THREAD_HARD_MAX)
        .unwrap_or_else(|| recommended_blocking_threads(cpus));
    ThreadPlan {
        cpus,
        fuse,
        tokio,
        blocking,
    }
}

fn recommended_tokio_threads(cpus: usize) -> usize {
    cpus.clamp(1, TOKIO_THREAD_HARD_MAX)
}

fn recommended_blocking_threads(cpus: usize) -> usize {
    // Tokio creates blocking workers lazily, so this is a ceiling rather than
    // startup overhead. Keep tiny machines tight while permitting parallel
    // compression and filesystem I/O on large hosts.
    cpus.saturating_mul(4).clamp(4, BLOCKING_THREAD_HARD_MAX)
}

fn env_override(name: &str, hard_max: usize) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(hard_max))
}

/// Host RAM, capped by the cgroup's memory limit when there is one (the
/// host's `Process::memory_budget`: `/proc/meminfo` and
/// `/sys/fs/cgroup` on Linux).
fn host_memory_bytes() -> Option<u64> {
    constellation_platform::native().process.memory_budget()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_pools_scale_and_cap() {
        assert_eq!(recommended_tokio_threads(1), 1);
        assert_eq!(recommended_tokio_threads(32), 32);
        assert_eq!(recommended_tokio_threads(1_920), 32);
        assert_eq!(recommended_blocking_threads(1), 4);
        assert_eq!(recommended_blocking_threads(32), 128);
        assert_eq!(recommended_blocking_threads(1_920), 256);
    }
}
