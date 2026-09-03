//! Host-aware thread sizing for the async runtime and FUSE dispatcher.

use std::num::NonZeroUsize;

const FUSE_THREAD_HARD_MAX: usize = 64;
const TOKIO_THREAD_HARD_MAX: usize = 32;
const BLOCKING_THREAD_HARD_MAX: usize = 256;
const FUSE_BUFFER_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ThreadPlan {
    pub(crate) cpus: usize,
    pub(crate) fuse: usize,
    pub(crate) tokio: usize,
    pub(crate) blocking: usize,
}

impl ThreadPlan {
    pub(crate) fn fuse_max_background(self) -> u16 {
        self.fuse.saturating_mul(8).clamp(16, 512) as u16
    }

    pub(crate) fn fuse_congestion_threshold(self) -> u16 {
        self.fuse_max_background() * 3 / 4
    }
}

pub(crate) fn thread_plan() -> ThreadPlan {
    let cpus = std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1);
    let memory = host_memory_bytes();
    let fuse = if cfg!(target_os = "linux") {
        env_override("CONSTELLATION_FUSE_THREADS", FUSE_THREAD_HARD_MAX)
            .unwrap_or_else(|| recommended_fuse_threads(cpus, memory))
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

fn recommended_fuse_threads(cpus: usize, memory_bytes: Option<u64>) -> usize {
    if cpus <= 1 {
        return 1;
    }

    // FUSE callbacks include hashing, staging I/O, SQLite, and waits on the
    // async runtime. Twice sqrt(CPUs) gives useful oversubscription without
    // creating thousands of 16 MiB request buffers on very large machines.
    let cpu_target = 2 * ceil_sqrt(cpus);
    let memory_cap = memory_bytes
        .map(|bytes| {
            // Spend at most one eighth of host RAM on fuser request buffers.
            (bytes / (FUSE_BUFFER_BYTES * 8)) as usize
        })
        .unwrap_or(FUSE_THREAD_HARD_MAX);
    cpu_target
        .min(cpus)
        .min(memory_cap.max(1))
        .clamp(1, FUSE_THREAD_HARD_MAX)
}

fn ceil_sqrt(value: usize) -> usize {
    let mut low = 0usize;
    let mut high = value.min(1 << (usize::BITS / 2));
    while low < high {
        let mid = low + (high - low) / 2;
        if mid.saturating_mul(mid) >= value {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    low
}

fn env_override(name: &str, hard_max: usize) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(hard_max))
}

#[cfg(target_os = "linux")]
fn host_memory_bytes() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    let host_kib = contents
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    let host = host_kib.checked_mul(1024)?;
    let cgroup = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .into_iter()
    .filter_map(|path| std::fs::read_to_string(path).ok())
    .filter_map(|value| value.trim().parse::<u64>().ok())
    .filter(|value| *value < u64::MAX / 2)
    .min();
    Some(cgroup.map_or(host, |limit| limit.min(host)))
}

#[cfg(not(target_os = "linux"))]
fn host_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuse_threads_scale_sublinearly() {
        let ample = Some(1024_u64 * 1024 * 1024 * 1024);
        assert_eq!(recommended_fuse_threads(1, ample), 1);
        assert_eq!(recommended_fuse_threads(2, ample), 2);
        assert_eq!(recommended_fuse_threads(32, ample), 12);
        assert_eq!(recommended_fuse_threads(1_920, ample), 64);
    }

    #[test]
    fn fuse_threads_respect_memory_budget() {
        assert_eq!(recommended_fuse_threads(1_920, Some(1024 * 1024 * 1024)), 8);
    }

    #[test]
    fn integer_square_root_rounds_up() {
        assert_eq!(ceil_sqrt(1), 1);
        assert_eq!(ceil_sqrt(32), 6);
        assert_eq!(ceil_sqrt(1_920), 44);
    }

    #[test]
    fn runtime_pools_scale_and_cap() {
        assert_eq!(recommended_tokio_threads(1), 1);
        assert_eq!(recommended_tokio_threads(32), 32);
        assert_eq!(recommended_tokio_threads(1_920), 32);
        assert_eq!(recommended_blocking_threads(1), 4);
        assert_eq!(recommended_blocking_threads(32), 128);
        assert_eq!(recommended_blocking_threads(1_920), 256);
    }

    #[test]
    fn kernel_queue_scales_with_fuse_workers() {
        let mut plan = ThreadPlan {
            cpus: 1,
            fuse: 1,
            tokio: 1,
            blocking: 4,
        };
        assert_eq!(plan.fuse_max_background(), 16);
        assert_eq!(plan.fuse_congestion_threshold(), 12);
        plan.fuse = 12;
        assert_eq!(plan.fuse_max_background(), 96);
        plan.fuse = 64;
        assert_eq!(plan.fuse_max_background(), 512);
    }
}
