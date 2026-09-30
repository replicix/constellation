//! Sizing the FUSE dispatcher: how many worker threads a mount runs
//! (`n_threads`), and the kernel request queue `FUSE_INIT` negotiates for
//! them (moved from the CLI's host thread plan by plan 31 C4: the
//! numbers are FUSE's, the host facts they start from are the CLI's).

/// The most workers a mount runs.
pub const FUSE_THREAD_HARD_MAX: usize = 64;

/// fuser's per-worker request buffer.
const FUSE_BUFFER_BYTES: u64 = 16 * 1024 * 1024;

/// Workers for a host with `cpus` CPUs and `memory_bytes` of RAM (the
/// cgroup's limit when there is one).
pub fn recommended_workers(cpus: usize, memory_bytes: Option<u64>) -> usize {
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

/// The kernel's `max_background` for `workers` workers.
pub fn max_background(workers: usize) -> u16 {
    workers.saturating_mul(8).clamp(16, 512) as u16
}

/// The kernel's `congestion_threshold` for `workers` workers.
pub fn congestion_threshold(workers: usize) -> u16 {
    max_background(workers) * 3 / 4
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuse_threads_scale_sublinearly() {
        let ample = Some(1024_u64 * 1024 * 1024 * 1024);
        assert_eq!(recommended_workers(1, ample), 1);
        assert_eq!(recommended_workers(2, ample), 2);
        assert_eq!(recommended_workers(32, ample), 12);
        assert_eq!(recommended_workers(1_920, ample), 64);
    }

    #[test]
    fn fuse_threads_respect_memory_budget() {
        assert_eq!(recommended_workers(1_920, Some(1024 * 1024 * 1024)), 8);
    }

    #[test]
    fn integer_square_root_rounds_up() {
        assert_eq!(ceil_sqrt(1), 1);
        assert_eq!(ceil_sqrt(32), 6);
        assert_eq!(ceil_sqrt(1_920), 44);
    }

    #[test]
    fn kernel_queue_scales_with_fuse_workers() {
        assert_eq!(max_background(1), 16);
        assert_eq!(congestion_threshold(1), 12);
        assert_eq!(max_background(12), 96);
        assert_eq!(max_background(64), 512);
    }
}
