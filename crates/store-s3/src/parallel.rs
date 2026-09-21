//! Private thread pools for the CPU-bound halves of metadata GC
//! (plan 28 §S7a, sized by §14.9).
//!
//! Both the reachability mark and the pack rewrite are compute-bound
//! walks over immutable bytes, and §14.9 measured what that is worth:
//! mark scales ~4×, compaction stalled at ~1.7× *because the pack
//! writer was serial*. Neither belongs on the tokio runtime — a
//! multi-second zstd burst on a runtime worker starves whatever FUSE
//! request shares it — and neither belongs on rayon's *global* pool
//! either, which is why this module exists at all.
//!
//! Two reasons for a private pool rather than `rayon::current_num_threads`:
//!
//! - GC is the one subsystem whose parallelism an operator must be able
//!   to cap. §14.5 measured GC and compaction consuming 939 s of
//!   1,205 s of wall clock; "how much of the machine may that take" is
//!   a knob, and the global pool has no per-caller answer to it.
//! - the measurement in `compact::tests::compaction_thread_scaling` has
//!   to set the width per run, and a global pool can only be sized
//!   once per process.
//!
//! The pool is built per call site, not per batch: construction is a
//! few `clone`s and N thread spawns, which is noise against a batch
//! that moves tens of MiB, but would not be noise per *pack*. Callers
//! that loop (the compactor) therefore hold one pool for their
//! lifetime.

use crate::error::StoreError;

/// Env `CONSTELLATION_GC_THREADS`: width of the mark and rewrite pools.
/// `0` or unset means "one per core".
pub fn gc_threads() -> usize {
    std::env::var("CONSTELLATION_GC_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(effective_threads)
        .unwrap_or_else(|| effective_threads(0))
}

/// Resolve `0` to the host's parallelism, and never return 0 — a pool
/// of width 0 is rayon's "use the default", which would silently ignore
/// a cap the caller asked for.
pub fn effective_threads(requested: usize) -> usize {
    match requested {
        0 => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        n => n,
    }
}

pub(crate) fn thread_pool(threads: usize) -> Result<rayon::ThreadPool, StoreError> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(effective_threads(threads))
        .thread_name(|i| format!("constellation-gc-{i}"))
        .build()
        .map_err(|e| StoreError::Parallel(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_means_one_per_core_and_never_zero() {
        assert!(effective_threads(0) >= 1);
        assert_eq!(effective_threads(3), 3);
    }

    #[test]
    fn a_pool_runs_at_the_width_it_was_asked_for() {
        let pool = thread_pool(3).unwrap();
        assert_eq!(pool.current_num_threads(), 3);
    }
}
