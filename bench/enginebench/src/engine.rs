//! The common surface every candidate engine implements: the FUSE-shaped
//! op set from plan 28 §P6's "What FUSE actually asks for" table.
//!
//! Every engine stores exactly the §P6 key/value encoding
//! (`constellation_mtree::keys` / `record`) *except* the SQLite engine,
//! which uses `constellation-meta`'s real relational schema — that
//! difference (in particular: SQLite's `dentry` table has no attr copy,
//! so its `readdirplus` needs a join/point-lookup per row where the
//! tree/redb/fjall engines already have the copy in the `0x02` value)
//! is the whole point of the comparison, not an oversight.

use constellation_mtree::record::{Attrs, Kind};

#[derive(Clone, Debug)]
pub struct PlusEntry {
    pub name: Vec<u8>,
    pub ino: u64,
    pub kind: Kind,
    pub attrs: Attrs,
}

/// What a mutating op should report, so the harness can attribute
/// physical bytes written without every engine re-deriving it.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteCost {
    /// Bytes handed to the engine's durability path for this one op
    /// (WAL record, SQL statement's logical payload, KV batch bytes).
    pub logical_bytes: u64,
}

pub trait Engine: Send + Sync {
    fn name(&self) -> &'static str;

    // ---- reads -----------------------------------------------------
    fn lookup(&self, parent: u64, name: &[u8]) -> Option<(u64, Attrs)>;
    fn getattr(&self, ino: u64) -> Option<Attrs>;
    /// Plain readdir: name + ino + kind. `start_after` is the last name
    /// seen (the cursor), matching §P6's "the cursor is a key" claim.
    fn readdir(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, u64, Kind)>;
    /// readdirplus: full attrs per entry. Left as a *separate* method
    /// (rather than a flag) so each engine's implementation makes its
    /// real cost visible instead of hiding it behind a shared code path.
    fn readdirplus(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<PlusEntry>;
    fn getxattr(&self, ino: u64, name: &[u8]) -> Option<Vec<u8>>;
    fn listxattr(&self, ino: u64) -> Vec<Vec<u8>>;

    // ---- writes ------------------------------------------------------
    fn create(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, xattrs: &[(Vec<u8>, Vec<u8>)]);
    fn mkdir(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs);
    fn symlink(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, target: &[u8]);
    fn unlink(&self, parent: u64, name: &[u8]);
    /// Emptiness probe + delete. Returns `false` (no changes made) if
    /// the directory was not empty.
    fn rmdir(&self, parent: u64, name: &[u8]) -> bool;
    fn rename(&self, old_parent: u64, old_name: &[u8], new_parent: u64, new_name: &[u8]);
    fn link(&self, parent: u64, name: &[u8], ino: u64);
    fn setattr(&self, ino: u64, attrs: Attrs);
    fn setxattr(&self, ino: u64, name: &[u8], value: &[u8]);
    fn removexattr(&self, ino: u64, name: &[u8]);

    /// Bootstrap the mount root (ino 1): an inode record with no
    /// dentry pointing at it, matching `meta::sqlite::init`.
    fn init_root(&self, attrs: Attrs);

    // ---- maintenance / introspection ----------------------------------
    /// Commit boundary: for mtree, apply the memtable and roll the WAL;
    /// for SQLite, a WAL checkpoint; for redb/fjall, whatever their own
    /// batching needs. Called by the harness between phases and,
    /// during the mixed workload, on a timer to model group commit.
    fn flush(&self);
    /// Bytes currently on stable storage (space amplification).
    fn disk_bytes(&self) -> u64;
    /// Cumulative bytes this process has written to the engine's files,
    /// best-effort (write amplification). Monotonic.
    fn bytes_written_total(&self) -> u64;

    /// Reclaim garbage from superseded versions, where the engine has
    /// any (mtree: §P10 reachability GC + pack compaction). Returns
    /// `(bytes_before, bytes_after)` when supported, `None` otherwise
    /// (SQLite/redb/fjall reclaim space as part of their own normal
    /// operation — VACUUM, copy-on-write page reuse, LSM compaction —
    /// and are left alone here to keep each engine at its own defaults).
    fn compact(&self) -> Option<(u64, u64)> {
        None
    }

    /// Cold (cache-cleared) leaf/node reads a full readdir of `dir`
    /// costs — a locality proxy for "distinct packs touched" (§14.10):
    /// a directory built in one bulk load should cost close to 1, one
    /// built incrementally over a long aging run should cost more.
    /// Only meaningful for engines with an instrumentable disk-backed
    /// node cache (mtree here); `None` elsewhere.
    fn cold_leaf_reads(&self, _dir: u64, _want: usize) -> Option<usize> {
        None
    }

    /// Compaction-debt snapshot: `(l0_tables, total_tables,
    /// outstanding_flushes, active_compactions, time_compacting_secs)`.
    /// Only meaningful for engines that expose it (fjall3 here);
    /// `None` elsewhere.
    fn compaction_debt(&self) -> Option<(usize, usize, usize, usize, f64)> {
        None
    }
}

/// Streaming percentile tracker without keeping every sample: reservoir
/// of raw nanosecond latencies, since the run sizes here (10^4-10^6 ops
/// per phase) fit comfortably in memory and exact p999 matters more
/// than staying under a tighter memory bound.
#[derive(Default)]
pub struct Latencies {
    pub samples_ns: Vec<u64>,
}

impl Latencies {
    pub fn push(&mut self, ns: u64) {
        self.samples_ns.push(ns);
    }
    pub fn merge(&mut self, other: Latencies) {
        self.samples_ns.extend(other.samples_ns);
    }
    pub fn summary(&mut self) -> LatSummary {
        if self.samples_ns.is_empty() {
            return LatSummary::default();
        }
        self.samples_ns.sort_unstable();
        let n = self.samples_ns.len();
        let at = |q: f64| -> u64 {
            let idx = ((n as f64 - 1.0) * q).round() as usize;
            self.samples_ns[idx.min(n - 1)]
        };
        LatSummary {
            n: n as u64,
            p50_ns: at(0.50),
            p99_ns: at(0.99),
            p999_ns: at(0.999),
            max_ns: *self.samples_ns.last().unwrap(),
            mean_ns: (self.samples_ns.iter().sum::<u64>() as f64 / n as f64) as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct LatSummary {
    pub n: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub mean_ns: u64,
}

impl LatSummary {
    pub fn ops_per_sec_single_thread(&self) -> f64 {
        if self.mean_ns == 0 {
            0.0
        } else {
            1_000_000_000.0 / self.mean_ns as f64
        }
    }
}
