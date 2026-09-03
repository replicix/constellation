//! Embedded-DB bake-off for constellation metadata (DECISIONS.md ADR-9).
//!
//! Loads a real namespace snapshot (`data/corpus-src.db`) into each engine,
//! then runs FUSE-shaped workloads with parallelism and resource counters.

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rayon::prelude::*;
use redb::ReadableTable;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LOOKUPS: u64 = 2_000_000;
const SCANS: u64 = 50_000;
const WRITE_OPS: u64 = 50_000;
const LOAD_BATCH: usize = 5_000;
const MTIME_OFF: usize = 41; // encode_inode layout
/// Enough samples for stable p99.9; every op is timed (no subsampling).
const LATENCY_SAMPLES: u64 = 200_000;
/// Mixed concurrent FUSE-shaped stress.
const MIXED_DURATION: Duration = Duration::from_secs(15);
const WINDOW_MS: u64 = 100;
/// Op mix: lookup / getattr / readdir / setattr / create (weights sum to 100).
const MIX_LOOKUP: u64 = 45;
const MIX_GETATTR: u64 = 30;
const MIX_READDIR: u64 = 15;
const MIX_SETATTR: u64 = 7;
const MIX_CREATE: u64 = 3;
const _: () = assert!(MIX_LOOKUP + MIX_GETATTR + MIX_READDIR + MIX_SETATTR + MIX_CREATE == 100);
/// Synthetic inodes / names for create-shaped multi-key txs (avoid corpus clash).
const SCRATCH_INO_BASE: u64 = 1 << 50;
const SCRATCH_NAME_POOL: u64 = 8_192;
/// Fake journal record payload (~typical Setattr / Create encoding size).
const JOURNAL_REC: [u8; 64] = [0x4A; 64];

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Engine {
    Sqlite,
    #[value(name = "sqlite-mt")]
    SqliteMt,
    Lmdb,
    Rocksdb,
    Redb,
    Fjall,
}

#[derive(Parser, Debug)]
#[command(about = "Constellation metadata DB bake-off on a real namespace corpus")]
struct Args {
    /// Engine to run; omit to run the active shortlist (sqlite, sqlite-mt, lmdb, rocksdb).
    engine: Option<Engine>,
    /// Also run archived engines (redb, fjall) — historical comparison only.
    #[arg(long)]
    include_archived: bool,
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "data/corpus-src.db")]
    source: PathBuf,
    #[arg(long, default_value_t = 8)]
    threads: usize,
}

#[derive(Clone)]
struct Inode {
    ino: u64,
    kind: u8,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    nlink: u32,
    atime_ns: i64,
    mtime_ns: i64,
    ctime_ns: i64,
    rdev: u64,
    manifest: Vec<u8>,
    symlink_target: Option<String>,
}

#[derive(Clone)]
struct Dentry {
    parent: u64,
    name: String,
    ino: u64,
}

struct Dataset {
    inodes: Vec<Inode>,
    dentries: Vec<Dentry>,
    lookup_keys: Vec<(u64, String, u64)>,
    scan_parents: Vec<u64>,
}

impl Dataset {
    fn load(path: &Path) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)
            .with_context(|| format!("open source DB {}", path.display()))?;
        conn.execute_batch("PRAGMA query_only=ON; PRAGMA mmap_size=268435456;")?;

        let mut inodes = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT ino, kind, size, mode, uid, gid, nlink,
                        atime_ns, mtime_ns, ctime_ns, rdev, manifest, symlink_target
                 FROM inode",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok(Inode {
                    ino: r.get::<_, i64>(0)? as u64,
                    kind: r.get::<_, i64>(1)? as u8,
                    size: r.get::<_, i64>(2)? as u64,
                    mode: r.get::<_, i64>(3)? as u32,
                    uid: r.get::<_, i64>(4)? as u32,
                    gid: r.get::<_, i64>(5)? as u32,
                    nlink: r.get::<_, i64>(6)? as u32,
                    atime_ns: r.get(7)?,
                    mtime_ns: r.get(8)?,
                    ctime_ns: r.get(9)?,
                    rdev: r.get::<_, i64>(10)? as u64,
                    manifest: r.get::<_, Option<Vec<u8>>>(11)?.unwrap_or_default(),
                    symlink_target: r.get(12)?,
                })
            })?;
            for row in rows {
                inodes.push(row?);
            }
        }

        let mut dentries = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT parent, name, ino FROM dentry")?;
            let rows = stmt.query_map([], |r| {
                Ok(Dentry {
                    parent: r.get::<_, i64>(0)? as u64,
                    name: r.get(1)?,
                    ino: r.get::<_, i64>(2)? as u64,
                })
            })?;
            for row in rows {
                dentries.push(row?);
            }
        }
        if inodes.is_empty() || dentries.is_empty() {
            bail!("source DB is empty: {}", path.display());
        }

        let mut lookup_keys = Vec::with_capacity(LOOKUPS as usize);
        for i in 0..LOOKUPS {
            let d = &dentries[mix(i) as usize % dentries.len()];
            lookup_keys.push((d.parent, d.name.clone(), d.ino));
        }

        let mut counts: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        for d in &dentries {
            *counts.entry(d.parent).or_default() += 1;
        }
        let mut parents: Vec<(u64, u32)> = counts.into_iter().collect();
        parents.sort_by(|a, b| b.1.cmp(&a.1));
        let dense: Vec<u64> = parents.iter().take(2_000).map(|(p, _)| *p).collect();
        let mut scan_parents = Vec::with_capacity(SCANS as usize);
        for i in 0..SCANS {
            scan_parents.push(dense[mix(i ^ 0xA5A5) as usize % dense.len()]);
        }

        Ok(Self {
            inodes,
            dentries,
            lookup_keys,
            scan_parents,
        })
    }

    fn bytes_logical(&self) -> u64 {
        let inode_bytes: u64 = self
            .inodes
            .iter()
            .map(|i| {
                64 + i.manifest.len() as u64
                    + i.symlink_target.as_ref().map(|s| s.len() as u64).unwrap_or(0)
            })
            .sum();
        let dentry_bytes: u64 = self
            .dentries
            .iter()
            .map(|d| 16 + d.name.len() as u64)
            .sum();
        inode_bytes + dentry_bytes
    }
}

#[inline]
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn encode_inode(i: &Inode) -> Vec<u8> {
    let target = i.symlink_target.as_deref().unwrap_or("");
    let mut v = Vec::with_capacity(64 + i.manifest.len() + target.len());
    v.extend_from_slice(&i.ino.to_le_bytes());
    v.push(i.kind);
    v.extend_from_slice(&i.size.to_le_bytes());
    v.extend_from_slice(&i.mode.to_le_bytes());
    v.extend_from_slice(&i.uid.to_le_bytes());
    v.extend_from_slice(&i.gid.to_le_bytes());
    v.extend_from_slice(&i.nlink.to_le_bytes());
    v.extend_from_slice(&i.atime_ns.to_le_bytes());
    debug_assert_eq!(v.len(), MTIME_OFF);
    v.extend_from_slice(&i.mtime_ns.to_le_bytes());
    v.extend_from_slice(&i.ctime_ns.to_le_bytes());
    v.extend_from_slice(&i.rdev.to_le_bytes());
    v.extend_from_slice(&(i.manifest.len() as u32).to_le_bytes());
    v.extend_from_slice(&i.manifest);
    v.extend_from_slice(&(target.len() as u32).to_le_bytes());
    v.extend_from_slice(target.as_bytes());
    v
}

fn patch_mtime(bytes: &mut [u8], mtime_ns: i64) {
    if bytes.len() >= MTIME_OFF + 8 {
        bytes[MTIME_OFF..MTIME_OFF + 8].copy_from_slice(&mtime_ns.to_le_bytes());
    }
}

fn dentry_key(parent: u64, name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + name.len());
    k.extend_from_slice(&parent.to_be_bytes());
    k.extend_from_slice(name.as_bytes());
    k
}

fn inode_key(ino: u64) -> [u8; 8] {
    ino.to_be_bytes()
}

fn dir_size(path: &Path) -> u64 {
    fn walk(p: &Path, acc: &mut u64) {
        let Ok(meta) = std::fs::metadata(p) else {
            return;
        };
        if meta.is_file() {
            *acc += meta.len();
            return;
        }
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                walk(&e.path(), acc);
            }
        }
    }
    let mut n = 0u64;
    walk(path, &mut n);
    n
}

#[derive(Clone, Debug, Default)]
struct Usage {
    wall: Duration,
    user: Duration,
    sys: Duration,
    max_rss_kb: u64,
    minflt: u64,
    majflt: u64,
    nvcsw: u64,
    nivcsw: u64,
    cycles: u64,
    instructions: Option<u64>,
}

struct PhaseTimer {
    wall0: Instant,
    usage0: libc::rusage,
    tsc0: u64,
    insn_fd: Option<i32>,
}

impl PhaseTimer {
    fn start() -> Self {
        let insn_fd = open_insn_counter();
        if let Some(fd) = insn_fd {
            unsafe {
                libc::ioctl(fd, 0x2403, 0); // RESET
                libc::ioctl(fd, 0x2400, 0); // ENABLE
            }
        }
        Self {
            wall0: Instant::now(),
            usage0: getrusage(),
            tsc0: rdtsc(),
            insn_fd,
        }
    }

    fn stop(self) -> Usage {
        let wall = self.wall0.elapsed();
        let usage1 = getrusage();
        let tsc1 = rdtsc();
        let instructions = self.insn_fd.and_then(|fd| {
            unsafe {
                libc::ioctl(fd, 0x2401, 0);
            }
            let mut buf = [0u8; 8];
            let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), 8) };
            unsafe {
                libc::close(fd);
            }
            (n == 8).then_some(u64::from_ne_bytes(buf))
        });
        Usage {
            wall,
            user: timeval_sub(usage1.ru_utime, self.usage0.ru_utime),
            sys: timeval_sub(usage1.ru_stime, self.usage0.ru_stime),
            max_rss_kb: usage1.ru_maxrss as u64,
            minflt: (usage1.ru_minflt - self.usage0.ru_minflt) as u64,
            majflt: (usage1.ru_majflt - self.usage0.ru_majflt) as u64,
            nvcsw: (usage1.ru_nvcsw - self.usage0.ru_nvcsw) as u64,
            nivcsw: (usage1.ru_nivcsw - self.usage0.ru_nivcsw) as u64,
            cycles: tsc1.saturating_sub(self.tsc0),
            instructions,
        }
    }
}

fn open_insn_counter() -> Option<i32> {
    #[repr(C)]
    struct Attr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        bp_addr: u64,
        bp_len: u64,
        branch_sample_type: u64,
        sample_regs_user: u64,
        sample_stack_user: u32,
        clockid: i32,
        sample_regs_intr: u64,
        aux_watermark: u32,
        sample_max_stack: u16,
        __reserved_2: u16,
        aux_sample_size: u32,
        __reserved_3: u32,
    }
    let mut attr = unsafe { std::mem::zeroed::<Attr>() };
    attr.type_ = 0;
    attr.size = std::mem::size_of::<Attr>() as u32;
    attr.config = 1;
    attr.flags = 0b11;
    let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr, 0, -1, -1, 0i64) };
    if fd < 0 {
        None
    } else {
        Some(fd as i32)
    }
}

fn insn_available() -> bool {
    open_insn_counter().map(|fd| unsafe { libc::close(fd) }).is_some()
}

fn getrusage() -> libc::rusage {
    let mut u = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
    }
    u
}

fn timeval_sub(a: libc::timeval, b: libc::timeval) -> Duration {
    let a_us = a.tv_sec as i128 * 1_000_000 + a.tv_usec as i128;
    let b_us = b.tv_sec as i128 * 1_000_000 + b.tv_usec as i128;
    Duration::from_micros(a_us.saturating_sub(b_us).max(0) as u64)
}

#[inline]
fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[derive(Default)]
struct Results {
    load: Usage,
    lookup: Usage,
    lookup_mt: Usage,
    scan: Usage,
    getattr: Usage,
    write: Usage,
    create: Usage,
    store_bytes: u64,
    threads: usize,
    /// Per-op latency for isolated phases (ns).
    lookup_lat: LatencySummary,
    getattr_lat: LatencySummary,
    readdir_lat: LatencySummary,
    setattr_lat: LatencySummary,
    create_lat: LatencySummary,
    /// Concurrent mixed workload.
    mixed: Option<MixedSummary>,
}

#[derive(Clone, Debug, Default)]
struct LatencySummary {
    n: u64,
    mean_ns: f64,
    p50_ns: u64,
    p90_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

impl LatencySummary {
    fn from_samples(mut ns: Vec<u64>) -> Self {
        if ns.is_empty() {
            return Self::default();
        }
        ns.sort_unstable();
        let n = ns.len() as u64;
        let sum: u128 = ns.iter().map(|&x| x as u128).sum();
        let pct = |p: f64| -> u64 {
            let idx = ((p * (n as f64 - 1.0)).round() as usize).min(ns.len() - 1);
            ns[idx]
        };
        Self {
            n,
            mean_ns: sum as f64 / n as f64,
            p50_ns: pct(0.50),
            p90_ns: pct(0.90),
            p95_ns: pct(0.95),
            p99_ns: pct(0.99),
            p999_ns: pct(0.999),
            max_ns: *ns.last().unwrap(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MixedSummary {
    #[allow(dead_code)]
    ops: u64,
    ops_s: f64,
    wall_s: f64,
    by_kind: [u64; 5], // lookup, getattr, readdir, setattr, create
    latency: LatencySummary,
    /// Per-window throughput (ops in WINDOW_MS).
    window_ops: Vec<u64>,
    window_median: f64,
    window_p10: f64,
    window_min: f64,
    stall_windows: u64,
    stall_threshold: f64,
}

impl MixedSummary {
    fn from_parts(
        ops: u64,
        wall: Duration,
        by_kind: [u64; 5],
        lat_ns: Vec<u64>,
        window_ops: Vec<u64>,
    ) -> Self {
        let wall_s = wall.as_secs_f64().max(1e-9);
        let latency = LatencySummary::from_samples(lat_ns);
        let mut sorted = window_ops.clone();
        sorted.sort_unstable();
        let wlen = sorted.len().max(1);
        let window_median = sorted[wlen / 2] as f64 * (1000.0 / WINDOW_MS as f64);
        let window_p10 = sorted[wlen / 10] as f64 * (1000.0 / WINDOW_MS as f64);
        let window_min = sorted.first().copied().unwrap_or(0) as f64 * (1000.0 / WINDOW_MS as f64);
        // Robust stall: window rate < median - 3 * (1.4826 * MAD).
        let rates: Vec<f64> = window_ops
            .iter()
            .map(|&c| c as f64 * (1000.0 / WINDOW_MS as f64))
            .collect();
        let med = window_median;
        let mut abs_dev: Vec<f64> = rates.iter().map(|r| (r - med).abs()).collect();
        abs_dev.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mad = abs_dev[abs_dev.len() / 2];
        let sigma = 1.4826 * mad;
        // Stall = robust z < -3, but never softer than half the median
        // (when MAD is large, med−3σ collapses to 0 and hides real dips).
        let stall_threshold = (med - 3.0 * sigma).max(med * 0.5);
        let stall_windows = rates.iter().filter(|&&r| r < stall_threshold).count() as u64;
        Self {
            ops,
            ops_s: ops as f64 / wall_s,
            wall_s,
            by_kind,
            latency,
            window_ops,
            window_median,
            window_p10,
            window_min,
            stall_windows,
            stall_threshold,
        }
    }
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

fn report(engine: &str, ds: &Dataset, r: &Results) {
    let insn = |u: &Usage| {
        u.instructions
            .map(|i| i.to_string())
            .unwrap_or_else(|| "n/a".into())
    };
    let rate = |ops: u64, u: &Usage| {
        if u.wall.as_secs_f64() > 0.0 {
            ops as f64 / u.wall.as_secs_f64()
        } else {
            0.0
        }
    };
    let per = |ops: u64, v: u64| {
        if ops > 0 {
            format!("{:.0}", v as f64 / ops as f64)
        } else {
            "n/a".into()
        }
    };
    let per_insn = |ops: u64, u: &Usage| match u.instructions {
        Some(i) => per(ops, i),
        None => "n/a".into(),
    };

    let n_rows = (ds.inodes.len() + ds.dentries.len()) as u64;
    println!("\n======== {engine} ========");
    println!(
        "dataset: {} inodes, {} dentries, {:.1} MiB logical",
        ds.inodes.len(),
        ds.dentries.len(),
        ds.bytes_logical() as f64 / (1024.0 * 1024.0)
    );
    println!(
        "store:   {:.1} MiB on disk ({:.2}x logical)",
        r.store_bytes as f64 / (1024.0 * 1024.0),
        r.store_bytes as f64 / ds.bytes_logical().max(1) as f64
    );
    println!(
        "{:<14} {:>8} {:>10} {:>8} {:>8} {:>12} {:>12} {:>8} {:>8} {:>8}",
        "phase", "ops", "ops/s", "wall_s", "cpu_s", "cycles", "insns", "cyc/op", "insn/op", "RSS_MB"
    );
    let row = |name: &str, ops: u64, u: &Usage| {
        println!(
            "{:<14} {:>8} {:>10.0} {:>8.3} {:>8.3} {:>12} {:>12} {:>8} {:>8} {:>8.1}",
            name,
            ops,
            rate(ops, u),
            u.wall.as_secs_f64(),
            u.user.as_secs_f64() + u.sys.as_secs_f64(),
            u.cycles,
            insn(u),
            per(ops, u.cycles),
            per_insn(ops, u),
            u.max_rss_kb as f64 / 1024.0
        );
    };
    row("load", n_rows, &r.load);
    row("lookup", LOOKUPS, &r.lookup);
    row(&format!("lookup/{}", r.threads), LOOKUPS, &r.lookup_mt);
    row("readdir", SCANS, &r.scan);
    row("getattr", LOOKUPS, &r.getattr);
    row("setattr", WRITE_OPS, &r.write);
    row("create", WRITE_OPS, &r.create);

    println!(
        "\n{:<10} {:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "latency", "n", "mean", "p50", "p90", "p95", "p99", "p99.9", "max"
    );
    let lat_row = |name: &str, s: &LatencySummary| {
        if s.n == 0 {
            return;
        }
        println!(
            "{:<10} {:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            name,
            s.n,
            fmt_ns(s.mean_ns as u64),
            fmt_ns(s.p50_ns),
            fmt_ns(s.p90_ns),
            fmt_ns(s.p95_ns),
            fmt_ns(s.p99_ns),
            fmt_ns(s.p999_ns),
            fmt_ns(s.max_ns),
        );
    };
    lat_row("lookup", &r.lookup_lat);
    lat_row("getattr", &r.getattr_lat);
    lat_row("readdir", &r.readdir_lat);
    lat_row("setattr", &r.setattr_lat);
    lat_row("create", &r.create_lat);

    if let Some(m) = &r.mixed {
        println!(
            "\nmixed/{}: {:.0} ops/s over {:.1}s  \
             (lookup={} getattr={} readdir={} setattr={} create={})",
            r.threads,
            m.ops_s,
            m.wall_s,
            m.by_kind[0],
            m.by_kind[1],
            m.by_kind[2],
            m.by_kind[3],
            m.by_kind[4],
        );
        lat_row("mixed", &m.latency);
        println!(
            "windows ({WINDOW_MS}ms): median={:.0} ops/s  p10={:.0}  min={:.0}  \
             stalls={}/{} (rate < {:.0} ops/s)",
            m.window_median,
            m.window_p10,
            m.window_min,
            m.stall_windows,
            m.window_ops.len(),
            m.stall_threshold,
        );
        // Compact sparkline of window rates relative to median / stall line.
        let spark: String = m
            .window_ops
            .iter()
            .map(|&c| {
                let rate = c as f64 * (1000.0 / WINDOW_MS as f64);
                if rate < m.stall_threshold {
                    '_'
                } else {
                    let rel = if m.window_median > 0.0 {
                        rate / m.window_median
                    } else {
                        1.0
                    };
                    if rel < 0.7 {
                        '.'
                    } else if rel < 0.9 {
                        'o'
                    } else if rel < 1.15 {
                        '#'
                    } else {
                        '^'
                    }
                }
            })
            .collect();
        println!("window spark (_stall .low o mid #med ^high): {spark}");
    }

    println!(
        "faults: lookup min/maj={}/{} csw={}/{} | mt min/maj={}/{} csw={}/{}",
        r.lookup.minflt,
        r.lookup.majflt,
        r.lookup.nvcsw,
        r.lookup.nivcsw,
        r.lookup_mt.minflt,
        r.lookup_mt.majflt,
        r.lookup_mt.nvcsw,
        r.lookup_mt.nivcsw
    );
    let mixed_s = r.mixed.as_ref();
    println!(
        "RESULT engine={engine} store_mib={:.1} load_s={:.3} \
         lookup_ops_s={:.0} lookup_{t}_ops_s={:.0} readdir_dirs_s={:.0} \
         getattr_ops_s={:.0} setattr_ops_s={:.0} \
         lookup_p50_ns={} lookup_p99_ns={} lookup_p999_ns={} lookup_max_ns={} \
         mixed_ops_s={:.0} mixed_p50_ns={} mixed_p99_ns={} mixed_p999_ns={} mixed_max_ns={} \
         mixed_stalls={} mixed_win_min={:.0} mixed_win_med={:.0} rss_mb={:.1}",
        r.store_bytes as f64 / (1024.0 * 1024.0),
        r.load.wall.as_secs_f64(),
        rate(LOOKUPS, &r.lookup),
        rate(LOOKUPS, &r.lookup_mt),
        rate(SCANS, &r.scan),
        rate(LOOKUPS, &r.getattr),
        rate(WRITE_OPS, &r.write),
        r.lookup_lat.p50_ns,
        r.lookup_lat.p99_ns,
        r.lookup_lat.p999_ns,
        r.lookup_lat.max_ns,
        mixed_s.map(|m| m.ops_s).unwrap_or(0.0),
        mixed_s.map(|m| m.latency.p50_ns).unwrap_or(0),
        mixed_s.map(|m| m.latency.p99_ns).unwrap_or(0),
        mixed_s.map(|m| m.latency.p999_ns).unwrap_or(0),
        mixed_s.map(|m| m.latency.max_ns).unwrap_or(0),
        mixed_s.map(|m| m.stall_windows).unwrap_or(0),
        mixed_s.map(|m| m.window_min).unwrap_or(0.0),
        mixed_s.map(|m| m.window_median).unwrap_or(0.0),
        r.lookup.max_rss_kb as f64 / 1024.0,
        t = r.threads,
    );
}

/// Minimal journal row inserted inside every write tx, mirroring production behaviour
/// where every namespace mutation journals the corresponding log record in the same
/// transaction as the namespace change (crates/meta/src/sqlite.rs comment line 4).
const JOURNAL_KEY: &[u8] = b"journal";

trait Reader: Send + Sync {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>>;
    fn getattr(&self, ino: u64) -> Result<Option<usize>>;
    fn readdir_count(&self, parent: u64) -> Result<usize>;
}

/// All write ops must:
/// 1. Mutate inode AND dentry (or inode only for setattr) atomically in one tx.
/// 2. Insert one `JOURNAL_REC`-sized journal row in the same tx.
/// This mirrors the real `SqliteMeta` contract: every mutation journals its log
/// record in the same transaction (crates/meta/src/lib.rs §journal).
trait Writer: Send {
    /// setattr: update one inode + append journal row (one tx).
    fn setattr_mtime(&mut self, ino: u64, mtime_ns: i64) -> Result<()>;
    /// create: insert inode + dentry + journal row (one tx).
    fn create_file(
        &mut self,
        parent: u64,
        name: &str,
        ino: u64,
        mtime_ns: i64,
    ) -> Result<()>;
}

fn time_op_ns(f: impl FnOnce() -> Result<()>) -> Result<u64> {
    let t0 = Instant::now();
    f()?;
    Ok(t0.elapsed().as_nanos() as u64)
}

fn run_phases(
    name: &str,
    ds: &Dataset,
    threads: usize,
    store_path: &Path,
    mut load: impl FnMut() -> Result<()>,
    reader: Arc<dyn Reader>,
    writer: Box<dyn Writer>,
) -> Result<()> {
    let mut r = Results {
        threads,
        ..Default::default()
    };
    let writer = Arc::new(Mutex::new(writer));

    let t = PhaseTimer::start();
    load()?;
    r.load = t.stop();
    r.store_bytes = dir_size(store_path);

    {
        let t = PhaseTimer::start();
        let mut hits = 0u64;
        for (p, n, e) in &ds.lookup_keys {
            if reader.lookup(*p, n)?.unwrap_or(0) == *e {
                hits += 1;
            }
        }
        r.lookup = t.stop();
        assert!(hits > LOOKUPS / 2, "{name}: low lookup hits {hits}");
    }

    {
        let hits = AtomicU64::new(0);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()?;
        let t = PhaseTimer::start();
        pool.install(|| {
            ds.lookup_keys.par_iter().for_each(|(p, n, e)| {
                if let Ok(Some(got)) = reader.lookup(*p, n) {
                    if got == *e {
                        hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        });
        r.lookup_mt = t.stop();
        assert!(
            hits.load(Ordering::Relaxed) > LOOKUPS / 2,
            "{name}: low mt lookup hits"
        );
    }

    {
        let t = PhaseTimer::start();
        let mut kids = 0usize;
        for p in &ds.scan_parents {
            kids += reader.readdir_count(*p)?;
        }
        r.scan = t.stop();
        assert!(kids > 0);
    }

    {
        let t = PhaseTimer::start();
        let mut bytes = 0usize;
        for i in 0..LOOKUPS {
            let ino = ds.inodes[mix(i ^ 0xC0FFEE) as usize % ds.inodes.len()].ino;
            bytes += reader.getattr(ino)?.unwrap_or(0);
        }
        r.getattr = t.stop();
        assert!(bytes > 0);
    }

    {
        let t = PhaseTimer::start();
        let mut w = writer.lock().unwrap();
        for i in 0..WRITE_OPS {
            let ino = ds.inodes[mix(i ^ 0xBEEF) as usize % ds.inodes.len()].ino;
            w.setattr_mtime(ino, 1_700_000_000_000_000_000 + i as i64)?;
        }
        r.write = t.stop();
    }

    // ---- Isolated create (multi-key tx + journal row) ----
    {
        let t = PhaseTimer::start();
        let nparents = ds.scan_parents.len().max(1);
        let mut w = writer.lock().unwrap();
        for i in 0..WRITE_OPS {
            let ino = SCRATCH_INO_BASE + i;
            let parent = ds.scan_parents[mix(i ^ 0xFACE) as usize % nparents];
            let name = format!(".bench_{i:08x}");
            w.create_file(parent, &name, ino, 1_700_000_000_000_000_000 + i as i64)?;
        }
        r.create = t.stop();
    }

    // ---- Isolated latency distributions (every sample timed) ----
    {
        let n = LATENCY_SAMPLES.min(ds.lookup_keys.len() as u64);
        let mut samples = Vec::with_capacity(n as usize);
        for i in 0..n {
            let (p, name, _) = &ds.lookup_keys[i as usize];
            let ns = time_op_ns(|| {
                reader.lookup(*p, name)?;
                Ok(())
            })?;
            samples.push(ns);
        }
        r.lookup_lat = LatencySummary::from_samples(samples);
    }
    {
        let mut samples = Vec::with_capacity(LATENCY_SAMPLES as usize);
        for i in 0..LATENCY_SAMPLES {
            let ino = ds.inodes[mix(i ^ 0xC0FFEE) as usize % ds.inodes.len()].ino;
            let ns = time_op_ns(|| {
                reader.getattr(ino)?;
                Ok(())
            })?;
            samples.push(ns);
        }
        r.getattr_lat = LatencySummary::from_samples(samples);
    }
    {
        // Repeat denser-parent list to fill LATENCY_SAMPLES.
        let mut samples = Vec::with_capacity(LATENCY_SAMPLES as usize);
        for i in 0..LATENCY_SAMPLES {
            let p = ds.scan_parents[i as usize % ds.scan_parents.len()];
            let ns = time_op_ns(|| {
                reader.readdir_count(p)?;
                Ok(())
            })?;
            samples.push(ns);
        }
        r.readdir_lat = LatencySummary::from_samples(samples);
    }
    {
        let mut samples = Vec::with_capacity(LATENCY_SAMPLES as usize);
        let mut w = writer.lock().unwrap();
        for i in 0..LATENCY_SAMPLES {
            let ino = ds.inodes[mix(i ^ 0xDEAD) as usize % ds.inodes.len()].ino;
            let ns = time_op_ns(|| {
                w.setattr_mtime(ino, 1_710_000_000_000_000_000 + i as i64)?;
                Ok(())
            })?;
            samples.push(ns);
        }
        r.setattr_lat = LatencySummary::from_samples(samples);
    }
    {
        // Pre-compute dir-ino list once; reuse across latency samples.
        let ndirs = ds.scan_parents.len().max(1);
        let mut samples = Vec::with_capacity(LATENCY_SAMPLES as usize);
        let mut w = writer.lock().unwrap();
        for i in 0..LATENCY_SAMPLES {
            let parent = ds.scan_parents[mix(i ^ 0xCAFE) as usize % ndirs];
            let ino = SCRATCH_INO_BASE + WRITE_OPS + i;
            let name = format!(".blat_{i:010x}");
            let ns = time_op_ns(|| {
                w.create_file(parent, &name, ino, 1_720_000_000_000_000_000 + i as i64)?;
                Ok(())
            })?;
            samples.push(ns);
        }
        r.create_lat = LatencySummary::from_samples(samples);
    }

    // ---- Concurrent mixed workload (reads + writes, windowed stall detect) ----
    {
        let n_windows =
            ((MIXED_DURATION.as_millis() as u64 / WINDOW_MS) + 4) as usize;
        let windows: Arc<Vec<AtomicU64>> =
            Arc::new((0..n_windows).map(|_| AtomicU64::new(0)).collect());
        let kind_counts = Arc::new([
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        ]);
        let lat_bags: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));
        // Pre-compute dir-ino list for the create arm.
        let dir_inos: Arc<Vec<u64>> = Arc::new(ds.scan_parents.clone());

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()?;
        let wall_start = Instant::now();
        let deadline = wall_start + MIXED_DURATION;
        pool.install(|| {
            (0..threads).into_par_iter().for_each(|tid| {
                let ndirs = dir_inos.len().max(1);
                let mut local_lat = Vec::with_capacity(64_000);
                let mut local_kind = [0u64; 5];
                let mut i = tid as u64;
                while Instant::now() < deadline {
                    let roll = mix(i ^ 0xA5A5_5A5A) % 100;
                    let t0 = Instant::now();
                    let kind = if roll < MIX_LOOKUP {
                        let (p, n, _) =
                            &ds.lookup_keys[mix(i) as usize % ds.lookup_keys.len()];
                        let _ = reader.lookup(*p, n);
                        0
                    } else if roll < MIX_LOOKUP + MIX_GETATTR {
                        let ino =
                            ds.inodes[mix(i ^ 1) as usize % ds.inodes.len()].ino;
                        let _ = reader.getattr(ino);
                        1
                    } else if roll < MIX_LOOKUP + MIX_GETATTR + MIX_READDIR {
                        let p = ds.scan_parents
                            [mix(i ^ 2) as usize % ds.scan_parents.len()];
                        let _ = reader.readdir_count(p);
                        2
                    } else if roll < MIX_LOOKUP + MIX_GETATTR + MIX_READDIR + MIX_SETATTR {
                        let ino =
                            ds.inodes[mix(i ^ 3) as usize % ds.inodes.len()].ino;
                        if let Ok(mut w) = writer.lock() {
                            let _ = w.setattr_mtime(
                                ino,
                                1_720_000_000_000_000_000 + i as i64,
                            );
                        }
                        3
                    } else {
                        // create: insert new ino+dentry+journal in one tx.
                        let parent = dir_inos[mix(i ^ 4) as usize % ndirs];
                        let ino = SCRATCH_INO_BASE
                            + (i % (SCRATCH_NAME_POOL * threads as u64));
                        let name = format!(
                            ".mx_{:04x}_{:08x}",
                            tid,
                            i % SCRATCH_NAME_POOL
                        );
                        if let Ok(mut w) = writer.lock() {
                            let _ = w.create_file(
                                parent,
                                &name,
                                ino,
                                1_730_000_000_000_000_000 + i as i64,
                            );
                        }
                        4
                    };
                    let ns = t0.elapsed().as_nanos() as u64;
                    local_lat.push(ns);
                    local_kind[kind] += 1;
                    let widx = (wall_start.elapsed().as_millis() as u64 / WINDOW_MS) as usize;
                    if widx < windows.len() {
                        windows[widx].fetch_add(1, Ordering::Relaxed);
                    }
                    i = i.wrapping_add(threads as u64);
                }
                for (k, c) in local_kind.iter().enumerate() {
                    kind_counts[k].fetch_add(*c, Ordering::Relaxed);
                }
                lat_bags.lock().unwrap().push(local_lat);
            });
        });
        let wall = wall_start.elapsed();

        let mut lat_ns = Vec::new();
        for bag in lat_bags.lock().unwrap().drain(..) {
            lat_ns.extend(bag);
        }
        let by_kind = [
            kind_counts[0].load(Ordering::Relaxed),
            kind_counts[1].load(Ordering::Relaxed),
            kind_counts[2].load(Ordering::Relaxed),
            kind_counts[3].load(Ordering::Relaxed),
            kind_counts[4].load(Ordering::Relaxed),
        ];
        let ops = by_kind.iter().sum::<u64>();
        // Drop leading/trailing empty windows (ramp / early stop).
        let mut window_ops: Vec<u64> = windows
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect();
        while window_ops.first().copied() == Some(0) {
            window_ops.remove(0);
        }
        while window_ops.last().copied() == Some(0) {
            window_ops.pop();
        }
        // Also drop a partial last window if it is much shorter than median.
        if window_ops.len() >= 3 {
            let mut tmp = window_ops.clone();
            tmp.sort_unstable();
            let med = tmp[tmp.len() / 2];
            if let Some(last) = window_ops.last().copied() {
                if last < med / 4 {
                    window_ops.pop();
                }
            }
        }

        r.mixed = Some(MixedSummary::from_parts(
            ops, wall, by_kind, lat_ns, window_ops,
        ));
    }

    r.store_bytes = dir_size(store_path);
    report(name, ds, &r);
    Ok(())
}

// ---- SQLite ----

fn sqlite_connect(path: &Path) -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA cache_size=-524288;
         PRAGMA mmap_size=268435456;
         PRAGMA temp_store=MEMORY;",
    )?;
    Ok(conn)
}

fn sqlite_schema(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE inode (
            ino INTEGER PRIMARY KEY,
            kind INTEGER NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            mode INTEGER NOT NULL,
            uid INTEGER NOT NULL,
            gid INTEGER NOT NULL,
            nlink INTEGER NOT NULL,
            atime_ns INTEGER NOT NULL DEFAULT 0,
            mtime_ns INTEGER NOT NULL,
            ctime_ns INTEGER NOT NULL,
            rdev INTEGER NOT NULL DEFAULT 0,
            manifest BLOB,
            symlink_target TEXT
         );
         CREATE TABLE dentry (
            parent INTEGER NOT NULL,
            name TEXT NOT NULL,
            ino INTEGER NOT NULL,
            PRIMARY KEY (parent, name)
         ) WITHOUT ROWID;
         CREATE INDEX dentry_by_ino ON dentry (ino);
         CREATE TABLE journal (
            seq   INTEGER PRIMARY KEY AUTOINCREMENT,
            record BLOB NOT NULL
         );",
    )?;
    Ok(())
}

fn sqlite_load(conn: &mut rusqlite::Connection, ds: &Dataset) -> Result<()> {
    for chunk in ds.inodes.chunks(LOAD_BATCH) {
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO inode (ino,kind,size,mode,uid,gid,nlink,atime_ns,mtime_ns,ctime_ns,rdev,manifest,symlink_target)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            )?;
            for i in chunk {
                stmt.execute(rusqlite::params![
                    i.ino as i64,
                    i.kind as i64,
                    i.size as i64,
                    i.mode as i64,
                    i.uid as i64,
                    i.gid as i64,
                    i.nlink as i64,
                    i.atime_ns,
                    i.mtime_ns,
                    i.ctime_ns,
                    i.rdev as i64,
                    if i.manifest.is_empty() {
                        None
                    } else {
                        Some(&i.manifest[..])
                    },
                    &i.symlink_target,
                ])?;
            }
        }
        tx.commit()?;
    }
    for chunk in ds.dentries.chunks(LOAD_BATCH) {
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt =
                tx.prepare_cached("INSERT INTO dentry (parent, name, ino) VALUES (?1,?2,?3)")?;
            for d in chunk {
                stmt.execute(rusqlite::params![d.parent as i64, d.name, d.ino as i64])?;
            }
        }
        tx.commit()?;
    }
    Ok(())
}

trait OptionalExt<T> {
    fn optional(self) -> rusqlite::Result<Option<T>>;
}
impl<T> OptionalExt<T> for rusqlite::Result<T> {
    fn optional(self) -> rusqlite::Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

struct SqliteMutexReader(Mutex<rusqlite::Connection>);
impl Reader for SqliteMutexReader {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare_cached("SELECT ino FROM dentry WHERE parent=?1 AND name=?2")?;
        Ok(stmt
            .query_row(rusqlite::params![parent as i64, name], |r| r.get::<_, i64>(0))
            .optional()?
            .map(|v| v as u64))
    }
    fn getattr(&self, ino: u64) -> Result<Option<usize>> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT length(COALESCE(manifest, X'')) + length(COALESCE(symlink_target,''))
             FROM inode WHERE ino=?1",
        )?;
        Ok(stmt
            .query_row([ino as i64], |r| r.get::<_, i64>(0))
            .optional()?
            .map(|v| v as usize))
    }
    fn readdir_count(&self, parent: u64) -> Result<usize> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare_cached("SELECT count(*) FROM dentry WHERE parent=?1")?;
        Ok(stmt.query_row([parent as i64], |r| r.get::<_, i64>(0))? as usize)
    }
}

struct SqlitePathReader(PathBuf);
impl Reader for SqlitePathReader {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        thread_local! {
            static CONN: std::cell::RefCell<Option<(PathBuf, rusqlite::Connection)>> =
                const { std::cell::RefCell::new(None) };
        }
        CONN.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != &self.0,
                None => true,
            };
            if need_open {
                *slot = Some((self.0.clone(), sqlite_connect(&self.0)?));
            }
            let conn = &slot.as_ref().unwrap().1;
            let mut stmt =
                conn.prepare_cached("SELECT ino FROM dentry WHERE parent=?1 AND name=?2")?;
            Ok(stmt
                .query_row(rusqlite::params![parent as i64, name], |r| r.get::<_, i64>(0))
                .optional()?
                .map(|v| v as u64))
        })
    }
    fn getattr(&self, ino: u64) -> Result<Option<usize>> {
        thread_local! {
            static CONN: std::cell::RefCell<Option<(PathBuf, rusqlite::Connection)>> =
                const { std::cell::RefCell::new(None) };
        }
        CONN.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != &self.0,
                None => true,
            };
            if need_open {
                *slot = Some((self.0.clone(), sqlite_connect(&self.0)?));
            }
            let conn = &slot.as_ref().unwrap().1;
            let mut stmt = conn.prepare_cached(
                "SELECT length(COALESCE(manifest, X'')) + length(COALESCE(symlink_target,''))
                 FROM inode WHERE ino=?1",
            )?;
            Ok(stmt
                .query_row([ino as i64], |r| r.get::<_, i64>(0))
                .optional()?
                .map(|v| v as usize))
        })
    }
    fn readdir_count(&self, parent: u64) -> Result<usize> {
        thread_local! {
            static CONN: std::cell::RefCell<Option<(PathBuf, rusqlite::Connection)>> =
                const { std::cell::RefCell::new(None) };
        }
        CONN.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != &self.0,
                None => true,
            };
            if need_open {
                *slot = Some((self.0.clone(), sqlite_connect(&self.0)?));
            }
            let conn = &slot.as_ref().unwrap().1;
            let mut stmt = conn.prepare_cached("SELECT count(*) FROM dentry WHERE parent=?1")?;
            Ok(stmt.query_row([parent as i64], |r| r.get::<_, i64>(0))? as usize)
        })
    }
}

struct SqlitePathWriter(PathBuf);
impl Writer for SqlitePathWriter {
    fn setattr_mtime(&mut self, ino: u64, mtime_ns: i64) -> Result<()> {
        thread_local! {
            static CONN: std::cell::RefCell<Option<(PathBuf, rusqlite::Connection)>> =
                const { std::cell::RefCell::new(None) };
        }
        CONN.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != &self.0,
                None => true,
            };
            if need_open {
                *slot = Some((self.0.clone(), sqlite_connect(&self.0)?));
            }
            let conn = &mut slot.as_mut().unwrap().1;
            // Immediate tx: matches production SqliteMeta::setattr.
            let tx = conn.transaction_with_behavior(
                rusqlite::TransactionBehavior::Immediate,
            )?;
            tx.execute(
                "UPDATE inode SET mtime_ns=?2, ctime_ns=?2 WHERE ino=?1",
                rusqlite::params![ino as i64, mtime_ns],
            )?;
            // Inline journal row (same tx) — mirrors production behaviour.
            tx.execute(
                "INSERT INTO journal (record) VALUES (?1)",
                rusqlite::params![&JOURNAL_REC[..]],
            )?;
            tx.commit()?;
            Ok(())
        })
    }

    fn create_file(
        &mut self,
        parent: u64,
        name: &str,
        ino: u64,
        mtime_ns: i64,
    ) -> Result<()> {
        thread_local! {
            static CONN: std::cell::RefCell<Option<(PathBuf, rusqlite::Connection)>> =
                const { std::cell::RefCell::new(None) };
        }
        CONN.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != &self.0,
                None => true,
            };
            if need_open {
                *slot = Some((self.0.clone(), sqlite_connect(&self.0)?));
            }
            let conn = &mut slot.as_mut().unwrap().1;
            let tx = conn.transaction_with_behavior(
                rusqlite::TransactionBehavior::Immediate,
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO inode \
                  (ino,kind,size,mode,uid,gid,nlink,atime_ns,mtime_ns,ctime_ns,rdev) \
                  VALUES (?1,8,0,420,0,0,1,?2,?2,?2,0)",
                rusqlite::params![ino as i64, mtime_ns],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO dentry (parent,name,ino) VALUES (?1,?2,?3)",
                rusqlite::params![parent as i64, name, ino as i64],
            )?;
            tx.execute(
                "INSERT INTO journal (record) VALUES (?1)",
                rusqlite::params![&JOURNAL_REC[..]],
            )?;
            tx.commit()?;
            Ok(())
        })
    }
}

fn bench_sqlite(dir: &Path, ds: &Dataset, threads: usize, mt: bool) -> Result<()> {
    let path = dir.join("meta.db");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
    let name = if mt { "sqlite-mt" } else { "sqlite" };

    if mt {
        let path_c = path.clone();
        run_phases(
            name,
            ds,
            threads,
            dir,
            || {
                let mut conn = sqlite_connect(&path_c)?;
                sqlite_schema(&conn)?;
                sqlite_load(&mut conn, ds)?;
                Ok(())
            },
            Arc::new(SqlitePathReader(path.clone())),
            Box::new(SqlitePathWriter(path)),
        )
    } else {
        let mut holder: Option<Arc<Mutex<rusqlite::Connection>>> = None;
        let path_c = path.clone();
        run_phases(
            name,
            ds,
            threads,
            dir,
            || {
                let mut conn = sqlite_connect(&path_c)?;
                sqlite_schema(&conn)?;
                sqlite_load(&mut conn, ds)?;
                holder = Some(Arc::new(Mutex::new(conn)));
                Ok(())
            },
            Arc::new(SqliteMutexReader(Mutex::new(sqlite_connect(&path)?))),
            // Writer is created before load finishes — use a path writer that
            // opens after load, then swap isn't needed: PathWriter is fine for
            // setattr volume here and still hits the same WAL file.
            Box::new(SqlitePathWriter(path)),
        )?;
        let _ = holder;
        Ok(())
    }
}

// ---- redb ----

struct RedbReader(Arc<redb::Database>);
impl Reader for RedbReader {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        const D: redb::TableDefinition<'_, &[u8], u64> = redb::TableDefinition::new("dentry");
        let tx = self.0.begin_read()?;
        let t = tx.open_table(D)?;
        Ok(t.get(dentry_key(parent, name).as_slice())?.map(|v| v.value()))
    }
    fn getattr(&self, ino: u64) -> Result<Option<usize>> {
        const I: redb::TableDefinition<'_, &[u8], &[u8]> = redb::TableDefinition::new("inode");
        let tx = self.0.begin_read()?;
        let t = tx.open_table(I)?;
        Ok(t.get(inode_key(ino).as_slice())?.map(|v| v.value().len()))
    }
    fn readdir_count(&self, parent: u64) -> Result<usize> {
        const D: redb::TableDefinition<'_, &[u8], u64> = redb::TableDefinition::new("dentry");
        let tx = self.0.begin_read()?;
        let t = tx.open_table(D)?;
        let lo = parent.to_be_bytes().to_vec();
        let hi = (parent + 1).to_be_bytes().to_vec();
        Ok(t.range(lo.as_slice()..hi.as_slice())?.count())
    }
}

struct RedbWriter(Arc<redb::Database>);
impl Writer for RedbWriter {
    fn setattr_mtime(&mut self, ino: u64, mtime_ns: i64) -> Result<()> {
        const I: redb::TableDefinition<'_, &[u8], &[u8]> = redb::TableDefinition::new("inode");
        const J: redb::TableDefinition<'_, &[u8], &[u8]> = redb::TableDefinition::new("journal");
        let tx = self.0.begin_write()?;
        {
            let mut ti = tx.open_table(I)?;
            let k = inode_key(ino);
            let mut bytes = match ti.get(k.as_slice())? {
                Some(v) => v.value().to_vec(),
                None => return Ok(()),
            };
            patch_mtime(&mut bytes, mtime_ns);
            ti.insert(k.as_slice(), bytes.as_slice())?;
            let mut tj = tx.open_table(J)?;
            tj.insert(mtime_ns.to_be_bytes().as_slice(), JOURNAL_REC.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
    fn create_file(
        &mut self,
        parent: u64,
        name: &str,
        ino: u64,
        mtime_ns: i64,
    ) -> Result<()> {
        const I: redb::TableDefinition<'_, &[u8], &[u8]> = redb::TableDefinition::new("inode");
        const D: redb::TableDefinition<'_, &[u8], u64>   = redb::TableDefinition::new("dentry");
        const J: redb::TableDefinition<'_, &[u8], &[u8]> = redb::TableDefinition::new("journal");
        let inode_val = encode_inode(&Inode {
            ino,
            kind: 8,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_ns: mtime_ns,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: 0,
            manifest: vec![],
            symlink_target: None,
        });
        let tx = self.0.begin_write()?;
        {
            tx.open_table(I)?.insert(inode_key(ino).as_slice(), inode_val.as_slice())?;
            tx.open_table(D)?.insert(dentry_key(parent, name).as_slice(), ino)?;
            tx.open_table(J)?.insert(mtime_ns.to_be_bytes().as_slice(), JOURNAL_REC.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
}

fn bench_redb(dir: &Path, ds: &Dataset, threads: usize) -> Result<()> {
    let path = dir.join("meta.redb");
    let _ = std::fs::remove_file(&path);
    let db = Arc::new(redb::Database::create(&path)?);
    const D: redb::TableDefinition<'_, &[u8], u64> = redb::TableDefinition::new("dentry");
    const I: redb::TableDefinition<'_, &[u8], &[u8]> = redb::TableDefinition::new("inode");
    let db_load = Arc::clone(&db);
    run_phases(
        "redb",
        ds,
        threads,
        dir,
        move || {
            for chunk in ds.inodes.chunks(LOAD_BATCH) {
                let tx = db_load.begin_write()?;
                {
                    let mut t = tx.open_table(I)?;
                    for i in chunk {
                        t.insert(inode_key(i.ino).as_slice(), encode_inode(i).as_slice())?;
                    }
                }
                tx.commit()?;
            }
            for chunk in ds.dentries.chunks(LOAD_BATCH) {
                let tx = db_load.begin_write()?;
                {
                    let mut t = tx.open_table(D)?;
                    for d in chunk {
                        t.insert(dentry_key(d.parent, &d.name).as_slice(), d.ino)?;
                    }
                }
                tx.commit()?;
            }
            Ok(())
        },
        Arc::new(RedbReader(Arc::clone(&db))),
        Box::new(RedbWriter(db)),
    )
}

// ---- fjall ----

struct FjallState {
    _ks: fjall::Keyspace,
    dentry: fjall::PartitionHandle,
    inode: fjall::PartitionHandle,
}

struct FjallReader(Arc<FjallState>);
impl Reader for FjallReader {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        Ok(self
            .0
            .dentry
            .get(dentry_key(parent, name))?
            .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap())))
    }
    fn getattr(&self, ino: u64) -> Result<Option<usize>> {
        Ok(self.0.inode.get(inode_key(ino))?.map(|v| v.len()))
    }
    fn readdir_count(&self, parent: u64) -> Result<usize> {
        Ok(self.0.dentry.prefix(parent.to_be_bytes()).count())
    }
}

struct FjallWriter(Arc<FjallState>);
impl Writer for FjallWriter {
    fn setattr_mtime(&mut self, ino: u64, mtime_ns: i64) -> Result<()> {
        let Some(v) = self.0.inode.get(inode_key(ino))? else {
            return Ok(());
        };
        let mut bytes = v.to_vec();
        patch_mtime(&mut bytes, mtime_ns);
        self.0.inode.insert(inode_key(ino), bytes)?;
        self.0.inode.insert(
            lmdb_journal_key(mtime_ns as u64),
            JOURNAL_REC.to_vec(),
        )?;
        Ok(())
    }
    fn create_file(
        &mut self,
        parent: u64,
        name: &str,
        ino: u64,
        mtime_ns: i64,
    ) -> Result<()> {
        let inode_val = encode_inode(&Inode {
            ino,
            kind: 8,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_ns: mtime_ns,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: 0,
            manifest: vec![],
            symlink_target: None,
        });
        self.0.inode.insert(inode_key(ino), inode_val)?;
        self.0.dentry.insert(dentry_key(parent, name), ino.to_be_bytes())?;
        self.0.inode.insert(lmdb_journal_key(mtime_ns as u64), JOURNAL_REC.to_vec())?;
        Ok(())
    }
}

fn bench_fjall(dir: &Path, ds: &Dataset, threads: usize) -> Result<()> {
    let path = dir.join("meta.fjall");
    let _ = std::fs::remove_dir_all(&path);
    let ks = fjall::Config::new(&path).open()?;
    let dentry = ks.open_partition("dentry", Default::default())?;
    let inode = ks.open_partition("inode", Default::default())?;
    let state = Arc::new(FjallState {
        _ks: ks.clone(),
        dentry,
        inode,
    });
    let load_state = Arc::clone(&state);
    let ks_persist = ks;
    run_phases(
        "fjall",
        ds,
        threads,
        dir,
        move || {
            for (i, inode) in ds.inodes.iter().enumerate() {
                load_state
                    .inode
                    .insert(inode_key(inode.ino), encode_inode(inode))?;
                if i % LOAD_BATCH == 0 {
                    ks_persist.persist(fjall::PersistMode::Buffer)?;
                }
            }
            for (i, d) in ds.dentries.iter().enumerate() {
                load_state
                    .dentry
                    .insert(dentry_key(d.parent, &d.name), d.ino.to_be_bytes())?;
                if i % LOAD_BATCH == 0 {
                    ks_persist.persist(fjall::PersistMode::Buffer)?;
                }
            }
            ks_persist.persist(fjall::PersistMode::SyncAll)?;
            Ok(())
        },
        Arc::new(FjallReader(Arc::clone(&state))),
        Box::new(FjallWriter(state)),
    )
}

// ---- LMDB ----

struct LmdbState {
    env: heed::Env,
    dentry: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    inode: heed::Database<heed::types::Bytes, heed::types::Bytes>,
}

struct LmdbReader(Arc<LmdbState>);
impl Reader for LmdbReader {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        let rtx = self.0.env.read_txn()?;
        Ok(self
            .0
            .dentry
            .get(&rtx, &dentry_key(parent, name))?
            .map(|v| u64::from_be_bytes(v.try_into().unwrap())))
    }
    fn getattr(&self, ino: u64) -> Result<Option<usize>> {
        let rtx = self.0.env.read_txn()?;
        Ok(self.0.inode.get(&rtx, &inode_key(ino))?.map(|v| v.len()))
    }
    fn readdir_count(&self, parent: u64) -> Result<usize> {
        let rtx = self.0.env.read_txn()?;
        let n = self
            .0
            .dentry
            .prefix_iter(&rtx, &parent.to_be_bytes())?
            .count();
        Ok(n)
    }
}

struct LmdbWriter(Arc<LmdbState>);
impl Writer for LmdbWriter {
    fn setattr_mtime(&mut self, ino: u64, mtime_ns: i64) -> Result<()> {
        let mut tx = self.0.env.write_txn()?;
        let key = inode_key(ino);
        if let Some(v) = self.0.inode.get(&tx, &key)? {
            let mut bytes = v.to_vec();
            patch_mtime(&mut bytes, mtime_ns);
            self.0.inode.put(&mut tx, &key, &bytes)?;
        }
        // Journal row in the same tx: append-only key = mtime_ns as seq.
        self.0.inode.put(&mut tx, &lmdb_journal_key(mtime_ns as u64), &JOURNAL_REC)?;
        tx.commit()?;
        Ok(())
    }
    fn create_file(
        &mut self,
        parent: u64,
        name: &str,
        ino: u64,
        mtime_ns: i64,
    ) -> Result<()> {
        let inode_val = encode_inode(&Inode {
            ino,
            kind: 8,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_ns: mtime_ns,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: 0,
            manifest: vec![],
            symlink_target: None,
        });
        let mut tx = self.0.env.write_txn()?;
        self.0.inode.put(&mut tx, &inode_key(ino), &inode_val)?;
        self.0.dentry.put(&mut tx, &dentry_key(parent, name), &ino.to_be_bytes())?;
        self.0.inode.put(&mut tx, &lmdb_journal_key(mtime_ns as u64), &JOURNAL_REC)?;
        tx.commit()?;
        Ok(())
    }
}

fn lmdb_journal_key(seq: u64) -> [u8; 9] {
    let mut k = [0u8; 9];
    k[0] = 0xFF; // sorts after all inode keys (ino ≤ 2^48)
    k[1..].copy_from_slice(&seq.to_be_bytes());
    k
}

fn bench_lmdb(dir: &Path, ds: &Dataset, threads: usize) -> Result<()> {
    let path = dir.join("meta.lmdb");
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path)?;
    // WRITE_MAP: pages written directly to the mmap; avoids the dirty-page
    // copy of NOSYNC while staying safe across orderly commits.  Map is
    // sized to 2× logical payload + write headroom; LMDB truncates on close.
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(256 << 20) // 256 MiB — enough for corpus + scratch
            .max_dbs(3)
            .flags(heed::EnvFlags::WRITE_MAP)
            .open(&path)?
    };
    let mut wtx = env.write_txn()?;
    let dentry = env.create_database(&mut wtx, Some("dentry"))?;
    let inode = env.create_database(&mut wtx, Some("inode"))?;
    wtx.commit()?;
    let state = Arc::new(LmdbState {
        env,
        dentry,
        inode,
    });
    let load_state = Arc::clone(&state);
    run_phases(
        "lmdb",
        ds,
        threads,
        dir,
        move || {
            for chunk in ds.inodes.chunks(LOAD_BATCH) {
                let mut tx = load_state.env.write_txn()?;
                for i in chunk {
                    load_state
                        .inode
                        .put(&mut tx, &inode_key(i.ino), &encode_inode(i))?;
                }
                tx.commit()?;
            }
            for chunk in ds.dentries.chunks(LOAD_BATCH) {
                let mut tx = load_state.env.write_txn()?;
                for d in chunk {
                    load_state.dentry.put(
                        &mut tx,
                        &dentry_key(d.parent, &d.name),
                        &d.ino.to_be_bytes(),
                    )?;
                }
                tx.commit()?;
            }
            load_state.env.force_sync()?;
            Ok(())
        },
        Arc::new(LmdbReader(Arc::clone(&state))),
        Box::new(LmdbWriter(state)),
    )
}

// ---- RocksDB ----

struct RocksReader(Arc<rocksdb::DB>);
impl Reader for RocksReader {
    fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        let dcf = self.0.cf_handle("dentry").unwrap();
        Ok(self
            .0
            .get_cf(&dcf, dentry_key(parent, name))?
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap())))
    }
    fn getattr(&self, ino: u64) -> Result<Option<usize>> {
        let icf = self.0.cf_handle("inode").unwrap();
        Ok(self.0.get_cf(&icf, inode_key(ino))?.map(|v| v.len()))
    }
    fn readdir_count(&self, parent: u64) -> Result<usize> {
        let dcf = self.0.cf_handle("dentry").unwrap();
        let prefix = parent.to_be_bytes();
        let end = (parent + 1).to_be_bytes();
        let mut n = 0usize;
        let mut iter = self.0.iterator_cf(
            &dcf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        while let Some(Ok((k, _))) = iter.next() {
            if k.as_ref() >= end.as_slice() {
                break;
            }
            n += 1;
        }
        Ok(n)
    }
}

struct RocksWriter(Arc<rocksdb::DB>);
impl Writer for RocksWriter {
    fn setattr_mtime(&mut self, ino: u64, mtime_ns: i64) -> Result<()> {
        let icf = self.0.cf_handle("inode").unwrap();
        let jcf = self.0.cf_handle("journal").unwrap();
        let key = inode_key(ino);
        if let Some(mut bytes) = self.0.get_cf(&icf, key)? {
            patch_mtime(&mut bytes, mtime_ns);
            // Atomic batch: inode update + journal row, WAL-synced.
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&icf, key, bytes);
            batch.put_cf(&jcf, &mtime_ns.to_be_bytes(), &JOURNAL_REC);
            let mut wo = rocksdb::WriteOptions::default();
            wo.set_sync(true);
            self.0.write_opt(batch, &wo)?;
        }
        Ok(())
    }
    fn create_file(
        &mut self,
        parent: u64,
        name: &str,
        ino: u64,
        mtime_ns: i64,
    ) -> Result<()> {
        let icf = self.0.cf_handle("inode").unwrap();
        let dcf = self.0.cf_handle("dentry").unwrap();
        let jcf = self.0.cf_handle("journal").unwrap();
        let inode_val = encode_inode(&Inode {
            ino,
            kind: 8,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_ns: mtime_ns,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: 0,
            manifest: vec![],
            symlink_target: None,
        });
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&icf, inode_key(ino), inode_val);
        batch.put_cf(&dcf, dentry_key(parent, name), ino.to_be_bytes());
        batch.put_cf(&jcf, &mtime_ns.to_be_bytes(), &JOURNAL_REC);
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.0.write_opt(batch, &wo)?;
        Ok(())
    }
}

fn rocks_opts() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_max_background_jobs(4);
    opts.increase_parallelism(4);
    let cache = rocksdb::Cache::new_lru_cache(512 * 1024 * 1024);
    let mut bopts = rocksdb::BlockBasedOptions::default();
    bopts.set_block_cache(&cache);
    opts.set_block_based_table_factory(&bopts);
    opts
}

fn bench_rocksdb(dir: &Path, ds: &Dataset, threads: usize) -> Result<()> {
    let path = dir.join("meta.rocks");
    let _ = std::fs::remove_dir_all(&path);
    let db = Arc::new(rocksdb::DB::open_cf(
        &rocks_opts(),
        &path,
        ["dentry", "inode", "journal"],
    )?);
    let load_db = Arc::clone(&db);
    run_phases(
        "rocksdb",
        ds,
        threads,
        dir,
        move || {
            let dcf = load_db.cf_handle("dentry").unwrap();
            let icf = load_db.cf_handle("inode").unwrap();
            for chunk in ds.inodes.chunks(LOAD_BATCH) {
                let mut batch = rocksdb::WriteBatch::default();
                for i in chunk {
                    batch.put_cf(&icf, inode_key(i.ino), encode_inode(i));
                }
                load_db.write(batch)?;
            }
            for chunk in ds.dentries.chunks(LOAD_BATCH) {
                let mut batch = rocksdb::WriteBatch::default();
                for d in chunk {
                    batch.put_cf(&dcf, dentry_key(d.parent, &d.name), d.ino.to_be_bytes());
                }
                load_db.write(batch)?;
            }
            load_db.flush()?;
            Ok(())
        },
        Arc::new(RocksReader(Arc::clone(&db))),
        Box::new(RocksWriter(db)),
    )
}

fn run_one(engine: Engine, args: &Args, ds: &Dataset) -> Result<()> {
    let dir = args.data_dir.join(match engine {
        Engine::Sqlite => "sqlite",
        Engine::SqliteMt => "sqlite-mt",
        Engine::Redb => "redb",
        Engine::Fjall => "fjall",
        Engine::Lmdb => "lmdb",
        Engine::Rocksdb => "rocksdb",
    });
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    match engine {
        Engine::Sqlite => bench_sqlite(&dir, ds, args.threads, false),
        Engine::SqliteMt => bench_sqlite(&dir, ds, args.threads, true),
        Engine::Redb => bench_redb(&dir, ds, args.threads),
        Engine::Fjall => bench_fjall(&dir, ds, args.threads),
        Engine::Lmdb => bench_lmdb(&dir, ds, args.threads),
        Engine::Rocksdb => bench_rocksdb(&dir, ds, args.threads),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.source.exists() {
        bail!(
            "source DB {} missing — vacuum a live meta.db into it first",
            args.source.display()
        );
    }

    eprintln!("loading corpus from {} …", args.source.display());
    let ds = Dataset::load(&args.source)?;
    eprintln!(
        "  {} inodes, {} dentries, {:.1} MiB logical",
        ds.inodes.len(),
        ds.dentries.len(),
        ds.bytes_logical() as f64 / (1024.0 * 1024.0)
    );
    if insn_available() {
        eprintln!("note: hardware instruction counters enabled");
    } else {
        eprintln!(
            "note: hardware instruction counters unavailable \
             (perf_event_paranoid / capabilities); reporting TSC cycles instead"
        );
    }
    eprintln!(
        "workloads: {LOOKUPS} lookups (serial + {}-way), {SCANS} readdirs, \
         {LOOKUPS} getattrs, {WRITE_OPS} setattrs",
        args.threads
    );

    let engines: Vec<Engine> = match args.engine {
        Some(e) => vec![e],
        None => {
            let mut list = vec![
                Engine::Sqlite,
                Engine::SqliteMt,
                Engine::Lmdb,
                Engine::Rocksdb,
            ];
            if args.include_archived {
                list.extend([Engine::Redb, Engine::Fjall]);
            }
            list
        }
    };
    for e in engines {
        eprintln!("\n--- {:?} ---", e);
        if let Err(err) = run_one(e, &args, &ds) {
            eprintln!("ERROR {:?}: {err:#}", e);
        }
    }
    Ok(())
}
