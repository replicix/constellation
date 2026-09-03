//! Census-scale import benchmark (ROADMAP.md phase-1 exit criterion):
//! stage a many-small-files tree, import it into a constellation mount,
//! and measure import, metadata-walk, read-back, and delete behaviour.

use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{Context, Result};
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct BenchConfig {
    pub files: u64,
    pub file_size: u64,
    pub fanout: u64,
    /// Optional gate: fail when the import takes longer than this.
    pub budget_s: Option<u64>,
    pub e2e: bool,
    pub json: bool,
    pub seed: u64,
    /// Generate an on-the-fly synthetic tree with realistic size/depth distribution.
    pub corpus_shape: bool,
    /// Add one-way + return-path latency via toxiproxy.
    pub s3_latency_ms: Option<u64>,
    /// Throttle S3 link bandwidth (megabits/s) via toxiproxy.
    pub s3_bandwidth_mbps: Option<u64>,
    /// Freeform report label (e.g. full / latency250 / bw50).
    pub label: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub label: String,
    pub seed: u64,
    pub corpus_shape: bool,
    pub s3_latency_ms: Option<u64>,
    pub s3_bandwidth_mbps: Option<u64>,

    pub staged_files: u64,
    pub staged_dirs: u64,
    pub staged_bytes: u64,

    pub import_s: f64,
    pub durable_import_s: f64,
    pub import_files_per_sec: f64,
    pub durable_import_files_per_sec: f64,

    pub writeback_import_files_per_sec: f64,
    pub writeback_durable_files_per_sec: f64,

    pub metadata_walk_files_per_sec: f64,
    pub cold_read_files_per_sec: f64,
    pub sequential_cold_read_mib_per_sec: f64,
    pub warm_random_read_iops: f64,

    pub delete_s: f64,
    pub durable_delete_s: f64,
    pub delete_files_per_sec: f64,
    pub durable_delete_files_per_sec: f64,

    pub import_window_p50_fps: f64,
    pub import_window_p95_fps: f64,
    pub import_window_min_fps: f64,
    pub delete_window_p50_fps: f64,
    pub delete_window_p95_fps: f64,
    pub delete_window_min_fps: f64,

    pub db_bytes: u64,
    pub db_inode_rows: u64,
    pub db_dentry_rows: u64,
    pub db_journal_rows: u64,
}

#[derive(Default)]
struct TreeStats {
    files: u64,
    dirs: u64,
    bytes: u64,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p * (sorted.len() as f64 - 1.0)).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn rate_summary(points: &[(f64, u64)], deleting: bool) -> (f64, f64, f64) {
    if points.len() < 2 {
        return (0.0, 0.0, 0.0);
    }
    let mut rates = Vec::with_capacity(points.len() - 1);
    for w in points.windows(2) {
        let dt = (w[1].0 - w[0].0).max(1e-6);
        let d = if deleting {
            (w[0].1 as i64 - w[1].1 as i64).max(0) as f64
        } else {
            (w[1].1 as i64 - w[0].1 as i64).max(0) as f64
        };
        rates.push(d / dt);
    }
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (pct(&rates, 0.50), pct(&rates, 0.95), *rates.first().unwrap_or(&0.0))
}

fn corpus_like_size(rng: &mut rand::rngs::StdRng, fallback: u64) -> usize {
    let roll: u32 = rng.random_range(0..10_000);
    let n = match roll {
        0..=6999 => rng.random_range(300..=8 * 1024),
        7000..=9199 => rng.random_range(8 * 1024..=128 * 1024),
        9200..=9849 => rng.random_range(128 * 1024..=2 * 1024 * 1024),
        _ => rng.random_range(2 * 1024 * 1024..=16 * 1024 * 1024),
    };
    n.max(fallback as usize)
}

fn staged_rel_path(rng: &mut rand::rngs::StdRng, i: u64, fanout: u64) -> PathBuf {
    // Deterministic anonymized hierarchy with realistic depth skew.
    let depth_roll: u32 = rng.random_range(0..10_000);
    let depth = match depth_roll {
        0..=6999 => 2,
        7000..=9099 => 3,
        9100..=9799 => 4,
        _ => 5,
    };
    let mut p = PathBuf::new();
    p.push(format!("org{:03}", i % fanout.max(1)));
    p.push(format!("proj{:04}", rng.random_range(0..fanout.max(2) * 8)));
    if depth >= 3 {
        p.push(format!("team{:04}", rng.random_range(0..fanout.max(2) * 12)));
    }
    if depth >= 4 {
        p.push(format!("sub{:05}", rng.random_range(0..fanout.max(2) * 24)));
    }
    if depth >= 5 {
        p.push(format!("leaf{:05}", rng.random_range(0..fanout.max(2) * 40)));
    }
    p.push(format!("f{i:08x}.bin"));
    p
}

fn stage_tree(root: &Path, cfg: &BenchConfig) -> Result<TreeStats> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(cfg.seed);
    let mut stats = TreeStats::default();
    let mut payload = vec![0u8; cfg.file_size.max(256) as usize];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 253) as u8;
    }

    for i in 0..cfg.files {
        let rel = if cfg.corpus_shape {
            staged_rel_path(&mut rng, i, cfg.fanout)
        } else {
            PathBuf::from(format!("d{:03}/f{i:07}", i % cfg.fanout))
        };
        let path = root.join(&rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let size = if cfg.corpus_shape {
            corpus_like_size(&mut rng, cfg.file_size)
        } else {
            cfg.file_size as usize
        };
        let mut f = std::fs::File::create(&path)?;
        // Unique prefix defeats whole-file dedup and keeps import realistic.
        writeln!(f, "file {i} seed {}", cfg.seed)?;
        let mut remain = size.saturating_sub(24);
        while remain > 0 {
            let n = remain.min(payload.len());
            f.write_all(&payload[..n])?;
            remain -= n;
        }
        stats.files += 1;
        stats.bytes += size as u64;
    }

    // Count dirs after staging.
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        stats.dirs += 1;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(stats)
}

fn count_tree(root: &Path) -> Result<u64> {
    let mut n = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                stack.push(entry.path());
            } else {
                n += 1;
            }
        }
    }
    Ok(n)
}

fn sample_file_counts(path: PathBuf, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<Vec<(f64, u64)>> {
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let t0 = Instant::now();
        loop {
            let n = if path.exists() {
                count_tree(&path).unwrap_or(0)
            } else {
                0
            };
            out.push((t0.elapsed().as_secs_f64(), n));
            if stop.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        out
    })
}

fn db_stats(db: &Path) -> Result<(u64, u64, u64, u64)> {
    let mut bytes = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
    let wal = db.with_extension("db-wal");
    bytes += std::fs::metadata(wal).map(|m| m.len()).unwrap_or(0);
    let shm = db.with_extension("db-shm");
    bytes += std::fs::metadata(shm).map(|m| m.len()).unwrap_or(0);

    let conn = rusqlite::Connection::open(db)?;
    let inode_rows: u64 = conn.query_row("SELECT COUNT(*) FROM inode", [], |r| r.get(0))?;
    let dentry_rows: u64 = conn.query_row("SELECT COUNT(*) FROM dentry", [], |r| r.get(0))?;
    let journal_rows: u64 = conn.query_row("SELECT COUNT(*) FROM journal", [], |r| r.get(0))?;
    Ok((bytes, inode_rows, dentry_rows, journal_rows))
}

pub fn run(cfg: &BenchConfig) -> Result<BenchReport> {
    let env = S3Env::start()?;
    let proxy = env.s3_proxy()?;
    if let Some(ms) = cfg.s3_latency_ms {
        proxy.latency(ms, 15)?;
    }
    if let Some(mbps) = cfg.s3_bandwidth_mbps {
        // toxiproxy bandwidth toxic takes KB/s.
        proxy.bandwidth((mbps.saturating_mul(1000) / 8).max(1))?;
    }

    let root = tempfile::Builder::new().prefix("harness-bench-").tempdir()?;
    let backend = format!("s3://{BUCKET}/bench-{}", std::process::id());
    let mut c = Client::new(root.path(), "bench", &env.endpoint, &backend)?;
    if cfg.e2e {
        c = c.with_e2e();
    }
    c.fs_create()?;
    c.mount()?;

    let staged = root.path().join("staged");
    std::fs::create_dir(&staged)?;
    let staged_stats = stage_tree(&staged, cfg)?;
    eprintln!(
        "staged {} files, {} dirs, {:.1} MiB ({})",
        staged_stats.files,
        staged_stats.dirs,
        staged_stats.bytes as f64 / (1024.0 * 1024.0),
        if cfg.corpus_shape { "corpus-like" } else { "flat" }
    );

    // Import: rsync -a into the mount, then clean unmount for durable timing.
    let census = c.mnt.join("census");
    let stop_import = Arc::new(AtomicBool::new(false));
    let sampler = sample_file_counts(census.clone(), Arc::clone(&stop_import));
    let t0 = Instant::now();
    let st = std::process::Command::new("rsync")
        .args(["-a", "--delete", "--inplace"])
        .arg(format!("{}/", staged.display()))
        .arg(format!("{}/", census.display()))
        .status()?;
    anyhow::ensure!(st.success(), "rsync import failed");
    let import_s = t0.elapsed().as_secs_f64();
    stop_import.store(true, Ordering::Relaxed);
    let import_points = sampler.join().unwrap_or_default();
    c.unmount()?;
    let durable_import_s = t0.elapsed().as_secs_f64();
    let import_files_per_sec = cfg.files as f64 / import_s.max(1e-9);
    let durable_import_files_per_sec = cfg.files as f64 / durable_import_s.max(1e-9);

    // Through-mode vs write-back import behavior.
    let back_backend = format!("s3://{BUCKET}/bench-back-{}", std::process::id());
    let mut back = Client::new(root.path(), "bench-back", &env.endpoint, &back_backend)?
        .with_write_mode("back");
    if cfg.e2e {
        back = back.with_e2e();
    }
    back.fs_create()?;
    back.mount()?;
    let back_t0 = Instant::now();
    let st = std::process::Command::new("rsync")
        .args(["-a", "--delete", "--inplace"])
        .arg(format!("{}/", staged.display()))
        .arg(format!("{}/", back.mnt.join("census").display()))
        .status()?;
    anyhow::ensure!(st.success(), "write-back rsync import failed");
    let back_import_s = back_t0.elapsed().as_secs_f64();
    back.set_write_mode("through")?;
    let back_durable_s = back_t0.elapsed().as_secs_f64();
    back.unmount()?;
    let writeback_import_files_per_sec = cfg.files as f64 / back_import_s.max(1e-9);
    let writeback_durable_files_per_sec = cfg.files as f64 / back_durable_s.max(1e-9);

    // Metadata walk over the full tree (warm replica).
    c.mount()?;
    let t1 = Instant::now();
    let n = count_tree(&c.mnt.join("census"))?;
    anyhow::ensure!(n == cfg.files, "walk found {n} files, expected {}", cfg.files);
    let walk_s = t1.elapsed().as_secs_f64();
    let metadata_walk_files_per_sec = cfg.files as f64 / walk_s.max(1e-9);

    // Cold read-back: empty chunk cache, everything pulled from S3.
    c.unmount()?;
    c.drop_cache()?;
    c.mount()?;
    let t2 = Instant::now();
    let st = std::process::Command::new("tar")
        .arg("cf")
        .arg("/dev/null")
        .arg("-C")
        .arg(&c.mnt)
        .arg("census")
        .status()?;
    anyhow::ensure!(st.success(), "tar read-back failed");
    let cold_s = t2.elapsed().as_secs_f64();
    let cold_read_files_per_sec = cfg.files as f64 / cold_s.max(1e-9);

    // Large sequential cold read + warm random reads.
    let large_path = c.mnt.join("large-read-probe");
    let large_bytes = 64 * 1024 * 1024_u64;
    let block = vec![0x5a; 1024 * 1024];
    let mut large = std::fs::File::create(&large_path)?;
    for _ in 0..(large_bytes / block.len() as u64) {
        large.write_all(&block)?;
    }
    large.sync_all()?;
    drop(large);
    c.unmount()?;
    c.drop_cache()?;
    c.mount()?;

    let mut large = std::fs::File::open(&large_path)?;
    let mut read_buf = vec![0_u8; 1024 * 1024];
    let seq_t0 = Instant::now();
    while large.read(&mut read_buf)? != 0 {}
    let seq_s = seq_t0.elapsed().as_secs_f64();
    let sequential_cold_read_mib_per_sec = 64.0 / seq_s.max(1e-9);

    let mut small = [0_u8; 4096];
    let random_ops = 4096_u64;
    let random_t0 = Instant::now();
    for i in 0..random_ops {
        let block_count = large_bytes / small.len() as u64;
        let block_no = i.wrapping_mul(2_654_435_761) % block_count;
        large.seek(SeekFrom::Start(block_no * small.len() as u64))?;
        large.read_exact(&mut small)?;
    }
    let warm_random_read_iops = random_ops as f64 / random_t0.elapsed().as_secs_f64().max(1e-9);
    drop(large);

    // Delete benchmark: rm -rf through the mount, then durable after unmount.
    let stop_delete = Arc::new(AtomicBool::new(false));
    let del_sampler = sample_file_counts(c.mnt.join("census"), Arc::clone(&stop_delete));
    let td = Instant::now();
    let st = std::process::Command::new("rm")
        .args(["-rf", "--"])
        .arg(c.mnt.join("census"))
        .status()?;
    anyhow::ensure!(st.success(), "rm -rf delete failed");
    let delete_s = td.elapsed().as_secs_f64();
    stop_delete.store(true, Ordering::Relaxed);
    let delete_points = del_sampler.join().unwrap_or_default();
    c.unmount()?;
    let durable_delete_s = td.elapsed().as_secs_f64();
    let delete_files_per_sec = cfg.files as f64 / delete_s.max(1e-9);
    let durable_delete_files_per_sec = cfg.files as f64 / durable_delete_s.max(1e-9);

    let (db_bytes, db_inode_rows, db_dentry_rows, db_journal_rows) = db_stats(&c.replica_db())?;

    let (import_window_p50_fps, import_window_p95_fps, import_window_min_fps) =
        rate_summary(&import_points, false);
    let (delete_window_p50_fps, delete_window_p95_fps, delete_window_min_fps) =
        rate_summary(&delete_points, true);

    if let Some(budget) = cfg.budget_s {
        anyhow::ensure!(
            durable_import_s <= budget as f64,
            "import took {durable_import_s:.1}s, over the {budget}s budget"
        );
    }

    Ok(BenchReport {
        label: cfg.label.clone().unwrap_or_else(|| "full".to_string()),
        seed: cfg.seed,
        corpus_shape: cfg.corpus_shape,
        s3_latency_ms: cfg.s3_latency_ms,
        s3_bandwidth_mbps: cfg.s3_bandwidth_mbps,
        staged_files: staged_stats.files,
        staged_dirs: staged_stats.dirs,
        staged_bytes: staged_stats.bytes,
        import_s,
        durable_import_s,
        import_files_per_sec,
        durable_import_files_per_sec,
        writeback_import_files_per_sec,
        writeback_durable_files_per_sec,
        metadata_walk_files_per_sec,
        cold_read_files_per_sec,
        sequential_cold_read_mib_per_sec,
        warm_random_read_iops,
        delete_s,
        durable_delete_s,
        delete_files_per_sec,
        durable_delete_files_per_sec,
        import_window_p50_fps,
        import_window_p95_fps,
        import_window_min_fps,
        delete_window_p50_fps,
        delete_window_p95_fps,
        delete_window_min_fps,
        db_bytes,
        db_inode_rows,
        db_dentry_rows,
        db_journal_rows,
    })
}

impl BenchConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(self.files > 0 && self.fanout > 0, "files and fanout must be > 0");
        anyhow::ensure!(self.seed > 0, "seed must be > 0");
        (self.files.checked_mul(self.file_size.max(1)))
            .context("files * file_size overflows")
            .map(|_| ())
    }
}
