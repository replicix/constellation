//! Census-scale import benchmark (ROADMAP.md phase-1 exit criterion):
//! stage a many-small-files tree, import it into a constellation mount,
//! and measure import, metadata-walk, and cold read-back rates.

use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{Context, Result};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

pub struct BenchConfig {
    pub files: u64,
    pub file_size: u64,
    pub fanout: u64,
    /// Optional gate: fail when the import takes longer than this.
    pub budget_s: Option<u64>,
}

fn stage_tree(root: &Path, cfg: &BenchConfig) -> Result<()> {
    let mut payload = vec![0u8; cfg.file_size as usize];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 253) as u8;
    }
    for i in 0..cfg.files {
        let dir = root.join(format!("d{:03}", i % cfg.fanout));
        if i < cfg.fanout {
            std::fs::create_dir_all(&dir)?;
        }
        let mut f = std::fs::File::create(dir.join(format!("f{i:07}")))?;
        // Unique prefix defeats whole-file dedup, matching real corpora.
        writeln!(f, "file {i}")?;
        f.write_all(&payload)?;
    }
    Ok(())
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

pub fn run(cfg: &BenchConfig) -> Result<()> {
    let env = S3Env::start()?;
    let _proxy = env.s3_proxy()?;
    let root = tempfile::Builder::new()
        .prefix("harness-bench-")
        .tempdir()?;
    let backend = format!("s3://{BUCKET}/bench-{}", std::process::id());
    let mut c = Client::new(root.path(), "bench", &env.endpoint, &backend)?;
    c.fs_create()?;
    c.mount()?;

    let staged = root.path().join("staged");
    std::fs::create_dir(&staged)?;
    stage_tree(&staged, cfg)?;
    eprintln!(
        "staged {} files x {} B in {} dirs",
        cfg.files, cfg.file_size, cfg.fanout
    );

    // Import: cp -r into the mount, then a clean unmount so the numbers
    // include chunk upload + full metadata shipping (durable in S3).
    let t0 = Instant::now();
    let st = std::process::Command::new("cp")
        .arg("-r")
        .arg(&staged)
        .arg(c.mnt.join("census"))
        .status()?;
    anyhow::ensure!(st.success(), "cp -r failed");
    let copy_s = t0.elapsed().as_secs_f64();
    c.unmount()?;
    let durable_s = t0.elapsed().as_secs_f64();
    eprintln!(
        "import: copy {copy_s:.1}s ({:.0} files/s), durable-in-S3 {durable_s:.1}s ({:.0} files/s)",
        cfg.files as f64 / copy_s,
        cfg.files as f64 / durable_s,
    );

    // Same staged tree under write-back. Copy time is the operator
    // visible import figure; switching back to through is the explicit
    // drain barrier and gives the corresponding durable time.
    let back_backend = format!("s3://{BUCKET}/bench-back-{}", std::process::id());
    let mut back = Client::new(root.path(), "bench-back", &env.endpoint, &back_backend)?
        .with_write_mode("back");
    back.fs_create()?;
    back.mount()?;
    let back_t0 = Instant::now();
    let status = std::process::Command::new("cp")
        .arg("-r")
        .arg(&staged)
        .arg(back.mnt.join("census"))
        .status()?;
    anyhow::ensure!(status.success(), "write-back cp -r failed");
    let back_copy_s = back_t0.elapsed().as_secs_f64();
    back.set_write_mode("through")?;
    let back_durable_s = back_t0.elapsed().as_secs_f64();
    eprintln!(
        "write-back import: copy {back_copy_s:.1}s ({:.0} files/s), drained {back_durable_s:.1}s ({:.0} files/s); through/back copy speedup {:.1}x",
        cfg.files as f64 / back_copy_s,
        cfg.files as f64 / back_durable_s,
        copy_s / back_copy_s,
    );
    back.unmount()?;

    // Metadata walk over the full tree (warm replica).
    c.mount()?;
    let t1 = Instant::now();
    let n = count_tree(&c.mnt.join("census"))?;
    anyhow::ensure!(
        n == cfg.files,
        "walk found {n} files, expected {}",
        cfg.files
    );
    eprintln!(
        "metadata walk: {:.1}s ({:.0} files/s)",
        t1.elapsed().as_secs_f64(),
        cfg.files as f64 / t1.elapsed().as_secs_f64()
    );

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
    eprintln!(
        "cold read-back: {:.1}s ({:.0} files/s)",
        t2.elapsed().as_secs_f64(),
        cfg.files as f64 / t2.elapsed().as_secs_f64()
    );
    c.unmount()?;

    if let Some(budget) = cfg.budget_s {
        anyhow::ensure!(
            durable_s <= budget as f64,
            "import took {durable_s:.1}s, over the {budget}s budget"
        );
    }
    Ok(())
}

impl BenchConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.files > 0 && self.fanout > 0,
            "files and fanout must be > 0"
        );
        (self.files.checked_mul(self.file_size))
            .context("files * file_size overflows")
            .map(|_| ())
    }
}
