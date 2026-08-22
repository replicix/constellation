//! Seeded random workload: applies the same operation to the real
//! mount and the model. Deterministic per seed, so failures reproduce
//! with `harness <scenario> --seed N`.

use crate::model::Model;
use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct Workload {
    rng: StdRng,
    counter: u64,
    /// Namespace prefix so multiple clients can share a mount later.
    prefix: String,
}

impl Workload {
    pub fn new(seed: u64, prefix: &str) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            counter: 0,
            prefix: prefix.to_string(),
        }
    }

    fn fresh_name(&mut self, kind: &str) -> String {
        self.counter += 1;
        format!("{}-{kind}-{}", self.prefix, self.counter)
    }

    fn data(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.rng.random_range(0..=max_len);
        let mut v = vec![0u8; len];
        self.rng.fill(&mut v[..]);
        v
    }

    /// Run one block of `n` random ops against `root`, mirroring into
    /// `model`. All files are closed when this returns (close-to-open
    /// durability point).
    pub fn run_block(&mut self, root: &Path, model: &mut Model, n: usize) -> Result<()> {
        for _ in 0..n {
            self.one_op(root, model)?;
        }
        Ok(())
    }

    fn one_op(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        // Weighted op mix; large-file writes are rarer.
        let dice = self.rng.random_range(0..100);
        let (name, r): (&str, Result<()>) = match dice {
            0..=24 => ("create", self.op_create(root, model)),
            25..=39 => ("overwrite", self.op_overwrite(root, model)),
            40..=49 => ("append", self.op_append(root, model)),
            50..=57 => ("truncate", self.op_truncate(root, model)),
            58..=69 => ("mkdir", self.op_mkdir(root, model)),
            70..=79 => ("rename", self.op_rename(root, model)),
            80..=89 => ("unlink", self.op_unlink(root, model)),
            90..=93 => ("rmdir", self.op_rmdir(root, model)),
            94..=96 => ("symlink", self.op_symlink(root, model)),
            _ => ("read-check", self.op_read_check(root, model)),
        };
        r.with_context(|| format!("op #{} ({name})", self.counter))
    }

    fn pick(&mut self, v: &[PathBuf]) -> Option<PathBuf> {
        v.choose(&mut self.rng).cloned()
    }

    fn op_create(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(dir) = self.pick(&model.dirs()) else {
            return Ok(());
        };
        let rel = dir.join(self.fresh_name("f"));
        // Mostly small files; occasionally multi-chunk (chunk = 1 MiB).
        let max = if self.rng.random_range(0..10) == 0 {
            3 << 20
        } else {
            64 << 10
        };
        let data = self.data(max);
        std::fs::write(root.join(&rel), &data).with_context(|| format!("create {rel:?}"))?;
        model.write_file(&rel, data);
        Ok(())
    }

    fn op_overwrite(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(rel) = self.pick(&model.files()) else {
            return Ok(());
        };
        let len = std::fs::metadata(root.join(&rel))?.len() as usize;
        let offset = if len == 0 {
            0
        } else {
            self.rng.random_range(0..len)
        };
        let patch = self.data(32 << 10);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(root.join(&rel))?;
        f.seek(SeekFrom::Start(offset as u64))?;
        f.write_all(&patch)?;
        drop(f);
        model.overwrite(&rel, offset, &patch);
        Ok(())
    }

    fn op_append(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(rel) = self.pick(&model.files()) else {
            return Ok(());
        };
        let extra = self.data(32 << 10);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(root.join(&rel))?;
        f.write_all(&extra)?;
        drop(f);
        model.append(&rel, &extra);
        Ok(())
    }

    fn op_truncate(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(rel) = self.pick(&model.files()) else {
            return Ok(());
        };
        let len = std::fs::metadata(root.join(&rel))
            .with_context(|| format!("stat {rel:?}"))?
            .len() as usize;
        let new_len = self.rng.random_range(0..=len.max(1));
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(root.join(&rel))?;
        f.set_len(new_len as u64)?;
        drop(f);
        model.truncate(&rel, new_len);
        Ok(())
    }

    fn op_mkdir(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(dir) = self.pick(&model.dirs()) else {
            return Ok(());
        };
        let rel = dir.join(self.fresh_name("d"));
        std::fs::create_dir(root.join(&rel))?;
        model.mkdir(&rel);
        Ok(())
    }

    fn op_rename(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(from) = self.pick(&model.files()) else {
            return Ok(());
        };
        let Some(dir) = self.pick(&model.dirs()) else {
            return Ok(());
        };
        let to = dir.join(self.fresh_name("r"));
        if to.starts_with(&from) {
            return Ok(());
        }
        std::fs::rename(root.join(&from), root.join(&to))?;
        model.rename(&from, &to);
        Ok(())
    }

    fn op_unlink(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(rel) = self.pick(&model.files()) else {
            return Ok(());
        };
        std::fs::remove_file(root.join(&rel))?;
        model.remove(&rel);
        Ok(())
    }

    fn op_rmdir(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let dirs: Vec<PathBuf> = model
            .dirs()
            .into_iter()
            .filter(|d| !d.as_os_str().is_empty() && model.children(d) == 0)
            .collect();
        let Some(rel) = self.pick(&dirs) else {
            return Ok(());
        };
        std::fs::remove_dir(root.join(&rel))?;
        model.remove(&rel);
        Ok(())
    }

    fn op_symlink(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(dir) = self.pick(&model.dirs()) else {
            return Ok(());
        };
        let rel = dir.join(self.fresh_name("l"));
        let target = self
            .pick(&model.files())
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "dangling".into());
        std::os::unix::fs::symlink(&target, root.join(&rel))?;
        model.symlink(&rel, &target);
        Ok(())
    }

    /// Read a random file and compare against the model immediately.
    fn op_read_check(&mut self, root: &Path, model: &mut Model) -> Result<()> {
        let Some(rel) = self.pick(&model.files()) else {
            return Ok(());
        };
        let real = std::fs::read(root.join(&rel))?;
        if let Some(crate::model::Node::File { data }) = model.nodes.get(&rel) {
            if blake3::hash(&real) != blake3::hash(data) {
                anyhow::bail!("MODEL DIVERGENCE on read of {rel:?}");
            }
        }
        Ok(())
    }
}
