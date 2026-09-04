//! Failure pack / run artifact store.

use crate::check::CheckFailure;
use crate::gen::Profile;
use crate::history::History;
use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct ConfigFile<'a> {
    run_id: &'a str,
    profile: &'a Profile,
}

pub struct RunStore {
    dir: PathBuf,
}

impl RunStore {
    pub fn create(parent: &Path, run_id: &str, profile: &Profile) -> Result<Self> {
        let dir = parent.join(run_id);
        fs::create_dir_all(&dir).with_context(|| format!("mkdir {}", dir.display()))?;
        let cfg = ConfigFile { run_id, profile };
        let mut f = File::create(dir.join("config.json"))?;
        serde_json::to_writer_pretty(&mut f, &cfg)?;
        f.write_all(b"\n")?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn write_history(&self, history: &History) -> Result<()> {
        history.write_jsonl(&self.dir.join("history.jsonl"))
    }

    pub fn write_failure(&self, failure: &CheckFailure) -> Result<()> {
        let path = self.dir.join("failure.md");
        let mut f = File::create(&path)?;
        writeln!(f, "# Chaos failure")?;
        writeln!(f)?;
        writeln!(f, "- **checker**: {}", failure.checker)?;
        writeln!(f, "- **message**: {}", failure.message)?;
        writeln!(f, "- **op_ids**: {:?}", failure.op_ids)?;
        writeln!(f)?;
        writeln!(
            f,
            "Re-check: `chaos check --history {}/history.jsonl`",
            self.dir.display()
        )?;
        Ok(())
    }

    pub fn write_success(&self) -> Result<()> {
        let mut f = File::create(self.dir.join("success"))?;
        writeln!(f, "ok")?;
        Ok(())
    }
}

pub fn new_run_id(profile_name: &str) -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{profile_name}-{secs}")
}
