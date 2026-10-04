//! Machine-readable results for `harness run` (`--results-json`) and the
//! `--shard i/n` partitioning.
//!
//! # Results file format (schema 1)
//!
//! `harness run --results-json <path> [--lane <name>] [--shard i/n]` writes
//! one JSON object after the run, whether or not scenarios failed:
//!
//! ```json
//! {
//!   "schema": 1,
//!   "lane": "linux-fuse",
//!   "s3_backend": "docker",
//!   "frontend": "fuse",
//!   "seed": 42,
//!   "shard": "2/4",
//!   "started_at": 1790000000,
//!   "scenarios": [
//!     {"name": "basic-rw", "outcome": "passed", "seconds": 12.3, "reason": null},
//!     {"name": "fio-latency", "outcome": "skipped", "seconds": 0.0,
//!      "reason": "fio not installed"},
//!     {"name": "crash-heal", "outcome": "failed", "seconds": 4.1,
//!      "reason": "expected ENOENT, got ..."}
//!   ]
//! }
//! ```
//!
//! - `schema`: integer, bumped only on an incompatible change.
//! - `lane`: lane name. Defaults to `<os>-<frontend>`, with a `-process`
//!   suffix under `--s3-backend process` (so `linux-fuse` and
//!   `linux-fuse-process` on Linux); an explicit `--lane` wins.
//! - `s3_backend` (optional, additive: schema stays 1): the S3 backend the
//!   run used, `"docker"` (floci + dockerised toxiproxy) or `"process"`
//!   (versitygw + native toxiproxy). Absent in files written before it was
//!   added; consumers must not require it.
//! - `frontend` (optional, additive): the `--frontend` the run mounted
//!   through (`"fuse"` is the only value so far).
//! - `seed`: the workload seed of the run.
//! - `shard`: the `--shard` argument verbatim (`"i/n"`), or `null` when the
//!   run was not sharded.
//! - `started_at`: Unix seconds at which the run started.
//! - `scenarios`: one entry per *selected* scenario (after name filtering and
//!   sharding), in execution order. `outcome` is `"passed"`, `"failed"` or
//!   `"skipped"`; `seconds` is wall-clock time; `reason` is the skip reason
//!   or the failure error text, `null` for a pass.
//!
//! Consumers (`tests/parity.py`) merge the files of several shards of one
//! lane by concatenating `scenarios`.

use anyhow::{bail, Result};
use serde::Serialize;

/// Current value of the top-level `schema` field.
pub const SCHEMA: u32 = 1;

/// Lane reported by [`RunResults::new`] until the caller says otherwise.
pub const DEFAULT_LANE: &str = "linux-fuse";

/// Frontends `--frontend` accepts. Only the kernel FUSE adapter exists so
/// far; NFS/WinFsp/SAF land with plans 34-36.
pub const FRONTENDS: &[&str] = &["fuse"];

/// Validate a `--frontend` value.
pub fn check_frontend(name: &str) -> Result<()> {
    if FRONTENDS.contains(&name) {
        Ok(())
    } else {
        bail!(
            "unsupported --frontend {name:?}: only {} {} supported for now",
            FRONTENDS
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", "),
            if FRONTENDS.len() == 1 { "is" } else { "are" }
        )
    }
}

/// The lane name for a run that gave no `--lane`: `<os>-<frontend>`, plus
/// `-process` when the S3 backend is the native-process one.
pub fn default_lane(frontend: &str, backend: crate::s3env::S3Backend) -> String {
    let suffix = match backend {
        crate::s3env::S3Backend::Docker => "",
        crate::s3env::S3Backend::Process => "-process",
    };
    format!("{}-{frontend}{suffix}", std::env::consts::OS)
}

/// A parsed `--shard i/n` argument (`index` is 1-based).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    pub index: usize,
    pub count: usize,
}

impl Shard {
    /// Parse `i/n` with `1 <= i <= n`.
    pub fn parse(s: &str) -> Result<Self> {
        let bad = || {
            anyhow::anyhow!(
                "invalid --shard {s:?}: expected `i/n` with 1 <= i <= n (e.g. `--shard 2/4`)"
            )
        };
        let (i, n) = s.split_once('/').ok_or_else(bad)?;
        let index: usize = i.trim().parse().map_err(|_| bad())?;
        let count: usize = n.trim().parse().map_err(|_| bad())?;
        if count == 0 || index == 0 || index > count {
            bail!("{}", bad());
        }
        Ok(Self { index, count })
    }

    /// Whether the item at position `idx` (0-based) of the selected list
    /// belongs to this shard.
    pub fn contains(&self, idx: usize) -> bool {
        idx % self.count == self.index - 1
    }

    /// Keep only this shard's items, preserving order.
    pub fn partition<T>(&self, items: Vec<T>) -> Vec<T> {
        items
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| self.contains(*idx))
            .map(|(_, t)| t)
            .collect()
    }
}

impl std::fmt::Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.index, self.count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub name: String,
    pub outcome: Outcome,
    pub seconds: f64,
    pub reason: Option<String>,
    /// What the scenario measured (`k8s-scenario`'s handoff durations),
    /// absent when it measured nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<serde_json::Value>,
}

/// The whole results file; see the module docs for the format.
#[derive(Debug, Serialize)]
pub struct RunResults {
    pub schema: u32,
    pub lane: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frontend: Option<String>,
    pub seed: u64,
    pub shard: Option<String>,
    pub started_at: u64,
    pub scenarios: Vec<ScenarioResult>,
}

impl RunResults {
    pub fn new(lane: &str, seed: u64, shard: Option<Shard>) -> Self {
        let started_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            schema: SCHEMA,
            lane: lane.to_string(),
            s3_backend: None,
            frontend: None,
            seed,
            shard: shard.map(|s| s.to_string()),
            started_at,
            scenarios: Vec::new(),
        }
    }

    /// Record the S3 backend and frontend the run used.
    pub fn with_setup(mut self, backend: crate::s3env::S3Backend, frontend: &str) -> Self {
        self.s3_backend = Some(backend.to_string());
        self.frontend = Some(frontend.to_string());
        self
    }

    pub fn push(&mut self, name: &str, outcome: Outcome, seconds: f64, reason: Option<String>) {
        self.push_measured(name, outcome, seconds, reason, None);
    }

    /// [`RunResults::push`] with what the scenario measured.
    pub fn push_measured(
        &mut self,
        name: &str,
        outcome: Outcome,
        seconds: f64,
        reason: Option<String>,
        measurements: Option<serde_json::Value>,
    ) {
        self.scenarios.push(ScenarioResult {
            name: name.to_string(),
            outcome,
            seconds,
            reason,
            measurements,
        });
    }

    /// Serialise (pretty JSON, trailing newline) and write to `path`.
    pub fn write(&self, path: &std::path::Path) -> Result<()> {
        let mut json = serde_json::to_string_pretty(self)?;
        json.push('\n');
        std::fs::write(path, json)
            .map_err(|e| anyhow::anyhow!("writing results to {}: {e}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_parses() {
        assert_eq!(Shard::parse("2/4").unwrap(), Shard { index: 2, count: 4 });
        assert_eq!(Shard::parse("1/1").unwrap(), Shard { index: 1, count: 1 });
        assert_eq!(Shard::parse("2/4").unwrap().to_string(), "2/4");
    }

    #[test]
    fn shard_rejects_bad_syntax() {
        for bad in [
            "", "3", "0/4", "5/4", "1/0", "a/b", "1/2/3", "-1/2", "/4", "2/",
        ] {
            let e = Shard::parse(bad).unwrap_err().to_string();
            assert!(e.contains("invalid --shard"), "{bad:?}: {e}");
        }
    }

    #[test]
    fn shards_partition_exactly() {
        let items: Vec<usize> = (0..10).collect();
        let mut all = Vec::new();
        for i in 1..=4 {
            let part = Shard::parse(&format!("{i}/4"))
                .unwrap()
                .partition(items.clone());
            all.extend(part);
        }
        all.sort_unstable();
        assert_eq!(all, items);
        assert_eq!(
            Shard::parse("2/4").unwrap().partition(items.clone()),
            vec![1, 5, 9]
        );
        assert_eq!(Shard::parse("1/1").unwrap().partition(items.clone()), items);
        // More shards than items: later shards are empty, not an error.
        assert!(Shard::parse("4/4")
            .unwrap()
            .partition(vec![0, 1])
            .is_empty());
    }

    #[test]
    fn json_shape() {
        let mut r = RunResults::new(DEFAULT_LANE, 7, Some(Shard::parse("1/2").unwrap()));
        r.push("a", Outcome::Passed, 1.5, None);
        r.push("b", Outcome::Skipped, 0.0, Some("fio not installed".into()));
        r.push("c", Outcome::Failed, 2.0, Some("boom".into()));
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["schema"], 1);
        assert_eq!(v["lane"], "linux-fuse");
        assert_eq!(v["seed"], 7);
        assert_eq!(v["shard"], "1/2");
        assert!(v["started_at"].as_u64().unwrap() > 1_600_000_000);
        let s = v["scenarios"].as_array().unwrap();
        assert_eq!(s.len(), 3);
        assert_eq!(s[0]["name"], "a");
        assert_eq!(s[0]["outcome"], "passed");
        assert_eq!(s[0]["seconds"], 1.5);
        assert!(s[0]["reason"].is_null());
        assert_eq!(s[1]["outcome"], "skipped");
        assert_eq!(s[1]["reason"], "fio not installed");
        assert_eq!(s[2]["outcome"], "failed");
        assert_eq!(s[2]["reason"], "boom");
    }

    #[test]
    fn records_backend_and_frontend() {
        use crate::s3env::S3Backend;
        let v = serde_json::to_value(
            RunResults::new("linux-fuse-process", 1, None).with_setup(S3Backend::Process, "fuse"),
        )
        .unwrap();
        assert_eq!(v["schema"], 1);
        assert_eq!(v["s3_backend"], "process");
        assert_eq!(v["frontend"], "fuse");
        // Additive: absent when not recorded.
        let v = serde_json::to_value(RunResults::new("x", 1, None)).unwrap();
        assert!(v.get("s3_backend").is_none() && v.get("frontend").is_none());
    }

    #[test]
    fn frontend_and_default_lane() {
        use crate::s3env::S3Backend;
        check_frontend("fuse").unwrap();
        let e = check_frontend("nfs").unwrap_err().to_string();
        assert!(e.contains("only `fuse` is supported"), "{e}");
        let os = std::env::consts::OS;
        assert_eq!(
            default_lane("fuse", S3Backend::Docker),
            format!("{os}-fuse")
        );
        assert_eq!(
            default_lane("fuse", S3Backend::Process),
            format!("{os}-fuse-process")
        );
    }

    #[test]
    fn unsharded_is_null() {
        let v = serde_json::to_value(RunResults::new("macos-fuse", 1, None)).unwrap();
        assert!(v["shard"].is_null());
        assert_eq!(v["lane"], "macos-fuse");
    }

    #[test]
    fn write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("r.json");
        let mut r = RunResults::new(DEFAULT_LANE, 42, None);
        r.push("a", Outcome::Passed, 0.1, None);
        r.write(&p).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        assert_eq!(v["scenarios"][0]["name"], "a");
    }
}
