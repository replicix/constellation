//! Seeded workload profiles and conflict generators.

use crate::op::Op;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// Which Tier-A scenario families to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioSet {
    Namespace,
    Data,
    Cto,
    #[default]
    All,
}

impl ScenarioSet {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "namespace" => Ok(Self::Namespace),
            "data" => Ok(Self::Data),
            "cto" => Ok(Self::Cto),
            "all" => Ok(Self::All),
            other => anyhow::bail!("unknown scenario set: {other} (namespace|data|cto|all)"),
        }
    }
}

/// Run knobs shared by CI and soak.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub name: String,
    pub seed: u64,
    /// Number of storm/duel rounds (CI) or ignored when duration_secs set.
    pub rounds: usize,
    /// Optional wall-clock budget for soak (seconds).
    pub duration_secs: Option<u64>,
    pub workers: usize,
    pub scenarios: ScenarioSet,
    /// Relative work root under each mount.
    pub work_root: String,
    /// Include multi-chunk (~1 MiB+) writes in data scenarios.
    pub multi_chunk: bool,
    /// How long to poll for cross-node close-to-open convergence after a
    /// quiesce barrier (Constellation log/gossip lag is async).
    pub quiesce_timeout_secs: u64,
}

impl Profile {
    pub fn ci(seed: u64, workers: usize) -> Self {
        Self {
            name: "ci".into(),
            seed,
            rounds: 8,
            duration_secs: None,
            workers,
            scenarios: ScenarioSet::All,
            work_root: "chaos-ci".into(),
            multi_chunk: false,
            quiesce_timeout_secs: 30,
        }
    }

    pub fn soak(seed: u64, workers: usize, duration_secs: u64) -> Self {
        Self {
            name: "soak".into(),
            seed,
            rounds: usize::MAX / 4,
            duration_secs: Some(duration_secs),
            workers,
            scenarios: ScenarioSet::All,
            work_root: "chaos-soak".into(),
            multi_chunk: true,
            quiesce_timeout_secs: 60,
        }
    }
}

/// One coordinated step: optionally a barrier name, then parallel ops per worker.
#[derive(Debug, Clone)]
pub struct Step {
    /// If set, workers barrier before invoking their ops.
    pub start_barrier: Option<String>,
    /// Per-worker op (None = idle this step).
    pub ops: Vec<Option<Op>>,
    /// Barrier after completions, then optional verify reads on every worker.
    pub quiesce_barrier: Option<String>,
    pub verify_reads: Vec<Op>,
    /// Tag for checkers (e.g. "create_storm:f0").
    pub tag: String,
}

pub struct Generator {
    rng: StdRng,
    profile: Profile,
    counter: u64,
    round: usize,
}

impl Generator {
    pub fn new(profile: Profile) -> Self {
        let rng = StdRng::seed_from_u64(profile.seed);
        Self {
            rng,
            profile,
            counter: 0,
            round: 0,
        }
    }

    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    pub fn round(&self) -> usize {
        self.round
    }

    fn next_id(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }

    fn unique_payload(&mut self, tag: &str, len: usize) -> Vec<u8> {
        let id = self.next_id();
        let mut v = format!("{tag}:{id}:").into_bytes();
        while v.len() < len {
            v.push(self.rng.random());
        }
        v.truncate(len);
        v
    }

    fn path(&self, name: &str) -> String {
        format!("{}/{}", self.profile.work_root, name)
    }

    /// Produce the next step, or `None` when the profile budget is exhausted.
    pub fn next_step(&mut self) -> Option<Step> {
        if self.round >= self.profile.rounds {
            return None;
        }
        let n = self.profile.workers;
        let families: &[&str] = match self.profile.scenarios {
            ScenarioSet::Namespace => &["create", "mkdir", "unlink", "rmdir", "rename"],
            ScenarioSet::Data => &["write_full", "write_overlap", "write_disjoint", "append", "truncate"],
            ScenarioSet::Cto => &["cto"],
            ScenarioSet::All => &[
                "create",
                "mkdir",
                "unlink",
                "rmdir",
                "rename",
                "write_full",
                "write_overlap",
                "write_disjoint",
                "append",
                "truncate",
                "chmod",
                "cto",
            ],
        };
        let family = families[self.rng.random_range(0..families.len())];
        let step = match family {
            "create" => self.storm_create(n),
            "mkdir" => self.storm_mkdir(n),
            "unlink" => self.storm_unlink(n),
            "rmdir" => self.storm_rmdir(n),
            "rename" => self.storm_rename(n),
            "write_full" => self.duel_write_full(n),
            "write_overlap" => self.duel_write_overlap(n),
            "write_disjoint" => self.duel_write_disjoint(n),
            "append" => self.duel_append(n),
            "truncate" => self.duel_truncate(n),
            "chmod" => self.duel_chmod(n),
            "cto" => self.cto(n),
            _ => self.storm_create(n),
        };
        self.round += 1;
        Some(step)
    }

    fn storm_create(&mut self, n: usize) -> Step {
        let name = format!("c{}", self.next_id());
        let path = self.path(&name);
        let mut ops = Vec::with_capacity(n);
        for i in 0..n {
            let content = self.unique_payload(&format!("w{i}"), 64);
            ops.push(Some(Op::Create {
                path: path.clone(),
                content,
            }));
        }
        Step {
            start_barrier: Some(format!("create-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("create-q-{}", self.round)),
            verify_reads: vec![Op::Read { path: path.clone() }],
            tag: format!("create_storm:{name}"),
        }
    }

    fn storm_mkdir(&mut self, n: usize) -> Step {
        let name = format!("d{}", self.next_id());
        let path = self.path(&name);
        let ops = (0..n)
            .map(|_| Some(Op::Mkdir { path: path.clone() }))
            .collect();
        Step {
            start_barrier: Some(format!("mkdir-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("mkdir-q-{}", self.round)),
            verify_reads: vec![Op::Stat { path: path.clone() }],
            tag: format!("mkdir_storm:{name}"),
        }
    }

    fn storm_unlink(&mut self, n: usize) -> Step {
        let name = format!("u{}", self.next_id());
        let path = self.path(&name);
        // Seed file is created by the coordinator before the storm (see tag).
        let ops = (0..n)
            .map(|_| Some(Op::Unlink { path: path.clone() }))
            .collect();
        Step {
            start_barrier: Some(format!("unlink-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("unlink-q-{}", self.round)),
            verify_reads: vec![Op::Stat { path: path.clone() }],
            tag: format!("unlink_storm:{name}"),
        }
    }

    fn storm_rmdir(&mut self, n: usize) -> Step {
        let name = format!("rd{}", self.next_id());
        let path = self.path(&name);
        let ops = (0..n)
            .map(|_| Some(Op::Rmdir { path: path.clone() }))
            .collect();
        Step {
            start_barrier: Some(format!("rmdir-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("rmdir-q-{}", self.round)),
            verify_reads: vec![Op::Stat { path: path.clone() }],
            tag: format!("rmdir_storm:{name}"),
        }
    }

    fn storm_rename(&mut self, n: usize) -> Step {
        let id = self.next_id();
        let src = self.path(&format!("rs{id}"));
        let ops: Vec<_> = (0..n)
            .map(|i| {
                Some(Op::Rename {
                    from: src.clone(),
                    to: self.path(&format!("rd{id}_{i}")),
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("rename-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("rename-q-{}", self.round)),
            verify_reads: (0..n)
                .map(|i| Op::Stat {
                    path: self.path(&format!("rd{id}_{i}")),
                })
                .collect(),
            tag: format!("rename_storm:{id}"),
        }
    }

    fn duel_write_full(&mut self, n: usize) -> Step {
        let name = format!("wf{}", self.next_id());
        let path = self.path(&name);
        let len = if self.profile.multi_chunk && self.rng.random_ratio(1, 4) {
            1 << 20
        } else {
            256
        };
        let ops: Vec<_> = (0..n)
            .map(|i| {
                let content = self.unique_payload(&format!("wf{i}"), len);
                Some(Op::WriteFull {
                    path: path.clone(),
                    content,
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("wf-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("wf-q-{}", self.round)),
            verify_reads: vec![Op::Read { path }],
            tag: format!("write_full_duel:{name}"),
        }
    }

    fn duel_write_overlap(&mut self, n: usize) -> Step {
        let name = format!("wo{}", self.next_id());
        let path = self.path(&name);
        let patch_len = 64usize;
        let ops: Vec<_> = (0..n)
            .map(|i| {
                let patch = self.unique_payload(&format!("wo{i}"), patch_len);
                Some(Op::WriteAt {
                    path: path.clone(),
                    offset: 0,
                    patch,
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("wo-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("wo-q-{}", self.round)),
            verify_reads: vec![Op::ReadAt {
                path,
                offset: 0,
                len: patch_len as u64,
            }],
            tag: format!("write_overlap:{name}"),
        }
    }

    fn duel_write_disjoint(&mut self, n: usize) -> Step {
        let name = format!("wd{}", self.next_id());
        let path = self.path(&name);
        let patch_len = 32usize;
        let ops: Vec<_> = (0..n)
            .map(|i| {
                let patch = self.unique_payload(&format!("wd{i}"), patch_len);
                Some(Op::WriteAt {
                    path: path.clone(),
                    offset: (i * patch_len) as u64,
                    patch,
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("wd-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("wd-q-{}", self.round)),
            verify_reads: (0..n)
                .map(|i| Op::ReadAt {
                    path: path.clone(),
                    offset: (i * patch_len) as u64,
                    len: patch_len as u64,
                })
                .collect(),
            tag: format!("write_disjoint:{name}"),
        }
    }

    fn duel_append(&mut self, n: usize) -> Step {
        let name = format!("ap{}", self.next_id());
        let path = self.path(&name);
        let ops: Vec<_> = (0..n)
            .map(|i| {
                let data = self.unique_payload(&format!("ap{i}"), 48);
                Some(Op::Append {
                    path: path.clone(),
                    data,
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("ap-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("ap-q-{}", self.round)),
            verify_reads: vec![Op::Read { path }],
            tag: format!("append_duel:{name}"),
        }
    }

    fn duel_truncate(&mut self, n: usize) -> Step {
        let name = format!("tr{}", self.next_id());
        let path = self.path(&name);
        let ops: Vec<_> = (0..n)
            .map(|i| {
                Some(Op::Truncate {
                    path: path.clone(),
                    size: 16 * (i as u64 + 1),
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("tr-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("tr-q-{}", self.round)),
            verify_reads: vec![Op::Stat { path }],
            tag: format!("truncate_duel:{name}"),
        }
    }

    fn duel_chmod(&mut self, n: usize) -> Step {
        let name = format!("cm{}", self.next_id());
        let path = self.path(&name);
        let modes = [0o644, 0o600, 0o755, 0o640];
        let ops: Vec<_> = (0..n)
            .map(|i| {
                Some(Op::Chmod {
                    path: path.clone(),
                    mode: modes[i % modes.len()],
                })
            })
            .collect();
        Step {
            start_barrier: Some(format!("cm-start-{}", self.round)),
            ops,
            quiesce_barrier: Some(format!("cm-q-{}", self.round)),
            verify_reads: vec![Op::Stat { path }],
            tag: format!("chmod_duel:{name}"),
        }
    }

    fn cto(&mut self, n: usize) -> Step {
        let name = format!("cto{}", self.next_id());
        let path = self.path(&name);
        let content = self.unique_payload("cto", 128);
        let mut ops: Vec<Option<Op>> = vec![Some(Op::WriteFull {
            path: path.clone(),
            content: content.clone(),
        })];
        while ops.len() < n {
            ops.push(None);
        }
        Step {
            start_barrier: None,
            ops,
            quiesce_barrier: Some(format!("cto-q-{}", self.round)),
            verify_reads: vec![Op::Read { path }],
            tag: format!("cto:{name}"),
        }
    }
}
