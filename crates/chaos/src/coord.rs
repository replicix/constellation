//! Transport-agnostic run loop.

use crate::check::{check_history, CheckFailure};
use crate::cluster::Cluster;
use crate::gen::{Generator, Profile, Step};
use crate::history::History;
use crate::op::{Op, Outcome};
use crate::store::{self, RunStore};
use anyhow::{bail, Context, Result};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct Coordinator;

impl Coordinator {
    /// Run `profile` against `cluster`, writing artifacts under `store_dir`.
    pub fn run(cluster: &mut dyn Cluster, profile: Profile, store_dir: &Path) -> Result<()> {
        let run_id = store::new_run_id(&profile.name);
        let store = RunStore::create(store_dir, &run_id, &profile)?;
        let n = cluster.worker_count();
        anyhow::ensure!(
            n == profile.workers,
            "cluster has {n} workers but profile expects {}",
            profile.workers
        );

        cluster.prepare(&run_id, &profile.work_root)?;

        let mut gen = Generator::new(profile.clone());
        let history = Mutex::new(History::new());
        let op_ids = AtomicU64::new(1);
        let started = Instant::now();
        let deadline = profile
            .duration_secs
            .map(|s| started + Duration::from_secs(s));

        tracing::info!(
            run_id = %run_id,
            profile = %profile.name,
            workers = n,
            "chaos run starting"
        );

        let result = (|| -> Result<()> {
            while let Some(step) = gen.next_step() {
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        break;
                    }
                }
                run_step(
                    cluster,
                    &history,
                    &op_ids,
                    &step,
                    Duration::from_secs(profile.quiesce_timeout_secs),
                )?;
                tracing::info!(tag = %step.tag, elapsed_s = started.elapsed().as_secs(), "step ok");
                {
                    let h = history.lock().expect("history");
                    if let Err(e) = check_history(&h) {
                        bail!("{e}");
                    }
                }
            }
            let h = history.lock().expect("history");
            check_history(&h).map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(())
        })();

        {
            let h = history.lock().expect("history");
            store.write_history(&h)?;
        }

        match result {
            Ok(()) => {
                store.write_success()?;
                tracing::info!(run_id = %run_id, "chaos run passed");
                Ok(())
            }
            Err(e) => {
                let failure = CheckFailure {
                    checker: "run".into(),
                    message: format!("{e:#}"),
                    op_ids: vec![],
                };
                store.write_failure(&failure)?;
                bail!(
                    "chaos run failed: {e:#} (artifacts in {})",
                    store.dir().display()
                );
            }
        }
    }
}

fn next_id(op_ids: &AtomicU64) -> u64 {
    op_ids.fetch_add(1, Ordering::Relaxed)
}

fn run_step(
    cluster: &mut dyn Cluster,
    history: &Mutex<History>,
    op_ids: &AtomicU64,
    step: &Step,
    quiesce_timeout: Duration,
) -> Result<()> {
    history
        .lock()
        .expect("history")
        .record_info(format!("step:{}", step.tag));

    prep_step(cluster, history, op_ids, step)?;

    if let Some(name) = &step.start_barrier {
        cluster.barrier(name)?;
        history
            .lock()
            .expect("history")
            .record_info(format!("barrier:{name}"));
    }

    let jobs: Vec<(usize, u64, Op)> = step
        .ops
        .iter()
        .enumerate()
        .filter_map(|(w, op)| {
            op.as_ref().map(|o| {
                let id = next_id(op_ids);
                (w, id, o.clone())
            })
        })
        .collect();

    for (w, id, op) in &jobs {
        history
            .lock()
            .expect("history")
            .record_invoke(*w, *id, op.clone());
    }

    let results = cluster.invoke_parallel(&jobs);
    for r in results {
        let (w, id, complete) = r.with_context(|| format!("step {}", step.tag))?;
        if complete.outcome == Outcome::Fail {
            let name = complete.errno_name.as_deref().unwrap_or("OTHER");
            if name == "OTHER" || name == "EIO" {
                history
                    .lock()
                    .expect("history")
                    .record_complete(w, id, complete.clone());
                bail!(
                    "unexpected errno {name} on worker {w} during {}",
                    step.tag
                );
            }
        }
        history
            .lock()
            .expect("history")
            .record_complete(w, id, complete);
    }

    if let Some(name) = &step.quiesce_barrier {
        cluster.barrier(name)?;
        // TCP/local barrier only syncs chaos workers. Constellation close-to-open
        // visibility is async (log tail + FUSE), so poll until all workers agree.
        wait_converged_verify(cluster, history, op_ids, step, quiesce_timeout)?;
    }

    Ok(())
}

fn observe_key(complete: &crate::op::Complete) -> String {
    if complete.outcome == Outcome::Fail {
        return format!(
            "FAIL:{}",
            complete.errno_name.as_deref().unwrap_or("OTHER")
        );
    }
    if let Some(h) = &complete.value_hash {
        return format!("hash:{h}");
    }
    if let Some(m) = complete.mode {
        return format!("mode:{m}");
    }
    if let Some(s) = complete.size {
        return format!("size:{s}");
    }
    "ok".into()
}

/// Identity under which verify observations must agree across workers.
fn verify_group_key(op: &Op) -> String {
    match op {
        Op::Read { path } | Op::Stat { path } => path.clone(),
        Op::ReadAt { path, offset, len } => format!("read_at:{path}@{offset}+{len}"),
        other => format!("{other:?}"),
    }
}

/// Poll verify reads across all workers until every path observation agrees,
/// then record that final round into the history for checkers.
fn wait_converged_verify(
    cluster: &mut dyn Cluster,
    history: &Mutex<History>,
    op_ids: &AtomicU64,
    step: &Step,
    timeout: Duration,
) -> Result<()> {
    history
        .lock()
        .expect("history")
        .record_info(format!("quiesce_begin:{}", step.tag));

    let start = Instant::now();
    let mut attempt = 0u64;

    loop {
        attempt += 1;
        let reads: Vec<(usize, u64, Op)> = (0..cluster.worker_count())
            .flat_map(|w| {
                step.verify_reads.iter().map(move |op| {
                    let id = next_id(op_ids);
                    (w, id, op.clone())
                })
            })
            .collect();

        // Do not record intermediate polls in history (would fail convergence
        // checker mid-retry). Only the final agreeing round is recorded.
        let read_results = cluster.invoke_parallel(&reads);
        let mut collected: Vec<(usize, u64, Op, crate::op::Complete)> = Vec::new();
        for ((w, id, op), r) in reads.into_iter().zip(read_results) {
            let (rw, rid, complete) = r?;
            anyhow::ensure!(rw == w && rid == id, "verify result mismatch");
            collected.push((w, id, op, complete));
        }

        // Group by verify-op identity (not path alone): write_disjoint
        // issues several ReadAt offsets on the same file; those must
        // converge per-span across workers, not with each other.
        let mut by_op: std::collections::BTreeMap<String, Vec<(usize, String)>> =
            std::collections::BTreeMap::new();
        for (w, _id, op, complete) in &collected {
            let key = verify_group_key(op);
            by_op
                .entry(key)
                .or_default()
                .push((*w, observe_key(complete)));
        }

        let mut disagreement: Option<String> = None;
        for (op_key, entries) in &by_op {
            if entries.is_empty() {
                continue;
            }
            let first = &entries[0].1;
            if entries.iter().any(|(_, o)| o != first) {
                disagreement = Some(format!("{op_key}: {entries:?}"));
                break;
            }
        }

        if disagreement.is_none() {
            for (w, id, op, complete) in collected {
                history
                    .lock()
                    .expect("history")
                    .record_invoke(w, id, op);
                history
                    .lock()
                    .expect("history")
                    .record_complete(w, id, complete);
            }
            history.lock().expect("history").record_info(format!(
                "quiesce_end:{} attempts={attempt} elapsed_ms={}",
                step.tag,
                start.elapsed().as_millis()
            ));
            return Ok(());
        }

        let detail = disagreement.expect("disagreement set");
        if start.elapsed() > timeout {
            for (w, id, op, complete) in collected {
                history
                    .lock()
                    .expect("history")
                    .record_invoke(w, id, op);
                history
                    .lock()
                    .expect("history")
                    .record_complete(w, id, complete);
            }
            history.lock().expect("history").record_info(format!(
                "quiesce_end:{} attempts={attempt} TIMEOUT",
                step.tag
            ));
            bail!(
                "convergence not reached within {timeout:?} after {}: {detail}",
                step.tag
            );
        }

        if attempt == 1 || attempt % 8 == 0 {
            tracing::debug!(
                tag = %step.tag,
                attempt,
                elapsed_ms = start.elapsed().as_millis(),
                %detail,
                "waiting for cross-node convergence"
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn prep_step(
    cluster: &mut dyn Cluster,
    history: &Mutex<History>,
    op_ids: &AtomicU64,
    step: &Step,
) -> Result<()> {
    let work = extract_work_root(step);
    if step.tag.starts_with("unlink_storm:") {
        let name = step.tag.strip_prefix("unlink_storm:").unwrap_or("");
        let path = format!("{work}/{name}");
        seed_create(cluster, history, op_ids, &path)?;
    } else if step.tag.starts_with("rmdir_storm:") {
        let name = step.tag.strip_prefix("rmdir_storm:").unwrap_or("");
        let path = format!("{work}/{name}");
        let id = next_id(op_ids);
        let op = Op::Mkdir { path };
        history
            .lock()
            .expect("history")
            .record_invoke(0, id, op.clone());
        let c = cluster.invoke(0, id, &op)?;
        history.lock().expect("history").record_complete(0, id, c);
    } else if step.tag.starts_with("rename_storm:") {
        let id_str = step.tag.strip_prefix("rename_storm:").unwrap_or("");
        let path = format!("{work}/rs{id_str}");
        seed_create(cluster, history, op_ids, &path)?;
    } else if step.tag.starts_with("chmod_duel:") {
        let name = step.tag.strip_prefix("chmod_duel:").unwrap_or("");
        let path = format!("{work}/{name}");
        seed_create(cluster, history, op_ids, &path)?;
    } else if step.tag.starts_with("write_overlap:")
        || step.tag.starts_with("write_disjoint:")
        || step.tag.starts_with("append_duel:")
        || step.tag.starts_with("truncate_duel:")
    {
        let name = step.tag.split(':').nth(1).unwrap_or("x");
        let path = format!("{work}/{name}");
        let id = next_id(op_ids);
        let content = vec![0u8; 512];
        let op = Op::WriteFull { path, content };
        history
            .lock()
            .expect("history")
            .record_invoke(0, id, op.clone());
        let c = cluster.invoke(0, id, &op)?;
        history.lock().expect("history").record_complete(0, id, c);
    }
    Ok(())
}

fn extract_work_root(step: &Step) -> String {
    for op in step.ops.iter().flatten() {
        let path = match op {
            Op::Create { path, .. }
            | Op::Mkdir { path }
            | Op::Unlink { path }
            | Op::Rmdir { path }
            | Op::WriteFull { path, .. }
            | Op::WriteAt { path, .. }
            | Op::Append { path, .. }
            | Op::Truncate { path, .. }
            | Op::Chmod { path, .. }
            | Op::Read { path }
            | Op::ReadAt { path, .. }
            | Op::Stat { path } => path.as_str(),
            Op::Rename { from, .. } => from.as_str(),
        };
        if let Some((root, _)) = path.split_once('/') {
            return root.to_string();
        }
    }
    for op in &step.verify_reads {
        if let Op::Read { path } | Op::ReadAt { path, .. } | Op::Stat { path } = op {
            if let Some((root, _)) = path.split_once('/') {
                return root.to_string();
            }
        }
    }
    "chaos".into()
}

fn seed_create(
    cluster: &mut dyn Cluster,
    history: &Mutex<History>,
    op_ids: &AtomicU64,
    path: &str,
) -> Result<()> {
    let id = next_id(op_ids);
    let op = Op::Create {
        path: path.to_string(),
        content: b"seed".to_vec(),
    };
    history
        .lock()
        .expect("history")
        .record_invoke(0, id, op.clone());
    let c = cluster.invoke(0, id, &op)?;
    // If already exists from a prior round, treat as ok enough to continue.
    if c.outcome == Outcome::Fail
        && c.errno_name.as_deref() == Some("EEXIST")
    {
        let id2 = next_id(op_ids);
        let op2 = Op::WriteFull {
            path: path.to_string(),
            content: b"seed".to_vec(),
        };
        history
            .lock()
            .expect("history")
            .record_complete(0, id, c);
        history
            .lock()
            .expect("history")
            .record_invoke(0, id2, op2.clone());
        let c2 = cluster.invoke(0, id2, &op2)?;
        history
            .lock()
            .expect("history")
            .record_complete(0, id2, c2);
        return Ok(());
    }
    history.lock().expect("history").record_complete(0, id, c);
    Ok(())
}
