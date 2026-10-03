//! EC2 campaign 6, finding B-1, the deadlock underneath: a FUSE reverse
//! invalidation (`FUSE_NOTIFY_INVAL_ENTRY`) takes the parent directory's
//! `i_rwsem` in the kernel, which the VFS holds for every request in
//! flight on that directory until the daemon answers it. A daemon that
//! sent one while it had such a request in flight parked its invalidation
//! thread until it answered — and, `kill -9`ed in that state, became a
//! zombie forever: the dead daemon's requests are only ended when its
//! last `/dev/fuse` descriptor closes, which needs its last thread to
//! exit, which is the one waiting. `kernel_inval` now holds a
//! notification back while a request is in flight on its inode (and
//! drops it once the TTL has done its job); the FUSE frontend writes an
//! entry invalidation only for a name its kernel can hold a valid dentry
//! for (`KernelEntries`); the daemon's zombie reaper aborts the
//! connection of a daemon left wedged anyway, and `daemon_lock::
//! abort_stale_mounts` does at the next mount.
//!
//! `fuse-inval-storm` makes the collision as likely as it gets: three
//! nodes create, rename and unlink in one shared directory at full speed,
//! so every node applies the others' ops (the holder executes the
//! forwarded ones itself) and invalidates the directory in its kernel
//! while its own workers keep requests in flight on it. Every 8th cycle a
//! worker lists the directory `ls -l` style, stat-ing every name, so for
//! an entry TTL after each listing a kernel also holds dentries for the
//! other nodes' names then present, and their changes do need entry
//! invalidations. Mid-load, the
//! lease holder is `kill -9`ed. Checks: no completed op exceeded its
//! bound, no worker hung, the killed daemon exited within 5 s (no zombie
//! with a thread in `fuse_reverse_inval_entry`), it remounts within 60 s,
//! the directory converges everywhere, and no conflict was recorded.

use super::m8::dist;
use super::m9::{cluster, unmount_all};
use super::rejoin::current_holder;
use super::{ensure_no_conflicts, eventually, wait_for_p2p};
use crate::client::Client;
use anyhow::{bail, Context, Result};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

const NAME: &str = "fuse-inval-storm";

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &u64| *v > 0)
        .unwrap_or(default)
}

/// What one worker did.
struct Report {
    node: usize,
    /// Every completed op's latency, with what it was and when it ended.
    ops: Vec<(Duration, &'static str, Instant)>,
    /// `(when, op, error)` for every failed op.
    errors: Vec<(Instant, &'static str, String)>,
}

/// The op a worker is inside right now, for the hang report.
type Current = Arc<Mutex<Option<(&'static str, PathBuf, Instant)>>>;

/// One step of a worker's cycle: its name, the path it acts on, the op.
type Step<'a> = (
    &'static str,
    &'a Path,
    Box<dyn Fn() -> std::io::Result<()> + 'a>,
);

fn timed<T>(
    current: &Current,
    what: &'static str,
    path: &Path,
    f: impl FnOnce() -> std::io::Result<T>,
) -> (Duration, std::io::Result<T>) {
    *current.lock().unwrap() = Some((what, path.to_path_buf(), Instant::now()));
    let t = Instant::now();
    let r = f();
    let took = t.elapsed();
    *current.lock().unwrap() = None;
    (took, r)
}

/// Create, write, rename, unlink — and a listing now and then — in
/// `dir`, until `stop`.
fn worker(
    node: usize,
    tag: String,
    dir: PathBuf,
    stop: Arc<AtomicBool>,
    current: Current,
    done: mpsc::Sender<Report>,
) {
    let mut report = Report {
        node,
        ops: Vec::new(),
        errors: Vec::new(),
    };
    let mut i = 0u64;
    while !stop.load(Ordering::Relaxed) {
        i += 1;
        let f = dir.join(format!("{tag}-{i}"));
        let r = dir.join(format!("{tag}-{i}.r"));
        let steps: [Step<'_>; 4] = [
            (
                "create",
                &f,
                Box::new(|| std::fs::write(&f, tag.as_bytes())),
            ),
            ("rename", &f, Box::new(|| std::fs::rename(&f, &r))),
            ("unlink", &r, Box::new(|| std::fs::remove_file(&r))),
            (
                "readdir",
                &dir,
                Box::new(|| {
                    if i.is_multiple_of(8) {
                        for e in std::fs::read_dir(&dir)? {
                            // A name gone since the listing is the storm.
                            let _ = e?.metadata();
                        }
                    }
                    Ok(())
                }),
            ),
        ];
        for (what, path, op) in steps {
            let (took, r) = timed(&current, what, path, op);
            match r {
                Ok(()) => report.ops.push((took, what, Instant::now())),
                Err(e) => {
                    report.errors.push((Instant::now(), what, e.to_string()));
                    // Do not spin on a dead mount.
                    std::thread::sleep(Duration::from_millis(100));
                    break;
                }
            }
        }
    }
    let _ = done.send(report);
}

/// Sets the flag when dropped.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn listing(dir: &Path) -> Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
        out.insert(e?.file_name().to_string_lossy().into_owned());
    }
    Ok(out)
}

pub fn fuse_inval_storm(_seed: u64) -> Result<()> {
    let rounds = env_or("INVAL_STORM_ROUNDS", 3) as usize;
    let secs = env_or("INVAL_STORM_SECS", 12);
    let workers_per_node = env_or("INVAL_STORM_WORKERS", 2) as usize;
    // A completed op may legitimately wait out a lease failover (20 s
    // TTL in this cluster) after the kill; anything longer is a stall.
    let op_bound = Duration::from_secs(env_or("INVAL_STORM_OP_BOUND_S", 45));
    let exit_bound = Duration::from_secs(5);
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[
            ("CONSTELLATION_SLOW_OP_MS", "1000"),
            ("CONSTELLATION_KERNEL_INVAL_STALL_S", "2"),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        std::fs::create_dir(clients[0].mnt.join("d"))?;
        for c in &clients {
            eventually(
                "the shared directory is visible",
                Duration::from_secs(30),
                || {
                    anyhow::ensure!(c.mnt.join("d").is_dir(), "{}: d is not there", c.name);
                    Ok(())
                },
            )?;
        }
        let mut exits = Vec::new();
        let mut reaped_rounds = 0usize;
        let mut all_ops: Vec<Duration> = Vec::new();
        let mut all_errors_elsewhere = 0usize;
        for round in 0..rounds {
            let holder = current_holder(&clients, Duration::from_secs(60))?;
            let stop = Arc::new(AtomicBool::new(false));
            // Every way out of the round stops the workers: a bail with
            // them still running kept two mounts busy, so the cleanup's
            // unmount failed and killed those daemons under load too.
            let _stop_on_exit = StopOnDrop(stop.clone());
            let (done_tx, done_rx) = mpsc::channel();
            let mut currents: Vec<(usize, Current)> = Vec::new();
            let mut n_workers = 0;
            for (i, c) in clients.iter().enumerate() {
                for w in 0..workers_per_node {
                    let current: Current = Arc::new(Mutex::new(None));
                    currents.push((i, current.clone()));
                    let dir = c.mnt.join("d");
                    let tag = format!("{}-r{round}-w{w}", c.name);
                    let stop = stop.clone();
                    let done = done_tx.clone();
                    std::thread::spawn(move || worker(i, tag, dir, stop, current, done));
                    n_workers += 1;
                }
            }
            drop(done_tx);
            std::thread::sleep(Duration::from_secs(secs / 2));
            eprintln!(
                "    {NAME}: round {round}: kill -9 the holder {} under load",
                clients[holder].name
            );
            let killed_at = Instant::now();
            let exit = clients[holder]
                .kill9_within(exit_bound)
                .with_context(|| format!("round {round}: the holder must exit, not linger"))?;
            exits.push(exit);
            // Did the daemon exit on its own, or did its reaper have to
            // abort a wedged connection? Both are within the bound; the
            // count tells how often the residual window was hit.
            let reaper_log = clients[holder].state_dir().join("reaper.log");
            let reaped = std::fs::read_to_string(&reaper_log).unwrap_or_default();
            if !reaped.trim().is_empty() {
                reaped_rounds += 1;
                eprintln!(
                    "    {NAME}: round {round}: the reaper had to release {}:\n{}",
                    clients[holder].name,
                    reaped.trim()
                );
                let _ = std::fs::remove_file(&reaper_log);
            }
            std::thread::sleep(Duration::from_secs(secs - secs / 2));
            stop.store(true, Ordering::Relaxed);
            // Every worker must come back: one that does not is inside
            // an op that hangs.
            let mut reports = Vec::new();
            let join_deadline = op_bound + Duration::from_secs(15);
            let t = Instant::now();
            while reports.len() < n_workers {
                let remaining = join_deadline.saturating_sub(t.elapsed());
                match done_rx.recv_timeout(remaining) {
                    Ok(r) => reports.push(r),
                    Err(_) => {
                        let stuck: Vec<String> = currents
                            .iter()
                            .filter_map(|(i, cur)| {
                                cur.lock().unwrap().as_ref().map(|(what, path, since)| {
                                    format!(
                                        "{}: {what} {} for {:?}",
                                        clients[*i].name,
                                        path.display(),
                                        since.elapsed()
                                    )
                                })
                            })
                            .collect();
                        bail!(
                            "round {round}: {} worker(s) did not finish within {join_deadline:?} \
                             after the stop; in flight: {stuck:?}",
                            n_workers - reports.len()
                        );
                    }
                }
            }
            let mut per_node_ops = vec![Vec::new(); clients.len()];
            let mut per_node_errors = vec![0usize; clients.len()];
            let mut over = Vec::new();
            for r in &reports {
                for (took, what, _) in &r.ops {
                    per_node_ops[r.node].push(*took);
                    if *took > op_bound {
                        over.push(format!("{}: {what} took {took:?}", clients[r.node].name));
                    }
                }
                for (when, what, e) in &r.errors {
                    // The killed node's workers fail from the kill on;
                    // that is the kill, not a finding.
                    if r.node == holder && *when >= killed_at {
                        continue;
                    }
                    per_node_errors[r.node] += 1;
                    if per_node_errors[r.node] <= 3 {
                        eprintln!(
                            "    {NAME}: round {round}: {} {what} failed: {e}",
                            clients[r.node].name
                        );
                    }
                }
            }
            if !over.is_empty() {
                bail!(
                    "round {round}: {} op(s) exceeded {op_bound:?}: {:?}",
                    over.len(),
                    &over[..over.len().min(10)]
                );
            }
            for (i, ops) in per_node_ops.iter().enumerate() {
                eprintln!(
                    "    {NAME}: round {round}: {} {} errors {}",
                    clients[i].name,
                    dist(ops.clone()),
                    per_node_errors[i]
                );
                all_ops.extend(ops.iter().copied());
                if i != holder {
                    all_errors_elsewhere += per_node_errors[i];
                }
            }
            eprintln!(
                "    {NAME}: round {round}: the holder exited {exit:?} after kill -9; remounting"
            );
            let t = Instant::now();
            clients[holder]
                .mount_within(Duration::from_secs(60))
                .with_context(|| format!("round {round}: remounting {}", clients[holder].name))?;
            let remount = t.elapsed();
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
            let mut lists = Vec::new();
            eventually(
                "the shared directory lists the same everywhere",
                Duration::from_secs(90),
                || {
                    lists = clients
                        .iter()
                        .map(|c| listing(&c.mnt.join("d")))
                        .collect::<Result<Vec<_>>>()?;
                    for (i, l) in lists.iter().enumerate().skip(1) {
                        anyhow::ensure!(
                            *l == lists[0],
                            "{} lists {} names, {} lists {}",
                            clients[i].name,
                            l.len(),
                            clients[0].name,
                            lists[0].len()
                        );
                    }
                    Ok(())
                },
            )?;
            eprintln!(
                "    {NAME}: round {round}: {} remounted in {remount:?}; the directory converged \
                 with {} leftover name(s) from the ops cut short by the kill",
                clients[holder].name,
                lists[0].len()
            );
        }
        eprintln!("    {NAME}: all completed ops {}", dist(all_ops));
        eprintln!(
            "    {NAME}: holder exit after kill -9 {}; rounds where the reaper had to release \
             it {reaped_rounds}/{rounds}; errors on surviving nodes {}",
            dist(exits),
            all_errors_elsewhere
        );
        for c in &clients {
            let log = c.log_text();
            let stalls = log
                .lines()
                .filter(|l| l.contains("blocked in the kernel"))
                .count();
            let slow = log.lines().filter(|l| l.contains("slow FUSE")).count();
            let left_behind = log.lines().filter(|l| l.contains("left behind")).count();
            eprintln!(
                "    {NAME}: {}: invalidation stall warnings {stalls}, slow FUSE ops {slow}, \
                 stale mounts aborted at takeover {left_behind}",
                c.name
            );
            for l in log
                .lines()
                .filter(|l| l.contains("blocked in the kernel"))
                .take(3)
            {
                eprintln!("    {NAME}: {}: {}", c.name, l.trim());
            }
        }
        let refs: Vec<&Client> = clients.iter().collect();
        ensure_no_conflicts(&refs)?;
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}
