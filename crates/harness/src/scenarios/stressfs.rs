//! `stress-ng-fs`: every applicable stress-ng filesystem stressor at once,
//! with `--verify`, against live mounts — the lane that covers what the
//! five-stressor `stress-ng-flap` churn never touched: cluster locks and
//! their owner fencing (`flock lockf lockofd locka lockmix lease fcntl`),
//! the data path with verification (`hdd iomix fallocate fpunch filehole
//! fsize sync-file pseek`), create/rename/unlink races (`filerace metamix
//! link unlink rename dirmany dirdeep`), setattr forwarding (`xattr chmod
//! chown utime touch access`) and handle lifetimes (`dup copy-file open`).
//! (`getdent` and `fd-fork` never touch the mount: excluded, like the
//! other stressors `tests/stress-ng-exclude.txt` gives evidence for;
//! `fstat`, which stats `/dev` by default, is pointed at a directory of
//! files on the mount, [`FSTAT_DIR`]. Stressors that only skip here —
//! `acl chattr fiemap verity` — still run, so that one that starts to
//! pass shows in the table.)
//!
//! Three scenarios (`stress-ng-fs-nodes` and `stress-ng-fs-faults` among
//! the known-bug reproductions, `KNOWN_BUG_REPROS`, until the bugs they
//! find are fixed):
//!
//! - `stress-ng-fs`: one node, one mount, one stress-ng invocation running
//!   every stressor of `stress-ng --class filesystem?` that
//!   `tests/stress-ng-exclude.txt` does not exclude, one instance each, for
//!   `STRESS_NG_FS_SECS` (120 s). Accounting snapshots are on, a few
//!   closed multi-chunk files are written first (so the snapshots hold
//!   data), and two snapshots of `/` are taken mid-run.
//! - `stress-ng-fs-nodes`: the same on three nodes of one filesystem at
//!   once, each in its own directory, with P2P (own node keys): the
//!   non-holders forward every mutation to the lease holder, and every
//!   lock is a cluster grant. The mid-run snapshots come from `n1`, a
//!   non-holder, so they are forwarded batches.
//! - `stress-ng-fs-faults`: the single-node run with 40 ± 20 ms of S3
//!   latency and a 1.5 s S3 cut every few seconds (the `stress-ng-flap`
//!   idiom); no snapshots (a cut may legitimately fail one).
//!
//! The exclude list's `name@root` entries apply to runs as root only (a
//! stressor that, privileged, acts on the whole host: `iomix` drops its
//! page caches).
//!
//! Pass criteria, all checked:
//!
//! - stress-ng's own verdicts (its `passed:`/`failed:`/`skipped:` summary
//!   lines, per stressor — not the exit code): a failure is allowed only
//!   for a stressor listed in `tests/stress-ng-baseline.txt`, and never one
//!   whose log mentions verification, a mismatch or corruption (data is
//!   never baselined). A selected stressor with no verdict at all (killed,
//!   or the run did not finish) fails. A baselined stressor that passes is
//!   reported as an improvement to remove from the baseline (the xfstests
//!   runner's two-way rule).
//! - stress-ng finishes within its timeout plus [`GRACE`]; otherwise the
//!   stuck processes are listed with their `wchan` (a FUSE request the
//!   daemon never answers shows as `request_wait_answer`), killed, and the
//!   mount's FUSE connection aborted if they are still there.
//! - The daemons logged no panic; outside the faults scenario, no `ERROR`
//!   line either. No FUSE request other than a blocking lock went
//!   unanswered past the watchdog threshold (`status.fuse_requests`), and
//!   on a ring none held there either, nor a reply flushed only by a ring
//!   thread's bounded wait (`ring_entries_held_long`,
//!   `ring_stranded_commits` per mount). No
//!   cluster lock grant lapsed and nothing was fenced for it
//!   (`status.locks`: `lost`, `fenced_io`, `owners_fenced`,
//!   `owner_fenced_ops` all 0) — stress-ng tolerates some of the `EIO`s a
//!   lapse causes.
//! - Afterwards every mount answers `stat` and `ls`, and a 1 MiB
//!   write + fsync + read round trip; on the multi-node run every node
//!   also reads every other node's file.
//! - The upload spool drains (journal backlog and pending uploads to 0,
//!   progress-based like `stress-ng-flap`).
//! - With snapshots on, `snapshot space --verify` reports no mismatch.
//! - No kernel message since the start names stress-ng or a hung task;
//!   other FUSE messages are printed (another agent's mount may have
//!   caused them). Read with `dmesg`, else `journalctl -k`; neither
//!   readable is said and skipped.
//!
//! Knobs: `STRESS_NG_FS_SECS` (120), `STRESS_NG_FS_INSTANCES` (1, per
//! stressor), `STRESS_NG_FS_ONLY` (comma-separated stressors to run instead
//! of the full list: one stressor through the whole harness, excluded ones
//! included), `STRESS_NG_FS_ARGS` (extra stress-ng arguments),
//! `STRESS_NG_FS_EXCLUDE`/`STRESS_NG_FS_BASELINE` (list files instead of
//! the ones compiled in), `STRESS_NG_FS_REPORT` (append the per-stressor
//! table as TSV: scenario, transport, node, stressor, verdict, bogo-ops),
//! `STRESS_NG_FS_TMP` (where the scenario's mounts and caches live,
//! default `/var/tmp`: the 1 GiB stressors do not fit a tmpfs `/tmp`),
//! `STRESS_NG_FS_HANG_HOOK` (a shell command run at a hang before anything
//! is killed, with `HANG_NODE`, `HANG_PGID`, `HANG_DAEMON_PID` and
//! `HANG_FUSE_CONN` set: for kernel stacks and the connection's `waiting`
//! count while the requests are still stuck).
//!
//! The stressors' file sizes are capped ([`SIZE_ARGS`]) so that one run
//! writes a few hundred MiB, not several GiB, and the spool drains in
//! bounded time against floci.

use super::snapacct::verify_clean;
use super::{eventually, setup_in, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use crate::spawn::TiedSpawn;
use anyhow::{bail, ensure, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Stressors that do not apply here, with a reason each.
const EXCLUDE: &str = include_str!("../../../../tests/stress-ng-exclude.txt");
/// Reproducible failures that are known semantic gaps, with a reason each.
const BASELINE: &str = include_str!("../../../../tests/stress-ng-baseline.txt");

/// Stressors the exclude/baseline lists reference that some installed
/// stress-ng does not compile in yet (e.g. `statmount`, over the Linux 6.8
/// `statmount`(2)/`listmount`(2) syscalls: an older build's package predates
/// them). Absent from `--class filesystem?` only for that reason, not
/// because the name is a mistake — the stale check below tolerates exactly
/// these, and nothing else, so a real typo still fails it. Add a name here
/// only after checking it against the stress-ng source (not merely because
/// a CI run reported it missing): the point is telling a version gap apart
/// from a mistake, not widening what the check accepts.
const VERSION_GATED: &[&str] = &["rofs", "statmount"];

/// How long past its own `--timeout` stress-ng may take to stop its
/// stressors and report. Generous: a stressor ends its current op first,
/// and `copy-file`/`hdd` close (flush) large files.
const GRACE: Duration = Duration::from_secs(120);

/// Per-stressor file-size caps (the defaults are 1 GiB for `hdd`,
/// `iomix`, `fallocate`, `sync-file` and 256 MiB for `copy-file`, whose
/// floor is 128 MiB).
const SIZE_ARGS: &[&str] = &[
    "--hdd-bytes",
    "128M",
    "--iomix-bytes",
    "128M",
    "--fallocate-bytes",
    "128M",
    "--sync-file-bytes",
    "128M",
    // Its floor.
    "--copy-file-bytes",
    "128M",
];

/// `fstat`'s `--fstat-dir`, under the run's own directory on the mount,
/// and how many files [`start`] puts in it first (the stressor stats what
/// it finds there, from several threads).
const FSTAT_DIR: &str = "fstat-dir";
const FSTAT_FILES: usize = 32;

/// Words in a failed stressor's log lines that make it a data failure,
/// which no baseline entry covers.
const DATA_WORDS: &[&str] = &["verif", "mismatch", "corrupt", "checksum"];

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// A list file (`name # REASON: why`): name → reason. Every entry must
/// give a reason.
fn parse_list(text: &str) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, reason) = line
            .split_once('#')
            .map(|(a, b)| (a.trim(), b.trim()))
            .unwrap_or((line, ""));
        let reason = reason.strip_prefix("REASON:").unwrap_or(reason).trim();
        ensure!(
            !name.is_empty() && !name.contains(char::is_whitespace),
            "line {}: {line:?} is not `stressor # REASON: ...`",
            n + 1
        );
        ensure!(!reason.is_empty(), "line {}: {name} has no reason", n + 1);
        ensure!(
            out.insert(name.to_string(), reason.to_string()).is_none(),
            "line {}: {name} listed twice",
            n + 1
        );
    }
    Ok(out)
}

fn list(env: &str, compiled: &str) -> Result<BTreeMap<String, String>> {
    match std::env::var_os(env) {
        Some(path) => {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("{env}={}", Path::new(&path).display()))?;
            parse_list(&text).with_context(|| format!("{env}={}", Path::new(&path).display()))
        }
        None => parse_list(compiled).with_context(|| format!("the compiled-in {env} list")),
    }
}

/// `stress-ng --class filesystem?`: `class 'filesystem' stressors: a b c`.
fn filesystem_class() -> Result<BTreeSet<String>> {
    let out = Command::new("stress-ng")
        .args(["--class", "filesystem?"])
        .output()
        .context("stress-ng --class filesystem?")?;
    let text =
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr);
    parse_class(&text).with_context(|| format!("stress-ng --class filesystem? said: {text}"))
}

fn parse_class(text: &str) -> Result<BTreeSet<String>> {
    let (_, names) = text
        .split_once("stressors:")
        .context("no `stressors:` in the class listing")?;
    let set: BTreeSet<String> = names.split_whitespace().map(str::to_string).collect();
    ensure!(!set.is_empty(), "an empty filesystem class");
    Ok(set)
}

/// What one run is configured with.
struct Plan {
    stressors: Vec<String>,
    instances: u32,
    secs: u64,
    extra: Vec<String>,
    baseline: BTreeMap<String, String>,
}

/// The exclusions that apply to a run as `root` or not: `name@root`
/// entries only for root (what a stressor does with privileges on a
/// shared host — `iomix` drops the host's page caches).
fn effective_exclude(listed: &BTreeMap<String, String>, root: bool) -> BTreeMap<String, String> {
    listed
        .iter()
        .filter_map(|(name, reason)| match name.strip_suffix("@root") {
            Some(base) => root.then(|| (base.to_string(), reason.clone())),
            None => Some((name.clone(), reason.clone())),
        })
        .collect()
}

/// Names in `listed` (exclude list) or `baseline` that `class` (this
/// stress-ng's `--class filesystem?`) does not have: split into genuinely
/// stale entries (a mistake, must fail) and ones [`VERSION_GATED`] excuses
/// (this stress-ng predates them, not a mistake).
fn missing_from_class<'a>(
    listed: &'a BTreeMap<String, String>,
    baseline: &'a BTreeMap<String, String>,
    class: &BTreeSet<String>,
) -> (Vec<&'a str>, Vec<&'a str>) {
    let names = listed
        .keys()
        .map(|s| s.strip_suffix("@root").unwrap_or(s))
        .chain(baseline.keys().map(String::as_str))
        .filter(|s| !class.contains(*s));
    let mut stale = Vec::new();
    let mut gated = Vec::new();
    for s in names {
        if VERSION_GATED.contains(&s) {
            gated.push(s);
        } else {
            stale.push(s);
        }
    }
    (stale, gated)
}

fn plan() -> Result<Plan> {
    let listed = list("STRESS_NG_FS_EXCLUDE", EXCLUDE)?;
    // SAFETY: geteuid(2) cannot fail.
    let root = unsafe { libc::geteuid() } == 0;
    let baseline = list("STRESS_NG_FS_BASELINE", BASELINE)?;
    let class = filesystem_class()?;
    let (stale, gated) = missing_from_class(&listed, &baseline, &class);
    ensure!(
        stale.is_empty(),
        "listed stressors this stress-ng does not have (stale list entries): {stale:?}"
    );
    if !gated.is_empty() {
        eprintln!(
            "    stress-ng-fs: this stress-ng does not have {gated:?} yet (version-gated, not a \
             stale entry)"
        );
    }
    let both: Vec<&String> = listed
        .keys()
        .filter(|s| baseline.contains_key(s.strip_suffix("@root").unwrap_or(s)))
        .collect();
    ensure!(both.is_empty(), "both excluded and baselined: {both:?}");
    let exclude = effective_exclude(&listed, root);
    let stressors: Vec<String> = match std::env::var("STRESS_NG_FS_ONLY") {
        Ok(only) if !only.trim().is_empty() => {
            let only: Vec<String> = only
                .split([',', ' '])
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            let unknown: Vec<&String> = only.iter().filter(|s| !class.contains(*s)).collect();
            ensure!(unknown.is_empty(), "STRESS_NG_FS_ONLY: unknown {unknown:?}");
            only
        }
        _ => class
            .into_iter()
            .filter(|s| !exclude.contains_key(s))
            .collect(),
    };
    let extra = std::env::var("STRESS_NG_FS_ARGS")
        .map(|a| a.split_whitespace().map(str::to_string).collect())
        .unwrap_or_default();
    Ok(Plan {
        stressors,
        instances: env_or("STRESS_NG_FS_INSTANCES", 1u32).max(1),
        secs: env_or("STRESS_NG_FS_SECS", 120u64).max(5),
        extra,
        baseline,
    })
}

/// stress-ng's verdicts, from its log's summary lines
/// (`passed: 2: dentry (1) dir (1)`), and each stressor's other lines.
#[derive(Debug, Default, PartialEq)]
struct Verdicts {
    passed: BTreeSet<String>,
    failed: BTreeSet<String>,
    skipped: BTreeSet<String>,
    untrustworthy: BTreeSet<String>,
    /// Lines a stressor logged at `fail:`/`error:`/`warn:` level, by
    /// stressor.
    complaints: BTreeMap<String, Vec<String>>,
}

fn parse_verdicts(log: &str) -> Verdicts {
    let mut v = Verdicts::default();
    for line in log.lines() {
        // `stress-ng: info:  [pid] passed: 1: xattr (1)`
        let Some((head, rest)) = line.split_once("] ") else {
            continue;
        };
        let rest = rest.trim();
        let summary = [
            ("passed:", &mut v.passed),
            ("failed:", &mut v.failed),
            ("skipped:", &mut v.skipped),
            ("metrics untrustworthy:", &mut v.untrustworthy),
        ];
        let mut was_summary = false;
        for (tag, set) in summary {
            if let Some(tail) = rest.strip_prefix(tag) {
                was_summary = true;
                // `N: a (1) b (2)` or just `0`.
                if let Some((_, names)) = tail.split_once(':') {
                    set.extend(
                        names
                            .split_whitespace()
                            .filter(|w| !w.starts_with('('))
                            .map(str::to_string),
                    );
                }
            }
        }
        if was_summary {
            continue;
        }
        let level = ["fail:", "error:", "warn:"]
            .into_iter()
            .any(|l| head.contains(l));
        if !level {
            continue;
        }
        // `stress-ng: fail:  [pid] hdd: ...` or, from the parent,
        // `stress-ng: error: [pid] filename: [pid] terminated ...`.
        if let Some((name, _)) = rest.split_once(':') {
            if !name.contains(char::is_whitespace) {
                v.complaints
                    .entry(name.to_string())
                    .or_default()
                    .push(line.to_string());
            }
        }
    }
    // A stressor with a failed instance failed, whatever the others did.
    v.passed.retain(|s| !v.failed.contains(s));
    v
}

/// `bogo-ops` per stressor from stress-ng's `--yaml` file (`- stressor:
/// x` followed by its `bogo-ops: n`).
fn parse_bogo_ops(yaml: &str) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in yaml.lines() {
        let t = line.trim();
        if let Some(name) = t.strip_prefix("- stressor:") {
            current = Some(name.trim().trim_matches('\'').to_string());
        } else if let Some(n) = t.strip_prefix("bogo-ops:") {
            if let (Some(name), Ok(n)) = (current.take(), n.trim().parse()) {
                out.insert(name, n);
            }
        }
    }
    out
}

/// The outcome of one stressor of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Passed,
    Skipped,
    /// Failed, and the baseline lists it (not a data failure).
    Baselined,
    Failed(String),
}

/// The run's verdict per selected stressor, and the improvements (baseline
/// entries that passed).
fn judge(
    selected: &[String],
    v: &Verdicts,
    baseline: &BTreeMap<String, String>,
) -> (BTreeMap<String, Outcome>, Vec<String>) {
    let mut out = BTreeMap::new();
    let mut improvements = Vec::new();
    for s in selected {
        let complaints = v.complaints.get(s).cloned().unwrap_or_default();
        let data = complaints.iter().find(|l| {
            let l = l.to_ascii_lowercase();
            DATA_WORDS.iter().any(|w| l.contains(w))
        });
        let outcome = if v.failed.contains(s) {
            match (data, baseline.contains_key(s)) {
                (Some(line), _) => {
                    Outcome::Failed(format!("data failure (never baselined): {line}"))
                }
                (None, true) => Outcome::Baselined,
                (None, false) => Outcome::Failed(
                    complaints
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "failed".into()),
                ),
            }
        } else if v.passed.contains(s) {
            if baseline.contains_key(s) {
                improvements.push(s.clone());
            }
            Outcome::Passed
        } else if v.skipped.contains(s) {
            Outcome::Skipped
        } else {
            Outcome::Failed("no verdict from stress-ng (killed, or the run did not finish)".into())
        };
        out.insert(s.clone(), outcome);
    }
    (out, improvements)
}

/// One stress-ng invocation on one mount.
struct Run {
    node: String,
    child: Child,
    started: Instant,
    deadline: Instant,
    log: PathBuf,
    yaml: PathBuf,
    out: PathBuf,
}

fn start(plan: &Plan, node: &str, dir: &Path, work: &Path, seed: u64) -> Result<Run> {
    std::fs::create_dir_all(dir)?;
    let log = work.join(format!("stress-ng-{node}.log"));
    let yaml = work.join(format!("stress-ng-{node}.yaml"));
    let out = work.join(format!("stress-ng-{node}.out"));
    let mut cmd = Command::new("stress-ng");
    for s in &plan.stressors {
        cmd.arg(format!("--{s}")).arg(plan.instances.to_string());
    }
    if plan.stressors.iter().any(|s| s == "fstat")
        && !plan.extra.iter().any(|a| a.starts_with("--fstat-dir"))
    {
        let fstat = dir.join(FSTAT_DIR);
        std::fs::create_dir_all(&fstat)?;
        for i in 0..FSTAT_FILES {
            std::fs::write(fstat.join(format!("f{i}")), format!("{i}\n"))?;
        }
        cmd.arg("--fstat-dir").arg(&fstat);
    }
    cmd.arg("--verify")
        .arg("--timeout")
        .arg(format!("{}s", plan.secs))
        .arg("--temp-path")
        .arg(dir)
        .arg("--metrics")
        .arg("--yaml")
        .arg(&yaml)
        .arg("--log-file")
        .arg(&log)
        .arg("--seed")
        .arg(seed.to_string())
        .args(SIZE_ARGS)
        .args(&plan.extra)
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&out)?)
        .stderr(std::fs::File::create(out.with_extension("err"))?)
        // Its own process group: a stuck run is killed as a whole, and
        // nothing but what this run started.
        .process_group(0);
    eprintln!(
        "    stress-ng[{node}]: {} stressors x{} for {}s in {}",
        plan.stressors.len(),
        plan.instances,
        plan.secs,
        dir.display()
    );
    let child = cmd.spawn_tied().context("spawning stress-ng")?;
    let started = Instant::now();
    Ok(Run {
        node: node.to_string(),
        child,
        started,
        deadline: started + Duration::from_secs(plan.secs) + GRACE,
        log,
        yaml,
        out,
    })
}

/// The processes of process group `pgid`: `pid state wchan comm`.
fn group_processes(pgid: u32) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return out;
    };
    for e in dir.flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // `pid (comm) state ppid pgrp ...`; comm may hold spaces.
        let Some((comm, rest)) = stat.split_once(" (").and_then(|(_, r)| r.rsplit_once(") "))
        else {
            continue;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        if fields.get(2).and_then(|g| g.parse::<u32>().ok()) != Some(pgid) {
            continue;
        }
        let wchan = std::fs::read_to_string(format!("/proc/{pid}/wchan")).unwrap_or_default();
        out.push(format!("{pid} {} {wchan} {comm}", fields[0]));
    }
    out
}

/// Wait for `run`; past its deadline, list what is stuck, kill the group
/// and, if it still does not go, abort `client`'s FUSE connection. `Ok`
/// with stress-ng's exit status, or the error describing the hang.
fn finish(run: &mut Run, client: &Client) -> Result<std::process::ExitStatus> {
    let pgid = run.child.id();
    loop {
        if let Some(status) = run.child.try_wait()? {
            // The group may outlive its leader (a stressor stuck in the
            // kernel); none may remain.
            let left = group_processes(pgid);
            if left.is_empty() {
                return Ok(status);
            }
            kill_group(pgid, client, &left)?;
            bail!(
                "stress-ng[{}] exited ({status}) but left processes behind:\n  {}",
                run.node,
                left.join("\n  ")
            );
        }
        if Instant::now() >= run.deadline {
            let stuck = group_processes(pgid);
            hang_hook(run, client, pgid);
            let requests = client
                .control_status()
                .map(|s| {
                    let rings: Vec<String> = s["fuse"]["mounts"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(ring_health)
                        .collect();
                    format!("{}\n  rings: {}", s["fuse_requests"], rings.join("; "))
                })
                .unwrap_or_else(|e| format!("(status failed: {e:#})"));
            kill_group(pgid, client, &stuck)?;
            let _ = run.child.wait();
            bail!(
                "stress-ng[{}] did not finish within {:?} of its start; stuck processes (pid state \
                 wchan comm):\n  {}\n  daemon fuse_requests: {requests}\n  last log lines:\n{}",
                run.node,
                run.deadline - run.started,
                stuck.join("\n  "),
                tail(&run.log, 20)
            );
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// `STRESS_NG_FS_HANG_HOOK`: a shell command run at a hang, before anything
/// is killed, with `HANG_NODE`, `HANG_PGID` (stress-ng's process group),
/// `HANG_DAEMON_PID` and `HANG_FUSE_CONN` (the mount's fusectl connection) in
/// its environment — for capturing kernel stacks and ring state while the
/// requests are still stuck.
fn hang_hook(run: &Run, client: &Client, pgid: u32) {
    let Ok(hook) = std::env::var("STRESS_NG_FS_HANG_HOOK") else {
        return;
    };
    let opt = |v: Option<u32>| v.map(|v| v.to_string()).unwrap_or_default();
    let status = Command::new("sh")
        .arg("-c")
        .arg(&hook)
        .env("HANG_NODE", &run.node)
        .env("HANG_PGID", pgid.to_string())
        .env("HANG_DAEMON_PID", opt(client.pid()))
        .env("HANG_FUSE_CONN", opt(client.fuse_connection()))
        .status();
    eprintln!("    hang hook for {}: {status:?}", run.node);
}

fn kill_group(pgid: u32, client: &Client, listed: &[String]) -> Result<()> {
    // SAFETY: a signal to the process group this run created.
    unsafe {
        libc::kill(-(pgid as i32), libc::SIGKILL);
    }
    let until = Instant::now() + Duration::from_secs(15);
    while Instant::now() < until {
        if group_processes(pgid).is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    // Killed and still there: waiting in the kernel for a FUSE answer that
    // never comes. Aborting the connection releases them (and ends the
    // mount, which the caller reports as the failure it is).
    eprintln!(
        "    stress-ng processes survive SIGKILL (in FUSE): aborting {}'s connection\n      {}",
        client.name,
        listed.join("\n      ")
    );
    if let Some(conn) = client.fuse_connection() {
        let _ = constellation_platform::native().mounts.abort_fuse(conn);
    }
    Ok(())
}

fn tail(path: &Path, n: usize) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..]
        .iter()
        .map(|l| format!("    {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Judge a finished run: print its table, append it to
/// `STRESS_NG_FS_REPORT`, and fail on any new failure.
fn judge_run(scenario: &str, transport: &str, plan: &Plan, run: &Run) -> Result<()> {
    let log = std::fs::read_to_string(&run.log).with_context(|| {
        format!(
            "reading {} (stress-ng's stderr:\n{})",
            run.log.display(),
            tail(&run.out.with_extension("err"), 10)
        )
    })?;
    let bogo = parse_bogo_ops(&std::fs::read_to_string(&run.yaml).unwrap_or_default());
    let verdicts = parse_verdicts(&log);
    let (outcomes, improvements) = judge(&plan.stressors, &verdicts, &plan.baseline);
    let mut table = String::new();
    let mut tsv = String::new();
    let mut failures = Vec::new();
    for (s, o) in &outcomes {
        let ops = bogo.get(s).copied().unwrap_or(0);
        let (word, note) = match o {
            Outcome::Passed => ("pass", String::new()),
            Outcome::Skipped => ("skip", skip_reason(&log, s)),
            Outcome::Baselined => ("fail*", plan.baseline[s].clone()),
            Outcome::Failed(why) => {
                failures.push(format!("{s}: {why}"));
                ("FAIL", why.clone())
            }
        };
        table += &format!("      {s:<12} {word:<5} {ops:>10}  {note}\n");
        tsv += &format!(
            "{scenario}\t{transport}\t{}\t{s}\t{word}\t{ops}\n",
            run.node
        );
    }
    eprintln!(
        "    stress-ng[{}] ({transport}, {:.0}s): {} passed, {} skipped, {} baselined, {} failed \
         (stressor verdict bogo-ops note):\n{table}",
        run.node,
        run.started.elapsed().as_secs_f64(),
        outcomes.values().filter(|o| **o == Outcome::Passed).count(),
        outcomes
            .values()
            .filter(|o| **o == Outcome::Skipped)
            .count(),
        outcomes
            .values()
            .filter(|o| **o == Outcome::Baselined)
            .count(),
        failures.len(),
    );
    if let Some(path) = std::env::var_os("STRESS_NG_FS_REPORT") {
        use std::io::Write;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?
            .write_all(tsv.as_bytes())?;
    }
    if !verdicts.untrustworthy.is_empty() {
        eprintln!(
            "    stress-ng[{}]: metrics untrustworthy (bogo-ops not comparable): {}",
            run.node,
            verdicts
                .untrustworthy
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(" ")
        );
    }
    if !improvements.is_empty() {
        eprintln!(
            "    IMPROVEMENTS (remove from tests/stress-ng-baseline.txt): {}",
            improvements.join(" ")
        );
    }
    ensure!(
        failures.is_empty(),
        "stress-ng[{}]: {} stressor(s) failed (stdout {}):\n  {}",
        run.node,
        failures.len(),
        run.out.display(),
        failures.join("\n  ")
    );
    Ok(())
}

/// The info line in which stress-ng says why it skipped `s`, if any.
fn skip_reason(log: &str, s: &str) -> String {
    log.lines()
        .filter_map(|l| l.split_once("] ").map(|(_, r)| r))
        .find(|r| {
            r.contains(s)
                && (r.contains("skip")
                    || r.contains("not supported")
                    || r.contains("not implemented"))
        })
        .map(|r| r.chars().take(100).collect())
        .unwrap_or_default()
}

/// Panics always; `ERROR` lines unless `errors_allowed` (S3 cuts log
/// them).
fn daemon_alarms(c: &Client, errors_allowed: bool) -> Result<()> {
    let mut panics = Vec::new();
    let mut errors = Vec::new();
    for file in c.log_files() {
        let text = std::fs::read_to_string(&file).unwrap_or_default();
        for line in text.lines() {
            let plain = strip_ansi(line);
            if plain.contains("panicked") {
                panics.push(plain);
            } else if plain.contains(" ERROR ") {
                errors.push(plain);
            }
        }
    }
    let show = |v: &[String]| {
        v.iter()
            .take(20)
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    ensure!(panics.is_empty(), "{} panicked:\n{}", c.name, show(&panics));
    if !errors.is_empty() {
        eprintln!(
            "    {}: {} ERROR line(s) in the daemon log{}:\n{}",
            c.name,
            errors.len(),
            if errors_allowed {
                " (allowed under S3 faults)"
            } else {
                ""
            },
            show(&errors)
        );
        ensure!(
            errors_allowed,
            "{} logged {} ERROR line(s)",
            c.name,
            errors.len()
        );
    }
    Ok(())
}

fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            // CSI: ESC [ ... final byte in @..~
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) && c != '[' {
                    break;
                }
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// No FUSE request but a blocking lock went unanswered past the
/// watchdog's threshold, ever.
fn no_stalled_requests(c: &Client) -> Result<()> {
    let status = c.control_status()?;
    let r = &status["fuse_requests"];
    ensure!(
        r["stalled_total"].as_u64() == Some(0),
        "{}: FUSE requests stalled past {}s: {r}",
        c.name,
        r["stall_threshold_s"]
    );
    // What the view's own count cannot see: ring entries held before the
    // view was handed the request (queued for a worker), and replies whose
    // ring thread was never woken (flushed only by its bounded wait).
    for m in status["fuse"]["mounts"].as_array().into_iter().flatten() {
        ensure!(
            m["ring_stranded_commits"].as_u64().unwrap_or(0) == 0
                && m["ring_entries_held_long"].as_u64().unwrap_or(0) == 0,
            "{}: the ring held requests the view did not see in flight: {}",
            c.name,
            ring_health(m)
        );
    }
    Ok(())
}

/// A mount's ring counters (`node.status`), for a report.
fn ring_health(m: &serde_json::Value) -> String {
    format!(
        "{} transport={} stranded_commits={} entries_held_long={}",
        m["mountpoint"].as_str().unwrap_or("?"),
        m["transport"].as_str().unwrap_or("?"),
        m["ring_stranded_commits"],
        m["ring_entries_held_long"]
    )
}

/// A few closed multi-chunk files written before the run, so that the
/// snapshots taken during it reference data (stress-ng's own files are
/// mostly open or gone at any instant) and `snapshot space --verify`
/// checks chunk accounting, not an empty index.
fn seed_files(c: &Client, seed: u64) -> Result<()> {
    let dir = c.mnt.join("stressfs-seed");
    std::fs::create_dir_all(&dir)?;
    for i in 0..4u64 {
        // xorshift64: no two chunks alike, or content addressing would
        // fold them into a handful.
        let mut x = (seed ^ (i + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1;
        let data: Vec<u8> = (0..(3u64 << 20) + i * 4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        std::fs::write(dir.join(format!("f{i}")), data)
            .with_context(|| format!("{}: writing seed file {i}", c.name))?;
    }
    Ok(())
}

/// No cluster lock grant was lost and nothing was fenced for it: no
/// stressor's process ever got `EIO` because its lock lapsed. stress-ng
/// tolerates some of those errors, so its verdicts alone miss them.
fn no_lock_fencing(c: &Client) -> Result<()> {
    let l = &c.control_status()?["locks"];
    let n = |key: &str| l[key].as_u64().unwrap_or(0);
    eprintln!(
        "    {}: locks: granted {} would_block {} unavailable {} lost {} fenced_io {} \
         owners_fenced {} owner_fenced_ops {}",
        c.name,
        n("granted"),
        n("would_block"),
        n("unavailable"),
        n("lost"),
        n("fenced_io"),
        n("owners_fenced"),
        n("owner_fenced_ops"),
    );
    for key in ["lost", "fenced_io", "owners_fenced", "owner_fenced_ops"] {
        ensure!(
            l[key].as_u64().unwrap_or(0) == 0,
            "{}: cluster locks lapsed under the run (locks.{key} = {}): {l}",
            c.name,
            l[key]
        );
    }
    Ok(())
}

/// `stat`, `ls`, and a 1 MiB write + fsync + read round trip.
fn healthy(c: &Client, seed: u64) -> Result<PathBuf> {
    use std::io::Write;
    std::fs::metadata(&c.mnt).with_context(|| format!("{}: stat of the mount", c.name))?;
    let entries = std::fs::read_dir(&c.mnt)
        .with_context(|| format!("{}: ls of the mount", c.name))?
        .count();
    let data: Vec<u8> = (0..1u64 << 20)
        .map(|i| (i.wrapping_mul(seed | 1) >> 3) as u8)
        .collect();
    let path = c.mnt.join(format!("stressfs-canary-{}", c.name));
    let mut f =
        std::fs::File::create(&path).with_context(|| format!("{}: creating the canary", c.name))?;
    f.write_all(&data)
        .and_then(|_| f.sync_all())
        .with_context(|| format!("{}: writing the canary", c.name))?;
    drop(f);
    let back = std::fs::read(&path).with_context(|| format!("{}: reading the canary", c.name))?;
    ensure!(back == data, "{}: the canary read back differently", c.name);
    eprintln!("    {}: healthy ({entries} entries at the root)", c.name);
    Ok(path)
}

/// The journal and the upload queue drain to zero; stalls (no progress
/// for 60 s) fail, slow progress does not.
fn drains(c: &Client) -> Result<()> {
    let started = Instant::now();
    let mut best = u64::MAX;
    let mut since = Instant::now();
    loop {
        let status = c.control_status()?;
        let backlog = status["spool"]["journal_backlog"]
            .as_u64()
            .unwrap_or(u64::MAX);
        let pending = status["writeback"]["pending_uploads"]
            .as_u64()
            .unwrap_or(u64::MAX);
        let left = backlog.saturating_add(pending);
        if left == 0 {
            eprintln!(
                "    {}: spool drained in {:.0}s",
                c.name,
                started.elapsed().as_secs_f64()
            );
            return Ok(());
        }
        if left < best {
            best = left;
            since = Instant::now();
        }
        ensure!(
            since.elapsed() < Duration::from_secs(60),
            "{}: the spool stopped draining (journal_backlog={backlog} pending_uploads={pending}): \
             {}",
            c.name,
            status["writeback"]
        );
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Kernel log lines since `since_unix` that name stress-ng, a hung task or
/// FUSE. The first two fail; the FUSE ones are printed (another agent's
/// mount may have caused them).
fn kernel_messages(since_unix: u64) -> Result<()> {
    let text = match Command::new("dmesg")
        .args(["--time-format", "iso"])
        .output()
    {
        Ok(o) if o.status.success() => {
            // dmesg has no --since; keep the lines whose timestamp is late
            // enough (ISO stamps compare as text within one zone).
            let since = chrono_iso(since_unix);
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|l| l.get(..19).is_some_and(|t| t >= since.as_str()))
                .collect::<Vec<_>>()
                .join("\n")
        }
        _ => {
            match Command::new("journalctl")
                .args(["-k", "--no-pager", "-q", "-o", "short-iso", "--since"])
                .arg(format!("@{since_unix}"))
                .output()
            {
                Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
                _ => {
                    eprintln!("    kernel log not readable without root (dmesg, journalctl -k): not checked");
                    return Ok(());
                }
            }
        }
    };
    let mut bad = Vec::new();
    let mut fuse = Vec::new();
    for line in text.lines() {
        let l = line.to_ascii_lowercase();
        if l.contains("stress-ng") || l.contains("blocked for more than") || l.contains("hung_task")
        {
            bad.push(line.to_string());
        } else if l.contains("fuse") {
            fuse.push(line.to_string());
        }
    }
    if !fuse.is_empty() {
        eprintln!(
            "    kernel FUSE messages during the run (not attributable to this mount alone):\n      {}",
            fuse.join("\n      ")
        );
    }
    ensure!(
        bad.is_empty(),
        "kernel messages about stress-ng or hung tasks:\n  {}",
        bad.join("\n  ")
    );
    Ok(())
}

/// `YYYY-MM-DDTHH:MM:SS` (local time, as `dmesg --time-format iso`) of a
/// unix time.
fn chrono_iso(unix: u64) -> String {
    let out = Command::new("date")
        .arg("-d")
        .arg(format!("@{unix}"))
        .arg("+%Y-%m-%dT%H:%M:%S")
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => String::new(),
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn transport_of(c: &Client) -> String {
    c.control_status()
        .ok()
        .and_then(|s| s["mounts"][0]["transport"].as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

/// The scenario's root: on disk, not on a tmpfs `/tmp`.
fn stress_setup(name: &str) -> Result<(S3Env, tempfile::TempDir)> {
    let dir = std::env::var_os("STRESS_NG_FS_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/tmp"));
    setup_in(name, &dir)
}

/// Snapshots of `/` while the stressors run: at a third and two thirds of
/// the run.
fn snapshots_during(c: &Client, plan: &Plan, done: &std::sync::atomic::AtomicBool) -> Result<u32> {
    let mut taken = 0;
    let started = Instant::now();
    for i in 1..=2u64 {
        let at = started + Duration::from_secs(plan.secs * i / 3);
        while Instant::now() < at {
            if done.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(taken);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        snapshot_with_retries(c, &format!("/@stressfs-{i}"))?;
        taken += 1;
    }
    Ok(taken)
}

/// `snapshot create` as its contract asks a caller to: one control request
/// is one attempt (`ControlService::snapshot_batch`), and a forward that
/// timed out at a busy holder is retried with a new request. The earlier
/// attempt may have completed meanwhile: then the retry finds the
/// snapshot already there, which is success. Says how long it took.
fn snapshot_with_retries(c: &Client, selector: &str) -> Result<()> {
    const ATTEMPTS: u32 = 6;
    let started = Instant::now();
    let mut timeouts = 0;
    loop {
        match c.snapshot_create(selector) {
            Ok(()) => break,
            Err(e) if timeouts > 0 && format!("{e:#}").contains("already exists") => break,
            Err(e) if format!("{e:#}").contains("timed out") && timeouts + 1 < ATTEMPTS => {
                timeouts += 1;
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "{}: snapshot {selector} during the run ({timeouts} timed-out attempt(s)                          before, {:.0}s in all)",
                        c.name,
                        started.elapsed().as_secs_f64()
                    )
                })
            }
        }
    }
    eprintln!(
        "    {}: snapshot {selector} in {:.1}s ({timeouts} timed-out attempt(s) retried)",
        c.name,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn fs_client(
    env: &S3Env,
    root: &Path,
    name: &str,
    backend: &str,
    snapshots: bool,
) -> Result<Client> {
    let mut c = Client::new(root, name, &env.endpoint, backend)?.with_own_node_key();
    if snapshots {
        c = c
            .with_env("CONSTELLATION_SNAPACCT", "on")
            .with_env("CONSTELLATION_SNAPACCT_REFRESH_S", "2");
    }
    Ok(c)
}

/// `stress-ng-fs`.
pub(super) fn stress_ng_fs(seed: u64) -> Result<()> {
    single("stress-ng-fs", seed, false)
}

/// `stress-ng-fs-faults`.
pub(super) fn stress_ng_fs_faults(seed: u64) -> Result<()> {
    single("stress-ng-fs-faults", seed, true)
}

fn single(scenario: &str, seed: u64, faults: bool) -> Result<()> {
    let plan = plan()?;
    let (env, root) = stress_setup(scenario)?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/sngfs-{}", ts());
    let mut c = fs_client(&env, root.path(), "c0", &backend, !faults)?;
    c.fs_create()?;
    c.mount()?;
    let transport = transport_of(&c);
    if !faults {
        seed_files(&c, seed)?;
    }
    let since = unix_now();
    let done = std::sync::atomic::AtomicBool::new(false);
    let (status, snaps) = std::thread::scope(|scope| -> Result<_> {
        let flapper = faults.then(|| {
            scope.spawn(|| -> Result<()> {
                let mut rng_state = seed | 1;
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    proxy.latency(40, 20)?;
                    // 2.5–5.5 s of latency only, then a 1.5 s cut.
                    rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let calm = 2500 + (rng_state >> 33) % 3000;
                    sleep_unless(&done, Duration::from_millis(calm));
                    proxy.cut()?;
                    std::thread::sleep(Duration::from_millis(1500));
                    proxy.heal()?;
                }
                Ok(())
            })
        });
        let snapper = (!faults).then(|| scope.spawn(|| snapshots_during(&c, &plan, &done)));
        let ran = start(&plan, "c0", &c.mnt.join("stressfs"), root.path(), seed)
            .and_then(|mut run| finish(&mut run, &c).map(|s| (s, run)));
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(f) = flapper {
            f.join().expect("flapper panicked")?;
        }
        let snaps = match snapper {
            Some(s) => s.join().expect("snapshotter panicked"),
            None => Ok(0),
        };
        Ok((ran, snaps))
    })?;
    proxy.heal()?;
    let (exit, run) = status?;
    eprintln!("    stress-ng[c0] exited: {exit}");
    judge_run(scenario, &transport, &plan, &run)?;
    let snaps = snaps?;
    daemon_alarms(&c, faults)?;
    no_stalled_requests(&c)?;
    no_lock_fencing(&c)?;
    healthy(&c, seed)?;
    drains(&c)?;
    if snaps > 0 {
        verify_clean(&c, "after the run")?;
    }
    kernel_messages(since)?;
    c.unmount()?;
    Ok(())
}

fn sleep_unless(done: &std::sync::atomic::AtomicBool, d: Duration) {
    let until = Instant::now() + d;
    while Instant::now() < until && !done.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `stress-ng-fs-nodes`: three nodes, each its own stress-ng in its own
/// directory, at once.
pub(super) fn stress_ng_fs_nodes(seed: u64) -> Result<()> {
    const NODES: usize = 3;
    let scenario = "stress-ng-fs-nodes";
    let plan = plan()?;
    let (env, root) = stress_setup(scenario)?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/sngfs-nodes-{}", ts());
    let mut nodes = Vec::new();
    for i in 0..NODES {
        nodes.push(fs_client(
            &env,
            root.path(),
            &format!("n{i}"),
            &backend,
            true,
        )?);
    }
    nodes[0].fs_create()?;
    for c in &mut nodes {
        c.mount()?;
    }
    wait_for_p2p(&nodes.iter().collect::<Vec<_>>())?;
    let transport = transport_of(&nodes[0]);
    seed_files(&nodes[0], seed)?;
    let since = unix_now();
    let done = std::sync::atomic::AtomicBool::new(false);
    let (results, snaps) = std::thread::scope(|scope| -> Result<_> {
        // The snapshots come from a node that does not hold the lease
        // (n0 created the filesystem and writes first).
        let snapper = scope.spawn(|| snapshots_during(&nodes[1], &plan, &done));
        let runs: Vec<_> = nodes
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let plan = &plan;
                let root = root.path();
                scope.spawn(move || {
                    start(
                        plan,
                        &c.name,
                        &c.mnt.join(format!("stressfs-{}", c.name)),
                        root,
                        seed.wrapping_add(i as u64),
                    )
                    .and_then(|mut run| finish(&mut run, c).map(|s| (s, run)))
                })
            })
            .collect();
        let results: Vec<Result<_>> = runs
            .into_iter()
            .map(|h| h.join().expect("stress-ng runner panicked"))
            .collect();
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        let snaps = snapper.join().expect("snapshotter panicked");
        Ok((results, snaps))
    })?;
    // Every node's verdict first, so one report shows them all.
    let mut errors = Vec::new();
    for (c, r) in nodes.iter().zip(results) {
        let judged = r.and_then(|(exit, run)| {
            eprintln!("    stress-ng[{}] exited: {exit}", c.name);
            judge_run(scenario, &transport, &plan, &run)
        });
        if let Err(e) = judged {
            errors.push(format!("{e:#}"));
        }
    }
    let snaps = match snaps {
        Ok(n) => n,
        Err(e) => {
            errors.push(format!("{e:#}"));
            0
        }
    };
    ensure!(errors.is_empty(), "{}", errors.join("\n"));
    for c in &nodes {
        daemon_alarms(c, false)?;
        no_stalled_requests(c)?;
        no_lock_fencing(c)?;
    }
    let mut canaries = Vec::new();
    for c in &nodes {
        canaries.push(healthy(c, seed)?);
    }
    // Each node reads every other node's canary, as written.
    for (i, reader) in nodes.iter().enumerate() {
        for (j, path) in canaries.iter().enumerate() {
            if i == j {
                continue;
            }
            let name = path.file_name().unwrap().to_owned();
            let want = std::fs::read(path)?;
            eventually(
                &format!("{} reads {}'s canary", reader.name, nodes[j].name),
                Duration::from_secs(60),
                || {
                    let got = std::fs::read(reader.mnt.join(&name))?;
                    ensure!(got == want, "different content ({} bytes)", got.len());
                    Ok(())
                },
            )?;
        }
    }
    for c in &nodes {
        drains(c)?;
    }
    if snaps > 0 {
        for c in &nodes {
            verify_clean(c, "after the run")?;
        }
    }
    kernel_messages(since)?;
    for c in &mut nodes {
        c.unmount()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_lists_parse_with_a_reason_on_every_entry() {
        let exclude = parse_list(EXCLUDE).unwrap();
        let baseline = parse_list(BASELINE).unwrap();
        assert!(!exclude.is_empty());
        for (name, reason) in exclude.iter().chain(baseline.iter()) {
            assert!(reason.len() > 10, "{name}: a real reason, not {reason:?}");
        }
        assert!(exclude
            .keys()
            .all(|s| !baseline.contains_key(s.strip_suffix("@root").unwrap_or(s))));
    }

    #[test]
    fn list_entries_need_a_reason_and_appear_once() {
        assert!(parse_list("rofs # REASON: needs a read-only fs\n").is_ok());
        assert!(parse_list("rofs\n").is_err());
        assert!(parse_list("rofs #\n").is_err());
        assert!(parse_list("a # REASON: x\na # REASON: y\n").is_err());
        assert!(parse_list("two words # REASON: x\n").is_err());
        let l = parse_list("# comment\n\nverity # REASON: no fs-verity\n").unwrap();
        assert_eq!(l["verity"], "no fs-verity");
    }

    #[test]
    fn root_only_exclusions_apply_to_root_runs_only() {
        let l = parse_list("rofs # REASON: x\niomix@root # REASON: drops caches\n").unwrap();
        let user = effective_exclude(&l, false);
        let root = effective_exclude(&l, true);
        assert_eq!(user.keys().collect::<Vec<_>>(), ["rofs"]);
        assert_eq!(root.keys().collect::<Vec<_>>(), ["iomix", "rofs"]);
        assert_eq!(root["iomix"], "drops caches");
    }

    #[test]
    fn the_class_listing_parses() {
        let c = parse_class("class 'filesystem' stressors: access acl dentry\n").unwrap();
        assert_eq!(c.len(), 3);
        assert!(c.contains("dentry"));
        assert!(parse_class("nothing here").is_err());
    }

    #[test]
    fn version_gated_names_missing_from_an_older_stress_ng_are_not_stale() {
        let listed =
            parse_list("rofs # REASON: x\nstatmount # REASON: y\ntypoed # REASON: z\n").unwrap();
        let baseline = parse_list("alsotypo # REASON: w\n").unwrap();
        // An older stress-ng: no rofs, statmount, or (of course) the typos.
        let class: BTreeSet<String> = ["dentry", "xattr"].map(String::from).into();
        let (stale, gated) = missing_from_class(&listed, &baseline, &class);
        assert_eq!(stale, vec!["typoed", "alsotypo"]);
        assert!(gated.contains(&"rofs") && gated.contains(&"statmount"));
        // A current stress-ng that does have them: no longer gated, and a
        // real mistake still fails just as it would on the older one.
        let class: BTreeSet<String> = ["dentry", "rofs", "statmount"].map(String::from).into();
        let (stale, gated) = missing_from_class(&listed, &baseline, &class);
        assert_eq!(stale, vec!["typoed", "alsotypo"]);
        assert!(gated.is_empty());
    }

    const LOG: &str = "\
stress-ng: info:  [10] setting to a 1 min run per stressor
stress-ng: error: [12] filename: creat() failed when probing for allowed filename characters, errno=36 (File name too long)
stress-ng: error: [10] filename: [12] terminated with an error, exit status=2 (stressor failed)
stress-ng: fail:  [13] hdd: verify failure at offset 4096
stress-ng: info:  [14] chattr: chattr not supported on filesystem, skipping stressor
stress-ng: info:  [10] skipped: 1: chattr (1)
stress-ng: info:  [10] passed: 3: dentry (1) xattr (2) hdd (1)
stress-ng: info:  [10] failed: 2: filename (1) hdd (1)
stress-ng: info:  [10] metrics untrustworthy: 0
";

    #[test]
    fn verdicts_come_from_the_summary_lines() {
        let v = parse_verdicts(LOG);
        assert_eq!(v.passed, ["dentry", "xattr"].map(String::from).into());
        assert_eq!(v.failed, ["filename", "hdd"].map(String::from).into());
        assert_eq!(v.skipped, ["chattr"].map(String::from).into());
        assert!(v.untrustworthy.is_empty());
        assert_eq!(v.complaints["filename"].len(), 2);
        assert_eq!(v.complaints["hdd"].len(), 1);
    }

    #[test]
    fn a_baseline_covers_failures_but_never_data_and_asks_for_removal() {
        let v = parse_verdicts(LOG);
        let selected: Vec<String> = ["dentry", "xattr", "hdd", "filename", "chattr", "lockf"]
            .map(String::from)
            .into();
        let baseline: BTreeMap<String, String> = [
            ("filename", "non-UTF-8 names"),
            ("hdd", "would hide a data failure"),
            ("dentry", "fixed since"),
        ]
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .into();
        let (o, improvements) = judge(&selected, &v, &baseline);
        assert_eq!(o["filename"], Outcome::Baselined);
        assert!(matches!(&o["hdd"], Outcome::Failed(w) if w.contains("data failure")));
        assert_eq!(o["chattr"], Outcome::Skipped);
        assert_eq!(o["xattr"], Outcome::Passed);
        assert!(matches!(&o["lockf"], Outcome::Failed(w) if w.contains("no verdict")));
        assert_eq!(improvements, vec!["dentry".to_string()]);
        // Without the baseline, filename is a new failure.
        let (o, _) = judge(&selected, &v, &BTreeMap::new());
        assert!(matches!(&o["filename"], Outcome::Failed(w) if w.contains("probing")));
    }

    #[test]
    fn bogo_ops_come_from_the_yaml() {
        let yaml = "metrics:\n    - stressor: dentry\n      bogo-ops: 4096\n      instances:\n          - instance: 0\n            bogo-ops: 4096\n    - stressor: xattr\n      bogo-ops: 0\n";
        let b = parse_bogo_ops(yaml);
        assert_eq!(b["dentry"], 4096);
        assert_eq!(b["xattr"], 0);
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn ansi_colour_is_stripped_from_log_lines() {
        let l = "\u{1b}[2m2026\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \u{1b}[2mx\u{1b}[0m: boom";
        assert_eq!(strip_ansi(l), "2026 ERROR x: boom");
    }
}
