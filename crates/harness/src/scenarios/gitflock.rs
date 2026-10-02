//! EC2 campaign 4, findings B-1/B-2, and campaign 5's back-to-back run:
//! a git repository shared by two committers who take turns under an
//! `flock` turn file.
//!
//! `git-under-flock` reproduces the soak workload: nodes 0 and 1
//! alternately commit (a note appended plus a few fresh files) under
//! `flock`. Git writes every loose object as `objects/xx/tmp_obj_*`
//! (`O_CREAT|O_EXCL`, write, close), `link`s it to `objects/xx/<hash>`
//! and unlinks the temporary name; refs go through `*.lock` plus
//! `rename`. Under the lock each committer first checks what the
//! previous turn left:
//!
//! - `refs/heads/master` must read as the last acknowledged commit (a
//!   stale read is B-1's lost update: the next commit is based on an old
//!   `HEAD` and the other node's commits dangle), and must equal the
//!   marker file the previous turn wrote under the lock after its commit
//!   (campaign 5's check, done right: one marker for the last commit by
//!   anyone, not one per other committer — a committer that takes two
//!   turns in a row reads its own commit);
//! - no other turn is in progress: every turn records when it got and
//!   released the lock, and two turns that overlap are a broken lock
//!   (campaign 5's `index.lock: File exists` stall began that way).
//!
//! Then every node drains, a fresh node mounts from the bucket, and on
//! every node (the fresh one included) the repository tree (every `.git`
//! file: path, size, nlink, content hash) is identical, `git fsck
//! --full` is clean, and every acknowledged commit is present and an
//! ancestor of `HEAD`.
//!
//! Variants:
//! - `git-under-flock-gc`: `git gc` under the lock every 8 commits;
//! - `git-under-flock-faults`: `kill -9` of a random node (the lease
//!   holder, a committer, the whole cluster), `SIGSTOP`, P2P isolation
//!   of one node and an S3 cut of one node meanwhile;
//! - `git-under-flock-b2b`: campaign 5's shape — back to back, 5–20
//!   files per commit (new files and appends to any tracked file), the
//!   repository growing all the time — with the turn duration reported
//!   per decile: a turn that takes longer and longer is the slowdown
//!   campaign 5 saw (turns from 1 s to 160 s in 30 minutes);
//! - `git-under-flock-rounds`: the b2b workload several times in a row
//!   (`GIT_FLOCK_ROUNDS`, default 3), each in a new repository, against
//!   the same daemons (campaign 5: the stall "never on a daemon's first
//!   workload, then on every later one");
//! - `git-under-flock-causal` (EC2 campaign 7, finding B-1): the b2b
//!   workload with every node that does not commit *reading*. A reader
//!   watches the reflog and `refs/heads/master` through its own mount
//!   and, for every commit either names, checks that the commit object,
//!   its tree and every object under it exist: git writes all of them
//!   before it appends the reflog line, and appends the reflog before it
//!   renames the ref, so a reader that sees the publication must see the
//!   objects (causal order: no effect visible before what it follows).
//!   An object counts as visible loose or in a pack; a look that the
//!   reader's own mount restart overlapped is not a finding (the commit
//!   is checked again on the new mount). The ref must never regress
//!   either. Every `GIT_FLOCK_FSCK_EVERY_S` (20) the reader takes the
//!   turn lock, checks that the ref is the last acknowledged commit, and
//!   runs `git fsck --full`. The lock matters:
//!   fsck scans the object directories first and reads the refs and
//!   reflogs afterwards, so a commit that lands in between makes it
//!   report `missing blob/tree/commit` and `invalid reflog entry` on any
//!   filesystem (campaign 7's checkpoints ran it while a turn was still
//!   in flight; `git fsck --full` racing a committer on a local disk
//!   reports the same). A reader is killed and remounted
//!   `GIT_FLOCK_READER_RESTARTS` (2) times and keeps checking while it
//!   catches up. The killed reader may be the sequencer or its backup,
//!   so once a restart happened the committers' turn checks (a stale
//!   ref under the lock, overlapping turns, a failed or slow turn) are
//!   reported as under faults, not fatal; the readers' checks and the
//!   end-state verification are.
//!
//! Knobs: `GIT_FLOCK_SECS` (workload duration, per round), `GIT_FLOCK_NODES`
//! (2-4; causal: 3-4), `GIT_FLOCK_COMMITTERS=last` (the last two nodes
//! commit, so neither is the sequencer; causal: the sequencer then reads),
//! `GIT_FLOCK_S3_LATENCY_MS`,
//! `GIT_FLOCK_ENV=K=V,...` (extra mount environment), `GIT_FLOCK_RUST_LOG`
//! (the daemons' `RUST_LOG`), `GIT_FLOCK_ROUNDS`,
//! `GIT_FLOCK_MAX_TURN_S` (b2b/rounds/causal: a turn longer than this
//! fails the run; default 30), `GIT_FLOCK_FSCK_EVERY_S`,
//! `GIT_FLOCK_READER_RESTARTS`.
//!
//! Git runs with a fixed configuration ([`GIT_CONFIG`]): an empty `HOME`,
//! no system config, and no automatic maintenance (a detached repack
//! outside the turn lock is not the workload).

use super::m9::{c_deny_path, node_id};
use super::{eventually, journal_drained, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{bail, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeSet;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A file each mount has while it is live: a worker whose mount is dead
/// (killed, lazily detached) would otherwise run git in the empty
/// mountpoint directory underneath.
const ALIVE: &str = "gitflock-alive";

#[derive(Clone, Copy, PartialEq)]
enum Variant {
    Plain,
    Gc,
    Faults,
    B2b,
    Rounds,
    /// Campaign 7's B-1: readers check causal order while the b2b
    /// workload runs.
    Causal,
}

impl Variant {
    /// Campaign 5's commit shape (5–20 files, appends to tracked files).
    fn campaign_shape(self) -> bool {
        matches!(self, Variant::B2b | Variant::Rounds | Variant::Causal)
    }
}

/// Where one workload's repository, turn file and marker live, relative
/// to a mount.
#[derive(Clone)]
struct Paths {
    repo: String,
    turn: String,
    marker: String,
}

impl Paths {
    fn legacy() -> Paths {
        Paths {
            repo: "gitrepo".into(),
            turn: "gitrepo.lock".into(),
            marker: "gitrepo.marker".into(),
        }
    }

    fn round(k: usize) -> Paths {
        Paths {
            repo: format!("round-{k}/repo"),
            turn: format!("round-{k}/turn.lock"),
            marker: format!("round-{k}/marker"),
        }
    }
}

pub(super) fn git_under_flock(seed: u64) -> Result<()> {
    run("git-under-flock", seed, Variant::Plain)
}

pub(super) fn git_under_flock_gc(seed: u64) -> Result<()> {
    run("git-under-flock-gc", seed, Variant::Gc)
}

pub(super) fn git_under_flock_faults(seed: u64) -> Result<()> {
    run("git-under-flock-faults", seed, Variant::Faults)
}

pub(super) fn git_under_flock_b2b(seed: u64) -> Result<()> {
    run("git-under-flock-b2b", seed, Variant::B2b)
}

pub(super) fn git_under_flock_rounds(seed: u64) -> Result<()> {
    run("git-under-flock-rounds", seed, Variant::Rounds)
}

pub(super) fn git_under_flock_causal(seed: u64) -> Result<()> {
    run("git-under-flock-causal", seed, Variant::Causal)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Run `f` on a thread of its own, for at most `limit`: a step that
/// touches a mount (a git command, a tree walk) must not hang the
/// scenario for good when a FUSE request goes unanswered (EC2 campaign 7
/// B-2) — it fails, and the verdict names the node. The thread is left
/// behind on a timeout (a process stuck in an uninterruptible FUSE wait
/// cannot be killed anyway; the daemon's unmount at the end releases it).
fn bounded<T: Send + 'static>(
    what: &str,
    limit: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(limit).map_err(|_| {
        anyhow::anyhow!("{what} did not finish within {limit:?} (a hung FUSE request?)")
    })
}

/// `GIT_FLOCK_GIT_TIMEOUT_S`: how long one git command may take
/// (default 120 s).
fn git_timeout() -> Duration {
    Duration::from_secs(env_u64("GIT_FLOCK_GIT_TIMEOUT_S", 120))
}

/// The configuration every git command here runs with (on top of an empty
/// `HOME` and no system config). Automatic maintenance is off: a commit
/// starts `git maintenance run --auto --detach`, which in recent git (2.55
/// on the EC2 build host) packs the loose objects (packs plus a
/// multi-pack-index) and deletes the loose copies — in the background,
/// outside the turn lock, on whichever node committed. The workloads are
/// about loose objects written under the lock (`tmp_obj_*`, `link`, `unlink`, refs through `*.lock`); a repack
/// running on one node while the other commits is a different workload
/// (`git-under-flock-gc` runs `git gc` explicitly, under the lock), and a
/// reader stat-ing loose objects would see the packed ones vanish (git
/// 2.55: 14 packs and no loose object after 40 commits on a local disk).
const GIT_CONFIG: [&str; 8] = [
    "-c",
    "user.name=gitflock",
    "-c",
    "user.email=gitflock@test",
    "-c",
    "maintenance.auto=false",
    "-c",
    "gc.auto=0",
];

/// `git -C repo args...` with a clean, fixed configuration ([`GIT_CONFIG`]),
/// bounded by [`git_timeout`].
fn git(repo: &Path, home: &Path, args: &[&str]) -> Result<std::process::Output> {
    let (repo, home) = (repo.to_path_buf(), home.to_path_buf());
    let owned: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let what = format!("git {args:?} in {}", repo.display());
    bounded(&what, git_timeout(), move || {
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(GIT_CONFIG)
            .args(&owned)
            .env("HOME", &home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
    })?
    .with_context(|| format!("running git {args:?}"))
}

/// The FUSE request watchdog's report of a node (`status.fuse_requests`):
/// `None` when the node does not answer (dead, or restarting).
fn fuse_requests_of(c: &Client) -> Option<serde_json::Value> {
    c.control_status().ok().map(|s| s["fuse_requests"].clone())
}

/// A stalled FUSE request on any of `clients` (a request unanswered past
/// the daemon's stall threshold, ever since it started), as one line per
/// node; empty when there is none. A blocking lock wait is not a stall.
fn fuse_stalls(clients: &[Client]) -> Vec<String> {
    let mut out = Vec::new();
    for c in clients {
        let Some(f) = fuse_requests_of(c) else {
            continue;
        };
        let total = f["stalled_total"].as_u64().unwrap_or(0);
        if total == 0 {
            continue;
        }
        let list: Vec<String> = f["stalled_requests"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|r| r["blocking"] != true)
                    .map(|r| {
                        format!(
                            "{} ino {} {}s in {:?} (tid {})",
                            r["op"].as_str().unwrap_or("?"),
                            r["ino"],
                            r["age_s"],
                            r["stage"].as_str().unwrap_or("?"),
                            r["tid"]
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.push(format!(
            "{}: {total} FUSE request(s) stalled past {}s since the daemon started ({} completed since, {} stalled now){}",
            c.name,
            f["stall_threshold_s"],
            f["stalled_completed"],
            f["stalled"],
            if list.is_empty() {
                String::new()
            } else {
                format!(": {}", list.join("; "))
            }
        ));
    }
    out
}

fn git_ok(repo: &Path, home: &Path, args: &[&str]) -> Result<String> {
    let out = git(repo, home, args)?;
    if !out.status.success() {
        bail!(
            "git {args:?} in {}: {}{}",
            repo.display(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn flock(f: &std::fs::File, op: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::flock(f.as_raw_fd(), op) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// One turn: who, and when (this process's monotonic clock) it held the
/// turn lock.
#[derive(Clone)]
struct Turn {
    who: String,
    i: u64,
    /// When the committer asked for the lock (the `flock` call).
    asked: Instant,
    got: Instant,
    released: Instant,
    /// How long each step of the turn took.
    steps: Vec<(&'static str, Duration)>,
    /// The turn's git (or the edit before git ran) failed while its node
    /// refused ops of a lapsed lock owner (its `owner_fenced_ops` rose
    /// during the turn): its lock grant lapsed under the turn, so the lock
    /// may have moved, and what git did after that was refused rather
    /// than written. Not a turn whose git succeeded and whose marker
    /// write then got `EIO`: git may have written under the lapsed grant
    /// then. Also cleared when the node counted no fenced op during the
    /// whole workload.
    fenced: bool,
    /// When the turn's last step that wrote to the repository or the
    /// marker (edit, git add, git commit, marker, git gc) completed.
    wrote_until: Option<Instant>,
    /// When each step began (to tell, for a turn overlapping it, what
    /// ran before and after the other got the lock).
    began: Vec<(&'static str, Instant)>,
}

/// Whether a turn's error is the lock fence's `EIO`.
fn is_fence_error(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}");
    s.contains("os error 5") || s.contains("Input/output error")
}

/// The node's `owner_fenced_ops` (`None`: its daemon does not answer).
fn owner_fenced_ops(state: &Path) -> Option<u64> {
    crate::client::control_call_at(
        state,
        "node.status",
        serde_json::json!({}),
        Duration::from_secs(5),
    )
    .ok()?["locks"]["owner_fenced_ops"]
        .as_u64()
}

/// The steps whose `EIO` shows the owner fence stopped the turn's git
/// before it wrote under a lapsed grant: git's own, and the edit before
/// git ran at all.
const FENCEABLE_STEPS: [&str; 5] = ["edit", "add", "commit", "rev-parse", "gc"];

/// The steps that write.
const WRITING_STEPS: [&str; 5] = ["edit", "add", "commit", "marker", "gc"];

#[derive(Default)]
struct WorkerLog {
    /// Every commit a committer's `git commit` acknowledged.
    acked: Vec<String>,
    errors: Vec<String>,
    /// Turns that began with a stale `refs/heads/master` or marker.
    stale: Vec<String>,
    gcs: u64,
}

/// What both committers share.
struct Shared {
    /// The last acknowledged commit (by either committer).
    last: Mutex<Option<String>>,
    turns: Mutex<Vec<Turn>>,
}

/// Campaign 5's commit: 5–20 files, up to half of them appends to files
/// already tracked (anyone's), the rest new files in the committer's own
/// directory.
fn campaign_edit(repo: &Path, name: &str, i: u64, rng: &mut StdRng) -> Result<()> {
    use std::io::Write;
    let mut existing = Vec::new();
    let mut stack = vec![repo.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir)? {
            let e = e?;
            if e.file_name() == ".git" {
                continue;
            }
            if e.file_type()?.is_dir() {
                stack.push(e.path());
            } else {
                existing.push(e.path());
            }
        }
    }
    existing.sort();
    let n = rng.random_range(5..=20usize);
    let n_modify = existing.len().min(rng.random_range(0..=n / 2));
    for _ in 0..n_modify {
        let p = &existing[rng.random_range(0..existing.len())];
        let mut f = std::fs::OpenOptions::new().append(true).open(p)?;
        let len = rng.random_range(20..4000usize);
        let data: Vec<u8> = (0..len).map(|_| rng.random_range(b'a'..=b'z')).collect();
        f.write_all(b"\n")?;
        f.write_all(&data)?;
    }
    let dir = repo.join(format!("node{name}"));
    std::fs::create_dir_all(&dir)?;
    for k in 0..n - n_modify {
        let len = rng.random_range(20..4000usize);
        let data: Vec<u8> = (0..len).map(|_| rng.random_range(b'a'..=b'z')).collect();
        std::fs::write(dir.join(format!("f-{i}-{k}.dat")), data)?;
    }
    Ok(())
}

/// The soak's commit: a note appended plus three fresh files.
fn soak_edit(repo: &Path, name: &str, i: u64) -> Result<()> {
    use std::io::Write;
    let mut note = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(repo.join(format!("note-{name}.txt")))
        .context("note")?;
    writeln!(note, "{name} {i}")?;
    drop(note);
    let dir = repo.join(format!("d-{name}"));
    std::fs::create_dir_all(&dir)?;
    for k in 0..3 {
        std::fs::write(
            dir.join(format!("f-{i}-{k}")),
            format!("{name} commit {i} file {k}\n"),
        )?;
    }
    Ok(())
}

/// One committer: take the turn lock, check what the previous turn left,
/// edit, commit, write the marker, (gc), release. Errors are recorded,
/// not fatal: under faults a mount may be dead or a lock grant fenced. A
/// stale git lock file left by a failed commit is removed under the turn
/// lock (nobody else can be inside git then).
#[allow(clippy::too_many_arguments)]
fn committer(
    name: String,
    mnt: PathBuf,
    state: PathBuf,
    home: PathBuf,
    stop: Arc<AtomicBool>,
    variant: Variant,
    paths: Paths,
    log: Arc<Mutex<WorkerLog>>,
    shared: Arc<Shared>,
    seed: u64,
) {
    let repo = mnt.join(&paths.repo);
    let marker = mnt.join(&paths.marker);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut i = 0u64;
    while !stop.load(Ordering::SeqCst) {
        if !mnt.join(ALIVE).exists() {
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        i += 1;
        let r = (|| -> Result<()> {
            let lf = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(mnt.join(&paths.turn))
                .context("opening the turn file")?;
            // The node's fenced-op count before the turn (no lock is held
            // here, so nothing of ours can be fenced until the `flock`).
            let fenced_before = owner_fenced_ops(&state);
            let asked = Instant::now();
            flock(&lf, libc::LOCK_EX).context("flock")?;
            let got = Instant::now();
            let steps = std::cell::RefCell::new(Vec::new());
            let mark = std::cell::Cell::new(Instant::now());
            // The step in progress, and when the last writing one ended.
            let current = std::cell::Cell::new("check");
            let wrote_until = std::cell::Cell::new(None);
            let step = |name: &'static str| {
                let now = Instant::now();
                steps.borrow_mut().push((name, now - mark.get()));
                mark.set(now);
                if WRITING_STEPS.contains(&name) {
                    wrote_until.set(Some(now));
                }
            };
            let began = std::cell::RefCell::new(vec![("check", got)]);
            let begin = |name: &'static str| {
                current.set(name);
                began.borrow_mut().push((name, Instant::now()));
            };
            let inner = (|| -> Result<()> {
                // What the previous turn left: `refs/heads/master` must
                // read as the last acknowledged commit, and as the marker
                // it wrote under the lock, now that we hold the lock.
                let expected = shared.last.lock().unwrap().clone();
                if let Some(want) = &expected {
                    let read = || {
                        let marker = std::fs::read_to_string(&marker)
                            .map(|s| s.trim().to_string())
                            .unwrap_or_else(|e| format!("<marker: {e}>"));
                        (master_ref(&repo), marker)
                    };
                    let seen = read();
                    if &seen.0 != want || &seen.1 != want {
                        let t = Instant::now();
                        let mut now = seen.clone();
                        while (&now.0 != want || &now.1 != want)
                            && t.elapsed() < Duration::from_secs(5)
                        {
                            std::thread::sleep(Duration::from_millis(20));
                            now = read();
                        }
                        log.lock().unwrap().stale.push(format!(
                            "{name}#{i}: under the turn lock refs/heads/master read {}, the marker {}; the last acknowledged commit is {want}; {}",
                            seen.0,
                            seen.1,
                            if &now.0 == want && &now.1 == want {
                                format!("caught up after {:?}", t.elapsed())
                            } else {
                                format!("still {now:?} after {:?}", t.elapsed())
                            }
                        ));
                    }
                }
                step("check");
                begin("edit");
                for stale in ["index.lock", "refs/heads/master.lock", "HEAD.lock"] {
                    let p = repo.join(".git").join(stale);
                    if p.exists() {
                        log.lock().unwrap().errors.push(format!(
                            "{name}#{i}: removed stale {stale} before committing"
                        ));
                        let _ = std::fs::remove_file(&p);
                    }
                }
                if variant.campaign_shape() {
                    campaign_edit(&repo, &name, i, &mut rng)?;
                } else {
                    soak_edit(&repo, &name, i)?;
                }
                step("edit");
                begin("add");
                git_ok(&repo, &home, &["add", "-A"])?;
                step("add");
                begin("commit");
                git_ok(
                    &repo,
                    &home,
                    &[
                        "commit",
                        "-q",
                        "--allow-empty",
                        "-m",
                        &format!("{name}-{i}"),
                    ],
                )?;
                step("commit");
                begin("rev-parse");
                let head = git_ok(&repo, &home, &["rev-parse", "HEAD"])?;
                begin("marker");
                std::fs::write(&marker, format!("{head}\n")).context("writing the marker")?;
                step("marker");
                *shared.last.lock().unwrap() = Some(head.clone());
                log.lock().unwrap().acked.push(head);
                if variant == Variant::Gc && i.is_multiple_of(8) {
                    begin("gc");
                    git_ok(&repo, &home, &["gc", "-q"])?;
                    step("gc");
                    log.lock().unwrap().gcs += 1;
                }
                Ok(())
            })();
            shared.turns.lock().unwrap().push(Turn {
                who: name.clone(),
                i,
                asked,
                got,
                released: Instant::now(),
                steps: steps.take(),
                fenced: inner.as_ref().err().is_some_and(|e| {
                    FENCEABLE_STEPS.contains(&current.get())
                        // The node says it refused an op of ours during
                        // the turn (git often drops the errno: "couldn't
                        // set 'refs/heads/master'"); with no answer (a
                        // dead daemon), the error itself must say `EIO`.
                        && match (fenced_before, owner_fenced_ops(&state)) {
                            (Some(was), Some(is)) => is > was || (is < was && is > 0),
                            _ => is_fence_error(e),
                        }
                }),
                wrote_until: wrote_until.get(),
                began: began.take(),
            });
            let _ = flock(&lf, libc::LOCK_UN);
            inner
        })();
        if let Err(e) = r {
            log.lock()
                .unwrap()
                .errors
                .push(format!("{name}#{i}: {e:#}"));
            std::thread::sleep(Duration::from_millis(300));
        }
    }
}

/// What one reader saw (`Variant::Causal`).
#[derive(Default)]
struct ReaderLog {
    /// Commits checked (the reflog and the ref each name one).
    checked: u64,
    /// A publication (a reflog line, the ref) visible before an object
    /// the commit it names depends on.
    violations: Vec<String>,
    /// `refs/heads/master` moved to a commit that does not descend from
    /// what it read before.
    regressions: Vec<String>,
    /// `git fsck --full` under the turn lock: runs, and what they found.
    fscks: u64,
    fsck_problems: Vec<String>,
    /// The ref under the turn lock was not the last acknowledged commit.
    stale: Vec<String>,
    /// Reads that failed for another reason than a missing object
    /// (reported, not fatal: a reader is killed now and then).
    read_errors: Vec<String>,
}

/// `git args...` with a deadline: a reader must not hang the run on a
/// mount that stopped answering.
fn git_within(
    repo: &Path,
    home: &Path,
    args: &[&str],
    within: Duration,
) -> Result<std::process::Output> {
    use std::io::Read;
    use std::process::Stdio;
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(GIT_CONFIG)
        .args(args)
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("running git {args:?}"))?;
    let mut out = child.stdout.take().unwrap();
    let mut err = child.stderr.take().unwrap();
    let out_t = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = out.read_to_end(&mut v);
        v
    });
    let err_t = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = err.read_to_end(&mut v);
        v
    });
    let t = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if t.elapsed() > within {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "git {args:?} in {} did not finish within {within:?}",
                repo.display()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    Ok(std::process::Output {
        status,
        stdout: out_t.join().unwrap_or_default(),
        stderr: err_t.join().unwrap_or_default(),
    })
}

/// UTC time of day (`HH:MM:SS.mmmZ`), to line a finding up with the
/// daemons' logs.
fn wall() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let s = ms / 1000;
    format!(
        "{:02}:{:02}:{:02}.{:03}Z",
        (s / 3600) % 24,
        (s / 60) % 60,
        s % 60,
        ms % 1000
    )
}

fn is_oid(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Whether the loose object is visible: `Ok(true)` present, `Ok(false)`
/// `ENOENT`, `Err` anything else.
fn object_visible(git_dir: &Path, oid: &str) -> std::io::Result<bool> {
    match std::fs::metadata(git_dir.join("objects").join(&oid[..2]).join(&oid[2..])) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Which of `oids` git cannot find (`cat-file --batch-check`): what is
/// not loose may be in a pack. Only asked when the repository has a pack
/// index at all, which with [`GIT_CONFIG`] only `git-under-flock-gc`'s
/// `git gc` writes.
fn missing_from_packs(
    repo: &Path,
    git_dir: &Path,
    home: &Path,
    oids: &[String],
) -> Result<Vec<String>> {
    let has_pack = std::fs::read_dir(git_dir.join("objects/pack"))
        .map(|d| {
            d.filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().ends_with(".idx"))
        })
        .unwrap_or(false);
    if !has_pack || oids.is_empty() {
        return Ok(oids.to_vec());
    }
    let (repo, home, input) = (
        repo.to_path_buf(),
        home.to_path_buf(),
        oids.join("\n") + "\n",
    );
    let out = bounded(
        "git cat-file --batch-check",
        Duration::from_secs(60),
        move || {
            use std::io::Write;
            use std::process::Stdio;
            let mut child = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(GIT_CONFIG)
                .args(["cat-file", "--batch-check=%(objectname)"])
                .env("HOME", &home)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            // Fed from a thread of its own: git answers while it reads,
            // and a list longer than the pipe would otherwise block both
            // ends. Stdin closes when the writer drops it.
            let mut stdin = child.stdin.take().unwrap();
            let feeder = std::thread::spawn(move || stdin.write_all(input.as_bytes()));
            let out = child.wait_with_output()?;
            feeder.join().unwrap_or(Ok(()))?;
            Ok::<_, std::io::Error>(out)
        },
    )??;
    if !out.status.success() {
        bail!(
            "cat-file --batch-check: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    // A missing object is answered `<oid> missing`.
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.strip_suffix(" missing"))
        .map(str::to_string)
        .collect())
}

/// Which incarnation of the mount `mnt` is: the inode of its [`ALIVE`]
/// file, which `mount_live` creates anew on every mount and `mark_dead`
/// removes before a kill. `None` while the mount is dead.
fn mount_incarnation(mnt: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(mnt.join(ALIVE)).ok().map(|m| m.ino())
}

/// Everything commit `oid` needs: itself, its tree, every tree and blob
/// under it — all written by git before the commit was published. `Err`
/// when the commit or its tree cannot be read.
fn closure_of(repo: &Path, home: &Path, oid: &str) -> Result<Vec<String>> {
    let commit = git_within(
        repo,
        home,
        &["cat-file", "-p", oid],
        Duration::from_secs(60),
    )?;
    if !commit.status.success() {
        bail!(
            "cat-file -p {oid}: {}",
            String::from_utf8_lossy(&commit.stderr).trim()
        );
    }
    let text = String::from_utf8_lossy(&commit.stdout);
    let tree = text
        .lines()
        .find_map(|l| l.strip_prefix("tree "))
        .context("a commit without a tree line")?
        .to_string();
    let listing = git_within(
        repo,
        home,
        &["ls-tree", "-r", "-t", &tree],
        Duration::from_secs(120),
    )?;
    if !listing.status.success() {
        bail!(
            "ls-tree {tree}: {}",
            String::from_utf8_lossy(&listing.stderr).trim()
        );
    }
    let mut objects = vec![oid.to_string(), tree];
    for line in String::from_utf8_lossy(&listing.stdout).lines() {
        // `<mode> <type> <oid>\t<path>`
        if let Some(o) = line.split_whitespace().nth(2) {
            if is_oid(o) {
                objects.push(o.to_string());
            }
        }
    }
    Ok(objects)
}

/// What one commit's check found.
enum Check {
    /// Every object it needs was visible.
    Clean,
    /// A publication visible before what it depends on.
    Violation(String),
    /// The reader's own mount was killed or remounted before anything was
    /// seen missing on a live mount: a dead mount's empty mountpoint says
    /// nothing about causal order. The commit is checked again.
    Interrupted,
}

/// The commit `oid`, named by `via` (a reflog line, the ref), must have
/// every object it depends on visible — loose or packed. What is missing
/// is polled until it appears (how long that took is part of the
/// finding) or 15 s pass. Only what a live mount showed counts: a look
/// that the mount's death or remount overlapped is not a finding.
fn check_publication(
    mnt: &Path,
    repo: &Path,
    git_dir: &Path,
    home: &Path,
    oid: &str,
    via: &str,
) -> Check {
    let t = Instant::now();
    let mut first: Option<String> = None;
    loop {
        let incarnation = mount_incarnation(mnt);
        let problem = match object_visible(git_dir, oid) {
            Err(e) => Some(format!("the commit object {oid}: {e}")),
            Ok(loose) => {
                let commit_missing = if loose {
                    Ok(Vec::new())
                } else {
                    missing_from_packs(repo, git_dir, home, &[oid.to_string()])
                };
                match commit_missing {
                    Err(e) => Some(format!("{e:#}")),
                    Ok(m) if !m.is_empty() => Some(format!("the commit object {oid} is missing")),
                    Ok(_) => match closure_of(repo, home, oid) {
                        Err(e) => Some(format!("{e:#}")),
                        Ok(objects) => {
                            let mut not_loose = Vec::new();
                            let mut errors = Vec::new();
                            for o in &objects {
                                match object_visible(git_dir, o) {
                                    Ok(true) => {}
                                    Ok(false) => not_loose.push(o.clone()),
                                    Err(e) => errors.push(format!("{o}: {e}")),
                                }
                            }
                            let missing = missing_from_packs(repo, git_dir, home, &not_loose)
                                .unwrap_or_else(|e| {
                                    errors.push(format!("{e:#}"));
                                    not_loose
                                });
                            if missing.is_empty() && errors.is_empty() {
                                None
                            } else {
                                Some(format!(
                                    "{} of the {} objects it needs missing{}{}",
                                    missing.len(),
                                    objects.len(),
                                    missing
                                        .iter()
                                        .take(3)
                                        .map(|m| format!(" {m}"))
                                        .collect::<String>(),
                                    errors
                                        .iter()
                                        .take(3)
                                        .map(|e| format!("; {e}"))
                                        .collect::<String>()
                                ))
                            }
                        }
                    },
                }
            }
        };
        if incarnation.is_none() || mount_incarnation(mnt) != incarnation {
            return match first {
                None => Check::Interrupted,
                Some(p) => Check::Violation(format!(
                    "{via} named {oid} with {p}; the reader's mount was restarted {:?} later, before it resolved",
                    t.elapsed()
                )),
            };
        }
        match problem {
            None => {
                return match first {
                    None => Check::Clean,
                    Some(p) => Check::Violation(format!(
                        "{via} named {oid} with {p}; all visible after {:?}",
                        t.elapsed()
                    )),
                };
            }
            Some(p) => {
                let p = format!("{p} (at {})", wall());
                if first.is_none() {
                    first = Some(p.clone());
                }
                if t.elapsed() > Duration::from_secs(15) {
                    return Check::Violation(format!(
                        "{via} named {oid} with {}; still {p} after {:?}",
                        first.unwrap(),
                        t.elapsed()
                    ));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Under the turn lock (no commit in flight): the ref must be the last
/// acknowledged commit, and `git fsck --full` must be clean.
fn fsck_under_lock(
    name: &str,
    mnt: &Path,
    repo: &Path,
    home: &Path,
    paths: &Paths,
    shared: &Shared,
    log: &Mutex<ReaderLog>,
) {
    let lf = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(mnt.join(&paths.turn))
    {
        Ok(f) => f,
        Err(e) => {
            log.lock()
                .unwrap()
                .read_errors
                .push(format!("{name}: opening the turn file: {e}"));
            return;
        }
    };
    if let Err(e) = flock(&lf, libc::LOCK_EX) {
        log.lock()
            .unwrap()
            .read_errors
            .push(format!("{name}: flock: {e}"));
        return;
    }
    let want = shared.last.lock().unwrap().clone();
    if let Some(want) = want {
        let seen = master_ref(repo);
        if seen != want {
            let t = Instant::now();
            let mut now = seen.clone();
            while now != want && t.elapsed() < Duration::from_secs(5) {
                std::thread::sleep(Duration::from_millis(20));
                now = master_ref(repo);
            }
            log.lock().unwrap().stale.push(format!(
                "{name}: under the turn lock refs/heads/master read {seen}; the last acknowledged commit is {want}; {}",
                if now == want {
                    format!("caught up after {:?}", t.elapsed())
                } else {
                    format!("still {now} after {:?}", t.elapsed())
                }
            ));
        }
    }
    let out = git_within(
        repo,
        home,
        &["fsck", "--full", "--no-dangling"],
        Duration::from_secs(600),
    );
    let mut l = log.lock().unwrap();
    l.fscks += 1;
    match out {
        Ok(o) => {
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            );
            if !o.status.success() || text.contains("missing") || text.contains("invalid reflog") {
                l.fsck_problems.push(format!(
                    "{name}: git fsck --full under the turn lock: {}",
                    text.lines().take(6).collect::<Vec<_>>().join(" | ")
                ));
            }
        }
        Err(e) => l.fsck_problems.push(format!("{name}: {e:#}")),
    }
    drop(l);
    let _ = flock(&lf, libc::LOCK_UN);
}

/// One reader: every 50 ms, read the reflog and the ref through this
/// mount and check every commit they newly name (`check_publication`);
/// the ref must only ever move to a descendant; every `fsck_every`,
/// `fsck_under_lock`. A dead mount (killed, being remounted) is waited
/// out, and the reflog and ref are re-read from scratch afterwards.
#[allow(clippy::too_many_arguments)]
fn reader(
    name: String,
    mnt: PathBuf,
    home: PathBuf,
    stop: Arc<AtomicBool>,
    paths: Paths,
    log: Arc<Mutex<ReaderLog>>,
    shared: Arc<Shared>,
    quiet: Arc<Mutex<()>>,
    fsck_every: Duration,
) {
    let repo = mnt.join(&paths.repo);
    let git_dir = repo.join(".git");
    let mut checked: BTreeSet<String> = BTreeSet::new();
    let mut reflog_lines = 0usize;
    let mut last_ref: Option<String> = None;
    let mut last_fsck = Instant::now();
    let mut alive = true;
    while !stop.load(Ordering::SeqCst) {
        if !mnt.join(ALIVE).exists() {
            alive = false;
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        if !alive {
            alive = true;
            reflog_lines = 0;
            last_ref = None;
        }
        // The reflog: every line names the commit the ref moved to.
        match std::fs::read_to_string(git_dir.join("logs/HEAD")) {
            Ok(text) => {
                let lines: Vec<&str> = text.lines().collect();
                if lines.len() < reflog_lines {
                    reflog_lines = 0;
                }
                let mut upto = lines.len();
                for (k, line) in lines.iter().enumerate().skip(reflog_lines) {
                    let Some(oid) = line.split(' ').nth(1) else {
                        continue;
                    };
                    if is_oid(oid) && checked.insert(oid.to_string()) {
                        match check_publication(
                            &mnt,
                            &repo,
                            &git_dir,
                            &home,
                            oid,
                            &format!("reflog line {}", k + 1),
                        ) {
                            Check::Clean => log.lock().unwrap().checked += 1,
                            Check::Violation(p) => {
                                let mut l = log.lock().unwrap();
                                l.checked += 1;
                                l.violations.push(format!("{name}: {p}"));
                            }
                            Check::Interrupted => {
                                // Again from this line on the next mount.
                                checked.remove(oid);
                                upto = k;
                                break;
                            }
                        }
                    }
                }
                reflog_lines = upto;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                let mut l = log.lock().unwrap();
                if l.read_errors.len() < 50 {
                    l.read_errors
                        .push(format!("{name}: reading logs/HEAD: {e}"));
                }
            }
        }
        // The ref.
        let r = master_ref(&repo);
        if is_oid(&r) && last_ref.as_deref() != Some(r.as_str()) {
            if checked.insert(r.clone()) {
                match check_publication(&mnt, &repo, &git_dir, &home, &r, "refs/heads/master") {
                    Check::Clean => log.lock().unwrap().checked += 1,
                    Check::Violation(p) => {
                        let mut l = log.lock().unwrap();
                        l.checked += 1;
                        l.violations.push(format!("{name}: {p}"));
                    }
                    Check::Interrupted => {
                        checked.remove(&r);
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                }
            }
            if let Some(prev) = &last_ref {
                match git_within(
                    &repo,
                    &home,
                    &["merge-base", "--is-ancestor", prev, &r],
                    Duration::from_secs(60),
                ) {
                    Ok(o) if o.status.success() => {}
                    Ok(o) if o.status.code() == Some(1) => {
                        log.lock().unwrap().regressions.push(format!(
                            "{name}: refs/heads/master went from {prev} to {r}, which does not descend from it"
                        ));
                    }
                    Ok(o) => {
                        let mut l = log.lock().unwrap();
                        if l.read_errors.len() < 50 {
                            l.read_errors.push(format!(
                                "{name}: merge-base --is-ancestor {prev} {r}: {}",
                                String::from_utf8_lossy(&o.stderr).trim()
                            ));
                        }
                    }
                    Err(e) => {
                        let mut l = log.lock().unwrap();
                        if l.read_errors.len() < 50 {
                            l.read_errors.push(format!("{name}: {e:#}"));
                        }
                    }
                }
            }
            last_ref = Some(r);
        }
        if last_fsck.elapsed() >= fsck_every {
            last_fsck = Instant::now();
            let _quiet = quiet.lock().unwrap();
            if mnt.join(ALIVE).exists() {
                fsck_under_lock(&name, &mnt, &repo, &home, &paths, &shared, &log);
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The turns, judged: `(overlaps, fenced overlaps, deciles, longest)` —
/// two turns that held the lock at once (a broken lock), and the turn
/// durations by decile. A turn that overlaps a *fenced* earlier one (its
/// committer's grant lapsed and its node refused its git with `EIO` from
/// then on) is not a broken lock, as long as the earlier turn wrote
/// nothing after the later one got the lock: the fence is what keeps the
/// two from both writing.
fn judge_turns(turns: &[Turn], base: Instant) -> (Vec<String>, Vec<String>, String, Duration) {
    let mut t: Vec<Turn> = turns.to_vec();
    t.sort_by_key(|x| x.got);
    let mut overlaps = Vec::new();
    let mut fenced = Vec::new();
    let mut open: Option<&Turn> = None;
    for x in &t {
        if let Some(prev) = open {
            if x.got < prev.released && x.who != prev.who {
                let fenced_in_time = prev.fenced && prev.wrote_until.is_none_or(|t| t <= x.got);
                let into = if fenced_in_time {
                    &mut fenced
                } else {
                    &mut overlaps
                };
                // Relative to the later turn's `got`: what of the earlier
                // turn ran before (-) and after (+) it.
                let rel = |t: Instant| {
                    if t >= x.got {
                        format!("+{:.3}s", (t - x.got).as_secs_f64())
                    } else {
                        format!("-{:.3}s", (x.got - t).as_secs_f64())
                    }
                };
                let timeline: Vec<String> = prev
                    .began
                    .iter()
                    .map(|(step, at)| format!("{step}@{}", rel(*at)))
                    .collect();
                into.push(format!(
                    "{}#{} held the lock {:?}..{:?} while {}#{} got it at {:?} \
                     ({}#{}: {}, last write done {}, released {})",
                    prev.who,
                    prev.i,
                    prev.got - base,
                    prev.released - base,
                    x.who,
                    x.i,
                    x.got - base,
                    prev.who,
                    prev.i,
                    timeline.join(" "),
                    prev.wrote_until.map_or("never".into(), rel),
                    rel(prev.released),
                ));
            }
        }
        if open.is_none_or(|p| x.released > p.released) {
            open = Some(x);
        }
    }
    let mut slowest: Vec<&Turn> = t.iter().collect();
    slowest.sort_by_key(|x| std::cmp::Reverse(x.released - x.got));
    for x in slowest.iter().take(3) {
        eprintln!(
            "      slow turn {}#{} at {:?}: {:?} {:?}",
            x.who,
            x.i,
            x.got - base,
            x.released - x.got,
            x.steps
        );
    }
    // Fairness (EC2 campaign 8 B-1, which measured this, not staleness:
    // its "stale local HEAD" fired whenever a committer got two turns in
    // a row): turns granted to a committer while the other one had asked
    // for the lock earlier and was still waiting, and the longest wait.
    let mut jumped = 0usize;
    let mut consecutive = 0usize;
    for (k, x) in t.iter().enumerate() {
        if k > 0 && t[k - 1].who == x.who {
            consecutive += 1;
        }
        if t.iter()
            .any(|y| y.who != x.who && y.asked < x.asked && y.got > x.got)
        {
            jumped += 1;
        }
    }
    let mut waits: Vec<Duration> = t.iter().map(|x| x.got - x.asked).collect();
    waits.sort();
    if !waits.is_empty() {
        eprintln!(
            "      lock acquire wait: p50 {:?} p90 {:?} max {:?}; {consecutive} of {} turns followed the same committer's turn, {jumped} were granted ahead of an earlier waiter",
            waits[waits.len() / 2],
            waits[waits.len() * 9 / 10],
            waits[waits.len() - 1],
            t.len()
        );
    }
    let mut durations: Vec<Duration> = t.iter().map(|x| x.released - x.got).collect();
    let max = durations.iter().copied().max().unwrap_or_default();
    let deciles = if durations.len() >= 10 {
        let n = durations.len();
        (0..10)
            .map(|d| {
                let mut chunk: Vec<Duration> = durations[d * n / 10..(d + 1) * n / 10].to_vec();
                chunk.sort();
                format!("{:.1}", chunk[chunk.len() / 2].as_secs_f64())
            })
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        durations.sort();
        format!("{durations:?}")
    };
    (overlaps, fenced, deciles, max)
}

/// `refs/heads/master` as git resolves it, read straight from the files
/// (no git process under the lock): the loose ref, else its line in
/// `packed-refs` (`git gc` packs refs and deletes the loose file).
fn master_ref(repo: &Path) -> String {
    let git = repo.join(".git");
    match std::fs::read_to_string(git.join("refs/heads/master")) {
        Ok(s) => s.trim().to_string(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match std::fs::read_to_string(git.join("packed-refs")) {
                Ok(packed) => packed
                    .lines()
                    .find_map(|l| l.strip_suffix(" refs/heads/master"))
                    .map(str::to_string)
                    .unwrap_or_else(|| "<no master in packed-refs>".into()),
                Err(e) => format!("<packed-refs: {e}>"),
            }
        }
        Err(e) => format!("<{e}>"),
    }
}

fn mount_live(c: &mut Client) -> Result<()> {
    let mut last = None;
    for _ in 0..5 {
        match c.mount() {
            Ok(()) => {
                std::fs::write(c.mnt.join(ALIVE), b"")?;
                return Ok(());
            }
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
    Err(last.unwrap().context(format!("remounting {}", c.name)))
}

/// Mark `c` dead for its worker before a kill: the worker re-checks
/// before every commit.
fn mark_dead(c: &Client) {
    let _ = std::fs::remove_file(c.mnt.join(ALIVE));
}

/// Abort `mnt`'s FUSE connection (what the kernel does when the daemon's
/// `/dev/fuse` closes). A `kill -9` alone could leave a git process's
/// request waiting on the dead mount — and with it the lazy unmount
/// that follows — for good: the connection stayed open behind the dead
/// daemon (seen once in `git-under-flock-faults`).
fn abort_fuse(mnt: &Path) {
    let Ok(info) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return;
    };
    let want = mnt.display().to_string();
    for line in info.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        if f.len() > 4 && f[4] == want {
            if let Some((_, minor)) = f[2].split_once(':') {
                let _ = std::fs::write(format!("/sys/fs/fuse/connections/{minor}/abort"), "1");
            }
        }
    }
}

fn crash(c: &mut Client) -> Result<()> {
    abort_fuse(&c.mnt);
    c.kill9()
}

struct Fleet<'a> {
    root: &'a Path,
    ids: Vec<u64>,
    /// Each node's own S3 path (a cut isolates one node from the bucket,
    /// as the soak's iptables rule did).
    s3: Vec<CountingProxy>,
    /// Stalled FUSE requests seen on any node during the workload
    /// (`fuse_stalls`): a request unanswered past the daemon's stall
    /// threshold is a bug whatever the faults (EC2 campaign 7 B-2).
    stalls: Mutex<Vec<String>>,
}

impl Fleet<'_> {
    fn isolate(&self, clients: &[Client], who: usize) -> Result<()> {
        for (j, c) in clients.iter().enumerate() {
            let deny: Vec<String> = if j == who {
                (0..clients.len())
                    .filter(|k| *k != who)
                    .map(|k| self.ids[k].to_string())
                    .collect()
            } else {
                vec![self.ids[who].to_string()]
            };
            std::fs::write(c_deny_path(self.root, &c.name), deny.join("\n") + "\n")?;
        }
        Ok(())
    }

    fn heal_p2p(&self, clients: &[Client]) {
        for c in clients {
            let _ = std::fs::remove_file(c_deny_path(self.root, &c.name));
        }
    }

    fn holder(&self, clients: &[Client]) -> Option<usize> {
        clients
            .iter()
            .position(|c| lease_of(c).map(|l| l["held"] == true).unwrap_or(false))
    }

    /// One seeded fault, healed before it returns.
    fn fault(&self, rng: &mut StdRng, clients: &mut [Client], n: u64) -> Result<String> {
        for p in &self.s3 {
            // Only the relay is wanted, not the request record.
            p.reset();
        }
        let len = clients.len();
        let pick = rng.random_range(0..100u32);
        let target = if rng.random_bool(0.5) {
            self.holder(clients).unwrap_or(0)
        } else {
            rng.random_range(0..len)
        };
        let name = clients[target].name.clone();
        Ok(match pick {
            0..=34 => {
                mark_dead(&clients[target]);
                std::thread::sleep(Duration::from_millis(rng.random_range(0..300)));
                crash(&mut clients[target])?;
                std::thread::sleep(Duration::from_millis(rng.random_range(500..3000)));
                mount_live(&mut clients[target])?;
                format!("#{n} kill -9 {name}")
            }
            35..=44 => {
                for c in clients.iter() {
                    mark_dead(c);
                }
                for c in clients.iter_mut() {
                    crash(c)?;
                }
                std::thread::sleep(Duration::from_millis(rng.random_range(500..2000)));
                for c in clients.iter_mut() {
                    mount_live(c)?;
                }
                let refs: Vec<&Client> = clients.iter().collect();
                wait_for_p2p(&refs)?;
                format!("#{n} kill -9 of the whole cluster")
            }
            45..=59 => {
                let ms = rng.random_range(2000..8000);
                clients[target].pause()?;
                std::thread::sleep(Duration::from_millis(ms));
                clients[target].resume()?;
                format!("#{n} SIGSTOP {name} {ms} ms")
            }
            60..=79 => {
                let ms = rng.random_range(3000..10000);
                self.isolate(clients, target)?;
                std::thread::sleep(Duration::from_millis(ms));
                self.heal_p2p(clients);
                format!("#{n} P2P isolation of {name} {ms} ms")
            }
            _ => {
                let ms = rng.random_range(3000..10000);
                self.s3[target].cut();
                std::thread::sleep(Duration::from_millis(ms));
                self.s3[target].heal();
                self.s3[target].reset();
                format!("#{n} S3 cut of {name} {ms} ms")
            }
        })
    }
}

/// Per-node git verdict.
struct GitView {
    head: String,
    commits: usize,
    fsck_ok: bool,
    fsck: String,
    missing_acked: Vec<String>,
    unreachable_acked: Vec<String>,
}

fn git_view(repo: &Path, home: &Path, acked: &[String]) -> Result<GitView> {
    let repo = repo.to_path_buf();
    let head = git_ok(&repo, home, &["rev-parse", "HEAD"])?;
    let log = git(&repo, home, &["log", "--oneline"])?;
    let commits = String::from_utf8_lossy(&log.stdout).lines().count();
    let fsck = git(&repo, home, &["fsck", "--full", "--no-dangling"])?;
    let fsck_text = format!(
        "{}{}",
        String::from_utf8_lossy(&fsck.stdout),
        String::from_utf8_lossy(&fsck.stderr)
    );
    let mut missing_acked = Vec::new();
    let mut unreachable_acked = Vec::new();
    for c in acked {
        let present = git(&repo, home, &["cat-file", "-e", c])?.status.success();
        if !present {
            missing_acked.push(c.clone());
            continue;
        }
        let anc = git(&repo, home, &["merge-base", "--is-ancestor", c, &head])?;
        if !anc.status.success() {
            unreachable_acked.push(c.clone());
        }
    }
    Ok(GitView {
        head,
        commits,
        fsck_ok: fsck.status.success() && !fsck_text.contains("missing"),
        fsck: fsck_text,
        missing_acked,
        unreachable_acked,
    })
}

/// One round's outcome.
struct RoundResult {
    paths: Paths,
    acked: Vec<String>,
}

fn run(scenario: &str, seed: u64, variant: Variant) -> Result<()> {
    if Command::new("git").arg("--version").output().is_err() {
        bail!("git is not installed");
    }
    let secs = env_u64(
        "GIT_FLOCK_SECS",
        match variant {
            Variant::Faults => 150,
            Variant::B2b | Variant::Causal => 180,
            Variant::Rounds => 90,
            _ => 60,
        },
    );
    let rounds = if variant == Variant::Rounds {
        env_u64("GIT_FLOCK_ROUNDS", 3).max(1) as usize
    } else {
        1
    };
    // Causal: two committers and at least one reader.
    let n_nodes = if variant == Variant::Causal {
        env_u64("GIT_FLOCK_NODES", 3).clamp(3, 4) as usize
    } else {
        env_u64("GIT_FLOCK_NODES", 4).clamp(2, 4) as usize
    };
    let (env, root) = setup(scenario)?;
    let proxy = env.s3_proxy()?;
    // `GIT_FLOCK_S3_LATENCY_MS`: a slower bucket, so a follower's replica
    // trails the holder's journal by more than a lock hand-over.
    let latency = env_u64("GIT_FLOCK_S3_LATENCY_MS", 0);
    if latency > 0 {
        proxy.latency(latency, latency / 5)?;
    }
    let backend = format!("s3://{BUCKET}/gitflock-{}", ts());
    let home = root.path().join("home");
    std::fs::create_dir_all(&home)?;
    let names = ["a", "b", "c", "d"];
    let mut clients = Vec::new();
    let mut s3 = Vec::new();
    for name in &names[..n_nodes] {
        let proxy = env.counting_proxy()?;
        let endpoint = proxy.endpoint();
        s3.push(proxy);
        clients.push(
            Client::new(root.path(), name, &endpoint, &backend)?
                .with_own_node_key()
                .with_env(
                    "CONSTELLATION_FAULT_P2P_DENY_FILE",
                    &c_deny_path(root.path(), name).display().to_string(),
                ),
        );
    }
    // The FUSE request watchdog's threshold (EC2 campaign 7 B-2): a
    // request unanswered past it fails the scenario at the end. Above
    // the longest wait the faults legitimately cause — the first write
    // after a `kill -9` of the whole cluster waits for the dead lease to
    // expire and a takeover (2 x TTL, ~50 s here); the B-2 hang is for
    // good. `GIT_FLOCK_ENV` may override it.
    if !std::env::var("GIT_FLOCK_ENV")
        .is_ok_and(|e| e.contains("CONSTELLATION_FUSE_REQUEST_STALL_S"))
    {
        clients = clients
            .into_iter()
            .map(|c| c.with_env("CONSTELLATION_FUSE_REQUEST_STALL_S", "90"))
            .collect();
    }
    // `GIT_FLOCK_ENV=K=V,K=V`: extra mount environment for every node.
    if let Ok(extra) = std::env::var("GIT_FLOCK_ENV") {
        for kv in extra.split(',').filter(|s| !s.is_empty()) {
            let (k, v) = kv.split_once('=').context("GIT_FLOCK_ENV: K=V,K=V")?;
            clients = clients.into_iter().map(|c| c.with_env(k, v)).collect();
        }
    }
    // `GIT_FLOCK_RUST_LOG`: the daemons' `RUST_LOG` (commas included).
    if let Ok(filter) = std::env::var("GIT_FLOCK_RUST_LOG") {
        clients = clients
            .into_iter()
            .map(|c| c.with_env("RUST_LOG", &filter))
            .collect();
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        mount_live(c)?;
    }
    {
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
    }
    let mut ids = Vec::new();
    for c in &clients {
        ids.push(node_id(c)?);
    }
    let fleet = Fleet {
        root: root.path(),
        ids,
        s3,
        stalls: Mutex::new(Vec::new()),
    };
    let result = (|| -> Result<()> {
        let mut done: Vec<RoundResult> = Vec::new();
        for k in 0..rounds {
            let paths = if variant == Variant::Rounds {
                Paths::round(k + 1)
            } else {
                Paths::legacy()
            };
            let label = if rounds > 1 {
                format!("{scenario} round {}/{rounds}", k + 1)
            } else {
                scenario.to_string()
            };
            let acked = workload(
                &label,
                seed.wrapping_add(k as u64 * 1000),
                variant,
                secs,
                &paths,
                &home,
                &fleet,
                &mut clients,
            )?;
            done.push(RoundResult { paths, acked });
            // Every round but the last is checked on the live nodes only;
            // the last one also mounts a fresh node and checks every round.
            let last = k + 1 == rounds;
            let check: Vec<&RoundResult> = if last {
                done.iter().collect()
            } else {
                vec![done.last().unwrap()]
            };
            verify(
                &env,
                root.path(),
                &backend,
                &clients,
                &home,
                &check,
                last,
                &label,
            )?;
            // No FUSE request may have gone unanswered past the daemon's
            // stall threshold on any node, faults or not (EC2 campaign 7
            // B-2: a `getattr` and a `read` that hung for good).
            let mut stalls = fleet.stalls.lock().unwrap().clone();
            for line in fuse_stalls(&clients) {
                if !stalls.contains(&line) {
                    stalls.push(line);
                }
            }
            if !stalls.is_empty() {
                bail!(
                    "FUSE requests stalled (unanswered past the stall threshold):\n    {}",
                    stalls.join("\n    ")
                );
            }
            eprintln!("    {label}: no FUSE request stalled on any node");
        }
        Ok(())
    })();
    fleet.heal_p2p(&clients);
    for p in &fleet.s3 {
        p.heal();
    }
    if result.is_err() {
        let dir = std::env::temp_dir().join(format!("harness-{scenario}-logs-{}", ts()));
        if std::fs::create_dir_all(&dir).is_ok() {
            for c in &clients {
                let _ = std::fs::write(dir.join(format!("{}.log", c.name)), c.log_text());
                // Earlier incarnations too (a `kill -9` started a new
                // log): the stall a fault phase caused is in one of them.
                for (i, path) in c.log_files().iter().enumerate() {
                    if path.extension().is_some_and(|e| e != "log") {
                        let _ = std::fs::copy(path, dir.join(format!("{}.log.{}", c.name, i + 1)));
                    }
                }
                if let Ok(status) = c.control_status() {
                    let _ = std::fs::write(
                        dir.join(format!("{}.status.json", c.name)),
                        status.to_string(),
                    );
                }
            }
            eprintln!("    {scenario}: mount logs kept in {}", dir.display());
        }
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// The committers' nodes' `[lost, owners_fenced, owner_fenced_ops]` lock
/// counters, summed over the workload: sampled now and then, a counter
/// that went down is a restarted daemon's (its new count is all new).
struct FenceTally {
    last: Vec<Option<[u64; 3]>>,
    sum: Vec<[u64; 3]>,
}

impl FenceTally {
    fn new(clients: &[Client], committers: &[usize]) -> FenceTally {
        let mut t = FenceTally {
            last: vec![None; committers.len()],
            sum: vec![[0; 3]; committers.len()],
        };
        t.sample(clients, committers);
        t.sum = vec![[0; 3]; committers.len()];
        t
    }

    fn sample(&mut self, clients: &[Client], committers: &[usize]) {
        for (k, &c) in committers.iter().enumerate() {
            let Ok(s) = clients[c].control_status() else {
                continue;
            };
            let l = &s["locks"];
            let now = ["lost", "owners_fenced", "owner_fenced_ops"]
                .map(|key| l[key].as_u64().unwrap_or(0));
            let was = self.last[k].unwrap_or([0; 3]);
            for j in 0..3 {
                // Down: a restarted daemon, all of its count is new.
                self.sum[k][j] += if now[j] >= was[j] {
                    now[j] - was[j]
                } else {
                    now[j]
                };
            }
            self.last[k] = Some(now);
        }
    }
}

/// One workload: the repository initialized, the two committers taking
/// turns for `secs` (with faults for `Variant::Faults`), then the turn
/// checks. Returns the acknowledged commits.
#[allow(clippy::too_many_arguments)]
fn workload(
    label: &str,
    seed: u64,
    variant: Variant,
    secs: u64,
    paths: &Paths,
    home: &Path,
    fleet: &Fleet<'_>,
    clients: &mut [Client],
) -> Result<Vec<String>> {
    let m0 = clients[0].mnt.clone();
    if let Some(parent) = m0.join(&paths.turn).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(m0.join(&paths.turn), b"")?;
    std::fs::create_dir_all(m0.join(&paths.repo))?;
    git_ok(&m0.join(&paths.repo), home, &["init", "-q"])?;
    for c in &clients[1..] {
        eventually(
            &format!("the repo reaches {}", c.name),
            Duration::from_secs(60),
            || {
                anyhow::ensure!(c.mnt.join(&paths.repo).join(".git/HEAD").exists());
                anyhow::ensure!(c.mnt.join(&paths.turn).exists());
                Ok(())
            },
        )?;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let logs: Vec<Arc<Mutex<WorkerLog>>> = (0..2).map(|_| Arc::default()).collect();
    let shared = Arc::new(Shared {
        last: Mutex::new(None),
        turns: Mutex::new(Vec::new()),
    });
    // The committers: nodes 0 and 1 (node 0 created the filesystem and
    // starts as the lease holder), or with `GIT_FLOCK_COMMITTERS=last`
    // the last two, so that neither committer is the sequencer.
    let committers: Vec<usize> = if std::env::var("GIT_FLOCK_COMMITTERS").as_deref() == Ok("last") {
        vec![clients.len() - 2, clients.len() - 1]
    } else {
        vec![0, 1]
    };
    let mut tally = FenceTally::new(clients, &committers);
    let started = Instant::now();
    let workers: Vec<_> = (0..2)
        .map(|k| {
            let c = &clients[committers[k]];
            let (name, mnt) = (c.name.clone(), c.mnt.clone());
            let state = c.state_dir().to_path_buf();
            let (home, stop, log) = (home.to_path_buf(), stop.clone(), logs[k].clone());
            let (shared, paths) = (shared.clone(), paths.clone());
            let seed = seed.wrapping_mul(31).wrapping_add(k as u64);
            std::thread::spawn(move || {
                committer(
                    name, mnt, state, home, stop, variant, paths, log, shared, seed,
                )
            })
        })
        .collect();
    // Causal: every other node reads, and is restarted now and then.
    let readers: Vec<usize> = if variant == Variant::Causal {
        (0..clients.len())
            .filter(|i| !committers.contains(i))
            .collect()
    } else {
        Vec::new()
    };
    let reader_logs: Vec<Arc<Mutex<ReaderLog>>> = readers.iter().map(|_| Arc::default()).collect();
    let reader_quiet: Vec<Arc<Mutex<()>>> = readers.iter().map(|_| Arc::default()).collect();
    let fsck_every = Duration::from_secs(env_u64("GIT_FLOCK_FSCK_EVERY_S", 20));
    let reader_threads: Vec<_> = readers
        .iter()
        .enumerate()
        .map(|(k, &i)| {
            let c = &clients[i];
            let (name, mnt) = (c.name.clone(), c.mnt.clone());
            let (home, stop, log) = (home.to_path_buf(), stop.clone(), reader_logs[k].clone());
            let (shared, paths, quiet) = (shared.clone(), paths.clone(), reader_quiet[k].clone());
            std::thread::spawn(move || {
                reader(name, mnt, home, stop, paths, log, shared, quiet, fsck_every)
            })
        })
        .collect();
    let restarts_wanted = if variant == Variant::Causal && !readers.is_empty() {
        env_u64("GIT_FLOCK_READER_RESTARTS", 2)
    } else {
        0
    };
    let mut restarts_done = 0u64;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut faults = Vec::new();
    let mut fault_err = None;
    while started.elapsed() < Duration::from_secs(secs) {
        tally.sample(clients, &committers);
        if variant == Variant::Faults {
            std::thread::sleep(Duration::from_millis(rng.random_range(2000..6000)));
            tally.sample(clients, &committers);
            match fleet.fault(&mut rng, clients, faults.len() as u64 + 1) {
                Ok(f) => {
                    eprintln!("    {label}: {f} ({:?})", started.elapsed());
                    faults.push(f);
                }
                Err(e) => {
                    fault_err = Some(e);
                    break;
                }
            }
            for line in fuse_stalls(clients) {
                let mut seen = fleet.stalls.lock().unwrap();
                if !seen.contains(&line) {
                    eprintln!("    {label}: STALL {line} ({:?})", started.elapsed());
                    seen.push(line);
                }
            }
        } else if restarts_done < restarts_wanted
            && started.elapsed()
                >= Duration::from_secs(secs * (restarts_done + 1) / (restarts_wanted + 1))
        {
            // A reader is killed and remounted; it keeps checking while
            // it catches up (campaign 7's part C: the errors right after
            // a killed node rejoined). Not during its fsck under the
            // turn lock.
            let k = rng.random_range(0..readers.len());
            let who = readers[k];
            let name = clients[who].name.clone();
            let r = (|| -> Result<()> {
                let _quiet = reader_quiet[k].lock().unwrap();
                mark_dead(&clients[who]);
                std::thread::sleep(Duration::from_millis(rng.random_range(0..300)));
                crash(&mut clients[who])?;
                std::thread::sleep(Duration::from_millis(rng.random_range(500..3000)));
                mount_live(&mut clients[who])
            })();
            restarts_done += 1;
            match r {
                Ok(()) => eprintln!(
                    "    {label}: reader {name} killed and remounted ({:?})",
                    started.elapsed()
                ),
                Err(e) => {
                    fault_err = Some(e.context(format!("restarting reader {name}")));
                    break;
                }
            }
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    stop.store(true, Ordering::SeqCst);
    for w in workers {
        let _ = w.join();
    }
    tally.sample(clients, &committers);
    for w in reader_threads {
        let _ = w.join();
    }
    if let Some(e) = fault_err {
        return Err(e.context("injecting a fault"));
    }
    for line in fuse_stalls(clients) {
        let mut seen = fleet.stalls.lock().unwrap();
        if !seen.contains(&line) {
            eprintln!("    {label}: STALL {line}");
            seen.push(line);
        }
    }
    let mut acked = Vec::new();
    let mut stale = Vec::new();
    let mut errors = 0;
    for (k, l) in logs.iter().enumerate() {
        let l = l.lock().unwrap();
        eprintln!(
            "    {label}: {} made {} commits ({} gc), {} errors{}",
            clients[committers[k]].name,
            l.acked.len(),
            l.gcs,
            l.errors.len(),
            l.errors
                .iter()
                .take(5)
                .map(|e| format!("\n      {e}"))
                .collect::<String>()
        );
        acked.extend(l.acked.iter().cloned());
        stale.extend(l.stale.iter().cloned());
        errors += l.errors.len();
    }
    for st in stale.iter().take(10) {
        eprintln!("    {label}: stale turn: {st}");
    }
    let mut turns = shared.turns.lock().unwrap().clone();
    // A fenced turn is one its node says it fenced: the committer's node
    // refused at least one op of a lapsed owner during the workload.
    let mut fence_counts = Vec::new();
    for (k, &c) in committers.iter().enumerate() {
        let [lost, owners, ops] = tally.sum[k];
        fence_counts.push(format!(
            "{} lost {lost} owners_fenced {owners} owner_fenced_ops {ops}",
            clients[c].name
        ));
        if ops == 0 {
            let name = &clients[c].name;
            let n = turns
                .iter_mut()
                .filter(|t| &t.who == name && t.fenced)
                .map(|t| t.fenced = false)
                .count();
            if n > 0 {
                eprintln!(
                    "    {label}: {n} turns of {name} failed with EIO but its node fenced no op: not counted as fenced"
                );
            }
        }
    }
    eprintln!(
        "    {label}: during the workload: {}",
        fence_counts.join("; ")
    );
    let (overlaps, fenced_overlaps, deciles, max) = judge_turns(&turns, started);
    eprintln!(
        "    {label}: {} turns, median duration per decile (s): {deciles}; longest {max:?}; \
         {} overlapping, {} overlapping a fenced turn, {} fenced",
        turns.len(),
        overlaps.len(),
        fenced_overlaps.len(),
        turns.iter().filter(|t| t.fenced).count()
    );
    for o in overlaps.iter().take(10) {
        eprintln!("    {label}: overlapping turns: {o}");
    }
    for o in fenced_overlaps.iter().take(10) {
        eprintln!("    {label}: a turn overlapping a fenced turn (its git got EIO): {o}");
    }
    for &k in &committers {
        if let Ok(s) = clients[k].control_status() {
            let l = &s["locks"];
            eprintln!(
                "    {label}: {} locks: lost {}, fenced_io {}, owners_fenced {}, owner_fenced_ops {}, first_use_abandoned {}",
                clients[k].name,
                l["lost"],
                l["fenced_io"],
                l["owners_fenced"],
                l["owner_fenced_ops"],
                l["first_use_abandoned"]
            );
        }
    }
    anyhow::ensure!(acked.len() >= 4, "only {} commits were made", acked.len());
    if variant == Variant::Faults {
        // Under faults a turn may begin before the previous turn's writes
        // are back (its holder, or the sequencer that acknowledged them,
        // crashed with them unshipped: they return when their requester
        // replays them by rid), and a killed node's lock is outwaited
        // while its turn is still in flight. Reported, not fatal; a
        // commit either would lose is caught by `verify`.
        if !stale.is_empty() || !overlaps.is_empty() {
            eprintln!(
                "    {label}: {} of {} turns began stale, {} overlapped (faults)",
                stale.len(),
                turns.len(),
                overlaps.len()
            );
        }
        return Ok(acked);
    }
    let mut problems = Vec::new();
    // Causal: a killed reader may be the sequencer or its backup, so a
    // restart is a fault for the committers: a lock grant lapses while
    // the turn is in flight (the design's documented limit: the turn
    // then runs unprotected), a forward to the dead sequencer fails a
    // git step. As under faults these are reported, not fatal; the
    // readers' checks and `verify` still are.
    let faulted = variant == Variant::Causal && restarts_done > 0;
    let mut report = |what: String| {
        if faulted {
            eprintln!("    {label}: {what} (a reader was killed and remounted; reported)");
        } else {
            problems.push(what);
        }
    };
    if !overlaps.is_empty() {
        report(format!(
            "{} turns held the turn lock at the same time as another (a broken lock), e.g. {}",
            overlaps.len(),
            overlaps[0]
        ));
    }
    if !stale.is_empty() {
        report(format!(
            "{} of {} turns began with a stale refs/heads/master or marker under the turn lock, e.g. {}",
            stale.len(),
            turns.len(),
            stale[0]
        ));
    }
    if variant.campaign_shape() {
        let limit = Duration::from_secs(env_u64("GIT_FLOCK_MAX_TURN_S", 30));
        if max > limit {
            report(format!(
                "the longest turn took {max:?} (limit {limit:?}; median per decile {deciles})"
            ));
        }
        if errors > 0 {
            report(format!("{errors} turns failed (see above)"));
        }
    }
    for (k, l) in reader_logs.iter().enumerate() {
        let l = l.lock().unwrap();
        let name = &clients[readers[k]].name;
        eprintln!(
            "    {label}: reader {name} checked {} commits ({} violations, {} regressions), {} fsck runs under the lock ({} failed, {} began stale), {} read errors{}",
            l.checked,
            l.violations.len(),
            l.regressions.len(),
            l.fscks,
            l.fsck_problems.len(),
            l.stale.len(),
            l.read_errors.len(),
            l.read_errors
                .iter()
                .take(5)
                .map(|e| format!("\n      {e}"))
                .collect::<String>()
        );
        anyhow::ensure!(
            l.checked >= 4,
            "reader {name} checked only {} commits",
            l.checked
        );
        for (what, list) in [
            (
                "publications visible before an object they depend on",
                &l.violations,
            ),
            (
                "ref moves to a commit not descending from the previous one",
                &l.regressions,
            ),
            ("fsck runs under the turn lock failed", &l.fsck_problems),
            (
                "fsck runs under the turn lock began with a stale ref",
                &l.stale,
            ),
        ] {
            if !list.is_empty() {
                problems.push(format!(
                    "reader {name}: {} {what}, e.g. {}",
                    list.len(),
                    list.iter().take(3).cloned().collect::<Vec<_>>().join("; ")
                ));
            }
        }
    }
    if variant == Variant::Causal {
        // Whether the run exercised delegation: the placement's grants
        // and recalls, the delegates' executions and what the root
        // appended from their streams.
        let n = |v: &serde_json::Value, key: &str| v[key].as_u64().unwrap_or(0);
        for c in clients.iter() {
            if let Ok(s) = c.control_status() {
                let d = &s["delegation"];
                // The session counters too: a reader whose watermark
                // names a position it never reaches shows up as reads
                // timing out on the session budget (its fsck crawls).
                let se = &s["session"];
                eprintln!(
                    "    {label}: {} delegation: placed {} recalled {} | as delegate: executed {} streamed {} deps-waits {} | as root: appended {} deps-unsatisfied {} exec-parked {} | session: reads {} waited {} timeouts {} wait {} ms",
                    c.name,
                    n(d, "place_delegated"),
                    n(d, "place_recalled"),
                    n(d, "executed") + n(d, "fast_path_executed"),
                    n(d, "streamed_txs"),
                    n(d, "deps_waits"),
                    n(d, "appended_txs"),
                    n(d, "deps_unsatisfied_at_append"),
                    n(d, "exec_parked"),
                    n(se, "reads"),
                    n(se, "waited"),
                    n(se, "timeouts"),
                    n(se, "wait_ms_total"),
                );
            }
        }
    }
    if !problems.is_empty() {
        bail!("{}", problems.join("\n    "));
    }
    Ok(acked)
}

#[allow(clippy::too_many_arguments)]
fn verify(
    env: &S3Env,
    root: &Path,
    backend: &str,
    clients: &[Client],
    home: &Path,
    rounds: &[&RoundResult],
    fresh_node: bool,
    label: &str,
) -> Result<()> {
    for c in clients {
        eventually(
            &format!("{} drains", c.name),
            Duration::from_secs(180),
            || {
                journal_drained(c)?;
                let spec = c.control_status()?["speculation"].clone();
                anyhow::ensure!(
                    spec["outstanding"].as_u64() == Some(0)
                        && spec["pending_replay"].as_u64() == Some(0),
                    "speculation still outstanding: {spec}"
                );
                Ok(())
            },
        )?;
    }
    let mut fresh = None;
    if fresh_node {
        let mut f = Client::new(root, "fresh", &env.endpoint, backend)?.with_own_node_key();
        f.mount().context("bootstrapping a fresh node")?;
        fresh = Some(f);
    }
    let result = (|| -> Result<()> {
        let mut named: Vec<(&str, &Client)> =
            clients.iter().map(|c| (c.name.as_str(), c)).collect();
        if let Some(f) = &fresh {
            named.push(("fresh", f));
        }
        let mut problems = Vec::new();
        for round in rounds {
            let repo = &round.paths.repo;
            // Convergence of every file under the repo, `.git` included.
            let mut last = None;
            let converged = eventually(
                "every replica shows the same repository",
                Duration::from_secs(120),
                || {
                    let mut snaps = Vec::new();
                    for (name, c) in &named {
                        let path = c.mnt.join(repo);
                        let tree = bounded(
                            &format!("walking {name}'s {repo}"),
                            git_timeout(),
                            move || constellation_chaos::snapshot_tree(&path),
                        )?
                        .with_context(|| format!("walking {name}'s {repo}"))?;
                        snaps.push((name.to_string(), tree));
                    }
                    let verdict = constellation_chaos::check_convergence(&snaps);
                    last = verdict.as_ref().err().cloned();
                    verdict.map_err(|e| anyhow::anyhow!("{e}"))
                },
            );
            if let Err(e) = converged {
                problems.push(format!(
                    "{repo}: trees differ: {}",
                    last.map(|f| f.message).unwrap_or_else(|| format!("{e:#}"))
                ));
            }
            let mut heads = BTreeSet::new();
            for (name, c) in &named {
                match git_view(&c.mnt.join(repo), home, &round.acked) {
                    Ok(v) => {
                        eprintln!(
                            "    {label}: {name} {repo}: HEAD {} {} commits, fsck {}, {} acked commits missing, {} not ancestors of HEAD",
                            &v.head[..v.head.len().min(12)],
                            v.commits,
                            if v.fsck_ok { "clean" } else { "FAILED" },
                            v.missing_acked.len(),
                            v.unreachable_acked.len()
                        );
                        heads.insert(v.head.clone());
                        if !v.fsck_ok {
                            let head: String =
                                v.fsck.lines().take(12).collect::<Vec<_>>().join("\n      ");
                            problems.push(format!("{name} {repo}: git fsck:\n      {head}"));
                        }
                        if !v.missing_acked.is_empty() {
                            problems.push(format!(
                                "{name} {repo}: {} acknowledged commits missing, e.g. {:?}",
                                v.missing_acked.len(),
                                &v.missing_acked[..v.missing_acked.len().min(3)]
                            ));
                        }
                        if !v.unreachable_acked.is_empty() {
                            problems.push(format!(
                                "{name} {repo}: {} acknowledged commits are not ancestors of HEAD (a commit based on a stale ref), e.g. {:?}",
                                v.unreachable_acked.len(),
                                &v.unreachable_acked[..v.unreachable_acked.len().min(3)]
                            ));
                        }
                    }
                    Err(e) => problems.push(format!(
                        "{name} {repo}: {e:#}{}",
                        fuse_requests_of(c)
                            .map(|f| format!("; the node's FUSE watchdog: {f}"))
                            .unwrap_or_default()
                    )),
                }
            }
            if heads.len() > 1 {
                problems.push(format!("{repo}: HEAD differs across nodes: {heads:?}"));
            }
        }
        if !problems.is_empty() {
            bail!("{}", problems.join("\n    "));
        }
        let commits: usize = rounds.iter().map(|r| r.acked.len()).sum();
        eprintln!(
            "    {label}: {commits} acknowledged commits intact on all {} nodes{}",
            clients.len(),
            if fresh.is_some() {
                " and a fresh one"
            } else {
                ""
            }
        );
        Ok(())
    })();
    if let Some(mut f) = fresh {
        let _ = f.unmount();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A repository on a local disk, committed to as the workloads do;
    /// `None` when git is not installed.
    fn repo_with_commits(commits: u64) -> Option<(tempfile::TempDir, PathBuf, PathBuf)> {
        Command::new("git").arg("--version").output().ok()?;
        let dir = tempfile::tempdir().unwrap();
        let (mnt, home) = (dir.path().join("mnt"), dir.path().join("home"));
        let repo = mnt.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        git_ok(&repo, &home, &["init", "-q"]).unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        for i in 0..commits {
            campaign_edit(&repo, "t", i, &mut rng).unwrap();
            git_ok(&repo, &home, &["add", "-A"]).unwrap();
            git_ok(&repo, &home, &["commit", "-q", "-m", &format!("t-{i}")]).unwrap();
        }
        Some((dir, mnt, home))
    }

    fn has_pack(git_dir: &Path) -> bool {
        std::fs::read_dir(git_dir.join("objects/pack"))
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .any(|e| e.path().extension().is_some_and(|x| x == "idx"))
            })
            .unwrap_or(false)
    }

    /// Git 2.55's automatic maintenance (a detached `git maintenance run
    /// --auto` after each commit) packed the loose objects outside any
    /// turn lock, and the causal reader then reported every packed object
    /// as missing. `GIT_CONFIG` keeps git from starting it, and the
    /// reader counts a packed object as visible.
    #[test]
    fn objects_stay_loose_and_packed_ones_count_as_visible() {
        let Some((_dir, mnt, home)) = repo_with_commits(3) else {
            eprintln!("git is not installed; skipped");
            return;
        };
        let repo = mnt.join("repo");
        let git_dir = repo.join(".git");
        let commit = |config: &[&str]| {
            let out = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(config)
                .args(["-c", "user.name=t", "-c", "user.email=t@t"])
                .args(["commit", "-q", "--allow-empty", "-m", "traced"])
                .env("HOME", &home)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_TRACE", "1")
                .output()
                .unwrap();
            assert!(out.status.success());
            String::from_utf8_lossy(&out.stderr).contains("maintenance run")
        };
        assert!(
            !commit(&GIT_CONFIG),
            "a commit started automatic maintenance"
        );
        // What the pin is for: unpinned, a commit starts it (git 2.29+).
        assert!(commit(&[]), "an unpinned commit did not start maintenance");
        std::thread::sleep(Duration::from_millis(200));
        std::fs::write(mnt.join(ALIVE), b"").unwrap();
        let head = git_ok(&repo, &home, &["rev-parse", "HEAD"]).unwrap();
        assert!(matches!(
            check_publication(&mnt, &repo, &git_dir, &home, &head, "test"),
            Check::Clean
        ));

        // Packed and pruned: no loose copy left, yet every object is
        // visible to git, and so to the check.
        git_ok(&repo, &home, &["gc", "-q", "--prune=now"]).unwrap();
        assert!(has_pack(&git_dir));
        let objects = closure_of(&repo, &home, &head).unwrap();
        assert!(objects.len() > 10);
        assert!(objects
            .iter()
            .all(|o| !object_visible(&git_dir, o).unwrap()));
        assert!(missing_from_packs(&repo, &git_dir, &home, &objects)
            .unwrap()
            .is_empty());
        let absent = "0123456789abcdef0123456789abcdef01234567".to_string();
        assert_eq!(
            missing_from_packs(&repo, &git_dir, &home, std::slice::from_ref(&absent)).unwrap(),
            vec![absent]
        );
        assert!(matches!(
            check_publication(&mnt, &repo, &git_dir, &home, &head, "test"),
            Check::Clean
        ));
    }

    /// A look at a dead mount (the reader killed mid-check: its empty
    /// mountpoint shows nothing) is not a causal violation; the commit is
    /// checked again on the next mount.
    #[test]
    fn a_dead_reader_mount_interrupts_the_check() {
        let Some((_dir, mnt, home)) = repo_with_commits(1) else {
            eprintln!("git is not installed; skipped");
            return;
        };
        let repo = mnt.join("repo");
        let git_dir = repo.join(".git");
        let head = git_ok(&repo, &home, &["rev-parse", "HEAD"]).unwrap();
        // The objects vanish with the mount, and so does `ALIVE`.
        std::fs::rename(&repo, mnt.join("elsewhere")).unwrap();
        assert!(matches!(
            check_publication(&mnt, &repo, &git_dir, &home, &head, "test"),
            Check::Interrupted
        ));
        // On a live mount the same absence is a violation.
        std::fs::write(mnt.join(ALIVE), b"").unwrap();
        std::fs::create_dir_all(git_dir.join("objects")).unwrap();
        let t = Instant::now();
        assert!(matches!(
            check_publication(&mnt, &repo, &git_dir, &home, &head, "test"),
            Check::Violation(_)
        ));
        assert!(t.elapsed() >= Duration::from_secs(15));
    }
}
