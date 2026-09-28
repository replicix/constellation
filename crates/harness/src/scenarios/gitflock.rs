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
//!   The ref must never regress either. Every `GIT_FLOCK_FSCK_EVERY_S`
//!   (20) the reader takes the turn lock, checks that the ref is the last
//!   acknowledged commit, and runs `git fsck --full`. The lock matters:
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

/// `git -C repo args...` with a clean, fixed configuration.
fn git(repo: &Path, home: &Path, args: &[&str]) -> Result<std::process::Output> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["-c", "user.name=gitflock", "-c", "user.email=gitflock@test"])
        .args(args)
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .with_context(|| format!("running git {args:?}"))?;
    Ok(out)
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
    got: Instant,
    released: Instant,
    /// How long each step of the turn took.
    steps: Vec<(&'static str, Duration)>,
}

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
            flock(&lf, libc::LOCK_EX).context("flock")?;
            let got = Instant::now();
            let steps = std::cell::RefCell::new(Vec::new());
            let mark = std::cell::Cell::new(Instant::now());
            let step = |name: &'static str| {
                let now = Instant::now();
                steps.borrow_mut().push((name, now - mark.get()));
                mark.set(now);
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
                git_ok(&repo, &home, &["add", "-A"])?;
                step("add");
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
                let head = git_ok(&repo, &home, &["rev-parse", "HEAD"])?;
                std::fs::write(&marker, format!("{head}\n")).context("writing the marker")?;
                step("marker");
                *shared.last.lock().unwrap() = Some(head.clone());
                log.lock().unwrap().acked.push(head);
                if variant == Variant::Gc && i.is_multiple_of(8) {
                    git_ok(&repo, &home, &["gc", "-q"])?;
                    log.lock().unwrap().gcs += 1;
                }
                Ok(())
            })();
            shared.turns.lock().unwrap().push(Turn {
                who: name.clone(),
                i,
                got,
                released: Instant::now(),
                steps: steps.take(),
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
        .args(["-c", "user.name=gitflock", "-c", "user.email=gitflock@test"])
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

/// The commit `oid`, named by `via` (a reflog line, the ref), must have
/// every object it depends on visible. What is missing is polled until
/// it appears (how long that took is part of the finding) or 15 s pass.
fn check_publication(
    repo: &Path,
    git_dir: &Path,
    home: &Path,
    oid: &str,
    via: &str,
) -> Option<String> {
    let t = Instant::now();
    let mut first: Option<String> = None;
    loop {
        let problem = match object_visible(git_dir, oid) {
            Ok(false) => Some(format!("the commit object {oid} is missing")),
            Err(e) => Some(format!("the commit object {oid}: {e}")),
            Ok(true) => match closure_of(repo, home, oid) {
                Err(e) => Some(format!("{e:#}")),
                Ok(objects) => {
                    let mut missing = Vec::new();
                    let mut errors = Vec::new();
                    for o in &objects {
                        match object_visible(git_dir, o) {
                            Ok(true) => {}
                            Ok(false) => missing.push(o.clone()),
                            Err(e) => errors.push(format!("{o}: {e}")),
                        }
                    }
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
        };
        match problem {
            None => {
                return first.map(|p| {
                    format!(
                        "{via} named {oid} with {p}; all visible after {:?}",
                        t.elapsed()
                    )
                });
            }
            Some(p) => {
                if first.is_none() {
                    first = Some(p.clone());
                }
                if t.elapsed() > Duration::from_secs(15) {
                    return Some(format!(
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
                for (k, line) in lines.iter().enumerate().skip(reflog_lines) {
                    let Some(oid) = line.split(' ').nth(1) else {
                        continue;
                    };
                    if is_oid(oid) && checked.insert(oid.to_string()) {
                        log.lock().unwrap().checked += 1;
                        if let Some(p) = check_publication(
                            &repo,
                            &git_dir,
                            &home,
                            oid,
                            &format!("reflog line {}", k + 1),
                        ) {
                            log.lock().unwrap().violations.push(format!("{name}: {p}"));
                        }
                    }
                }
                reflog_lines = lines.len();
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
                log.lock().unwrap().checked += 1;
                if let Some(p) = check_publication(&repo, &git_dir, &home, &r, "refs/heads/master")
                {
                    log.lock().unwrap().violations.push(format!("{name}: {p}"));
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

/// Two turns that held the lock at once (a broken lock), and the turn
/// durations by decile.
fn judge_turns(turns: &[Turn], base: Instant) -> (Vec<String>, String, Duration) {
    let mut t: Vec<Turn> = turns.to_vec();
    t.sort_by_key(|x| x.got);
    let mut overlaps = Vec::new();
    let mut open: Option<&Turn> = None;
    for x in &t {
        if let Some(prev) = open {
            if x.got < prev.released && x.who != prev.who {
                overlaps.push(format!(
                    "{}#{} held the lock {:?}..{:?} while {}#{} got it at {:?}",
                    prev.who,
                    prev.i,
                    prev.got - base,
                    prev.released - base,
                    x.who,
                    x.i,
                    x.got - base
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
    (overlaps, deciles, max)
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
    let started = Instant::now();
    let workers: Vec<_> = (0..2)
        .map(|k| {
            let c = &clients[committers[k]];
            let (name, mnt) = (c.name.clone(), c.mnt.clone());
            let (home, stop, log) = (home.to_path_buf(), stop.clone(), logs[k].clone());
            let (shared, paths) = (shared.clone(), paths.clone());
            let seed = seed.wrapping_mul(31).wrapping_add(k as u64);
            std::thread::spawn(move || {
                committer(name, mnt, home, stop, variant, paths, log, shared, seed)
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
        if variant == Variant::Faults {
            std::thread::sleep(Duration::from_millis(rng.random_range(2000..6000)));
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
    for w in reader_threads {
        let _ = w.join();
    }
    if let Some(e) = fault_err {
        return Err(e.context("injecting a fault"));
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
    let turns = shared.turns.lock().unwrap().clone();
    let (overlaps, deciles, max) = judge_turns(&turns, started);
    eprintln!(
        "    {label}: {} turns, median duration per decile (s): {deciles}; longest {max:?}",
        turns.len()
    );
    for o in overlaps.iter().take(10) {
        eprintln!("    {label}: overlapping turns: {o}");
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
                        let tree = constellation_chaos::snapshot_tree(&c.mnt.join(repo))
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
                    Err(e) => problems.push(format!("{name} {repo}: {e:#}")),
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
