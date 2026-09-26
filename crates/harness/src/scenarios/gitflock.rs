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
//!   workload, then on every later one").
//!
//! Knobs: `GIT_FLOCK_SECS` (workload duration, per round), `GIT_FLOCK_NODES`
//! (2-4), `GIT_FLOCK_COMMITTERS=last` (the last two nodes commit, so
//! neither is the sequencer), `GIT_FLOCK_S3_LATENCY_MS`,
//! `GIT_FLOCK_ENV=K=V,...` (extra mount environment), `GIT_FLOCK_RUST_LOG`
//! (the daemons' `RUST_LOG`), `GIT_FLOCK_ROUNDS`,
//! `GIT_FLOCK_MAX_TURN_S` (b2b/rounds: a turn longer than this fails the
//! run; default 30).

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
}

impl Variant {
    /// Campaign 5's commit shape (5–20 files, appends to tracked files).
    fn campaign_shape(self) -> bool {
        matches!(self, Variant::B2b | Variant::Rounds)
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
            Variant::B2b => 180,
            Variant::Rounds => 90,
            _ => 60,
        },
    );
    let rounds = if variant == Variant::Rounds {
        env_u64("GIT_FLOCK_ROUNDS", 3).max(1) as usize
    } else {
        1
    };
    let n_nodes = env_u64("GIT_FLOCK_NODES", 4).clamp(2, 4) as usize;
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
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    stop.store(true, Ordering::SeqCst);
    for w in workers {
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
    if !overlaps.is_empty() {
        problems.push(format!(
            "{} turns held the turn lock at the same time as another (a broken lock), e.g. {}",
            overlaps.len(),
            overlaps[0]
        ));
    }
    if !stale.is_empty() {
        problems.push(format!(
            "{} of {} turns began with a stale refs/heads/master or marker under the turn lock, e.g. {}",
            stale.len(),
            turns.len(),
            stale[0]
        ));
    }
    if variant.campaign_shape() {
        let limit = Duration::from_secs(env_u64("GIT_FLOCK_MAX_TURN_S", 30));
        if max > limit {
            problems.push(format!(
                "the longest turn took {max:?} (limit {limit:?}; median per decile {deciles})"
            ));
        }
        if errors > 0 {
            problems.push(format!("{errors} turns failed (see above)"));
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
