//! EC2 campaign 4, findings B-1/B-2: a git repository shared by two
//! committers who take turns under an `flock` turn file. After the soak
//! quiesced, every node agreed on `HEAD` and on every other file, but
//! each was missing a different set of `.git/objects` files, and a fresh
//! node mounted from the bucket alone was missing them too.
//!
//! `git-under-flock` reproduces the workload: nodes 0 and 1 alternately
//! commit (several files per commit: a note appended plus a few fresh
//! files) under `flock` on `gitrepo.lock`, as the soak worker did. Git
//! writes every loose object as `objects/xx/tmp_obj_*` (`O_CREAT|O_EXCL`,
//! write, close), `link`s it to `objects/xx/<hash>` and unlinks the
//! temporary name; refs go through `*.lock` plus `rename`. Under the
//! lock each committer first checks that `refs/heads/master` reads as
//! the last acknowledged commit: a stale read there is B-1's lost update
//! (the next commit is based on an old `HEAD`, the other node's commits
//! dangle). Then every node drains, a fresh node mounts from the bucket,
//! and on every node (the fresh one included):
//!
//! - the tree under `gitrepo/` (every `.git` file: path, size, nlink,
//!   content hash) is identical;
//! - `git fsck --full` is clean and every acknowledged commit (the
//!   committers' `git rev-parse HEAD` after each successful commit) is
//!   present and an ancestor of `HEAD`: a commit made under the lock can
//!   neither lose its objects nor be overwritten by a later commit based
//!   on a stale ref.
//!
//! `git-under-flock-gc` also runs `git gc` (pack, prune loose objects)
//! under the lock every few commits. `git-under-flock-faults` injects the
//! soak's faults meanwhile: `kill -9` of a random node (the lease holder
//! and the committers included) and of the whole cluster, `SIGSTOP`, P2P
//! isolation of one node, and an S3 cut of one node.
//!
//! Knobs: `GIT_FLOCK_SECS` (workload duration), `GIT_FLOCK_NODES` (2-4),
//! `GIT_FLOCK_COMMITTERS=last` (the last two nodes commit, so neither is
//! the sequencer), `GIT_FLOCK_S3_LATENCY_MS`, `GIT_FLOCK_ENV=K=V,...`
//! (extra mount environment).

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

const REPO: &str = "gitrepo";
const TURN: &str = "gitrepo.lock";
/// A file each mount has while it is live: a worker whose mount is dead
/// (killed, lazily detached) would otherwise run git in the empty
/// mountpoint directory underneath.
const ALIVE: &str = "gitflock-alive";

#[derive(Clone, Copy, PartialEq)]
enum Variant {
    Plain,
    Gc,
    Faults,
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

#[derive(Default)]
struct WorkerLog {
    /// Every commit a committer's `git commit` acknowledged.
    acked: Vec<String>,
    errors: Vec<String>,
    /// Turns that began with a stale `refs/heads/master`.
    stale: Vec<String>,
    gcs: u64,
}

/// One committer: take the turn lock, append a note, add a few fresh
/// files, commit, (gc), release. Errors are recorded, not fatal: under
/// faults a mount may be dead or a lock grant fenced. A stale git lock
/// file left by a failed commit is removed under the turn lock (nobody
/// else can be inside git then).
fn committer(
    name: String,
    mnt: PathBuf,
    home: PathBuf,
    stop: Arc<AtomicBool>,
    variant: Variant,
    log: Arc<Mutex<WorkerLog>>,
    last: Arc<Mutex<Option<String>>>,
) {
    let repo = mnt.join(REPO);
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
                .open(mnt.join(TURN))
                .context("opening the turn file")?;
            flock(&lf, libc::LOCK_EX).context("flock")?;
            let inner = (|| -> Result<()> {
                // What the previous turn left: `refs/heads/master` must
                // read as the last acknowledged commit now that we hold
                // the turn lock.
                let expected = last.lock().unwrap().clone();
                if let Some(want) = &expected {
                    let read = || master_ref(&repo);
                    let seen = read();
                    if &seen != want {
                        let t = Instant::now();
                        let mut now = seen.clone();
                        while &now != want && t.elapsed() < Duration::from_secs(5) {
                            std::thread::sleep(Duration::from_millis(20));
                            now = read();
                        }
                        log.lock().unwrap().stale.push(format!(
                            "{name}#{i}: under the turn lock refs/heads/master read {seen}, the last acknowledged commit is {want}; {}",
                            if &now == want {
                                format!("caught up after {:?}", t.elapsed())
                            } else {
                                format!("still {now} after {:?}", t.elapsed())
                            }
                        ));
                    }
                }
                for stale in ["index.lock", "refs/heads/master.lock", "HEAD.lock"] {
                    let p = repo.join(".git").join(stale);
                    if p.exists() {
                        log.lock().unwrap().errors.push(format!(
                            "{name}#{i}: removed stale {stale} before committing"
                        ));
                        let _ = std::fs::remove_file(&p);
                    }
                }
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
                git_ok(&repo, &home, &["add", "-A"])?;
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
                let head = git_ok(&repo, &home, &["rev-parse", "HEAD"])?;
                *last.lock().unwrap() = Some(head.clone());
                log.lock().unwrap().acked.push(head);
                if variant == Variant::Gc && i.is_multiple_of(8) {
                    git_ok(&repo, &home, &["gc", "-q"])?;
                    log.lock().unwrap().gcs += 1;
                }
                Ok(())
            })();
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

fn git_view(mnt: &Path, home: &Path, acked: &[String]) -> Result<GitView> {
    let repo = mnt.join(REPO);
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

fn run(scenario: &str, seed: u64, variant: Variant) -> Result<()> {
    if Command::new("git").arg("--version").output().is_err() {
        bail!("git is not installed");
    }
    let secs = env_u64(
        "GIT_FLOCK_SECS",
        if variant == Variant::Faults { 150 } else { 60 },
    );
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
        let m0 = clients[0].mnt.clone();
        std::fs::write(m0.join(TURN), b"")?;
        std::fs::create_dir_all(m0.join(REPO))?;
        git_ok(&m0.join(REPO), &home, &["init", "-q"])?;
        for c in &clients[1..] {
            eventually(
                &format!("the repo reaches {}", c.name),
                Duration::from_secs(60),
                || {
                    anyhow::ensure!(c.mnt.join(REPO).join(".git/HEAD").exists());
                    anyhow::ensure!(c.mnt.join(TURN).exists());
                    Ok(())
                },
            )?;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let logs: Vec<Arc<Mutex<WorkerLog>>> = (0..2).map(|_| Arc::default()).collect();
        let last: Arc<Mutex<Option<String>>> = Arc::default();
        // The committers: nodes 0 and 1 (node 0 created the filesystem and
        // starts as the lease holder), or with `GIT_FLOCK_COMMITTERS=last`
        // the last two, so that neither committer is the sequencer.
        let committers: Vec<usize> =
            if std::env::var("GIT_FLOCK_COMMITTERS").as_deref() == Ok("last") {
                vec![clients.len() - 2, clients.len() - 1]
            } else {
                vec![0, 1]
            };
        let workers: Vec<_> = (0..2)
            .map(|k| {
                let c = &clients[committers[k]];
                let (name, mnt) = (c.name.clone(), c.mnt.clone());
                let (home, stop, log) = (home.clone(), stop.clone(), logs[k].clone());
                let last = last.clone();
                std::thread::spawn(move || committer(name, mnt, home, stop, variant, log, last))
            })
            .collect();
        let started = Instant::now();
        let mut rng = StdRng::seed_from_u64(seed);
        let mut faults = Vec::new();
        let mut fault_err = None;
        while started.elapsed() < Duration::from_secs(secs) {
            if variant == Variant::Faults {
                std::thread::sleep(Duration::from_millis(rng.random_range(2000..6000)));
                match fleet.fault(&mut rng, &mut clients, faults.len() as u64 + 1) {
                    Ok(f) => {
                        eprintln!("    {scenario}: {f} ({:?})", started.elapsed());
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
        for (k, l) in logs.iter().enumerate() {
            let l = l.lock().unwrap();
            eprintln!(
                "    {scenario}: {} made {} commits ({} gc), {} errors{}",
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
        }
        for st in stale.iter().take(10) {
            eprintln!("    {scenario}: stale turn: {st}");
        }
        anyhow::ensure!(acked.len() >= 4, "only {} commits were made", acked.len());
        let verified = verify(
            &env,
            root.path(),
            &backend,
            &clients,
            &home,
            &acked,
            scenario,
        );
        // Under faults a turn may begin before the previous turn's writes
        // are back: its holder, or the sequencer that acknowledged them,
        // crashed with them unshipped (Layer A: they return when their
        // requester replays them by rid). That is reported, not fatal; a
        // commit it would lose is caught by `verify` all the same.
        if !stale.is_empty() && variant == Variant::Faults {
            eprintln!(
                "    {scenario}: {} of {} turns began with a stale refs/heads/master (faults)",
                stale.len(),
                acked.len()
            );
        } else if !stale.is_empty() {
            let also = verified
                .err()
                .map(|e| format!("; and {e:#}"))
                .unwrap_or_default();
            bail!(
                "{} of {} turns began with a stale refs/heads/master under the turn lock, e.g. {}{also}",
                stale.len(),
                acked.len(),
                stale[0]
            );
        }
        verified
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
            }
            eprintln!("    {scenario}: mount logs kept in {}", dir.display());
        }
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

fn verify(
    env: &S3Env,
    root: &Path,
    backend: &str,
    clients: &[Client],
    home: &Path,
    acked: &[String],
    scenario: &str,
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
    let mut fresh = Client::new(root, "fresh", &env.endpoint, backend)?.with_own_node_key();
    fresh.mount().context("bootstrapping a fresh node")?;
    let result = (|| -> Result<()> {
        let mut named: Vec<(&str, &Client)> =
            clients.iter().map(|c| (c.name.as_str(), c)).collect();
        named.push(("fresh", &fresh));
        // Convergence of every file under the repo, `.git` included.
        let mut last = None;
        let converged = eventually(
            "every replica, a fresh one included, shows the same repository",
            Duration::from_secs(120),
            || {
                let mut snaps = Vec::new();
                for (name, c) in &named {
                    let tree = constellation_chaos::snapshot_tree(&c.mnt.join(REPO))
                        .with_context(|| format!("walking {name}'s repository"))?;
                    snaps.push((name.to_string(), tree));
                }
                let verdict = constellation_chaos::check_convergence(&snaps);
                last = verdict.as_ref().err().cloned();
                verdict.map_err(|e| anyhow::anyhow!("{e}"))
            },
        );
        let mut problems = Vec::new();
        if let Err(e) = converged {
            problems.push(format!(
                "repository trees differ: {}",
                last.map(|f| f.message).unwrap_or_else(|| format!("{e:#}"))
            ));
        }
        let mut heads = BTreeSet::new();
        for (name, c) in &named {
            match git_view(&c.mnt, home, acked) {
                Ok(v) => {
                    eprintln!(
                        "    {scenario}: {name}: HEAD {} {} commits, fsck {}, {} acked commits missing, {} not ancestors of HEAD",
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
                        problems.push(format!("{name}: git fsck:\n      {head}"));
                    }
                    if !v.missing_acked.is_empty() {
                        problems.push(format!(
                            "{name}: {} acknowledged commits missing, e.g. {:?}",
                            v.missing_acked.len(),
                            &v.missing_acked[..v.missing_acked.len().min(3)]
                        ));
                    }
                    if !v.unreachable_acked.is_empty() {
                        problems.push(format!(
                            "{name}: {} acknowledged commits are not ancestors of HEAD (a commit based on a stale ref), e.g. {:?}",
                            v.unreachable_acked.len(),
                            &v.unreachable_acked[..v.unreachable_acked.len().min(3)]
                        ));
                    }
                }
                Err(e) => problems.push(format!("{name}: {e:#}")),
            }
        }
        if heads.len() > 1 {
            problems.push(format!("HEAD differs across nodes: {heads:?}"));
        }
        if !problems.is_empty() {
            bail!("{}", problems.join("\n    "));
        }
        eprintln!(
            "    {scenario}: {} acknowledged commits intact on all {} nodes and a fresh one",
            acked.len(),
            clients.len()
        );
        Ok(())
    })();
    let _ = fresh.unmount();
    result
}
