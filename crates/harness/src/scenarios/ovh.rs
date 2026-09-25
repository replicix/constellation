//! Scenarios from the OVH real-S3 run (four EC2 nodes in us-west-2, an
//! S3 bucket in Milan):
//!
//! - `concurrent-create-no-excl` (Finding 1): nodes racing
//!   `open(O_CREAT)` *without* `O_EXCL` on one not-yet-existing name all
//!   open the one file (the losers used to get `EEXIST`, which SQLite
//!   turns into "attempt to write a readonly database"); with `O_EXCL`
//!   exactly one wins. On the holder, forwarding nodes, a delegated
//!   subtree and the S3 inbox path.
//! - `nonowner-op-latency` (Findings 4 and 6): under injected S3
//!   latency, a non-owning node's `open(O_CREAT|O_EXCL)+write+close` into
//!   a shared directory, and the per-entry syscalls `tar` issues
//!   (mkdir, symlink, link, chmod, chown, utimensat), cost no S3 round
//!   trip — they are forwarded to the sequencer over P2P.

use super::m11::{delegate, s3_breakdown, wait_installed};
use super::m8::dist;
use super::m9::{c_deny_path, cluster, node_id};
use super::{eventually, lease_of};
use crate::client::Client;
use anyhow::{Context, Result};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// `open(path, O_RDWR|O_CREAT[|O_EXCL][|O_TRUNC], 0644)`, then (when
/// `mark` is given) `pwrite` one byte at its offset through the new
/// descriptor, `fstat` and `close`. The inode number, or the errno of the
/// first call that failed.
fn open_create(
    path: &Path,
    excl: bool,
    trunc: bool,
    mark: Option<(u8, u64)>,
) -> std::result::Result<u64, i32> {
    let c = CString::new(path.as_os_str().as_bytes()).expect("path");
    let mut flags = libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC;
    if excl {
        flags |= libc::O_EXCL;
    }
    if trunc {
        flags |= libc::O_TRUNC;
    }
    // SAFETY: plain syscalls on a descriptor this function owns.
    unsafe {
        let fd = libc::open(c.as_ptr(), flags, 0o644 as libc::c_uint);
        if fd < 0 {
            return Err(errno());
        }
        if let Some((byte, off)) = mark {
            let buf = [byte];
            if libc::pwrite(fd, buf.as_ptr().cast(), 1, off as libc::off_t) != 1 {
                let e = errno();
                libc::close(fd);
                return Err(e);
            }
        }
        let mut st: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut st) != 0 {
            let e = errno();
            libc::close(fd);
            return Err(e);
        }
        if libc::close(fd) != 0 {
            return Err(errno());
        }
        Ok(st.st_ino)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Race {
    /// `O_CREAT` alone; every racer writes its mark byte.
    Plain,
    /// `O_CREAT|O_TRUNC`, no writes.
    Trunc,
    /// `O_CREAT|O_EXCL`: exactly one winner, the rest `EEXIST`.
    Excl,
}

/// `rounds` rounds of every mount in `racers` opening `dir/<tag><i>` at
/// once (a barrier releases them together). Returns the names raced and,
/// per name, the inode the winners opened.
fn race(
    scenario: &str,
    racers: &[&Client],
    dir: &str,
    tag: &str,
    rounds: usize,
    kind: Race,
) -> Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    let mut losers_opened = 0usize;
    let started = Instant::now();
    for i in 0..rounds {
        let name = format!("{dir}/{tag}{i}");
        let barrier = Arc::new(Barrier::new(racers.len()));
        let handles: Vec<_> = racers
            .iter()
            .enumerate()
            .map(|(k, c)| {
                let path = c.mnt.join(&name);
                let barrier = barrier.clone();
                let who = c.name.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let mark = (kind == Race::Plain).then_some((b'a' + k as u8, k as u64));
                    (
                        who,
                        open_create(&path, kind == Race::Excl, kind == Race::Trunc, mark),
                    )
                })
            })
            .collect();
        let results: Vec<(String, std::result::Result<u64, i32>)> = handles
            .into_iter()
            .map(|h| h.join().expect("racer"))
            .collect();
        match kind {
            Race::Plain | Race::Trunc => {
                let failed: Vec<_> = results.iter().filter(|(_, r)| r.is_err()).collect();
                anyhow::ensure!(
                    failed.is_empty(),
                    "{scenario}: {name} ({kind:?}): open(O_CREAT) without O_EXCL failed on {:?} \
                     (errno {:?}); results {results:?}",
                    failed.iter().map(|(w, _)| w).collect::<Vec<_>>(),
                    failed.iter().map(|(_, r)| r.err()).collect::<Vec<_>>()
                );
                let inos: std::collections::BTreeSet<u64> =
                    results.iter().filter_map(|(_, r)| r.ok()).collect();
                anyhow::ensure!(
                    inos.len() == 1,
                    "{scenario}: {name} ({kind:?}): the racers opened different inodes: {results:?}"
                );
                losers_opened += racers.len() - 1;
                out.push((name, *inos.iter().next().expect("one")));
            }
            Race::Excl => {
                let won: Vec<_> = results.iter().filter(|(_, r)| r.is_ok()).collect();
                let other: Vec<_> = results
                    .iter()
                    .filter(|(_, r)| matches!(r, Err(e) if *e != libc::EEXIST))
                    .collect();
                anyhow::ensure!(
                    won.len() == 1 && other.is_empty(),
                    "{scenario}: {name} (O_EXCL): want exactly one winner and EEXIST for the rest: \
                     {results:?}"
                );
                out.push((name, won[0].1.expect("won")));
            }
        }
    }
    eprintln!(
        "    {scenario}: {tag}* in {dir} ({kind:?}): {rounds} rounds x {} racers in {:?}{}",
        racers.len(),
        started.elapsed(),
        if kind == Race::Excl {
            String::new()
        } else {
            format!("; every racer opened the one file ({losers_opened} of the opens did not create it)")
        }
    );
    Ok(out)
}

/// Every name resolves to the inode its race opened on every mount, and
/// a `Plain` race's file holds every racer's mark byte.
fn converged(
    clients: &[&Client],
    names: &[(String, u64)],
    marks: Option<&[u8]>,
    deadline: Duration,
) -> Result<()> {
    for c in clients {
        eventually(
            &format!("{} agrees on every raced inode", c.name),
            deadline,
            || {
                for (name, ino) in names {
                    let md = std::fs::metadata(c.mnt.join(name))
                        .with_context(|| format!("{}: stat {name}", c.name))?;
                    anyhow::ensure!(
                        md.ino() == *ino && md.is_file(),
                        "{}: {name} is inode {} (file: {}), the race opened {ino}",
                        c.name,
                        md.ino(),
                        md.is_file()
                    );
                    if let Some(marks) = marks {
                        let got = std::fs::read(c.mnt.join(name))?;
                        anyhow::ensure!(
                            got == marks,
                            "{}: {name} reads {:?}, want every racer's mark {:?}",
                            c.name,
                            String::from_utf8_lossy(&got),
                            String::from_utf8_lossy(marks)
                        );
                    }
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn inbox_ops(c: &Client) -> Result<u64> {
    Ok(c.control_status()?["inbox"]["submitted_ops"]
        .as_u64()
        .unwrap_or(0))
}

fn sqlite_available() -> bool {
    std::process::Command::new("sqlite3")
        .arg("-version")
        .output()
        .is_ok_and(|o| o.status.success())
}

/// Two nodes open a brand-new SQLite database at once and create a table
/// in it (`CREATE TABLE IF NOT EXISTS`, then an insert): neither may fail
/// (the loser of the create race used to see "attempt to write a
/// readonly database").
fn sqlite_first_touch(scenario: &str, a: &Client, b: &Client, rounds: usize) -> Result<()> {
    for i in 0..rounds {
        let name = format!("race/first-touch-{i}.db");
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [a, b]
            .iter()
            .map(|c| {
                let db = c.mnt.join(&name);
                let who = c.name.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || -> Result<()> {
                    barrier.wait();
                    let out = std::process::Command::new("sqlite3")
                        .arg("-cmd")
                        .arg(".timeout 60000")
                        .arg(&db)
                        .arg(format!(
                            "CREATE TABLE IF NOT EXISTS t(node TEXT); INSERT INTO t VALUES('{who}');"
                        ))
                        .output()
                        .context("running sqlite3")?;
                    anyhow::ensure!(
                        out.status.success(),
                        "{who}: sqlite3 on a new database: {}{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    );
                    Ok(())
                })
            })
            .collect();
        for h in handles {
            h.join().expect("sqlite racer")?;
        }
    }
    eprintln!(
        "    {scenario}: sqlite first touch from two nodes at once: {rounds} rounds, no failure"
    );
    Ok(())
}

pub fn concurrent_create_no_excl(_seed: u64) -> Result<()> {
    const NAME: &str = "concurrent-create-no-excl";
    let rounds = knob("CREATE_RACE_ROUNDS", 20) as usize;
    let (_env, root, mut clients, _) = cluster(NAME, &["a", "b", "c", "d"], &[], 0)?;
    let result = (|| -> Result<()> {
        let all: Vec<&Client> = clients.iter().collect();
        let a = &clients[0];
        let b = &clients[1];
        let d = &clients[3];
        let ids: Vec<u64> = clients.iter().map(node_id).collect::<Result<_>>()?;
        std::fs::create_dir(a.mnt.join("race"))?;
        std::fs::create_dir(a.mnt.join("deleg"))?;
        for x in &all {
            eventually("dirs visible", Duration::from_secs(30), || {
                anyhow::ensure!(x.mnt.join("race").is_dir() && x.mnt.join("deleg").is_dir());
                Ok(())
            })?;
        }
        anyhow::ensure!(lease_of(a)?["held"] == true, "a does not hold the lease");

        // The sequencer (a) and three forwarding nodes.
        let plain = race(NAME, &all, "race", "p", rounds, Race::Plain)?;
        let trunc = race(NAME, &all, "race", "t", rounds, Race::Trunc)?;
        let excl = race(NAME, &all, "race", "x", rounds, Race::Excl)?;
        converged(&all, &plain, Some(b"abcd"), Duration::from_secs(60))?;
        converged(&all, &trunc, None, Duration::from_secs(60))?;
        converged(&all, &excl, None, Duration::from_secs(60))?;
        if sqlite_available() {
            sqlite_first_touch(NAME, b, clients.get(2).expect("c"), rounds.min(10))?;
        } else {
            eprintln!("    {NAME}: no sqlite3 on the host; the first-touch check is skipped");
        }

        // A delegated subtree: b executes, a and the others forward to it.
        delegate(a, "/deleg", ids[1])?;
        wait_installed(b, "/deleg", Duration::from_secs(30))?;
        let dplain = race(NAME, &all, "deleg", "p", rounds, Race::Plain)?;
        let dexcl = race(NAME, &all, "deleg", "x", rounds, Race::Excl)?;
        converged(&all, &dplain, Some(b"abcd"), Duration::from_secs(60))?;
        converged(&all, &dexcl, None, Duration::from_secs(60))?;

        // The S3 inbox: d has no P2P path to anyone, a (the sequencer)
        // and d race.
        let others: Vec<String> = ids
            .iter()
            .filter(|id| **id != ids[3])
            .map(|id| id.to_string())
            .collect();
        std::fs::write(c_deny_path(root.path(), &d.name), others.join("\n") + "\n")?;
        for x in &clients[..3] {
            std::fs::write(c_deny_path(root.path(), &x.name), format!("{}\n", ids[3]))?;
        }
        let before = inbox_ops(d)?;
        let pair = [a, d];
        let iplain = race(NAME, &pair, "race", "ip", rounds.min(8), Race::Plain)?;
        let iexcl = race(NAME, &pair, "race", "ix", rounds.min(8), Race::Excl)?;
        let after = inbox_ops(d)?;
        eprintln!(
            "    {NAME}: d submitted {} ops through the inbox",
            after - before
        );
        anyhow::ensure!(
            after > before,
            "d's creates did not go through the inbox (submitted_ops {before} -> {after})"
        );
        for x in &clients[..3] {
            let _ = std::fs::remove_file(c_deny_path(root.path(), &x.name));
        }
        let _ = std::fs::remove_file(c_deny_path(root.path(), &d.name));
        converged(&pair, &iplain, Some(b"ab"), Duration::from_secs(90))?;
        converged(&all, &iexcl, None, Duration::from_secs(90))?;
        Ok(())
    })();
    if result.is_err() {
        keep_logs(NAME, &clients);
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

fn keep_logs(scenario: &str, clients: &[Client]) {
    let dir = std::env::temp_dir().join(format!("harness-{scenario}-logs-{}", super::ts()));
    if std::fs::create_dir_all(&dir).is_ok() {
        for c in clients {
            if let Some(log) = c.mnt.parent().map(|p| p.join("mount.log")) {
                let _ = std::fs::copy(log, dir.join(format!("{}.log", c.name)));
            }
        }
        eprintln!("    {scenario}: mount logs kept in {}", dir.display());
    }
}

/// One timed operation kind of [`nonowner_op_latency`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    /// `open(O_CREAT|O_EXCL|O_WRONLY)` + 4 KiB `write` + `close` (the
    /// bench's `small_write`; the payload repeats per node, as there).
    SmallWrite,
    /// The same with a payload unique to each file (its chunk is new).
    UniqueWrite,
    Mkdir,
    Symlink,
    Link,
    Chmod,
    Chown,
    Utimes,
    /// What `tar` does per regular file: create+write+close, then
    /// `utimensat` and `chmod` of the file it just closed (the payload
    /// repeats per node, so no chunk upload is in it).
    TarFile,
}

const OPS: [Op; 9] = [
    Op::SmallWrite,
    Op::TarFile,
    Op::UniqueWrite,
    Op::Mkdir,
    Op::Symlink,
    Op::Link,
    Op::Chmod,
    Op::Chown,
    Op::Utimes,
];

fn run_op(op: Op, dir: &Path, tag: &str, i: usize, payload: &[u8]) -> Result<Duration> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let base = dir.join(format!("{tag}-{i}"));
    // The file every attribute op works on, made (untimed) first.
    let target = dir.join(format!("{tag}-t{i}"));
    if matches!(op, Op::Link | Op::Chmod | Op::Chown | Op::Utimes) {
        std::fs::write(&target, b"t")?;
    }
    let t = Instant::now();
    match op {
        Op::SmallWrite | Op::UniqueWrite => {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o644)
                .open(&base)?;
            if op == Op::SmallWrite {
                f.write_all(payload)?;
            } else {
                let mut unique = payload.to_vec();
                unique[..tag.len()].copy_from_slice(tag.as_bytes());
                unique[tag.len()..tag.len() + 8].copy_from_slice(&(i as u64).to_le_bytes());
                f.write_all(&unique)?;
            }
            drop(f);
        }
        Op::TarFile => {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&base)?;
            f.write_all(payload)?;
            drop(f);
            let f = std::fs::File::options().write(true).open(&base)?;
            let then = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
            f.set_times(
                std::fs::FileTimes::new()
                    .set_accessed(then)
                    .set_modified(then),
            )?;
            drop(f);
            std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o644))?;
        }
        Op::Mkdir => std::fs::create_dir(&base)?,
        Op::Symlink => std::os::unix::fs::symlink("some/target", &base)?,
        Op::Link => std::fs::hard_link(&target, &base)?,
        Op::Chmod => std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?,
        Op::Chown => {
            // To the owner it has (what tar does as a non-root user).
            // SAFETY: getuid/getgid never fail.
            let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
            std::os::unix::fs::chown(&target, Some(uid), Some(gid))?
        }
        Op::Utimes => {
            let f = std::fs::File::options().write(true).open(&target)?;
            let then = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
            f.set_times(
                std::fs::FileTimes::new()
                    .set_accessed(then)
                    .set_modified(then),
            )?;
        }
    }
    Ok(t.elapsed())
}

fn p50(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v.get(v.len() / 2).copied().unwrap_or_default()
}

/// Findings 4 and 6 of the OVH run: a non-owning node's small-file
/// create+write+close into a shared directory cost one S3 round trip
/// (p50 53 ms on AWS, 402 ms on OVH, 2-3 ms on the sequencer), and an
/// untar ran at 0.63 s per file. Under injected S3 latency (every S3
/// request costs at least `2 x OWNER_LAT_MS`), each node in turn runs
/// every per-entry operation `tar` issues; the non-owners' median must
/// stay well under one S3 round trip — they go to the sequencer over P2P
/// — except where a documented rule waits for S3 (the write-through
/// close of a file whose chunk S3 does not have yet: the sequencer's
/// own waits the same).
pub fn nonowner_op_latency(_seed: u64) -> Result<()> {
    const NAME: &str = "nonowner-op-latency";
    let lat = knob("NONOWNER_LAT_MS", 100);
    let count = knob("NONOWNER_OPS", 12) as usize;
    // The root stays the directory's only sequencer: with placement on
    // (the default) the first node to dominate `shared` would be handed
    // it, and every other node's ops would go to that delegate instead —
    // a different path (see PROGRESS.md: a delegate's replies behind its
    // own unshipped rows still wait for the log).
    let (env, _root, mut clients, proxies) = cluster(
        NAME,
        &["a", "b", "c", "d"],
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        4,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        std::fs::create_dir(a.mnt.join("shared"))?;
        for x in &clients {
            eventually("shared visible", Duration::from_secs(30), || {
                anyhow::ensure!(x.mnt.join("shared").is_dir());
                Ok(())
            })?;
        }
        // Warm-up: forwarding paths up (P2P connections made).
        for x in &clients {
            for i in 0..3 {
                std::fs::write(x.mnt.join(format!("shared/warm-{}-{i}", x.name)), b"w")?;
            }
        }
        // The steady state plan 30 §M9 describes: the sequencer has a
        // backup on the LAN, so it streams its journal to the other
        // nodes ahead of S3. (Until one is committed — a few seconds
        // after the mount — an acknowledgement waits for the log, by
        // design: `durable_jseq`.)
        let backups = super::m9::wait_for_backup(a, Duration::from_secs(60))?;
        eprintln!("    {NAME}: a's backups {backups:?}");
        std::thread::sleep(Duration::from_secs(1));
        env.existing_s3_proxy().latency(lat, 0)?;
        let rtt = Duration::from_millis(2 * lat);
        eprintln!("    {NAME}: S3 latency injected: every request >= {rtt:?}");
        let mut failures = Vec::new();
        let mut table = Vec::new();
        // Debugging knobs: `NONOWNER_ONLY=Chmod,Link` and
        // `NONOWNER_NODES=c` narrow the run.
        let only = std::env::var("NONOWNER_ONLY").unwrap_or_default();
        let nodes = std::env::var("NONOWNER_NODES").unwrap_or_default();
        for (k, x) in clients.iter().enumerate() {
            if !nodes.is_empty() && !nodes.split(',').any(|n| n == x.name) {
                continue;
            }
            let owner = lease_of(x)?["held"] == true;
            let payload = vec![b'a' + k as u8; 4096];
            let dir = x.mnt.join("shared");
            for op in OPS {
                if !only.is_empty() && !only.split(',').any(|o| o == format!("{op:?}")) {
                    continue;
                }
                for p in &proxies {
                    p.reset();
                }
                let mut lats = Vec::new();
                for i in 0..count {
                    lats.push(
                        run_op(op, &dir, &format!("{}-{op:?}", x.name), i, &payload)
                            .with_context(|| format!("{}: {op:?} {i}", x.name))?,
                    );
                }
                let median = p50(lats.clone());
                let breakdown = proxies
                    .iter()
                    .zip(&clients)
                    .map(|(p, c)| format!("[{}] {}", c.name, s3_breakdown(p)))
                    .collect::<Vec<_>>()
                    .join(" ");
                table.push(format!(
                    "{:>2} {:<5} {op:<11?} p50 {median:>12?}  {}  S3: {breakdown}",
                    x.name,
                    if owner { "owner" } else { "fwd" },
                    dist(lats)
                ));
                // The write-through close of a new chunk waits for its
                // upload on every node (`writeback.mode = through`).
                let s3_bound = op == Op::UniqueWrite;
                if !s3_bound && median >= rtt / 2 {
                    failures.push(format!(
                        "{} ({}) {op:?}: p50 {median:?} >= half an S3 round trip ({:?})",
                        x.name,
                        if owner { "owner" } else { "non-owner" },
                        rtt / 2
                    ));
                }
            }
        }
        for row in &table {
            eprintln!("    {NAME}: {row}");
        }
        for x in &clients {
            let s = x.control_status()?;
            eprintln!(
                "    {NAME}: {}: inbox ops {}, lease held {}, write mode {}, forwards that \
                 waited for their transaction {} (answered from the pre-S3 stream {}); streamed \
                 transactions installed {} dropped {}",
                x.name,
                s["inbox"]["submitted_ops"],
                s["lease"]["held"],
                s["writeback"]["mode"],
                s["ack"]["awaited_log"],
                s["ack"]["awaited_log_streamed"],
                s["ack"]["streamed_installed"],
                s["ack"]["streamed_dropped"]
            );
        }
        env.existing_s3_proxy().remove_all_toxics()?;
        anyhow::ensure!(failures.is_empty(), "{NAME}: {failures:#?}");
        Ok(())
    })();
    if result.is_err() {
        keep_logs(NAME, &clients);
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}
