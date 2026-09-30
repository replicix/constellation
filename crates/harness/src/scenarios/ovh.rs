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
//! - `delegated-op-latency`: the same with the shared directory delegated
//!   to one node (what placement does for a dominant writer): the other
//!   nodes' ops go to that delegate, and a reply it evaluated behind its
//!   own unappended rows is answered from the root's pre-S3 stream of
//!   its append, not from the log.

use super::m11::{delegate, s3_breakdown, wait_installed};
use super::m8::dist;
use super::m9::{c_deny_path, cluster, node_id};
use super::{eventually, lease_of};
use crate::client::Client;
use anyhow::{Context, Result};
use constellation_types::Code;
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
                    .filter(|(_, r)| {
                        matches!(r, Err(e) if Code::try_from_native(*e) != Some(Code::Exists))
                    })
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

pub(super) fn keep_logs(scenario: &str, clients: &[Client]) {
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
    op_latency("nonowner-op-latency", false)
}

/// [`nonowner_op_latency`] with `shared` delegated to `b` (a manual
/// delegation; placement off, so nothing recalls it): `c`, `d` and the
/// root `a` forward to `b`. A non-owner's close after its create (and a
/// `chmod`/`utimensat` after its close) finds the delegate's own
/// transaction for the file still unappended, so the reply names no base
/// and the requester waits for that transaction: before, until the
/// root's segment came back from S3 (one S3 round trip per such op, the
/// cost placement put on every other writer of a directory it had given
/// to a dominant one); now until the root's pre-S3 stream installs the
/// root's append of it (the root has a backup on the LAN). Also checks
/// that the delegation stayed `b`'s, and that the other nodes' waits
/// were answered from the stream (`awaited_log_streamed_deleg`).
pub fn delegated_op_latency(_seed: u64) -> Result<()> {
    op_latency("delegated-op-latency", true)
}

fn op_latency(name: &'static str, delegated: bool) -> Result<()> {
    let lat = knob("NONOWNER_LAT_MS", 100);
    let count = knob("NONOWNER_OPS", 12) as usize;
    // The root stays the directory's only sequencer: with placement on
    // (the default) the first node to dominate `shared` would be handed
    // it, and every other node's ops would go to that delegate instead —
    // a different path (see PROGRESS.md: a delegate's replies behind its
    // own unshipped rows still wait for the log).
    let (env, _root, mut clients, proxies) = cluster(
        name,
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
        let deleg_gen = if delegated {
            let b = &clients[1];
            delegate(a, "/shared", node_id(b)?)?;
            let gen = wait_installed(b, "/shared", Duration::from_secs(30))?;
            // The delegate acknowledges under a backup of its own.
            eventually("b chose a backup", Duration::from_secs(30), || {
                let d = b.control_status()?["delegation"].clone();
                anyhow::ensure!(
                    d["backups"].as_array().is_some_and(|v| !v.is_empty()),
                    "b has no backup yet: {d}"
                );
                Ok(())
            })?;
            eprintln!("    {name}: /shared delegated to b (gen {gen})");
            Some(gen)
        } else {
            None
        };
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
        eprintln!("    {name}: a's backups {backups:?}");
        std::thread::sleep(Duration::from_secs(1));
        env.existing_s3_proxy().latency(lat, 0)?;
        let rtt = Duration::from_millis(2 * lat);
        eprintln!("    {name}: S3 latency injected: every request >= {rtt:?}");
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
            let owner = if delegated {
                k == 1
            } else {
                lease_of(x)?["held"] == true
            };
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
            eprintln!("    {name}: {row}");
        }
        let mut deleg_streamed = 0u64;
        for x in &clients {
            let s = x.control_status()?;
            eprintln!(
                "    {name}: {}: inbox ops {}, lease held {}, write mode {}, forwards that \
                 waited for their transaction {} (answered from the pre-S3 stream {}, a \
                 delegate's {}); streamed transactions installed {} dropped {}",
                x.name,
                s["inbox"]["submitted_ops"],
                s["lease"]["held"],
                s["writeback"]["mode"],
                s["ack"]["awaited_log"],
                s["ack"]["awaited_log_streamed"],
                s["ack"]["awaited_log_streamed_deleg"],
                s["ack"]["streamed_installed"],
                s["ack"]["streamed_dropped"]
            );
            deleg_streamed += s["ack"]["awaited_log_streamed_deleg"].as_u64().unwrap_or(0);
        }
        env.existing_s3_proxy().remove_all_toxics()?;
        if let Some(gen) = deleg_gen {
            // The run measured the delegated path throughout.
            let b = &clients[1];
            let d = b.control_status()?["delegation"].clone();
            let table = d["table"].as_array().cloned().unwrap_or_default();
            anyhow::ensure!(
                table
                    .iter()
                    .any(|e| e["path"] == "/shared" && e["gen"].as_u64() == Some(gen)),
                "{name}: /shared is no longer b's gen {gen}: {d}"
            );
            anyhow::ensure!(
                deleg_streamed > 0,
                "{name}: no delegate reply was answered from the root's pre-S3 stream"
            );
        }
        anyhow::ensure!(failures.is_empty(), "{name}: {failures:#?}");
        Ok(())
    })();
    if result.is_err() {
        keep_logs(name, &clients);
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// The product defaults the latency scenarios below mount with (the
/// harness otherwise shortens S3 retries and the sync round for every
/// client, which is not what the EC2 runs measured).
const PRODUCT_DEFAULTS: [(&str, &str); 4] = [
    ("CONSTELLATION_SYNC_INTERVAL_MS", "500"),
    ("CONSTELLATION_LEASE_TTL_MS", "60000"),
    ("CONSTELLATION_S3_MAX_RETRIES", "\u{0}unset"),
    ("CONSTELLATION_S3_RETRY_TIMEOUT_MS", "\u{0}unset"),
];

/// One paced write+`fsync`+close of `v<i>` holding `"<tag>:<i>"` (a
/// chunk no node has yet). Returns (when the open began, when the close
/// returned).
fn vis_write(dir: &Path, tag: &str, i: usize) -> Result<(Instant, Instant)> {
    use std::io::Write;
    let t0 = Instant::now();
    let mut f = std::fs::File::create(dir.join(format!("v{i}")))?;
    f.write_all(format!("{tag}:{i}").as_bytes())?;
    f.sync_all()?;
    drop(f);
    Ok((t0, Instant::now()))
}

/// What one poller saw of one event.
#[derive(Clone, Copy, Default, Debug)]
struct Seen {
    /// The first successful read of the right content.
    at: Option<Instant>,
    /// How long that one successful open+read took.
    read_cost: Duration,
    /// Failed attempts before it (the name missing, or wrong content).
    misses: u32,
}

/// Campaign 6's D2 poller: every event in order, polling each until its
/// content reads back (or `deadline` passes).
fn vis_poll(dir: &Path, tag: &str, events: usize, deadline: Duration) -> Vec<Seen> {
    let mut out = vec![Seen::default(); events];
    let end = Instant::now() + deadline;
    for (i, seen) in out.iter_mut().enumerate() {
        let want = format!("{tag}:{i}");
        while Instant::now() < end {
            let t = Instant::now();
            if std::fs::read(dir.join(format!("v{i}"))).ok().as_deref() == Some(want.as_bytes()) {
                seen.at = Some(Instant::now());
                seen.read_cost = t.elapsed();
                break;
            }
            seen.misses += 1;
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    out
}

/// Campaign 6 finding D2-OVH: with the S3 bucket an ocean away (every
/// request >= 2 x `VISLAT_MS`), one node writes a paced series of small
/// files (write+`fsync`+close each, the default `--write-mode through`)
/// while two others poll for each in order, as the EC2 D2 driver did.
/// Visibility travels over P2P (the holder's log stream and its pre-S3
/// stream) and must not wait for S3: every poller's p99, measured from
/// the writer's `open` to the first read of the right content, stays
/// under `VISLAT_P99_MS` (2 s). The content of a file another node just
/// wrote comes from the writer, not the bucket: a poller's median from
/// the writer's close to its read stays under half an S3 round trip,
/// and the pollers GET (almost) no chunks. Runs twice: a non-holder
/// writing (its closes forwarded to the holder; root lease placement
/// moves the lease to it), then the other node writing.
pub fn visibility_s3_latency(_seed: u64) -> Result<()> {
    const NAME: &str = "visibility-s3-latency";
    let lat = knob("VISLAT_MS", 150);
    let events = knob("VISLAT_EVENTS", 60) as usize;
    let interval = Duration::from_millis(knob("VISLAT_INTERVAL_MS", 50));
    let bound = Duration::from_millis(knob("VISLAT_P99_MS", 2000));
    // Diagnosis: `VISLAT_RUST_LOG=<filter>` for the mounts, and
    // `VISLAT_KEEP_LOGS=1` keeps their logs even when the run passes.
    let mut extra = PRODUCT_DEFAULTS.to_vec();
    let filter = std::env::var("VISLAT_RUST_LOG").unwrap_or_default();
    if !filter.is_empty() {
        extra.push(("RUST_LOG", filter.as_str()));
    }
    let pin = std::env::var_os("VISLAT_PIN_LEASE").is_some();
    if pin {
        extra.push(("CONSTELLATION_LEASE_PLACEMENT", "off"));
    }
    let (env, _root, mut clients, proxies) = cluster(NAME, &["a", "b", "c"], &extra, 3)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        std::fs::create_dir(a.mnt.join("vis-b"))?;
        std::fs::create_dir(a.mnt.join("vis-a"))?;
        for x in &clients {
            eventually("vis dirs visible", Duration::from_secs(30), || {
                anyhow::ensure!(x.mnt.join("vis-a").is_dir() && x.mnt.join("vis-b").is_dir());
                Ok(())
            })?;
        }
        let backups = super::m9::wait_for_backup(a, Duration::from_secs(60))?;
        eprintln!("    {NAME}: a holds the lease, backups {backups:?}");
        env.existing_s3_proxy().latency(lat, 0)?;
        eprintln!(
            "    {NAME}: S3 latency injected: every request >= {:?}",
            Duration::from_millis(2 * lat)
        );
        let mut failures = Vec::new();
        // (writer, pollers)
        for (w, pollers) in [(1usize, [0usize, 2]), (0, [1, 2])] {
            let writer = &clients[w];
            let sub = format!("vis-{}", writer.name);
            let holder = clients
                .iter()
                .find(|c| lease_of(c).is_ok_and(|l| l["held"] == true))
                .map(|c| c.name.clone())
                .unwrap_or_default();
            eprintln!("    {NAME}: writer {}, lease holder {holder}", writer.name);
            let tag = writer.name.clone();
            for p in &proxies {
                p.reset();
            }
            let handles: Vec<_> = pollers
                .iter()
                .map(|&k| {
                    let dir = clients[k].mnt.join(&sub);
                    let who = clients[k].name.clone();
                    let tag = tag.clone();
                    std::thread::spawn(move || {
                        (who, vis_poll(&dir, &tag, events, Duration::from_secs(120)))
                    })
                })
                .collect();
            let wdir = writer.mnt.join(&sub);
            let mut written = Vec::new();
            for i in 0..events {
                let (t0, t1) = vis_write(&wdir, &tag, i)
                    .with_context(|| format!("{}: write {i}", writer.name))?;
                written.push((t0, t1));
                let spent = t1 - t0;
                if spent < interval {
                    std::thread::sleep(interval - spent);
                }
            }
            let close_cost: Vec<Duration> = written.iter().map(|(a, b)| *b - *a).collect();
            eprintln!(
                "    {NAME}: writer {}: write+fsync+close {}",
                writer.name,
                dist(close_cost)
            );
            for h in handles {
                let (who, seen) = h.join().map_err(|_| anyhow::anyhow!("poller panicked"))?;
                let mut lat_open = Vec::new();
                let mut lat_close = Vec::new();
                let mut lat_close_all = Vec::new();
                let mut costs = Vec::new();
                let mut missing = 0;
                for (i, (s, (t0, t1))) in seen.iter().zip(&written).enumerate() {
                    match s.at {
                        Some(at) => {
                            lat_open.push(at.saturating_duration_since(*t0));
                            lat_close.push(at.saturating_duration_since(*t1));
                            lat_close_all.push(at.saturating_duration_since(*t1));
                            costs.push(s.read_cost);
                            if std::env::var_os("VIS_DEBUG").is_some() {
                                eprintln!(
                                    "      {who} v{i}: +{:?} after open, +{:?} after close, \
                                     read {:?}, {} misses",
                                    at.saturating_duration_since(*t0),
                                    at.saturating_duration_since(*t1),
                                    s.read_cost,
                                    s.misses
                                );
                            }
                        }
                        None => missing += 1,
                    }
                }
                let mut sorted = lat_open.clone();
                sorted.sort();
                let p99 = sorted
                    .get(((sorted.len() as f64 - 1.0) * 0.99).round() as usize)
                    .copied()
                    .unwrap_or_default();
                eprintln!(
                    "    {NAME}: writer {} -> poller {who}: from open {}; from close {}; \
                     successful read {}; never seen {missing}",
                    writer.name,
                    dist(lat_open),
                    dist(lat_close),
                    dist(costs)
                );
                if missing > 0 {
                    failures.push(format!(
                        "{who} never saw {missing} of {}'s files",
                        writer.name
                    ));
                }
                if p99 >= bound {
                    failures.push(format!(
                        "writer {} -> {who}: visibility p99 {p99:?} >= {bound:?}",
                        writer.name
                    ));
                }
                let after_close = p50(lat_close_all.clone());
                if after_close >= Duration::from_millis(lat) {
                    failures.push(format!(
                        "writer {} -> {who}: median from close to read {after_close:?} >= half \
                         an S3 round trip ({lat} ms): the read waited for S3",
                        writer.name
                    ));
                }
            }
            for (k, p) in proxies.iter().enumerate() {
                if k == w {
                    continue;
                }
                let gets = p
                    .requests()
                    .iter()
                    .filter(|r| r.method == "GET" && !r.is_list() && r.area() == "chunks")
                    .count();
                if gets > events / 10 {
                    failures.push(format!(
                        "writer {}: poller {} GET {gets} chunks from S3 for {events} files its \
                         peer had",
                        writer.name, clients[k].name
                    ));
                }
            }
            let breakdown = proxies
                .iter()
                .zip(&clients)
                .map(|(p, c)| format!("[{}] {}", c.name, s3_breakdown(p)))
                .collect::<Vec<_>>()
                .join(" ");
            eprintln!("    {NAME}: writer {}: S3 {breakdown}", writer.name);
        }
        for x in &clients {
            let s = x.control_status()?;
            eprintln!(
                "    {NAME}: {}: coop peer hits {} fresh-hint hits {} misses {}, S3 fetches {}, \
                 hedges {}; streamed ahead {} installed {} dropped {}; log stream {}",
                x.name,
                s["coop"]["peer_hits"],
                s["coop"]["fresh_hint_hits"],
                s["coop"]["fresh_hint_misses"],
                s["coop"]["s3_fetches"],
                s["coop"]["hedges_fired"],
                s["ack"]["streamed_ahead"],
                s["ack"]["streamed_installed"],
                s["ack"]["streamed_dropped"],
                s["log_stream"]
            );
        }
        env.existing_s3_proxy().remove_all_toxics()?;
        anyhow::ensure!(failures.is_empty(), "{NAME}: {failures:#?}");
        Ok(())
    })();
    if result.is_err() || std::env::var_os("VISLAT_KEEP_LOGS").is_some() {
        keep_logs(NAME, &clients);
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// Campaign 6 finding A-1 (and campaign 4's): SQLite's first touch of a
/// new database from two nodes at once hit `disk I/O error` on the OVH
/// bucket (4 of 40 rounds) and never on AWS. The failing rounds began
/// the moment placement delegated the race directory to one of the two
/// racers. Under injected S3 latency (every request >= 2 x
/// `SQLITE_LAT_MS`, so every write-through `fsync` holds SQLite's lock
/// for a few hundred ms), `SQLITE_ROUNDS` (50) rounds of two nodes
/// running `CREATE TABLE IF NOT EXISTS` + `INSERT` on one new database,
/// cycling through every pair of the three nodes, alternately in a
/// directory the root sequences and in one delegated to `b` (so the
/// locks are granted by a delegate, capped by its delegation): no round
/// may fail, and every database holds both rows on every node.
pub fn sqlite_first_touch_latency(_seed: u64) -> Result<()> {
    const NAME: &str = "sqlite-first-touch-latency";
    if !sqlite_available() {
        eprintln!("    {NAME}: no sqlite3 on the host; skipped");
        return Ok(());
    }
    let lat = knob("SQLITE_LAT_MS", 150);
    let rounds = knob("SQLITE_ROUNDS", 50) as usize;
    let mut extra = PRODUCT_DEFAULTS.to_vec();
    // The delegation below stays put (placement would recall it once
    // its share drops).
    extra.push(("CONSTELLATION_DELEGATION_PLACEMENT", "off"));
    let (env, _root, mut clients, _proxies) = cluster(NAME, &["a", "b", "c"], &extra, 3)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        std::fs::create_dir(a.mnt.join("race"))?;
        std::fs::create_dir(a.mnt.join("deleg"))?;
        for x in &clients {
            eventually("race dirs visible", Duration::from_secs(30), || {
                anyhow::ensure!(x.mnt.join("race").is_dir() && x.mnt.join("deleg").is_dir());
                Ok(())
            })?;
        }
        super::m9::wait_for_backup(a, Duration::from_secs(60))?;
        delegate(a, "/deleg", node_id(&clients[1])?)?;
        let gen = wait_installed(&clients[1], "/deleg", Duration::from_secs(30))?;
        eprintln!("    {NAME}: /deleg delegated to b (gen {gen})");
        env.existing_s3_proxy().latency(lat, 0)?;
        let mut failures = Vec::new();
        let started = Instant::now();
        const PAIRS: [[usize; 2]; 3] = [[1, 2], [0, 1], [0, 2]];
        for i in 0..rounds {
            let pair = PAIRS[(i / 2) % 3];
            let dir = if i % 2 == 0 { "race" } else { "deleg" };
            let name = format!("{dir}/first-touch-{i}.db");
            let barrier = Arc::new(Barrier::new(2));
            let handles: Vec<_> = pair
                .iter()
                .map(|&k| {
                    let db = clients[k].mnt.join(&name);
                    let who = clients[k].name.clone();
                    let barrier = barrier.clone();
                    // Diagnosis: `SQLITE_STRACE_DIR=<dir>` records every
                    // failed syscall of each racer (`strace -Z`).
                    let strace = std::env::var("SQLITE_STRACE_DIR")
                        .ok()
                        .map(|d| format!("{d}/round{i}-{who}.strace"));
                    std::thread::spawn(move || {
                        barrier.wait();
                        let t = Instant::now();
                        let mut cmd = match &strace {
                            Some(out) => {
                                let mut c = std::process::Command::new("strace");
                                c.args(["-f", "-Z", "-tt", "-o", out, "sqlite3"]);
                                c
                            }
                            None => std::process::Command::new("sqlite3"),
                        };
                        let out = cmd
                            .arg("-cmd")
                            .arg(".timeout 10000")
                            .arg(&db)
                            .arg(format!(
                                "CREATE TABLE IF NOT EXISTS t(node TEXT); \
                                 INSERT INTO t VALUES('{who}');"
                            ))
                            .output();
                        (who, t.elapsed(), out)
                    })
                })
                .collect();
            for h in handles {
                let (who, took, out) = h.join().expect("sqlite racer");
                let out = out.context("running sqlite3")?;
                if !out.status.success() {
                    let msg = format!(
                        "round {i}: {who} after {took:?}: {}{}",
                        String::from_utf8_lossy(&out.stdout).trim(),
                        String::from_utf8_lossy(&out.stderr).trim()
                    );
                    eprintln!("    {NAME}: FAILED {msg}");
                    failures.push(msg);
                }
            }
        }
        eprintln!(
            "    {NAME}: {rounds} rounds in {:?}, {} failed",
            started.elapsed(),
            failures.len()
        );
        env.existing_s3_proxy().remove_all_toxics()?;
        // Every database holds both racers' rows, on every node.
        anyhow::ensure!(failures.is_empty(), "{NAME}: {failures:#?}");
        for x in &clients {
            eventually(
                &format!("{} reads both rows of every database", x.name),
                Duration::from_secs(60),
                || {
                    for i in 0..rounds {
                        let dir = if i % 2 == 0 { "race" } else { "deleg" };
                        let name = format!("{dir}/first-touch-{i}.db");
                        let out = std::process::Command::new("sqlite3")
                            .arg("-cmd")
                            .arg(".timeout 10000")
                            .arg(x.mnt.join(&name))
                            .arg("SELECT count(*) FROM t;")
                            .output()?;
                        let rows = String::from_utf8_lossy(&out.stdout).trim().to_string();
                        anyhow::ensure!(
                            out.status.success() && rows == "2",
                            "{}: {name} holds {rows:?} rows ({})",
                            x.name,
                            String::from_utf8_lossy(&out.stderr).trim()
                        );
                    }
                    Ok(())
                },
            )?;
        }
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
