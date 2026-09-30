//! EC2 campaign 6, finding B-1: a node `kill -9`ed while it held the
//! lease hung on its remount. The hang was not in P2P: the killed daemon
//! lingered as a zombie whose last thread was stuck in the kernel, so its
//! file table — `daemon.lock`'s flock and the `control.sock` listener —
//! stayed alive; the remount found the lock held, connected to the
//! listener (the kernel accepts for a listener nobody serves), and waited
//! forever for an answer. `constellation status` hung the same way.
//!
//! - `holder-kill-rejoin`: the campaign's fault, many times over, with
//!   the remount bounded: kill -9 the lease holder (plain, with a flock
//!   held on it, or one of its backups instead), remount it with P2P on,
//!   converge.
//! - `stale-daemon-lock`: the exact condition, with a stand-in for the
//!   zombie (`harness mute-daemon`: holds the lock and the listener,
//!   never answers). A live holder is never taken over: the mount fails
//!   within the attach timeout, explained, and `status` fails within the
//!   control timeout. A holder the kernel has killed (the classifier's
//!   verdict, here via the `CONSTELLATION_FAULT_ASSUME_WEDGED_PID` hook,
//!   since no test can fabricate a thread stuck in the kernel) is taken
//!   over: the next mount rotates the lock and serves.

use super::m8::{dist, write_timed};
use super::m9::{ack_of, all_visible, cluster, node_id, unmount_all, wait_for_backup, write_files};
use super::{ensure_no_conflicts, eventually, lease_of, wait_for_p2p};
use crate::client::Client;
use anyhow::{bail, Context, Result};
use std::time::{Duration, Instant};

/// The index of the client that holds the lease, within `deadline`. On
/// failure, every node's lease and acknowledgement state and log tail.
pub(super) fn current_holder(clients: &[Client], deadline: Duration) -> Result<usize> {
    let mut holder = None;
    let waited = eventually("some node holds the lease", deadline, || {
        for (i, c) in clients.iter().enumerate() {
            if lease_of(c).is_ok_and(|l| l["held"] == true) {
                holder = Some(i);
                return Ok(());
            }
        }
        bail!("no node holds the lease")
    });
    if let Err(e) = waited {
        for c in clients {
            eprintln!(
                "    {}: lease {} ack {}\n{}",
                c.name,
                lease_of(c).unwrap_or_default(),
                ack_of(c).unwrap_or_default(),
                c.tail_log_n(40)
            );
        }
        return Err(e);
    }
    holder.context("no holder")
}

/// A `flock(LOCK_EX)` held on `path` from another thread until
/// [`FlockGuard::release`].
struct FlockGuard {
    release: std::sync::mpsc::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

impl FlockGuard {
    /// Hold the lock, which must be granted within `within`.
    fn hold(path: std::path::PathBuf, within: Duration) -> Result<Self> {
        let path_name = path.display().to_string();
        let (held_tx, held_rx) = std::sync::mpsc::channel::<Result<()>>();
        let (release, release_rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            use std::os::fd::AsRawFd;
            let file = match std::fs::File::create(&path) {
                Ok(f) => f,
                Err(e) => {
                    let _ = held_tx.send(Err(e.into()));
                    return;
                }
            };
            // SAFETY: `file` owns a valid open fd for the duration of the call.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                let _ = held_tx.send(Err(std::io::Error::last_os_error().into()));
                return;
            }
            let _ = held_tx.send(Ok(()));
            let _ = release_rx.recv();
            // The mount is gone by now (its daemon was killed); closing
            // the fd on a dead FUSE mount returns at once.
            drop(file);
        });
        held_rx
            .recv_timeout(within)
            .with_context(|| format!("flock on {} not granted within {within:?}", path_name))?
            .context("holding a flock")?;
        Ok(Self { release, thread })
    }

    fn release(self) {
        let _ = self.release.send(());
        let _ = self.thread.join();
    }
}

pub fn holder_kill_rejoin(_seed: u64) -> Result<()> {
    const NAME: &str = "holder-kill-rejoin";
    let rounds: usize = std::env::var("HOLDER_KILL_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &usize| *v > 0)
        .unwrap_or(10);
    let bound = Duration::from_secs(60);
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c", "d"], &[], 0)?;
    let result = (|| -> Result<()> {
        let mut all: Vec<String> = Vec::new();
        let mut remounts = Vec::new();
        let mut last_victim = 0usize;
        let mut first_writes = Vec::new();
        for round in 0..rounds {
            // The lease is claimed on demand: a write on some node (the
            // one remounted last round, every other round) claims it or
            // forwards to whoever holds it. Its latency is the cluster's
            // write outage around the kill: nil when the returning
            // holder adopts its own unexpired lease, seconds when a
            // backup sealed and took over, the 20 s TTL otherwise.
            let writer = if round % 2 == 1 {
                last_victim
            } else {
                round % clients.len()
            };
            let probe = format!("probe-{round}");
            let t = Instant::now();
            write_timed(&clients[writer].mnt.join(&probe), probe.as_bytes()).with_context(
                || format!("round {round}: first write on {}", clients[writer].name),
            )?;
            let holder = current_holder(&clients, Duration::from_secs(60))?;
            if round > 0 {
                first_writes.push(t.elapsed());
                eprintln!(
                    "    {NAME}: round {round}: first write after the remount ({}) took {:?}; \
                     holder {}",
                    clients[writer].name,
                    t.elapsed(),
                    clients[holder].name
                );
            }
            all.push(probe);
            let variant = round % 3;
            let victim = if variant == 2 {
                // One of the holder's backups (a node whose tail the
                // holder's acknowledgements depend on), if it has one yet.
                match wait_for_backup(&clients[holder], Duration::from_secs(20)) {
                    Ok(backups) => (0..clients.len())
                        .find(|i| node_id(&clients[*i]).ok() == Some(backups[0]))
                        .unwrap_or(holder),
                    Err(_) => holder,
                }
            } else {
                holder
            };
            let what = match variant {
                0 => "the holder",
                1 => "the holder, with a flock held on it",
                _ if victim != holder => "a backup of the holder",
                _ => "the holder (no backup yet)",
            };
            let (names, _) = write_files(&clients[holder], &format!("r{round}"), 5)?;
            all.extend(names);
            let flock = if variant == 1 {
                Some(FlockGuard::hold(
                    clients[victim].mnt.join(format!("lock-{round}")),
                    Duration::from_secs(30),
                )?)
            } else {
                None
            };
            let epoch = lease_of(&clients[holder])?["epoch"].clone();
            eprintln!(
                "    {NAME}: round {round}: kill -9 {} ({what}; holder {} epoch {epoch})",
                clients[victim].name, clients[holder].name
            );
            clients[victim].kill9()?;
            if let Some(flock) = flock {
                flock.release();
            }
            // Vary the gap between the kill and the remount: at once,
            // inside the backup's seal window, and after it.
            std::thread::sleep(Duration::from_millis([0, 1500, 4000][variant]));
            let t = Instant::now();
            clients[victim]
                .mount_within(bound)
                .with_context(|| format!("round {round}: remounting {}", clients[victim].name))?;
            let took = t.elapsed();
            remounts.push(took);
            last_victim = victim;
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
            for c in &clients {
                all_visible(c, &all, Duration::from_secs(60))?;
            }
            if variant == 1 {
                // The lock the dead node held is grantable again: its
                // grant is outwaited or recalled, never leaked.
                let t = Instant::now();
                let other = (victim + 1) % clients.len();
                FlockGuard::hold(
                    clients[other].mnt.join(format!("lock-{round}")),
                    Duration::from_secs(60),
                )?
                .release();
                eprintln!(
                    "    {NAME}: round {round}: {} took the dead node's lock after {:?}",
                    clients[other].name,
                    t.elapsed()
                );
            }
            let lease = lease_of(&clients[victim])?;
            eprintln!(
                "    {NAME}: round {round}: {} remounted in {took:?} and every node converged \
                 ({} files); it now reports held={} epoch={}",
                clients[victim].name,
                all.len(),
                lease["held"],
                lease["epoch"]
            );
        }
        eprintln!(
            "    {NAME}: remount time over {rounds} rounds {}",
            dist(remounts)
        );
        eprintln!(
            "    {NAME}: first write after a remount {}",
            dist(first_writes)
        );
        let refs: Vec<&Client> = clients.iter().collect();
        ensure_no_conflicts(&refs)?;
        for c in &clients {
            let a = ack_of(c)?;
            eprintln!(
                "    {NAME}: {} seals {} takeovers {}",
                c.name, a["seals"], a["backup_takeovers"]
            );
        }
        Ok(())
    })();
    unmount_all(&mut clients);
    result
}

/// Spawn `harness mute-daemon <state_dir>` and wait for it to hold the
/// lock and the socket.
fn spawn_mute_daemon(
    state_dir: &std::path::Path,
    log: &std::path::Path,
) -> Result<std::process::Child> {
    let logf = std::fs::File::create(log)?;
    let child = std::process::Command::new(std::env::current_exe()?)
        .arg("mute-daemon")
        .arg(state_dir)
        .stdout(std::process::Stdio::from(logf.try_clone()?))
        .stderr(std::process::Stdio::from(logf))
        .spawn()
        .context("spawning the mute daemon")?;
    let pid = state_dir.join("daemon.pid");
    eventually(
        "mute daemon holds the state dir",
        Duration::from_secs(10),
        || {
            let sock = constellation_control::transport::locate_socket(state_dir);
            anyhow::ensure!(sock.is_some_and(|s| s.exists()) && pid.exists(), "not yet");
            Ok(())
        },
    )?;
    Ok(child)
}

pub fn stale_daemon_lock(_seed: u64) -> Result<()> {
    const NAME: &str = "stale-daemon-lock";
    let (_env, root, mut clients, _) = cluster(NAME, &["a"], &[], 0)?;
    let mut mute: Option<std::process::Child> = None;
    let result = (|| -> Result<()> {
        let a = &mut clients[0];
        std::fs::write(a.mnt.join("keep"), b"keep")?;
        a.unmount()?;
        let state_dir = a.state_dir().to_path_buf();
        let child = spawn_mute_daemon(&state_dir, &root.path().join("mute.log"))?;
        let mute_pid = child.id();
        mute = Some(child);
        eprintln!("    {NAME}: mute daemon {mute_pid} holds daemon.lock and control.sock");

        // 1. A holder that is alive (however mute) is never taken over:
        //    the mount fails within the attach timeout and says why.
        a.set_env("CONSTELLATION_CONTROL_TIMEOUT_MS", "2000");
        a.set_env("CONSTELLATION_ATTACH_TIMEOUT_MS", "6000");
        let t = Instant::now();
        let err = match a.mount_within(Duration::from_secs(60)) {
            Ok(()) => bail!("the mount attached to (or took over) a live, mute holder"),
            Err(e) => format!("{e:#}"),
        };
        let took = t.elapsed();
        eprintln!("    {NAME}: mount against the live mute holder failed after {took:?}");
        anyhow::ensure!(
            err.contains("mount died at startup"),
            "the mount did not exit on its own: {err}"
        );
        for needle in [
            "could not be attached to within",
            &format!("pid {mute_pid}"),
            "is alive",
            "Not taking its state dir over",
        ] {
            anyhow::ensure!(err.contains(needle), "mount error lacks {needle:?}: {err}");
        }
        anyhow::ensure!(
            took < Duration::from_secs(30),
            "the mount took {took:?} to give up (attach timeout 6 s)"
        );
        anyhow::ensure!(
            !state_dir
                .join(format!("daemon.lock.wedged-{mute_pid}"))
                .exists(),
            "the live holder's lock was rotated"
        );

        // `status` is bounded the same way.
        let t = Instant::now();
        let out = a.status_cli()?;
        let took = t.elapsed();
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::ensure!(
            !out.status.success(),
            "status succeeded against a mute daemon"
        );
        anyhow::ensure!(
            stderr.contains("did not answer within"),
            "status did not say the daemon was mute: {stderr}"
        );
        anyhow::ensure!(took < Duration::from_secs(15), "status took {took:?}");
        eprintln!("    {NAME}: status against the mute holder failed after {took:?}");

        // 2. The same holder, once the kernel has killed it (the
        //    classifier's verdict, injected): the mount takes over.
        a.set_env(
            "CONSTELLATION_FAULT_ASSUME_WEDGED_PID",
            &mute_pid.to_string(),
        );
        let t = Instant::now();
        a.mount_within(Duration::from_secs(60))
            .context("mount after the holder counts as killed")?;
        let took = t.elapsed();
        eprintln!("    {NAME}: mount took the state dir over in {took:?}");
        anyhow::ensure!(
            state_dir
                .join(format!("daemon.lock.wedged-{mute_pid}"))
                .exists(),
            "the wedged holder's lock was not moved aside"
        );
        anyhow::ensure!(
            std::fs::read(a.mnt.join("keep"))? == b"keep",
            "the state dir's data is not served after the takeover"
        );
        let log = std::fs::read_to_string(root.path().join("a").join("mount.log"))?;
        anyhow::ensure!(
            log.contains("took the state dir over"),
            "the takeover was not logged: {}",
            a.tail_log()
        );
        anyhow::ensure!(
            log.contains("startup complete"),
            "startup phases were not logged: {}",
            a.tail_log()
        );
        anyhow::ensure!(
            a.control_status()?["node_id"].as_u64().is_some(),
            "the new daemon does not answer status"
        );
        std::fs::write(a.mnt.join("after"), b"after")?;
        a.unmount()?;
        let mute_child = mute.as_mut().context("mute daemon")?;
        anyhow::ensure!(
            mute_child.try_wait()?.is_none(),
            "the mute daemon died on its own"
        );
        mute_child.kill()?;
        let _ = mute_child.wait();
        mute = None;

        // 3. With the old holder gone, an ordinary mount serves both
        //    files.
        a.unset_env("CONSTELLATION_FAULT_ASSUME_WEDGED_PID");
        a.mount_within(Duration::from_secs(60))?;
        anyhow::ensure!(std::fs::read(a.mnt.join("keep"))? == b"keep");
        anyhow::ensure!(std::fs::read(a.mnt.join("after"))? == b"after");
        a.unmount()?;
        eprintln!("    {NAME}: clean remount after the holder is gone serves everything");
        Ok(())
    })();
    if let Some(mut child) = mute {
        let _ = child.kill();
        let _ = child.wait();
    }
    unmount_all(&mut clients);
    result
}
