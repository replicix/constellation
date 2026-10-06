//! The state dir's `daemon.lock`, and what a new `mount` does when it
//! finds the lock held by a daemon that does not answer (EC2 campaign 6,
//! finding B-1).
//!
//! `mount` takes `daemon.lock` (`flock`) to decide between becoming the
//! daemon and attaching to the one that holds it. The lock and the
//! control socket belong to the holder's file table, and a file table
//! outlives `kill -9` for as long as *any* thread of the process is still
//! in the kernel: a thread stuck in an uninterruptible wait (a FUSE
//! request against a connection that is only aborted when the very file
//! table it is waiting in closes, a hung disk) keeps the whole table —
//! lock, listener, `/dev/fuse` — alive while the process is a zombie
//! (`State: Z`, `Threads: 2`, SIGKILL pending) that will never run user
//! code again. A remount then found the lock held, connected to the
//! listener (the kernel accepts for a listener nobody serves), and waited
//! forever for an answer that could not come; so did every `status`.
//!
//! The rule now: a lock holder that does not answer a bounded ping is
//! classified from what the kernel reports about it (`/proc` on Linux,
//! read by `constellation_platform`'s `Process::facts`; the decision
//! itself, [`classify`], stays here and is pure). Only a holder the
//! kernel itself says can never run again — thread-group leader a zombie
//! *and* (SIGKILL pending or every remaining thread already past
//! `exit_mm`) — is taken over:
//! its `daemon.lock` inode is unlinked (the zombie keeps its lock on the
//! orphan inode), the stale control socket (and `control.path`)/`daemon.pid` are removed, and
//! the caller takes a fresh lock. Anything alive (running, sleeping,
//! stopped by SIGSTOP, or a zombie leader whose other threads could still
//! run) is never taken over: the mount fails within a bound, naming the
//! pid and its state. Safety never depends on a liveness guess: the only
//! takeover is of a process the kernel has already killed.

use anyhow::{bail, Context, Result};
use constellation_platform::{native, ProcessFacts, UnmountMode};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const LOCK_NAME: &str = "daemon.lock";
/// A second lock, never rotated, that serialises takeovers so two
/// concurrent mounts cannot both unlink-and-recreate `daemon.lock` and
/// end up as two daemons on one state dir.
const TAKEOVER_MUTEX: &str = "daemon.takeover.lock";

/// What the kernel says about the process holding `daemon.lock`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Holder {
    /// A process that can still run: attach to it, or fail; never take
    /// its state dir.
    Live {
        pid: u32,
        state: String,
        name: String,
    },
    /// A process that can never run user code again, still holding the
    /// lock only because one thread is stuck in the kernel on its way
    /// out.
    Wedged { pid: u32, name: String, why: String },
    /// The holder could not be identified (no `/proc`, another pid
    /// namespace, the lock released meanwhile).
    Unknown { detail: String },
}

impl std::fmt::Display for Holder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Holder::Live { pid, state, name } => {
                write!(f, "pid {pid} ({name}, state {state}) is alive")
            }
            Holder::Wedged { pid, name, why } => {
                write!(f, "pid {pid} ({name}) can never run again: {why}")
            }
            Holder::Unknown { detail } => write!(f, "holder unknown: {detail}"),
        }
    }
}

/// Identify and classify whoever holds `daemon.lock` in `state_dir`.
pub fn holder(state_dir: &Path) -> Holder {
    let lock = state_dir.join(LOCK_NAME);
    let candidates = lock_holder_pids(&lock);
    let pid = if candidates.is_empty() {
        match pid_file(state_dir) {
            Some(pid) => pid,
            None => {
                return Holder::Unknown {
                    detail: "no flock on daemon.lock in /proc/locks and no daemon.pid".into(),
                }
            }
        }
    } else {
        match confirmed_holder(state_dir, &candidates) {
            Some(pid) => pid,
            None => {
                return Holder::Unknown {
                    detail: format!(
                        "/proc/locks names pid(s) {candidates:?} for a flock under daemon.lock's                          device and inode, but none is daemon.pid or has daemon.lock open (the                          key is shared: btrfs subvolumes number their inodes alike)"
                    ),
                }
            }
        }
    };
    classify_pid(pid)
}

/// Of the pids `/proc/locks` names under `daemon.lock`'s device and inode
/// (which may hold a lock on another subvolume's file of the same number,
/// see `FileLock::holder_pids`), the one confirmed to hold *this* file:
/// the pid in `daemon.pid`, or one that has this `daemon.lock` open.
fn confirmed_holder(state_dir: &Path, candidates: &[u32]) -> Option<u32> {
    let pid_file = pid_file(state_dir);
    if let Some(pid) = candidates.iter().find(|&&pid| Some(pid) == pid_file) {
        return Some(*pid);
    }
    let lock = state_dir.join(LOCK_NAME);
    let mut confirmed = candidates
        .iter()
        .filter(|&&pid| native().file_lock.opened_by(pid, &lock));
    match (confirmed.next(), confirmed.next()) {
        (Some(pid), None) => Some(*pid),
        _ => None,
    }
}

/// The pid from `daemon.pid`, if any (written by a daemon once it is up;
/// a fallback for hosts whose `/proc/locks` does not name the locker).
fn pid_file(state_dir: &Path) -> Option<u32> {
    std::fs::read_to_string(state_dir.join("daemon.pid"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// The pids that may hold the `flock` on `lock`, when the host can tell
/// (Linux: `/proc/locks` names them; more than one, or the wrong one, when
/// another file shares the key — confirm with `confirmed_holder`).
fn lock_holder_pids(lock: &Path) -> Vec<u32> {
    native().file_lock.holder_pids(lock)
}

/// Classify `pid` from what the kernel reports about it and its threads.
fn classify_pid(pid: u32) -> Holder {
    match native().process.facts(pid) {
        Ok(facts) => classify(pid, &facts),
        Err(e) => Holder::Unknown {
            detail: e.to_string(),
        },
    }
}

/// The decision, from the kernel's facts about the thread-group leader
/// and its other threads (on Linux, parsed from `/proc/<pid>/status` and
/// `task/<tid>/status`). Pure, so the shapes seen in the field can be
/// tested verbatim.
pub fn classify(pid: u32, facts: &ProcessFacts) -> Holder {
    let name = facts.name.clone();
    let state = facts.state.clone();
    let tasks = &facts.tasks;
    if !facts.zombie {
        // A process that is not exiting is alive, however unresponsive
        // (`T`: stopped by SIGSTOP; `D`: momentarily in the kernel).
        // `X` (dead) never shows for long; treat it as gone.
        if facts.dead {
            return Holder::Wedged {
                pid,
                name,
                why: "the process is dead (state X)".into(),
            };
        }
        return Holder::Live { pid, state, name };
    }
    // Leader is a zombie. Fully reaped-and-gone processes vanish from
    // /proc; one that lingers here has other threads still in the
    // kernel. It can never run user code again if the kernel has
    // already killed it (SIGKILL pending on the group: every return to
    // user space ends in death) or if every remaining thread has
    // released its address space (past `exit_mm`).
    let killed = facts.sigkill_pending || tasks.iter().any(|t| t.sigkill_pending);
    let no_mm = tasks.iter().all(|t| !t.has_mm);
    let threads = tasks.len();
    if killed {
        return Holder::Wedged {
            pid,
            name,
            why: format!(
                "its thread-group leader is a zombie with SIGKILL pending and {threads} thread(s) \
                 still stuck in the kernel (states {})",
                task_states(tasks)
            ),
        };
    }
    if no_mm {
        return Holder::Wedged {
            pid,
            name,
            why: format!(
                "its thread-group leader is a zombie and its remaining {threads} thread(s) have \
                 released their address space (states {})",
                task_states(tasks)
            ),
        };
    }
    // A leader that exited on its own while its threads live on.
    Holder::Live { pid, state, name }
}

fn task_states(tasks: &[constellation_platform::TaskFacts]) -> String {
    let states: Vec<&str> = tasks.iter().map(|t| t.state.as_str()).collect();
    if states.is_empty() {
        "none".into()
    } else {
        states.join(", ")
    }
}

/// How long `mount` and `status` wait for a control-socket answer before
/// declaring the daemon mute (`CONSTELLATION_CONTROL_TIMEOUT_MS`,
/// default 10 s).
pub fn control_timeout() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_CONTROL_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(10_000),
    )
}

/// How long a `mount` that found `daemon.lock` held keeps trying to
/// attach (a daemon still bootstrapping has no socket yet; a stopped one
/// may be continued) before failing (`CONSTELLATION_ATTACH_TIMEOUT_MS`,
/// default 120 s).
pub fn attach_timeout() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_ATTACH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(120_000),
    )
}

/// Test hook: `CONSTELLATION_FAULT_ASSUME_WEDGED_PID=<pid>` makes the
/// classifier report that pid as wedged, so the harness can exercise the
/// takeover with an ordinary live process standing in for a zombie whose
/// last thread is stuck in the kernel (which no test can fabricate).
fn assumed_wedged(pid: u32) -> bool {
    std::env::var("CONSTELLATION_FAULT_ASSUME_WEDGED_PID")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        == Some(pid)
}

/// The holder, with the test hook applied.
pub fn holder_for_takeover(state_dir: &Path) -> Holder {
    match holder(state_dir) {
        Holder::Live { pid, name, .. } if assumed_wedged(pid) => Holder::Wedged {
            pid,
            name,
            why: "CONSTELLATION_FAULT_ASSUME_WEDGED_PID (test hook)".into(),
        },
        h => h,
    }
}

/// Take `state_dir` over from `pid`, a holder classified as
/// [`Holder::Wedged`]: under the takeover mutex, re-verify that the lock
/// is still held by that same wedged process, then unlink its
/// `daemon.lock` inode and the stale control socket (with `control.path`) and `daemon.pid`. The
/// caller then takes a fresh `daemon.lock` as usual.
pub fn take_over(state_dir: &Path, pid: u32) -> Result<()> {
    let _mutex = takeover_mutex(state_dir)?;
    match holder_for_takeover(state_dir) {
        Holder::Wedged { pid: now, .. } if now == pid => {}
        Holder::Unknown { .. } if lock_holder_pids(&state_dir.join(LOCK_NAME)).is_empty() => {
            // Released meanwhile (or another mount already rotated it):
            // nothing to do, the caller's next flock decides.
            return Ok(());
        }
        other => bail!("daemon.lock changed hands during the takeover: {other}"),
    }
    let lock = state_dir.join(LOCK_NAME);
    let wedged = state_dir.join(format!("daemon.lock.wedged-{pid}"));
    // Keep the orphan inode under a forensic name rather than unlinking
    // it: `lslocks` then still shows the zombie's lock with a path.
    let _ = std::fs::remove_file(&wedged);
    std::fs::rename(&lock, &wedged).with_context(|| format!("moving {} aside", lock.display()))?;
    constellation_control::transport::forget_socket(state_dir);
    let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
    tracing::warn!(
        pid,
        state_dir = %state_dir.display(),
        "took the state dir over from a daemon the kernel has killed but that still held \
         daemon.lock and its control socket; its lock file is kept as daemon.lock.wedged-{pid}"
    );
    Ok(())
}

/// The takeover mutex, held for the returned guard's lifetime. Bounded:
/// a mutex holder is a `mount` mid-takeover (milliseconds).
fn takeover_mutex(state_dir: &Path) -> Result<constellation_platform::LockGuard> {
    let path: PathBuf = state_dir.join(TAKEOVER_MUTEX);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let file = constellation_platform::lock::open_lock_file(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        if let Some(guard) = native()
            .file_lock
            .try_lock(file)
            .context("locking the takeover mutex")?
        {
            return Ok(guard);
        }
        if Instant::now() >= deadline {
            bail!("another mount has held {} for 10 s", path.display());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `<state_dir>/mounts/<id>`: one record per FUSE view the daemon has
/// mounted (`record_mount`; removed again by `forget_mount` when the
/// session ends), so that whoever takes the state dir over from a daemon
/// the kernel has killed can find that daemon's mounts and abort their
/// connections (`abort_stale_mounts`).
pub const MOUNTS_DIR: &str = "mounts";

/// Note that this daemon mounted `fs_name` at `mountpoint`.
pub fn record_mount(state_dir: &Path, id: u64, mountpoint: &Path, fs_name: &str) -> Result<()> {
    let dir = state_dir.join(MOUNTS_DIR);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = dir.join(id.to_string());
    std::fs::write(&path, format!("{}\n{fs_name}\n", mountpoint.display()))
        .with_context(|| format!("writing {}", path.display()))
}

/// The view `id` is unmounted: drop its record.
pub fn forget_mount(state_dir: &Path, id: u64) {
    let _ = std::fs::remove_file(state_dir.join(MOUNTS_DIR).join(id.to_string()));
}

/// [`forget_mount`], keeping the record for [`restore_mount`] should the
/// step it was dropped for fail.
pub fn take_mount(state_dir: &Path, id: u64) -> Option<String> {
    let path = state_dir.join(MOUNTS_DIR).join(id.to_string());
    let record = std::fs::read_to_string(&path).ok()?;
    let _ = std::fs::remove_file(&path);
    Some(record)
}

/// Put back a record [`take_mount`] dropped.
pub fn restore_mount(state_dir: &Path, id: u64, record: &str) {
    let _ = std::fs::write(state_dir.join(MOUNTS_DIR).join(id.to_string()), record);
}

/// A mount a previous daemon of this state dir left behind, and what
/// became of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleMount {
    pub mountpoint: PathBuf,
    pub fs_name: String,
    /// The FUSE connection number (`/sys/fs/fuse/connections/<n>`) of
    /// the mount still at `mountpoint`, if one is; `None` when nothing
    /// of that name is mounted there any more.
    pub connection: Option<u32>,
    /// That connection's `waiting` count before the abort: requests
    /// nobody will ever answer.
    pub waiting: Option<u64>,
    /// `Ok` once the connection was aborted, the error otherwise.
    pub aborted: Option<std::result::Result<(), String>>,
}

impl std::fmt::Display for StaleMount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.mountpoint.display(), self.fs_name)?;
        match (&self.connection, &self.aborted) {
            (None, _) => write!(f, ": not mounted any more"),
            (Some(n), Some(Ok(()))) => write!(
                f,
                ": FUSE connection {n} aborted ({} request(s) were waiting for the dead daemon)",
                self.waiting.unwrap_or(0)
            ),
            (Some(n), Some(Err(e))) => write!(f, ": aborting FUSE connection {n} failed: {e}"),
            (Some(n), None) => write!(f, ": FUSE connection {n}"),
        }
    }
}

/// Abort the FUSE connections of the mounts recorded in `state_dir` by a
/// previous daemon, and drop the records. Only for a caller that holds
/// `daemon.lock`: the records then belong to a daemon that is gone, and
/// its mounts serve nothing any more — at best they answer `ENOTCONN`,
/// at worst (the daemon `kill -9`ed while a thread was inside a
/// `FUSE_NOTIFY_INVAL_ENTRY` write, EC2 campaign 6 B-1) they hang every
/// process that touches them, keep the dead daemon's last thread wedged
/// in the kernel forever with the daemon's whole file table, and block
/// `umount` too. The abort ends the requests the dead daemon can never
/// answer, which releases the wedged thread, which lets the zombie go.
pub fn abort_stale_mounts(state_dir: &Path) -> Vec<StaleMount> {
    let dir = state_dir.join(MOUNTS_DIR);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let table = &native().mounts;
    let mounts = table.list().unwrap_or_default();
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let mut lines = text.lines();
        let (Some(mountpoint), Some(fs_name)) = (lines.next(), lines.next()) else {
            let _ = std::fs::remove_file(&path);
            continue;
        };
        let mut stale = StaleMount {
            mountpoint: PathBuf::from(mountpoint),
            fs_name: fs_name.to_string(),
            connection: None,
            waiting: None,
            aborted: None,
        };
        if let Some(connection) = mounts
            .iter()
            .find(|m| m.fstype == "fuse" && m.source == fs_name && m.mountpoint == stale.mountpoint)
            .and_then(|m| m.fuse_connection())
        {
            stale.connection = Some(connection);
            stale.waiting = table.fuse_waiting(connection).ok();
            // The fusectl files belong to the user who mounted, so the
            // daemon's own user may abort.
            stale.aborted = Some(table.abort_fuse(connection).map_err(|e| e.to_string()));
        }
        let _ = std::fs::remove_file(&path);
        out.push(stale);
    }
    out
}

/// The reaper's log in the state dir: one line per mount it aborted.
pub const REAPER_LOG: &str = "reaper.log";

/// How long the parent must have been a zombie the kernel has killed
/// before the reaper acts (a clean exit is a zombie for milliseconds,
/// and releases the lock, which ends the reaper first anyway).
const REAPER_GRACE: Duration = Duration::from_secs(2);

const REAPER_POLL: Duration = Duration::from_millis(500);

/// Consecutive polls a live parent must look unlocked before the reaper
/// leaves (see `reaper_main`).
const REAPER_RELEASED_POLLS: u32 = 3;

/// Start the zombie reaper for this daemon: `constellation zombie-reaper`
/// as a separate process with no descriptor of ours (stdio to null, all
/// else close-on-exec), so it survives our `kill -9` and shares no fate
/// with our file table. A thread waits for it so it never lingers as a
/// zombie of ours; when we die first it is reparented and carries on.
///
/// Why: a daemon `kill -9`ed while a thread is inside a
/// `FUSE_NOTIFY_INVAL_ENTRY` write stays a zombie for good (EC2 campaign
/// 6 B-1; `kernel_inval`'s module doc has the cycle), holding
/// `daemon.lock`, the control socket and a mount every process hangs on.
/// Nothing inside the dead process can act; the next `mount` takes over
/// (`abort_stale_mounts`), but that may be minutes or hours away. The
/// reaper does the same abort within seconds of the kill.
pub fn spawn_reaper(state_dir: &Path) -> Result<u32> {
    let exe = std::env::current_exe().context("locating the constellation binary")?;
    let mut child = std::process::Command::new(exe)
        .arg("zombie-reaper")
        .arg("--parent")
        .arg(std::process::id().to_string())
        .arg("--state-dir")
        .arg(state_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawning the zombie reaper")?;
    let pid = child.id();
    std::thread::Builder::new()
        .name("reaper-wait".into())
        .spawn(move || {
            let _ = child.wait();
        })
        .context("spawning the reaper's waiter thread")?;
    Ok(pid)
}

/// `constellation zombie-reaper --parent <pid> --state-dir <dir>`: watch
/// `parent` until it releases `daemon.lock` (its exit, clean or not, or
/// its takeover by another daemon) — or until the kernel says it can
/// never run again while it still holds the lock (`classify`, the
/// takeover's own verdict: a zombie leader with a thread stuck in the
/// kernel). Then abort its recorded mounts' FUSE connections, which ends
/// the requests its wedged thread waits behind, lets the zombie go, and
/// with it the lock and the socket. Every step is logged to
/// `<state_dir>/reaper.log`.
pub fn reaper_main(parent: u32, state_dir: &Path) -> Result<()> {
    let lock = state_dir.join(LOCK_NAME);
    let mut wedged_since: Option<Instant> = None;
    let mut released_polls = 0u32;
    loop {
        std::thread::sleep(REAPER_POLL);
        if !lock_holder_pids(&lock).contains(&parent) {
            // Released (a clean exit, a crash whose file table closed, a
            // takeover), or `/proc/locks` unreadable: nothing to reap.
            // Leaving is for good, so a live parent must look released on
            // several polls running: `/proc/locks` is a seq_file read a
            // page per call, each call resuming at an index into a list
            // other processes keep changing, so one read can skip our
            // line on a busy host.
            released_polls += 1;
            if parent_gone(parent) {
                // Gone however it died: what it left mounted goes too.
                reap_left_mounts(state_dir, parent);
                return Ok(());
            }
            if released_polls >= REAPER_RELEASED_POLLS {
                return Ok(());
            }
            continue;
        }
        released_polls = 0;
        let holder = classify_pid(parent);
        let Holder::Wedged { why, .. } = holder else {
            wedged_since = None;
            if matches!(holder, Holder::Unknown { .. }) && parent_gone(parent) {
                reap_left_mounts(state_dir, parent);
                return Ok(());
            }
            continue;
        };
        let since = *wedged_since.get_or_insert_with(Instant::now);
        if since.elapsed() < REAPER_GRACE {
            continue;
        }
        // `/proc/locks` names the parent under daemon.lock's key, but the
        // key is shared across btrfs subvolumes: once the state dir is
        // taken over (the parent's lock moved aside as
        // `daemon.lock.wedged-<pid>`), a parent seen there holds another
        // file, and the mounts recorded now are the new daemon's. Act
        // only on a parent confirmed to hold this very `daemon.lock`.
        if confirmed_holder(state_dir, &[parent]) != Some(parent) {
            continue;
        }
        let stale = abort_stale_mounts(state_dir);
        let mut log = String::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if stale.is_empty() {
            log.push_str(&format!(
                "{now} daemon {parent} is dead but wedged in the kernel ({why}); it recorded no \
                 mount to abort\n"
            ));
        }
        for s in &stale {
            log.push_str(&format!(
                "{now} daemon {parent} is dead but wedged in the kernel ({why}); {s}\n"
            ));
        }
        append_reaper_log(state_dir, &log);
        // Give the zombie a moment to go and release the lock. Its
        // aborted mounts are then dropped too, under `daemon.lock` (so no
        // new daemon of this state dir mounts meanwhile), and only where
        // the aborted connection is still the one mounted there.
        let t = Instant::now();
        let mut guard = None;
        while t.elapsed() < Duration::from_secs(10) {
            if parent_gone(parent) {
                guard = try_take_lock(state_dir);
                if guard.is_some() {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let gone = parent_gone(parent);
        let unmounted = if guard.is_some() {
            stale.iter().filter_map(lazy_unmount_connection).count()
        } else {
            0
        };
        drop(guard);
        append_reaper_log(
            state_dir,
            &format!(
                "{now} daemon {parent} {} {:?} after the abort; {unmounted} aborted mount(s) \
                 lazily unmounted\n",
                if gone { "exited" } else { "is still there" },
                t.elapsed()
            ),
        );
        return Ok(());
    }
}

/// Whether `pid` is gone for good: no longer in the process table, or
/// an exited (zombie or dead) leader its parent has not reaped yet. Only
/// asked once `daemon.lock` looks released, or when the holder cannot be
/// classified: a zombie leader whose threads still run keeps the lock (the
/// file table is shared), and `try_take_lock` is the final word anyway.
fn parent_gone(pid: u32) -> bool {
    match native().process.facts(pid) {
        Ok(facts) => facts.zombie || facts.dead,
        Err(_) => !native().process.is_alive(pid),
    }
}

/// `daemon.lock`, taken without waiting, for the reaper's own work on a
/// state dir its daemon has left: while the reaper holds it no daemon of
/// this state dir can start (a `mount` meanwhile sees a live holder that
/// is not a daemon and retries within its attach timeout), so the records
/// it reaps are the dead daemon's. `None` when anyone holds it (a new
/// daemon: the records are its own), or when there is no lock file (never
/// created here: it would only appear between a takeover's rename and the
/// new daemon's lock).
fn try_take_lock(state_dir: &Path) -> Option<constellation_platform::LockGuard> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(state_dir.join(LOCK_NAME))
        .ok()?;
    native().file_lock.try_lock(file).ok().flatten()
}

/// Lazily unmount `s`'s mountpoint if the mount on top there is still the
/// FUSE connection that was aborted (a dead FUSE mount answers `ENOTCONN`
/// for good until someone drops it); whatever was mounted there since is
/// left alone. `None` when nothing was aborted or it is no longer there.
fn lazy_unmount_connection(s: &StaleMount) -> Option<Result<(), String>> {
    let connection = s.connection?;
    let table = &native().mounts;
    // Mountinfo lists a stacked mount after the one it covers.
    let top = table
        .list()
        .ok()?
        .into_iter()
        .rfind(|m| m.mountpoint == s.mountpoint)?;
    (top.fuse_connection() == Some(connection)).then(|| {
        table
            .unmount(&s.mountpoint, UnmountMode::Lazy)
            .map_err(|e| e.to_string())
    })
}

/// The daemon `parent` is gone: abort and lazily unmount every mount it
/// recorded and never forgot (`forget_mount` is its clean-exit path, so a
/// record still there means the daemon died with the mount up: `kill -9`,
/// its harness dying, anything). Done holding `daemon.lock`
/// (`try_take_lock`), and skipped when that cannot be had: another daemon
/// holds it and the records are its own. Logged to `reaper.log`.
fn reap_left_mounts(state_dir: &Path, parent: u32) {
    let Some(_guard) = try_take_lock(state_dir) else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut log = String::new();
    for s in abort_stale_mounts(state_dir) {
        let unmounted = lazy_unmount_connection(&s);
        log.push_str(&format!(
            "{now} daemon {parent} is gone; {s}; lazy unmount: {}\n",
            match unmounted {
                None => "nothing to unmount".to_string(),
                Some(Ok(())) => "done".to_string(),
                Some(Err(e)) => format!("failed ({e})"),
            }
        ));
    }
    append_reaper_log(state_dir, &log);
}

fn append_reaper_log(state_dir: &Path, text: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(state_dir.join(REAPER_LOG))
    {
        let _ = f.write_all(text.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_of_a_mount_that_is_gone_is_reported_and_dropped_and_nothing_is_aborted() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        record_mount(
            state,
            7,
            Path::new("/nonexistent/mountpoint"),
            "constellation-test",
        )
        .unwrap();
        record_mount(state, 8, Path::new("/another"), "constellation-test").unwrap();
        forget_mount(state, 8);
        std::fs::write(state.join(MOUNTS_DIR).join("9"), "").unwrap();
        let got = abort_stale_mounts(state);
        assert_eq!(
            got,
            vec![StaleMount {
                mountpoint: PathBuf::from("/nonexistent/mountpoint"),
                fs_name: "constellation-test".into(),
                connection: None,
                waiting: None,
                aborted: None,
            }]
        );
        assert_eq!(
            got[0].to_string(),
            "/nonexistent/mountpoint (constellation-test): not mounted any more"
        );
        assert!(std::fs::read_dir(state.join(MOUNTS_DIR))
            .unwrap()
            .next()
            .is_none());
        assert!(abort_stale_mounts(state).is_empty());
    }

    /// The Linux `/proc` text through the platform's parser, then the
    /// decision: the path `classify_pid` takes on Linux.
    #[cfg(target_os = "linux")]
    fn classify_text(pid: u32, status: &str, tasks: &[String]) -> Holder {
        classify(
            pid,
            &constellation_platform::linux::parse_process_facts(status, tasks),
        )
    }

    /// Node b's zombie daemon, verbatim from the campaign 6 evidence
    /// (`/proc/161984/status`): leader zombie, `Threads: 2`, SIGKILL in
    /// `ShdPnd`.
    #[cfg(target_os = "linux")]
    const CAMPAIGN6_LEADER: &str = "Name:\tconstellation-2\nState:\tZ (zombie)\nTgid:\t161984\n\
        Ngid:\t0\nPid:\t161984\nPPid:\t1\nTracerPid:\t0\nUid:\t1000\t1000\t1000\t1000\n\
        FDSize:\t0\nThreads:\t2\nSigQ:\t0/74607\nSigPnd:\t0000000000000000\n\
        ShdPnd:\t0000000000000100\nSigBlk:\t0000000000000000\nSigIgn:\t0000000000001000\n\
        SigCgt:\t0000000100004442\nvoluntary_ctxt_switches:\t764\n";

    #[cfg(target_os = "linux")]
    fn live(state: &str, vm: bool) -> String {
        format!(
            "Name:\tconstellation-2\nState:\t{state}\nTgid:\t4242\nPid:\t4242\n{}Threads:\t9\n\
             SigPnd:\t0000000000000000\nShdPnd:\t0000000000000000\n",
            if vm {
                "VmSize:\t  593048 kB\nVmRSS:\t    6616 kB\n"
            } else {
                ""
            }
        )
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn campaign6_zombie_is_wedged() {
        let stuck = "Name:\tconstellation-2\nState:\tD (disk sleep)\nTgid:\t161984\nPid:\t161990\n\
                     SigPnd:\t0000000000000100\nShdPnd:\t0000000000000100\n"
            .to_string();
        match classify_text(161984, CAMPAIGN6_LEADER, &[stuck]) {
            Holder::Wedged { pid, name, why } => {
                assert_eq!(pid, 161984);
                assert_eq!(name, "constellation-2");
                assert!(why.contains("SIGKILL pending"), "{why}");
                assert!(why.contains("D (disk sleep)"), "{why}");
            }
            other => panic!("expected wedged, got {other:?}"),
        }
        // The leader alone (task listing unavailable) still decides it.
        assert!(matches!(
            classify_text(161984, CAMPAIGN6_LEADER, &[]),
            Holder::Wedged { .. }
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn zombie_leader_whose_threads_left_user_space_is_wedged() {
        let leader =
            CAMPAIGN6_LEADER.replace("ShdPnd:\t0000000000000100", "ShdPnd:\t0000000000000000");
        let exiting =
            "Name:\tconstellation-2\nState:\tD (disk sleep)\nTgid:\t161984\nPid:\t161990\n\
                       SigPnd:\t0000000000000000\nShdPnd:\t0000000000000000\n"
                .to_string();
        assert!(matches!(
            classify_text(161984, &leader, &[exiting]),
            Holder::Wedged { why, .. } if why.contains("released their address space")
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn zombie_leader_with_a_running_thread_is_live() {
        // A main thread that exited on its own (`pthread_exit`) while a
        // worker keeps running: no SIGKILL, the worker has its mm.
        let leader =
            CAMPAIGN6_LEADER.replace("ShdPnd:\t0000000000000100", "ShdPnd:\t0000000000000000");
        let worker = live("S (sleeping)", true);
        assert!(matches!(
            classify_text(161984, &leader, &[worker]),
            Holder::Live { .. }
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn running_sleeping_stopped_and_disk_sleep_are_live() {
        for state in [
            "R (running)",
            "S (sleeping)",
            "T (stopped)",
            "D (disk sleep)",
        ] {
            let status = live(state, true);
            match classify_text(4242, &status, &[live("S (sleeping)", true)]) {
                Holder::Live {
                    pid, state: got, ..
                } => {
                    assert_eq!(pid, 4242);
                    assert_eq!(got, state);
                }
                other => panic!("{state}: expected live, got {other:?}"),
            }
        }
    }

    /// `takeover_rotates_the_held_lock` sets the process-wide
    /// `CONSTELLATION_FAULT_ASSUME_WEDGED_PID` to this very process's pid
    /// while it runs; a test that classifies this process's own lock in
    /// parallel would see it as wedged (flaky under full-suite load). The
    /// two hold this while they run.
    static WEDGED_HOOK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn wedged_hook_guard() -> std::sync::MutexGuard<'static, ()> {
        WEDGED_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The real thing on this host: a lock this process holds is
    /// attributed to this process, and this process is live.
    #[cfg(target_os = "linux")]
    #[test]
    fn own_flock_is_found_and_live() {
        let _hook = wedged_hook_guard();
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join(LOCK_NAME);
        let file = native()
            .file_lock
            .lock(std::fs::File::create(&lock).unwrap())
            .unwrap();
        assert!(lock_holder_pids(&lock).contains(&std::process::id()));
        match holder(dir.path()) {
            Holder::Live { pid, .. } => assert_eq!(pid, std::process::id()),
            other => panic!("expected live, got {other:?}"),
        }
        // Not wedged, so a takeover refuses.
        let err = take_over(dir.path(), std::process::id()).unwrap_err();
        assert!(err.to_string().contains("changed hands"), "{err}");
        assert!(lock.exists());
        drop(file);
        assert!(!lock_holder_pids(&lock).contains(&std::process::id()));
    }

    /// The reaper stays while its parent holds the lock and leaves once
    /// it is released — also where `stat`'s device is not the one
    /// `/proc/locks` names (a btrfs subvolume: the build tree of a typical
    /// dev host), where it once read the held lock as released and left
    /// within 1.5 s of its start, so no wedged daemon was ever reaped.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_reaper_stays_while_its_parent_holds_the_lock() {
        let _hook = wedged_hook_guard();
        let dir = build_tree_tmp();
        let lock = dir.path().join(LOCK_NAME);
        let file = native()
            .file_lock
            .lock(std::fs::File::create(&lock).unwrap())
            .unwrap();
        let state = dir.path().to_path_buf();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(reaper_main(std::process::id(), &state).is_ok());
        });
        assert!(
            rx.recv_timeout(REAPER_POLL * (REAPER_RELEASED_POLLS + 3))
                .is_err(),
            "the reaper left while its parent held the lock"
        );
        drop(file);
        assert_eq!(rx.recv_timeout(Duration::from_secs(10)), Ok(true));
        assert!(!dir.path().join(REAPER_LOG).exists(), "nothing was reaped");
    }

    /// A directory under the build's target dir (`target/<profile>/
    /// test-tmp`), for tests that need the build tree's filesystem (a
    /// btrfs subvolume on a typical dev host; `/tmp` is usually a tmpfs).
    fn build_tree_tmp() -> tempfile::TempDir {
        let exe = std::env::current_exe().unwrap();
        // `target/<profile>/deps/<test binary>`
        let dir = exe.parent().unwrap().parent().unwrap().join("test-tmp");
        std::fs::create_dir_all(&dir).unwrap();
        tempfile::tempdir_in(dir).unwrap()
    }

    /// Two btrfs subvolumes, `a` and `b`, under a fresh build-tree temp
    /// dir, each with a `daemon.lock` of the same inode number (every
    /// subvolume numbers its inodes from 257), so `/proc/locks` names the
    /// two alike. `None` where that cannot be made here (not btrfs, no
    /// `btrfs` tool, numbers that differ).
    struct Subvolumes {
        dir: tempfile::TempDir,
    }

    impl Subvolumes {
        fn make() -> Option<Subvolumes> {
            use std::os::unix::fs::MetadataExt;
            let dir = build_tree_tmp();
            for sv in ["a", "b"] {
                let made = std::process::Command::new("btrfs")
                    .args(["subvolume", "create"])
                    .arg(dir.path().join(sv))
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
                if !matches!(made, Ok(s) if s.success()) {
                    eprintln!("skipped: `btrfs subvolume create` is unavailable here");
                    return None;
                }
            }
            let subvolumes = Subvolumes { dir };
            let ino = |sv: &str| {
                std::fs::File::create(subvolumes.state(sv).join(LOCK_NAME))
                    .and_then(|f| f.metadata())
                    .map(|m| m.ino())
                    .ok()
            };
            match (ino("a"), ino("b")) {
                (Some(a), Some(b)) if a == b => Some(subvolumes),
                other => {
                    eprintln!("skipped: the two lock files' inodes differ: {other:?}");
                    None
                }
            }
        }

        fn state(&self, sv: &str) -> PathBuf {
            self.dir.path().join(sv)
        }
    }

    impl Drop for Subvolumes {
        fn drop(&mut self) {
            // An empty subvolume is removed by `rmdir` (Linux 4.18+); a
            // `btrfs subvolume delete` would need root.
            for sv in ["a", "b"] {
                let _ = std::fs::remove_dir_all(self.state(sv));
            }
        }
    }

    /// Must-fix of the fuse-inval-hang review: two state dirs in two
    /// subvolumes of one btrfs, whose `daemon.lock`s share an inode number,
    /// look alike in `/proc/locks`. Another process holds `b`'s lock; `a`'s
    /// holder must not be taken for it — not classified (it is no holder
    /// of `a`'s), not taken over (even when it would be wedged), and a
    /// reaper watching `a`'s daemon must not act on it.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_lock_on_another_subvolume_is_not_taken_for_ours() {
        let _hook = wedged_hook_guard();
        let Some(subvolumes) = Subvolumes::make() else {
            return;
        };
        let (a, b) = (subvolumes.state("a"), subvolumes.state("b"));
        // `-o`: only flock(1) itself holds the lock (its `sleep` does not
        // inherit the fd); its own process group, so both go at the end.
        let mut other = {
            use std::os::unix::process::CommandExt;
            std::process::Command::new("flock")
                .arg("-o")
                .arg(b.join(LOCK_NAME))
                .args(["sleep", "60"])
                .process_group(0)
                .spawn()
                .expect("running flock(1)")
        };
        let other_pid = other.id();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !lock_holder_pids(&b.join(LOCK_NAME)).contains(&other_pid) {
            assert!(Instant::now() < deadline, "flock(1) never took b's lock");
            std::thread::sleep(Duration::from_millis(20));
        }
        let outcome = std::panic::catch_unwind(|| {
            // The ambiguity itself: `a`'s unheld lock reads as held by
            // `b`'s holder, which has only `b`'s open.
            assert!(lock_holder_pids(&a.join(LOCK_NAME)).contains(&other_pid));
            assert!(!native().file_lock.opened_by(other_pid, &a.join(LOCK_NAME)));
            assert!(native().file_lock.opened_by(other_pid, &b.join(LOCK_NAME)));
            assert!(
                matches!(holder(&a), Holder::Unknown { .. }),
                "{:?}",
                holder(&a)
            );
            // b's holder is confirmed by its open file (no daemon.pid).
            assert!(
                matches!(holder(&b), Holder::Live { pid, .. } if pid == other_pid),
                "{:?}",
                holder(&b)
            );
            // Even assumed wedged, it is no holder of `a`'s to take over.
            std::env::set_var(
                "CONSTELLATION_FAULT_ASSUME_WEDGED_PID",
                other_pid.to_string(),
            );
            let taken = take_over(&a, other_pid);
            std::env::remove_var("CONSTELLATION_FAULT_ASSUME_WEDGED_PID");
            assert!(taken.is_err(), "took a over from b's holder");
            assert!(a.join(LOCK_NAME).exists());
            // A `daemon.pid` naming it is what confirms a holder.
            std::fs::write(a.join("daemon.pid"), other_pid.to_string()).unwrap();
            assert!(
                matches!(holder(&a), Holder::Live { pid, .. } if pid == other_pid),
                "{:?}",
                holder(&a)
            );
            std::fs::remove_file(a.join("daemon.pid")).unwrap();
            // Our own lock on `a` beside it: the reaper finds its parent
            // among the pids and stays until the lock is released.
            let file = native()
                .file_lock
                .lock(std::fs::File::create(a.join(LOCK_NAME)).unwrap())
                .unwrap();
            assert!(matches!(holder(&a), Holder::Live { pid, .. } if pid == std::process::id()));
            let (tx, rx) = std::sync::mpsc::channel();
            let state = a.clone();
            std::thread::spawn(move || {
                let _ = tx.send(reaper_main(std::process::id(), &state).is_ok());
            });
            assert!(
                rx.recv_timeout(REAPER_POLL * (REAPER_RELEASED_POLLS + 3))
                    .is_err(),
                "the reaper left while its parent held the lock"
            );
            drop(file);
            assert_eq!(rx.recv_timeout(Duration::from_secs(10)), Ok(true));
            assert!(!a.join(REAPER_LOG).exists(), "nothing was reaped");
        });
        // SAFETY: plain syscall on the process group of our own child.
        unsafe { libc::kill(-(other_pid as libc::pid_t), libc::SIGKILL) };
        let _ = other.wait();
        drop(subvolumes);
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    /// With the holder assumed wedged (the test hook), the takeover moves
    /// the held inode aside and clears the stale socket and pid file;
    /// a fresh lock is then free to take even though the old one is
    /// still held.
    #[cfg(target_os = "linux")]
    #[test]
    fn takeover_rotates_the_held_lock() {
        let _hook = wedged_hook_guard();
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join(LOCK_NAME);
        let file = native()
            .file_lock
            .lock(std::fs::File::create(&lock).unwrap())
            .unwrap();
        std::fs::write(
            dir.path().join("daemon.pid"),
            std::process::id().to_string(),
        )
        .unwrap();
        // The wedged daemon's socket, where its locator says it is.
        let sock = dir.path().join("wedged.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        constellation_control::transport::record_socket(dir.path(), &sock).unwrap();
        let pid = std::process::id();
        std::env::set_var("CONSTELLATION_FAULT_ASSUME_WEDGED_PID", pid.to_string());
        let outcome = take_over(dir.path(), pid);
        std::env::remove_var("CONSTELLATION_FAULT_ASSUME_WEDGED_PID");
        outcome.unwrap();
        assert!(!lock.exists());
        assert!(dir
            .path()
            .join(format!("daemon.lock.wedged-{pid}"))
            .exists());
        assert!(!sock.exists());
        assert!(constellation_control::transport::locate_socket(dir.path()).is_none());
        assert!(!dir.path().join("daemon.pid").exists());
        // The fresh lock is free while the old inode stays held.
        let fresh = std::fs::File::create(&lock).unwrap();
        assert!(native().file_lock.try_lock(fresh).unwrap().is_some());
        drop(file);
    }
}
