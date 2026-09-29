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
//! classified from `/proc`. Only a holder the kernel itself says can
//! never run again — thread-group leader a zombie *and* (SIGKILL pending
//! or every remaining thread already past `exit_mm`) — is taken over:
//! its `daemon.lock` inode is unlinked (the zombie keeps its lock on the
//! orphan inode), the stale `control.sock`/`daemon.pid` are removed, and
//! the caller takes a fresh lock. Anything alive (running, sleeping,
//! stopped by SIGSTOP, or a zombie leader whose other threads could still
//! run) is never taken over: the mount fails within a bound, naming the
//! pid and its state. Safety never depends on a liveness guess: the only
//! takeover is of a process the kernel has already killed.

use anyhow::{bail, Context, Result};
use constellation_types::Code;
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
    let pid = match lock_holder_pid(&lock) {
        Some(pid) => pid,
        None => match pid_file(state_dir) {
            Some(pid) => pid,
            None => {
                return Holder::Unknown {
                    detail: "no flock on daemon.lock in /proc/locks and no daemon.pid".into(),
                }
            }
        },
    };
    classify_pid(pid)
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

/// The pid `/proc/locks` names for the `FLOCK` on `lock`'s inode.
#[cfg(target_os = "linux")]
fn lock_holder_pid(lock: &Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(lock).ok()?;
    let locks = std::fs::read_to_string("/proc/locks").ok()?;
    let dev = meta.dev();
    let (major, minor) = (libc::major(dev), libc::minor(dev));
    let want = format!("{major:02x}:{minor:02x}:{}", meta.ino());
    flock_pid_in(&locks, &want)
}

#[cfg(not(target_os = "linux"))]
fn lock_holder_pid(_lock: &Path) -> Option<u32> {
    None
}

/// Find the pid of the `FLOCK` line on `inode` (`maj:min:ino`, as
/// `/proc/locks` prints it) in `locks`.
fn flock_pid_in(locks: &str, inode: &str) -> Option<u32> {
    for line in locks.lines() {
        // `1: FLOCK  ADVISORY  WRITE 161984 103:01:2098107 0 EOF`
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 6 && fields[1] == "FLOCK" && fields[5] == inode {
            return fields[4].parse().ok();
        }
    }
    None
}

/// Classify `pid` from `/proc/<pid>/status` and its threads.
fn classify_pid(pid: u32) -> Holder {
    let status = match std::fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(s) => s,
        Err(e) => {
            return Holder::Unknown {
                detail: format!("/proc/{pid}/status: {e}"),
            }
        }
    };
    let mut tasks = Vec::new();
    if let Ok(dir) = std::fs::read_dir(format!("/proc/{pid}/task")) {
        for entry in dir.flatten() {
            if entry.file_name().to_string_lossy() == pid.to_string() {
                continue;
            }
            if let Ok(s) = std::fs::read_to_string(entry.path().join("status")) {
                tasks.push(s);
            }
        }
    }
    classify(pid, &status, &tasks)
}

fn field<'a>(status: &'a str, key: &str) -> Option<&'a str> {
    status
        .lines()
        .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix(':')))
        .map(str::trim)
}

fn sigkill_pending(status: &str) -> bool {
    // Signal 9 is bit 8 of the pending masks.
    ["SigPnd", "ShdPnd"].iter().any(|key| {
        field(status, key)
            .and_then(|hex| u64::from_str_radix(hex, 16).ok())
            .is_some_and(|mask| mask & (1 << 8) != 0)
    })
}

/// The decision, from the leader's `/proc/<pid>/status` and the other
/// threads' `task/<tid>/status`. Pure, so the shapes seen in the field
/// can be tested verbatim.
pub fn classify(pid: u32, status: &str, tasks: &[String]) -> Holder {
    let name = field(status, "Name").unwrap_or("?").to_string();
    let state = field(status, "State").unwrap_or("?").to_string();
    let leader_zombie = state.starts_with('Z');
    if !leader_zombie {
        // A process that is not exiting is alive, however unresponsive
        // (`T`: stopped by SIGSTOP; `D`: momentarily in the kernel).
        // `X` (dead) never shows for long; treat it as gone.
        if state.starts_with('X') {
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
    let killed = sigkill_pending(status) || tasks.iter().any(|t| sigkill_pending(t));
    let no_mm = tasks.iter().all(|t| field(t, "VmSize").is_none());
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

fn task_states(tasks: &[String]) -> String {
    let states: Vec<&str> = tasks
        .iter()
        .map(|t| field(t, "State").unwrap_or("?"))
        .collect();
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
/// `daemon.lock` inode and the stale `control.sock`/`daemon.pid`. The
/// caller then takes a fresh `daemon.lock` as usual.
pub fn take_over(state_dir: &Path, pid: u32) -> Result<()> {
    let _mutex = takeover_mutex(state_dir)?;
    match holder_for_takeover(state_dir) {
        Holder::Wedged { pid: now, .. } if now == pid => {}
        Holder::Unknown { .. } if lock_holder_pid(&state_dir.join(LOCK_NAME)).is_none() => {
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
    let _ = std::fs::remove_file(state_dir.join(constellation_api::SOCKET_NAME));
    let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
    tracing::warn!(
        pid,
        state_dir = %state_dir.display(),
        "took the state dir over from a daemon the kernel has killed but that still held \
         daemon.lock and control.sock; its lock file is kept as daemon.lock.wedged-{pid}"
    );
    Ok(())
}

/// The takeover mutex, held for the returned file's lifetime. Bounded:
/// a mutex holder is a `mount` mid-takeover (milliseconds).
fn takeover_mutex(state_dir: &Path) -> Result<std::fs::File> {
    let path: PathBuf = state_dir.join(TAKEOVER_MUTEX);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    use std::os::fd::AsRawFd;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // SAFETY: `file` owns a valid open fd for the duration of the call.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            return Ok(file);
        }
        let err = std::io::Error::last_os_error();
        if Code::from_io_error(&err) != Code::Again {
            return Err(err).context("locking the takeover mutex");
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
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mounts = parse_mountinfo(&mountinfo);
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
        if let Some(m) = mounts
            .iter()
            .find(|m| m.fstype == "fuse" && m.source == fs_name && m.mountpoint == stale.mountpoint)
        {
            stale.connection = Some(m.minor);
            stale.waiting = connection_waiting(m.minor);
            stale.aborted = Some(abort_connection(m.minor).map_err(|e| e.to_string()));
        }
        let _ = std::fs::remove_file(&path);
        out.push(stale);
    }
    out
}

/// One line of `/proc/self/mountinfo`, as far as this module needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MountInfo {
    /// The minor of `major:minor` (the FUSE connection number: a FUSE
    /// superblock's `s_dev` is an anonymous device, major 0).
    minor: u32,
    mountpoint: PathBuf,
    fstype: String,
    source: String,
}

/// `\040`-style escapes in a mountinfo path.
fn unescape_mountinfo(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 4 <= bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 4], 8) {
                out.push(v);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_mountinfo(text: &str) -> Vec<MountInfo> {
    let mut out = Vec::new();
    for line in text.lines() {
        // `36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue`
        let Some((pre, post)) = line.split_once(" - ") else {
            continue;
        };
        let pre: Vec<&str> = pre.split(' ').collect();
        let post: Vec<&str> = post.split(' ').collect();
        if pre.len() < 5 || post.len() < 2 {
            continue;
        }
        let Some(minor) = pre[2]
            .split_once(':')
            .and_then(|(_, m)| m.parse::<u32>().ok())
        else {
            continue;
        };
        out.push(MountInfo {
            minor,
            mountpoint: PathBuf::from(unescape_mountinfo(pre[4])),
            fstype: post[0].to_string(),
            source: unescape_mountinfo(post[1]),
        });
    }
    out
}

fn fusectl(minor: u32, file: &str) -> PathBuf {
    PathBuf::from(format!("/sys/fs/fuse/connections/{minor}/{file}"))
}

/// The connection's `waiting` count (requests not answered yet).
fn connection_waiting(minor: u32) -> Option<u64> {
    std::fs::read_to_string(fusectl(minor, "waiting"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// `echo 1 > /sys/fs/fuse/connections/<minor>/abort`: the fusectl files
/// belong to the user who mounted, so the daemon's own user may.
fn abort_connection(minor: u32) -> std::io::Result<()> {
    std::fs::write(fusectl(minor, "abort"), "1\n")
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
        if lock_holder_pid(&lock) != Some(parent) {
            // Released (a clean exit, a crash whose file table closed, a
            // takeover), or `/proc/locks` unreadable: nothing to reap.
            // Leaving is for good, so a live parent must look released on
            // several polls running: `/proc/locks` is a seq_file read a
            // page per call, each call resuming at an index into a list
            // other processes keep changing, so one read can skip our
            // line on a busy host.
            released_polls += 1;
            if released_polls >= REAPER_RELEASED_POLLS
                || !Path::new(&format!("/proc/{parent}")).exists()
            {
                return Ok(());
            }
            continue;
        }
        released_polls = 0;
        let holder = classify_pid(parent);
        let Holder::Wedged { why, .. } = holder else {
            wedged_since = None;
            if matches!(holder, Holder::Unknown { .. })
                && !Path::new(&format!("/proc/{parent}")).exists()
            {
                return Ok(());
            }
            continue;
        };
        let since = *wedged_since.get_or_insert_with(Instant::now);
        if since.elapsed() < REAPER_GRACE {
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
        // Give the zombie a moment to go, for the log's sake only.
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(10)
            && Path::new(&format!("/proc/{parent}")).exists()
        {
            std::thread::sleep(Duration::from_millis(100));
        }
        let gone = !Path::new(&format!("/proc/{parent}")).exists();
        append_reaper_log(
            state_dir,
            &format!(
                "{now} daemon {parent} {} {:?} after the abort\n",
                if gone { "exited" } else { "is still there" },
                t.elapsed()
            ),
        );
        return Ok(());
    }
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
    fn mountinfo_lines_yield_the_connection_number_type_source_and_unescaped_mountpoint() {
        let text = "\
36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
1207 30 0:48 / /home/ubuntu/cbench/mnt6/c6daws rw,nosuid,nodev,relatime - fuse c6daws rw,user_id=1000,group_id=1000,default_permissions
1208 30 0:49 / /tmp/with\\040space rw - fuse my\\011fs rw
garbage line without the separator
";
        let got = parse_mountinfo(text);
        assert_eq!(
            got,
            vec![
                MountInfo {
                    minor: 0,
                    mountpoint: PathBuf::from("/mnt2"),
                    fstype: "ext3".into(),
                    source: "/dev/root".into(),
                },
                MountInfo {
                    minor: 48,
                    mountpoint: PathBuf::from("/home/ubuntu/cbench/mnt6/c6daws"),
                    fstype: "fuse".into(),
                    source: "c6daws".into(),
                },
                MountInfo {
                    minor: 49,
                    mountpoint: PathBuf::from("/tmp/with space"),
                    fstype: "fuse".into(),
                    source: "my\tfs".into(),
                },
            ]
        );
    }

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

    /// Node b's zombie daemon, verbatim from the campaign 6 evidence
    /// (`/proc/161984/status`): leader zombie, `Threads: 2`, SIGKILL in
    /// `ShdPnd`.
    const CAMPAIGN6_LEADER: &str = "Name:\tconstellation-2\nState:\tZ (zombie)\nTgid:\t161984\n\
        Ngid:\t0\nPid:\t161984\nPPid:\t1\nTracerPid:\t0\nUid:\t1000\t1000\t1000\t1000\n\
        FDSize:\t0\nThreads:\t2\nSigQ:\t0/74607\nSigPnd:\t0000000000000000\n\
        ShdPnd:\t0000000000000100\nSigBlk:\t0000000000000000\nSigIgn:\t0000000000001000\n\
        SigCgt:\t0000000100004442\nvoluntary_ctxt_switches:\t764\n";

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

    #[test]
    fn campaign6_zombie_is_wedged() {
        let stuck = "Name:\tconstellation-2\nState:\tD (disk sleep)\nTgid:\t161984\nPid:\t161990\n\
                     SigPnd:\t0000000000000100\nShdPnd:\t0000000000000100\n"
            .to_string();
        match classify(161984, CAMPAIGN6_LEADER, &[stuck]) {
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
            classify(161984, CAMPAIGN6_LEADER, &[]),
            Holder::Wedged { .. }
        ));
    }

    #[test]
    fn zombie_leader_whose_threads_left_user_space_is_wedged() {
        let leader =
            CAMPAIGN6_LEADER.replace("ShdPnd:\t0000000000000100", "ShdPnd:\t0000000000000000");
        let exiting =
            "Name:\tconstellation-2\nState:\tD (disk sleep)\nTgid:\t161984\nPid:\t161990\n\
                       SigPnd:\t0000000000000000\nShdPnd:\t0000000000000000\n"
                .to_string();
        assert!(matches!(
            classify(161984, &leader, &[exiting]),
            Holder::Wedged { why, .. } if why.contains("released their address space")
        ));
    }

    #[test]
    fn zombie_leader_with_a_running_thread_is_live() {
        // A main thread that exited on its own (`pthread_exit`) while a
        // worker keeps running: no SIGKILL, the worker has its mm.
        let leader =
            CAMPAIGN6_LEADER.replace("ShdPnd:\t0000000000000100", "ShdPnd:\t0000000000000000");
        let worker = live("S (sleeping)", true);
        assert!(matches!(
            classify(161984, &leader, &[worker]),
            Holder::Live { .. }
        ));
    }

    #[test]
    fn running_sleeping_stopped_and_disk_sleep_are_live() {
        for state in [
            "R (running)",
            "S (sleeping)",
            "T (stopped)",
            "D (disk sleep)",
        ] {
            let status = live(state, true);
            match classify(4242, &status, &[live("S (sleeping)", true)]) {
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

    #[test]
    fn proc_locks_lookup_matches_the_inode() {
        let locks = "1: POSIX  ADVISORY  WRITE 1234 fd:01:5678 0 EOF\n\
                     2: FLOCK  ADVISORY  WRITE 161984 103:01:2098107 0 EOF\n\
                     3: FLOCK  ADVISORY  WRITE 999 103:01:42 0 EOF\n";
        assert_eq!(flock_pid_in(locks, "103:01:2098107"), Some(161984));
        assert_eq!(flock_pid_in(locks, "103:01:42"), Some(999));
        assert_eq!(flock_pid_in(locks, "fd:01:5678"), None);
        assert_eq!(flock_pid_in(locks, "103:01:1"), None);
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
        use std::os::fd::AsRawFd;
        let _hook = wedged_hook_guard();
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join(LOCK_NAME);
        let file = std::fs::File::create(&lock).unwrap();
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        assert_eq!(lock_holder_pid(&lock), Some(std::process::id()));
        match holder(dir.path()) {
            Holder::Live { pid, .. } => assert_eq!(pid, std::process::id()),
            other => panic!("expected live, got {other:?}"),
        }
        // Not wedged, so a takeover refuses.
        let err = take_over(dir.path(), std::process::id()).unwrap_err();
        assert!(err.to_string().contains("changed hands"), "{err}");
        assert!(lock.exists());
        drop(file);
        assert_eq!(lock_holder_pid(&lock), None);
    }

    /// With the holder assumed wedged (the test hook), the takeover moves
    /// the held inode aside and clears the stale socket and pid file;
    /// a fresh lock is then free to take even though the old one is
    /// still held.
    #[cfg(target_os = "linux")]
    #[test]
    fn takeover_rotates_the_held_lock() {
        use std::os::fd::AsRawFd;
        let _hook = wedged_hook_guard();
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join(LOCK_NAME);
        let file = std::fs::File::create(&lock).unwrap();
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        std::fs::write(
            dir.path().join("daemon.pid"),
            std::process::id().to_string(),
        )
        .unwrap();
        let _listener =
            std::os::unix::net::UnixListener::bind(dir.path().join(constellation_api::SOCKET_NAME))
                .unwrap();
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
        assert!(!dir.path().join(constellation_api::SOCKET_NAME).exists());
        assert!(!dir.path().join("daemon.pid").exists());
        // The fresh lock is free while the old inode stays held.
        let fresh = std::fs::File::create(&lock).unwrap();
        assert_eq!(
            unsafe { libc::flock(fresh.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        drop(file);
    }
}
