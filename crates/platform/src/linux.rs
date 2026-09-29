//! Linux: everything from `/proc`, `/sys/fs/fuse`, `fallocate(2)`,
//! `flock(2)` and `fusermount3`, plus the privileged, fusermount3-free
//! FUSE mount ([`fuse_mount_fd`]).
//!
//! The `/proc` text parsers are public free functions ([`parse_mountinfo`],
//! [`parse_process_facts`], [`flock_pid_in`]) so the shapes seen in the
//! field can be tested verbatim, here and by the callers whose decisions
//! depend on them.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use constellation_types::Rdev;

use crate::fs::FsPrimitives;
use crate::lifecycle::ManualLifecycle;
use crate::lock::FileLock;
use crate::mounts::{MountEntry, MountOpts, MountTable, UnmountMode};
use crate::process::{Process, ProcessFacts, TaskFacts, ThreadRef};
use crate::secrets::FileSecretStore;
use crate::unix::{ForkDaemon, UnixFileLock, XdgDirs};
use crate::HostServices;

pub(crate) fn host_services() -> HostServices {
    let dirs = Arc::new(XdgDirs);
    let file_lock = file_lock();
    HostServices {
        dirs: dirs.clone(),
        process: Arc::new(LinuxProcess),
        daemon: Arc::new(ForkDaemon),
        file_lock: file_lock.clone(),
        fs: Arc::new(LinuxFs),
        secrets: Arc::new(FileSecretStore::in_config_dir(dirs, file_lock)),
        lifecycle: Arc::new(ManualLifecycle::new()),
        mounts: Arc::new(LinuxMounts),
    }
}

pub(crate) fn file_lock() -> Arc<dyn FileLock> {
    Arc::new(UnixFileLock {
        holder: flock_holder_pid,
    })
}

fn off_t(n: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(n).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset {n} out of range"),
        )
    })
}

fn cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))
}

// ---------------------------------------------------------------- locks

/// The pid `/proc/locks` names for the `FLOCK` on `lock`'s inode.
fn flock_holder_pid(lock: &Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(lock).ok()?;
    let locks = std::fs::read_to_string("/proc/locks").ok()?;
    let dev = meta.dev();
    let (major, minor) = (libc::major(dev), libc::minor(dev));
    let want = format!("{major:02x}:{minor:02x}:{}", meta.ino());
    flock_pid_in(&locks, &want)
}

/// Find the pid of the `FLOCK` line on `inode` (`maj:min:ino`, as
/// `/proc/locks` prints it) in `locks`.
pub fn flock_pid_in(locks: &str, inode: &str) -> Option<u32> {
    for line in locks.lines() {
        // `1: FLOCK  ADVISORY  WRITE 161984 103:01:2098107 0 EOF`
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 6 && fields[1] == "FLOCK" && fields[5] == inode {
            return fields[4].parse().ok();
        }
    }
    None
}

// -------------------------------------------------------------- process

struct LinuxProcess;

/// `Key:\tvalue` from a `/proc/<pid>/status`.
pub fn status_field<'a>(status: &'a str, key: &str) -> Option<&'a str> {
    status
        .lines()
        .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix(':')))
        .map(str::trim)
}

fn sigkill_pending(status: &str) -> bool {
    // Signal 9 is bit 8 of the pending masks.
    ["SigPnd", "ShdPnd"].iter().any(|key| {
        status_field(status, key)
            .and_then(|hex| u64::from_str_radix(hex, 16).ok())
            .is_some_and(|mask| mask & (1 << 8) != 0)
    })
}

/// [`ProcessFacts`] from the leader's `/proc/<pid>/status` and the other
/// threads' `task/<tid>/status`.
pub fn parse_process_facts(status: &str, tasks: &[String]) -> ProcessFacts {
    let state = status_field(status, "State").unwrap_or("?").to_string();
    ProcessFacts {
        name: status_field(status, "Name").unwrap_or("?").to_string(),
        zombie: state.starts_with('Z'),
        dead: state.starts_with('X'),
        state,
        sigkill_pending: sigkill_pending(status),
        tasks: tasks
            .iter()
            .map(|t| TaskFacts {
                state: status_field(t, "State").unwrap_or("?").to_string(),
                sigkill_pending: sigkill_pending(t),
                has_mm: status_field(t, "VmSize").is_some(),
            })
            .collect(),
    }
}

/// `Groups:` of a `/proc/<pid>/status`.
pub fn parse_groups(status: &str) -> Vec<u32> {
    status_field(status, "Groups")
        .unwrap_or("")
        .split_whitespace()
        .filter_map(|g| g.parse().ok())
        .collect()
}

fn gettid() -> u64 {
    // SAFETY: gettid(2) takes no arguments and cannot fail.
    unsafe { libc::gettid() as u64 }
}

impl Process for LinuxProcess {
    fn hostname(&self) -> io::Result<String> {
        crate::unix::hostname()
    }

    fn is_alive(&self, pid: u32) -> bool {
        Path::new(&format!("/proc/{pid}")).exists()
    }

    fn facts(&self, pid: u32) -> io::Result<ProcessFacts> {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
            .map_err(|e| io::Error::new(e.kind(), format!("/proc/{pid}/status: {e}")))?;
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
        Ok(parse_process_facts(&status, &tasks))
    }

    fn supplementary_groups(&self, pid: u32) -> io::Result<Vec<u32>> {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
        Ok(parse_groups(&status))
    }

    fn effective_ids(&self) -> (u32, u32) {
        crate::unix::effective_ids()
    }

    fn thread_count(&self) -> Option<u64> {
        std::fs::read_to_string("/proc/self/status")
            .ok()?
            .lines()
            .find_map(|l| l.strip_prefix("Threads:"))
            .and_then(|v| v.trim().parse().ok())
    }

    fn memory_budget(&self) -> Option<u64> {
        let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
        let host_kib = contents
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        let host = host_kib.checked_mul(1024)?;
        let cgroup = [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .filter_map(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value < u64::MAX / 2)
        .min();
        Some(cgroup.map_or(host, |limit| limit.min(host)))
    }

    fn current_thread(&self) -> ThreadRef {
        let tid = gettid();
        ThreadRef { tid, handle: tid }
    }

    fn enable_backtraces(&self, label: &'static str) -> io::Result<()> {
        crate::unix::enable_backtraces(label, gettid)
    }

    fn request_backtrace(&self, thread: ThreadRef) -> io::Result<()> {
        // `tgkill` rather than `pthread_kill`: a tid that has exited
        // meanwhile fails with ESRCH instead of being undefined behaviour.
        // SAFETY: tgkill(2) with this process's pid and a thread id.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_tgkill,
                libc::getpid(),
                thread.handle as libc::pid_t,
                libc::SIGUSR2,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn suspend(&self, pid: u32) -> io::Result<()> {
        crate::unix::signal_process(pid, libc::SIGSTOP)
    }

    fn resume(&self, pid: u32) -> io::Result<()> {
        crate::unix::signal_process(pid, libc::SIGCONT)
    }

    fn kill(&self, pid: u32) -> io::Result<()> {
        crate::unix::signal_process(pid, libc::SIGKILL)
    }
}

// ------------------------------------------------------------------- fs

struct LinuxFs;

fn fallocate(file: &File, mode: libc::c_int, offset: u64, len: u64) -> io::Result<()> {
    // SAFETY: `file` owns a valid open fd for the duration of the call.
    let rc = unsafe { libc::fallocate(file.as_raw_fd(), mode, off_t(offset)?, off_t(len)?) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl FsPrimitives for LinuxFs {
    fn punch_hole(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
        fallocate(
            file,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset,
            len,
        )
    }

    fn preallocate(&self, file: &File, offset: u64, len: u64, keep_size: bool) -> io::Result<()> {
        let mode = if keep_size {
            libc::FALLOC_FL_KEEP_SIZE
        } else {
            0
        };
        fallocate(file, mode, offset, len)
    }

    fn full_fsync(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }

    fn drop_cache(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
        // SAFETY: `file` owns a valid open fd; the advice is advisory.
        let rc = unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                off_t(offset)?,
                off_t(len)?,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        // posix_fadvise returns the error number instead of setting errno.
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }
}

// --------------------------------------------------------------- mounts

struct LinuxMounts;

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

/// The mounts in a `/proc/<pid>/mountinfo`.
pub fn parse_mountinfo(text: &str) -> Vec<MountEntry> {
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
        let Some((major, minor)) = pre[2]
            .split_once(':')
            .and_then(|(a, b)| Some((a.parse::<u32>().ok()?, b.parse::<u32>().ok()?)))
        else {
            continue;
        };
        out.push(MountEntry {
            mountpoint: PathBuf::from(unescape_mountinfo(pre[4])),
            fstype: post[0].to_string(),
            source: unescape_mountinfo(post[1]),
            device: Some(Rdev::new(major, minor)),
        });
    }
    out
}

fn fusectl(connection: u32, file: &str) -> PathBuf {
    PathBuf::from(format!("/sys/fs/fuse/connections/{connection}/{file}"))
}

fn umount2(path: &Path, flags: libc::c_int) -> io::Result<()> {
    let c = cstring(path)?;
    // SAFETY: `c` is a valid NUL-terminated path for the call.
    if unsafe { libc::umount2(c.as_ptr(), flags) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl MountTable for LinuxMounts {
    fn list(&self) -> io::Result<Vec<MountEntry>> {
        Ok(parse_mountinfo(&std::fs::read_to_string(
            "/proc/self/mountinfo",
        )?))
    }

    fn unmount(&self, path: &Path, mode: UnmountMode) -> io::Result<()> {
        let args: &[&str] = match mode {
            UnmountMode::Normal => &["-u"],
            UnmountMode::Lazy => &["-uz"],
            UnmountMode::Force => return umount2(path, libc::MNT_FORCE),
        };
        // `fusermount3` (setuid) is how an unprivileged user unmounts
        // their own FUSE mount; fall back to `fusermount` for older
        // systems.
        let mut last = None;
        for bin in ["fusermount3", "fusermount"] {
            match std::process::Command::new(bin)
                .args(args)
                .arg(path)
                .status()
            {
                Ok(s) if s.success() => return Ok(()),
                Ok(s) => last = Some(format!("{bin} {}: {s}", args.join(" "))),
                Err(e) => last = Some(format!("{bin}: {e}")),
            }
        }
        // Root without either helper installed can still do it directly.
        if crate::unix::effective_ids().0 == 0 {
            let flags = if mode == UnmountMode::Lazy {
                libc::MNT_DETACH
            } else {
                0
            };
            return umount2(path, flags);
        }
        Err(io::Error::other(format!(
            "could not unmount {}: {}",
            path.display(),
            last.unwrap_or_default()
        )))
    }

    fn fuse_waiting(&self, connection: u32) -> io::Result<u64> {
        std::fs::read_to_string(fusectl(connection, "waiting"))?
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn abort_fuse(&self, connection: u32) -> io::Result<()> {
        // `echo 1 > /sys/fs/fuse/connections/<n>/abort`.
        std::fs::write(fusectl(connection, "abort"), "1\n")
    }
}

// ------------------------------------------------------ privileged mount

/// Mount a FUSE filesystem at `target` with `mount(2)` directly and return
/// the `/dev/fuse` descriptor that serves it — no `fusermount3`, so it
/// needs `CAP_SYS_ADMIN` (root, or a privileged container such as plan
/// 37's CSI node plugin). The descriptor is close-on-exec; whoever serves
/// the mount reads requests from it (C4's in-place upgrade hands it to the
/// new process, plan 37's node plugin passes it over `SCM_RIGHTS`).
///
/// The kernel options are fuser's own direct-mount ones (`fuse_pure.rs`,
/// `fuse_mount_sys`): `fd=<n>,rootmode=<type bits of target>,
/// user_id=<uid>,group_id=<gid>`, then `allow_other`,
/// `default_permissions` and `max_read=` as `opts` asks. The source is
/// `opts.fsname`, the type `fuse` (or `fuse.<subtype>`), and the flags
/// `MS_NOSUID|MS_NODEV` by default, plus `MS_RDONLY` for a read-only
/// mount.
///
/// The kernel sends `FUSE_INIT` at once and every access to the mount
/// waits for an answer on the returned descriptor: do not `stat` the
/// mountpoint from the thread that is going to serve it before it does.
pub fn fuse_mount_fd(target: &Path, opts: &MountOpts) -> io::Result<OwnedFd> {
    use std::os::unix::fs::MetadataExt;
    let rootmode = std::fs::metadata(target)?.mode() & libc::S_IFMT;
    let dev = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")?;
    let (euid, egid) = crate::unix::effective_ids();
    let mut data = format!(
        "fd={},rootmode={rootmode:o},user_id={},group_id={}",
        dev.as_raw_fd(),
        opts.user_id.unwrap_or(euid),
        opts.group_id.unwrap_or(egid),
    );
    if opts.allow_other {
        data.push_str(",allow_other");
    }
    if opts.default_permissions {
        data.push_str(",default_permissions");
    }
    if let Some(max_read) = opts.max_read {
        data.push_str(&format!(",max_read={max_read}"));
    }
    let mut flags: libc::c_ulong = 0;
    if opts.nosuid {
        flags |= libc::MS_NOSUID;
    }
    if opts.nodev {
        flags |= libc::MS_NODEV;
    }
    if opts.read_only {
        flags |= libc::MS_RDONLY;
    }
    let fstype = match &opts.subtype {
        Some(subtype) => format!("fuse.{subtype}"),
        None => "fuse".to_string(),
    };
    let invalid = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} contains a NUL byte"),
        )
    };
    let source = CString::new(opts.fsname.as_str()).map_err(|_| invalid("fsname"))?;
    let fstype = CString::new(fstype).map_err(|_| invalid("subtype"))?;
    let data = CString::new(data).map_err(|_| invalid("mount options"))?;
    let target_c = cstring(target)?;
    // SAFETY: every pointer is a valid NUL-terminated string that outlives
    // the call; `dev` stays open across it (the kernel takes its own
    // reference to the file from `fd=`).
    let rc = unsafe {
        libc::mount(
            source.as_ptr(),
            target_c.as_ptr(),
            fstype.as_ptr(),
            flags,
            data.as_ptr().cast(),
        )
    };
    if rc != 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(
            e.kind(),
            format!("mounting FUSE at {}: {e}", target.display()),
        ));
    }
    Ok(OwnedFd::from(dev))
}

/// Unmount a FUSE mount made by [`fuse_mount_fd`] (`umount2`; `lazy`:
/// `MNT_DETACH`, which also works while requests are pending). Privileged,
/// like the mount.
pub fn fuse_unmount(target: &Path, lazy: bool) -> io::Result<()> {
    umount2(target, if lazy { libc::MNT_DETACH } else { 0 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock::open_lock_file;

    #[test]
    fn mountinfo_lines_yield_the_device_type_source_and_unescaped_mountpoint() {
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
                MountEntry {
                    mountpoint: PathBuf::from("/mnt2"),
                    fstype: "ext3".into(),
                    source: "/dev/root".into(),
                    device: Some(Rdev::new(98, 0)),
                },
                MountEntry {
                    mountpoint: PathBuf::from("/home/ubuntu/cbench/mnt6/c6daws"),
                    fstype: "fuse".into(),
                    source: "c6daws".into(),
                    device: Some(Rdev::new(0, 48)),
                },
                MountEntry {
                    mountpoint: PathBuf::from("/tmp/with space"),
                    fstype: "fuse".into(),
                    source: "my\tfs".into(),
                    device: Some(Rdev::new(0, 49)),
                },
            ]
        );
        assert_eq!(got[0].fuse_connection(), None);
        assert_eq!(got[1].fuse_connection(), Some(48));
    }

    #[test]
    fn this_hosts_mount_table_has_a_root() {
        let mounts = LinuxMounts;
        assert!(mounts
            .list()
            .unwrap()
            .iter()
            .any(|m| m.mountpoint == Path::new("/")));
        assert!(mounts.is_mountpoint(Path::new("/")).unwrap());
        let dir = tempfile::tempdir().unwrap();
        assert!(!mounts.is_mountpoint(dir.path()).unwrap());
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

    /// The real thing: a lock this process holds is attributed to this
    /// process through `/proc/locks`, and a released one to nobody.
    #[test]
    fn own_flock_is_found_in_proc_locks() {
        let locks = file_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.lock");
        let guard = locks.lock(open_lock_file(&path).unwrap()).unwrap();
        assert_eq!(locks.holder_pid(&path), Some(std::process::id()));
        drop(guard);
        assert_eq!(locks.holder_pid(&path), None);
    }

    #[test]
    fn status_text_becomes_facts() {
        let leader = "Name:\tconstellation-2\nState:\tZ (zombie)\nTgid:\t161984\n\
                      SigPnd:\t0000000000000000\nShdPnd:\t0000000000000100\n\
                      Groups:\t4 24 27 1000 \n";
        let task = "Name:\tconstellation-2\nState:\tD (disk sleep)\nVmSize:\t  593048 kB\n\
                    SigPnd:\t0000000000000000\nShdPnd:\t0000000000000000\n"
            .to_string();
        let facts = parse_process_facts(leader, &[task]);
        assert_eq!(
            facts,
            ProcessFacts {
                name: "constellation-2".into(),
                state: "Z (zombie)".into(),
                zombie: true,
                dead: false,
                sigkill_pending: true,
                tasks: vec![TaskFacts {
                    state: "D (disk sleep)".into(),
                    sigkill_pending: false,
                    has_mm: true,
                }],
            }
        );
        assert_eq!(parse_groups(leader), vec![4, 24, 27, 1000]);
        assert_eq!(parse_groups("Name:\tx\n"), Vec::<u32>::new());
        let empty = parse_process_facts("", &[]);
        assert_eq!((empty.name.as_str(), empty.state.as_str()), ("?", "?"));
    }

    #[test]
    fn this_process_is_alive_running_and_threaded() {
        let p = LinuxProcess;
        let me = std::process::id();
        assert!(p.is_alive(me));
        assert!(!p.is_alive(u32::MAX));
        let facts = p.facts(me).unwrap();
        assert!(!facts.zombie && !facts.dead, "{facts:?}");
        assert!(p
            .facts(u32::MAX)
            .unwrap_err()
            .to_string()
            .starts_with("/proc/"));
        assert!(p.thread_count().unwrap() >= 1);
        assert!(p.memory_budget().unwrap() > 0);
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        assert_eq!(p.supplementary_groups(me).unwrap(), parse_groups(&status));
        let t = p.current_thread();
        let other = std::thread::spawn(|| LinuxProcess.current_thread())
            .join()
            .unwrap();
        assert_ne!(t, other);
        assert!(
            Path::new(&format!("/proc/self/task/{}", t.tid)).exists(),
            "{t:?}"
        );
    }

    /// A thread asked for its backtrace gets the signal and survives it.
    #[test]
    fn a_thread_can_be_asked_for_its_backtrace() {
        let p = LinuxProcess;
        p.enable_backtraces("platform-test").unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let worker = std::thread::spawn(move || {
            tx.send(LinuxProcess.current_thread()).unwrap();
            // Parked in a futex wait, as a stalled request would be.
            let _ = done_rx.recv();
        });
        let thread = rx.recv().unwrap();
        p.request_backtrace(thread).unwrap();
        done_tx.send(()).unwrap();
        worker.join().unwrap();
        // A thread that no longer exists is an error, not a crash.
        assert!(p
            .request_backtrace(ThreadRef {
                tid: 0,
                handle: i32::MAX as u64
            })
            .is_err());
    }

    #[test]
    fn punch_hole_makes_a_hole_and_keeps_the_size() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("staging");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        const MIB: u64 = 1 << 20;
        file.write_all(&vec![0xa5u8; (4 * MIB) as usize]).unwrap();
        file.sync_all().unwrap();
        match LinuxFs.punch_hole(&file, MIB, MIB) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EOPNOTSUPP) => {
                eprintln!("skipping: this filesystem cannot punch holes ({e})");
                return;
            }
            Err(e) => panic!("punch_hole: {e}"),
        }
        assert_eq!(file.metadata().unwrap().len(), 4 * MIB, "size kept");
        // SAFETY: lseek on an fd we own.
        let hole = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_HOLE) };
        assert_eq!(hole as u64, MIB, "SEEK_HOLE finds the punched range");
        // SAFETY: as above.
        let data = unsafe { libc::lseek(file.as_raw_fd(), MIB as libc::off_t, libc::SEEK_DATA) };
        assert_eq!(data as u64, 2 * MIB, "data resumes after it");
        let mut buf = vec![1u8; 4096];
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut buf, MIB + 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        file.seek(SeekFrom::Start(0)).unwrap();
        // Out-of-range offsets are refused before the syscall.
        assert_eq!(
            LinuxFs.punch_hole(&file, u64::MAX, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn preallocate_fsync_and_drop_cache_work_on_a_plain_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(dir.path().join("f"))
            .unwrap();
        LinuxFs.preallocate(&file, 0, 65536, true).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
        LinuxFs.preallocate(&file, 0, 65536, false).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 65536);
        LinuxFs.full_fsync(&file).unwrap();
        LinuxFs.drop_cache(&file, 0, 0).unwrap();
    }

    #[test]
    fn host_services_are_the_linux_ones() {
        let host = host_services();
        assert!(host.dirs.config_dir().is_ok() || std::env::var_os("HOME").is_none());
        assert!(!host.process.hostname().unwrap().is_empty());
    }

    /// The privileged direct mount, end to end: needs root and
    /// `/dev/fuse` (skipped otherwise). Nothing serves the connection, so
    /// the test never touches the mountpoint itself — only the mount
    /// table — and detaches it before closing the descriptor.
    #[test]
    fn fuse_mount_fd_mounts_without_fusermount() {
        if crate::unix::effective_ids().0 != 0 || !Path::new("/dev/fuse").exists() {
            eprintln!("skipping: needs root and /dev/fuse");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().canonicalize().unwrap();
        let mut opts = MountOpts::new("constellation-platform-test");
        opts.allow_other = true;
        let fd = match fuse_mount_fd(&target, &opts) {
            Ok(fd) => fd,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                eprintln!("skipping: mount(2) refused in this container ({e})");
                return;
            }
            Err(e) => panic!("fuse_mount_fd: {e}"),
        };
        let mounts = LinuxMounts.list().unwrap();
        let entry = mounts
            .iter()
            .find(|m| m.mountpoint == target)
            .unwrap_or_else(|| panic!("{} not in mountinfo: {mounts:?}", target.display()));
        assert_eq!(entry.fstype, "fuse");
        assert_eq!(entry.source, "constellation-platform-test");
        let connection = entry
            .fuse_connection()
            .expect("an anonymous-device FUSE mount");
        // The FUSE_INIT the kernel sent on mount is waiting for us, if
        // fusectl is mounted here to say so.
        if let Ok(waiting) = LinuxMounts.fuse_waiting(connection) {
            assert!(waiting <= 1, "waiting {waiting}");
        }
        let fd_raw = fd.as_raw_fd();
        // SAFETY: F_GETFD on an fd we own.
        let flags = unsafe { libc::fcntl(fd_raw, libc::F_GETFD) };
        assert!(flags & libc::FD_CLOEXEC != 0);
        fuse_unmount(&target, true).unwrap();
        drop(fd);
        assert!(!LinuxMounts
            .list()
            .unwrap()
            .iter()
            .any(|m| m.mountpoint == target));
    }

    /// `suspend`/`resume`/`kill` against a real child: `/proc` shows the
    /// stop and the continue, and the kill is a `SIGKILL` the child's exit
    /// status reports.
    #[test]
    fn a_process_can_be_frozen_thawed_and_killed() {
        use std::os::unix::process::ExitStatusExt;
        use std::time::{Duration, Instant};
        let process = LinuxProcess;
        let mut child = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = child.id();
        let state = || {
            let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
            status_field(&status, "State").unwrap().chars().next()
        };
        let wait_for = |want: char| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while state() != Some(want) {
                assert!(Instant::now() < deadline, "never reached state {want}");
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        process.suspend(pid).unwrap();
        wait_for('T');
        process.resume(pid).unwrap();
        wait_for('S');
        process.kill(pid).unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(libc::SIGKILL));
        // Nothing to signal any more, and pids that name no single process
        // are refused before the syscall.
        assert!(process.kill(pid).is_err());
        assert_eq!(
            process.suspend(0).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            process.resume(u32::MAX).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
