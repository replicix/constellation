//! What Linux and macOS do identically: XDG-style dirs, `flock(2)`,
//! `fork`-based daemonizing, `gethostname`, the effective ids, and the
//! backtrace signal handler.

use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::daemon::{Daemon, Detached};
use crate::dirs::{self, Dirs};
use crate::lock::{FileLock, LockGuard};

/// `$XDG_CONFIG_HOME`/`$XDG_DATA_HOME`/`$XDG_RUNTIME_DIR` with the usual
/// `$HOME` fallbacks, on Linux and macOS alike (see [`crate::dirs`]).
pub(crate) struct XdgDirs;

impl Dirs for XdgDirs {
    fn config_dir(&self) -> io::Result<PathBuf> {
        dirs::xdg_dir_from(
            dirs::env_nonempty("XDG_CONFIG_HOME"),
            std::env::var_os("HOME"),
            ".config",
        )
    }

    fn data_dir(&self) -> io::Result<PathBuf> {
        dirs::xdg_dir_from(
            dirs::env_nonempty("XDG_DATA_HOME"),
            std::env::var_os("HOME"),
            ".local/share",
        )
    }

    fn runtime_dir(&self) -> io::Result<PathBuf> {
        // macOS's `$TMPDIR` is per user (`/var/folders/…/T/`), so it is a
        // fine runtime dir there; Linux's `/tmp` is shared.
        let tmp: Option<OsString> = if cfg!(target_os = "macos") {
            dirs::env_nonempty("TMPDIR")
        } else {
            None
        };
        Ok(dirs::runtime_dir_from(
            dirs::env_nonempty("XDG_RUNTIME_DIR"),
            tmp,
            effective_ids().0,
        ))
    }
}

/// `flock(2)` locks. `holders` answers [`FileLock::holder_pids`] and
/// `opened_by` [`FileLock::opened_by`], which only Linux can (from
/// `/proc`).
pub(crate) struct UnixFileLock {
    pub(crate) holders: fn(&Path) -> Vec<u32>,
    pub(crate) opened_by: fn(u32, &Path) -> bool,
}

fn flock(file: &File, op: libc::c_int) -> io::Result<()> {
    // SAFETY: `file` owns a valid open fd for the duration of the call;
    // `flock` does not touch memory.
    if unsafe { libc::flock(file.as_raw_fd(), op) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

impl FileLock for UnixFileLock {
    fn lock(&self, file: File) -> io::Result<LockGuard> {
        loop {
            match flock(&file, libc::LOCK_EX) {
                Ok(()) => return Ok(LockGuard::new(file)),
                // A signal handler (SA_RESTART-less) interrupted the wait.
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn try_lock(&self, file: File) -> io::Result<Option<LockGuard>> {
        match flock(&file, libc::LOCK_EX | libc::LOCK_NB) {
            Ok(()) => Ok(Some(LockGuard::new(file))),
            Err(e)
                if constellation_types::Code::from_io_error(&e)
                    == constellation_types::Code::Again =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn holder_pids(&self, path: &Path) -> Vec<u32> {
        (self.holders)(path)
    }

    fn opened_by(&self, pid: u32, path: &Path) -> bool {
        (self.opened_by)(pid, path)
    }
}

/// `fork` + `setsid` + stdio to the log (see [`crate::daemon`]).
pub(crate) struct ForkDaemon;

/// The status pipe: `std::io::pipe` makes both ends close-on-exec
/// (`pipe2(O_CLOEXEC)` on Linux), which `detach` relies on.
pub(crate) fn status_pipe() -> io::Result<(File, File)> {
    let (r, w) = io::pipe()?;
    Ok((File::from(OwnedFd::from(r)), File::from(OwnedFd::from(w))))
}

/// Point this process's stdout and stderr at `log` (appending).
fn redirect_stdio(log: &Path) -> io::Result<()> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .map_err(|e| io::Error::new(e.kind(), format!("opening {}: {e}", log.display())))?;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid, open fd for the duration of these calls;
    // dup2 onto 1/2 replaces our own stdout/stderr, which is exactly the
    // point (and the daemon's terminal is gone anyway after setsid()).
    unsafe {
        if libc::dup2(fd, 1) < 0 || libc::dup2(fd, 2) < 0 {
            let e = io::Error::last_os_error();
            return Err(io::Error::new(
                e.kind(),
                format!("redirecting stdio to {}: {e}", log.display()),
            ));
        }
    }
    Ok(())
}

impl Daemon for ForkDaemon {
    unsafe fn detach(&self, log: &Path) -> io::Result<Detached> {
        let (read, write) = status_pipe()?;
        // SAFETY: the caller guarantees this process is single-threaded
        // (the trait's contract), so the child inherits no half-held
        // locks or dead worker threads.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid > 0 {
            // Parent: drop the write end (the child owns its copy), so
            // that EOF means the child is gone.
            drop(write);
            return Ok(Detached::Parent(read));
        }
        // Child: drop the read end, leave the parent's session and
        // controlling terminal, then send stdio to the log before any
        // real work prints to a terminal nobody is watching.
        drop(read);
        // SAFETY: setsid takes no arguments; failure leaves us a
        // background process in the old session, which is not fatal.
        if unsafe { libc::setsid() } < 0 {
            use std::io::Write;
            let _ = writeln!(
                io::stderr(),
                "setsid failed: {}",
                io::Error::last_os_error()
            );
        }
        redirect_stdio(log)?;
        Ok(Detached::Child(write))
    }
}

pub(crate) fn hostname() -> io::Result<String> {
    // HOST_NAME_MAX is 64 on Linux, 255 on macOS; a larger buffer is
    // harmless.
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for `buf.len()` bytes.
    if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..len]).trim().to_string())
}

pub(crate) fn effective_ids() -> (u32, u32) {
    // SAFETY: geteuid/getegid take no arguments and cannot fail.
    unsafe { (libc::geteuid(), libc::getegid()) }
}

/// `kill(pid, 0)`: the process exists (`EPERM`: it exists but is not
/// ours to signal).
#[cfg_attr(target_os = "linux", allow(dead_code))]
pub(crate) fn kill_probe(pid: u32) -> bool {
    // 0 and negative pids name process groups, not a process.
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    // SAFETY: signal 0 only checks for existence and permission.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Deliver `signal` to process `pid`. `pid` 0 and anything above
/// `pid_t`'s range would name a process group or nothing: refused, since
/// no caller means them.
pub(crate) fn signal_process(pid: u32, signal: libc::c_int) -> io::Result<()> {
    let pid = libc::pid_t::try_from(pid)
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("pid {pid}")))?;
    // SAFETY: kill(2) with a positive pid and a valid signal number.
    if unsafe { libc::kill(pid, signal) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The label the backtrace handler prints; set once by
/// [`enable_backtraces`].
static BACKTRACE_LABEL: OnceLock<&'static str> = OnceLock::new();

/// Install the `SIGUSR2` handler that writes the receiving thread's
/// backtrace to stderr (the daemon log). Not async-signal-safe in the
/// strict sense (the capture allocates); a diagnostic for a stalled
/// thread, which sits in a futex wait, never inside the allocator.
/// `SIGUSR2` is used by nothing else in the daemon.
pub(crate) fn enable_backtraces(label: &'static str, tid: fn() -> u64) -> io::Result<()> {
    static TID: OnceLock<fn() -> u64> = OnceLock::new();
    static INSTALLED: OnceLock<io::Result<()>> = OnceLock::new();
    BACKTRACE_LABEL.get_or_init(|| label);
    TID.get_or_init(|| tid);

    extern "C" fn on_sigusr2(_: libc::c_int) {
        let bt = std::backtrace::Backtrace::force_capture();
        let label = BACKTRACE_LABEL.get().copied().unwrap_or("constellation");
        let tid = TID.get().map(|f| f()).unwrap_or(0);
        let text = format!(
            "\n=== {label}: backtrace of stalled thread tid={tid} ===\n{bt}\n=== end ===\n"
        );
        // SAFETY: write(2) on stderr with a valid buffer.
        unsafe {
            libc::write(2, text.as_ptr().cast(), text.len());
        }
    }

    let installed = INSTALLED.get_or_init(|| {
        // SAFETY: a zeroed `sigaction` is a valid starting value; the
        // handler is an `extern "C" fn(c_int)`, as `sa_sigaction` expects
        // without `SA_SIGINFO`.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = (on_sigusr2 as extern "C" fn(libc::c_int)) as *const () as usize;
            sa.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut sa.sa_mask);
            if libc::sigaction(libc::SIGUSR2, &sa, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    });
    match installed {
        Ok(()) => Ok(()),
        Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn the_status_pipe_is_close_on_exec_at_both_ends() {
        let (r, w) = status_pipe().unwrap();
        for fd in [r.as_raw_fd(), w.as_raw_fd()] {
            // SAFETY: F_GETFD on an fd we own.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(
                flags >= 0 && flags & libc::FD_CLOEXEC != 0,
                "fd {fd} flags {flags:#x}"
            );
        }
        let (mut r, mut w) = (r, w);
        w.write_all(b"OK\n").unwrap();
        drop(w);
        let mut got = String::new();
        r.read_to_string(&mut got).unwrap();
        assert_eq!(got, "OK\n");
    }

    #[test]
    fn a_second_open_cannot_take_a_held_lock() {
        let locks = UnixFileLock {
            holders: |_| Vec::new(),
            opened_by: |_, _| false,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.lock");
        let open = || crate::lock::open_lock_file(&path).unwrap();
        let held = locks.try_lock(open()).unwrap().expect("free lock");
        // A second open file description contends (flock is per open
        // file, so this holds within one process too).
        assert!(locks.try_lock(open()).unwrap().is_none());
        // A blocking lock waits until the holder lets go.
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = {
            let path = path.clone();
            std::thread::spawn(move || {
                let locks = UnixFileLock {
                    holders: |_| Vec::new(),
                    opened_by: |_, _| false,
                };
                let guard = locks
                    .lock(crate::lock::open_lock_file(&path).unwrap())
                    .unwrap();
                tx.send(()).unwrap();
                drop(guard);
            })
        };
        assert!(rx
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_err());
        drop(held);
        rx.recv_timeout(std::time::Duration::from_secs(10))
            .expect("the blocking lock was granted once released");
        waiter.join().unwrap();
        assert!(locks.try_lock(open()).unwrap().is_some());
    }

    #[test]
    fn a_leaked_guard_keeps_the_lock() {
        let locks = UnixFileLock {
            holders: |_| Vec::new(),
            opened_by: |_, _| false,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.lock");
        let guard = locks
            .try_lock(crate::lock::open_lock_file(&path).unwrap())
            .unwrap()
            .unwrap();
        let fd = guard.file().as_raw_fd();
        std::mem::forget(guard);
        assert!(locks
            .try_lock(crate::lock::open_lock_file(&path).unwrap())
            .unwrap()
            .is_none());
        // SAFETY: the leaked guard's fd, closed exactly once here.
        unsafe { libc::close(fd) };
        assert!(locks
            .try_lock(crate::lock::open_lock_file(&path).unwrap())
            .unwrap()
            .is_some());
    }

    #[test]
    fn hostname_and_ids_are_this_hosts() {
        let name = hostname().unwrap();
        assert!(!name.is_empty());
        #[cfg(target_os = "linux")]
        assert_eq!(
            name,
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .unwrap()
                .trim()
        );
        let (uid, gid) = effective_ids();
        // SAFETY: getuid/getgid cannot fail.
        unsafe {
            assert_eq!(uid, libc::geteuid());
            assert_eq!(gid, libc::getegid());
        }
        assert!(kill_probe(std::process::id()));
        assert!(!kill_probe(u32::MAX));
        assert!(!kill_probe(0));
    }
}
