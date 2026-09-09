//! JuiceFS-style daemonization (plan 21, step 5): `mount` backgrounds
//! itself by default, forking *before* any runtime work happens — before
//! `NodeRuntime::start`, before the tokio runtime spawns workers, before
//! any FUSE session thread exists. Forking later would give the child a
//! corpse: worker threads gone, mutexes possibly locked by threads that
//! no longer run.
//!
//! The parent blocks on a status pipe until the child reports either
//! "control socket bound and every requested view attached" or an error,
//! then exits with the child's verdict — a failure (bad `--s3`, a bind
//! conflict, an unmountable mountpoint) surfaces synchronously to the
//! invoking shell instead of silently backgrounding a dead process.

use anyhow::{Context, Result};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

/// What the caller should do after `fork_if_needed` returns.
pub enum Outcome {
    /// `--foreground` (or `CONSTELLATION_NO_DAEMONIZE`): no fork
    /// happened: behave exactly like `mount` did before this plan.
    Foreground,
    /// This is the forked, `setsid()`'d child. Continue building the
    /// node/mounts, then call exactly one of `Verdict::success` /
    /// `Verdict::failure` on the returned handle before serving.
    Daemon(Verdict),
}

/// The still-open write end of the status pipe, plus the state dir the
/// PID file belongs to. Consuming methods so a verdict can only be
/// reported once.
pub struct Verdict {
    write_fd: OwnedFd,
    state_dir: std::path::PathBuf,
}

impl Verdict {
    /// This invocation became the daemon: write the PID file, then
    /// report success up the pipe. Call this only once *every* requested
    /// view is attached.
    pub fn success_daemon(self) -> Result<()> {
        self.write_pid_file()?;
        self.send(b"OK\n")
    }

    /// This invocation attached to an already-running daemon instead —
    /// there is no new PID file to write, since the daemon that already
    /// owns the state dir wrote its own.
    pub fn success_attached(self) -> Result<()> {
        self.send(b"OK\n")
    }

    /// Report failure up the pipe (the parent prints `message` and exits
    /// non-zero) without leaving a PID file behind — this daemon is not
    /// going to keep running.
    pub fn failure(self, message: &str) -> Result<()> {
        self.send(format!("ERR:{message}\n").as_bytes())
    }

    fn write_pid_file(&self) -> Result<()> {
        std::fs::write(
            self.state_dir.join("daemon.pid"),
            std::process::id().to_string(),
        )
        .context("writing daemon.pid")
    }

    fn send(self, bytes: &[u8]) -> Result<()> {
        let mut f = std::fs::File::from(self.write_fd);
        f.write_all(bytes).context("writing to status pipe")
    }
}

/// Redirect this process's stdout/stderr to `state_dir/daemon.log`
/// (append mode) — `tracing`'s writer already tees every event to
/// stderr via `log_buffer::LogWriter`, so this alone is what makes
/// daemonized output land in the log file instead of a terminal nobody
/// is attached to anymore.
fn redirect_stdio_to_log(state_dir: &Path) -> Result<()> {
    let log_path = state_dir.join("daemon.log");
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("opening {}", log_path.display()))?;
    let fd = log_file.as_raw_fd();
    // SAFETY: `fd` is a valid, open fd for the duration of these calls;
    // dup2 onto 1/2 replaces our own stdout/stderr, which is exactly the
    // point (and the daemon's terminal is gone anyway after setsid()).
    unsafe {
        if libc::dup2(fd, 1) < 0 || libc::dup2(fd, 2) < 0 {
            return Err(std::io::Error::last_os_error()).context("redirecting stdio to daemon.log");
        }
    }
    Ok(())
}

/// Fork unless `foreground` (or `CONSTELLATION_NO_DAEMONIZE`) says not
/// to. Never returns in the parent on the daemonizing path: it blocks
/// reading the child's verdict, prints it, and exits the process with
/// the child's status.
pub fn fork_if_needed(foreground: bool, state_dir: &Path) -> Result<Outcome> {
    if foreground || std::env::var_os("CONSTELLATION_NO_DAEMONIZE").is_some() {
        return Ok(Outcome::Foreground);
    }
    std::fs::create_dir_all(state_dir)
        .with_context(|| format!("creating {}", state_dir.display()))?;

    let mut fds = [0i32; 2];
    // SAFETY: `fds` is a valid 2-element buffer for `pipe(2)` to fill.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("creating status pipe");
    }
    let (read_fd, write_fd) = (fds[0], fds[1]);

    // SAFETY: `fork()` is safe to call here specifically because this
    // runs before the tokio runtime, the shipper, any lease keeper, the
    // cache, or any FUSE session thread exist — the process is still
    // single-threaded, so the child inherits no half-held locks or dead
    // worker threads.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(std::io::Error::last_os_error()).context("fork");
    }
    if pid > 0 {
        // Parent: close the write end (ours to close; the child owns
        // its copy), then block reading the child's verdict. EOF with
        // no recognized "OK" is treated as failure, not success — a
        // child that dies mid-startup without writing must not be
        // silently reported as a healthy background daemon.
        unsafe {
            libc::close(write_fd);
        }
        let mut pipe_read = unsafe { std::fs::File::from_raw_fd(read_fd) };
        let mut buf = Vec::new();
        let _ = pipe_read.read_to_end(&mut buf);
        let text = String::from_utf8_lossy(&buf);
        let verdict = text.lines().next_back().unwrap_or("");
        if let Some(message) = verdict.strip_prefix("ERR:") {
            eprintln!("mount failed: {message}");
            std::process::exit(1);
        }
        if verdict == "OK" {
            println!("daemon started (pid file in the state dir)");
            std::process::exit(0);
        }
        eprintln!("mount failed: daemon exited before reporting status");
        std::process::exit(1);
    }

    // Child: close the read end, detach from the controlling terminal
    // and the parent's session, then redirect stdio before doing any
    // real work so nothing prints to a terminal nobody is watching.
    unsafe {
        libc::close(read_fd);
        if libc::setsid() < 0 {
            // Not fatal — we're still a background process either way —
            // but worth knowing about if it ever happens.
            let _ = writeln!(
                std::io::stderr(),
                "setsid failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    redirect_stdio_to_log(state_dir)?;
    // SAFETY: `write_fd` came from our own successful `pipe(2)` call
    // above and has not been closed.
    let write_fd = unsafe { OwnedFd::from_raw_fd(write_fd) };
    Ok(Outcome::Daemon(Verdict {
        write_fd,
        state_dir: state_dir.to_path_buf(),
    }))
}
