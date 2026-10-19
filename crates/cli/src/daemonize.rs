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
//! invoking shell instead of silently backgrounding a dead process. Before
//! its verdict the child may send progress lines (waiting for the previous
//! daemon's clean-unmount drain), which the parent prints on stderr.
//!
//! The detaching itself (fork, `setsid`, stdio to `daemon.log`, the
//! close-on-exec status pipe) is the host's: `constellation_platform`'s
//! `Daemon::detach`. This module owns the protocol on the pipe and the
//! parent's exit.

use anyhow::{Context, Result};
use constellation_platform::Detached;
use std::io::{Read, Write};
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
    write: std::fs::File,
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
        // One line: the parent reads the verdict up to its newline.
        let message = message.replace('\n', " ");
        self.send(format!("ERR:{message}\n").as_bytes())
    }

    /// A line for the waiting parent to print on stderr: what this mount
    /// is waiting for. Best-effort (a parent that is gone reads nothing).
    pub fn progress(&self, message: &str) {
        let message = message.replace('\n', " ");
        let _ = (&self.write).write_all(format!("{PROGRESS}{message}\n").as_bytes());
    }

    fn write_pid_file(&self) -> Result<()> {
        std::fs::write(
            self.state_dir.join("daemon.pid"),
            std::process::id().to_string(),
        )
        .context("writing daemon.pid")
    }

    fn send(mut self, bytes: &[u8]) -> Result<()> {
        self.write
            .write_all(bytes)
            .context("writing to status pipe")
    }
}

// The detach's redirect of stdout/stderr to `state_dir/daemon.log` (append
// mode) alone is what makes daemonized output land in the log file instead
// of a terminal nobody is attached to anymore: `tracing`'s writer
// (`log_buffer::LogBuffer`) writes to whatever fd 2 is. Until the daemon
// starts the buffer's writer thread (`start_daemon_logging`, after this
// fork) every line is written by the logging thread itself, and nothing is
// queued. That is what keeps the fork safe: it copies only the calling
// thread, so a writer thread (or a queued line) that existed before it
// would be dead in the child, or written twice. `fork_if_needed` asserts it.
//
// The status pipe is close-on-exec at both ends. `fork` keeps them (the
// child needs its write end), but nothing the daemon later *executes* may
// inherit the write end: the parent waits for the verdict line, and
// before that line arrives only EOF — every write end closed — tells it
// the child died. A descendant holding a copy (the zombie reaper, which
// lives as long as the daemon) would turn a child that dies before
// reporting into a parent hung for the daemon's lifetime. (Before the
// parent stopped at the verdict line, it also hung every successful
// daemonized `mount` until the reaper exited: harness
// `named-shared-daemon` took 583 s.)

/// What starts a progress line on the status pipe (any other line is the
/// verdict).
const PROGRESS: &str = "PROGRESS:";

/// The child's verdict: the first complete line on the status pipe that
/// is not progress (each progress line goes to `progress` as it comes),
/// or whatever arrived before EOF when the child died without finishing
/// one. It never waits past the verdict's line: the child keeps serving
/// (and its write end open) after reporting.
fn read_verdict(pipe: impl Read, progress: &mut dyn FnMut(&str)) -> String {
    use std::io::BufRead;
    let mut reader = std::io::BufReader::new(pipe);
    loop {
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let line = line.trim_end_matches('\n');
        match line.strip_prefix(PROGRESS) {
            Some(message) => progress(message),
            None => return line.to_string(),
        }
    }
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
    debug_assert!(
        !crate::log_buffer::process_started(),
        "the log writer thread must start after the daemonizer's fork, never before it"
    );

    // SAFETY: detaching (a `fork()` here) is safe specifically because
    // this runs before the tokio runtime, the shipper, any lease keeper,
    // the cache, or any FUSE session thread exist — the process is still
    // single-threaded, so the child inherits no half-held locks or dead
    // worker threads.
    let detached = unsafe {
        constellation_platform::native()
            .daemon
            .detach(&state_dir.join("daemon.log"))
    }
    .context("detaching into the background")?;
    let write = match detached {
        // Child: in its own session, stdio already in the log, before any
        // real work prints to a terminal nobody is watching.
        Detached::Child(write) => write,
        // Parent: the write end is closed (the child owns its copy);
        // block reading the child's verdict. EOF with no recognized "OK"
        // is treated as failure, not success — a child that dies
        // mid-startup without writing must not be silently reported as a
        // healthy background daemon.
        Detached::Parent(pipe_read) => parent_exit(pipe_read),
    };
    Ok(Outcome::Daemon(Verdict {
        write,
        state_dir: state_dir.to_path_buf(),
    }))
}

/// The invoking process's side: report the child's verdict and exit with
/// it.
fn parent_exit(pipe_read: std::fs::File) -> ! {
    let text = read_verdict(pipe_read, &mut |message| eprintln!("{message}"));
    let verdict = text.as_str();
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

#[cfg(test)]
mod tests {
    use super::*;

    // The pipe's close-on-exec ends are the platform's (tested there);
    // these use a plain `std::io::pipe`, which is close-on-exec too.

    /// The verdict is read up to its line, not to EOF: the child (and
    /// anything it spawned) keeps the write end open while it serves.
    #[test]
    fn the_verdict_is_read_without_waiting_for_eof() {
        let (reader, mut writer) = std::io::pipe().unwrap();
        writer.write_all(b"OK\n").unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(read_verdict(reader, &mut |_| {}));
        });
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("read_verdict waited for EOF with the write end still open");
        assert_eq!(got, "OK");
        drop(writer);
    }

    #[test]
    fn a_child_that_dies_before_reporting_reads_as_no_verdict() {
        let (reader, writer) = std::io::pipe().unwrap();
        drop(writer);
        assert_eq!(read_verdict(reader, &mut |_| {}), "");
        let (reader, mut writer) = std::io::pipe().unwrap();
        writer.write_all(b"ERR:bad --s3\n").unwrap();
        drop(writer);
        assert_eq!(read_verdict(reader, &mut |_| {}), "ERR:bad --s3");
    }

    /// A mount waiting for the previous daemon's drain says so through
    /// the parent, line by line, before its verdict; a child that dies
    /// after progress still reads as no verdict.
    #[test]
    fn progress_lines_come_before_the_verdict() {
        let (reader, mut writer) = std::io::pipe().unwrap();
        writer
            .write_all(b"PROGRESS:waiting for the drain\nPROGRESS:3 uploads left\nOK\n")
            .unwrap();
        let mut seen = Vec::new();
        assert_eq!(
            read_verdict(reader, &mut |m| seen.push(m.to_string())),
            "OK"
        );
        assert_eq!(seen, ["waiting for the drain", "3 uploads left"]);
        let (reader, mut writer) = std::io::pipe().unwrap();
        writer.write_all(b"PROGRESS:waiting\n").unwrap();
        drop(writer);
        assert_eq!(read_verdict(reader, &mut |_| {}), "");
    }
}
