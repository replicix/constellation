//! Children whose output is collected while they run.
//!
//! A child spawned with `Stdio::piped()` stdout or stderr blocks in
//! `write(2)` once the pipe is full, and a parent that waits for the child
//! to exit before it reads the pipe waits for ever. How much a pipe holds
//! is not a constant: 64 KiB by default, but Linux hands out reduced pipes
//! (two pages, 8 KiB) once a user's pipes together pass
//! `/proc/sys/fs/pipe-user-pages-soft`, and a workstation running many
//! processes gets there. Code that was fine for years then deadlocks on a
//! child that writes 19 KiB.
//!
//! The rule this module encodes: **read every pipe you create, from the
//! moment the child exists until it closes it.** [`DrainedChild`] does that
//! for the cases that need a deadline or an early look at the child
//! (`try_wait`, `kill`) which [`std::process::Command::output`] cannot
//! give: one reader thread per pipe, started right after the spawn, reads
//! to end-of-file into a buffer.
//!
//! The buffer keeps the **last** [`DEFAULT_KEEP`] bytes of each stream (a
//! test that wants the tail of a chatty daemon's log wants the end, and a
//! runaway child must not take the parent's memory); the reader keeps
//! reading past that, so the child is never blocked by it. An output that
//! fits is returned whole, byte for byte.
//!
//! [`small_stdio_pipes`] is the other half, for tests: it makes a child's
//! stdout and stderr pipes small *before the child's first instruction*, so
//! a test reproduces the reduced pipe of a loaded host on any host.

use std::io::{self, Read};
use std::ops::{Deref, DerefMut};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// How much of each stream [`DrainedChild`] keeps by default (the end of
/// it). Far more than a log excerpt needs, far less than a runaway child
/// can write.
pub const DEFAULT_KEEP: usize = 8 << 20;

/// How long, after a child has exited, [`DrainedChild`] waits for the end
/// of its streams. A grandchild that inherited the pipe keeps it open past
/// the child's exit; what was read by then is returned and the reader
/// thread finishes on its own when the pipe finally closes.
const EOF_GRACE: Duration = Duration::from_secs(10);

/// The same wait after the child was killed: it has no more to say, and a
/// grandchild that outlived it (a shell's `sleep`) must not stall the
/// report of why the child was killed.
const KILLED_EOF_GRACE: Duration = Duration::from_secs(1);

/// How often [`DrainedChild::wait_timeout`] looks at the child.
const POLL: Duration = Duration::from_millis(10);

/// One stream's buffer, filled by its reader thread.
struct Tail {
    state: Mutex<TailState>,
    eof: Condvar,
    keep: usize,
}

struct TailState {
    buf: Vec<u8>,
    eof: bool,
}

impl Tail {
    fn start(mut pipe: impl Read + Send + 'static, keep: usize) -> Arc<Tail> {
        let tail = Arc::new(Tail {
            state: Mutex::new(TailState {
                buf: Vec::new(),
                eof: false,
            }),
            eof: Condvar::new(),
            keep,
        });
        let sink = Arc::clone(&tail);
        std::thread::Builder::new()
            .name("child-drain".into())
            .spawn(move || {
                let mut chunk = vec![0u8; 64 << 10];
                loop {
                    match pipe.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => sink.push(&chunk[..n]),
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(_) => break,
                    }
                }
                let mut st = sink.state.lock().unwrap_or_else(|e| e.into_inner());
                st.eof = true;
                sink.eof.notify_all();
            })
            .expect("spawning a pipe reader thread");
        tail
    }

    fn push(&self, data: &[u8]) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.buf.extend_from_slice(data);
        // Trim in batches, not on every read.
        if st.buf.len() > self.keep.saturating_mul(2) {
            let cut = st.buf.len() - self.keep;
            st.buf.drain(..cut);
        }
    }

    /// What has been read, once the stream has ended or `until` has passed.
    fn collect(&self, until: Instant) -> Vec<u8> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while !st.eof {
            let Some(left) = until.checked_duration_since(Instant::now()) else {
                break;
            };
            st = self
                .eof
                .wait_timeout(st, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        let from = st.buf.len().saturating_sub(self.keep);
        st.buf[from..].to_vec()
    }
}

/// A [`Child`] whose piped stdout and stderr are read, to the end, by
/// threads of their own from the moment it is created (`usize::MAX` as
/// `keep` keeps everything, for output that is parsed whole). It derefs to the
/// [`Child`] (`id`, `try_wait`, `kill`, `stdin`...); `stdout` and `stderr`
/// of that child are already taken.
pub struct DrainedChild {
    child: Child,
    out: Option<Arc<Tail>>,
    err: Option<Arc<Tail>>,
}

/// The result of [`DrainedChild::finish`].
#[derive(Debug)]
pub struct Finished {
    /// Everything the child wrote (the last [`DEFAULT_KEEP`] bytes of each
    /// stream), and how it ended: the child was killed when `timed_out`.
    pub output: Output,
    /// The deadline passed first, and the child was killed.
    pub timed_out: bool,
}

impl DrainedChild {
    /// Spawn `cmd` with stdout and stderr piped and drained.
    pub fn spawn(cmd: &mut Command) -> io::Result<DrainedChild> {
        Ok(DrainedChild::new(
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?,
        ))
    }

    /// Drain the piped stdout and stderr of an already spawned `child` (a
    /// stream that is not piped is simply empty), keeping the last
    /// [`DEFAULT_KEEP`] bytes of each.
    pub fn new(child: Child) -> DrainedChild {
        DrainedChild::with_keep(child, DEFAULT_KEEP)
    }

    /// [`DrainedChild::new`], keeping the last `keep` bytes of each stream.
    pub fn with_keep(mut child: Child, keep: usize) -> DrainedChild {
        let out = child.stdout.take().map(|p| Tail::start(p, keep));
        let err = child.stderr.take().map(|p| Tail::start(p, keep));
        DrainedChild { child, out, err }
    }

    /// Wait up to `within` for the child to exit: its status, or `None`
    /// while it still runs. The pipes keep draining either way.
    pub fn wait_timeout(&mut self, within: Duration) -> io::Result<Option<ExitStatus>> {
        let until = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= until {
                return Ok(None);
            }
            std::thread::sleep(POLL);
        }
    }

    /// What the child has written so far, without waiting for it to end.
    pub fn output_so_far(&self) -> (Vec<u8>, Vec<u8>) {
        let now = Instant::now();
        let read = |t: &Option<Arc<Tail>>| t.as_ref().map(|t| t.collect(now)).unwrap_or_default();
        (read(&self.out), read(&self.err))
    }

    /// [`Child::wait_with_output`]: wait for the child, then for the ends
    /// of its streams.
    pub fn wait_with_output(mut self) -> io::Result<Output> {
        let status = self.child.wait()?;
        Ok(self.collect(status, EOF_GRACE))
    }

    /// Wait up to `within` for the child; past that, kill and reap it. Its
    /// output either way (what it wrote before the kill, if it was).
    pub fn finish(mut self, within: Duration) -> io::Result<Finished> {
        let (status, timed_out) = match self.wait_timeout(within)? {
            Some(status) => (status, false),
            None => {
                // Already gone is fine: the kill is a no-op then.
                let _ = self.child.kill();
                (self.child.wait()?, true)
            }
        };
        Ok(Finished {
            output: self.collect(
                status,
                if timed_out {
                    KILLED_EOF_GRACE
                } else {
                    EOF_GRACE
                },
            ),
            timed_out,
        })
    }

    fn collect(self, status: ExitStatus, grace: Duration) -> Output {
        let until = Instant::now() + grace;
        let read = |t: Option<Arc<Tail>>| t.map(|t| t.collect(until)).unwrap_or_default();
        Output {
            status,
            stdout: read(self.out),
            stderr: read(self.err),
        }
    }
}

impl Deref for DrainedChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}

impl DerefMut for DrainedChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

/// For tests: make the stdout and stderr pipes of the child `cmd` spawns
/// `bytes` large (at least one page; Linux rounds up to a page) before the
/// child runs anything, whatever the host's pipe pressure. Only a pipe is
/// affected (a file or `/dev/null` is left alone), and only on Linux,
/// where `F_SETPIPE_SZ` exists; elsewhere it does nothing.
///
/// This is what a loaded Linux host does to every new pipe (8 KiB past
/// `pipe-user-pages-soft`), made deterministic: a child that writes more
/// than `bytes` blocks until the parent reads.
pub fn small_stdio_pipes(cmd: &mut Command, bytes: usize) -> &mut Command {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let bytes = bytes.min(libc::c_int::MAX as usize) as libc::c_int;
        // SAFETY: the closure runs between fork and exec and makes two
        // `fcntl` calls (async-signal-safe) on the descriptors std has just
        // set up as the child's stdout and stderr; it allocates nothing
        // and ignores their result (a non-pipe answers an error).
        unsafe {
            cmd.pre_exec(move || {
                libc::fcntl(1, libc::F_SETPIPE_SZ, bytes);
                libc::fcntl(2, libc::F_SETPIPE_SZ, bytes);
                Ok(())
            });
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = bytes;
    cmd
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn pipe_size(fd: &impl std::os::fd::AsRawFd) -> i32 {
        // SAFETY: F_GETPIPE_SZ on a descriptor this test owns.
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETPIPE_SZ) }
    }

    /// A shell that writes `n` bytes to stdout and `n` to stderr, then
    /// `ok` to stdout.
    fn chatty(n: usize) -> Command {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(format!(
            "head -c {n} /dev/zero | tr '\\0' o; head -c {n} /dev/zero | tr '\\0' e >&2; echo ok"
        ));
        cmd
    }

    #[test]
    fn small_stdio_pipes_shrinks_what_the_child_gets() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 1");
        small_stdio_pipes(&mut cmd, 4096)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        assert_eq!(pipe_size(child.stdout.as_ref().unwrap()), 4096);
        assert_eq!(pipe_size(child.stderr.as_ref().unwrap()), 4096);
        let _ = child.kill();
        let _ = child.wait();
    }

    /// The deadlock itself: a parent that waits for the exit before it
    /// reads blocks the child on its first full pipe.
    #[test]
    fn waiting_before_reading_deadlocks_a_child_on_a_small_pipe() {
        let mut cmd = chatty(20_000);
        small_stdio_pipes(&mut cmd, 4096)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        std::thread::sleep(Duration::from_millis(500));
        assert!(child.try_wait().unwrap().is_none(), "the child exited");
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn a_child_larger_than_its_pipes_is_drained_whole() {
        let mut cmd = chatty(300_000);
        small_stdio_pipes(&mut cmd, 4096);
        let child = DrainedChild::spawn(&mut cmd).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        assert_eq!(out.stdout.len(), 300_000 + 3);
        assert!(out.stdout.starts_with(b"ooo") && out.stdout.ends_with(b"ook\n".as_ref()));
        assert_eq!(out.stderr.len(), 300_000);
        assert!(out.stderr.iter().all(|b| *b == b'e'));
    }

    #[test]
    fn try_wait_sees_a_chatty_child_exit_before_its_output_is_read() {
        let mut cmd = chatty(300_000);
        small_stdio_pipes(&mut cmd, 4096);
        let mut child = DrainedChild::spawn(&mut cmd).unwrap();
        let status = child
            .wait_timeout(Duration::from_secs(30))
            .unwrap()
            .expect("a drained child exits");
        assert!(status.success());
        let out = child.wait_with_output().unwrap();
        assert_eq!(out.stdout.len(), 300_000 + 3);
    }

    #[test]
    fn finish_kills_a_child_past_its_deadline_and_keeps_what_it_wrote() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("echo before; echo oops >&2; sleep 60");
        let child = DrainedChild::spawn(&mut cmd).unwrap();
        let t = Instant::now();
        let done = child.finish(Duration::from_millis(500)).unwrap();
        assert!(done.timed_out);
        assert!(t.elapsed() < Duration::from_secs(20));
        assert_eq!(done.output.stdout, b"before\n");
        assert_eq!(done.output.stderr, b"oops\n");
        assert!(!done.output.status.success());
    }

    #[test]
    fn finish_returns_a_child_that_exits_in_time() {
        let mut cmd = chatty(100_000);
        small_stdio_pipes(&mut cmd, 4096);
        let done = DrainedChild::spawn(&mut cmd)
            .unwrap()
            .finish(Duration::from_secs(60))
            .unwrap();
        assert!(!done.timed_out);
        assert!(done.output.status.success());
        assert_eq!(done.output.stdout.len(), 100_000 + 3);
    }

    #[test]
    fn only_the_end_of_a_runaway_stream_is_kept() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("head -c 100000 /dev/zero | tr '\\0' a; echo END");
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let out = DrainedChild::with_keep(cmd.spawn().unwrap(), 1000)
            .wait_with_output()
            .unwrap();
        assert_eq!(out.stdout.len(), 1000);
        assert!(out.stdout.ends_with(b"aaaEND\n"));
    }

    #[test]
    fn output_so_far_is_readable_while_the_child_runs() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("echo first; sleep 60");
        let mut child = DrainedChild::spawn(&mut cmd).unwrap();
        let t = Instant::now();
        let seen = loop {
            let (out, _) = child.output_so_far();
            if !out.is_empty() {
                break out;
            }
            assert!(t.elapsed() < Duration::from_secs(20), "no output");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(seen, b"first\n");
        let _ = child.kill();
        let _ = child.wait();
    }
}
