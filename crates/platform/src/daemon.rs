//! Detaching into the background (plan 21 step 5's JuiceFS-style
//! daemonization).
//!
//! The mechanism — fork, new session, stdio to the log, a status pipe back
//! to the invoking process — is the host's; the protocol spoken over the
//! pipe (one verdict line) and what the invoking process does with it stay
//! with the caller (`crates/cli/src/daemonize.rs`).
//!
//! macOS forks too for now (as the pre-plan-31 code did); plan 34 M2
//! replaces that with `posix_spawn` of the current executable, because the
//! pre-fork S3/TLS check initialises Security.framework, which does not
//! survive a fork without exec. The pipe protocol stays the same.

use std::fs::File;
use std::io;
use std::path::Path;

/// Which side of the detach this process is on.
#[derive(Debug)]
pub enum Detached {
    /// The invoking process, holding the read end of the status pipe. It
    /// reads the child's verdict and exits; before the verdict arrives,
    /// EOF (every write end closed) means the child died.
    Parent(File),
    /// The detached child: a new session with no controlling terminal,
    /// stdout/stderr appended to the log, holding the write end of the
    /// status pipe.
    Child(File),
}

pub trait Daemon: Send + Sync {
    /// Detach, with stdout/stderr of the detached process appended to
    /// `log` (created if missing). Both pipe ends are close-on-exec: a
    /// process the daemon later executes (the zombie reaper) must not keep
    /// the parent waiting on a write end it inherited.
    ///
    /// # Safety
    ///
    /// On hosts that fork, the calling process must be single-threaded:
    /// the child inherits only the calling thread, and any lock another
    /// thread held (the allocator's, a runtime's) stays held forever. Call
    /// before starting any runtime, pool or background thread.
    unsafe fn detach(&self, log: &Path) -> io::Result<Detached>;
}
