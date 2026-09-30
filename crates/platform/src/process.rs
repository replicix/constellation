//! Facts about processes and threads on this host: the hostname, whether
//! a pid is alive and what state it is in (the `daemon.lock` takeover's
//! evidence), a process's supplementary groups (the permission check of a
//! create that found its name existing), this process's thread count and
//! memory budget (startup diagnostics, thread sizing), and asking a thread
//! for its backtrace (the stalled-request watchdog,
//! `CONSTELLATION_FUSE_STALL_BACKTRACE`, plan 31 §6.8).
//!
//! What a *decision* made from these facts should be (take a state dir
//! over, report a stall) is the caller's policy, not the host's: this
//! module only reports.

use std::io;

/// One thread, as [`Process::current_thread`] saw it: an id for people
/// (logs, `status`) and whatever this host needs to signal that thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ThreadRef {
    /// The OS thread id: the kernel tid on Linux, `pthread_threadid_np`
    /// on macOS.
    pub tid: u64,
    /// The signal target: the tid again on Linux (`tgkill`), the
    /// `pthread_t` on macOS (`pthread_kill`).
    pub(crate) handle: u64,
}

impl ThreadRef {
    /// A thread reference for hosts that cannot name threads (the stubs),
    /// and for tests.
    pub const fn unknown() -> ThreadRef {
        ThreadRef { tid: 0, handle: 0 }
    }
}

/// What the kernel says about one process, as far as the `daemon.lock`
/// takeover needs it (plan 30's EC2 campaign 6, finding B-1): the
/// thread-group leader's state and name, whether SIGKILL is pending on it,
/// and the same for every other thread of the group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProcessFacts {
    pub name: String,
    /// The state as the host prints it (Linux: `Z (zombie)`, `S
    /// (sleeping)`, ...), kept verbatim for messages.
    pub state: String,
    /// The thread-group leader has exited but the group has not been
    /// reaped (Linux state `Z`).
    pub zombie: bool,
    /// The process is dead and about to vanish (Linux state `X`).
    pub dead: bool,
    /// SIGKILL is pending on the leader or on the thread group.
    pub sigkill_pending: bool,
    /// Every thread but the leader.
    pub tasks: Vec<TaskFacts>,
}

/// One non-leader thread of a [`ProcessFacts`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskFacts {
    pub state: String,
    pub sigkill_pending: bool,
    /// The thread still has an address space (Linux: `VmSize` present in
    /// its status); a thread past `exit_mm` has none and can never return
    /// to user space.
    pub has_mm: bool,
}

pub trait Process: Send + Sync {
    /// This host's name, as `uname -n` prints it.
    fn hostname(&self) -> io::Result<String>;

    /// Whether `pid` still has an entry in the process table (a zombie
    /// does).
    fn is_alive(&self, pid: u32) -> bool;

    /// The kernel's view of `pid` (see [`ProcessFacts`]). The error names
    /// what could not be read.
    fn facts(&self, pid: u32) -> io::Result<ProcessFacts>;

    /// The supplementary group ids of process `pid`.
    fn supplementary_groups(&self, pid: u32) -> io::Result<Vec<u32>>;

    /// This process's effective `(uid, gid)`: the owner a file this
    /// process creates on its own behalf gets.
    fn effective_ids(&self) -> (u32, u32);

    /// How many threads this process has.
    fn thread_count(&self) -> Option<u64>;

    /// The memory this process may use: host RAM, capped by a
    /// container/cgroup limit when there is one.
    fn memory_budget(&self) -> Option<u64>;

    /// The calling thread.
    fn current_thread(&self) -> ThreadRef;

    /// Arrange for [`Process::request_backtrace`] to work: a signal
    /// handler that writes the receiving thread's backtrace to stderr
    /// between `=== <label>: backtrace of stalled thread tid=<tid> ===`
    /// and `=== end ===`. Process-wide and idempotent; the first label
    /// wins.
    fn enable_backtraces(&self, label: &'static str) -> io::Result<()>;

    /// Ask `thread` (of this process) to write its backtrace to stderr
    /// (after [`Process::enable_backtraces`]). A diagnostic: the capture
    /// in the handler allocates, which is fine for a thread parked in a
    /// futex wait, not for one inside the allocator.
    fn request_backtrace(&self, thread: ThreadRef) -> io::Result<()>;

    /// Freeze process `pid` (Unix `SIGSTOP`): it stays alive, with its
    /// mounts and memory, but runs nothing until [`Process::resume`]. The
    /// fault-injection harness's "unreachable holder". Hosts that cannot
    /// signal another process answer [`io::ErrorKind::Unsupported`].
    fn suspend(&self, _pid: u32) -> io::Result<()> {
        Err(crate::unsupported("suspending a process"))
    }

    /// Thaw a process [`Process::suspend`] froze (Unix `SIGCONT`).
    fn resume(&self, _pid: u32) -> io::Result<()> {
        Err(crate::unsupported("resuming a process"))
    }

    /// Kill process `pid` without giving it a chance to clean up (Unix
    /// `SIGKILL`): a crash, for the harness's kill-and-recover scenarios.
    fn kill(&self, _pid: u32) -> io::Result<()> {
        Err(crate::unsupported("killing a process"))
    }
}
