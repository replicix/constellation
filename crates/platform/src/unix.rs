//! What Linux and macOS do identically: XDG-style dirs, `flock(2)`,
//! `fork`-based daemonizing, `gethostname`, the effective ids, the
//! backtrace signal handler, and the extended-attribute calls.

use std::ffi::{CString, OsString};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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

/// Frames kept per captured backtrace.
const BT_FRAMES: usize = 64;
/// Captures that can wait for the writer thread; a burst beyond this is dropped.
const BT_SLOTS: usize = 16;

const SLOT_FREE: u32 = 0;
const SLOT_FILLING: u32 = 1;
const SLOT_READY: u32 = 2;

/// One preallocated capture: the handler fills it with raw instruction
/// pointers, the writer thread formats it.
struct BtSlot {
    state: AtomicU32,
    tid: AtomicU64,
    len: AtomicUsize,
    ips: [AtomicUsize; BT_FRAMES],
}

#[allow(clippy::declare_interior_mutable_const)]
const BT_SLOT_INIT: BtSlot = BtSlot {
    state: AtomicU32::new(SLOT_FREE),
    tid: AtomicU64::new(0),
    len: AtomicUsize::new(0),
    ips: [const { AtomicUsize::new(0) }; BT_FRAMES],
};
static BT_RING: [BtSlot; BT_SLOTS] = [BT_SLOT_INIT; BT_SLOTS];
/// Write end of the self-pipe the handler pokes (non-blocking); -1 until set.
static BT_PIPE_W: AtomicI32 = AtomicI32::new(-1);
/// Captures dropped because every slot was busy.
static BT_DROPPED: AtomicU64 = AtomicU64::new(0);
/// Dumps the writer thread has logged (read by tests).
#[cfg(test)]
pub(crate) static BT_EMITTED: AtomicU64 = AtomicU64::new(0);
/// `(tid, frame count)` of every dump the writer logged (read by tests).
#[cfg(test)]
pub(crate) static BT_DUMPS: std::sync::Mutex<Vec<(u64, usize)>> = std::sync::Mutex::new(Vec::new());
/// Allocations made on a thread while it ran the handler (tests assert 0).
#[cfg(test)]
pub(crate) static BT_HANDLER_ALLOCS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
thread_local! {
    /// Set while this thread is inside the handler. Const-initialised and
    /// needing no destructor, so the test allocator can read it safely.
    pub(crate) static BT_IN_HANDLER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
/// Test allocator: counts allocations made while the calling thread is
/// inside the backtrace handler.
#[cfg(test)]
struct CountingAlloc;
#[cfg(test)]
impl CountingAlloc {
    fn note() {
        if BT_IN_HANDLER.try_with(|c| c.get()).unwrap_or(false) {
            BT_HANDLER_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
    }
}
// SAFETY: every method defers to the system allocator unchanged.
#[cfg(test)]
unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        Self::note();
        std::alloc::System.alloc(l)
    }
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        Self::note();
        std::alloc::System.alloc_zeroed(l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, n: usize) -> *mut u8 {
        Self::note();
        std::alloc::System.realloc(p, l, n)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        std::alloc::System.dealloc(p, l)
    }
}
#[cfg(test)]
#[global_allocator]
static TEST_ALLOC: CountingAlloc = CountingAlloc;
static BT_TID: OnceLock<fn() -> u64> = OnceLock::new();

extern "C" {
    fn _Unwind_Backtrace(
        trace: extern "C" fn(*mut libc::c_void, *mut libc::c_void) -> libc::c_int,
        arg: *mut libc::c_void,
    ) -> libc::c_int;
    fn _Unwind_GetIP(ctx: *mut libc::c_void) -> usize;
}

struct Walk {
    slot: &'static BtSlot,
    n: usize,
}

extern "C" fn walk_frame(ctx: *mut libc::c_void, arg: *mut libc::c_void) -> libc::c_int {
    // SAFETY: `arg` is the `Walk` on the handler's stack, passed below.
    let w = unsafe { &mut *arg.cast::<Walk>() };
    if w.n >= BT_FRAMES {
        return 5; // _URC_END_OF_STACK: stop
    }
    // SAFETY: `ctx` is the unwinder's live context for this callback.
    w.slot.ips[w.n].store(unsafe { _Unwind_GetIP(ctx) }, Ordering::Relaxed);
    w.n += 1;
    0 // _URC_NO_REASON: continue
}

/// The `SIGUSR2` handler. Async-signal-safe by construction: no heap, no
/// locks, no formatting. It claims a preallocated slot with a CAS, walks the
/// stack into it with the unwinder (which allocates nothing; the first
/// use, which loads the unwinder's lazy state, is done at install time), and
/// pokes the self-pipe with `write(2)`. `errno` is preserved.
extern "C" fn on_sigusr2(_: libc::c_int) {
    // SAFETY: errno location is thread-local and always valid.
    let saved = unsafe { *errno_location() };
    #[cfg(test)]
    BT_IN_HANDLER.with(|c| c.set(true));
    on_sigusr2_body();
    #[cfg(test)]
    BT_IN_HANDLER.with(|c| c.set(false));
    // SAFETY: as above.
    unsafe { *errno_location() = saved };
}

fn on_sigusr2_body() {
    for slot in BT_RING.iter() {
        if slot
            .state
            .compare_exchange(
                SLOT_FREE,
                SLOT_FILLING,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            continue;
        }
        let tid = BT_TID.get().map(|f| f()).unwrap_or(0);
        slot.tid.store(tid, Ordering::Relaxed);
        let mut w = Walk { slot, n: 0 };
        // SAFETY: the callback only touches `w` and atomics.
        unsafe { _Unwind_Backtrace(walk_frame, (&mut w as *mut Walk).cast()) };
        slot.len.store(w.n, Ordering::Relaxed);
        slot.state.store(SLOT_READY, Ordering::Release);
        let fd = BT_PIPE_W.load(Ordering::Relaxed);
        if fd >= 0 {
            // A full pipe already holds a wake-up: ignore the result.
            // SAFETY: write(2) of one byte from a valid buffer.
            unsafe { libc::write(fd, [1u8].as_ptr().cast(), 1) };
        }
        return;
    }
    BT_DROPPED.fetch_add(1, Ordering::Relaxed);
}

#[cfg(target_os = "linux")]
unsafe fn errno_location() -> *mut libc::c_int {
    libc::__errno_location()
}
#[cfg(not(target_os = "linux"))]
unsafe fn errno_location() -> *mut libc::c_int {
    libc::__error()
}

/// Write all of `bytes` to `fd`, retrying on `EINTR`, `EAGAIN` and partial
/// writes; gives up silently on any other error (the log is a diagnostic).
fn write_all_fd(fd: libc::c_int, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        // SAFETY: write(2) from a valid buffer.
        let r = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if r > 0 {
            bytes = &bytes[r as usize..];
            continue;
        }
        match io::Error::last_os_error().kind() {
            io::ErrorKind::Interrupted => {}
            io::ErrorKind::WouldBlock => std::thread::sleep(std::time::Duration::from_millis(1)),
            _ => return,
        }
    }
}

/// Format one captured slot (on the writer thread, where allocating is fine)
/// and write it to stderr. Frames show the module-relative address and the
/// dynamic symbol when `dladdr` knows one. The addresses are *return*
/// addresses (except the frame the signal interrupted): give `addr2line -e
/// <binary>` the address minus 1 to get the call's own line. A stripped
/// release binary (`strip = true`) has no symbols for `dladdr` to name and
/// `addr2line` needs an unstripped build of the same commit.
fn emit_slot(label: &str, slot: &BtSlot) -> String {
    use std::fmt::Write as _;
    let tid = slot.tid.load(Ordering::Relaxed);
    let n = slot.len.load(Ordering::Relaxed).min(BT_FRAMES);
    let mut out = format!("\n=== {label}: backtrace of stalled thread tid={tid} ===\n");
    for i in 0..n {
        let ip = slot.ips[i].load(Ordering::Relaxed);
        // SAFETY: a zeroed Dl_info is valid; dladdr fills it for any address.
        let mut info: libc::Dl_info = unsafe { std::mem::zeroed() };
        // SAFETY: dladdr with a valid out-pointer.
        let found = unsafe { libc::dladdr(ip as *const libc::c_void, &mut info) } != 0;
        let _ = write!(out, "{i:3}: {ip:#018x}");
        if found {
            if !info.dli_fname.is_null() {
                // SAFETY: dladdr's NUL-terminated module path.
                let f = unsafe { std::ffi::CStr::from_ptr(info.dli_fname) }.to_string_lossy();
                let _ = write!(out, " {f}+{:#x}", ip.wrapping_sub(info.dli_fbase as usize));
            }
            if !info.dli_sname.is_null() {
                // SAFETY: dladdr's NUL-terminated symbol name.
                let s = unsafe { std::ffi::CStr::from_ptr(info.dli_sname) }.to_string_lossy();
                let _ = write!(out, " ({s})");
            }
        }
        out.push('\n');
    }
    out.push_str("=== end ===\n");
    out
}

/// Install the `SIGUSR2` handler that dumps the receiving thread's backtrace
/// to stderr (the daemon log).
///
/// The handler is async-signal-safe (see [`on_sigusr2`]): a stalled thread
/// can be anywhere, including inside `malloc` (a stopped and thawed process),
/// where an allocating handler corrupted the heap. It records raw frames into
/// a preallocated ring and wakes a writer thread through a self-pipe; the
/// writer formats and logs. Trade-offs: frames are addresses plus `dladdr`
/// names (resolve with `addr2line`), not a symbolized `std::backtrace`; the
/// unwinder takes the loader lock on glibc older than 2.35, so a thread
/// interrupted inside `dlopen` could deadlock; bursts beyond 16 pending
/// captures are dropped. Static musl builds use LLVM libunwind, which does not
/// step past the signal trampoline: measured on the static musl target, a dump
/// holds only the handler and the trampoline (two raw frames, no names), so
/// there the dump says a thread stalled but not where (not a regression:
/// the old `std::backtrace` used the same unwinder). `SIGUSR2` is used by
/// nothing else in the daemon.
pub(crate) fn enable_backtraces(label: &'static str, tid: fn() -> u64) -> io::Result<()> {
    static INSTALLED: OnceLock<io::Result<()>> = OnceLock::new();
    BT_TID.get_or_init(|| tid);

    let installed = INSTALLED.get_or_init(|| {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: pipe(2) into a two-element array.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        for fd in fds {
            // SAFETY: fcntl on descriptors just created. The write end is
            // non-blocking so the handler never waits; both are close-on-exec.
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                if fd == fds[1] {
                    libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                }
            }
        }
        let read_fd = fds[0];
        BT_PIPE_W.store(fds[1], Ordering::Relaxed);
        // Warm the unwinder (loads its lazy state) outside any handler.
        let mut warm = Walk {
            slot: &BT_RING[0],
            n: 0,
        };
        // SAFETY: the callback only touches `warm` and atomics; slot 0 is
        // unused until the handler is installed below.
        unsafe { _Unwind_Backtrace(walk_frame, (&mut warm as *mut Walk).cast()) };
        let spawned = std::thread::Builder::new()
            .name("backtrace-writer".into())
            .spawn(move || {
                let mut buf = [0u8; 64];
                loop {
                    // SAFETY: read(2) into a valid buffer.
                    let r = unsafe { libc::read(read_fd, buf.as_mut_ptr().cast(), buf.len()) };
                    if r == 0 {
                        // EOF: every write end is closed, nothing more comes.
                        write_all_fd(
                            2,
                            format!("=== {label}: backtrace pipe closed ===\n").as_bytes(),
                        );
                        return;
                    }
                    if r < 0 {
                        if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        let e = io::Error::last_os_error();
                        write_all_fd(
                            2,
                            format!("=== {label}: backtrace writer stops: {e} ===\n").as_bytes(),
                        );
                        return;
                    }
                    for slot in BT_RING.iter() {
                        if slot.state.load(Ordering::Acquire) == SLOT_READY {
                            let text = emit_slot(label, slot);
                            #[cfg(test)]
                            {
                                let tid = slot.tid.load(Ordering::Relaxed);
                                let n = slot.len.load(Ordering::Relaxed);
                                BT_DUMPS.lock().unwrap().push((tid, n));
                            }
                            slot.state.store(SLOT_FREE, Ordering::Release);
                            #[cfg(test)]
                            BT_EMITTED.fetch_add(1, Ordering::Relaxed);
                            write_all_fd(2, text.as_bytes());
                        }
                    }
                    let dropped = BT_DROPPED.swap(0, Ordering::Relaxed);
                    if dropped > 0 {
                        let t =
                            format!("=== {label}: {dropped} backtrace request(s) dropped ===\n");
                        write_all_fd(2, t.as_bytes());
                    }
                }
            });
        spawned?;
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

// ------------------------------------------------------------ xattrs
//
// The same four calls on both hosts; macOS's take a `position` (resource
// forks; 0 for everything else) and an `options` word that Linux's do not.
// The `sys_*` shims are that one difference.

fn cpath(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))
}

fn cname(name: &str) -> io::Result<CString> {
    CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains a NUL byte"))
}

/// # Safety
/// `path` and `name` are NUL-terminated; `value` is valid for `size` bytes.
#[cfg(target_os = "linux")]
unsafe fn sys_setxattr(
    path: *const libc::c_char,
    name: *const libc::c_char,
    value: *const libc::c_void,
    size: usize,
) -> libc::c_int {
    unsafe { libc::setxattr(path, name, value, size, 0) }
}

/// # Safety
/// As the Linux one.
#[cfg(target_os = "macos")]
unsafe fn sys_setxattr(
    path: *const libc::c_char,
    name: *const libc::c_char,
    value: *const libc::c_void,
    size: usize,
) -> libc::c_int {
    unsafe { libc::setxattr(path, name, value, size, 0, 0) }
}

/// # Safety
/// `path` and `name` are NUL-terminated; `value` is null with `size` 0, or
/// valid for writes of `size` bytes.
#[cfg(target_os = "linux")]
unsafe fn sys_getxattr(
    path: *const libc::c_char,
    name: *const libc::c_char,
    value: *mut libc::c_void,
    size: usize,
) -> libc::ssize_t {
    unsafe { libc::getxattr(path, name, value, size) }
}

/// # Safety
/// As the Linux one.
#[cfg(target_os = "macos")]
unsafe fn sys_getxattr(
    path: *const libc::c_char,
    name: *const libc::c_char,
    value: *mut libc::c_void,
    size: usize,
) -> libc::ssize_t {
    unsafe { libc::getxattr(path, name, value, size, 0, 0) }
}

/// # Safety
/// `path` is NUL-terminated; `list` is null with `size` 0, or valid for
/// writes of `size` bytes.
#[cfg(target_os = "linux")]
unsafe fn sys_listxattr(
    path: *const libc::c_char,
    list: *mut libc::c_char,
    size: usize,
) -> libc::ssize_t {
    unsafe { libc::listxattr(path, list, size) }
}

/// # Safety
/// As the Linux one.
#[cfg(target_os = "macos")]
unsafe fn sys_listxattr(
    path: *const libc::c_char,
    list: *mut libc::c_char,
    size: usize,
) -> libc::ssize_t {
    unsafe { libc::listxattr(path, list, size, 0) }
}

/// # Safety
/// `path` and `name` are NUL-terminated.
#[cfg(target_os = "linux")]
unsafe fn sys_removexattr(path: *const libc::c_char, name: *const libc::c_char) -> libc::c_int {
    unsafe { libc::removexattr(path, name) }
}

/// # Safety
/// As the Linux one.
#[cfg(target_os = "macos")]
unsafe fn sys_removexattr(path: *const libc::c_char, name: *const libc::c_char) -> libc::c_int {
    unsafe { libc::removexattr(path, name, 0) }
}

pub(crate) fn set_xattr(path: &Path, name: &str, value: &[u8]) -> io::Result<()> {
    let (path, name) = (cpath(path)?, cname(name)?);
    // SAFETY: valid NUL-terminated strings and a live buffer.
    let rc = unsafe {
        sys_setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Read a variable-length xattr result: ask for its size, then for the
/// bytes, asking again if it grew in between (`ERANGE`).
fn read_sized(
    mut call: impl FnMut(*mut libc::c_void, usize) -> libc::ssize_t,
) -> io::Result<Vec<u8>> {
    loop {
        let size = call(std::ptr::null_mut(), 0);
        if size < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut buf = vec![0u8; size as usize];
        let read = call(buf.as_mut_ptr().cast(), buf.len());
        if read < 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::ERANGE) {
                continue;
            }
            return Err(e);
        }
        buf.truncate(read as usize);
        return Ok(buf);
    }
}

pub(crate) fn get_xattr(path: &Path, name: &str) -> io::Result<Vec<u8>> {
    let (path, name) = (cpath(path)?, cname(name)?);
    // SAFETY: valid NUL-terminated strings; `read_sized` passes a null
    // buffer with size 0, or a live one of the size it gives.
    read_sized(|buf, len| unsafe { sys_getxattr(path.as_ptr(), name.as_ptr(), buf, len) })
}

pub(crate) fn list_xattr(path: &Path) -> io::Result<Vec<String>> {
    let path = cpath(path)?;
    // SAFETY: as in `get_xattr`.
    let names = read_sized(|buf, len| unsafe { sys_listxattr(path.as_ptr(), buf.cast(), len) })?;
    Ok(names
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect())
}

pub(crate) fn remove_xattr(path: &Path, name: &str) -> io::Result<()> {
    let (path, name) = (cpath(path)?, cname(name)?);
    // SAFETY: valid NUL-terminated strings.
    if unsafe { sys_removexattr(path.as_ptr(), name.as_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
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
    fn xattrs_round_trip_and_report_a_missing_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"x").unwrap();
        match set_xattr(&path, "user.constellation.test", b"blue") {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::ENOTSUP) => {
                eprintln!("SKIPPED (not run): this filesystem has no extended attributes ({e})");
                return;
            }
            Err(e) => panic!("set_xattr: {e}"),
        }
        assert_eq!(
            get_xattr(&path, "user.constellation.test").unwrap(),
            b"blue"
        );
        // A value longer than any first guess at a buffer, an empty one,
        // and a replacement.
        let big = vec![7u8; 20_000];
        set_xattr(&path, "user.constellation.big", &big).unwrap();
        set_xattr(&path, "user.constellation.empty", b"").unwrap();
        set_xattr(&path, "user.constellation.test", b"red").unwrap();
        assert_eq!(get_xattr(&path, "user.constellation.big").unwrap(), big);
        assert_eq!(get_xattr(&path, "user.constellation.empty").unwrap(), b"");
        assert_eq!(get_xattr(&path, "user.constellation.test").unwrap(), b"red");
        let mut names = list_xattr(&path).unwrap();
        names.retain(|n| n.starts_with("user.constellation."));
        names.sort();
        assert_eq!(
            names,
            [
                "user.constellation.big",
                "user.constellation.empty",
                "user.constellation.test"
            ]
        );
        remove_xattr(&path, "user.constellation.test").unwrap();
        // The documented contract the harness's NoData checks rely on:
        // ENODATA on Linux, ENOATTR on macOS, both `Code::NoData`.
        for e in [
            get_xattr(&path, "user.constellation.test").unwrap_err(),
            remove_xattr(&path, "user.constellation.test").unwrap_err(),
        ] {
            assert_eq!(
                constellation_types::Code::from_io_error(&e),
                constellation_types::Code::NoData,
                "{e}"
            );
        }
        // Names and paths with a NUL are refused before the syscall.
        assert_eq!(
            set_xattr(&path, "user.a\0b", b"v").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
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
