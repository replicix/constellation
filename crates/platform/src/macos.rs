//! macOS: the baseline plan 34 builds on (plan 34 M2 "completing
//! `platform::macos`").
//!
//! Real here: XDG-style dirs, `flock(2)`, fork-based daemonizing (as the
//! pre-plan-31 code did on macOS), the hostname and ids, thread count
//! (`proc_pidinfo`), memory size (`hw.memsize`), per-thread backtrace
//! signals (`pthread_kill`), `F_PUNCHHOLE`/`F_PREALLOCATE`/`F_FULLFSYNC`,
//! the mount table (`getmntinfo`) and `unmount(2)`.
//!
//! Left to plan 34 M2, answering `Unsupported` (which every caller treats
//! as "fact unknown", the pre-plan-31 behaviour on macOS where the `/proc`
//! reads simply failed): libproc process facts for the `daemon.lock`
//! takeover and supplementary groups (`getgrouplist`), `posix_spawn`
//! daemonizing, the Keychain secret store, and FUSE connection controls
//! (the NFS frontend has none). There is no `/proc/locks`, so a lock's
//! holder is never known and the takeover falls back to `daemon.pid`.

use std::ffi::{CStr, CString};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::fs::FsPrimitives;
use crate::lifecycle::ManualLifecycle;
use crate::lock::FileLock;
use crate::mounts::{MountEntry, MountTable, UnmountMode};
use crate::process::{Process, ProcessFacts, ThreadRef};
use crate::secrets::FileSecretStore;
use crate::unix::{ForkDaemon, UnixFileLock, XdgDirs};
use crate::{unsupported, HostServices};

pub(crate) fn host_services() -> HostServices {
    let dirs = Arc::new(XdgDirs);
    let file_lock = file_lock();
    HostServices {
        dirs: dirs.clone(),
        process: Arc::new(MacProcess),
        daemon: Arc::new(ForkDaemon),
        file_lock: file_lock.clone(),
        fs: Arc::new(MacFs),
        secrets: Arc::new(FileSecretStore::in_config_dir(dirs, file_lock)),
        lifecycle: Arc::new(ManualLifecycle::new()),
        mounts: Arc::new(MacMounts),
    }
}

pub(crate) fn file_lock() -> Arc<dyn FileLock> {
    Arc::new(UnixFileLock { holder: |_| None })
}

fn off_t(n: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(n).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset {n} out of range"),
        )
    })
}

// -------------------------------------------------------------- process

struct MacProcess;

fn thread_id() -> u64 {
    let mut tid: u64 = 0;
    // SAFETY: a null thread means the calling thread; `tid` is a valid
    // out-pointer.
    unsafe { libc::pthread_threadid_np(0 as libc::pthread_t, &mut tid) };
    tid
}

impl Process for MacProcess {
    fn hostname(&self) -> io::Result<String> {
        crate::unix::hostname()
    }

    fn is_alive(&self, pid: u32) -> bool {
        crate::unix::kill_probe(pid)
    }

    fn facts(&self, _pid: u32) -> io::Result<ProcessFacts> {
        Err(unsupported(
            "process facts without /proc (libproc: plan 34 M2)",
        ))
    }

    fn supplementary_groups(&self, _pid: u32) -> io::Result<Vec<u32>> {
        Err(unsupported("another process's groups (plan 34 M2)"))
    }

    fn lineage(&self, _pid: u32) -> io::Result<crate::Lineage> {
        Err(unsupported("another process's parent (plan 34 M2)"))
    }

    fn effective_ids(&self) -> (u32, u32) {
        crate::unix::effective_ids()
    }

    fn thread_count(&self) -> Option<u64> {
        // SAFETY: an all-zero `proc_taskinfo` is valid; the buffer size
        // passed is its own.
        let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        // SAFETY: `info` is valid for `size` bytes.
        let got = unsafe {
            libc::proc_pidinfo(
                std::process::id() as libc::c_int,
                libc::PROC_PIDTASKINFO,
                0,
                (&mut info as *mut libc::proc_taskinfo).cast(),
                size,
            )
        };
        (got == size).then(|| info.pti_threadnum.max(0) as u64)
    }

    fn memory_budget(&self) -> Option<u64> {
        let mut bytes: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        // SAFETY: `hw.memsize` is a u64; the out-pointer and its length
        // match.
        let rc = unsafe {
            libc::sysctlbyname(
                c"hw.memsize".as_ptr(),
                (&mut bytes as *mut u64).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        (rc == 0 && bytes > 0).then_some(bytes)
    }

    fn current_thread(&self) -> ThreadRef {
        // SAFETY: pthread_self cannot fail.
        let handle = unsafe { libc::pthread_self() } as usize as u64;
        ThreadRef {
            tid: thread_id(),
            handle,
        }
    }

    fn enable_backtraces(&self, label: &'static str) -> io::Result<()> {
        crate::unix::enable_backtraces(label, thread_id)
    }

    fn request_backtrace(&self, thread: ThreadRef) -> io::Result<()> {
        // The target is a watched thread that is still registered, so
        // still running (FUSE/NFS worker threads live as long as their
        // session); a `pthread_t` of an exited thread would be undefined.
        // SAFETY: see above.
        let rc =
            unsafe { libc::pthread_kill(thread.handle as usize as libc::pthread_t, libc::SIGUSR2) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
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

struct MacFs;

fn fcntl_ptr<T>(file: &File, cmd: libc::c_int, arg: &mut T) -> io::Result<()> {
    // SAFETY: `file` owns a valid fd; `arg` is the struct `cmd` expects.
    if unsafe { libc::fcntl(file.as_raw_fd(), cmd, arg as *mut T) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl FsPrimitives for MacFs {
    fn punch_hole(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
        // APFS supports it; HFS+ answers ENOTSUP (the caller ignores it).
        let mut hole = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: off_t(offset)?,
            fp_length: off_t(len)?,
        };
        fcntl_ptr(file, libc::F_PUNCHHOLE, &mut hole)
    }

    fn preallocate(&self, file: &File, offset: u64, len: u64, keep_size: bool) -> io::Result<()> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range overflows"))?;
        let size = file.metadata()?.len();
        if end > size {
            // F_PREALLOCATE allocates relative to the physical end of
            // file (F_PEOFPOSMODE): ask for the part past it.
            let mut store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATEALL,
                fst_posmode: libc::F_PEOFPOSMODE,
                fst_offset: 0,
                fst_length: off_t(end - size)?,
                fst_bytesalloc: 0,
            };
            fcntl_ptr(file, libc::F_PREALLOCATE, &mut store)?;
            if !keep_size {
                file.set_len(end)?;
            }
        }
        Ok(())
    }

    fn full_fsync(&self, file: &File) -> io::Result<()> {
        // SAFETY: `file` owns a valid fd.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == -1 {
            // Filesystems without it (some network/FUSE ones) still take
            // a plain fsync, which is what std falls back to as well.
            return file.sync_all();
        }
        Ok(())
    }

    fn drop_cache(&self, _file: &File, _offset: u64, _len: u64) -> io::Result<()> {
        Err(unsupported("dropping cached pages (no posix_fadvise)"))
    }
}

// --------------------------------------------------------------- mounts

struct MacMounts;

fn c_field(field: &[libc::c_char]) -> String {
    // SAFETY: getmntinfo's name fields are NUL-terminated within their
    // arrays.
    unsafe { CStr::from_ptr(field.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

impl MountTable for MacMounts {
    fn list(&self) -> io::Result<Vec<MountEntry>> {
        let mut buf: *mut libc::statfs = std::ptr::null_mut();
        // SAFETY: getmntinfo points `buf` at a static buffer of `n`
        // entries it owns (valid until the next call on this thread).
        let n = unsafe { libc::getmntinfo(&mut buf, libc::MNT_NOWAIT) };
        if n <= 0 || buf.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: see above.
        let entries = unsafe { std::slice::from_raw_parts(buf, n as usize) };
        Ok(entries
            .iter()
            .map(|s| MountEntry {
                mountpoint: PathBuf::from(c_field(&s.f_mntonname)),
                fstype: c_field(&s.f_fstypename),
                source: c_field(&s.f_mntfromname),
                device: None,
            })
            .collect())
    }

    fn unmount(&self, path: &Path, mode: UnmountMode) -> io::Result<()> {
        let c = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))?;
        let flags = match mode {
            UnmountMode::Normal => 0,
            UnmountMode::Lazy | UnmountMode::Force => libc::MNT_FORCE,
        };
        // SAFETY: `c` is a valid NUL-terminated path for the call.
        if unsafe { libc::unmount(c.as_ptr(), flags) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn fuse_waiting(&self, _connection: u32) -> io::Result<u64> {
        Err(unsupported("FUSE connection control"))
    }

    fn abort_fuse(&self, _connection: u32) -> io::Result<()> {
        Err(unsupported("FUSE connection control"))
    }
}
