//! The kernel-fix probes (`super`'s module doc): a private mount of
//! [`ProbeVfs`], an in-memory filesystem of one file (`f`, node 2) and one
//! directory (`d`, node 3) that can hold a `WRITE`'s reply and a
//! directory `GETATTR`'s reply on the probe's signal.

use super::{set_probed, KernelFix, Verdict};
use constellation_types::Code;
use constellation_vfs::{
    Attr, DirSink, Durability, Entry, FallocateMode, Fh, FileKind, Ino, LockOwner, LockRange,
    LockSpec, LockStatus, Name, OpCtx, OpenFlags, OpenOwner, Opened, ReadData, RenameFlags,
    Renamed, Responder, SeekWhence, SetAttr, SetXattrFlags, StatFs, Vfs, VfsError, WriteData,
    XattrName, XattrNameBuf, ROOT_INO,
};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Once};
use std::time::{Duration, Instant};

const FILE: Ino = 2;
const DIR: Ino = 3;

/// How long a held reply waits for the probe's release at most (a probe
/// thread that died must not wedge the probe mount).
const HOLD_MAX: Duration = Duration::from_secs(5);

/// An exit gives a running probe this long to end by itself before it is
/// aborted, and then this long to unwind.
const EXIT_GRACE: Duration = Duration::from_millis(200);
const EXIT_UNWIND: Duration = Duration::from_secs(10);

/// How long a notification may take to count as having waited for the
/// held `WRITE`.
const WAITED: Duration = Duration::from_secs(1);

/// The probe directory's attribute TTL: long enough that a held reply the
/// kernel cached cannot expire before the probe reads it back.
const DIR_TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct State {
    /// `d`'s subdirectories.
    children: Vec<Vec<u8>>,
    /// The next `WRITE` is held until `release_write`.
    hold_write: bool,
    write_held: bool,
    release_write: bool,
    /// The next `GETATTR` of `d` is held until `release_getattr`.
    hold_getattr: bool,
    getattr_held: bool,
    release_getattr: bool,
    /// [`abort`]: nothing is held from now on, and every wait ends.
    aborted: bool,
}

/// The probe's filesystem (module doc).
#[derive(Default)]
pub(crate) struct ProbeVfs {
    state: Mutex<State>,
    cv: Condvar,
}

fn attr(ino: Ino, kind: FileKind, nlink: u32, size: u64) -> Attr {
    Attr {
        ino,
        kind,
        size,
        blocks: size.div_ceil(512),
        mode: if kind == FileKind::Dir { 0o755 } else { 0o644 },
        nlink,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: Default::default(),
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        blksize: 4096,
        ttl: Duration::from_secs(1),
    }
}

fn no<T, R: Responder<T>>(r: R) {
    r.done(Err(VfsError::new(Code::NotSupported)));
}

impl ProbeVfs {
    fn attr_of(&self, st: &State, ino: Ino) -> Result<Attr, VfsError> {
        Ok(match ino {
            ROOT_INO => attr(ROOT_INO, FileKind::Dir, 3, 0),
            FILE => attr(FILE, FileKind::File, 1, 4096),
            DIR => Attr {
                ttl: DIR_TTL,
                ..attr(DIR, FileKind::Dir, 2 + st.children.len() as u32, 0)
            },
            n if n >= 10 && ((n - 10) as usize) < st.children.len() => attr(n, FileKind::Dir, 2, 0),
            _ => return Err(VfsError::new(Code::NotFound)),
        })
    }

    /// Wait (bounded) for `released` to turn true.
    fn wait_release(&self, released: impl Fn(&State) -> bool) {
        let deadline = Instant::now() + HOLD_MAX;
        let mut st = self.state.lock().unwrap();
        while !released(&st) && !st.aborted {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            st = self.cv.wait_timeout(st, left).unwrap().0;
        }
    }

    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
        self.cv.notify_all();
    }

    /// Wait (bounded) until `cond`.
    fn wait_for(&self, within: Duration, cond: impl Fn(&State) -> bool) -> bool {
        let deadline = Instant::now() + within;
        let mut st = self.state.lock().unwrap();
        while !cond(&st) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() || st.aborted {
                return false;
            }
            st = self.cv.wait_timeout(st, left).unwrap().0;
        }
        true
    }
}

impl Vfs for ProbeVfs {
    fn lookup<R: Responder<Entry>>(&self, _cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let st = self.state.lock().unwrap();
        let ino = match (parent, name.as_bytes()) {
            (ROOT_INO, b"f") => FILE,
            (ROOT_INO, b"d") => DIR,
            (DIR, n) => match st.children.iter().position(|c| c == n) {
                Some(i) => 10 + i as Ino,
                None => return r.done(Err(VfsError::new(Code::NotFound))),
            },
            _ => return r.done(Err(VfsError::new(Code::NotFound))),
        };
        r.done(self.attr_of(&st, ino).map(|attr| Entry {
            attr,
            generation: 0,
        }))
    }

    fn getattr<R: Responder<Attr>>(&self, _cx: &OpCtx<'_>, ino: Ino, _fh: Option<Fh>, r: R) {
        let (attr, hold) = {
            let mut st = self.state.lock().unwrap();
            let attr = self.attr_of(&st, ino);
            let hold = ino == DIR && st.hold_getattr && !st.aborted;
            if hold {
                st.hold_getattr = false;
                st.getattr_held = true;
            }
            (attr, hold)
        };
        if hold {
            // The attributes read before the change, answered after it.
            self.cv.notify_all();
            self.wait_release(|st| st.release_getattr);
        }
        r.done(attr)
    }

    fn setattr<R: Responder<Attr>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Option<Fh>,
        _set: &SetAttr,
        r: R,
    ) {
        let st = self.state.lock().unwrap();
        r.done(self.attr_of(&st, ino))
    }

    fn readlink<R: Responder<Vec<u8>>>(&self, _cx: &OpCtx<'_>, _ino: Ino, r: R) {
        no(r)
    }

    fn mknod<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _mode: u32,
        _rdev: constellation_types::Rdev,
        r: R,
    ) {
        no(r)
    }

    fn mkdir<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        _mode: u32,
        r: R,
    ) {
        if parent != DIR {
            return no(r);
        }
        let mut st = self.state.lock().unwrap();
        st.children.push(name.as_bytes().to_vec());
        let ino = 10 + st.children.len() as Ino - 1;
        r.done(self.attr_of(&st, ino).map(|attr| Entry {
            attr,
            generation: 0,
        }))
    }

    fn symlink<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _target: &[u8],
        r: R,
    ) {
        no(r)
    }

    fn link<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _new_parent: Ino,
        _new_name: &Name,
        r: R,
    ) {
        no(r)
    }

    fn unlink<R: Responder<()>>(&self, _cx: &OpCtx<'_>, _parent: Ino, _name: &Name, r: R) {
        no(r)
    }

    fn rmdir<R: Responder<()>>(&self, _cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        if parent != DIR {
            return no(r);
        }
        let mut st = self.state.lock().unwrap();
        // The last one only (the probe removes what it made, last first),
        // which keeps the numbering of the others.
        if st.children.last().map(|c| c.as_slice()) == Some(name.as_bytes()) {
            st.children.pop();
            r.done(Ok(()))
        } else {
            r.done(Err(VfsError::new(Code::NotFound)))
        }
    }

    fn rename<R: Responder<Renamed>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _new_parent: Ino,
        _new_name: &Name,
        _flags: RenameFlags,
        r: R,
    ) {
        no(r)
    }

    fn open<R: Responder<Opened>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _flags: OpenFlags,
        _owner: OpenOwner,
        r: R,
    ) {
        r.done(Ok(Opened::new(Fh(ino))))
    }

    fn create<R: Responder<(Entry, Opened)>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _mode: u32,
        _flags: OpenFlags,
        _owner: OpenOwner,
        r: R,
    ) {
        no(r)
    }

    fn read<R: Responder<ReadData>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        off: u64,
        len: u32,
        r: R,
    ) {
        let n = 4096u64.saturating_sub(off).min(u64::from(len)) as usize;
        r.done(Ok(ReadData::from_vec(vec![0; n])))
    }

    fn write<R: Responder<u32>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _off: u64,
        data: WriteData<'_>,
        _flags: OpenFlags,
        r: R,
    ) {
        let n = data.as_slice().len() as u32;
        let hold = {
            let mut st = self.state.lock().unwrap();
            let hold = st.hold_write && !st.aborted;
            if hold {
                st.hold_write = false;
                st.write_held = true;
            }
            hold
        };
        if hold {
            self.cv.notify_all();
            self.wait_release(|st| st.release_write);
        }
        r.done(Ok(n))
    }

    fn flush<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _owner: LockOwner,
        r: R,
    ) {
        r.done(Ok(()))
    }

    fn release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _flags: OpenFlags,
        _owner: Option<LockOwner>,
        r: R,
    ) {
        r.done(Ok(()))
    }

    fn fsync<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _level: Durability,
        r: R,
    ) {
        r.done(Ok(()))
    }

    fn readdir<R: DirSink + Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _cookie: u64,
        _plus: bool,
        r: R,
    ) {
        r.done(Ok(()))
    }

    fn statfs<R: Responder<StatFs>>(&self, _cx: &OpCtx<'_>, _ino: Ino, r: R) {
        r.done(Ok(StatFs {
            blocks: 1,
            bfree: 0,
            bavail: 0,
            files: 4,
            ffree: 0,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        }))
    }

    fn fallocate<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _offset: u64,
        _length: u64,
        _mode: FallocateMode,
        r: R,
    ) {
        no(r)
    }

    fn seek<R: Responder<u64>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _offset: u64,
        _whence: SeekWhence,
        r: R,
    ) {
        no(r)
    }

    fn copy_file_range<R: Responder<u64>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino_in: Ino,
        _fh_in: Fh,
        _offset_in: u64,
        _ino_out: Ino,
        _fh_out: Fh,
        _offset_out: u64,
        _len: u64,
        _flags: constellation_vfs::CopyFlags,
        r: R,
    ) {
        r.done(Err(VfsError::new(Code::NotImplemented)))
    }

    fn getxattr<R: Responder<Vec<u8>>>(&self, _cx: &OpCtx<'_>, _ino: Ino, _name: &XattrName, r: R) {
        r.done(Err(VfsError::new(Code::NoData)))
    }

    fn setxattr<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _name: &XattrName,
        _value: &[u8],
        _flags: SetXattrFlags,
        r: R,
    ) {
        no(r)
    }

    fn listxattr<R: Responder<Vec<XattrNameBuf>>>(&self, _cx: &OpCtx<'_>, _ino: Ino, r: R) {
        r.done(Ok(Vec::new()))
    }

    fn removexattr<R: Responder<()>>(&self, _cx: &OpCtx<'_>, _ino: Ino, _name: &XattrName, r: R) {
        no(r)
    }

    fn lock_test<R: Responder<LockStatus>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _lock: LockSpec,
        r: R,
    ) {
        no(r)
    }

    fn lock_acquire<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _lock: LockSpec,
        _sleep: bool,
        r: R,
    ) {
        no(r)
    }

    fn lock_release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _owner: LockOwner,
        _range: LockRange,
        r: R,
    ) {
        no(r)
    }

    fn sync_view<R: Responder<()>>(&self, _cx: &OpCtx<'_>, r: R) {
        r.done(Ok(()))
    }
}

/// `CONSTELLATION_FUSE_KERNEL_PROBE`: `auto` (default) runs the probes at
/// the first mount, `off` never (the table and the overrides decide).
pub const PROBE_ENV: &str = "CONSTELLATION_FUSE_KERNEL_PROBE";

/// Whether the probe's thread is running: its mount holds a `WRITE`'s
/// reply on purpose, and on a kernel with fix 0001 its notification thread
/// sits in `D` until that reply is released by a timer of this process.
/// An `exec` (a handover) or an exit in that window would wait for the
/// `D` thread, which waits for a reply only the gone timer could send: a
/// stuck process. A handover waits for it first ([`wait_done`]); an exit
/// gives it a moment and then aborts it ([`stop`], run `atexit`).
static RUNNING: Mutex<bool> = Mutex::new(false);
static DONE: Condvar = Condvar::new();

/// The running probe's filesystem, for [`abort`].
static ACTIVE: Mutex<Option<Arc<ProbeVfs>>> = Mutex::new(None);
/// [`abort`] was called: a probe that has not started does not, and one
/// that has ends with `Unknown`.
static ABORTED: AtomicBool = AtomicBool::new(false);

/// Release whatever the probe holds, and hold nothing more: its held
/// `WRITE` and `GETATTR` are answered at once, its waits end, its
/// verdicts are `Unknown`. For the process's exit, while the mount's
/// workers still run: the held `WRITE` is the one reply a thread of this
/// process (the probe's notification) waits for in `D` on a kernel with
/// fix 0001, so the exit would wait forever for a reply only a worker the
/// exit itself ends could send (found by §7.5's run on a patched kernel).
pub(crate) fn abort() {
    ABORTED.store(true, Ordering::SeqCst);
    let active = ACTIVE.lock().unwrap().clone();
    if let Some(vfs) = active {
        vfs.set(abort_state);
    }
}

fn abort_state(st: &mut State) {
    st.aborted = true;
    st.release_write = true;
    st.release_getattr = true;
}

/// For an exit: give a probe `grace` to end by itself, then [`abort`] it
/// and wait for it to unwind (at most `unwind`); `false` if it is still
/// running after that.
pub(crate) fn stop(grace: Duration, unwind: Duration) -> bool {
    if wait_done(grace) {
        return true;
    }
    abort();
    wait_done(unwind)
}

/// Wait until the probe is not running, at most `bound` (its own waits
/// are bounded by `HOLD_MAX`); `false` if it still is.
pub(crate) fn wait_done(bound: Duration) -> bool {
    let running = RUNNING.lock().unwrap();
    let (running, _) = DONE
        .wait_timeout_while(running, bound, |running| *running)
        .unwrap();
    !*running
}

/// [`MountRecorder::record`].
pub type RecordFn = Box<dyn Fn(&Path, &str) + Send + Sync>;

/// How the probe tells the daemon about its mount, so that a daemon killed
/// while the probe holds a reply (a zombie with a thread in `D` on a kernel
/// with fix 0001) leaves a record whoever takes over can find the mount by
/// and abort its connection (the daemon's `mounts` record, as for a view's).
pub struct MountRecorder {
    /// Called before the probe mounts, with its mountpoint and the file
    /// system name the mount carries.
    pub record: RecordFn,
    /// Called when the probe has unmounted, or could not mount.
    pub forget: Box<dyn Fn() + Send + Sync>,
}

/// The file system name of the probe's mount.
const PROBE_FS_NAME: &str = "constellation-probe";

/// Run the probes once per process, from a thread (`super`'s module doc).
pub(crate) fn start_once(recorder: MountRecorder) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let off = std::env::var(PROBE_ENV).is_ok_and(|v| v.trim().eq_ignore_ascii_case("off"));
        if off {
            return;
        }
        *RUNNING.lock().unwrap() = true;
        // An exit while the probe holds a reply must not wait for it
        // (`abort`): `exit(3)` runs this on every `std::process::exit` and
        // on a return from `main`.
        extern "C" fn stop_at_exit() {
            let _ = std::panic::catch_unwind(|| stop(EXIT_GRACE, EXIT_UNWIND));
        }
        // SAFETY: registers a handler that only touches this module's
        // statics; a nonzero return (out of memory) costs the exit
        // protection, not correctness.
        unsafe { libc::atexit(stop_at_exit) };
        let spawned = std::thread::Builder::new()
            .name("fuse-kernel-probe".into())
            .spawn(move || {
                struct Done;
                impl Drop for Done {
                    fn drop(&mut self) {
                        *RUNNING.lock().unwrap() = false;
                        DONE.notify_all();
                    }
                }
                let _done = Done;
                let (w, d) = run(&recorder);
                set_probed(KernelFix::WriteThroughWriteback, w);
                set_probed(KernelFix::DirChangeBumpsVersion, d);
                tracing::info!(
                    write_through_writeback = w.as_str(),
                    dir_change_bumps_version = d.as_str(),
                    release = ?super::running_release(),
                    "probed the kernel's FUSE fixes (plan 42 §5.4)"
                );
            });
        if let Err(error) = spawned {
            *RUNNING.lock().unwrap() = false;
            tracing::warn!(%error, "could not start the FUSE kernel probe");
        }
    });
}

/// The mount, both probes, the unmount: `(0001, 0003)`.
fn run(recorder: &MountRecorder) -> (Verdict, Verdict) {
    // A fresh directory of our own: `create` (not `create_dir_all`) fails
    // on anything already at the path — a directory or a symlink planted
    // in the shared temp directory, which a daemon running as root would
    // otherwise mount over — and the name is not predictable from the pid.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let dir = std::env::temp_dir().join(format!(
        "constellation-kernel-probe-{}-{nonce:08x}",
        std::process::id()
    ));
    {
        use std::os::unix::fs::DirBuilderExt;
        if std::fs::DirBuilder::new().mode(0o700).create(&dir).is_err() {
            return (Verdict::Unknown, Verdict::Unknown);
        }
    }
    let vfs = Arc::new(ProbeVfs::default());
    *ACTIVE.lock().unwrap() = Some(vfs.clone());
    if ABORTED.load(Ordering::SeqCst) {
        vfs.set(abort_state);
    }
    let mut cfg = crate::TransportConfig {
        policy: crate::TransportPolicy::DevFuse,
        // The probe lists nothing it needs attributes of.
        readdirplus: crate::ReaddirPlus::Off,
        ..Default::default()
    };
    // The probe's own mount writes nothing that would close the races it
    // looks for.
    cfg.workarounds = crate::Workarounds {
        keep_cache: crate::KeepCache::Off,
        write_repair: crate::Switch::Off,
        dir_change_notify: crate::Switch::Off,
        dir_attr_ttl: crate::DirAttrTtl::Entry,
        ..Default::default()
    };
    let opts = crate::MountOptions::new(PROBE_FS_NAME, 2, crate::KernelTuning::for_workers(2), cfg);
    // Recorded before the mount exists: a kill between the two leaves a
    // record of a mount that is not there, which is dropped.
    (recorder.record)(&dir, PROBE_FS_NAME);
    let session = match crate::mount(vfs.clone(), &dir, &opts, crate::caps(false)) {
        Ok(s) => s,
        Err(error) => {
            tracing::debug!(%error, "the FUSE kernel probe could not mount");
            (recorder.forget)();
            let _ = std::fs::remove_dir(&dir);
            *ACTIVE.lock().unwrap() = None;
            return (Verdict::Unknown, Verdict::Unknown);
        }
    };
    let sink = session.notifier();
    let control = session.control();
    let served = std::thread::spawn(move || session.run());
    let gate = sink.gate().clone();
    let w = probe_write(&dir, &vfs, &gate);
    let d = probe_dir(&dir, &vfs);
    let _ = control.unmount();
    let _ = served.join();
    (recorder.forget)();
    let _ = std::fs::remove_dir(&dir);
    *ACTIVE.lock().unwrap() = None;
    if ABORTED.load(Ordering::SeqCst) {
        return (Verdict::Unknown, Verdict::Unknown);
    }
    (w, d)
}

/// 0001, twice (`super`'s module doc).
fn probe_write(mnt: &Path, vfs: &Arc<ProbeVfs>, gate: &Arc<crate::notify::NotifyGate>) -> Verdict {
    let mut verdicts = Vec::new();
    for _ in 0..2 {
        verdicts.push(probe_write_once(mnt, vfs, gate));
    }
    if verdicts.iter().all(|v| *v == Verdict::Fixed) {
        Verdict::Fixed
    } else if verdicts.contains(&Verdict::NotFixed) {
        Verdict::NotFixed
    } else {
        Verdict::Unknown
    }
}

fn probe_write_once(
    mnt: &Path,
    vfs: &Arc<ProbeVfs>,
    gate: &Arc<crate::notify::NotifyGate>,
) -> Verdict {
    vfs.set(|st| {
        st.hold_write = true;
        st.write_held = false;
        st.release_write = false;
    });
    let path = mnt.join("f");
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(path)?;
        f.write_all_at(&vec![1u8; crate::workarounds::page_size() as usize], 0)
    });
    if !vfs.wait_for(HOLD_MAX, |st| st.write_held) {
        vfs.set(|st| st.release_write = true);
        let _ = writer.join();
        return Verdict::Unknown;
    }
    // The write's folio is filled and its WRITE held: does a drop of it
    // wait for the reply?
    let notifier = match gate.enter() {
        Ok((notifier, _, _)) => notifier,
        Err(_) => {
            vfs.set(|st| st.release_write = true);
            let _ = writer.join();
            return Verdict::Unknown;
        }
    };
    let (tx, rx) = std::sync::mpsc::channel();
    let page = crate::workarounds::page_size() as i64;
    let inval = std::thread::spawn(move || {
        // SAFETY: no preconditions.
        let _ = tx.send(Some(unsafe { libc::gettid() }));
        let _ = notifier.inval_inode(fuser::INodeNo(FILE), 0, page);
        let _ = tx.send(None);
    });
    let verdict = match rx.recv_timeout(WAITED) {
        Ok(Some(tid)) => match rx.recv_timeout(WAITED) {
            Ok(_) => Verdict::NotFixed,
            // Still out: waited for the reply only if blocked in the
            // kernel now (not merely unscheduled).
            Err(_) if blocked_in_kernel(tid) => Verdict::Fixed,
            Err(_) => Verdict::Unknown,
        },
        _ => Verdict::Unknown,
    };
    vfs.set(|st| st.release_write = true);
    let _ = inval.join();
    gate.exit();
    let wrote = writer.join().map(|r| r.is_ok()).unwrap_or(false);
    if !wrote {
        return Verdict::Unknown;
    }
    verdict
}

/// Whether thread `tid` of this process is in uninterruptible sleep (`D`):
/// inside a syscall, waiting for a folio's lock or writeback.
fn blocked_in_kernel(tid: libc::pid_t) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")) else {
        return false;
    };
    // `pid (comm) state …`: the state follows the last `)`.
    stat.rsplit_once(')')
        .and_then(|(_, rest)| rest.split_whitespace().next())
        == Some("D")
}

/// 0003: race 2b on `d` (`super`'s module doc).
fn probe_dir(mnt: &Path, vfs: &Arc<ProbeVfs>) -> Verdict {
    let d = mnt.join("d");
    let Ok(cd) = CString::new(d.as_os_str().as_bytes()) else {
        return Verdict::Unknown;
    };
    // SAFETY: a NUL-terminated path.
    let fd = unsafe { libc::open(cd.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if fd < 0 {
        return Verdict::Unknown;
    }
    let statx = |force: bool| -> Option<u32> {
        let mut st: libc::statx = unsafe { std::mem::zeroed() };
        let flags = libc::AT_EMPTY_PATH | if force { libc::AT_STATX_FORCE_SYNC } else { 0 };
        // SAFETY: an open descriptor, an empty path, a statx buffer.
        let r = unsafe { libc::statx(fd, c"".as_ptr(), flags, libc::STATX_BASIC_STATS, &mut st) };
        (r == 0).then_some(st.stx_nlink)
    };
    let verdict = (|| {
        let before = statx(false)?;
        vfs.set(|st| {
            st.hold_getattr = true;
            st.getattr_held = false;
            st.release_getattr = false;
        });
        let held = std::thread::spawn(move || {
            let mut st: libc::statx = unsafe { std::mem::zeroed() };
            // SAFETY: as above.
            unsafe {
                libc::statx(
                    fd,
                    c"".as_ptr(),
                    libc::AT_EMPTY_PATH | libc::AT_STATX_FORCE_SYNC,
                    libc::STATX_BASIC_STATS,
                    &mut st,
                )
            }
        });
        let got = vfs.wait_for(HOLD_MAX, |st| st.getattr_held);
        let name = CString::new("x").unwrap();
        // SAFETY: an open directory descriptor, a NUL-terminated name.
        let made = got && unsafe { libc::mkdirat(fd, name.as_ptr(), 0o755) } == 0;
        vfs.set(|st| st.release_getattr = true);
        let _ = held.join();
        let released = Instant::now();
        if !made {
            return None;
        }
        let after = statx(false)?;
        if released.elapsed() > DIR_TTL / 2 {
            // The held reply may have expired: no answer either way.
            return None;
        }
        // SAFETY: as above.
        unsafe { libc::unlinkat(fd, name.as_ptr(), libc::AT_REMOVEDIR) };
        Some(if after == before + 1 {
            Verdict::Fixed
        } else {
            Verdict::NotFixed
        })
    })()
    .unwrap_or(Verdict::Unknown);
    // SAFETY: our descriptor.
    unsafe { libc::close(fd) };
    verdict
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An exit aborts a probe that holds a reply: the held wait ends at
    /// once instead of at `HOLD_MAX`, nothing is held afterwards, and the
    /// probe's own waits end. (Without this the held `WRITE` is the reply a
    /// thread in `D` waits for when the process exits: a stuck process.)
    #[test]
    fn an_abort_releases_what_the_probe_holds() {
        let vfs = Arc::new(ProbeVfs::default());
        vfs.set(|st| st.hold_write = true);
        let waiter = {
            let vfs = vfs.clone();
            std::thread::spawn(move || {
                let t = Instant::now();
                vfs.wait_release(|st| st.release_write);
                t.elapsed()
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        vfs.set(abort_state);
        let waited = waiter.join().unwrap();
        assert!(
            waited < Duration::from_secs(2),
            "released by the abort, not by HOLD_MAX: {waited:?}"
        );
        // Nothing is held or waited for afterwards.
        let t = Instant::now();
        assert!(!vfs.wait_for(HOLD_MAX, |st| st.write_held));
        vfs.wait_release(|st| st.release_getattr && !st.aborted);
        assert!(t.elapsed() < Duration::from_secs(2));
        assert!(vfs.state.lock().unwrap().aborted);
    }

    /// The probe's mount is recorded before it exists and forgotten after
    /// it is gone, and while it is up the kernel lists it under the very
    /// mountpoint and name the record carries: that pair is how whoever
    /// takes over from a killed daemon finds the connection to abort (the
    /// daemon's `mounts` records). Needs `/dev/fuse` and `fusermount3`.
    #[test]
    fn the_probe_records_its_mount_while_it_is_up() {
        let have_fusermount = std::env::var_os("PATH")
            .is_some_and(|p| std::env::split_paths(&p).any(|d| d.join("fusermount3").exists()));
        if !Path::new("/dev/fuse").exists() || !have_fusermount {
            eprintln!("skipping: needs /dev/fuse and fusermount3");
            return;
        }
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let ended = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(AtomicBool::new(false));
        let sampler = Arc::new(Mutex::new(None::<std::thread::JoinHandle<()>>));
        let recorder = MountRecorder {
            record: {
                let (events, ended, seen) = (events.clone(), ended.clone(), seen.clone());
                let sampler = sampler.clone();
                Box::new(move |dir, name| {
                    events
                        .lock()
                        .unwrap()
                        .push(format!("record {} {name}", dir.display()));
                    let (dir, name) = (dir.to_path_buf(), name.to_string());
                    let (ended, seen) = (ended.clone(), seen.clone());
                    let handle = std::thread::spawn(move || {
                        while !ended.load(Ordering::SeqCst) {
                            let table =
                                std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
                            let listed = table.lines().any(|l| {
                                let f: Vec<&str> = l.split(' ').collect();
                                f.get(4) == Some(&dir.to_str().unwrap())
                                    && l.contains(&format!(" - fuse {name} "))
                            });
                            if listed {
                                seen.store(true, Ordering::SeqCst);
                            }
                            std::thread::sleep(Duration::from_micros(200));
                        }
                    });
                    *sampler.lock().unwrap() = Some(handle);
                })
            },
            forget: {
                let (events, ended) = (events.clone(), ended.clone());
                Box::new(move || {
                    events.lock().unwrap().push("forget".into());
                    ended.store(true, Ordering::SeqCst);
                })
            },
        };
        let _ = run(&recorder);
        ended.store(true, Ordering::SeqCst);
        if let Some(h) = sampler.lock().unwrap().take() {
            h.join().unwrap();
        }
        let events = events.lock().unwrap().clone();
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(
            events[0].starts_with("record ") && events[0].ends_with(" constellation-probe"),
            "{events:?}"
        );
        assert_eq!(events[1], "forget");
        assert!(
            seen.load(Ordering::SeqCst),
            "the kernel never listed a mount for the recorded pair: {events:?}"
        );
    }

    /// A running thread, a sleeping one and one that does not exist are
    /// not "blocked in the kernel": under host load a notification thread
    /// that was not scheduled yields `Unknown`, never `Fixed`.
    #[test]
    fn only_uninterruptible_sleep_counts_as_blocked() {
        // SAFETY: no preconditions.
        assert!(!blocked_in_kernel(unsafe { libc::gettid() }));
        let (tx, rx) = std::sync::mpsc::channel();
        let sleeper = std::thread::spawn(move || {
            // SAFETY: no preconditions.
            tx.send(unsafe { libc::gettid() }).unwrap();
            std::thread::sleep(Duration::from_millis(300));
        });
        let tid = rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert!(!blocked_in_kernel(tid), "S, not D");
        sleeper.join().unwrap();
        assert!(!blocked_in_kernel(i32::MAX), "no such thread");
    }
}
