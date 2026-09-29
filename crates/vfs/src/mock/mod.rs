//! [`MockVfs`]: a recording, scriptable [`Vfs`] (plan 31 §8).
//!
//! It exists so a frontend adapter's *translation* (its flag decoding, its
//! errno mapping, its reply encoding, which thread completes a reply) can
//! be unit-tested without the engine underneath and without a kernel
//! above, and so the conformance kit has a target that needs neither.
//!
//! - **Records** every call: the op, its arguments ([`Args`], typed, with
//!   a one-line [`Call::summary`]), the caller, the thread it arrived on,
//!   its deadline and whether it was already cancelled; and every
//!   completion ([`Completion`]: which call, with which outcome, on which
//!   thread). [`MockVfs::calls`], [`MockVfs::completions`],
//!   [`MockVfs::wait_for_completions`].
//! - **Scripts** replies per op ([`Script`], queued per op with
//!   [`MockVfs::on`]-style setters generated for every op): a fixed
//!   result; a closure of the call; "return now, complete from another
//!   thread after N ms"; "never complete: drop the responder" (the
//!   fail-safe path); "hold the responder, complete it never" (a hung op,
//!   released when the mock drops). A script queue that runs dry falls
//!   through to the reference filesystem if there is one, else answers
//!   `Code::NotImplemented`.
//! - **Reference mode** ([`MockVfs::reference`]): the same type backed by
//!   [`RefFs`], a small correct in-memory filesystem (see its module doc
//!   for exactly what it models), so a `MockVfs` can be a stand-in engine
//!   for frontend tests and the kit's reference target. Scripts still take
//!   precedence, so a test can make one op fail in an otherwise working
//!   filesystem.
//!
//! Every op is recorded *before* it runs and completed *after* any lock is
//! released, on the calling thread unless the script (or a blocking lock
//! wait) says otherwise.

pub mod reffs;

pub use reffs::{RefFs, RefView, LINK_DOMAIN_XATTR};

use crate::caps::FrontendCaps;
use crate::ctx::{CancelToken, OpCtx, OpId, OpKind};
use crate::error::{VfsError, VfsResult};
use crate::events::FrontendEvents;
use crate::name::{Name, NameBuf, XattrName, XattrNameBuf};
use crate::responder::{DirEntry, DirSink, Responder};
use crate::types::{
    Attr, Durability, Entry, FallocateMode, Fh, Ino, LockOwner, LockRange, LockSpec, LockStatus,
    OpenFlags, OpenOwner, Opened, ReadData, RenameFlags, SeekWhence, SetAttr, SetXattrFlags,
    StatFs, WriteData,
};
use crate::vfs::Vfs;
use constellation_types::{Code, Rdev};
use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

/// One op's arguments, as it was called.
#[derive(Debug, Clone, PartialEq)]
pub enum Args {
    Lookup {
        parent: Ino,
        name: NameBuf,
    },
    Getattr {
        ino: Ino,
        fh: Option<Fh>,
    },
    Setattr {
        ino: Ino,
        fh: Option<Fh>,
        set: SetAttr,
    },
    Readlink {
        ino: Ino,
    },
    Mknod {
        parent: Ino,
        name: NameBuf,
        mode: u32,
        rdev: Rdev,
    },
    Mkdir {
        parent: Ino,
        name: NameBuf,
        mode: u32,
    },
    Symlink {
        parent: Ino,
        name: NameBuf,
        target: Vec<u8>,
    },
    Link {
        ino: Ino,
        new_parent: Ino,
        new_name: NameBuf,
    },
    Unlink {
        parent: Ino,
        name: NameBuf,
    },
    Rmdir {
        parent: Ino,
        name: NameBuf,
    },
    Rename {
        parent: Ino,
        name: NameBuf,
        new_parent: Ino,
        new_name: NameBuf,
        flags: RenameFlags,
    },
    Open {
        ino: Ino,
        flags: OpenFlags,
        owner: OpenOwner,
    },
    Create {
        parent: Ino,
        name: NameBuf,
        mode: u32,
        flags: OpenFlags,
        owner: OpenOwner,
    },
    Read {
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u32,
    },
    Write {
        ino: Ino,
        fh: Fh,
        off: u64,
        data: Vec<u8>,
        flags: OpenFlags,
    },
    Flush {
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
    },
    Release {
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
    },
    Fsync {
        ino: Ino,
        fh: Fh,
        level: Durability,
    },
    Readdir {
        ino: Ino,
        fh: Fh,
        cookie: u64,
        plus: bool,
    },
    Statfs {
        ino: Ino,
    },
    Fallocate {
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
    },
    Seek {
        ino: Ino,
        fh: Fh,
        off: u64,
        whence: SeekWhence,
    },
    Getxattr {
        ino: Ino,
        name: XattrNameBuf,
    },
    Setxattr {
        ino: Ino,
        name: XattrNameBuf,
        value: Vec<u8>,
        flags: SetXattrFlags,
    },
    Listxattr {
        ino: Ino,
    },
    Removexattr {
        ino: Ino,
        name: XattrNameBuf,
    },
    LockTest {
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
    },
    LockAcquire {
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        sleep: bool,
    },
    LockRelease {
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        range: LockRange,
    },
    SyncView,
}

/// The thread a call arrived on or completed on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadInfo {
    pub id: ThreadId,
    pub name: Option<String>,
}

impl ThreadInfo {
    fn current() -> Self {
        let t = std::thread::current();
        Self {
            id: t.id(),
            name: t.name().map(str::to_string),
        }
    }
}

/// Who called.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerInfo {
    pub uid: u32,
    pub gid: u32,
    pub pid: Option<u32>,
}

/// One recorded call.
#[derive(Debug, Clone)]
pub struct Call {
    /// Position in the mock's call order, from 0.
    pub seq: u64,
    pub op: OpKind,
    pub op_id: OpId,
    pub args: Args,
    pub caller: CallerInfo,
    pub thread: ThreadInfo,
    pub deadline: Option<Instant>,
    /// The op's cancel token was already set when it was called.
    pub cancelled: bool,
}

impl Call {
    /// `lookup(Lookup { parent: 1, name: "a" })`, one line, for assertion
    /// messages.
    pub fn summary(&self) -> String {
        format!(
            "#{} {} by uid {} on {:?}: {:?}",
            self.seq,
            self.op.name(),
            self.caller.uid,
            self.thread.name.as_deref().unwrap_or("<unnamed>"),
            self.args
        )
    }
}

/// One recorded completion.
#[derive(Debug, Clone)]
pub struct Completion {
    /// The [`Call::seq`] it completes.
    pub seq: u64,
    pub op: OpKind,
    /// `Ok(())` for any success, else the code.
    pub outcome: Result<(), Code>,
    pub thread: ThreadInfo,
}

/// A closure that answers a call.
pub type CallFn<T> = Arc<dyn Fn(&Call) -> VfsResult<T> + Send + Sync>;

/// What a scripted op does.
pub enum Script<T> {
    /// Complete inline with this result.
    Reply(VfsResult<T>),
    /// Complete inline with what the closure says for this call.
    With(CallFn<T>),
    /// Return at once and complete from another thread after `after`.
    Defer {
        after: Duration,
        result: VfsResult<T>,
    },
    /// As [`Script::Defer`], the closure evaluated on the completing
    /// thread.
    DeferWith { after: Duration, f: CallFn<T> },
    /// Drop the responder without completing it (its fail-safe answers).
    Drop,
    /// Keep the responder and never complete it: a hung op. Released (so
    /// its fail-safe answers) when the mock drops.
    Hold,
    /// Take the reference filesystem's answer (or `NotImplemented`).
    Reference,
}

impl<T: Clone> Script<T> {
    fn take_for(&self, call: &Call) -> Script<T> {
        match self {
            Script::Reply(r) => Script::Reply(r.clone()),
            Script::With(f) => Script::With(f.clone()),
            Script::Defer { after, result } => Script::Defer {
                after: *after,
                result: result.clone(),
            },
            Script::DeferWith { after, f } => Script::DeferWith {
                after: *after,
                f: f.clone(),
            },
            Script::Drop => Script::Drop,
            Script::Hold => Script::Hold,
            Script::Reference => {
                let _ = call;
                Script::Reference
            }
        }
    }

    /// A reply with a failure.
    pub fn fail(code: Code) -> Script<T> {
        Script::Reply(Err(VfsError::new(code)))
    }

    /// A reply with a success.
    pub fn ok(value: T) -> Script<T> {
        Script::Reply(Ok(value))
    }
}

/// The queue of scripts for one op: one-shot scripts first, then the
/// sticky one (if any) for every call after.
struct Slot<T> {
    once: VecDeque<Script<T>>,
    sticky: Option<Script<T>>,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self {
            once: VecDeque::new(),
            sticky: None,
        }
    }
}

impl<T: Clone> Slot<T> {
    fn next(&mut self, call: &Call) -> Option<Script<T>> {
        if let Some(s) = self.once.pop_front() {
            return Some(s);
        }
        self.sticky.as_ref().map(|s| s.take_for(call))
    }
}

macro_rules! scripts {
    ($($field:ident : $ty:ty => $once:ident, $always:ident;)*) => {
        #[derive(Default)]
        struct Scripts {
            $($field: Mutex<Slot<$ty>>,)*
        }

        impl MockVfs {
            $(
                /// Script the next call of this op (queued in call order).
                pub fn $once(&self, script: Script<$ty>) -> &Self {
                    self.inner.scripts.$field.lock().unwrap().once.push_back(script);
                    self
                }

                /// Script every call of this op that no queued script covers.
                pub fn $always(&self, script: Script<$ty>) -> &Self {
                    self.inner.scripts.$field.lock().unwrap().sticky = Some(script);
                    self
                }
            )*
        }
    };
}

scripts! {
    lookup: Entry => on_lookup, always_lookup;
    getattr: Attr => on_getattr, always_getattr;
    setattr: Attr => on_setattr, always_setattr;
    readlink: Vec<u8> => on_readlink, always_readlink;
    mknod: Entry => on_mknod, always_mknod;
    mkdir: Entry => on_mkdir, always_mkdir;
    symlink: Entry => on_symlink, always_symlink;
    link: Entry => on_link, always_link;
    unlink: () => on_unlink, always_unlink;
    rmdir: () => on_rmdir, always_rmdir;
    rename: () => on_rename, always_rename;
    open: Opened => on_open, always_open;
    create: (Entry, Opened) => on_create, always_create;
    read: ReadData => on_read, always_read;
    write: u32 => on_write, always_write;
    flush: () => on_flush, always_flush;
    release: () => on_release, always_release;
    fsync: () => on_fsync, always_fsync;
    readdir: Vec<DirEntry> => on_readdir, always_readdir;
    statfs: StatFs => on_statfs, always_statfs;
    fallocate: () => on_fallocate, always_fallocate;
    seek: u64 => on_seek, always_seek;
    getxattr: Vec<u8> => on_getxattr, always_getxattr;
    setxattr: () => on_setxattr, always_setxattr;
    listxattr: Vec<XattrNameBuf> => on_listxattr, always_listxattr;
    removexattr: () => on_removexattr, always_removexattr;
    lock_test: LockStatus => on_lock_test, always_lock_test;
    lock_acquire: () => on_lock_acquire, always_lock_acquire;
    lock_release: () => on_lock_release, always_lock_release;
    sync_view: () => on_sync_view, always_sync_view;
}

struct Inner {
    calls: Mutex<Vec<Call>>,
    completions: Mutex<Vec<Completion>>,
    completed: Condvar,
    seq: AtomicU64,
    scripts: Scripts,
    view: Option<Arc<RefView>>,
    held: Mutex<Vec<Box<dyn Any + Send>>>,
    /// Deferred completions started and not yet finished.
    deferred: AtomicUsize,
}

/// A recording, scriptable [`Vfs`]; see the module doc.
#[derive(Clone)]
pub struct MockVfs {
    inner: Arc<Inner>,
}

/// Records a responder's completion, then forwards it. Dropping it
/// without `done` drops the inner responder, whose fail-safe answers (and
/// is recorded by the inner tap, if any, as its own drop).
///
/// It holds the mock weakly: a held (never completed) responder lives in
/// the mock, and a strong reference back would keep both alive forever.
struct Tap<R> {
    r: R,
    inner: std::sync::Weak<Inner>,
    call: Call,
}

impl<T, R: Responder<T>> Responder<T> for Tap<R>
where
    R: Responder<T>,
    T: 'static,
{
    fn done(self, result: VfsResult<T>) {
        if let Some(inner) = self.inner.upgrade() {
            inner.record_completion(&self.call, &result);
        }
        self.r.done(result);
    }
}

impl<R: DirSink> DirSink for Tap<R> {
    fn add(&mut self, ino: Ino, next: u64, kind: crate::types::FileKind, name: &[u8]) -> bool {
        self.r.add(ino, next, kind, name)
    }
}

impl Inner {
    fn record_completion<T>(&self, call: &Call, result: &VfsResult<T>) {
        self.completions.lock().unwrap().push(Completion {
            seq: call.seq,
            op: call.op,
            outcome: match result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.code()),
            },
            thread: ThreadInfo::current(),
        });
        self.completed.notify_all();
    }
}

impl MockVfs {
    fn build(view: Option<Arc<RefView>>) -> MockVfs {
        MockVfs {
            inner: Arc::new(Inner {
                calls: Mutex::new(Vec::new()),
                completions: Mutex::new(Vec::new()),
                completed: Condvar::new(),
                seq: AtomicU64::new(0),
                scripts: Scripts::default(),
                view,
                held: Mutex::new(Vec::new()),
                deferred: AtomicUsize::new(0),
            }),
        }
    }

    /// A mock with no filesystem: every op that is not scripted answers
    /// `Code::NotImplemented`.
    pub fn new() -> MockVfs {
        Self::build(None)
    }

    /// A mock over a fresh reference filesystem, behaving as a frontend
    /// with `caps` sees it, rooted at the filesystem's root.
    pub fn reference(caps: FrontendCaps) -> MockVfs {
        let fs = RefFs::new(caps);
        Self::over(&fs, "/", false).expect("the root exists")
    }

    /// A mock viewing the directory `path` of an existing reference
    /// filesystem (its root is `ROOT_INO` there), `confine_links` per
    /// plan 31 §6.12.
    pub fn over(fs: &Arc<RefFs>, path: &str, confine_links: bool) -> Result<MockVfs, Code> {
        Ok(Self::build(Some(Arc::new(fs.view(path, confine_links)?))))
    }

    /// The reference filesystem behind this mock, if it has one.
    pub fn ref_fs(&self) -> Option<&Arc<RefFs>> {
        self.inner.view.as_deref().map(RefView::fs)
    }

    /// Another mock over the same filesystem: a view of `path`.
    pub fn view_of(&self, path: &str, confine_links: bool) -> Result<MockVfs, Code> {
        let fs = self.ref_fs().ok_or(Code::NotSupported)?;
        Self::over(fs, path, confine_links)
    }

    /// Freeze the reference filesystem's tree as of now as snapshot `name`
    /// of the directory `path`.
    pub fn snapshot(&self, path: &str, name: &str) -> Result<(), Code> {
        self.ref_fs()
            .ok_or(Code::NotSupported)?
            .snapshot(path, name)
    }

    /// Deliver this view's invalidations (other views' mutations) to
    /// `events`, from a notifier thread of its own.
    pub fn set_events(&self, events: Arc<dyn FrontendEvents>) {
        if let Some(view) = &self.inner.view {
            view.set_events(events);
        }
    }

    /// Wait until everything published to this view's events sink so far
    /// has been delivered.
    pub fn settle(&self) {
        if let Some(view) = &self.inner.view {
            view.settle();
        }
    }

    // ------------------------------------------------------------ recording

    /// Every call so far, in call order.
    pub fn calls(&self) -> Vec<Call> {
        self.inner.calls.lock().unwrap().clone()
    }

    /// The calls of `op`.
    pub fn calls_of(&self, op: OpKind) -> Vec<Call> {
        self.calls().into_iter().filter(|c| c.op == op).collect()
    }

    /// The last call, if any.
    pub fn last_call(&self) -> Option<Call> {
        self.inner.calls.lock().unwrap().last().cloned()
    }

    /// Every completion so far, in completion order.
    pub fn completions(&self) -> Vec<Completion> {
        self.inner.completions.lock().unwrap().clone()
    }

    /// Wait until at least `n` completions were recorded (`false` after
    /// `timeout`).
    pub fn wait_for_completions(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut done = self.inner.completions.lock().unwrap();
        while done.len() < n {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            done = self.inner.completed.wait_timeout(done, left).unwrap().0;
        }
        true
    }

    /// Wait until no deferred completion is pending (`false` after
    /// `timeout`).
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while self.inner.deferred.load(Ordering::Acquire) > 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
    }

    /// Forget the recorded calls and completions (scripts stay).
    pub fn clear_records(&self) {
        self.inner.calls.lock().unwrap().clear();
        self.inner.completions.lock().unwrap().clear();
    }

    // ------------------------------------------------------------ dispatch

    fn record(&self, cx: &OpCtx<'_>, args: Args) -> Call {
        let call = Call {
            seq: self.inner.seq.fetch_add(1, Ordering::Relaxed),
            op: cx.kind,
            op_id: cx.op,
            args,
            caller: CallerInfo {
                uid: cx.caller.uid,
                gid: cx.caller.gid,
                pid: cx.caller.pid,
            },
            thread: ThreadInfo::current(),
            deadline: cx.deadline,
            cancelled: cx.cancelled(),
        };
        self.inner.calls.lock().unwrap().push(call.clone());
        call
    }

    /// Run one op: script first, then the reference filesystem, then
    /// `NotImplemented`.
    fn run<T, R>(
        &self,
        call: Call,
        slot: impl FnOnce(&Scripts) -> &Mutex<Slot<T>>,
        r: R,
        reference: impl FnOnce(&RefView) -> VfsResult<T>,
    ) where
        T: Clone + Send + 'static,
        R: Responder<T>,
    {
        let r = Tap {
            r,
            inner: Arc::downgrade(&self.inner),
            call: call.clone(),
        };
        let script = slot(&self.inner.scripts).lock().unwrap().next(&call);
        let fallback = |r: Tap<R>| {
            let result = match &self.inner.view {
                Some(view) => reference(view.as_ref()),
                None => Err(VfsError::new(Code::NotImplemented)),
            };
            r.done(result);
        };
        match script {
            None | Some(Script::Reference) => fallback(r),
            Some(Script::Reply(result)) => r.done(result),
            Some(Script::With(f)) => {
                let result = f(&call);
                r.done(result)
            }
            Some(Script::Defer { after, result }) => self.defer(after, r, move |_| result),
            Some(Script::DeferWith { after, f }) => {
                self.defer(after, r, move |call| f(call));
            }
            Some(Script::Drop) => drop(r),
            Some(Script::Hold) => self.inner.held.lock().unwrap().push(Box::new(r)),
        }
    }

    fn defer<T, R>(
        &self,
        after: Duration,
        r: Tap<R>,
        f: impl FnOnce(&Call) -> VfsResult<T> + Send + 'static,
    ) where
        T: Send + 'static,
        R: Responder<T>,
    {
        let inner = self.inner.clone();
        inner.deferred.fetch_add(1, Ordering::AcqRel);
        let spawned = std::thread::Builder::new()
            .name("mock-deferred".into())
            .spawn({
                let inner = inner.clone();
                move || {
                    std::thread::sleep(after);
                    let result = f(&r.call);
                    r.done(result);
                    inner.deferred.fetch_sub(1, Ordering::AcqRel);
                }
            });
        if spawned.is_err() {
            // The closure (and the responder in it) is gone: its drop
            // fail-safe answered.
            inner.deferred.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Default for MockVfs {
    fn default() -> Self {
        Self::new()
    }
}

fn nb(name: &Name) -> NameBuf {
    name.to_buf()
}

impl Vfs for MockVfs {
    fn lookup<R: Responder<Entry>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let call = self.record(
            cx,
            Args::Lookup {
                parent,
                name: nb(name),
            },
        );
        self.run(call, |s| &s.lookup, r, |v| v.lookup(parent, name));
    }

    fn getattr<R: Responder<Attr>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Option<Fh>, r: R) {
        let call = self.record(cx, Args::Getattr { ino, fh });
        self.run(call, |s| &s.getattr, r, |v| v.getattr(ino, fh));
    }

    fn setattr<R: Responder<Attr>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Option<Fh>,
        set: &SetAttr,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Setattr {
                ino,
                fh,
                set: set.clone(),
            },
        );
        self.run(call, |s| &s.setattr, r, |v| v.setattr(ino, fh, set));
    }

    fn readlink<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let call = self.record(cx, Args::Readlink { ino });
        self.run(call, |s| &s.readlink, r, |v| v.readlink(ino));
    }

    fn mknod<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        rdev: Rdev,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Mknod {
                parent,
                name: nb(name),
                mode,
                rdev,
            },
        );
        let caller = cx.caller;
        self.run(
            call,
            |s| &s.mknod,
            r,
            |v| v.mknod(caller, parent, name, mode, rdev),
        );
    }

    fn mkdir<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Mkdir {
                parent,
                name: nb(name),
                mode,
            },
        );
        let caller = cx.caller;
        self.run(
            call,
            |s| &s.mkdir,
            r,
            |v| v.mkdir(caller, parent, name, mode),
        );
    }

    fn symlink<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        target: &[u8],
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Symlink {
                parent,
                name: nb(name),
                target: target.to_vec(),
            },
        );
        let caller = cx.caller;
        self.run(
            call,
            |s| &s.symlink,
            r,
            |v| v.symlink(caller, parent, name, target),
        );
    }

    fn link<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        new_parent: Ino,
        new_name: &Name,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Link {
                ino,
                new_parent,
                new_name: nb(new_name),
            },
        );
        self.run(call, |s| &s.link, r, |v| v.link(ino, new_parent, new_name));
    }

    fn unlink<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let call = self.record(
            cx,
            Args::Unlink {
                parent,
                name: nb(name),
            },
        );
        self.run(call, |s| &s.unlink, r, |v| v.unlink(parent, name));
    }

    fn rmdir<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let call = self.record(
            cx,
            Args::Rmdir {
                parent,
                name: nb(name),
            },
        );
        self.run(call, |s| &s.rmdir, r, |v| v.rmdir(parent, name));
    }

    fn rename<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        new_parent: Ino,
        new_name: &Name,
        flags: RenameFlags,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Rename {
                parent,
                name: nb(name),
                new_parent,
                new_name: nb(new_name),
                flags,
            },
        );
        self.run(
            call,
            |s| &s.rename,
            r,
            |v| v.rename(parent, name, new_parent, new_name, flags),
        );
    }

    fn open<R: Responder<Opened>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        flags: OpenFlags,
        owner: OpenOwner,
        r: R,
    ) {
        let call = self.record(cx, Args::Open { ino, flags, owner });
        self.run(call, |s| &s.open, r, |v| v.open(ino, flags));
    }

    fn create<R: Responder<(Entry, Opened)>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        flags: OpenFlags,
        owner: OpenOwner,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Create {
                parent,
                name: nb(name),
                mode,
                flags,
                owner,
            },
        );
        let caller = cx.caller;
        self.run(
            call,
            |s| &s.create,
            r,
            |v| v.create(caller, parent, name, mode, flags),
        );
    }

    fn read<R: Responder<ReadData>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u32,
        r: R,
    ) {
        let call = self.record(cx, Args::Read { ino, fh, off, len });
        self.run(call, |s| &s.read, r, |v| v.read(ino, fh, off, len));
    }

    fn write<R: Responder<u32>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        data: WriteData<'_>,
        flags: OpenFlags,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Write {
                ino,
                fh,
                off,
                data: data.as_slice().to_vec(),
                flags,
            },
        );
        self.run(
            call,
            |s| &s.write,
            r,
            |v| v.write(ino, fh, off, data.as_slice()),
        );
    }

    fn flush<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, owner: LockOwner, r: R) {
        let call = self.record(cx, Args::Flush { ino, fh, owner });
        self.run(call, |s| &s.flush, r, |v| v.flush(ino, fh, owner));
    }

    fn release<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Release {
                ino,
                fh,
                flags,
                owner,
            },
        );
        self.run(call, |s| &s.release, r, |v| v.release(ino, fh, owner));
    }

    fn fsync<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, level: Durability, r: R) {
        let call = self.record(cx, Args::Fsync { ino, fh, level });
        self.run(call, |s| &s.fsync, r, |v| v.fsync(ino, fh));
    }

    fn readdir<R: DirSink + Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        cookie: u64,
        plus: bool,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Readdir {
                ino,
                fh,
                cookie,
                plus,
            },
        );
        let mut tap = Tap {
            r,
            inner: Arc::downgrade(&self.inner),
            call: call.clone(),
        };
        let script = self.inner.scripts.readdir.lock().unwrap().next(&call);
        let result = match script {
            None | Some(Script::Reference) => match &self.inner.view {
                Some(view) => view.readdir(ino, cookie, &mut tap),
                None => Err(VfsError::new(Code::NotImplemented)),
            },
            Some(Script::Reply(entries)) => entries.map(|entries| {
                for e in entries {
                    if tap.add(e.ino, e.next, e.kind, e.name.as_bytes()) {
                        break;
                    }
                }
            }),
            Some(Script::With(f)) => f(&call).map(|entries| {
                for e in entries {
                    if tap.add(e.ino, e.next, e.kind, e.name.as_bytes()) {
                        break;
                    }
                }
            }),
            Some(Script::Defer { after, result }) => {
                let inner = self.inner.clone();
                inner.deferred.fetch_add(1, Ordering::AcqRel);
                let spawned = std::thread::Builder::new()
                    .name("mock-deferred".into())
                    .spawn({
                        let inner = inner.clone();
                        move || {
                            std::thread::sleep(after);
                            let mut tap = tap;
                            let result = result.map(|entries| {
                                for e in entries {
                                    if tap.add(e.ino, e.next, e.kind, e.name.as_bytes()) {
                                        break;
                                    }
                                }
                            });
                            tap.done(result);
                            inner.deferred.fetch_sub(1, Ordering::AcqRel);
                        }
                    });
                if spawned.is_err() {
                    inner.deferred.fetch_sub(1, Ordering::AcqRel);
                }
                return;
            }
            Some(Script::DeferWith { .. }) => Err(VfsError::new(Code::NotSupported)),
            Some(Script::Drop) => return drop(tap),
            Some(Script::Hold) => {
                self.inner.held.lock().unwrap().push(Box::new(tap));
                return;
            }
        };
        tap.done(result);
    }

    fn statfs<R: Responder<StatFs>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let call = self.record(cx, Args::Statfs { ino });
        self.run(call, |s| &s.statfs, r, |v| v.statfs(ino));
    }

    fn fallocate<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Fallocate {
                ino,
                fh,
                off,
                len,
                mode,
            },
        );
        self.run(
            call,
            |s| &s.fallocate,
            r,
            |v| v.fallocate(ino, fh, off, len, mode),
        );
    }

    fn seek<R: Responder<u64>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        whence: SeekWhence,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Seek {
                ino,
                fh,
                off,
                whence,
            },
        );
        self.run(call, |s| &s.seek, r, |v| v.seek(ino, fh, off, whence));
    }

    fn getxattr<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R) {
        let call = self.record(
            cx,
            Args::Getxattr {
                ino,
                name: name.to_buf(),
            },
        );
        let caller = cx.caller;
        self.run(call, |s| &s.getxattr, r, |v| v.getxattr(caller, ino, name));
    }

    fn setxattr<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        name: &XattrName,
        value: &[u8],
        flags: SetXattrFlags,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::Setxattr {
                ino,
                name: name.to_buf(),
                value: value.to_vec(),
                flags,
            },
        );
        let caller = cx.caller;
        self.run(
            call,
            |s| &s.setxattr,
            r,
            |v| v.setxattr(caller, ino, name, value, flags),
        );
    }

    fn listxattr<R: Responder<Vec<XattrNameBuf>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let call = self.record(cx, Args::Listxattr { ino });
        let caller = cx.caller;
        self.run(call, |s| &s.listxattr, r, |v| v.listxattr(caller, ino));
    }

    fn removexattr<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R) {
        let call = self.record(
            cx,
            Args::Removexattr {
                ino,
                name: name.to_buf(),
            },
        );
        let caller = cx.caller;
        self.run(
            call,
            |s| &s.removexattr,
            r,
            |v| v.removexattr(caller, ino, name),
        );
    }

    fn lock_test<R: Responder<LockStatus>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        r: R,
    ) {
        let call = self.record(cx, Args::LockTest { ino, fh, lock });
        self.run(call, |s| &s.lock_test, r, |v| v.lock_test(ino, lock));
    }

    fn lock_acquire<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        sleep: bool,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::LockAcquire {
                ino,
                fh,
                lock,
                sleep,
            },
        );
        let cancel: Option<CancelToken> = cx.cancel.cloned();
        let scripted = {
            let slot = self.inner.scripts.lock_acquire.lock().unwrap();
            !slot.once.is_empty() || slot.sticky.is_some()
        };
        let Some(view) = self.inner.view.as_ref().filter(|_| !scripted) else {
            return self.run(
                call,
                |s| &s.lock_acquire,
                r,
                |v| match v.lock_try(ino, lock)? {
                    reffs::LockTry::Granted => Ok(()),
                    reffs::LockTry::Conflict => Err(VfsError::new(Code::Again)),
                },
            );
        };
        let r = Tap {
            r,
            inner: Arc::downgrade(&self.inner),
            call,
        };
        let cancelled = cancel.as_ref().is_some_and(CancelToken::is_cancelled);
        match view.lock_try(ino, lock) {
            Ok(reffs::LockTry::Granted) => r.done(Ok(())),
            Ok(reffs::LockTry::Conflict) if !sleep => r.done(Err(VfsError::new(Code::Again))),
            Ok(reffs::LockTry::Conflict) if cancelled => r.done(Err(VfsError::new(Code::Intr))),
            // The wait: a thread of its own completes the responder.
            Ok(reffs::LockTry::Conflict) => view.lock_wait(ino, lock, cancel, cx.deadline, r),
            Err(e) => r.done(Err(e)),
        }
    }

    fn lock_release<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        range: LockRange,
        r: R,
    ) {
        let call = self.record(
            cx,
            Args::LockRelease {
                ino,
                fh,
                owner,
                range,
            },
        );
        self.run(
            call,
            |s| &s.lock_release,
            r,
            |v| v.lock_release(ino, owner, range),
        );
    }

    fn sync_view<R: Responder<()>>(&self, cx: &OpCtx<'_>, r: R) {
        let call = self.record(cx, Args::SyncView);
        self.run(call, |s| &s.sync_view, r, |v| v.sync_view());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx::Caller;
    use crate::responder::{Blocking, CollectDir};
    use crate::types::{FileKind, LockKind, ROOT_INO};

    fn caller() -> Caller {
        Caller::with_groups(1000, 100, &[])
    }

    fn cx(kind: OpKind, caller: &Caller) -> OpCtx<'_> {
        OpCtx::new(kind, caller)
    }

    fn attr(ino: Ino) -> Attr {
        Attr {
            ino,
            kind: FileKind::File,
            size: 3,
            blocks: 1,
            mode: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: Rdev::default(),
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            blksize: 4096,
            ttl: Duration::from_secs(1),
        }
    }

    fn entry(ino: Ino) -> Entry {
        Entry {
            attr: attr(ino),
            generation: 5,
        }
    }

    #[test]
    fn an_unscripted_mock_without_a_filesystem_is_not_implemented() {
        let mock = MockVfs::new();
        let c = caller();
        let r = Blocking::run(|r| mock.getattr(&cx(OpKind::Getattr, &c), 7, None, r));
        assert_eq!(r, Err(Code::NotImplemented.into()));
        // The call and its completion were both recorded.
        let calls = mock.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].args, Args::Getattr { ino: 7, fh: None });
        assert_eq!(calls[0].op, OpKind::Getattr);
        let done = mock.completions();
        assert_eq!(
            (done[0].seq, done[0].outcome),
            (0, Err(Code::NotImplemented))
        );
    }

    #[test]
    fn calls_record_the_op_arguments_caller_thread_deadline_and_cancellation() {
        let mock = MockVfs::new();
        mock.always_lookup(Script::ok(entry(9)));
        let c = Caller::new(42, 43, Some(4444));
        let token = CancelToken::new();
        token.cancel();
        let deadline = Instant::now() + Duration::from_secs(5);
        let op = OpCtx::new(OpKind::Lookup, &c)
            .with_cancel(&token)
            .with_deadline(deadline);
        let id = op.op;
        let got = Blocking::run(|r| mock.lookup(&op, 3, Name::new("name"), r)).unwrap();
        assert_eq!(got.attr.ino, 9);
        let call = mock.last_call().unwrap();
        assert_eq!(
            call.args,
            Args::Lookup {
                parent: 3,
                name: "name".into()
            }
        );
        assert_eq!(call.op_id, id);
        assert_eq!(
            call.caller,
            CallerInfo {
                uid: 42,
                gid: 43,
                pid: Some(4444)
            }
        );
        assert_eq!(call.thread.id, std::thread::current().id());
        assert_eq!(call.deadline, Some(deadline));
        assert!(call.cancelled);
        assert!(call.summary().contains("lookup") && call.summary().contains("\"name\""));
        // Order and filtering.
        let c2 = caller();
        let _ = Blocking::run(|r| mock.lookup(&cx(OpKind::Lookup, &c2), 1, Name::new("b"), r));
        let _ = Blocking::run(|r| mock.getattr(&cx(OpKind::Getattr, &c2), 1, None, r));
        let seqs: Vec<u64> = mock.calls().iter().map(|c| c.seq).collect();
        assert_eq!(seqs, [0, 1, 2]);
        assert_eq!(mock.calls_of(OpKind::Lookup).len(), 2);
        mock.clear_records();
        assert!(mock.calls().is_empty() && mock.completions().is_empty());
    }

    #[test]
    fn every_op_records_its_arguments() {
        let mock = MockVfs::reference(FrontendCaps::linux_fuse(true));
        let c = caller();
        let mut kinds = std::collections::HashSet::new();
        let f = |kind| cx(kind, &c);
        let root = ROOT_INO;
        let e = Blocking::run(|r| mock.mkdir(&f(OpKind::Mkdir), root, Name::new("d"), 0o755, r))
            .unwrap();
        let (fe, fo) = Blocking::run(|r| {
            mock.create(
                &f(OpKind::Create),
                root,
                Name::new("f"),
                0o100_644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner(3),
                r,
            )
        })
        .unwrap();
        let ino = fe.attr.ino;
        Blocking::run(|r| {
            mock.write(
                &f(OpKind::Write),
                ino,
                fo.fh,
                0,
                WriteData::Borrowed(b"abc"),
                OpenFlags::WRITE,
                r,
            )
        })
        .unwrap();
        Blocking::run(|r| mock.read(&f(OpKind::Read), ino, fo.fh, 0, 8, r)).unwrap();
        Blocking::run(|r| mock.fsync(&f(OpKind::Fsync), ino, fo.fh, Durability::Local, r)).unwrap();
        Blocking::run(|r| {
            mock.setxattr(
                &f(OpKind::Setxattr),
                ino,
                XattrName::new("user.a"),
                b"1",
                SetXattrFlags::CREATE,
                r,
            )
        })
        .unwrap();
        Blocking::run(|r| mock.seek(&f(OpKind::Seek), ino, fo.fh, 0, SeekWhence::Data, r)).unwrap();
        Blocking::run(|r| {
            mock.fallocate(
                &f(OpKind::Fallocate),
                ino,
                fo.fh,
                0,
                8,
                FallocateMode::KEEP_SIZE,
                r,
            )
        })
        .unwrap();
        Blocking::run(|r| {
            mock.rename(
                &f(OpKind::Rename),
                root,
                Name::new("f"),
                e.attr.ino,
                Name::new("g"),
                RenameFlags::NOREPLACE,
                r,
            )
        })
        .unwrap();
        let calls = mock.calls();
        for call in &calls {
            kinds.insert(call.op);
        }
        assert!(calls.iter().any(|c| c.args
            == Args::Write {
                ino,
                fh: fo.fh,
                off: 0,
                data: b"abc".to_vec(),
                flags: OpenFlags::WRITE
            }));
        assert!(calls.iter().any(|c| c.args
            == Args::Create {
                parent: root,
                name: "f".into(),
                mode: 0o100_644,
                flags: OpenFlags::READ | OpenFlags::WRITE,
                owner: OpenOwner(3)
            }));
        assert!(calls.iter().any(
            |c| matches!(&c.args, Args::Rename { flags, .. } if *flags == RenameFlags::NOREPLACE)
        ));
        assert!(calls.iter().any(|c| matches!(&c.args, Args::Setxattr { flags, value, .. } if *flags == SetXattrFlags::CREATE && value == b"1")));
        assert_eq!(kinds.len(), 9);
    }

    #[test]
    fn scripts_reply_in_order_then_stick() {
        let mock = MockVfs::new();
        mock.on_getattr(Script::ok(attr(1)))
            .on_getattr(Script::fail(Code::Stale))
            .always_getattr(Script::fail(Code::Busy));
        let c = caller();
        let get = || Blocking::run(|r| mock.getattr(&cx(OpKind::Getattr, &c), 1, None, r));
        assert_eq!(get().unwrap().ino, 1);
        assert_eq!(get(), Err(Code::Stale.into()));
        assert_eq!(get(), Err(Code::Busy.into()));
        assert_eq!(get(), Err(Code::Busy.into()));
    }

    #[test]
    fn a_closure_script_sees_the_call() {
        let mock = MockVfs::new();
        mock.always_lookup(Script::With(Arc::new(|call| match &call.args {
            Args::Lookup { name, .. } if name.as_bytes() == b"here" => Ok(entry(11)),
            _ => Err(Code::NotFound.into()),
        })));
        let c = caller();
        let look = |n: &'static str| {
            Blocking::run(|r| mock.lookup(&cx(OpKind::Lookup, &c), 1, Name::new(n), r))
        };
        assert_eq!(look("here").unwrap().attr.ino, 11);
        assert_eq!(look("gone"), Err(Code::NotFound.into()));
    }

    #[test]
    fn a_deferred_script_returns_at_once_and_completes_from_another_thread_after_the_delay() {
        let mock = MockVfs::new();
        mock.on_unlink(Script::Defer {
            after: Duration::from_millis(60),
            result: Err(Code::Busy.into()),
        });
        let c = caller();
        let (r, wait) = Blocking::<()>::pair();
        let started = Instant::now();
        mock.unlink(&cx(OpKind::Unlink, &c), 1, Name::new("x"), r);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "the call returned before completing"
        );
        assert!(mock.completions().is_empty());
        assert_eq!(wait.wait(), Err(Code::Busy.into()));
        assert!(started.elapsed() >= Duration::from_millis(55));
        assert!(mock.wait_for_completions(1, Duration::from_secs(5)));
        assert!(mock.wait_idle(Duration::from_secs(5)));
        let done = mock.completions();
        assert_eq!(done.len(), 1);
        assert_ne!(done[0].thread.id, std::thread::current().id());
        assert_eq!(done[0].thread.name.as_deref(), Some("mock-deferred"));
        assert_eq!(done[0].outcome, Err(Code::Busy));
    }

    #[test]
    fn a_deferred_closure_runs_on_the_completing_thread() {
        let mock = MockVfs::new();
        let seen = Arc::new(Mutex::new(None));
        let s2 = seen.clone();
        mock.on_statfs(Script::DeferWith {
            after: Duration::from_millis(1),
            f: Arc::new(move |_| {
                *s2.lock().unwrap() = Some(std::thread::current().id());
                Err(Code::Io.into())
            }),
        });
        let c = caller();
        let r = Blocking::run(|r| mock.statfs(&cx(OpKind::Statfs, &c), 1, r));
        assert_eq!(r, Err(Code::Io.into()));
        assert_ne!(*seen.lock().unwrap(), Some(std::thread::current().id()));
    }

    #[test]
    fn a_dropped_script_completes_through_the_responders_fail_safe() {
        let mock = MockVfs::new();
        mock.always_readlink(Script::Drop);
        let c = caller();
        // Blocking answers Io from its drop; the mock records nothing to
        // complete, since nothing completed.
        let r = Blocking::run(|r| mock.readlink(&cx(OpKind::Readlink, &c), 1, r));
        assert_eq!(r, Err(Code::Io.into()));
        assert!(mock.completions().is_empty());
        assert_eq!(mock.calls().len(), 1);
        // The same through a FnResponder, exactly once.
        let calls = Arc::new(Mutex::new(Vec::new()));
        let c2 = calls.clone();
        mock.readlink(
            &cx(OpKind::Readlink, &c),
            1,
            crate::FnResponder::new(move |r| c2.lock().unwrap().push(r)),
        );
        assert_eq!(*calls.lock().unwrap(), vec![Err(Code::Io.into())]);
    }

    #[test]
    fn a_held_op_never_completes_until_the_mock_drops() {
        let mock = MockVfs::new();
        mock.always_fsync(Script::Hold);
        let c = caller();
        let (r, wait) = Blocking::<()>::pair();
        mock.fsync(&cx(OpKind::Fsync, &c), 1, Fh(1), Durability::Local, r);
        assert_eq!(
            wait.wait_timeout(Duration::from_millis(80)),
            Err(Code::TimedOut.into())
        );
        drop(mock);
        assert_eq!(
            wait.wait(),
            Err(Code::Io.into()),
            "released, the responder's fail-safe answers"
        );
    }

    #[test]
    fn a_readdir_script_fills_the_sink_until_it_is_full() {
        let mock = MockVfs::new();
        let entries: Vec<DirEntry> = (0..5)
            .map(|i| DirEntry {
                ino: 10 + i,
                next: i + 1,
                kind: FileKind::File,
                name: format!("e{i}").into(),
            })
            .collect();
        mock.always_readdir(Script::ok(entries));
        let c = caller();
        let (sink, wait) = CollectDir::pair(3);
        mock.readdir(&cx(OpKind::Readdir, &c), 1, Fh(0), 0, false, sink);
        let got = wait.wait().unwrap();
        assert_eq!(got.len(), 3, "the sink was full after three");
        assert_eq!(got[2].name, "e2".into());
        assert_eq!(
            mock.last_call().unwrap().args,
            Args::Readdir {
                ino: 1,
                fh: Fh(0),
                cookie: 0,
                plus: false
            }
        );
    }

    #[test]
    fn reference_mode_is_a_working_filesystem_and_scripts_take_precedence() {
        let mock = MockVfs::reference(FrontendCaps::linux_fuse(false));
        let c = caller();
        let made = Blocking::run(|r| {
            mock.mkdir(&cx(OpKind::Mkdir, &c), ROOT_INO, Name::new("d"), 0o755, r)
        })
        .unwrap();
        assert_eq!(made.attr.kind, FileKind::Dir);
        assert_eq!(
            (made.attr.uid, made.attr.gid),
            (1000, 100),
            "owned by the caller"
        );
        let looked =
            Blocking::run(|r| mock.lookup(&cx(OpKind::Lookup, &c), ROOT_INO, Name::new("d"), r))
                .unwrap();
        assert_eq!(looked.attr.ino, made.attr.ino);
        // One injected failure in an otherwise working filesystem; then the
        // reference again.
        mock.on_lookup(Script::fail(Code::Io));
        assert_eq!(
            Blocking::run(|r| mock.lookup(&cx(OpKind::Lookup, &c), ROOT_INO, Name::new("d"), r)),
            Err(Code::Io.into())
        );
        assert!(Blocking::run(|r| mock.lookup(
            &cx(OpKind::Lookup, &c),
            ROOT_INO,
            Name::new("d"),
            r
        ))
        .is_ok());
        // An explicit fall-through.
        mock.always_lookup(Script::Reference);
        assert!(Blocking::run(|r| mock.lookup(
            &cx(OpKind::Lookup, &c),
            ROOT_INO,
            Name::new("d"),
            r
        ))
        .is_ok());
        assert_eq!(mock.completions().len(), mock.calls().len());
    }

    #[test]
    fn reference_mode_honours_the_capabilities_it_is_given() {
        let mut caps = FrontendCaps::linux_fuse(false);
        caps.hard_links = false;
        caps.xattrs = crate::XattrSupport::None;
        caps.fallocate = false;
        let mock = MockVfs::reference(caps);
        let c = caller();
        let (e, o) = Blocking::run(|r| {
            mock.create(
                &cx(OpKind::Create, &c),
                ROOT_INO,
                Name::new("f"),
                0o100_644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        let ino = e.attr.ino;
        assert_eq!(
            Blocking::run(|r| mock.link(&cx(OpKind::Link, &c), ino, ROOT_INO, Name::new("l"), r))
                .unwrap_err()
                .code(),
            Code::NotSupported
        );
        assert_eq!(
            Blocking::run(|r| mock.listxattr(&cx(OpKind::Listxattr, &c), ino, r))
                .unwrap_err()
                .code(),
            Code::NotSupported
        );
        assert_eq!(
            Blocking::run(|r| mock.fallocate(
                &cx(OpKind::Fallocate, &c),
                ino,
                o.fh,
                0,
                4,
                FallocateMode::empty(),
                r
            ))
            .unwrap_err()
            .code(),
            Code::NotSupported
        );
        // Locks are not implemented without cluster_locks.
        let spec = LockSpec {
            owner: LockOwner(1),
            range: LockRange { start: 0, end: 10 },
            kind: LockKind::Write,
            pid: 1,
        };
        assert_eq!(
            Blocking::run(|r| mock.lock_acquire(
                &cx(OpKind::LockAcquire, &c),
                ino,
                o.fh,
                spec,
                false,
                r
            ))
            .unwrap_err()
            .code(),
            Code::NotImplemented
        );
    }

    #[test]
    fn a_blocked_lock_completes_from_the_wait_thread_and_honours_cancellation() {
        let mock = MockVfs::reference(FrontendCaps::linux_fuse(true));
        let c = caller();
        let (e, o) = Blocking::run(|r| {
            mock.create(
                &cx(OpKind::Create, &c),
                ROOT_INO,
                Name::new("f"),
                0o100_644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        let ino = e.attr.ino;
        let lock = |owner| LockSpec {
            owner: LockOwner(owner),
            range: LockRange { start: 0, end: 100 },
            kind: LockKind::Write,
            pid: owner as u32,
        };
        let acquire = |owner: u64, sleep: bool, token: Option<&CancelToken>| {
            let (r, wait) = Blocking::<()>::pair();
            let mut op = OpCtx::new(OpKind::LockAcquire, &c);
            if let Some(t) = token {
                op = op.with_cancel(t);
            }
            mock.lock_acquire(&op, ino, o.fh, lock(owner), sleep, r);
            wait
        };
        assert_eq!(acquire(1, false, None).wait(), Ok(()));
        assert_eq!(
            acquire(2, false, None).wait(),
            Err(Code::Again.into()),
            "F_SETLK does not wait"
        );
        let waiting = acquire(2, true, None);
        assert_eq!(
            waiting.wait_timeout(Duration::from_millis(60)),
            Err(Code::TimedOut.into())
        );
        let test =
            Blocking::run(|r| mock.lock_test(&cx(OpKind::LockTest, &c), ino, o.fh, lock(3), r))
                .unwrap();
        assert!(matches!(test, LockStatus::Locked { pid: 1, .. }));
        Blocking::run(|r| {
            mock.lock_release(
                &cx(OpKind::LockRelease, &c),
                ino,
                o.fh,
                LockOwner(1),
                LockRange { start: 0, end: 100 },
                r,
            )
        })
        .unwrap();
        assert_eq!(waiting.wait(), Ok(()));
        let done = mock.completions();
        let waiter = done
            .iter()
            .rev()
            .find(|d| d.op == OpKind::LockAcquire && d.outcome.is_ok())
            .unwrap();
        assert_eq!(
            waiter.thread.name.as_deref(),
            Some("ref-lock-wait"),
            "granted from the wait thread"
        );
        // A token cancels a wait, before or during.
        let token = CancelToken::new();
        let waiting = acquire(3, true, Some(&token));
        assert_eq!(
            waiting.wait_timeout(Duration::from_millis(40)),
            Err(Code::TimedOut.into())
        );
        token.cancel();
        assert_eq!(waiting.wait(), Err(Code::Intr.into()));
        assert_eq!(
            acquire(3, true, Some(&token)).wait(),
            Err(Code::Intr.into()),
            "already cancelled"
        );
    }

    #[test]
    fn views_share_one_tree_and_events_reach_the_other_view_only() {
        struct Rec(Mutex<Vec<crate::Invalidation>>);
        impl FrontendEvents for Rec {
            fn invalidate(&self, batch: &[crate::Invalidation]) {
                self.0.lock().unwrap().extend_from_slice(batch);
            }
        }
        let a = MockVfs::reference(FrontendCaps::linux_fuse(false));
        let b = a.view_of("/", false).unwrap();
        let rec = Arc::new(Rec(Mutex::new(Vec::new())));
        a.set_events(rec.clone());
        let c = caller();
        // b's mutation reaches a's sink; a's own does not.
        Blocking::run(|r| {
            b.mkdir(
                &cx(OpKind::Mkdir, &c),
                ROOT_INO,
                Name::new("from-b"),
                0o755,
                r,
            )
        })
        .unwrap();
        Blocking::run(|r| {
            a.mkdir(
                &cx(OpKind::Mkdir, &c),
                ROOT_INO,
                Name::new("from-a"),
                0o755,
                r,
            )
        })
        .unwrap();
        a.settle();
        let told = rec.0.lock().unwrap().clone();
        assert_eq!(
            told,
            vec![crate::Invalidation::Entry {
                parent: ROOT_INO,
                name: "from-b".into()
            }]
        );
        // The tree is shared.
        assert!(Blocking::run(|r| a.lookup(
            &cx(OpKind::Lookup, &c),
            ROOT_INO,
            Name::new("from-b"),
            r
        ))
        .is_ok());
        assert!(Blocking::run(|r| b.lookup(
            &cx(OpKind::Lookup, &c),
            ROOT_INO,
            Name::new("from-a"),
            r
        ))
        .is_ok());
        assert_eq!(
            MockVfs::new().view_of("/", false).err(),
            Some(Code::NotSupported)
        );
    }

    #[test]
    fn a_mock_is_cloneable_and_clones_share_records() {
        let mock = MockVfs::new();
        let other = mock.clone();
        let c = caller();
        let _ = Blocking::run(|r| other.getattr(&cx(OpKind::Getattr, &c), 1, None, r));
        assert_eq!(mock.calls().len(), 1);
    }
}
