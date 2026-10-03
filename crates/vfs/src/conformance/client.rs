//! [`Client`]: the synchronous face the conformance tests drive a target
//! through, and [`Pending`], its asynchronous one.
//!
//! A `Client` is one caller (a uid/gid) on one view. Each method is one
//! [`crate::Vfs`] op, run to completion with a bounded wait: an op that
//! never answers is a *failure* of the target ("hung"), reported with the
//! op's name, not a hung test run.

use super::facade::{Done, DynVfs};
use crate::ctx::{Caller, CancelToken, OpCtx, OpKind};
use crate::error::VfsResult;
use crate::name::XattrNameBuf;
use crate::responder::DirEntry;
use crate::types::{
    Attr, Durability, Entry, FallocateMode, Fh, Ino, LockKind, LockOwner, LockRange, LockSpec,
    LockStatus, OpenFlags, Opened, ReadData, RenameFlags, SeekWhence, SetAttr, SetXattrFlags,
    StatFs,
};
use constellation_types::{Code, Rdev};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::{Duration, Instant};

/// How long an op may take before the target is called hung.
pub const HANG: Duration = Duration::from_secs(60);

/// An op's completion as the probe saw it: the result (`None`: the
/// responder was dropped without completing) and the thread it completed
/// on.
pub type Completed<T> = (Option<VfsResult<T>>, ThreadId);

/// An op in flight.
pub struct Pending<T> {
    rx: mpsc::Receiver<Completed<T>>,
    got: Option<Completed<T>>,
    what: &'static str,
}

impl<T> Pending<T> {
    fn new(what: &'static str) -> (Pending<T>, Done<T>)
    where
        T: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let done: Done<T> = Box::new(move |result| {
            let _ = tx.send((result, std::thread::current().id()));
        });
        (
            Pending {
                rx,
                got: None,
                what,
            },
            done,
        )
    }

    /// Whether the op completes within `within` (its result is kept for
    /// [`Pending::wait`]).
    pub fn completes_within(&mut self, within: Duration) -> bool {
        if self.got.is_some() {
            return true;
        }
        match self.rx.recv_timeout(within) {
            Ok(c) => {
                self.got = Some(c);
                true
            }
            Err(_) => false,
        }
    }

    /// The completion (result or drop, and the thread), waiting at most
    /// [`HANG`].
    pub fn outcome(mut self) -> Completed<T> {
        if !self.completes_within(HANG) {
            panic!("{} hung: not completed within {HANG:?}", self.what);
        }
        self.got.take().expect("completed")
    }

    /// The op's result. A responder dropped without completing is a
    /// failure of the target.
    pub fn wait(self) -> VfsResult<T> {
        let what = self.what;
        match self.outcome().0 {
            Some(result) => result,
            None => panic!("{what}: the target dropped the responder without completing it"),
        }
    }
}

/// One caller on one view; see the module doc.
#[derive(Clone)]
pub struct Client {
    vfs: Arc<dyn DynVfs>,
    uid: u32,
    gid: u32,
    root: Ino,
}

const RW: OpenFlags = OpenFlags::from_bits(OpenFlags::READ.bits() | OpenFlags::WRITE.bits());

impl Client {
    pub(crate) fn new(vfs: Arc<dyn DynVfs>, root: Ino, uid: u32, gid: u32) -> Client {
        Client {
            vfs,
            uid,
            gid,
            root,
        }
    }

    /// The view's root inode.
    pub fn root(&self) -> Ino {
        self.root
    }

    pub fn uid(&self) -> u32 {
        self.uid
    }

    pub fn gid(&self) -> u32 {
        self.gid
    }

    /// The same view as another caller.
    pub fn as_user(&self, uid: u32, gid: u32) -> Client {
        Client {
            vfs: self.vfs.clone(),
            uid,
            gid,
            root: self.root,
        }
    }

    /// The same caller as `root` (uid 0).
    pub fn as_root(&self) -> Client {
        self.as_user(0, 0)
    }

    pub fn dyn_vfs(&self) -> &Arc<dyn DynVfs> {
        &self.vfs
    }

    fn caller(&self) -> Caller {
        Caller::with_groups(self.uid, self.gid, &[])
    }

    /// Start an op with a plain context.
    fn start<T: Send + 'static>(
        &self,
        kind: OpKind,
        what: &'static str,
        f: impl FnOnce(&dyn DynVfs, &OpCtx<'_>, Done<T>),
    ) -> Pending<T> {
        let caller = self.caller();
        let cx = OpCtx::new(kind, &caller);
        let (pending, done) = Pending::new(what);
        f(&*self.vfs, &cx, done);
        pending
    }

    // ------------------------------------------------------------ namespace

    pub fn lookup(&self, parent: Ino, name: &str) -> VfsResult<Entry> {
        self.lookup_bytes(parent, name.as_bytes())
    }

    pub fn lookup_bytes(&self, parent: Ino, name: &[u8]) -> VfsResult<Entry> {
        self.start(OpKind::Lookup, "lookup", |v, cx, d| {
            v.lookup(cx, parent, name, d)
        })
        .wait()
    }

    pub fn getattr(&self, ino: Ino) -> VfsResult<Attr> {
        self.getattr_fh(ino, None)
    }

    pub fn getattr_fh(&self, ino: Ino, fh: Option<Fh>) -> VfsResult<Attr> {
        self.start(OpKind::Getattr, "getattr", |v, cx, d| {
            v.getattr(cx, ino, fh, d)
        })
        .wait()
    }

    pub fn setattr(&self, ino: Ino, fh: Option<Fh>, set: &SetAttr) -> VfsResult<Attr> {
        self.start(OpKind::Setattr, "setattr", |v, cx, d| {
            v.setattr(cx, ino, fh, set, d)
        })
        .wait()
    }

    pub fn truncate(&self, ino: Ino, fh: Option<Fh>, size: u64) -> VfsResult<Attr> {
        self.setattr(
            ino,
            fh,
            &SetAttr {
                size: Some(size),
                ..SetAttr::default()
            },
        )
    }

    pub fn readlink(&self, ino: Ino) -> VfsResult<Vec<u8>> {
        self.start(OpKind::Readlink, "readlink", |v, cx, d| {
            v.readlink(cx, ino, d)
        })
        .wait()
    }

    pub fn mknod(&self, parent: Ino, name: &str, mode: u32, rdev: Rdev) -> VfsResult<Entry> {
        self.start(OpKind::Mknod, "mknod", |v, cx, d| {
            v.mknod(cx, parent, name.as_bytes(), mode, rdev, d)
        })
        .wait()
    }

    pub fn mkdir(&self, parent: Ino, name: &str) -> VfsResult<Entry> {
        self.mkdir_bytes(parent, name.as_bytes())
    }

    pub fn mkdir_bytes(&self, parent: Ino, name: &[u8]) -> VfsResult<Entry> {
        self.start(OpKind::Mkdir, "mkdir", |v, cx, d| {
            v.mkdir(cx, parent, name, 0o755, d)
        })
        .wait()
    }

    pub fn symlink(&self, parent: Ino, name: &str, target: &str) -> VfsResult<Entry> {
        self.start(OpKind::Symlink, "symlink", |v, cx, d| {
            v.symlink(cx, parent, name.as_bytes(), target.as_bytes(), d)
        })
        .wait()
    }

    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &str) -> VfsResult<Entry> {
        self.start(OpKind::Link, "link", |v, cx, d| {
            v.link(cx, ino, new_parent, new_name.as_bytes(), d)
        })
        .wait()
    }

    pub fn unlink(&self, parent: Ino, name: &str) -> VfsResult<()> {
        self.start(OpKind::Unlink, "unlink", |v, cx, d| {
            v.unlink(cx, parent, name.as_bytes(), d)
        })
        .wait()
    }

    pub fn rmdir(&self, parent: Ino, name: &str) -> VfsResult<()> {
        self.start(OpKind::Rmdir, "rmdir", |v, cx, d| {
            v.rmdir(cx, parent, name.as_bytes(), d)
        })
        .wait()
    }

    pub fn rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> VfsResult<()> {
        self.rename_flags(parent, name, new_parent, new_name, RenameFlags::empty())
    }

    pub fn rename_flags(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: RenameFlags,
    ) -> VfsResult<()> {
        self.start(OpKind::Rename, "rename", |v, cx, d| {
            v.rename(
                cx,
                parent,
                name.as_bytes(),
                new_parent,
                new_name.as_bytes(),
                flags,
                d,
            )
        })
        .wait()
    }

    // -------------------------------------------------------------- file io

    pub fn open(&self, ino: Ino, flags: OpenFlags) -> VfsResult<Opened> {
        self.start(OpKind::Open, "open", |v, cx, d| v.open(cx, ino, flags, d))
            .wait()
    }

    /// `open` for reading and writing.
    pub fn open_rw(&self, ino: Ino) -> VfsResult<Opened> {
        self.open(ino, RW)
    }

    pub fn create_flags(
        &self,
        parent: Ino,
        name: &str,
        flags: OpenFlags,
    ) -> VfsResult<(Entry, Opened)> {
        self.start(OpKind::Create, "create", |v, cx, d| {
            v.create(cx, parent, name.as_bytes(), 0o100_644, flags, d)
        })
        .wait()
    }

    /// `create` for reading and writing (no `EXCL`).
    pub fn create(&self, parent: Ino, name: &str) -> VfsResult<(Entry, Opened)> {
        self.create_flags(parent, name, RW)
    }

    /// `create` with `EXCL`.
    pub fn create_excl(&self, parent: Ino, name: &str) -> VfsResult<(Entry, Opened)> {
        self.create_flags(parent, name, RW | OpenFlags::EXCL)
    }

    pub fn read(&self, ino: Ino, fh: Fh, off: u64, len: u32) -> VfsResult<Vec<u8>> {
        self.read_async(ino, fh, off, len)
            .wait()
            .and_then(|data: ReadData| data.contiguous().map(std::borrow::Cow::into_owned))
    }

    pub fn read_async(&self, ino: Ino, fh: Fh, off: u64, len: u32) -> Pending<ReadData> {
        self.start(OpKind::Read, "read", |v, cx, d| {
            v.read(cx, ino, fh, off, len, d)
        })
    }

    pub fn write(&self, ino: Ino, fh: Fh, off: u64, data: &[u8]) -> VfsResult<u32> {
        self.write_flags(ino, fh, off, data, OpenFlags::WRITE)
    }

    pub fn write_flags(
        &self,
        ino: Ino,
        fh: Fh,
        off: u64,
        data: &[u8],
        flags: OpenFlags,
    ) -> VfsResult<u32> {
        self.start(OpKind::Write, "write", |v, cx, d| {
            v.write(cx, ino, fh, off, data, flags, d)
        })
        .wait()
    }

    pub fn flush(&self, ino: Ino, fh: Fh, owner: LockOwner) -> VfsResult<()> {
        self.start(OpKind::Flush, "flush", |v, cx, d| {
            v.flush(cx, ino, fh, owner, d)
        })
        .wait()
    }

    pub fn release(&self, ino: Ino, fh: Fh) -> VfsResult<()> {
        self.start(OpKind::Release, "release", |v, cx, d| {
            v.release(cx, ino, fh, RW, None, d)
        })
        .wait()
    }

    /// `close(2)`: the flush fence, then the release.
    pub fn close(&self, ino: Ino, fh: Fh) -> VfsResult<()> {
        self.flush(ino, fh, LockOwner(fh.0 | 1 << 40))?;
        self.release(ino, fh)
    }

    pub fn fsync(&self, ino: Ino, fh: Fh, level: Durability) -> VfsResult<()> {
        self.start(OpKind::Fsync, "fsync", |v, cx, d| {
            v.fsync(cx, ino, fh, level, d)
        })
        .wait()
    }

    /// One `readdir` page of at most `limit` entries from `cookie`.
    pub fn readdir_page(
        &self,
        ino: Ino,
        cookie: u64,
        limit: usize,
        plus: bool,
    ) -> VfsResult<Vec<DirEntry>> {
        self.start(OpKind::Readdir, "readdir", |v, cx, d| {
            v.readdir(cx, ino, Fh(0), cookie, plus, limit, d)
        })
        .wait()
    }

    /// Every entry of the directory (including `.` and `..`), read in
    /// pages of `page` entries, resuming from each page's last cookie.
    pub fn readdir_all(&self, ino: Ino, page: usize) -> VfsResult<Vec<DirEntry>> {
        let mut all = Vec::new();
        let mut cookie = 0;
        let mut cookies = std::collections::HashSet::new();
        loop {
            let got = self.readdir_page(ino, cookie, page, false)?;
            let Some(last) = got.last() else { break };
            // A cookie that comes back (not just the one resumed from) is
            // a cycle: the listing would never end.
            assert!(
                last.next != cookie && cookies.insert(last.next),
                "readdir cookie {cookie} did not advance past {:?}",
                last.name
            );
            cookie = last.next;
            all.extend(got);
        }
        Ok(all)
    }

    /// The names in the directory, without `.` and `..`, sorted.
    pub fn names(&self, ino: Ino) -> VfsResult<Vec<String>> {
        let mut names: Vec<String> = self
            .readdir_all(ino, 64)?
            .into_iter()
            .map(|e| String::from_utf8_lossy(e.name.as_bytes()).into_owned())
            .filter(|n| n != "." && n != "..")
            .collect();
        names.sort();
        Ok(names)
    }

    pub fn statfs(&self, ino: Ino) -> VfsResult<StatFs> {
        self.start(OpKind::Statfs, "statfs", |v, cx, d| v.statfs(cx, ino, d))
            .wait()
    }

    pub fn fallocate(
        &self,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
    ) -> VfsResult<()> {
        self.start(OpKind::Fallocate, "fallocate", |v, cx, d| {
            v.fallocate(cx, ino, fh, off, len, mode, d)
        })
        .wait()
    }

    pub fn seek(&self, ino: Ino, fh: Fh, off: u64, whence: SeekWhence) -> VfsResult<u64> {
        self.start(OpKind::Seek, "seek", |v, cx, d| {
            v.seek(cx, ino, fh, off, whence, d)
        })
        .wait()
    }

    // --------------------------------------------------------------- xattrs

    pub fn getxattr(&self, ino: Ino, name: &str) -> VfsResult<Vec<u8>> {
        self.start(OpKind::Getxattr, "getxattr", |v, cx, d| {
            v.getxattr(cx, ino, name.as_bytes(), d)
        })
        .wait()
    }

    pub fn setxattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        flags: SetXattrFlags,
    ) -> VfsResult<()> {
        self.setxattr_bytes(ino, name.as_bytes(), value, flags)
    }

    pub fn setxattr_bytes(
        &self,
        ino: Ino,
        name: &[u8],
        value: &[u8],
        flags: SetXattrFlags,
    ) -> VfsResult<()> {
        self.start(OpKind::Setxattr, "setxattr", |v, cx, d| {
            v.setxattr(cx, ino, name, value, flags, d)
        })
        .wait()
    }

    /// The xattr names, as strings.
    pub fn listxattr(&self, ino: Ino) -> VfsResult<Vec<String>> {
        let names: Vec<XattrNameBuf> = self
            .start(OpKind::Listxattr, "listxattr", |v, cx, d| {
                v.listxattr(cx, ino, d)
            })
            .wait()?;
        Ok(names
            .into_iter()
            .map(|n| String::from_utf8_lossy(n.as_bytes()).into_owned())
            .collect())
    }

    pub fn removexattr(&self, ino: Ino, name: &str) -> VfsResult<()> {
        self.start(OpKind::Removexattr, "removexattr", |v, cx, d| {
            v.removexattr(cx, ino, name.as_bytes(), d)
        })
        .wait()
    }

    // ---------------------------------------------------------------- locks

    pub fn lock_test(&self, ino: Ino, fh: Fh, lock: LockSpec) -> VfsResult<LockStatus> {
        self.start(OpKind::LockTest, "lock_test", |v, cx, d| {
            v.lock_test(cx, ino, fh, lock, d)
        })
        .wait()
    }

    /// `F_SETLK`: never waits.
    pub fn try_lock(&self, ino: Ino, fh: Fh, lock: LockSpec) -> VfsResult<()> {
        self.start(OpKind::LockAcquire, "lock_acquire", |v, cx, d| {
            v.lock_acquire(cx, ino, fh, lock, false, d)
        })
        .wait()
    }

    /// `F_SETLKW`, started: the returned op completes when the lock is
    /// granted, cancelled or timed out. `cancel` and `deadline` are the
    /// op's own.
    pub fn lock_wait_async(
        &self,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        cancel: Option<&CancelToken>,
        deadline: Option<Instant>,
    ) -> Pending<()> {
        let caller = self.caller();
        let mut cx = OpCtx::new(OpKind::LockAcquire, &caller);
        if let Some(c) = cancel {
            cx = cx.with_cancel(c);
        }
        if let Some(d) = deadline {
            cx = cx.with_deadline(d);
        }
        let (pending, done) = Pending::new("lock_acquire(sleep)");
        self.vfs.lock_acquire(&cx, ino, fh, lock, true, done);
        pending
    }

    pub fn unlock(&self, ino: Ino, fh: Fh, owner: LockOwner, range: LockRange) -> VfsResult<()> {
        self.start(OpKind::LockRelease, "lock_release", |v, cx, d| {
            v.lock_release(cx, ino, fh, owner, range, d)
        })
        .wait()
    }

    pub fn sync_view(&self) -> VfsResult<()> {
        self.start(OpKind::SyncView, "sync_view", |v, cx, d| v.sync_view(cx, d))
            .wait()
    }

    // ------------------------------------------------- whole-file helpers

    /// Create `name` in `parent` holding `content`, closed.
    pub fn put(&self, parent: Ino, name: &str, content: &[u8]) -> VfsResult<Entry> {
        let (entry, opened) = self.create_excl(parent, name)?;
        let ino = entry.attr.ino;
        let mut off = 0;
        while off < content.len() {
            let end = (off + (256 << 10)).min(content.len());
            let n = self.write(ino, opened.fh, off as u64, &content[off..end])? as usize;
            assert!(n > 0, "a write of {} bytes wrote none", end - off);
            off += n;
        }
        self.close(ino, opened.fh)?;
        self.lookup(parent, name)
    }

    /// The whole content of `ino`, through a fresh handle.
    pub fn slurp(&self, ino: Ino) -> VfsResult<Vec<u8>> {
        let opened = self.open(ino, OpenFlags::READ)?;
        let mut out = Vec::new();
        loop {
            let chunk = self.read(ino, opened.fh, out.len() as u64, 256 << 10)?;
            if chunk.is_empty() {
                break;
            }
            out.extend_from_slice(&chunk);
        }
        self.release(ino, opened.fh)?;
        Ok(out)
    }

    /// `mkdir -p`: every directory of `path` (relative to the view root),
    /// the last one's inode.
    pub fn mkdirs(&self, path: &str) -> VfsResult<Ino> {
        let mut cur = self.root;
        for part in path.split('/').filter(|p| !p.is_empty()) {
            cur = match self.lookup(cur, part) {
                Ok(e) => e.attr.ino,
                Err(e) if e.code() == Code::NotFound => self.mkdir(cur, part)?.attr.ino,
                Err(e) => return Err(e),
            };
        }
        Ok(cur)
    }

    /// The entry at `path` (relative to the view root).
    pub fn resolve(&self, path: &str) -> VfsResult<Entry> {
        let mut cur = self.getattr(self.root)?;
        let mut entry = Entry {
            attr: cur.clone(),
            generation: 0,
        };
        for part in path.split('/').filter(|p| !p.is_empty()) {
            entry = self.lookup(cur.ino, part)?;
            cur = entry.attr.clone();
        }
        Ok(entry)
    }

    /// A whole-range write lock for `owner`.
    pub fn whole_file_lock(owner: u64, kind: LockKind) -> LockSpec {
        LockSpec {
            owner: LockOwner(owner),
            range: LockRange {
                start: 0,
                end: i64::MAX as u64,
            },
            kind,
            pid: owner as u32,
        }
    }
}
