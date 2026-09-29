//! An object-safe face of [`Vfs`], so a conformance test is one plain
//! function that runs against every target.
//!
//! [`Vfs`]'s ops are generic over their responder, which makes the trait
//! itself not object-safe. [`DynVfs`] repeats each op with a boxed
//! continuation instead of a generic responder and a blanket impl adapts
//! every `V: Vfs` to it. The continuation is called through a [`Probe`], a
//! responder that tells "completed" from "dropped without completing" —
//! the two look the same to [`crate::FnResponder`] (both call the function,
//! one with `Code::Io`), and a conformance test has to tell them apart.

use crate::ctx::OpCtx;
use crate::error::VfsResult;
use crate::name::NameBuf;
use crate::name::{Name, XattrName, XattrNameBuf};
use crate::responder::{DirEntry, DirSink, Responder};
use crate::types::{
    Attr, Durability, Entry, FallocateMode, Fh, FileKind, Ino, LockOwner, LockRange, LockSpec,
    LockStatus, OpenFlags, OpenOwner, Opened, ReadData, RenameFlags, SeekWhence, SetAttr,
    SetXattrFlags, StatFs, WriteData,
};
use crate::vfs::Vfs;
use constellation_types::Rdev;

/// What a probe reports: the op's result, or `None` when the responder was
/// dropped without ever completing.
pub type Done<T> = Box<dyn FnOnce(Option<VfsResult<T>>) + Send + 'static>;

/// A responder that reports to a [`Done`] continuation, telling completion
/// from drop.
pub struct Probe<T> {
    done: Option<Done<T>>,
}

impl<T> Probe<T> {
    pub fn new(done: Done<T>) -> Self {
        Self { done: Some(done) }
    }
}

impl<T: 'static> Responder<T> for Probe<T> {
    fn done(mut self, result: VfsResult<T>) {
        if let Some(done) = self.done.take() {
            done(Some(result));
        }
    }
}

impl<T> Drop for Probe<T> {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            done(None);
        }
    }
}

/// `readdir`'s combined sink and responder: collects up to `limit`
/// entries, reports them (or the failure) on completion.
struct ProbeDir {
    entries: Vec<DirEntry>,
    limit: usize,
    inner: Probe<Vec<DirEntry>>,
}

impl DirSink for ProbeDir {
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        if self.entries.len() >= self.limit {
            return true;
        }
        self.entries.push(DirEntry {
            ino,
            next,
            kind,
            name: NameBuf::new(name),
        });
        false
    }
}

impl Responder<()> for ProbeDir {
    fn done(self, result: VfsResult<()>) {
        let ProbeDir { entries, inner, .. } = self;
        inner.done(result.map(|()| entries));
    }
}

/// The ops of [`Vfs`] with boxed continuations; see the module doc. Names
/// are bytes; `limit` bounds a `readdir` page.
#[allow(clippy::too_many_arguments)]
pub trait DynVfs: Send + Sync {
    fn lookup(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], done: Done<Entry>);
    fn getattr(&self, cx: &OpCtx<'_>, ino: Ino, fh: Option<Fh>, done: Done<Attr>);
    fn setattr(&self, cx: &OpCtx<'_>, ino: Ino, fh: Option<Fh>, set: &SetAttr, done: Done<Attr>);
    fn readlink(&self, cx: &OpCtx<'_>, ino: Ino, done: Done<Vec<u8>>);
    fn mknod(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &[u8],
        mode: u32,
        rdev: Rdev,
        done: Done<Entry>,
    );
    fn mkdir(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], mode: u32, done: Done<Entry>);
    fn symlink(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], target: &[u8], done: Done<Entry>);
    fn link(&self, cx: &OpCtx<'_>, ino: Ino, new_parent: Ino, new_name: &[u8], done: Done<Entry>);
    fn unlink(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], done: Done<()>);
    fn rmdir(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], done: Done<()>);
    fn rename(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        flags: RenameFlags,
        done: Done<()>,
    );
    fn open(&self, cx: &OpCtx<'_>, ino: Ino, flags: OpenFlags, done: Done<Opened>);
    fn create(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &[u8],
        mode: u32,
        flags: OpenFlags,
        done: Done<(Entry, Opened)>,
    );
    fn read(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, off: u64, len: u32, done: Done<ReadData>);
    fn write(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        data: &[u8],
        flags: OpenFlags,
        done: Done<u32>,
    );
    fn flush(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, owner: LockOwner, done: Done<()>);
    fn release(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
        done: Done<()>,
    );
    fn fsync(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, level: Durability, done: Done<()>);
    fn readdir(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        cookie: u64,
        plus: bool,
        limit: usize,
        done: Done<Vec<DirEntry>>,
    );
    fn statfs(&self, cx: &OpCtx<'_>, ino: Ino, done: Done<StatFs>);
    fn fallocate(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
        done: Done<()>,
    );
    fn seek(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, off: u64, whence: SeekWhence, done: Done<u64>);
    fn getxattr(&self, cx: &OpCtx<'_>, ino: Ino, name: &[u8], done: Done<Vec<u8>>);
    fn setxattr(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        name: &[u8],
        value: &[u8],
        flags: SetXattrFlags,
        done: Done<()>,
    );
    fn listxattr(&self, cx: &OpCtx<'_>, ino: Ino, done: Done<Vec<XattrNameBuf>>);
    fn removexattr(&self, cx: &OpCtx<'_>, ino: Ino, name: &[u8], done: Done<()>);
    fn lock_test(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, lock: LockSpec, done: Done<LockStatus>);
    fn lock_acquire(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        sleep: bool,
        done: Done<()>,
    );
    fn lock_release(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        range: LockRange,
        done: Done<()>,
    );
    fn sync_view(&self, cx: &OpCtx<'_>, done: Done<()>);
}

impl<V: Vfs> DynVfs for V {
    fn lookup(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], done: Done<Entry>) {
        Vfs::lookup(self, cx, parent, Name::new(name), Probe::new(done));
    }

    fn getattr(&self, cx: &OpCtx<'_>, ino: Ino, fh: Option<Fh>, done: Done<Attr>) {
        Vfs::getattr(self, cx, ino, fh, Probe::new(done));
    }

    fn setattr(&self, cx: &OpCtx<'_>, ino: Ino, fh: Option<Fh>, set: &SetAttr, done: Done<Attr>) {
        Vfs::setattr(self, cx, ino, fh, set, Probe::new(done));
    }

    fn readlink(&self, cx: &OpCtx<'_>, ino: Ino, done: Done<Vec<u8>>) {
        Vfs::readlink(self, cx, ino, Probe::new(done));
    }

    fn mknod(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &[u8],
        mode: u32,
        rdev: Rdev,
        done: Done<Entry>,
    ) {
        Vfs::mknod(
            self,
            cx,
            parent,
            Name::new(name),
            mode,
            rdev,
            Probe::new(done),
        );
    }

    fn mkdir(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], mode: u32, done: Done<Entry>) {
        Vfs::mkdir(self, cx, parent, Name::new(name), mode, Probe::new(done));
    }

    fn symlink(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], target: &[u8], done: Done<Entry>) {
        Vfs::symlink(self, cx, parent, Name::new(name), target, Probe::new(done));
    }

    fn link(&self, cx: &OpCtx<'_>, ino: Ino, new_parent: Ino, new_name: &[u8], done: Done<Entry>) {
        Vfs::link(
            self,
            cx,
            ino,
            new_parent,
            Name::new(new_name),
            Probe::new(done),
        );
    }

    fn unlink(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], done: Done<()>) {
        Vfs::unlink(self, cx, parent, Name::new(name), Probe::new(done));
    }

    fn rmdir(&self, cx: &OpCtx<'_>, parent: Ino, name: &[u8], done: Done<()>) {
        Vfs::rmdir(self, cx, parent, Name::new(name), Probe::new(done));
    }

    fn rename(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        flags: RenameFlags,
        done: Done<()>,
    ) {
        Vfs::rename(
            self,
            cx,
            parent,
            Name::new(name),
            new_parent,
            Name::new(new_name),
            flags,
            Probe::new(done),
        );
    }

    fn open(&self, cx: &OpCtx<'_>, ino: Ino, flags: OpenFlags, done: Done<Opened>) {
        Vfs::open(self, cx, ino, flags, OpenOwner::NONE, Probe::new(done));
    }

    fn create(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &[u8],
        mode: u32,
        flags: OpenFlags,
        done: Done<(Entry, Opened)>,
    ) {
        Vfs::create(
            self,
            cx,
            parent,
            Name::new(name),
            mode,
            flags,
            OpenOwner::NONE,
            Probe::new(done),
        );
    }

    fn read(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, off: u64, len: u32, done: Done<ReadData>) {
        Vfs::read(self, cx, ino, fh, off, len, Probe::new(done));
    }

    fn write(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        data: &[u8],
        flags: OpenFlags,
        done: Done<u32>,
    ) {
        Vfs::write(
            self,
            cx,
            ino,
            fh,
            off,
            WriteData::Borrowed(data),
            flags,
            Probe::new(done),
        );
    }

    fn flush(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, owner: LockOwner, done: Done<()>) {
        Vfs::flush(self, cx, ino, fh, owner, Probe::new(done));
    }

    fn release(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
        done: Done<()>,
    ) {
        Vfs::release(self, cx, ino, fh, flags, owner, Probe::new(done));
    }

    fn fsync(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, level: Durability, done: Done<()>) {
        Vfs::fsync(self, cx, ino, fh, level, Probe::new(done));
    }

    fn readdir(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        cookie: u64,
        plus: bool,
        limit: usize,
        done: Done<Vec<DirEntry>>,
    ) {
        let sink = ProbeDir {
            entries: Vec::new(),
            limit,
            inner: Probe::new(done),
        };
        Vfs::readdir(self, cx, ino, fh, cookie, plus, sink);
    }

    fn statfs(&self, cx: &OpCtx<'_>, ino: Ino, done: Done<StatFs>) {
        Vfs::statfs(self, cx, ino, Probe::new(done));
    }

    fn fallocate(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
        done: Done<()>,
    ) {
        Vfs::fallocate(self, cx, ino, fh, off, len, mode, Probe::new(done));
    }

    fn seek(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        whence: SeekWhence,
        done: Done<u64>,
    ) {
        Vfs::seek(self, cx, ino, fh, off, whence, Probe::new(done));
    }

    fn getxattr(&self, cx: &OpCtx<'_>, ino: Ino, name: &[u8], done: Done<Vec<u8>>) {
        Vfs::getxattr(self, cx, ino, XattrName::new(name), Probe::new(done));
    }

    fn setxattr(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        name: &[u8],
        value: &[u8],
        flags: SetXattrFlags,
        done: Done<()>,
    ) {
        Vfs::setxattr(
            self,
            cx,
            ino,
            XattrName::new(name),
            value,
            flags,
            Probe::new(done),
        );
    }

    fn listxattr(&self, cx: &OpCtx<'_>, ino: Ino, done: Done<Vec<XattrNameBuf>>) {
        Vfs::listxattr(self, cx, ino, Probe::new(done));
    }

    fn removexattr(&self, cx: &OpCtx<'_>, ino: Ino, name: &[u8], done: Done<()>) {
        Vfs::removexattr(self, cx, ino, XattrName::new(name), Probe::new(done));
    }

    fn lock_test(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, lock: LockSpec, done: Done<LockStatus>) {
        Vfs::lock_test(self, cx, ino, fh, lock, Probe::new(done));
    }

    fn lock_acquire(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        sleep: bool,
        done: Done<()>,
    ) {
        Vfs::lock_acquire(self, cx, ino, fh, lock, sleep, Probe::new(done));
    }

    fn lock_release(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        range: LockRange,
        done: Done<()>,
    ) {
        Vfs::lock_release(self, cx, ino, fh, owner, range, Probe::new(done));
    }

    fn sync_view(&self, cx: &OpCtx<'_>, done: Done<()>) {
        Vfs::sync_view(self, cx, Probe::new(done));
    }
}
