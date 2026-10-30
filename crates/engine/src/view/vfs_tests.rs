//! In-process `Vfs` calls against a real [`View`] (plan 31 C4a), each op
//! driven through the trait with a [`Blocking`] responder exactly as a
//! frontend drives it — no kernel, no FUSE. The seed of plan 31 C6's
//! conformance kit.

use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{
    Blocking, CollectDir, Fh, FnResponder, LockKind, LockOwner, LockRange, LockSpec, Name, OpCtx,
    OpKind, OpenOwner, Opened, RenameFlags, SetAttr, SetXattrFlags, TimeSet, Vfs, VfsResult,
    WriteData, XattrName, XattrNameBuf,
};

/// A caller driving one view through the trait.
struct Client {
    view: View,
    caller: Caller,
    _meta: Arc<Meta>,
    _dir: tempfile::TempDir,
}

fn client() -> Client {
    client_with(|_| {})
}

/// A client for a frontend that reports the kernel's `FORGET`s, as the
/// FUSE adapter's caps declare (`FrontendCaps::reports_forget`).
fn fuse_client() -> Client {
    client_with(|view| view.caps.reports_forget = true)
}

fn client_with(tune: impl FnOnce(&mut View)) -> Client {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut view, dir) = super::quota_tests::test_fs(meta.clone());
    tune(&mut view);
    Client {
        view,
        caller: Caller::new(1000, 1000, None),
        _meta: meta,
        _dir: dir,
    }
}

fn code<T: std::fmt::Debug>(r: VfsResult<T>) -> Code {
    r.expect_err("the op should have failed").code()
}

impl Client {
    fn cx(&self, kind: OpKind) -> OpCtx<'_> {
        OpCtx::new(kind, &self.caller)
    }

    fn lookup(&self, parent: Ino, name: &str) -> VfsResult<Entry> {
        Blocking::run(|r| {
            self.view
                .lookup(&self.cx(OpKind::Lookup), parent, Name::new(name), r)
        })
    }

    fn getattr(&self, ino: Ino) -> VfsResult<Attr> {
        Blocking::run(|r| self.view.getattr(&self.cx(OpKind::Getattr), ino, None, r))
    }

    fn create(&self, parent: Ino, name: &str) -> VfsResult<(Entry, Opened)> {
        Blocking::run(|r| {
            self.view.create(
                &self.cx(OpKind::Create),
                parent,
                Name::new(name),
                0o100644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
    }

    fn mkdir(&self, parent: Ino, name: &str) -> VfsResult<Entry> {
        Blocking::run(|r| {
            self.view
                .mkdir(&self.cx(OpKind::Mkdir), parent, Name::new(name), 0o755, r)
        })
    }

    fn write(&self, ino: Ino, fh: Fh, off: u64, data: &[u8]) -> VfsResult<u32> {
        Blocking::run(|r| {
            self.view.write(
                &self.cx(OpKind::Write),
                ino,
                fh,
                off,
                WriteData::Borrowed(data),
                OpenFlags::WRITE,
                r,
            )
        })
    }

    fn read(&self, ino: Ino, fh: Fh, off: u64, len: u32) -> VfsResult<Vec<u8>> {
        Blocking::run(|r| self.view.read(&self.cx(OpKind::Read), ino, fh, off, len, r))
            .map(|data| data.contiguous().unwrap().into_owned())
    }

    fn close(&self, ino: Ino, fh: Fh) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view
                .flush(&self.cx(OpKind::Flush), ino, fh, LockOwner(7), r)
        })?;
        Blocking::run(|r| {
            self.view.release(
                &self.cx(OpKind::Release),
                ino,
                fh,
                OpenFlags::WRITE,
                None,
                r,
            )
        })
    }

    fn readdir(&self, ino: Ino, cookie: u64, limit: usize) -> VfsResult<Vec<(String, u64)>> {
        let (sink, wait) = CollectDir::pair(limit);
        self.view
            .readdir(&self.cx(OpKind::Readdir), ino, Fh(0), cookie, false, sink);
        wait.wait().map(|entries| {
            entries
                .into_iter()
                .map(|e| (String::from_utf8(e.name.into_bytes()).unwrap(), e.next))
                .collect()
        })
    }

    /// One plus page of at most `limit` entries from `cookie`.
    fn readdir_plus(
        &self,
        ino: Ino,
        cookie: u64,
        limit: usize,
    ) -> VfsResult<Vec<constellation_vfs::DirEntry>> {
        let (sink, wait) = CollectDir::pair(limit);
        self.view.readdir(
            &self.cx(OpKind::Readdirplus),
            ino,
            Fh(0),
            cookie,
            true,
            sink,
        );
        wait.wait()
    }

    /// Every entry of a plus listing, in pages of `PAGE` entries, each
    /// resumed from the last cookie of the one before.
    fn readdir_plus_all(&self, ino: Ino) -> Vec<constellation_vfs::DirEntry> {
        const PAGE: usize = 7;
        let (mut all, mut cookie) = (Vec::new(), 0);
        loop {
            let page = self.readdir_plus(ino, cookie, PAGE).unwrap();
            let Some(last) = page.last() else {
                return all;
            };
            cookie = last.next;
            all.extend(page);
        }
    }

    fn rename(&self, parent: Ino, name: &str, new_parent: Ino, new_name: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view.rename(
                &self.cx(OpKind::Rename),
                parent,
                Name::new(name),
                new_parent,
                Name::new(new_name),
                RenameFlags::empty(),
                constellation_vfs::Unit(r),
            )
        })
    }

    fn open(&self, ino: Ino, flags: OpenFlags) -> VfsResult<Opened> {
        Blocking::run(|r| {
            self.view
                .open(&self.cx(OpKind::Open), ino, flags, OpenOwner::NONE, r)
        })
    }

    fn setattr(&self, ino: Ino, set: &SetAttr) -> VfsResult<Attr> {
        Blocking::run(|r| {
            self.view
                .setattr(&self.cx(OpKind::Setattr), ino, None, set, r)
        })
    }

    fn unlink(&self, parent: Ino, name: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view
                .unlink(&self.cx(OpKind::Unlink), parent, Name::new(name), r)
        })
    }

    fn rmdir(&self, parent: Ino, name: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view
                .rmdir(&self.cx(OpKind::Rmdir), parent, Name::new(name), r)
        })
    }

    fn setxattr(&self, ino: Ino, name: &str, value: &[u8], flags: SetXattrFlags) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view.setxattr(
                &self.cx(OpKind::Setxattr),
                ino,
                XattrName::new(name),
                value,
                flags,
                r,
            )
        })
    }

    fn getxattr(&self, ino: Ino, name: &str) -> VfsResult<Vec<u8>> {
        Blocking::run(|r| {
            self.view
                .getxattr(&self.cx(OpKind::Getxattr), ino, XattrName::new(name), r)
        })
    }

    fn listxattr(&self, ino: Ino) -> VfsResult<Vec<String>> {
        Blocking::run(|r| self.view.listxattr(&self.cx(OpKind::Listxattr), ino, r)).map(
            |names: Vec<XattrNameBuf>| {
                names
                    .into_iter()
                    .map(|n| String::from_utf8(n.into_bytes()).unwrap())
                    .collect()
            },
        )
    }

    fn removexattr(&self, ino: Ino, name: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view
                .removexattr(&self.cx(OpKind::Removexattr), ino, XattrName::new(name), r)
        })
    }
}

#[test]
fn create_write_read_close_and_the_size_is_visible_throughout() {
    let c = client();
    let (entry, opened) = c.create(ROOT_INO, "f").unwrap();
    assert_eq!(entry.attr.kind, FileKind::File);
    assert_eq!(entry.attr.ttl, TTL);
    let ino = entry.attr.ino;
    // One handle per open, addressing only its inode (plan 39 §3.7).
    assert_ne!(opened.fh, Fh(0));
    assert_eq!(c.write(ino, opened.fh, 0, b"hello").unwrap(), 5);
    assert_eq!(c.write(ino, opened.fh, 5, b" world").unwrap(), 6);
    // Unflushed: served from the write session, and the pending size
    // overlays the committed one in `getattr` and `lookup`.
    assert_eq!(c.read(ino, opened.fh, 0, 64).unwrap(), b"hello world");
    assert_eq!(c.read(ino, opened.fh, 6, 3).unwrap(), b"wor");
    assert_eq!(c.getattr(ino).unwrap().size, 11);
    assert_eq!(c.lookup(ROOT_INO, "f").unwrap().attr.size, 11);
    c.close(ino, opened.fh).unwrap();
    let attr = c.getattr(ino).unwrap();
    assert_eq!((attr.size, attr.blocks, attr.blksize), (11, 1, BLOCK_SIZE));
    assert_eq!(
        code(c.read(ino, opened.fh, 0, 64)),
        Code::BadFd,
        "a released handle is gone"
    );
    let reopened = Blocking::run(|r| {
        c.view.open(
            &c.cx(OpKind::Open),
            ino,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    assert_ne!(reopened.fh, opened.fh);
    assert_eq!(c.read(ino, reopened.fh, 0, 64).unwrap(), b"hello world");
    assert_eq!(
        c.read(ino, reopened.fh, 64, 8).unwrap(),
        b"",
        "past the end"
    );
    // The root answers as the root.
    assert_eq!(c.getattr(ROOT_INO).unwrap().kind, FileKind::Dir);
}

#[test]
fn mkdir_and_readdir_resume_from_a_cookie() {
    let c = client();
    let dir = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    for name in ["a", "b", "c"] {
        let (e, o) = c.create(dir, name).unwrap();
        c.close(e.attr.ino, o.fh).unwrap();
    }
    let all = c.readdir(dir, 0, 100).unwrap();
    let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names[..2], [".", ".."]);
    let mut children = names[2..].to_vec();
    children.sort_unstable();
    assert_eq!(children, ["a", "b", "c"]);
    // A full buffer stops the listing; the last cookie resumes it.
    let first = c.readdir(dir, 0, 3).unwrap();
    assert_eq!(first.len(), 3);
    let rest = c.readdir(dir, first.last().unwrap().1, 100).unwrap();
    assert_eq!(first.len() + rest.len(), all.len());
    assert_eq!(rest.last(), all.last());
    assert_eq!(code(c.mkdir(ROOT_INO, "d")), Code::Exists);
}

/// A removed inode the kernel still references without a handle (an
/// `O_PATH` descriptor, a working directory) stats as `nlink == 0` until
/// the kernel's `FORGET`.
#[test]
fn a_removed_inode_stats_with_no_links_until_it_is_forgotten() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    c.write(e.attr.ino, o.fh, 0, b"abc").unwrap();
    c.close(e.attr.ino, o.fh).unwrap();
    let f = e.attr.ino;
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let (v, o) = c.create(ROOT_INO, "victim").unwrap();
    c.close(v.attr.ino, o.fh).unwrap();
    let (src, o) = c.create(ROOT_INO, "src").unwrap();
    c.close(src.attr.ino, o.fh).unwrap();

    c.unlink(ROOT_INO, "f").unwrap();
    c.rmdir(ROOT_INO, "d").unwrap();
    c.rename(ROOT_INO, "src", ROOT_INO, "victim").unwrap();
    let gone = c.getattr(f).unwrap();
    assert_eq!((gone.nlink, gone.size), (0, 3));
    assert_eq!(c.getattr(d).unwrap().nlink, 0);
    assert_eq!(c.getattr(v.attr.ino).unwrap().nlink, 0);
    assert_eq!(
        c.getattr(src.attr.ino).unwrap().nlink,
        1,
        "the renamed file lives"
    );

    for ino in [f, d, v.attr.ino] {
        c.view.forget(ino);
        // Nothing answers for it any more: a stale reference.
        assert_eq!(code(c.getattr(ino)), Code::Stale);
    }
}

/// An unlinked file the kernel still references without a handle (an
/// `O_PATH` descriptor) reopens through it (`open("/proc/self/fd/N")`)
/// and reads, writes, truncates and chmods like any file — before and
/// after its handles close — until the kernel's `FORGET`, which reaps it.
#[test]
fn an_unlinked_file_reopens_until_it_is_forgotten() {
    let c = fuse_client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let f = e.attr.ino;
    c.write(f, o.fh, 0, &b"hello".repeat(100)).unwrap();
    c.close(f, o.fh).unwrap();
    // Unlinked while open, and then closed.
    let (e, o) = c.create(ROOT_INO, "g").unwrap();
    let g = e.attr.ino;
    c.write(g, o.fh, 0, b"open at unlink").unwrap();

    c.unlink(ROOT_INO, "f").unwrap();
    c.unlink(ROOT_INO, "g").unwrap();
    c.close(g, o.fh).unwrap();

    let ro = c.open(f, OpenFlags::READ).unwrap();
    assert_eq!(c.read(f, ro.fh, 0, 10_000).unwrap(), b"hello".repeat(100));
    let rw = c.open(f, OpenFlags::READ | OpenFlags::WRITE).unwrap();
    c.write(f, rw.fh, 0, b"HELLO").unwrap();
    assert_eq!(c.read(f, ro.fh, 0, 5).unwrap(), b"HELLO");
    let attr = c
        .setattr(
            f,
            &SetAttr {
                size: Some(7),
                mode: Some(0o600),
                ..SetAttr::default()
            },
        )
        .unwrap();
    assert_eq!((attr.size, attr.mode & 0o7777, attr.nlink), (7, 0o600, 0));
    c.close(f, rw.fh).unwrap();
    c.close(f, ro.fh).unwrap();
    // No handle left: the kernel's reference alone keeps it, writes
    // included.
    let again = c.open(f, OpenFlags::READ).unwrap();
    assert_eq!(c.read(f, again.fh, 0, 64).unwrap(), b"HELLOhe");
    c.close(f, again.fh).unwrap();
    let attr = c.getattr(f).unwrap();
    assert_eq!((attr.nlink, attr.size), (0, 7));
    let reopened = c.open(g, OpenFlags::READ).unwrap();
    assert_eq!(c.read(g, reopened.fh, 0, 64).unwrap(), b"open at unlink");
    c.close(g, reopened.fh).unwrap();

    for ino in [f, g] {
        c.view.forget(ino);
        assert_eq!(code(c.getattr(ino)), Code::Stale);
        assert_eq!(code(c.open(ino, OpenFlags::READ)), Code::Stale);
    }
    assert!(
        c._meta.orphans().unwrap().is_empty(),
        "reaped at the forget"
    );
    assert!(c.view.export_handles().unlinked_refs.is_empty());
}

/// An unlinked file the kernel still references keeps its xattrs, and
/// `setxattr`/`removexattr` through the reference (`/proc/self/fd/N`)
/// change them, as on a local filesystem (both answered `ENOENT`, and the
/// xattrs read back empty, before).
#[test]
fn an_unlinked_file_keeps_and_changes_its_xattrs() {
    let c = fuse_client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let f = e.attr.ino;
    c.close(f, o.fh).unwrap();
    let big = vec![b'x'; 2000];
    c.setxattr(f, "user.small", b"1", SetXattrFlags::empty())
        .unwrap();
    c.setxattr(f, "user.big", &big, SetXattrFlags::empty())
        .unwrap();
    c.unlink(ROOT_INO, "f").unwrap();

    let mut names = c.listxattr(f).unwrap();
    names.sort();
    assert_eq!(names, ["user.big", "user.small"]);
    assert_eq!(c.getxattr(f, "user.big").unwrap(), big);
    c.setxattr(f, "user.new", b"n", SetXattrFlags::empty())
        .unwrap();
    assert_eq!(
        code(c.setxattr(f, "user.new", b"n", SetXattrFlags::CREATE)),
        Code::Exists
    );
    c.removexattr(f, "user.small").unwrap();
    assert_eq!(code(c.removexattr(f, "user.small")), Code::NoData);
    assert_eq!(code(c.getxattr(f, "user.small")), Code::NoData);
    let mut names = c.listxattr(f).unwrap();
    names.sort();
    assert_eq!(names, ["user.big", "user.new"]);
    assert_eq!(c.getxattr(f, "user.new").unwrap(), b"n");

    c.view.forget(f);
    assert_eq!(code(c.listxattr(f)), Code::Stale);
}

/// A file unlinked elsewhere (another node's unlink, applied here) that
/// this view still had open: its reference answers `ESTALE` once the
/// last handle's close reaped it — not `nlink` 0 to `fstat` and `ENOENT`
/// to a reopen, as when the close took it for this view's own removal.
#[test]
fn a_file_unlinked_elsewhere_is_stale_after_its_last_close_here() {
    let c = fuse_client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let f = e.attr.ino;
    c.close(f, o.fh).unwrap();
    let held = c.open(f, OpenFlags::READ).unwrap();
    c._meta
        .apply_records(&[constellation_meta::LogRecord::Unlink {
            parent: ROOT_INO,
            name: "f".into(),
            time_ns: 9,
        }])
        .unwrap();
    // Still open here: still the file (nlink 0).
    assert_eq!(c.getattr(f).unwrap().nlink, 0);
    c.close(f, held.fh).unwrap();
    assert!(c._meta.orphans().unwrap().is_empty(), "reaped at the close");
    assert_eq!(code(c.getattr(f)), Code::Stale);
    assert_eq!(code(c.open(f, OpenFlags::READ)), Code::Stale);
}

/// A removed directory the kernel still references (a process's working
/// directory, an `O_PATH` descriptor) keeps its attributes and xattrs,
/// and `chmod`, `utimes` and the xattr ops through the reference change
/// them, as on a local filesystem (all answered `ENOENT` before), until
/// the kernel's `FORGET` reaps it. Listing it answers `ENOENT`.
#[test]
fn a_removed_directory_is_kept_until_it_is_forgotten() {
    let c = fuse_client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    c.setxattr(d, "user.a", b"1", SetXattrFlags::empty())
        .unwrap();
    c.rmdir(ROOT_INO, "d").unwrap();
    assert_eq!(code(c.lookup(ROOT_INO, "d")), Code::NotFound);

    let attr = c.getattr(d).unwrap();
    assert_eq!((attr.kind, attr.nlink), (FileKind::Dir, 0));
    assert_eq!(c.listxattr(d).unwrap(), ["user.a"]);
    c.setxattr(d, "user.b", b"2", SetXattrFlags::empty())
        .unwrap();
    c.removexattr(d, "user.a").unwrap();
    assert_eq!(c.listxattr(d).unwrap(), ["user.b"]);
    let attr = c
        .setattr(
            d,
            &SetAttr {
                mode: Some(0o700),
                mtime: Some(TimeSet::At(2_000_000_000)),
                ..SetAttr::default()
            },
        )
        .unwrap();
    assert_eq!((attr.mode & 0o7777, attr.mtime_ns), (0o700, 2_000_000_000));
    assert_eq!(code(c.readdir(d, 0, 10)), Code::NotFound);
    assert_eq!(code(c.mkdir(d, "inside")), Code::NotFound);

    c.view.forget(d);
    assert_eq!(code(c.getattr(d)), Code::Stale);
    assert!(c._meta.orphans().unwrap().is_empty());
}

/// The kernel may send an unlinked file's `FORGET` before the `RELEASE`
/// of its last handle (the two travel apart): the forget leaves the
/// orphan to the handle, whose close then reaps it.
#[test]
fn a_forget_before_the_last_close_leaves_the_reap_to_the_close() {
    let c = fuse_client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let f = e.attr.ino;
    c.write(f, o.fh, 0, b"still open").unwrap();
    c.unlink(ROOT_INO, "f").unwrap();
    c.view.forget(f);
    assert_eq!(c.read(f, o.fh, 0, 64).unwrap(), b"still open");
    assert_eq!(c._meta.orphans().unwrap(), vec![f]);
    c.close(f, o.fh).unwrap();
    assert!(c._meta.orphans().unwrap().is_empty(), "reaped at the close");
    assert!(c.view.writes.pending_inos().is_empty());
    assert!(c.view.export_handles().unlinked_refs.is_empty());
}

/// A frontend that does not report `forget`s (the control service's
/// browse view, a test or bench host) keeps nothing for references it
/// cannot have: an unlink or a rename over a file reaps it at once, as
/// before, and leaves no orphan record or reference behind.
#[test]
fn a_view_without_forgets_keeps_no_unlinked_file() {
    let c = client();
    assert!(!c.view.caps.reports_forget);
    for name in ["a", "b", "c"] {
        let (e, o) = c.create(ROOT_INO, name).unwrap();
        c.write(e.attr.ino, o.fh, 0, name.as_bytes()).unwrap();
        c.close(e.attr.ino, o.fh).unwrap();
    }
    c.unlink(ROOT_INO, "a").unwrap();
    c.rename(ROOT_INO, "b", ROOT_INO, "c").unwrap();
    assert!(c.view.export_handles().unlinked_refs.is_empty());
    assert!(c._meta.orphans().unwrap().is_empty());
}

/// An inode this view handed out that was removed elsewhere — another
/// node's unlink its reaper applied, or another view's — answers
/// `ESTALE` to every op on the inode itself (a stale handle, as NFS
/// answers), where a lookup of its old name answers `ENOENT`.
#[test]
fn an_inode_removed_elsewhere_is_a_stale_handle() {
    let c = client();
    let mut inos = Vec::new();
    for name in ["f", "g"] {
        let (e, o) = c.create(ROOT_INO, name).unwrap();
        c.close(e.attr.ino, o.fh).unwrap();
        inos.push(e.attr.ino);
    }
    // Another node's unlink, then this node's reaper.
    c._meta.unlink(ROOT_INO, "f").unwrap();
    c._meta.reap_orphan(inos[0]).unwrap();
    // Another view of this node.
    let (other, _dir) = super::quota_tests::test_fs(c._meta.clone());
    let cx = OpCtx::new(OpKind::Unlink, &c.caller);
    Blocking::run(|r| other.unlink(&cx, ROOT_INO, Name::new("g"), r)).unwrap();

    for (ino, name) in inos.into_iter().zip(["f", "g"]) {
        assert_eq!(code(c.lookup(ROOT_INO, name)), Code::NotFound);
        assert_eq!(code(c.getattr(ino)), Code::Stale);
        assert_eq!(code(c.open(ino, OpenFlags::READ)), Code::Stale);
        let chmod = SetAttr {
            mode: Some(0o600),
            ..SetAttr::default()
        };
        assert_eq!(code(c.setattr(ino, &chmod)), Code::Stale);
        let truncate = SetAttr {
            size: Some(0),
            ..SetAttr::default()
        };
        assert_eq!(code(c.setattr(ino, &truncate)), Code::Stale);
        assert_eq!(code(c.getxattr(ino, "user.a")), Code::Stale);
        assert_eq!(code(c.listxattr(ino)), Code::Stale);
        assert_eq!(
            code(c.setxattr(ino, "user.a", b"1", SetXattrFlags::empty())),
            Code::Stale
        );
        assert_eq!(code(c.removexattr(ino, "user.a")), Code::Stale);
        let link = Blocking::run(|r| {
            c.view
                .link(&c.cx(OpKind::Link), ino, ROOT_INO, Name::new("again"), r)
        });
        assert_eq!(code(link), Code::Stale);
    }

    // A directory and a symlink removed by another node.
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let l = Blocking::run(|r| {
        c.view
            .symlink(&c.cx(OpKind::Symlink), ROOT_INO, Name::new("l"), b"t", r)
    })
    .unwrap()
    .attr
    .ino;
    c._meta.rmdir(ROOT_INO, "d").unwrap();
    c._meta.unlink(ROOT_INO, "l").unwrap();
    for ino in [d, l] {
        c._meta.reap_orphan(ino).unwrap();
    }
    assert_eq!(code(c.readdir(d, 0, 10)), Code::Stale);
    assert_eq!(code(c.getattr(d)), Code::Stale);
    assert_eq!(
        code(Blocking::run(|r| c.view.readlink(
            &c.cx(OpKind::Readlink),
            l,
            r
        ))),
        Code::Stale
    );
    // One this view removed itself is not stale: it answers as on a
    // local filesystem until the kernel's `FORGET`.
    let mine = c.mkdir(ROOT_INO, "mine").unwrap().attr.ino;
    c.rmdir(ROOT_INO, "mine").unwrap();
    assert_eq!(c.getattr(mine).unwrap().nlink, 0);
    assert_eq!(code(c.readdir(mine, 0, 10)), Code::NotFound);
    // Nor is a synthetic inode, which the replica never has.
    let synthetic = SYNTHETIC_INO_BIT + 5;
    assert!(View::is_synthetic(synthetic));
    assert_eq!(c.view.gone(synthetic, Code::NotFound), Code::NotFound);
}

/// Plan 40 D11: `unlinked_refs_len` counts the orphans the view keeps
/// for its platform's references: an unlink of a file moves it up, the
/// platform's `forget` back down, and a view whose platform reports no
/// `forget` keeps none.
#[test]
fn unlinked_refs_len_counts_the_orphans_kept_for_the_kernel() {
    let c = fuse_client();
    assert_eq!(c.view.unlinked_refs_len(), 0);
    let mut inos = Vec::new();
    for name in ["a", "b"] {
        let (e, o) = c.create(ROOT_INO, name).unwrap();
        c.close(e.attr.ino, o.fh).unwrap();
        inos.push(e.attr.ino);
    }
    assert_eq!(c.view.unlinked_refs_len(), 0, "still linked");
    c.unlink(ROOT_INO, "a").unwrap();
    assert_eq!(c.view.unlinked_refs_len(), 1);
    c.unlink(ROOT_INO, "b").unwrap();
    assert_eq!(c.view.unlinked_refs_len(), 2);
    c.view.forget(inos[0]);
    assert_eq!(c.view.unlinked_refs_len(), 1);
    c.view.forget(inos[1]);
    assert_eq!(c.view.unlinked_refs_len(), 0);

    // No `forget` is ever reported: nothing is kept, so nothing is counted.
    let plain = client_with(|_| {});
    let (e, o) = plain.create(ROOT_INO, "f").unwrap();
    plain.close(e.attr.ino, o.fh).unwrap();
    plain.unlink(ROOT_INO, "f").unwrap();
    assert_eq!(plain.view.unlinked_refs_len(), 0);
}

/// The kernel's reference to an unlinked file crosses a handover: the
/// next process's view keeps it openable until the `FORGET` it receives.
#[test]
fn an_unlinked_file_the_kernel_references_crosses_a_handover() {
    let c = fuse_client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let f = e.attr.ino;
    c.close(f, o.fh).unwrap();
    c.unlink(ROOT_INO, "f").unwrap();
    let snapshot = c.view.export_handles();
    assert_eq!(snapshot.unlinked_refs, vec![f]);

    let (mut next, _dir) = super::quota_tests::test_fs(c._meta.clone());
    next.caps.reports_forget = true;
    next.import_handles(&snapshot);
    let cx = OpCtx::new(OpKind::Open, &c.caller);
    let opened = Blocking::run(|r| next.open(&cx, f, OpenFlags::READ, OpenOwner::NONE, r)).unwrap();
    let cx = OpCtx::new(OpKind::Release, &c.caller);
    Blocking::run(|r| next.release(&cx, f, opened.fh, OpenFlags::READ, None, r)).unwrap();
    assert_eq!(c._meta.orphans().unwrap(), vec![f], "still referenced");
    next.forget(f);
    assert!(c._meta.orphans().unwrap().is_empty());
}

#[test]
fn rename_unlink_and_rmdir() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "old").unwrap();
    c.write(e.attr.ino, o.fh, 0, b"x").unwrap();
    c.close(e.attr.ino, o.fh).unwrap();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    c.rename(ROOT_INO, "old", d, "new").unwrap();
    assert_eq!(code(c.lookup(ROOT_INO, "old")), Code::NotFound);
    let moved = c.lookup(d, "new").unwrap();
    assert_eq!(moved.attr.ino, e.attr.ino);
    assert_eq!(moved.attr.size, 1);
    assert_eq!(code(c.rmdir(ROOT_INO, "d")), Code::NotEmpty);
    c.unlink(d, "new").unwrap();
    assert_eq!(code(c.lookup(d, "new")), Code::NotFound);
    assert_eq!(code(c.unlink(d, "new")), Code::NotFound);
    c.rmdir(ROOT_INO, "d").unwrap();
    assert_eq!(code(c.lookup(ROOT_INO, "d")), Code::NotFound);
}

#[test]
fn xattrs_round_trip_and_the_virtual_ones_are_read_only_and_unlisted() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let (e, o) = c.create(d, "f").unwrap();
    c.write(e.attr.ino, o.fh, 0, b"12345").unwrap();
    c.close(e.attr.ino, o.fh).unwrap();
    let f = e.attr.ino;
    c.setxattr(f, "user.color", b"blue", SetXattrFlags::empty())
        .unwrap();
    assert_eq!(c.getxattr(f, "user.color").unwrap(), b"blue");
    assert_eq!(c.listxattr(f).unwrap(), ["user.color"]);
    // The virtual ones: computed, never stored or settable.
    assert_eq!(c.getxattr(d, "user.constellation.rsize").unwrap(), b"5");
    assert_eq!(c.getxattr(d, "user.constellation.rcount").unwrap(), b"1");
    assert_eq!(
        code(c.setxattr(d, "user.constellation.rsize", b"1", SetXattrFlags::empty())),
        Code::Perm
    );
    assert_eq!(
        code(c.removexattr(d, "user.constellation.rcount")),
        Code::Perm
    );
    // Namespaces and flags.
    assert_eq!(code(c.getxattr(f, "security.capability")), Code::NoData);
    c.setxattr(f, "security.x", b"1", SetXattrFlags::empty())
        .unwrap();
    assert_eq!(c.getxattr(f, "security.x").unwrap(), b"1");
    c.removexattr(f, "security.x").unwrap();
    assert_eq!(
        code(c.setxattr(f, "system.x", b"1", SetXattrFlags::empty())),
        Code::NotSupported
    );
    assert_eq!(
        code(c.setxattr(f, "trusted.x", b"1", SetXattrFlags::empty())),
        Code::Perm
    );
    assert_eq!(
        code(c.setxattr(
            f,
            "user.color",
            b"red",
            SetXattrFlags::CREATE | SetXattrFlags::REPLACE
        )),
        Code::Invalid
    );
    assert_eq!(
        code(c.setxattr(f, "user.color", &[0; 64 * 1024 + 1], SetXattrFlags::empty())),
        Code::TooBig
    );
    c.removexattr(f, "user.color").unwrap();
    assert_eq!(code(c.getxattr(f, "user.color")), Code::NoData);
    assert!(c.listxattr(f).unwrap().is_empty());
}

#[test]
fn a_frontend_that_does_not_list_virtual_xattrs_still_reads_them() {
    let c = client_with(|view| {
        view.caps.virtual_xattrs_listed = false;
        view.policies = PolicyStack::for_caps(&view.caps);
    });
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    c.setxattr(d, "user.a", b"1", SetXattrFlags::empty())
        .unwrap();
    assert_eq!(c.listxattr(d).unwrap(), ["user.a"]);
    assert_eq!(c.getxattr(d, "user.constellation.rcount").unwrap(), b"0");
}

#[test]
fn names_past_name_max_are_refused_before_anything_else() {
    let c = client();
    let long = "x".repeat(256);
    assert_eq!(code(c.lookup(ROOT_INO, &long)), Code::NameTooLong);
    assert_eq!(code(c.mkdir(ROOT_INO, &long)), Code::NameTooLong);
    assert_eq!(code(c.create(ROOT_INO, &long)), Code::NameTooLong);
    assert_eq!(
        code(c.rename(ROOT_INO, "a", ROOT_INO, &long)),
        Code::NameTooLong
    );
    c.mkdir(ROOT_INO, &"y".repeat(255)).unwrap();
}

#[test]
fn symlink_hard_link_and_special_files() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    c.close(e.attr.ino, o.fh).unwrap();
    let sym = Blocking::run(|r| {
        c.view
            .symlink(&c.cx(OpKind::Symlink), ROOT_INO, Name::new("s"), b"f", r)
    })
    .unwrap();
    assert_eq!(sym.attr.kind, FileKind::Symlink);
    let target =
        Blocking::run(|r| c.view.readlink(&c.cx(OpKind::Readlink), sym.attr.ino, r)).unwrap();
    assert_eq!(target, b"f");
    let linked = Blocking::run(|r| {
        c.view.link(
            &c.cx(OpKind::Link),
            e.attr.ino,
            ROOT_INO,
            Name::new("hard"),
            r,
        )
    })
    .unwrap();
    assert_eq!((linked.attr.ino, linked.attr.nlink), (e.attr.ino, 2));
    let mknod = |name: &str, mode: u32| {
        Blocking::run(|r| {
            c.view.mknod(
                &c.cx(OpKind::Mknod),
                ROOT_INO,
                Name::new(name),
                mode,
                constellation_types::Rdev::default(),
                r,
            )
        })
    };
    let fifo = mknod("p", constellation_vfs::types::mode::S_IFIFO | 0o644).unwrap();
    assert_eq!(fifo.attr.kind, FileKind::Fifo);
    assert_eq!(fifo.attr.mode & 0o7777, 0o644);
    assert_eq!(
        code(mknod(
            "dir",
            constellation_vfs::types::mode::S_IFDIR | 0o755
        )),
        Code::Invalid
    );
}

#[test]
fn setattr_truncates_and_sets_times() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"abcdefgh").unwrap();
    let attr = Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            Some(o.fh),
            &SetAttr {
                size: Some(3),
                mtime: Some(TimeSet::At(7_000_000_000)),
                mode: Some(0o600),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    assert_eq!(attr.size, 3);
    assert_eq!(attr.mtime_ns, 7_000_000_000);
    assert_eq!(attr.mode & 0o7777, 0o600);
    assert_eq!(c.read(ino, o.fh, 0, 64).unwrap(), b"abc");
    c.close(ino, o.fh).unwrap();
}

/// Only a writer's close publishes (close-to-open): a read-only
/// description's close leaves another descriptor's pending writes alone
/// (it used to publish them, a chunk upload and commit per reader close).
#[test]
fn a_read_only_close_does_not_publish_anothers_writes() {
    let c = client();
    let (e, w) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, w.fh, 0, b"12345").unwrap();
    let r = Blocking::run(|r| {
        c.view.open(
            &c.cx(OpKind::Open),
            ino,
            OpenFlags::empty(),
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    assert_eq!(
        c.read(ino, r.fh, 0, 64).unwrap(),
        b"12345",
        "the reader sees the pending writes"
    );
    c.close(ino, r.fh).unwrap();
    assert_eq!(
        c._meta.getattr(ino).unwrap().unwrap().size,
        0,
        "not published by the reader"
    );
    assert_eq!(c.getattr(ino).unwrap().size, 5);
    c.close(ino, w.fh).unwrap();
    assert_eq!(
        c._meta.getattr(ino).unwrap().unwrap().size,
        5,
        "published by the writer"
    );
}

/// mtime follows the writes of an open file, not its flush: each write
/// moves it (visible before the close), the close keeps the last write's
/// time rather than stamping its own, and a time set through the open
/// descriptor before the close (`cp -p`) is the one the file keeps.
#[test]
fn mtime_is_the_last_writes_or_the_one_set_not_the_flushs() {
    let c = client();
    let pause = || std::thread::sleep(std::time::Duration::from_millis(20));
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"one").unwrap();
    let first = c.getattr(ino).unwrap().mtime_ns;
    pause();
    c.write(ino, o.fh, 3, b"two").unwrap();
    let second = c.getattr(ino).unwrap().mtime_ns;
    assert!(second > first, "a write moves mtime before the close");
    pause();
    c.close(ino, o.fh).unwrap();
    let closed = c.getattr(ino).unwrap();
    assert_eq!(
        closed.mtime_ns, second,
        "the close keeps the last write's time"
    );
    assert!(closed.ctime_ns >= second);

    let (e, o) = c.create(ROOT_INO, "p").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"copied").unwrap();
    Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            Some(o.fh),
            &SetAttr {
                mtime: Some(TimeSet::At(-1_000_000_000)),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    pause();
    c.close(ino, o.fh).unwrap();
    assert_eq!(
        c.getattr(ino).unwrap().mtime_ns,
        -1_000_000_000,
        "the time set survives the close"
    );
}

/// The reverse order: a write that reaches the file after a time set is
/// a data change after it, and moves mtime past it — also within one
/// session, and also when the set came by path (no handle).
///
/// xcheckfs finding 11 is this case seen from user space: a store into a
/// shared writable `mmap`; `utimensat(OLD)`; `munmap` left the file at the
/// `munmap`'s time, where ext4 keeps `OLD`. Without the writeback cache
/// the kernel neither stamps the store at its page fault (`S_NOCMTIME`)
/// nor writes the page back before a time-setting `setattr`, so the
/// store's bytes arrive as this second write, at the `munmap` (or an
/// `msync`, `fsync`, the close, the kernel's flusher). A store made after
/// the set arrives exactly the same way; leaving the set's time for it
/// would leave a changed file with a time from before the change
/// (`make`, rsync's size-and-mtime check). Every FUSE file system that
/// stamps its writes does the same (DESIGN §3 "File times").
#[test]
fn a_write_after_a_time_set_moves_mtime_past_it_in_the_same_session() {
    const OLD: i64 = 2_000_000 * 1_000_000_000;
    let c = client();
    let (e, o) = c.create(ROOT_INO, "mapped").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, &[0u8; 4096]).unwrap();
    let set = Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            None,
            &SetAttr {
                mtime: Some(TimeSet::At(OLD)),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    assert_eq!(set.mtime_ns, OLD);
    assert_eq!(c.getattr(ino).unwrap().mtime_ns, OLD, "the set is live");
    // The page's writeback.
    c.write(ino, o.fh, 0, b"abcd").unwrap();
    let written = c.getattr(ino).unwrap().mtime_ns;
    assert!(written > OLD, "the write left mtime at the set: {written}");
    c.close(ino, o.fh).unwrap();
    assert_eq!(
        c.getattr(ino).unwrap().mtime_ns,
        written,
        "the close publishes the write's time, not the set's"
    );
}

/// `setattr` with an mtime, on another thread, while `ino`'s inode
/// operation lock is held by this one: returns once the setattr has
/// answered, or waits for that lock.
fn set_mtime_meanwhile<'s>(
    s: &'s std::thread::Scope<'s, '_>,
    c: &'s Client,
    ino: Ino,
    mtime: i64,
) -> std::thread::ScopedJoinHandle<'s, VfsResult<Attr>> {
    let setattr = s.spawn(move || {
        Blocking::run(|r| {
            c.view.setattr(
                &c.cx(OpKind::Setattr),
                ino,
                None,
                &SetAttr {
                    mtime: Some(TimeSet::At(mtime)),
                    ..SetAttr::default()
                },
                r,
            )
        })
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !setattr.is_finished()
        && !c
            .view
            .watch
            .ops()
            .iter()
            .any(|op| op.op == "setattr" && op.stage == "inode operation lock")
    {
        assert!(std::time::Instant::now() < deadline, "setattr stuck");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    setattr
}

/// xcheckfs: a `utime` racing the close of a write that had returned
/// before it was lost (293 of 300 rounds on a mount; never on ext4). The
/// close's flush takes the session out of its shard under the inode's
/// operation lock, and commits the session's mtime — the write's. A
/// `setattr` in that window found no session to fold its time into, set
/// the row, and the commit then put the write's time back. It is ordered
/// against the flush now: folded before it, or set after its commit.
#[test]
fn a_time_set_while_a_flush_holds_the_session_is_the_one_the_file_keeps() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "raced").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"written").unwrap();
    let written = c.getattr(ino).unwrap().mtime_ns;
    let set = -1_000_000_000;
    std::thread::scope(|s| {
        // A flush between its detach and its commit (a read holds the
        // same while it reads).
        let op = c.view.inode_ops.lock(ino);
        let ws = c.view.writes.detach(ino).expect("the write's session");
        let setattr = set_mtime_meanwhile(s, &c, ino, set);
        c.view.writes.reattach(ino, ws);
        // The commit (re-entering the lock this thread holds).
        c.view.flush_inode(ino, false).unwrap();
        drop(op);
        setattr.join().unwrap().unwrap();
    });
    assert_ne!(written, set);
    assert_eq!(
        c.getattr(ino).unwrap().mtime_ns,
        set,
        "the time set after the write returned survives its commit"
    );
    c.close(ino, o.fh).unwrap();
    assert_eq!(c.getattr(ino).unwrap().mtime_ns, set);
}

/// The lock above is taken only for a file with a write session on this
/// node: a `touch` of a file that is only being read never waits for the
/// read (a chunk fetch, maybe from S3).
#[test]
fn a_time_set_of_a_file_without_a_session_does_not_wait_for_its_reads() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "read").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"written").unwrap();
    c.close(ino, o.fh).unwrap();
    let set = -2_000_000_000;
    std::thread::scope(|s| {
        // A read in flight.
        let op = c.view.inode_ops.lock(ino);
        let setattr = set_mtime_meanwhile(s, &c, ino, set);
        assert!(setattr.is_finished(), "the set waited for the read");
        drop(op);
        setattr.join().unwrap().unwrap();
    });
    assert_eq!(c.getattr(ino).unwrap().mtime_ns, set);
}

/// The kind lane's busy-writer loss: the kernel stores every attribute
/// reply's size in `i_size` and positions an `O_APPEND` write there. A
/// `chmod` (or a `link`) of a file whose write session holds more than
/// its committed row answered the committed size, so the appender's next
/// writes landed on bytes already acknowledged — every call, and the
/// close, succeeding. Each reply about the inode reports the pending
/// size; appending at it keeps every byte.
#[test]
fn setattr_and_link_report_the_pending_size_an_appender_continues_from() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "busy").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"aaaa").unwrap();
    // A second descriptor's `fsync` publishes: the committed row says 4.
    c.fsync(ino, o.fh).unwrap();
    assert_eq!(c._meta.getattr(ino).unwrap().unwrap().size, 4);
    // The long-lived descriptor appends on: a new session, pending 8.
    c.write(ino, o.fh, 4, b"bbbb").unwrap();
    assert_eq!(c._meta.getattr(ino).unwrap().unwrap().size, 4);
    let chmod = Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            None,
            &SetAttr {
                mode: Some(0o640),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    assert_eq!(chmod.mode & 0o7777, 0o640);
    assert_eq!(chmod.size, 8, "a chmod reports the session's size");
    let touch = Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            None,
            &SetAttr {
                mtime: Some(TimeSet::At(7_000_000_000)),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    assert_eq!(touch.size, 8, "a utimes reports the session's size");
    let linked = Blocking::run(|r| {
        c.view.link(
            &c.cx(OpKind::Link),
            ino,
            ROOT_INO,
            Name::new("busy.link"),
            r,
        )
    })
    .unwrap();
    assert_eq!(linked.attr.size, 8, "a link's entry reports it too");
    assert_eq!(c.lookup(ROOT_INO, "busy.link").unwrap().attr.size, 8);
    // The kernel appends at the size the last reply gave it.
    c.write(ino, o.fh, chmod.size, b"cccc").unwrap();
    c.close(ino, o.fh).unwrap();
    assert_eq!(c.getattr(ino).unwrap().size, 12);
    let reopened = Blocking::run(|r| {
        c.view.open(
            &c.cx(OpKind::Open),
            ino,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    assert_eq!(c.read(ino, reopened.fh, 0, 64).unwrap(), b"aaaabbbbcccc");
}

/// A scratch-directory file being appended to is looked up at its pending
/// size (the lookup overlay is the scratch branch's own), and so is an
/// `open(O_CREAT)` of a name that already exists (the create path's reply).
#[test]
fn lookup_and_create_of_an_existing_file_report_the_pending_size() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "busy").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"aaaa").unwrap();
    c.fsync(ino, o.fh).unwrap();
    c.write(ino, o.fh, 4, b"bbbb").unwrap();
    assert_eq!(c._meta.getattr(ino).unwrap().unwrap().size, 4);
    let (again, again_o) = c.create(ROOT_INO, "busy").unwrap();
    assert_eq!(again.attr.ino, ino);
    assert_eq!(again.attr.size, 8, "create of an existing name");
    c.close(ino, again_o.fh).unwrap();
    c.close(ino, o.fh).unwrap();

    let scratch = c.mkdir(ROOT_INO, "tmp").unwrap().attr.ino;
    c.setxattr(
        scratch,
        constellation_meta::SCRATCH_XATTR,
        b"1",
        SetXattrFlags::default(),
    )
    .unwrap();
    let (e, o) = c.create(scratch, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"abcdef").unwrap();
    assert_eq!(c.lookup(scratch, "f").unwrap().attr.size, 6);
    c.close(ino, o.fh).unwrap();
}

/// In a scratch directory `rmdir` removes an empty directory and `unlink`
/// a file, as anywhere else: the two were swapped (`rmdir` of a scratch
/// directory answered `EISDIR`, so `rm -r` of one failed on a mount, and
/// `unlink` removed an empty one).
#[test]
fn rmdir_and_unlink_in_a_scratch_directory() {
    let c = client();
    let scratch = c.mkdir(ROOT_INO, "tmp").unwrap().attr.ino;
    c.setxattr(
        scratch,
        constellation_meta::SCRATCH_XATTR,
        b"1",
        SetXattrFlags::default(),
    )
    .unwrap();
    let sub = c.mkdir(scratch, "sub").unwrap().attr.ino;
    let (e, o) = c.create(sub, "f").unwrap();
    c.close(e.attr.ino, o.fh).unwrap();
    assert_eq!(code(c.unlink(scratch, "sub")), Code::IsDir);
    assert_eq!(code(c.rmdir(sub, "f")), Code::NotDir);
    assert_eq!(code(c.rmdir(scratch, "sub")), Code::NotEmpty);
    c.unlink(sub, "f").unwrap();
    assert_eq!(code(c.lookup(sub, "f")), Code::NotFound);
    c.rmdir(scratch, "sub").unwrap();
    assert_eq!(code(c.lookup(scratch, "sub")), Code::NotFound);
    assert_eq!(code(c.rmdir(scratch, "sub")), Code::NotFound);
}

#[test]
fn seek_fallocate_statfs_and_fsync() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"data").unwrap();
    let seek = |off: u64, whence: SeekWhence| {
        Blocking::run(|r| c.view.seek(&c.cx(OpKind::Seek), ino, o.fh, off, whence, r))
    };
    assert_eq!(seek(0, SeekWhence::Data), Ok(0));
    assert_eq!(seek(0, SeekWhence::Hole), Ok(4));
    assert_eq!(code(seek(9, SeekWhence::Data)), Code::NoDeviceOrAddress);
    assert_eq!(code(seek(0, SeekWhence::Set)), Code::Invalid);
    let fallocate = |len: u64, mode: FallocateMode| {
        Blocking::run(|r| {
            c.view
                .fallocate(&c.cx(OpKind::Fallocate), ino, o.fh, 0, len, mode, r)
        })
    };
    assert_eq!(code(fallocate(0, FallocateMode::empty())), Code::Invalid);
    assert_eq!(
        code(fallocate(8, FallocateMode::PUNCH_HOLE)),
        Code::NotSupported,
        "a punch must keep the size"
    );
    assert_eq!(
        code(fallocate(8, FallocateMode::UNSUPPORTED)),
        Code::NotSupported
    );
    fallocate(64, FallocateMode::empty()).unwrap();
    assert_eq!(c.getattr(ino).unwrap().size, 64);
    Blocking::run(|r| {
        c.view
            .fsync(&c.cx(OpKind::Fsync), ino, o.fh, Durability::Configured, r)
    })
    .unwrap();
    c.close(ino, o.fh).unwrap();
    let st = Blocking::run(|r| c.view.statfs(&c.cx(OpKind::Statfs), ROOT_INO, r)).unwrap();
    assert_eq!((st.files, st.bsize, st.namelen), (1, BLOCK_SIZE, 255));
    assert!(st.blocks >= st.bfree);
}

/// xcheckfs: an allocation that changes neither a byte nor the size (one
/// inside the file, a `KEEP_SIZE` one) left mtime where `utimes` had put
/// it. Like every other `fallocate` it stamps the write session, and the
/// close commits that mtime into the inode's row — what replicates.
#[test]
fn an_allocation_that_changes_nothing_still_moves_mtime() {
    const OLD: i64 = 1_000_000_000_000_000;
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, &[1u8; 65536]).unwrap();
    c.close(ino, o.fh).unwrap();
    for (off, len, mode) in [
        (0, 4096, FallocateMode::empty()),
        (0, 4096, FallocateMode::KEEP_SIZE),
        (65536, 65536, FallocateMode::KEEP_SIZE),
    ] {
        Blocking::run(|r| {
            c.view.setattr(
                &c.cx(OpKind::Setattr),
                ino,
                None,
                &SetAttr {
                    mtime: Some(TimeSet::At(OLD)),
                    ..SetAttr::default()
                },
                r,
            )
        })
        .unwrap();
        let o = Blocking::run(|r| {
            c.view.open(
                &c.cx(OpKind::Open),
                ino,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        Blocking::run(|r| {
            c.view
                .fallocate(&c.cx(OpKind::Fallocate), ino, o.fh, off, len, mode, r)
        })
        .unwrap();
        let live = c.getattr(ino).unwrap();
        assert!(live.mtime_ns > OLD, "{mode:?} {off}+{len}: mtime not moved");
        c.close(ino, o.fh).unwrap();
        let row = c._meta.getattr(ino).unwrap().unwrap();
        assert_eq!(
            (row.mtime_ns, row.size),
            (live.mtime_ns, 65536),
            "{mode:?} {off}+{len}: the committed row"
        );
        assert!(row.ctime_ns >= row.mtime_ns);
    }
}

/// A write or truncate the quota refuses changes nothing, and neither
/// does the close after it: it used to leave a write session that had
/// changed nothing, whose close committed a base-identical manifest
/// stamped with the commit's time for both mtime and ctime.
#[test]
fn a_refused_write_or_truncate_leaves_the_times_to_the_close() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"data").unwrap();
    c.close(ino, o.fh).unwrap();
    c._meta.set_quota(Some(100)).unwrap();
    View::invalidate_quota_cache(&c.view.quota_cache);
    let before = c._meta.getattr(ino).unwrap().unwrap();
    let times = |a: &constellation_fs_core::FileAttr| (a.ctime_ns, a.mtime_ns, a.size);
    let manifests = || {
        c._meta
            .peek_journal_after(0)
            .unwrap()
            .into_iter()
            .filter(|(_, r)| matches!(r, constellation_meta::LogRecord::WriteManifest { ino: i, .. } if *i == ino))
            .count()
    };
    let committed = manifests();
    let open = || {
        Blocking::run(|r| {
            c.view.open(
                &c.cx(OpKind::Open),
                ino,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap()
    };
    std::thread::sleep(std::time::Duration::from_millis(2));
    let o = open();
    assert_eq!(code(c.write(ino, o.fh, 0, &[1u8; 4096])), Code::NoSpace);
    c.close(ino, o.fh).unwrap();
    assert_eq!(
        times(&c._meta.getattr(ino).unwrap().unwrap()),
        times(&before),
        "after a refused write"
    );
    let o = open();
    let refused = Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            Some(o.fh),
            &SetAttr {
                size: Some(4096),
                ..SetAttr::default()
            },
            r,
        )
    });
    assert_eq!(code(refused), Code::NoSpace);
    c.close(ino, o.fh).unwrap();
    assert_eq!(
        times(&c._meta.getattr(ino).unwrap().unwrap()),
        times(&before),
        "after a refused truncate"
    );
    assert_eq!(manifests(), committed, "a close published a manifest");
}

/// The other side of [`a_refused_write_or_truncate_leaves_the_times_to_the_close`]:
/// a session whose first write was refused and whose next one succeeded
/// changed data, and its close publishes it.
#[test]
fn a_write_after_a_refused_one_is_published() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"data").unwrap();
    c.close(ino, o.fh).unwrap();
    c._meta.set_quota(Some(100)).unwrap();
    View::invalidate_quota_cache(&c.view.quota_cache);
    let before = c._meta.getattr(ino).unwrap().unwrap();
    let o = Blocking::run(|r| {
        c.view.open(
            &c.cx(OpKind::Open),
            ino,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    assert_eq!(code(c.write(ino, o.fh, 0, &[1u8; 4096])), Code::NoSpace);
    c.write(ino, o.fh, 0, b"0123456789").unwrap();
    c.close(ino, o.fh).unwrap();
    let after = c._meta.getattr(ino).unwrap().unwrap();
    assert_eq!(after.size, 10);
    assert!(after.mtime_ns > before.mtime_ns && after.ctime_ns > before.ctime_ns);
    let o = Blocking::run(|r| {
        c.view.open(
            &c.cx(OpKind::Open),
            ino,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    assert_eq!(c.read(ino, o.fh, 0, 64).unwrap(), b"0123456789");
    c.close(ino, o.fh).unwrap();
}

#[test]
fn sync_view_publishes_every_pending_write() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    c.write(e.attr.ino, o.fh, 0, b"hello").unwrap();
    assert_eq!(
        c._meta.getattr(e.attr.ino).unwrap().unwrap().size,
        0,
        "not published yet"
    );
    Blocking::run(|r| c.view.sync_view(&c.cx(OpKind::SyncView), r)).unwrap();
    assert_eq!(c._meta.getattr(e.attr.ino).unwrap().unwrap().size, 5);
    assert_eq!(c.read(e.attr.ino, o.fh, 0, 16).unwrap(), b"hello");
}

#[test]
fn without_cluster_locks_the_lock_ops_are_not_implemented() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let lock = LockSpec {
        owner: LockOwner(1),
        range: LockRange {
            start: 0,
            end: u64::MAX,
        },
        kind: LockKind::Write,
        pid: 1,
    };
    let ino = e.attr.ino;
    let test = Blocking::run(|r| {
        c.view
            .lock_test(&c.cx(OpKind::LockTest), ino, o.fh, lock, r)
    });
    assert_eq!(code(test), Code::NotImplemented);
    for sleep in [false, true] {
        let acquire = Blocking::run(|r| {
            c.view
                .lock_acquire(&c.cx(OpKind::LockAcquire), ino, o.fh, lock, sleep, r)
        });
        assert_eq!(code(acquire), Code::NotImplemented);
    }
    let release = Blocking::run(|r| {
        c.view.lock_release(
            &c.cx(OpKind::LockRelease),
            ino,
            o.fh,
            lock.owner,
            lock.range,
            r,
        )
    });
    assert_eq!(code(release), Code::NotImplemented);
}

#[test]
fn the_synthetic_tree_is_browsable_and_read_only() {
    let c = client();
    let meta_dir = c.lookup(ROOT_INO, ".constellation").unwrap();
    assert_eq!(meta_dir.attr.kind, FileKind::Dir);
    assert!(View::is_synthetic(meta_dir.attr.ino));
    let snap = c.lookup(meta_dir.attr.ino, "snapshot").unwrap();
    assert_eq!(snap.attr.kind, FileKind::Dir);
    let names: Vec<String> = c
        .readdir(meta_dir.attr.ino, 0, 10)
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(names, [".", "..", "snapshot"]);
    assert_eq!(code(c.mkdir(meta_dir.attr.ino, "x")), Code::ReadOnly);
    let opened = Blocking::run(|r| {
        c.view.open(
            &c.cx(OpKind::Open),
            meta_dir.attr.ino,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    });
    assert_eq!(code(opened), Code::IsDir);
    assert_eq!(
        code(c.setxattr(meta_dir.attr.ino, "user.a", b"1", SetXattrFlags::empty())),
        Code::ReadOnly
    );
}

#[test]
fn every_op_is_watched_until_its_responder_answered() {
    let c = client();
    let watch = c.view.op_watch().clone();
    // The responder runs while the op is still registered: the watchdog
    // (like the in-flight registry) releases it only after the answer.
    let seen = Arc::new(Mutex::new(None));
    let during = seen.clone();
    let watch_in = watch.clone();
    c.view.getattr(
        &c.cx(OpKind::Getattr),
        ROOT_INO,
        None,
        FnResponder::new(move |r: VfsResult<Attr>| {
            *during.lock().unwrap() = Some((r.is_ok(), watch_in.snapshot().in_flight));
        }),
    );
    assert_eq!(*seen.lock().unwrap(), Some((true, 1)));
    assert_eq!(watch.snapshot().in_flight, 0);
}

impl Client {
    fn rename_flags(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: RenameFlags,
    ) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view.rename(
                &self.cx(OpKind::Rename),
                parent,
                Name::new(name),
                new_parent,
                Name::new(new_name),
                flags,
                constellation_vfs::Unit(r),
            )
        })
    }

    fn fsync(&self, ino: Ino, fh: Fh) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view.fsync(
                &self.cx(OpKind::Fsync),
                ino,
                fh,
                constellation_vfs::Durability::Configured,
                r,
            )
        })
    }

    fn put(&self, parent: Ino, name: &str, data: &[u8]) -> Ino {
        let (e, o) = self.create(parent, name).unwrap();
        self.write(e.attr.ino, o.fh, 0, data).unwrap();
        self.close(e.attr.ino, o.fh).unwrap();
        e.attr.ino
    }
}

/// renameat2 flags through `View::rename` (plan 31 C4 follow-up): before,
/// the view ignored them, so `RENAME_EXCHANGE` replaced — unlinked — the
/// target instead of swapping, and `RENAME_NOREPLACE` was only the
/// kernel's dcache check.
#[test]
fn rename_honours_noreplace_and_exchange_and_refuses_whiteout() {
    let c = client();
    let a = c.put(ROOT_INO, "a", b"A");
    let b = c.put(ROOT_INO, "b", b"B");
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    assert_eq!(
        code(c.rename_flags(ROOT_INO, "a", ROOT_INO, "b", RenameFlags::NOREPLACE)),
        Code::Exists
    );
    assert_eq!(c.lookup(ROOT_INO, "b").unwrap().attr.ino, b);
    c.rename_flags(ROOT_INO, "a", d, "a", RenameFlags::NOREPLACE)
        .unwrap();
    assert_eq!(c.lookup(d, "a").unwrap().attr.ino, a);
    // EXCHANGE across directories: both files survive, swapped.
    c.rename_flags(d, "a", ROOT_INO, "b", RenameFlags::EXCHANGE)
        .unwrap();
    assert_eq!(c.lookup(d, "a").unwrap().attr.ino, b);
    assert_eq!(c.lookup(ROOT_INO, "b").unwrap().attr.ino, a);
    let o = Blocking::run(|r| {
        c.view
            .open(&c.cx(OpKind::Open), a, OpenFlags::READ, OpenOwner::NONE, r)
    })
    .unwrap();
    assert_eq!(c.read(a, o.fh, 0, 8).unwrap(), b"A");
    assert_eq!(
        code(c.rename_flags(ROOT_INO, "b", d, "zz", RenameFlags::EXCHANGE)),
        Code::NotFound
    );
    for flags in [
        RenameFlags::WHITEOUT,
        RenameFlags::UNSUPPORTED,
        RenameFlags::NOREPLACE | RenameFlags::EXCHANGE,
    ] {
        assert_eq!(
            code(c.rename_flags(ROOT_INO, "b", d, "a", flags)),
            Code::Invalid,
            "{flags:?}"
        );
    }
    assert_eq!(c.lookup(d, "a").unwrap().attr.ino, b);
}

/// POSIX: a file written, unlinked and still open keeps working through
/// its descriptor — `write`, `fsync`, `ftruncate`, `fchmod`, reads, and
/// the closing `flush`/`release` all succeed. Before, the flush committed
/// the manifest of an inode with no name and every one of them answered
/// `ENOENT` (on a real mount: `fsync`/`close` of such a file failed).
#[test]
fn an_unlinked_open_file_keeps_working_until_its_last_close() {
    let c = client();
    let (e, o) = c.create(ROOT_INO, "f").unwrap();
    let ino = e.attr.ino;
    c.write(ino, o.fh, 0, b"hello world").unwrap();
    c.unlink(ROOT_INO, "f").unwrap();
    assert_eq!(code(c.lookup(ROOT_INO, "f")), Code::NotFound);
    c.write(ino, o.fh, 0, b"HELLO").unwrap();
    c.fsync(ino, o.fh).unwrap();
    assert_eq!(c.read(ino, o.fh, 0, 64).unwrap(), b"HELLO world");
    let attr = Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            ino,
            Some(o.fh),
            &SetAttr {
                size: Some(8),
                mode: Some(0o600),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    assert_eq!((attr.size, attr.mode & 0o7777, attr.nlink), (8, 0o600, 0));
    assert_eq!(c.read(ino, o.fh, 0, 64).unwrap(), b"HELLO wo");
    // A second descriptor's close (`flush` only), then more writes.
    Blocking::run(|r| {
        c.view
            .flush(&c.cx(OpKind::Flush), ino, o.fh, LockOwner(9), r)
    })
    .unwrap();
    c.write(ino, o.fh, 8, b"!").unwrap();
    assert_eq!(c.getattr(ino).unwrap().size, 9);
    // The last close succeeds, drops the session and reaps the orphan.
    c.close(ino, o.fh).unwrap();
    assert!(c.view.writes.pending_inos().is_empty());
    assert!(c.view.meta.getattr(ino).unwrap().is_none(), "reaped");
    assert!(c.view.meta.orphans().unwrap().is_empty());
}

/// Plan 39 §3.7: a discard of writes made under a lapsed lock grant is an
/// error event reported errseq-style — every open file description that
/// was open when it happened sees `EIO` exactly once, at its next `fsync`
/// or close, and one opened afterwards never does. Before, the inode owed
/// one `EIO` the first publication point took, and a second descriptor's
/// `fsync` returned 0 for data that had been thrown away.
#[test]
fn a_discard_is_reported_once_to_every_description_open_when_it_happened() {
    let c = client();
    let ino = c.put(ROOT_INO, "f", b"data");
    let open = || {
        Blocking::run(|r| {
            c.view.open(
                &c.cx(OpKind::Open),
                ino,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap()
        .fh
    };
    let flush =
        |fh: Fh| Blocking::run(|r| c.view.flush(&c.cx(OpKind::Flush), ino, fh, LockOwner(3), r));
    let (first, second, third) = (open(), open(), open());
    // What `lock_discard_tainted`/a recalled grant's flush record when
    // they throw dirty data away with no publication point to tell.
    c.view.meta.locks().note_discard(ino);
    let later = open();

    assert_eq!(code(c.fsync(ino, first)), Code::Io);
    c.fsync(ino, first).expect("reported once per description");
    assert_eq!(code(c.fsync(ino, second)), Code::Io, "a second descriptor");
    c.fsync(ino, second).unwrap();
    assert_eq!(code(flush(third)), Code::Io, "a close reports it too");
    flush(third).unwrap();
    c.fsync(ino, later).expect("opened after the discard");
    flush(later).unwrap();

    // A second event: owed again to every description still open.
    c.view.meta.locks().note_discard(ino);
    assert_eq!(code(c.fsync(ino, later)), Code::Io);
    assert_eq!(code(flush(first)), Code::Io);
    for fh in [first, second, third, later] {
        let _ = flush(fh);
        Blocking::run(|r| {
            c.view
                .release(&c.cx(OpKind::Release), ino, fh, OpenFlags::WRITE, None, r)
        })
        .unwrap();
    }
    // The last close forgets the inode's events: a new open owes nothing.
    assert_eq!(c.view.meta.locks().error_seq(ino), 0);
    let fresh = open();
    c.fsync(ino, fresh).unwrap();
    c.close(ino, fresh).unwrap();
}

/// POSIX set-group-ID directories (Linux `inode_init_owner`), which FUSE
/// leaves to the filesystem: what is made in one takes its group, and a
/// subdirectory the bit as well; outside one the caller's group stands.
/// Kubernetes' `fsGroup` rests on it (plan 37 settled decision 11).
#[test]
fn a_setgid_directory_hands_its_group_to_new_entries() {
    let mut c = client();
    let shared = c.mkdir(ROOT_INO, "shared").unwrap().attr.ino;
    let plain = c.mkdir(ROOT_INO, "plain").unwrap().attr.ino;
    // The owner marks it g+s with group 2000 (root may chown to any group).
    c.caller = Caller::new(0, 0, None);
    Blocking::run(|r| {
        c.view.setattr(
            &c.cx(OpKind::Setattr),
            shared,
            None,
            &SetAttr {
                gid: Some(2000),
                mode: Some(0o2777),
                ..SetAttr::default()
            },
            r,
        )
    })
    .unwrap();
    c.caller = Caller::new(1000, 1000, None);

    let (file, o) = c.create(shared, "f").unwrap();
    c.close(file.attr.ino, o.fh).unwrap();
    assert_eq!((file.attr.uid, file.attr.gid), (1000, 2000));
    assert_eq!(file.attr.mode & 0o7777, 0o644);
    let sub = c.mkdir(shared, "sub").unwrap();
    assert_eq!((sub.attr.uid, sub.attr.gid), (1000, 2000));
    assert_eq!(
        sub.attr.mode & 0o7777,
        0o2755,
        "a subdirectory inherits S_ISGID"
    );
    // ... and hands the group on in turn.
    let (deep, o) = c.create(sub.attr.ino, "deep").unwrap();
    c.close(deep.attr.ino, o.fh).unwrap();
    assert_eq!(deep.attr.gid, 2000);
    let sym = Blocking::run(|r| {
        c.view
            .symlink(&c.cx(OpKind::Symlink), shared, Name::new("s"), b"f", r)
    })
    .unwrap();
    assert_eq!(sym.attr.gid, 2000);
    let fifo = Blocking::run(|r| {
        c.view.mknod(
            &c.cx(OpKind::Mknod),
            shared,
            Name::new("p"),
            constellation_vfs::types::mode::S_IFIFO | 0o640,
            constellation_types::Rdev::default(),
            r,
        )
    })
    .unwrap();
    assert_eq!((fifo.attr.gid, fifo.attr.mode & 0o7777), (2000, 0o640));
    // What a lookup reads back is what was stored, not just the reply.
    assert_eq!(c.lookup(shared, "f").unwrap().attr.gid, 2000);

    // A member of the directory's group creating a file with its own
    // S_ISGID: the kernel leaves the bit for a member, and it is kept,
    // with the group inherited.
    let member = Caller::new(1000, 2000, None);
    let (sgid_file, o) = Blocking::run(|r| {
        c.view.create(
            &OpCtx::new(OpKind::Create, &member),
            shared,
            Name::new("sgid"),
            0o102755,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    c.close(sgid_file.attr.ino, o.fh).unwrap();
    assert_eq!(sgid_file.attr.gid, 2000);
    assert_eq!(
        sgid_file.attr.mode & 0o7777,
        0o2755,
        "a member keeps S_ISGID"
    );
    // Opening an existing entry through create (the lost race) keeps what
    // is stored, whatever group the caller has.
    let stranger = Caller::new(1000, 3000, None);
    let (again, o) = Blocking::run(|r| {
        c.view.create(
            &OpCtx::new(OpKind::Create, &stranger),
            shared,
            Name::new("f"),
            0o100644,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    c.close(again.attr.ino, o.fh).unwrap();
    assert_eq!(again.attr.gid, 2000, "an existing entry keeps its gid");

    // Outside a set-group-ID directory: the caller's group, no bit.
    let (other, o) = c.create(plain, "f").unwrap();
    c.close(other.attr.ino, o.fh).unwrap();
    assert_eq!(other.attr.gid, 1000);
    let other_dir = c.mkdir(plain, "d").unwrap();
    assert_eq!(
        (other_dir.attr.gid, other_dir.attr.mode & 0o7777),
        (1000, 0o755)
    );
}

impl Client {
    #[allow(clippy::too_many_arguments)]
    fn copy(
        &self,
        src: Ino,
        fh_in: Fh,
        off_in: u64,
        dst: Ino,
        fh_out: Fh,
        off_out: u64,
        len: u64,
    ) -> VfsResult<u64> {
        self.copy_cx(
            &self.cx(OpKind::CopyFileRange),
            src,
            fh_in,
            off_in,
            dst,
            fh_out,
            off_out,
            len,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_cx(
        &self,
        cx: &OpCtx<'_>,
        src: Ino,
        fh_in: Fh,
        off_in: u64,
        dst: Ino,
        fh_out: Fh,
        off_out: u64,
        len: u64,
    ) -> VfsResult<u64> {
        Blocking::run(|r| {
            self.view.copy_file_range(
                cx,
                src,
                fh_in,
                off_in,
                dst,
                fh_out,
                off_out,
                len,
                constellation_vfs::CopyFlags::empty(),
                r,
            )
        })
    }
}

/// `copy_file_range` copies what a read of the view answers — here a
/// write of another description, pending (not yet published) — and never
/// anything the frontend cached. Before, the view did not implement the
/// op (`NotImplemented`, FUSE `ENOSYS`) and Linux copied with a splice
/// that read the source through its page cache, which can hold a stale
/// page for a write already acknowledged (the `view::copy` module doc):
/// the copy persisted those bytes. The copy publishes nothing of the
/// source, and the destination's bytes wait in its own session for its
/// close, exactly as a write's would.
#[test]
fn copy_file_range_copies_what_the_view_reads_including_pending_writes() {
    let c = client();
    let src = c.put(ROOT_INO, "src", &[b'A'; 8192]);
    let w = c.open(src, OpenFlags::READ | OpenFlags::WRITE).unwrap();
    c.write(src, w.fh, 4096, b"BBBB").unwrap();
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    assert_eq!(c.copy(src, r.fh, 4094, dst, d.fh, 0, 8).unwrap(), 8);
    assert_eq!(c.read(dst, d.fh, 0, 64).unwrap(), b"AABBBBAA");
    assert_eq!(c.getattr(dst).unwrap().size, 8);
    assert_eq!(
        c._meta.getattr(dst).unwrap().unwrap().size,
        0,
        "pending in the destination's session, as a write's bytes are"
    );
    assert!(
        c.view.writes.lock(src).contains_key(&src),
        "the source's session is its writer's, unpublished"
    );
    c.close(dst, d.fh).unwrap();
    assert_eq!(c._meta.getattr(dst).unwrap().unwrap().size, 8);
    c.close(src, r.fh).unwrap();
    c.close(src, w.fh).unwrap();
    let o = c.open(dst, OpenFlags::READ).unwrap();
    assert_eq!(c.read(dst, o.fh, 0, 64).unwrap(), b"AABBBBAA");
}

/// Into an `O_SYNC` description the copy publishes before it answers, as
/// each write through it does (the handle remembers the flag: a FUSE copy
/// carries no open flags); through any other it does not.
#[test]
fn a_copy_into_an_o_sync_description_is_published_before_it_answers() {
    let c = client();
    let src = c.put(ROOT_INO, "src", b"durable bytes");
    let r = c.open(src, OpenFlags::READ).unwrap();
    let dst = c.put(ROOT_INO, "dst", b"");
    let s = c.open(dst, OpenFlags::WRITE | OpenFlags::SYNC).unwrap();
    assert_eq!(c.copy(src, r.fh, 0, dst, s.fh, 0, 64).unwrap(), 13);
    assert_eq!(
        c._meta.getattr(dst).unwrap().unwrap().size,
        13,
        "published before the answer"
    );
    let plain = c.open(dst, OpenFlags::WRITE).unwrap();
    assert_eq!(c.copy(src, r.fh, 0, dst, plain.fh, 13, 64).unwrap(), 13);
    assert_eq!(c._meta.getattr(dst).unwrap().unwrap().size, 13);
    assert_eq!(c.getattr(dst).unwrap().size, 26);
    c.close(dst, plain.fh).unwrap();
    c.close(dst, s.fh).unwrap();
    assert_eq!(c._meta.getattr(dst).unwrap().unwrap().size, 26);
}

/// A caller that is gone before the copy starts (its token cancelled: the
/// FUSE adapter's killable policy) gets `EINTR` and nothing is written.
#[test]
fn a_cancelled_copy_writes_nothing() {
    let c = client();
    let src = c.put(ROOT_INO, "src", b"0123456789");
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let token = constellation_vfs::CancelToken::new();
    token.cancel();
    let cx = c.cx(OpKind::CopyFileRange).with_cancel(&token);
    assert_eq!(
        code(c.copy_cx(&cx, src, r.fh, 0, e.attr.ino, d.fh, 0, 10)),
        Code::Intr
    );
    assert_eq!(c.getattr(e.attr.ino).unwrap().size, 0);
}

/// A copy longer than the chunk size runs in steps that end on the
/// destination's chunk boundaries (memory stays one chunk), seals the
/// chunks it fills as a sequential writer's are sealed, and a copy that
/// lands whole chunks at aligned offsets yields the source's own chunks:
/// content addressing shares them at rest.
#[test]
fn an_aligned_copy_yields_the_sources_chunks() {
    let c = client();
    let cs = c.view.chunk_size as usize;
    let data: Vec<u8> = (0..3 * cs + 100).map(|i| (i * 7 % 251) as u8).collect();
    let src = c.put(ROOT_INO, "src", &data);
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    let mut done = 0u64;
    while done < data.len() as u64 {
        let n = c
            .copy(src, r.fh, done, dst, d.fh, done, data.len() as u64 - done)
            .unwrap();
        assert!(n > 0);
        done += n;
    }
    c.close(dst, d.fh).unwrap();
    let chunks = |ino| {
        let manifest = c.view.load_manifest(ino).unwrap();
        c.view.chunk_list(&manifest).unwrap()
    };
    assert_eq!(chunks(dst), chunks(src), "the same chunks, shared");
    let o = c.open(dst, OpenFlags::READ).unwrap();
    let mut got = Vec::new();
    while got.len() < data.len() {
        let part = c.read(dst, o.fh, got.len() as u64, 1 << 20).unwrap();
        assert!(!part.is_empty());
        got.extend_from_slice(&part);
    }
    assert!(got == data);
}

/// An unlinked file still open is a file like any other to a copy, as
/// source and as destination (POSIX: the inode lives while open).
#[test]
fn a_copy_reads_and_writes_unlinked_open_files() {
    let c = client();
    let src = c.put(ROOT_INO, "src", b"still here");
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    c.unlink(ROOT_INO, "src").unwrap();
    c.unlink(ROOT_INO, "dst").unwrap();
    assert_eq!(c.copy(src, r.fh, 0, dst, d.fh, 0, 64).unwrap(), 10);
    assert_eq!(c.read(dst, d.fh, 0, 64).unwrap(), b"still here");
    c.close(src, r.fh).unwrap();
    c.close(dst, d.fh).unwrap();
}

/// Runs `hook` between the steps of the copies this thread makes until
/// dropped (`copy::AFTER_STEP`).
pub(super) struct BetweenSteps;

impl BetweenSteps {
    pub(super) fn set(hook: impl FnMut(u64) + 'static) -> Self {
        super::copy::AFTER_STEP.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
        BetweenSteps
    }
}

impl Drop for BetweenSteps {
    fn drop(&mut self) {
        super::copy::AFTER_STEP.with(|h| *h.borrow_mut() = None);
    }
}

/// A copy never answers a code that would make Linux copy the request
/// itself through its page cache: `ENOSYS` would also stop it asking for
/// the mount's lifetime (`fc->no_copy_file_range`).
#[test]
fn a_copy_never_answers_a_code_that_makes_the_kernel_copy_itself() {
    use super::copy::never_a_fallback;
    for code in [Code::NotImplemented, Code::NotSupported, Code::CrossDevice] {
        assert_eq!(never_a_fallback(code), Code::Io, "{code:?}");
    }
    for code in [
        Code::Io,
        Code::NoSpace,
        Code::BadFd,
        Code::Invalid,
        Code::Intr,
    ] {
        assert_eq!(never_a_fallback(code), code);
    }
}

/// The size, then every byte, of `ino`.
fn contents(c: &Client, ino: Ino) -> Vec<u8> {
    let o = c.open(ino, OpenFlags::READ).unwrap();
    let mut all = Vec::new();
    loop {
        let part = c.read(ino, o.fh, all.len() as u64, 1 << 20).unwrap();
        if part.is_empty() {
            break;
        }
        all.extend_from_slice(&part);
    }
    c.close(ino, o.fh).unwrap();
    assert_eq!(all.len() as u64, c.getattr(ino).unwrap().size);
    all
}

/// A copy whose second step cannot read its source (the chunk is in no
/// cache and not in the store) answers the first step's bytes — exactly
/// what the destination then holds — and the next call, from there,
/// reports the error.
#[test]
fn a_copy_that_cannot_read_its_source_midway_is_short_by_exactly_the_rest() {
    let c = client();
    let cs = c.view.chunk_size as usize;
    let data: Vec<u8> = (0..cs + 10).map(|i| (i % 253) as u8 + 1).collect();
    let src = c.put(ROOT_INO, "src", &data);
    let manifest = c.view.load_manifest(src).unwrap();
    let hashes = c.view.chunk_list(&manifest).unwrap();
    // Chunk 1 is lost: never uploaded (no store sync here), now evicted.
    c.view.cache.remove(&hashes[&1]).unwrap();
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    // Two steps: the destination's chunk 0 ends 10 bytes in, where the
    // source's chunk 1 begins.
    let at = cs as u64 - 10;
    assert_eq!(c.copy(src, r.fh, at, dst, d.fh, at, 20).unwrap(), 10);
    let mut expect = vec![0u8; at as usize];
    expect.extend_from_slice(&data[at as usize..cs]);
    assert!(
        contents(&c, dst) == expect,
        "the short count is what is there"
    );
    let next = c.copy(src, r.fh, cs as u64, dst, d.fh, cs as u64, 10);
    assert!(
        next.as_ref()
            .is_err_and(|e| e.code() != Code::NotImplemented),
        "{next:?}"
    );
    assert_eq!(c.getattr(dst).unwrap().size, cs as u64);
    c.close(dst, d.fh).unwrap();
}

/// A copy refused space midway (`ENOSPC`, here the quota) is short by
/// exactly what it could not write, and the next call reports `ENOSPC`.
#[test]
fn a_copy_refused_space_midway_is_short_by_exactly_the_rest() {
    let c = client();
    let cs = c.view.chunk_size as u64;
    let src = c.put(ROOT_INO, "src", b"0123456789abcdefghij");
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    // Room for the destination's growth to the first step's end only.
    let (used, _) = c._meta.usage();
    c._meta.set_quota(Some(used + cs + 5)).unwrap();
    View::invalidate_quota_cache(&c.view.quota_cache);
    assert_eq!(c.copy(src, r.fh, 0, dst, d.fh, cs - 10, 20).unwrap(), 10);
    assert_eq!(c.getattr(dst).unwrap().size, cs);
    assert_eq!(
        code(c.copy(src, r.fh, 10, dst, d.fh, cs, 10)),
        Code::NoSpace
    );
    let all = contents(&c, dst);
    assert_eq!(&all[(cs - 10) as usize..], b"0123456789");
    assert!(all[..(cs - 10) as usize].iter().all(|b| *b == 0));
    c.close(dst, d.fh).unwrap();
}

/// A caller that dies while its copy runs ends it at the next step: the
/// answer counts the steps done, which the destination holds.
#[test]
fn a_copy_cancelled_between_steps_answers_the_steps_done() {
    let c = client();
    let cs = c.view.chunk_size as u64;
    let data: Vec<u8> = (0..cs + 20).map(|i| (i % 251) as u8 + 1).collect();
    let src = c.put(ROOT_INO, "src", &data);
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    let token = constellation_vfs::CancelToken::new();
    let cancel = token.clone();
    let _hook = BetweenSteps::set(move |_| cancel.cancel());
    let cx = c.cx(OpKind::CopyFileRange).with_cancel(&token);
    // Three steps (10, a chunk, 10): cancelled after the first.
    assert_eq!(
        c.copy_cx(&cx, src, r.fh, 0, dst, d.fh, cs - 10, cs + 20)
            .unwrap(),
        10
    );
    assert_eq!(c.getattr(dst).unwrap().size, cs);
    assert_eq!(&contents(&c, dst)[(cs - 10) as usize..], &data[..10]);
    c.close(dst, d.fh).unwrap();
}

/// One call copies at most `COPY_CHUNKS` chunks (`copy`'s module doc: the
/// kernel holds the destination's `i_rwsem` across it, unkillably); the
/// caller loops for the rest.
#[test]
fn one_copy_call_is_capped_at_a_few_chunks() {
    use super::copy::{COPY_CALL_MAX, COPY_CHUNKS};
    let c = client();
    let cs = c.view.chunk_size as u64;
    let cap = (COPY_CHUNKS * cs).min(COPY_CALL_MAX).max(cs);
    let data: Vec<u8> = (0..cap + cs / 2).map(|i| (i % 249) as u8).collect();
    let src = c.put(ROOT_INO, "src", &data);
    let r = c.open(src, OpenFlags::READ).unwrap();
    let (e, d) = c.create(ROOT_INO, "dst").unwrap();
    let dst = e.attr.ino;
    let total = data.len() as u64;
    assert_eq!(c.copy(src, r.fh, 0, dst, d.fh, 0, total).unwrap(), cap);
    assert_eq!(
        c.copy(src, r.fh, cap, dst, d.fh, cap, total - cap).unwrap(),
        total - cap
    );
    c.close(dst, d.fh).unwrap();
    assert!(contents(&c, dst) == data);
}

/// A listing with attributes: the entries a plus listing of `dir` hands
/// out (plan 40 §4.5), every name but `.` and `..` with one.
fn list_with_attributes(c: &Client, dir: Ino) -> Vec<Entry> {
    // One page: a page that fills makes the entry that did not fit for
    // nothing (`DirSink` cannot be asked for room first).
    c.readdir_plus(dir, 0, 10_000)
        .unwrap()
        .into_iter()
        .filter(|e| e.name.as_bytes() != b"." && e.name.as_bytes() != b"..")
        .map(|e| {
            let name = String::from_utf8_lossy(e.name.as_bytes()).into_owned();
            e.entry
                .unwrap_or_else(|| panic!("{name:?} is name-only in a plus listing"))
        })
        .collect()
}

/// Plan 45, the first priority: a plus listing of a directory whose names
/// are all their inodes' only name answers every entry from the dentry's
/// copy and reads no `0x01` record for it.
#[test]
fn a_listing_of_single_name_entries_reads_no_inode() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    for i in 0..40 {
        c.put(d, &format!("f{i}"), b"data");
    }
    c.mkdir(d, "sub").unwrap();
    let before = c._meta.inode_reads();
    let listed = list_with_attributes(&c, d);
    assert_eq!(listed.len(), 41);
    assert!(listed
        .iter()
        .all(|e| e.attr.nlink == 1 || e.attr.nlink == 2));
    assert_eq!(
        c._meta.inode_reads(),
        before,
        "a name that is its inode's only name needs no 0x01 read"
    );
}

/// The designed cost of a hard link: one `0x01` point read per hard-linked
/// entry, and the entry answers what `getattr` answers.
#[test]
fn a_listing_of_hard_linked_entries_reads_each_inode_once_per_entry() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let f = c.put(d, "f", b"data");
    for i in 0..10 {
        Blocking::run(|r| {
            c.view
                .link(&c.cx(OpKind::Link), f, d, Name::new(&format!("h{i}")), r)
        })
        .unwrap();
    }
    for i in 0..5 {
        c.put(d, &format!("single{i}"), b"x");
    }
    let before = c._meta.inode_reads();
    let listed = list_with_attributes(&c, d);
    assert_eq!(listed.len(), 16);
    assert_eq!(
        c._meta.inode_reads() - before,
        11,
        "f's eleven names cost one read each, the five single names none"
    );
    let want = c.getattr(f).unwrap();
    for e in listed.iter().filter(|e| e.attr.ino == f) {
        assert_eq!((e.attr.nlink, e.attr.size), (11, want.size));
        assert_eq!(e.attr.nlink, want.nlink);
    }
}

// ---- Plan 40 P7: plus entries are what a lookup answers ------------------
//
// Each test that guards a step runs twice (`plus::testing::guarded`): on the
// real page, where it passes, and on the naive implementation of the step it
// guards (`plus::Naive`), where it has to fail.

use super::plus::testing::{guarded, BetweenGuard};
use super::plus::Naive;

/// The plus listing of `dir`, name by name: its entry, or `None` for a
/// name-only one. `.` and `..` are always name-only.
fn plus_by_name(c: &Client, dir: Ino) -> std::collections::BTreeMap<String, Option<Entry>> {
    c.readdir_plus_all(dir)
        .into_iter()
        .map(|e| {
            let name = String::from_utf8_lossy(e.name.as_bytes()).into_owned();
            if name == "." || name == ".." {
                assert!(e.entry.is_none(), "{name:?} carries an entry");
            }
            (name, e.entry)
        })
        .collect()
}

/// Every name of `dir` (but `.` and `..`) has a plus entry equal to what
/// a lookup of it answers now.
fn plus_is_lookup(c: &Client, dir: Ino) -> Result<(), String> {
    for (name, entry) in plus_by_name(c, dir) {
        if name == "." || name == ".." {
            continue;
        }
        let Some(entry) = entry else {
            return Err(format!("{name:?} is name-only"));
        };
        let want = c.lookup(dir, &name).unwrap();
        if entry != want {
            return Err(format!("{name:?}: plus {entry:?}, lookup {want:?}"));
        }
    }
    Ok(())
}

/// A name's size is what `getattr` of its inode says, with a write session
/// pending on it (the committed row says less).
#[test]
fn plus_attrs_equal_getattr_with_a_pending_write_session() {
    guarded(Naive::NoOverlay, |_| {
        let c = client();
        let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
        c.put(d, "committed", b"abc");
        c.mkdir(d, "sub").unwrap();
        let (open, o) = c.create(d, "open").unwrap();
        c.write(open.attr.ino, o.fh, 0, &[9u8; 5000]).unwrap();
        assert_eq!(c._meta.getattr(open.attr.ino).unwrap().unwrap().size, 0);
        plus_is_lookup(&c, d)?;
        let entry = plus_by_name(&c, d)["open"].clone().unwrap();
        let want = c.getattr(open.attr.ino).unwrap();
        if entry.attr != want || want.size != 5000 {
            return Err(format!("plus {:?}, getattr {want:?}", entry.attr));
        }
        c.close(open.attr.ino, o.fh).unwrap();
        Ok(())
    });
}

/// A scratch directory's names are looked up in the scratch namespace, a
/// file being written there at its pending size.
#[test]
fn plus_entries_of_a_scratch_directory_are_what_lookup_answers() {
    guarded(Naive::NoScratch, |_| {
        let c = client();
        let scratch = c.mkdir(ROOT_INO, "tmp").unwrap().attr.ino;
        c.setxattr(
            scratch,
            constellation_meta::SCRATCH_XATTR,
            b"1",
            SetXattrFlags::default(),
        )
        .unwrap();
        c.mkdir(scratch, "sub").unwrap();
        let (e, o) = c.create(scratch, "f").unwrap();
        c.write(e.attr.ino, o.fh, 0, b"abcdef").unwrap();
        plus_is_lookup(&c, scratch)?;
        let size = plus_by_name(&c, scratch)["f"].clone().unwrap().attr.size;
        if size != 6 {
            return Err(format!("the pending size is 6, the entry says {size}"));
        }
        c.close(e.attr.ino, o.fh).unwrap();
        Ok(())
    });
}

/// A flush that commits and retires the session between the page's
/// listing and its attribute read never makes the entry report the size
/// from before it: the dentry is read under the child's shard, where the
/// session is either still there or already committed.
#[test]
fn a_flush_between_listing_and_attr_read_never_reports_the_old_size() {
    guarded(Naive::ReadBeforeShard, |_| {
        let c = client();
        let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
        let (f, o) = c.create(d, "f").unwrap();
        let f = f.attr.ino;
        c.write(f, o.fh, 0, &[7u8; 100]).unwrap();
        c.put(d, "other", b"x");
        let listed = std::thread::scope(|s| {
            let hook = BetweenGuard::acting(s, |name| {
                if name == "f" {
                    c.close(f, o.fh).unwrap();
                }
            });
            let listed = plus_by_name(&c, d);
            drop(hook);
            listed
        });
        let size = listed["f"].clone().unwrap().attr.size;
        if size != 100 {
            return Err(format!("the file holds 100 bytes, the entry says {size}"));
        }
        if c.getattr(f).unwrap().size != 100 {
            return Err("the flush did not commit".into());
        }
        Ok(())
    });
}

/// `--cto strict` on a node that is not a lone sequencer: the kernel
/// would cache nothing of an entry, so no entry is made. (From cookie 2,
/// past the listing's strict read, which needs a sequencer to answer.)
#[test]
fn strict_cto_plus_entries_are_name_only() {
    guarded(Naive::KeepZeroTtl, |_| {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
        for name in ["a", "b", "c"] {
            meta.create(d, name, 0o644, 0, 0).unwrap();
        }
        let (mut fs, _tmp, _core) = super::durable_ack_tests::holder_fs(meta.clone(), false);
        fs.sync.as_mut().unwrap().cto_strict = true;
        // Another node showed itself: the TTL is zero from now on.
        meta.read_delegations()
            .leave_alone(constellation_authority::WaitMs(0), 0);
        assert!(fs.ttl().is_zero());
        let page = super::plus::testing::plus_page(&fs, d, 2, 100).unwrap();
        let mut names: Vec<_> = page.iter().map(|e| e.name.as_bytes().to_vec()).collect();
        names.sort();
        assert_eq!(names, [b"a", b"b", b"c"].map(|n| n.to_vec()));
        match page.iter().find_map(|e| e.entry.as_ref()) {
            Some(entry) => Err(format!("an entry with a zero TTL: {entry:?}")),
            None => Ok(()),
        }
    });
}

/// A lone strict sequencer's TTL is short but not zero, and its pages
/// carry entries.
#[test]
fn a_lone_strict_sequencer_gets_entries() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    for name in ["a", "b", "c"] {
        meta.create(d, name, 0o644, 0, 0).unwrap();
    }
    let (mut fs, _tmp, _core) = super::durable_ack_tests::holder_fs(meta, false);
    fs.sync.as_mut().unwrap().cto_strict = true;
    assert!(!fs.ttl().is_zero(), "a lone strict sequencer's TTL");
    // From the start: the listing's strict read is the sequencer's own.
    let page = super::plus::testing::plus_page(&fs, d, 0, 100).unwrap();
    let entries: Vec<_> = page.iter().filter_map(|e| e.entry.as_ref()).collect();
    assert_eq!(entries.len(), 3, "{page:?}");
    assert!(entries.iter().all(|e| e.attr.ttl == *fs.ttl()));
}

/// A rename on another view between the batch and the attribute read: the
/// name no longer leads to the listed inode (gone, or another's), and
/// the entry is name-only. The page does not fail and does not say the
/// other inode's attributes under the listed one's number.
#[test]
fn a_rename_on_another_view_between_batch_and_attr_read_makes_the_entry_name_only() {
    guarded(Naive::AnyInode, |_| {
        let a = client();
        let d = a.mkdir(ROOT_INO, "d").unwrap().attr.ino;
        for name in ["gone", "replaced", "stays", "mover"] {
            a.put(d, name, name.as_bytes());
        }
        let (view_b, dir_b) = super::quota_tests::test_fs(a._meta.clone());
        let b = Client {
            view: view_b,
            caller: Caller::new(1000, 1000, None),
            _meta: a._meta.clone(),
            _dir: dir_b,
        };
        let listed = std::thread::scope(|s| {
            let hook = BetweenGuard::acting(s, |name| match name {
                "gone" => b.rename(d, "gone", d, "elsewhere").unwrap(),
                // "mover" takes over the name, so "replaced" now leads to
                // another inode.
                "replaced" => b.rename(d, "mover", d, "replaced").unwrap(),
                _ => {}
            });
            let listed = a.readdir_plus_all(d);
            drop(hook);
            listed
        });
        for e in &listed {
            let name = String::from_utf8_lossy(e.name.as_bytes()).into_owned();
            match (name.as_str(), &e.entry) {
                ("gone" | "replaced", Some(entry)) => {
                    return Err(format!(
                        "{name:?} (listed as inode {}) carries an entry: {entry:?}",
                        e.ino
                    ))
                }
                ("stays", None) => return Err("an untouched name is name-only".into()),
                (_, Some(entry)) if entry.attr.ino != e.ino => {
                    return Err(format!(
                        "{name:?}: the entry is for inode {}",
                        entry.attr.ino
                    ))
                }
                _ => {}
            }
        }
        Ok(())
    });
}

/// Plan 45: a hard-linked name's dentry holds no attributes, so its plus
/// entry is read from the inode, and says what a lookup does: the link
/// count and the change time the inode holds, a size a session pending on
/// it overlays.
#[test]
fn plus_entries_of_hard_links_are_what_lookup_answers() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let f = c.put(d, "f", b"data");
    c.put(d, "single", b"x");
    for name in ["h1", "h2"] {
        Blocking::run(|r| c.view.link(&c.cx(OpKind::Link), f, d, Name::new(name), r)).unwrap();
    }
    // Changes through one name that only the inode records: the mode and
    // its change time, and a pending write.
    let ctime = c.getattr(f).unwrap().ctime_ns;
    c.setattr(
        f,
        &SetAttr {
            mode: Some(0o600),
            ..SetAttr::default()
        },
    )
    .unwrap();
    let o = c.open(f, OpenFlags::READ | OpenFlags::WRITE).unwrap();
    c.write(f, o.fh, 0, &[5u8; 3000]).unwrap();
    plus_is_lookup(&c, d).unwrap();
    let listed = plus_by_name(&c, d);
    let want = c.getattr(f).unwrap();
    assert!(want.ctime_ns > ctime, "the chmod changed the ctime");
    assert_eq!(
        (want.nlink, want.size, want.mode & 0o7777),
        (3, 3000, 0o600)
    );
    for name in ["f", "h1", "h2"] {
        let entry = listed[name].as_ref().unwrap_or_else(|| panic!("{name}"));
        assert_eq!(entry.attr, want, "{name}");
    }
    assert_eq!(listed["single"].as_ref().unwrap().attr.nlink, 1);
    c.close(f, o.fh).unwrap();
}

/// A sink whose platform holds the names of `held` (each with the inode
/// it leads to there): what the FUSE adapter answers for dentries its
/// kernel has.
struct HoldingSink {
    inner: CollectDir,
    held: Vec<(Ino, &'static str)>,
    asked: Arc<std::sync::atomic::AtomicU64>,
}

impl constellation_vfs::DirSink for HoldingSink {
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        self.inner.add(ino, next, kind, name)
    }

    fn add_entry(
        &mut self,
        ino: Ino,
        next: u64,
        kind: FileKind,
        name: &[u8],
        entry: Option<&Entry>,
    ) -> bool {
        self.inner.add_entry(ino, next, kind, name, entry)
    }

    fn holds(&mut self, ino: Ino, name: &[u8]) -> bool {
        self.asked
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.held
            .iter()
            .any(|(i, n)| *i == ino && n.as_bytes() == name)
    }
}

impl constellation_vfs::Responder<()> for HoldingSink {
    fn done(self, r: VfsResult<()>) {
        self.inner.done(r)
    }
}

/// A name the platform already holds, leading to the inode the listing
/// says, is added name-only and its attributes are not read (a
/// hard-linked name's would cost an inode read); a held name that leads
/// to another inode now, and every other name, get their entries. What
/// `git status` makes: it looks every tracked name up, then lists every
/// directory.
#[test]
fn a_plus_page_makes_no_entry_for_a_name_the_platform_holds() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let f = c.put(d, "f", b"data");
    let other = c.put(d, "other", b"x");
    for name in ["h1", "h2"] {
        Blocking::run(|r| c.view.link(&c.cx(OpKind::Link), f, d, Name::new(name), r)).unwrap();
    }
    let page = |held: Vec<(Ino, &'static str)>| {
        let (inner, wait) = CollectDir::pair(100);
        let asked = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let sink = HoldingSink {
            inner,
            held,
            asked: asked.clone(),
        };
        let reads = c._meta.inode_reads();
        c.view
            .readdir(&c.cx(OpKind::Readdirplus), d, Fh(0), 0, true, sink);
        let by: std::collections::BTreeMap<String, Option<Entry>> = wait
            .wait()
            .unwrap()
            .into_iter()
            .map(|e| (String::from_utf8(e.name.into_bytes()).unwrap(), e.entry))
            .collect();
        (
            by,
            c._meta.inode_reads() - reads,
            asked.load(std::sync::atomic::Ordering::Relaxed),
        )
    };
    // Nothing held: every name has its entry, the linked ones from the
    // inode.
    let (all, reads, asked) = page(vec![]);
    assert!(["f", "h1", "h2", "other"].iter().all(|n| all[*n].is_some()));
    assert_eq!(
        (reads, asked),
        (3, 4),
        "one inode read per linked name, `.`/`..` not asked"
    );
    // The linked names held (and `other` held as another inode's).
    let (listed, reads, _) = page(vec![
        (f, "f"),
        (f, "h1"),
        (f, "h2"),
        (other + 1000, "other"),
    ]);
    for name in ["f", "h1", "h2"] {
        assert_eq!(listed[name], None, "{name} is held");
    }
    assert_eq!(
        listed["other"], all["other"],
        "held as another inode: handed"
    );
    assert_eq!(reads, 0, "no attributes read for a held name");
}

/// A page that fills makes one entry more than it adds (the sink cannot be
/// asked for room first): a hard-linked name costs its read again on the
/// next page, but the cost is at most one entry a page, and a single name
/// costs no inode read at all.
#[test]
fn a_full_page_wastes_at_most_one_entry() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let f = c.put(d, "f", b"data");
    for i in 0..30 {
        Blocking::run(|r| {
            c.view
                .link(&c.cx(OpKind::Link), f, d, Name::new(&format!("h{i}")), r)
        })
        .unwrap();
    }
    for i in 0..30 {
        c.put(d, &format!("single{i}"), b"x");
    }
    let before = c._meta.inode_reads();
    let listed = c.readdir_plus_all(d);
    let pages = listed.len().div_ceil(7) as u64 + 1;
    let reads = c._meta.inode_reads() - before;
    assert!(
        (31..31 + pages).contains(&reads),
        "31 hard-linked names over {pages} pages cost {reads} reads"
    );
}

/// The dentry's copy is as current as the inode's row for every kind of
/// change (plan 45's form rule; plan 40 §4.5 step 3 reads the copy where a
/// lookup reads the row): after each of these, every plus entry of the
/// directory is what a lookup answers.
#[test]
fn plus_entries_follow_every_kind_of_change() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let f = c.put(d, "f", b"0123456789");
    let sub = c.mkdir(d, "sub").unwrap().attr.ino;
    c.put(d, "other", b"o");
    let step = |what: &str| {
        if let Err(e) = plus_is_lookup(&c, d) {
            panic!("after {what}: {e}");
        }
    };
    step("the setup");
    let set = |s: SetAttr| c.setattr(f, &s).unwrap();
    set(SetAttr {
        mode: Some(0o640),
        ..SetAttr::default()
    });
    step("chmod");
    set(SetAttr {
        uid: Some(7),
        gid: Some(8),
        ..SetAttr::default()
    });
    step("chown");
    set(SetAttr {
        atime: Some(TimeSet::At(1_000_000_000)),
        mtime: Some(TimeSet::At(2_000_000_000)),
        ..SetAttr::default()
    });
    step("utimens");
    set(SetAttr {
        size: Some(3),
        ..SetAttr::default()
    });
    step("truncate");
    c.setxattr(f, "user.k", b"v", SetXattrFlags::default())
        .unwrap();
    step("setxattr");
    let o = c.open(f, OpenFlags::READ | OpenFlags::WRITE).unwrap();
    c.write(f, o.fh, 0, b"abcdefgh").unwrap();
    step("a write with a session pending");
    c.close(f, o.fh).unwrap();
    step("its close");
    // Inside the child directory: its times and link count change.
    c.put(sub, "inner", b"i");
    step("a create in a child directory");
    c.mkdir(sub, "innerdir").unwrap();
    step("a mkdir in a child directory");
    c.unlink(sub, "inner").unwrap();
    step("an unlink in a child directory");
    // Names: the form transitions of plan 45 and renames.
    Blocking::run(|r| c.view.link(&c.cx(OpKind::Link), f, d, Name::new("f2"), r)).unwrap();
    step("a link");
    set(SetAttr {
        mode: Some(0o600),
        ..SetAttr::default()
    });
    step("a chmod of a linked file");
    c.unlink(d, "f2").unwrap();
    step("an unlink back to one name");
    c.rename(d, "f", d, "renamed").unwrap();
    step("a rename");
    c.rename(d, "sub", d, "sub2").unwrap();
    step("a rename of a directory");
    c.rename(d, "other", d, "renamed").unwrap();
    step("a rename over a name");
}
