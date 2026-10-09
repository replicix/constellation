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

    fn rename(&self, parent: Ino, name: &str, new_parent: Ino, new_name: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.view.rename(
                &self.cx(OpKind::Rename),
                parent,
                Name::new(name),
                new_parent,
                Name::new(new_name),
                RenameFlags::empty(),
                r,
            )
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
        assert_eq!(code(c.getattr(ino)), Code::NotFound);
    }
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
                r,
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
