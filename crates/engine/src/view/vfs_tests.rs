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
            .map(|data| data.contiguous().into_owned())
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
    assert_eq!(opened.fh, Fh(ino));
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
    assert_eq!(c.read(ino, opened.fh, 0, 64).unwrap(), b"hello world");
    assert_eq!(c.read(ino, opened.fh, 64, 8).unwrap(), b"", "past the end");
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
fn xattrs_round_trip_and_the_virtual_ones_are_listed_but_read_only() {
    let c = client();
    let d = c.mkdir(ROOT_INO, "d").unwrap().attr.ino;
    let (e, o) = c.create(d, "f").unwrap();
    c.write(e.attr.ino, o.fh, 0, b"12345").unwrap();
    c.close(e.attr.ino, o.fh).unwrap();
    let f = e.attr.ino;
    c.setxattr(f, "user.color", b"blue", SetXattrFlags::empty())
        .unwrap();
    assert_eq!(c.getxattr(f, "user.color").unwrap(), b"blue");
    assert_eq!(
        c.listxattr(f).unwrap(),
        [
            "user.color",
            "user.constellation.rcount",
            "user.constellation.rsize"
        ]
    );
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
    assert_eq!(
        code(c.setxattr(f, "security.x", b"1", SetXattrFlags::empty())),
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
    assert_eq!(
        c.listxattr(f).unwrap(),
        ["user.constellation.rcount", "user.constellation.rsize"]
    );
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
