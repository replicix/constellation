//! `namespace`: create, lookup, mkdir, rmdir, unlink, rename (with
//! `NOREPLACE`/`EXCHANGE`), hard links, symlinks, special files, name
//! limits and the refusals (`EEXIST`, `ENOENT`, `ENOTEMPTY`, `ENOTDIR`,
//! `EISDIR`, `EXDEV` lives in `confinement`).

use super::oracle::Driver;
use super::{must, refused, refused_any, skip, Env, TestResult};
use crate::types::{mode, FileKind, RenameFlags};
use constellation_types::{Code, Rdev};

pub(super) fn create_lookup_getattr(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let (entry, opened) = must("create f", c.create(root, "f"));
    let ino = entry.attr.ino;
    assert_eq!(entry.attr.kind, FileKind::File);
    assert_eq!((entry.attr.size, entry.attr.nlink), (0, 1));
    assert_eq!(
        entry.attr.mode & 0o7777,
        0o644,
        "the creation mode's permission bits"
    );
    assert_eq!(must("write", c.write(ino, opened.fh, 0, b"hello")), 5);
    must("close", c.close(ino, opened.fh));
    let looked = must("lookup f", c.lookup(root, "f"));
    assert_eq!(looked.attr.ino, ino, "lookup names the created inode");
    let attr = must("getattr f", c.getattr(ino));
    assert_eq!((attr.ino, attr.kind, attr.size), (ino, FileKind::File, 5));
    assert_eq!(must("getattr root", c.getattr(root)).kind, FileKind::Dir);
    Ok(())
}

pub(super) fn entries_carry_the_callers_identity(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let root = fx.client().root();
    for (uid, gid) in [(1000, 1000), (0, 0), (7, 8)] {
        let c = fx.client().as_user(uid, gid);
        let d = must("mkdir", c.mkdir(root, &format!("d{uid}")));
        assert_eq!((d.attr.uid, d.attr.gid), (uid, gid), "mkdir owner");
        let (f, o) = must("create", c.create(root, &format!("f{uid}")));
        assert_eq!((f.attr.uid, f.attr.gid), (uid, gid), "create owner");
        must("close", c.close(f.attr.ino, o.fh));
        let s = must("symlink", c.symlink(root, &format!("s{uid}"), "t"));
        assert_eq!((s.attr.uid, s.attr.gid), (uid, gid), "symlink owner");
    }
    Ok(())
}

pub(super) fn mkdir_rmdir_and_their_refusals(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    assert_eq!(
        must("lookup d", c.lookup(root, "d")).attr.kind,
        FileKind::Dir
    );
    refused("mkdir d again", c.mkdir(root, "d"), Code::Exists);
    must("rmdir d", c.rmdir(root, "d"));
    refused("lookup d", c.lookup(root, "d"), Code::NotFound);
    refused("rmdir d again", c.rmdir(root, "d"), Code::NotFound);
    refused(
        "getattr of the removed directory",
        c.getattr(d),
        Code::NotFound,
    );
    // Not empty, not a directory.
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    let f = must("put d/f", c.put(d, "f", b"x"));
    refused(
        "rmdir of a non-empty directory",
        c.rmdir(root, "d"),
        Code::NotEmpty,
    );
    let file = must("put file", c.put(root, "file", b"y"));
    refused("rmdir of a file", c.rmdir(root, "file"), Code::NotDir);
    refused(
        "mkdir beneath a file",
        c.mkdir(file.attr.ino, "x"),
        Code::NotDir,
    );
    must("unlink d/f", c.unlink(d, "f"));
    must("rmdir emptied d", c.rmdir(root, "d"));
    let _ = f;
    Ok(())
}

pub(super) fn unlink_and_its_refusals(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    must("put f", c.put(root, "f", b"data"));
    must("unlink f", c.unlink(root, "f"));
    refused("lookup f", c.lookup(root, "f"), Code::NotFound);
    refused("unlink f again", c.unlink(root, "f"), Code::NotFound);
    // The name is reusable, and the new file is empty.
    let again = must("put f again", c.put(root, "f", b""));
    assert_eq!(again.attr.size, 0);
    // A directory is not unlinked (Linux: EISDIR; BSD: EPERM).
    must("mkdir d", c.mkdir(root, "d"));
    refused_any(
        "unlink of a directory",
        c.unlink(root, "d"),
        &[Code::IsDir, Code::Perm],
    );
    refused(
        "unlink beneath a file",
        c.unlink(again.attr.ino, "x"),
        Code::NotDir,
    );
    assert!(c.lookup(root, "d").is_ok(), "the directory survived");
    Ok(())
}

pub(super) fn lookup_refusals(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    refused(
        "lookup of a missing name",
        c.lookup(root, "nope"),
        Code::NotFound,
    );
    let f = must("put f", c.put(root, "f", b"x"));
    refused(
        "lookup beneath a file",
        c.lookup(f.attr.ino, "x"),
        Code::NotDir,
    );
    refused(
        "lookup of a name past NAME_MAX",
        c.lookup(root, &"n".repeat(256)),
        Code::NameTooLong,
    );
    Ok(())
}

pub(super) fn create_without_excl_opens_what_is_there(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let first = must("put f", c.put(root, "f", b"keep"));
    // `open(O_CREAT)` without `O_EXCL` on an existing name opens it (a race
    // another node won); nothing is truncated without `O_TRUNC`.
    let (e, o) = must("create f again", c.create(root, "f"));
    assert_eq!(e.attr.ino, first.attr.ino, "the same file");
    assert_eq!(e.attr.size, 4, "not truncated");
    assert_eq!(must("read", c.read(e.attr.ino, o.fh, 0, 16)), b"keep");
    must("close", c.close(e.attr.ino, o.fh));
    refused("create f with EXCL", c.create_excl(root, "f"), Code::Exists);
    must("mkdir d", c.mkdir(root, "d"));
    refused("create over a directory", c.create(root, "d"), Code::IsDir);
    Ok(())
}

pub(super) fn rename_within_and_across_directories(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let a = must("put a", c.put(root, "a", b"payload"));
    must("rename a b", c.rename(root, "a", root, "b"));
    refused("lookup a", c.lookup(root, "a"), Code::NotFound);
    let b = must("lookup b", c.lookup(root, "b"));
    assert_eq!(b.attr.ino, a.attr.ino, "a rename keeps the inode");
    assert_eq!(must("slurp", c.slurp(b.attr.ino)), b"payload");
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    must("rename b d/c", c.rename(root, "b", d, "c"));
    assert_eq!(must("lookup d/c", c.lookup(d, "c")).attr.ino, a.attr.ino);
    assert_eq!(must("names root", c.names(root)), ["d"]);
    assert_eq!(must("names d", c.names(d)), ["c"]);
    // A directory moves with its content.
    let e = must("mkdir e", c.mkdir(root, "e")).attr.ino;
    must("put e/x", c.put(e, "x", b"1"));
    must("rename e d/e2", c.rename(root, "e", d, "e2"));
    let moved = must("lookup d/e2", c.lookup(d, "e2"));
    assert_eq!(moved.attr.ino, e);
    assert_eq!(must("names d/e2", c.names(e)), ["x"]);
    Ok(())
}

pub(super) fn rename_over_existing_targets(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    // file over file: the target's name now names the source.
    let a = must("put a", c.put(root, "a", b"A"));
    must("put b", c.put(root, "b", b"BB"));
    must("rename a b", c.rename(root, "a", root, "b"));
    let b = must("lookup b", c.lookup(root, "b"));
    assert_eq!(b.attr.ino, a.attr.ino);
    assert_eq!(must("slurp", c.slurp(b.attr.ino)), b"A");
    assert_eq!(must("names", c.names(root)), ["b"]);
    // directory over an empty directory.
    let d1 = must("mkdir d1", c.mkdir(root, "d1")).attr.ino;
    must("mkdir d2", c.mkdir(root, "d2"));
    must("put d1/x", c.put(d1, "x", b"x"));
    must("rename d1 d2", c.rename(root, "d1", root, "d2"));
    assert_eq!(must("lookup d2", c.lookup(root, "d2")).attr.ino, d1);
    assert_eq!(must("names d2", c.names(d1)), ["x"]);
    // The refusals.
    let f = must("put f", c.put(root, "f", b"f"));
    let full = must("mkdir full", c.mkdir(root, "full")).attr.ino;
    must("put full/y", c.put(full, "y", b"y"));
    must("mkdir empty", c.mkdir(root, "empty"));
    refused(
        "file over directory",
        c.rename(root, "f", root, "empty"),
        Code::IsDir,
    );
    refused(
        "directory over file",
        c.rename(root, "empty", root, "f"),
        Code::NotDir,
    );
    refused(
        "directory over non-empty directory",
        c.rename(root, "d2", root, "full"),
        Code::NotEmpty,
    );
    // Nothing moved.
    assert_eq!(must("lookup f", c.lookup(root, "f")).attr.ino, f.attr.ino);
    assert_eq!(must("names full", c.names(full)), ["y"]);
    // Renaming a name to itself is a no-op.
    must("rename f f", c.rename(root, "f", root, "f"));
    assert_eq!(must("lookup f", c.lookup(root, "f")).attr.ino, f.attr.ino);
    Ok(())
}

pub(super) fn rename_refusals(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    refused(
        "rename of a missing name",
        c.rename(root, "nope", root, "x"),
        Code::NotFound,
    );
    let a = must("mkdir a", c.mkdir(root, "a")).attr.ino;
    let b = must("mkdir a/b", c.mkdir(a, "b")).attr.ino;
    refused(
        "a directory into its own descendant",
        c.rename(root, "a", b, "x"),
        Code::Invalid,
    );
    refused(
        "a directory into itself",
        c.rename(root, "a", a, "x"),
        Code::Invalid,
    );
    let f = must("put f", c.put(root, "f", b"f"));
    refused(
        "rename into a file as the new parent",
        c.rename(root, "a", f.attr.ino, "x"),
        Code::NotDir,
    );
    let long = "x".repeat(256);
    refused(
        "rename to a name past NAME_MAX",
        c.rename(root, "f", root, &long),
        Code::NameTooLong,
    );
    assert!(
        c.lookup(root, "a").is_ok() && c.lookup(root, "f").is_ok(),
        "nothing moved"
    );
    Ok(())
}

pub(super) fn rename_noreplace(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.declared.rename_flags {
        skip!("the target does not honour RENAME_NOREPLACE (declared rename-flags gap)");
    }
    let c = fx.client();
    let root = c.root();
    let a = must("put a", c.put(root, "a", b"A"));
    let b = must("put b", c.put(root, "b", b"B"));
    refused(
        "NOREPLACE onto an existing name",
        c.rename_flags(root, "a", root, "b", RenameFlags::NOREPLACE),
        Code::Exists,
    );
    assert_eq!(must("lookup a", c.lookup(root, "a")).attr.ino, a.attr.ino);
    assert_eq!(must("lookup b", c.lookup(root, "b")).attr.ino, b.attr.ino);
    must(
        "NOREPLACE onto a free name",
        c.rename_flags(root, "a", root, "c", RenameFlags::NOREPLACE),
    );
    assert_eq!(must("lookup c", c.lookup(root, "c")).attr.ino, a.attr.ino);
    refused("lookup a", c.lookup(root, "a"), Code::NotFound);
    Ok(())
}

pub(super) fn rename_exchange(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.declared.rename_flags {
        skip!("the target does not honour RENAME_EXCHANGE (declared rename-flags gap)");
    }
    let c = fx.client();
    let root = c.root();
    let a = must("put a", c.put(root, "a", b"A"));
    let b = must("put b", c.put(root, "b", b"B"));
    must(
        "EXCHANGE a b",
        c.rename_flags(root, "a", root, "b", RenameFlags::EXCHANGE),
    );
    assert_eq!(must("lookup a", c.lookup(root, "a")).attr.ino, b.attr.ino);
    assert_eq!(must("lookup b", c.lookup(root, "b")).attr.ino, a.attr.ino);
    assert_eq!(must("slurp a", c.slurp(b.attr.ino)), b"B");
    refused(
        "EXCHANGE with a missing name",
        c.rename_flags(root, "a", root, "zz", RenameFlags::EXCHANGE),
        Code::NotFound,
    );
    // A file and a directory may be exchanged.
    let d = must("mkdir d", c.mkdir(root, "d"));
    must(
        "EXCHANGE a d",
        c.rename_flags(root, "a", root, "d", RenameFlags::EXCHANGE),
    );
    assert_eq!(
        must("lookup a", c.lookup(root, "a")).attr.kind,
        FileKind::Dir
    );
    assert_eq!(must("lookup a", c.lookup(root, "a")).attr.ino, d.attr.ino);
    assert_eq!(
        must("lookup d", c.lookup(root, "d")).attr.kind,
        FileKind::File
    );
    refused(
        "NOREPLACE and EXCHANGE together",
        c.rename_flags(
            root,
            "a",
            root,
            "b",
            RenameFlags::NOREPLACE | RenameFlags::EXCHANGE,
        ),
        Code::Invalid,
    );
    Ok(())
}

pub(super) fn hard_links_count_names(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let f = must("put f", c.put(root, "f", b"shared"));
    let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
    let l1 = must("link f d/l", c.link(f.attr.ino, d, "l"));
    assert_eq!((l1.attr.ino, l1.attr.nlink), (f.attr.ino, 2));
    let l2 = must("link f l2", c.link(f.attr.ino, root, "l2"));
    assert_eq!(l2.attr.nlink, 3);
    assert_eq!(must("getattr", c.getattr(f.attr.ino)).nlink, 3);
    // One inode, three names: a write through one shows through the rest.
    let o = must("open d/l", c.open_rw(f.attr.ino));
    must("write", c.write(f.attr.ino, o.fh, 0, b"SHARED"));
    must("close", c.close(f.attr.ino, o.fh));
    assert_eq!(
        must("slurp", c.slurp(must("lookup", c.lookup(d, "l")).attr.ino)),
        b"SHARED"
    );
    must("unlink f", c.unlink(root, "f"));
    assert_eq!(must("getattr", c.getattr(f.attr.ino)).nlink, 2);
    must("unlink l2", c.unlink(root, "l2"));
    must("unlink d/l", c.unlink(d, "l"));
    refused(
        "getattr of the last unlinked name",
        c.getattr(f.attr.ino),
        Code::NotFound,
    );
    Ok(())
}

pub(super) fn hard_link_refusals(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let f = must("put f", c.put(root, "f", b"x"));
    must("put g", c.put(root, "g", b"y"));
    refused(
        "link onto an existing name",
        c.link(f.attr.ino, root, "g"),
        Code::Exists,
    );
    let d = must("mkdir d", c.mkdir(root, "d"));
    // POSIX: EPERM; the engine's replica says EISDIR.
    refused_any(
        "hard link to a directory",
        c.link(d.attr.ino, root, "dl"),
        &[Code::Perm, Code::IsDir],
    );
    refused(
        "link beneath a file",
        c.link(f.attr.ino, f.attr.ino, "x"),
        Code::NotDir,
    );
    refused(
        "link to a name past NAME_MAX",
        c.link(f.attr.ino, root, &"l".repeat(256)),
        Code::NameTooLong,
    );
    assert_eq!(
        must("getattr", c.getattr(f.attr.ino)).nlink,
        1,
        "nothing was linked"
    );
    Ok(())
}

pub(super) fn symlink_and_readlink(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let s = must("symlink", c.symlink(root, "s", "some/target"));
    assert_eq!(s.attr.kind, FileKind::Symlink);
    assert_eq!(
        s.attr.size,
        "some/target".len() as u64,
        "the size of a symlink is its target's length"
    );
    assert_eq!(must("readlink", c.readlink(s.attr.ino)), b"some/target");
    assert_eq!(must("lookup s", c.lookup(root, "s")).attr.ino, s.attr.ino);
    refused(
        "symlink over an existing name",
        c.symlink(root, "s", "x"),
        Code::Exists,
    );
    let f = must("put f", c.put(root, "f", b"x"));
    refused("readlink of a file", c.readlink(f.attr.ino), Code::Invalid);
    // A dangling target is fine; a symlink is unlinked, not followed.
    must("unlink s", c.unlink(root, "s"));
    refused("lookup s", c.lookup(root, "s"), Code::NotFound);
    Ok(())
}

pub(super) fn special_files(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let fifo = must(
        "mknod fifo",
        c.mknod(root, "p", mode::S_IFIFO | 0o644, Rdev::default()),
    );
    assert_eq!(fifo.attr.kind, FileKind::Fifo);
    assert_eq!(fifo.attr.mode & 0o7777, 0o644);
    let sock = must(
        "mknod socket",
        c.mknod(root, "s", mode::S_IFSOCK | 0o600, Rdev::default()),
    );
    assert_eq!(sock.attr.kind, FileKind::Socket);
    let dev = must(
        "mknod chardev",
        c.mknod(root, "null", mode::S_IFCHR | 0o666, Rdev::new(1, 3)),
    );
    assert_eq!(dev.attr.kind, FileKind::CharDev);
    assert_eq!(
        dev.attr.rdev,
        Rdev::new(1, 3),
        "the device number is portable (major, minor)"
    );
    let blk = must(
        "mknod blockdev",
        c.mknod(root, "sda1", mode::S_IFBLK | 0o660, Rdev::new(8, 1)),
    );
    assert_eq!(
        (blk.attr.kind, blk.attr.rdev),
        (FileKind::BlockDev, Rdev::new(8, 1))
    );
    assert_eq!(
        must("lookup", c.lookup(root, "null")).attr.rdev,
        Rdev::new(1, 3)
    );
    refused(
        "mknod of a directory",
        c.mknod(root, "dir", mode::S_IFDIR | 0o755, Rdev::default()),
        Code::Invalid,
    );
    refused(
        "mknod over an existing name",
        c.mknod(root, "p", mode::S_IFIFO | 0o644, Rdev::default()),
        Code::Exists,
    );
    must("unlink fifo", c.unlink(root, "p"));
    Ok(())
}

pub(super) fn name_length_limits(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let ok = "y".repeat(255);
    must("mkdir of a 255-byte name", c.mkdir(root, &ok));
    must("lookup of it", c.lookup(root, &ok));
    let long = "x".repeat(256);
    // Refused before anything else, by every name-taking op.
    refused("lookup", c.lookup(root, &long), Code::NameTooLong);
    refused("mkdir", c.mkdir(root, &long), Code::NameTooLong);
    refused("create", c.create(root, &long), Code::NameTooLong);
    refused("symlink", c.symlink(root, &long, "t"), Code::NameTooLong);
    refused("unlink", c.unlink(root, &long), Code::NameTooLong);
    refused("rmdir", c.rmdir(root, &long), Code::NameTooLong);
    refused(
        "rename to",
        c.rename(root, &ok, root, &long),
        Code::NameTooLong,
    );
    if fx.caps.special_files {
        refused(
            "mknod",
            c.mknod(root, &long, mode::S_IFIFO | 0o644, Rdev::default()),
            Code::NameTooLong,
        );
    }
    // The limit is on the stored (UTF-8) form: 128 two-byte characters
    // are 256 bytes.
    let wide = "é".repeat(128);
    assert_eq!(wide.len(), 256);
    refused(
        "mkdir of 256 bytes of UTF-8",
        c.mkdir(root, &wide),
        Code::NameTooLong,
    );
    must(
        "mkdir of 254 bytes of UTF-8",
        c.mkdir(root, &"é".repeat(127)),
    );
    Ok(())
}

pub(super) fn unlinked_open_file_stays_usable(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let (e, o) = must("create", c.create(root, "f"));
    let ino = e.attr.ino;
    must("write", c.write(ino, o.fh, 0, b"still here"));
    must("unlink", c.unlink(root, "f"));
    refused("lookup", c.lookup(root, "f"), Code::NotFound);
    // The handle keeps the inode alive: I/O through it works.
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"still here");
    must("write more", c.write(ino, o.fh, 10, b"!"));
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"still here!");
    assert_eq!(must("getattr", c.getattr_fh(ino, Some(o.fh))).size, 11);
    must("close", c.close(ino, o.fh));
    // The name is free again and is a different file.
    let again = must("put f", c.put(root, "f", b"new"));
    assert_ne!(again.attr.ino, ino);
    Ok(())
}

pub(super) fn model_replay_sequential(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let mut rng = env.rng();
    let base = must("mkdir work", c.mkdir(c.root(), "work")).attr.ino;
    let mut driver = Driver::new(&c, base, env.caps().hard_links);
    let steps = 400;
    for i in 0..steps {
        if let Err(e) = driver.step(&mut rng) {
            super::fail!("step {i} (seed {:#x}): {e}", env.seed());
        }
        if i % 100 == 99 {
            if let Err(e) = driver.verify_tree() {
                super::fail!("after step {i} (seed {:#x}): {e}", env.seed());
            }
        }
    }
    if let Err(e) = driver.verify_tree() {
        super::fail!("after {steps} steps (seed {:#x}): {e}", env.seed());
    }
    Ok(())
}
