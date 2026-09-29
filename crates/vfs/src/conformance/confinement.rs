//! `confinement` (plan 31 §6.12): a view rooted at a subtree reaches
//! nothing above it.
//!
//! - `..` at the view's root is the root.
//! - An inode the view's root does not dominate — by name, by a stale or
//!   replayed number, through a handle — is refused with `Code::Stale`.
//! - `.constellation/snapshot/<name>/...` mirrors history at the same
//!   relative path under the *view's own root*, never a sibling's.
//! - With `ViewSpec::confine_links`, `link()` across a link domain (the
//!   view's root, or a directory carrying the root-only
//!   [`super::LINK_DOMAIN_XATTR`] marker) is `EXDEV`, and only then.
//!
//! The tests build the layout below on the whole-tree fixture and open
//! views of it through the target's subtree hook:
//!
//! ```text
//! /top                  file "top"
//! /vol/inside/f         file "inside-f"
//! /vol/inside/sub/g     file "g"
//! /vol/outside/secret   file "s3cret"
//! ```

use super::{must, refused, refused_any, Client, Env, Fx, TestResult, LINK_DOMAIN_XATTR};
use crate::types::{Fh, FileKind, Ino, OpenFlags, SetAttr, SetXattrFlags};
use crate::LockOwner;
use constellation_types::Code;

struct Layout {
    top: Ino,
    vol: Ino,
    inside: Ino,
    f: Ino,
    sub: Ino,
    outside: Ino,
    secret: Ino,
}

fn layout(fx: &Fx) -> Layout {
    let c = fx.client();
    let root = c.root();
    let top = must("put top", c.put(root, "top", b"top")).attr.ino;
    let vol = must("mkdir vol", c.mkdir(root, "vol")).attr.ino;
    let inside = must("mkdir inside", c.mkdir(vol, "inside")).attr.ino;
    let f = must("put f", c.put(inside, "f", b"inside-f")).attr.ino;
    let sub = must("mkdir sub", c.mkdir(inside, "sub")).attr.ino;
    must("put g", c.put(sub, "g", b"g"));
    let outside = must("mkdir outside", c.mkdir(vol, "outside")).attr.ino;
    let secret = must("put secret", c.put(outside, "secret", b"s3cret"))
        .attr
        .ino;
    Layout {
        top,
        vol,
        inside,
        f,
        sub,
        outside,
        secret,
    }
}

pub(super) fn dotdot_at_the_view_root_is_the_root(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    // The whole tree: `..` of the root is the root.
    let c = fx.client();
    let up = must("lookup ..", c.lookup(c.root(), ".."));
    assert_eq!((up.attr.ino, up.attr.kind), (c.root(), FileKind::Dir));
    // A subtree view: `..` of *its* root is its root, not `/vol`.
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    let r = v.root();
    let up = must("lookup .. at the view root", v.lookup(r, ".."));
    assert_eq!(up.attr.ino, r, "`..` at the view root stays at the root");
    assert_eq!(up.attr.kind, FileKind::Dir);
    assert_ne!(up.attr.ino, l.vol, "and is not the subtree's parent");
    let sub = must("lookup sub", v.lookup(r, "sub"));
    let up = must("lookup .. of sub", v.lookup(sub.attr.ino, ".."));
    assert_eq!(up.attr.ino, r, "`..` of a top-level directory is the root");
    // Through the root, the parent's names are not there.
    let names = must("names", v.names(up.attr.ino));
    assert_eq!(names, ["f", "sub"]);
    Ok(())
}

pub(super) fn the_view_root_is_the_root_inode(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    let r = v.root();
    let attr = must("getattr of the root", v.getattr(r));
    assert_eq!(
        (attr.ino, attr.kind),
        (r, FileKind::Dir),
        "the root answers as the root"
    );
    let entries = must("readdir", v.readdir_all(r, 10));
    let dot = entries
        .iter()
        .find(|e| e.name.as_bytes() == b".")
        .expect(".");
    let dotdot = entries
        .iter()
        .find(|e| e.name.as_bytes() == b"..")
        .expect("..");
    assert_eq!(
        (dot.ino, dotdot.ino),
        (r, r),
        "`.` and `..` of the root are the root"
    );
    assert_eq!(
        must("names", v.names(r)),
        ["f", "sub"],
        "only the subtree's own names"
    );
    // What the view calls an entry is addressable in the view, and is the
    // same inode as underneath.
    assert_eq!(must("lookup f", v.lookup(r, "f")).attr.ino, l.f);
    assert_eq!(must("slurp", v.slurp(l.f)), b"inside-f");
    let _ = (l.inside, l.sub);
    Ok(())
}

pub(super) fn inodes_outside_the_subtree_are_refused(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    let r = v.root();
    let caps = &fx.caps;
    for (what, ino) in [
        ("the parent directory", l.vol),
        ("a sibling directory", l.outside),
        ("a file in a sibling", l.secret),
        ("a file above", l.top),
    ] {
        let s = |op: &str| format!("{op} on {what}");
        refused(&s("getattr"), v.getattr(ino), Code::Stale);
        refused(&s("lookup"), v.lookup(ino, "secret"), Code::Stale);
        refused(&s("open"), v.open(ino, OpenFlags::READ), Code::Stale);
        refused(&s("readlink"), v.readlink(ino), Code::Stale);
        refused(
            &s("readdir"),
            v.readdir_page(ino, 0, 10, false),
            Code::Stale,
        );
        refused(
            &s("setattr"),
            v.setattr(
                ino,
                None,
                &SetAttr {
                    mode: Some(0o777),
                    ..SetAttr::default()
                },
            ),
            Code::Stale,
        );
        refused(&s("mkdir"), v.mkdir(ino, "made"), Code::Stale);
        refused(&s("create"), v.create(ino, "made"), Code::Stale);
        refused(&s("symlink"), v.symlink(ino, "made", "t"), Code::Stale);
        refused(&s("unlink"), v.unlink(ino, "secret"), Code::Stale);
        refused(&s("rmdir"), v.rmdir(ino, "outside"), Code::Stale);
        refused(
            &s("rename from"),
            v.rename(ino, "secret", r, "stolen"),
            Code::Stale,
        );
        refused(&s("rename to"), v.rename(r, "f", ino, "moved"), Code::Stale);
        refused(&s("read"), v.read(ino, Fh(1), 0, 8), Code::Stale);
        refused(&s("statfs"), v.statfs(ino), Code::Stale);
        if caps.hard_links {
            refused(&s("link from"), v.link(ino, r, "linked"), Code::Stale);
            refused(&s("link to"), v.link(l.f, ino, "linked"), Code::Stale);
        }
        if caps.xattrs != crate::XattrSupport::None {
            refused(&s("getxattr"), v.getxattr(ino, "user.a"), Code::Stale);
            refused(&s("listxattr"), v.listxattr(ino), Code::Stale);
            refused(
                &s("setxattr"),
                v.setxattr(ino, "user.a", b"1", SetXattrFlags::empty()),
                Code::Stale,
            );
            refused(&s("removexattr"), v.removexattr(ino, "user.a"), Code::Stale);
        }
    }
    // A handle of the view's own does not open a door to another inode.
    let mine = must("open f", v.open_rw(l.f));
    refused(
        "read of another inode through a valid handle",
        v.read(l.secret, mine.fh, 0, 8),
        Code::Stale,
    );
    refused(
        "write of another inode through a valid handle",
        v.write(l.secret, mine.fh, 0, b"pwn"),
        Code::Stale,
    );
    refused(
        "fsync of another inode",
        v.fsync(l.secret, mine.fh, crate::Durability::Configured),
        Code::Stale,
    );
    refused(
        "flush of another inode",
        v.flush(l.secret, mine.fh, LockOwner(1)),
        Code::Stale,
    );
    must("close f", v.close(l.f, mine.fh));
    // Nothing above changed.
    let c = fx.client();
    assert_eq!(must("names outside", c.names(l.outside)), ["secret"]);
    assert_eq!(must("secret", c.slurp(l.secret)), b"s3cret");
    assert_eq!(must("names inside", c.names(l.inside)), ["f", "sub"]);
    assert_eq!(must("names vol", c.names(l.vol)), ["inside", "outside"]);
    Ok(())
}

pub(super) fn forged_inodes_and_handles_are_refused(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    // A number that names nothing, and one that named something once.
    let (gone, _) = {
        let c = fx.client();
        let e = must("put gone", c.put(l.outside, "gone", b"x"));
        must("unlink gone", c.unlink(l.outside, "gone"));
        (e.attr.ino, ())
    };
    for ino in [u64::MAX - 5, 1 << 40, gone] {
        refused_any(
            &format!("getattr of {ino}"),
            v.getattr(ino),
            &[Code::Stale, Code::NotFound],
        );
        refused_any(
            &format!("lookup in {ino}"),
            v.lookup(ino, "x"),
            &[Code::Stale, Code::NotFound],
        );
        refused_any(
            &format!("read of {ino}"),
            v.read(ino, Fh(9_999_999), 0, 8),
            &[Code::Stale, Code::NotFound, Code::BadFd],
        );
    }
    // A handle number nobody was given, on an inode of the view.
    refused_any(
        "read through a handle nobody was given",
        v.read(l.f, Fh(9_999_999), 0, 8),
        &[Code::BadFd, Code::Stale, Code::NotFound, Code::Invalid],
    );
    Ok(())
}

pub(super) fn an_open_file_stays_addressable_when_unlinked(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    layout(&fx);
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    let (e, o) = must("create tmp", v.create(v.root(), "tmp"));
    let ino = e.attr.ino;
    must("write", v.write(ino, o.fh, 0, b"scratch"));
    must("unlink", v.unlink(v.root(), "tmp"));
    // The inode has no name left to walk from, but the view holds it open.
    assert_eq!(must("getattr", v.getattr(ino)).size, 7);
    assert_eq!(must("read", v.read(ino, o.fh, 0, 16)), b"scratch");
    must("close", v.close(ino, o.fh));
    Ok(())
}

/// The snapshot names listed under `dir/.constellation/snapshot`.
fn snapshots_of(c: &Client, dir: Ino) -> (Ino, Vec<String>) {
    let meta = must("lookup .constellation", c.lookup(dir, ".constellation"));
    assert_eq!(meta.attr.kind, FileKind::Dir);
    let snap = must("lookup snapshot", c.lookup(meta.attr.ino, "snapshot"));
    assert_eq!(snap.attr.kind, FileKind::Dir);
    (snap.attr.ino, must("names", c.names(snap.attr.ino)))
}

pub(super) fn constellation_snapshots_stay_inside_the_subtree(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    fx.snapshot("/", "s-all")?;
    fx.snapshot("vol", "s-vol")?;
    fx.snapshot("vol/outside", "s-out")?;
    // History moves on after the snapshots.
    let c = fx.client();
    let o = must("open f", c.open_rw(l.f));
    must("rewrite f", c.write(l.f, o.fh, 0, b"CHANGED!"));
    must("close", c.close(l.f, o.fh));
    must("put later", c.put(l.inside, "later", b"new"));

    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    // Only the snapshots covering this view's own path: taken of it or of
    // an ancestor. The sibling's is not there.
    let (snap_dir, names) = snapshots_of(&v, v.root());
    assert_eq!(names, ["s-all", "s-vol"], "snapshots covering /vol/inside");
    let mirror = must("lookup s-all", v.lookup(snap_dir, "s-all"));
    assert_eq!(mirror.attr.kind, FileKind::Dir);
    // The mirror is the view's own directory as it was: same relative
    // paths, frozen content.
    assert_eq!(
        must("names of the mirror", v.names(mirror.attr.ino)),
        ["f", "sub"]
    );
    let mf = must("lookup mirror/f", v.lookup(mirror.attr.ino, "f"));
    let fh = must("open frozen f", v.open(mf.attr.ino, OpenFlags::READ));
    assert_eq!(
        must("read frozen f", v.read(mf.attr.ino, fh.fh, 0, 64)),
        b"inside-f"
    );
    must("release", v.release(mf.attr.ino, fh.fh));
    // Never a sibling volume's history or the root's content.
    for absent in ["outside", "vol", "inside", "top", "secret"] {
        refused(
            &format!("mirror/{absent}"),
            v.lookup(mirror.attr.ino, absent),
            Code::NotFound,
        );
    }
    refused(
        "the sibling's snapshot",
        v.lookup(snap_dir, "s-out"),
        Code::NotFound,
    );
    // Below the root the same holds (`sub` is covered by all three too).
    let (_, sub_names) = snapshots_of(&v, must("lookup sub", v.lookup(v.root(), "sub")).attr.ino);
    assert_eq!(sub_names, ["s-all", "s-vol"]);
    // From the whole tree, each directory lists what covers *its* path.
    let (_, root_names) = snapshots_of(&c, c.root());
    assert_eq!(root_names, ["s-all"]);
    let (_, outside_names) = snapshots_of(&c, l.outside);
    assert_eq!(outside_names, ["s-all", "s-out", "s-vol"]);
    Ok(())
}

pub(super) fn snapshot_mirrors_are_read_only(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    fx.snapshot("/", "s")?;
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    let (snap_dir, _) = snapshots_of(&v, v.root());
    let mirror = must("lookup s", v.lookup(snap_dir, "s"));
    let mf = must("lookup mirror/f", v.lookup(mirror.attr.ino, "f"));
    let meta = must(
        "lookup .constellation",
        v.lookup(v.root(), ".constellation"),
    );
    let ro = &[Code::ReadOnly, Code::Perm, Code::Access];
    refused(
        "mkdir in .constellation",
        v.mkdir(meta.attr.ino, "x"),
        Code::ReadOnly,
    );
    refused_any("mkdir in a mirror", v.mkdir(mirror.attr.ino, "x"), ro);
    refused_any("create in a mirror", v.create(mirror.attr.ino, "x"), ro);
    refused_any("unlink in a mirror", v.unlink(mirror.attr.ino, "f"), ro);
    refused_any(
        "rename in a mirror",
        v.rename(mirror.attr.ino, "f", mirror.attr.ino, "g"),
        ro,
    );
    refused_any(
        "setattr on a mirrored file",
        v.setattr(
            mf.attr.ino,
            None,
            &SetAttr {
                mode: Some(0o777),
                ..SetAttr::default()
            },
        ),
        ro,
    );
    refused_any(
        "open for writing",
        v.open(mf.attr.ino, OpenFlags::WRITE),
        ro,
    );
    if fx.caps.xattrs != crate::XattrSupport::None {
        refused(
            "setxattr on a synthetic directory",
            v.setxattr(meta.attr.ino, "user.a", b"1", SetXattrFlags::empty()),
            Code::ReadOnly,
        );
    }
    // Nothing changed underneath.
    assert_eq!(must("names", fx.client().names(l.inside)), ["f", "sub"]);
    assert_eq!(
        must("names of the mirror", v.names(mirror.attr.ino)),
        ["f", "sub"]
    );
    Ok(())
}

pub(super) fn links_are_free_without_confine_links(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    let c = fx.client();
    // Across what would be domains, on the whole tree: a plain POSIX link.
    let linked = must("link f into outside", c.link(l.f, l.outside, "f2"));
    assert_eq!((linked.attr.ino, linked.attr.nlink), (l.f, 2));
    // In a subtree view without confine_links: links inside the view work;
    // an inode from outside is refused by confinement (Stale), not EXDEV.
    let view = fx.subtree("vol/inside", false)?;
    let v = view.client();
    must("link inside the view", v.link(l.f, l.sub, "again"));
    refused(
        "link of an outside inode",
        v.link(l.secret, v.root(), "s"),
        Code::Stale,
    );
    assert_eq!(must("getattr", c.getattr(l.f)).nlink, 3);
    Ok(())
}

/// Mark `dir` as a link domain (root only).
fn mark(fx: &Fx, dir: Ino) {
    must(
        "mark a link domain",
        fx.root_client()
            .setxattr(dir, LINK_DOMAIN_XATTR, b"1", SetXattrFlags::empty()),
    );
}

pub(super) fn confine_links_refuses_links_across_domains(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    // Two volumes, each a link domain, in a pool at the root.
    mark(&fx, l.inside);
    mark(&fx, l.outside);
    let plain = must(
        "mkdir plain",
        fx.client().mkdir(fx.client().root(), "plain"),
    )
    .attr
    .ino;
    let view = fx.subtree("/", true)?;
    let v = view.client();
    // Within a domain, anywhere below its marker.
    let within = must("link f into inside/sub", v.link(l.f, l.sub, "l"));
    assert_eq!(within.attr.nlink, 2);
    // Across domains: EXDEV, and only when the view says so.
    refused(
        "inside -> outside",
        v.link(l.f, l.outside, "l"),
        Code::CrossDevice,
    );
    refused(
        "outside -> inside",
        v.link(l.secret, l.inside, "l"),
        Code::CrossDevice,
    );
    // The unmarked root area is a domain of its own (the root's).
    refused(
        "inside -> root area",
        v.link(l.f, plain, "l"),
        Code::CrossDevice,
    );
    refused(
        "root area -> inside",
        v.link(l.top, l.inside, "l"),
        Code::CrossDevice,
    );
    let root_ok = must("link top within the root domain", v.link(l.top, plain, "t"));
    assert_eq!(root_ok.attr.nlink, 2);
    // Nothing else was linked.
    let c = fx.client();
    assert_eq!(must("names outside", c.names(l.outside)), ["secret"]);
    assert_eq!(must("names inside", c.names(l.inside)), ["f", "sub"]);
    // The very same links on a view without confine_links succeed.
    let free = fx.subtree("/", false)?;
    let fc = free.client();
    must(
        "inside -> outside without confine_links",
        fc.link(l.f, l.outside, "l"),
    );
    // A marker deeper down starts a domain of its own within a view: a file
    // named only in the parent domain cannot get a name in it.
    let h = must("put h", fx.client().put(l.inside, "h", b"h")).attr.ino;
    mark(&fx, l.sub);
    refused(
        "inside -> a nested domain",
        v.link(h, l.sub, "l2"),
        Code::CrossDevice,
    );
    // A subtree view is its own domain at its root.
    let inner = fx.subtree("vol/inside", true)?;
    let ic = inner.client();
    must(
        "link in the view root's domain",
        ic.link(h, ic.root(), "top-level"),
    );
    refused(
        "into a nested domain",
        ic.link(h, l.sub, "x"),
        Code::CrossDevice,
    );
    refused(
        "an outside inode",
        ic.link(l.secret, ic.root(), "s"),
        Code::Stale,
    );
    Ok(())
}

pub(super) fn confine_links_refuses_renames_of_linked_files_across_domains(
    env: &Env<'_>,
) -> TestResult {
    let fx = env.fresh();
    let l = layout(&fx);
    mark(&fx, l.inside);
    mark(&fx, l.outside);
    let view = fx.subtree("/", true)?;
    let v = view.client();
    // A file with two names cannot change domain by rename (`mv` falls back
    // to copy and unlink); a file with one can, and so can a directory.
    must("link", v.link(l.f, l.sub, "f-link"));
    refused(
        "rename of a multiply-linked file across domains",
        v.rename(l.inside, "f", l.outside, "f"),
        Code::CrossDevice,
    );
    assert_eq!(
        must("lookup", v.lookup(l.inside, "f")).attr.ino,
        l.f,
        "it did not move"
    );
    let single = must("put single", v.put(l.inside, "single", b"1")).attr.ino;
    must(
        "rename of a single-named file",
        v.rename(l.inside, "single", l.outside, "single"),
    );
    assert_eq!(
        must("lookup", v.lookup(l.outside, "single")).attr.ino,
        single
    );
    let dir = must("mkdir moved", v.mkdir(l.inside, "moved")).attr.ino;
    must(
        "rename of a directory",
        v.rename(l.inside, "moved", l.outside, "moved"),
    );
    assert_eq!(must("lookup", v.lookup(l.outside, "moved")).attr.ino, dir);
    // Within one domain a linked file renames freely.
    must(
        "rename within the domain",
        v.rename(l.inside, "f", l.sub, "f-moved"),
    );
    Ok(())
}
