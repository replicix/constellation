//! Subtree confinement (plan 31 §6.12), in process through the `Vfs`
//! trait: `..` at the view's root, handles outside the subtree,
//! `.constellation` under a subtree view, snapshot views, and
//! `confine_links` — the rules in the module doc of [`crate::view`].

use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::DEFAULT_CHUNK_SIZE;
use constellation_vfs::{
    Blocking, CollectDir, Fh, LockOwner, Name, OpCtx, OpKind, OpenOwner, Opened, RenameFlags,
    SetXattrFlags, Vfs, VfsResult, WriteData, XattrName,
};
use object_store::memory::InMemory;

fn caller() -> Caller {
    Caller::new(0, 0, None)
}

fn code<T: std::fmt::Debug>(r: VfsResult<T>) -> Code {
    r.expect_err("the op should have failed").code()
}

/// A view over `meta`, rooted at `root` (`None`: the whole filesystem).
fn view_at(meta: &Arc<Meta>, root: Option<&str>) -> (View, tempfile::TempDir) {
    let (mut view, dir) = super::quota_tests::test_fs(meta.clone());
    if let Some(root) = root {
        view.set_subtree_root(root).unwrap();
    }
    (view, dir)
}

fn lookup(v: &View, parent: Ino, name: &str) -> VfsResult<Entry> {
    let c = caller();
    Blocking::run(|r| v.lookup(&OpCtx::new(OpKind::Lookup, &c), parent, Name::new(name), r))
}

fn getattr(v: &View, ino: Ino) -> VfsResult<Attr> {
    let c = caller();
    Blocking::run(|r| v.getattr(&OpCtx::new(OpKind::Getattr, &c), ino, None, r))
}

fn open(v: &View, ino: Ino) -> VfsResult<Opened> {
    let c = caller();
    Blocking::run(|r| {
        v.open(
            &OpCtx::new(OpKind::Open, &c),
            ino,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    })
}

fn readlink(v: &View, ino: Ino) -> VfsResult<Vec<u8>> {
    let c = caller();
    Blocking::run(|r| v.readlink(&OpCtx::new(OpKind::Readlink, &c), ino, r))
}

fn readdir(v: &View, ino: Ino) -> VfsResult<Vec<String>> {
    let c = caller();
    let (sink, wait) = CollectDir::pair(1000);
    v.readdir(&OpCtx::new(OpKind::Readdir, &c), ino, Fh(0), 0, false, sink);
    wait.wait().map(|entries| {
        entries
            .into_iter()
            .map(|e| String::from_utf8(e.name.into_bytes()).unwrap())
            .filter(|n| n != "." && n != "..")
            .collect()
    })
}

fn link(v: &View, ino: Ino, parent: Ino, name: &str) -> VfsResult<Entry> {
    let c = caller();
    Blocking::run(|r| {
        v.link(
            &OpCtx::new(OpKind::Link, &c),
            ino,
            parent,
            Name::new(name),
            r,
        )
    })
}

fn rename(v: &View, parent: Ino, name: &str, new_parent: Ino, new_name: &str) -> VfsResult<()> {
    let c = caller();
    Blocking::run(|r| {
        v.rename(
            &OpCtx::new(OpKind::Rename, &c),
            parent,
            Name::new(name),
            new_parent,
            Name::new(new_name),
            RenameFlags::empty(),
            r,
        )
    })
}

#[test]
fn dotdot_at_the_view_root_is_the_root() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap();
    let b = meta.mkdir(a.ino, "b", 0o755, 0, 0).unwrap();
    meta.create(ROOT_INO, "sibling", 0o644, 0, 0).unwrap();
    let (v, _dir) = view_at(&meta, Some("/a"));
    // `..` and `.` of the view's root: the root, in the view's numbering.
    assert_eq!(lookup(&v, ROOT_INO, "..").unwrap().attr.ino, ROOT_INO);
    assert_eq!(lookup(&v, ROOT_INO, ".").unwrap().attr.ino, ROOT_INO);
    // `..` of a directory below it: its parent (here the root again).
    let b_entry = lookup(&v, ROOT_INO, "b").unwrap();
    assert_eq!(b_entry.attr.ino, b.ino);
    assert_eq!(lookup(&v, b.ino, "..").unwrap().attr.ino, ROOT_INO);
    assert_eq!(lookup(&v, b.ino, ".").unwrap().attr.ino, b.ino);
    // Never the subtree's real parent: nothing of `/` is reachable.
    assert_eq!(code(lookup(&v, ROOT_INO, "sibling")), Code::NotFound);
    assert_eq!(readdir(&v, ROOT_INO).unwrap(), ["b"]);
    // A whole-filesystem view: `..` of the root is the root too.
    let (whole, _dir2) = view_at(&meta, None);
    assert_eq!(lookup(&whole, ROOT_INO, "..").unwrap().attr.ino, ROOT_INO);
    assert_eq!(lookup(&whole, b.ino, "..").unwrap().attr.ino, a.ino);
}

#[test]
fn inodes_outside_the_subtree_are_stale_by_handle() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap();
    let deep = meta.mkdir(a.ino, "deep", 0o755, 0, 0).unwrap();
    let inside = meta.create(deep.ino, "inside", 0o644, 0, 0).unwrap();
    let out = meta.mkdir(ROOT_INO, "out", 0o755, 0, 0).unwrap();
    let secret = meta.create(out.ino, "secret", 0o644, 0, 0).unwrap();
    let sym = meta.symlink(out.ino, "sym", "secret", 0, 0).unwrap();
    let (v, _dir) = view_at(&meta, Some("/a"));

    // Forged, stale or replayed numbers of inodes the root does not
    // dominate: every addressing op answers ESTALE.
    assert_eq!(code(getattr(&v, secret.ino)), Code::Stale);
    assert_eq!(code(getattr(&v, out.ino)), Code::Stale);
    assert_eq!(code(lookup(&v, out.ino, "secret")), Code::Stale);
    assert_eq!(code(open(&v, secret.ino)), Code::Stale);
    assert_eq!(code(readlink(&v, sym.ino)), Code::Stale);
    assert_eq!(code(readdir(&v, out.ino)), Code::Stale);
    let c = caller();
    let read = Blocking::run(|r| {
        v.read(
            &OpCtx::new(OpKind::Read, &c),
            secret.ino,
            Fh(secret.ino),
            0,
            8,
            r,
        )
    });
    assert_eq!(code(read), Code::Stale);
    let write = Blocking::run(|r| {
        v.write(
            &OpCtx::new(OpKind::Write, &c),
            secret.ino,
            Fh(secret.ino),
            0,
            WriteData::Borrowed(b"x"),
            OpenFlags::WRITE,
            r,
        )
    });
    assert_eq!(code(write), Code::Stale);
    let setxattr = Blocking::run(|r| {
        v.setxattr(
            &OpCtx::new(OpKind::Setxattr, &c),
            secret.ino,
            XattrName::new("user.x"),
            b"1",
            SetXattrFlags::empty(),
            r,
        )
    });
    assert_eq!(code(setxattr), Code::Stale);
    assert_eq!(
        code(rename(&v, out.ino, "secret", ROOT_INO, "stolen")),
        Code::Stale
    );
    assert_eq!(meta.getattr(secret.ino).unwrap().unwrap().nlink, 1);

    // An inside inode this view never looked up (a handle from before a
    // restart): proven by the parent walk, then served.
    assert_eq!(getattr(&v, inside.ino).unwrap().ino, inside.ino);
    assert!(v.reach.contains(inside.ino));
    assert!(v.reach.contains(inside.ino), "cached after the walk");

    // A whole-filesystem view checks nothing.
    let (whole, _dir2) = view_at(&meta, None);
    assert_eq!(getattr(&whole, secret.ino).unwrap().ino, secret.ino);
    assert_eq!(readlink(&whole, sym.ino).unwrap(), b"secret");
}

#[test]
fn an_unlinked_open_file_stays_addressable_and_the_cache_is_only_a_cache() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap();
    let f = meta.create(a.ino, "f", 0o644, 0, 0).unwrap();
    let g = meta.create(a.ino, "g", 0o644, 0, 0).unwrap();
    let (v, _dir) = view_at(&meta, Some("/a"));
    assert_eq!(lookup(&v, ROOT_INO, "f").unwrap().attr.ino, f.ino);
    open(&v, f.ino).unwrap();
    meta.unlink(a.ino, "f").unwrap();
    // Evicted from the cache (a full shard is cleared): the open handle
    // still addresses it, though no name is left to walk from.
    v.reach.shard(f.ino).lock().unwrap().clear();
    assert_eq!(getattr(&v, f.ino).unwrap().nlink, 0);
    // An evicted inode with a name is walked again.
    v.reach.shard(g.ino).lock().unwrap().clear();
    assert_eq!(getattr(&v, g.ino).unwrap().ino, g.ino);
}

#[test]
fn a_snapshot_view_refuses_every_live_inode() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let source = meta.mkdir(ROOT_INO, "source", 0o755, 0, 0).unwrap();
    let file = meta.create(source.ino, "file", 0o644, 0, 0).unwrap();
    let (mut v, _dir, _nodes) = view_with_snapshots(&meta, &[("/source", "snap")]);
    v.set_snapshot_root("/source", "snap").unwrap();
    assert_eq!(getattr(&v, ROOT_INO).unwrap().kind, FileKind::Dir);
    let frozen = lookup(&v, ROOT_INO, "file").unwrap();
    assert!(View::is_synthetic(frozen.attr.ino));
    assert_eq!(code(getattr(&v, file.ino)), Code::Stale);
    assert_eq!(code(getattr(&v, source.ino)), Code::Stale);
    assert_eq!(code(lookup(&v, source.ino, "file")), Code::Stale);
}

/// A view with a tree-capable snapshot manager, and `snaps` taken.
fn view_with_snapshots(
    meta: &Arc<Meta>,
    snaps: &[(&str, &str)],
) -> (View, tempfile::TempDir, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 1 << 30).unwrap());
    let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
    let (manager, nodes) =
        crate::snapshot::test_manager(meta.clone(), store.clone(), DEFAULT_CHUNK_SIZE);
    let snapshots = Arc::new(manager);
    {
        let setup = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for (i, (path, name)) in snaps.iter().enumerate() {
            let rows = meta.take_journal(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
            meta.ack_journal_rows_at(&seqs, i as u64 + 1).unwrap();
            setup.block_on(snapshots.create(path, name)).unwrap();
        }
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let view = View::new(
        FsDependencies {
            meta: meta.clone(),
            store,
            cache,
            rt: rt.handle().clone(),
            sync: None,
            coop: None,
            staging_dir: dir.path().join("staging"),
            staging_budget: StagingBudget::new(1 << 30),
            snapshots,
            atime: Arc::new(crate::atime::AtimeAccumulator::new(
                crate::atime::AtimeMode::Off,
                crate::atime::AtimeStats::new(),
            )),
            prune_stats: crate::prune::PruneStats::new(),
            inflight: crate::kernel_inval::InFlight::disabled(),
            holds: None,
            watch: OpWatch::manual("test-watch", Duration::from_secs(30)),
            caps: FrontendCaps::linux_fuse(false),
            host: constellation_platform::HostServices::native(),
        },
        DEFAULT_CHUNK_SIZE,
        CompressionSetting::RAW,
    );
    std::mem::forget(rt);
    (view, dir, nodes)
}

#[test]
fn dot_constellation_under_a_subtree_view_mirrors_only_its_own_history() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let volumes = meta.mkdir(ROOT_INO, "volumes", 0o755, 0, 0).unwrap();
    let pv1 = meta.mkdir(volumes.ino, "pv-1", 0o755, 0, 0).unwrap();
    let pv2 = meta.mkdir(volumes.ino, "pv-2", 0o755, 0, 0).unwrap();
    meta.create(pv1.ino, "mine.txt", 0o644, 0, 0).unwrap();
    meta.create(pv2.ino, "theirs.txt", 0o644, 0, 0).unwrap();
    meta.create(ROOT_INO, "top.txt", 0o644, 0, 0).unwrap();
    let (mut v, _dir, _nodes) = view_with_snapshots(
        &meta,
        &[
            ("/volumes/pv-1", "own"),
            ("/volumes/pv-2", "sibling"),
            ("/", "whole"),
        ],
    );
    v.set_subtree_root("/volumes/pv-1").unwrap();

    let dot = lookup(&v, ROOT_INO, ".constellation").unwrap().attr.ino;
    let snapdir = lookup(&v, dot, "snapshot").unwrap().attr.ino;
    let mut names = readdir(&v, snapdir).unwrap();
    names.sort();
    // The view's own snapshot and the whole-filesystem one (which covers
    // it); never the sibling volume's.
    assert_eq!(names, ["own", "whole"]);
    assert_eq!(code(lookup(&v, snapdir, "sibling")), Code::NotFound);
    // Each mirrors the view's root at the same relative path: pv-1's
    // content, even through the snapshot of `/`.
    for snap in ["own", "whole"] {
        let root = lookup(&v, snapdir, snap).unwrap().attr.ino;
        assert_eq!(readdir(&v, root).unwrap(), ["mine.txt"], "{snap}");
        assert_eq!(code(lookup(&v, root, "top.txt")), Code::NotFound);
        assert_eq!(code(lookup(&v, root, "volumes")), Code::NotFound);
        assert_eq!(code(lookup(&v, root, "pv-2")), Code::NotFound);
    }
}

/// Plan 32 §0.5 through the view, the way a mount reaches it: the
/// `.constellation` and `snapshot` nodes are interned on first lookup and
/// kept, so they must follow the directory's inode, not the path it had
/// then. `/a` is looked up and listed, renamed to `/b`, snapshotted under
/// the new name, and a new `/a` is made and snapshotted: the moved
/// directory lists its own history (taken under either name) and never
/// the new `/a`'s.
#[test]
fn snapshot_listing_follows_a_renamed_directory() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap().ino;
    meta.create(a, "f", 0o644, 0, 0).unwrap();
    let (v, _dir, _nodes) = view_with_snapshots(&meta, &[("/a", "before")]);
    let mut segment = 100;
    let mut snapshot = |path: &str, name: &str| {
        segment += 1;
        let rows = meta.take_journal(usize::MAX).unwrap();
        let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
        meta.ack_journal_rows_at(&seqs, segment).unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(v.snapshots.create(path, name))
            .unwrap();
    };
    let sorted = |ino| {
        let mut names = readdir(&v, ino).unwrap();
        names.sort();
        names
    };

    let dot = lookup(&v, a, ".constellation").unwrap().attr.ino;
    let snapdir = lookup(&v, dot, "snapshot").unwrap().attr.ino;
    assert_eq!(sorted(snapdir), ["before"]);

    meta.rename(ROOT_INO, "a", ROOT_INO, "b").unwrap();
    snapshot("/b", "after");
    // The nodes the kernel already holds, and a fresh lookup (which
    // finds the same interned numbers).
    assert_eq!(sorted(snapdir), ["after", "before"]);
    assert_eq!(lookup(&v, a, ".constellation").unwrap().attr.ino, dot);
    assert_eq!(lookup(&v, dot, "snapshot").unwrap().attr.ino, snapdir);
    let after = lookup(&v, snapdir, "after").unwrap().attr.ino;
    assert_eq!(readdir(&v, after).unwrap(), ["f"]);

    let new_a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap().ino;
    snapshot("/a", "newa");
    assert_eq!(sorted(snapdir), ["after", "before"]);
    assert_eq!(code(lookup(&v, snapdir, "newa")), Code::NotFound);
    // The new `/a` lists its own snapshot, and the path rule's: `/a@before`
    // was taken at its path (the replaced-directory behaviour, kept).
    let new_dot = lookup(&v, new_a, ".constellation").unwrap().attr.ino;
    let new_snapdir = lookup(&v, new_dot, "snapshot").unwrap().attr.ino;
    assert_eq!(sorted(new_snapdir), ["before", "newa"]);
}

/// `/volumes/pv-1/f` with a second name in `/volumes/pv-1/sub`, pv-2
/// beside it, `/loose` unmarked; the volumes marked as link domains.
struct Pool {
    meta: Arc<Meta>,
    volumes: Ino,
    pv1: Ino,
    pv1_sub: Ino,
    pv2: Ino,
    f: Ino,
    loose: Ino,
    loose_file: Ino,
}

fn pool(marked: bool) -> Pool {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let volumes = meta.mkdir(ROOT_INO, "volumes", 0o755, 0, 0).unwrap().ino;
    let pv1 = meta.mkdir(volumes, "pv-1", 0o755, 0, 0).unwrap().ino;
    let pv1_sub = meta.mkdir(pv1, "sub", 0o755, 0, 0).unwrap().ino;
    let pv2 = meta.mkdir(volumes, "pv-2", 0o755, 0, 0).unwrap().ino;
    let f = meta.create(pv1, "f", 0o644, 0, 0).unwrap().ino;
    let loose = meta.mkdir(ROOT_INO, "loose", 0o755, 0, 0).unwrap().ino;
    let loose_file = meta.create(loose, "lf", 0o644, 0, 0).unwrap().ino;
    if marked {
        for pv in [pv1, pv2] {
            meta.set_xattr(
                pv,
                LINK_DOMAIN_XATTR,
                b"1",
                constellation_meta::SetXattrMode::Set,
            )
            .unwrap();
        }
    }
    Pool {
        meta,
        volumes,
        pv1,
        pv1_sub,
        pv2,
        f,
        loose,
        loose_file,
    }
}

fn confined(view: &mut View) {
    view.apply_spec(&ViewSpec {
        confine_links: true,
        ..ViewSpec::default()
    });
}

#[test]
fn a_maintenance_view_with_confine_links_keeps_marked_volumes_link_disjoint() {
    let p = pool(true);
    let (mut v, _dir) = view_at(&p.meta, None);
    confined(&mut v);
    // Across volumes: EXDEV, both ways, and into the root's domain.
    assert_eq!(code(link(&v, p.f, p.pv2, "x")), Code::CrossDevice);
    assert_eq!(code(link(&v, p.f, ROOT_INO, "x")), Code::CrossDevice);
    assert_eq!(code(link(&v, p.loose_file, p.pv1, "x")), Code::CrossDevice);
    // Within one volume, and within the unmarked rest: POSIX.
    assert_eq!(link(&v, p.f, p.pv1_sub, "f2").unwrap().attr.nlink, 2);
    assert_eq!(
        link(&v, p.loose_file, ROOT_INO, "lf2").unwrap().attr.nlink,
        2
    );
    // Moving one of several names across volumes would share the inode:
    // EXDEV; within its volume, or a file with a single name, moves.
    assert_eq!(
        code(rename(&v, p.pv1_sub, "f2", p.pv2, "f2")),
        Code::CrossDevice
    );
    rename(&v, p.pv1_sub, "f2", p.pv1, "f3").unwrap();
    let single = p.meta.create(p.pv1, "single", 0o644, 0, 0).unwrap().ino;
    rename(&v, p.pv1, "single", p.pv2, "single").unwrap();
    assert_eq!(lookup(&v, p.pv2, "single").unwrap().attr.ino, single);
    // Directories always move (their files' other names are their own).
    rename(&v, p.pv1, "sub", p.pv2, "sub").unwrap();
    assert_eq!(p.meta.getattr(p.f).unwrap().unwrap().nlink, 2);
    let _ = (p.volumes, p.loose);
}

#[test]
fn without_confine_links_hard_links_cross_domains_as_posix_does() {
    let p = pool(true);
    let (v, _dir) = view_at(&p.meta, None);
    assert_eq!(link(&v, p.f, p.pv2, "x").unwrap().attr.nlink, 2);
    assert_eq!(link(&v, p.loose_file, p.pv1, "y").unwrap().attr.nlink, 2);
}

#[test]
fn a_volume_view_refuses_links_to_anything_with_no_name_inside() {
    let p = pool(true);
    let (mut v, _dir) = view_at(&p.meta, Some("/volumes/pv-1"));
    confined(&mut v);
    // Inside the view: fine.
    let f = lookup(&v, ROOT_INO, "f").unwrap().attr.ino;
    assert_eq!(link(&v, f, p.pv1_sub, "g").unwrap().attr.nlink, 2);
    // An inode outside (a forged or replayed handle): already ESTALE, by
    // confinement, with or without `confine_links`.
    assert_eq!(code(link(&v, p.loose_file, ROOT_INO, "x")), Code::Stale);
    // One the view resolved, since renamed out of it by another view: the
    // handle stays addressable (the cache), but it has no name inside, so
    // `confine_links` refuses the link.
    let h = p.meta.create(p.pv1, "h", 0o644, 0, 0).unwrap().ino;
    assert_eq!(lookup(&v, ROOT_INO, "h").unwrap().attr.ino, h);
    p.meta.rename(p.pv1, "h", p.pv2, "h").unwrap();
    assert_eq!(code(link(&v, h, ROOT_INO, "h-again")), Code::CrossDevice);
    // Without it the same link succeeds (the limit the module doc names).
    let (mut plain, _dir2) = view_at(&p.meta, Some("/volumes/pv-1"));
    plain.apply_spec(&ViewSpec::default());
    assert_eq!(lookup(&plain, ROOT_INO, "f").unwrap().attr.ino, f);
    // Both of `f`'s names move to pv-2; `plain` still has it cached.
    p.meta.rename(p.pv1, "f", p.pv2, "f").unwrap();
    p.meta.rename(p.pv1_sub, "g", p.pv2, "g").unwrap();
    assert_eq!(link(&plain, f, ROOT_INO, "back").unwrap().attr.nlink, 3);
}

#[test]
fn an_unmarked_subtree_view_is_one_link_domain() {
    let p = pool(false);
    let (mut v, _dir) = view_at(&p.meta, Some("/volumes"));
    confined(&mut v);
    // No markers: the view's root is the only domain; everything inside
    // links freely, which is what `confine_links` means for a plain
    // subtree view.
    assert_eq!(link(&v, p.f, p.pv2, "x").unwrap().attr.nlink, 2);
}

#[test]
fn close_on_a_stale_handle_still_answers() {
    // `flush`/`release` of an inode outside the subtree answer (ESTALE)
    // rather than hang or touch it.
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap();
    let out = meta.create(ROOT_INO, "out", 0o644, 0, 0).unwrap();
    let (v, _dir) = view_at(&meta, Some("/a"));
    let c = caller();
    let flush = Blocking::run(|r| {
        v.flush(
            &OpCtx::new(OpKind::Flush, &c),
            out.ino,
            Fh(out.ino),
            LockOwner(1),
            r,
        )
    });
    assert_eq!(code(flush), Code::Stale);
}

/// The hot-path cost of confinement, per addressed inode: run with
/// `cargo test --release -p constellation-engine -- --ignored
/// confinement_hot_path_cost --nocapture`.
#[test]
#[ignore]
fn confinement_hot_path_cost() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let mut dir = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap().ino;
    for depth in 0..8 {
        dir = meta
            .mkdir(dir, &format!("d{depth}"), 0o755, 0, 0)
            .unwrap()
            .ino;
    }
    let files: Vec<Ino> = (0..1000)
        .map(|i| meta.create(dir, &format!("f{i}"), 0o644, 0, 0).unwrap().ino)
        .collect();
    let (whole, _d1) = view_at(&meta, None);
    let (sub, _d2) = view_at(&meta, Some("/a"));
    let rounds = 1000;
    let time = |label: &str, f: &dyn Fn(Ino)| {
        let started = Instant::now();
        for _ in 0..rounds {
            for &ino in &files {
                f(ino);
            }
        }
        let per = started.elapsed() / (rounds * files.len()) as u32;
        eprintln!("{label}: {per:?} per op");
    };
    time("whole-filesystem view", &|ino| {
        whole.enter_ino(ino).unwrap();
    });
    for &ino in &files {
        sub.enter_ino(ino).unwrap();
    }
    time("subtree view, inode already resolved", &|ino| {
        sub.enter_ino(ino).unwrap();
    });
    let started = Instant::now();
    for &ino in &files {
        sub.reach.shard(ino).lock().unwrap().clear();
        assert!(sub.dominated(ino, false).unwrap());
    }
    eprintln!(
        "subtree view, full walk (9 levels, no cache): {:?} per op",
        started.elapsed() / files.len() as u32
    );
}
