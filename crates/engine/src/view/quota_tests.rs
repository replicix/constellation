use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::DEFAULT_CHUNK_SIZE;
use object_store::memory::InMemory;
use tempfile::TempDir;

/// Plan 30 §M3b: `publish_now` refuses while the journal is non-empty
/// (`SPECULATION_OUTSTANDING`) so it never publishes speculation.
/// This test's bare `Meta` has no shipper acking it, so simulate one
/// ship of everything journaled so far under `segment`, exactly as
/// production does when a segment lands.
fn ship_all(meta: &Meta, segment: u64) {
    let rows = meta.take_journal(usize::MAX).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    meta.ack_journal_rows_at(&seqs, segment).unwrap();
}

pub(super) fn test_fs(meta: Arc<Meta>) -> (View, TempDir) {
    let (fs, dir, _cache) = test_fs_with(meta, DEFAULT_CHUNK_SIZE, |root| {
        DiskCache::open(root, 1 << 30).unwrap()
    });
    (fs, dir)
}

/// [`test_fs`], with the chunk size and the disk cache under the caller's
/// control — `open_cache` is handed the cache root before anything opens
/// it, so a test can seed chunk files there and have the rescan find them
/// — and that cache handed back. The passthrough tests need all three.
pub(super) fn test_fs_with(
    meta: Arc<Meta>,
    chunk_size: u32,
    open_cache: impl FnOnce(&std::path::Path) -> DiskCache,
) -> (View, TempDir, Arc<DiskCache>) {
    let dir = TempDir::new().unwrap();
    let cache_root = dir.path().join("cache");
    std::fs::create_dir_all(&cache_root).unwrap();
    let cache = Arc::new(open_cache(&cache_root));
    let fs = test_fs_on(meta, chunk_size, cache.clone(), dir.path().join("staging"));
    (fs, dir, cache)
}

/// A view over an existing `cache` (a second process's view of the same
/// node, in the handover tests), staging under `staging_dir`.
pub(super) fn test_fs_on(
    meta: Arc<Meta>,
    chunk_size: u32,
    cache: Arc<DiskCache>,
    staging_dir: std::path::PathBuf,
) -> View {
    let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
    let snapshots = Arc::new(crate::snapshot::SnapshotManager::new(
        meta.clone(),
        store.clone(),
        chunk_size,
        1,
    ));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    let _enter = handle.enter();
    let fs = View::new(
        FsDependencies {
            meta,
            store,
            cache,
            rt: handle,
            sync: None,
            coop: None,
            staging_dir,
            staging_budget: StagingBudget::new(1 << 30),
            snapshots,
            atime: Arc::new(crate::atime::AtimeAccumulator::new(
                crate::atime::AtimeMode::Off,
                crate::atime::AtimeStats::new(),
            )),
            prune_stats: crate::prune::PruneStats::new(),
            snapsched_stats: crate::snapsched::SnapSchedStats::new(),
            inflight: crate::kernel_inval::InFlight::disabled(),
            holds: None,
            watch: OpWatch::manual("test-watch", Duration::from_secs(30)),
            caps: FrontendCaps::linux_fuse(false),
            host: constellation_platform::HostServices::native(),
        },
        chunk_size,
        CompressionSetting::RAW,
    );
    std::mem::forget(rt);
    fs
}

/// A second view of the same node has the inode open: `unlink`'s
/// fast reap must not reap it (before, only the unlinking view's own
/// table was consulted). With no handle anywhere it reaps.
#[test]
fn unlink_does_not_reap_an_inode_another_view_has_open() {
    use crate::holds::{HoldConfig, HoldSources, Holds, OpenHandles};
    use std::sync::Mutex;
    struct OtherView(Mutex<Vec<Ino>>);
    impl OpenHandles for OtherView {
        fn open_inos(&self) -> Vec<Ino> {
            self.0.lock().unwrap().clone()
        }
    }
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let (mut fs, _tmpdir) = test_fs(meta.clone());
    let other = Arc::new(OtherView(Mutex::new(vec![f.ino])));
    let sources = Arc::new(HoldSources::default());
    sources.register(
        2,
        Arc::downgrade(&other) as std::sync::Weak<dyn OpenHandles>,
    );
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    fs.holds = Some(Holds::new(
        store.clone(),
        Arc::new(ChunkStore::new(store)),
        meta.clone(),
        1,
        sources,
        HoldConfig {
            refresh: std::time::Duration::from_secs(1),
            ttl: std::time::Duration::from_secs(3),
        },
    ));
    meta.unlink(ROOT_INO, "f").unwrap();
    assert!(!fs.reap_after_unlink(f.ino), "open in another view: kept");
    assert!(meta.getattr(f.ino).unwrap().is_some());
    assert_eq!(meta.orphans().unwrap(), vec![f.ino]);
    other.0.lock().unwrap().clear();
    assert!(fs.reap_after_unlink(f.ino), "open nowhere: reaped");
    assert!(meta.getattr(f.ino).unwrap().is_none());
}

/// The last name of a file the kernel references is reported to the hold
/// writer from before the unlink until the kernel's `FORGET`: a reaper
/// pass between the unlink and `reap_after_unlink` leaves the orphan, and
/// so does one after an unlink this replica applies only later (a
/// forwarded one). One of several names is not reported.
#[test]
fn an_unlinked_last_name_is_reported_from_before_the_unlink() {
    use crate::holds::{HoldConfig, HoldSources, Holds, OpenHandles};
    use constellation_vfs::Vfs;
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let g = meta.create(ROOT_INO, "g", 0o644, 0, 0).unwrap();
    let linked = meta.link(g.ino, ROOT_INO, "g2").unwrap();
    let (mut fs, _tmpdir) = test_fs(meta.clone());
    fs.caps.reports_forget = true;
    let sources = Arc::new(HoldSources::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    fs.holds = Some(Holds::new(
        store.clone(),
        Arc::new(ChunkStore::new(store)),
        meta.clone(),
        1,
        sources.clone(),
        HoldConfig {
            refresh: std::time::Duration::from_secs(1),
            ttl: std::time::Duration::from_secs(3),
        },
    ));
    let fs = Arc::new(fs);
    sources.register(1, Arc::downgrade(&fs) as std::sync::Weak<dyn OpenHandles>);

    fs.unlinking(&f);
    fs.unlinking(&linked);
    assert!(!sources.is_open(g.ino), "one of two names: not reported");
    // Not applied here yet: still named, kept reported.
    assert!(!fs.reap_after_unlink(f.ino));
    meta.unlink(ROOT_INO, "f").unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(fs.holds.as_ref().unwrap().refresh_once())
        .unwrap();
    assert_eq!(meta.orphans().unwrap(), vec![f.ino], "the reaper leaves it");
    fs.forget(f.ino);
    assert!(
        meta.getattr(f.ino).unwrap().is_none(),
        "reaped at the forget"
    );
}

/// A one-chunk manifest: data a hold protects.
fn with_data() -> Vec<u8> {
    use constellation_fs_core::manifest::{ChunkInfo, Manifest};
    Manifest {
        layout: constellation_fs_core::ChunkLayout::new(4096),
        file_len: 4,
        chunks: ChunkInfo::Inline(
            [(0u64, constellation_fs_core::ChunkHash::of(b"data"))]
                .into_iter()
                .collect(),
        ),
    }
    .encode()
}

/// Two views of one node over a shared hold writer, both reporting
/// `forget`s (two FUSE mounts).
struct TwoViews {
    a: Arc<View>,
    b: Arc<View>,
    holds: Arc<crate::holds::Holds>,
    store: Arc<dyn object_store::ObjectStore>,
    _dirs: Vec<TempDir>,
}

fn two_forgetting_views(meta: &Arc<Meta>, refresh: std::time::Duration) -> TwoViews {
    use crate::holds::{HoldConfig, HoldSources, Holds, OpenHandles};
    let sources = Arc::new(HoldSources::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let holds = Holds::new(
        store.clone(),
        Arc::new(ChunkStore::new(store.clone())),
        meta.clone(),
        1,
        sources.clone(),
        HoldConfig {
            refresh,
            ttl: refresh * 3,
        },
    );
    let mut dirs = Vec::new();
    let mut views = Vec::new();
    for id in [1, 2] {
        let (mut fs, dir) = test_fs(meta.clone());
        fs.caps.reports_forget = true;
        fs.holds = Some(holds.clone());
        let fs = Arc::new(fs);
        sources.register(id, Arc::downgrade(&fs) as std::sync::Weak<dyn OpenHandles>);
        views.push(fs);
        dirs.push(dir);
    }
    let b = views.pop().unwrap();
    let a = views.pop().unwrap();
    TwoViews {
        a,
        b,
        holds,
        store,
        _dirs: dirs,
    }
}

/// View A unlinked a file its kernel still references; view B's last
/// close of it must not reap it under A (before, `release` asked only
/// its own view): A reopens it, and A's `FORGET` reaps it.
#[test]
fn another_views_last_close_leaves_a_referenced_orphan() {
    use constellation_vfs::{Blocking, Name, OpCtx, OpKind, OpenOwner, Vfs};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let views = two_forgetting_views(&meta, std::time::Duration::from_secs(60));
    let (a, b) = (&views.a, &views.b);
    let caller = constellation_vfs::Caller::new(0, 0, None);
    let open = |view: &View| {
        let cx = OpCtx::new(OpKind::Open, &caller);
        Blocking::run(|r| view.open(&cx, f.ino, OpenFlags::READ, OpenOwner::NONE, r))
    };
    let close = |view: &View, fh| {
        let cx = OpCtx::new(OpKind::Release, &caller);
        Blocking::run(|r| view.release(&cx, f.ino, fh, OpenFlags::READ, None, r)).unwrap()
    };
    let in_b = open(b).unwrap();
    let cx = OpCtx::new(OpKind::Unlink, &caller);
    Blocking::run(|r| a.unlink(&cx, ROOT_INO, Name::new("f"), r)).unwrap();
    close(b, in_b.fh);
    assert_eq!(meta.orphans().unwrap(), vec![f.ino], "kept for A");
    let in_a = open(a).expect("A's reference reopens it");
    close(a, in_a.fh);
    a.forget(f.ino);
    assert!(
        meta.getattr(f.ino).unwrap().is_none(),
        "reaped at A's forget"
    );
}

/// A reference without a handle is claimed in the hold within
/// `claim_after` of its unlink on a node where nothing else runs a pass
/// (no applied segments to nudge it), not at the next regular refresh:
/// the view asks the hold writer to wake for it.
#[test]
fn a_referenced_orphan_is_claimed_without_waiting_for_the_refresh() {
    use constellation_vfs::{Blocking, Name, OpCtx, OpKind, Vfs};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    // Data for a hold to protect (an orphan without any is never claimed).
    meta.set_manifest(f.ino, &with_data(), 4).unwrap();
    let views = two_forgetting_views(&meta, std::time::Duration::from_secs(30));
    let (a, holds, store) = (&views.a, &views.holds, &views.store);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    rt.spawn(holds.clone().run(stop.clone()));
    let claimed = || {
        rt.block_on(object_store::ObjectStoreExt::head(
            &**store,
            &constellation_store_s3::layout::hold(1),
        ))
        .is_ok()
    };
    // The loop's first pass is behind it.
    std::thread::sleep(std::time::Duration::from_millis(200));
    let caller = constellation_vfs::Caller::new(0, 0, None);
    let cx = OpCtx::new(OpKind::Unlink, &caller);
    Blocking::run(|r| a.unlink(&cx, ROOT_INO, Name::new("f"), r)).unwrap();
    let unlinked = std::time::Instant::now();
    assert!(!claimed(), "not claimed at once");
    while !claimed() && unlinked.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        claimed(),
        "not claimed within {:?} (claim_after {:?})",
        unlinked.elapsed(),
        holds.claim_after()
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// The kernel's `FORGET` of a file it referenced only for the unlink —
/// within the claim grace, so never claimed — costs no hold pass (an
/// `rm -rf` would run them back to back); one of a reference that
/// outlived the grace withdraws its claim at once, not at the refresh.
#[test]
fn a_forget_runs_a_hold_pass_only_for_a_claimed_reference() {
    use constellation_vfs::{Blocking, Name, OpCtx, OpKind, Vfs};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let g = meta.create(ROOT_INO, "g", 0o644, 0, 0).unwrap();
    for ino in [f.ino, g.ino] {
        meta.set_manifest(ino, &with_data(), 4).unwrap();
    }
    let views = two_forgetting_views(&meta, std::time::Duration::from_secs(30));
    let (a, holds, store) = (&views.a, &views.holds, &views.store);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    rt.spawn(holds.clone().run(stop.clone()));
    let claimed = || {
        rt.block_on(object_store::ObjectStoreExt::head(
            &**store,
            &constellation_store_s3::layout::hold(1),
        ))
        .is_ok()
    };
    let caller = constellation_vfs::Caller::new(0, 0, None);
    let unlink = |name: &str| {
        let cx = OpCtx::new(OpKind::Unlink, &caller);
        Blocking::run(|r| a.unlink(&cx, ROOT_INO, Name::new(name), r)).unwrap()
    };
    let settle = || std::thread::sleep(std::time::Duration::from_millis(300));
    settle();
    let passes = holds.passes();
    assert!(passes >= 1, "the loop's first pass");

    // Let go of within the grace: reaped, and no pass.
    unlink("f");
    a.forget(f.ino);
    assert!(meta.getattr(f.ino).unwrap().is_none(), "reaped");
    settle();
    assert_eq!(holds.passes(), passes, "no pass for an unclaimed reference");

    // Held past the grace: claimed; let go of: withdrawn at once.
    unlink("g");
    let unlinked = std::time::Instant::now();
    while !claimed() && unlinked.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(claimed(), "claimed after the grace");
    a.forget(g.ino);
    let forgot = std::time::Instant::now();
    while claimed() && forgot.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        !claimed(),
        "withdrawn at the forget, not at the 30 s refresh"
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Publishing a scratch file over a shared one (a rename out of a
/// scratch directory) replaces it as a rename does: the replaced file's
/// last name goes, and it is an orphan kept for the kernel's reference to
/// it — before, the view never learned of it, and the hold writer's
/// reaper dropped it under that reference (`ESTALE` instead of the file).
#[test]
fn a_file_a_publish_replaces_is_kept_for_the_kernels_reference() {
    use constellation_vfs::{
        Blocking, Caller, LockOwner, Name, OpCtx, OpKind, OpenOwner, RenameFlags, SetXattrFlags,
        Vfs, WriteData, XattrName,
    };
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    meta.set_manifest(f.ino, &with_data(), 4).unwrap();
    let s = meta.mkdir(ROOT_INO, "s", 0o755, 0, 0).unwrap();
    let views = two_forgetting_views(&meta, std::time::Duration::from_secs(60));
    let (a, holds) = (&views.a, &views.holds);
    let caller = Caller::new(0, 0, None);
    let cx = |kind| OpCtx::new(kind, &caller);
    Blocking::run(|r| {
        a.setxattr(
            &cx(OpKind::Setxattr),
            s.ino,
            XattrName::new(constellation_meta::SCRATCH_XATTR),
            b"1",
            SetXattrFlags::empty(),
            r,
        )
    })
    .unwrap();
    let (entry, opened) = Blocking::run(|r| {
        a.create(
            &cx(OpKind::Create),
            s.ino,
            Name::new("new"),
            0o100644,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    let new = entry.attr.ino;
    Blocking::run(|r| {
        a.write(
            &cx(OpKind::Write),
            new,
            opened.fh,
            0,
            WriteData::Borrowed(b"published"),
            OpenFlags::WRITE,
            r,
        )
    })
    .unwrap();
    Blocking::run(|r| a.flush(&cx(OpKind::Flush), new, opened.fh, LockOwner(1), r)).unwrap();
    Blocking::run(|r| {
        a.release(
            &cx(OpKind::Release),
            new,
            opened.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    Blocking::run(|r| {
        a.rename(
            &cx(OpKind::Rename),
            s.ino,
            Name::new("new"),
            ROOT_INO,
            Name::new("f"),
            RenameFlags::empty(),
            r,
        )
    })
    .unwrap();
    assert_eq!(meta.lookup(ROOT_INO, "f").unwrap().unwrap().ino, new);

    // A reaper pass: the replaced file stays for the kernel's reference.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(holds.refresh_once()).unwrap();
    assert_eq!(meta.getattr(f.ino).unwrap().unwrap().nlink, 0);
    a.forget(f.ino);
    assert!(
        meta.getattr(f.ino).unwrap().is_none(),
        "reaped at the forget"
    );
}

#[test]
fn quota_check_unlimited_always_ok() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let (fs, _tmpdir) = test_fs(meta);
    assert!(fs.quota_check(f.ino, 1 << 40).is_ok());
}

#[test]
fn quota_check_under_cap_ok_over_cap_enospc() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_quota(Some(100)).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    meta.setattr(f.ino, None, None, None, Some(40), None, None)
        .unwrap();
    let (fs, _tmpdir) = test_fs(meta);
    View::invalidate_quota_cache(&fs.quota_cache);
    assert!(fs.quota_check(f.ino, 90).is_ok());
    assert!(fs.quota_check(f.ino, 100).is_ok());
    assert_eq!(fs.quota_check(f.ino, 101).unwrap_err(), Code::NoSpace);
    // Shrinking never grows past the cap: pending growth is zero once
    // the intended length is at or below the committed size.
    assert!(fs.quota_check(f.ino, 10).is_ok());
}

/// The committed length is `inode.size`, not the manifest's: after a
/// sparse `ftruncate` the manifest still reads 0, and charging growth
/// against it would bill the same bytes twice.
#[test]
fn quota_check_does_not_double_count_a_sparse_truncate() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_quota(Some(100)).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    // `truncate -s 60` with no manifest committed yet.
    meta.setattr(f.ino, None, None, None, Some(60), None, None)
        .unwrap();
    assert_eq!(meta.usage(), (60, 1));
    assert!(meta.manifest(f.ino).unwrap().is_none());
    let (fs, _tmpdir) = test_fs(meta);
    View::invalidate_quota_cache(&fs.quota_cache);
    // Writing inside the truncated length adds nothing to the total.
    assert!(fs.quota_check(f.ino, 60).is_ok());
    // Growing to 100 fits exactly; 101 does not.
    assert!(fs.quota_check(f.ino, 100).is_ok());
    assert_eq!(fs.quota_check(f.ino, 101).unwrap_err(), Code::NoSpace);
}

#[test]
fn usage_counter_tracks_create_setattr_unlink() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    assert_eq!(meta.usage(), (0, 0));
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    assert_eq!(meta.usage(), (0, 1));
    meta.setattr(f.ino, None, None, None, Some(50), None, None)
        .unwrap();
    assert_eq!(meta.usage(), (50, 1));
    meta.set_manifest(f.ino, b"m", 80).unwrap();
    assert_eq!(meta.usage(), (80, 1));
    meta.unlink(ROOT_INO, "f").unwrap();
    assert_eq!(meta.usage(), (0, 0));
    let recomputed = meta.recursive_size(ROOT_INO).unwrap();
    assert_eq!(meta.usage(), recomputed);
}

/// A rename that replaces an existing file drops that file from the
/// reachable set, so the counter has to shed it (replay's
/// `evict_dentry` does the same on every peer).
#[test]
fn usage_counter_tracks_replacing_rename() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let a = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
    meta.setattr(a.ino, None, None, None, Some(100), None, None)
        .unwrap();
    meta.setattr(b.ino, None, None, None, Some(7), None, None)
        .unwrap();
    assert_eq!(meta.usage(), (107, 2));
    meta.rename(ROOT_INO, "b", ROOT_INO, "a").unwrap();
    assert_eq!(meta.usage(), (7, 1));
    assert_eq!(meta.usage(), meta.recursive_size(ROOT_INO).unwrap());

    // A rename onto a still-linked target only unlinks one name.
    let c = meta.create(ROOT_INO, "c", 0o644, 0, 0).unwrap();
    meta.setattr(c.ino, None, None, None, Some(9), None, None)
        .unwrap();
    meta.link(c.ino, ROOT_INO, "c2").unwrap();
    let d = meta.create(ROOT_INO, "d", 0o644, 0, 0).unwrap();
    assert_eq!(meta.usage(), (16, 3));
    meta.rename(ROOT_INO, "d", ROOT_INO, "c").unwrap();
    assert_eq!(meta.usage(), (16, 3));
    assert_eq!(meta.usage(), meta.recursive_size(ROOT_INO).unwrap());
    let _ = d;
}

#[test]
fn set_quota_round_trip_and_replay() {
    let src = Arc::new(Meta::open_in_memory().unwrap());
    src.set_quota(Some(1234)).unwrap();
    assert_eq!(src.quota().unwrap(), Some(1234));
    src.set_quota(None).unwrap();
    assert_eq!(src.quota().unwrap(), None);

    let src2 = Arc::new(Meta::open_in_memory().unwrap());
    src2.set_quota(Some(999)).unwrap();
    let records: Vec<_> = src2
        .take_journal(100)
        .unwrap()
        .into_iter()
        .map(|(_, r)| r)
        .collect();
    let dst = Arc::new(Meta::open_in_memory().unwrap());
    dst.apply_records(&records).unwrap();
    assert_eq!(dst.quota().unwrap(), Some(999));
}

#[test]
fn statfs_reports_view_usage_against_whole_fs_headroom() {
    let block = 131072u64;
    // Whole-filesystem mount under a cap: total collapses to the cap.
    let (total, free) = statfs_blocks(40 * block, 40 * block, Some(100 * block), block);
    assert_eq!((total, free), (100, 60));
    assert_eq!(total - free, 40, "used is the view's own bytes");

    // Subtree mount holding 10 blocks of a filesystem using 40: used
    // scopes to the view, free still reflects the cluster-wide cap.
    let (total, free) = statfs_blocks(10 * block, 40 * block, Some(100 * block), block);
    assert_eq!((total - free, free), (10, 60));

    // Overshooting the cap reports full rather than negative free.
    let (total, free) = statfs_blocks(120 * block, 120 * block, Some(100 * block), block);
    assert_eq!((total, free), (120, 0));

    // Uncapped: effectively unbounded free space, exact used.
    let huge = u64::MAX / block / 2;
    let (total, free) = statfs_blocks(7 * block, 7 * block, None, block);
    assert_eq!((total, free), (huge + 7, huge));

    // Partial blocks round used up and free down.
    let (total, free) = statfs_blocks(1, 1, Some(2 * block), block);
    assert_eq!((total - free, free), (1, 1));
}

#[test]
fn parse_statfs_ttl_defaults_to_five_seconds() {
    assert_eq!(parse_statfs_ttl_secs(None), Duration::from_secs(5));
    assert_eq!(parse_statfs_ttl_secs(Some("")), Duration::from_secs(5));
    assert_eq!(parse_statfs_ttl_secs(Some("bogus")), Duration::from_secs(5));
    assert_eq!(parse_statfs_ttl_secs(Some("0")), Duration::from_secs(0));
    assert_eq!(parse_statfs_ttl_secs(Some("12")), Duration::from_secs(12));
}

/// A whole-filesystem mount answers from the maintained counter, so it
/// is exact and never serves a stale aggregate.
#[test]
fn view_usage_of_a_full_mount_tracks_the_counter() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
    let a = meta.create(dir.ino, "a", 0o644, 0, 0).unwrap();
    let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
    meta.setattr(a.ino, None, None, None, Some(7), None, None)
        .unwrap();
    meta.setattr(b.ino, None, None, None, Some(100), None, None)
        .unwrap();

    let (fs, _tmpdir) = test_fs(meta.clone());
    assert_eq!(fs.view_usage(), (107, 2));
    meta.setattr(b.ino, None, None, None, Some(1), None, None)
        .unwrap();
    assert_eq!(fs.view_usage(), (8, 2), "no TTL between a write and df");
}

#[test]
fn view_usage_scopes_to_subtree_mount() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
    let a = meta.create(dir.ino, "a", 0o644, 0, 0).unwrap();
    let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
    meta.setattr(a.ino, None, None, None, Some(7), None, None)
        .unwrap();
    meta.setattr(b.ino, None, None, None, Some(100), None, None)
        .unwrap();

    let (mut fs, _tmpdir) = test_fs(meta);
    assert_eq!(fs.view_usage(), (107, 2));
    fs.set_subtree_root("/d").unwrap();
    assert_eq!(fs.view_usage(), (7, 1));
}

#[test]
fn zero_ttl_disables_cache_while_positive_ttl_caches() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
    let f = meta.create(dir.ino, "f", 0o644, 0, 0).unwrap();
    meta.setattr(f.ino, None, None, None, Some(10), None, None)
        .unwrap();

    // Only a scoped mount pays for (and caches) the recursive walk.
    let (mut fs, _tmpdir) = test_fs(meta.clone());
    fs.set_subtree_root("/d").unwrap();
    fs.statfs_ttl = Duration::from_secs(60);
    assert_eq!(fs.view_usage(), (10, 1));
    meta.setattr(f.ino, None, None, None, Some(99), None, None)
        .unwrap();
    assert_eq!(
        fs.view_usage(),
        (10, 1),
        "positive TTL must serve the stale aggregate"
    );

    fs.statfs_ttl = Duration::from_secs(0);
    *fs.usage_cache.lock().unwrap() = None;
    assert_eq!(fs.view_usage(), (99, 1));
    meta.setattr(f.ino, None, None, None, Some(1), None, None)
        .unwrap();
    assert_eq!(
        fs.view_usage(),
        (1, 1),
        "TTL 0 must recompute on every call"
    );
}

#[test]
fn view_usage_scopes_to_snapshot_mount() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let source = meta.mkdir(ROOT_INO, "source", 0o755, 0, 0).unwrap();
    let file = meta.create(source.ino, "file", 0o644, 0, 0).unwrap();
    meta.setattr(file.ino, None, None, None, Some(42), None, None)
        .unwrap();
    let outside = meta.create(ROOT_INO, "outside", 0o644, 0, 0).unwrap();
    meta.setattr(outside.ino, None, None, None, Some(1000), None, None)
        .unwrap();

    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 1 << 30).unwrap());
    let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
    let (manager, _nodes) =
        crate::snapshot::test_manager(meta.clone(), store.clone(), DEFAULT_CHUNK_SIZE);
    let snapshots = Arc::new(manager);
    ship_all(&meta, 1);
    // Create the snapshot on a throwaway runtime so the FUSE handle's
    // runtime is idle when view_usage later block_on's tree loads.
    {
        let setup = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        setup.block_on(snapshots.create("/source", "snap")).unwrap();
    }

    let fs_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut fs = View::new(
        FsDependencies {
            meta,
            store,
            cache,
            rt: fs_rt.handle().clone(),
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
            snapsched_stats: crate::snapsched::SnapSchedStats::new(),
            inflight: crate::kernel_inval::InFlight::disabled(),
            holds: None,
            watch: OpWatch::manual("test-watch", Duration::from_secs(30)),
            caps: FrontendCaps::linux_fuse(false),
            host: constellation_platform::HostServices::native(),
        },
        DEFAULT_CHUNK_SIZE,
        CompressionSetting::RAW,
    );
    std::mem::forget(fs_rt);
    fs.statfs_ttl = Duration::from_secs(0);
    assert_eq!(fs.view_usage(), (1042, 2));
    fs.set_snapshot_root("/source", "snap").unwrap();
    assert_eq!(fs.view_usage(), (42, 1));
}

/// Plan 39 §3.7: a node's discard error events are shared by every view
/// of it, so one view's last close must not forget an event a
/// description in another view (another mount of the node) still owes.
/// Forgotten once no view has the inode open.
#[test]
fn a_discard_error_outlives_one_views_last_close_while_another_has_it_open() {
    use crate::holds::{HoldConfig, HoldSources, Holds, OpenHandles};
    use constellation_vfs::{Blocking, Caller, OpCtx, OpKind, OpenFlags, OpenOwner, Vfs};
    use std::sync::Mutex;
    struct OtherView(Mutex<Vec<Ino>>);
    impl OpenHandles for OtherView {
        fn open_inos(&self) -> Vec<Ino> {
            self.0.lock().unwrap().clone()
        }
    }
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.set_node_prefix(1).unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let (mut fs, _tmpdir) = test_fs(meta.clone());
    let other = Arc::new(OtherView(Mutex::new(vec![f.ino])));
    let sources = Arc::new(HoldSources::default());
    sources.register(
        2,
        Arc::downgrade(&other) as std::sync::Weak<dyn OpenHandles>,
    );
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    fs.holds = Some(Holds::new(
        store.clone(),
        Arc::new(ChunkStore::new(store)),
        meta.clone(),
        1,
        sources,
        HoldConfig {
            refresh: std::time::Duration::from_secs(1),
            ttl: std::time::Duration::from_secs(3),
        },
    ));
    let caller = Caller::new(0, 0, None);
    let open_close = || {
        let fh = Blocking::run(|r| {
            fs.open(
                &OpCtx::new(OpKind::Open, &caller),
                f.ino,
                OpenFlags::READ,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap()
        .fh;
        Blocking::run(|r| {
            fs.release(
                &OpCtx::new(OpKind::Release, &caller),
                f.ino,
                fh,
                OpenFlags::READ,
                None,
                r,
            )
        })
    };
    let seq = meta.locks().note_discard(f.ino);
    // This view's last close: the other view still has the inode open.
    open_close().unwrap();
    assert_eq!(meta.locks().error_seq(f.ino), seq, "still owed elsewhere");
    other.0.lock().unwrap().clear();
    open_close().unwrap();
    assert_eq!(meta.locks().error_seq(f.ino), 0, "open nowhere: forgotten");
}
