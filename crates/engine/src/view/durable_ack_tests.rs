//! Plan 30 §M9 with §M6: under a durability gate, the holder's own
//! manifest commit on close is acknowledged only once its row is
//! durable, so its next read never waits for it
//! (`session-forwarded-ryw`'s regression: the close returned at once
//! and the file's next `stat`/`cat` waited in
//! `Meta::durability_pending` instead).
use super::*;
use constellation_authority::core::LeaseState;
use constellation_authority::{Config, Ms};
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{ReadKey, SessionWait};
use constellation_store_s3::lease::now_unix_ms;
use constellation_store_s3::{Lease, LeaseMode, LeaseStore};
use object_store::memory::InMemory;
use tempfile::TempDir;

/// A holder's filesystem: the lease view is held with the durability
/// gate `gated`; the core's side of the channel is returned (kept
/// open, never answered — nothing here goes through the core).
fn holder_fs(
    meta: Arc<Meta>,
    gated: bool,
) -> (
    View,
    TempDir,
    tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
) {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 1 << 30).unwrap());
    let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
    let snapshots = Arc::new(crate::snapshot::SnapshotManager::new(
        meta.clone(),
        store.clone(),
        constellation_fs_core::DEFAULT_CHUNK_SIZE,
        1,
    ));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tag = rt.block_on(async {
        LeaseStore::new(Arc::new(InMemory::new()), "p0", LeaseMode::Cas)
            .try_create(&Lease::granted("p0", 1, 1, 1000))
            .await
            .unwrap()
    });
    let cfg = Config::defaults(1, 1);
    let now = Ms(now_unix_ms());
    let mut state = LeaseState::default();
    let lease = state.granted_lease(now, &cfg, None);
    state.adopt(now, lease, tag, None);
    let view = Arc::new(crate::lease::LeaseView::default());
    view.mirror(&state, now, &cfg, gated);
    assert!(
        view.admit().is_some(),
        "the fast path is open to the holder"
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = rt.handle().clone();
    let fs = View::new(
        FsDependencies {
            meta,
            store,
            cache,
            rt: handle,
            sync: Some(SyncHandle {
                tx,
                fsync_s3: false,
                cto_strict: false,
                lease: view,
                delegates: Arc::new(crate::lease::DelegateView::default()),
                acquire_deadline: Duration::from_secs(1),
                epoch_frozen: None,
                epoch_active: None,
                departed: None,
                read_only_member: false,
                write_mode: Arc::new(crate::writeback::WriteModeState::new(
                    crate::writeback::WriteMode::Back,
                )),
                node_id: 1,
                incarnation: 1,
                locks: None,
                next_rid_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                acked: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
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
        constellation_fs_core::DEFAULT_CHUNK_SIZE,
        CompressionSetting::RAW,
    );
    std::mem::forget(rt);
    (fs, dir, rx)
}

/// A non-owner's close, driven against a scripted core: what it asks
/// the sync task for, in order, answering each (a drain succeeds, a
/// forward is accepted).
fn nonowner_close(mode: crate::writeback::WriteMode) -> (Vec<&'static str>, Arc<Meta>, Ino) {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (mut fs, _dir, mut core) = holder_fs(meta.clone(), false);
    {
        let h = fs.sync.as_mut().unwrap();
        // Not the holder: the fast path is closed, the close forwards.
        h.lease = Arc::new(crate::lease::LeaseView::default());
        h.write_mode.set(mode);
    }
    fs.do_write(file.ino, 0, b"small file").unwrap();
    let mut seen = Vec::new();
    std::thread::scope(|scope| {
        let close = scope.spawn(|| fs.flush_inode(file.ino, false));
        let started = std::time::Instant::now();
        loop {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { reply, .. }) => {
                    seen.push("drain");
                    reply.send(Ok(())).unwrap();
                }
                Ok(SyncRequest::Submit { op, reply, .. }) => {
                    assert!(matches!(
                        op,
                        constellation_meta::MutateOp::SetManifest { .. }
                    ));
                    seen.push("forward");
                    reply
                        .send(constellation_authority::ClientReply::Outcome(
                            constellation_meta::MutateOutcome::Accepted {
                                epoch: 1,
                                records: Vec::new(),
                            },
                        ))
                        .unwrap();
                }
                Ok(SyncRequest::Nudge) => seen.push("nudge"),
                Ok(_) => {}
                Err(_) if close.is_finished() => break,
                Err(_) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(10),
                        "the close is stuck: {seen:?}"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        close.join().unwrap().unwrap();
    });
    while let Ok(req) = core.try_recv() {
        if matches!(req, SyncRequest::Nudge) {
            seen.push("nudge");
        }
    }
    (seen, meta, file.ino)
}

/// The small-file fix: under `through` a non-owner uploads before it
/// forwards (the log never names a chunk S3 lacks); under `back` it
/// forwards at once — its chunks stay enrolled here, the forward names
/// them as pending, and the next round uploads them.
#[test]
fn a_nonowner_back_close_forwards_before_its_upload() {
    let (seen, _, _) = nonowner_close(crate::writeback::WriteMode::Through);
    assert_eq!(&seen[..2], ["drain", "forward"], "through: {seen:?}");
    let (seen, meta, ino) = nonowner_close(crate::writeback::WriteMode::Back);
    assert!(!seen.contains(&"drain"), "back: {seen:?}");
    assert_eq!(seen.first(), Some(&"forward"), "back: {seen:?}");
    assert!(
        seen.contains(&"nudge"),
        "back: the upload is started: {seen:?}"
    );
    let pending: Vec<_> = meta
        .pending_uploads()
        .unwrap()
        .into_iter()
        .filter(|(_, i)| *i == ino)
        .collect();
    assert_eq!(pending.len(), 1, "the chunk stays enrolled for the upload");
}

/// EC2 finding 1: a close waiting on its drain (S3 unreachable) holds
/// the inode's operation lock, never its write shard — `getattr` and
/// `lookup` of every inode sharing the shard (the round-1 `ls -la`)
/// answer at once, and the file's own size is its new one — while a
/// write to the same file still waits for the close to finish.
#[test]
fn a_close_stuck_in_its_drain_holds_no_write_shard() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (fs, _dir, mut core) = holder_fs(meta.clone(), false);
    fs.sync
        .as_ref()
        .unwrap()
        .write_mode
        .set(crate::writeback::WriteMode::Through);
    fs.do_write(file.ino, 0, b"hello").unwrap();
    std::thread::scope(|scope| {
        let close = scope.spawn(|| fs.flush_inode(file.ino, false));
        let started = std::time::Instant::now();
        let reply = loop {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { ino, reply }) => {
                    assert_eq!(ino, file.ino);
                    break reply;
                }
                Ok(_) => {}
                Err(_) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(5),
                        "the close never asked for its drain"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        };
        assert!(
            fs.writes.maps[file.ino as usize % WRITE_SHARDS]
                .try_lock()
                .is_ok(),
            "a close waiting on S3 holds its write shard"
        );
        let shard = fs.writes.lock(file.ino);
        let size = fs
            .writes
            .pending_len(&shard, file.ino)
            .unwrap_or_else(|| meta.getattr(file.ino).unwrap().unwrap().size);
        drop(shard);
        assert_eq!(size, 5);
        let write = scope.spawn(|| fs.do_write(file.ino, 5, b"!"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !write.is_finished(),
            "a write overtook the same file's close"
        );
        reply.send(Ok(())).unwrap();
        close.join().unwrap().unwrap();
        write.join().unwrap().unwrap();
    });
}

#[test]
fn a_gated_holders_close_returns_once_its_manifest_row_is_durable() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    // Everything journaled so far is durable (on the backups).
    let session = meta.session();
    session.set_durable(true, meta.journal_tip().unwrap(), false);
    let (fs, _dir, _core) = holder_fs(meta.clone(), true);
    fs.do_write(file.ino, 0, b"hello").unwrap();

    let keys = [ReadKey::Ino(file.ino)];
    std::thread::scope(|scope| {
        let close = scope.spawn(|| fs.flush_inode(file.ino, false));
        // The manifest row is journaled, not yet durable: the close
        // has not returned.
        let tip = {
            let started = std::time::Instant::now();
            loop {
                let tip = meta.journal_tip().unwrap();
                if tip > session.durable_jseq() {
                    break tip;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "the manifest commit never journaled"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        };
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !close.is_finished(),
            "the close returned before its manifest row was durable"
        );
        assert!(meta.durability_pending(&keys));
        // The backups acknowledge it: the close returns.
        session.set_durable(true, tip, false);
        assert_eq!(close.join().unwrap(), Ok(()));
    });
    // Read-your-writes without a wait: nothing of the file's is
    // tentative any more.
    assert!(!meta.durability_pending(&keys));
    assert_eq!(meta.session_wait(&keys), SessionWait::Fast);
    assert_eq!(meta.getattr(file.ino).unwrap().unwrap().size, 5);
    assert_eq!(session.stats().fast_acks_waited, 1);
}

#[test]
fn an_ungated_holders_close_does_not_wait() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (fs, _dir, _core) = holder_fs(meta.clone(), false);
    fs.do_write(file.ino, 0, b"hello").unwrap();
    fs.flush_inode(file.ino, false).unwrap();
    assert_eq!(meta.getattr(file.ino).unwrap().unwrap().size, 5);
    assert_eq!(meta.session().stats().fast_acks_waited, 0);
}
