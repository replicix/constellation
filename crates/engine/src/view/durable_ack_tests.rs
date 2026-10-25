//! Plan 30 §M9 with §M6: under a durability gate, the holder's own
//! manifest commit on close is acknowledged only once its row is
//! durable, so its next read never waits for it
//! (`session-forwarded-ryw`'s regression: the close returned at once
//! and the file's next `stat`/`cat` waited in
//! `Meta::durability_pending` instead).
use super::*;
use constellation_authority::core::LeaseState;
use constellation_authority::{Config, Ms, Now};
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{ReadKey, SessionWait};
use constellation_store_s3::lease::now_unix_ms;
use constellation_store_s3::{Lease, LeaseMode, LeaseStore};
use object_store::memory::InMemory;
use tempfile::TempDir;

/// A holder's filesystem: the lease view is held with the durability
/// gate `gated`; the core's side of the channel is returned (kept
/// open, never answered — nothing here goes through the core).
pub(super) fn holder_fs(
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
    let lease = state.granted_lease(Now::same(now.0), &cfg, None, false);
    state.adopt(Now::same(now.0), lease, tag, None);
    let view = Arc::new(crate::lease::LeaseView::default());
    view.mirror(&state, Now::same(now.0), &cfg, gated);
    assert!(
        view.admit().is_some(),
        "the fast path is open to the holder"
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = rt.handle().clone();
    let fs = View::new(
        FsDependencies {
            listing: crate::view::Listing::for_test(),
            meta,
            store,
            cache,
            rt: handle,
            sync: Some(SyncHandle {
                tx,
                fsync_s3: false,
                fsync: crate::fsync_wait::default_waits(),
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
            snapsched_stats: crate::snapsched::SnapSchedStats::new(),
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
/// no write shard — `getattr` and `lookup` of every inode sharing the
/// shard (the round-1 `ls -la`) answer at once, and the file's own size
/// is its new one. Nor the inode's operation lock: its commit was
/// answered before the drain began, so a write and a read of the same
/// file go on meanwhile (the write in a new session over the published
/// manifest), and the next close publishes over it. Until the lock
/// ended with the commit, every op on the file waited one upload per
/// close (xcheckfs `lock_traffic_concurrent_with_io`).
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
                Ok(SyncRequest::DrainInode { ino, reply, .. }) => {
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
        assert_eq!(
            meta.getattr(file.ino).unwrap().unwrap().size,
            5,
            "the close's commit was answered before its drain"
        );
        // Each waited for at most a while, and the drain answered
        // whatever happened: a write still waiting would never end.
        fn done_soon<T>(h: &std::thread::ScopedJoinHandle<'_, T>) -> bool {
            let started = std::time::Instant::now();
            while !h.is_finished() && started.elapsed() < Duration::from_secs(5) {
                std::thread::sleep(Duration::from_millis(1));
            }
            h.is_finished()
        }
        let write = scope.spawn(|| fs.do_write(file.ino, 5, b"!"));
        let wrote = done_soon(&write);
        let read = wrote.then(|| scope.spawn(|| fs.do_read(file.ino, 0, 16)));
        let read_back = read.as_ref().is_some_and(|r| done_soon(r));
        reply.send(Ok(())).unwrap();
        close.join().unwrap().unwrap();
        write.join().unwrap().unwrap();
        assert!(wrote, "a write waited for the same file's drain");
        assert!(read_back, "a read waited for the same file's drain");
        let data = read.unwrap().join().unwrap().unwrap();
        assert_eq!(&*data.contiguous().unwrap(), b"hello!");
    });
    // The next close publishes the write over the first close's manifest.
    std::thread::scope(|scope| {
        let close = scope.spawn(|| fs.flush_inode(file.ino, false));
        let started = std::time::Instant::now();
        loop {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { reply, .. }) => {
                    reply.send(Ok(())).unwrap();
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    assert!(started.elapsed() < Duration::from_secs(5));
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        close.join().unwrap().unwrap();
    });
    assert_eq!(meta.getattr(file.ino).unwrap().unwrap().size, 6);
    assert_eq!(
        &*fs.do_read(file.ino, 0, 16).unwrap().contiguous().unwrap(),
        b"hello!"
    );
}

#[test]
fn a_gated_holders_close_returns_once_its_manifest_row_is_durable() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    // Everything journaled so far is durable (on the backups).
    let session = meta.session();
    session.set_durable(
        true,
        meta.journal_tip().unwrap(),
        false,
        constellation_meta::RowClass::RootGroup,
    );
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
        session.set_durable(true, tip, false, constellation_meta::RowClass::RootGroup);
        assert_eq!(close.join().unwrap(), Ok(()));
    });
    // Read-your-writes without a wait: nothing of the file's is
    // tentative any more.
    assert!(!meta.durability_pending(&keys));
    assert_eq!(meta.session_wait(&keys), SessionWait::Fast);
    assert_eq!(meta.getattr(file.ino).unwrap().unwrap().size, 5);
    assert_eq!(session.stats().fast_acks_waited, 1);
}

/// Plan 43 C1-2 (D4): the gate goes off while a fast-path close waits (the
/// group is gone, the lease still this node's): the close returns, the row
/// it journaled is re-tagged `Lone` — it was answered on this disk alone,
/// and what a deposition does with the row must match. Fails on the
/// parent: the row stayed gated-class.
#[test]
fn ungated_fast_path_answer_retags_the_row_lone() {
    use constellation_meta::RowClass;
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let session = meta.session();
    session.set_durable(
        true,
        meta.journal_tip().unwrap(),
        false,
        RowClass::RootGroup,
    );
    let (fs, _dir, _core) = holder_fs(meta.clone(), true);
    fs.do_write(file.ino, 0, b"hello").unwrap();
    std::thread::scope(|scope| {
        let close = scope.spawn(|| fs.flush_inode(file.ino, false));
        let started = std::time::Instant::now();
        let tip = loop {
            let tip = meta.journal_tip().unwrap();
            if tip > session.durable_jseq() {
                break tip;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "never journaled"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        // Journaled under the class the core published with the gate.
        assert_eq!(
            meta.journal_tx_class(tip).unwrap(),
            Some(RowClass::RootGroup)
        );
        std::thread::sleep(Duration::from_millis(100));
        assert!(!close.is_finished(), "the close waits for the group");
        // The gate goes off, the lease still this node's.
        session.set_durable(false, u64::MAX, false, RowClass::Lone);
        assert_eq!(close.join().unwrap(), Ok(()));
        assert_eq!(meta.journal_tx_class(tip).unwrap(), Some(RowClass::Lone));
    });
    assert_eq!(session.stats().fast_acks_downgraded, 1);
}

/// Waits until something journaled past the durable watermark; returns
/// the journal's tip.
fn journaled_past_durable(meta: &Meta) -> u64 {
    let session = meta.session();
    let started = std::time::Instant::now();
    loop {
        let tip = meta.journal_tip().unwrap();
        if tip > session.durable_jseq() {
            return tip;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never journaled"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Plan 43 C1-2 (D4), the C1-2/C1-3 review: the gate goes off while a
/// fast-path close waits, and the store fails the `Lone` re-tag. The close
/// is not acknowledged (`EIO`, the staged state kept): the row is still
/// gated-class, and an answer on it would claim a class nothing recorded.
/// Fails on the parent: the error was ignored and the close returned
/// `Ok`.
#[test]
fn a_failed_retag_fails_the_fast_path_close() {
    use constellation_meta::RowClass;
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let session = meta.session();
    session.set_durable(
        true,
        meta.journal_tip().unwrap(),
        false,
        RowClass::RootGroup,
    );
    let (fs, _dir, _core) = holder_fs(meta.clone(), true);
    fs.do_write(file.ino, 0, b"hello").unwrap();
    std::thread::scope(|scope| {
        let close = scope.spawn(|| {
            constellation_meta::fail_reclass_on_this_thread_for_tests(1);
            let r = fs.flush_inode(file.ino, false);
            constellation_meta::fail_reclass_on_this_thread_for_tests(0);
            r
        });
        let tip = journaled_past_durable(&meta);
        std::thread::sleep(Duration::from_millis(100));
        assert!(!close.is_finished(), "the close waits for the group");
        session.set_durable(false, u64::MAX, false, RowClass::Lone);
        assert_eq!(close.join().unwrap(), Err(Code::Io));
        assert_eq!(
            meta.journal_tx_class(tip).unwrap(),
            Some(RowClass::RootGroup)
        );
    });
    assert_eq!(session.stats().fast_acks_downgraded, 0);
}

/// The same for a namespace op on the fast path: `EIO`, not `Ok`, when the
/// re-tag fails. Fails on the parent: the create was answered `Ok`.
#[test]
fn a_failed_retag_fails_the_fast_path_op() {
    use constellation_meta::RowClass;
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let session = meta.session();
    session.set_durable(
        true,
        meta.journal_tip().unwrap(),
        false,
        RowClass::RootGroup,
    );
    let (fs, _dir, _core) = holder_fs(meta.clone(), true);
    let ino = meta.allocate_ino(ROOT_INO).unwrap();
    std::thread::scope(|scope| {
        let op = scope.spawn(|| {
            constellation_meta::fail_reclass_on_this_thread_for_tests(1);
            let r = fs.mutate_op(
                ROOT_INO,
                constellation_meta::MutateOp::Create {
                    parent: ROOT_INO,
                    name: "c".into(),
                    ino,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                },
            );
            constellation_meta::fail_reclass_on_this_thread_for_tests(0);
            r
        });
        let tip = journaled_past_durable(&meta);
        std::thread::sleep(Duration::from_millis(100));
        assert!(!op.is_finished(), "the op waits for the group");
        session.set_durable(false, u64::MAX, false, RowClass::Lone);
        assert_eq!(
            op.join().unwrap(),
            Err(Code::Io),
            "answered on a row whose class could not be recorded"
        );
        assert_eq!(
            meta.journal_tx_class(tip).unwrap(),
            Some(RowClass::RootGroup)
        );
    });
    assert_eq!(session.stats().fast_acks_downgraded, 0);
}

/// Plan 43 C1-2: the fast path's wait follows the class the row was
/// journaled under, not the gate as it stands after the execution: a row
/// journaled `Lone` (the gate was off) is answered at once, whatever the
/// lease view says by then.
#[test]
fn the_fast_path_waits_by_the_rows_own_class() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    // The view says gated (a stale mirror, a gate that came on a moment
    // ago); the published class is the lone node's: the row is `Lone`.
    let (fs, _dir, _core) = holder_fs(meta.clone(), true);
    fs.do_write(file.ino, 0, b"hello").unwrap();
    fs.flush_inode(file.ino, false).unwrap();
    assert_eq!(meta.session().stats().fast_acks_waited, 0);
    assert_eq!(meta.session().stats().fast_acks_in_doubt, 0);
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

/// What [`ops_against`] runs, in order.
#[derive(Clone, Copy)]
enum SyncOp {
    Fsync,
    /// A write on an `O_SYNC` descriptor.
    SyncWrite,
}

/// Plan 39 §3.2: `fsyncs` `fsync`s drive the scripted core's drain
/// replies — each failure classified as `class` — and the drains they
/// asked for.
fn fsync_against(
    replies: Vec<Result<(), crate::sync::ErrorClass>>,
    fsyncs: usize,
) -> (Vec<Result<(), Code>>, usize) {
    ops_against(replies, &vec![SyncOp::Fsync; fsyncs])
}

fn ops_against(
    replies: Vec<Result<(), crate::sync::ErrorClass>>,
    ops: &[SyncOp],
) -> (Vec<Result<(), Code>>, usize) {
    use constellation_vfs::{Blocking, Caller, OpCtx, OpKind, OpenFlags, Vfs, WriteData};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (fs, _dir, mut core) = holder_fs(meta, false);
    fs.do_write(file.ino, 0, b"must reach the bucket").unwrap();
    let caller = Caller::new(0, 0, None);
    let mut drains = 0;
    let mut results = Vec::new();
    std::thread::scope(|scope| {
        let fsync = scope.spawn(|| {
            ops.iter()
                .map(|op| match op {
                    SyncOp::Fsync => Blocking::run(|r| {
                        fs.fsync(
                            &OpCtx::new(OpKind::Fsync, &caller),
                            file.ino,
                            constellation_vfs::Fh(0),
                            constellation_vfs::Durability::Configured,
                            r,
                        )
                    })
                    .map_err(|e| e.code()),
                    SyncOp::SyncWrite => Blocking::run(|r| {
                        fs.write(
                            &OpCtx::new(OpKind::Write, &caller),
                            file.ino,
                            constellation_vfs::Fh(0),
                            0,
                            WriteData::Borrowed(b"must reach"),
                            OpenFlags::WRITE | OpenFlags::SYNC,
                            r,
                        )
                    })
                    .map(|_| ())
                    .map_err(|e| e.code()),
                })
                .collect::<Vec<_>>()
        });
        let mut replies = replies.into_iter();
        while !fsync.is_finished() {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { ino, reply, .. }) => {
                    assert_eq!(ino, file.ino);
                    drains += 1;
                    let answer = replies.next().expect("more drains than scripted");
                    let _ = reply.send(answer.map_err(|class| crate::sync::SyncFailure {
                        class,
                        message: format!("scripted {} failure", class.as_str()),
                    }));
                }
                Ok(_) => {}
                Err(_) => std::thread::sleep(Duration::from_millis(2)),
            }
        }
        results = fsync.join().unwrap();
    });
    (results, drains)
}

/// The `hard` contract on the drain: a transient failure is retried — the
/// retry drains again although the first attempt already published the
/// write session — and the `fsync` returns 0 only once a drain succeeded.
#[test]
fn an_fsync_retries_a_transient_drain_failure_until_it_succeeds() {
    use crate::sync::ErrorClass::Transient;
    let (results, drains) = fsync_against(vec![Err(Transient), Err(Transient), Ok(())], 1);
    assert_eq!(results, vec![Ok(())]);
    assert_eq!(drains, 3, "every retry drains; only the last one succeeded");
}

/// A permanent failure is `EIO` at once, and the next `fsync` — with
/// nothing new written — still waits for the chunks the failed one could
/// not upload: never a false success.
#[test]
fn a_permanent_drain_failure_is_eio_and_the_next_fsync_drains_again() {
    use crate::sync::ErrorClass::Permanent;
    let (results, drains) = fsync_against(vec![Err(Permanent), Err(Permanent), Ok(())], 3);
    assert_eq!(results, vec![Err(Code::Io), Err(Code::Io), Ok(())]);
    assert_eq!(drains, 3);
}

/// The same for an `O_SYNC` write that fails: it publishes the write
/// session, so the `fsync` after it finds none — and under `--fsync-mode
/// local` would skip the drain and answer 0 for chunks still only on this
/// node — unless the failed write left the inode owed, as a failed
/// `fsync` does.
#[test]
fn a_failed_o_sync_write_leaves_the_next_fsync_draining_again() {
    use crate::sync::ErrorClass::Permanent;
    let (results, drains) = ops_against(
        vec![Err(Permanent), Err(Permanent), Ok(())],
        &[SyncOp::SyncWrite, SyncOp::Fsync, SyncOp::Fsync],
    );
    assert_eq!(results, vec![Err(Code::Io), Err(Code::Io), Ok(())]);
    assert_eq!(drains, 3, "each fsync after the failed write drained");
}

/// Plan 39 §3.7 through the real fence: data written under a cluster-lock
/// grant that then lapses is discarded at the first publication point
/// (`lock_publish_gate`), and every open file description that was open
/// then reports `EIO` exactly once — the descriptor whose `fsync` found
/// the discard, and a second one at its own next `fsync` (before, the
/// inode's one-shot "owed" flag let the second `fsync` return 0 for
/// thrown-away data). A descriptor opened after the discard owes nothing.
#[test]
fn a_lapsed_grants_discard_is_eio_once_on_every_descriptor_open_at_the_time() {
    use constellation_meta::locks::{GrantId, HeldGrant, LockMode};
    use constellation_vfs::{Blocking, Caller, Fh, OpCtx, OpKind, OpenFlags, OpenOwner, Vfs};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (mut fs, _dir, mut core) = holder_fs(meta.clone(), false);
    {
        let h = fs.sync.as_mut().unwrap();
        h.locks = Some(Arc::new(crate::locks::ClusterLocks {
            meta: meta.clone(),
            tx: h.tx.clone(),
            inval: None,
            lineage: Default::default(),
            owner_wait_max: std::time::Duration::from_secs(50),
            poll_hint: std::time::Duration::from_millis(50),
            waits: Default::default(),
            orphans: Default::default(),
        }));
    }
    let caller = Caller::new(0, 0, None);
    let open = || {
        Blocking::run(|r| {
            fs.open(
                &OpCtx::new(OpKind::Open, &caller),
                file.ino,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap()
        .fh
    };
    let fsync = |fh: Fh| {
        Blocking::run(|r| {
            fs.fsync(
                &OpCtx::new(OpKind::Fsync, &caller),
                file.ino,
                fh,
                constellation_vfs::Durability::Configured,
                r,
            )
        })
        .map_err(|e| e.code())
    };
    let (first, second) = (open(), open());
    // An exclusive grant, about to lapse without its release's flush.
    let now = crate::locks::now().honour;
    meta.locks().install_held(
        file.ino,
        HeldGrant {
            id: GrantId { node: 1, seq: 1 },
            mode: LockMode::Exclusive,
            until: now.plus(300),
            renew_at: now.plus(150),
            owner: 1,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: now,
            lapsing: None,
            written: false,
            route: None,
        },
    );
    Blocking::run(|r| {
        fs.write(
            &OpCtx::new(OpKind::Write, &caller),
            file.ino,
            first,
            0,
            constellation_vfs::WriteData::Borrowed(b"written under the grant"),
            OpenFlags::WRITE,
            r,
        )
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        // The core: every drain succeeds (nothing is left to drain).
        scope.spawn(|| {
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                match core.try_recv() {
                    Ok(SyncRequest::DrainInode { reply, .. }) => {
                        let _ = reply.send(Ok(()));
                    }
                    Ok(_) => {}
                    Err(_) => std::thread::sleep(Duration::from_millis(2)),
                }
            }
        });
        assert_eq!(fsync(first), Err(Code::Io), "the fence discards");
        let later = open();
        assert_eq!(fsync(second), Err(Code::Io), "a second descriptor");
        assert_eq!(fsync(second), Ok(()), "reported once");
        assert_eq!(fsync(first), Ok(()), "the first reported it itself");
        assert_eq!(fsync(later), Ok(()), "opened after the discard");
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    assert_eq!(
        meta.getattr(file.ino).unwrap().unwrap().size,
        0,
        "nothing written under the lapsed grant was published"
    );
}

/// Plan 39b, scripted: one `fsync` of `file` (already written as the
/// caller left it) against a core that answers every drain `Ok` and
/// counts them.
fn fsync_counting_drains(
    fs: &View,
    core: &mut tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
    ino: Ino,
) -> (Result<(), Code>, usize) {
    use constellation_vfs::{Blocking, Caller, OpCtx, OpKind, Vfs};
    let caller = Caller::new(0, 0, None);
    let mut drains = 0;
    let mut result = Ok(());
    std::thread::scope(|scope| {
        let fsync = scope.spawn(|| {
            Blocking::run(|r| {
                fs.fsync(
                    &OpCtx::new(OpKind::Fsync, &caller),
                    ino,
                    constellation_vfs::Fh(0),
                    constellation_vfs::Durability::Configured,
                    r,
                )
            })
            .map_err(|e| e.code())
        });
        while !fsync.is_finished() {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode {
                    ino: drained,
                    reply,
                    ..
                }) => {
                    assert_eq!(drained, ino);
                    drains += 1;
                    let _ = reply.send(Ok(()));
                }
                Ok(_) => {}
                Err(_) => std::thread::sleep(Duration::from_millis(2)),
            }
        }
        result = fsync.join().unwrap();
    });
    (result, drains)
}

/// Plan 39b: an `fsync` of a file whose chunks are all up — nothing of it
/// in `pending_upload` — asks the sync task for no drain at all, however
/// the file got there (here: a `back` close whose upload has since
/// finished).
#[test]
fn an_fsync_with_nothing_pending_asks_for_no_drain() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (fs, _dir, mut core) = holder_fs(meta.clone(), false);
    fs.do_write(file.ino, 0, b"closed under back").unwrap();
    fs.flush_inode(file.ino, false).unwrap();
    let rows: Vec<_> = meta
        .pending_uploads()
        .unwrap()
        .into_iter()
        .filter(|(_, i)| *i == file.ino)
        .collect();
    assert!(!rows.is_empty(), "the back close queued its chunk");
    // The background upload went through.
    for (hash, ino) in rows {
        meta.ack_upload(&hash, ino).unwrap();
    }
    let (result, drains) = fsync_counting_drains(&fs, &mut core, file.ino);
    assert_eq!(result, Ok(()));
    assert_eq!(drains, 0, "nothing pending: no drain");
}

/// Plan 39b: a `back` close's queued chunks are drained by the next
/// `fsync` of the file under `--fsync-mode local` (no write session left
/// to publish), and so they are inside a continuation epoch: the epoch
/// exempts a `close()`, never a barrier (before, both returned 0 with the
/// chunks only on this node).
#[test]
fn an_fsync_drains_a_back_closes_chunks_in_and_out_of_an_epoch() {
    for epoch in [false, true] {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let (mut fs, _dir, mut core) = holder_fs(meta.clone(), false);
        fs.sync.as_mut().unwrap().epoch_active =
            Some(Arc::new(std::sync::atomic::AtomicBool::new(epoch)));
        fs.do_write(file.ino, 0, b"closed under back").unwrap();
        fs.flush_inode(file.ino, false).unwrap();
        let (result, drains) = fsync_counting_drains(&fs, &mut core, file.ino);
        assert_eq!(result, Ok(()), "epoch={epoch}");
        assert_eq!(
            drains, 1,
            "epoch={epoch}: the fsync drained the queued chunk"
        );
    }
}

/// The loser of a create race (harness `concurrent-create-no-excl`):
/// `ino` is the winner's file, which this replica knows only through the
/// `Exists` hint; the replica wrote `b` at offset 1, and then a later
/// epoch's segment stranded the hint (the answering tenure ended before
/// shipping it), so the inode is gone here. `fs` is not the holder.
fn hinted_and_lost(
    meta: &Arc<Meta>,
    ino: Ino,
) -> (
    View,
    TempDir,
    tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
) {
    let hinted = meta
        .install_hint(
            &[constellation_meta::LogRecord::Create {
                parent: ROOT_INO,
                name: "p19".into(),
                ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            }],
            constellation_meta::Position {
                seq: 0,
                pending: Some(constellation_meta::JournalPos { epoch: 1, jseq: 5 }),
                ..constellation_meta::Position::ZERO
            },
            1,
        )
        .unwrap();
    assert!(hinted);
    let (mut fs, dir, core) = holder_fs(meta.clone(), false);
    // Not the holder: the close forwards.
    fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
    fs.do_write(ino, 1, b"b").unwrap();
    // A later epoch's segment: the hint strands, the inode is gone here.
    meta.apply_segment(1, 2, &[], &constellation_meta::TouchSet::default())
        .unwrap();
    assert!(meta.getattr(ino).unwrap().is_none());
    (fs, dir, core)
}

/// Close `ino` on `fs`, the sequencer answering its forward with
/// `answer`: the close's result and the op it forwarded.
fn close_answered(
    fs: &View,
    core: &mut tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
    ino: Ino,
    answer: constellation_meta::MutateOutcome,
) -> (Result<(), Code>, Option<constellation_meta::MutateOp>) {
    let mut forwarded = None;
    let mut answer = Some(answer);
    let result = std::thread::scope(|scope| {
        let close = scope.spawn(|| fs.flush_inode(ino, false));
        let started = std::time::Instant::now();
        loop {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { reply, .. }) => reply.send(Ok(())).unwrap(),
                Ok(SyncRequest::Submit { op, reply, .. }) => {
                    forwarded = Some(op);
                    reply
                        .send(constellation_authority::ClientReply::Outcome(
                            answer.take().expect("one forward"),
                        ))
                        .unwrap();
                }
                Ok(_) => {}
                Err(_) if close.is_finished() => break,
                Err(_) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(10),
                        "the close is stuck"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        close.join().unwrap()
    });
    (result, forwarded)
}

/// Harness `concurrent-create-no-excl`'s lost byte: the hint is rolled
/// back before the loser's close. The close must not take an inode the
/// replica merely lacks for an unlinked one — that answered success,
/// published nothing, and the last close dropped the bytes. It asks the
/// sequencer, which has the file.
#[test]
fn a_close_of_a_file_the_replica_lost_asks_the_sequencer() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let ino = (7 << 40) | 19;
    let (fs, _dir, mut core) = hinted_and_lost(&meta, ino);
    let (result, forwarded) = close_answered(
        &fs,
        &mut core,
        ino,
        constellation_meta::MutateOutcome::Accepted {
            epoch: 2,
            records: Vec::new(),
        },
    );
    result.unwrap();
    match forwarded {
        Some(constellation_meta::MutateOp::SetManifest { ino: i, size, .. }) => {
            assert_eq!((i, size), (ino, 2), "the write went to the sequencer");
        }
        other => panic!("the close published nothing: {other:?}"),
    }
}

/// The takeover window: the hint is rolled back because the create it
/// carried was stranded with the old holder's tenure, so the new holder
/// has no such file either and answers the forwarded manifest
/// `NotFound`. The close must fail — a close that answered 0 must never
/// have lost its write — and the bytes stay for a later close to retry.
#[test]
fn a_close_the_sequencer_answers_not_found_fails_and_keeps_the_bytes() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let ino = (7 << 40) | 19;
    let (fs, _dir, mut core) = hinted_and_lost(&meta, ino);
    let (result, forwarded) = close_answered(
        &fs,
        &mut core,
        ino,
        constellation_meta::MutateOutcome::Errno(Code::NotFound),
    );
    assert!(
        matches!(
            forwarded,
            Some(constellation_meta::MutateOp::SetManifest { ino: i, .. }) if i == ino
        ),
        "the close asked the sequencer: {forwarded:?}"
    );
    assert_eq!(
        result,
        Err(Code::Stale),
        "nothing was written: not a success"
    );
    let writes = fs.writes.lock(ino);
    assert_eq!(
        fs.writes.pending_len(&writes, ino),
        Some(2),
        "the session stays for a retry"
    );
}

/// stress-ng-fs-nodes: a non-holder's `fallocate` (allocate, keep-size,
/// punch, zero-range) changes its write state like a write or a truncation
/// and never asks the core for the lease — before, it waited out a busy
/// holder for 2xTTL and failed with EIO. The new size and the zeroed
/// ranges are this session's until the close forwards them.
#[test]
fn a_nonowner_fallocate_never_asks_for_the_lease() {
    use constellation_vfs::{Blocking, FallocateMode, Fh, OpCtx, OpKind, Vfs};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (mut fs, _dir, mut core) = holder_fs(meta.clone(), false);
    // Not the holder: the fast path is closed.
    fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
    fs.do_write(file.ino, 0, &[7u8; 4096]).unwrap();
    let caller = constellation_vfs::Caller::root();
    let falloc = |off: u64, len: u64, mode: FallocateMode| {
        Blocking::run(|r| {
            fs.fallocate(
                &OpCtx::new(OpKind::Fallocate, &caller),
                file.ino,
                Fh(0),
                off,
                len,
                mode,
                r,
            )
        })
    };
    std::thread::scope(|scope| {
        let ops = scope.spawn(|| {
            falloc(0, 1 << 20, FallocateMode::empty())?;
            falloc(1 << 20, 1 << 20, FallocateMode::KEEP_SIZE)?;
            falloc(
                1024,
                1024,
                FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
            )?;
            falloc(2048, 1024, FallocateMode::ZERO_RANGE)
        });
        let started = std::time::Instant::now();
        while !ops.is_finished() {
            if let Ok(req) = core.try_recv() {
                assert!(
                    !matches!(req, SyncRequest::Acquire { .. }),
                    "fallocate asked for the lease"
                );
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "fallocate is stuck"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        ops.join().unwrap().unwrap();
    });
    while let Ok(req) = core.try_recv() {
        assert!(!matches!(req, SyncRequest::Acquire { .. }));
    }
    let ws = fs.writes.lock(file.ino);
    assert_eq!(ws.get(&file.ino).unwrap().file_len, 1 << 20);
}

/// F1/F9: the fast path judges its lease again once the op's transaction
/// holds the store's writer lock. One that ran out while the op waited for
/// the lock (a slow writer ahead of it) journals nothing locally — a row a
/// takeover would strand and replay — and sends the op through the core
/// under the same rid, as an op refused at admission goes.
#[test]
fn a_lease_that_ran_out_before_the_write_sends_the_op_through_the_core() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (fs, _dir, mut core) = holder_fs(meta.clone(), false);
    let lease = fs.sync.as_ref().unwrap().lease.clone();
    let before = meta.journal_len().unwrap();
    let ino = meta.allocate_ino(ROOT_INO).unwrap();
    let mut submitted = 0;
    std::thread::scope(|scope| {
        let op = scope.spawn(|| {
            // Admitted; then, waiting for the writer lock, the lease runs
            // out (the core mirrors a lease it no longer holds).
            constellation_meta::set_before_write_hook_for_tests(Some(Box::new(move || {
                lease.mirror(
                    &LeaseState::default(),
                    Now::same(now_unix_ms()),
                    &Config::defaults(1, 1),
                    false,
                );
                true
            })));
            let r = fs.mutate_op(
                ROOT_INO,
                constellation_meta::MutateOp::Create {
                    parent: ROOT_INO,
                    name: "late".into(),
                    ino,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                },
            );
            constellation_meta::set_before_write_hook_for_tests(None);
            r
        });
        let started = std::time::Instant::now();
        loop {
            match core.try_recv() {
                Ok(SyncRequest::Submit { op, reply, .. }) => {
                    assert!(matches!(op, constellation_meta::MutateOp::Create { .. }));
                    assert_eq!(
                        meta.journal_len().unwrap(),
                        before,
                        "journaled locally under a lease that ran out"
                    );
                    submitted += 1;
                    reply
                        .send(constellation_authority::ClientReply::Outcome(
                            constellation_meta::MutateOutcome::Accepted {
                                epoch: 1,
                                records: Vec::new(),
                            },
                        ))
                        .unwrap();
                }
                Ok(_) => {}
                Err(_) if op.is_finished() => break,
                Err(_) => {
                    assert!(started.elapsed() < Duration::from_secs(10), "stuck");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        op.join().unwrap().unwrap();
    });
    assert_eq!(submitted, 1, "the op went through the core once");
    assert!(meta.lookup(ROOT_INO, "late").unwrap().is_none());
}

/// The view of a node the core has just reported deposed (`LEASE LOST`:
/// node 2 holds epoch 2): the mirror says `lost`, the fast path is
/// closed, and the recovery round has not run yet.
fn mirror_deposed(fs: &View) {
    let mut state = LeaseState::default();
    state.mark_lost(2, 2, 1);
    let lease = &fs.sync.as_ref().unwrap().lease;
    lease.mirror(
        &state,
        Now::same(now_unix_ms()),
        &Config::defaults(1, 1),
        false,
    );
    assert!(lease.is_lost());
    assert!(lease.admit().is_none(), "the fast path is closed");
}

/// `load-probe-takeover` under `pause:lock-lapse+500:10000@b`: a deposed
/// node's mutation admitted between `LEASE LOST` and the recovery round
/// (a replica rebuild from the log: 90 to 200 ms there, seconds on a slow
/// S3) was refused with `EIO` at the write gate. It goes through the
/// core, which forwards it to the new holder once the node's replays are
/// settled, like the one resubmitted in doubt and every one admitted
/// after the round.
#[test]
fn a_deposed_nodes_write_goes_through_the_core_not_eio() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (fs, _dir, mut core) = holder_fs(meta.clone(), false);
    mirror_deposed(&fs);
    let before = meta.journal_len().unwrap();
    let ino = meta.allocate_ino(ROOT_INO).unwrap();
    let mut submitted = 0;
    std::thread::scope(|scope| {
        let op = scope.spawn(|| {
            fs.mutate_op(
                ROOT_INO,
                constellation_meta::MutateOp::Create {
                    parent: ROOT_INO,
                    name: "deposed".into(),
                    ino,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                },
            )
        });
        let started = std::time::Instant::now();
        loop {
            match core.try_recv() {
                Ok(SyncRequest::Submit { op, reply, .. }) => {
                    assert!(matches!(op, constellation_meta::MutateOp::Create { .. }));
                    assert_eq!(
                        meta.journal_len().unwrap(),
                        before,
                        "journaled locally by a deposed node"
                    );
                    submitted += 1;
                    reply
                        .send(constellation_authority::ClientReply::Outcome(
                            constellation_meta::MutateOutcome::Accepted {
                                epoch: 2,
                                records: Vec::new(),
                            },
                        ))
                        .unwrap();
                }
                Ok(_) => {}
                Err(_) if op.is_finished() => break,
                Err(_) => {
                    assert!(started.elapsed() < Duration::from_secs(10), "stuck");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        op.join().unwrap().expect("not refused while deposed");
    });
    assert_eq!(submitted, 1, "the op went through the core once");
}

/// The same refusal met by the commit an `fsync` flushes: the manifest
/// commit of a deposed node is submitted to the core (which forwards it),
/// and the `fsync` returns once the core answers it.
#[test]
fn a_deposed_nodes_fsync_commits_through_the_core_not_eio() {
    use constellation_vfs::{Blocking, Caller, OpCtx, OpKind, Vfs};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let (fs, _dir, mut core) = holder_fs(meta.clone(), false);
    fs.do_write(file.ino, 0, b"must reach the bucket").unwrap();
    mirror_deposed(&fs);
    let caller = Caller::new(0, 0, None);
    let mut commits = 0;
    let mut result = None;
    std::thread::scope(|scope| {
        let fsync = scope.spawn(|| {
            Blocking::run(|r| {
                fs.fsync(
                    &OpCtx::new(OpKind::Fsync, &caller),
                    file.ino,
                    constellation_vfs::Fh(0),
                    constellation_vfs::Durability::Configured,
                    r,
                )
            })
            .map_err(|e| e.code())
        });
        let started = std::time::Instant::now();
        while !fsync.is_finished() {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { reply, .. }) => {
                    let _ = reply.send(Ok(()));
                }
                Ok(SyncRequest::Submit { op, reply, .. }) => {
                    assert!(
                        matches!(op, constellation_meta::MutateOp::SetManifest { .. }),
                        "{op:?}"
                    );
                    commits += 1;
                    reply
                        .send(constellation_authority::ClientReply::Outcome(
                            constellation_meta::MutateOutcome::Accepted {
                                epoch: 2,
                                records: Vec::new(),
                            },
                        ))
                        .unwrap();
                }
                Ok(_) => {}
                Err(_) => {
                    assert!(started.elapsed() < Duration::from_secs(10), "stuck");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        result = Some(fsync.join().unwrap());
    });
    assert_eq!(commits, 1, "the commit went through the core once");
    assert_eq!(result, Some(Ok(())));
}

// ---- plan 43 C2d: the delegate's fast path writes, then waits ----

const DELEG_GEN: u64 = 7;

/// A delegate's filesystem: `holder_fs` for node 1, with a directory
/// delegated to it as generation [`DELEG_GEN`], the fast path's view of
/// that delegation honoured, and the core's publication of the class
/// (`class`) and the committed stream index (`through`) for it. Returns
/// the directory too.
fn delegate_fs(
    class: constellation_meta::RowClass,
    through: u64,
) -> (
    View,
    Arc<Meta>,
    Ino,
    TempDir,
    tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
) {
    use constellation_meta::LogRecord;
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir: Ino = (1 << 40) | 9;
    meta.apply_records(&[
        LogRecord::Mkdir {
            parent: ROOT_INO,
            name: "d".into(),
            ino: dir,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        },
        LogRecord::Delegate {
            dir,
            node: 1,
            gen: DELEG_GEN,
            designated: false,
            range: (0, 0),
        },
    ])
    .unwrap();
    let (fs, tmp, core) = holder_fs(meta.clone(), false);
    fs.sync
        .as_ref()
        .unwrap()
        .delegates
        .mirror(&constellation_authority::core::DelegView {
            mine: vec![(dir, DELEG_GEN, i64::MAX, false, 0, 0, 0)],
            ..Default::default()
        });
    meta.session().set_deleg_durable(
        DELEG_GEN,
        Some(constellation_meta::DelegDurable {
            class,
            through,
            lost: false,
        }),
    );
    (fs, meta, dir, tmp, core)
}

fn delegate_create(dir: Ino, seq: u64) -> (constellation_meta::MutateOp, constellation_meta::Rid) {
    (
        constellation_meta::MutateOp::Create {
            parent: dir,
            name: format!("f{seq}"),
            ino: (1 << 40) | (100 + seq),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq,
        },
    )
}

/// Waits until the delegate has journaled its `n`th row.
fn journaled_stream_row(meta: &Meta, n: u64) {
    let started = std::time::Instant::now();
    while meta.delegate_idx(DELEG_GEN).unwrap() < n {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never journaled"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A stand-in for the core that answers whatever the write sends it with
/// "in doubt" (for at most 20 s), counting the submissions: a write that
/// should stay on the fast path never sends one, and one that does not
/// (the interim gate of C2c) fails the test instead of hanging it.
fn core_answering_in_doubt(
    core: &mut tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
    stop: &std::sync::atomic::AtomicBool,
    submitted: &Mutex<Vec<(constellation_meta::Rid, bool)>>,
) {
    use std::sync::atomic::Ordering;
    let started = std::time::Instant::now();
    while !stop.load(Ordering::Relaxed) && started.elapsed() < Duration::from_secs(20) {
        match core.try_recv() {
            Ok(SyncRequest::Submit {
                reply,
                rid,
                in_doubt,
                ..
            }) => {
                submitted.lock().unwrap().push((rid, in_doubt));
                let _ = reply.send(constellation_authority::ClientReply::InDoubt);
            }
            Ok(_) => {}
            Err(_) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
}

/// Plan 43 C2d: with a class that waits, the delegate's fast path
/// journals the row with it and returns only once the core's watermark
/// covers the row. The interim gate (C2c) closed the path whenever the
/// class waited: the write went through the core and parked there.
#[test]
fn the_delegates_fast_path_writes_then_waits_for_its_group() {
    use constellation_meta::RowClass;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (fs, meta, dir, _tmp, mut core) = delegate_fs(RowClass::DelegGroup, 0);
    let h = fs.sync.as_ref().unwrap();
    let (op, rid) = delegate_create(dir, 1);
    let (stop, submitted) = (AtomicBool::new(false), Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        let responder = scope.spawn(|| core_answering_in_doubt(&mut core, &stop, &submitted));
        let write = scope.spawn(|| fs.mutate_op_rebasable_with_rid(h, dir, &op, rid));
        let journaled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            journaled_stream_row(&meta, 1);
            // Journaled on this thread, under the published class.
            assert_eq!(
                meta.journal_tx_class(meta.journal_tip().unwrap()).unwrap(),
                Some(RowClass::DelegGroup)
            );
            std::thread::sleep(Duration::from_millis(100));
            assert!(!write.is_finished(), "the write waits for its group");
            // The group holds the stream through the row.
            meta.session().set_deleg_durable(
                DELEG_GEN,
                Some(constellation_meta::DelegDurable {
                    class: RowClass::DelegGroup,
                    through: 1,
                    lost: false,
                }),
            );
        }));
        let result = write.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        responder.join().unwrap();
        if let Err(e) = journaled {
            std::panic::resume_unwind(e);
        }
        assert!(result.is_ok());
    });
    // It never went through the core: only the stream's nudge.
    assert!(
        submitted.lock().unwrap().is_empty(),
        "the write went through the core"
    );
    let stats = meta.session().stats();
    assert_eq!((stats.fast_acks_waited, stats.fast_acks_in_doubt), (1, 0));
    assert_eq!(h.delegates.executed(), 1);
}

/// A row the core says is committed already (the watermark covers it by
/// the time the write is done) is answered without a wait; a class that
/// does not wait (`Lone`) never looks at the watermark.
#[test]
fn the_delegates_fast_path_answers_a_lone_row_and_a_committed_row_at_once() {
    use constellation_meta::RowClass;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (fs, meta, dir, _tmp, mut core) = delegate_fs(RowClass::Lone, 0);
    let h = fs.sync.as_ref().unwrap();
    let (stop, submitted) = (AtomicBool::new(false), Mutex::new(Vec::new()));
    let (first, second) = std::thread::scope(|scope| {
        let responder = scope.spawn(|| core_answering_in_doubt(&mut core, &stop, &submitted));
        let (op, rid) = delegate_create(dir, 1);
        let first = fs.mutate_op_rebasable_with_rid(h, dir, &op, rid).is_ok();
        assert_eq!(
            meta.journal_tx_class(meta.journal_tip().unwrap()).unwrap(),
            Some(RowClass::Lone)
        );
        assert_eq!(meta.session().stats().fast_acks_waited, 0);
        // A group class with the watermark past the row: answered, counted.
        meta.session().set_deleg_durable(
            DELEG_GEN,
            Some(constellation_meta::DelegDurable {
                class: RowClass::DelegGroup,
                through: u64::MAX,
                lost: false,
            }),
        );
        let (op, rid) = delegate_create(dir, 2);
        let second = fs.mutate_op_rebasable_with_rid(h, dir, &op, rid).is_ok();
        stop.store(true, Ordering::Relaxed);
        responder.join().unwrap();
        (first, second)
    });
    assert!(first && second);
    assert!(
        submitted.lock().unwrap().is_empty(),
        "went through the core"
    );
    assert_eq!(
        meta.journal_tx_class(meta.journal_tip().unwrap()).unwrap(),
        Some(RowClass::DelegGroup)
    );
    assert_eq!(meta.session().stats().fast_acks_waited, 1);
    assert_eq!(h.delegates.executed(), 2);
}

/// A generation the core stops publishing (ended here), or one it marks
/// lost, is not an answer: the op is in doubt and goes back through the
/// core under its rid, which answers from the row's commit.
#[test]
fn the_delegates_fast_path_resubmits_in_doubt_when_its_generation_is_lost() {
    use constellation_meta::RowClass;
    use std::sync::atomic::{AtomicBool, Ordering};
    for withdraw in [false, true] {
        let (fs, meta, dir, _tmp, mut core) = delegate_fs(RowClass::DelegGroup, 0);
        let h = fs.sync.as_ref().unwrap();
        let (op, rid) = delegate_create(dir, 1);
        let (stop, submitted) = (AtomicBool::new(false), Mutex::new(Vec::new()));
        let result = std::thread::scope(|scope| {
            let responder = scope.spawn(|| core_answering_in_doubt(&mut core, &stop, &submitted));
            let write = scope.spawn(|| fs.mutate_op_rebasable_with_rid(h, dir, &op, rid));
            let waiting = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                journaled_stream_row(&meta, 1);
                std::thread::sleep(Duration::from_millis(50));
                assert!(!write.is_finished());
                if withdraw {
                    meta.session().set_deleg_durable(DELEG_GEN, None);
                } else {
                    meta.session().set_deleg_durable(
                        DELEG_GEN,
                        Some(constellation_meta::DelegDurable {
                            class: RowClass::DelegGroup,
                            through: 0,
                            lost: true,
                        }),
                    );
                }
            }));
            let result = write.join().unwrap();
            stop.store(true, Ordering::Relaxed);
            responder.join().unwrap();
            if let Err(e) = waiting {
                std::panic::resume_unwind(e);
            }
            result
        });
        assert_eq!(
            *submitted.lock().unwrap(),
            vec![(rid, true)],
            "resubmitted once, in doubt"
        );
        assert!(result.is_err(), "not answered OK");
        assert_eq!(meta.session().stats().fast_acks_in_doubt, 1);
    }
}

/// Review claim 1 (fail-before): the row's class is decided when it is
/// journaled (plan 43 §5.1), not at the admission. A write admitted while
/// the core published `Lone`, then waiting for the writer lock while the
/// core publishes `DelegGroup` (a backup candidate became stable), must be
/// journaled `DelegGroup` and wait for its group.
#[test]
fn a_delegates_row_is_classed_when_it_is_journaled_not_when_admitted() {
    use constellation_meta::RowClass;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (fs, meta, dir, _tmp, mut core) = delegate_fs(RowClass::Lone, 0);
    let h = fs.sync.as_ref().unwrap();
    let (op, rid) = delegate_create(dir, 1);
    let (stop, submitted) = (AtomicBool::new(false), Mutex::new(Vec::new()));
    let hook_ran = std::sync::Arc::new(AtomicBool::new(false));
    let (class, waited) = std::thread::scope(|scope| {
        let responder = scope.spawn(|| core_answering_in_doubt(&mut core, &stop, &submitted));
        let write = scope.spawn(|| {
            // Admitted under `Lone`; while it waits for the writer lock the
            // core publishes the group's class.
            let m = meta.clone();
            let ran = hook_ran.clone();
            constellation_meta::set_before_write_hook_for_tests(Some(Box::new(move || {
                ran.store(true, Ordering::Relaxed);
                m.session().set_deleg_durable(
                    DELEG_GEN,
                    Some(constellation_meta::DelegDurable {
                        class: RowClass::DelegGroup,
                        through: 0,
                        lost: false,
                    }),
                );
                true
            })));
            let r = fs.mutate_op_rebasable_with_rid(h, dir, &op, rid);
            constellation_meta::set_before_write_hook_for_tests(None);
            r
        });
        journaled_stream_row(&meta, 1);
        let class = meta.journal_tx_class(meta.journal_tip().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let waited = !write.is_finished();
        // The group holds the stream through the row.
        meta.session().set_deleg_durable(
            DELEG_GEN,
            Some(constellation_meta::DelegDurable {
                class: RowClass::DelegGroup,
                through: 1,
                lost: false,
            }),
        );
        let _ = write.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        responder.join().unwrap();
        (class, waited)
    });
    assert!(hook_ran.load(Ordering::Relaxed), "the hook never ran");
    assert!(
        class == Some(RowClass::DelegGroup) && waited,
        "the core published DelegGroup before the row was journaled: journaled class {class:?}, \
         write waited for its group: {waited}"
    );
}
