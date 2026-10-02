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
    let now = now_unix_ms();
    meta.locks().install_held(
        file.ino,
        HeldGrant {
            id: GrantId { node: 1, seq: 1 },
            mode: LockMode::Exclusive,
            until_ms: now + 300,
            renew_at_ms: now + 150,
            owner: 1,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
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
