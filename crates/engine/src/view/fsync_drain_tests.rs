//! Plan 39b: an `fsync` drains every chunk of the file still pending
//! upload on this node, whoever queued it — in both `--fsync-mode`s. The
//! Linux `fsync(2)` contract ("all modified in-core data of the file"),
//! the one a PostgreSQL checkpointer relies on: backends write and close,
//! the checkpointer later opens the file and `fsync`s it.
//!
//! One real engine on a `file://` backend, its background uploads held
//! (`UploadHold`, plan 31 C8: an explicit durability request is never
//! held), so a chunk is in the bucket only if the `fsync` put it there.
use crate::{Engine, EngineConfig, EngineProfile};
use constellation_fs_core::ChunkHash;
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_vfs::{
    Blocking, Caller, Durability, FrontendCaps, Name, OpCtx, OpKind, OpenFlags, OpenOwner, Vfs,
    WriteData,
};
use std::sync::Arc;

struct Node {
    engine: Arc<Engine>,
    rt: tokio::runtime::Runtime,
    _dir: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.engine.shutdown();
    }
}

fn node(fsync_s3: bool, write_mode: crate::writeback::WriteMode) -> Node {
    node_with(fsync_s3, write_mode, Some(None))
}

/// [`node`] with `--fsync-timeout` (`EngineConfig::fsync_timeout`).
fn node_with(
    fsync_s3: bool,
    write_mode: crate::writeback::WriteMode,
    fsync_timeout: Option<Option<std::time::Duration>>,
) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let backend = format!("file://{}", dir.path().join("backend").display());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let store = ChunkStore::new(
        rt.block_on(crate::backend::open_backend(&backend))
            .expect("open backend"),
    );
    rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
        .expect("create_fs");
    let engine = Engine::start(
        EngineConfig {
            state_dir: Some(dir.path().join("state")),
            cache_size: 64 * 1024 * 1024,
            runtime: Some(rt.handle().clone()),
            fsync_s3,
            fsync_timeout,
            initial_write_mode: write_mode,
            ..EngineConfig::new(&backend)
        },
        HostServices::native(),
        EngineProfile {
            p2p: crate::P2pMode::Off,
            ..EngineProfile::desktop()
        },
    )
    .expect("Engine::start");
    Node {
        engine: Arc::new(engine),
        rt,
        _dir: dir,
    }
}

fn view(node: &Node) -> Arc<super::View> {
    node.engine
        .open_view(
            crate::ViewSpec::new("/"),
            FrontendCaps::linux_fuse(false),
            crate::DeferredEvents::new(),
        )
        .expect("open_view")
}

fn cx(kind: OpKind, caller: &Caller) -> OpCtx<'_> {
    OpCtx::new(kind, caller)
}

/// Create `name`, write `data` and close it — no `fsync` — as one process
/// (`writer`) on `view`.
fn write_and_close(view: &super::View, writer: &Caller, name: &str, data: &[u8]) -> u64 {
    let root = view.view_root();
    let (entry, opened) = Blocking::run(|r| {
        view.create(
            &cx(OpKind::Create, writer),
            root,
            Name::new(name),
            0o100644,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    let ino = entry.attr.ino;
    Blocking::run(|r| {
        view.write(
            &cx(OpKind::Write, writer),
            ino,
            opened.fh,
            0,
            WriteData::Borrowed(data),
            OpenFlags::WRITE,
            r,
        )
    })
    .unwrap();
    Blocking::run(|r| {
        view.flush(
            &cx(OpKind::Flush, writer),
            ino,
            opened.fh,
            constellation_vfs::LockOwner(0),
            r,
        )
    })
    .unwrap();
    Blocking::run(|r| {
        view.release(
            &cx(OpKind::Release, writer),
            ino,
            opened.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    ino
}

/// Open `ino` read-only on `view` as `caller`, `fsync` it, close it.
fn open_fsync_close(view: &super::View, caller: &Caller, ino: u64) {
    try_open_fsync_close(view, caller, ino).expect("fsync");
}

/// [`open_fsync_close`], answering what the `fsync` answered.
fn try_open_fsync_close(
    view: &super::View,
    caller: &Caller,
    ino: u64,
) -> Result<(), constellation_types::Code> {
    let fh = Blocking::run(|r| {
        view.open(
            &cx(OpKind::Open, caller),
            ino,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap()
    .fh;
    let result = Blocking::run(|r| {
        view.fsync(
            &cx(OpKind::Fsync, caller),
            ino,
            fh,
            Durability::Configured,
            r,
        )
    })
    .map_err(|e| e.code());
    Blocking::run(|r| {
        view.release(
            &cx(OpKind::Release, caller),
            ino,
            fh,
            OpenFlags::READ,
            None,
            r,
        )
    })
    .unwrap();
    result
}

fn pending_of(node: &Node, ino: u64) -> Vec<ChunkHash> {
    node.engine
        .meta()
        .pending_uploads()
        .unwrap()
        .into_iter()
        .filter(|(_, i)| *i == ino)
        .map(|(h, _)| h)
        .collect()
}

fn in_bucket(node: &Node, hash: &ChunkHash) -> bool {
    node.rt
        .block_on(node.engine.store().chunk_durable(hash))
        .unwrap()
}

/// Bytes that do not repeat at any chunk boundary (xorshift).
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// The checkpointer shape: one process writes the file and closes it —
/// under `--write-mode back` its chunks stay queued on this node (under
/// `--fsync-mode s3`, which forces write-through, the close uploads
/// them) — and one more chunk of the file is queued as a write-through
/// close that failed leaves it (cached dirty, a `pending_upload` row).
/// A different process on a different mount (view) of the node opens
/// the file and `fsync`s through its own descriptor. When that `fsync`
/// returns, every queued chunk is in the bucket and nothing of the file
/// is pending, in both `--fsync-mode`s. Before plan 39b, `local` returned
/// at once with the chunks only on this node.
fn fsync_from_another_descriptor_uploads_what_is_queued(fsync_s3: bool) {
    let node = node(fsync_s3, crate::writeback::WriteMode::Back);
    node.engine.upload().hold.set(true);
    let (writer_view, checkpointer_view) = (view(&node), view(&node));
    let backend = Caller::new(0, 0, Some(1001));
    let checkpointer = Caller::new(0, 0, Some(1002));
    // Three full chunks and a tail.
    let data = noise(39, 3 * 1024 * 1024 + 4096);
    let ino = write_and_close(&writer_view, &backend, "relation", &data);
    let closed = pending_of(&node, ino);
    if fsync_s3 {
        assert!(closed.is_empty(), "s3 forces write-through: {closed:?}");
    } else {
        assert_eq!(closed.len(), 4, "the back close left its chunks queued");
    }
    let left = noise(40, 64 * 1024);
    let left_hash = node.engine.store().hash(&left);
    node.engine
        .cache()
        .insert(
            &left_hash,
            &left,
            constellation_fs_core::cache::ChunkState::Dirty,
        )
        .unwrap();
    node.engine
        .meta()
        .add_pending_upload(&left_hash, ino)
        .unwrap();
    let queued = pending_of(&node, ino);
    assert!(
        queued.iter().all(|h| !in_bucket(&node, h)),
        "a held upload reached the bucket before any fsync"
    );

    open_fsync_close(&checkpointer_view, &checkpointer, ino);

    assert_eq!(
        pending_of(&node, ino),
        Vec::new(),
        "fsync_s3={fsync_s3}: the fsync returned with the file's chunks still pending"
    );
    for hash in &queued {
        assert!(
            in_bucket(&node, hash),
            "fsync_s3={fsync_s3}: chunk {hash} not in the bucket when the fsync returned"
        );
    }
}

#[test]
fn fsync_local_from_another_descriptor_uploads_what_is_queued() {
    fsync_from_another_descriptor_uploads_what_is_queued(false);
}

#[test]
fn fsync_s3_from_another_descriptor_uploads_what_is_queued() {
    fsync_from_another_descriptor_uploads_what_is_queued(true);
}

/// The cheap case: a file a write-through close already put in the
/// bucket has no pending row, so an `fsync` of it runs no upload pass and
/// attempts no PUT (the scripted-core twin,
/// `durable_ack_tests::an_fsync_with_nothing_pending_asks_for_no_drain`,
/// checks that it sends no drain request at all).
#[test]
fn fsync_of_a_through_closed_file_has_nothing_to_upload() {
    let node = node(false, crate::writeback::WriteMode::Through);
    node.engine.upload().hold.set(true);
    let v = view(&node);
    let caller = Caller::new(0, 0, Some(1001));
    let ino = write_and_close(&v, &caller, "small", b"already in the bucket");
    assert!(
        !node.engine.meta().upload_pending_for_ino(ino).unwrap(),
        "a through close leaves nothing pending"
    );
    let upload = node.engine.upload();
    let (passes, puts) = (
        upload.passes.load(std::sync::atomic::Ordering::Relaxed),
        upload
            .put_attempts
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    let started = std::time::Instant::now();
    open_fsync_close(&v, &caller, ino);
    assert_eq!(
        (
            upload.passes.load(std::sync::atomic::Ordering::Relaxed),
            upload
                .put_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
        ),
        (passes, puts),
        "the fsync of a durable file ran an upload pass or a PUT"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "the fsync of a durable file took {:?}",
        started.elapsed()
    );
}

/// Should-fix 1 of the 39b review: a pending chunk of the file that is
/// neither in this node's cache nor in S3 (a torn disk) makes the `fsync`
/// fail `EIO` — never 0 — in both modes, and every later `fsync` of the
/// file too while the row stays; the rest of the file's queued chunks
/// still go up.
fn fsync_with_a_lost_chunk_is_eio(fsync_s3: bool) {
    let node = node(fsync_s3, crate::writeback::WriteMode::Back);
    node.engine.upload().hold.set(true);
    let v = view(&node);
    let caller = Caller::new(0, 0, Some(1001));
    let ino = write_and_close(&v, &caller, "torn", &noise(41, 2 * 1024 * 1024 + 512));
    let mut lost = Vec::new();
    if !fsync_s3 {
        // A chunk the `back` close queued, gone from the cache.
        let queued = pending_of(&node, ino);
        assert_eq!(queued.len(), 3, "the back close left its chunks queued");
        node.engine.cache().remove(&queued[0]).unwrap();
        lost.push(queued[0]);
    }
    // And a row whose chunk never reached the cache's disk (a failed
    // write-through close, then the disk lost it).
    let never = node.engine.store().hash(&noise(42, 4096));
    node.engine.meta().add_pending_upload(&never, ino).unwrap();
    lost.push(never);

    for attempt in 0..2 {
        assert_eq!(
            try_open_fsync_close(&v, &caller, ino),
            Err(constellation_types::Code::Io),
            "fsync_s3={fsync_s3}, attempt {attempt}: an fsync with a lost chunk answered"
        );
        let left = pending_of(&node, ino);
        for hash in &lost {
            assert!(
                left.contains(hash),
                "fsync_s3={fsync_s3}: the lost chunk's row was dropped"
            );
            assert!(!in_bucket(&node, hash));
        }
        assert_eq!(
            left.len(),
            lost.len(),
            "fsync_s3={fsync_s3}: the file's other queued chunks did not go up"
        );
    }
}

#[test]
fn fsync_local_with_a_lost_chunk_is_eio() {
    fsync_with_a_lost_chunk_is_eio(false);
}

#[test]
fn fsync_s3_with_a_lost_chunk_is_eio() {
    fsync_with_a_lost_chunk_is_eio(true);
}

/// A row of the file that another node forwarded as pending (this node
/// sequenced that node's `back` close, `meta::store::remote`): the chunk
/// is on the writer only.
fn enroll_remote(node: &Node, ino: u64, seed: u64) -> (ChunkHash, Vec<u8>) {
    let data = noise(seed, 8192);
    let hash = node.engine.store().hash(&data);
    node.engine
        .meta()
        .enroll_remote_chunks(ino, &[hash], 2)
        .unwrap();
    assert!(!in_bucket(node, &hash));
    (hash, data)
}

/// Should-fix 2 of the 39b review (the coordinator's decision): an
/// `fsync` on the sequencer waits for the file's chunks another node
/// still has to upload — in both modes — and returns 0 only once they
/// are in S3 (here: found there by this node's S3 check, the fallback
/// for a report that never comes).
fn fsync_waits_for_remote_rows(fsync_s3: bool) {
    let node = node(fsync_s3, crate::writeback::WriteMode::Back);
    node.engine.upload().hold.set(true);
    let v = view(&node);
    let caller = Caller::new(0, 0, Some(1001));
    let ino = write_and_close(&v, &caller, "forwarded", b"the sequencer's view");
    let (hash, data) = enroll_remote(&node, ino, 43);
    std::thread::scope(|scope| {
        let fsync = scope.spawn(|| try_open_fsync_close(&v, &caller, ino));
        std::thread::sleep(std::time::Duration::from_millis(1000));
        assert!(
            !fsync.is_finished(),
            "fsync_s3={fsync_s3}: the fsync returned with another node's chunk not in S3: {:?}",
            pending_of(&node, ino)
        );
        // The writer's upload lands.
        node.rt
            .block_on(node.engine.store().put_chunk(
                &hash,
                &data,
                constellation_store_s3::CompressionSetting::RAW,
            ))
            .unwrap();
        assert_eq!(fsync.join().unwrap(), Ok(()), "fsync_s3={fsync_s3}");
    });
    assert!(
        !node.engine.meta().upload_pending_for_ino(ino).unwrap(),
        "fsync_s3={fsync_s3}: the remote row is acked"
    );
}

#[test]
fn fsync_local_waits_for_remote_rows() {
    fsync_waits_for_remote_rows(false);
}

#[test]
fn fsync_s3_waits_for_remote_rows() {
    fsync_waits_for_remote_rows(true);
}

/// The same wait under a soft `--fsync-timeout`: past it, `EIO` — never
/// 0 — and the row stays (a `CONSTELLATION_REMOTE_CHUNK_WAIT_S` elapsing
/// no longer turns into a success either: the timeout here is longer
/// than one drain attempt's slice of the wait).
#[test]
fn fsync_waiting_for_remote_rows_times_out_eio() {
    let node = node_with(
        false,
        crate::writeback::WriteMode::Back,
        Some(Some(std::time::Duration::from_secs(7))),
    );
    node.engine.upload().hold.set(true);
    let v = view(&node);
    let caller = Caller::new(0, 0, Some(1001));
    let ino = write_and_close(&v, &caller, "forwarded", b"never arrives");
    let (hash, _) = enroll_remote(&node, ino, 44);
    let started = std::time::Instant::now();
    assert_eq!(
        try_open_fsync_close(&v, &caller, ino),
        Err(constellation_types::Code::Io)
    );
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(7),
        "EIO before the timeout: {:?}",
        started.elapsed()
    );
    assert!(pending_of(&node, ino).contains(&hash), "the row stays");
    // The writer's report, at last (else the unmount's final flush waits
    // `CONSTELLATION_REMOTE_CHUNK_WAIT_S` for it).
    node.engine.meta().ack_remote_chunks(&[hash]).unwrap();
}

/// Plan 39b's measurement (run by hand: `cargo test --release -p
/// constellation-engine --lib -- --ignored --nocapture
/// fsync_with_nothing_pending_cost`): an `fsync` of a file with nothing
/// pending while 100k rows of other inodes are queued.
#[test]
#[ignore]
fn fsync_with_nothing_pending_cost() {
    let node = node(false, crate::writeback::WriteMode::Through);
    node.engine.upload().hold.set(true);
    let v = view(&node);
    let caller = Caller::new(0, 0, Some(1001));
    let ino = write_and_close(&v, &caller, "durable", b"already in the bucket");
    for i in 0..100_000u64 {
        let hash = ChunkHash::of(&i.to_le_bytes());
        node.engine
            .meta()
            .add_pending_upload(&hash, 1_000_000 + i % 5_000)
            .unwrap();
    }
    let reps = 50u32;
    let started = std::time::Instant::now();
    for _ in 0..reps {
        open_fsync_close(&v, &caller, ino);
    }
    println!(
        "open+fsync+close with nothing pending, 100000 other rows queued: {:?} each",
        started.elapsed() / reps
    );
    node.engine.meta().clear_pending_uploads().unwrap();
}
