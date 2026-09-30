//! Per-view admission (`ViewQos`, plan 31 §9.10), through the `Vfs`
//! trait: over a limit an op waits, past its deadline it answers `Again`;
//! the ops that end work are never held back.

use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{
    Blocking, Fh, FnResponder, LockOwner, Name, OpCtx, OpKind, OpenFlags, OpenOwner, Opened,
    ReadData, Vfs, VfsResult, WriteData,
};
use std::sync::mpsc;

fn caller() -> Caller {
    Caller::new(0, 0, None)
}

fn limited(qos: ViewQos) -> (Arc<View>, tempfile::TempDir) {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut view, dir) = super::quota_tests::test_fs(meta);
    view.apply_spec(&ViewSpec {
        qos,
        ..ViewSpec::default()
    });
    (Arc::new(view), dir)
}

fn getattr_by(v: &View, deadline: Option<Duration>) -> VfsResult<Attr> {
    let c = caller();
    let mut cx = OpCtx::new(OpKind::Getattr, &c);
    if let Some(d) = deadline {
        cx = cx.with_deadline(Instant::now() + d);
    }
    Blocking::run(|r| v.getattr(&cx, ROOT_INO, None, r))
}

/// Hold one op of `v` in flight (its responder parks) until `release`
/// is sent; returns once it is admitted.
fn hold_one(v: &Arc<View>) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let (release, released) = mpsc::channel::<()>();
    let (entered, is_in) = mpsc::channel::<()>();
    let v = v.clone();
    let holder = std::thread::spawn(move || {
        let c = caller();
        v.getattr(
            &OpCtx::new(OpKind::Getattr, &c),
            ROOT_INO,
            None,
            FnResponder::new(move |_: VfsResult<Attr>| {
                entered.send(()).unwrap();
                released.recv().unwrap();
            }),
        );
    });
    is_in.recv().unwrap();
    (release, holder)
}

#[test]
fn over_the_inflight_limit_an_op_waits_and_past_its_deadline_is_again() {
    let (v, _dir) = limited(ViewQos {
        max_inflight_ops: Some(1),
        max_staging_bytes: None,
    });
    let (release, holder) = hold_one(&v);
    // Full: a short deadline answers `Again` after waiting it out.
    let started = Instant::now();
    let refused = getattr_by(&v, Some(Duration::from_millis(150)));
    assert_eq!(refused.unwrap_err().code(), Code::Again);
    assert!(started.elapsed() >= Duration::from_millis(150), "it waited");
    // A longer one is admitted as soon as the holder leaves.
    let waiter = {
        let v = v.clone();
        std::thread::spawn(move || getattr_by(&v, Some(Duration::from_secs(10))))
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(!waiter.is_finished(), "held back while the view is full");
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(waiter.join().unwrap().unwrap().kind, FileKind::Dir);
    // Empty again: no wait at all.
    let started = Instant::now();
    getattr_by(&v, Some(Duration::from_millis(1))).unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn a_full_view_still_closes_and_a_cancelled_wait_is_intr() {
    let (v, _dir) = limited(ViewQos {
        max_inflight_ops: Some(1),
        max_staging_bytes: None,
    });
    let c = caller();
    let (e, o): (Entry, Opened) = Blocking::run(|r| {
        v.create(
            &OpCtx::new(OpKind::Create, &c),
            ROOT_INO,
            Name::new("f"),
            0o644,
            OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    let (release, holder) = hold_one(&v);
    // `flush` and `release` are never held back behind a full view.
    Blocking::run(|r| {
        v.flush(
            &OpCtx::new(OpKind::Flush, &c),
            e.attr.ino,
            o.fh,
            LockOwner(1),
            r,
        )
    })
    .unwrap();
    Blocking::run(|r| {
        v.release(
            &OpCtx::new(OpKind::Release, &c),
            e.attr.ino,
            o.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    // A cancelled wait answers `Intr`.
    let cancel = constellation_vfs::CancelToken::new();
    cancel.cancel();
    let cx = OpCtx::new(OpKind::Getattr, &c)
        .with_deadline(Instant::now() + Duration::from_secs(10))
        .with_cancel(&cancel);
    let refused = Blocking::run(|r| v.getattr(&cx, ROOT_INO, None, r));
    assert_eq!(refused.unwrap_err().code(), Code::Intr);
    release.send(()).unwrap();
    holder.join().unwrap();
}

#[test]
fn over_the_staging_limit_a_write_waits_for_the_views_own_flushes() {
    let (v, _dir) = limited(ViewQos {
        max_inflight_ops: None,
        max_staging_bytes: Some(64),
    });
    let c = caller();
    let create = |name: &str| -> (Ino, Fh) {
        let (e, o): (Entry, Opened) = Blocking::run(|r| {
            v.create(
                &OpCtx::new(OpKind::Create, &c),
                ROOT_INO,
                Name::new(name),
                0o644,
                OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        (e.attr.ino, o.fh)
    };
    let write = |ino: Ino, fh: Fh, data: &[u8], deadline: Duration| {
        let cx = OpCtx::new(OpKind::Write, &c).with_deadline(Instant::now() + deadline);
        Blocking::run(|r| {
            v.write(
                &cx,
                ino,
                fh,
                0,
                WriteData::Borrowed(data),
                OpenFlags::WRITE,
                r,
            )
        })
    };
    let (a, fa) = create("a");
    // Nothing staged yet: a write larger than the whole limit proceeds.
    assert_eq!(
        write(a, fa, &[1; 100], Duration::from_secs(1)).unwrap(),
        100
    );
    let staging = v.admission.staging().unwrap().clone();
    assert!(staging.used() >= 100);
    // The view is over its limit: another write waits, then `Again`.
    let (b, fb) = create("b");
    let refused = write(b, fb, &[2; 10], Duration::from_millis(100));
    assert_eq!(refused.unwrap_err().code(), Code::Again);
    // The first file's close publishes it and gives its staging back.
    Blocking::run(|r| {
        v.release(
            &OpCtx::new(OpKind::Release, &c),
            a,
            fa,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    assert_eq!(staging.used(), 0);
    assert_eq!(write(b, fb, &[2; 10], Duration::from_secs(1)).unwrap(), 10);
    // The node's budget counted the view's bytes too.
    assert_eq!(v.staging_budget.used(), staging.used());
}

#[test]
fn an_unlimited_view_has_no_admission_state() {
    let (v, _dir) = limited(ViewQos::default());
    assert!(v.admission.staging().is_none());
    let c = caller();
    let _a = v.admission.admit(&OpCtx::new(OpKind::Getattr, &c)).unwrap();
    let _b = v.admission.admit(&OpCtx::new(OpKind::Getattr, &c)).unwrap();
    getattr_by(&v, Some(Duration::ZERO)).unwrap();
}

/// Plan 31 C7b: a cold read defers to the completion pool — the call
/// returns before it is answered, the answer comes from a pool thread —
/// and it keeps its admission slot until then; a warm read answers inline.
#[test]
fn a_deferred_cold_read_answers_off_the_caller_and_holds_its_slot() {
    let (v, _dir) = limited(ViewQos {
        max_inflight_ops: Some(1),
        max_staging_bytes: None,
    });
    v.bind();
    let c = caller();
    let rw = OpenFlags::READ | OpenFlags::WRITE | OpenFlags::CREATE;
    let (entry, opened) = Blocking::run(|r| {
        v.create(
            &OpCtx::new(OpKind::Create, &c),
            ROOT_INO,
            Name::new(b"f"),
            0o644,
            rw,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    let ino = entry.attr.ino;
    let content: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    Blocking::run(|r| {
        v.write(
            &OpCtx::new(OpKind::Write, &c),
            ino,
            opened.fh,
            0,
            WriteData::Borrowed(&content),
            rw,
            r,
        )
    })
    .unwrap();
    Blocking::run(|r| {
        v.flush(
            &OpCtx::new(OpKind::Flush, &c),
            ino,
            opened.fh,
            LockOwner(1),
            r,
        )
    })
    .unwrap();
    // No sync task here: upload by hand, then drop the clean chunk.
    v.rt.block_on(crate::upload::upload_dirty_chunks(
        &v.cache,
        &v.meta,
        &v.store,
        CompressionSetting::RAW,
        &crate::upload::UploadRuntime::for_test(true),
        None,
        None,
    ))
    .unwrap();
    assert_eq!(v.evict_cached(ino).unwrap(), 1, "one chunk left the cache");

    let me = std::thread::current().id();
    let (entered, is_in) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let len = content.len() as u32;
    v.read(
        &OpCtx::new(OpKind::Read, &c),
        ino,
        opened.fh,
        0,
        len,
        FnResponder::new(move |got: VfsResult<ReadData>| {
            entered
                .send((
                    got.map(|d| d.contiguous().into_owned()),
                    std::thread::current().id(),
                ))
                .unwrap();
            released.recv().unwrap();
        }),
    );
    // The call returned; the answer arrives from elsewhere.
    let (got, on) = is_in.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(got.unwrap(), content);
    assert_ne!(on, me, "the cold read was answered on the calling thread");
    // Until the answer is through, the view's one slot is the read's.
    assert_eq!(
        getattr_by(&v, Some(Duration::from_millis(100)))
            .unwrap_err()
            .code(),
        Code::Again
    );
    release.send(()).unwrap();
    getattr_by(&v, Some(Duration::from_secs(10))).unwrap();
    // Warm now: answered inline, on this thread, before the call returns.
    let (tx, rx) = mpsc::channel();
    v.read(
        &OpCtx::new(OpKind::Read, &c),
        ino,
        opened.fh,
        0,
        len,
        FnResponder::new(move |got: VfsResult<ReadData>| {
            tx.send((got.unwrap().len(), std::thread::current().id()))
                .unwrap();
        }),
    );
    assert_eq!(rx.try_recv().unwrap(), (content.len(), me));
}
