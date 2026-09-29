//! `deferral`: a responder completes exactly once, on the calling thread
//! unless the op waits (and the frontend allows it), from another thread
//! when it does; a responder that panics or is dropped never leaves the
//! target stuck or the op pending.

use super::facade::{Done, Probe};
use super::{fail, must, refused, Env, TestResult, HANG};

use crate::ctx::{Caller, OpCtx, OpKind};
use crate::error::VfsResult;
use crate::responder::{Blocking, FnResponder, Responder};
use crate::types::{FallocateMode, Fh, LockKind, LockOwner, LockRange, OpenFlags, SeekWhence};
use crate::{Durability, OpKindSet, SetAttr, SetXattrFlags};
use constellation_types::Code;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

/// What `once` saw of one op.
struct Seen<T> {
    result: Option<VfsResult<T>>,
    completions: usize,
    completed_on: Option<ThreadId>,
    /// The completion had already happened when the op call returned.
    before_return: bool,
}

/// Run one op with a counting continuation; wait for it, then a moment
/// longer to see whether it completes again.
fn once<T: Send + 'static>(start: impl FnOnce(Done<T>)) -> Seen<T> {
    let count = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel();
    let done: Done<T> = {
        let count = count.clone();
        Box::new(move |result| {
            count.fetch_add(1, Ordering::SeqCst);
            let _ = tx.send((result, std::thread::current().id()));
        })
    };
    start(done);
    let before_return = count.load(Ordering::SeqCst) > 0;
    let (result, thread) = rx
        .recv_timeout(HANG)
        .unwrap_or_else(|_| panic!("the op never completed"));
    std::thread::sleep(Duration::from_millis(15));
    Seen {
        result,
        completions: count.load(Ordering::SeqCst),
        completed_on: Some(thread),
        before_return,
    }
}

pub(super) fn every_op_completes_exactly_once(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let vfs = c.dyn_vfs().clone();
    let root = c.root();
    let caller = Caller::with_groups(1000, 1000, &[]);
    let me = std::thread::current().id();
    let deferrable = env.caps().deferrable;
    let mut seen: Vec<(OpKind, usize, bool, Option<ThreadId>, bool)> = Vec::new();
    macro_rules! op {
        ($kind:expr, |$cx:ident, $d:ident| $call:expr) => {{
            let $cx = OpCtx::new($kind, &caller);
            let s = once(|$d| $call);
            assert!(
                s.result.is_some(),
                "{}: the responder was dropped without completing it",
                $kind.name()
            );
            seen.push(($kind, s.completions, s.before_return, s.completed_on, true));
            s.result.unwrap()
        }};
    }
    // A workout that touches every op, successes and refusals.
    let entry = must(
        "create",
        op!(OpKind::Create, |cx, d| vfs.create(
            &cx,
            root,
            b"f",
            0o100_644,
            OpenFlags::READ | OpenFlags::WRITE,
            d
        )),
    );
    let (e, o) = entry;
    let ino = e.attr.ino;
    must(
        "write",
        op!(OpKind::Write, |cx, d| vfs.write(
            &cx,
            ino,
            o.fh,
            0,
            b"data",
            OpenFlags::WRITE,
            d
        )),
    );
    must(
        "read",
        op!(OpKind::Read, |cx, d| vfs.read(&cx, ino, o.fh, 0, 16, d)),
    );
    must(
        "fsync",
        op!(OpKind::Fsync, |cx, d| vfs.fsync(
            &cx,
            ino,
            o.fh,
            Durability::Configured,
            d
        )),
    );
    must(
        "getattr",
        op!(OpKind::Getattr, |cx, d| vfs.getattr(
            &cx,
            ino,
            Some(o.fh),
            d
        )),
    );
    must(
        "setattr",
        op!(OpKind::Setattr, |cx, d| vfs.setattr(
            &cx,
            ino,
            None,
            &SetAttr {
                mode: Some(0o600),
                ..SetAttr::default()
            },
            d
        )),
    );
    must(
        "lookup",
        op!(OpKind::Lookup, |cx, d| vfs.lookup(&cx, root, b"f", d)),
    );
    refused(
        "lookup of a missing name",
        op!(OpKind::Lookup, |cx, d| vfs.lookup(&cx, root, b"nope", d)),
        Code::NotFound,
    );
    must(
        "mkdir",
        op!(OpKind::Mkdir, |cx, d| vfs.mkdir(&cx, root, b"d", 0o755, d)),
    );
    refused(
        "mkdir again",
        op!(OpKind::Mkdir, |cx, d| vfs.mkdir(&cx, root, b"d", 0o755, d)),
        Code::Exists,
    );
    must(
        "symlink",
        op!(OpKind::Symlink, |cx, d| vfs
            .symlink(&cx, root, b"s", b"f", d)),
    );
    must(
        "readlink",
        op!(OpKind::Readlink, |cx, d| vfs.readlink(
            &cx,
            must("lookup s", c.lookup(root, "s")).attr.ino,
            d
        )),
    );
    if env.caps().hard_links {
        must(
            "link",
            op!(OpKind::Link, |cx, d| vfs.link(&cx, ino, root, b"h", d)),
        );
    }
    if env.caps().special_files {
        must(
            "mknod",
            op!(OpKind::Mknod, |cx, d| vfs.mknod(
                &cx,
                root,
                b"p",
                crate::types::mode::S_IFIFO | 0o644,
                Default::default(),
                d
            )),
        );
    }
    must(
        "readdir",
        op!(OpKind::Readdir, |cx, d| vfs.readdir(
            &cx,
            root,
            Fh(0),
            0,
            false,
            100,
            d
        )),
    );
    must(
        "statfs",
        op!(OpKind::Statfs, |cx, d| vfs.statfs(&cx, root, d)),
    );
    if env.caps().xattrs != crate::XattrSupport::None {
        must(
            "setxattr",
            op!(OpKind::Setxattr, |cx, d| vfs.setxattr(
                &cx,
                ino,
                b"user.a",
                b"1",
                SetXattrFlags::empty(),
                d
            )),
        );
        must(
            "getxattr",
            op!(OpKind::Getxattr, |cx, d| vfs
                .getxattr(&cx, ino, b"user.a", d)),
        );
        must(
            "listxattr",
            op!(OpKind::Listxattr, |cx, d| vfs.listxattr(&cx, ino, d)),
        );
        must(
            "removexattr",
            op!(OpKind::Removexattr, |cx, d| vfs
                .removexattr(&cx, ino, b"user.a", d)),
        );
    }
    if env.caps().fallocate {
        must(
            "fallocate",
            op!(OpKind::Fallocate, |cx, d| vfs.fallocate(
                &cx,
                ino,
                o.fh,
                0,
                8192,
                FallocateMode::empty(),
                d
            )),
        );
    }
    if env.caps().seek_hole {
        must(
            "seek",
            op!(OpKind::Seek, |cx, d| vfs.seek(
                &cx,
                ino,
                o.fh,
                0,
                SeekWhence::Data,
                d
            )),
        );
    }
    must(
        "rename",
        op!(OpKind::Rename, |cx, d| vfs.rename(
            &cx,
            root,
            b"f",
            root,
            b"g",
            Default::default(),
            d
        )),
    );
    must(
        "sync_view",
        op!(OpKind::SyncView, |cx, d| vfs.sync_view(&cx, d)),
    );
    must(
        "flush",
        op!(OpKind::Flush, |cx, d| vfs.flush(
            &cx,
            ino,
            o.fh,
            LockOwner(9),
            d
        )),
    );
    must(
        "release",
        op!(OpKind::Release, |cx, d| vfs.release(
            &cx,
            ino,
            o.fh,
            OpenFlags::READ,
            None,
            d
        )),
    );
    must("open", {
        let r = op!(OpKind::Open, |cx, d| vfs.open(&cx, ino, OpenFlags::READ, d));
        r.map(|o| {
            let _ = c.release(ino, o.fh);
        })
    });
    must(
        "unlink",
        op!(OpKind::Unlink, |cx, d| vfs.unlink(&cx, root, b"g", d)),
    );
    refused(
        "unlink again",
        op!(OpKind::Unlink, |cx, d| vfs.unlink(&cx, root, b"g", d)),
        Code::NotFound,
    );
    must(
        "rmdir",
        op!(OpKind::Rmdir, |cx, d| vfs.rmdir(&cx, root, b"d", d)),
    );
    refused(
        "rmdir again",
        op!(OpKind::Rmdir, |cx, d| vfs.rmdir(&cx, root, b"d", d)),
        Code::NotFound,
    );
    for (kind, completions, before_return, thread, _) in &seen {
        if *completions != 1 {
            fail!(
                "{}: completed {completions} times, not exactly once",
                kind.name()
            );
        }
        // Only the ops a frontend allows to defer may complete after the
        // call returned or on another thread.
        if !deferrable.contains(*kind) && (!*before_return || *thread != Some(me)) {
            fail!(
                "{}: not in the frontend's deferrable set, yet it completed {}",
                kind.name(),
                if *before_return {
                    "on another thread"
                } else {
                    "after the call returned"
                }
            );
        }
    }
    Ok(())
}

pub(super) fn a_blocked_lock_completes_from_another_thread(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.caps.deferrable.contains(OpKind::LockAcquire) {
        super::skip!(
            "this frontend does not allow lock_acquire to defer (FrontendCaps::deferrable)"
        );
    }
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x"));
    let ino = f.attr.ino;
    let holder = must("open", c.open_rw(ino));
    let lock = |owner| super::Client::whole_file_lock(owner, LockKind::Write);
    must("holder locks", c.try_lock(ino, holder.fh, lock(1)));
    refused(
        "F_SETLK against the holder",
        c.try_lock(ino, holder.fh, lock(2)),
        Code::Again,
    );
    // Five waiters queue behind the holder. Each call must return at once
    // (the wait is not on this thread) and complete later, elsewhere.
    let me = std::thread::current().id();
    let started = Instant::now();
    let mut waiters: Vec<_> = (2..=6u64)
        .map(|owner| {
            let o = must("open", c.open_rw(ino));
            (
                owner,
                o,
                c.lock_wait_async(ino, o.fh, lock(owner), None, None),
            )
        })
        .collect();
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "queueing five blocking locks took {:?}: the calling thread waited",
        started.elapsed()
    );
    for (owner, _, p) in waiters.iter_mut() {
        assert!(
            !p.completes_within(Duration::from_millis(100)),
            "waiter {owner} was granted while the holder still holds the lock"
        );
    }
    let probe = must("lock_test", c.lock_test(ino, holder.fh, lock(99)));
    assert!(
        matches!(probe, crate::LockStatus::Locked { .. }),
        "{probe:?}"
    );
    // Release: the waiters are granted one at a time, each on a thread of
    // the target's, exactly once.
    let range = LockRange {
        start: 0,
        end: i64::MAX as u64,
    };
    must(
        "holder unlocks",
        c.unlock(ino, holder.fh, LockOwner(1), range),
    );
    let mut granted = 0;
    let mut remaining = waiters;
    // Measured from the last grant: a waiter that is never granted fails
    // the test after `HANG`, not the run by never ending.
    let mut deadline = Instant::now() + HANG;
    while !remaining.is_empty() {
        let mut progressed = false;
        let mut still = Vec::new();
        for (owner, o, mut p) in remaining {
            if p.completes_within(Duration::from_millis(20)) {
                let (result, thread) = p.outcome();
                assert_eq!(result, Some(Ok(())), "waiter {owner}");
                assert_ne!(
                    thread, me,
                    "waiter {owner} was completed on the thread that called lock_acquire"
                );
                granted += 1;
                progressed = true;
                must(
                    "waiter unlocks",
                    c.unlock(ino, o.fh, LockOwner(owner), range),
                );
                must("close", c.close(ino, o.fh));
            } else {
                still.push((owner, o, p));
            }
        }
        remaining = still;
        if progressed {
            deadline = Instant::now() + HANG;
        } else if Instant::now() > deadline {
            fail!(
                "{} waiter(s) were never granted after the holder unlocked",
                remaining.len()
            );
        }
    }
    assert_eq!(granted, 5);
    must("close holder", c.close(ino, holder.fh));
    Ok(())
}

pub(super) fn non_deferrable_waits_park_the_calling_thread(env: &Env<'_>) -> TestResult {
    let mut caps = env.caps().clone();
    caps.cluster_locks = true;
    caps.deferrable = OpKindSet::ALL;
    let mut none = OpKindSet::EMPTY;
    for k in OpKind::ALL {
        if *k != OpKind::LockAcquire {
            none = none.with(*k);
        }
    }
    caps.deferrable = none;
    let fx = env.fresh_with(&caps);
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x"));
    let ino = f.attr.ino;
    let holder = must("open", c.open_rw(ino));
    let waiter = must("open", c.open_rw(ino));
    let lock = |owner| super::Client::whole_file_lock(owner, LockKind::Write);
    must("holder locks", c.try_lock(ino, holder.fh, lock(1)));
    let range = LockRange {
        start: 0,
        end: i64::MAX as u64,
    };
    // Another thread releases after a while; this thread is parked in the
    // op until then, because the frontend cannot take a deferred answer.
    let me = std::thread::current().id();
    let (pending, returned_after) = std::thread::scope(|s| {
        let releaser = s.spawn(|| {
            std::thread::sleep(Duration::from_millis(150));
            fx.client().unlock(ino, holder.fh, LockOwner(1), range)
        });
        let started = Instant::now();
        let pending = c.lock_wait_async(ino, waiter.fh, lock(2), None, None);
        let took = started.elapsed();
        must("releaser", releaser.join().expect("releaser"));
        (pending, took)
    });
    let (result, thread) = pending.outcome();
    assert_eq!(result, Some(Ok(())));
    assert_eq!(
        thread, me,
        "a non-deferrable op completes on the calling thread"
    );
    assert!(
        returned_after >= Duration::from_millis(100),
        "the call returned after {returned_after:?}, before the lock could have been granted"
    );
    must("unlock", c.unlock(ino, waiter.fh, LockOwner(2), range));
    Ok(())
}

pub(super) fn locks_are_refused_without_the_capability(env: &Env<'_>) -> TestResult {
    let mut caps = env.caps().clone();
    caps.cluster_locks = false;
    let fx = env.fresh_with(&caps);
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x"));
    let ino = f.attr.ino;
    let o = must("open", c.open_rw(ino));
    let lock = super::Client::whole_file_lock(1, LockKind::Write);
    // Not asked when the frontend keeps locks node-local: `NotImplemented`
    // (the kernel then never asks again).
    refused(
        "lock_test",
        c.lock_test(ino, o.fh, lock),
        Code::NotImplemented,
    );
    refused("F_SETLK", c.try_lock(ino, o.fh, lock), Code::NotImplemented);
    refused(
        "F_SETLKW",
        c.lock_wait_async(ino, o.fh, lock, None, None).wait(),
        Code::NotImplemented,
    );
    refused(
        "unlock",
        c.unlock(ino, o.fh, LockOwner(1), lock.range),
        Code::NotImplemented,
    );
    must("close", c.close(ino, o.fh));
    Ok(())
}

pub(super) fn a_panicking_responder_leaves_the_target_usable(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let vfs = c.dyn_vfs().clone();
    let root = c.root();
    let caller = Caller::with_groups(1000, 1000, &[]);
    // A responder that panics when completed, on the caller's thread: the
    // panic unwinds through the op. The target must have applied the op's
    // effect before completing, must complete outside its locks (a lock
    // held across the unwind would poison it or deadlock the next op), and
    // must stay usable.
    let boom = || -> Done<crate::Entry> { Box::new(|_| panic!("responder panics")) };
    let cx = OpCtx::new(OpKind::Mkdir, &caller);
    let caught = catch_unwind(AssertUnwindSafe(|| {
        vfs.mkdir(&cx, root, b"made", 0o755, boom());
    }));
    assert!(
        caught.is_err(),
        "the responder's panic should have propagated to the caller"
    );
    let cx = OpCtx::new(OpKind::Lookup, &caller);
    let caught = catch_unwind(AssertUnwindSafe(|| {
        vfs.lookup(&cx, root, b"nope", boom());
    }));
    assert!(caught.is_err());
    let cx = OpCtx::new(OpKind::Create, &caller);
    let caught = catch_unwind(AssertUnwindSafe(|| {
        vfs.create(
            &cx,
            root,
            b"created",
            0o100_644,
            OpenFlags::READ | OpenFlags::WRITE,
            Box::new(|_| panic!("responder panics")),
        );
    }));
    assert!(caught.is_err());
    // Still usable, and the effects are there.
    assert_eq!(
        must("lookup made", c.lookup(root, "made")).attr.kind,
        crate::FileKind::Dir,
        "the mkdir took effect before its responder ran"
    );
    must("lookup created", c.lookup(root, "created"));
    must("put after", c.put(root, "after", b"ok"));
    must("rmdir", c.rmdir(root, "made"));
    assert_eq!(must("names", c.names(root)), ["after", "created"]);
    Ok(())
}

pub(super) fn the_provided_responders_fail_safe(_env: &Env<'_>) -> TestResult {
    // A dropped Blocking answers the waiter with Io, on this thread and on
    // another.
    assert_eq!(Blocking::<u32>::run(drop), Err(Code::Io.into()));
    let got = Blocking::<u32>::run(|r| {
        std::thread::spawn(move || drop(r)).join().unwrap();
    });
    assert_eq!(got, Err(Code::Io.into()));
    // A dropped FnResponder answers Io exactly once; a completed one never
    // answers again on drop.
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let responder = |calls: &Arc<std::sync::Mutex<Vec<VfsResult<u8>>>>| {
        let calls = calls.clone();
        FnResponder::new(move |r| calls.lock().unwrap().push(r))
    };
    responder(&calls).done(Ok(1));
    drop(responder(&calls));
    assert_eq!(*calls.lock().unwrap(), vec![Ok(1), Err(Code::Io.into())]);
    // The probe the kit uses to tell the two apart.
    let (tx, rx) = mpsc::channel();
    let probe = Probe::<u8>::new(Box::new(move |r| tx.send(r).unwrap()));
    drop(probe);
    assert_eq!(rx.recv().unwrap(), None, "a dropped probe reports the drop");
    let (tx, rx) = mpsc::channel();
    Probe::<u8>::new(Box::new(move |r| tx.send(r).unwrap())).done(Ok(7));
    assert_eq!(rx.recv().unwrap(), Some(Ok(7)));
    assert!(rx.try_recv().is_err(), "and reports once");
    Ok(())
}
