//! Plan 30 §M14, the owner fence, through the `Vfs` trait against a real
//! [`View`]: a lock owner whose grant lapsed with its `flock` held gets
//! `EIO` from its writes and namespace ops on *other* files (git under
//! one turn lock creates, links and renames everywhere); another process
//! on the node does not; the fence lifts with the owner's unlock.
use super::durable_ack_tests::holder_fs;
use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::locks::{GrantId, HeldGrant, LockMode};
use constellation_store_s3::lease::now_unix_ms;
use constellation_vfs::{
    Blocking, Fh, LockKind, LockOwner, LockRange, LockSpec, LockStatus, Name, OpCtx, OpKind,
    OpenOwner, Opened, RenameFlags, Vfs, VfsResult, WriteData,
};

fn code<T: std::fmt::Debug>(r: VfsResult<T>) -> Code {
    r.expect_err("the op should have been fenced").code()
}

struct Fenced {
    fs: View,
    meta: Arc<Meta>,
    _dir: tempfile::TempDir,
    _core: tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
    _locks_core: tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
}

fn fenced_fs() -> Fenced {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut fs, dir, core) = holder_fs(meta.clone(), false);
    let (tx, locks_core) = tokio::sync::mpsc::unbounded_channel();
    fs.sync.as_mut().unwrap().locks = Some(Arc::new(crate::locks::ClusterLocks {
        meta: meta.clone(),
        tx,
        inval: None,
        lineage: Default::default(),
    }));
    fs.caps.cluster_locks = true;
    Fenced {
        fs,
        meta,
        _dir: dir,
        _core: core,
        _locks_core: locks_core,
    }
}

const TURN_OWNER: u64 = 0xf10c;

impl Fenced {
    fn create(&self, caller: &Caller, parent: Ino, name: &str) -> VfsResult<(Entry, Opened)> {
        Blocking::run(|r| {
            self.fs.create(
                &OpCtx::new(OpKind::Create, caller),
                parent,
                Name::new(name),
                0o100644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
    }

    fn write(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh) -> VfsResult<u32> {
        Blocking::run(|r| {
            self.fs.write(
                cx,
                ino,
                fh,
                0,
                WriteData::Borrowed(b"object"),
                OpenFlags::WRITE,
                r,
            )
        })
    }

    /// `copy_file_range` of `ino`'s first 6 bytes to its offset 64.
    fn copy(&self, caller: &Caller, ino: Ino, fh: Fh) -> VfsResult<u64> {
        Blocking::run(|r| {
            self.fs.copy_file_range(
                &OpCtx::new(OpKind::CopyFileRange, caller),
                ino,
                fh,
                0,
                ino,
                fh,
                64,
                6,
                constellation_vfs::CopyFlags::empty(),
                r,
            )
        })
    }

    fn rename(&self, caller: &Caller, from: &str, to: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.fs.rename(
                &OpCtx::new(OpKind::Rename, caller),
                ROOT_INO,
                Name::new(from),
                ROOT_INO,
                Name::new(to),
                RenameFlags::empty(),
                r,
            )
        })
    }

    fn mkdir(&self, caller: &Caller, name: &str) -> VfsResult<Entry> {
        Blocking::run(|r| {
            self.fs.mkdir(
                &OpCtx::new(OpKind::Mkdir, caller),
                ROOT_INO,
                Name::new(name),
                0o755,
                r,
            )
        })
    }

    fn unlink(&self, caller: &Caller, name: &str) -> VfsResult<()> {
        Blocking::run(|r| {
            self.fs.unlink(
                &OpCtx::new(OpKind::Unlink, caller),
                ROOT_INO,
                Name::new(name),
                r,
            )
        })
    }

    fn open_write(&self, caller: &Caller, ino: Ino) -> VfsResult<Opened> {
        Blocking::run(|r| {
            self.fs.open(
                &OpCtx::new(OpKind::Open, caller),
                ino,
                OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
    }

    fn getattr(&self, caller: &Caller, ino: Ino) -> VfsResult<Attr> {
        Blocking::run(|r| {
            self.fs
                .getattr(&OpCtx::new(OpKind::Getattr, caller), ino, None, r)
        })
    }

    fn unlock(&self, caller: &Caller, ino: Ino) -> VfsResult<()> {
        Blocking::run(|r| {
            self.fs.lock_release(
                &OpCtx::new(OpKind::LockRelease, caller),
                ino,
                Fh(ino),
                LockOwner(TURN_OWNER),
                LockRange {
                    start: 0,
                    end: u64::MAX,
                },
                r,
            )
        })
    }

    /// `pid` holds an exclusive `flock` on `ino` under a grant this node
    /// honours until `until_ms`.
    fn lock(&self, ino: Ino, pid: u32, until_ms: i64) {
        let t = self.meta.locks();
        let now = now_unix_ms();
        t.install_held(
            ino,
            HeldGrant {
                id: GrantId { node: 9, seq: 1 },
                mode: LockMode::Exclusive,
                until_ms,
                renew_at_ms: until_ms,
                owner: 9,
                recalled: false,
                position: constellation_meta::Position::ZERO,
                renewing: None,
                releasing: false,
                first_use: false,
                idle_since_ms: None,
                installed_ms: now,
            },
        );
        // Through the view, as FUSE asks: `pid` is the locking thread.
        Blocking::run(|r| {
            self.fs.lock_acquire(
                &OpCtx::new(OpKind::LockAcquire, &Caller::new(0, 0, Some(pid))),
                ino,
                Fh(ino),
                LockSpec {
                    owner: LockOwner(TURN_OWNER),
                    range: LockRange {
                        start: 0,
                        end: u64::MAX,
                    },
                    kind: LockKind::Write,
                    pid,
                },
                false,
                r,
            )
        })
        .unwrap();
        assert_eq!(t.local_locks(ino).len(), 1);
    }
}

#[test]
fn a_lapsed_owner_is_fenced_on_every_file_and_others_are_not() {
    let f = fenced_fs();
    let me = std::process::id();
    // The committer: this test process (a thread of it, as FUSE names the
    // caller), and a process it started (git under `flock`) is the same
    // owner as far as the fence goes.
    let committer = Caller::new(0, 0, Some(me));
    let tid = || {
        let link = std::fs::read_link("/proc/thread-self").unwrap();
        link.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u32>()
            .unwrap()
    };
    let (tx, rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::spawn(move || {
        tx.send(tid()).unwrap();
        let _ = done_rx.recv();
    });
    let committer_thread = Caller::new(0, 0, Some(rx.recv().unwrap()));
    // Another process on the node: our parent (not started by us).
    let other = Caller::new(0, 0, Some(std::os::unix::process::parent_id()));
    let root = Caller::root();

    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let (obj, oo) = f.create(&root, ROOT_INO, "tmp_obj").unwrap();
    f.lock(turn.attr.ino, me, now_unix_ms() + 60_000);

    // Under an honoured grant nothing is fenced.
    f.write(&OpCtx::new(OpKind::Write, &committer), obj.attr.ino, oo.fh)
        .unwrap();
    f.mkdir(&committer, "objects").unwrap();

    // The grant is lost (a stall past the ttl: the owner outwaited it).
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    assert!(f.meta.locks().lost(turn.attr.ino, id));

    // The committer's writes and namespace ops on *other* files: EIO.
    assert_eq!(
        code(f.write(&OpCtx::new(OpKind::Write, &committer), obj.attr.ino, oo.fh)),
        Code::Io
    );
    assert_eq!(code(f.create(&committer, ROOT_INO, "new_obj")), Code::Io);
    assert_eq!(code(f.mkdir(&committer, "refs")), Code::Io);
    assert_eq!(code(f.rename(&committer, "tmp_obj", "obj")), Code::Io);
    assert_eq!(code(f.unlink(&committer, "tmp_obj")), Code::Io);
    assert_eq!(code(f.open_write(&committer, obj.attr.ino)), Code::Io);
    assert_eq!(
        code(f.copy(&committer, obj.attr.ino, oo.fh)),
        Code::Io,
        "a copy is a write"
    );
    assert_eq!(
        code(f.create(&committer_thread, ROOT_INO, "x")),
        Code::Io,
        "a thread of the owner's process"
    );
    drop(done_tx);
    thread.join().unwrap();
    // By the kernel's lock owner, whatever the pid says.
    let unknown = Caller::new(0, 0, None);
    assert_eq!(
        code(f.write(
            &OpCtx::new(OpKind::Write, &unknown).with_lock_owner(Some(TURN_OWNER)),
            obj.attr.ino,
            oo.fh
        )),
        Code::Io
    );
    // Lookups and attributes still answer (the failure path must see
    // the tree).
    f.getattr(&committer, obj.attr.ino).unwrap();
    let s = f.meta.locks().stats();
    assert_eq!(s.owners_fenced, 1);
    assert!(s.owner_fenced_ops >= 8, "{s:?}");

    // Another process on the same node is not fenced.
    f.write(&OpCtx::new(OpKind::Write, &other), obj.attr.ino, oo.fh)
        .unwrap();
    assert_eq!(f.copy(&other, obj.attr.ino, oo.fh).unwrap(), 6);
    f.create(&other, ROOT_INO, "theirs").unwrap();
    f.rename(&other, "theirs", "theirs2").unwrap();
    f.write(&OpCtx::new(OpKind::Write, &unknown), obj.attr.ino, oo.fh)
        .unwrap();

    // The owner unlocks: the fence lifts.
    f.unlock(&committer, turn.attr.ino).unwrap();
    f.create(&committer, ROOT_INO, "after").unwrap();
    f.rename(&committer, "tmp_obj", "obj").unwrap();
}

/// A grant that runs out by time (no event at all: the clock passes its
/// end) fences the owner from the first op after it.
#[test]
fn a_grant_that_runs_out_fences_its_owner_without_any_event() {
    let f = fenced_fs();
    let me = std::process::id();
    let committer = Caller::new(0, 0, Some(me));
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    f.lock(turn.attr.ino, me, now_unix_ms() + 300);
    f.mkdir(&committer, "a").unwrap();
    // The grant's end passes on this thread's clock (no sleep: a loaded
    // host could stretch a real one past anything the test assumes).
    crate::locks::advance_test_clock(350);
    assert_eq!(code(f.mkdir(&committer, "b")), Code::Io);
    f.mkdir(&root, "c").unwrap();
    f.unlock(&committer, turn.attr.ino).unwrap();
    f.mkdir(&committer, "b").unwrap();
}

/// The id of the calling thread (FUSE's `req.pid()` for a request it
/// issues).
fn tid() -> u32 {
    let link = std::fs::read_link("/proc/thread-self").unwrap();
    link.file_name().unwrap().to_str().unwrap().parse().unwrap()
}

/// A lock taken by a thread that is not its process's main thread (a
/// Go, Java or Python program; the git harness's committer thread): FUSE
/// names the request by the *thread*, whose id no child's parent pid
/// ever is. The fence still covers the whole process — a sibling thread,
/// and a process it started (git) — and nobody else.
#[test]
fn a_lock_taken_by_a_non_main_thread_fences_its_whole_process_and_children() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let (obj, oo) = f.create(&root, ROOT_INO, "obj").unwrap();

    // Two threads that stay alive while the fence is checked.
    let spawn_thread = || {
        let (tx, rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let h = std::thread::spawn(move || {
            tx.send(tid()).unwrap();
            let _ = done_rx.recv();
        });
        (rx.recv().unwrap(), done_tx, h)
    };
    let (locker, locker_done, locker_h) = spawn_thread();
    let (sibling, sibling_done, sibling_h) = spawn_thread();
    assert_ne!(locker, me);
    assert_ne!(sibling, me);
    // A process this process started (git under the committer).
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let child_pid = child.id();
    // A process this one did not start.
    let other = Caller::new(0, 0, Some(std::os::unix::process::parent_id()));

    f.lock(turn.attr.ino, locker, now_unix_ms() + 60_000);
    let l = f.meta.locks().local_locks(turn.attr.ino)[0];
    assert_eq!(l.pid, me, "the lock names the locker's process");
    assert!(l.pid_start > 0, "and its start time");
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    assert!(f.meta.locks().lost(turn.attr.ino, id));

    for (who, pid) in [
        ("the locking thread", locker),
        ("a sibling thread", sibling),
        ("the main thread", me),
        ("a child process", child_pid),
    ] {
        let caller = Caller::new(0, 0, Some(pid));
        assert_eq!(
            code(f.mkdir(&caller, &format!("d{pid}"))),
            Code::Io,
            "{who} is fenced"
        );
        assert_eq!(
            code(f.write(&OpCtx::new(OpKind::Write, &caller), obj.attr.ino, oo.fh)),
            Code::Io,
            "{who} is fenced"
        );
    }
    f.mkdir(&other, "theirs").unwrap();
    f.write(&OpCtx::new(OpKind::Write, &other), obj.attr.ino, oo.fh)
        .unwrap();

    child.kill().unwrap();
    child.wait().unwrap();
    drop((locker_done, sibling_done));
    locker_h.join().unwrap();
    sibling_h.join().unwrap();
}

/// A fenced pid's verdict is keyed by its process's start time: another
/// process that gets the same pid later (the locker exited while a child
/// still holds the inherited `flock`) is not fenced.
#[test]
fn a_recycled_pid_is_not_taken_for_the_fenced_owner() {
    let f = fenced_fs();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    // A live process with a known pid stands for the recycled one; the
    // lock names its pid with another start time.
    let mut stand_in = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let pid = stand_in.id();
    let (_, real_start) = crate::locks::process_of(pid);
    assert!(real_start > 0);
    f.lock(turn.attr.ino, pid, now_unix_ms() + 60_000);
    let t = f.meta.locks();
    let mut l = t.local_locks(turn.attr.ino)[0];
    assert_eq!((l.pid, l.pid_start), (pid, real_start));
    // Re-recorded as taken by an earlier process with that pid.
    t.local_release_owner(turn.attr.ino, TURN_OWNER, now_unix_ms());
    l.pid_start = real_start.saturating_sub(1);
    assert_eq!(
        t.local_set(turn.attr.ino, l, now_unix_ms()),
        constellation_meta::locks::LocalOutcome::Done
    );
    let id = t.held(turn.attr.ino).unwrap().id;
    assert!(t.lost(turn.attr.ino, id));
    let caller = Caller::new(0, 0, Some(pid));
    assert_eq!(t.fenced_owners(now_unix_ms()).len(), 1);
    f.mkdir(&caller, "not-fenced").unwrap();
    stand_in.kill().unwrap();
    stand_in.wait().unwrap();
}

/// Plan 39 §3.7 composes with the owner fence: a discard is reported
/// `EIO` once on every description open at the time — a fenced owner's
/// `fsync` reports it with its own `EIO` (not again once the fence
/// lifts), another process's description still gets it at its own
/// `fsync`.
#[test]
fn a_fenced_owners_fsync_reports_a_pending_discard_once() {
    let f = fenced_fs();
    let me = std::process::id();
    let committer = Caller::new(0, 0, Some(me));
    let other = Caller::new(0, 0, Some(std::os::unix::process::parent_id()));
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let (obj, mine) = f.create(&root, ROOT_INO, "obj").unwrap();
    let theirs = f.open_write(&root, obj.attr.ino).unwrap();
    let fsync = |caller: &Caller, fh: Fh| {
        Blocking::run(|r| {
            f.fs.fsync(
                &OpCtx::new(OpKind::Fsync, caller),
                obj.attr.ino,
                fh,
                constellation_vfs::Durability::Configured,
                r,
            )
        })
        .map_err(|e| e.code())
    };
    f.lock(turn.attr.ino, me, now_unix_ms() + 60_000);
    // Data of `obj` was thrown away under an earlier lapse.
    f.meta.locks().note_discard(obj.attr.ino);
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    assert!(f.meta.locks().lost(turn.attr.ino, id));

    assert_eq!(fsync(&committer, mine.fh), Err(Code::Io), "fenced");
    f.unlock(&committer, turn.attr.ino).unwrap();
    assert_eq!(
        fsync(&committer, mine.fh),
        Ok(()),
        "the fenced EIO reported the discard"
    );
    assert_eq!(fsync(&other, theirs.fh), Err(Code::Io), "reported once");
    assert_eq!(fsync(&other, theirs.fh), Ok(()));
}

/// A blocked lock wait ends with `Intr` when its op is cancelled (the
/// FUSE adapter cancels a blocking `setlk` on any signal, as POSIX has
/// `F_SETLKW` return `EINTR`): stress-ng's `lockf`/`lockmix` workers,
/// which wait on each other, otherwise stayed unkillable in the kernel
/// for ever. The cancelled waiter holds nothing; the holder keeps its lock.
#[test]
fn a_cancelled_blocking_lock_wait_answers_intr_and_holds_nothing() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (file, _) = f.create(&root, ROOT_INO, "contended").unwrap();
    let ino = file.attr.ino;
    f.lock(ino, me, now_unix_ms() + 60_000);
    let cancel = constellation_vfs::CancelToken::new();
    let waiter = Caller::new(0, 0, Some(me));
    let wait = |cancel: &constellation_vfs::CancelToken| {
        Blocking::run(|r| {
            f.fs.lock_acquire(
                &OpCtx::new(OpKind::LockAcquire, &waiter).with_cancel(cancel),
                ino,
                Fh(ino),
                LockSpec {
                    owner: LockOwner(TURN_OWNER + 1),
                    range: LockRange { start: 0, end: 10 },
                    kind: LockKind::Write,
                    pid: me,
                },
                true,
                r,
            )
        })
    };
    let started = std::time::Instant::now();
    let result = std::thread::scope(|s| {
        let waiting = s.spawn(|| wait(&cancel));
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(!waiting.is_finished(), "the conflicting lock is held");
        cancel.cancel();
        waiting.join().unwrap()
    });
    assert_eq!(code(result), Code::Intr);
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    let held = f.meta.locks().local_locks(ino);
    assert_eq!(held.len(), 1, "only the holder's lock: {held:?}");
    assert_eq!(held[0].owner, TURN_OWNER);
    // Already cancelled when it would have to wait: `Intr` at once.
    assert_eq!(code(wait(&cancel)), Code::Intr);
    // Released, the next wait is granted.
    f.unlock(&root, ino).unwrap();
    wait(&constellation_vfs::CancelToken::new()).unwrap();
}

/// A blocked lock wait past its op's deadline answers `TimedOut` and
/// holds nothing, as a cancelled one answers `Intr`; the conformance
/// kit's `cancellation` group asserts the same through `Vfs`
/// (`a_timed_out_wait_leaves_no_waiter_behind`).
#[test]
fn a_blocking_lock_wait_past_its_deadline_answers_timed_out_and_holds_nothing() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (file, _) = f.create(&root, ROOT_INO, "contended").unwrap();
    let ino = file.attr.ino;
    f.lock(ino, me, now_unix_ms() + 60_000);
    let waiter = Caller::new(0, 0, Some(me));
    let wait = |within: std::time::Duration| {
        let started = std::time::Instant::now();
        let result = Blocking::run(|r| {
            f.fs.lock_acquire(
                &OpCtx::new(OpKind::LockAcquire, &waiter).with_deadline(started + within),
                ino,
                Fh(ino),
                LockSpec {
                    owner: LockOwner(TURN_OWNER + 1),
                    range: LockRange { start: 0, end: 10 },
                    kind: LockKind::Write,
                    pid: me,
                },
                true,
                r,
            )
        });
        (code(result), started.elapsed())
    };
    let (refused, took) = wait(std::time::Duration::from_millis(200));
    assert_eq!(refused, Code::TimedOut);
    assert!(
        took >= std::time::Duration::from_millis(200) && took < std::time::Duration::from_secs(5),
        "ended after {took:?}"
    );
    let held = f.meta.locks().local_locks(ino);
    assert_eq!(held.len(), 1, "only the holder's lock: {held:?}");
    assert_eq!(held[0].owner, TURN_OWNER);
    // Past its deadline already when it would have to wait: at once.
    assert_eq!(wait(std::time::Duration::ZERO).0, Code::TimedOut);
    // Released, the next wait is granted.
    f.unlock(&root, ino).unwrap();
    let granted = Blocking::run(|r| {
        f.fs.lock_acquire(
            &OpCtx::new(OpKind::LockAcquire, &waiter),
            ino,
            Fh(ino),
            LockSpec {
                owner: LockOwner(TURN_OWNER + 1),
                range: LockRange { start: 0, end: 10 },
                kind: LockKind::Write,
                pid: me,
            },
            true,
            r,
        )
    });
    granted.unwrap();
}

/// A deadline ends a blocking lock wait, never a non-blocking request:
/// that one is granted, or refused, or (here) goes on to ask the sequencer
/// — whose channel is closed in this fixture, so it answers `Io`, where a
/// deadline that applied to it would have answered `TimedOut` first.
#[test]
fn a_deadline_times_out_a_blocking_lock_request_and_never_a_non_blocking_one() {
    let f = fenced_fs();
    let root = Caller::root();
    let (file, _) = f.create(&root, ROOT_INO, "ungranted").unwrap();
    let ino = file.attr.ino;
    let (tx, closed) = tokio::sync::mpsc::unbounded_channel();
    drop(closed);
    let locks = Arc::new(crate::locks::ClusterLocks {
        meta: f.meta.clone(),
        tx,
        inval: None,
        lineage: Default::default(),
    });
    let lock = || constellation_meta::locks::LocalLock {
        owner: TURN_OWNER + 2,
        pid: std::process::id(),
        pid_start: 0,
        write: true,
        start: 0,
        end: 10,
    };
    let past = std::time::Instant::now();
    assert_eq!(
        locks.lock_here(ino, lock(), true, None, Some(past)),
        Err(Code::TimedOut)
    );
    assert_eq!(
        locks.lock_here(ino, lock(), false, None, Some(past)),
        Err(Code::Io)
    );
}

// ---- plan 30 §M14 phase 2: the fencing token ----

/// The owner's mutations carry the grant its lock is under (the token:
/// the grant and the window this node honours it for); another process's
/// carry nothing. While the owner's op is in flight its grant is not
/// released; a mutation it journals keeps the token in its replay row.
#[test]
fn the_lock_owners_mutations_carry_its_grant_and_nobody_elses_do() {
    let f = fenced_fs();
    let me = std::process::id();
    let committer = Caller::new(0, 0, Some(me));
    let other = Caller::new(0, 0, Some(std::os::unix::process::parent_id()));
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let until = now_unix_ms() + 60_000;
    f.lock(turn.attr.ino, me, until);
    let locks = f.fs.cluster_locks().unwrap().clone();
    let grant = GrantId { node: 9, seq: 1 };
    {
        let _scope = locks.tag_scope(Some(me), None);
        let tag = crate::locks::current_tag().unwrap();
        assert_eq!(
            tag.0,
            vec![constellation_meta::locks::LockToken {
                grant,
                until_ms: until
            }]
        );
        assert_eq!(
            f.meta.locks().release_blocked(grant, now_unix_ms()),
            Some(None),
            "the op is in flight"
        );
    }
    assert_eq!(f.meta.locks().release_blocked(grant, now_unix_ms()), None);
    {
        let _scope = locks.tag_scope(other.pid, None);
        assert!(crate::locks::current_tag().unwrap().is_empty());
    }
    // Through the view: the owner's create journals its token.
    f.meta.set_holder_epoch(1);
    f.create(&committer, ROOT_INO, "obj").unwrap();
    // Deposed: the unshipped create is rolled back and queued by rid.
    f.meta.set_holder_epoch(0);
    f.meta.strand_below_epoch(2).unwrap();
    let queued = f.meta.pending_replays().unwrap();
    let obj = queued
        .iter()
        .find(
            |q| matches!(&q.op, constellation_meta::MutateOp::Create { name, .. } if name == "obj"),
        )
        .unwrap_or_else(|| panic!("the create is queued for replay: {queued:?}"));
    assert_eq!(obj.lock_tag.0.len(), 1, "{obj:?}");
    assert_eq!(obj.lock_tag.0[0].grant, grant);
    assert!(f.meta.locks().stats().tagged_ops >= 2);
}

/// The gap phase 1 could not close, on the sequencer itself: the owner's
/// op passes the node-local fence (its token is taken while the grant is
/// honoured), the owner stalls past the grant, then the op reaches the
/// journal — refused there (`EIO`), nothing created.
#[test]
fn an_op_that_passed_the_fence_then_stalled_past_its_grant_is_refused() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    f.lock(turn.attr.ino, me, now_unix_ms() + 300);
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None);
    assert!(!locks.owner_fenced(Some(me), None), "the fence passes");
    let tag = crate::locks::current_tag().unwrap();
    assert_eq!(tag.0.len(), 1);
    // The stall: the grant's window ends before the op executes.
    crate::locks::advance_test_clock(400);
    let ino = f.meta.allocate_ino(ROOT_INO).unwrap();
    let r = f.fs.mutate_op(
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
    assert_eq!(r, Err(Code::Io));
    assert!(f.meta.lookup(ROOT_INO, "late").unwrap().is_none());
    assert_eq!(f.meta.locks().stats().token_rejections, 1);
}

/// Lock-fence-token review must-fix 2: a token is taken when its op is
/// sent, not when the op began. A close (or any op) that set out under
/// one window and was sent after its holder renewed carries the renewed
/// window, so a continuously renewed holder is not refused because its
/// op was slow; once the grant is no longer honoured here the op keeps
/// its old window and is refused where it lands.
#[test]
fn an_op_sent_after_a_renewal_carries_the_renewed_window() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let first = crate::locks::now_ms() + 300;
    f.lock(turn.attr.ino, me, first);
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None);
    // Taken at the op's start (as `flush` and `release` do).
    assert_eq!(crate::locks::current_tag().unwrap().0[0].until_ms, first);
    // Renewed meanwhile, then the old window passes.
    let renewed = crate::locks::now_ms() + 60_000;
    let mut h = f.meta.locks().held(turn.attr.ino).unwrap();
    h.until_ms = renewed;
    f.meta.locks().install_held(turn.attr.ino, h);
    crate::locks::advance_test_clock(400);
    let ino = f.meta.allocate_ino(ROOT_INO).unwrap();
    let create = |name: &str, ino| constellation_meta::MutateOp::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    f.fs.mutate_op(ROOT_INO, create("slow", ino)).unwrap();
    assert!(f.meta.lookup(ROOT_INO, "slow").unwrap().is_some());
    assert_eq!(f.meta.locks().stats().token_rejections, 0);
    // Past the renewed window too: refused.
    crate::locks::advance_test_clock(61_000);
    let ino = f.meta.allocate_ino(ROOT_INO).unwrap();
    assert_eq!(f.fs.mutate_op(ROOT_INO, create("late", ino)), Err(Code::Io));
    assert!(f.meta.lookup(ROOT_INO, "late").unwrap().is_none());
}

/// Must-fix 2's re-issue rule: an op refused for its token's window is
/// sent again (fresh rid, fresh token) only while every grant it named is
/// still held and honoured here under a window renewed since — never for
/// a grant that is no longer honoured, and not for one whose window did
/// not move (an ended grant is renewed no more), so it cannot loop.
#[test]
fn a_refused_op_is_reissued_only_under_a_renewed_honoured_grant() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    f.lock(turn.attr.ino, me, crate::locks::now_ms() + 300);
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None);
    assert!(!crate::locks::reissue_after_lapse(), "nothing sent yet");
    crate::locks::current_tag().unwrap();
    assert!(
        !crate::locks::reissue_after_lapse(),
        "the window did not move"
    );
    let mut h = f.meta.locks().held(turn.attr.ino).unwrap();
    h.until_ms = crate::locks::now_ms() + 5_000;
    f.meta.locks().install_held(turn.attr.ino, h);
    assert!(crate::locks::reissue_after_lapse(), "renewed and honoured");
    // The re-sent op takes the renewed window: nothing left to re-issue.
    crate::locks::current_tag().unwrap();
    assert!(!crate::locks::reissue_after_lapse());
    crate::locks::advance_test_clock(6_000);
    assert!(!crate::locks::reissue_after_lapse(), "no longer honoured");
}

/// Must-fix 2: a commit refused for a lock grant that truly ended (here
/// it lapsed between the close's fence check and the commit) is not
/// silently discarded: its content becomes a conflict copy (a replay
/// already refused, which the drain materializes under the view's root), the
/// file keeps what it had, and the close answers `EIO`.
#[test]
fn a_commit_refused_for_an_ended_grant_becomes_a_conflict_copy() {
    let f = fenced_fs();
    let me = std::process::id();
    let owner = Caller::new(0, 0, Some(me));
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let (file, opened) = f.create(&root, ROOT_INO, "data").unwrap();
    f.lock(turn.attr.ino, me, crate::locks::now_ms() + 300);
    let ino = file.attr.ino;
    let cx = OpCtx::new(OpKind::Write, &owner);
    assert_eq!(f.write(&cx, ino, opened.fh), Ok(6));
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None);
    crate::locks::current_tag().unwrap();
    // The grant lapses after the close's fence check, before its commit.
    crate::locks::advance_test_clock(400);
    assert_eq!(f.fs.flush_inode(ino, false), Err(Code::Io));
    assert_eq!(
        f.meta.getattr(ino).unwrap().unwrap().size,
        0,
        "not published"
    );
    let queued = f.meta.pending_replays().unwrap();
    let copy = queued
        .iter()
        .find(|q| {
            matches!(&q.op, constellation_meta::MutateOp::Publish { parent, name, size: 6, .. }
                if *parent == ROOT_INO && name == "data")
        })
        .unwrap_or_else(|| panic!("a conflict copy is queued at the root: {queued:?}"));
    assert!(copy.refused.is_some(), "already refused: never executed");
    assert!(
        f.fs.writes.detach(ino).is_none(),
        "the session went with it"
    );
}

/// Harness `lock-contention-stalled-owner`: an `fsync` (or a close) whose
/// own commit is refused for an ended grant — the store read its clock
/// past the grant's window while the commit waited for the writer lock —
/// answers `EIO`, and that `EIO` is its description's report of the
/// discard. The next `fsync` through it, of data written and published
/// under a window still honoured, answers 0. Before, the refusal's discard
/// event stayed owed to the very description that had just reported it:
/// the next increment's `fsync` answered `EIO` for data that landed, and
/// the counter ended one above what was acknowledged. The same for a close
/// and an `O_SYNC` write. A description that was open at the time and
/// reported nothing yet still reports it, once.
#[test]
fn a_refused_commits_eio_is_its_own_report_of_the_discard() {
    use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
    let mut f = fenced_fs();
    // The sync task: every drain and barrier succeeds.
    let mut core = std::mem::replace(&mut f._core, tokio::sync::mpsc::unbounded_channel().1);
    std::thread::spawn(move || {
        while let Some(req) = core.blocking_recv() {
            match req {
                SyncRequest::DrainInode { reply, .. } | SyncRequest::Barrier { reply, .. } => {
                    let _ = reply.send(Ok(()));
                }
                _ => {}
            }
        }
    });
    let me = std::process::id();
    let owner = Caller::new(0, 0, Some(me));
    let other = Caller::new(0, 0, Some(std::os::unix::process::parent_id()));
    let root = Caller::root();
    let (file, mine) = f.create(&root, ROOT_INO, "counter").unwrap();
    let ino = file.attr.ino;
    let theirs = f.open_write(&root, ino).unwrap();
    // The store's clock, `ahead` ms past this node's.
    let ahead = Arc::new(AtomicI64::new(0));
    let store_clock = ahead.clone();
    f.meta
        .locks()
        .set_clock(Arc::new(move || now_unix_ms() + store_clock.load(Relaxed)));
    f.lock(ino, me, now_unix_ms() + 60_000);
    let fsync = |caller: &Caller, fh: Fh| {
        Blocking::run(|r| {
            f.fs.fsync(
                &OpCtx::new(OpKind::Fsync, caller),
                ino,
                fh,
                constellation_vfs::Durability::Configured,
                r,
            )
        })
        .map_err(|e| e.code())
    };
    let flush = |caller: &Caller, fh: Fh| {
        Blocking::run(|r| {
            f.fs.flush(
                &OpCtx::new(OpKind::Flush, caller),
                ino,
                fh,
                LockOwner(0xf00),
                r,
            )
        })
        .map_err(|e| e.code())
    };
    let size = || f.meta.getattr(ino).unwrap().unwrap().size;
    let cx = OpCtx::new(OpKind::Write, &owner);

    assert_eq!(f.write(&cx, ino, mine.fh), Ok(6));
    ahead.store(120_000, Relaxed);
    assert_eq!(
        fsync(&owner, mine.fh),
        Err(Code::Io),
        "refused at the store"
    );
    assert_eq!(size(), 0, "not published");
    ahead.store(0, Relaxed);
    assert_eq!(f.write(&cx, ino, mine.fh), Ok(6));
    assert_eq!(
        fsync(&owner, mine.fh),
        Ok(()),
        "the refused fsync's EIO reported the discard"
    );
    assert_eq!(size(), 6, "published");

    // The same through a close (`FLUSH`) that a `dup` of the description
    // outlives.
    assert_eq!(f.write(&cx, ino, mine.fh), Ok(6));
    ahead.store(120_000, Relaxed);
    assert_eq!(
        flush(&owner, mine.fh),
        Err(Code::Io),
        "refused at the store"
    );
    ahead.store(0, Relaxed);
    assert_eq!(f.write(&cx, ino, mine.fh), Ok(6));
    assert_eq!(
        fsync(&owner, mine.fh),
        Ok(()),
        "the refused close's EIO reported the discard"
    );

    // And through an `O_SYNC` write, which publishes before it answers.
    let sync_write = |fh: Fh| {
        Blocking::run(|r| {
            f.fs.write(
                &cx,
                ino,
                fh,
                0,
                WriteData::Borrowed(b"object"),
                OpenFlags::WRITE | OpenFlags::SYNC,
                r,
            )
        })
        .map_err(|e| e.code())
    };
    ahead.store(120_000, Relaxed);
    assert_eq!(sync_write(mine.fh), Err(Code::Io), "refused at the store");
    ahead.store(0, Relaxed);
    assert_eq!(f.write(&cx, ino, mine.fh), Ok(6));
    assert_eq!(
        fsync(&owner, mine.fh),
        Ok(()),
        "the refused O_SYNC write's EIO reported the discard"
    );

    assert_eq!(fsync(&other, theirs.fh), Err(Code::Io), "reported once");
    assert_eq!(fsync(&other, theirs.fh), Ok(()));
}

/// Review round 2, must-fix 1: on a subtree view, the refused commit's
/// copy is queued under the view's root (named after the path below it),
/// not the filesystem root, and carries the file's owner and its owner
/// bits only (`0640` → `0600`).
#[test]
fn a_refused_commits_copy_stays_in_the_view_and_keeps_its_owner() {
    let mut f = fenced_fs();
    let vol = f.meta.mkdir(ROOT_INO, "vol", 0o755, 1000, 1000).unwrap();
    let db = f.meta.mkdir(vol.ino, "db", 0o700, 1000, 1000).unwrap();
    f.fs.set_subtree_root("/vol").unwrap();
    let me = std::process::id();
    let owner = Caller::new(1000, 1001, Some(me));
    let (turn, _) = f.create(&owner, ROOT_INO, "turn.lock").unwrap();
    let (file, opened) = f.create(&owner, db.ino, "data").unwrap();
    f.meta
        .setattr(file.attr.ino, Some(0o640), None, None, None, None, None)
        .unwrap();
    f.lock(turn.attr.ino, me, crate::locks::now_ms() + 300);
    let ino = file.attr.ino;
    let cx = OpCtx::new(OpKind::Write, &owner);
    assert_eq!(f.write(&cx, ino, opened.fh), Ok(6));
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None);
    crate::locks::current_tag().unwrap();
    crate::locks::advance_test_clock(400);
    assert_eq!(f.fs.flush_inode(ino, false), Err(Code::Io));
    let queued = f.meta.pending_replays().unwrap();
    let copy = queued
        .iter()
        .find(|q| matches!(&q.op, constellation_meta::MutateOp::Publish { size: 6, .. }))
        .unwrap_or_else(|| panic!("a conflict copy is queued: {queued:?}"));
    match &copy.op {
        constellation_meta::MutateOp::Publish {
            parent,
            name,
            mode,
            uid,
            gid,
            ..
        } => {
            assert_eq!(*parent, vol.ino, "under the view's root");
            assert_eq!(name, "db%2Fdata", "named below the view's root");
            assert_eq!((*uid, *gid, *mode), (1000, 1001, 0o600));
        }
        other => panic!("{other:?}"),
    }
}

/// An open file description's (`F_OFD_SETLK`) lock ends with the
/// description: FUSE sends no unlock for it on close, only `RELEASE` of
/// its handle (with no lock owner), which must drop it — and only it.
#[test]
fn releasing_a_handle_drops_the_locks_taken_through_it() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (e, a) = f.create(&caller, ROOT_INO, "ofd").unwrap();
    let ino = e.attr.ino;
    let b = f.open_write(&caller, ino).unwrap();
    f.meta.locks().install_held(
        ino,
        HeldGrant {
            id: GrantId { node: 9, seq: 1 },
            mode: LockMode::Exclusive,
            until_ms: i64::MAX,
            renew_at_ms: i64::MAX,
            owner: 9,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: now_unix_ms(),
        },
    );
    // Two descriptions, each its own owner (the kernel names an OFD
    // lock's owner after its `struct file`), on disjoint ranges.
    for (fh, owner, start) in [(a.fh, 0xa, 0), (b.fh, 0xb, 100)] {
        Blocking::run(|r| {
            f.fs.lock_acquire(
                &OpCtx::new(OpKind::LockAcquire, &caller),
                ino,
                fh,
                LockSpec {
                    owner: LockOwner(owner),
                    range: LockRange {
                        start,
                        end: start + 9,
                    },
                    kind: LockKind::Write,
                    pid: 0,
                },
                false,
                r,
            )
        })
        .unwrap();
    }
    assert_eq!(f.meta.locks().local_locks(ino).len(), 2);
    Blocking::run(|r| {
        f.fs.release(
            &OpCtx::new(OpKind::Release, &caller),
            ino,
            a.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    let left = f.meta.locks().local_locks(ino);
    assert_eq!(left.len(), 1, "{left:?}");
    assert_eq!(
        left[0].owner, 0xb,
        "only the released description's lock went"
    );
}

/// The kernel names an OFD owner after its `struct file`, which a new
/// open may reuse while the old description's `RELEASE` is still on its
/// way: that late release leaves the new description's lock alone.
#[test]
fn a_late_release_spares_a_reused_owners_new_lock() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (e, old) = f.create(&caller, ROOT_INO, "reuse").unwrap();
    let ino = e.attr.ino;
    let new = f.open_write(&caller, ino).unwrap();
    f.meta.locks().install_held(
        ino,
        HeldGrant {
            id: GrantId { node: 9, seq: 1 },
            mode: LockMode::Exclusive,
            until_ms: i64::MAX,
            renew_at_ms: i64::MAX,
            owner: 9,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: now_unix_ms(),
        },
    );
    let lock = |fh: Fh| {
        Blocking::run(|r| {
            f.fs.lock_acquire(
                &OpCtx::new(OpKind::LockAcquire, &caller),
                ino,
                fh,
                LockSpec {
                    owner: LockOwner(0x5a),
                    range: LockRange { start: 0, end: 9 },
                    kind: LockKind::Write,
                    pid: 0,
                },
                false,
                r,
            )
        })
        .unwrap()
    };
    lock(old.fh);
    lock(new.fh);
    Blocking::run(|r| {
        f.fs.release(
            &OpCtx::new(OpKind::Release, &caller),
            ino,
            old.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    assert_eq!(
        f.meta.locks().local_locks(ino).len(),
        1,
        "the new description keeps its lock"
    );
    Blocking::run(|r| {
        f.fs.release(
            &OpCtx::new(OpKind::Release, &caller),
            ino,
            new.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
    assert!(f.meta.locks().local_locks(ino).is_empty());
}

/// Two descriptions of one file, an exclusive grant held, and each a lock
/// owner of its own (an OFD or `flock` owner: the kernel names it after
/// the description).
fn two_descriptions(f: &Fenced, caller: &Caller, name: &str) -> (Ino, Fh, Fh) {
    let (e, a) = f.create(caller, ROOT_INO, name).unwrap();
    let ino = e.attr.ino;
    let b = f.open_write(caller, ino).unwrap();
    f.meta.locks().install_held(
        ino,
        HeldGrant {
            id: GrantId { node: 9, seq: 1 },
            mode: LockMode::Exclusive,
            until_ms: i64::MAX,
            renew_at_ms: i64::MAX,
            owner: 9,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: now_unix_ms(),
        },
    );
    (ino, a.fh, b.fh)
}

/// A non-blocking write lock of `owner` on `[0, 9]` through `fh`.
fn try_lock(f: &Fenced, caller: &Caller, ino: Ino, fh: Fh, owner: u64) -> VfsResult<()> {
    Blocking::run(|r| {
        f.fs.lock_acquire(
            &OpCtx::new(OpKind::LockAcquire, caller),
            ino,
            fh,
            LockSpec {
                owner: LockOwner(owner),
                range: LockRange { start: 0, end: 9 },
                kind: LockKind::Write,
                pid: 0,
            },
            false,
            r,
        )
    })
}

fn test_lock(f: &Fenced, caller: &Caller, ino: Ino, fh: Fh, owner: u64) -> LockStatus {
    Blocking::run(|r| {
        f.fs.lock_test(
            &OpCtx::new(OpKind::LockTest, caller),
            ino,
            fh,
            LockSpec {
                owner: LockOwner(owner),
                range: LockRange { start: 0, end: 9 },
                kind: LockKind::Write,
                pid: 0,
            },
            r,
        )
    })
    .unwrap()
}

/// `close(2)`'s `FLUSH` of `fh` by the process whose POSIX owner is
/// `0xf00` (not the lock's owner: an OFD or `flock` lock survives it).
fn close(f: &Fenced, caller: &Caller, ino: Ino, fh: Fh) {
    Blocking::run(|r| {
        f.fs.flush(
            &OpCtx::new(OpKind::Flush, caller),
            ino,
            fh,
            LockOwner(0xf00),
            r,
        )
    })
    .unwrap();
}

fn release(f: &Fenced, caller: &Caller, ino: Ino, fh: Fh) {
    Blocking::run(|r| {
        f.fs.release(
            &OpCtx::new(OpKind::Release, caller),
            ino,
            fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .unwrap();
}

/// The last `close(2)` of a description holding an OFD (or `flock`) lock
/// returns once its `FLUSH` is answered; the kernel sends the `RELEASE`
/// that drops the lock after that, in the background, and another worker
/// may serve the next request first. A non-blocking lock or a test of
/// another description that the process issues after the close returned
/// must not see the lock (xcheckfs: `EAGAIN` in 40–80 % of tries on a
/// mount, never on a local file system).
#[test]
fn a_lock_request_after_the_last_close_does_not_see_the_closed_descriptions_lock() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "closed");
    let (owner_a, owner_b) = (0xa, 0xb);
    for test in [false, true] {
        try_lock(&f, &caller, ino, a, owner_a).unwrap();
        close(&f, &caller, ino, a);
        std::thread::scope(|s| {
            // The `RELEASE`, on another worker, a moment later.
            s.spawn(|| {
                std::thread::sleep(Duration::from_millis(20));
                release(&f, &caller, ino, a);
            });
            if test {
                assert_eq!(
                    test_lock(&f, &caller, ino, b, owner_b),
                    LockStatus::Unlocked,
                    "F_GETLK after the last close"
                );
            } else {
                assert_eq!(
                    try_lock(&f, &caller, ino, b, owner_b),
                    Ok(()),
                    "F_SETLK after the last close"
                );
            }
        });
        Blocking::run(|r| {
            f.fs.lock_release(
                &OpCtx::new(OpKind::LockRelease, &caller),
                ino,
                b,
                LockOwner(owner_b),
                LockRange { start: 0, end: 9 },
                r,
            )
        })
        .unwrap();
        assert!(f.meta.locks().local_locks(ino).is_empty());
    }
}

/// A close that is not the description's last (a `dup`'s, a forked
/// child's, an `exec`'s `O_CLOEXEC` one) is answered by no `RELEASE`: the
/// description's lock stays, a conflicting request is still refused —
/// after the close's window at the latest — and once that window is over
/// it is refused at once again.
#[test]
fn a_dups_close_keeps_the_descriptions_lock() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "dup");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    close(&f, &caller, ino, a);
    let window = crate::locks::close_wait();
    let started = Instant::now();
    assert_eq!(code(try_lock(&f, &caller, ino, b, 0xb)), Code::Again);
    assert!(
        started.elapsed() < window + Duration::from_secs(2),
        "the wait is bounded by the close's window: {:?}",
        started.elapsed()
    );
    let started = Instant::now();
    assert_eq!(code(try_lock(&f, &caller, ino, b, 0xb)), Code::Again);
    assert!(
        matches!(
            test_lock(&f, &caller, ino, b, 0xb),
            LockStatus::Locked { .. }
        ),
        "F_GETLK still reports the open description's lock"
    );
    assert!(
        started.elapsed() < window,
        "past the window, refused without a wait: {:?}",
        started.elapsed()
    );
    // A lock request through the description proves it open: no wait.
    close(&f, &caller, ino, a);
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    let started = Instant::now();
    assert_eq!(code(try_lock(&f, &caller, ino, b, 0xb)), Code::Again);
    assert!(started.elapsed() < window, "{:?}", started.elapsed());
    assert_eq!(f.meta.locks().local_locks(ino).len(), 1);
    release(&f, &caller, ino, a);
    assert!(f.meta.locks().local_locks(ino).is_empty());
}

/// `F_UNLCK` of `owner`'s `[0, 9]` through `fh`.
fn unlock_range(f: &Fenced, caller: &Caller, ino: Ino, fh: Fh, owner: u64) {
    Blocking::run(|r| {
        f.fs.lock_release(
            &OpCtx::new(OpKind::LockRelease, caller),
            ino,
            fh,
            LockOwner(owner),
            LockRange { start: 0, end: 9 },
            r,
        )
    })
    .unwrap();
}

/// A close publishes what was written under the locks after it dropped
/// them (a `FLUSH` drops its caller's POSIX locks first; a `RELEASE` comes
/// after `close(2)` returned), under the grant's fencing token, and a new
/// turn waits for that publication (`Predecessor`). A non-blocking request
/// with nothing else in its way waits for it (a local file system would
/// grant it) instead of answering `EAGAIN`.
#[test]
fn a_non_blocking_lock_waits_for_the_earlier_turns_close_publication() {
    let f = fenced_fs();
    let me = std::process::id();
    let caller = Caller::new(0, 0, Some(me));
    let (ino, a, b) = two_descriptions(&f, &caller, "turns");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    let locks = f.fs.cluster_locks().unwrap().clone();
    let (tagged, in_flight) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        // The earlier turn's close, tagged with the grant while 0xa held
        // its lock, still publishing the file after the lock is gone.
        s.spawn(|| {
            let _scope = locks.tag_scope(Some(me), Some(0xa));
            let _publishing = crate::locks::begin_close(ino, true);
            tagged.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        });
        in_flight.recv().unwrap();
        release(&f, &caller, ino, a);
        assert!(f.meta.locks().local_locks(ino).is_empty());
        let before = f.meta.locks().stats().predecessor_waits;
        assert_eq!(
            try_lock(&f, &caller, ino, b, 0xb),
            Ok(()),
            "granted once the earlier turn's publication ended"
        );
        assert_eq!(
            f.meta.locks().stats().predecessor_waits,
            before + 1,
            "counted once per request, not per look"
        );
    });
    release(&f, &caller, ino, b);
}

/// Through the view: a `FLUSH` that drops its caller's POSIX lock and a
/// `RELEASE` that drops the description's OFD lock publish what was
/// written under it afterwards (a non-holder forwards the commit; the
/// core's answer is held back here). A non-blocking lock of another
/// description waits for that publication (`Predecessor`), from before
/// the drop, and is granted once the commit is answered. A close by a
/// process whose locks are on another file only (a commit unrelated to
/// any turn on this one) holds no new turn here.
#[test]
fn a_new_turn_waits_for_a_closes_publication_through_the_view() {
    for (last_close, ends_turn) in [(false, true), (true, true), (false, false)] {
        let mut f = fenced_fs();
        let me = std::process::id();
        let caller = Caller::new(0, 0, Some(me));
        let (ino, a, b) = two_descriptions(&f, &caller, "published");
        if ends_turn {
            try_lock(&f, &caller, ino, a, 0xa).unwrap();
        } else {
            // 0xa holds a lock on another file, none on this one, whose
            // grant stays cached here from an earlier turn.
            let (other, c, _) = two_descriptions(&f, &caller, "elsewhere");
            f.meta.locks().install_held(
                other,
                HeldGrant {
                    id: GrantId { node: 9, seq: 2 },
                    ..f.meta.locks().held(ino).unwrap()
                },
            );
            try_lock(&f, &caller, other, c, 0xa).unwrap();
        }
        f.write(&OpCtx::new(OpKind::Write, &caller), ino, a)
            .unwrap();
        // Not the holder from here on: the close's commit goes to the
        // core, which answers it when this test says so.
        f.fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
        let mut core = std::mem::replace(&mut f._core, tokio::sync::mpsc::unbounded_channel().1);
        let accepted = || {
            constellation_authority::ClientReply::Outcome(
                constellation_meta::MutateOutcome::Accepted {
                    epoch: 1,
                    records: Vec::new(),
                },
            )
        };
        let before = f.meta.locks().stats().predecessor_waits;
        std::thread::scope(|s| {
            let close = s.spawn(|| {
                if last_close {
                    release(&f, &caller, ino, a);
                } else {
                    Blocking::run(|r| {
                        f.fs.flush(
                            &OpCtx::new(OpKind::Flush, &caller),
                            ino,
                            a,
                            LockOwner(0xa),
                            r,
                        )
                    })
                    .unwrap();
                }
            });
            let started = Instant::now();
            let mut lock = None;
            let mut held = None;
            loop {
                match core.try_recv() {
                    Ok(SyncRequest::DrainInode { reply, .. }) => reply.send(Ok(())).unwrap(),
                    Ok(SyncRequest::Submit { reply, .. }) if held.is_none() && lock.is_none() => {
                        // The commit is on its way, the lock already gone.
                        assert!(f.meta.locks().local_locks(ino).is_empty());
                        held = Some(reply);
                        lock = Some(s.spawn(|| try_lock(&f, &caller, ino, b, 0xb)));
                    }
                    Ok(SyncRequest::Submit { reply, .. }) => reply.send(accepted()).unwrap(),
                    Ok(_) => {}
                    Err(_) if close.is_finished() && held.is_none() => break,
                    Err(_) => {
                        assert!(
                            started.elapsed() < Duration::from_secs(20),
                            "the close is stuck"
                        );
                        let waiting = f.meta.locks().stats().predecessor_waits == before + 1;
                        let answered = lock.as_ref().is_some_and(|l| l.is_finished());
                        if held.is_some() && (waiting || answered) {
                            // The lock request waits for the publication
                            // (or was answered without): answer the commit.
                            assert_eq!(answered, !ends_turn, "waits only behind its turn's close");
                            held.take().unwrap().send(accepted()).unwrap();
                        }
                        std::thread::sleep(Duration::from_micros(200));
                    }
                }
            }
            let lock = lock.expect("the close forwarded its commit");
            assert_eq!(
                lock.join().unwrap(),
                Ok(()),
                "granted (last close: {last_close}, ends a turn: {ends_turn})"
            );
        });
        assert_eq!(
            f.meta.locks().stats().predecessor_waits,
            before + u64::from(ends_turn)
        );
        unlock_range(&f, &caller, ino, b, 0xb);
    }
}

/// Whether `h` finished within a few seconds.
fn done_soon<T>(h: &std::thread::ScopedJoinHandle<'_, T>) -> bool {
    let started = Instant::now();
    while !h.is_finished() && started.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(1));
    }
    h.is_finished()
}

/// A close that ends its caller's turn under `--write-mode through`, on
/// the holder: its commit is local and answered at once, then the close
/// waits for the file's chunks to reach S3 (held back here). The turn's
/// publication is that commit (`crate::locks::begin_close`): while the
/// upload still waits, a non-blocking lock of the next turn is granted
/// with no predecessor wait, and a write of the file goes on (the file's
/// operation lock ended with the commit too). Both used to wait for the
/// upload — the lock up to the close wait, then `EAGAIN` with no lock in
/// the way; every op on the file for the whole upload.
#[test]
fn a_new_turn_does_not_wait_for_a_closes_upload() {
    let mut f = fenced_fs();
    f.fs.sync
        .as_ref()
        .unwrap()
        .write_mode
        .set(crate::writeback::WriteMode::Through);
    let me = std::process::id();
    let caller = Caller::new(0, 0, Some(me));
    let (ino, a, b) = two_descriptions(&f, &caller, "uploading");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    f.write(&OpCtx::new(OpKind::Write, &caller), ino, a)
        .unwrap();
    let mut core = std::mem::replace(&mut f._core, tokio::sync::mpsc::unbounded_channel().1);
    let before = f.meta.locks().stats().predecessor_waits;
    std::thread::scope(|s| {
        let close = s.spawn(|| {
            Blocking::run(|r| {
                f.fs.flush(
                    &OpCtx::new(OpKind::Flush, &caller),
                    ino,
                    a,
                    LockOwner(0xa),
                    r,
                )
            })
        });
        let started = Instant::now();
        let drained = loop {
            match core.try_recv() {
                Ok(SyncRequest::DrainInode { ino: i, reply, .. }) if i == ino => break reply,
                Ok(_) => {}
                Err(_) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(5),
                        "the close never asked for its drain"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        };
        assert!(
            f.meta.locks().local_locks(ino).is_empty(),
            "the close dropped its caller's lock"
        );
        let lock = s.spawn(|| try_lock(&f, &caller, ino, b, 0xb));
        let locked = done_soon(&lock);
        let write = s.spawn(|| f.write(&OpCtx::new(OpKind::Write, &caller), ino, b));
        let wrote = done_soon(&write);
        // Answered whatever happened: a write still waiting never ends.
        drained.send(Ok(())).unwrap();
        close.join().unwrap().unwrap();
        assert!(locked, "the next turn's lock waited for the upload");
        assert_eq!(lock.join().unwrap(), Ok(()), "granted during the upload");
        assert!(wrote, "a write waited for the same file's upload");
        write.join().unwrap().unwrap();
    });
    assert_eq!(
        f.meta.locks().stats().predecessor_waits,
        before,
        "the next turn did not wait for the closed turn's upload"
    );
    unlock_range(&f, &caller, ino, b, 0xb);
}

/// Every op a process issues while it holds a lock carries the lock's
/// grant (the fencing token is per process), so another thread's op —
/// a slow `fsync` of another file, or, through xcheckfs, which mirrors
/// every application through one process, any application's — can still
/// be in flight under the grant when the turn ends. It is concurrent with
/// the unlock (its caller has no answer yet), and the next turn does not
/// wait for it: a non-blocking lock nobody else holds is granted at once.
/// It used to wait for the op up to the close wait, then answer `EAGAIN`
/// with no lock in the way (xcheckfs: `setlk` OK on ext4, conflict here,
/// under load; one process, 40 of 5400 `F_SETLK`s beside a thread
/// writing and fsyncing another file).
#[test]
fn a_non_blocking_lock_does_not_wait_for_another_threads_unanswered_op() {
    let f = fenced_fs();
    let me = std::process::id();
    let caller = Caller::new(0, 0, Some(me));
    let (ino, a, b) = two_descriptions(&f, &caller, "concurrent");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    let locks = f.fs.cluster_locks().unwrap().clone();
    let (tagged, in_flight) = std::sync::mpsc::channel();
    let (answered, done) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|s| {
        // The other thread's op, tagged with the grant while 0xa held its
        // lock, unanswered until the lock request below is.
        s.spawn(move || {
            let _scope = locks.tag_scope(Some(me), Some(0xa));
            crate::locks::current_tag().unwrap();
            tagged.send(()).unwrap();
            let _ = done.recv_timeout(Duration::from_secs(10));
        });
        in_flight.recv().unwrap();
        unlock_range(&f, &caller, ino, a, 0xa);
        assert!(f.meta.locks().local_locks(ino).is_empty());
        let started = Instant::now();
        let r = try_lock(&f, &caller, ino, b, 0xb);
        let took = started.elapsed();
        answered.send(()).unwrap();
        assert_eq!(r, Ok(()), "F_SETLK with no lock in the way");
        assert!(
            took < crate::locks::close_wait(),
            "granted without waiting for the unanswered op: {took:?}"
        );
    });
    unlock_range(&f, &caller, ino, b, 0xb);
    release(&f, &caller, ino, a);
    release(&f, &caller, ino, b);
}

/// An op that ended in doubt may still execute at the minter, which
/// honours the grant, after the next turn's writes: its grant is pinned
/// from the moment it was found in doubt, before its answer lets its
/// caller unlock — not only once its scope ends — and a new turn waits
/// for the pin (a non-blocking one answers `EAGAIN` after the close wait).
#[test]
fn a_new_turn_waits_for_an_op_in_doubt_before_its_scope_ends() {
    let f = fenced_fs();
    let me = std::process::id();
    let caller = Caller::new(0, 0, Some(me));
    let (ino, a, b) = two_descriptions(&f, &caller, "doubt");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    let locks = f.fs.cluster_locks().unwrap().clone();
    let (tagged, in_doubt) = std::sync::mpsc::channel();
    let (answered, done) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|s| {
        s.spawn(move || {
            let _scope = locks.tag_scope(Some(me), Some(0xa));
            crate::locks::current_tag().unwrap();
            crate::locks::note_tag_in_doubt(None);
            tagged.send(()).unwrap();
            let _ = done.recv_timeout(Duration::from_secs(10));
        });
        in_doubt.recv().unwrap();
        unlock_range(&f, &caller, ino, a, 0xa);
        let started = Instant::now();
        let r = try_lock(&f, &caller, ino, b, 0xb);
        let took = started.elapsed();
        answered.send(()).unwrap();
        assert_eq!(code(r), Code::Again, "the op in doubt may still land");
        assert!(
            took >= crate::locks::close_wait(),
            "refused after waiting for the pin, not at once: {took:?}"
        );
    });
    release(&f, &caller, ino, a);
    release(&f, &caller, ino, b);
}

/// The review's must-fix: a description whose descriptors keep being
/// closed (`exec 9>lock; flock -n 9 || exit; while :; do cmd; done` —
/// each child's exit flushes descriptor 9) renews its close window with
/// every `FLUSH`. A conflicting non-blocking request (or test) is still
/// refused within its own bound, not held for as long as the closes go
/// on.
#[test]
fn closes_that_keep_coming_do_not_hold_a_request_past_its_bound() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "renewed");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    close(&f, &caller, ino, a);
    let window = crate::locks::close_wait();
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(window / 4);
                close(&f, &caller, ino, a);
            }
        });
        let started = Instant::now();
        let refused = code(try_lock(&f, &caller, ino, b, 0xb));
        let took_lock = started.elapsed();
        let started = Instant::now();
        let tested = test_lock(&f, &caller, ino, b, 0xb);
        let took_test = started.elapsed();
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(refused, Code::Again);
        assert!(matches!(tested, LockStatus::Locked { .. }));
        for took in [took_lock, took_test] {
            assert!(
                took < window + Duration::from_millis(500),
                "bounded by the request's own deadline: {took:?}"
            );
        }
    });
    release(&f, &caller, ino, a);
}

/// An unlock or a test through the description shows it open: a
/// conflicting request is refused without waiting out the close window.
#[test]
fn an_unlock_or_a_test_through_the_description_ends_its_close_window() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "shown-open");
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    let window = crate::locks::close_wait();
    for through in ["unlock", "test"] {
        close(&f, &caller, ino, a);
        if through == "unlock" {
            // Of a range the lock does not cover: the lock stays.
            Blocking::run(|r| {
                f.fs.lock_release(
                    &OpCtx::new(OpKind::LockRelease, &caller),
                    ino,
                    a,
                    LockOwner(0xa),
                    LockRange {
                        start: 100,
                        end: 109,
                    },
                    r,
                )
            })
            .unwrap();
        } else {
            assert_eq!(test_lock(&f, &caller, ino, a, 0xa), LockStatus::Unlocked);
        }
        let started = Instant::now();
        assert_eq!(code(try_lock(&f, &caller, ino, b, 0xb)), Code::Again);
        assert!(
            started.elapsed() < window,
            "{through}: {:?}",
            started.elapsed()
        );
    }
    release(&f, &caller, ino, a);
}

/// A view served by a single worker (`FrontendCaps::single_worker`)
/// waits for no closing description: its `RELEASE` is queued before the
/// next request and served first, and a wait would hold the one worker
/// that serves it. A just-closed description's lock is refused at once.
#[test]
fn a_single_worker_does_not_wait_for_a_closing_description() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "one-worker");
    f.fs.frontend_negotiated(&FrontendCaps {
        single_worker: true,
        ..f.fs.caps.clone()
    });
    try_lock(&f, &caller, ino, a, 0xa).unwrap();
    close(&f, &caller, ino, a);
    let window = crate::locks::close_wait();
    let started = Instant::now();
    assert_eq!(code(try_lock(&f, &caller, ino, b, 0xb)), Code::Again);
    assert!(
        matches!(
            test_lock(&f, &caller, ino, b, 0xb),
            LockStatus::Locked { .. }
        ),
        "the description is not released yet"
    );
    assert!(
        started.elapsed() < window / 2,
        "no wait: {:?}",
        started.elapsed()
    );
    release(&f, &caller, ino, a);
    try_lock(&f, &caller, ino, b, 0xb).unwrap();
    release(&f, &caller, ino, b);
}

/// A lock of `owner` through `fh`, `write` or not, on `[start, end]`.
fn lock_range(
    f: &Fenced,
    caller: &Caller,
    ino: Ino,
    fh: Fh,
    owner: u64,
    write: bool,
    (start, end): (u64, u64),
) -> VfsResult<()> {
    Blocking::run(|r| {
        f.fs.lock_acquire(
            &OpCtx::new(OpKind::LockAcquire, caller),
            ino,
            fh,
            LockSpec {
                owner: LockOwner(owner),
                range: LockRange { start, end },
                kind: if write {
                    LockKind::Write
                } else {
                    LockKind::Read
                },
                pid: 0,
            },
            false,
            r,
        )
    })
}

/// Finding 12 (xcheckfs, seed 77, op 3161): a description holding
/// `W[48,48]` is closed, and before its `RELEASE` arrives another owner
/// tests `W[43,66]`, which another description's live `R[53,EOF]` is in
/// the way of too. The test does not wait (a live lock is in the way),
/// and it named the closing description's lock, the first in the table,
/// where Linux, which dropped that one at the close, names the live one.
#[test]
fn a_test_names_a_live_lock_before_one_whose_release_is_on_its_way() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "going");
    let c = f.open_write(&caller, ino).unwrap().fh;
    lock_range(&f, &caller, ino, a, 0xa, true, (48, 48)).unwrap();
    lock_range(&f, &caller, ino, c, 0xc, false, (53, u64::MAX >> 1)).unwrap();
    close(&f, &caller, ino, a);
    let started = Instant::now();
    let found = Blocking::run(|r| {
        f.fs.lock_test(
            &OpCtx::new(OpKind::LockTest, &caller),
            ino,
            b,
            LockSpec {
                owner: LockOwner(0xb),
                range: LockRange { start: 43, end: 66 },
                kind: LockKind::Write,
                pid: 0,
            },
            r,
        )
    })
    .unwrap();
    assert!(
        matches!(
            found,
            LockStatus::Locked {
                range: LockRange { start: 53, .. },
                kind: LockKind::Read,
                ..
            }
        ),
        "{found:?}"
    );
    assert!(
        started.elapsed() < crate::locks::close_wait(),
        "a live lock is in the way: no wait"
    );
    release(&f, &caller, ino, a);
    release(&f, &caller, ino, c);
}

/// Finding 12, the other cause: the kernel names an OFD owner after its
/// `struct file`, and a description opened right after another's last
/// close can get its address, and so its owner id, before that one's
/// `RELEASE` arrives. Its first lock moved the owner to the new handle,
/// with the closed description's locks still under it (merged with the
/// new one's): the old `RELEASE` then found nothing of the owner's to
/// drop, and they stayed, held by nobody on Linux, until the new
/// description was released. The request now waits for that `RELEASE`.
#[test]
fn a_reused_owner_id_does_not_adopt_the_closed_descriptions_locks() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "reused");
    lock_range(&f, &caller, ino, a, 0xa, false, (4, 17)).unwrap();
    close(&f, &caller, ino, a);
    // The new description, at the old one's address: the same owner id.
    let c = f.open_write(&caller, ino).unwrap().fh;
    std::thread::scope(|s| {
        s.spawn(|| {
            std::thread::sleep(Duration::from_millis(20));
            release(&f, &caller, ino, a);
        });
        lock_range(&f, &caller, ino, c, 0xa, false, (13, 20)).unwrap();
    });
    let held: Vec<_> = f
        .meta
        .locks()
        .local_locks(ino)
        .iter()
        .map(|l| (l.owner, l.start, l.end))
        .collect();
    assert_eq!(held, [(0xa, 13, 20)], "only the new description's lock");
    lock_range(&f, &caller, ino, b, 0xb, true, (4, 12)).expect("nothing held there any more");
    release(&f, &caller, ino, b);
    release(&f, &caller, ino, c);
}

/// A POSIX owner locks through several descriptions of a file, and
/// another process's close of one of them (a child's copy) is followed by
/// no `RELEASE`: its next lock through another description waits out
/// the close window at most, then goes on with every lock kept.
#[test]
fn a_posix_owner_keeps_its_locks_when_another_process_closes_a_shared_description() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, a, b) = two_descriptions(&f, &caller, "shared");
    lock_range(&f, &caller, ino, a, 0xa, true, (0, 9)).unwrap();
    close(&f, &caller, ino, a);
    let started = Instant::now();
    lock_range(&f, &caller, ino, b, 0xa, true, (30, 39)).unwrap();
    assert!(
        started.elapsed() < crate::locks::close_wait() + Duration::from_millis(500),
        "bounded: {:?}",
        started.elapsed()
    );
    let held: Vec<_> = f
        .meta
        .locks()
        .local_locks(ino)
        .iter()
        .map(|l| (l.owner, l.start, l.end))
        .collect();
    assert_eq!(held, [(0xa, 0, 9), (0xa, 30, 39)]);
    release(&f, &caller, ino, a);
    release(&f, &caller, ino, b);
}

/// The `RELEASE` of `fh` naming its `flock` owner
/// (`FUSE_RELEASE_FLOCK_UNLOCK`).
fn release_flock(f: &Fenced, caller: &Caller, ino: Ino, fh: Fh, owner: u64) {
    Blocking::run(|r| {
        f.fs.release(
            &OpCtx::new(OpKind::Release, caller),
            ino,
            fh,
            OpenFlags::WRITE,
            Some(LockOwner(owner)),
            r,
        )
    })
    .unwrap();
}

/// [`a_late_release_spares_a_reused_owners_new_lock`] for `flock`: the
/// late `RELEASE` names the owner itself (`FUSE_RELEASE_FLOCK_UNLOCK`),
/// and it dropped the new description's lock under that id.
#[test]
fn a_late_flock_release_spares_a_reused_owners_new_lock() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, old, new) = two_descriptions(&f, &caller, "reuse-flock");
    lock_range(&f, &caller, ino, old, 0x5a, true, (0, u64::MAX >> 1)).unwrap();
    lock_range(&f, &caller, ino, new, 0x5a, true, (0, u64::MAX >> 1)).unwrap();
    release_flock(&f, &caller, ino, old, 0x5a);
    assert_eq!(
        f.meta.locks().local_locks(ino).len(),
        1,
        "the new description keeps its lock"
    );
    release_flock(&f, &caller, ino, new, 0x5a);
    assert!(f.meta.locks().local_locks(ino).is_empty());
}

/// `flock(fd1, LOCK_EX); flock(fd1, LOCK_UN); close(fd1); fd2 =
/// open(same file); flock(fd2, LOCK_EX)` with `fd2`'s description at
/// `fd1`'s address: the owner holds nothing at the close, so the new
/// lock does not wait, and `fd1`'s late `RELEASE`, naming the same
/// owner, dropped the live lock, which another process could then take.
#[test]
fn an_unlocked_closed_descriptions_late_release_spares_the_new_flock() {
    let f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (ino, fd1, other) = two_descriptions(&f, &caller, "unlocked-reuse");
    let whole = (0, u64::MAX >> 1);
    lock_range(&f, &caller, ino, fd1, 0x5a, true, whole).unwrap();
    Blocking::run(|r| {
        f.fs.lock_release(
            &OpCtx::new(OpKind::LockRelease, &caller),
            ino,
            fd1,
            LockOwner(0x5a),
            LockRange {
                start: whole.0,
                end: whole.1,
            },
            r,
        )
    })
    .unwrap();
    close(&f, &caller, ino, fd1);
    let fd2 = f.open_write(&caller, ino).unwrap().fh;
    let started = Instant::now();
    lock_range(&f, &caller, ino, fd2, 0x5a, true, whole).unwrap();
    assert!(
        started.elapsed() < crate::locks::close_wait() / 2,
        "nothing held at the close: no wait ({:?})",
        started.elapsed()
    );
    release_flock(&f, &caller, ino, fd1, 0x5a);
    assert_eq!(
        f.meta.locks().local_locks(ino).len(),
        1,
        "fd2 keeps its lock"
    );
    assert_eq!(
        code(lock_range(&f, &caller, ino, other, 0x77, true, whole)),
        Code::Again,
        "and nobody else gets it"
    );
    release_flock(&f, &caller, ino, fd2, 0x5a);
    release(&f, &caller, ino, other);
}

/// A copy is a run of writes, fenced step by step (`view::copy`): a lock
/// on its destination whose grant lapses between two steps ends it short
/// (the node's lock fence, for any caller), and so does the caller's own
/// grant lapsing on another file (the owner fence) — the bytes answered
/// are the ones written before.
#[test]
fn a_grant_lapsing_between_a_copys_steps_ends_it_short() {
    let f = fenced_fs();
    let me = std::process::id();
    let committer = Caller::new(0, 0, Some(me));
    let other = Caller::new(0, 0, Some(std::os::unix::process::parent_id()));
    let root = Caller::root();
    let cs = u64::from(f.fs.chunk_size);
    let (src, so) = f.create(&root, ROOT_INO, "src").unwrap();
    Blocking::run(|r| {
        f.fs.write(
            &OpCtx::new(OpKind::Write, &root),
            src.attr.ino,
            so.fh,
            0,
            WriteData::Borrowed(b"0123456789abcdefghij"),
            OpenFlags::WRITE,
            r,
        )
    })
    .unwrap();
    let copy = |caller: &Caller, dst: Ino, fh: Fh| {
        Blocking::run(|r| {
            f.fs.copy_file_range(
                &OpCtx::new(OpKind::CopyFileRange, caller),
                src.attr.ino,
                so.fh,
                0,
                dst,
                fh,
                cs - 10,
                20,
                constellation_vfs::CopyFlags::empty(),
                r,
            )
        })
    };
    let size = |ino| f.fs.current_attr(ino).unwrap().unwrap().size;

    // The destination's own lock lapses: the lock fence.
    let (locked, lo) = f.create(&root, ROOT_INO, "locked").unwrap();
    f.lock(locked.attr.ino, me, now_unix_ms() + 60_000);
    let id = f.meta.locks().held(locked.attr.ino).unwrap().id;
    let (meta, ino) = (f.meta.clone(), locked.attr.ino);
    let hook = super::vfs_tests::BetweenSteps::set(move |_| {
        assert!(meta.locks().lost(ino, id));
    });
    assert_eq!(copy(&other, locked.attr.ino, lo.fh).unwrap(), 10);
    drop(hook);
    assert_eq!(size(locked.attr.ino), cs);

    // The caller's grant on another file lapses: the owner fence.
    let f = fenced_fs();
    let (src2, so2) = f.create(&root, ROOT_INO, "src").unwrap();
    Blocking::run(|r| {
        f.fs.write(
            &OpCtx::new(OpKind::Write, &root),
            src2.attr.ino,
            so2.fh,
            0,
            WriteData::Borrowed(b"0123456789abcdefghij"),
            OpenFlags::WRITE,
            r,
        )
    })
    .unwrap();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let (obj, oo) = f.create(&root, ROOT_INO, "obj").unwrap();
    f.lock(turn.attr.ino, me, now_unix_ms() + 60_000);
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    let (meta, ino) = (f.meta.clone(), turn.attr.ino);
    let hook = super::vfs_tests::BetweenSteps::set(move |_| {
        assert!(meta.locks().lost(ino, id));
    });
    let n = Blocking::run(|r| {
        f.fs.copy_file_range(
            &OpCtx::new(OpKind::CopyFileRange, &committer),
            src2.attr.ino,
            so2.fh,
            0,
            obj.attr.ino,
            oo.fh,
            cs - 10,
            20,
            constellation_vfs::CopyFlags::empty(),
            r,
        )
    })
    .unwrap();
    drop(hook);
    assert_eq!(n, 10);
    assert_eq!(f.fs.current_attr(obj.attr.ino).unwrap().unwrap().size, cs);
}
