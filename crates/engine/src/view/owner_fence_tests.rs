//! Plan 30 §M14, the owner fence, through the `Vfs` trait against a real
//! [`View`]: a lock owner whose grant lapsed with its `flock` held gets
//! `EIO` from its writes and namespace ops on *other* files (git under
//! one turn lock creates, links and renames everywhere); another process
//! on the node does not; the fence lifts with the owner's unlock.
use super::durable_ack_tests::holder_fs;
use super::*;
use crate::locks::Reissue;
use constellation_authority::HonourMs;
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::locks::{Fence, GrantId, HeldGrant, LockMode};
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
    fenced_fs_waiting(std::time::Duration::from_secs(50))
}

/// [`fenced_fs`], whose non-blocking lock requests give up waiting for an
/// owner's grace after `owner_wait_max`.
fn fenced_fs_waiting(owner_wait_max: std::time::Duration) -> Fenced {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut fs, dir, core) = holder_fs(meta.clone(), false);
    let (tx, locks_core) = tokio::sync::mpsc::unbounded_channel();
    fs.sync.as_mut().unwrap().locks = Some(Arc::new(crate::locks::ClusterLocks {
        meta: meta.clone(),
        tx,
        inval: None,
        lineage: Default::default(),
        owner_wait_max,
        poll_hint: std::time::Duration::from_millis(50),
        waits: Default::default(),
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
                constellation_vfs::Unit(r),
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
    fn lock(&self, ino: Ino, pid: u32, until: HonourMs) {
        let t = self.meta.locks();
        let now = crate::locks::now().honour;
        t.install_held(
            ino,
            HeldGrant {
                id: GrantId { node: 9, seq: 1 },
                mode: LockMode::Exclusive,
                until,
                renew_at: until,
                owner: 9,
                recalled: false,
                position: constellation_meta::Position::ZERO,
                renewing: None,
                releasing: false,
                first_use: false,
                idle_since: None,
                installed: now,
                lapsing: None,
                written: false,
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(60_000));

    // Under an honoured grant nothing is fenced.
    f.write(&OpCtx::new(OpKind::Write, &committer), obj.attr.ino, oo.fh)
        .unwrap();
    f.mkdir(&committer, "objects").unwrap();

    // The grant is lost (a stall past the ttl: the owner outwaited it).
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    assert!(f.meta.locks().lost(turn.attr.ino, id, crate::locks::now()));

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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(300));
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

    f.lock(
        turn.attr.ino,
        locker,
        crate::locks::now().honour.plus(60_000),
    );
    let l = f.meta.locks().local_locks(turn.attr.ino)[0];
    assert_eq!(l.pid, me, "the lock names the locker's process");
    assert!(l.pid_start > 0, "and its start time");
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    assert!(f.meta.locks().lost(turn.attr.ino, id, crate::locks::now()));

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
    f.lock(turn.attr.ino, pid, crate::locks::now().honour.plus(60_000));
    let t = f.meta.locks();
    let mut l = t.local_locks(turn.attr.ino)[0];
    assert_eq!((l.pid, l.pid_start), (pid, real_start));
    // Re-recorded as taken by an earlier process with that pid.
    t.local_release_owner(turn.attr.ino, TURN_OWNER, crate::locks::now());
    l.pid_start = real_start.saturating_sub(1);
    assert_eq!(
        t.local_set(turn.attr.ino, l, crate::locks::now()),
        constellation_meta::locks::LocalOutcome::Done
    );
    let id = t.held(turn.attr.ino).unwrap().id;
    assert!(t.lost(turn.attr.ino, id, crate::locks::now()));
    let caller = Caller::new(0, 0, Some(pid));
    assert_eq!(t.fenced_owners(crate::locks::now()).len(), 1);
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(60_000));
    // Data of `obj` was thrown away under an earlier lapse.
    f.meta.locks().note_discard(obj.attr.ino);
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    assert!(f.meta.locks().lost(turn.attr.ino, id, crate::locks::now()));

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
    f.lock(ino, me, crate::locks::now().honour.plus(60_000));
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

/// Every round of one blocking lock call carries how long the application
/// has waited since the call began (`waited_ms`), so that a call that lost
/// a round keeps its place in the owner's queue; the first round carries
/// nothing, and a round that got a grant does not lengthen the back-off of
/// the next (a client that lost the local race to another is not a reason
/// to wait longer than the others).
#[test]
fn every_round_of_a_blocking_lock_call_carries_how_long_it_has_waited() {
    let mut f = fenced_fs();
    let root = Caller::root();
    let (file, _) = f.create(&root, ROOT_INO, "rounds").unwrap();
    let ino = file.attr.ino;
    let (seen, waits) = script_core_waits(
        &mut f,
        ino,
        vec![
            Scripted::WouldBlock,
            Scripted::Wait(40),
            Scripted::WouldBlock,
            Scripted::Grant,
        ],
    );
    let me = std::process::id();
    let waiter = Caller::new(0, 0, Some(me));
    Blocking::run(|r| {
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
    })
    .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 4);
    let waits = waits.lock().unwrap().clone();
    assert_eq!(
        waits[0], 0,
        "the first round: nothing waited yet: {waits:?}"
    );
    assert!(
        waits.windows(2).all(|w| w[0] <= w[1]) && waits[3] >= 40,
        "each round carries what the call has waited since it began: {waits:?}"
    );
}

/// A core that keeps answering `Granted` for a lock the local tables do
/// not take (`local_set` still says `NeedGrant`) is not spun on: the
/// back-off the rounds build is kept (it was reset by every `Granted`: no
/// pause at all). Eight such rounds pause 10 ms times 2 to 8, 350 ms.
#[test]
fn a_grant_the_local_tables_refuse_keeps_the_back_off() {
    let mut f = fenced_fs();
    let root = Caller::root();
    let (file, _) = f.create(&root, ROOT_INO, "refused").unwrap();
    let ino = file.attr.ino;
    let mut script = vec![Scripted::GrantNothing; 8];
    script.push(Scripted::Grant);
    let seen = script_core(&mut f, ino, script);
    let me = std::process::id();
    let waiter = Caller::new(0, 0, Some(me));
    let started = std::time::Instant::now();
    Blocking::run(|r| {
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
    })
    .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 9);
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(300),
        "no back-off between rounds that were granted and refused: {:?}",
        started.elapsed()
    );
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
    f.lock(ino, me, crate::locks::now().honour.plus(60_000));
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
        owner_wait_max: std::time::Duration::from_secs(50),
        poll_hint: std::time::Duration::from_millis(50),
        waits: Default::default(),
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
    let until = crate::locks::now().honour.plus(60_000);
    f.lock(turn.attr.ino, me, until);
    let locks = f.fs.cluster_locks().unwrap().clone();
    let grant = GrantId { node: 9, seq: 1 };
    {
        let _scope = locks.tag_scope(Some(me), None, None);
        let tag = crate::locks::current_tag().unwrap();
        // The token: the honour window minted on the wall clock now.
        assert_eq!(tag.0.len(), 1);
        assert_eq!(tag.0[0].grant, grant);
        assert!(
            (tag.0[0].until_ms - crate::locks::now().wall_of_honour(until).0).abs() <= 5,
            "{tag:?}"
        );
        assert_eq!(
            f.meta.locks().release_blocked(grant, crate::locks::now()),
            Some(None),
            "the op is in flight"
        );
    }
    assert_eq!(
        f.meta.locks().release_blocked(grant, crate::locks::now()),
        None
    );
    {
        let _scope = locks.tag_scope(other.pid, None, None);
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(300));
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None, None);
    assert_eq!(
        locks.owner_fenced(Some(me), None),
        Fence::Ok,
        "the fence passes"
    );
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
/// Plan 43 C7-H1 (2/4): a grant this node holds is honoured until its end on
/// the honour clock, which a backward wall step does not move: its owner is
/// fenced when the window ends, not 3 s later. Fails on the parent: the
/// window was a wall value, compared with the stepped-back wall clock.
#[test]
fn a_held_grant_stepped_back_stays_fenced_on_time() {
    let f = fenced_fs();
    let me = std::process::id();
    let (turn, _) = f.create(&Caller::root(), ROOT_INO, "turn.lock").unwrap();
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(300));
    let locks = f.fs.cluster_locks().unwrap().clone();
    assert_eq!(locks.fenced(turn.attr.ino), Fence::Ok);
    crate::locks::step_test_wall(-3_000);
    crate::locks::advance_test_clock(250);
    assert_eq!(locks.fenced(turn.attr.ino), Fence::Ok, "inside the window");
    crate::locks::advance_test_clock(100);
    assert_eq!(
        locks.fenced(turn.attr.ino),
        Fence::Fenced,
        "honoured past its honour end because the wall clock stepped back"
    );
    // A forward step ends a window at once.
    let f = fenced_fs();
    let (turn, _) = f.create(&Caller::root(), ROOT_INO, "turn2.lock").unwrap();
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(60_000));
    let locks = f.fs.cluster_locks().unwrap().clone();
    assert_eq!(locks.fenced(turn.attr.ino), Fence::Ok);
    crate::locks::step_test_wall(61_000);
    assert_eq!(locks.fenced(turn.attr.ino), Fence::Fenced);
}

#[test]
fn an_op_sent_after_a_renewal_carries_the_renewed_window() {
    let f = fenced_fs();
    let me = std::process::id();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let first = crate::locks::now().honour.plus(300);
    f.lock(turn.attr.ino, me, first);
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None, None);
    // Taken at the op's start (as `flush` and `release` do).
    // Minted on the wall clock from the honour window (plan 43 C7-H1, R8).
    let minted = crate::locks::current_tag().unwrap().0[0].until_ms;
    assert!(
        (minted - crate::locks::now().wall_of_honour(first).0).abs() <= 5,
        "{minted} for a window ending at {first:?}"
    );
    // Renewed meanwhile, then the old window passes.
    // Inside the lease's own window (60 s): the fast path stays open here, so
    // the refusal below is the token's, not a trip to the (absent) core.
    let renewed = crate::locks::now().honour.plus(30_000);
    let mut h = f.meta.locks().held(turn.attr.ino).unwrap();
    h.until = renewed;
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
    crate::locks::advance_test_clock(31_000);
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(300));
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None, None);
    assert_eq!(
        crate::locks::reissue_after_lapse(),
        Reissue::No,
        "nothing sent yet"
    );
    crate::locks::current_tag().unwrap();
    assert_eq!(
        crate::locks::reissue_after_lapse(),
        Reissue::No,
        "the window did not move"
    );
    let mut h = f.meta.locks().held(turn.attr.ino).unwrap();
    h.until = crate::locks::now().honour.plus(5_000);
    f.meta.locks().install_held(turn.attr.ino, h);
    assert_eq!(
        crate::locks::reissue_after_lapse(),
        Reissue::Yes,
        "renewed and honoured"
    );
    // The re-sent op takes the renewed window: nothing left to re-issue.
    crate::locks::current_tag().unwrap();
    assert_eq!(crate::locks::reissue_after_lapse(), Reissue::No);
    crate::locks::advance_test_clock(6_000);
    assert_eq!(
        crate::locks::reissue_after_lapse(),
        Reissue::No,
        "no longer honoured"
    );
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(300));
    let ino = file.attr.ino;
    let cx = OpCtx::new(OpKind::Write, &owner);
    assert_eq!(f.write(&cx, ino, opened.fh), Ok(6));
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None, None);
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
    f.meta.locks().set_clock(Arc::new(move || {
        crate::locks::now().plus(store_clock.load(Relaxed) as u64)
    }));
    f.lock(ino, me, crate::locks::now().honour.plus(60_000));
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(300));
    let ino = file.attr.ino;
    let cx = OpCtx::new(OpKind::Write, &owner);
    assert_eq!(f.write(&cx, ino, opened.fh), Ok(6));
    let locks = f.fs.cluster_locks().unwrap().clone();
    let _scope = locks.tag_scope(Some(me), None, None);
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
            until: HonourMs(i64::MAX),
            renew_at: HonourMs(i64::MAX),
            owner: 9,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: crate::locks::now().honour,
            lapsing: None,
            written: false,
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
            until: HonourMs(i64::MAX),
            renew_at: HonourMs(i64::MAX),
            owner: 9,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: crate::locks::now().honour,
            lapsing: None,
            written: false,
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
            until: HonourMs(i64::MAX),
            renew_at: HonourMs(i64::MAX),
            owner: 9,
            recalled: false,
            position: constellation_meta::Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: crate::locks::now().honour,
            lapsing: None,
            written: false,
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
            let _scope = locks.tag_scope(Some(me), Some(0xa), None);
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
            let _scope = locks.tag_scope(Some(me), Some(0xa), None);
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
            let _scope = locks.tag_scope(Some(me), Some(0xa), None);
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
    f.lock(locked.attr.ino, me, crate::locks::now().honour.plus(60_000));
    let id = f.meta.locks().held(locked.attr.ino).unwrap().id;
    let (meta, ino) = (f.meta.clone(), locked.attr.ino);
    let hook = super::vfs_tests::BetweenSteps::set(move |_| {
        assert!(meta.locks().lost(ino, id, crate::locks::now()));
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
    f.lock(turn.attr.ino, me, crate::locks::now().honour.plus(60_000));
    let id = f.meta.locks().held(turn.attr.ino).unwrap().id;
    let (meta, ino) = (f.meta.clone(), turn.attr.ino);
    let hook = super::vfs_tests::BetweenSteps::set(move |_| {
        assert!(meta.locks().lost(ino, id, crate::locks::now()));
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

// ---- plan 43 F-b: a non-blocking lock request waits for an owner's grace ----

/// What the core's lock path answers a request, in order (the last one
/// again for any further request).
#[derive(Clone, Copy)]
enum Scripted {
    /// "Nothing conflicts, ask again in this many ms."
    Wait(u64),
    WouldBlock,
    /// Installs a grant on the inode, then grants.
    Grant,
    /// Answers `Granted` and installs nothing: a core granting what the
    /// local tables refuse.
    GrantNothing,
    /// Takes this many ms to answer `WouldBlock`: a core asking an owner
    /// that is gone, a few times, before it says so.
    Slow(u64),
}

/// A stand-in for the core: answers each `SyncRequest::Lock` from
/// `script`, noting whether the request was `blocking`.
fn script_core(
    f: &mut Fenced,
    ino: Ino,
    script: Vec<Scripted>,
) -> Arc<std::sync::Mutex<Vec<bool>>> {
    script_core_waits(f, ino, script).0
}

/// What a scripted core saw: per request, whether it was `blocking`, and
/// the `waited_ms` it carried.
type Seen = (
    Arc<std::sync::Mutex<Vec<bool>>>,
    Arc<std::sync::Mutex<Vec<u64>>>,
);

/// [`script_core`], and the `waited_ms` each request carried.
fn script_core_waits(f: &mut Fenced, ino: Ino, script: Vec<Scripted>) -> Seen {
    let (_dead, closed) = tokio::sync::mpsc::unbounded_channel();
    let mut rx = std::mem::replace(&mut f._locks_core, closed);
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    let waits = Arc::new(std::sync::Mutex::new(Vec::new()));
    let waited_log = waits.clone();
    let meta = f.meta.clone();
    std::thread::spawn(move || {
        let mut n = 0;
        while let Some(req) = rx.blocking_recv() {
            let SyncRequest::Lock {
                blocking,
                waited_ms,
                reply,
                ..
            } = req
            else {
                continue;
            };
            log.lock().unwrap().push(blocking);
            waited_log.lock().unwrap().push(waited_ms);
            let step = script[n.min(script.len() - 1)];
            n += 1;
            let answer = match step {
                Scripted::Wait(retry_ms) => constellation_authority::LockAnswer::Wait { retry_ms },
                Scripted::WouldBlock => constellation_authority::LockAnswer::WouldBlock,
                Scripted::Slow(ms) => {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                    constellation_authority::LockAnswer::WouldBlock
                }
                Scripted::GrantNothing => constellation_authority::LockAnswer::Granted {
                    position: constellation_meta::Position::ZERO,
                },
                Scripted::Grant => {
                    let now = crate::locks::now().honour;
                    meta.locks().install_held(
                        ino,
                        HeldGrant {
                            id: GrantId { node: 9, seq: 1 },
                            mode: LockMode::Exclusive,
                            until: now.plus(60_000),
                            renew_at: now.plus(30_000),
                            owner: 9,
                            recalled: false,
                            position: constellation_meta::Position::ZERO,
                            renewing: None,
                            releasing: false,
                            first_use: false,
                            idle_since: None,
                            installed: now,
                            lapsing: None,
                            written: false,
                        },
                    );
                    constellation_authority::LockAnswer::Granted {
                        position: constellation_meta::Position::ZERO,
                    }
                }
            };
            let _ = reply.send(answer);
        }
    });
    (seen, waits)
}

/// Reports who answered the op, and when.
struct Reports(std::sync::mpsc::Sender<(Result<(), Code>, Option<String>, std::time::Instant)>);

impl constellation_vfs::Responder<()> for Reports {
    fn done(self, result: VfsResult<()>) {
        let _ = self.0.send((
            result.map_err(|e| e.code()),
            std::thread::current().name().map(String::from),
            std::time::Instant::now(),
        ));
    }
}

type Answer = (Result<(), Code>, Option<String>, std::time::Instant);

/// [`Reports`] of a frontend that cannot hold one more waiting request
/// (`Responder::begin_wait` refuses: a ring queue's lock-wait budget is
/// spent).
struct RefusesToWait(Reports);

impl constellation_vfs::Responder<()> for RefusesToWait {
    fn done(self, result: VfsResult<()>) {
        self.0.done(result);
    }

    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        constellation_vfs::WaitStart::Refused
    }
}

/// [`Reports`] of a frontend that registers the wait for its caller's
/// signal at `begin_wait` (a lazy registration), by this token.
struct RegistersAtWait(
    Reports,
    constellation_vfs::CancelToken,
    Arc<std::sync::atomic::AtomicUsize>,
);

impl constellation_vfs::Responder<()> for RegistersAtWait {
    fn done(self, result: VfsResult<()>) {
        self.0.done(result);
    }

    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        self.2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        constellation_vfs::WaitStart::Held(Some(self.1.clone()))
    }
}

impl Fenced {
    /// A non-blocking exclusive lock request on `ino`, asked of the view
    /// like a frontend does; the answer comes through the returned
    /// channel. `cancel`: the op's cancellation.
    fn lock_nb(
        &self,
        ino: Ino,
        cancel: Option<&constellation_vfs::CancelToken>,
    ) -> std::sync::mpsc::Receiver<Answer> {
        self.lock_nb_with(ino, Fh(ino), cancel, Box::new(Reports))
    }

    /// [`Self::lock_nb`] from the description `fh`, with the responder
    /// `make` builds around the reporter.
    fn lock_nb_with<R: constellation_vfs::Responder<()>>(
        &self,
        ino: Ino,
        fh: Fh,
        cancel: Option<&constellation_vfs::CancelToken>,
        make: Box<dyn FnOnce(std::sync::mpsc::Sender<Answer>) -> R>,
    ) -> std::sync::mpsc::Receiver<Answer> {
        let (tx, rx) = std::sync::mpsc::channel();
        let caller = Caller::new(0, 0, Some(std::process::id()));
        let mut cx = OpCtx::new(OpKind::LockAcquire, &caller);
        if let Some(cancel) = cancel {
            cx = cx.with_cancel(cancel);
        }
        self.fs.lock_acquire(
            &cx,
            ino,
            fh,
            LockSpec {
                owner: LockOwner(TURN_OWNER + 7),
                range: LockRange {
                    start: 0,
                    end: u64::MAX,
                },
                kind: LockKind::Write,
                pid: std::process::id(),
            },
            false,
            make(tx),
        );
        rx
    }
}

const SECOND: std::time::Duration = std::time::Duration::from_secs(1);

/// The core answers a non-blocking request "nothing conflicts, ask again"
/// (a grace): the call returns at once, without an answer, and the waiting
/// is done by a `lock-wait` pool thread, which asks again until it is
/// granted. Neither the calling worker nor (over a ring) an offload thread
/// is held for the grace: the pool thread is. On c9bee29 the core answered
/// `WouldBlock`, the view `EAGAIN` at once.
#[test]
fn a_nonblocking_lock_in_a_grace_does_not_hold_the_calling_thread() {
    let mut f = fenced_fs();
    let (file, _) = f.create(&Caller::root(), ROOT_INO, "graced").unwrap();
    let ino = file.attr.ino;
    let seen = script_core(
        &mut f,
        ino,
        vec![Scripted::Wait(200), Scripted::Wait(200), Scripted::Grant],
    );
    let started = std::time::Instant::now();
    let rx = f.lock_nb(ino, None);
    let returned = started.elapsed();
    assert!(
        returned < std::time::Duration::from_millis(150),
        "the calling thread waited {returned:?}"
    );
    assert!(rx.try_recv().is_err(), "no answer yet");
    let (result, thread, at) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Ok(()));
    assert_eq!(thread.as_deref(), Some("lock-wait"));
    assert!(
        at.duration_since(started) >= std::time::Duration::from_millis(400),
        "waited the two intervals it was told: {:?}",
        at.duration_since(started)
    );
    assert_eq!(*seen.lock().unwrap(), vec![false, false, false]);
    let stats = f.meta.locks().stats();
    assert_eq!(stats.grace_waits, 1);
    assert!(stats.grace_wait_ms_total >= 400, "{stats:?}");
    assert_eq!(stats.grace_wait_gave_up, 0);
}

/// Waiting for a grace spends no attempt: more answers than the rounds a
/// request that keeps losing to recalls gets (`TRY_ROUNDS`) still end in
/// the grant.
#[test]
fn waiting_for_a_grace_is_no_lost_round() {
    let mut f = fenced_fs();
    let (file, _) = f.create(&Caller::root(), ROOT_INO, "many").unwrap();
    let ino = file.attr.ino;
    let mut script = vec![Scripted::Wait(10); 9];
    script.push(Scripted::Grant);
    script_core(&mut f, ino, script);
    let rx = f.lock_nb(ino, None);
    let (result, _, _) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Ok(()));
}

/// A signal ends the wait: the op's cancellation answers `EINTR`, within
/// a slice, holding nothing.
#[test]
fn a_cancellation_ends_a_graced_wait_with_eintr() {
    let mut f = fenced_fs();
    let (file, _) = f.create(&Caller::root(), ROOT_INO, "signalled").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::Wait(10_000)]);
    let cancel = constellation_vfs::CancelToken::new();
    let rx = f.lock_nb(ino, Some(&cancel));
    assert!(rx.recv_timeout(SECOND / 5).is_err(), "waiting");
    let cancelled = std::time::Instant::now();
    cancel.cancel();
    let (result, thread, at) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Intr));
    assert_eq!(thread.as_deref(), Some("lock-wait"));
    assert!(
        at.duration_since(cancelled) < SECOND / 2,
        "ended within a slice: {:?}",
        at.duration_since(cancelled)
    );
    assert!(f.meta.locks().local_locks(ino).is_empty());
    assert_eq!(f.meta.locks().stats().grace_wait_gave_up, 0);
}

/// Plan 43 C7-F F-b N2: a signal ends a request whose first attempt is in
/// flight against a core that does not answer for seconds (it asks an owner
/// that is gone, a few times, before it says so) within a slice of the wait
/// for the answer, not after the round trip. The request registers for the
/// signal late, when the answer is slow (a request answered at once
/// registers nothing). On the landed F-b the wait for the core's answer was
/// a blocking receive: the signal was seen after the 5 s here.
#[test]
fn a_signal_ends_a_first_attempt_in_flight_against_a_slow_core() {
    let mut f = fenced_fs();
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "in_flight").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::Slow(5_000)]);
    let token = constellation_vfs::CancelToken::new();
    let registered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (t, r) = (token.clone(), registered.clone());
    let (rx, took) = std::thread::scope(|s| {
        let call = s.spawn(|| {
            f.lock_nb_with(
                ino,
                opened.fh,
                None,
                Box::new(move |tx| RegistersAtWait(Reports(tx), t, r)),
            )
        });
        std::thread::sleep(SECOND / 3);
        assert!(!call.is_finished(), "the core has not answered");
        assert_eq!(
            registered.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "registered for the signal once the answer was slow"
        );
        let cancelled = std::time::Instant::now();
        token.cancel();
        let rx = call.join().unwrap();
        (rx, cancelled.elapsed())
    });
    let (result, _, _) = rx.try_recv().expect("answered by the call");
    assert_eq!(result, Err(Code::Intr));
    assert!(
        took < SECOND / 4,
        "the signal ended the request within a slice: {took:?}"
    );
    assert!(f.meta.locks().local_locks(ino).is_empty());
}

/// Plan 43 C7-F F-b N2: the same for a re-issue on the wait pool: its
/// attempt in flight against a slow core ends within a slice of the
/// signal (the pool thread then goes on to the next request).
#[test]
fn a_signal_ends_a_reissue_in_flight_against_a_slow_core() {
    let mut f = fenced_fs();
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "reissue").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::Wait(50), Scripted::Slow(5_000)]);
    let token = constellation_vfs::CancelToken::new();
    let registered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (t, r) = (token.clone(), registered.clone());
    let rx = f.lock_nb_with(
        ino,
        opened.fh,
        None,
        Box::new(move |tx| RegistersAtWait(Reports(tx), t, r)),
    );
    assert!(
        rx.recv_timeout(SECOND / 3).is_err(),
        "the second attempt is in flight"
    );
    let cancelled = std::time::Instant::now();
    token.cancel();
    let (result, thread, at) = rx.recv_timeout(2 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Intr));
    assert_eq!(thread.as_deref(), Some("lock-wait"));
    assert!(
        at.duration_since(cancelled) < SECOND / 4,
        "ended within a slice: {:?}",
        at.duration_since(cancelled)
    );
}

/// Past the bound the request gives up with `EAGAIN`, counted.
#[test]
fn a_graced_wait_past_its_bound_gives_up_with_eagain_and_is_counted() {
    let mut f = fenced_fs_waiting(SECOND / 3);
    let (file, _) = f.create(&Caller::root(), ROOT_INO, "too_long").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::Wait(50)]);
    let started = std::time::Instant::now();
    let rx = f.lock_nb(ino, None);
    let (result, thread, at) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Again));
    assert_eq!(thread.as_deref(), Some("lock-wait"));
    let took = at.duration_since(started);
    assert!(took >= SECOND / 3 && took < 2 * SECOND, "{took:?}");
    let stats = f.meta.locks().stats();
    assert_eq!(stats.grace_waits, 1);
    assert_eq!(stats.grace_wait_gave_up, 1);
}

/// A known conflict is no wait: `EAGAIN` at once, on the calling thread.
#[test]
fn a_known_conflict_still_refuses_a_nonblocking_lock_at_once() {
    let mut f = fenced_fs();
    let (file, _) = f.create(&Caller::root(), ROOT_INO, "held").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::WouldBlock]);
    let rx = f.lock_nb(ino, None);
    let (result, thread, _) = rx.try_recv().expect("answered before the call returned");
    assert_eq!(result, Err(Code::Again));
    assert_ne!(thread.as_deref(), Some("lock-wait"));
    assert_eq!(f.meta.locks().stats().grace_waits, 0);
}

/// A frontend that cannot answer from another thread waits inline, as it
/// does for a blocking lock.
#[test]
fn a_frontend_that_cannot_defer_waits_in_the_call() {
    let mut f = fenced_fs();
    f.fs.caps.deferrable = constellation_vfs::OpKindSet::EMPTY;
    let (file, _) = f.create(&Caller::root(), ROOT_INO, "inline").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::Wait(100), Scripted::Grant]);
    let started = std::time::Instant::now();
    let rx = f.lock_nb(ino, None);
    let (result, thread, _) = rx.try_recv().expect("answered before the call returned");
    assert_eq!(result, Ok(()));
    assert_ne!(thread.as_deref(), Some("lock-wait"));
    assert!(started.elapsed() >= std::time::Duration::from_millis(100));
    assert_eq!(f.meta.locks().stats().grace_waits, 1);
}

/// Plan 43 F-b review F2: the frontend is told when a request is about to
/// wait (`Responder::begin_wait`), and the wait begins only if it may hold
/// what it holds (over a ring, one of its queue's `depth - 1` entries that
/// lock waits may take, so one stays free for every other request of that
/// CPU). When it may not, the request is answered now, as it was before it
/// could wait: `EAGAIN`, on the calling thread, counted apart
/// (`grace_wait_refused_ring`), and the core is asked once.
#[test]
fn a_wait_the_frontend_cannot_hold_is_answered_at_once() {
    let mut f = fenced_fs();
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "refused").unwrap();
    let ino = file.attr.ino;
    let seen = script_core(&mut f, ino, vec![Scripted::Wait(100), Scripted::Grant]);
    let rx = f.lock_nb_with(
        ino,
        opened.fh,
        None,
        Box::new(|tx| RefusesToWait(Reports(tx))),
    );
    let (result, thread, _) = rx.try_recv().expect("answered before the call returned");
    assert_eq!(result, Err(Code::Again));
    assert_ne!(thread.as_deref(), Some("lock-wait"));
    assert_eq!(seen.lock().unwrap().len(), 1, "asked once, never again");
    let stats = f.meta.locks().stats();
    assert_eq!(stats.grace_wait_refused_ring, 1);
    assert_eq!(stats.grace_waits, 0, "it never waited");
    assert!(f.meta.locks().local_locks(ino).is_empty());
}

/// The wait is registered for the caller's signal when it begins, by the
/// token `begin_wait` returns, not by the request's context (a non-blocking
/// request carries none, so an answer at once registers nothing): the token
/// ends the wait `EINTR`, and a request answered at once never asks.
#[test]
fn a_wait_registers_for_signals_only_when_it_begins() {
    let mut f = fenced_fs();
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "late").unwrap();
    let ino = file.attr.ino;
    script_core(&mut f, ino, vec![Scripted::Wait(10_000)]);
    let token = constellation_vfs::CancelToken::new();
    let registered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let r = registered.clone();
    let t = token.clone();
    let rx = f.lock_nb_with(
        ino,
        opened.fh,
        None,
        Box::new(move |tx| RegistersAtWait(Reports(tx), t, r)),
    );
    assert_eq!(registered.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(rx.recv_timeout(SECOND / 5).is_err(), "waiting");
    token.cancel();
    let (result, thread, _) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Intr));
    assert_eq!(thread.as_deref(), Some("lock-wait"));
    // Answered at once: never registered.
    let g = fenced_fs();
    let (file, opened) = g.create(&Caller::root(), ROOT_INO, "at_once").unwrap();
    let mut g = g;
    script_core(&mut g, file.attr.ino, vec![Scripted::WouldBlock]);
    let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = n.clone();
    let rx = g.lock_nb_with(
        file.attr.ino,
        opened.fh,
        None,
        Box::new(move |tx| RegistersAtWait(Reports(tx), constellation_vfs::CancelToken::new(), c)),
    );
    assert_eq!(rx.try_recv().expect("answered").0, Err(Code::Again));
    assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// Plan 43 F-b review F8: the waits of any number of requests are served
/// by a bounded pool of `lock-wait` threads, not a thread each. Forty
/// requests wait, ask again twice, and are all answered (here `EAGAIN`, a
/// conflict having appeared); the pool never had more than
/// `WAIT_POOL_THREADS` threads.
#[test]
fn many_waits_are_served_by_a_bounded_pool() {
    let mut f = fenced_fs();
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "crowd").unwrap();
    let ino = file.attr.ino;
    let mut script = vec![Scripted::Wait(100); 80];
    script.push(Scripted::WouldBlock);
    script_core(&mut f, ino, script);
    let rxs: Vec<_> = (0..40)
        .map(|_| f.lock_nb_with(ino, opened.fh, None, Box::new(Reports)))
        .collect();
    let locks = f.fs.cluster_locks().expect("cluster locks");
    let workers = locks.waits.threads();
    assert!(
        (1..=crate::locks::WAIT_POOL_THREADS).contains(&workers),
        "{workers} pool threads for 40 waits"
    );
    for rx in rxs {
        let (result, thread, _) = rx.recv_timeout(10 * SECOND).expect("answered");
        assert_eq!(result, Err(Code::Again));
        assert_eq!(thread.as_deref(), Some("lock-wait"));
    }
    assert!(locks.waits.threads() <= crate::locks::WAIT_POOL_THREADS);
    let stats = f.meta.locks().stats();
    assert_eq!(stats.grace_waits, 40);
    assert_eq!(stats.grace_wait_gave_up, 0);
}

/// A pool that cannot start a thread answers the request now, `EAGAIN` as
/// before F-b and counted (`grace_wait_refused_pool`), never `EIO`; one
/// that has threads queues the wait for them.
#[test]
fn a_pool_that_cannot_start_a_thread_answers_eagain_and_counts_it() {
    let mut f = fenced_fs();
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "nothreads").unwrap();
    let ino = file.attr.ino;
    script_core(
        &mut f,
        ino,
        vec![
            Scripted::Wait(50),
            Scripted::Wait(50),
            Scripted::Wait(50),
            Scripted::Wait(50),
            Scripted::WouldBlock,
        ],
    );
    let locks = f.fs.cluster_locks().expect("cluster locks");
    locks.waits.fail_spawn(true);
    let rx = f.lock_nb_with(ino, opened.fh, None, Box::new(Reports));
    let (result, thread, _) = rx.try_recv().expect("answered before the call returned");
    assert_eq!(result, Err(Code::Again), "not EIO");
    assert_ne!(thread.as_deref(), Some("lock-wait"));
    assert_eq!(f.meta.locks().stats().grace_wait_refused_pool, 1);
    assert_eq!(
        f.meta.locks().stats().grace_waits,
        0,
        "refused for want of a thread: not also counted as a wait"
    );
    assert_eq!(locks.waits.threads(), 0);
    // With a thread running, the next one only queues.
    locks.waits.fail_spawn(false);
    let first = f.lock_nb_with(ino, opened.fh, None, Box::new(Reports));
    locks.waits.fail_spawn(true);
    let second = f.lock_nb_with(ino, opened.fh, None, Box::new(Reports));
    assert!(second.try_recv().is_err(), "queued for the running thread");
    for rx in [first, second] {
        let (result, thread, _) = rx.recv_timeout(5 * SECOND).expect("answered");
        assert_eq!(result, Err(Code::Again));
        assert_eq!(thread.as_deref(), Some("lock-wait"));
    }
    assert_eq!(f.meta.locks().stats().grace_wait_refused_pool, 1);
    assert_eq!(f.meta.locks().stats().grace_waits, 2, "the two that waited");
}

/// Plan 43 F-b review F8: the file is closed during the wait, and the
/// process exits (its files closed and its request interrupted): the wait
/// ends `EINTR`, asks the core no more, installs no local lock and keeps
/// no grant for an owner that is gone.
#[test]
fn closing_the_file_during_a_graced_wait_leaves_nothing_behind() {
    let mut f = fenced_fs();
    let caller = Caller::new(0, 0, Some(std::process::id()));
    let (file, opened) = f.create(&Caller::root(), ROOT_INO, "closed").unwrap();
    let ino = file.attr.ino;
    let seen = script_core(&mut f, ino, vec![Scripted::Wait(300), Scripted::Grant]);
    let cancel = constellation_vfs::CancelToken::new();
    let rx = f.lock_nb_with(ino, opened.fh, Some(&cancel), Box::new(Reports));
    assert!(rx.recv_timeout(SECOND / 10).is_err(), "waiting");
    Blocking::run(|r| {
        f.fs.release(
            &OpCtx::new(OpKind::Release, &caller),
            ino,
            opened.fh,
            OpenFlags::WRITE,
            None,
            r,
        )
    })
    .expect("the close answers while a lock waits on the file");
    let exited = std::time::Instant::now();
    cancel.cancel();
    let (result, _, at) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Intr));
    assert!(at.duration_since(exited) < SECOND / 2);
    std::thread::sleep(std::time::Duration::from_millis(400));
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "never asked again: no grant to a dead owner"
    );
    assert!(f.meta.locks().local_locks(ino).is_empty());
    assert!(f.meta.locks().held(ino).is_none());
}

/// Plan 43 F-b review F6: a request told again and again to wait for an
/// owner that cannot time it (the owner's polling hint: 50 ms) asks less
/// and less often, twice the last interval up to a second, instead of
/// every 50 ms for the whole of a stale owner's lease; a hint that is a
/// grace's remainder (longer than the polling hint) is exact and followed,
/// and ends the run.
#[test]
fn a_run_of_polling_hints_backs_off_and_a_graces_remainder_is_followed() {
    let f = fenced_fs();
    let locks = f.fs.cluster_locks().expect("cluster locks");
    let ms = std::time::Duration::from_millis;
    let mut poll = None;
    let seq: Vec<_> = (0..9).map(|_| locks.next_wait(&mut poll, ms(50))).collect();
    assert_eq!(
        seq,
        [50, 100, 200, 400, 800, 1_000, 1_000, 1_000, 1_000].map(ms)
    );
    // A grace's remainder: followed as told, and the run starts again.
    assert_eq!(locks.next_wait(&mut poll, ms(700)), ms(700));
    assert_eq!(locks.next_wait(&mut poll, ms(50)), ms(50));
    assert_eq!(locks.next_wait(&mut poll, ms(50)), ms(100));
    // A hint below the polling one (a grace's last few ms) is followed
    // too, never lengthened past what the run had.
    let mut poll = None;
    assert_eq!(locks.next_wait(&mut poll, ms(20)), ms(20));
}

// ---- plan 43 C7-F F-c: a lapsed lock owner's I/O waits for its grant's
// verdict, without holding the calling thread ----

/// What a read answered, and on which thread.
struct ReadReports(
    std::sync::mpsc::Sender<ReadAnswer>,
    Option<(
        constellation_vfs::CancelToken,
        Arc<std::sync::atomic::AtomicUsize>,
    )>,
    bool,
);

impl constellation_vfs::Responder<constellation_vfs::ReadData> for ReadReports {
    fn done(self, result: VfsResult<constellation_vfs::ReadData>) {
        let _ = self.0.send((
            result.map(|d| d.segments().concat()).map_err(|e| e.code()),
            std::thread::current().name().map(String::from),
        ));
    }

    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        if self.2 {
            return constellation_vfs::WaitStart::Refused;
        }
        match &self.1 {
            Some((token, n)) => {
                n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                constellation_vfs::WaitStart::Held(Some(token.clone()))
            }
            None => constellation_vfs::WaitStart::Held(None),
        }
    }
}

type ReadAnswer = (Result<Vec<u8>, Code>, Option<String>);

/// A view bound to its engine's completion machinery (`View::bind`), a
/// file `obj` with six bytes in it, and the committer's lock on `turn.lock`
/// under a grant honoured for `window_ms`; the table parks lapses.
struct Parking {
    fs: Arc<View>,
    meta: Arc<Meta>,
    committer: Caller,
    turn: Ino,
    obj: Ino,
    fh: Fh,
    _keep: (
        tempfile::TempDir,
        tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
        tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
    ),
}

fn parking(window_ms: u64) -> Parking {
    parking_with(window_ms, true)
}

fn parking_with(window_ms: u64, deferrable: bool) -> Parking {
    let mut f = fenced_fs();
    if !deferrable {
        f.fs.caps.deferrable = constellation_vfs::OpKindSet::EMPTY;
    }
    f.meta.locks().set_lapse_park_ms(60_000);
    let me = std::process::id();
    let root = Caller::root();
    let (turn, _) = f.create(&root, ROOT_INO, "turn.lock").unwrap();
    let (obj, oo) = f.create(&root, ROOT_INO, "obj").unwrap();
    let committer = Caller::new(0, 0, Some(me));
    f.write(&OpCtx::new(OpKind::Write, &committer), obj.attr.ino, oo.fh)
        .unwrap();
    f.lock(
        turn.attr.ino,
        me,
        crate::locks::now().honour.plus(window_ms),
    );
    let Fenced {
        fs,
        meta,
        _dir,
        _core,
        _locks_core,
    } = f;
    let fs = Arc::new(fs);
    fs.bind();
    Parking {
        fs,
        meta,
        committer,
        turn: turn.attr.ino,
        obj: obj.attr.ino,
        fh: oo.fh,
        _keep: (_dir, _core, _locks_core),
    }
}

impl Parking {
    /// A read of `obj` by the committer; the answer comes through the channel.
    fn read(
        &self,
        reg: Option<(
            constellation_vfs::CancelToken,
            Arc<std::sync::atomic::AtomicUsize>,
        )>,
        refuse: bool,
    ) -> std::sync::mpsc::Receiver<ReadAnswer> {
        let (tx, rx) = std::sync::mpsc::channel();
        let cx = OpCtx::new(OpKind::Read, &self.committer);
        self.fs
            .read(&cx, self.obj, self.fh, 0, 6, ReadReports(tx, reg, refuse));
        rx
    }

    /// The owner admits a late renewal of the committer's grant.
    fn resume(&self) {
        let id = self.meta.locks().held(self.turn).unwrap().id;
        let now = crate::locks::now();
        self.meta.locks().renewed(
            self.turn,
            id,
            id,
            LockMode::Exclusive,
            now.honour,
            60_000,
            1_000,
            false,
            now,
        );
    }

    fn lapse(&self) {
        // The window (a few hundred ms) ends.
        let until = self.meta.locks().held(self.turn).unwrap().until;
        while crate::locks::now().honour < until {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// The fix's end to end through the `Vfs` trait. The committer's grant
/// lapsed with its `flock` held: its read does not fail `EIO` (as it did) and
/// does not hold the calling thread — the call returns at once — it is
/// answered, from the `lock-wait` pool, once the owner has admitted the late
/// renewal, with the data; nothing was captured or tainted meanwhile.
#[test]
fn a_lapsed_owners_read_waits_for_the_verdict_and_runs_after_the_admission() {
    let p = parking(300);
    p.lapse();
    let rx = p.read(None, false);
    let first = rx.recv_timeout(SECOND / 5);
    assert!(
        first.is_err(),
        "the call returned and the read is parked, not answered (before F-c: EIO at once): {first:?}"
    );
    let s = p.meta.locks().stats();
    assert_eq!((s.lapse_parks, s.lapse_parked_ops, s.fenced_io), (1, 1, 0));
    assert!(p.meta.locks().fenced_owners(crate::locks::now()).is_empty());
    p.resume();
    let (result, thread, _) = rx
        .recv_timeout(5 * SECOND)
        .map(|(a, b)| (a, b, ()))
        .expect("answered");
    assert_eq!(result, Ok(b"object".to_vec()));
    assert_eq!(thread.as_deref(), Some("lock-wait"));
    let s = p.meta.locks().stats();
    assert_eq!((s.lapse_parks_resumed, s.lapse_parks_refused), (1, 0));
    // No grant is lapsing now: the next read is inline, one relaxed load.
    assert!(!p.meta.locks().lapsing_any());
    let rx = p.read(None, false);
    assert_eq!(rx.try_recv().expect("inline").0, Ok(b"object".to_vec()));
}

/// The owner refused: the parked read fails `EIO` as before — from the
/// moment the refusal arrives, and the owner is fenced since the lapse.
#[test]
fn a_parked_read_fails_eio_when_the_owner_refuses() {
    let p = parking(300);
    p.lapse();
    let rx = p.read(None, false);
    assert!(rx.recv_timeout(SECOND / 5).is_err());
    let id = p.meta.locks().held(p.turn).unwrap().id;
    assert!(p.meta.locks().lost(p.turn, id, crate::locks::now()));
    let (result, _) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Io));
    assert_eq!(p.meta.locks().stats().lapse_parks_refused, 1);
}

/// A signal ends the op's park, `EINTR` (a FUSE request cannot be left to
/// the kernel's uninterruptible wait for up to the park's bound); the
/// grant's park goes on for the others.
#[test]
fn a_signal_ends_a_parked_read_with_eintr_and_the_park_goes_on() {
    let p = parking(300);
    p.lapse();
    let token = constellation_vfs::CancelToken::new();
    let registered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rx = p.read(Some((token.clone(), registered.clone())), false);
    assert_eq!(registered.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(rx.recv_timeout(SECOND / 5).is_err());
    token.cancel();
    let (result, _) = rx.recv_timeout(5 * SECOND).expect("answered");
    assert_eq!(result, Err(Code::Intr));
    assert_eq!(p.meta.locks().stats().lapse_park_interrupted, 1);
    assert!(p.meta.locks().lapsing(p.turn), "the grant's park goes on");
    // Another reader is still parked, and resumes.
    let other = p.read(None, false);
    assert!(other.recv_timeout(SECOND / 5).is_err());
    p.resume();
    assert_eq!(
        other.recv_timeout(5 * SECOND).expect("answered").0,
        Ok(b"object".to_vec())
    );
}

/// A frontend that cannot hold one more waiting request (a ring queue's
/// lock-wait budget is spent): `EIO` at once, counted apart, as before F-c;
/// and no pool thread: the same.
#[test]
fn a_park_the_frontend_or_the_pool_cannot_hold_is_eio_at_once_and_counted() {
    let p = parking(300);
    p.lapse();
    let rx = p.read(None, true);
    assert_eq!(rx.try_recv().expect("answered at once").0, Err(Code::Io));
    assert_eq!(p.meta.locks().stats().lapse_park_refused_ring, 1);
    let locks = p.fs.cluster_locks().expect("cluster locks").clone();
    locks.waits.fail_spawn(true);
    let rx = p.read(None, false);
    assert_eq!(rx.try_recv().expect("answered at once").0, Err(Code::Io));
    assert_eq!(p.meta.locks().stats().lapse_park_refused_pool, 1);
}

/// A write parks too, its data copied then (never on the hot path), and
/// lands after the admission.
#[test]
fn a_parked_write_lands_after_the_admission() {
    let p = parking(300);
    p.lapse();
    let (tx, rx) = std::sync::mpsc::channel();
    struct W(std::sync::mpsc::Sender<Result<u32, Code>>);
    impl constellation_vfs::Responder<u32> for W {
        fn done(self, r: VfsResult<u32>) {
            let _ = self.0.send(r.map_err(|e| e.code()));
        }
    }
    let data = b"rewritten".to_vec();
    p.fs.write(
        &OpCtx::new(OpKind::Write, &p.committer),
        p.obj,
        p.fh,
        0,
        WriteData::Borrowed(&data),
        OpenFlags::WRITE,
        W(tx),
    );
    drop(data);
    assert!(rx.recv_timeout(SECOND / 5).is_err(), "parked");
    p.resume();
    assert_eq!(rx.recv_timeout(5 * SECOND).unwrap(), Ok(9));
    let got = p.read(None, false).recv_timeout(5 * SECOND).unwrap().0;
    assert_eq!(got, Ok(b"rewrit".to_vec()));
}

/// A frontend that cannot answer from another thread waits in the call, as
/// F-b's lock waits do: the read is answered on the calling thread, once the
/// verdict is in (here from another thread, 150 ms after the call began).
#[test]
fn a_frontend_that_cannot_defer_waits_in_the_call_for_the_verdict() {
    let p = parking_with(300, false);
    p.lapse();
    let meta = p.meta.clone();
    let turn = p.turn;
    let renewer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        let id = meta.locks().held(turn).unwrap().id;
        let now = crate::locks::now();
        meta.locks().renewed(
            turn,
            id,
            id,
            LockMode::Exclusive,
            now.honour,
            60_000,
            1_000,
            false,
            now,
        );
    });
    let started = std::time::Instant::now();
    let rx = p.read(None, false);
    let (result, thread) = rx.try_recv().expect("answered before the call returned");
    assert_eq!(result, Ok(b"object".to_vec()));
    assert_ne!(thread.as_deref(), Some("lock-wait"));
    assert!(started.elapsed() >= std::time::Duration::from_millis(100));
    renewer.join().unwrap();
}
