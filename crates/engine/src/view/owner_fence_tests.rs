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
    Blocking, Fh, LockKind, LockOwner, LockRange, LockSpec, Name, OpCtx, OpKind, OpenOwner, Opened,
    RenameFlags, Vfs, VfsResult, WriteData,
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
