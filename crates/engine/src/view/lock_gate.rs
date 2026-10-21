//! Plan 30 §M14: the lock fence at I/O and publication points, and a
//! recalled grant's flush.

use super::*;
use constellation_meta::locks::{Discard, Fence};
use constellation_vfs::{CancelToken, OpCtx, OpId, OpKind, Responder, VfsError, WaitStart};
use std::time::{Duration, Instant};

thread_local! {
    /// Set while a thread runs an op the park pool asked again: that op does
    /// not park a second time (`Responder::begin_wait` is called once per
    /// op); the fences it meets wait inline, bounded.
    static REDISPATCHED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// What an op's park gate looks at (plan 43 C7-F F-c): the checks the op
/// makes — and so what a lapsing grant makes it wait for — before its first
/// effect.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ParkGate {
    /// The owner fence applies to the op (`owner_fence_applies`).
    pub owner: bool,
    /// The inodes it fences I/O on (`lock_fenced`), by real number.
    pub fence: [Option<Ino>; 2],
    /// The inodes it discards a taint on before writing
    /// (`lock_discard_tainted`).
    pub discard: Option<Ino>,
    /// The inode it publishes (`lock_publish_gate`).
    pub publish: Option<Ino>,
}

/// An op's context, owned: what a parked op asks again with.
pub(crate) struct OwnedCx {
    op: OpId,
    kind: OpKind,
    caller: constellation_vfs::Caller,
    lock_owner: Option<u64>,
    deadline: Option<Instant>,
    cancel: Option<CancelToken>,
    span: tracing::Span,
}

impl OwnedCx {
    fn of(cx: &OpCtx<'_>, cancel: Option<CancelToken>) -> Self {
        OwnedCx {
            op: cx.op,
            kind: cx.kind,
            caller: constellation_vfs::Caller::new(cx.caller.uid, cx.caller.gid, cx.caller.pid),
            lock_owner: cx.lock_owner,
            deadline: cx.deadline,
            cancel: cancel.or_else(|| cx.cancel.cloned()),
            span: cx.span.clone(),
        }
    }

    fn cx(&self) -> OpCtx<'_> {
        OpCtx {
            op: self.op,
            kind: self.kind,
            caller: &self.caller,
            lock_owner: self.lock_owner,
            deadline: self.deadline,
            cancel: self.cancel.as_ref(),
            span: &self.span,
        }
    }
}

/// Plan 30 §M14: the ops the owner fence refuses — every op that reads
/// or changes file data or the namespace. Lookups, `getattr`, `readdir`
/// and the like are not (git's failure path and the shell around it must
/// still see the tree), nor are `flush`, `release` and the lock ops: the
/// close and the unlock are how the fence lifts. An open for writing is
/// checked in `open` itself.
fn owner_fence_applies(kind: OpKind) -> bool {
    matches!(
        kind,
        OpKind::Setattr
            | OpKind::Mknod
            | OpKind::Mkdir
            | OpKind::Symlink
            | OpKind::Link
            | OpKind::Unlink
            | OpKind::Rmdir
            | OpKind::Rename
            | OpKind::Create
            | OpKind::Read
            | OpKind::Write
            | OpKind::Fsync
            | OpKind::Fallocate
            | OpKind::CopyFileRange
            | OpKind::Setxattr
            | OpKind::Removexattr
    )
}

impl View {
    /// Plan 30 §M14, the owner fence: whether `cx` is an op the fence
    /// refuses ([`owner_fence_applies`]) issued by a lock owner whose
    /// grant lapsed — by its lock owner id, or by its process or one that
    /// process started. One relaxed atomic load while this node holds no
    /// local lock; two while every grant under one is still honoured.
    pub(crate) fn lock_owner_fenced(&self, cx: &OpCtx<'_>) -> bool {
        owner_fence_applies(cx.kind) && self.lock_owner_fenced_any(cx)
    }

    /// Plan 30 §M14 phase 2: enter the fencing-token scope of `cx`'s op
    /// on this thread (`crate::locks::TagScope`): its mutations carry the
    /// grants its caller's locks are under. Nothing under `--locks
    /// local`; one relaxed load while this node holds no local lock.
    pub(crate) fn lock_tag_scope(&self, cx: &OpCtx<'_>) -> Option<crate::locks::TagScope> {
        self.cluster_locks()
            .map(|l| l.tag_scope(cx.caller.pid, cx.lock_owner))
    }

    /// [`Self::lock_owner_fenced`] whatever the op.
    pub(crate) fn lock_owner_fenced_any(&self, cx: &OpCtx<'_>) -> bool {
        let Some(l) = self.cluster_locks() else {
            return false;
        };
        let fenced = self.lock_owner_fenced_for(l, cx.caller.pid, cx.lock_owner, cx.cancel);
        if fenced {
            tracing::debug!(
                target: "constellation::locks",
                op = cx.kind.name(),
                pid = ?cx.caller.pid,
                lock_owner = ?cx.lock_owner,
                "refused: the caller's lock grant lapsed (EIO until its locks are gone)"
            );
        }
        fenced
    }

    /// The owner fence for the caller `pid` / `lock_owner`. Plan 43 C7-F
    /// F-c: a grant that is lapsing is waited for (an op the park gate did
    /// not hold — a race, or one that cannot park — waits here, its thread
    /// held, bounded).
    pub(crate) fn lock_owner_fenced_for(
        &self,
        l: &Arc<crate::locks::ClusterLocks>,
        pid: Option<u32>,
        lock_owner: Option<u64>,
        cancel: Option<&CancelToken>,
    ) -> bool {
        loop {
            match l.owner_fenced(pid, lock_owner) {
                Fence::Ok => return false,
                Fence::Fenced => return true,
                Fence::Parked => {
                    if !self.lock_wait_inline(l, cancel, || {
                        l.owner_fenced(pid, lock_owner) == Fence::Parked
                    }) {
                        return true;
                    }
                }
            }
        }
    }

    /// Plan 30 §M14: I/O on `ino` is fenced (`EIO`) — this node holds
    /// local locks on it under a grant that lapsed. One relaxed atomic
    /// load while no local lock exists anywhere (and nothing at all under
    /// `--locks local`).
    ///
    /// Plan 43 C7-F F-c: while the grant is lapsing the answer is not yet
    /// known: the op waits here (inline, bounded: the park gate holds ops
    /// that can be held without a thread, `lock_would_park`) and asks again.
    pub(crate) fn lock_fenced(&self, ino: Ino) -> bool {
        let Some(l) = self.cluster_locks() else {
            return false;
        };
        loop {
            match l.fenced(ino) {
                Fence::Ok => return false,
                Fence::Fenced => return true,
                Fence::Parked => {
                    if !self.lock_wait_inline(l, None, || l.fenced(ino) == Fence::Parked) {
                        return true;
                    }
                }
            }
        }
    }

    /// Plan 43 C7-F F-c: wait on this thread while `parked` says the grant
    /// is lapsing: `true` once it does not (the caller asks again), `false`
    /// when `cancel` fired or the lapse's bound passed (the caller treats
    /// the grant as fenced; the table gives up at the bound by itself).
    pub(crate) fn lock_wait_inline(
        &self,
        l: &crate::locks::ClusterLocks,
        cancel: Option<&CancelToken>,
        parked: impl Fn() -> bool,
    ) -> bool {
        let started = Instant::now();
        let bound = l.owner_wait_max + Duration::from_secs(5);
        while parked() {
            if cancel.is_some_and(CancelToken::is_cancelled) || started.elapsed() >= bound {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// Plan 43 C7-F F-c: whether an op may find a grant lapsing: the node
    /// holds a grant, local lock or taint (one relaxed load once the locks
    /// are known) and a held grant is lapsing or ends within the lookahead
    /// ([`constellation_meta::locks::LockTables::may_park`]). A grant lapses
    /// unobserved, so the park gate of every op must look in that window;
    /// it builds nothing outside it, and the frontend wraps no responder.
    pub(crate) fn lock_parking(&self) -> bool {
        self.cluster_locks().is_some_and(|l| {
            let t = l.meta.locks();
            t.active()
                && t.may_park_est(crate::locks::wait_now())
                && t.may_park(crate::locks::now())
        })
    }

    /// Whether the frontend should wrap an op's responder so that it can park
    /// ([`Vfs::parking`]): this node holds a grant, a local lock or a taint,
    /// and a held grant is lapsing or ends within
    /// [`constellation_meta::locks::WRAP_LOOKAHEAD_MS`] (one wait-clock read
    /// and three relaxed loads). Wider than the op's own gate
    /// ([`Self::lock_parking`]), so an op the gate parks was wrapped. Wrapping
    /// every op of a node that holds a lock cost the write path two shared
    /// reference counts, taken and dropped (+1 % on tmpfs/ext4/xfs, +3 % on
    /// btrfs: review of F-c).
    pub(crate) fn lock_may_wrap(&self) -> bool {
        self.cluster_locks().is_some_and(|l| {
            let t = l.meta.locks();
            t.active()
                && t.may_park_est_ahead(
                    crate::locks::wait_now(),
                    constellation_meta::locks::WRAP_LOOKAHEAD_MS,
                )
        })
    }

    /// The park gates of the ops (see [`ParkGate`]).
    pub(crate) fn gate_owner(&self) -> ParkGate {
        ParkGate {
            owner: true,
            ..ParkGate::default()
        }
    }

    /// A read (`write: false`) or a write (`true`: its taint is consumed
    /// first) of `ino`.
    pub(crate) fn gate_io(&self, ino: Ino, owner: bool, write: bool) -> ParkGate {
        let real = self.enter_ino(ino).ok();
        ParkGate {
            owner,
            fence: [real, None],
            discard: real.filter(|_| write),
            publish: None,
        }
    }

    /// `setattr`: only a size change is I/O (a truncation is a write).
    pub(crate) fn gate_setattr(&self, ino: Ino, size: bool) -> ParkGate {
        if size {
            self.gate_io(ino, true, true)
        } else {
            self.gate_owner()
        }
    }

    /// `copy_file_range`: a read of the source, a write of the destination.
    pub(crate) fn gate_copy(&self, src: Ino, dst: Ino) -> ParkGate {
        ParkGate {
            owner: true,
            fence: [self.enter_ino(src).ok(), self.enter_ino(dst).ok()],
            discard: self.enter_ino(dst).ok(),
            publish: None,
        }
    }

    /// A publication point of `ino` (`fsync`, a close).
    pub(crate) fn gate_publish(&self, ino: Ino, owner: bool) -> ParkGate {
        ParkGate {
            owner,
            publish: self.enter_ino(ino).ok(),
            ..ParkGate::default()
        }
    }

    /// Plan 43 C7-F F-c: whether `cx`'s op, as `gate` describes it, would
    /// find a lapsing grant on a check it is about to make — and so waits.
    /// One relaxed load while no grant is lapsing.
    pub(crate) fn lock_would_park(&self, cx: &OpCtx<'_>, gate: &ParkGate) -> bool {
        let Some(l) = self.cluster_locks() else {
            return false;
        };
        if !l.meta.locks().active() || REDISPATCHED.with(|f| f.get()) {
            return false;
        }
        self.lock_gate_parked(l, cx.caller.pid, cx.lock_owner, gate)
    }

    fn lock_gate_parked(
        &self,
        l: &crate::locks::ClusterLocks,
        pid: Option<u32>,
        lock_owner: Option<u64>,
        gate: &ParkGate,
    ) -> bool {
        if gate.owner && l.owner_fenced(pid, lock_owner) == Fence::Parked {
            return true;
        }
        let locks = l.meta.locks();
        let now = crate::locks::now();
        if gate
            .fence
            .iter()
            .flatten()
            .any(|i| l.fenced(*i) == Fence::Parked)
        {
            return true;
        }
        if gate.discard.is_some_and(|i| locks.lapse_pending(i, now)) {
            return true;
        }
        gate.publish.is_some_and(|i| locks.lapse_pending(i, now))
    }

    /// Plan 43 C7-F F-c: hold `cx`'s op, whose gate found a lapsing grant,
    /// without a frontend thread: the frontend registers the wait for the
    /// caller's signal and reserves what it holds (`Responder::begin_wait`:
    /// over a ring, one of the queue's lock-wait entries), the `lock-wait`
    /// pool polls the grant's verdict, and `redo` asks the op again from
    /// the start — every fence precedes its first effect — once it is in
    /// (admitted: the op runs; refused or given up: the fences answer
    /// `EIO`). A signal ends the wait with `EINTR`: after a FUSE request is
    /// sent the kernel waits uninterruptibly for its answer, so an op that
    /// cannot be interrupted would leave its process unkillable for up to
    /// the park's bound. `Err(r)`: it cannot be held (the frontend cannot
    /// defer, or the view was not opened by an engine): the caller waits
    /// inline.
    pub(crate) fn park_op<T: 'static, R: Responder<T>>(
        &self,
        cx: &OpCtx<'_>,
        gate: ParkGate,
        mut r: R,
        redo: impl FnOnce(&View, &OpCtx<'_>, R) + Send + 'static,
    ) -> Result<(), R> {
        let (Some(l), Some(view)) = (
            self.cluster_locks(),
            self.this.get().and_then(std::sync::Weak::upgrade),
        ) else {
            return Err(r);
        };
        if !self.caps.deferrable.contains(cx.kind) {
            return Err(r);
        }
        let own = match Responder::<T>::begin_wait(&mut r) {
            WaitStart::Held(own) => own,
            WaitStart::Refused => {
                l.meta
                    .locks()
                    .with_stats(|s| s.lapse_park_refused_ring += 1);
                r.done(Err(Code::Io.into()));
                return Ok(());
            }
        };
        l.meta.locks().with_stats(|s| s.lapse_parked_ops += 1);
        let owned = OwnedCx::of(cx, own);
        let cancel = owned.cancel.clone();
        let (pid, lock_owner) = (owned.caller.pid, owned.lock_owner);
        let l = l.clone();
        let ready_view = view.clone();
        let ready = move || {
            ready_view
                .cluster_locks()
                .is_none_or(|l| !ready_view.lock_gate_parked(l, pid, lock_owner, &gate))
        };
        let done = move |end: crate::locks::ParkEnd| match end {
            crate::locks::ParkEnd::Resolved => {
                REDISPATCHED.with(|f| f.set(true));
                redo(&view, &owned.cx(), r);
                REDISPATCHED.with(|f| f.set(false));
            }
            crate::locks::ParkEnd::Interrupted => r.done(Err(VfsError::from(Code::Intr))),
            crate::locks::ParkEnd::NoThread => r.done(Err(VfsError::from(Code::Io))),
        };
        l.park_deferred(cancel, ready, done);
        Ok(())
    }

    /// Plan 30 §M14: the fence at a point that publishes `ino`'s dirty
    /// data (close, release, `fsync`; checked *before* the closing
    /// owner's locks are dropped, which would lift the fence). Data
    /// written under a grant this node no longer honours — lapsed, lost,
    /// or unlocked under the fence (`LockTables::take_discard`) — is
    /// discarded, never published: a later publication would overwrite
    /// what the next holder wrote under its own grant. `Err(EIO)` then,
    /// if anything was discarded or local locks are still fenced.
    ///
    /// `Ok(true)`: `fh`, the description publishing, was open when such a
    /// discard happened earlier and has not reported it yet (plan 39
    /// §3.7, errseq-style: [`super::durable`]'s module doc); the caller
    /// reports `EIO` after its own flush. A discard found here is an error
    /// event too: `fh` reports it now, every other description open on the
    /// inode at its next close or `fsync`. One or two relaxed loads when
    /// this node holds no grant, lock or taint and never discarded.
    pub(crate) fn lock_publish_gate(
        &self,
        ino: Ino,
        fh: constellation_vfs::Fh,
    ) -> Result<bool, Code> {
        if View::is_synthetic(ino) {
            return Ok(false);
        }
        if let Some(l) = self.cluster_locks() {
            if let Discard::Discard { fenced } = self.lock_take_discard(l, ino, false) {
                let dirty = self.discard_lock_writes(ino);
                if dirty {
                    let seq = l.note_discard(ino);
                    self.handles.mark_seen(fh, seq);
                }
                if fenced || dirty {
                    return Err(Code::Io);
                }
            }
        }
        let current = self.meta.locks().error_seq(ino);
        Ok(current != 0 && self.handles.take_error(fh, ino, current))
    }

    /// `fh` reports an `EIO` for another reason (the owner fence, or its
    /// own `fsync` or close failing — a commit refused for an ended grant
    /// raises a discard event as it fails): it counts as the report of
    /// every discard on `ino` it has not reported yet.
    pub(crate) fn lock_errors_reported(&self, ino: Ino, fh: constellation_vfs::Fh) {
        let current = self.meta.locks().error_seq(ino);
        if current != 0 {
            self.handles.take_error(fh, ino, current);
        }
    }

    /// Plan 30 §M14: before a new local lock or a write on `ino` — dirty
    /// data written under an earlier grant that ended without its flush
    /// must not ride along with what comes next (under a fresh grant, or
    /// unlocked). Discarded; every description open now reports `EIO` at
    /// its next close or `fsync`.
    pub(crate) fn lock_discard_tainted(&self, ino: Ino) {
        self.lock_discard_tainted_as(ino, false);
    }

    /// [`Self::lock_discard_tainted`] for a write (or a truncation, an
    /// allocation, a copy's destination): plan 43 C7-F F-c, what is written
    /// next is pinned to the grant that covers `ino` — if it lapses before
    /// the data is published, its fate waits for the owner's verdict
    /// ([`constellation_meta::locks::HeldGrant::written`]).
    pub(crate) fn lock_discard_tainted_for_write(&self, ino: Ino) {
        self.lock_discard_tainted_as(ino, true);
    }

    fn lock_discard_tainted_as(&self, ino: Ino, write: bool) {
        let Some(l) = self.cluster_locks() else {
            return;
        };
        if matches!(
            self.lock_take_discard(l, ino, write),
            Discard::Discard { .. }
        ) && self.discard_lock_writes(ino)
        {
            l.note_discard(ino);
        }
    }

    /// [`crate::locks::ClusterLocks::take_discard`], waiting inline (bounded) while the
    /// inode's grant is lapsing (plan 43 C7-F F-c): what comes back is the
    /// verdict, never `Parked`.
    pub(crate) fn lock_take_discard(
        &self,
        l: &Arc<crate::locks::ClusterLocks>,
        ino: Ino,
        write: bool,
    ) -> Discard {
        loop {
            match l.take_discard(ino, write) {
                Discard::Parked => {
                    let pending = || l.meta.locks().lapse_pending(ino, crate::locks::now());
                    if !self.lock_wait_inline(l, None, pending) {
                        return Discard::Discard { fenced: true };
                    }
                }
                other => return other,
            }
        }
    }

    /// Plan 30 §M14: drop `ino`'s unpublished writes (staged bytes, sealed
    /// chunks and their pending-upload claims) and the kernel's pages of
    /// the file, which hold the same bytes. `true` if there were any.
    pub(super) fn discard_lock_writes(&self, ino: Ino) -> bool {
        // After any flush of the file in flight: what it publishes was its
        // to publish, as when the flush held the shard throughout.
        if !self.drop_writes(ino) {
            return false;
        }
        tracing::warn!(
            target: "constellation::locks",
            ino,
            "discarded writes made under a lock grant this node no longer holds (EIO)"
        );
        if let Some(l) = self.cluster_locks() {
            l.invalidate(ino);
        }
        true
    }
}

/// Plan 30 §M14: a recalled grant's flush — what `fsync` guarantees,
/// for one inode: the write state flushed and its manifest committed at
/// the sequencer, the inode's chunks uploaded (write-back included: the
/// next holder must be able to fetch them), and the local journal synced
/// (the log too under `--fsync-mode s3`).
impl crate::locks::LockFlush for View {
    fn view_root(&self) -> Ino {
        View::view_root(self)
    }

    fn flush_for_lock(&self, ino: Ino, grant: constellation_meta::locks::GrantId) -> bool {
        // Plan 30 §M14 phase 2: the file's commit carries the grant being
        // released as its fencing token — a commit stalled past the
        // grant's window is refused where it would land, never published
        // after the next holder's writes (phase 1 checked the fence only
        // when the flush started).
        let _lock_tag = self.cluster_locks().map(|l| {
            let tag = l
                .meta
                .locks()
                .held(ino)
                .filter(|h| h.id == grant)
                .map(|h| {
                    constellation_meta::locks::LockTag(vec![constellation_meta::locks::LockToken {
                        grant,
                        // Minted on the wall clock from the honour window,
                        // now (plan 43 C7-H1, R8).
                        until_ms: crate::locks::now().wall_of_honour(h.until).0,
                    }])
                })
                .unwrap_or_default();
            l.fixed_tag_scope(tag, ino)
        });
        // What the flush publishes was written under the grant: a new
        // local turn waits for it (`crate::locks::begin_close`). (One under
        // a recalled grant asks for a new grant anyway; counted all the
        // same, as the release waits for it.)
        let _publishing = self
            .cluster_locks()
            .filter(|l| l.meta.locks().held(ino).is_some_and(|h| h.id == grant))
            .map(|l| l.publishing(grant));
        // The release's flush publishes only what its grant still covers:
        // one that lapsed meanwhile (a partition, a stalled node) has
        // been outwaited by its owner, who may have granted the file
        // since — its data is discarded (reported at the next close).
        if let Some(l) = self.cluster_locks() {
            if matches!(
                self.lock_take_discard(l, ino, false),
                Discard::Discard { .. }
            ) && self.discard_lock_writes(ino)
            {
                l.note_discard(ino);
            }
        }
        let r = self
            .flush_inode(ino, true)
            .and_then(|()| self.drain_inode(ino))
            .and_then(|()| self.sync_barrier(ino));
        if let Err(code) = r {
            tracing::warn!(target: "constellation::locks", ino, %code, "flush before a lock release failed");
        }
        r.is_ok()
    }
}
