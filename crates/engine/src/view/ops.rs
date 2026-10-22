//! `impl Vfs for View`: every op, in the order its checks answer.
//!
//! Each op is what the FUSE adapter's `Filesystem` method was before plan
//! 31 C4 (`cli/src/fusefs_ops.rs`), minus the protocol: the watchdog
//! registration, the root renumbering, the kernel-invalidation hold-back
//! registration, the name check, the synthetic and scratch short-circuits,
//! the read waits, the lock fence — in the same order, answering the same
//! [`Code`]s — and then the backend call. The responder is completed while
//! the op is still registered with the watchdog and with the in-flight
//! registry (`_w`, `_inflight` drop after it), exactly as the reply was:
//! a kernel notification held back for an op in flight is released only
//! once that op has been answered (`crate::kernel_inval`'s module doc).

use super::*;
use constellation_vfs::policy::{RCOUNT_XATTR, RSIZE_XATTR};
use constellation_vfs::types::mode::{S_IFBLK, S_IFCHR, S_IFIFO, S_IFMT, S_IFREG, S_IFSOCK};
use constellation_vfs::{
    CopyFlags, DirSink, Fh, LockKind, LockOwner, LockRange, LockSpec, LockStatus, Name, OpCtx,
    OpKind, OpenOwner, Opened, ReadData, RenameFlags, Responder, SetAttr, SetXattrFlags,
    SetXattrMode, StatFs, TimeSet, Vfs, VfsError, ViewIdentity, WatchKey, WriteData, XattrName,
    XattrNameBuf,
};

/// The stored form of a name, or answer `NameTooLong` and return.
macro_rules! checked_name {
    ($self:expr, $name:expr, $r:ident) => {
        match $self.policies.names.check($name) {
            Ok(name) => name,
            Err(code) => {
                $r.done(Err(code.into()));
                return;
            }
        }
    };
}

/// The replica inode a frontend's `$ino` names, or answer `Stale` and
/// return: the view's root renumbered, and confinement (`View::enter_ino`).
macro_rules! enter {
    ($self:expr, $ino:expr, $r:ident) => {
        match $self.enter_ino($ino) {
            Ok(ino) => ino,
            Err(code) => {
                $r.done(Err(code.into()));
                return;
            }
        }
    };
}

/// Per-view admission (`ViewQos`): held until the op has answered, or
/// answer `Again`/`Intr` and return.
macro_rules! admit {
    ($self:expr, $cx:expr, $r:ident, $admitted:ident) => {
        let $admitted = match $self.admission.admit($cx) {
            Ok(admitted) => {
                // Plan 30 §M14: the caller is a lock owner (or its
                // process) whose grant lapsed: `EIO`, on any file. A
                // refused `write` does not count as the report of a
                // pending discard error (plan 39 §3.7): as with Linux's
                // errseq, only `fsync` and close report one — `fsync`
                // checks this fence itself, after `enter!`, for that.
                if $self.lock_owner_fenced($cx) {
                    $r.done(Err(Code::Io.into()));
                    return;
                }
                admitted
            }
            Err(code) => {
                $r.done(Err(code.into()));
                return;
            }
        };
        // Plan 30 §M14 phase 2: the op's mutations carry the fencing
        // token of the locks its caller holds (one relaxed load while
        // this node holds none). Dropped before the admission. (With it,
        // whose request the writes are for, for the store-stall fault
        // point: inert while unset.)
        let _lock_tag = (
            $self.lock_tag_scope($cx),
            constellation_meta::fault_caller_scope($cx.caller.pid),
        );
    };
    // The caller checks the owner fence itself (`fsync`: after `enter!`).
    ($self:expr, $cx:expr, $r:ident, $admitted:ident, owner_fence_checked_later) => {
        let $admitted = match $self.admission.admit($cx) {
            Ok(admitted) => admitted,
            Err(code) => {
                $r.done(Err(code.into()));
                return;
            }
        };
        let _lock_tag = (
            $self.lock_tag_scope($cx),
            constellation_meta::fault_caller_scope($cx.caller.pid),
        );
    };
}

/// Plan 43 C7-F F-c: an op that finds its caller's lock grant lapsing waits
/// for the owner's verdict *without a frontend thread*: where the checks
/// below it (the fences, the publication points) would see a lapsing grant,
/// the op is parked (`View::park_op`) and asked again from the start once
/// the verdict is in. `$r` is given back (`Err`) when it cannot be held, and
/// the op goes on: its fences then wait inline, bounded. One relaxed load
/// while no grant is lapsing.
macro_rules! park_gate {
    ($self:expr, $cx:expr, $r:ident, $t:ty, $gate:expr, [$($prep:tt)*], |$v:ident, $c:ident, $r2:ident| $redo:expr) => {
        if $self.lock_parking() {
            let gate = $gate;
            if $self.lock_would_park($cx, &gate) {
                // Owned copies of what the op borrows: only on this path.
                $($prep)*
                match $self.park_op::<$t, _>($cx, gate, $r, move |$v, $c, $r2| $redo) {
                    Ok(()) => return,
                    Err(back) => $r = back,
                }
            }
        }
    };
}

/// A `setattr` time: the engine's clock for "now" (plan 30 §M12: the
/// node's HLC, never below a stamp it applied, like every other timestamp
/// a mutation writes).
fn time_ns(t: TimeSet) -> i64 {
    match t {
        TimeSet::Now => constellation_meta::hlc::now_ns(),
        TimeSet::At(ns) => ns,
    }
}

fn err<T>(code: Code) -> Result<T, VfsError> {
    Err(code.into())
}

/// The largest xattr value (Linux's `XATTR_SIZE_MAX`).
const XATTR_VALUE_MAX: usize = 64 * 1024;

impl Vfs for View {
    fn parking(&self) -> bool {
        self.lock_may_wrap()
    }

    fn handover_blockers(&self) -> Vec<String> {
        View::handover_blockers(self)
    }

    fn frontend_negotiated(&self, caps: &FrontendCaps) {
        self.set_single_worker(caps.single_worker);
        self.set_passthrough_on(caps.passthrough);
        self.set_zero_copy_on(caps.zero_copy, caps.zero_copy_min_read);
    }

    fn passthrough_probe(&self) -> Option<std::io::Result<std::fs::File>> {
        Some(self.cache.probe_file())
    }

    fn identity(&self) -> ViewIdentity {
        ViewIdentity {
            id: self.id,
            metric_view: super::spec::metric_view_label(&self.labels),
        }
    }

    fn strict_cto(&self) -> bool {
        self.sync.as_ref().is_some_and(|h| h.cto_strict)
    }

    fn lookup<R: Responder<Entry>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let _w = self.watch.enter("lookup", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        // `.` and `..` (§6.12): `..` of the view's root is the root, as on
        // a real filesystem's root, and never the subtree's parent.
        if (name == "." || name == "..") && !View::is_synthetic(parent) {
            let target = if name == "." || parent == self.view_root {
                Ok(Some(parent))
            } else {
                self.meta
                    .parents_of(parent)
                    .map(|parents| parents.first().copied())
                    .map_err(|e| e.code())
            };
            let attr = match target {
                Ok(Some(ino)) => {
                    // The attributes of the directory itself or of its
                    // parent are waited for as `getattr` waits for its
                    // own (plan 30 §M6: a lookup returns attributes).
                    self.session_wait(&[ReadKey::Ino(ino)]);
                    self.meta.getattr(ino).and_then(|attr| match attr {
                        Some(attr) => Ok(Some(attr)),
                        None => self.meta.scratch_getattr(ino),
                    })
                }
                Ok(None) => Ok(None),
                Err(code) => {
                    r.done(err(code));
                    return;
                }
            };
            match attr {
                Ok(Some(attr)) => r.done(Ok(self.entry_out(&self.visible_attr(attr)))),
                Ok(None) => r.done(err(Code::NotFound)),
                Err(e) => r.done(err(e.code())),
            }
            return;
        }
        match self.lookup_synthetic(parent, &name) {
            Ok(Some((_ino, attr))) => {
                r.done(Ok(self.entry_out(&attr)));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                r.done(err(error));
                return;
            }
        }
        let scratch_parent = self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self
                .meta
                .scratch_getattr(parent)
                .ok()
                .flatten()
                .is_some_and(|attr| attr.kind == InodeKind::Dir);
        let scratch = scratch_parent
            .then(|| self.meta.scratch_lookup(parent, &name))
            .transpose();
        match scratch {
            Ok(Some(Some(attr))) => {
                // A scratch file being written has a session too.
                // `None`: the inode vanished since the lookup; the looked-up
                // row is the best there is.
                match self.current_attr(attr.ino) {
                    Ok(current) => r.done(Ok(self.entry_out(&current.unwrap_or(attr)))),
                    Err(code) => r.done(err(code)),
                }
                return;
            }
            Ok(_) => {}
            Err(e) => {
                r.done(err(e.code()));
                return;
            }
        }
        // Plan 30 §M8: under `--cto strict` a lookup sees every entry
        // another node's completed close made (the first step of an
        // open by path).
        self.strict_read(
            parent,
            true,
            Some(&name),
            &[ReadKey::Dentry(parent, name.to_string())],
        );
        constellation_vfs::watch::stage("meta read");
        match self.meta.lookup(parent, &name) {
            Ok(Some(attr)) => {
                // Plan 30 §M6: a lookup returns the child's attributes,
                // so it waits for the child's `Ino` as `getattr` does for
                // its own — the dentry's wait above says nothing of the
                // inode it leads to (a rename's or an exchange's shadow
                // covers the entries, not the inode). The child is only known after the
                // dentry is read, and the wait comes before `current_attr`
                // takes the write shard: never block while holding one.
                // The entry is not looked up again afterwards: a rename
                // during the wait is concurrent with this lookup, and the
                // reply names the inode the name led to; `current_attr`
                // reads the inode's row anew.
                self.session_wait(&[ReadKey::Ino(attr.ino)]);
                // A name looked up while its inode has pending writes
                // must report the pending size, or the entry reply caches
                // a stale one (`current_attr`). The ino is only known
                // after the lookup, so the row is re-read under the shard.
                // `None`: the inode vanished since the lookup; the looked-up
                // row is the best there is.
                match self.current_attr(attr.ino) {
                    Ok(current) => r.done(Ok(self.entry_out(&current.unwrap_or(attr)))),
                    Err(code) => r.done(err(code)),
                }
            }
            Ok(None) => r.done(err(self.beneath_non_dir(Code::NotFound, &[parent]))),
            Err(e) => r.done(err(e.code())),
        }
    }

    fn getattr<R: Responder<Attr>>(&self, cx: &OpCtx<'_>, ino: Ino, _fh: Option<Fh>, r: R) {
        let _w = self.watch.enter("getattr", ino);
        admit!(self, cx, r, _admitted);
        let requested_ino = ino;
        let ino = enter!(self, ino, r);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                r.done(self.ino_err(ino, Code::Stale));
            } else {
                let attr = self.visible_attr(self.synthetic_attr(ino, &node));
                r.done(Ok(self.attr_out(&attr)));
            }
            return;
        }
        // Pending writes shadow the committed size (a session detached
        // by an in-flight flush or read included), read under the write
        // shard (`current_attr`): with host-sized concurrent FUSE
        // dispatch, a row read before the shard check is routinely the
        // pre-flush size with the session already retired — the harness's
        // single-client `baseline` read sizes of 0 back for just-closed
        // files about half the time.
        //
        // Plan 30 §M6: the session wait comes first — never block while
        // holding a write shard.
        self.session_wait(&[ReadKey::Ino(ino)]);
        match self.current_attr(ino) {
            Ok(Some(attr)) => {
                // Plan 42 K1's fault point: a reply generated now and
                // answered later (race 2b).
                if let Some(delay) = crate::fault::getattr_delay(ino, attr.kind == InodeKind::Dir) {
                    std::thread::sleep(delay);
                }
                let attr = if requested_ino == constellation_fs_core::types::ROOT_INO {
                    self.visible_attr(attr)
                } else {
                    attr
                };
                r.done(Ok(self.attr_out(&attr)))
            }
            Ok(None) => match self.dead_attr(ino) {
                Some(attr) => r.done(Ok(self.attr_out(&attr))),
                None => r.done(self.ino_err(ino, Code::NotFound)),
            },
            Err(code) => r.done(self.ino_err(ino, code)),
        }
    }

    fn forget(&self, ino: Ino) {
        let ino = self.real_ino(ino);
        self.forget_dead(ino);
        self.forget_unlinked(ino);
    }

    fn setattr<R: Responder<Attr>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh_of_setattr: Option<Fh>,
        set: &SetAttr,
        mut r: R,
    ) {
        park_gate!(self, cx, r, Attr, self.gate_setattr(ino, set.size.is_some()), [
            let set_owned = set.clone();
        ], |v, c, r2| {
            <View as Vfs>::setattr(v, c, ino, fh_of_setattr, &set_owned, r2)
        });
        let _w = self.watch.enter("setattr", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        // An mtime set (`utimes`, `futimens`, `touch`) of a file with a
        // write session on this node changes the session, so it is one
        // of the operations [`InodeOps`] orders: it holds the inode's
        // operation lock across the row's change and the fold into the
        // session below. A flush holds that lock from taking the session
        // out of its shard until its commit (which publishes the
        // session's mtime) has returned, and a read holds it while the
        // session is out. A set in that window found no session to fold
        // into, and the commit then published the write's time over it:
        // a `utime` lost to the close of a write that had returned before
        // it. Now the set is ordered before the flush (folded: the commit
        // carries it) or after it, and the log orders the two records
        // alike, so every replica replays the same mtime.
        //
        // No session here, no lock: a session opened after this check is
        // a write's that overlaps the set (either order is right), and a
        // write that returned before it left one — attached, detached, or
        // retired only once its commit returned. So a `touch` of a file
        // that is only being read never waits for a chunk fetch, nor for
        // the upload drain that follows a local commit of a close. With a
        // session it waits as a write to the file would: on a non-owner in
        // write-through that is the close's upload, which precedes the
        // forwarded commit. A truncate opens a session itself (`self.truncate` re-enters
        // the lock). Mode, owner and atime need no lock: a manifest commit
        // carries none of them, and it leaves the file the later of its
        // ctime and the row's (`WriteState::changed`).
        let _op = (set.mtime.is_some() && (set.size.is_some() || self.writes.has_session(ino)))
            .then(|| self.inode_ops.lock(ino));
        let atime_ns = set.atime.map(time_ns);
        // `TimeSet::Now` read once ordered: the set's time, not a wait's.
        let mtime_ns = set.mtime.map(time_ns);
        // An explicit atime set must drop any queued read-bump for this
        // inode, so a pending coalesced bump cannot clobber a fresh
        // `touch -a` locally (the remote case is handled by the ctime
        // guard in replay).
        if atime_ns.is_some() {
            self.atime.purge(ino);
        }
        if let Some(new_size) = set.size {
            // truncate(2): `EISDIR` for a directory, `EINVAL` for anything
            // else that is not a regular file (the kernel checks this
            // above a FUSE mount; a frontend that does not must get it
            // here, not a size written into a directory's record).
            let kind = match self.meta.getattr(ino) {
                Ok(Some(attr)) => Some(attr.kind),
                Ok(None) => self
                    .meta
                    .scratch_getattr(ino)
                    .ok()
                    .flatten()
                    .map(|attr| attr.kind),
                Err(_) => None,
            };
            match kind {
                Some(InodeKind::Dir) => {
                    r.done(self.ino_err(ino, Code::IsDir));
                    return;
                }
                Some(InodeKind::File) | None => {}
                Some(_) => {
                    r.done(self.ino_err(ino, Code::Invalid));
                    return;
                }
            }
            // Plan 30 §M14: a truncation is a write (fenced under a
            // lapsed lock grant).
            if self.lock_fenced(ino) {
                r.done(self.ino_err(ino, Code::Io));
                return;
            }
            self.lock_discard_tainted_for_write(ino);
            if let Err(error) = self.truncate(ino, new_size) {
                r.done(self.ino_err(ino, error));
                return;
            }
        }
        let result = match self.mutate_op(
            ino,
            constellation_meta::MutateOp::Setattr {
                ino,
                mode: set.mode,
                uid: set.uid,
                gid: set.gid,
                size: set.size,
                atime_ns,
                mtime_ns,
            },
        ) {
            // `fchmod`/`ftruncate`/... on a descriptor of an unlinked
            // file: nothing to publish, the orphan's own record changes.
            Err(Code::NotFound) if self.unlinked(ino) => self
                .meta
                .orphan_setattr(
                    ino, set.mode, set.uid, set.gid, set.size, atime_ns, mtime_ns,
                )
                .map_err(|error| error.code())
                .and_then(|attr| attr.ok_or(Code::NotFound))
                .map(|mut attr| {
                    let mut writes = self.writes.lock(ino);
                    if let (Some(mtime), Some(ws)) = (mtime_ns, writes.get_mut(&ino)) {
                        ws.times_set(mtime);
                    }
                    if let Some(pending) = self.writes.pending(&writes, ino) {
                        pending.overlay(&mut attr);
                    }
                    attr
                }),
            Err(error) => Err(error),
            // A `chmod`/`chown`/`utimes` of a file still being written
            // reports the session's size, not the committed row's
            // (`current_attr`: the kernel takes this reply's size). An
            // mtime set goes into the session too: its flush commits the
            // session's mtime, which must now be this one. The session is
            // in its shard here whenever one exists: `_op` keeps every
            // flush and read that takes it out off this inode.
            Ok(()) => {
                if let Some(mtime) = mtime_ns {
                    if let Some(ws) = self.writes.lock(ino).get_mut(&ino) {
                        ws.times_set(mtime);
                    }
                }
                self.current_attr(ino)
                    .and_then(|attr| attr.ok_or(Code::NotFound))
            }
        };
        match result {
            Ok(attr) => r.done(Ok(self.attr_out(&attr))),
            Err(error) => r.done(self.ino_err(ino, error)),
        }
    }

    fn readlink<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let _w = self.watch.enter("readlink", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                r.done(self.ino_err(ino, Code::Stale));
            } else if let SyntheticNode::Frozen {
                kind: InodeKind::Symlink,
                target: Some(target),
                ..
            } = node
            {
                r.done(Ok(constellation_vfs::name::wire_bytes(&target).into_owned()));
            } else {
                r.done(self.ino_err(ino, Code::Invalid));
            }
            return;
        }
        self.session_wait(&[ReadKey::Ino(ino)]);
        match self.meta.readlink(ino) {
            Ok(Some(target)) => {
                r.done(Ok(constellation_vfs::name::wire_bytes(&target).into_owned()))
            }
            // Not a symlink — or no inode at all any more (`View::gone`).
            Ok(None) => r.done(err(match self.gone(ino, Code::NotFound) {
                Code::Stale => Code::Stale,
                _ => Code::Invalid,
            })),
            Err(e) => r.done(self.ino_err(ino, e.code())),
        }
    }

    fn mknod<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        rdev: constellation_types::Rdev,
        mut r: R,
    ) {
        park_gate!(self, cx, r, Entry, self.gate_owner(), [
            let name_owned = name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::mknod(v, c, parent, &name_owned, mode, rdev, r2)
        });
        let _w = self.watch.enter("mknod", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let kind = match mode & S_IFMT {
            S_IFREG | 0 => InodeKind::File,
            S_IFIFO => InodeKind::Fifo,
            S_IFSOCK => InodeKind::Socket,
            S_IFBLK => InodeKind::BlockDev,
            S_IFCHR => InodeKind::CharDev,
            _ => {
                r.done(err(Code::Invalid));
                return;
            }
        };
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return r.done(err(e.code())),
        };
        let (uid, gid, mode) = self.new_owner(cx.caller, parent, mode & 0o7777, false);
        let op = if kind == InodeKind::File {
            constellation_meta::MutateOp::Create {
                parent,
                name: name.into_owned(),
                ino,
                mode: mode & 0o7777,
                uid,
                gid,
            }
        } else {
            constellation_meta::MutateOp::Mknod {
                parent,
                name: name.into_owned(),
                ino,
                kind: kind.as_u8(),
                mode: mode & 0o7777,
                uid,
                gid,
                // The journal carries the portable pair (plan 31 §7); the
                // frontend decoded its platform's encoding.
                rdev,
            }
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => r.done(Ok(self.entry_out(&attr))),
                Ok(None) => r.done(err(Code::Io)),
                Err(e) => r.done(err(e.code())),
            },
            Err(e) => r.done(err(e)),
        }
    }

    fn mkdir<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        mut r: R,
    ) {
        park_gate!(self, cx, r, Entry, self.gate_owner(), [
            let name_owned = name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::mkdir(v, c, parent, &name_owned, mode, r2)
        });
        let _w = self.watch.enter("mkdir", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return r.done(err(e.code())),
        };
        let (uid, gid, mode) = self.new_owner(cx.caller, parent, mode, true);
        if self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some()
        {
            return match self.meta.scratch_mkdir(parent, &name, ino, mode, uid, gid) {
                Ok(attr) => r.done(Ok(self.entry_out(&attr))),
                Err(e) => r.done(err(e.code())),
            };
        }
        let op = constellation_meta::MutateOp::Mkdir {
            parent,
            name: name.into_owned(),
            ino,
            mode,
            uid,
            gid,
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => r.done(Ok(self.entry_out(&attr))),
                Ok(None) => r.done(err(Code::Io)),
                Err(e) => r.done(err(e.code())),
            },
            Err(e) => r.done(err(e)),
        }
    }

    fn symlink<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        target: &[u8],
        mut r: R,
    ) {
        park_gate!(self, cx, r, Entry, self.gate_owner(), [
            let name_owned = name.to_buf();
            let target_owned = target.to_vec();
        ], |v, c, r2| {
            <View as Vfs>::symlink(v, c, parent, &name_owned, &target_owned, r2)
        });
        let _w = self.watch.enter("symlink", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let target = constellation_vfs::name::stored_form(target);
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return r.done(err(e.code())),
        };
        // A symlink's mode is fixed (0777): only the group is inherited.
        let (uid, gid, _) = self.new_owner(cx.caller, parent, 0o777, false);
        let op = constellation_meta::MutateOp::Symlink {
            parent,
            name: name.into_owned(),
            ino,
            target: target.into_owned(),
            uid,
            gid,
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => r.done(Ok(self.entry_out(&attr))),
                Ok(None) => r.done(err(Code::Io)),
                Err(e) => r.done(err(e.code())),
            },
            Err(e) => r.done(err(e)),
        }
    }

    fn link<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        new_parent: Ino,
        new_name: &Name,
        mut r: R,
    ) {
        park_gate!(self, cx, r, Entry, self.gate_owner(), [
            let name_owned = new_name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::link(v, c, ino, new_parent, &name_owned, r2)
        });
        let _w = self.watch.enter("link", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        let new_parent = enter!(self, new_parent, r);
        let _inflight = self.inflight.enter(&[ino, new_parent]);
        let name = checked_name!(self, new_name, r);
        // §6.12 `confine_links`: a second name only within one of the
        // inode's link domains (the view module doc's "Link domains").
        if self.confine_links && !View::is_synthetic(ino) && !View::is_synthetic(new_parent) {
            if let Err(code) = self.link_within_domain(ino, new_parent) {
                r.done(self.ino_err(ino, code));
                return;
            }
        }
        let op = constellation_meta::MutateOp::Link {
            ino,
            parent: new_parent,
            name: name.into_owned(),
        };
        match self.mutate_op(new_parent, op) {
            // The linked inode may have a session open (`current_attr`).
            Ok(()) => match self.current_attr(ino) {
                Ok(Some(attr)) => r.done(Ok(self.entry_out(&attr))),
                Ok(None) => r.done(self.ino_err(ino, Code::NotFound)),
                Err(code) => r.done(self.ino_err(ino, code)),
            },
            Err(e) => r.done(self.ino_err(ino, e)),
        }
    }

    fn unlink<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, mut r: R) {
        park_gate!(self, cx, r, (), self.gate_owner(), [
            let name_owned = name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::unlink(v, c, parent, &name_owned, r2)
        });
        let _w = self.watch.enter("unlink", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let target = self.meta.lookup(parent, &name);
        let result = if (self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some())
            && self
                .meta
                .scratch_lookup(parent, &name)
                .ok()
                .flatten()
                .is_some()
        {
            // unlink(2) of a directory is `EISDIR`; `rmdir` removes one
            // (the two branches were swapped from the scratch area's
            // start, f11c20e: `unlink` removed an empty scratch directory
            // and `rmdir` of one answered `EISDIR`).
            match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr)) if attr.kind == InodeKind::Dir => Err(Code::IsDir),
                Ok(Some(_)) => self
                    .meta
                    .scratch_unlink(parent, &name)
                    .map_err(|e| e.code()),
                Ok(None) => Err(Code::NotFound),
                Err(e) => Err(e.code()),
            }
        } else {
            if let Ok(Some(attr)) = &target {
                self.unlinking(attr);
            }
            self.mutate_op(
                parent,
                constellation_meta::MutateOp::Unlink {
                    parent,
                    name: name.into_owned(),
                },
            )
        };
        match result {
            Ok(()) => {
                // No open handle in any view of this node, nor a
                // reference of the kernel's: reap now.
                if let Ok(Some(attr)) = target {
                    self.reap_after_unlink(attr.ino);
                }
                r.done(Ok(()))
            }
            Err(e) => {
                if let Ok(Some(attr)) = target {
                    self.unlink_failed(attr.ino);
                }
                r.done(err(self.beneath_non_dir(e, &[parent])))
            }
        }
    }

    fn rmdir<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, mut r: R) {
        park_gate!(self, cx, r, (), self.gate_owner(), [
            let name_owned = name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::rmdir(v, c, parent, &name_owned, r2)
        });
        let _w = self.watch.enter("rmdir", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let target = self.meta.lookup(parent, &name);
        let result = if (self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some())
            && self
                .meta
                .scratch_lookup(parent, &name)
                .ok()
                .flatten()
                .is_some()
        {
            // rmdir(2): `ENOTDIR` for anything but a directory,
            // `ENOTEMPTY` for one with entries (see `unlink`).
            match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr)) if attr.kind != InodeKind::Dir => Err(Code::NotDir),
                Ok(Some(attr)) => match self.meta.scratch_readdir(attr.ino) {
                    Ok(entries) if !entries.is_empty() => Err(Code::NotEmpty),
                    Ok(_) => self
                        .meta
                        .scratch_unlink(parent, &name)
                        .map_err(|e| e.code()),
                    Err(e) => Err(e.code()),
                },
                Ok(None) => Err(Code::NotFound),
                Err(e) => Err(e.code()),
            }
        } else {
            if let Ok(Some(attr)) = &target {
                self.unlinking(attr);
            }
            self.mutate_op(
                parent,
                constellation_meta::MutateOp::Rmdir {
                    parent,
                    name: name.into_owned(),
                },
            )
        };
        match result {
            Ok(()) => {
                // A removed directory is an orphan as an unlinked file
                // is (`ns::orphan_dir`): kept for the kernel's
                // reference to it, else reaped now.
                if let Ok(Some(attr)) = target {
                    self.reap_after_unlink(attr.ino);
                }
                r.done(Ok(()))
            }
            Err(e) => {
                if let Ok(Some(attr)) = target {
                    self.unlink_failed(attr.ino);
                }
                r.done(err(self.beneath_non_dir(e, &[parent])))
            }
        }
    }

    fn rename<R: Responder<Renamed>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        new_parent: Ino,
        new_name: &Name,
        flags: RenameFlags,
        mut r: R,
    ) {
        park_gate!(self, cx, r, Renamed, self.gate_owner(), [
            let name_owned = name.to_buf();
            let new_name_owned = new_name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::rename(v, c, parent, &name_owned, new_parent, &new_name_owned, flags, r2)
        });
        let _w = self.watch.enter("rename", parent);
        admit!(self, cx, r, _admitted);
        // renameat2(2): `EINVAL` for a flag the filesystem does not
        // support (`RENAME_WHITEOUT`, anything unknown) and for
        // `NOREPLACE|EXCHANGE` together — never `ENOSYS`/`EOPNOTSUPP`,
        // which would make the Linux kernel stop sending `FUSE_RENAME2`
        // (and so the flags it does support) for the rest of the mount.
        let noreplace = flags.contains(RenameFlags::NOREPLACE);
        let exchange = flags.contains(RenameFlags::EXCHANGE);
        if flags.intersects(RenameFlags::WHITEOUT | RenameFlags::UNSUPPORTED)
            || (noreplace && exchange)
        {
            r.done(err(Code::Invalid));
            return;
        }
        let parent = enter!(self, parent, r);
        let newparent = enter!(self, new_parent, r);
        let _inflight = self.inflight.enter(&[parent, newparent]);
        // Enforce NAME_MAX on both names, like every other name-taking op;
        // rename previously converted them with `to_string_lossy` directly
        // and so accepted names the metadata plane would otherwise store
        // over the POSIX limit. `checked_name!` answers `NameTooLong` and
        // returns for an over-long name (`r` is consumed only on that
        // diverging path, so it stays available for the second check and
        // the operation itself).
        let name = checked_name!(self, name, r).into_owned();
        let newname = checked_name!(self, new_name, r).into_owned();
        let src_scratch = self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some();
        let dst_scratch = self.meta.is_scratch_dir(newparent).unwrap_or(false)
            || self
                .meta
                .scratch_getattr(newparent)
                .ok()
                .flatten()
                .is_some();
        if exchange && (src_scratch || dst_scratch) {
            // The node-local scratch area keeps its own namespace; a swap
            // there (or across its boundary) is not implemented.
            r.done(err(Code::Invalid));
            return;
        }
        if src_scratch && dst_scratch {
            if noreplace {
                // The scratch area is node-local: its entries change only
                // through this node's own ops, and a kernel frontend holds
                // both directories' `i_rwsem` for the length of a rename,
                // so no other node can race this check (the scratch
                // namespace has no sequencer to decide it).
                match self.meta.scratch_lookup(newparent, &newname) {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        r.done(err(Code::Exists));
                        return;
                    }
                    Err(error) => {
                        r.done(err(error.code()));
                        return;
                    }
                }
            }
            // Plan 42 §5.3.1: the inode moved, for the frontend's kernel.
            let moved = self
                .meta
                .scratch_lookup(parent, &name)
                .ok()
                .flatten()
                .map(|a| self.out_ino(a.ino));
            return match self.meta.scratch_rename(parent, &name, newparent, &newname) {
                Ok(()) => r.done(Ok(Renamed {
                    moved,
                    exchanged: None,
                })),
                Err(error) => r.done(err(error.code())),
            };
        }
        if dst_scratch {
            r.done(err(Code::CrossDevice));
            return;
        }
        if src_scratch {
            let attr = match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr)) => attr,
                Ok(None) => {
                    r.done(err(Code::NotFound));
                    return;
                }
                Err(error) => {
                    r.done(err(error.code()));
                    return;
                }
            };
            if attr.kind != InodeKind::File {
                r.done(err(Code::CrossDevice));
                return;
            }
            // Plan 30 §M14: publishing the file is a publication point.
            // No description publishes here (`Fh(0)`): an earlier
            // discard stays owed to the application's descriptions.
            if let Err(error) = self.lock_publish_gate(attr.ino, Fh(0)) {
                r.done(err(error));
                return;
            }
            if let Err(error) = self.flush_inode(attr.ino, true) {
                r.done(err(error));
                return;
            }
            if let Err(error) = self.drain_inode(attr.ino) {
                r.done(err(error));
                return;
            }
            let manifest = match self.meta.scratch_manifest(attr.ino) {
                Ok(Some(manifest)) => manifest,
                Ok(None) => Manifest::empty(self.chunk_size).encode(),
                Err(error) => {
                    r.done(err(error.code()));
                    return;
                }
            };
            let xattrs = match self.meta.scratch_xattrs(attr.ino) {
                Ok(xattrs) => xattrs,
                Err(error) => {
                    r.done(err(error.code()));
                    return;
                }
            };
            // The file the publish replaces, as a rename's below: its last
            // name goes, and it is an orphan kept for the kernel's
            // reference to it, else reaped now.
            let replaced = if noreplace {
                None
            } else {
                self.meta.lookup(newparent, &newname).ok().flatten()
            };
            if let Some(attr) = &replaced {
                self.unlinking(attr);
            }
            let op = constellation_meta::MutateOp::Publish {
                ino: attr.ino,
                parent: newparent,
                name: newname,
                mode: attr.mode,
                uid: attr.uid,
                gid: attr.gid,
                mtime_ns: attr.mtime_ns,
                manifest,
                size: attr.size,
                xattrs,
                noreplace,
            };
            return match self.mutate_op(newparent, op) {
                Ok(()) => {
                    if let Some(attr) = replaced {
                        self.reap_after_unlink(attr.ino);
                    }
                    match self.meta.scratch_unlink(parent, &name) {
                        Ok(()) => r.done(Ok(Renamed {
                            moved: Some(self.out_ino(attr.ino)),
                            exchanged: None,
                        })),
                        Err(error) => r.done(err(error.code())),
                    }
                }
                Err(error) => {
                    if let Some(attr) = replaced {
                        self.unlink_failed(attr.ino);
                    }
                    r.done(err(error))
                }
            };
        }
        // §6.12 `confine_links`: moving one of several names of a file
        // into another link domain would leave the inode in two (an
        // exchange moves both entries, each the other way).
        if self.confine_links && parent != newparent {
            let crossing = self
                .rename_within_domains(parent, &name, newparent)
                .and_then(|()| {
                    if exchange {
                        self.rename_within_domains(newparent, &newname, parent)
                    } else {
                        Ok(())
                    }
                });
            if let Err(code) = crossing {
                r.done(err(code));
                return;
            }
        }
        // Plan 42 §5.3.1: the inode(s) the rename moves, read before it
        // (the kernel holds both directories' locks across the rename, so
        // this node's own changes cannot interleave; another node's could,
        // and then the frontend tells an inode that did not move, which
        // costs one attribute refresh).
        let moved = self
            .meta
            .lookup(parent, &name)
            .ok()
            .flatten()
            .map(|a| a.ino);
        let exchanged = if exchange {
            self.meta
                .lookup(newparent, &newname)
                .ok()
                .flatten()
                .map(|a| a.ino)
        } else {
            None
        };
        // The target the rename replaces, if any.
        let replaced = if exchange {
            None
        } else {
            self.meta.lookup(newparent, &newname).ok().flatten()
        };
        if let Some(attr) = &replaced {
            self.unlinking(attr);
        }
        let op = if exchange {
            constellation_meta::MutateOp::Exchange {
                parent,
                name,
                new_parent: newparent,
                new_name: newname,
            }
        } else {
            constellation_meta::MutateOp::Rename {
                parent,
                name,
                new_parent: newparent,
                new_name: newname,
                noreplace,
            }
        };
        match self.mutate_op(parent, op) {
            Ok(()) => {
                // Like `unlink`'s: the target's last name went with no
                // open handle in any view of this node, so reap it now
                // (it was left in `orphans` for good).
                // A replaced directory too (`rmdir`'s).
                if let Some(attr) = replaced {
                    self.reap_after_unlink(attr.ino);
                }
                r.done(Ok(Renamed {
                    moved: moved.map(|ino| self.out_ino(ino)),
                    exchanged: exchanged.map(|ino| self.out_ino(ino)),
                }))
            }
            Err(e) => {
                if let Some(attr) = replaced {
                    self.unlink_failed(attr.ino);
                }
                r.done(err(self.beneath_non_dir(e, &[parent, newparent])))
            }
        }
    }

    fn open<R: Responder<Opened>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        flags: OpenFlags,
        owner_of_open: OpenOwner,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            Opened,
            if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC) {
                self.gate_owner()
            } else {
                super::lock_gate::ParkGate::default()
            },
            [],
            |v, c, r2| { <View as Vfs>::open(v, c, ino, flags, owner_of_open, r2) }
        );
        let _w = self.watch.enter("open", ino);
        admit!(self, cx, r, _admitted);
        // Plan 30 §M14: opening for writing is a write (a read-only open
        // is not fenced: the fence must not stop a fenced owner from
        // reaching its own close).
        if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC) && self.lock_owner_fenced_any(cx) {
            r.done(self.ino_err(ino, Code::Io));
            return;
        }
        let ino = enter!(self, ino, r);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                r.done(self.ino_err(ino, Code::Stale));
            } else if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC) {
                // A snapshot mirror is frozen: `EROFS`, as a read-only
                // mount answers an open for writing.
                r.done(self.ino_err(ino, Code::ReadOnly));
            } else if matches!(
                node,
                SyntheticNode::Frozen {
                    kind: InodeKind::File,
                    ..
                }
            ) {
                // The handle first: `release_frozen` trims the pins to the
                // handles the table still lists, so a pin must never exist
                // without its handle there (plan 38 Z3c).
                let fh = self.open_handle(ino, OpenFlags::READ);
                let backing = self.frozen_passthrough_backing(ino, flags, &node);
                // Plan 38 §3(d), as for a live file below; nothing writes
                // a frozen one.
                let zero_copy = backing.is_none() && self.zero_copy_open_frozen(flags, &node);
                if zero_copy {
                    self.note_zero_copy_open(fh);
                }
                r.done(Ok(Opened {
                    fh,
                    backing,
                    zero_copy,
                    cache: self.frozen_stamp(ino),
                }));
            } else {
                r.done(self.ino_err(ino, Code::IsDir));
            }
            return;
        }
        // Plan 30 §M8: under `--cto strict`, the close-to-open point.
        self.strict_read(ino, false, None, &[ReadKey::Ino(ino)]);
        // Plan 42 §5.1.1: the record read answers the cache stamp too
        // (a node-local scratch file has none).
        let attr = self.meta.getattr_stamped(ino).and_then(|attr| match attr {
            Some(stamped) => Ok(Some(stamped)),
            None => Ok(self.meta.scratch_getattr(ino)?.map(|attr| (attr, None))),
        });
        match attr {
            Ok(Some((attr, stamp))) => {
                *self.opens.lock().unwrap().entry(ino).or_insert(0) += 1;
                self.note_writer_open(ino, flags);
                // Plan 38 §3(c): a read-only open of a one-chunk file
                // whose chunk is cached and verified is answered with the
                // chunk file itself, and the pin that keeps it where the
                // handle expects it is dropped in `release`.
                let backing = self.passthrough_backing(ino, flags, &attr);
                let fh = self.open_handle(ino, flags);
                // Plan 38 §3(d): a read-only open the frontend can serve
                // zero-copy reads for is marked so; which of its reads are
                // zero-copy is decided per read (`zero_copy`'s module doc).
                // A passthrough open's reads never reach the daemon, so it
                // is not marked.
                let zero_copy = backing.is_none() && self.zero_copy_open(ino, flags, &attr);
                if zero_copy {
                    self.note_zero_copy_open(fh);
                }
                r.done(Ok(Opened {
                    fh,
                    backing,
                    zero_copy,
                    cache: self.open_stamp(ino, stamp),
                }))
            }
            Ok(None) => r.done(self.ino_err(ino, Code::NotFound)),
            Err(e) => r.done(self.ino_err(ino, e.code())),
        }
    }

    fn create<R: Responder<(Entry, Opened)>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        flags: OpenFlags,
        owner_of_create: OpenOwner,
        mut r: R,
    ) {
        park_gate!(self, cx, r, (Entry, Opened), self.gate_owner(), [
            let name_owned = name.to_buf();
        ], |v, c, r2| {
            <View as Vfs>::create(v, c, parent, &name_owned, mode, flags, owner_of_create, r2)
        });
        let _w = self.watch.enter("create", parent);
        admit!(self, cx, r, _admitted);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let started = std::time::Instant::now();
        let result = self.create_or_open(parent, &name, mode, flags, cx.caller);
        let took = started.elapsed();
        if took >= slow_fuse_op() {
            tracing::warn!(parent, name = %name, ?took, ok = result.is_ok(), "slow FUSE create");
        }
        match result {
            Ok((attr, created)) => {
                *self.opens.lock().unwrap().entry(attr.ino).or_insert(0) += 1;
                self.note_writer_open(attr.ino, flags);
                let mut opened = Opened::new(self.open_handle(attr.ino, flags));
                // Plan 42 §5.1.1: an existing file opened by a create has
                // pages a kernel may keep; a new one has none (no stamp:
                // its next open records one, and the create's path stays
                // one record read short).
                if !created {
                    let stamp = self.meta.content_stamp(attr.ino).ok().flatten();
                    opened.cache = self.open_stamp(attr.ino, stamp);
                }
                r.done(Ok((self.entry_out(&attr), opened)))
            }
            Err(e) => r.done(err(e)),
        }
    }

    fn read<R: Responder<ReadData>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u32,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            ReadData,
            self.gate_io(ino, true, false),
            [],
            |v, c, r2| { <View as Vfs>::read(v, c, ino, fh, off, len, r2) }
        );
        let _w = self.watch.enter("read", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        // §6.12: a handle addresses only the inode it was opened on (the
        // view hands out one per open and knows its inode); anything else
        // was never given out.
        if self.handle_ino(fh) != Some(ino) {
            r.done(self.ino_err(ino, Code::BadFd));
            return;
        }
        let _inflight = self.inflight.enter(&[ino]);
        // A handle marked zero-copy at its open (plan 38 §3(d)).
        let zc = self.zero_copy_handle(fh);
        if View::is_synthetic(ino) {
            match self.read_frozen(ino, zc.as_deref(), off, len as u64) {
                Ok(data) => r.done(Ok(data)),
                Err(error) => r.done(self.ino_err(ino, error)),
            }
            return;
        }
        if self.lock_fenced(ino) {
            r.done(self.ino_err(ino, Code::Io));
            return;
        }
        // A cold read defers to the completion pool (`io`'s module doc).
        let Some(view) = self.read_deferral() else {
            match self.do_read_zc(ino, zc.as_deref(), off, len as u64) {
                Ok(data) => r.done(Ok(data)),
                Err(e) => r.done(self.ino_err(ino, e)),
            }
            return;
        };
        if let Some(result) =
            super::io::cold_probe::inline(|| self.do_read_zc(ino, zc.as_deref(), off, len as u64))
        {
            match result {
                Ok(data) => r.done(Ok(data)),
                Err(e) => r.done(self.ino_err(ino, e)),
            }
            return;
        }
        // The op's registrations travel with it and end with its answer:
        // the watchdog entry (named on the pool thread's stages), the
        // kernel-invalidation hold-back, the view's admission slot.
        let admitted = _admitted.defer();
        let (watch, inflight) = (_w, _inflight);
        crate::completion::CompletionPool::global().submit(move || {
            watch.adopt();
            let result = view.do_read_zc(ino, zc.as_deref(), off, len as u64);
            match result {
                Ok(data) => r.done(Ok(data)),
                Err(e) => r.done(view.ino_err(ino, e)),
            }
            drop(inflight);
            view.admission.leave_deferred(admitted);
            drop(watch);
        });
    }

    fn write<R: Responder<u32>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        data: WriteData<'_>,
        flags: OpenFlags,
        mut r: R,
    ) {
        park_gate!(self, cx, r, u32, self.gate_io(ino, true, true), [
            let owned = bytes::Bytes::copy_from_slice(data.as_slice());
        ], |v, c, r2| {
            <View as Vfs>::write(v, c, ino, fh, off, WriteData::Shared(owned), flags, r2)
        });
        let _w = self.watch.enter("write", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        // Plan 42 K1's fault point: a slow server (race 1's window).
        if let Some(delay) = crate::fault::view_write_delay() {
            std::thread::sleep(delay);
        }
        if View::is_synthetic(ino) {
            r.done(self.ino_err(ino, Code::ReadOnly));
            return;
        }
        // `ViewQos::max_staging_bytes`: wait for this view's own flushes.
        if let Err(code) = self
            .admission
            .admit_staging(cx, data.as_slice().len() as u64)
        {
            r.done(self.ino_err(ino, code));
            return;
        }
        if self.lock_fenced(ino) {
            r.done(self.ino_err(ino, Code::Io));
            return;
        }
        self.lock_discard_tainted_for_write(ino);
        let n = match self.do_write(ino, off, data.as_slice()) {
            Ok(n) if flags.contains(OpenFlags::SYNC) => n,
            Ok(n) => return r.done(Ok(n)),
            Err(e) => return r.done(self.ino_err(ino, e)),
        };
        // `O_SYNC`/`O_DSYNC` (plan 39 §3.3): the publication waits like
        // an `fsync`, and so, like one, on the `fsync` pool — a database
        // writing its WAL this way during an outage must not occupy every
        // frontend worker (they deliver the interrupt that ends a killed
        // caller's wait).
        let cancel = cx.cancel.cloned();
        let Some(view) = self.fsync_deferral(OpKind::Write) else {
            r.done(
                self.flush_sync_write(ino, fh, cancel)
                    .map(|()| n)
                    .map_err(VfsError::from),
            );
            return;
        };
        let admitted = _admitted.defer();
        let (watch, inflight) = (_w, _inflight);
        // Plan 30 §M14 phase 2: the publication carries the caller's
        // fencing token on the pool thread too.
        let caller = crate::locks::scope_caller();
        crate::fsync_wait::pool().submit(move || {
            watch.adopt();
            let _lock_tag = caller.as_ref().map(|c| c.enter());
            r.done(
                view.flush_sync_write(ino, fh, cancel)
                    .map(|()| n)
                    .map_err(VfsError::from),
            );
            drop(inflight);
            view.admission.leave_deferred(admitted);
            drop(watch);
        });
    }

    fn flush<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            (),
            self.gate_publish(ino, false),
            [],
            |v, c, r2| { <View as Vfs>::flush(v, c, ino, fh, owner, r2) }
        );
        let _w = self.watch.enter("flush", ino);
        let ino = enter!(self, ino, r);
        // Plan 30 §M14 phase 2: the close publishes under the locks its
        // caller holds now — the tag is taken before the close drops the
        // caller's locks on this file below. A close that ends its
        // caller's turn on the file (it drops the caller's locks there)
        // and publishes counts as that turn's publication from before the
        // drop until its commit is answered: a new turn on the file waits
        // for it (`crate::locks::begin_close`).
        let _lock_tag = (
            self.lock_tag_scope(cx),
            constellation_meta::fault_caller_scope(cx.caller.pid),
        );
        let locks = self.cluster_locks().filter(|_| !View::is_synthetic(ino));
        let publishes = self.handles.writes(fh);
        let ends_turn = publishes
            && locks.is_some_and(|l| {
                l.meta
                    .locks()
                    .local_locks(ino)
                    .iter()
                    .any(|held| held.owner == owner.0)
            });
        let publishing = crate::locks::begin_close(ino, ends_turn);
        // Plan 30 §M14: the fence first, while the closing owner's locks
        // are still there (dropping them would lift it): data written
        // under a lapsed grant is discarded, never published. Then the
        // close drops the process's POSIX locks on the file (any
        // descriptor's close, as POSIX says). The kernel sends no unlock
        // of its own: it keeps no lock for a file whose locks it forwards
        // (`lock_handles`' module doc), so this is the only one.
        let gate = self.lock_publish_gate(ino, fh);
        let idle = locks.map(|l| l.drop_owner(ino, owner.0));
        let published = gate.and_then(|owed| {
            let drain = publishes && self.publish_inode(ino, false)?;
            Ok((owed, drain))
        });
        // The turn's publication is over once its commit is answered: the
        // write-through drain after it only waits for what is published to
        // reach S3 (`View::publish_inode`), and a new turn on the file does
        // not wait for that. A recalled grant's release does not need this
        // guard for it either: its own flush (`LockFlush`, before the
        // release is even considered) drains the file's pending chunks,
        // sharing this drain's in-flight uploads, and syncs the journal.
        drop(publishing);
        let result = published.and_then(|(owed, drain)| {
            if drain {
                self.drain_inode(ino)?;
            }
            if owed {
                return Err(Code::Io);
            }
            Ok(())
        });
        // As in `fsync`: this close's `EIO` reports every discard its
        // description has not reported yet, including the one its own
        // publication raised; the description (a `dup` of it may stay
        // open) does not report it again.
        if result == Err(Code::Io) {
            self.lock_errors_reported(ino, fh);
        }
        // The release of a recalled grant flushes the file first; asking
        // for it only after this flush lets that flush find it done.
        if let (Some(l), Some(true)) = (locks, idle) {
            l.idle(ino);
        }
        // This may be the description's last close: its OFD and `flock`
        // locks go with the `RELEASE` the kernel sends after this answer
        // without waiting for it, and a request that conflicts with them
        // waits for that (`lock_handles`' module doc). Marked once the
        // close's work is done, just before the kernel moves on.
        if locks.is_some_and(|l| !l.meta.locks().lockers_none()) {
            self.note_handle_closed(fh);
        }
        // The close's fencing-token scope ends before its answer, not
        // after (its publication ended above): nothing is published past
        // here, and the release of a recalled grant is not held behind
        // this op once it is answered.
        drop(_lock_tag);
        match result {
            Ok(()) => r.done(Ok(())),
            Err(e) => r.done(self.ino_err(ino, e)),
        }
    }

    fn release<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            (),
            self.gate_publish(ino, false),
            [],
            |v, c, r2| { <View as Vfs>::release(v, c, ino, fh, flags, owner, r2) }
        );
        // The description ends with this call however it answers (an
        // `enter!` refusal included): nothing addresses it any more.
        let publishes = self.handles.writes(fh);
        let _closed = self.handles.closing(fh);
        self.drop_zero_copy(fh);
        let _w = self.watch.enter("release", ino);
        let ino = enter!(self, ino, r);
        if View::is_synthetic(ino) {
            // Out of the handle table first, so the count the pins are
            // trimmed to is of the handles still open.
            drop(_closed);
            self.release_frozen(ino);
            r.done(Ok(()));
            return;
        }
        // Plan 30 §M14: the fence first (as in `flush`), then
        // `FUSE_RELEASE_FLOCK_UNLOCK` — the last close of an open file
        // drops its `flock` lock (whose owner is the open file). Phase 2:
        // the fencing token is taken before that, as in `flush`.
        let _lock_tag = (
            self.lock_tag_scope(cx),
            constellation_meta::fault_caller_scope(cx.caller.pid),
        );
        let locks = self.cluster_locks();
        // The description's own owners (and the `flock` owner the kernel
        // names) lose their locks on the file below: when they held any
        // and the close publishes, it is their turn's publication, which a
        // new turn waits for from before the drop (as in `flush`).
        let ends_turn = publishes
            && locks.is_some_and(|l| {
                let mut owners = self.lock_handles.owners_on(fh.0, ino);
                owners.extend(owner.map(|o| o.0));
                l.meta
                    .locks()
                    .local_locks(ino)
                    .iter()
                    .any(|held| owners.contains(&held.owner))
            });
        let publishing = crate::locks::begin_close(ino, ends_turn);
        let gate = self.lock_publish_gate(ino, fh);
        drop(_closed);
        // The open file description's own (OFD) locks end with it too. A
        // request waiting for them (`past_closing_locks`) asks again.
        let mut idle = false;
        if let Some(l) = locks {
            let flock = owner.map(|owner| (ino, owner.0));
            for (locked, none_left) in self.release_lock_handle(fh, flock, l) {
                if locked == ino {
                    idle |= none_left;
                } else if none_left {
                    l.idle(locked);
                }
            }
        }
        let published = gate.and_then(|owed| {
            let drain = publishes && self.publish_inode(ino, flags.contains(OpenFlags::SYNC))?;
            Ok((owed, drain))
        });
        // As in `flush`: the turn's publication ends with its commit, not
        // with the drain after it.
        drop(publishing);
        let flush_result = published.and_then(|(owed, drain)| {
            if drain {
                self.drain_inode(ino)?;
            }
            if owed {
                return Err(Code::Io);
            }
            Ok(())
        });
        if let (Some(l), true) = (locks, idle) {
            l.idle(ino);
        }
        let (last, still_open) = {
            let mut opens = self.opens.lock().unwrap();
            match opens.get_mut(&ino) {
                Some(n) => {
                    *n = n.saturating_sub(1);
                    let last = *n == 0;
                    let still_open = *n;
                    if last {
                        opens.remove(&ino);
                        self.prefetch.forget(ino);
                    }
                    (last, still_open)
                }
                None => (false, 0),
            }
        };
        // The chunk file this handle was opened on, if it got one, and the
        // disk cache's pin on it (plan 38 §3(c)). `release` does not name
        // which handle closed, so what is dropped is whatever the inode
        // holds beyond the handles still open on it.
        self.note_writer_release(ino, flags);
        self.drop_passthrough(ino, still_open);
        if last {
            self.forget_discard_errors(ino);
        }
        // Orphan reap on last close (unlink-while-open, DESIGN.md §3),
        // with the write session a flush kept for the descriptors
        // (`flush_inode`: an unlinked file publishes nothing). An inode
        // the replica lacks but holds no orphan record of is the
        // sequencer's to judge: its session goes only once the flush
        // above answered `ESTALE` (no such file there), never with a
        // success; after any other failure it stays, as a present
        // file's does. An orphan this view's platform still references
        // is left to its `forget`, session included: a reopen through
        // that reference, in this view, reads what this handle wrote
        // (`View::reap_after_unlink`; sessions are per view). One another
        // view still has open or references keeps its record for that
        // view — as `unlink`'s reap already asked every view (84a59be) —
        // and this view's session, which nothing in this view reaches any
        // more, goes.
        let reap = last && !self.unlinked_referenced(ino);
        let orphan = reap && self.orphaned(ino);
        if orphan || (reap && self.unlinked(ino) && flush_result == Err(Code::Stale)) {
            if self.drop_writes(ino) && !orphan {
                tracing::warn!(
                    ino,
                    "the last close dropped writes to a file the sequencer does not have; \
                     the close answered ESTALE"
                );
            }
            let elsewhere = self
                .holds
                .as_ref()
                .is_some_and(|h| h.sources().is_open(ino));
            if let Ok(Some(attr)) = self.meta.getattr(ino) {
                if attr.nlink == 0 && !elsewhere {
                    // Under a frontend that reports `forget`s, this view's
                    // own unlinks wait for theirs (above), so one reaped
                    // here was unlinked elsewhere — another node or view —
                    // and a reference that outlives it is stale
                    // (`View::gone`): remembered as this view's own
                    // removal, its `fstat` answered `nlink` 0 while its
                    // reopen answered `ENOENT`.
                    if !self.keeps_referenced() {
                        self.remember_dead(attr);
                    }
                    let _ = self.meta.reap_orphan(ino);
                }
            }
        }
        match flush_result {
            Ok(()) => {
                // close() is the close-to-open publication point.
                self.nudge_sync();
                r.done(Ok(()))
            }
            Err(e) => r.done(err(e)),
        }
    }

    fn fsync<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        level: Durability,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            (),
            self.gate_publish(ino, true),
            [],
            |v, c, r2| { <View as Vfs>::fsync(v, c, ino, fh, level, r2) }
        );
        let _w = self.watch.enter("fsync", ino);
        admit!(self, cx, r, _admitted, owner_fence_checked_later);
        let ino = enter!(self, ino, r);
        if self.lock_owner_fenced(cx) {
            // This `EIO` also reports a discard this description has not
            // reported yet (plan 39 §3.7: once, not again after the fence
            // lifts).
            self.lock_errors_reported(ino, fh);
            r.done(self.ino_err(ino, Code::Io));
            return;
        }
        // Plan 30 §M14: nothing written under a lapsed grant is made
        // durable (`lock_publish_gate`); a discard this description has
        // not reported yet is reported after the barrier.
        let owed = match self.lock_publish_gate(ino, fh) {
            Ok(owed) => owed,
            Err(e) => {
                // As the owner fence's `EIO` above: the report of every
                // discard this description has not reported yet.
                if e == Code::Io {
                    self.lock_errors_reported(ino, fh);
                }
                r.done(self.ino_err(ino, e));
                return;
            }
        };
        let cancel = cx.cancel.cloned();
        let finish = move |view: &View| match view.fsync_durable(ino, level, cancel) {
            Ok(()) if owed => Err(Code::Io),
            // This `EIO` is the report of every discard on the file this
            // description has not reported yet — the one its own
            // publication just raised (a commit refused for an ended
            // grant, `View::publish_inode`) among them: the next `fsync`
            // must not report it again, for data that then did land.
            Err(Code::Io) => {
                view.lock_errors_reported(ino, fh);
                Err(Code::Io)
            }
            result => result,
        };
        // Plan 39 §3.3: the wait may last an S3 outage, so it runs on the
        // `fsync` pool, never on a frontend worker (the workers must stay
        // free to deliver the interrupt that ends it).
        let Some(view) = self.fsync_deferral(OpKind::Fsync) else {
            r.done(finish(self).map_err(VfsError::from));
            return;
        };
        let admitted = _admitted.defer();
        let watch = _w;
        let caller = crate::locks::scope_caller();
        crate::fsync_wait::pool().submit(move || {
            watch.adopt();
            let _lock_tag = caller.as_ref().map(|c| c.enter());
            r.done(finish(&view).map_err(VfsError::from));
            view.admission.leave_deferred(admitted);
            drop(watch);
        });
    }

    fn readdir<R: DirSink + Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        cookie: u64,
        _plus: bool,
        mut r: R,
    ) {
        let _w = self.watch.enter("readdir", ino);
        admit!(self, cx, r, _admitted);
        let visible_ino = ino;
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        if View::is_synthetic(ino) {
            let entries = match self.synthetic_entries(ino) {
                Ok(entries) => entries,
                Err(error) => return r.done(self.ino_err(ino, error)),
            };
            let mut idx = cookie;
            loop {
                let next = idx + 1;
                let full = match idx {
                    0 => r.add(visible_ino, next, FileKind::Dir, b"."),
                    1 => r.add(visible_ino, next, FileKind::Dir, b".."),
                    _ => {
                        let Some((child_ino, kind, name)) = entries.get((idx - 2) as usize) else {
                            break;
                        };
                        r.add(
                            *child_ino,
                            next,
                            kind_out(*kind),
                            &constellation_vfs::name::wire_bytes(name),
                        )
                    }
                };
                if full {
                    break;
                }
                idx = next;
            }
            r.done(Ok(()));
            return;
        }
        // Plan 30 §M6: once per listing (its first chunk). §M8: a strict
        // listing sees every entry a completed close elsewhere made.
        if cookie == 0 {
            self.strict_read(ino, true, None, &[ReadKey::Dir(ino)]);
        }
        let entries = match if self.meta.is_scratch_dir(ino).unwrap_or(false)
            || self.meta.scratch_getattr(ino).ok().flatten().is_some()
        {
            self.meta.scratch_readdir(ino)
        } else {
            self.meta.readdir(ino)
        } {
            Ok(e) => e,
            Err(e) => return r.done(self.ino_err(ino, e.code())),
        };
        // Cookies: "." answers 1, ".." 2, and each child its name's
        // `dir_cookie` — so a listing resumes after the *name* it stopped
        // at, not at a position that an unlink or a create in between
        // (here, or replayed from another node) shifts: a positional
        // cookie repeated or skipped entries that were there throughout.
        // Children are listed in cookie order.
        let mut children: Vec<(u64, &constellation_meta::DirEntry)> = entries
            .iter()
            .map(|entry| (dir_cookie(entry.name.as_bytes()), entry))
            .collect();
        children.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.name.cmp(&b.1.name)));
        if cookie == 0 && r.add(visible_ino, 1, FileKind::Dir, b".") {
            return r.done(Ok(()));
        }
        if cookie <= 1 && r.add(visible_ino, 2, FileKind::Dir, b"..") {
            return r.done(Ok(()));
        }
        let from = children.partition_point(|(c, _)| *c <= cookie);
        for (next, child) in &children[from..] {
            if r.add(
                child.ino,
                *next,
                kind_out(child.kind),
                &constellation_vfs::name::wire_bytes(&child.name),
            ) {
                break;
            }
        }
        r.done(Ok(()));
    }

    fn statfs<R: Responder<StatFs>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let _w = self.watch.enter("statfs", ino);
        admit!(self, cx, r, _admitted);
        // §6.12: an inode outside the view answers `Stale` here too.
        let _ino = enter!(self, ino, r);
        // Used space is logical bytes under the mounted view; free space
        // is whole-filesystem headroom under the cap, bounded by the view
        // root's own subtree cap. See `statfs_blocks_capped`.
        // Block size mirrors blksize.
        let bsize: u32 = BLOCK_SIZE;
        let (used_bytes, file_count) = self.view_usage();
        let (total_blocks, bfree) = statfs_blocks_capped(
            used_bytes,
            self.meta.usage().0,
            self.cached_quota(),
            self.cached_subtree_quota(),
            bsize as u64,
        );
        let ffree = (u64::MAX / 2).saturating_sub(file_count);
        r.done(Ok(StatFs {
            blocks: total_blocks,
            bfree,
            bavail: bfree,
            files: file_count,
            ffree,
            bsize,
            namelen: 255,
            frsize: bsize,
        }));
    }

    fn fallocate<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh_of_fallocate: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            (),
            self.gate_io(ino, true, true),
            [],
            |v, c, r2| { <View as Vfs>::fallocate(v, c, ino, fh_of_fallocate, off, len, mode, r2) }
        );
        let _w = self.watch.enter("fallocate", ino);
        admit!(self, cx, r, _admitted);
        // Plan 31 §6.2: absent from a frontend that does not declare it.
        if !self.caps.fallocate {
            r.done(self.ino_err(ino, Code::NotSupported));
            return;
        }
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        if View::is_synthetic(ino) {
            r.done(self.ino_err(ino, Code::ReadOnly));
            return;
        }
        // No lease: like a write or a truncation, an allocation changes
        // this session's write state only, published with the file's next
        // manifest — forwarded to the holder from any other node. Asking
        // for the lease here (as before forwarding existed) made a
        // non-holder's `fallocate` wait out a busy holder for 2xTTL and
        // fail with EIO (stress-ng-fs-nodes: `iomix`, `fsize`, `sync-file`).
        if self.lock_fenced(ino) {
            r.done(self.ino_err(ino, Code::Io));
            return;
        }
        self.lock_discard_tainted_for_write(ino);
        if len == 0 {
            r.done(self.ino_err(ino, Code::Invalid));
            return;
        }
        match self.do_fallocate(ino, off, len, mode) {
            Ok(()) => r.done(Ok(())),
            Err(error) => r.done(self.ino_err(ino, error)),
        }
    }

    fn seek<R: Responder<u64>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        off: u64,
        whence: SeekWhence,
        r: R,
    ) {
        let _w = self.watch.enter("lseek", ino);
        admit!(self, cx, r, _admitted);
        // Plan 31 §6.2: `SEEK_DATA`/`SEEK_HOLE` are absent from a frontend
        // that does not declare them.
        if !self.caps.seek_hole {
            r.done(self.ino_err(ino, Code::NotSupported));
            return;
        }
        let ino = enter!(self, ino, r);
        match self.seek_sparse(ino, off, whence) {
            Ok(position) => r.done(Ok(position as u64)),
            Err(error) => r.done(self.ino_err(ino, error)),
        }
    }

    fn copy_file_range<R: Responder<u64>>(
        &self,
        cx: &OpCtx<'_>,
        ino_in: Ino,
        fh_in: Fh,
        off_in: u64,
        ino_out: Ino,
        fh_out: Fh,
        off_out: u64,
        len: u64,
        flags: CopyFlags,
        mut r: R,
    ) {
        park_gate!(
            self,
            cx,
            r,
            u64,
            self.gate_copy(ino_in, ino_out),
            [],
            |v, c, r2| {
                <View as Vfs>::copy_file_range(
                    v, c, ino_in, fh_in, off_in, ino_out, fh_out, off_out, len, flags, r2,
                )
            }
        );
        // A write of the destination (`copy`'s module doc): the owner
        // fence and the fencing token are the op's, as a write's.
        let _w = self.watch.enter("copy_file_range", ino_out);
        admit!(self, cx, r, _admitted);
        let src = enter!(self, ino_in, r);
        let dst = enter!(self, ino_out, r);
        // The syscall refuses every flag before a filesystem sees one.
        if !flags.is_empty() {
            r.done(self.ino_err(dst, Code::Invalid));
            return;
        }
        // §6.12: a handle addresses only the inode it was opened on; the
        // destination's must be open for writing (the kernel's own check,
        // `FMODE_WRITE`, repeated for a frontend that does not make it).
        if self.handle_ino(fh_in) != Some(src) {
            r.done(self.ino_err(src, Code::BadFd));
            return;
        }
        if View::is_synthetic(dst) {
            r.done(self.ino_err(dst, Code::ReadOnly));
            return;
        }
        let sync = match self.handles.mode(fh_out) {
            Some((ino, true, sync)) if ino == dst => sync,
            _ => {
                r.done(self.ino_err(dst, Code::BadFd));
                return;
            }
        };
        let _inflight = self.inflight.enter(&[src, dst]);
        if self.lock_fenced(src) || self.lock_fenced(dst) {
            r.done(self.ino_err(dst, Code::Io));
            return;
        }
        // Plan 39 §3.3: into an `O_SYNC`/`O_DSYNC` description, the copy
        // publishes before it answers, as each write through it does, and
        // waits on the `fsync` pool as they do; any other copy, which may
        // run for seconds and wait for chunks, on the completion pool, so
        // that no frontend thread (a ring's above all) is held by it.
        let cancel = cx.cancel.cloned();
        let deadline = cx.deadline;
        let caller = super::copy::CopyCaller {
            pid: cx.caller.pid,
            lock_owner: cx.lock_owner,
        };
        // Never `ENOSYS`/`EOPNOTSUPP`/`EXDEV` from here, whatever failed
        // beneath: Linux would copy the request itself, through its page
        // cache (`copy`'s module doc). An error names the file it is
        // about: a source gone from the replica is `Stale`, as for a read.
        let run = move |view: &View| -> Result<u64, VfsError> {
            let fail = |ino, code| view.ino_err(ino, super::copy::never_a_fallback(code));
            let n = match view.copy_range(
                src,
                off_in,
                dst,
                off_out,
                len,
                caller,
                cancel.as_ref(),
                deadline,
            ) {
                Ok(n) => n,
                Err((ino, code)) => return fail(ino, code),
            };
            if sync && n > 0 {
                if let Err(code) = view.flush_sync_write(dst, fh_out, cancel.clone()) {
                    return fail(dst, code);
                }
            }
            Ok(n)
        };
        let deferral = if sync {
            self.fsync_deferral(OpKind::CopyFileRange)
        } else {
            self.copy_deferral()
        };
        let Some(view) = deferral else {
            r.done(run(self));
            return;
        };
        let admitted = _admitted.defer();
        let (watch, inflight) = (_w, _inflight);
        // Plan 30 §M14 phase 2: the copy's writes carry the caller's
        // fencing token on the pool thread too.
        let caller = crate::locks::scope_caller();
        let pool = if sync {
            crate::fsync_wait::pool()
        } else {
            crate::completion::CompletionPool::global()
        };
        pool.submit(move || {
            watch.adopt();
            let _lock_tag = caller.as_ref().map(|c| c.enter());
            r.done(run(&view));
            drop(inflight);
            view.admission.leave_deferred(admitted);
            drop(watch);
        });
    }

    fn getxattr<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R) {
        let _w = self.watch.enter("getxattr", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        let name = match self.policies.xattrs.check_name(name, cx.caller) {
            Ok(name) => name,
            Err(error) => return r.done(self.ino_err(ino, error)),
        };
        if !View::is_synthetic(ino) {
            self.session_wait(&[ReadKey::Ino(ino)]);
        }
        let value = if self.policies.xattrs.is_virtual(&name) {
            let aggregate = if View::is_synthetic(ino) {
                self.synthetic_recursive_size(ino)
            } else {
                self.meta.recursive_size(ino).map_err(|error| error.code())
            };
            match aggregate {
                Ok((rsize, rcount)) => {
                    if name == RSIZE_XATTR {
                        rsize.to_string().into_bytes()
                    } else {
                        debug_assert_eq!(name, RCOUNT_XATTR);
                        rcount.to_string().into_bytes()
                    }
                }
                Err(error) => return r.done(self.ino_err(ino, error)),
            }
        } else {
            let result = if View::is_synthetic(ino) {
                self.synthetic_xattrs(ino)
                    .map(|attrs| attrs.into_iter().find(|(key, _)| key == &name).map(|x| x.1))
            } else if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
                self.meta
                    .scratch_get_xattr(ino, &name)
                    .map_err(|error| error.code())
            } else {
                self.meta
                    .get_xattr(ino, &name)
                    .map_err(|error| error.code())
            };
            match result {
                Ok(Some(value)) => value,
                Ok(None) => return r.done(self.ino_err(ino, Code::NoData)),
                Err(error) => return r.done(self.ino_err(ino, error)),
            }
        };
        r.done(Ok(value));
    }

    fn setxattr<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        name: &XattrName,
        value: &[u8],
        flags: SetXattrFlags,
        r: R,
    ) {
        let _w = self.watch.enter("setxattr", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        let name = match self.policies.xattrs.check_name(name, cx.caller) {
            Ok(name) => name,
            Err(error) => return r.done(self.ino_err(ino, error)),
        };
        if self.policies.xattrs.is_virtual(&name) {
            r.done(self.ino_err(ino, Code::Perm));
            return;
        }
        if View::is_synthetic(ino) {
            r.done(self.ino_err(ino, Code::ReadOnly));
            return;
        }
        if value.len() > XATTR_VALUE_MAX {
            r.done(self.ino_err(ino, Code::TooBig));
            return;
        }
        let mode = match flags.mode() {
            Ok(SetXattrMode::Set) => constellation_meta::SetXattrMode::Set,
            Ok(SetXattrMode::Create) => constellation_meta::SetXattrMode::Create,
            Ok(SetXattrMode::Replace) => constellation_meta::SetXattrMode::Replace,
            Err(error) => return r.done(self.ino_err(ino, error)),
        };
        let wire_mode = match mode {
            constellation_meta::SetXattrMode::Create => 1,
            constellation_meta::SetXattrMode::Replace => 2,
            constellation_meta::SetXattrMode::Set => 0,
        };
        // Prune-policy validation gate (plan 22, Step 2). Reject an
        // unparseable or misplaced policy at the frontend boundary with
        // EINVAL, so it never reaches the log; stash the reason where the
        // CLI/status can surface it, since errno carries no message.
        if name == constellation_meta::prune::PRUNE_XATTR {
            // Only directories may carry a policy.
            match self.meta.getattr(ino) {
                Ok(Some(attr)) if attr.kind == InodeKind::Dir => {}
                Ok(Some(_)) => return r.done(self.ino_err(ino, Code::Invalid)),
                Ok(None) => return r.done(self.ino_err(ino, Code::NotFound)),
                Err(e) => return r.done(self.ino_err(ino, e.code())),
            }
            let expr = String::from_utf8_lossy(value);
            match constellation_meta::prune::Policy::parse(&expr) {
                Ok(policy) => {
                    // Set-time courtesy: reject an atime-driven policy on a
                    // mount with atime off. This consults the mount (not
                    // the policy bytes), so it never affects the pruner's
                    // pure re-parse — only whether the write is accepted.
                    if policy.needs_atime() && self.atime.mode() == crate::atime::AtimeMode::Off {
                        self.prune_stats.record_parse_error(
                            &expr,
                            0,
                            "policy needs atime; mount with --atime relatime",
                        );
                        return r.done(self.ino_err(ino, Code::Invalid));
                    }
                }
                Err(e) => {
                    self.prune_stats.record_parse_error(&expr, e.offset, &e.msg);
                    return r.done(self.ino_err(ino, Code::Invalid));
                }
            }
        }
        // Snapshot-policy gate (plan 32 Step 3.1), the same posture as
        // prune's: refuse a misplaced or unparseable policy here, so it
        // never reaches the log. Replay never validates; the scheduler
        // re-parses and skips what it cannot read.
        if name == constellation_meta::snapsched::SNAPSHOT_POLICY_XATTR {
            if let Err(refusal) = self.snapshot_policy_gate(ino, value) {
                if let Some((offset, msg)) = &refusal.reason {
                    let expr = String::from_utf8_lossy(value);
                    self.snapsched_stats.record_parse_error(&expr, *offset, msg);
                }
                return r.done(self.ino_err(ino, refusal.code));
            }
        }
        // ... and the converse: no directory at or above a policy root may
        // become a scratch root, whose content is node-private and
        // meaningless to snapshot.
        if name == constellation_meta::SCRATCH_XATTR && value == b"1" {
            if let Err(refusal) = self.scratch_policy_gate(ino) {
                if let Some((offset, msg)) = &refusal.reason {
                    let marking = format!("{}=1", constellation_meta::SCRATCH_XATTR);
                    self.snapsched_stats
                        .record_parse_error(&marking, *offset, msg);
                }
                return r.done(self.ino_err(ino, refusal.code));
            }
        }
        // Scratch files are node-private (scratch-directories.md) and
        // live outside the shared inode/xattr tables until `Publish`, so
        // their xattrs are staged locally here rather than mutated
        // through the lease-gated shared path.
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_set_xattr(ino, &name, value, mode) {
                Ok(()) => r.done(Ok(())),
                Err(error) => r.done(self.ino_err(ino, error.code())),
            };
        }
        let op = constellation_meta::MutateOp::SetXattr {
            ino,
            name: name.clone(),
            value: value.to_vec(),
            mode: wire_mode,
        };
        match self.mutate_op(ino, op) {
            Ok(()) => {
                self.nudge_sync();
                r.done(Ok(()))
            }
            // An unlinked file a descriptor or the kernel still references
            // (`setxattr` of `/proc/self/fd/N`): nothing to publish, the
            // orphan's own xattrs change, as `setattr`'s do.
            Err(Code::NotFound) if self.unlinked(ino) => {
                match self.meta.orphan_set_xattr(ino, &name, value, mode) {
                    Ok(true) => r.done(Ok(())),
                    Ok(false) => r.done(self.ino_err(ino, Code::NotFound)),
                    Err(error) => r.done(self.ino_err(ino, error.code())),
                }
            }
            Err(error) => r.done(self.ino_err(ino, error)),
        }
    }

    fn listxattr<R: Responder<Vec<XattrNameBuf>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let _w = self.watch.enter("listxattr", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        if !View::is_synthetic(ino) {
            self.session_wait(&[ReadKey::Ino(ino)]);
        }
        let names = if View::is_synthetic(ino) {
            self.synthetic_xattrs(ino)
                .map(|attrs| attrs.into_iter().map(|(name, _)| name).collect())
        } else if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            self.meta
                .scratch_list_xattrs(ino)
                .map_err(|error| error.code())
        } else {
            self.meta.list_xattrs(ino).map_err(|error| error.code())
        };
        match names {
            Ok(mut names) => {
                names.retain(|n| !is_internal_xattr(n));
                r.done(Ok(self.policies.xattrs.listing(names)))
            }
            Err(error) => r.done(self.ino_err(ino, error)),
        }
    }

    fn removexattr<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R) {
        let _w = self.watch.enter("removexattr", ino);
        admit!(self, cx, r, _admitted);
        let ino = enter!(self, ino, r);
        let name = match self.policies.xattrs.check_name(name, cx.caller) {
            Ok(name) => name,
            Err(error) => return r.done(self.ino_err(ino, error)),
        };
        if self.policies.xattrs.is_virtual(&name) {
            r.done(self.ino_err(ino, Code::Perm));
            return;
        }
        if View::is_synthetic(ino) {
            r.done(self.ino_err(ino, Code::ReadOnly));
            return;
        }
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_remove_xattr(ino, &name) {
                Ok(()) => r.done(Ok(())),
                Err(error) => r.done(self.ino_err(ino, error.code())),
            };
        }
        let op = constellation_meta::MutateOp::RemoveXattr {
            ino,
            name: name.clone(),
        };
        match self.mutate_op(ino, op) {
            Ok(()) => {
                self.nudge_sync();
                r.done(Ok(()))
            }
            // As `setxattr`'s: an unlinked, still referenced file's own.
            Err(Code::NotFound) if self.unlinked(ino) => {
                match self.meta.orphan_remove_xattr(ino, &name) {
                    Ok(true) => r.done(Ok(())),
                    Ok(false) => r.done(self.ino_err(ino, Code::NotFound)),
                    Err(error) => r.done(self.ino_err(ino, error.code())),
                }
            }
            Err(error) => r.done(self.ino_err(ino, error)),
        }
    }

    /// Plan 30 §M14: `F_GETLK` under `--locks cluster` (see
    /// `crate::locks`). Under `--locks local` the kernel never asks.
    fn lock_test<R: Responder<LockStatus>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        r: R,
    ) {
        let _w = self.watch.enter("getlk", ino);
        let ino = enter!(self, ino, r);
        let Some(locks) = self.vfs_cluster_locks() else {
            r.done(err(Code::NotImplemented));
            return;
        };
        if View::is_synthetic(ino) {
            // Frozen snapshot files take no cluster locks (see
            // `lock_acquire`).
            r.done(Ok(LockStatus::Unlocked));
            return;
        }
        let write = lock.kind == LockKind::Write;
        let (owner, start, end) = (lock.owner.0, lock.range.start, lock.range.end);
        // A test through `fh` shows its description open.
        self.note_handle_open(fh);
        // A lock of a description whose last close was just answered is
        // not reported while its `RELEASE` is on its way (`lock_handles`):
        // another lock in the way is named first, as Linux, which dropped
        // that one already, names it, and with none the test waits for
        // the `RELEASE`.
        let found = self.past_closing_locks(
            locks,
            ino,
            owner,
            (start, end),
            write,
            std::time::Instant::now() + crate::locks::close_wait(),
            |_| {
                let going = self.closing_lock_owners(ino);
                locks.test(ino, owner, start, end, write, |o| going.contains(&o))
            },
            Option::is_some,
        );
        let status = match found {
            Some((start, end, write, pid)) => LockStatus::Locked {
                range: LockRange { start, end },
                kind: if write {
                    LockKind::Write
                } else {
                    LockKind::Read
                },
                pid,
            },
            None => LockStatus::Unlocked,
        };
        r.done(Ok(status));
    }

    /// Plan 30 §M14: `F_SETLK`/`F_SETLKW`/`flock` under `--locks
    /// cluster`. Non-blocking requests are answered on this thread; a
    /// blocking one (`sleep`) waits on a thread of its own
    /// (`ClusterLocks::lock`: its `lock-wait` thread, never the tokio
    /// blocking pool — see there), so a contended lock never pins a
    /// frontend worker, and completes the responder from there — the
    /// deferred path of plan 31 §6.3, for a frontend that can answer from
    /// another thread (`FrontendCaps::deferrable`; one that cannot waits
    /// here). A blocking wait honours `cx.cancel`: the FUSE adapter
    /// cancels it on any signal (interruptible, as POSIX's `F_SETLKW`),
    /// and the wait then answers `Intr` (`EINTR`) holding nothing; and
    /// `cx.deadline`, past which it answers `TimedOut` the same way —
    /// except a wait parked at another node's sequencer, which stays
    /// uninterruptible by either (`crate::locks` module doc).
    fn lock_acquire<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        sleep: bool,
        mut r: R,
    ) {
        let started = std::time::Instant::now();
        let mut watch = if sleep {
            self.watch.enter_blocking("setlk", ino)
        } else {
            self.watch.enter("setlk", ino)
        };
        let ino = enter!(self, ino, r);
        let Some(locks) = self.vfs_cluster_locks() else {
            r.done(err(Code::NotImplemented));
            return;
        };
        if View::is_synthetic(ino) {
            // A frozen snapshot file (inside a live view's `.snapshots`)
            // has no sequencer to lease a grant from.
            r.done(err(Code::NoLock));
            return;
        }
        // Data written under an earlier grant that ended without its
        // flush does not ride along under the new one.
        self.lock_discard_tainted(ino);
        // The request's bound, for every wait it takes on the way.
        let deadline = std::time::Instant::now() + crate::locks::close_wait();
        self.note_lock_handle(fh, ino, lock.owner.0, locks, deadline);
        let local = constellation_meta::locks::LocalLock {
            owner: lock.owner.0,
            pid: lock.pid,
            pid_start: 0,
            write: lock.kind == LockKind::Write,
            start: lock.range.start,
            end: lock.range.end,
        };
        if !sleep {
            // Answered here, as before. A refusal by locks of descriptions
            // whose last close was just answered waits for their
            // `RELEASE`, which the kernel sends without waiting for it
            // (`lock_handles`' module doc), and asks again.
            if !self.caps.deferrable.contains(OpKind::LockAcquire) {
                // A frontend that cannot answer from another thread waits
                // here for an owner that cannot grant yet (plan 43 F-b),
                // as it does for a blocking lock.
                let result = self.past_closing_locks(
                    locks,
                    ino,
                    local.owner,
                    (local.start, local.end),
                    local.write,
                    deadline,
                    |deadline| locks.try_lock_by(ino, local, deadline, cx.cancel),
                    |r| *r == Err(Code::Again),
                );
                r.done(result.map_err(VfsError::from));
                return;
            }
            // A request whose answer from the core is slow asks its frontend
            // for what a signal can end it with (plan 43 C7-F F-b N2): one
            // answered at once registers nothing.
            let step = {
                let late = crate::locks::LateToken::new(|| match r.begin_wait() {
                    constellation_vfs::WaitStart::Held(own) => own,
                    constellation_vfs::WaitStart::Refused => None,
                });
                self.past_closing_locks(
                    locks,
                    ino,
                    local.owner,
                    (local.start, local.end),
                    local.write,
                    deadline,
                    |deadline| locks.try_lock_step(ino, local, deadline, cx.cancel, Some(&late)),
                    // (A request to wait is no refusal.)
                    |step| *step == crate::locks::NbStep::Done(Err(Code::Again)),
                )
            };
            match step {
                crate::locks::NbStep::Done(result) => {
                    r.done(result.map_err(VfsError::from));
                }
                crate::locks::NbStep::Wait(first_wait) => {
                    // Nothing conflicts but the owner may not grant yet
                    // (a grace after a takeover or a restart, the tenure's
                    // floor...): the request waits on a `lock-wait` thread,
                    // like a blocking one, so it holds neither this worker
                    // nor, over a ring, an offload thread that every other
                    // request offloaded to it needs. It may
                    // wait for tens of seconds by design: not a stall.
                    //
                    // The frontend is told first: it registers the wait
                    // for the caller's signal, and reserves what the wait
                    // holds (over a ring, one of the queue's `depth - 1`
                    // entries that lock waits may hold, so that one stays
                    // free for every other request of that CPU). When it
                    // cannot, the request is answered now, as it was before
                    // it could wait.
                    let own = match r.begin_wait() {
                        constellation_vfs::WaitStart::Held(own) => own,
                        constellation_vfs::WaitStart::Refused => {
                            locks
                                .meta
                                .locks()
                                .with_stats(|s| s.grace_wait_refused_ring += 1);
                            r.done(err(Code::Again));
                            return;
                        }
                    };
                    drop(std::mem::replace(
                        &mut watch,
                        self.watch.enter_blocking("setlk", ino),
                    ));
                    let cancel = own.or_else(|| cx.cancel.cloned());
                    locks.lock_nb_deferred(
                        ino,
                        local,
                        first_wait,
                        started,
                        cancel,
                        move |result: Result<(), Code>| {
                            r.done(result.map_err(VfsError::from));
                            drop(watch);
                        },
                    );
                }
            }
            return;
        }
        if !self.caps.deferrable.contains(OpKind::LockAcquire) {
            r.done(
                locks
                    .lock_here(ino, local, sleep, cx.cancel, cx.deadline)
                    .map_err(VfsError::from),
            );
            return;
        }
        // The engine answers through this callback, from this thread or
        // from its `lock-wait` thread; the watchdog registration ends
        // with the answer, wherever it is given.
        let cancel = cx.cancel.cloned();
        locks.lock(
            ino,
            local,
            sleep,
            cancel,
            cx.deadline,
            move |result: Result<(), Code>| {
                r.done(result.map_err(VfsError::from));
                drop(watch);
            },
        );
    }

    /// `F_UNLCK`: drop `owner`'s locks in `range` (and, with none left on
    /// the file, release a recalled grant).
    fn lock_release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        range: LockRange,
        r: R,
    ) {
        let _w = self.watch.enter("setlk", ino);
        let ino = enter!(self, ino, r);
        let Some(locks) = self.vfs_cluster_locks() else {
            r.done(err(Code::NotImplemented));
            return;
        };
        // An unlock through `fh` shows its description open.
        self.note_handle_open(fh);
        locks.unlock(ino, owner.0, range.start, range.end);
        r.done(Ok(()));
    }

    fn sync_view<R: Responder<()>>(&self, _cx: &OpCtx<'_>, r: R) {
        let _w = self.watch.enter("sync_view", WatchKey::View);
        // Every write session published (write-through: its chunks up),
        // then the journal as durable as `fsync` makes it.
        for ino in self.writes.pending_inos() {
            if let Err(error) = self.flush_inode(ino, true) {
                r.done(err(error));
                return;
            }
        }
        r.done(self.sync_barrier(self.view_root).map_err(VfsError::from));
    }
}

/// A child's `readdir` cookie: a 64-bit FNV-1a of its name, kept clear of
/// the `.`/`..` cookies (1, 2) and below `i64::MAX` (fuser's offsets are
/// signed). Two names of one directory sharing a cookie (odds ~n²/2⁶³)
/// are both listed in one page but a listing that stops between them
/// resumes after both.
pub(super) fn dir_cookie(name: &[u8]) -> u64 {
    let hash = name.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (hash >> 2) + 3
}

impl View {
    /// The error of an op on inode `ino` itself: `NotFound` answered
    /// `Stale` once the inode is gone from the replica ([`View::gone`]).
    fn ino_err<T>(&self, ino: Ino, code: Code) -> Result<T, VfsError> {
        Err(self.gone(ino, code).into())
    }

    /// The cluster locks the `lock_*` ops serve: none unless this view's
    /// frontend declared `cluster_locks` (plan 31 §6.2: "only called if
    /// caps.cluster_locks") — a frontend that keeps locks node-local is
    /// answered `NotImplemented`, as the contract says, even on an engine
    /// that forwards them for its other views.
    fn vfs_cluster_locks(&self) -> Option<&Arc<crate::locks::ClusterLocks>> {
        self.cluster_locks().filter(|_| self.caps.cluster_locks)
    }

    /// POSIX `ENOTDIR`: a name looked up, removed or moved beneath
    /// something that is not a directory. The replica answers "no such
    /// entry" for a non-directory parent; a kernel frontend never asks
    /// (its path walk stops first), anything else must see `NotDir`.
    fn beneath_non_dir(&self, code: Code, parents: &[Ino]) -> Code {
        if code != Code::NotFound {
            return code;
        }
        let non_dir = parents.iter().any(|parent| {
            matches!(self.meta.getattr(*parent), Ok(Some(attr)) if attr.kind != InodeKind::Dir)
        });
        if non_dir {
            Code::NotDir
        } else {
            code
        }
    }

    /// Plan 32 Step 3.1: may `value` become `ino`'s snapshot policy? Only
    /// a shared directory outside every scratch tree may carry one (a
    /// snapshot of node-private content is meaningless), and only a
    /// parseable expression. Every policy refusal is `EINVAL` with its
    /// reason returned (`(offset, message)`; the offset is 0 unless the
    /// expression failed to parse): `setxattr` stashes it in
    /// `SnapSchedStats::last_parse_error`, since errno carries no message,
    /// and `snapshot.policy.set` pre-checks with this very function to put
    /// it in its own answer, never reading the shared slot back.
    ///
    /// The value is stored verbatim, as plan 22 stores a prune policy: a
    /// `setxattr` reads back exactly what it wrote. The canonical form is
    /// what `snapshot.policy.set` writes, and what every reader displays.
    pub(crate) fn snapshot_policy_gate(&self, ino: Ino, value: &[u8]) -> Result<(), GateRefusal> {
        let refuse = |msg: &str| GateRefusal {
            code: Code::Invalid,
            reason: Some((0, msg.to_string())),
        };
        let bare = |code: Code| GateRefusal { code, reason: None };
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return Err(refuse(
                "a snapshot policy cannot be set inside a scratch directory",
            ));
        }
        match self.meta.getattr(ino) {
            Ok(Some(attr)) if attr.kind == InodeKind::Dir => {}
            Ok(Some(_)) => return Err(refuse("a snapshot policy belongs to a directory")),
            Ok(None) => return Err(bare(Code::NotFound)),
            Err(e) => return Err(bare(e.code())),
        }
        // The directory itself, or an ancestor, marked scratch. Entries
        // below a scratch root are node-private (caught above), but a
        // shared directory can predate its parent's marking.
        let mut cursor = Some(ino);
        while let Some(cur) = cursor {
            if self.meta.is_scratch_dir(cur).map_err(|e| bare(e.code()))? {
                return Err(refuse(
                    "a snapshot policy cannot be set on or inside a scratch directory",
                ));
            }
            cursor = self.meta.parent_of(cur).map_err(|e| bare(e.code()))?;
        }
        let expr = String::from_utf8_lossy(value);
        match constellation_meta::snapsched::SnapPolicy::parse(&expr) {
            Ok(_) => Ok(()),
            Err(e) => Err(GateRefusal {
                code: Code::Invalid,
                reason: Some((e.offset, e.msg)),
            }),
        }
    }

    /// The converse of [`View::snapshot_policy_gate`]: may `ino` be marked
    /// scratch? Not when it is a policy root or an ancestor of one — the
    /// policy would then sit inside a scratch tree. The roots come from
    /// the `xattr_by_name` index (a handful), each walked up to the root.
    ///
    /// A refusal is `EINVAL` with a reason that starts "scratch refused"
    /// and names the policy in the way. `setxattr` records it in
    /// `last_parse_error` under the expression `user.constellation.scratch=1`
    /// (plan 22 likewise records its non-parse atime refusal in the prune
    /// slot), so a status reader sees it was the scratch marking, not a
    /// policy, that failed.
    fn scratch_policy_gate(&self, ino: Ino) -> Result<(), GateRefusal> {
        let bare = |e: constellation_meta::MetaError| GateRefusal {
            code: e.code(),
            reason: None,
        };
        for (root, expr) in self.meta.snapshot_policy_roots().map_err(bare)? {
            let mut cursor = Some(root);
            while let Some(cur) = cursor {
                if cur == ino {
                    let msg = if root == ino {
                        format!("scratch refused: this directory carries snapshot policy `{expr}`")
                    } else {
                        format!(
                            "scratch refused: directory inode {root} below this one carries \
                             snapshot policy `{expr}`"
                        )
                    };
                    return Err(GateRefusal {
                        code: Code::Invalid,
                        reason: Some((0, msg)),
                    });
                }
                cursor = self.meta.parent_of(cur).map_err(bare)?;
            }
        }
        Ok(())
    }
}

/// Why [`View::snapshot_policy_gate`] refused: the errno, and for a policy
/// refusal (`EINVAL`) the `(offset, message)` reason.
pub(crate) struct GateRefusal {
    pub(crate) code: Code,
    pub(crate) reason: Option<(usize, String)>,
}
