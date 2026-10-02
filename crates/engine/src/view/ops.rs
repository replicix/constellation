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
    DirSink, Fh, LockKind, LockOwner, LockRange, LockSpec, LockStatus, Name, OpCtx, OpKind,
    OpenOwner, Opened, ReadData, RenameFlags, Responder, SetAttr, SetXattrFlags, SetXattrMode,
    StatFs, TimeSet, Vfs, VfsError, ViewIdentity, WatchKey, WriteData, XattrName, XattrNameBuf,
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
    ($self:expr, $cx:expr, $r:ident) => {
        match $self.admission.admit($cx) {
            Ok(admitted) => admitted,
            Err(code) => {
                $r.done(Err(code.into()));
                return;
            }
        }
    };
}

/// Write gate (DESIGN.md §5): a mutating op may only proceed while this
/// node holds the partition lease for `ino`'s partition. Acquisition is
/// lazy — the first mutation after a mount or an idle release blocks
/// here for one CAS.
macro_rules! gate {
    ($self:expr, $ino:expr, $r:ident) => {
        if View::is_synthetic($ino) {
            $r.done(Err(Code::ReadOnly.into()));
            return;
        }
        if let Err(e) = $self.require_lease_for($ino) {
            $r.done(Err(e.into()));
            return;
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
    fn frontend_negotiated(&self, caps: &FrontendCaps) {
        self.set_passthrough_on(caps.passthrough);
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

    fn lookup<R: Responder<Entry>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let _w = self.watch.enter("lookup", parent);
        let _admitted = admit!(self, cx, r);
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
                Ok(Some(ino)) => self.meta.getattr(ino).and_then(|attr| match attr {
                    Some(attr) => Ok(Some(attr)),
                    None => self.meta.scratch_getattr(ino),
                }),
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
                r.done(Ok(self.entry_out(&attr)));
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
            Ok(Some(mut attr)) => {
                // Same overlay and the same ordering argument as
                // `getattr`: a name looked up while its inode has pending
                // writes must report the pending size, or the entry reply
                // caches a stale one. The ino is only known after the
                // lookup, so re-read the row under the shard lock.
                let writes = self.writes.lock(attr.ino);
                if !writes.contains_key(&attr.ino) {
                    if let Ok(Some(fresh)) = self.meta.getattr(attr.ino) {
                        attr = fresh;
                    }
                }
                if let Some(len) = self.writes.pending_len(&writes, attr.ino) {
                    attr.size = len;
                }
                drop(writes);
                r.done(Ok(self.entry_out(&attr)))
            }
            Ok(None) => r.done(err(self.beneath_non_dir(Code::NotFound, &[parent]))),
            Err(e) => r.done(err(e.code())),
        }
    }

    fn getattr<R: Responder<Attr>>(&self, cx: &OpCtx<'_>, ino: Ino, _fh: Option<Fh>, r: R) {
        let _w = self.watch.enter("getattr", ino);
        let _admitted = admit!(self, cx, r);
        let requested_ino = ino;
        let ino = enter!(self, ino, r);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                r.done(err(Code::Stale));
            } else {
                let attr = self.visible_attr(self.synthetic_attr(ino, &node));
                r.done(Ok(self.attr_out(&attr)));
            }
            return;
        }
        // The write shard is taken *before* the committed row is read and
        // held until the overlay below is applied. `flush_inode` commits
        // the manifest and retires the write state under this same lock,
        // so reading the row first and checking the shard afterwards
        // could see the pre-flush size *and* no write state -- a reply
        // the kernel then caches for TTL. With host-sized concurrent
        // FUSE dispatch that interleaving is routine: the harness's
        // single-client `baseline` read sizes of 0 back for just-closed
        // files about half the time.
        //
        // Plan 30 §M6: the session wait comes first — never block while
        // holding a write shard.
        self.session_wait(&[ReadKey::Ino(ino)]);
        let writes = self.writes.lock(ino);
        constellation_vfs::watch::stage("meta read");
        let attr = self.meta.getattr(ino).and_then(|attr| {
            if attr.is_some() {
                Ok(attr)
            } else {
                self.meta.scratch_getattr(ino)
            }
        });
        match attr {
            Ok(Some(mut attr)) => {
                // Pending writes shadow the committed size (a session
                // detached by an in-flight flush or read included).
                if let Some(len) = self.writes.pending_len(&writes, ino) {
                    attr.size = len;
                }
                drop(writes);
                let attr = if requested_ino == constellation_fs_core::types::ROOT_INO {
                    self.visible_attr(attr)
                } else {
                    attr
                };
                r.done(Ok(self.attr_out(&attr)))
            }
            Ok(None) => r.done(err(Code::NotFound)),
            Err(e) => r.done(err(e.code())),
        }
    }

    fn setattr<R: Responder<Attr>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Option<Fh>,
        set: &SetAttr,
        r: R,
    ) {
        let _w = self.watch.enter("setattr", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        let atime_ns = set.atime.map(time_ns);
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
                    r.done(err(Code::IsDir));
                    return;
                }
                Some(InodeKind::File) | None => {}
                Some(_) => {
                    r.done(err(Code::Invalid));
                    return;
                }
            }
            // Plan 30 §M14: a truncation is a write (fenced under a
            // lapsed lock grant).
            if self.lock_fenced(ino) {
                r.done(err(Code::Io));
                return;
            }
            self.lock_discard_tainted(ino);
            if let Err(error) = self.truncate(ino, new_size) {
                r.done(err(error));
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
                    let writes = self.writes.lock(ino);
                    if let Some(len) = self.writes.pending_len(&writes, ino) {
                        attr.size = len;
                    }
                    attr
                }),
            Err(error) => Err(error),
            Ok(()) => self
                .meta
                .getattr(ino)
                .map_err(|error| error.code())
                .and_then(|attr| attr.ok_or(Code::NotFound)),
        };
        match result {
            Ok(attr) => r.done(Ok(self.attr_out(&attr))),
            Err(error) => r.done(err(error)),
        }
    }

    fn readlink<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let _w = self.watch.enter("readlink", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                r.done(err(Code::Stale));
            } else if let SyntheticNode::Frozen {
                kind: InodeKind::Symlink,
                target: Some(target),
                ..
            } = node
            {
                r.done(Ok(target.into_bytes()));
            } else {
                r.done(err(Code::Invalid));
            }
            return;
        }
        self.session_wait(&[ReadKey::Ino(ino)]);
        match self.meta.readlink(ino) {
            Ok(Some(target)) => r.done(Ok(target.into_bytes())),
            Ok(None) => r.done(err(Code::Invalid)),
            Err(e) => r.done(err(e.code())),
        }
    }

    fn mknod<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        rdev: constellation_types::Rdev,
        r: R,
    ) {
        let _w = self.watch.enter("mknod", parent);
        let _admitted = admit!(self, cx, r);
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
        let (uid, gid) = self.policies.identity.owner(cx.caller);
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
        r: R,
    ) {
        let _w = self.watch.enter("mkdir", parent);
        let _admitted = admit!(self, cx, r);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return r.done(err(e.code())),
        };
        let (uid, gid) = self.policies.identity.owner(cx.caller);
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
        r: R,
    ) {
        let _w = self.watch.enter("symlink", parent);
        let _admitted = admit!(self, cx, r);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let target = String::from_utf8_lossy(target);
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return r.done(err(e.code())),
        };
        let (uid, gid) = self.policies.identity.owner(cx.caller);
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
        r: R,
    ) {
        let _w = self.watch.enter("link", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        let new_parent = enter!(self, new_parent, r);
        let _inflight = self.inflight.enter(&[ino, new_parent]);
        let name = checked_name!(self, new_name, r);
        // §6.12 `confine_links`: a second name only within one of the
        // inode's link domains (the view module doc's "Link domains").
        if self.confine_links && !View::is_synthetic(ino) && !View::is_synthetic(new_parent) {
            if let Err(code) = self.link_within_domain(ino, new_parent) {
                r.done(err(code));
                return;
            }
        }
        let op = constellation_meta::MutateOp::Link {
            ino,
            parent: new_parent,
            name: name.into_owned(),
        };
        match self.mutate_op(new_parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => r.done(Ok(self.entry_out(&attr))),
                Ok(None) => r.done(err(Code::NotFound)),
                Err(e) => r.done(err(e.code())),
            },
            Err(e) => r.done(err(e)),
        }
    }

    fn unlink<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let _w = self.watch.enter("unlink", parent);
        let _admitted = admit!(self, cx, r);
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
            match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr))
                    if attr.kind == InodeKind::Dir
                        && !self
                            .meta
                            .scratch_readdir(attr.ino)
                            .unwrap_or_default()
                            .is_empty() =>
                {
                    Err(Code::NotEmpty)
                }
                Ok(Some(_)) => self
                    .meta
                    .scratch_unlink(parent, &name)
                    .map_err(|e| e.code()),
                Ok(None) => Err(Code::NotFound),
                Err(e) => Err(e.code()),
            }
        } else {
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
                // No open handle in any view of this node: reap now.
                if let Ok(Some(attr)) = target {
                    self.reap_after_unlink(attr.ino);
                }
                r.done(Ok(()))
            }
            Err(e) => r.done(err(self.beneath_non_dir(e, &[parent]))),
        }
    }

    fn rmdir<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        let _w = self.watch.enter("rmdir", parent);
        let _admitted = admit!(self, cx, r);
        let parent = enter!(self, parent, r);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(self, name, r);
        let result = if (self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some())
            && self
                .meta
                .scratch_lookup(parent, &name)
                .ok()
                .flatten()
                .is_some()
        {
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
            self.mutate_op(
                parent,
                constellation_meta::MutateOp::Rmdir {
                    parent,
                    name: name.into_owned(),
                },
            )
        };
        match result {
            Ok(()) => r.done(Ok(())),
            Err(e) => r.done(err(self.beneath_non_dir(e, &[parent]))),
        }
    }

    fn rename<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        new_parent: Ino,
        new_name: &Name,
        flags: RenameFlags,
        r: R,
    ) {
        let _w = self.watch.enter("rename", parent);
        let _admitted = admit!(self, cx, r);
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
            return match self.meta.scratch_rename(parent, &name, newparent, &newname) {
                Ok(()) => r.done(Ok(())),
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
                Ok(()) => match self.meta.scratch_unlink(parent, &name) {
                    Ok(()) => r.done(Ok(())),
                    Err(error) => r.done(err(error.code())),
                },
                Err(error) => r.done(err(error)),
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
            Ok(()) => r.done(Ok(())),
            Err(e) => r.done(err(self.beneath_non_dir(e, &[parent, newparent]))),
        }
    }

    fn open<R: Responder<Opened>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        flags: OpenFlags,
        _owner: OpenOwner,
        r: R,
    ) {
        let _w = self.watch.enter("open", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                r.done(err(Code::Stale));
            } else if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC) {
                // A snapshot mirror is frozen: `EROFS`, as a read-only
                // mount answers an open for writing.
                r.done(err(Code::ReadOnly));
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
                let fh = self.open_handle(ino);
                let backing = self.frozen_passthrough_backing(ino, flags, &node);
                r.done(Ok(Opened { fh, backing }));
            } else {
                r.done(err(Code::IsDir));
            }
            return;
        }
        // Plan 30 §M8: under `--cto strict`, the close-to-open point.
        self.strict_read(ino, false, None, &[ReadKey::Ino(ino)]);
        let attr = self.meta.getattr(ino).and_then(|attr| {
            if attr.is_some() {
                Ok(attr)
            } else {
                self.meta.scratch_getattr(ino)
            }
        });
        match attr {
            Ok(Some(attr)) => {
                *self.opens.lock().unwrap().entry(ino).or_insert(0) += 1;
                self.note_writer_open(ino, flags);
                // Plan 38 §3(c): a read-only open of a one-chunk file
                // whose chunk is cached and verified is answered with the
                // chunk file itself, and the pin that keeps it where the
                // handle expects it is dropped in `release`.
                let backing = self.passthrough_backing(ino, flags, &attr);
                r.done(Ok(Opened {
                    fh: self.open_handle(ino),
                    backing,
                }))
            }
            Ok(None) => r.done(err(Code::NotFound)),
            Err(e) => r.done(err(e.code())),
        }
    }

    fn create<R: Responder<(Entry, Opened)>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        flags: OpenFlags,
        _owner: OpenOwner,
        r: R,
    ) {
        let _w = self.watch.enter("create", parent);
        let _admitted = admit!(self, cx, r);
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
            Ok((attr, _created)) => {
                *self.opens.lock().unwrap().entry(attr.ino).or_insert(0) += 1;
                self.note_writer_open(attr.ino, flags);
                r.done(Ok((
                    self.entry_out(&attr),
                    Opened::new(self.open_handle(attr.ino)),
                )))
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
        r: R,
    ) {
        let _w = self.watch.enter("read", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        // §6.12: a handle addresses only the inode it was opened on (the
        // view hands out one per open and knows its inode); anything else
        // was never given out.
        if self.handle_ino(fh) != Some(ino) {
            r.done(err(Code::BadFd));
            return;
        }
        let _inflight = self.inflight.enter(&[ino]);
        if View::is_synthetic(ino) {
            match self.read_frozen(ino, off, len as u64) {
                Ok(data) => r.done(Ok(ReadData::from_vec(data))),
                Err(error) => r.done(err(error)),
            }
            return;
        }
        if self.lock_fenced(ino) {
            r.done(err(Code::Io));
            return;
        }
        // A cold read defers to the completion pool (`io`'s module doc).
        let Some(view) = self.read_deferral() else {
            match self.do_read(ino, off, len as u64) {
                Ok(data) => r.done(Ok(data)),
                Err(e) => r.done(err(e)),
            }
            return;
        };
        if let Some(result) = super::io::cold_probe::inline(|| self.do_read(ino, off, len as u64)) {
            match result {
                Ok(data) => r.done(Ok(data)),
                Err(e) => r.done(err(e)),
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
            let result = view.do_read(ino, off, len as u64);
            match result {
                Ok(data) => r.done(Ok(data)),
                Err(e) => r.done(err(e)),
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
        _fh: Fh,
        off: u64,
        data: WriteData<'_>,
        flags: OpenFlags,
        r: R,
    ) {
        let _w = self.watch.enter("write", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        if View::is_synthetic(ino) {
            r.done(err(Code::ReadOnly));
            return;
        }
        // `ViewQos::max_staging_bytes`: wait for this view's own flushes.
        if let Err(code) = self
            .admission
            .admit_staging(cx, data.as_slice().len() as u64)
        {
            r.done(err(code));
            return;
        }
        if self.lock_fenced(ino) {
            r.done(err(Code::Io));
            return;
        }
        self.lock_discard_tainted(ino);
        let n = match self.do_write(ino, off, data.as_slice()) {
            Ok(n) if flags.contains(OpenFlags::SYNC) => n,
            Ok(n) => return r.done(Ok(n)),
            Err(e) => return r.done(err(e)),
        };
        // `O_SYNC`/`O_DSYNC` (plan 39 §3.3): the publication waits like
        // an `fsync`, and so, like one, on the `fsync` pool — a database
        // writing its WAL this way during an outage must not occupy every
        // frontend worker (they deliver the interrupt that ends a killed
        // caller's wait).
        let cancel = cx.cancel.cloned();
        let Some(view) = self.fsync_deferral(OpKind::Write) else {
            r.done(
                self.flush_sync_write(ino, cancel)
                    .map(|()| n)
                    .map_err(VfsError::from),
            );
            return;
        };
        let admitted = _admitted.defer();
        let (watch, inflight) = (_w, _inflight);
        crate::fsync_wait::pool().submit(move || {
            watch.adopt();
            r.done(
                view.flush_sync_write(ino, cancel)
                    .map(|()| n)
                    .map_err(VfsError::from),
            );
            drop(inflight);
            view.admission.leave_deferred(admitted);
            drop(watch);
        });
    }

    fn flush<R: Responder<()>>(&self, _cx: &OpCtx<'_>, ino: Ino, fh: Fh, owner: LockOwner, r: R) {
        let _w = self.watch.enter("flush", ino);
        let ino = enter!(self, ino, r);
        // Plan 30 §M14: the fence first, while the closing owner's locks
        // are still there (dropping them would lift it): data written
        // under a lapsed grant is discarded, never published. Then the
        // close drops the process's POSIX locks on the file (any
        // descriptor's close, as POSIX says); the kernel also sends an
        // explicit unlock, which then finds nothing.
        let locks = self.cluster_locks().filter(|_| !View::is_synthetic(ino));
        let gate = self.lock_publish_gate(ino, fh);
        let idle = locks.map(|l| l.drop_owner(ino, owner.0));
        let result = gate.and_then(|owed| {
            self.flush_inode(ino, false)?;
            if owed {
                return Err(Code::Io);
            }
            Ok(())
        });
        // The release of a recalled grant flushes the file first; asking
        // for it only after this flush lets that flush find it done.
        if let (Some(l), Some(true)) = (locks, idle) {
            l.idle(ino);
        }
        match result {
            Ok(()) => r.done(Ok(())),
            Err(e) => r.done(err(e)),
        }
    }

    fn release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
        r: R,
    ) {
        // The description ends with this call however it answers (an
        // `enter!` refusal included): nothing addresses it any more.
        let _closed = self.handles.closing(fh);
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
        // drops its `flock` lock (whose owner is the open file).
        let locks = self.cluster_locks();
        let gate = self.lock_publish_gate(ino, fh);
        drop(_closed);
        let idle = match (locks, owner) {
            (Some(l), Some(owner)) => l.drop_owner(ino, owner.0),
            _ => false,
        };
        let flush_result = gate.and_then(|owed| {
            self.flush_inode(ino, flags.contains(OpenFlags::SYNC))?;
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
        // (`flush_inode`: an unlinked file publishes nothing).
        if last && self.unlinked(ino) {
            self.drop_writes(ino);
            if let Ok(Some(attr)) = self.meta.getattr(ino) {
                if attr.nlink == 0 {
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

    fn fsync<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, level: Durability, r: R) {
        let _w = self.watch.enter("fsync", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        // Plan 30 §M14: nothing written under a lapsed grant is made
        // durable (`lock_publish_gate`); a discard this description has
        // not reported yet is reported after the barrier.
        let owed = match self.lock_publish_gate(ino, fh) {
            Ok(owed) => owed,
            Err(e) => {
                r.done(err(e));
                return;
            }
        };
        let cancel = cx.cancel.cloned();
        let finish = move |view: &View| match view.fsync_durable(ino, level, cancel) {
            Ok(()) if owed => Err(Code::Io),
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
        crate::fsync_wait::pool().submit(move || {
            watch.adopt();
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
        let _admitted = admit!(self, cx, r);
        let visible_ino = ino;
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        if View::is_synthetic(ino) {
            let entries = match self.synthetic_entries(ino) {
                Ok(entries) => entries,
                Err(error) => return r.done(err(error)),
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
                        r.add(*child_ino, next, kind_out(*kind), name.as_bytes())
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
            Err(e) => return r.done(err(e.code())),
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
                child.name.as_bytes(),
            ) {
                break;
            }
        }
        r.done(Ok(()));
    }

    fn statfs<R: Responder<StatFs>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let _w = self.watch.enter("statfs", ino);
        let _admitted = admit!(self, cx, r);
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
        _fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
        r: R,
    ) {
        let _w = self.watch.enter("fallocate", ino);
        let _admitted = admit!(self, cx, r);
        // Plan 31 §6.2: absent from a frontend that does not declare it.
        if !self.caps.fallocate {
            r.done(err(Code::NotSupported));
            return;
        }
        let ino = enter!(self, ino, r);
        let _inflight = self.inflight.enter(&[ino]);
        gate!(self, ino, r);
        if self.lock_fenced(ino) {
            r.done(err(Code::Io));
            return;
        }
        if len == 0 {
            r.done(err(Code::Invalid));
            return;
        }
        match self.do_fallocate(ino, off, len, mode) {
            Ok(()) => r.done(Ok(())),
            Err(error) => r.done(err(error)),
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
        let _admitted = admit!(self, cx, r);
        // Plan 31 §6.2: `SEEK_DATA`/`SEEK_HOLE` are absent from a frontend
        // that does not declare them.
        if !self.caps.seek_hole {
            r.done(err(Code::NotSupported));
            return;
        }
        let ino = enter!(self, ino, r);
        match self.seek_sparse(ino, off, whence) {
            Ok(position) => r.done(Ok(position as u64)),
            Err(error) => r.done(err(error)),
        }
    }

    fn getxattr<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R) {
        let _w = self.watch.enter("getxattr", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        let name = match self.policies.xattrs.check_name(name, cx.caller) {
            Ok(name) => name,
            Err(error) => return r.done(err(error)),
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
                Err(error) => return r.done(err(error)),
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
                Ok(None) => return r.done(err(Code::NoData)),
                Err(error) => return r.done(err(error)),
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
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        let name = match self.policies.xattrs.check_name(name, cx.caller) {
            Ok(name) => name,
            Err(error) => return r.done(err(error)),
        };
        if self.policies.xattrs.is_virtual(&name) {
            r.done(err(Code::Perm));
            return;
        }
        if View::is_synthetic(ino) {
            r.done(err(Code::ReadOnly));
            return;
        }
        if value.len() > XATTR_VALUE_MAX {
            r.done(err(Code::TooBig));
            return;
        }
        let mode = match flags.mode() {
            Ok(SetXattrMode::Set) => constellation_meta::SetXattrMode::Set,
            Ok(SetXattrMode::Create) => constellation_meta::SetXattrMode::Create,
            Ok(SetXattrMode::Replace) => constellation_meta::SetXattrMode::Replace,
            Err(error) => return r.done(err(error)),
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
                Ok(Some(_)) => return r.done(err(Code::Invalid)),
                Ok(None) => return r.done(err(Code::NotFound)),
                Err(e) => return r.done(err(e.code())),
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
                        return r.done(err(Code::Invalid));
                    }
                }
                Err(e) => {
                    self.prune_stats.record_parse_error(&expr, e.offset, &e.msg);
                    return r.done(err(Code::Invalid));
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
                return r.done(err(refusal.code));
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
                return r.done(err(refusal.code));
            }
        }
        // Scratch files are node-private (scratch-directories.md) and
        // live outside the shared inode/xattr tables until `Publish`, so
        // their xattrs are staged locally here rather than mutated
        // through the lease-gated shared path.
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_set_xattr(ino, &name, value, mode) {
                Ok(()) => r.done(Ok(())),
                Err(error) => r.done(err(error.code())),
            };
        }
        match self.mutate_op(
            ino,
            constellation_meta::MutateOp::SetXattr {
                ino,
                name,
                value: value.to_vec(),
                mode: wire_mode,
            },
        ) {
            Ok(()) => {
                self.nudge_sync();
                r.done(Ok(()))
            }
            Err(error) => r.done(err(error)),
        }
    }

    fn listxattr<R: Responder<Vec<XattrNameBuf>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R) {
        let _w = self.watch.enter("listxattr", ino);
        let _admitted = admit!(self, cx, r);
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
            Err(error) => r.done(err(error)),
        }
    }

    fn removexattr<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R) {
        let _w = self.watch.enter("removexattr", ino);
        let _admitted = admit!(self, cx, r);
        let ino = enter!(self, ino, r);
        let name = match self.policies.xattrs.check_name(name, cx.caller) {
            Ok(name) => name,
            Err(error) => return r.done(err(error)),
        };
        if self.policies.xattrs.is_virtual(&name) {
            r.done(err(Code::Perm));
            return;
        }
        if View::is_synthetic(ino) {
            r.done(err(Code::ReadOnly));
            return;
        }
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_remove_xattr(ino, &name) {
                Ok(()) => r.done(Ok(())),
                Err(error) => r.done(err(error.code())),
            };
        }
        match self.mutate_op(ino, constellation_meta::MutateOp::RemoveXattr { ino, name }) {
            Ok(()) => {
                self.nudge_sync();
                r.done(Ok(()))
            }
            Err(error) => r.done(err(error)),
        }
    }

    /// Plan 30 §M14: `F_GETLK` under `--locks cluster` (see
    /// `crate::locks`). Under `--locks local` the kernel never asks.
    fn lock_test<R: Responder<LockStatus>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
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
        let status = match locks.test(
            ino,
            lock.owner.0,
            lock.range.start,
            lock.range.end,
            lock.kind == LockKind::Write,
        ) {
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
    /// here). The FUSE adapter wires interrupts to the `fsync` family only
    /// (plan 39 §3.3): a blocked lock wait cannot be cancelled by a signal.
    fn lock_acquire<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        lock: LockSpec,
        sleep: bool,
        r: R,
    ) {
        let watch = if sleep {
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
        let local = constellation_meta::locks::LocalLock {
            owner: lock.owner.0,
            pid: lock.pid,
            write: lock.kind == LockKind::Write,
            start: lock.range.start,
            end: lock.range.end,
        };
        if !self.caps.deferrable.contains(OpKind::LockAcquire) {
            r.done(locks.lock_here(ino, local, sleep).map_err(VfsError::from));
            return;
        }
        // The engine answers through this callback, from this thread or
        // from its `lock-wait` thread; the watchdog registration ends
        // with the answer, wherever it is given.
        locks.lock(ino, local, sleep, move |result: Result<(), Code>| {
            r.done(result.map_err(VfsError::from));
            drop(watch);
        });
    }

    /// `F_UNLCK`: drop `owner`'s locks in `range` (and, with none left on
    /// the file, release a recalled grant).
    fn lock_release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
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
