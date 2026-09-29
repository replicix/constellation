// Included by fusefs.rs — the fuser::Filesystem implementation.

/// POSIX NAME_MAX; the metadata plane accepts longer, so enforce here.
const NAME_MAX: usize = 255;

/// Returns the borrowed name or replies ENAMETOOLONG / EINVAL.
macro_rules! checked_name {
    ($name:expr, $reply:expr) => {{
        let n = $name.to_string_lossy();
        if n.as_bytes().len() > NAME_MAX {
            $reply.error(reply_code(Code::NameTooLong));
            return;
        }
        n
    }};
}

/// Write gate (DESIGN.md §5): a mutating op may only proceed while this
/// node holds the partition lease for `ino`'s partition. Acquisition is
/// lazy — the first mutation after a mount or an idle release blocks
/// here for one CAS.
macro_rules! gate {
    ($self:expr, $ino:expr, $reply:expr) => {
        if ConstellationFs::is_synthetic($ino) {
            $reply.error(reply_code(Code::ReadOnly));
            return;
        }
        if let Err(e) = $self.require_lease_for($ino) {
            $reply.error(reply_code(e));
            return;
        }
    };
}

impl Filesystem for FuseFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let threads = crate::parallelism::thread_plan();
        let _ = config.set_max_background(threads.fuse_max_background());
        let _ = config.set_congestion_threshold(threads.fuse_congestion_threshold());
        if threads.fuse > 1 {
            // Older kernels may not advertise this capability. Multi-reader
            // dispatch still works; only directory operations remain ordered.
            let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);
        }
        // Plan 30 §M14: under `--locks cluster` the kernel hands POSIX and
        // `flock` locks to `getlk`/`setlk`; without the capabilities (and
        // on a frozen snapshot view, where nothing can be written) it
        // keeps them node-local, as it always did.
        if self.cluster_locks().is_some() && !ConstellationFs::is_synthetic(self.view_root()) {
            if let Err(missing) =
                config.add_capabilities(InitFlags::FUSE_POSIX_LOCKS | InitFlags::FUSE_FLOCK_LOCKS)
            {
                tracing::warn!(
                    ?missing,
                    "the kernel does not offer FUSE lock forwarding; locks on this mount stay node-local"
                );
            }
        }
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _w = crate::fuse_watch::enter("lookup", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(name, reply);
        match self.lookup_synthetic(parent, &name) {
            Ok(Some((_ino, attr))) => {
                reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                reply.error(reply_code(error));
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
                reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0));
                return;
            }
            Ok(_) => {}
            Err(e) => {
                reply.error(reply_code(e.code()));
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
        crate::fuse_watch::stage("meta read");
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
                reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0))
            }
            Ok(None) => reply.error(reply_code(Code::NotFound)),
            Err(e) => reply.error(reply_code(e.code())),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let _w = crate::fuse_watch::enter("getattr", ino.0);
        let ino = ino.0;
        let requested_ino = ino;
        let ino = self.real_ino(ino);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                reply.error(reply_code(Code::Stale));
            } else {
                let attr = self.visible_attr(self.synthetic_attr(ino, &node));
                reply.attr(self.ttl(), &to_fuse_attr(&attr));
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
        crate::fuse_watch::stage("meta read");
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
                reply.attr(self.ttl(), &to_fuse_attr(&attr))
            }
            Ok(None) => reply.error(reply_code(Code::NotFound)),
            Err(e) => reply.error(reply_code(e.code())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let _w = crate::fuse_watch::enter("setattr", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let _inflight = self.inflight.enter(&[ino]);
        let atime_ns = _atime.map(time_or_now_ns);
        let mtime_ns = mtime.map(time_or_now_ns);
        // An explicit atime set must drop any queued read-bump for this
        // inode, so a pending coalesced bump cannot clobber a fresh
        // `touch -a` locally (the remote case is handled by the ctime
        // guard in replay).
        if atime_ns.is_some() {
            self.atime.purge(ino);
        }
        if let Some(new_size) = size {
            // Plan 30 §M14: a truncation is a write (fenced under a
            // lapsed lock grant).
            if self.lock_fenced(ino) {
                reply.error(reply_code(Code::Io));
                return;
            }
            self.lock_discard_tainted(ino);
            if let Err(error) = self.truncate(ino, new_size) {
                reply.error(reply_code(error));
                return;
            }
        }
        let result = self
            .mutate_op(
                ino,
                constellation_meta::MutateOp::Setattr {
                    ino,
                    mode,
                    uid,
                    gid,
                    size,
                    atime_ns,
                    mtime_ns,
                },
            )
            .and_then(|()| {
                self.meta
                    .getattr(ino)
                    .map_err(|error| error.code())?
                    .ok_or(Code::NotFound)
            });
        match result {
            Ok(attr) => reply.attr(self.ttl(), &to_fuse_attr(&attr)),
            Err(error) => reply.error(reply_code(error)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let _w = crate::fuse_watch::enter("readlink", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                reply.error(reply_code(Code::Stale));
            } else if let SyntheticNode::Frozen {
                kind: InodeKind::Symlink,
                target: Some(target),
                ..
            } = node
            {
                reply.data(target.as_bytes());
            } else {
                reply.error(reply_code(Code::Invalid));
            }
            return;
        }
        self.session_wait(&[ReadKey::Ino(ino)]);
        match self.meta.readlink(ino) {
            Ok(Some(target)) => reply.data(target.as_bytes()),
            Ok(None) => reply.error(reply_code(Code::Invalid)),
            Err(e) => reply.error(reply_code(e.code())),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let _w = crate::fuse_watch::enter("mkdir", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(name, reply);
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return reply.error(reply_code(e.code())),
        };
        if self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some()
        {
            return match self
                .meta
                .scratch_mkdir(parent, &name, ino, mode, req.uid(), req.gid())
            {
                Ok(attr) => reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0)),
                Err(e) => reply.error(reply_code(e.code())),
            };
        }
        let op = constellation_meta::MutateOp::Mkdir {
            parent,
            name: name.into_owned(),
            ino,
            mode,
            uid: req.uid(),
            gid: req.gid(),
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => {
                    reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0))
                }
                Ok(None) => reply.error(reply_code(Code::Io)),
                Err(e) => reply.error(reply_code(e.code())),
            },
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let _w = crate::fuse_watch::enter("mknod", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(name, reply);
        let kind = match mode & libc::S_IFMT {
            libc::S_IFREG | 0 => InodeKind::File,
            libc::S_IFIFO => InodeKind::Fifo,
            libc::S_IFSOCK => InodeKind::Socket,
            libc::S_IFBLK => InodeKind::BlockDev,
            libc::S_IFCHR => InodeKind::CharDev,
            _ => {
                reply.error(reply_code(Code::Invalid));
                return;
            }
        };
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return reply.error(reply_code(e.code())),
        };
        let op = if kind == InodeKind::File {
            constellation_meta::MutateOp::Create {
                parent,
                name: name.into_owned(),
                ino,
                mode: mode & 0o7777,
                uid: req.uid(),
                gid: req.gid(),
            }
        } else {
            constellation_meta::MutateOp::Mknod {
                parent,
                name: name.into_owned(),
                ino,
                kind: kind.as_u8(),
                mode: mode & 0o7777,
                uid: req.uid(),
                gid: req.gid(),
                // FUSE carries the kernel's 32-bit `new_encode_dev`; the
                // journal carries the portable pair (plan 31 §7).
                rdev: constellation_types::rdev::from_linux_fuse_rdev(rdev),
            }
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => {
                    reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0))
                }
                Ok(None) => reply.error(reply_code(Code::Io)),
                Err(e) => reply.error(reply_code(e.code())),
            },
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let _w = crate::fuse_watch::enter("link", ino.0);
        let ino = ino.0;
        let newparent = newparent.0;
        let ino = self.real_ino(ino);
        let newparent = self.real_ino(newparent);
        let _inflight = self.inflight.enter(&[ino, newparent]);
        let name = checked_name!(newname, reply);
        let op = constellation_meta::MutateOp::Link {
            ino,
            parent: newparent,
            name: name.into_owned(),
        };
        match self.mutate_op(newparent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => {
                    reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0))
                }
                Ok(None) => reply.error(reply_code(Code::NotFound)),
                Err(e) => reply.error(reply_code(e.code())),
            },
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let _w = crate::fuse_watch::enter("create", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(name, reply);
        let caller = Caller {
            uid: req.uid(),
            gid: req.gid(),
            pid: req.pid(),
        };
        let started = std::time::Instant::now();
        let result = self.create_or_open(parent, &name, mode, flags, &caller);
        let took = started.elapsed();
        if took >= crate::fusefs::slow_fuse_op() {
            tracing::warn!(parent, name = %name, ?took, ok = result.is_ok(), "slow FUSE create");
        }
        match result {
            Ok((attr, _created)) => {
                *self.opens.lock().unwrap().entry(attr.ino).or_insert(0) += 1;
                reply.created(
                    self.ttl(),
                    &to_fuse_attr(&attr),
                    fuser::Generation(0),
                    FileHandle(attr.ino),
                    fuser::FopenFlags::empty(),
                )
            }
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let _w = crate::fuse_watch::enter("symlink", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(link_name, reply);
        let target = target.to_string_lossy();
        let ino = match self.meta.allocate_ino(parent) {
            Ok(ino) => ino,
            Err(e) => return reply.error(reply_code(e.code())),
        };
        let op = constellation_meta::MutateOp::Symlink {
            parent,
            name: name.into_owned(),
            ino,
            target: target.into_owned(),
            uid: req.uid(),
            gid: req.gid(),
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => {
                    reply.entry(self.ttl(), &to_fuse_attr(&attr), fuser::Generation(0))
                }
                Ok(None) => reply.error(reply_code(Code::Io)),
                Err(e) => reply.error(reply_code(e.code())),
            },
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _w = crate::fuse_watch::enter("unlink", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(name, reply);
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
                reply.ok()
            }
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _w = crate::fuse_watch::enter("rmdir", parent.0);
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let _inflight = self.inflight.enter(&[parent]);
        let name = checked_name!(name, reply);
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
            Ok(()) => reply.ok(),
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let _w = crate::fuse_watch::enter("rename", parent.0);
        let parent = parent.0;
        let newparent = newparent.0;
        let parent = self.real_ino(parent);
        let newparent = self.real_ino(newparent);
        let _inflight = self.inflight.enter(&[parent, newparent]);
        // Enforce NAME_MAX on both names, like every other name-taking op;
        // rename previously converted them with `to_string_lossy` directly
        // and so accepted names the metadata plane would otherwise store
        // over the POSIX limit. `checked_name!` replies ENAMETOOLONG and
        // returns for an over-long name (`reply` is consumed only on that
        // diverging path, so it stays available for the second check and
        // the operation itself).
        let name = checked_name!(name, reply).into_owned();
        let newname = checked_name!(newname, reply).into_owned();
        let src_scratch = self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some();
        let dst_scratch = self.meta.is_scratch_dir(newparent).unwrap_or(false)
            || self
                .meta
                .scratch_getattr(newparent)
                .ok()
                .flatten()
                .is_some();
        if src_scratch && dst_scratch {
            return match self.meta.scratch_rename(parent, &name, newparent, &newname) {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(reply_code(error.code())),
            };
        }
        if dst_scratch {
            reply.error(reply_code(Code::CrossDevice));
            return;
        }
        if src_scratch {
            let attr = match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr)) => attr,
                Ok(None) => {
                    reply.error(reply_code(Code::NotFound));
                    return;
                }
                Err(error) => {
                    reply.error(reply_code(error.code()));
                    return;
                }
            };
            if attr.kind != InodeKind::File {
                reply.error(reply_code(Code::CrossDevice));
                return;
            }
            // Plan 30 §M14: publishing the file is a publication point.
            match self.lock_publish_gate(attr.ino) {
                Err(error) => {
                    reply.error(reply_code(error));
                    return;
                }
                Ok(true) => {
                    // Owed to the application's next close, not to this
                    // rename.
                    if let Some(l) = self.cluster_locks() {
                        l.owe(attr.ino);
                    }
                }
                Ok(false) => {}
            }
            if let Err(error) = self.flush_inode(attr.ino, true) {
                reply.error(reply_code(error));
                return;
            }
            if let Err(error) = self.drain_inode(attr.ino) {
                reply.error(reply_code(error));
                return;
            }
            let manifest = match self.meta.scratch_manifest(attr.ino) {
                Ok(Some(manifest)) => manifest,
                Ok(None) => Manifest::empty(self.chunk_size).encode(),
                Err(error) => {
                    reply.error(reply_code(error.code()));
                    return;
                }
            };
            let xattrs = match self.meta.scratch_xattrs(attr.ino) {
                Ok(xattrs) => xattrs,
                Err(error) => {
                    reply.error(reply_code(error.code()));
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
            };
            return match self.mutate_op(newparent, op) {
                Ok(()) => match self.meta.scratch_unlink(parent, &name) {
                    Ok(()) => reply.ok(),
                    Err(error) => reply.error(reply_code(error.code())),
                },
                Err(error) => reply.error(reply_code(error)),
            };
        }
        let op = constellation_meta::MutateOp::Rename {
            parent,
            name,
            new_parent: newparent,
            new_name: newname,
        };
        match self.mutate_op(parent, op) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let _w = crate::fuse_watch::enter("open", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                reply.error(reply_code(Code::Stale));
            } else if matches!(
                node,
                SyntheticNode::Frozen {
                    kind: InodeKind::File,
                    ..
                }
            ) {
                reply.opened(FileHandle(ino), fuser::FopenFlags::empty());
            } else {
                reply.error(reply_code(Code::IsDir));
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
            Ok(Some(_)) => {
                *self.opens.lock().unwrap().entry(ino).or_insert(0) += 1;
                reply.opened(FileHandle(ino), fuser::FopenFlags::empty())
            }
            Ok(None) => reply.error(reply_code(Code::NotFound)),
            Err(e) => reply.error(reply_code(e.code())),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let _w = crate::fuse_watch::enter("read", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let _inflight = self.inflight.enter(&[ino]);
        if ConstellationFs::is_synthetic(ino) {
            match self.read_frozen(ino, offset, size as u64) {
                Ok(data) => reply.data(&data),
                Err(error) => reply.error(reply_code(error)),
            }
            return;
        }
        if self.lock_fenced(ino) {
            reply.error(reply_code(Code::Io));
            return;
        }
        match self.do_read(ino, offset, size as u64) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let _w = crate::fuse_watch::enter("write", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let _inflight = self.inflight.enter(&[ino]);
        if ConstellationFs::is_synthetic(ino) {
            reply.error(reply_code(Code::ReadOnly));
            return;
        }
        if self.lock_fenced(ino) {
            reply.error(reply_code(Code::Io));
            return;
        }
        self.lock_discard_tainted(ino);
        match self.do_write(ino, offset, data) {
            Ok(n) if flags.0 & (libc::O_SYNC | libc::O_DSYNC) != 0 => {
                match self.flush_inode(ino, true) {
                    Ok(()) => reply.written(n),
                    Err(error) => reply.error(reply_code(error)),
                }
            }
            Ok(n) => reply.written(n),
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let _w = crate::fuse_watch::enter("flush", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        // Plan 30 §M14: the fence first, while the closing owner's locks
        // are still there (dropping them would lift it): data written
        // under a lapsed grant is discarded, never published. Then the
        // close drops the process's POSIX locks on the file (any
        // descriptor's close, as POSIX says); the kernel also sends an
        // explicit unlock, which then finds nothing.
        let locks = self
            .cluster_locks()
            .filter(|_| !ConstellationFs::is_synthetic(ino));
        let gate = self.lock_publish_gate(ino);
        let idle = locks.map(|l| l.drop_owner(ino, lock_owner.0));
        let r = gate.and_then(|owed| {
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
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _w = crate::fuse_watch::enter("fsync", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        // Plan 30 §M14: nothing written under a lapsed grant is made
        // durable (`lock_publish_gate`).
        let owed = match self.lock_publish_gate(ino) {
            Ok(owed) => owed,
            Err(e) => {
                reply.error(reply_code(e));
                return;
            }
        };
        match self.flush_inode(ino, true) {
            Ok(()) => match self.sync_barrier(ino) {
                Ok(()) if owed => reply.error(reply_code(Code::Io)),
                Ok(()) => reply.ok(),
                Err(e) => reply.error(reply_code(e)),
            },
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let _w = crate::fuse_watch::enter("release", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if ConstellationFs::is_synthetic(ino) {
            reply.ok();
            return;
        }
        // Plan 30 §M14: the fence first (as in `flush`), then
        // `FUSE_RELEASE_FLOCK_UNLOCK` — the last close of an open file
        // drops its `flock` lock (whose owner is the open file).
        let locks = self.cluster_locks();
        let gate = self.lock_publish_gate(ino);
        let idle = match (locks, lock_owner) {
            (Some(l), Some(owner)) => l.drop_owner(ino, owner.0),
            _ => false,
        };
        let flush_result = gate.and_then(|owed| {
            self.flush_inode(ino, flags.0 & (libc::O_SYNC | libc::O_DSYNC) != 0)?;
            if owed {
                return Err(Code::Io);
            }
            Ok(())
        });
        if let (Some(l), true) = (locks, idle) {
            l.idle(ino);
        }
        let last = {
            let mut opens = self.opens.lock().unwrap();
            match opens.get_mut(&ino) {
                Some(n) => {
                    *n = n.saturating_sub(1);
                    let last = *n == 0;
                    if last {
                        opens.remove(&ino);
                        self.prefetch.forget(ino);
                    }
                    last
                }
                None => false,
            }
        };
        // Orphan reap on last close (unlink-while-open, DESIGN.md §3).
        if last {
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
                reply.ok()
            }
            Err(e) => reply.error(reply_code(e)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let _w = crate::fuse_watch::enter("readdir", ino.0);
        let ino = ino.0;
        let visible_ino = ino;
        let ino = self.real_ino(ino);
        let _inflight = self.inflight.enter(&[ino]);
        if ConstellationFs::is_synthetic(ino) {
            let entries = match self.synthetic_entries(ino) {
                Ok(entries) => entries,
                Err(error) => return reply.error(reply_code(error)),
            };
            let mut idx = offset;
            loop {
                let next = idx + 1;
                let full = match idx {
                    0 => reply.add(INodeNo(visible_ino), next, FileType::Directory, "."),
                    1 => reply.add(INodeNo(visible_ino), next, FileType::Directory, ".."),
                    _ => {
                        let Some((child_ino, kind, name)) = entries.get((idx - 2) as usize) else {
                            break;
                        };
                        let file_type = match kind {
                            InodeKind::File => FileType::RegularFile,
                            InodeKind::Dir => FileType::Directory,
                            InodeKind::Symlink => FileType::Symlink,
                            InodeKind::Fifo => FileType::NamedPipe,
                            InodeKind::Socket => FileType::Socket,
                            InodeKind::BlockDev => FileType::BlockDevice,
                            InodeKind::CharDev => FileType::CharDevice,
                        };
                        reply.add(INodeNo(*child_ino), next, file_type, name)
                    }
                };
                if full {
                    break;
                }
                idx = next;
            }
            reply.ok();
            return;
        }
        // Plan 30 §M6: once per listing (its first chunk). §M8: a strict
        // listing sees every entry a completed close elsewhere made.
        if offset == 0 {
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
            Err(e) => return reply.error(reply_code(e.code())),
        };
        // Stable cursor: "." = 1, ".." = 2, children from 3.
        let mut idx = offset;
        loop {
            let next = idx + 1;
            let full = match idx {
                0 => reply.add(INodeNo(visible_ino), next, FileType::Directory, "."),
                1 => reply.add(INodeNo(visible_ino), next, FileType::Directory, ".."),
                _ => {
                    let child = match entries.get((idx - 2) as usize) {
                        Some(c) => c,
                        None => break,
                    };
                    let ft = match child.kind {
                        InodeKind::File => FileType::RegularFile,
                        InodeKind::Dir => FileType::Directory,
                        InodeKind::Symlink => FileType::Symlink,
                        InodeKind::Fifo => FileType::NamedPipe,
                        InodeKind::Socket => FileType::Socket,
                        InodeKind::BlockDev => FileType::BlockDevice,
                        InodeKind::CharDev => FileType::CharDevice,
                    };
                    reply.add(INodeNo(child.ino), next, ft, &child.name)
                }
            };
            if full {
                break;
            }
            idx = next;
        }
        reply.ok();
    }

    fn setxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let _w = crate::fuse_watch::enter("setxattr", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let name = match checked_xattr_name(req, name) {
            Ok(name) => name,
            Err(error) => return reply.error(reply_code(error)),
        };
        if virtual_xattr(&name) {
            reply.error(reply_code(Code::Perm));
            return;
        }
        if ConstellationFs::is_synthetic(ino) {
            reply.error(reply_code(Code::ReadOnly));
            return;
        }
        if value.len() > 64 * 1024 {
            reply.error(reply_code(Code::TooBig));
            return;
        }
        if position != 0 {
            reply.error(reply_code(Code::Invalid));
            return;
        }
        let mode = match flags {
            0 => constellation_meta::SetXattrMode::Set,
            libc::XATTR_CREATE => constellation_meta::SetXattrMode::Create,
            libc::XATTR_REPLACE => constellation_meta::SetXattrMode::Replace,
            _ => return reply.error(reply_code(Code::Invalid)),
        };
        let wire_mode = match mode {
            constellation_meta::SetXattrMode::Create => 1,
            constellation_meta::SetXattrMode::Replace => 2,
            constellation_meta::SetXattrMode::Set => 0,
        };
        // Prune-policy validation gate (plan 22, Step 2). Reject an
        // unparseable or misplaced policy at the FUSE boundary with
        // EINVAL, so it never reaches the log; stash the reason where the
        // CLI/status can surface it, since errno carries no message.
        if name == constellation_meta::prune::PRUNE_XATTR {
            // Only directories may carry a policy.
            match self.meta.getattr(ino) {
                Ok(Some(attr)) if attr.kind == InodeKind::Dir => {}
                Ok(Some(_)) => return reply.error(reply_code(Code::Invalid)),
                Ok(None) => return reply.error(reply_code(Code::NotFound)),
                Err(e) => return reply.error(reply_code(e.code())),
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
                        return reply.error(reply_code(Code::Invalid));
                    }
                }
                Err(e) => {
                    self.prune_stats.record_parse_error(&expr, e.offset, &e.msg);
                    return reply.error(reply_code(Code::Invalid));
                }
            }
        }
        // Scratch files are node-private (scratch-directories.md) and
        // live outside the shared inode/xattr tables until `Publish`, so
        // their xattrs are staged locally here rather than mutated
        // through the lease-gated shared path.
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_set_xattr(ino, &name, value, mode) {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(reply_code(error.code())),
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
                reply.ok()
            }
            Err(error) => reply.error(reply_code(error)),
        }
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let _w = crate::fuse_watch::enter("getxattr", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let name = match checked_xattr_name(req, name) {
            Ok(name) => name,
            Err(error) => return reply.error(reply_code(error)),
        };
        if !ConstellationFs::is_synthetic(ino) {
            self.session_wait(&[ReadKey::Ino(ino)]);
        }
        let value = if virtual_xattr(&name) {
            let aggregate = if ConstellationFs::is_synthetic(ino) {
                self.synthetic_recursive_size(ino)
            } else {
                self.meta.recursive_size(ino).map_err(|error| error.code())
            };
            match aggregate {
                Ok((rsize, rcount)) => {
                    if name == RSIZE_XATTR {
                        rsize.to_string().into_bytes()
                    } else {
                        rcount.to_string().into_bytes()
                    }
                }
                Err(error) => return reply.error(reply_code(error)),
            }
        } else {
            let result = if ConstellationFs::is_synthetic(ino) {
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
                Ok(None) => return reply.error(reply_code(Code::NoData)),
                Err(error) => return reply.error(reply_code(error)),
            }
        };
        reply_xattr(value, size, reply);
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let _w = crate::fuse_watch::enter("listxattr", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if !ConstellationFs::is_synthetic(ino) {
            self.session_wait(&[ReadKey::Ino(ino)]);
        }
        let names = if ConstellationFs::is_synthetic(ino) {
            self.synthetic_xattrs(ino)
                .map(|attrs| attrs.into_iter().map(|(name, _)| name).collect())
        } else if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            self.meta
                .scratch_list_xattrs(ino)
                .map_err(|error| error.code())
        } else {
            self.meta.list_xattrs(ino).map_err(|error| error.code())
        };
        let mut names: Vec<String> = match names {
            Ok(names) => names,
            Err(error) => return reply.error(reply_code(error)),
        };
        names.push(RSIZE_XATTR.to_string());
        names.push(RCOUNT_XATTR.to_string());
        names.sort();
        names.dedup();
        let mut encoded = Vec::new();
        for name in names {
            encoded.extend_from_slice(name.as_bytes());
            encoded.push(0);
        }
        reply_xattr(encoded, size, reply);
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _w = crate::fuse_watch::enter("removexattr", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let name = match checked_xattr_name(req, name) {
            Ok(name) => name,
            Err(error) => return reply.error(reply_code(error)),
        };
        if virtual_xattr(&name) {
            reply.error(reply_code(Code::Perm));
            return;
        }
        if ConstellationFs::is_synthetic(ino) {
            reply.error(reply_code(Code::ReadOnly));
            return;
        }
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_remove_xattr(ino, &name) {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(reply_code(error.code())),
            };
        }
        match self.mutate_op(ino, constellation_meta::MutateOp::RemoveXattr { ino, name }) {
            Ok(()) => {
                self.nudge_sync();
                reply.ok()
            }
            Err(error) => reply.error(reply_code(error)),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: fuser::ReplyStatfs) {
        let _w = crate::fuse_watch::enter("statfs", _ino.0);
        // Used space is logical bytes under the mounted view; free space
        // is whole-filesystem headroom under the cap. See `statfs_blocks`.
        // Block size mirrors blksize.
        let bsize: u32 = 131072;
        let (used_bytes, file_count) = self.view_usage();
        let (total_blocks, bfree) = statfs_blocks(
            used_bytes,
            self.meta.usage().0,
            self.cached_quota(),
            bsize as u64,
        );
        let ffree = (u64::MAX / 2).saturating_sub(file_count);
        reply.statfs(
            total_blocks,
            bfree,
            bfree,
            file_count,
            ffree,
            bsize,
            255,
            bsize,
        );
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let _w = crate::fuse_watch::enter("fallocate", ino.0);
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let _inflight = self.inflight.enter(&[ino]);
        gate!(self, ino, reply);
        if self.lock_fenced(ino) {
            reply.error(reply_code(Code::Io));
            return;
        }
        if length == 0 {
            reply.error(reply_code(Code::Invalid));
            return;
        }
        match self.do_fallocate(ino, offset, length, mode) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(reply_code(error)),
        }
    }

    fn lseek(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let _w = crate::fuse_watch::enter("lseek", ino.0);
        let ino = ino.0;
        if offset < 0 {
            reply.error(reply_code(Code::NoDeviceOrAddress));
            return;
        }
        match self.seek_sparse(self.real_ino(ino), offset as u64, whence) {
            Ok(position) => reply.offset(position),
            Err(error) => reply.error(reply_code(error)),
        }
    }

    /// Plan 30 §M14: `F_GETLK` under `--locks cluster` (see
    /// `crate::locks`). Under `--locks local` the kernel never asks.
    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        let _w = crate::fuse_watch::enter("getlk", ino.0);
        let ino = self.real_ino(ino.0);
        let Some(locks) = self.cluster_locks() else {
            reply.error(reply_code(Code::NotImplemented));
            return;
        };
        if ConstellationFs::is_synthetic(ino) {
            // Frozen snapshot files take no cluster locks (see `setlk`).
            reply.locked(0, 0, libc::F_UNLCK, 0);
            return;
        }
        let (start, end, typ, pid) = locks.test(ino, lock_owner.0, start, end, typ);
        reply.locked(start, end, typ, pid);
    }

    /// Plan 30 §M14: `F_SETLK`/`F_SETLKW`/`flock` under `--locks
    /// cluster`. Non-blocking requests are answered on this worker; a
    /// blocking one (`sleep`) waits on a thread of its own
    /// (`ClusterLocks::lock`), so a contended lock never pins a FUSE
    /// worker. fuser 0.18 delivers no interrupts: a blocked wait cannot be
    /// cancelled by a signal.
    fn setlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let watch = if sleep {
            crate::fuse_watch::enter_blocking("setlk", ino.0)
        } else {
            crate::fuse_watch::enter("setlk", ino.0)
        };
        let ino = self.real_ino(ino.0);
        let Some(locks) = self.cluster_locks() else {
            reply.error(reply_code(Code::NotImplemented));
            return;
        };
        if typ == libc::F_UNLCK {
            locks.unlock(ino, lock_owner.0, start, end);
            reply.ok();
            return;
        }
        if typ != libc::F_RDLCK && typ != libc::F_WRLCK {
            reply.error(reply_code(Code::Invalid));
            return;
        }
        if ConstellationFs::is_synthetic(ino) {
            // A frozen snapshot file (inside a live view's `.snapshots`)
            // has no sequencer to lease a grant from.
            reply.error(reply_code(Code::NoLock));
            return;
        }
        // Data written under an earlier grant that ended without its
        // flush does not ride along under the new one.
        self.lock_discard_tainted(ino);
        let lock = constellation_meta::locks::LocalLock {
            owner: lock_owner.0,
            pid,
            write: typ == libc::F_WRLCK,
            start,
            end,
        };
        locks.lock(ino, lock, sleep, reply, watch);
    }
}

fn checked_xattr_name(req: &Request, name: &OsStr) -> Result<String, Code> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return Err(Code::Range);
    }
    let name = std::str::from_utf8(bytes).map_err(|_| Code::Invalid)?;
    if name.starts_with("user.") {
        return Ok(name.to_string());
    }
    if name.starts_with("trusted.") {
        // FUSE does not carry Linux capability bits. Treat uid 0 as the
        // kernel-authenticated privileged caller and refuse everyone else.
        return if req.uid() == 0 {
            Ok(name.to_string())
        } else {
            Err(Code::Perm)
        };
    }
    Err(Code::NotSupported)
}

fn reply_xattr(value: Vec<u8>, size: u32, reply: ReplyXattr) {
    if size == 0 {
        reply.size(value.len() as u32);
    } else if (size as usize) < value.len() {
        reply.error(reply_code(Code::Range));
    } else {
        reply.data(&value);
    }
}

impl ConstellationFs {
    fn do_read(&self, ino: Ino, offset: u64, size: u64) -> Result<Vec<u8>, Code> {
        // Serve pending (unflushed) state when present so read-after-write
        // within an open handle is coherent. The inode's operation lock
        // orders the read against writes and flushes of the same file for
        // its whole duration; the session is detached from its shard so
        // a chunk fetch (which may wait on S3) holds no shard lock
        // (EC2 finding 1).
        let _op = self.inode_ops.lock(ino);
        let ws = self.writes.detach(ino);
        let result = self.do_read_detached(ino, ws.as_ref(), offset, size);
        if let Some(ws) = ws {
            self.writes.reattach(ino, ws);
        }
        result
    }

    fn do_read_detached(
        &self,
        ino: Ino,
        ws: Option<&WriteState>,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, Code> {
        let manifest = self.load_manifest(ino)?;
        let attr = self.meta.getattr(ino).map_err(|e| e.code())?;
        let committed_len = attr.as_ref().map(|a| a.size).unwrap_or(manifest.file_len);
        let file_len = ws.as_ref().map(|w| w.file_len).unwrap_or(committed_len);
        if offset >= file_len {
            return Ok(Vec::new());
        }
        // Read-time atime (plan 20): best-effort, policy-gated, and a
        // no-op unless the operator opted in. Records into a separate
        // accumulator shard (never the write shard held here), so it
        // adds a lock only in the rare bump case and cannot deadlock the
        // read. A missing attr row simply skips the bump. Placed after
        // the EOF early return: a zero-byte read need not move atime.
        if let Some(attr) = &attr {
            self.atime
                .on_read(attr, constellation_fs_core::types::now_ns());
        }
        let len = size.min(file_len - offset);
        let hashes = self.chunk_list(&manifest)?;
        if offset == 0 {
            let files = self.scan.note_read(ino);
            self.prefetch.enqueue_scan(files);
        }
        // Kick sequential readahead for upcoming committed chunks.
        self.prefetch
            .on_read(ino, offset, len, self.chunk_size, &hashes);
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut out = Vec::with_capacity(len as usize);
        for slice in layout.slices(offset, len) {
            let full_len = layout.chunk_len(file_len, slice.index);
            let chunk: Vec<u8> = match &ws {
                Some(w) if w.sealed.contains_key(&slice.index) => self
                    .cache
                    .get(w.sealed.get(&slice.index).unwrap())
                    .map_err(|_| Code::Io)?
                    .ok_or(Code::Io)?,
                Some(w) if w.staging.is_dirty(slice.index) => {
                    let mut buf = vec![0u8; full_len as usize];
                    w.staging
                        .read_at(slice.index * self.chunk_size as u64, &mut buf)
                        .map_err(|e| staging_code(&e))?;
                    buf
                }
                // Punched whole by this session: zeros.
                Some(w) if w.zeroed.contains(slice.index) => Vec::new(),
                Some(w) => {
                    // Untouched by this session: the base's bytes, dead
                    // past a truncation (`WriteState::floor`).
                    let mut chunk = self.read_committed_chunk(ino, &hashes, slice.index)?;
                    w.clip_base(
                        &mut chunk,
                        slice.index * self.chunk_size as u64,
                        manifest.file_len,
                    );
                    chunk
                }
                None => {
                    // No session: the committed manifest, valid only
                    // below its `file_len` (a truncate lowered it; the
                    // chunk straddling it keeps dead bytes past it, and
                    // the inode's size may have grown past it since).
                    let mut chunk = self.read_committed_chunk(ino, &hashes, slice.index)?;
                    clip_at(
                        Some(manifest.file_len),
                        &mut chunk,
                        slice.index * self.chunk_size as u64,
                    );
                    chunk
                }
            };
            let start = slice.offset as usize;
            let end = (slice.offset + slice.len) as usize;
            if chunk.len() >= end {
                out.extend_from_slice(&chunk[start..end]);
            } else {
                // Short chunk (hole/EOF): zero-fill the gap.
                let have = chunk.len().saturating_sub(start.min(chunk.len()));
                if have > 0 {
                    out.extend_from_slice(&chunk[start..start + have]);
                }
                out.resize(out.len() + (end - start - have), 0);
            }
        }
        Ok(out)
    }

    fn read_committed_chunk(
        &self,
        ino: Ino,
        hashes: &constellation_fs_core::manifest::SparseChunks,
        idx: u64,
    ) -> Result<Vec<u8>, Code> {
        match hashes.get(&idx) {
            Some(h) => self.fetch_chunk_for_inode(Some(ino), h),
            None => Ok(Vec::new()),
        }
    }

    fn do_write(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32, Code> {
        if data.is_empty() {
            return Ok(0);
        }
        let dirty = self.cache.dirty_bytes();
        let budget = self.cache.usage().budget;
        match crate::writeback::throttle_delay(dirty, budget) {
            Ok(delay) if !delay.is_zero() => {
                crate::fuse_watch::stage("writeback throttle (dirty cache)");
                std::thread::sleep(delay)
            }
            Ok(_) => {}
            Err(()) => {
                // Sealed chunks grow the dirty-cache budget in
                // chunk-sized steps (plan 29 M6), exactly like staging
                // below: a small `--cache-size` relative to the chunk
                // size can cross the whole 75%-99% soft-pressure band in
                // one write and hit the hard limit with no prior
                // `throttle_delay` ever having slept. Without this grace
                // sleep, whichever of the two budgets (this one or
                // staging's, right below) happened to be the one that
                // ran out first was timing-dependent, which is what made
                // `writeback-backpressure` fail intermittently ("ENOSPC
                // arrived without observable throttling") rather than
                // consistently either way.
                std::thread::sleep(Duration::from_millis(100));
                return Err(Code::NoSpace);
            }
        }
        match crate::writeback::throttle_delay(
            self.staging_budget.used(),
            self.staging_budget.budget(),
        ) {
            Ok(delay) if !delay.is_zero() => {
                crate::fuse_watch::stage("writeback throttle (staging)");
                std::thread::sleep(delay)
            }
            Ok(_) => {}
            Err(()) => {
                // Staging reservations grow in chunk-sized steps, so a
                // tiny budget can cross the soft-pressure band in one
                // write. Preserve observable backpressure before ENOSPC.
                std::thread::sleep(Duration::from_millis(100));
                return Err(Code::NoSpace);
            }
        }
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let hashes = self.chunk_list(&manifest)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut writes = self.writes.lock(ino);
        let ws = self.write_state(&mut writes, ino, &manifest)?;
        // Guard the range arithmetic: a huge offset near u64::MAX would
        // otherwise overflow and panic here while holding the write-shard
        // lock (poisoning it — see WriteShards::lock). Refuse with EFBIG,
        // as `do_fallocate` already does for the same overflow.
        let write_end = offset
            .checked_add(data.len() as u64)
            .ok_or(Code::FileTooBig)?;
        let new_file_len = ws.file_len.max(write_end);
        self.quota_check(ino, new_file_len)?;
        ws.staging
            .set_len_sparse(new_file_len)
            .map_err(|e| staging_code(&e))?;
        let mut consumed = 0usize;
        for slice in layout.slices(offset, data.len() as u64) {
            let full_len = layout.chunk_len(new_file_len, slice.index);
            let is_whole_chunk = slice.offset == 0 && slice.len == full_len;
            let chunk_start = slice.index * self.chunk_size as u64;
            let sealed = self.unseal(ws, ino, slice.index)?;
            // A chunk this session punched (or zeroed) whole has no base
            // content any more: seeded from zeros, never the base's
            // bytes (a sealed copy is this session's own, and valid).
            let punched = ws.zeroed.contains(slice.index);
            ws.holes.clear(slice.index);
            ws.staging
                .prepare_chunk(slice.index, self.chunk_size)
                .map_err(|e| staging_code(&e))?;
            // A partial (not-whole-chunk) write into a chunk this open
            // handle has not touched yet must first seed the untouched
            // bytes from the committed content — otherwise they would
            // read back as a spurious hole (zero) instead of their real
            // pre-write value.
            if !is_whole_chunk && !ws.staging.is_dirty(slice.index) {
                let seed = match sealed {
                    Some(hash) => {
                        let mut data = self
                            .cache
                            .get(&hash)
                            .map_err(|_| Code::Io)?
                            .ok_or(Code::Io)?;
                        data.resize(full_len as usize, 0);
                        data
                    }
                    None if punched => vec![0u8; full_len as usize],
                    None => {
                        let mut seed =
                            self.committed_chunk_padded(&hashes, slice.index, full_len)?;
                        ws.clip_base(&mut seed, chunk_start, manifest.file_len);
                        seed
                    }
                };
                ws.staging
                    .write_at(chunk_start, &seed)
                    .map_err(|e| staging_code(&e))?;
            }
            let write_start = chunk_start + u64::from(slice.offset);
            let write_end = write_start + u64::from(slice.len);
            ws.staging
                .write_at(write_start, &data[consumed..consumed + slice.len as usize])
                .map_err(|e| staging_code(&e))?;
            ws.staging.mark_dirty(slice.index);
            ws.written.push((write_start, write_end));
            consumed += slice.len as usize;
        }
        ws.file_len = new_file_len;
        if offset <= ws.high_water && write_end > ws.high_water {
            ws.high_water = write_end;
        }
        self.seal_crossed_chunks(ino, ws)?;
        Ok(data.len() as u32)
    }

    fn truncate(&self, ino: Ino, new_size: u64) -> Result<(), Code> {
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let mut writes = self.writes.lock(ino);
        self.truncate_locked(&mut writes, ino, new_size, &manifest)
    }

    fn truncate_locked(
        &self,
        writes: &mut HashMap<Ino, WriteState>,
        ino: Ino,
        new_size: u64,
        manifest: &Manifest,
    ) -> Result<(), Code> {
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let cs = u64::from(self.chunk_size);
        let ws = self.write_state(writes, ino, manifest)?;
        self.quota_check(ino, new_size)?;
        if new_size < ws.file_len {
            // Everything at or past `new_size` is dead, whatever holds it
            // — the base (`floor`: zeroed wherever base content is read
            // for this session), this session's sealed chunks, its
            // staged bytes, its recorded writes — so that a later
            // extension reads zeros there.
            ws.floor = Some(ws.floor.map_or(new_size, |f| f.min(new_size)));
            let keep = layout.chunk_count(new_size);
            let old_chunks = layout.chunk_count(ws.file_len);
            // Sealed chunks: wholly past the point, dropped; the one the
            // point falls inside goes back to staging, to be cut below.
            let sealed: Vec<u64> = ws
                .sealed
                .keys()
                .copied()
                .filter(|i| *i * cs < ws.file_len)
                .collect();
            for idx in sealed {
                if idx * cs >= new_size {
                    self.unseal(ws, ino, idx)?;
                } else if (idx + 1) * cs > new_size {
                    let hash = self.unseal(ws, ino, idx)?.expect("sealed");
                    let data = self
                        .cache
                        .get(&hash)
                        .map_err(|_| Code::Io)?
                        .ok_or(Code::Io)?;
                    ws.staging
                        .prepare_chunk(idx, self.chunk_size)
                        .map_err(|e| staging_code(&e))?;
                    ws.staging
                        .write_at(idx * cs, &data)
                        .map_err(|e| staging_code(&e))?;
                    // (Its written ranges are already recorded; the rest
                    // of it was seeded from the base, which the
                    // composition re-reads, clipped.)
                    ws.staging.mark_dirty(idx);
                }
            }
            // Drop dirty runs past the new end; `Staging::set_len`
            // (ftruncate) cuts the boundary chunk's staged bytes.
            ws.staging.retain_dirty_below(keep);
            ws.staging.punch_chunks(keep, old_chunks, self.chunk_size);
            ws.written.retain_mut(|(start, end)| {
                *end = (*end).min(new_size);
                *start < *end
            });
            ws.high_water = ws.high_water.min(new_size);
        }
        ws.staging
            .set_len_sparse(new_size)
            .map_err(|e| staging_code(&e))?;
        ws.file_len = new_size;
        Ok(())
    }

    fn do_fallocate(&self, ino: Ino, offset: u64, length: u64, mode: i32) -> Result<(), Code> {
        let keep_size = mode & libc::FALLOC_FL_KEEP_SIZE != 0;
        let punch = mode & libc::FALLOC_FL_PUNCH_HOLE != 0;
        let zero = mode & libc::FALLOC_FL_ZERO_RANGE != 0;
        let supported =
            libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_ZERO_RANGE;
        if mode & !supported != 0 || (punch && !keep_size) || (punch && zero) {
            return Err(Code::NotSupported);
        }
        let end = offset.checked_add(length).ok_or(Code::FileTooBig)?;
        // Held across the truncate and the boundary writes below (the
        // lock is re-entrant on this thread).
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let old_size = self
            .writes
            .lock(ino)
            .get(&ino)
            .map(|state| state.file_len)
            .unwrap_or(manifest.file_len);
        let new_size = if keep_size {
            old_size
        } else {
            old_size.max(end)
        };
        // Gate growth before any staging mutation: the zero-range branch
        // below extends the file itself and never reaches `truncate`.
        if new_size > old_size {
            self.quota_check(ino, new_size)?;
        }
        if !punch && !zero {
            if new_size != old_size {
                self.truncate(ino, new_size)?;
            }
            return Ok(());
        }

        let effective_end = end.min(new_size);
        if offset >= effective_end {
            return Ok(());
        }
        let chunk_size = u64::from(self.chunk_size);
        let full_start = offset.div_ceil(chunk_size);
        let full_end = effective_end / chunk_size;
        {
            let mut writes = self.writes.lock(ino);
            let ws = self.write_state(&mut writes, ino, &manifest)?;
            if new_size > ws.file_len {
                ws.staging
                    .set_len_sparse(new_size)
                    .map_err(|error| staging_code(&error))?;
                ws.file_len = new_size;
            }
            if full_start < full_end {
                ws.holes.mark_range(full_start, full_end);
                ws.zeroed.mark_range(full_start, full_end);
                let sealed: Vec<_> = ws
                    .sealed
                    .keys()
                    .copied()
                    .filter(|index| *index >= full_start && *index < full_end)
                    .collect();
                for index in sealed {
                    self.unseal(ws, ino, index)?;
                }
                ws.staging
                    .punch_chunks(full_start, full_end, self.chunk_size);
            }
        }

        // Boundary chunks remain ordinary data chunks after zeroing the
        // selected bytes. At most two chunk-sized buffers are materialized.
        if full_start >= full_end {
            self.do_write(ino, offset, &vec![0; (effective_end - offset) as usize])?;
            return Ok(());
        }
        let first_boundary_end = effective_end.min(full_start * chunk_size);
        if offset < first_boundary_end {
            self.do_write(
                ino,
                offset,
                &vec![0; (first_boundary_end - offset) as usize],
            )?;
        }
        let last_boundary_start = offset.max(full_end * chunk_size);
        if last_boundary_start < effective_end {
            self.do_write(
                ino,
                last_boundary_start,
                &vec![0; (effective_end - last_boundary_start) as usize],
            )?;
        }
        Ok(())
    }

    fn seek_sparse(&self, ino: Ino, offset: u64, whence: i32) -> Result<i64, Code> {
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let mut chunks = self.chunk_list(&manifest)?;
        let writes = self.writes.lock(ino);
        let file_len = if let Some(state) = writes.get(&ino) {
            let floor = state.base_floor(manifest.file_len);
            let cs = u64::from(self.chunk_size);
            chunks.retain(|index, _| !state.zeroed.contains(*index) && *index * cs < floor);
            chunks.extend(state.sealed.iter().map(|(index, hash)| (*index, *hash)));
            for index in state.staging.dirty_indices() {
                chunks.insert(index, ChunkHash([1; 32]));
            }
            state.file_len
        } else {
            manifest.file_len
        };
        if offset >= file_len {
            return Err(Code::NoDeviceOrAddress);
        }
        let chunk_size = u64::from(self.chunk_size);
        let start_index = offset / chunk_size;
        match whence {
            libc::SEEK_DATA => {
                if chunks.contains_key(&start_index) {
                    return Ok(offset as i64);
                }
                chunks
                    .range(start_index + 1..)
                    .next()
                    .map(|(&index, _)| (index * chunk_size) as i64)
                    .filter(|position| *position < file_len as i64)
                    .ok_or(Code::NoDeviceOrAddress)
            }
            libc::SEEK_HOLE => {
                if !chunks.contains_key(&start_index) {
                    return Ok(offset as i64);
                }
                let mut index = start_index + 1;
                while chunks.contains_key(&index) {
                    index += 1;
                }
                Ok((index * chunk_size).min(file_len) as i64)
            }
            _ => Err(Code::Invalid),
        }
    }
}
