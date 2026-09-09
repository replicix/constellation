// Included by fusefs.rs — the fuser::Filesystem implementation.

/// POSIX NAME_MAX; the metadata plane accepts longer, so enforce here.
const NAME_MAX: usize = 255;

/// Returns the borrowed name or replies ENAMETOOLONG / EINVAL.
macro_rules! checked_name {
    ($name:expr, $reply:expr) => {{
        let n = $name.to_string_lossy();
        if n.as_bytes().len() > NAME_MAX {
            $reply.error(Errno::from_i32(libc::ENAMETOOLONG));
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
            $reply.error(Errno::from_i32(libc::EROFS));
            return;
        }
        if let Err(e) = $self.require_lease_for($ino) {
            $reply.error(Errno::from_i32(e));
            return;
        }
    };
}

impl Filesystem for ConstellationFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let threads = crate::parallelism::thread_plan();
        let _ = config.set_max_background(threads.fuse_max_background());
        let _ = config.set_congestion_threshold(threads.fuse_congestion_threshold());
        if threads.fuse > 1 {
            // Older kernels may not advertise this capability. Multi-reader
            // dispatch still works; only directory operations remain ordered.
            let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);
        }
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(name, reply);
        match self.lookup_synthetic(parent, &name) {
            Ok(Some((_ino, attr))) => {
                reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                reply.error(Errno::from_i32(error));
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
                reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0));
                return;
            }
            Ok(_) => {}
            Err(e) => {
                reply.error(Errno::from_i32(errno(&e)));
                return;
            }
        }
        match self.meta.lookup(parent, &name) {
            Ok(Some(attr)) => reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0)),
            Ok(None) => reply.error(Errno::from_i32(libc::ENOENT)),
            Err(e) => reply.error(Errno::from_i32(errno(&e))),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino = ino.0;
        let requested_ino = ino;
        let ino = self.real_ino(ino);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                reply.error(Errno::from_i32(libc::ESTALE));
            } else {
                let attr = self.visible_attr(self.synthetic_attr(ino, &node));
                reply.attr(&TTL, &to_fuse_attr(&attr));
            }
            return;
        }
        let attr = self
            .meta
            .getattr(ino)
            .and_then(|attr| {
                if attr.is_some() {
                    Ok(attr)
                } else {
                    self.meta.scratch_getattr(ino)
                }
            });
        match attr {
            Ok(Some(mut attr)) => {
                // Pending writes shadow the committed size.
                if let Some(ws) = self.writes.lock(ino).get(&ino) {
                    attr.size = ws.file_len;
                }
                let attr = if requested_ino == constellation_fs_core::types::ROOT_INO {
                    self.visible_attr(attr)
                } else {
                    attr
                };
                reply.attr(&TTL, &to_fuse_attr(&attr))
            }
            Ok(None) => reply.error(Errno::from_i32(libc::ENOENT)),
            Err(e) => reply.error(Errno::from_i32(errno(&e))),
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
        let ino = ino.0;
        let ino = self.real_ino(ino);
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
            if let Err(error) = self.truncate(ino, new_size) {
                reply.error(Errno::from_i32(error));
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
                    .map_err(|error| errno(&error))?
                    .ok_or(libc::ENOENT)
            });
        match result {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr)),
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                reply.error(Errno::from_i32(libc::ESTALE));
            } else if let SyntheticNode::Frozen {
                kind: InodeKind::Symlink,
                target: Some(target),
                ..
            } = node
            {
                reply.data(target.as_bytes());
            } else {
                reply.error(Errno::from_i32(libc::EINVAL));
            }
            return;
        }
        match self.meta.readlink(ino) {
            Ok(Some(target)) => reply.data(target.as_bytes()),
            Ok(None) => reply.error(Errno::from_i32(libc::EINVAL)),
            Err(e) => reply.error(Errno::from_i32(errno(&e))),
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
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(name, reply);
        let ino = match self.meta.allocate_ino() {
            Ok(ino) => ino,
            Err(e) => return reply.error(Errno::from_i32(errno(&e))),
        };
        if self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some()
        {
            return match self
                .meta
                .scratch_mkdir(parent, &name, ino, mode, req.uid(), req.gid())
            {
                Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0)),
                Err(e) => reply.error(Errno::from_i32(errno(&e))),
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
                Ok(Some(attr)) => reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0)),
                Ok(None) => reply.error(Errno::from_i32(libc::EIO)),
                Err(e) => reply.error(Errno::from_i32(errno(&e))),
            },
            Err(e) => reply.error(Errno::from_i32(e)),
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
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(name, reply);
        let kind = match mode & libc::S_IFMT {
            libc::S_IFREG | 0 => InodeKind::File,
            libc::S_IFIFO => InodeKind::Fifo,
            libc::S_IFSOCK => InodeKind::Socket,
            libc::S_IFBLK => InodeKind::BlockDev,
            libc::S_IFCHR => InodeKind::CharDev,
            _ => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let ino = match self.meta.allocate_ino() {
            Ok(ino) => ino,
            Err(e) => return reply.error(Errno::from_i32(errno(&e))),
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
                rdev: rdev as u64,
            }
        };
        match self.mutate_op(parent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0)),
                Ok(None) => reply.error(Errno::from_i32(libc::EIO)),
                Err(e) => reply.error(Errno::from_i32(errno(&e))),
            },
            Err(e) => reply.error(Errno::from_i32(e)),
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
        let ino = ino.0;
        let newparent = newparent.0;
        let ino = self.real_ino(ino);
        let newparent = self.real_ino(newparent);
        let name = checked_name!(newname, reply);
        let op = constellation_meta::MutateOp::Link {
            ino,
            parent: newparent,
            name: name.into_owned(),
        };
        match self.mutate_op(newparent, op) {
            Ok(()) => match self.meta.getattr(ino) {
                Ok(Some(attr)) => reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0)),
                Ok(None) => reply.error(Errno::from_i32(libc::ENOENT)),
                Err(e) => reply.error(Errno::from_i32(errno(&e))),
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(name, reply);
        let ino = match self.meta.allocate_ino() {
            Ok(ino) => ino,
            Err(e) => return reply.error(Errno::from_i32(errno(&e))),
        };
        let result = if self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some()
        {
            self.meta
                .scratch_create(parent, &name, ino, mode, req.uid(), req.gid())
                .map_err(|e| errno(&e))
        } else {
            self.mutate_op(
                parent,
                constellation_meta::MutateOp::Create {
                    parent,
                    name: name.into_owned(),
                    ino,
                    mode,
                    uid: req.uid(),
                    gid: req.gid(),
                },
            )
            .and_then(|()| {
                self.meta
                    .getattr(ino)
                    .map_err(|e| errno(&e))?
                    .ok_or(libc::EIO)
            })
        };
        match result {
            Ok(attr) => {
                *self.opens.lock().unwrap().entry(attr.ino).or_insert(0) += 1;
                reply.created(
                    &TTL,
                    &to_fuse_attr(&attr),
                    fuser::Generation(0),
                    FileHandle(attr.ino),
                    fuser::FopenFlags::empty(),
                )
            }
            Err(e) => reply.error(Errno::from_i32(e)),
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
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(link_name, reply);
        let target = target.to_string_lossy();
        let ino = match self.meta.allocate_ino() {
            Ok(ino) => ino,
            Err(e) => return reply.error(Errno::from_i32(errno(&e))),
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
                Ok(Some(attr)) => reply.entry(&TTL, &to_fuse_attr(&attr), fuser::Generation(0)),
                Ok(None) => reply.error(Errno::from_i32(libc::EIO)),
                Err(e) => reply.error(Errno::from_i32(errno(&e))),
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(name, reply);
        let target = self.meta.lookup(parent, &name);
        let result = if (self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some())
            && self.meta.scratch_lookup(parent, &name).ok().flatten().is_some()
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
                    Err(libc::ENOTEMPTY)
                }
                Ok(Some(_)) => self
                    .meta
                    .scratch_unlink(parent, &name)
                    .map_err(|e| errno(&e)),
                Ok(None) => Err(libc::ENOENT),
                Err(e) => Err(errno(&e)),
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
                // No open handles anywhere (single node): reap now.
                if let Ok(Some(attr)) = target {
                    let opens = self.opens.lock().unwrap();
                    if opens.get(&attr.ino).copied().unwrap_or(0) == 0 {
                        let _ = self.meta.reap_orphan(attr.ino);
                    }
                }
                reply.ok()
            }
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent = parent.0;
        let parent = self.real_ino(parent);
        let name = checked_name!(name, reply);
        let result = if (self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some())
            && self.meta.scratch_lookup(parent, &name).ok().flatten().is_some()
        {
            match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr)) if attr.kind == InodeKind::Dir => Err(libc::EISDIR),
                Ok(Some(_)) => self
                    .meta
                    .scratch_unlink(parent, &name)
                    .map_err(|e| errno(&e)),
                Ok(None) => Err(libc::ENOENT),
                Err(e) => Err(errno(&e)),
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
            Err(e) => reply.error(Errno::from_i32(e)),
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
        let parent = parent.0;
        let newparent = newparent.0;
        let parent = self.real_ino(parent);
        let newparent = self.real_ino(newparent);
        let name = name.to_string_lossy().into_owned();
        let newname = newname.to_string_lossy().into_owned();
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
            return match self
                .meta
                .scratch_rename(parent, &name, newparent, &newname)
            {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(Errno::from_i32(errno(&error))),
            };
        }
        if dst_scratch {
            reply.error(Errno::from_i32(libc::EXDEV));
            return;
        }
        if src_scratch {
            let attr = match self.meta.scratch_lookup(parent, &name) {
                Ok(Some(attr)) => attr,
                Ok(None) => {
                    reply.error(Errno::from_i32(libc::ENOENT));
                    return;
                }
                Err(error) => {
                    reply.error(Errno::from_i32(errno(&error)));
                    return;
                }
            };
            if attr.kind != InodeKind::File {
                reply.error(Errno::from_i32(libc::EXDEV));
                return;
            }
            if let Err(error) = self.flush_inode(attr.ino, true) {
                reply.error(Errno::from_i32(error));
                return;
            }
            if let Err(error) = self.drain_inode(attr.ino) {
                reply.error(Errno::from_i32(error));
                return;
            }
            let manifest = match self.meta.scratch_manifest(attr.ino) {
                Ok(Some(manifest)) => manifest,
                Ok(None) => Manifest::empty(self.chunk_size).encode(),
                Err(error) => {
                    reply.error(Errno::from_i32(errno(&error)));
                    return;
                }
            };
            let xattrs = match self.meta.scratch_xattrs(attr.ino) {
                Ok(xattrs) => xattrs,
                Err(error) => {
                    reply.error(Errno::from_i32(errno(&error)));
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
                    Err(error) => reply.error(Errno::from_i32(errno(&error))),
                },
                Err(error) => reply.error(Errno::from_i32(error)),
            };
        }
        let src_part = self.meta.partition_of(parent).unwrap_or_else(|_| "p0".into());
        let dst_part = self
            .meta
            .partition_of(newparent)
            .unwrap_or_else(|_| "p0".into());
        // Canonical lock order: sort by partition id so two concurrent
        // cross-partition renames cannot deadlock.
        let (first, second) = if src_part <= dst_part {
            (parent, newparent)
        } else {
            (newparent, parent)
        };
        if src_part == dst_part {
            let op = constellation_meta::MutateOp::Rename {
                parent,
                name,
                new_parent: newparent,
                new_name: newname,
            };
            return match self.mutate_op(parent, op) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(Errno::from_i32(e)),
            };
        }
        gate!(self, first, reply);
        if first != second {
            if let Err(e) = self.require_lease_for(second) {
                reply.error(Errno::from_i32(e));
                return;
            }
        }
        let result = self
            .meta
            .rename_xpart(parent, &name, newparent, &newname, &src_part, &dst_part);
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(errno(&e))),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if let Some(node) = self.synthetic_node(ino) {
            if !self.synthetic_active(&node) {
                reply.error(Errno::from_i32(libc::ESTALE));
            } else if matches!(
                node,
                SyntheticNode::Frozen {
                    kind: InodeKind::File,
                    ..
                }
            ) {
                reply.opened(FileHandle(ino), fuser::FopenFlags::empty());
            } else {
                reply.error(Errno::from_i32(libc::EISDIR));
            }
            return;
        }
        let attr = self
            .meta
            .getattr(ino)
            .and_then(|attr| {
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
            Ok(None) => reply.error(Errno::from_i32(libc::ENOENT)),
            Err(e) => reply.error(Errno::from_i32(errno(&e))),
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
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if Self::is_synthetic(ino) {
            match self.read_frozen(ino, offset, size as u64) {
                Ok(data) => reply.data(&data),
                Err(error) => reply.error(Errno::from_i32(error)),
            }
            return;
        }
        match self.do_read(ino, offset, size as u64) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(Errno::from_i32(e)),
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
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if Self::is_synthetic(ino) {
            reply.error(Errno::from_i32(libc::EROFS));
            return;
        }
        match self.do_write(ino, offset, data) {
            Ok(n) if flags.0 & (libc::O_SYNC | libc::O_DSYNC) != 0 => {
                match self.flush_inode(ino, true) {
                    Ok(()) => reply.written(n),
                    Err(error) => reply.error(Errno::from_i32(error)),
                }
            }
            Ok(n) => reply.written(n),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn flush(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, _lock_owner: LockOwner, reply: ReplyEmpty) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        match self.flush_inode(ino, false) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn fsync(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        match self.flush_inode(ino, true) {
            Ok(()) => match self.sync_barrier(ino) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(Errno::from_i32(e)),
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        if Self::is_synthetic(ino) {
            reply.ok();
            return;
        }
        let flush_result = self.flush_inode(ino, flags.0 & (libc::O_SYNC | libc::O_DSYNC) != 0);
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
            Err(e) => reply.error(Errno::from_i32(e)),
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
        let ino = ino.0;
        let visible_ino = ino;
        let ino = self.real_ino(ino);
        if Self::is_synthetic(ino) {
            let entries = match self.synthetic_entries(ino) {
                Ok(entries) => entries,
                Err(error) => return reply.error(Errno::from_i32(error)),
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
        let entries = match if self.meta.is_scratch_dir(ino).unwrap_or(false)
            || self.meta.scratch_getattr(ino).ok().flatten().is_some()
        {
            self.meta.scratch_readdir(ino)
        } else {
            self.meta.readdir(ino)
        } {
            Ok(e) => e,
            Err(e) => return reply.error(Errno::from_i32(errno(&e))),
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
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let name = match checked_xattr_name(req, name) {
            Ok(name) => name,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        if virtual_xattr(&name) {
            reply.error(Errno::from_i32(libc::EPERM));
            return;
        }
        if Self::is_synthetic(ino) {
            reply.error(Errno::from_i32(libc::EROFS));
            return;
        }
        if value.len() > 64 * 1024 {
            reply.error(Errno::from_i32(libc::E2BIG));
            return;
        }
        if position != 0 {
            reply.error(Errno::from_i32(libc::EINVAL));
            return;
        }
        let mode = match flags {
            0 => constellation_meta::SetXattrMode::Set,
            libc::XATTR_CREATE => constellation_meta::SetXattrMode::Create,
            libc::XATTR_REPLACE => constellation_meta::SetXattrMode::Replace,
            _ => return reply.error(Errno::from_i32(libc::EINVAL)),
        };
        let wire_mode = match mode {
            constellation_meta::SetXattrMode::Create => 1,
            constellation_meta::SetXattrMode::Replace => 2,
            constellation_meta::SetXattrMode::Set => 0,
        };
        // Scratch files are node-private (scratch-directories.md) and
        // live outside the shared inode/xattr tables until `Publish`, so
        // their xattrs are staged locally here rather than mutated
        // through the lease-gated shared path.
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_set_xattr(ino, &name, value, mode) {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(Errno::from_i32(errno(&error))),
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
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn getxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let name = match checked_xattr_name(req, name) {
            Ok(name) => name,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let value = if virtual_xattr(&name) {
            let aggregate = if Self::is_synthetic(ino) {
                self.synthetic_recursive_size(ino)
            } else {
                self.meta.recursive_size(ino).map_err(|error| errno(&error))
            };
            match aggregate {
                Ok((rsize, rcount)) => {
                    if name == RSIZE_XATTR {
                        rsize.to_string().into_bytes()
                    } else {
                        rcount.to_string().into_bytes()
                    }
                }
                Err(error) => return reply.error(Errno::from_i32(error)),
            }
        } else {
            let result = if Self::is_synthetic(ino) {
                self.synthetic_xattrs(ino).map(|attrs| {
                    attrs
                        .into_iter()
                        .find(|(key, _)| key == &name)
                        .map(|x| x.1)
                })
            } else if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
                self.meta
                    .scratch_get_xattr(ino, &name)
                    .map_err(|error| errno(&error))
            } else {
                self.meta
                    .get_xattr(ino, &name)
                    .map_err(|error| errno(&error))
            };
            match result {
                Ok(Some(value)) => value,
                Ok(None) => return reply.error(Errno::from_i32(libc::ENODATA)),
                Err(error) => return reply.error(Errno::from_i32(error)),
            }
        };
        reply_xattr(value, size, reply);
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let names = if Self::is_synthetic(ino) {
            self.synthetic_xattrs(ino)
                .map(|attrs| attrs.into_iter().map(|(name, _)| name).collect())
        } else if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            self.meta
                .scratch_list_xattrs(ino)
                .map_err(|error| errno(&error))
        } else {
            self.meta
                .list_xattrs(ino)
                .map_err(|error| errno(&error))
        };
        let mut names: Vec<String> = match names {
            Ok(names) => names,
            Err(error) => return reply.error(Errno::from_i32(error)),
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

    fn removexattr(
        &self,
        req: &Request,
        ino: INodeNo,
        name: &OsStr,
        reply: ReplyEmpty,
    ) {
        let ino = ino.0;
        let ino = self.real_ino(ino);
        let name = match checked_xattr_name(req, name) {
            Ok(name) => name,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        if virtual_xattr(&name) {
            reply.error(Errno::from_i32(libc::EPERM));
            return;
        }
        if Self::is_synthetic(ino) {
            reply.error(Errno::from_i32(libc::EROFS));
            return;
        }
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return match self.meta.scratch_remove_xattr(ino, &name) {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(Errno::from_i32(errno(&error))),
            };
        }
        match self.mutate_op(
            ino,
            constellation_meta::MutateOp::RemoveXattr { ino, name },
        ) {
            Ok(()) => {
                self.nudge_sync();
                reply.ok()
            }
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: fuser::ReplyStatfs) {
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
        let ino = ino.0;
        let ino = self.real_ino(ino);
        gate!(self, ino, reply);
        if length == 0 {
            reply.error(Errno::from_i32(libc::EINVAL));
            return;
        }
        match self.do_fallocate(ino, offset, length, mode) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(Errno::from_i32(error)),
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
        let ino = ino.0;
        if offset < 0 {
            reply.error(Errno::from_i32(libc::ENXIO));
            return;
        }
        match self.seek_sparse(self.real_ino(ino), offset as u64, whence) {
            Ok(position) => reply.offset(position),
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }
}

fn checked_xattr_name(req: &Request, name: &OsStr) -> Result<String, i32> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return Err(libc::ERANGE);
    }
    let name = std::str::from_utf8(bytes).map_err(|_| libc::EINVAL)?;
    if name.starts_with("user.") {
        return Ok(name.to_string());
    }
    if name.starts_with("trusted.") {
        // FUSE does not carry Linux capability bits. Treat uid 0 as the
        // kernel-authenticated privileged caller and refuse everyone else.
        return if req.uid() == 0 {
            Ok(name.to_string())
        } else {
            Err(libc::EPERM)
        };
    }
    Err(libc::ENOTSUP)
}

fn reply_xattr(value: Vec<u8>, size: u32, reply: ReplyXattr) {
    if size == 0 {
        reply.size(value.len() as u32);
    } else if (size as usize) < value.len() {
        reply.error(Errno::from_i32(libc::ERANGE));
    } else {
        reply.data(&value);
    }
}

impl ConstellationFs {
    fn do_read(&self, ino: Ino, offset: u64, size: u64) -> Result<Vec<u8>, i32> {
        // Serve pending (unflushed) state when present so read-after-write
        // within an open handle is coherent. Held for the whole read: the
        // FUSE dispatch already serializes ops per-request, and a chunk
        // read is bounded (a few MiB), same cost as the old full-clone.
        let mut writes = self.writes.lock(ino);
        let ws = writes.get_mut(&ino);
        let manifest = self.load_manifest(ino)?;
        let attr = self.meta.getattr(ino).map_err(|e| errno(&e))?;
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
        self.prefetch.on_read(ino, offset, len, self.chunk_size, &hashes);
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut out = Vec::with_capacity(len as usize);
        for slice in layout.slices(offset, len) {
            let full_len = layout.chunk_len(file_len, slice.index);
            let chunk: Vec<u8> = match &ws {
                Some(w) if w.sealed.contains_key(&slice.index) => self
                    .cache
                    .get(w.sealed.get(&slice.index).unwrap())
                    .map_err(|_| libc::EIO)?
                    .ok_or(libc::EIO)?,
                Some(w) if w.staging.is_dirty(slice.index) => {
                    let mut buf = vec![0u8; full_len as usize];
                    w.staging
                        .read_at(slice.index * self.chunk_size as u64, &mut buf)
                        .map_err(|e| staging_errno(&e))?;
                    buf
                }
                _ => self.read_committed_chunk(ino, &hashes, slice.index)?,
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
    ) -> Result<Vec<u8>, i32> {
        match hashes.get(&idx) {
            Some(h) => self.fetch_chunk_for_inode(Some(ino), h),
            None => Ok(Vec::new()),
        }
    }

    fn do_write(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32, i32> {
        if data.is_empty() {
            return Ok(0);
        }
        let dirty = self.cache.dirty_bytes();
        let budget = self.cache.usage().budget;
        match crate::writeback::throttle_delay(dirty, budget) {
            Ok(delay) if !delay.is_zero() => std::thread::sleep(delay),
            Ok(_) => {}
            Err(()) => return Err(libc::ENOSPC),
        }
        match crate::writeback::throttle_delay(
            self.staging_budget.used(),
            self.staging_budget.budget(),
        ) {
            Ok(delay) if !delay.is_zero() => std::thread::sleep(delay),
            Ok(_) => {}
            Err(()) => {
                // Staging reservations grow in chunk-sized steps, so a
                // tiny budget can cross the soft-pressure band in one
                // write. Preserve observable backpressure before ENOSPC.
                std::thread::sleep(Duration::from_millis(100));
                return Err(libc::ENOSPC);
            }
        }
        let manifest = self.load_manifest(ino)?;
        let hashes = self.chunk_list(&manifest)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut writes = self.writes.lock(ino);
        let ws = self.write_state(&mut writes, ino, &manifest)?;
        let write_end = offset + data.len() as u64;
        let new_file_len = ws.file_len.max(write_end);
        self.quota_check(ino, new_file_len)?;
        ws.staging
            .set_len_sparse(new_file_len)
            .map_err(|e| staging_errno(&e))?;
        let mut consumed = 0usize;
        for slice in layout.slices(offset, data.len() as u64) {
            let full_len = layout.chunk_len(new_file_len, slice.index);
            let is_whole_chunk = slice.offset == 0 && slice.len == full_len;
            let chunk_start = slice.index * self.chunk_size as u64;
            let sealed = ws.sealed.remove(&slice.index);
            ws.holes.clear(slice.index);
            ws.staging
                .prepare_chunk(slice.index, self.chunk_size)
                .map_err(|e| staging_errno(&e))?;
            if let Some(old_hash) = sealed {
                self.meta
                    .cancel_pending_upload(&old_hash, ino)
                    .map_err(|error| errno(&error))?;
            }
            // A partial (not-whole-chunk) write into a chunk this open
            // handle has not touched yet must first seed the untouched
            // bytes from the committed content — otherwise they would
            // read back as a spurious hole (zero) instead of their real
            // pre-write value.
            if !is_whole_chunk && !ws.staging.is_dirty(slice.index) {
                let seed = match sealed {
                    Some(hash) => {
                        let mut data = self.cache.get(&hash).map_err(|_| libc::EIO)?.ok_or(libc::EIO)?;
                        data.resize(full_len as usize, 0);
                        data
                    }
                    None => self.committed_chunk_padded(&hashes, slice.index, full_len)?,
                };
                ws.staging
                    .write_at(chunk_start, &seed)
                    .map_err(|e| staging_errno(&e))?;
            }
            let write_start = chunk_start + u64::from(slice.offset);
            let write_end = write_start + u64::from(slice.len);
            ws.staging
                .write_at(
                    write_start,
                    &data[consumed..consumed + slice.len as usize],
                )
                .map_err(|e| staging_errno(&e))?;
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

    fn truncate(&self, ino: Ino, new_size: u64) -> Result<(), i32> {
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
    ) -> Result<(), i32> {
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let ws = self.write_state(writes, ino, manifest)?;
        self.quota_check(ino, new_size)?;
        if new_size < ws.file_len {
            // Drop dirty runs past the new end; `Staging::set_len`
            // (ftruncate) re-cuts the boundary chunk's on-disk bytes for
            // free if it was already staged. An untouched boundary chunk
            // is re-cut later, from the committed hash, by `flush_inode`.
            let keep = layout.chunk_count(new_size);
            let old_chunks = layout.chunk_count(ws.file_len);
            ws.staging.retain_dirty_below(keep);
            ws.staging.punch_chunks(keep, old_chunks, self.chunk_size);
        }
        ws.staging
            .set_len_sparse(new_size)
            .map_err(|e| staging_errno(&e))?;
        ws.file_len = new_size;
        Ok(())
    }

    fn do_fallocate(&self, ino: Ino, offset: u64, length: u64, mode: i32) -> Result<(), i32> {
        let keep_size = mode & libc::FALLOC_FL_KEEP_SIZE != 0;
        let punch = mode & libc::FALLOC_FL_PUNCH_HOLE != 0;
        let zero = mode & libc::FALLOC_FL_ZERO_RANGE != 0;
        let supported = libc::FALLOC_FL_KEEP_SIZE
            | libc::FALLOC_FL_PUNCH_HOLE
            | libc::FALLOC_FL_ZERO_RANGE;
        if mode & !supported != 0 || (punch && !keep_size) || (punch && zero) {
            return Err(libc::EOPNOTSUPP);
        }
        let end = offset.checked_add(length).ok_or(libc::EFBIG)?;
        let manifest = self.load_manifest(ino)?;
        let old_size = self.writes.lock(ino)
            .get(&ino)
            .map(|state| state.file_len)
            .unwrap_or(manifest.file_len);
        let new_size = if keep_size { old_size } else { old_size.max(end) };
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
                    .map_err(|error| staging_errno(&error))?;
                ws.file_len = new_size;
            }
            if full_start < full_end {
                ws.holes.mark_range(full_start, full_end);
                let sealed: Vec<_> = ws
                    .sealed
                    .keys()
                    .copied()
                    .filter(|index| *index >= full_start && *index < full_end)
                    .collect();
                for index in sealed {
                    if let Some(hash) = ws.sealed.remove(&index) {
                        self.meta
                            .cancel_pending_upload(&hash, ino)
                            .map_err(|error| errno(&error))?;
                    }
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
            self.do_write(ino, offset, &vec![0; (first_boundary_end - offset) as usize])?;
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

    fn seek_sparse(&self, ino: Ino, offset: u64, whence: i32) -> Result<i64, i32> {
        let manifest = self.load_manifest(ino)?;
        let mut chunks = self.chunk_list(&manifest)?;
        let writes = self.writes.lock(ino);
        let file_len = if let Some(state) = writes.get(&ino) {
            chunks.retain(|index, _| !state.holes.contains(*index));
            chunks.extend(state.sealed.iter().map(|(index, hash)| (*index, *hash)));
            for index in state.staging.dirty_indices() {
                chunks.insert(index, ChunkHash([1; 32]));
            }
            state.file_len
        } else {
            manifest.file_len
        };
        if offset >= file_len {
            return Err(libc::ENXIO);
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
                    .ok_or(libc::ENXIO)
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
            _ => Err(libc::EINVAL),
        }
    }
}
