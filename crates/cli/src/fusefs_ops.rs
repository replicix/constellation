// Included by fusefs.rs — the fuser::Filesystem implementation.

/// POSIX NAME_MAX; the metadata plane accepts longer, so enforce here.
const NAME_MAX: usize = 255;

/// Returns the borrowed name or replies ENAMETOOLONG / EINVAL.
macro_rules! checked_name {
    ($name:expr, $reply:expr) => {{
        let n = $name.to_string_lossy();
        if n.as_bytes().len() > NAME_MAX {
            $reply.error(libc::ENAMETOOLONG);
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
        if let Err(e) = $self.require_lease_for($ino) {
            $reply.error(e);
            return;
        }
    };
}

impl Filesystem for ConstellationFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = checked_name!(name, reply);
        match self.meta.lookup(parent, &name) {
            Ok(Some(attr)) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.meta.getattr(ino) {
            Ok(Some(mut attr)) => {
                // Pending writes shadow the committed size.
                if let Some(ws) = self.writes.lock().unwrap().get(&ino) {
                    attr.size = ws.file_len;
                }
                reply.attr(&TTL, &to_fuse_attr(&attr))
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => reply.error(errno(&e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Truncate/extend goes through write state so data and metadata
        // commit together at flush.
        gate!(self, ino, reply);
        if let Some(new_size) = size {
            if let Err(e) = self.truncate(ino, new_size) {
                reply.error(e);
                return;
            }
        }
        let atime_ns = _atime.map(time_or_now_ns);
        let mtime_ns = mtime.map(time_or_now_ns);
        match self.meta.setattr(ino, mode, uid, gid, size, atime_ns, mtime_ns) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.meta.readlink(ino) {
            Ok(Some(target)) => reply.data(target.as_bytes()),
            Ok(None) => reply.error(libc::EINVAL),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = checked_name!(name, reply);
        gate!(self, parent, reply);
        match self.meta.mkdir(parent, &name, mode, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = checked_name!(name, reply);
        gate!(self, parent, reply);
        let kind = match mode & libc::S_IFMT {
            libc::S_IFREG | 0 => {
                // Some callers use mknod for regular files.
                match self.meta.create(parent, &name, mode & 0o7777, req.uid(), req.gid()) {
                    Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
                    Err(e) => reply.error(errno(&e)),
                }
                return;
            }
            libc::S_IFIFO => InodeKind::Fifo,
            libc::S_IFSOCK => InodeKind::Socket,
            libc::S_IFBLK => InodeKind::BlockDev,
            libc::S_IFCHR => InodeKind::CharDev,
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        match self.meta.mknod(
            parent,
            &name,
            kind,
            mode & 0o7777,
            req.uid(),
            req.gid(),
            rdev as u64,
        ) {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let name = checked_name!(newname, reply);
        gate!(self, ino, reply);
        match self.meta.link(ino, newparent, &name) {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name = checked_name!(name, reply);
        gate!(self, parent, reply);
        match self.meta.create(parent, &name, mode, req.uid(), req.gid()) {
            Ok(attr) => {
                *self.opens.lock().unwrap().entry(attr.ino).or_insert(0) += 1;
                reply.created(&TTL, &to_fuse_attr(&attr), 0, attr.ino, 0)
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let name = checked_name!(link_name, reply);
        gate!(self, parent, reply);
        let target = target.to_string_lossy();
        match self.meta.symlink(parent, &name, &target, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = checked_name!(name, reply);
        gate!(self, parent, reply);
        let target = self.meta.lookup(parent, &name);
        match self.meta.unlink(parent, &name) {
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
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = checked_name!(name, reply);
        gate!(self, parent, reply);
        match self.meta.rmdir(parent, &name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name = name.to_string_lossy();
        let newname = newname.to_string_lossy();
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
        gate!(self, first, reply);
        if first != second {
            if let Err(e) = self.require_lease_for(second) {
                reply.error(e);
                return;
            }
        }
        let result = if src_part == dst_part {
            self.meta.rename(parent, &name, newparent, &newname)
        } else {
            self.meta
                .rename_xpart(parent, &name, newparent, &newname, &src_part, &dst_part)
        };
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.meta.getattr(ino) {
            Ok(Some(_)) => {
                *self.opens.lock().unwrap().entry(ino).or_insert(0) += 1;
                reply.opened(ino, 0)
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.do_read(ino, offset as u64, size as u64) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.do_write(ino, offset as u64, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(e),
        }
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        match self.flush_inode(ino) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        match self.flush_inode(ino) {
            Ok(()) => match self.sync_barrier() {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            },
            Err(e) => reply.error(e),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let flush_result = self.flush_inode(ino);
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
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.meta.readdir(ino) {
            Ok(e) => e,
            Err(e) => return reply.error(errno(&e)),
        };
        // Stable cursor: "." = 1, ".." = 2, children from 3.
        let mut idx = offset;
        loop {
            let next = idx + 1;
            let full = match idx {
                0 => reply.add(ino, next, FileType::Directory, "."),
                1 => reply.add(ino, next, FileType::Directory, ".."),
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
                    reply.add(child.ino, next, ft, &child.name)
                }
            };
            if full {
                break;
            }
            idx = next;
        }
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        // Effectively unlimited backing store; block size mirrors blksize.
        let bsize: u32 = 131072;
        let huge = u64::MAX / bsize as u64 / 2;
        reply.statfs(huge, huge, huge, 0, u64::MAX / 2, bsize, 255, bsize);
    }
}

impl ConstellationFs {
    fn do_read(&mut self, ino: Ino, offset: u64, size: u64) -> Result<Vec<u8>, i32> {
        // Serve pending (unflushed) state when present so read-after-write
        // within an open handle is coherent. Held for the whole read: the
        // FUSE dispatch already serializes ops per-request, and a chunk
        // read is bounded (a few MiB), same cost as the old full-clone.
        let mut writes = self.writes.lock().unwrap();
        let ws = writes.get_mut(&ino);
        let manifest = self.load_manifest(ino)?;
        let committed_len = self
            .meta
            .getattr(ino)
            .map_err(|e| errno(&e))?
            .map(|a| a.size)
            .unwrap_or(manifest.file_len);
        let file_len = ws.as_ref().map(|w| w.file_len).unwrap_or(committed_len);
        if offset >= file_len {
            return Ok(Vec::new());
        }
        let len = size.min(file_len - offset);
        let hashes = self.chunk_list(&manifest)?;
        // Kick sequential readahead for upcoming committed chunks.
        self.prefetch.on_read(ino, offset, len, self.chunk_size, &hashes);
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut out = Vec::with_capacity(len as usize);
        for slice in layout.slices(offset, len) {
            let full_len = layout.chunk_len(file_len, slice.index);
            let chunk: Vec<u8> = match &ws {
                Some(w) if w.staging.is_dirty(slice.index) => {
                    let mut buf = vec![0u8; full_len as usize];
                    w.staging
                        .read_at(slice.index * self.chunk_size as u64, &mut buf)
                        .map_err(|e| staging_errno(&e))?;
                    buf
                }
                _ => self.read_committed_chunk(&hashes, slice.index)?,
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

    fn read_committed_chunk(&self, hashes: &[ChunkHash], idx: u64) -> Result<Vec<u8>, i32> {
        match hashes.get(idx as usize) {
            Some(h) => self.fetch_chunk(h),
            None => Ok(Vec::new()),
        }
    }

    fn do_write(&mut self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32, i32> {
        if data.is_empty() {
            return Ok(0);
        }
        let manifest = self.load_manifest(ino)?;
        let hashes = self.chunk_list(&manifest)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut writes = self.writes.lock().unwrap();
        let ws = self.write_state(&mut writes, ino, &manifest)?;
        let new_file_len = ws.file_len.max(offset + data.len() as u64);
        let mut consumed = 0usize;
        for slice in layout.slices(offset, data.len() as u64) {
            let full_len = layout.chunk_len(new_file_len, slice.index);
            let is_whole_chunk = slice.offset == 0 && slice.len == full_len;
            let chunk_start = slice.index * self.chunk_size as u64;
            // A partial (not-whole-chunk) write into a chunk this open
            // handle has not touched yet must first seed the untouched
            // bytes from the committed content — otherwise they would
            // read back as a spurious hole (zero) instead of their real
            // pre-write value.
            if !is_whole_chunk && !ws.staging.is_dirty(slice.index) {
                let seed = self.committed_chunk_padded(&hashes, slice.index, full_len)?;
                ws.staging
                    .write_at(chunk_start, &seed)
                    .map_err(|e| staging_errno(&e))?;
            }
            ws.staging
                .write_at(
                    chunk_start + slice.offset as u64,
                    &data[consumed..consumed + slice.len as usize],
                )
                .map_err(|e| staging_errno(&e))?;
            ws.staging.mark_dirty(slice.index);
            consumed += slice.len as usize;
        }
        ws.file_len = new_file_len;
        Ok(data.len() as u32)
    }

    fn truncate(&mut self, ino: Ino, new_size: u64) -> Result<(), i32> {
        let manifest = self.load_manifest(ino)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut writes = self.writes.lock().unwrap();
        let ws = self.write_state(&mut writes, ino, &manifest)?;
        if new_size < ws.file_len {
            // Drop dirty runs past the new end; `Staging::set_len`
            // (ftruncate) re-cuts the boundary chunk's on-disk bytes for
            // free if it was already staged. An untouched boundary chunk
            // is re-cut later, from the committed hash, by `flush_inode`.
            let keep = layout.chunk_count(new_size);
            ws.staging.retain_dirty_below(keep);
        }
        ws.staging.set_len(new_size).map_err(|e| staging_errno(&e))?;
        ws.file_len = new_size;
        Ok(())
    }
}
