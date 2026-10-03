//! `create(2)` as POSIX has it, on a cluster (`View::create_or_open`).

use super::*;

/// How many times a non-`O_EXCL` create retries when the name it lost
/// to is gone again before it could be opened (a create/unlink storm);
/// past that, the last `EEXIST` stands.
pub(super) const CREATE_OR_OPEN_ATTEMPTS: usize = 8;

/// The kernel's `may_open` for an existing regular file opened with
/// `flags` (mode bits only: the mount enables no ACLs).
pub(super) fn may_open(caller: &Caller, attr: &FileAttr, flags: OpenFlags) -> bool {
    if caller.uid == 0 {
        return true;
    }
    let read = flags.contains(OpenFlags::READ);
    let write = flags.contains(OpenFlags::WRITE) || flags.contains(OpenFlags::TRUNC);
    let bits = if caller.uid == attr.uid {
        (attr.mode >> 6) & 7
    } else if caller.in_group(attr.gid) {
        (attr.mode >> 3) & 7
    } else {
        attr.mode & 7
    };
    (!read || bits & 4 != 0) && (!write || bits & 2 != 0)
}

impl View {
    /// `create(2)` as POSIX has it: create `name` in `parent`, or — when
    /// the name exists and `flags` lacks `O_EXCL` — open what is there.
    /// `Ok((attr, created))`.
    ///
    /// The kernel sends `create` only after its own lookup found no
    /// entry, but on a cluster that lookup can be stale: another node's
    /// create of the same name lands between it and this call (at the
    /// sequencer, which refuses our forwarded op `Exists`, or in this
    /// replica already, where the local execution does), and the kernel
    /// surfaces whatever this returns. `EEXIST` there broke every
    /// "create if missing" opener racing another node (the OVH run's
    /// finding 1: three of four racing nodes got `EEXIST`; SQLite said
    /// "attempt to write a readonly database"). Every path — the
    /// sequencer's fast path, a delegate's, a forward, the S3 inbox —
    /// reports the lost race the same way, so it is handled here once:
    /// look the name up (after the refusal's read floor: the `Exists`
    /// hint, or the observed position, covers the winner's entry) and
    /// open it:
    /// - a regular file is opened here, with the permission check and
    ///   the `O_TRUNC` the kernel skips for a file it believes this call
    ///   created (`FMODE_CREATED`);
    /// - a directory is `EISDIR`, as `open(O_CREAT)` of one is;
    /// - anything else (a symlink to follow, a FIFO or device node the
    ///   kernel opens itself) is `ESTALE`: the kernel's `do_filp_open`
    ///   repeats the walk once with `LOOKUP_REVAL`, which looks the
    ///   now-existing entry up and opens it the ordinary way;
    /// - a name gone again (unlinked meanwhile) is created afresh.
    ///
    /// With `O_EXCL` the `EEXIST` stands.
    pub(crate) fn create_or_open(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        flags: OpenFlags,
        caller: &Caller,
    ) -> Result<(FileAttr, bool), Code> {
        let excl = flags.contains(OpenFlags::EXCL);
        let scratch = self.meta.is_scratch_dir(parent).unwrap_or(false)
            || self.meta.scratch_getattr(parent).ok().flatten().is_some();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let ino = self.meta.allocate_ino(parent).map_err(|e| e.code())?;
            let created = if scratch {
                self.meta
                    .scratch_create(parent, name, ino, mode, caller.uid, caller.gid)
                    .map_err(|e| e.code())
            } else {
                self.mutate_op(
                    parent,
                    constellation_meta::MutateOp::Create {
                        parent,
                        name: name.to_string(),
                        ino,
                        mode,
                        uid: caller.uid,
                        gid: caller.gid,
                    },
                )
                .and_then(|()| {
                    self.meta
                        .getattr(ino)
                        .map_err(|e| e.code())?
                        .ok_or(Code::Io)
                })
            };
            match created {
                Ok(attr) => return Ok((attr, true)),
                Err(Code::Exists) if !excl => {
                    if let Some(attr) = self.open_existing(parent, name, flags, caller, scratch)? {
                        tracing::debug!(
                            parent,
                            name,
                            ino = attr.ino,
                            attempt,
                            "create found its name existing; opened the file there"
                        );
                        return Ok((attr, false));
                    }
                    if attempt >= CREATE_OR_OPEN_ATTEMPTS {
                        return Err(Code::Exists);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// `setattr(size)` alone — what `ftruncate` and an `O_TRUNC` open
    /// send (`setattr` with only a size does exactly this).
    pub(super) fn setattr_size(&self, ino: Ino, size: u64) -> Result<(), Code> {
        // Plan 30 §M14: a truncation is a write (fenced under a lapsed
        // lock grant).
        if self.lock_fenced(ino) {
            return Err(Code::Io);
        }
        self.truncate(ino, size)?;
        match self.mutate_op(
            ino,
            constellation_meta::MutateOp::Setattr {
                ino,
                mode: None,
                uid: None,
                gid: None,
                size: Some(size),
                atime_ns: None,
                mtime_ns: None,
            },
        ) {
            // An unlinked-but-open file (see `View::setattr`).
            Err(Code::NotFound) if self.unlinked(ino) => self
                .meta
                .orphan_setattr(ino, None, None, None, Some(size), None, None)
                .map(|_| ())
                .map_err(|error| error.code()),
            other => other,
        }
    }

    /// The open half of [`Self::create_or_open`]: `Ok(None)` when `name`
    /// is not there (any more).
    pub(super) fn open_existing(
        &self,
        parent: Ino,
        name: &str,
        flags: OpenFlags,
        caller: &Caller,
        scratch: bool,
    ) -> Result<Option<FileAttr>, Code> {
        let found = if scratch {
            self.meta
                .scratch_lookup(parent, name)
                .map_err(|e| e.code())?
        } else {
            // The lookup's read wait (and under `--cto strict` its
            // freshness): the refusal raised this node's read floor to
            // the sequencer's state that holds the entry.
            self.strict_read(
                parent,
                true,
                Some(name),
                &[ReadKey::Dentry(parent, name.to_string())],
            );
            self.meta.lookup(parent, name).map_err(|e| e.code())?
        };
        let Some(attr) = found else {
            return Ok(None);
        };
        match attr.kind {
            InodeKind::File => {}
            InodeKind::Dir => return Err(Code::IsDir),
            _ => return Err(Code::Stale),
        }
        if !may_open(caller, &attr, flags) {
            return Err(Code::Access);
        }
        let ino = attr.ino;
        if !scratch {
            // The close-to-open point, as `open` has it.
            self.strict_read(ino, false, None, &[ReadKey::Ino(ino)]);
        }
        if flags.contains(OpenFlags::TRUNC) {
            // What the kernel's `handle_truncate` would have sent.
            self.setattr_size(ino, 0)?;
        }
        // The current attributes, a pending write's size included (as a
        // lookup reports them).
        self.current_attr(ino)
    }
}

/// The OVH run's finding 1: `create` without `O_EXCL` that finds its name
/// existing (another node's create landed after the kernel's lookup)
/// opens the file there; with `O_EXCL` it is `EEXIST`.
#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;

    const RDONLY: OpenFlags = OpenFlags::READ;
    const WRONLY: OpenFlags = OpenFlags::WRITE;
    const RDWR: OpenFlags = OpenFlags::READ.union(OpenFlags::WRITE);

    fn me() -> Caller {
        Caller::new(1000, 1000, None)
    }

    fn fs() -> (Arc<Meta>, View, tempfile::TempDir) {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (fs, dir) = super::super::quota_tests::test_fs(meta.clone());
        (meta, fs, dir)
    }

    #[test]
    fn an_absent_name_is_created() {
        let (_meta, fs, _dir) = fs();
        for flags in [RDWR, RDWR | OpenFlags::EXCL] {
            let name = format!("new-{}", flags.bits());
            let (attr, created) = fs
                .create_or_open(ROOT_INO, &name, 0o100644, flags, &me())
                .unwrap();
            assert!(created);
            assert_eq!(attr.kind, InodeKind::File);
            assert_eq!(
                fs.meta.lookup(ROOT_INO, &name).unwrap().unwrap().ino,
                attr.ino
            );
        }
    }

    #[test]
    fn a_lost_race_opens_the_winners_file_without_o_excl() {
        let (meta, fs, _dir) = fs();
        // The winner's create, already in this replica when ours runs.
        let won = meta.create(ROOT_INO, "db", 0o644, 1000, 1000).unwrap();
        for flags in [RDWR, WRONLY, RDONLY] {
            let (attr, created) = fs
                .create_or_open(ROOT_INO, "db", 0o100644, flags, &me())
                .unwrap();
            assert!(!created, "flags {flags:?}");
            assert_eq!(attr.ino, won.ino);
        }
        assert_eq!(
            fs.create_or_open(ROOT_INO, "db", 0o100644, RDWR | OpenFlags::EXCL, &me())
                .unwrap_err(),
            Code::Exists
        );
        // Nothing else was created under the name.
        assert_eq!(meta.lookup(ROOT_INO, "db").unwrap().unwrap().ino, won.ino);
    }

    #[test]
    fn o_trunc_truncates_the_file_it_found() {
        let (meta, fs, _dir) = fs();
        let won = meta.create(ROOT_INO, "f", 0o644, 1000, 1000).unwrap();
        meta.setattr(won.ino, None, None, None, Some(40), None, None)
            .unwrap();
        let (attr, created) = fs
            .create_or_open(ROOT_INO, "f", 0o100644, RDWR | OpenFlags::TRUNC, &me())
            .unwrap();
        assert!(!created);
        assert_eq!(attr.ino, won.ino);
        assert_eq!(attr.size, 0);
        assert_eq!(meta.getattr(won.ino).unwrap().unwrap().size, 0);
        // Without O_TRUNC the size is what it is (the truncating open's
        // release flushed first).
        fs.flush_inode(won.ino, false).unwrap();
        meta.setattr(won.ino, None, None, None, Some(7), None, None)
            .unwrap();
        let (attr, _) = fs
            .create_or_open(ROOT_INO, "f", 0o100644, RDWR, &me())
            .unwrap();
        assert_eq!(attr.size, 7);
    }

    #[test]
    fn a_directory_is_eisdir_and_a_symlink_or_node_is_estale() {
        let (meta, fs, _dir) = fs();
        meta.mkdir(ROOT_INO, "d", 0o755, 1000, 1000).unwrap();
        meta.symlink(ROOT_INO, "l", "target", 1000, 1000).unwrap();
        meta.mknod(
            ROOT_INO,
            "p",
            InodeKind::Fifo,
            0o644,
            1000,
            1000,
            Default::default(),
        )
        .unwrap();
        let open = |name: &str| {
            fs.create_or_open(ROOT_INO, name, 0o100644, RDWR, &me())
                .unwrap_err()
        };
        assert_eq!(open("d"), Code::IsDir);
        // The kernel walks again (`LOOKUP_REVAL`) and follows or opens it.
        assert_eq!(open("l"), Code::Stale);
        assert_eq!(open("p"), Code::Stale);
    }

    #[test]
    fn the_permission_check_the_kernel_skips_is_made() {
        let (meta, fs, _dir) = fs();
        meta.create(ROOT_INO, "mine", 0o600, 1000, 1000).unwrap();
        meta.create(ROOT_INO, "group-r", 0o640, 2000, 1000).unwrap();
        meta.create(ROOT_INO, "other-r", 0o644, 2000, 2000).unwrap();
        let open = |name: &str, flags: OpenFlags, who: &Caller| {
            fs.create_or_open(ROOT_INO, name, 0o100644, flags, who)
                .map(|(_, created)| created)
        };
        assert_eq!(open("mine", RDWR, &me()), Ok(false));
        let stranger = Caller::new(3000, 3000, None);
        assert_eq!(open("mine", RDONLY, &stranger), Err(Code::Access));
        assert_eq!(open("group-r", RDONLY, &me()), Ok(false));
        assert_eq!(open("group-r", RDWR, &me()), Err(Code::Access));
        assert_eq!(open("other-r", RDONLY, &me()), Ok(false));
        assert_eq!(open("other-r", WRONLY, &me()), Err(Code::Access));
        // O_TRUNC needs write permission even on a read-only open.
        assert_eq!(
            open("other-r", RDONLY | OpenFlags::TRUNC, &me()),
            Err(Code::Access)
        );
        let root = Caller::new(0, 0, None);
        assert_eq!(open("mine", RDWR, &root), Ok(false));
    }
}
