//! [`ControlVfs`]: the control protocol's file browser as just another
//! frontend of a [`View`] (plan 31 §9.7).
//!
//! The UI's file browser, upload/download and rename/delete drive the very
//! [`Vfs`] trait a kernel mount does, with the authenticated control
//! principal as the op's [`Caller`] — so they get every response barrier,
//! the write gate, name and xattr policy, quota and confinement for free,
//! instead of an ad hoc path of their own. What this adds is only the
//! frontend's own job: turning a path into inode lookups, and a call into
//! `open`/`read`/`release` sequences, completed synchronously through
//! [`Blocking`] responders (the router runs every browse call on a blocking
//! thread).
//!
//! The `browse.readdir`/`browse.inspect` methods keep the old
//! `ReadDir`/`Inspect` semantics (the metadata replica, with the manifest
//! summary) and do not come through here; everything added in C5 does.

use crate::view::View;
use constellation_control::proto::types::{FileStat, WriteResult, XattrOp, XattrResult};
use constellation_control::proto::ControlError;
use constellation_types::Code;
use constellation_vfs::{
    Attr, Blocking, Caller, CollectDir, Durability, Entry, Fh, FileKind, LockOwner, Name, OpCtx,
    OpKind, OpenFlags, OpenOwner, RenameFlags, SetAttr, SetXattrFlags, Vfs, VfsError, XattrName,
    ROOT_INO,
};
use std::sync::Arc;

/// The largest single `read` a browse call asks of the view.
const READ_SLICE: u32 = 1024 * 1024;

/// A path-addressed client of one [`View`], as one caller.
pub struct ControlVfs {
    view: Arc<View>,
    caller: Caller,
}

fn vfs_err(path: &str, e: VfsError) -> ControlError {
    let code: Code = e.code();
    ControlError::from(code).with_details(serde_json::json!({ "path": path }))
}

/// `/a//b/` → `["a", "b"]`; `..` and `.` are refused rather than resolved
/// (a control path names one object, it does not navigate).
fn components(path: &str) -> Result<Vec<&str>, ControlError> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    if parts.iter().any(|p| *p == "." || *p == "..") {
        return Err(ControlError::invalid(format!(
            "{path}: `.` and `..` are not allowed in a control path"
        )));
    }
    Ok(parts)
}

fn kind_name(kind: FileKind) -> String {
    format!("{kind:?}").to_lowercase()
}

impl ControlVfs {
    pub fn new(view: Arc<View>, caller: Caller) -> ControlVfs {
        ControlVfs { view, caller }
    }

    fn cx(&self, kind: OpKind) -> OpCtx<'_> {
        OpCtx::new(kind, &self.caller)
    }

    fn lookup(&self, parent: u64, name: &str, path: &str) -> Result<Entry, ControlError> {
        Blocking::run(|r| {
            self.view
                .lookup(&self.cx(OpKind::Lookup), parent, Name::new(name), r)
        })
        .map_err(|e| vfs_err(path, e))
    }

    /// The inode `path` names.
    fn resolve(&self, path: &str) -> Result<u64, ControlError> {
        let mut ino = ROOT_INO;
        for part in components(path)? {
            ino = self.lookup(ino, part, path)?.attr.ino;
        }
        Ok(ino)
    }

    /// The parent directory's inode and the final name.
    fn parent_of<'p>(&self, path: &'p str) -> Result<(u64, &'p str), ControlError> {
        let mut parts = components(path)?;
        let name = parts
            .pop()
            .ok_or_else(|| ControlError::invalid(format!("{path}: names the root")))?;
        let mut ino = ROOT_INO;
        for part in parts {
            ino = self.lookup(ino, part, path)?.attr.ino;
        }
        Ok((ino, name))
    }

    fn getattr(&self, ino: u64, path: &str) -> Result<Attr, ControlError> {
        Blocking::run(|r| self.view.getattr(&self.cx(OpKind::Getattr), ino, None, r))
            .map_err(|e| vfs_err(path, e))
    }

    fn file_stat(path: &str, a: &Attr) -> FileStat {
        FileStat {
            path: path.to_string(),
            ino: a.ino,
            kind: kind_name(a.kind),
            size: a.size,
            mode: a.mode,
            uid: a.uid,
            gid: a.gid,
            nlink: a.nlink,
            atime_ns: a.atime_ns,
            mtime_ns: a.mtime_ns,
            ctime_ns: a.ctime_ns,
            rdev: a.rdev,
        }
    }

    /// `browse.stat`.
    pub fn stat(&self, path: &str) -> Result<FileStat, ControlError> {
        let ino = self.resolve(path)?;
        Ok(Self::file_stat(path, &self.getattr(ino, path)?))
    }

    /// Open `ino`, run `f` with the handle, and release it whatever `f`
    /// answered (with the close-time flush first when it wrote).
    fn with_handle<T>(
        &self,
        ino: u64,
        fh: Fh,
        flags: OpenFlags,
        path: &str,
        f: impl FnOnce(Fh) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let result = f(fh);
        let owner = LockOwner(0);
        let flushed = if flags.contains(OpenFlags::WRITE) {
            Blocking::run(|r| self.view.flush(&self.cx(OpKind::Flush), ino, fh, owner, r))
                .map_err(|e| vfs_err(path, e))
        } else {
            Ok(())
        };
        let _ = Blocking::run(|r| {
            self.view
                .release(&self.cx(OpKind::Release), ino, fh, flags, None, r)
        });
        let value = result?;
        flushed?;
        Ok(value)
    }

    /// Up to `len` bytes of `path` from `offset` (to the end when `None`),
    /// handed to `sink` one slice at a time; `sink` answering `false` stops
    /// early (the consumer went away).
    pub fn read(
        &self,
        path: &str,
        offset: u64,
        len: Option<u64>,
        mut sink: impl FnMut(Vec<u8>) -> bool,
    ) -> Result<u64, ControlError> {
        let ino = self.resolve(path)?;
        let flags = OpenFlags::READ;
        let opened = Blocking::run(|r| {
            self.view
                .open(&self.cx(OpKind::Open), ino, flags, OpenOwner::NONE, r)
        })
        .map_err(|e| vfs_err(path, e))?;
        self.with_handle(ino, opened.fh, flags, path, |fh| {
            let mut off = offset;
            let end = len.map(|l| offset.saturating_add(l));
            let mut total = 0u64;
            loop {
                let want = match end {
                    Some(end) if off >= end => break,
                    Some(end) => (end - off).min(READ_SLICE as u64) as u32,
                    None => READ_SLICE,
                };
                let data = Blocking::run(|r| {
                    self.view
                        .read(&self.cx(OpKind::Read), ino, fh, off, want, r)
                })
                .map_err(|e| vfs_err(path, e))?;
                if data.is_empty() {
                    break;
                }
                let bytes = data.contiguous().into_owned();
                off += bytes.len() as u64;
                total += bytes.len() as u64;
                if !sink(bytes) {
                    break;
                }
            }
            Ok(total)
        })
    }

    /// `browse.write`.
    #[allow(clippy::too_many_arguments)]
    pub fn write(
        &self,
        path: &str,
        offset: u64,
        data: &[u8],
        create: bool,
        create_mode: Option<u32>,
        truncate: bool,
    ) -> Result<WriteResult, ControlError> {
        let (parent, name) = self.parent_of(path)?;
        let flags = OpenFlags::WRITE;
        let existing = match self.lookup(parent, name, path) {
            Ok(entry) => Some(entry.attr.ino),
            Err(e) if e.code == Some(Code::NotFound) && create => None,
            Err(e) => return Err(e),
        };
        let (ino, fh) = match existing {
            Some(ino) => {
                let opened = Blocking::run(|r| {
                    self.view
                        .open(&self.cx(OpKind::Open), ino, flags, OpenOwner::NONE, r)
                })
                .map_err(|e| vfs_err(path, e))?;
                (ino, opened.fh)
            }
            None => {
                let mode = constellation_vfs::types::mode::S_IFREG | create_mode.unwrap_or(0o644);
                let (entry, opened) = Blocking::run(|r| {
                    self.view.create(
                        &self.cx(OpKind::Create),
                        parent,
                        Name::new(name),
                        mode,
                        flags | OpenFlags::CREATE,
                        OpenOwner::NONE,
                        r,
                    )
                })
                .map_err(|e| vfs_err(path, e))?;
                (entry.attr.ino, opened.fh)
            }
        };
        self.with_handle(ino, fh, flags, path, |fh| {
            let mut written = 0u64;
            while (written as usize) < data.len() {
                let rest = &data[written as usize..];
                let slice = &rest[..rest.len().min(READ_SLICE as usize)];
                let n = Blocking::run(|r| {
                    self.view.write(
                        &self.cx(OpKind::Write),
                        ino,
                        fh,
                        offset + written,
                        constellation_vfs::WriteData::Borrowed(slice),
                        flags,
                        r,
                    )
                })
                .map_err(|e| vfs_err(path, e))?;
                if n == 0 {
                    return Err(ControlError::failed(format!("{path}: short write")));
                }
                written += n as u64;
            }
            if truncate {
                let set = SetAttr {
                    size: Some(offset + written),
                    ..SetAttr::default()
                };
                Blocking::run(|r| {
                    self.view
                        .setattr(&self.cx(OpKind::Setattr), ino, Some(fh), &set, r)
                })
                .map_err(|e| vfs_err(path, e))?;
            }
            Blocking::run(|r| {
                self.view
                    .fsync(&self.cx(OpKind::Fsync), ino, fh, Durability::Configured, r)
            })
            .map_err(|e| vfs_err(path, e))?;
            Ok(written)
        })
        .and_then(|written| {
            Ok(WriteResult {
                written,
                size: self.getattr(ino, path)?.size,
            })
        })
    }

    /// `browse.mkdir`.
    pub fn mkdir(
        &self,
        path: &str,
        mode: Option<u32>,
        parents: bool,
    ) -> Result<FileStat, ControlError> {
        let mode = mode.unwrap_or(0o755);
        let parts = components(path)?;
        if parts.is_empty() {
            return Err(ControlError::invalid(format!("{path}: names the root")));
        }
        let mut ino = ROOT_INO;
        for (i, part) in parts.iter().enumerate() {
            let last = i + 1 == parts.len();
            match self.lookup(ino, part, path) {
                Ok(entry) if !last && parents => ino = entry.attr.ino,
                Ok(entry) if last && parents && entry.attr.kind == FileKind::Dir => {
                    return Ok(Self::file_stat(path, &entry.attr));
                }
                Ok(entry) if !last => ino = entry.attr.ino,
                Ok(_) => return Err(ControlError::from(Code::Exists)),
                Err(e) if e.code == Some(Code::NotFound) && (last || parents) => {
                    let entry = Blocking::run(|r| {
                        self.view
                            .mkdir(&self.cx(OpKind::Mkdir), ino, Name::new(part), mode, r)
                    })
                    .map_err(|e| vfs_err(path, e))?;
                    if last {
                        return Ok(Self::file_stat(path, &entry.attr));
                    }
                    ino = entry.attr.ino;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("the last component returns")
    }

    /// `browse.rename`.
    pub fn rename(&self, from: &str, to: &str, overwrite: bool) -> Result<(), ControlError> {
        let (p1, n1) = self.parent_of(from)?;
        let (p2, n2) = self.parent_of(to)?;
        let flags = if overwrite {
            RenameFlags::empty()
        } else {
            RenameFlags::NOREPLACE
        };
        Blocking::run(|r| {
            self.view.rename(
                &self.cx(OpKind::Rename),
                p1,
                Name::new(n1),
                p2,
                Name::new(n2),
                flags,
                r,
            )
        })
        .map_err(|e| vfs_err(from, e))
    }

    fn children(&self, ino: u64, path: &str) -> Result<Vec<(String, FileKind)>, ControlError> {
        let mut out = Vec::new();
        let mut cookie = 0;
        loop {
            let (sink, wait) = CollectDir::pair(512);
            self.view
                .readdir(&self.cx(OpKind::Readdir), ino, Fh(0), cookie, false, sink);
            let batch = wait.wait().map_err(|e| vfs_err(path, e))?;
            if batch.is_empty() {
                return Ok(out);
            }
            for entry in &batch {
                let name = String::from_utf8_lossy(entry.name.as_bytes()).into_owned();
                if name != "." && name != ".." {
                    out.push((name, entry.kind));
                }
                cookie = entry.next;
            }
        }
    }

    fn delete_in(
        &self,
        parent: u64,
        name: &str,
        path: &str,
        recursive: bool,
    ) -> Result<(), ControlError> {
        let entry = self.lookup(parent, name, path)?;
        if entry.attr.kind == FileKind::Dir {
            if recursive {
                for (child, _) in self.children(entry.attr.ino, path)? {
                    let child_path = format!("{}/{child}", path.trim_end_matches('/'));
                    self.delete_in(entry.attr.ino, &child, &child_path, true)?;
                }
            }
            Blocking::run(|r| {
                self.view
                    .rmdir(&self.cx(OpKind::Rmdir), parent, Name::new(name), r)
            })
            .map_err(|e| vfs_err(path, e))
        } else {
            Blocking::run(|r| {
                self.view
                    .unlink(&self.cx(OpKind::Unlink), parent, Name::new(name), r)
            })
            .map_err(|e| vfs_err(path, e))
        }
    }

    /// `browse.delete`.
    pub fn delete(&self, path: &str, recursive: bool) -> Result<(), ControlError> {
        let (parent, name) = self.parent_of(path)?;
        self.delete_in(parent, name, path, recursive)
    }

    /// `browse.xattr`.
    pub fn xattr(&self, path: &str, op: &XattrOp) -> Result<XattrResult, ControlError> {
        let ino = self.resolve(path)?;
        let mut out = XattrResult::default();
        match op {
            XattrOp::Get { name } => {
                let value = Blocking::run(|r| {
                    self.view
                        .getxattr(&self.cx(OpKind::Getxattr), ino, XattrName::new(name), r)
                })
                .map_err(|e| vfs_err(path, e))?;
                out.value = Some(value.into());
            }
            XattrOp::List => {
                let names =
                    Blocking::run(|r| self.view.listxattr(&self.cx(OpKind::Listxattr), ino, r))
                        .map_err(|e| vfs_err(path, e))?;
                out.names = names
                    .iter()
                    .map(|n| String::from_utf8_lossy(n.as_bytes()).into_owned())
                    .collect();
            }
            XattrOp::Set { name, value } => {
                Blocking::run(|r| {
                    self.view.setxattr(
                        &self.cx(OpKind::Setxattr),
                        ino,
                        XattrName::new(name),
                        &value.0,
                        SetXattrFlags::empty(),
                        r,
                    )
                })
                .map_err(|e| vfs_err(path, e))?;
            }
            XattrOp::Remove { name } => {
                Blocking::run(|r| {
                    self.view.removexattr(
                        &self.cx(OpKind::Removexattr),
                        ino,
                        XattrName::new(name),
                        r,
                    )
                })
                .map_err(|e| vfs_err(path, e))?;
            }
        }
        Ok(out)
    }

    /// `statfs` of the view's root and its recursive size/count (the
    /// virtual `user.constellation.{rsize,rcount}` xattrs): `view.stats`.
    pub fn stats(&self) -> Result<(constellation_vfs::StatFs, u64, u64), ControlError> {
        let fs = Blocking::run(|r| self.view.statfs(&self.cx(OpKind::Statfs), ROOT_INO, r))
            .map_err(|e| vfs_err("/", e))?;
        let number = |name: &str| -> Result<u64, ControlError> {
            let raw = Blocking::run(|r| {
                self.view.getxattr(
                    &self.cx(OpKind::Getxattr),
                    ROOT_INO,
                    XattrName::new(name),
                    r,
                )
            })
            .map_err(|e| vfs_err("/", e))?;
            String::from_utf8_lossy(&raw)
                .trim()
                .parse()
                .map_err(|_| ControlError::failed(format!("{name} is not a number")))
        };
        Ok((
            fs,
            number("user.constellation.rsize")?,
            number("user.constellation.rcount")?,
        ))
    }
}
