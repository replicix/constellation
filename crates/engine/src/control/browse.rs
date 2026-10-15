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
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_types::Code;
use constellation_vfs::{
    Attr, Blocking, Caller, CollectDir, Durability, Entry, Fh, FileKind, LockOwner, Name, Observer,
    OpCtx, OpKind, OpenFlags, OpenOwner, RenameFlags, SetAttr, SetXattrFlags, Timed, Vfs, VfsError,
    VfsResult, XattrName, NO_TRANSPORT, ROOT_INO,
};
use std::sync::Arc;

/// The largest single `read` a browse call asks of the view.
const READ_SLICE: u32 = 1024 * 1024;

/// A path-addressed client of one [`View`], as one caller.
pub struct ControlVfs {
    view: Arc<View>,
    caller: Caller,
    /// Counts and times every op into the unified op metrics, as the FUSE
    /// session's does (plan 31 §6.10): `frontend="control"`.
    obs: Observer,
}

/// The `frontend` label of the browser's ops in `constellation_vfs_ops_total`.
pub const FRONTEND: &str = "control";

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
        let obs = Observer::new(FRONTEND, &view.identity(), NO_TRANSPORT);
        ControlVfs { view, caller, obs }
    }

    /// One op of `kind` on `ino`, the way a frontend session runs it
    /// ([`Observer::begin`]): an id and a `vfs.op` span for the engine call
    /// to nest under, and its count and latency recorded when the view
    /// answers. The responder blocks this (blocking-pool) thread for the
    /// answer.
    fn op<T: Send + 'static>(
        &self,
        kind: OpKind,
        ino: u64,
        call: impl FnOnce(&OpCtx<'_>, Timed<Blocking<T>>),
    ) -> VfsResult<T> {
        let op = self.obs.begin(kind, ino);
        let _in = op.enter();
        Blocking::run(|r| call(&op.ctx(&self.caller), op.responder(r)))
    }

    fn lookup(&self, parent: u64, name: &str, path: &str) -> Result<Entry, ControlError> {
        self.op(OpKind::Lookup, parent, |cx, r| {
            self.view.lookup(cx, parent, Name::new(name), r)
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
        self.op(OpKind::Getattr, ino, |cx, r| {
            self.view.getattr(cx, ino, None, r)
        })
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
            self.op(OpKind::Flush, ino, |cx, r| {
                self.view.flush(cx, ino, fh, owner, r)
            })
            .map_err(|e| vfs_err(path, e))
        } else {
            Ok(())
        };
        let _ = self.op(OpKind::Release, ino, |cx, r| {
            self.view.release(cx, ino, fh, flags, None, r)
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
        let opened = self
            .op(OpKind::Open, ino, |cx, r| {
                self.view.open(cx, ino, flags, OpenOwner::NONE, r)
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
                let data = self
                    .op(OpKind::Read, ino, |cx, r| {
                        self.view.read(cx, ino, fh, off, want, r)
                    })
                    .map_err(|e| vfs_err(path, e))?;
                if data.is_empty() {
                    break;
                }
                let bytes = data
                    .contiguous()
                    .map_err(|e| vfs_err(path, e))?
                    .into_owned();
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
                let opened = self
                    .op(OpKind::Open, ino, |cx, r| {
                        self.view.open(cx, ino, flags, OpenOwner::NONE, r)
                    })
                    .map_err(|e| vfs_err(path, e))?;
                (ino, opened.fh)
            }
            None => {
                let mode = constellation_vfs::types::mode::S_IFREG | create_mode.unwrap_or(0o644);
                let (entry, opened) = self
                    .op(OpKind::Create, parent, |cx, r| {
                        self.view.create(
                            cx,
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
                let n = self
                    .op(OpKind::Write, ino, |cx, r| {
                        self.view.write(
                            cx,
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
                self.op(OpKind::Setattr, ino, |cx, r| {
                    self.view.setattr(cx, ino, Some(fh), &set, r)
                })
                .map_err(|e| vfs_err(path, e))?;
            }
            self.op(OpKind::Fsync, ino, |cx, r| {
                self.view.fsync(cx, ino, fh, Durability::Configured, r)
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
                    let entry = self
                        .op(OpKind::Mkdir, ino, |cx, r| {
                            self.view.mkdir(cx, ino, Name::new(part), mode, r)
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
        self.op(OpKind::Rename, p1, |cx, r| {
            self.view
                .rename(cx, p1, Name::new(n1), p2, Name::new(n2), flags, r)
        })
        .map_err(|e| vfs_err(from, e))
    }

    fn children(&self, ino: u64, path: &str) -> Result<Vec<(String, FileKind)>, ControlError> {
        let mut out = Vec::new();
        let mut cookie = 0;
        loop {
            let (sink, wait) = CollectDir::pair(512);
            let op = self.obs.begin(OpKind::Readdir, ino);
            let _in = op.enter();
            self.view.readdir(
                &op.ctx(&self.caller),
                ino,
                Fh(0),
                cookie,
                false,
                op.responder(sink),
            );
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
            self.op(OpKind::Rmdir, parent, |cx, r| {
                self.view.rmdir(cx, parent, Name::new(name), r)
            })
            .map_err(|e| vfs_err(path, e))
        } else {
            self.op(OpKind::Unlink, parent, |cx, r| {
                self.view.unlink(cx, parent, Name::new(name), r)
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
                let value = self
                    .op(OpKind::Getxattr, ino, |cx, r| {
                        self.view.getxattr(cx, ino, XattrName::new(name), r)
                    })
                    .map_err(|e| vfs_err(path, e))?;
                out.value = Some(value.into());
            }
            XattrOp::List => {
                let names = self
                    .op(OpKind::Listxattr, ino, |cx, r| {
                        self.view.listxattr(cx, ino, r)
                    })
                    .map_err(|e| vfs_err(path, e))?;
                out.names = names
                    .iter()
                    .map(|n| String::from_utf8_lossy(n.as_bytes()).into_owned())
                    .collect();
            }
            XattrOp::Set { name, value } => {
                self.op(OpKind::Setxattr, ino, |cx, r| {
                    self.view.setxattr(
                        cx,
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
                self.op(OpKind::Removexattr, ino, |cx, r| {
                    self.view.removexattr(cx, ino, XattrName::new(name), r)
                })
                .map_err(|e| vfs_err(path, e))?;
            }
        }
        Ok(out)
    }

    /// Plan 32: set (`Some`) or remove (`None`) the snapshot-policy xattr
    /// of the directory `path` names — provided it still is inode
    /// `expected`, the directory the caller evaluated. A rename in between
    /// is a `conflict`, so a guard computed over one directory's rows can
    /// never land on another. The write is addressed by inode, and a value
    /// is first put through the View's own gate so a refusal carries its
    /// reason; the setxattr then meets that gate again, as any would.
    pub(crate) fn snapshot_policy(
        &self,
        path: &str,
        expected: u64,
        value: Option<&str>,
    ) -> Result<(), ControlError> {
        let name = constellation_meta::snapsched::SNAPSHOT_POLICY_XATTR;
        let ino = self.resolve(path)?;
        if ino != expected {
            return Err(ControlError::new(
                ErrorKind::Conflict,
                format!(
                    "{path}: now names inode {ino}, not the directory {expected} that was \
                     evaluated (renamed meanwhile); retry"
                ),
            ));
        }
        let Some(value) = value else {
            return self
                .op(OpKind::Removexattr, ino, |cx, r| {
                    self.view.removexattr(cx, ino, XattrName::new(name), r)
                })
                .map_err(|e| vfs_err(path, e));
        };
        if let Err(refusal) = self.view.snapshot_policy_gate(ino, value.as_bytes()) {
            return Err(match refusal.reason {
                Some((offset, msg)) => ControlError::invalid(format!("{path}: {msg}"))
                    .with_details(serde_json::json!({
                        "path": path,
                        "offset": offset,
                        "message": msg,
                    })),
                None => ControlError::from(refusal.code)
                    .with_details(serde_json::json!({ "path": path })),
            });
        }
        self.op(OpKind::Setxattr, ino, |cx, r| {
            self.view.setxattr(
                cx,
                ino,
                XattrName::new(name),
                value.as_bytes(),
                SetXattrFlags::empty(),
                r,
            )
        })
        .map_err(|e| vfs_err(path, e))
    }

    /// `statfs` of the view's root and its recursive size/count (the
    /// virtual `user.constellation.{rsize,rcount}` xattrs): `view.stats`.
    pub fn stats(&self) -> Result<(constellation_vfs::StatFs, u64, u64), ControlError> {
        let fs = self
            .op(OpKind::Statfs, ROOT_INO, |cx, r| {
                self.view.statfs(cx, ROOT_INO, r)
            })
            .map_err(|e| vfs_err("/", e))?;
        let number = |name: &str| -> Result<u64, ControlError> {
            let raw = self
                .op(OpKind::Getxattr, ROOT_INO, |cx, r| {
                    self.view.getxattr(cx, ROOT_INO, XattrName::new(name), r)
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
