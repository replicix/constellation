//! Filesystem operations and the single place that touches the mount.

use anyhow::{Context, Result};
use constellation_types::Code;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Drop this inode's page-cache pages before a verify read, so a
/// convergence failure always describes daemon state rather than a
/// client-side cache. Constellation does not set `FOPEN_KEEP_CACHE`
/// today, which makes this redundant but keeps the checker independent
/// of that choice. Best-effort; filesystems may refuse the advice.
fn drop_cached_pages(f: &File) {
    let fd = f.as_raw_fd();
    // SAFETY: fd is a live open file; POSIX_FADV_DONTNEED is advisory.
    let _ = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
}

/// A single FS operation issued by the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Op {
    Create {
        path: String,
        content: Vec<u8>,
    },
    Mkdir {
        path: String,
    },
    Unlink {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
    /// Hard link `to` to the file at `from` (plan 30 §M4: link histories
    /// for the dependency-cycle checker).
    Link {
        from: String,
        to: String,
    },
    WriteFull {
        path: String,
        content: Vec<u8>,
    },
    WriteAt {
        path: String,
        offset: u64,
        patch: Vec<u8>,
    },
    Append {
        path: String,
        data: Vec<u8>,
    },
    Truncate {
        path: String,
        size: u64,
    },
    Read {
        path: String,
    },
    ReadAt {
        path: String,
        offset: u64,
        len: u64,
    },
    Chmod {
        path: String,
        mode: u32,
    },
    Stat {
        path: String,
    },
}

/// Outcome of executing an [`Op`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Complete {
    pub outcome: Outcome,
    /// libc errno when `outcome == Fail`.
    pub errno: Option<i32>,
    pub errno_name: Option<String>,
    /// blake3 hex of file/span contents when relevant.
    pub value_hash: Option<String>,
    /// Raw bytes for small reads (used by checkers).
    pub bytes: Option<Vec<u8>>,
    /// File size / mode from Stat.
    pub size: Option<u64>,
    pub mode: Option<u32>,
    pub wall_ns: u64,
}

impl Complete {
    fn ok_timed(start: Instant) -> Self {
        Self {
            outcome: Outcome::Ok,
            errno: None,
            errno_name: None,
            value_hash: None,
            bytes: None,
            size: None,
            mode: None,
            wall_ns: start.elapsed().as_nanos() as u64,
        }
    }

    fn fail(start: Instant, err: std::io::Error) -> Self {
        let raw = err.raw_os_error().unwrap_or(0);
        Self {
            outcome: Outcome::Fail,
            errno: Some(raw),
            errno_name: Some(errno_name(raw).to_string()),
            value_hash: None,
            bytes: None,
            size: None,
            mode: None,
            wall_ns: start.elapsed().as_nanos() as u64,
        }
    }
}

/// The checkers' name for a raw OS errno: the portable [`Code`]'s POSIX
/// name for the errnos they reason about, `OTHER` for everything else —
/// including 0 (a non-OS error) and numbers `Code` has no variant for,
/// which must not pass for a real `EIO`.
fn errno_name(errno: i32) -> &'static str {
    let Some(code) = Code::try_from_native(errno) else {
        return "OTHER";
    };
    match code {
        Code::Exists
        | Code::NotFound
        | Code::IsDir
        | Code::NotDir
        | Code::NotEmpty
        | Code::Stale
        | Code::Access
        | Code::Perm
        | Code::Busy
        | Code::Invalid
        | Code::Io
        | Code::NoSpace => code.posix_name(),
        _ => "OTHER",
    }
}

pub fn hash_bytes(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

fn resolve(root: &Path, rel: &str) -> Result<PathBuf> {
    // Rel paths are coordinator-controlled; reject escapes.
    let p = Path::new(rel);
    anyhow::ensure!(
        p.is_relative() && !rel.contains(".."),
        "op path must be relative without ..: {rel}"
    );
    Ok(root.join(p))
}

/// Execute `op` against mount `root`. Always closes files before returning Ok.
pub fn execute_op(root: &Path, op: &Op) -> Result<Complete> {
    let start = Instant::now();
    match op {
        Op::Create { path, content } => {
            let full = resolve(root, path)?;
            if let Some(parent) = full.parent() {
                let _ = fs::create_dir_all(parent);
            }
            match OpenOptions::new().write(true).create_new(true).open(&full) {
                Ok(mut f) => {
                    f.write_all(content)
                        .with_context(|| format!("write create {path}"))?;
                    f.sync_all().ok();
                    drop(f);
                    let mut c = Complete::ok_timed(start);
                    c.value_hash = Some(hash_bytes(content));
                    c.size = Some(content.len() as u64);
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Mkdir { path } => {
            let full = resolve(root, path)?;
            if let Some(parent) = full.parent() {
                let _ = fs::create_dir_all(parent);
            }
            match fs::create_dir(&full) {
                Ok(()) => Ok(Complete::ok_timed(start)),
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Unlink { path } => {
            let full = resolve(root, path)?;
            match fs::remove_file(&full) {
                Ok(()) => Ok(Complete::ok_timed(start)),
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Rmdir { path } => {
            let full = resolve(root, path)?;
            match fs::remove_dir(&full) {
                Ok(()) => Ok(Complete::ok_timed(start)),
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Rename { from, to } => {
            let src = resolve(root, from)?;
            let dst = resolve(root, to)?;
            if let Some(parent) = dst.parent() {
                let _ = fs::create_dir_all(parent);
            }
            match fs::rename(&src, &dst) {
                Ok(()) => Ok(Complete::ok_timed(start)),
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Link { from, to } => {
            let src = resolve(root, from)?;
            let dst = resolve(root, to)?;
            if let Some(parent) = dst.parent() {
                let _ = fs::create_dir_all(parent);
            }
            match fs::hard_link(&src, &dst) {
                Ok(()) => Ok(Complete::ok_timed(start)),
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::WriteFull { path, content } => {
            let full = resolve(root, path)?;
            if let Some(parent) = full.parent() {
                let _ = fs::create_dir_all(parent);
            }
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&full)
            {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(content) {
                        return Ok(Complete::fail(start, e));
                    }
                    f.sync_all().ok();
                    drop(f);
                    let mut c = Complete::ok_timed(start);
                    c.value_hash = Some(hash_bytes(content));
                    c.size = Some(content.len() as u64);
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::WriteAt {
            path,
            offset,
            patch,
        } => {
            let full = resolve(root, path)?;
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&full)
            {
                Ok(mut f) => {
                    if let Err(e) = f.seek(SeekFrom::Start(*offset)) {
                        return Ok(Complete::fail(start, e));
                    }
                    if let Err(e) = f.write_all(patch) {
                        return Ok(Complete::fail(start, e));
                    }
                    f.sync_all().ok();
                    drop(f);
                    let mut c = Complete::ok_timed(start);
                    c.value_hash = Some(hash_bytes(patch));
                    c.size = Some(patch.len() as u64);
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Append { path, data } => {
            let full = resolve(root, path)?;
            match OpenOptions::new().append(true).create(true).open(&full) {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(data) {
                        return Ok(Complete::fail(start, e));
                    }
                    f.sync_all().ok();
                    drop(f);
                    let mut c = Complete::ok_timed(start);
                    c.value_hash = Some(hash_bytes(data));
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Truncate { path, size } => {
            let full = resolve(root, path)?;
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&full)
            {
                Ok(f) => {
                    if let Err(e) = f.set_len(*size) {
                        return Ok(Complete::fail(start, e));
                    }
                    f.sync_all().ok();
                    drop(f);
                    let mut c = Complete::ok_timed(start);
                    c.size = Some(*size);
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Read { path } => {
            let full = resolve(root, path)?;
            match File::open(&full) {
                Ok(mut f) => {
                    drop_cached_pages(&f);
                    let mut data = Vec::new();
                    match f.read_to_end(&mut data) {
                        Ok(_) => {
                            let mut c = Complete::ok_timed(start);
                            c.value_hash = Some(hash_bytes(&data));
                            c.size = Some(data.len() as u64);
                            c.bytes = Some(data);
                            Ok(c)
                        }
                        Err(e) => Ok(Complete::fail(start, e)),
                    }
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::ReadAt { path, offset, len } => {
            let full = resolve(root, path)?;
            match File::open(&full) {
                Ok(mut f) => {
                    drop_cached_pages(&f);
                    if let Err(e) = f.seek(SeekFrom::Start(*offset)) {
                        return Ok(Complete::fail(start, e));
                    }
                    let mut buf = vec![0u8; *len as usize];
                    match f.read_exact(&mut buf) {
                        Ok(()) => {
                            let mut c = Complete::ok_timed(start);
                            c.value_hash = Some(hash_bytes(&buf));
                            c.bytes = Some(buf);
                            c.size = Some(*len);
                            Ok(c)
                        }
                        Err(e) => Ok(Complete::fail(start, e)),
                    }
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Chmod { path, mode } => {
            let full = resolve(root, path)?;
            match fs::set_permissions(&full, fs::Permissions::from_mode(*mode)) {
                Ok(()) => {
                    let mut c = Complete::ok_timed(start);
                    c.mode = Some(*mode);
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
        Op::Stat { path } => {
            let full = resolve(root, path)?;
            match fs::symlink_metadata(&full) {
                Ok(meta) => {
                    let mut c = Complete::ok_timed(start);
                    c.size = Some(meta.len());
                    c.mode = Some(meta.permissions().mode());
                    Ok(c)
                }
                Err(e) => Ok(Complete::fail(start, e)),
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let op = Op::Create {
            path: "a/f".into(),
            content: b"hello".to_vec(),
        };
        let c = execute_op(dir.path(), &op).unwrap();
        assert_eq!(c.outcome, Outcome::Ok);
        let c2 = execute_op(dir.path(), &Op::Read { path: "a/f".into() }).unwrap();
        assert_eq!(c2.bytes.as_deref(), Some(b"hello".as_slice()));
    }

    #[test]
    fn create_race_second_eexist() {
        let dir = tempdir().unwrap();
        let op = Op::Create {
            path: "x".into(),
            content: b"1".to_vec(),
        };
        assert_eq!(execute_op(dir.path(), &op).unwrap().outcome, Outcome::Ok);
        let c = execute_op(dir.path(), &op).unwrap();
        assert_eq!(c.outcome, Outcome::Fail);
        assert_eq!(c.errno.map(Code::from_native), Some(Code::Exists));
    }
}
