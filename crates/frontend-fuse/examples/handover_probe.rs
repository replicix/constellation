//! Plan 37 K0 Track A: the three-process FUSE fd-passing and
//! session-handover probe (plan 37 §8, §15 questions 1-4).
//!
//! A throwaway measurement binary, outside the product, with no
//! Kubernetes and no engine in it: it walks exactly the chain a CSI node
//! plugin and two engine pods would, and counts what a writer and a
//! reader on the mountpoint see while it does.
//!
//! ```text
//!   A (driver, "node plugin")      B ("old engine pod")      C ("new engine pod")
//!   ── fuse_mount_fd(mnt) ────────▶ SCM_RIGHTS ──▶ from_fd + FUSE_INIT, serves
//!      (closes its own copy)
//!   ── {"cmd":"detach"} ──────────▶ SessionControl::detach
//!   ◀── SCM_RIGHTS + NegotiatedInit ─┘   (nobody reads /dev/fuse from here)
//!   ── SCM_RIGHTS + NegotiatedInit ───────────────────────▶ FuseSession::resume, serves
//!   ── {"cmd":"exit"} ────────────▶ B exits
//! ```
//!
//! Every hop closes its own copy of the descriptor as soon as the
//! `SCM_RIGHTS` send succeeded (§8 step 4's ownership discipline), so
//! exactly one process holds a live copy at every point.
//!
//! # What it serves
//!
//! `ProbeVfs`: a flat read/write passthrough to one scratch directory,
//! with a fixed inode table (`2 + index` over the directory's names,
//! sorted) and `fh == ino`. Two properties matter, and an in-memory mock
//! has neither:
//!
//! - **Any process can serve the connection.** The numbering comes from
//!   the directory, so B and C agree on it with no handle table crossing
//!   between them: this probe measures the FUSE *transport* of a
//!   handover, not the engine's `HandleTableSnapshot`, which the
//!   `upgrade-under-load` harness scenario already covers against a real
//!   engine.
//! - **The data is continuous across the handover**: the bytes live in
//!   the scratch directory, so a reader verifies after a handoff what a
//!   writer wrote before it. Writers write an offset-keyed pattern into
//!   their own files; readers `pread` *those* files with `O_DIRECT` and
//!   check it, because a cached read is answered without the connection
//!   being touched and would say nothing about a handover.
//!
//! # Phases
//!
//! - `q1`: hand a detached (already-initialised) connection to plain
//!   `fuser::Session::from_fd` and record how it fails, what the client
//!   syscall racing it sees, and how much CPU the attempt burns. Three
//!   passes: with a request queued, with nothing queued, and with nothing
//!   queued on a descriptor whose `O_NONBLOCK` was cleared first — the
//!   two knobs behind its two failure modes.
//! - `chain`: `--handoffs` handoffs with writers and readers running
//!   throughout (questions 2 and 3), then one handoff per `--pauses`
//!   entry with nobody reading `/dev/fuse` for that long (question 4),
//!   sampling `/sys/fs/fuse/connections/<id>/waiting`.
//!
//! Needs root (`mount(2)`) and `/dev/fuse`: run it in a privileged
//! container or a VM. `--json <path>` writes the whole result. The chain
//! phase *asserts* §15's "zero I/O errors": a client error, a byte
//! mismatch, a failed post-resume check or a non-zero fio status is a
//! non-zero exit, not just a line in the JSON.

use constellation_frontend_fuse::{
    caps, mount_source, FuseHandoff, FuseSession, HandoverCapable, KernelTuning, MountOptions,
    MountSource, SessionControl, SessionExit, TransportConfig,
};
use constellation_types::Code;
use constellation_vfs::{
    Attr, DirSink, Durability, Entry, FallocateMode, Fh, FileKind, Ino, LockOwner, LockRange,
    LockSpec, LockStatus, Name, OpCtx, OpenFlags, OpenOwner, Opened, ReadData, RenameFlags,
    Responder, SeekWhence, SetAttr, SetXattrFlags, StatFs, Vfs, VfsError, WriteData, XattrName,
    XattrNameBuf, ROOT_INO,
};
use fuser::NegotiatedInit;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Fail = Box<dyn std::error::Error>;

// ---------------------------------------------------------------------------
// The served tree
// ---------------------------------------------------------------------------

/// A flat passthrough to one directory: the module doc's fixed numbering,
/// `fh == ino`, nothing created or removed through the mount (the driver
/// populates the directory before anybody serves it).
struct ProbeVfs {
    names: Vec<Vec<u8>>,
    files: Vec<File>,
}

impl ProbeVfs {
    /// Open every entry of `dir`, in name order: the numbering every
    /// server of this connection independently arrives at.
    fn open(dir: &Path) -> std::io::Result<ProbeVfs> {
        let mut names: Vec<Vec<u8>> = std::fs::read_dir(dir)?
            .map(|e| Ok(e?.file_name().as_bytes().to_vec()))
            .collect::<std::io::Result<_>>()?;
        names.sort();
        let files = names
            .iter()
            .map(|name| {
                File::options()
                    .read(true)
                    .write(true)
                    .open(dir.join(std::ffi::OsStr::from_bytes(name)))
            })
            .collect::<std::io::Result<_>>()?;
        Ok(ProbeVfs { names, files })
    }

    fn index(&self, ino: Ino) -> Option<usize> {
        if ino < 2 {
            return None;
        }
        let i = (ino - 2) as usize;
        (i < self.files.len()).then_some(i)
    }

    fn file(&self, ino: Ino) -> Result<&File, VfsError> {
        self.index(ino)
            .map(|i| &self.files[i])
            .ok_or(VfsError::new(Code::NotFound))
    }

    fn attr(&self, ino: Ino) -> Result<Attr, VfsError> {
        if ino == ROOT_INO {
            return Ok(dir_attr());
        }
        let md = self.file(ino)?.metadata().map_err(io_code)?;
        Ok(Attr {
            ino,
            kind: FileKind::File,
            size: md.size(),
            blocks: md.blocks(),
            mode: 0o644,
            nlink: 1,
            uid: md.uid(),
            gid: md.gid(),
            rdev: Default::default(),
            atime_ns: md.atime() * 1_000_000_000 + md.atime_nsec(),
            mtime_ns: md.mtime() * 1_000_000_000 + md.mtime_nsec(),
            ctime_ns: md.ctime() * 1_000_000_000 + md.ctime_nsec(),
            blksize: 128 * 1024,
            ttl: Duration::from_secs(1),
        })
    }
}

fn dir_attr() -> Attr {
    Attr {
        ino: ROOT_INO,
        kind: FileKind::Dir,
        size: 4096,
        blocks: 8,
        mode: 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: Default::default(),
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        blksize: 4096,
        ttl: Duration::from_secs(1),
    }
}

fn io_code(e: std::io::Error) -> VfsError {
    VfsError::new(Code::from_linux_errno(
        e.raw_os_error().unwrap_or(libc::EIO),
    ))
}

/// An op this probe's tree has no business serving (it is a fixed set of
/// regular files, created and removed from outside the mount).
fn unsupported<T, R: Responder<T>>(r: R) {
    r.done(Err(VfsError::new(Code::NotSupported)));
}

impl Vfs for ProbeVfs {
    fn lookup<R: Responder<Entry>>(&self, _cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R) {
        if parent != ROOT_INO {
            return r.done(Err(VfsError::new(Code::NotDir)));
        }
        let found = self.names.iter().position(|n| n == name.as_bytes());
        r.done(match found {
            Some(i) => self.attr(i as Ino + 2).map(|attr| Entry {
                attr,
                generation: 1,
            }),
            None => Err(VfsError::new(Code::NotFound)),
        });
    }

    fn getattr<R: Responder<Attr>>(&self, _cx: &OpCtx<'_>, ino: Ino, _fh: Option<Fh>, r: R) {
        r.done(self.attr(ino));
    }

    fn setattr<R: Responder<Attr>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Option<Fh>,
        set: &SetAttr,
        r: R,
    ) {
        // Only a truncate changes anything here; modes and times are
        // accepted and dropped (the probe asserts on neither).
        if let Some(size) = set.size {
            if let Err(e) = self
                .file(ino)
                .and_then(|f| f.set_len(size).map_err(io_code))
            {
                return r.done(Err(e));
            }
        }
        r.done(self.attr(ino));
    }

    fn readlink<R: Responder<Vec<u8>>>(&self, _cx: &OpCtx<'_>, _ino: Ino, r: R) {
        r.done(Err(VfsError::new(Code::Invalid)));
    }

    fn mknod<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _mode: u32,
        _rdev: constellation_types::Rdev,
        r: R,
    ) {
        unsupported(r);
    }

    fn mkdir<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _mode: u32,
        r: R,
    ) {
        unsupported(r);
    }

    fn symlink<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _target: &[u8],
        r: R,
    ) {
        unsupported(r);
    }

    fn link<R: Responder<Entry>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _new_parent: Ino,
        _new_name: &Name,
        r: R,
    ) {
        unsupported(r);
    }

    fn unlink<R: Responder<()>>(&self, _cx: &OpCtx<'_>, _parent: Ino, _name: &Name, r: R) {
        unsupported(r);
    }

    fn rmdir<R: Responder<()>>(&self, _cx: &OpCtx<'_>, _parent: Ino, _name: &Name, r: R) {
        unsupported(r);
    }

    fn rename<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _new_parent: Ino,
        _new_name: &Name,
        _flags: RenameFlags,
        r: R,
    ) {
        unsupported(r);
    }

    fn open<R: Responder<Opened>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _flags: OpenFlags,
        _owner: OpenOwner,
        r: R,
    ) {
        // `fh == ino`: no per-process handle state, so whoever serves the
        // connection next answers a handle the previous server gave out.
        r.done(match self.index(ino) {
            Some(_) => Ok(Opened::new(Fh(ino))),
            None => Err(VfsError::new(Code::NotFound)),
        });
    }

    fn create<R: Responder<(Entry, Opened)>>(
        &self,
        _cx: &OpCtx<'_>,
        _parent: Ino,
        _name: &Name,
        _mode: u32,
        _flags: OpenFlags,
        _owner: OpenOwner,
        r: R,
    ) {
        unsupported(r);
    }

    fn read<R: Responder<ReadData>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        off: u64,
        len: u32,
        r: R,
    ) {
        r.done(self.file(ino).and_then(|f| {
            let mut buf = vec![0u8; len as usize];
            let mut got = 0usize;
            while got < buf.len() {
                match f.read_at(&mut buf[got..], off + got as u64) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(io_code(e)),
                }
            }
            buf.truncate(got);
            Ok(ReadData::from_vec(buf))
        }));
    }

    fn write<R: Responder<u32>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        off: u64,
        data: WriteData<'_>,
        _flags: OpenFlags,
        r: R,
    ) {
        r.done(self.file(ino).and_then(|f| {
            let bytes = data.as_slice();
            let mut done = 0usize;
            while done < bytes.len() {
                match f.write_at(&bytes[done..], off + done as u64) {
                    Ok(0) => return Err(VfsError::new(Code::NoSpace)),
                    Ok(n) => done += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(io_code(e)),
                }
            }
            Ok(done as u32)
        }));
    }

    fn flush<R: Responder<()>>(&self, _cx: &OpCtx<'_>, _ino: Ino, _fh: Fh, _o: LockOwner, r: R) {
        r.done(Ok(()));
    }

    fn release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _flags: OpenFlags,
        _owner: Option<LockOwner>,
        r: R,
    ) {
        r.done(Ok(()));
    }

    fn fsync<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        _level: Durability,
        r: R,
    ) {
        r.done(self.file(ino).and_then(|f| f.sync_data().map_err(io_code)));
    }

    fn readdir<R: DirSink + Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        ino: Ino,
        _fh: Fh,
        cookie: u64,
        _plus: bool,
        mut r: R,
    ) {
        if ino != ROOT_INO {
            return r.done(Err(VfsError::new(Code::NotDir)));
        }
        let mut at = cookie;
        loop {
            let full = match at {
                0 => r.add(ROOT_INO, 1, FileKind::Dir, b"."),
                1 => r.add(ROOT_INO, 2, FileKind::Dir, b".."),
                n => match self.names.get((n - 2) as usize) {
                    Some(name) => r.add(n, n + 1, FileKind::File, name),
                    None => break,
                },
            };
            if full {
                break;
            }
            at += 1;
        }
        r.done(Ok(()));
    }

    fn statfs<R: Responder<StatFs>>(&self, _cx: &OpCtx<'_>, _ino: Ino, r: R) {
        r.done(Ok(StatFs {
            blocks: 1 << 20,
            bfree: 1 << 19,
            bavail: 1 << 19,
            files: self.files.len() as u64 + 1,
            ffree: 0,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        }));
    }

    fn fallocate<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _off: u64,
        _len: u64,
        _mode: FallocateMode,
        r: R,
    ) {
        unsupported(r);
    }

    fn seek<R: Responder<u64>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _off: u64,
        _whence: SeekWhence,
        r: R,
    ) {
        unsupported(r);
    }

    fn getxattr<R: Responder<Vec<u8>>>(&self, _cx: &OpCtx<'_>, _i: Ino, _n: &XattrName, r: R) {
        r.done(Err(VfsError::new(Code::NoData)));
    }

    fn setxattr<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _name: &XattrName,
        _value: &[u8],
        _flags: SetXattrFlags,
        r: R,
    ) {
        unsupported(r);
    }

    fn listxattr<R: Responder<Vec<XattrNameBuf>>>(&self, _cx: &OpCtx<'_>, _ino: Ino, r: R) {
        r.done(Ok(Vec::new()));
    }

    fn removexattr<R: Responder<()>>(&self, _cx: &OpCtx<'_>, _i: Ino, _n: &XattrName, r: R) {
        unsupported(r);
    }

    fn lock_test<R: Responder<LockStatus>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _lock: LockSpec,
        r: R,
    ) {
        r.done(Err(VfsError::new(Code::NotImplemented)));
    }

    fn lock_acquire<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _lock: LockSpec,
        _sleep: bool,
        r: R,
    ) {
        r.done(Err(VfsError::new(Code::NotImplemented)));
    }

    fn lock_release<R: Responder<()>>(
        &self,
        _cx: &OpCtx<'_>,
        _ino: Ino,
        _fh: Fh,
        _owner: LockOwner,
        _range: LockRange,
        r: R,
    ) {
        r.done(Err(VfsError::new(Code::NotImplemented)));
    }

    fn sync_view<R: Responder<()>>(&self, _cx: &OpCtx<'_>, r: R) {
        for file in &self.files {
            if let Err(e) = file.sync_data() {
                return r.done(Err(io_code(e)));
            }
        }
        r.done(Ok(()));
    }
}

// ---------------------------------------------------------------------------
// The wire: JSON messages, one optional descriptor per message
// ---------------------------------------------------------------------------

/// A message is a 4-byte big-endian length sent by `sendmsg` (carrying
/// the `SCM_RIGHTS` control message, when there is a descriptor), then
/// that many bytes of JSON. Asking for exactly the header's bytes keeps
/// the descriptor with its own message on a `SOCK_STREAM` socket.
struct Wire {
    sock: UnixStream,
    fds: VecDeque<OwnedFd>,
}

impl Wire {
    fn new(sock: UnixStream) -> Wire {
        Wire {
            sock,
            fds: VecDeque::new(),
        }
    }

    fn send(&mut self, msg: &Value, fd: Option<RawFd>) -> std::io::Result<()> {
        let payload = serde_json::to_vec(msg)?;
        let header = (payload.len() as u32).to_be_bytes();
        let fds: Vec<RawFd> = fd.into_iter().collect();
        let mut sent = send_with_fds(self.sock.as_raw_fd(), &header, &fds)?;
        while sent < header.len() {
            sent += send_with_fds(self.sock.as_raw_fd(), &header[sent..], &[])?;
        }
        self.sock.write_all(&payload)
    }

    fn recv(&mut self) -> std::io::Result<(Value, Option<OwnedFd>)> {
        let mut header = [0u8; 4];
        let mut got = recv_with_fds(self.sock.as_raw_fd(), &mut header, &mut self.fds)?;
        if got == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the peer closed the probe socket",
            ));
        }
        while got < header.len() {
            match self.sock.read(&mut header[got..])? {
                0 => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "the peer closed the probe socket mid-header",
                    ))
                }
                n => got += n,
            }
        }
        let mut payload = vec![0u8; u32::from_be_bytes(header) as usize];
        self.sock.read_exact(&mut payload)?;
        Ok((serde_json::from_slice(&payload)?, self.fds.pop_front()))
    }
}

/// One blocking `sendmsg`, with `fds` as one `SCM_RIGHTS` message when
/// non-empty (`crates/control/src/transport/unix.rs`'s idiom, without the
/// non-blocking machinery a probe does not need).
fn send_with_fds(sock: RawFd, data: &[u8], fds: &[RawFd]) -> std::io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };
    // SAFETY: msghdr is plain data; all-zero is a valid empty message.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    // u64 elements: the control buffer must be aligned for cmsghdr, and
    // it must outlive the sendmsg below.
    let mut control: Vec<u64> = Vec::new();
    if !fds.is_empty() {
        let bytes = std::mem::size_of_val(fds) as u32;
        // SAFETY: CMSG_SPACE/CMSG_LEN are pure arithmetic.
        let (space, len) = unsafe { (libc::CMSG_SPACE(bytes) as usize, libc::CMSG_LEN(bytes)) };
        control.resize(space.div_ceil(8), 0);
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        // SAFETY: `control` holds CMSG_SPACE(bytes) aligned bytes, so the
        // header and its descriptors fit; CMSG_* only compute offsets in it.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = len as _;
            let out = libc::CMSG_DATA(cmsg).cast::<libc::c_int>();
            for (i, fd) in fds.iter().enumerate() {
                std::ptr::write_unaligned(out.add(i), *fd);
            }
        }
    }
    loop {
        // SAFETY: `msg` points at live iov/control buffers for the call.
        let n = unsafe { libc::sendmsg(sock, &msg, 0) };
        if n >= 0 {
            return Ok(n as usize);
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// One blocking `recvmsg` into `buf`, pushing the descriptors it carried
/// onto `fds`.
fn recv_with_fds(
    sock: RawFd,
    buf: &mut [u8],
    fds: &mut VecDeque<OwnedFd>,
) -> std::io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: as in `send_with_fds`.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    let mut control = [0u64; 16];
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = std::mem::size_of_val(&control) as _;
    let n = loop {
        // SAFETY: `msg` points at live iov/control buffers for the call.
        let n = unsafe { libc::recvmsg(sock, &mut msg, libc::MSG_CMSG_CLOEXEC) };
        if n >= 0 {
            break n as usize;
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    };
    // SAFETY: the kernel filled `control`/`msg_controllen`; CMSG_* walk
    // only within it, and each SCM_RIGHTS payload is an array of c_int
    // whose ownership we take exactly once.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let header = libc::CMSG_LEN(0) as usize;
                let data = libc::CMSG_DATA(cmsg).cast::<libc::c_int>();
                let count =
                    ((*cmsg).cmsg_len as usize).saturating_sub(header) / std::mem::size_of::<i32>();
                for i in 0..count {
                    let raw = std::ptr::read_unaligned(data.add(i));
                    fds.push_back(<OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// The server: process B, C, D … of the module doc
// ---------------------------------------------------------------------------

struct Live {
    control: SessionControl,
    thread: std::thread::JoinHandle<std::io::Result<SessionExit>>,
}

fn options(fs_name: &str, threads: usize) -> MountOptions {
    // The probe exercises the /dev/fuse handover: a ring session is never
    // handed over, and the marker pins the transport to say so.
    let mut opts = MountOptions::handover_capable(
        fs_name,
        threads,
        KernelTuning::for_workers(threads),
        TransportConfig::default(),
        HandoverCapable,
    );
    opts.allow_other = true;
    opts
}

fn serve_main(args: &Args) -> Result<(), Fail> {
    let data = PathBuf::from(args.req("--data")?);
    let threads = args.num("--threads", 8) as usize;
    let mut wire = Wire::new(UnixStream::connect(args.req("--socket")?)?);
    wire.send(&json!({ "hello": std::process::id() }), None)?;
    let opts = options("constellation-handover-probe", threads);
    let mut live: Option<Live> = None;
    loop {
        let (msg, fd) = wire.recv()?;
        let answer = match msg["cmd"].as_str().unwrap_or_default() {
            "init" => {
                let fd = fd.ok_or("no descriptor with the mount request")?;
                let vfs = Arc::new(ProbeVfs::open(&data)?);
                match mount_source(vfs, MountSource::PreopenedFd(fd), &opts, caps(false)) {
                    Ok(session) => {
                        let init = session.negotiated_init();
                        live = Some(spawn_session(session));
                        json!({ "ok": true, "init": init })
                    }
                    Err(e) => {
                        json!({ "ok": false, "error": e.to_string(), "kind": format!("{:?}", e.kind()) })
                    }
                }
            }
            // Question 1: the stock constructor on a connection whose
            // `FUSE_INIT` is long answered. Bounded, because one of its
            // two failure modes does not return at all; this process does
            // nothing else while it waits, so its own CPU time over the
            // window is that constructor's (a spin and a block both fail
            // to return, and only one of them burns a core).
            "from_fd" => {
                let fd = fd.ok_or("no descriptor with the mount request")?;
                let was_nonblocking = nonblocking(&fd);
                if msg["clear_nonblock"].as_bool().unwrap_or(false) {
                    set_blocking(&fd)?;
                }
                let vfs = Arc::new(ProbeVfs::open(&data)?);
                let (tx, rx) = std::sync::mpsc::channel();
                let opts = opts.clone();
                std::thread::spawn(move || {
                    let _ = tx.send(
                        mount_source(vfs, MountSource::PreopenedFd(fd), &opts, caps(false))
                            .map(spawn_session)
                            .map_err(|e| (e.to_string(), format!("{:?}", e.kind()))),
                    );
                });
                let within = Duration::from_millis(msg["timeout_ms"].as_u64().unwrap_or(5_000));
                let (cpu_before, wall) = (process_cpu_s(), Instant::now());
                let outcome = match rx.recv_timeout(within) {
                    Ok(Ok(session)) => {
                        live = Some(session);
                        json!({ "ok": true })
                    }
                    Ok(Err((error, kind))) => json!({ "ok": false, "error": error, "kind": kind }),
                    Err(_) => json!({
                        "ok": false,
                        "returned": false,
                        "error": format!("Session::from_fd did not return within {within:?}"),
                    }),
                };
                let (cpu_ms, wall_ms) = (
                    (process_cpu_s() - cpu_before) * 1e3,
                    wall.elapsed().as_secs_f64() * 1e3,
                );
                json!({
                    "fd_was_nonblocking": was_nonblocking,
                    "cleared_nonblock": msg["clear_nonblock"].as_bool().unwrap_or(false),
                    "server_cpu_ms": cpu_ms,
                    "wall_ms": wall_ms,
                    "server_cpu_per_wall": cpu_ms / wall_ms,
                    "from_fd": outcome,
                })
            }
            "resume" => {
                let fd = fd.ok_or("no descriptor with the resume request")?;
                let carried: NegotiatedInit = serde_json::from_value(msg["init"].clone())?;
                let vfs = Arc::new(ProbeVfs::open(&data)?);
                let handoff = FuseHandoff {
                    fuse_fd: fd,
                    init: carried,
                    mountpoint: msg["mountpoint"].as_str().map(PathBuf::from),
                    foreign: false,
                };
                match FuseSession::resume(handoff, vfs, &opts, caps(false), None) {
                    Ok(session) => {
                        // What the *resumed session* reports, read back out
                        // of it, not the value the request carried in: an
                        // echo of the request would compare equal however
                        // `resume` treated it.
                        let init = session.negotiated_init();
                        live = Some(spawn_session(session));
                        json!({ "ok": true, "init": init })
                    }
                    Err(e) => json!({
                        "ok": false,
                        "error": e.to_string(),
                        "kind": format!("{:?}", e.kind()),
                    }),
                }
            }
            "detach" => {
                let Some(session) = live.take() else {
                    return Err("nothing to detach".into());
                };
                match session.control.detach(|| ()) {
                    Ok(handoff) => {
                        let exit = session.thread.join().expect("the session thread")?;
                        let answer = json!({
                            "ok": true,
                            "init": handoff.fuse.init,
                            "exit": format!("{exit:?}"),
                        });
                        // The next holder gets a duplicate; this process's
                        // own copy goes as soon as the send succeeded
                        // (§8 step 4's ownership discipline).
                        wire.send(&answer, Some(handoff.fuse.fuse_fd.as_raw_fd()))?;
                        drop(handoff.fuse.fuse_fd);
                        continue;
                    }
                    Err(e) => {
                        live = Some(session);
                        json!({ "ok": false, "error": e.to_string(), "code": e.code.name() })
                    }
                }
            }
            "exit" => {
                wire.send(&json!({ "ok": true }), None)?;
                std::process::exit(0);
            }
            other => json!({ "ok": false, "error": format!("unknown command {other:?}") }),
        };
        wire.send(&answer, None)?;
    }
}

/// Whether the handed-over descriptor is still in the non-blocking mode a
/// detachable session put its *shared* open file description into.
fn nonblocking(fd: &OwnedFd) -> bool {
    // SAFETY: F_GETFL on an owned descriptor; no arguments to get wrong.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    flags >= 0 && flags & libc::O_NONBLOCK != 0
}

/// This process's CPU time (all threads), seconds. Question 1's second
/// failure mode is "does not return"; this is how the probe tells a spin
/// from a block instead of inferring it.
fn process_cpu_s() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: a valid out-struct and a constant clock id.
    unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

fn set_blocking(fd: &OwnedFd) -> std::io::Result<()> {
    // SAFETY: F_GETFL/F_SETFL on an owned descriptor.
    unsafe {
        let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
        if flags < 0 || libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags & !libc::O_NONBLOCK) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

fn spawn_session(session: FuseSession<ProbeVfs>) -> Live {
    let control = session.control();
    Live {
        control,
        thread: std::thread::spawn(move || session.run()),
    }
}

// ---------------------------------------------------------------------------
// The driver: process A of the module doc ("the node plugin")
// ---------------------------------------------------------------------------

/// A spawned server and the socket to it.
struct Server {
    id: u32,
    pid: u32,
    child: std::process::Child,
    wire: Wire,
}

impl Server {
    /// Spawn one and wait for its hello (so a measured window never
    /// contains a process start: in Kubernetes the new engine pod is
    /// already up when the handover begins).
    fn spawn(cfg: &Driver, listener: &UnixListener, id: u32) -> Result<Server, Fail> {
        let child = std::process::Command::new(&cfg.exe)
            .arg("serve")
            .arg("--socket")
            .arg(&cfg.socket)
            .arg("--data")
            .arg(&cfg.data)
            .arg("--threads")
            .arg(cfg.threads.to_string())
            .spawn()?;
        let (sock, _) = listener.accept()?;
        let mut wire = Wire::new(sock);
        let (hello, _) = wire.recv()?;
        let pid = hello["hello"].as_u64().unwrap_or_default() as u32;
        Ok(Server {
            id,
            pid,
            child,
            wire,
        })
    }

    fn ask(&mut self, msg: Value, fd: Option<RawFd>) -> Result<Value, Fail> {
        self.wire.send(&msg, fd)?;
        let (answer, _) = self.wire.recv()?;
        Ok(answer)
    }

    /// `detach`: the answer carries the connection back.
    fn detach(&mut self) -> Result<(Value, Option<OwnedFd>), Fail> {
        self.wire.send(&json!({ "cmd": "detach" }), None)?;
        Ok(self.wire.recv()?)
    }

    fn quit(mut self) -> Result<(), Fail> {
        let _ = self.ask(json!({ "cmd": "exit" }), None);
        let status = self.child.wait()?;
        if !status.success() {
            return Err(format!("server {} exited with {status}", self.id).into());
        }
        Ok(())
    }
}

struct Driver {
    exe: PathBuf,
    socket: PathBuf,
    data: PathBuf,
    threads: usize,
}

fn mount_fd(mnt: &Path, fs_name: &str) -> std::io::Result<OwnedFd> {
    let mut opts = constellation_platform::MountOpts::new(fs_name.to_string());
    opts.allow_other = true;
    constellation_platform::linux::fuse_mount_fd(mnt, &opts)
}

fn unmount(path: &Path, detach: bool) -> std::io::Result<()> {
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let flags = if detach { libc::MNT_DETACH } else { 0 };
    // SAFETY: a valid NUL-terminated path that outlives the call.
    if unsafe { libc::umount2(c.as_ptr(), flags) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// The connection's number under `/sys/fs/fuse/connections`: the minor of
/// the mount's device (the kernel's `new_decode_dev` layout).
fn connection_id(mnt: &Path) -> std::io::Result<u64> {
    let dev = std::fs::metadata(mnt)?.dev();
    Ok((dev & 0xff) | ((dev >> 12) & 0xfff00))
}

/// `/sys/fs/fuse/connections` is a `fusectl` mount, and a container gets a
/// fresh `sysfs` without it: mount it, so question 4's queue depths are
/// readable wherever the probe runs.
fn mount_fusectl() -> std::io::Result<()> {
    // An empty `/sys/fs/fuse/connections` is ambiguous (mounted with no
    // connection yet, or not mounted at all); `mountinfo` is not, and
    // re-mounting over a live `fusectl` only earns an `EBUSY`.
    let mounted = std::fs::read_to_string("/proc/self/mountinfo")
        .map(|m| m.lines().any(|l| l.contains(" fusectl ")))
        .unwrap_or(false);
    if mounted {
        return Ok(());
    }
    let (none, fusectl, at) = (c"none", c"fusectl", c"/sys/fs/fuse/connections");
    // SAFETY: three NUL-terminated literals and no options.
    let rc = unsafe {
        libc::mount(
            none.as_ptr(),
            at.as_ptr(),
            fusectl.as_ptr(),
            0,
            std::ptr::null(),
        )
    };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        // Somebody mounted it between the check and the call.
        if e.raw_os_error() != Some(libc::EBUSY) {
            return Err(e);
        }
    }
    Ok(())
}

fn conn_value(id: u64, name: &str) -> Option<u64> {
    std::fs::read_to_string(format!("/sys/fs/fuse/connections/{id}/{name}"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

// ---------------------------------------------------------------------------
// The workload
// ---------------------------------------------------------------------------

const CHUNK: usize = 64 * 1024;

/// What a writer or a reader saw. `max_ns` is taken (and reset) per
/// handoff, so each phase reports its own longest syscall.
#[derive(Default)]
struct Counters {
    ops: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    mismatches: AtomicU64,
    max_ns: AtomicU64,
    first_error: Mutex<Option<String>>,
}

impl Counters {
    fn record(&self, started: Instant, outcome: std::io::Result<usize>) {
        let ns = started.elapsed().as_nanos() as u64;
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
        self.ops.fetch_add(1, Ordering::Relaxed);
        match outcome {
            Ok(n) => {
                self.bytes.fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                let mut first = self.first_error.lock().unwrap();
                if first.is_none() {
                    *first = Some(format!("{e} (errno {:?})", e.raw_os_error()));
                }
            }
        }
    }

    fn take_max_ms(&self) -> f64 {
        self.max_ns.swap(0, Ordering::Relaxed) as f64 / 1e6
    }

    fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    fn report(&self) -> Value {
        json!({
            "ops": self.ops.load(Ordering::Relaxed),
            "bytes": self.bytes.load(Ordering::Relaxed),
            "errors": self.errors(),
            "mismatches": self.mismatches.load(Ordering::Relaxed),
            "first_error": *self.first_error.lock().unwrap(),
        })
    }
}

/// The byte that belongs at absolute offset `at`: every writer writes
/// this and every reader verifies it, so two clients racing on the same
/// range still agree byte for byte.
fn pattern_byte(at: u64) -> u8 {
    (at % 251) as u8
}

/// `CHUNK + 251` pattern bytes. The slice starting at `off % 251` is
/// exactly `pattern_byte(off + i)` for a whole chunk, so a writer never
/// rebuilds its buffer.
fn pattern_block() -> Vec<u8> {
    (0..CHUNK as u64 + 251).map(pattern_byte).collect()
}

/// A page-aligned buffer for the readers' `O_DIRECT` `pread`s. FUSE
/// imposes no alignment of its own, but asking for one costs nothing and
/// keeps the probe runnable over other filesystems.
struct Aligned {
    raw: Vec<u8>,
    at: usize,
    len: usize,
}

impl Aligned {
    fn new(len: usize) -> Aligned {
        let raw = vec![0u8; len + 4096];
        let at = match raw.as_ptr().align_offset(4096) {
            usize::MAX => 0,
            at => at,
        };
        Aligned { raw, at, len }
    }

    fn bytes(&mut self) -> &mut [u8] {
        &mut self.raw[self.at..self.at + self.len]
    }
}

/// Open a file for reading with the page cache out of the way: without
/// `O_DIRECT` the kernel answers a reader from cached pages and the read
/// never crosses the connection, which would measure nothing at all about
/// a handover.
fn open_direct(path: &Path) -> std::io::Result<File> {
    File::options()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

struct Workload {
    stop: Arc<AtomicBool>,
    writes: Arc<Counters>,
    reads: Arc<Counters>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Workload {
    /// `writers` threads each pwrite()ing the offset-keyed pattern into
    /// its own file, and `readers` threads each `O_DIRECT`-pread()ing one
    /// of *those* files and verifying that pattern — the writers' bytes,
    /// across every handoff, with no page cache in between. Every
    /// descriptor is opened once, before the first handoff: the handles
    /// have to survive every handover, exactly as an application's do.
    fn start(mnt: &Path, writers: usize, readers: usize, span: u64) -> Workload {
        let stop = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(Counters::default());
        let reads = Arc::new(Counters::default());
        let mut threads = Vec::new();
        for k in 0..writers {
            let (stop, counters) = (stop.clone(), writes.clone());
            let path = mnt.join(format!("w{k}"));
            threads.push(std::thread::spawn(move || {
                let file = match File::options().read(true).write(true).open(&path) {
                    Ok(file) => file,
                    Err(e) => return counters.record(Instant::now(), Err(e)),
                };
                let block = pattern_block();
                let mut off = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let at = (off % 251) as usize;
                    let started = Instant::now();
                    counters.record(started, file.write_at(&block[at..at + CHUNK], off));
                    off = (off + CHUNK as u64) % span;
                }
            }));
        }
        for k in 0..readers {
            let (stop, counters) = (stop.clone(), reads.clone());
            // A writer's file, not a file nobody touches: a reader is here
            // to verify what the writers wrote, before and after a handoff.
            let path = match writers {
                0 => mnt.join("const.bin"),
                n => mnt.join(format!("w{}", k % n)),
            };
            threads.push(std::thread::spawn(move || {
                let file = match open_direct(&path) {
                    Ok(file) => file,
                    Err(e) => return counters.record(Instant::now(), Err(e)),
                };
                let mut buf = Aligned::new(CHUNK);
                let mut off = (k as u64 * CHUNK as u64) % span;
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let outcome = file.read_at(buf.bytes(), off);
                    if let Ok(n) = outcome {
                        // The pattern is a function of the absolute
                        // offset, so a read that raced a write at the same
                        // range still has to match byte for byte.
                        let bytes = buf.bytes();
                        let bad = (0..n).filter(|i| bytes[*i] != pattern_byte(off + *i as u64));
                        counters
                            .mismatches
                            .fetch_add(bad.count() as u64, Ordering::Relaxed);
                    }
                    counters.record(started, outcome);
                    off = (off + CHUNK as u64) % span;
                }
            }));
        }
        Workload {
            stop,
            writes,
            reads,
            threads,
        }
    }

    fn finish(self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads {
            let _ = thread.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Question 1: plain `from_fd` on an already-initialised connection
// ---------------------------------------------------------------------------

/// One pass: mount, serve, detach, then hand the already-initialised
/// connection to the *stock* `Session::from_fd`. Two knobs isolate the two
/// mechanisms behind question 1's two failure modes (see §"K0 results"):
/// `queue_a_request` decides whether a real client request is waiting when
/// the handshake reads the connection, and `clear_nonblock` decides
/// whether the handover descriptor keeps the `O_NONBLOCK` that an armed
/// session left on the open file description.
fn q1_once(
    cfg: &Driver,
    listener: &UnixListener,
    work: &Path,
    tag: &str,
    queue_a_request: bool,
    clear_nonblock: bool,
) -> Result<Value, Fail> {
    let mnt = work.join(format!("mnt-{tag}"));
    std::fs::create_dir_all(&mnt)?;
    let fd = mount_fd(&mnt, "constellation-probe-q1")?;
    let mut old = Server::spawn(cfg, listener, 101)?;
    let served = old.ask(json!({ "cmd": "init" }), Some(fd.as_raw_fd()))?;
    drop(fd);
    if !served["ok"].as_bool().unwrap_or(false) {
        return Err(format!("the first server could not mount: {served}").into());
    }
    let conn = connection_id(&mnt)?;
    let size = std::fs::metadata(mnt.join("const.bin"))?.len();

    // Stop reading /dev/fuse: the connection stays mounted and
    // initialised, with nobody serving it.
    let old_pid = old.pid;
    let (detached, fd) = old.detach()?;
    let fd = fd.ok_or("the detach carried no descriptor")?;
    old.quit()?;

    // One real request, waiting in the kernel's queue: whoever reads the
    // connection next reads *this*, not a `FUSE_INIT`. `statfs` is used
    // rather than a `stat`, which the kernel answers from the attribute
    // cache without troubling the filesystem at all.
    let (tx, rx) = std::sync::mpsc::channel();
    let queued_at = Instant::now();
    if queue_a_request {
        let client_path = mnt.to_path_buf();
        std::thread::spawn(move || {
            let _ = tx.send((statfs_blocks(&client_path), Instant::now()));
        });
        std::thread::sleep(Duration::from_millis(300));
    }
    let waiting = conn_value(conn, "waiting");

    let mut new = Server::spawn(cfg, listener, 102)?;
    let handed_at = Instant::now();
    let verdict = new.ask(
        json!({
            "cmd": "from_fd",
            "clear_nonblock": clear_nonblock,
            "timeout_ms": 5_000,
        }),
        Some(fd.as_raw_fd()),
    )?;
    drop(fd);
    // Whatever happened, the connection ends with this server: either it
    // closed the descriptor on the error path, or it still holds it. With
    // nothing queued there is nobody to hear from, so nothing is waited
    // for; `from_fd`'s own bound has already passed by here.
    let client = if queue_a_request {
        rx.recv_timeout(Duration::from_secs(5)).ok()
    } else {
        None
    };
    let new_pid = new.pid;
    let _ = new.quit();
    let _ = unmount(&mnt, false).or_else(|_| unmount(&mnt, true));
    // Two clocks for the client: `queued_ms` is its whole syscall, which
    // contains the probe's own 300 ms of deliberate queueing, and
    // `after_the_handover_ms` is how long after the descriptor reached the
    // second process the answer came back — the only part `from_fd` caused.
    let client_request = match client {
        None if queue_a_request => {
            json!({ "error": "the client request never returned (5 s past from_fd's bound)" })
        }
        None => json!(null),
        Some((outcome, done)) => {
            let (queued, after) = (
                done.duration_since(queued_at).as_secs_f64() * 1e3,
                done.saturating_duration_since(handed_at).as_secs_f64() * 1e3,
            );
            let mut row = match outcome {
                Ok(blocks) => json!({ "ok": blocks }),
                Err(e) => json!({ "error": e }),
            };
            row["queued_ms"] = json!(queued);
            row["after_the_handover_ms"] = json!(after);
            row
        }
    };
    Ok(json!({
        "pids": [old_pid, new_pid],
        "a_request_was_queued": queue_a_request,
        "nonblock_cleared_first": clear_nonblock,
        "detached_init": detached["init"],
        "waiting_with_nobody_reading": waiting,
        "const_bin_size": size,
        "verdict": verdict,
        "client_request_racing_it": client_request,
    }))
}

/// Three passes, because question 1 has two mechanisms in it: a queued
/// request is what makes the handshake fail, and `O_NONBLOCK` is what
/// makes it spin instead of block when there is nothing to read.
fn phase_q1(cfg: &Driver, listener: &UnixListener, work: &Path) -> Result<Value, Fail> {
    Ok(json!({
        "with_a_request_queued": q1_once(cfg, listener, work, "q1a", true, false)?,
        "with_nothing_queued": q1_once(cfg, listener, work, "q1b", false, false)?,
        "with_nothing_queued_blocking_fd": q1_once(cfg, listener, work, "q1c", false, true)?,
    }))
}

/// `statfs(2)`: the one cheap call the kernel never answers from a cache,
/// so it always becomes a FUSE request.
fn statfs_blocks(path: &Path) -> Result<u64, String> {
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    // SAFETY: a valid path and an out-struct the call fills.
    let mut out: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut out) } != 0 {
        let e = std::io::Error::last_os_error();
        return Err(format!("{e} (errno {:?})", e.raw_os_error()));
    }
    Ok(out.f_blocks as u64)
}

// ---------------------------------------------------------------------------
// Questions 2-4: the handoff chain under load
// ---------------------------------------------------------------------------

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[at]
}

/// p50/p90/p99/max of one timed leg over the back-to-back handoffs
/// (`pause_ms == 0`; a paused row's legs are not comparable). Every number
/// the plan's question-3 row cites comes from here, so nobody has to
/// re-derive it from the rows by hand.
fn leg_stats(rows: &[Value], leg: &str) -> Value {
    let mut v: Vec<f64> = rows
        .iter()
        .filter(|r| r["pause_ms"] == json!(0))
        .filter_map(|r| r[leg].as_f64())
        .collect();
    v.sort_by(f64::total_cmp);
    json!({
        "n": v.len(),
        "p50": percentile(&v, 0.50),
        "p90": percentile(&v, 0.90),
        "p99": percentile(&v, 0.99),
        "max": v.last().copied(),
    })
}

#[allow(clippy::too_many_lines)]
fn phase_chain(
    cfg: &Driver,
    listener: &UnixListener,
    work: &Path,
    a: &Args,
) -> Result<Value, Fail> {
    let mnt = work.join("mnt");
    std::fs::create_dir_all(&mnt)?;
    let span = a.num("--span-mib", 16) * 1024 * 1024;
    let writers = a.num("--writers", 8) as usize;
    let readers = a.num("--readers", 4) as usize;
    let handoffs = a.num("--handoffs", 40) as usize;
    let pauses: Vec<u64> = a
        .get("--pauses")
        .unwrap_or("500,2000,5000,10000")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse())
        .collect::<Result<_, _>>()?;

    let fd = mount_fd(&mnt, "constellation-handover-probe")?;
    let mut server = Server::spawn(cfg, listener, 1)?;
    let served = server.ask(json!({ "cmd": "init" }), Some(fd.as_raw_fd()))?;
    // The node plugin's own copy goes as soon as the send succeeded.
    drop(fd);
    if !served["ok"].as_bool().unwrap_or(false) {
        return Err(format!("the first server could not mount: {served}").into());
    }
    let conn = connection_id(&mnt)?;
    let queue = json!({
        "connection": conn,
        "max_background": conn_value(conn, "max_background"),
        "congestion_threshold": conn_value(conn, "congestion_threshold"),
    });

    let work_load = Workload::start(&mnt, writers, readers, span);
    let mut fio = a.get("--fio").map(|bin| {
        let out = work.join("fio.json");
        let child = std::process::Command::new(bin)
            .args([
                "--name=handover-probe",
                "--rw=randrw",
                "--rwmixread=50",
                "--bs=64k",
                "--ioengine=psync",
                "--numjobs=2",
                "--thread",
                "--direct=0",
                "--fallocate=none",
                "--create_on_open=0",
                "--allow_file_create=0",
                "--group_reporting",
                "--time_based",
                "--output-format=json",
            ])
            .arg(format!("--filename={}", mnt.join("fio.bin").display()))
            .arg(format!(
                "--size={}",
                a.num("--fio-size-mib", 64) * 1024 * 1024
            ))
            .arg(format!("--runtime={}", a.num("--fio-seconds", 90)))
            .arg(format!("--output={}", out.display()))
            .spawn();
        (child, out)
    });

    // One handoff: detach, hold the connection for `pause`, hand it to a
    // server that is already up, and time each leg.
    let mut rows = Vec::new();
    let plan: Vec<u64> = std::iter::repeat_n(0, handoffs).chain(pauses).collect();
    for (nth, pause) in plan.iter().enumerate() {
        let mut next = Server::spawn(cfg, listener, nth as u32 + 2)?;
        let pids = [server.pid, next.pid];
        let errors_before = (work_load.writes.errors(), work_load.reads.errors());
        work_load.writes.take_max_ms();
        work_load.reads.take_max_ms();
        let waiting_before = conn_value(conn, "waiting");

        let at = Instant::now();
        let (detached, fd) = server.detach()?;
        let detach_ms = at.elapsed().as_secs_f64() * 1e3;
        let fd = fd.ok_or("the detach carried no descriptor")?;
        if !detached["ok"].as_bool().unwrap_or(false) {
            return Err(format!("detach {nth} refused: {detached}").into());
        }

        // Nobody reads /dev/fuse for `pause` ms (§8 steps 1-2).
        let mut waiting_paused = waiting_before;
        if *pause > 0 {
            let until = Instant::now() + Duration::from_millis(*pause);
            while Instant::now() < until {
                std::thread::sleep(Duration::from_millis(50).min(until - Instant::now()));
                waiting_paused = waiting_paused.max(conn_value(conn, "waiting"));
            }
        }

        let at = Instant::now();
        let resumed = next.ask(
            json!({
                "cmd": "resume",
                "init": detached["init"],
                "mountpoint": mnt.to_string_lossy(),
            }),
            Some(fd.as_raw_fd()),
        )?;
        let resume_ms = at.elapsed().as_secs_f64() * 1e3;
        drop(fd);
        if !resumed["ok"].as_bool().unwrap_or(false) {
            return Err(format!("resume {nth} failed: {resumed}").into());
        }
        let at = Instant::now();
        let first_op = std::fs::metadata(mnt.join("const.bin")).map(|m| m.len());
        let first_op_ms = at.elapsed().as_secs_f64() * 1e3;
        // Let the requests that waited out the pause complete before
        // reading the counters: their latency is only recorded once they
        // are answered, which is after the resume.
        std::thread::sleep(Duration::from_millis(a.num("--settle-ms", 150)));
        let waiting_after = conn_value(conn, "waiting");

        let previous = std::mem::replace(&mut server, next);
        previous.quit()?;

        rows.push(json!({
            "nth": nth,
            "pause_ms": pause,
            "pids": pids,
            "detach_ms": detach_ms,
            "resume_ms": resume_ms,
            "roundtrip_ms": detach_ms + resume_ms,
            "first_op_ms": first_op_ms,
            "first_op": first_op.map_err(|e| e.to_string()).err(),
            "waiting": [waiting_before, waiting_paused, waiting_after],
            "writer_longest_ms": work_load.writes.take_max_ms(),
            "reader_longest_ms": work_load.reads.take_max_ms(),
            "writer_errors": work_load.writes.errors() - errors_before.0,
            "reader_errors": work_load.reads.errors() - errors_before.1,
            "init_matches": detached["init"] == resumed["init"],
        }));
    }

    // Question 2, explicitly, on the last resumed session.
    let checks = post_resume_checks(&mnt, span);

    if let Some((child, out)) = fio.take() {
        let report = match child {
            Ok(mut child) => {
                let status = child.wait()?;
                let parsed: Option<Value> = std::fs::read(&out)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok());
                let errors: Vec<Value> = parsed
                    .as_ref()
                    .and_then(|v| v["jobs"].as_array())
                    .map(|jobs| jobs.iter().map(|j| j["error"].clone()).collect())
                    .unwrap_or_default();
                json!({ "status": status.code(), "job_errors": errors })
            }
            Err(e) => json!({ "error": e.to_string() }),
        };
        rows.push(json!({ "fio": report }));
    }

    let (writer, reader) = (work_load.writes.report(), work_load.reads.report());
    work_load.finish();
    let _ = unmount(&mnt, false).or_else(|_| unmount(&mnt, true));
    std::thread::sleep(Duration::from_millis(100));
    server.quit()?;

    let first_op_max = rows
        .iter()
        .filter_map(|r| r["first_op_ms"].as_f64())
        .fold(f64::NAN, f64::max);

    Ok(json!({
        "queue": queue,
        "negotiated_init": served["init"],
        "workload": {
            "writers": writers,
            "readers": readers,
            "writer_files": "w{k}, cached pwrite of the offset pattern",
            "reader_files": "w{k % writers}, O_DIRECT pread + verify",
            "span_bytes": span,
        },
        "handoffs": rows,
        "legs_ms": {
            "detach": leg_stats(&rows, "detach_ms"),
            "resume": leg_stats(&rows, "resume_ms"),
            "roundtrip": leg_stats(&rows, "roundtrip_ms"),
            "first_op": leg_stats(&rows, "first_op_ms"),
        },
        "first_op_ms_max_any_row": first_op_max,
        "writer": writer,
        "reader": reader,
        "post_resume_checks": checks,
    }))
}

/// §15 asks for a chain that runs with *zero* client-visible I/O errors,
/// so the probe fails on one rather than only writing it into the JSON for
/// somebody to notice: a newer kernel that does surface `EIO` to a writer
/// has to turn `run.sh` red.
fn chain_violations(chain: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    for who in ["writer", "reader"] {
        for what in ["errors", "mismatches"] {
            let n = chain[who][what].as_u64().unwrap_or(0);
            if n > 0 {
                bad.push(format!(
                    "{who} {what}: {n} (first: {})",
                    chain[who]["first_error"]
                ));
            }
        }
    }
    for check in chain["post_resume_checks"].as_array().into_iter().flatten() {
        if !check["ok"].as_bool().unwrap_or(false) {
            bad.push(format!(
                "post-resume check {} failed: {}",
                check["check"], check["detail"]
            ));
        }
    }
    for row in chain["handoffs"].as_array().into_iter().flatten() {
        if let Some(fio) = row.get("fio") {
            if let Some(e) = fio.get("error") {
                bad.push(format!("fio did not run: {e}"));
            } else if fio["status"].as_i64() != Some(0) {
                bad.push(format!("fio exited with {}", fio["status"]));
            }
            for e in fio["job_errors"].as_array().into_iter().flatten() {
                if e.as_i64() != Some(0) {
                    bad.push(format!("fio job error {e}"));
                }
            }
            continue;
        }
        let nth = &row["nth"];
        if !row["first_op"].is_null() {
            bad.push(format!(
                "the first client op after handoff {nth} failed: {}",
                row["first_op"]
            ));
        }
        if !row["init_matches"].as_bool().unwrap_or(false) {
            bad.push(format!(
                "handoff {nth}: the resumed session reports a different NegotiatedInit"
            ));
        }
    }
    bad
}

/// Question 2 in so many words: a resumed session answers getattr, read,
/// write, a fresh open and readdir. This — not the `init_matches` column
/// — is what says the resumed connection *works*; "with no re-`INIT`" is
/// what `from_fd_resumed` does by construction (it never calls
/// `handshake()`), which these checks then confirm end to end.
fn post_resume_checks(mnt: &Path, span: u64) -> Value {
    let check = |name: &str, outcome: Result<String, String>| {
        json!({ "check": name, "ok": outcome.is_ok(), "detail": match outcome {
            Ok(d) => d,
            Err(d) => d,
        }})
    };
    let getattr = std::fs::metadata(mnt.join("const.bin"))
        .map(|m| format!("size {}", m.len()))
        .map_err(|e| e.to_string());
    // `O_DIRECT` on a writer's file: a read the resumed session has to
    // answer itself, of bytes the writers wrote before the handoff.
    let read = open_direct(&mnt.join("w0"))
        .and_then(|f| {
            let mut buf = Aligned::new(4096);
            let off = span / 2;
            f.read_exact_at(buf.bytes(), off)?;
            let bytes = buf.bytes();
            let bad = (0..bytes.len()).filter(|i| bytes[*i] != pattern_byte(off + *i as u64));
            Ok(format!("{} byte(s) differ at {off}", bad.count()))
        })
        .map_err(|e| e.to_string());
    let write = File::options()
        .read(true)
        .write(true)
        .open(mnt.join("check.bin"))
        .and_then(|f| {
            let marker = b"resumed-session-write";
            f.write_all_at(marker, 0)?;
            let mut back = vec![0u8; marker.len()];
            f.read_exact_at(&mut back, 0)?;
            Ok(format!("read back {}", back == marker))
        })
        .map_err(|e| e.to_string());
    let readdir = std::fs::read_dir(mnt)
        .map(|d| format!("{} entries", d.count()))
        .map_err(|e| e.to_string());
    json!([
        check("getattr", getattr),
        check("read", read),
        check("write-through-a-new-handle", write),
        check("readdir", readdir),
    ])
}

// ---------------------------------------------------------------------------
// Arguments and entry point
// ---------------------------------------------------------------------------

struct Args {
    items: Vec<String>,
}

impl Args {
    fn get(&self, name: &str) -> Option<&str> {
        let at = self.items.iter().position(|i| i == name)?;
        self.items.get(at + 1).map(String::as_str)
    }

    fn req(&self, name: &str) -> Result<&str, Fail> {
        self.get(name)
            .ok_or_else(|| format!("{name} is required").into())
    }

    fn num(&self, name: &str, default: u64) -> u64 {
        self.get(name)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn has(&self, name: &str) -> bool {
        self.items.iter().any(|i| i == name)
    }
}

/// The tree every server opens: one file per writer, one file the
/// post-resume write check owns, one constant file, and fio's, all created
/// from outside the mount (the probe's `Vfs` refuses creation, so the
/// numbering cannot drift). The writers' files start out holding the
/// pattern, so a reader verifies every offset from the first second,
/// including ranges its writer has not reached yet.
fn prepare_data(dir: &Path, writers: usize, span: u64, fio: Option<u64>) -> Result<(), Fail> {
    std::fs::create_dir_all(dir)?;
    let mut bytes = vec![0u8; span as usize];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = pattern_byte(i as u64);
    }
    std::fs::write(dir.join("const.bin"), &bytes)?;
    for k in 0..writers {
        std::fs::write(dir.join(format!("w{k}")), &bytes)?;
    }
    // Not a writer's file: the marker it writes would read as a mismatch.
    File::create(dir.join("check.bin"))?.set_len(4096)?;
    if let Some(size) = fio {
        File::create(dir.join("fio.bin"))?.set_len(size)?;
    }
    Ok(())
}

fn drive_main(a: &Args) -> Result<(), Fail> {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!(
            "handover_probe needs root (it calls mount(2) itself, as a CSI node plugin does) \
             and /dev/fuse: run it in a privileged container or a VM."
        );
        std::process::exit(2);
    }
    if let Err(e) = mount_fusectl() {
        eprintln!("note: /sys/fs/fuse/connections is not readable here ({e})");
    }
    let work = match a.get("--work") {
        Some(dir) => PathBuf::from(dir),
        None => std::env::temp_dir().join(format!("handover-probe-{}", std::process::id())),
    };
    std::fs::create_dir_all(&work)?;
    let data = work.join("data");
    let writers = a.num("--writers", 8) as usize;
    let span = a.num("--span-mib", 16) * 1024 * 1024;
    let fio_size = a
        .has("--fio")
        .then(|| a.num("--fio-size-mib", 64) * 1024 * 1024);
    prepare_data(&data, writers, span, fio_size)?;

    let socket = work.join("probe.sock");
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    let cfg = Driver {
        exe: std::env::current_exe()?,
        socket,
        data,
        threads: a.num("--threads", 8) as usize,
    };

    let mut result = json!({
        "probe": "plan 37 K0 Track A",
        "kernel": std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim()
            .to_string(),
        "fuse_threads": cfg.threads,
        "writers": writers,
        "readers": a.num("--readers", 4),
    });
    if !a.has("--skip-q1") {
        result["q1_from_fd_on_an_initialised_connection"] = phase_q1(&cfg, &listener, &work)?;
    }
    if !a.has("--skip-chain") {
        result["chain"] = phase_chain(&cfg, &listener, &work, a)?;
    }

    let text = serde_json::to_string_pretty(&result)?;
    println!("{text}");
    if let Some(path) = a.get("--json") {
        std::fs::write(path, format!("{text}\n"))?;
    }
    if !a.has("--keep") {
        let _ = std::fs::remove_dir_all(&work);
    }
    // The data is written first: a failing run is exactly the one whose
    // numbers somebody will want to read.
    if let Some(chain) = result.get("chain") {
        let bad = chain_violations(chain);
        if !bad.is_empty() {
            return Err(format!(
                "the chain did not run clean ({} problem(s)):\n  {}",
                bad.len(),
                bad.join("\n  ")
            )
            .into());
        }
    }
    Ok(())
}

fn main() -> Result<(), Fail> {
    let a = Args {
        items: std::env::args().collect(),
    };
    match a.items.get(1).map(String::as_str) {
        Some("serve") => serve_main(&a),
        _ => drive_main(&a),
    }
}
