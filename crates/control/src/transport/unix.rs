//! [`UnixSocket`]: the control transport for local processes.
//!
//! ## Identity
//!
//! The kernel tells us who connected: `SO_PEERCRED` on Linux, `getpeereid`
//! (plus `LOCAL_PEERPID`) on macOS — tokio's `peer_cred` wraps both. The
//! credentials carry only the connecting process's uid, primary gid and
//! pid; supplementary groups (needed for `group = "ops"` grants) are looked
//! up from the group database with `getgrouplist` for that uid. The result
//! is a [`Principal::Unix`]; the connecting process cannot influence it.
//!
//! ## File descriptors
//!
//! See the [module docs of `transport`](super) for how a frame says an fd is
//! attached. Here: the writer builds `header + payload`, then loops
//! `sendmsg` with the `SCM_RIGHTS` control message on the *first* call only
//! (the kernel attaches it to the first byte of that call's data, i.e. to
//! the frame header); if the socket buffer accepts only part of the frame,
//! the remainder goes out without ancillary data. The reader does every
//! read with `recvmsg`, appends whatever bytes arrive to a buffer, appends
//! whatever descriptors arrive to a FIFO, and hands the *n*-th flagged frame
//! the *n*-th descriptor. Received descriptors are made close-on-exec.
//!
//! Both directions use tokio's `async_io` on the raw descriptor, so the
//! non-blocking `sendmsg`/`recvmsg` cooperate with the reactor's readiness
//! tracking (no extra threads, no busy loop).
//!
//! ## The listener
//!
//! [`UnixSocketListener::bind`] refuses to clobber a live daemon's socket:
//! if something is already accepting on the path it fails `AddrInUse`, and
//! only a *stale* socket file (nothing listening) is removed. A path that
//! exists but is not a socket is never deleted. The socket file is chmod
//! 0600; the directory is 0700 (see `default_socket_path`), so the mode is
//! belt and braces.

use super::{Frame, Listener, Transport, TransportError};
use crate::authz::{sys, Principal};
use crate::fd::OwnedFd;
use crate::proto::{decode_frame, encode_header};
use bytes::BytesMut;
use constellation_platform::dirs::fits_sun_path;
use futures::future::BoxFuture;
use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::Interest;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

/// Descriptors one `recvmsg` is sized for (the control buffer actually
/// holds a few more; beyond that the kernel truncates and we fail).
const RECV_FDS_MAX: usize = 8;
/// Descriptors that may wait, unpaired, before the peer is cut off.
const MAX_QUEUED_FDS: usize = 16;
/// Words of control-message space: room for `RECV_FDS_MAX` ints plus header.
const CMSG_WORDS: usize = 16;
const _: () = assert!(CMSG_WORDS * 8 >= 16 + RECV_FDS_MAX * 4);

#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_FLAGS: libc::c_int = libc::MSG_NOSIGNAL;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SEND_FLAGS: libc::c_int = 0;

struct ReadState {
    buf: BytesMut,
    scratch: Vec<u8>,
    fds: VecDeque<OwnedFd>,
}

/// A connected unix stream socket speaking frames, with fd passing.
pub struct UnixSocket {
    stream: UnixStream,
    read: Mutex<ReadState>,
    write: Mutex<()>,
    poisoned: AtomicBool,
    peer: Principal,
}

impl UnixSocket {
    /// Connect to the daemon at `path`.
    pub async fn connect(path: &Path) -> io::Result<Arc<UnixSocket>> {
        UnixSocket::from_stream_async(UnixStream::connect(path).await?).await
    }

    /// [`from_stream`](Self::from_stream) off the async threads: the
    /// supplementary-group lookup goes through NSS (`getpwuid_r`,
    /// `getgrouplist`), which may block on LDAP/SSSD for as long as it
    /// likes and must not stall the reactor.
    pub async fn from_stream_async(stream: UnixStream) -> io::Result<Arc<UnixSocket>> {
        tokio::task::spawn_blocking(move || UnixSocket::from_stream(stream))
            .await
            .map_err(io::Error::other)?
    }

    /// Wrap a connected stream, reading the peer's credentials. Blocking
    /// (group database lookups); async callers use
    /// [`from_stream_async`](Self::from_stream_async).
    pub fn from_stream(stream: UnixStream) -> io::Result<Arc<UnixSocket>> {
        // Apple has no MSG_NOSIGNAL: ask the socket not to raise SIGPIPE on
        // a write to a closed peer (the error still comes back as EPIPE).
        #[cfg(target_vendor = "apple")]
        {
            let on: libc::c_int = 1;
            // SAFETY: a valid descriptor, and a pointer/length pair naming `on`.
            unsafe {
                libc::setsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    (&on as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
        let cred = stream.peer_cred()?;
        let peer = Principal::Unix {
            uid: cred.uid(),
            gids: sys::groups_of_user(cred.uid(), cred.gid()),
            pid: cred.pid().and_then(|p| u32::try_from(p).ok()),
        };
        Ok(Arc::new(UnixSocket {
            stream,
            read: Mutex::new(ReadState {
                buf: BytesMut::with_capacity(8192),
                scratch: vec![0; 64 * 1024],
                fds: VecDeque::new(),
            }),
            write: Mutex::new(()),
            poisoned: AtomicBool::new(false),
            peer,
        }))
    }
}

/// Marks the socket poisoned if a send is dropped before it finishes.
struct PoisonGuard<'a> {
    flag: &'a AtomicBool,
    finished: bool,
}

impl Drop for PoisonGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.flag.store(true, Ordering::SeqCst);
        }
    }
}

impl Transport for UnixSocket {
    fn send_frame(&self, frame: Frame) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            let Frame { kind, payload, fd } = frame;
            let header = encode_header(kind, fd.is_some(), payload.len())?;
            let mut data = Vec::with_capacity(header.len() + payload.len());
            data.extend_from_slice(&header);
            data.extend_from_slice(&payload);

            let _writing = self.write.lock().await;
            if self.poisoned.load(Ordering::SeqCst) {
                return Err(TransportError::Closed);
            }
            let mut guard = PoisonGuard {
                flag: &self.poisoned,
                finished: false,
            };
            let sock = self.stream.as_raw_fd();
            let mut pending_fd = fd;
            let mut sent = 0usize;
            let result: io::Result<()> = async {
                while sent < data.len() {
                    let raw: Option<RawFd> = pending_fd.as_ref().map(|f| f.as_raw_fd());
                    let n = self
                        .stream
                        .async_io(Interest::WRITABLE, || {
                            send_with_fds(sock, &data[sent..], raw.as_slice())
                        })
                        .await?;
                    if n > 0 {
                        // The descriptor went out with the first byte(s).
                        pending_fd = None;
                    }
                    sent += n;
                }
                Ok(())
            }
            .await;
            guard.finished = true;
            if result.is_err() {
                // A half-written frame: nothing after it can be trusted.
                self.poisoned.store(true, Ordering::SeqCst);
            }
            result.map_err(|e| match e.kind() {
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset => {
                    TransportError::Closed
                }
                _ => TransportError::Io(e),
            })
        })
    }

    fn recv_frame(&self) -> BoxFuture<'_, Result<Option<Frame>, TransportError>> {
        Box::pin(async move {
            let mut state = self.read.lock().await;
            let sock = self.stream.as_raw_fd();
            loop {
                if let Some(raw) = decode_frame(&mut state.buf)? {
                    let fd = if raw.has_fd {
                        Some(state.fds.pop_front().ok_or_else(|| {
                            TransportError::Protocol(
                                "frame announces a file descriptor but none arrived".into(),
                            )
                        })?)
                    } else {
                        None
                    };
                    return Ok(Some(Frame {
                        kind: raw.kind,
                        payload: raw.payload,
                        fd,
                    }));
                }
                let ReadState { buf, scratch, fds } = &mut *state;
                let n = self
                    .stream
                    .async_io(Interest::READABLE, || recv_with_fds(sock, scratch, fds))
                    .await?;
                if fds.len() > MAX_QUEUED_FDS {
                    return Err(TransportError::Protocol(
                        "peer sent too many unclaimed file descriptors".into(),
                    ));
                }
                if n == 0 {
                    return if buf.is_empty() {
                        Ok(None)
                    } else {
                        Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
                    };
                }
                buf.extend_from_slice(&scratch[..n]);
            }
        })
    }

    fn supports_fd_passing(&self) -> bool {
        true
    }

    fn peer(&self) -> Principal {
        self.peer.clone()
    }

    fn name(&self) -> &'static str {
        "unix"
    }
}

/// One non-blocking `sendmsg`, with `fds` as one `SCM_RIGHTS` message when
/// non-empty (the transport sends at most one; tests send more).
/// `WouldBlock` is returned as-is for `async_io` to wait on.
pub(crate) fn send_with_fds(sock: RawFd, data: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };
    // SAFETY: msghdr is plain data; all-zero is a valid empty message.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    // u64 elements: the control buffer must be aligned for cmsghdr. It is
    // declared out here so it outlives every sendmsg below.
    let mut control: Vec<u64> = Vec::new();
    if !fds.is_empty() {
        let bytes = std::mem::size_of_val(fds) as u32;
        // SAFETY: CMSG_SPACE/CMSG_LEN are pure arithmetic.
        let (space, len) = unsafe { (libc::CMSG_SPACE(bytes) as usize, libc::CMSG_LEN(bytes)) };
        control.resize(space.div_ceil(8), 0);
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        // SAFETY: `control` holds CMSG_SPACE(bytes) aligned bytes, so the
        // first header and its `fds.len()` ints fit; CMSG_* only compute
        // offsets within it.
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
        let n = unsafe { libc::sendmsg(sock, &msg, SEND_FLAGS) };
        if n >= 0 {
            return Ok(n as usize);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// One non-blocking `recvmsg` into `buf`, pushing received descriptors onto
/// `fds`. Returns the byte count (0: end of stream).
pub(crate) fn recv_with_fds(
    sock: RawFd,
    buf: &mut [u8],
    fds: &mut VecDeque<OwnedFd>,
) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: as in `send_with_fds`.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    let mut control = [0u64; CMSG_WORDS];
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = std::mem::size_of_val(&control) as _;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let flags = 0;
    let n = loop {
        // SAFETY: `msg` points at live iov/control buffers for the call.
        let n = unsafe { libc::recvmsg(sock, &mut msg, flags) };
        if n >= 0 {
            break n as usize;
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    };
    // SAFETY: the kernel filled `control`/`msg_controllen`; CMSG_* walk only
    // within it, and each SCM_RIGHTS payload is an array of c_int whose
    // ownership we take exactly once.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let header = libc::CMSG_LEN(0) as usize;
                let data = libc::CMSG_DATA(cmsg).cast::<libc::c_int>();
                // Never read past what the kernel filled, whatever
                // `cmsg_len` claims (a truncated message on some kernels).
                let filled = msg.msg_control as usize + msg.msg_controllen as usize;
                let room =
                    filled.saturating_sub(data as usize) / std::mem::size_of::<libc::c_int>();
                let count = (((*cmsg).cmsg_len as usize).saturating_sub(header)
                    / std::mem::size_of::<libc::c_int>())
                .min(room);
                for i in 0..count {
                    let raw = std::ptr::read_unaligned(data.add(i));
                    #[cfg(not(any(target_os = "linux", target_os = "android")))]
                    {
                        // No MSG_CMSG_CLOEXEC here: close-on-exec after the fact.
                        libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC);
                    }
                    fds.push_back(OwnedFd::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ancillary data truncated (too many descriptors in one message)",
        ));
    }
    Ok(n)
}

/// A bound, listening control socket.
pub struct UnixSocketListener {
    listener: UnixListener,
    path: PathBuf,
    /// Remove the socket file on drop (the default).
    unlink: bool,
}

impl UnixSocketListener {
    /// Bind `path`, replacing a stale socket file but never a live daemon's
    /// socket or a non-socket file.
    pub fn bind(path: &Path) -> io::Result<UnixSocketListener> {
        UnixSocketListener::bind_with_mode(path, 0o600)
    }

    /// [`bind`](Self::bind) with an explicit socket file mode. The default
    /// (0600, inside a 0700 directory) admits only the daemon's owner. A
    /// deployment that wants allowlisted *other* users or a group to connect
    /// must widen both the socket (e.g. 0660 plus `chgrp`) and the directory
    /// it lives in; the allowlist then decides what they may do, but the
    /// filesystem permissions decide whether they can knock at all.
    pub fn bind_with_mode(path: &Path, mode: u32) -> io::Result<UnixSocketListener> {
        if !fits_sun_path(path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("socket path {} is too long for sun_path", path.display()),
            ));
        }
        match std::fs::symlink_metadata(path) {
            Ok(meta) => {
                use std::os::unix::fs::FileTypeExt;
                if !meta.file_type().is_socket() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("{} exists and is not a socket", path.display()),
                    ));
                }
                match std::os::unix::net::UnixStream::connect(path) {
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            format!("another daemon is already serving {}", path.display()),
                        ))
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                        ) =>
                    {
                        // Stale: nothing accepts on it. (It may vanish
                        // between the probe and here; that is fine too.)
                        match std::fs::remove_file(path) {
                            Ok(()) => {}
                            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let listener = UnixListener::bind(path)?;
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
        }
        Ok(UnixSocketListener {
            listener,
            path: path.to_path_buf(),
            unlink: true,
        })
    }

    /// Serve a listener bound elsewhere — one a previous image of the daemon
    /// bound and handed over across `exec` (plan 31 C4b), so a client that
    /// connects during the handover waits in the backlog instead of finding
    /// no daemon. `path` is where it is bound (removed when this drops).
    pub fn from_std(
        listener: std::os::unix::net::UnixListener,
        path: &Path,
    ) -> io::Result<UnixSocketListener> {
        listener.set_nonblocking(true)?;
        Ok(UnixSocketListener {
            listener: UnixListener::from_std(listener)?,
            path: path.to_path_buf(),
            unlink: true,
        })
    }

    /// Leave the socket file in place when this drops: another descriptor
    /// of the same listening socket ([`Self::try_clone_std`]) serves on
    /// (plan 37 K6a: `serve --await-unlock` answers on a clone until the
    /// engine starts, then hands the original to the daemon).
    pub fn keep_path_on_drop(mut self) -> UnixSocketListener {
        self.unlink = false;
        self
    }

    /// A second descriptor for the same listening socket (to hand on across
    /// an in-place upgrade while this one keeps serving).
    pub fn try_clone_std(&self) -> io::Result<std::os::unix::net::UnixListener> {
        use std::os::fd::AsFd;
        let fd = self.listener.as_fd().try_clone_to_owned()?;
        Ok(std::os::unix::net::UnixListener::from(fd))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Listener for UnixSocketListener {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<Arc<dyn Transport>>> {
        Box::pin(async move {
            let (stream, _) = self.listener.accept().await?;
            let socket: Arc<dyn Transport> = UnixSocket::from_stream_async(stream).await?;
            Ok(socket)
        })
    }
}

impl Drop for UnixSocketListener {
    fn drop(&mut self) {
        if self.unlink {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FrameKind;
    use bytes::Bytes;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::fd::AsFd;

    fn pair() -> (Arc<UnixSocket>, Arc<UnixSocket>) {
        let (a, b) = UnixStream::pair().unwrap();
        (
            UnixSocket::from_stream(a).unwrap(),
            UnixSocket::from_stream(b).unwrap(),
        )
    }

    fn tempfile_with(text: &str) -> std::fs::File {
        let mut f = tempfile::tempfile().unwrap();
        f.write_all(text.as_bytes()).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f
    }

    fn read_all(fd: OwnedFd) -> String {
        let mut file = std::fs::File::from(fd);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut s = String::new();
        file.read_to_string(&mut s).unwrap();
        s
    }

    #[tokio::test]
    async fn peer_credentials_are_the_real_process() {
        let (a, _b) = pair();
        // SAFETY: getuid cannot fail.
        let me = unsafe { libc::getuid() };
        match a.peer() {
            Principal::Unix { uid, gids, pid } => {
                assert_eq!(uid, me);
                assert!(!gids.is_empty());
                assert_eq!(pid, Some(std::process::id()));
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_descriptor_rides_with_its_frame() {
        let (a, b) = pair();
        let file = tempfile_with("hello through the fd");
        a.send_fd(FrameKind::Request, Bytes::from_static(b"req"), file.as_fd())
            .await
            .unwrap();
        let mut got = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(got.kind, FrameKind::Request);
        assert_eq!(&got.payload[..], b"req");
        assert_eq!(
            read_all(got.take_fd().expect("fd attached")),
            "hello through the fd"
        );
        // The sender still owns its copy (send_fd duplicates).
        drop(file);
    }

    #[tokio::test]
    async fn received_descriptors_are_close_on_exec() {
        let (a, b) = pair();
        let file = tempfile_with("x");
        a.send_fd(FrameKind::Request, Bytes::from_static(b"r"), file.as_fd())
            .await
            .unwrap();
        let fd = b.recv_frame().await.unwrap().unwrap().take_fd().unwrap();
        // SAFETY: F_GETFD on a descriptor we own.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0 && flags & libc::FD_CLOEXEC != 0, "{flags}");
    }

    /// Raw frame bytes (header + payload) for `kind`, flag clear.
    fn raw_frame(kind: FrameKind, payload: &[u8]) -> Vec<u8> {
        let mut out = encode_header(kind, false, payload.len()).unwrap().to_vec();
        out.extend_from_slice(payload);
        out
    }

    #[tokio::test]
    async fn a_peer_cannot_park_unlimited_descriptors() {
        // Descriptors riding on frames that do not announce one pile up
        // unclaimed; past the cap the connection is refused.
        let (raw, other) = UnixStream::pair().unwrap();
        let b = UnixSocket::from_stream(other).unwrap();
        let file = tempfile_with("x");
        for _ in 0..=MAX_QUEUED_FDS {
            let n = send_with_fds(
                raw.as_raw_fd(),
                &raw_frame(FrameKind::Cancel, b"c"),
                &[file.as_raw_fd()],
            )
            .unwrap();
            assert_eq!(n, 6);
        }
        // (Linux hands over one fd-carrying message per recvmsg, so the
        // cut comes exactly at the cap; other kernels may batch.)
        let mut refused = None;
        for _ in 0..=MAX_QUEUED_FDS {
            match b.recv_frame().await {
                Ok(fr) => assert!(fr.unwrap().fd.is_none(), "unflagged frames get no fd"),
                Err(e) => {
                    refused = Some(e);
                    break;
                }
            }
        }
        assert!(
            matches!(refused, Some(TransportError::Protocol(_))),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn truncated_ancillary_data_is_an_error() {
        // More descriptors in one message than the receive buffer holds:
        // the kernel sets MSG_CTRUNC and the reader must not carry on.
        let (raw, other) = UnixStream::pair().unwrap();
        let b = UnixSocket::from_stream(other).unwrap();
        let file = tempfile_with("x");
        let fds = vec![file.as_raw_fd(); 64];
        send_with_fds(raw.as_raw_fd(), &raw_frame(FrameKind::Cancel, b"c"), &fds).unwrap();
        match b.recv_frame().await {
            Err(TransportError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn descriptors_pair_with_the_right_frames_in_order() {
        let (a, b) = pair();
        let f1 = tempfile_with("one");
        let f2 = tempfile_with("two");
        // fd, plain, plain, fd, plain — sent back to back so the reader sees
        // several frames per recvmsg.
        a.send_fd(FrameKind::Request, Bytes::from_static(b"1"), f1.as_fd())
            .await
            .unwrap();
        a.send_frame(Frame::new(FrameKind::Cancel, Bytes::from_static(b"x")))
            .await
            .unwrap();
        a.send_frame(Frame::new(FrameKind::Cancel, Bytes::from_static(b"y")))
            .await
            .unwrap();
        a.send_fd(FrameKind::Request, Bytes::from_static(b"2"), f2.as_fd())
            .await
            .unwrap();
        a.send_frame(Frame::new(FrameKind::Cancel, Bytes::from_static(b"z")))
            .await
            .unwrap();

        let mut fr = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(read_all(fr.take_fd().unwrap()), "one");
        for expect in [&b"x"[..], b"y"] {
            let fr = b.recv_frame().await.unwrap().unwrap();
            assert!(fr.fd.is_none());
            assert_eq!(&fr.payload[..], expect);
        }
        let mut fr = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(&fr.payload[..], b"2");
        assert_eq!(read_all(fr.take_fd().unwrap()), "two");
        let fr = b.recv_frame().await.unwrap().unwrap();
        assert!(fr.fd.is_none());
    }

    #[tokio::test]
    async fn a_large_frame_with_an_fd_survives_partial_writes() {
        let (a, b) = pair();
        let payload = Bytes::from(vec![0xAB; 3 * 1024 * 1024]); // >> socket buffer
        let file = tempfile_with("big");
        let a2 = a.clone();
        let sender = tokio::spawn(async move {
            a2.send_fd(FrameKind::Request, payload, file.as_fd())
                .await
                .unwrap();
            // A second frame right behind it must not corrupt the first.
            a2.send_frame(Frame::new(FrameKind::Cancel, Bytes::from_static(b"tail")))
                .await
                .unwrap();
        });
        let mut fr = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(fr.payload.len(), 3 * 1024 * 1024);
        assert!(fr.payload.iter().all(|&x| x == 0xAB));
        assert_eq!(read_all(fr.take_fd().unwrap()), "big");
        let tail = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(&tail.payload[..], b"tail");
        sender.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_senders_never_interleave_frames() {
        let (a, b) = pair();
        let mut tasks = Vec::new();
        for i in 0u8..8 {
            let a = a.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..20 {
                    a.send_frame(Frame::new(FrameKind::Event, Bytes::from(vec![i; 200_000])))
                        .await
                        .unwrap();
                }
            }));
        }
        for _ in 0..160 {
            let fr = b.recv_frame().await.unwrap().unwrap();
            let first = fr.payload[0];
            assert_eq!(fr.payload.len(), 200_000);
            assert!(fr.payload.iter().all(|&x| x == first), "interleaved frame");
        }
        for t in tasks {
            t.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_flagged_frame_without_a_descriptor_is_an_error_not_a_hang() {
        use tokio::io::AsyncWriteExt;
        let (mut raw, other) = UnixStream::pair().unwrap();
        let b = UnixSocket::from_stream(other).unwrap();
        // Hand-write a frame whose kind byte claims an fd.
        let header = [0, 0, 0, 1, FrameKind::Request as u8 | crate::proto::FLAG_FD];
        raw.write_all(&header).await.unwrap();
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), b.recv_frame())
            .await
            .expect("must not hang")
            .unwrap_err();
        assert!(matches!(err, TransportError::Protocol(_)), "{err:?}");
    }

    #[tokio::test]
    async fn clean_close_and_truncation() {
        let (a, b) = pair();
        drop(a);
        assert!(b.recv_frame().await.unwrap().is_none());

        use tokio::io::AsyncWriteExt;
        let (mut raw, other) = UnixStream::pair().unwrap();
        let b = UnixSocket::from_stream(other).unwrap();
        raw.write_all(&[0, 0, 0, 9, 3, 1]).await.unwrap();
        drop(raw);
        assert!(matches!(b.recv_frame().await, Err(TransportError::Io(_))));
    }

    #[tokio::test]
    async fn listener_replaces_stale_sockets_but_not_live_ones_or_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.sock");

        // A stale socket file (bound, then abandoned without unlinking).
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        assert!(path.exists());
        let live = UnixSocketListener::bind(&path).expect("stale socket is replaced");

        // A second daemon must not steal it.
        let err = UnixSocketListener::bind(&path).err().expect("live socket");
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);

        // Dropping the listener removes its file.
        drop(live);
        assert!(!path.exists());

        // A regular file is never deleted.
        std::fs::write(&path, b"precious").unwrap();
        let err = UnixSocketListener::bind(&path).err().expect("not a socket");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"precious");
    }

    #[tokio::test]
    async fn accept_connect_round_trip_over_a_path() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.sock");
        let mut listener = UnixSocketListener::bind(&path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let server = tokio::spawn(async move {
            let t = listener.accept().await.unwrap();
            let fr = t.recv_frame().await.unwrap().unwrap();
            t.send_frame(Frame::new(FrameKind::Response, fr.payload))
                .await
                .unwrap();
            listener
        });
        let c = UnixSocket::connect(&path).await.unwrap();
        c.send_frame(Frame::new(FrameKind::Request, Bytes::from_static(b"ping")))
            .await
            .unwrap();
        let back = c.recv_frame().await.unwrap().unwrap();
        assert_eq!(
            (back.kind, &back.payload[..]),
            (FrameKind::Response, &b"ping"[..])
        );
        drop(server.await.unwrap());
    }
}
