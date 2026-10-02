//! A daemon run under limits the harness sets itself, for plan 38 Z2a's
//! transport fault scenarios (`scenarios::transport`): a seccomp filter
//! that refuses `io_uring_setup(2)` — the plan 37 container case, where a
//! runtime's default profile denies io_uring outright — and an
//! address-space limit that refuses the ring's buffer reservation.
//!
//! Both are applied in the spawned daemon between `fork` and `exec`
//! ([`deny_io_uring_setup`], [`limit_address_space`]), so nothing else the
//! harness runs is affected, and the daemon is the real binary, unchanged.
//!
//! **The `fusermount3` relay.** An unprivileged process may only install a
//! seccomp filter after setting `no_new_privs`, and `no_new_privs` makes
//! `execve` ignore set-uid bits — so the daemon could no longer mount, since
//! an unprivileged FUSE mount is made by the set-uid `fusermount3`. fuser
//! runs whatever `FUSERMOUNT_PATH` names, though: a sandboxed daemon is
//! given a `fusermount3` that is this harness binary under another name
//! ([`relay_main`]), which hands its arguments and fuser's `_FUSE_COMMFD`
//! socket over a Unix socket to a broker thread in the (unsandboxed)
//! harness process ([`Broker`]). The broker runs the real `fusermount3`
//! with that socket as its fd 3; the real one mounts, sends the
//! `/dev/fuse` descriptor straight to the daemon over the socket, and its
//! exit status and stderr travel back. The mount is the ordinary,
//! host-visible one; only who exec'd the set-uid helper changed.

use anyhow::{Context, Result};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

/// The relay's broker socket, in the daemon's environment.
pub const BROKER_ENV: &str = "CONSTELLATION_HARNESS_FUSERMOUNT_BROKER";
/// The name the relay is exec'd under (and fuser's own default helper).
pub const RELAY_NAME: &str = "fusermount3";

#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xC000_003E; // AUDIT_ARCH_X86_64
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xC000_00B7; // AUDIT_ARCH_AARCH64

/// Installs, in the calling (single-threaded, about to `exec`) process, a
/// seccomp filter that fails `io_uring_setup(2)` with `EPERM` — what
/// Docker's default profile does — and allows everything else. Sets
/// `no_new_privs` first, which an unprivileged filter requires (see the
/// module doc for what that costs and how the relay pays it).
///
/// Async-signal-safe: two syscalls on a filter built on the stack, for a
/// `pre_exec` hook.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub fn deny_io_uring_setup() -> io::Result<()> {
    // classic BPF over `struct seccomp_data { int nr; __u32 arch; ... }`
    const LD_W_ABS: u16 = 0x20; // BPF_LD (0x00) | BPF_W (0x00) | BPF_ABS (0x20)
    const JEQ_K: u16 = 0x15; // BPF_JMP (0x05) | BPF_JEQ (0x10) | BPF_K (0x00)
    const RET_K: u16 = 0x06; // BPF_RET | BPF_K
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
    const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
    let stmt = |code: u16, k: u32| libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    };
    let jump = |k: u32, jt: u8, jf: u8| libc::sock_filter {
        code: JEQ_K,
        jt,
        jf,
        k,
    };
    let filter = [
        stmt(LD_W_ABS, 4),      // arch
        jump(AUDIT_ARCH, 0, 3), // another ABI: allow
        stmt(LD_W_ABS, 0),      // nr
        jump(libc::SYS_io_uring_setup as u32, 0, 1),
        stmt(RET_K, SECCOMP_RET_ERRNO | libc::EPERM as u32),
        stmt(RET_K, SECCOMP_RET_ALLOW),
    ];
    let prog = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_ptr() as *mut libc::sock_filter,
    };
    // SAFETY: plain syscalls on valid pointers to stack memory that
    // outlives them.
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::syscall(
            libc::SYS_seccomp,
            libc::SECCOMP_SET_MODE_FILTER,
            0,
            &prog as *const libc::sock_fprog,
        ) != 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
pub fn deny_io_uring_setup() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the io_uring seccomp filter is built for Linux x86_64/aarch64 only",
    ))
}

/// `RLIMIT_AS` = `bytes` for the calling process; async-signal-safe.
pub fn limit_address_space(bytes: u64) -> io::Result<()> {
    let lim = libc::rlimit {
        rlim_cur: bytes as libc::rlim_t,
        rlim_max: bytes as libc::rlim_t,
    };
    // SAFETY: a valid pointer to a stack value.
    if unsafe { libc::setrlimit(libc::RLIMIT_AS, &lim) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The harness process's one relay broker (see the module doc), started on
/// first use and serving until the process exits.
pub struct Broker {
    socket: PathBuf,
    relay: PathBuf,
}

impl Broker {
    pub fn get() -> Result<&'static Broker> {
        static BROKER: OnceLock<Broker> = OnceLock::new();
        if let Some(b) = BROKER.get() {
            return Ok(b);
        }
        let real = which_fusermount()?;
        let dir = tempfile::Builder::new()
            .prefix("harness-fusermount-")
            .tempdir()?
            .keep();
        let socket = dir.join("broker.sock");
        let relay = dir.join(RELAY_NAME);
        std::os::unix::fs::symlink(std::env::current_exe()?, &relay)
            .context("linking the fusermount3 relay")?;
        let listener = UnixListener::bind(&socket).context("binding the relay broker")?;
        std::thread::Builder::new()
            .name("fusermount-broker".into())
            .spawn(move || {
                for conn in listener.incoming().flatten() {
                    let real = real.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = serve(conn, &real) {
                            eprintln!("fusermount relay broker: {e:#}");
                        }
                    });
                }
            })?;
        Ok(BROKER.get_or_init(|| Broker { socket, relay }))
    }

    /// The environment that makes a daemon's fuser use the relay.
    pub fn env(&self) -> [(&'static str, &Path); 2] {
        [("FUSERMOUNT_PATH", &self.relay), (BROKER_ENV, &self.socket)]
    }
}

fn which_fusermount() -> Result<PathBuf> {
    let out = Command::new("which").arg(RELAY_NAME).output()?;
    anyhow::ensure!(out.status.success(), "fusermount3 is not installed");
    Ok(PathBuf::from(String::from_utf8(out.stdout)?.trim()))
}

/// One relayed run: `[u32 len][args, NUL-separated]` with fuser's comm
/// socket attached (if any), answered `[i32 status][stderr]`.
fn serve(mut conn: UnixStream, real: &Path) -> Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    let (n, fd) = recv_with_fd(&conn, &mut buf)?;
    anyhow::ensure!(n >= 4, "short relay request");
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    let mut msg = buf[4..n].to_vec();
    while msg.len() < len {
        let k = conn.read(&mut buf)?;
        anyhow::ensure!(k > 0, "truncated relay request");
        msg.extend_from_slice(&buf[..k]);
    }
    let args: Vec<String> = msg
        .split(|b| *b == 0)
        .filter(|a| !a.is_empty())
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .collect();
    let mut cmd = Command::new(real);
    cmd.args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(fd) = &fd {
        let raw = fd.as_raw_fd();
        cmd.env("_FUSE_COMMFD", "3");
        // SAFETY: `dup2` and `fcntl` are async-signal-safe; fd 3 ends up
        // not close-on-exec, which is the point.
        unsafe {
            use std::os::unix::process::CommandExt;
            cmd.pre_exec(move || {
                // Already fd 3: `dup2` would be a no-op and leave it
                // close-on-exec, so clear the flag instead
                let ok = if raw == 3 {
                    libc::fcntl(3, libc::F_SETFD, 0) >= 0
                } else {
                    libc::dup2(raw, 3) >= 0
                };
                if !ok {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let out = cmd.output().context("running fusermount3")?;
    drop(fd);
    let mut reply = out.status.code().unwrap_or(1).to_le_bytes().to_vec();
    reply.extend_from_slice(&out.stderr);
    conn.write_all(&reply)?;
    Ok(())
}

/// The relay end, when this binary was exec'd as `fusermount3`: `None`
/// otherwise. Returns the exit code to leave with.
pub fn relay_main() -> Option<i32> {
    let argv0 = std::env::args_os().next()?;
    if Path::new(&argv0).file_name()? != RELAY_NAME {
        return None;
    }
    Some(match relay() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("fusermount3 relay: {e:#}");
            1
        }
    })
}

fn relay() -> Result<i32> {
    let socket = std::env::var_os(BROKER_ENV).context("no relay broker in the environment")?;
    let conn = UnixStream::connect(&socket).context("connecting to the relay broker")?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut msg = Vec::new();
    for a in &args {
        msg.extend_from_slice(a.as_bytes());
        msg.push(0);
    }
    let mut framed = (msg.len() as u32).to_le_bytes().to_vec();
    framed.extend_from_slice(&msg);
    let fd: Option<RawFd> = std::env::var("_FUSE_COMMFD")
        .ok()
        .and_then(|v| v.parse().ok());
    send_with_fd(&conn, &framed, fd)?;
    let mut reply = Vec::new();
    (&conn).read_to_end(&mut reply)?;
    anyhow::ensure!(reply.len() >= 4, "the broker did not answer");
    io::stderr().write_all(&reply[4..])?;
    Ok(i32::from_le_bytes(reply[..4].try_into().unwrap()))
}

fn send_with_fd(sock: &UnixStream, data: &[u8], fd: Option<RawFd>) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };
    // SAFETY: CMSG_SPACE is a pure size computation.
    let space = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as usize;
    let mut control = vec![0u8; space];
    // SAFETY: zeroed msghdr is a valid empty header.
    let mut hdr: libc::msghdr = unsafe { std::mem::zeroed() };
    hdr.msg_iov = &mut iov;
    hdr.msg_iovlen = 1;
    if let Some(fd) = fd {
        hdr.msg_control = control.as_mut_ptr().cast();
        hdr.msg_controllen = space as _;
        // SAFETY: `control` is CMSG_SPACE bytes for one fd, so the first
        // header and its data fit.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&hdr);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
            std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>(), fd);
        }
    }
    // SAFETY: every pointer in `hdr` is valid for the call.
    let n = unsafe { libc::sendmsg(sock.as_raw_fd(), &hdr, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut rest = &data[n as usize..];
    let mut w = sock;
    while !rest.is_empty() {
        let k = w.write(rest)?;
        rest = &rest[k..];
    }
    Ok(())
}

fn recv_with_fd(sock: &UnixStream, buf: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: CMSG_SPACE is a pure size computation.
    let space = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as usize;
    let mut control = vec![0u8; space];
    // SAFETY: zeroed msghdr is a valid empty header.
    let mut hdr: libc::msghdr = unsafe { std::mem::zeroed() };
    hdr.msg_iov = &mut iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = control.as_mut_ptr().cast();
    hdr.msg_controllen = space as _;
    // SAFETY: every pointer in `hdr` is valid for the call.
    let n = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut hdr, libc::MSG_CMSG_CLOEXEC) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut fd = None;
    // SAFETY: the kernel filled `control` up to `msg_controllen`; the
    // CMSG_* walk stays inside it.
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&hdr);
        if !cmsg.is_null()
            && (*cmsg).cmsg_level == libc::SOL_SOCKET
            && (*cmsg).cmsg_type == libc::SCM_RIGHTS
        {
            let raw = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>());
            fd = Some(OwnedFd::from_raw_fd(raw));
        }
    }
    Ok((n as usize, fd))
}
