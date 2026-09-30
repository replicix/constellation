//! The FUSE adapter, end to end without a kernel (plan 31 §8, C6).
//!
//! `fuser` builds its `Request` and `Reply*` types only from bytes read off
//! a `/dev/fuse` descriptor, so an adapter's callbacks cannot be called
//! directly from a test. `fuser::Session::from_fd` accepts *any*
//! descriptor, though, and the kernel's side of the protocol is a
//! sequence of `read`/`write` calls of whole messages — which a
//! `SOCK_DGRAM` socket pair carries just as well. [`Kernel`] is that
//! other end: it writes real FUSE requests (`fuse_in_header` + the op's
//! body, Linux layouts) and parses real replies, so what these tests
//! exercise is the whole adapter: fuser's decoding, [`FuseFs`]'s
//! translation into `constellation_vfs` calls, the responder newtypes, and
//! fuser's encoding — against a [`MockVfs`] that records what it was
//! asked and answers as scripted.
//!
//! What this covers: flag/mode/whence/lock-type/rename-flag/xattr-flag
//! decoding as the `Vfs` sees it, the attribute/entry/statfs/dirent/lock
//! encodings, the size-probe protocol of xattrs, the errno of every
//! `Code`, `FUSE_INIT`'s negotiation, and the responder completion paths
//! (inline on the fuser worker, deferred from another thread with the
//! event loop free meanwhile, dropped and answered `EIO`, a contended
//! `F_SETLKW` end to end).
//!
//! What it cannot: anything the kernel's VFS does above the daemon
//! (permission checks, path walks, page cache, `FUSE_INTERRUPT`, which
//! fuser 0.18 does not deliver anyway) and the real `/dev/fuse` and mount
//! plumbing, which the harness scenarios and pjdfstest cover.

#![cfg(target_os = "linux")]

use constellation_frontend_fuse::{FuseFs, KernelTuning};
use constellation_types::{Code, Rdev};
use constellation_vfs::mock::{Args, MockVfs, Script};
use constellation_vfs::types::mode as modebits;
use constellation_vfs::{
    Attr, DirEntry, Durability, Entry, FallocateMode, Fh, FileKind, FrontendCaps, Ino, LockKind,
    LockOwner, LockRange, LockSpec, LockStatus, OpKind, OpenFlags, OpenOwner, Opened, ReadData,
    RenameFlags, SeekWhence, SetXattrFlags, StatFs, TimeSet, Vfs, XattrNameBuf, ROOT_INO,
};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixDatagram;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

// ------------------------------------------------------------ the protocol

mod op {
    pub const LOOKUP: u32 = 1;
    pub const FORGET: u32 = 2;
    pub const GETATTR: u32 = 3;
    pub const SETATTR: u32 = 4;
    pub const READLINK: u32 = 5;
    pub const SYMLINK: u32 = 6;
    pub const MKNOD: u32 = 8;
    pub const MKDIR: u32 = 9;
    pub const UNLINK: u32 = 10;
    pub const RMDIR: u32 = 11;
    pub const RENAME: u32 = 12;
    pub const LINK: u32 = 13;
    pub const OPEN: u32 = 14;
    pub const READ: u32 = 15;
    pub const WRITE: u32 = 16;
    pub const STATFS: u32 = 17;
    pub const RELEASE: u32 = 18;
    pub const FSYNC: u32 = 20;
    pub const SETXATTR: u32 = 21;
    pub const GETXATTR: u32 = 22;
    pub const LISTXATTR: u32 = 23;
    pub const REMOVEXATTR: u32 = 24;
    pub const FLUSH: u32 = 25;
    pub const INIT: u32 = 26;
    pub const OPENDIR: u32 = 27;
    pub const READDIR: u32 = 28;
    pub const GETLK: u32 = 31;
    pub const SETLK: u32 = 32;
    pub const SETLKW: u32 = 33;
    pub const CREATE: u32 = 35;
    pub const DESTROY: u32 = 38;
    pub const FALLOCATE: u32 = 43;
    pub const RENAME2: u32 = 45;
    pub const LSEEK: u32 = 46;
}

/// Request bodies, built field by field in the kernel's little-endian
/// layouts.
#[derive(Default)]
struct Body(Vec<u8>);

impl Body {
    fn new() -> Self {
        Self::default()
    }
    fn u32(mut self, v: u32) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn i32(self, v: i32) -> Self {
        self.u32(v as u32)
    }
    fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn i64(self, v: i64) -> Self {
        self.u64(v as u64)
    }
    fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend_from_slice(b);
        self
    }
    fn cstr(self, s: &str) -> Self {
        self.bytes(s.as_bytes()).bytes(&[0])
    }
}

/// One reply off the wire.
#[derive(Debug)]
struct Reply {
    unique: u64,
    /// The `error` field: 0, or the negated errno.
    error: i32,
    body: Vec<u8>,
}

impl Reply {
    fn ok(&self) -> &[u8] {
        assert_eq!(self.error, 0, "the reply is an error: {}", self.errno());
        &self.body
    }
    fn errno(&self) -> i32 {
        -self.error
    }
    fn u32_at(&self, at: usize) -> u32 {
        u32::from_le_bytes(self.body[at..at + 4].try_into().unwrap())
    }
    fn u64_at(&self, at: usize) -> u64 {
        u64::from_le_bytes(self.body[at..at + 8].try_into().unwrap())
    }
}

/// `fuse_attr` (Linux layout, 88 bytes) decoded.
#[derive(Debug, PartialEq, Eq)]
struct WireAttr {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: i64,
    mtime: i64,
    ctime: i64,
    atimensec: u32,
    mode: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    blksize: u32,
}

fn wire_attr(b: &[u8]) -> WireAttr {
    let u32_at = |o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    WireAttr {
        ino: u64_at(0),
        size: u64_at(8),
        blocks: u64_at(16),
        atime: u64_at(24) as i64,
        mtime: u64_at(32) as i64,
        ctime: u64_at(40) as i64,
        atimensec: u32_at(48),
        mode: u32_at(60),
        nlink: u32_at(64),
        uid: u32_at(68),
        gid: u32_at(72),
        rdev: u32_at(76),
        blksize: u32_at(80),
    }
}

/// The kernel's end of the socket pair.
struct Kernel {
    sock: UnixDatagram,
    next_unique: u64,
    session: Option<JoinHandle<std::io::Result<()>>>,
    init: Reply,
    uid: u32,
    gid: u32,
    pid: u32,
}

const KERNEL_INIT_FLAGS: u32 = (1 << 1) | (1 << 10) | (1 << 18); // POSIX_LOCKS | FLOCK_LOCKS | PARALLEL_DIROPS

impl Kernel {
    fn start<V: Vfs>(vfs: Arc<V>, caps: FrontendCaps, workers: usize) -> Kernel {
        let (kernel, daemon) = UnixDatagram::pair().expect("socketpair");
        kernel
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut k = Kernel {
            sock: kernel,
            next_unique: 1,
            session: None,
            init: Reply {
                unique: 0,
                error: 0,
                body: Vec::new(),
            },
            uid: 1000,
            gid: 100,
            pid: 4242,
        };
        // FUSE_INIT is written first: `Session::from_fd` handshakes before
        // it returns.
        let body = Body::new()
            .u32(7)
            .u32(36)
            .u32(1 << 17)
            .u32(KERNEL_INIT_FLAGS)
            .u32(0)
            .bytes(&[0u8; 44]);
        let init_unique = k.send(op::INIT, 0, &body.0);
        let mut config = fuser::Config::default();
        config.acl = fuser::SessionACL::All;
        config.n_threads = Some(1);
        let fs = FuseFs::new(vfs, caps, KernelTuning::for_workers(workers));
        let session =
            fuser::Session::from_fd(fs, OwnedFd::from(daemon), fuser::SessionACL::All, config)
                .expect("the FUSE handshake");
        k.init = k.recv();
        assert_eq!(k.init.unique, init_unique);
        k.session = Some(std::thread::spawn(move || session.run()));
        k
    }

    /// A request as (uid, gid, pid) of this kernel's caller; returns its
    /// unique.
    fn send(&mut self, opcode: u32, nodeid: u64, body: &[u8]) -> u64 {
        self.send_as(opcode, nodeid, (self.uid, self.gid, self.pid), body)
    }

    fn send_as(&mut self, opcode: u32, nodeid: u64, who: (u32, u32, u32), body: &[u8]) -> u64 {
        let unique = self.next_unique;
        self.next_unique += 1;
        let mut msg = Vec::with_capacity(40 + body.len());
        msg.extend_from_slice(&((40 + body.len()) as u32).to_le_bytes());
        msg.extend_from_slice(&opcode.to_le_bytes());
        msg.extend_from_slice(&unique.to_le_bytes());
        msg.extend_from_slice(&nodeid.to_le_bytes());
        msg.extend_from_slice(&who.0.to_le_bytes());
        msg.extend_from_slice(&who.1.to_le_bytes());
        msg.extend_from_slice(&who.2.to_le_bytes());
        msg.extend_from_slice(&0u32.to_le_bytes());
        msg.extend_from_slice(body);
        self.sock.send(&msg).expect("send a request");
        unique
    }

    fn recv(&mut self) -> Reply {
        let mut buf = vec![0u8; 1 << 20];
        let n = self.sock.recv(&mut buf).expect("a reply within 20 s");
        assert!(n >= 16, "a reply shorter than its header");
        let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, n, "the reply's length field");
        Reply {
            error: i32::from_le_bytes(buf[4..8].try_into().unwrap()),
            unique: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            body: buf[16..n].to_vec(),
        }
    }

    /// Whether a reply arrives within `within`.
    fn recv_within(&mut self, within: Duration) -> Option<Reply> {
        self.sock.set_read_timeout(Some(within)).unwrap();
        let mut buf = vec![0u8; 1 << 20];
        let got = self.sock.recv(&mut buf);
        self.sock
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let n = got.ok()?;
        Some(Reply {
            error: i32::from_le_bytes(buf[4..8].try_into().unwrap()),
            unique: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            body: buf[16..n].to_vec(),
        })
    }

    /// One request and its reply (the next reply must be this request's).
    fn call(&mut self, opcode: u32, nodeid: u64, body: &Body) -> Reply {
        let unique = self.send(opcode, nodeid, &body.0);
        let reply = self.recv();
        assert_eq!(reply.unique, unique, "a reply for another request");
        reply
    }

    /// `DESTROY`: the session ends cleanly.
    fn finish(mut self) {
        let unique = self.send(op::DESTROY, 0, &[]);
        let reply = self.recv();
        assert_eq!(reply.unique, unique);
        assert_eq!(reply.error, 0);
        let session = self.session.take().unwrap();
        session
            .join()
            .expect("the session thread")
            .expect("the session ends without error");
    }
}

// ------------------------------------------------------------------ setup

fn attr(ino: Ino, kind: FileKind) -> Attr {
    Attr {
        ino,
        kind,
        size: 1000,
        blocks: 2,
        mode: 0o644,
        nlink: 3,
        uid: 7,
        gid: 8,
        rdev: Rdev::new(4, 5),
        atime_ns: 1_500_000_123,
        mtime_ns: 2_500_000_456,
        ctime_ns: 3_500_000_789,
        blksize: 4096,
        ttl: Duration::from_secs(3),
    }
}

fn entry(ino: Ino, kind: FileKind) -> Entry {
    Entry {
        attr: attr(ino, kind),
        generation: 9,
    }
}

fn started(caps: FrontendCaps) -> (Kernel, MockVfs) {
    let mock = MockVfs::new();
    let kernel = Kernel::start(Arc::new(mock.clone()), caps, 1);
    (kernel, mock)
}

fn plain() -> (Kernel, MockVfs) {
    started(FrontendCaps::linux_fuse(false))
}

const RW: i32 = libc::O_RDWR;

// ------------------------------------------------------------------ tests

#[test]
fn init_negotiates_the_kernels_queue_and_the_lock_capabilities() {
    for (cluster, workers) in [(true, 4), (false, 4), (true, 1)] {
        let mock = MockVfs::new();
        let kernel = Kernel::start(
            Arc::new(mock.clone()),
            FrontendCaps::linux_fuse(cluster),
            workers,
        );
        let flags = kernel.init.u32_at(12);
        let locks = (1 << 1) | (1 << 10);
        assert_eq!(kernel.init.u32_at(0), 7, "major");
        if cluster {
            assert_eq!(
                flags & locks,
                locks,
                "cluster locks ask for POSIX and flock locks"
            );
        } else {
            assert_eq!(flags & locks, 0, "local locks stay in the kernel");
        }
        let tuning = KernelTuning::for_workers(workers);
        assert_eq!(
            kernel.init.body[16..18],
            tuning.max_background.to_le_bytes()
        );
        assert_eq!(
            kernel.init.body[18..20],
            tuning.congestion_threshold.to_le_bytes()
        );
        assert_eq!(
            flags & (1 << 18) != 0,
            workers > 1,
            "parallel directory operations follow the worker count"
        );
        kernel.finish();
    }
}

#[test]
fn lookup_translates_the_call_and_encodes_the_entry() {
    let (mut k, mock) = plain();
    mock.always_lookup(Script::ok(entry(42, FileKind::CharDev)));
    let r = k.call(op::LOOKUP, 5, &Body::new().cstr("name"));
    let body = r.ok();
    assert_eq!(body.len(), 128, "fuse_entry_out");
    assert_eq!(r.u64_at(0), 42, "nodeid is the inode");
    assert_eq!(r.u64_at(8), 9, "generation");
    assert_eq!(r.u64_at(16), 3, "entry_valid is the attr ttl in seconds");
    assert_eq!(r.u64_at(24), 3, "attr_valid");
    let a = wire_attr(&body[40..]);
    assert_eq!(
        a,
        WireAttr {
            ino: 42,
            size: 1000,
            blocks: 2,
            atime: 1,
            mtime: 2,
            ctime: 3,
            atimensec: 500_000_123,
            mode: modebits::S_IFCHR | 0o644,
            nlink: 3,
            uid: 7,
            gid: 8,
            rdev: (4 << 8) | 5,
            blksize: 4096,
        }
    );
    // What the Vfs was asked, by whom, on which thread.
    let call = mock.last_call().unwrap();
    assert_eq!(
        call.args,
        Args::Lookup {
            parent: 5,
            name: "name".into()
        }
    );
    assert_eq!(
        (call.caller.uid, call.caller.gid, call.caller.pid),
        (1000, 100, Some(4242))
    );
    assert_eq!(
        call.thread.name.as_deref(),
        Some("fuser-0"),
        "inline, on the fuser worker"
    );
    k.finish();
}

#[test]
fn a_request_from_the_kernel_itself_has_no_pid() {
    let (mut k, mock) = plain();
    mock.always_getattr(Script::ok(attr(1, FileKind::Dir)));
    let unique = k.send_as(
        op::GETATTR,
        1,
        (0, 0, 0),
        &Body::new().u32(0).u32(0).u64(0).0,
    );
    let r = k.recv();
    assert_eq!(r.unique, unique);
    let call = mock.last_call().unwrap();
    assert_eq!((call.caller.uid, call.caller.pid), (0, None));
    k.finish();
}

#[test]
fn getattr_passes_the_handle_only_when_the_kernel_sent_one() {
    let (mut k, mock) = plain();
    mock.always_getattr(Script::ok(attr(7, FileKind::File)));
    let r = k.call(op::GETATTR, 7, &Body::new().u32(1).u32(0).u64(55));
    assert_eq!(r.ok().len(), 104, "fuse_attr_out");
    assert_eq!(r.u64_at(0), 3, "attr_valid");
    assert_eq!(wire_attr(&r.body[16..]).mode, modebits::S_IFREG | 0o644);
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Getattr {
            ino: 7,
            fh: Some(Fh(55))
        }
    );
    k.call(op::GETATTR, 7, &Body::new().u32(0).u32(0).u64(55))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Getattr { ino: 7, fh: None }
    );
    k.finish();
}

#[test]
fn setattr_decodes_the_valid_bits_and_the_times() {
    let (mut k, mock) = plain();
    mock.always_setattr(Script::ok(attr(7, FileKind::File)));
    const MODE: u32 = 1;
    const UID: u32 = 2;
    const GID: u32 = 4;
    const SIZE: u32 = 8;
    const ATIME: u32 = 16;
    const MTIME: u32 = 32;
    const FH: u32 = 64;
    const ATIME_NOW: u32 = 128;
    let setattr = |valid: u32| {
        Body::new()
            .u32(valid)
            .u32(0)
            .u64(11) // fh
            .u64(4096) // size
            .u64(0) // lock_owner
            .i64(1_000) // atime
            .i64(2_000) // mtime
            .i64(0) // ctime
            .u32(111) // atimensec
            .u32(222) // mtimensec
            .u32(0)
            .u32(0o640) // mode
            .u32(0)
            .u32(501) // uid
            .u32(502) // gid
            .u32(0)
    };
    k.call(
        op::SETATTR,
        7,
        &setattr(MODE | UID | GID | SIZE | ATIME | MTIME | FH),
    )
    .ok();
    let Args::Setattr { ino, fh, set } = mock.last_call().unwrap().args else {
        panic!("not a setattr");
    };
    assert_eq!((ino, fh), (7, Some(Fh(11))));
    assert_eq!(set.mode, Some(0o640));
    assert_eq!(
        (set.uid, set.gid, set.size),
        (Some(501), Some(502), Some(4096))
    );
    assert_eq!(set.atime, Some(TimeSet::At(1_000_000_000_111)));
    assert_eq!(set.mtime, Some(TimeSet::At(2_000_000_000_222)));
    // Nothing valid, nothing set; "now" is the engine's clock.
    k.call(op::SETATTR, 7, &setattr(0)).ok();
    let Args::Setattr { fh, set, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(fh, None);
    assert_eq!(
        (set.mode, set.uid, set.gid, set.size, set.atime, set.mtime),
        (None, None, None, None, None, None)
    );
    // The kernel sets FATTR_ATIME_NOW together with FATTR_ATIME (utimensat's
    // UTIME_NOW).
    k.call(op::SETATTR, 7, &setattr(ATIME | ATIME_NOW | MTIME))
        .ok();
    let Args::Setattr { set, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(set.atime, Some(TimeSet::Now));
    assert_eq!(set.mtime, Some(TimeSet::At(2_000_000_000_222)));
    k.finish();
}

#[test]
fn the_namespace_ops_carry_their_names_and_modes() {
    let (mut k, mock) = plain();
    mock.always_mknod(Script::ok(entry(20, FileKind::Fifo)));
    mock.always_mkdir(Script::ok(entry(21, FileKind::Dir)));
    mock.always_symlink(Script::ok(entry(22, FileKind::Symlink)));
    mock.always_link(Script::ok(entry(23, FileKind::File)));
    mock.always_unlink(Script::ok(()));
    mock.always_rmdir(Script::ok(()));
    mock.always_readlink(Script::ok(b"target/path".to_vec()));
    // mknod: the kernel's 32-bit rdev becomes the portable pair.
    let dev = (8u32 << 8) | 1;
    k.call(
        op::MKNOD,
        1,
        &Body::new()
            .u32(modebits::S_IFBLK | 0o660)
            .u32(dev)
            .u32(0o22)
            .u32(0)
            .cstr("sda1"),
    )
    .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Mknod {
            parent: 1,
            name: "sda1".into(),
            mode: modebits::S_IFBLK | 0o660,
            rdev: Rdev::new(8, 1)
        }
    );
    k.call(op::MKDIR, 1, &Body::new().u32(0o755).u32(0o22).cstr("d"))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Mkdir {
            parent: 1,
            name: "d".into(),
            mode: 0o755
        }
    );
    k.call(op::SYMLINK, 1, &Body::new().cstr("ln").cstr("some/where"))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Symlink {
            parent: 1,
            name: "ln".into(),
            target: b"some/where".to_vec()
        }
    );
    k.call(op::LINK, 3, &Body::new().u64(30).cstr("hard")).ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Link {
            ino: 30,
            new_parent: 3,
            new_name: "hard".into()
        }
    );
    k.call(op::UNLINK, 1, &Body::new().cstr("gone")).ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Unlink {
            parent: 1,
            name: "gone".into()
        }
    );
    k.call(op::RMDIR, 1, &Body::new().cstr("dir")).ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Rmdir {
            parent: 1,
            name: "dir".into()
        }
    );
    let r = k.call(op::READLINK, 22, &Body::new());
    assert_eq!(r.ok(), b"target/path");
    // A name that is not UTF-8 reaches the Vfs as the bytes it is.
    k.call(op::UNLINK, 1, &Body::new().bytes(b"caf\xe9").bytes(&[0]))
        .ok();
    let Args::Unlink { name, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(name.as_bytes(), b"caf\xe9");
    k.finish();
}

#[test]
fn rename_carries_its_flags_decoded() {
    let (mut k, mock) = plain();
    mock.always_rename(Script::ok(()));
    k.call(op::RENAME, 1, &Body::new().u64(2).cstr("a").cstr("b"))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Rename {
            parent: 1,
            name: "a".into(),
            new_parent: 2,
            new_name: "b".into(),
            flags: RenameFlags::empty()
        }
    );
    for (raw, want) in [
        (libc::RENAME_NOREPLACE, RenameFlags::NOREPLACE),
        (libc::RENAME_EXCHANGE, RenameFlags::EXCHANGE),
        (
            libc::RENAME_NOREPLACE | libc::RENAME_WHITEOUT,
            RenameFlags::NOREPLACE | RenameFlags::WHITEOUT,
        ),
        (1 << 20, RenameFlags::UNSUPPORTED),
    ] {
        k.call(
            op::RENAME2,
            1,
            &Body::new().u64(2).u32(raw).u32(0).cstr("a").cstr("b"),
        )
        .ok();
        let Args::Rename {
            flags, new_parent, ..
        } = mock.last_call().unwrap().args
        else {
            panic!()
        };
        assert_eq!((flags, new_parent), (want, 2), "raw {raw:#x}");
    }
    k.finish();
}

#[test]
fn open_and_create_decode_the_flag_word() {
    let (mut k, mock) = plain();
    mock.always_open(Script::ok(Opened { fh: Fh(77) }));
    mock.always_create(Script::ok((
        entry(50, FileKind::File),
        Opened { fh: Fh(78) },
    )));
    let r = k.call(
        op::OPEN,
        50,
        &Body::new()
            .i32(libc::O_WRONLY | libc::O_APPEND | libc::O_SYNC)
            .u32(0),
    );
    assert_eq!(r.ok().len(), 16, "fuse_open_out");
    assert_eq!(r.u64_at(0), 77, "the handle");
    assert_eq!(r.u32_at(8), 0, "no fopen flags");
    let Args::Open { ino, flags, owner } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!((ino, owner), (50, OpenOwner::NONE));
    assert_eq!(
        flags,
        OpenFlags::WRITE | OpenFlags::APPEND | OpenFlags::SYNC
    );
    k.call(op::OPEN, 50, &Body::new().i32(libc::O_RDONLY).u32(0))
        .ok();
    let Args::Open { flags, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(flags, OpenFlags::READ);
    k.call(op::OPEN, 50, &Body::new().i32(RW | libc::O_TRUNC).u32(0))
        .ok();
    let Args::Open { flags, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(flags, OpenFlags::READ | OpenFlags::WRITE | OpenFlags::TRUNC);
    // create: an entry and an open in one reply.
    let r = k.call(
        op::CREATE,
        1,
        &Body::new()
            .i32(RW | libc::O_CREAT | libc::O_EXCL)
            .u32(0o100_644)
            .u32(0o22)
            .u32(0)
            .cstr("new"),
    );
    assert_eq!(r.ok().len(), 128 + 16, "fuse_entry_out + fuse_open_out");
    assert_eq!(r.u64_at(0), 50);
    assert_eq!(r.u64_at(128), 78, "the handle follows the entry");
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Create {
            parent: 1,
            name: "new".into(),
            mode: 0o100_644,
            flags: OpenFlags::READ | OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::EXCL,
            owner: OpenOwner::NONE,
        }
    );
    k.finish();
}

#[test]
fn read_write_flush_release_and_fsync_translate() {
    let (mut k, mock) = plain();
    mock.always_read(Script::ok(ReadData::from_vec(b"hello".to_vec())));
    mock.always_write(Script::ok(5));
    mock.always_flush(Script::ok(()));
    mock.always_release(Script::ok(()));
    mock.always_fsync(Script::ok(()));
    let r = k.call(
        op::READ,
        9,
        &Body::new()
            .u64(3)
            .u64(4096)
            .u32(64)
            .u32(0)
            .u64(0)
            .i32(RW)
            .u32(0),
    );
    assert_eq!(r.ok(), b"hello");
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Read {
            ino: 9,
            fh: Fh(3),
            off: 4096,
            len: 64
        }
    );
    let r = k.call(
        op::WRITE,
        9,
        &Body::new()
            .u64(3)
            .u64(10)
            .u32(5)
            .u32(0)
            .u64(0)
            .i32(libc::O_WRONLY | libc::O_SYNC)
            .u32(0)
            .bytes(b"world"),
    );
    assert_eq!(r.ok().len(), 8, "fuse_write_out");
    assert_eq!(r.u32_at(0), 5);
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Write {
            ino: 9,
            fh: Fh(3),
            off: 10,
            data: b"world".to_vec(),
            flags: OpenFlags::WRITE | OpenFlags::SYNC
        }
    );
    k.call(op::FLUSH, 9, &Body::new().u64(3).u32(0).u32(0).u64(0xABCD))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Flush {
            ino: 9,
            fh: Fh(3),
            owner: LockOwner(0xABCD)
        }
    );
    // The lock owner reaches release only for the flock-unlock flavour.
    k.call(op::RELEASE, 9, &Body::new().u64(3).i32(RW).u32(0).u64(0xEE))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Release {
            ino: 9,
            fh: Fh(3),
            flags: OpenFlags::READ | OpenFlags::WRITE,
            owner: None
        }
    );
    k.call(
        op::RELEASE,
        9,
        &Body::new().u64(3).i32(RW).u32(1 << 1).u64(0xEE),
    )
    .ok();
    let Args::Release { owner, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(owner, Some(LockOwner(0xEE)));
    for datasync in [0u32, 1] {
        k.call(op::FSYNC, 9, &Body::new().u64(3).u32(datasync).u32(0))
            .ok();
        assert_eq!(
            mock.last_call().unwrap().args,
            Args::Fsync {
                ino: 9,
                fh: Fh(3),
                level: Durability::Configured
            },
            "fsync(2) and fdatasync(2) both ask for what the view is configured for"
        );
    }
    k.finish();
}

fn dirent(ino: u64, next: u64, kind: FileKind, name: &str) -> DirEntry {
    DirEntry {
        ino,
        next,
        kind,
        name: name.into(),
    }
}

/// The dirents in a `READDIR` reply: (ino, off, type, name).
fn parse_dirents(mut b: &[u8]) -> Vec<(u64, u64, u32, String)> {
    let mut out = Vec::new();
    while b.len() >= 24 {
        let ino = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let off = u64::from_le_bytes(b[8..16].try_into().unwrap());
        let namelen = u32::from_le_bytes(b[16..20].try_into().unwrap()) as usize;
        let typ = u32::from_le_bytes(b[20..24].try_into().unwrap());
        let name = String::from_utf8_lossy(&b[24..24 + namelen]).into_owned();
        out.push((ino, off, typ, name));
        b = &b[(24 + namelen).next_multiple_of(8)..];
    }
    out
}

#[test]
fn readdir_encodes_dirents_into_the_kernels_buffer_and_stops_when_it_is_full() {
    let (mut k, mock) = plain();
    let entries: Vec<DirEntry> = std::iter::once(dirent(1, 1, FileKind::Dir, "."))
        .chain([dirent(1, 2, FileKind::Dir, "..")])
        .chain((0..20).map(|i| dirent(100 + i, 3 + i, FileKind::File, &format!("file-{i:02}"))))
        .collect();
    mock.always_readdir(Script::ok(entries));
    // A big buffer: everything.
    let readdir = |off: u64, size: u32| {
        Body::new()
            .u64(4)
            .u64(off)
            .u32(size)
            .u32(0)
            .u64(0)
            .i32(0)
            .u32(0)
    };
    let r = k.call(op::READDIR, 1, &readdir(0, 8192));
    let all = parse_dirents(r.ok());
    assert_eq!(all.len(), 22);
    assert_eq!(all[0], (1, 1, 4, ".".to_string()), "DT_DIR");
    assert_eq!(
        all[2],
        (100, 3, 8, "file-00".to_string()),
        "DT_REG, the resume cookie"
    );
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Readdir {
            ino: 1,
            fh: Fh(4),
            cookie: 0,
            plus: false
        }
    );
    // A small one: the sink says "full" and the reply stops early, at a
    // whole dirent.
    let r = k.call(op::READDIR, 1, &readdir(0, 128));
    let some = parse_dirents(r.ok());
    assert!(
        !some.is_empty() && some.len() < 22,
        "{} entries in 128 bytes",
        some.len()
    );
    assert!(r.body.len() <= 128);
    assert_eq!(some[..], all[..some.len()], "a prefix of the listing");
    // The cookie is passed through to resume from.
    k.call(op::READDIR, 1, &readdir(17, 8192)).ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Readdir {
            ino: 1,
            fh: Fh(4),
            cookie: 17,
            plus: false
        }
    );
    k.finish();
}

#[test]
fn statfs_encodes_every_field() {
    let (mut k, mock) = plain();
    mock.always_statfs(Script::ok(StatFs {
        blocks: 1000,
        bfree: 600,
        bavail: 500,
        files: 77,
        ffree: 66,
        bsize: 4096,
        namelen: 255,
        frsize: 4096,
    }));
    let r = k.call(op::STATFS, 1, &Body::new());
    assert_eq!(r.ok().len(), 80);
    let want = [1000u64, 600, 500, 77, 66];
    for (i, w) in want.iter().enumerate() {
        assert_eq!(r.u64_at(i * 8), *w, "field {i}");
    }
    assert_eq!(
        (r.u32_at(40), r.u32_at(44), r.u32_at(48)),
        (4096, 255, 4096)
    );
    k.finish();
}

#[test]
fn xattrs_decode_their_flags_and_follow_the_size_probe_protocol() {
    let (mut k, mock) = plain();
    mock.always_setxattr(Script::ok(()));
    mock.always_getxattr(Script::ok(b"blue".to_vec()));
    mock.always_listxattr(Script::ok(vec![
        XattrNameBuf::from("user.a"),
        XattrNameBuf::from("user.bb"),
    ]));
    mock.always_removexattr(Script::ok(()));
    for (raw, want) in [
        (0, SetXattrFlags::empty()),
        (libc::XATTR_CREATE, SetXattrFlags::CREATE),
        (libc::XATTR_REPLACE, SetXattrFlags::REPLACE),
        (
            libc::XATTR_CREATE | libc::XATTR_REPLACE,
            SetXattrFlags::CREATE | SetXattrFlags::REPLACE,
        ),
        (8, SetXattrFlags::UNSUPPORTED),
    ] {
        k.call(
            op::SETXATTR,
            4,
            &Body::new().u32(3).i32(raw).cstr("user.color").bytes(b"red"),
        )
        .ok();
        assert_eq!(
            mock.last_call().unwrap().args,
            Args::Setxattr {
                ino: 4,
                name: "user.color".into(),
                value: b"red".to_vec(),
                flags: want
            },
            "raw {raw}"
        );
    }
    // getxattr: size 0 probes the length, too small is ERANGE, else data.
    let get = |size: u32| Body::new().u32(size).u32(0).cstr("user.color");
    let r = k.call(op::GETXATTR, 4, &get(0));
    assert_eq!(
        (r.ok().len(), r.u32_at(0)),
        (8, 4),
        "fuse_getxattr_out with the size"
    );
    assert_eq!(k.call(op::GETXATTR, 4, &get(3)).errno(), libc::ERANGE);
    assert_eq!(k.call(op::GETXATTR, 4, &get(4)).ok(), b"blue");
    assert_eq!(k.call(op::GETXATTR, 4, &get(100)).ok(), b"blue");
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Getxattr {
            ino: 4,
            name: "user.color".into()
        }
    );
    // listxattr: NUL-terminated names, the same probe.
    let list = |size: u32| Body::new().u32(size).u32(0);
    let r = k.call(op::LISTXATTR, 4, &list(0));
    assert_eq!(r.u32_at(0), 15, "6 + 1 + 7 + 1 bytes");
    assert_eq!(k.call(op::LISTXATTR, 4, &list(10)).errno(), libc::ERANGE);
    assert_eq!(
        k.call(op::LISTXATTR, 4, &list(64)).ok(),
        b"user.a\0user.bb\0"
    );
    k.call(op::REMOVEXATTR, 4, &Body::new().cstr("user.color"))
        .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Removexattr {
            ino: 4,
            name: "user.color".into()
        }
    );
    k.finish();
}

#[test]
fn fallocate_and_lseek_decode_their_modes_and_refuse_what_the_kernel_never_sends() {
    let (mut k, mock) = plain();
    mock.always_fallocate(Script::ok(()));
    mock.always_seek(Script::ok(4096));
    for (raw, want) in [
        (0, FallocateMode::empty()),
        (libc::FALLOC_FL_KEEP_SIZE, FallocateMode::KEEP_SIZE),
        (
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
        ),
        (libc::FALLOC_FL_ZERO_RANGE, FallocateMode::ZERO_RANGE),
        (libc::FALLOC_FL_COLLAPSE_RANGE, FallocateMode::UNSUPPORTED),
    ] {
        k.call(
            op::FALLOCATE,
            6,
            &Body::new().u64(2).u64(100).u64(200).i32(raw).u32(0),
        )
        .ok();
        assert_eq!(
            mock.last_call().unwrap().args,
            Args::Fallocate {
                ino: 6,
                fh: Fh(2),
                off: 100,
                len: 200,
                mode: want
            },
            "raw {raw:#x}"
        );
    }
    let r = k.call(
        op::LSEEK,
        6,
        &Body::new().u64(2).i64(10).i32(libc::SEEK_DATA).u32(0),
    );
    assert_eq!((r.ok().len(), r.u64_at(0)), (8, 4096));
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Seek {
            ino: 6,
            fh: Fh(2),
            off: 10,
            whence: SeekWhence::Data
        }
    );
    k.call(
        op::LSEEK,
        6,
        &Body::new().u64(2).i64(0).i32(libc::SEEK_HOLE).u32(0),
    )
    .ok();
    assert_eq!(
        mock.last_call().unwrap().args,
        Args::Seek {
            ino: 6,
            fh: Fh(2),
            off: 0,
            whence: SeekWhence::Hole
        }
    );
    // Refused by the adapter, never asked of the Vfs.
    let calls = mock.calls().len();
    let r = k.call(
        op::LSEEK,
        6,
        &Body::new().u64(2).i64(-1).i32(libc::SEEK_DATA).u32(0),
    );
    assert_eq!(r.errno(), libc::ENXIO, "a negative offset");
    let r = k.call(op::LSEEK, 6, &Body::new().u64(2).i64(0).i32(99).u32(0));
    assert_eq!(r.errno(), libc::EINVAL, "a whence past SEEK_MAX");
    assert_eq!(mock.calls().len(), calls, "neither reached the Vfs");
    k.finish();
}

#[test]
fn locks_decode_their_types_and_the_blocking_flavour() {
    let (mut k, mock) = started(FrontendCaps::linux_fuse(true));
    mock.always_lock_acquire(Script::ok(()));
    mock.always_lock_release(Script::ok(()));
    let lk = |owner: u64, start: u64, end: u64, typ: i32, pid: u32| {
        Body::new()
            .u64(8)
            .u64(owner)
            .u64(start)
            .u64(end)
            .i32(typ)
            .u32(pid)
            .u32(0)
            .u32(0)
    };
    // F_SETLK / F_SETLKW read and write locks.
    for (opcode, typ, sleep, kind) in [
        (op::SETLK, libc::F_RDLCK, false, LockKind::Read),
        (op::SETLK, libc::F_WRLCK, false, LockKind::Write),
        (op::SETLKW, libc::F_WRLCK, true, LockKind::Write),
        (op::SETLKW, libc::F_RDLCK, true, LockKind::Read),
    ] {
        k.call(opcode, 12, &lk(0x11, 100, 199, typ, 3131)).ok();
        assert_eq!(
            mock.last_call().unwrap().args,
            Args::LockAcquire {
                ino: 12,
                fh: Fh(8),
                lock: LockSpec {
                    owner: LockOwner(0x11),
                    range: LockRange {
                        start: 100,
                        end: 199
                    },
                    kind,
                    pid: 3131
                },
                sleep
            }
        );
    }
    // F_UNLCK is a release, whichever opcode carried it.
    for opcode in [op::SETLK, op::SETLKW] {
        k.call(opcode, 12, &lk(0x11, 100, 199, libc::F_UNLCK, 3131))
            .ok();
        assert_eq!(
            mock.last_call().unwrap().args,
            Args::LockRelease {
                ino: 12,
                fh: Fh(8),
                owner: LockOwner(0x11),
                range: LockRange {
                    start: 100,
                    end: 199
                }
            }
        );
    }
    // A type the kernel never sends is EINVAL and never reaches the Vfs.
    let calls = mock.calls().len();
    assert_eq!(
        k.call(op::SETLK, 12, &lk(1, 0, 1, 7, 1)).errno(),
        libc::EINVAL
    );
    assert_eq!(mock.calls().len(), calls);
    k.finish();
}

#[test]
fn getlk_encodes_a_conflict_and_the_lack_of_one() {
    let (mut k, mock) = started(FrontendCaps::linux_fuse(true));
    mock.on_lock_test(Script::ok(LockStatus::Locked {
        range: LockRange { start: 10, end: 20 },
        kind: LockKind::Write,
        pid: 999,
    }));
    mock.on_lock_test(Script::ok(LockStatus::Locked {
        range: LockRange { start: 0, end: 5 },
        kind: LockKind::Read,
        pid: 0,
    }));
    mock.on_lock_test(Script::ok(LockStatus::Unlocked));
    let lk = |typ: i32| {
        Body::new()
            .u64(8)
            .u64(0x22)
            .u64(0)
            .u64(100)
            .i32(typ)
            .u32(55)
            .u32(0)
            .u32(0)
    };
    let r = k.call(op::GETLK, 12, &lk(libc::F_WRLCK));
    assert_eq!(
        (
            r.ok().len(),
            r.u64_at(0),
            r.u64_at(8),
            r.u32_at(16) as i32,
            r.u32_at(20)
        ),
        (24, 10, 20, libc::F_WRLCK, 999)
    );
    // A write test asks about any lock; a read test about write locks only.
    let Args::LockTest { lock, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(
        (lock.kind, lock.owner, lock.pid),
        (LockKind::Write, LockOwner(0x22), 55)
    );
    let r = k.call(op::GETLK, 12, &lk(libc::F_RDLCK));
    assert_eq!(
        (r.u64_at(0), r.u64_at(8), r.u32_at(16) as i32, r.u32_at(20)),
        (0, 5, libc::F_RDLCK, 0)
    );
    let Args::LockTest { lock, .. } = mock.last_call().unwrap().args else {
        panic!()
    };
    assert_eq!(lock.kind, LockKind::Read);
    let r = k.call(op::GETLK, 12, &lk(libc::F_WRLCK));
    assert_eq!(
        (r.u64_at(0), r.u64_at(8), r.u32_at(16) as i32, r.u32_at(20)),
        (0, 0, libc::F_UNLCK, 0)
    );
    k.finish();
}

#[test]
fn every_code_reaches_the_kernel_as_its_linux_errno() {
    let (mut k, mock) = plain();
    for &code in Code::ALL {
        mock.always_getattr(Script::fail(code));
        mock.always_unlink(Script::fail(code));
        mock.always_open(Script::fail(code));
        let r = k.call(op::GETATTR, 1, &Body::new().u32(0).u32(0).u64(0));
        assert_eq!(r.error, -code.to_linux_errno(), "getattr {code:?}");
        assert!(r.body.is_empty());
        assert_eq!(
            k.call(op::UNLINK, 1, &Body::new().cstr("x")).errno(),
            code.to_linux_errno(),
            "unlink {code:?}"
        );
        assert_eq!(
            k.call(op::OPEN, 1, &Body::new().i32(0).u32(0)).errno(),
            code.to_linux_errno(),
            "open {code:?}"
        );
    }
    // Spot checks against the libc names, so the table itself is checked
    // from outside.
    for (code, errno) in [
        (Code::NotFound, libc::ENOENT),
        (Code::Exists, libc::EEXIST),
        (Code::NotEmpty, libc::ENOTEMPTY),
        (Code::Stale, libc::ESTALE),
        (Code::NoData, libc::ENODATA),
        (Code::CrossDevice, libc::EXDEV),
        (Code::NotImplemented, libc::ENOSYS),
        (Code::Intr, libc::EINTR),
    ] {
        mock.always_getattr(Script::fail(code));
        assert_eq!(
            k.call(op::GETATTR, 1, &Body::new().u32(0).u32(0).u64(0))
                .errno(),
            errno
        );
    }
    k.finish();
}

#[test]
fn a_reply_completed_from_another_thread_does_not_hold_up_the_event_loop() {
    let (mut k, mock) = plain();
    mock.on_lookup(Script::Defer {
        after: Duration::from_millis(300),
        result: Ok(entry(70, FileKind::File)),
    });
    mock.always_getattr(Script::ok(attr(1, FileKind::Dir)));
    let slow = k.send(op::LOOKUP, 1, &Body::new().cstr("slow").0);
    let fast = k.send(op::GETATTR, 1, &Body::new().u32(0).u32(0).u64(0).0);
    // The later request is answered first: the loop was free.
    let first = k.recv();
    assert_eq!(first.unique, fast);
    assert_eq!(first.ok().len(), 104);
    let second = k.recv();
    assert_eq!(second.unique, slow);
    assert_eq!(second.u64_at(0), 70);
    assert!(mock.wait_for_completions(2, Duration::from_secs(5)));
    let done = mock.completions();
    let lookup = done.iter().find(|c| c.op == OpKind::Lookup).unwrap();
    let getattr = done.iter().find(|c| c.op == OpKind::Getattr).unwrap();
    assert_eq!(
        getattr.thread.name.as_deref(),
        Some("fuser-0"),
        "inline on the worker"
    );
    assert_eq!(
        lookup.thread.name.as_deref(),
        Some("mock-deferred"),
        "deferred, elsewhere"
    );
    k.finish();
}

#[test]
fn a_responder_dropped_without_completing_is_answered_eio() {
    let (mut k, mock) = plain();
    mock.on_lookup(Script::Drop);
    mock.on_read(Script::Drop);
    let r = k.call(op::LOOKUP, 1, &Body::new().cstr("lost"));
    assert_eq!(
        r.errno(),
        libc::EIO,
        "fuser's reply answers EIO from its Drop"
    );
    assert!(mock.completions().is_empty(), "nothing completed it");
    let r = k.call(
        op::READ,
        9,
        &Body::new().u64(3).u64(0).u32(8).u32(0).u64(0).i32(0).u32(0),
    );
    assert_eq!(r.errno(), libc::EIO);
    // And the session carries on.
    mock.always_getattr(Script::ok(attr(1, FileKind::Dir)));
    k.call(op::GETATTR, 1, &Body::new().u32(0).u32(0).u64(0))
        .ok();
    k.finish();
}

#[test]
fn a_contended_setlkw_is_answered_from_the_view_s_wait_thread_once_released() {
    let mock = MockVfs::reference(FrontendCaps::linux_fuse(true));
    let mut k = Kernel::start(Arc::new(mock.clone()), FrontendCaps::linux_fuse(true), 1);
    // A file, through the adapter.
    let r = k.call(
        op::CREATE,
        ROOT_INO,
        &Body::new()
            .i32(RW | libc::O_CREAT)
            .u32(0o100_644)
            .u32(0)
            .u32(0)
            .cstr("locked"),
    );
    let ino = r.u64_at(0);
    let fh = r.u64_at(128);
    let lk = |owner: u64, typ: i32| {
        Body::new()
            .u64(fh)
            .u64(owner)
            .u64(0)
            .u64(i64::MAX as u64)
            .i32(typ)
            .u32(owner as u32)
            .u32(0)
            .u32(0)
    };
    k.call(op::SETLK, ino, &lk(1, libc::F_WRLCK)).ok();
    assert_eq!(
        k.call(op::SETLK, ino, &lk(2, libc::F_WRLCK)).errno(),
        libc::EAGAIN,
        "F_SETLK does not wait"
    );
    // F_SETLKW parks the request, not the worker.
    let waiter = k.send(op::SETLKW, ino, &lk(2, libc::F_WRLCK).0);
    assert!(
        k.recv_within(Duration::from_millis(200)).is_none(),
        "no answer while the lock is held"
    );
    // The event loop is free meanwhile: a getattr is answered.
    let r = k.call(op::GETATTR, ino, &Body::new().u32(0).u32(0).u64(0));
    assert_eq!(r.ok().len(), 104);
    // Release: the unlock is answered inline, the waiter from its thread.
    let unlock = k.send(op::SETLK, ino, &lk(1, libc::F_UNLCK).0);
    let mut got = [k.recv(), k.recv()];
    got.sort_by_key(|r| r.unique);
    assert_eq!(got[0].unique, waiter);
    assert_eq!(got[1].unique, unlock);
    assert_eq!((got[0].error, got[1].error), (0, 0));
    let wait_done = mock
        .completions()
        .into_iter()
        .rev()
        .find(|c| c.op == OpKind::LockAcquire)
        .unwrap();
    assert_eq!(wait_done.thread.name.as_deref(), Some("ref-lock-wait"));
    k.finish();
}

#[test]
fn forget_needs_no_reply_and_unknown_opcodes_are_refused_not_fatal() {
    let (mut k, mock) = plain();
    mock.always_getattr(Script::ok(attr(1, FileKind::Dir)));
    // FORGET has no reply: the next reply belongs to the next request.
    k.send(op::FORGET, 5, &Body::new().u64(1).0);
    let r = k.call(op::GETATTR, 1, &Body::new().u32(0).u32(0).u64(0));
    r.ok();
    // OPENDIR is not implemented by the adapter: fuser's default opens
    // directory handle 0.
    let r = k.call(op::OPENDIR, 1, &Body::new().i32(0).u32(0));
    assert_eq!(r.error, 0);
    // An opcode nobody implements: ENOSYS, and the session lives.
    let unique = k.send(9999, 1, &[]);
    let r = k.recv();
    assert_eq!(r.unique, unique);
    assert_eq!(r.errno(), libc::ENOSYS);
    k.call(op::GETATTR, 1, &Body::new().u32(0).u32(0).u64(0))
        .ok();
    k.finish();
}
