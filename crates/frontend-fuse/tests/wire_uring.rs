//! The FUSE adapter over FUSE-over-io_uring, end to end without a kernel
//! (plan 38 §6, Z2a): `tests/wire.rs`'s method, with a ring.
//!
//! `wire.rs` drives the adapter through a socket pair standing in for
//! `/dev/fuse`. A ring cannot be faked that way — `IORING_OP_URING_CMD`
//! does not target a socket — so the seam is one level down, at the ring's
//! own io_uring: fuser's `InMemoryRingKernel` decodes the SQEs a ring
//! thread pushes exactly as `fs/fuse/dev_uring.c` does (`REGISTER` with the
//! entry's two iovecs, `COMMIT_AND_FETCH` with a commit id, the wake
//! eventfd's poll), writes each request into a registered entry the way
//! the kernel scatters it (`fuse_in_header`, `op_in`, payload, trailer), and
//! reads each reply back out of the entry. Everything above that — the
//! session, its ring threads, the entry state machine, the offload threads,
//! fuser's decoding, [`FuseFs`], the responders, fuser's encoding — is the
//! production code. `FUSE_INIT` (offering `FUSE_OVER_IO_URING`) and
//! `FUSE_DESTROY` still travel over the socket pair, as they travel over
//! `/dev/fuse` in the kernel.
//!
//! Plan 38 Z4 adds a third leg, a ring with zero-copy queues
//! (`InMemoryRingKernel::with_buffer_pools`, `FUSE_HAS_IO_URING_BUFPOOL`
//! offered): every request's payload travels in a pool buffer the "kernel"
//! picks, and each read is sent as a zero-copied one, its data expected in
//! registered pages rather than in the payload buffer — which the adapter,
//! answering with `gather` as it does on any ring, reaches through fuser's
//! bounce `READ_FIXED`.
//!
//! What this proves: the same requests, against the same scripted
//! [`MockVfs`], produce **byte-identical replies and identical `Vfs`
//! calls** on every transport — reads of one segment, of several, short and
//! empty, writes, opens, getattrs, lookups, the close path; a cold read
//! answered from a foreign thread completes over the ring; and a request
//! that blocks in the filesystem (a flush waiting on S3) does not stall an
//! unrelated request the kernel queued on the same ring (plan 38 Z1b's
//! `s3-cut-one-node` finding, owned by Z2a).

#![cfg(all(target_os = "linux", feature = "io-uring"))]

use constellation_frontend_fuse::{FuseFs, KernelTuning};
use constellation_types::Rdev;
use constellation_vfs::mock::{Args, MockVfs, Script};
use constellation_vfs::{
    Attr, Entry, Fh, FileKind, FrontendCaps, Ino, LockOwner, OpKind, OpenFlags, OpenOwner, Opened,
    ReadData,
};
use fuser::{InMemoryRingKernel, RegistrationRefused, Transport};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixDatagram;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

mod op {
    pub const LOOKUP: u32 = 1;
    pub const GETATTR: u32 = 3;
    pub const OPEN: u32 = 14;
    pub const READ: u32 = 15;
    pub const WRITE: u32 = 16;
    pub const RELEASE: u32 = 18;
    pub const FSYNC: u32 = 20;
    pub const FLUSH: u32 = 25;
    pub const INIT: u32 = 26;
    pub const SETLK: u32 = 32;
    pub const SETLKW: u32 = 33;
    pub const DESTROY: u32 = 38;
}

/// The size of each op's fixed argument struct, which the kernel puts in
/// the entry's `op_in` (`in_args[0]`); everything after it is payload.
/// `LOOKUP`'s only argument is its name, so it is all payload.
fn op_in_len(opcode: u32) -> usize {
    match opcode {
        op::LOOKUP => 0,
        op::GETATTR => 16, // fuse_getattr_in
        op::OPEN => 8,     // fuse_open_in
        op::READ | op::WRITE => 40,
        op::FLUSH | op::RELEASE => 24,
        op::FSYNC => 16,
        op::SETLK | op::SETLKW => 48, // fuse_lk_in
        other => panic!("no op_in size for opcode {other}"),
    }
}

#[derive(Default)]
struct Body(Vec<u8>);

impl Body {
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
    fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend_from_slice(b);
        self
    }
    fn cstr(self, s: &str) -> Self {
        self.bytes(s.as_bytes()).bytes(&[0])
    }
}

fn getattr_body() -> Body {
    Body::default().u32(0).u32(0).u64(0)
}

fn read_body(fh: u64, off: u64, size: u32) -> Body {
    Body::default()
        .u64(fh)
        .u64(off)
        .u32(size)
        .u32(0)
        .u64(0)
        .i32(libc::O_RDWR)
        .u32(0)
}

/// One reply: the raw bytes, header included.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reply(Vec<u8>);

impl Reply {
    fn unique(&self) -> u64 {
        u64::from_le_bytes(self.0[8..16].try_into().unwrap())
    }
    fn error(&self) -> i32 {
        i32::from_le_bytes(self.0[4..8].try_into().unwrap())
    }
    fn body(&self) -> &[u8] {
        assert_eq!(self.error(), 0, "the reply is an error");
        assert_eq!(
            u32::from_le_bytes(self.0[0..4].try_into().unwrap()) as usize,
            self.0.len(),
            "the reply's length field"
        );
        &self.0[16..]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    DevFuse,
    Ring,
    /// Plan 38 Z4: a ring of zero-copy queues with buffer pools.
    RingZeroCopy,
}

/// The kernel's end, over either transport.
struct Kernel {
    sock: UnixDatagram,
    ring: Option<InMemoryRingKernel>,
    next_unique: u64,
    session: Option<JoinHandle<std::io::Result<()>>>,
    transport: Transport,
    /// Blocking lock requests the lock-wait budget served as non-blocking
    /// (`Config::io_uring_lock_wait_downgrades`).
    lock_wait_downgrades: Arc<std::sync::atomic::AtomicU64>,
}

const KERNEL_INIT_FLAGS: u32 = (1 << 1) | (1 << 10) | (1 << 18);
const FUSE_INIT_EXT: u32 = 1 << 30;
/// `FUSE_OVER_IO_URING`, bit 41: bit 9 of `flags2`.
const FUSE_OVER_IO_URING_HI: u32 = 1 << 9;
/// `FUSE_HAS_IO_URING_BUFPOOL`, bit 43: bit 11 of `flags2` (7.3+).
const FUSE_HAS_IO_URING_BUFPOOL_HI: u32 = 1 << 11;
/// Entries per queue: two, so a queue whose one entry is held by a
/// blocked request still has one to fetch the next.
const DEPTH: u32 = 2;

impl Kernel {
    fn start(mock: &MockVfs, leg: Leg) -> Kernel {
        Self::try_start(mock, leg, None).expect("the session starts")
    }

    fn try_start(
        mock: &MockVfs,
        leg: Leg,
        kernel: Option<InMemoryRingKernel>,
    ) -> std::io::Result<Kernel> {
        Self::try_start_with(mock, leg, kernel, FrontendCaps::linux_fuse(false))
    }

    fn try_start_with(
        mock: &MockVfs,
        leg: Leg,
        kernel: Option<InMemoryRingKernel>,
        caps: FrontendCaps,
    ) -> std::io::Result<Kernel> {
        let (sock, daemon) = UnixDatagram::pair().expect("socketpair");
        sock.set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let ring = match leg {
            Leg::DevFuse => None,
            Leg::Ring => Some(kernel.unwrap_or_default()),
            Leg::RingZeroCopy => {
                Some(kernel.unwrap_or_else(|| InMemoryRingKernel::with_buffer_pools(true)))
            }
        };
        let mut k = Kernel {
            sock,
            ring: ring.clone(),
            next_unique: 1,
            session: None,
            transport: Transport::DevFuse,
            lock_wait_downgrades: Arc::default(),
        };
        let (flags, flags2) = match leg {
            Leg::DevFuse => (KERNEL_INIT_FLAGS, 0),
            Leg::Ring => (KERNEL_INIT_FLAGS | FUSE_INIT_EXT, FUSE_OVER_IO_URING_HI),
            Leg::RingZeroCopy => (
                KERNEL_INIT_FLAGS | FUSE_INIT_EXT,
                FUSE_OVER_IO_URING_HI | FUSE_HAS_IO_URING_BUFPOOL_HI,
            ),
        };
        let body = Body::default()
            .u32(7)
            .u32(45)
            .u32(1 << 17)
            .u32(flags)
            .u32(flags2)
            .bytes(&[0u8; 44]);
        let init = k.send_dev(op::INIT, 0, &body.0);
        let mut config = fuser::Config::default();
        config.acl = fuser::SessionACL::All;
        // Over the socket pair a second `/dev/fuse` reader could never be
        // woken at the end (nothing plays the kernel's `ENODEV`), so the
        // dev leg has one, as `wire.rs` does; the ring leg has two rings
        // and its single `/dev/fuse` reader ends with `DESTROY`.
        config.n_threads = Some(if leg == Leg::DevFuse { 1 } else { 2 });
        config.io_uring = leg != Leg::DevFuse;
        config.io_uring_queue_depth = DEPTH;
        config.io_uring_kernel = ring;
        config.io_uring_lock_wait_downgrades = Some(fuser::LockWaitDowngrades::new({
            let n = k.lock_wait_downgrades.clone();
            move || {
                n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }));
        let fs = FuseFs::new(Arc::new(mock.clone()), caps, KernelTuning::for_workers(2));
        let session =
            fuser::Session::from_fd(fs, OwnedFd::from(daemon), fuser::SessionACL::All, config)?;
        k.transport = session.transport();
        let reply = k.recv_dev();
        assert_eq!(reply.unique(), init);
        assert_eq!(reply.error(), 0, "FUSE_INIT");
        k.session = Some(std::thread::spawn(move || session.run()));
        Ok(k)
    }

    fn message(&mut self, opcode: u32, nodeid: u64, body: &[u8]) -> (u64, Vec<u8>) {
        let unique = self.next_unique;
        self.next_unique += 1;
        let mut msg = Vec::with_capacity(40 + body.len());
        msg.extend_from_slice(&((40 + body.len()) as u32).to_le_bytes());
        msg.extend_from_slice(&opcode.to_le_bytes());
        msg.extend_from_slice(&unique.to_le_bytes());
        msg.extend_from_slice(&nodeid.to_le_bytes());
        msg.extend_from_slice(&1000u32.to_le_bytes());
        msg.extend_from_slice(&100u32.to_le_bytes());
        msg.extend_from_slice(&4242u32.to_le_bytes());
        msg.extend_from_slice(&0u32.to_le_bytes());
        msg.extend_from_slice(body);
        (unique, msg)
    }

    /// Over `/dev/fuse` (the socket), whatever the session's transport.
    fn send_dev(&mut self, opcode: u32, nodeid: u64, body: &[u8]) -> u64 {
        let (unique, msg) = self.message(opcode, nodeid, body);
        self.sock.send(&msg).expect("send a request");
        unique
    }

    fn recv_dev(&mut self) -> Reply {
        let mut buf = vec![0u8; 1 << 20];
        let n = self.sock.recv(&mut buf).expect("a reply within 20 s");
        buf.truncate(n);
        Reply(buf)
    }

    /// A request over this kernel's transport; over the ring, onto queue
    /// `qid` (the CPU the kernel would have queued it from).
    fn send_on(&mut self, qid: u16, opcode: u32, nodeid: u64, body: &Body) -> u64 {
        match self.ring.clone() {
            None => self.send_dev(opcode, nodeid, &body.0),
            Some(ring) => {
                let (unique, msg) = self.message(opcode, nodeid, &body.0);
                if opcode == op::READ && self.transport == Transport::UringZeroCopy {
                    // `fuse_read_in.size`: the pages the kernel would register
                    let size = u32::from_le_bytes(body.0[16..20].try_into().unwrap());
                    ring.send_zero_copy(qid, &msg, op_in_len(opcode), size as usize);
                } else {
                    ring.send(qid, &msg, op_in_len(opcode));
                }
                unique
            }
        }
    }

    fn send(&mut self, opcode: u32, nodeid: u64, body: &Body) -> u64 {
        self.send_on(0, opcode, nodeid, body)
    }

    fn recv_within(&mut self, within: Duration) -> Option<Reply> {
        match &self.ring {
            None => {
                self.sock.set_read_timeout(Some(within)).unwrap();
                let mut buf = vec![0u8; 1 << 20];
                let got = self.sock.recv(&mut buf);
                self.sock
                    .set_read_timeout(Some(Duration::from_secs(20)))
                    .unwrap();
                let n = got.ok()?;
                buf.truncate(n);
                Some(Reply(buf))
            }
            Some(ring) => ring.recv(within).map(Reply),
        }
    }

    fn recv(&mut self) -> Reply {
        self.recv_within(Duration::from_secs(20))
            .expect("a reply within 20 s")
    }

    fn call(&mut self, opcode: u32, nodeid: u64, body: &Body) -> Reply {
        let unique = self.send(opcode, nodeid, body);
        let reply = self.recv();
        assert_eq!(reply.unique(), unique, "a reply for another request");
        reply
    }

    /// `DESTROY` (over the socket, as the kernel sends it), then the
    /// connection ends: the session leaves without an error.
    fn finish(mut self) {
        let unique = self.send_dev(op::DESTROY, 0, &[]);
        let reply = self.recv_dev();
        assert_eq!((reply.unique(), reply.error()), (unique, 0));
        if let Some(ring) = &self.ring {
            ring.hang_up();
        }
        let session = self.session.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(session.join());
        });
        rx.recv_timeout(Duration::from_secs(20))
            .expect("the session ends")
            .expect("the session thread")
            .expect("the session ends without error");
    }
}

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

/// Three segments, as a read spanning two chunks and a hole is answered.
fn multi_segment() -> ReadData {
    let mut data = ReadData::from_vec(b"first chunk's tail|".to_vec());
    data.push(bytes::Bytes::from_static(b"second chunk|"));
    data.push(bytes::Bytes::from(vec![0u8; 4096]));
    data
}

/// One scripted round of requests, the same on either transport: the raw
/// replies and the `Vfs` calls they made.
fn round(leg: Leg) -> (Vec<Reply>, Vec<Args>) {
    let mock = MockVfs::new();
    mock.always_getattr(Script::ok(attr(9, FileKind::File)));
    mock.always_lookup(Script::ok(entry(9, FileKind::File)));
    mock.always_open(Script::ok(Opened::new(Fh(3))));
    mock.always_write(Script::ok(5));
    mock.always_flush(Script::ok(()));
    mock.always_release(Script::ok(()));
    mock.always_fsync(Script::ok(()));
    mock.on_read(Script::ok(ReadData::from_vec(b"hello".to_vec())));
    mock.on_read(Script::ok(multi_segment()));
    // A short read (EOF inside the request) and an empty one (at EOF).
    mock.on_read(Script::ok(ReadData::from_vec(b"tail".to_vec())));
    mock.on_read(Script::ok(ReadData::default()));
    let mut k = Kernel::start(&mock, leg);
    let expected = match leg {
        Leg::DevFuse => Transport::DevFuse,
        Leg::Ring => Transport::Uring,
        Leg::RingZeroCopy => Transport::UringZeroCopy,
    };
    assert_eq!(
        k.transport, expected,
        "the session serves the leg it was asked for"
    );
    let mut replies = vec![
        k.call(op::LOOKUP, 1, &Body::default().cstr("file")),
        k.call(op::GETATTR, 9, &getattr_body()),
        k.call(
            op::OPEN,
            9,
            &Body::default().i32(libc::O_RDWR | libc::O_APPEND).u32(0),
        ),
    ];
    for size in [64, 8192, 64, 64] {
        replies.push(k.call(op::READ, 9, &read_body(3, 4096, size)));
    }
    if leg == Leg::RingZeroCopy {
        // Each read with data reached its pages through one bounce
        // `READ_FIXED`; the empty one at EOF needed none
        assert_eq!(k.ring.as_ref().unwrap().read_fixed_served(), 3);
    }
    replies.push(
        k.call(
            op::WRITE,
            9,
            &Body::default()
                .u64(3)
                .u64(10)
                .u32(5)
                .u32(0)
                .u64(0)
                .i32(libc::O_WRONLY | libc::O_SYNC)
                .u32(0)
                .bytes(b"world"),
        ),
    );
    replies.push(k.call(op::FSYNC, 9, &Body::default().u64(3).u32(0).u32(0)));
    replies.push(k.call(
        op::FLUSH,
        9,
        &Body::default().u64(3).u32(0).u32(0).u64(0xABCD),
    ));
    replies.push(k.call(
        op::RELEASE,
        9,
        &Body::default().u64(3).i32(libc::O_RDWR).u32(0).u64(0),
    ));
    k.finish();
    let calls = mock.calls().into_iter().map(|c| c.args).collect();
    (replies, calls)
}

#[test]
fn the_ring_and_dev_fuse_answer_byte_for_byte_alike() {
    let (dev, dev_calls) = round(Leg::DevFuse);
    let (ring, ring_calls) = round(Leg::Ring);
    assert_eq!(dev.len(), ring.len());
    for (i, (d, r)) in dev.iter().zip(&ring).enumerate() {
        assert_eq!(d, r, "reply {i} differs between the transports");
    }
    assert_eq!(dev_calls, ring_calls, "the Vfs saw different calls");
    // Plan 38 Z4: the same on zero-copy queues, every payload in a pool
    // buffer and every read's data bounced into its registered pages
    let (zc, zc_calls) = round(Leg::RingZeroCopy);
    assert_eq!(dev.len(), zc.len());
    for (i, (d, z)) in dev.iter().zip(&zc).enumerate() {
        assert_eq!(d, z, "reply {i} differs on zero-copy queues");
    }
    assert_eq!(
        dev_calls, zc_calls,
        "the Vfs saw different calls on zero-copy queues"
    );
    // And they are the right bytes, not merely the same ones.
    assert_eq!(ring[3].body(), b"hello", "a one-segment read");
    let joined = multi_segment().contiguous().into_owned();
    assert_eq!(
        ring[4].body(),
        &joined[..],
        "a gathered read is the segments in order"
    );
    assert_eq!(ring[5].body(), b"tail", "a short read");
    assert_eq!(ring[6].body(), b"", "a read at EOF");
    assert_eq!(
        ring[7].body(),
        5u32.to_le_bytes()
            .iter()
            .chain(&[0u8; 4])
            .copied()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        ring_calls[7],
        Args::Write {
            ino: 9,
            fh: Fh(3),
            off: 10,
            data: b"world".to_vec(),
            flags: OpenFlags::WRITE | OpenFlags::SYNC
        },
        "the write's payload, read from the ring entry's buffer"
    );
    assert_eq!(
        ring_calls[0],
        Args::Lookup {
            parent: 1,
            name: "file".into()
        }
    );
    assert_eq!(
        ring_calls[2],
        Args::Open {
            ino: 9,
            flags: OpenFlags::READ | OpenFlags::WRITE | OpenFlags::APPEND,
            owner: OpenOwner::NONE
        }
    );
    assert_eq!(
        ring_calls[9],
        Args::Flush {
            ino: 9,
            fh: Fh(3),
            owner: LockOwner(0xABCD)
        }
    );
}

/// Reads and the metadata reads stay on the ring thread that fetched them;
/// everything that may wait — opens, writes, syncs, the close path, lookups
/// — is dispatched on an offload thread, with the request still in the
/// ring entry (`dispatch_on_ring` in the vendored fuser).
#[test]
fn reads_stay_on_the_ring_and_whatever_may_block_is_offloaded() {
    let mock = MockVfs::new();
    mock.always_getattr(Script::ok(attr(9, FileKind::File)));
    mock.always_lookup(Script::ok(entry(9, FileKind::File)));
    mock.always_open(Script::ok(Opened::new(Fh(3))));
    mock.always_read(Script::ok(ReadData::from_vec(b"x".to_vec())));
    mock.always_write(Script::ok(1));
    mock.always_flush(Script::ok(()));
    mock.always_release(Script::ok(()));
    mock.always_fsync(Script::ok(()));
    let mut k = Kernel::start(&mock, Leg::Ring);
    k.call(op::GETATTR, 9, &getattr_body()).body();
    k.call(op::READ, 9, &read_body(3, 0, 1)).body();
    k.call(op::LOOKUP, 1, &Body::default().cstr("f")).body();
    k.call(op::OPEN, 9, &Body::default().i32(libc::O_RDWR).u32(0))
        .body();
    k.call(
        op::WRITE,
        9,
        &Body::default()
            .u64(3)
            .u64(0)
            .u32(1)
            .u32(0)
            .u64(0)
            .i32(libc::O_RDWR)
            .u32(0)
            .bytes(b"y"),
    )
    .body();
    k.call(op::FSYNC, 9, &Body::default().u64(3).u32(0).u32(0))
        .body();
    k.call(op::FLUSH, 9, &Body::default().u64(3).u32(0).u32(0).u64(1))
        .body();
    k.call(
        op::RELEASE,
        9,
        &Body::default().u64(3).i32(libc::O_RDWR).u32(0).u64(0),
    )
    .body();
    k.finish();
    for call in mock.calls() {
        let thread = call.thread.name.clone().unwrap_or_default();
        let on_ring = matches!(call.op, OpKind::Getattr | OpKind::Read);
        let prefix = if on_ring {
            "fuser-ring-"
        } else {
            "fuser-offload-"
        };
        assert!(thread.starts_with(prefix), "{}", call.summary());
    }
}

/// Plan 38 Z1b's ring-leg finding (`s3-cut-one-node`): a flush blocked in
/// the filesystem — waiting on a cut S3 — must not stall a `stat` or a
/// lookup the kernel queued on the same queue, as it never did over
/// `/dev/fuse`. It holds one offload thread and one entry of its queue;
/// the ring thread and the queue's other entry keep serving.
#[test]
fn a_blocked_flush_does_not_stall_its_queue() {
    let mock = MockVfs::new();
    let (entered, is_in) = mpsc::channel::<()>();
    let (release, released) = mpsc::channel::<()>();
    let released = Arc::new(std::sync::Mutex::new(released));
    let entered = Arc::new(std::sync::Mutex::new(entered));
    mock.on_flush(Script::With(Arc::new(move |_| {
        entered.lock().unwrap().send(()).unwrap();
        released.lock().unwrap().recv().unwrap();
        Ok(())
    })));
    mock.always_getattr(Script::ok(attr(1, FileKind::Dir)));
    mock.always_lookup(Script::ok(entry(9, FileKind::File)));
    let mut k = Kernel::start(&mock, Leg::Ring);
    let flush = k.send_on(
        0,
        op::FLUSH,
        9,
        &Body::default().u64(3).u32(0).u32(0).u64(1),
    );
    is_in
        .recv_timeout(Duration::from_secs(10))
        .expect("the flush reached the filesystem");
    let started = Instant::now();
    for _ in 0..3 {
        let stat = k.send_on(0, op::GETATTR, 1, &getattr_body());
        let r = k
            .recv_within(Duration::from_secs(5))
            .expect("a stat on the flush's queue is answered while the flush is blocked");
        assert_eq!(r.unique(), stat);
        r.body();
        let lookup = k.send_on(0, op::LOOKUP, 1, &Body::default().cstr("f"));
        let r = k
            .recv_within(Duration::from_secs(5))
            .expect("so is a lookup");
        assert_eq!(r.unique(), lookup);
    }
    assert!(started.elapsed() < Duration::from_secs(5));
    release.send(()).unwrap();
    let r = k.recv();
    assert_eq!(
        (r.unique(), r.error()),
        (flush, 0),
        "the flush completes after"
    );
    k.finish();
}

/// A cold read — the engine answers it from its completion pool, long
/// after the ring thread moved on — completes over the ring, and the ring
/// served other requests meanwhile. On zero-copy queues (plan 38 Z4) the
/// foreign thread's reply is a bounce `READ_FIXED` the ring thread issues
/// once woken.
#[test]
fn a_cold_read_answered_from_a_foreign_thread_completes() {
    for leg in [Leg::DevFuse, Leg::Ring, Leg::RingZeroCopy] {
        let mock = MockVfs::new();
        mock.on_read(Script::Defer {
            after: Duration::from_millis(300),
            result: Ok(multi_segment()),
        });
        mock.always_getattr(Script::ok(attr(1, FileKind::Dir)));
        let mut k = Kernel::start(&mock, leg);
        let slow = k.send(op::READ, 9, &read_body(3, 0, 8192));
        let fast = k.send(op::GETATTR, 1, &getattr_body());
        let first = k.recv();
        assert_eq!(first.unique(), fast, "{leg:?}: the getattr is not held up");
        let second = k.recv();
        assert_eq!(second.unique(), slow);
        assert_eq!(second.body(), &multi_segment().contiguous()[..], "{leg:?}");
        assert!(mock.wait_for_completions(2, Duration::from_secs(5)));
        let read = mock
            .completions()
            .into_iter()
            .find(|c| c.op == OpKind::Read)
            .unwrap();
        assert_eq!(
            read.thread.name.as_deref(),
            Some("mock-deferred"),
            "{leg:?}"
        );
        k.finish();
    }
}

/// A kernel that refuses the rings' registration after `FUSE_INIT`
/// committed the connection to them fails the session's constructor with
/// `RegistrationRefused` — which is what the frontend's mount falls back
/// from, by mounting again over `/dev/fuse` (plan 38 §2.4).
#[test]
fn a_refused_registration_fails_the_session_by_name() {
    let mock = MockVfs::new();
    let refusing = InMemoryRingKernel::refusing_registration(libc::EINVAL);
    let err = Kernel::try_start(&mock, Leg::Ring, Some(refusing.clone()))
        .err()
        .expect("refused");
    assert!(RegistrationRefused::is(&err), "{err}");
    assert_eq!(refusing.registered(), 0);
    assert!(mock.calls().is_empty());
}

/// A `fuse_lk_in` for a POSIX lock of the whole file by `owner`.
fn lk_body(fh: u64, owner: u64, typ: i32) -> Body {
    Body::default()
        .u64(fh)
        .u64(owner)
        .u64(0)
        .u64(u64::MAX)
        .i32(typ)
        .u32(4242)
        .u32(0)
        .u32(0)
}

/// The review finding on plan 38 Z2a: a ring request holds its entry until
/// it is answered, and the kernel queues a request on its CPU's queue
/// until one of that queue's entries is free — it cannot be answered
/// anywhere else. Blocking lock requests waiting on a lock held by a
/// process P could take every entry of P's queue; P's next request (a
/// `write()` before its unlock) would then never be served, nor the
/// unlock, nor the waiters: a deadlock `/dev/fuse` cannot have. With a
/// depth of two, the queue lends one entry to a waiter; the second
/// waiter is served without waiting (`ENOLCK` on a conflict), and P's
/// write and unlock go through the entry that was kept free. Once the
/// first waiter is answered its entry counts no longer, and the next
/// blocking request waits again.
#[test]
fn blocking_lock_waits_never_take_a_queues_last_entry() {
    let mock = MockVfs::reference(FrontendCaps::linux_fuse(true));
    let c = constellation_vfs::Caller::with_groups(1000, 100, &[]);
    let (made, opened) = constellation_vfs::Blocking::run(|r| {
        use constellation_vfs::Vfs;
        mock.create(
            &constellation_vfs::OpCtx::new(OpKind::Create, &c),
            constellation_vfs::ROOT_INO,
            constellation_vfs::Name::new("locked"),
            0o100_644,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .expect("create");
    let (ino, fh) = (made.attr.ino, opened.fh.0);
    mock.always_write(Script::ok(5));
    let mut k =
        Kernel::try_start_with(&mock, Leg::Ring, None, FrontendCaps::linux_fuse(true)).unwrap();
    assert_eq!(k.transport, Transport::Uring);
    let k_downgrades = k.lock_wait_downgrades.clone();
    let (p, waiter, other) = (0xA, 0xB, 0xC);
    let quiet = Duration::from_millis(300);
    let answer = |k: &mut Kernel, unique: u64, what: &str| {
        let r = k
            .recv_within(Duration::from_secs(5))
            .unwrap_or_else(|| panic!("{what}: no answer within 5 s"));
        assert_eq!(r.unique(), unique, "{what}");
        r
    };

    let held = k.send_on(0, op::SETLK, ino, &lk_body(fh, p, libc::F_WRLCK));
    assert_eq!(answer(&mut k, held, "P locks").error(), 0);
    // The first waiter takes one of the queue's two entries, and waits.
    let first = k.send_on(0, op::SETLKW, ino, &lk_body(fh, waiter, libc::F_WRLCK));
    assert!(k.recv_within(quiet).is_none(), "the waiter waits");
    // A second would take the last: it is answered at once instead.
    let second = k.send_on(0, op::SETLKW, ino, &lk_body(fh, other, libc::F_WRLCK));
    let r = answer(&mut k, second, "the second waiter");
    assert_eq!(r.error(), -libc::ENOLCK, "no entry to wait on");
    let downgrades = || k_downgrades.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(downgrades(), 1, "the downgrade is counted, once");
    // P's write and unlock reach the filesystem on the entry kept free.
    let write = k.send_on(
        0,
        op::WRITE,
        ino,
        &Body::default()
            .u64(fh)
            .u64(0)
            .u32(5)
            .u32(0)
            .u64(p)
            .i32(libc::O_RDWR)
            .u32(0)
            .bytes(b"hello"),
    );
    answer(&mut k, write, "P's write, queued behind the waiter").body();
    let unlock = k.send_on(0, op::SETLK, ino, &lk_body(fh, p, libc::F_UNLCK));
    let mut got: Vec<(u64, i32)> = (0..2)
        .map(|_| {
            let r = k.recv_within(Duration::from_secs(5)).expect("an answer");
            (r.unique(), r.error())
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![(first, 0), (unlock, 0)], "the waiter is granted");
    // Its entry is no longer counted: the next blocking request waits.
    let again = k.send_on(0, op::SETLKW, ino, &lk_body(fh, other, libc::F_WRLCK));
    assert!(k.recv_within(quiet).is_none(), "a waiter waits again");
    let release = k.send_on(0, op::SETLK, ino, &lk_body(fh, waiter, libc::F_UNLCK));
    let mut got: Vec<(u64, i32)> = (0..2)
        .map(|_| {
            let r = k.recv_within(Duration::from_secs(5)).expect("an answer");
            (r.unique(), r.error())
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![(again, 0), (release, 0)]);
    let done = k.send_on(0, op::SETLK, ino, &lk_body(fh, other, libc::F_UNLCK));
    assert_eq!(answer(&mut k, done, "the last unlock").error(), 0);
    assert_eq!(downgrades(), 1, "a waiter that waited is no downgrade");
    k.finish();
}
