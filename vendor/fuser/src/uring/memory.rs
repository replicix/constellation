//! CONSTELLATION PATCH (io-uring): this whole file is Constellation's, not upstream
//! fuser's (vendor/fuser/CONSTELLATION-PATCH.md, patches/0002-io-uring-transport.patch).
//!
//! An in-memory stand-in for the kernel's half of FUSE-over-io_uring, so that a test can drive
//! a whole ring session -- `Session`, its ring threads, the entry state machine, the commit
//! protocol, replies from foreign threads -- without io_uring, a mount, root or
//! `fuse.enable_uring=Y` (plan 38 §6).
//!
//! The seam is `RingIo` (`ring.rs`): a ring thread pushes exactly the SQEs it pushes to the
//! kernel and reaps CQEs of exactly the kernel's shape. This backend decodes each SQE the way
//! `fs/fuse/dev_uring.c` does -- `IORING_OP_URING_CMD` carrying `FUSE_IO_URING_CMD_REGISTER`
//! (two iovecs: the entry's header and payload) or `COMMIT_AND_FETCH` (a `commit_id` and a
//! queue), and `IORING_OP_POLL_ADD` of the ring's wake eventfd -- writes each request into a
//! registered entry through the iovecs its REGISTER named (`fuse_in_header` into the header,
//! the op's fixed arguments into `op_in`, the rest into the payload, `commit_id` and
//! `payload_sz` into the trailer), and on a commit reads the reply back out of the same
//! buffers, in the `/dev/fuse` wire shape. Everything above `RingIo` is the production code.
//!
//! What it does not model: which CPU's queue a request lands on (the test names the queue),
//! `FUSE_INTERRUPT`, and the kernel's own request lifetimes. `FUSE_INIT` goes over the session's
//! `/dev/fuse` descriptor as it does in the kernel, so a test pairs this with a socket pair.
//!
//! CONSTELLATION PATCH (io-uring), plan 38 Z4: [`InMemoryRingKernel::with_buffer_pools`] is a
//! 7.3 kernel: it also decodes `FUSE_IO_URING_CMD_ADD_QUEUE` (refusing `FUSE_URING_ZERO_COPY`
//! with `EPERM` unless it was told the process passes `capable(CAP_SYS_ADMIN)`) and
//! `FUSE_IO_URING_CMD_ADD_BUFPOOL`, checks every command of a queue with a registered pool for
//! `IORING_URING_CMD_FIXED` and the pool's buffer index, refuses an entry with its own payload
//! buffer on a zero-copy queue, hands each request a pool buffer and reports it in
//! `fuse_uring_ent_in_out.offset`, keeps a ring's buffer table (`register_buffer_table`), and
//! for a request sent with [`InMemoryRingKernel::send_zero_copy`] on a zero-copy queue
//! registers stand-in "client pages" in the entry's slot, sets `FUSE_URING_ENT_ZERO_COPY`,
//! serves `IORING_OP_READ_FIXED` into them with `pread(2)` from the descriptor the SQE names,
//! and reads the reply's data back out of them on commit -- copying nothing from the payload
//! buffer, as the kernel's zero-copy path does not.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::RawFd;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use nix::poll::PollFd;
use nix::poll::PollFlags;
use nix::poll::PollTimeout;
use nix::sys::eventfd::EfdFlags;
use nix::sys::eventfd::EventFd;
use parking_lot::Condvar;
use parking_lot::Mutex;

use crate::ll::fuse_abi as abi;
use crate::uring::mem::COMMIT_ID_OFFSET;
use crate::uring::mem::FLAGS_OFFSET;
use crate::uring::mem::HEADER_SZ;
use crate::uring::mem::OP_IN_OFFSET;
use crate::uring::mem::PAYLOAD_SZ_OFFSET;
use crate::uring::mem::POOL_OFFSET_OFFSET;
use crate::uring::ring::WAKE;

const IORING_OP_READ_FIXED: u8 = 4;
const IORING_OP_POLL_ADD: u8 = 6;
const IORING_OP_URING_CMD: u8 = 46;
/// `io_uring_sqe.uring_cmd_flags`' `IORING_URING_CMD_FIXED`, at byte 28 of the SQE.
const IORING_URING_CMD_FIXED: u32 = 1 << 0;
/// Byte offset of `fuse_uring_cmd_req`'s 7.46 union in the SQE: the cmd area starts at 48.
const CMD_UNION: usize = 48 + abi::FUSE_URING_CMD_REQ_UNION_OFFSET;
const MAX_REG_BUFFERS: u32 = 1 << 14;
const IORING_CQE_F_MORE: u32 = 1 << 1;
const IN_HEADER_SZ: usize = size_of::<abi::fuse_in_header>();
const OUT_HEADER_SZ: usize = size_of::<abi::fuse_out_header>();

/// The kernel side of an in-memory FUSE-over-io_uring connection; see the module doc.
///
/// Give it to a session through `Config::io_uring_kernel` (with `Config::io_uring` set) and
/// offer `FUSE_OVER_IO_URING` in the `FUSE_INIT` written to the session's descriptor: the
/// session then creates its rings on this backend instead of `io_uring_setup(2)`, registers
/// every entry here, and serves what [`Self::send`] queues. Cloning shares the connection.
#[derive(Clone)]
pub struct InMemoryRingKernel {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<State>,
    replied: Condvar,
}

/// A registered entry's two buffers, as the REGISTER's iovecs named them.
#[derive(Clone, Copy)]
struct Buffers {
    header: *mut u8,
    payload: *mut u8,
    payload_len: usize,
}

// SAFETY: the pointers name a ring's mapping, which its `Ring` keeps mapped while an entry is
// registered (it leaks the mapping rather than unmap it while a command is pending), and are
// only dereferenced under `Shared::state`'s lock while the entry is with this "kernel".
unsafe impl Send for Buffers {}

struct Entry {
    ring: usize,
    qid: u16,
    buffers: Buffers,
    /// `Some(commit_id)` while a request delivered into it awaits its commit.
    busy: Option<u64>,
    /// CONSTELLATION PATCH (io-uring): the entry's buffer-table slot on a zero-copy queue.
    slot: u16,
    /// CONSTELLATION PATCH (io-uring): the pool buffer the current request was given, as its
    /// address and size.
    pool_buf: Option<(usize, usize)>,
    /// CONSTELLATION PATCH (io-uring): the current request's "client pages", registered in
    /// `slot`, when it was zero-copied.
    pages: Option<Vec<u8>>,
}

/// A queued request: its bytes, the length of its `op_in`, and -- CONSTELLATION PATCH
/// (io-uring) -- the size of its "client pages" when it was sent to be zero-copied.
type Queued = (Vec<u8>, usize, Option<usize>);

/// CONSTELLATION PATCH (io-uring): a 7.3 kernel's buffer pools and zero-copy queues.
#[derive(Default)]
struct Pools {
    /// `ADD_QUEUE` and `ADD_BUFPOOL` are understood at all (`with_buffer_pools`).
    offered: bool,
    /// What `capable(CAP_SYS_ADMIN)` answers for the session's process.
    capable: bool,
    /// Every zero-copy `ADD_QUEUE` is refused with this errno.
    refuse_zero_copy: Option<i32>,
    /// Every `ADD_BUFPOOL` of a registered pool is refused with this errno.
    refuse_fixed: Option<i32>,
    /// The buffer table of this ring (by index) is refused with this errno.
    refuse_table: Option<(usize, i32)>,
    /// Every `READ_FIXED` reads at most this many bytes, as a short read does.
    read_fixed_cap: Option<usize>,
    /// `max_payload_sz`, from what the session's INIT reply negotiated.
    max_payload: usize,
    queues: HashMap<u16, Queue>,
    /// Per ring: the buffer table's slot count and the fixed buffer at index 0, if any.
    tables: HashMap<usize, (u32, Option<(usize, usize)>)>,
    read_fixed: u64,
}

/// CONSTELLATION PATCH (io-uring): one queue, as `ADD_QUEUE` or a first REGISTER made it.
struct Queue {
    zero_copy: bool,
    pool: Option<Pool>,
}

/// CONSTELLATION PATCH (io-uring): one queue's buffer pool.
struct Pool {
    uaddr: usize,
    buf_size: usize,
    /// The fixed-buffer index every command of the queue must name, for a registered pool.
    fixed: Option<u16>,
    free: Vec<bool>,
}

struct RingState {
    /// Completions not yet reaped, as `(user_data, res, flags)`.
    cq: VecDeque<(u64, i32, u32)>,
    /// The eventfd the ring's multishot `POLL_ADD` watches, once armed.
    wake: Option<RawFd>,
    /// Rings the ring thread's `poll(2)` when this side posts a completion.
    doorbell: Arc<EventFd>,
}

#[derive(Default)]
struct State {
    rings: Vec<RingState>,
    /// By `user_data`, which is unique per entry across the session's rings.
    entries: HashMap<u64, Entry>,
    /// CONSTELLATION PATCH (io-uring): with the size of the request's "client pages" when it
    /// was sent to be zero-copied (`send_zero_copy`).
    queued: HashMap<u16, VecDeque<Queued>>,
    pools: Pools,
    replies: VecDeque<Vec<u8>>,
    /// The connection ended: every parked fetch and later command completes `-ENOTCONN`.
    hung_up: bool,
    /// Refuse every REGISTER with this errno, as the kernel refuses a malformed one.
    refuse_register: Option<i32>,
    delivered: u64,
}

impl fmt::Debug for InMemoryRingKernel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.state.lock();
        f.debug_struct("InMemoryRingKernel")
            .field("rings", &state.rings.len())
            .field("registered", &state.entries.len())
            .field("hung_up", &state.hung_up)
            .finish_non_exhaustive()
    }
}

/// Two handles are equal when they are the same connection (`Config` is `Eq`).
impl PartialEq for InMemoryRingKernel {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl Eq for InMemoryRingKernel {}

impl Default for InMemoryRingKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRingKernel {
    /// A connection that accepts every well-formed REGISTER.
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                replied: Condvar::new(),
            }),
        }
    }

    /// A connection that refuses every REGISTER with `errno`, synchronously, as the kernel
    /// refuses one it cannot accept: the session's constructor then fails (the INIT reply has
    /// already committed the connection to rings) and the caller decides what to do.
    pub fn refusing_registration(errno: i32) -> Self {
        let kernel = Self::new();
        kernel.shared.state.lock().refuse_register = Some(errno);
        kernel
    }

    /// CONSTELLATION PATCH (io-uring): a 7.3 connection, with buffer pools and zero-copy
    /// queues (module doc); `capable` is what the kernel's `capable(CAP_SYS_ADMIN)` says for
    /// the session's process, which is all a session asks before it tries zero-copy (the
    /// in-memory kernel stands in for the process's capabilities, so that an unprivileged test
    /// can drive both sides of that decision). Offer `FUSE_HAS_IO_URING_BUFPOOL` in the
    /// `FUSE_INIT` for the session to use it.
    pub fn with_buffer_pools(capable: bool) -> Self {
        let kernel = Self::new();
        {
            let mut state = kernel.shared.state.lock();
            state.pools.offered = true;
            state.pools.capable = capable;
        }
        kernel
    }

    /// CONSTELLATION PATCH (io-uring): refuses every zero-copy `ADD_QUEUE` with `errno`, as
    /// a kernel whose `capable()` check fails does (`EPERM`).
    pub fn refusing_zero_copy_queues(self, errno: i32) -> Self {
        self.shared.state.lock().pools.refuse_zero_copy = Some(errno);
        self
    }

    /// CONSTELLATION PATCH (io-uring): refuses every `ADD_BUFPOOL` of a registered pool with
    /// `errno` (an unregistered one is still taken).
    pub fn refusing_registered_pools(self, errno: i32) -> Self {
        self.shared.state.lock().pools.refuse_fixed = Some(errno);
        self
    }

    /// CONSTELLATION PATCH (io-uring): refuses the buffer table of ring `ring` with `errno`, as
    /// a kernel refuses one it cannot pin or account (`ENOMEM` past `RLIMIT_MEMLOCK`); every
    /// other ring's is taken.
    pub fn refusing_buffer_table(self, ring: usize, errno: i32) -> Self {
        self.shared.state.lock().pools.refuse_table = Some((ring, errno));
        self
    }

    /// CONSTELLATION PATCH (io-uring): every `IORING_OP_READ_FIXED` reads at most `cap` bytes
    /// -- a short read, which a file at its end gives and a bounce memfd must not.
    pub fn short_read_fixed(self, cap: usize) -> Self {
        self.shared.state.lock().pools.read_fixed_cap = Some(cap);
        self
    }

    /// CONSTELLATION PATCH (io-uring): buffer tables registered so far, one per ring at most.
    pub fn buffer_tables(&self) -> usize {
        self.shared.state.lock().pools.tables.len()
    }

    /// CONSTELLATION PATCH (io-uring): whether the session's process passes
    /// `capable(CAP_SYS_ADMIN)` here (`with_buffer_pools`).
    pub(crate) fn capable_sys_admin(&self) -> bool {
        self.shared.state.lock().pools.capable
    }

    /// CONSTELLATION PATCH (io-uring): what the session's INIT reply negotiated, from which the
    /// kernel sizes a pool's buffers (`max_payload_sz`), as `fuse_uring_create` does.
    pub(crate) fn negotiated(&self, max_payload: usize) {
        self.shared.state.lock().pools.max_payload = max_payload;
    }

    /// CONSTELLATION PATCH (io-uring): queues a request whose data the kernel would hand over
    /// as `pages` bytes of registered pages (a `FUSE_READ` of a `FOPEN_IO_URING_ZERO_COPY`
    /// open): zero-copied when it lands on a zero-copy queue, copied as [`Self::send`] does
    /// otherwise, as the kernel decides per queue (`can_zero_copy_req`). The reply's data is
    /// whatever the session put into the pages.
    pub fn send_zero_copy(&self, qid: u16, request: &[u8], op_in_len: usize, pages: usize) {
        assert!(request.len() >= IN_HEADER_SZ + op_in_len, "a whole request");
        let mut state = self.shared.state.lock();
        state
            .queued
            .entry(qid)
            .or_default()
            .push_back((request.to_vec(), op_in_len, Some(pages)));
        state.deliver();
    }

    /// CONSTELLATION PATCH (io-uring): queues created for zero-copy so far.
    pub fn zero_copy_queues(&self) -> usize {
        let state = self.shared.state.lock();
        state.pools.queues.values().filter(|q| q.zero_copy).count()
    }

    /// CONSTELLATION PATCH (io-uring): queues given a buffer pool so far, and how many of
    /// those pools are registered fixed buffers.
    pub fn pools(&self) -> (usize, usize) {
        let state = self.shared.state.lock();
        let pools: Vec<&Pool> = state
            .pools
            .queues
            .values()
            .filter_map(|q| q.pool.as_ref())
            .collect();
        (
            pools.len(),
            pools.iter().filter(|p| p.fixed.is_some()).count(),
        )
    }

    /// CONSTELLATION PATCH (io-uring): `IORING_OP_READ_FIXED`s served into registered pages.
    pub fn read_fixed_served(&self) -> u64 {
        self.shared.state.lock().pools.read_fixed
    }

    /// Queues `request` -- a whole `/dev/fuse` request: `fuse_in_header`, the op's fixed
    /// arguments (`op_in_len` bytes, a multiple of 8, at most 128), then its payload -- on
    /// queue `qid`. It is delivered into the next entry of that queue the session has
    /// registered and not yet been handed a request in, scattered as the kernel scatters it.
    pub fn send(&self, qid: u16, request: &[u8], op_in_len: usize) {
        assert!(request.len() >= IN_HEADER_SZ + op_in_len, "a whole request");
        assert!(
            op_in_len % 8 == 0 && op_in_len <= abi::FUSE_URING_OP_IN_OUT_SZ,
            "op_in is whole 8-byte words, at most 128 bytes"
        );
        let mut state = self.shared.state.lock();
        state
            .queued
            .entry(qid)
            .or_default()
            .push_back((request.to_vec(), op_in_len, None));
        state.deliver();
    }

    /// The next reply a commit carried, in the `/dev/fuse` wire shape (`fuse_out_header`,
    /// then `payload_sz` bytes), or `None` if none arrives within `timeout`.
    pub fn recv(&self, timeout: Duration) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.shared.state.lock();
        loop {
            if let Some(reply) = state.replies.pop_front() {
                return Some(reply);
            }
            if self
                .shared
                .replied
                .wait_until(&mut state, deadline)
                .timed_out()
            {
                return state.replies.pop_front();
            }
        }
    }

    /// Entries registered by the session (on every ring) and not refused.
    pub fn registered(&self) -> usize {
        self.shared.state.lock().entries.len()
    }

    /// Requests delivered into an entry so far.
    pub fn delivered(&self) -> u64 {
        self.shared.state.lock().delivered
    }

    /// The connection ends, as at unmount or a fusectl abort: every entry parked for a fetch
    /// completes `-ENOTCONN`, and so does every command pushed later.
    pub fn hang_up(&self) {
        let mut state = self.shared.state.lock();
        state.hung_up = true;
        let parked: Vec<u64> = state
            .entries
            .iter()
            .filter(|(_, e)| e.busy.is_none())
            .map(|(ud, _)| *ud)
            .collect();
        for ud in parked {
            let ring = state.entries.remove(&ud).unwrap().ring;
            state.complete(ring, ud, -libc::ENOTCONN, 0);
        }
    }

    /// The backend of ring `index` of the session (`RingSet::new`).
    pub(crate) fn ring(&self, index: usize) -> io::Result<MemoryRing> {
        let doorbell = Arc::new(
            EventFd::from_value_and_flags(0, EfdFlags::EFD_CLOEXEC | EfdFlags::EFD_NONBLOCK)
                .map_err(io::Error::from)?,
        );
        let mut state = self.shared.state.lock();
        if state.rings.len() <= index {
            state.rings.resize_with(index + 1, || RingState {
                cq: VecDeque::new(),
                wake: None,
                doorbell: doorbell.clone(),
            });
        }
        state.rings[index] = RingState {
            cq: VecDeque::new(),
            wake: None,
            doorbell: doorbell.clone(),
        };
        Ok(MemoryRing {
            kernel: self.clone(),
            index,
            doorbell,
            sq: Vec::new(),
        })
    }
}

fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(b[off..off + 2].try_into().unwrap())
}

fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}

impl State {
    fn complete(&mut self, ring: usize, user_data: u64, res: i32, flags: u32) {
        if let Some(r) = self.rings.get_mut(ring) {
            r.cq.push_back((user_data, res, flags));
            let _ = r.doorbell.write(1);
        }
    }

    /// One SQE of ring `ring`, as `io_uring_enter` issues it.
    fn issue(&mut self, ring: usize, sqe: &[u8; 128], replied: &Condvar) {
        let user_data = u64_at(sqe, 32);
        match sqe[0] {
            IORING_OP_POLL_ADD => {
                if let Some(r) = self.rings.get_mut(ring) {
                    r.wake = Some(u32_at(sqe, 4) as RawFd);
                }
            }
            IORING_OP_READ_FIXED => {
                let res = self.read_fixed(ring, sqe);
                self.complete(ring, user_data, res, 0);
            }
            IORING_OP_URING_CMD if self.hung_up => {
                self.complete(ring, user_data, -libc::ENOTCONN, 0)
            }
            IORING_OP_URING_CMD => {
                let qid = u16_at(sqe, 64);
                let commit_id = u64_at(sqe, 56);
                // `io_uring_sqe.uring_cmd_flags` and `buf_index`
                let fixed =
                    (u32_at(sqe, 28) & IORING_URING_CMD_FIXED != 0).then(|| u16_at(sqe, 40));
                match u32_at(sqe, 8) {
                    c if c == abi::fuse_uring_cmd::FUSE_IO_URING_CMD_REGISTER as u32 => {
                        // `fuse_uring_get_iovec_from_sqe`: exactly two iovecs
                        if u32_at(sqe, 24) != 2 {
                            return self.complete(ring, user_data, -libc::EINVAL, 0);
                        }
                        if let Some(errno) = self.refuse_register {
                            return self.complete(ring, user_data, -errno, 0);
                        }
                        let iov = u64_at(sqe, 16) as *const libc::iovec;
                        // SAFETY: a REGISTER names its entry's `[libc::iovec; 2]`, which lives
                        // as long as the ring (`RingEntry::iov`).
                        let (header, payload) = unsafe { (ptr::read(iov), ptr::read(iov.add(1))) };
                        if header.iov_len < HEADER_SZ {
                            return self.complete(ring, user_data, -libc::EINVAL, 0);
                        }
                        // CONSTELLATION PATCH (io-uring): `fuse_uring_create_ring_ent`
                        let slot = u16_at(sqe, CMD_UNION);
                        let queue = self.pools.queues.entry(qid).or_insert(Queue {
                            zero_copy: false,
                            pool: None,
                        });
                        let refused = if slot != 0 && !queue.zero_copy {
                            true
                        } else if let Some(pool) = &queue.pool {
                            !payload.iov_base.is_null()
                                || payload.iov_len != 0
                                || pool.fixed.is_some_and(|i| fixed != Some(i))
                        } else {
                            queue.zero_copy
                        };
                        if refused {
                            return self.complete(ring, user_data, -libc::EINVAL, 0);
                        }
                        self.entries.insert(
                            user_data,
                            Entry {
                                ring,
                                qid,
                                buffers: Buffers {
                                    header: header.iov_base.cast(),
                                    payload: payload.iov_base.cast(),
                                    payload_len: payload.iov_len,
                                },
                                busy: None,
                                slot,
                                pool_buf: None,
                                pages: None,
                            },
                        );
                        self.deliver();
                    }
                    c if c == abi::fuse_uring_cmd::FUSE_IO_URING_CMD_COMMIT_AND_FETCH as u32 => {
                        // CONSTELLATION PATCH (io-uring): `fuse_uring_cmd_index_ok`
                        let index_ok = self
                            .pools
                            .queues
                            .get(&qid)
                            .and_then(|q| q.pool.as_ref())
                            .and_then(|p| p.fixed)
                            .is_none_or(|i| fixed == Some(i));
                        if !index_ok {
                            return self.complete(ring, user_data, -libc::EINVAL, 0);
                        }
                        let Some(entry) = self
                            .entries
                            .get_mut(&user_data)
                            .filter(|e| e.qid == qid && e.busy == Some(commit_id))
                        else {
                            // `fuse_uring_commit_fetch`: no request with this id in flight
                            return self.complete(ring, user_data, -libc::ENOENT, 0);
                        };
                        entry.busy = None;
                        let b = entry.buffers;
                        let pages = entry.pages.take();
                        let pool_buf = entry.pool_buf.take();
                        let at = pool_buf.map_or(b.payload, |(addr, _)| addr as *mut u8);
                        let cap = pool_buf.map_or(b.payload_len, |(_, len)| len);
                        // SAFETY: the entry was with userspace until this commit, whose
                        // submit hands its buffers back; the header area holds the 16-byte
                        // out header and the trailer, the payload buffer (the entry's own, or
                        // the pool buffer this request was given) `payload_sz` bytes, as the
                        // kernel's `fuse_uring_copy_from_ring` reads them -- except for a
                        // zero-copied request, whose data is in its pages.
                        let reply = unsafe {
                            let payload_sz =
                                ptr::read_unaligned(b.header.add(PAYLOAD_SZ_OFFSET).cast::<u32>())
                                    as usize;
                            let mut reply = vec![0u8; OUT_HEADER_SZ];
                            ptr::copy_nonoverlapping(b.header, reply.as_mut_ptr(), OUT_HEADER_SZ);
                            match &pages {
                                Some(pages) => {
                                    reply.extend_from_slice(&pages[..payload_sz.min(pages.len())])
                                }
                                None => {
                                    let n = payload_sz.min(cap);
                                    reply.resize(OUT_HEADER_SZ + n, 0);
                                    ptr::copy_nonoverlapping(
                                        at,
                                        reply.as_mut_ptr().add(OUT_HEADER_SZ),
                                        n,
                                    );
                                }
                            }
                            reply
                        };
                        if let Some((addr, _)) = pool_buf {
                            self.free_pool_buf(qid, addr);
                        }
                        self.replies.push_back(reply);
                        replied.notify_all();
                        self.deliver();
                    }
                    c if self.pools.offered
                        && c == abi::fuse_uring_cmd::FUSE_IO_URING_CMD_ADD_QUEUE as u32 =>
                    {
                        let res = self.add_queue(qid, u64_at(sqe, 48));
                        self.complete(ring, user_data, res, 0);
                    }
                    c if self.pools.offered
                        && c == abi::fuse_uring_cmd::FUSE_IO_URING_CMD_ADD_BUFPOOL as u32 =>
                    {
                        let res = self.add_bufpool(qid, sqe, fixed);
                        self.complete(ring, user_data, res, 0);
                    }
                    _ => self.complete(ring, user_data, -libc::EINVAL, 0),
                }
            }
            _ => self.complete(ring, user_data, -libc::EINVAL, 0),
        }
    }

    /// CONSTELLATION PATCH (io-uring): `fuse_uring_add_queue`.
    fn add_queue(&mut self, qid: u16, flags: u64) -> i32 {
        if flags & !abi::FUSE_URING_ZERO_COPY != 0 {
            return -libc::EINVAL;
        }
        let zero_copy = flags & abi::FUSE_URING_ZERO_COPY != 0;
        if zero_copy {
            if !self.pools.capable {
                return -libc::EPERM;
            }
            if let Some(errno) = self.pools.refuse_zero_copy {
                return -errno;
            }
        }
        if self.pools.queues.contains_key(&qid) {
            return -libc::EEXIST;
        }
        self.pools.queues.insert(
            qid,
            Queue {
                zero_copy,
                pool: None,
            },
        );
        0
    }

    /// CONSTELLATION PATCH (io-uring): `fuse_uring_add_bufpool`.
    fn add_bufpool(&mut self, qid: u16, sqe: &[u8; 128], fixed: Option<u16>) -> i32 {
        let (uaddr, len, reserved) = (
            u64_at(sqe, CMD_UNION) as usize,
            u32_at(sqe, CMD_UNION + 8) as usize,
            u32_at(sqe, CMD_UNION + 12),
        );
        let buf_size = self.pools.max_payload;
        if u64_at(sqe, 48) != 0 || reserved != 0 || buf_size == 0 || len / buf_size == 0 {
            return -libc::EINVAL;
        }
        if let (Some(_), Some(errno)) = (fixed, self.pools.refuse_fixed) {
            return -errno;
        }
        let Some(queue) = self.pools.queues.get_mut(&qid) else {
            return -libc::EINVAL;
        };
        if queue.pool.is_some() {
            return -libc::EINVAL;
        }
        queue.pool = Some(Pool {
            uaddr,
            buf_size,
            fixed,
            free: vec![true; len / buf_size],
        });
        0
    }

    fn free_pool_buf(&mut self, qid: u16, addr: usize) {
        if let Some(pool) = self
            .pools
            .queues
            .get_mut(&qid)
            .and_then(|q| q.pool.as_mut())
        {
            let i = (addr - pool.uaddr) / pool.buf_size;
            pool.free[i] = true;
        }
    }

    /// CONSTELLATION PATCH (io-uring): `IORING_OP_READ_FIXED` into a buffer-table slot that
    /// holds a zero-copied request's pages; the destination address is an offset into them.
    fn read_fixed(&mut self, ring: usize, sqe: &[u8; 128]) -> i32 {
        let (fd, off, addr, len, index) = (
            u32_at(sqe, 4) as RawFd,
            u64_at(sqe, 8),
            u64_at(sqe, 16) as usize,
            u32_at(sqe, 24) as usize,
            u16_at(sqe, 40),
        );
        let slots = self.pools.tables.get(&ring).map_or(0, |t| t.0);
        if u32::from(index) >= slots {
            return -libc::EFAULT;
        }
        let Some(pages) = self
            .entries
            .values_mut()
            .filter(|e| e.ring == ring && e.slot == index)
            .find_map(|e| e.pages.as_mut())
        else {
            return -libc::EFAULT;
        };
        if addr.checked_add(len).is_none_or(|end| end > pages.len()) {
            return -libc::EFAULT;
        }
        let len = self.pools.read_fixed_cap.map_or(len, |cap| len.min(cap));
        // SAFETY: the destination is inside `pages`; `fd` is the session's to keep open until
        // this completes, which it does right here.
        let n = unsafe {
            libc::pread(
                fd,
                pages.as_mut_ptr().add(addr).cast(),
                len,
                off as libc::off_t,
            )
        };
        self.pools.read_fixed += 1;
        if n < 0 {
            -io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO)
        } else {
            n as i32
        }
    }

    /// Hands queued requests to parked entries of their queues.
    fn deliver(&mut self) {
        if self.hung_up {
            return;
        }
        let mut parked: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| e.busy.is_none())
            .map(|(ud, _)| *ud)
            .collect();
        parked.sort_unstable();
        for ud in parked {
            let qid = self.entries[&ud].qid;
            // CONSTELLATION PATCH (io-uring): a request on a queue with a pool waits for a free
            // buffer, unless it is zero-copied (its data is in its pages); one sent for
            // zero-copy is zero-copied only on a zero-copy queue (`can_zero_copy_req`)
            let (zero_copy_queue, pooled) = self
                .pools
                .queues
                .get(&qid)
                .map_or((false, false), |q| (q.zero_copy, q.pool.is_some()));
            let Some(&(_, _, pages)) = self.queued.get(&qid).and_then(|q| q.front()) else {
                continue;
            };
            let zero_copied = zero_copy_queue && pages.is_some();
            let pool_buf = if pooled && !zero_copied {
                let pool = self
                    .pools
                    .queues
                    .get_mut(&qid)
                    .and_then(|q| q.pool.as_mut())
                    .unwrap();
                let Some(i) = pool.free.iter().position(|f| *f) else {
                    continue;
                };
                pool.free[i] = false;
                Some((
                    pool.uaddr + i * pool.buf_size,
                    pool.buf_size,
                    i * pool.buf_size,
                ))
            } else {
                None
            };
            let (request, op_in_len, _) = self.queued.get_mut(&qid).unwrap().pop_front().unwrap();
            let entry = self.entries.get_mut(&ud).unwrap();
            let b = entry.buffers;
            let unique = u64_at(&request, 8);
            let op_in = &request[IN_HEADER_SZ..IN_HEADER_SZ + op_in_len];
            let payload: &[u8] = if zero_copied {
                &[]
            } else {
                &request[IN_HEADER_SZ + op_in_len..]
            };
            let (payload_at, payload_len, offset) = match pool_buf {
                Some((addr, len, offset)) => (addr as *mut u8, len, offset as u32),
                None => (b.payload, b.payload_len, 0),
            };
            assert!(
                payload.len() <= payload_len,
                "a request's payload fits the entry's buffer"
            );
            // SAFETY: the entry is parked with this "kernel" (its REGISTER or commit is
            // pending), so nothing in userspace touches its buffers; every write lies inside
            // the header area or the payload buffer the REGISTER named (or the pool buffer
            // just taken for it).
            unsafe {
                ptr::copy_nonoverlapping(request.as_ptr(), b.header, IN_HEADER_SZ);
                ptr::copy_nonoverlapping(op_in.as_ptr(), b.header.add(OP_IN_OFFSET), op_in.len());
                ptr::write_unaligned(
                    b.header.add(FLAGS_OFFSET).cast::<u64>(),
                    if zero_copied {
                        abi::FUSE_URING_ENT_ZERO_COPY
                    } else {
                        0
                    },
                );
                ptr::write_unaligned(b.header.add(COMMIT_ID_OFFSET).cast::<u64>(), unique);
                ptr::write_unaligned(
                    b.header.add(PAYLOAD_SZ_OFFSET).cast::<u32>(),
                    payload.len() as u32,
                );
                ptr::write_unaligned(b.header.add(POOL_OFFSET_OFFSET).cast::<u32>(), offset);
                ptr::copy_nonoverlapping(payload.as_ptr(), payload_at, payload.len());
            }
            entry.busy = Some(unique);
            entry.pool_buf = pool_buf.map(|(addr, len, _)| (addr, len));
            entry.pages = zero_copied.then(|| vec![0; pages.unwrap_or(0)]);
            let ring = entry.ring;
            self.delivered += 1;
            self.complete(ring, ud, 0, 0);
        }
    }
}

/// One ring's view of an `InMemoryRingKernel`: its submission queue, and its completion queue
/// in the shared state. Owned by the ring thread, as an io_uring is.
pub(crate) struct MemoryRing {
    kernel: InMemoryRingKernel,
    index: usize,
    doorbell: Arc<EventFd>,
    sq: Vec<[u8; 128]>,
}

impl MemoryRing {
    pub(crate) fn push(&mut self, sqe: [u8; 128]) {
        self.sq.push(sqe);
    }

    pub(crate) fn submit(&mut self) -> io::Result<usize> {
        let n = self.sq.len();
        let shared = &self.kernel.shared;
        let mut state = shared.state.lock();
        for sqe in self.sq.drain(..) {
            state.issue(self.index, &sqe, &shared.replied);
        }
        Ok(n)
    }

    /// Submits, then blocks until a completion is posted or the wake eventfd is readable (a
    /// commit from another thread), as `io_uring_enter(.., min_complete = 1, ..)` with the
    /// multishot poll armed would; `timed` bounds the wait to 10 ms.
    pub(crate) fn submit_and_wait(&mut self, timeout: std::time::Duration) -> io::Result<usize> {
        let n = self.submit()?;
        loop {
            let wake = {
                let mut state = self.kernel.shared.state.lock();
                let ring = &mut state.rings[self.index];
                if !ring.cq.is_empty() {
                    return Ok(n);
                }
                ring.wake
            };
            // SAFETY: the wake eventfd belongs to the `Ring` this ring thread serves, which
            // outlives the thread's loop.
            let wake_fd = wake.map(|fd| unsafe { BorrowedFd::borrow_raw(fd) });
            let mut fds = vec![PollFd::new(self.doorbell.as_fd(), PollFlags::POLLIN)];
            if let Some(fd) = wake_fd {
                fds.push(PollFd::new(fd, PollFlags::POLLIN));
            }
            let timeout =
                PollTimeout::try_from(timeout.as_millis()).unwrap_or(PollTimeout::MAX);
            match nix::poll::poll(&mut fds, timeout) {
                Ok(0) => return Ok(n),
                Ok(_) => {}
                Err(nix::errno::Errno::EINTR) => continue,
                Err(err) => return Err(err.into()),
            }
            let woken = fds
                .get(1)
                .and_then(PollFd::revents)
                .is_some_and(|r| r.contains(PollFlags::POLLIN));
            let _ = self.doorbell.read();
            if woken {
                // The multishot poll fires; the ring thread drains the eventfd itself
                let mut state = self.kernel.shared.state.lock();
                state.rings[self.index].cq.push_back((
                    WAKE,
                    libc::POLLIN as i32,
                    IORING_CQE_F_MORE,
                ));
                return Ok(n);
            }
        }
    }

    pub(crate) fn reap(&mut self, out: &mut impl Extend<(u64, i32, u32)>) {
        let mut state = self.kernel.shared.state.lock();
        out.extend(state.rings[self.index].cq.drain(..));
    }

    /// CONSTELLATION PATCH (io-uring): `IORING_REGISTER_BUFFERS2` with sparse slots, then the
    /// pool at index 0 (`RingIo::register_buffer_table`).
    pub(crate) fn register_buffer_table(
        &mut self,
        pool: Option<libc::iovec>,
        slots: u32,
    ) -> io::Result<()> {
        let mut state = self.kernel.shared.state.lock();
        if slots == 0 || slots > MAX_REG_BUFFERS {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        if state.pools.tables.contains_key(&self.index) {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        if let Some((_, errno)) = state.pools.refuse_table.filter(|(r, _)| *r == self.index) {
            return Err(io::Error::from_raw_os_error(errno));
        }
        let pool = pool.map(|iov| (iov.iov_base as usize, iov.iov_len));
        state.pools.tables.insert(self.index, (slots, pool));
        Ok(())
    }

    /// CONSTELLATION PATCH (io-uring): `IORING_UNREGISTER_BUFFERS`.
    pub(crate) fn unregister_buffer_table(&mut self) -> io::Result<()> {
        let mut state = self.kernel.shared.state.lock();
        match state.pools.tables.remove(&self.index) {
            Some(_) => Ok(()),
            None => Err(io::Error::from_raw_os_error(libc::ENXIO)),
        }
    }
}
